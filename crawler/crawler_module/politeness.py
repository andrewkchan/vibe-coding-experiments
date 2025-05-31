import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio # Added for asyncio.to_thread
import sqlite3 # For type hinting in db access functions
from typing import Optional

from .config import CrawlerConfig
from .storage import StorageManager
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult # Added Fetcher import

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_TXT_TTL = 24 * 60 * 60  # 24 hours in seconds
MIN_CRAWL_DELAY_SECONDS = 70 # Our project's default minimum

class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage
        self.fetcher = fetcher # Added fetcher instance
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {} # Cache for parsed robots.txt
        # _load_manual_exclusions is synchronous and uses DB.
        # It's called during init, so it will manage its own connection.
        self._load_manual_exclusions()

    def _load_manual_exclusions(self, conn_in: Optional[sqlite3.Connection] = None):
        """Loads manually excluded domains from the config file into the DB."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_managed_internally = False
            conn: Optional[sqlite3.Connection] = conn_in
            cursor: Optional[sqlite3.Cursor] = None # Ensure cursor is defined for finally

            if conn is None:
                conn = self.storage.db_pool.get_connection()
                conn_managed_internally = True
            
            if not conn: # Should not happen if pool works or conn_in is valid
                logger.error("Failed to get a database connection for loading manual exclusions.")
                return

            try:
                cursor = conn.cursor()
                with open(self.config.exclude_file, 'r') as f:
                    count = 0
                    for line in f:
                        domain_to_exclude = line.strip().lower()
                        if domain_to_exclude and not domain_to_exclude.startswith("#"):
                            cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                            cursor.execute(
                                "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                (domain_to_exclude,)
                            )
                            count += 1
                    conn.commit() # Commit changes
                    logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file}.")
            except IOError as e:
                logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                if conn: conn.rollback() # Rollback if IO error after potential DB ops
            except sqlite3.Error as e: # Catch DB errors from execute/commit
                logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                if conn: conn.rollback()
                raise 
            finally:
                if cursor: cursor.close()
                if conn_managed_internally and conn:
                    self.storage.db_pool.return_connection(conn)
        else:
            logger.info("No manual exclude file specified or found.")

    def _db_get_cached_robots_sync(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> tuple | None:
        conn_managed_internally = False
        conn: Optional[sqlite3.Connection] = conn_in
        cursor: Optional[sqlite3.Cursor] = None

        if conn is None:
            conn = self.storage.db_pool.get_connection()
            conn_managed_internally = True
        
        if not conn:
            logger.error(f"DB: Failed to get connection for cached robots ({domain}).")
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                (domain,)
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.warning(f"DB error fetching cached robots.txt for {domain}: {e}")
            return None
        finally:
            if cursor: cursor.close()
            if conn_managed_internally and conn:
                self.storage.db_pool.return_connection(conn)

    def _db_update_robots_cache_sync(self, domain: str, robots_content: str, fetched_timestamp: int, expires_timestamp: int, conn_in: Optional[sqlite3.Connection] = None):
        conn_managed_internally = False
        conn: Optional[sqlite3.Connection] = conn_in
        cursor: Optional[sqlite3.Cursor] = None

        if conn is None:
            conn = self.storage.db_pool.get_connection()
            conn_managed_internally = True

        if not conn:
            logger.error(f"DB: Failed to get connection for updating robots cache ({domain}).")
            return

        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
            cursor.execute(
                "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                (robots_content, fetched_timestamp, expires_timestamp, domain)
            )
            conn.commit()
            logger.debug(f"Cached robots.txt for {domain} in DB.")
        except sqlite3.Error as e_inner: 
            logger.error(f"DB execute/commit error caching robots.txt for {domain}: {e_inner}")
            if conn: conn.rollback()
            raise
        finally:
            if cursor: cursor.close()
            if conn_managed_internally and conn:
                self.storage.db_pool.return_connection(conn)

    async def _get_robots_for_domain(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain. Uses provided conn for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None

        try:
            # Use passed-in connection for this synchronous DB operation executed in a thread
            db_row = await asyncio.to_thread(self._db_get_cached_robots_sync, domain, conn_in)
        except Exception as e: 
            logger.error(f"Unexpected error during DB cache check for {domain}: {e}", exc_info=True)


        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            # If this fresh DB content matches an existing in-memory parser (by source), reuse parser object
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
            # Else, we have fresh content from DB, will parse it below and update in-memory cache.
        elif domain in self.robots_parsers and (not db_row or not (db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time)):
            if not (db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time):
                if domain in self.robots_parsers:
                     logger.debug(f"DB cache miss/stale for {domain}. Using pre-existing in-memory robots_parser.")
                     return self.robots_parsers[domain]
        
        # If robots_content is still None here, it means DB was not fresh/valid and we need to fetch
        if robots_content is None:
            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found: # If HTTP didn't succeed with 200
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else: 
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            # Ensure robots_content has a default value if all attempts failed to set it
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            if robots_content is not None: # robots_content will always be a string here
                try:
                    # Use passed-in connection for this synchronous DB operation executed in a thread
                    await asyncio.to_thread(self._db_update_robots_cache_sync, domain, robots_content, fetched_timestamp, expires_timestamp, conn_in)
                except Exception as e: 
                     logger.error(f"Unexpected error caching robots.txt for {domain}: {e}")

        if robots_content is not None:
            rerp.parse(robots_content)
            rerp.source_content = robots_content 
        else: 
            rerp.parse("") 
            rerp.source_content = ""
        
        self.robots_parsers[domain] = rerp # Update/add to in-memory cache
        return rerp

    def _db_check_manual_exclusion_sync(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> tuple | None:
        conn_managed_internally = False
        conn: Optional[sqlite3.Connection] = conn_in
        cursor: Optional[sqlite3.Cursor] = None

        if conn is None:
            conn = self.storage.db_pool.get_connection()
            conn_managed_internally = True
        
        if not conn:
            logger.error(f"DB: Failed to get connection for manual exclusion check ({domain}).")
            return None

        try:
            cursor = conn.cursor()
            if self.config.seeded_urls_only:
                cursor.execute("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?", (domain,))
            else:
                cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.warning(f"DB error checking manual exclusion for {domain}: {e}")
            return None
        finally:
            if cursor: cursor.close()
            if conn_managed_internally and conn:
                self.storage.db_pool.return_connection(conn)

    async def is_url_allowed(self, url: str, conn_in: Optional[sqlite3.Connection] = None) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded. Uses provided conn."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        conn_managed_internally = False
        active_conn: Optional[sqlite3.Connection] = conn_in

        if active_conn is None:
            try:
                active_conn = self.storage.db_pool.get_connection()
                conn_managed_internally = True
            except Exception as e: # Catch pool errors specifically
                logger.error(f"Failed to get DB connection for is_url_allowed ({domain}): {e}")
                return True # Default to allow if DB is unavailable for checks

        if not active_conn: # Should be caught by above, but as a safeguard
             logger.error(f"DB connection unavailable for is_url_allowed checks ({domain}). Defaulting to allow.")
             return True

        try:
            manual_exclusion_check_passed = False
            try:
                # Use the active_conn for this synchronous DB operation executed in a thread
                row = await asyncio.to_thread(self._db_check_manual_exclusion_sync, domain, active_conn)
                if row and row[0] == 1:
                    logger.debug(f"URL {url} from manually excluded or non-seeded domain: {domain}")
                    # No need to return False yet, let finally block handle connection return
                else:
                    manual_exclusion_check_passed = True # Not excluded
            except Exception as e: 
                logger.warning(f"Error checking manual exclusion for {domain} ({type(e).__name__}: {e}). Defaulting to allow this check.")
                manual_exclusion_check_passed = True # If check fails, assume not excluded for safety

            if not manual_exclusion_check_passed:
                return False # Manually excluded or error that defaults to excluded (though current logic defaults to allow)

            # If not manually excluded, check robots.txt, passing the same active_conn
            rerp = await self._get_robots_for_domain(domain, active_conn)
            if rerp:
                is_allowed_by_robots = rerp.is_allowed(self.config.user_agent, url)
                if not is_allowed_by_robots:
                    logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
                return is_allowed_by_robots
            
            logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
            return True # Default to allow if RERP is None

        finally:
            if conn_managed_internally and active_conn:
                self.storage.db_pool.return_connection(active_conn)

    async def get_crawl_delay(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> float:
        """Gets the crawl delay for a domain. Uses provided conn for DB ops in _get_robots_for_domain."""
        # Pass the connection to _get_robots_for_domain
        rerp = await self._get_robots_for_domain(domain, conn_in)
        delay = None
        if rerp:
            agent_delay = rerp.get_crawl_delay(self.config.user_agent)
            if agent_delay is not None:
                delay = float(agent_delay)
                logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (agent: {self.config.user_agent})")
        
        if delay is None:
            if rerp and rerp.get_crawl_delay("*") is not None:
                 wildcard_delay = rerp.get_crawl_delay("*")
                 if wildcard_delay is not None:
                    delay = float(wildcard_delay)
                    logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (wildcard agent)")
            
        if delay is None:
            logger.debug(f"No crawl delay specified in robots.txt for {domain}. Using default: {MIN_CRAWL_DELAY_SECONDS}s")
            return float(MIN_CRAWL_DELAY_SECONDS)
        
        return max(float(delay), float(MIN_CRAWL_DELAY_SECONDS))

    def _db_get_last_fetch_sync(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> tuple | None:
        conn_managed_internally = False
        conn: Optional[sqlite3.Connection] = conn_in
        cursor: Optional[sqlite3.Cursor] = None

        if conn is None:
            conn = self.storage.db_pool.get_connection()
            conn_managed_internally = True
        
        if not conn:
            logger.error(f"DB: Failed to get connection for last fetch time ({domain}).")
            return None
            
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"DB error checking can_fetch_domain_now for {domain}: {e}")
            return None # Propagate as None, effectively preventing fetch if DB error
        finally:
            if cursor: cursor.close()
            if conn_managed_internally and conn:
                self.storage.db_pool.return_connection(conn)

    async def can_fetch_domain_now(self, domain: str, conn_in: Optional[sqlite3.Connection] = None) -> bool:
        """Checks if the domain can be fetched. Uses provided conn for DB ops."""
        last_fetch_time = 0
        
        conn_managed_internally = False
        active_conn: Optional[sqlite3.Connection] = conn_in

        if active_conn is None:
            try:
                active_conn = self.storage.db_pool.get_connection()
                conn_managed_internally = True
            except Exception as e: # Catch pool errors specifically
                logger.error(f"Failed to get DB connection for can_fetch_domain_now ({domain}): {e}")
                return False # If DB is unavailable for checks, assume can't fetch

        if not active_conn: # Should be caught by above, but as a safeguard
             logger.error(f"DB connection unavailable for can_fetch_domain_now checks ({domain}). Assuming cannot fetch.")
             return False

        try:
            try:
                # Use the active_conn for this synchronous DB operation executed in a thread
                row = await asyncio.to_thread(self._db_get_last_fetch_sync, domain, active_conn)
                if row and row[0] is not None: 
                    last_fetch_time = row[0] 
            except Exception as e: 
                logger.error(f"Error checking last fetch time for {domain} ({type(e).__name__}: {e}). Assuming cannot fetch.")
                return False
                
            # Pass the same active_conn to get_crawl_delay
            crawl_delay = await self.get_crawl_delay(domain, active_conn)
            current_time = int(time.time())
            
            if current_time >= last_fetch_time + crawl_delay:
                return True
            else:
                return False
        finally:
            if conn_managed_internally and active_conn:
                self.storage.db_pool.return_connection(active_conn)

    def _db_record_fetch_sync(self, domain: str, current_time: int, conn_in: Optional[sqlite3.Connection] = None):
        conn_managed_internally = False
        conn: Optional[sqlite3.Connection] = conn_in
        cursor: Optional[sqlite3.Cursor] = None

        if conn is None:
            conn = self.storage.db_pool.get_connection()
            conn_managed_internally = True

        if not conn:
            logger.error(f"DB: Failed to get connection for recording fetch attempt ({domain}).")
            return

        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)",
                                                                  (domain, current_time))
            cursor.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?",
                                                                  (current_time, domain))
            conn.commit()
        except sqlite3.Error as e_inner: 
            logger.error(f"DB execute/commit error recording fetch attempt for {domain}: {e_inner}")
            if conn: conn.rollback()
            raise
        finally:
            if cursor: cursor.close()
            if conn_managed_internally and conn:
                self.storage.db_pool.return_connection(conn)

    async def record_domain_fetch_attempt(self, domain: str, conn_in: Optional[sqlite3.Connection] = None):
        """Records that we are about to fetch. Uses provided conn for DB ops."""
        try:
            current_time = int(time.time())
            # Use passed-in connection for this synchronous DB operation executed in a thread
            await asyncio.to_thread(self._db_record_fetch_sync, domain, current_time, conn_in)
        except Exception as e: 
            logger.error(f"Unexpected error recording fetch attempt for {domain}: {e}") 