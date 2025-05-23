import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio # Added for asyncio.to_thread
import sqlite3 # For type hinting in db access functions

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
        # _load_manual_exclusions is synchronous and uses DB, could be called in an executor if it becomes slow
        # For now, direct call is fine as it's part of init.
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            if not self.storage.conn:
                logger.error("Cannot load manual exclusions, no DB connection.")
                return
            try:
                # This is a synchronous DB operation. Consider to_thread if it becomes a bottleneck.
                cursor = self.storage.conn.cursor()
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
                    self.storage.conn.commit()
                    logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file}")
            except IOError as e:
                logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
            except self.storage.conn.Error as e:
                logger.error(f"DB error loading manual exclusions: {e}")
                if self.storage.conn: self.storage.conn.rollback()
            finally:
                if 'cursor' in locals() and cursor: cursor.close() # Ensure cursor is defined before closing
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain."""
        # In-memory cache check (remains synchronous)
        if domain in self.robots_parsers:
             # Simple in-memory check, could add TTL to this dict too
             # Check if DB has a more recent version or if TTL expired.
             # This part needs careful thought for consistency between in-memory and DB cache.
             # For now, if in-memory, let's assume it might be stale and re-evaluate against DB.
             pass # Let it proceed to DB check

        rerp = RobotExclusionRulesParser()
        rerp.user_agent = self.config.user_agent
        
        robots_content: str | None = None
        current_time = int(time.time())
        # expires_timestamp = current_time + DEFAULT_ROBOTS_TXT_TTL # Default, might be overridden by DB

        # 1. Try to load from DB cache if not expired (using asyncio.to_thread for DB access)
        db_row = None
        db_path = self.storage.db_path

        try:
            def _db_get_cached_robots_sync_threaded():
                with sqlite3.connect(db_path, timeout=10) as conn_threaded:
                    with conn_threaded.cursor() as db_cursor:
                        db_cursor.execute(
                            "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                            (domain,)
                        )
                        return db_cursor.fetchone()
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded)
        except sqlite3.Error as e:
            logger.warning(f"DB error fetching cached robots.txt for {domain}: {e}")

        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using cached robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            # If we load from DB, use its expiry. The in-memory RERP should be updated.
            if domain in self.robots_parsers and self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB cache. Reusing parser.")
                return self.robots_parsers[domain]
        elif domain in self.robots_parsers:
            # If DB cache is stale or missing, but we have an in-memory one, it might be okay for a short while
            # or if fetches are failing. This logic could be more complex regarding TTLs for in-memory too.
            # For now, if DB is not fresh, we re-fetch. If re-fetch fails, the old in-memory one is better than nothing.
            logger.debug(f"DB cache for {domain} is stale or missing. Will attempt re-fetch.")

        # 2. If not in valid DB cache, fetch it asynchronously
        if robots_content is None:
            fetched_timestamp = int(time.time()) # Reset timestamp for new fetch
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL

            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
            elif fetch_result_http.status_code == 404:
                logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Assuming allow all.")
                robots_content = ""
            else: 
                logger.info(f"HTTP fetch for robots.txt failed for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")
                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTP (status: {fetch_result_http.status_code}, error: {fetch_result_http.error_message}) and HTTPS (status: {fetch_result_https.status_code}, error: {fetch_result_https.error_message}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is not None:
                try:
                    def _db_update_robots_cache_sync_threaded():
                        with sqlite3.connect(db_path, timeout=10) as conn_threaded:
                            with conn_threaded.cursor() as db_cursor:
                                db_cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                                db_cursor.execute(
                                    "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                                    (robots_content, fetched_timestamp, expires_timestamp, domain)
                                )
                                conn_threaded.commit()
                                logger.debug(f"Cached robots.txt for {domain} in DB.")
                    await asyncio.to_thread(_db_update_robots_cache_sync_threaded)
                except sqlite3.Error as e:
                    logger.error(f"DB error caching robots.txt for {domain}: {e}")

        if robots_content is not None:
            rerp.parse(robots_content)
            rerp.source_content = robots_content # Store source for later comparison
        else:
            rerp.parse("") 
            rerp.source_content = ""
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded."""
        db_path = self.storage.db_path
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        # 1. Check manual exclusion first (DB access in thread)
        try:
            def _db_check_manual_exclusion_sync_threaded():
                with sqlite3.connect(db_path, timeout=10) as conn_threaded:
                    with conn_threaded.cursor() as cursor_obj:
                        cursor_obj.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
                        return cursor_obj.fetchone()
            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded)
            if row and row[0] == 1:
                logger.debug(f"URL {url} from manually excluded domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error checking manual exclusion for {domain}: {e}")

        # 2. Check robots.txt (already async)
        rerp = await self._get_robots_for_domain(domain)
        if rerp:
            is_allowed = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed
        
        logger.warning(f"No robots.txt parser available for {domain}. Allowing URL: {url}")
        return True

    async def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain (from robots.txt or default)."""
        rerp = await self._get_robots_for_domain(domain)
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

    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay."""
        db_path = self.storage.db_path
        if not self.storage.conn:
            logger.error("Cannot check fetch_domain_now, no DB connection.")
            return False

        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded():
                with sqlite3.connect(db_path, timeout=10) as conn_threaded:
                    with conn_threaded.cursor() as cursor_obj:
                        cursor_obj.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,))
                        return cursor_obj.fetchone()
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded)
            if row and row[0] is not None:
                last_fetch_time = row[0]
        except sqlite3.Error as e:
            logger.error(f"DB error checking can_fetch_domain_now for {domain}: {e}")
            return False 
            
        crawl_delay = await self.get_crawl_delay(domain) # Now async
        current_time = int(time.time())
        
        if current_time >= last_fetch_time + crawl_delay:
            return True
        else:
            return False

    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain."""
        db_path = self.storage.db_path
        if not self.storage.conn:
            logger.error("Cannot record domain fetch, no DB connection.")
            return
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded():
                with sqlite3.connect(db_path, timeout=10) as conn_threaded:
                    with conn_threaded.cursor() as cursor_obj:
                        cursor_obj.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)", 
                                       (domain, current_time))
                        cursor_obj.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
                                       (current_time, domain))
                        conn_threaded.commit()
            await asyncio.to_thread(_db_record_fetch_sync_threaded)
        except sqlite3.Error as e:
            logger.error(f"DB error recording fetch attempt for {domain}: {e}")
            # No rollback needed for SELECT usually, and commit is in the thread 