import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio

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
        # Load manual exclusions will be async now
        self._manual_exclusions_loaded = False

    async def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB."""
        if self._manual_exclusions_loaded:
            return
        
        if self.config.exclude_file and self.config.exclude_file.exists():
            try:
                with open(self.config.exclude_file, 'r') as f:
                    count = 0
                    for line in f:
                        domain_to_exclude = line.strip().lower()
                        if domain_to_exclude and not domain_to_exclude.startswith("#"):
                            if self.storage.config.db_type == "postgresql":
                                await self.storage.db.execute(
                                    "INSERT INTO domain_metadata (domain) VALUES (%s) ON CONFLICT DO NOTHING", 
                                    (domain_to_exclude,),
                                    query_name="load_exclusions_insert_pg"
                                )
                            else:  # SQLite
                                await self.storage.db.execute(
                                    "INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", 
                                    (domain_to_exclude,),
                                    query_name="load_exclusions_insert_sqlite"
                                )
                            await self.storage.db.execute(
                                "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = %s" if self.storage.config.db_type == "postgresql" else "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                (domain_to_exclude,),
                                query_name="load_exclusions_update"
                            )
                            count += 1
                    logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file}.")
            except IOError as e:
                logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
            except Exception as e:
                logger.error(f"DB error loading manual exclusions: {e}")
                raise
        else:
            logger.info("No manual exclude file specified or found.")
        
        self._manual_exclusions_loaded = True

    async def _get_cached_robots(self, domain: str) -> tuple | None:
        try:
            row = await self.storage.db.fetch_one(
                "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                (domain,),
                query_name="get_cached_robots"
            )
            return row
        except Exception as e:
            logger.warning(f"DB error fetching cached robots.txt for {domain}: {e}")
            return None

    async def _update_robots_cache(self, domain: str, robots_content: str, fetched_timestamp: int, expires_timestamp: int):
        try:
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute(
                    "INSERT INTO domain_metadata (domain) VALUES (%s) ON CONFLICT DO NOTHING", 
                    (domain,),
                    query_name="update_robots_cache_insert_pg"
                )
                await self.storage.db.execute(
                    "UPDATE domain_metadata SET robots_txt_content = %s, robots_txt_fetched_timestamp = %s, robots_txt_expires_timestamp = %s WHERE domain = %s",
                    (robots_content, fetched_timestamp, expires_timestamp, domain),
                    query_name="update_robots_cache_update_pg"
                )
            else:  # SQLite
                await self.storage.db.execute(
                    "INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", 
                    (domain,),
                    query_name="update_robots_cache_insert_sqlite"
                )
                await self.storage.db.execute(
                    "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                    (robots_content, fetched_timestamp, expires_timestamp, domain),
                    query_name="update_robots_cache_update_sqlite"
                )
            logger.debug(f"Cached robots.txt for {domain} in DB.")
        except Exception as e:
            logger.error(f"DB error caching robots.txt for {domain}: {e}")
            raise

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        
        # Check DB cache
        db_row = await self._get_cached_robots(domain)

        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            # If this fresh DB content matches an existing in-memory parser (by source), reuse parser object
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        elif domain in self.robots_parsers:
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
                    await self._update_robots_cache(domain, robots_content, fetched_timestamp, expires_timestamp)
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

    async def _check_manual_exclusion(self, domain: str) -> bool:
        """Returns True if domain is manually excluded or (if seeded_urls_only) not seeded."""
        try:
            if self.config.seeded_urls_only:
                row = await self.storage.db.fetch_one(
                    "SELECT is_manually_excluded, is_seeded FROM domain_metadata WHERE domain = ?", 
                    (domain,),
                    query_name="check_manual_exclusion_seeded"
                )
                if row:
                    is_excluded = bool(row[0] == 1)
                    is_seeded = bool(row[1] == 1)
                    return is_excluded or not is_seeded
                else:
                    # Domain not in metadata - it's not seeded
                    return True
            else:
                row = await self.storage.db.fetch_one(
                    "SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", 
                    (domain,),
                    query_name="check_manual_exclusion_unseeded"
                )
                return bool(row and row[0] == 1)
        except Exception as e:
            logger.warning(f"DB error checking manual exclusion for {domain}: {e}")
            return False  # Default to not excluded on error

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded."""
        # Ensure manual exclusions are loaded
        await self._load_manual_exclusions()
        
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        # Check manual exclusion
        if await self._check_manual_exclusion(domain):
            logger.debug(f"URL {url} from manually excluded or non-seeded domain: {domain}")
            return False

        # If not manually excluded, check robots.txt
        rerp = await self._get_robots_for_domain(domain)
        if rerp:
            is_allowed_by_robots = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed_by_robots:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed_by_robots
        
        logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
        return True # Default to allow if RERP is None

    async def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain."""
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
        """Checks if the domain can be fetched now based on crawl delay."""
        last_fetch_time = 0
        
        try:
            row = await self.storage.db.fetch_one(
                "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", 
                (domain,)
            )
            if row and row[0] is not None: 
                last_fetch_time = row[0]
        except Exception as e:
            logger.error(f"Error checking last fetch time for {domain}: {e}. Assuming cannot fetch.")
            return False
        
        crawl_delay = await self.get_crawl_delay(domain)
        current_time = int(time.time())
        
        return current_time >= last_fetch_time + crawl_delay

    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch from a domain."""
        try:
            current_time = int(time.time())
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute(
                    "INSERT INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (%s, %s) ON CONFLICT (domain) DO UPDATE SET last_scheduled_fetch_timestamp = %s",
                    (domain, current_time, current_time),
                    query_name="record_fetch_attempt_pg"
                )
            else:  # SQLite
                await self.storage.db.execute(
                    "INSERT OR REPLACE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?, ?)",
                    (domain, current_time),
                    query_name="record_fetch_attempt_sqlite"
                )
        except Exception as e: 
            logger.error(f"Error recording fetch attempt for {domain}: {e}") 