import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio
from collections import OrderedDict
from typing import OrderedDict as TypingOrderedDict # For type hinting

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
        
        self.exclusion_cache_max_size = 100_000
        self.exclusion_cache: dict[str, bool] = {}
        self.exclusion_cache_order: TypingOrderedDict[str, None] = OrderedDict()

        self._manual_exclusions_loaded = False

    async def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB."""
        if self._manual_exclusions_loaded:
            return
        
        if self.config.exclude_file and self.config.exclude_file.exists():
            try:
                domains_to_exclude = []
                with open(self.config.exclude_file, 'r') as f:
                    for line in f:
                        domain = line.strip().lower()
                        if domain and not domain.startswith("#"):
                            domains_to_exclude.append((domain,))
                
                if not domains_to_exclude:
                    return

                if self.storage.config.db_type == "postgresql":
                    # Efficiently UPSERT all domains, setting the exclusion flag
                    await self.storage.db.execute_many(
                        """
                        INSERT INTO domain_metadata (domain, is_manually_excluded) VALUES (%s, 1)
                        ON CONFLICT (domain) DO UPDATE SET is_manually_excluded = 1
                        """,
                        domains_to_exclude,
                        query_name="load_exclusions_upsert_pg"
                    )
                else:  # SQLite
                    # SQLite requires separate INSERT and UPDATE for this logic
                    await self.storage.db.execute_many(
                        "INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)",
                        domains_to_exclude,
                        query_name="load_exclusions_insert_sqlite"
                    )
                    await self.storage.db.execute_many(
                        "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                        domains_to_exclude,
                        query_name="load_exclusions_update_sqlite"
                    )
                logger.info(f"Loaded and marked {len(domains_to_exclude)} domains as manually excluded from {self.config.exclude_file}.")
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
        """Updates the robots.txt cache in the database using a single UPSERT."""
        try:
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute(
                    """
                    INSERT INTO domain_metadata (domain, robots_txt_content, robots_txt_fetched_timestamp, robots_txt_expires_timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (domain) DO UPDATE SET
                        robots_txt_content = excluded.robots_txt_content,
                        robots_txt_fetched_timestamp = excluded.robots_txt_fetched_timestamp,
                        robots_txt_expires_timestamp = excluded.robots_txt_expires_timestamp
                    """,
                    (domain, robots_content, fetched_timestamp, expires_timestamp),
                    query_name="update_robots_cache_upsert_pg"
                )
            else:  # SQLite
                await self.storage.db.execute(
                    """
                    INSERT OR REPLACE INTO domain_metadata 
                    (domain, robots_txt_content, robots_txt_fetched_timestamp, robots_txt_expires_timestamp) 
                    VALUES (?, ?, ?, ?)
                    """,
                    (domain, robots_content, fetched_timestamp, expires_timestamp),
                    query_name="update_robots_cache_upsert_sqlite"
                )
            logger.debug(f"Cached robots.txt for {domain} in DB.")
        except Exception as e:
            logger.error(f"DB error caching robots.txt for {domain}: {e}")
            raise

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain.
        Uses a clean cache-aside pattern: check memory, then DB, then fetch.
        """
        # 1. Check in-memory cache first
        if domain in self.robots_parsers:
            return self.robots_parsers[domain]

        # 2. Check DB cache
        db_row = await self._get_cached_robots(domain)
        current_time = int(time.time())

        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Loaded fresh robots.txt for {domain} from DB.")
            robots_content = db_row[0]
            rerp = RobotExclusionRulesParser()
            rerp.parse(robots_content)
            rerp.source_content = robots_content # For potential future diffing
            self.robots_parsers[domain] = rerp
            return rerp
        
        # 3. If not in caches or caches are stale, fetch from the web
        fetched_timestamp = int(time.time())
        expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
        
        robots_url_http = f"http://{domain}/robots.txt"
        robots_url_https = f"https://{domain}/robots.txt"
        logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
        fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

        fetched_content: str | None = None
        if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
            fetched_content = fetch_result_http.text_content
        else:
            logger.debug(f"robots.txt not found or error on HTTP for {domain}. Trying HTTPS.")
            fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
            if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                fetched_content = fetch_result_https.text_content
            else:
                logger.debug(f"Failed to fetch robots.txt for {domain} via HTTPS. Assuming allow all.")
                fetched_content = "" # Treat as empty, meaning allow all
        if '\0' in fetched_content:
            # The robots.txt is malformed, and Postgres will reject it.
            # Treat as empty, meaning allow all.
            # TODO: Handle this by storing byte field in DB instead.
            logger.debug(f"robots.txt for {domain} contains null byte. Assuming allow all.")
            fetched_content = ""

        # Cache the newly fetched content
        await self._update_robots_cache(domain, fetched_content, fetched_timestamp, expires_timestamp)

        # Parse and cache in memory
        rerp = RobotExclusionRulesParser()
        rerp.parse(fetched_content)
        rerp.source_content = fetched_content
        self.robots_parsers[domain] = rerp
        return rerp

    async def _check_manual_exclusion(self, domain: str) -> bool:
        """Returns True if domain is manually excluded or (if seeded_urls_only) not seeded.
        Uses an in-memory LRU cache to reduce database queries.
        """
        # 1. Check cache first
        if domain in self.exclusion_cache:
            self.exclusion_cache_order.move_to_end(domain) # Mark as recently used
            return self.exclusion_cache[domain]

        # 2. If not in cache, query database
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
                    result = is_excluded or not is_seeded
                else:
                    # Domain not in metadata - it's not seeded
                    result = True
            else:
                row = await self.storage.db.fetch_one(
                    "SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", 
                    (domain,),
                    query_name="check_manual_exclusion_unseeded"
                )
                result = bool(row and row[0] == 1)
        except Exception as e:
            logger.warning(f"DB error checking manual exclusion for {domain}: {e}")
            return False  # Default to not excluded on error
            
        # 3. Update cache
        self.exclusion_cache[domain] = result
        self.exclusion_cache_order[domain] = None

        # 4. Enforce cache size limit
        if len(self.exclusion_cache) > self.exclusion_cache_max_size:
            oldest_domain, _ = self.exclusion_cache_order.popitem(last=False)
            del self.exclusion_cache[oldest_domain]
            
        return result

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded."""
        
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


class RedisPolitenessEnforcer:
    """Redis-backed implementation of PolitenessEnforcer.
    
    Uses Redis for storing domain metadata including robots.txt cache,
    last fetch times, and manual exclusions.
    """
    
    def __init__(self, config: CrawlerConfig, redis_client, fetcher: Fetcher):
        self.config = config
        self.redis = redis_client
        self.fetcher = fetcher
        self.robots_parsers: dict[str, RobotFileParser] = {}  # In-memory cache for parsed robots.txt
        
        # In-memory exclusion cache (LRU-style)
        self.exclusion_cache_max_size = 100_000
        self.exclusion_cache: dict[str, bool] = {}
        self.exclusion_cache_order: TypingOrderedDict[str, None] = OrderedDict()
        
        self._manual_exclusions_loaded = False
    
    async def initialize(self):
        """Initialize the politeness enforcer."""
        await self._load_manual_exclusions()
    
    async def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into Redis."""
        if self._manual_exclusions_loaded:
            return
        
        if self.config.exclude_file and self.config.exclude_file.exists():
            try:
                domains_to_exclude = []
                with open(self.config.exclude_file, 'r') as f:
                    for line in f:
                        domain = line.strip().lower()
                        if domain and not domain.startswith("#"):
                            domains_to_exclude.append(domain)
                
                if not domains_to_exclude:
                    return
                
                # Mark domains as excluded in Redis
                pipe = self.redis.pipeline()
                for domain in domains_to_exclude:
                    pipe.hset(f'domain:{domain}', 'is_excluded', '1')
                await pipe.execute()
                
                logger.info(f"Loaded and marked {len(domains_to_exclude)} domains as manually excluded from {self.config.exclude_file}.")
            except IOError as e:
                logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
            except Exception as e:
                logger.error(f"Redis error loading manual exclusions: {e}")
                raise
        else:
            logger.info("No manual exclude file specified or found.")
        
        self._manual_exclusions_loaded = True
    
    async def _get_cached_robots(self, domain: str) -> tuple[str | None, int | None]:
        """Get cached robots.txt content and expiry from Redis."""
        try:
            domain_key = f'domain:{domain}'
            result = await self.redis.hmget(domain_key, ['robots_txt', 'robots_expires'])
            
            if result[0] and result[1]:
                return (result[0], int(result[1]))
            return (None, None)
        except Exception as e:
            logger.warning(f"Redis error fetching cached robots.txt for {domain}: {e}")
            return (None, None)
    
    async def _update_robots_cache(self, domain: str, robots_content: str, fetched_timestamp: int, expires_timestamp: int):
        """Updates the robots.txt cache in Redis."""
        try:
            domain_key = f'domain:{domain}'
            await self.redis.hset(domain_key, mapping={
                'robots_txt': robots_content,
                'robots_expires': str(expires_timestamp)
            })
            logger.debug(f"Cached robots.txt for {domain} in Redis.")
        except Exception as e:
            logger.error(f"Redis error caching robots.txt for {domain}: {e}")
            raise
    
    async def _get_robots_for_domain(self, domain: str) -> RobotFileParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain."""
        # 1. Check in-memory cache first
        if domain in self.robots_parsers:
            return self.robots_parsers[domain]
        
        # 2. Check Redis cache
        robots_content, expires_timestamp = await self._get_cached_robots(domain)
        current_time = int(time.time())
        
        if robots_content is not None and expires_timestamp is not None and expires_timestamp > current_time:
            logger.debug(f"Loaded fresh robots.txt for {domain} from Redis.")
            rfp = RobotFileParser()
            rfp.parse(robots_content.split('\n'))
            # Store the content for reference
            rfp._content = robots_content  # type: ignore[attr-defined]
            self.robots_parsers[domain] = rfp
            return rfp
        
        # 3. If not in caches or caches are stale, fetch from the web
        fetched_timestamp = int(time.time())
        expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
        
        robots_url_http = f"http://{domain}/robots.txt"
        robots_url_https = f"https://{domain}/robots.txt"
        logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
        fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)
        
        fetched_robots_content: str | None = None
        if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
            fetched_robots_content = fetch_result_http.text_content
        else:
            logger.debug(f"robots.txt not found or error on HTTP for {domain}. Trying HTTPS.")
            fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
            if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                fetched_robots_content = fetch_result_https.text_content
            else:
                logger.debug(f"Failed to fetch robots.txt for {domain} via HTTPS. Assuming allow all.")
                fetched_robots_content = ""  # Treat as empty, meaning allow all
        
        if '\0' in fetched_robots_content:
            # The robots.txt is malformed
            logger.debug(f"robots.txt for {domain} contains null byte. Assuming allow all.")
            fetched_robots_content = ""
        
        # Cache the newly fetched content
        await self._update_robots_cache(domain, fetched_robots_content, fetched_timestamp, expires_timestamp)
        
        # Parse and cache in memory
        rfp = RobotFileParser()
        rfp.parse(fetched_robots_content.split('\n'))
        rfp._content = fetched_robots_content  # type: ignore[attr-defined]
        self.robots_parsers[domain] = rfp
        return rfp
    
    async def _check_manual_exclusion(self, domain: str) -> bool:
        """Returns True if domain is manually excluded or (if seeded_urls_only) not seeded.
        Uses an in-memory LRU cache to reduce Redis queries.
        """
        # 1. Check cache first
        if domain in self.exclusion_cache:
            self.exclusion_cache_order.move_to_end(domain)  # Mark as recently used
            return self.exclusion_cache[domain]
        
        # 2. If not in cache, query Redis
        try:
            domain_key = f'domain:{domain}'
            
            if self.config.seeded_urls_only:
                result = await self.redis.hmget(domain_key, ['is_excluded', 'is_seeded'])
                is_excluded = result[0] == '1' if result[0] else False
                is_seeded = result[1] == '1' if result[1] else False
                result_bool = is_excluded or not is_seeded
            else:
                is_excluded = await self.redis.hget(domain_key, 'is_excluded')
                result_bool = is_excluded == '1' if is_excluded else False
        except Exception as e:
            logger.warning(f"Redis error checking manual exclusion for {domain}: {e}")
            return False  # Default to not excluded on error
        
        # 3. Update cache
        self.exclusion_cache[domain] = result_bool
        self.exclusion_cache_order[domain] = None
        
        # 4. Enforce cache size limit
        if len(self.exclusion_cache) > self.exclusion_cache_max_size:
            oldest_domain, _ = self.exclusion_cache_order.popitem(last=False)
            del self.exclusion_cache[oldest_domain]
        
        return result_bool
    
    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True
        
        # Check manual exclusion
        if await self._check_manual_exclusion(domain):
            logger.debug(f"URL {url} from manually excluded or non-seeded domain: {domain}")
            return False
        
        # If not manually excluded, check robots.txt
        rfp = await self._get_robots_for_domain(domain)
        if rfp:
            is_allowed_by_robots = rfp.can_fetch(self.config.user_agent, url)
            if not is_allowed_by_robots:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed_by_robots
        
        logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
        return True  # Default to allow if RFP is None
    
    async def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain."""
        rfp = await self._get_robots_for_domain(domain)
        delay = None
        if rfp:
            agent_delay = rfp.crawl_delay(self.config.user_agent)
            if agent_delay is not None:
                delay = float(agent_delay)
                logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (agent: {self.config.user_agent})")
        
        if delay is None:
            if rfp and rfp.crawl_delay("*") is not None:
                wildcard_delay = rfp.crawl_delay("*")
                if wildcard_delay is not None:
                    delay = float(wildcard_delay)
                    logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (wildcard agent)")
        
        if delay is None:
            logger.debug(f"No crawl delay specified in robots.txt for {domain}. Using default: {MIN_CRAWL_DELAY_SECONDS}s")
            return float(MIN_CRAWL_DELAY_SECONDS)
        
        return max(float(delay), float(MIN_CRAWL_DELAY_SECONDS))
    
    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched now based on crawl delay."""
        domain_key = f'domain:{domain}'
        
        try:
            next_fetch_time = await self.redis.hget(domain_key, 'next_fetch_time')
            if next_fetch_time:
                next_fetch_timestamp = int(next_fetch_time)
            else:
                next_fetch_timestamp = 0
        except Exception as e:
            logger.error(f"Error checking next fetch time for {domain}: {e}. Assuming cannot fetch.")
            return False
        
        current_time = int(time.time())
        return current_time >= next_fetch_timestamp
    
    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch from a domain."""
        try:
            current_time = int(time.time())
            crawl_delay = await self.get_crawl_delay(domain)
            next_fetch_time = current_time + int(crawl_delay)
            
            domain_key = f'domain:{domain}'
            await self.redis.hset(domain_key, 'next_fetch_time', str(next_fetch_time))
            
            logger.debug(f"Recorded fetch attempt for {domain}. Next fetch allowed at: {next_fetch_time}")
        except Exception as e:
            logger.error(f"Error recording fetch attempt for {domain}: {e}") 