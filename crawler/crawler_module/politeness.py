import logging
import time
import asyncio
from urllib.robotparser import RobotFileParser

from .config import CrawlerConfig
from .utils import extract_domain, LRUCache
from .fetcher import Fetcher, MAX_ROBOTS_LENGTH

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_TXT_TTL = 24 * 60 * 60  # 24 hours in seconds
MIN_CRAWL_DELAY_SECONDS = 70 # Our project's default minimum

class PolitenessEnforcer:
    """Redis-backed implementation of PolitenessEnforcer.
    
    Uses Redis for storing domain metadata including robots.txt cache,
    last fetch times, and manual exclusions.
    """
    
    def __init__(self, config: CrawlerConfig, redis_client, fetcher: Fetcher):
        self.config = config
        self.redis = redis_client
        self.fetcher = fetcher
        self.robots_parsers_max_size = 100_000
        self.robots_parsers: LRUCache[str, RobotFileParser] = LRUCache(self.robots_parsers_max_size)
        
        # In-memory exclusion cache (LRU-style)
        self.exclusion_cache_max_size = 100_000
        self.exclusion_cache: LRUCache[str, bool] = LRUCache(self.exclusion_cache_max_size)
        
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
    
    async def batch_load_robots_txt(self, domains: list[str]):
        """Batch load robots.txt for a list of domains."""
        tasks = [self._get_robots_for_domain(domain) for domain in domains]
        await asyncio.gather(*tasks)
    
    async def _get_cached_robots(self, domain: str) -> tuple[str | None, int | None]:
        """Get cached robots.txt content and expiry from Redis."""
        try:
            domain_key = f'domain:{domain}'
            result = await self.redis.hmget(domain_key, ['robots_txt', 'robots_expires'])
            
            if result[0] is not None and result[1] is not None:
                return (result[0][:MAX_ROBOTS_LENGTH], int(result[1]))
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
        cached_rfp = self.robots_parsers.get(domain)
        if cached_rfp is not None:
            return cached_rfp
        
        # 2. Check Redis cache
        robots_content, expires_timestamp = await self._get_cached_robots(domain)
        current_time = int(time.time())
        
        if robots_content is not None and expires_timestamp is not None and expires_timestamp > current_time:
            logger.debug(f"Loaded fresh robots.txt for {domain} from Redis.")
            rfp = RobotFileParser()
            rfp.parse(robots_content.split('\n'))
            self.robots_parsers.put(domain, rfp)
            return rfp
        
        # 3. If not cached or stale, fetch from the web
        logger.info(f"Fetching robots.txt for {domain}")
        fetched_robots_content = ""
        try:
            # Try HTTPS first as it's more common now
            fetch_result = await self.fetcher.fetch_url(f"https://{domain}/robots.txt", is_robots_txt=True)
            if fetch_result.status_code == 200 and fetch_result.text_content:
                fetched_robots_content = fetch_result.text_content
            else:
                # Fallback to HTTP
                fetch_result = await self.fetcher.fetch_url(f"http://{domain}/robots.txt", is_robots_txt=True)
                if fetch_result.status_code == 200 and fetch_result.text_content:
                    fetched_robots_content = fetch_result.text_content
        except Exception as e:
            logger.error(f"Exception fetching robots.txt for {domain}: {e}")
        
        # Sanitize content
        fetched_robots_content = fetched_robots_content[:MAX_ROBOTS_LENGTH]
        if '\0' in fetched_robots_content:
            logger.warning(f"robots.txt for {domain} contains null bytes. Treating as empty.")
            fetched_robots_content = ""
            
        # 4. Cache the result (even if it's empty)
        fetched_timestamp = int(time.time())
        expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
        await self._update_robots_cache(domain, fetched_robots_content, fetched_timestamp, expires_timestamp)
        
        # 5. Parse and update in-memory cache
        rfp = RobotFileParser()
        rfp.parse(fetched_robots_content.split('\n'))
        self.robots_parsers.put(domain, rfp)
        
        return rfp
    
    async def _check_manual_exclusion(self, domain: str) -> bool:
        """Returns True if domain is manually excluded or (if seeded_urls_only) not seeded.
        Uses an in-memory LRU cache to reduce Redis queries.
        """
        # 1. Check cache first
        cached_result = self.exclusion_cache.get(domain)
        if cached_result is not None:
            return cached_result
        
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
        self.exclusion_cache.put(domain, result_bool)
        
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