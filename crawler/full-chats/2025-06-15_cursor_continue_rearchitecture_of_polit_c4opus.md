# Continue rearchitecture of politeness enforcer
_Exported on 6/15/2025 at 18:01:33 PDT from Cursor (1.0.0)_

---

**User**

Let's continue our rearchitecture of the web crawler from PostgreSQL to Redis. See `redis_only_architecture.md` for the rearchitecture plan, and `PLAN.MD` + `README.md` for how the crawler currently works with the PostgreSQL backend. We are in phase 2 and previously completed a redis + filesystem-backed implementation of the frontier (HybridFrontierManager) along with a test suite (test_frontier_redis.py). 

The next step is to create a similar version of the politeness enforcer, then update the politeness tests. Specifically let's build a version of PolitenessEnforcer from politeness.py implementing the same public interface but with a redis backend. Then we should create a new file `test_politeness_redis.py` which ports over the tests from `test_politeness.py` to work with the new version of PolitenessEnforcer, similar to what `test_frontier_redis.py` does for frontier tests.

---

**Cursor**

```plaintext
domain:{domain} → HASH {
    frontier_offset: 1234,
    frontier_size: 5678,
    file_path: "a5/example.com.frontier",
    next_fetch_time: 1699564800,  # When domain can be fetched next
    robots_txt: "...",            # Cached robots.txt
    robots_expires: 1699568400,   # When to refetch robots.txt
    is_excluded: 0,               # Manual exclusion flag
}
```

Looking at the plan and the current implementation, I'll create a Redis-backed version of PolitenessEnforcer and corresponding tests. Let me start by implementing the Redis version of PolitenessEnforcer.

```python
// ... existing code ...
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
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}  # In-memory cache for parsed robots.txt
        
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
    
    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain."""
        # 1. Check in-memory cache first
        if domain in self.robots_parsers:
            return self.robots_parsers[domain]
        
        # 2. Check Redis cache
        robots_content, expires_timestamp = await self._get_cached_robots(domain)
        current_time = int(time.time())
        
        if robots_content is not None and expires_timestamp is not None and expires_timestamp > current_time:
            logger.debug(f"Loaded fresh robots.txt for {domain} from Redis.")
            rerp = RobotExclusionRulesParser()
            rerp.parse(robots_content)
            rerp.source_content = robots_content
            self.robots_parsers[domain] = rerp
            return rerp
        
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
        rerp = RobotExclusionRulesParser()
        rerp.parse(fetched_robots_content)
        rerp.source_content = fetched_robots_content
        self.robots_parsers[domain] = rerp
        return rerp
    
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
        rerp = await self._get_robots_for_domain(domain)
        if rerp:
            is_allowed_by_robots = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed_by_robots:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed_by_robots
        
        logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
        return True  # Default to allow if RERP is None
    
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
```

Now let me create the test file for the Redis-backed PolitenessEnforcer:

---

**User**

continue

---

**Cursor**

I'll create the test file for the Redis-backed PolitenessEnforcer. First, let me check the existing test_politeness.py to understand what tests we need to port:

Now I'll create the test file for the Redis-backed PolitenessEnforcer:

```python
import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import time
from unittest.mock import MagicMock, AsyncMock, patch
import redis.asyncio as redis
from robotexclusionrulesparser import RobotExclusionRulesParser

from crawler_module.politeness import RedisPolitenessEnforcer, DEFAULT_ROBOTS_TXT_TTL, MIN_CRAWL_DELAY_SECONDS
from crawler_module.config import CrawlerConfig
from crawler_module.fetcher import Fetcher, FetchResult

@pytest.fixture
def dummy_config(tmp_path: Path) -> CrawlerConfig:
    return CrawlerConfig(
        seed_file=tmp_path / "seeds.txt",
        email="test@example.com",
        data_dir=tmp_path / "test_data",
        exclude_file=None,  # Will be set in specific tests
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="TestCrawler/1.0 (pytest)",
        seeded_urls_only=False,
        db_type="postgresql",  # Changed to indicate we're using Redis architecture
        db_url=None
    )

@pytest_asyncio.fixture
async def redis_client():
    """Provides a Redis client for tests."""
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Clean up test data before and after
    await client.flushdb()
    
    yield client
    
    # Cleanup after test
    await client.flushdb()
    await client.aclose()

@pytest.fixture
def mock_fetcher() -> MagicMock:
    return MagicMock(spec=Fetcher)

@pytest_asyncio.fixture
async def redis_politeness_enforcer(
    dummy_config: CrawlerConfig,
    redis_client: redis.Redis,
    mock_fetcher: MagicMock
) -> RedisPolitenessEnforcer:
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    pe = RedisPolitenessEnforcer(config=dummy_config, redis_client=redis_client, fetcher=mock_fetcher)
    await pe.initialize()
    return pe

# --- Tests for _load_manual_exclusions ---
@pytest.mark.asyncio
async def test_load_manual_exclusions_no_file(dummy_config: CrawlerConfig, redis_client: redis.Redis, mock_fetcher: MagicMock):
    dummy_config.exclude_file = None
    pe = RedisPolitenessEnforcer(config=dummy_config, redis_client=redis_client, fetcher=mock_fetcher)
    
    # Should initialize without error
    await pe.initialize()
    assert pe is not None

@pytest.mark.asyncio
async def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig,
    redis_client: redis.Redis,
    mock_fetcher: MagicMock,
    tmp_path: Path
):
    exclude_file_path = tmp_path / "custom_excludes.txt"
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    pe = RedisPolitenessEnforcer(config=dummy_config, redis_client=redis_client, fetcher=mock_fetcher)
    await pe.initialize()
    
    # Verify domains are marked as excluded in Redis
    expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
    for domain in expected_domains:
        is_excluded = await redis_client.hget(f'domain:{domain}', 'is_excluded')
        assert is_excluded == '1', f"Domain {domain} should be marked as excluded"
    
    if exclude_file_path.exists():
        exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) ---
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Tests that if a parser is in memory, Redis is not hit."""
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    redis_politeness_enforcer.robots_parsers[domain] = mock_rerp

    # Act
    rerp = await redis_politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp == mock_rerp
    # No Redis or fetcher calls should be made
    redis_politeness_enforcer.fetcher.fetch_url.assert_not_called()

@pytest.mark.asyncio
async def test_get_robots_from_redis_cache_fresh(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    """Tests getting a fresh robots.txt from the Redis cache when not in memory."""
    domain = "rediscached.com"
    robots_text = "User-agent: TestCrawler\nDisallow: /private"
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    # Setup: Ensure not in memory, but Redis will return a fresh record
    redis_politeness_enforcer.robots_parsers.pop(domain, None)
    
    # Store in Redis
    await redis_politeness_enforcer.redis.hset(f'domain:{domain}', mapping={
        'robots_txt': robots_text,
        'robots_expires': str(future_expiry)
    })
    
    # Act
    rerp = await redis_politeness_enforcer._get_robots_for_domain(domain)
    
    # Assert
    # 1. Web was NOT fetched
    redis_politeness_enforcer.fetcher.fetch_url.assert_not_called()
    # 2. Parser is correct
    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/private") is False
    # 3. In-memory cache is now populated
    assert domain in redis_politeness_enforcer.robots_parsers
    assert redis_politeness_enforcer.robots_parsers[domain] == rerp

@pytest.mark.asyncio
async def test_get_robots_redis_cache_stale_then_fetch_success(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Tests fetching from web when Redis cache is stale."""
    domain = "staleredis.com"
    past_expiry = int(time.time()) - 1000
    new_robots_text = "User-agent: *\nDisallow: /new"

    # Setup: Redis returns a stale record
    redis_politeness_enforcer.robots_parsers.pop(domain, None)
    await redis_politeness_enforcer.redis.hset(f'domain:{domain}', mapping={
        'robots_txt': 'stale content',
        'robots_expires': str(past_expiry)
    })
    
    # Mock the web fetch to succeed
    redis_politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=new_robots_text
    )

    # Act
    rerp = await redis_politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp is not None
    # 1. Web was fetched (since Redis was stale)
    redis_politeness_enforcer.fetcher.fetch_url.assert_called_once()
    # 2. Redis was updated with the new content
    stored_content = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == new_robots_text
    # 3. In-memory cache is now populated
    assert domain in redis_politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_no_cache_fetch_http_success(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Tests fetching from web when no caches are available."""
    domain = "newfetch.com"
    robots_text = "User-agent: *\nAllow: /"

    # Setup: No in-memory or Redis cache
    redis_politeness_enforcer.robots_parsers.pop(domain, None)
    # Mock web fetch
    redis_politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=robots_text
    )

    # Act
    await redis_politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    redis_politeness_enforcer.fetcher.fetch_url.assert_called_once()
    # Redis should be updated
    stored_content = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == robots_text
    assert domain in redis_politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_fail_then_https_success(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Tests fallback from HTTP to HTTPS for robots.txt fetch."""
    domain = "httpsfallback.com"
    robots_text_https = "User-agent: *\nDisallow: /onlyhttps"

    # Setup
    redis_politeness_enforcer.robots_parsers.pop(domain, None)
    # Mock fetcher to fail on HTTP and succeed on HTTPS
    fetch_result_http = FetchResult(initial_url="...", final_url="...", status_code=500)
    fetch_result_https = FetchResult(
        initial_url="...", final_url="...", status_code=200, text_content=robots_text_https
    )
    redis_politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_http, fetch_result_https]

    # Act
    await redis_politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert redis_politeness_enforcer.fetcher.fetch_url.call_count == 2
    stored_content = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == robots_text_https
    assert domain in redis_politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_404_https_404(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Tests that two 404s result in an empty (allow all) robots rule."""
    domain = "all404.com"
    # Setup
    redis_politeness_enforcer.robots_parsers.pop(domain, None)
    # Mock fetcher to return 404 for both calls
    fetch_result_404 = FetchResult(initial_url="...", final_url="...", status_code=404)
    redis_politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_404, fetch_result_404]

    # Act
    rerp = await redis_politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp is not None
    assert rerp.is_allowed(redis_politeness_enforcer.config.user_agent, "/") is True
    assert redis_politeness_enforcer.fetcher.fetch_url.call_count == 2
    # Redis should be updated with an empty string to cache the "not found" result
    stored_content = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == ""
    assert domain in redis_politeness_enforcer.robots_parsers

# --- Tests for is_url_allowed (async) ---
@pytest.mark.asyncio
async def test_is_url_allowed_manually_excluded(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "manualexclude.com"
    url = f"http://{domain}/somepage"
    
    # Setup: Prime the manual exclusion cache correctly
    redis_politeness_enforcer.exclusion_cache[domain] = True
    redis_politeness_enforcer.exclusion_cache_order[domain] = None

    # Act
    is_allowed = await redis_politeness_enforcer.is_url_allowed(url)

    # Assert
    assert is_allowed is False
    # Ensure _get_robots_for_domain is NOT called if manually excluded
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock()
    await redis_politeness_enforcer.is_url_allowed(url)
    redis_politeness_enforcer._get_robots_for_domain.assert_not_called()

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # Setup: Ensure not manually excluded (prime cache correctly)
    redis_politeness_enforcer.exclusion_cache[domain] = False
    redis_politeness_enforcer.exclusion_cache_order[domain] = None
    
    # Mock _get_robots_for_domain to return a pre-configured parser
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # Test Allowed URL
    assert await redis_politeness_enforcer.is_url_allowed(allowed_url) is True
    redis_politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Test Disallowed URL
    redis_politeness_enforcer._get_robots_for_domain.reset_mock()
    assert await redis_politeness_enforcer.is_url_allowed(disallowed_url) is False
    redis_politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

@pytest.mark.asyncio
async def test_is_url_allowed_seeded_urls_only(redis_client: redis.Redis, mock_fetcher: MagicMock, dummy_config: CrawlerConfig):
    """Test that non-seeded domains are excluded when seeded_urls_only is True."""
    dummy_config.seeded_urls_only = True
    pe = RedisPolitenessEnforcer(config=dummy_config, redis_client=redis_client, fetcher=mock_fetcher)
    await pe.initialize()
    
    seeded_domain = "seeded.com"
    non_seeded_domain = "notseeded.com"
    
    # Mark one domain as seeded
    await redis_client.hset(f'domain:{seeded_domain}', 'is_seeded', '1')
    
    # Seeded domain should be allowed
    assert await pe.is_url_allowed(f"http://{seeded_domain}/page") is True
    
    # Non-seeded domain should not be allowed
    assert await pe.is_url_allowed(f"http://{non_seeded_domain}/page") is False

# --- Tests for get_crawl_delay (async) ---
@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_agent_specific(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "delaytest.com"
    agent_delay = 10
    robots_text = f"User-agent: {dummy_config.user_agent}\nCrawl-delay: {agent_delay}"
    
    # Mock _get_robots_for_domain to return a parser with this rule
    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp)

    delay = await redis_politeness_enforcer.get_crawl_delay(domain)
    assert delay == max(float(agent_delay), float(MIN_CRAWL_DELAY_SECONDS))

@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_wildcard(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "wildcarddelay.com"
    wildcard_delay = 5
    robots_text = f"User-agent: AnotherBot\nCrawl-delay: 50\nUser-agent: *\nCrawl-delay: {wildcard_delay}"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp)

    delay = await redis_politeness_enforcer.get_crawl_delay(domain)
    assert delay == max(float(wildcard_delay), float(MIN_CRAWL_DELAY_SECONDS))

@pytest.mark.asyncio
async def test_get_crawl_delay_default_no_robots_rule(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "nodelayrule.com"
    robots_text = "User-agent: *\nDisallow: /"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp)

    delay = await redis_politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(MIN_CRAWL_DELAY_SECONDS)

@pytest.mark.asyncio
async def test_get_crawl_delay_respects_min_crawl_delay(redis_politeness_enforcer: RedisPolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "shortdelay.com"
    short_robot_delay = 1  # Shorter than MIN_CRAWL_DELAY_SECONDS
    robots_text = f"User-agent: *\nCrawl-delay: {short_robot_delay}"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    redis_politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp)

    delay = await redis_politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(MIN_CRAWL_DELAY_SECONDS)

# --- Tests for can_fetch_domain_now (async) ---
@pytest.mark.asyncio
async def test_can_fetch_domain_now_no_previous_fetch(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "newdomain.com"
    
    # No previous fetch time in Redis
    can_fetch = await redis_politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is True

@pytest.mark.asyncio
async def test_can_fetch_domain_now_after_sufficient_delay(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "readyagain.com"
    current_time = int(time.time())
    
    # Set next fetch time to be in the past
    await redis_politeness_enforcer.redis.hset(
        f'domain:{domain}', 
        'next_fetch_time', 
        str(current_time - 10)
    )
    
    can_fetch = await redis_politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is True

@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "notready.com"
    current_time = int(time.time())
    
    # Set next fetch time to be in the future
    await redis_politeness_enforcer.redis.hset(
        f'domain:{domain}', 
        'next_fetch_time', 
        str(current_time + 30)
    )
    
    can_fetch = await redis_politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is False

# --- Tests for record_domain_fetch_attempt (async) ---
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_new_domain(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "recordnew.com"
    
    # Mock get_crawl_delay
    redis_politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))
    
    # Record fetch
    await redis_politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify
    redis_politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)
    
    # Check that next_fetch_time was set correctly
    next_fetch_time = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'next_fetch_time')
    assert next_fetch_time is not None
    assert int(next_fetch_time) > int(time.time())

@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_existing_domain(redis_politeness_enforcer: RedisPolitenessEnforcer):
    domain = "recordexisting.com"
    
    # Set initial fetch time
    initial_time = str(int(time.time()) - 100)
    await redis_politeness_enforcer.redis.hset(f'domain:{domain}', 'next_fetch_time', initial_time)
    
    # Mock get_crawl_delay
    redis_politeness_enforcer.get_crawl_delay = AsyncMock(return_value=70.0)
    
    # Record new fetch
    await redis_politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify next_fetch_time was updated
    new_next_fetch_time = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'next_fetch_time')
    assert int(new_next_fetch_time) > int(initial_time)
    assert int(new_next_fetch_time) >= int(time.time()) + 70

# --- Tests for exclusion cache ---
@pytest.mark.asyncio
async def test_exclusion_cache_lru_behavior(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Test that exclusion cache implements LRU eviction."""
    # Set a small cache size for testing
    redis_politeness_enforcer.exclusion_cache_max_size = 3
    
    # Add domains to cache
    domains = ["domain1.com", "domain2.com", "domain3.com", "domain4.com"]
    
    for i, domain in enumerate(domains[:3]):
        # Store in Redis and cache
        await redis_politeness_enforcer.redis.hset(f'domain:{domain}', 'is_excluded', '0')
        await redis_politeness_enforcer._check_manual_exclusion(domain)
    
    # Cache should have 3 items
    assert len(redis_politeness_enforcer.exclusion_cache) == 3
    
    # Access domain1 to make it recently used
    await redis_politeness_enforcer._check_manual_exclusion("domain1.com")
    
    # Add domain4, which should evict domain2 (least recently used)
    await redis_politeness_enforcer.redis.hset(f'domain:{domains[3]}', 'is_excluded', '0')
    await redis_politeness_enforcer._check_manual_exclusion(domains[3])
    
    # Check cache contents
    assert "domain1.com" in redis_politeness_enforcer.exclusion_cache
    assert "domain2.com" not in redis_politeness_enforcer.exclusion_cache  # Evicted
    assert "domain3.com" in redis_politeness_enforcer.exclusion_cache
    assert "domain4.com" in redis_politeness_enforcer.exclusion_cache

# --- Tests for null byte handling in robots.txt ---
@pytest.mark.asyncio
async def test_robots_txt_with_null_byte(redis_politeness_enforcer: RedisPolitenessEnforcer):
    """Test that robots.txt with null bytes is handled correctly."""
    domain = "nullbyte.com"
    robots_text_with_null = "User-agent: *\x00Disallow: /private"
    
    # Mock fetcher to return robots.txt with null byte
    redis_politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt",
        final_url=f"http://{domain}/robots.txt", 
        status_code=200,
        text_content=robots_text_with_null
    )
    
    # Get robots for domain
    rerp = await redis_politeness_enforcer._get_robots_for_domain(domain)
    
    # Should treat as empty (allow all)
    assert rerp is not None
    assert rerp.is_allowed(redis_politeness_enforcer.config.user_agent, "/private") is True
    
    # Check that empty string was cached
    stored_content = await redis_politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == ""
```

Let me fix the linter errors in the test file:

```python
import redis.asyncio as redis
from robotexclusionrulesparser import RobotExclusionRulesParser

from crawler_module.politeness import RedisPolitenessEnforcer, DEFAULT_ROBOTS_TXT_TTL, MIN_CRAWL_DELAY_SECONDS
from crawler_module.config import CrawlerConfig
from crawler_module.fetcher import Fetcher, FetchResult

->

import redis.asyncio as redis
from robotexclusionrulesparser import RobotExclusionRulesParser  # type: ignore

from crawler_module.politeness import RedisPolitenessEnforcer, DEFAULT_ROBOTS_TXT_TTL, MIN_CRAWL_DELAY_SECONDS  # type: ignore
from crawler_module.config import CrawlerConfig  # type: ignore
from crawler_module.fetcher import Fetcher, FetchResult  # type: ignore
```

```python
is_excluded = await redis_client.hget(f'domain:{domain}', 'is_excluded')
```

```python
assert await pe.is_url_allowed(f"http://{seeded_domain}/page") is True
```

Let me fix the await type issues:

```python
    # Verify domains are marked as excluded in Redis
    expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
    for domain in expected_domains:
        is_excluded = await redis_client.hget(f'domain:{domain}', 'is_excluded')
        assert is_excluded == '1', f"Domain {domain} should be marked as excluded"

->

    # Verify domains are marked as excluded in Redis
    expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
    for domain in expected_domains:
        is_excluded = await redis_client.hget(f'domain:{domain}', 'is_excluded')  # type: ignore[misc]
        assert is_excluded == '1', f"Domain {domain} should be marked as excluded"
```

```python
    # Mark one domain as seeded
    await redis_client.hset(f'domain:{seeded_domain}', 'is_seeded', '1')
    
    # Seeded domain should be allowed
    assert await pe.is_url_allowed(f"http://{seeded_domain}/page") is True
    
    # Non-seeded domain should not be allowed
    assert await pe.is_url_allowed(f"http://{non_seeded_domain}/page") is False

->

    # Mark one domain as seeded
    await redis_client.hset(f'domain:{seeded_domain}', 'is_seeded', '1')  # type: ignore[misc]
    
    # Seeded domain should be allowed
    assert await pe.is_url_allowed(f"http://{seeded_domain}/page") is True
    
    # Non-seeded domain should not be allowed
    assert await pe.is_url_allowed(f"http://{non_seeded_domain}/page") is False
```

Let me run a quick test to see if the new tests work correctly:

Let me run a more comprehensive test to ensure the Redis integration is working properly:

Let's run all the Redis-based politeness tests to ensure they all pass:

Let's also verify that the Redis frontier tests still pass:

## Summary

I've successfully completed the Redis-backed implementation of the PolitenessEnforcer! Here's what was accomplished:

### 1. **Created RedisPolitenessEnforcer Class** (in `politeness.py`)
- Implements the same public interface as the original PolitenessEnforcer
- Uses Redis for storing:
  - Domain metadata (robots.txt cache, last fetch times, manual exclusions)
  - Uses Redis hash structures with keys like `domain:{domain}`
- Maintains an in-memory LRU cache for frequently accessed exclusion data
- Properly handles robots.txt fetching, caching, and expiration

### 2. **Created Comprehensive Test Suite** (`test_politeness_redis.py`)
- Ported all tests from the original `test_politeness.py`
- All 22 tests are passing, covering:
  - Manual exclusions loading from files
  - Robots.txt caching and expiration in Redis
  - URL allowance checks (robots.txt + manual exclusions)
  - Crawl delay calculations
  - Domain fetch timing with `next_fetch_time` tracking
  - LRU cache behavior for exclusions
  - Edge cases like null bytes in robots.txt

### Key Implementation Details:
- **Redis Structure**: Uses hash keys like `domain:{domain}` with fields:
  - `robots_txt`: Cached robots.txt content
  - `robots_expires`: Expiration timestamp
  - `is_excluded`: Manual exclusion flag
  - `is_seeded`: Seeded domain flag
  - `next_fetch_time`: When domain can be fetched next
- **Compatibility**: Maintains the same interface as the database-backed version
- **Performance**: In-memory caching reduces Redis queries for frequently accessed domains

According to the `redis_only_architecture.md` plan, the next steps in Phase 2 would be:
- Update StorageManager (add_visited_page) for Redis
- Update StorageManager unit tests
- Update CrawlerOrchestrator to use new frontier and politeness

What would you like to do next?

