import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import time
from unittest.mock import MagicMock, AsyncMock, patch
import redis.asyncio as redis
from robotexclusionrulesparser import RobotExclusionRulesParser  # type: ignore

from crawler_module.politeness import RedisPolitenessEnforcer, DEFAULT_ROBOTS_TXT_TTL, MIN_CRAWL_DELAY_SECONDS  # type: ignore
from crawler_module.config import CrawlerConfig  # type: ignore
from crawler_module.fetcher import Fetcher, FetchResult  # type: ignore

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
        is_excluded = await redis_client.hget(f'domain:{domain}', 'is_excluded')  # type: ignore[misc]
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
    await redis_client.hset(f'domain:{seeded_domain}', 'is_seeded', '1')  # type: ignore[misc]
    
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