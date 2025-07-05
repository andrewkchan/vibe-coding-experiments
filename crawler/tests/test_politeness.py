import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import time
from unittest.mock import MagicMock, AsyncMock, patch
import redis.asyncio as redis
from urllib.robotparser import RobotFileParser

from crawler_module.politeness import PolitenessEnforcer
from crawler_module.config import CrawlerConfig
from crawler_module.fetcher import Fetcher, FetchResult

# Note: redis_test_client fixture is imported from conftest.py
# This ensures we use db=15 for tests, not production db=0

@pytest.fixture
def mock_fetcher() -> MagicMock:
    return MagicMock(spec=Fetcher)

@pytest_asyncio.fixture
async def politeness_enforcer(
    test_config: CrawlerConfig,
    redis_test_client: redis.Redis,
    mock_fetcher: MagicMock
) -> PolitenessEnforcer:
    test_config.data_dir.mkdir(parents=True, exist_ok=True)
    pe = PolitenessEnforcer(config=test_config, redis_client=redis_test_client, fetcher=mock_fetcher, pod_id=0)
    await pe.initialize()
    return pe

# --- Tests for _load_manual_exclusions ---
@pytest.mark.asyncio
async def test_load_manual_exclusions_no_file(test_config: CrawlerConfig, redis_test_client: redis.Redis, mock_fetcher: MagicMock):
    test_config.exclude_file = None
    pe = PolitenessEnforcer(config=test_config, redis_client=redis_test_client, fetcher=mock_fetcher, pod_id=0)
    
    # Should initialize without error
    await pe.initialize()
    assert pe is not None

@pytest.mark.asyncio
async def test_load_manual_exclusions_with_file(
    test_config: CrawlerConfig,
    redis_test_client: redis.Redis,
    mock_fetcher: MagicMock,
    tmp_path: Path
):
    exclude_file_path = tmp_path / "custom_excludes.txt"
    test_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    pe = PolitenessEnforcer(config=test_config, redis_client=redis_test_client, fetcher=mock_fetcher, pod_id=0)
    await pe.initialize()
    
    # Verify domains are marked as excluded in Redis
    expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
    for domain in expected_domains:
        is_excluded = await redis_test_client.hget(f'domain:{domain}', 'is_excluded')  # type: ignore[misc]
        assert is_excluded == '1', f"Domain {domain} should be marked as excluded"
    
    if exclude_file_path.exists():
        exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) ---
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer):
    """Tests that if a parser is in memory, Redis is not hit."""
    domain = "example.com"
    mock_rfp = MagicMock(spec=RobotFileParser)
    politeness_enforcer.robots_parsers.put(domain, mock_rfp)

    # Act
    rfp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rfp == mock_rfp
    # No Redis or fetcher calls should be made
    politeness_enforcer.fetcher.fetch_url.assert_not_called()

@pytest.mark.asyncio
async def test_get_robots_from_redis_cache_fresh(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    """Tests getting a fresh robots.txt from the Redis cache when not in memory."""
    domain = "rediscached.com"
    robots_text = "User-agent: TestCrawler\nDisallow: /private"
    future_expiry = int(time.time()) + test_config.robots_cache_ttl_seconds

    # Setup: Ensure not in memory, but Redis will return a fresh record
    assert domain not in politeness_enforcer.robots_parsers
    
    # Store in Redis
    await politeness_enforcer.redis.hset(f'domain:{domain}', mapping={
        'robots_txt': robots_text,
        'robots_expires': str(future_expiry)
    })
    
    # Act
    rfp = await politeness_enforcer._get_robots_for_domain(domain)
    
    # Assert
    # 1. Web was NOT fetched
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    # 2. Parser is correct
    assert rfp is not None
    assert rfp.can_fetch(test_config.user_agent, f"http://{domain}/private") is False
    # 3. In-memory cache is now populated
    assert domain in politeness_enforcer.robots_parsers
    assert politeness_enforcer.robots_parsers.get(domain) == rfp

@pytest.mark.asyncio
async def test_get_robots_redis_cache_stale_then_fetch_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fetching from web when Redis cache is stale."""
    domain = "staleredis.com"
    past_expiry = int(time.time()) - 1000
    new_robots_text = "User-agent: *\nDisallow: /new"

    # Setup: Redis returns a stale record
    assert domain not in politeness_enforcer.robots_parsers
    await politeness_enforcer.redis.hset(f'domain:{domain}', mapping={
        'robots_txt': 'stale content',
        'robots_expires': str(past_expiry)
    })
    
    # Mock the web fetch to succeed
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=new_robots_text
    )

    # Act
    rfp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rfp is not None
    # 1. Web was fetched (since Redis was stale)
    politeness_enforcer.fetcher.fetch_url.assert_called_once()
    # 2. Redis was updated with the new content
    stored_content = await politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == new_robots_text
    # 3. In-memory cache is now populated
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_no_cache_fetch_http_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fetching from web when no caches are available."""
    domain = "newfetch.com"
    robots_text = "User-agent: *\nAllow: /"

    # Setup: No in-memory or Redis cache
    assert domain not in politeness_enforcer.robots_parsers
    # Mock web fetch
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=robots_text
    )

    # Act
    await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    politeness_enforcer.fetcher.fetch_url.assert_called_once()
    # Redis should be updated
    stored_content = await politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == robots_text
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_fail_then_https_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fallback from HTTP to HTTPS for robots.txt fetch."""
    domain = "httpsfallback.com"
    robots_text_https = "User-agent: *\nDisallow: /onlyhttps"

    # Setup
    assert domain not in politeness_enforcer.robots_parsers
    # Mock fetcher to fail on HTTP and succeed on HTTPS
    fetch_result_http = FetchResult(initial_url="...", final_url="...", status_code=500)
    fetch_result_https = FetchResult(
        initial_url="...", final_url="...", status_code=200, text_content=robots_text_https
    )
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_http, fetch_result_https]

    # Act
    await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    stored_content = await politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == robots_text_https
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_404_https_404(politeness_enforcer: PolitenessEnforcer):
    """Tests that two 404s result in an empty (allow all) robots rule."""
    domain = "all404.com"
    # Setup
    assert domain not in politeness_enforcer.robots_parsers
    # Mock fetcher to return 404 for both calls
    fetch_result_404 = FetchResult(initial_url="...", final_url="...", status_code=404)
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_404, fetch_result_404]

    # Act
    rfp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rfp is not None
    assert rfp.can_fetch(politeness_enforcer.config.user_agent, "/") is True
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    # Redis should be updated with an empty string to cache the "not found" result
    stored_content = await politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == ""
    assert domain in politeness_enforcer.robots_parsers

# --- Tests for is_url_allowed (async) ---
@pytest.mark.asyncio
async def test_is_url_allowed_manually_excluded(politeness_enforcer: PolitenessEnforcer):
    domain = "manualexclude.com"
    url = f"http://{domain}/somepage"
    
    # Setup: Prime the manual exclusion cache correctly
    politeness_enforcer.exclusion_cache.put(domain, True)

    # Act
    is_allowed = await politeness_enforcer.is_url_allowed(url)

    # Assert
    assert is_allowed is False
    # Ensure _get_robots_for_domain is NOT called if manually excluded
    politeness_enforcer._get_robots_for_domain = AsyncMock()
    await politeness_enforcer.is_url_allowed(url)
    politeness_enforcer._get_robots_for_domain.assert_not_called()

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    # Use simplified user agent without parentheses for RobotFileParser compatibility
    robots_text = "User-agent: TestCrawler\nDisallow: /disallowed"

    # Setup: Ensure not manually excluded (prime cache correctly)
    politeness_enforcer.exclusion_cache.put(domain, False)
    
    # Create a RobotFileParser and parse the robots.txt
    rfp = RobotFileParser()
    rfp.parse(robots_text.split('\n'))
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rfp)

    # Test Allowed URL
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Test Disallowed URL
    politeness_enforcer._get_robots_for_domain.reset_mock()
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

@pytest.mark.asyncio
async def test_is_url_allowed_seeded_urls_only(redis_test_client: redis.Redis, mock_fetcher: MagicMock, test_config: CrawlerConfig):
    """Test that non-seeded domains are excluded when seeded_urls_only is True."""
    test_config.seeded_urls_only = True
    pe = PolitenessEnforcer(config=test_config, redis_client=redis_test_client, fetcher=mock_fetcher, pod_id=0)
    await pe.initialize()
    
    seeded_domain = "seeded.com"
    non_seeded_domain = "notseeded.com"
    
    # Mark one domain as seeded
    await redis_test_client.hset(f'domain:{seeded_domain}', 'is_seeded', '1')  # type: ignore[misc]
    
    # Seeded domain should be allowed
    assert await pe.is_url_allowed(f"http://{seeded_domain}/page") is True
    
    # Non-seeded domain should not be allowed
    assert await pe.is_url_allowed(f"http://{non_seeded_domain}/page") is False

# --- Tests for get_crawl_delay (async) ---
@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_agent_specific(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "delaytest.com"
    for agent_delay in [test_config.politeness_delay_seconds - 1, test_config.politeness_delay_seconds, test_config.politeness_delay_seconds + 1]:
        # Use simplified user agent for robots.txt (without version/parens)
        robots_text = f"User-agent: TestCrawler\nCrawl-delay: {agent_delay}"
        
        # Create a RobotFileParser and parse the robots.txt
        rfp = RobotFileParser()
        rfp.parse(robots_text.split('\n'))
        rfp._content = robots_text  # type: ignore[attr-defined]
        
        # Mock _get_robots_for_domain to return our parser
        politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rfp)

        delay = await politeness_enforcer.get_crawl_delay(domain)
        assert delay == max(float(agent_delay), float(test_config.politeness_delay_seconds))

@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_wildcard(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "wildcarddelay.com"
    for wildcard_delay in [test_config.politeness_delay_seconds - 10, test_config.politeness_delay_seconds, test_config.politeness_delay_seconds + 10]:
        robots_text = f"User-agent: AnotherBot\nCrawl-delay: 50\nUser-agent: *\nCrawl-delay: {wildcard_delay}"

        # Create a RobotFileParser and parse the robots.txt
        rfp = RobotFileParser()
        rfp.parse(robots_text.split('\n'))
        rfp._content = robots_text  # type: ignore[attr-defined]
        
        # Mock _get_robots_for_domain to return our parser
        politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rfp)

        delay = await politeness_enforcer.get_crawl_delay(domain)
        assert delay == max(float(wildcard_delay), float(test_config.politeness_delay_seconds))

@pytest.mark.asyncio
async def test_get_crawl_delay_default_no_robots_rule(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "nodelayrule.com"
    robots_text = "User-agent: *\nDisallow: /"

    rfp = RobotFileParser()
    rfp.parse(robots_text.split('\n'))
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rfp)

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(test_config.politeness_delay_seconds)

@pytest.mark.asyncio
async def test_get_crawl_delay_respects_min_crawl_delay(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "shortdelay.com"
    for robot_delay in [test_config.politeness_delay_seconds - 10, test_config.politeness_delay_seconds, test_config.politeness_delay_seconds + 10]:
        robots_text = f"User-agent: *\nCrawl-delay: {robot_delay}"

        # Create a RobotFileParser and parse the robots.txt
        rfp = RobotFileParser()
        rfp.parse(robots_text.split('\n'))
        rfp._content = robots_text  # type: ignore[attr-defined]
        
        # Mock _get_robots_for_domain to return our parser
        politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rfp)

        delay = await politeness_enforcer.get_crawl_delay(domain)
        assert delay == max(float(robot_delay), float(test_config.politeness_delay_seconds))

# --- Tests for can_fetch_domain_now (async) ---
@pytest.mark.asyncio
async def test_can_fetch_domain_now_no_previous_fetch(politeness_enforcer: PolitenessEnforcer):
    domain = "newdomain.com"
    
    # No previous fetch time in Redis
    can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is True

@pytest.mark.asyncio
async def test_can_fetch_domain_now_after_sufficient_delay(politeness_enforcer: PolitenessEnforcer):
    domain = "readyagain.com"
    current_time = int(time.time())
    
    # Set next fetch time to be in the past
    await politeness_enforcer.redis.hset(
        f'domain:{domain}', 
        'next_fetch_time', 
        str(current_time - 10)
    )
    
    can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is True

@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(politeness_enforcer: PolitenessEnforcer):
    domain = "notready.com"
    current_time = int(time.time())
    
    # Set next fetch time to be in the future
    await politeness_enforcer.redis.hset(
        f'domain:{domain}', 
        'next_fetch_time', 
        str(current_time + 30)
    )
    
    can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)
    assert can_fetch is False

# --- Tests for record_domain_fetch_attempt (async) ---
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_new_domain(politeness_enforcer: PolitenessEnforcer, test_config: CrawlerConfig):
    domain = "recordnew.com"
    
    # Mock get_crawl_delay
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(test_config.politeness_delay_seconds))
    
    # Record fetch
    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify
    politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)
    
    # Check that next_fetch_time was set correctly
    next_fetch_time = await politeness_enforcer.redis.hget(f'domain:{domain}', 'next_fetch_time')
    assert next_fetch_time is not None
    assert int(next_fetch_time) > int(time.time())

@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_existing_domain(politeness_enforcer: PolitenessEnforcer):
    domain = "recordexisting.com"
    
    # Set initial fetch time
    initial_time = str(int(time.time()) - 100)
    await politeness_enforcer.redis.hset(f'domain:{domain}', 'next_fetch_time', initial_time)
    
    # Mock get_crawl_delay
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=70.0)
    
    # Record new fetch
    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify next_fetch_time was updated
    new_next_fetch_time = await politeness_enforcer.redis.hget(f'domain:{domain}', 'next_fetch_time')
    assert int(new_next_fetch_time) > int(initial_time)
    assert int(new_next_fetch_time) >= int(time.time()) + 70

# --- Tests for exclusion cache ---
@pytest.mark.asyncio
async def test_exclusion_cache_lru_behavior(politeness_enforcer: PolitenessEnforcer):
    """Test that exclusion cache implements LRU eviction."""
    # Create a new cache with a small size for testing
    from crawler_module.utils import LRUCache
    politeness_enforcer.exclusion_cache = LRUCache(3)
    
    # Add domains to cache
    domains = ["domain1.com", "domain2.com", "domain3.com", "domain4.com"]
    
    for i, domain in enumerate(domains[:3]):
        # Store in Redis and cache
        await politeness_enforcer.redis.hset(f'domain:{domain}', 'is_excluded', '0')
        await politeness_enforcer._check_manual_exclusion(domain)
    
    # Cache should have 3 items
    assert len(politeness_enforcer.exclusion_cache) == 3
    
    # Access domain1 to make it recently used
    await politeness_enforcer._check_manual_exclusion("domain1.com")
    
    # Add domain4, which should evict domain2 (least recently used)
    await politeness_enforcer.redis.hset(f'domain:{domains[3]}', 'is_excluded', '0')
    await politeness_enforcer._check_manual_exclusion(domains[3])
    
    # Check cache contents
    assert "domain1.com" in politeness_enforcer.exclusion_cache
    assert "domain2.com" not in politeness_enforcer.exclusion_cache  # Evicted
    assert "domain3.com" in politeness_enforcer.exclusion_cache
    assert "domain4.com" in politeness_enforcer.exclusion_cache

# --- Tests for null byte handling in robots.txt ---
@pytest.mark.asyncio
async def test_robots_txt_with_null_byte(politeness_enforcer: PolitenessEnforcer):
    """Test that robots.txt with null bytes is handled correctly."""
    domain = "nullbyte.com"
    robots_text_with_null = "User-agent: *\x00Disallow: /private"
    
    # Mock fetcher to return robots.txt with null byte
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt",
        final_url=f"http://{domain}/robots.txt", 
        status_code=200,
        text_content=robots_text_with_null
    )
    
    # Get robots for domain
    rfp = await politeness_enforcer._get_robots_for_domain(domain)
    
    # Should treat as empty (allow all)
    assert rfp is not None
    assert rfp.can_fetch(politeness_enforcer.config.user_agent, "/private") is True
    
    # Check that empty string was cached
    stored_content = await politeness_enforcer.redis.hget(f'domain:{domain}', 'robots_txt')
    assert stored_content == "" 