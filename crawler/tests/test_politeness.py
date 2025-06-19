import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import time
import sqlite3
from unittest.mock import MagicMock, mock_open, patch, AsyncMock

from crawler_module.politeness import PolitenessEnforcer, DEFAULT_ROBOTS_TXT_TTL, MIN_CRAWL_DELAY_SECONDS
from crawler_module.config import CrawlerConfig
from crawler_module.storage import StorageManager # For type hinting, will be mocked
from crawler_module.fetcher import Fetcher, FetchResult # For type hinting and mocking
from robotexclusionrulesparser import RobotExclusionRulesParser
from crawler_module.db_backends import create_backend, DatabaseBackend # New import for db backend

@pytest.fixture
def dummy_config(tmp_path: Path) -> CrawlerConfig:
    return CrawlerConfig(
        seed_file=tmp_path / "seeds.txt",
        email="test@example.com",
        data_dir=tmp_path / "test_data",
        exclude_file=None, # Will be set in specific tests
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="TestCrawler/1.0 (pytest)",
        seeded_urls_only=False,
        db_type="sqlite",  # Added - default to SQLite for tests
        db_url=None,  # Added - SQLite doesn't need a URL
        # Redis configuration - use default values
        redis_host="localhost",
        redis_port=6379,
        redis_db=15,  # Tests will override this via redis_test_client fixture
        redis_password=None
    )

@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # PolitenessEnforcer will now access self.storage.db for database operations
    mock.db = AsyncMock()
    mock.config = MagicMock()
    mock.config.db_type = "sqlite"  # Default to SQLite for tests
    
    # Mock the async database methods that PolitenessEnforcer uses
    mock.db.execute = AsyncMock()
    mock.db.fetch_one = AsyncMock()
    
    # db_path might still be needed if any part of PolitenessEnforcer uses it directly (it shouldn't for DB ops now)
    # For _load_manual_exclusions, it uses config.exclude_file, not storage.db_path for the file read.
    # storage.db_path was primarily for direct sqlite3.connect calls, which are now gone from PE's DB methods.
    # Let's keep it for now in case any non-DB logic in PE might reference it, though ideally not.
    mock.db_path = tmp_path / "mock_politeness_state.db" 
    return mock

@pytest.fixture
def mock_fetcher() -> MagicMock:
    return MagicMock(spec=Fetcher)

@pytest_asyncio.fixture
async def politeness_enforcer(
    dummy_config: CrawlerConfig, 
    mock_storage_manager: MagicMock, 
    mock_fetcher: MagicMock
) -> PolitenessEnforcer:
    # Ensure data_dir for politeness enforcer if it tries to use it (though not directly here)
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    pe = PolitenessEnforcer(config=dummy_config, storage=mock_storage_manager, fetcher=mock_fetcher)
    return pe

@pytest_asyncio.fixture
async def storage_manager_for_exclusion_test(dummy_config: CrawlerConfig, db_backend: DatabaseBackend) -> StorageManager:
    """Provides a real StorageManager instance using a temporary DB path from dummy_config and a real backend."""
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    sm = StorageManager(config=dummy_config, db_backend=db_backend)
    await sm.init_db_schema()
    yield sm

# --- Tests for _load_manual_exclusions --- 
def test_load_manual_exclusions_no_file(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    dummy_config.exclude_file = None
    pe = PolitenessEnforcer(config=dummy_config, storage=politeness_enforcer.storage, fetcher=politeness_enforcer.fetcher)
    
    # Since _load_manual_exclusions is now async and doesn't get called in __init__,
    # we don't need to check for database calls here
    # The test just verifies that creating a PolitenessEnforcer with no exclude file works
    assert pe is not None


@pytest.mark.asyncio
async def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager,
    mock_fetcher: MagicMock,
    tmp_path: Path,
    db_backend: DatabaseBackend
):
    exclude_file_path = tmp_path / "custom_excludes.txt"
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # Call _load_manual_exclusions manually since it's async
    await pe._load_manual_exclusions()
    
    # Use the database backend to verify the data
    expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
    for domain in expected_domains:
        row = await db_backend.fetch_one("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
        assert row is not None, f"Domain {domain} should be in domain_metadata"
        assert row[0] == 1, f"Domain {domain} should be marked as manually excluded"
    
    row = await db_backend.fetch_one("SELECT COUNT(*) FROM domain_metadata WHERE is_manually_excluded = 1")
    count = row[0]
    assert count == len(expected_domains)

    if exclude_file_path.exists(): 
        exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) --- 
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer):
    """Tests that if a parser is in memory, the DB is not hit."""
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    politeness_enforcer.robots_parsers[domain] = mock_rerp

    # Act
    rerp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp == mock_rerp
    # With the new logic, if it's in the memory cache, no other calls are made.
    politeness_enforcer.storage.db.fetch_one.assert_not_called()
    politeness_enforcer.fetcher.fetch_url.assert_not_called()

@pytest.mark.asyncio
async def test_get_robots_from_db_cache_fresh(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    """Tests getting a fresh robots.txt from the DB cache when not in memory."""
    domain = "dbcached.com"
    robots_text = "User-agent: TestCrawler\nDisallow: /private"
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    # Setup: Ensure not in memory, but DB mock will return a fresh record
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = (robots_text, future_expiry)
    
    # Act
    rerp = await politeness_enforcer._get_robots_for_domain(domain)
        
    # Assert
    # 1. DB was checked
    politeness_enforcer.storage.db.fetch_one.assert_called_once_with(
        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?",
        (domain,),
        query_name="get_cached_robots"
    )
    # 2. Web was NOT fetched
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    # 3. Parser is correct
    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/private") is False
    # 4. In-memory cache is now populated
    assert domain in politeness_enforcer.robots_parsers
    assert politeness_enforcer.robots_parsers[domain] == rerp

@pytest.mark.asyncio
async def test_get_robots_db_cache_stale_then_fetch_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fetching from web when DB cache is stale."""
    domain = "staledb.com"
    past_expiry = int(time.time()) - 1000
    new_robots_text = "User-agent: *\nDisallow: /new"

    # Setup: DB returns a stale record
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = ("stale content", past_expiry)
    # Mock the web fetch to succeed
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=new_robots_text
    )
    # Mock the DB update
    politeness_enforcer.storage.db.execute = AsyncMock()

    # Act
    rerp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp is not None
    # 1. DB was checked for cache
    politeness_enforcer.storage.db.fetch_one.assert_called_once()
    # 2. Web was fetched (since DB was stale)
    politeness_enforcer.fetcher.fetch_url.assert_called_once()
    # 3. DB was updated with the new content
    politeness_enforcer.storage.db.execute.assert_called_once()
    call_args = politeness_enforcer.storage.db.execute.call_args
    # Correctly access positional args: call_args[0] for args, call_args[1] for kwargs
    query_arg = call_args[0][0]
    params_arg = call_args[0][1]
    assert "INSERT OR REPLACE" in query_arg
    assert params_arg[1] == new_robots_text # Check content
    # 4. In-memory cache is now populated
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_no_cache_fetch_http_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fetching from web when no caches are available."""
    domain = "newfetch.com"
    robots_text = "User-agent: *\nAllow: /"

    # Setup: No in-memory or DB cache
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = None
    # Mock web fetch and DB update
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt",
        status_code=200, text_content=robots_text
    )
    politeness_enforcer.storage.db.execute = AsyncMock()

    # Act
    await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    politeness_enforcer.storage.db.fetch_one.assert_called_once() # DB cache check
    politeness_enforcer.fetcher.fetch_url.assert_called_once() # Web fetch
    politeness_enforcer.storage.db.execute.assert_called_once() # DB cache update
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_fail_then_https_success(politeness_enforcer: PolitenessEnforcer):
    """Tests fallback from HTTP to HTTPS for robots.txt fetch."""
    domain = "httpsfallback.com"
    robots_text_https = "User-agent: *\nDisallow: /onlyhttps"

    # Setup
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = None
    politeness_enforcer.storage.db.execute = AsyncMock()
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
    politeness_enforcer.storage.db.execute.assert_called_once() # DB update should happen
    params_arg = politeness_enforcer.storage.db.execute.call_args[0][1]
    assert params_arg[1] == robots_text_https
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_404_https_404(politeness_enforcer: PolitenessEnforcer):
    """Tests that two 404s result in an empty (allow all) robots rule."""
    domain = "all404.com"
    # Setup
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = None
    politeness_enforcer.storage.db.execute = AsyncMock()
    # Mock fetcher to return 404 for both calls
    fetch_result_404 = FetchResult(initial_url="...", final_url="...", status_code=404)
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_404, fetch_result_404]

    # Act
    rerp = await politeness_enforcer._get_robots_for_domain(domain)

    # Assert
    assert rerp is not None
    assert rerp.is_allowed(politeness_enforcer.config.user_agent, "/") is True
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    # DB should be updated with an empty string to cache the "not found" result
    politeness_enforcer.storage.db.execute.assert_called_once()
    params_arg = politeness_enforcer.storage.db.execute.call_args[0][1]
    assert params_arg[1] == ""
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_all_fetches_fail_connection_error(politeness_enforcer: PolitenessEnforcer):
    """Tests that connection errors result in an empty (allow all) robots rule."""
    domain = "allconnectfail.com"
    # Setup
    politeness_enforcer.robots_parsers.pop(domain, None)
    politeness_enforcer.storage.db.fetch_one.return_value = None
    politeness_enforcer.storage.db.execute = AsyncMock()
    # Mock fetcher to simulate connection error
    fetch_fail_result = FetchResult(initial_url="...", final_url="...", status_code=901)
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_fail_result, fetch_fail_result]

    # Act
    rerp = await politeness_enforcer._get_robots_for_domain(domain)
    
    # Assert
    assert rerp is not None
    assert rerp.is_allowed(politeness_enforcer.config.user_agent, "/") is True
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    politeness_enforcer.storage.db.execute.assert_called_once()
    params_arg = politeness_enforcer.storage.db.execute.call_args[0][1]
    assert params_arg[1] == "" # Cache empty string
    assert domain in politeness_enforcer.robots_parsers

# --- Tests for is_url_allowed (async) ---
@pytest.mark.asyncio
async def test_is_url_allowed_manually_excluded(politeness_enforcer: PolitenessEnforcer):
    domain = "manualexclude.com"
    url = f"http://{domain}/somepage"
    
    # Setup: Prime the manual exclusion cache correctly
    politeness_enforcer.exclusion_cache[domain] = True
    politeness_enforcer.exclusion_cache_order[domain] = None

    # Act
    is_allowed = await politeness_enforcer.is_url_allowed(url)

    # Assert
    assert is_allowed is False
    # Ensure _get_robots_for_domain is NOT called if manually excluded
    politeness_enforcer._get_robots_for_domain = AsyncMock()
    await politeness_enforcer.is_url_allowed(url)
    politeness_enforcer._get_robots_for_domain.assert_not_called()

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # Setup: Ensure not manually excluded (prime cache correctly)
    politeness_enforcer.exclusion_cache[domain] = False
    politeness_enforcer.exclusion_cache_order[domain] = None
    
    # Mock _get_robots_for_domain to return a pre-configured parser
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # Test Allowed URL
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Test Disallowed URL
    politeness_enforcer._get_robots_for_domain.reset_mock()
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Ensure the DB mock for exclusion checks was not hit because the cache was primed
    politeness_enforcer.storage.db.fetch_one.assert_not_called()

@pytest.mark.asyncio
async def test_is_url_allowed_no_domain(politeness_enforcer: PolitenessEnforcer):
    malformed_url = "nodomainurl"
    # Mock extract_domain to return None for this specific input if needed, though it should handle it.
    # Default behavior is to allow if domain cannot be extracted.
    assert await politeness_enforcer.is_url_allowed(malformed_url) is True

@pytest.mark.asyncio
async def test_is_url_allowed_robots_fetch_fails_defaults_to_allow(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsfail.com"
    url = f"http://{domain}/anypage"

    # Mock the async database methods for manual exclusion check
    politeness_enforcer.storage.db.fetch_one.return_value = (0,)  # Domain is NOT manually excluded
    
    # Mock _get_robots_for_domain to simulate it returning None (e.g., all fetch attempts failed)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=None)
    
    # Ensure no in-memory RERP for this domain
    politeness_enforcer.robots_parsers.pop(domain, None)
    
    assert await politeness_enforcer.is_url_allowed(url) is True 
    politeness_enforcer.storage.db.fetch_one.assert_called_once()  # For manual exclusion
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

# --- Tests for get_crawl_delay (async) ---
@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_agent_specific(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "delaytest.com"
    agent_delay = 10
    robots_text = f"User-agent: {dummy_config.user_agent}\nCrawl-delay: {agent_delay}"
    
    # Mock _get_robots_for_domain to return a parser with this rule
    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp) # type: ignore

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == max(float(agent_delay), float(MIN_CRAWL_DELAY_SECONDS)) 
    # If agent_delay was > MIN_CRAWL_DELAY_SECONDS, it would be agent_delay
    # If agent_delay was < MIN_CRAWL_DELAY_SECONDS, it would be MIN_CRAWL_DELAY_SECONDS

@pytest.mark.asyncio
async def test_get_crawl_delay_from_robots_wildcard(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "wildcarddelay.com"
    wildcard_delay = 5
    # Ensure our specific agent has no rule, but wildcard does
    robots_text = f"User-agent: AnotherBot\nCrawl-delay: 50\nUser-agent: *\nCrawl-delay: {wildcard_delay}"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent # This is important for how RERP resolves rules
    rerp.parse(robots_text)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp) # type: ignore

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == max(float(wildcard_delay), float(MIN_CRAWL_DELAY_SECONDS))

@pytest.mark.asyncio
async def test_get_crawl_delay_default_no_robots_rule(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "nodelayrule.com"
    robots_text = "User-agent: *\nDisallow: /"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp) # type: ignore

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(MIN_CRAWL_DELAY_SECONDS)

@pytest.mark.asyncio
async def test_get_crawl_delay_robots_fetch_fails(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsfaildelay.com"
    # Simulate _get_robots_for_domain returning None (e.g., fetcher completely failed)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=None) # type: ignore

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(MIN_CRAWL_DELAY_SECONDS)

@pytest.mark.asyncio
async def test_get_crawl_delay_respects_min_crawl_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "shortdelay.com"
    short_robot_delay = 1 # Shorter than MIN_CRAWL_DELAY_SECONDS
    robots_text = f"User-agent: *\nCrawl-delay: {short_robot_delay}"

    rerp = RobotExclusionRulesParser()
    rerp.user_agent = dummy_config.user_agent
    rerp.parse(robots_text)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp) # type: ignore

    delay = await politeness_enforcer.get_crawl_delay(domain)
    assert delay == float(MIN_CRAWL_DELAY_SECONDS)

# --- Tests for can_fetch_domain_now (async) ---
@pytest.mark.asyncio
async def test_can_fetch_domain_now_no_previous_fetch(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "newdomain.com"
    
    # Mock the async database methods
    politeness_enforcer.storage.db.fetch_one.return_value = None  # No previous fetch
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)

    assert can_fetch is True
    # Verify that the database was queried for last fetch time
    politeness_enforcer.storage.db.fetch_one.assert_called_once_with(
        "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", 
        (domain,)
    )


@pytest.mark.asyncio
async def test_can_fetch_domain_now_after_sufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "readyagain.com"
    crawl_delay_val = 30.0
    last_fetch = int(time.time()) - int(crawl_delay_val) - 1 

    # Mock the async database methods
    politeness_enforcer.storage.db.fetch_one.return_value = (last_fetch,)  # Previous fetch timestamp
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=crawl_delay_val)

    can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)
        
    assert can_fetch is True
    # Verify database query
    politeness_enforcer.storage.db.fetch_one.assert_called_once_with(
        "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", 
        (domain,)
    )
    # Verify get_crawl_delay was called
    politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)


@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "notready.com"
    crawl_delay_val = MIN_CRAWL_DELAY_SECONDS
    last_fetch = int(time.time()) - int(crawl_delay_val / 2)  # Fetched half the delay time ago

    # Mock the async database methods
    politeness_enforcer.storage.db.fetch_one.return_value = (last_fetch,)  # Previous fetch timestamp
    
    # Mock get_crawl_delay
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False

    # Verify database query
    politeness_enforcer.storage.db.fetch_one.assert_called_once_with(
        "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", 
        (domain,)
    )
    # Verify get_crawl_delay was called
    politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)

@pytest.mark.asyncio
async def test_can_fetch_domain_now_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbfetcherror.com"
    
    # Simulate DB error when trying to fetch
    politeness_enforcer.storage.db.fetch_one.side_effect = Exception("Simulated DB error")
    
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False
    
    # Verify fetch_one was attempted
    politeness_enforcer.storage.db.fetch_one.assert_called_once_with(
        "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", 
        (domain,)
    )
    # get_crawl_delay should not be called due to DB error
    politeness_enforcer.get_crawl_delay.assert_not_called()

# --- Tests for record_domain_fetch_attempt (async) ---
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_new_domain(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recordnew.com"
    
    # Mock the async database methods
    politeness_enforcer.storage.db.execute.return_value = None

    await politeness_enforcer.record_domain_fetch_attempt(domain)

    # Should only be 1 execute call with INSERT OR REPLACE
    assert politeness_enforcer.storage.db.execute.call_count == 1
    current_time_val = pytest.approx(int(time.time()), abs=2)
    
    # Check the INSERT OR REPLACE call
    call_args = politeness_enforcer.storage.db.execute.call_args
    assert "INSERT OR REPLACE INTO domain_metadata" in call_args[0][0]
    assert domain in call_args[0][1]
    # The timestamp should be approximately current time
    assert call_args[0][1][1] == current_time_val


@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_existing_domain(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recordexisting.com"

    # Mock the async database methods
    politeness_enforcer.storage.db.execute.return_value = None

    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Should only be 1 execute call with INSERT OR REPLACE
    assert politeness_enforcer.storage.db.execute.call_count == 1
    current_time_val = pytest.approx(int(time.time()), abs=2)
    
    # Check the INSERT OR REPLACE call
    call_args = politeness_enforcer.storage.db.execute.call_args
    assert "INSERT OR REPLACE INTO domain_metadata" in call_args[0][0]
    assert domain in call_args[0][1]
    # The timestamp should be approximately current time
    assert call_args[0][1][1] == current_time_val


@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recorddberror.com"
    
    # Simulate DB error during the execute call
    politeness_enforcer.storage.db.execute.side_effect = Exception("Simulated DB error")

    # Should not raise, but log the error
    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify that execute was attempted
    politeness_enforcer.storage.db.execute.assert_called_once()
    # The call should be INSERT OR REPLACE
    assert "INSERT OR REPLACE INTO domain_metadata" in politeness_enforcer.storage.db.execute.call_args[0][0] 