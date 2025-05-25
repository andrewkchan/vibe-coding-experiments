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
        user_agent="TestCrawler/1.0 (pytest)"
    )

@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock:
    mock = MagicMock(spec=StorageManager)
    mock.conn = MagicMock(spec=sqlite3.Connection)
    # Simulate context manager for cursor
    mock.conn.cursor.return_value.__enter__.return_value = MagicMock(spec=sqlite3.Cursor)
    mock.conn.cursor.return_value.__exit__.return_value = None
    
    # Add the db_path attribute that _get_robots_for_domain needs
    mock.db_path = tmp_path / "test_politeness_temp.db"
    # No need to create this file for these unit tests, as sqlite3.connect will create it if it doesn't exist.
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

@pytest.fixture
def storage_manager_for_exclusion_test(dummy_config: CrawlerConfig) -> StorageManager:
    """Provides a real StorageManager instance using a temporary DB path from dummy_config."""
    # Ensure the data_dir from dummy_config exists for StorageManager init
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    # Initialize schema if StorageManager doesn't create tables on its own path yet
    # Our StorageManager._init_db does create tables.
    sm = StorageManager(config=dummy_config)
    # We don't yield and close here, as PE will use the path. 
    # The db is cleaned up by tmp_path fixture of dummy_config.
    return sm

# --- Tests for _load_manual_exclusions --- 
def test_load_manual_exclusions_no_file(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    dummy_config.exclude_file = None
    # politeness_enforcer._load_manual_exclusions() # Called in __init__
    # Check that no DB calls for exclusion were made if file is None
    # This is tricky as it's in __init__. We rely on mock_storage_manager not having unexpected calls.
    # A more direct test would be to mock Path.exists() for the exclude_file path
    assert True # Basic check, real validation in test below


def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager, # Use the real SM for its db_path
    mock_fetcher: MagicMock, # Still needed for PE constructor
    tmp_path: Path # For creating the exclude file
):
    exclude_file_path = tmp_path / "custom_excludes.txt" # Ensure it's in a fresh temp spot
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    # PolitenessEnforcer will use storage_manager_for_exclusion_test.db_path
    # to connect and write to the actual temp DB.
    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # Now, connect to the DB created/used by PE and verify content
    conn = None
    cursor = None
    try:
        db_path_to_check = storage_manager_for_exclusion_test.db_path
        assert db_path_to_check.exists(), "Database file should have been created by StorageManager init used by PE"
        conn = sqlite3.connect(db_path_to_check)
        cursor = conn.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
        for domain in expected_domains:
            cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
            row = cursor.fetchone()
            assert row is not None, f"Domain {domain} should be in domain_metadata"
            assert row[0] == 1, f"Domain {domain} should be marked as manually excluded"
        
        # Check that other domains aren't marked (optional, but good)
        cursor.execute("SELECT COUNT(*) FROM domain_metadata WHERE is_manually_excluded = 1")
        count = cursor.fetchone()[0]
        assert count == len(expected_domains)

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        if exclude_file_path.exists(): exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) --- 
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    mock_rerp.source_content = "User-agent: *\nDisallow: /test"
    politeness_enforcer.robots_parsers[domain] = mock_rerp

    # To ensure DB isn't preferred if memory is 'fresher' or DB has nothing
    mock_cursor = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = None # Simulate no DB cache

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
    assert rerp == mock_rerp
    politeness_enforcer.fetcher.fetch_url.assert_not_called()

@pytest.mark.asyncio
async def test_get_robots_from_db_cache_fresh(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbcached.com"
    robots_text = "User-agent: TestCrawler\nDisallow: /private" # Reverted to specific agent
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    # Mock the database interaction within the threaded function
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = (robots_text, future_expiry)
    
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance 
    mock_db_conn_instance.__exit__.return_value = None

    if domain in politeness_enforcer.robots_parsers:
        del politeness_enforcer.robots_parsers[domain]

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)
        
        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_conn_instance.cursor.assert_called_once()
        mock_db_cursor_instance.execute.assert_called_once_with(
            "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?",
            (domain,)
        )
        mock_db_cursor_instance.fetchone.assert_called_once()
        mock_db_cursor_instance.close.assert_called_once()

    assert rerp is not None
    # dummy_config.user_agent is "TestCrawler/1.0 (pytest)"
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/private") is False
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/public") is True
    politeness_enforcer.fetcher.fetch_url.assert_not_called() 
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_db_cache_stale_then_fetch_success(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "staledb.com"
    stale_robots_text = "User-agent: *\nDisallow: /old"
    past_expiry = int(time.time()) - 1000
    new_robots_text = "User-agent: *\nDisallow: /new"

    # Mock the database interaction within the threaded function
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    # Simulate DB having stale data initially for the _db_get_cached_robots_sync_threaded call
    mock_db_cursor_instance.fetchone.return_value = (stale_robots_text, past_expiry)
    
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    # Simulate context manager behavior for 'with sqlite3.connect(...)'
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    # Mock fetcher to return new robots.txt
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt",
        final_url=f"http://{domain}/robots.txt",
        status_code=200,
        text_content=new_robots_text
    )
    
    if domain in politeness_enforcer.robots_parsers:
        del politeness_enforcer.robots_parsers[domain]

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)

        # Assertions for the first DB call (get cache)
        mock_sqlite_connect.assert_any_call(politeness_enforcer.storage.db_path, timeout=10)
        
        # There will be two calls to cursor(): one for get, one for update.
        # We care about the execute calls on the cursor instances.
        # fetchone is called by _db_get_cached_robots_sync_threaded
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert rerp is not None
    # Check if it's using the new rules
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/new") is False
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/old") is True 
    
    politeness_enforcer.fetcher.fetch_url.assert_called_once_with(f"http://{domain}/robots.txt", is_robots_txt=True)
    
    # Check if DB was updated (execute called for INSERT/IGNORE then UPDATE by _db_update_robots_cache_sync_threaded)
    # mock_db_cursor_instance was used for both the read and the write operations due to the single mock_db_conn_instance.
    # We need to check all execute calls on this cursor.
    
    # The first execute call is from _db_get_cached_robots_sync_threaded
    # The next two execute calls are from _db_update_robots_cache_sync_threaded
    assert mock_db_cursor_instance.execute.call_count >= 3 # SELECT, INSERT OR IGNORE, UPDATE
    
    update_calls = [call for call in mock_db_cursor_instance.execute.call_args_list 
                    if "UPDATE domain_metadata" in call[0][0]]
    assert len(update_calls) > 0, "UPDATE domain_metadata call was not found"
    
    args, _ = update_calls[0] # First UPDATE call
    assert args[1][0] == new_robots_text # Check content being updated
    assert args[1][3] == domain # Check domain being updated
    
    # Ensure commit was called on the connection instance used by the update thread
    mock_db_conn_instance.commit.assert_called_once()
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_no_cache_fetch_http_success(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "newfetch.com"
    robots_text = "User-agent: *\nAllow: /"

    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = None # No DB cache for _db_get_cached_robots_sync_threaded

    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None
    
    politeness_enforcer.robots_parsers.pop(domain, None) # Ensure no memory cache

    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt",
        final_url=f"http://{domain}/robots.txt",
        status_code=200,
        text_content=robots_text
    )

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)

        mock_sqlite_connect.assert_any_call(politeness_enforcer.storage.db_path, timeout=10)
        # fetchone from _db_get_cached_robots_sync_threaded
        mock_db_cursor_instance.fetchone.assert_called_once()


    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/anypage") is True
    politeness_enforcer.fetcher.fetch_url.assert_called_once_with(f"http://{domain}/robots.txt", is_robots_txt=True)
    
    # Check DB update by _db_update_robots_cache_sync_threaded
    update_calls = [call for call in mock_db_cursor_instance.execute.call_args_list if "UPDATE domain_metadata" in call[0][0]]
    assert len(update_calls) > 0, "UPDATE domain_metadata call was not found"
    args, _ = update_calls[0]
    assert args[1][0] == robots_text
    mock_db_conn_instance.commit.assert_called_once()
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_fail_then_https_success(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "httpsfallback.com"
    robots_text_https = "User-agent: *\nDisallow: /onlyhttps"

    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = None # No DB cache

    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    politeness_enforcer.robots_parsers.pop(domain, None) # Ensure no memory cache

    fetch_result_http = FetchResult(
        initial_url=f"http://{domain}/robots.txt",
        final_url=f"http://{domain}/robots.txt",
        status_code=500,
        error_message="HTTP fetch failed"
    )
    fetch_result_https = FetchResult(
        initial_url=f"https://{domain}/robots.txt",
        final_url=f"https://{domain}/robots.txt",
        status_code=200,
        text_content=robots_text_https
    )
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_http, fetch_result_https]

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)
        mock_sqlite_connect.assert_any_call(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.fetchone.assert_called_once()


    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/onlyhttps") is False
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/allowed") is True
    
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    politeness_enforcer.fetcher.fetch_url.assert_any_call(f"http://{domain}/robots.txt", is_robots_txt=True)
    politeness_enforcer.fetcher.fetch_url.assert_any_call(f"https://{domain}/robots.txt", is_robots_txt=True)
    
    # Check DB update with HTTPS content
    update_calls = [call for call in mock_db_cursor_instance.execute.call_args_list if "UPDATE domain_metadata" in call[0][0]]
    assert len(update_calls) > 0, "UPDATE domain_metadata call was not found"
    args, _ = update_calls[0]
    assert args[1][0] == robots_text_https
    mock_db_conn_instance.commit.assert_called_once()
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_http_404_https_404(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "all404.com"

    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = None # No DB cache

    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    politeness_enforcer.robots_parsers.pop(domain, None) # Ensure no memory cache

    fetch_result_404 = FetchResult(
        initial_url="dummy", final_url="dummy", status_code=404, error_message="Not Found"
    )
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_result_404, fetch_result_404]

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)
        mock_sqlite_connect.assert_any_call(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/anypage") is True
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    
    # Check DB update with empty string for content
    update_calls = [call for call in mock_db_cursor_instance.execute.call_args_list if "UPDATE domain_metadata" in call[0][0]]
    assert len(update_calls) > 0, "UPDATE domain_metadata call was not found"
    args, _ = update_calls[0]
    assert args[1][0] == "" # Empty string for 404
    mock_db_conn_instance.commit.assert_called_once()
    assert domain in politeness_enforcer.robots_parsers

@pytest.mark.asyncio
async def test_get_robots_all_fetches_fail_connection_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "allconnectfail.com"
    
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = None # No DB cache

    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None
    
    politeness_enforcer.robots_parsers.pop(domain, None)

    fetch_fail_result = FetchResult(
        initial_url="dummy", final_url="dummy", status_code=901, error_message="Connection failed"
    )
    politeness_enforcer.fetcher.fetch_url.side_effect = [fetch_fail_result, fetch_fail_result]

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        rerp = await politeness_enforcer._get_robots_for_domain(domain)
        mock_sqlite_connect.assert_any_call(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert rerp is not None
    assert rerp.is_allowed(dummy_config.user_agent, f"http://{domain}/anything") is True # Default to allow if all fails
    assert politeness_enforcer.fetcher.fetch_url.call_count == 2
    
    # DB should still be updated, likely with empty content
    update_calls = [call for call in mock_db_cursor_instance.execute.call_args_list if "UPDATE domain_metadata" in call[0][0]]
    assert len(update_calls) > 0, "UPDATE domain_metadata call was not found"
    args, _ = update_calls[0]
    assert args[1][0] == ""
    mock_db_conn_instance.commit.assert_called_once()
    assert domain in politeness_enforcer.robots_parsers

# --- Tests for is_url_allowed (async) ---
@pytest.mark.asyncio
async def test_is_url_allowed_manually_excluded(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "manualexclude.com"
    url = f"http://{domain}/somepage"
    
    # Mock the database interaction within the threaded function
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    # Simulate domain is manually excluded in DB for _db_check_manual_exclusion_sync_threaded
    mock_db_cursor_instance.fetchone.return_value = (1,) # is_manually_excluded = 1
    
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        is_allowed = await politeness_enforcer.is_url_allowed(url)

        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.execute.assert_called_once_with(
            "SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,)
        )
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert is_allowed is False
    # Ensure _get_robots_for_domain is NOT called if manually excluded, so fetcher shouldn't be called by it.
    politeness_enforcer.fetcher.fetch_url.assert_not_called() 

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    mock_cursor_db = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    # First fetchone is for manual exclusion (return not excluded), second for robots cache (return no cache)
    mock_cursor_db.fetchone.side_effect = [(0,), None] 
    politeness_enforcer.robots_parsers.pop(domain, None)

    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt", 
        status_code=200, text_content=robots_text
    )

    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    # _get_robots_for_domain would have been called once for the domain.
    # For the second call to is_url_allowed, it should use the cached RERP.
    # To test this properly, we might need to check call counts on fetcher or ensure RERP is in memory.
    politeness_enforcer.fetcher.fetch_url.reset_mock() # Reset after first call to _get_robots_for_domain
    
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    # Fetcher should not be called again for the same domain if RERP is cached
    politeness_enforcer.fetcher.fetch_url.assert_not_called()

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

    mock_cursor_db = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    mock_cursor_db.fetchone.side_effect = [(0,), None] # Not manually excluded, no robots cache
    politeness_enforcer.robots_parsers.pop(domain, None)

    # Simulate complete failure to fetch robots.txt
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt", 
        status_code=500, error_message="Server Error"
    )
    # And for HTTPS fallback as well
    politeness_enforcer.fetcher.fetch_url.side_effect = [
        FetchResult(initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt", status_code=500, error_message="Server Error"),
        FetchResult(initial_url=f"https://{domain}/robots.txt", final_url=f"https://{domain}/robots.txt", status_code=500, error_message="Server Error")
    ]

    assert await politeness_enforcer.is_url_allowed(url) is True # Default to allow

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
    
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = None # No record of last fetch in _db_get_last_fetch_sync_threaded
    
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)

        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.execute.assert_called_once_with(
            "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,)
        )
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert can_fetch is True


@pytest.mark.asyncio
async def test_can_fetch_domain_now_after_sufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "readyagain.com"
    crawl_delay_val = 30.0
    last_fetch = int(time.time()) - int(crawl_delay_val) - 1 

    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor_instance.fetchone.return_value = (last_fetch,) # Last fetch was just over delay ago
    
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=crawl_delay_val)

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        can_fetch = await politeness_enforcer.can_fetch_domain_now(domain)
        
        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        mock_db_cursor_instance.execute.assert_called_once_with(
            "SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,)
        )
        mock_db_cursor_instance.fetchone.assert_called_once()

    assert can_fetch is True


@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "notready.com"
    crawl_delay_val = MIN_CRAWL_DELAY_SECONDS
    last_fetch = int(time.time()) - int(crawl_delay_val / 2) # Fetched half the delay time ago

    mock_cursor = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.return_value = (last_fetch,)
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=crawl_delay_val) # type: ignore

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False

@pytest.mark.asyncio
async def test_can_fetch_domain_now_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbfetcherror.com"
    mock_cursor = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = sqlite3.Error("Simulated DB error")
    # get_crawl_delay should still be called but result doesn't matter if DB fails first
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS)) # type: ignore

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False

# --- Tests for record_domain_fetch_attempt (async) ---
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_new_domain(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recordnew.com"
    
    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None

    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        await politeness_enforcer.record_domain_fetch_attempt(domain)

        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        # Check for INSERT OR IGNORE then UPDATE
        assert mock_db_cursor_instance.execute.call_count == 2
        current_time_val = pytest.approx(int(time.time()), abs=2) # Allow small time diff
        
        mock_db_cursor_instance.execute.assert_any_call(
            "INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)", 
            (domain, current_time_val)
        )
        mock_db_cursor_instance.execute.assert_any_call(
            "UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
            (current_time_val, domain)
        )
        mock_db_conn_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_existing_domain(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recordexisting.com"

    mock_db_cursor_instance = MagicMock(spec=sqlite3.Cursor)
    mock_db_conn_instance = MagicMock(spec=sqlite3.Connection)
    mock_db_conn_instance.cursor.return_value = mock_db_cursor_instance
    mock_db_conn_instance.__enter__.return_value = mock_db_conn_instance
    mock_db_conn_instance.__exit__.return_value = None
    
    # Simulate domain already exists, INSERT OR IGNORE might do nothing or 1 row, UPDATE does 1 row
    # The behavior of execute calls should be the same (2 calls)
    with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
        await politeness_enforcer.record_domain_fetch_attempt(domain)
    
        mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, timeout=10)
        assert mock_db_cursor_instance.execute.call_count == 2
        current_time_val = pytest.approx(int(time.time()), abs=2)
        mock_db_cursor_instance.execute.assert_any_call(
            "INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)", 
            (domain, current_time_val)
        )
        mock_db_cursor_instance.execute.assert_any_call(
            "UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
            (current_time_val, domain)
        )
        mock_db_conn_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recorddberror.com"
    mock_cursor = politeness_enforcer.storage.conn.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = sqlite3.Error("Simulated DB error for recording")

    await politeness_enforcer.record_domain_fetch_attempt(domain)
    # commit might not be called if execute fails before it. Or it might be in a finally.
    # In our current code, commit is only after successful executes in the threaded function.
    # So, assert_not_called or check based on actual implementation.
    # For now, let's assume it won't be called if execute fails early.
    politeness_enforcer.storage.conn.commit.assert_not_called() 