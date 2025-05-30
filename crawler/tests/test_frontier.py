import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock # Added AsyncMock
import sqlite3

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer # Added
from crawler_module.fetcher import Fetcher # Added for PolitenessEnforcer mock typing
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation in specific tests

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
    data_dir: Path
    seed_file: Path
    email: str = "frontier_test@example.com"
    exclude_file: Path | None = None
    max_workers: int = 1 # Keep low for predictable test flow
    max_pages: int | None = None
    max_duration: int | None = None
    log_level: str = "DEBUG"
    resume: bool = False
    user_agent: str = "FrontierTestCrawler/1.0"
    seeded_urls_only: bool = False
@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
    test_data_dir = tmp_path / "test_crawler_data_frontier"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for frontier: {test_data_dir}")
    return test_data_dir

@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
    """Provides the FrontierTestConfig object, seeds file created here."""
    seed_file_path = temp_test_frontier_dir / "test_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("http://example.com/seed1\n")
        sf.write("http://example.org/seed2\n")
        sf.write("http://example.com/seed1\n") # Duplicate to test seen
    return FrontierTestConfig(data_dir=temp_test_frontier_dir, seed_file=seed_file_path)

@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
    """Provides the actual CrawlerConfig based on FrontierTestConfig."""
    return CrawlerConfig(**vars(frontier_test_config_obj))

@pytest_asyncio.fixture
async def storage_manager_for_frontier(actual_config_for_frontier: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    sm = StorageManager(config=actual_config_for_frontier, db_pool=db_pool)
    yield sm

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier(actual_config_for_frontier: CrawlerConfig, 
                                          mock_storage_manager: MagicMock, 
                                          mock_fetcher: MagicMock) -> MagicMock:
    """Provides a mocked PolitenessEnforcer for FrontierManager tests."""
    # mock_pe = MagicMock(spec=PolitenessEnforcer)
    # If PolitenessEnforcer constructor does things (like _load_manual_exclusions), 
    # we might need to mock those if they interfere, or use a real PE with mocked sub-components.
    # For now, let's try a real PE with mocked fetcher and storage for its init, then mock its methods.
    
    # Create a basic mock storage for PE's init if it needs one.
    # The one passed to FrontierManager (storage_manager_for_frontier) is real for its tests.
    # This can be tricky. Let's assume PE init is tested elsewhere and mock its methods directly.
    mock_pe = AsyncMock(spec=PolitenessEnforcer)
    
    # Default mock behaviors for permissive testing of FrontierManager
    mock_pe.is_url_allowed = AsyncMock(return_value=True)
    mock_pe.can_fetch_domain_now = AsyncMock(return_value=True)
    mock_pe.record_domain_fetch_attempt = AsyncMock()
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0) # So it doesn't delay tests
    return mock_pe

# Fixture for a mock fetcher (can be shared or defined per test file)
@pytest.fixture
def mock_fetcher() -> MagicMock:
    return AsyncMock(spec=Fetcher)

@pytest.fixture
def mock_storage_manager() -> MagicMock: # This mock is for PolitenessEnforcer, may not need db_pool if methods are mocked
    mock = MagicMock(spec=StorageManager)
    # If PolitenessEnforcer's init accesses db_pool on storage, mock that attribute
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool) 
    # Mocking individual connection/cursor calls might be too complex if PE uses the pool directly.
    # It's better if PE methods that interact with DB are mocked if PE itself isn't the focus.
    # For now, adding db_pool attribute to the mock.
    return mock

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig, 
    storage_manager_for_frontier: StorageManager, # This SM instance now uses the db_pool fixture
    mock_politeness_enforcer_for_frontier: MagicMock
) -> FrontierManager:
    fm = FrontierManager(
        config=actual_config_for_frontier, 
        storage=storage_manager_for_frontier, 
        politeness=mock_politeness_enforcer_for_frontier
    )
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing Frontier Initialization (New Crawl)")
    # Ensure is_url_allowed is permissive during seed loading
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True 
    
    await frontier_manager.initialize_frontier() 
    # count_frontier is synchronous but uses DB; wrap for asyncio test
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 2 
    logger.info("Frontier initialization (new) test passed.")
    # Check that is_url_allowed was called for seeds
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count >= 2 

@pytest.mark.asyncio
async def test_add_and_get_urls(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing Add and Get URLs from Frontier")
    # Permissive politeness for initialization and adding
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True

    await frontier_manager.initialize_frontier() 
    initial_count = await asyncio.to_thread(frontier_manager.count_frontier)
    assert initial_count == 2

    # Test adding a new URL
    await frontier_manager.add_url("http://test.com/page1")
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 3
    # is_url_allowed should be called by add_url
    # Initial calls for seeds + 1 for this add_url
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count >= 3 

    # Try adding a duplicate of a seed - should not increase count, is_url_allowed may or may not be called based on seen_urls check order
    # Reset call count for is_url_allowed before this specific check if needed, or check total count carefully.
    current_is_allowed_calls = mock_politeness_enforcer_for_frontier.is_url_allowed.call_count
    await frontier_manager.add_url("http://example.com/seed1")
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 3
    # If URL is in seen_urls, is_url_allowed might not be called. This depends on FrontierManager's internal logic order.
    # Let's assume for now it might be called or not, so we don't assert exact count here strictly.

    # Get URLs
    # For get_next_url, ensure politeness checks are permissive for testing FIFO logic here
    mock_politeness_enforcer_for_frontier.is_url_allowed.reset_mock(return_value=True) # Reset and keep permissive
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.reset_mock(return_value=True)
    mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.reset_mock()

    expected_order = ["http://example.com/seed1", "http://example.org/seed2", "http://test.com/page1"]
    retrieved_urls = []

    for i in range(len(expected_order)):
        next_url_info = await frontier_manager.get_next_url()
        assert next_url_info is not None, f"Expected URL, got None at iteration {i}"
        retrieved_urls.append(next_url_info[0])
        # Politeness checks for get_next_url
        # is_url_allowed is called for each candidate fetched from DB before can_fetch_domain_now
        # can_fetch_domain_now is called if is_url_allowed passes
        # record_domain_fetch_attempt is called if both pass
        assert mock_politeness_enforcer_for_frontier.is_url_allowed.called
        assert mock_politeness_enforcer_for_frontier.can_fetch_domain_now.called
        assert mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.called
        # Reset mocks for next iteration if asserting per-iteration calls
        mock_politeness_enforcer_for_frontier.is_url_allowed.reset_mock(return_value=True)
        mock_politeness_enforcer_for_frontier.can_fetch_domain_now.reset_mock(return_value=True)
        mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.reset_mock()

    assert retrieved_urls == expected_order
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 0

    next_url_info = await frontier_manager.get_next_url()
    assert next_url_info is None, "Frontier should be empty"
    logger.info("Add and get URLs test passed with politeness mocks.")

@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path, 
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    # tmp_path is needed to create distinct DBs for each run if not using a shared fixture pool
    tmp_path: Path 
):
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.return_value = None # AsyncMock should return None by default

    # Define a unique DB file for this multi-run test
    # to ensure state isolation between "runs" if we create separate pools.
    # Using the `temp_test_frontier_dir` for consistency in paths for data_dir.
    db_file_for_resume_test = temp_test_frontier_dir / "resume_test_frontier.db"


    # --- First run: populate and close ---\n    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False # Ensure it's a new run
    # Ensure data_dir points to where db_file_for_resume_test will be (or its parent)
    # The frontier_test_config_obj already uses temp_test_frontier_dir as its data_dir
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    pool_run1 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run1 = StorageManager(config=cfg_run1, db_pool=pool_run1)
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run1.initialize_frontier() 
    await frontier_run1.add_url("http://persistent.com/page_from_run1")
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 3
    
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 2 
    # storage_run1.close() # Not needed, pool_run1.close_all() will handle it
    pool_run1.close_all()

    # --- Second run: resume --- 
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict) # Same data_dir, db will be reused

    pool_run2 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run2 = StorageManager(config=cfg_run2, db_pool=pool_run2)
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run2.initialize_frontier() 
    
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    # Order depends on timestamps; assuming example.org/seed2 was added after example.com/seed1 was taken
    # and persistent.com after that. So, example.org/seed2 should be next if it was second seed.
    # Original seeds: example.com/seed1, example.org/seed2. First get_next_url got seed1.
    # So, example.org/seed2 should be next.
    assert next_url[0] == "http://example.org/seed2" 

    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    assert next_url[0] == "http://persistent.com/page_from_run1"

    assert await asyncio.to_thread(frontier_run2.count_frontier) == 0
    # storage_run2.close() # Not needed
    pool_run2.close_all()
    logger.info("Frontier resume test passed with politeness mocks.") 