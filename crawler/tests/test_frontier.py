import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock # Added AsyncMock

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer # Added
from crawler_module.fetcher import Fetcher # Added for PolitenessEnforcer mock typing
from crawler_module.db_backends import create_backend # Use new database abstraction

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
    db_type: str = "sqlite"  # Use SQLite for tests
    db_url: str | None = None

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
async def db_backend(actual_config_for_frontier: CrawlerConfig):
    """Provides a database backend for tests."""
    backend = create_backend(
        'sqlite',
        db_path=actual_config_for_frontier.data_dir / "test_crawler_state.db",
        pool_size=1,
        timeout=10
    )
    await backend.initialize()
    yield backend
    await backend.close()

@pytest_asyncio.fixture
async def storage_manager_for_frontier(actual_config_for_frontier: CrawlerConfig, db_backend) -> StorageManager:
    sm = StorageManager(config=actual_config_for_frontier, db_backend=db_backend)
    await sm.init_db_schema()
    yield sm

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier(actual_config_for_frontier: CrawlerConfig, 
                                          mock_storage_manager: MagicMock, 
                                          mock_fetcher: MagicMock) -> MagicMock:
    """Provides a mocked PolitenessEnforcer for FrontierManager tests."""
    mock_pe = AsyncMock(spec=PolitenessEnforcer)
    
    # Default mock behaviors for permissive testing of FrontierManager
    mock_pe.is_url_allowed = AsyncMock(return_value=True)
    mock_pe.can_fetch_domain_now = AsyncMock(return_value=True)
    mock_pe.record_domain_fetch_attempt = AsyncMock()
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0) # So it doesn't delay tests
    mock_pe._load_manual_exclusions = AsyncMock()  # Mock the async initialization
    return mock_pe

# Fixture for a mock fetcher (can be shared or defined per test file)
@pytest.fixture
def mock_fetcher() -> MagicMock:
    return AsyncMock(spec=Fetcher)

@pytest.fixture
def mock_storage_manager() -> MagicMock: # This mock is for PolitenessEnforcer, may not need db if methods are mocked
    mock = MagicMock(spec=StorageManager)
    # Mock the db attribute with async methods
    mock.db = AsyncMock()
    return mock

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig, 
    storage_manager_for_frontier: StorageManager, # This SM instance now uses the db_backend fixture
    mock_politeness_enforcer_for_frontier: MagicMock
) -> FrontierManager:
    fm = FrontierManager(
        config=actual_config_for_frontier, 
        storage=storage_manager_for_frontier, 
        politeness=mock_politeness_enforcer_for_frontier
    )
    # Patch the batch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock()
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch) # Keep original behavior but allow asserts
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager):
    logger.info("Testing Frontier Initialization (New Crawl)")
    
    # In the new implementation, initialize_frontier calls _load_seeds,
    # which in turn calls the batch methods.
    await frontier_manager.initialize_frontier() 
    
    # Assert that the batch methods were called by _load_seeds
    frontier_manager._mark_domains_as_seeded_batch.assert_called_once()
    frontier_manager.add_urls_batch.assert_called_once()
    
    # Check the domains that were marked as seeded
    seeded_domains_call = frontier_manager._mark_domains_as_seeded_batch.call_args[0][0]
    assert set(seeded_domains_call) == {"example.com", "example.org"}

    # Check the URLs that were added to the frontier
    urls_added_call = frontier_manager.add_urls_batch.call_args[0][0]
    assert set(urls_added_call) == {"http://example.com/seed1", "http://example.org/seed2"}
    
    logger.info("Frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_add_urls_batch(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing batch adding of URLs to Frontier")
    
    # 1. Setup - Ensure frontier is empty and politeness is permissive
    await frontier_manager._clear_frontier_db()
    assert await frontier_manager.is_empty() is True
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True

    # 2. Add a batch of new URLs
    urls_to_add = ["http://test.com/batch1", "http://test.org/batch2", "http://test.com/batch1"]
    added_count = await frontier_manager.add_urls_batch(urls_to_add)

    # Assert that duplicates within the batch are handled and politeness was checked
    assert added_count == 2
    assert await frontier_manager.count_frontier() == 2
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2
    
    # 3. Add another batch, some new, some disallowed, some already seen
    mock_politeness_enforcer_for_frontier.is_url_allowed.reset_mock()
    
    # Make one URL disallowed
    def side_effect(url):
        if "disallowed" in url:
            return False
        return True
    mock_politeness_enforcer_for_frontier.is_url_allowed.side_effect = side_effect
    
    next_urls_to_add = ["http://new.com/page1", "http://test.org/batch2", "http://disallowed.com/page"]
    added_count_2 = await frontier_manager.add_urls_batch(next_urls_to_add)
    
    assert added_count_2 == 1 # Only new.com/page1 should be added
    assert await frontier_manager.count_frontier() == 3 # 2 from before + 1 new
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2 # disallowed.com and new.com (batch2 is in seen_urls)
    
    logger.info("Batch URL adding test passed.")

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
    db_file_for_resume_test = temp_test_frontier_dir / "resume_test_frontier.db"

    # --- First run: populate and close ---
    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False # Ensure it's a new run
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    backend_run1 = create_backend('sqlite', db_path=db_file_for_resume_test, pool_size=1)
    await backend_run1.initialize()
    
    storage_run1 = StorageManager(config=cfg_run1, db_backend=backend_run1)
    await storage_run1.init_db_schema()
    
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run1.initialize_frontier() 
    # Use batch add for the test
    await frontier_run1.add_urls_batch(["http://persistent.com/page_from_run1"])
    assert await frontier_run1.count_frontier() == 3
    
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await frontier_run1.count_frontier() == 2 
    await backend_run1.close()

    # --- Second run: resume --- 
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict)

    backend_run2 = create_backend('sqlite', db_path=db_file_for_resume_test, pool_size=1)
    await backend_run2.initialize()
    
    storage_run2 = StorageManager(config=cfg_run2, db_backend=backend_run2)
    await storage_run2.init_db_schema()
    
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run2.initialize_frontier() 
    
    assert await frontier_run2.count_frontier() == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    assert next_url[0] == "http://example.org/seed2" 

    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    assert next_url[0] == "http://persistent.com/page_from_run1"

    assert await frontier_run2.count_frontier() == 0
    await backend_run2.close()
    logger.info("Frontier resume test passed with politeness mocks.") 