import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
    data_dir: Path
    seed_file: Path
    email: str = "frontier_test@example.com"
    exclude_file: Path | None = None
    max_workers: int = 1
    max_pages: int | None = None
    max_duration: int | None = None
    log_level: str = "DEBUG"
    resume: bool = False
    user_agent: str = "FrontierTestCrawler/1.0"

@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
    test_data_dir = tmp_path / "test_crawler_data_frontier"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for frontier: {test_data_dir}")
    return test_data_dir

@pytest_asyncio.fixture
async def frontier_test_config(temp_test_frontier_dir: Path) -> FrontierTestConfig:
    seed_file_path = temp_test_frontier_dir / "test_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("http://example.com/seed1\n")
        sf.write("http://example.org/seed2\n")
        sf.write("http://example.com/seed1\n") # Duplicate to test seen
    return FrontierTestConfig(data_dir=temp_test_frontier_dir, seed_file=seed_file_path)

@pytest_asyncio.fixture
async def storage_manager_for_frontier(frontier_test_config: FrontierTestConfig) -> StorageManager:
    # Cast FrontierTestConfig to CrawlerConfig for StorageManager compatibility
    # This is okay if FrontierTestConfig is a superset or identical in relevant fields
    sm_config = CrawlerConfig(**vars(frontier_test_config))
    sm = StorageManager(config=sm_config)
    yield sm
    sm.close()
    # Cleanup handled by tmp_path for the directory
    if frontier_test_config.seed_file.exists():
        frontier_test_config.seed_file.unlink()

@pytest_asyncio.fixture
async def frontier_manager(frontier_test_config: FrontierTestConfig, storage_manager_for_frontier: StorageManager) -> FrontierManager:
    # Cast FrontierTestConfig to CrawlerConfig for FrontierManager compatibility
    fm_config = CrawlerConfig(**vars(frontier_test_config))
    fm = FrontierManager(config=fm_config, storage=storage_manager_for_frontier)
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager):
    logger.info("Testing Frontier Initialization (New Crawl)")
    await frontier_manager.initialize_frontier() 
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 2 # Expect 2 unique seeds
    logger.info("Frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_add_and_get_urls(frontier_manager: FrontierManager):
    logger.info("Testing Add and Get URLs from Frontier")
    await frontier_manager.initialize_frontier() # Start with seeds
    initial_count = await asyncio.to_thread(frontier_manager.count_frontier)
    assert initial_count == 2

    await frontier_manager.add_url("http://test.com/page1")
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 3
    
    # Try adding a duplicate of a seed - should not increase count
    await frontier_manager.add_url("http://example.com/seed1")
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 3

    # Get URLs in expected order (FIFO based on current simple implementation)
    next_url_info = await frontier_manager.get_next_url()
    assert next_url_info is not None
    assert next_url_info[0] == "http://example.com/seed1"
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 2

    next_url_info = await frontier_manager.get_next_url()
    assert next_url_info is not None
    assert next_url_info[0] == "http://example.org/seed2"
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 1

    next_url_info = await frontier_manager.get_next_url()
    assert next_url_info is not None
    assert next_url_info[0] == "http://test.com/page1"
    assert await asyncio.to_thread(frontier_manager.count_frontier) == 0

    next_url_info = await frontier_manager.get_next_url()
    assert next_url_info is None, "Frontier should be empty"
    logger.info("Add and get URLs test passed.")

@pytest.mark.asyncio
async def test_frontier_resume(temp_test_frontier_dir: Path, frontier_test_config: FrontierTestConfig):
    logger.info("Testing Frontier Resume Functionality")
    # --- First run: populate and close ---
    cfg_run1 = CrawlerConfig(**vars(frontier_test_config)) # Use the original config for run1
    storage_run1 = StorageManager(config=cfg_run1)
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1)
    await frontier_run1.initialize_frontier() # Loads initial 2 seeds
    await frontier_run1.add_url("http://persistent.com/page_from_run1")
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 3
    url_to_retrieve = await frontier_run1.get_next_url() # Retrieve one to change state
    assert url_to_retrieve is not None
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 2 # 2 left
    storage_run1.close()

    # --- Second run: resume --- 
    # Update config for resume=True, ensure it uses the same data_dir
    resume_config_dict = vars(frontier_test_config).copy()
    resume_config_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**resume_config_dict)

    storage_run2 = StorageManager(config=cfg_run2)
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2)
    await frontier_run2.initialize_frontier() # Should load existing state
    
    # Check count of URLs remaining from run1
    # It should load the 2 URLs that were left in the frontier table.
    # The initialize_frontier also populates seen_urls from visited and frontier tables.
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    assert next_url[0] == "http://example.org/seed2" # Assuming FIFO from original seeds

    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    assert next_url[0] == "http://persistent.com/page_from_run1"

    assert await asyncio.to_thread(frontier_run2.count_frontier) == 0
    storage_run2.close()
    logger.info("Frontier resume test passed.") 