import sys
import os
import pytest
from pathlib import Path
from typing import Iterator

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import the SQLiteConnectionPool after sys.path is modified
from crawler_module.db_pool import SQLiteConnectionPool
from crawler_module.config import CrawlerConfig # For a default config if needed
from crawler_module.storage import StorageManager

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        max_workers=1, # Keep low for tests unless testing concurrency
        log_level="DEBUG",
        # Add other necessary minimal config options
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
        seeded_urls_only=False,
        email="test@example.com",
        exclude_file=None
    )

@pytest.fixture(scope="function")
def db_pool(test_config: CrawlerConfig) -> Iterator[SQLiteConnectionPool]:
    """
    Pytest fixture for a SQLiteConnectionPool.
    Creates the DB inside the test_config.data_dir.
    Ensures the pool is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the pool point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db"
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True)
    yield pool
    pool.close_all()

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    return StorageManager(config=test_config, db_pool=db_pool) 