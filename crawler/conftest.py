import sys
import os
import pytest
import pytest_asyncio
from pathlib import Path
from typing import AsyncIterator

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import the database backend and storage after sys.path is modified
from crawler_module.db_backends import create_backend, DatabaseBackend
from crawler_module.config import CrawlerConfig # For a default config if needed
from crawler_module.storage import StorageManager

# Add Redis imports
import redis.asyncio as redis

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
        exclude_file=None,
        db_type="sqlite",  # Added - default to SQLite for tests
        db_url=None,  # Added - SQLite doesn't need a URL
        # Redis configuration - use default values
        redis_host="localhost",
        redis_port=6379,
        redis_db=15,  # Tests will override this via redis_test_client fixture
        redis_password=None
    )

@pytest_asyncio.fixture(scope="function")
async def db_backend(test_config: CrawlerConfig) -> AsyncIterator[DatabaseBackend]:
    """
    Pytest fixture for a DatabaseBackend.
    Creates the DB inside the test_config.data_dir.
    Ensures the backend is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the backend point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db"
    backend = create_backend('sqlite', db_path=db_file, pool_size=1)
    await backend.initialize()
    yield backend
    await backend.close()

@pytest_asyncio.fixture(scope="function")
async def storage_manager(test_config: CrawlerConfig, db_backend: DatabaseBackend) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    sm = StorageManager(config=test_config, db_backend=db_backend)
    await sm.init_db_schema()
    return sm

@pytest_asyncio.fixture(scope="function")
async def redis_test_client() -> AsyncIterator[redis.Redis]:
    """
    Pytest fixture for a Redis client that uses a separate test database.
    Uses db=15 to ensure tests never touch production data (db=0).
    
    IMPORTANT: This fixture clears the test database before and after each test!
    """
    # Use a different database number for tests (default is 0)
    TEST_REDIS_DB = 15
    
    client = redis.Redis(
        host='localhost', 
        port=6379, 
        db=TEST_REDIS_DB,  # Different database for tests!
        decode_responses=True
    )
    
    # Verify we're not on the production database
    assert await client.config_get('databases')['databases'] != '1', \
        "Redis must be configured with multiple databases for safe testing"
    
    # Clear test database before test
    await client.flushdb()
    
    yield client
    
    # Clean up after test
    await client.flushdb()
    await client.aclose()

@pytest.fixture(scope="function") 
def redis_test_config(test_config: CrawlerConfig) -> CrawlerConfig:
    """
    Returns a test config specifically for Redis-based crawler testing.
    """
    test_config.db_type = "redis"
    test_config.db_url = None  # Not used for Redis
    test_config.redis_db = 15  # Use test database
    return test_config 