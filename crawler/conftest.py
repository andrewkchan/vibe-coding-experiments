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

# Import the config after sys.path is modified
from crawler_module.config import CrawlerConfig

# Add Redis imports
import redis.asyncio as redis

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory and is configured for Redis.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        fetcher_workers=1,
        parser_workers=1,
        num_fetcher_processes=1,
        num_parser_processes=1,
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
        seeded_urls_only=False,
        email="test@example.com",
        exclude_file=None,
        # Redis configuration for tests
        redis_host="localhost",
        redis_port=6379,
        redis_db=15,  # Use a dedicated test database
        redis_password=None
    )

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
    try:
        config = await client.config_get('databases')
        if not (config and int(config.get('databases', 1)) > 1):
            pytest.fail("Redis must be configured with multiple databases for safe testing.")
    except redis.ResponseError:
        # Some Redis versions might not support CONFIG GET databases
        pass
    
    # Clear test database before test
    await client.flushdb()
    
    yield client
    
    # Clean up after test
    await client.flushdb()
    await client.aclose() 