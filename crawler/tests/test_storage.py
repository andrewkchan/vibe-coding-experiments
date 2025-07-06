import pytest
import pytest_asyncio
from pathlib import Path
import logging
import time
import aiofiles
import redis.asyncio as redis
import asyncio

from crawler_module.storage import StorageManager, REDIS_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


# Note: redis_test_client fixture is imported from conftest.py
# This ensures we use db=15 for tests, not production db=0
@pytest_asyncio.fixture
async def storage_manager(test_config: CrawlerConfig, redis_test_client: redis.Redis) -> StorageManager:
    """Fixture to create a StorageManager instance."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir}")
    sm = StorageManager(config=test_config, redis_client=redis_test_client)
    await sm.init_db_schema()
    yield sm


@pytest.mark.asyncio
async def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, redis_test_client: redis.Redis):
    """Test if StorageManager initializes directories and Redis correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    
    # Check that schema version is set in Redis
    schema_version = await redis_test_client.get('schema_version')
    assert schema_version is not None, "Schema version should be set in Redis"
    assert int(schema_version) == REDIS_SCHEMA_VERSION, "Schema version in Redis should match"
    logger.info("Storage manager initialization test passed.")


@pytest.mark.asyncio
async def test_schema_initialization(storage_manager: StorageManager, redis_test_client: redis.Redis):
    """Test if schema version is properly set in Redis."""
    # The storage_manager fixture already initializes the schema
    schema_version = await redis_test_client.get('schema_version')
    assert schema_version is not None
    assert int(schema_version) == REDIS_SCHEMA_VERSION
    
    # Test that re-initializing is safe
    await storage_manager.init_db_schema()
    schema_version_after = await redis_test_client.get('schema_version')
    assert int(schema_version_after) == REDIS_SCHEMA_VERSION
    logger.info("Schema initialization test passed.")


def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")


@pytest.mark.asyncio
async def test_reinitialization_safety(test_config: CrawlerConfig, redis_test_client: redis.Redis):
    """Test that re-initializing StorageManager with the same Redis instance is safe."""
    
    sm1 = StorageManager(config=test_config, redis_client=redis_test_client)
    await sm1.init_db_schema()
    
    # Verify schema version is set
    schema_version = await redis_test_client.get("schema_version")
    assert schema_version == str(REDIS_SCHEMA_VERSION)
    
    sm2 = StorageManager(config=test_config, redis_client=redis_test_client)
    await sm2.init_db_schema()
    
    # Schema version should still be the same
    schema_version = await redis_test_client.get("schema_version")
    assert schema_version == str(REDIS_SCHEMA_VERSION)
    
    logger.info("StorageManager re-initialization test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager): 
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content[:len(text_content)] == text_content
    logger.info("Save content to file test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "This content should not be saved."

    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Disk is full")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")


@pytest.mark.asyncio
async def test_add_visited_page(storage_manager: StorageManager, redis_test_client: redis.Redis): 
    """Test adding a visited page record to Redis."""
    url = "http://example.com/visited_page"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    url_hash_for_file = storage_manager.get_url_sha256(url)
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    await storage_manager.add_visited_page(
        url=url,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_storage_path_str=content_storage_path_str
    )

    # Check data in Redis
    expected_url_sha256 = storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]

    # Get the visited page data from Redis
    visited_data = await redis_test_client.hgetall(f'visited:{expected_url_hash}')
    
    assert visited_data is not None
    assert visited_data['url'] == url
    assert int(visited_data['fetched_at']) == crawled_timestamp
    assert int(visited_data['status_code']) == status_code
    assert visited_data['content_type'] == content_type
    assert visited_data['content_path'] == content_storage_path_str
    
    logger.info("Add visited page test passed.")


@pytest.mark.asyncio
async def test_add_visited_page_no_content(storage_manager: StorageManager, redis_test_client: redis.Redis):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    status_code = 301
    crawled_timestamp = int(time.time())
    content_type = "text/html"

    await storage_manager.add_visited_page(
        url=url,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_storage_path_str=None
    )

    expected_url_sha256 = storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]
    
    visited_data = await redis_test_client.hgetall(f'visited:{expected_url_hash}')
    
    assert visited_data is not None
    assert visited_data['content_path'] == ''
    
    logger.info("Add visited page with no content test passed.")


@pytest.mark.asyncio
async def test_add_visited_page_update_existing(storage_manager: StorageManager, redis_test_client: redis.Redis):
    """Test updating an existing visited page in Redis."""
    url = "http://example.com/update_test"

    # First visit
    first_timestamp = int(time.time())
    await storage_manager.add_visited_page(
        url=url,
        status_code=404,
        crawled_timestamp=first_timestamp,
        content_type="text/html"
    )

    # Second visit (e.g., page is now available)
    await asyncio.sleep(0.01) # Ensure timestamp changes
    second_timestamp = first_timestamp + 100
    url_hash_for_file = storage_manager.get_url_sha256(url)
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    await storage_manager.add_visited_page(
        url=url,
        status_code=200,
        crawled_timestamp=second_timestamp,
        content_type="text/html",
        content_storage_path_str=content_storage_path_str
    )
    
    # Check that data was updated
    expected_url_sha256 = storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]
    
    visited_key = f"visited:{expected_url_hash}"
    stored_data = await redis_test_client.hgetall(visited_key)
    
    assert stored_data is not None
    assert int(stored_data['status_code']) == 200
    assert int(stored_data['fetched_at']) == second_timestamp
    assert stored_data['content_path'] == content_storage_path_str
    logger.info("Update visited page test passed.") 