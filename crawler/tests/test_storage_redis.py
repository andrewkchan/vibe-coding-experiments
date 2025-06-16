import pytest
import pytest_asyncio
from pathlib import Path
import shutil
import logging
import aiofiles
import time
import hashlib
import redis.asyncio as redis
from typing import AsyncIterator

from crawler_module.redis_storage import RedisStorageManager, REDIS_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    """Fixture to create a Redis client for testing."""
    # Connect to Redis (assumes Redis is running locally)
    client = redis.Redis(host='localhost', port=6379, db=15, decode_responses=True)  # Use db 15 for tests
    
    # Clear the test database
    await client.flushdb()
    
    yield client
    
    # Cleanup after tests
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def redis_storage_manager(test_config: CrawlerConfig, redis_client: redis.Redis) -> RedisStorageManager:
    """Fixture to create a RedisStorageManager instance."""
    logger.debug(f"Initializing RedisStorageManager with config data_dir: {test_config.data_dir}")
    sm = RedisStorageManager(config=test_config, redis_client=redis_client)
    await sm.init_db_schema()
    yield sm


@pytest.mark.asyncio
async def test_redis_storage_manager_initialization(redis_storage_manager: RedisStorageManager, test_config: CrawlerConfig, redis_client: redis.Redis):
    """Test if RedisStorageManager initializes directories and Redis correctly."""
    assert redis_storage_manager.data_dir.exists()
    assert redis_storage_manager.content_dir.exists()
    
    # Check that schema version is set in Redis
    schema_version = await redis_client.get("schema_version")
    assert schema_version is not None
    assert int(schema_version) == REDIS_SCHEMA_VERSION
    
    logger.info("RedisStorageManager initialization test passed.")


@pytest.mark.asyncio
async def test_redis_schema_initialization(redis_storage_manager: RedisStorageManager, redis_client: redis.Redis):
    """Test if schema version is properly set in Redis."""
    # The redis_storage_manager fixture already initializes the schema
    
    schema_version = await redis_client.get("schema_version")
    assert schema_version == str(REDIS_SCHEMA_VERSION)
    
    logger.info("Redis schema initialization test passed.")


def test_get_url_sha256(redis_storage_manager: RedisStorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = redis_storage_manager.get_url_sha256(url1)
    hash2 = redis_storage_manager.get_url_sha256(url2)
    hash1_again = redis_storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")


@pytest.mark.asyncio
async def test_redis_storage_manager_reinitialization(test_config: CrawlerConfig, redis_client: redis.Redis):
    """Test that re-initializing RedisStorageManager with the same Redis instance is safe."""
    
    sm1 = RedisStorageManager(config=test_config, redis_client=redis_client)
    await sm1.init_db_schema()
    
    # Verify schema version is set
    schema_version = await redis_client.get("schema_version")
    assert schema_version == str(REDIS_SCHEMA_VERSION)
    
    sm2 = RedisStorageManager(config=test_config, redis_client=redis_client)
    await sm2.init_db_schema()
    
    # Schema version should still be the same
    schema_version = await redis_client.get("schema_version")
    assert schema_version == str(REDIS_SCHEMA_VERSION)
    
    logger.info("RedisStorageManager re-initialization test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file(redis_storage_manager: RedisStorageManager): 
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await redis_storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == redis_storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == text_content
    logger.info("Save content to file test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(redis_storage_manager: RedisStorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await redis_storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = redis_storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")


@pytest.mark.asyncio
async def test_save_content_to_file_io_error(redis_storage_manager: RedisStorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await redis_storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = redis_storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")


@pytest.mark.asyncio
async def test_add_visited_page(redis_storage_manager: RedisStorageManager, redis_client: redis.Redis): 
    """Test adding a visited page record to Redis."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    url_hash_for_file = redis_storage_manager.get_url_sha256(url)
    content_storage_path_str = str(redis_storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    await redis_storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, 
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    # Check data in Redis
    expected_url_sha256 = redis_storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]
    expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

    # Get the visited page data from Redis
    visited_data = await redis_client.hgetall(f'visited:{expected_url_hash}')
    
    assert visited_data is not None
    assert visited_data['url'] == url
    assert visited_data['url_sha256'] == expected_url_sha256
    assert visited_data['domain'] == domain
    assert int(visited_data['fetched_at']) == crawled_timestamp
    assert int(visited_data['status_code']) == status_code
    assert visited_data['content_type'] == content_type
    assert visited_data['content_hash'] == expected_content_hash
    assert visited_data['content_path'] == content_storage_path_str
    assert visited_data['error'] == ''
    assert 'redirected_to_url' not in visited_data  # Not set when None
    
    # Check time index
    score = await redis_client.zscore('visited:by_time', expected_url_hash)
    assert score is not None
    assert int(score) == crawled_timestamp
    
    logger.info("Add visited page test passed.")


@pytest.mark.asyncio
async def test_add_visited_page_no_content(redis_storage_manager: RedisStorageManager, redis_client: redis.Redis):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    await redis_storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    expected_url_sha256 = redis_storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]
    
    visited_data = await redis_client.hgetall(f'visited:{expected_url_hash}')
    
    assert visited_data is not None
    assert visited_data['content_path'] == ''
    assert 'content_hash' not in visited_data
    assert 'content_type' not in visited_data
    assert visited_data['redirected_to_url'] == redirected_to_url
    
    logger.info("Add visited page with no content test passed.")


@pytest.mark.asyncio
async def test_add_visited_page_update_existing(redis_storage_manager: RedisStorageManager, redis_client: redis.Redis):
    """Test updating an existing visited page in Redis."""
    url = "http://example.com/update_test"
    domain = "example.com"
    
    # First visit
    first_timestamp = int(time.time())
    await redis_storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=404,
        crawled_timestamp=first_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=None
    )
    
    # Second visit with different data
    second_timestamp = first_timestamp + 100
    text_content = "Now it has content!"
    url_hash_for_file = redis_storage_manager.get_url_sha256(url)
    content_storage_path_str = str(redis_storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    await redis_storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=200,
        crawled_timestamp=second_timestamp,
        content_type="text/html",
        content_text=text_content,
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )
    
    # Check that data was updated
    expected_url_sha256 = redis_storage_manager.get_url_sha256(url)
    expected_url_hash = expected_url_sha256[:16]
    
    visited_data = await redis_client.hgetall(f'visited:{expected_url_hash}')
    
    assert int(visited_data['status_code']) == 200
    assert int(visited_data['fetched_at']) == second_timestamp
    assert visited_data['content_type'] == "text/html"
    assert visited_data['content_path'] == content_storage_path_str
    
    # Check time index was updated
    score = await redis_client.zscore('visited:by_time', expected_url_hash)
    assert int(score) == second_timestamp
    
    logger.info("Update existing visited page test passed.") 