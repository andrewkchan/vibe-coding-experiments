# Implement redis version of StorageManager
_Exported on 6/15/2025 at 19:55:13 PDT from Cursor (1.0.0)_

---

**User**

Let's continue our rearchitecture of the web crawler from PostgreSQL to Redis. See `redis_only_architecture.md` for the rearchitecture plan, and `PLAN.MD` + `README.md` for how the crawler currently works with the PostgreSQL backend. We are in phase 2 and previously completed rearchitecting the frontier and politeness enforcers. Next let's create a redis version of the StorageManager from storage.py (new class, not replacement) implementing the same public interface but with a redis backend to handle add_visited_page. Then let's port the tests in test_storage.py over to test_storage_redis.py.

Note the current version of StorageManager creates tables for the schema and does migrations. I think since Redis doesn't require you to explicitly create a schema, we don't need to do that for the data structures described in redis_only_architecture.md, but I think it will be valuable to store the schema version somewhere in case we do decide to change the schema later and want to resume from old data. The schema version for Redis should be different than the DB schema version.

---

**Cursor**

Looking at the requirements, I'll create a Redis-based StorageManager that implements the same interface as the current PostgreSQL/SQLite version. Let me start by implementing the RedisStorageManager class and then port the tests.

Now I'll create the Redis version of StorageManager and then port the tests. First, let me create the RedisStorageManager class:

```python
import logging
from pathlib import Path
import hashlib
import time
import json
from typing import Optional
import aiofiles
import redis.asyncio as redis

from .config import CrawlerConfig

logger = logging.getLogger(__name__)

# Redis schema version - separate from DB schema version
REDIS_SCHEMA_VERSION = 1

class RedisStorageManager:
    """Redis-based storage manager for visited URLs and content."""
    
    def __init__(self, config: CrawlerConfig, redis_client: redis.Redis):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.content_dir = self.data_dir / "content"
        self.redis = redis_client
        
        self._init_storage()
    
    def _init_storage(self):
        """Initializes the data directory and content directory."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise
    
    async def init_db_schema(self):
        """Initializes Redis schema version."""
        try:
            logger.info("Initializing Redis schema")
            # Check and set schema version
            current_version = await self.redis.get("schema_version")
            if current_version:
                current_version = int(current_version)
                if current_version != REDIS_SCHEMA_VERSION:
                    logger.warning(f"Redis schema version mismatch. Current: {current_version}, Expected: {REDIS_SCHEMA_VERSION}")
                    # In the future, handle migrations here
            else:
                # First time initialization
                await self.redis.set("schema_version", str(REDIS_SCHEMA_VERSION))
                logger.info(f"Set Redis schema version to {REDIS_SCHEMA_VERSION}")
        except Exception as e:
            logger.error(f"Error during Redis schema initialization: {e}")
            raise
    
    def get_url_sha256(self, url: str) -> str:
        """Generates a SHA256 hash for a given URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    async def save_content_to_file(self, url_hash: str, text_content: str) -> Optional[Path]:
        """Saves extracted text content to a file asynchronously."""
        if not text_content:  # Do not save if no text content
            return None
        
        file_path = self.content_dir / f"{url_hash}.txt"
        try:
            async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
                await f.write(text_content)
            logger.debug(f"Saved content for {url_hash} to {file_path}")
            return file_path
        except IOError as e:
            logger.error(f"IOError saving content file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving content file {file_path}: {e}", exc_info=True)
            return None
    
    async def add_visited_page(
        self, 
        url: str, 
        domain: str,
        status_code: int, 
        crawled_timestamp: int,
        content_type: Optional[str] = None, 
        content_text: Optional[str] = None, 
        content_storage_path_str: Optional[str] = None, 
        redirected_to_url: Optional[str] = None
    ):
        """Adds a record of a visited page to Redis."""
        url_sha256 = self.get_url_sha256(url)
        # Use first 16 chars of SHA256 for the key (as per architecture doc)
        url_hash = url_sha256[:16]
        
        content_hash: Optional[str] = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()
        
        try:
            # Prepare the hash data
            visited_data = {
                'url': url,
                'url_sha256': url_sha256,  # Store full hash for compatibility
                'domain': domain,
                'status_code': str(status_code),
                'fetched_at': str(crawled_timestamp),
                'content_path': content_storage_path_str or '',
                'error': ''  # No error if we're adding a visited page
            }
            
            # Add optional fields if present
            if content_type:
                visited_data['content_type'] = content_type
            if content_hash:
                visited_data['content_hash'] = content_hash
            if redirected_to_url:
                visited_data['redirected_to_url'] = redirected_to_url
            
            # Store in Redis using pipeline for atomicity
            pipe = self.redis.pipeline()
            
            # Store visited metadata in hash
            pipe.hset(f'visited:{url_hash}', mapping=visited_data)
            
            # Add to time-sorted index
            pipe.zadd('visited:by_time', {url_hash: crawled_timestamp})
            
            # Mark URL as seen in bloom filter (if it exists)
            # Note: In production, the bloom filter should be created during migration
            # For now, we'll try to add and ignore if it doesn't exist
            pipe.execute_command('BF.ADD', 'seen:bloom', url, _target_nodes='primaries')
            
            await pipe.execute()
            
            logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
            
        except redis.ResponseError as e:
            if "not found" in str(e).lower():
                # Bloom filter doesn't exist, log warning but continue
                logger.warning(f"Bloom filter 'seen:bloom' not found. URL {url} not added to bloom filter.")
                # Still store the visited data without bloom filter
                await self.redis.hset(f'visited:{url_hash}', mapping=visited_data)
                await self.redis.zadd('visited:by_time', {url_hash: crawled_timestamp})
            else:
                logger.error(f"Redis error adding visited page {url}: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error adding visited page {url}: {e}")
            raise
    
    async def close(self):
        """Close Redis connection."""
        await self.redis.close()
```

```python
            # Mark URL as seen in bloom filter (if it exists)
            # Note: In production, the bloom filter should be created during migration
            # For now, we'll try to add and ignore if it doesn't exist
            pipe.execute_command('BF.ADD', 'seen:bloom', url, _target_nodes='primaries')
            
            await pipe.execute()

->

            # Mark URL as seen in bloom filter (if it exists)
            # Note: In production, the bloom filter should be created during migration
            # For now, we'll try to add and ignore if it doesn't exist
            pipe.execute_command('BF.ADD', 'seen:bloom', url)
            
            await pipe.execute()
```

Now let me create the test file for the Redis storage manager by porting the existing tests:

```python
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
    await client.close()


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
    async def mock_aio_open_raiser(*args, **kwargs):
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
```

Now let's run the tests to make sure they work correctly:

All the tests are passing! Let me fix the warnings that appeared:

```python
    # Cleanup after tests
    await client.flushdb()
    await client.close()

->

    # Cleanup after tests
    await client.flushdb()
    await client.aclose()
```

```python
    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)

->

    # Mock aiofiles.open to raise IOError
    def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
```

Let me run the tests again to confirm the warnings are fixed:

Excellent! All tests are passing cleanly. Let me also add the RedisStorageManager to the `__init__.py` file to make it easily importable:

```python
# Core crawler logic 

from .storage import StorageManager
from .redis_storage import RedisStorageManager

__all__ = ['StorageManager', 'RedisStorageManager']
```

## Summary

I've successfully created a Redis version of the StorageManager and ported all the tests. Here's what was accomplished:

### 1. **Created `redis_storage.py`** with:
- `RedisStorageManager` class that implements the same public interface as the original `StorageManager`
- Schema versioning for Redis (separate from DB schema version)
- Stores visited URL metadata in Redis hashes and sorted sets as per the architecture document
- Gracefully handles missing bloom filter (logs warning but continues)
- Same file-based content storage as the original

### 2. **Created `test_storage_redis.py`** with:
- All tests ported from `test_storage.py`
- A `redis_client` fixture that uses Redis database 15 for testing
- Tests adapted to verify data in Redis instead of SQL database
- All 10 tests passing successfully

### 3. **Key implementation details**:
- Uses first 16 characters of URL SHA256 hash as the Redis key (as per architecture doc)
- Stores all the same fields as the SQL version for compatibility
- Uses Redis pipeline for atomic operations
- Maintains both `visited:{url_hash}` hash and `visited:by_time` sorted set
- Schema version stored as `schema_version` key in Redis

### 4. **Updated `__init__.py`** to export both storage managers for easy importing

The RedisStorageManager is now ready to be integrated with the crawler. It provides the same interface as the original StorageManager, so the CrawlerOrchestrator can use it as a drop-in replacement when configured to use Redis backend.

The next steps in the migration plan would be:
- Update CrawlerOrchestrator to use the new Redis-based components (frontier, politeness, and storage)
- Implement the PostgreSQL to Redis migration script
- Add monitoring/metrics for the new system

