import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# pytest will automatically discover fixtures in conftest.py in the same directory or parent directories.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig # Will use test_config from conftest
from crawler_module.db_pool import SQLiteConnectionPool

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


@pytest.fixture
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=test_config, db_pool=db_pool)
    yield sm

def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    assert storage_manager.db_path == Path(db_pool.db_path), \
        f"SM DB path {storage_manager.db_path} should match pool DB path {Path(db_pool.db_path)}"
    
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # The storage_manager fixture already initializes the schema via StorageManager.__init__
    with db_pool as conn: # Use the pool's context manager to get a connection
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64 # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")

def test_storage_manager_reinitialization(test_config: CrawlerConfig, tmp_path: Path): # Uses test_config from conftest
    """Test that re-initializing StorageManager with the same path is safe."""
    
    # The standard DB path that StorageManager will use based on test_config
    target_db_path = Path(test_config.data_dir) / "crawler_state.db"

    pool_reinit1 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm1 = StorageManager(config=test_config, db_pool=pool_reinit1)
    # Verify StorageManager calculates its db_path correctly based on the config
    assert sm1.db_path == target_db_path, \
        f"SM1 path {sm1.db_path} should match target DB path {target_db_path}"
    pool_reinit1.close_all()

    pool_reinit2 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm2 = StorageManager(config=test_config, db_pool=pool_reinit2)
    assert sm2.db_path == target_db_path, \
        f"SM2 path {sm2.db_path} should match target DB path {target_db_path}"
    pool_reinit2.close_all()

    assert target_db_path.exists(), "Database file should exist after StorageManager initializations."
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
    assert saved_content == text_content
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
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")

def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool): 
    """Test adding a visited page record to the database."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    url_hash_for_file = storage_manager.get_url_sha256(url)
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, 
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    (db_url_sha256, db_url, db_domain, db_crawled_ts, db_status, 
     db_content_type, db_content_hash, db_storage_path, db_redirected_to) = row
    
    assert db_url_sha256 == expected_url_sha256
    assert db_url == url
    assert db_domain == domain
    assert db_crawled_ts == crawled_timestamp
    assert db_status == status_code
    assert db_content_type == content_type
    assert db_content_hash == expected_content_hash
    assert db_storage_path == content_storage_path_str
    assert db_redirected_to is None
    logger.info("Add visited page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    content_hash, content_storage_path, db_redirected_to = row
    assert content_hash is None
    assert content_storage_path is None
    assert db_redirected_to == redirected_to_url
    logger.info("Add visited page with no content test passed.") 