import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
# Adjust the import path if CrawlerConfig is in a different location or make a simpler test double
from crawler_module.config import CrawlerConfig 

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data."""
    test_data_dir = tmp_path / "test_crawler_data_storage"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config(temp_test_dir: Path) -> CrawlerConfig:
    """Create a dummy CrawlerConfig for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds.txt"
    with open(seed_file_path, 'w') as f:
        f.write("http://example.com\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir,
        seed_file=seed_file_path,
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0"
    )
    logger.debug(f"Created dummy config with data_dir: {cfg.data_dir}")
    return cfg

@pytest.fixture
def storage_manager(dummy_config: CrawlerConfig) -> StorageManager:
    """Fixture to create and tear down a StorageManager instance."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config.data_dir}")
    sm = StorageManager(config=dummy_config)
    yield sm
    logger.debug("Closing StorageManager connection.")
    sm.close()
    # Optional: shutil.rmtree(dummy_config.data_dir) # tmp_path fixture handles cleanup

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config: CrawlerConfig):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert storage_manager.db_path.exists()
    assert storage_manager.conn is not None
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager):
    """Test if all tables and indexes are created as expected."""
    conn = storage_manager.conn
    assert conn is not None
    cursor = conn.cursor()

    tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
    for table in tables_to_check:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
        assert cursor.fetchone() is not None, f"Table {table} should exist."

    # Check schema version
    cursor.execute("SELECT version FROM schema_version")
    assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

    # Example: Check for an index
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
    assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
    assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
    
    cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url = "http://example.com/testpage"
    expected_hash = "a6f1491faf69SOMETESTHASH" # Not a real hash, just for structure
    # A real test would precompute the expected hash or use a known vector
    # For now, just test that it produces a string of expected length (64 for sha256 hex)
    actual_hash = storage_manager.get_url_sha256(url)
    assert isinstance(actual_hash, str)
    assert len(actual_hash) == 64
    logger.info("URL SHA256 generation test passed (structure check).")

# Test re-initialization (simulating resume or multiple starts)
def test_storage_manager_reinitialization(dummy_config: CrawlerConfig):
    """Test that re-initializing StorageManager with the same path is safe."""
    sm1 = StorageManager(config=dummy_config)
    db_path1 = sm1.db_path
    sm1.close()

    sm2 = StorageManager(config=dummy_config)
    db_path2 = sm2.db_path
    sm2.close()

    assert db_path1 == db_path2
    assert db_path1.exists()
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager, tmp_path: Path):
    """Test saving text content to a file asynchronously."""
    # storage_manager fixture already creates content_dir inside its own temp_test_dir
    # Ensure the content_dir used by the method is the one we expect from the fixture
    assert storage_manager.content_dir.exists()

    url_hash = "test_hash_123"
    test_text = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, test_text)
    assert file_path is not None
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir
    assert file_path.exists()

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == test_text

@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that empty content is not saved."""
    url_hash = "empty_content_hash"
    file_path = await storage_manager.save_content_to_file(url_hash, "")
    assert file_path is None
    assert not (storage_manager.content_dir / f"{url_hash}.txt").exists()

    file_path_none = await storage_manager.save_content_to_file(url_hash, None) # type: ignore
    assert file_path_none is None

@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    test_text = "Some content"

    # Mock aiofiles.open to raise an IOError
    async def mock_aio_open(*args, **kwargs):
        raise IOError("Simulated write error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open)

    file_path = await storage_manager.save_content_to_file(url_hash, test_text)
    assert file_path is None

def test_add_visited_page(storage_manager: StorageManager, dummy_config: CrawlerConfig):
    """Test adding a visited page record to the database."""
    url = "http://example.com/visitedpage"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    # Simulate saving content and getting a path
    # In a real scenario, this path would come from save_content_to_file
    url_hash_for_file = storage_manager.get_url_sha256(url)
    # Assume content_dir relative to data_dir for storage path consistency in DB
    relative_content_path = Path("content") / f"{url_hash_for_file}.txt"
    content_storage_path_str = str(relative_content_path)

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, # To generate content_hash
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    # Verify the record in the database
    conn = storage_manager.conn
    assert conn is not None
    cursor = conn.cursor()
    expected_url_sha256 = storage_manager.get_url_sha256(url)
    expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

    cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
    row = cursor.fetchone()
    cursor.close()

    assert row is not None
    assert row[0] == expected_url_sha256
    assert row[1] == url
    assert row[2] == domain
    assert row[3] == crawled_timestamp
    assert row[4] == status_code
    assert row[5] == content_type
    assert row[6] == expected_content_hash
    assert row[7] == content_storage_path_str
    assert row[8] is None # redirected_to_url

    logger.info("add_visited_page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        redirected_to_url=redirected_to
        # No content_type, content_text, or content_storage_path_str
    )

    conn = storage_manager.conn
    assert conn is not None
    cursor = conn.cursor()
    expected_url_sha256 = storage_manager.get_url_sha256(url)
    cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
    row = cursor.fetchone()
    cursor.close()

    assert row is not None
    assert row[0] is None # content_hash
    assert row[1] is None # content_storage_path
    assert row[2] == redirected_to
    logger.info("add_visited_page_no_content test passed.") 