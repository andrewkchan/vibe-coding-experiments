import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass

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