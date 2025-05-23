import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time

from .config import CrawlerConfig # Assuming config.py is in the same directory

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 1

class StorageManager:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.db_path = self.data_dir / "crawler_state.db"
        self.content_dir = self.data_dir / "content"
        self.conn = None

        self._init_storage()

    def _init_storage(self):
        """Initializes the data directory, content directory, and database."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise

        self._init_db()

    def _init_db(self):
        """Initializes the SQLite database and creates tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=10) # Increased timeout
            logger.info(f"Connected to database: {self.db_path}")
            self._create_tables()
        except sqlite3.Error as e:
            logger.error(f"Database error during initialization: {e}")
            if self.conn:
                self.conn.close()
            raise

    def _create_tables(self):
        """Creates the necessary tables in the database if they don't already exist."""
        if not self.conn:
            raise sqlite3.Error("Database connection not established.")

        cursor = self.conn.cursor()
        try:
            # Check current schema version
            cursor.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
            cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            row = cursor.fetchone()
            current_version = row[0] if row else 0

            if current_version < DB_SCHEMA_VERSION:
                logger.info(f"Database schema version is {current_version}. Upgrading to {DB_SCHEMA_VERSION}.")
                
                # Frontier Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS frontier (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT NOT NULL,
                    depth INTEGER DEFAULT 0,
                    added_timestamp INTEGER NOT NULL,
                    priority_score REAL DEFAULT 0
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")

                # Visited URLs Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url_sha256 TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    crawled_timestamp INTEGER NOT NULL,
                    http_status_code INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    content_storage_path TEXT,
                    redirected_to_url TEXT
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_visited_domain ON visited_urls (domain)")

                # Domain Metadata Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS domain_metadata (
                    domain TEXT PRIMARY KEY,
                    last_scheduled_fetch_timestamp INTEGER DEFAULT 0,
                    robots_txt_content TEXT,
                    robots_txt_fetched_timestamp INTEGER,
                    robots_txt_expires_timestamp INTEGER,
                    is_manually_excluded INTEGER DEFAULT 0
                )
                """)
                
                # Update schema version
                cursor.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
                self.conn.commit()
                logger.info("Database tables created/updated successfully.")
            else:
                logger.info(f"Database schema is up to date (version {current_version}).")

        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def get_url_sha256(self, url: str) -> str:
        """Generates a SHA256 hash for a given URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.") 