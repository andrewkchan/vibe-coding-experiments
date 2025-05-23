import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time
import asyncio # For to_thread if needed for DB, and for async file saving
import aiofiles # For async file operations

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

        with self.conn as conn: # Use context manager for cursor and commit/rollback
            cursor = conn.cursor()
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
                    # conn.commit() is handled by with statement
                    logger.info("Database tables created/updated successfully.")
                else:
                    logger.info(f"Database schema is up to date (version {current_version}).")

            except sqlite3.Error as e:
                logger.error(f"Error creating database tables: {e}")
                # conn.rollback() handled by with statement on error
                raise
            finally:
                cursor.close()

    def get_url_sha256(self, url: str) -> str:
        """Generates a SHA256 hash for a given URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    async def save_content_to_file(self, url_hash: str, text_content: str) -> Path | None:
        """Saves extracted text content to a file asynchronously."""
        if not text_content: # Do not save if no text content
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

    def add_visited_page(
        self, 
        url: str, 
        domain: str,
        status_code: int, 
        crawled_timestamp: int,
        content_type: str | None = None, 
        content_text: str | None = None, # Pass text to hash it
        content_storage_path_str: str | None = None, # String representation of Path
        redirected_to_url: str | None = None
    ):
        """Adds a record of a visited page to the database."""
        if not self.conn:
            logger.error("Cannot add visited page, no DB connection.")
            return

        url_sha256 = self.get_url_sha256(url)
        content_hash: str | None = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()

        try:
            # Use asyncio.to_thread for DB operations if this method becomes async
            # For now, assuming it's called from a context that can handle sync DB.
            with self.conn as conn: # Context manager for cursor and commit/rollback
                cursor = conn.cursor()
                cursor.execute("""
                INSERT OR REPLACE INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ))
            logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
        except sqlite3.Error as e:
            logger.error(f"DB error adding visited page {url}: {e}")
            # Rollback handled by with statement

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.") 