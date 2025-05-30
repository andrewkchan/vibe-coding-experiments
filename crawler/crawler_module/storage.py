import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time
import asyncio # For to_thread if needed for DB, and for async file saving
import aiofiles # For async file operations

from .config import CrawlerConfig # Assuming config.py is in the same directory
from .db_pool import SQLiteConnectionPool

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 1

class StorageManager:
    def __init__(self, config: CrawlerConfig, db_pool: SQLiteConnectionPool):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.db_path = self.data_dir / "crawler_state.db"
        self.content_dir = self.data_dir / "content"
        self.db_pool = db_pool

        self._init_storage()

    def _init_storage(self):
        """Initializes the data directory, content directory, and database schema."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise

        self._init_db_schema()

    def _init_db_schema(self):
        """Initializes the SQLite database schema if it doesn't exist or needs upgrade."""
        # This method will now be called from the main thread (Orchestrator init)
        # or at least a context where it's okay to block for a short while.
        # It uses a connection from the pool.
        try:
            # Get a connection from the pool for schema initialization
            with self.db_pool as conn: # Use context manager for get/return
                logger.info(f"Initializing database schema using connection from pool: {self.db_path}")
                self._create_tables(conn)
        except sqlite3.Error as e:
            logger.error(f"Database error during schema initialization with pool: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to get connection from pool for schema init: {e}")
            raise


    def _create_tables(self, conn: sqlite3.Connection): # Takes a connection argument
        """Creates the necessary tables in the database if they don't already exist."""
        cursor = conn.cursor() # Create cursor from the provided connection
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
                    is_manually_excluded INTEGER DEFAULT 0,
                    is_seeded INTEGER DEFAULT 0
                )
                """)
                
                # Update schema version
                cursor.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
                conn.commit() # Commit changes
                logger.info("Database tables created/updated successfully.")
            else:
                logger.info(f"Database schema is up to date (version {current_version}).")

        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            try:
                conn.rollback() # Rollback on error
            except sqlite3.Error as re:
                logger.error(f"Error during rollback: {re}")
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
        content_text: str | None = None, 
        content_storage_path_str: str | None = None, 
        redirected_to_url: str | None = None
    ):
        """Adds a record of a visited page to the database using a pooled connection.
           This method is expected to be called via asyncio.to_thread if in an async context.
        """
        url_sha256 = self.get_url_sha256(url)
        content_hash: str | None = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()

        conn_from_pool: sqlite3.Connection | None = None
        try:
            # This method is run in a separate thread by asyncio.to_thread,
            # so it gets a connection from the pool.
            conn_from_pool = self.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("""
                INSERT OR REPLACE INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ))
                conn_from_pool.commit()
                logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
            except sqlite3.Error as e: # Inner try for cursor/execute specific errors
                logger.error(f"DB (execute/commit) error adding visited page {url}: {e}")
                if conn_from_pool: 
                    try:
                        conn_from_pool.rollback()
                    except sqlite3.Error as re:
                        logger.error(f"DB error during rollback for {url}: {re}")
                raise # Re-raise to be caught by outer block if necessary, or to signal failure
            finally:
                if cursor: cursor.close()
        except sqlite3.Error as e: # Outer try for connection pool errors or other sqlite errors
            logger.error(f"DB (pool/connection) error adding visited page {url}: {e}")
            # Rollback is handled by the inner block if the error was post-connection
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected error (e.g. pool issue) adding visited page {url}: {e}")
            raise
        finally:
            if conn_from_pool:
                self.db_pool.return_connection(conn_from_pool)