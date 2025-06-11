import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time
import asyncio # For to_thread if needed for DB, and for async file saving
import aiofiles # For async file operations

from .config import CrawlerConfig # Assuming config.py is in the same directory
from .db_backends import DatabaseBackend, create_backend

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 7

class StorageManager:
    def __init__(self, config: CrawlerConfig, db_backend: DatabaseBackend):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.db_path = self.data_dir / "crawler_state.db" if config.db_type == "sqlite" else None
        self.content_dir = self.data_dir / "content"
        self.db = db_backend

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

    async def init_db_schema(self):
        """Initializes the database schema if it doesn't exist or needs upgrade."""
        try:
            logger.info(f"Initializing database schema for {self.config.db_type}")
            await self._create_tables()
        except Exception as e:
            logger.error(f"Database error during schema initialization: {e}")
            raise

    async def _create_tables(self):
        """Creates the necessary tables in the database if they don't already exist."""
        try:
            # Check current schema version
            await self.db.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)
            """, query_name="create_schema_version_table")
            
            row = await self.db.fetch_one("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1", query_name="get_schema_version")
            current_version = row[0] if row else 0

            if current_version < DB_SCHEMA_VERSION:
                logger.info(f"Database schema version is {current_version}. Upgrading to {DB_SCHEMA_VERSION}.")
                
                # Frontier Table - adjust for PostgreSQL/SQLite differences
                if self.config.db_type == "postgresql":
                    await self.db.execute("""
                    CREATE TABLE IF NOT EXISTS frontier (
                        id SERIAL PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        domain TEXT NOT NULL,
                        depth INTEGER DEFAULT 0,
                        added_timestamp BIGINT NOT NULL,
                        priority_score REAL DEFAULT 0,
                        claimed_at BIGINT DEFAULT NULL
                    )
                    """, query_name="create_frontier_table_pg")
                else:  # SQLite
                    await self.db.execute("""
                    CREATE TABLE IF NOT EXISTS frontier (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL,
                        domain TEXT NOT NULL,
                        depth INTEGER DEFAULT 0,
                        added_timestamp INTEGER NOT NULL,
                        priority_score REAL DEFAULT 0,
                        claimed_at INTEGER DEFAULT NULL
                    )
                    """, query_name="create_frontier_table_sqlite")
                
                # Create indexes
                await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)", query_name="create_idx_frontier_domain")
                await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)", query_name="create_idx_frontier_priority")
                
                # Add claimed_at column if upgrading from version 1 (SQLite only)
                if current_version == 1 and self.config.db_type == "sqlite":
                    try:
                        await self.db.execute("ALTER TABLE frontier ADD COLUMN claimed_at INTEGER DEFAULT NULL", query_name="alter_frontier_add_claimed_at")
                        logger.info("Added claimed_at column to frontier table")
                    except:
                        # Column might already exist
                        pass
                if self.config.db_type == "postgresql":
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_unclaimed_order_by_time ON frontier (added_timestamp ASC) WHERE claimed_at IS NULL", query_name="create_idx_frontier_unclaimed")
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed_at_for_expiry ON frontier (claimed_at) WHERE claimed_at IS NOT NULL", query_name="create_idx_frontier_claimed_at_expiry")
                    # New index for the implicit expiry logic (expired part of UNION ALL)
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_expired_order_by_time ON frontier (added_timestamp ASC) WHERE claimed_at IS NOT NULL", query_name="create_idx_frontier_expired")
                    # Index for domain-aware claiming to optimize JOIN with domain_metadata
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain_claimed_at ON frontier (domain, claimed_at, added_timestamp)", query_name="create_idx_frontier_domain_claimed")
                    # Index for domain sharding - index the hash value itself, not the modulo
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain_hash ON frontier ((hashtext(domain)::BIGINT), added_timestamp) WHERE claimed_at IS NULL", query_name="create_idx_frontier_domain_hash")
                    # Drop the old incorrect index if it exists
                    await self.db.execute("DROP INDEX IF EXISTS idx_frontier_domain_hash_shard", query_name="drop_old_domain_hash_shard_index")
                else: # SQLite
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed_at_added_timestamp_sqlite ON frontier (claimed_at, added_timestamp)", query_name="create_idx_frontier_claimed_at_added_ts")
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed_at_sqlite ON frontier (claimed_at)", query_name="create_idx_frontier_claimed_at")
                    # For SQLite, an index on added_timestamp would be beneficial for the parts of the UNION ALL equivalent.
                    # Note: SQLite doesn't have partial indexes on expressions like PG for claimed_at IS NOT NULL.
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_added_timestamp_sqlite ON frontier (added_timestamp)", query_name="create_idx_frontier_added_ts")

                # Visited URLs Table
                await self.db.execute("""
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url_sha256 TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    crawled_timestamp BIGINT NOT NULL,
                    http_status_code INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    content_storage_path TEXT,
                    redirected_to_url TEXT
                )
                """, query_name="create_visited_urls_table")
                await self.db.execute("CREATE INDEX IF NOT EXISTS idx_visited_domain ON visited_urls (domain)", query_name="create_idx_visited_domain")

                # Domain Metadata Table
                await self.db.execute("""
                CREATE TABLE IF NOT EXISTS domain_metadata (
                    domain TEXT PRIMARY KEY,
                    last_scheduled_fetch_timestamp BIGINT DEFAULT 0,
                    robots_txt_content TEXT,
                    robots_txt_fetched_timestamp BIGINT,
                    robots_txt_expires_timestamp BIGINT,
                    is_manually_excluded INTEGER DEFAULT 0,
                    is_seeded INTEGER DEFAULT 0
                )
                """, query_name="create_domain_metadata_table")
                
                # Update schema version
                if self.config.db_type == "postgresql":
                    await self.db.execute("INSERT INTO schema_version (version) VALUES (%s) ON CONFLICT DO NOTHING", (DB_SCHEMA_VERSION,), query_name="update_schema_version_pg")
                else:  # SQLite
                    await self.db.execute("INSERT OR IGNORE INTO schema_version (version) VALUES (?)", (DB_SCHEMA_VERSION,), query_name="update_schema_version_sqlite")
                logger.info("Database tables created/updated successfully.")
            else:
                logger.info(f"Database schema is up to date (version {current_version}).")

        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise

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

    async def add_visited_page(
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
        """Adds a record of a visited page to the database."""
        url_sha256 = self.get_url_sha256(url)
        content_hash: str | None = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()

        try:
            if self.config.db_type == "postgresql":
                await self.db.execute("""
                INSERT INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url_sha256) DO UPDATE SET
                    crawled_timestamp = excluded.crawled_timestamp,
                    http_status_code = excluded.http_status_code,
                    content_type = excluded.content_type,
                    content_hash = excluded.content_hash,
                    content_storage_path = excluded.content_storage_path,
                    redirected_to_url = excluded.redirected_to_url
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ), query_name="add_visited_page_pg")
            else:  # SQLite
                await self.db.execute("""
                INSERT OR REPLACE INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ), query_name="add_visited_page_sqlite")
            logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
        except Exception as e:
            logger.error(f"DB error adding visited page {url}: {e}")
            raise

    async def close(self):
        """Close database connections."""
        await self.db.close()