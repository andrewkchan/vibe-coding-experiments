#!/usr/bin/env python3
"""
Migration script to transfer data from SQLite to PostgreSQL.
Usage: python migrate_to_postgresql.py --sqlite-db path/to/crawler_state.db --pg-url postgresql://user:pass@host/dbname
"""

import argparse
import sqlite3
import asyncio
import logging
from pathlib import Path
import sys

# Add the crawler module to path
sys.path.insert(0, str(Path(__file__).parent))

from crawler_module.db_backends import create_backend

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def migrate_table(sqlite_conn, pg_backend, table_name, batch_size=1000):
    """Migrate a single table from SQLite to PostgreSQL."""
    cursor = sqlite_conn.cursor()
    
    try:
        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        logger.info(f"Migrating {total_rows} rows from {table_name}")
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        placeholders = ', '.join(['?' for _ in columns])
        column_list = ', '.join(columns)
        
        # Migrate in batches
        offset = 0
        migrated = 0
        
        while offset < total_rows:
            cursor.execute(f"SELECT {column_list} FROM {table_name} LIMIT ? OFFSET ?", (batch_size, offset))
            rows = cursor.fetchall()
            
            if not rows:
                break
            
            # Insert into PostgreSQL
            insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
            await pg_backend.execute_many(insert_query, rows)
            
            migrated += len(rows)
            offset += batch_size
            logger.info(f"  Migrated {migrated}/{total_rows} rows from {table_name}")
        
        logger.info(f"Successfully migrated {table_name}")
        
    except Exception as e:
        logger.error(f"Error migrating {table_name}: {e}")
        raise
    finally:
        cursor.close()

async def main():
    parser = argparse.ArgumentParser(description="Migrate crawler data from SQLite to PostgreSQL")
    parser.add_argument("--sqlite-db", required=True, help="Path to SQLite database file")
    parser.add_argument("--pg-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for migration (default: 1000)")
    
    args = parser.parse_args()
    
    if not Path(args.sqlite_db).exists():
        logger.error(f"SQLite database not found: {args.sqlite_db}")
        return 1
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(args.sqlite_db)
    sqlite_conn.row_factory = sqlite3.Row
    
    # Create PostgreSQL backend
    pg_backend = create_backend('postgresql', db_url=args.pg_url)
    
    try:
        # Initialize PostgreSQL connection
        await pg_backend.initialize()
        logger.info("Connected to PostgreSQL")
        
        # Create schema in PostgreSQL
        logger.info("Creating schema in PostgreSQL...")
        
        # Create tables (PostgreSQL version)
        await pg_backend.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)
        """)
        
        await pg_backend.execute("""
            CREATE TABLE IF NOT EXISTS frontier (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                depth INTEGER DEFAULT 0,
                added_timestamp BIGINT NOT NULL,
                priority_score REAL DEFAULT 0,
                claimed_at BIGINT DEFAULT NULL
            )
        """)
        
        await pg_backend.execute("""
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
        """)
        
        await pg_backend.execute("""
            CREATE TABLE IF NOT EXISTS domain_metadata (
                domain TEXT PRIMARY KEY,
                last_scheduled_fetch_timestamp BIGINT DEFAULT 0,
                robots_txt_content TEXT,
                robots_txt_fetched_timestamp BIGINT,
                robots_txt_expires_timestamp BIGINT,
                is_manually_excluded INTEGER DEFAULT 0,
                is_seeded INTEGER DEFAULT 0
            )
        """)
        
        # Create indexes
        logger.info("Creating indexes...")
        await pg_backend.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
        await pg_backend.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")
        await pg_backend.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed ON frontier (claimed_at)")
        await pg_backend.execute("CREATE INDEX IF NOT EXISTS idx_visited_domain ON visited_urls (domain)")
        
        # Migrate data
        logger.info("Starting data migration...")
        
        # Order matters due to foreign keys
        tables = ['schema_version', 'domain_metadata', 'visited_urls', 'frontier']
        
        for table in tables:
            # Check if table exists in SQLite
            cursor = sqlite_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if cursor.fetchone():
                await migrate_table(sqlite_conn, pg_backend, table, args.batch_size)
            else:
                logger.warning(f"Table {table} not found in SQLite database, skipping")
            cursor.close()
        
        logger.info("Migration completed successfully!")
        
        # Show summary
        cursor = sqlite_conn.cursor()
        logger.info("\nMigration Summary:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0] if cursor.fetchone() else 0
            logger.info(f"  {table}: {count} rows")
        cursor.close()
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1
    finally:
        sqlite_conn.close()
        await pg_backend.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 