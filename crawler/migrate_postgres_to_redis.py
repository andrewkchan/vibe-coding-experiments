#!/usr/bin/env python3
"""
Migrate crawler data from PostgreSQL to Redis + file-based hybrid storage.

Usage:
    python migrate_postgres_to_redis.py postgresql://user:pass@host:port/dbname
"""

import asyncio
import sys
import time
from pathlib import Path
import asyncpg
import redis.asyncio as redis
from datetime import datetime
import logging

# Add the crawler module to the path
sys.path.insert(0, str(Path(__file__).parent))

from crawler_module.config import CrawlerConfig
from crawler_module.frontier import HybridFrontierManager
from crawler_module.politeness import RedisPolitenessEnforcer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgreSQLToHybridMigrator:
    def __init__(self, pg_conn: asyncpg.Connection, redis_client: redis.Redis, frontier_manager: HybridFrontierManager):
        self.pg = pg_conn
        self.redis = redis_client
        self.fm = frontier_manager
        
    async def migrate(self):
        start_phase = 1
        """Migrate data from PostgreSQL to hybrid storage."""
        start_time = time.time()
        logger.info("Starting migration from PostgreSQL to Redis + files...")
        
        # Get counts for progress tracking
        domain_count = await self.pg.fetchval("SELECT COUNT(DISTINCT domain) FROM domain_metadata")
        frontier_count = await self.pg.fetchval("SELECT COUNT(*) FROM frontier")
        visited_count = await self.pg.fetchval("SELECT COUNT(*) FROM visited_urls")
        
        logger.info(f"Data to migrate: {domain_count} domains, {frontier_count} frontier URLs, {visited_count} visited URLs")
        
        if start_phase <= 1:
            # 1. Create bloom filter for seen URLs
            logger.info("Creating bloom filter for seen URLs...")
            try:
                await self.redis.execute_command('BF.INFO', 'seen:bloom')
                logger.info("Bloom filter already exists, skipping creation")
            except:
                # Create bloom filter - size for visited + frontier + growth room
                await self.redis.execute_command(
                    'BF.RESERVE', 'seen:bloom', 0.001, 160_000_000
                )
                logger.info("Created bloom filter for 160M URLs with 0.1% FPR")
        
        if start_phase <= 2:
            logger.info(f"Clearing existing frontier data...")
            await self.fm._clear_frontier()

            # 2. Migrate domain metadata
            logger.info(f"Migrating {domain_count} domain metadata records...")
            
            batch_size = 1000
            current_time = int(time.time())
            
            for i in range(0, domain_count, batch_size):
                batch = await self.pg.fetch("""
                    SELECT domain, robots_txt_content, robots_txt_expires_timestamp,
                        is_manually_excluded, last_scheduled_fetch_timestamp, is_seeded
                    FROM domain_metadata
                    ORDER BY domain LIMIT $1 OFFSET $2
                """, batch_size, i)
                
                pipe = self.redis.pipeline()
                
                for row in batch:
                    domain_key = f"domain:{row['domain']}"
                    logger.info(f"Migrating domain: {domain_key} with robots_txt size: {len(row['robots_txt_content'] or "")}")
                    
                    # Set next_fetch_time to now since it's been days
                    mapping = {
                        'robots_txt': row['robots_txt_content'] or '',
                        'robots_expires': str(row['robots_txt_expires_timestamp'] or 0),
                        'is_excluded': str(row['is_manually_excluded'] or 0),
                        'next_fetch_time': str(current_time),  # Set to now as requested
                        'frontier_offset': '0',  # Start at 0 for all domains
                        'frontier_size': '0',    # Will be updated when we add URLs
                        'is_seeded': str(row['is_seeded'] or 0)
                    }
                    
                    pipe.hset(domain_key, mapping=mapping)
                
                await pipe.execute()
                logger.info(f"Migrated {min(i + batch_size, domain_count)}/{domain_count} domains")
        
        if start_phase <= 3:
            # 3a. Migrate frontier URLs to files
            logger.info(f"Migrating {frontier_count} frontier URLs to files...")
            
            # Process frontier in batches
            batch_size = 5000
            offset = 0
            domains_with_urls = set()
            
            while offset < frontier_count:
                rows = await self.pg.fetch("""
                    SELECT url, domain
                    FROM frontier 
                    ORDER BY domain, added_timestamp
                    LIMIT $1 OFFSET $2
                """, batch_size, offset)
                
                if not rows:
                    break
                
                # Extract URLs and track domains
                urls = []
                for row in rows:
                    urls.append(row['url'])
                    domains_with_urls.add(row['domain'])
                
                # Add all URLs in this batch at once (depth defaults to 0)
                added = await self.fm.add_urls_batch(urls)
                
                offset += len(rows)
                logger.info(f"Migrated {min(offset, frontier_count)}/{frontier_count} frontier URLs (added {added} new of {len(urls)})")
        
        if start_phase <= 4:
            # 4. Migrate visited URLs
            logger.info(f"Migrating {visited_count} visited URLs...")
            batch_size = 5000
            offset = 0
            
            while offset < visited_count:
                rows = await self.pg.fetch("""
                    SELECT url, url_sha256, crawled_timestamp, http_status_code,
                        content_storage_path, redirected_to_url
                    FROM visited_urls
                    ORDER BY crawled_timestamp
                    LIMIT $1 OFFSET $2
                """, batch_size, offset)
                
                if not rows:
                    break
                
                pipe = self.redis.pipeline()
                for row in rows:
                    url_hash = row['url_sha256'][:16]  # Use first 16 chars as in the code
                    
                    # Convert timestamp to Unix timestamp if it's a datetime
                    if isinstance(row['crawled_timestamp'], datetime):
                        fetched_at = int(row['crawled_timestamp'].timestamp())
                    else:
                        fetched_at = row['crawled_timestamp'] or 0
                    
                    # Store visited metadata
                    pipe.hset(f'visited:{url_hash}', mapping={
                        'url': row['url'],
                        'status_code': str(row['http_status_code'] or 0),
                        'fetched_at': str(fetched_at),
                        'content_path': row['content_storage_path'] or '',
                        'error': ''
                    })
                    
                    # Mark as seen in bloom filter
                    pipe.execute_command('BF.ADD', 'seen:bloom', row['url'])
                
                await pipe.execute()
                offset += len(rows)
                logger.info(f"Migrated {min(offset, visited_count)}/{visited_count} visited URLs")
        
        # Summary
        total_time = time.time() - start_time
        logger.info(f"\nMigration complete in {total_time:.1f} seconds!")
        logger.info(f"Migrated:")
        logger.info(f"  - {domain_count} domain metadata records")
        logger.info(f"  - {frontier_count} frontier URLs to files")
        logger.info(f"  - {visited_count} visited URLs")
        
        # Verify migration
        logger.info("\nVerifying migration...")
        
        # Check Redis structures
        bloom_info = await self.redis.execute_command('BF.INFO', 'seen:bloom')
        logger.info(f"Bloom filter info: {dict(zip(bloom_info[::2], bloom_info[1::2]))}")
        
        queue_size = await self.redis.llen('domains:queue')
        logger.info(f"Domain queue size: {queue_size}")
        
        # Sample domain check
        sample_domain_key = await self.redis.randomkey()
        if sample_domain_key and sample_domain_key.startswith('domain:'):
            sample_data = await self.redis.hgetall(sample_domain_key)
            logger.info(f"Sample domain data for {sample_domain_key}: {sample_data}")


async def main():
    if len(sys.argv) != 2:
        print("Usage: python migrate_postgres_to_redis.py postgresql://user:pass@host:port/dbname")
        sys.exit(1)
    
    pg_url = sys.argv[1]
    
    # Connect to PostgreSQL
    logger.info(f"Connecting to PostgreSQL: {pg_url}")
    pg_conn = await asyncpg.connect(pg_url)
    
    # Connect to Redis
    logger.info("Connecting to Redis at localhost:6379")
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Create a minimal config for the frontier manager
    config = CrawlerConfig(
        data_dir=Path("./crawler_data"),
        seed_file=Path("./seeds.txt"),  # Not used in migration
        exclude_file=None,
        max_workers=100,
        max_pages=None,
        max_duration=None,
        user_agent="MigrationCrawler/1.0",
        db_type="redis",
        db_url=None,
        email="migration@example.com",
        resume=False,
        seeded_urls_only=True,
        log_level="INFO",
        # Redis configuration
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        redis_password=None
    )
    
    # Create a minimal politeness enforcer (not used in migration)
    politeness = RedisPolitenessEnforcer(config, redis_client, fetcher=None)
    
    # Create frontier manager
    frontier_manager = HybridFrontierManager(config, politeness, redis_client)
    
    # Run migration
    migrator = PostgreSQLToHybridMigrator(pg_conn, redis_client, frontier_manager)
    
    try:
        await migrator.migrate()
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await pg_conn.close()
        await redis_client.aclose()
    
    logger.info("Migration completed successfully!")


if __name__ == "__main__":
    asyncio.run(main()) 