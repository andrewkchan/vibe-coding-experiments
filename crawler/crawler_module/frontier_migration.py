"""Migration script to move frontier from filesystem to Redis.

Usage:
    python -m crawler.crawler_module.frontier_migration [--dry-run] [--batch-size=1000]
"""

import asyncio
import logging
from pathlib import Path
from typing import List
import redis.asyncio as redis
import click
from tqdm import tqdm

logger = logging.getLogger(__name__)

class FrontierMigrator:
    """Migrate frontier data from filesystem to Redis."""
    
    def __init__(self, data_dir: str, redis_client: redis.Redis, dry_run: bool = False):
        self.redis = redis_client
        self.dry_run = dry_run
        self.frontier_dir = Path(data_dir) / "frontiers"
        self.stats = {
            'domains_processed': 0,
            'urls_migrated': 0,
            'files_read': 0,
            'errors': 0
        }
    
    async def migrate(self, batch_size: int = 1000):
        """Perform the migration from filesystem to Redis."""
        logger.info(f"Starting frontier migration (dry_run={self.dry_run})")
        
        # Step 1: Discover all frontier files
        frontier_files = list(self.frontier_dir.rglob("*.frontier"))
        logger.info(f"Found {len(frontier_files)} frontier files to migrate")
        
        if not frontier_files:
            logger.info("No frontier files found. Migration complete.")
            return
        
        # Step 2: Process each domain's frontier file
        for file_path in tqdm(frontier_files, desc="Migrating domains"):
            try:
                await self._migrate_domain(file_path, batch_size)
            except Exception as e:
                logger.error(f"Error migrating {file_path}: {e}")
                self.stats['errors'] += 1
        
        # Step 3: Clean up domain metadata
        await self._cleanup_metadata()
        
        # Step 4: Report results
        logger.info("Migration complete!")
        logger.info(f"Stats: {self.stats}")
        
        if not self.dry_run:
            logger.info("Remember to delete the frontier directory after verifying the migration")
    
    async def _migrate_domain(self, file_path: Path, batch_size: int):
        """Migrate a single domain's frontier file to Redis."""
        domain = file_path.stem  # Extract domain from filename
        self.stats['domains_processed'] += 1
        
        # Read current metadata
        domain_key = f"domain:{domain}"
        metadata = await self.redis.hgetall(domain_key)
        
        current_offset = int(metadata.get('frontier_offset', 0))
        
        # Read URLs from file starting at current offset
        urls_to_migrate = []
        
        with open(file_path, 'r') as f:
            if current_offset > 0:
                f.seek(current_offset)
            
            for line in f:
                line = line.strip()
                if line:
                    urls_to_migrate.append(line)
                    self.stats['urls_migrated'] += 1
                    
                    # Batch insert to Redis
                    if len(urls_to_migrate) >= batch_size:
                        await self._push_urls_to_redis(domain, urls_to_migrate)
                        urls_to_migrate = []
        
        # Push remaining URLs
        if urls_to_migrate:
            await self._push_urls_to_redis(domain, urls_to_migrate)
        
        self.stats['files_read'] += 1
        
        if not self.dry_run:
            # Remove file-related metadata
            pipe = self.redis.pipeline()
            pipe.hdel(domain_key, 'file_path', 'frontier_offset', 'frontier_size')
            await pipe.execute()
    
    async def _push_urls_to_redis(self, domain: str, urls: List[str]):
        """Push URLs to Redis frontier list."""
        if self.dry_run:
            logger.debug(f"[DRY RUN] Would push {len(urls)} URLs to frontier:{domain}")
            return
        
        frontier_key = f"frontier:{domain}"
        
        # Use pipeline for efficiency
        pipe = self.redis.pipeline()
        for url_line in urls:
            # url_line is already in "url|depth" format
            pipe.lpush(frontier_key, url_line)
        
        await pipe.execute()
    
    async def _cleanup_metadata(self):
        """Remove file-related fields from all domain metadata."""
        logger.info("Cleaning up domain metadata...")
        
        cursor = 0
        cleaned = 0
        
        while True:
            cursor, keys = await self.redis.scan(cursor, match='domain:*', count=1000)
            
            if keys and not self.dry_run:
                pipe = self.redis.pipeline()
                for key in keys:
                    pipe.hdel(key, 'file_path', 'frontier_offset', 'frontier_size')
                    cleaned += 1
                await pipe.execute()
            
            if cursor == 0:
                break
        
        logger.info(f"Cleaned metadata for {cleaned} domains")

async def run_migration(dry_run: bool, batch_size: int, data_dir: str):
    """Run the async migration."""
    logging.basicConfig(level=logging.INFO)
    
    # Connect to Redis
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
    
    try:
        # Run migration
        migrator = FrontierMigrator(data_dir, redis_client, dry_run)
        await migrator.migrate(batch_size)
    finally:
        await redis_client.aclose()

@click.command()
@click.option('--dry-run', is_flag=True, help='Simulate migration without making changes')
@click.option('--batch-size', default=1000, help='Number of URLs to migrate per batch')
@click.option('--data-dir', type=click.Path(exists=True), help='Data directory')
def main(dry_run: bool, batch_size: int, data_dir: str):
    """Migrate frontier from filesystem to Redis."""
    asyncio.run(run_migration(dry_run, batch_size, data_dir))

if __name__ == '__main__':
    main() 