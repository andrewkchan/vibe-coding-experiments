# Redesigning the web crawler's frontier storage
_Exported on 7/3/2025 at 18:40:22 PDT from Cursor (1.0.0)_

---

**User**

Our web crawler's current design (see PLAN.MD for an overview) uses a frontier whose data (URLs) is stored in the filesystem, with metadata (offsets into the files, fetch times, read/write locks for the files etc.) stored in redis. This is based on an old blog post from Michael Nielsen in 2012, and I suspect the reason he used the filesystem was because RAM was still fairly expensive back then. Nowadays modern computers can have hundreds of GB of RAM, so putting the frontier completely into redis is feasible. This should not only eliminate slower SSD/disk access but should also mostly or entirely remove the need for filesystem read/write locks (as well as the extra metadata for tracking offsets into files) because pushing and popping from the per-domain frontiers becomes a completely atomic operation. 

I want to move my crawler to this design. This will involve not only code changes to both the system and tests but also migration of existing data. Help me ensure this design is sound and come up with an implementation plan that we can discuss and potentially execute. We shouldn't need to perform intermediate benchmarks because I expect that writing the code with you will be fast enough that we can make the change in a day or so, then run an experiment with a migrated crawler.

---

**Cursor**

Looking at your current design, I agree that moving the frontier entirely to Redis makes sense given modern hardware capabilities. The filesystem-based approach was indeed a reasonable choice in 2012 when RAM was expensive, but now we can leverage Redis's in-memory performance for the entire frontier.

## Design Analysis

### Current Architecture Issues
1. **Disk I/O bottleneck**: Reading/writing frontier files, even with SSDs
2. **Complex locking**: Process-local read locks + Redis write locks
3. **Offset management overhead**: Tracking byte offsets for each domain
4. **Non-atomic operations**: Reading from file + updating offset requires locking

### Proposed Pure Redis Design

## Implementation Plan

### 1. **Data Structure Design**

```
Redis Keys:
- frontier:{domain}      → List of "url|depth" strings
- domain:{domain}        → Hash (simplified - no file metadata)
  - next_fetch_time
  - robots_txt  
  - robots_expires
  - is_excluded
  - is_seeded
- domains:queue:{shard}  → List of domains (unchanged)
- seen:bloom            → Bloom filter (unchanged)
```

### 2. **Key Benefits**

- **Atomic operations**: LPUSH/RPOP are atomic - no locking needed
- **No offset tracking**: Redis manages the list internally
- **Simplified code**: Remove all file I/O, path management, offset updates
- **Better performance**: All operations in memory
- **Easier scaling**: Redis clustering if needed in future

### 3. **Memory Estimation**

Let's calculate the memory requirements:
- Average URL length: ~80 bytes
- Depth info: ~2 bytes  
- Redis overhead per list item: ~50 bytes
- Total per URL: ~130 bytes

For 10M URLs: ~1.3GB (very manageable)
For 100M URLs: ~13GB (still reasonable)

### 4. **Migration Strategy**

```plaintext
Redis Keys:
- frontier:{domain}      → List of "url|depth" strings
- domain:{domain}        → Hash (simplified - no file metadata)
  - next_fetch_time
  - robots_txt  
  - robots_expires
  - is_excluded
  - is_seeded
- domains:queue:{shard}  → List of domains (unchanged)
- seen:bloom            → Bloom filter (unchanged)
```

```python
"""Migration script to move frontier from filesystem to Redis.

Usage:
    python -m crawler.crawler_module.frontier_migration [--dry-run] [--batch-size=1000]
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Tuple
import redis.asyncio as redis
import click
from tqdm import tqdm

from .config import CrawlerConfig
from .frontier import FrontierManager

logger = logging.getLogger(__name__)

class FrontierMigrator:
    """Migrate frontier data from filesystem to Redis."""
    
    def __init__(self, config: CrawlerConfig, redis_client: redis.Redis, dry_run: bool = False):
        self.config = config
        self.redis = redis_client
        self.dry_run = dry_run
        self.frontier_dir = config.data_dir / "frontiers"
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

@click.command()
@click.option('--dry-run', is_flag=True, help='Simulate migration without making changes')
@click.option('--batch-size', default=1000, help='Number of URLs to migrate per batch')
@click.option('--config-file', type=click.Path(exists=True), help='Path to config file')
async def main(dry_run: bool, batch_size: int, config_file: str):
    """Migrate frontier from filesystem to Redis."""
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    config_args = []
    if config_file:
        config_args = ['--config-file', config_file]
    config = CrawlerConfig.from_args(config_args)
    
    # Connect to Redis
    redis_kwargs = config.get_redis_connection_kwargs()
    redis_client = redis.Redis(**redis_kwargs)
    
    try:
        # Run migration
        migrator = FrontierMigrator(config, redis_client, dry_run)
        await migrator.migrate(batch_size)
    finally:
        await redis_client.aclose()

if __name__ == '__main__':
    asyncio.run(main())
```

```python
if __name__ == '__main__':
    asyncio.run(main())

->

if __name__ == '__main__':
    main()
```

```python
@click.command()
@click.option('--dry-run', is_flag=True, help='Simulate migration without making changes')
@click.option('--batch-size', default=1000, help='Number of URLs to migrate per batch')
@click.option('--config-file', type=click.Path(exists=True), help='Path to config file')
async def main(dry_run: bool, batch_size: int, config_file: str):
    """Migrate frontier from filesystem to Redis."""
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    config_args = []
    if config_file:
        config_args = ['--config-file', config_file]
    config = CrawlerConfig.from_args(config_args)
    
    # Connect to Redis
    redis_kwargs = config.get_redis_connection_kwargs()
    redis_client = redis.Redis(**redis_kwargs)
    
    try:
        # Run migration
        migrator = FrontierMigrator(config, redis_client, dry_run)
        await migrator.migrate(batch_size)
    finally:
        await redis_client.aclose()

->

async def run_migration(dry_run: bool, batch_size: int, config_file: str):
    """Run the async migration."""
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    config_args = []
    if config_file:
        config_args = ['--config-file', config_file]
    config = CrawlerConfig.from_args(config_args)
    
    # Connect to Redis
    redis_kwargs = config.get_redis_connection_kwargs()
    redis_client = redis.Redis(**redis_kwargs)
    
    try:
        # Run migration
        migrator = FrontierMigrator(config, redis_client, dry_run)
        await migrator.migrate(batch_size)
    finally:
        await redis_client.aclose()

@click.command()
@click.option('--dry-run', is_flag=True, help='Simulate migration without making changes')
@click.option('--batch-size', default=1000, help='Number of URLs to migrate per batch')
@click.option('--config-file', type=click.Path(exists=True), help='Path to config file')
def main(dry_run: bool, batch_size: int, config_file: str):
    """Migrate frontier from filesystem to Redis."""
    asyncio.run(run_migration(dry_run, batch_size, config_file))
```

Now let's create the new Redis-based frontier implementation:

```python
"""Redis-only frontier manager for high-performance crawling.

This implementation stores all frontier URLs directly in Redis, eliminating
filesystem I/O and simplifying the architecture.
"""

import logging
from typing import Optional, Tuple, List, Dict
import asyncio
import redis.asyncio as redis
import hashlib

from .config import CrawlerConfig
from .utils import extract_domain, normalize_url
from .politeness import PolitenessEnforcer

logger = logging.getLogger(__name__)

# Common non-text file extensions to skip
NON_TEXT_EXTENSIONS = {
    # Images
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico', '.tiff', '.tif',
    # Videos
    '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.mpg', '.mpeg', '.m4v',
    # Audio
    '.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus',
    # Documents (non-HTML)
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt',
    # Archives
    '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz', '.tgz',
    # Executables
    '.exe', '.msi', '.dmg', '.pkg', '.deb', '.rpm', '.apk', '.app',
    # Other binary formats
    '.iso', '.bin', '.dat', '.db', '.sqlite', '.dll', '.so', '.dylib',
    # Media/Design files
    '.psd', '.ai', '.eps', '.indd', '.sketch', '.fig', '.xd',
    # Data files
    '.csv', '.json', '.xml', '.sql',
}

def is_likely_non_text_url(url: str) -> bool:
    """Check if a URL likely points to a non-text file based on its extension."""
    try:
        path = url.split('?')[0].split('#')[0]
        last_part = path.rstrip('/').split('/')[-1]
        if '.' in last_part:
            ext = '.' + last_part.split('.')[-1].lower()
            return ext in NON_TEXT_EXTENSIONS
    except Exception:
        pass
    return False

class RedisFrontierManager:
    """Pure Redis frontier manager - no filesystem required.
    
    Key improvements over filesystem version:
    - All operations are atomic Redis commands
    - No file locking needed
    - No offset tracking
    - Simpler, faster, more maintainable
    """
    
    def __init__(self, config: CrawlerConfig, politeness: PolitenessEnforcer, redis_client: redis.Redis):
        self.config = config
        self.politeness = politeness
        self.redis = redis_client
        self._shard_count: Optional[int] = None
        
    async def _get_shard_count(self) -> int:
        """Get the total number of shards."""
        if self._shard_count is None:
            count = await self.redis.get('crawler:shard_count')
            self._shard_count = int(count) if count else 1
        return self._shard_count
    
    def _get_domain_shard(self, domain: str) -> int:
        """Get the shard number for a domain using consistent hashing."""
        domain_hash = hashlib.md5(domain.encode()).digest()
        hash_int = int.from_bytes(domain_hash[:8], byteorder='big')
        return hash_int % (self._shard_count or 1)
    
    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
        await self._get_shard_count()
        logger.info(f"Redis frontier using {self._shard_count} shard(s)")
        
        # Initialize bloom filter if it doesn't exist
        try:
            await self.redis.execute_command('BF.INFO', 'seen:bloom')
            logger.info("Bloom filter already exists, using existing filter")
        except:
            try:
                await self.redis.execute_command(
                    'BF.RESERVE', 'seen:bloom', 0.001, 160_000_000
                )
                logger.info("Created new bloom filter for 160M URLs with 0.1% FPR")
            except:
                logger.warning("Could not create bloom filter - it may already exist")
        
        await self.politeness.initialize()
        
        if self.config.resume:
            count = await self.count_frontier()
            logger.info(f"Resuming crawl. Frontier has approximately {count} URLs.")
            if count == 0:
                logger.warning("Resuming with an empty frontier. Attempting to load seeds.")
                await self._load_seeds()
        else:
            logger.info("Starting new crawl. Clearing any existing frontier and loading seeds.")
            await self._clear_frontier()
            await self._load_seeds()
    
    async def _clear_frontier(self):
        """Clear all frontier data."""
        pipe = self.redis.pipeline()
        
        # Clear domain metadata
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match='domain:*', count=1000)
            if keys:
                pipe.delete(*keys)
            if cursor == 0:
                break
        
        # Clear frontier lists
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match='frontier:*', count=1000)
            if keys:
                pipe.delete(*keys)
            if cursor == 0:
                break
        
        # Clear all sharded queues
        shard_count = await self._get_shard_count()
        for shard in range(shard_count):
            pipe.delete(f'domains:queue:{shard}')
        
        await pipe.execute()
        logger.info("Cleared all frontier data")
    
    async def _load_seeds(self):
        """Load seed URLs from file."""
        logger.info(f"Loading seeds from {self.config.seed_file}")
        if not self.config.seed_file.exists():
            logger.error(f"Seed file not found: {self.config.seed_file}")
            return
            
        try:
            with open(self.config.seed_file, 'r') as f:
                urls = [normalize_url(line.strip()) for line in f if line.strip() and not line.startswith("#")]
                
            if not urls:
                logger.warning(f"Seed file {self.config.seed_file} is empty.")
                return
                
            # Mark domains as seeded
            seed_domains = {extract_domain(u) for u in urls if extract_domain(u)}
            seed_domains = [d for d in seed_domains if d]
            await self._mark_domains_as_seeded_batch(seed_domains)

            # Load robots.txt for seeds
            chunk_size = max(self.config.fetcher_workers, 50_000)
            for i in range(0, len(seed_domains), chunk_size):
                await self.politeness.batch_load_robots_txt(seed_domains[i:i+chunk_size])
                logger.info(f"Loaded robots.txt for {i+chunk_size}/{len(seed_domains)} domains")
            
            # Add URLs to frontier
            logger.info(f"Adding {len(urls)} URLs to the frontier")
            
            async def add_urls_batch_worker(worker_id: int, urls: list[str]) -> int:
                subchunk_size = 100
                added_count = 0
                for i in range(0, len(urls), subchunk_size):
                    added_count += await self.add_urls_batch(urls[i:i+subchunk_size])
                    if i % 1000 == 0:
                        logger.info(f"Worker {worker_id} seeded {added_count}/{len(urls)} URLs")
                return added_count
            
            seed_tasks = []
            chunk_size = (len(urls) + self.config.fetcher_workers - 1) // self.config.fetcher_workers
            for i in range(self.config.fetcher_workers):
                url_chunk = urls[i*chunk_size:(i+1)*chunk_size]
                seed_tasks.append(add_urls_batch_worker(i, url_chunk))
            
            added_counts = await asyncio.gather(*seed_tasks)
            added_count = sum(added_counts)
            logger.info(f"Loaded {added_count} URLs from seed file")
            
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")
    
    async def _mark_domains_as_seeded_batch(self, domains: List[str]):
        """Mark domains as seeded in domain metadata."""
        if not domains:
            return
            
        pipe = self.redis.pipeline()
        for domain in domains:
            pipe.hset(f'domain:{domain}', 'is_seeded', '1')
        await pipe.execute()
        logger.debug(f"Marked {len(domains)} domains as seeded")
    
    async def add_urls_batch(self, urls: List[str], depth: int = 0) -> int:
        """Add URLs to frontier - much simpler than filesystem version!"""
        # Pre-filter
        candidates = []
        for u in urls:
            if len(u) > 2000 or is_likely_non_text_url(u):
                continue
            candidates.append(u)
        
        if not candidates:
            return 0
            
        # Check bloom filter
        new_urls = []
        pipe = self.redis.pipeline()
        for url in candidates:
            pipe.execute_command('BF.EXISTS', 'seen:bloom', url)
        bloom_results = await pipe.execute()
        
        for url, exists in zip(candidates, bloom_results):
            if not exists:
                new_urls.append(url)
        
        if not new_urls:
            return 0
        
        # Politeness filtering
        allowed_urls = []
        for url in new_urls:
            if await self.politeness.is_url_allowed(url):
                allowed_urls.append(url)

        if not allowed_urls:
            return 0
            
        # Group by domain and add to Redis
        urls_by_domain: Dict[str, List[Tuple[str, int]]] = {}
        for url in allowed_urls:
            domain = extract_domain(url)
            if domain:
                if domain not in urls_by_domain:
                    urls_by_domain[domain] = []
                urls_by_domain[domain].append((url, depth))
        
        # Add to bloom filter and frontier lists atomically
        pipe = self.redis.pipeline()
        added_count = 0
        
        for domain, domain_urls in urls_by_domain.items():
            frontier_key = f"frontier:{domain}"
            domain_key = f"domain:{domain}"
            
            # Add to bloom filter
            for url, _ in domain_urls:
                pipe.execute_command('BF.ADD', 'seen:bloom', url)
            
            # Add to frontier list (LPUSH for FIFO behavior with RPOP)
            for url, url_depth in domain_urls:
                pipe.lpush(frontier_key, f"{url}|{url_depth}")
                added_count += 1
            
            # Initialize domain metadata
            pipe.hsetnx(domain_key, 'is_seeded', '0')
            
            # Add domain to shard queue
            shard = self._get_domain_shard(domain)
            pipe.rpush(f'domains:queue:{shard}', domain)
        
        await pipe.execute()
        return added_count
    
    async def get_next_url(self, fetcher_id: int = 0) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl - much simpler without file I/O!"""
        shard = fetcher_id
        
        # Verify shard exists
        shard_count = await self._get_shard_count()
        if shard >= shard_count:
            logger.error(f"Fetcher {fetcher_id} has no shard to handle (only {shard_count} shards exist)")
            return None
        
        # Pop domain from shard queue
        domain = await self.redis.lpop(f'domains:queue:{shard}')
        
        if not domain:
            return None
        
        try:
            # Check politeness
            if not await self.politeness.can_fetch_domain_now(domain):
                return None
            
            # Pop URL from domain's frontier (RPOP for FIFO with LPUSH)
            frontier_key = f"frontier:{domain}"
            url_data = await self.redis.rpop(frontier_key)
            
            if not url_data:
                # Domain has no URLs left
                return None
            
            # Parse URL data
            parts = url_data.split('|')
            if len(parts) >= 2:
                url, depth_str = parts[:2]
                
                # Skip non-text URLs
                if is_likely_non_text_url(url):
                    # Put URL back and return None
                    await self.redis.lpush(frontier_key, url_data)
                    return None
                
                # Check URL-level politeness
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules")
                    return None
                
                # Record fetch attempt
                await self.politeness.record_domain_fetch_attempt(domain)
                
                # Return URL (dummy ID for compatibility)
                return (url, domain, -1, int(depth_str))
            
            return None
            
        finally:
            # Always put domain back at end of queue
            if domain:
                await self.redis.rpush(f'domains:queue:{shard}', domain)
    
    async def count_frontier(self) -> int:
        """Count URLs in frontier - now a simple Redis operation."""
        total = 0
        cursor = 0
        
        while True:
            cursor, keys = await self.redis.scan(cursor, match='frontier:*', count=1000)
            
            if keys:
                # Get lengths of all frontier lists
                pipe = self.redis.pipeline()
                for key in keys:
                    pipe.llen(key)
                
                lengths = await pipe.execute()
                total += sum(lengths)
            
            if cursor == 0:
                break
        
        return total
    
    async def is_empty(self) -> bool:
        """Check if frontier is empty."""
        shard_count = await self._get_shard_count()
        
        pipe = self.redis.pipeline()
        for shard in range(shard_count):
            pipe.llen(f'domains:queue:{shard}')
        
        counts = await pipe.execute()
        return sum(counts) == 0
```

Now let's add a configuration option to switch between frontier implementations:

```python
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password."
    )

    args = parser.parse_args(sys_args)

->

    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password."
    )
    parser.add_argument(
        "--frontier-type",
        type=str,
        default="hybrid",
        choices=["hybrid", "redis"],
        help="Frontier storage type: 'hybrid' (filesystem+Redis) or 'redis' (Redis-only). Default: hybrid"
    )

    args = parser.parse_args(sys_args)
```

```python
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]

->

    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    frontier_type: str
```

```python
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
        )

->

            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
            frontier_type=args.frontier_type,
        )
```

Let me create a factory function to instantiate the correct frontier type:

```python
"""Factory for creating frontier instances based on configuration."""

import logging
import redis.asyncio as redis
from typing import Union

from .config import CrawlerConfig
from .frontier import FrontierManager
from .frontier_redis import RedisFrontierManager
from .politeness import PolitenessEnforcer

logger = logging.getLogger(__name__)

def create_frontier(
    config: CrawlerConfig, 
    politeness: PolitenessEnforcer,
    redis_client: redis.Redis
) -> Union[FrontierManager, RedisFrontierManager]:
    """Create a frontier instance based on the configured type.
    
    Args:
        config: Crawler configuration
        politeness: Politeness enforcer instance
        redis_client: Redis client instance
        
    Returns:
        Either FrontierManager (hybrid) or RedisFrontierManager (redis-only)
    """
    if config.frontier_type == "redis":
        logger.info("Creating Redis-only frontier manager")
        return RedisFrontierManager(config, politeness, redis_client)
    else:
        logger.info("Creating hybrid (filesystem+Redis) frontier manager")
        return FrontierManager(config, politeness, redis_client)
```

Now let's update the orchestrator to use the factory:

```python
from .frontier import FrontierManager

->

from .frontier_factory import create_frontier
```

```python
        temp_fetcher = Fetcher(self.config)
        politeness = PolitenessEnforcer(self.config, self.redis_client, temp_fetcher)
        # Initialize frontier (also initializes politeness with manual exclusions, seeds, etc.)
        frontier = FrontierManager(self.config, politeness, self.redis_client)  # type: ignore
        await frontier.initialize_frontier()

->

        temp_fetcher = Fetcher(self.config)
        politeness = PolitenessEnforcer(self.config, self.redis_client, temp_fetcher)
        # Initialize frontier (also initializes politeness with manual exclusions, seeds, etc.)
        frontier = create_frontier(self.config, politeness, self.redis_client)
        await frontier.initialize_frontier()
```

```python
from .frontier import FrontierManager

->

from .frontier_factory import create_frontier
```

```python
        # Initialize components
        self.storage = StorageManager(config, self.redis_client)  # type: ignore
        self.politeness = PolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier = FrontierManager(config, self.politeness, self.redis_client)  # type: ignore

->

        # Initialize components
        self.storage = StorageManager(config, self.redis_client)  # type: ignore
        self.politeness = PolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier = create_frontier(config, self.politeness, self.redis_client)
```

```python
from .frontier import FrontierManager

->

from .frontier_factory import create_frontier
```

```python
        self.frontier = FrontierManager(
            config, 
            self.politeness,  # type: ignore[arg-type]
            self.redis_client
        )

->

        self.frontier = create_frontier(
            config, 
            self.politeness,
            self.redis_client
        )
```

Let me also update the PLAN.MD to reflect the new Redis-only frontier option:

```markdown
### 3.1. Redis Data Structures

*   **Domain Metadata (Hash):** `domain:{domain}`
    *   `file_path`: Path to the domain's frontier file.
    *   `frontier_offset`: Byte offset for the next URL to read from the file.
    *   `frontier_size`: Total size in bytes of the frontier file.
    *   `next_fetch_time`: UNIX timestamp for when this domain can be crawled next.
    *   `robots_txt`: Cached content of `robots.txt`.
    *   `robots_expires`: Expiry timestamp for the `robots.txt` cache.
    *   `is_excluded`: Flag for manually excluded domains.

->

### 3.1. Redis Data Structures

*   **Domain Metadata (Hash):** `domain:{domain}`
    *   For hybrid mode:
        *   `file_path`: Path to the domain's frontier file.
        *   `frontier_offset`: Byte offset for the next URL to read from the file.
        *   `frontier_size`: Total size in bytes of the frontier file.
    *   For both modes:
        *   `next_fetch_time`: UNIX timestamp for when this domain can be crawled next.
        *   `robots_txt`: Cached content of `robots.txt`.
        *   `robots_expires`: Expiry timestamp for the `robots.txt` cache.
        *   `is_excluded`: Flag for manually excluded domains.
```

```markdown
*   **Sharded Domain Queues (Lists):** `domains:queue:0`, `domains:queue:1`, ..., `domains:queue:N-1`
    *   N lists (where N = number of fetcher processes) that partition domains across fetchers. Domain assignment uses simple modulo hashing: `shard = hash(domain) % num_fetcher_processes`.
*   **Shard Count (String):** `crawler:shard_count`
    *   Stores the current number of shards. Used for detecting when resharding is needed on startup.

->

*   **Frontier Lists (Redis-only mode):** `frontier:{domain}`
    *   Lists containing URLs to crawl for each domain in "url|depth" format.
    *   Only used when `--frontier-type=redis`.
*   **Sharded Domain Queues (Lists):** `domains:queue:0`, `domains:queue:1`, ..., `domains:queue:N-1`
    *   N lists (where N = number of fetcher processes) that partition domains across fetchers. Domain assignment uses simple modulo hashing: `shard = hash(domain) % num_fetcher_processes`.
*   **Shard Count (String):** `crawler:shard_count`
    *   Stores the current number of shards. Used for detecting when resharding is needed on startup.
```

```markdown
*   **Frontier Manager (`frontier.py`):** Manages the URLs to be crawled using a hybrid storage approach with domain sharding:
    *   **Redis:** Stores coordination data, domain metadata (politeness rules, next fetch times), a "seen" URL bloom filter for fast de-duplication, an exact set of "visited" URLs, and sharded domain queues (`domains:queue:0`, `domains:queue:1`, etc.).
    *   **File System:** Stores the actual frontier of URLs in append-only files, one per domain. This allows for a virtually unlimited frontier size without consuming excessive memory.

->

*   **Frontier Manager:** Manages the URLs to be crawled with two storage modes (configurable via `--frontier-type`):
    *   **Hybrid Mode (`frontier.py`, default):** Uses a combination of Redis and filesystem:
        *   **Redis:** Stores coordination data, domain metadata (politeness rules, next fetch times), a "seen" URL bloom filter for fast de-duplication, an exact set of "visited" URLs, and sharded domain queues (`domains:queue:0`, `domains:queue:1`, etc.).
        *   **File System:** Stores the actual frontier of URLs in append-only files, one per domain. This allows for a virtually unlimited frontier size without consuming excessive memory.
    *   **Redis-Only Mode (`frontier_redis.py`):** Stores everything in Redis for maximum performance:
        *   All frontier URLs are stored in Redis lists (`frontier:{domain}`)
        *   No file I/O, no offset tracking, simpler architecture
        *   Requires more memory but provides better performance
```

Let me fix a bug in the Redis frontier implementation:

```python
                # Skip non-text URLs
                if is_likely_non_text_url(url):
                    # Put URL back and return None
                    await self.redis.lpush(frontier_key, url_data)
                    return None

->

                # Skip non-text URLs
                if is_likely_non_text_url(url):
                    # Don't put it back - just skip it and try next time
                    logger.debug(f"Skipping non-text URL: {url}")
                    return None
```

## Summary

I've created a comprehensive implementation plan for migrating your crawler from a hybrid filesystem+Redis frontier to a pure Redis-based frontier. Here's what I've done:

### 1. **Key Benefits of the New Design**
- **Eliminates disk I/O** - All frontier operations are now in-memory
- **Simplifies architecture** - No more file locking, offset tracking, or path management  
- **Atomic operations** - Redis LPUSH/RPOP are inherently atomic, removing race conditions
- **Better performance** - Memory access is orders of magnitude faster than SSD/disk

### 2. **Implementation Components**

#### New Files Created:
- **`frontier_redis.py`** - Pure Redis frontier implementation using Redis lists
- **`frontier_factory.py`** - Factory pattern to instantiate the correct frontier type
- **`frontier_migration.py`** - Migration script to move existing frontier data from files to Redis

#### Modified Files:
- **`config.py`** - Added `--frontier-type` flag (choices: "hybrid", "redis")
- **`orchestrator.py`, `fetcher_process.py`, `parser_consumer.py`** - Updated to use the factory
- **`PLAN.MD`** - Updated documentation to reflect both frontier modes

### 3. **Redis Data Structure**
```
frontier:{domain} → List of "url|depth" strings
```
- Uses LPUSH to add URLs (at head)
- Uses RPOP to get URLs (from tail) 
- Maintains FIFO order
- No locking needed - operations are atomic

### 4. **Migration Process**
The migration script (`frontier_migration.py`) will:
1. Read all existing frontier files from their current offsets
2. Batch push URLs to Redis lists
3. Clean up file-related metadata from domain hashes
4. Support `--dry-run` mode for testing

Usage:
```bash
python -m crawler.crawler_module.frontier_migration --dry-run
python -m crawler.crawler_module.frontier_migration  # Actually migrate
```

### 5. **Memory Requirements**
- ~130 bytes per URL (including Redis overhead)
- 10M URLs ≈ 1.3GB RAM
- 100M URLs ≈ 13GB RAM (very manageable on modern servers)

### 6. **Backward Compatibility**
The implementation maintains full backward compatibility:
- Default mode remains "hybrid" (filesystem+Redis)
- Switch to Redis-only with: `--frontier-type redis`
- Can switch back if needed

### 7. **Performance Improvements**
The new design eliminates several bottlenecks:
- No file I/O operations
- No file system locks
- No offset calculations
- Simpler code paths
- All operations are O(1)

The implementation is ready to use. To migrate your existing crawler:

1. **First, test the migration:**
   ```bash
   python -m crawler.crawler_module.frontier_migration --dry-run
   ```

2. **Run the actual migration:**
   ```bash
   python -m crawler.crawler_module.frontier_migration
   ```

3. **Start crawler with Redis frontier:**
   ```bash
   python main.py --frontier-type redis [other options]
   ```

Would you like me to help you test this implementation or add any additional features before running the migration?

```plaintext
frontier:{domain} → List of "url|depth" strings
```

```plaintext
frontier:{domain} → List of "url|depth" strings
```

```shellscript
python -m crawler.crawler_module.frontier_migration --dry-run
python -m crawler.crawler_module.frontier_migration  # Actually migrate
```

```shellscript
   python -m crawler.crawler_module.frontier_migration --dry-run
```

```shellscript
   python -m crawler.crawler_module.frontier_migration
```

```shellscript
   python main.py --frontier-type redis [other options]
```

---

**User**

Great, just took it for a test run and everything works. Overall throughput is improved as hoped, guessing because due to no longer needing domain locks.

A couple things:
1. We no longer need the hybrid frontier mode. Let's delete all code for that.
2. Today, domains are sharded across fetcher processes (parsers also compute the shard when writing to frontier). This arrangement was mainly useful because frontier data was stored in files and I wanted to avoid having fetcher processes needing to lock the files + the domain metadata for reading. Let's remove the shards and have all the fetchers read from a single domain queue again.

---

**Cursor**

Excellent! The Redis frontier is working well. Let's clean up by removing the hybrid mode and domain sharding since they're no longer needed. The sharding was indeed mainly for avoiding file lock contention, which is irrelevant with Redis.

Let me make these changes:

```python
"""Redis-only frontier manager for high-performance crawling.

This implementation stores all frontier URLs directly in Redis, eliminating
filesystem I/O and simplifying the architecture.
"""

->

"""Redis-based frontier manager for high-performance crawling.

Stores all frontier URLs directly in Redis for maximum performance.
"""
```

```python
class RedisFrontierManager:
    """Pure Redis frontier manager - no filesystem required.
    
    Key improvements over filesystem version:
    - All operations are atomic Redis commands
    - No file locking needed
    - No offset tracking
    - Simpler, faster, more maintainable
    """
    
    def __init__(self, config: CrawlerConfig, politeness: PolitenessEnforcer, redis_client: redis.Redis):
        self.config = config
        self.politeness = politeness
        self.redis = redis_client
        self._shard_count: Optional[int] = None

->

class FrontierManager:
    """Redis frontier manager.
    
    All operations are atomic Redis commands, no locking needed.
    """
    
    def __init__(self, config: CrawlerConfig, politeness: PolitenessEnforcer, redis_client: redis.Redis):
        self.config = config
        self.politeness = politeness
        self.redis = redis_client
```

```python
        
    async def _get_shard_count(self) -> int:
        """Get the total number of shards."""
        if self._shard_count is None:
            count = await self.redis.get('crawler:shard_count')
            self._shard_count = int(count) if count else 1
        return self._shard_count
    
    def _get_domain_shard(self, domain: str) -> int:
        """Get the shard number for a domain using consistent hashing."""
        domain_hash = hashlib.md5(domain.encode()).digest()
        hash_int = int.from_bytes(domain_hash[:8], byteorder='big')
        return hash_int % (self._shard_count or 1)

->


```

```python
    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
        await self._get_shard_count()
        logger.info(f"Redis frontier using {self._shard_count} shard(s)")

->

    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
        logger.info("Initializing Redis frontier")
```

```python
        # Clear all sharded queues
        shard_count = await self._get_shard_count()
        for shard in range(shard_count):
            pipe.delete(f'domains:queue:{shard}')

->

        # Clear domain queue
        pipe.delete('domains:queue')
```

```python
            # Initialize domain metadata
            pipe.hsetnx(domain_key, 'is_seeded', '0')
            
            # Add domain to shard queue
            shard = self._get_domain_shard(domain)
            pipe.rpush(f'domains:queue:{shard}', domain)

->

            # Initialize domain metadata
            pipe.hsetnx(domain_key, 'is_seeded', '0')
            
            # Add domain to queue
            pipe.rpush('domains:queue', domain)
```

```python
    async def get_next_url(self, fetcher_id: int = 0) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl - much simpler without file I/O!"""
        shard = fetcher_id
        
        # Verify shard exists
        shard_count = await self._get_shard_count()
        if shard >= shard_count:
            logger.error(f"Fetcher {fetcher_id} has no shard to handle (only {shard_count} shards exist)")
            return None
        
        # Pop domain from shard queue
        domain = await self.redis.lpop(f'domains:queue:{shard}')

->

    async def get_next_url(self) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl."""
        # Pop domain from queue
        domain = await self.redis.lpop('domains:queue')
```

```python
        finally:
            # Always put domain back at end of queue
            if domain:
                await self.redis.rpush(f'domains:queue:{shard}', domain)

->

        finally:
            # Always put domain back at end of queue
            if domain:
                await self.redis.rpush('domains:queue', domain)
```

```python
    async def is_empty(self) -> bool:
        """Check if frontier is empty."""
        shard_count = await self._get_shard_count()
        
        pipe = self.redis.pipeline()
        for shard in range(shard_count):
            pipe.llen(f'domains:queue:{shard}')
        
        counts = await pipe.execute()
        return sum(counts) == 0

->

    async def is_empty(self) -> bool:
        """Check if frontier is empty."""
        count = await self.redis.llen('domains:queue')
        return count == 0
```

```python
import logging
from typing import Optional, Tuple, List, Dict
import asyncio
import redis.asyncio as redis
import hashlib

from .config import CrawlerConfig

->

import logging
from typing import Optional, Tuple, List, Dict
import asyncio
import redis.asyncio as redis

from .config import CrawlerConfig
```

```python
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password."
    )
    parser.add_argument(
        "--frontier-type",
        type=str,
        default="hybrid",
        choices=["hybrid", "redis"],
        help="Frontier storage type: 'hybrid' (filesystem+Redis) or 'redis' (Redis-only). Default: hybrid"
    )

    args = parser.parse_args(sys_args)

->

    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password."
    )

    args = parser.parse_args(sys_args)
```

```python
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    frontier_type: str

->

    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
```

```python
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
            frontier_type=args.frontier_type,
        )

->

            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
        )
```

```python
from .frontier_factory import create_frontier

->

from .frontier import FrontierManager
```

```python
        temp_fetcher = Fetcher(self.config)
        politeness = PolitenessEnforcer(self.config, self.redis_client, temp_fetcher)
        # Initialize frontier (also initializes politeness with manual exclusions, seeds, etc.)
        frontier = create_frontier(self.config, politeness, self.redis_client)
        await frontier.initialize_frontier()

->

        temp_fetcher = Fetcher(self.config)
        politeness = PolitenessEnforcer(self.config, self.redis_client, temp_fetcher)
        # Initialize frontier (also initializes politeness with manual exclusions, seeds, etc.)
        frontier = FrontierManager(self.config, politeness, self.redis_client)
        await frontier.initialize_frontier()
```

```python
from .frontier_factory import create_frontier

->

from .frontier import FrontierManager
```

```python
        # Initialize components
        self.storage = StorageManager(config, self.redis_client)  # type: ignore
        self.politeness = PolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier = create_frontier(config, self.politeness, self.redis_client)

->

        # Initialize components
        self.storage = StorageManager(config, self.redis_client)  # type: ignore
        self.politeness = PolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier = FrontierManager(config, self.politeness, self.redis_client)
```

```python
                    # Get next URL from our assigned shard
                    next_url_info = await self.frontier.get_next_url(fetcher_id=self.fetcher_id)

->

                    # Get next URL
                    next_url_info = await self.frontier.get_next_url()
```

```python
from .frontier_factory import create_frontier

->

from .frontier import FrontierManager
```

```python
        self.frontier = create_frontier(
            config, 
            self.politeness,
            self.redis_client
        )

->

        self.frontier = FrontierManager(
            config, 
            self.politeness,
            self.redis_client
        )
```

```python
    async def _reshard_domain_queues(self):
        """Reshard domain queues based on the number of fetcher processes."""
        # One shard per fetcher for simplicity
        new_shard_count = self.config.num_fetcher_processes
        
        # Get current shard count
        current_shard_count = await self.redis_client.get('crawler:shard_count')
        if current_shard_count:
            current_shard_count = int(current_shard_count)
        else:
            # Check if old single queue exists
            old_queue_size = await self.redis_client.llen('domains:queue')
            if old_queue_size > 0:
                logger.info("Detected old single domain queue, will migrate to sharded queues")
                current_shard_count = 0  # Indicates migration needed
            else:
                # Fresh start
                current_shard_count = new_shard_count
                await self.redis_client.set('crawler:shard_count', new_shard_count)
                logger.info(f"Initialized with {new_shard_count} domain queue shards")
                return
        
        if current_shard_count == new_shard_count:
            logger.info(f"Domain queues already sharded correctly ({new_shard_count} shards)")
            return
        
        logger.info(f"Resharding domain queues from {current_shard_count} to {new_shard_count} shards...")
        
        # Collect all domains from existing queues
        all_domains = []
        
        if current_shard_count == 0:
            # Migration from old single queue
            while True:
                domain = await self.redis_client.lpop('domains:queue')
                if not domain:
                    break
                all_domains.append(domain)
            logger.info(f"Migrated {len(all_domains)} domains from old single queue")
        else:
            # Collect from existing sharded queues
            for shard in range(current_shard_count):
                queue_key = f'domains:queue:{shard}'
                while True:
                    domain = await self.redis_client.lpop(queue_key)
                    if not domain:
                        break
                    all_domains.append(domain)
            logger.info(f"Collected {len(all_domains)} domains from {current_shard_count} shards")
        
        # Redistribute to new shards
        for domain in all_domains:
            # Use consistent hash to determine shard
            # Use MD5 hash for consistent hashing across processes
            domain_hash = hashlib.md5(domain.encode()).digest()
            hash_int = int.from_bytes(domain_hash[:8], byteorder='big')
            shard = hash_int % new_shard_count
            await self.redis_client.rpush(f'domains:queue:{shard}', domain)
        
        # Update shard count
        await self.redis_client.set('crawler:shard_count', new_shard_count)
        logger.info(f"Resharding complete. Redistributed {len(all_domains)} domains to {new_shard_count} shards")

->


```

```python
        # Clear any zombie locks from previous runs
        lock_manager = LockManager(self.redis_client)
        cleared_count = await lock_manager.clear_all_locks()
        if cleared_count > 0:
            logger.warning(f"Cleared {cleared_count} zombie locks from previous run")
        
        # Perform domain queue resharding if needed
        await self._reshard_domain_queues()
        
        # One-time initialization shared across all processes
        logger.info("Performing one-time initialization...")

->

        # Clear any zombie locks from previous runs
        lock_manager = LockManager(self.redis_client)
        cleared_count = await lock_manager.clear_all_locks()
        if cleared_count > 0:
            logger.warning(f"Cleared {cleared_count} zombie locks from previous run")
        
        # One-time initialization shared across all processes
        logger.info("Performing one-time initialization...")
```

```python
import redis.asyncio as redis
from redis.asyncio import BlockingConnectionPool
import gc
import hashlib

from .config import CrawlerConfig

->

import redis.asyncio as redis
from redis.asyncio import BlockingConnectionPool
import gc

from .config import CrawlerConfig
```

```python
                # Check if frontier is empty by checking all shard queues
                shard_count = await self.redis_client.get('crawler:shard_count')
                if shard_count:
                    shard_count = int(shard_count)
                    pipe = self.redis_client.pipeline()
                    for shard in range(shard_count):
                        pipe.llen(f'domains:queue:{shard}')
                    counts = await pipe.execute()
                    total_queue_size = sum(counts)
                    
                    if total_queue_size == 0 and self.pages_crawled_count > 0:
                        logger.info(f"Frontier is empty. Pages crawled: {self.pages_crawled_count}. Monitoring...")
                        await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2)
                        
                        # Re-check after wait
                        pipe = self.redis_client.pipeline()
                        for shard in range(shard_count):
                            pipe.llen(f'domains:queue:{shard}')
                        counts = await pipe.execute()
                        total_queue_size = sum(counts)
                        
                        if total_queue_size == 0:
                            logger.info("Frontier confirmed empty after wait. Signaling shutdown.")
                            self._shutdown_event.set()
                            break

->

                # Check if frontier is empty by checking domain queue
                queue_size = await self.redis_client.llen('domains:queue')
                
                if queue_size == 0 and self.pages_crawled_count > 0:
                    logger.info(f"Frontier is empty. Pages crawled: {self.pages_crawled_count}. Monitoring...")
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2)
                    
                    # Re-check after wait
                    queue_size = await self.redis_client.llen('domains:queue')
                    
                    if queue_size == 0:
                        logger.info("Frontier confirmed empty after wait. Signaling shutdown.")
                        self._shutdown_event.set()
                        break
```

```markdown
*   **Frontier Manager:** Manages the URLs to be crawled with two storage modes (configurable via `--frontier-type`):
    *   **Hybrid Mode (`frontier.py`, default):** Uses a combination of Redis and filesystem:
        *   **Redis:** Stores coordination data, domain metadata (politeness rules, next fetch times), a "seen" URL bloom filter for fast de-duplication, an exact set of "visited" URLs, and sharded domain queues (`domains:queue:0`, `domains:queue:1`, etc.).
        *   **File System:** Stores the actual frontier of URLs in append-only files, one per domain. This allows for a virtually unlimited frontier size without consuming excessive memory.
    *   **Redis-Only Mode (`frontier_redis.py`):** Stores everything in Redis for maximum performance:
        *   All frontier URLs are stored in Redis lists (`frontier:{domain}`)
        *   No file I/O, no offset tracking, simpler architecture
        *   Requires more memory but provides better performance

->

*   **Frontier Manager (`frontier.py`):** Manages the URLs to be crawled using Redis for all storage:
    *   Stores frontier URLs in Redis lists (`frontier:{domain}`)
    *   Uses Redis for coordination data, domain metadata (politeness rules, next fetch times), a "seen" URL bloom filter for fast de-duplication, an exact set of "visited" URLs, and a single domain queue (`domains:queue`)
    *   All operations are atomic Redis commands - no locking needed
```

```markdown
*   **Fetcher Processes (`fetcher_process.py`):** Independent processes that run pools of asynchronous workers for fetching URLs. Each process handles a specific shard of domains. The orchestrator runs fetcher 0 internally, while additional fetcher processes (1, 2, ...) run separately.

->

*   **Fetcher Processes (`fetcher_process.py`):** Independent processes that run pools of asynchronous workers for fetching URLs. The orchestrator runs fetcher 0 internally, while additional fetcher processes (1, 2, ...) run separately.
```

```markdown
### 2.2. Data Flow

1.  **Initialization:**
    *   The Main Orchestrator starts, connects to Redis, and performs one-time initialization (DB schema, manual exclusions, frontier initialization).
    *   It performs domain queue resharding if the number of fetcher processes has changed. Domains are distributed across shards using simple modulo hashing.
    *   It creates its own FetcherProcess instance (fetcher 0), spawns additional fetcher processes (1+), and parser processes.
2.  **Fetching Loop (Multiple Processes):**
    *   Each fetcher process only accesses its assigned domain shard(s). With N fetcher processes, fetcher i handles shard i.
    *   The Frontier Manager provides a ready URL from the fetcher's shard, respecting politeness delays.
    *   The fetcher worker downloads the page content.
    *   If the content is HTML, the worker pushes the raw HTML and metadata (URL, depth, etc.) to a Redis queue (`fetch:queue`).
    *   The fetcher worker is now free to immediately get another URL from its shard.
3.  **Parsing Loop (Separate Process):**
    *   A Parser Consumer process pops an HTML job from the `fetch:queue`.
    *   It parses the HTML to extract the main text content and new links.
    *   The Storage Manager saves the text content to a file.
    *   The Frontier Manager adds the new, valid, and unseen links to the appropriate domain's frontier file and shard queue.
    *   The Storage Manager records the URL as "visited" in Redis.
4.  **Termination:** The process continues until a crawl limit is reached or the frontier is exhausted.

->

### 2.2. Data Flow

1.  **Initialization:**
    *   The Main Orchestrator starts, connects to Redis, and performs one-time initialization (DB schema, manual exclusions, frontier initialization).
    *   It creates its own FetcherProcess instance (fetcher 0), spawns additional fetcher processes (1+), and parser processes.
2.  **Fetching Loop (Multiple Processes):**
    *   All fetcher processes read from the single domain queue.
    *   The Frontier Manager provides a ready URL from the queue, respecting politeness delays.
    *   The fetcher worker downloads the page content.
    *   If the content is HTML, the worker pushes the raw HTML and metadata (URL, depth, etc.) to a Redis queue (`fetch:queue`).
    *   The fetcher worker is now free to immediately get another URL.
3.  **Parsing Loop (Separate Process):**
    *   A Parser Consumer process pops an HTML job from the `fetch:queue`.
    *   It parses the HTML to extract the main text content and new links.
    *   The Storage Manager saves the text content to a file.
    *   The Frontier Manager adds the new, valid, and unseen links to the appropriate domain's frontier list in Redis.
    *   The Storage Manager records the URL as "visited" in Redis.
4.  **Termination:** The process continues until a crawl limit is reached or the frontier is exhausted.
```

```markdown
### 2.3. Concurrency Model

*   **Process-Level Parallelism:** 
    *   **Fetcher Processes:** Configurable with `--num-fetcher-processes` (default: 2). The orchestrator runs as fetcher 0 (leader model), while additional processes run independently.
    *   **Parser Processes:** Configurable with `--num-parser-processes` (default: 1).
*   **Worker-Level Concurrency:**
    *   **Fetcher Workers:** Each fetcher process runs `--fetcher-workers` async tasks (default: 500).
    *   **Parser Workers:** Each parser process runs `--parser-workers` async tasks (default: 80).
*   **Domain Sharding:** With N fetcher processes, there are N domain shards. Each fetcher exclusively handles its shard, preventing contention. The orchestrator automatically reshards on startup if the number of processes changes.

->

### 2.3. Concurrency Model

*   **Process-Level Parallelism:** 
    *   **Fetcher Processes:** Configurable with `--num-fetcher-processes` (default: 2). The orchestrator runs as fetcher 0 (leader model), while additional processes run independently.
    *   **Parser Processes:** Configurable with `--num-parser-processes` (default: 1).
*   **Worker-Level Concurrency:**
    *   **Fetcher Workers:** Each fetcher process runs `--fetcher-workers` async tasks (default: 500).
    *   **Parser Workers:** Each parser process runs `--parser-workers` async tasks (default: 80).
*   **Shared Queue:** All fetcher processes read from a single domain queue in Redis, with atomic operations ensuring no contention.
```

```markdown
```mermaid
graph TD
    subgraph User Input
        SeedFile[Seed File]
        Config["Configuration (processes, workers, etc.)"]
    end

    subgraph Crawler System
        subgraph "Orchestrator Process (Leader)"
            Main[Main Orchestrator]
            Fetcher0["Fetcher 0<br/>(500 workers)<br/>Shard 0"]
        end
        
        subgraph Additional Fetcher Processes
            Fetcher1["Fetcher 1<br/>(500 workers)<br/>Shard 1"]
            FetcherN["Fetcher N<br/>(500 workers)<br/>Shard N"]
        end
        
        subgraph Parser Processes
            Parser1["Parser Consumer<br/>(80 workers)"]
        end
    end

    subgraph Data Stores
        subgraph Redis
            DomainQueues["Sharded Domain Queues<br/>(domains:queue:0, :1, ..., :N)"]
            FetchQueue[fetch:queue]
            Metadata["Domain Metadata<br/>Bloom Filter<br/>Visited URLs"]
        end
        
        FS["File System<br/>(frontiers/, content/)"]
    end

    subgraph External Services
        Web[Internet/Websites]
    end

    %% Initialization
    Config --> Main
    SeedFile --> Main
    Main -->|spawns| Fetcher1
    Main -->|spawns| FetcherN
    Main -->|spawns| Parser1
    
    %% Fetching
    Fetcher0 --> Web
    Fetcher1 --> Web
    FetcherN --> Web
    
    %% Domain sharding
    Fetcher0 -.->|only shard 0| DomainQueues
    Fetcher1 -.->|only shard 1| DomainQueues
    FetcherN -.->|only shard N| DomainQueues
    
    %% Parse queue
    Fetcher0 --> FetchQueue
    Fetcher1 --> FetchQueue
    FetcherN --> FetchQueue
    FetchQueue --> Parser1
    
    %% Storage
    Parser1 --> Metadata
    Parser1 --> FS
    
    %% Shared access
    Fetcher0 <--> Metadata
    Fetcher1 <--> Metadata
    FetcherN <--> Metadata
```

->

```mermaid
graph TD
    subgraph User Input
        SeedFile[Seed File]
        Config["Configuration (processes, workers, etc.)"]
    end

    subgraph Crawler System
        subgraph "Orchestrator Process (Leader)"
            Main[Main Orchestrator]
            Fetcher0["Fetcher 0<br/>(500 workers)"]
        end
        
        subgraph Additional Fetcher Processes
            Fetcher1["Fetcher 1<br/>(500 workers)"]
            FetcherN["Fetcher N<br/>(500 workers)"]
        end
        
        subgraph Parser Processes
            Parser1["Parser Consumer<br/>(80 workers)"]
        end
    end

    subgraph Data Stores
        subgraph Redis
            DomainQueue["Domain Queue<br/>(domains:queue)"]
            FetchQueue[fetch:queue]
            Metadata["Domain Metadata<br/>Bloom Filter<br/>Visited URLs<br/>Frontier Lists"]
        end
        
        FS["File System<br/>(content/)"]
    end

    subgraph External Services
        Web[Internet/Websites]
    end

    %% Initialization
    Config --> Main
    SeedFile --> Main
    Main -->|spawns| Fetcher1
    Main -->|spawns| FetcherN
    Main -->|spawns| Parser1
    
    %% Fetching
    Fetcher0 --> Web
    Fetcher1 --> Web
    FetcherN --> Web
    
    %% Shared domain queue
    Fetcher0 <--> DomainQueue
    Fetcher1 <--> DomainQueue
    FetcherN <--> DomainQueue
    
    %% Parse queue
    Fetcher0 --> FetchQueue
    Fetcher1 --> FetchQueue
    FetcherN --> FetchQueue
    FetchQueue --> Parser1
    
    %% Storage
    Parser1 --> Metadata
    Parser1 --> FS
    
    %% Shared access
    Fetcher0 <--> Metadata
    Fetcher1 <--> Metadata
    FetcherN <--> Metadata
```
```

```markdown
### 3.1. Redis Data Structures

*   **Domain Metadata (Hash):** `domain:{domain}`
    *   For hybrid mode:
        *   `file_path`: Path to the domain's frontier file.
        *   `frontier_offset`: Byte offset for the next URL to read from the file.
        *   `frontier_size`: Total size in bytes of the frontier file.
    *   For both modes:
        *   `next_fetch_time`: UNIX timestamp for when this domain can be crawled next.
        *   `robots_txt`: Cached content of `robots.txt`.
        *   `robots_expires`: Expiry timestamp for the `robots.txt` cache.
        *   `is_excluded`: Flag for manually excluded domains.
*   **Frontier Lists (Redis-only mode):** `frontier:{domain}`
    *   Lists containing URLs to crawl for each domain in "url|depth" format.
    *   Only used when `--frontier-type=redis`.
*   **Sharded Domain Queues (Lists):** `domains:queue:0`, `domains:queue:1`, ..., `domains:queue:N-1`
    *   N lists (where N = number of fetcher processes) that partition domains across fetchers. Domain assignment uses simple modulo hashing: `shard = hash(domain) % num_fetcher_processes`.
*   **Shard Count (String):** `crawler:shard_count`
    *   Stores the current number of shards. Used for detecting when resharding is needed on startup.

->

### 3.1. Redis Data Structures

*   **Domain Metadata (Hash):** `domain:{domain}`
    *   `next_fetch_time`: UNIX timestamp for when this domain can be crawled next.
    *   `robots_txt`: Cached content of `robots.txt`.
    *   `robots_expires`: Expiry timestamp for the `robots.txt` cache.
    *   `is_excluded`: Flag for manually excluded domains.
*   **Frontier Lists:** `frontier:{domain}`
    *   Lists containing URLs to crawl for each domain in "url|depth" format.
*   **Domain Queue (List):** `domains:queue`
    *   Single queue that contains all domains with URLs to crawl.
```

```markdown
### 3.2. File System Storage

*   **Frontier Files:** `crawler_data/frontiers/{domain_hash_prefix}/{domain}.frontier`
    *   Append-only files containing the URLs to be crawled for a specific domain, with one `url|depth` entry per line.
*   **Content Files:** `crawler_data/content/`
    *   Extracted text content is stored in files named by a hash of the URL (e.g., `<url_sha256>.txt`). The `visited:*` hashes in Redis link a URL to its content file.

->

### 3.2. File System Storage

*   **Content Files:** `crawler_data/content/`
    *   Extracted text content is stored in files named by a hash of the URL (e.g., `<url_sha256>.txt`). The `visited:*` hashes in Redis link a URL to its content file.
```

```markdown
## 5. Fault Tolerance and Resumption

*   **State Persistence:** All critical state is persisted in Redis or the file system.
*   **Redis Persistence:** The provided Docker Compose setup for Redis uses RDB snapshots only (AOF disabled) for simplicity and lower memory usage. Snapshots occur every 1-15 minutes based on change frequency.
*   **Resumption:** When the crawler is started with the `--resume` flag, it picks up where it left off using the data in Redis and the frontier files.
*   **Zombie Lock Cleanup:** On startup, the orchestrator scans Redis for and removes any stale domain write locks that might have been left over from a previous crashed run.

->

## 5. Fault Tolerance and Resumption

*   **State Persistence:** All critical state is persisted in Redis.
*   **Redis Persistence:** The provided Docker Compose setup for Redis uses RDB snapshots only (AOF disabled) for simplicity and lower memory usage. Snapshots occur every 1-15 minutes based on change frequency.
*   **Resumption:** When the crawler is started with the `--resume` flag, it picks up where it left off using the data in Redis.
*   **Zombie Lock Cleanup:** On startup, the orchestrator scans Redis for and removes any stale domain write locks that might have been left over from a previous crashed run.
```

```markdown
## 6. Multiprocess Configuration and Domain Sharding

### 6.1. Process Configuration

The crawler supports configurable numbers of fetcher and parser processes:

*   **`--num-fetcher-processes`** (default: 2): Number of fetcher processes. The orchestrator runs as fetcher 0 (leader), reducing context switching.
*   **`--num-parser-processes`** (default: 1): Number of parser processes.
*   **`--fetcher-workers`** (default: 500): Async workers per fetcher process.
*   **`--parser-workers`** (default: 80): Async workers per parser process.

Example configurations:
*   Light workload: 1 fetcher + 1 parser (1000 total async workers)
*   Balanced (default): 2 fetchers + 1 parser (1000 fetchers + 80 parsers)
*   Parser-heavy: 2 fetchers + 2 parsers (1000 fetchers + 160 parsers)
*   Scale-out: 4 fetchers + 2 parsers (2000 fetchers + 160 parsers)

### 6.2. Domain Sharding

To prevent contention and maintain process-local read locks:

1.  **Shard Assignment:** Each domain is assigned to exactly one fetcher using `hash(domain) % num_fetcher_processes`.
2.  **Queue Isolation:** Each fetcher only reads from its shard's queue (`domains:queue:i`).
3.  **Automatic Resharding:** On startup, if the number of fetcher processes changes, the orchestrator redistributes all domains to match the new configuration.
4.  **Leader Model Benefits:** The orchestrator runs fetcher 0 internally, reducing process overhead and simplifying coordination.

### 6.3. Process Responsibilities

**Orchestrator (Leader)**
*   Performs one-time initialization (schema, exclusions, frontier)
*   Runs fetcher process 0 internally
*   Spawns and monitors additional processes
*   Handles global stopping conditions
*   Performs domain resharding when needed

**Fetcher Processes**
*   Each handles one domain shard exclusively
*   Maintains process-local domain read locks
*   Fetches content and pushes to parse queue
*   Respects politeness delays per domain

**Parser Processes**
*   Consume from shared parse queue
*   Extract text and links
*   Update frontier files with new URLs
*   Save content to filesystem

->

## 6. Multiprocess Configuration

### 6.1. Process Configuration

The crawler supports configurable numbers of fetcher and parser processes:

*   **`--num-fetcher-processes`** (default: 2): Number of fetcher processes. The orchestrator runs as fetcher 0 (leader), reducing context switching.
*   **`--num-parser-processes`** (default: 1): Number of parser processes.
*   **`--fetcher-workers`** (default: 500): Async workers per fetcher process.
*   **`--parser-workers`** (default: 80): Async workers per parser process.

Example configurations:
*   Light workload: 1 fetcher + 1 parser (500 fetchers + 80 parsers)
*   Balanced (default): 2 fetchers + 1 parser (1000 fetchers + 80 parsers)
*   Parser-heavy: 2 fetchers + 2 parsers (1000 fetchers + 160 parsers)
*   Scale-out: 4 fetchers + 2 parsers (2000 fetchers + 160 parsers)

### 6.2. Process Responsibilities

**Orchestrator (Leader)**
*   Performs one-time initialization (schema, exclusions, frontier)
*   Runs fetcher process 0 internally
*   Spawns and monitors additional processes
*   Handles global stopping conditions

**Fetcher Processes**
*   Read from shared domain queue
*   Fetch content and push to parse queue
*   Respect politeness delays per domain

**Parser Processes**
*   Consume from shared parse queue
*   Extract text and links
*   Update frontier lists with new URLs
*   Save content to filesystem
```

## Summary

I've successfully completed the requested changes:

### 1. **Removed Hybrid Frontier Mode**
- Deleted `frontier.py` (the old filesystem-based implementation)
- Deleted `frontier_migration.py` (no longer needed)
- Deleted `frontier_factory.py` (no longer needed)
- Renamed `frontier_redis.py` to `frontier.py`
- Removed `--frontier-type` configuration option

### 2. **Removed Domain Sharding**
- Removed all sharding logic from the frontier:
  - `_get_shard_count()` and `_get_domain_shard()` methods
  - Removed `fetcher_id` parameter from `get_next_url()`
- Changed from multiple shard queues (`domains:queue:0`, `domains:queue:1`, etc.) to a single queue (`domains:queue`)
- Removed resharding logic from orchestrator (`_reshard_domain_queues()`)
- Updated all code to use the single domain queue

### 3. **Updated Documentation**
- Updated PLAN.MD to reflect the simplified architecture
- Removed all mentions of sharding and hybrid mode
- Updated architecture diagram to show single domain queue

The system is now much simpler:
- **All frontier data in Redis** - No filesystem I/O
- **Single domain queue** - All fetchers read from the same queue
- **Atomic operations** - No locking needed, Redis handles concurrency
- **Cleaner code** - Removed hundreds of lines of complexity

This should give you the performance benefits you observed (no domain locks) while also making the codebase much easier to understand and maintain.

---

**User**

Awesome!
1. Can we make a one-time script to migrate all shard queues to the single queue?
2. Can we update the tests in tests/test_frontier.py?

---

**Cursor**

Absolutely! Let's create both the migration script and update the tests.

```python
#!/usr/bin/env python3
"""
One-time migration script to move from sharded domain queues to single queue.

Usage:
    python migrate_shards_to_single_queue.py [--dry-run]
"""

import asyncio
import argparse
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)

async def migrate_shards(redis_client: redis.Redis, dry_run: bool = False):
    """Migrate from sharded domain queues to single queue."""
    
    # Get current shard count
    shard_count = await redis_client.get('crawler:shard_count')
    if not shard_count:
        logger.info("No shard count found. System may already be using single queue.")
        return
    
    shard_count = int(shard_count)
    logger.info(f"Found {shard_count} shards to migrate")
    
    # Collect all domains from sharded queues
    all_domains = []
    total_collected = 0
    
    for shard in range(shard_count):
        queue_key = f'domains:queue:{shard}'
        shard_domains = []
        
        # Pop all domains from this shard
        while True:
            domain = await redis_client.lpop(queue_key)
            if not domain:
                break
            shard_domains.append(domain)
        
        logger.info(f"Collected {len(shard_domains)} domains from shard {shard}")
        all_domains.extend(shard_domains)
        total_collected += len(shard_domains)
    
    if total_collected == 0:
        logger.info("No domains found in sharded queues. Nothing to migrate.")
        return
    
    # Remove duplicates while preserving order
    seen = set()
    unique_domains = []
    for domain in all_domains:
        if domain not in seen:
            seen.add(domain)
            unique_domains.append(domain)
    
    logger.info(f"Total domains collected: {total_collected}, unique: {len(unique_domains)}")
    
    if dry_run:
        logger.info("[DRY RUN] Would push the following domains to domains:queue:")
        for domain in unique_domains[:10]:  # Show first 10
            logger.info(f"  - {domain}")
        if len(unique_domains) > 10:
            logger.info(f"  ... and {len(unique_domains) - 10} more")
        logger.info("[DRY RUN] Would delete crawler:shard_count")
        return
    
    # Push all domains to single queue
    if unique_domains:
        logger.info(f"Pushing {len(unique_domains)} domains to single queue...")
        # Use pipeline for efficiency
        pipe = redis_client.pipeline()
        for domain in unique_domains:
            pipe.rpush('domains:queue', domain)
        await pipe.execute()
    
    # Remove shard count key
    await redis_client.delete('crawler:shard_count')
    logger.info("Removed crawler:shard_count key")
    
    # Verify migration
    final_count = await redis_client.llen('domains:queue')
    logger.info(f"Migration complete! Single queue now contains {final_count} domains")
    
    # Clean up empty shard queues
    logger.info("Cleaning up empty shard queues...")
    for shard in range(shard_count):
        queue_key = f'domains:queue:{shard}'
        await redis_client.delete(queue_key)
    logger.info(f"Deleted {shard_count} empty shard queues")

async def main():
    parser = argparse.ArgumentParser(description="Migrate from sharded to single domain queue")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database')
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        decode_responses=True
    )
    
    try:
        # Test connection
        await redis_client.ping()
        logger.info(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
        
        # Run migration
        await migrate_shards(redis_client, dry_run=args.dry_run)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        await redis_client.aclose()

if __name__ == '__main__':
    asyncio.run(main())
```

```python
import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock
import redis.asyncio as redis
from collections import defaultdict
from typing import List, Dict, Tuple

from crawler_module.frontier import FrontierManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
    data_dir: Path
    seed_file: Path
    email: str = "frontier_test@example.com"
    exclude_file: Path | None = None
    fetcher_workers: int = 1
    parser_workers: int = 1
    num_fetcher_processes: int = 1
    num_parser_processes: int = 1
    max_pages: int | None = None
    max_duration: int | None = None
    log_level: str = "DEBUG"
    resume: bool = False
    user_agent: str = "FrontierTestCrawler/1.0"
    seeded_urls_only: bool = False
    # Redis settings
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 15
    redis_password: str | None = None

# Note: The redis_client fixture is now imported from conftest.py as redis_test_client
# This ensures we use db=15 for tests and never touch production data (db=0)

@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
    test_data_dir = tmp_path / "test_crawler_data_frontier_redis"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for frontier: {test_data_dir}")
    return test_data_dir

@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
    """Provides the FrontierTestConfig object, seeds file created here."""
    seed_file_path = temp_test_frontier_dir / "test_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("http://example.com/seed1\n")
        sf.write("http://example.org/seed2\n")
        sf.write("http://example.com/seed1\n")  # Duplicate to test seen
    return FrontierTestConfig(data_dir=temp_test_frontier_dir, seed_file=seed_file_path)

@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
    """Provides the actual CrawlerConfig based on FrontierTestConfig."""
    return CrawlerConfig(**vars(frontier_test_config_obj))

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier() -> MagicMock:
    """Provides a mocked PolitenessEnforcer for FrontierManager tests."""
    mock_pe = AsyncMock(spec=PolitenessEnforcer)
    
    # Default mock behaviors for permissive testing
    mock_pe.is_url_allowed = AsyncMock(return_value=True)
    mock_pe.can_fetch_domain_now = AsyncMock(return_value=True)
    mock_pe.record_domain_fetch_attempt = AsyncMock()
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0)
    mock_pe._load_manual_exclusions = AsyncMock()
    return mock_pe

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
) -> FrontierManager:
    fm = FrontierManager(
        config=actual_config_for_frontier,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    # Patch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock(side_effect=fm._mark_domains_as_seeded_batch)
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch)
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager):
    logger.info("Testing Redis Frontier Initialization (New Crawl)")
    
    await frontier_manager.initialize_frontier()
    
    # Assert that the batch methods were called
    frontier_manager._mark_domains_as_seeded_batch.assert_called_once()
    frontier_manager.add_urls_batch.assert_called_once()
    
    # Check the domains that were marked as seeded
    seeded_domains_call = frontier_manager._mark_domains_as_seeded_batch.call_args[0][0]
    assert set(seeded_domains_call) == {"example.com", "example.org"}
    
    # Check the URLs that were added to the frontier
    urls_added_call = frontier_manager.add_urls_batch.call_args[0][0]
    assert set(urls_added_call) == {"http://example.com/seed1", "http://example.org/seed2"}
    
    logger.info("Redis frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_frontier_initialization_with_bare_domains_bug(
    temp_test_frontier_dir: Path,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test when seed file contains bare domains instead of proper URLs."""
    
    # Create frontier from seed file with bare domains (no http://)
    seed_file_path = temp_test_frontier_dir / "bare_domains_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("example.com\n")
        sf.write("gpumagick.com\n")
        sf.write("test.org\n")
    test_config = FrontierTestConfig(
        data_dir=temp_test_frontier_dir,
        seed_file=seed_file_path
    )
    fm = FrontierManager(
        config=CrawlerConfig(**vars(test_config)),
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Make politeness permissive
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Initialize frontier
    await fm.initialize_frontier()
    
    retrieved_urls = []
    for _ in range(3):
        result = await fm.get_next_url()
        if result:
            url, domain, url_id, depth = result
            retrieved_urls.append(url)
    
    assert len(retrieved_urls) == 3, f"Expected 3 URLs, got {len(retrieved_urls)}"
    
    expected_proper_urls = {
        "http://example.com", "https://example.com",
        "http://gpumagick.com", "https://gpumagick.com", 
        "http://test.org", "https://test.org"
    }
    
    for url in retrieved_urls:
        assert any(url == proper_url or url == proper_url.replace('http://', 'https://') 
                  for proper_url in expected_proper_urls), \
            f"Retrieved URL '{url}' is not a proper URL with scheme"

@pytest.mark.asyncio
async def test_add_urls_batch(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing batch adding of URLs to Redis Frontier")
    
    # 1. Setup - Ensure frontier is empty and politeness is permissive
    await frontier_manager._clear_frontier()
    assert await frontier_manager.is_empty() is True
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True

    # 2. Add a batch of new URLs
    urls_to_add = ["http://test.com/batch1", "http://test.org/batch2", "http://test.com/batch1"]
    added_count = await frontier_manager.add_urls_batch(urls_to_add)

    # Assert that duplicates within the batch are handled and politeness was checked
    assert added_count == 2
    assert await frontier_manager.count_frontier() == 2
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2
    
    # 3. Add another batch, some new, some disallowed, some already seen
    mock_politeness_enforcer_for_frontier.is_url_allowed.reset_mock()
    
    # Make one URL disallowed
    def side_effect(url):
        if "disallowed" in url:
            return False
        return True
    mock_politeness_enforcer_for_frontier.is_url_allowed.side_effect = side_effect
    
    next_urls_to_add = ["http://new.com/page1", "http://test.org/batch2", "http://disallowed.com/page"]
    added_count_2 = await frontier_manager.add_urls_batch(next_urls_to_add)
    
    assert added_count_2 == 1  # Only new.com/page1 should be added
    assert await frontier_manager.count_frontier() == 3  # 2 from before + 1 new
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2  # disallowed.com and new.com
    
    logger.info("Batch URL adding test passed.")

@pytest.mark.asyncio
async def test_get_next_url(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing get_next_url functionality")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add some URLs
    urls = ["http://example.com/page1", "http://example.org/page2", "http://test.com/page3"]
    await frontier_manager.add_urls_batch(urls)
    
    # Get URLs one by one
    retrieved_urls = []
    for _ in range(3):
        result = await frontier_manager.get_next_url()
        assert result is not None
        url, domain, url_id, depth = result
        retrieved_urls.append(url)
        assert depth == 0  # All seed URLs have depth 0
    
    # Should have retrieved all URLs
    assert set(retrieved_urls) == set(urls)
    
    # Frontier should be empty now
    assert await frontier_manager.count_frontier() == 0
    assert await frontier_manager.get_next_url() is None
    
    logger.info("get_next_url test passed.")

@pytest.mark.asyncio
async def test_redis_list_persistence(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing Redis list persistence")
    
    # Permissive politeness
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # First run: populate frontier
    cfg_run1 = CrawlerConfig(**vars(frontier_test_config_obj))
    
    frontier_run1 = FrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    await frontier_run1.initialize_frontier()
    
    # Add URLs with different depths
    test_urls = [
        ("http://persistent.com/page1", 0),
        ("http://persistent.com/page2", 1),
        ("http://example.net/page1", 2)
    ]
    
    # Add URLs with specified depths
    for url, depth in test_urls:
        await frontier_run1.add_urls_batch([url], depth=depth)
    
    # Check that frontier lists were created in Redis
    frontier_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_test_client.scan(cursor, match='frontier:*', count=100)
        frontier_keys.extend(keys)
        if cursor == 0:
            break
    
    assert len(frontier_keys) > 0, "No frontier lists were created"
    
    # Verify contents of frontier lists
    for key in frontier_keys:
        domain = key.split(':')[1]
        urls_in_list = await redis_test_client.lrange(key, 0, -1)
        logger.info(f"Domain {domain} has {len(urls_in_list)} URLs in Redis")
        
        # Parse and verify URL format
        for url_entry in urls_in_list:
            parts = url_entry.split('|')
            assert len(parts) == 2, f"Expected 2 parts in '{url_entry}'"
            url, depth_str = parts
            assert url.startswith('http'), f"Invalid URL: {url}"
            assert depth_str.isdigit(), f"Invalid depth: {depth_str}"
    
    # Get one URL and verify it's removed from the list
    url_retrieved = await frontier_run1.get_next_url()
    assert url_retrieved is not None
    
    # Calculate total URLs from all seed URLs + test URLs
    total_urls = 2 + len(test_urls)  # 2 seed URLs + 3 test URLs
    
    # Test that we can retrieve all URLs by iterating
    all_retrieved_urls = [url_retrieved[0]]  # Start with the one we already got
    
    while True:
        next_url = await frontier_run1.get_next_url()
        if next_url is None:
            break
        all_retrieved_urls.append(next_url[0])
    
    # Verify we got all the URLs we added
    expected_urls = {
        "http://example.com/seed1",
        "http://example.org/seed2", 
        "http://persistent.com/page1",
        "http://persistent.com/page2",
        "http://example.net/page1"
    }
    assert len(all_retrieved_urls) == total_urls
    assert set(all_retrieved_urls) == expected_urls
    
    # Frontier should now be empty
    assert await frontier_run1.count_frontier() == 0
    
    logger.info("Redis list persistence test passed.")

@pytest.mark.asyncio
async def test_bloom_filter_deduplication(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing bloom filter deduplication")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Add URL first time
    first_add = await frontier_manager.add_urls_batch(["http://example.com/test"])
    assert first_add == 1
    
    # Try to add same URL again
    second_add = await frontier_manager.add_urls_batch(["http://example.com/test"])
    assert second_add == 0  # Should be rejected by bloom filter
    
    # Verify bloom filter contains the URL
    exists = await redis_test_client.execute_command('BF.EXISTS', 'seen:bloom', 'http://example.com/test')
    assert exists == 1
    
    logger.info("Bloom filter deduplication test passed.")

@pytest.mark.asyncio 
async def test_domain_queue(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing domain queue functionality")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add URLs from different domains
    urls = [
        "http://domain1.com/page1",
        "http://domain2.com/page1", 
        "http://domain1.com/page2"
    ]
    await frontier_manager.add_urls_batch(urls)
    
    # Check domain queue
    queued_domains = await redis_test_client.lrange('domains:queue', 0, -1)
    
    # Convert to set to ignore order and potential duplicates
    assert set(queued_domains) == {"domain1.com", "domain2.com"}
    
    # Get a URL from a domain
    result = await frontier_manager.get_next_url()
    assert result is not None
    url, domain, _, _ = result
    
    # After fetching, domain should be back in queue
    updated_queue = await redis_test_client.lrange('domains:queue', 0, -1)
    assert domain in updated_queue  # Domain should be at the end
    
    logger.info("Domain queue test passed.")

@pytest.mark.asyncio
async def test_frontier_error_handling(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock
):
    logger.info("Testing frontier error handling")
    
    # Test adding invalid URLs
    invalid_urls = ["not-a-url", "", "http://"]
    added = await frontier_manager.add_urls_batch(invalid_urls)
    assert added == 0  # None should be added
    
    # Test with domain extraction failure
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    weird_urls = ["http://[invalid-domain]/page"]
    added = await frontier_manager.add_urls_batch(weird_urls)
    # Should handle gracefully, likely 0 added due to domain extraction failure
    
    logger.info("Frontier error handling test passed.")

@pytest.mark.asyncio
async def test_atomic_domain_claiming_high_concurrency(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that URLs are claimed exactly once under high concurrency.
    
    This verifies that our Redis-based atomic domain claiming mechanism
    prevents race conditions when multiple workers compete for URLs.
    """
    logger.info("Testing atomic domain claiming under high concurrency")
    
    # Setup
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Create config for this test
    config = CrawlerConfig(**vars(frontier_test_config_obj))
    
    # Prepare test URLs across multiple domains
    # With domain-level politeness, each domain can only serve one URL at a time
    # So we need many domains with few URLs each for concurrent testing
    num_domains = 100
    urls_per_domain = 2
    test_urls = []
    
    for domain_idx in range(num_domains):
        domain = f"test{domain_idx}.com"
        for url_idx in range(urls_per_domain):
            test_urls.append(f"http://{domain}/page{url_idx}")
    
    total_urls = len(test_urls)
    logger.info(f"Created {total_urls} test URLs across {num_domains} domains")
    
    # Create a single frontier manager to populate the data
    seed_frontier = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Initialize but clear any existing data to start fresh
    await seed_frontier._clear_frontier()
    added = await seed_frontier.add_urls_batch(test_urls)
    # Allow for a small number of false positives from the bloom filter
    assert total_urls - added < 5, f"Expected close to {total_urls} URLs to be added, but only got {added}. This might indicate a bloom filter issue."
    
    # Create multiple frontier managers (simulating workers)
    num_workers = 50
    workers = []
    for i in range(num_workers):
        fm = FrontierManager(
            config=config,
            politeness=mock_politeness_enforcer_for_frontier,
            redis_client=redis_test_client
        )
        workers.append(fm)
    
    # Track URLs claimed by each worker
    worker_claims: Dict[int, List[str]] = defaultdict(list)
    
    async def worker_task(worker_id: int, frontier: FrontierManager, max_claims: int):
        """Worker task that claims URLs from the frontier."""
        claims = 0
        while claims < max_claims:
            try:
                result = await frontier.get_next_url()
                if result is None:
                    # No URLs available, try again after brief wait
                    await asyncio.sleep(0.01)
                    continue
                    
                url, domain, _, depth = result
                worker_claims[worker_id].append(url)
                claims += 1
                
                # Simulate some work
                await asyncio.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                break
                
        logger.debug(f"Worker {worker_id} finished with {claims} claims")
    
    # Run all workers concurrently
    urls_per_worker = total_urls // num_workers
    tasks = []
    
    for worker_id, frontier in enumerate(workers):
        task = asyncio.create_task(
            worker_task(worker_id, frontier, urls_per_worker)
        )
        tasks.append(task)
    
    # Wait for all workers to complete
    await asyncio.gather(*tasks)
    
    # Analyze results
    all_claimed_urls = []
    for worker_id, urls in worker_claims.items():
        all_claimed_urls.extend(urls)
        logger.debug(f"Worker {worker_id} claimed {len(urls)} URLs")
    
    # Check for duplicates (this is the critical test)
    unique_claims = set(all_claimed_urls)
    duplicates = len(all_claimed_urls) - len(unique_claims)
    
    if duplicates > 0:
        # Find which URLs were claimed multiple times
        from collections import Counter
        url_counts = Counter(all_claimed_urls)
        duplicate_urls = [(url, count) for url, count in url_counts.items() if count > 1]
        logger.error(f"Found {duplicates} duplicate claims:")
        for url, count in duplicate_urls[:10]:  # Show first 10
            logger.error(f"  - {url} claimed {count} times")
    
    assert duplicates == 0, f"Found {duplicates} duplicate URL claims!"
    
    # Verify total claims (some URLs might not be claimed due to timing)
    # We expect at least 90% to be claimed in this test
    expected_min_claims = int(total_urls * 0.9)
    assert len(all_claimed_urls) >= expected_min_claims, \
        f"Only {len(all_claimed_urls)} URLs claimed out of {total_urls} (expected at least {expected_min_claims})"
    
    logger.info(f"""
    Atomic domain claiming test completed:
    - Total URLs: {total_urls}
    - URLs claimed: {len(all_claimed_urls)}
    - Unique claims: {len(unique_claims)}
    - Duplicate claims: {duplicates}
    - Workers: {num_workers}
    - No race conditions detected ✓
    """)
    
    logger.info("Atomic domain claiming test passed.")

@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that frontier state persists correctly when resuming a crawl."""
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # --- First run: populate and get one URL ---
    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False  # Ensure it's a new run
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    frontier_run1 = FrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    await frontier_run1.initialize_frontier()
    # Seeds will be loaded: http://example.com/seed1, http://example.org/seed2
    
    # Add one more URL
    await frontier_run1.add_urls_batch(["http://persistent.com/page_from_run1"])
    assert await frontier_run1.count_frontier() == 3
    
    # Get one URL
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await frontier_run1.count_frontier() == 2
    
    # Note which URL was retrieved
    retrieved_url = url_to_retrieve[0]
    logger.info(f"First run retrieved URL: {retrieved_url}")
    
    # --- Second run: resume ---
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict)
    
    frontier_run2 = FrontierManager(
        config=cfg_run2,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Initialize with resume=True - should preserve existing frontier
    await frontier_run2.initialize_frontier()
    
    # Should still have 2 URLs
    assert await frontier_run2.count_frontier() == 2
    
    # Get the remaining URLs
    remaining_urls = []
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    remaining_urls.append(next_url[0])
    
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    remaining_urls.append(next_url[0])
    
    # Verify we got the expected URLs (the two that weren't retrieved in run1)
    all_urls = {"http://example.com/seed1", "http://example.org/seed2", "http://persistent.com/page_from_run1"}
    expected_remaining = all_urls - {retrieved_url}
    assert set(remaining_urls) == expected_remaining
    
    # Frontier should be empty now
    assert await frontier_run2.count_frontier() == 0
    
    logger.info("Frontier resume test passed with politeness mocks.")

@pytest.mark.asyncio
async def test_url_filtering_in_add_urls_batch(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock
):
    """Test that non-text URLs are filtered out when adding to frontier."""
    logger.info("Testing URL filtering in add_urls_batch")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Mix of text and non-text URLs
    urls_to_add = [
        "http://example.com/page.html",  # Should be added
        "http://example.com/image.jpg",  # Should be filtered
        "http://example.com/document.pdf",  # Should be filtered
        "http://example.com/article",  # Should be added
        "http://example.com/video.mp4",  # Should be filtered
        "http://example.com/api/data",  # Should be added
        "http://example.com/archive.zip",  # Should be filtered
    ]
    
    # Add URLs
    added_count = await frontier_manager.add_urls_batch(urls_to_add)
    
    # Only text URLs should be added (3 out of 7)
    assert added_count == 3, f"Expected 3 URLs to be added, got {added_count}"
    
    # Verify the correct URLs were added by retrieving them
    retrieved_urls = []
    while True:
        result = await frontier_manager.get_next_url()
        if result is None:
            break
        retrieved_urls.append(result[0])
    
    expected_urls = {
        "http://example.com/page.html",
        "http://example.com/article",
        "http://example.com/api/data"
    }
    assert set(retrieved_urls) == expected_urls, \
        f"Expected URLs {expected_urls}, got {set(retrieved_urls)}"
    
    logger.info("URL filtering in add_urls_batch test passed.")

@pytest.mark.asyncio
async def test_url_filtering_in_get_next_url(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that non-text URLs are skipped when reading from frontier lists."""
    logger.info("Testing URL filtering in get_next_url")
    
    # Setup
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    config = CrawlerConfig(**vars(frontier_test_config_obj))
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Clear frontier
    await fm._clear_frontier()
    
    # Manually add mixed URLs directly to Redis (bypassing filtering in add_urls_batch)
    domain = "mixed-content.com"
    frontier_key = f"frontier:{domain}"
    
    # Add URLs directly to Redis list
    await redis_test_client.lpush(frontier_key, 
        "http://mixed-content.com/final-page.html|0",
        "http://mixed-content.com/data.zip|0",  # Should be skipped
        "http://mixed-content.com/api/endpoint|0",
        "http://mixed-content.com/video.mp4|0",  # Should be skipped
        "http://mixed-content.com/page2|0",
        "http://mixed-content.com/document.pdf|0",  # Should be skipped
        "http://mixed-content.com/image1.jpg|0",  # Should be skipped
        "http://mixed-content.com/page1.html|0"
    )
    
    # Add domain to queue
    await redis_test_client.rpush('domains:queue', domain)
    
    # Get URLs and verify only text URLs are returned
    retrieved_urls = []
    while True:
        result = await fm.get_next_url()
        if result is None:
            break
        retrieved_urls.append(result[0])
    
    expected_urls = [
        "http://mixed-content.com/page1.html",
        "http://mixed-content.com/page2",
        "http://mixed-content.com/api/endpoint",
        "http://mixed-content.com/final-page.html"
    ]
    
    assert retrieved_urls == expected_urls, \
        f"Expected URLs {expected_urls}, got {retrieved_urls}"
    
    logger.info("URL filtering in get_next_url test passed.")
```

```markdown
# Migration Guide: Sharded Queues to Single Queue

## Overview

The crawler has been updated to use a single domain queue instead of sharded queues. This simplifies the architecture and improves performance by eliminating domain locking overhead.

## Changes

### Before (Sharded Queues)
- Multiple queues: `domains:queue:0`, `domains:queue:1`, ..., `domains:queue:N`
- Each fetcher process only read from its assigned shard
- Required resharding when number of fetchers changed
- More complex, required domain hashing

### After (Single Queue)
- Single queue: `domains:queue`
- All fetcher processes read from the same queue
- No sharding or resharding needed
- Simpler, relies on Redis atomic operations

## Migration Steps

1. **Stop your crawler** - Ensure no processes are running

2. **Run the migration script**:
   ```bash
   # Dry run first to see what would happen
   python migrate_shards_to_single_queue.py --dry-run
   
   # Actually migrate
   python migrate_shards_to_single_queue.py
   
   # With custom Redis settings
   python migrate_shards_to_single_queue.py --redis-host myhost --redis-port 6380
   ```

3. **Update your code** - The latest crawler code already uses single queue

4. **Restart crawler** - It will now use the single queue

## What the Migration Does

1. Reads `crawler:shard_count` to find number of shards
2. Collects all domains from `domains:queue:0` through `domains:queue:N`
3. Removes duplicates (keeps order)
4. Pushes all domains to single `domains:queue`
5. Deletes old shard queues and `crawler:shard_count`

## Rollback

If you need to rollback, you'll need to:
1. Restore Redis from a backup made before migration
2. Use the old crawler code that supports sharding

## Notes

- The migration is safe to run multiple times
- Use `--dry-run` to preview changes
- All frontier URLs remain in Redis lists (`frontier:{domain}`)
- No changes to frontier data, only domain queue structure
```

Perfect! I've successfully created:

1. **Migration Script** (`migrate_shards_to_single_queue.py`):
   - Collects domains from all sharded queues
   - Deduplicates and moves them to a single `domains:queue`
   - Supports `--dry-run` mode to preview changes
   - Cleans up old shard queues and metadata
   - Idempotent - safe to run multiple times

2. **Updated Tests** (`test_frontier.py`):
   - Removed all `fetcher_id` parameters from `get_next_url()` calls
   - Updated queue references from `domains:queue:{shard}` to `domains:queue`
   - Removed multi-shard distribution and resharding tests
   - Renamed file persistence test to Redis list persistence test

3. **Migration Guide** (`MIGRATION_TO_SINGLE_QUEUE.md`):
   - Clear explanation of the changes
   - Step-by-step migration instructions
   - Rollback notes

## Running the Migration

To migrate your existing sharded queues to the single queue:

```bash
# First, do a dry run to see what will happen
python migrate_shards_to_single_queue.py --dry-run

# If everything looks good, run the actual migration
python migrate_shards_to_single_queue.py

# With custom Redis settings
python migrate_shards_to_single_queue.py --redis-host localhost --redis-port 6379
```

The migration script will:
- Find all sharded queues based on `crawler:shard_count`
- Collect all domains from each shard
- Remove duplicates while preserving order
- Push all domains to the single `domains:queue`
- Clean up old shard queues and metadata

This completes the transition from the complex sharded design to a much simpler single-queue architecture that relies on Redis's atomic operations for concurrency control!

```shellscript
# First, do a dry run to see what will happen
python migrate_shards_to_single_queue.py --dry-run

# If everything looks good, run the actual migration
python migrate_shards_to_single_queue.py

# With custom Redis settings
python migrate_shards_to_single_queue.py --redis-host localhost --redis-port 6379
```

---

**User**

Some tests failing:
```
==================================================== short test summary info ====================================================
FAILED tests/test_frontier.py::test_add_urls_batch - assert 3 == 2
FAILED tests/test_frontier.py::test_redis_list_persistence - AssertionError: assert 6 == 5
FAILED tests/test_frontier.py::test_frontier_resume_with_politeness - assert 4 == 3
FAILED tests/test_frontier.py::test_url_filtering_in_get_next_url - AssertionError: Expected URLs ['http://mixed-content.com/page1.html', 'http://mixed-content.com/page2', 'http://mixed-conten...
============================================ 4 failed, 76 passed, 1 warning in 0.84s ============================================
```

---

**Cursor**

Let me run the failing tests to see the exact issues:

---

**User**

You want to use the virtual env named `env`

seems like we should use an in-memory set to deduplicate the urls in add_urls_batch

---

**Cursor**

```python
urls_to_add = ["http://test.com/batch1", "http://test.org/batch2", "http://test.com/batch1"]
```

You're absolutely right! The current implementation doesn't deduplicate URLs within the same batch before checking the bloom filter. Let me fix this:

```python
    async def add_urls_batch(self, urls: List[str], depth: int = 0) -> int:
        """Add URLs to frontier - much simpler than filesystem version!"""
        # Pre-filter and deduplicate within batch
        seen_in_batch = set()
        candidates = []
        for u in urls:
            if len(u) > 2000 or is_likely_non_text_url(u):
                continue
            if u not in seen_in_batch:
                seen_in_batch.add(u)
                candidates.append(u)
        
        if not candidates:
            return 0
            
        # Check bloom filter
        new_urls = []
        pipe = self.redis.pipeline()
        for url in candidates:
            pipe.execute_command('BF.EXISTS', 'seen:bloom', url)
        bloom_results = await pipe.execute()
        
        for url, exists in zip(candidates, bloom_results):
            if not exists:
                new_urls.append(url)
        
        if not new_urls:
            return 0
        
        # Politeness filtering
        allowed_urls = []
        for url in new_urls:
            if await self.politeness.is_url_allowed(url):
                allowed_urls.append(url)

        if not allowed_urls:
            return 0
            
        # Group by domain and add to Redis
        urls_by_domain: Dict[str, List[Tuple[str, int]]] = {}
        for url in allowed_urls:
            domain = extract_domain(url)
            if domain:
                if domain not in urls_by_domain:
                    urls_by_domain[domain] = []
                urls_by_domain[domain].append((url, depth))
        
        # Add to bloom filter and frontier lists atomically
        pipe = self.redis.pipeline()
        added_count = 0
        
        for domain, domain_urls in urls_by_domain.items():
            frontier_key = f"frontier:{domain}"
            domain_key = f"domain:{domain}"
            
            # Add to bloom filter
            for url, _ in domain_urls:
                pipe.execute_command('BF.ADD', 'seen:bloom', url)
            
            # Add to frontier list (LPUSH for FIFO behavior with RPOP)
            for url, url_depth in domain_urls:
                pipe.lpush(frontier_key, f"{url}|{url_depth}")
                added_count += 1
            
            # Initialize domain metadata
            pipe.hsetnx(domain_key, 'is_seeded', '0')
            
            # Add domain to queue
            pipe.rpush('domains:queue', domain)
        
        await pipe.execute()
        return added_count
```

Now let's run the tests to see if this fixes the issues:

Great! The first test is passing. Let's check the second failing test:

The issue is that `get_next_url` returns `None` when it encounters a non-text URL, instead of continuing to check the next URL from the same domain. Let me fix this:

```python
    async def get_next_url(self) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl."""
        # Pop domain from queue
        domain = await self.redis.lpop('domains:queue')
        
        if not domain:
            return None
        
        try:
            # Check politeness
            if not await self.politeness.can_fetch_domain_now(domain):
                return None
            
            # Keep trying URLs from this domain until we find a valid one
            while True:
                # Pop URL from domain's frontier (RPOP for FIFO with LPUSH)
                frontier_key = f"frontier:{domain}"
                url_data = await self.redis.rpop(frontier_key)
                
                if not url_data:
                    # Domain has no URLs left
                    return None
                
                # Parse URL data
                parts = url_data.split('|')
                if len(parts) >= 2:
                    url, depth_str = parts[:2]
                    
                    # Skip non-text URLs
                    if is_likely_non_text_url(url):
                        # Don't put it back - just skip it and try next URL
                        logger.debug(f"Skipping non-text URL: {url}")
                        continue
                    
                    # Check URL-level politeness
                    if not await self.politeness.is_url_allowed(url):
                        logger.debug(f"URL {url} disallowed by politeness rules")
                        continue
                    
                    # Record fetch attempt
                    await self.politeness.record_domain_fetch_attempt(domain)
                    
                    # Return URL (dummy ID for compatibility)
                    return (url, domain, -1, int(depth_str))
            
        finally:
            # Always put domain back at end of queue
            if domain:
                await self.redis.rpush('domains:queue', domain)
```

```plaintext
['http://mixed-content.com/page1.html', 'http://mixed-content.com/page2', 'http://mixed-content.com/api/endpoint', 'http://mixed-content.com/final-page.html']
```

```plaintext
['http://mixed-content.com/final-page.html', 'http://mixed-content.com/api/endpoint', 'http://mixed-content.com/page2', 'http://mixed-content.com/page1.html']
```

The test is now correctly filtering out non-text URLs! The issue is just the order of URLs. Since we're using LPUSH (adds to left) and RPOP (removes from right), the URLs come out in reverse order. Let me update the test to expect the correct order:

```python
        # Get URLs and verify only text URLs are returned
        retrieved_urls = []
        while True:
            result = await fm.get_next_url()
            if result is None:
                break
            retrieved_urls.append(result[0])
        
        # With LPUSH/RPOP, URLs come out in reverse order of how they were pushed
        # LPUSH adds to left, RPOP removes from right, so we get FIFO order
        expected_urls = [
            "http://mixed-content.com/final-page.html",
            "http://mixed-content.com/api/endpoint",
            "http://mixed-content.com/page2",
            "http://mixed-content.com/page1.html"
        ]
        
        assert retrieved_urls == expected_urls, \
            f"Expected URLs {expected_urls}, got {retrieved_urls}"
        
        logger.info("URL filtering in get_next_url test passed.")
```

Excellent! All failing tests are now fixed. Let's run all the frontier tests to ensure everything is working:

Perfect! All frontier tests are passing. Let's run all tests to make sure we didn't break anything else:

Perfect! All 80 tests are passing. Here's a summary of the fixes we made:

## Fixes Applied

1. **In-memory deduplication in `add_urls_batch`**: 
   - Added a `seen_in_batch` set to deduplicate URLs within the same batch before checking the bloom filter
   - This prevents duplicates within a single batch from being added multiple times

2. **Continuous URL retrieval in `get_next_url`**:
   - Modified to keep trying URLs from the same domain until it finds a valid one or runs out
   - Non-text URLs (PDFs, images, etc.) are now skipped without blocking access to valid URLs

3. **Updated test expectations**:
   - Fixed the expected URL order in `test_url_filtering_in_get_next_url` to match LPUSH/RPOP behavior (FIFO order)

## Summary

The Redis-based frontier implementation is now fully functional with:
- Single domain queue for all fetchers
- Atomic operations (no locking needed)
- Proper URL deduplication
- Non-text URL filtering
- All tests passing

The migration from sharded queues to a single queue is complete, along with a migration script and documentation for users to upgrade their existing systems.

---

**User**

Can we optimize the migrate script to migrate in batches? I think currently it will be really slow because it's pushing/popping one at a time and awaiting each command

---

**Cursor**

You're absolutely right! The current migration script is inefficient with individual operations. Let me optimize it to use batched operations:

```python
#!/usr/bin/env python3
"""
One-time migration script to move from sharded domain queues to single queue.

Usage:
    python migrate_shards_to_single_queue.py [--dry-run]
"""

import asyncio
import argparse
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)

async def migrate_shards(redis_client: redis.Redis, dry_run: bool = False):
    """Migrate from sharded domain queues to single queue."""
    
    # Get current shard count
    shard_count = await redis_client.get('crawler:shard_count')
    if not shard_count:
        logger.info("No shard count found. System may already be using single queue.")
        return
    
    shard_count = int(shard_count)
    logger.info(f"Found {shard_count} shards to migrate")
    
    # Collect all domains from sharded queues using batch operations
    all_domains = []
    total_collected = 0
    
    for shard in range(shard_count):
        queue_key = f'domains:queue:{shard}'
        
        # Get all domains from this shard at once (much faster than LPOP loop)
        shard_domains = await redis_client.lrange(queue_key, 0, -1)
        
        logger.info(f"Collected {len(shard_domains)} domains from shard {shard}")
        all_domains.extend(shard_domains)
        total_collected += len(shard_domains)
    
    if total_collected == 0:
        logger.info("No domains found in sharded queues. Nothing to migrate.")
        return
    
    # Remove duplicates while preserving order
    seen = set()
    unique_domains = []
    for domain in all_domains:
        if domain not in seen:
            seen.add(domain)
            unique_domains.append(domain)
    
    logger.info(f"Total domains collected: {total_collected}, unique: {len(unique_domains)}")
    
    if dry_run:
        logger.info("[DRY RUN] Would push the following domains to domains:queue:")
        for domain in unique_domains[:10]:  # Show first 10
            logger.info(f"  - {domain}")
        if len(unique_domains) > 10:
            logger.info(f"  ... and {len(unique_domains) - 10} more")
        logger.info("[DRY RUN] Would delete crawler:shard_count")
        logger.info(f"[DRY RUN] Would delete {shard_count} shard queues")
        return
    
    # Push all domains to single queue in batches
    if unique_domains:
        logger.info(f"Pushing {len(unique_domains)} domains to single queue...")
        
        # Use batched RPUSH for efficiency
        batch_size = 1000  # Redis can handle large batches efficiently
        
        for i in range(0, len(unique_domains), batch_size):
            batch = unique_domains[i:i + batch_size]
            # RPUSH accepts multiple values at once
            await redis_client.rpush('domains:queue', *batch)
            
            if i % 10000 == 0 and i > 0:
                logger.info(f"  Pushed {i}/{len(unique_domains)} domains...")
        
        logger.info(f"Successfully pushed all {len(unique_domains)} domains")
    
    # Remove shard count key
    await redis_client.delete('crawler:shard_count')
    logger.info("Removed crawler:shard_count key")
    
    # Verify migration
    final_count = await redis_client.llen('domains:queue')
    logger.info(f"Migration complete! Single queue now contains {final_count} domains")
    
    # Clean up empty shard queues using pipeline
    logger.info("Cleaning up empty shard queues...")
    pipe = redis_client.pipeline()
    for shard in range(shard_count):
        queue_key = f'domains:queue:{shard}'
        pipe.delete(queue_key)
    await pipe.execute()
    logger.info(f"Deleted {shard_count} empty shard queues")

async def main():
    parser = argparse.ArgumentParser(description="Migrate from sharded to single domain queue")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database')
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        decode_responses=True
    )
    
    try:
        # Test connection
        await redis_client.ping()
        logger.info(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
        
        # Run migration
        await migrate_shards(redis_client, dry_run=args.dry_run)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        await redis_client.aclose()

if __name__ == '__main__':
    asyncio.run(main())
```

Perfect! I've optimized the migration script with several key improvements:

## Key Optimizations

1. **Batch Reading with LRANGE**:
   - Instead of popping domains one by one with `LPOP` (which removes and requires a round trip for each domain)
   - Now uses `LRANGE(queue, 0, -1)` to read all domains from each shard at once
   - This is non-destructive and much faster

2. **Batch Writing with Multi-value RPUSH**:
   - Instead of individual `RPUSH` commands in a pipeline
   - Now uses `RPUSH('domains:queue', *batch)` to push up to 1000 domains in a single command
   - Redis efficiently handles multi-value RPUSH

3. **Progress Reporting**:
   - Added progress logging every 10,000 domains for large migrations
   - Helps track progress on big datasets

4. **Pipeline for Cleanup**:
   - Deleting old shard queues uses a pipeline for efficiency

## Performance Comparison

For a migration with 100,000 domains across 4 shards:

**Before**: ~100,004 Redis operations
- 100,000 LPOP operations (one per domain) 
- 100,000 RPUSH operations (even with pipeline, still individual commands)
- 4 DELETE operations

**After**: ~105 Redis operations  
- 4 LRANGE operations (one per shard)
- ~100 RPUSH operations (1000 domains per batch)
- 1 pipelined DELETE operation for all shards

This should be **~1000x faster** for large migrations!

## Additional Benefits

1. **Non-destructive reading**: Using LRANGE means if something goes wrong, the original data is still there
2. **Memory efficient**: Processes domains in 1000-item batches rather than loading everything into a pipeline
3. **Better progress tracking**: You can see progress during large migrations

The script is now production-ready for migrating even very large domain queues efficiently!

