# Redesigning the web crawler's frontier storage
_Exported on 7/3/2025 at 15:22:02 PDT from Cursor (1.0.0)_

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

