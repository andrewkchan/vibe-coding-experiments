import logging
import time
from pathlib import Path
from typing import Set, Optional, Tuple, List, Dict
import asyncio
import redis.asyncio as redis
import aiofiles  # type: ignore
import hashlib
import shutil

from .config import CrawlerConfig
from .storage import StorageManager
from .utils import extract_domain, normalize_url
from .politeness import PolitenessEnforcer
from .redis_lock import LockManager, DomainLock

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
    # Extract the path component, ignoring query string and fragment
    try:
        # Find the path part before any ? or #
        path = url.split('?')[0].split('#')[0]
        # Get the last part of the path
        last_part = path.rstrip('/').split('/')[-1]
        # Check if it has an extension
        if '.' in last_part:
            # Get the extension (everything after the last dot)
            ext = '.' + last_part.split('.')[-1].lower()
            return ext in NON_TEXT_EXTENSIONS
    except Exception:
        # If we can't parse the URL, assume it's okay to crawl
        pass
    return False

class FrontierManager:
    """Redis + File-based frontier manager for high-performance crawling.
    
    Uses Redis for coordination and metadata, with file-based storage for actual frontier URLs.
    This gives us Redis performance for the hot path while keeping memory usage reasonable.
    """
    
    def __init__(self, config: CrawlerConfig, politeness: PolitenessEnforcer, redis_client: redis.Redis):
        self.config = config
        self.politeness = politeness
        self.redis = redis_client
        self.frontier_dir = config.data_dir / "frontiers"
        self.frontier_dir.mkdir(exist_ok=True)
        self.lock_manager = LockManager(redis_client)
        # Process-local locks for reading - no need for Redis round-trips
        self._read_locks: Dict[str, asyncio.Lock] = {}
        # Process-local write locks (only used if num_parser_processes == 1)
        self._write_locks: Dict[str, asyncio.Lock] = {}
        self._shard_count: Optional[int] = None  # Cached shard count
        
    def _get_frontier_path(self, domain: str) -> Path:
        """Get file path for domain's frontier."""
        # Use first 2 chars of hash for subdirectory (256 subdirs)
        domain_hash = hashlib.md5(domain.encode()).hexdigest()
        subdir = domain_hash[:2]
        path = self.frontier_dir / subdir / f"{domain}.frontier"
        path.parent.mkdir(exist_ok=True)
        return path
    
    def _get_read_lock(self, domain: str) -> asyncio.Lock:
        """Get process-local read lock for domain."""
        if domain not in self._read_locks:
            self._read_locks[domain] = asyncio.Lock()
        # Note: In a long-running crawler with millions of domains, this dict could grow large.
        # Consider implementing an LRU cache or periodic cleanup if memory becomes an issue.
        return self._read_locks[domain]

    def _get_write_lock(self, domain: str) -> asyncio.Lock | DomainLock:
        """Get process-local write lock for domain."""
        if self.config.num_parser_processes > 1:
            # Use Redis-based lock for cross-process safety
            domain_lock = self.lock_manager.get_domain_write_lock(domain)
            return domain_lock
        if domain not in self._write_locks:
            self._write_locks[domain] = asyncio.Lock()
        return self._write_locks[domain]
    
    async def _get_shard_count(self) -> int:
        """Get the total number of shards."""
        if self._shard_count is None:
            count = await self.redis.get('crawler:shard_count')
            self._shard_count = int(count) if count else 1
        return self._shard_count
    
    def _get_domain_shard(self, domain: str) -> int:
        """Get the shard number for a domain using simple modulo hashing.
        
        With 1 shard per fetcher, shard assignment is straightforward:
        fetcher 0 handles shard 0, fetcher 1 handles shard 1, etc.
        """
        # Use MD5 hash for consistent hashing across processes
        # Python's built-in hash() is not consistent across runs/processes
        domain_hash = hashlib.md5(domain.encode()).digest()
        # Convert first 8 bytes to integer for modulo operation
        hash_int = int.from_bytes(domain_hash[:8], byteorder='big')
        return hash_int % (self._shard_count or 1)
    
    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
        # Load shard count
        await self._get_shard_count()
        logger.info(f"Frontier using {self._shard_count} shard(s)")
        
        # Initialize bloom filter if it doesn't exist
        try:
            # Try to get bloom filter info - will fail if it doesn't exist
            await self.redis.execute_command('BF.INFO', 'seen:bloom')
            logger.info("Bloom filter already exists, using existing filter")
        except:
            # Create bloom filter for seen URLs
            # Estimate: visited + frontier + some growth room (default to 10M for new crawls)
            try:
                await self.redis.execute_command(
                    'BF.RESERVE', 'seen:bloom', 0.001, 160_000_000
                )
                logger.info("Created new bloom filter for 160M URLs with 0.1% FPR")
            except:
                logger.warning("Could not create bloom filter - it may already exist")
        
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
        # Clear Redis structures
        pipe = self.redis.pipeline()
        
        # Clear domain metadata and ready queue
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor, match='domain:*', count=1000
            )
            if keys:
                pipe.delete(*keys)
            if cursor == 0:
                break
        
        # Clear all sharded queues
        shard_count = await self._get_shard_count()
        for shard in range(shard_count):
            pipe.delete(f'domains:queue:{shard}')
        
        # Also clear old single queue if it exists
        pipe.delete('domains:queue')
        await pipe.execute()
        
        # Clear frontier files
        if self.frontier_dir.exists():
            shutil.rmtree(self.frontier_dir)
            self.frontier_dir.mkdir(exist_ok=True)
            
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
            await self._mark_domains_as_seeded_batch(list(seed_domains))
            
            # Add URLs to frontier
            logger.info(f"Adding {len(urls)} URLs to the frontier")
            async def add_urls_batch_worker(worker_id: int, urls: list[str]) -> int:
                subchunk_size = 100
                added_count = 0
                for i in range(0, len(urls), subchunk_size):
                    added_count += await self.add_urls_batch(urls[i:i+subchunk_size])
                    logger.info(f"Worker {worker_id} seeded {added_count}/{len(urls)} URLs")
                return added_count
            seed_tasks = []
            chunk_size = (len(urls) + self.config.fetcher_workers - 1) // self.config.fetcher_workers
            for i in range(self.config.fetcher_workers):
                url_chunk = urls[i*chunk_size:(i+1)*chunk_size]
                seed_tasks.append(add_urls_batch_worker(i, url_chunk))
            added_counts = await asyncio.gather(*seed_tasks)
            added_count = sum(added_counts)
            logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
            
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")
    
    async def _mark_domains_as_seeded_batch(self, domains: List[str]):
        """Mark domains as seeded in domain metadata."""
        if not domains:
            return
            
        pipe = self.redis.pipeline()
        for i, domain in enumerate(domains):
            pipe.hset(f'domain:{domain}', 'is_seeded', '1')
            if i % 10_000 == 0:
                logger.info(f"Mark {i}/{len(domains)} domains as seeded")
        await pipe.execute()
        logger.debug(f"Marked {len(domains)} domains as seeded")
    
    async def add_urls_batch(self, urls: List[str], depth: int = 0) -> int:
        """Add URLs to frontier files."""
        # 1. Pre-filter
        candidates = set()
        for u in urls:
            # Skip URLs that are too long (> 2000 chars)
            if len(u) > 2000:
                logger.debug(f"Skipping URL longer than 2000 chars: {u[:100]}...")
                continue
            # Skip non-text URLs early
            if is_likely_non_text_url(u):
                logger.debug(f"Skipping non-text URL during add: {u}")
                continue
            candidates.add(u)
        
        if not candidates:
            return 0
            
        # 2. Check against bloom filter
        # Note we check the bloom filter twice; once here and once in _add_urls_to_domain (which adds to the filter).
        # This redundant check is a performance optimization because checking politeness below can be expensive.
        # TODO: Validate that this actually improves performance.
        new_urls = []
        candidate_list = list(candidates)
        
        pipe = self.redis.pipeline()
        for url in candidate_list:
            pipe.execute_command('BF.EXISTS', 'seen:bloom', url)
        bloom_results = await pipe.execute()
        for url, exists in zip(candidate_list, bloom_results):
            if not exists:
                new_urls.append(url)
        
        if not new_urls:
            return 0
        
        # 3. Politeness filtering
        allowed_urls = []
        for url in new_urls:
            if await self.politeness.is_url_allowed(url):
                allowed_urls.append(url)

        if not allowed_urls:
            return 0
            
        # 4. Group URLs by domain
        urls_by_domain: Dict[str, List[Tuple[str, int]]] = {}
        for url in allowed_urls:
            domain = extract_domain(url)
            if domain:
                if domain not in urls_by_domain:
                    urls_by_domain[domain] = []
                urls_by_domain[domain].append((url, depth))
                
        # 5. Add URLs to domain frontier files
        added_total = 0
        for domain, domain_urls in urls_by_domain.items():
            added = await self._add_urls_to_domain(domain, domain_urls)
            added_total += added
            
        return added_total
    
    async def _add_urls_to_domain(self, domain: str, urls: List[Tuple[str, int]]) -> int:
        """Add URLs to a specific domain's frontier file."""
        domain_lock = self._get_write_lock(domain)
        
        try:
            async with domain_lock:
                frontier_path = self._get_frontier_path(domain)
                domain_key = f"domain:{domain}"
                
                # Batch add all URLs to bloom filter
                pipe = self.redis.pipeline()
                for url, depth in urls:
                    pipe.execute_command('BF.ADD', 'seen:bloom', url)
                bloom_add_results = await pipe.execute()
                
                # Filter out URLs that were already in bloom filter
                new_urls = []
                for (url, depth), was_new in zip(urls, bloom_add_results):
                    if was_new:  # BF.ADD returns 1 if new, 0 if already existed
                        new_urls.append((url, depth))
                
                if not new_urls:
                    return 0
                    
                # Append to frontier file
                lines_to_write = []
                for url, depth in new_urls:
                    # Format: url|depth
                    line = f"{url}|{depth}\n"
                    lines_to_write.append(line)
                    
                async with aiofiles.open(frontier_path, 'a') as f:
                    await f.writelines(lines_to_write)
                    
                # Get file size after writing
                new_size_bytes = frontier_path.stat().st_size
                    
                # Update Redis metadata
                pipe = self.redis.pipeline()
                
                # Update metadata - we'll preserve is_seeded if it exists
                pipe.hset(domain_key, mapping={
                    'frontier_size': str(new_size_bytes),
                    'file_path': str(frontier_path.relative_to(self.frontier_dir))
                })
                
                # Only set is_seeded to '0' if it doesn't exist
                pipe.hsetnx(domain_key, 'is_seeded', '0')
                
                # Initialize offset if needed (0 bytes)
                pipe.hsetnx(domain_key, 'frontier_offset', '0')
                
                # Add domain to the correct shard queue
                # Note: This might add duplicates, but we handle that when popping
                shard = self._get_domain_shard(domain)
                pipe.rpush(f'domains:queue:{shard}', domain)
                
                await pipe.execute()
                
                return len(new_urls)
                
        except TimeoutError:
            logger.error(f"Failed to acquire lock for domain {domain} - another process may be stuck")
            return 0
        except Exception as e:
            logger.error(f"Error adding URLs to domain {domain}: {e}")
            return 0
    
    async def is_empty(self) -> bool:
        """Check if frontier is empty."""
        # Check if there are any domains in any shard queue
        shard_count = await self._get_shard_count()
        
        pipe = self.redis.pipeline()
        for shard in range(shard_count):
            pipe.llen(f'domains:queue:{shard}')
        
        counts = await pipe.execute()
        total_count = sum(counts)
        
        return total_count == 0
    
    async def count_frontier(self) -> int:
        """Estimate the number of URLs in the frontier."""
        total = 0
        cursor = 0
        
        # Sum up frontier sizes from all domains
        while True:
            cursor, keys = await self.redis.scan(
                cursor, match='domain:*', count=1000
            )
            
            if keys:
                pipe = self.redis.pipeline()
                for key in keys:
                    pipe.hget(key, 'frontier_size')
                    pipe.hget(key, 'frontier_offset')
                    
                results = await pipe.execute()
                
                # Process results in pairs (size, offset)
                for i in range(0, len(results), 2):
                    size_bytes = int(results[i] or 0)
                    offset_bytes = int(results[i + 1] or 0)
                    # If we haven't consumed all bytes, there are URLs remaining
                    # We can't know exact count without reading the file, but we know if any remain
                    if size_bytes > offset_bytes:
                        # This is an approximation - assumes 100 bytes per URL on average
                        # This method is noted as flawed and not used in production
                        remaining_bytes = size_bytes - offset_bytes
                        estimated_urls = remaining_bytes // 100
                        total += max(1, estimated_urls)  # At least 1 if bytes remain
            
            # Exit when cursor returns to 0
            if cursor == 0:
                break
                    
        return total
    
    async def get_next_url(self, fetcher_id: int = 0) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl.
        
        Args:
            fetcher_id: ID of this fetcher process (0-based), which directly maps to shard number
        
        Returns None if no URLs are available OR if politeness rules prevent fetching.
        The caller is responsible for retrying.
        """
        # With 1 shard per fetcher, this fetcher only handles its own shard
        shard = fetcher_id
        
        # Verify shard exists (in case of misconfiguration)
        shard_count = await self._get_shard_count()
        if shard >= shard_count:
            logger.error(f"Fetcher {fetcher_id} has no shard to handle (only {shard_count} shards exist)")
            return None
        # Atomically pop a domain from the front of the shard queue
        domain = await self.redis.lpop(f'domains:queue:{shard}')  # type: ignore[misc]
        
        if not domain:
            return None  # This shard is empty
        
        try:
            # Check if we can fetch from this domain now
            # This is where PolitenessEnforcer decides based on its rules
            if not await self.politeness.can_fetch_domain_now(domain):
                # Domain not ready
                return None
                
            # Get URL from this domain
            url_data = await self._get_url_from_domain(domain)
            
            if url_data:
                url, extracted_domain, depth = url_data
                
                # Double-check URL-level politeness rules
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules")
                    # Put domain back, caller decides whether to retry
                    return None
                    
                # Record the fetch attempt in PolitenessEnforcer
                await self.politeness.record_domain_fetch_attempt(domain)
                
                # Return URL with a dummy ID (for interface compatibility)
                return (url, domain, -1, depth)
        finally:
            # Domain always goes back to the end of the same shard queue
            if domain:  # Only push back if we actually got a domain
                await self.redis.rpush(f'domains:queue:{shard}', domain)  # type: ignore[misc]
        
        return None
    
    async def _get_url_from_domain(self, domain: str) -> Optional[Tuple[str, str, int]]:
        """Read next URL from domain's frontier file."""
        domain_key = f"domain:{domain}"
        
        try:
            # Use process-local lock for reading instead of Redis lock
            domain_lock = self._get_read_lock(domain)
            async with domain_lock:
                # Get file info from Redis
                file_info = await self.redis.hmget(  # type: ignore[misc]
                    domain_key, 
                    ['file_path', 'frontier_offset', 'frontier_size']
                )
                
                if not file_info[0]:  # No file path
                    return None
                    
                file_path = self.frontier_dir / file_info[0]
                offset_bytes = int(file_info[1] or 0)
                size_bytes = int(file_info[2] or 0)
                
                if offset_bytes >= size_bytes:  # All URLs consumed
                    return None
                
                # Read URLs from file until we find a text URL
                async with aiofiles.open(file_path, 'r') as f:
                    # Seek to the byte offset
                    await f.seek(offset_bytes)
                    
                    # Keep reading lines until we find a text URL
                    skipped_count = 0
                    while True:
                        # Read one line from current position
                        line = await f.readline()
                        
                        if not line:  # End of file
                            # Update offset to end of file if we skipped any URLs
                            if skipped_count > 0:
                                new_offset = await f.tell()
                                await self.redis.hset(domain_key, 'frontier_offset', str(new_offset))  # type: ignore[misc]
                                logger.debug(f"Reached end of frontier file for {domain} after skipping {skipped_count} non-text URLs")
                            return None
                            
                        line = line.strip()
                        if not line:  # Empty line
                            continue
                            
                        # Parse URL data
                        parts = line.split('|')
                        if len(parts) >= 2:
                            url, depth_str = parts[:2]
                            
                            # Check if this looks like a non-text URL
                            if is_likely_non_text_url(url):
                                skipped_count += 1
                                if skipped_count % 100 == 0:  # Log every 100 skipped
                                    logger.debug(f"Skipped {skipped_count} non-text URLs for domain {domain}")
                                continue  # Skip this URL and try the next one
                                
                            # Found a text URL! Update offset and return it
                            new_offset_bytes = await f.tell()
                            await self.redis.hset(domain_key, 'frontier_offset', str(new_offset_bytes))  # type: ignore[misc]
                            
                            if skipped_count > 0:
                                logger.debug(f"Skipped {skipped_count} non-text URLs before finding text URL: {url}")
                            
                            return url, domain, int(depth_str)
        except TimeoutError:
            logger.error(f"Failed to acquire lock for domain {domain} when reading URL")
            return None
        except Exception as e:
            logger.error(f"Error getting URL from domain {domain}: {e}")
            return None
            
        return None