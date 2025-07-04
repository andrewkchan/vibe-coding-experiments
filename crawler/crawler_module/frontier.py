"""Redis-based frontier manager for high-performance crawling.

Stores all frontier URLs directly in Redis for maximum performance.
"""

import logging
from typing import Optional, Tuple, List, Dict
import asyncio
import redis.asyncio as redis

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

class FrontierManager:
    """Redis frontier manager.
    
    All operations are atomic Redis commands, no locking needed.
    """
    
    def __init__(self, config: CrawlerConfig, politeness: PolitenessEnforcer, redis_client: redis.Redis):
        self.config = config
        self.politeness = politeness
        self.redis = redis_client

    
    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
        logger.info("Initializing Redis frontier")
        
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
        
        # Clear domain queue
        pipe.delete('domains:queue')
        
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
            # Most domains don't have robots.txt, so batch size can be large
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
        """Add URLs to frontier"""
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
            
        # Check against bloom filter
        # Note we check the bloom filter twice; once here and once in _add_urls_to_domain (which adds to the filter).
        # This redundant check is a performance optimization because checking politeness below can be expensive.
        # TODO: Validate that this actually improves performance.
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
        count = await self.redis.llen('domains:queue')
        return count == 0 