import logging
import time
from pathlib import Path
from typing import Set, Optional, Tuple, List, Dict
import asyncio
import os
import redis.asyncio as redis
import aiofiles  # type: ignore
import hashlib
import json
import shutil

from .config import CrawlerConfig
from .storage import StorageManager
from .utils import normalize_url, extract_domain
from .politeness import PolitenessEnforcer

logger = logging.getLogger(__name__)

class FrontierManager:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, politeness: PolitenessEnforcer):
        self.config = config
        self.storage = storage
        self.politeness = politeness
        self.seen_urls: Set[str] = set()  # In-memory set for quick checks during a session

    async def _populate_seen_urls_from_db(self) -> Set[str]:
        """Loads all URLs from visited_urls and frontier tables in chunks to avoid OOM."""
        seen = set()
        chunk_size = 1_000_000
        
        try:
            # Load from visited_urls in chunks
            offset = 0
            visited_count = 0
            while True:
                if self.storage.config.db_type == "postgresql":
                    rows = await self.storage.db.fetch_all(
                        "SELECT url FROM visited_urls ORDER BY url_sha256 LIMIT %s OFFSET %s",
                        (chunk_size, offset),
                        query_name="populate_seen_from_visited_chunked"
                    )
                else:  # SQLite
                    rows = await self.storage.db.fetch_all(
                        "SELECT url FROM visited_urls ORDER BY url_sha256 LIMIT ? OFFSET ?",
                        (chunk_size, offset),
                        query_name="populate_seen_from_visited_chunked"
                    )
                
                if not rows:
                    break
                    
                for row in rows:
                    seen.add(row[0])
                
                visited_count += len(rows)
                offset += chunk_size
                
                if len(rows) < chunk_size:
                    break
                    
                # Log progress for large datasets
                if visited_count % 1_000_000 == 0:
                    logger.info(f"Progress: Loaded {visited_count} URLs from visited_urls...")
            
            logger.info(f"Loaded {visited_count} URLs from visited_urls table")
            
            # Load from current frontier in chunks (in case of resume)
            offset = 0
            frontier_count = 0
            while True:
                if self.storage.config.db_type == "postgresql":
                    rows = await self.storage.db.fetch_all(
                        "SELECT url FROM frontier ORDER BY id LIMIT %s OFFSET %s",
                        (chunk_size, offset),
                        query_name="populate_seen_from_frontier_chunked"
                    )
                else:  # SQLite
                    rows = await self.storage.db.fetch_all(
                        "SELECT url FROM frontier ORDER BY id LIMIT ? OFFSET ?",
                        (chunk_size, offset),
                        query_name="populate_seen_from_frontier_chunked"
                    )
                
                if not rows:
                    break
                    
                for row in rows:
                    seen.add(row[0])
                
                frontier_count += len(rows)
                offset += chunk_size
                
                if len(rows) < chunk_size:
                    break
                    
                # Log progress for large datasets
                if frontier_count % 1_000_000 == 0:
                    logger.info(f"Progress: Loaded {frontier_count} URLs from frontier...")
            
            logger.info(f"Loaded {frontier_count} URLs from frontier table")
            logger.info(f"Total: Loaded {len(seen)} unique URLs for seen_urls set")
            
        except Exception as e:
            logger.error(f"Error loading seen_urls set: {e}")
            raise  # Re-raise to handle the error at a higher level
        
        return seen

    async def initialize_frontier(self):
        """Loads seed URLs and previously saved frontier URLs if resuming."""
        # Populate seen_urls from DB
        self.seen_urls = await self._populate_seen_urls_from_db()
        logger.info(f"Initialized seen_urls with {len(self.seen_urls)} URLs from DB.")

        if self.config.resume:
            count = await self.count_frontier()
            logger.info(f"Resuming crawl. Frontier has {count} URLs.")
            if count == 0:
                logger.warning("Resuming with an empty frontier. Attempting to load seeds.")
                await self._load_seeds()
        else:
            logger.info("Starting new crawl. Clearing any existing frontier and loading seeds.")
            await self._clear_frontier_db()
            await self._load_seeds()

    async def _mark_domains_as_seeded_batch(self, domains: list[str]):
        """Marks a batch of domains as seeded in the domain_metadata table."""
        if not domains:
            return
        
        domain_tuples = [(d,) for d in domains]
        try:
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute_many(
                    "INSERT INTO domain_metadata (domain, is_seeded) VALUES (%s, 1) ON CONFLICT (domain) DO UPDATE SET is_seeded = 1",
                    domain_tuples,
                    query_name="mark_seeded_batch_upsert_pg"
                )
            else:  # SQLite
                await self.storage.db.execute_many(
                    "INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)",
                    domain_tuples,
                    query_name="mark_seeded_batch_insert_sqlite"
                )
                await self.storage.db.execute_many(
                    "UPDATE domain_metadata SET is_seeded = 1 WHERE domain = ?",
                    domain_tuples,
                    query_name="mark_seeded_batch_update_sqlite"
                )
            logger.debug(f"Marked {len(domains)} domains as seeded in DB.")
        except Exception as e:
            logger.error(f"DB error batch marking domains as seeded: {e}")
            raise
    
    async def _load_seeds(self):
        """Reads seed URLs from the seed file and adds them to the frontier."""
        if not self.config.seed_file.exists():
            logger.error(f"Seed file not found: {self.config.seed_file}")
            return

        try:
            with open(self.config.seed_file, 'r') as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

            if not urls:
                logger.warning(f"Seed file {self.config.seed_file} is empty.")
                return

            # Mark all domains from seeds as seeded
            seed_domains = {extract_domain(u) for u in urls if extract_domain(u)}
            await self._mark_domains_as_seeded_batch(list(seed_domains))
            
            logger.info(f"Adding {len(urls)} URLs to the frontier")
            async def add_urls_batch_worker(urls: list[str]):
                added_count = await self.add_urls_batch(urls)
                return added_count
            seed_tasks = []
            chunk_size = (len(urls) + self.config.max_workers - 1) // self.config.max_workers
            for i in range(self.config.max_workers):
                url_chunk = urls[i*chunk_size:(i+1)*chunk_size]
                seed_tasks.append(add_urls_batch_worker(url_chunk))
            added_counts = await asyncio.gather(*seed_tasks)
            added_count = sum(added_counts)
            
            logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")

    async def _clear_frontier_db(self):
        """Clears the frontier table."""
        try:
            await self.storage.db.execute("DELETE FROM frontier", query_name="clear_frontier")
            logger.info("Cleared frontier table in database.")
            self.seen_urls.clear()
        except Exception as e:
            logger.error(f"DB error clearing frontier: {e}")
            raise

    async def add_urls_batch(self, urls: list[str], depth: int = 0) -> int:
        """Adds a batch of URLs to the frontier, designed for efficiency."""
        # 1. Normalize and pre-filter
        normalized_urls = {normalize_url(u) for u in urls}
        # Remove None from failed normalizations and anything already seen in-memory
        candidates = {u for u in normalized_urls if u and u not in self.seen_urls}
        
        if not candidates:
            return 0
            
        # 2. Politeness filtering
        allowed_urls = []
        for url in candidates:
            # This is a performance optimization. By checking here, we avoid inserting
            # known-disallowed URLs into the frontier table. It also populates the
            # in-memory seen_urls set for disallowed links, preventing us from
            # re-checking them every time they are discovered.
            if await self.politeness.is_url_allowed(url):
                allowed_urls.append(url)
            else:
                self.seen_urls.add(url) # Mark as seen to prevent re-checking
        
        if not allowed_urls:
            return 0
        
        # 3. Bulk check against visited_urls table
        # Create SHA256 hashes for the query
        url_hashes_to_check = [self.storage.get_url_sha256(u) for u in allowed_urls]
        
        # Chunk the check for existing hashes to avoid very large IN clauses.
        existing_hashes = set()
        chunk_size = 500
        
        for i in range(0, len(url_hashes_to_check), chunk_size):
            chunk = url_hashes_to_check[i:i + chunk_size]
            
            # Fetch hashes from the current chunk that already exist in visited_urls
            rows = await self.storage.db.fetch_all(
                f"SELECT url_sha256 FROM visited_urls WHERE url_sha256 IN ({','.join(['?' for _ in chunk])})",
                tuple(chunk),
                query_name="batch_check_visited"
            )
            if rows:
                for row in rows:
                    existing_hashes.add(row[0])

        # Filter out URLs that are already visited
        new_urls_to_add = [u for u in allowed_urls if self.storage.get_url_sha256(u) not in existing_hashes]

        if not new_urls_to_add:
            return 0

        # 4. Bulk insert new URLs into the frontier in chunks
        added_count = 0
        current_time = int(time.time())
        
        records_to_insert = []
        for url in new_urls_to_add:
            domain = extract_domain(url)
            if domain:
                records_to_insert.append((url, domain, depth, current_time, 0, None))
        
        if not records_to_insert:
            return 0
            
        try:
            for i in range(0, len(records_to_insert), chunk_size):
                chunk_of_records = records_to_insert[i:i + chunk_size]
                
                if self.storage.config.db_type == "postgresql":
                    await self.storage.db.execute_many(
                        "INSERT INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING",
                        chunk_of_records,
                        query_name="batch_insert_frontier_pg"
                    )
                else: # SQLite
                    await self.storage.db.execute_many(
                        "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                        chunk_of_records,
                        query_name="batch_insert_frontier_sqlite"
                    )

                # Update in-memory cache and count for the processed chunk
                for record in chunk_of_records:
                    self.seen_urls.add(record[0])
                
                added_count += len(chunk_of_records)
            
        except Exception as e:
            # This can happen in a race condition if another worker adds the same URL between our checks and this insert.
            logger.error(f"DB error during batch insert to frontier: {e}")

        return added_count

    async def is_empty(self) -> bool:
        """Checks if the frontier is empty using an efficient query."""
        try:
            row = await self.storage.db.fetch_one("SELECT 1 FROM frontier LIMIT 1", query_name="is_frontier_empty")
            return row is None
        except Exception as e:
            logger.error(f"Error checking if frontier is empty: {e}")
            return True # Assume empty on error to be safe

    async def count_frontier(self) -> int:
        """
        Estimates the number of URLs in the frontier.
        For PostgreSQL, this uses a fast statistical estimate.
        For SQLite, it performs a full count.
        """
        try:
            if self.storage.config.db_type == "postgresql":
                # This is a very fast way to get an estimate from table statistics
                row = await self.storage.db.fetch_one(
                    "SELECT reltuples::BIGINT FROM pg_class WHERE relname = 'frontier'",
                    query_name="count_frontier_pg_estimated"
                )
                return row[0] if row else 0
            else: # SQLite
                row = await self.storage.db.fetch_one("SELECT COUNT(*) FROM frontier", query_name="count_frontier_sqlite")
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error counting frontier: {e}")
            return 0

    async def get_next_url(self, worker_id: int = 0, total_workers: int = 1) -> tuple[str, str, int, int] | None:
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read to minimize contention.
        
        Args:
            worker_id: The ID of this worker (0-based)
            total_workers: Total number of workers
            
        Returns:
            A tuple of (url, domain, url_id, depth) or None.
        """
        batch_size = 1  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            # Atomically claim a batch of URLs
            claimed_urls = await self._atomic_claim_urls(batch_size, claim_expiry_seconds, worker_id, total_workers)
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Track which URLs we need to unclaim if we return early
            urls_to_unclaim = []
            selected_url_info = None
            
            # Process claimed URLs with politeness checks
            for i, (url_id, url, domain, depth) in enumerate(claimed_urls):
                # This is a correctness check. It ensures that any URL pulled from the
                # frontier, which may have been added hours or days ago, is re-validated
                # against the LATEST politeness rules before we fetch it. This is
                # crucial for long-running crawls or crawls that are paused and resumed
                # after rule changes (e.g., an updated robots.txt or exclusion list).
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await self._delete_url(url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                logger.debug(f"{worker_name} checking if URL ID {url_id} from domain {domain} is ready for fetch")
                if await self.politeness.can_fetch_domain_now(domain):
                    await self.politeness.record_domain_fetch_attempt(domain)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await self._delete_url(url_id)
                    selected_url_info = (url, domain, url_id, depth)
                    # Mark remaining URLs to be unclaimed
                    urls_to_unclaim = [(uid, u, d, dep) for uid, u, d, dep in claimed_urls[i+1:]]
                    break
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await self._unclaim_url(url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, {worker_name} unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain, depth in urls_to_unclaim:
                await self._unclaim_url(url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except Exception as e:
            logger.error(f"Error in get_next_url: {e}", exc_info=True)
            return None

    async def _atomic_claim_urls(self, batch_size: int, claim_expiry_seconds: int, worker_id: int = 0, total_workers: int = 1) -> list[tuple[int, str, str, int]]:
        """Atomically claims a batch of URLs from the frontier for a specific worker's domain shard."""
        try:
            claim_timestamp = int(time.time() * 1000) # Current time in milliseconds
            current_time_seconds = int(time.time())

            if self.storage.config.db_type == "postgresql":
                # Calculate the threshold for considering a claim expired
                expired_claim_timestamp_threshold = claim_timestamp - (claim_expiry_seconds * 1000)
                # Calculate when a domain is ready (70 seconds have passed since last fetch)
                domain_ready_threshold = current_time_seconds - 70

                # Domain-aware claiming with domain sharding
                # Try unclaimed URLs (both with and without domain metadata)
                sql_query = '''
                UPDATE frontier
                SET claimed_at = %s
                WHERE id = (
                    SELECT f.id
                    FROM frontier f
                    LEFT JOIN domain_metadata dm ON f.domain = dm.domain
                    WHERE f.claimed_at IS NULL
                    AND MOD(hashtext(f.domain)::BIGINT, %s) = %s
                    AND (
                        dm.domain IS NULL  -- New domain (no metadata)
                        OR dm.last_scheduled_fetch_timestamp IS NULL  -- Never fetched
                        OR dm.last_scheduled_fetch_timestamp <= %s  -- Ready to fetch
                    )
                    ORDER BY f.added_timestamp ASC
                    LIMIT 1
                    FOR UPDATE OF f SKIP LOCKED
                )
                RETURNING id, url, domain, depth;
                '''
                
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (claim_timestamp, total_workers, worker_id, domain_ready_threshold),
                    query_name="atomic_claim_urls_pg_unclaimed"
                )
                
                # If no unclaimed URLs, try expired ones (both with and without domain metadata)
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        LEFT JOIN domain_metadata dm ON f.domain = dm.domain
                        WHERE f.claimed_at IS NOT NULL 
                        AND f.claimed_at < %s
                        AND MOD(hashtext(f.domain)::BIGINT, %s) = %s
                        AND (
                            dm.domain IS NULL  -- New domain (no metadata)
                            OR dm.last_scheduled_fetch_timestamp IS NULL  -- Never fetched
                            OR dm.last_scheduled_fetch_timestamp <= %s  -- Ready to fetch
                        )
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp, expired_claim_timestamp_threshold, total_workers, worker_id, domain_ready_threshold),
                        query_name="atomic_claim_urls_pg_expired"
                    )
            else:
                # SQLite doesn't support FOR UPDATE SKIP LOCKED or complex CTEs as well.
                # Keep the old explicit release and simpler claim for SQLite.
                # This path will be less performant for high-volume crawling.
                expired_timestamp = claim_timestamp - (claim_expiry_seconds * 1000)
                await self.storage.db.execute("""
                    UPDATE frontier 
                    SET claimed_at = NULL 
                    WHERE claimed_at IS NOT NULL AND claimed_at < ?
                """, (expired_timestamp,), query_name="release_expired_claims")

                # Original SQLite claiming logic
                claimed_urls = await self.storage.db.execute_returning(
                    """
                    WITH to_claim_ids AS (
                        SELECT id
                        FROM frontier
                        WHERE claimed_at IS NULL
                        ORDER BY added_timestamp ASC
                        LIMIT ?
                    )
                    UPDATE frontier
                    SET claimed_at = ?
                    WHERE id IN (SELECT id FROM to_claim_ids)
                    RETURNING id, url, domain, depth
                    """, (batch_size, claim_timestamp), # Corrected params for SQLite query
                    query_name="atomic_claim_urls_sqlite"
                )
            
            return claimed_urls
            
        except Exception as e:
            logger.error(f"DB error in atomic claim: {e}")
            raise

    async def _delete_url(self, url_id: int):
        """Deletes a URL from the frontier after processing or if disallowed."""
        try:
            await self.storage.db.execute("DELETE FROM frontier WHERE id = ?", (url_id,), query_name="delete_frontier_url")
        except Exception as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")

    async def _unclaim_url(self, url_id: int) -> None:
        """Unclaims a specific URL by its ID, setting claimed_at to NULL."""
        try:
            await self.storage.db.execute(
                "UPDATE frontier SET claimed_at = NULL WHERE id = ?", 
                (url_id,),
                query_name="unclaim_url_set_null"
            )
        except Exception as e:
            logger.error(f"Error unclaiming URL ID {url_id}: {e}")

class HybridFrontierManager:
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
        self.write_locks: Dict[str, asyncio.Lock] = {}  # Per-domain write locks
        
    def _get_frontier_path(self, domain: str) -> Path:
        """Get file path for domain's frontier."""
        # Use first 2 chars of hash for subdirectory (256 subdirs)
        domain_hash = hashlib.md5(domain.encode()).hexdigest()
        subdir = domain_hash[:2]
        path = self.frontier_dir / subdir / f"{domain}.frontier"
        path.parent.mkdir(exist_ok=True)
        return path
    
    async def initialize_frontier(self):
        """Initialize the frontier, loading seeds or resuming from existing data."""
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
        
        # Load seen URLs into memory cache (for compatibility with existing interface)
        await self._populate_seen_urls_from_redis()
        
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
    
    async def _populate_seen_urls_from_redis(self):
        """Populate in-memory seen_urls from Redis visited records."""
        # For compatibility, we load a subset of visited URLs into memory
        # In production, we'd rely entirely on the bloom filter
        visited_count = 0
        cursor = 0
        
        while True:
            cursor, keys = await self.redis.scan(
                cursor, match='visited:*', count=1000
            )
            
            if keys:
                pipe = self.redis.pipeline()
                for key in keys:
                    pipe.hget(key, 'url')
                urls = await pipe.execute()
                
                for url in urls:
                    if url:
                        visited_count += 1
            
            # Exit when cursor returns to 0    
            if cursor == 0:
                break
                
        logger.info(f"Loaded {visited_count} URLs into in-memory seen_urls cache")
    
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
                urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
                
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
            chunk_size = (len(urls) + self.config.max_workers - 1) // self.config.max_workers
            for i in range(self.config.max_workers):
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
        # 1. Normalize and pre-filter
        candidates = set()
        for u in urls:
            try:
                normalized = normalize_url(u)
                if normalized:
                    candidates.add(normalized)
            except Exception as e:
                logger.debug(f"Failed to normalize URL {u}: {e}")
        
        if not candidates:
            return 0
            
        # 2. Check against bloom filter and visited URLs
        new_urls = []
        
        for url in candidates:
            # Check bloom filter
            exists = await self.redis.execute_command('BF.EXISTS', 'seen:bloom', url)
            if not exists:
                # Double-check against visited URLs (for exact match)
                url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
                visited = await self.redis.exists(f'visited:{url_hash}')
                if not visited:
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
        # Get or create write lock for this domain
        if domain not in self.write_locks:
            self.write_locks[domain] = asyncio.Lock()
            
        async with self.write_locks[domain]:
            frontier_path = self._get_frontier_path(domain)
            domain_key = f"domain:{domain}"
            
            # Filter out URLs already in bloom filter
            new_urls = []
            for url, depth in urls:
                # Add to bloom filter (idempotent operation)
                await self.redis.execute_command('BF.ADD', 'seen:bloom', url)
                # Check if it was already there (BF.ADD returns 0 if already existed)
                # For simplicity, we'll just add all URLs and rely on file deduplication
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
                
            # Update Redis metadata
            pipe = self.redis.pipeline()
            
            # Get current size
            current_size_result = self.redis.hget(domain_key, 'frontier_size')
            if asyncio.iscoroutine(current_size_result):
                current_size = await current_size_result
            else:
                current_size = current_size_result
            new_size = int(current_size or 0) + len(new_urls)
            
            # Get is_seeded status
            is_seeded_result = self.redis.hget(domain_key, 'is_seeded')
            if asyncio.iscoroutine(is_seeded_result):
                is_seeded = await is_seeded_result
            else:
                is_seeded = is_seeded_result
            
            # Update metadata
            pipe.hset(domain_key, mapping={
                'frontier_size': str(new_size),
                'file_path': str(frontier_path.relative_to(self.frontier_dir)),
                'is_seeded': '1' if is_seeded else '0'
            })
            
            # Initialize offset if needed
            pipe.hsetnx(domain_key, 'frontier_offset', '0')
            
            # Add domain to queue
            # Note: This might add duplicates, but we handle that when popping
            pipe.rpush('domains:queue', domain)
            
            await pipe.execute()
            
            return len(new_urls)
    
    async def is_empty(self) -> bool:
        """Check if frontier is empty."""
        # Check if there are any domains in the queue
        count = await self.redis.llen('domains:queue')  # type: ignore[misc]
        return count == 0
    
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
                    size = int(results[i] or 0)
                    offset = int(results[i + 1] or 0)
                    # Remaining URLs = size - (offset / bytes_per_line)
                    # We need to estimate bytes per line or track line count
                    # For now, assume offset tracks line count
                    remaining = max(0, size - offset)
                    total += remaining
            
            # Exit when cursor returns to 0
            if cursor == 0:
                break
                    
        return total
    
    async def get_next_url(self, worker_id: int = 0, total_workers: int = 1) -> Optional[Tuple[str, str, int, int]]:
        """Get next URL to crawl.
        
        Returns None if no URLs are available OR if politeness rules prevent fetching.
        The caller is responsible for retrying.
        """
        # Atomically pop a domain from the front of the queue
        domain = await self.redis.lpop('domains:queue')  # type: ignore[misc]
        
        if not domain:
            return None  # Queue is empty
        
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
            # Domain always goes back to the end of the queue, regardless of success or failure
            await self.redis.rpush('domains:queue', domain)  # type: ignore[misc]
        
        return None
    
    async def _get_url_from_domain(self, domain: str) -> Optional[Tuple[str, str, int]]:
        """Read next URL from domain's frontier file."""
        domain_key = f"domain:{domain}"
        
        # Get file info from Redis
        file_info = await self.redis.hmget(  # type: ignore[misc]
            domain_key, 
            ['file_path', 'frontier_offset', 'frontier_size']
        )
        
        if not file_info[0]:  # No file path
            return None
            
        file_path = self.frontier_dir / file_info[0]
        offset = int(file_info[1] or 0)
        size = int(file_info[2] or 0)
        
        if offset >= size:  # All URLs consumed
            return None
            
        # Read URL from file
        try:
            async with aiofiles.open(file_path, 'r') as f:
                # Read all lines to find the next valid URL
                lines = await f.readlines()
                
                # Find the line at the current offset
                if offset < len(lines):
                    line = lines[offset].strip()
                    
                    if line:
                        # Update offset
                        new_offset = offset + 1
                        await self.redis.hset(domain_key, 'frontier_offset', str(new_offset))  # type: ignore[misc]
                        
                        # Parse URL data
                        parts = line.split('|')
                        if len(parts) >= 2:
                            url, depth_str = parts[:2]
                            return url, domain, int(depth_str)
                            
        except Exception as e:
            logger.error(f"Error reading frontier file {file_path}: {e}")
            
        return None