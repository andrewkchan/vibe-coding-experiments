import logging
import time
from pathlib import Path
from typing import Set
import asyncio

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
        """Loads all URLs from visited_urls and frontier tables."""
        seen = set()
        try:
            # Load from visited_urls
            rows = await self.storage.db.fetch_all("SELECT url FROM visited_urls", query_name="populate_seen_from_visited")
            for row in rows:
                seen.add(row[0])
            
            # Load from current frontier (in case of resume)
            rows = await self.storage.db.fetch_all("SELECT url FROM frontier", query_name="populate_seen_from_frontier")
            for row in rows:
                seen.add(row[0])
            
            logger.info(f"Loaded {len(seen)} URLs for seen_urls set.")
        except Exception as e:
            logger.error(f"Error loading for seen_urls set: {e}")
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
            
            # Add all URLs to the frontier in one batch
            added_count = await self.add_urls_batch(urls)
            
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
        
        # This part is tricky. A large IN clause can be slow.
        # However, for a single page, the number of links is usually manageable.
        # This could be a future optimization.
        
        # Fetch all hashes that already exist in visited_urls
        existing_hashes = set()
        rows = await self.storage.db.fetch_all(
            f"SELECT url_sha256 FROM visited_urls WHERE url_sha256 IN ({','.join(['?' for _ in url_hashes_to_check])})",
            tuple(url_hashes_to_check),
            query_name="batch_check_visited"
        )
        if rows:
            for row in rows:
                existing_hashes.add(row[0])

        # Filter out URLs that are already visited
        new_urls_to_add = [u for u in allowed_urls if self.storage.get_url_sha256(u) not in existing_hashes]

        if not new_urls_to_add:
            return 0

        # 4. Bulk insert new URLs into the frontier
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
            # Use execute_many for efficient bulk insertion
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute_many(
                    "INSERT INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING",
                    records_to_insert,
                    query_name="batch_insert_frontier_pg"
                )
            else: # SQLite
                await self.storage.db.execute_many(
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    records_to_insert,
                    query_name="batch_insert_frontier_sqlite"
                )

            # Update in-memory cache and count
            for url, _, _, _, _, _ in records_to_insert:
                self.seen_urls.add(url)
            
            added_count = len(records_to_insert)
            
        except Exception as e:
            # This can happen in a race condition if another worker adds the same URL between our checks and this insert.
            logger.error(f"DB error during batch insert to frontier: {e}")
            # We could try to insert one-by-one here as a fallback, but for now, we'll just log.

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

    async def get_next_url(self) -> tuple[str, str, int, int] | None:
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read to minimize contention.
        Returns a tuple of (url, domain, url_id, depth) or None.
        """
        batch_size = 1  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            # Atomically claim a batch of URLs
            claimed_urls = await self._atomic_claim_urls(batch_size, claim_expiry_seconds)
            
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
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain, depth in urls_to_unclaim:
                await self._unclaim_url(url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except Exception as e:
            logger.error(f"Error in get_next_url: {e}", exc_info=True)
            return None

    async def _atomic_claim_urls(self, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str, int]]:
        """Atomically claims a batch of URLs from the frontier."""
        try:
            claim_timestamp = int(time.time() * 1000) # Current time in milliseconds

            if self.storage.config.db_type == "postgresql":
                # Calculate the threshold for considering a claim expired
                expired_claim_timestamp_threshold = claim_timestamp - (claim_expiry_seconds * 1000)

                sql_query = '''
                WITH unexpired_candidates AS (
                    SELECT id, url, domain, added_timestamp, depth
                    FROM frontier
                    WHERE claimed_at IS NULL
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                expired_candidates AS (
                    SELECT id, url, domain, added_timestamp, depth
                    FROM frontier
                    WHERE claimed_at IS NOT NULL AND claimed_at < %s
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                all_candidates AS (
                    SELECT id, url, domain, added_timestamp, depth FROM unexpired_candidates
                    UNION ALL
                    SELECT id, url, domain, added_timestamp, depth FROM expired_candidates
                ),
                ordered_limited_candidates AS (
                    SELECT id, url, domain, depth
                    FROM all_candidates
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                to_claim AS (
                    SELECT id, url, domain, depth FROM ordered_limited_candidates
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE frontier
                SET claimed_at = %s
                FROM to_claim
                WHERE frontier.id = to_claim.id
                RETURNING frontier.id, frontier.url, frontier.domain, frontier.depth;
                '''
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (batch_size, expired_claim_timestamp_threshold, batch_size, batch_size, claim_timestamp),
                    query_name="atomic_claim_urls_pg_implicit_expiry"
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