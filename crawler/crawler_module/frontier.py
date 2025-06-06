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
            rows = await self.storage.db.fetch_all("SELECT url FROM visited_urls")
            for row in rows:
                seen.add(row[0])
            
            # Load from current frontier (in case of resume)
            rows = await self.storage.db.fetch_all("SELECT url FROM frontier")
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

    async def _mark_domain_as_seeded(self, domain: str):
        """Marks a domain as seeded in the domain_metadata table."""
        try:
            if self.storage.config.db_type == "postgresql":
                await self.storage.db.execute("INSERT INTO domain_metadata (domain) VALUES (%s) ON CONFLICT DO NOTHING", (domain,))
                await self.storage.db.execute("UPDATE domain_metadata SET is_seeded = 1 WHERE domain = %s", (domain,))
            else:  # SQLite
                await self.storage.db.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                await self.storage.db.execute("UPDATE domain_metadata SET is_seeded = 1 WHERE domain = ?", (domain,))
            logger.debug(f"Marked domain {domain} as seeded in DB.")
        except Exception as e:
            logger.error(f"DB error marking domain {domain} as seeded: {e}")
            raise
    
    async def _load_seeds(self):
        """Reads seed URLs from the seed file and adds them to the frontier."""
        if not self.config.seed_file.exists():
            logger.error(f"Seed file not found: {self.config.seed_file}")
            return

        added_count = 0
        urls = []
        
        async def _seed_worker(start, end):
            nonlocal added_count
            for url in urls[start:end]:
                if await self.add_url(url):
                    added_count += 1
                # Also mark domain as is_seeded = 1 in domain_metadata table
                domain = extract_domain(url)
                if domain:
                    await self._mark_domain_as_seeded(domain)
        
        try:
            with open(self.config.seed_file, 'r') as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):
                        urls.append(url)
            
            num_per_worker = (len(urls) + self.config.max_workers - 1) // self.config.max_workers
            seed_workers = []
            for i in range(self.config.max_workers):
                start = i * num_per_worker
                end = min((i + 1) * num_per_worker, len(urls))
                if start < len(urls):
                    seed_workers.append(_seed_worker(start, end))
            
            await asyncio.gather(*seed_workers)
            logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")

    async def _clear_frontier_db(self):
        """Clears the frontier table."""
        try:
            await self.storage.db.execute("DELETE FROM frontier")
            logger.info("Cleared frontier table in database.")
            self.seen_urls.clear()
        except Exception as e:
            logger.error(f"DB error clearing frontier: {e}")
            raise

    async def add_url(self, url: str, depth: int = 0) -> bool:
        """Adds a normalized URL to the frontier if not already seen or invalid domain."""
        normalized_url = normalize_url(url)
        if not normalized_url:
            return False

        if normalized_url in self.seen_urls:
            return False # Already processed or in frontier (in-memory check)

        domain = extract_domain(normalized_url)
        if not domain:
            logger.warning(f"Could not extract domain from URL: {normalized_url}, skipping.")
            return False
        
        if not await self.politeness.is_url_allowed(normalized_url):
            logger.debug(f"URL {normalized_url} disallowed by politeness, not adding.")
            self.seen_urls.add(normalized_url) # Mark as seen to avoid re-checking politeness
            return False

        # Check if already visited
        row = await self.storage.db.fetch_one(
            "SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
            (self.storage.get_url_sha256(normalized_url),)
        )
        if row:
            self.seen_urls.add(normalized_url)
            return False

        # Try to add to frontier
        try:
            await self.storage.db.execute(
                "INSERT INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                (normalized_url, domain, depth, int(time.time()), 0, None)
            )
            self.seen_urls.add(normalized_url)
            return True
        except Exception as e:
            # Likely a unique constraint violation (URL already in frontier)
            logger.debug(f"Could not add URL {normalized_url} to frontier: {e}")
            self.seen_urls.add(normalized_url)
            return False

    async def count_frontier(self) -> int:
        """Counts the number of URLs currently in the frontier table."""
        try:
            row = await self.storage.db.fetch_one("SELECT COUNT(*) FROM frontier")
            return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error counting frontier: {e}")
            return 0

    async def get_next_url(self) -> tuple[str, str, int] | None:
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read to minimize contention.
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
            for i, (url_id, url, domain) in enumerate(claimed_urls):
                # Check if URL is allowed by robots.txt and manual exclusions
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
                    selected_url_info = (url, domain, url_id)
                    # Mark remaining URLs to be unclaimed
                    urls_to_unclaim = [(uid, u, d) for uid, u, d in claimed_urls[i+1:]]
                    break
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await self._unclaim_url(url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain in urls_to_unclaim:
                await self._unclaim_url(url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except Exception as e:
            logger.error(f"Error in get_next_url: {e}", exc_info=True)
            return None

    async def _atomic_claim_urls(self, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs from the frontier."""
        try:
            claim_timestamp = int(time.time() * 1000) # Current time in milliseconds

            if self.storage.config.db_type == "postgresql":
                # Calculate the threshold for considering a claim expired
                expired_claim_timestamp_threshold = claim_timestamp - (claim_expiry_seconds * 1000)

                sql_query = '''
                WITH unexpired_candidates AS (
                    SELECT id, url, domain, added_timestamp
                    FROM frontier
                    WHERE claimed_at IS NULL
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                expired_candidates AS (
                    SELECT id, url, domain, added_timestamp
                    FROM frontier
                    WHERE claimed_at IS NOT NULL AND claimed_at < %s
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                all_candidates AS (
                    SELECT id, url, domain, added_timestamp FROM unexpired_candidates
                    UNION ALL
                    SELECT id, url, domain, added_timestamp FROM expired_candidates
                ),
                ordered_limited_candidates AS (
                    SELECT id, url, domain
                    FROM all_candidates
                    ORDER BY added_timestamp ASC
                    LIMIT %s
                ),
                to_claim AS (
                    SELECT id, url, domain FROM ordered_limited_candidates
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE frontier
                SET claimed_at = %s
                FROM to_claim
                WHERE frontier.id = to_claim.id
                RETURNING frontier.id, frontier.url, frontier.domain;
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
                    RETURNING id, url, domain
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