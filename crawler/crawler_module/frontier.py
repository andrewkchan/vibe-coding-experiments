import logging
import time
from pathlib import Path
from typing import Set
import asyncio
import sqlite3

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

    def _populate_seen_urls_from_db_sync(self) -> Set[str]:
        """Synchronously loads all URLs from visited_urls and frontier tables using a pooled connection."""
        seen = set()
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                # Load from visited_urls
                cursor.execute("SELECT url FROM visited_urls")
                for row in cursor.fetchall():
                    seen.add(row[0])
                
                # Load from current frontier (in case of resume)
                cursor.execute("SELECT url FROM frontier")
                for row in cursor.fetchall():
                    seen.add(row[0])
            finally:
                cursor.close()
            logger.info(f"DB THREAD (pool): Loaded {len(seen)} URLs for seen_urls set.")
        except sqlite3.Error as e:
            logger.error(f"DB THREAD (pool): Error loading for seen_urls set: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB THREAD (pool): Pool error loading for seen_urls set: {e}")
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        return seen

    async def initialize_frontier(self):
        """Loads seed URLs and previously saved frontier URLs if resuming."""
        # Populate seen_urls from DB in a thread
        self.seen_urls = await asyncio.to_thread(self._populate_seen_urls_from_db_sync)
        logger.info(f"Initialized seen_urls with {len(self.seen_urls)} URLs from DB.")

        if self.config.resume:
            # count_frontier is already thread-safe
            count = await asyncio.to_thread(self.count_frontier) 
            logger.info(f"Resuming crawl. Frontier has {count} URLs.")
            if count == 0:
                 logger.warning("Resuming with an empty frontier. Attempting to load seeds.")
                 await self._load_seeds()
        else:
            logger.info("Starting new crawl. Clearing any existing frontier and loading seeds.")
            await self._clear_frontier_db() # Already refactored to be thread-safe
            await self._load_seeds() # add_url within is now thread-safe for DB ops

    def _mark_domain_as_seeded(self, domain: str):
        """Marks a domain as seeded in the domain_metadata table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                cursor.execute("UPDATE domain_metadata SET is_seeded = 1 WHERE domain = ?", (domain,))
                conn_from_pool.commit()
                logger.debug(f"Marked domain {domain} as seeded in DB (pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error marking domain {domain} as seeded: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error marking domain {domain} as seeded: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error marking domain {domain} as seeded: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _load_seeds(self):
        """Reads seed URLs from the seed file and adds them to the frontier."""
        if not self.config.seed_file.exists():
            logger.error(f"Seed file not found: {self.config.seed_file}")
            return

        added_count = 0
        urls = []
        async def _seed_worker(start, end):
            nonlocal added_count
            for url in urls[start: end]:
                await self.add_url(url)
                added_count +=1
                # Also mark domain as is_seeded = 1 in domain_metadata table
                domain = extract_domain(url)
                await asyncio.to_thread(self._mark_domain_as_seeded, domain)
        try:
            with open(self.config.seed_file, 'r') as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):
                        urls.append(url)
            num_per_worker = (len(urls) + self.config.max_workers - 1)//self.config.max_workers
            seed_workers = []
            for i in range(self.config.max_workers):
                start = i * num_per_worker
                end = (i + 1) * num_per_worker
                seed_workers.append(_seed_worker(start, end))
            await asyncio.gather(*seed_workers)
            logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")

    def _clear_frontier_db_sync(self):
        """Synchronous part of clearing frontier DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("DELETE FROM frontier")
                conn_from_pool.commit() # Explicit commit for DELETE
                logger.info("Cleared frontier table in database (via thread, pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error clearing frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error clearing frontier: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error clearing frontier: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _clear_frontier_db(self):
        await asyncio.to_thread(self._clear_frontier_db_sync)
        self.seen_urls.clear() # Clear in-memory set after DB operation completes

    def _add_url_sync(self, normalized_url: str, domain: str, depth: int) -> bool:
        """Synchronous part of adding a URL to DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
                               (self.storage.get_url_sha256(normalized_url),))
                if cursor.fetchone():
                    return False 

                cursor.execute(
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0, None)
                )
                if cursor.rowcount > 0:
                    conn_from_pool.commit() # Explicit commit for INSERT
                    return True 
                else:
                    # This means INSERT OR IGNORE found an existing row (URL already in frontier)
                    return False 
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) THREAD: Error adding URL {normalized_url} to frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise # Propagate to mark as not added
            finally:
                cursor.close()
        except sqlite3.Error as e: # Outer SQLite errors (e.g. connection issue)
            logger.error(f"DB (pool/connection) THREAD: Error adding URL {normalized_url} to frontier: {e}")
            return False # Ensure it's marked as not added on any DB error
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) THREAD: Pool error adding URL {normalized_url} to frontier: {e}")
            return False
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        # Fallback, should be covered by try/except logic.
        # If an error occurred before return True, it should have returned False or raised.
        return False

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

        # Now, try to add to DB in a thread
        was_added_to_db = await asyncio.to_thread(
            self._add_url_sync, normalized_url, domain, depth
        )

        if was_added_to_db:
            self.seen_urls.add(normalized_url) # Add to in-memory set if successfully added to DB frontier
            return True
        else:
            # If not added to DB (either duplicate in frontier, or visited, or error),
            # ensure it's in seen_urls to prevent reprocessing this path.
            self.seen_urls.add(normalized_url)
            return False

    def _get_db_connection(self): # Helper to get a new connection
        return sqlite3.connect(self.storage.db_path, timeout=10)

    def count_frontier(self) -> int:
        """Counts the number of URLs currently in the frontier table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM frontier")
                count_row = cursor.fetchone()
                return count_row[0] if count_row else 0
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"Error counting frontier (pool): {e}")
            return 0
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Pool error counting frontier: {e}")
            return 0
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Track which URLs we need to unclaim if we return early
            urls_to_unclaim = []
            selected_url_info = None
            
            # Process claimed URLs with politeness checks
            for i, (url_id, url, domain) in enumerate(claimed_urls):
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    selected_url_info = (url, domain, url_id)
                    # Mark remaining URLs to be unclaimed
                    urls_to_unclaim = [(uid, u, d) for uid, u, d in claimed_urls[i+1:]]
                    break
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain in urls_to_unclaim:
                await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _atomic_claim_urls_sync(self, conn: sqlite3.Connection, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs using UPDATE...RETURNING.
        Returns list of (id, url, domain) tuples for claimed URLs."""
        cursor = conn.cursor()
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        expired_timestamp = claim_timestamp - (claim_expiry_seconds * 1000000)
        
        try:
            # First, release any expired claims
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Atomically claim unclaimed URLs using RETURNING
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = ? 
                WHERE id IN (
                    SELECT id FROM frontier 
                    WHERE claimed_at IS NULL
                    ORDER BY added_timestamp ASC 
                    LIMIT ?
                )
                RETURNING id, url, domain
            """, (claim_timestamp, batch_size))
            
            claimed_urls = cursor.fetchall()
            conn.commit()
            
            return claimed_urls
            
        except sqlite3.Error as e:
            logger.error(f"DB error in atomic claim: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier after processing or if disallowed."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Releases a claim on a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()