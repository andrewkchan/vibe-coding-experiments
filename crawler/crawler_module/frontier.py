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
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score) VALUES (?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0)
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
        Uses pooled connections for DB operations.
        Removes the URL from the frontier upon retrieval.
        """
        candidate_check_limit = self.config.max_workers * 5 # Make this configurable?
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_candidates_sync_threaded_pooled(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                        (candidate_check_limit,)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Pass the obtained connection to the threaded function
            candidates = await asyncio.to_thread(_get_candidates_sync_threaded_pooled, conn_from_pool)

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                # Pass the existing connection to politeness methods
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} disallowed by politeness rules. Removing from frontier (pool).")
                    
                    def _delete_disallowed_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_disallowed_url_sync_threaded_pooled, conn_from_pool)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool) # Async, non-DB
                    
                    def _delete_selected_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_selected_url_sync_threaded_pooled, conn_from_pool)
                    
                    logger.debug(f"Retrieved from frontier: {url} (ID: {url_id}) for domain {domain} (pool)")
                    selected_url_info = (url, domain, url_id)
                    break # Found a suitable URL
                else:
                    pass 
            
            if not selected_url_info:
                logger.debug(f"No suitable URL found in the first {len(candidates)} candidates that respects politeness rules now.")
                pass

        except sqlite3.Error as e: 
            logger.error(f"DB Error getting next URL from frontier (pool): {e}", exc_info=True)
            if conn_from_pool: # Try to rollback if commit failed or other op failed
                try: conn_from_pool.rollback()
                except: pass
            return None 
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected or Pool error during get_next_url (pool): {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        
        return selected_url_info 