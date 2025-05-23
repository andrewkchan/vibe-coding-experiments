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
        """Synchronously loads all URLs from visited_urls and frontier tables."""
        seen = set()
        try:
            with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
                with conn_threaded.cursor() as cursor:
                    # Load from visited_urls
                    cursor.execute("SELECT url FROM visited_urls")
                    for row in cursor.fetchall():
                        seen.add(row[0])
                    
                    # Load from current frontier (in case of resume)
                    cursor.execute("SELECT url FROM frontier")
                    for row in cursor.fetchall():
                        seen.add(row[0])
            logger.info(f"DB THREAD: Loaded {len(seen)} URLs for seen_urls set.")
        except sqlite3.Error as e:
            logger.error(f"DB THREAD: Error loading for seen_urls set: {e}")
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

    async def _load_seeds(self):
        """Reads seed URLs from the seed file and adds them to the frontier."""
        if not self.config.seed_file.exists():
            logger.error(f"Seed file not found: {self.config.seed_file}")
            return

        added_count = 0
        try:
            with open(self.config.seed_file, 'r') as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):
                        await self.add_url(url)
                        added_count +=1
            logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
        except IOError as e:
            logger.error(f"Error reading seed file {self.config.seed_file}: {e}")

    def _clear_frontier_db_sync(self):
        """Synchronous part of clearing frontier DB. To be run in a thread."""
        # No self.storage.conn check here, as it establishes its own.
        with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
            with conn_threaded.cursor() as cursor_obj:
                cursor_obj.execute("DELETE FROM frontier")
                conn_threaded.commit()
                # self.seen_urls.clear() # This should be done in the async part after thread finishes
                logger.info("Cleared frontier table in database (via thread).")
    
    async def _clear_frontier_db(self):
        await asyncio.to_thread(self._clear_frontier_db_sync)
        self.seen_urls.clear() # Clear in-memory set after DB operation completes

    def _add_url_sync(self, normalized_url: str, domain: str, depth: int) -> bool:
        """Synchronous part of adding a URL to DB. To be run in a thread."""
        try:
            with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
                with conn_threaded.cursor() as cursor:
                    # Check if visited first
                    cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
                                   (self.storage.get_url_sha256(normalized_url),))
                    if cursor.fetchone():
                        # logger.debug(f"DB THREAD: URL already visited: {normalized_url}")
                        return False # Already visited, not added to frontier

                    cursor.execute(
                        "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score) VALUES (?, ?, ?, ?, ?)",
                        (normalized_url, domain, depth, int(time.time()), 0)
                    )
                    if cursor.rowcount > 0:
                        conn_threaded.commit()
                        # logger.debug(f"DB THREAD: Added to frontier: {normalized_url}")
                        return True # Newly added to frontier
                    else:
                        # logger.debug(f"DB THREAD: URL likely already in frontier (IGNORE): {normalized_url}")
                        return False # Not newly added (already in frontier)
        except sqlite3.Error as e:
            logger.error(f"DB THREAD: Error adding URL {normalized_url} to frontier: {e}")
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
        """Counts the number of URLs currently in the frontier table."""
        # This method is called via asyncio.to_thread, so it needs its own DB connection.
        conn = None
        cursor = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM frontier")
            count_row = cursor.fetchone()
            return count_row[0] if count_row else 0
        except sqlite3.Error as e:
            logger.error(f"Error counting frontier: {e}")
            return 0
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Removes the URL from the frontier upon retrieval.
        """
        # if not self.storage.conn: # We won't use self.storage.conn directly in threaded parts
        #     logger.error("Cannot get next URL, an initial StorageManager connection was expected for path.")
        #     return None

        candidate_check_limit = self.config.max_workers * 5
        selected_url_info = None

        try:
            def _get_candidates_sync_threaded():
                with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
                    with conn_threaded.cursor() as cursor_obj:
                        cursor_obj.execute(
                            "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                            (candidate_check_limit,)
                        )
                        return cursor_obj.fetchall()
            
            candidates = await asyncio.to_thread(_get_candidates_sync_threaded)

            if not candidates:
                # logger.info("Frontier is empty based on current query.") # Less noisy for tests
                return None

            for url_id, url, domain in candidates:
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules. Removing from frontier.")
                    def _delete_disallowed_url_sync_threaded():
                        with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
                            with conn_threaded.cursor() as cursor_obj:
                                cursor_obj.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                                conn_threaded.commit()
                    await asyncio.to_thread(_delete_disallowed_url_sync_threaded)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain):
                    await self.politeness.record_domain_fetch_attempt(domain)
                    
                    def _delete_selected_url_sync_threaded():
                        with sqlite3.connect(self.storage.db_path, timeout=10) as conn_threaded:
                            with conn_threaded.cursor() as cursor_obj:
                                cursor_obj.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                                conn_threaded.commit()
                    await asyncio.to_thread(_delete_selected_url_sync_threaded)
                    
                    logger.debug(f"Retrieved from frontier: {url} (ID: {url_id}) for domain {domain}")
                    selected_url_info = (url, domain, url_id)
                    break 
                else:
                    pass 
            
            if not selected_url_info:
                # logger.debug(f"No suitable URL found in the first {len(candidates)} candidates that respects politeness rules now.")
                pass

        except sqlite3.Error as e: 
            logger.error(f"DB Error getting next URL from frontier: {e}", exc_info=True)
            return None 
        except Exception as e: 
            logger.error(f"Unexpected error during get_next_url: {e}", exc_info=True)
            return None
        
        return selected_url_info 