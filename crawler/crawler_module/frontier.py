import logging
import time
from pathlib import Path
from typing import Set

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

    async def initialize_frontier(self):
        """Loads seed URLs and previously saved frontier URLs if resuming."""
        # Load already visited URLs to populate the seen_urls set for this session
        # This prevents re-adding URLs that were processed in previous sessions immediately
        # The database UNIQUE constraint on frontier.url is the ultimate guard.
        if self.storage.conn:
            try:
                cursor = self.storage.conn.cursor()
                # Load from visited_urls
                cursor.execute("SELECT url FROM visited_urls")
                for row in cursor.fetchall():
                    self.seen_urls.add(row[0])
                
                # Load from current frontier (in case of resume)
                cursor.execute("SELECT url FROM frontier")
                for row in cursor.fetchall():
                    self.seen_urls.add(row[0])
                cursor.close()
                logger.info(f"Initialized seen_urls with {len(self.seen_urls)} URLs from DB.")
            except self.storage.conn.Error as e:
                logger.error(f"Error loading seen URLs from DB: {e}")

        if self.config.resume:
            # Resuming: Frontier is already in DB. We might check its size or status.
            # For now, we assume StorageManager handles DB state correctly.
            count = self.count_frontier()
            logger.info(f"Resuming crawl. Frontier has {count} URLs.")
            if count == 0:
                 logger.warning("Resuming with an empty frontier. Attempting to load seeds.")
                 await self._load_seeds()
        else:
            # New crawl: clear existing frontier (if any) and load seeds.
            # Note: PLAN.md implies we might error if data_dir exists and not resuming.
            # For now, let's clear and proceed for simplicity in this stage.
            logger.info("Starting new crawl. Clearing any existing frontier and loading seeds.")
            self._clear_frontier_db() # Ensure a fresh start if not resuming
            await self._load_seeds()

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

    def _clear_frontier_db(self):
        if not self.storage.conn:
            logger.error("Cannot clear frontier, no DB connection.")
            return
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("DELETE FROM frontier")
            self.storage.conn.commit()
            self.seen_urls.clear() # Also clear in-memory set
            logger.info("Cleared frontier table in database.")
        except self.storage.conn.Error as e:
            logger.error(f"Error clearing frontier table: {e}")
            self.storage.conn.rollback()
        finally:
            if cursor: cursor.close()

    async def add_url(self, url: str, depth: int = 0) -> bool:
        """Adds a normalized URL to the frontier if not already seen or invalid domain."""
        normalized_url = normalize_url(url)
        if not normalized_url:
            return False

        if normalized_url in self.seen_urls:
            return False

        domain = extract_domain(normalized_url)
        if not domain:
            logger.warning(f"Could not extract domain from URL: {normalized_url}, skipping.")
            return False
        
        # Use PolitenessEnforcer to check if URL is allowed before even adding to DB seen_urls or frontier
        # This is an early check. is_url_allowed also checks manual exclusions.
        if not self.politeness.is_url_allowed(normalized_url):
            logger.debug(f"URL {normalized_url} disallowed by politeness rules (e.g. manual exclude or robots), not adding.")
            self.seen_urls.add(normalized_url) # Add to seen so we don't re-check repeatedly
            return False

        if not self.storage.conn:
            logger.error("Cannot add URL to frontier, no DB connection.")
            return False

        try:
            cursor = self.storage.conn.cursor()
            # The UNIQUE constraint on url will prevent duplicates at DB level.
            # We also check seen_urls for an in-memory speedup.
            cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", (self.storage.get_url_sha256(normalized_url),))
            if cursor.fetchone():
                # logger.debug(f"URL already visited (in DB): {normalized_url}")
                self.seen_urls.add(normalized_url) # Ensure it's in memory for future checks
                return False

            cursor.execute(
                "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score) VALUES (?, ?, ?, ?, ?)",
                (normalized_url, domain, depth, int(time.time()), 0) # Default priority 0 for now
            )
            if cursor.rowcount > 0:
                self.seen_urls.add(normalized_url)
                # logger.debug(f"Added to frontier: {normalized_url}")
                self.storage.conn.commit()
                return True
            else:
                # logger.debug(f"URL likely already in frontier (DB IGNORE): {normalized_url}")
                self.seen_urls.add(normalized_url) # Ensure it's in memory
                return False # Not newly added
        except self.storage.conn.Error as e:
            logger.error(f"Error adding URL {normalized_url} to frontier: {e}")
            self.storage.conn.rollback()
            return False
        finally:
            if cursor: cursor.close()
    
    def count_frontier(self) -> int:
        """Counts the number of URLs currently in the frontier table."""
        if not self.storage.conn:
            return 0
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM frontier")
            count = cursor.fetchone()[0]
            return count
        except self.storage.conn.Error as e:
            logger.error(f"Error counting frontier: {e}")
            return 0
        finally:
            if cursor: cursor.close()

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Removes the URL from the frontier upon retrieval.
        """
        if not self.storage.conn:
            logger.error("Cannot get next URL, no DB connection.")
            return None

        # In a more advanced system, we might fetch a batch of candidates
        # and use a more sophisticated priority queue that considers domain diversity and politeness.
        # For now, we iterate and take the first compliant URL.
        # The ORDER BY clause can be adjusted for different prioritization strategies.
        # e.g., ORDER BY priority_score DESC, added_timestamp ASC
        
        # How many candidates to check from DB before giving up for this attempt.
        # This prevents stalling if the top N urls are all from domains waiting for cooldown.
        candidate_check_limit = self.config.max_workers * 5 # Check a few per worker
        
        selected_url_info = None
        cursor = None # Ensure cursor is defined for finally block

        try:
            cursor = self.storage.conn.cursor()
            # Fetch a batch of candidates. ORDER BY added_timestamp for FIFO within available domains.
            # A more complex query could pre-filter by domain last_fetch_time if it were easily joinable.
            cursor.execute(
                "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                (candidate_check_limit,)
            )
            candidates = cursor.fetchall()

            if not candidates:
                logger.info("Frontier is empty based on current query.")
                return None

            for url_id, url, domain in candidates:
                # Check 1: Is the URL allowed by robots.txt and not manually excluded?
                # This call might trigger a robots.txt fetch if not cached by PolitenessEnforcer.
                # In a fully async model, this would be an awaitable call.
                if not self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules. Removing from frontier.")
                    # If disallowed, remove from frontier to prevent re-checking.
                    cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                    self.storage.conn.commit() # Commit removal of bad URL
                    self.seen_urls.add(url) # Ensure it is marked as seen to avoid re-adding
                    continue # Try next candidate

                # Check 2: Can we fetch from this domain now (respecting crawl delay)?
                if self.politeness.can_fetch_domain_now(domain):
                    # This URL is good to go!
                    self.politeness.record_domain_fetch_attempt(domain) # Important: update last fetch time
                    
                    # Delete the URL from frontier as it's being handed out
                    cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                    self.storage.conn.commit() 
                    
                    logger.debug(f"Retrieved from frontier: {url} (ID: {url_id}) for domain {domain}")
                    selected_url_info = (url, domain, url_id)
                    break # Found a suitable URL
                else:
                    # logger.debug(f"Domain {domain} (for URL {url}) not ready due to crawl delay.")
                    pass # Keep in frontier, try another candidate
            
            if not selected_url_info:
                logger.debug(f"No suitable URL found in the first {len(candidates)} candidates that respects politeness rules now.")
                # This might mean all available domains are on cooldown.

        except self.storage.conn.Error as e:
            logger.error(f"Error getting next URL from frontier: {e}")
            if self.storage.conn: self.storage.conn.rollback()
            return None # Ensure rollback on error before returning
        except Exception as e: # Catch other unexpected errors from politeness checks etc.
            logger.error(f"Unexpected error during get_next_url: {e}", exc_info=True)
            return None
        finally:
            if cursor: cursor.close()
        
        return selected_url_info 