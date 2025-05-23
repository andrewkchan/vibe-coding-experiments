import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import httpx # For synchronous robots.txt fetching for now

from .config import CrawlerConfig
from .storage import StorageManager
from .utils import extract_domain 

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_TXT_TTL = 24 * 60 * 60  # 24 hours in seconds
MIN_CRAWL_DELAY_SECONDS = 70 # Our project's default minimum

class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager):
        self.config = config
        self.storage = storage
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {} # Cache for parsed robots.txt
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            if not self.storage.conn:
                logger.error("Cannot load manual exclusions, no DB connection.")
                return
            try:
                cursor = self.storage.conn.cursor()
                with open(self.config.exclude_file, 'r') as f:
                    count = 0
                    for line in f:
                        domain_to_exclude = line.strip().lower()
                        if domain_to_exclude and not domain_to_exclude.startswith("#"):
                            # Ensure domain exists in domain_metadata or insert it
                            cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                            cursor.execute(
                                "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                (domain_to_exclude,)
                            )
                            count += 1
                    self.storage.conn.commit()
                    logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file}")
            except IOError as e:
                logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
            except self.storage.conn.Error as e:
                logger.error(f"DB error loading manual exclusions: {e}")
                if self.storage.conn: self.storage.conn.rollback()
            finally:
                if cursor: cursor.close()
        else:
            logger.info("No manual exclude file specified or found.")

    def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches, parses, and caches robots.txt for a domain."""
        if domain in self.robots_parsers:
            # TODO: Check TTL from storage before returning cached parser
            # For now, if in memory, assume it's fresh enough for this session part
            return self.robots_parsers[domain]

        if not self.storage.conn:
            logger.error(f"Cannot get robots.txt for {domain}, no DB connection.")
            return None

        rerp = RobotExclusionRulesParser()
        rerp.user_agent = self.config.user_agent # Set our user agent for the parser
        robots_url = f"http://{domain}/robots.txt" # Try HTTP first
        # In a full async system, this fetching would use the async Fetcher
        
        robots_content: str | None = None
        fetched_timestamp = int(time.time())
        expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
        
        # 1. Try to load from DB cache if not expired
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute(
                "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                (domain,)
            )
            row = cursor.fetchone()
            if row and row[0] and row[1] and row[1] > fetched_timestamp:
                logger.debug(f"Using cached robots.txt for {domain} from DB.")
                robots_content = row[0]
                expires_timestamp = row[1] # Keep original expiry
            cursor.close()
        except self.storage.conn.Error as e:
            logger.warning(f"DB error fetching cached robots.txt for {domain}: {e}")
            if cursor: cursor.close()

        # 2. If not in DB cache or expired, fetch it
        if robots_content is None:
            logger.info(f"Fetching robots.txt for {domain} from {robots_url}")
            try:
                # Using httpx for a simple synchronous GET for now.
                # This will be replaced by an async call to our Fetcher module later.
                with httpx.Client(follow_redirects=True, timeout=10.0) as client:
                    try:
                        response = client.get(robots_url, headers={"User-Agent": self.config.user_agent})
                        if response.status_code == 200:
                            robots_content = response.text
                        elif response.status_code == 404:
                            logger.debug(f"robots.txt not found (404) for {domain}. Assuming allow all.")
                            robots_content = "" # Empty content means allow all for parser
                        else:
                            logger.warning(f"Failed to fetch robots.txt for {domain}. Status: {response.status_code}. Assuming allow all.")
                            robots_content = "" 
                    except httpx.RequestError as e:
                        logger.warning(f"HTTP request error fetching robots.txt for {domain}: {e}. Assuming allow all.")
                        robots_content = ""
                    
                    # Try HTTPS if HTTP failed and wasn't just a 404 or similar
                    if robots_content is None or (response.status_code != 200 and response.status_code != 404):
                        robots_url_https = f"https://{domain}/robots.txt"
                        logger.info(f"Retrying robots.txt for {domain} with HTTPS: {robots_url_https}")
                        try:
                            response_https = client.get(robots_url_https, headers={"User-Agent": self.config.user_agent})
                            if response_https.status_code == 200:
                                robots_content = response_https.text
                            elif response_https.status_code == 404:
                                logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                                robots_content = "" 
                            else:
                                logger.warning(f"Failed to fetch robots.txt via HTTPS for {domain}. Status: {response_https.status_code}. Assuming allow all.")
                                robots_content = ""
                        except httpx.RequestError as e:
                            logger.warning(f"HTTP request error fetching robots.txt via HTTPS for {domain}: {e}. Assuming allow all.")
                            robots_content = ""

                # Update DB with fetched content (even if empty for 404s)
                if robots_content is not None and self.storage.conn:
                    cursor = self.storage.conn.cursor()
                    cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                    cursor.execute(
                        "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                        (robots_content, fetched_timestamp, expires_timestamp, domain)
                    )
                    self.storage.conn.commit()
                    logger.debug(f"Cached robots.txt for {domain} in DB.")
                    if cursor: cursor.close()

            except Exception as e:
                logger.error(f"Unexpected error fetching robots.txt for {domain}: {e}")
                robots_content = "" # Fallback to allow all on unexpected error
        
        if robots_content is not None:
            rerp.parse(robots_content)
        else: # Should not happen if fallback works, but as a safeguard
            rerp.parse("") # Default to allow all if content is truly None
        
        self.robots_parsers[domain] = rerp
        return rerp

    def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True # Or False, depending on strictness. Allowing is safer for not missing pages.

        # 1. Check manual exclusion first
        if self.storage.conn:
            try:
                cursor = self.storage.conn.cursor()
                cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
                row = cursor.fetchone()
                if cursor: cursor.close()
                if row and row[0] == 1:
                    logger.debug(f"URL {url} from manually excluded domain: {domain}")
                    return False
            except self.storage.conn.Error as e:
                logger.warning(f"DB error checking manual exclusion for {domain}: {e}")

        # 2. Check robots.txt
        rerp = self._get_robots_for_domain(domain)
        if rerp:
            is_allowed = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed
        
        logger.warning(f"No robots.txt parser available for {domain}. Allowing URL: {url}")
        return True # Default to allow if robots.txt processing failed severely

    def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain (from robots.txt or default)."""
        rerp = self._get_robots_for_domain(domain)
        delay = None
        if rerp:
            # robotexclusionrulesparser uses get_crawl_delay, expects user_agent
            # It can return None if no specific delay for our agent, or a float
            agent_delay = rerp.get_crawl_delay(self.config.user_agent)
            if agent_delay is not None:
                delay = float(agent_delay)
                logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (agent: {self.config.user_agent})")
        
        if delay is None: # No specific delay, or no delay for our agent, or generic delay
            if rerp and rerp.get_crawl_delay("*") is not None: # Check for wildcard agent delay
                 wildcard_delay = rerp.get_crawl_delay("*")
                 if wildcard_delay is not None:
                    delay = float(wildcard_delay)
                    logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (wildcard agent)")
            
        if delay is None:
            logger.debug(f"No crawl delay specified in robots.txt for {domain}. Using default: {MIN_CRAWL_DELAY_SECONDS}s")
            return float(MIN_CRAWL_DELAY_SECONDS)
        
        return max(float(delay), float(MIN_CRAWL_DELAY_SECONDS)) # Ensure our minimum is respected

    def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay."""
        if not self.storage.conn:
            logger.error("Cannot check fetch_domain_now, no DB connection.")
            return False # Safer to not fetch

        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,))
            row = cursor.fetchone()
            if cursor: cursor.close()

            last_fetch_time = 0
            if row and row[0] is not None:
                last_fetch_time = row[0]
            
            crawl_delay = self.get_crawl_delay(domain)
            current_time = int(time.time())
            
            if current_time >= last_fetch_time + crawl_delay:
                return True
            else:
                # logger.debug(f"Domain {domain} cannot be fetched yet. Last fetch: {last_fetch_time}, delay: {crawl_delay}, current: {current_time}")
                return False
        except self.storage.conn.Error as e:
            logger.error(f"DB error checking can_fetch_domain_now for {domain}: {e}")
            return False # Safer to not fetch

    def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain."""
        if not self.storage.conn:
            logger.error("Cannot record domain fetch, no DB connection.")
            return
        try:
            current_time = int(time.time())
            cursor = self.storage.conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)", 
                           (domain, current_time))
            cursor.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
                           (current_time, domain))
            self.storage.conn.commit()
            # logger.debug(f"Recorded fetch attempt for domain {domain} at {current_time}")
        except self.storage.conn.Error as e:
            logger.error(f"DB error recording fetch attempt for {domain}: {e}")
            if self.storage.conn: self.storage.conn.rollback()
        finally:
            if cursor: cursor.close() 