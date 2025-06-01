import asyncio
import logging
import time
from pathlib import Path
from typing import Set, Optional
import os # For process ID
import psutil

from .config import CrawlerConfig
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .parser import PageParser, ParseResult
from .utils import extract_domain # For getting domain for storage
from .db_backends import create_backend

logger = logging.getLogger(__name__)

# How long a worker should sleep if the frontier is temporarily empty 
# or all domains are on cooldown, before trying again.
EMPTY_FRONTIER_SLEEP_SECONDS = 10
# How often the main orchestrator loop checks status and stopping conditions
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

class CrawlerOrchestrator:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        data_dir_path = Path(config.data_dir)
        try:
            data_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured data directory exists: {data_dir_path}")
        except OSError as e:
            logger.error(f"Critical error creating data directory {data_dir_path}: {e}")
            raise # Stop if we can't create data directory

        # Initialize database backend
        if config.db_type == 'sqlite':
            self.db_backend = create_backend(
                'sqlite',
                db_path=data_dir_path / "crawler_state.db",
                pool_size=config.max_workers,
                timeout=30
            )
        else:  # PostgreSQL
            # For PostgreSQL, scale pool size with workers but cap it
            min_pool = min(10, config.max_workers)
            max_pool = min(config.max_workers + 10, 100)  # Cap at 100 connections
            self.db_backend = create_backend(
                'postgresql',
                db_url=config.db_url,
                min_size=min_pool,
                max_size=max_pool
            )
        
        self.storage: StorageManager = StorageManager(config, self.db_backend)
        self.fetcher: Fetcher = Fetcher(config)
        self.politeness: PolitenessEnforcer = PolitenessEnforcer(config, self.storage, self.fetcher)
        self.frontier: FrontierManager = FrontierManager(config, self.storage, self.politeness)
        self.parser: PageParser = PageParser()

        self.pages_crawled_count: int = 0
        self.start_time: float = 0.0
        self.worker_tasks: Set[asyncio.Task] = set()
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self.total_urls_added_to_frontier: int = 0

    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Worker-{worker_id}: Starting.")
        try:
            while not self._shutdown_event.is_set():
                next_url_info = await self.frontier.get_next_url()

                if next_url_info is None:
                    # Check if the frontier is truly empty (not just all domains on cooldown)
                    current_frontier_size_for_worker = await self.frontier.count_frontier()
                    if current_frontier_size_for_worker == 0:
                        logger.info(f"Worker-{worker_id}: Frontier is confirmed empty by count. Waiting...")
                    else:
                        # logger.debug(f"Worker-{worker_id}: No suitable URL available (cooldowns?). Waiting... Frontier size: {current_frontier_size_for_worker}")
                        pass # Still URLs, but none fetchable now
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                    continue

                url_to_crawl, domain, frontier_id = next_url_info
                logger.info(f"Worker-{worker_id}: Processing URL ID {frontier_id}: {url_to_crawl} from domain {domain}")

                fetch_result: FetchResult = await self.fetcher.fetch_url(url_to_crawl)
                crawled_timestamp = int(time.time())

                if fetch_result.error_message or fetch_result.status_code >= 400:
                    logger.warning(f"Worker-{worker_id}: Fetch failed for {url_to_crawl}. Status: {fetch_result.status_code}, Error: {fetch_result.error_message}")
                    # Record failed attempt
                    await self.storage.add_visited_page(
                        url=fetch_result.final_url, 
                        domain=extract_domain(fetch_result.final_url) or domain, # Use original domain as fallback
                        status_code=fetch_result.status_code,
                        crawled_timestamp=crawled_timestamp,
                        content_type=fetch_result.content_type,
                        redirected_to_url=fetch_result.final_url if fetch_result.is_redirect and fetch_result.initial_url != fetch_result.final_url else None
                    )
                else: # Successful fetch (2xx or 3xx that was followed)
                    self.pages_crawled_count += 1
                    logger.info(f"Worker-{worker_id}: Successfully fetched {fetch_result.final_url} (Status: {fetch_result.status_code}). Total crawled: {self.pages_crawled_count}")
                    
                    content_storage_path_str: str | None = None
                    parse_data: Optional[ParseResult] = None

                    if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
                        logger.debug(f"Worker-{worker_id}: Parsing HTML content for {fetch_result.final_url}")
                        parse_data = self.parser.parse_html_content(fetch_result.text_content, fetch_result.final_url)
                        
                        if parse_data.text_content:
                            url_hash = self.storage.get_url_sha256(fetch_result.final_url)
                            saved_path = await self.storage.save_content_to_file(url_hash, parse_data.text_content)
                            if saved_path:
                                # Store relative path from data_dir for portability
                                try:
                                    content_storage_path_str = str(saved_path.relative_to(self.config.data_dir))
                                except ValueError: # Should not happen if content_dir is child of data_dir
                                    content_storage_path_str = str(saved_path)
                                logger.debug(f"Worker-{worker_id}: Saved content for {fetch_result.final_url} to {content_storage_path_str}")
                        
                        if parse_data.extracted_links:
                            logger.debug(f"Worker-{worker_id}: Found {len(parse_data.extracted_links)} links on {fetch_result.final_url}")
                            links_added_this_page = 0
                            for link in parse_data.extracted_links:
                                if await self.frontier.add_url(link):
                                    links_added_this_page +=1
                            if links_added_this_page > 0:
                                self.total_urls_added_to_frontier += links_added_this_page
                                logger.info(f"Worker-{worker_id}: Added {links_added_this_page} new URLs to frontier from {fetch_result.final_url}. Total added: {self.total_urls_added_to_frontier}")
                    else:
                        logger.debug(f"Worker-{worker_id}: Not HTML or no text content for {fetch_result.final_url} (Type: {fetch_result.content_type})")
                    
                    # Record success
                    await self.storage.add_visited_page(
                        url=fetch_result.final_url,
                        domain=extract_domain(fetch_result.final_url) or domain, # Use original domain as fallback
                        status_code=fetch_result.status_code,
                        crawled_timestamp=crawled_timestamp,
                        content_type=fetch_result.content_type,
                        content_text=parse_data.text_content if parse_data else None, # For hashing
                        content_storage_path_str=content_storage_path_str,
                        redirected_to_url=fetch_result.final_url if fetch_result.is_redirect and fetch_result.initial_url != fetch_result.final_url else None
                    )
                
                # Check stopping conditions after processing a page
                if self._check_stopping_conditions():
                    self._shutdown_event.set()
                    logger.info(f"Worker-{worker_id}: Stopping condition met, signaling shutdown.")
                    break 
                
                await asyncio.sleep(0) # Yield control to event loop

        except asyncio.CancelledError:
            logger.info(f"Worker-{worker_id}: Cancelled.")
        except Exception as e:
            logger.error(f"Worker-{worker_id}: Unhandled exception: {e}", exc_info=True)
        finally:
            logger.info(f"Worker-{worker_id}: Shutting down.")

    def _check_stopping_conditions(self) -> bool:
        if self.config.max_pages and self.pages_crawled_count >= self.config.max_pages:
            logger.info(f"Stopping: Max pages reached ({self.pages_crawled_count}/{self.config.max_pages})")
            return True
        if self.config.max_duration and (time.time() - self.start_time) >= self.config.max_duration:
            logger.info(f"Stopping: Max duration reached ({time.time() - self.start_time:.0f}s / {self.config.max_duration}s)")
            return True
        # More complex: check if frontier is empty AND all workers are idle (e.g. waiting on frontier.get_next_url)
        # This is handled by the orchestrator's main loop watching worker states and frontier count.
        return False

    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")
        current_process = psutil.Process(os.getpid())

        try:
            # Initialize database
            await self.db_backend.initialize()
            await self.storage.init_db_schema()
            
            # Initialize frontier (loads seeds etc.)
            await self.frontier.initialize_frontier()
            initial_frontier_size = await self.frontier.count_frontier()
            self.total_urls_added_to_frontier = initial_frontier_size
            logger.info(f"Frontier initialized. Size: {initial_frontier_size}")

            if initial_frontier_size == 0 and not self.config.resume:
                logger.warning("No URLs in frontier after seed loading. Nothing to crawl.")
                self._shutdown_event.set() # Ensure a clean shutdown path
            elif initial_frontier_size == 0 and self.config.resume:
                logger.warning("Resuming with an empty frontier. Crawler will wait for new URLs or shutdown if none appear.")
            
            # Start worker tasks
            for i in range(self.config.max_workers):
                task = asyncio.create_task(self._worker(i + 1))
                self.worker_tasks.add(task)
            
            logger.info(f"Started {len(self.worker_tasks)} worker tasks.")

            # Main monitoring loop
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All worker tasks have completed.")
                    self._shutdown_event.set()
                    break
                
                current_frontier_size = await self.frontier.count_frontier()
                active_workers = sum(1 for task in self.worker_tasks if not task.done())
                
                # Resource monitoring
                rss_mem_mb = "N/A"
                fds_count = "N/A"
                if current_process:
                    try:
                        rss_mem_bytes = current_process.memory_info().rss
                        rss_mem_mb = f"{rss_mem_bytes / (1024 * 1024):.2f} MB"
                        fds_count = current_process.num_fds()
                    except psutil.Error as e:
                        logger.warning(f"psutil error during resource monitoring: {e}")
                        # Fallback or disable further psutil calls for this iteration if needed
                
                # Connection pool info - different for each backend
                pool_info = "N/A"
                if self.config.db_type == 'sqlite' and hasattr(self.db_backend, '_pool'):
                    pool_info = f"available={self.db_backend._pool.qsize()}"
                elif self.config.db_type == 'postgresql':
                    pool_info = f"min={self.db_backend.min_size},max={self.db_backend.max_size}"

                status_parts = [
                    f"Crawled={self.pages_crawled_count}",
                    f"Frontier={current_frontier_size}",
                    f"AddedToFrontier={self.total_urls_added_to_frontier}",
                    f"ActiveWorkers={active_workers}/{len(self.worker_tasks)}",
                    f"DBPool({self.config.db_type})={pool_info}",
                    f"MemRSS={rss_mem_mb}",
                    f"OpenFDs={fds_count}",
                    f"Runtime={(time.time() - self.start_time):.0f}s"
                ]
                logger.info(f"Status: {', '.join(status_parts)}")
                
                # If frontier is empty and workers might be idle, it could be a natural end
                # This condition needs to be careful not to prematurely shut down if workers are just between tasks.
                # A more robust check would involve seeing if workers are truly blocked on an empty frontier for a while.
                if current_frontier_size == 0 and self.pages_crawled_count > 0: # Check if any crawling happened
                    # Wait a bit to see if workers add more URLs or finish
                    logger.info(f"Frontier is empty. Pages crawled: {self.pages_crawled_count}. Monitoring workers...")
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2) # Wait longer than worker sleep
                    current_frontier_size = await self.frontier.count_frontier()
                    if current_frontier_size == 0 and not any(not task.done() for task in self.worker_tasks):
                        logger.info("Frontier empty and all workers appear done. Signaling shutdown.")
                        self._shutdown_event.set()
                        break

                if self._check_stopping_conditions(): # Check global conditions again
                    self._shutdown_event.set()

                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=ORCHESTRATOR_STATUS_INTERVAL_SECONDS)
                except asyncio.TimeoutError:
                    pass # Continue loop for status update and checks
            
            logger.info("Shutdown signaled. Waiting for workers to finish...")
            if self.worker_tasks:
                # Give workers a chance to finish up gracefully
                done, pending = await asyncio.wait(self.worker_tasks, timeout=EMPTY_FRONTIER_SLEEP_SECONDS * 2)
                for task in pending:
                    logger.warning(f"Worker task {task.get_name()} did not finish in time, cancelling.")
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True) # Await cancellations
            logger.info("All worker tasks are finalized.")

        except Exception as e:
            logger.critical(f"Orchestrator critical error: {e}", exc_info=True)
            self._shutdown_event.set() # Ensure shutdown on critical error
            # Propagate worker cancellations if any are still running
            for task in self.worker_tasks:
                if not task.done():
                    task.cancel()
            if self.worker_tasks:
                 await asyncio.gather(*[task for task in self.worker_tasks if not task.done()], return_exceptions=True)
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            
            # Close the database
            await self.storage.close()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
            logger.info(f"Total runtime: {(time.time() - self.start_time):.2f} seconds.") 