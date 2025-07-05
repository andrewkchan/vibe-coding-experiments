import asyncio
import logging
import time
import pickle
import os
from typing import Set, Optional
import psutil
import random
import functools
from urllib.parse import urlparse

import redis.asyncio as redis
from redis.asyncio import BlockingConnectionPool

from .config import CrawlerConfig
from .pod_manager import PodManager
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .redis_shield import ShieldedRedis
from .utils import extract_domain
from .metrics import (
    pages_crawled_counter,
    backpressure_events_counter,
    fetch_duration_histogram,
    active_workers_gauge,
    fetch_counter,
    fetch_error_counter,
    parse_queue_size_gauge,
    content_size_histogram,
    fetcher_pages_per_second_gauge,
    process_memory_usage_gauge,
    process_open_fds_gauge,
)

logger = logging.getLogger(__name__)

# How long a worker should sleep if the frontier is temporarily empty 
# or all domains are on cooldown, before trying again.
EMPTY_FRONTIER_SLEEP_SECONDS = 10

class FetcherProcess:
    """Fetcher process that runs multiple async workers to fetch URLs."""
    
    def __init__(self, config: CrawlerConfig, fetcher_id: int, pod_id: int = 0):
        self.config = config
        self.fetcher_id = fetcher_id
        self.pod_id = pod_id
        self.fetcher = Fetcher(config)
        
        # Initialize PodManager to get Redis connections for this pod
        self.pod_manager = PodManager(config)
        
        # We'll initialize Redis clients in async _init_redis method
        self.redis_client: Optional[ShieldedRedis] = None
        self.redis_client_binary: Optional[ShieldedRedis] = None
        
        # Components will be initialized after Redis clients
        self.storage: Optional[StorageManager] = None
        self.politeness: Optional[PolitenessEnforcer] = None
        self.frontier: Optional[FrontierManager] = None
        
        # Fetcher state
        self.worker_tasks: Set[asyncio.Task] = set()
        self.pages_crawled_count = 0
        self.max_backpressure_sleep = 5.0  # Maximum sleep time for backpressure
        self._shutdown_event = asyncio.Event()
        
        # Tracking for pages per second metric
        self.pages_crawled_in_interval = 0
        self.last_metrics_update_time = time.time()
        
        # For resource monitoring
        self.process = psutil.Process(os.getpid())
        
        # Active workers tracking
        self.active_workers_count = 0
        
        # Backpressure configuration (same as orchestrator)
        self.fetch_queue_soft_limit = config.parse_queue_soft_limit
        self.fetch_queue_hard_limit = config.parse_queue_hard_limit
        
        logger.info(f"Fetcher process {fetcher_id} for pod {pod_id} initialized")
    
    async def _init_async_components(self):
        """Initialize components that require async setup."""
        # Get Redis clients for this pod
        self.redis_client = await self.pod_manager.get_redis_client(self.pod_id, binary=False)
        self.redis_client_binary = await self.pod_manager.get_redis_client(self.pod_id, binary=True)
        
        # Initialize components with pod-specific Redis
        self.storage = StorageManager(self.config, self.redis_client)
        self.politeness = PolitenessEnforcer(self.config, self.redis_client, self.fetcher)
        self.frontier = FrontierManager(self.config, self.politeness, self.redis_client, pod_id=self.pod_id)
        
        logger.info(f"Fetcher-{self.fetcher_id}: Async components initialized for pod {self.pod_id}")
    
    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Starting.")
        
        # Add small random delay to spread out initial requests
        startup_delay = (worker_id % 100) * (5 / 100)  # 0-5 second delay based on worker ID
        await asyncio.sleep(startup_delay)
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Get next URL from this pod's frontier
                    next_url_info = await self.frontier.get_next_url()

                    if next_url_info is None:
                        # No suitable URL available
                        await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                        continue

                    url_to_crawl, domain, frontier_id, depth = next_url_info
                    logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Processing URL at depth {depth}: {url_to_crawl}")

                    # Track fetch duration
                    fetch_start_time = time.time()
                    fetch_result: FetchResult = await self.fetcher.fetch_url(url_to_crawl)
                    fetch_duration = time.time() - fetch_start_time
                    fetch_duration_histogram.labels(
                        pod_id=str(self.pod_id),
                        fetch_type='page'
                    ).observe(fetch_duration)
                    
                    crawled_timestamp = int(time.time())

                    if fetch_result.error_message or fetch_result.status_code >= 400:
                        logger.warning(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Fetch failed for {url_to_crawl}. Status: {fetch_result.status_code}, Error: {fetch_result.error_message}")
                        
                        # Track error in Prometheus
                        if fetch_result.error_message:
                            fetch_error_counter.labels(
                                pod_id=str(self.pod_id),
                                error_type='fetch_error',
                                fetch_type='page'
                            ).inc()
                        else:
                            fetch_error_counter.labels(
                                pod_id=str(self.pod_id),
                                error_type=f'http_{fetch_result.status_code}',
                                fetch_type='page'
                            ).inc()
                        
                        # Record failed attempt
                        await self.storage.add_visited_page(
                            url=fetch_result.final_url, 
                            status_code=fetch_result.status_code,
                            crawled_timestamp=crawled_timestamp,
                            content_type=fetch_result.content_type
                        )
                    else:  # Successful fetch
                        self.pages_crawled_count += 1
                        self.pages_crawled_in_interval += 1
                        pages_crawled_counter.labels(pod_id=str(self.pod_id)).inc()
                        logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Successfully fetched {fetch_result.final_url}. Total crawled: {self.pages_crawled_count}")
                        
                        if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
                            # Push to this pod's parse queue
                            queue_item = {
                                'url': fetch_result.final_url,
                                'domain': domain,
                                'depth': depth,
                                'html_content': fetch_result.text_content,
                                'content_type': fetch_result.content_type,
                                'crawled_timestamp': crawled_timestamp,
                                'status_code': fetch_result.status_code
                            }
                            
                            # Push to Redis queue for parsing
                            queue_size = await self.redis_client_binary.rpush('fetch:queue', pickle.dumps(queue_item, protocol=pickle.HIGHEST_PROTOCOL))
                            logger.debug(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Pushed HTML to parse queue")
                            
                            # Explicitly delete to keep peak memory down
                            del queue_item
                            
                            # Backpressure logic
                            if queue_size > self.fetch_queue_hard_limit:
                                logger.warning(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Queue at hard limit ({queue_size}/{self.fetch_queue_hard_limit}), waiting...")
                                backpressure_events_counter.labels(
                                    pod_id=str(self.pod_id),
                                    backpressure_type='hard_limit'
                                ).inc()
                                while queue_size > self.fetch_queue_soft_limit:
                                    await asyncio.sleep(5.0)
                                    queue_size = await self.redis_client_binary.llen('fetch:queue')
                                logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Queue below soft limit, resuming")
                                
                            elif queue_size > self.fetch_queue_soft_limit:
                                # Progressive backoff
                                overflow_ratio = (queue_size - self.fetch_queue_soft_limit) / (self.fetch_queue_hard_limit - self.fetch_queue_soft_limit)
                                overflow_ratio = min(overflow_ratio, 1.0)
                                
                                base_sleep = overflow_ratio * 2.0
                                jitter = random.random() * 0.5
                                sleep_time = base_sleep + jitter
                                
                                if worker_id % 10 == 1:  # Log from only 10% of workers
                                    logger.debug(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Queue at {queue_size}, backpressure sleep {sleep_time:.2f}s")
                                backpressure_events_counter.labels(
                                    pod_id=str(self.pod_id),
                                    backpressure_type='soft_limit'
                                ).inc()
                                await asyncio.sleep(sleep_time)
                        else:
                            logger.debug(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Not HTML for {fetch_result.final_url}")
                            
                            # For non-HTML content, still record in visited pages
                            await self.storage.add_visited_page(
                                url=fetch_result.final_url,
                                status_code=fetch_result.status_code,
                                crawled_timestamp=crawled_timestamp,
                                content_type=fetch_result.content_type,
                                content_storage_path_str=None
                            )
                    
                    # Delete fetch results to free memory
                    del fetch_result
                    
                    await asyncio.sleep(0)  # Yield control

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Error processing URL: {e}")
                    fetch_error_counter.labels(
                        pod_id=str(self.pod_id),
                        error_type='worker_error',
                        fetch_type='page'
                    ).inc()
                    await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Cancelled.")
        finally:
            logger.info(f"Fetcher-{self.fetcher_id} Worker-{worker_id}: Shutting down.")
    
    def _get_ready_domain_timeout(self) -> float:
        """Calculate timeout for getting ready domains based on active workers."""
        # If we have many active workers, use a shorter timeout to be more responsive
        # If few workers, use a longer timeout to avoid busy-waiting
        if self.active_workers_count > self.config.fetcher_workers * 0.8:
            return 0.1  # 100ms when busy
        elif self.active_workers_count > self.config.fetcher_workers * 0.5:
            return 0.5  # 500ms when moderately busy
        else:
            return 2.0  # 2s when idle
    
    async def _update_metrics(self):
        """Update per-process metrics periodically."""
        current_time = time.time()
        time_elapsed = current_time - self.last_metrics_update_time
        
        if time_elapsed >= 5.0:  # Update every 5 seconds
            # Calculate pages per second
            pages_per_second = self.pages_crawled_in_interval / time_elapsed if time_elapsed > 0 else 0
            fetcher_pages_per_second_gauge.labels(
                pod_id=str(self.pod_id),
                fetcher_id=str(self.fetcher_id)
            ).set(pages_per_second)
            
            # Update resource metrics
            try:
                memory_usage = self.process.memory_info().rss
                process_memory_usage_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='fetcher',
                    process_id=str(self.fetcher_id)
                ).set(memory_usage)
                
                open_fds = self.process.num_fds()
                process_open_fds_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='fetcher', 
                    process_id=str(self.fetcher_id)
                ).set(open_fds)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass  # Process might be shutting down
            
            # Reset interval counters
            self.pages_crawled_in_interval = 0
            self.last_metrics_update_time = current_time
            
            logger.info(f"[Metrics] Fetcher-{self.fetcher_id} Pod-{self.pod_id}: Pages/sec: {pages_per_second:.2f}")
    
    async def run(self):
        """Main entry point for the fetcher process."""
        logger.info(f"Fetcher process {self.fetcher_id} for pod {self.pod_id} starting...")
        
        # Initialize async components
        await self._init_async_components()
        
        # Note: One-time initialization (init_db_schema, load_manual_exclusions, initialize_frontier)
        # is handled by the orchestrator before starting fetcher processes
        
        # Start worker tasks
        logger.info(f"Fetcher-{self.fetcher_id}: Starting {self.config.fetcher_workers} workers...")
        
        # Stagger worker startup
        workers_per_batch = 100
        startup_delay = 5
        
        for i in range(self.config.fetcher_workers):
            task = asyncio.create_task(self._worker(i + 1))
            self.worker_tasks.add(task)
            
            if (i + 1) % workers_per_batch == 0:
                logger.info(f"Fetcher-{self.fetcher_id}: Started {i + 1}/{self.config.fetcher_workers} workers...")
                await asyncio.sleep(startup_delay)
        
        logger.info(f"Fetcher-{self.fetcher_id}: Started all {len(self.worker_tasks)} workers.")
        
        # Wait for shutdown signal or all workers to complete
        try:
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info(f"Fetcher-{self.fetcher_id}: All workers completed.")
                    break
                
                # Update active workers gauge
                active_count = sum(1 for task in self.worker_tasks if not task.done())
                if active_count != self.active_workers_count:
                    self.active_workers_count = active_count
                    active_workers_gauge.labels(pod_id=str(self.pod_id)).set(active_count)
                
                # Update per-process metrics
                await self._update_metrics()
                
                # Check status periodically
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Fetcher-{self.fetcher_id}: Critical error: {e}", exc_info=True)
        finally:
            logger.info(f"Fetcher-{self.fetcher_id}: Shutting down...")
            
            # Cancel any remaining workers
            for task in self.worker_tasks:
                if not task.done():
                    task.cancel()
            
            if self.worker_tasks:
                await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            
            # Close connections
            await self.fetcher.close_session()
            if self.storage:
                await self.storage.close()
            await self.pod_manager.close_all()
            
            logger.info(f"Fetcher-{self.fetcher_id}: Shutdown complete. Pages crawled: {self.pages_crawled_count}")


def run_fetcher_process(config: CrawlerConfig, fetcher_id: int, pod_id: int = 0):
    """Entry point for running a fetcher as a separate process."""
    from .logging_utils import setup_pod_logging
    
    # Setup per-pod logging for the child process
    setup_pod_logging(config, pod_id, 'fetcher', fetcher_id)
    
    # Create and run the fetcher
    fetcher = FetcherProcess(config, fetcher_id, pod_id)
    asyncio.run(fetcher.run()) 