import asyncio
import logging
import time
import pickle
from pathlib import Path
from typing import Set, Optional, Dict, List, Union
import os # For process ID
import psutil
from collections import defaultdict
import functools
from pympler import tracker, muppy, summary
import objgraph
from multiprocessing import Process
import redis.asyncio as redis
import random

from .config import CrawlerConfig
from .storage import StorageManager
from .redis_storage import RedisStorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer, RedisPolitenessEnforcer
from .frontier import FrontierManager, HybridFrontierManager

from .utils import extract_domain # For getting domain for storage
from .db_backends import create_backend, PostgreSQLBackend
from .metrics_utils import calculate_percentiles
from .metrics import (
    start_metrics_server,
    pages_crawled_counter,
    urls_added_counter,
    errors_counter,
    backpressure_events_counter,
    pages_per_second_gauge,
    frontier_size_gauge,
    active_workers_gauge,
    memory_usage_gauge,
    open_fds_gauge,
    db_pool_available_gauge,
    fetch_duration_histogram,
    db_connection_acquire_histogram,
    db_query_duration_histogram,
    content_size_histogram
)

logger = logging.getLogger(__name__)

# How long a worker should sleep if the frontier is temporarily empty 
# or all domains are on cooldown, before trying again.
EMPTY_FRONTIER_SLEEP_SECONDS = 10
# How often the main orchestrator loop checks status and stopping conditions
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

METRICS_LOG_INTERVAL_SECONDS = 60
LOG_MEM_DIAGNOSTICS = False
MEM_DIAGNOSTICS_INTERVAL_SECONDS = 30

# Number of domain shards - hardcoded to 500 to match the database index
# This is not scalable but we're testing performance with a matching index
DOMAIN_SHARD_COUNT = 500

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
        
        # Set parent process ID for multiprocess metrics if using Redis
        if config.db_type == 'redis':
            # The multiprocess directory should already be set up by main.py
            # We just need to mark this as the parent process
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
            logger.info(f"Set PROMETHEUS_PARENT_PROCESS to {os.getpid()}")

        # Initialize fetcher (common to all backends)
        self.fetcher: Fetcher = Fetcher(config)
        
        # Initialize backend-specific components
        if config.db_type == 'redis':
            # Initialize Redis client with configurable connection
            self.redis_client = redis.Redis(**config.get_redis_connection_kwargs())
            # Monkey-patch redis client method to remove a useless decorator which incurs extra overhead
            self.redis_client.connection_pool.get_connection = functools.partial(
                self.redis_client.connection_pool.get_connection.__wrapped__, 
                self.redis_client.connection_pool
            )
            
            # Create a separate Redis client for binary data (pickle)
            binary_redis_kwargs = config.get_redis_connection_kwargs()
            binary_redis_kwargs['decode_responses'] = False
            self.redis_client_binary = redis.Redis(**binary_redis_kwargs)
            # Same monkey patching as above
            self.redis_client_binary.connection_pool.get_connection = functools.partial(
                self.redis_client_binary.connection_pool.get_connection.__wrapped__, 
                self.redis_client_binary.connection_pool
            )
            
            self.db_backend = None  # No SQL backend for Redis
            
            # Use Redis-based components
            self.storage: Union[StorageManager, RedisStorageManager] = RedisStorageManager(config, self.redis_client)
            self.politeness: Union[PolitenessEnforcer, RedisPolitenessEnforcer] = RedisPolitenessEnforcer(config, self.redis_client, self.fetcher)
            # HybridFrontierManager expects a PolitenessEnforcer-compatible object
            self.frontier: Union[FrontierManager, HybridFrontierManager] = HybridFrontierManager(
                config, 
                self.politeness,  # type: ignore[arg-type]  # RedisPolitenessEnforcer implements same interface
                self.redis_client
            )
            
        else:
            # SQL-based backends (SQLite or PostgreSQL)
            self.redis_client = None
            
            if config.db_type == 'sqlite':
                self.db_backend = create_backend(
                    'sqlite',
                    db_path=data_dir_path / "crawler_state.db",
                    pool_size=config.max_workers,
                    timeout=60
                )
            else:  # PostgreSQL
                # For PostgreSQL, we need to be careful about connection limits
                # PostgreSQL default max_connections is often 100
                # We need to leave some connections for monitoring and other processes
                
                # Check for manual pool size override
                pool_size_override = os.environ.get('CRAWLER_PG_POOL_SIZE')
                if pool_size_override:
                    try:
                        max_pool = int(pool_size_override)
                        min_pool = min(10, max_pool)
                        logger.info(f"Using manual PostgreSQL pool size: min={min_pool}, max={max_pool}")
                    except ValueError:
                        logger.warning(f"Invalid CRAWLER_PG_POOL_SIZE value: {pool_size_override}, using automatic sizing")
                        pool_size_override = None
                
                if not pool_size_override:
                    # Estimate reasonable pool size based on workers
                    # Not all workers need a connection simultaneously
                    # Workers spend time fetching URLs, parsing, etc.
                    concurrent_db_fraction = 0.7  # Assume ~70% of workers need DB at once
                    
                    min_pool = min(10, config.max_workers)
                    # Calculate max based on expected concurrent needs
                    estimated_concurrent = int(config.max_workers * concurrent_db_fraction)
                    # Leave 20 connections for psql, monitoring, etc.
                    safe_max = 80  # Assuming max_connections=100
                    max_pool = min(max(estimated_concurrent, 20), safe_max)
                    
                    logger.info(f"PostgreSQL pool configuration: min={min_pool}, max={max_pool} "
                               f"(for {config.max_workers} workers)")
                
                self.db_backend = create_backend(
                    'postgresql',
                    db_url=config.db_url,
                    min_size=min_pool,
                    max_size=max_pool
                )
            
            # Initialize SQL-based components
            self.storage = StorageManager(config, self.db_backend)
            self.politeness = PolitenessEnforcer(config, self.storage, self.fetcher)
            self.frontier = FrontierManager(config, self.storage, self.politeness)

        self.pages_crawled_count: int = 0
        self.start_time: float = 0.0
        self.worker_tasks: Set[asyncio.Task] = set()
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self.total_urls_added_to_frontier: int = 0

        self.pages_crawled_in_interval: int = 0
        self.last_metrics_log_time: float = time.time()
        self.mem_tracker = tracker.SummaryTracker()
        self.last_mem_diagnostics_time: float = time.time()
        
        # Parser process management
        self.parser_process: Optional[Process] = None
        self.parser_processes: List[Process] = []  # Support multiple parsers
        self.num_parser_processes = 2
        
        # Backpressure configuration
        self.fetch_queue_soft_limit = 20000  # Start slowing down at this size
        self.fetch_queue_hard_limit = 80000  # Maximum queue size

    def _start_parser_process(self):
        """Start the parser consumer as a child process."""
        if self.config.db_type != 'redis':
            logger.warning("Parser process only supported with Redis backend")
            return
            
        def run_parser():
            """Function to run in the parser process."""
            # Import inside the function to avoid issues with multiprocessing
            import asyncio
            import logging
            from .parser_consumer import ParserConsumer
            
            # Setup logging for the child process
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - [Parser] %(message)s'
            )
            
            # Create and run the parser consumer
            consumer = ParserConsumer(self.config)
            asyncio.run(consumer.run())
        
        logger.info("Starting parser consumer process...")
        self.parser_process = Process(target=run_parser, name="ParserConsumer")
        self.parser_process.daemon = False  # Don't make it daemon so it can clean up properly
        self.parser_process.start()
        logger.info(f"Parser consumer process started with PID: {self.parser_process.pid}")
    
    def _start_parser_processes(self, num_processes: Optional[int] = None):
        """Start multiple parser consumer processes."""
        if self.config.db_type != 'redis':
            logger.warning("Parser processes only supported with Redis backend")
            return
        
        num_processes = num_processes or self.num_parser_processes
        
        def run_parser(parser_id: int):
            """Function to run in the parser process."""
            # Import inside the function to avoid issues with multiprocessing
            import asyncio
            import logging
            from .parser_consumer import ParserConsumer
            
            # Setup logging for the child process
            logging.basicConfig(
                level=logging.INFO,
                format=f'%(asctime)s - %(name)s - %(levelname)s - [Parser-{parser_id}] %(message)s'
            )
            
            # Create and run the parser consumer
            consumer = ParserConsumer(self.config)
            asyncio.run(consumer.run())
        
        logger.info(f"Starting {num_processes} parser consumer processes...")
        for i in range(num_processes):
            process = Process(target=run_parser, args=(i+1,), name=f"ParserConsumer-{i+1}")
            process.daemon = False  # Don't make it daemon so it can clean up properly
            process.start()
            self.parser_processes.append(process)
            logger.info(f"Parser consumer process {i+1} started with PID: {process.pid}")
    
    def _stop_parser_process(self):
        """Stop the parser consumer process gracefully."""
        if self.parser_process and self.parser_process.is_alive():
            logger.info("Stopping parser consumer process...")
            self.parser_process.terminate()
            self.parser_process.join(timeout=10)  # Wait up to 10 seconds
            
            if self.parser_process.is_alive():
                logger.warning("Parser process didn't terminate gracefully, forcing...")
                self.parser_process.kill()
                self.parser_process.join()
            
            logger.info("Parser consumer process stopped.")
    
    def _stop_parser_processes(self):
        """Stop all parser consumer processes gracefully."""
        if not self.parser_processes:
            # Fallback to single process stop
            self._stop_parser_process()
            return
            
        logger.info(f"Stopping {len(self.parser_processes)} parser consumer processes...")
        
        # First try to terminate gracefully
        for process in self.parser_processes:
            if process.is_alive():
                process.terminate()
        
        # Wait for all to terminate
        for process in self.parser_processes:
            process.join(timeout=10)  # Wait up to 10 seconds
            
            if process.is_alive():
                logger.warning(f"Parser process {process.name} didn't terminate gracefully, forcing...")
                process.kill()
                process.join()
        
        self.parser_processes.clear()
        logger.info("All parser consumer processes stopped.")

    async def _initialize_components(self):
        """Initializes all crawler components that require async setup."""
        logger.info("Initializing crawler components...")
        
        if self.config.db_type == 'redis':
            # Clear any zombie locks from previous runs
            from .redis_lock import LockManager
            lock_manager = LockManager(self.redis_client)
            cleared_count = await lock_manager.clear_all_locks()
            if cleared_count > 0:
                logger.warning(f"Cleared {cleared_count} zombie locks from previous run")
            
            # Redis-based initialization
            await self.storage.init_db_schema()  # Sets schema version
            # RedisPolitenesEnforcer loads exclusions differently
            if hasattr(self.politeness, '_load_manual_exclusions'):
                await self.politeness._load_manual_exclusions()
            await self.frontier.initialize_frontier()
        else:
            # SQL-based initialization
            await self.db_backend.initialize()
            await self.storage.init_db_schema()
            # Load politeness-related data that needs to be available before crawl starts
            await self.politeness._load_manual_exclusions()
            await self.frontier.initialize_frontier()
            
        logger.info("Crawler components initialized.")

    async def _log_metrics(self):
        current_time = time.time()
        time_elapsed_seconds = current_time - self.last_metrics_log_time
        if time_elapsed_seconds == 0: time_elapsed_seconds = 1 # Avoid division by zero

        pages_per_second = self.pages_crawled_in_interval / time_elapsed_seconds
        logger.info(f"[Metrics] Pages Crawled/sec: {pages_per_second:.2f}")
        
        # Update Prometheus gauge
        pages_per_second_gauge.set(pages_per_second)

        # DB-specific metrics
        if self.config.db_type == 'redis':
            # Redis doesn't have connection pool metrics in the same way
            # Could add Redis-specific metrics here if needed
            pass
        elif isinstance(self.db_backend, PostgreSQLBackend): # type: ignore
            # Connection Acquire Times
            acquire_times = list(self.db_backend.connection_acquire_times) # Copy for processing
            self.db_backend.connection_acquire_times.clear()
            if acquire_times:
                p50_acquire = calculate_percentiles(acquire_times, [50])[0]
                p95_acquire = calculate_percentiles(acquire_times, [95])[0]
                logger.info(f"[Metrics] DB Conn Acquire Time (ms): P50={p50_acquire*1000:.2f}, P95={p95_acquire*1000:.2f} (from {len(acquire_times)} samples)")
                
                # Update Prometheus histogram with acquire times
                for acquire_time in acquire_times:
                    db_connection_acquire_histogram.observe(acquire_time)
            else:
                logger.info("[Metrics] DB Conn Acquire Time (ms): No samples")

            # Query Hold Times (for specific queries)
            query_hold_times_copy: Dict[str, List[float]] = defaultdict(list)
            for name, times in self.db_backend.query_hold_times.items():
                query_hold_times_copy[name] = list(times)
                times.clear() # Clear original list
            
            for query_name, hold_times in query_hold_times_copy.items():
                if hold_times:
                    p50_hold = calculate_percentiles(hold_times, [50])[0]
                    p95_hold = calculate_percentiles(hold_times, [95])[0]
                    max_hold = max(hold_times)
                    logger.info(f"[Metrics] Query Hold Time '{query_name}' (ms): P50={p50_hold*1000:.2f}, P95={p95_hold*1000:.2f}, MAX={max_hold*1000:.2f} (from {len(hold_times)} samples)")
                    
                    # Update Prometheus histogram with query times
                    for hold_time in hold_times:
                        db_query_duration_histogram.labels(query_name=query_name).observe(hold_time)
                # else: logger.info(f"[Metrics] Query Hold Time '{query_name}' (ms): No samples") # Optional: log if no samples

        # Content size metrics - extract data from Prometheus histogram
        try:
            # Get samples from content_size_histogram for HTML pages
            html_sizes = []
            non_html_sizes = []
            
            # Access the internal metrics structure (this is a bit hacky but works)
            for metric in content_size_histogram._metrics.values():
                labels = metric._labelnames
                if 'page' in labels.get('fetch_type', '') or labels.get('fetch_type') == 'page':
                    if labels.get('content_type') == 'html':
                        # Get the sum and count from the histogram
                        sum_value = metric._sum._value.get()
                        count_value = metric._count._value.get()
                        if count_value > 0:
                            avg_size = sum_value / count_value
                            logger.info(f"[Metrics] HTML Content Size: Avg={avg_size/1024:.1f}KB, Count={count_value}")
                    elif labels.get('content_type') == 'non_html':
                        sum_value = metric._sum._value.get()
                        count_value = metric._count._value.get()
                        if count_value > 0:
                            avg_size = sum_value / count_value
                            logger.info(f"[Metrics] Non-HTML Content Size: Avg={avg_size/1024:.1f}KB, Count={count_value}")
        except Exception as e:
            # If we can't extract from Prometheus, just log that metrics are available
            logger.debug(f"Could not extract content size metrics: {e}")
            logger.info("[Metrics] Content size metrics available at Prometheus endpoint /metrics")

        # Reset for next interval
        self.pages_crawled_in_interval = 0
        self.last_metrics_log_time = current_time

    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Worker-{worker_id}: Starting.")
        
        # Add small random delay to spread out initial DB access
        startup_delay = (worker_id % 100) * (5 / 100)  # 0-5 second delay based on worker ID
        await asyncio.sleep(startup_delay)
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Calculate which domain shard this worker should handle
                    shard_id = (worker_id - 1) % DOMAIN_SHARD_COUNT  # worker_id is 1-based, convert to 0-based
                    next_url_info = await self.frontier.get_next_url(shard_id, DOMAIN_SHARD_COUNT)

                    if next_url_info is None:
                        # Check if the frontier is truly empty before sleeping
                        if await self.frontier.is_empty():
                            logger.info(f"Worker-{worker_id}: Frontier is confirmed empty. Waiting...")
                        else:
                            # logger.debug(f"Worker-{worker_id}: No suitable URL available (cooldowns?). Waiting...")
                            pass # Still URLs, but none fetchable now
                        await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                        continue

                    url_to_crawl, domain, frontier_id, depth = next_url_info
                    logger.info(f"Worker-{worker_id}: Processing URL ID {frontier_id} at depth {depth}: {url_to_crawl} from domain {domain}")

                    # Track fetch duration
                    fetch_start_time = time.time()
                    fetch_result: FetchResult = await self.fetcher.fetch_url(url_to_crawl)
                    fetch_duration = time.time() - fetch_start_time
                    fetch_duration_histogram.labels(fetch_type='page').observe(fetch_duration)
                    
                    crawled_timestamp = int(time.time())

                    if fetch_result.error_message or fetch_result.status_code >= 400:
                        logger.warning(f"Worker-{worker_id}: Fetch failed for {url_to_crawl}. Status: {fetch_result.status_code}, Error: {fetch_result.error_message}")
                        
                        # Track error in Prometheus
                        if fetch_result.error_message:
                            errors_counter.labels(error_type='fetch_error').inc()
                        else:
                            errors_counter.labels(error_type=f'http_{fetch_result.status_code}').inc()
                        
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
                        pages_crawled_counter.inc()  # Increment Prometheus counter
                        logger.info(f"Worker-{worker_id}: Successfully fetched {fetch_result.final_url} (Status: {fetch_result.status_code}). Total crawled: {self.pages_crawled_count}")
                        
                        if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
                            # Push to parse queue instead of parsing inline
                            queue_item = {
                                'url': fetch_result.final_url,
                                'domain': domain,
                                'depth': depth,
                                'html_content': fetch_result.text_content,
                                'content_type': fetch_result.content_type,
                                'crawled_timestamp': crawled_timestamp,
                                'status_code': fetch_result.status_code,
                                'is_redirect': fetch_result.is_redirect,
                                'initial_url': fetch_result.initial_url
                            }
                            
                            # Push to Redis queue for parsing
                            queue_size = await self.redis_client_binary.rpush('fetch:queue', pickle.dumps(queue_item, protocol=pickle.HIGHEST_PROTOCOL))
                            logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")
                            
                            # Backpressure
                            if queue_size > self.fetch_queue_hard_limit:
                                # Hard limit - wait until queue shrinks
                                logger.warning(f"Worker-{worker_id}: Fetch queue at hard limit ({queue_size}/{self.fetch_queue_hard_limit}), waiting...")
                                backpressure_events_counter.labels(backpressure_type='hard_limit').inc()
                                while queue_size > self.fetch_queue_soft_limit:
                                    await asyncio.sleep(5.0)
                                    queue_size = await self.redis_client_binary.llen('fetch:queue')
                                logger.info(f"Worker-{worker_id}: Fetch queue below soft limit ({queue_size}), resuming")
                                
                            elif queue_size > self.fetch_queue_soft_limit:
                                # Soft limit - progressive backoff based on queue size
                                # Sleep between 0 and 2 seconds based on how far above soft limit we are
                                overflow_ratio = (queue_size - self.fetch_queue_soft_limit) / (self.fetch_queue_hard_limit - self.fetch_queue_soft_limit)
                                overflow_ratio = min(overflow_ratio, 1.0)  # Cap at 1.0
                                
                                # Add randomization to prevent thundering herd
                                base_sleep = overflow_ratio * 2.0
                                jitter = random.random() * 0.5  # 0-0.5 seconds of jitter
                                sleep_time = base_sleep + jitter
                                
                                if worker_id % 10 == 1:  # Log from only 10% of workers to reduce spam
                                    logger.debug(f"Worker-{worker_id}: Fetch queue at {queue_size}, applying backpressure (sleep {sleep_time:.2f}s)")
                                backpressure_events_counter.labels(backpressure_type='soft_limit').inc()
                                await asyncio.sleep(sleep_time)
                        else:
                            logger.debug(f"Worker-{worker_id}: Not HTML or no text content for {fetch_result.final_url} (Type: {fetch_result.content_type})")
                            
                            # For non-HTML content, still record in visited pages
                            await self.storage.add_visited_page(
                                url=fetch_result.final_url,
                                domain=extract_domain(fetch_result.final_url) or domain,
                                status_code=fetch_result.status_code,
                                crawled_timestamp=crawled_timestamp,
                                content_type=fetch_result.content_type,
                                content_text=None,
                                content_storage_path_str=None,
                                redirected_to_url=fetch_result.final_url if fetch_result.is_redirect and fetch_result.initial_url != fetch_result.final_url else None
                            )
                    
                    # Delete the fetched results here because otherwise they cannot be garbage 
                    # collected until the worker issues a new fetch, increasing peak memory usage
                    del fetch_result
                    
                    # Check stopping conditions after processing a page
                    if self._check_stopping_conditions():
                        self._shutdown_event.set()
                        logger.info(f"Worker-{worker_id}: Stopping condition met, signaling shutdown.")
                        break 
                    
                    await asyncio.sleep(0) # Yield control to event loop

                    self.pages_crawled_in_interval += 1

                except asyncio.CancelledError:
                    # Re-raise to allow proper shutdown
                    raise
                except Exception as e:
                    # Log the error but continue processing
                    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")
                    errors_counter.labels(error_type='worker_error').inc()
                    # Small delay before continuing to avoid tight error loops
                    await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info(f"Worker-{worker_id}: Cancelled.")
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
            # Initialize components that need async setup
            await self._initialize_components()
            
            # Start Prometheus metrics server
            # In multiprocess mode, this aggregates metrics from all processes
            if start_metrics_server(port=8001):
                logger.info("Prometheus metrics server started on port 8001")
            
            # Start parser process if using Redis backend
            if self.config.db_type == 'redis':
                self._start_parser_processes()
                # Give parser a moment to initialize
                await asyncio.sleep(2)

            # Start worker tasks
            logger.info(f"Starting {self.config.max_workers} workers with staggered startup...")
            
            # Stagger worker startup to avoid thundering herd on DB pool
            workers_per_batch = 100  # Start 100 workers at a time
            startup_delay = 5  # Delay between batches in seconds
            
            for i in range(self.config.max_workers):
                task = asyncio.create_task(self._worker(i + 1))
                self.worker_tasks.add(task)
                
                # Add delay between batches
                if (i + 1) % workers_per_batch == 0:
                    logger.info(f"Started {i + 1}/{self.config.max_workers} workers...")
                    await asyncio.sleep(startup_delay)
            
            logger.info(f"Started all {len(self.worker_tasks)} worker tasks.")

            if LOG_MEM_DIAGNOSTICS:
                with open(f'mem_diagnostics.txt', 'w+') as f:
                    logger.info(f"Logging memory diagnostics to mem_diagnostics.txt")
                    all_objects = muppy.get_objects()
                    sum1 = summary.summarize(all_objects)
                    f.write("\n".join(summary.format_(sum1)))
                    logger.info(f"Logged memory diagnostics to mem_diagnostics.txt")

            # Main monitoring loop
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All worker tasks have completed.")
                    self._shutdown_event.set()
                    break
                
                # Check parser process health
                # Handle single parser process (backward compatibility)
                if self.parser_process and not self.parser_processes and not self.parser_process.is_alive():
                    logger.error("Parser process died unexpectedly!")
                    if self.parser_process.exitcode != 0:
                        logger.error(f"Parser process exit code: {self.parser_process.exitcode}")
                    # Restart parser processes
                    self._start_parser_processes(1)  # Start single parser
                
                # Check multiple parser processes health
                if self.parser_processes:
                    dead_processes = []
                    for i, process in enumerate(self.parser_processes):
                        if not process.is_alive():
                            logger.error(f"Parser process {process.name} died unexpectedly!")
                            if process.exitcode != 0:
                                logger.error(f"Parser process {process.name} exit code: {process.exitcode}")
                            dead_processes.append(i)
                    
                    # Remove dead processes
                    for i in reversed(dead_processes):  # Reverse to avoid index issues
                        self.parser_processes.pop(i)
                    
                    # Restart if any died
                    if dead_processes:
                        missing_count = self.num_parser_processes - len(self.parser_processes)
                        logger.info(f"Restarting {missing_count} parser processes...")
                        
                        def run_parser(parser_id: int):
                            """Function to run in the parser process."""
                            import asyncio
                            import logging
                            from .parser_consumer import ParserConsumer
                            
                            logging.basicConfig(
                                level=logging.INFO,
                                format=f'%(asctime)s - %(name)s - %(levelname)s - [Parser-{parser_id}] %(message)s'
                            )
                            
                            consumer = ParserConsumer(self.config)
                            asyncio.run(consumer.run())
                        
                        for i in range(missing_count):
                            process_id = max([int(p.name.split('-')[-1]) for p in self.parser_processes] + [0]) + 1
                            process = Process(
                                target=run_parser,
                                args=(process_id,),
                                name=f"ParserConsumer-{process_id}"
                            )
                            process.daemon = False
                            process.start()
                            self.parser_processes.append(process)
                            logger.info(f"Restarted parser process {process_id} with PID: {process.pid}")
                
                if time.time() - self.last_metrics_log_time >= METRICS_LOG_INTERVAL_SECONDS:
                    await self._log_metrics()

                if LOG_MEM_DIAGNOSTICS and time.time() - self.last_mem_diagnostics_time >= MEM_DIAGNOSTICS_INTERVAL_SECONDS:
                    logger.info(f"Logging memory diagnostics to mem_diagnostics_{time.time()}.txt")
                    with open(f'mem_diagnostics_{time.time()}.txt', 'w+') as f:
                        import gc
                        gc.collect()
                        t = time.time()
                        all_objects = muppy.get_objects()
                        sum1 = summary.summarize(all_objects)
                        f.write("\n".join(summary.format_(sum1)))
                        strings = [obj for obj in all_objects if isinstance(obj, str)]
                        bytes_objects = [obj for obj in all_objects if isinstance(obj, bytes)]
                        # Sample the largest strings
                        strings_by_size = sorted(strings, key=len, reverse=True)
                        f.write("\nTop 5 largest strings:")
                        for i, s in enumerate(strings_by_size[:5]):
                            preview = repr(s[:100]) if len(s) > 100 else repr(s)
                            f.write(f"\n{i+1:2d}. Size: {len(s):,} chars - {preview}")
                            objgraph.show_backrefs(s, max_depth=4, filename=f'mem_diagnostics_{t}_str_{i+1:2d}.png')
                        bytes_by_size = sorted(bytes_objects, key=len, reverse=True)
                        f.write("\nTop 5 largest bytes objects:")
                        for i, s in enumerate(bytes_by_size[:5]):
                            preview = repr(s[:100]) if len(s) > 100 else repr(s)
                            f.write(f"\n{i+1:2d}. Size: {len(s):,} bytes - {preview}")
                    self.last_mem_diagnostics_time = t
                    logger.info(f"Logged memory diagnostics to mem_diagnostics_{t}.txt")

                # Use the fast, estimated count for logging
                if self.config.db_type == 'redis':
                    # TODO: frontier count is very slow with redis right now
                    estimated_frontier_size = -1
                else:
                    estimated_frontier_size = await self.frontier.count_frontier()
                active_workers = sum(1 for task in self.worker_tasks if not task.done())
                
                # Update Prometheus gauges
                frontier_size_gauge.set(estimated_frontier_size)
                active_workers_gauge.set(active_workers)
                
                # Resource monitoring
                rss_mem_mb = "N/A"
                rss_mem_bytes = 0
                fds_count = "N/A"
                fds_count_int = 0
                if current_process:
                    try:
                        rss_mem_bytes = current_process.memory_info().rss
                        rss_mem_mb = f"{rss_mem_bytes / (1024 * 1024):.2f} MB"
                        fds_count_int = current_process.num_fds()
                        fds_count = fds_count_int
                        
                        # Update Prometheus gauges
                        memory_usage_gauge.set(rss_mem_bytes)
                        open_fds_gauge.set(fds_count_int)
                    except psutil.Error as e:
                        logger.warning(f"psutil error during resource monitoring: {e}")
                        # Fallback or disable further psutil calls for this iteration if needed
                
                # Connection pool info - different for each backend
                pool_info = "N/A"
                if self.config.db_type == 'redis':
                    # Redis connection info
                    pool_info = "Redis"
                    # Could add Redis connection pool stats if needed
                elif self.config.db_type == 'sqlite' and hasattr(self.db_backend, '_pool'):
                    pool_info = f"available={self.db_backend._pool.qsize()}"
                    db_pool_available_gauge.set(self.db_backend._pool.qsize())
                elif self.config.db_type == 'postgresql':
                    pool_info = f"min={self.db_backend.min_size},max={self.db_backend.max_size}"
                    # For PostgreSQL, we'd need to expose pool stats from the backend
                    # This is a simplified version - actual implementation would query the pool

                status_parts = [
                    f"Crawled={self.pages_crawled_count}",
                    f"Frontier={estimated_frontier_size}",
                    f"AddedToFrontier={self.total_urls_added_to_frontier}",
                    f"ActiveWorkers={active_workers}/{len(self.worker_tasks)}",
                    f"DBPool({self.config.db_type})={pool_info}",
                    f"MemRSS={rss_mem_mb}",
                    f"OpenFDs={fds_count}",
                    f"Runtime={(time.time() - self.start_time):.0f}s"
                ]
                logger.info(f"Status: {', '.join(status_parts)}")
                
                # If frontier is empty and workers might be idle, it could be a natural end
                # Use the accurate is_empty() check for shutdown logic
                if await self.frontier.is_empty() and self.pages_crawled_count > 0: # Check if any crawling happened
                    # Wait a bit to see if workers add more URLs or finish
                    logger.info(f"Frontier is empty. Pages crawled: {self.pages_crawled_count}. Monitoring workers...")
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2) # Wait longer than worker sleep
                    
                    # Final, accurate check before shutdown
                    if await self.frontier.is_empty():
                        logger.info("Frontier confirmed empty after wait. Signaling shutdown.")
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
            
            # Stop parser process first to prevent it from trying to process more items
            self._stop_parser_processes()
            
            await self.fetcher.close_session()
            
            # Close storage and database connections
            await self.storage.close()
            
            # Close Redis connection if using Redis backend
            if self.config.db_type == 'redis' and self.redis_client:
                await self.redis_client.aclose()
                await self.redis_client_binary.aclose()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
            logger.info(f"Total runtime: {(time.time() - self.start_time):.2f} seconds.") 