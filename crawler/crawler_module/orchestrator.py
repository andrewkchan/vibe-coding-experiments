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
from redis.asyncio import BlockingConnectionPool
import random
import gc

from .config import CrawlerConfig
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .redis_shield import ShieldedRedis

from .utils import extract_domain # For getting domain for storage
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
    content_size_histogram,
    # System metrics
    cpu_usage_gauge,
    memory_free_gauge,
    memory_available_gauge,
    disk_free_gauge,
    disk_usage_percent_gauge,
    io_read_count_gauge,
    io_write_count_gauge,
    io_read_bytes_gauge,
    io_write_bytes_gauge,
    network_bytes_sent_gauge,
    network_bytes_recv_gauge,
    network_packets_sent_gauge,
    network_packets_recv_gauge,
    # Redis metrics
    redis_ops_per_sec_gauge,
    redis_memory_usage_gauge,
    redis_connected_clients_gauge,
    redis_hit_rate_gauge,
    redis_latency_histogram,
    # FD type metrics
    fd_redis_gauge,
    fd_http_gauge,
    fd_https_gauge,
    fd_frontier_files_gauge,
    fd_prometheus_gauge,
    fd_other_sockets_gauge,
    fd_pipes_gauge,
    fd_other_gauge
)

logger = logging.getLogger(__name__)

# How long a worker should sleep if the frontier is temporarily empty 
# or all domains are on cooldown, before trying again.
EMPTY_FRONTIER_SLEEP_SECONDS = 10
# How often the main orchestrator loop checks status and stopping conditions
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

METRICS_LOG_INTERVAL_SECONDS = 60
LOG_MEM_DIAGNOSTICS = False
MEM_DIAGNOSTICS_INTERVAL_SECONDS = 60

GC_INTERVAL_SECONDS = 120
GC_RSS_THRESHOLD_MB = 4096

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
        
        # Set parent process ID for multiprocess metrics
        # The multiprocess directory should already be set up by main.py
        # We just need to mark this as the parent process
        os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
        logger.info(f"Set PROMETHEUS_PARENT_PROCESS to {os.getpid()}")

        # Initialize fetcher (common to all backends)
        self.fetcher: Fetcher = Fetcher(config)
        
        # Initialize Redis client with BlockingConnectionPool to prevent "Too many connections" errors
        redis_kwargs = config.get_redis_connection_kwargs()
        # Create a blocking connection pool for text data
        text_pool = BlockingConnectionPool(**redis_kwargs)
        base_redis_client = redis.Redis(connection_pool=text_pool)
        # Monkey-patch redis client method to remove a useless decorator which incurs extra overhead
        base_redis_client.connection_pool.get_connection = functools.partial(
            base_redis_client.connection_pool.get_connection.__wrapped__, 
            base_redis_client.connection_pool
        )
        # Wrap with ShieldedRedis to prevent connection leaks from cancelled operations
        self.redis_client = ShieldedRedis(base_redis_client)
        
        # Create a separate Redis client for binary data (pickle)
        binary_redis_kwargs = config.get_redis_connection_kwargs()
        binary_redis_kwargs['decode_responses'] = False
        # Create a blocking connection pool for binary data
        binary_pool = BlockingConnectionPool(**binary_redis_kwargs)
        base_redis_client_binary = redis.Redis(connection_pool=binary_pool)
        # Same monkey patching as above
        base_redis_client_binary.connection_pool.get_connection = functools.partial(
            base_redis_client_binary.connection_pool.get_connection.__wrapped__, 
            base_redis_client_binary.connection_pool
        )
        # Wrap with ShieldedRedis to prevent connection leaks
        self.redis_client_binary = ShieldedRedis(base_redis_client_binary)
        
        # Use Redis-based components
        self.storage: StorageManager = StorageManager(config, self.redis_client) # type: ignore
        self.politeness: PolitenessEnforcer = PolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier: FrontierManager = FrontierManager(
            config, 
            self.politeness,
            self.redis_client # type: ignore
        )

        self.pages_crawled_count: int = 0
        self.start_time: float = 0.0
        self.worker_tasks: Set[asyncio.Task] = set()
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self.total_urls_added_to_frontier: int = 0

        self.pages_crawled_in_interval: int = 0
        self.last_metrics_log_time: float = time.time()
        self.mem_tracker = tracker.SummaryTracker()
        self.last_mem_diagnostics_time: float = time.time()
        self.last_mem_diagnostics_rss: int = 0

        self.last_gc_time: float = time.time()
        
        # System metrics tracking for rate calculations
        self.last_io_stats = None
        self.last_network_stats = None
        self.last_redis_stats = None
        self.last_redis_stats_time = None
        
        # Parser process management
        self.parser_process: Optional[Process] = None
        self.parser_processes: List[Process] = []  # Support multiple parsers
        self.num_parser_processes = 2
        
        # Backpressure configuration
        self.fetch_queue_soft_limit = 20000  # Start slowing down at this size
        self.fetch_queue_hard_limit = 80000  # Maximum queue size

    def _start_parser_process(self):
        """Start the parser consumer as a child process."""
            
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
        
        # Clear any zombie locks from previous runs
        from .redis_lock import LockManager
        lock_manager = LockManager(self.redis_client)
        cleared_count = await lock_manager.clear_all_locks()
        if cleared_count > 0:
            logger.warning(f"Cleared {cleared_count} zombie locks from previous run")
        
        # Redis-based initialization
        await self.storage.init_db_schema()  # Sets schema version
        # PolitenessEnforcer loads exclusions differently
        if hasattr(self.politeness, '_load_manual_exclusions'):
            await self.politeness._load_manual_exclusions()
        await self.frontier.initialize_frontier()
            
        logger.info("Crawler components initialized.")

    async def _analyze_fd_types(self):
        """Analyze FD types and update Prometheus metrics."""
        try:
            pid = os.getpid()
            
            # First, read socket inode -> connection mapping
            socket_map = {}
            
            # Parse /proc/net/tcp and /proc/net/tcp6
            for proto_file in ['/proc/net/tcp', '/proc/net/tcp6']:
                try:
                    with open(proto_file, 'r') as f:
                        lines = f.readlines()[1:]  # Skip header
                        for line in lines:
                            parts = line.strip().split()
                            if len(parts) >= 10:
                                local_addr = parts[1]  # hex IP:port
                                remote_addr = parts[2]  # hex IP:port
                                inode = parts[9]
                                
                                # Parse hex addresses
                                local_ip, local_port = local_addr.split(':')
                                remote_ip, remote_port = remote_addr.split(':')
                                local_port = int(local_port, 16)
                                remote_port = int(remote_port, 16)
                                
                                socket_map[inode] = {
                                    'local_port': local_port,
                                    'remote_port': remote_port,
                                    'proto': proto_file.split('/')[-1]
                                }
                except:
                    pass
            
            # Now check FDs
            fd_stats = {
                'redis': 0,
                'http': 0,
                'https': 0,
                'frontier_files': 0,
                'prometheus': 0,
                'other_sockets': 0,
                'pipes': 0,
                'other': 0
            }
            
            fd_dir = f'/proc/{pid}/fd'
            for fd in os.listdir(fd_dir):
                try:
                    link = os.readlink(f'{fd_dir}/{fd}')
                    
                    if 'socket:' in link:
                        # Extract inode from socket:[12345]
                        inode = link.split('[')[1].split(']')[0]
                        
                        if inode in socket_map:
                            info = socket_map[inode]
                            # Redis typically on port 6379
                            if info['remote_port'] == 6379 or info['local_port'] == 6379:
                                fd_stats['redis'] += 1
                            # HTTP/HTTPS
                            elif info['remote_port'] == 443:
                                fd_stats['https'] += 1
                            elif info['remote_port'] == 80:
                                fd_stats['http'] += 1
                            else:
                                fd_stats['other_sockets'] += 1
                        else:
                            fd_stats['other_sockets'] += 1
                            
                    elif 'pipe:' in link:
                        fd_stats['pipes'] += 1
                    elif '/frontier' in link:
                        fd_stats['frontier_files'] += 1
                    elif '/prometheus' in link:
                        fd_stats['prometheus'] += 1
                    else:
                        fd_stats['other'] += 1
                        
                except:
                    pass  # FD might have closed
            
            # Update Prometheus metrics
            fd_redis_gauge.set(fd_stats['redis'])
            fd_http_gauge.set(fd_stats['http'])
            fd_https_gauge.set(fd_stats['https'])
            fd_frontier_files_gauge.set(fd_stats['frontier_files'])
            fd_prometheus_gauge.set(fd_stats['prometheus'])
            fd_other_sockets_gauge.set(fd_stats['other_sockets'])
            fd_pipes_gauge.set(fd_stats['pipes'])
            fd_other_gauge.set(fd_stats['other'])
            
            logger.info(f"[Metrics] FD breakdown: {fd_stats}")
            
            if hasattr(self, 'fetcher') and hasattr(self.fetcher, 'session') and self.fetcher.session:
                connector = self.fetcher.session.connector
                if hasattr(connector, '_acquired'):
                    logger.info(f"[Metrics] aiohttp acquired connections: {len(connector._acquired)}")
                if hasattr(connector, '_conns'):
                    total_conns = sum(len(conns) for conns in connector._conns.values())
                    logger.info(f"[Metrics] aiohttp total cached connections: {total_conns}")
                    
        except Exception as e:
            logger.error(f"Error in FD analysis: {e}")

    async def _log_metrics(self):
        current_time = time.time()
        time_elapsed_seconds = current_time - self.last_metrics_log_time
        if time_elapsed_seconds == 0: time_elapsed_seconds = 1 # Avoid division by zero

        pages_per_second = self.pages_crawled_in_interval / time_elapsed_seconds
        logger.info(f"[Metrics] Pages Crawled/sec: {pages_per_second:.2f}")
        
        # Update Prometheus gauge
        pages_per_second_gauge.set(pages_per_second)
        
        # Collect system metrics
        try:
            # CPU usage (non-blocking - returns usage since last call)
            cpu_percent = psutil.cpu_percent(interval=None)
            if cpu_percent is not None:  # First call returns None
                cpu_usage_gauge.set(cpu_percent)
                logger.info(f"[Metrics] CPU Usage: {cpu_percent:.1f}%")
            
            # Memory metrics
            mem = psutil.virtual_memory()
            memory_free_gauge.set(mem.free)
            memory_available_gauge.set(mem.available)
            logger.info(f"[Metrics] Memory: Free={mem.free/1024/1024/1024:.1f}GB, Available={mem.available/1024/1024/1024:.1f}GB, Used={mem.percent:.1f}%")
            
            # Warn if system memory is low (important for Redis fork operations)
            available_gb = mem.available / 1024 / 1024 / 1024
            if available_gb < 2.0:
                logger.warning(f"[Metrics] SYSTEM MEMORY WARNING: Only {available_gb:.1f}GB available - Redis background operations may fail!")
            elif available_gb < 4.0:
                logger.info(f"[Metrics] System memory getting low: {available_gb:.1f}GB available")
            
            # Disk metrics (for the crawler's data directory)
            disk = psutil.disk_usage(self.config.data_dir)
            disk_free_gauge.set(disk.free)
            disk_usage_percent_gauge.set(disk.percent)
            logger.info(f"[Metrics] Disk ({self.config.data_dir}): Free={disk.free/1024/1024/1024:.1f}GB, Used={disk.percent:.1f}%")
            
            # IO metrics
            io_counters = psutil.disk_io_counters()
            if io_counters:
                io_read_count_gauge.set(io_counters.read_count)
                io_write_count_gauge.set(io_counters.write_count)
                io_read_bytes_gauge.set(io_counters.read_bytes)
                io_write_bytes_gauge.set(io_counters.write_bytes)
                
                # Calculate IOPS and throughput rates
                if self.last_io_stats:
                    read_iops = (io_counters.read_count - self.last_io_stats.read_count) / time_elapsed_seconds
                    write_iops = (io_counters.write_count - self.last_io_stats.write_count) / time_elapsed_seconds
                    read_throughput_mb = (io_counters.read_bytes - self.last_io_stats.read_bytes) / time_elapsed_seconds / 1024 / 1024
                    write_throughput_mb = (io_counters.write_bytes - self.last_io_stats.write_bytes) / time_elapsed_seconds / 1024 / 1024
                    logger.info(f"[Metrics] IO: Read={read_iops:.0f} IOPS/{read_throughput_mb:.1f} MB/s, Write={write_iops:.0f} IOPS/{write_throughput_mb:.1f} MB/s")
                
                self.last_io_stats = io_counters
            
            # Network metrics
            net_counters = psutil.net_io_counters()
            if net_counters:
                network_bytes_sent_gauge.set(net_counters.bytes_sent)
                network_bytes_recv_gauge.set(net_counters.bytes_recv)
                network_packets_sent_gauge.set(net_counters.packets_sent)
                network_packets_recv_gauge.set(net_counters.packets_recv)
                
                # Calculate network rates
                if self.last_network_stats:
                    sent_mb_per_sec = (net_counters.bytes_sent - self.last_network_stats.bytes_sent) / time_elapsed_seconds / 1024 / 1024
                    recv_mb_per_sec = (net_counters.bytes_recv - self.last_network_stats.bytes_recv) / time_elapsed_seconds / 1024 / 1024
                    logger.info(f"[Metrics] Network: Sent={sent_mb_per_sec:.1f} MB/s, Recv={recv_mb_per_sec:.1f} MB/s")
                
                self.last_network_stats = net_counters
            
            # Redis metrics
            redis_info = await self.redis_client.info()
            redis_memory = await self.redis_client.info('memory')
            redis_stats = await self.redis_client.info('stats')
            
            # Basic Redis metrics
            redis_memory_usage_gauge.set(redis_memory.get('used_memory', 0))
            redis_connected_clients_gauge.set(redis_info.get('connected_clients', 0))
            
            # Redis ops/sec
            instantaneous_ops = redis_stats.get('instantaneous_ops_per_sec', 0)
            redis_ops_per_sec_gauge.set(instantaneous_ops)
            
            # Additional Redis memory metrics
            redis_rss = redis_memory.get('used_memory_rss_human', 'N/A')
            redis_dataset_perc = redis_memory.get('used_memory_dataset_perc', 'N/A')
            redis_used = redis_memory.get('used_memory_human', 'N/A')
            redis_max = redis_memory.get('maxmemory_human', 'N/A')
            redis_evicted_keys = redis_stats.get('evicted_keys', 0)
            
            # Check Redis memory usage percentage and warn if high
            used_memory_bytes = redis_memory.get('used_memory', 0)
            max_memory_bytes = redis_memory.get('maxmemory', 0)
            if max_memory_bytes > 0:
                memory_usage_percent = (used_memory_bytes / max_memory_bytes) * 100
                if memory_usage_percent > 90:
                    logger.warning(f"[Metrics] REDIS MEMORY WARNING: Using {memory_usage_percent:.1f}% of maxmemory ({redis_used}/{redis_max})")
                elif memory_usage_percent > 80:
                    logger.info(f"[Metrics] Redis memory usage at {memory_usage_percent:.1f}% of maxmemory")
            
            # Calculate hit rate
            if self.last_redis_stats:
                time_diff = current_time - self.last_redis_stats_time
                if time_diff > 0:
                    hits_diff = redis_stats.get('keyspace_hits', 0) - self.last_redis_stats.get('keyspace_hits', 0)
                    misses_diff = redis_stats.get('keyspace_misses', 0) - self.last_redis_stats.get('keyspace_misses', 0)
                    total_diff = hits_diff + misses_diff
                    if total_diff > 0:
                        hit_rate = (hits_diff / total_diff) * 100
                        redis_hit_rate_gauge.set(hit_rate)
                        logger.info(f"[Metrics] Redis: Ops/sec={instantaneous_ops}, Memory={redis_used}/{redis_max} (RSS: {redis_rss}, Dataset: {redis_dataset_perc}), Hit Rate={hit_rate:.1f}%, Evicted={redis_evicted_keys}")
                    else:
                        logger.info(f"[Metrics] Redis: Ops/sec={instantaneous_ops}, Memory={redis_used}/{redis_max} (RSS: {redis_rss}, Dataset: {redis_dataset_perc}), Evicted={redis_evicted_keys}")
            else:
                logger.info(f"[Metrics] Redis: Ops/sec={instantaneous_ops}, Memory={redis_used}/{redis_max} (RSS: {redis_rss}, Dataset: {redis_dataset_perc}), Evicted={redis_evicted_keys}")
            
            self.last_redis_stats = redis_stats
            self.last_redis_stats_time = current_time
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")

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
                            
                            # Explicitly delete to keep peak memory down
                            del queue_item
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

    def log_mem_diagnostics(self):
        ENABLE_TRACEMALLOC = os.environ.get('ENABLE_TRACEMALLOC', '').lower() == 'true'
        current_process = psutil.Process(os.getpid())
        t = time.time()
        logger.info(f"Logging memory diagnostics to mem_diagnostics/mem_diagnostics_{t}.txt")
        with open(f'mem_diagnostics/mem_diagnostics_{t}.txt', 'w+') as f:
            gc.collect()
            # Tracemalloc analysis if enabled
            if ENABLE_TRACEMALLOC:
                import tracemalloc
                snapshot = tracemalloc.take_snapshot()
                
                # Compare with previous snapshot if available
                if self.tracemalloc_snapshots:
                    previous = self.tracemalloc_snapshots[-1]
                    top_stats = snapshot.compare_to(previous, 'lineno')
                    
                    f.write("\n=== TRACEMALLOC: Top 10 memory allocation increases ===")
                    for stat in top_stats[:10]:
                        if stat.size_diff > 0:
                            logger.info(f"{stat}")
                    
                    # Find allocations of large byte objects
                    f.write("\n=== TRACEMALLOC: Checking for large allocations ===")
                    for stat in snapshot.statistics('lineno')[:20]:
                        # Look for allocations over 10MB
                        if stat.size > 10 * 1024 * 1024:
                            f.write(f"\nLARGE ALLOCATION: {stat.size / 1024 / 1024:.1f} MB at {stat}")
                            # Get traceback for this allocation
                            for line in stat.traceback.format():
                                f.write(f"\n  {line}")
                
                # Keep only last 5 snapshots to avoid memory growth
                self.tracemalloc_snapshots.append(snapshot)
                if len(self.tracemalloc_snapshots) > 5:
                    self.tracemalloc_snapshots.pop(0)
                
                self.last_tracemalloc_time = time.time()
                
                # Also get current memory usage
                current, peak = tracemalloc.get_traced_memory()
                logger.info(f"TRACEMALLOC: Current traced: {current / 1024 / 1024:.1f} MB, Peak: {peak / 1024 / 1024:.1f} MB")
            all_objects = muppy.get_objects()
            sum1 = summary.summarize(all_objects)
            f.write("\n".join(summary.format_(sum1)))
            rss_mem_bytes = current_process.memory_info().rss
            rss_mem_mb = f"{rss_mem_bytes / (1024 * 1024):.2f} MB"
            f.write(f"\nRSS Memory: {rss_mem_mb}")
            delta_bytes = rss_mem_bytes - self.last_mem_diagnostics_rss
            self.last_mem_diagnostics_rss = rss_mem_bytes
            strings = [obj for obj in all_objects if isinstance(obj, str)]
            bytes_objects = [obj for obj in all_objects if isinstance(obj, bytes)]
            # Sample the largest strings
            strings_by_size = sorted(strings, key=len, reverse=True)
            f.write("\nTop 5 largest strings:")
            for i, s in enumerate(strings_by_size[:5]):
                preview = repr(s[:100]) if len(s) > 100 else repr(s)
                f.write(f"\n{i+1:2d}. Size: {len(s):,} chars - {preview}")
                if delta_bytes > 500_000_000:
                    objgraph.show_backrefs(s, max_depth=4, filename=f'mem_diagnostics/mem_diagnostics_{t}_str_{i+1:2d}.png')
            bytes_by_size = sorted(bytes_objects, key=len, reverse=True)
            f.write("\nTop 5 largest bytes objects:")
            for i, s in enumerate(bytes_by_size[:5]):
                preview = repr(s[:100]) if len(s) > 100 else repr(s)
                f.write(f"\n{i+1:2d}. Size: {len(s):,} bytes - {preview}")
                if delta_bytes > 500_000_000:
                    objgraph.show_backrefs(s, max_depth=4, filename=f'mem_diagnostics/mem_diagnostics_{t}_bytes_{i+1:2d}.png')
            if delta_bytes > 100_000_000 and rss_mem_bytes > 1_500_000_000:
                # Enhanced debugging for memory leak investigation
                logger.warning(f"MEMORY SPIKE DETECTED! Delta: {delta_bytes/1024/1024:.1f} MB, Total RSS: {rss_mem_bytes/1024/1024:.1f} MB")
                def trace_refs(obj, depth=3, seen=None):
                    """Helper to trace reference chain from an object"""
                    if seen is None:
                        seen = set()
                    if id(obj) in seen or depth == 0:
                        return
                    seen.add(id(obj))
                    
                    refs = gc.get_referrers(obj)
                    print(f"\n{'  ' * (3-depth)}Object: {type(obj).__name__} at {hex(id(obj))}")
                    print(f"{'  ' * (3-depth)}Referrers: {len(refs)}")
                    
                    for i, ref in enumerate(refs[:3]):  # First 3 refs at each level
                        ref_type = type(ref).__name__
                        ref_module = getattr(type(ref), '__module__', 'unknown')
                        print(f"{'  ' * (3-depth)}  [{i}] {ref_module}.{ref_type}")
                        
                        if isinstance(ref, dict) and 'url' in ref:
                            print(f"{'  ' * (3-depth)}      URL: {ref.get('url')}")
                        elif hasattr(ref, 'url'):
                            print(f"{'  ' * (3-depth)}      URL attr: {ref.url}")
                        
                        if depth > 1:
                            trace_refs(ref, depth-1, seen)
                
                logger.warning("Entering debugger. Useful variables: largest_bytes, referrers, bytes_by_size, all_objects")
                logger.warning("Helper function available: trace_refs(obj, depth=3) to trace reference chains")
                f.close()
                import pdb; pdb.set_trace()
        self.last_mem_diagnostics_time = t
        logger.info(f"Logged memory diagnostics to mem_diagnostics/mem_diagnostics_{t}.txt")

    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")
        current_process = psutil.Process(os.getpid())
        
        # Initialize CPU monitoring for non-blocking measurements
        psutil.cpu_percent(interval=None)  # First call to establish baseline
        
        # Optional: Enable tracemalloc for memory tracking
        ENABLE_TRACEMALLOC = os.environ.get('ENABLE_TRACEMALLOC', '').lower() == 'true'
        if ENABLE_TRACEMALLOC:
            import tracemalloc
            # Start with 10 frames - higher values give more context but use more memory
            # Each frame adds ~40 bytes per memory block tracked
            tracemalloc.start(5)
            logger.warning("TRACEMALLOC ENABLED - This will increase memory usage and slow down execution!")
            self.tracemalloc_snapshots = []  # Store snapshots for comparison
            self.last_tracemalloc_time = time.time()

        try:
            # Initialize components that need async setup
            await self._initialize_components()
            
            # Start Prometheus metrics server
            # In multiprocess mode, this aggregates metrics from all processes
            if start_metrics_server(port=8001):
                logger.info("Prometheus metrics server started on port 8001")
            
            # Start parser process
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
                with open(f'mem_diagnostics/mem_diagnostics.txt', 'w+') as f:
                    logger.info(f"Logging memory diagnostics to mem_diagnostics/mem_diagnostics.txt")
                    all_objects = muppy.get_objects()
                    sum1 = summary.summarize(all_objects)
                    f.write("\n".join(summary.format_(sum1)))
                    logger.info(f"Logged memory diagnostics to mem_diagnostics/mem_diagnostics.txt")

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
                    
                    # Analyze FD types
                    await self._analyze_fd_types()
                

                if LOG_MEM_DIAGNOSTICS and time.time() - self.last_mem_diagnostics_time >= MEM_DIAGNOSTICS_INTERVAL_SECONDS:
                    self.log_mem_diagnostics()
                
                if time.time() - self.last_gc_time >= GC_INTERVAL_SECONDS:
                    import pympler
                    rss_mem_bytes = current_process.memory_info().rss
                    rss_mem_mb = rss_mem_bytes / (1024 * 1024)
                    if rss_mem_mb > GC_RSS_THRESHOLD_MB:
                        logger.info(f"[GC] Running garbage collection due to memory usage: {rss_mem_mb:.2f} MB")
                        gc.collect()
                    robots_cache_size_bytes = pympler.asizeof.asizeof(self.politeness.robots_parsers)
                    robots_cache_size_mb = robots_cache_size_bytes / (1024 * 1024)
                    logger.info(f"[GC] Approx size of robots in-mem cache: {robots_cache_size_mb:.2f} MB")
                    logger.info(f"[GC] Items in robots in-mem cache: {len(self.politeness.robots_parsers)}")
                    self.last_gc_time = time.time()

                # TODO: frontier count is very slow with redis right now
                estimated_frontier_size = -1
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
                
                status_parts = [
                    f"Crawled={self.pages_crawled_count}",
                    f"Frontier={estimated_frontier_size}",
                    f"AddedToFrontier={self.total_urls_added_to_frontier}",
                    f"ActiveWorkers={active_workers}/{len(self.worker_tasks)}",
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
            
            # Close Redis connection
            await self.redis_client.aclose()
            await self.redis_client_binary.aclose()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
            logger.info(f"Total runtime: {(time.time() - self.start_time):.2f} seconds.") 