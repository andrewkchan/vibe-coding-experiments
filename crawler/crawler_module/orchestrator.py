import asyncio
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
import os # For process ID
import psutil
import functools
from pympler import muppy, summary
from multiprocessing import Process
import redis.asyncio as redis
from redis.asyncio import BlockingConnectionPool
import gc

from .config import CrawlerConfig
from .pod_manager import PodManager
from .redis_shield import ShieldedRedis
from .fetcher_process import FetcherProcess, run_fetcher_process
from .parser_consumer import ParserConsumer
from .storage import StorageManager
from .fetcher import Fetcher
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .memory_diagnostics import memory_diagnostics
from .metrics_utils import get_pages_crawled_total
from .metrics import (
    start_metrics_server,
    frontier_size_gauge,
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
    # FD type metrics
    fd_redis_gauge,
    fd_http_gauge,
    fd_https_gauge,
    fd_frontier_files_gauge,
    fd_prometheus_gauge,
    fd_other_sockets_gauge,
    fd_pipes_gauge,
    fd_other_gauge,
    process_memory_usage_gauge,
    process_open_fds_gauge
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

        # Initialize PodManager for pod-based architecture
        self.pod_manager = PodManager(config)
        
        # For backward compatibility, keep redis_client as pod 0's client
        # This is used for global coordination
        self.redis_client: Optional[ShieldedRedis] = None
        self.redis_client_binary: Optional[ShieldedRedis] = None

        self.pages_crawled_count: int = 0
        self.start_time: float = 0.0
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self.total_urls_added_to_frontier: int = 0

        self.last_metrics_log_time: float = time.time()
        self.last_mem_diagnostics_time: float = time.time()
        self.last_mem_diagnostics_rss: int = 0

        self.last_gc_time: float = time.time()
        
        # System metrics tracking for rate calculations
        self.last_io_stats = None
        self.last_network_stats = None
        self.last_redis_stats = None
        self.last_redis_stats_time = None
        
        # Process management - now organized by pods
        self.pod_processes: Dict[int, Dict[str, List[Process]]] = {}
        self.local_pod_fetchers: Dict[int, FetcherProcess] = {}  # Local fetchers by pod
        
        # Backpressure configuration
        self.fetch_queue_soft_limit = config.parse_queue_soft_limit
        self.fetch_queue_hard_limit = config.parse_queue_hard_limit
    
    async def _init_redis_clients(self):
        """Initialize Redis clients for global coordination (pod 0)."""
        self.redis_client = await self.pod_manager.get_redis_client(
            pod_id=self.config.global_coordination_redis_pod, 
            binary=False
        )
        self.redis_client_binary = await self.pod_manager.get_redis_client(
            pod_id=self.config.global_coordination_redis_pod,
            binary=True
        )
        logger.info(f"Initialized global coordination Redis clients (pod {self.config.global_coordination_redis_pod})")

    def _start_pod_processes(self, pod_id: int):
        """Start all processes for a specific pod."""
        self.pod_processes[pod_id] = {
            'fetchers': [],
            'parsers': []
        }
        
        def run_fetcher(pod_id: int, fetcher_id: int):
            """Function to run in the fetcher process."""
            import asyncio
            from .logging_utils import setup_pod_logging
            from .process_utils import set_cpu_affinity
            from .fetcher_process import run_fetcher_process
            
            # Setup per-pod logging
            setup_pod_logging(self.config, pod_id, 'fetcher', fetcher_id)
            
            # Set CPU affinity if enabled
            if self.config.enable_cpu_affinity:
                set_cpu_affinity(
                    pod_id=pod_id,
                    process_type='fetcher',
                    process_id=fetcher_id,
                    fetchers_per_pod=self.config.fetchers_per_pod,
                    parsers_per_pod=self.config.parsers_per_pod,
                    enabled=True
                )
            
            # Run the fetcher with pod ID
            run_fetcher_process(self.config, fetcher_id, pod_id=pod_id)
        
        def run_parser(pod_id: int, parser_id: int):
            """Function to run in the parser process."""
            import asyncio
            from .logging_utils import setup_pod_logging
            from .process_utils import set_cpu_affinity
            from .parser_consumer import ParserConsumer
            
            # Setup per-pod logging
            setup_pod_logging(self.config, pod_id, 'parser', parser_id)
            
            # Set CPU affinity if enabled
            if self.config.enable_cpu_affinity:
                set_cpu_affinity(
                    pod_id=pod_id,
                    process_type='parser',
                    process_id=parser_id,
                    fetchers_per_pod=self.config.fetchers_per_pod,
                    parsers_per_pod=self.config.parsers_per_pod,
                    enabled=True
                )
            
            # Create and run the parser consumer for this pod
            consumer = ParserConsumer(self.config, pod_id=pod_id)
            asyncio.run(consumer.run())
        
        # Start fetcher processes for this pod
        for i in range(self.config.fetchers_per_pod):
            # Skip fetcher 0 of pod 0 (runs in orchestrator)
            if pod_id == 0 and i == 0:
                continue
                
            process = Process(
                target=run_fetcher,
                args=(pod_id, i),
                name=f"Pod-{pod_id}-Fetcher-{i}"
            )
            process.daemon = False
            process.start()
            self.pod_processes[pod_id]['fetchers'].append(process)
            logger.info(f"Started fetcher {i} for pod {pod_id} with PID: {process.pid}")
        
        # Start parser processes for this pod
        for i in range(self.config.parsers_per_pod):
            process = Process(
                target=run_parser,
                args=(pod_id, i),
                name=f"Pod-{pod_id}-Parser-{i}"
            )
            process.daemon = False
            process.start()
            self.pod_processes[pod_id]['parsers'].append(process)
            logger.info(f"Started parser {i} for pod {pod_id} with PID: {process.pid}")
    
    def _stop_pod_processes(self, pod_id: int):
        """Stop all processes for a specific pod."""
        if pod_id not in self.pod_processes:
            return
            
        logger.info(f"Stopping processes for pod {pod_id}...")
        
        # Stop fetchers first
        for process in self.pod_processes[pod_id]['fetchers']:
            if process.is_alive():
                process.terminate()
        
        # Stop parsers
        for process in self.pod_processes[pod_id]['parsers']:
            if process.is_alive():
                process.terminate()
        
        # Wait for all to terminate
        for process in self.pod_processes[pod_id]['fetchers'] + self.pod_processes[pod_id]['parsers']:
            process.join(timeout=10)
            if process.is_alive():
                logger.warning(f"Process {process.name} didn't terminate gracefully, forcing...")
                process.kill()
                process.join()
        
        del self.pod_processes[pod_id]
        logger.info(f"All processes for pod {pod_id} stopped.")
    
    def _stop_all_pod_processes(self):
        """Stop all pod processes."""
        pod_ids = list(self.pod_processes.keys())
        for pod_id in pod_ids:
            self._stop_pod_processes(pod_id)

    async def _initialize_components(self):
        """Initializes orchestrator components that require async setup."""
        logger.info("Initializing orchestrator components...")
        
        # Initialize Redis clients for global coordination
        await self._init_redis_clients()
        
        # One-time initialization shared across all processes
        logger.info("Performing one-time initialization...")
        
        # For each pod, initialize storage schema and frontier
        for pod_id in self.pod_manager.get_all_pod_ids():
            logger.info(f"Initializing pod {pod_id}...")
            
            # Get Redis clients for this pod
            redis_client = await self.pod_manager.get_redis_client(pod_id, binary=False)
            
            # Initialize storage schema
            storage = StorageManager(self.config, redis_client)
            await storage.init_db_schema()
            await storage.close()
            
            # Initialize frontier for this pod
            temp_fetcher = Fetcher(self.config)
            politeness = PolitenessEnforcer(self.config, redis_client, temp_fetcher)
            frontier = FrontierManager(self.config, politeness, redis_client, pod_id=pod_id)
            await frontier.initialize_frontier()
            
            # Clean up temporary instances
            await temp_fetcher.close_session()
            
            # Add debug info for this pod
            pod_config = self.pod_manager.get_pod_config(pod_id)
            await redis_client.hset('pod:info', mapping={
                'pod_id': str(pod_id),
                'redis_url': pod_config.redis_url,
                'initialized_at': str(int(time.time()))
            })
            
            logger.info(f"Pod {pod_id} initialized.")
        
        # Pod distribution will be logged when seeds are loaded
            
        logger.info("Orchestrator components initialized.")

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
                    
        except Exception as e:
            logger.error(f"Error in FD analysis: {e}")

    async def _log_metrics(self):
        current_time = time.time()
        time_elapsed_seconds = current_time - self.last_metrics_log_time
        if time_elapsed_seconds == 0: time_elapsed_seconds = 1 # Avoid division by zero

        # Note: pages_per_second is now calculated by individual fetcher processes
        # and aggregated by Prometheus across all processes
        logger.info(f"[Metrics] Check Prometheus endpoint for aggregated pages/sec across all fetchers")
        
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
            disk = psutil.disk_usage(str(self.config.data_dir))
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
            if self.redis_client:
                redis_info = await self.redis_client.info()
                redis_memory = await self.redis_client.info('memory')
                redis_stats = await self.redis_client.info('stats')
            else:
                redis_info = {}
                redis_memory = {}
                redis_stats = {}
            
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
        self.last_metrics_log_time = current_time

    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")
        current_process = psutil.Process(os.getpid())
        
        # Set CPU affinity for orchestrator (pod 0)
        if self.config.enable_cpu_affinity:
            from .process_utils import set_cpu_affinity, log_cpu_info
            log_cpu_info()
            set_cpu_affinity(
                pod_id=0,
                process_type='orchestrator',
                process_id=0,
                fetchers_per_pod=self.config.fetchers_per_pod,
                parsers_per_pod=self.config.parsers_per_pod,
                enabled=True
            )
        
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
            
            # Start processes for all pods
            logger.info(f"Starting processes for {self.config.num_pods} pods...")
            
            # Start pod 0's local fetcher in this process
            self.local_pod_fetchers[0] = FetcherProcess(self.config, fetcher_id=0, pod_id=0)
            
            # Set CPU affinity for the local fetcher (shares orchestrator's cores)
            # Note: We already set affinity for the orchestrator process above
            
            local_fetcher_task = asyncio.create_task(self.local_pod_fetchers[0].run())
            logger.info("Started local fetcher (Pod 0, Fetcher 0) in orchestrator process")
            
            # Start all pod processes
            for pod_id in range(self.config.num_pods):
                self._start_pod_processes(pod_id)
            
            # Give all processes a moment to initialize
            await asyncio.sleep(2)
            
            total_fetchers = self.config.num_pods * self.config.fetchers_per_pod
            total_parsers = self.config.num_pods * self.config.parsers_per_pod
            logger.info(f"All processes started: {total_fetchers} fetchers, {total_parsers} parsers across {self.config.num_pods} pods")

            if LOG_MEM_DIAGNOSTICS:
                with open(f'mem_diagnostics/mem_diagnostics.txt', 'w+') as f:
                    logger.info(f"Logging memory diagnostics to mem_diagnostics/mem_diagnostics.txt")
                    all_objects = muppy.get_objects()
                    sum1 = summary.summarize(all_objects)
                    f.write("\n".join(summary.format_(sum1)))
                    logger.info(f"Logged memory diagnostics to mem_diagnostics/mem_diagnostics.txt")

            # Main monitoring loop
            while not self._shutdown_event.is_set():
                # Check pod process health
                all_processes_dead = True
                dead_pods = []
                
                for pod_id, pod_procs in self.pod_processes.items():
                    # Check fetchers
                    for proc in pod_procs['fetchers']:
                        if proc.is_alive():
                            all_processes_dead = False
                        else:
                            logger.error(f"Process {proc.name} died unexpectedly!")
                            if proc.exitcode != 0:
                                logger.error(f"Process {proc.name} exit code: {proc.exitcode}")
                    
                    # Check parsers
                    for proc in pod_procs['parsers']:
                        if proc.is_alive():
                            all_processes_dead = False
                        else:
                            logger.error(f"Process {proc.name} died unexpectedly!")
                            if proc.exitcode != 0:
                                logger.error(f"Process {proc.name} exit code: {proc.exitcode}")
                    
                    # Check if all processes in this pod are dead
                    pod_dead = all(not proc.is_alive() for proc in pod_procs['fetchers'] + pod_procs['parsers'])
                    if pod_dead:
                        dead_pods.append(pod_id)
                
                # Check local fetchers too
                for pod_id, fetcher in self.local_pod_fetchers.items():
                    if fetcher:
                        all_processes_dead = False
                
                if all_processes_dead and (self.pod_processes or self.local_pod_fetchers):
                    logger.error("All child processes have died!")
                    self._shutdown_event.set()
                    break
                
                # Log dead pods but don't restart automatically (would require careful resharding)
                if dead_pods:
                    logger.error(f"Lost all processes for pods: {dead_pods}. Manual restart required.")
                
                if time.time() - self.last_metrics_log_time >= METRICS_LOG_INTERVAL_SECONDS:
                    await self._log_metrics()
                    
                    # Analyze FD types
                    await self._analyze_fd_types()
                

                if LOG_MEM_DIAGNOSTICS and time.time() - self.last_mem_diagnostics_time >= MEM_DIAGNOSTICS_INTERVAL_SECONDS:
                    import datetime
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    rss_mem_bytes = current_process.memory_info().rss
                    rss_mem_mb = rss_mem_bytes / (1024 * 1024)
                    report_file = f"mem_diagnostics/memory_report_{timestamp}_{rss_mem_mb:.0f}MB.txt"
                    try:
                        # Enhanced memory diagnostics every GC interval
                        logger.info("[Memory] Generating detailed memory diagnostics...")
                        report = memory_diagnostics.generate_report(self)
                        
                        # Log the report line by line to avoid truncation
                        for line in report.split('\n'):
                            logger.info(f"[Memory] {line}")
                        with open(report_file, 'w') as f:
                            f.write(report)
                        logger.warning(f"[Memory] High memory usage detected! Detailed report saved to {report_file}")
                    except Exception as e:
                        logger.error(f"[Memory] Failed to save memory report: {e}")
                
                if time.time() - self.last_gc_time >= GC_INTERVAL_SECONDS:
                    import pympler
                    rss_mem_bytes = current_process.memory_info().rss
                    rss_mem_mb = rss_mem_bytes / (1024 * 1024)
                    if rss_mem_mb > GC_RSS_THRESHOLD_MB:
                        logger.info(f"[GC] Running garbage collection due to memory usage: {rss_mem_mb:.2f} MB")
                        gc.collect()
                    self.last_gc_time = time.time()

                # TODO: frontier count is very slow with redis right now
                estimated_frontier_size = -1
                # Count active fetcher processes across all pods
                active_fetcher_processes = len(self.local_pod_fetchers)  # Local fetchers
                for pod_procs in self.pod_processes.values():
                    active_fetcher_processes += sum(1 for p in pod_procs['fetchers'] if p.is_alive())
                
                # Update Prometheus gauges
                frontier_size_gauge.set(estimated_frontier_size)
                
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
                        
                        # Update Prometheus gauges with labels
                        process_memory_usage_gauge.labels(
                            process_type='orchestrator',
                            process_id='0'
                        ).set(rss_mem_bytes)
                        process_open_fds_gauge.labels(
                            process_type='orchestrator',
                            process_id='0'
                        ).set(fds_count_int)
                    except psutil.Error as e:
                        logger.warning(f"psutil error during resource monitoring: {e}")
                        # Fallback or disable further psutil calls for this iteration if needed
                
                status_parts = [
                    f"Crawled={self.pages_crawled_count}",
                    f"Frontier={estimated_frontier_size}",
                    f"AddedToFrontier={self.total_urls_added_to_frontier}",
                    f"FetcherProcs={active_fetcher_processes}/{self.config.num_fetcher_processes}",
                    f"MemRSS={rss_mem_mb}",
                    f"OpenFDs={fds_count}",
                    f"Runtime={(time.time() - self.start_time):.0f}s"
                ]
                logger.info(f"Status: {', '.join(status_parts)}")
                
                # Check if frontier is empty by checking domain queues across all pods
                if self.redis_client:
                    total_queue_size = 0
                    for pod_id in range(self.config.num_pods):
                        redis_client = await self.pod_manager.get_redis_client(pod_id)
                        queue_size = await redis_client.llen('domains:queue')
                        total_queue_size += queue_size
                    
                    if total_queue_size == 0:
                        # Check if we've actually started crawling
                        total_pages = get_pages_crawled_total()
                        if total_pages > 0:
                            logger.info(f"All frontiers empty. Pages crawled: {total_pages}. Monitoring...")
                        await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2)
                        
                        # Re-check after wait
                        total_queue_size = 0
                        for pod_id in range(self.config.num_pods):
                            redis_client = await self.pod_manager.get_redis_client(pod_id)
                            queue_size = await redis_client.llen('domains:queue')
                            total_queue_size += queue_size
                        
                        if total_queue_size == 0:
                            logger.info("All frontiers confirmed empty after wait. Signaling shutdown.")
                            self._shutdown_event.set()
                            break
                
                # Check global stopping conditions (max_pages, max_duration)
                if self.config.max_pages:
                    # Get actual page count from aggregated Prometheus metrics
                    total_pages_crawled = get_pages_crawled_total()
                    self.pages_crawled_count = total_pages_crawled  # Update local count for logging
                    
                    if total_pages_crawled >= self.config.max_pages:
                        logger.info(f"Stopping: Max pages reached ({total_pages_crawled}/{self.config.max_pages})")
                        self._shutdown_event.set()
                if self.config.max_duration and (time.time() - self.start_time) >= self.config.max_duration:
                    logger.info(f"Stopping: Max duration reached ({time.time() - self.start_time:.0f}s / {self.config.max_duration}s)")
                    self._shutdown_event.set()

                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=ORCHESTRATOR_STATUS_INTERVAL_SECONDS)
                except asyncio.TimeoutError:
                    pass # Continue loop for status update and checks
            
            logger.info("Shutdown signaled. Stopping processes...")
            
            # Stop local fetchers first
            for pod_id, fetcher in self.local_pod_fetchers.items():
                if fetcher:
                    logger.info(f"Stopping local fetcher for pod {pod_id}...")
                    fetcher._shutdown_event.set()
            
            # Stop all pod processes
            self._stop_all_pod_processes()
            
            logger.info("All processes stopped.")

        except Exception as e:
            logger.critical(f"Orchestrator critical error: {e}", exc_info=True)
            self._shutdown_event.set() # Ensure shutdown on critical error
        finally:
            logger.info("Performing final cleanup...")
            
            # Ensure local fetchers are stopped
            for fetcher in self.local_pod_fetchers.values():
                if fetcher:
                    fetcher._shutdown_event.set()
            await asyncio.sleep(1)  # Give them a moment to clean up
            
            # Processes should already be stopped, but ensure they are
            self._stop_all_pod_processes()
            
            # Wait for local fetcher task to complete
            if 'local_fetcher_task' in locals() and not local_fetcher_task.done():
                logger.info("Waiting for local fetcher to finish...")
                try:
                    await asyncio.wait_for(local_fetcher_task, timeout=10)
                except asyncio.TimeoutError:
                    logger.warning("Local fetcher didn't finish in time, cancelling...")
                    local_fetcher_task.cancel()
                    try:
                        await local_fetcher_task
                    except asyncio.CancelledError:
                        pass
            
            # Close all Redis connections
            await self.pod_manager.close_all()
            
            # Get final page count from aggregated metrics
            final_pages_crawled = get_pages_crawled_total()
            logger.info(f"Crawl finished. Total pages crawled across all fetchers: {final_pages_crawled}")
            logger.info(f"Total runtime: {(time.time() - self.start_time):.2f} seconds.") 