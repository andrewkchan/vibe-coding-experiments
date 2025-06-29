"""
Prometheus metrics definitions and server setup for the crawler.
Supports both single-process and multi-process modes.
"""
import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from prometheus_client import multiprocess
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client import values

# Check if we're in multiprocess mode
PROMETHEUS_MULTIPROC_DIR = os.environ.get('prometheus_multiproc_dir')

# Create registry based on mode
if PROMETHEUS_MULTIPROC_DIR:
    # Multiprocess mode - metrics will be aggregated across processes
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
else:
    # Single process mode - use default registry (None means use the global default)
    registry = None  # type: ignore[assignment]

# Define metrics with explicit registry
pages_crawled_counter = Counter(
    'crawler_pages_crawled_total', 
    'Total number of pages successfully crawled',
    registry=registry
)

urls_added_counter = Counter(
    'crawler_urls_added_total', 
    'Total number of URLs added to the frontier',
    registry=registry
)

errors_counter = Counter(
    'crawler_errors_total', 
    'Total number of errors by type', 
    ['error_type'],
    registry=registry
)

# Fetch-specific counters
fetch_counter = Counter(
    'crawler_fetches_total',
    'Total number of fetch attempts',
    ['fetch_type'],  # Labels: robots_txt, page
    registry=registry
)

fetch_error_counter = Counter(
    'crawler_fetch_errors_total',
    'Total number of fetch errors by type',
    ['error_type', 'fetch_type'],  # Labels: (timeout, connection_error, etc.), (robots_txt, page)
    registry=registry
)

backpressure_events_counter = Counter(
    'backpressure_events_total',
    'Total number of backpressure events by type',
    ['backpressure_type'],  # soft_limit, hard_limit
    registry=registry
)

# Parser-specific counters
parse_processed_counter = Counter(
    'crawler_parse_processed_total',
    'Total pages parsed',
    registry=registry
)

parse_errors_counter = Counter(
    'crawler_parse_errors_total',
    'Total parsing errors',
    ['error_type'],
    registry=registry
)

# For gauges in multiprocess mode, we need to specify the multiprocess_mode
# 'livesum' means the gauge shows the sum of all process values
# Other options: 'liveall' (show all values), 'min', 'max'
pages_per_second_gauge = Gauge(
    'crawler_pages_per_second', 
    'Pages crawled per second (recent rate)',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

parser_pages_per_second_gauge = Gauge(
    'crawler_parser_pages_per_second', 
    'Pages parsed per second',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

frontier_size_gauge = Gauge(
    'crawler_frontier_size', 
    'Current size of the URL frontier',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

active_workers_gauge = Gauge(
    'crawler_active_workers', 
    'Number of active crawler workers',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

active_parser_workers_gauge = Gauge(
    'crawler_active_parser_workers', 
    'Number of active parser workers',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

parse_queue_size_gauge = Gauge(
    'crawler_parse_queue_size', 
    'Number of items in parse queue (fetch:queue)',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

memory_usage_gauge = Gauge(
    'crawler_memory_usage_bytes', 
    'Current memory usage in bytes',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

open_fds_gauge = Gauge(
    'crawler_open_fds', 
    'Number of open file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

db_pool_available_gauge = Gauge(
    'crawler_db_pool_available_connections', 
    'Available database connections in the pool',
    multiprocess_mode='livemin' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# Histograms work normally in multiprocess mode
fetch_duration_histogram = Histogram(
    'crawler_fetch_duration_seconds', 
    'Time taken to fetch a URL',
    ['fetch_type'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=registry
)

fetch_timing_histogram = Histogram(
    'crawler_fetch_timing_seconds',
    'Detailed fetch timing breakdown',
    ['phase', 'fetch_type'],  # Labels: (dns_lookup, connect, ssl_handshake, transfer, total), (robots_txt, page)
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=registry
)

parse_duration_histogram = Histogram(
    'crawler_parse_duration_seconds', 
    'Time to parse HTML',
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=registry
)

db_connection_acquire_histogram = Histogram(
    'crawler_db_connection_acquire_seconds',
    'Time to acquire a database connection from the pool',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0),
    registry=registry
)

db_query_duration_histogram = Histogram(
    'crawler_db_query_duration_seconds',
    'Time taken for database queries',
    ['query_name'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=registry
)

content_size_histogram = Histogram(
    'crawler_content_size_bytes',
    'Size of fetched content in bytes',
    ['content_type', 'fetch_type'],
    buckets=(1024, 10240, 102400, 1048576, 10485760),  # 1KB, 10KB, 100KB, 1MB, 10MB
    registry=registry
)

# System metrics
cpu_usage_gauge = Gauge(
    'system_cpu_percent',
    'CPU usage percentage',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

memory_free_gauge = Gauge(
    'system_memory_free_bytes',
    'Free system memory in bytes',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

memory_available_gauge = Gauge(
    'system_memory_available_bytes',
    'Available system memory in bytes',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

disk_free_gauge = Gauge(
    'system_disk_free_bytes',
    'Free disk space in bytes',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

disk_usage_percent_gauge = Gauge(
    'system_disk_usage_percent',
    'Disk usage percentage',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# IO metrics
io_read_count_gauge = Gauge(
    'system_io_read_count',
    'Number of read operations',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

io_write_count_gauge = Gauge(
    'system_io_write_count',
    'Number of write operations',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

io_read_bytes_gauge = Gauge(
    'system_io_read_bytes',
    'Bytes read',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

io_write_bytes_gauge = Gauge(
    'system_io_write_bytes',
    'Bytes written',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# Network metrics
network_bytes_sent_gauge = Gauge(
    'system_network_bytes_sent',
    'Network bytes sent',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

network_bytes_recv_gauge = Gauge(
    'system_network_bytes_received',
    'Network bytes received',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

network_packets_sent_gauge = Gauge(
    'system_network_packets_sent',
    'Network packets sent',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

network_packets_recv_gauge = Gauge(
    'system_network_packets_received',
    'Network packets received',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# Redis metrics
redis_ops_per_sec_gauge = Gauge(
    'redis_ops_per_second',
    'Redis operations per second',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

redis_memory_usage_gauge = Gauge(
    'redis_memory_usage_bytes',
    'Redis memory usage in bytes',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

redis_connected_clients_gauge = Gauge(
    'redis_connected_clients',
    'Number of Redis connected clients',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

redis_hit_rate_gauge = Gauge(
    'redis_hit_rate_percent',
    'Redis cache hit rate percentage',
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

redis_latency_histogram = Histogram(
    'redis_command_latency_seconds',
    'Redis command latency',
    ['command_type'],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1),
    registry=registry
)

def start_metrics_server(port=8001):
    """Start the Prometheus metrics HTTP server.
    
    In multiprocess mode, only the parent process should start the server.
    Child processes just write metrics to the shared directory.
    """
    if PROMETHEUS_MULTIPROC_DIR:
        # In multiprocess mode, check if we're the main process
        # The orchestrator sets this environment variable
        if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
            # Start a custom HTTP server that aggregates metrics
            from wsgiref.simple_server import make_server, WSGIRequestHandler
            # Suppress logging from wsgiref
            class NoLoggingWSGIRequestHandler(WSGIRequestHandler):
                def log_message(self, format, *args):
                    pass
            
            def metrics_app(environ, start_response):
                registry = CollectorRegistry()
                multiprocess.MultiProcessCollector(registry)
                data = generate_latest(registry)
                status = '200 OK'
                headers = [
                    ('Content-Type', CONTENT_TYPE_LATEST),
                    ('Content-Length', str(len(data)))
                ]
                start_response(status, headers)
                return [data]
            
            httpd = make_server('', port, metrics_app, handler_class=NoLoggingWSGIRequestHandler)
            import threading
            t = threading.Thread(target=httpd.serve_forever)
            t.daemon = True
            t.start()
            return True
        else:
            # Child process - don't start server
            return False
    else:
        # Single process mode - start server normally
        start_http_server(port, registry=registry)
        return True 