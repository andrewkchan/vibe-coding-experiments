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
    # Single process mode - use default registry
    registry = None

# Define metrics with explicit registry
pages_crawled_counter = Counter(
    'pages_crawled_total', 
    'Total number of pages successfully crawled',
    registry=registry
)

urls_added_counter = Counter(
    'urls_added_total', 
    'Total number of URLs added to the frontier',
    registry=registry
)

errors_counter = Counter(
    'errors_total', 
    'Total number of errors by type', 
    ['error_type'],
    registry=registry
)

# For gauges in multiprocess mode, we need to specify the multiprocess_mode
# 'livesum' means the gauge shows the sum of all process values
# Other options: 'liveall' (show all values), 'min', 'max'
pages_per_second_gauge = Gauge(
    'pages_per_second', 
    'Pages crawled per second (recent rate)',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

parser_pages_per_second_gauge = Gauge(
    'parser_pages_per_second', 
    'Pages parsed per second',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

frontier_size_gauge = Gauge(
    'frontier_size', 
    'Current size of the URL frontier',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

active_workers_gauge = Gauge(
    'active_workers', 
    'Number of active crawler workers',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

active_parser_workers_gauge = Gauge(
    'active_parser_workers', 
    'Number of active parser workers',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

parse_queue_size_gauge = Gauge(
    'parse_queue_size', 
    'Number of items in parse queue',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

memory_usage_gauge = Gauge(
    'memory_usage_bytes', 
    'Current memory usage in bytes',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

open_fds_gauge = Gauge(
    'open_file_descriptors', 
    'Number of open file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

db_pool_available_gauge = Gauge(
    'db_pool_available_connections', 
    'Available database connections in the pool',
    multiprocess_mode='livemin' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# Histograms work normally in multiprocess mode
fetch_duration_histogram = Histogram(
    'fetch_duration_seconds', 
    'Time taken to fetch a URL',
    ['fetch_type'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=registry
)

parse_duration_histogram = Histogram(
    'parse_duration_seconds', 
    'Time to parse HTML',
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=registry
)

db_connection_acquire_histogram = Histogram(
    'db_connection_acquire_seconds',
    'Time to acquire a database connection from the pool',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0),
    registry=registry
)

db_query_duration_histogram = Histogram(
    'db_query_duration_seconds',
    'Time taken for database queries',
    ['query_name'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=registry
)

content_size_histogram = Histogram(
    'content_size_bytes',
    'Size of fetched content in bytes',
    ['content_type', 'fetch_type'],
    buckets=(1024, 10240, 102400, 1048576, 10485760),  # 1KB, 10KB, 100KB, 1MB, 10MB
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
        if os.environ.get('PROMETHEUS_PARENT_PROCESS') == 'true':
            # Start a custom HTTP server that aggregates metrics
            from wsgiref.simple_server import make_server
            
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
            
            httpd = make_server('', port, metrics_app)
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