"""Metrics collection for the web crawler using Prometheus."""

import asyncio
import logging
from typing import Optional
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from prometheus_client.core import CollectorRegistry

logger = logging.getLogger(__name__)

# Create a custom registry to avoid conflicts
REGISTRY = CollectorRegistry()

# Counters (always increasing)
pages_crawled_counter = Counter(
    'crawler_pages_crawled_total',
    'Total number of pages successfully crawled',
    registry=REGISTRY
)

urls_added_counter = Counter(
    'crawler_urls_added_total',
    'Total number of URLs added to frontier',
    registry=REGISTRY
)

errors_counter = Counter(
    'crawler_errors_total',
    'Total number of errors by type',
    ['error_type'],  # Labels: fetch_error, parse_error, db_error, etc.
    registry=REGISTRY
)

# Fetch-specific counters
fetch_counter = Counter(
    'crawler_fetches_total',
    'Total number of fetch attempts',
    ['fetch_type'],  # Labels: robots_txt, page
    registry=REGISTRY
)

fetch_error_counter = Counter(
    'crawler_fetch_errors_total',
    'Total number of fetch errors by type',
    ['error_type', 'fetch_type'],  # Labels: (timeout, connection_error, etc.), (robots_txt, page)
    registry=REGISTRY
)

# Gauges (can go up or down)
pages_per_second_gauge = Gauge(
    'crawler_pages_per_second',
    'Current crawl rate in pages per second',
    registry=REGISTRY
)

frontier_size_gauge = Gauge(
    'crawler_frontier_size',
    'Current size of the URL frontier',
    registry=REGISTRY
)

active_workers_gauge = Gauge(
    'crawler_active_workers',
    'Number of currently active worker tasks',
    registry=REGISTRY
)

memory_usage_gauge = Gauge(
    'crawler_memory_usage_bytes',
    'Current RSS memory usage in bytes',
    registry=REGISTRY
)

open_fds_gauge = Gauge(
    'crawler_open_fds',
    'Number of open file descriptors',
    registry=REGISTRY
)

db_pool_available_gauge = Gauge(
    'crawler_db_pool_available',
    'Number of available database connections in pool',
    registry=REGISTRY
)

# Histograms (measure distributions)
fetch_duration_histogram = Histogram(
    'crawler_fetch_duration_seconds',
    'Time taken to fetch a URL',
    ['fetch_type'],  # Label to distinguish robots.txt vs page fetches
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=REGISTRY
)

fetch_timing_histogram = Histogram(
    'crawler_fetch_timing_seconds',
    'Detailed fetch timing breakdown',
    ['phase', 'fetch_type'],  # Labels: (dns_lookup, connect, ssl_handshake, transfer, total), (robots_txt, page)
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=REGISTRY
)

db_connection_acquire_histogram = Histogram(
    'crawler_db_connection_acquire_seconds',
    'Time taken to acquire a database connection',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
    registry=REGISTRY
)

db_query_duration_histogram = Histogram(
    'crawler_db_query_duration_seconds',
    'Time taken to execute database queries',
    ['query_name'],  # Label for different query types
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=REGISTRY
)

class MetricsServer:
    """Manages the Prometheus metrics HTTP server."""
    
    def __init__(self, port: int = 8001):
        self.port = port
        self._server_started = False
    
    def start(self):
        """Start the metrics HTTP server."""
        if not self._server_started:
            try:
                start_http_server(self.port, registry=REGISTRY)
                self._server_started = True
                logger.info(f"Metrics server started on port {self.port}")
            except Exception as e:
                logger.error(f"Failed to start metrics server: {e}")
                raise
    
    def is_running(self) -> bool:
        """Check if the metrics server is running."""
        return self._server_started

# Global metrics server instance
metrics_server = MetricsServer()

def start_metrics_server(port: int = 8001):
    """Start the Prometheus metrics HTTP server."""
    metrics_server.port = port
    metrics_server.start() 