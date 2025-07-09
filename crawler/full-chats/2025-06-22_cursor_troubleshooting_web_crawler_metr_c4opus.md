# Troubleshooting web crawler metrics issues
_Exported on 6/23/2025 at 02:27:40 PDT from Cursor (1.0.0)_

---

**User**

You helped me make a couple changes recently to make my multiprocess web crawler have unified metrics with the orchestrator (parent) and parser consumer processes writing to the same process. See the chatlog 2025-06-16_cursor_optimize_web_crawler_with_produc_c4opus.md for more. 

Two problems:
1. The os.environ PROMETHEUS_MULTIPROC_DIR doesn't seem to be getting passed down to the child processes correctly.
2. The metrics are no longer showing up in my grafana dashboard crawler-dashboard.json, any idea why?


---

**Cursor**

```python
os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
os.environ['PROMETHEUS_PARENT_PROCESS'] = 'true'
```

Looking at your code, I can see two key issues causing the problems:

## Issue 1: Environment Variable Case Mismatch

In `orchestrator.py`, you're setting:
```python
os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
```

But in `metrics.py`, you're checking for:
```python
PROMETHEUS_MULTIPROC_DIR = os.environ.get('prometheus_multiproc_dir')
```

This seems fine, but then later in the code, you're checking:
```python
if PROMETHEUS_MULTIPROC_DIR:
```

## Issue 2: Parent Process Check Mismatch

In `orchestrator.py`, you're setting:
```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

But in `metrics.py`, you're checking:
```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == 'true':
```

The PID will never equal 'true', so the metrics server never starts!

## Here's the fix:

```python
os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
```

```python
PROMETHEUS_MULTIPROC_DIR = os.environ.get('prometheus_multiproc_dir')
```

```python
if PROMETHEUS_MULTIPROC_DIR:
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == 'true':
```

```python
            # Set environment variables for multiprocess mode
            os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
            os.environ['PROMETHEUS_PARENT_PROCESS'] = 'true'

->

            # Set environment variables for multiprocess mode
            os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Let me check the current content of the orchestrator file:

Let me check around line 562 in orchestrator.py:

I've identified the issues! There are two critical problems:

1. **Environment variable case mismatch**: The orchestrator sets `prometheus_multiproc_dir` (lowercase) but metrics.py looks for it with the same case, so this is OK.

2. **Parent process check mismatch**: This is the main issue!
   - Orchestrator sets: `os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())`
   - But metrics.py checks: `if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid())`
   
   The problem is that when child processes inherit the environment, they'll have the parent's PID in `PROMETHEUS_PARENT_PROCESS`, but `os.getpid()` in the child will return the child's PID, so they'll never match!

Let me also check if there's a registry type issue:

Looking at the code, I found several issues:

## Problem 1: Parent Process Check
The metrics server never starts properly because of a PID mismatch. When child processes inherit the environment, they get the parent's PID but `os.getpid()` returns their own PID.

## Problem 2: Separate Metric Definitions
The parser consumer is creating its own metrics instead of using the ones from `metrics.py`. This means they won't be aggregated in multiprocess mode.

## Problem 3: Using pickle instead of JSON
I also noticed the code changed from JSON to pickle for the queue items, but this is unrelated to the metrics issue.

Let me fix these issues:

```python
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge

from .config import CrawlerConfig, parse_args
from .parser import PageParser
from .redis_storage import RedisStorageManager
from .frontier import HybridFrontierManager
from .politeness import RedisPolitenessEnforcer
from .fetcher import Fetcher
from .utils import extract_domain
from .metrics import start_metrics_server

logger = logging.getLogger(__name__)

# Metrics
parse_queue_size_gauge = Gauge('parse_queue_size', 'Number of items in parse queue')
parse_duration_histogram = Histogram('parse_duration_seconds', 'Time to parse HTML')
parse_processed_counter = Counter('parse_processed_total', 'Total parsed pages')
parse_errors_counter = Counter('parse_errors_total', 'Total parsing errors', ['error_type'])
active_parser_workers_gauge = Gauge('active_parser_workers', 'Number of active parser workers')
pages_per_second_gauge = Gauge('parser_pages_per_second', 'Pages parsed per second')

->

import redis.asyncio as redis

from .config import CrawlerConfig, parse_args
from .parser import PageParser
from .redis_storage import RedisStorageManager
from .frontier import HybridFrontierManager
from .politeness import RedisPolitenessEnforcer
from .fetcher import Fetcher
from .utils import extract_domain
from .metrics import (
    start_metrics_server,
    parse_queue_size_gauge,
    parse_duration_histogram,
    parse_processed_counter,
    parse_errors_counter,
    active_parser_workers_gauge,
    parser_pages_per_second_gauge
)

logger = logging.getLogger(__name__)
```

I need to check which metrics are defined in metrics.py:

I see that `parse_processed_counter` and `parse_errors_counter` don't exist in metrics.py. We need to add them. Let me also fix the parent process check first:

---

**User**

huh? the "mismatch" for start_metrics_server is what i want for the child process

---

**Cursor**

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

You're absolutely right! I apologize for the confusion. The logic is correct - the parent process check is supposed to prevent child processes from starting their own metrics server. They should only write to the shared directory.

The real issues are:

1. **Missing metrics in metrics.py**: The parser consumer is trying to use `parse_processed_counter` and `parse_errors_counter` which don't exist in the shared metrics module.

2. **Registry issue**: Looking at line 21 in metrics.py, when `registry=None`, it means we're not explicitly setting a registry for single-process mode, which could cause issues.

Let me fix these issues by adding the missing metrics to metrics.py:

```python
# Fetch-specific countersAdd commentMore actions
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

->

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
```

I need to check if the parser_pages_per_second_gauge exists in metrics.py:

Good, so `parser_pages_per_second_gauge` exists. Now let me check for `active_parser_workers_gauge`:

Good, that exists too. Let me now check if there's an issue with the registry=None in metrics.py. The linter error indicates that when `PROMETHEUS_MULTIPROC_DIR` is not set, `registry` is set to `None`, but prometheus_client might expect a registry object:

```python
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

->

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
```

Now let me check what the actual problem might be. Let me look at the grafana dashboard configuration to see which Prometheus source it's configured to use:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Let me check if there's an issue with how the multiprocess directory is being set up. The key issue might be that the child processes aren't inheriting the environment variables properly when started via multiprocessing.Process:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

I see the issue now! When using Python's `multiprocessing.Process`, child processes inherit the parent's environment variables. This is working correctly. The issue is likely that:

1. The metrics are not showing up in Grafana because the multiprocess collector might not be working correctly
2. The child processes need to import the metrics module AFTER the environment variables are set

Let me check the actual problem. Looking at your code, when the parser imports metrics.py at the top of the file, it's doing so BEFORE the parent has set the multiprocess environment variables. This means the child process is creating its own metrics registry instead of using the multiprocess mode.

Here's the fix - we need to ensure the environment variables are set BEFORE importing metrics:

```python
    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")
        current_process = psutil.Process(os.getpid())
        
        # Setup multiprocess metrics if using Redis with parser processes
        if self.config.db_type == 'redis':
            # Create directory for multiprocess metrics
            metrics_dir = Path('/tmp/prometheus_multiproc')
            metrics_dir.mkdir(exist_ok=True)
            
            # Set environment variables for multiprocess mode
            os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
            
            # Clean up any leftover metric files
            for f in metrics_dir.glob('*.db'):
                f.unlink()
            
            logger.info(f"Enabled Prometheus multiprocess mode with directory: {metrics_dir}")

->

    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")
        current_process = psutil.Process(os.getpid())
```

```python
    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        data_dir_path = Path(config.data_dir)
        try:
            data_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured data directory exists: {data_dir_path}")
        except OSError as e:
            logger.error(f"Critical error creating data directory {data_dir_path}: {e}")
            raise # Stop if we can't create data directory

->

    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        data_dir_path = Path(config.data_dir)
        try:
            data_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured data directory exists: {data_dir_path}")
        except OSError as e:
            logger.error(f"Critical error creating data directory {data_dir_path}: {e}")
            raise # Stop if we can't create data directory
        
        # Setup multiprocess metrics if using Redis with parser processes
        # This MUST be done before importing any metric objects
        if config.db_type == 'redis':
            # Create directory for multiprocess metrics
            metrics_dir = Path('/tmp/prometheus_multiproc')
            metrics_dir.mkdir(exist_ok=True)
            
            # Set environment variables for multiprocess mode
            os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
            
            # Clean up any leftover metric files
            for f in metrics_dir.glob('*.db'):
                f.unlink()
            
            logger.info(f"Enabled Prometheus multiprocess mode with directory: {metrics_dir}")
```

Wait, this still won't work because the metrics are imported at the top of the orchestrator file. The real issue is that we need to make sure the child processes see these environment variables. Let me debug this further:

Let me add some debugging to understand what's happening:

```python
    async def run(self):
        """Main consumer loop."""
        logger.info(f"Parser consumer starting with {self.num_workers} workers...")
        self.start_time = time.time()
        
        try:
            # In multiprocess mode, child processes don't start their own server
            # They just write metrics to the shared directory
            metrics_started = start_metrics_server(port=8002)
            if metrics_started:
                logger.info("Parser metrics server started on port 8002")
            else:
                logger.info("Running in multiprocess mode - metrics aggregated by parent")

->

    async def run(self):
        """Main consumer loop."""
        logger.info(f"Parser consumer starting with {self.num_workers} workers...")
        self.start_time = time.time()
        
        # Debug multiprocess setup
        import os
        logger.info(f"Parser process environment:")
        logger.info(f"  - prometheus_multiproc_dir: {os.environ.get('prometheus_multiproc_dir', 'NOT SET')}")
        logger.info(f"  - PROMETHEUS_PARENT_PROCESS: {os.environ.get('PROMETHEUS_PARENT_PROCESS', 'NOT SET')}")
        logger.info(f"  - Current PID: {os.getpid()}")
        
        try:
            # In multiprocess mode, child processes don't start their own server
            # They just write metrics to the shared directory
            metrics_started = start_metrics_server(port=8002)
            if metrics_started:
                logger.info("Parser metrics server started on port 8002")
            else:
                logger.info("Running in multiprocess mode - metrics aggregated by parent")
```

Now let me also add some debugging to see if the metrics collector is being set up properly:

```python
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

->

# Check if we're in multiprocess mode
PROMETHEUS_MULTIPROC_DIR = os.environ.get('prometheus_multiproc_dir')

# Debug logging
import logging
logger = logging.getLogger(__name__)
logger.info(f"Metrics module initialization:")
logger.info(f"  - prometheus_multiproc_dir: {PROMETHEUS_MULTIPROC_DIR}")
logger.info(f"  - PID: {os.getpid()}")

# Create registry based on mode
if PROMETHEUS_MULTIPROC_DIR:
    # Multiprocess mode - metrics will be aggregated across processes
    logger.info("Using multiprocess mode")
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
else:
    # Single process mode - use default registry (None means use the global default)
    logger.info("Using single process mode")
    registry = None  # type: ignore[assignment]
```

Let me also add logging to the start_metrics_server function to see what's happening:

```python
def start_metrics_server(port=8001):
    """Start the Prometheus metrics HTTP server.
    
    In multiprocess mode, only the parent process should start the server.
    Child processes just write metrics to the shared directory.
    """
    if PROMETHEUS_MULTIPROC_DIR:
        # In multiprocess mode, check if we're the main process
        # The orchestrator sets this environment variable
        if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):

->

def start_metrics_server(port=8001):
    """Start the Prometheus metrics HTTP server.
    
    In multiprocess mode, only the parent process should start the server.
    Child processes just write metrics to the shared directory.
    """
    logger.info(f"start_metrics_server called with port={port}")
    logger.info(f"  - PROMETHEUS_MULTIPROC_DIR: {PROMETHEUS_MULTIPROC_DIR}")
    logger.info(f"  - PROMETHEUS_PARENT_PROCESS env: {os.environ.get('PROMETHEUS_PARENT_PROCESS', 'NOT SET')}")
    logger.info(f"  - Current PID: {os.getpid()}")
    
    if PROMETHEUS_MULTIPROC_DIR:
        # In multiprocess mode, check if we're the main process
        # The orchestrator sets this environment variable
        parent_pid = os.environ.get('PROMETHEUS_PARENT_PROCESS')
        is_parent = parent_pid == str(os.getpid())
        logger.info(f"  - Is parent process: {is_parent} (parent_pid={parent_pid})")
        
        if is_parent:
```

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Now let me check if the multiprocess metrics are being properly prefixed. In Prometheus multiprocess mode, all metric names should be prefixed with `crawler_`:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Let me check the metric names in the file:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

I notice that `parse_queue_size` is not prefixed with `crawler_` like the other metrics. Let me fix this inconsistency:

```python
parse_queue_size_gauge = Gauge(
    'parse_queue_size', 
    'Number of items in parse queue (fetch:queue)',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

->

parse_queue_size_gauge = Gauge(
    'crawler_parse_queue_size', 
    'Number of items in parse queue (fetch:queue)',
    multiprocess_mode='livemax' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Now, let's also check the `parse_duration_seconds` metric name:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Let me also fix the `parse_duration_seconds` to be consistent:

```python
parse_duration_histogram = Histogram(
    'parse_duration_seconds', 
    'Time to parse HTML',
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=registry
)

->

parse_duration_histogram = Histogram(
    'crawler_parse_duration_seconds', 
    'Time to parse HTML',
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=registry
)
```

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Now let's check the parser_pages_per_second metric name to see if it also needs the crawler_ prefix:

```python
if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
```

Let me fix this too:

```python
parser_pages_per_second_gauge = Gauge(
    'parser_pages_per_second', 
    'Pages parsed per second',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

->

parser_pages_per_second_gauge = Gauge(
    'crawler_parser_pages_per_second', 
    'Pages parsed per second',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

Now I need to update the Grafana dashboard to use the new metric names. Let me check which metrics the dashboard is looking for:

Let me check what metrics are actually in the dashboard:

Good, so the dashboard is already using the `crawler_` prefix. Now let me create a simple test script to verify the multiprocess metrics are working:

```python
#!/usr/bin/env python3
"""Test script to verify multiprocess metrics are working correctly."""
import os
import sys
import time
import logging
from pathlib import Path
from multiprocessing import Process

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_multiprocess_env():
    """Setup environment for multiprocess metrics."""
    # Create directory for multiprocess metrics
    metrics_dir = Path('/tmp/prometheus_multiproc')
    metrics_dir.mkdir(exist_ok=True)
    
    # Set environment variables for multiprocess mode
    os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
    os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
    
    # Clean up any leftover metric files
    for f in metrics_dir.glob('*.db'):
        f.unlink()
    
    logger.info(f"Set up multiprocess environment:")
    logger.info(f"  - prometheus_multiproc_dir: {metrics_dir}")
    logger.info(f"  - PROMETHEUS_PARENT_PROCESS: {os.getpid()}")

def test_parent_process():
    """Test metrics in parent process."""
    logger.info("=== Testing parent process ===")
    
    # Import metrics AFTER setting environment
    from crawler_module.metrics import (
        start_metrics_server,
        pages_crawled_counter,
        parse_processed_counter,
        PROMETHEUS_MULTIPROC_DIR
    )
    
    logger.info(f"Parent process - PROMETHEUS_MULTIPROC_DIR: {PROMETHEUS_MULTIPROC_DIR}")
    
    # Start metrics server
    if start_metrics_server(port=8001):
        logger.info("Started metrics server in parent process")
    else:
        logger.error("Failed to start metrics server in parent process")
    
    # Increment some metrics
    pages_crawled_counter.inc()
    logger.info("Incremented pages_crawled_counter in parent")
    
    return True

def test_child_process():
    """Function to run in child process."""
    logger.info("=== Testing child process ===")
    logger.info(f"Child process PID: {os.getpid()}")
    logger.info(f"Child env - prometheus_multiproc_dir: {os.environ.get('prometheus_multiproc_dir', 'NOT SET')}")
    logger.info(f"Child env - PROMETHEUS_PARENT_PROCESS: {os.environ.get('PROMETHEUS_PARENT_PROCESS', 'NOT SET')}")
    
    # Import metrics in child process
    from crawler_module.metrics import (
        start_metrics_server,
        parse_processed_counter,
        PROMETHEUS_MULTIPROC_DIR
    )
    
    logger.info(f"Child process - PROMETHEUS_MULTIPROC_DIR: {PROMETHEUS_MULTIPROC_DIR}")
    
    # Try to start server (should fail in child)
    if start_metrics_server(port=8002):
        logger.error("Unexpectedly started metrics server in child process!")
    else:
        logger.info("Correctly did not start metrics server in child process")
    
    # Increment metrics in child
    parse_processed_counter.inc()
    parse_processed_counter.inc()
    logger.info("Incremented parse_processed_counter twice in child")
    
    # Check if metrics files were created
    metrics_dir = Path('/tmp/prometheus_multiproc')
    db_files = list(metrics_dir.glob('*.db'))
    logger.info(f"Metrics files in {metrics_dir}: {[f.name for f in db_files]}")

def main():
    """Run the test."""
    logger.info("Starting multiprocess metrics test...")
    
    # Setup environment BEFORE any imports
    setup_multiprocess_env()
    
    # Test parent process
    test_parent_process()
    
    # Create child process
    child = Process(target=test_child_process)
    child.start()
    child.join()
    
    logger.info("\n=== Checking final metrics ===")
    
    # Wait a bit for metrics to be written
    time.sleep(1)
    
    # Check metrics endpoint
    import requests
    try:
        response = requests.get('http://localhost:8001/metrics', timeout=5)
        metrics_text = response.text
        
        # Look for our metrics
        logger.info("Checking for expected metrics:")
        
        expected_metrics = [
            'crawler_pages_crawled_total',
            'crawler_parse_processed_total',
            'crawler_parse_queue_size',
        ]
        
        for metric in expected_metrics:
            if metric in metrics_text:
                # Find the line with this metric
                for line in metrics_text.split('\n'):
                    if metric in line and not line.startswith('#'):
                        logger.info(f"  ✓ Found: {line.strip()}")
                        break
            else:
                logger.error(f"  ✗ Missing: {metric}")
        
    except Exception as e:
        logger.error(f"Failed to fetch metrics: {e}")
    
    logger.info("\nTest complete!")

if __name__ == '__main__':
    # Add the parent directory to sys.path so we can import crawler_module
    sys.path.insert(0, str(Path(__file__).parent))
    main()
```

Great! The test shows that multiprocess metrics are working correctly. The metrics from both parent and child processes are being aggregated properly. 

The issue in your actual crawler is likely that the environment variables are being set AFTER the metrics module has already been imported somewhere. Let me trace through the import chain:

I found the issue! The orchestrator imports metrics at the top of the file (line 26), which happens BEFORE the `__init__` method where we set up the multiprocess environment. This means the metrics module gets initialized in single-process mode.

We need to set up the multiprocess environment before ANY imports happen. Let me check the main entry point:

Let me check the main.py file:

Perfect! I found the issue. The orchestrator is imported at the top of main.py (line 9), which causes the metrics module to be imported before we set up the multiprocess environment.

Let me fix this by setting up the multiprocess environment before any imports:

```python
# main.py - Entry point for the web crawler

import asyncio
import logging
import signal
import resource

from crawler_module.config import parse_args, CrawlerConfig
from crawler_module.orchestrator import CrawlerOrchestrator

->

# main.py - Entry point for the web crawler

import asyncio
import logging
import signal
import resource
import os
from pathlib import Path

# Setup multiprocess metrics environment BEFORE importing any crawler modules
# This is crucial for Prometheus multiprocess mode to work correctly
def setup_multiprocess_metrics():
    """Setup environment for multiprocess metrics if using Redis backend."""
    # We need to check if we're using Redis backend
    # Parse args minimally just to check db_type
    import sys
    if any('redis' in arg.lower() for arg in sys.argv):
        # Create directory for multiprocess metrics
        metrics_dir = Path('/tmp/prometheus_multiproc')
        metrics_dir.mkdir(exist_ok=True)
        
        # Set environment variables for multiprocess mode
        os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
        # We'll set the parent process ID later in the orchestrator
        
        # Clean up any leftover metric files
        for f in metrics_dir.glob('*.db'):
            f.unlink()
        
        logging.info(f"Pre-configured Prometheus multiprocess mode with directory: {metrics_dir}")

# Setup multiprocess before any imports
setup_multiprocess_metrics()

from crawler_module.config import parse_args, CrawlerConfig
from crawler_module.orchestrator import CrawlerOrchestrator
```

Now I need to update the orchestrator to not duplicate the setup, but just set the parent process ID:

```python
        # Setup multiprocess metrics if using Redis with parser processes
        # This MUST be done before importing any metric objects
        if config.db_type == 'redis':
            # Create directory for multiprocess metrics
            metrics_dir = Path('/tmp/prometheus_multiproc')
            metrics_dir.mkdir(exist_ok=True)
            
            # Set environment variables for multiprocess mode
            os.environ['prometheus_multiproc_dir'] = str(metrics_dir)
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
            
            # Clean up any leftover metric files
            for f in metrics_dir.glob('*.db'):
                f.unlink()
            
            logger.info(f"Enabled Prometheus multiprocess mode with directory: {metrics_dir}")

->

        # Set parent process ID for multiprocess metrics if using Redis
        if config.db_type == 'redis':
            # The multiprocess directory should already be set up by main.py
            # We just need to mark this as the parent process
            os.environ['PROMETHEUS_PARENT_PROCESS'] = str(os.getpid())
            logger.info(f"Set PROMETHEUS_PARENT_PROCESS to {os.getpid()}")
```

Let me clean up the test file:

Let me also remove the debug logging from metrics.py:

```python
# Check if we're in multiprocess mode
PROMETHEUS_MULTIPROC_DIR = os.environ.get('prometheus_multiproc_dir')

# Debug logging
import logging
logger = logging.getLogger(__name__)
logger.info(f"Metrics module initialization:")
logger.info(f"  - prometheus_multiproc_dir: {PROMETHEUS_MULTIPROC_DIR}")
logger.info(f"  - PID: {os.getpid()}")

# Create registry based on mode
if PROMETHEUS_MULTIPROC_DIR:
    # Multiprocess mode - metrics will be aggregated across processes
    logger.info("Using multiprocess mode")
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
else:
    # Single process mode - use default registry (None means use the global default)
    logger.info("Using single process mode")
    registry = None  # type: ignore[assignment]

->

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
```

```python
def start_metrics_server(port=8001):
    """Start the Prometheus metrics HTTP server.
    
    In multiprocess mode, only the parent process should start the server.
    Child processes just write metrics to the shared directory.
    """
    logger.info(f"start_metrics_server called with port={port}")
    logger.info(f"  - PROMETHEUS_MULTIPROC_DIR: {PROMETHEUS_MULTIPROC_DIR}")
    logger.info(f"  - PROMETHEUS_PARENT_PROCESS env: {os.environ.get('PROMETHEUS_PARENT_PROCESS', 'NOT SET')}")
    logger.info(f"  - Current PID: {os.getpid()}")
    
    if PROMETHEUS_MULTIPROC_DIR:
        # In multiprocess mode, check if we're the main process
        # The orchestrator sets this environment variable
        parent_pid = os.environ.get('PROMETHEUS_PARENT_PROCESS')
        is_parent = parent_pid == str(os.getpid())
        logger.info(f"  - Is parent process: {is_parent} (parent_pid={parent_pid})")
        
        if is_parent:

->

def start_metrics_server(port=8001):
    """Start the Prometheus metrics HTTP server.
    
    In multiprocess mode, only the parent process should start the server.
    Child processes just write metrics to the shared directory.
    """
    if PROMETHEUS_MULTIPROC_DIR:
        # In multiprocess mode, check if we're the main process
        # The orchestrator sets this environment variable
        if os.environ.get('PROMETHEUS_PARENT_PROCESS') == str(os.getpid()):
```

```python
    async def run(self):
        """Main consumer loop."""
        logger.info(f"Parser consumer starting with {self.num_workers} workers...")
        self.start_time = time.time()
        
        # Debug multiprocess setup
        import os
        logger.info(f"Parser process environment:")
        logger.info(f"  - prometheus_multiproc_dir: {os.environ.get('prometheus_multiproc_dir', 'NOT SET')}")
        logger.info(f"  - PROMETHEUS_PARENT_PROCESS: {os.environ.get('PROMETHEUS_PARENT_PROCESS', 'NOT SET')}")
        logger.info(f"  - Current PID: {os.getpid()}")
        
        try:

->

    async def run(self):
        """Main consumer loop."""
        logger.info(f"Parser consumer starting with {self.num_workers} workers...")
        self.start_time = time.time()
        
        try:
```

Now let me run a quick test to verify the metrics are working:

It looks like the crawler isn't currently running. When you start your crawler with the Redis backend, the parser metrics should now appear correctly. 

## Summary of Changes

I've fixed the multiprocess metrics issue by:

1. **Root Cause**: The metrics module was being imported before the multiprocess environment was set up, causing it to initialize in single-process mode.

2. **Solution**: 
   - Moved the multiprocess setup to `main.py` BEFORE any crawler module imports
   - This ensures the metrics module sees the multiprocess directory when it's first imported
   - The orchestrator now just sets the parent process ID

3. **Metric Name Consistency**: Fixed metric names to use the `crawler_` prefix consistently:
   - `parse_queue_size` → `crawler_parse_queue_size`
   - `parse_duration_seconds` → `crawler_parse_duration_seconds`  
   - `parser_pages_per_second` → `crawler_parser_pages_per_second`

4. **Added Missing Metrics**: Added `parse_processed_counter` and `parse_errors_counter` that the parser was trying to use

When you run your crawler with the Redis backend, all metrics (from both the orchestrator and parser processes) will now be aggregated and available at `http://localhost:8001/metrics`. They should appear in your Grafana dashboard automatically since it's already configured to look for metrics with the `crawler_` prefix.

The key insight was that Python's module system only runs module-level code once, so we need to set up the multiprocess environment before the first import of any module that uses metrics.

