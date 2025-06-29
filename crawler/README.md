# Experimental Web Crawler

This project is an experimental web crawler built in Python. It is designed to be a high-performance, single-machine crawler that uses a hybrid Redis and file-based storage backend.

Refer to `PLAN.MD` for a detailed project plan and architecture.

## Architecture Overview

The crawler uses a multi-process architecture to maximize performance:
*   **Main Orchestrator Process**: Manages a pool of asynchronous worker tasks that fetch URLs.
*   **Parser Processes**: One or more separate processes that consume raw HTML content, parse it, extract links, and save the content.

This design separates I/O-bound work (fetching) from CPU-bound work (parsing), allowing the system to better utilize machine resources. Communication between the fetchers and parsers is handled via a Redis queue.

## Core Components

*   `crawler_module/orchestrator.py`: The main controller. It spawns and manages fetcher workers and parser processes.
*   `crawler_module/parser_consumer.py`: The consumer process that handles all HTML parsing logic.
*   `crawler_module/frontier.py`: Manages the queue of URLs to crawl. It uses a hybrid approach:
    *   **Redis**: Stores domain metadata, scheduling information, and URL de-duplication data (via a Bloom filter).
    *   **File System**: Stores the actual frontier URLs in per-domain files to handle a virtually unlimited frontier size.
*   `crawler_module/storage.py`: Handles saving visited page metadata to Redis and text content to the file system.
*   `crawler_module/politeness.py`: Enforces politeness rules (`robots.txt`, crawl delays) using Redis for caching.
*   `crawler_module/fetcher.py`: Asynchronously fetches web content using `aiohttp`.
*   `crawler_module/parser.py`: The core HTML parsing logic using `lxml`.
*   `main.py`: The main entry point to run the crawler.

## Setup

### 1. Dependencies

*   Python 3.8+
*   Docker and Docker Compose

### 2. Environment Setup

1.  **Clone Project**: Get the project files into a local directory.
2.  **Navigate to `crawler/` directory**.
3.  **Create Python Virtual Environment**:
    ```bash
    python3 -m venv .venv
    ```
4.  **Activate Virtual Environment**:
    *   macOS/Linux: `source .venv/bin/activate`
5.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### 3. Start Redis

The crawler requires a Redis instance with RedisBloom support. The simplest way to get this is with Docker.

1.  **Start Redis using Docker Compose:**
    ```bash
    cd crawler
    docker-compose up -d redis
    ```
    This starts a Redis container with persistence enabled and a 12GB memory limit.

2.  **Verify it's working (optional):**
    ```bash
    # Connect to the Redis container
    docker exec -it crawler-redis redis-cli
    
    # Test a Bloom Filter command
    127.0.0.1:6379> BF.RESERVE test:bloom 0.001 1000
    OK
    ```

## Running the Crawler

The crawler is run via `main.py`.

```bash
python main.py --seed-file path/to/your/seeds.txt --email your.email@example.com [options]
```

**Required Arguments:**

*   `--seed-file <path>`: Path to the seed file (newline-separated domains/URLs).
*   `--email <email_address>`: Your contact email for the User-Agent string.

**Key Optional Arguments:**

*   `--data-dir <path>`: Directory to store frontier files and crawled content (default: `./crawler_data`).
*   `--exclude-file <path>`: File of domains to exclude.
*   `--max-workers <int>`: Number of concurrent fetcher tasks (default: 500).
*   `--max-pages <int>`: Stop after crawling this many pages.
*   `--max-duration <seconds>`: Max crawl duration.
*   `--log-level <LEVEL>`: DEBUG, INFO, WARNING, ERROR (default: INFO).
*   `--resume`: Attempt to resume a crawl from existing data.
*   `--seeded-urls-only`: Only crawl seeded URLs

## System Requirements for High Concurrency

When running with many workers, you may need to increase system limits:

```bash
# Run the provided setup script
chmod +x setup_system_limits.sh
./setup_system_limits.sh

# Or manually set limits
ulimit -n 65536  # Increase file descriptor limit
```

## Running Tests

The project uses `pytest`.

1.  Make sure you have activated your virtual environment and installed dependencies.
2.  Navigate to the `crawler/` directory.
3.  Run pytest:
    ```bash
    pytest
    ```
    Or, for more verbose output:
    ```bash
    pytest -v
    ```

## Monitoring

The crawler exposes real-time metrics via Prometheus, which can be visualized with Grafana.

### Quick Start

1.  **Start the monitoring stack**:
    ```bash
    cd crawler
    docker-compose up -d
    ```
    This starts Prometheus (port 9090), Grafana (port 3000), and the required Redis instance.

2.  **Run the crawler**:
    ```bash
    python main.py --seed-file seeds.txt --email your@email.com
    ```
    The orchestrator exposes aggregated metrics from all processes on port 8001.

3.  **Access Grafana**:
    *   Open `http://localhost:3000` in your browser.
    *   Login: `admin` / `admin`.
    *   The "Crawler Dashboard" is pre-configured and will show live data.

### Key Metrics to Watch

*   **Crawl Performance**: `pages_crawled_total`, `parser_pages_per_second`
*   **Queue Health**: `parse_queue_size` (if this grows continuously, your parsers can't keep up with the fetchers)
*   **Resource Usage**: `memory_usage_bytes`
*   **Error Tracking**: `errors_total`

### Stopping the Monitoring Stack

```bash
docker-compose down
```

## Progress Inspection

*   **Logs:** Check console output. The orchestrator, fetcher workers, and parser processes all log to standard output.
*   **Redis Data:** Connect to the Redis instance to inspect metadata.
    ```bash
    docker exec -it crawler-redis redis-cli
    
    # Check number of domains in the ready queue
    127.0.0.1:6379> LLEN domains:queue 
    
    # Get metadata for a domain
    127.0.0.1:6379> HGETALL domain:example.com
    ```
*   **Content and Frontier Files:**
    *   Frontier URLs are stored in `[data-dir]/frontiers/`.
    *   Extracted text content is stored in `[data-dir]/content/`. 