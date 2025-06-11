# Experimental Web Crawler

This project is an experimental/educational web crawler built in Python, designed to crawl web pages, extract text content, and manage politeness and fault tolerance.

Refer to `PLAN.MD` for a detailed project plan and architecture.

## Core Components

*   `crawler_module/config.py`: Handles command-line argument parsing and configuration management.
*   `crawler_module/utils.py`: Provides utility functions, especially for URL normalization and domain extraction.
*   `crawler_module/storage.py`: Manages data persistence using SQLite for state (frontier, visited URLs, domain metadata) and the file system for crawled text content.
*   `crawler_module/fetcher.py`: Asynchronously fetches web content (HTML, robots.txt) using `aiohttp`.
*   `crawler_module/politeness.py`: Enforces politeness rules, including `robots.txt` parsing, crawl delays, and manual exclusion lists.
*   `crawler_module/frontier.py`: Manages the queue of URLs to be crawled, interacting with storage and politeness rules.
*   `crawler_module/parser.py`: Parses HTML content using `lxml` to extract links and text.
*   `crawler_module/orchestrator.py`: Coordinates all components, manages concurrent worker tasks, and controls the overall crawl lifecycle.
*   `crawler_module/db_backends.py`: Database abstraction layer supporting both SQLite and PostgreSQL.
*   `main.py`: The main entry point to run the crawler.

## Database Backend Support

The crawler supports two database backends:

### SQLite (Default)
- **Best for**: Testing, development, and small-scale crawls
- **Max concurrent workers**: ~50-100
- **Setup**: No additional setup required
- **Limitations**: Single writer limitation can cause "database is locked" errors at high concurrency

### PostgreSQL
- **Best for**: Production use and large-scale crawls
- **Max concurrent workers**: 500+
- **Setup**: Requires PostgreSQL installation and configuration
- **Benefits**: True concurrent writes, better performance at scale

For detailed PostgreSQL setup and migration instructions, see [`PostgreSQL_Migration.md`](PostgreSQL_Migration.md).

## Setup

1.  **Clone/Create Project**: Get the project files into a local directory.
2.  **Navigate to `crawler/` directory**.
3.  **Create a Python Virtual Environment**:
    ```bash
    python3 -m venv .venv
    ```
4.  **Activate the Virtual Environment**:
    *   On macOS/Linux:
        ```bash
        source .venv/bin/activate
        ```
    *   On Windows:
        ```bash
        .venv\Scripts\activate
        ```
5.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    Key dependencies include: `aiohttp`, `lxml`, `robotexclusionrulesparser`, `aiofiles`, `cchardet`, `tldextract`, `pytest`, `pytest-asyncio`, `httpx`, and optionally `psycopg[binary,pool]` for PostgreSQL support.

## Running the Crawler

The crawler is run via `main.py` from the project root directory (e.g., `vibe-coding-experiments/` if `crawler` is inside it, or directly from `crawler/` if you `cd` into it first):

### Using SQLite (Default)
```bash
python crawler/main.py --seed-file path/to/your/seeds.txt --email your.email@example.com [options]
```
Or if you are inside the `crawler` directory:
```bash
python main.py --seed-file path/to/your/seeds.txt --email your.email@example.com [options]
```

### Using PostgreSQL
```bash
python main.py --seed-file path/to/your/seeds.txt --email your.email@example.com \
    --db-type postgresql \
    --db-url "postgresql://user:password@localhost/crawler_db" \
    --max-workers 500
```

**Required Arguments:**

*   `--seed-file <path>`: Path to the seed file (newline-separated domains/URLs).
*   `--email <email_address>`: Your contact email for the User-Agent string.

**Optional Arguments (see `crawler_module/config.py` or run with `--help` for full list):**

*   `--data-dir <path>`: Directory for database and content (default: `./crawler_data`).
*   `--exclude-file <path>`: File of domains to exclude.
*   `--max-workers <int>`: Number of concurrent fetchers (default: 20).
*   `--max-pages <int>`: Max pages to crawl.
*   `--max-duration <seconds>`: Max crawl duration.
*   `--log-level <LEVEL>`: DEBUG, INFO, WARNING, ERROR (default: INFO).
*   `--resume`: Attempt to resume from existing data.
*   `--seeded-urls-only`: Only crawl seeded URLs.
*   `--db-type <sqlite|postgresql>`: Database backend to use (default: sqlite).
*   `--db-url <url>`: PostgreSQL connection URL (required if db-type is postgresql).

## System Requirements for High Concurrency

When running with many workers (especially with PostgreSQL), you may need to increase system limits:

```bash
# Run the provided setup script
chmod +x setup_system_limits.sh
./setup_system_limits.sh

# Or manually set limits
ulimit -n 65536  # Increase file descriptor limit
```

## Running Tests

The project uses `pytest` for testing. To run the tests:

1.  Ensure you have activated your virtual environment and installed dependencies (including `pytest` and `pytest-asyncio` from `requirements.txt`).
2.  Navigate to the `crawler/` directory (the root of the crawler project where `tests/` is located).
3.  Run pytest:
    ```bash
    pytest
    ```
    Or, for more verbose output:
    ```bash
    pytest -v
    ```

## Monitoring with Prometheus and Grafana

The crawler includes built-in metrics collection using Prometheus, with visualization through Grafana dashboards.

### Quick Start

1. **Install Docker and Docker Compose** (if not already installed)

2. **Start the monitoring stack**:
   ```bash
   cd crawler
   docker-compose up -d
   ```
   This starts Prometheus (port 9090) and Grafana (port 3000).

3. **Run the crawler** - it will automatically expose metrics on port 8001:
   ```bash
   python main.py --seed-file seeds.txt --email your@email.com
   ```

4. **Access Grafana**:
   - Open http://localhost:3000 in your browser
   - Login with username: `admin`, password: `admin`
   - The crawler dashboard is pre-configured and will show real-time metrics

### Available Metrics

The crawler tracks the following metrics:

- **Crawl Performance**:
  - Pages crawled per second
  - Total pages crawled
  - URLs added to frontier
  - Frontier size

- **Resource Usage**:
  - Memory usage (RSS)
  - Open file descriptors
  - Active worker threads
  - Database connection pool status

- **Performance Metrics**:
  - URL fetch duration (percentiles)
  - Database connection acquisition time
  - Database query execution time

- **Error Tracking**:
  - Errors by type (fetch errors, HTTP errors, etc.)

### Stopping the Monitoring Stack

```bash
docker-compose down
```

To also remove the stored metrics data:
```bash
docker-compose down -v
```

## Progress Inspection

*   **Logs:** Check console output or redirect to a file. Log level is configurable.
*   **Database (`crawler_data/crawler_state.db` within your specified data directory):** Use any SQLite client (e.g., `sqlite3` CLI, DB Browser for SQLite) to inspect tables like `frontier`, `visited_urls`, `domain_metadata`.
*   **Content Files (`crawler_data/content/` within your specified data directory):** Extracted text content is stored in `.txt` files named by URL hash.

## Migrating from SQLite to PostgreSQL

If you have an existing SQLite crawler database and want to migrate to PostgreSQL:

```bash
python migrate_to_postgresql.py \
    --sqlite-db crawler_data/crawler_state.db \
    --pg-url "postgresql://user:password@localhost/crawler_db"
```

See [`PostgreSQL_Migration.md`](PostgreSQL_Migration.md) for detailed instructions. 