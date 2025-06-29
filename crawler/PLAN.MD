# Web Crawler Project Plan

## 1. Introduction

This document outlines the plan for building an experimental/educational web crawler. The primary goals are:
1.  Run on a single machine, crawling up to 50 million pages in 24 hours from a seed list of domains.
2.  Store text content of crawled pages.
3.  Adhere to politeness standards (`robots.txt`, crawl delays, informative User-Agent).
4.  Be fault-tolerant, allowing stopping and resuming.
5.  Provide a way to inspect progress and crawled data.

The project prioritizes simplicity and educational value, using Python as the core language.

## 2. Architecture

The crawler will be an asynchronous, single-process application with support for high concurrency through PostgreSQL.

### 2.1. Core Components

*   **Main Controller:** Initializes and orchestrates the crawl. Manages configuration.
*   **Seed Loader:** Reads the initial list of domains/URLs from the seed file.
*   **Frontier Manager:**
    *   Manages the queue of URLs to be crawled (the "frontier").
    *   Prioritizes URLs based on domain politeness rules (e.g., 70-second delay per domain).
    *   Persists the frontier for fault tolerance.
    *   Uses atomic claim-and-read pattern to prevent worker contention.
*   **Fetcher (`asyncio`/`aiohttp`):**
    *   Downloads web page content for a given URL.
    *   Sets the custom User-Agent string.
*   **Robots.txt Manager:**
    *   Fetches and parses `robots.txt` for each domain.
    *   Provides an interface to check if a URL is allowed for crawling.
    *   Caches `robots.txt` rules.
*   **Parser (`BeautifulSoup4` or `lxml`):**
    *   Extracts new links (URLs) from downloaded HTML content.
    *   Extracts the main text content from the page.
*   **Storage Manager:**
    *   **State Storage (PostgreSQL/SQLite):** Persists the frontier, set of visited URLs, domain last-crawl timestamps, `robots.txt` cache, and manually excluded domains.
        - **PostgreSQL (recommended for production)**: Handles high concurrency with MVCC, supports hundreds of concurrent workers
        - **SQLite (for testing/small scale)**: Simple setup, limited to ~50-100 concurrent workers
    *   **Content Storage (File System):** Stores extracted text content in files.
*   **Politeness Enforcer:**
    *   Ensures `robots.txt` rules are followed.
    *   Enforces the 70-second delay between crawls of the same domain.
    *   Manages the user-provided exclusion list.
*   **Database Abstraction Layer:**
    *   Provides a common interface for both PostgreSQL and SQLite backends
    *   Handles connection pooling and query translation if needed

### 2.2. Data Flow

1.  **Initialization:**
    *   Load configuration (seed file, contact email, exclude list path).
    *   Initialize Storage Manager (connect to SQLite, prepare content directory).
    *   Load initial URLs from seed file into the Frontier. If resuming, load saved frontier.
2.  **Main Crawl Loop (Asynchronous Tasks):**
    *   The Frontier Manager selects the next available URL, prioritizing those whose domains can be crawled (respecting delays).
    *   If the domain's `robots.txt` is not cached or stale, the Robots.txt Manager fetches and parses it.
    *   If the URL is allowed by `robots.txt` and not in the manually excluded list:
        *   The Fetcher downloads the page.
        *   The domain's last crawl timestamp is updated.
        *   The Parser extracts text and new links.
        *   Text content is saved by the Storage Manager.
        *   The URL is marked as visited.
        *   New, valid, and unvisited links are normalized and added to the Frontier.
    *   This process continues until the frontier is empty or a crawl limit (e.g., number of pages, time) is reached.

### 2.3. Concurrency Model

*   The crawler will use Python's `asyncio` library for managing concurrent network operations (fetching pages, `robots.txt`).
*   A configurable number of concurrent "worker" tasks will pull URLs from the frontier.

### 2.4. Architecture Diagram

```mermaid
graph TD
    subgraph User Input
        SeedFile[Seed File]
        ExcludeList[Exclude List - Optional]
        Config["Configuration (email, limits, etc.)"]
    end

    subgraph Crawler Core
        Main[Main Controller]
        Frontier[Frontier Manager]
        Fetcher["Fetcher - aiohttp"]
        RobotsMgr["Robots.txt Manager"]
        Parser["Parser - BeautifulSoup/lxml"]
        Storage[Storage Manager]
        Politeness[Politeness Enforcer]
    end

    subgraph Data Stores
        SQLiteDB["SQLite Database - crawler_state.db"]
        ContentFS["File System - crawler_data/content/"]
    end

    subgraph External Services
        Web[Internet/Websites]
        RobotsTxtFiles[robots.txt files]
    end

    %% Connections
    Config --> Main
    SeedFile --> Main
    ExcludeList --> Politeness

    Main --> Frontier
    Main --> Fetcher
    Main --> Parser
    Main --> Storage
    Main --> Politeness
    Main --> RobotsMgr

    Frontier --> Fetcher
    Fetcher --> Web
    Web --> Fetcher
    Fetcher --> Parser
    Fetcher --> Storage
    Parser --> Frontier
    Parser --> Storage

    Storage --> SQLiteDB
    Storage --> ContentFS

    RobotsMgr --> RobotsTxtFiles
    RobotsTxtFiles --> RobotsMgr
    RobotsMgr --> Politeness
    RobotsMgr --> Fetcher

    Politeness --> Fetcher
    Politeness --> Frontier

    SQLiteDB -.-> Main
    ContentFS -.-> User
    SQLiteDB -.-> User
```

## 3. Key Data Structures and Storage

### 3.1. Configuration

*   **Seed File:** Path to a newline-separated list of initial domains/URLs.
*   **Contact Email:** Required for the User-Agent string.
*   **Exclude File (Optional):** Path to a newline-separated list of domains to exclude.
*   **Data Directory:** Base directory for storing crawled content and the SQLite database.
*   **Max Concurrent Requests:** Number of simultaneous fetch operations.
*   **Target Page Count / Duration:** Optional limits for the crawl.

### 3.2. State Storage (Database Schema)

The schema is designed to work with both PostgreSQL and SQLite:

*   **`frontier` table:**
    *   `id` SERIAL/INTEGER PRIMARY KEY
    *   `url` TEXT UNIQUE NOT NULL
    *   `domain` TEXT NOT NULL
    *   `depth` INTEGER DEFAULT 0
    *   `added_timestamp` BIGINT NOT NULL
    *   `priority_score` REAL DEFAULT 0
    *   `claimed_at` BIGINT DEFAULT NULL (for atomic claim-and-read pattern)
    *   *Indexes: `(domain)`, `(priority_score, added_timestamp)`, `(claimed_at)`*
*   **`visited_urls` table:**
    *   `url_sha256` TEXT PRIMARY KEY (SHA256 hash of the normalized URL)
    *   `url` TEXT NOT NULL
    *   `domain` TEXT NOT NULL
    *   `crawled_timestamp` INTEGER NOT NULL
    *   `http_status_code` INTEGER
    *   `content_type` TEXT
    *   `content_hash` TEXT (e.g., SHA256 of the extracted text, for de-duplication if needed)
    *   `content_storage_path` TEXT (relative path to the stored text file)
    *   `redirected_to_url` TEXT (if redirected)
    *   *Indexes: `(domain)`*
*   **`domain_metadata` table:**
    *   `domain` TEXT PRIMARY KEY
    *   `last_scheduled_fetch_timestamp` INTEGER DEFAULT 0 (when a URL from this domain was last given to a fetcher)
    *   `robots_txt_content` TEXT
    *   `robots_txt_fetched_timestamp` INTEGER
    *   `robots_txt_expires_timestamp` INTEGER (based on `robots.txt` cache headers or a default TTL)
    *   `is_manually_excluded` INTEGER DEFAULT 0 (boolean)
*   **`schema_version` table:**
    *   `version` INTEGER

### 3.3. Content Storage (File System)

*   Raw text content will be stored in files within a dedicated directory (e.g., `crawler_data/content/`).
*   File naming convention: `<url_sha256_hex>.txt`. This avoids deep directory structures and issues with special characters in URLs. The `visited_urls` table will link the URL to its content file.
*   This structure facilitates easy inspection using standard command-line tools.
*   *Potential Issue for 50M pages:* Storing 50 million small files can strain some filesystems (inode limits, directory listing performance). For this educational version, we'll proceed with this simple approach and note it as a scaling concern. Alternatives (like WARC or packing into larger files) add complexity.

## 4. Politeness Implementation Details

*   **User-Agent:** The User-Agent string will be configurable and include:
    *   Project name/purpose.
    *   A link to a page explaining the project (optional, if one exists).
    *   The contact email provided at startup.
    *   Example: `MyEducationalCrawler/1.0 (+http://example.com/crawler-info; mailto:user@example.com)`
*   **`robots.txt`:**
    *   Use a library like `reppy` or `robotexclusionrulesparser` for robust parsing.
    *   Fetch `robots.txt` before crawling any page from a new domain.
    *   Cache `robots.txt` content and parsed rules in the `domain_metadata` table, respecting cache directives or using a default TTL (e.g., 24 hours).
    *   Honor `Crawl-delay` directives if present, otherwise use the default 70-second delay.
*   **Crawl Delay:**
    *   A minimum of 70 seconds between successive requests to the *same domain*.
    *   The `domain_metadata.last_scheduled_fetch_timestamp` will track when a URL from a domain was last handed to a fetcher.
    *   The Frontier Manager will only select URLs from domains where `current_time >= last_scheduled_fetch_timestamp + effective_crawl_delay`.
*   **Exclude List:**
    *   An optional user-provided file containing a newline-separated list of domains to completely exclude. These will be loaded into `domain_metadata` with `is_manually_excluded=1`.

## 5. Fault Tolerance and Resumption

*   All critical state (frontier, visited URLs, domain metadata) will be persisted in the SQLite database.
*   Writes to the database should be committed frequently or after batches of operations.
*   **Resumption:** Upon startup, the crawler will check for an existing `crawler_state.db`.
    *   If found, it will load the frontier, visited set, and other metadata to resume the crawl from where it left off.
    *   If not found, it will start a new crawl, initializing from the seed file.
*   **Signal Handling:** The crawler should attempt a graceful shutdown on signals like `SIGINT` (Ctrl+C) or `SIGTERM`, ensuring current work is saved and the database is in a consistent state.

## 6. Progress Inspection

*   **Database Inspection:** The `crawler_state.db` SQLite file can be inspected using any SQLite client (e.g., `sqlite3` command-line tool).
    *   `SELECT COUNT(*) FROM visited_urls;` (total pages crawled)
    *   `SELECT COUNT(*) FROM frontier;` (size of the current frontier)
    *   `SELECT url, crawled_timestamp FROM visited_urls ORDER BY crawled_timestamp DESC LIMIT 10;` (recently crawled URLs)
*   **Content Inspection:** Text content files (e.g., `crawler_data/content/*.txt`) can be directly viewed using standard tools like `cat`, `less`, `grep`.
    *   To find the content for a specific URL, one would first query the `visited_urls` table for its `content_storage_path`.
*   **Logging:** Implement structured logging to a file and/or console, indicating:
    *   Startup parameters.
    *   Number of URLs added to frontier.
    *   Pages crawled (with URL, status code).
    *   Errors encountered (network issues, parsing errors).
    *   Politeness actions (e.g., respecting `robots.txt`, delays).
    *   Shutdown status.

## 7. Proposed Directory Structure (within `crawler/`)

```
crawler/
├── main.py               # Main script to run the crawler
├── crawler_module/       # Core crawler logic
│   ├── __init__.py
│   ├── fetcher.py
│   ├── parser.py
│   ├── frontier.py
│   ├── storage.py
│   ├── politeness.py
│   ├── utils.py            # Helper functions (e.g., URL normalization)
│   └── config.py           # Configuration handling
├── requirements.txt      # Python dependencies
├── PLAN.md               # This plan document
├── README.md             # Instructions for setup, running, inspection
├── .gitignore
└── crawler_data/         # Created at runtime, contains DB and content
    ├── crawler_state.db
    └── content/
        └── <url_sha256_hex>.txt
        └── ...
```

## 8. Dependencies

*   **Python 3.8+**
*   **`aiohttp`:** For asynchronous HTTP requests.
*   **`beautifulsoup4` or `lxml`:** For HTML parsing. (lxml is generally faster)
*   **Database drivers:**
    *   **`psycopg3[binary]`:** PostgreSQL async driver (recommended for production)
    *   **`sqlite3`:** (Python standard library) For small-scale testing
*   **`robotexclusionrulesparser` or `reppy`:** For `robots.txt` parsing. `reppy` is often well-regarded.
*   **`aiofiles`:** For asynchronous file operations (writing content).
*   *(Optional)* `cchardet` or `charset_normalizer`: For robust character encoding detection. `aiohttp` handles basic cases.

## 9. Setup and Running

### 9.1. Setup

1.  Clone the repository / create the project directory.
2.  Navigate to the `crawler/` directory.
3.  Create a Python virtual environment: `python3 -m venv .venv`
4.  Activate the virtual environment: `source .venv/bin/activate`
5.  Install dependencies: `pip install -r requirements.txt`

### 9.2. Running the Crawler

The crawler will be run via `main.py` with command-line arguments:

```bash
python main.py --seed-file path/to/seeds.txt --email your.email@example.com [options]
```

**Required Arguments:**

*   `--seed-file <path>`: Path to the seed file (newline-separated domains/URLs).
*   `--email <email_address>`: Your contact email for the User-Agent string.

**Optional Arguments:**

*   `--data-dir <path>`: Directory to store database and crawled content (default: `./crawler_data`).
*   `--exclude-file <path>`: Path to a file of domains to exclude.
*   `--max-workers <int>`: Number of concurrent fetcher tasks (default: e.g., 10 or 50).
*   `--max-pages <int>`: Maximum number of pages to crawl.
*   `--max-duration <seconds>`: Maximum duration for the crawl.
*   `--log-level <level>`: Logging level (e.g., INFO, DEBUG).
*   `--resume`: Flag to attempt resuming from existing data directory.
*   `--db-type <sqlite|postgresql>`: Database backend to use (default: sqlite for compatibility, postgresql recommended for scale).
*   `--db-url <url>`: PostgreSQL connection URL (required if db-type is postgresql).

## 10. Deployment Considerations (Linux/Ubuntu)

*   **Virtual Environment:** Always use a virtual environment (`venv`) for managing dependencies.
*   **System Resources:**
    *   **CPU:** `asyncio` is mostly I/O bound, but parsing can be CPU-intensive. Multiple cores will be beneficial.
    *   **RAM:** Memory usage will depend on the size of the frontier, cached data, and number of concurrent tasks. Monitor closely. Storing full page content for parsing can be memory intensive; stream processing or careful management needed.
    *   **Disk I/O:** High throughput is required for writing 50M content files and SQLite transactions. Use a fast SSD.
    *   **Network Bandwidth:** Significant bandwidth will be consumed. Ensure the machine has a good network connection and appropriate quotas.
*   **File Descriptors:** Fetching many connections and writing many files might require increasing the open file descriptor limit (`ulimit -n`).
*   **Performance Target (50M pages/24h):**
    *   ~578 pages/second. This is aggressive for a single Python process on a single machine, even with `asyncio`.
    *   The 70-second delay per domain is a major constraint. The crawler must manage a large and diverse set of domains in its frontier to achieve this throughput.
    *   The number of concurrent workers (`--max-workers`) will need careful tuning. Too few will underutilize the network; too many could overwhelm local resources or the network, or appear abusive to remote servers.
    *   Extensive testing and profiling will be needed. The primary bottlenecks are likely to be politeness delays, network latency, CPU for parsing, and disk I/O for storage.
*   **Database Setup:**
    *   **For SQLite:** No additional setup required, limited to ~50-100 workers
    *   **For PostgreSQL:** 
        - Install PostgreSQL 14+ for better performance
        - Configure `max_connections` to handle worker count + overhead (e.g., 600 for 500 workers)
        - Tune `shared_buffers`, `work_mem`, and `maintenance_work_mem` for performance
        - Use connection pooling (built into psycopg3)

## 11. Educational Aspects & Simplicity

*   **Modular Design:** Code will be organized into logical modules for clarity.
*   **Clear Naming:** Variables and functions will be named descriptively.
*   **Focused Scope:** Avoids advanced features like distributed crawling, JavaScript rendering, or complex anti-bot measure bypasses.
*   **Step-by-Step Implementation:** The project can be built and tested incrementally (e.g., fetcher first, then parser, then storage, etc.).
*   **Direct Inspection:** Using SQLite and flat text files allows for easy examination of the crawler's state and output. 

## 12. Addendum: Performance Target Realism (Post-Initial Discussion)

Following a discussion on the initial performance target of 50 million pages in 24 hours, and considering historical benchmarks (e.g., Michael Nielsen's 2012 crawl achieving ~7.5 million URLs per machine per 24 hours with Python, lxml, and a 70-second politeness delay), a more realistic initial expectation for this single-machine `asyncio`-based Python crawler is in the range of **5-15 million pages in 24 hours**.

The 50 million page target remains an ambitious stretch goal. Achieving it would likely require:
*   An exceptionally broad and high-quality seed list of domains to sustain the necessary crawl diversity given the 70-second per-domain delay.
*   Favorable network conditions and highly optimized system performance (CPU for parsing, I/O for storage).

The project will proceed with the current architecture, aiming for high performance, and actual throughput will be measured and optimized iteratively. The initial 5-15M pages/24h range serves as a more grounded baseline for success. 

## 13. Database Migration (New Section)

### 13.1. Migrating from SQLite to PostgreSQL

For scaling beyond ~100 workers, migration to PostgreSQL is recommended:

1. **Export SQLite data**: Use provided migration scripts
2. **Create PostgreSQL database**: 
   ```sql
   CREATE DATABASE web_crawler;
   ```
3. **Run schema creation**: The crawler will auto-create tables on first run
4. **Import data**: Use provided import scripts
5. **Update configuration**: Switch `--db-type` to `postgresql` and provide `--db-url`

### 13.2. Database Abstraction

The crawler uses a database abstraction layer that:
- Provides async/await interface for both backends
- Handles connection pooling appropriately for each database
- Translates queries when needed (most SQL is compatible)
- Allows easy switching between backends for testing vs. production 

## 14. Monitoring and Metrics Collection

The crawler includes integrated monitoring capabilities using Prometheus for metrics collection and Grafana for visualization.

### 14.1. Metrics Architecture

*   **Metrics Module (`metrics.py`):**
    *   Uses `prometheus_client` library to expose metrics
    *   Runs an HTTP server on port 8001 for Prometheus to scrape
    *   Provides counters, gauges, and histograms for comprehensive monitoring

*   **Integration with Orchestrator:**
    *   Metrics are updated throughout the crawl lifecycle
    *   Minimal performance overhead with asynchronous updates
    *   Real-time visibility into crawler performance

### 14.2. Available Metrics

*   **Counters (monotonically increasing):**
    *   `crawler_pages_crawled_total`: Total successful page fetches
    *   `crawler_urls_added_total`: Total URLs added to frontier
    *   `crawler_errors_total`: Errors by type (fetch_error, HTTP status codes)

*   **Gauges (point-in-time values):**
    *   `crawler_pages_per_second`: Current crawl rate
    *   `crawler_frontier_size`: Current frontier size
    *   `crawler_active_workers`: Number of active worker tasks
    *   `crawler_memory_usage_bytes`: RSS memory usage
    *   `crawler_open_fds`: Open file descriptors
    *   `crawler_db_pool_available`: Available database connections

*   **Histograms (distributions):**
    *   `crawler_fetch_duration_seconds`: URL fetch time distribution
    *   `crawler_db_connection_acquire_seconds`: Database connection acquisition time
    *   `crawler_db_query_duration_seconds`: Database query execution time by query type