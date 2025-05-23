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

The crawler will be an asynchronous, single-process application.

### 2.1. Core Components

*   **Main Controller:** Initializes and orchestrates the crawl. Manages configuration.
*   **Seed Loader:** Reads the initial list of domains/URLs from the seed file.
*   **Frontier Manager:**
    *   Manages the queue of URLs to be crawled (the "frontier").
    *   Prioritizes URLs based on domain politeness rules (e.g., 70-second delay per domain).
    *   Persists the frontier for fault tolerance.
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
    *   **State Storage (SQLite):** Persists the frontier, set of visited URLs, domain last-crawl timestamps, `robots.txt` cache, and manually excluded domains.
    *   **Content Storage (File System):** Stores extracted text content in files.
*   **Politeness Enforcer:**
    *   Ensures `robots.txt` rules are followed.
    *   Enforces the 70-second delay between crawls of the same domain.
    *   Manages the user-provided exclusion list.

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
        Fetcher[Fetcher (aiohttp)]
        RobotsMgr[Robots.txt Manager]
        Parser[Parser (BeautifulSoup/lxml)]
        Storage[Storage Manager]
        Politeness[Politeness Enforcer]
    end

    subgraph Data Stores
        SQLiteDB[SQLite Database (crawler_state.db)]
        ContentFS[File System (crawler_data/content/)]
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

    SQLiteDB -.-> Main  # For resumption
    ContentFS -.-> User # For inspection via external tools
    SQLiteDB -.-> User  # For inspection via external tools
```

## 3. Key Data Structures and Storage

### 3.1. Configuration

*   **Seed File:** Path to a newline-separated list of initial domains/URLs.
*   **Contact Email:** Required for the User-Agent string.
*   **Exclude File (Optional):** Path to a newline-separated list of domains to exclude.
*   **Data Directory:** Base directory for storing crawled content and the SQLite database.
*   **Max Concurrent Requests:** Number of simultaneous fetch operations.
*   **Target Page Count / Duration:** Optional limits for the crawl.

### 3.2. State Storage (SQLite Database - `crawler_state.db`)

*   **`frontier` table:**
    *   `id` INTEGER PRIMARY KEY AUTOINCREMENT
    *   `url` TEXT UNIQUE NOT NULL
    *   `domain` TEXT NOT NULL
    *   `depth` INTEGER DEFAULT 0 (optional, for crawl depth limiting)
    *   `added_timestamp` INTEGER NOT NULL
    *   `priority_score` REAL DEFAULT 0 (can be used for scheduling, e.g., based on domain crawl delay)
    *   *Indexes: `(domain)`, `(priority_score, added_timestamp)`*
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

\`\`\`
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
\`\`\`

## 8. Dependencies

*   **Python 3.8+**
*   **`aiohttp`:** For asynchronous HTTP requests.
*   **`beautifulsoup4` or `lxml`:** For HTML parsing. (lxml is generally faster)
*   **`sqlite3`:** (Python standard library) For state persistence.
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

\`\`\`bash
python main.py --seed-file path/to/seeds.txt --email your.email@example.com [options]
\`\`\`

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
*   `--resume`: Flag to attempt resuming from existing data directory. If not set and data dir exists, it might ask or error. (Or, by default, resume if data dir and DB exist).

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

## 11. Educational Aspects & Simplicity

*   **Modular Design:** Code will be organized into logical modules for clarity.
*   **Clear Naming:** Variables and functions will be named descriptively.
*   **Focused Scope:** Avoids advanced features like distributed crawling, JavaScript rendering, or complex anti-bot measure bypasses.
*   **Step-by-Step Implementation:** The project can be built and tested incrementally (e.g., fetcher first, then parser, then storage, etc.).
*   **Direct Inspection:** Using SQLite and flat text files allows for easy examination of the crawler's state and output. 