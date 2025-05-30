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
*   `main.py`: The main entry point to run the crawler.

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
    Key dependencies include: `aiohttp`, `lxml`, `robotexclusionrulesparser`, `aiofiles`, `cchardet`, `tldextract`, `pytest`, `pytest-asyncio`, `httpx`.

## Running the Crawler

The crawler is run via `main.py` from the project root directory (e.g., `vibe-coding-experiments/` if `crawler` is inside it, or directly from `crawler/` if you `cd` into it first):

```bash
python crawler/main.py --seed-file path/to/your/seeds.txt --email your.email@example.com [options]
```
Or if you are inside the `crawler` directory:
```bash
python main.py --seed-file path/to/your/seeds.txt --email your.email@example.com [options]
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

## Progress Inspection

*   **Logs:** Check console output or redirect to a file. Log level is configurable.
*   **Database (`crawler_data/crawler_state.db` within your specified data directory):** Use any SQLite client (e.g., `sqlite3` CLI, DB Browser for SQLite) to inspect tables like `frontier`, `visited_urls`, `domain_metadata`.
*   **Content Files (`crawler_data/content/` within your specified data directory):** Extracted text content is stored in `.txt` files named by URL hash. 