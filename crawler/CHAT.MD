# Project Development Summary: Web Crawler

This document provides a high-level summary of the development process for the experimental web crawler project.

## 1. Planning Phase

*   **Goal Definition**: Established the primary objectives for the web crawler, including running on a single machine, aiming for politeness (respecting `robots.txt`, crawl delays, informative User-Agent), fault tolerance (stop/resume capabilities), storage of text content from crawled pages, and providing a means for progress inspection. Non-goals included handling non-text data or complex dynamic content requiring logins.
*   **Plan Document (`PLAN.MD`)**: Created a comprehensive plan detailing:
    *   **Architecture**: An asynchronous, single-process Python application.
    *   **Core Components**: Identified key modules: Main Controller, Seed Loader, Frontier Manager, Fetcher, Robots.txt Manager, Parser, Storage Manager, and Politeness Enforcer.
    *   **Data Storage**: Defined SQLite for storing state (frontier, visited URLs, domain metadata like `robots.txt` cache and last crawl times) and the file system for storing extracted text content (named by URL hash).
    *   **Politeness**: Outlined mechanisms including `robots.txt` parsing, a 70-second default minimum crawl delay per domain, and handling for a manual exclusion list.
    *   **Fault Tolerance**: Designed for state persistence in SQLite to allow stopping and resuming crawls.
    *   **Directory Structure**: Proposed a modular layout for the codebase (`crawler_module/`) and tests (`tests/`).
    *   **Dependencies**: Listed initial Python libraries (`aiohttp`, `lxml`, `robotexclusionrulesparser`, etc.).
    *   **Diagrams**: Included a Mermaid diagram to visualize the architecture.
    *   **Performance**: Discussed the ambitious target of 50 million pages/24h and later added an addendum acknowledging a more realistic initial target of 5-15M pages/24h based on further discussion and comparisons.

## 2. Initial Setup & Core Module Implementation (Iterative)

*   **Project Structure**: Created the base directory layout (`crawler/`, `crawler_module/`, `tests/`) and foundational files (`requirements.txt`, `.gitignore`, `README.md`, `main.py`).
*   **`crawler_module/config.py`**: Implemented command-line argument parsing using `argparse` and a `CrawlerConfig` dataclass to hold configuration settings (seed file, email, data directory, concurrency limits, etc.).
*   **`crawler_module/utils.py`**: Developed utility functions, primarily `normalize_url` for canonicalizing URLs and `extract_domain` using the `tldextract` library.
*   **`crawler_module/storage.py`**: Built the `StorageManager` class. This component handles:
    *   Initialization of the SQLite database (`crawler_state.db`) and creation of the schema (tables: `frontier`, `visited_urls`, `domain_metadata`, `schema_version`).
    *   Setup of the directory for storing crawled text content (`crawler_data/content/`).
    *   Methods for saving extracted text content to files asynchronously (`aiofiles`).
    *   Methods for adding records of visited pages to the `visited_urls` table.
*   **`crawler_module/fetcher.py`**: Implemented an asynchronous `Fetcher` class using `aiohttp`. This class is responsible for:
    *   Downloading web content (HTML pages, `robots.txt` files).
    *   Managing an `aiohttp.ClientSession` and request headers (including the User-Agent).
    *   Handling HTTP redirects, timeouts, and common connection/response errors.
    *   Attempting character encoding detection (using `cchardet`) and decoding content to text.
    *   Returning a structured `FetchResult` dataclass.
*   **`crawler_module/politeness.py`**: Developed the `PolitenessEnforcer` class. Its responsibilities include:
    *   Loading and parsing `robots.txt` files for domains (using `robotexclusionrulesparser` and the `Fetcher`).
    *   Caching `robots.txt` rules and their expiration in the `domain_metadata` table.
    *   Providing methods to check if a URL is allowed (`is_url_allowed`) and to get the appropriate crawl delay (`get_crawl_delay`), respecting `Crawl-delay` directives or a default (70s).
    *   Checking if a domain can be fetched based on the last attempt time (`can_fetch_domain_now`).
    *   Recording fetch attempts to update `last_scheduled_fetch_timestamp`.
    *   Loading and applying a user-provided list of manually excluded domains.
*   **`crawler_module/frontier.py`**: Created the `FrontierManager` class to manage the queue of URLs to be crawled. This involved:
    *   Loading initial seed URLs from a file.
    *   Adding newly discovered URLs, ensuring normalization and basic validation.
    *   Interacting with `StorageManager` for persisting the frontier in the database.
    *   Integrating with `PolitenessEnforcer` in its `get_next_url` method to select URLs that are allowed by `robots.txt` and whose domains are ready for a new fetch (respecting crawl delays).
    *   Managing an in-memory set of `seen_urls` to optimize duplicate checks within a session.
*   **`crawler_module/parser.py`**: Implemented the `PageParser` class using `lxml` for processing HTML content. This class:
    *   Parses HTML strings.
    *   Extracts hyperlink (`<a>` hrefs) and resolves them to absolute, normalized URLs.
    *   Filters for valid HTTP/HTTPS links.
    *   Extracts a simple version of the page's main text content (using `lxml.html.clean.Cleaner` to remove scripts, styles, comments, etc., then getting `text_content()` from the body).
    *   Extracts the page title.
    *   Returns a `ParseResult` dataclass containing extracted links, text, and title.

## 3. Testing & Debugging Phase (Iterative & Ongoing)

*   **Test Suite Setup**: Migrated initial `if __name__ == "__main__":` test blocks into a dedicated `tests/` directory, using `pytest` as the testing framework. Added `pytest-asyncio` for handling asynchronous test code.
*   **Dependency Management**: Updated `requirements.txt` with necessary libraries as they were introduced or updated (e.g., `lxml[html_clean]` after an `ImportError`).
*   **Import Resolution for Tests**: Created `crawler/conftest.py` to modify `sys.path`, enabling `pytest` to correctly discover and import the `crawler_module` package from the `tests/` directory.
*   **SQLite Threading Issues**: This was a significant debugging effort. Addressed multiple `sqlite3.Error: SQLite objects created in a thread can only be used in that same thread.` errors and related `UnboundLocalError`s. The resolution involved refactoring synchronous database operations within modules like `FrontierManager`, `PolitenessEnforcer`, and `StorageManager` (specifically, methods called via `asyncio.to_thread`). These synchronous functions were modified to establish their own `sqlite3` connections when executed within a new thread, rather than relying on a shared connection object created in the main thread.
*   **Cursor Management**: Corrected `TypeError: 'sqlite3.Cursor' object does not support the context manager protocol` by ensuring explicit `cursor.close()` calls or correct usage of connection context managers for synchronous DB helper functions.
*   **Parser Logic & Test Refinement**: Debugged `PageParser` tests related to:
    *   Extraction of non-HTTP/HTTPS links (e.g., `mailto:`, `javascript:`), refining parser logic to filter these out correctly.
    *   Text extraction from HTML lacking a `<body>` tag or from malformed HTML, adjusting parser logic and test expectations for consistency.
    *   Ensuring the HTML `Cleaner` was correctly applied to remove script content before text extraction.
*   **PolitenessEnforcer Test Refinement**: Debugged and improved tests for `PolitenessEnforcer`:
    *   Corrected mocking of `Path.exists()`.
    *   Refactored `test_load_manual_exclusions_with_file` to directly verify database content after `PolitenessEnforcer` initialization.
    *   Addressed `AttributeError` for `db_path` on mocked `StorageManager` by ensuring the mock provided the attribute.
    *   Resolved `UnboundLocalError` for `db_row` in `_get_robots_for_domain`.
    *   Refined `robots.txt` parsing logic and caching within `_get_robots_for_domain`.
    *   Updated tests that check DB interactions (like `test_get_robots_from_db_cache_fresh`) to use `unittest.mock.patch` on `sqlite3.connect` for more controlled unit testing of methods that internally create DB connections in threads. This helped isolate user-agent matching issues for `robots.txt` rules.

## 4. Orchestration & Main Entry Point

*   **`crawler_module/orchestrator.py`**: Implemented the `CrawlerOrchestrator` class. This class:
    *   Initializes all other core components (`StorageManager`, `Fetcher`, `PolitenessEnforcer`, `FrontierManager`, `PageParser`).
    *   Manages an `asyncio.Event` for graceful shutdown signaling.
    *   Contains the `async _worker` method defining the lifecycle of a single crawl task: get URL from frontier, fetch, parse (if HTML), save content, record visit, add new links to frontier. All I/O-bound operations are `async` or delegated to threads (`asyncio.to_thread` for synchronous DB calls).
    *   Implements the `async run_crawl` method, which starts and manages a pool of worker tasks, monitors progress and stopping conditions (max pages, duration, empty frontier), and handles graceful shutdown of workers and cleanup of resources (closing fetcher session, DB connection).
*   **`main.py`**: Updated the main script to:
    *   Parse command-line arguments via `config.parse_args()`.
    *   Configure logging based on the parsed configuration.
    *   Instantiate and run the `CrawlerOrchestrator`.
    *   Include basic signal handling (`SIGINT`, `SIGTERM`) to trigger a graceful shutdown of the orchestrator.

This iterative process of planning, implementing features module by module, writing tests, and debugging issues (especially around asynchronous operations and database threading) has brought the project to a runnable state, ready for further testing and refinement.

## 5. Current Known Test Failures & Debugging Notes (as of last update)

Further test runs have revealed a new set of failures. These are currently being investigated:

*   **`tests/test_politeness.py`**: Multiple tests are failing with `NameError: name 'AsyncMock' is not defined`. This suggests `from unittest.mock import AsyncMock` might be missing or incorrectly scoped in these specific test functions or the test file itself, where `AsyncMock` is used to mock asynchronous methods of `PolitenessEnforcer` dependencies (like `_get_robots_for_domain` in some tests for `get_crawl_delay` or `can_fetch_domain_now`).
*   **`tests/test_politeness.py`**: Several assertion errors like `assert 0 > 0` or `assert 0 == 2` (e.g., in `test_get_robots_db_cache_stale_then_fetch_success`, `test_record_domain_fetch_attempt_new_domain`). These often indicate that mocked method call counts (`mock_cursor.execute.call_count` or `politeness_enforcer.fetcher.fetch_url.call_count`) are not matching expectations. This could be due to changes in the underlying DB interaction logic (how many times `execute` is now called by threaded helpers) or how mocks are set up/reset for methods that are called multiple times.
*   **`tests/test_politeness.py::test_get_robots_http_404_https_404`**: Failing with `AssertionError: assert 1 == 2`. This points to an incorrect `call_count` for `politeness_enforcer.fetcher.fetch_url` (expected 2 calls, got 1).
*   **`tests/test_politeness.py::test_is_url_allowed_manually_excluded`**: Failing with `AssertionError: assert True is False`. This test expects `is_url_allowed` to return `False` for a manually excluded domain. The failure means it's returning `True`. This could be an issue with how the manual exclusion is checked in `is_url_allowed` or how the DB state is mocked/verified for that test.
*   **`tests/test_utils.py`**: Several `AssertionError` failures in `test_normalize_url` related to trailing slashes (e.g., `assert 'https://example.com:80/' == 'https://example.com:80'`) and one in `test_extract_domain` for an FTP URL (`assert None == 'example.org'`). These indicate that the `normalize_url` and `extract_domain` functions are not producing the exact expected output for these specific test cases, likely around handling of trailing slashes or schemes other than http/https.

Addressing these failures will be the next focus. 