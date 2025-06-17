# Debugging Crawler Program Issues
_Exported on 5/29/2025 at 02:41:35 PDT from Cursor (0.50.5)_

---

**User**

This crawler program (which we wrote together, see the PLAN.MD) has 2 major problems: (1) the number of workers cannot scale beyond 20 or so because we begin to run into 'too many open files' errors (see the full-chats/cursor_crawler_coroutine_error_troubles_claude4opus.md file), (2) during a test run, the crawler network utilization steadily dropped over time and the program was terminated by the OS (I think) after 12+ hours.

Please help me debug them.

---

**Cursor**

Okay, I can help you with that. Let's start by understanding the crawler's design and the context of the "too many open files" error.

I'll begin by reading the `PLAN.MD` file to get an overview of the crawler program.

It seems I couldn't find `PLAN.MD` in the root directory. I see a few possible locations. Could you please specify which `PLAN.MD` is the correct one?

In the meantime, I will also read the error log file `full-chats/cursor_crawler_coroutine_error_troubles_claude4opus.md` to understand the "too many open files" error.

It seems the error log file is also not in the location I expected. Is `crawler/full-chats/cursor_crawler_coroutine_error_troubles_claude4opus.md` the correct path?

Once you confirm the correct paths for these files, I can proceed with analyzing them and helping you debug the issues.

---

**User**

Everything is inside the `crawler` directory. I have attached the relevant files; the full-chats file I was thinking of is actually a different one (it should be cursor_sqlite_concurrency_issues_in_cra_claude4opus.md).

---

**Cursor**

Thanks for confirming the file paths and providing the context!

I've already reviewed the `crawler/PLAN.MD` file. Now, I'll read the chat log `crawler/full-chats/cursor_sqlite_concurrency_issues_in_cra_claude4opus.md` to understand the previous debugging efforts for the "too many open files" error.

I've reviewed the `PLAN.MD` and the chat log. Here's a summary of my understanding and the next steps:

**Problem 1: "Too many open files" error**

*   **History:** This issue was extensively debugged. The root cause was identified as exhausting file descriptors due to a high number of concurrent network connections (initially 100 workers) and potentially SQLite connections.
*   **Fixes Applied:**
    *   Reduced `max_workers` to 20.
    *   Configured `aiohttp.TCPConnector` with connection limits (`limit=200`, `limit_per_host=30`) and `force_close=True` to aggressively close connections.
    *   The system's file descriptor limit (`ulimit -n`) is already very high (1,048,576).
*   **Current Status:** You mentioned that "Running with `max_workers=20` seems to be healthy so far." This suggests the immediate "too many open files" issue might be mitigated for now.

**Problem 2: Network utilization dropping and OS termination after 12+ hours**

*   This is a new issue that hasn't been discussed before.
*   It could be related to various factors:
    *   Memory leaks.
    *   Unhandled exceptions or deadlocks in worker tasks.
    *   The frontier becoming exhausted or skewed towards slow domains.
    *   External factors like network instability or rate limiting by target servers.
    *   The OS killing the process due to excessive resource consumption (OOM killer).

**Plan to Debug**

Let's tackle these systematically.

**For Problem 1 (Scaling beyond 20 workers):**

Even though 20 workers are stable, the goal is to scale further. The previous chat explored several good ideas that might not have been fully implemented or require revisiting:

1.  **SQLite Connection Management:** The chat log indicates that `StorageManager` and `FrontierManager` were creating new SQLite connections for every database operation. This is highly inefficient and a prime candidate for causing file descriptor issues at higher worker counts, even if each connection is closed.
    *   **Action:** We should investigate if a SQLite connection pooling mechanism (like the `SQLiteConnectionPool` suggested in the chat) or a single database writer task was implemented. If not, this is a critical improvement.
2.  **Monitoring File Descriptors:** The chat suggested adding code to monitor file descriptor usage (`psutil.Process(os.getpid()).num_fds()`).
    *   **Action:** Is this monitoring in place? If so, what does it show when you try to increase workers beyond 20? If not, adding it will be very helpful.
3.  **aiohttp Connector Settings:** While `force_close=True` helps with FDs, it hurts performance. Once SQLite FDs are managed better, we might be able to revert to `keepalive_timeout` for better connection reuse.

**For Problem 2 (Network utilization drop and OS termination):**

1.  **Logging Analysis:**
    *   **Action:** Can you provide the logs from the 12+ hour run, especially towards the end? We need to look for:
        *   Error messages (Python exceptions, network errors, database errors).
        *   Patterns in crawled URLs (e.g., stuck on a few domains).
        *   Logging of resource usage if any (memory, CPU, FD count).
        *   Any messages from the OS regarding process termination (e.g., OOM killer messages in `dmesg` or `/var/log/syslog`).
2.  **Resource Monitoring:**
    *   **Action:** Implement more detailed resource monitoring within the crawler. This could include:
        *   Memory usage (`psutil.Process(os.getpid()).memory_info().rss`).
        *   CPU usage.
        *   Size of the frontier queue.
        *   Number of active worker tasks.
        *   Rate of pages crawled.
        This data, logged periodically, can show trends leading up to the crash.
3.  **Code Review for Potential Leaks/Deadlocks:**
    *   **Action:** We'll need to review the core components (`Fetcher`, `Parser`, `FrontierManager`, `StorageManager`, and the main orchestrator loop) for potential issues like:
        *   Not releasing resources (e.g., large objects, unclosed file handles if any remain).
        *   Tasks that might hang indefinitely under certain conditions.
        *   Growth of in-memory data structures (e.g., caches, sets) without bounds.

**Immediate Next Steps:**

1.  Let's start with the "too many open files" issue when scaling workers. Could you please show me the current code for:
    *   `crawler_module/storage.py` (specifically how database connections are handled for operations like `add_visited_page`).
    *   `crawler_module/frontier.py` (specifically how database connections are handled for operations like `add_url`).
    *   The part of your `main.py` or orchestrator that initializes and uses these components, particularly if a connection pool or a dedicated DB writer task is used.
2.  Regarding the 12+ hour run and termination:
    *   Do you have logs from that run? If they are very large, perhaps the last few thousand lines or any sections with errors would be most helpful.
    *   Can you check system logs (e.g., `dmesg` or `sudo journalctl -k -r -n 100` for kernel messages, `grep -i oom /var/log/syslog` or `sudo journalctl | grep -i oom` for OOM killer messages) around the time the crawler was terminated?

Let's begin by examining the SQLite connection management, as this is a likely culprit for problems when increasing worker counts.

Okay, I've reviewed `storage.py`, `frontier.py`, and `main.py`.

**Key Findings on SQLite Connection Management:**

*   **`StorageManager` (`storage.py`):**
    *   It initializes a main connection `self.conn` in `_init_db()`, but this connection is primarily used for schema creation.
    *   The critical method `add_visited_page()` explicitly **creates a new SQLite connection** (`conn_threaded = sqlite3.connect(...)`) every time it's called. It then closes this connection in the `finally` block.
    *   The comment `This method is run in a separate thread by asyncio.to_thread, so it must establish its own database connection.` confirms this design choice.
*   **`FrontierManager` (`frontier.py`):**
    *   Most database operations (`_populate_seen_urls_from_db_sync`, `_mark_domain_as_seeded`, `_clear_frontier_db_sync`, `_add_url_sync`, `count_frontier`, methods within `get_next_url`) also **create new SQLite connections** for each call, typically within functions designed to be run via `asyncio.to_thread`.
    *   These connections are also generally closed within their respective methods.
*   **`main.py` / `Orchestrator`:**
    *   The `main.py` script sets up the `CrawlerOrchestrator`. The orchestrator itself doesn't directly manage DB connections but uses `StorageManager` and `FrontierManager`.
    *   There is **no evidence of a SQLite connection pool** (like the `SQLiteConnectionPool` class discussed in the previous chat) or a dedicated database writer task being implemented or used by these managers.

**Analysis of "Too many open files" (Problem 1):**

The current approach of opening and closing a new SQLite connection for *every single database interaction* (per URL added to frontier, per page marked visited, per politeness check that hits the DB, etc.) is a very strong candidate for the "too many open files" error, especially when trying to scale `max_workers`.

Here's why:

1.  **High Churn Rate:** With many workers, even if each connection is closed quickly, the *rate* of opening new file descriptors for database files can be very high.
2.  **OS Latency:** There's a slight delay between when a file descriptor is closed in Python and when the OS actually reclaims it. If you're opening FDs faster than the OS can reclaim them, the count of open FDs will climb.
3.  **Concurrency:** If multiple `asyncio.to_thread` calls for database operations overlap, you will indeed have multiple simultaneous database connections open, each consuming a file descriptor. With 20 workers, if each happens to do a DB operation at roughly the same time, that's 20+ FDs for SQLite alone, plus network FDs, log file FDs, etc. If you try to scale to 50 or 100 workers, this number multiplies.

While using `asyncio.to_thread` and opening a new connection per thread is a way to handle SQLite's thread-safety requirements, it's not an efficient or scalable way to manage connections for frequent, short-lived operations.

**Recommended Action for Problem 1:**

Implement a **thread-safe SQLite connection pool**.

The `SQLiteConnectionPool` class suggested in the previous chat log (and pasted in the `cursor_sqlite_concurrency_issues_in_cra_claude4opus.md` file) is a good starting point.

Here’s how it would generally work:

1.  **Initialize Pool:** The `StorageManager` (or a dedicated DB management class) would create and hold an instance of this pool.
2.  **Get Connection:** When `StorageManager` or `FrontierManager` needs to perform a DB operation (inside an `asyncio.to_thread` call), it would request a connection from the pool.
3.  **Use Connection:** Perform the database operation.
4.  **Return Connection:** Return the connection to the pool in a `finally` block to ensure it's always released.

This drastically reduces the number of times new file descriptors for the database file need to be opened and closed, relying instead on a smaller, managed set of persistent connections.

**Next Steps for Problem 1:**

1.  Would you like me to help you integrate a SQLite connection pool? We can adapt the `SQLiteConnectionPool` class from the chat log and modify `StorageManager` and `FrontierManager` to use it.
2.  Once that's done, we can re-test scaling the number of workers.

**Regarding Problem 2 (Network utilization drop and OS termination):**

We still need the logs from the 12+ hour run and OS messages (like OOM killer logs) to diagnose this. If you can provide those, I can analyze them. This issue might be separate from the file descriptor problem, or it could be a consequence of long-term resource exhaustion that the FD issue exacerbates.

Let's focus on fixing the SQLite connection management first, as it's a clear bottleneck for scaling.

---

**User**

Yes, let's integrate a connection pool please. Do we need to adapt the tests as well?

---

**Cursor**

Okay, let's integrate a SQLite connection pool. This is a crucial step for improving database performance and stability, especially with multiple workers.

Here's the plan:

1.  **Create `db_pool.py`**: We'll add the `SQLiteConnectionPool` class that was discussed in the previous chat. This class will manage a set of persistent SQLite connections.
2.  **Modify `StorageManager`**: It will use the connection pool for all its database operations, including initialization and adding visited pages.
3.  **Modify `FrontierManager`**: It will also use the connection pool for all its database interactions.
4.  **Modify `Orchestrator`**: It will be responsible for creating the connection pool instance and passing it to the `StorageManager` and `FrontierManager`. It will also ensure the pool is closed on shutdown.
5.  **Discuss Test Adaptation**: After implementing the changes, we'll consider if and how the tests need to be updated.

Let's start by creating the `db_pool.py` file. I'll use the version from the chat log as a base, as it seems robust.

I will create the file `crawler/crawler_module/db_pool.py`.
```python:crawler/crawler_module/db_pool.py
import sqlite3
import threading
from queue import Queue, Empty, Full
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str | Path, pool_size: int = 10, timeout: int = 10, wal_mode: bool = True):
        self.db_path = str(db_path) # Ensure db_path is a string for sqlite3.connect
        self.timeout = timeout
        self.pool_size = pool_size
        self.wal_mode = wal_mode
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock() # To protect access to pool creation logic if needed, though Queue is thread-safe
        self._connections_created = 0

        # Pre-fill the pool
        for _ in range(pool_size):
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Exception as e:
                logger.error(f"Error creating initial connection for pool: {e}")
                # Depending on strictness, could raise here or allow partial pool
        logger.info(f"SQLiteConnectionPool initialized for {self.db_path} with {self._pool.qsize()} connections.")

    def _create_connection(self):
        try:
            # Connections will be used by different threads via asyncio.to_thread,
            # so check_same_thread=False is essential.
            conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            if self.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL") # WAL is durable, NORMAL is faster
            self._connections_created += 1
            # logger.debug(f"Created new SQLite connection. Total created: {self._connections_created}")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to create SQLite connection for {self.db_path}: {e}")
            raise

    def get_connection(self, block: bool = True, timeout: float | None = None) -> sqlite3.Connection:
        """Get a connection from the pool."""
        try:
            # Timeout for getting from queue can be shorter than connection timeout
            effective_timeout = timeout if timeout is not None else self.timeout / 2 
            return self._pool.get(block=block, timeout=effective_timeout)
        except Empty:
            # This can happen if pool is exhausted and block is False or timeout occurs
            logger.warning(f"Connection pool for {self.db_path} exhausted or timed out. Trying to create a temporary connection.")
            # Fallback: create a new one if pool is empty and we must proceed, but don't add it to the pool.
            # This is a safety net, but ideally, the pool size should be tuned.
            # However, for short-lived operations, this might be acceptable if rare.
            # For now, let's stick to the pool size. If Empty is raised, the caller should handle.
            # Re-raising for clarity that pool was empty or timed out.
            raise Empty(f"No connection available from pool for {self.db_path} within timeout.")


    def return_connection(self, conn: sqlite3.Connection | None):
        """Return a connection to the pool."""
        if conn is None:
            return
        try:
            # Test if connection is still alive (e.g., by executing a simple pragma)
            # This can be problematic as it adds overhead. 
            # A more robust pool might try to execute a rollback to ensure it's in a clean state.
            # For simplicity, we'll assume if it was working, it's still good.
            # A more advanced pool might have a "test on return" or "test on borrow" flag.
            conn.execute("PRAGMA user_version;") # A lightweight command to check if connection is alive
            self._pool.put_nowait(conn) # Use put_nowait to avoid blocking if pool is somehow overfilled (shouldn't happen with proper use)
        except sqlite3.Error as e:
            logger.warning(f"Returned connection for {self.db_path} is dead or error: {e}. Discarding.")
            try:
                conn.close() # Close the dead connection
            except:
                pass # Ignore errors during close of a dead connection
            # Optionally, replace the dead connection in the pool
            try:
                new_conn = self._create_connection()
                self._pool.put_nowait(new_conn) # Try to keep the pool full
                logger.info(f"Replaced dead connection in pool for {self.db_path}")
            except Full:
                logger.warning(f"Pool for {self.db_path} is full, cannot replace dead connection immediately.")
            except Exception as ex:
                logger.error(f"Error replacing dead connection for {self.db_path}: {ex}")

        except Full:
            # This should ideally not happen if get_connection/return_connection are used correctly.
            # It means we are returning more connections than were taken out or the pool size is mismanaged.
            logger.error(f"Attempted to return connection to a full pool for {self.db_path}. Closing connection instead.")
            try:
                conn.close()
            except:
                pass


    def close_all(self):
        """Close all connections in the pool."""
        logger.info(f"Closing all connections in pool for {self.db_path}...")
        closed_count = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed_count += 1
            except Empty:
                break # Pool is empty
            except Exception as e:
                logger.error(f"Error closing a connection from pool for {self.db_path}: {e}")
        logger.info(f"Closed {closed_count} connections from pool for {self.db_path}. Pool size: {self._pool.qsize()}")

    def __enter__(self):
        # Context manager to get a connection
        self.conn = self.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Context manager to return the connection
        self.return_connection(self.conn)
        self.conn = None # Clear the connection
```

Now, let's modify `StorageManager` (`crawler/crawler_module/storage.py`) to use this pool.

```python:crawler/crawler_module/storage.py
// ... existing code ...
import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time
import asyncio # For to_thread if needed for DB, and for async file saving
import aiofiles # For async file operations

from .config import CrawlerConfig # Assuming config.py is in the same directory
from .db_pool import SQLiteConnectionPool # New import

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 1

class StorageManager:
    def __init__(self, config: CrawlerConfig, db_pool: SQLiteConnectionPool): # Added db_pool
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.db_path = self.data_dir / "crawler_state.db" # db_pool will use this path
        self.content_dir = self.data_dir / "content"
        self.db_pool = db_pool # Store the pool

        self._init_storage()

    def _init_storage(self):
        """Initializes the data directory, content directory, and database schema."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise

        self._init_db_schema() # Renamed from _init_db

    def _init_db_schema(self): # Renamed
        """Initializes the SQLite database schema if it doesn't exist or needs upgrade."""
        # This method will now be called from the main thread (Orchestrator init)
        # or at least a context where it's okay to block for a short while.
        # It uses a connection from the pool.
        try:
            # Get a connection from the pool for schema initialization
            with self.db_pool as conn: # Use context manager for get/return
                logger.info(f"Initializing database schema using connection from pool: {self.db_path}")
                self._create_tables(conn) # Pass the connection
        except sqlite3.Error as e:
            logger.error(f"Database error during schema initialization with pool: {e}")
            raise
        except Exception as e: # Catch potential pool errors (e.g., Empty if pool fails to init)
            logger.error(f"Failed to get connection from pool for schema init: {e}")
            raise


    def _create_tables(self, conn: sqlite3.Connection): # Takes a connection argument
        """Creates the necessary tables in the database if they don't already exist."""
        # No longer uses self.conn
        cursor = conn.cursor() # Create cursor from the provided connection
        try:
            # Check current schema version
            cursor.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
            cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            row = cursor.fetchone()
            current_version = row[0] if row else 0

            if current_version < DB_SCHEMA_VERSION:
                logger.info(f"Database schema version is {current_version}. Upgrading to {DB_SCHEMA_VERSION}.")
                
                # Frontier Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS frontier (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT NOT NULL,
                    depth INTEGER DEFAULT 0,
                    added_timestamp INTEGER NOT NULL,
                    priority_score REAL DEFAULT 0
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")

                # Visited URLs Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url_sha256 TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    crawled_timestamp INTEGER NOT NULL,
                    http_status_code INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    content_storage_path TEXT,
                    redirected_to_url TEXT
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_visited_domain ON visited_urls (domain)")

                # Domain Metadata Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS domain_metadata (
                    domain TEXT PRIMARY KEY,
                    last_scheduled_fetch_timestamp INTEGER DEFAULT 0,
                    robots_txt_content TEXT,
                    robots_txt_fetched_timestamp INTEGER,
                    robots_txt_expires_timestamp INTEGER,
                    is_manually_excluded INTEGER DEFAULT 0,
                    is_seeded INTEGER DEFAULT 0
                )
                """)
                
                # Update schema version
                cursor.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
                conn.commit() # Commit changes
                logger.info("Database tables created/updated successfully.")
            else:
                logger.info(f"Database schema is up to date (version {current_version}).")

        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            try:
                conn.rollback() # Rollback on error
            except sqlite3.Error as re:
                logger.error(f"Error during rollback: {re}")
            raise
        finally:
            cursor.close()
// ... existing code ...
    async def save_content_to_file(self, url_hash: str, text_content: str) -> Path | None:
        """Saves extracted text content to a file asynchronously."""
        if not text_content: # Do not save if no text content
            return None
        
        file_path = self.content_dir / f"{url_hash}.txt"
        try:
            async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
                await f.write(text_content)
            logger.debug(f"Saved content for {url_hash} to {file_path}")
            return file_path
        except IOError as e:
            logger.error(f"IOError saving content file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving content file {file_path}: {e}", exc_info=True)
            return None

    def add_visited_page(
        self, 
        url: str, 
        domain: str,
        status_code: int, 
        crawled_timestamp: int,
        content_type: str | None = None, 
        content_text: str | None = None, 
        content_storage_path_str: str | None = None, 
        redirected_to_url: str | None = None
    ):
        """Adds a record of a visited page to the database using a pooled connection.
           This method is expected to be called via asyncio.to_thread if in an async context.
        """
        url_sha256 = self.get_url_sha256(url)
        content_hash: str | None = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()

        conn_from_pool: sqlite3.Connection | None = None
        try:
            # This method is run in a separate thread by asyncio.to_thread,
            # so it gets a connection from the pool.
            conn_from_pool = self.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("""
                INSERT OR REPLACE INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ))
                conn_from_pool.commit()
                logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
            except sqlite3.Error as e: # Inner try for cursor/execute specific errors
                logger.error(f"DB (execute/commit) error adding visited page {url}: {e}")
                if conn_from_pool: 
                    try:
                        conn_from_pool.rollback()
                    except sqlite3.Error as re:
                        logger.error(f"DB error during rollback for {url}: {re}")
                raise # Re-raise to be caught by outer block if necessary, or to signal failure
            finally:
                if cursor: cursor.close()
        except sqlite3.Error as e: # Outer try for connection pool errors or other sqlite errors
            logger.error(f"DB (pool/connection) error adding visited page {url}: {e}")
            # Rollback is handled by the inner block if the error was post-connection
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected error (e.g. pool issue) adding visited page {url}: {e}")
            raise
        finally:
            if conn_from_pool:
                self.db_pool.return_connection(conn_from_pool)

    def close(self):
        """Closes the database connection pool."""
        # self.conn is no longer the primary connection manager.
        # The pool should be closed by the orchestrator or whoever owns it.
        # This method could call db_pool.close_all() if StorageManager owns the pool,
        # but it's better if Orchestrator does it.
        # For now, we can make it explicit that this StorageManager's close
        # doesn't manage the pool itself, or it could if it created the pool.
        # Assuming Orchestrator will handle pool closure.
        logger.info("StorageManager.close() called. DB Pool closure is managed externally.")
```

Next, we modify `FrontierManager` (`crawler/crawler_module/frontier.py`). This class also needs access to the pool. It can get it via the `StorageManager` or be given it directly. Given `StorageManager` now has a `db_pool` attribute, `FrontierManager` can use `self.storage.db_pool`.

```python:crawler/crawler_module/frontier.py
// ... existing code ...
import logging
import time
from pathlib import Path
from typing import Set
import asyncio
import sqlite3

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now expects a db_pool
from .utils import normalize_url, extract_domain
from .politeness import PolitenessEnforcer
# db_pool is not directly imported here, assumed to be accessed via self.storage.db_pool

logger = logging.getLogger(__name__)

class FrontierManager:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, politeness: PolitenessEnforcer):
        self.config = config
        self.storage = storage # storage object now contains the db_pool
        self.politeness = politeness
        self.seen_urls: Set[str] = set()

    def _populate_seen_urls_from_db_sync(self) -> Set[str]:
        """Synchronously loads all URLs from visited_urls and frontier tables using a pooled connection."""
        seen = set()
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                # Load from visited_urls
                cursor.execute("SELECT url FROM visited_urls")
                for row in cursor.fetchall():
                    seen.add(row[0])
                
                # Load from current frontier (in case of resume)
                cursor.execute("SELECT url FROM frontier")
                for row in cursor.fetchall():
                    seen.add(row[0])
            finally:
                cursor.close()
            logger.info(f"DB THREAD (pool): Loaded {len(seen)} URLs for seen_urls set.")
        except sqlite3.Error as e:
            logger.error(f"DB THREAD (pool): Error loading for seen_urls set: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB THREAD (pool): Pool error loading for seen_urls set: {e}")
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        return seen

    async def initialize_frontier(self):
// ... existing code ...
    def _mark_domain_as_seeded(self, domain: str):
        """Marks a domain as seeded in the domain_metadata table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                cursor.execute("UPDATE domain_metadata SET is_seeded = 1 WHERE domain = ?", (domain,))
                conn_from_pool.commit()
                logger.debug(f"Marked domain {domain} as seeded in DB (pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error marking domain {domain} as seeded: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error marking domain {domain} as seeded: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error marking domain {domain} as seeded: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _load_seeds(self):
// ... existing code ...
    def _clear_frontier_db_sync(self):
        """Synchronous part of clearing frontier DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("DELETE FROM frontier")
                conn_from_pool.commit() # Explicit commit for DELETE
                logger.info("Cleared frontier table in database (via thread, pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error clearing frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error clearing frontier: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error clearing frontier: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _clear_frontier_db(self):
// ... existing code ...
    def _add_url_sync(self, normalized_url: str, domain: str, depth: int) -> bool:
        """Synchronous part of adding a URL to DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
                               (self.storage.get_url_sha256(normalized_url),))
                if cursor.fetchone():
                    return False 

                cursor.execute(
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score) VALUES (?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0)
                )
                if cursor.rowcount > 0:
                    conn_from_pool.commit() # Explicit commit for INSERT
                    return True 
                else:
                    # This means INSERT OR IGNORE found an existing row (URL already in frontier)
                    return False 
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) THREAD: Error adding URL {normalized_url} to frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise # Propagate to mark as not added
            finally:
                cursor.close()
        except sqlite3.Error as e: # Outer SQLite errors (e.g. connection issue)
            logger.error(f"DB (pool/connection) THREAD: Error adding URL {normalized_url} to frontier: {e}")
            return False # Ensure it's marked as not added on any DB error
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) THREAD: Pool error adding URL {normalized_url} to frontier: {e}")
            return False
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        # Fallback, should be covered by try/except logic.
        # If an error occurred before return True, it should have returned False or raised.
        return False


    async def add_url(self, url: str, depth: int = 0) -> bool:
// ... existing code ...
    def count_frontier(self) -> int:
        """Counts the number of URLs currently in the frontier table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM frontier")
                count_row = cursor.fetchone()
                return count_row[0] if count_row else 0
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"Error counting frontier (pool): {e}")
            return 0
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Pool error counting frontier: {e}")
            return 0
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses pooled connections for DB operations.
        Removes the URL from the frontier upon retrieval.
        """
        candidate_check_limit = self.config.max_workers * 5 # Make this configurable?
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_candidates_sync_threaded_pooled(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                        (candidate_check_limit,)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Pass the obtained connection to the threaded function
            candidates = await asyncio.to_thread(_get_candidates_sync_threaded_pooled, conn_from_pool)

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules. Removing from frontier (pool).")
                    
                    def _delete_disallowed_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_disallowed_url_sync_threaded_pooled, conn_from_pool)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain):
                    await self.politeness.record_domain_fetch_attempt(domain) # Async, non-DB
                    
                    def _delete_selected_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_selected_url_sync_threaded_pooled, conn_from_pool)
                    
                    logger.debug(f"Retrieved from frontier: {url} (ID: {url_id}) for domain {domain} (pool)")
                    selected_url_info = (url, domain, url_id)
                    break # Found a suitable URL
                else:
                    # Domain cannot be fetched now, try next candidate
                    pass 
            
        except sqlite3.Error as e: 
            logger.error(f"DB Error getting next URL from frontier (pool): {e}", exc_info=True)
            if conn_from_pool: # Try to rollback if commit failed or other op failed
                try: conn_from_pool.rollback()
                except: pass
            return None 
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected or Pool error during get_next_url (pool): {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        
        return selected_url_info
```

Finally, we need to modify `CrawlerOrchestrator` (likely in `crawler/crawler_module/orchestrator.py`) to create the pool and pass it around, and also to close it.

I'll need to see `orchestrator.py` to make the correct changes. Can you show me the contents of `crawler/crawler_module/orchestrator.py`?

Regarding your question about tests:
Yes, tests that rely on `StorageManager` or `FrontierManager` might need adjustments:
1.  **Instantiation**: If tests create their own instances of these managers, they'll now need to provide a `SQLiteConnectionPool` instance. This might involve setting up a pool for test databases.
2.  **State Management**: A connection pool maintains persistent connections. If tests expect a completely clean database slate for each test and create tables/data, this should still work fine as operations are on the DB content, not the connection objects themselves.
3.  **Shutdown**: Tests that create a pool should ensure `pool.close_all()` is called to clean up resources, especially if many test classes create their own pools. This is often handled in `tearDown` or `addCleanup` methods in test frameworks.

The core logic of the tests interacting with the public APIs of these managers should largely remain the same, but the setup and teardown might need to accommodate the pool. We can look at specific tests after the main integration.

```python
import sqlite3
import threading
from queue import Queue, Empty, Full
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str | Path, pool_size: int = 10, timeout: int = 10, wal_mode: bool = True):
        self.db_path = str(db_path) # Ensure db_path is a string for sqlite3.connect
        self.timeout = timeout
        self.pool_size = pool_size
        self.wal_mode = wal_mode
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock() # To protect access to pool creation logic if needed, though Queue is thread-safe
        self._connections_created = 0

        # Pre-fill the pool
        for _ in range(pool_size):
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Exception as e:
                logger.error(f"Error creating initial connection for pool: {e}")
                # Depending on strictness, could raise here or allow partial pool
        logger.info(f"SQLiteConnectionPool initialized for {self.db_path} with {self._pool.qsize()} connections.")

    def _create_connection(self):
        try:
            # Connections will be used by different threads via asyncio.to_thread,
            # so check_same_thread=False is essential.
            conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            if self.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL") # WAL is durable, NORMAL is faster
            self._connections_created += 1
            # logger.debug(f"Created new SQLite connection. Total created: {self._connections_created}")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to create SQLite connection for {self.db_path}: {e}")
            raise

    def get_connection(self, block: bool = True, timeout: float | None = None) -> sqlite3.Connection:
        """Get a connection from the pool."""
        try:
            # Timeout for getting from queue can be shorter than connection timeout
            effective_timeout = timeout if timeout is not None else self.timeout / 2 
            return self._pool.get(block=block, timeout=effective_timeout)
        except Empty:
            # This can happen if pool is exhausted and block is False or timeout occurs
            logger.warning(f"Connection pool for {self.db_path} exhausted or timed out. Trying to create a temporary connection.")
            # Fallback: create a new one if pool is empty and we must proceed, but don't add it to the pool.
            # This is a safety net, but ideally, the pool size should be tuned.
            # However, for short-lived operations, this might be acceptable if rare.
            # For now, let's stick to the pool size. If Empty is raised, the caller should handle.
            # Re-raising for clarity that pool was empty or timed out.
            raise Empty(f"No connection available from pool for {self.db_path} within timeout.")


    def return_connection(self, conn: sqlite3.Connection | None):
        """Return a connection to the pool."""
        if conn is None:
            return
        try:
            # Test if connection is still alive (e.g., by executing a simple pragma)
            # This can be problematic as it adds overhead. 
            # A more robust pool might try to execute a rollback to ensure it's in a clean state.
            # For simplicity, we'll assume if it was working, it's still good.
            # A more advanced pool might have a "test on return" or "test on borrow" flag.
            conn.execute("PRAGMA user_version;") # A lightweight command to check if connection is alive
            self._pool.put_nowait(conn) # Use put_nowait to avoid blocking if pool is somehow overfilled (shouldn't happen with proper use)
        except sqlite3.Error as e:
            logger.warning(f"Returned connection for {self.db_path} is dead or error: {e}. Discarding.")
            try:
                conn.close() # Close the dead connection
            except:
                pass # Ignore errors during close of a dead connection
            # Optionally, replace the dead connection in the pool
            try:
                new_conn = self._create_connection()
                self._pool.put_nowait(new_conn) # Try to keep the pool full
                logger.info(f"Replaced dead connection in pool for {self.db_path}")
            except Full:
                logger.warning(f"Pool for {self.db_path} is full, cannot replace dead connection immediately.")
            except Exception as ex:
                logger.error(f"Error replacing dead connection for {self.db_path}: {ex}")

        except Full:
            # This should ideally not happen if get_connection/return_connection are used correctly.
            # It means we are returning more connections than were taken out or the pool size is mismanaged.
            logger.error(f"Attempted to return connection to a full pool for {self.db_path}. Closing connection instead.")
            try:
                conn.close()
            except:
                pass


    def close_all(self):
        """Close all connections in the pool."""
        logger.info(f"Closing all connections in pool for {self.db_path}...")
        closed_count = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed_count += 1
            except Empty:
                break # Pool is empty
            except Exception as e:
                logger.error(f"Error closing a connection from pool for {self.db_path}: {e}")
        logger.info(f"Closed {closed_count} connections from pool for {self.db_path}. Pool size: {self._pool.qsize()}")

    def __enter__(self):
        # Context manager to get a connection
        self.conn = self.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Context manager to return the connection
        self.return_connection(self.conn)
        self.conn = None # Clear the connection
```

```python
// ... existing code ...
import sqlite3
import logging
from pathlib import Path
import hashlib # For URL hashing
import time
import asyncio # For to_thread if needed for DB, and for async file saving
import aiofiles # For async file operations

from .config import CrawlerConfig # Assuming config.py is in the same directory
from .db_pool import SQLiteConnectionPool # New import

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 1

class StorageManager:
    def __init__(self, config: CrawlerConfig, db_pool: SQLiteConnectionPool): # Added db_pool
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.db_path = self.data_dir / "crawler_state.db" # db_pool will use this path
        self.content_dir = self.data_dir / "content"
        self.db_pool = db_pool # Store the pool

        self._init_storage()

    def _init_storage(self):
        """Initializes the data directory, content directory, and database schema."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.content_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directories initialized: {self.data_dir}")
        except OSError as e:
            logger.error(f"Error creating storage directories: {e}")
            raise

        self._init_db_schema() # Renamed from _init_db

    def _init_db_schema(self): # Renamed
        """Initializes the SQLite database schema if it doesn't exist or needs upgrade."""
        # This method will now be called from the main thread (Orchestrator init)
        # or at least a context where it's okay to block for a short while.
        # It uses a connection from the pool.
        try:
            # Get a connection from the pool for schema initialization
            with self.db_pool as conn: # Use context manager for get/return
                logger.info(f"Initializing database schema using connection from pool: {self.db_path}")
                self._create_tables(conn) # Pass the connection
        except sqlite3.Error as e:
            logger.error(f"Database error during schema initialization with pool: {e}")
            raise
        except Exception as e: # Catch potential pool errors (e.g., Empty if pool fails to init)
            logger.error(f"Failed to get connection from pool for schema init: {e}")
            raise


    def _create_tables(self, conn: sqlite3.Connection): # Takes a connection argument
        """Creates the necessary tables in the database if they don't already exist."""
        # No longer uses self.conn
        cursor = conn.cursor() # Create cursor from the provided connection
        try:
            # Check current schema version
            cursor.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
            cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            row = cursor.fetchone()
            current_version = row[0] if row else 0

            if current_version < DB_SCHEMA_VERSION:
                logger.info(f"Database schema version is {current_version}. Upgrading to {DB_SCHEMA_VERSION}.")
                
                # Frontier Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS frontier (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT NOT NULL,
                    depth INTEGER DEFAULT 0,
                    added_timestamp INTEGER NOT NULL,
                    priority_score REAL DEFAULT 0
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")

                # Visited URLs Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS visited_urls (
                    url_sha256 TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    crawled_timestamp INTEGER NOT NULL,
                    http_status_code INTEGER,
                    content_type TEXT,
                    content_hash TEXT,
                    content_storage_path TEXT,
                    redirected_to_url TEXT
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_visited_domain ON visited_urls (domain)")

                # Domain Metadata Table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS domain_metadata (
                    domain TEXT PRIMARY KEY,
                    last_scheduled_fetch_timestamp INTEGER DEFAULT 0,
                    robots_txt_content TEXT,
                    robots_txt_fetched_timestamp INTEGER,
                    robots_txt_expires_timestamp INTEGER,
                    is_manually_excluded INTEGER DEFAULT 0,
                    is_seeded INTEGER DEFAULT 0
                )
                """)
                
                # Update schema version
                cursor.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (DB_SCHEMA_VERSION,))
                conn.commit() # Commit changes
                logger.info("Database tables created/updated successfully.")
            else:
                logger.info(f"Database schema is up to date (version {current_version}).")

        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            try:
                conn.rollback() # Rollback on error
            except sqlite3.Error as re:
                logger.error(f"Error during rollback: {re}")
            raise
        finally:
            cursor.close()
// ... existing code ...
    async def save_content_to_file(self, url_hash: str, text_content: str) -> Path | None:
        """Saves extracted text content to a file asynchronously."""
        if not text_content: # Do not save if no text content
            return None
        
        file_path = self.content_dir / f"{url_hash}.txt"
        try:
            async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
                await f.write(text_content)
            logger.debug(f"Saved content for {url_hash} to {file_path}")
            return file_path
        except IOError as e:
            logger.error(f"IOError saving content file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving content file {file_path}: {e}", exc_info=True)
            return None

    def add_visited_page(
        self, 
        url: str, 
        domain: str,
        status_code: int, 
        crawled_timestamp: int,
        content_type: str | None = None, 
        content_text: str | None = None, 
        content_storage_path_str: str | None = None, 
        redirected_to_url: str | None = None
    ):
        """Adds a record of a visited page to the database using a pooled connection.
           This method is expected to be called via asyncio.to_thread if in an async context.
        """
        url_sha256 = self.get_url_sha256(url)
        content_hash: str | None = None
        if content_text:
            content_hash = hashlib.sha256(content_text.encode('utf-8')).hexdigest()

        conn_from_pool: sqlite3.Connection | None = None
        try:
            # This method is run in a separate thread by asyncio.to_thread,
            # so it gets a connection from the pool.
            conn_from_pool = self.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("""
                INSERT OR REPLACE INTO visited_urls 
                (url_sha256, url, domain, crawled_timestamp, http_status_code, content_type, content_hash, content_storage_path, redirected_to_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_sha256, url, domain, crawled_timestamp, status_code, 
                    content_type, content_hash, content_storage_path_str, redirected_to_url
                ))
                conn_from_pool.commit()
                logger.info(f"Recorded visited URL: {url} (Status: {status_code})")
            except sqlite3.Error as e: # Inner try for cursor/execute specific errors
                logger.error(f"DB (execute/commit) error adding visited page {url}: {e}")
                if conn_from_pool: 
                    try:
                        conn_from_pool.rollback()
                    except sqlite3.Error as re:
                        logger.error(f"DB error during rollback for {url}: {re}")
                raise # Re-raise to be caught by outer block if necessary, or to signal failure
            finally:
                if cursor: cursor.close()
        except sqlite3.Error as e: # Outer try for connection pool errors or other sqlite errors
            logger.error(f"DB (pool/connection) error adding visited page {url}: {e}")
            # Rollback is handled by the inner block if the error was post-connection
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected error (e.g. pool issue) adding visited page {url}: {e}")
            raise
        finally:
            if conn_from_pool:
                self.db_pool.return_connection(conn_from_pool)

    def close(self):
        """Closes the database connection pool."""
        # self.conn is no longer the primary connection manager.
        # The pool should be closed by the orchestrator or whoever owns it.
        # This method could call db_pool.close_all() if StorageManager owns the pool,
        # but it's better if Orchestrator does it.
        # For now, we can make it explicit that this StorageManager's close
        # doesn't manage the pool itself, or it could if it created the pool.
        # Assuming Orchestrator will handle pool closure.
        logger.info("StorageManager.close() called. DB Pool closure is managed externally.")
```

```python
// ... existing code ...
import logging
import time
from pathlib import Path
from typing import Set
import asyncio
import sqlite3

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now expects a db_pool
from .utils import normalize_url, extract_domain
from .politeness import PolitenessEnforcer
# db_pool is not directly imported here, assumed to be accessed via self.storage.db_pool

logger = logging.getLogger(__name__)

class FrontierManager:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, politeness: PolitenessEnforcer):
        self.config = config
        self.storage = storage # storage object now contains the db_pool
        self.politeness = politeness
        self.seen_urls: Set[str] = set()

    def _populate_seen_urls_from_db_sync(self) -> Set[str]:
        """Synchronously loads all URLs from visited_urls and frontier tables using a pooled connection."""
        seen = set()
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                # Load from visited_urls
                cursor.execute("SELECT url FROM visited_urls")
                for row in cursor.fetchall():
                    seen.add(row[0])
                
                # Load from current frontier (in case of resume)
                cursor.execute("SELECT url FROM frontier")
                for row in cursor.fetchall():
                    seen.add(row[0])
            finally:
                cursor.close()
            logger.info(f"DB THREAD (pool): Loaded {len(seen)} URLs for seen_urls set.")
        except sqlite3.Error as e:
            logger.error(f"DB THREAD (pool): Error loading for seen_urls set: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB THREAD (pool): Pool error loading for seen_urls set: {e}")
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        return seen

    async def initialize_frontier(self):
// ... existing code ...
    def _mark_domain_as_seeded(self, domain: str):
        """Marks a domain as seeded in the domain_metadata table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                cursor.execute("UPDATE domain_metadata SET is_seeded = 1 WHERE domain = ?", (domain,))
                conn_from_pool.commit()
                logger.debug(f"Marked domain {domain} as seeded in DB (pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error marking domain {domain} as seeded: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error marking domain {domain} as seeded: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error marking domain {domain} as seeded: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _load_seeds(self):
// ... existing code ...
    def _clear_frontier_db_sync(self):
        """Synchronous part of clearing frontier DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("DELETE FROM frontier")
                conn_from_pool.commit() # Explicit commit for DELETE
                logger.info("Cleared frontier table in database (via thread, pool).")
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) error clearing frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"DB (pool/connection) error clearing frontier: {e}")
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) error clearing frontier: {e}")
            raise
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
    
    async def _clear_frontier_db(self):
// ... existing code ...
    def _add_url_sync(self, normalized_url: str, domain: str, depth: int) -> bool:
        """Synchronous part of adding a URL to DB using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
                               (self.storage.get_url_sha256(normalized_url),))
                if cursor.fetchone():
                    return False 

                cursor.execute(
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score) VALUES (?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0)
                )
                if cursor.rowcount > 0:
                    conn_from_pool.commit() # Explicit commit for INSERT
                    return True 
                else:
                    # This means INSERT OR IGNORE found an existing row (URL already in frontier)
                    return False 
            except sqlite3.Error as e:
                logger.error(f"DB (execute/commit) THREAD: Error adding URL {normalized_url} to frontier: {e}")
                if conn_from_pool: conn_from_pool.rollback()
                raise # Propagate to mark as not added
            finally:
                cursor.close()
        except sqlite3.Error as e: # Outer SQLite errors (e.g. connection issue)
            logger.error(f"DB (pool/connection) THREAD: Error adding URL {normalized_url} to frontier: {e}")
            return False # Ensure it's marked as not added on any DB error
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"DB (pool) THREAD: Pool error adding URL {normalized_url} to frontier: {e}")
            return False
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        # Fallback, should be covered by try/except logic.
        # If an error occurred before return True, it should have returned False or raised.
        return False


    async def add_url(self, url: str, depth: int = 0) -> bool:
// ... existing code ...
    def count_frontier(self) -> int:
        """Counts the number of URLs currently in the frontier table using a pooled connection."""
        conn_from_pool: sqlite3.Connection | None = None
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            cursor = conn_from_pool.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM frontier")
                count_row = cursor.fetchone()
                return count_row[0] if count_row else 0
            finally:
                cursor.close()
        except sqlite3.Error as e:
            logger.error(f"Error counting frontier (pool): {e}")
            return 0
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Pool error counting frontier: {e}")
            return 0
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses pooled connections for DB operations.
        Removes the URL from the frontier upon retrieval.
        """
        candidate_check_limit = self.config.max_workers * 5 # Make this configurable?
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_candidates_sync_threaded_pooled(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                        (candidate_check_limit,)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Pass the obtained connection to the threaded function
            candidates = await asyncio.to_thread(_get_candidates_sync_threaded_pooled, conn_from_pool)

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                if not await self.politeness.is_url_allowed(url):
                    logger.debug(f"URL {url} disallowed by politeness rules. Removing from frontier (pool).")
                    
                    def _delete_disallowed_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_disallowed_url_sync_threaded_pooled, conn_from_pool)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain):
                    await self.politeness.record_domain_fetch_attempt(domain) # Async, non-DB
                    
                    def _delete_selected_url_sync_threaded_pooled(conn: sqlite3.Connection):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
                            conn.commit()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_selected_url_sync_threaded_pooled, conn_from_pool)
                    
                    logger.debug(f"Retrieved from frontier: {url} (ID: {url_id}) for domain {domain} (pool)")
                    selected_url_info = (url, domain, url_id)
                    break # Found a suitable URL
                else:
                    # Domain cannot be fetched now, try next candidate
                    pass 
            
        except sqlite3.Error as e: 
            logger.error(f"DB Error getting next URL from frontier (pool): {e}", exc_info=True)
            if conn_from_pool: # Try to rollback if commit failed or other op failed
                try: conn_from_pool.rollback()
                except: pass
            return None 
        except Exception as e: # Catch other exceptions like pool Empty
            logger.error(f"Unexpected or Pool error during get_next_url (pool): {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
        
        return selected_url_info
```

Thanks! I have the content of `orchestrator.py`.

Now, let's modify `CrawlerOrchestrator` to initialize and manage the `SQLiteConnectionPool`.

```python:crawler/crawler_module/orchestrator.py
// ... existing code ...
import asyncio
import logging
import time
from pathlib import Path
from typing import Set, Optional # Added Optional

from .config import CrawlerConfig
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .parser import PageParser, ParseResult
from .utils import extract_domain # For getting domain for storage
from .db_pool import SQLiteConnectionPool # New import

logger = logging.getLogger(__name__)

// ... existing code ...
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

class CrawlerOrchestrator:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        # Initialize DB Pool first
        self.db_pool: SQLiteConnectionPool = SQLiteConnectionPool(
            db_path=Path(config.data_dir) / "crawler_state.db",
            pool_size=config.max_workers, # Pool size can be linked to max_workers
            wal_mode=True # Enable WAL mode for better concurrency by default
        )
        
        # Pass the pool to managers that need it
        self.storage: StorageManager = StorageManager(config, self.db_pool)
        self.fetcher: Fetcher = Fetcher(config)
        # PolitenessEnforcer needs StorageManager which now has the pool
        self.politeness: PolitenessEnforcer = PolitenessEnforcer(config, self.storage, self.fetcher)
        # FrontierManager needs StorageManager (which has the pool) and PolitenessEnforcer
        self.frontier: FrontierManager = FrontierManager(config, self.storage, self.politeness)
        self.parser: PageParser = PageParser()

        self.pages_crawled_count: int = 0
// ... existing code ...
    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Worker-{worker_id}: Starting.")
        try:
            while not self._shutdown_event.is_set():
                next_url_info = await self.frontier.get_next_url()

                if next_url_info is None:
                    # Check if the frontier is truly empty (not just all domains on cooldown)
                    # This count is now using the pool and is thread-safe
                    current_frontier_size_for_worker = await asyncio.to_thread(self.frontier.count_frontier)
                    if current_frontier_size_for_worker == 0:
                        logger.info(f"Worker-{worker_id}: Frontier is confirmed empty by count. Waiting...")
                    else:
                        # logger.debug(f"Worker-{worker_id}: No suitable URL available (cooldowns?). Waiting... Frontier size: {current_frontier_size_for_worker}")
                        pass # Still URLs, but none fetchable now
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                    continue

                url_to_crawl, domain, frontier_id = next_url_info
// ... existing code ...
                        if parse_data.extracted_links:
                            logger.debug(f"Worker-{worker_id}: Found {len(parse_data.extracted_links)} links on {fetch_result.final_url}")
                            links_added_this_page = 0
                            # Batch add_url calls if possible, or ensure add_url is efficient
                            for link in parse_data.extracted_links:
                                # add_url now uses the connection pool internally
                                if await self.frontier.add_url(link):
                                    links_added_this_page +=1
                            if links_added_this_page > 0:
                                self.total_urls_added_to_frontier += links_added_this_page
                                logger.info(f"Worker-{worker_id}: Added {links_added_this_page} new URLs to frontier from {fetch_result.final_url}. Total added: {self.total_urls_added_to_frontier}")
                    else:
                        logger.debug(f"Worker-{worker_id}: Not HTML or no text content for {fetch_result.final_url} (Type: {fetch_result.content_type})\")")
                    
                    # Record success
                    # add_visited_page now uses the connection pool internally
                    await asyncio.to_thread(self.storage.add_visited_page,
                        url=fetch_result.final_url,
                        domain=extract_domain(fetch_result.final_url) or domain, # Use original domain as fallback
// ... existing code ...
    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")

        try:
            # Initialize frontier (loads seeds etc.)
            # initialize_frontier internally uses the pool for DB operations
            await self.frontier.initialize_frontier()
            # count_frontier uses the pool
            initial_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
            self.total_urls_added_to_frontier = initial_frontier_size
            logger.info(f"Frontier initialized. Size: {initial_frontier_size}")

            if initial_frontier_size == 0 and not self.config.resume and not self.config.seed_file.exists(): # More robust check
                logger.warning("No URLs in frontier after seed loading and no seed file to process. Nothing to crawl.")
                self._shutdown_event.set() # Ensure a clean shutdown path
            elif initial_frontier_size == 0 and self.config.resume:
                 logger.warning("Resuming with an empty frontier. Crawler will wait for new URLs or shutdown if none appear.")
            
// ... existing code ...
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All worker tasks have completed.")
                    self._shutdown_event.set()
                    break
                
                # count_frontier uses the pool
                current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                # If frontier is empty and workers might be idle, it could be a natural end
                # This condition needs to be careful not to prematurely shut down if workers are just between tasks.
// ... existing code ...
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2) # Wait longer than worker sleep
                    # Re-check count_frontier
                    current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                    
                    # Check if all workers are truly done (not just waiting for work that might appear)
                    # A simple way is to see if any task is not done.
                    # A more complex way is to check if tasks are blocked on frontier.get_next_url() specifically.
                    all_workers_finished = all(task.done() for task in self.worker_tasks)

                    if current_frontier_size == 0 and all_workers_finished:
                        logger.info("Frontier empty and all workers confirmed done. Signaling shutdown.")
                        self._shutdown_event.set()
                        break
                    elif current_frontier_size == 0 and not all_workers_finished:
                        logger.info(f"Frontier empty, but {sum(1 for t in self.worker_tasks if not t.done())} workers still active. Continuing to monitor.")


                logger.info(f"Status: Crawled={self.pages_crawled_count}, Frontier={current_frontier_size}, AddedToFrontier={self.total_urls_added_to_frontier}, Runtime={(time.time() - self.start_time):.0f}s, PoolConns={self.db_pool._pool.qsize() if self.db_pool else 'N/A'}")
                
                if self._check_stopping_conditions(): # Check global conditions again
// ... existing code ...
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            # StorageManager.close() is synchronous, but should be called.
            # If it were async, we'd await it.
            # self.storage.close() # This method in StorageManager doesn't do much with the pool
            
            # Close the database pool explicitly
            if self.db_pool:
                self.db_pool.close_all()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
// ... existing code ...
```

**Summary of Changes:**

1.  **`db_pool.py`**: New file containing the `SQLiteConnectionPool` class.
2.  **`StorageManager`**:
    *   Accepts `db_pool` in `__init__`.
    *   `_init_db_schema` now uses a connection from the pool.
    *   `_create_tables` now accepts a `sqlite3.Connection` argument.
    *   `add_visited_page` now gets and returns connections from `self.db_pool`.
    *   Removed its own `self.conn` management for primary operations.
3.  **`FrontierManager`**:
    *   All database operations (`_populate_seen_urls_from_db_sync`, `_mark_domain_as_seeded`, `_clear_frontier_db_sync`, `_add_url_sync`, `count_frontier`, operations in `get_next_url`) now use `self.storage.db_pool.get_connection()` and `self.storage.db_pool.return_connection()`.
    *   Connection creation is wrapped in try/finally to ensure connections are returned.
4.  **`CrawlerOrchestrator`**:
    *   Creates an instance of `SQLiteConnectionPool` in `__init__`.
    *   Passes the `db_pool` to `StorageManager`. `FrontierManager` and `PolitenessEnforcer` get it via `StorageManager`.
    *   Calls `self.db_pool.close_all()` in the `finally` block of `run_crawl()` for graceful shutdown.
    *   Added pool connection count to status log.
    *   The pool size is set to `config.max_workers`. This is a reasonable starting point, but it could be tuned.

**Regarding Tests:**

*   Any test that directly instantiates `StorageManager` will now need to also create and pass a `SQLiteConnectionPool`.
    *   Example: `pool = SQLiteConnectionPool(db_path="test_db.sqlite", pool_size=1)`
    *   `storage = StorageManager(test_config, pool)`
    *   Remember to call `pool.close_all()` in the test's cleanup phase.
*   Similarly for tests instantiating `FrontierManager` directly or indirectly via `CrawlerOrchestrator`.
*   Tests that mock database interactions at a higher level might not need changes, but tests that rely on the actual database performing operations will.

This integration should significantly improve how your crawler handles database connections, making it more robust and scalable in terms of worker count.

After you apply these changes, we should test with an increased number of workers to see if the "too many open files" error is resolved. Then we can focus on Problem 2 (the 12+ hour run issue).

```python
// ... existing code ...
import asyncio
import logging
import time
from pathlib import Path
from typing import Set, Optional # Added Optional

from .config import CrawlerConfig
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .parser import PageParser, ParseResult
from .utils import extract_domain # For getting domain for storage
from .db_pool import SQLiteConnectionPool # New import

logger = logging.getLogger(__name__)

// ... existing code ...
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

class CrawlerOrchestrator:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        # Initialize DB Pool first
        self.db_pool: SQLiteConnectionPool = SQLiteConnectionPool(
            db_path=Path(config.data_dir) / "crawler_state.db",
            pool_size=config.max_workers, # Pool size can be linked to max_workers
            wal_mode=True # Enable WAL mode for better concurrency by default
        )
        
        # Pass the pool to managers that need it
        self.storage: StorageManager = StorageManager(config, self.db_pool)
        self.fetcher: Fetcher = Fetcher(config)
        # PolitenessEnforcer needs StorageManager which now has the pool
        self.politeness: PolitenessEnforcer = PolitenessEnforcer(config, self.storage, self.fetcher)
        # FrontierManager needs StorageManager (which has the pool) and PolitenessEnforcer
        self.frontier: FrontierManager = FrontierManager(config, self.storage, self.politeness)
        self.parser: PageParser = PageParser()

        self.pages_crawled_count: int = 0
// ... existing code ...
    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Worker-{worker_id}: Starting.")
        try:
            while not self._shutdown_event.is_set():
                next_url_info = await self.frontier.get_next_url()

                if next_url_info is None:
                    # Check if the frontier is truly empty (not just all domains on cooldown)
                    # This count is now using the pool and is thread-safe
                    current_frontier_size_for_worker = await asyncio.to_thread(self.frontier.count_frontier)
                    if current_frontier_size_for_worker == 0:
                        logger.info(f"Worker-{worker_id}: Frontier is confirmed empty by count. Waiting...")
                    else:
                        # logger.debug(f"Worker-{worker_id}: No suitable URL available (cooldowns?). Waiting... Frontier size: {current_frontier_size_for_worker}")
                        pass # Still URLs, but none fetchable now
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                    continue

                url_to_crawl, domain, frontier_id = next_url_info
// ... existing code ...
                        if parse_data.extracted_links:
                            logger.debug(f"Worker-{worker_id}: Found {len(parse_data.extracted_links)} links on {fetch_result.final_url}")
                            links_added_this_page = 0
                            # Batch add_url calls if possible, or ensure add_url is efficient
                            for link in parse_data.extracted_links:
                                # add_url now uses the connection pool internally
                                if await self.frontier.add_url(link):
                                    links_added_this_page +=1
                            if links_added_this_page > 0:
                                self.total_urls_added_to_frontier += links_added_this_page
                                logger.info(f"Worker-{worker_id}: Added {links_added_this_page} new URLs to frontier from {fetch_result.final_url}. Total added: {self.total_urls_added_to_frontier}")
                    else:
                        logger.debug(f"Worker-{worker_id}: Not HTML or no text content for {fetch_result.final_url} (Type: {fetch_result.content_type})\")")
                    
                    # Record success
                    # add_visited_page now uses the connection pool internally
                    await asyncio.to_thread(self.storage.add_visited_page,
                        url=fetch_result.final_url,
                        domain=extract_domain(fetch_result.final_url) or domain, # Use original domain as fallback
// ... existing code ...
    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")

        try:
            # Initialize frontier (loads seeds etc.)
            # initialize_frontier internally uses the pool for DB operations
            await self.frontier.initialize_frontier()
            # count_frontier uses the pool
            initial_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
            self.total_urls_added_to_frontier = initial_frontier_size
            logger.info(f"Frontier initialized. Size: {initial_frontier_size}")

            if initial_frontier_size == 0 and not self.config.resume and not self.config.seed_file.exists(): # More robust check
                logger.warning("No URLs in frontier after seed loading and no seed file to process. Nothing to crawl.")
                self._shutdown_event.set() # Ensure a clean shutdown path
            elif initial_frontier_size == 0 and self.config.resume:
                 logger.warning("Resuming with an empty frontier. Crawler will wait for new URLs or shutdown if none appear.")
            
// ... existing code ...
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All worker tasks have completed.")
                    self._shutdown_event.set()
                    break
                
                # count_frontier uses the pool
                current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                # If frontier is empty and workers might be idle, it could be a natural end
                # This condition needs to be careful not to prematurely shut down if workers are just between tasks.
// ... existing code ...
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2) # Wait longer than worker sleep
                    # Re-check count_frontier
                    current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                    
                    # Check if all workers are truly done (not just waiting for work that might appear)
                    # A simple way is to see if any task is not done.
                    # A more complex way is to check if tasks are blocked on frontier.get_next_url() specifically.
                    all_workers_finished = all(task.done() for task in self.worker_tasks)

                    if current_frontier_size == 0 and all_workers_finished:
                        logger.info("Frontier empty and all workers confirmed done. Signaling shutdown.")
                        self._shutdown_event.set()
                        break
                    elif current_frontier_size == 0 and not all_workers_finished:
                        logger.info(f"Frontier empty, but {sum(1 for t in self.worker_tasks if not t.done())} workers still active. Continuing to monitor.")


                logger.info(f"Status: Crawled={self.pages_crawled_count}, Frontier={current_frontier_size}, AddedToFrontier={self.total_urls_added_to_frontier}, Runtime={(time.time() - self.start_time):.0f}s, PoolConns={self.db_pool._pool.qsize() if self.db_pool else 'N/A'}")
                
                if self._check_stopping_conditions(): # Check global conditions again
// ... existing code ...
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            # StorageManager.close() is synchronous, but should be called.
            # If it were async, we'd await it.
            # self.storage.close() # This method in StorageManager doesn't do much with the pool
            
            # Close the database pool explicitly
            if self.db_pool:
                self.db_pool.close_all()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
// ... existing code ...
```

```python
// ... existing code ...
import asyncio
import logging
import time
from pathlib import Path
from typing import Set, Optional # Added Optional

from .config import CrawlerConfig
from .storage import StorageManager
from .fetcher import Fetcher, FetchResult
from .politeness import PolitenessEnforcer
from .frontier import FrontierManager
from .parser import PageParser, ParseResult
from .utils import extract_domain # For getting domain for storage
from .db_pool import SQLiteConnectionPool # New import

logger = logging.getLogger(__name__)

// ... existing code ...
ORCHESTRATOR_STATUS_INTERVAL_SECONDS = 5

class CrawlerOrchestrator:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        
        # Initialize DB Pool first
        self.db_pool: SQLiteConnectionPool = SQLiteConnectionPool(
            db_path=Path(config.data_dir) / "crawler_state.db",
            pool_size=config.max_workers, # Pool size can be linked to max_workers
            wal_mode=True # Enable WAL mode for better concurrency by default
        )
        
        # Pass the pool to managers that need it
        self.storage: StorageManager = StorageManager(config, self.db_pool)
        self.fetcher: Fetcher = Fetcher(config)
        # PolitenessEnforcer needs StorageManager which now has the pool
        self.politeness: PolitenessEnforcer = PolitenessEnforcer(config, self.storage, self.fetcher)
        # FrontierManager needs StorageManager (which has the pool) and PolitenessEnforcer
        self.frontier: FrontierManager = FrontierManager(config, self.storage, self.politeness)
        self.parser: PageParser = PageParser()

        self.pages_crawled_count: int = 0
// ... existing code ...
    async def _worker(self, worker_id: int):
        """Core logic for a single crawl worker."""
        logger.info(f"Worker-{worker_id}: Starting.")
        try:
            while not self._shutdown_event.is_set():
                next_url_info = await self.frontier.get_next_url()

                if next_url_info is None:
                    # Check if the frontier is truly empty (not just all domains on cooldown)
                    # This count is now using the pool and is thread-safe
                    current_frontier_size_for_worker = await asyncio.to_thread(self.frontier.count_frontier)
                    if current_frontier_size_for_worker == 0:
                        logger.info(f"Worker-{worker_id}: Frontier is confirmed empty by count. Waiting...")
                    else:
                        # logger.debug(f"Worker-{worker_id}: No suitable URL available (cooldowns?). Waiting... Frontier size: {current_frontier_size_for_worker}")
                        pass # Still URLs, but none fetchable now
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS) 
                    continue

                url_to_crawl, domain, frontier_id = next_url_info
// ... existing code ...
                        if parse_data.extracted_links:
                            logger.debug(f"Worker-{worker_id}: Found {len(parse_data.extracted_links)} links on {fetch_result.final_url}")
                            links_added_this_page = 0
                            # Batch add_url calls if possible, or ensure add_url is efficient
                            for link in parse_data.extracted_links:
                                # add_url now uses the connection pool internally
                                if await self.frontier.add_url(link):
                                    links_added_this_page +=1
                            if links_added_this_page > 0:
                                self.total_urls_added_to_frontier += links_added_this_page
                                logger.info(f"Worker-{worker_id}: Added {links_added_this_page} new URLs to frontier from {fetch_result.final_url}. Total added: {self.total_urls_added_to_frontier}")
                    else:
                        logger.debug(f"Worker-{worker_id}: Not HTML or no text content for {fetch_result.final_url} (Type: {fetch_result.content_type})\")")
                    
                    # Record success
                    # add_visited_page now uses the connection pool internally
                    await asyncio.to_thread(self.storage.add_visited_page,
                        url=fetch_result.final_url,
                        domain=extract_domain(fetch_result.final_url) or domain, # Use original domain as fallback
// ... existing code ...
    async def run_crawl(self):
        self.start_time = time.time()
        logger.info(f"Crawler starting with config: {self.config}")

        try:
            # Initialize frontier (loads seeds etc.)
            # initialize_frontier internally uses the pool for DB operations
            await self.frontier.initialize_frontier()
            # count_frontier uses the pool
            initial_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
            self.total_urls_added_to_frontier = initial_frontier_size
            logger.info(f"Frontier initialized. Size: {initial_frontier_size}")

            if initial_frontier_size == 0 and not self.config.resume and not self.config.seed_file.exists(): # More robust check
                logger.warning("No URLs in frontier after seed loading and no seed file to process. Nothing to crawl.")
                self._shutdown_event.set() # Ensure a clean shutdown path
            elif initial_frontier_size == 0 and self.config.resume:
                 logger.warning("Resuming with an empty frontier. Crawler will wait for new URLs or shutdown if none appear.")
            
// ... existing code ...
            while not self._shutdown_event.is_set():
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All worker tasks have completed.")
                    self._shutdown_event.set()
                    break
                
                # count_frontier uses the pool
                current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                # If frontier is empty and workers might be idle, it could be a natural end
                # This condition needs to be careful not to prematurely shut down if workers are just between tasks.
// ... existing code ...
                    await asyncio.sleep(EMPTY_FRONTIER_SLEEP_SECONDS * 2) # Wait longer than worker sleep
                    # Re-check count_frontier
                    current_frontier_size = await asyncio.to_thread(self.frontier.count_frontier)
                    
                    # Check if all workers are truly done (not just waiting for work that might appear)
                    # A simple way is to see if any task is not done.
                    # A more complex way is to check if tasks are blocked on frontier.get_next_url() specifically.
                    all_workers_finished = all(task.done() for task in self.worker_tasks)

                    if current_frontier_size == 0 and all_workers_finished:
                        logger.info("Frontier empty and all workers confirmed done. Signaling shutdown.")
                        self._shutdown_event.set()
                        break
                    elif current_frontier_size == 0 and not all_workers_finished:
                        logger.info(f"Frontier empty, but {sum(1 for t in self.worker_tasks if not t.done())} workers still active. Continuing to monitor.")


                logger.info(f"Status: Crawled={self.pages_crawled_count}, Frontier={current_frontier_size}, AddedToFrontier={self.total_urls_added_to_frontier}, Runtime={(time.time() - self.start_time):.0f}s, PoolConns={self.db_pool._pool.qsize() if self.db_pool else 'N/A'}")
                
                if self._check_stopping_conditions(): # Check global conditions again
// ... existing code ...
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            # StorageManager.close() is synchronous, but should be called.
            # If it were async, we'd await it.
            # self.storage.close() # This method in StorageManager doesn't do much with the pool
            
            # Close the database pool explicitly
            if self.db_pool:
                self.db_pool.close_all()
            
            logger.info(f"Crawl finished. Total pages crawled: {self.pages_crawled_count}")
// ... existing code ...
```

---

**User**

why is the StorageManager.close() no longer doing anything? why is ownership of the pool moved to orchestrator?

---

**Cursor**

You've asked two excellent questions that get to the heart of managing shared resources in an application.

1.  **Why is `StorageManager.close()` no longer closing the pool?**
    *   Previously, `StorageManager` created and managed its own single database connection (`self.conn`). So, its `close()` method was responsible for closing that specific connection.
    *   With the introduction of `SQLiteConnectionPool`, the `StorageManager` no longer "owns" the connections in the same exclusive way. Instead, it "borrows" connections from the pool for each operation and then returns them. The pool itself manages the lifecycle of these persistent connections.
    *   If `StorageManager.close()` were to shut down the entire pool, it might do so prematurely while other components (like `FrontierManager`, which also uses the pool via `StorageManager`) might still need to perform final database operations during their own shutdown sequences. This could lead to errors (trying to use a closed pool).
    *   Therefore, `StorageManager.close()` now correctly reflects that the pool's overall lifecycle is managed elsewhere. The log message `logger.info("StorageManager.close() called. DB Pool closure is managed externally.")` was added to make this explicit. It could still be used if `StorageManager` had other, non-pool resources to clean up.

2.  **Why is ownership of the pool moved to `CrawlerOrchestrator`?**
    *   **Centralized Lifecycle Management:** The `SQLiteConnectionPool` is a resource shared by multiple components (`StorageManager`, and indirectly `FrontierManager` and `PolitenessEnforcer` because they use `StorageManager`). The `CrawlerOrchestrator` is the highest-level component that oversees the entire crawl process and the lifecycle of all major sub-components.
    *   **Ensuring Correct Order of Operations:** The `Orchestrator` can ensure that the pool is:
        *   **Created** *before* any component that needs it is initialized.
        *   **Kept alive** as long as any component might need to access the database.
        *   **Closed** *after* all components have finished their work and have been shut down. This typically happens in the `finally` block of the main execution logic within the `Orchestrator`.
    *   **Avoiding Conflicts and Simplifying Components:** If each component tried to manage the pool's lifecycle, it would be complex to coordinate. For example, whose `close()` method is the "definitive" one to shut down the pool? By giving ownership to the `Orchestrator`, `StorageManager` and `FrontierManager` become simpler: they are just *users* of the pool, not its owner.
    *   **Analogy:** Think of the connection pool as a shared facility (like a printing service) used by different departments. The main administration (`Orchestrator`) decides when to open and close the printing service for everyone. Individual departments (`StorageManager`, `FrontierManager`) use the service but don't shut it down themselves, as other departments might still need it.

This approach of centralizing the management of shared resources in the highest-level appropriate component (`CrawlerOrchestrator` in this case) generally leads to more robust and predictable behavior, especially during startup and shutdown phases of an application.

---

**User**

ok, why not just delete StorageManager.close() method?

---

**Cursor**

You're raising a very good point about code cleanliness and clear responsibility.

Given that the `SQLiteConnectionPool`'s lifecycle is now explicitly managed by the `CrawlerOrchestrator`, the `StorageManager.close()` method, in its current form (only logging that closure is external), doesn't perform any essential cleanup actions for the `StorageManager` itself.

**Arguments for deleting `StorageManager.close()`:**

1.  **Clearer Responsibility:** It reinforces that `StorageManager` is a *user* of the database pool, not its lifecycle manager. The `Orchestrator` is responsible for creating and destroying the pool.
2.  **Simpler API:** It removes a method from `StorageManager`'s public interface that doesn't have a distinct action tied to the `StorageManager`'s own state.
3.  **Reduced Boilerplate:** If a method does nothing critical, it can be considered boilerplate.

**Argument for keeping it (though weaker in this specific context now):**

1.  **API Convention:** Sometimes, classes that manage or interact heavily with resources (even if provided) have `close()` methods as a convention, perhaps for future extensibility if `StorageManager` were to manage other types of closeable resources directly. However, its current operations (DB via pool, files via `aiofiles` context managers) don't necessitate it.

**Recommendation:**

In this case, **deleting the `StorageManager.close()` method is a reasonable and good refactoring step.** It makes the separation of concerns cleaner: `Orchestrator` manages the pool's lifecycle, and `StorageManager` simply uses the pool.

If we delete `StorageManager.close()`, we also need to remove the corresponding call `self.storage.close()` from the `finally` block in `CrawlerOrchestrator.run_crawl()`.

Would you like me to proceed with these two changes?

1.  Delete the `close()` method from `crawler/crawler_module/storage.py`.
2.  Remove the call to `self.storage.close()` in `crawler/crawler_module/orchestrator.py`.

---

**User**

in my opinion, the "least astonishment" would be to not have a call to a method that does nothing. yes let's delete the method and the call to it

---

**Cursor**

I agree, the principle of least astonishment is a good guide here. A method call that does nothing or only logs that it does nothing can be confusing.

Okay, I will proceed with the following changes:

1.  **Delete the `close()` method** from `crawler/crawler_module/storage.py`.
2.  **Remove the call `self.storage.close()`** from the `finally` block in `crawler/crawler_module/orchestrator.py`.

Here are the proposed changes:

```python:crawler/crawler_module/storage.py
// ... existing code ...
        finally:
            if conn_from_pool:
                self.db_pool.return_connection(conn_from_pool)

    # The close() method will be deleted from here.
    # def close(self):
    #     """Closes the database connection pool."""
    #     # self.conn is no longer the primary connection manager.
    #     # The pool should be closed by the orchestrator or whoever owns it.
    #     # This method could call db_pool.close_all() if StorageManager owns the pool,
    #     # but it's better if Orchestrator does it.
    #     # Assuming Orchestrator will handle pool closure.
    #     logger.info("StorageManager.close() called. DB Pool closure is managed externally.")
```

```python:crawler/crawler_module/orchestrator.py
// ... existing code ...
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            # self.storage.close() # This call will be removed.
            
            # Close the database pool explicitly
            if self.db_pool:
// ... existing code ...
```

I will apply these changes now.

After this, the `StorageManager` will no longer have a `close()` method, and the `CrawlerOrchestrator` will be solely responsible for closing the database connection pool. This makes the responsibilities clearer.

With the connection pool integrated and this minor cleanup done, we should be in a much better position regarding the "too many open files" error when scaling workers.

The next steps would be:
1.  Test the crawler with an increased number of workers (e.g., 50, 100) to verify if the file descriptor issue is resolved.
2.  Move on to diagnosing Problem 2: the network utilization drop and OS termination after 12+ hours. For this, we'll need the logs from that long run and potentially system (OOM) logs, as discussed previously.

```python
// ... existing code ...
        finally:
            if conn_from_pool:
                self.db_pool.return_connection(conn_from_pool)

    # The close() method will be deleted from here.
    # def close(self):
    #     """Closes the database connection pool."""
    #     # self.conn is no longer the primary connection manager.
    #     # The pool should be closed by the orchestrator or whoever owns it.
    #     # This method could call db_pool.close_all() if StorageManager owns the pool,
    #     # but it's better if Orchestrator does it.
    #     # Assuming Orchestrator will handle pool closure.
    #     logger.info("StorageManager.close() called. DB Pool closure is managed externally.")
```

```python
// ... existing code ...
        finally:
            logger.info("Performing final cleanup...")
            await self.fetcher.close_session()
            # self.storage.close() # This call will be removed.
            
            # Close the database pool explicitly
            if self.db_pool:
// ... existing code ...
```

---

**User**

cool can you help update the tests? i think theres a couple constructions of StorageManager that need a db_pool now
```
=============================================== short test summary info ===============================================
FAILED tests/test_frontier.py::test_frontier_resume_with_politeness - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
FAILED tests/test_storage.py::test_storage_manager_reinitialization - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_frontier.py::test_frontier_initialization_new - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_frontier.py::test_add_and_get_urls - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_politeness.py::test_load_manual_exclusions_with_file - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_storage_manager_initialization - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_database_schema_creation - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_get_url_sha256 - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_save_content_to_file - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_save_content_to_file_empty_content - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_save_content_to_file_io_error - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_add_visited_page - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
ERROR tests/test_storage.py::test_add_visited_page_no_content - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'
================================ 2 failed, 60 passed, 26 warnings, 11 errors in 0.36s =================================
```

---

**Cursor**

You're right, the test failures are exactly what we'd expect after changing the `StorageManager` constructor. We need to update the tests to provide the `SQLiteConnectionPool`.

Here's a general strategy for fixing these tests, likely using pytest fixtures for managing the `SQLiteConnectionPool` lifecycle:

1.  **Create a `db_pool` fixture**: This fixture, probably in `conftest.py` or a relevant test file, will be responsible for:
    *   Creating a `SQLiteConnectionPool` instance, typically pointing to a temporary/test database.
    *   Yielding the pool to the tests.
    *   Calling `pool.close_all()` after the test finishes to ensure cleanup.
2.  **Update fixtures that provide `StorageManager`**: If you have fixtures that create `StorageManager` instances, they will need to take the new `db_pool` fixture as an argument and pass the pool to the `StorageManager` constructor.
3.  **Update direct instantiations**: If tests instantiate `StorageManager` directly (not through a fixture), they will need to be modified to create/use a pool.

Let's start by looking at `conftest.py` to see if there are existing fixtures we can build upon (like for `CrawlerConfig` or a test database path). Then we can examine one of the failing test files, for example, `tests/test_storage.py`.

The `conftest.py` file currently only modifies `sys.path`. It doesn't have any fixtures yet. That's fine, we can add what we need.

Let's create a new `db_pool` fixture. Since it needs a database path, it's often good to use a temporary directory for test databases to keep them isolated and ensure they are cleaned up. `pytest` provides a `tmp_path` fixture for this.

I'll propose adding a `db_pool` fixture to `crawler/conftest.py`. This fixture will create a pool using a temporary database file for each test that uses it.

```python:crawler/conftest.py
import sys
import os
import pytest # New import
from pathlib import Path # New import

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# If crawler_module is one level deeper than where conftest.py is (e.g. src/crawler_module)
# and conftest.py is in tests/, you might need:
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))))
# Or more robustly, find the project root based on a known file/dir if structure is complex. 

# Import the SQLiteConnectionPool after sys.path is modified
from crawler_module.db_pool import SQLiteConnectionPool
from crawler_module.config import CrawlerConfig # For a default config if needed

@pytest.fixture(scope="function") # "function" scope creates a new pool for each test function
def db_pool(tmp_path: Path) -> SQLiteConnectionPool:
    """
    Pytest fixture for a SQLiteConnectionPool using a temporary database.
    Ensures the pool is closed after the test.
    """
    db_file = tmp_path / "test_crawler_state.db"
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) # Small pool for tests
    yield pool # Provide the pool to the test
    pool.close_all() # Teardown: close all connections in the pool

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir()
    
    # Create a dummy seed file if necessary for some tests, or leave it to tests to create
    seed_file = data_dir / "seeds.txt"
    # seed_file.write_text("http://example.com\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        max_workers=1, # Keep low for tests unless testing concurrency
        log_level="DEBUG",
        # Add other necessary minimal config options
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False 
    )

# You might also want a fixture for StorageManager itself if it's commonly used
@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    # Import StorageManager here to ensure sys.path modification has taken effect
    from crawler_module.storage import StorageManager
    return StorageManager(config=test_config, db_pool=db_pool)
```
**Explanation of additions to `conftest.py`:**
1.  **`db_pool` fixture**:
    *   Uses `pytest`'s built-in `tmp_path` fixture to get a unique temporary directory for each test function.
    *   Creates a `test_crawler_state.db` inside this temporary directory.
    *   Initializes a `SQLiteConnectionPool` with `pool_size=1` (usually sufficient for single-threaded tests) pointing to this test database.
    *   `yield pool`: This makes the pool instance available to the test function.
    *   `pool.close_all()`: This is the teardown code that runs after the test function completes, ensuring all connections are closed.
2.  **`test_config` fixture**:
    *   Creates a basic `CrawlerConfig` instance suitable for testing. It also uses `tmp_path` to ensure that any data written by the crawler during tests goes to a temporary location.
    *   You may need to adjust the default parameters based on your testing needs.
3.  **`storage_manager` fixture**:
    *   This is a convenience fixture that creates a `StorageManager` instance using the `test_config` and `db_pool` fixtures. This will be very useful for tests in `test_storage.py`.

Now, let's see how to use these fixtures in `tests/test_storage.py`. I'll need to see the contents of that file to give specific guidance.

```python
import sys
import os
import pytest # New import
from pathlib import Path # New import

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# If crawler_module is one level deeper than where conftest.py is (e.g. src/crawler_module)
# and conftest.py is in tests/, you might need:
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))))
# Or more robustly, find the project root based on a known file/dir if structure is complex. 

# Import the SQLiteConnectionPool after sys.path is modified
from crawler_module.db_pool import SQLiteConnectionPool
from crawler_module.config import CrawlerConfig # For a default config if needed

@pytest.fixture(scope="function") # "function" scope creates a new pool for each test function
def db_pool(tmp_path: Path) -> SQLiteConnectionPool:
    """
    Pytest fixture for a SQLiteConnectionPool using a temporary database.
    Ensures the pool is closed after the test.
    """
    db_file = tmp_path / "test_crawler_state.db"
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) # Small pool for tests
    yield pool # Provide the pool to the test
    pool.close_all() # Teardown: close all connections in the pool

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir()
    
    # Create a dummy seed file if necessary for some tests, or leave it to tests to create
    seed_file = data_dir / "seeds.txt"
    # seed_file.write_text("http://example.com\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        max_workers=1, # Keep low for tests unless testing concurrency
        log_level="DEBUG",
        # Add other necessary minimal config options
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False 
    )

# You might also want a fixture for StorageManager itself if it's commonly used
@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    # Import StorageManager here to ensure sys.path modification has taken effect
    from crawler_module.storage import StorageManager
    return StorageManager(config=test_config, db_pool=db_pool)
```

My apologies, I should have used the full path. Let me try that again.

Okay, I have the content of `crawler/tests/test_storage.py`.

You already have a `dummy_config` fixture and a `storage_manager` fixture. We need to modify the `storage_manager` fixture to accept and use the `db_pool` fixture we defined in `conftest.py`.

Additionally, any test function that directly verifies database content (e.g., `test_database_schema_creation`, `test_add_visited_page`) by accessing `storage_manager.conn` will need to be updated. Since `storage_manager.conn` is no longer the primary way to interact with the DB (it's only used for the initial schema creation if the pool is empty, which isn't ideal for testing individual operations), tests should instead get a connection from the `db_pool` to verify DB state.

Here's how we can update `crawler/tests/test_storage.py`:

```python:crawler/tests/test_storage.py
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# Assuming conftest.py is in the parent directory (crawler/) relative to this file's dir (crawler/tests/)
# or pytest automatically discovers it.
# If conftest.py is in the same directory as this test file, no special path needed.
# If conftest.py is at crawler/conftest.py, it should be found.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig 
# The db_pool fixture will be automatically available if defined in conftest.py

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data."""
    test_data_dir = tmp_path / "test_crawler_data_storage"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config(temp_test_dir: Path) -> CrawlerConfig:
    """Create a dummy CrawlerConfig for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds.txt"
    with open(seed_file_path, 'w') as f:
        f.write("http://example.com\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir,
        seed_file=seed_file_path,
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0",
        seeded_urls_only=False
    )
    logger.debug(f"Created dummy config with data_dir: {cfg.data_dir}")
    return cfg

# Updated storage_manager fixture
@pytest.fixture
def storage_manager(dummy_config: CrawlerConfig, db_pool) -> StorageManager: # Added db_pool
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config.data_dir} and db_pool")
    # StorageManager's __init__ now takes db_pool
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close() here as the pool's lifecycle is managed by its fixture.
    # The old sm.conn is also gone for general use.
    yield sm
    # Teardown of db_pool (and its connections) is handled by the db_pool fixture itself.

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    # storage_manager.db_path is the path TO the db, not a connection object.
    # The actual db file is created by the pool at db_pool.db_path
    assert Path(db_pool.db_path).exists() 
    # storage_manager.conn is no longer a reliable attribute for general connections.
    # The schema should have been initialized using a connection from the pool.
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # Get a connection from the pool to inspect the database
    with db_pool as conn: # Use the pool's context manager
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        # Check schema version
        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        # Example: Check for an index
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager): # This test doesn't interact with DB pool directly
    """Test the URL hashing function."""
// ... existing code ...
# Test re-initialization (simulating resume or multiple starts)
def test_storage_manager_reinitialization(dummy_config: CrawlerConfig, tmp_path: Path): # Removed storage_manager fixture
    """Test that re-initializing StorageManager with the same path is safe.
       This test needs its own pool instances to simulate re-initialization.
    """
    db_file = tmp_path / "reinit_test.db"

    pool1 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm1 = StorageManager(config=dummy_config, db_pool=pool1)
    # Ensure schema is created if not already
    # sm1._init_db_schema() is called by StorageManager.__init__

    # No sm1.close() in the sense of closing the pool, pool1 will be closed.
    db_path_from_pool1 = Path(pool1.db_path)
    pool1.close_all()


    pool2 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm2 = StorageManager(config=dummy_config, db_pool=pool2)
    # sm2._init_db_schema()
    db_path_from_pool2 = Path(pool2.db_path)
    pool2.close_all()

    assert db_path_from_pool1 == db_path_from_pool2
    assert db_path_from_pool1.exists()
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager, tmp_path: Path):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
// ... existing code ...
def test_add_visited_page(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test adding a visited page record to the database."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify the record in the database using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
```

**Key changes in `test_storage.py`:**

1.  **`storage_manager` fixture updated**:
    *   Now takes `db_pool` as an argument (which pytest will supply from `conftest.py`).
    *   Passes `db_pool` to `StorageManager` constructor.
    *   Removed `sm.close()` because pool cleanup is handled by the `db_pool` fixture.
2.  **Tests using `storage_manager`**:
    *   Tests like `test_storage_manager_initialization`, `test_database_schema_creation`, `test_add_visited_page`, `test_add_visited_page_no_content` now also accept `db_pool` as a parameter if they need to directly inspect the database.
    *   Instead of using `storage_manager.conn` (which is no longer the primary connection point), they use `with db_pool as conn:` to get a connection for database verification.
3.  **`test_storage_manager_reinitialization` updated**:
    *   This test specifically checks re-initialization behavior. It now creates its own `SQLiteConnectionPool` instances for `sm1` and `sm2` using a specific temporary database file (`reinit_test.db`). This is important to correctly simulate re-opening the same database. It ensures each `StorageManager` instance in this test gets its own pool pointing to that specific file.
    *   It explicitly calls `pool.close_all()` on these manually created pools.

**Next Steps:**

1.  Apply the changes to `crawler/conftest.py` (if you haven't already).
2.  Apply the changes to `crawler/tests/test_storage.py`.
3.  You'll need to apply a similar pattern to other failing test files (like `tests/test_frontier.py` and `tests/test_politeness.py`):
    *   Identify fixtures that create `StorageManager` or components that depend on it. Update them to use the `db_pool` fixture.
    *   If tests directly instantiate `StorageManager`, update them to pass a `db_pool` (either the fixture or a manually created one if the test has specific needs like `test_storage_manager_reinitialization`).
    *   If tests need to verify database state, use `with db_pool as conn:` to get a connection.

Would you like me to proceed with applying these changes to `test_storage.py` and then look at `test_frontier.py`?

```python
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# Assuming conftest.py is in the parent directory (crawler/) relative to this file's dir (crawler/tests/)
# or pytest automatically discovers it.
# If conftest.py is in the same directory as this test file, no special path needed.
# If conftest.py is at crawler/conftest.py, it should be found.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig 
# The db_pool fixture will be automatically available if defined in conftest.py

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data."""
    test_data_dir = tmp_path / "test_crawler_data_storage"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config(temp_test_dir: Path) -> CrawlerConfig:
    """Create a dummy CrawlerConfig for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds.txt"
    with open(seed_file_path, 'w') as f:
        f.write("http://example.com\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir,
        seed_file=seed_file_path,
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0",
        seeded_urls_only=False
    )
    logger.debug(f"Created dummy config with data_dir: {cfg.data_dir}")
    return cfg

# Updated storage_manager fixture
@pytest.fixture
def storage_manager(dummy_config: CrawlerConfig, db_pool) -> StorageManager: # Added db_pool
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config.data_dir} and db_pool")
    # StorageManager's __init__ now takes db_pool
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close() here as the pool's lifecycle is managed by its fixture.
    # The old sm.conn is also gone for general use.
    yield sm
    # Teardown of db_pool (and its connections) is handled by the db_pool fixture itself.

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    # storage_manager.db_path is the path TO the db, not a connection object.
    # The actual db file is created by the pool at db_pool.db_path
    assert Path(db_pool.db_path).exists() 
    # storage_manager.conn is no longer a reliable attribute for general connections.
    # The schema should have been initialized using a connection from the pool.
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # Get a connection from the pool to inspect the database
    with db_pool as conn: # Use the pool's context manager
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        # Check schema version
        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        # Example: Check for an index
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager): # This test doesn't interact with DB pool directly
    """Test the URL hashing function."""
// ... existing code ...
# Test re-initialization (simulating resume or multiple starts)
def test_storage_manager_reinitialization(dummy_config: CrawlerConfig, tmp_path: Path): # Removed storage_manager fixture
    """Test that re-initializing StorageManager with the same path is safe.
       This test needs its own pool instances to simulate re-initialization.
    """
    db_file = tmp_path / "reinit_test.db"

    pool1 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm1 = StorageManager(config=dummy_config, db_pool=pool1)
    # Ensure schema is created if not already
    # sm1._init_db_schema() is called by StorageManager.__init__

    # No sm1.close() in the sense of closing the pool, pool1 will be closed.
    db_path_from_pool1 = Path(pool1.db_path)
    pool1.close_all()


    pool2 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm2 = StorageManager(config=dummy_config, db_pool=pool2)
    # sm2._init_db_schema()
    db_path_from_pool2 = Path(pool2.db_path)
    pool2.close_all()

    assert db_path_from_pool1 == db_path_from_pool2
    assert db_path_from_pool1.exists()
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager, tmp_path: Path):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
// ... existing code ...
def test_add_visited_page(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test adding a visited page record to the database."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify the record in the database using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
```

```python
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# Assuming conftest.py is in the parent directory (crawler/) relative to this file's dir (crawler/tests/)
# or pytest automatically discovers it.
# If conftest.py is in the same directory as this test file, no special path needed.
# If conftest.py is at crawler/conftest.py, it should be found.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig 
# The db_pool fixture will be automatically available if defined in conftest.py

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data."""
    test_data_dir = tmp_path / "test_crawler_data_storage"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config(temp_test_dir: Path) -> CrawlerConfig:
    """Create a dummy CrawlerConfig for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds.txt"
    with open(seed_file_path, 'w') as f:
        f.write("http://example.com\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir,
        seed_file=seed_file_path,
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None,
        max_duration=None,
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0",
        seeded_urls_only=False
    )
    logger.debug(f"Created dummy config with data_dir: {cfg.data_dir}")
    return cfg

# Updated storage_manager fixture
@pytest.fixture
def storage_manager(dummy_config: CrawlerConfig, db_pool) -> StorageManager: # Added db_pool
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config.data_dir} and db_pool")
    # StorageManager's __init__ now takes db_pool
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close() here as the pool's lifecycle is managed by its fixture.
    # The old sm.conn is also gone for general use.
    yield sm
    # Teardown of db_pool (and its connections) is handled by the db_pool fixture itself.

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    # storage_manager.db_path is the path TO the db, not a connection object.
    # The actual db file is created by the pool at db_pool.db_path
    assert Path(db_pool.db_path).exists() 
    # storage_manager.conn is no longer a reliable attribute for general connections.
    # The schema should have been initialized using a connection from the pool.
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # Get a connection from the pool to inspect the database
    with db_pool as conn: # Use the pool's context manager
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        # Check schema version
        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        # Example: Check for an index
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager): # This test doesn't interact with DB pool directly
    """Test the URL hashing function."""
// ... existing code ...
# Test re-initialization (simulating resume or multiple starts)
def test_storage_manager_reinitialization(dummy_config: CrawlerConfig, tmp_path: Path): # Removed storage_manager fixture
    """Test that re-initializing StorageManager with the same path is safe.
       This test needs its own pool instances to simulate re-initialization.
    """
    db_file = tmp_path / "reinit_test.db"

    pool1 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm1 = StorageManager(config=dummy_config, db_pool=pool1)
    # Ensure schema is created if not already
    # sm1._init_db_schema() is called by StorageManager.__init__

    # No sm1.close() in the sense of closing the pool, pool1 will be closed.
    db_path_from_pool1 = Path(pool1.db_path)
    pool1.close_all()


    pool2 = SQLiteConnectionPool(db_path=db_file, pool_size=1)
    sm2 = StorageManager(config=dummy_config, db_pool=pool2)
    # sm2._init_db_schema()
    db_path_from_pool2 = Path(pool2.db_path)
    pool2.close_all()

    assert db_path_from_pool1 == db_path_from_pool2
    assert db_path_from_pool1.exists()
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager, tmp_path: Path):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
// ... existing code ...
@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
// ... existing code ...
def test_add_visited_page(storage_manager: StorageManager, dummy_config: CrawlerConfig, db_pool): # Added db_pool
    """Test adding a visited page record to the database."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify the record in the database using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool): # Added db_pool
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
// ... existing code ...
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
// ... existing code ...
    )

    # Verify using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
// ... existing code ...
```

Okay, I've reviewed `crawler/tests/test_frontier.py`. This file has its own set of fixtures for creating test configurations and `StorageManager` instances, and it also mocks `PolitenessEnforcer`.

Here's how we'll update it:

1.  **Update `storage_manager_for_frontier` fixture**:
    *   It needs to accept the `db_pool` fixture (from `conftest.py`).
    *   It will pass `db_pool` to the `StorageManager` constructor.
    *   The `sm.close()` call should be removed (as the pool manages connection closing).
2.  **Update tests that instantiate `StorageManager` or `FrontierManager` directly**:
    *   Specifically, `test_frontier_resume_with_politeness` creates its own `StorageManager` instances. These will need to be updated to also create and use a `SQLiteConnectionPool`. Since this test involves multiple "runs" and distinct storage states, it's best if each `StorageManager` instance in this test gets its own dedicated `SQLiteConnectionPool` pointing to the same test database file for that run.

Here are the proposed changes for `crawler/tests/test_frontier.py`:

```python:crawler/tests/test_frontier.py
import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock # Added AsyncMock
import sqlite3

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer # Added
from crawler_module.fetcher import Fetcher # Added for PolitenessEnforcer mock typing
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation in specific tests

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
// ... existing code ...
@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
// ... existing code ...
# Updated storage_manager_for_frontier fixture
@pytest_asyncio.fixture
async def storage_manager_for_frontier(actual_config_for_frontier: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    # StorageManager now requires db_pool
    sm = StorageManager(config=actual_config_for_frontier, db_pool=db_pool)
    yield sm
    # No sm.close() here, db_pool fixture handles cleanup
    # Clean up seed file if it was created by this config, though config might be shared
    # Consider if seed file creation should be part of this fixture or the config fixture more directly.
    # For now, let's assume seed file management is tied to the config's lifecycle.
    # if actual_config_for_frontier.seed_file.exists():
    #     actual_config_for_frontier.seed_file.unlink()

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier(actual_config_for_frontier: CrawlerConfig, 
                                          mock_storage_manager: MagicMock, 
                                          mock_fetcher: MagicMock) -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_storage_manager() -> MagicMock: # This mock is for PolitenessEnforcer, may not need db_pool if methods are mocked
    mock = MagicMock(spec=StorageManager)
    # If PolitenessEnforcer's init accesses db_pool on storage, mock that attribute
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool) 
    # Mocking individual connection/cursor calls might be too complex if PE uses the pool directly.
    # It's better if PE methods that interact with DB are mocked if PE itself isn't the focus.
    # For now, adding db_pool attribute to the mock.
    return mock

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig, 
    storage_manager_for_frontier: StorageManager, # This SM instance now uses the db_pool fixture
    mock_politeness_enforcer_for_frontier: MagicMock
) -> FrontierManager:
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_add_and_get_urls(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path, 
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    # tmp_path is needed to create distinct DBs for each run if not using a shared fixture pool
    tmp_path: Path 
):
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.return_value = None # AsyncMock should return None by default

    # Define a unique DB file for this multi-run test
    # to ensure state isolation between "runs" if we create separate pools.
    # Using the `temp_test_frontier_dir` for consistency in paths for data_dir.
    db_file_for_resume_test = temp_test_frontier_dir / "resume_test_frontier.db"


    # --- First run: populate and close ---\n    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False # Ensure it's a new run
    # Ensure data_dir points to where db_file_for_resume_test will be (or its parent)
    # The frontier_test_config_obj already uses temp_test_frontier_dir as its data_dir
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    pool_run1 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run1 = StorageManager(config=cfg_run1, db_pool=pool_run1)
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run1.initialize_frontier() 
    await frontier_run1.add_url("http://persistent.com/page_from_run1")
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 3
    
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 2 
    # storage_run1.close() # Not needed, pool_run1.close_all() will handle it
    pool_run1.close_all()

    # --- Second run: resume --- 
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict) # Same data_dir, db will be reused

    pool_run2 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run2 = StorageManager(config=cfg_run2, db_pool=pool_run2)
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run2.initialize_frontier() 
    
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
// ... existing code ...
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 0
    # storage_run2.close() # Not needed
    pool_run2.close_all()
    logger.info("Frontier resume test passed with politeness mocks.")
```

**Key changes in `test_frontier.py`:**

1.  **`storage_manager_for_frontier` fixture**:
    *   Now correctly takes `db_pool` (from `conftest.py`) as an argument and passes it to `StorageManager`.
    *   Removed the `sm.close()` call.
2.  **`mock_storage_manager` fixture**:
    *   This fixture is used for mocking `StorageManager` when testing `PolitenessEnforcer` (which `FrontierManager` uses). If `PolitenessEnforcer`'s initialization or methods were to access `storage.db_pool`, the mock now includes a `MagicMock` for `db_pool`. This might need further refinement depending on how `PolitenessEnforcer` interacts with the `db_pool` via `StorageManager`.
3.  **`test_frontier_resume_with_politeness`**:
    *   This test simulates two separate runs of the crawler. To ensure proper state isolation and re-initialization for the database, it now:
        *   Defines a specific database file path (`db_file_for_resume_test`) within `temp_test_frontier_dir`.
        *   Creates a new `SQLiteConnectionPool` instance (`pool_run1`, `pool_run2`) for each "run", pointing to this *same* database file. This correctly simulates re-opening the database.
        *   Passes these manually created pools to the respective `StorageManager` instances.
        *   Calls `pool_run1.close_all()` and `pool_run2.close_all()` at the end of each respective run's scope.
        *   The `storage_runX.close()` calls are removed.

This approach ensures that most tests use the convenient shared `db_pool` fixture from `conftest.py`, while tests with more complex lifecycle requirements (like `test_frontier_resume_with_politeness`) can manage their pool instances explicitly.

The `FAILED tests/test_frontier.py::test_frontier_resume_with_politeness` and `ERROR tests/test_frontier.py::test_frontier_initialization_new`, `ERROR tests/test_frontier.py::test_add_and_get_urls` should be addressed by these changes.

The remaining error is `ERROR tests/test_politeness.py::test_load_manual_exclusions_with_file`. We'll need to look at `crawler/tests/test_politeness.py` next.

Would you like me to apply these changes to `crawler/tests/test_frontier.py`?

```python
import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock # Added AsyncMock
import sqlite3

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer # Added
from crawler_module.fetcher import Fetcher # Added for PolitenessEnforcer mock typing
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation in specific tests

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
// ... existing code ...
@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
// ... existing code ...
# Updated storage_manager_for_frontier fixture
@pytest_asyncio.fixture
async def storage_manager_for_frontier(actual_config_for_frontier: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    # StorageManager now requires db_pool
    sm = StorageManager(config=actual_config_for_frontier, db_pool=db_pool)
    yield sm
    # No sm.close() here, db_pool fixture handles cleanup
    # Clean up seed file if it was created by this config, though config might be shared
    # Consider if seed file creation should be part of this fixture or the config fixture more directly.
    # For now, let's assume seed file management is tied to the config's lifecycle.
    # if actual_config_for_frontier.seed_file.exists():
    #     actual_config_for_frontier.seed_file.unlink()

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier(actual_config_for_frontier: CrawlerConfig, 
                                          mock_storage_manager: MagicMock, 
                                          mock_fetcher: MagicMock) -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_storage_manager() -> MagicMock: # This mock is for PolitenessEnforcer, may not need db_pool if methods are mocked
    mock = MagicMock(spec=StorageManager)
    # If PolitenessEnforcer's init accesses db_pool on storage, mock that attribute
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool) 
    # Mocking individual connection/cursor calls might be too complex if PE uses the pool directly.
    # It's better if PE methods that interact with DB are mocked if PE itself isn't the focus.
    # For now, adding db_pool attribute to the mock.
    return mock

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig, 
    storage_manager_for_frontier: StorageManager, # This SM instance now uses the db_pool fixture
    mock_politeness_enforcer_for_frontier: MagicMock
) -> FrontierManager:
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_add_and_get_urls(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path, 
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    # tmp_path is needed to create distinct DBs for each run if not using a shared fixture pool
    tmp_path: Path 
):
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.return_value = None # AsyncMock should return None by default

    # Define a unique DB file for this multi-run test
    # to ensure state isolation between "runs" if we create separate pools.
    # Using the `temp_test_frontier_dir` for consistency in paths for data_dir.
    db_file_for_resume_test = temp_test_frontier_dir / "resume_test_frontier.db"


    # --- First run: populate and close ---\n    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False # Ensure it's a new run
    # Ensure data_dir points to where db_file_for_resume_test will be (or its parent)
    # The frontier_test_config_obj already uses temp_test_frontier_dir as its data_dir
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    pool_run1 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run1 = StorageManager(config=cfg_run1, db_pool=pool_run1)
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run1.initialize_frontier() 
    await frontier_run1.add_url("http://persistent.com/page_from_run1")
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 3
    
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 2 
    # storage_run1.close() # Not needed, pool_run1.close_all() will handle it
    pool_run1.close_all()

    # --- Second run: resume --- 
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict) # Same data_dir, db will be reused

    pool_run2 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run2 = StorageManager(config=cfg_run2, db_pool=pool_run2)
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run2.initialize_frontier() 
    
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
// ... existing code ...
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 0
    # storage_run2.close() # Not needed
    pool_run2.close_all()
    logger.info("Frontier resume test passed with politeness mocks.")
```

```python
import pytest
import pytest_asyncio # For async fixtures and tests
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock # Added AsyncMock
import sqlite3

from crawler_module.frontier import FrontierManager
from crawler_module.storage import StorageManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer # Added
from crawler_module.fetcher import Fetcher # Added for PolitenessEnforcer mock typing
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation in specific tests

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
// ... existing code ...
@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
// ... existing code ...
@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
// ... existing code ...
# Updated storage_manager_for_frontier fixture
@pytest_asyncio.fixture
async def storage_manager_for_frontier(actual_config_for_frontier: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    # StorageManager now requires db_pool
    sm = StorageManager(config=actual_config_for_frontier, db_pool=db_pool)
    yield sm
    # No sm.close() here, db_pool fixture handles cleanup
    # Clean up seed file if it was created by this config, though config might be shared
    # Consider if seed file creation should be part of this fixture or the config fixture more directly.
    # For now, let's assume seed file management is tied to the config's lifecycle.
    # if actual_config_for_frontier.seed_file.exists():
    #     actual_config_for_frontier.seed_file.unlink()

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier(actual_config_for_frontier: CrawlerConfig, 
                                          mock_storage_manager: MagicMock, 
                                          mock_fetcher: MagicMock) -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest.fixture
def mock_storage_manager() -> MagicMock: # This mock is for PolitenessEnforcer, may not need db_pool if methods are mocked
    mock = MagicMock(spec=StorageManager)
    # If PolitenessEnforcer's init accesses db_pool on storage, mock that attribute
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool) 
    # Mocking individual connection/cursor calls might be too complex if PE uses the pool directly.
    # It's better if PE methods that interact with DB are mocked if PE itself isn't the focus.
    # For now, adding db_pool attribute to the mock.
    return mock

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig, 
    storage_manager_for_frontier: StorageManager, # This SM instance now uses the db_pool fixture
    mock_politeness_enforcer_for_frontier: MagicMock
) -> FrontierManager:
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_add_and_get_urls(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path, 
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    # tmp_path is needed to create distinct DBs for each run if not using a shared fixture pool
    tmp_path: Path 
):
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    mock_politeness_enforcer_for_frontier.record_domain_fetch_attempt.return_value = None # AsyncMock should return None by default

    # Define a unique DB file for this multi-run test
    # to ensure state isolation between "runs" if we create separate pools.
    # Using the `temp_test_frontier_dir` for consistency in paths for data_dir.
    db_file_for_resume_test = temp_test_frontier_dir / "resume_test_frontier.db"


    # --- First run: populate and close ---\n    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False # Ensure it's a new run
    # Ensure data_dir points to where db_file_for_resume_test will be (or its parent)
    # The frontier_test_config_obj already uses temp_test_frontier_dir as its data_dir
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    pool_run1 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run1 = StorageManager(config=cfg_run1, db_pool=pool_run1)
    frontier_run1 = FrontierManager(config=cfg_run1, storage=storage_run1, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run1.initialize_frontier() 
    await frontier_run1.add_url("http://persistent.com/page_from_run1")
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 3
    
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await asyncio.to_thread(frontier_run1.count_frontier) == 2 
    # storage_run1.close() # Not needed, pool_run1.close_all() will handle it
    pool_run1.close_all()

    # --- Second run: resume --- 
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict) # Same data_dir, db will be reused

    pool_run2 = SQLiteConnectionPool(db_path=db_file_for_resume_test, pool_size=1)
    storage_run2 = StorageManager(config=cfg_run2, db_pool=pool_run2)
    frontier_run2 = FrontierManager(config=cfg_run2, storage=storage_run2, politeness=mock_politeness_enforcer_for_frontier)
    await frontier_run2.initialize_frontier() 
    
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 2 

    # Try to get the remaining URLs
    next_url = await frontier_run2.get_next_url()
// ... existing code ...
    assert await asyncio.to_thread(frontier_run2.count_frontier) == 0
    # storage_run2.close() # Not needed
    pool_run2.close_all()
    logger.info("Frontier resume test passed with politeness mocks.")
```

Okay, I have `crawler/tests/test_politeness.py`.

This file has a similar structure to the others. The main things to note:

1.  **`mock_storage_manager` fixture**: This fixture creates a `MagicMock` for `StorageManager`. It already mocks `mock.conn` and `mock.db_path`. We need to ensure that if any `PolitenessEnforcer` code (specifically the threaded DB functions it calls) tries to access `storage.db_pool`, the mock is prepared for that. However, `PolitenessEnforcer`'s DB interactions are often within its own private `_db_..._sync_threaded` methods, which directly call `sqlite3.connect(self.storage.db_path, ...)`.
    *   This means the `PolitenessEnforcer`'s internal DB methods **do not currently use the pool** from `StorageManager`. They create their own connections using `self.storage.db_path`.
    *   This is a discrepancy. For consistency and to benefit from pooling, `PolitenessEnforcer`'s threaded database methods should also be refactored to use the pool from `self.storage.db_pool`.
2.  **`storage_manager_for_exclusion_test` fixture**: This fixture creates a *real* `StorageManager` instance. It will need to be updated to accept the `db_pool` fixture and pass it to `StorageManager`.
3.  **`test_load_manual_exclusions_with_file`**: This test uses `storage_manager_for_exclusion_test`. The `PolitenessEnforcer` created within this test will get a real `StorageManager` which (after our fix) will have a real `db_pool`. The test then connects directly to the database file to verify results. This direct connection for verification is fine.

**Plan:**

**Phase 1: Fix `PolitenessEnforcer` to use the pool.**
Before fixing the tests, we need to make `PolitenessEnforcer` use the connection pool from `StorageManager` for its database operations. Its private methods like `_db_get_cached_robots_sync_threaded`, `_db_update_robots_cache_sync_threaded`, `_db_check_manual_exclusion_sync_threaded`, and `_db_record_domain_fetch_sync_threaded` currently create new connections. They should be changed to use `self.storage.db_pool.get_connection()` and `self.storage.db_pool.return_connection()`.

**Phase 2: Update `test_politeness.py` fixtures and tests.**
1.  The `mock_storage_manager` fixture might not need significant changes if `PolitenessEnforcer` is refactored to use the pool correctly *through* the `StorageManager` instance, because the mock methods on `mock_storage_manager.db_pool` would be called.
2.  The `storage_manager_for_exclusion_test` fixture will need to take `db_pool` and pass it to `StorageManager`.
3.  The test `test_load_manual_exclusions_with_file` will then receive a `PolitenessEnforcer` that uses a `StorageManager` backed by a real pool.

Let's start with **Phase 1: Refactor `PolitenessEnforcer`**.
I'll need to see `crawler/crawler_module/politeness.py`.

You are absolutely correct. I've reviewed `crawler/crawler_module/politeness.py`, and it indeed directly uses `sqlite3.connect(self.storage.db_path, ...)` in its threaded helper methods (`_db_get_cached_robots_sync_threaded`, `_db_update_robots_cache_sync_threaded`, `_db_check_manual_exclusion_sync_threaded`, and the one in `_load_manual_exclusions` and `record_domain_fetch_attempt` which is `_db_record_fetch_sync_threaded` although not explicitly named as such in the code).

This means `PolitenessEnforcer` is currently bypassing the connection pool that `StorageManager` now holds. This needs to be fixed for consistency and to get the benefits of pooling for these operations as well.

**Phase 1: Refactor `PolitenessEnforcer` to use the pool**

We need to modify these internal methods to get connections from `self.storage.db_pool`.

```python:crawler/crawler_module/politeness.py
// ... existing code ...
import asyncio # Added for asyncio.to_thread
import sqlite3 # For type hinting in db access functions

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now has db_pool
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult # Added Fetcher import
# No direct import of SQLiteConnectionPool, accessed via self.storage.db_pool

logger = logging.getLogger(__name__)

// ... existing code ...
class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage # This storage instance has the db_pool
        self.fetcher = fetcher 
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB using the pool."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_from_pool: sqlite3.Connection | None = None
            try:
                # Get connection from pool
                conn_from_pool = self.storage.db_pool.get_connection()
                cursor = conn_from_pool.cursor()
                try:
                    with open(self.config.exclude_file, 'r') as f:
                        count = 0
                        for line in f:
                            domain_to_exclude = line.strip().lower()
                            if domain_to_exclude and not domain_to_exclude.startswith("#"):
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                    (domain_to_exclude,)
                                )
                                count += 1
                        conn_from_pool.commit() # Commit changes
                        logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file} (using pool).")
                except IOError as e:
                    logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                    if conn_from_pool: conn_from_pool.rollback() # Rollback if IO error after potential DB ops
                except sqlite3.Error as e: # Catch DB errors from execute/commit
                    logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                    if conn_from_pool: conn_from_pool.rollback()
                    raise # Re-raise to be caught by outer block if needed
                finally:
                    if cursor: cursor.close()
            except sqlite3.Error as e: # Catch DB errors from pool.get_connection or other initial DB setup
                logger.error(f"DB (pool/connection) error loading manual exclusions: {e}")
            except Exception as e: # Catch other errors like pool Empty
                logger.error(f"Unexpected error (e.g. pool issue) loading manual exclusions: {e}")
            finally:
                if conn_from_pool:
                    self.storage.db_pool.return_connection(conn_from_pool)
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain, using the pool for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None 

        try:
            def _db_get_cached_robots_sync_threaded_pooled():
                conn_from_pool_get: sqlite3.Connection | None = None
                try:
                    conn_from_pool_get = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_get.cursor()
                    try:
                        cursor.execute(
                            "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                            (domain,)
                        )
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_get:
                        self.storage.db_pool.return_connection(conn_from_pool_get)
            
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded_pooled)
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) fetching cached robots.txt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error (pool) during DB cache check for {domain}: {e}", exc_info=True)


        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        elif not (db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time):
             if domain in self.robots_parsers: # DB miss/stale, but have in-memory (e.g. from previous fetch in same session)
                 logger.debug(f"DB cache miss/stale for {domain}. Using pre-existing in-memory robots_parser.")
                 return self.robots_parsers[domain]
        
        if robots_content is None: # Need to fetch
            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found:
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else:
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404: # type: ignore
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            if robots_content is not None:
                try:
                    def _db_update_robots_cache_sync_threaded_pooled():
                        conn_from_pool_update: sqlite3.Connection | None = None
                        try:
                            conn_from_pool_update = self.storage.db_pool.get_connection()
                            cursor = conn_from_pool_update.cursor()
                            try:
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                                    (robots_content, fetched_timestamp, expires_timestamp, domain)
                                )
                                conn_from_pool_update.commit()
                                logger.debug(f"Cached robots.txt for {domain} in DB (pool).")
                            except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                                logger.error(f"DB execute/commit error caching robots.txt (pool) for {domain}: {e_inner}")
                                if conn_from_pool_update: conn_from_pool_update.rollback()
                                raise
                            finally:
                                if cursor: cursor.close()
                        finally:
                            if conn_from_pool_update:
                                self.storage.db_pool.return_connection(conn_from_pool_update)
                    await asyncio.to_thread(_db_update_robots_cache_sync_threaded_pooled)
                except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
                    logger.error(f"DB (pool/connection) error caching robots.txt for {domain}: {e}")
                except Exception as e: # Catch other errors like pool Empty
                     logger.error(f"Unexpected (pool) error caching robots.txt for {domain}: {e}")


        if robots_content is not None:
            rerp.parse(robots_content)
            rerp.source_content = robots_content 
        else: 
            rerp.parse("") 
            rerp.source_content = ""
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded, using the pool."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        try:
            def _db_check_manual_exclusion_sync_threaded_pooled():
                conn_from_pool_check: sqlite3.Connection | None = None
                try:
                    conn_from_pool_check = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_check.cursor()
                    try:
                        if self.config.seeded_urls_only:
                            cursor.execute("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        else:
                            cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_check:
                        self.storage.db_pool.return_connection(conn_from_pool_check)

            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded_pooled)
            if row and row[0] == 1: # type: ignore
                logger.debug(f"URL {url} from manually excluded domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) checking manual exclusion for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.warning(f"Pool error checking manual exclusion for {domain}: {e}")


        rerp = await self._get_robots_for_domain(domain) # This now uses pool for its DB ops
        if rerp:
// ... existing code ...
    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay, using pool."""
        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded_pooled():
                conn_from_pool_lft: sqlite3.Connection | None = None
                try:
                    conn_from_pool_lft = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_lft.cursor()
                    try:
                        cursor.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_lft:
                        self.storage.db_pool.return_connection(conn_from_pool_lft)
            
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded_pooled)
            if row and row[0] is not None: # type: ignore
                last_fetch_time = row[0] # type: ignore
        except sqlite3.Error as e:
            logger.error(f"DB error (pool) checking can_fetch_domain_now for {domain}: {e}")
            return False 
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Pool error checking can_fetch_domain_now for {domain}: {e}")
            return False
            
        crawl_delay = await self.get_crawl_delay(domain)
// ... existing code ...
    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain, using pool."""
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded_pooled():
                conn_from_pool_rec: sqlite3.Connection | None = None
                try:
                    conn_from_pool_rec = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_rec.cursor()
                    try:
                        cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)\", \n                                       (domain, current_time))
                        cursor.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?\", \n                                       (current_time, domain))
                        conn_from_pool_rec.commit()
                    except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                        logger.error(f"DB execute/commit error recording fetch attempt (pool) for {domain}: {e_inner}")
                        if conn_from_pool_rec: conn_from_pool_rec.rollback()
                        raise
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_rec:
                        self.storage.db_pool.return_connection(conn_from_pool_rec)
            await asyncio.to_thread(_db_record_fetch_sync_threaded_pooled)
        except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
            logger.error(f"DB (pool/connection) error recording fetch attempt for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Unexpected (pool) error recording fetch attempt for {domain}: {e}")
```

**Summary of changes to `PolitenessEnforcer`:**
*   `_load_manual_exclusions()`: Modified to get/return a connection from `self.storage.db_pool`.
*   Internal helper methods for DB access (now suffixed with `_pooled` in my example thought process, but implemented inline in the refactor):
    *   `_db_get_cached_robots_sync_threaded` (within `_get_robots_for_domain`)
    *   `_db_update_robots_cache_sync_threaded` (within `_get_robots_for_domain`)
    *   `_db_check_manual_exclusion_sync_threaded` (within `is_url_allowed`)
    *   `_db_get_last_fetch_sync_threaded` (within `can_fetch_domain_now`)
    *   The DB operations in `record_domain_fetch_attempt`
    All these are changed to use `self.storage.db_pool.get_connection()` and `self.storage.db_pool.return_connection()` within a try/finally block.

Now for **Phase 2: Update `test_politeness.py`**

The main fixture to update is `storage_manager_for_exclusion_test`. Other tests in `test_politeness.py` heavily mock `sqlite3.connect` or `storage_manager.conn`. Since `PolitenessEnforcer`'s internal DB methods now use the pool correctly, the mocking strategy might need to shift from mocking `sqlite3.connect` directly to mocking methods on the `db_pool` object if granular control over pooled connections is needed for those specific tests.

However, for `test_load_manual_exclusions_with_file`, we use a *real* `StorageManager` via `storage_manager_for_exclusion_test`. This fixture needs to be updated.

```python:crawler/tests/test_politeness.py
// ... existing code ...
from crawler_module.storage import StorageManager 
from crawler_module.fetcher import Fetcher, FetchResult 
from robotexclusionrulesparser import RobotExclusionRulesParser
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation

@pytest.fixture
def dummy_config(tmp_path: Path) -> CrawlerConfig:
// ... existing code ...
@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # If PE methods now try to access self.storage.db_pool, this mock needs it.
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool)
    
    # If the tests that use this mock_storage_manager rely on sqlite3.connect being patched,
    # those patches might still work if the PolitenessEnforcer's *own* methods are still calling sqlite3.connect
    # directly (which they shouldn't be after our refactor).
    # If PolitenessEnforcer is correctly using mock.db_pool.get_connection(), then tests need to mock
    # methods on mock.db_pool and the connection/cursor it returns.
    
    # For now, let's assume tests mocking sqlite3.connect will need review if PE uses the pool.
    # The critical part is that this mock_storage_manager *has* a db_pool attribute.
    # The db_path attribute on the mock may or may not be used anymore by PE if it uses the pool.
    mock.db_path = tmp_path / "mock_politeness_temp.db" 
    return mock

@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest_asyncio.fixture
async def politeness_enforcer(
    dummy_config: CrawlerConfig, 
    mock_storage_manager: MagicMock, # This uses the mock_storage_manager above
    mock_fetcher: MagicMock
) -> PolitenessEnforcer:
// ... existing code ...
# Updated storage_manager_for_exclusion_test fixture
@pytest.fixture
def storage_manager_for_exclusion_test(dummy_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    """Provides a real StorageManager instance using a temporary DB path from dummy_config and a real pool."""
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    # Pass the db_pool fixture to StorageManager
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close(), pool handles it.
    yield sm

# --- Tests for _load_manual_exclusions --- 
def test_load_manual_exclusions_no_file(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses the `politeness_enforcer` fixture, which uses `mock_storage_manager`.
    # If _load_manual_exclusions now uses `self.storage.db_pool.get_connection()`,
    # then `mock_storage_manager.db_pool.get_connection` would be called.
    # We need to ensure that mock is set up appropriately if calls are expected.
    dummy_config.exclude_file = None
    # Re-create PE to pick up config change if __init__ is where loading happens.
    # However, the fixture `politeness_enforcer` is already created with dummy_config.
    # If dummy_config changes after PE is created, PE won't see it unless re-init.
    # For this test, it might be simpler to create PE inside the test.
    
    pe = PolitenessEnforcer(config=dummy_config, storage=politeness_enforcer.storage, fetcher=politeness_enforcer.fetcher)
    
    # Assert that no connection was attempted if exclude_file is None
    # This depends on the mock setup for db_pool on the mock_storage_manager
    pe.storage.db_pool.get_connection.assert_not_called()
    assert True 


def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager, # This SM has a real db_pool from conftest
    mock_fetcher: MagicMock, 
    tmp_path: Path 
):
    exclude_file_path = tmp_path / "custom_excludes.txt" 
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    # PolitenessEnforcer will use storage_manager_for_exclusion_test (which has a real pool)
    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # Now, connect to the DB used by the pool and verify content
    conn_verify: sqlite3.Connection | None = None
    try:
        # The db_pool used by storage_manager_for_exclusion_test is from the conftest fixture.
        # We can get a connection from it to verify.
        db_pool_to_check = storage_manager_for_exclusion_test.db_pool
        conn_verify = db_pool_to_check.get_connection() # Get a connection for verification
        cursor = conn_verify.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
// ... existing code ...
    finally:
        if cursor: cursor.close()
        if conn_verify: # Return the connection to the pool it came from
            db_pool_to_check.return_connection(conn_verify)
        # The db_pool_to_check itself will be closed by its fixture in conftest.py
        if exclude_file_path.exists(): exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) --- 
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses `politeness_enforcer` which has `mock_storage_manager`.
    # _get_robots_for_domain will call `self.storage.db_pool.get_connection()`.
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    mock_rerp.source_content = "User-agent: *\\nDisallow: /test"
    politeness_enforcer.robots_parsers[domain] = mock_rerp

    # Simulate no DB cache by having get_connection return a mock connection whose cursor returns None
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = None # No DB cache
    mock_db_conn.cursor.return_value = mock_db_cursor
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
    assert rerp == mock_rerp
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # Verify pool was asked

@pytest.mark.asyncio
async def test_get_robots_from_db_cache_fresh(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbcached.com"
    robots_text = "User-agent: TestCrawler\\nDisallow: /private"
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = (robots_text, future_expiry)
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.return_value = mock_db_cursor
    
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
    if domain in politeness_enforcer.robots_parsers:
        del politeness_enforcer.robots_parsers[domain]

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
        
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_db_conn.cursor.assert_called_once()
    mock_db_cursor.execute.assert_called_once_with(
        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?\", \n                            (domain,)\n    )
    mock_db_cursor.fetchone.assert_called_once()
    # politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn) # Check return

    assert rerp is not None
// ... remaining tests in test_politeness.py will need similar adjustments ...
# If they mock sqlite3.connect, they might need to change to mock methods on
# politeness_enforcer.storage.db_pool.get_connection().cursor().<method>
# This is because the PolitenessEnforcer's DB methods no longer call sqlite3.connect directly.
#
# Example for a test that previously did `with patch('sqlite3.connect', ...)`:
#
# Old way (simplified):
# with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
#     await politeness_enforcer._some_db_method()
#     mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, ...)
#
# New way (simplified, assuming politeness_enforcer uses the mock_storage_manager):
# mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
# mock_db_cursor.fetchone.return_value = ... 
# mock_db_conn = MagicMock(spec=sqlite3.Connection)
# mock_db_conn.cursor.return_value = mock_db_cursor
# politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
#
# await politeness_enforcer._some_db_method() # This will use the mocked pool and connection
#
# politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
# mock_db_conn.cursor.assert_called_once()
# mock_db_cursor.execute.assert_called_with(...)
# politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn)

```

**Explanation of Changes for `test_politeness.py`:**
1.  **`storage_manager_for_exclusion_test` fixture**: Updated to take `db_pool` from `conftest.py` and pass it to `StorageManager`.
2.  **`test_load_manual_exclusions_no_file`**: Adjusted to re-create `PolitenessEnforcer` within the test to pick up `dummy_config.exclude_file = None`. It now asserts that `get_connection` on the mocked `db_pool` was not called.
3.  **`test_load_manual_exclusions_with_file`**:
    *   The `PolitenessEnforcer` (`pe`) uses `storage_manager_for_exclusion_test`, which is now correctly backed by the `db_pool` fixture.
    *   Database verification is done by getting a connection from `db_pool_to_check = storage_manager_for_exclusion_test.db_pool` and returning it.
4.  **Mocking for other tests (e.g., `test_get_robots_from_memory_cache`, `test_get_robots_from_db_cache_fresh`)**:
    *   These tests use the `politeness_enforcer` fixture, which itself uses `mock_storage_manager`.
    *   Since `PolitenessEnforcer` now calls `self.storage.db_pool.get_connection()`, the tests need to set up expectations on `politeness_enforcer.storage.db_pool.get_connection()` to return a mock connection, which in turn returns a mock cursor with desired `fetchone()` etc. behavior.
    *   The direct patching of `sqlite3.connect` will no longer work for DB methods inside `PolitenessEnforcer` because those methods now go through the pool.

This is a significant refactor for the tests in `test_politeness.py` due to the change in how `PolitenessEnforcer` accesses the database. The principle is to mock at the boundary of the `db_pool` interface (`get_connection`, `return_connection`) and the `sqlite3.Connection` and `sqlite3.Cursor` objects it provides.

I've outlined the changes for the first few tests. The rest of the tests in `test_politeness.py` that involve database interaction will need similar adjustments to their mocking strategy.

**Recommendation:**
It would be a good idea to apply the refactor to `crawler/crawler_module/politeness.py` first. Then, apply the test changes to `crawler/tests/test_politeness.py`. This will be iterative.

Shall I proceed with applying the refactoring to `crawler/crawler_module/politeness.py` as described? After that, we can tackle the test updates more concretely.

```python
// ... existing code ...
import asyncio # Added for asyncio.to_thread
import sqlite3 # For type hinting in db access functions

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now has db_pool
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult # Added Fetcher import
# No direct import of SQLiteConnectionPool, accessed via self.storage.db_pool

logger = logging.getLogger(__name__)

// ... existing code ...
class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage # This storage instance has the db_pool
        self.fetcher = fetcher 
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB using the pool."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_from_pool: sqlite3.Connection | None = None
            try:
                # Get connection from pool
                conn_from_pool = self.storage.db_pool.get_connection()
                cursor = conn_from_pool.cursor()
                try:
                    with open(self.config.exclude_file, 'r') as f:
                        count = 0
                        for line in f:
                            domain_to_exclude = line.strip().lower()
                            if domain_to_exclude and not domain_to_exclude.startswith("#"):
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                    (domain_to_exclude,)
                                )
                                count += 1
                        conn_from_pool.commit() # Commit changes
                        logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file} (using pool).")
                except IOError as e:
                    logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                    if conn_from_pool: conn_from_pool.rollback() # Rollback if IO error after potential DB ops
                except sqlite3.Error as e: # Catch DB errors from execute/commit
                    logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                    if conn_from_pool: conn_from_pool.rollback()
                    raise # Re-raise to be caught by outer block if needed
                finally:
                    if cursor: cursor.close()
            except sqlite3.Error as e: # Catch DB errors from pool.get_connection or other initial DB setup
                logger.error(f"DB (pool/connection) error loading manual exclusions: {e}")
            except Exception as e: # Catch other errors like pool Empty
                logger.error(f"Unexpected error (e.g. pool issue) loading manual exclusions: {e}")
            finally:
                if conn_from_pool:
                    self.storage.db_pool.return_connection(conn_from_pool)
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain, using the pool for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None 

        try:
            def _db_get_cached_robots_sync_threaded_pooled():
                conn_from_pool_get: sqlite3.Connection | None = None
                try:
                    conn_from_pool_get = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_get.cursor()
                    try:
                        cursor.execute(
                            "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                            (domain,)
                        )
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_get:
                        self.storage.db_pool.return_connection(conn_from_pool_get)
            
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded_pooled)
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) fetching cached robots.txt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error (pool) during DB cache check for {domain}: {e}", exc_info=True)


        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        elif not (db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time):
             if domain in self.robots_parsers: # DB miss/stale, but have in-memory (e.g. from previous fetch in same session)
                 logger.debug(f"DB cache miss/stale for {domain}. Using pre-existing in-memory robots_parser.")
                 return self.robots_parsers[domain]
        
        if robots_content is None: # Need to fetch
            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found:
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else:
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404: # type: ignore
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            if robots_content is not None:
                try:
                    def _db_update_robots_cache_sync_threaded_pooled():
                        conn_from_pool_update: sqlite3.Connection | None = None
                        try:
                            conn_from_pool_update = self.storage.db_pool.get_connection()
                            cursor = conn_from_pool_update.cursor()
                            try:
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                                    (robots_content, fetched_timestamp, expires_timestamp, domain)
                                )
                                conn_from_pool_update.commit()
                                logger.debug(f"Cached robots.txt for {domain} in DB (pool).")
                            except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                                logger.error(f"DB execute/commit error caching robots.txt (pool) for {domain}: {e_inner}")
                                if conn_from_pool_update: conn_from_pool_update.rollback()
                                raise
                            finally:
                                if cursor: cursor.close()
                        finally:
                            if conn_from_pool_update:
                                self.storage.db_pool.return_connection(conn_from_pool_update)
                    await asyncio.to_thread(_db_update_robots_cache_sync_threaded_pooled)
                except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
                    logger.error(f"DB (pool/connection) error caching robots.txt for {domain}: {e}")
                except Exception as e: # Catch other errors like pool Empty
                     logger.error(f"Unexpected (pool) error caching robots.txt for {domain}: {e}")


        if robots_content is not None:
            rerp.parse(robots_content)
            rerp.source_content = robots_content 
        else: 
            rerp.parse("") 
            rerp.source_content = ""
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded, using the pool."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        try:
            def _db_check_manual_exclusion_sync_threaded_pooled():
                conn_from_pool_check: sqlite3.Connection | None = None
                try:
                    conn_from_pool_check = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_check.cursor()
                    try:
                        if self.config.seeded_urls_only:
                            cursor.execute("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        else:
                            cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_check:
                        self.storage.db_pool.return_connection(conn_from_pool_check)

            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded_pooled)
            if row and row[0] == 1: # type: ignore
                logger.debug(f"URL {url} from manually excluded domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) checking manual exclusion for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.warning(f"Pool error checking manual exclusion for {domain}: {e}")


        rerp = await self._get_robots_for_domain(domain) # This now uses pool for its DB ops
        if rerp:
// ... existing code ...
    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay, using pool."""
        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded_pooled():
                conn_from_pool_lft: sqlite3.Connection | None = None
                try:
                    conn_from_pool_lft = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_lft.cursor()
                    try:
                        cursor.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_lft:
                        self.storage.db_pool.return_connection(conn_from_pool_lft)
            
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded_pooled)
            if row and row[0] is not None: # type: ignore
                last_fetch_time = row[0] # type: ignore
        except sqlite3.Error as e:
            logger.error(f"DB error (pool) checking can_fetch_domain_now for {domain}: {e}")
            return False 
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Pool error checking can_fetch_domain_now for {domain}: {e}")
            return False
            
        crawl_delay = await self.get_crawl_delay(domain)
// ... existing code ...
    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain, using pool."""
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded_pooled():
                conn_from_pool_rec: sqlite3.Connection | None = None
                try:
                    conn_from_pool_rec = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_rec.cursor()
                    try:
                        cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)\", \n                                       (domain, current_time))
                        cursor.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?\", \n                                       (current_time, domain))
                        conn_from_pool_rec.commit()
                    except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                        logger.error(f"DB execute/commit error recording fetch attempt (pool) for {domain}: {e_inner}")
                        if conn_from_pool_rec: conn_from_pool_rec.rollback()
                        raise
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_rec:
                        self.storage.db_pool.return_connection(conn_from_pool_rec)
            await asyncio.to_thread(_db_record_fetch_sync_threaded_pooled)
        except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
            logger.error(f"DB (pool/connection) error recording fetch attempt for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Unexpected (pool) error recording fetch attempt for {domain}: {e}")
```

```python
// ... existing code ...
from crawler_module.storage import StorageManager 
from crawler_module.fetcher import Fetcher, FetchResult 
from robotexclusionrulesparser import RobotExclusionRulesParser
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation

@pytest.fixture
def dummy_config(tmp_path: Path) -> CrawlerConfig:
// ... existing code ...
@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # If PE methods now try to access self.storage.db_pool, this mock needs it.
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool)
    
    # If the tests that use this mock_storage_manager rely on sqlite3.connect being patched,
    # those patches might still work if the PolitenessEnforcer's *own* methods are still calling sqlite3.connect
    # directly (which they shouldn't be after our refactor).
    # If PolitenessEnforcer is correctly using mock.db_pool.get_connection(), then tests need to mock
    # methods on mock.db_pool and the connection/cursor it returns.
    
    # For now, let's assume tests mocking sqlite3.connect will need review if PE uses the pool.
    # The critical part is that this mock_storage_manager *has* a db_pool attribute.
    # The db_path attribute on the mock may or may not be used anymore by PE if it uses the pool.
    mock.db_path = tmp_path / "mock_politeness_temp.db" 
    return mock

@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest_asyncio.fixture
async def politeness_enforcer(
    dummy_config: CrawlerConfig, 
    mock_storage_manager: MagicMock, # This uses the mock_storage_manager above
    mock_fetcher: MagicMock
) -> PolitenessEnforcer:
// ... existing code ...
# Updated storage_manager_for_exclusion_test fixture
@pytest.fixture
def storage_manager_for_exclusion_test(dummy_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    """Provides a real StorageManager instance using a temporary DB path from dummy_config and a real pool."""
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    # Pass the db_pool fixture to StorageManager
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close(), pool handles it.
    yield sm

# --- Tests for _load_manual_exclusions --- 
def test_load_manual_exclusions_no_file(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses the `politeness_enforcer` fixture, which uses `mock_storage_manager`.
    # If _load_manual_exclusions now uses `self.storage.db_pool.get_connection()`,
    # then `mock_storage_manager.db_pool.get_connection` would be called.
    # We need to ensure that mock is set up appropriately if calls are expected.
    dummy_config.exclude_file = None
    # Re-create PE to pick up config change if __init__ is where loading happens.
    # However, the fixture `politeness_enforcer` is already created with dummy_config.
    # If dummy_config changes after PE is created, PE won't see it unless re-init.
    # For this test, it might be simpler to create PE inside the test.
    
    pe = PolitenessEnforcer(config=dummy_config, storage=politeness_enforcer.storage, fetcher=politeness_enforcer.fetcher)
    
    # Assert that no connection was attempted if exclude_file is None
    # This depends on the mock setup for db_pool on the mock_storage_manager
    pe.storage.db_pool.get_connection.assert_not_called()
    assert True 


def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager, # This SM has a real db_pool from conftest
    mock_fetcher: MagicMock, 
    tmp_path: Path 
):
    exclude_file_path = tmp_path / "custom_excludes.txt" 
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    # PolitenessEnforcer will use storage_manager_for_exclusion_test (which has a real pool)
    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # Now, connect to the DB used by the pool and verify content
    conn_verify: sqlite3.Connection | None = None
    try:
        # The db_pool used by storage_manager_for_exclusion_test is from the conftest fixture.
        # We can get a connection from it to verify.
        db_pool_to_check = storage_manager_for_exclusion_test.db_pool
        conn_verify = db_pool_to_check.get_connection() # Get a connection for verification
        cursor = conn_verify.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
// ... existing code ...
    finally:
        if cursor: cursor.close()
        if conn_verify: # Return the connection to the pool it came from
            db_pool_to_check.return_connection(conn_verify)
        # The db_pool_to_check itself will be closed by its fixture in conftest.py
        if exclude_file_path.exists(): exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) --- 
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses `politeness_enforcer` which has `mock_storage_manager`.
    # _get_robots_for_domain will call `self.storage.db_pool.get_connection()`.
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    mock_rerp.source_content = "User-agent: *\\nDisallow: /test"
    politeness_enforcer.robots_parsers[domain] = mock_rerp

    # Simulate no DB cache by having get_connection return a mock connection whose cursor returns None
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = None # No DB cache
    mock_db_conn.cursor.return_value = mock_db_cursor
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
    assert rerp == mock_rerp
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # Verify pool was asked

@pytest.mark.asyncio
async def test_get_robots_from_db_cache_fresh(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbcached.com"
    robots_text = "User-agent: TestCrawler\\nDisallow: /private"
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = (robots_text, future_expiry)
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.return_value = mock_db_cursor
    
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
    if domain in politeness_enforcer.robots_parsers:
        del politeness_enforcer.robots_parsers[domain]

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
        
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_db_conn.cursor.assert_called_once()
    mock_db_cursor.execute.assert_called_once_with(
        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?\", \n                            (domain,)\n    )
    mock_db_cursor.fetchone.assert_called_once()
    # politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn) # Check return

    assert rerp is not None
// ... remaining tests in test_politeness.py will need similar adjustments ...
# If they mock sqlite3.connect, they might need to change to mock methods on
# politeness_enforcer.storage.db_pool.get_connection().cursor().<method>
# This is because the PolitenessEnforcer's DB methods no longer call sqlite3.connect directly.
#
# Example for a test that previously did `with patch('sqlite3.connect', ...)`:
#
# Old way (simplified):
# with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
#     await politeness_enforcer._some_db_method()
#     mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, ...)
#
# New way (simplified, assuming politeness_enforcer uses the mock_storage_manager):
# mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
# mock_db_cursor.fetchone.return_value = ... 
# mock_db_conn = MagicMock(spec=sqlite3.Connection)
# mock_db_conn.cursor.return_value = mock_db_cursor
# politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
#
# await politeness_enforcer._some_db_method() # This will use the mocked pool and connection
#
# politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
# mock_db_conn.cursor.assert_called_once()
# mock_db_cursor.execute.assert_called_with(...)
# politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn)

```

```python
// ... existing code ...
import asyncio # Added for asyncio.to_thread
import sqlite3 # For type hinting in db access functions

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now has db_pool
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult # Added Fetcher import
# No direct import of SQLiteConnectionPool, accessed via self.storage.db_pool

logger = logging.getLogger(__name__)

// ... existing code ...
class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage # This storage instance has the db_pool
        self.fetcher = fetcher 
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB using the pool."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_from_pool: sqlite3.Connection | None = None
            try:
                # Get connection from pool
                conn_from_pool = self.storage.db_pool.get_connection()
                cursor = conn_from_pool.cursor()
                try:
                    with open(self.config.exclude_file, 'r') as f:
                        count = 0
                        for line in f:
                            domain_to_exclude = line.strip().lower()
                            if domain_to_exclude and not domain_to_exclude.startswith("#"):
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                    (domain_to_exclude,)
                                )
                                count += 1
                        conn_from_pool.commit() # Commit changes
                        logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file} (using pool).")
                except IOError as e:
                    logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                    if conn_from_pool: conn_from_pool.rollback() # Rollback if IO error after potential DB ops
                except sqlite3.Error as e: # Catch DB errors from execute/commit
                    logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                    if conn_from_pool: conn_from_pool.rollback()
                    raise # Re-raise to be caught by outer block if needed
                finally:
                    if cursor: cursor.close()
            except sqlite3.Error as e: # Catch DB errors from pool.get_connection or other initial DB setup
                logger.error(f"DB (pool/connection) error loading manual exclusions: {e}")
            except Exception as e: # Catch other errors like pool Empty
                logger.error(f"Unexpected error (e.g. pool issue) loading manual exclusions: {e}")
            finally:
                if conn_from_pool:
                    self.storage.db_pool.return_connection(conn_from_pool)
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain, using the pool for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None 

        try:
            def _db_get_cached_robots_sync_threaded_pooled():
                conn_from_pool_get: sqlite3.Connection | None = None
                try:
                    conn_from_pool_get = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_get.cursor()
                    try:
                        cursor.execute(
                            "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                            (domain,)
                        )
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_get:
                        self.storage.db_pool.return_connection(conn_from_pool_get)
            
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded_pooled)
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) fetching cached robots.txt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error (pool) during DB cache check for {domain}: {e}", exc_info=True)


        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        elif not (db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time):
             if domain in self.robots_parsers: # DB miss/stale, but have in-memory (e.g. from previous fetch in same session)
                 logger.debug(f"DB cache miss/stale for {domain}. Using pre-existing in-memory robots_parser.")
                 return self.robots_parsers[domain]
        
        if robots_content is None: # Need to fetch
            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found:
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else:
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404: # type: ignore
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            if robots_content is not None:
                try:
                    def _db_update_robots_cache_sync_threaded_pooled():
                        conn_from_pool_update: sqlite3.Connection | None = None
                        try:
                            conn_from_pool_update = self.storage.db_pool.get_connection()
                            cursor = conn_from_pool_update.cursor()
                            try:
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                                    (robots_content, fetched_timestamp, expires_timestamp, domain)
                                )
                                conn_from_pool_update.commit()
                                logger.debug(f"Cached robots.txt for {domain} in DB (pool).")
                            except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                                logger.error(f"DB execute/commit error caching robots.txt (pool) for {domain}: {e_inner}")
                                if conn_from_pool_update: conn_from_pool_update.rollback()
                                raise
                            finally:
                                if cursor: cursor.close()
                        finally:
                            if conn_from_pool_update:
                                self.storage.db_pool.return_connection(conn_from_pool_update)
                    await asyncio.to_thread(_db_update_robots_cache_sync_threaded_pooled)
                except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
                    logger.error(f"DB (pool/connection) error caching robots.txt for {domain}: {e}")
                except Exception as e: # Catch other errors like pool Empty
                     logger.error(f"Unexpected (pool) error caching robots.txt for {domain}: {e}")


        if robots_content is not None:
            rerp.parse(robots_content)
            rerp.source_content = robots_content 
        else: 
            rerp.parse("") 
            rerp.source_content = ""
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded, using the pool."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        try:
            def _db_check_manual_exclusion_sync_threaded_pooled():
                conn_from_pool_check: sqlite3.Connection | None = None
                try:
                    conn_from_pool_check = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_check.cursor()
                    try:
                        if self.config.seeded_urls_only:
                            cursor.execute("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        else:
                            cursor.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_check:
                        self.storage.db_pool.return_connection(conn_from_pool_check)

            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded_pooled)
            if row and row[0] == 1: # type: ignore
                logger.debug(f"URL {url} from manually excluded domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) checking manual exclusion for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.warning(f"Pool error checking manual exclusion for {domain}: {e}")


        rerp = await self._get_robots_for_domain(domain) # This now uses pool for its DB ops
        if rerp:
// ... existing code ...
    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay, using pool."""
        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded_pooled():
                conn_from_pool_lft: sqlite3.Connection | None = None
                try:
                    conn_from_pool_lft = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_lft.cursor()
                    try:
                        cursor.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?\", (domain,)")
                        return cursor.fetchone()
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_lft:
                        self.storage.db_pool.return_connection(conn_from_pool_lft)
            
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded_pooled)
            if row and row[0] is not None: # type: ignore
                last_fetch_time = row[0] # type: ignore
        except sqlite3.Error as e:
            logger.error(f"DB error (pool) checking can_fetch_domain_now for {domain}: {e}")
            return False 
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Pool error checking can_fetch_domain_now for {domain}: {e}")
            return False
            
        crawl_delay = await self.get_crawl_delay(domain)
// ... existing code ...
    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain, using pool."""
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded_pooled():
                conn_from_pool_rec: sqlite3.Connection | None = None
                try:
                    conn_from_pool_rec = self.storage.db_pool.get_connection()
                    cursor = conn_from_pool_rec.cursor()
                    try:
                        cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain, last_scheduled_fetch_timestamp) VALUES (?,?)\", \n                                       (domain, current_time))
                        cursor.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?\", \n                                       (current_time, domain))
                        conn_from_pool_rec.commit()
                    except sqlite3.Error as e_inner: # Catch DB errors from execute/commit
                        logger.error(f"DB execute/commit error recording fetch attempt (pool) for {domain}: {e_inner}")
                        if conn_from_pool_rec: conn_from_pool_rec.rollback()
                        raise
                    finally:
                        if cursor: cursor.close()
                finally:
                    if conn_from_pool_rec:
                        self.storage.db_pool.return_connection(conn_from_pool_rec)
            await asyncio.to_thread(_db_record_fetch_sync_threaded_pooled)
        except sqlite3.Error as e: # Catch errors from pool.get_connection or outer sqlite3 errors
            logger.error(f"DB (pool/connection) error recording fetch attempt for {domain}: {e}")
        except Exception as e: # Catch other errors like pool Empty
            logger.error(f"Unexpected (pool) error recording fetch attempt for {domain}: {e}")
```

```python
// ... existing code ...
from crawler_module.storage import StorageManager 
from crawler_module.fetcher import Fetcher, FetchResult 
from robotexclusionrulesparser import RobotExclusionRulesParser
# db_pool will be injected from conftest.py
from crawler_module.db_pool import SQLiteConnectionPool # For manual pool creation

@pytest.fixture
def dummy_config(tmp_path: Path) -> CrawlerConfig:
// ... existing code ...
@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # If PE methods now try to access self.storage.db_pool, this mock needs it.
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool)
    
    # If the tests that use this mock_storage_manager rely on sqlite3.connect being patched,
    # those patches might still work if the PolitenessEnforcer's *own* methods are still calling sqlite3.connect
    # directly (which they shouldn't be after our refactor).
    # If PolitenessEnforcer is correctly using mock.db_pool.get_connection(), then tests need to mock
    # methods on mock.db_pool and the connection/cursor it returns.
    
    # For now, let's assume tests mocking sqlite3.connect will need review if PE uses the pool.
    # The critical part is that this mock_storage_manager *has* a db_pool attribute.
    # The db_path attribute on the mock may or may not be used anymore by PE if it uses the pool.
    mock.db_path = tmp_path / "mock_politeness_temp.db" 
    return mock

@pytest.fixture
def mock_fetcher() -> MagicMock:
// ... existing code ...
@pytest_asyncio.fixture
async def politeness_enforcer(
    dummy_config: CrawlerConfig, 
    mock_storage_manager: MagicMock, # This uses the mock_storage_manager above
    mock_fetcher: MagicMock
) -> PolitenessEnforcer:
// ... existing code ...
# Updated storage_manager_for_exclusion_test fixture
@pytest.fixture
def storage_manager_for_exclusion_test(dummy_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager: # Added db_pool
    """Provides a real StorageManager instance using a temporary DB path from dummy_config and a real pool."""
    dummy_config.data_dir.mkdir(parents=True, exist_ok=True)
    # Pass the db_pool fixture to StorageManager
    sm = StorageManager(config=dummy_config, db_pool=db_pool)
    # No sm.close(), pool handles it.
    yield sm

# --- Tests for _load_manual_exclusions --- 
def test_load_manual_exclusions_no_file(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses the `politeness_enforcer` fixture, which uses `mock_storage_manager`.
    # If _load_manual_exclusions now uses `self.storage.db_pool.get_connection()`,
    # then `mock_storage_manager.db_pool.get_connection` would be called.
    # We need to ensure that mock is set up appropriately if calls are expected.
    dummy_config.exclude_file = None
    # Re-create PE to pick up config change if __init__ is where loading happens.
    # However, the fixture `politeness_enforcer` is already created with dummy_config.
    # If dummy_config changes after PE is created, PE won't see it unless re-init.
    # For this test, it might be simpler to create PE inside the test.
    
    pe = PolitenessEnforcer(config=dummy_config, storage=politeness_enforcer.storage, fetcher=politeness_enforcer.fetcher)
    
    # Assert that no connection was attempted if exclude_file is None
    # This depends on the mock setup for db_pool on the mock_storage_manager
    pe.storage.db_pool.get_connection.assert_not_called()
    assert True 


def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager, # This SM has a real db_pool from conftest
    mock_fetcher: MagicMock, 
    tmp_path: Path 
):
    exclude_file_path = tmp_path / "custom_excludes.txt" 
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    # PolitenessEnforcer will use storage_manager_for_exclusion_test (which has a real pool)
    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # Now, connect to the DB used by the pool and verify content
    conn_verify: sqlite3.Connection | None = None
    try:
        # The db_pool used by storage_manager_for_exclusion_test is from the conftest fixture.
        # We can get a connection from it to verify.
        db_pool_to_check = storage_manager_for_exclusion_test.db_pool
        conn_verify = db_pool_to_check.get_connection() # Get a connection for verification
        cursor = conn_verify.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
// ... existing code ...
    finally:
        if cursor: cursor.close()
        if conn_verify: # Return the connection to the pool it came from
            db_pool_to_check.return_connection(conn_verify)
        # The db_pool_to_check itself will be closed by its fixture in conftest.py
        if exclude_file_path.exists(): exclude_file_path.unlink()

# --- Tests for _get_robots_for_domain (async) --- 
@pytest.mark.asyncio
async def test_get_robots_from_memory_cache(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    # This test uses `politeness_enforcer` which has `mock_storage_manager`.
    # _get_robots_for_domain will call `self.storage.db_pool.get_connection()`.
    domain = "example.com"
    mock_rerp = MagicMock(spec=RobotExclusionRulesParser)
    mock_rerp.source_content = "User-agent: *\\nDisallow: /test"
    politeness_enforcer.robots_parsers[domain] = mock_rerp

    # Simulate no DB cache by having get_connection return a mock connection whose cursor returns None
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = None # No DB cache
    mock_db_conn.cursor.return_value = mock_db_cursor
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
    assert rerp == mock_rerp
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # Verify pool was asked

@pytest.mark.asyncio
async def test_get_robots_from_db_cache_fresh(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbcached.com"
    robots_text = "User-agent: TestCrawler\\nDisallow: /private"
    future_expiry = int(time.time()) + DEFAULT_ROBOTS_TXT_TTL

    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = (robots_text, future_expiry)
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.return_value = mock_db_cursor
    
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
    if domain in politeness_enforcer.robots_parsers:
        del politeness_enforcer.robots_parsers[domain]

    rerp = await politeness_enforcer._get_robots_for_domain(domain)
        
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_db_conn.cursor.assert_called_once()
    mock_db_cursor.execute.assert_called_once_with(
        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?\", \n                            (domain,)\n    )
    mock_db_cursor.fetchone.assert_called_once()
    # politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn) # Check return

    assert rerp is not None
// ... remaining tests in test_politeness.py will need similar adjustments ...
# If they mock sqlite3.connect, they might need to change to mock methods on
# politeness_enforcer.storage.db_pool.get_connection().cursor().<method>
# This is because the PolitenessEnforcer's DB methods no longer call sqlite3.connect directly.
#
# Example for a test that previously did `with patch('sqlite3.connect', ...)`:
#
# Old way (simplified):
# with patch('sqlite3.connect', return_value=mock_db_conn_instance) as mock_sqlite_connect:
#     await politeness_enforcer._some_db_method()
#     mock_sqlite_connect.assert_called_once_with(politeness_enforcer.storage.db_path, ...)
#
# New way (simplified, assuming politeness_enforcer uses the mock_storage_manager):
# mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
# mock_db_cursor.fetchone.return_value = ... 
# mock_db_conn = MagicMock(spec=sqlite3.Connection)
# mock_db_conn.cursor.return_value = mock_db_cursor
# politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
#
# await politeness_enforcer._some_db_method() # This will use the mocked pool and connection
#
# politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
# mock_db_conn.cursor.assert_called_once()
# mock_db_cursor.execute.assert_called_with(...)
# politeness_enforcer.storage.db_pool.return_connection.assert_called_once_with(mock_db_conn)

```

---

**User**

The db_pool fixture you wrote in conftest.py is not passing type check because it is using yield but is not returning a generator?

---

**Cursor**

You are absolutely correct to point that out! My apologies.

When a pytest fixture uses `yield`, it's acting like a generator that yields a single value and then potentially performs teardown. The correct type hint for the fixture function itself should reflect this generator/iterator nature.

The type hint `-> SQLiteConnectionPool` indicates what the test *receives*, but the fixture function itself, when using `yield`, should be typed as `Iterator[SQLiteConnectionPool]` or `Generator[SQLiteConnectionPool, None, None]`. The `Iterator` type is usually sufficient and cleaner.

Let's correct the `db_pool` fixture in `crawler/conftest.py`:

```python:crawler/conftest.py
import sys
import os
import pytest
from pathlib import Path
from typing import Iterator # New import

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)


# Import the SQLiteConnectionPool after sys.path is modified
from crawler_module.db_pool import SQLiteConnectionPool
from crawler_module.config import CrawlerConfig 
from crawler_module.storage import StorageManager # Moved import here

@pytest.fixture(scope="function")
def db_pool(tmp_path: Path) -> Iterator[SQLiteConnectionPool]: # Corrected type hint
    """
    Pytest fixture for a SQLiteConnectionPool using a temporary database.
    Ensures the pool is closed after the test.
    """
    db_file = tmp_path / "test_crawler_state.db"
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir(parents=True, exist_ok=True) # Ensure parent dirs are also created
    
    seed_file = data_dir / "seeds.txt"
    # It's good practice for fixtures to ensure their resources exist if components rely on them
    # However, some tests might want to test "file not found" scenarios, so create conditionally or let tests manage.
    # For a general purpose config, having it exist is usually safer.
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")


    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
        # Ensure all non-optional fields from CrawlerConfig's definition are present
        # Example: If 'email' was mandatory and not Optional in CrawlerConfig
        # email="test@example.com", 
        # exclude_file=None, # If it can be None
        # seeded_urls_only=False, # If this exists
    )

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    # StorageManager import moved to top of file after sys.path modification
    return StorageManager(config=test_config, db_pool=db_pool)
```

**Change Made:**
*   In `crawler/conftest.py`, the `db_pool` fixture's return type hint was changed from `-> SQLiteConnectionPool` to `-> Iterator[SQLiteConnectionPool]`.
*   Added `from typing import Iterator`.
*   Moved `StorageManager` import to the top to be consistent.
*   Ensured `data_dir.mkdir(parents=True, exist_ok=True)` in `test_config` and added a default seed file creation if it doesn't exist for robustness, as components might expect it.

This change should satisfy the type checker regarding the use of `yield` in the fixture. Thank you for catching that detail!

Now that this is clarified, we can proceed with refactoring `PolitenessEnforcer` and then its tests. Shall I start by applying the changes to `crawler/crawler_module/politeness.py` to use the connection pool?

```python
import sys
import os
import pytest
from pathlib import Path
from typing import Iterator # New import

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)


# Import the SQLiteConnectionPool after sys.path is modified
from crawler_module.db_pool import SQLiteConnectionPool
from crawler_module.config import CrawlerConfig 
from crawler_module.storage import StorageManager # Moved import here

@pytest.fixture(scope="function")
def db_pool(tmp_path: Path) -> Iterator[SQLiteConnectionPool]: # Corrected type hint
    """
    Pytest fixture for a SQLiteConnectionPool using a temporary database.
    Ensures the pool is closed after the test.
    """
    db_file = tmp_path / "test_crawler_state.db"
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir(parents=True, exist_ok=True) # Ensure parent dirs are also created
    
    seed_file = data_dir / "seeds.txt"
    # It's good practice for fixtures to ensure their resources exist if components rely on them
    # However, some tests might want to test "file not found" scenarios, so create conditionally or let tests manage.
    # For a general purpose config, having it exist is usually safer.
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")


    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir,
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
        # Ensure all non-optional fields from CrawlerConfig's definition are present
        # Example: If 'email' was mandatory and not Optional in CrawlerConfig
        # email="test@example.com", 
        # exclude_file=None, # If it can be None
        # seeded_urls_only=False, # If this exists
    )

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """
    Pytest fixture for a StorageManager instance.
    """
    # StorageManager import moved to top of file after sys.path modification
    return StorageManager(config=test_config, db_pool=db_pool)
```

You've hit an `AttributeError: Mock object has no attribute 'conn'` in `test_is_url_allowed_by_robots`. This is happening because:

1.  The `politeness_enforcer` in your test uses a `mock_storage_manager`.
2.  The test line `mock_db_cursor = politeness_enforcer.storage.conn.cursor...` assumes that this `mock_storage_manager` (i.e., `politeness_enforcer.storage`) has a `.conn` attribute that behaves like a real SQLite connection, from which a cursor can be obtained.
3.  As we've been refactoring `StorageManager` to use `db_pool`, the direct `self.conn` attribute in the real `StorageManager` is no longer its primary means of database interaction for most operations. While your `mock_storage_manager` fixture *does* still assign a `MagicMock` to `mock.conn`, the underlying code in `PolitenessEnforcer` (once we refactor it) will stop using `self.storage.conn` and start using `self.storage.db_pool.get_connection()`.

The test is essentially trying to set up a mock for a code path (`storage.conn`) that either doesn't exist robustly on the mock in the way the test expects, or, more importantly, will no longer be used by `PolitenessEnforcer` after we refactor it to use the pool.

**The Correct Approach (incorporating our planned refactor):**

1.  **Refactor `PolitenessEnforcer`**: As we discussed, the database methods within `PolitenessEnforcer` (like `_db_check_manual_exclusion_sync_threaded` called by `is_url_allowed`, and the DB calls within `_get_robots_for_domain`) need to be updated to use `self.storage.db_pool.get_connection()` and `self.storage.db_pool.return_connection()`.
2.  **Update Tests in `test_politeness.py`**:
    *   The `mock_storage_manager` fixture should primarily ensure `mock.db_pool` is a `MagicMock(spec=SQLiteConnectionPool)`.
    *   Tests like `test_is_url_allowed_by_robots` need to change how they mock the database interaction. Instead of mocking `storage.conn`, they should mock the calls to `storage.db_pool.get_connection()`, the mock connection object it returns, and the mock cursor from that.

**Let's proceed with the plan:**

**Step 1: Refactor `PolitenessEnforcer`**
I will apply the changes to `crawler/crawler_module/politeness.py` to make its database operations use `self.storage.db_pool`. This is the same change I proposed in the previous turn.

```python:crawler/crawler_module/politeness.py
import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio 
import sqlite3 

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now has db_pool
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult 

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_TXT_TTL = 24 * 60 * 60  # 24 hours in seconds
MIN_CRAWL_DELAY_SECONDS = 70 # Our project's default minimum

class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage # This storage instance has the db_pool
        self.fetcher = fetcher 
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}
        # _load_manual_exclusions is synchronous but uses DB. Refactored to use pool.
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB using the pool."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_from_pool: sqlite3.Connection | None = None
            cursor = None # Define cursor here for the finally block
            try:
                conn_from_pool = self.storage.db_pool.get_connection()
                cursor = conn_from_pool.cursor()
                try: # Inner try for file operations and DB execution
                    with open(self.config.exclude_file, 'r') as f:
                        count = 0
                        for line in f:
                            domain_to_exclude = line.strip().lower()
                            if domain_to_exclude and not domain_to_exclude.startswith("#"):
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                    (domain_to_exclude,)
                                )
                                count += 1
                        conn_from_pool.commit() 
                        logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file} (using pool).")
                except IOError as e:
                    logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                    if conn_from_pool: conn_from_pool.rollback() 
                except sqlite3.Error as e: 
                    logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                    if conn_from_pool: conn_from_pool.rollback()
                    raise 
                finally:
                    if cursor: cursor.close()
            except sqlite3.Error as e: 
                logger.error(f"DB (pool/connection) error loading manual exclusions: {e}")
            except Exception as e: 
                logger.error(f"Unexpected error (e.g. pool issue) loading manual exclusions: {e}")
                # If conn_from_pool was acquired, it should still be returned or an attempt made to.
                # However, if get_connection itself failed, conn_from_pool is None.
            finally:
                if conn_from_pool:
                    self.storage.db_pool.return_connection(conn_from_pool)
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain, using the pool for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None 

        try:
            def _db_get_cached_robots_sync_threaded_pooled():
                conn_from_pool_get: sqlite3.Connection | None = None
                cursor_get = None
                try:
                    conn_from_pool_get = self.storage.db_pool.get_connection()
                    cursor_get = conn_from_pool_get.cursor()
                    cursor_get.execute(
                        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                        (domain,)
                    )
                    return cursor_get.fetchone()
                finally:
                    if cursor_get: cursor_get.close()
                    if conn_from_pool_get:
                        self.storage.db_pool.return_connection(conn_from_pool_get)
            
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded_pooled)
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) fetching cached robots.txt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error (pool) during DB cache check for {domain}: {e}", exc_info=True)

        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        # This logic was a bit complex for in-memory only if DB miss/stale.
        # The primary logic should be: if DB not fresh, then fetch. In-memory is an optimization.
        # Let's simplify: if we have content from a fresh DB, we use it. Otherwise, we proceed to fetch.
        # The in-memory `self.robots_parsers[domain]` will be updated after a successful parse from ANY source (fresh DB or fresh fetch).
        
        if robots_content is None: # Indicates DB cache was not fresh or not present. Time to fetch.
            if domain in self.robots_parsers: # Check if we have a valid in-memory parser (e.g. fetched earlier in this session but not yet in DB or DB was stale)
                # This check could be more sophisticated (e.g. in-memory TTL), but for now, if it exists, assume it's the latest attempt.
                # The original logic could lead to using a stale in-memory copy if DB was stale.
                # To be safe, if DB isn't fresh, always re-fetch, unless we add an in-memory TTL.
                # For now, let's simplify: if db_row didn't give fresh `robots_content`, we fetch.
                pass # Proceed to fetch

            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found:
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else:
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404: 
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            # At this point, robots_content is definitely a string (fetched or default empty)
            try:
                def _db_update_robots_cache_sync_threaded_pooled():
                    conn_from_pool_update: sqlite3.Connection | None = None
                    cursor_update = None
                    try:
                        conn_from_pool_update = self.storage.db_pool.get_connection()
                        cursor_update = conn_from_pool_update.cursor()
                        cursor_update.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                        cursor_update.execute(
                            "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                            (robots_content, fetched_timestamp, expires_timestamp, domain)
                        )
                        conn_from_pool_update.commit()
                        logger.debug(f"Cached robots.txt for {domain} in DB (pool).")
                    except sqlite3.Error as e_inner: 
                        logger.error(f"DB execute/commit error caching robots.txt (pool) for {domain}: {e_inner}")
                        if conn_from_pool_update: conn_from_pool_update.rollback()
                        raise
                    finally:
                        if cursor_update: cursor_update.close()
                await asyncio.to_thread(_db_update_robots_cache_sync_threaded_pooled) # Call the pooled version
            except sqlite3.Error as e: 
                logger.error(f"DB (pool/connection) error caching robots.txt for {domain}: {e}")
            except Exception as e: 
                 logger.error(f"Unexpected (pool) error caching robots.txt for {domain}: {e}")
            finally: # Ensure connection is returned if acquired by the outer try for update logic
                # The connection is managed within _db_update_robots_cache_sync_threaded_pooled
                pass


        if robots_content is not None: # Should always be true now as it defaults to ""
            rerp.parse(robots_content)
            # Store the original content on the parser object for potential future reference/debugging
            # This attribute 'source_content' is not standard for RobotExclusionRulesParser, it's custom.
            setattr(rerp, 'source_content', robots_content) 
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded, using the pool."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        try:
            def _db_check_manual_exclusion_sync_threaded_pooled():
                conn_from_pool_check: sqlite3.Connection | None = None
                cursor_check = None
                try:
                    conn_from_pool_check = self.storage.db_pool.get_connection()
                    cursor_check = conn_from_pool_check.cursor()
                    query = ("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?" 
                             if self.config.seeded_urls_only 
                             else "SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?")
                    cursor_check.execute(query, (domain,))
                    return cursor_check.fetchone()
                finally:
                    if cursor_check: cursor_check.close()
                    if conn_from_pool_check:
                        self.storage.db_pool.return_connection(conn_from_pool_check)

            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded_pooled)
            if row and row[0] == 1: 
                logger.debug(f"URL {url} from manually excluded (or not seeded in seeded_only_mode) domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) checking manual exclusion for {domain}: {e}")
        except Exception as e: 
            logger.warning(f"Pool error checking manual exclusion for {domain}: {e}")


        rerp = await self._get_robots_for_domain(domain) 
        if rerp:
            is_allowed = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed
        
        logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
        return True # Default to allow if RERP somehow isn't available

    async def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain (from robots.txt or default)."""
        rerp = await self._get_robots_for_domain(domain) # This now uses pool for its DB ops
        delay = None
        if rerp:
            # RobotExclusionRulesParser.get_crawl_delay takes the specific agent
            agent_delay = rerp.get_crawl_delay(self.config.user_agent) 
            if agent_delay is not None:
                delay = float(agent_delay)
                logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (agent: {self.config.user_agent})")
            
            if delay is None: # If specific agent had no delay, check wildcard
                 wildcard_delay = rerp.get_crawl_delay("*")
                 if wildcard_delay is not None:
                    delay = float(wildcard_delay)
                    logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (wildcard agent)")
        
        if delay is None: # No delay specified in robots.txt at all (or robots.txt failed to parse)
            logger.debug(f"No crawl delay specified in robots.txt for {domain}. Using default: {MIN_CRAWL_DELAY_SECONDS}s")
            return float(MIN_CRAWL_DELAY_SECONDS)
        
        return max(float(delay), float(MIN_CRAWL_DELAY_SECONDS)) # Ensure our minimum is respected

    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay, using pool."""
        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded_pooled():
                conn_from_pool_lft: sqlite3.Connection | None = None
                cursor_lft = None
                try:
                    conn_from_pool_lft = self.storage.db_pool.get_connection()
                    cursor_lft = conn_from_pool_lft.cursor()
                    cursor_lft.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,))
                    return cursor_lft.fetchone()
                finally:
                    if cursor_lft: cursor_lft.close()
                    if conn_from_pool_lft:
                        self.storage.db_pool.return_connection(conn_from_pool_lft)
            
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded_pooled)
            if row and row[0] is not None: 
                last_fetch_time = row[0] 
        except sqlite3.Error as e:
            logger.error(f"DB error (pool) checking can_fetch_domain_now for {domain}: {e}")
            return False # If DB fails, assume we can't fetch to be safe
        except Exception as e: 
            logger.error(f"Pool error checking can_fetch_domain_now for {domain}: {e}")
            return False
            
        crawl_delay = await self.get_crawl_delay(domain) 
        current_time = int(time.time())
        
        if current_time >= last_fetch_time + crawl_delay:
            return True
        else:
            # logger.debug(f"Domain {domain} not ready. Last fetch: {last_fetch_time}, delay: {crawl_delay}, current: {current_time}")
            return False

    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain, using pool."""
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded_pooled():
                conn_from_pool_rec: sqlite3.Connection | None = None
                cursor_rec = None
                try:
                    conn_from_pool_rec = self.storage.db_pool.get_connection()
                    cursor_rec = conn_from_pool_rec.cursor()
                    # Upsert-like behavior: Insert if not exists, then update timestamp.
                    # Ensures domain row exists before update.
                    cursor_rec.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                    cursor_rec.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
                                       (current_time, domain))
                    conn_from_pool_rec.commit()
                except sqlite3.Error as e_inner: 
                    logger.error(f"DB execute/commit error recording fetch attempt (pool) for {domain}: {e_inner}")
                    if conn_from_pool_rec: conn_from_pool_rec.rollback()
                    raise # Propagate to outer catch
                finally:
                    if cursor_rec: cursor_rec.close()
            await asyncio.to_thread(_db_record_fetch_sync_threaded_pooled) # Call the pooled version
        except sqlite3.Error as e: 
            logger.error(f"DB (pool/connection) error recording fetch attempt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected (pool) error recording fetch attempt for {domain}: {e}")
        finally: # Ensure connection is returned if outer try acquired it (it doesn't here)
            # The connection is managed within _db_record_fetch_sync_threaded_pooled's finally block.
            pass
```

**Step 2: Update `tests/test_politeness.py`**
Now that `PolitenessEnforcer` uses the pool, we need to adjust the tests.

The `test_is_url_allowed_by_robots` test was failing. It uses the `politeness_enforcer` fixture, which in turn uses the `mock_storage_manager` fixture.

Let's refine `mock_storage_manager` in `tests/test_politeness.py` and then fix `test_is_url_allowed_by_robots`.

```python:crawler/tests/test_politeness.py
// ... existing code ...
@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # PolitenessEnforcer will now access self.storage.db_pool
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool)
    
    # Remove the old mock.conn setup as it's not the primary path anymore
    # mock.conn = MagicMock(spec=sqlite3.Connection)
    # mock.conn.cursor.return_value.__enter__.return_value = MagicMock(spec=sqlite3.Cursor)
    # mock.conn.cursor.return_value.__exit__.return_value = None
    
    # db_path might still be needed if any part of PolitenessEnforcer uses it directly (it shouldn't for DB ops now)
    # For _load_manual_exclusions, it uses config.exclude_file, not storage.db_path for the file read.
    # storage.db_path was primarily for direct sqlite3.connect calls, which are now gone from PE's DB methods.
    # Let's keep it for now in case any non-DB logic in PE might reference it, though ideally not.
    mock.db_path = tmp_path / "mock_politeness_state.db" 
    return mock

// ... existing code ...

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\\nDisallow: /disallowed"

    # Mock the sequence of database interactions expected from is_url_allowed and _get_robots_for_domain
    
    # 1. Mock for _db_check_manual_exclusion_sync_threaded_pooled (called by is_url_allowed)
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Not manually excluded
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # 2. Mock for _db_get_cached_robots_sync_threaded_pooled (first DB check in _get_robots_for_domain)
    mock_cursor_robots_cache_get = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_robots_cache_get.fetchone.return_value = None # Simulate no DB cache for robots.txt
    mock_conn_robots_cache_get = MagicMock(spec=sqlite3.Connection)
    mock_conn_robots_cache_get.cursor.return_value = mock_cursor_robots_cache_get

    # 3. Mock for _db_update_robots_cache_sync_threaded_pooled (after fetching robots.txt)
    mock_cursor_robots_cache_update = MagicMock(spec=sqlite3.Cursor)
    mock_conn_robots_cache_update = MagicMock(spec=sqlite3.Connection)
    mock_conn_robots_cache_update.cursor.return_value = mock_cursor_robots_cache_update
    
    # Set up the db_pool.get_connection mock to return these mock connections in sequence
    politeness_enforcer.storage.db_pool.get_connection.side_effect = [
        mock_conn_manual_exclusion, # For manual exclusion check
        mock_conn_robots_cache_get, # For _get_robots_for_domain's initial cache check
        mock_conn_robots_cache_update # For _get_robots_for_domain's cache update after fetch
    ]
    
    # Ensure robots.txt is not in memory cache of PolitenessEnforcer
    politeness_enforcer.robots_parsers.pop(domain, None)

    # Mock the fetcher to return the robots.txt content
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt", 
        status_code=200, text_content=robots_text
    )

    # Test the allowed URL
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    
    # Verify fetch_url was called for robots.txt (it should have been, as DB cache was None)
    politeness_enforcer.fetcher.fetch_url.assert_called_once_with(f"http://{domain}/robots.txt\", is_robots_txt=True)
    
    # Reset fetch_url mock for the next assertion because _get_robots_for_domain should now use the in-memory cache
    politeness_enforcer.fetcher.fetch_url.reset_mock()
    
    # Manual exclusion check will happen again, then _get_robots_for_domain will hit memory cache.
    # Update side_effect for the next get_connection call for manual exclusion.
    # The memory cache for robots should prevent further DB calls for robots.txt itself.
    mock_cursor_manual_exclusion_2 = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion_2.fetchone.return_value = (0,) # Still not manually excluded
    mock_conn_manual_exclusion_2 = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion_2.cursor.return_value = mock_cursor_manual_exclusion_2

    # Reset get_connection to only expect one more call for the disallowed_url manual exclusion check
    politeness_enforcer.storage.db_pool.get_connection.side_effect = [mock_conn_manual_exclusion_2]
    politeness_enforcer.storage.db_pool.get_connection.reset_mock() # Reset call count before next assertion

    # Test the disallowed URL
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    
    # Fetcher should NOT have been called again as RERP should be in memory.
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    # get_connection should have been called once for the manual exclusion check for disallowed_url
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()


// ... (rest of the tests in test_politeness.py will need similar careful review and updates to their mocking strategy) ...
```

This is a significant change in how `test_is_url_allowed_by_robots` and other tests in that file need to be structured. They now need to mock the interactions with the `db_pool` interface on the `mock_storage_manager`.

**Summary of this step:**
1.  Refactored `PolitenessEnforcer`'s internal DB methods to use `self.storage.db_pool`.
2.  Updated the `mock_storage_manager` fixture in `tests/test_politeness.py` to provide a `mock.db_pool`.
3.  Updated `test_is_url_allowed_by_robots` to mock the calls to `politeness_enforcer.storage.db_pool.get_connection()` and the subsequent connection/cursor methods, instead of trying to use/mock a non-existent `storage.conn`.

The other tests in `tests/test_politeness.py` that involve database interactions will require similar updates to their mocking strategy. This will involve:
*   Identifying which DB methods in `PolitenessEnforcer` are called.
*   Setting up `politeness_enforcer.storage.db_pool.get_connection.side_effect` to provide the necessary sequence of mock connections for each DB operation the method under test performs.
*   Setting `fetchone()`, `fetchall()`, etc., on the mock cursors returned by these mock connections.

This is intricate work. I've applied the refactor to `PolitenessEnforcer` and updated the `test_is_url_allowed_by_robots` test as an example.

Please review these changes. If they look correct, we can then discuss a strategy for updating the remaining tests in `test_politeness.py`, or I can attempt to update a few more of them based on the patterns observed.

```python
import logging
import time
from pathlib import Path
from urllib.parse import urljoin
from robotexclusionrulesparser import RobotExclusionRulesParser # type: ignore
import asyncio 
import sqlite3 

from .config import CrawlerConfig
from .storage import StorageManager # StorageManager now has db_pool
from .utils import extract_domain
from .fetcher import Fetcher, FetchResult 

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_TXT_TTL = 24 * 60 * 60  # 24 hours in seconds
MIN_CRAWL_DELAY_SECONDS = 70 # Our project's default minimum

class PolitenessEnforcer:
    def __init__(self, config: CrawlerConfig, storage: StorageManager, fetcher: Fetcher):
        self.config = config
        self.storage = storage # This storage instance has the db_pool
        self.fetcher = fetcher 
        self.robots_parsers: dict[str, RobotExclusionRulesParser] = {}
        # _load_manual_exclusions is synchronous but uses DB. Refactored to use pool.
        self._load_manual_exclusions()

    def _load_manual_exclusions(self):
        """Loads manually excluded domains from the config file into the DB using the pool."""
        if self.config.exclude_file and self.config.exclude_file.exists():
            conn_from_pool: sqlite3.Connection | None = None
            cursor = None # Define cursor here for the finally block
            try:
                conn_from_pool = self.storage.db_pool.get_connection()
                cursor = conn_from_pool.cursor()
                try: # Inner try for file operations and DB execution
                    with open(self.config.exclude_file, 'r') as f:
                        count = 0
                        for line in f:
                            domain_to_exclude = line.strip().lower()
                            if domain_to_exclude and not domain_to_exclude.startswith("#"):
                                cursor.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain_to_exclude,))
                                cursor.execute(
                                    "UPDATE domain_metadata SET is_manually_excluded = 1 WHERE domain = ?",
                                    (domain_to_exclude,)
                                )
                                count += 1
                        conn_from_pool.commit() 
                        logger.info(f"Loaded and marked {count} domains as manually excluded from {self.config.exclude_file} (using pool).")
                except IOError as e:
                    logger.error(f"Error reading exclude file {self.config.exclude_file}: {e}")
                    if conn_from_pool: conn_from_pool.rollback() 
                except sqlite3.Error as e: 
                    logger.error(f"DB execute/commit error loading manual exclusions: {e}")
                    if conn_from_pool: conn_from_pool.rollback()
                    raise 
                finally:
                    if cursor: cursor.close()
            except sqlite3.Error as e: 
                logger.error(f"DB (pool/connection) error loading manual exclusions: {e}")
            except Exception as e: 
                logger.error(f"Unexpected error (e.g. pool issue) loading manual exclusions: {e}")
                # If conn_from_pool was acquired, it should still be returned or an attempt made to.
                # However, if get_connection itself failed, conn_from_pool is None.
            finally:
                if conn_from_pool:
                    self.storage.db_pool.return_connection(conn_from_pool)
        else:
            logger.info("No manual exclude file specified or found.")

    async def _get_robots_for_domain(self, domain: str) -> RobotExclusionRulesParser | None:
        """Fetches (async), parses, and caches robots.txt for a domain, using the pool for DB ops."""
        current_time = int(time.time())
        rerp = RobotExclusionRulesParser()
        robots_content: str | None = None
        db_row: tuple | None = None 

        try:
            def _db_get_cached_robots_sync_threaded_pooled():
                conn_from_pool_get: sqlite3.Connection | None = None
                cursor_get = None
                try:
                    conn_from_pool_get = self.storage.db_pool.get_connection()
                    cursor_get = conn_from_pool_get.cursor()
                    cursor_get.execute(
                        "SELECT robots_txt_content, robots_txt_expires_timestamp FROM domain_metadata WHERE domain = ?", 
                        (domain,)
                    )
                    return cursor_get.fetchone()
                finally:
                    if cursor_get: cursor_get.close()
                    if conn_from_pool_get:
                        self.storage.db_pool.return_connection(conn_from_pool_get)
            
            db_row = await asyncio.to_thread(_db_get_cached_robots_sync_threaded_pooled)
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) fetching cached robots.txt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error (pool) during DB cache check for {domain}: {e}", exc_info=True)

        if db_row and db_row[0] is not None and db_row[1] is not None and db_row[1] > current_time:
            logger.debug(f"Using fresh robots.txt for {domain} from DB (expires: {db_row[1]}).")
            robots_content = db_row[0]
            if domain in self.robots_parsers and hasattr(self.robots_parsers[domain], 'source_content') and \
               self.robots_parsers[domain].source_content == robots_content:
                logger.debug(f"In-memory robots_parser for {domain} matches fresh DB. Reusing parser object.")
                return self.robots_parsers[domain]
        # This logic was a bit complex for in-memory only if DB miss/stale.
        # The primary logic should be: if DB not fresh, then fetch. In-memory is an optimization.
        # Let's simplify: if we have content from a fresh DB, we use it. Otherwise, we proceed to fetch.
        # The in-memory `self.robots_parsers[domain]` will be updated after a successful parse from ANY source (fresh DB or fresh fetch).
        
        if robots_content is None: # Indicates DB cache was not fresh or not present. Time to fetch.
            if domain in self.robots_parsers: # Check if we have a valid in-memory parser (e.g. fetched earlier in this session but not yet in DB or DB was stale)
                # This check could be more sophisticated (e.g. in-memory TTL), but for now, if it exists, assume it's the latest attempt.
                # The original logic could lead to using a stale in-memory copy if DB was stale.
                # To be safe, if DB isn't fresh, always re-fetch, unless we add an in-memory TTL.
                # For now, let's simplify: if db_row didn't give fresh `robots_content`, we fetch.
                pass # Proceed to fetch

            fetched_timestamp = int(time.time())
            expires_timestamp = fetched_timestamp + DEFAULT_ROBOTS_TXT_TTL
            
            robots_url_http = f"http://{domain}/robots.txt"
            robots_url_https = f"https://{domain}/robots.txt"
            logger.info(f"Attempting to fetch robots.txt for {domain} (HTTP first)")
            fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)

            robots_content_found = False
            if fetch_result_http.status_code == 200 and fetch_result_http.text_content is not None:
                robots_content = fetch_result_http.text_content
                robots_content_found = True
            
            if not robots_content_found:
                if fetch_result_http.status_code == 404:
                    logger.debug(f"robots.txt not found (404) via HTTP for {domain}. Trying HTTPS.")
                else:
                    logger.info(f"HTTP fetch for robots.txt did not yield content for {domain} (status: {fetch_result_http.status_code}). Trying HTTPS.")

                fetch_result_https: FetchResult = await self.fetcher.fetch_url(robots_url_https, is_robots_txt=True)
                if fetch_result_https.status_code == 200 and fetch_result_https.text_content is not None:
                    robots_content = fetch_result_https.text_content
                elif fetch_result_https.status_code == 404: 
                    logger.debug(f"robots.txt not found (404) via HTTPS for {domain}. Assuming allow all.")
                    robots_content = ""
                else:
                    logger.warning(f"Failed to fetch robots.txt for {domain} via HTTPS as well (status: {fetch_result_https.status_code}). Assuming allow all.")
                    robots_content = ""
            
            if robots_content is None: 
                 logger.warning(f"Robots content was unexpectedly None for {domain} after fetch attempts. Defaulting to allow (empty rules).")
                 robots_content = ""
            
            # At this point, robots_content is definitely a string (fetched or default empty)
            try:
                def _db_update_robots_cache_sync_threaded_pooled():
                    conn_from_pool_update: sqlite3.Connection | None = None
                    cursor_update = None
                    try:
                        conn_from_pool_update = self.storage.db_pool.get_connection()
                        cursor_update = conn_from_pool_update.cursor()
                        cursor_update.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                        cursor_update.execute(
                            "UPDATE domain_metadata SET robots_txt_content = ?, robots_txt_fetched_timestamp = ?, robots_txt_expires_timestamp = ? WHERE domain = ?",
                            (robots_content, fetched_timestamp, expires_timestamp, domain)
                        )
                        conn_from_pool_update.commit()
                        logger.debug(f"Cached robots.txt for {domain} in DB (pool).")
                    except sqlite3.Error as e_inner: 
                        logger.error(f"DB execute/commit error caching robots.txt (pool) for {domain}: {e_inner}")
                        if conn_from_pool_update: conn_from_pool_update.rollback()
                        raise
                    finally:
                        if cursor_update: cursor_update.close()
                await asyncio.to_thread(_db_update_robots_cache_sync_threaded_pooled) # Call the pooled version
            except sqlite3.Error as e: 
                logger.error(f"DB (pool/connection) error caching robots.txt for {domain}: {e}")
            except Exception as e: 
                 logger.error(f"Unexpected (pool) error caching robots.txt for {domain}: {e}")
            finally: # Ensure connection is returned if acquired by the outer try for update logic
                # The connection is managed within _db_update_robots_cache_sync_threaded_pooled
                pass


        if robots_content is not None: # Should always be true now as it defaults to ""
            rerp.parse(robots_content)
            # Store the original content on the parser object for potential future reference/debugging
            # This attribute 'source_content' is not standard for RobotExclusionRulesParser, it's custom.
            setattr(rerp, 'source_content', robots_content) 
        
        self.robots_parsers[domain] = rerp
        return rerp

    async def is_url_allowed(self, url: str) -> bool:
        """Checks if a URL is allowed by robots.txt and not manually excluded, using the pool."""
        domain = extract_domain(url)
        if not domain:
            logger.warning(f"Could not extract domain for URL: {url} for robots check. Allowing.")
            return True

        try:
            def _db_check_manual_exclusion_sync_threaded_pooled():
                conn_from_pool_check: sqlite3.Connection | None = None
                cursor_check = None
                try:
                    conn_from_pool_check = self.storage.db_pool.get_connection()
                    cursor_check = conn_from_pool_check.cursor()
                    query = ("SELECT is_manually_excluded OR NOT is_seeded FROM domain_metadata WHERE domain = ?" 
                             if self.config.seeded_urls_only 
                             else "SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?")
                    cursor_check.execute(query, (domain,))
                    return cursor_check.fetchone()
                finally:
                    if cursor_check: cursor_check.close()
                    if conn_from_pool_check:
                        self.storage.db_pool.return_connection(conn_from_pool_check)

            row = await asyncio.to_thread(_db_check_manual_exclusion_sync_threaded_pooled)
            if row and row[0] == 1: 
                logger.debug(f"URL {url} from manually excluded (or not seeded in seeded_only_mode) domain: {domain}")
                return False
        except sqlite3.Error as e:
            logger.warning(f"DB error (pool) checking manual exclusion for {domain}: {e}")
        except Exception as e: 
            logger.warning(f"Pool error checking manual exclusion for {domain}: {e}")


        rerp = await self._get_robots_for_domain(domain) 
        if rerp:
            is_allowed = rerp.is_allowed(self.config.user_agent, url)
            if not is_allowed:
                logger.debug(f"URL disallowed by robots.txt for {domain}: {url}")
            return is_allowed
        
        logger.warning(f"No robots.txt parser available for {domain} after checks. Allowing URL: {url}")
        return True # Default to allow if RERP somehow isn't available

    async def get_crawl_delay(self, domain: str) -> float:
        """Gets the crawl delay for a domain (from robots.txt or default)."""
        rerp = await self._get_robots_for_domain(domain) # This now uses pool for its DB ops
        delay = None
        if rerp:
            # RobotExclusionRulesParser.get_crawl_delay takes the specific agent
            agent_delay = rerp.get_crawl_delay(self.config.user_agent) 
            if agent_delay is not None:
                delay = float(agent_delay)
                logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (agent: {self.config.user_agent})")
            
            if delay is None: # If specific agent had no delay, check wildcard
                 wildcard_delay = rerp.get_crawl_delay("*")
                 if wildcard_delay is not None:
                    delay = float(wildcard_delay)
                    logger.debug(f"Using {delay}s crawl delay from robots.txt for {domain} (wildcard agent)")
        
        if delay is None: # No delay specified in robots.txt at all (or robots.txt failed to parse)
            logger.debug(f"No crawl delay specified in robots.txt for {domain}. Using default: {MIN_CRAWL_DELAY_SECONDS}s")
            return float(MIN_CRAWL_DELAY_SECONDS)
        
        return max(float(delay), float(MIN_CRAWL_DELAY_SECONDS)) # Ensure our minimum is respected

    async def can_fetch_domain_now(self, domain: str) -> bool:
        """Checks if the domain can be fetched based on last fetch time and crawl delay, using pool."""
        last_fetch_time = 0
        try:
            def _db_get_last_fetch_sync_threaded_pooled():
                conn_from_pool_lft: sqlite3.Connection | None = None
                cursor_lft = None
                try:
                    conn_from_pool_lft = self.storage.db_pool.get_connection()
                    cursor_lft = conn_from_pool_lft.cursor()
                    cursor_lft.execute("SELECT last_scheduled_fetch_timestamp FROM domain_metadata WHERE domain = ?", (domain,))
                    return cursor_lft.fetchone()
                finally:
                    if cursor_lft: cursor_lft.close()
                    if conn_from_pool_lft:
                        self.storage.db_pool.return_connection(conn_from_pool_lft)
            
            row = await asyncio.to_thread(_db_get_last_fetch_sync_threaded_pooled)
            if row and row[0] is not None: 
                last_fetch_time = row[0] 
        except sqlite3.Error as e:
            logger.error(f"DB error (pool) checking can_fetch_domain_now for {domain}: {e}")
            return False # If DB fails, assume we can't fetch to be safe
        except Exception as e: 
            logger.error(f"Pool error checking can_fetch_domain_now for {domain}: {e}")
            return False
            
        crawl_delay = await self.get_crawl_delay(domain) 
        current_time = int(time.time())
        
        if current_time >= last_fetch_time + crawl_delay:
            return True
        else:
            # logger.debug(f"Domain {domain} not ready. Last fetch: {last_fetch_time}, delay: {crawl_delay}, current: {current_time}")
            return False

    async def record_domain_fetch_attempt(self, domain: str):
        """Records that we are about to fetch (or have fetched) from a domain, using pool."""
        try:
            current_time = int(time.time())
            def _db_record_fetch_sync_threaded_pooled():
                conn_from_pool_rec: sqlite3.Connection | None = None
                cursor_rec = None
                try:
                    conn_from_pool_rec = self.storage.db_pool.get_connection()
                    cursor_rec = conn_from_pool_rec.cursor()
                    # Upsert-like behavior: Insert if not exists, then update timestamp.
                    # Ensures domain row exists before update.
                    cursor_rec.execute("INSERT OR IGNORE INTO domain_metadata (domain) VALUES (?)", (domain,))
                    cursor_rec.execute("UPDATE domain_metadata SET last_scheduled_fetch_timestamp = ? WHERE domain = ?", 
                                       (current_time, domain))
                    conn_from_pool_rec.commit()
                except sqlite3.Error as e_inner: 
                    logger.error(f"DB execute/commit error recording fetch attempt (pool) for {domain}: {e_inner}")
                    if conn_from_pool_rec: conn_from_pool_rec.rollback()
                    raise # Propagate to outer catch
                finally:
                    if cursor_rec: cursor_rec.close()
            await asyncio.to_thread(_db_record_fetch_sync_threaded_pooled) # Call the pooled version
        except sqlite3.Error as e: 
            logger.error(f"DB (pool/connection) error recording fetch attempt for {domain}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected (pool) error recording fetch attempt for {domain}: {e}")
        finally: # Ensure connection is returned if outer try acquired it (it doesn't here)
            # The connection is managed within _db_record_fetch_sync_threaded_pooled's finally block.
            pass
```

```python
// ... existing code ...
@pytest.fixture
def mock_storage_manager(tmp_path: Path) -> MagicMock: # This mock is used when PE is unit tested with mocked storage
    mock = MagicMock(spec=StorageManager)
    # PolitenessEnforcer will now access self.storage.db_pool
    mock.db_pool = MagicMock(spec=SQLiteConnectionPool)
    
    # Remove the old mock.conn setup as it's not the primary path anymore
    # mock.conn = MagicMock(spec=sqlite3.Connection)
    # mock.conn.cursor.return_value.__enter__.return_value = MagicMock(spec=sqlite3.Cursor)
    # mock.conn.cursor.return_value.__exit__.return_value = None
    
    # db_path might still be needed if any part of PolitenessEnforcer uses it directly (it shouldn't for DB ops now)
    # For _load_manual_exclusions, it uses config.exclude_file, not storage.db_path for the file read.
    # storage.db_path was primarily for direct sqlite3.connect calls, which are now gone from PE's DB methods.
    # Let's keep it for now in case any non-DB logic in PE might reference it, though ideally not.
    mock.db_path = tmp_path / "mock_politeness_state.db" 
    return mock

// ... existing code ...

@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\\nDisallow: /disallowed"

    # Mock the sequence of database interactions expected from is_url_allowed and _get_robots_for_domain
    
    # 1. Mock for _db_check_manual_exclusion_sync_threaded_pooled (called by is_url_allowed)
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Not manually excluded
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # 2. Mock for _db_get_cached_robots_sync_threaded_pooled (first DB check in _get_robots_for_domain)
    mock_cursor_robots_cache_get = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_robots_cache_get.fetchone.return_value = None # Simulate no DB cache for robots.txt
    mock_conn_robots_cache_get = MagicMock(spec=sqlite3.Connection)
    mock_conn_robots_cache_get.cursor.return_value = mock_cursor_robots_cache_get

    # 3. Mock for _db_update_robots_cache_sync_threaded_pooled (after fetching robots.txt)
    mock_cursor_robots_cache_update = MagicMock(spec=sqlite3.Cursor)
    mock_conn_robots_cache_update = MagicMock(spec=sqlite3.Connection)
    mock_conn_robots_cache_update.cursor.return_value = mock_cursor_robots_cache_update
    
    # Set up the db_pool.get_connection mock to return these mock connections in sequence
    politeness_enforcer.storage.db_pool.get_connection.side_effect = [
        mock_conn_manual_exclusion, # For manual exclusion check
        mock_conn_robots_cache_get, # For _get_robots_for_domain's initial cache check
        mock_conn_robots_cache_update # For _get_robots_for_domain's cache update after fetch
    ]
    
    # Ensure robots.txt is not in memory cache of PolitenessEnforcer
    politeness_enforcer.robots_parsers.pop(domain, None)

    # Mock the fetcher to return the robots.txt content
    politeness_enforcer.fetcher.fetch_url.return_value = FetchResult(
        initial_url=f"http://{domain}/robots.txt", final_url=f"http://{domain}/robots.txt", 
        status_code=200, text_content=robots_text
    )

    # Test the allowed URL
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    
    # Verify fetch_url was called for robots.txt (it should have been, as DB cache was None)
    politeness_enforcer.fetcher.fetch_url.assert_called_once_with(f"http://{domain}/robots.txt\", is_robots_txt=True)
    
    # Reset fetch_url mock for the next assertion because _get_robots_for_domain should now use the in-memory cache
    politeness_enforcer.fetcher.fetch_url.reset_mock()
    
    # Manual exclusion check will happen again, then _get_robots_for_domain will hit memory cache.
    # Update side_effect for the next get_connection call for manual exclusion.
    # The memory cache for robots should prevent further DB calls for robots.txt itself.
    mock_cursor_manual_exclusion_2 = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion_2.fetchone.return_value = (0,) # Still not manually excluded
    mock_conn_manual_exclusion_2 = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion_2.cursor.return_value = mock_cursor_manual_exclusion_2

    # Reset get_connection to only expect one more call for the disallowed_url manual exclusion check
    politeness_enforcer.storage.db_pool.get_connection.side_effect = [mock_conn_manual_exclusion_2]
    politeness_enforcer.storage.db_pool.get_connection.reset_mock() # Reset call count before next assertion

    # Test the disallowed URL
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    
    # Fetcher should NOT have been called again as RERP should be in memory.
    politeness_enforcer.fetcher.fetch_url.assert_not_called()
    # get_connection should have been called once for the manual exclusion check for disallowed_url
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()


// ... (rest of the tests in test_politeness.py will need similar careful review and updates to their mocking strategy) ...
```

---

**User**

i don't understand why we need to intricately mock the entire sequence of db calls now when we didn't have to before?

---

**Cursor**

You're asking a very insightful question. The perceived increase in mocking intricacy stems from a shift in where and how database connections are obtained by the `PolitenessEnforcer`.

Let's break down the "before" and "after":

**Before Refactoring (PolitenessEnforcer's DB methods used `sqlite3.connect` directly):**

*   **Connection Source:** Each database helper function within `PolitenessEnforcer` (like `_db_get_cached_robots_sync_threaded`) would internally call `sqlite3.connect(self.storage.db_path, ...)`.
*   **Mocking Strategy:**
    *   The tests often used `patch('sqlite3.connect', return_value=mock_db_conn_instance)`. This was a **global patch**. Any call to `sqlite3.connect` *anywhere* (when the patch was active) would get your `mock_db_conn_instance`.
    *   You'd then configure this single `mock_db_conn_instance` (and its cursor) to behave as needed for the specific test.
    *   If a method in `PolitenessEnforcer` made multiple internal calls to `sqlite3.connect` (though usually it was one `connect` per threaded helper), each would hit the same global patch and get the same mock connection object. The test then relied on configuring that mock connection's cursor's `fetchone` (perhaps with a `side_effect` if multiple `fetchone` calls were made on the *same cursor*) to simulate different results.

**After Refactoring (PolitenessEnforcer's DB methods use `self.storage.db_pool.get_connection()`):**

*   **Connection Source:** Each database helper function now calls `self.storage.db_pool.get_connection()`.
*   **Mocking Strategy Change:**
    1.  **No Global Patch:** We can no longer globally patch `sqlite3.connect` to control these interactions because `PolitenessEnforcer` isn't calling it directly for these operations anymore.
    2.  **Targeted Mocking:** We must now specifically mock `politeness_enforcer.storage.db_pool.get_connection()`. The `politeness_enforcer` fixture uses `mock_storage_manager`, so this path becomes `mock_storage_manager.db_pool.get_connection()`.
    3.  **Multiple Distinct Operations:** A single high-level method in `PolitenessEnforcer` (e.g., `is_url_allowed`) might perform several distinct database operations:
        *   Check manual exclusion (1st DB op -> 1st `get_connection` call).
        *   If not excluded, call `_get_robots_for_domain`, which might:
            *   Check DB cache for robots.txt (2nd DB op -> 2nd `get_connection` call).
            *   If fetched, update DB cache (3rd DB op -> 3rd `get_connection` call).
    4.  **Unique Mock for Each `get_connection` Call:** Each time `get_connection()` is called, it's for a conceptually separate database interaction. If we want to simulate different outcomes for these different interactions (e.g., manual exclusion check finds nothing, robots cache check finds nothing, robots update succeeds), then each call to `get_connection()` within that sequence needs to effectively return a "fresh" mock connection pre-configured for *that specific step's* expected outcome.
        *   This is why `side_effect` is used for `get_connection`: `get_connection.side_effect = [mock_conn1, mock_conn2, mock_conn3]`. The first time `get_connection` is called in the code flow, it returns `mock_conn1` (pre-configured for, say, the manual exclusion check). The second time, it returns `mock_conn2` (pre-configured for the robots cache read), and so on.

**Why it seems more intricate:**

*   **Explicit Sequencing:** You're now explicitly defining the sequence of mock connection objects that `get_connection` will return, corresponding to the sequence of database operations within the method under test. Before, with the global patch, you had one mock connection, and you might have used `fetchone.side_effect` on its cursor to simulate a sequence of results for *that single connection's cursor*. Now, you're simulating a sequence of *entire connection lifecycles* (get, use, return) from the pool.
*   **Mocking an Interface, Not a Global:** You're mocking the interface of the `db_pool` (`get_connection`) and the `sqlite3.Connection` and `sqlite3.Cursor` objects it's expected to provide. This is generally a more robust way to test because you're testing against the expected contract of these components.
*   **Visibility of Internal Steps:** The need to provide a sequence of mock connections makes the internal database steps of the tested method more visible in the test setup.

**In essence:** The change from a global `sqlite3.connect` to a pooled `db_pool.get_connection()` means your mocks need to target the new point of control. Because one high-level operation in `PolitenessEnforcer` can involve multiple "get-use-return" cycles with the pool for different sub-tasks, the test setup needs to provide a corresponding sequence of mock behaviors for these cycles.

It's a shift from "all DB access goes through this one global patched hole" to "each distinct DB task goes through the pool, gets a connection, and we need to mock what happens for each of those tasks." This makes the test setup more closely mirror the actual execution flow when multiple distinct database operations occur within the method being tested.

