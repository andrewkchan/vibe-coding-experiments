# SQLite Concurrency Issues in Crawler
_Exported on 5/28/2025 at 01:54:22 PDT from Cursor (0.50.5)_

---

**User**

Getting lots of "unable to open database file" sqlite errors in the initialization of my crawler in the frontier module, which spins up a bunch of asyncio workers that may try to do writes to the db. 

Any idea what could be happening? I'm pretty sure the sqlite client can access the db file just fine, is it possible there's some concurrency issue?



---

**Cursor**

I can see you're experiencing SQLite "unable to open database file" errors during crawler initialization. This looks like a classic SQLite concurrency issue, especially given that you're using multiple asyncio workers that may be trying to write to the database simultaneously.

Let me examine the frontier module to better understand the implementation:

Now let me check the storage manager to understand how the database connections are being managed:

Now let me check the orchestrator to see how the frontier is being initialized:

Let me check how many workers are being spawned and look at the configuration:

I can see the issue now. Looking at the log and the code:

1. **The crawler is using `max_workers=100`** (as shown in the log), which is quite high
2. The error "unable to open database file" often occurs when there are too many file descriptors/connections being opened
3. In the `_load_seeds` method, the code creates up to `max_workers` (100) concurrent tasks that all try to open database connections simultaneously

Here's what's happening:
- During initialization, `_load_seeds` splits the seed URLs among 100 workers
- Each worker calls `add_url` and `_mark_domain_as_seeded` 
- Each of these operations opens a new SQLite connection via `sqlite3.connect()`
- With 100 concurrent workers, you're trying to open 100+ database connections simultaneously
- This exhausts the file descriptor limit (note the "Too many open files" error in the log)

## Solutions:

### 1. **Immediate Fix - Reduce concurrent DB connections during initialization**
Modify the `_load_seeds` method to use fewer concurrent workers:

```python:crawler/crawler_module/frontier.py
async def _load_seeds(self):
    """Reads seed URLs from the seed file and adds them to the frontier."""
    if not self.config.seed_file.exists():
        logger.error(f"Seed file not found: {self.config.seed_file}")
        return

    added_count = 0
    urls = []
    
    # Limit concurrent workers for seed loading to avoid exhausting file descriptors
    max_seed_workers = min(10, self.config.max_workers)  # Cap at 10 concurrent DB operations
    
    async def _seed_worker(start, end):
        nonlocal added_count
        for url in urls[start: end]:
            await self.add_url(url)
            added_count +=1
            # Also mark domain as is_seeded = 1 in domain_metadata table
            domain = extract_domain(url)
            await asyncio.to_thread(self._mark_domain_as_seeded, domain)
            
    try:
        with open(self.config.seed_file, 'r') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith("#"):
                    urls.append(url)
                    
        num_per_worker = (len(urls) + max_seed_workers - 1) // max_seed_workers
        seed_workers = []
        for i in range(max_seed_workers):
            start = i * num_per_worker
            end = (i + 1) * num_per_worker
            seed_workers.append(_seed_worker(start, end))
            
        await asyncio.gather(*seed_workers)
        logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
    except IOError as e:
        logger.error(f"Error reading seed file {self.config.seed_file}: {e}")
```

### 2. **Better Solution - Use connection pooling or a single writer pattern**
Create a dedicated database writer that serializes all write operations:

```python:crawler/crawler_module/db_writer.py
import asyncio
import sqlite3
import logging
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)

class DatabaseWriter:
    """Manages database writes through a single connection to avoid concurrency issues."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._queue: asyncio.Queue = asyncio.Queue()
        self._writer_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the database writer task."""
        self._writer_task = asyncio.create_task(self._writer_loop())
        
    async def stop(self):
        """Stop the database writer task."""
        if self._writer_task:
            await self._queue.put(None)  # Sentinel to stop
            await self._writer_task
            
    async def _writer_loop(self):
        """Process database operations from the queue."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            while True:
                item = await self._queue.get()
                if item is None:  # Sentinel to stop
                    break
                    
                func, args, kwargs, future = item
                try:
                    result = func(conn, *args, **kwargs)
                    conn.commit()
                    future.set_result(result)
                except Exception as e:
                    conn.rollback()
                    future.set_exception(e)
        finally:
            conn.close()
            
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Queue a database operation for execution."""
        future = asyncio.Future()
        await self._queue.put((func, args, kwargs, future))
        return await future
```

### 3. **Alternative - Use WAL mode for better concurrency**
Enable Write-Ahead Logging mode in SQLite:

```python:crawler/crawler_module/storage.py
def _init_db(self):
    """Initializes the SQLite database and creates tables if they don't exist."""
    try:
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        
        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        logger.info(f"Connected to database: {self.db_path}")
        self._create_tables()
    except sqlite3.Error as e:
        logger.error(f"Database error during initialization: {e}")
        if self.conn:
            self.conn.close()
        raise
```

### 4. **System-level fix - Increase file descriptor limit**
Check and increase the file descriptor limit:

```bash
# Check current limit
ulimit -n

# Increase limit for current session
ulimit -n 4096

# Or permanently in /etc/security/limits.conf
# Add these lines:
# * soft nofile 4096
# * hard nofile 8192
```

The most robust solution would be to implement option 2 (database writer pattern) to ensure all writes go through a single connection, preventing these concurrency issues entirely. For now, option 1 (limiting concurrent workers during seed loading) should resolve the immediate problem.

```python
async def _load_seeds(self):
    """Reads seed URLs from the seed file and adds them to the frontier."""
    if not self.config.seed_file.exists():
        logger.error(f"Seed file not found: {self.config.seed_file}")
        return

    added_count = 0
    urls = []
    
    # Limit concurrent workers for seed loading to avoid exhausting file descriptors
    max_seed_workers = min(10, self.config.max_workers)  # Cap at 10 concurrent DB operations
    
    async def _seed_worker(start, end):
        nonlocal added_count
        for url in urls[start: end]:
            await self.add_url(url)
            added_count +=1
            # Also mark domain as is_seeded = 1 in domain_metadata table
            domain = extract_domain(url)
            await asyncio.to_thread(self._mark_domain_as_seeded, domain)
            
    try:
        with open(self.config.seed_file, 'r') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith("#"):
                    urls.append(url)
                    
        num_per_worker = (len(urls) + max_seed_workers - 1) // max_seed_workers
        seed_workers = []
        for i in range(max_seed_workers):
            start = i * num_per_worker
            end = (i + 1) * num_per_worker
            seed_workers.append(_seed_worker(start, end))
            
        await asyncio.gather(*seed_workers)
        logger.info(f"Loaded {added_count} URLs from seed file: {self.config.seed_file}")
    except IOError as e:
        logger.error(f"Error reading seed file {self.config.seed_file}: {e}")
```

```python
import asyncio
import sqlite3
import logging
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)

class DatabaseWriter:
    """Manages database writes through a single connection to avoid concurrency issues."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._queue: asyncio.Queue = asyncio.Queue()
        self._writer_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the database writer task."""
        self._writer_task = asyncio.create_task(self._writer_loop())
        
    async def stop(self):
        """Stop the database writer task."""
        if self._writer_task:
            await self._queue.put(None)  # Sentinel to stop
            await self._writer_task
            
    async def _writer_loop(self):
        """Process database operations from the queue."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            while True:
                item = await self._queue.get()
                if item is None:  # Sentinel to stop
                    break
                    
                func, args, kwargs, future = item
                try:
                    result = func(conn, *args, **kwargs)
                    conn.commit()
                    future.set_result(result)
                except Exception as e:
                    conn.rollback()
                    future.set_exception(e)
        finally:
            conn.close()
            
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Queue a database operation for execution."""
        future = asyncio.Future()
        await self._queue.put((func, args, kwargs, future))
        return await future
```

```python
def _init_db(self):
    """Initializes the SQLite database and creates tables if they don't exist."""
    try:
        self.conn = sqlite3.connect(self.db_path, timeout=10)
        
        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        logger.info(f"Connected to database: {self.db_path}")
        self._create_tables()
    except sqlite3.Error as e:
        logger.error(f"Database error during initialization: {e}")
        if self.conn:
            self.conn.close()
        raise
```

```shellscript
# Check current limit
ulimit -n

# Increase limit for current session
ulimit -n 4096

# Or permanently in /etc/security/limits.conf
# Add these lines:
# * soft nofile 4096
# * hard nofile 8192
```

---

**User**

hmm, the current limit seems pretty high

---

**Cursor**

You're absolutely right - with a file descriptor limit of 1,048,576, that's definitely not the issue. Let me look more closely at the error pattern in your log.

Looking at the log again, I notice something interesting:
1. There's an error about "Too many open files" for a network connection
2. Then immediately after, SQLite errors about "unable to open database file"

Let me check the exact error pattern more carefully:

So the "Too many open files" errors are coming from network connections, not SQLite. This suggests that the issue might be:

1. **Too many concurrent network connections** exhausting some resource (like ephemeral ports or socket buffers)
2. This is then causing SQLite to fail to open database files as a secondary effect

Let me check what's happening with the fetcher module and how many concurrent connections it's making:

I see the issue now. The aiohttp ClientSession is being created without any connection limits. Let me check if there's a connector configuration:

I found the issue! The `aiohttp.ClientSession` is created without specifying a `TCPConnector` with connection limits. By default, aiohttp has a limit of 100 connections per host, but **no global limit on total connections**.

With 100 workers all fetching different domains simultaneously, you can easily have thousands of concurrent connections, which would exhaust resources and cause the "Too many open files" error, even with a high ulimit.

Here's the fix - add connection limits to the aiohttp session:

```python:crawler/crawler_module/fetcher.py
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit
            limit_per_host=30,  # Connections per host
            ttl_dns_cache=300,  # DNS cache timeout
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession with connection limits")
    return self.session
```

Additionally, the SQLite errors are likely a cascading failure. When the system runs out of resources due to too many network connections, it can't open new file descriptors for SQLite connections either.

Another issue I noticed is that each worker might be creating its own Fetcher instance. Let me check the orchestrator:

```python
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit
            limit_per_host=30,  # Connections per host
            ttl_dns_cache=300,  # DNS cache timeout
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession with connection limits")
    return self.session
```

Good news! The orchestrator creates a single `Fetcher` instance (`self.fetcher`) that's shared among all workers. So we only need to fix the connection limits in one place.

Here's the complete solution:

**Fix the aiohttp connection limits in the Fetcher class:**

```python:crawler/crawler_module/fetcher.py
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession with connection limits (total: 200, per-host: 30)")
    return self.session
```

This will:
1. Limit total concurrent connections to 200 (reasonable for 100 workers)
2. Limit connections per host to 30 (prevents hammering single domains)
3. Enable cleanup of closed connections to free resources

The SQLite "unable to open database file" errors are likely a secondary effect of resource exhaustion from too many network connections. Once you limit the aiohttp connections, the SQLite errors should also disappear.

If you want to be extra safe with the database connections during initialization, you could also apply the seed loading limit I mentioned earlier, but the main issue is definitely the unlimited aiohttp connections.

```python
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession with connection limits (total: 200, per-host: 30)")
    return self.session
```

```python
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession with connection limits (total: 200, per-host: 30)")
    return self.session
```

---

**User**

Thanks! looks like i'm still hitting "too many open files" though?

---

**Cursor**

Looking at the error, you're still hitting "Too many open files" errors even after the connection limit fix. Let me check what the current implementation looks like and see if the fix was applied correctly:

Good! I can see the TCPConnector is properly configured with limits:
- `limit=200` (total connection pool)
- `limit_per_host=30` (per host limit)

However, you're still hitting "Too many open files" errors. Let me check a few things:

1. First, let's see how many workers are actually running:

So you're running with 100 workers. Even with the connection limits (200 total connections), there might be other file descriptors being consumed. Let me check if there are multiple processes or if the SQLite connections are still an issue:

I see it's all running in a single process (PID 11502). Let me check the timestamp of when you started getting these errors:

The issue might be that with 100 workers and various other file descriptors (SQLite connections, log files, etc.), you're still exceeding limits. Let me check how SQLite connections are being managed:

I see the issue! Each visited page creates a new SQLite connection in `add_visited_page` (line 169). With 100 workers processing URLs concurrently, this could create many simultaneous SQLite connections, each consuming a file descriptor.

Let me also check how the frontier is managing its database connections:

I can see the problem! Both `StorageManager` and `FrontierManager` are creating new SQLite connections for every single database operation. With 100 workers running concurrently, this means:

1. Each URL fetch creates HTTP connections (limited to 200 total)
2. Each URL added to frontier creates a new SQLite connection
3. Each visited page creates another SQLite connection
4. Each politeness check might create more connections

The cumulative effect with 100 workers could easily exceed your file descriptor limits. Here's what I recommend:

## Solution: Reduce File Descriptor Usage

### Option 1: Reduce the number of workers (Quick fix)
```python:crawler/crawler.py
# ... existing code ...
# Change from max_workers=100 to something more reasonable
config = CrawlerConfig(
    # ... other config ...
    max_workers=20,  # Reduced from 100
    # ... other config ...
)
```

### Option 2: Use a connection pool for SQLite (Better long-term solution)
Create a simple connection pool to reuse SQLite connections instead of creating new ones for each operation:

```python:crawler/crawler_module/db_pool.py
import sqlite3
import threading
from queue import Queue
import logging

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str, pool_size: int = 10, timeout: int = 10):
        self.db_path = db_path
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        
        # Pre-create connections
        for _ in range(pool_size):
            conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            self.pool.put(conn)
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.pool.get()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        try:
            # Test if connection is still alive
            conn.execute("SELECT 1")
            self.pool.put(conn)
        except sqlite3.Error:
            # Connection is dead, create a new one
            logger.warning("Dead connection detected, creating new one")
            new_conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            new_conn.execute("PRAGMA journal_mode=WAL")
            self.pool.put(new_conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass
```

### Option 3: Adjust system limits further (if needed)
Even though your limit is already high, you might want to check the per-process limit:

```bash
# Check current limit for the running process
cat /proc/$(pgrep -f crawler)/limits | grep "open files"

# Or set it explicitly before running the crawler
ulimit -n 65536  # Set a reasonable limit per process
```

### Option 4: Monitor file descriptor usage
Add monitoring to see what's consuming file descriptors:

```python:crawler/crawler_module/monitor.py
import os
import psutil
import logging

logger = logging.getLogger(__name__)

def log_fd_usage():
    """Log current file descriptor usage"""
    try:
        process = psutil.Process(os.getpid())
        num_fds = process.num_fds()
        
        # Get details about open files
        open_files = process.open_files()
        connections = process.connections()
        
        logger.info(f"File descriptors in use: {num_fds}")
        logger.info(f"Open files: {len(open_files)}")
        logger.info(f"Network connections: {len(connections)}")
        
        # Log some details for debugging
        file_types = {}
        for f in open_files:
            ext = f.path.split('.')[-1] if '.' in f.path else 'no_ext'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        logger.info(f"File types: {file_types}")
        
    except Exception as e:
        logger.error(f"Error monitoring FD usage: {e}")
```

Given that you're hitting the limit with the current setup, I'd recommend starting with **Option 1** (reducing workers to 20-30) as an immediate fix, then implementing **Option 2** (connection pooling) for better long-term performance. The connection pooling will significantly reduce the number of file descriptors used by SQLite connections.

```python
# ... existing code ...
# Change from max_workers=100 to something more reasonable
config = CrawlerConfig(
    # ... other config ...
    max_workers=20,  # Reduced from 100
    # ... other config ...
)
```

```python
import sqlite3
import threading
from queue import Queue
import logging

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str, pool_size: int = 10, timeout: int = 10):
        self.db_path = db_path
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        
        # Pre-create connections
        for _ in range(pool_size):
            conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            self.pool.put(conn)
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.pool.get()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        try:
            # Test if connection is still alive
            conn.execute("SELECT 1")
            self.pool.put(conn)
        except sqlite3.Error:
            # Connection is dead, create a new one
            logger.warning("Dead connection detected, creating new one")
            new_conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            new_conn.execute("PRAGMA journal_mode=WAL")
            self.pool.put(new_conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass
```

```shellscript
# Check current limit for the running process
cat /proc/$(pgrep -f crawler)/limits | grep "open files"

# Or set it explicitly before running the crawler
ulimit -n 65536  # Set a reasonable limit per process
```

```python
import os
import psutil
import logging

logger = logging.getLogger(__name__)

def log_fd_usage():
    """Log current file descriptor usage"""
    try:
        process = psutil.Process(os.getpid())
        num_fds = process.num_fds()
        
        # Get details about open files
        open_files = process.open_files()
        connections = process.connections()
        
        logger.info(f"File descriptors in use: {num_fds}")
        logger.info(f"Open files: {len(open_files)}")
        logger.info(f"Network connections: {len(connections)}")
        
        # Log some details for debugging
        file_types = {}
        for f in open_files:
            ext = f.path.split('.')[-1] if '.' in f.path else 'no_ext'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        logger.info(f"File types: {file_types}")
        
    except Exception as e:
        logger.error(f"Error monitoring FD usage: {e}")
```

---

**User**

I don't understand how 100 workers could exceed the file descriptor limit of over 1 million, doesn't each worker have at most a few SQLite connections open at a time (in fact I would expect at most one, since a worker will close each connection before opening a new one, including in the `add_visited_page` method)?

---

**Cursor**

You're absolutely right to question this! Let's think through the math more carefully.

With the code I reviewed:
- Each worker processes URLs sequentially (not concurrently within a worker)
- Each SQLite operation opens a connection, performs the operation, then closes it
- The aiohttp session has a limit of 200 total connections

So theoretically, at any given moment you should have at most:
- 200 HTTP connections (from the aiohttp limit)
- ~100 SQLite connections (if all workers happen to be in a DB operation simultaneously)
- A handful of other file descriptors (log files, Python internals, etc.)

This should be nowhere near 1 million file descriptors!

Let me investigate more carefully. The error message shows it's specifically happening during network connections. Let me check if there's something else going on:

Good, it looks like there's only one `Fetcher` instance with one `ClientSession`. Let me check if there might be a connection leak or if connections aren't being properly released:

The connector has `enable_cleanup_closed=True`, which should help. Let me look for DNS resolution issues or other potential problems:

Let me check the actual error pattern more carefully. The error says "ssl:default [Too many open files]" which suggests it's happening during SSL/TLS connection establishment:

Ah! Now I think I understand the issue better. Looking at the aiohttp documentation and the GitHub issue, the problem is likely **not** with SQLite at all, but with how the operating system counts file descriptors.

Let me check something:

Let me look at the actual error pattern in more detail:

Now I see the issue! The "unable to open database file" SQLite errors are happening **after** the "Too many open files" network errors. This suggests that once you hit the system's file descriptor limit, **everything** that tries to open a file (including SQLite trying to open the database) will fail.

The real question is: why are you hitting the file descriptor limit with what should be a reasonable setup?

A few possibilities:

1. **Connection leaks in aiohttp**: Even with the connection limits set, there might be connections not being properly closed, especially for failed connections.

2. **DNS resolution file descriptors**: The DNS resolver might be opening file descriptors that aren't being counted in the connection limit.

3. **Other system resources**: There might be other processes on the system consuming file descriptors.

Let me check the politeness module to see if it's creating additional connections:

Good, the politeness module is using the shared `Fetcher` instance, so it's not creating additional connections.

Based on my analysis, here's what's happening:

## The Real Issue

You're right to be confused - with proper connection limits and closing, 100 workers shouldn't exceed 1 million file descriptors. However, the issue is likely:

1. **Connection lifecycle timing**: Even though connections are properly closed, there's a window between when a connection is closed and when the OS actually releases the file descriptor. With 100 workers rapidly creating and closing connections, you can accumulate many file descriptors in the TIME_WAIT or CLOSE_WAIT state.

2. **DNS resolution overhead**: Each DNS lookup may create additional file descriptors that aren't counted in the aiohttp connection limit.

3. **Failed connection attempts**: Failed connections (like the "Too many open files" errors) might not be cleaned up as quickly as successful ones.

## Solutions

### 1. Add connection pooling timeouts and cleanup
The current setup already has most of the right settings, but you could try:

```python:crawler/crawler_module/fetcher.py
connector = aiohttp.TCPConnector(
    limit=200,  # Total connection pool limit
    limit_per_host=30,  # Max connections per host
    ttl_dns_cache=300,  # DNS cache timeout
    enable_cleanup_closed=True,  # Clean up closed connections
    force_close=True,  # Force close connections after each request
    keepalive_timeout=30,  # Reduce keepalive timeout
)
```

### 2. Add exponential backoff when hitting limits
When you get "Too many open files" errors, the crawler should back off:

```python:crawler/crawler_module/orchestrator.py
# ... existing code ...
async def _worker(self, worker_id: int):
    """Worker coroutine that processes URLs from the frontier."""
    consecutive_errors = 0
    while not self._stop_crawling:
        try:
            # ... existing code ...
        except OSError as e:
            if "Too many open files" in str(e):
                consecutive_errors += 1
                wait_time = min(60, 2 ** consecutive_errors)  # Exponential backoff
                logger.warning(f"Worker {worker_id}: Too many open files, backing off for {wait_time}s")
                await asyncio.sleep(wait_time)
            else:
                raise
```

### 3. Monitor and log file descriptor usage
Add periodic monitoring to understand what's consuming file descriptors:

```python:crawler/crawler_module/orchestrator.py
import os
import subprocess

async def _monitor_fd_usage(self):
    """Monitor file descriptor usage periodically."""
    while not self._stop_crawling:
        try:
            # Get current FD count
            pid = os.getpid()
            fd_count = len(os.listdir(f'/proc/{pid}/fd'))
            
            # Get connection stats
            active_connections = len(self.fetcher.session._connector._acquired) if self.fetcher.session else 0
            
            logger.info(f"FD usage: {fd_count} | Active connections: {active_connections}")
            
            if fd_count > 50000:  # Warning threshold
                logger.warning(f"High FD usage detected: {fd_count}")
                
        except Exception as e:
            logger.error(f"Error monitoring FD usage: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds
```

### 4. Reduce worker count (as you mentioned)
The simplest immediate fix is to reduce workers to 20-50 instead of 100. This gives more breathing room for the OS to clean up file descriptors between connections.

The key insight is that even with proper connection management, the OS needs time to fully release file descriptors, and with 100 workers aggressively opening connections, you can hit transient spikes that exceed the limit.

```python
connector = aiohttp.TCPConnector(
    limit=200,  # Total connection pool limit
    limit_per_host=30,  # Max connections per host
    ttl_dns_cache=300,  # DNS cache timeout
    enable_cleanup_closed=True,  # Clean up closed connections
    force_close=True,  # Force close connections after each request
    keepalive_timeout=30,  # Reduce keepalive timeout
)
```

```python
# ... existing code ...
async def _worker(self, worker_id: int):
    """Worker coroutine that processes URLs from the frontier."""
    consecutive_errors = 0
    while not self._stop_crawling:
        try:
            # ... existing code ...
        except OSError as e:
            if "Too many open files" in str(e):
                consecutive_errors += 1
                wait_time = min(60, 2 ** consecutive_errors)  # Exponential backoff
                logger.warning(f"Worker {worker_id}: Too many open files, backing off for {wait_time}s")
                await asyncio.sleep(wait_time)
            else:
                raise
```

```python
import os
import subprocess

async def _monitor_fd_usage(self):
    """Monitor file descriptor usage periodically."""
    while not self._stop_crawling:
        try:
            # Get current FD count
            pid = os.getpid()
            fd_count = len(os.listdir(f'/proc/{pid}/fd'))
            
            # Get connection stats
            active_connections = len(self.fetcher.session._connector._acquired) if self.fetcher.session else 0
            
            logger.info(f"FD usage: {fd_count} | Active connections: {active_connections}")
            
            if fd_count > 50000:  # Warning threshold
                logger.warning(f"High FD usage detected: {fd_count}")
                
        except Exception as e:
            logger.error(f"Error monitoring FD usage: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds
```

---

**User**

2025-05-28 08:08:52 - CRITICAL - crawler_module.orchestrator [11755] - Orchestrator critical error: keepalive_timeout cannot be set if force_close is True
Traceback (most recent call last):
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/orchestrator.py", line 150, in run_crawl
    await self.frontier.initialize_frontier()
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 61, in initialize_frontier
    await self._load_seeds() # add_url within is now thread-safe for DB ops
    ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 103, in _load_seeds
    await asyncio.gather(*seed_workers)
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 86, in _seed_worker
    await self.add_url(url)
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 163, in add_url
    if not await self.politeness.is_url_allowed(normalized_url):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/politeness.py", line 220, in is_url_allowed
    rerp = await self._get_robots_for_domain(domain)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/politeness.py", line 129, in _get_robots_for_domain
    fetch_result_http: FetchResult = await self.fetcher.fetch_url(robots_url_http, is_robots_txt=True)
                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/fetcher.py", line 67, in fetch_url
    session = await self._get_session()
              ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/fetcher.py", line 37, in _get_session
    connector = aiohttp.TCPConnector(
                ^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/.venv/lib/python3.12/site-packages/aiohttp/connector.py", line 857, in __init__
    super().__init__(
  File "/home/ubuntu/vibe-coding-experiments/crawler/.venv/lib/python3.12/site-packages/aiohttp/connector.py", line 255, in __init__
    raise ValueError(
ValueError: keepalive_timeout cannot be set if force_close is True


---

**Cursor**

Ah, I made an error in my suggestion! You can't use both `force_close=True` and `keepalive_timeout` together in aiohttp. They're mutually exclusive because:

- `force_close=True` means close the connection immediately after each request
- `keepalive_timeout` means keep the connection alive for reuse for a certain period

Let me check the current fetcher configuration:

I see the problem! The code has both `force_close=True` and `keepalive_timeout=30` on lines 41-42, which are incompatible. We need to fix this. Let's remove the conflicting parameters:

```python:crawler/crawler_module/fetcher.py
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
            # Remove the conflicting lines - choose one strategy:
            # Option 1: Keep connections alive for reuse (better performance)
            keepalive_timeout=30,  # Keep connections alive for 30 seconds
            # Option 2: Force close after each request (may help with FD limits)
            # force_close=True,  # Close connection after each request
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession")
    return self.session
```

Since you're experiencing "Too many open files" errors, let's go with the more aggressive approach of forcing connections to close:

```python:crawler/crawler_module/fetcher.py
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
            force_close=True,  # Force close connections after each request to free up FDs
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession")
    return self.session
```

This will:
1. Force connections to close after each request, which should help free up file descriptors more quickly
2. Still maintain the connection limits to prevent too many simultaneous connections
3. Keep DNS caching to reduce DNS lookups
4. Enable cleanup of closed connections

The tradeoff is slightly worse performance (no connection reuse) but better resource management, which seems appropriate given the file descriptor issues you're experiencing.

```python
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
            # Remove the conflicting lines - choose one strategy:
            # Option 1: Keep connections alive for reuse (better performance)
            keepalive_timeout=30,  # Keep connections alive for 30 seconds
            # Option 2: Force close after each request (may help with FD limits)
            # force_close=True,  # Close connection after each request
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession")
    return self.session
```

```python
async def _get_session(self) -> aiohttp.ClientSession:
    """Returns the existing aiohttp session or creates a new one."""
    if self.session is None or self.session.closed:
        # Create a connector with reasonable limits to prevent resource exhaustion
        connector = aiohttp.TCPConnector(
            limit=200,  # Total connection pool limit (across all hosts)
            limit_per_host=30,  # Max connections per host
            ttl_dns_cache=300,  # DNS cache timeout in seconds
            enable_cleanup_closed=True,  # Clean up closed connections
            force_close=True,  # Force close connections after each request to free up FDs
        )
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={"User-Agent": self.config.user_agent},
            connector=connector,
        )
        logger.debug("Created new aiohttp.ClientSession")
    return self.session
```

---

**User**

Ok, thanks. Running with max_workers=20 seems to be healthy so far.

I am currently writing to a log file which is getting pretty big, can I rotate the log without interrupting the process? I heard `logrotate` can do that

---

**Cursor**

Yes, you can absolutely use `logrotate` to rotate logs without interrupting your crawler process! This is a common practice for long-running applications.

Here's how to set it up:

## 1. Create a logrotate configuration file

Create a file like `/etc/logrotate.d/crawler` (you'll need sudo):

```bash
sudo nano /etc/logrotate.d/crawler
```

Add this configuration:

```
/home/ubuntu/vibe-coding-experiments/crawler/*.log {
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

Let me explain each option:
- `daily`: Rotate logs daily (you can also use `hourly`, `weekly`, etc.)
- `rotate 7`: Keep 7 rotated logs
- `maxsize 100M`: Also rotate if log exceeds 100MB (even before daily rotation)
- `missingok`: Don't error if log file is missing
- `notifempty`: Don't rotate empty logs
- `compress`: Compress rotated logs with gzip
- `delaycompress`: Compress on the next rotation (gives time for any processes still writing)
- `copytruncate`: **This is the key!** Copy the log file then truncate the original (allows the process to keep writing)
- `dateext`: Add date to rotated filename
- `dateformat`: Custom date format for rotated files

## 2. Test the configuration

You can test it manually without waiting for the scheduled rotation:

```bash
sudo logrotate -d /etc/logrotate.d/crawler  # Dry run (debug mode)
sudo logrotate -f /etc/logrotate.d/crawler  # Force rotation now
```

## 3. Important note about `copytruncate`

The `copytruncate` option is what allows rotation without interrupting the process. Here's what happens:

1. Logrotate copies the current log file to a new name
2. It then truncates (empties) the original file
3. Your crawler keeps writing to the same file handle (now empty)

There's a tiny window between copy and truncate where a few log lines might be lost, but it's usually negligible.

## 4. Alternative: Configure Python logging to handle rotation

If you want to avoid even that tiny data loss window, you can configure Python's logging to handle rotation internally:

```python
import logging
from logging.handlers import RotatingFileHandler

# In your logging setup
handler = RotatingFileHandler(
    'crawler.log',
    maxBytes=100*1024*1024,  # 100MB
    backupCount=7
)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
))
logger.addHandler(handler)
```

But since you already have a running process, the logrotate approach with `copytruncate` is your best bet.

## 5. Check if logrotate is running

Logrotate usually runs via cron. Check if it's scheduled:

```bash
cat /etc/cron.daily/logrotate
# or
systemctl status logrotate.timer  # On systems using systemd timers
```

If you need more frequent rotation (e.g., hourly), you can add a cron job:

```bash
# Add to crontab
0 * * * * /usr/sbin/logrotate /etc/logrotate.d/crawler
```

This setup will keep your logs manageable without any interruption to your crawler!

```shellscript
sudo nano /etc/logrotate.d/crawler
```

```plaintext
/home/ubuntu/vibe-coding-experiments/crawler/*.log {
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

```shellscript
sudo logrotate -d /etc/logrotate.d/crawler  # Dry run (debug mode)
sudo logrotate -f /etc/logrotate.d/crawler  # Force rotation now
```

```python
import logging
from logging.handlers import RotatingFileHandler

# In your logging setup
handler = RotatingFileHandler(
    'crawler.log',
    maxBytes=100*1024*1024,  # 100MB
    backupCount=7
)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
))
logger.addHandler(handler)
```

```shellscript
cat /etc/cron.daily/logrotate
# or
systemctl status logrotate.timer  # On systems using systemd timers
```

```shellscript
# Add to crontab
0 * * * * /usr/sbin/logrotate /etc/logrotate.d/crawler
```

---

**User**

hmm, running the logrotate dry run and getting an error

---

**Cursor**

Ah, logrotate is complaining about the directory permissions being too open. This is a security feature to prevent potential privilege escalation. Here's how to fix it:

## Option 1: Fix the directory permissions (Recommended)

Check and fix the permissions on the parent directory:

```bash
# Check current permissions
ls -ld /home/ubuntu/vibe-coding-experiments/crawler/

# Fix permissions - remove group/world write permissions
chmod 755 /home/ubuntu/vibe-coding-experiments/crawler/

# Or if you want to be more restrictive
chmod 750 /home/ubuntu/vibe-coding-experiments/crawler/
```

## Option 2: Add the `su` directive to your logrotate config

Edit your logrotate configuration to specify which user should perform the rotation:

```bash
sudo nano /etc/logrotate.d/crawler
```

Add the `su` directive:

```
/home/ubuntu/vibe-coding-experiments/crawler/*.log {
    su ubuntu ubuntu
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

The `su ubuntu ubuntu` tells logrotate to switch to user `ubuntu` and group `ubuntu` when rotating these logs.

## Option 3: Create a dedicated log directory with proper permissions

```bash
# Create a dedicated logs directory
mkdir -p /home/ubuntu/vibe-coding-experiments/crawler/logs

# Set appropriate permissions
chmod 755 /home/ubuntu/vibe-coding-experiments/crawler/logs

# Move your log file there (after stopping the crawler briefly)
mv /home/ubuntu/vibe-coding-experiments/crawler/*.log /home/ubuntu/vibe-coding-experiments/crawler/logs/

# Update your logrotate config to point to the new location
```

Then update the logrotate config path:
```
/home/ubuntu/vibe-coding-experiments/crawler/logs/*.log {
    su ubuntu ubuntu
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

After making these changes, test again:

```bash
# Test with debug mode
sudo logrotate -d /etc/logrotate.d/crawler

# If it looks good, force a rotation
sudo logrotate -f /etc/logrotate.d/crawler
```

I'd recommend Option 1 or 2 - just fix the permissions or add the `su` directive. The directory probably got world-writable permissions from some earlier operation, and tightening them to 755 is generally a good security practice anyway.

```shellscript
# Check current permissions
ls -ld /home/ubuntu/vibe-coding-experiments/crawler/

# Fix permissions - remove group/world write permissions
chmod 755 /home/ubuntu/vibe-coding-experiments/crawler/

# Or if you want to be more restrictive
chmod 750 /home/ubuntu/vibe-coding-experiments/crawler/
```

```shellscript
sudo nano /etc/logrotate.d/crawler
```

```plaintext
/home/ubuntu/vibe-coding-experiments/crawler/*.log {
    su ubuntu ubuntu
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

```shellscript
# Create a dedicated logs directory
mkdir -p /home/ubuntu/vibe-coding-experiments/crawler/logs

# Set appropriate permissions
chmod 755 /home/ubuntu/vibe-coding-experiments/crawler/logs

# Move your log file there (after stopping the crawler briefly)
mv /home/ubuntu/vibe-coding-experiments/crawler/*.log /home/ubuntu/vibe-coding-experiments/crawler/logs/

# Update your logrotate config to point to the new location
```

```plaintext
/home/ubuntu/vibe-coding-experiments/crawler/logs/*.log {
    su ubuntu ubuntu
    daily
    rotate 7
    maxsize 100M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
}
```

```shellscript
# Test with debug mode
sudo logrotate -d /etc/logrotate.d/crawler

# If it looks good, force a rotation
sudo logrotate -f /etc/logrotate.d/crawler
```

---

**User**

Great, the logrotate cron job looks like so, is it correct that this will rotate my logs according to `/etc/logrotate.d/crawler` daily?

---

**Cursor**

Yes, but there's an important detail to note! The cron script shows that on systems with systemd (which Ubuntu uses), it exits early:

```bash
# skip in favour of systemd timer
if [ -d /run/systemd/system ]; then
    exit 0
fi
```

This means on your system, logrotate is actually run by a **systemd timer** instead of cron. Let's check:

```bash
# Check the systemd timer status
systemctl status logrotate.timer

# See when it will next run
systemctl list-timers logrotate.timer

# Check the timer configuration
systemctl cat logrotate.timer
```

The systemd timer typically runs logrotate daily at a specific time (often around midnight or 00:00).

To confirm your `/etc/logrotate.d/crawler` config will be included:

```bash
# Check the main logrotate config
grep "include" /etc/logrotate.conf
```

You should see something like:
```
include /etc/logrotate.d
```

This means logrotate will process all files in `/etc/logrotate.d/`, including your `crawler` config.

So yes, your logs will be rotated daily according to your configuration! The workflow is:
1. systemd timer triggers logrotate daily
2. logrotate reads `/etc/logrotate.conf`
3. Which includes all configs in `/etc/logrotate.d/`
4. Including your `/etc/logrotate.d/crawler` config

If you want to rotate more frequently than daily, you can:
- Add your own cron job: `0 */6 * * * /usr/sbin/logrotate /etc/logrotate.d/crawler` (every 6 hours)
- Or create a custom systemd timer
- Or just manually run `sudo logrotate /etc/logrotate.d/crawler` when needed

```shellscript
# skip in favour of systemd timer
if [ -d /run/systemd/system ]; then
    exit 0
fi
```

```shellscript
# Check the systemd timer status
systemctl status logrotate.timer

# See when it will next run
systemctl list-timers logrotate.timer

# Check the timer configuration
systemctl cat logrotate.timer
```

```shellscript
# Check the main logrotate config
grep "include" /etc/logrotate.conf
```

```plaintext
include /etc/logrotate.d
```

