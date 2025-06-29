# Debugging file descriptor leak in web crawler
_Exported on 6/28/2025 at 21:25:31 PDT from Cursor (1.0.0)_

---

**User**

I think there is a file descriptor leak in my web crawler. See the attached screenshot for the metrics (the metric logging is in orchestrator.py). AFAIK file descriptors are created by the aiohttp connection pool (in the fetcher), the redis client (used to manipulate the frontier in both the main process and parsers), and the frontier file readers/writers (main process reads, parsers write) - there may be others but those are what I can think of, see PLAN.MD for an overview of how the crawler works. Note the metric is a bit noisy but the trend becomes clear over an hour or so. Thoughts? How can we debug this? Let's discuss before coding

---

**Cursor**

Looking at the Open FDs graph, there's a clear upward trend from ~1,500 to over 4,000 file descriptors over about 11 hours. This is definitely a file descriptor leak. I also notice that the crawl rate drops to 0 around 11:00, which is concerning.

Based on the code, here are the main sources of file descriptors:

1. **aiohttp connections (fetcher.py)**:
   - Creates a TCPConnector with connection pooling
   - Has `force_close=True` which *should* close connections after each request
   - But the session is shared across all fetcher workers

2. **Redis connections**:
   - Two Redis clients in the orchestrator (`redis_client` and `redis_client_binary`)
   - Each parser process creates its own Redis clients
   - Redis uses connection pooling internally

3. **Frontier files**:
   - Read operations in `_get_url_from_domain()` 
   - Write operations in `_add_urls_to_domain()`
   - Both use `async with` context managers

4. **Other potential sources**:
   - Parser process lifecycle issues
   - Temporary files during parsing
   - Prometheus metrics server

The fact that crawl rate drops to 0 while FDs continue climbing suggests the crawler might be stuck or hitting errors that prevent cleanup.

Here's how we can debug this:

1. **Add FD tracking by component**:
   ```python
   # Track FDs before/after major operations
   import resource
   soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
   current_fds = len(os.listdir('/proc/self/fd'))
   ```

2. **Use lsof to inspect open files**:
   ```bash
   # While crawler is running with high FD count:
   lsof -p <crawler_pid> | grep -E "(ESTABLISHED|CLOSE_WAIT|REG)" | sort | uniq -c | sort -nr
   ```

3. **Check for specific leak patterns**:
   - Are they TCP sockets? (aiohttp/Redis connections)
   - Are they regular files? (frontier files)
   - Are they in CLOSE_WAIT state? (not properly closed)

4. **Add connection monitoring**:
   - Log aiohttp connector stats: `connector._conns`
   - Monitor Redis connection pool: `redis_client.connection_pool._created_connections`

5. **Check for exception paths**:
   - Any exceptions during file operations that bypass cleanup?
   - Parser processes crashing and restarting?

My initial suspicions:

1. **aiohttp**: Despite `force_close=True`, connections might be leaking if requests fail or timeout
2. **Redis connections**: The monkey-patching of `get_connection` might interfere with proper cleanup
3. **Parser process restarts**: The code shows parser processes can die and restart - are they cleaning up properly?

Before coding fixes, could you:
1. Run `lsof -p <pid>` on the crawler process and share what types of FDs are accumulating?
2. Check if there are any error messages in the logs around when the crawl rate drops?
3. Let me know if the parser processes are restarting frequently?

This will help narrow down which component is leaking FDs.

```python
   # Track FDs before/after major operations
   import resource
   soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
   current_fds = len(os.listdir('/proc/self/fd'))
```

```shellscript
   # While crawler is running with high FD count:
   lsof -p <crawler_pid> | grep -E "(ESTABLISHED|CLOSE_WAIT|REG)" | sort | uniq -c | sort -nr
```

---

**User**

The open FDs are extremely high churn and the metric is noisy. Is it a problem for `lsof` if the process is creating and destroying hundreds to thousands of FDs each minute?

---

**Cursor**

You're absolutely right - with high FD churn (hundreds to thousands per minute), `lsof` will give you an inconsistent snapshot that's hard to interpret. By the time it reads `/proc/<pid>/fd/`, many FDs will have changed.

For high-churn scenarios, here are better debugging approaches:

1. **Periodic FD type sampling**:
   ```python
   # Add this to your metrics logging interval
   async def _log_fd_types(self):
       try:
           fd_types = defaultdict(int)
           fd_dir = f'/proc/{os.getpid()}/fd'
           for fd in os.listdir(fd_dir):
               try:
                   link = os.readlink(f'{fd_dir}/{fd}')
                   if 'socket:' in link:
                       fd_types['socket'] += 1
                   elif 'pipe:' in link:
                       fd_types['pipe'] += 1
                   elif link.startswith('/'):
                       if 'frontier' in link:
                           fd_types['frontier_file'] += 1
                       else:
                           fd_types['regular_file'] += 1
                   else:
                       fd_types['other'] += 1
               except:
                   pass  # FD might have closed
           logger.info(f"FD Types: {dict(fd_types)}")
       except:
           pass
   ```

2. **Track component-specific FD creation/destruction**:
   - Wrap aiohttp requests with FD counting
   - Count before/after Redis operations
   - Monitor frontier file operations

3. **Use SystemTap or eBPF** for precise tracking:
   ```bash
   # Count file opens/closes by type
   sudo bpftrace -e '
   tracepoint:syscalls:sys_enter_open* { @opens[comm] = count(); }
   tracepoint:syscalls:sys_enter_close { @closes[comm] = count(); }
   interval:s:10 { print(@opens); print(@closes); clear(@opens); clear(@closes); }'
   ```

4. **Add connection pool metrics**:
   ```python
   # In your metrics logging
   if self.session and hasattr(self.session.connector, '_conns'):
       active_conns = sum(len(conns) for conns in self.session.connector._conns.values())
       logger.info(f"aiohttp active connections: {active_conns}")
   ```

5. **Instrument with context managers** to track specific operations:
   ```python
   @contextmanager
   def track_fds(operation_name):
       before = len(os.listdir(f'/proc/{os.getpid()}/fd'))
       yield
       after = len(os.listdir(f'/proc/{os.getpid()}/fd'))
       if after > before + 10:  # Threshold for logging
           logger.warning(f"{operation_name} leaked {after - before} FDs")
   ```

Given the high churn and the fact that the leak is gradual (increasing from 1,500 to 4,000 over 11 hours), I'd focus on:

1. **Tracking FD growth rate** - Are we leaking a consistent X FDs per thousand requests?
2. **Correlating with operations** - Does the leak rate increase during specific activities (errors, timeouts, parser restarts)?
3. **Looking for patterns** - Is the leak happening in bursts (suggesting error conditions) or steadily (suggesting every Nth operation leaks)?

The drop in crawl rate at 11:00 while FDs continue climbing is particularly interesting - it suggests the crawler hit some error condition that stopped fetching but continued leaking FDs. Check logs around that time for errors or unusual behavior.

Would you like me to add some lightweight FD tracking instrumentation to the code that can handle the high churn?

```python
   # Add this to your metrics logging interval
   async def _log_fd_types(self):
       try:
           fd_types = defaultdict(int)
           fd_dir = f'/proc/{os.getpid()}/fd'
           for fd in os.listdir(fd_dir):
               try:
                   link = os.readlink(f'{fd_dir}/{fd}')
                   if 'socket:' in link:
                       fd_types['socket'] += 1
                   elif 'pipe:' in link:
                       fd_types['pipe'] += 1
                   elif link.startswith('/'):
                       if 'frontier' in link:
                           fd_types['frontier_file'] += 1
                       else:
                           fd_types['regular_file'] += 1
                   else:
                       fd_types['other'] += 1
               except:
                   pass  # FD might have closed
           logger.info(f"FD Types: {dict(fd_types)}")
       except:
           pass
```

```shellscript
   # Count file opens/closes by type
   sudo bpftrace -e '
   tracepoint:syscalls:sys_enter_open* { @opens[comm] = count(); }
   tracepoint:syscalls:sys_enter_close { @closes[comm] = count(); }
   interval:s:10 { print(@opens); print(@closes); clear(@opens); clear(@closes); }'
```

```python
   # In your metrics logging
   if self.session and hasattr(self.session.connector, '_conns'):
       active_conns = sum(len(conns) for conns in self.session.connector._conns.values())
       logger.info(f"aiohttp active connections: {active_conns}")
```

```python
   @contextmanager
   def track_fds(operation_name):
       before = len(os.listdir(f'/proc/{os.getpid()}/fd'))
       yield
       after = len(os.listdir(f'/proc/{os.getpid()}/fd'))
       if after > before + 10:  # Threshold for logging
           logger.warning(f"{operation_name} leaked {after - before} FDs")
```

---

**User**

Those are great suggestions. Before we add anything, I tried taking your FD logging script and instead logging the FD and link for every FD, here is a sample for the fetcher process:
```
FD: 0, Link: /dev/pts/1
FD: 1, Link: pipe:[4486164]
FD: 2, Link: pipe:[4486164]
FD: 3, Link: /tmp/prometheus_multiproc/counter_172915.db
FD: 4, Link: /tmp/prometheus_multiproc/counter_172915.db
FD: 5, Link: /tmp/prometheus_multiproc/gauge_livesum_172915.db
FD: 6, Link: /tmp/prometheus_multiproc/gauge_livesum_172915.db
FD: 7, Link: /tmp/prometheus_multiproc/gauge_livemax_172915.db
FD: 8, Link: /tmp/prometheus_multiproc/gauge_livemax_172915.db
FD: 9, Link: /tmp/prometheus_multiproc/gauge_livemin_172915.db
FD: 10, Link: /tmp/prometheus_multiproc/gauge_livemin_172915.db
FD: 11, Link: /tmp/prometheus_multiproc/histogram_172915.db
FD: 12, Link: /tmp/prometheus_multiproc/histogram_172915.db
FD: 13, Link: /tmp/prometheus_multiproc/gauge_liveall_172915.db
FD: 14, Link: /tmp/prometheus_multiproc/gauge_liveall_172915.db
FD: 15, Link: anon_inode:[eventpoll]
FD: 16, Link: socket:[4484729]
FD: 17, Link: socket:[4484730]
FD: 18, Link: socket:[4484731]
FD: 19, Link: socket:[4486543]
FD: 20, Link: pipe:[4488382]
FD: 21, Link: pipe:[4488384]
```

It's not clear to me how do differentiate redis client connections from aiohttp connections (I think they both use sockets), do you know how to do that? Wondering if we can follow the socket link somehow and get info about what it's for

---

**Cursor**

Great sample! You're right that both Redis and aiohttp use sockets. Here's how to differentiate them:

The socket numbers (like `socket:[4484729]`) are inode numbers. We can match these against `/proc/net/tcp` and `/proc/net/tcp6` to get connection details:

```python
async def _log_fd_details(self):
    """Log detailed FD information including socket connections."""
    try:
        pid = os.getpid()
        
        # First, read socket inode -> connection mapping
        socket_map = {}
        
        # Parse /proc/net/tcp and /proc/net/tcp6
        for proto_file in ['/proc/net/tcp', '/proc/net/tcp6']:
            try:
                with open(proto_file, 'r') as f:
                    lines = f.readlines()[1:]  # Skip header
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) >= 10:
                            local_addr = parts[1]  # hex IP:port
                            remote_addr = parts[2]  # hex IP:port
                            inode = parts[9]
                            
                            # Parse hex addresses
                            local_ip, local_port = local_addr.split(':')
                            remote_ip, remote_port = remote_addr.split(':')
                            local_port = int(local_port, 16)
                            remote_port = int(remote_port, 16)
                            
                            socket_map[inode] = {
                                'local_port': local_port,
                                'remote_port': remote_port,
                                'proto': proto_file.split('/')[-1]
                            }
            except:
                pass
        
        # Now check FDs
        fd_stats = {
            'redis': 0,
            'http': 0,
            'https': 0,
            'frontier_files': 0,
            'prometheus': 0,
            'other_sockets': 0,
            'other': 0
        }
        
        fd_dir = f'/proc/{pid}/fd'
        for fd in os.listdir(fd_dir):
            try:
                link = os.readlink(f'{fd_dir}/{fd}')
                
                if 'socket:' in link:
                    # Extract inode from socket:[12345]
                    inode = link.split('[')[1].split(']')[0]
                    
                    if inode in socket_map:
                        info = socket_map[inode]
                        # Redis typically on port 6379
                        if info['remote_port'] == 6379 or info['local_port'] == 6379:
                            fd_stats['redis'] += 1
                        # HTTP/HTTPS
                        elif info['remote_port'] == 443:
                            fd_stats['https'] += 1
                        elif info['remote_port'] == 80:
                            fd_stats['http'] += 1
                        else:
                            fd_stats['other_sockets'] += 1
                    else:
                        fd_stats['other_sockets'] += 1
                        
                elif '/frontier' in link:
                    fd_stats['frontier_files'] += 1
                elif '/prometheus' in link:
                    fd_stats['prometheus'] += 1
                else:
                    fd_stats['other'] += 1
                    
            except:
                pass  # FD might have closed
                
        logger.info(f"FD breakdown: {fd_stats}")
        
        # Also log connection pool stats
        if hasattr(self, 'redis_client') and hasattr(self.redis_client.connection_pool, '_created_connections'):
            logger.info(f"Redis pool connections: {self.redis_client.connection_pool._created_connections}")
        
        if hasattr(self, 'fetcher') and hasattr(self.fetcher, 'session') and self.fetcher.session:
            connector = self.fetcher.session.connector
            if hasattr(connector, '_acquired'):
                logger.info(f"aiohttp acquired connections: {len(connector._acquired)}")
            if hasattr(connector, '_conns'):
                total_conns = sum(len(conns) for conns in connector._conns.values())
                logger.info(f"aiohttp total cached connections: {total_conns}")
                
    except Exception as e:
        logger.error(f"Error in FD analysis: {e}")
```

For your immediate case, you can also use `ss` command for a quick check:
```bash
# Show all connections for the process
ss -tupn | grep "pid=$CRAWLER_PID"

# Count connections by type
ss -tupn | grep "pid=$CRAWLER_PID" | grep ":6379" | wc -l  # Redis
ss -tupn | grep "pid=$CRAWLER_PID" | grep -E ":(80|443)" | wc -l  # HTTP/HTTPS
```

Looking at your sample:
- FDs 3-14 are Prometheus multiprocess metrics files (these are memory-mapped, not a leak concern)
- FDs 16-19 are sockets (need to check ports to identify)
- FDs 20-21 are pipes (likely asyncio internal communication)

The fact that you have so few sockets in this snapshot suggests either:
1. The leak is bursty (happens during specific operations)
2. The leaked FDs are files, not sockets
3. The high churn makes it hard to catch the leak in a snapshot

Would you like to add this socket identification code to your metrics logging to track Redis vs HTTP connections over time?

```python
async def _log_fd_details(self):
    """Log detailed FD information including socket connections."""
    try:
        pid = os.getpid()
        
        # First, read socket inode -> connection mapping
        socket_map = {}
        
        # Parse /proc/net/tcp and /proc/net/tcp6
        for proto_file in ['/proc/net/tcp', '/proc/net/tcp6']:
            try:
                with open(proto_file, 'r') as f:
                    lines = f.readlines()[1:]  # Skip header
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) >= 10:
                            local_addr = parts[1]  # hex IP:port
                            remote_addr = parts[2]  # hex IP:port
                            inode = parts[9]
                            
                            # Parse hex addresses
                            local_ip, local_port = local_addr.split(':')
                            remote_ip, remote_port = remote_addr.split(':')
                            local_port = int(local_port, 16)
                            remote_port = int(remote_port, 16)
                            
                            socket_map[inode] = {
                                'local_port': local_port,
                                'remote_port': remote_port,
                                'proto': proto_file.split('/')[-1]
                            }
            except:
                pass
        
        # Now check FDs
        fd_stats = {
            'redis': 0,
            'http': 0,
            'https': 0,
            'frontier_files': 0,
            'prometheus': 0,
            'other_sockets': 0,
            'other': 0
        }
        
        fd_dir = f'/proc/{pid}/fd'
        for fd in os.listdir(fd_dir):
            try:
                link = os.readlink(f'{fd_dir}/{fd}')
                
                if 'socket:' in link:
                    # Extract inode from socket:[12345]
                    inode = link.split('[')[1].split(']')[0]
                    
                    if inode in socket_map:
                        info = socket_map[inode]
                        # Redis typically on port 6379
                        if info['remote_port'] == 6379 or info['local_port'] == 6379:
                            fd_stats['redis'] += 1
                        # HTTP/HTTPS
                        elif info['remote_port'] == 443:
                            fd_stats['https'] += 1
                        elif info['remote_port'] == 80:
                            fd_stats['http'] += 1
                        else:
                            fd_stats['other_sockets'] += 1
                    else:
                        fd_stats['other_sockets'] += 1
                        
                elif '/frontier' in link:
                    fd_stats['frontier_files'] += 1
                elif '/prometheus' in link:
                    fd_stats['prometheus'] += 1
                else:
                    fd_stats['other'] += 1
                    
            except:
                pass  # FD might have closed
                
        logger.info(f"FD breakdown: {fd_stats}")
        
        # Also log connection pool stats
        if hasattr(self, 'redis_client') and hasattr(self.redis_client.connection_pool, '_created_connections'):
            logger.info(f"Redis pool connections: {self.redis_client.connection_pool._created_connections}")
        
        if hasattr(self, 'fetcher') and hasattr(self.fetcher, 'session') and self.fetcher.session:
            connector = self.fetcher.session.connector
            if hasattr(connector, '_acquired'):
                logger.info(f"aiohttp acquired connections: {len(connector._acquired)}")
            if hasattr(connector, '_conns'):
                total_conns = sum(len(conns) for conns in connector._conns.values())
                logger.info(f"aiohttp total cached connections: {total_conns}")
                
    except Exception as e:
        logger.error(f"Error in FD analysis: {e}")
```

```shellscript
# Show all connections for the process
ss -tupn | grep "pid=$CRAWLER_PID"

# Count connections by type
ss -tupn | grep "pid=$CRAWLER_PID" | grep ":6379" | wc -l  # Redis
ss -tupn | grep "pid=$CRAWLER_PID" | grep -E ":(80|443)" | wc -l  # HTTP/HTTPS
```

---

**User**

What I sent you is just a piece of the FDs dump.

This modified function is perfect thanks. Yeah can we add this to the orchestrator and run it in the periodic metrics logger, and add metrics to track all the different FD type counts (including the separate socket types)?

---

**Cursor**

I'll add the FD tracking to the orchestrator with Prometheus metrics for each FD type. Let me first add the new metrics definitions:

```python
open_fds_gauge = Gauge(
    'crawler_open_fds', 
    'Number of open file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

->

open_fds_gauge = Gauge(
    'crawler_open_fds', 
    'Number of open file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

# FD type breakdown metrics
fd_redis_gauge = Gauge(
    'crawler_fd_redis_connections',
    'Number of Redis connection file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_http_gauge = Gauge(
    'crawler_fd_http_connections',
    'Number of HTTP connection file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_https_gauge = Gauge(
    'crawler_fd_https_connections',
    'Number of HTTPS connection file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_frontier_files_gauge = Gauge(
    'crawler_fd_frontier_files',
    'Number of frontier file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_prometheus_gauge = Gauge(
    'crawler_fd_prometheus_files',
    'Number of Prometheus metric file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_other_sockets_gauge = Gauge(
    'crawler_fd_other_sockets',
    'Number of other socket file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_pipes_gauge = Gauge(
    'crawler_fd_pipes',
    'Number of pipe file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)

fd_other_gauge = Gauge(
    'crawler_fd_other',
    'Number of other file descriptors',
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
from .metrics import (
    start_metrics_server,
    pages_crawled_counter,
    urls_added_counter,
    errors_counter,
    backpressure_events_counter,
    pages_per_second_gauge,
    frontier_size_gauge,
    active_workers_gauge,
    memory_usage_gauge,
    open_fds_gauge,
    db_pool_available_gauge,
    fetch_duration_histogram,
    db_connection_acquire_histogram,
    db_query_duration_histogram,
    content_size_histogram,
    # System metrics
    cpu_usage_gauge,
    memory_free_gauge,
    memory_available_gauge,
    disk_free_gauge,
    disk_usage_percent_gauge,
    io_read_count_gauge,
    io_write_count_gauge,
    io_read_bytes_gauge,
    io_write_bytes_gauge,
    network_bytes_sent_gauge,
    network_bytes_recv_gauge,
    network_packets_sent_gauge,
    network_packets_recv_gauge,
    # Redis metrics
    redis_ops_per_sec_gauge,
    redis_memory_usage_gauge,
    redis_connected_clients_gauge,
    redis_hit_rate_gauge,
    redis_latency_histogram
)

->

from .metrics import (
    start_metrics_server,
    pages_crawled_counter,
    urls_added_counter,
    errors_counter,
    backpressure_events_counter,
    pages_per_second_gauge,
    frontier_size_gauge,
    active_workers_gauge,
    memory_usage_gauge,
    open_fds_gauge,
    db_pool_available_gauge,
    fetch_duration_histogram,
    db_connection_acquire_histogram,
    db_query_duration_histogram,
    content_size_histogram,
    # System metrics
    cpu_usage_gauge,
    memory_free_gauge,
    memory_available_gauge,
    disk_free_gauge,
    disk_usage_percent_gauge,
    io_read_count_gauge,
    io_write_count_gauge,
    io_read_bytes_gauge,
    io_write_bytes_gauge,
    network_bytes_sent_gauge,
    network_bytes_recv_gauge,
    network_packets_sent_gauge,
    network_packets_recv_gauge,
    # Redis metrics
    redis_ops_per_sec_gauge,
    redis_memory_usage_gauge,
    redis_connected_clients_gauge,
    redis_hit_rate_gauge,
    redis_latency_histogram,
    # FD type metrics
    fd_redis_gauge,
    fd_http_gauge,
    fd_https_gauge,
    fd_frontier_files_gauge,
    fd_prometheus_gauge,
    fd_other_sockets_gauge,
    fd_pipes_gauge,
    fd_other_gauge
)
```

```python
    async def _log_metrics(self):
        current_time = time.time()
        time_elapsed_seconds = current_time - self.last_metrics_log_time
        if time_elapsed_seconds == 0: time_elapsed_seconds = 1 # Avoid division by zero

        pages_per_second = self.pages_crawled_in_interval / time_elapsed_seconds
        logger.info(f"[Metrics] Pages Crawled/sec: {pages_per_second:.2f}")
        
        # Update Prometheus gauge
        pages_per_second_gauge.set(pages_per_second)

->

    async def _analyze_fd_types(self):
        """Analyze FD types and update Prometheus metrics."""
        try:
            pid = os.getpid()
            
            # First, read socket inode -> connection mapping
            socket_map = {}
            
            # Parse /proc/net/tcp and /proc/net/tcp6
            for proto_file in ['/proc/net/tcp', '/proc/net/tcp6']:
                try:
                    with open(proto_file, 'r') as f:
                        lines = f.readlines()[1:]  # Skip header
                        for line in lines:
                            parts = line.strip().split()
                            if len(parts) >= 10:
                                local_addr = parts[1]  # hex IP:port
                                remote_addr = parts[2]  # hex IP:port
                                inode = parts[9]
                                
                                # Parse hex addresses
                                local_ip, local_port = local_addr.split(':')
                                remote_ip, remote_port = remote_addr.split(':')
                                local_port = int(local_port, 16)
                                remote_port = int(remote_port, 16)
                                
                                socket_map[inode] = {
                                    'local_port': local_port,
                                    'remote_port': remote_port,
                                    'proto': proto_file.split('/')[-1]
                                }
                except:
                    pass
            
            # Now check FDs
            fd_stats = {
                'redis': 0,
                'http': 0,
                'https': 0,
                'frontier_files': 0,
                'prometheus': 0,
                'other_sockets': 0,
                'pipes': 0,
                'other': 0
            }
            
            fd_dir = f'/proc/{pid}/fd'
            for fd in os.listdir(fd_dir):
                try:
                    link = os.readlink(f'{fd_dir}/{fd}')
                    
                    if 'socket:' in link:
                        # Extract inode from socket:[12345]
                        inode = link.split('[')[1].split(']')[0]
                        
                        if inode in socket_map:
                            info = socket_map[inode]
                            # Redis typically on port 6379
                            if info['remote_port'] == 6379 or info['local_port'] == 6379:
                                fd_stats['redis'] += 1
                            # HTTP/HTTPS
                            elif info['remote_port'] == 443:
                                fd_stats['https'] += 1
                            elif info['remote_port'] == 80:
                                fd_stats['http'] += 1
                            else:
                                fd_stats['other_sockets'] += 1
                        else:
                            fd_stats['other_sockets'] += 1
                            
                    elif 'pipe:' in link:
                        fd_stats['pipes'] += 1
                    elif '/frontier' in link:
                        fd_stats['frontier_files'] += 1
                    elif '/prometheus' in link:
                        fd_stats['prometheus'] += 1
                    else:
                        fd_stats['other'] += 1
                        
                except:
                    pass  # FD might have closed
            
            # Update Prometheus metrics
            fd_redis_gauge.set(fd_stats['redis'])
            fd_http_gauge.set(fd_stats['http'])
            fd_https_gauge.set(fd_stats['https'])
            fd_frontier_files_gauge.set(fd_stats['frontier_files'])
            fd_prometheus_gauge.set(fd_stats['prometheus'])
            fd_other_sockets_gauge.set(fd_stats['other_sockets'])
            fd_pipes_gauge.set(fd_stats['pipes'])
            fd_other_gauge.set(fd_stats['other'])
            
            logger.info(f"[Metrics] FD breakdown: {fd_stats}")
            
            # Also log connection pool stats
            if hasattr(self, 'redis_client') and hasattr(self.redis_client.connection_pool, '_created_connections'):
                logger.info(f"[Metrics] Redis pool connections: {self.redis_client.connection_pool._created_connections}")
            
            if hasattr(self, 'fetcher') and hasattr(self.fetcher, 'session') and self.fetcher.session:
                connector = self.fetcher.session.connector
                if hasattr(connector, '_acquired'):
                    logger.info(f"[Metrics] aiohttp acquired connections: {len(connector._acquired)}")
                if hasattr(connector, '_conns'):
                    total_conns = sum(len(conns) for conns in connector._conns.values())
                    logger.info(f"[Metrics] aiohttp total cached connections: {total_conns}")
                    
        except Exception as e:
            logger.error(f"Error in FD analysis: {e}")

    async def _log_metrics(self):
        current_time = time.time()
        time_elapsed_seconds = current_time - self.last_metrics_log_time
        if time_elapsed_seconds == 0: time_elapsed_seconds = 1 # Avoid division by zero

        pages_per_second = self.pages_crawled_in_interval / time_elapsed_seconds
        logger.info(f"[Metrics] Pages Crawled/sec: {pages_per_second:.2f}")
        
        # Update Prometheus gauge
        pages_per_second_gauge.set(pages_per_second)
```

```python
                if time.time() - self.last_metrics_log_time >= METRICS_LOG_INTERVAL_SECONDS:
                    await self._log_metrics()
                    
                    # Log lightweight memory tracker summary

->

                if time.time() - self.last_metrics_log_time >= METRICS_LOG_INTERVAL_SECONDS:
                    await self._log_metrics()
                    
                    # Analyze FD types
                    await self._analyze_fd_types()
                    
                    # Log lightweight memory tracker summary
```

Perfect! I've successfully integrated the FD type tracking into your crawler. Here's what I've done:

## Changes Made:

1. **Added new Prometheus metrics** in `metrics.py`:
   - `crawler_fd_redis_connections` - Redis connection FDs
   - `crawler_fd_http_connections` - HTTP connection FDs  
   - `crawler_fd_https_connections` - HTTPS connection FDs
   - `crawler_fd_frontier_files` - Frontier file FDs
   - `crawler_fd_prometheus_files` - Prometheus metric file FDs
   - `crawler_fd_other_sockets` - Other socket FDs
   - `crawler_fd_pipes` - Pipe FDs
   - `crawler_fd_other` - Other FDs

2. **Added `_analyze_fd_types()` method** to the orchestrator that:
   - Parses `/proc/net/tcp` and `/proc/net/tcp6` to map socket inodes to connection details
   - Identifies socket types by port numbers (6379=Redis, 80=HTTP, 443=HTTPS)
   - Categorizes all FDs by type
   - Updates the Prometheus metrics
   - Logs the FD breakdown
   - Also logs Redis and aiohttp connection pool statistics

3. **Integrated into periodic metrics logging**:
   - The analysis runs every 60 seconds along with other metrics
   - Results are both logged and exposed as Prometheus metrics

## What to Expect:

When the crawler runs, you'll now see periodic log entries like:
```
[Metrics] FD breakdown: {'redis': 12, 'http': 45, 'https': 234, 'frontier_files': 3, 'prometheus': 14, 'other_sockets': 5, 'pipes': 4, 'other': 2}
[Metrics] Redis pool connections: 10
[Metrics] aiohttp acquired connections: 45
[Metrics] aiohttp total cached connections: 234
```

These metrics will help identify which component is leaking FDs:
- If `redis` grows unbounded → Redis connection leak
- If `http/https` grows unbounded → aiohttp connection leak  
- If `frontier_files` grows unbounded → Frontier file handle leak
- If `other_sockets` grows → Unidentified socket leak (check ports)

You can now monitor these metrics over time to see which type of FD is increasing and correlate with crawler behavior (errors, restarts, etc.) to identify the root cause of the leak.

```plaintext
[Metrics] FD breakdown: {'redis': 12, 'http': 45, 'https': 234, 'frontier_files': 3, 'prometheus': 14, 'other_sockets': 5, 'pipes': 4, 'other': 2}
[Metrics] Redis pool connections: 10
[Metrics] aiohttp acquired connections: 45
[Metrics] aiohttp total cached connections: 234
```

