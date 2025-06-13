# Rethinking web crawler architecture
_Exported on 6/13/2025 at 22:04:24 PDT from Cursor (1.0.0)_

---

**User**

I am rethinking the architecture of the web crawler we've been building (see PLAN.MD and README.md) because we are still far from the target throughput but are hitting a wall of complexity. Originally (see crawler/full-chats/2025-05-25_cursor_collaborative_web_crawler_projec.md) we chose SQLite for its simplicity. However it did not support concurrent accesses to the frontier well and so we moved to PostgreSQL, which after many optimizations has gotten us to ~20 pages/sec. This is short of our target of 7.5 million pages in 24 hours which would require ~86 pages/sec. 

I think we don't need the flexibility of a relational database and are ultimately doing fairly limited set of operations on our data which could be more performantly represented using different structures. One possibility to retain the fault tolerance / persistence of the current system while using more specialized data structures could be to use Redis. 
- Instead of a frontier table, we would have each domain map to a key in Redis, with the value being a Redis list representing the frontier URLs for that domain from which we can push and pop. 
- Instead of a domain_metadata table, we would again have each domain map to a key (a different one than the frontier key), with the value providing the robots.txt information. We would not store the last_scheduled_fetch_timestamp in this value, instead we could have a single key with the ordered list of domains (so that ready domains are at the head and non-ready domains / more recently fetched at the tail) as the value.
- Instead of a visited_urls table, we would have each visited URL map to a key in Redis. Or alternatively, have a single key with a redis set, whichever is more performant.

I think the challenges with implementing this system mainly have to do with it being a large code change.
- All queries in frontier.py, politeness.py, storage.py, etc. must be updated.
- All the tests currently use SQLite and need to be updated.
- Data using the current schema must be migrated to redis.
- Documentation must be updated.
However, I also have not worked with redis before and could be wrong about the tradeoffs of the proposed design vs. what we currently have.

Let's discuss this. Provide candid feedback. Does this sound like a good idea to you? Any alternatives?

---

**Cursor**

Looking at your performance gap (20 pages/sec vs target 86 pages/sec), I think your Redis proposal has merit, but let me give you candid feedback on both the benefits and challenges.

## My Take on the Redis Migration Proposal

Your analysis is thoughtful, and I think you're right to question whether a relational database is the best fit for this use case. Let me give you my candid assessment.

### Where Redis Would Excel

Your proposed Redis architecture maps well to the crawler's access patterns:

1. **Frontier operations** - Redis lists with O(1) push/pop are perfect for per-domain URL queues
2. **Domain scheduling** - A Redis sorted set (ZSET) with timestamp scores would elegantly handle the "which domains are ready" problem
3. **Visited URL checks** - Redis sets offer O(1) membership testing vs. indexed lookups in PostgreSQL

The performance gains could be substantial - Redis can handle 100K+ ops/sec on a single instance.

### Major Concerns I See

**1. Memory Constraints**
- 7.5M URLs × ~100 bytes/URL = ~750MB just for URLs
- Add metadata, and you're looking at several GB in RAM
- Redis's memory overhead can be 2-3x the raw data size
- You'd need careful memory management and possibly Redis Cluster

**2. Persistence Trade-offs**
- RDB snapshots: Risk losing minutes of work
- AOF: Significant write amplification, slower restarts
- Neither matches PostgreSQL's durability guarantees

**3. Atomic Operations Complexity**
- Your current "claim-and-read" pattern in PostgreSQL is elegantly atomic
- In Redis, you'd need Lua scripts or transactions to maintain consistency
- Domain sharding across workers becomes trickier

### Alternative Approaches to Consider

Before committing to Redis, I'd explore these options:

**1. PostgreSQL Optimization**
```sql
-- Partition frontier by domain hash for better parallelism
CREATE TABLE frontier_0 PARTITION OF frontier 
FOR VALUES FROM (0) TO (100);
-- ... create 10 partitions

-- Use SKIP LOCKED more aggressively
-- Consider prepared statements
-- Tune shared_buffers, work_mem
```

**2. Hybrid Approach**
Keep PostgreSQL for durability but add Redis as a cache:
- Hot frontier domains in Redis
- Write-through to PostgreSQL
- Best of both worlds

**3. Alternative Databases**
- **RocksDB**: LSM-tree storage, great for high write throughput
- **ScyllaDB**: Cassandra-compatible, handles millions of ops/sec
- **FoundationDB**: ACID + high performance

**4. Application-Level Optimization**
Looking at your code, I see opportunities:
- Batch more operations (you're already doing some)
- Use prepared statements
- Connection pooling tuning
- Consider partitioning URLs by domain at the application level

### My Recommendation

Given your situation, I'd suggest this pragmatic approach:

1. **First, try PostgreSQL partitioning** and more aggressive batching. This could get you to 50+ pages/sec with minimal code changes.

2. **If that's insufficient, implement a hybrid model**:
   ```python
   class HybridFrontier:
       def __init__(self, redis_client, pg_storage):
           self.redis = redis_client
           self.pg = pg_storage
           self.hot_domains = {}  # LRU cache of active domains
   
       async def get_next_url(self):
           # Check Redis first for hot domains
           # Fall back to PostgreSQL
           # Write-through updates
   ```

3. **Only go full Redis if you need 100+ pages/sec** and can accept the operational complexity.

### If You Do Choose Redis

Here's what I'd recommend:
- Use Redis Cluster for sharding
- Implement proper Lua scripts for atomic operations
- Set up Redis Sentinel for HA
- Use AOF with fsync every second
- Monitor memory usage closely
- Keep PostgreSQL as a backup/archive

The code changes would indeed be substantial, but if you modularize well (which you already have with your storage abstraction), it's manageable.

What's your current bottleneck based on the metrics? Is it DB query time, connection acquisition, or something else? That might help us identify whether Redis is truly necessary or if there are simpler optimizations to try first.

```sql
-- Partition frontier by domain hash for better parallelism
CREATE TABLE frontier_0 PARTITION OF frontier 
FOR VALUES FROM (0) TO (100);
-- ... create 10 partitions

-- Use SKIP LOCKED more aggressively
-- Consider prepared statements
-- Tune shared_buffers, work_mem
```

```python
   class HybridFrontier:
       def __init__(self, redis_client, pg_storage):
           self.redis = redis_client
           self.pg = pg_storage
           self.hot_domains = {}  # LRU cache of active domains
   
       async def get_next_url(self):
           # Check Redis first for hot domains
           # Fall back to PostgreSQL
           # Write-through updates
```

---

**User**

Looking at an example segment from the most recent run, it looks like DB connection acquisition is the bottleneck:
```
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Pages Crawled/sec: 20.70
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] DB Conn Acquire Time (ms): P50=1881.83, P95=4125.20 (from 11467 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'count_frontier_pg_estimated' (ms): P50=32.85, P95=115.00, MAX=117.16 (from 7 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'atomic_claim_urls_pg_unclaimed' (ms): P50=70.57, P95=616.87, MAX=19644.27 (from 1736 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'check_manual_exclusion_seeded' (ms): P50=66.48, P95=649.66, MAX=19461.36 (from 566 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'get_cached_robots' (ms): P50=80.38, P95=658.70, MAX=2573.90 (from 79 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'record_fetch_attempt_pg' (ms): P50=63.24, P95=575.74, MAX=19641.93 (from 1676 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'delete_frontier_url' (ms): P50=61.73, P95=587.54, MAX=19642.74 (from 1658 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'add_visited_page_pg' (ms): P50=66.82, P95=546.19, MAX=19640.75 (from 1683 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'batch_check_visited' (ms): P50=53.90, P95=561.75, MAX=19642.53 (from 1061 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'batch_insert_frontier_pg' (ms): P50=1125.20, P95=21064.14, MAX=64862.01 (from 1073 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'update_robots_cache_upsert_pg' (ms): P50=67.72, P95=433.42, MAX=19640.47 (from 78 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'is_frontier_empty' (ms): P50=99.91, P95=375.40, MAX=768.70 (from 64 samples)
2025-06-12 04:35:32 - INFO - crawler_module.orchestrator [2281816] - [Metrics] Query Hold Time 'unclaim_url_set_null' (ms): P50=89.69, P95=545.96, MAX=845.37 (from 57 samples)
```

The critical path are the queries which have 1000+ samples (starting from atomic_claim_urls_pg_unclaimed with successful fetches also doing batch_check_visited and batch_insert_frontier_pg). Each query also requires acquiring a connection. 

I think a reasonable sanity check is to divide 500 workers by the <sum of all the P50 query times and an equal number of P50 DB conn acquire times>, then multiply by the successful fetch rate (approximately 10/16). This yields ~21 pages/sec which is close to the reported throughput for this segment.

Thoughts?

---

**Cursor**

Excellent analysis! Your math checks out perfectly. The smoking gun is that **P50 connection acquisition time of 1.9 seconds** - that's catastrophic and explains everything.

## The Real Problem

You don't have a database performance problem, you have a **connection pool starvation problem**. Your workers are spending 90%+ of their time waiting for a database connection, not actually doing work.

```python
concurrent_db_fraction = 0.7  # Assume ~70% of workers need DB at once
min_pool = min(10, config.max_workers)
estimated_concurrent = int(config.max_workers * concurrent_db_fraction)
safe_max = 80  # Assuming max_connections=100
max_pool = min(max(estimated_concurrent, 20), safe_max)
```

Found it! The problem is crystal clear. Looking at the orchestrator code, with 500 workers, the connection pool is configured as:

```python
concurrent_db_fraction = 0.7  # Assume ~70% of workers need DB at once
estimated_concurrent = int(500 * 0.7) = 350
safe_max = 80  # Assuming max_connections=100
max_pool = min(350, 80) = 80
```

So you have **500 workers competing for only 80 connections**. That's a 6:1 ratio, which perfectly explains the 2-second connection acquisition times.

## The Solution (No Redis Needed!)

You have several options, in order of ease:

### 1. Increase PostgreSQL Connection Pool (Immediate Fix)

The code already supports an environment variable override:

```bash
export CRAWLER_PG_POOL_SIZE=600
python main.py --seed-file seeds.txt --email your@email.com --max-workers 500
```

But first, you need to increase PostgreSQL's `max_connections`:

```sql
-- Check current setting
SHOW max_connections;

-- Increase it (requires restart)
ALTER SYSTEM SET max_connections = 800;
```

Then restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

### 2. Reduce Worker Count (Alternative)

If you can't increase PostgreSQL connections, reduce workers to match available connections:

```bash
python main.py --seed-file seeds.txt --email your@email.com --max-workers 70
```

With 70 workers and 80 connections, you'd have a healthy buffer.

### 3. Connection Multiplexing with PgBouncer (Best Long-term)

Install PgBouncer as a connection pooler:

```bash
sudo apt install pgbouncer
```

Configure it for transaction pooling mode, which can handle 1000+ client connections with only 100 PostgreSQL connections.

## Expected Performance Impact

With proper connection pooling (option 1 or 3), your connection acquisition time should drop from 1900ms to <5ms. This alone should get you from 20 pages/sec to approximately:

- Current: 16.6 seconds per fetch (mostly waiting for connections)
- Fixed: ~1.6 seconds per fetch (actual query time)
- Throughput: 500 workers / 1.6 sec = **~300 fetches/sec**
- With 62.5% success rate: **~190 pages/sec**

That's well above your 86 pages/sec target!

## Why Not Redis?

While Redis could work, you'd be solving the wrong problem. Your database queries are fast (50-100ms). The issue is connection starvation, not database performance. Fix the connection pool first - it's a configuration change vs. a complete rewrite.

If after fixing connections you still need more performance, then consider:
1. PostgreSQL partitioning (as you mentioned)
2. More aggressive batching
3. Prepared statements
4. Redis as a cache layer (not a replacement)

But I'd bet fixing the connection pool will get you to your target with minimal code changes.

```python
concurrent_db_fraction = 0.7  # Assume ~70% of workers need DB at once
estimated_concurrent = int(500 * 0.7) = 350
safe_max = 80  # Assuming max_connections=100
max_pool = min(350, 80) = 80
```

```shellscript
export CRAWLER_PG_POOL_SIZE=600
python main.py --seed-file seeds.txt --email your@email.com --max-workers 500
```

```sql
-- Check current setting
SHOW max_connections;

-- Increase it (requires restart)
ALTER SYSTEM SET max_connections = 800;
```

```shellscript
sudo systemctl restart postgresql
```

```shellscript
python main.py --seed-file seeds.txt --email your@email.com --max-workers 70
```

```shellscript
sudo apt install pgbouncer
```

---

**User**

D'oh! Great suggestion. I had looked up increasing the PG connection pool size above 100 before but for some reason got discouraged by a post online. Looking again the post says it's a bad idea *if I don't do it carefully* and that *typical* apps don't require a large number of connections. I guess my web crawler is not really a typical app as it's a batch process and we're trying to push the performance of a single machine. The post also says I should increase shared_buffers and kernel.shmmax along with the max_connections.

---

**Cursor**

Exactly! You're absolutely right - a high-performance web crawler is NOT a typical application. Those warnings are for web apps serving user requests where each connection uses memory even when idle. Your crawler is a batch processing system where you WANT to maximize throughput and all connections are actively working.

## PostgreSQL Tuning for High Connection Batch Processing

Here's a comprehensive tuning guide for your use case:

### 1. Calculate Memory Requirements

First, let's calculate what you need (assuming 64GB RAM server):

```bash
# Check your current RAM
free -h

# Check current PostgreSQL settings
sudo -u postgres psql -c "SHOW shared_buffers;"
sudo -u postgres psql -c "SHOW max_connections;"
sudo -u postgres psql -c "SHOW work_mem;"
```

### 2. PostgreSQL Configuration

Create a tuning configuration file:

```shellscript
# Check your current RAM
free -h

# Check current PostgreSQL settings
sudo -u postgres psql -c "SHOW shared_buffers;"
sudo -u postgres psql -c "SHOW max_connections;"
sudo -u postgres psql -c "SHOW work_mem;"
```

```properties
# PostgreSQL Tuning for Web Crawler Batch Processing
# Place this in /etc/postgresql/14/main/conf.d/ or include in postgresql.conf

# Connection Settings
max_connections = 800                    # Support 500-600 active crawler connections
superuser_reserved_connections = 10      # Keep some for admin access

# Memory Settings (adjust based on your RAM - these assume 64GB)
shared_buffers = 16GB                    # 25% of RAM
effective_cache_size = 48GB              # 75% of RAM
maintenance_work_mem = 2GB               # For VACUUM, index creation
work_mem = 16MB                          # Per sort/hash operation (careful with high connections!)
                                        # 16MB × 800 connections × 2 ops = 25GB worst case

# Checkpoint Settings (optimize for write-heavy workload)
checkpoint_timeout = 15min               # Longer checkpoints for batch processing
checkpoint_completion_target = 0.9       # Spread checkpoint I/O
max_wal_size = 8GB
min_wal_size = 2GB

# Query Planner
random_page_cost = 1.1                   # For SSD storage
effective_io_concurrency = 200           # For SSD storage

# Logging (helpful for debugging)
log_min_duration_statement = 1000        # Log queries taking > 1 second
log_connections = on
log_disconnections = on
log_lock_waits = on
log_statement = 'ddl'                    # Log schema changes

# Connection Pooling Behavior
idle_in_transaction_session_timeout = '5min'  # Kill idle transactions

# Parallel Query (PostgreSQL 14+)
max_parallel_workers_per_gather = 4
max_parallel_workers = 16
max_parallel_maintenance_workers = 4

# Autovacuum Tuning (important for high-write workload)
autovacuum_max_workers = 8
autovacuum_naptime = 10s                # Check more frequently
autovacuum_vacuum_cost_limit = 2000     # More aggressive vacuuming

# Statement Timeout (prevent runaway queries)
statement_timeout = '5min'               # Adjust as needed
```

### 3. Kernel Parameters

```shellscript
#!/bin/bash
# System tuning for PostgreSQL web crawler
# Run as root

set -e

echo "=== PostgreSQL Web Crawler System Tuning ==="

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Get system memory in KB
TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
TOTAL_MEM_GB=$((TOTAL_MEM_KB / 1024 / 1024))

echo "Detected system memory: ${TOTAL_MEM_GB}GB"

# Calculate shared memory requirements
# PostgreSQL needs: shared_buffers + (work_mem * max_connections * 2) + OS overhead
# For our settings: 16GB + (16MB * 800 * 2) + 10GB overhead ≈ 50GB
REQUIRED_SHMMAX=$((50 * 1024 * 1024 * 1024))  # 50GB in bytes
REQUIRED_SHMALL=$((REQUIRED_SHMMAX / 4096))   # in 4KB pages

echo "Setting kernel parameters for high-connection PostgreSQL..."

# Apply runtime settings
sysctl -w kernel.shmmax=$REQUIRED_SHMMAX
sysctl -w kernel.shmall=$REQUIRED_SHMALL
sysctl -w kernel.shmmni=4096
sysctl -w kernel.sem="1000 128000 128 1024"

# Network tuning for many connections
sysctl -w net.core.somaxconn=1024
sysctl -w net.ipv4.tcp_max_syn_backlog=4096
sysctl -w net.ipv4.ip_local_port_range="10000 65000"

# File descriptor limits
sysctl -w fs.file-max=1000000

echo "Runtime settings applied."

# Make settings persistent
cat > /etc/sysctl.d/30-postgresql-crawler.conf << EOF
# PostgreSQL Web Crawler Tuning
# Generated on $(date)

# Shared memory settings
kernel.shmmax = $REQUIRED_SHMMAX
kernel.shmall = $REQUIRED_SHMALL
kernel.shmmni = 4096
kernel.sem = 1000 128000 128 1024

# Network settings for many connections
net.core.somaxconn = 1024
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.ip_local_port_range = 10000 65000

# File descriptors
fs.file-max = 1000000
EOF

echo "Persistent settings saved to /etc/sysctl.d/30-postgresql-crawler.conf"

# Set PostgreSQL service limits
mkdir -p /etc/systemd/system/postgresql.service.d/
cat > /etc/systemd/system/postgresql.service.d/limits.conf << EOF
[Service]
# Increase limits for PostgreSQL
LimitNOFILE=100000
LimitNPROC=10000
EOF

echo "PostgreSQL service limits configured."

# Set limits for the postgres user
cat >> /etc/security/limits.conf << EOF

# PostgreSQL limits for web crawler
postgres soft nofile 100000
postgres hard nofile 100000
postgres soft nproc 10000
postgres hard nproc 10000
EOF

echo "User limits configured."

# Reload systemd
systemctl daemon-reload

echo ""
echo "=== Configuration Complete ==="
echo "Next steps:"
echo "1. Review and apply PostgreSQL configuration from postgresql_crawler_tuning.conf"
echo "2. Restart PostgreSQL: systemctl restart postgresql"
echo "3. Verify settings:"
echo "   - sysctl kernel.shmmax kernel.shmall"
echo "   - sudo -u postgres psql -c 'SHOW max_connections;'"
echo "   - sudo -u postgres psql -c 'SHOW shared_buffers;'"
```

### 4. Monitoring Script

```shellscript
#!/bin/bash
# Monitor PostgreSQL connections and performance for the crawler

DB_NAME="web_crawler"  # Adjust to your database name
INTERVAL=5  # Refresh every 5 seconds

while true; do
    clear
    echo "=== PostgreSQL Crawler Monitor - $(date) ==="
    echo ""
    
    # Connection statistics
    echo "CONNECTION STATS:"
    sudo -u postgres psql -d $DB_NAME -t << EOF
SELECT 
    'Active' as status, count(*) 
FROM pg_stat_activity 
WHERE state = 'active' AND pid != pg_backend_pid()
UNION ALL
SELECT 
    'Idle', count(*) 
FROM pg_stat_activity 
WHERE state = 'idle' AND pid != pg_backend_pid()
UNION ALL
SELECT 
    'Idle in Transaction', count(*) 
FROM pg_stat_activity 
WHERE state = 'idle in transaction' AND pid != pg_backend_pid()
UNION ALL
SELECT 
    'Waiting', count(*) 
FROM pg_stat_activity 
WHERE wait_event IS NOT NULL AND pid != pg_backend_pid()
UNION ALL
SELECT 
    'Total', count(*) 
FROM pg_stat_activity 
WHERE pid != pg_backend_pid();
EOF
    
    echo ""
    echo "LONGEST RUNNING QUERIES:"
    sudo -u postgres psql -d $DB_NAME -x -t << EOF
SELECT 
    pid,
    usename,
    state,
    substring(query, 1, 80) as query_preview,
    round(extract(epoch from (now() - query_start))) as duration_seconds
FROM pg_stat_activity
WHERE state = 'active' 
    AND pid != pg_backend_pid()
    AND query NOT ILIKE '%pg_stat_activity%'
ORDER BY query_start
LIMIT 5;
EOF
    
    echo ""
    echo "DATABASE SIZE:"
    sudo -u postgres psql -d $DB_NAME -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));"
    
    echo ""
    echo "SHARED BUFFER HIT RATIO:"
    sudo -u postgres psql -d $DB_NAME -t << EOF
SELECT 
    round(100.0 * sum(blks_hit) / (sum(blks_hit) + sum(blks_read)), 2) as hit_ratio
FROM pg_stat_database 
WHERE datname = '$DB_NAME';
EOF
    
    echo ""
    echo "Press Ctrl+C to exit..."
    
    sleep $INTERVAL
done
```

### 5. Step-by-Step Implementation

Here's how to safely apply these changes:

#### Step 1: Backup Current Configuration
```bash
sudo cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup
sudo -u postgres pg_dumpall > crawler_backup_$(date +%Y%m%d).sql
```

#### Step 2: Apply System Tuning
```bash
cd crawler
sudo ./tune_system_for_crawler.sh
```

#### Step 3: Apply PostgreSQL Configuration
```bash
# Copy the tuning config
sudo cp postgresql_crawler_tuning.conf /etc/postgresql/14/main/conf.d/

# Or append to main config
sudo cat postgresql_crawler_tuning.conf >> /etc/postgresql/14/main/postgresql.conf
```

#### Step 4: Restart PostgreSQL
```bash
sudo systemctl restart postgresql

# Verify it started correctly
sudo systemctl status postgresql
```

#### Step 5: Verify Settings
```bash
# Check connection limit
sudo -u postgres psql -c "SHOW max_connections;"

# Check memory settings
sudo -u postgres psql -c "SHOW shared_buffers;"
sudo -u postgres psql -c "SHOW work_mem;"

# Check kernel parameters
sysctl kernel.shmmax kernel.shmall
```

#### Step 6: Run Crawler with Optimal Settings
```bash
# Set the pool size to match available connections
export CRAWLER_PG_POOL_SIZE=600

# Run with monitoring in another terminal
./monitor_crawler_db.sh &

# Start crawler
python main.py --seed-file seeds.txt --email your@email.com --max-workers 500
```

### 6. Safety Considerations

1. **Memory Budget**: The configuration assumes 64GB RAM. If you have less:
   - Reduce `shared_buffers` to 25% of your RAM
   - Reduce `work_mem` proportionally
   - Consider fewer connections

2. **Monitor During First Run**:
   ```bash
   # Watch memory usage
   watch -n 2 free -h
   
   # Watch PostgreSQL connections
   ./monitor_crawler_db.sh
   ```

3. **Rollback Plan**: If issues occur:
   ```bash
   # Restore original config
   sudo mv /etc/postgresql/14/main/postgresql.conf.backup /etc/postgresql/14/main/postgresql.conf
   sudo systemctl restart postgresql
   ```

### 7. Expected Results

With these settings, you should see:
- Connection acquisition time drop from 1900ms to <10ms
- Throughput increase from 20 pages/sec to 150-200+ pages/sec
- Stable memory usage around 50-60% of total RAM
- No "FATAL: too many connections" errors

### Key Insight

You're absolutely right that web crawlers aren't typical apps. They're designed to:
- Maximize throughput, not minimize latency
- Use all available resources efficiently
- Handle massive concurrent operations

These PostgreSQL settings reflect that reality. The warnings about high connection counts are for web apps where each connection might serve one user. In your case, each connection is actively working to crawl the web - exactly what you want!

```shellscript
sudo cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup
sudo -u postgres pg_dumpall > crawler_backup_$(date +%Y%m%d).sql
```

```shellscript
sudo cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup
sudo -u postgres pg_dumpall > crawler_backup_$(date +%Y%m%d).sql
```

```shellscript
cd crawler
sudo ./tune_system_for_crawler.sh
```

```shellscript
# Copy the tuning config
sudo cp postgresql_crawler_tuning.conf /etc/postgresql/14/main/conf.d/

# Or append to main config
sudo cat postgresql_crawler_tuning.conf >> /etc/postgresql/14/main/postgresql.conf
```

```shellscript
sudo systemctl restart postgresql

# Verify it started correctly
sudo systemctl status postgresql
```

```shellscript
# Check connection limit
sudo -u postgres psql -c "SHOW max_connections;"

# Check memory settings
sudo -u postgres psql -c "SHOW shared_buffers;"
sudo -u postgres psql -c "SHOW work_mem;"

# Check kernel parameters
sysctl kernel.shmmax kernel.shmall
```

```shellscript
# Set the pool size to match available connections
export CRAWLER_PG_POOL_SIZE=600

# Run with monitoring in another terminal
./monitor_crawler_db.sh &

# Start crawler
python main.py --seed-file seeds.txt --email your@email.com --max-workers 500
```

```shellscript
   # Watch memory usage
   watch -n 2 free -h
   
   # Watch PostgreSQL connections
   ./monitor_crawler_db.sh
```

```shellscript
   # Restore original config
   sudo mv /etc/postgresql/14/main/postgresql.conf.backup /etc/postgresql/14/main/postgresql.conf
   sudo systemctl restart postgresql
```

