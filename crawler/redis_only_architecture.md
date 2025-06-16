# Hybrid Redis + File Storage Crawler Architecture

## Executive Summary for Implementation

### The Problem
The web crawler is currently achieving only 20 pages/sec, far short of the 86 pages/sec needed to reach 7.5M pages in 24 hours. After extensive investigation, we discovered:

1. **Initial Issue**: Connection pool starvation (1.9 second wait times) with only 80 PostgreSQL connections for 500 workers
2. **Deeper Issue**: After increasing to 600 connections, the bottleneck moved to PostgreSQL internals:
   - `batch_insert_frontier` queries taking 8+ seconds (was 1 second)
   - `atomic_claim_urls` queries taking 700ms (was 70ms)
   - Massive write contention on the frontier table with 500 concurrent workers

### Root Cause Analysis
PostgreSQL is being used as a high-concurrency queue, which it's not designed for:
- The `frontier` table is essentially a work queue
- 500 workers competing to INSERT/UPDATE/DELETE the same table
- Index updates, lock contention, and WAL writes create exponential slowdowns
- PostgreSQL is optimized for ~50-100 concurrent connections, not 500+

### The Journey
1. **Started with SQLite** → Hit connection limits at ~50 workers
2. **Moved to PostgreSQL** → Hit internal contention at ~500 workers  
3. **Considered pure Redis** → Would need 32GB RAM just for frontier URLs
4. **Arrived at hybrid solution** → Redis coordination + file storage

### The Solution: Hybrid Architecture
- **Redis** (4.5GB RAM): Handles coordination, scheduling, and URL tracking
  - Bloom filter for "seen" URLs (fast deduplication of 160M+ URLs)
  - Exact storage for "visited" URLs (7.5M URLs we actually fetched)
- **Files** (32GB disk): One append-only file per domain for URL storage
- **Result**: Microsecond scheduling latency with unlimited frontier size

### Key Insights from Discussion
1. **Connection pool fix revealed true bottleneck**: Removing the 1.9s connection wait time didn't improve throughput, just moved the queue from the pool to PostgreSQL internals
2. **Per-domain partitioning is natural**: Instead of arbitrary partitions, one file per domain provides perfect isolation
3. **PostgreSQL is the wrong tool**: Using a relational database as a queue at this scale fundamentally doesn't work
4. **Memory constraints matter**: With 160M URLs in frontier, pure in-memory solutions aren't feasible on a 32GB machine

### Expected Impact
- Current: 20 pages/sec (limited by database contention)
- Expected: 200-300 pages/sec (limited only by network/parsing)
- 10-15x performance improvement with simpler code

## Overview
Use Redis for coordination and metadata, with file-based storage for the actual frontier URLs. This gives us Redis performance for the hot path while keeping memory usage reasonable.

## Architecture Design

### Core Insight
- **Redis**: Stores metadata, offsets, and coordination data (small, fast)
- **Files**: Store actual URL lists per domain (large, sequential access)
- **Result**: Microsecond latency for scheduling with unlimited frontier size

## Data Structures

### 1. Frontier Files (One file per domain)
```
# File structure: crawler_data/frontiers/{domain_hash}/{domain}.frontier
# Format: One URL entry per line
url|depth|priority|added_timestamp
https://example.com/page1|0|1.0|1699564800
https://example.com/page2|1|0.9|1699564900

# Append new URLs to end
# Read from current offset position
```

### 2. Redis Structures

#### Domain Metadata (Hash per domain)
```redis
domain:{domain} → HASH {
    frontier_offset: 1234,        # Current read position in frontier file
    frontier_size: 5678,          # Total URLs in frontier file
    file_path: "a5/example.com.frontier",  # Relative path to frontier file
    next_fetch_time: 1699564800,  # When domain can be fetched next
    robots_txt: "...",            # Cached robots.txt
    robots_expires: 1699568400,   # When to refetch robots.txt
    is_excluded: 0,               # Manual exclusion flag
}
```

#### Domain Queue (Redis list)
```redis
domains:queue → LIST {
    "example.com",
    "site.org",
    ...
}
```

#### Seen URLs (Bloom Filter for Deduplication)
```redis
# Use RedisBloom for fast "have we seen this URL before?" checks
seen:bloom → BLOOM FILTER (0.1% false positive rate)

# For 160M URLs at 0.1% FPR: ~2GB memory
BF.RESERVE seen:bloom 0.001 160000000
```

#### Visited URLs (Exact Set)
```redis
# Exact record of URLs we've actually fetched/attempted
# Stored as hash for metadata
visited:{url_hash} → HASH {
    url: "https://example.com/page",
    status_code: 200,
    fetched_at: 1699564800,
    content_path: "content/a5/b7c9d2e4f6.txt",
    error: null
}

# Also maintain a sorted set for time-based queries
visited:by_time → ZSET {
    "url_hash1": timestamp1,
    "url_hash2": timestamp2
}
```

## Memory Usage (Revised)

For 160M URLs across 1M domains, 7.5M visited:
- Domain metadata: 1M domains × 500 bytes = 500MB
- Domain ready queue: 1M domains × 50 bytes = 50MB
- Seen bloom filter: 2GB (0.1% false positive rate)
- Visited URL hashes: 7.5M × 200 bytes = 1.5GB
- Visited time index: 7.5M × 50 bytes = 375MB
- Active domains set: ~10K domains × 50 bytes = 500KB
- **Total Redis memory: ~4.5GB** (vs 32GB for full frontier)

File storage:
- Frontier files: 160M URLs × 200 bytes = 32GB on disk
- Organized in subdirectories by domain hash prefix

## Implementation

### Frontier Manager

See `HybridFrontierManager` in `frontier.py`.

### Migration from PostgreSQL

```python
class PostgreSQLToHybridMigrator:
    def __init__(self, pg_conn, redis_client, frontier_manager):
        self.pg = pg_conn
        self.redis = redis_client
        self.fm = frontier_manager
        
    async def migrate(self):
        """Migrate data from PostgreSQL to hybrid storage."""
        print("Starting migration...")
        
        # 1. Create bloom filter for seen URLs
        print("Creating bloom filter for seen URLs...")
        # Estimate: visited + frontier + some growth room
        visited_count = await self.pg.fetchval("SELECT COUNT(*) FROM visited_urls")
        frontier_count = await self.pg.fetchval("SELECT COUNT(*) FROM frontier")
        total_seen = visited_count + frontier_count
        await self.redis.execute_command(
            'BF.RESERVE', 'seen:bloom', 0.001, int(total_seen * 1.5)
        )
        
        # 2. Migrate visited URLs (exact records)
        print(f"Migrating {visited_count} visited URLs...")
        batch_size = 10000
        offset = 0
        
        while offset < visited_count:
            rows = await self.pg.fetch("""
                SELECT url, url_sha256, crawled_timestamp, http_status_code,
                       content_storage_path, redirected_to_url
                FROM visited_urls
                ORDER BY crawled_timestamp
                LIMIT $1 OFFSET $2
            """, batch_size, offset)
            
            pipe = self.redis.pipeline()
            for row in rows:
                url_hash = row['url_sha256'][:16]  # Use first 16 chars
                
                # Store visited metadata
                pipe.hset(f'visited:{url_hash}', mapping={
                    'url': row['url'],
                    'status_code': row['http_status_code'] or 0,
                    'fetched_at': row['crawled_timestamp'],
                    'content_path': row['content_storage_path'] or '',
                    'error': ''
                })
                
                # Add to time index
                pipe.zadd('visited:by_time', {url_hash: row['crawled_timestamp']})
                
                # Mark as seen
                pipe.execute_command('BF.ADD', 'seen:bloom', row['url'])
            
            await pipe.execute()
            offset += batch_size
            print(f"Migrated {min(offset, visited_count)}/{visited_count} visited URLs")
        
        # 3. Mark all frontier URLs as seen
        print("Marking frontier URLs as seen...")
        frontier_batch_size = 50000
        frontier_offset = 0
        
        while frontier_offset < frontier_count:
            rows = await self.pg.fetch("""
                SELECT url FROM frontier
                LIMIT $1 OFFSET $2
            """, frontier_batch_size, frontier_offset)
            
            pipe = self.redis.pipeline()
            for row in rows:
                pipe.execute_command('BF.ADD', 'seen:bloom', row['url'])
            await pipe.execute()
            
            frontier_offset += frontier_batch_size
            print(f"Marked {min(frontier_offset, frontier_count)}/{frontier_count} frontier URLs as seen")
        
        # 4. Migrate frontier URLs to files
        print("Migrating frontier URLs to files...")
        frontier_count = await self.pg.fetchval("SELECT COUNT(*) FROM frontier")
        
        batch_size = 10000
        offset = 0
        
        while offset < frontier_count:
            rows = await self.pg.fetch("""
                SELECT url, domain, depth 
                FROM frontier 
                ORDER BY domain, added_timestamp
                LIMIT $1 OFFSET $2
            """, batch_size, offset)
            
            urls = [(row['url'], row['domain'], row['depth']) for row in rows]
            await self.fm.add_urls_batch(urls, batch_id=f"migration_{offset}")
            
            offset += batch_size
            print(f"Migrated {min(offset, frontier_count)}/{frontier_count} frontier URLs")
        
        # 5. Migrate domain metadata
        print("Migrating domain metadata...")
        domains = await self.pg.fetch("""
            SELECT domain, robots_txt_content, robots_txt_expires_timestamp,
                   is_manually_excluded, last_scheduled_fetch_timestamp
            FROM domain_metadata
        """)
        
        pipe = self.redis.pipeline()
        for row in domains:
            domain_key = f"domain:{row['domain']}"
            pipe.hset(domain_key, mapping={
                'robots_txt': row['robots_txt_content'] or '',
                'robots_expires': row['robots_txt_expires_timestamp'] or 0,
                'is_excluded': row['is_manually_excluded'] or 0,
                'next_fetch_time': row['last_scheduled_fetch_timestamp'] or 0
            })
        await pipe.execute()
        
        print("Migration complete!")
```

## Task List for Migration

### Phase 1: Setup and Testing (Day 1)
- [X] Install Redis with RedisBloom module
- [X] Configure Redis persistence (AOF + RDB)

### Phase 2: Code Implementation (Days 2-3)
- [X] Implement HybridFrontierManager class
- [X] Write unit tests for HybridFrontierManager
- [X] Implement file-based frontier storage
- [X] Add bloom filter integration
- [X] Update PolitenessEnforcer for Redis
- [X] Update PolitenessEnforcer unit tests
- [X] Update StorageManager (add_visited_page) for Redis
- [X] Update StorageManager unit tests
- [ ] Update CrawlerOrchestrator to use new frontier and politeness
- [ ] Implement PostgreSQL migration script
- [ ] Add monitoring/metrics for new system

### Phase 3: Migration Preparation (Day 4)
- [ ] Backup PostgreSQL database
- [ ] Run migration script
- [ ] Verify data integrity
- [ ] Start crawler with new system
- [ ] Monitor performance

### Phase 5: Optimization (Day 5+)
- [ ] Tune Redis configuration
- [ ] Optimize file I/O (buffering, async)
- [ ] Add frontier compaction/rotation (remove consumed URLs)
- [ ] Add S3 backup for frontier files

## Performance Expectations

With hybrid architecture:
- **URL scheduling**: ~1ms (Redis sorted set lookup)
- **URL retrieval**: ~5ms (file seek + read)
- **URL insertion**: ~1ms (file append)
- **Visited check**: ~0.1ms (bloom filter)

Expected throughput: **200-300 pages/sec** with 500 workers

## Advantages Over Pure Redis
1. **Unlimited frontier size** - Only limited by disk space
2. **3GB RAM instead of 32GB** - Fits comfortably in memory
3. **Same performance** - File I/O is fast for sequential access
4. **Simple backup** - Just copy frontier directory

## Advantages Over PostgreSQL
1. **No write contention** - Each domain writes to its own file
2. **No index overhead** - Append-only files
3. **Natural partitioning** - One file per domain
4. **10x faster operations** - No SQL parsing or locking 