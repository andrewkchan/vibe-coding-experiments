# Optimizing web crawler domain handling
_Exported on 6/9/2025 at 00:15:38 PDT from Cursor (0.50.5)_

---

**User**

We are continuing optimizing our web crawler, you can see a recent chat log and architecture overview (PLAN.MD) attached.

The newest issue is that (possibly because of batching adds of URLs to the frontier) our crawler is hitting bursts of the same domain while chewing through the frontier and throughput is slowing to a crawl as nearly all URLs get rejected due to politeness and have to be re-inserted into the queue. See the attached image correlating a run's throughput to the volume of `unclaim_url_set_null` counts.

I want to brainstorm some ideas to fix this with you. Please be candid in the feedback and offer better ideas if you have them.

The first idea I have might be stupid, it is to have each call to get_next_url start at some random offset in the frontier. However, I'm not sure how we'd decide the random offset or if it would actually improve things.

The next idea is to change the index on the frontier to be something like (domain, added_at). Then we can have each worker work through domains in a round-robin fashion. To avoid uncoordinated herding on the domains, we can try sharding the domains among the workers; if domain was an integer, this would be easy because we could just use the domain modulo number of workers, but it's a text field so not sure what's the best way to do this (having the worker figure out the "next round" of the round robin seems like it would require either a query or denormalizing the domains into an in-memory table, both of which seem unclean).

Thoughts? Any better ideas? Let's discuss

---

**Cursor**

Looking at your metrics and the correlation with `unclaim_url_set_null`, you've identified the exact problem: when URLs from the same domain cluster together in the frontier (which happens naturally when batching), workers claim them but can only process one due to the 70-second politeness delay, leading to massive unclaim churn.

Your analysis is spot-on. Let me give you candid feedback on your ideas and propose some alternatives:

## Your Ideas:

1. **Random offset approach**: This is actually not stupid at all - it's a form of randomization that could help. However, it's a band-aid that doesn't address the root cause. You'd still have clustering, just at different points for different workers.

2. **Domain-based round-robin with sharding**: This is a much stronger approach. For the text domain issue, you could use a hash function (like `hashtext()` in PostgreSQL or MD5) to convert domains to integers for modulo operations. However, this adds complexity and might create hotspots if domain distribution is uneven.

## Better Ideas:

### 1. **Domain-Aware Claiming** (My top recommendation)
Modify `_atomic_claim_urls` to only claim URLs from domains that are actually ready to be fetched. This eliminates the unclaim problem entirely:

```sql
UPDATE frontier
SET claimed_at = %s
WHERE id IN (
    SELECT DISTINCT ON (domain) id
    FROM frontier f
    LEFT JOIN domain_metadata dm ON f.domain = dm.domain
    WHERE f.claimed_at IS NULL
    AND (
        dm.last_scheduled_fetch_timestamp IS NULL 
        OR dm.last_scheduled_fetch_timestamp < %s - 70000
    )
    ORDER BY domain, added_timestamp ASC
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
RETURNING id, url, domain, depth;
```

This claims one URL per domain, but only from domains that are ready. No more unclaiming!

### 2. **Frontier Stratification by Domain**
Instead of ordering the frontier purely by `added_timestamp`, create a composite ordering that naturally spreads domains:

```sql
CREATE INDEX idx_frontier_domain_time ON frontier (
    (hashtext(domain) % 100), -- Creates 100 buckets
    added_timestamp ASC
) WHERE claimed_at IS NULL;
```

Workers would claim from different buckets, naturally spreading domain access.

### 3. **Domain Ready Time Materialization**
Add a `ready_at` column to the frontier table that pre-calculates when each URL can be fetched based on its domain's last access. Update this when recording fetch attempts. Then claiming becomes trivial:

```sql
WHERE claimed_at IS NULL AND ready_at <= %s
ORDER BY ready_at ASC
```

### 4. **Worker-Domain Affinity**
Assign each worker a "shard" of the domain space:
- Worker 0 handles domains where `hashtext(domain) % num_workers = 0`
- Worker 1 handles domains where `hashtext(domain) % num_workers = 1`
- etc.

This eliminates competition between workers for the same domains.

## My Recommendation:

Start with **Domain-Aware Claiming** (#1). It's the most elegant solution that directly addresses the problem without major architectural changes. The query might be slightly more complex, but it eliminates the wasteful claim/unclaim cycle entirely.

If that's not sufficient, layer on **Domain Ready Time Materialization** (#3) for even better performance.

What do you think? Would you like me to help implement the domain-aware claiming approach?

```sql
UPDATE frontier
SET claimed_at = %s
WHERE id IN (
    SELECT DISTINCT ON (domain) id
    FROM frontier f
    LEFT JOIN domain_metadata dm ON f.domain = dm.domain
    WHERE f.claimed_at IS NULL
    AND (
        dm.last_scheduled_fetch_timestamp IS NULL 
        OR dm.last_scheduled_fetch_timestamp < %s - 70000
    )
    ORDER BY domain, added_timestamp ASC
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
RETURNING id, url, domain, depth;
```

```sql
CREATE INDEX idx_frontier_domain_time ON frontier (
    (hashtext(domain) % 100), -- Creates 100 buckets
    added_timestamp ASC
) WHERE claimed_at IS NULL;
```

```sql
WHERE claimed_at IS NULL AND ready_at <= %s
ORDER BY ready_at ASC
```

---

**User**

How do (1) and (3) work with very high concurrency (500 workers)?

---

**Cursor**

Excellent question! With 500 concurrent workers, both approaches face significant challenges. Let me break down the issues:

## Solution #1 (Domain-Aware Claiming) at 500 Workers:

**Problems:**
- The `DISTINCT ON (domain)` with a JOIN is expensive and gets worse under contention
- 500 workers fighting over the same query with `FOR UPDATE SKIP LOCKED` creates a thundering herd
- To keep 500 workers busy with 70-second delays, you need ~35,000 active domains (500 × 70)
- The complex query would likely become a bottleneck

## Solution #3 (Domain Ready Time) at 500 Workers:

**Problems:**
- Every fetch requires updating potentially thousands of frontier entries for that domain
- With 500 workers constantly updating `ready_at` times, you'd have massive write contention
- Index maintenance on `ready_at` becomes expensive with constant updates

## Better Approach for 500 Workers: **Hybrid with Batched Claims**

Here's what I'd actually recommend for this scale:

```python
# Modified frontier claiming - claims a diverse batch of domains
async def _atomic_claim_urls_diverse(self, batch_size: int = 50):
    """Claims URLs from diverse domains to minimize politeness conflicts"""
    
    if self.storage.config.db_type == "postgresql":
        # Claim URLs from different domain buckets to ensure diversity
        sql_query = '''
        WITH domain_buckets AS (
            SELECT 
                id, url, domain, depth,
                ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_timestamp) as domain_rank,
                hashtext(domain) % 100 as bucket
            FROM frontier
            WHERE claimed_at IS NULL
        ),
        ready_domains AS (
            SELECT DISTINCT f.domain
            FROM domain_buckets f
            LEFT JOIN domain_metadata dm ON f.domain = dm.domain
            WHERE domain_rank = 1  -- Only first URL per domain
            AND (dm.last_scheduled_fetch_timestamp IS NULL 
                 OR dm.last_scheduled_fetch_timestamp < %s - 70000)
            ORDER BY f.bucket, f.domain  -- Spread across buckets
            LIMIT %s
        )
        UPDATE frontier
        SET claimed_at = %s
        WHERE id IN (
            SELECT MIN(f.id)
            FROM frontier f
            INNER JOIN ready_domains rd ON f.domain = rd.domain
            WHERE f.claimed_at IS NULL
            GROUP BY f.domain
        )
        RETURNING id, url, domain, depth;
        '''
```

## Even Better: **Pre-computed Work Queues**

For 500 workers, consider a different architecture:

1. **Background process** continuously populates work queues:
```sql
CREATE TABLE work_queues (
    worker_id INTEGER,
    url_id INTEGER REFERENCES frontier(id),
    domain TEXT,
    assigned_at TIMESTAMP DEFAULT NOW()
);
```

2. **Workers claim from their dedicated queues**:
```sql
-- Worker N only queries:
SELECT * FROM work_queues 
WHERE worker_id = %s 
ORDER BY assigned_at 
LIMIT 10;
```

3. **Queue populator** ensures domain diversity per worker using round-robin or hashing

This separates the complex "what to crawl" logic from the hot path of workers claiming URLs.

## My Recommendation for 500 Workers:

1. **Short term**: Implement batched diverse claiming (get 10-50 URLs from different domains per claim)
2. **Medium term**: Add domain sharding so workers don't compete
3. **Long term**: Consider a queue-based architecture that pre-assigns work to avoid runtime coordination

The key insight is that at 500 workers, you need to minimize coordination overhead. Pre-computing work assignments is much more scalable than having all workers compete in real-time.

Would you like me to elaborate on any of these approaches?

```python
# Modified frontier claiming - claims a diverse batch of domains
async def _atomic_claim_urls_diverse(self, batch_size: int = 50):
    """Claims URLs from diverse domains to minimize politeness conflicts"""
    
    if self.storage.config.db_type == "postgresql":
        # Claim URLs from different domain buckets to ensure diversity
        sql_query = '''
        WITH domain_buckets AS (
            SELECT 
                id, url, domain, depth,
                ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_timestamp) as domain_rank,
                hashtext(domain) % 100 as bucket
            FROM frontier
            WHERE claimed_at IS NULL
        ),
        ready_domains AS (
            SELECT DISTINCT f.domain
            FROM domain_buckets f
            LEFT JOIN domain_metadata dm ON f.domain = dm.domain
            WHERE domain_rank = 1  -- Only first URL per domain
            AND (dm.last_scheduled_fetch_timestamp IS NULL 
                 OR dm.last_scheduled_fetch_timestamp < %s - 70000)
            ORDER BY f.bucket, f.domain  -- Spread across buckets
            LIMIT %s
        )
        UPDATE frontier
        SET claimed_at = %s
        WHERE id IN (
            SELECT MIN(f.id)
            FROM frontier f
            INNER JOIN ready_domains rd ON f.domain = rd.domain
            WHERE f.claimed_at IS NULL
            GROUP BY f.domain
        )
        RETURNING id, url, domain, depth;
        '''
```

```sql
CREATE TABLE work_queues (
    worker_id INTEGER,
    url_id INTEGER REFERENCES frontier(id),
    domain TEXT,
    assigned_at TIMESTAMP DEFAULT NOW()
);
```

```sql
-- Worker N only queries:
SELECT * FROM work_queues 
WHERE worker_id = %s 
ORDER BY assigned_at 
LIMIT 10;
```

---

**User**

Does the batched diverse claim query ensure multiple workers cannot claim the same URL? I see you didn't put a lock anywhere. If it needs to be locked, does it offer any benefits over the first recommendation (domain-aware claiming)?

---

**Cursor**

You're absolutely right - I made a critical error. Without `FOR UPDATE SKIP LOCKED`, multiple workers would claim the same URLs, leading to duplicate processing. That's a serious bug.

Let me fix it and then reassess:

```sql
WITH domain_buckets AS (
    SELECT 
        id, url, domain, depth,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_timestamp) as domain_rank
    FROM frontier
    WHERE claimed_at IS NULL
    FOR UPDATE SKIP LOCKED  -- This lock needs to be here
),
-- ... rest of query
```

But now we hit the problem: once we add the lock, this query becomes **worse** than the original domain-aware claiming because:

1. It's more complex (more CPU overhead)
2. It still has all 500 workers competing for the same rows
3. The CTE with window functions + locking is particularly expensive

So you've exposed that my "better" solution offers no real benefits and is actually worse.

## The Real Problem at 500 Workers

With PostgreSQL's row-level locking, having 500 workers all trying to claim from the same frontier table creates fundamental contention, regardless of how clever our queries are. Each approach hits the same wall:

- **Domain-aware claiming**: Complex query + lock contention
- **Batched diverse claiming**: Even more complex query + same lock contention  
- **Ready time materialization**: Massive write amplification

## The Actual Solution

At 500 workers, we need to **eliminate the contention point entirely**. The pre-computed work queues approach I mentioned is really the only scalable solution:

```python
# Background process assigns work without locking
async def assign_work_to_queues():
    # Get unclaimed URLs from diverse domains
    urls = await db.fetch_all("""
        SELECT DISTINCT ON (domain) id, domain 
        FROM frontier 
        WHERE claimed_at IS NULL 
        AND domain NOT IN (
            SELECT domain FROM domain_metadata 
            WHERE last_scheduled_fetch_timestamp > %s - 70000
        )
        LIMIT 5000
    """)
    
    # Distribute round-robin to workers
    for i, (url_id, domain) in enumerate(urls):
        worker_id = i % 500
        await db.execute(
            "INSERT INTO work_queues (worker_id, url_id) VALUES (%s, %s)",
            (worker_id, url_id)
        )
```

Workers then just claim from their assigned queue with no contention.

You're right to push back on my initial suggestions - they don't actually solve the problem at this scale.

```sql
WITH domain_buckets AS (
    SELECT 
        id, url, domain, depth,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY added_timestamp) as domain_rank
    FROM frontier
    WHERE claimed_at IS NULL
    FOR UPDATE SKIP LOCKED  -- This lock needs to be here
),
-- ... rest of query
```

```python
# Background process assigns work without locking
async def assign_work_to_queues():
    # Get unclaimed URLs from diverse domains
    urls = await db.fetch_all("""
        SELECT DISTINCT ON (domain) id, domain 
        FROM frontier 
        WHERE claimed_at IS NULL 
        AND domain NOT IN (
            SELECT domain FROM domain_metadata 
            WHERE last_scheduled_fetch_timestamp > %s - 70000
        )
        LIMIT 5000
    """)
    
    # Distribute round-robin to workers
    for i, (url_id, domain) in enumerate(urls):
        worker_id = i % 500
        await db.execute(
            "INSERT INTO work_queues (worker_id, url_id) VALUES (%s, %s)",
            (worker_id, url_id)
        )
```

---

**User**

Hmm, to keep things simple for now can we try the first solution (domain-aware claiming)? I want to run the crawler with high concurrency and see how it performs

---

**Cursor**

Absolutely! Let's implement the domain-aware claiming solution. This will prevent the wasteful claim/unclaim cycle by only claiming URLs from domains that are actually ready to be fetched.

Now I'll implement the domain-aware claiming solution by modifying the `_atomic_claim_urls` method:

```python
    async def _atomic_claim_urls(self, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str, int]]:
        """Atomically claims a batch of URLs from the frontier, only from domains that are ready."""
        try:
            claim_timestamp = int(time.time() * 1000) # Current time in milliseconds
            current_time_seconds = int(time.time())

            if self.storage.config.db_type == "postgresql":
                # Calculate the threshold for considering a claim expired
                expired_claim_timestamp_threshold = claim_timestamp - (claim_expiry_seconds * 1000)
                # Calculate when a domain is ready (70 seconds have passed since last fetch)
                domain_ready_threshold = current_time_seconds - 70

                # Domain-aware claiming: only claim URLs from domains that are ready
                sql_query = '''
                UPDATE frontier
                SET claimed_at = %s
                WHERE id = (
                    SELECT f.id
                    FROM frontier f
                    LEFT JOIN domain_metadata dm ON f.domain = dm.domain
                    WHERE f.claimed_at IS NULL
                    AND (
                        dm.last_scheduled_fetch_timestamp IS NULL 
                        OR dm.last_scheduled_fetch_timestamp <= %s
                    )
                    ORDER BY f.added_timestamp ASC
                    LIMIT 1
                    FOR UPDATE OF f SKIP LOCKED
                )
                RETURNING id, url, domain, depth;
                '''
                
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (claim_timestamp, domain_ready_threshold),
                    query_name="atomic_claim_urls_pg_domain_aware"
                )
                
                # If no unclaimed URLs found, try expired ones (also domain-aware)
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        LEFT JOIN domain_metadata dm ON f.domain = dm.domain
                        WHERE f.claimed_at IS NOT NULL 
                        AND f.claimed_at < %s
                        AND (
                            dm.last_scheduled_fetch_timestamp IS NULL 
                            OR dm.last_scheduled_fetch_timestamp <= %s
                        )
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp, expired_claim_timestamp_threshold, domain_ready_threshold),
                        query_name="atomic_claim_urls_pg_expired_domain_aware"
                    )
            else:
                # SQLite doesn't support FOR UPDATE SKIP LOCKED or complex CTEs as well.
                # Keep the old explicit release and simpler claim for SQLite.
                # This path will be less performant for high-volume crawling.
                expired_timestamp = claim_timestamp - (claim_expiry_seconds * 1000)
                await self.storage.db.execute("""
                    UPDATE frontier 
                    SET claimed_at = NULL 
                    WHERE claimed_at IS NOT NULL AND claimed_at < ?
                """, (expired_timestamp,), query_name="release_expired_claims")

                # Original SQLite claiming logic
                claimed_urls = await self.storage.db.execute_returning(
                    """
                    WITH to_claim_ids AS (
                        SELECT id
                        FROM frontier
                        WHERE claimed_at IS NULL
                        ORDER BY added_timestamp ASC
                        LIMIT ?
                    )
                    UPDATE frontier
                    SET claimed_at = ?
                    WHERE id IN (SELECT id FROM to_claim_ids)
                    RETURNING id, url, domain, depth
                    """, (batch_size, claim_timestamp), # Corrected params for SQLite query
                    query_name="atomic_claim_urls_sqlite"
                )
            
            return claimed_urls
            
        except Exception as e:
            logger.error(f"DB error in atomic claim: {e}")
            raise
```

Now let's add an index to optimize the new domain-aware claiming query:

Let me add an index to optimize the domain-aware claiming query:

```python
                if self.config.db_type == "postgresql":
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_unclaimed_order_by_time ON frontier (added_timestamp ASC) WHERE claimed_at IS NULL", query_name="create_idx_frontier_unclaimed")
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed_at_for_expiry ON frontier (claimed_at) WHERE claimed_at IS NOT NULL", query_name="create_idx_frontier_claimed_at_expiry")
                    # New index for the implicit expiry logic (expired part of UNION ALL)
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_expired_order_by_time ON frontier (added_timestamp ASC) WHERE claimed_at IS NOT NULL", query_name="create_idx_frontier_expired")
                    # Index for domain-aware claiming to optimize JOIN with domain_metadata
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain_claimed_at ON frontier (domain, claimed_at, added_timestamp)", query_name="create_idx_frontier_domain_claimed")
                else: # SQLite
```

Let's update the schema version to ensure existing databases get the new index:

```python
logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 5

class StorageManager:
```

Perfect! I've implemented the domain-aware claiming solution. Here's what I changed:

1. **Modified `_atomic_claim_urls` method**: Now it only claims URLs from domains that are ready to be fetched (respecting the 70-second delay). This is done by joining with `domain_metadata` and checking that either:
   - The domain has never been fetched (`last_scheduled_fetch_timestamp IS NULL`)
   - Or 70+ seconds have passed since the last fetch

2. **Added an optimization index**: Created `idx_frontier_domain_claimed_at` on `frontier (domain, claimed_at, added_timestamp)` to make the JOIN with `domain_metadata` more efficient.

3. **Updated schema version**: Changed from 4 to 5 to ensure existing databases get the new index when upgraded.

The key benefits:
- Workers will only claim URLs they can actually process, eliminating most unclaim operations
- The query remains atomic with `FOR UPDATE SKIP LOCKED` to prevent race conditions
- The solution is simple and doesn't require major architectural changes

This should dramatically reduce the `unclaim_url_set_null` operations and improve throughput when the frontier contains many URLs from the same domain.

You can now run the crawler with high concurrency and see how it performs!

