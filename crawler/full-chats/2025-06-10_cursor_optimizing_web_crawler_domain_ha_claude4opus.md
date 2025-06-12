# Optimizing web crawler domain handling
_Exported on 6/10/2025 at 01:05:43 PDT from Cursor (0.50.5)_

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

---

**User**

So IIUC this solution still leads to domain collisions because workers don't lock the domain rows when they read them, therefore 2 workers could each claim URLs from the same domain in the window before the fetch attempt timestamp on the domain is updated. 

I think this means politeness congestion can still happen, and it seemed to appear halfway through last night's run (see attached image - correlating pages/sec plot with unclaim_url_set_null plot).

Do you think adapting the claim queries to lock the domain_metadata rows is possible / a good idea? Let's discuss before writing any code

---

**Cursor**

You're absolutely correct! I see the flaw now. The current solution has a race condition:

1. Worker A and Worker B both check domain X in their claim queries and see it's ready (last_fetch was 70+ seconds ago)
2. Both workers claim URLs from domain X
3. Worker A gets to `record_domain_fetch_attempt` first and updates the timestamp
4. Worker B then hits `can_fetch_domain_now`, sees the domain is no longer ready, and has to unclaim

This explains why the unclaim spike appears later in your run - once you have enough domain diversity in the frontier, this race condition becomes more frequent.

Regarding locking domain_metadata rows, there are a few approaches we could consider:

## Option 1: Lock domain_metadata in the claim query
```sql
LEFT JOIN domain_metadata dm ON f.domain = dm.domain
FOR UPDATE OF f, dm SKIP LOCKED
```
**Pros:** Prevents race conditions
**Cons:** Could create deadlocks if not careful, and might skip viable URLs because their domain row is temporarily locked

## Option 2: Atomically update domain timestamp during claim
Instead of checking and updating separately, we could update `last_scheduled_fetch_timestamp` directly in the claim query:
```sql
UPDATE frontier f
SET claimed_at = %s,
    -- Also update domain timestamp atomically
    dummy = (UPDATE domain_metadata SET last_scheduled_fetch_timestamp = %s WHERE domain = f.domain)
WHERE...
```
**Problem:** PostgreSQL doesn't allow this syntax

## Option 3: Two-phase claim with domain reservation
1. First query: atomically update domain_metadata timestamp for one ready domain
2. Second query: claim a URL from that specific domain

**Pros:** Truly atomic domain reservation
**Cons:** More complex, requires two queries

## Option 4: Claim by domain (my preference)
Change the claiming logic to claim one URL per domain atomically:
```sql
WITH ready_domain AS (
    UPDATE domain_metadata
    SET last_scheduled_fetch_timestamp = %s
    WHERE domain = (
        SELECT DISTINCT f.domain
        FROM frontier f
        WHERE f.claimed_at IS NULL
        AND EXISTS (
            SELECT 1 FROM domain_metadata dm 
            WHERE dm.domain = f.domain 
            AND (dm.last_scheduled_fetch_timestamp IS NULL OR dm.last_scheduled_fetch_timestamp <= %s)
        )
        ORDER BY random()  -- Or some other strategy
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING domain
)
UPDATE frontier
SET claimed_at = %s
WHERE id = (
    SELECT id FROM frontier 
    WHERE domain = (SELECT domain FROM ready_domain)
    AND claimed_at IS NULL
    ORDER BY added_timestamp ASC
    LIMIT 1
)
RETURNING id, url, domain, depth;
```

This atomically "claims" a domain by updating its timestamp, then claims a URL from that domain.

What do you think? The fourth option seems cleanest to me as it truly prevents domain collisions while keeping the logic in a single atomic operation.

```sql
LEFT JOIN domain_metadata dm ON f.domain = dm.domain
FOR UPDATE OF f, dm SKIP LOCKED
```

```sql
UPDATE frontier f
SET claimed_at = %s,
    -- Also update domain timestamp atomically
    dummy = (UPDATE domain_metadata SET last_scheduled_fetch_timestamp = %s WHERE domain = f.domain)
WHERE...
```

```sql
WITH ready_domain AS (
    UPDATE domain_metadata
    SET last_scheduled_fetch_timestamp = %s
    WHERE domain = (
        SELECT DISTINCT f.domain
        FROM frontier f
        WHERE f.claimed_at IS NULL
        AND EXISTS (
            SELECT 1 FROM domain_metadata dm 
            WHERE dm.domain = f.domain 
            AND (dm.last_scheduled_fetch_timestamp IS NULL OR dm.last_scheduled_fetch_timestamp <= %s)
        )
        ORDER BY random()  -- Or some other strategy
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING domain
)
UPDATE frontier
SET claimed_at = %s
WHERE id = (
    SELECT id FROM frontier 
    WHERE domain = (SELECT domain FROM ready_domain)
    AND claimed_at IS NULL
    ORDER BY added_timestamp ASC
    LIMIT 1
)
RETURNING id, url, domain, depth;
```

---

**User**

Can you explain more what is keeping the race from still happening if we have the following concurrent queries from worker A and B execute like so:
1. worker A selects `example.com/a` for update based on `example.com` being ready
2. worker B selects `example.com/b` for update based on `example.com` being ready
3. worker A updates `example.com` domain_metadata last_scheduled_fetch_timestamp
4. worker B updates `example.com` domain_metadata last_scheduled_fetch_timestamp
5. both return their respective URLs

Am I missing some postgres feature that prevents this type of execution of the concurrent queries?

---

**Cursor**

```sql
WITH ready_domain AS (
    UPDATE domain_metadata
    SET last_scheduled_fetch_timestamp = %s
    WHERE domain = (
        SELECT DISTINCT f.domain
        FROM frontier f
        WHERE f.claimed_at IS NULL
        AND EXISTS (...)
        ORDER BY random()
        LIMIT 1
        FOR UPDATE SKIP LOCKED  -- This locks the domain_metadata row
    )
    RETURNING domain
)
UPDATE frontier...
```

You're absolutely right to question this - there's still a race condition in my proposed solution! 

Your execution sequence is exactly what would happen. Both workers can see `example.com` is ready in their SELECT subqueries before either has updated the timestamp. The `FOR UPDATE SKIP LOCKED` in my query is actually flawed - it's trying to lock frontier rows, not the domain_metadata row we actually need to lock.

The fundamental problem is that PostgreSQL doesn't let us atomically:
1. Find a domain that's ready
2. Lock it
3. Check it's still ready
4. Update its timestamp

Without locking ALL domain_metadata rows upfront (which would be terrible for performance).

Here are the actual options that would work:

## Option A: Pessimistic Domain Locking (Simple but potentially slow)
```sql
BEGIN;
-- Lock the domain first
SELECT * FROM domain_metadata 
WHERE domain = 'example.com' 
FOR UPDATE;

-- Check if ready and claim if so
UPDATE frontier SET claimed_at = %s
WHERE domain = 'example.com' 
AND claimed_at IS NULL
AND EXISTS (
    SELECT 1 FROM domain_metadata 
    WHERE domain = 'example.com' 
    AND (last_scheduled_fetch_timestamp IS NULL OR last_scheduled_fetch_timestamp <= %s)
)
LIMIT 1
RETURNING ...;

-- Update domain timestamp if we got a URL
UPDATE domain_metadata SET last_scheduled_fetch_timestamp = %s WHERE domain = 'example.com';
COMMIT;
```

But this requires knowing which domain to try, leading to coordination problems.

## Option B: PostgreSQL Advisory Locks
```python
# Use domain name hash as advisory lock ID
domain_lock_id = hash(domain) % 2147483647
await db.execute("SELECT pg_try_advisory_lock(%s)", (domain_lock_id,))
# ... do claim ...
await db.execute("SELECT pg_advisory_unlock(%s)", (domain_lock_id,))
```

## Option C: Accept Some Collisions (Pragmatic)
Keep the current domain-aware approach but accept that some collisions will happen. The reduction from constant collisions to occasional ones might be good enough.

## Option D: Worker-Domain Sharding (My actual recommendation)
Assign domains to workers using consistent hashing:
```python
def get_worker_for_domain(domain: str, num_workers: int) -> int:
    return hash(domain) % num_workers

# In claim query, worker N only claims from domains where hash(domain) % num_workers = N
```

This eliminates domain collisions entirely by ensuring only one worker can ever claim from a given domain.

Given the complexity of true atomic domain locking in PostgreSQL, I think Option D (sharding) is the most practical solution for 500 workers. What do you think?

```sql
BEGIN;
-- Lock the domain first
SELECT * FROM domain_metadata 
WHERE domain = 'example.com' 
FOR UPDATE;

-- Check if ready and claim if so
UPDATE frontier SET claimed_at = %s
WHERE domain = 'example.com' 
AND claimed_at IS NULL
AND EXISTS (
    SELECT 1 FROM domain_metadata 
    WHERE domain = 'example.com' 
    AND (last_scheduled_fetch_timestamp IS NULL OR last_scheduled_fetch_timestamp <= %s)
)
LIMIT 1
RETURNING ...;

-- Update domain timestamp if we got a URL
UPDATE domain_metadata SET last_scheduled_fetch_timestamp = %s WHERE domain = 'example.com';
COMMIT;
```

```python
# Use domain name hash as advisory lock ID
domain_lock_id = hash(domain) % 2147483647
await db.execute("SELECT pg_try_advisory_lock(%s)", (domain_lock_id,))
# ... do claim ...
await db.execute("SELECT pg_advisory_unlock(%s)", (domain_lock_id,))
```

```python
def get_worker_for_domain(domain: str, num_workers: int) -> int:
    return hash(domain) % num_workers

# In claim query, worker N only claims from domains where hash(domain) % num_workers = N
```

---

**User**

sorry how come we can't do the "option 1: lock domian_metadata in the claim query" - how come we can only "lock ALL domain_metadata rows upfront"?

---

**Cursor**

```sql
LEFT JOIN domain_metadata dm ON f.domain = dm.domain
FOR UPDATE OF f, dm SKIP LOCKED
```

You're absolutely right! I apologize for the confusion. We CAN do Option 1, and it's actually elegant.

Let me trace through why it works:

```sql
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
    FOR UPDATE OF f, dm SKIP LOCKED
)
RETURNING id, url, domain, depth;
```

Here's what happens:

1. Worker A starts the query and encounters `example.com/a`
2. It locks BOTH the frontier row for `example.com/a` AND the domain_metadata row for `example.com`
3. Worker B starts the same query and encounters `example.com/b`
4. Worker B tries to lock the domain_metadata row for `example.com` but it's already locked by Worker A
5. Due to `SKIP LOCKED`, Worker B skips ALL frontier entries from `example.com` and moves on to try other domains
6. Worker A completes its claim and releases the locks

The beauty is that `SKIP LOCKED` means "if you can't get the lock, just skip that row and try the next one." So workers automatically avoid domains that are currently being processed by other workers.

I was wrong when I said this would require locking "ALL domain_metadata rows upfront" - the locks are acquired row-by-row as the query executes, and `SKIP LOCKED` ensures we only lock what we can actually use.

This is much simpler than my other suggestions and should completely eliminate domain collisions. Should we implement this approach?

```sql
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
    FOR UPDATE OF f, dm SKIP LOCKED
)
RETURNING id, url, domain, depth;
```

---

**User**

ok yeah can we try that? i think I can just do a test run to see if the perf is acceptable after you make the changes

---

**Cursor**

Absolutely! Let me implement the domain locking approach by modifying the `_atomic_claim_urls` method:

```python
    async def _atomic_claim_urls(self, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str, int]]:
        """Atomically claims a batch of URLs from the frontier."""
        try:
            claim_timestamp = int(time.time() * 1000) # Current time in milliseconds
            current_time_seconds = int(time.time())

            if self.storage.config.db_type == "postgresql":
                # Calculate the threshold for considering a claim expired
                expired_claim_timestamp_threshold = claim_timestamp - (claim_expiry_seconds * 1000)
                # Calculate when a domain is ready (70 seconds have passed since last fetch)
                domain_ready_threshold = current_time_seconds - 70

                # Domain-aware claiming with domain locking to prevent collisions
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
                    FOR UPDATE OF f, dm SKIP LOCKED
                )
                RETURNING id, url, domain, depth;
                '''
                
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (claim_timestamp, domain_ready_threshold),
                    query_name="atomic_claim_urls_pg_domain_aware"
                )
                
                # If no unclaimed URLs found, try expired ones (also with domain locking)
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
                        FOR UPDATE OF f, dm SKIP LOCKED
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

Perfect! I've implemented the domain locking approach. The key changes are:

1. Added `dm` to the `FOR UPDATE OF` clause: `FOR UPDATE OF f, dm SKIP LOCKED`
2. This applies to both the regular claim query and the expired URLs claim query

Now when a worker claims a URL from a domain, it will lock both:
- The frontier row (f) 
- The domain_metadata row (dm)

This means:
- When Worker A is processing `example.com`, it holds a lock on the `example.com` domain_metadata row
- Any other workers that encounter `example.com` URLs will skip them due to `SKIP LOCKED`
- This completely prevents domain collisions - only one worker can process a domain at a time

The beauty of this approach is that it's automatic - workers naturally distribute across different domains without any explicit coordination.

You can now run your test to see if this eliminates the unclaim spikes while maintaining acceptable performance!

---

**User**

hmm
```
2025-06-10 07:37:11 - ERROR - crawler_module.frontier [2103539] - DB error in atomic claim: FOR UPDATE cannot be applied to the nullable side of an outer join
2025-06-10 07:37:11 - ERROR - crawler_module.frontier [2103539] - Error in get_next_url: FOR UPDATE cannot be applied to the nullable side of an outer join
Traceback (most recent call last):
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 259, in get_next_url
    claimed_urls = await self._atomic_claim_urls(batch_size, claim_expiry_seconds)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 342, in _atomic_claim_urls
    claimed_urls = await self.storage.db.execute_returning(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/db_backends.py", line 440, in execute_returning
    await cur.execute(query, params or ())
  File "/home/ubuntu/vibe-coding-experiments/crawler/.venv/lib/python3.12/site-packages/psycopg/cursor_async.py", line 97, in execute
    raise ex.with_traceback(None)
psycopg.errors.FeatureNotSupported: FOR UPDATE cannot be applied to the nullable side of an outer join

```

---

**Cursor**

Ah, that's a PostgreSQL limitation - you can't lock rows on the nullable side of a LEFT JOIN (because there might not be a row to lock!).

We need to restructure the query. Here are a couple of options:

```python
                # Domain-aware claiming with domain locking to prevent collisions
                sql_query = '''
                UPDATE frontier
                SET claimed_at = %s
                WHERE id = (
                    SELECT id FROM (
                        -- Case 1: URLs from domains that have never been fetched (no metadata row)
                        SELECT f.id, f.added_timestamp
                        FROM frontier f
                        WHERE f.claimed_at IS NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM domain_metadata dm 
                            WHERE dm.domain = f.domain
                        )
                        
                        UNION ALL
                        
                        -- Case 2: URLs from domains with metadata that are ready
                        SELECT f.id, f.added_timestamp
                        FROM frontier f
                        INNER JOIN domain_metadata dm ON f.domain = dm.domain
                        WHERE f.claimed_at IS NULL
                        AND dm.last_scheduled_fetch_timestamp <= %s
                        FOR UPDATE OF f, dm SKIP LOCKED
                    ) AS eligible_urls
                    ORDER BY added_timestamp ASC
                    LIMIT 1
                )
                RETURNING id, url, domain, depth;
                '''
                
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (claim_timestamp, domain_ready_threshold),
                    query_name="atomic_claim_urls_pg_domain_aware"
                )
```

Now I need to fix the expired URLs query as well:

```python
                # If no unclaimed URLs found, try expired ones (also with domain locking)
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT id FROM (
                            -- Case 1: Expired URLs from domains that have never been fetched
                            SELECT f.id, f.added_timestamp
                            FROM frontier f
                            WHERE f.claimed_at IS NOT NULL 
                            AND f.claimed_at < %s
                            AND NOT EXISTS (
                                SELECT 1 FROM domain_metadata dm 
                                WHERE dm.domain = f.domain
                            )
                            
                            UNION ALL
                            
                            -- Case 2: Expired URLs from domains with metadata that are ready
                            SELECT f.id, f.added_timestamp
                            FROM frontier f
                            INNER JOIN domain_metadata dm ON f.domain = dm.domain
                            WHERE f.claimed_at IS NOT NULL 
                            AND f.claimed_at < %s
                            AND dm.last_scheduled_fetch_timestamp <= %s
                            FOR UPDATE OF f, dm SKIP LOCKED
                        ) AS eligible_urls
                        ORDER BY added_timestamp ASC
                        LIMIT 1
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp, expired_claim_timestamp_threshold, expired_claim_timestamp_threshold, domain_ready_threshold),
                        query_name="atomic_claim_urls_pg_expired_domain_aware"
                    )
```

Perfect! I've fixed the PostgreSQL error by restructuring the queries. The key changes:

1. **Split the query into two cases with UNION ALL:**
   - Case 1: URLs from domains that have never been fetched (no `domain_metadata` row exists)
   - Case 2: URLs from domains that have a `domain_metadata` row and are ready

2. **Only lock existing rows:**
   - For Case 1: No locking needed since there's no `domain_metadata` row
   - For Case 2: Use `FOR UPDATE OF f, dm SKIP LOCKED` to lock both tables

3. **Trade-off for new domains:**
   - Multiple workers might claim URLs from a brand new domain simultaneously
   - But this only happens once per domain (when first discovered)
   - The politeness check in `get_next_url` will catch this and unclaim extras

This solution provides domain collision prevention for established domains (the vast majority) while gracefully handling new domains without the PostgreSQL error.

The query should now work correctly. Give it a test run!

---

**User**

```
2025-06-10 07:42:31 - ERROR - crawler_module.frontier [2105672] - DB error in atomic claim: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT
2025-06-10 07:42:31 - ERROR - crawler_module.frontier [2105672] - Error in get_next_url: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT
Traceback (most recent call last):
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 259, in get_next_url
    claimed_urls = await self._atomic_claim_urls(batch_size, claim_expiry_seconds)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 353, in _atomic_claim_urls
    claimed_urls = await self.storage.db.execute_returning(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/db_backends.py", line 440, in execute_returning
    await cur.execute(query, params or ())
  File "/home/ubuntu/vibe-coding-experiments/crawler/.venv/lib/python3.12/site-packages/psycopg/cursor_async.py", line 97, in execute
    raise ex.with_traceback(None)
psycopg.errors.FeatureNotSupported: FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT

```

I think we probably want to just have separate queries for URLs without domain_metadata?

---

**Cursor**

You're absolutely right! PostgreSQL doesn't allow `FOR UPDATE` with `UNION`. Let's implement your suggestion of separate queries:

```python
                # Domain-aware claiming with domain locking to prevent collisions
                # First try: URLs from domains with metadata that are ready (with locking)
                sql_query = '''
                UPDATE frontier
                SET claimed_at = %s
                WHERE id = (
                    SELECT f.id
                    FROM frontier f
                    INNER JOIN domain_metadata dm ON f.domain = dm.domain
                    WHERE f.claimed_at IS NULL
                    AND dm.last_scheduled_fetch_timestamp <= %s
                    ORDER BY f.added_timestamp ASC
                    LIMIT 1
                    FOR UPDATE OF f, dm SKIP LOCKED
                )
                RETURNING id, url, domain, depth;
                '''
                
                claimed_urls = await self.storage.db.execute_returning(
                    sql_query,
                    (claim_timestamp, domain_ready_threshold),
                    query_name="atomic_claim_urls_pg_domain_aware"
                )
                
                # If no URLs from known domains, try new domains (no metadata yet)
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        WHERE f.claimed_at IS NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM domain_metadata dm 
                            WHERE dm.domain = f.domain
                        )
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp,),
                        query_name="atomic_claim_urls_pg_new_domain"
                    )
```

Now I need to add the logic for handling expired URLs:

```python
                # If no URLs from known domains, try new domains (no metadata yet)
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        WHERE f.claimed_at IS NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM domain_metadata dm 
                            WHERE dm.domain = f.domain
                        )
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp,),
                        query_name="atomic_claim_urls_pg_new_domain"
                    )
                
                # If still no URLs, try expired ones from known domains
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        INNER JOIN domain_metadata dm ON f.domain = dm.domain
                        WHERE f.claimed_at IS NOT NULL 
                        AND f.claimed_at < %s
                        AND dm.last_scheduled_fetch_timestamp <= %s
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f, dm SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp, expired_claim_timestamp_threshold, domain_ready_threshold),
                        query_name="atomic_claim_urls_pg_expired_known_domain"
                    )
                
                # Finally, try expired URLs from new domains
                if not claimed_urls:
                    sql_query = '''
                    UPDATE frontier
                    SET claimed_at = %s
                    WHERE id = (
                        SELECT f.id
                        FROM frontier f
                        WHERE f.claimed_at IS NOT NULL 
                        AND f.claimed_at < %s
                        AND NOT EXISTS (
                            SELECT 1 FROM domain_metadata dm 
                            WHERE dm.domain = f.domain
                        )
                        ORDER BY f.added_timestamp ASC
                        LIMIT 1
                        FOR UPDATE OF f SKIP LOCKED
                    )
                    RETURNING id, url, domain, depth;
                    '''
                    
                    claimed_urls = await self.storage.db.execute_returning(
                        sql_query,
                        (claim_timestamp, expired_claim_timestamp_threshold),
                        query_name="atomic_claim_urls_pg_expired_new_domain"
                    )
```

Perfect! I've restructured the claiming logic into a four-stage process that avoids PostgreSQL's limitations:

1. **First try**: URLs from domains with metadata that are ready (with `FOR UPDATE OF f, dm SKIP LOCKED` to prevent collisions)
2. **Second try**: URLs from new domains (no metadata yet, so only `FOR UPDATE OF f SKIP LOCKED`)
3. **Third try**: Expired URLs from known domains that are ready (with domain locking)
4. **Fourth try**: Expired URLs from new domains

This approach:
- **Prevents domain collisions** for established domains by locking both the frontier and domain_metadata rows
- **Handles new domains gracefully** (though multiple workers might claim from the same new domain initially, which is acceptable)
- **Falls back progressively** through different URL pools
- **Avoids PostgreSQL errors** by keeping queries simple and separate

The performance impact should be minimal since:
- Most claims will succeed on the first query (the common case)
- Each query is efficient with proper indexing
- Queries stop as soon as one succeeds

This should now work correctly. Give it a test run!

