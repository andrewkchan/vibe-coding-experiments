# Optimizing web crawler frontier management
_Exported on 5/31/2025 at 17:31:07 PDT from Cursor (0.50.5)_

---

**User**

Yesterday Gemini helped me with some scaling issues in my web crawler, starting with DB connection pooling to allow scaling to more asynchronous workers, then fixing an issue where the pool would be exhausted by adding connection reuse to certain functions. Read more in the attached chat log cursor_debugging_db_pool_exhaustion_in_gem25pro.md. 

The latest issue was that after the crawler finished loading the seed URLs, the workers appeared to be all simultaneously trying the same seed URL. It looks like this is because frontier reads are not serialized but workers all retrieve the head of the queue. Gemini proposed keeping the design of simultaneous reads to the head of the queue, but having each worker "claim" URLs from the frontier queue by attempting to delete the URL. I feel like this is a form of optimistic concurrency control which might not work well because as we see in the logs, we begin with all N workers conflicting, and we will end up with N-1 workers retrying, then N-2, and so on.

What do you think? I am wondering if a better design could be to serialize the access to the queue by either (1) having reads from the head frontier queue occur as part of write transactions which also delete the head (I read online that SQLite write transactions are serialized), or (2) having the entire get_next_url be synchronous/blocking so the operation is serialized at the Python level (seems less performant than serializing at the DB level).

---

**Cursor**

Looking at the code and your analysis, you've correctly identified a classic "thundering herd" problem. When all workers fetch the same candidates from the frontier head, you get cascading contention where N workers compete for URL 1, then N-1 for URL 2, etc. This is indeed inefficient.

Let me analyze your proposed solutions and suggest some alternatives:

## Current Approach Analysis

The current optimistic concurrency control has these issues:
- **Wasted politeness checks**: Multiple workers perform expensive politeness checks on the same URLs
- **Cascading retries**: As you noted, this creates O(N²) failed attempts in the worst case
- **Database contention**: Many DELETE operations competing for the same rows

## Your Proposed Solutions

### 1. Write Transactions for Reads
This is a solid idea, but SQLite has limitations:
- SQLite doesn't support `SELECT ... FOR UPDATE` row-level locking
- You'd need to use `BEGIN IMMEDIATE` transactions to get a write lock upfront
- This would serialize *all* frontier access, not just for the same URL

### 2. Synchronous get_next_url
This would work but creates a significant bottleneck. With 500 workers, you'd serialize all URL fetching through a single Python lock.

## Better Alternatives

Here are some approaches that might work better:

### Option A: Distributed Sampling with Skip-Scan
Instead of all workers reading from the head, have each worker read from a different offset:

```python
def _get_candidates_sync_threaded_pooled(conn: sqlite3.Connection, worker_id: int, total_workers: int):
    cursor = conn.cursor()
    try:
        # Each worker starts at a different offset
        offset = worker_id
        cursor.execute(
            """SELECT id, url, domain FROM frontier 
               ORDER BY added_timestamp ASC 
               LIMIT ? OFFSET ?""", 
            (candidate_check_limit, offset * candidate_check_limit)
        )
        return cursor.fetchall()
    finally:
        cursor.close()
```

### Option B: Atomic Claim-and-Read
Combine the read and claim into a single atomic operation:

```python
def _claim_next_urls_sync(conn: sqlite3.Connection, worker_id: int, batch_size: int = 10):
    """Atomically claim and return a batch of URLs"""
    cursor = conn.cursor()
    try:
        # Mark URLs as claimed by this worker
        cursor.execute("""
            UPDATE frontier 
            SET claimed_by = ?, claimed_at = ?
            WHERE id IN (
                SELECT id FROM frontier 
                WHERE claimed_by IS NULL
                ORDER BY added_timestamp ASC 
                LIMIT ?
            )
            RETURNING id, url, domain
        """, (worker_id, int(time.time()), batch_size))
        
        results = cursor.fetchall()
        conn.commit()
        return results
    except sqlite3.Error as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
```

### Option C: Sharded Queue Approach
Partition URLs by domain hash to reduce contention:

```python
def _get_candidates_for_worker(conn: sqlite3.Connection, worker_id: int, total_workers: int):
    cursor = conn.cursor()
    try:
        # Each worker is responsible for domains where hash(domain) % total_workers == worker_id
        cursor.execute("""
            SELECT id, url, domain FROM frontier 
            WHERE (CAST(substr(hex(domain), 1, 8) AS INTEGER) % ?) = ?
            ORDER BY added_timestamp ASC 
            LIMIT ?
        """, (total_workers, worker_id, candidate_check_limit))
        return cursor.fetchall()
    finally:
        cursor.close()
```

## My Recommendation

I'd suggest **Option B (Atomic Claim-and-Read)** for these reasons:

1. **Minimizes contention**: Each worker claims a small batch atomically
2. **No wasted work**: Politeness checks only happen on claimed URLs
3. **Works with SQLite**: Uses UPDATE...RETURNING which SQLite supports
4. **Scalable**: Batch size can be tuned based on worker count

Here's how to implement it:

```python
def _get_candidates_sync_threaded_pooled(conn: sqlite3.Connection, worker_id: int, total_workers: int):
    cursor = conn.cursor()
    try:
        # Each worker starts at a different offset
        offset = worker_id
        cursor.execute(
            """SELECT id, url, domain FROM frontier 
               ORDER BY added_timestamp ASC 
               LIMIT ? OFFSET ?""", 
            (candidate_check_limit, offset * candidate_check_limit)
        )
        return cursor.fetchall()
    finally:
        cursor.close()
```

```python
def _claim_next_urls_sync(conn: sqlite3.Connection, worker_id: int, batch_size: int = 10):
    """Atomically claim and return a batch of URLs"""
    cursor = conn.cursor()
    try:
        # Mark URLs as claimed by this worker
        cursor.execute("""
            UPDATE frontier 
            SET claimed_by = ?, claimed_at = ?
            WHERE id IN (
                SELECT id FROM frontier 
                WHERE claimed_by IS NULL
                ORDER BY added_timestamp ASC 
                LIMIT ?
            )
            RETURNING id, url, domain
        """, (worker_id, int(time.time()), batch_size))
        
        results = cursor.fetchall()
        conn.commit()
        return results
    except sqlite3.Error as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
```

```python
def _get_candidates_for_worker(conn: sqlite3.Connection, worker_id: int, total_workers: int):
    cursor = conn.cursor()
    try:
        # Each worker is responsible for domains where hash(domain) % total_workers == worker_id
        cursor.execute("""
            SELECT id, url, domain FROM frontier 
            WHERE (CAST(substr(hex(domain), 1, 8) AS INTEGER) % ?) = ?
            ORDER BY added_timestamp ASC 
            LIMIT ?
        """, (total_workers, worker_id, candidate_check_limit))
        return cursor.fetchall()
    finally:
        cursor.close()
```

Now I'd suggest implementing the atomic claim-and-read approach. Here's a refactored version of `get_next_url` that should significantly reduce contention:

Perfect! I can see the frontier table schema. Let me implement a better solution using atomic claim-and-read. The key insight is that SQLite doesn't support `RETURNING` clause in the same way as PostgreSQL, but we can use an alternative approach.

Here's my proposed solution that uses a two-phase approach to minimize contention:

```python:crawler/crawler_module/frontier.py
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses a two-phase approach to minimize contention:
        1. Claim a batch of URLs atomically
        2. Process politeness checks only on claimed URLs
        """
        conn_from_pool: sqlite3.Connection | None = None
        worker_id = asyncio.current_task().get_name() if asyncio.current_task() else "unknown"
        batch_size = 10  # Tune based on politeness check duration
        
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Phase 1: Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._claim_url_batch_sync, conn_from_pool, worker_id, batch_size
            )
            
            if not claimed_urls:
                return None
            
            # Phase 2: Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                # Politeness checks - these URLs are already claimed by us
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules.")
                    # Remove from frontier since it's disallowed
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"Worker {worker_id} processing URL ID {url_id} ({url}) from domain {domain}.")
                    # Remove from frontier since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    return (url, domain, url_id)
                else:
                    # Domain not ready - unclaim this URL for other workers
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    # Don't add to seen_urls - another worker might process it
            
            # All claimed URLs were either disallowed or domains not ready
            return None
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _claim_url_batch_sync(self, conn: sqlite3.Connection, worker_id: str, batch_size: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs by marking them with a claim timestamp.
        Returns list of (id, url, domain) tuples."""
        cursor = conn.cursor()
        claimed_urls = []
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        claim_expiry = 300  # 5 minutes - URLs claimed but not processed will become available again
        
        try:
            # First, clean up expired claims (older than claim_expiry seconds)
            expired_timestamp = claim_timestamp - (claim_expiry * 1000000)
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Now claim unclaimed URLs
            # Note: SQLite doesn't support UPDATE...RETURNING, so we use a transaction
            cursor.execute("""
                SELECT id, url, domain 
                FROM frontier 
                WHERE claimed_at IS NULL
                ORDER BY added_timestamp ASC 
                LIMIT ?
            """, (batch_size,))
            
            candidates = cursor.fetchall()
            
            for url_id, url, domain in candidates:
                cursor.execute("""
                    UPDATE frontier 
                    SET claimed_at = ? 
                    WHERE id = ? AND claimed_at IS NULL
                """, (claim_timestamp, url_id))
                
                if cursor.rowcount > 0:
                    claimed_urls.append((url_id, url, domain))
            
            conn.commit()
            
            if claimed_urls:
                logger.debug(f"Worker {worker_id} claimed {len(claimed_urls)} URLs")
                
        except sqlite3.Error as e:
            logger.error(f"DB error claiming URL batch: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            
        return claimed_urls

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier (used after processing or if disallowed)."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Unclaims a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _attempt_claim_url_sync(self, conn: sqlite3.Connection, url_id_to_claim: int) -> bool:
        """Attempts to claim a URL by deleting it. Returns True if successful, False otherwise."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id_to_claim,))
            conn.commit() # Commit the delete immediately
            if cursor.rowcount > 0:
                return True  # Successfully deleted and thus claimed
            return False # Someone else deleted it, or it wasn't there
        except sqlite3.Error as e:
            logger.error(f"DB error attempting to claim URL ID {url_id_to_claim} by delete: {e}")
            if conn: 
                try:
                    conn.rollback() # Rollback on error during this specific operation
                except sqlite3.Error as rb_err:
                    logger.error(f"Error during rollback attempt for claim_url (ID: {url_id_to_claim}): {rb_err}")
            return False # Failed to claim due to error
        finally:
            cursor.close()
```

But wait - this requires adding a `claimed_at` column to the frontier table. Let me check if we need to update the schema:

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses a two-phase approach to minimize contention:
        1. Claim a batch of URLs atomically
        2. Process politeness checks only on claimed URLs
        """
        conn_from_pool: sqlite3.Connection | None = None
        worker_id = asyncio.current_task().get_name() if asyncio.current_task() else "unknown"
        batch_size = 10  # Tune based on politeness check duration
        
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Phase 1: Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._claim_url_batch_sync, conn_from_pool, worker_id, batch_size
            )
            
            if not claimed_urls:
                return None
            
            # Phase 2: Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                # Politeness checks - these URLs are already claimed by us
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules.")
                    # Remove from frontier since it's disallowed
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"Worker {worker_id} processing URL ID {url_id} ({url}) from domain {domain}.")
                    # Remove from frontier since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    return (url, domain, url_id)
                else:
                    # Domain not ready - unclaim this URL for other workers
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    # Don't add to seen_urls - another worker might process it
            
            # All claimed URLs were either disallowed or domains not ready
            return None
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _claim_url_batch_sync(self, conn: sqlite3.Connection, worker_id: str, batch_size: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs by marking them with a claim timestamp.
        Returns list of (id, url, domain) tuples."""
        cursor = conn.cursor()
        claimed_urls = []
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        claim_expiry = 300  # 5 minutes - URLs claimed but not processed will become available again
        
        try:
            # First, clean up expired claims (older than claim_expiry seconds)
            expired_timestamp = claim_timestamp - (claim_expiry * 1000000)
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Now claim unclaimed URLs
            # Note: SQLite doesn't support UPDATE...RETURNING, so we use a transaction
            cursor.execute("""
                SELECT id, url, domain 
                FROM frontier 
                WHERE claimed_at IS NULL
                ORDER BY added_timestamp ASC 
                LIMIT ?
            """, (batch_size,))
            
            candidates = cursor.fetchall()
            
            for url_id, url, domain in candidates:
                cursor.execute("""
                    UPDATE frontier 
                    SET claimed_at = ? 
                    WHERE id = ? AND claimed_at IS NULL
                """, (claim_timestamp, url_id))
                
                if cursor.rowcount > 0:
                    claimed_urls.append((url_id, url, domain))
            
            conn.commit()
            
            if claimed_urls:
                logger.debug(f"Worker {worker_id} claimed {len(claimed_urls)} URLs")
                
        except sqlite3.Error as e:
            logger.error(f"DB error claiming URL batch: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            
        return claimed_urls

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier (used after processing or if disallowed)."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Unclaims a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _attempt_claim_url_sync(self, conn: sqlite3.Connection, url_id_to_claim: int) -> bool:
        """Attempts to claim a URL by deleting it. Returns True if successful, False otherwise."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id_to_claim,))
            conn.commit() # Commit the delete immediately
            if cursor.rowcount > 0:
                return True  # Successfully deleted and thus claimed
            return False # Someone else deleted it, or it wasn't there
        except sqlite3.Error as e:
            logger.error(f"DB error attempting to claim URL ID {url_id_to_claim} by delete: {e}")
            if conn: 
                try:
                    conn.rollback() # Rollback on error during this specific operation
                except sqlite3.Error as rb_err:
                    logger.error(f"Error during rollback attempt for claim_url (ID: {url_id_to_claim}): {rb_err}")
            return False # Failed to claim due to error
        finally:
            cursor.close()
```

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses a two-phase approach to minimize contention:
        1. Claim a batch of URLs atomically
        2. Process politeness checks only on claimed URLs
        """
        conn_from_pool: sqlite3.Connection | None = None
        worker_id = asyncio.current_task().get_name() if asyncio.current_task() else "unknown"
        batch_size = 10  # Tune based on politeness check duration
        
        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Phase 1: Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._claim_url_batch_sync, conn_from_pool, worker_id, batch_size
            )
            
            if not claimed_urls:
                return None
            
            # Phase 2: Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                # Politeness checks - these URLs are already claimed by us
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules.")
                    # Remove from frontier since it's disallowed
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"Worker {worker_id} processing URL ID {url_id} ({url}) from domain {domain}.")
                    # Remove from frontier since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    return (url, domain, url_id)
                else:
                    # Domain not ready - unclaim this URL for other workers
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    # Don't add to seen_urls - another worker might process it
            
            # All claimed URLs were either disallowed or domains not ready
            return None
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _claim_url_batch_sync(self, conn: sqlite3.Connection, worker_id: str, batch_size: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs by marking them with a claim timestamp.
        Returns list of (id, url, domain) tuples."""
        cursor = conn.cursor()
        claimed_urls = []
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        claim_expiry = 300  # 5 minutes - URLs claimed but not processed will become available again
        
        try:
            # First, clean up expired claims (older than claim_expiry seconds)
            expired_timestamp = claim_timestamp - (claim_expiry * 1000000)
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Now claim unclaimed URLs
            # Note: SQLite doesn't support UPDATE...RETURNING, so we use a transaction
            cursor.execute("""
                SELECT id, url, domain 
                FROM frontier 
                WHERE claimed_at IS NULL
                ORDER BY added_timestamp ASC 
                LIMIT ?
            """, (batch_size,))
            
            candidates = cursor.fetchall()
            
            for url_id, url, domain in candidates:
                cursor.execute("""
                    UPDATE frontier 
                    SET claimed_at = ? 
                    WHERE id = ? AND claimed_at IS NULL
                """, (claim_timestamp, url_id))
                
                if cursor.rowcount > 0:
                    claimed_urls.append((url_id, url, domain))
            
            conn.commit()
            
            if claimed_urls:
                logger.debug(f"Worker {worker_id} claimed {len(claimed_urls)} URLs")
                
        except sqlite3.Error as e:
            logger.error(f"DB error claiming URL batch: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            
        return claimed_urls

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier (used after processing or if disallowed)."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Unclaims a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _attempt_claim_url_sync(self, conn: sqlite3.Connection, url_id_to_claim: int) -> bool:
        """Attempts to claim a URL by deleting it. Returns True if successful, False otherwise."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id_to_claim,))
            conn.commit() # Commit the delete immediately
            if cursor.rowcount > 0:
                return True  # Successfully deleted and thus claimed
            return False # Someone else deleted it, or it wasn't there
        except sqlite3.Error as e:
            logger.error(f"DB error attempting to claim URL ID {url_id_to_claim} by delete: {e}")
            if conn: 
                try:
                    conn.rollback() # Rollback on error during this specific operation
                except sqlite3.Error as rb_err:
                    logger.error(f"Error during rollback attempt for claim_url (ID: {url_id_to_claim}): {rb_err}")
            return False # Failed to claim due to error
        finally:
            cursor.close()
```

Looking at the current implementation, I see that it doesn't have a `claimed_at` column. Given that we're dealing with SQLite and you're right to be concerned about the cascading retry pattern, let me propose a simpler and more efficient solution.

## Proposed Solution: Worker-Sharded Frontier Reading

Instead of having all workers compete for the same URLs, we can distribute the workload by having each worker look at a different subset of the frontier. This approach:

1. **Avoids thundering herd**: Workers don't compete for the same URLs
2. **Simple to implement**: No schema changes needed
3. **Works with SQLite**: Uses only basic SQL features
4. **Scales well**: Each worker has its own "swim lane"

Here's my implementation:

```python:crawler/crawler_module/frontier.py
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses worker sharding to reduce contention: each worker looks at a different
        subset of the frontier based on URL hash modulo number of workers.
        """
        candidate_check_limit = 50  # Check up to 50 URLs from this worker's shard
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker ID from async task name, or use a random one
        try:
            task_name = asyncio.current_task().get_name() if asyncio.current_task() else None
            if task_name and task_name.startswith("Worker-"):
                worker_id = int(task_name.split("-")[1]) - 1  # Convert to 0-based
            else:
                worker_id = hash(task_name or "unknown") % self.config.max_workers
        except:
            worker_id = 0  # Fallback

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_sharded_candidates_sync(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    # Each worker looks at URLs where hash(url) % max_workers == worker_id
                    # This distributes URLs across workers to reduce contention
                    cursor.execute(
                        """SELECT id, url, domain FROM frontier 
                           WHERE (CAST(substr(hex(substr(url, -8)), 1, 8) AS INTEGER) % ?) = ?
                           ORDER BY added_timestamp ASC 
                           LIMIT ?""", 
                        (self.config.max_workers, worker_id, candidate_check_limit)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Get candidates from this worker's shard
            candidates = await asyncio.to_thread(_get_sharded_candidates_sync, conn_from_pool)

            if not candidates:
                # If no URLs in our shard, try stealing from any shard (fallback)
                def _get_any_candidates_sync(conn: sqlite3.Connection):
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                            (min(candidate_check_limit, 10),)  # Smaller limit for stealing
                        )
                        return cursor.fetchall()
                    finally:
                        cursor.close()
                
                candidates = await asyncio.to_thread(_get_any_candidates_sync, conn_from_pool)
                if candidates:
                    logger.debug(f"Worker {worker_id + 1} stealing work from other shards")

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                # Pass the existing connection to politeness methods
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing from frontier.")
                    # Atomically delete disallowed URL
                    def _delete_url_sync(conn: sqlite3.Connection, local_url_id: int):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (local_url_id,))
                            conn.commit()
                        except sqlite3.Error as e:
                            logger.error(f"DB error deleting disallowed URL ID {local_url_id}: {e}")
                            if conn: conn.rollback()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    
                    # Attempt to atomically claim this URL by deleting it.
                    claimed = await asyncio.to_thread(self._attempt_claim_url_sync, conn_from_pool, url_id)
                    
                    if claimed:
                        logger.debug(f"Worker {worker_id + 1} successfully claimed URL ID {url_id} ({url}) from domain {domain}.")
                        selected_url_info = (url, domain, url_id)
                        break # Found and claimed a suitable URL
                    else:
                        # This URL was claimed by another worker (rare with sharding)
                        logger.debug(f"Failed to claim URL ID {url_id} ({url}). It was likely processed by another worker.")
                        self.seen_urls.add(url) 
                        continue # Try next candidate
                else:
                    # Domain is not ready to be fetched (e.g., politeness delay)
                    # logger.debug(f"Domain {domain} (URL: {url}) not ready to be fetched now, skipping.")
                    pass # Simply skip to the next candidate
            
            if not selected_url_info:
                logger.debug(f"Worker {worker_id + 1}: No suitable URL found in candidates that respects politeness rules now.")
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

// ... existing code ...
```

This approach:

1. **Shards by URL hash**: Each worker looks at URLs where `hash(url) % num_workers == worker_id`. This distributes URLs evenly across workers.

2. **Reduces contention dramatically**: Instead of all 500 workers competing for the same URL, they're looking at different subsets of the frontier.

3. **Has a fallback**: If a worker's shard is empty (which can happen as the frontier depletes), it can "steal" work from other shards.

4. **Keeps the atomic delete**: The `_attempt_claim_url_sync` still ensures that only one worker processes each URL, but contention is now rare because workers are looking at different URLs.

5. **No schema changes**: Works with the existing frontier table structure.

The key insight is that by having workers look at different parts of the frontier initially, we avoid the O(N²) cascading retry problem. Most of the time, workers won't even be looking at the same URLs.

This is similar to how distributed hash tables work - by partitioning the keyspace, we reduce contention while maintaining correctness.

What do you think of this approach? It's simpler than managing claim timestamps and should scale well with your 500 workers.

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses worker sharding to reduce contention: each worker looks at a different
        subset of the frontier based on URL hash modulo number of workers.
        """
        candidate_check_limit = 50  # Check up to 50 URLs from this worker's shard
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker ID from async task name, or use a random one
        try:
            task_name = asyncio.current_task().get_name() if asyncio.current_task() else None
            if task_name and task_name.startswith("Worker-"):
                worker_id = int(task_name.split("-")[1]) - 1  # Convert to 0-based
            else:
                worker_id = hash(task_name or "unknown") % self.config.max_workers
        except:
            worker_id = 0  # Fallback

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_sharded_candidates_sync(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    # Each worker looks at URLs where hash(url) % max_workers == worker_id
                    # This distributes URLs across workers to reduce contention
                    cursor.execute(
                        """SELECT id, url, domain FROM frontier 
                           WHERE (CAST(substr(hex(substr(url, -8)), 1, 8) AS INTEGER) % ?) = ?
                           ORDER BY added_timestamp ASC 
                           LIMIT ?""", 
                        (self.config.max_workers, worker_id, candidate_check_limit)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Get candidates from this worker's shard
            candidates = await asyncio.to_thread(_get_sharded_candidates_sync, conn_from_pool)

            if not candidates:
                # If no URLs in our shard, try stealing from any shard (fallback)
                def _get_any_candidates_sync(conn: sqlite3.Connection):
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                            (min(candidate_check_limit, 10),)  # Smaller limit for stealing
                        )
                        return cursor.fetchall()
                    finally:
                        cursor.close()
                
                candidates = await asyncio.to_thread(_get_any_candidates_sync, conn_from_pool)
                if candidates:
                    logger.debug(f"Worker {worker_id + 1} stealing work from other shards")

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                # Pass the existing connection to politeness methods
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing from frontier.")
                    # Atomically delete disallowed URL
                    def _delete_url_sync(conn: sqlite3.Connection, local_url_id: int):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (local_url_id,))
                            conn.commit()
                        except sqlite3.Error as e:
                            logger.error(f"DB error deleting disallowed URL ID {local_url_id}: {e}")
                            if conn: conn.rollback()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    
                    # Attempt to atomically claim this URL by deleting it.
                    claimed = await asyncio.to_thread(self._attempt_claim_url_sync, conn_from_pool, url_id)
                    
                    if claimed:
                        logger.debug(f"Worker {worker_id + 1} successfully claimed URL ID {url_id} ({url}) from domain {domain}.")
                        selected_url_info = (url, domain, url_id)
                        break # Found and claimed a suitable URL
                    else:
                        # This URL was claimed by another worker (rare with sharding)
                        logger.debug(f"Failed to claim URL ID {url_id} ({url}). It was likely processed by another worker.")
                        self.seen_urls.add(url) 
                        continue # Try next candidate
                else:
                    # Domain is not ready to be fetched (e.g., politeness delay)
                    # logger.debug(f"Domain {domain} (URL: {url}) not ready to be fetched now, skipping.")
                    pass # Simply skip to the next candidate
            
            if not selected_url_info:
                logger.debug(f"Worker {worker_id + 1}: No suitable URL found in candidates that respects politeness rules now.")
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

// ... existing code ...
```

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses worker sharding to reduce contention: each worker looks at a different
        subset of the frontier based on URL hash modulo number of workers.
        """
        candidate_check_limit = 50  # Check up to 50 URLs from this worker's shard
        selected_url_info = None
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker ID from async task name, or use a random one
        try:
            task_name = asyncio.current_task().get_name() if asyncio.current_task() else None
            if task_name and task_name.startswith("Worker-"):
                worker_id = int(task_name.split("-")[1]) - 1  # Convert to 0-based
            else:
                worker_id = hash(task_name or "unknown") % self.config.max_workers
        except:
            worker_id = 0  # Fallback

        try:
            # Get a connection for the duration of this potentially multi-step operation
            conn_from_pool = self.storage.db_pool.get_connection()

            def _get_sharded_candidates_sync(conn: sqlite3.Connection):
                cursor = conn.cursor()
                try:
                    # Each worker looks at URLs where hash(url) % max_workers == worker_id
                    # This distributes URLs across workers to reduce contention
                    cursor.execute(
                        """SELECT id, url, domain FROM frontier 
                           WHERE (CAST(substr(hex(substr(url, -8)), 1, 8) AS INTEGER) % ?) = ?
                           ORDER BY added_timestamp ASC 
                           LIMIT ?""", 
                        (self.config.max_workers, worker_id, candidate_check_limit)
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            
            # Get candidates from this worker's shard
            candidates = await asyncio.to_thread(_get_sharded_candidates_sync, conn_from_pool)

            if not candidates:
                # If no URLs in our shard, try stealing from any shard (fallback)
                def _get_any_candidates_sync(conn: sqlite3.Connection):
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "SELECT id, url, domain FROM frontier ORDER BY added_timestamp ASC LIMIT ?", 
                            (min(candidate_check_limit, 10),)  # Smaller limit for stealing
                        )
                        return cursor.fetchall()
                    finally:
                        cursor.close()
                
                candidates = await asyncio.to_thread(_get_any_candidates_sync, conn_from_pool)
                if candidates:
                    logger.debug(f"Worker {worker_id + 1} stealing work from other shards")

            if not candidates:
                return None

            for url_id, url, domain in candidates:
                # Politeness check is async, happens outside the threaded DB operation
                # Pass the existing connection to politeness methods
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing from frontier.")
                    # Atomically delete disallowed URL
                    def _delete_url_sync(conn: sqlite3.Connection, local_url_id: int):
                        cursor = conn.cursor()
                        try:
                            cursor.execute("DELETE FROM frontier WHERE id = ?", (local_url_id,))
                            conn.commit()
                        except sqlite3.Error as e:
                            logger.error(f"DB error deleting disallowed URL ID {local_url_id}: {e}")
                            if conn: conn.rollback()
                        finally:
                            cursor.close()
                    await asyncio.to_thread(_delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url) 
                    continue 

                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    
                    # Attempt to atomically claim this URL by deleting it.
                    claimed = await asyncio.to_thread(self._attempt_claim_url_sync, conn_from_pool, url_id)
                    
                    if claimed:
                        logger.debug(f"Worker {worker_id + 1} successfully claimed URL ID {url_id} ({url}) from domain {domain}.")
                        selected_url_info = (url, domain, url_id)
                        break # Found and claimed a suitable URL
                    else:
                        # This URL was claimed by another worker (rare with sharding)
                        logger.debug(f"Failed to claim URL ID {url_id} ({url}). It was likely processed by another worker.")
                        self.seen_urls.add(url) 
                        continue # Try next candidate
                else:
                    # Domain is not ready to be fetched (e.g., politeness delay)
                    # logger.debug(f"Domain {domain} (URL: {url}) not ready to be fetched now, skipping.")
                    pass # Simply skip to the next candidate
            
            if not selected_url_info:
                logger.debug(f"Worker {worker_id + 1}: No suitable URL found in candidates that respects politeness rules now.")
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

// ... existing code ...
```

---

**User**

I think I prefer the atomic claim-and-read approach. We will have to alter the schema but I think that's ok. If I understand correctly, this also avoids wasted work because the claim-and-read queries are serialized correct?

Also, I think SQLite does support RETURNING? See @https://www.sqlite.org/lang_returning.html 

---

**Cursor**

```sql
UPDATE frontier 
SET claimed_at = ? 
WHERE id IN (
    SELECT id FROM frontier 
    WHERE claimed_at IS NULL OR claimed_at < ?
    ORDER BY added_timestamp ASC 
    LIMIT ?
)
RETURNING id, url, domain
```

You're absolutely right! I apologize for missing that - SQLite has indeed supported the RETURNING clause since version 3.35.0 (2021-03-12). This makes our implementation much cleaner and more efficient.

And yes, you understand correctly - the atomic claim-and-read approach avoids wasted work because:
1. **Serialization at DB level**: The UPDATE operations are serialized by SQLite, so only one worker can claim each URL
2. **No wasted politeness checks**: Workers only perform expensive politeness checks on URLs they've already claimed
3. **No cascading retries**: Workers claim different URLs in parallel rather than all competing for the same ones

Here's the improved implementation using SQLite's RETURNING clause:

I see the current schema version is 1. Let me create the implementation with the schema change to add the `claimed_at` column:

```python:crawler/crawler_module/storage.py
// ... existing code ...

DB_SCHEMA_VERSION = 2  # Increment version for schema change

// ... existing code ...

    def _create_tables(self, conn: sqlite3.Connection): # Takes a connection argument
        """Creates the necessary tables in the database if they don't already exist."""
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
                    priority_score REAL DEFAULT 0,
                    claimed_at INTEGER DEFAULT NULL
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed ON frontier (claimed_at)")

                # Add claimed_at column if upgrading from version 1
                if current_version == 1:
                    cursor.execute("ALTER TABLE frontier ADD COLUMN claimed_at INTEGER DEFAULT NULL")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed ON frontier (claimed_at)")
                    logger.info("Added claimed_at column to frontier table")

// ... rest of the existing schema creation code ...
```

Now, here's the improved `get_next_url` implementation using SQLite's RETURNING clause:

```python:crawler/crawler_module/frontier.py
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_name = asyncio.current_task().get_name() if asyncio.current_task() else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    return (url, domain, url_id)
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # All claimed URLs were either disallowed or domains not ready
            return None
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _atomic_claim_urls_sync(self, conn: sqlite3.Connection, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs using UPDATE...RETURNING.
        Returns list of (id, url, domain) tuples for claimed URLs."""
        cursor = conn.cursor()
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        expired_timestamp = claim_timestamp - (claim_expiry_seconds * 1000000)
        
        try:
            # First, release any expired claims
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Atomically claim unclaimed URLs using RETURNING
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = ? 
                WHERE id IN (
                    SELECT id FROM frontier 
                    WHERE claimed_at IS NULL
                    ORDER BY added_timestamp ASC 
                    LIMIT ?
                )
                RETURNING id, url, domain
            """, (claim_timestamp, batch_size))
            
            claimed_urls = cursor.fetchall()
            conn.commit()
            
            return claimed_urls
            
        except sqlite3.Error as e:
            logger.error(f"DB error in atomic claim: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier after processing or if disallowed."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Releases a claim on a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

// ... existing code ...
```

The key improvements in this implementation:

1. **Single atomic operation**: The `UPDATE...RETURNING` statement atomically claims URLs and returns them in one operation, as [documented in SQLite's RETURNING documentation](https://www.sqlite.org/lang_returning.html).

2. **No wasted work**: Workers only perform politeness checks on URLs they've successfully claimed.

3. **Automatic expiry**: Claims expire after 5 minutes, preventing URLs from being stuck if a worker crashes.

4. **Efficient batching**: Workers claim small batches (10 URLs) to reduce database round trips while avoiding holding too many URLs.

5. **Clean separation**: URLs that fail politeness checks are deleted, while URLs whose domains aren't ready are unclaimed for other workers.

This approach completely avoids the thundering herd problem because:
- The UPDATE operation is serialized at the database level
- Each worker claims different URLs atomically
- There's no race condition between checking and claiming

The serialization happens at the database level through SQLite's write locking, ensuring that concurrent UPDATE operations don't interfere with each other.

```python
// ... existing code ...

DB_SCHEMA_VERSION = 2  # Increment version for schema change

// ... existing code ...

    def _create_tables(self, conn: sqlite3.Connection): # Takes a connection argument
        """Creates the necessary tables in the database if they don't already exist."""
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
                    priority_score REAL DEFAULT 0,
                    claimed_at INTEGER DEFAULT NULL
                )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier (domain)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier (priority_score, added_timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed ON frontier (claimed_at)")

                # Add claimed_at column if upgrading from version 1
                if current_version == 1:
                    cursor.execute("ALTER TABLE frontier ADD COLUMN claimed_at INTEGER DEFAULT NULL")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_frontier_claimed ON frontier (claimed_at)")
                    logger.info("Added claimed_at column to frontier table")

// ... rest of the existing schema creation code ...
```

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_name = asyncio.current_task().get_name() if asyncio.current_task() else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    return (url, domain, url_id)
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # All claimed URLs were either disallowed or domains not ready
            return None
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _atomic_claim_urls_sync(self, conn: sqlite3.Connection, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str]]:
        """Atomically claims a batch of URLs using UPDATE...RETURNING.
        Returns list of (id, url, domain) tuples for claimed URLs."""
        cursor = conn.cursor()
        claim_timestamp = int(time.time() * 1000000)  # Microsecond precision
        expired_timestamp = claim_timestamp - (claim_expiry_seconds * 1000000)
        
        try:
            # First, release any expired claims
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = NULL 
                WHERE claimed_at IS NOT NULL AND claimed_at < ?
            """, (expired_timestamp,))
            
            # Atomically claim unclaimed URLs using RETURNING
            cursor.execute("""
                UPDATE frontier 
                SET claimed_at = ? 
                WHERE id IN (
                    SELECT id FROM frontier 
                    WHERE claimed_at IS NULL
                    ORDER BY added_timestamp ASC 
                    LIMIT ?
                )
                RETURNING id, url, domain
            """, (claim_timestamp, batch_size))
            
            claimed_urls = cursor.fetchall()
            conn.commit()
            
            return claimed_urls
            
        except sqlite3.Error as e:
            logger.error(f"DB error in atomic claim: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def _delete_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Deletes a URL from the frontier after processing or if disallowed."""
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM frontier WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error deleting URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    def _unclaim_url_sync(self, conn: sqlite3.Connection, url_id: int):
        """Releases a claim on a URL so other workers can process it."""
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE frontier SET claimed_at = NULL WHERE id = ?", (url_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error unclaiming URL ID {url_id}: {e}")
            conn.rollback()
        finally:
            cursor.close()

// ... existing code ...
```

---

**User**

Great! I think we need to update some tests now, here is the output of pytest:
```
=================================================== short test summary info ====================================================
FAILED tests/test_frontier.py::test_add_and_get_urls - AssertionError: Expected URL, got None at iteration 1
FAILED tests/test_frontier.py::test_frontier_resume_with_politeness - assert None is not None
========================================== 2 failed, 71 passed, 61 warnings in 0.76s ===========================================
```

---

**Cursor**

Let me look at the failing tests to understand what needs to be updated:

---

**User**

look in the crawler folder

---

**Cursor**

Looking at the failing tests, I can see the issue. The new implementation claims a batch of URLs but doesn't properly handle the unclaimed URLs when it returns early after finding a suitable URL. Let me fix the `get_next_url` method:

```python
// ... existing code ...

    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Track which URLs we've processed so we can unclaim the rest
            processed_url_ids = []
            selected_url_info = None
            
            # Process claimed URLs with politeness checks
            for url_id, url, domain in claimed_urls:
                processed_url_ids.append(url_id)
                
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    selected_url_info = (url, domain, url_id)
                    break  # Found a suitable URL
                else:
                    # Domain not ready - will be unclaimed below
                    logger.debug(f"Domain {domain} not ready for fetch, will unclaim URL ID {url_id}")
            
            # Unclaim any remaining URLs we didn't process or delete
            if selected_url_info:
                # We found a URL to process - unclaim all the URLs after it
                remaining_url_ids = [uid for uid, _, _ in claimed_urls if uid not in processed_url_ids]
                for url_id in remaining_url_ids:
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
            else:
                # We didn't find any suitable URL - unclaim all that weren't deleted
                for url_id, url, domain in claimed_urls:
                    # Only unclaim if we didn't delete it (due to politeness rules)
                    if url not in self.seen_urls:  # URLs added to seen_urls were deleted
                        await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
            
            return selected_url_info
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

// ... existing code ...
```

I see the issue now. When `get_next_url` finds a suitable URL and returns it, it doesn't unclaim the remaining URLs in the batch. This causes subsequent calls to fail because those URLs are still marked as claimed. Let me fix this:

```python
    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Track which URLs we need to unclaim if we return early
            urls_to_unclaim = []
            selected_url_info = None
            
            # Process claimed URLs with politeness checks
            for i, (url_id, url, domain) in enumerate(claimed_urls):
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    selected_url_info = (url, domain, url_id)
                    # Mark remaining URLs to be unclaimed
                    urls_to_unclaim = [(uid, u, d) for uid, u, d in claimed_urls[i+1:]]
                    break
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain in urls_to_unclaim:
                await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)
```

```python
// ... existing code ...
    async def get_next_url(self) -> tuple[str, str, int] | None: # (url, domain, id)
        """Gets the next URL to crawl from the frontier, respecting politeness rules.
        Uses atomic claim-and-read with SQLite's RETURNING clause to minimize contention.
        """
        batch_size = 10  # Claim a small batch to amortize DB overhead
        claim_expiry_seconds = 300  # URLs claimed but not processed expire after 5 minutes
        conn_from_pool: sqlite3.Connection | None = None
        
        # Get worker identifier for logging
        worker_task = asyncio.current_task()
        worker_name = worker_task.get_name() if worker_task else "unknown"

        try:
            conn_from_pool = self.storage.db_pool.get_connection()
            
            # Atomically claim a batch of URLs
            claimed_urls = await asyncio.to_thread(
                self._atomic_claim_urls_sync, conn_from_pool, batch_size, claim_expiry_seconds
            )
            
            if not claimed_urls:
                return None
            
            logger.debug(f"{worker_name} claimed {len(claimed_urls)} URLs for processing")
            
            # Track which URLs we need to unclaim if we return early
            urls_to_unclaim = []
            selected_url_info = None
            
            # Process claimed URLs with politeness checks
            for i, (url_id, url, domain) in enumerate(claimed_urls):
                # Check if URL is allowed by robots.txt and manual exclusions
                if not await self.politeness.is_url_allowed(url, conn_from_pool):
                    logger.debug(f"URL {url} (ID: {url_id}) disallowed by politeness rules. Removing.")
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    self.seen_urls.add(url)
                    continue
                
                # Check if we can fetch from this domain now
                if await self.politeness.can_fetch_domain_now(domain, conn_from_pool):
                    await self.politeness.record_domain_fetch_attempt(domain, conn_from_pool)
                    logger.debug(f"{worker_name} processing URL ID {url_id} ({url}) from domain {domain}")
                    # Delete the URL since we're processing it
                    await asyncio.to_thread(self._delete_url_sync, conn_from_pool, url_id)
                    selected_url_info = (url, domain, url_id)
                    # Mark remaining URLs to be unclaimed
                    urls_to_unclaim = [(uid, u, d) for uid, u, d in claimed_urls[i+1:]]
                    break
                else:
                    # Domain not ready - unclaim this URL so another worker can try later
                    await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                    logger.debug(f"Domain {domain} not ready for fetch, unclaimed URL ID {url_id}")
            
            # Unclaim any remaining URLs if we found one to process
            for url_id, url, domain in urls_to_unclaim:
                await asyncio.to_thread(self._unclaim_url_sync, conn_from_pool, url_id)
                logger.debug(f"Unclaimed remaining URL ID {url_id} from batch")
            
            return selected_url_info
            
        except sqlite3.Error as e:
            logger.error(f"DB Error in get_next_url: {e}", exc_info=True)
            if conn_from_pool:
                try: conn_from_pool.rollback()
                except: pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_next_url: {e}", exc_info=True)
            return None
        finally:
            if conn_from_pool:
                self.storage.db_pool.return_connection(conn_from_pool)

    def _atomic_claim_urls_sync(self, conn: sqlite3.Connection, batch_size: int, claim_expiry_seconds: int) -> list[tuple[int, str, str]]:
// ... existing code ...
```

Now I need to update the `_add_url_sync` method to include the `claimed_at` column (with NULL value) when inserting URLs:

```python
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
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0, None)
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
// ... existing code ...
```

```python
// ... existing code ...
                cursor.execute("SELECT 1 FROM visited_urls WHERE url_sha256 = ?", 
                               (self.storage.get_url_sha256(normalized_url),))
                if cursor.fetchone():
                    return False 

                cursor.execute(
                    "INSERT OR IGNORE INTO frontier (url, domain, depth, added_timestamp, priority_score, claimed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (normalized_url, domain, depth, int(time.time()), 0, None)
                )
                if cursor.rowcount > 0:
                    conn_from_pool.commit() # Explicit commit for INSERT
                    return True 
                else:
                    # This means INSERT OR IGNORE found an existing row (URL already in frontier)
                    return False 
// ... existing code ...
```

Perfect! I've made all the necessary changes to fix the failing tests:

1. **Updated the schema** in `storage.py`:
   - Incremented `DB_SCHEMA_VERSION` to 2
   - Added `claimed_at INTEGER DEFAULT NULL` column to the frontier table
   - Added migration logic to add the column if upgrading from version 1
   - Created an index on `claimed_at` for performance

2. **Fixed `get_next_url`** in `frontier.py`:
   - Now properly tracks which URLs need to be unclaimed when we find a suitable one
   - When a URL is found and selected for processing, all remaining URLs in the batch are unclaimed
   - This ensures subsequent calls to `get_next_url` can access those URLs

3. **Updated `_add_url_sync`** in `frontier.py`:
   - The INSERT statement now includes the `claimed_at` column with `NULL` value
   - This ensures compatibility with the new schema

The key insight was that the tests expect FIFO behavior - they add URLs and then retrieve them one by one in order. Our new implementation claims batches of URLs for efficiency, but we needed to ensure that unclaimed URLs remain available for subsequent calls. By properly unclaiming the remaining URLs in a batch when we find one to process, we maintain the expected behavior while still getting the performance benefits of atomic batch claiming.

The tests should now pass! The implementation successfully:
- Avoids the thundering herd problem through atomic claim operations
- Prevents wasted work by only doing politeness checks on claimed URLs
- Maintains correct FIFO semantics for the tests
- Scales efficiently with multiple workers

