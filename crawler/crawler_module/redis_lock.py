"""
Redis-based distributed locking for domain frontier writes.
Simple implementation using SETNX without timeout for single-machine deployments.
"""
import asyncio
import logging
import time
from typing import Optional
import redis.asyncio as redis

logger = logging.getLogger(__name__)

class DomainLock:
    """Manages write locks for domain frontiers using Redis."""
    
    def __init__(self, redis_client: redis.Redis, domain: str, is_reader: bool = False):
        self.redis = redis_client
        self.domain = domain
        # Readers can have a separate lock from writers because at every stage
        # of the domain write process, a consistent view is exposed to all
        # readers
        if is_reader:
            self.lock_key = f"lock:domain:{domain}:reader"
        else:
            self.lock_key = f"lock:domain:{domain}:writer"
        self.lock_acquired = False
        
    async def acquire(self, max_wait_seconds: float = 30.0) -> bool:
        """
        Try to acquire the lock for this domain.
        
        Args:
            max_wait_seconds: Maximum time to wait for the lock
            
        Returns:
            True if lock acquired, False if timeout
        """
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < max_wait_seconds:
            # Try to set the lock using SETNX (set if not exists)
            # Returns 1 if key was set, 0 if it already existed
            acquired = await self.redis.setnx(self.lock_key, "1")
            
            if acquired:
                self.lock_acquired = True
                logger.debug(f"Acquired lock for domain {self.domain} after {attempt} attempts")
                return True
            
            # Lock is held by someone else, wait and retry
            attempt += 1
            if attempt == 1:
                logger.debug(f"Waiting for lock on domain {self.domain}...")
            elif attempt % 10 == 0:
                logger.warning(f"Still waiting for lock on domain {self.domain} (attempt {attempt})")
            
            # Exponential backoff with jitter to avoid thundering herd
            wait_time = min(0.1 * (1.5 ** min(attempt, 10)), 2.0)  # Cap at 2 seconds
            jitter = 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
            await asyncio.sleep(wait_time + jitter)
        
        logger.error(f"Failed to acquire lock for domain {self.domain} after {max_wait_seconds}s")
        return False
    
    async def release(self):
        """Release the lock if we hold it."""
        if self.lock_acquired:
            await self.redis.delete(self.lock_key)
            self.lock_acquired = False
            logger.debug(f"Released lock for domain {self.domain}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        if not await self.acquire():
            raise TimeoutError(f"Could not acquire lock for domain {self.domain}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - always release the lock."""
        await self.release()


class LockManager:
    """Manages all domain locks and provides utilities."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        
    async def clear_all_locks(self):
        """
        Clear all domain locks. Used on startup to remove zombie locks.
        Should only be called when no other processes are running.
        """
        logger.info("Clearing all domain locks...")
        
        # Find all lock keys
        cursor = 0
        cleared_count = 0
        
        while True:
            cursor, keys = await self.redis.scan(
                cursor, 
                match="lock:domain:*",
                count=1000
            )
            
            if keys:
                # Delete all found lock keys
                await self.redis.delete(*keys)
                cleared_count += len(keys)
            
            if cursor == 0:
                break
        
        if cleared_count > 0:
            logger.warning(f"Cleared {cleared_count} zombie locks from previous run")
        else:
            logger.info("No zombie locks found")
        
        return cleared_count
    
    async def get_locked_domains(self) -> list[str]:
        """Get list of currently locked domains for monitoring."""
        locked_domains = []
        cursor = 0
        
        while True:
            cursor, keys = await self.redis.scan(
                cursor,
                match="lock:domain:*", 
                count=1000
            )
            
            for key in keys:
                # Extract domain from key format "lock:domain:{domain}"
                domain = key.split(":", 2)[2]
                locked_domains.append(domain)
            
            if cursor == 0:
                break
                
        return locked_domains
    
    def get_domain_read_lock(self, domain: str) -> DomainLock:
        """Create a DomainLock instance for the given domain."""
        return DomainLock(self.redis, domain, is_reader=True) 
    
    def get_domain_write_lock(self, domain: str) -> DomainLock:
        """Create a DomainLock instance for the given domain."""
        return DomainLock(self.redis, domain, is_reader=False) 