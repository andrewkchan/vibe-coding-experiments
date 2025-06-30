import asyncio
import redis.asyncio as redis
from typing import Any, TypeVar, Callable, Optional, Dict
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ShieldedRedis:
    """
    A wrapper around redis.asyncio.Redis that shields critical operations from cancellation.
    
    When async Redis operations are cancelled, they can leave connections in an unsafe state,
    causing connection leaks. This wrapper uses asyncio.shield() to ensure operations
    complete even if the calling task is cancelled.
    """
    
    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client
        self._shield_enabled = True
    
    async def _shielded_execute(self, coro):
        """Execute a Redis operation with cancellation protection."""
        if not self._shield_enabled:
            return await coro
        
        # Shield the operation from cancellation
        # This ensures the Redis operation completes and the connection is properly returned
        return await asyncio.shield(coro)
    
    # Wrap common methods that could leak connections
    async def get(self, key: str) -> Any:
        return await self._shielded_execute(self._redis.get(key))
    
    async def set(self, key: str, value: Any, **kwargs) -> Any:
        return await self._shielded_execute(self._redis.set(key, value, **kwargs))
    
    async def hget(self, name: str, key: str) -> Any:
        return await self._shielded_execute(self._redis.hget(name, key))
    
    async def hset(self, name: str, key: Optional[str] = None, value: Any = None, mapping: Optional[Dict[str, Any]] = None) -> Any:
        if mapping is not None:
            return await self._shielded_execute(self._redis.hset(name, mapping=mapping))
        return await self._shielded_execute(self._redis.hset(name, key, value))
    
    async def hmget(self, name: str, keys: list) -> Any:
        return await self._shielded_execute(self._redis.hmget(name, keys))
    
    async def hsetnx(self, name: str, key: str, value: Any) -> Any:
        return await self._shielded_execute(self._redis.hsetnx(name, key, value))
    
    async def lpop(self, name: str) -> Any:
        return await self._shielded_execute(self._redis.lpop(name))
    
    async def rpush(self, name: str, *values) -> Any:
        return await self._shielded_execute(self._redis.rpush(name, *values))
    
    async def llen(self, name: str) -> Any:
        return await self._shielded_execute(self._redis.llen(name))
    
    async def blpop(self, keys: Any, timeout: int = 0) -> Any:
        return await self._shielded_execute(self._redis.blpop(keys, timeout))
    
    async def execute_command(self, *args, **kwargs) -> Any:
        return await self._shielded_execute(self._redis.execute_command(*args, **kwargs))
    
    async def info(self, section: Optional[str] = None) -> Any:
        return await self._shielded_execute(self._redis.info(section))
    
    async def scan(self, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> Any:
        return await self._shielded_execute(self._redis.scan(cursor, match=match, count=count))
    
    async def delete(self, *keys) -> Any:
        return await self._shielded_execute(self._redis.delete(*keys))
    
    async def aclose(self) -> None:
        """Close the underlying Redis connection."""
        await self._redis.aclose()
    
    def pipeline(self, transaction: bool = True):
        """Return a pipeline with shielded execution."""
        return ShieldedPipeline(self._redis.pipeline(transaction), self._shield_enabled)
    
    @property
    def connection_pool(self):
        """Expose the underlying connection pool for monitoring."""
        return self._redis.connection_pool
    
    # Delegate other attributes to the underlying Redis client
    def __getattr__(self, name):
        attr = getattr(self._redis, name)
        if asyncio.iscoroutinefunction(attr):
            # Wrap any other async methods
            async def wrapped(*args, **kwargs):
                return await self._shielded_execute(attr(*args, **kwargs))
            return wrapped
        return attr
    
    # Support async context manager
    async def __aenter__(self):
        await self._redis.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._redis.__aexit__(exc_type, exc_val, exc_tb)


class ShieldedPipeline:
    """A pipeline wrapper that shields execution from cancellation."""
    
    def __init__(self, pipeline, shield_enabled: bool = True):
        self._pipeline = pipeline
        self._shield_enabled = shield_enabled
    
    async def execute(self):
        """Execute the pipeline with cancellation protection."""
        if not self._shield_enabled:
            return await self._pipeline.execute()
        return await asyncio.shield(self._pipeline.execute())
    
    def __getattr__(self, name):
        # Delegate all other methods to the underlying pipeline
        return getattr(self._pipeline, name) 