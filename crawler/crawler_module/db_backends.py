"""
Database backend abstraction layer for the web crawler.
Supports both SQLite and PostgreSQL with a common interface.
"""

import abc
import asyncio
import logging
import sqlite3
import time
from typing import Any, List, Tuple, Optional, Dict, TYPE_CHECKING
from pathlib import Path
from contextlib import asynccontextmanager
import queue
import threading
import traceback

# Type checking imports
if TYPE_CHECKING:
    try:
        import psycopg_pool
        import psycopg
    except ImportError:
        psycopg_pool = None
        psycopg = None

logger = logging.getLogger(__name__)

# Connections held longer than this threshold in seconds log a trace
CONNECTION_HOLD_THRESHOLD = 2.0

class DatabaseBackend(abc.ABC):
    """Abstract base class for database backends."""
    
    @abc.abstractmethod
    async def initialize(self) -> None:
        """Initialize the database connection pool."""
        pass
    
    @abc.abstractmethod
    async def close(self) -> None:
        """Close all database connections."""
        pass
    
    @abc.abstractmethod
    async def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query without returning results."""
        pass
    
    @abc.abstractmethod
    async def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """Execute a query multiple times with different parameters."""
        pass
    
    @abc.abstractmethod
    async def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Execute a query and return a single row."""
        pass
    
    @abc.abstractmethod
    async def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query and return all rows."""
        pass
    
    @abc.abstractmethod
    async def execute_returning(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query with RETURNING clause and return results."""
        pass
    
    @abc.abstractmethod
    async def begin_transaction(self) -> Any:
        """Begin a transaction. Returns a transaction context."""
        pass
    
    @abc.abstractmethod
    async def commit_transaction(self, tx: Any) -> None:
        """Commit a transaction."""
        pass
    
    @abc.abstractmethod
    async def rollback_transaction(self, tx: Any) -> None:
        """Rollback a transaction."""
        pass


class SQLiteBackend(DatabaseBackend):
    """SQLite implementation of the database backend."""
    
    def __init__(self, db_path: str | Path, pool_size: int = 10, timeout: int = 30):
        self.db_path = str(db_path)
        self.pool_size = pool_size
        self.timeout = timeout
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._connections_created = 0
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize the SQLite connection pool."""
        if self._initialized:
            return
        
        # Create connections in a thread to avoid blocking
        await asyncio.to_thread(self._initialize_pool)
        self._initialized = True
    
    def _initialize_pool(self):
        """Initialize the connection pool (runs in thread)."""
        for _ in range(self.pool_size):
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Exception as e:
                logger.error(f"Error creating initial connection for pool: {e}")
        logger.info(f"SQLite pool initialized with {self._pool.qsize()} connections.")
    
    def _create_connection(self):
        """Create a new SQLite connection."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
            self._connections_created += 1
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to create SQLite connection: {e}")
            raise
    
    def _get_connection(self, timeout: float = 5.0):
        """Get a connection from the pool."""
        try:
            return self._pool.get(timeout=timeout)
        except queue.Empty:
            raise Exception(f"Connection pool exhausted (timeout={timeout}s)")
    
    def _return_connection(self, conn):
        """Return a connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            logger.warning("Connection pool is full, closing connection")
            conn.close()
    
    async def close(self) -> None:
        """Close all SQLite connections."""
        def _close_all():
            closed_count = 0
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                    closed_count += 1
                except queue.Empty:
                    break
            logger.info(f"Closed {closed_count} SQLite connections")
        
        await asyncio.to_thread(_close_all)
    
    async def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query without returning results."""
        def _execute(conn):
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                conn.commit()
            finally:
                cursor.close()
        
        conn = None
        try:
            conn = await asyncio.to_thread(self._get_connection)
            await asyncio.to_thread(_execute, conn)
        finally:
            if conn:
                await asyncio.to_thread(self._return_connection, conn)
    
    async def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """Execute a query multiple times with different parameters."""
        def _execute_many(conn):
            cursor = conn.cursor()
            try:
                cursor.executemany(query, params_list)
                conn.commit()
            finally:
                cursor.close()
        
        conn = None
        try:
            conn = await asyncio.to_thread(self._get_connection)
            await asyncio.to_thread(_execute_many, conn)
        finally:
            if conn:
                await asyncio.to_thread(self._return_connection, conn)
    
    async def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Execute a query and return a single row."""
        def _fetch_one(conn):
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                return cursor.fetchone()
            finally:
                cursor.close()
        
        conn = None
        try:
            conn = await asyncio.to_thread(self._get_connection)
            return await asyncio.to_thread(_fetch_one, conn)
        finally:
            if conn:
                await asyncio.to_thread(self._return_connection, conn)
    
    async def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query and return all rows."""
        def _fetch_all(conn):
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                return cursor.fetchall()
            finally:
                cursor.close()
        
        conn = None
        try:
            conn = await asyncio.to_thread(self._get_connection)
            return await asyncio.to_thread(_fetch_all, conn)
        finally:
            if conn:
                await asyncio.to_thread(self._return_connection, conn)
    
    async def execute_returning(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query with RETURNING clause and return results."""
        def _execute_returning(conn):
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                conn.commit()
                return results
            finally:
                cursor.close()
        
        conn = None
        try:
            conn = await asyncio.to_thread(self._get_connection)
            return await asyncio.to_thread(_execute_returning, conn)
        finally:
            if conn:
                await asyncio.to_thread(self._return_connection, conn)
    
    async def begin_transaction(self) -> sqlite3.Connection:
        """Begin a transaction by getting a connection."""
        return await asyncio.to_thread(self._get_connection)
    
    async def commit_transaction(self, conn: sqlite3.Connection) -> None:
        """Commit a transaction and return connection to pool."""
        def _commit_and_return():
            conn.commit()
            self._return_connection(conn)
        await asyncio.to_thread(_commit_and_return)
    
    async def rollback_transaction(self, conn: sqlite3.Connection) -> None:
        """Rollback a transaction and return connection to pool."""
        def _rollback_and_return():
            conn.rollback()
            self._return_connection(conn)
        await asyncio.to_thread(_rollback_and_return)


class PostgreSQLBackend(DatabaseBackend):
    """PostgreSQL implementation of the database backend using psycopg3."""
    
    def __init__(self, db_url: str, min_size: int = 10, max_size: int = 50):
        self.db_url = db_url
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Optional[Any] = None  # Will be psycopg_pool.AsyncConnectionPool
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize the PostgreSQL connection pool."""
        if self._initialized:
            return
        
        try:
            import psycopg_pool
            import psycopg
            
            # Create async connection pool without opening it
            self._pool = psycopg_pool.AsyncConnectionPool(
                self.db_url,
                min_size=self.min_size,
                max_size=self.max_size,
                timeout=30,
                # Configure for high concurrency
                configure=self._configure_connection,
                open=False  # Don't open in constructor
            )
            
            # Now explicitly open the pool
            await self._pool.open()
            await self._pool.wait()
            self._initialized = True
            logger.info(f"PostgreSQL pool initialized with min={self.min_size}, max={self.max_size}")
            
        except ImportError:
            raise ImportError("psycopg3 is required for PostgreSQL support. Install with: pip install 'psycopg[binary,pool]'")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise
    
    async def _configure_connection(self, conn) -> None:
        """Configure each connection for optimal performance."""
        # Use cursor to execute configuration commands
        async with conn.cursor() as cur:
            await cur.execute("SET synchronous_commit = OFF")  # Faster writes, still durable
            await cur.execute("SET work_mem = '16MB'")
            await cur.execute("SET maintenance_work_mem = '64MB'")
        # Commit to end the transaction and leave connection in idle state
        await conn.commit()
    
    async def close(self) -> None:
        """Close the PostgreSQL connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL connection pool closed")
    
    @asynccontextmanager
    async def _get_connection(self):
        """Get a connection from the pool (context manager)."""
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialized")
        
        conn = None
        start_time = time.time()
        try:
            # More detailed pool stats logging
            stats = self._pool.get_stats()
            pool_min = stats.get('min_size', 'N/A')
            pool_max = stats.get('max_size', 'N/A')
            pool_used = stats.get('used_conns', 'N/A') # Or 'pool_size' - 'idle_conns' if 'used_conns' isn't available
            pool_idle = stats.get('idle_conns', 'N/A')
            pool_queued = stats.get('pending_requests', 'N/A') # Check psycopg_pool docs for correct key
            
            logger.debug(
                f"Requesting connection. Pool stats: min={pool_min}, max={pool_max}, "
                f"used={pool_used}, idle={pool_idle}, queued={pool_queued}"
            )
            acquire_time = -1
            async with self._pool.connection() as conn:
                acquire_time = time.time()
                time_to_acquire = acquire_time - start_time
                if time_to_acquire > 1.0:
                    logger.warning(f"Slow connection acquisition: {time_to_acquire:.2f}s")
                logger.debug(f"Connection acquired in {time_to_acquire:.3f}s")
                yield conn
        finally:
            if conn:
                hold_time = time.time() - acquire_time
                logger.debug(f"Connection returned to pool after {hold_time:.3f}s")
                
                if hold_time > CONNECTION_HOLD_THRESHOLD:
                    logger.warning(f"Connection held for {hold_time:.3f}s (threshold: {CONNECTION_HOLD_THRESHOLD}s)")
                    logger.warning("Stack trace of long-held connection:")
                    traceback.print_stack()
    
    async def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query without returning results."""
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        if "?" in query:
            query = query.replace("?", "%s")
        
        async with self._get_connection() as conn:
            # psycopg3 automatically handles parameter placeholders
            await conn.execute(query, params or ())
    
    async def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """Execute a query multiple times with different parameters."""
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        if "?" in query:
            query = query.replace("?", "%s")
        
        async with self._get_connection() as conn:
            async with conn.cursor() as cur:
                for params in params_list:
                    await cur.execute(query, params)
    
    async def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Execute a query and return a single row."""
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        if "?" in query:
            query = query.replace("?", "%s")
        
        async with self._get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or ())
                return await cur.fetchone()
    
    async def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query and return all rows."""
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        if "?" in query:
            query = query.replace("?", "%s")
        
        async with self._get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or ())
                return await cur.fetchall()
    
    async def execute_returning(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query with RETURNING clause and return results."""
        # Convert SQLite-style ? placeholders to PostgreSQL %s
        if "?" in query:
            query = query.replace("?", "%s")
        
        async with self._get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or ())
                return await cur.fetchall()
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction. Returns a connection from the pool."""
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialized")
        # In psycopg3, we need to manage the connection lifecycle manually for transactions
        conn = await self._pool.getconn()
        return conn
    
    async def commit_transaction(self, conn: Any) -> None:
        """Commit a transaction and return connection to pool."""
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialized")
        await conn.commit()
        await self._pool.putconn(conn)
    
    async def rollback_transaction(self, conn: Any) -> None:
        """Rollback a transaction and return connection to pool."""
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialized")
        await conn.rollback()
        await self._pool.putconn(conn)


def create_backend(db_type: str, **kwargs) -> DatabaseBackend:
    """
    Factory function to create a database backend.
    
    Args:
        db_type: Either 'sqlite' or 'postgresql'
        **kwargs: Backend-specific arguments
            For SQLite: db_path, pool_size, timeout
            For PostgreSQL: db_url, min_size, max_size
    
    Returns:
        DatabaseBackend instance
    """
    if db_type.lower() == 'sqlite':
        return SQLiteBackend(
            db_path=kwargs['db_path'],
            pool_size=kwargs.get('pool_size', 10),
            timeout=kwargs.get('timeout', 30)
        )
    elif db_type.lower() in ('postgresql', 'postgres', 'pg'):
        return PostgreSQLBackend(
            db_url=kwargs['db_url'],
            min_size=kwargs.get('min_size', 10),
            max_size=kwargs.get('max_size', 50)
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}") 