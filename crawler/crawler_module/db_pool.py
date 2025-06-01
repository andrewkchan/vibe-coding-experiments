import sqlite3
import threading
from queue import Queue, Empty, Full
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str | Path, pool_size: int = 10, timeout: int = 30, wal_mode: bool = True):
        self.db_path = str(db_path) # Ensure db_path is a string for sqlite3.connect
        self.timeout = timeout
        self.pool_size = pool_size
        self.wal_mode = wal_mode
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock() # To protect access to pool creation logic if needed, though Queue is thread-safe
        self._connections_created = 0

        # Pre-fill the pool
        for _ in range(pool_size):
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Exception as e:
                logger.error(f"Error creating initial connection for pool: {e}")
                # Depending on strictness, could raise here or allow partial pool
        logger.info(f"SQLiteConnectionPool initialized for {self.db_path} with {self._pool.qsize()} connections.")

    def _create_connection(self):
        try:
            # Connections will be used by different threads via asyncio.to_thread,
            # so check_same_thread=False is essential.
            conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
            if self.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL") # WAL is durable, NORMAL is faster
                # Additional optimizations for high concurrency
                conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
                conn.execute("PRAGMA temp_store=MEMORY")  # Use memory for temp tables
                conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
            self._connections_created += 1
            # logger.debug(f"Created new SQLite connection. Total created: {self._connections_created}")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Failed to create SQLite connection for {self.db_path}: {e}")
            raise

    def get_connection(self, block: bool = True, timeout: float | None = None) -> sqlite3.Connection:
        """Get a connection from the pool."""
        try:
            # Timeout for getting from queue can be shorter than connection timeout
            effective_timeout = timeout if timeout is not None else self.timeout / 2 
            return self._pool.get(block=block, timeout=effective_timeout)
        except Empty:
            # This can happen if pool is exhausted and block is False or timeout occurs
            logger.warning(f"Connection pool for {self.db_path} exhausted or timed out. Trying to create a temporary connection.")
            # Fallback: create a new one if pool is empty and we must proceed, but don't add it to the pool.
            # This is a safety net, but ideally, the pool size should be tuned.
            # However, for short-lived operations, this might be acceptable if rare.
            # For now, let's stick to the pool size. If Empty is raised, the caller should handle.
            # Re-raising for clarity that pool was empty or timed out.
            raise Empty(f"No connection available from pool for {self.db_path} within timeout.")


    def return_connection(self, conn: sqlite3.Connection | None):
        """Return a connection to the pool."""
        if conn is None:
            return
        try:
            # Test if connection is still alive (e.g., by executing a simple pragma)
            # This can be problematic as it adds overhead. 
            # A more robust pool might try to execute a rollback to ensure it's in a clean state.
            # For simplicity, we'll assume if it was working, it's still good.
            # A more advanced pool might have a "test on return" or "test on borrow" flag.
            conn.execute("PRAGMA user_version;") # A lightweight command to check if connection is alive
            self._pool.put_nowait(conn) # Use put_nowait to avoid blocking if pool is somehow overfilled (shouldn't happen with proper use)
        except sqlite3.Error as e:
            logger.warning(f"Returned connection for {self.db_path} is dead or error: {e}. Discarding.")
            try:
                conn.close() # Close the dead connection
            except:
                pass # Ignore errors during close of a dead connection
            # Optionally, replace the dead connection in the pool
            try:
                new_conn = self._create_connection()
                self._pool.put_nowait(new_conn) # Try to keep the pool full
                logger.info(f"Replaced dead connection in pool for {self.db_path}")
            except Full:
                logger.warning(f"Pool for {self.db_path} is full, cannot replace dead connection immediately.")
            except Exception as ex:
                logger.error(f"Error replacing dead connection for {self.db_path}: {ex}")

        except Full:
            # This should ideally not happen if get_connection/return_connection are used correctly.
            # It means we are returning more connections than were taken out or the pool size is mismanaged.
            logger.error(f"Attempted to return connection to a full pool for {self.db_path}. Closing connection instead.")
            try:
                conn.close()
            except:
                pass


    def close_all(self):
        """Close all connections in the pool."""
        logger.info(f"Closing all connections in pool for {self.db_path}...")
        closed_count = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed_count += 1
            except Empty:
                break # Pool is empty
            except Exception as e:
                logger.error(f"Error closing a connection from pool for {self.db_path}: {e}")
        logger.info(f"Closed {closed_count} connections from pool for {self.db_path}. Pool size: {self._pool.qsize()}")

    def __enter__(self):
        # Context manager to get a connection
        self.conn = self.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Context manager to return the connection
        self.return_connection(self.conn)
        self.conn = None # Clear the connection