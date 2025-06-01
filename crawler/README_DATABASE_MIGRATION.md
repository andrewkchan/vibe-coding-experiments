# Database Migration Summary

This document summarizes the changes made to support PostgreSQL as a database backend for the web crawler, enabling scaling to 500+ concurrent workers.

## Overview

The crawler has been refactored to support both SQLite (for small-scale testing) and PostgreSQL (for production use with high concurrency). The migration addresses the "database is locked" errors that occurred with SQLite when running hundreds of workers.

## Key Changes

### 1. Database Abstraction Layer (`db_backends.py`)
- Created abstract `DatabaseBackend` interface
- Implemented `SQLiteBackend` with connection pooling
- Implemented `PostgreSQLBackend` using psycopg3 with async support
- Factory function `create_backend()` to instantiate the appropriate backend

### 2. Configuration Updates (`config.py`)
- Added `--db-type` parameter (sqlite/postgresql)
- Added `--db-url` parameter for PostgreSQL connection string
- Defaults to SQLite for backward compatibility

### 3. Storage Manager Changes (`storage.py`)
- Refactored to use `DatabaseBackend` instead of direct SQLite connections
- Updated schema creation to handle both database types
- Converted all database operations to async
- Fixed SQL syntax differences (e.g., `INSERT OR IGNORE` vs `ON CONFLICT DO NOTHING`)

### 4. Frontier Manager Updates (`frontier.py`)
- Removed direct SQLite connection handling
- All database operations now use the storage manager's backend
- Maintained atomic claim-and-read pattern for URL processing

### 5. Politeness Enforcer Updates (`politeness.py`)
- Removed `conn_in` parameters from all methods
- Converted to fully async database operations
- Fixed SQL parameter placeholders (`?` for SQLite, `%s` for PostgreSQL)

### 6. Orchestrator Changes (`orchestrator.py`)
- Initialize database backend based on configuration
- Improved connection pool configuration for PostgreSQL
- Added database-specific monitoring in status logs

### 7. Test Updates (`test_frontier.py`)
- Tests now use the database abstraction layer
- Maintain SQLite for fast testing
- All tests passing with the new architecture

### 8. Migration Tools
- Created `migrate_to_postgresql.py` script for data migration
- Created `setup_system_limits.sh` for OS configuration
- Created `PostgreSQL_Migration.md` with detailed setup instructions

## SQL Compatibility

The code handles SQL dialect differences between SQLite and PostgreSQL:

| Operation | SQLite | PostgreSQL |
|-----------|--------|------------|
| Upsert | `INSERT OR IGNORE/REPLACE` | `ON CONFLICT DO ...` |
| Auto-increment | `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| Parameter placeholder | `?` | `%s` |
| Integer types | `INTEGER` | `BIGINT` for timestamps |

## Performance Benefits

With PostgreSQL:
- **No more "database is locked" errors** at high concurrency
- **Scales to 500+ workers** without contention
- **MVCC** allows multiple concurrent writers
- **Built-in connection pooling** with psycopg3
- **Better monitoring** through pg_stat views

## Usage

### SQLite (default, for testing)
```bash
python main.py --seed-file seeds.txt --email user@example.com --max-workers 50
```

### PostgreSQL (for production)
```bash
python main.py \
    --seed-file seeds.txt \
    --email user@example.com \
    --db-type postgresql \
    --db-url "postgresql://user:pass@localhost/crawler_db" \
    --max-workers 500
```

## Backward Compatibility

- SQLite remains the default database
- No changes required for existing SQLite deployments
- Migration script available for moving data to PostgreSQL
- All tests continue to use SQLite for speed

## Next Steps

1. **Install PostgreSQL dependencies**: 
   ```bash
   pip install 'psycopg[binary,pool]>=3.1'
   ```

2. **Set up PostgreSQL** (see PostgreSQL_Migration.md)

3. **Migrate existing data** (optional):
   ```bash
   python migrate_to_postgresql.py \
       --sqlite-db crawler_data/crawler_state.db \
       --pg-url "postgresql://user:pass@localhost/crawler_db"
   ```

4. **Configure system limits**:
   ```bash
   ./setup_system_limits.sh
   ```

5. **Run with PostgreSQL** and enjoy 500+ concurrent workers!

## Files Changed

- `crawler_module/db_backends.py` (new)
- `crawler_module/config.py`
- `crawler_module/storage.py` 
- `crawler_module/frontier.py`
- `crawler_module/politeness.py`
- `crawler_module/orchestrator.py`
- `tests/test_frontier.py`
- `requirements.txt`
- `migrate_to_postgresql.py` (new)
- `setup_system_limits.sh` (new)
- `PostgreSQL_Migration.md` (new)
- `PLAN.MD` (updated) 