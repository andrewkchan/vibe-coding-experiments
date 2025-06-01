# PostgreSQL Migration Guide

This guide explains how to migrate your web crawler from SQLite to PostgreSQL for better concurrency and scaling to hundreds of workers.

## Why PostgreSQL?

- **Better concurrency**: PostgreSQL's MVCC allows many concurrent writers without locking
- **Scales to 500+ workers**: No "database is locked" errors
- **Same SQL queries**: Minimal code changes required (we use the same RETURNING clause)
- **Full ACID compliance**: Same durability guarantees as SQLite

## Prerequisites

1. PostgreSQL 14+ installed and running
2. A database created for the crawler
3. Python PostgreSQL driver: `pip install 'psycopg[binary,pool]'`

## Setup Instructions

### 1. Install PostgreSQL (Ubuntu/Debian)

```bash
# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### 2. Create Database and User

```bash
# Switch to postgres user
sudo -u postgres psql

# In PostgreSQL prompt:
CREATE DATABASE web_crawler;
CREATE USER crawler_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE web_crawler TO crawler_user;
\q
```

### 3. Configure PostgreSQL for High Concurrency

Edit `/etc/postgresql/14/main/postgresql.conf`:

```ini
# Connection settings
max_connections = 600              # Support 500 workers + overhead
shared_buffers = 256MB            # 25% of RAM for dedicated server
effective_cache_size = 1GB        # 50-75% of RAM
work_mem = 16MB                   # Per sort/hash operation
maintenance_work_mem = 64MB       # For vacuum, index creation

# Write performance
synchronous_commit = off          # Faster writes, still durable
wal_buffers = 16MB
checkpoint_completion_target = 0.9
```

Restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

### 4. Install Python Dependencies

```bash
# Uncomment the psycopg line in requirements.txt, then:
pip install -r requirements.txt

# Or install directly:
pip install 'psycopg[binary,pool]>=3.1'
```

## Migration Process

### Option 1: Fresh Start with PostgreSQL

Simply run the crawler with PostgreSQL parameters:

```bash
python main.py \
    --seed-file seeds.txt \
    --email your@email.com \
    --db-type postgresql \
    --db-url "postgresql://crawler_user:your_secure_password@localhost/web_crawler" \
    --max-workers 500
```

### Option 2: Migrate Existing SQLite Data

Use the provided migration script:

```bash
# Basic migration
python migrate_to_postgresql.py \
    --sqlite-db crawler_data/crawler_state.db \
    --pg-url "postgresql://crawler_user:your_secure_password@localhost/web_crawler"

# With custom batch size for large databases
python migrate_to_postgresql.py \
    --sqlite-db crawler_data/crawler_state.db \
    --pg-url "postgresql://crawler_user:your_secure_password@localhost/web_crawler" \
    --batch-size 5000
```

## Running with PostgreSQL

### Basic Usage

```bash
python main.py \
    --seed-file top-1M-domains.txt \
    --email your@email.com \
    --db-type postgresql \
    --db-url "postgresql://crawler_user:password@localhost/web_crawler" \
    --max-workers 500
```

### PostgreSQL URL Format

```
postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
```

Examples:
- Local: `postgresql://crawler_user:pass@localhost/web_crawler`
- Remote: `postgresql://crawler_user:pass@db.example.com:5432/web_crawler`
- With SSL: `postgresql://user:pass@host/db?sslmode=require`

## Performance Tuning

### System Limits

Before running with many workers, increase system limits:

```bash
# Run the setup script
chmod +x setup_system_limits.sh
./setup_system_limits.sh

# Or manually:
ulimit -n 65536  # File descriptors
ulimit -u 32768  # Max processes
```

### PostgreSQL Connection Pooling

The crawler automatically configures connection pooling based on worker count:
- Min pool size: `min(10, max_workers)`
- Max pool size: `min(max_workers + 10, 100)`

You can monitor connections:
```sql
-- Active connections
SELECT count(*) FROM pg_stat_activity WHERE datname = 'web_crawler';

-- Connection details
SELECT pid, usename, application_name, state, query_start 
FROM pg_stat_activity 
WHERE datname = 'web_crawler';
```

## Monitoring

### Crawler Status
The crawler logs database pool information:
```
Status: ... DBPool(postgresql)=min=10,max=60 ...
```

### PostgreSQL Monitoring

```sql
-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Slow queries
SELECT query, mean_exec_time, calls 
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC 
LIMIT 10;

-- Lock monitoring
SELECT * FROM pg_locks WHERE NOT granted;
```

## Troubleshooting

### Connection Refused
- Check PostgreSQL is running: `sudo systemctl status postgresql`
- Verify connection settings in `pg_hba.conf`
- Check firewall rules if remote

### Too Many Connections
- Increase `max_connections` in `postgresql.conf`
- Reduce `--max-workers`
- Check for connection leaks

### Performance Issues
- Run `ANALYZE` on tables after migration
- Check indexes exist (migration script creates them)
- Monitor slow query log
- Consider partitioning frontier table for very large crawls

## Comparison: SQLite vs PostgreSQL

| Feature | SQLite | PostgreSQL |
|---------|---------|------------|
| Max concurrent workers | ~50-100 | 500+ |
| Write concurrency | Single writer | Multiple writers (MVCC) |
| "Database locked" errors | Common at scale | None |
| Setup complexity | None | Moderate |
| Memory usage | Low | Higher (configurable) |
| Best for | Testing, small crawls | Production, large scale |

## Rollback to SQLite

If needed, you can always switch back to SQLite:

```bash
python main.py \
    --seed-file seeds.txt \
    --email your@email.com \
    --db-type sqlite \
    --max-workers 50  # Reduce workers for SQLite
```

The crawler maintains compatibility with both databases. 