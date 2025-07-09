# Redis Operations Playbook

## Table of Contents
1. [Data Location & Configuration](#data-location--configuration)
2. [Automatic Recovery](#automatic-recovery)
3. [Manual Recovery & Rollback](#manual-recovery--rollback)
4. [Backup Procedures](#backup-procedures)
5. [Monitoring & Health Checks](#monitoring--health-checks)
6. [Common Operations](#common-operations)
7. [Troubleshooting](#troubleshooting)

## Data Location & Configuration

### Docker Volume Location
```bash
# Find exact location of Redis data on host
docker volume inspect crawler_redis-data
```

### Redis Configuration
- **Data Directory**: `/data` (inside container)
- **Volume Name**: `crawler_redis-data`
- **Max Memory**: 24GB
- **Memory Policy**: `noeviction`
- **Persistence**: RDB only (AOF disabled for simplicity and lower memory usage)

### Snapshot Schedule
- **Every 1 minute**: if ≥10,000 keys changed
- **Every 5 minutes**: if ≥10 keys changed
- **Every 15 minutes**: if ≥1 key changed

### RDB Configuration
- **Compression**: Enabled (`rdbcompression yes`)
- **Checksum**: Enabled (`rdbchecksum yes`)
- **Stop on Error**: Enabled (`stop-writes-on-bgsave-error yes`)
- **Filename**: `dump.rdb`

## Automatic Recovery

### Normal Startup Behavior
When Redis starts, it automatically:
1. Looks for `dump.rdb` in the data directory
2. Loads the RDB file if it exists
3. Starts accepting connections with restored data

Note: With AOF disabled, recovery always uses the last successful RDB snapshot.

### Commands
```bash
# Start Redis with automatic recovery
docker-compose up -d redis

# Verify recovery
docker exec crawler-redis redis-cli INFO persistence
docker exec crawler-redis redis-cli INFO keyspace
```

## Manual Recovery & Rollback

### Method 1: Quick Rollback to Specific RDB
```bash
# 1. Stop Redis
docker-compose stop redis

# 2. Backup current state (ALWAYS DO THIS FIRST!)
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data /backup/redis_backup_$(date +%Y%m%d_%H%M%S)

# 3. Access Redis volume
docker run --rm -v crawler_redis-data:/data -it alpine sh

# 4. Inside container - replace files
cd /data
ls -la  # View available .rdb files
cp your_backup.rdb dump.rdb

# 5. Exit and restart
exit
docker-compose up -d redis
```

### Method 2: Test Recovery First
```bash
# 1. Create test directory
mkdir -p test_recovery
cp /path/to/backup.rdb test_recovery/dump.rdb

# 2. Start test Redis instance
docker run --rm -d --name redis-test \
  -v $(pwd)/test_recovery:/data \
  -p 6380:6379 \
  redis:latest redis-server --appendonly no

# 3. Verify data
docker exec redis-test redis-cli INFO keyspace
docker exec redis-test redis-cli KEYS "*" | head -20

# 4. If satisfied, stop test and apply to production
docker stop redis-test
# Then follow Method 1
```

### Method 3: Point-in-Time Recovery
```bash
# 1. Create manual snapshot
docker exec crawler-redis redis-cli BGSAVE

# 2. Wait for completion
while [ $(docker exec crawler-redis redis-cli LASTSAVE) -eq $(docker exec crawler-redis redis-cli LASTSAVE) ]; do
  echo "Waiting for snapshot..."
  sleep 1
done

# 3. Stop and examine options
docker-compose stop redis
docker run --rm -v crawler_redis-data:/data alpine ls -la /data/

# 4. Choose recovery point and apply Method 1
```

## Backup Procedures

### Manual Backup Script
Create `redis_backup.sh`:
```bash
#!/bin/bash
# Redis backup script

BACKUP_DIR="redis_backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Create snapshot
echo "Creating Redis snapshot..."
docker exec crawler-redis redis-cli BGSAVE

# Wait for completion
LAST_SAVE=$(docker exec crawler-redis redis-cli LASTSAVE)
while [ $(docker exec crawler-redis redis-cli LASTSAVE) -eq $LAST_SAVE ]; do
  echo "Waiting for snapshot completion..."
  sleep 1
done

# Copy data files
echo "Copying Redis data files..."
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data "/backup/$BACKUP_DIR"

echo "Backup completed: $BACKUP_DIR"
ls -la "$BACKUP_DIR"
```

### Automated Backup (Cron)
```bash
# Add to crontab for daily backups at 3 AM
0 3 * * * /path/to/redis_backup.sh >> /var/log/redis_backup.log 2>&1
```

## Monitoring & Health Checks

### Quick Status Commands
```bash
# Check last snapshot time
docker exec crawler-redis redis-cli LASTSAVE

# View persistence info
docker exec crawler-redis redis-cli INFO persistence

# Check memory usage
docker exec crawler-redis redis-cli INFO memory

# Monitor operations per second
docker exec crawler-redis redis-cli INFO stats

# View all databases
docker exec crawler-redis redis-cli INFO keyspace
```

### Real-time Monitoring
```bash
# Use the custom monitor script
python3 monitor_redis.py

# Watch Redis logs
docker-compose logs -f redis

# Monitor persistence events
docker-compose logs -f redis | grep -E "DB saved|Background saving|AOF"

# Real-time command monitoring
docker exec crawler-redis redis-cli MONITOR
```

### Check Data Integrity
```bash
# Verify RDB file
docker exec crawler-redis redis-check-rdb /data/dump.rdb

# Check RDB file from outside container
docker run --rm -v crawler_redis-data:/data --entrypoint redis-check-rdb redis:latest /data/dump.rdb
```

## Common Operations

### Connect to Redis CLI
```bash
# Direct connection
docker exec -it crawler-redis redis-cli

# With specific database
docker exec -it crawler-redis redis-cli -n 1
```

### Force Immediate Snapshot
```bash
# Synchronous save (blocks Redis)
docker exec crawler-redis redis-cli SAVE

# Asynchronous save (preferred)
docker exec crawler-redis redis-cli BGSAVE
```

### Clear Data (DANGER!)
```bash
# Clear current database
docker exec crawler-redis redis-cli FLUSHDB

# Clear all databases
docker exec crawler-redis redis-cli FLUSHALL
```

### Export/Import Data
```bash
# Export to RDB file
docker exec crawler-redis redis-cli --rdb /data/export.rdb

# Import from RDB (requires restart)
docker-compose stop redis
docker run --rm -v crawler_redis-data:/data -v $(pwd):/import alpine \
  cp /import/export.rdb /data/dump.rdb
docker-compose up -d redis
```

## Troubleshooting

### Redis Won't Start
```bash
# Check logs
docker-compose logs redis

# Common issues:
# 1. Corrupted RDB - use backup or recovery procedure
# 2. Permission issues - check volume permissions
# 3. Wrong Redis version - ensure using redis:latest
```

### High Memory Usage
```bash
# Check memory stats
docker exec crawler-redis redis-cli INFO memory

# Find large keys
docker exec crawler-redis redis-cli --bigkeys

# Sample keys for analysis
docker exec crawler-redis redis-cli --memkeys
```

### Slow Performance
```bash
# Check slow log
docker exec crawler-redis redis-cli SLOWLOG GET 10

# Monitor latency
docker exec crawler-redis redis-cli --latency

# Check background saves
docker exec crawler-redis redis-cli INFO persistence | grep rdb_bgsave_in_progress
```

### Recovery Validation
```bash
# After recovery, verify:
# 1. Key count matches expectation
docker exec crawler-redis redis-cli INFO keyspace

# 2. Critical keys exist
docker exec crawler-redis redis-cli EXISTS domains:ready
docker exec crawler-redis redis-cli EXISTS seen:bloom

# 3. Data structures are intact
docker exec crawler-redis redis-cli TYPE domains:ready  # Should be "zset"
docker exec crawler-redis redis-cli ZCARD domains:ready # Check size
```

### Recovering from Corrupted RDB Files

When encountering corrupted RDB files (e.g. `temp-*.rdb` from interrupted background saves), you may see errors like:
- `Unexpected EOF reading RDB file`
- `Internal error in RDB reading offset X`
- `Unknown RDB string encoding type`

#### Diagnosis
```bash
# Check if RDB file is corrupted
docker run --rm -v crawler_redis-data:/data --entrypoint redis-check-rdb redis:latest /data/dump.rdb

# Look for error messages showing:
# - Offset where corruption occurred
# - Number of keys successfully read before error
# - The key being read when failure occurred
```

#### Partial Recovery Process
If you have a corrupted RDB file but want to recover as much data as possible:

```bash
# 1. Identify the corruption point
docker run --rm -v crawler_redis-data:/data --entrypoint redis-check-rdb redis:latest /data/temp-XXXXX.rdb 2>&1 | grep -E "offset|keys read"

# 2. Find the last complete key-value pair before corruption
# Look at hex dump before the error offset to find structure boundaries
docker run --rm -v crawler_redis-data:/data alpine sh -c "cd /data && dd if=corrupted.rdb bs=1 skip=$((ERROR_OFFSET-1000)) count=1000 2>/dev/null | od -A x -t x1 -v"

# 3. Truncate file at clean boundary (use head for speed, not dd with bs=1)
docker run --rm -v crawler_redis-data:/data alpine sh -c "cd /data && head -c TRUNCATE_POSITION corrupted.rdb > recovery.rdb"

# 4. Add proper RDB EOF marker and empty checksum
docker run --rm -v crawler_redis-data:/data alpine sh -c "cd /data && printf '\xff\x00\x00\x00\x00\x00\x00\x00\x00' >> recovery.rdb"

# 5. Load with checksum validation disabled
# Note: Must use --appendonly no to prevent AOF from overriding the recovered RDB
docker run -d --name redis-recovery -v crawler_redis-data:/data -p 6379:6379 redis:latest redis-server --rdbchecksum no --appendonly no

# 6. After successful load, save with proper checksum
docker exec redis-recovery redis-cli SAVE

# 7. Verify recovery
docker exec redis-recovery redis-cli INFO keyspace
```

#### Prevention
- **Monitor background saves**: Check `rdb_bgsave_in_progress` regularly
- **Clean up temp files**: Remove old `temp-*.rdb` files after successful saves
- **Ensure adequate disk space**: Need 2x your dataset size for safe BGSAVE
- **Set up monitoring**: Alert on `rdb_last_bgsave_status:err` in INFO persistence
- **Control save timing**: Use manual BGSAVE during low-traffic periods

## Emergency Contacts & Resources

- Redis Documentation: https://redis.io/documentation
- Docker Redis Image: https://hub.docker.com/_/redis
- Bloom Filter Commands: https://redis.io/docs/stack/bloom/

## Quick Reference Card

| Operation | Command |
|-----------|---------|
| Start Redis | `docker-compose up -d redis` |
| Stop Redis | `docker-compose stop redis` |
| View logs | `docker-compose logs -f redis` |
| Connect CLI | `docker exec -it crawler-redis redis-cli` |
| Create backup | `docker exec crawler-redis redis-cli BGSAVE` |
| Check last save | `docker exec crawler-redis redis-cli LASTSAVE` |
| Monitor status | `python3 monitor_redis.py` |
| View persistence | `docker exec crawler-redis redis-cli INFO persistence` |

---
*Last Updated: Created on demand for Redis operations reference* 