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
- **Persistence**: RDB + AOF enabled

### Snapshot Schedule
- **Every 1 minute**: if ≥10,000 keys changed
- **Every 5 minutes**: if ≥10 keys changed
- **Every 15 minutes**: if ≥1 key changed

## Automatic Recovery

### Normal Startup Behavior
When Redis starts, it automatically:
1. Checks for `appendonly.aof` (most recent data)
2. If AOF exists → loads from AOF
3. If no AOF → loads from `dump.rdb`
4. Starts with restored data

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
# Forces Redis to load from RDB by deleting the AOF files
rm appendonly.aof
rm -rf appendonlydir

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

# Verify AOF file
docker exec crawler-redis redis-check-aof /data/appendonly.aof
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
# 1. Corrupted AOF - remove it and restart
# 2. Corrupted RDB - use backup
# 3. Permission issues - check volume permissions
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