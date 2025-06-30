# Finding Redis server data snapshots
_Exported on 6/29/2025 at 18:52:23 PDT from Cursor (1.0.0)_

---

**User**

where is the data (snapshots) for the redis-server located? I want to see how often snapshots are being taken and understand the current level of fault tolerance

---

**Cursor**

Based on your Docker Compose configuration, I can see the Redis data location and snapshot configuration. Let me provide you with the details and then help you check the current state.

## Redis Data Location and Configuration

From your `docker-compose.yml`, the Redis data is stored in:
- **Docker Volume**: `redis-data` (mapped to `/data` inside the container)
- **Volume Driver**: Local storage on the host machine

## Snapshot Configuration

Your Redis server is configured with these snapshot rules:
- **Every 15 minutes** (`--save 900 1`): If at least 1 key changed
- **Every 5 minutes** (`--save 300 10`): If at least 10 keys changed  
- **Every 1 minute** (`--save 60 10000`): If at least 10,000 keys changed

Additionally, you have **AOF (Append Only File)** enabled (`--appendonly yes`) for maximum fault tolerance.

## Let me help you check the current state:

Perfect! I found more details about your Redis setup. Based on your configuration and the monitoring tools available, here's the complete picture:

## 📍 Data Location

Your Redis data is stored in a Docker volume `redis-data`. To find the exact location on your host:

```bash
docker volume inspect crawler_redis-data
```

## 📊 Current Snapshot Configuration & Fault Tolerance

**Snapshot Frequency:**
- **Every 1 minute**: if ≥10,000 keys changed
- **Every 5 minutes**: if ≥10 keys changed  
- **Every 15 minutes**: if ≥1 key changed

**Fault Tolerance Level: EXCELLENT** ⭐⭐⭐
- **RDB Snapshots**: Point-in-time backups (as configured above)
- **AOF (Append Only File)**: Real-time write logging enabled
- **Memory Policy**: `noeviction` (won't lose data when memory is full)
- **Max Memory**: 24GB allocated

## 🔍 Check Current Status

I noticed you have a Redis monitoring script available. Run these commands to check your current state:

```bash
# Check Redis status and last snapshot time
docker exec crawler-redis redis-cli LASTSAVE

# Check Redis info including persistence stats
docker exec crawler-redis redis-cli INFO persistence

# Use your custom monitoring script
python3 crawler/monitor_redis.py

# Check Redis logs for snapshot activity
docker-compose logs redis | grep -E "(RDB|AOF|save)"
```

## 📈 Monitor Snapshot Activity

To see when snapshots are being taken:

```bash
# Monitor Redis logs in real-time for persistence events
docker-compose logs -f redis | grep -E "DB saved|Background saving|AOF"

# Check detailed persistence info
docker exec crawler-redis redis-cli INFO persistence
```

The `INFO persistence` command will show you:
- `rdb_last_save_time`: Unix timestamp of last successful save
- `rdb_changes_since_last_save`: Number of changes since last save
- `aof_enabled`: Whether AOF is active
- `aof_last_rewrite_time_sec`: Last AOF rewrite duration

Your setup provides **maximum fault tolerance** with both snapshot-based (RDB) and log-based (AOF) persistence enabled!

```shellscript
docker volume inspect crawler_redis-data
```

```shellscript
# Check Redis status and last snapshot time
docker exec crawler-redis redis-cli LASTSAVE

# Check Redis info including persistence stats
docker exec crawler-redis redis-cli INFO persistence

# Use your custom monitoring script
python3 crawler/monitor_redis.py

# Check Redis logs for snapshot activity
docker-compose logs redis | grep -E "(RDB|AOF|save)"
```

```shellscript
# Monitor Redis logs in real-time for persistence events
docker-compose logs -f redis | grep -E "DB saved|Background saving|AOF"

# Check detailed persistence info
docker exec crawler-redis redis-cli INFO persistence
```

---

**User**

Ok, I see the data directory and it has some .rdb files. Before I ask about those, how am I supposed to do operations more generally on redis, if the system is rebooted and I start the redis service back up, will it automatically restore from the latest checkpoint?

And what if I want to make it restore from an earlier checkpoint (for example to roll back a set of unintended changes)?

---

**Cursor**

Great questions! Let me explain Redis recovery operations for your setup.

## 🔄 Automatic Recovery (Normal Startup)

**Yes, Redis will automatically restore from the latest checkpoint** when you restart the service. Here's how it works:

### Recovery Priority Order:
1. **AOF file first** (if exists) - since you have `--appendonly yes`
2. **RDB file** (if no AOF or AOF disabled)

When you run:
```bash
docker-compose up redis
```

Redis will automatically:
1. Look for `appendonly.aof` in `/data` (inside container)
2. If AOF exists, load from that (most recent data)
3. If no AOF, load from the latest `dump.rdb` file
4. Start normally with restored data

## 🔙 Manual Recovery from Earlier Checkpoint

To restore from a specific RDB file (rollback scenario):

### Method 1: Replace RDB and Disable AOF Temporarily

```bash
# 1. Stop Redis
docker-compose stop redis

# 2. Access the Redis data volume
docker run --rm -v crawler_redis-data:/data -it ubuntu:20.04 bash

# Inside the container, you'll see your .rdb files:
ls -la /data/
# You should see: dump.rdb, appendonly.aof, and possibly timestamped .rdb files

# 3. Replace current dump.rdb with your desired backup
cp dump_2024_01_15.rdb dump.rdb  # example

# 4. Remove AOF to force RDB loading
rm appendonly.aof

# 5. Exit container
exit
```

### Method 2: Restore to New Container (Safer)

```bash
# 1. Create backup of current volume
docker run --rm -v crawler_redis-data:/from -v $(pwd)/redis_backup:/to alpine cp -r /from /to

# 2. Stop current Redis
docker-compose stop redis

# 3. Start temporary Redis with specific RDB file
docker run --rm -d --name temp-redis \
  -v $(pwd)/path/to/your/backup.rdb:/data/dump.rdb \
  redis:latest redis-server --appendonly no

# 4. Verify data is correct
docker exec temp-redis redis-cli INFO keyspace

# 5. If good, save to new RDB and replace your volume
```

## 🛠️ Practical Commands for Your Setup

```shellscript
docker-compose up redis
```

```shellscript
# 1. Stop Redis
docker-compose stop redis

# 2. Access the Redis data volume
docker run --rm -v crawler_redis-data:/data -it ubuntu:20.04 bash

# Inside the container, you'll see your .rdb files:
ls -la /data/
# You should see: dump.rdb, appendonly.aof, and possibly timestamped .rdb files

# 3. Replace current dump.rdb with your desired backup
cp dump_2024_01_15.rdb dump.rdb  # example

# 4. Remove AOF to force RDB loading
rm appendonly.aof

# 5. Exit container
exit
```

```shellscript
# 1. Create backup of current volume
docker run --rm -v crawler_redis-data:/from -v $(pwd)/redis_backup:/to alpine cp -r /from /to

# 2. Stop current Redis
docker-compose stop redis

# 3. Start temporary Redis with specific RDB file
docker run --rm -d --name temp-redis \
  -v $(pwd)/path/to/your/backup.rdb:/data/dump.rdb \
  redis:latest redis-server --appendonly no

# 4. Verify data is correct
docker exec temp-redis redis-cli INFO keyspace

# 5. If good, save to new RDB and replace your volume
```

Perfect! Now I can give you a complete Redis operations guide based on your setup.

## 🔄 Redis Operations Guide

### **Automatic Recovery (Normal Operation)**

Yes! Redis automatically restores from the latest checkpoint. Here's what happens:

```bash
# When you restart Redis
docker-compose up redis

# Redis automatically:
# 1. Loads AOF file (appendonly.aof) if it exists - MOST RECENT DATA
# 2. Falls back to RDB file (dump.rdb) if no AOF
# 3. Starts with restored data
```

Your setup has **both AOF and RDB** enabled, so you get maximum safety.

### **Quick Status Check Commands**

```bash
# Check what data Redis will load on next startup
docker exec crawler-redis redis-cli INFO persistence

# See when last snapshot was taken
docker exec crawler-redis redis-cli LASTSAVE

# Check current memory usage
docker exec crawler-redis redis-cli INFO memory

# Inspect your data volume
docker volume inspect crawler_redis-data
```

## 🔙 Manual Recovery/Rollback Procedures

### **Method 1: Rollback to Specific RDB File (Simplest)**

```bash
# 1. Stop Redis
docker-compose stop redis

# 2. Backup current state first (safety!)
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data /backup/redis_backup_$(date +%Y%m%d_%H%M%S)

# 3. Access the Redis data volume
docker run --rm -v crawler_redis-data:/data -it alpine sh

# Inside the container:
cd /data
ls -la  # You'll see your .rdb files and appendonly.aof

# 4. Replace current files with your desired RDB backup
cp your_desired_backup.rdb dump.rdb
rm appendonly.aof  # Remove AOF so Redis loads from RDB

# 5. Exit and restart Redis
exit
docker-compose up redis
```

### **Method 2: Point-in-Time Recovery (More Control)**

```bash
# 1. Create manual snapshot first
docker exec crawler-redis redis-cli BGSAVE

# 2. Stop Redis and examine your options
docker-compose stop redis
docker run --rm -v crawler_redis-data:/data alpine ls -la /data/

# You'll see files like:
# - dump.rdb (latest snapshot)
# - appendonly.aof (transaction log)
# - possibly timestamped .rdb files

# 3. Choose recovery point and follow Method 1 steps
```

### **Method 3: Test Recovery Without Affecting Production**

```bash
# 1. Create test environment with specific RDB file
mkdir test_recovery
cp /path/to/your/backup.rdb test_recovery/dump.rdb

# 2. Start temporary Redis with that data
docker run --rm -d --name redis-test \
  -v $(pwd)/test_recovery:/data \
  -p 6380:6379 \
  redis:latest redis-server --appendonly no

# 3. Connect and verify data
docker exec redis-test redis-cli -p 6379 INFO keyspace
docker exec redis-test redis-cli -p 6379 KEYS "*" | head -10

# 4. If satisfied, apply to production (follow Method 1)
docker stop redis-test
```

## 🛡️ Recovery Best Practices

### **Create Recovery Scripts**

Here's a backup script you can create:

```bash
#!/bin/bash
# save as: redis_backup.sh

BACKUP_DIR="redis_backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Create snapshot
echo "Creating Redis snapshot..."
docker exec crawler-redis redis-cli BGSAVE

# Wait for completion
while [ $(docker exec crawler-redis redis-cli LASTSAVE) = $(docker exec crawler-redis redis-cli LASTSAVE) ]; do
  sleep 1
done

# Copy data files
echo "Copying Redis data files..."
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data "/backup/$BACKUP_DIR"

echo "Backup completed: $BACKUP_DIR"
ls -la "$BACKUP_DIR"
```

### **Monitoring Your Recovery Setup**

Use your existing monitoring script:

```bash
# Monitor Redis persistence status
python3 monitor_redis.py

# Check logs for persistence events
docker-compose logs redis | grep -E "DB saved|Background saving|AOF"
```

## 📋 Summary

**Normal Operations**: ✅ Automatic recovery on restart  
**Your Recovery Options**: 
- RDB snapshots every 1-15 minutes
- AOF logging for real-time protection  
- Manual `BGSAVE` for on-demand snapshots
- Volume-level backups for full protection

**To Rollback**: Stop Redis → Replace `dump.rdb` → Remove `appendonly.aof` → Restart

Your setup is **extremely fault-tolerant** with both RDB and AOF enabled!

```shellscript
# When you restart Redis
docker-compose up redis

# Redis automatically:
# 1. Loads AOF file (appendonly.aof) if it exists - MOST RECENT DATA
# 2. Falls back to RDB file (dump.rdb) if no AOF
# 3. Starts with restored data
```

```shellscript
# Check what data Redis will load on next startup
docker exec crawler-redis redis-cli INFO persistence

# See when last snapshot was taken
docker exec crawler-redis redis-cli LASTSAVE

# Check current memory usage
docker exec crawler-redis redis-cli INFO memory

# Inspect your data volume
docker volume inspect crawler_redis-data
```

```shellscript
# 1. Stop Redis
docker-compose stop redis

# 2. Backup current state first (safety!)
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data /backup/redis_backup_$(date +%Y%m%d_%H%M%S)

# 3. Access the Redis data volume
docker run --rm -v crawler_redis-data:/data -it alpine sh

# Inside the container:
cd /data
ls -la  # You'll see your .rdb files and appendonly.aof

# 4. Replace current files with your desired RDB backup
cp your_desired_backup.rdb dump.rdb
rm appendonly.aof  # Remove AOF so Redis loads from RDB

# 5. Exit and restart Redis
exit
docker-compose up redis
```

```shellscript
# When you restart Redis
docker-compose up redis

# Redis automatically:
# 1. Loads AOF file (appendonly.aof) if it exists - MOST RECENT DATA
# 2. Falls back to RDB file (dump.rdb) if no AOF
# 3. Starts with restored data
```

```shellscript
# Check what data Redis will load on next startup
docker exec crawler-redis redis-cli INFO persistence

# See when last snapshot was taken
docker exec crawler-redis redis-cli LASTSAVE

# Check current memory usage
docker exec crawler-redis redis-cli INFO memory

# Inspect your data volume
docker volume inspect crawler_redis-data
```

```shellscript
# 1. Stop Redis
docker-compose stop redis

# 2. Backup current state first (safety!)
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data /backup/redis_backup_$(date +%Y%m%d_%H%M%S)

# 3. Access the Redis data volume
docker run --rm -v crawler_redis-data:/data -it alpine sh

# Inside the container:
cd /data
ls -la  # You'll see your .rdb files and appendonly.aof

# 4. Replace current files with your desired RDB backup
cp your_desired_backup.rdb dump.rdb
rm appendonly.aof  # Remove AOF so Redis loads from RDB

# 5. Exit and restart Redis
exit
docker-compose up redis
```

```shellscript
# 1. Create manual snapshot first
docker exec crawler-redis redis-cli BGSAVE

# 2. Stop Redis and examine your options
docker-compose stop redis
docker run --rm -v crawler_redis-data:/data alpine ls -la /data/

# You'll see files like:
# - dump.rdb (latest snapshot)
# - appendonly.aof (transaction log)
# - possibly timestamped .rdb files

# 3. Choose recovery point and follow Method 1 steps
```

```shellscript
# 1. Create test environment with specific RDB file
mkdir test_recovery
cp /path/to/your/backup.rdb test_recovery/dump.rdb

# 2. Start temporary Redis with that data
docker run --rm -d --name redis-test \
  -v $(pwd)/test_recovery:/data \
  -p 6380:6379 \
  redis:latest redis-server --appendonly no

# 3. Connect and verify data
docker exec redis-test redis-cli -p 6379 INFO keyspace
docker exec redis-test redis-cli -p 6379 KEYS "*" | head -10

# 4. If satisfied, apply to production (follow Method 1)
docker stop redis-test
```

```shellscript
#!/bin/bash
# save as: redis_backup.sh

BACKUP_DIR="redis_backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Create snapshot
echo "Creating Redis snapshot..."
docker exec crawler-redis redis-cli BGSAVE

# Wait for completion
while [ $(docker exec crawler-redis redis-cli LASTSAVE) = $(docker exec crawler-redis redis-cli LASTSAVE) ]; do
  sleep 1
done

# Copy data files
echo "Copying Redis data files..."
docker run --rm -v crawler_redis-data:/data -v $(pwd):/backup alpine \
  cp -r /data "/backup/$BACKUP_DIR"

echo "Backup completed: $BACKUP_DIR"
ls -la "$BACKUP_DIR"
```

```shellscript
# Monitor Redis persistence status
python3 monitor_redis.py

# Check logs for persistence events
docker-compose logs redis | grep -E "DB saved|Background saving|AOF"
```

---

**User**

Can you add these operational instructions in a playbook in crawler/REDIS_PLAYBOOK.md?

---

**Cursor**

I'll create a comprehensive Redis operations playbook for you with all the instructions we've discussed.

```markdown
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
rm appendonly.aof  # Forces Redis to load from RDB

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
```

Perfect! I've created the `REDIS_PLAYBOOK.md` file with all the operational instructions. Let me also create the backup script that's referenced in the playbook:

```shellscript
#!/bin/bash
# Redis backup script for crawler

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="crawler-redis"
VOLUME_NAME="crawler_redis-data"
BACKUP_BASE_DIR="${BACKUP_BASE_DIR:-redis_backups}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BACKUP_BASE_DIR/$TIMESTAMP"

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo -e "${GREEN}=== Redis Backup Script ===${NC}"
echo "Backup directory: $BACKUP_DIR"
echo ""

# Check if Redis is running
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo -e "${RED}Error: Redis container '$CONTAINER_NAME' is not running${NC}"
    exit 1
fi

# Get initial snapshot time
echo -e "${YELLOW}Creating Redis snapshot...${NC}"
INITIAL_SAVE=$(docker exec $CONTAINER_NAME redis-cli LASTSAVE)

# Trigger background save
docker exec $CONTAINER_NAME redis-cli BGSAVE > /dev/null

# Wait for snapshot to complete
while true; do
    CURRENT_SAVE=$(docker exec $CONTAINER_NAME redis-cli LASTSAVE)
    if [ "$CURRENT_SAVE" != "$INITIAL_SAVE" ]; then
        echo -e "${GREEN}Snapshot completed!${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# Copy data files from volume
echo -e "${YELLOW}Copying Redis data files...${NC}"
docker run --rm -v $VOLUME_NAME:/data -v $(pwd):/backup alpine \
    sh -c "cp -r /data/* /backup/$BACKUP_DIR/" 2>/dev/null

# Get Redis info for documentation
echo -e "${YELLOW}Gathering Redis info...${NC}"
docker exec $CONTAINER_NAME redis-cli INFO > "$BACKUP_DIR/redis_info.txt"
docker exec $CONTAINER_NAME redis-cli CONFIG GET "save" > "$BACKUP_DIR/redis_save_config.txt"
docker exec $CONTAINER_NAME redis-cli INFO keyspace > "$BACKUP_DIR/redis_keyspace.txt"

# Create backup metadata
cat > "$BACKUP_DIR/backup_metadata.txt" << EOF
Backup Timestamp: $TIMESTAMP
Redis Container: $CONTAINER_NAME
Volume Name: $VOLUME_NAME
Host: $(hostname)
User: $(whoami)
EOF

# List backup contents
echo -e "${GREEN}Backup completed successfully!${NC}"
echo "Contents:"
ls -la "$BACKUP_DIR"

# Calculate backup size
BACKUP_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
echo -e "\nTotal backup size: ${GREEN}$BACKUP_SIZE${NC}"

# Optional: Remove old backups (keep last 7 days)
if [ "${CLEANUP_OLD_BACKUPS}" = "true" ]; then
    echo -e "\n${YELLOW}Cleaning up old backups...${NC}"
    find "$BACKUP_BASE_DIR" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null
    echo "Removed backups older than 7 days"
fi

echo -e "\n${GREEN}Backup location: $BACKUP_DIR${NC}"
```

## ✅ Redis Operations Playbook Created!

I've successfully created two files for you:

### 1. **`crawler/REDIS_PLAYBOOK.md`** - Complete operational guide including:
- **Data Location & Configuration**: Where Redis stores data, snapshot schedules
- **Automatic Recovery**: How Redis restores data on startup
- **Manual Recovery & Rollback**: Three methods for restoring from backups
- **Backup Procedures**: Manual and automated backup scripts
- **Monitoring & Health Checks**: Commands to check Redis status
- **Common Operations**: Daily operational commands
- **Troubleshooting**: Solutions for common issues
- **Quick Reference Card**: One-line commands for common tasks

### 2. **`crawler/redis_backup.sh`** - Automated backup script that:
- Creates timestamped backups
- Waits for snapshot completion
- Copies all Redis data files
- Saves Redis configuration and metadata
- Shows backup size and contents
- Optionally cleans up old backups (when `CLEANUP_OLD_BACKUPS=true`)

## 📋 Quick Usage Examples:

```bash
# View the playbook
less crawler/REDIS_PLAYBOOK.md

# Run a backup
./crawler/redis_backup.sh

# Run backup with cleanup of old backups
CLEANUP_OLD_BACKUPS=true ./crawler/redis_backup.sh

# Check current Redis status
docker exec crawler-redis redis-cli INFO persistence
```

The playbook contains everything you need to manage Redis operations, from routine backups to emergency recovery procedures. Your Redis setup with both RDB snapshots and AOF provides excellent fault tolerance!

```shellscript
# View the playbook
less crawler/REDIS_PLAYBOOK.md

# Run a backup
./crawler/redis_backup.sh

# Run backup with cleanup of old backups
CLEANUP_OLD_BACKUPS=true ./crawler/redis_backup.sh

# Check current Redis status
docker exec crawler-redis redis-cli INFO persistence
```

