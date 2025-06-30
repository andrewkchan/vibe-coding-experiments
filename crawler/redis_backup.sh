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