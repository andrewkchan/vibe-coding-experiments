#!/bin/bash
# Setup script to create directories with proper permissions for Docker Compose
# This prevents permission issues when Docker creates directories as root

set -e  # Exit on error

# Function to create directory with proper permissions
create_log_dir() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        echo "Creating directory: $dir"
        mkdir -p "$dir"
        # Set permissions to allow Redis container to write
        # 777 is used here because the Redis container runs as a different user
        # In production, you might want to use a more restrictive permission
        # and ensure the container user matches
        chmod 777 "$dir"
    else
        echo "Directory already exists: $dir"
        # Ensure permissions are correct even if directory exists
        chmod 777 "$dir"
    fi
}

# Determine number of pods from docker-compose file or argument
if [ -n "$1" ]; then
    NUM_PODS=$1
else
    # Default to 16 for production, or detect from docker-compose.yml
    if [ -f "docker-compose.yml" ]; then
        NUM_PODS=$(grep -c "redis-[0-9]" docker-compose.yml || echo 16)
    else
        NUM_PODS=16
    fi
fi

echo "Setting up directories for $NUM_PODS Redis pods..."

# Create log directories for each pod
for i in $(seq 0 $((NUM_PODS - 1))); do
    create_log_dir "./logs/redis/pod-$i"
done

# Create monitoring directories if using Prometheus/Grafana with bind mounts
if [ -d "./monitoring" ]; then
    create_log_dir "./monitoring/prometheus/data"
    create_log_dir "./monitoring/grafana/data"
fi

echo "Directory setup complete!"
echo ""
echo "You can now run docker-compose without sudo:"
echo "  docker-compose up -d"
echo ""
echo "Note: If you still need to use sudo for Docker, the directories now have"
echo "permissions that allow the Redis containers to write logs." 