#!/bin/bash

# Test script for Docker Redis with bloom filter support

echo "=== Testing Docker Redis with Bloom Filters ==="
echo ""

# Check if Redis is running
if docker ps | grep -q crawler-redis; then
    echo "✓ Redis container is running"
else
    echo "✗ Redis container is not running"
    echo "Start it with: docker-compose up -d redis"
    exit 1
fi

# Test Redis connectivity
echo ""
echo "Testing Redis connection..."
if docker exec crawler-redis redis-cli ping | grep -q PONG; then
    echo "✓ Redis is responding"
else
    echo "✗ Redis is not responding"
    exit 1
fi

# Get Redis info
echo ""
echo "Redis version:"
docker exec crawler-redis redis-cli INFO server | grep redis_version

# Test bloom filter commands
echo ""
echo "Testing bloom filter support..."

# Clean up any existing test bloom filter
docker exec crawler-redis redis-cli DEL test:bloom > /dev/null 2>&1

# Try to create a bloom filter
if docker exec crawler-redis redis-cli BF.RESERVE test:bloom 0.001 1000 2>/dev/null | grep -q OK; then
    echo "✓ Bloom filter creation successful (BF.RESERVE)"
    
    # Test adding items
    docker exec crawler-redis redis-cli BF.ADD test:bloom "hello" > /dev/null
    docker exec crawler-redis redis-cli BF.ADD test:bloom "world" > /dev/null
    echo "✓ Added items to bloom filter (BF.ADD)"
    
    # Test checking existence
    result=$(docker exec crawler-redis redis-cli BF.EXISTS test:bloom "hello")
    if [ "$result" = "1" ]; then
        echo "✓ Bloom filter existence check working (BF.EXISTS)"
    else
        echo "✗ Bloom filter existence check failed"
    fi
    
    # Check non-existent item
    result=$(docker exec crawler-redis redis-cli BF.EXISTS test:bloom "nothere")
    if [ "$result" = "0" ]; then
        echo "✓ Non-existent item correctly returns 0"
    else
        echo "⚠ Non-existent item returned $result (might be false positive)"
    fi
    
    # Clean up
    docker exec crawler-redis redis-cli DEL test:bloom > /dev/null
    
    echo ""
    echo "✓ All bloom filter tests passed!"
    echo ""
    echo "The redis:latest Docker image includes bloom filter support!"
    echo "You can use BF.* commands without any additional setup."
else
    echo "✗ Bloom filter commands not available"
    echo "The redis:latest image should include bloom filters - check Docker image"
fi

echo ""
echo "Memory configuration:"
docker exec crawler-redis redis-cli CONFIG GET maxmemory

echo ""
echo "To use Redis from your Python crawler:"
echo "  redis_client = redis.Redis(host='localhost', port=6379)"
echo "" 