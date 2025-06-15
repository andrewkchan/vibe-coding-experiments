# Redis Docker Setup for Hybrid Crawler

## The Simplest Solution: Docker

Thanks to your discovery, we found that the official `redis:latest` Docker image includes bloom filter support out of the box! This is by far the simplest way to get Redis with bloom filters running.

## Quick Start

1. **Start Redis with Docker Compose:**
   ```bash
   cd crawler
   docker-compose up -d redis
   ```

2. **Verify it's working:**
   ```bash
   chmod +x test_docker_redis.sh
   ./test_docker_redis.sh
   ```

3. **Test bloom filters manually:**
   ```bash
   # Connect to Redis
   docker exec -it crawler-redis redis-cli
   
   # Create a bloom filter
   127.0.0.1:6379> BF.RESERVE test:bloom 0.001 1000
   OK
   
   # Add and check items
   127.0.0.1:6379> BF.ADD test:bloom "hello"
   (integer) 1
   127.0.0.1:6379> BF.EXISTS test:bloom "hello"
   (integer) 1
   ```

## Configuration

The Docker Compose setup includes:
- **Memory limit**: 12GB (plenty of headroom for 4.5GB expected usage) via `maxmemory 12gb`
- **Persistence**: Both RDB snapshots and AOF enabled via `appendonly yes`
- **Data volume**: Persists data between container restarts

## Why This Approach?

1. **Zero complexity**: No building from source, no modules to install
2. **Bloom filters included**: BF.* commands work immediately
3. **Well-tested**: Official Docker image used by millions
4. **Easy updates**: Just pull the latest image
5. **Portable**: Same setup works on any system with Docker

## Using from Python

Your crawler code remains the same:

```python
import redis

# Connect to Docker Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Use bloom filters
r.execute_command('BF.RESERVE', 'seen:bloom', 0.001, 160000000)
r.execute_command('BF.ADD', 'seen:bloom', 'https://example.com')
exists = r.execute_command('BF.EXISTS', 'seen:bloom', 'https://example.com')
```

## Memory Usage

With 160M URLs and 0.1% false positive rate:
- Bloom filter: ~2GB
- Other structures: ~2.5GB
- Total: ~4.5GB (well within 12GB limit)

## Management Commands

```bash
# Start Redis
docker-compose up -d redis

# Stop Redis
docker-compose stop redis

# View logs
docker-compose logs -f redis

# Connect to Redis CLI
docker exec -it crawler-redis redis-cli

# Check memory usage
docker exec crawler-redis redis-cli INFO memory

# Backup data
docker exec crawler-redis redis-cli BGSAVE
```

## Data Persistence

Data is stored in a Docker volume and persists across container restarts. The volume is located at:
- Docker managed: `docker volume inspect crawler_redis-data`
- Snapshots: Every 15 min (1 change), 5 min (10 changes), 1 min (10k changes)
- AOF: Synced every second

## Comparison with Other Approaches

| Approach | Complexity | Setup Time | Bloom Filters |
|----------|------------|------------|---------------|
| Docker redis:latest | ⭐ Simplest | 1 minute | ✅ Included |
| Redis from APT | ⭐⭐ Medium | 5 minutes | ❌ Module needed |
| Build from source | ⭐⭐⭐ Complex | 30 minutes | ❌ Module needed |

## Next Steps

You're ready to proceed with Phase 2 of your crawler implementation! The Redis infrastructure is fully set up with bloom filter support. 