# Redis Configuration for Web Crawler

## Overview

The web crawler uses Redis as its backend for storing crawl state, frontier management, and caching. To prevent accidental data loss during testing, the system uses separate Redis databases for production and testing.

## Configuration Methods

### 1. CLI Arguments (Production)

The crawler supports the following CLI arguments for Redis configuration:

```bash
--redis-host localhost      # Redis server hostname (default: localhost)
--redis-port 6379           # Redis server port (default: 6379)  
--redis-db 0                # Redis database number (default: 0)
--redis-password secret     # Redis password (optional)
```

### 2. Test Configuration

Tests **always** use database 15 to ensure production data is never touched. This is configured automatically through the `redis_test_client` fixture in `conftest.py`.

## Database Separation

| Environment | Database Number | Notes |
|-------------|----------------|-------|
| Production | 0 (default) | Can be changed via `--redis-db` argument |
| Tests | 15 | Hardcoded in `conftest.py` for safety |

## Usage Examples

### Running the Crawler in Production

```bash
# Use default settings (localhost:6379, db=0)
python -m crawler_module.crawl --seed-file seed_urls.txt --email contact@example.com --db-type redis

# Use a remote Redis server
python -m crawler_module.crawl --seed-file seed_urls.txt --email contact@example.com \
    --db-type redis \
    --redis-host redis.example.com \
    --redis-port 6380 \
    --redis-db 1
```

### Running Tests

```bash
# Tests automatically use db=15
pytest tests/

# Tests will clear db=15 before and after each test
# Production data in db=0 is never touched
```

### Running Multiple Crawlers

You can run multiple independent crawlers by using different database numbers:

```bash
# Crawler 1: News sites
python -m crawler_module.crawl --seed-file news_seeds.txt --email contact@example.com \
    --db-type redis --redis-db 1

# Crawler 2: Blog sites  
python -m crawler_module.crawl --seed-file blog_seeds.txt --email contact@example.com \
    --db-type redis --redis-db 2

# Crawler 3: Academic sites
python -m crawler_module.crawl --seed-file academic_seeds.txt --email contact@example.com \
    --db-type redis --redis-db 3
```

## Safety Features

1. **Test Fixture Protection**: The `redis_test_client` fixture in `conftest.py`:
   - Always uses db=15
   - Clears the database before and after each test
   - Verifies Redis has multiple databases enabled

2. **Explicit Configuration**: All Redis connections go through `CrawlerConfig` which:
   - Accepts Redis settings via CLI arguments
   - Logs the configuration (without passwords)
   - Validates port numbers through argparse
   - Provides defaults for all Redis settings

3. **Production/Test Separation**: Tests can never accidentally use production database because:
   - Test fixture is hardcoded to db=15
   - All test files import the centralized fixture
   - Production code reads from CLI arguments

## Troubleshooting

### Verify Redis Configuration

```python
# Check current configuration
from crawler_module.config import CrawlerConfig, parse_args
config = parse_args()  # Parse CLI args
print(config.get_redis_connection_kwargs())
```

### Check Redis has Multiple Databases

```bash
redis-cli CONFIG GET databases
# Should return "databases" "16" (or higher)
```

### Monitor Which Database is Being Used

```bash
# Monitor Redis commands in real-time
redis-cli MONITOR | grep SELECT
```

## Best Practices

1. **Always use CLI arguments** for explicit Redis configuration
2. **Use different database numbers** for different crawlers/environments
3. **Run tests before deployment** to ensure they use the test database (db=15)
4. **Monitor Redis memory usage** as each database uses separate memory
5. **Back up production databases** before major changes
6. **Document your database numbering scheme** when running multiple crawlers 