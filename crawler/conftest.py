import sys
import os
import pytest
import pytest_asyncio
from pathlib import Path
from typing import AsyncIterator, Dict

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import the config after sys.path is modified
from crawler_module.config import CrawlerConfig, PodConfig

# Add Redis imports
import redis.asyncio as redis

@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory and is configured for Redis.
    Sets up a single-pod configuration for backward compatibility.
    """
    data_dir = tmp_path / "test_crawler_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    # Create a single pod config for testing (backward compatibility mode)
    test_redis_db = 15  # Use a dedicated test database
    pod_configs = [
        PodConfig(
            pod_id=0,
            redis_url=f"redis://localhost:6379/{test_redis_db}"
        )
    ]

    return CrawlerConfig(
        seed_file=seed_file,
        email="test@example.com",
        exclude_file=None,
        max_pages=10,
        max_duration=60,
        log_level="DEBUG",
        resume=False,
        seeded_urls_only=False,
        user_agent="TestCrawler/1.0 (test@example.com)",
        
        # Pod configuration (single pod for tests)
        pod_configs=pod_configs,
        fetchers_per_pod=1,  # Single fetcher process for tests
        parsers_per_pod=1,   # Single parser process for tests
        fetcher_workers=1,
        parser_workers=1,
        
        # Storage configuration
        data_dirs=[data_dir],  # List of directories
        log_dir=data_dir / "logs",
        
        # Redis configuration
        redis_db=test_redis_db,
        redis_password=None,
        redis_max_connections=10,
        
        # Legacy single-Redis support (for backward compatibility)
        redis_host="localhost",
        redis_port=6379,
        
        # Other configs with reasonable test defaults
        enable_cpu_affinity=False,
        politeness_delay_seconds=1,  # Fast for tests
        robots_cache_ttl_seconds=300,
        parse_queue_soft_limit=100,
        parse_queue_hard_limit=500,
        bloom_filter_capacity=100000,  # Smaller for tests
        bloom_filter_error_rate=0.01,
        global_coordination_redis_pod=0,
        global_metrics_update_interval=5,
        prometheus_port=8001
    )

@pytest_asyncio.fixture(scope="function")
async def redis_test_client() -> AsyncIterator[redis.Redis]:
    """
    Pytest fixture for a Redis client that uses a separate test database.
    Uses db=15 to ensure tests never touch production data (db=0).
    
    IMPORTANT: This fixture clears the test database before and after each test!
    """
    # Use a different database number for tests (default is 0)
    TEST_REDIS_DB = 15
    
    client = redis.Redis(
        host='localhost', 
        port=6379, 
        db=TEST_REDIS_DB,  # Different database for tests!
        decode_responses=True
    )
    
    # Verify we're not on the production database
    try:
        config = await client.config_get('databases')
        if not (config and int(config.get('databases', 1)) > 1):
            pytest.fail("Redis must be configured with multiple databases for safe testing.")
    except redis.ResponseError:
        # Some Redis versions might not support CONFIG GET databases
        pass
    
    # Clear test database before test
    await client.flushdb()
    
    yield client
    
    # Clean up after test
    await client.flushdb()
    await client.aclose()

@pytest.fixture(scope="function")
def multi_pod_test_config(tmp_path: Path) -> CrawlerConfig:
    """
    Pytest fixture for a multi-pod CrawlerConfig for testing.
    
    IMPORTANT: This assumes docker-compose-test.yml is running with:
    - redis-0 on localhost:6379
    - redis-1 on localhost:6380
    
    Uses database 15 for all Redis instances to avoid touching production data.
    """
    data_dirs = []
    for i in range(2):
        data_dir = tmp_path / f"test_ssd{i}"
        data_dir.mkdir(parents=True, exist_ok=True)
        data_dirs.append(data_dir)
    
    seed_file = tmp_path / "seeds.txt"
    seed_file.write_text("http://example.com/seed\nhttp://test.org/seed\n")
    
    # Create pod configs for 2 test pods
    test_redis_db = 15
    pod_configs = [
        PodConfig(
            pod_id=0,
            redis_url=f"redis://localhost:6379/{test_redis_db}"
        ),
        PodConfig(
            pod_id=1, 
            redis_url=f"redis://localhost:6380/{test_redis_db}"
        )
    ]
    
    return CrawlerConfig(
        seed_file=seed_file,
        email="test@example.com",
        exclude_file=None,
        max_pages=100,
        max_duration=60,
        log_level="DEBUG",
        resume=False,
        seeded_urls_only=False,
        user_agent="TestCrawler/1.0 (test@example.com)",
        
        # Multi-pod configuration
        pod_configs=pod_configs,
        fetchers_per_pod=2,
        parsers_per_pod=1,
        fetcher_workers=5,
        parser_workers=2,
        
        # Storage configuration
        data_dirs=data_dirs,
        log_dir=tmp_path / "logs",
        
        # Redis configuration
        redis_db=test_redis_db,
        redis_password=None,
        redis_max_connections=10,
        
        # Other configs
        enable_cpu_affinity=False,
        politeness_delay_seconds=1,
        robots_cache_ttl_seconds=300,
        parse_queue_soft_limit=100,
        parse_queue_hard_limit=500,
        bloom_filter_capacity=100000,
        bloom_filter_error_rate=0.01,
        global_coordination_redis_pod=0,
        global_metrics_update_interval=5,
        prometheus_port=8001
    )

@pytest_asyncio.fixture(scope="function")
async def multi_pod_redis_clients() -> AsyncIterator[Dict[int, redis.Redis]]:
    """
    Pytest fixture for Redis clients connected to multiple pod instances.
    
    IMPORTANT: Requires docker-compose-test.yml to be running with:
    - redis-0 on localhost:6379  
    - redis-1 on localhost:6380
    
    Uses database 15 for testing and clears it before/after tests.
    """
    TEST_REDIS_DB = 15
    
    clients = {
        0: redis.Redis(host='localhost', port=6379, db=TEST_REDIS_DB, decode_responses=True),
        1: redis.Redis(host='localhost', port=6380, db=TEST_REDIS_DB, decode_responses=True)
    }
    
    # Clear test databases before test
    for pod_id, client in clients.items():
        try:
            await client.flushdb()
        except redis.ConnectionError:
            pytest.fail(f"Redis instance for pod {pod_id} not available. Please run: docker-compose -f docker-compose-test.yml up -d")
    
    yield clients
    
    # Clean up after test
    for client in clients.values():
        await client.flushdb()
        await client.aclose() 