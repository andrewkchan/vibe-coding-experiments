# Crawler Tests

## Running Tests

### Prerequisites

#### Single-Pod Tests (Default)
Most tests use a single Redis instance and require:
- Redis running on `localhost:6379`
- Database 15 available for testing (tests will clear this database)

#### Multi-Pod Tests
Some tests (particularly in `test_pod_sharding.py`) require multiple Redis instances:
- Redis instance 0 on `localhost:6379`
- Redis instance 1 on `localhost:6380`

To set up the test environment:

```bash
# Start the test Redis instances
docker-compose -f docker-compose-test.yml up -d

# Verify Redis instances are running
redis-cli -p 6379 ping  # Should return PONG
redis-cli -p 6380 ping  # Should return PONG

# Run tests
pytest
```

### Test Configuration

All tests use Redis database 15 to avoid interfering with production data (database 0).

Tests automatically:
- Clear the test database before each test
- Clear the test database after each test
- Verify Redis connectivity and fail gracefully if Redis is not available

### Running Specific Test Suites

```bash
# Run all tests
pytest

# Run only pod sharding tests
pytest tests/test_pod_sharding.py

# Run with verbose output
pytest -v

# Run a specific test
pytest tests/test_pod_sharding.py::TestCrossPodFunctionality::test_parser_cross_pod_frontier_writes -v
```

### Test Fixtures

#### `test_config`
Provides a single-pod configuration for backward compatibility tests.

#### `multi_pod_test_config`
Provides a 2-pod configuration for testing pod-based sharding:
- Pod 0: Redis on `localhost:6379`
- Pod 1: Redis on `localhost:6380`

#### `redis_test_client`
Provides a Redis client connected to the default test instance (`localhost:6379`, database 15).

#### `multi_pod_redis_clients`
Provides Redis clients for each pod in the multi-pod setup. Requires docker-compose-test.yml to be running.

### Troubleshooting

If tests fail with connection errors:
1. Ensure Redis instances are running: `docker-compose -f docker-compose-test.yml ps`
2. Check Redis logs: `docker-compose -f docker-compose-test.yml logs redis-0 redis-1`
3. Verify ports are not in use: `lsof -i :6379` and `lsof -i :6380`
4. Restart Redis: `docker-compose -f docker-compose-test.yml restart` 