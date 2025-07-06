import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
import shutil
import logging
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock
import redis.asyncio as redis
from collections import defaultdict
from typing import List, Dict, Tuple

from crawler_module.frontier import FrontierManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer
from crawler_module.orchestrator import CrawlerOrchestrator

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
    data_dir: Path
    seed_file: Path
    cpu_alloc_start: int = 0
    email: str = "frontier_test@example.com"
    exclude_file: Path | None = None
    fetcher_workers: int = 1
    parser_workers: int = 1
    num_fetcher_processes: int = 1
    num_parser_processes: int = 1
    max_pages: int | None = None
    max_duration: int | None = None
    log_level: str = "DEBUG"
    resume: bool = False
    user_agent: str = "FrontierTestCrawler/1.0"
    seeded_urls_only: bool = False
    # Redis settings
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 15
    redis_password: str | None = None

# Note: The redis_client fixture is now imported from conftest.py as redis_test_client
# This ensures we use db=15 for tests and never touch production data (db=0)

@pytest_asyncio.fixture
async def temp_test_frontier_dir(tmp_path: Path) -> Path:
    test_data_dir = tmp_path / "test_crawler_data_frontier_redis"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for frontier: {test_data_dir}")
    return test_data_dir

@pytest_asyncio.fixture
async def frontier_test_config_obj(temp_test_frontier_dir: Path) -> FrontierTestConfig:
    """Provides the FrontierTestConfig object, seeds file created here."""
    seed_file_path = temp_test_frontier_dir / "test_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("http://example.com/seed1\n")
        sf.write("http://example.org/seed2\n")
        sf.write("http://example.com/seed1\n")  # Duplicate to test seen
    return FrontierTestConfig(data_dir=temp_test_frontier_dir, seed_file=seed_file_path)

@pytest_asyncio.fixture
async def actual_config_for_frontier(frontier_test_config_obj: FrontierTestConfig) -> CrawlerConfig:
    """Provides the actual CrawlerConfig based on FrontierTestConfig."""
    return CrawlerConfig(**vars(frontier_test_config_obj))

@pytest_asyncio.fixture
def mock_politeness_enforcer_for_frontier() -> MagicMock:
    """Provides a mocked PolitenessEnforcer for FrontierManager tests."""
    mock_pe = AsyncMock(spec=PolitenessEnforcer)
    
    # Default mock behaviors for permissive testing
    mock_pe.is_url_allowed = AsyncMock(return_value=True)
    mock_pe.can_fetch_domain_now = AsyncMock(return_value=True)
    mock_pe.record_domain_fetch_attempt = AsyncMock(return_value=int(time.time()) + 1)  # Return future time
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0)
    mock_pe._load_manual_exclusions = AsyncMock()
    mock_pe.initialize = AsyncMock()  # Add mock for initialize method
    return mock_pe

@pytest_asyncio.fixture
async def frontier_manager(
    actual_config_for_frontier: CrawlerConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
) -> FrontierManager:
    fm = FrontierManager(
        config=actual_config_for_frontier,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    # Patch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock(side_effect=fm._mark_domains_as_seeded_batch)
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch)
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(frontier_manager: FrontierManager):
    logger.info("Testing Redis Frontier Initialization (New Crawl)")
    
    await frontier_manager.initialize_frontier()
    
    # Assert that the batch methods were called
    frontier_manager._mark_domains_as_seeded_batch.assert_called_once()
    frontier_manager.add_urls_batch.assert_called_once()
    
    # Check the domains that were marked as seeded
    seeded_domains_call = frontier_manager._mark_domains_as_seeded_batch.call_args[0][0]
    assert set(seeded_domains_call) == {"example.com", "example.org"}
    
    # Check the URLs that were added to the frontier
    urls_added_call = frontier_manager.add_urls_batch.call_args[0][0]
    assert set(urls_added_call) == {"http://example.com/seed1", "http://example.org/seed2"}
    
    logger.info("Redis frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_frontier_initialization_with_bare_domains_bug(
    temp_test_frontier_dir: Path,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test when seed file contains bare domains instead of proper URLs."""
    
    # Create frontier from seed file with bare domains (no http://)
    seed_file_path = temp_test_frontier_dir / "bare_domains_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("example.com\n")
        sf.write("gpumagick.com\n")
        sf.write("test.org\n")
    test_config = FrontierTestConfig(
        data_dir=temp_test_frontier_dir,
        seed_file=seed_file_path
    )
    fm = FrontierManager(
        config=CrawlerConfig(**vars(test_config)),
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Make politeness permissive
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Initialize frontier
    await fm.initialize_frontier()
    
    retrieved_urls = []
    for _ in range(3):
        result = await fm.get_next_url()
        if result:
            url, domain, url_id, depth = result
            retrieved_urls.append(url)
    
    assert len(retrieved_urls) == 3, f"Expected 3 URLs, got {len(retrieved_urls)}"
    
    expected_proper_urls = {
        "http://example.com", "https://example.com",
        "http://gpumagick.com", "https://gpumagick.com", 
        "http://test.org", "https://test.org"
    }
    
    for url in retrieved_urls:
        assert any(url == proper_url or url == proper_url.replace('http://', 'https://') 
                  for proper_url in expected_proper_urls), \
            f"Retrieved URL '{url}' is not a proper URL with scheme"

@pytest.mark.asyncio
async def test_add_urls_batch(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing batch adding of URLs to Redis Frontier")
    
    # 1. Setup - Ensure frontier is empty and politeness is permissive
    await frontier_manager._clear_frontier()
    assert await frontier_manager.is_empty() is True
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True

    # 2. Add a batch of new URLs
    urls_to_add = ["http://test.com/batch1", "http://test.org/batch2", "http://test.com/batch1"]
    added_count = await frontier_manager.add_urls_batch(urls_to_add)

    # Assert that duplicates within the batch are handled and politeness was checked
    assert added_count == 2
    assert await frontier_manager.count_frontier() == 2
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2
    
    # 3. Add another batch, some new, some disallowed, some already seen
    mock_politeness_enforcer_for_frontier.is_url_allowed.reset_mock()
    
    # Make one URL disallowed
    def side_effect(url):
        if "disallowed" in url:
            return False
        return True
    mock_politeness_enforcer_for_frontier.is_url_allowed.side_effect = side_effect
    
    next_urls_to_add = ["http://new.com/page1", "http://test.org/batch2", "http://disallowed.com/page"]
    added_count_2 = await frontier_manager.add_urls_batch(next_urls_to_add)
    
    assert added_count_2 == 1  # Only new.com/page1 should be added
    assert await frontier_manager.count_frontier() == 3  # 2 from before + 1 new
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2  # disallowed.com and new.com
    
    logger.info("Batch URL adding test passed.")

@pytest.mark.asyncio
async def test_get_next_url(frontier_manager: FrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing get_next_url functionality")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add some URLs
    urls = ["http://example.com/page1", "http://example.org/page2", "http://test.com/page3"]
    await frontier_manager.add_urls_batch(urls)
    
    # Get URLs one by one
    retrieved_urls = []
    for _ in range(3):
        result = await frontier_manager.get_next_url()
        assert result is not None
        url, domain, url_id, depth = result
        retrieved_urls.append(url)
        assert depth == 0  # All seed URLs have depth 0
    
    # Should have retrieved all URLs
    assert set(retrieved_urls) == set(urls)
    
    # Frontier should be empty now
    assert await frontier_manager.count_frontier() == 0
    assert await frontier_manager.get_next_url() is None
    
    logger.info("get_next_url test passed.")

@pytest.mark.asyncio
async def test_redis_list_persistence(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing Redis list persistence")
    
    # Permissive politeness
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # First run: populate frontier
    cfg_run1 = CrawlerConfig(**vars(frontier_test_config_obj))
    
    frontier_run1 = FrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    await frontier_run1.initialize_frontier()
    
    # Add URLs with different depths
    test_urls = [
        ("http://persistent.com/page1", 0),
        ("http://persistent.com/page2", 1),
        ("http://example.net/page1", 2)
    ]
    
    # Add URLs with specified depths
    for url, depth in test_urls:
        await frontier_run1.add_urls_batch([url], depth=depth)
    
    # Check that frontier lists were created in Redis
    frontier_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_test_client.scan(cursor, match='frontier:*', count=100)
        frontier_keys.extend(keys)
        if cursor == 0:
            break
    
    assert len(frontier_keys) > 0, "No frontier lists were created"
    
    # Verify contents of frontier lists
    for key in frontier_keys:
        domain = key.split(':')[1]
        urls_in_list = await redis_test_client.lrange(key, 0, -1)
        logger.info(f"Domain {domain} has {len(urls_in_list)} URLs in Redis")
        
        # Parse and verify URL format
        for url_entry in urls_in_list:
            parts = url_entry.split('|')
            assert len(parts) == 2, f"Expected 2 parts in '{url_entry}'"
            url, depth_str = parts
            assert url.startswith('http'), f"Invalid URL: {url}"
            assert depth_str.isdigit(), f"Invalid depth: {depth_str}"
    
    # Get one URL and verify it's removed from the list
    url_retrieved = await frontier_run1.get_next_url()
    assert url_retrieved is not None
    
    # Calculate total URLs from all seed URLs + test URLs
    total_urls = 2 + len(test_urls)  # 2 seed URLs + 3 test URLs
    
    # Test that we can retrieve all URLs by iterating
    all_retrieved_urls = [url_retrieved[0]]  # Start with the one we already got
    
    while not await frontier_run1.is_empty():
        next_url = await frontier_run1.get_next_url()
        if next_url is None:
            continue
        all_retrieved_urls.append(next_url[0])
    
    # Verify we got all the URLs we added
    expected_urls = {
        "http://example.com/seed1",
        "http://example.org/seed2", 
        "http://persistent.com/page1",
        "http://persistent.com/page2",
        "http://example.net/page1"
    }
    assert len(all_retrieved_urls) == total_urls
    assert set(all_retrieved_urls) == expected_urls
    
    # Frontier should now be empty
    assert await frontier_run1.count_frontier() == 0
    
    logger.info("Redis list persistence test passed.")

@pytest.mark.asyncio
async def test_bloom_filter_deduplication(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing bloom filter deduplication")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Add URL first time
    first_add = await frontier_manager.add_urls_batch(["http://example.com/test"])
    assert first_add == 1
    
    # Try to add same URL again
    second_add = await frontier_manager.add_urls_batch(["http://example.com/test"])
    assert second_add == 0  # Should be rejected by bloom filter
    
    # Verify bloom filter contains the URL
    exists = await redis_test_client.execute_command('BF.EXISTS', 'seen:bloom', 'http://example.com/test')
    assert exists == 1
    
    logger.info("Bloom filter deduplication test passed.")

@pytest.mark.asyncio 
async def test_domain_queue(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing domain queue functionality")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add URLs from different domains
    urls = [
        "http://domain1.com/page1",
        "http://domain2.com/page1", 
        "http://domain1.com/page2"
    ]
    await frontier_manager.add_urls_batch(urls)
    
    # Check domain queue
    # Get all domains with scores
    queued_domains = await redis_test_client.zrange('domains:queue', 0, -1, withscores=True)
    
    # Extract just the domain names
    domain_names = {domain for domain, score in queued_domains}
    assert domain_names == {"domain1.com", "domain2.com"}
    
    # Verify all domains have valid scores (timestamps)
    current_time = time.time()
    for domain, score in queued_domains:
        assert isinstance(score, (int, float))
        assert score > 0
        assert score <= current_time + 1  # Allow 1 second buffer
    
    # Get a URL from a domain
    result = await frontier_manager.get_next_url()
    assert result is not None
    url, domain, _, _ = result
    
    # After fetching, domain should be back in queue with updated score
    await asyncio.sleep(0.1)  # Brief wait for async operations
    updated_queue = await redis_test_client.zrange('domains:queue', 0, -1, withscores=True)
    domain_dict = dict(updated_queue)
    
    # The domain should still be in the queue (it was put back after fetch)
    assert domain in domain_dict
    
    # The score should be updated to a future time (politeness delay)
    # Note: The exact score depends on the politeness delay, but it should be in the future
    new_score = domain_dict[domain]
    assert new_score > current_time  # Should have a future fetch time
    
    logger.info("Domain queue test passed.")

@pytest.mark.asyncio
async def test_frontier_error_handling(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock
):
    logger.info("Testing frontier error handling")
    
    # Test adding invalid URLs
    invalid_urls = ["not-a-url", "", "http://"]
    added = await frontier_manager.add_urls_batch(invalid_urls)
    assert added == 0  # None should be added
    
    # Test with domain extraction failure
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    weird_urls = ["http://[invalid-domain]/page"]
    added = await frontier_manager.add_urls_batch(weird_urls)
    # Should handle gracefully, likely 0 added due to domain extraction failure
    
    logger.info("Frontier error handling test passed.")

@pytest.mark.asyncio
async def test_atomic_domain_claiming_high_concurrency(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that URLs are claimed exactly once under high concurrency.
    
    This verifies that our Redis-based atomic domain claiming mechanism
    prevents race conditions when multiple workers compete for URLs.
    """
    logger.info("Testing atomic domain claiming under high concurrency")
    
    # Setup
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Create config for this test
    config = CrawlerConfig(**vars(frontier_test_config_obj))
    
    # Prepare test URLs across multiple domains
    # With domain-level politeness, each domain can only serve one URL at a time
    # So we need many domains with few URLs each for concurrent testing
    num_domains = 100
    urls_per_domain = 2
    test_urls = []
    
    for domain_idx in range(num_domains):
        domain = f"test{domain_idx}.com"
        for url_idx in range(urls_per_domain):
            test_urls.append(f"http://{domain}/page{url_idx}")
    
    total_urls = len(test_urls)
    logger.info(f"Created {total_urls} test URLs across {num_domains} domains")
    
    # Create a single frontier manager to populate the data
    seed_frontier = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Initialize but clear any existing data to start fresh
    await seed_frontier._clear_frontier()
    added = await seed_frontier.add_urls_batch(test_urls)
    # Allow for a small number of false positives from the bloom filter
    assert total_urls - added < 5, f"Expected close to {total_urls} URLs to be added, but only got {added}. This might indicate a bloom filter issue."
    
    # Verify domains are in ZSET with proper scores
    domains_in_queue = await redis_test_client.zcard('domains:queue')
    assert domains_in_queue == num_domains, f"Expected {num_domains} domains in queue, got {domains_in_queue}"
    
    # Create multiple frontier managers (simulating workers)
    num_workers = 50
    workers = []
    for i in range(num_workers):
        fm = FrontierManager(
            config=config,
            politeness=mock_politeness_enforcer_for_frontier,
            redis_client=redis_test_client
        )
        workers.append(fm)
    
    # Track URLs claimed by each worker
    worker_claims: Dict[int, List[str]] = defaultdict(list)
    
    async def worker_task(worker_id: int, frontier: FrontierManager, max_claims: int):
        """Worker task that claims URLs from the frontier."""
        claims = 0
        empty_attempts = 0
        max_empty_attempts = 100  # Prevent infinite loops
        
        while claims < max_claims and empty_attempts < max_empty_attempts:
            try:
                result = await frontier.get_next_url()
                if result is None:
                    # No URLs available, try again after brief wait
                    await asyncio.sleep(0.01)
                    empty_attempts += 1
                    continue
                    
                url, domain, _, depth = result
                worker_claims[worker_id].append(url)
                claims += 1
                empty_attempts = 0  # Reset empty attempts counter
                
                # Simulate some work
                await asyncio.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                break
                
        logger.debug(f"Worker {worker_id} finished with {claims} claims")
    
    # Run all workers concurrently
    urls_per_worker = total_urls // num_workers
    tasks = []
    
    for worker_id, frontier in enumerate(workers):
        task = asyncio.create_task(
            worker_task(worker_id, frontier, urls_per_worker)
        )
        tasks.append(task)
    
    # Wait for all workers to complete
    await asyncio.gather(*tasks)
    
    # Analyze results
    all_claimed_urls = []
    for worker_id, urls in worker_claims.items():
        all_claimed_urls.extend(urls)
        logger.debug(f"Worker {worker_id} claimed {len(urls)} URLs")
    
    # Check for duplicates (this is the critical test)
    unique_claims = set(all_claimed_urls)
    duplicates = len(all_claimed_urls) - len(unique_claims)
    
    if duplicates > 0:
        # Find which URLs were claimed multiple times
        from collections import Counter
        url_counts = Counter(all_claimed_urls)
        duplicate_urls = [(url, count) for url, count in url_counts.items() if count > 1]
        logger.error(f"Found {duplicates} duplicate claims:")
        for url, count in duplicate_urls[:10]:  # Show first 10
            logger.error(f"  - {url} claimed {count} times")
    
    assert duplicates == 0, f"Found {duplicates} duplicate URL claims!"
    
    # Verify total claims (some URLs might not be claimed due to timing)
    # We expect at least 90% to be claimed in this test
    expected_min_claims = int(total_urls * 0.9)
    assert len(all_claimed_urls) >= expected_min_claims, \
        f"Only {len(all_claimed_urls)} URLs claimed out of {total_urls} (expected at least {expected_min_claims})"
    
    logger.info(f"""
    Atomic domain claiming test completed:
    - Total URLs: {total_urls}
    - URLs claimed: {len(all_claimed_urls)}
    - Unique claims: {len(unique_claims)}
    - Duplicate claims: {duplicates}
    - Workers: {num_workers}
    - No race conditions detected ✓
    """)
    
    logger.info("Atomic domain claiming test passed.")

@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that frontier state persists correctly when resuming a crawl."""
    logger.info("Testing Frontier Resume Functionality with Politeness Mocks")
    
    # Permissive politeness for all operations in this test
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # --- First run: populate and get one URL ---
    cfg_run1_dict = vars(frontier_test_config_obj).copy()
    cfg_run1_dict['resume'] = False  # Ensure it's a new run
    cfg_run1 = CrawlerConfig(**cfg_run1_dict)
    
    frontier_run1 = FrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    await frontier_run1.initialize_frontier()
    # Seeds will be loaded: http://example.com/seed1, http://example.org/seed2
    
    # Add one more URL
    await frontier_run1.add_urls_batch(["http://persistent.com/page_from_run1"])
    assert await frontier_run1.count_frontier() == 3
    
    # Get one URL
    url_to_retrieve = await frontier_run1.get_next_url()
    assert url_to_retrieve is not None
    assert await frontier_run1.count_frontier() == 2
    
    # Note which URL was retrieved
    retrieved_url = url_to_retrieve[0]
    logger.info(f"First run retrieved URL: {retrieved_url}")
    
    # --- Second run: resume ---
    cfg_run2_dict = vars(frontier_test_config_obj).copy()
    cfg_run2_dict['resume'] = True
    cfg_run2 = CrawlerConfig(**cfg_run2_dict)
    
    frontier_run2 = FrontierManager(
        config=cfg_run2,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Initialize with resume=True - should preserve existing frontier
    await frontier_run2.initialize_frontier()
    
    # Should still have 2 URLs
    assert await frontier_run2.count_frontier() == 2
    
    # Get the remaining URLs
    remaining_urls = []
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    remaining_urls.append(next_url[0])
    
    next_url = await frontier_run2.get_next_url()
    assert next_url is not None
    remaining_urls.append(next_url[0])
    
    # Verify we got the expected URLs (the two that weren't retrieved in run1)
    all_urls = {"http://example.com/seed1", "http://example.org/seed2", "http://persistent.com/page_from_run1"}
    expected_remaining = all_urls - {retrieved_url}
    assert set(remaining_urls) == expected_remaining
    
    # Frontier should be empty now
    assert await frontier_run2.count_frontier() == 0
    
    logger.info("Frontier resume test passed with politeness mocks.")

@pytest.mark.asyncio
async def test_url_filtering_in_add_urls_batch(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock
):
    """Test that non-text URLs are filtered out when adding to frontier."""
    logger.info("Testing URL filtering in add_urls_batch")
    
    # Setup
    await frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Mix of text and non-text URLs
    urls_to_add = [
        "http://example.com/page.html",  # Should be added
        "http://example.com/image.jpg",  # Should be filtered
        "http://example.com/document.pdf",  # Should be filtered
        "http://example.com/article",  # Should be added
        "http://example.com/video.mp4",  # Should be filtered
        "http://example.com/api/data",  # Should be added
        "http://example.com/archive.zip",  # Should be filtered
    ]
    
    # Add URLs
    added_count = await frontier_manager.add_urls_batch(urls_to_add)
    
    # Only text URLs should be added (3 out of 7)
    assert added_count == 3, f"Expected 3 URLs to be added, got {added_count}"
    
    # Verify the correct URLs were added by retrieving them
    retrieved_urls = []
    while not await frontier_manager.is_empty():
        result = await frontier_manager.get_next_url()
        if result is None:
            continue
        retrieved_urls.append(result[0])
    
    expected_urls = {
        "http://example.com/page.html",
        "http://example.com/article",
        "http://example.com/api/data"
    }
    assert set(retrieved_urls) == expected_urls, \
        f"Expected URLs {expected_urls}, got {set(retrieved_urls)}"
    
    logger.info("URL filtering in add_urls_batch test passed.")

@pytest.mark.asyncio
async def test_url_filtering_in_get_next_url(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that non-text URLs are skipped when reading from frontier lists."""
    logger.info("Testing URL filtering in get_next_url")
    
    # Setup
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    config = CrawlerConfig(**vars(frontier_test_config_obj))
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Clear frontier
    await fm._clear_frontier()
    
    # Manually add mixed URLs directly to Redis (bypassing filtering in add_urls_batch)
    domain = "mixed-content.com"
    frontier_key = f"frontier:{domain}"
    
    # Add URLs directly to Redis list
    await redis_test_client.lpush(frontier_key, 
        "http://mixed-content.com/final-page.html|0",
        "http://mixed-content.com/data.zip|0",  # Should be skipped
        "http://mixed-content.com/api/endpoint|0",
        "http://mixed-content.com/video.mp4|0",  # Should be skipped
        "http://mixed-content.com/page2|0",
        "http://mixed-content.com/document.pdf|0",  # Should be skipped
        "http://mixed-content.com/image1.jpg|0",  # Should be skipped
        "http://mixed-content.com/page1.html|0"
    )
    
    # Add domain to queue
    await redis_test_client.zadd('domains:queue', {domain: int(time.time())})
    
    # Get URLs and verify only text URLs are returned
    retrieved_urls = []
    while not await fm.is_empty():
        result = await fm.get_next_url()
        if result is None:
            continue
        retrieved_urls.append(result[0])
    
    expected_urls = [
        "http://mixed-content.com/final-page.html",
        "http://mixed-content.com/api/endpoint",
        "http://mixed-content.com/page2",
        "http://mixed-content.com/page1.html"
    ]
    
    assert retrieved_urls == expected_urls, \
        f"Expected URLs {expected_urls}, got {retrieved_urls}"
    
    logger.info("URL filtering in get_next_url test passed.")
