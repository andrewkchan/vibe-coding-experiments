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

from crawler_module.frontier import HybridFrontierManager
from crawler_module.config import CrawlerConfig
from crawler_module.politeness import PolitenessEnforcer

# Configure basic logging for tests
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FrontierTestConfig:
    data_dir: Path
    seed_file: Path
    email: str = "frontier_test@example.com"
    exclude_file: Path | None = None
    max_workers: int = 1
    max_pages: int | None = None
    max_duration: int | None = None
    log_level: str = "DEBUG"
    resume: bool = False
    user_agent: str = "FrontierTestCrawler/1.0"
    seeded_urls_only: bool = False
    db_type: str = "sqlite"  # Still use SQLite for storage manager
    db_url: str | None = None

@pytest_asyncio.fixture
async def redis_client():
    """Provides a Redis client for tests."""
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Clean up test data before and after
    await client.flushdb()
    
    yield client
    
    # Cleanup after test
    await client.flushdb()
    await client.aclose()

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
    mock_pe.record_domain_fetch_attempt = AsyncMock()
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0)
    mock_pe._load_manual_exclusions = AsyncMock()
    return mock_pe

@pytest_asyncio.fixture
async def hybrid_frontier_manager(
    actual_config_for_frontier: CrawlerConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
) -> HybridFrontierManager:
    fm = HybridFrontierManager(
        config=actual_config_for_frontier,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_client
    )
    # Patch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock(side_effect=fm._mark_domains_as_seeded_batch)
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch)
    return fm

@pytest.mark.asyncio
async def test_frontier_initialization_new(hybrid_frontier_manager: HybridFrontierManager):
    logger.info("Testing Hybrid Frontier Initialization (New Crawl)")
    
    await hybrid_frontier_manager.initialize_frontier()
    
    # Assert that the batch methods were called
    hybrid_frontier_manager._mark_domains_as_seeded_batch.assert_called_once()
    hybrid_frontier_manager.add_urls_batch.assert_called_once()
    
    # Check the domains that were marked as seeded
    seeded_domains_call = hybrid_frontier_manager._mark_domains_as_seeded_batch.call_args[0][0]
    assert set(seeded_domains_call) == {"example.com", "example.org"}
    
    # Check the URLs that were added to the frontier
    urls_added_call = hybrid_frontier_manager.add_urls_batch.call_args[0][0]
    assert set(urls_added_call) == {"http://example.com/seed1", "http://example.org/seed2"}
    
    logger.info("Hybrid frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_add_urls_batch(hybrid_frontier_manager: HybridFrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing batch adding of URLs to Hybrid Frontier")
    
    # 1. Setup - Ensure frontier is empty and politeness is permissive
    await hybrid_frontier_manager._clear_frontier()
    assert await hybrid_frontier_manager.is_empty() is True
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True

    # 2. Add a batch of new URLs
    urls_to_add = ["http://test.com/batch1", "http://test.org/batch2", "http://test.com/batch1"]
    added_count = await hybrid_frontier_manager.add_urls_batch(urls_to_add)

    # Assert that duplicates within the batch are handled and politeness was checked
    assert added_count == 2
    assert await hybrid_frontier_manager.count_frontier() == 2
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
    added_count_2 = await hybrid_frontier_manager.add_urls_batch(next_urls_to_add)
    
    assert added_count_2 == 1  # Only new.com/page1 should be added
    assert await hybrid_frontier_manager.count_frontier() == 3  # 2 from before + 1 new
    assert mock_politeness_enforcer_for_frontier.is_url_allowed.call_count == 2  # disallowed.com and new.com
    
    logger.info("Batch URL adding test passed.")

@pytest.mark.asyncio
async def test_get_next_url(hybrid_frontier_manager: HybridFrontierManager, mock_politeness_enforcer_for_frontier: MagicMock):
    logger.info("Testing get_next_url functionality")
    
    # Setup
    await hybrid_frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add some URLs
    urls = ["http://example.com/page1", "http://example.org/page2", "http://test.com/page3"]
    await hybrid_frontier_manager.add_urls_batch(urls)
    
    # Get URLs one by one
    retrieved_urls = []
    for _ in range(3):
        result = await hybrid_frontier_manager.get_next_url()
        assert result is not None
        url, domain, url_id, depth = result
        retrieved_urls.append(url)
        assert depth == 0  # All seed URLs have depth 0
    
    # Should have retrieved all URLs
    assert set(retrieved_urls) == set(urls)
    
    # Frontier should be empty now
    assert await hybrid_frontier_manager.count_frontier() == 0
    assert await hybrid_frontier_manager.get_next_url() is None
    
    logger.info("get_next_url test passed.")

@pytest.mark.asyncio
async def test_frontier_file_persistence(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
):
    logger.info("Testing Frontier file persistence")
    
    # Permissive politeness
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # First run: populate frontier
    cfg_run1 = CrawlerConfig(**vars(frontier_test_config_obj))
    
    frontier_run1 = HybridFrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_client
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
    
    # Check that frontier files were created
    frontier_dir = cfg_run1.data_dir / "frontiers"
    assert frontier_dir.exists()
    
    # Find and read the frontier files to verify their contents
    frontier_files = list(frontier_dir.glob("*/*.frontier"))
    assert len(frontier_files) > 0, "No frontier files were created"
    
    # Read all URLs from files
    urls_from_files: Dict[str, List[Tuple[str, int]]] = {}
    for file_path in frontier_files:
        domain_name = file_path.stem  # filename without extension
        with open(file_path, 'r') as f:
            lines = f.readlines()
            urls_from_files[domain_name] = []
            for line in lines:
                line = line.strip()
                if line:
                    parts = line.split('|')
                    assert len(parts) == 2, f"Expected 2 parts in line '{line}', got {len(parts)}"
                    url, depth_str = parts
                    urls_from_files[domain_name].append((url, int(depth_str)))
    
    # Verify persistent.com file contains the right URLs with correct depths
    assert "persistent.com" in urls_from_files
    persistent_urls = urls_from_files["persistent.com"]
    assert len(persistent_urls) == 2
    assert ("http://persistent.com/page1", 0) in persistent_urls
    assert ("http://persistent.com/page2", 1) in persistent_urls
    
    # Verify example.net file
    assert "example.net" in urls_from_files
    example_net_urls = urls_from_files["example.net"]
    assert len(example_net_urls) == 1
    assert ("http://example.net/page1", 2) in example_net_urls
    
    # Also verify seed URLs were written (from initialize_frontier)
    assert "example.com" in urls_from_files
    assert "example.org" in urls_from_files
    
    # Get one URL and verify the offset is updated
    url_retrieved = await frontier_run1.get_next_url()
    assert url_retrieved is not None
    
    # Check Redis metadata for the domain we just fetched from
    retrieved_domain = url_retrieved[1]
    domain_metadata = await redis_client.hgetall(f"domain:{retrieved_domain}")  # type: ignore[misc]
    assert "frontier_offset" in domain_metadata
    assert int(domain_metadata["frontier_offset"]) == 1  # Should have read 1 URL
    
    # Calculate total URLs from all seed URLs + test URLs
    total_urls = 2 + len(test_urls)  # 2 seed URLs + 3 test URLs
    remaining_count = await frontier_run1.count_frontier()
    assert remaining_count == total_urls - 1  # One URL was retrieved
    
    logger.info("Frontier file persistence test passed.")

@pytest.mark.asyncio
async def test_bloom_filter_deduplication(
    hybrid_frontier_manager: HybridFrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
):
    logger.info("Testing bloom filter deduplication")
    
    # Setup
    await hybrid_frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Add URL first time
    first_add = await hybrid_frontier_manager.add_urls_batch(["http://example.com/test"])
    assert first_add == 1
    
    # Try to add same URL again
    second_add = await hybrid_frontier_manager.add_urls_batch(["http://example.com/test"])
    assert second_add == 0  # Should be rejected by bloom filter
    
    # Verify bloom filter contains the URL
    exists = await redis_client.execute_command('BF.EXISTS', 'seen:bloom', 'http://example.com/test')
    assert exists == 1
    
    logger.info("Bloom filter deduplication test passed.")

@pytest.mark.asyncio 
async def test_domain_ready_queue(
    hybrid_frontier_manager: HybridFrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
):
    logger.info("Testing domain ready queue functionality")
    
    # Setup
    await hybrid_frontier_manager._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add URLs from different domains
    urls = [
        "http://domain1.com/page1",
        "http://domain2.com/page1", 
        "http://domain1.com/page2"
    ]
    await hybrid_frontier_manager.add_urls_batch(urls)
    
    # Check domains in queue
    queued_domains = await redis_client.lrange('domains:queue', 0, -1)  # type: ignore[misc]
    # Convert to set to ignore order and potential duplicates
    assert set(queued_domains) == {"domain1.com", "domain2.com"}
    
    # Get a URL from a domain
    result = await hybrid_frontier_manager.get_next_url()
    assert result is not None
    url, domain, _, _ = result
    
    # After fetching, domain should still be in the queue (at the end)
    updated_queue = await redis_client.lrange('domains:queue', 0, -1)  # type: ignore[misc]
    assert domain in updated_queue  # Domain should still be there
    
    logger.info("Domain ready queue test passed.")

@pytest.mark.asyncio
async def test_frontier_error_handling(
    hybrid_frontier_manager: HybridFrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock
):
    logger.info("Testing frontier error handling")
    
    # Test adding invalid URLs
    invalid_urls = ["not-a-url", "", "http://"]
    added = await hybrid_frontier_manager.add_urls_batch(invalid_urls)
    assert added == 0  # None should be added
    
    # Test with domain extraction failure
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    weird_urls = ["http://[invalid-domain]/page"]
    added = await hybrid_frontier_manager.add_urls_batch(weird_urls)
    # Should handle gracefully, likely 0 added due to domain extraction failure
    
    logger.info("Frontier error handling test passed.")

@pytest.mark.asyncio
async def test_atomic_domain_claiming_high_concurrency(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
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
    seed_frontier = HybridFrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_client
    )
    
    # Initialize but clear any existing data to start fresh
    await seed_frontier._clear_frontier()
    added = await seed_frontier.add_urls_batch(test_urls)
    assert added == total_urls, f"Failed to add all test URLs: {added}/{total_urls}"
    
    # Create multiple frontier managers (simulating workers)
    num_workers = 50
    workers = []
    for i in range(num_workers):
        fm = HybridFrontierManager(
            config=config,
            politeness=mock_politeness_enforcer_for_frontier,
            redis_client=redis_client
        )
        # Don't initialize frontier for workers - they share the same data
        workers.append((i, fm))
    
    # Track claims by worker
    claims_by_worker: Dict[int, List[str]] = defaultdict(list)
    all_claimed_urls: List[str] = []
    
    async def worker_task(worker_id: int, frontier: HybridFrontierManager, max_claims: int):
        """Simulates a worker claiming and processing URLs."""
        local_claims = []
        for _ in range(max_claims):
            result = await frontier.get_next_url(worker_id=worker_id, total_workers=num_workers)
            
            if result:
                url, domain, url_id, depth = result
                local_claims.append(url)
                # Simulate minimal processing time
                await asyncio.sleep(0.001)
            else:
                # No more URLs available
                break
        
        return worker_id, local_claims
    
    # Run all workers concurrently
    start_time = time.time()
    tasks = [
        worker_task(worker_id, frontier, 10)  # Each worker tries to claim up to 10 URLs
        for worker_id, frontier in workers
    ]
    
    results = await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    # Process results
    for worker_id, claims in results:
        claims_by_worker[worker_id] = claims
        all_claimed_urls.extend(claims)
    
    # Assertions
    
    # 1. No URL should be claimed by multiple workers
    unique_urls = set(all_claimed_urls)
    duplicates = len(all_claimed_urls) - len(unique_urls)
    assert duplicates == 0, \
        f"Race condition detected! {duplicates} URLs were claimed multiple times"
    
    # 2. Total claims should not exceed available URLs
    assert len(all_claimed_urls) <= total_urls, \
        f"Claimed {len(all_claimed_urls)} URLs but only {total_urls} were available"
    
    # 3. With domain-level politeness, we expect to get at least one URL per domain
    # in the first pass
    min_expected = min(num_domains, total_urls)  # At least one per domain
    assert len(all_claimed_urls) >= min_expected, \
        f"Only {len(all_claimed_urls)} URLs claimed, expected at least {min_expected} (one per domain)"
    
    # 4. Verify work distribution
    claims_per_worker = [len(claims) for claims in claims_by_worker.values() if claims]
    workers_with_claims = len(claims_per_worker)
    
    if claims_per_worker:
        min_claims = min(claims_per_worker)
        max_claims = max(claims_per_worker)
        avg_claims = sum(claims_per_worker) / len(claims_per_worker)
        
        logger.info(f"Work distribution: min={min_claims}, max={max_claims}, avg={avg_claims:.1f}")
        logger.info(f"Workers that got URLs: {workers_with_claims}/{num_workers}")
    
    # 5. Verify remaining URLs in frontier (should be roughly one per domain due to politeness)
    remaining_count = await seed_frontier.count_frontier()
    # We expect approximately (urls_per_domain - 1) * num_domains URLs remaining
    expected_remaining = (urls_per_domain - 1) * num_domains
    assert remaining_count <= expected_remaining, \
        f"Frontier has {remaining_count} URLs, expected at most {expected_remaining}"
    
    logger.info(f"""
    Redis Atomic Claim Test Results:
    - Total URLs: {total_urls}
    - Workers: {num_workers}
    - Workers that claimed URLs: {workers_with_claims}
    - Total claimed: {len(all_claimed_urls)}
    - Unique claims: {len(unique_urls)}
    - Total time: {total_time:.2f}s
    - URLs/second: {len(all_claimed_urls) / total_time:.0f}
    - No race conditions detected ✓
    """)
    
    logger.info("Atomic domain claiming test passed.")

@pytest.mark.asyncio
async def test_frontier_resume_with_politeness(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_client: redis.Redis
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
    
    frontier_run1 = HybridFrontierManager(
        config=cfg_run1,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_client
    )
    
    await frontier_run1.initialize_frontier()
    # Seeds will be loaded: http://example.com/seed1, http://example.org/seed2
    
    # Add one more URL
    await frontier_run1.add_urls_batch(["http://persistent.com/page_from_run1"])
    assert await frontier_run1.count_frontier() == 3
    
    # Get one URL (should be from first domain in queue)
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
    
    frontier_run2 = HybridFrontierManager(
        config=cfg_run2,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_client
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
