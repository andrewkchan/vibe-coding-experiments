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
    mock_pe.record_domain_fetch_attempt = AsyncMock()
    mock_pe.get_crawl_delay = AsyncMock(return_value=0.0)
    mock_pe._load_manual_exclusions = AsyncMock()
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
    logger.info("Testing Hybrid Frontier Initialization (New Crawl)")
    
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
    
    logger.info("Hybrid frontier initialization (new) test passed.")

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
        result = await fm.get_next_url(fetcher_id=0)
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
    logger.info("Testing batch adding of URLs to Hybrid Frontier")
    
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
        result = await frontier_manager.get_next_url(fetcher_id=0)
        assert result is not None
        url, domain, url_id, depth = result
        retrieved_urls.append(url)
        assert depth == 0  # All seed URLs have depth 0
    
    # Should have retrieved all URLs
    assert set(retrieved_urls) == set(urls)
    
    # Frontier should be empty now
    assert await frontier_manager.count_frontier() == 0
    assert await frontier_manager.get_next_url(fetcher_id=0) is None
    
    logger.info("get_next_url test passed.")

@pytest.mark.asyncio
async def test_frontier_file_persistence(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing Frontier file persistence")
    
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
    url_retrieved = await frontier_run1.get_next_url(fetcher_id=0)
    assert url_retrieved is not None
    
    # Check Redis metadata for the domain we just fetched from
    retrieved_domain = url_retrieved[1]
    domain_metadata = await redis_test_client.hgetall(f"domain:{retrieved_domain}")  # type: ignore[misc]
    assert "frontier_offset" in domain_metadata
    # frontier_offset is now in bytes, should be > 0 after reading first URL
    offset_bytes = int(domain_metadata["frontier_offset"])
    assert offset_bytes > 0  # Should have advanced past the first URL
    
    # Calculate total URLs from all seed URLs + test URLs
    total_urls = 2 + len(test_urls)  # 2 seed URLs + 3 test URLs
    
    # Test that we can retrieve all URLs by iterating
    all_retrieved_urls = [url_retrieved[0]]  # Start with the one we already got
    
    while True:
        next_url = await frontier_run1.get_next_url(fetcher_id=0)
        if next_url is None:
            break
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
    
    logger.info("Frontier file persistence test passed.")

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
async def test_domain_ready_queue(
    frontier_manager: FrontierManager,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    logger.info("Testing domain ready queue functionality")
    
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
    
    # Initialize shard count if not set (for compatibility)
    await redis_test_client.set('crawler:shard_count', 1)
    shard_count = 1
    
    # Collect all domains from all shards
    all_queued_domains = set()
    for shard in range(shard_count):
        shard_domains = await redis_test_client.lrange(f'domains:queue:{shard}', 0, -1)  # type: ignore[misc]
        all_queued_domains.update(shard_domains)
    
    # Convert to set to ignore order and potential duplicates
    assert all_queued_domains == {"domain1.com", "domain2.com"}
    
    # Get a URL from a domain (fetcher_id=0 for test)
    result = await frontier_manager.get_next_url(fetcher_id=0)
    assert result is not None
    url, domain, _, _ = result
    
    # After fetching, check the domain's shard queue
    domain_shard = frontier_manager._get_domain_shard(domain)
    updated_queue = await redis_test_client.lrange(f'domains:queue:{domain_shard}', 0, -1)  # type: ignore[misc]
    assert domain in updated_queue  # Domain should be at the end
    
    logger.info("Domain ready queue test passed.")

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
    
    # Create multiple frontier managers (simulating workers)
    num_workers = 50
    workers = []
    for i in range(num_workers):
        fm = FrontierManager(
            config=config,
            politeness=mock_politeness_enforcer_for_frontier,
            redis_client=redis_test_client
        )
        # Don't initialize frontier for workers - they share the same data
        workers.append((i, fm))
    
    # Track claims by worker
    claims_by_worker: Dict[int, List[str]] = defaultdict(list)
    all_claimed_urls: List[str] = []
    
    async def worker_task(worker_id: int, frontier: FrontierManager, max_claims: int):
        """Simulates a worker claiming and processing URLs."""
        local_claims = []
        for _ in range(max_claims):
            result = await frontier.get_next_url(fetcher_id=0)
            
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
    
    # Get one URL (should be from first domain in queue)
    url_to_retrieve = await frontier_run1.get_next_url(fetcher_id=0)
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
    next_url = await frontier_run2.get_next_url(fetcher_id=0)
    assert next_url is not None
    remaining_urls.append(next_url[0])
    
    next_url = await frontier_run2.get_next_url(fetcher_id=0)
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
    while True:
        result = await frontier_manager.get_next_url(fetcher_id=0)
        if result is None:
            break
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
    """Test that non-text URLs are skipped when reading from frontier files."""
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
    
    # Clear and manually create a frontier file with mixed content
    await fm._clear_frontier()
    
    # Manually write a frontier file with mixed URLs
    domain = "mixed-content.com"
    frontier_path = fm._get_frontier_path(domain)
    frontier_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write URLs directly to file (bypassing filtering in add_urls_batch)
    with open(frontier_path, 'w') as f:
        f.write("http://mixed-content.com/page1.html|0\n")
        f.write("http://mixed-content.com/image1.jpg|0\n")  # Should be skipped
        f.write("http://mixed-content.com/document.pdf|0\n")  # Should be skipped
        f.write("http://mixed-content.com/page2|0\n")
        f.write("http://mixed-content.com/video.mp4|0\n")  # Should be skipped
        f.write("http://mixed-content.com/api/endpoint|0\n")
        f.write("http://mixed-content.com/data.zip|0\n")  # Should be skipped
        f.write("http://mixed-content.com/final-page.html|0\n")
    
    # Set up Redis metadata for this domain
    domain_key = f"domain:{domain}"
    await redis_test_client.hset(domain_key, mapping={  # type: ignore[misc]
        'frontier_size': str(frontier_path.stat().st_size),
        'file_path': str(frontier_path.relative_to(fm.frontier_dir)),
        'frontier_offset': '0'
    })
    
    # Add domain to the appropriate shard queue
    shard_count = await redis_test_client.get('crawler:shard_count')
    if not shard_count:
        # Initialize shard count if not set
        await redis_test_client.set('crawler:shard_count', 1)
        shard_count = 1
    else:
        shard_count = int(shard_count)
    
    domain_shard = fm._get_domain_shard(domain)
    await redis_test_client.rpush(f'domains:queue:{domain_shard}', domain)  # type: ignore[misc]
    
    # Get URLs and verify only text URLs are returned
    retrieved_urls = []
    while True:
        result = await fm.get_next_url(fetcher_id=0)
        if result is None:
            break
        retrieved_urls.append(result[0])
    
    expected_urls = [
        "http://mixed-content.com/page1.html",
        "http://mixed-content.com/page2",
        "http://mixed-content.com/api/endpoint",
        "http://mixed-content.com/final-page.html"
    ]
    
    assert retrieved_urls == expected_urls, \
        f"Expected URLs {expected_urls}, got {retrieved_urls}"
    
    # Verify that the offset was updated correctly (should be at end of file)
    final_metadata = await redis_test_client.hgetall(domain_key)  # type: ignore[misc]
    final_offset = int(final_metadata['frontier_offset'])
    file_size = frontier_path.stat().st_size
    assert final_offset == file_size, \
        f"Expected offset to be at end of file ({file_size}), got {final_offset}"
    
    logger.info("URL filtering in get_next_url test passed.")

@pytest.mark.asyncio
async def test_multi_shard_domain_distribution(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that domains are properly distributed across multiple shards."""
    logger.info("Testing multi-shard domain distribution")
    
    # Setup with 3 fetcher processes
    config_dict = vars(frontier_test_config_obj).copy()
    config_dict['num_fetcher_processes'] = 3  # 3 shards
    config = CrawlerConfig(**config_dict)
    
    # Set shard count in Redis
    await redis_test_client.set('crawler:shard_count', 3)
    
    # Create frontier manager
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Force load shard count
    await fm._get_shard_count()
    
    # Setup
    await fm._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    
    # Add URLs from many domains
    test_domains = [f"domain{i}.com" for i in range(30)]
    urls_to_add = []
    for domain in test_domains:
        urls_to_add.extend([
            f"http://{domain}/page1",
            f"http://{domain}/page2"
        ])
    
    added = await fm.add_urls_batch(urls_to_add)
    assert added == len(urls_to_add)
    
    # Check domain distribution across shards
    shard_domains: Dict[int, List[str]] = {0: [], 1: [], 2: []}
    
    for shard in range(3):
        domains = await redis_test_client.lrange(f'domains:queue:{shard}', 0, -1)  # type: ignore[misc]
        shard_domains[shard] = list(set(domains))  # Remove duplicates
        logger.info(f"Shard {shard} has {len(shard_domains[shard])} domains")
    
    # Verify each domain is in exactly one shard
    all_domains_in_shards = set()
    for shard, domains in shard_domains.items():
        for domain in domains:
            assert domain not in all_domains_in_shards, f"Domain {domain} found in multiple shards!"
            all_domains_in_shards.add(domain)
    
    assert all_domains_in_shards == set(test_domains), "Not all domains were distributed to shards"
    
    # Verify distribution is reasonably balanced
    # With hash-based distribution, perfect balance isn't guaranteed
    # We'll check that no shard has more than 60% of domains
    max_allowed = int(len(test_domains) * 0.6)
    min_allowed = int(len(test_domains) * 0.1)  # At least 10% per shard
    
    for shard, domains in shard_domains.items():
        assert min_allowed <= len(domains) <= max_allowed, \
            f"Shard {shard} has unbalanced count: {len(domains)} (expected {min_allowed}-{max_allowed})"
    
    # Verify that domains are assigned to correct shards based on hash
    for domain in test_domains:
        expected_shard = fm._get_domain_shard(domain)
        assert domain in shard_domains[expected_shard], \
            f"Domain {domain} not in expected shard {expected_shard}"
    
    logger.info("Multi-shard domain distribution test passed.")

@pytest.mark.asyncio
async def test_multi_shard_fetcher_isolation(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that fetchers only get URLs from their assigned shard."""
    logger.info("Testing multi-shard fetcher isolation")
    
    # Setup with 2 fetcher processes
    config_dict = vars(frontier_test_config_obj).copy()
    config_dict['num_fetcher_processes'] = 2  # 2 shards
    config = CrawlerConfig(**config_dict)
    
    # Set shard count in Redis
    await redis_test_client.set('crawler:shard_count', 2)
    
    # Create two frontier managers (simulating two fetcher processes)
    fm0 = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    fm1 = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Force load shard count
    await fm0._get_shard_count()
    await fm1._get_shard_count()
    
    # Setup
    await fm0._clear_frontier()
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Add URLs from domains that we know will hash to different shards
    # We'll add many domains and track which ones go to which shard
    test_urls = []
    for i in range(20):
        domain = f"test{i}.com"
        test_urls.append(f"http://{domain}/page1")
    
    added = await fm0.add_urls_batch(test_urls)
    assert added > 0
    
    # Track which domains are in which shard
    shard0_domains = set()
    shard1_domains = set()
    
    for i in range(20):
        domain = f"test{i}.com"
        shard = fm0._get_domain_shard(domain)
        if shard == 0:
            shard0_domains.add(domain)
        else:
            shard1_domains.add(domain)
    
    logger.info(f"Shard 0 has {len(shard0_domains)} domains, Shard 1 has {len(shard1_domains)} domains")
    
    # Both shards should have some domains
    assert len(shard0_domains) > 0, "Shard 0 has no domains"
    assert len(shard1_domains) > 0, "Shard 1 has no domains"
    
    # Fetcher 0 should only get URLs from shard 0
    fetcher0_urls = []
    for _ in range(len(shard0_domains)):
        result = await fm0.get_next_url(fetcher_id=0)
        if result:
            url, domain, _, _ = result
            fetcher0_urls.append(url)
            assert domain in shard0_domains, f"Fetcher 0 got URL from wrong shard: {domain}"
    
    # Fetcher 0 should not be able to get any more URLs
    result = await fm0.get_next_url(fetcher_id=0)
    assert result is None, "Fetcher 0 got URL from wrong shard"
    
    # Fetcher 1 should only get URLs from shard 1
    fetcher1_urls = []
    for _ in range(len(shard1_domains)):
        result = await fm1.get_next_url(fetcher_id=1)
        if result:
            url, domain, _, _ = result
            fetcher1_urls.append(url)
            assert domain in shard1_domains, f"Fetcher 1 got URL from wrong shard: {domain}"
    
    # Fetcher 1 should not be able to get any more URLs
    result = await fm1.get_next_url(fetcher_id=1)
    assert result is None, "Fetcher 1 got URL from wrong shard"
    
    # Verify total URLs fetched
    assert len(fetcher0_urls) == len(shard0_domains)
    assert len(fetcher1_urls) == len(shard1_domains)
    assert len(fetcher0_urls) + len(fetcher1_urls) == added
    
    logger.info("Multi-shard fetcher isolation test passed.")

@pytest.mark.asyncio
async def test_resharding_on_config_change(
    temp_test_frontier_dir: Path,
    frontier_test_config_obj: FrontierTestConfig,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that the orchestrator properly reshards when fetcher count changes."""
    logger.info("Testing resharding on configuration change")
    
    # Start with 2 shards
    await redis_test_client.set('crawler:shard_count', 2)
    
    # Create a dummy frontier to use its sharding method
    config_2_shards_dict = vars(frontier_test_config_obj).copy()
    config_2_shards_dict['num_fetcher_processes'] = 2
    config_2_shards = CrawlerConfig(**config_2_shards_dict)
    
    dummy_frontier_2 = FrontierManager(
        config=config_2_shards,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    await dummy_frontier_2._get_shard_count()  # Load shard count
    
    # Manually add domains to the old shard structure
    test_domains = [f"domain{i}.com" for i in range(10)]
    
    # Distribute domains to 2 shards using frontier's sharding method
    for domain in test_domains:
        shard = dummy_frontier_2._get_domain_shard(domain)
        await redis_test_client.rpush(f'domains:queue:{shard}', domain)
    
    # Verify initial distribution
    shard0_before = await redis_test_client.lrange('domains:queue:0', 0, -1)
    shard1_before = await redis_test_client.lrange('domains:queue:1', 0, -1)
    total_before = len(shard0_before) + len(shard1_before)
    assert total_before == len(test_domains)
    logger.info(f"Initial distribution: shard 0={len(shard0_before)}, shard 1={len(shard1_before)}")
    
    # Create config with 3 fetcher processes
    config_3_shards_dict = vars(frontier_test_config_obj).copy()
    config_3_shards_dict['num_fetcher_processes'] = 3  # This will trigger resharding to 3 shards
    config_3_shards = CrawlerConfig(**config_3_shards_dict)
    
    # Create orchestrator (just for resharding, won't run the crawler)
    orchestrator = CrawlerOrchestrator(config_3_shards)
    orchestrator.redis_client = redis_test_client  # Use test Redis client
    
    # Call the actual resharding method
    await orchestrator._reshard_domain_queues()
    
    # Create a dummy frontier with 3 shards to verify the results
    dummy_frontier_3 = FrontierManager(
        config=config_3_shards,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    await dummy_frontier_3._get_shard_count()  # Load shard count
    
    # Verify new distribution
    shard_counts = {}
    all_redistributed_domains = set()
    
    for shard in range(3):  # New shard count
        domains = await redis_test_client.lrange(f'domains:queue:{shard}', 0, -1)
        shard_counts[shard] = len(domains)
        all_redistributed_domains.update(domains)
        
        # Verify each domain is in the correct shard using frontier's method
        for domain in domains:
            expected_shard = dummy_frontier_3._get_domain_shard(domain)
            assert expected_shard == shard, f"Domain {domain} in wrong shard after resharding"
    
    # All domains should still be present
    assert len(all_redistributed_domains) == len(test_domains)
    assert all_redistributed_domains == set(test_domains)
    
    # Verify shard count was updated
    new_shard_count = await redis_test_client.get('crawler:shard_count')
    assert int(new_shard_count) == 3
    
    # Distribution should be reasonably balanced
    # With only 10 domains and 3 shards, it's possible (though unlikely) that
    # one shard gets 0 domains due to hash distribution
    total_redistributed = sum(shard_counts.values())
    assert total_redistributed == len(test_domains), \
        f"Lost domains during resharding: {total_redistributed} vs {len(test_domains)}"
    
    # Log distribution for debugging
    for shard, count in shard_counts.items():
        logger.info(f"Shard {shard} has {count} domains after resharding")
    
    # With 10 domains and 3 shards, at least 2 shards should have domains
    shards_with_domains = sum(1 for count in shard_counts.values() if count > 0)
    assert shards_with_domains >= 2, f"Too many empty shards: only {shards_with_domains} have domains"
    
    # Ensure no shard has more than 70% of domains (7 out of 10)
    max_count = max(shard_counts.values())
    assert max_count <= 7, f"Unbalanced distribution: max shard has {max_count} domains"
    
    logger.info("Resharding test passed.")
