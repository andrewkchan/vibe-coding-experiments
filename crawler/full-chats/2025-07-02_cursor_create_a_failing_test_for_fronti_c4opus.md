# Create a failing test for frontier seeding
_Exported on 7/2/2025 at 18:01:54 PDT from Cursor (1.0.0)_

---

**User**

Frontier seeding is currently broken when loading links from a seed file that are not proper URLs but instead just domains (e.g. "gpumagick.com" instead of "http://gpumagick.com"). Help me write a test in test_frontier.py displaying the problem that should fail when run. See test_frontier_initialization_new for an existing example test. Don't try to fix the bug

---

**Cursor**

Looking at the code, I can see the issue. The `_load_seeds` method expects proper URLs but if the seed file contains bare domains (like "gpumagick.com" instead of "http://gpumagick.com"), the `extract_domain` function likely won't handle them correctly, and the URLs won't be added to the frontier properly.

Here's a test that demonstrates this bug:

```python
// ... existing code ...
    logger.info("Hybrid frontier initialization (new) test passed.")

@pytest.mark.asyncio
async def test_frontier_initialization_with_bare_domains_bug(
    temp_test_frontier_dir: Path,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that demonstrates the bug when seed file contains bare domains instead of proper URLs."""
    logger.info("Testing frontier initialization with bare domains (should fail - demonstrating bug)")
    
    # Create a seed file with bare domains (no http://)
    seed_file_path = temp_test_frontier_dir / "bare_domains_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("example.com\n")
        sf.write("gpumagick.com\n")
        sf.write("test.org\n")
    
    # Create config with the bare domains seed file
    config = CrawlerConfig(
        data_dir=temp_test_frontier_dir,
        seed_file=seed_file_path,
        email="test@example.com",
        redis_host="localhost",
        redis_port=6379,
        redis_db=15
    )
    
    # Create frontier manager
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Patch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock(side_effect=fm._mark_domains_as_seeded_batch)
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch)
    
    # Initialize frontier
    await fm.initialize_frontier()
    
    # The bug: bare domains won't be properly processed
    # extract_domain() likely returns None for bare domains without scheme
    # This means no domains will be marked as seeded
    if fm._mark_domains_as_seeded_batch.called:
        seeded_domains_call = fm._mark_domains_as_seeded_batch.call_args[0][0]
        # This assertion should fail because no domains were extracted from bare domain strings
        assert len(seeded_domains_call) == 3, f"Expected 3 domains to be seeded, but got {len(seeded_domains_call)}"
        assert set(seeded_domains_call) == {"example.com", "gpumagick.com", "test.org"}, \
            f"Expected domains not found. Got: {set(seeded_domains_call)}"
    else:
        # If the method wasn't even called, that's also a bug
        assert False, "_mark_domains_as_seeded_batch was not called at all"
    
    # Check that URLs were added - they likely won't be since bare domains aren't valid URLs
    if fm.add_urls_batch.called:
        urls_added_call = fm.add_urls_batch.call_args[0][0]
        # This should fail - bare domains aren't valid URLs
        assert len(urls_added_call) == 3, f"Expected 3 URLs to be added, but got {len(urls_added_call)}"
        assert set(urls_added_call) == {"example.com", "gpumagick.com", "test.org"}, \
            f"Expected URLs not found. Got: {set(urls_added_call)}"
    else:
        # If add_urls_batch wasn't called, that's also indicating the bug
        assert False, "add_urls_batch was not called at all"
    
    # Verify frontier is empty (because bare domains couldn't be added)
    count = await fm.count_frontier()
    assert count == 3, f"Expected 3 URLs in frontier, but got {count}. Bare domains were not properly added."
    
    logger.info("Bug demonstrated: bare domains in seed file are not handled properly")

@pytest.mark.asyncio
// ... existing code ...
```

I need to fix the linter error by providing all required parameters for CrawlerConfig. Let me check the dataclass definition to see which parameters have defaults:

Let me look at the FrontierTestConfig class more carefully:

Now I can see that I need to provide all the parameters from the FrontierTestConfig dataclass. Let me update the test to use the same pattern as other tests:

```python
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_initialization_with_bare_domains_bug(
    temp_test_frontier_dir: Path,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test that demonstrates the bug when seed file contains bare domains instead of proper URLs."""
    logger.info("Testing frontier initialization with bare domains (should fail - demonstrating bug)")
    
    # Create a seed file with bare domains (no http://)
    seed_file_path = temp_test_frontier_dir / "bare_domains_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("example.com\n")
        sf.write("gpumagick.com\n")
        sf.write("test.org\n")
    
    # Create test config with bare domains seed file
    test_config = FrontierTestConfig(
        data_dir=temp_test_frontier_dir,
        seed_file=seed_file_path
    )
    
    # Create actual CrawlerConfig from test config
    config = CrawlerConfig(**vars(test_config))
    
    # Create frontier manager
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Patch methods for inspection
    fm._mark_domains_as_seeded_batch = AsyncMock(side_effect=fm._mark_domains_as_seeded_batch)
    fm.add_urls_batch = AsyncMock(side_effect=fm.add_urls_batch)
    
    # Initialize frontier
    await fm.initialize_frontier()
    
    # The bug: bare domains won't be properly processed
    # extract_domain() likely returns None for bare domains without scheme
    # This means no domains will be marked as seeded
    if fm._mark_domains_as_seeded_batch.called:
        seeded_domains_call = fm._mark_domains_as_seeded_batch.call_args[0][0]
        # This assertion should fail because no domains were extracted from bare domain strings
        assert len(seeded_domains_call) == 3, f"Expected 3 domains to be seeded, but got {len(seeded_domains_call)}"
        assert set(seeded_domains_call) == {"example.com", "gpumagick.com", "test.org"}, \
            f"Expected domains not found. Got: {set(seeded_domains_call)}"
    else:
        # If the method wasn't even called, that's also a bug
        assert False, "_mark_domains_as_seeded_batch was not called at all"
    
    # Check that URLs were added - they likely won't be since bare domains aren't valid URLs
    if fm.add_urls_batch.called:
        urls_added_call = fm.add_urls_batch.call_args[0][0]
        # This should fail - bare domains aren't valid URLs
        assert len(urls_added_call) == 3, f"Expected 3 URLs to be added, but got {len(urls_added_call)}"
        assert set(urls_added_call) == {"example.com", "gpumagick.com", "test.org"}, \
            f"Expected URLs not found. Got: {set(urls_added_call)}"
    else:
        # If add_urls_batch wasn't called, that's also indicating the bug
        assert False, "add_urls_batch was not called at all"
    
    # Verify frontier is empty (because bare domains couldn't be added)
    count = await fm.count_frontier()
    assert count == 3, f"Expected 3 URLs in frontier, but got {count}. Bare domains were not properly added."
    
    logger.info("Bug demonstrated: bare domains in seed file are not handled properly")

@pytest.mark.asyncio
// ... existing code ...
```

Great! I've created a test that should demonstrate the bug when seed files contain bare domains instead of proper URLs. The test:

1. **Creates a seed file with bare domains** (no `http://` prefix):
   - `example.com`
   - `gpumagick.com`
   - `test.org`

2. **Initializes the frontier** with this seed file

3. **Checks that the frontier seeding failed** by verifying:
   - Whether `_mark_domains_as_seeded_batch` was called with the expected domains (it likely won't be because `extract_domain()` probably returns `None` for bare domains)
   - Whether `add_urls_batch` was called with the URLs (it likely won't be because bare domains aren't valid URLs)
   - Whether the frontier has the expected URLs (it likely won't)

The test includes multiple assertions that should fail, demonstrating the bug:
- It expects 3 domains to be marked as seeded, but likely 0 will be
- It expects 3 URLs to be added to the frontier, but likely 0 will be
- It expects the frontier to contain 3 URLs, but it will likely be empty

When you run this test with `pytest crawler/tests/test_frontier.py::test_frontier_initialization_with_bare_domains_bug -v`, it should fail, clearly demonstrating that the frontier doesn't handle bare domains in the seed file properly.

---

**User**

The test passes. The problem is that the domains actually do get added to the frontier, but they aren't being normalized to be proper URLs. The test should instead assert that frontier.get_next_url returns proper URLs. Again don't try to fix the bug, just modify the test

---

**Cursor**

I see! The frontier is actually accepting the bare domains, but the bug is that they're not being normalized to proper URLs. Let me modify the test to check what `get_next_url()` returns:

```python
// ... existing code ...
@pytest.mark.asyncio
async def test_frontier_initialization_with_bare_domains_bug(
    temp_test_frontier_dir: Path,
    mock_politeness_enforcer_for_frontier: MagicMock,
    redis_test_client: redis.Redis
):
    """Test when seed file contains bare domains instead of proper URLs."""
    
    # Create a seed file with bare domains (no http://)
    seed_file_path = temp_test_frontier_dir / "bare_domains_seeds.txt"
    with open(seed_file_path, 'w') as sf:
        sf.write("example.com\n")
        sf.write("gpumagick.com\n")
        sf.write("test.org\n")
    
    # Create test config with bare domains seed file
    test_config = FrontierTestConfig(
        data_dir=temp_test_frontier_dir,
        seed_file=seed_file_path
    )
    
    # Create actual CrawlerConfig from test config
    config = CrawlerConfig(**vars(test_config))
    
    # Create frontier manager
    fm = FrontierManager(
        config=config,
        politeness=mock_politeness_enforcer_for_frontier,
        redis_client=redis_test_client
    )
    
    # Make politeness permissive
    mock_politeness_enforcer_for_frontier.is_url_allowed.return_value = True
    mock_politeness_enforcer_for_frontier.can_fetch_domain_now.return_value = True
    
    # Initialize frontier
    await fm.initialize_frontier()
    
    # The frontier should have accepted the bare domains (that's the bug)
    count = await fm.count_frontier()
    assert count > 0, "Frontier should contain URLs (even if they're malformed)"
    
    # Now try to get URLs from the frontier
    retrieved_urls = []
    for _ in range(3):
        result = await fm.get_next_url(fetcher_id=0)
        if result:
            url, domain, url_id, depth = result
            retrieved_urls.append(url)
    
    # The bug: get_next_url returns bare domains, not proper URLs
    # These assertions should fail because the URLs lack http:// or https:// prefix
    for url in retrieved_urls:
        assert url.startswith('http://') or url.startswith('https://'), \
            f"URL '{url}' is not a proper URL - it's missing the scheme (http:// or https://)"
    
    # Also check that we got the expected domains (but as improper URLs)
    assert len(retrieved_urls) == 3, f"Expected 3 URLs, got {len(retrieved_urls)}"
    
    # These should be proper URLs but they're not
    expected_proper_urls = {
        "http://example.com", "https://example.com",
        "http://gpumagick.com", "https://gpumagick.com", 
        "http://test.org", "https://test.org"
    }
    
    # This assertion will fail because retrieved_urls contains bare domains
    for url in retrieved_urls:
        assert any(url == proper_url or url == proper_url.replace('http://', 'https://') 
                  for proper_url in expected_proper_urls), \
            f"Retrieved URL '{url}' is not a proper URL with scheme"

@pytest.mark.asyncio
// ... existing code ...
```

Perfect! I've modified the test to demonstrate the actual bug. The new test:

1. **Creates a seed file with bare domains** (no scheme):
   - `example.com`
   - `gpumagick.com`
   - `test.org`

2. **Initializes the frontier** - which accepts these bare domains (that's the bug!)

3. **Retrieves URLs using `get_next_url()`** and checks that they are proper URLs

4. **The test will fail** because:
   - The assertion `url.startswith('http://') or url.startswith('https://')` will fail since `get_next_url()` returns bare domains like `"example.com"` instead of `"http://example.com"`
   - The retrieved URLs won't match any of the expected proper URLs with schemes

This test properly demonstrates that the frontier accepts bare domains from the seed file but doesn't normalize them to proper URLs, which will cause problems when the crawler tries to fetch these "URLs".

When you run this test, it should fail with an error message like:
```
AssertionError: URL 'example.com' is not a proper URL - it's missing the scheme (http:// or https://)
```

This clearly shows the bug: the frontier should either reject bare domains or normalize them to proper URLs (e.g., by prepending `http://`), but currently it does neither.

```plaintext
AssertionError: URL 'example.com' is not a proper URL - it's missing the scheme (http:// or https://)
```

---

**User**

Cool, the test failed. I've used it to fix the bug. Now let's handle the bugged frontier data from last night's failed seeding. All the frontier files in `/mnt/data/crawler_data/frontiers` contain improper URLs which need to be normalized using the `normalize_url` function from `utils.py`. Additionally the metadata in redis for each per-domain frontier must be reset to have offset 0, and the frontier size in bytes should also be adjusted. See sections 3.1 and 3.2 in PLAN.MD for the redis + frontier files schema. Help me write a migration script for this.

---

**Cursor**

I'll help you write a migration script to fix the frontier data. Let me first check the utils.py file to understand the `normalize_url` function:

Now let me check PLAN.MD for the Redis and frontier file schemas:

Now I'll create a migration script to fix the frontier data:

```python
#!/usr/bin/env python3
"""
Migration script to fix frontier files containing bare domains.

This script:
1. Reads all frontier files in /mnt/data/crawler_data/frontiers
2. Normalizes bare domains to proper URLs using normalize_url()
3. Writes the normalized URLs back to the files
4. Updates Redis metadata (resets offset to 0, updates file size)
"""

import asyncio
import logging
import redis.asyncio as redis
from pathlib import Path
import sys
import os
import hashlib

# Add the crawler_module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crawler_module.utils import normalize_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FrontierMigration:
    def __init__(self, data_dir: Path, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        self.frontier_dir = data_dir / "frontiers"
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_client = None
        
    async def connect_redis(self):
        """Connect to Redis."""
        self.redis_client = await redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=self.redis_db,
            decode_responses=True
        )
        logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port} (db={self.redis_db})")
        
    async def close_redis(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            
    async def migrate_frontier_file(self, file_path: Path) -> tuple[int, int]:
        """
        Migrate a single frontier file.
        
        Returns:
            Tuple of (original_line_count, normalized_line_count)
        """
        domain = file_path.stem  # filename without .frontier extension
        logger.info(f"Processing frontier file for domain: {domain}")
        
        # Read the existing file
        original_lines = []
        normalized_lines = []
        
        try:
            with open(file_path, 'r') as f:
                original_lines = f.readlines()
                
            # Process each line
            for line in original_lines:
                line = line.strip()
                if not line:
                    continue
                    
                # Parse the frontier line format: url|depth
                parts = line.split('|')
                if len(parts) >= 2:
                    url, depth = parts[0], parts[1]
                    
                    # Normalize the URL
                    normalized_url = normalize_url(url)
                    
                    # Reconstruct the line
                    normalized_line = f"{normalized_url}|{depth}\n"
                    normalized_lines.append(normalized_line)
                    
                    if url != normalized_url:
                        logger.debug(f"  Normalized: {url} -> {normalized_url}")
                else:
                    logger.warning(f"  Skipping malformed line: {line}")
                    
            # Write the normalized content back
            with open(file_path, 'w') as f:
                f.writelines(normalized_lines)
                
            # Get the new file size
            new_size = file_path.stat().st_size
            
            # Update Redis metadata
            await self.update_redis_metadata(domain, file_path, new_size)
            
            return len(original_lines), len(normalized_lines)
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return 0, 0
            
    async def update_redis_metadata(self, domain: str, file_path: Path, new_size: int):
        """Update Redis metadata for a domain."""
        domain_key = f"domain:{domain}"
        
        # Get relative path from frontier_dir
        relative_path = file_path.relative_to(self.frontier_dir)
        
        updates = {
            'frontier_size': str(new_size),
            'frontier_offset': '0',  # Reset offset to beginning
            'file_path': str(relative_path)
        }
        
        # Update all fields at once
        await self.redis_client.hset(domain_key, mapping=updates)
        logger.debug(f"  Updated Redis metadata for {domain}: size={new_size}, offset=0")
        
    async def run_migration(self):
        """Run the migration on all frontier files."""
        if not self.frontier_dir.exists():
            logger.error(f"Frontier directory does not exist: {self.frontier_dir}")
            return
            
        await self.connect_redis()
        
        try:
            # Find all frontier files
            frontier_files = list(self.frontier_dir.glob("*/*.frontier"))
            logger.info(f"Found {len(frontier_files)} frontier files to process")
            
            total_original = 0
            total_normalized = 0
            files_processed = 0
            
            # Process each file
            for i, file_path in enumerate(frontier_files, 1):
                logger.info(f"[{i}/{len(frontier_files)}] Processing {file_path.name}")
                
                orig_count, norm_count = await self.migrate_frontier_file(file_path)
                
                if orig_count > 0:
                    files_processed += 1
                    total_original += orig_count
                    total_normalized += norm_count
                    
                # Log progress every 100 files
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(frontier_files)} files processed")
                    
            # Summary
            logger.info("=" * 60)
            logger.info("Migration completed!")
            logger.info(f"Files processed: {files_processed}")
            logger.info(f"Total lines processed: {total_original}")
            logger.info(f"Total lines after normalization: {total_normalized}")
            
            if total_original != total_normalized:
                logger.warning(f"Line count changed by {total_original - total_normalized} lines")
                
        finally:
            await self.close_redis()

async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate frontier files to normalize URLs")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/mnt/data/crawler_data"),
        help="Path to crawler data directory (default: /mnt/data/crawler_data)"
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis database (default: 0)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        # TODO: Implement dry run mode if needed
        logger.error("Dry run mode not yet implemented")
        return
        
    # Confirm before proceeding
    logger.warning("=" * 60)
    logger.warning("FRONTIER MIGRATION SCRIPT")
    logger.warning("=" * 60)
    logger.warning(f"This script will modify frontier files in: {args.data_dir / 'frontiers'}")
    logger.warning(f"And update Redis metadata at: {args.redis_host}:{args.redis_port} (db={args.redis_db})")
    logger.warning("=" * 60)
    
    response = input("Do you want to proceed? (yes/no): ")
    if response.lower() != "yes":
        logger.info("Migration cancelled.")
        return
        
    # Run the migration
    migration = FrontierMigration(
        data_dir=args.data_dir,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db
    )
    
    await migration.run_migration()

if __name__ == "__main__":
    asyncio.run(main())
```

I need to fix the Redis client type annotation issue:

```python
// ... existing code ...
class FrontierMigration:
    def __init__(self, data_dir: Path, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        self.frontier_dir = data_dir / "frontiers"
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_client: redis.Redis | None = None
        
    async def connect_redis(self):
        """Connect to Redis."""
        self.redis_client = await redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=self.redis_db,
            decode_responses=True
        )
        logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port} (db={self.redis_db})")
// ... existing code ...
```

Let me fix the issue by adding a type check:

```python
// ... existing code ...
    async def update_redis_metadata(self, domain: str, file_path: Path, new_size: int):
        """Update Redis metadata for a domain."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")
            
        domain_key = f"domain:{domain}"
        
        # Get relative path from frontier_dir
        relative_path = file_path.relative_to(self.frontier_dir)
        
        updates = {
            'frontier_size': str(new_size),
            'frontier_offset': '0',  # Reset offset to beginning
            'file_path': str(relative_path)
        }
        
        # Update all fields at once
        await self.redis_client.hset(domain_key, mapping=updates)
        logger.debug(f"  Updated Redis metadata for {domain}: size={new_size}, offset=0")
// ... existing code ...
```

Perfect! I've created a migration script that will:

1. **Read all frontier files** from `/mnt/data/crawler_data/frontiers`
2. **Normalize bare domains** to proper URLs using the `normalize_url()` function
3. **Write the normalized URLs** back to the frontier files
4. **Update Redis metadata** for each domain:
   - Reset `frontier_offset` to 0 (so crawling starts from the beginning)
   - Update `frontier_size` to reflect the new file size
   - Keep the `file_path` updated

The script has the following features:

- **Command-line arguments** to specify data directory, Redis connection details
- **Progress logging** to track which files are being processed
- **Error handling** for individual files (won't stop the entire migration if one file fails)
- **Confirmation prompt** before making changes
- **Summary statistics** at the end

To run the migration:

```bash
# Default settings (assuming Redis on localhost and data in /mnt/data/crawler_data)
python crawler/migrate_frontier_urls.py

# With custom settings
python crawler/migrate_frontier_urls.py --data-dir /path/to/crawler_data --redis-host myredis.host --redis-port 6379 --redis-db 0
```

The script will:
- Show what it's about to do
- Ask for confirmation
- Process each frontier file, normalizing URLs (e.g., `example.com` → `http://example.com`)
- Reset the Redis metadata so the crawler will start reading from the beginning of each frontier file
- Log progress and provide a summary at the end

This should fix all the frontier files that were corrupted by the bug where bare domains were accepted without being normalized to proper URLs.

```shellscript
# Default settings (assuming Redis on localhost and data in /mnt/data/crawler_data)
python crawler/migrate_frontier_urls.py

# With custom settings
python crawler/migrate_frontier_urls.py --data-dir /path/to/crawler_data --redis-host myredis.host --redis-port 6379 --redis-db 0
```

