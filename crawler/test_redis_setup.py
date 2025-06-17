#!/usr/bin/env python3
"""
Test script for Redis setup and bloom filter performance.
This verifies that Redis is properly configured for the hybrid crawler architecture.
"""

import redis
import time
import hashlib
import sys
from datetime import datetime

def test_redis_connection():
    """Test basic Redis connectivity."""
    print("Testing Redis connection...")
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        pong = r.ping()
        print(f"✓ Redis connection successful: {pong}")
        
        # Get Redis info
        info = r.info()
        redis_version = info['redis_version']
        print(f"✓ Redis version: {redis_version}")
        print(f"✓ Memory used: {info['used_memory_human']}")
        
        # Check Redis major version
        major_version = int(redis_version.split('.')[0])
        
        if major_version >= 8:
            print(f"✓ Redis {major_version}.x detected - bloom filters are built-in!")
            return True
        else:
            # For Redis 7.x, check if RedisBloom module is loaded
            modules = r.module_list()
            bloom_loaded = any(mod[b'name'] == b'bf' for mod in modules)
            if bloom_loaded:
                print("✓ RedisBloom module is loaded (Redis 7.x)")
            else:
                print(f"⚠ Redis {redis_version} requires RedisBloom module for bloom filters")
                print("  Consider upgrading to Redis 8+ for native bloom filter support")
            return True  # Connection is still successful
            
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        return False


def test_bloom_filter_operations():
    """Test bloom filter creation and operations."""
    print("\nTesting Bloom Filter operations...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    try:
        # Delete test bloom filter if it exists
        r.delete('test:bloom')
        
        # Create a bloom filter for testing
        # 0.001 false positive rate, 1M capacity
        r.execute_command('BF.RESERVE', 'test:bloom', '0.001', '1000000')
        print("✓ Created test bloom filter (1M capacity, 0.1% FPR)")
        
        # Test adding and checking items
        test_urls = [
            "https://example.com/page1",
            "https://example.com/page2",
            "https://test.org/article",
            "https://demo.net/index"
        ]
        
        for url in test_urls:
            r.execute_command('BF.ADD', 'test:bloom', url)
        print(f"✓ Added {len(test_urls)} test URLs")
        
        # Check if items exist
        for url in test_urls:
            exists = r.execute_command('BF.EXISTS', 'test:bloom', url)
            assert exists == 1, f"URL {url} should exist in bloom filter"
        print("✓ All test URLs found in bloom filter")
        
        # Check non-existent item
        not_exists = r.execute_command('BF.EXISTS', 'test:bloom', 'https://notinfilter.com')
        assert not_exists == 0, "Non-existent URL should not be in bloom filter"
        print("✓ Non-existent URL correctly not found")
        
        # Clean up
        r.delete('test:bloom')
        
        return True
    except Exception as e:
        print(f"✗ Bloom filter test failed: {e}")
        return False


def test_bloom_filter_performance():
    """Test bloom filter performance with crawler-scale data."""
    print("\nTesting Bloom Filter performance at scale...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    try:
        # Delete test bloom filter if it exists
        r.delete('perf:bloom')
        
        # Create a larger bloom filter (10M capacity for performance testing)
        print("Creating bloom filter with 10M capacity...")
        r.execute_command('BF.RESERVE', 'perf:bloom', '0.001', '10000000')
        
        # Test adding URLs in batches
        batch_size = 10000
        num_batches = 10
        
        print(f"Adding {batch_size * num_batches:,} URLs in {num_batches} batches...")
        
        total_add_time = 0
        for batch in range(num_batches):
            urls = []
            for i in range(batch_size):
                # Generate unique URLs
                url = f"https://domain{batch}.com/page{i}/article"
                urls.append(url)
            
            # Time the batch add
            start = time.time()
            # Use pipeline for batch operations
            pipe = r.pipeline()
            for url in urls:
                pipe.execute_command('BF.ADD', 'perf:bloom', url)
            pipe.execute()
            elapsed = time.time() - start
            total_add_time += elapsed
            
            print(f"  Batch {batch + 1}: {batch_size:,} URLs in {elapsed:.3f}s "
                  f"({batch_size/elapsed:.0f} URLs/sec)")
        
        avg_rate = (batch_size * num_batches) / total_add_time
        print(f"✓ Average insertion rate: {avg_rate:,.0f} URLs/sec")
        
        # Test lookup performance
        print("\nTesting lookup performance...")
        test_lookups = 100000
        lookup_urls = [f"https://domain5.com/page{i}/article" for i in range(1000)]
        
        start = time.time()
        for _ in range(test_lookups // 1000):
            for url in lookup_urls:
                r.execute_command('BF.EXISTS', 'perf:bloom', url)
        elapsed = time.time() - start
        
        lookup_rate = test_lookups / elapsed
        print(f"✓ Lookup rate: {lookup_rate:,.0f} lookups/sec")
        
        # Get bloom filter info
        info = r.execute_command('BF.INFO', 'perf:bloom')
        info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
        print(f"\nBloom filter stats:")
        print(f"  Capacity: {info_dict.get(b'Capacity', 'N/A')}")
        print(f"  Size (bytes): {info_dict.get(b'Size', 'N/A')}")
        print(f"  Number of filters: {info_dict.get(b'Number of filters', 'N/A')}")
        
        # Clean up
        r.delete('perf:bloom')
        
        return True
    except Exception as e:
        print(f"✗ Performance test failed: {e}")
        return False


def test_crawler_data_structures():
    """Test the data structures planned for the crawler."""
    print("\nTesting crawler data structures...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    try:
        # Test domain metadata hash
        r.hset('domain:example.com', mapping={
            'frontier_offset': 0,
            'frontier_size': 1000,
            'file_path': 'ex/example.com.frontier',
            'next_fetch_time': int(time.time()),
            'robots_txt': 'User-agent: *\nDisallow: /private',
            'robots_expires': int(time.time()) + 86400,
            'is_excluded': 0
        })
        print("✓ Created domain metadata hash")
        
        # Test domain ready queue (sorted set)
        domains = ['example.com', 'test.org', 'demo.net']
        for i, domain in enumerate(domains):
            r.zadd('domains:ready', {domain: time.time() + i * 10})
        print("✓ Created domain ready queue")
        
        # Test visited URL hash
        url = "https://example.com/page1"
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        r.hset(f'visited:{url_hash}', mapping={
            'url': url,
            'status_code': 200,
            'fetched_at': int(time.time()),
            'content_path': 'content/a5/b7c9d2e4f6.txt',
            'error': ''
        })
        print("✓ Created visited URL record")
        
        # Test active domains set
        r.sadd('domains:active', 'example.com', 'test.org')
        print("✓ Created active domains set")
        
        # Verify data
        domain_data = r.hgetall('domain:example.com')
        assert domain_data['frontier_size'] == '1000'
        print("✓ Domain metadata verified")
        
        ready_domains = r.zrange('domains:ready', 0, -1)
        assert len(ready_domains) == 3
        print("✓ Domain ready queue verified")
        
        # Clean up test data
        r.delete('domain:example.com', 'domains:ready', f'visited:{url_hash}', 'domains:active')
        
        return True
    except Exception as e:
        print(f"✗ Data structure test failed: {e}")
        return False


def estimate_memory_usage():
    """Estimate memory usage for the full crawler scale."""
    print("\nMemory usage estimates for production scale:")
    print("=" * 50)
    
    # Based on the architecture document
    estimates = {
        "Domain metadata (1M domains)": "500 MB",
        "Domain ready queue (1M domains)": "50 MB", 
        "Seen bloom filter (160M URLs, 0.1% FPR)": "2 GB",
        "Visited URL hashes (7.5M URLs)": "1.5 GB",
        "Visited time index (7.5M URLs)": "375 MB",
        "Active domains set (~10K domains)": "500 KB",
        "Redis overhead & buffers": "~500 MB"
    }
    
    total_mb = 0
    for component, size in estimates.items():
        if size[0] == "~":
            size = size[1:]
        print(f"  {component:<40} {size:>10}")
        # Extract numeric value for total
        if "GB" in size:
            mb = float(size.split()[0]) * 1024
        elif "MB" in size:
            mb = float(size.split()[0])
        elif "KB" in size:
            mb = float(size.split()[0]) / 1024
        else:
            mb = 0
        total_mb += mb
    
    print("  " + "-" * 50)
    print(f"  {'Total estimated:':<40} {f'~{total_mb/1024:.1f} GB':>10}")
    print("\nNote: Actual usage may vary based on Redis internals and data patterns")


def main():
    """Run all tests."""
    print("Redis Setup Test Suite for Web Crawler")
    print("=" * 50)
    
    tests = [
        ("Redis Connection", test_redis_connection),
        ("Bloom Filter Operations", test_bloom_filter_operations),
        ("Bloom Filter Performance", test_bloom_filter_performance),
        ("Crawler Data Structures", test_crawler_data_structures)
    ]
    
    all_passed = True
    for test_name, test_func in tests:
        if not test_func():
            all_passed = False
            print(f"\n{test_name} test failed!")
            break
    
    if all_passed:
        print("\n✓ All tests passed!")
        estimate_memory_usage()
        
        print("\n" + "=" * 50)
        print("Redis is ready for the hybrid crawler architecture!")
        print("\nNext steps:")
        print("1. Run the setup script: ./setup_redis.sh")
        print("2. Initialize production bloom filter:")
        print("   redis-cli BF.RESERVE seen:bloom 0.001 160000000")
        print("3. Start implementing the HybridFrontierManager")
    else:
        print("\n✗ Some tests failed. Please check your Redis installation.")
        sys.exit(1)


if __name__ == "__main__":
    main() 