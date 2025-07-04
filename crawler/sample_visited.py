#!/usr/bin/env python3
"""
Sample visited URLs from a specific domain in the crawler's datastore.

Usage:
    python sample_visited.py <domain> [--sample-size N] [--show-content] [--analytics]
"""

import argparse
import hashlib
import json
import os
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import redis


class VisitedSampler:
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        """Initialize the sampler with Redis connection."""
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.content_dir = "crawler_data/content"
    
    def get_url_hash(self, url: str) -> str:
        """Generate SHA256 hash of a URL."""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return ""
    
    def scan_visited_urls(self, target_domain: str) -> List[Tuple[str, Dict]]:
        """Scan all visited URLs and filter by domain."""
        matching_urls = []
        
        print(f"Scanning visited URLs for domain: {target_domain}")
        
        # Use SCAN to iterate through keys matching visited:* pattern
        cursor = 0
        count = 0
        
        while True:
            cursor, keys = self.redis_client.scan(
                cursor=cursor,
                match="visited:*",
                count=1000  # Process in batches
            )
            
            for key in keys:
                count += 1
                if count % 1000 == 0:
                    print(f"  Scanned {count} URLs...", end='\r')
                
                # Get the hash data
                url_data = self.redis_client.hgetall(key)
                
                if url_data and 'url' in url_data:
                    url = url_data['url']
                    domain = self.extract_domain(url)
                    
                    if domain == target_domain.lower():
                        matching_urls.append((url, url_data))
            
            if cursor == 0:
                break
        
        print(f"\nFound {len(matching_urls)} URLs from {target_domain}")
        return matching_urls
    
    def sample_urls(self, urls: List[Tuple[str, Dict]], sample_size: Optional[int]) -> List[Tuple[str, Dict]]:
        """Sample N URLs from the list."""
        if sample_size is None or sample_size >= len(urls):
            return urls
        
        return random.sample(urls, sample_size)
    
    def read_content(self, content_path: Optional[str]) -> Optional[str]:
        """Read content from filesystem."""
        if not content_path:
            return None
        
        full_path = os.path.join(self.content_dir, content_path)
        
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"Error reading content: {e}"
    
    def display_url_info(self, url: str, data: Dict, show_content: bool = False):
        """Display information about a visited URL."""
        print(f"\n{'='*80}")
        print(f"URL: {url}")
        print(f"Status Code: {data.get('status_code', 'N/A')}")
        print(f"Content Type: {data.get('content_type', 'N/A')}")
        print(f"Timestamp: {data.get('timestamp', 'N/A')}")
        print(f"Content Hash: {data.get('content_hash', 'N/A')}")
        print(f"Content Path: {data.get('content_path', 'N/A')}")
        
        if show_content and data.get('content_path'):
            content = self.read_content(data.get('content_path'))
            if content:
                print(f"\nContent Preview (first 500 chars):")
                print("-" * 40)
                print(content[:500])
                if len(content) > 500:
                    print(f"... (truncated, total length: {len(content)} chars)")
    
    def compute_analytics(self, urls: List[Tuple[str, Dict]]):
        """Compute and display analytics for the sampled URLs."""
        print(f"\n{'='*80}")
        print("ANALYTICS")
        print(f"{'='*80}")
        
        status_codes = Counter()
        content_types = Counter()
        content_sizes = []
        timestamps = []
        
        for url, data in urls:
            # Status codes
            status_code = data.get('status_code', 'unknown')
            status_codes[status_code] += 1
            
            # Content types
            content_type = data.get('content_type', 'unknown')
            # Simplify content type (remove charset, etc.)
            if ';' in str(content_type):
                content_type = content_type.split(';')[0].strip()
            content_types[content_type] += 1
            
            # Content sizes
            if data.get('content_path'):
                content = self.read_content(data.get('content_path'))
                if content and isinstance(content, str) and not content.startswith("Error"):
                    content_sizes.append(len(content))
            
            # Timestamps
            timestamp = data.get('timestamp')
            if timestamp:
                try:
                    timestamps.append(float(timestamp))
                except:
                    pass
        
        # Display status code distribution
        print(f"\nStatus Code Distribution:")
        for status, count in status_codes.most_common():
            percentage = (count / len(urls)) * 100
            print(f"  {status}: {count} ({percentage:.1f}%)")
        
        # Display content type distribution
        print(f"\nContent Type Distribution:")
        for ctype, count in content_types.most_common(10):  # Top 10
            percentage = (count / len(urls)) * 100
            print(f"  {ctype}: {count} ({percentage:.1f}%)")
        
        # Display content size statistics
        if content_sizes:
            print(f"\nContent Size Statistics:")
            print(f"  Total files with content: {len(content_sizes)}")
            print(f"  Average size: {sum(content_sizes) / len(content_sizes):,.0f} chars")
            print(f"  Minimum size: {min(content_sizes):,} chars")
            print(f"  Maximum size: {max(content_sizes):,} chars")
            print(f"  Total size: {sum(content_sizes):,} chars")
            
            # Size distribution
            size_buckets = defaultdict(int)
            for size in content_sizes:
                if size < 1000:
                    size_buckets["< 1KB"] += 1
                elif size < 10000:
                    size_buckets["1KB - 10KB"] += 1
                elif size < 100000:
                    size_buckets["10KB - 100KB"] += 1
                elif size < 1000000:
                    size_buckets["100KB - 1MB"] += 1
                else:
                    size_buckets["> 1MB"] += 1
            
            print(f"\n  Size Distribution:")
            for bucket in ["< 1KB", "1KB - 10KB", "10KB - 100KB", "100KB - 1MB", "> 1MB"]:
                if bucket in size_buckets:
                    count = size_buckets[bucket]
                    percentage = (count / len(content_sizes)) * 100
                    print(f"    {bucket}: {count} ({percentage:.1f}%)")
        
        # Display crawl time statistics
        if timestamps:
            print(f"\nCrawl Time Statistics:")
            min_time = min(timestamps)
            max_time = max(timestamps)
            print(f"  First crawl: {datetime.fromtimestamp(min_time)}")
            print(f"  Last crawl: {datetime.fromtimestamp(max_time)}")
            print(f"  Time span: {(max_time - min_time) / 3600:.1f} hours")


def main():
    parser = argparse.ArgumentParser(
        description="Sample visited URLs from a specific domain in the crawler's datastore."
    )
    parser.add_argument("domain", help="Domain to sample URLs from (e.g., facebook.com)")
    parser.add_argument(
        "--sample-size", "-n",
        type=int,
        default=None,
        help="Number of URLs to sample (default: all)"
    )
    parser.add_argument(
        "--show-content", "-c",
        action="store_true",
        help="Show content preview for each URL"
    )
    parser.add_argument(
        "--analytics", "-a",
        action="store_true",
        help="Show analytics (size distribution, status codes, etc.)"
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
    
    args = parser.parse_args()
    
    # Initialize sampler
    sampler = VisitedSampler(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db
    )
    
    try:
        # Test Redis connection
        sampler.redis_client.ping()
    except redis.ConnectionError:
        print(f"Error: Could not connect to Redis at {args.redis_host}:{args.redis_port}")
        sys.exit(1)
    
    # Scan for URLs from the domain
    all_urls = sampler.scan_visited_urls(args.domain)
    
    if not all_urls:
        print(f"No visited URLs found for domain: {args.domain}")
        sys.exit(0)
    
    # Sample URLs
    sampled_urls = sampler.sample_urls(all_urls, args.sample_size)
    
    if args.sample_size and args.sample_size < len(all_urls):
        print(f"\nShowing {len(sampled_urls)} randomly sampled URLs out of {len(all_urls)} total")
    
    # Display individual URL information
    if not args.analytics or args.show_content:
        for url, data in sampled_urls:
            sampler.display_url_info(url, data, args.show_content)
    
    # Display analytics
    if args.analytics:
        sampler.compute_analytics(sampled_urls)


if __name__ == "__main__":
    main() 