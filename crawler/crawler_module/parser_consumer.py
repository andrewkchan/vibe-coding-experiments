"""
Parser Consumer Process
Consumes raw HTML from Redis queue and performs parsing, content saving, and link extraction.
"""
import asyncio
import logging
import json
import time
from typing import Optional, Dict, Any
from pathlib import Path
import signal
import sys

import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge

from .config import CrawlerConfig, parse_args
from .parser import PageParser
from .redis_storage import RedisStorageManager
from .frontier import HybridFrontierManager
from .politeness import RedisPolitenessEnforcer
from .fetcher import Fetcher
from .utils import extract_domain
from .metrics import start_metrics_server

logger = logging.getLogger(__name__)

# Metrics
parse_queue_size_gauge = Gauge('parse_queue_size', 'Number of items in parse queue')
parse_duration_histogram = Histogram('parse_duration_seconds', 'Time to parse HTML')
parse_processed_counter = Counter('parse_processed_total', 'Total parsed pages')
parse_errors_counter = Counter('parse_errors_total', 'Total parsing errors', ['error_type'])

class ParserConsumer:
    """Consumes raw HTML from Redis queue and processes it."""
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.parser = PageParser()
        
        # Initialize storage and frontier for adding URLs
        self.storage = RedisStorageManager(config, self.redis_client)
        # Need fetcher for politeness enforcer
        self.fetcher = Fetcher(config)
        self.politeness = RedisPolitenessEnforcer(config, self.redis_client, self.fetcher)
        self.frontier = HybridFrontierManager(
            config, 
            self.politeness,  # type: ignore[arg-type]
            self.redis_client
        )
        
        self._shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._shutdown_event.set()
            
        # Only setup signal handlers if we're not a child process
        # (child processes should respond to parent's signals)
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except ValueError:
            # Can happen in some contexts, ignore
            pass
    
    async def _process_item(self, item_data: Dict[str, Any]) -> None:
        """Process a single item from the queue."""
        try:
            # Extract data from queue item
            url = item_data['url']
            domain = item_data['domain']
            depth = item_data['depth']
            html_content = item_data['html_content']
            content_type = item_data.get('content_type', 'text/html')
            crawled_timestamp = item_data['crawled_timestamp']
            status_code = item_data['status_code']
            is_redirect = item_data.get('is_redirect', False)
            initial_url = item_data.get('initial_url', url)
            
            # Track parsing time
            parse_start = time.time()
            
            # Parse HTML
            parse_result = self.parser.parse_html_content(html_content, url)
            
            parse_duration = time.time() - parse_start
            parse_duration_histogram.observe(parse_duration)
            
            content_storage_path_str: Optional[str] = None
            
            # Save parsed text content if available
            if parse_result.text_content:
                url_hash = self.storage.get_url_sha256(url)
                saved_path = await self.storage.save_content_to_file(url_hash, parse_result.text_content)
                if saved_path:
                    # Store relative path from data_dir for portability
                    try:
                        content_storage_path_str = str(saved_path.relative_to(self.config.data_dir))
                    except ValueError:
                        content_storage_path_str = str(saved_path)
                    logger.debug(f"Saved content for {url} to {content_storage_path_str}")
            
            # Add extracted links to frontier
            links_added = 0
            if parse_result.extracted_links:
                logger.debug(f"Found {len(parse_result.extracted_links)} links on {url}")
                links_added = await self.frontier.add_urls_batch(
                    list(parse_result.extracted_links), 
                    depth=depth + 1
                )
                if links_added > 0:
                    logger.info(f"Added {links_added} new URLs to frontier from {url}")
                    # Track URLs added in Redis for the orchestrator to read
                    await self.redis_client.incrby('stats:urls_added', links_added)
            
            # Record the visited page with all metadata
            await self.storage.add_visited_page(
                url=url,
                domain=extract_domain(url) or domain,
                status_code=status_code,
                crawled_timestamp=crawled_timestamp,
                content_type=content_type,
                content_text=parse_result.text_content,  # For hashing
                content_storage_path_str=content_storage_path_str,
                redirected_to_url=url if is_redirect and initial_url != url else None
            )
            
            parse_processed_counter.inc()
            logger.info(f"Successfully processed {url}: saved={bool(content_storage_path_str)}, links_added={links_added}")
            
        except Exception as e:
            logger.error(f"Error processing item: {e}", exc_info=True)
            parse_errors_counter.labels(error_type=type(e).__name__).inc()
    
    async def run(self):
        """Main consumer loop."""
        logger.info("Parser consumer starting...")
        
        try:
            # Initialize components
            await self.storage.init_db_schema()
            if hasattr(self.politeness, '_load_manual_exclusions'):
                await self.politeness._load_manual_exclusions()
            await self.frontier.initialize_frontier()
            
            # Start metrics server on different port than main crawler
            start_metrics_server(port=8002)
            logger.info("Parser metrics server started on port 8002")
            
            consecutive_empty = 0
            max_consecutive_empty = 10  # Exit after 10 consecutive empty reads
            
            while not self._shutdown_event.is_set():
                try:
                    # Use BLPOP for blocking pop with timeout
                    # Returns tuple of (queue_name, item) or None on timeout
                    result = await self.redis_client.blpop('fetch:queue', timeout=5)
                    
                    if result is None:
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            logger.info(f"Queue empty for {max_consecutive_empty} consecutive reads, checking if crawl is done...")
                            # Check if main crawler is still running by looking at active workers
                            # This is a simple heuristic - you might want a more sophisticated check
                            queue_size = await self.redis_client.llen('fetch:queue')
                            if queue_size == 0:
                                logger.info("Queue confirmed empty and no new items arriving, shutting down...")
                                break
                        continue
                    
                    consecutive_empty = 0  # Reset counter on successful pop
                    _, item_json = result
                    
                    # Update queue size metric
                    queue_size = await self.redis_client.llen('fetch:queue')
                    parse_queue_size_gauge.set(queue_size)
                    
                    # Parse JSON data
                    try:
                        item_data = json.loads(item_json)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in queue: {e}")
                        parse_errors_counter.labels(error_type='json_decode').inc()
                        continue
                    
                    # Process the item
                    await self._process_item(item_data)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}", exc_info=True)
                    parse_errors_counter.labels(error_type='consumer_loop').inc()
                    await asyncio.sleep(1)  # Brief pause on error
                    
        except Exception as e:
            logger.critical(f"Parser consumer critical error: {e}", exc_info=True)
        finally:
            logger.info("Parser consumer shutting down...")
            await self.fetcher.close_session()
            await self.storage.close()
            await self.redis_client.aclose()
            logger.info("Parser consumer shutdown complete.")


async def main():
    """Entry point for parser consumer process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    config: CrawlerConfig = parse_args()
    
    consumer = ParserConsumer(config)
    await consumer.run()


if __name__ == '__main__':
    asyncio.run(main()) 