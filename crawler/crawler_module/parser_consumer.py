"""
Parser Consumer Process
Consumes raw HTML from Redis queue and performs parsing, content saving, and link extraction.
"""
import asyncio
import logging
import json
import time
from typing import Optional, Dict, Any, Set
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
active_parser_workers_gauge = Gauge('active_parser_workers', 'Number of active parser workers')

class ParserConsumer:
    """Consumes raw HTML from Redis queue and processes it."""
    
    def __init__(self, config: CrawlerConfig, num_workers: int = 50):
        self.config = config
        self.num_workers = num_workers
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
        self.worker_tasks: Set[asyncio.Task] = set()
        
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
    
    async def _worker(self, worker_id: int):
        """Worker coroutine that processes items from the queue."""
        logger.info(f"Parser worker {worker_id} starting...")
        
        consecutive_empty = 0
        max_consecutive_empty = 10  # Exit after 10 consecutive empty reads
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Use BLPOP for blocking pop with timeout
                    # Returns tuple of (queue_name, item) or None on timeout
                    result = await self.redis_client.blpop('fetch:queue', timeout=5)
                    
                    if result is None:
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            logger.debug(f"Worker {worker_id}: Queue empty for {max_consecutive_empty} consecutive reads")
                        continue
                    
                    consecutive_empty = 0  # Reset counter on successful pop
                    _, item_json = result
                    
                    # Parse JSON data
                    try:
                        item_data = json.loads(item_json)
                    except json.JSONDecodeError as e:
                        logger.error(f"Worker {worker_id}: Invalid JSON in queue: {e}")
                        parse_errors_counter.labels(error_type='json_decode').inc()
                        continue
                    
                    # Process the item
                    await self._process_item(item_data)
                    
                    # Yield to other workers
                    await asyncio.sleep(0)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error in worker loop: {e}", exc_info=True)
                    parse_errors_counter.labels(error_type='worker_loop').inc()
                    await asyncio.sleep(1)  # Brief pause on error
                    
        except asyncio.CancelledError:
            logger.info(f"Parser worker {worker_id} cancelled")
        finally:
            logger.info(f"Parser worker {worker_id} shutting down")
    
    async def run(self):
        """Main consumer loop."""
        logger.info(f"Parser consumer starting with {self.num_workers} workers...")
        
        try:
            # Don't re-initialize components - they're already initialized by the orchestrator
            # Just start the metrics server on a different port
            start_metrics_server(port=8002)
            logger.info("Parser metrics server started on port 8002")
            
            # Start worker tasks
            for i in range(self.num_workers):
                task = asyncio.create_task(self._worker(i + 1))
                self.worker_tasks.add(task)
            
            logger.info(f"Started {len(self.worker_tasks)} parser workers")
            
            # Monitor queue size periodically
            while not self._shutdown_event.is_set():
                # Update metrics
                queue_size = await self.redis_client.llen('fetch:queue')
                parse_queue_size_gauge.set(queue_size)
                
                active_workers = sum(1 for task in self.worker_tasks if not task.done())
                active_parser_workers_gauge.set(active_workers)
                
                # Check if all workers have exited
                if not any(not task.done() for task in self.worker_tasks):
                    logger.info("All parser workers have completed")
                    break
                
                # Wait a bit before next check
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass  # Continue monitoring
                
        except Exception as e:
            logger.critical(f"Parser consumer critical error: {e}", exc_info=True)
            self._shutdown_event.set()
        finally:
            logger.info("Parser consumer shutting down...")
            
            # Cancel all worker tasks
            for task in self.worker_tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for all tasks to complete
            if self.worker_tasks:
                await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            
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
    config = parse_args()
    
    # When run standalone, assume the developer has initialized Redis properly
    logger.info("Starting standalone parser consumer (assuming Redis is initialized)")
    
    consumer = ParserConsumer(config)
    await consumer.run()


if __name__ == '__main__':
    asyncio.run(main()) 