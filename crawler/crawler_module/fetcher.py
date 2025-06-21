import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import aiohttp
import time
import cchardet # For fast character encoding detection
from aiohttp.client_reqrep import ClientResponse as AiohttpClientResponse # Correct import

from .config import CrawlerConfig

logger = logging.getLogger(__name__)

@dataclass
class FetchResult:
    initial_url: str
    final_url: str
    status_code: int
    content_type: Optional[str] = None
    content_bytes: Optional[bytes] = None
    text_content: Optional[str] = None
    error_message: Optional[str] = None
    is_redirect: bool = False
    redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple) # Corrected type hint

class Fetcher:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        # Standard timeout settings (can be made configurable)
        self.timeout = aiohttp.ClientTimeout(total=45, connect=10, sock_read=30, sock_connect=10)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Returns the existing aiohttp session or creates a new one."""
        if self.session is None or self.session.closed:
            # Create a connector with conservative limits for high worker count
            # With 500 workers, we need to be careful about file descriptors
            max_total_connections = min(1000, self.config.max_workers * 2)  # Scale with workers
            max_per_host = max(5, min(20, max_total_connections // 50))  # Conservative per-host limit
            
            # Create SSL context that doesn't verify certificates (similar to curl -k)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(
                limit=max_total_connections,  # Total connection pool limit
                limit_per_host=max_per_host,  # Max connections per host
                ttl_dns_cache=300,  # DNS cache timeout in seconds
                enable_cleanup_closed=True,  # Clean up closed connections
                force_close=True,  # Force close connections after each request to free up FDs
                ssl=ssl_context,  # Use our SSL context that ignores certificate errors
            )
            
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={"User-Agent": self.config.user_agent},
                connector=connector,
            )
            logger.debug(f"Created new aiohttp.ClientSession with limit={max_total_connections}, limit_per_host={max_per_host}, SSL verification disabled")
        return self.session

    async def close_session(self):
        """Closes the aiohttp session if it exists and is open."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("Closed aiohttp.ClientSession")
        self.session = None

    async def fetch_url(
        self, 
        url: str,
        is_robots_txt: bool = False,
        max_redirects: int = 5
    ) -> FetchResult:
        session = await self._get_session()
        actual_final_url = url # Will be updated upon redirects

        try:
            async with session.get(url, allow_redirects=True, max_redirects=max_redirects) as response:
                actual_final_url = str(response.url)
                content_bytes = await response.read() # Read content first
                status_code = response.status
                content_type = response.headers.get('Content-Type')
                
                text_content: Optional[str] = None
                if content_bytes and content_type and content_type.startswith('text/'):
                    # Try to decode using cchardet for speed and accuracy
                    try:
                        start_time = time.time()
                        detected_encoding = cchardet.detect(content_bytes)['encoding']
                        end_time = time.time()
                        if end_time - start_time > 0.5:
                            logger.warning(f"ENCODER: Encoding detection time: {end_time - start_time} seconds for content of size {len(content_bytes)} bytes")
                            logger.warning(f"ENCODER: Detected encoding: {detected_encoding}")
                            logger.warning(f"ENCODER: Content type: {content_type}")
                            logger.warning(f"ENCODER: URL: {actual_final_url}")
                        if detected_encoding:
                            text_content = content_bytes.decode(detected_encoding, errors='replace')
                        else:
                            # Fallback to aiohttp's guessed encoding or utf-8
                            text_content = await response.text(errors='replace') # response.text() re-reads if not careful
                    except (UnicodeDecodeError, LookupError, TypeError) as e:
                        logger.warning(f"Encoding detection/decoding error for {actual_final_url}: {e}. Falling back.")
                        try:
                            # Fallback to requests library style (chardet, then utf-8)
                            text_content = await response.text(errors='replace') 
                        except Exception as ex_inner:
                             logger.error(f"Final fallback decoding error for {actual_final_url}: {ex_inner}")
                             text_content = "DECODING_ERROR"
                
                is_redirect = len(response.history) > 0

                if status_code >= 400:
                    logger.warning(f"HTTP error {status_code} for {actual_final_url} (from {url})")
                    return FetchResult(
                        initial_url=url,
                        final_url=actual_final_url,
                        status_code=status_code,
                        content_type=content_type,
                        error_message=f"HTTP {status_code}",
                        is_redirect=is_redirect,
                        redirect_history=response.history
                    )

                return FetchResult(
                    initial_url=url,
                    final_url=actual_final_url,
                    status_code=status_code,
                    content_type=content_type,
                    content_bytes=content_bytes,
                    text_content=text_content,
                    is_redirect=is_redirect,
                    redirect_history=response.history
                )

        except aiohttp.ClientResponseError as e:
            logger.error(f"ClientResponseError for {url}: {e.message} (status: {e.status})", exc_info=False)
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=e.status, error_message=str(e.message), redirect_history=e.history if hasattr(e, 'history') else tuple())
        except aiohttp.ClientConnectionError as e:
            logger.error(f"ClientConnectionError for {url}: {e}", exc_info=False)
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=901, error_message=f"Connection error: {e}") # Custom status for connection error
        except asyncio.TimeoutError:
            logger.error(f"Timeout for {url}", exc_info=False)
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=902, error_message="Request timed out") # Custom status for timeout
        except Exception as e:
            logger.error(f"Generic error fetching {url}: {e}", exc_info=True)
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=900, error_message=str(e)) # Custom status for other errors

# Example Usage (for testing and illustration)
if __name__ == "__main__":
    from pathlib import Path
    from .config import parse_args # Assuming parse_args can be run without actual args for a default config

    # A simplified config for testing the fetcher directly
    @dataclass
    class DummyFetcherConfig:
        user_agent: str = "FetcherTest/1.0 (mailto:test@example.com)"
        # Add other fields if CrawlerConfig is more complex and fetcher needs them
        seed_file: Path = Path("dummy_seeds.txt") 
        email: str = "test@example.com"
        data_dir: Path = Path("./test_fetcher_data")
        exclude_file: Path | None = None
        max_workers: int = 1
        max_pages: int | None = None
        max_duration: int | None = None
        log_level: str = "DEBUG"
        resume: bool = False

    async def run_fetch_test():
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # test_config = parse_args() # This would require command line args or mocking
        test_config = DummyFetcherConfig() # Use the simpler dummy for direct testing
        
        fetcher = Fetcher(config=test_config) # type: ignore

        urls_to_test = [
            "http://httpbin.org/html", 
            "http://httpbin.org/encoding/utf8",
            "http://httpbin.org/status/404",
            "http://httpbin.org/status/500",
            "http://httpbin.org/redirect/3", # Test redirects
            "http://example.com/nonexistentpageforsure", # Should be a connection error or 404 depending on server
            "https://jigsaw.w3.org/HTTP/ChunkedScript" # Chunked encoding test
        ]

        for test_url in urls_to_test:
            print(f"\n--- Fetching: {test_url} ---")
            result = await fetcher.fetch_url(test_url)
            print(f"Initial URL: {result.initial_url}")
            print(f"Final URL:   {result.final_url}")
            print(f"Status Code: {result.status_code}")
            print(f"Content-Type: {result.content_type}")
            print(f"Is Redirect: {result.is_redirect}")
            if result.redirect_history:
                print(f"Redirect History: {[(h.method, str(h.url)) for h in result.redirect_history]}")
            if result.error_message:
                print(f"Error: {result.error_message}")
            # print(f"Text Content Preview: {result.text_content[:200] if result.text_content else 'N/A'}...")
            print("-" * 30)

        await fetcher.close_session()

    asyncio.run(run_fetch_test()) 