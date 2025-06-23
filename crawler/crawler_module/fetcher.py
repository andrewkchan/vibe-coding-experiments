import asyncio
import logging
import ssl
from dataclasses import dataclass
from typing import Optional

import aiohttp
from aiohttp import TraceConfig
import time
import cchardet # For fast character encoding detection

from .config import CrawlerConfig
from .metrics import fetch_counter, fetch_error_counter, fetch_timing_histogram

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

class Fetcher:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        # Standard timeout settings (can be made configurable)
        self.timeout = aiohttp.ClientTimeout(total=45, connect=10, sock_read=30, sock_connect=10)
        
        # Create trace config for detailed timing
        self.trace_config = TraceConfig()
        self._setup_tracing()
    
    def _setup_tracing(self):
        """Set up aiohttp tracing callbacks for detailed timing metrics."""
        async def on_request_start(session, trace_config_ctx, params):
            trace_config_ctx.start_time = time.time()
            trace_config_ctx.timings = {}
            trace_config_ctx.fetch_type = trace_config_ctx.trace_request_ctx.get('fetch_type', 'page')
        
        async def on_dns_resolvehost_start(session, trace_config_ctx, params):
            trace_config_ctx.dns_start = time.time()
        
        async def on_dns_resolvehost_end(session, trace_config_ctx, params):
            if hasattr(trace_config_ctx, 'dns_start'):
                dns_time = time.time() - trace_config_ctx.dns_start
                trace_config_ctx.timings['dns_lookup'] = dns_time
        
        async def on_connection_create_start(session, trace_config_ctx, params):
            trace_config_ctx.connect_start = time.time()
        
        async def on_connection_create_end(session, trace_config_ctx, params):
            if hasattr(trace_config_ctx, 'connect_start'):
                connect_time = time.time() - trace_config_ctx.connect_start
                trace_config_ctx.timings['connect'] = connect_time
        
        async def on_request_end(session, trace_config_ctx, params):
            if hasattr(trace_config_ctx, 'start_time'):
                total_time = time.time() - trace_config_ctx.start_time
                trace_config_ctx.timings['total'] = total_time
                
                # Record metrics
                fetch_type = trace_config_ctx.fetch_type
                for phase, duration in trace_config_ctx.timings.items():
                    fetch_timing_histogram.labels(phase=phase, fetch_type=fetch_type).observe(duration)
        
        self.trace_config.on_request_start.append(on_request_start)
        self.trace_config.on_dns_resolvehost_start.append(on_dns_resolvehost_start)
        self.trace_config.on_dns_resolvehost_end.append(on_dns_resolvehost_end)
        self.trace_config.on_connection_create_start.append(on_connection_create_start)
        self.trace_config.on_connection_create_end.append(on_connection_create_end)
        self.trace_config.on_request_end.append(on_request_end)

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
            # Optimize SSL settings for speed
            ssl_context.options |= ssl.OP_NO_SSLv2
            ssl_context.options |= ssl.OP_NO_SSLv3
            ssl_context.options |= ssl.OP_NO_COMPRESSION
            ssl_context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS')
            # Enable session caching (helps if hitting the same server)
            ssl_context.session_stats()  # Enable session statistics
            
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
                trace_configs=[self.trace_config],
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
        
        # Track fetch attempt
        fetch_type = 'robots_txt' if is_robots_txt else 'page'
        fetch_counter.labels(fetch_type=fetch_type).inc()
        
        # Create trace context with fetch type
        trace_request_ctx = {'fetch_type': fetch_type}

        try:
            async with session.get(url, allow_redirects=True, max_redirects=max_redirects, trace_request_ctx=trace_request_ctx) as response:
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
                    fetch_error_counter.labels(error_type=f'http_{status_code}', fetch_type=fetch_type).inc()
                    return FetchResult(
                        initial_url=url,
                        final_url=actual_final_url,
                        status_code=status_code,
                        content_type=content_type,
                        error_message=f"HTTP {status_code}",
                        is_redirect=is_redirect,
                    )

                return FetchResult(
                    initial_url=url,
                    final_url=actual_final_url,
                    status_code=status_code,
                    content_type=content_type,
                    content_bytes=content_bytes,
                    text_content=text_content,
                    is_redirect=is_redirect,
                )

        except aiohttp.ClientResponseError as e:
            logger.error(f"ClientResponseError for {url}: {e.message} (status: {e.status})", exc_info=False)
            fetch_error_counter.labels(error_type='client_response_error', fetch_type=fetch_type).inc()
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=e.status, error_message=str(e.message))
        except aiohttp.ClientConnectionError as e:
            logger.error(f"ClientConnectionError for {url}: {e}", exc_info=False)
            fetch_error_counter.labels(error_type='connection_error', fetch_type=fetch_type).inc()
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=901, error_message=f"Connection error: {e}") # Custom status for connection error
        except asyncio.TimeoutError:
            logger.error(f"Timeout for {url}", exc_info=False)
            fetch_error_counter.labels(error_type='timeout', fetch_type=fetch_type).inc()
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=902, error_message="Request timed out") # Custom status for timeout
        except Exception as e:
            logger.error(f"Generic error fetching {url}: {e}")
            fetch_error_counter.labels(error_type='generic_error', fetch_type=fetch_type).inc()
            return FetchResult(initial_url=url, final_url=actual_final_url, status_code=900, error_message=str(e)) # Custom status for other errors
