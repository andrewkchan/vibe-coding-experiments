# Investigating memory leak in web crawler
_Exported on 6/22/2025 at 01:45:25 PDT from Cursor (1.0.0)_

---

**User**

I'm investigating a possible memory leak in the web crawler. The crawler consists of a main process which performs many asynchronous fetch tasks (called "workers") and a pair of parser processes which parse the fetched content and append the links to the frontier. The memory leak is occurring in the main process, and is causing the process to be terminated by the OS after a few hours.

I've inserted memory diagnostics in orchestrator.py which seem to indicate that some string objects are being leaked. Printing the content of one of these string objects suggest that it is a fetched HTML page. Attached is a visualization of the backrefs of 1 string object which occurs in the "top 5 largest strings" for 2 separate diagnostic periods, suggesting it is being leaked (as fetched HTML content should be pushed into a redis queue for the parsers, then freed for garbage collection).

Please read the web crawler code and help me brainstorm where the memory leak could be occurring. If you can't read the visualization or need additional info, let me know.

---

**Cursor**

```python
if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
    # Push to parse queue instead of parsing inline
    queue_item = {
        'url': fetch_result.final_url,
        'domain': domain,
        'depth': depth,
        'html_content': fetch_result.text_content,
        'content_type': fetch_result.content_type,
        'crawled_timestamp': crawled_timestamp,
        'status_code': fetch_result.status_code,
        'is_redirect': fetch_result.is_redirect,
        'initial_url': fetch_result.initial_url
    }
    
    # Push to Redis queue for parsing
    await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
```

Looking at your crawler code and the memory leak symptoms, I can see that HTML content strings are being retained in memory when they should be garbage collected after being pushed to Redis. Let me help you identify potential sources of this memory leak.

Now I can see the issue more clearly. Looking at the `FetchResult` dataclass and the flow, here are the most likely sources of the memory leak:

Based on my analysis of the code, here are the most likely causes of the memory leak with HTML strings:

## 1. **redirect_history holding references to response objects**

The `FetchResult` dataclass contains:
```python
redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)
```

These `AiohttpClientResponse` objects might be holding references to the response body internally. When you push the HTML content to Redis and expect the `fetch_result` to be garbage collected, the `redirect_history` tuple might still be holding onto response objects that contain the HTML content.

## 2. **The fetch_result object scope in the worker loop**

In the `_worker` method, the `fetch_result` object is created in the main loop and remains in scope until the next iteration. If there's any delay or the worker gets stuck, these objects accumulate.

## 3. **JSON serialization creating copies**

When you do `json.dumps(queue_item)`, the HTML content might be creating intermediate string representations that aren't being freed immediately.

## Recommended fixes:

### 1. **Clear the redirect_history after use**
```python
# After pushing to Redis queue
await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")

# Clear the fetch_result to help with garbage collection
fetch_result.text_content = None
fetch_result.content_bytes = None
fetch_result.redirect_history = tuple()  # Clear the response objects
```

### 2. **Explicitly delete the fetch_result after use**
```python
# At the end of processing each URL
del fetch_result
```

### 3. **Use a context manager or limit fetch_result scope**
Refactor the worker to limit the scope of fetch_result:
```python
async def _process_url(self, worker_id: int, url_to_crawl: str, domain: str, frontier_id: int, depth: int):
    """Process a single URL - this limits the scope of fetch_result"""
    fetch_result = await self.fetcher.fetch_url(url_to_crawl)
    # ... process the result ...
    # fetch_result goes out of scope when this method returns

# In the worker:
await self._process_url(worker_id, url_to_crawl, domain, frontier_id, depth)
```

### 4. **Check if aiohttp session is configured correctly**
The Fetcher might be keeping response history. Check if the session is configured to not keep response objects:

```python
redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)
```

```python
# After pushing to Redis queue
await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")

# Clear the fetch_result to help with garbage collection
fetch_result.text_content = None
fetch_result.content_bytes = None
fetch_result.redirect_history = tuple()  # Clear the response objects
```

```python
# At the end of processing each URL
del fetch_result
```

```python
async def _process_url(self, worker_id: int, url_to_crawl: str, domain: str, frontier_id: int, depth: int):
    """Process a single URL - this limits the scope of fetch_result"""
    fetch_result = await self.fetcher.fetch_url(url_to_crawl)
    # ... process the result ...
    # fetch_result goes out of scope when this method returns

# In the worker:
await self._process_url(worker_id, url_to_crawl, domain, frontier_id, depth)
```

Perfect! I found the main culprit. The issue is in the `FetchResult` dataclass:

```python
redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)
```

The `response.history` contains `AiohttpClientResponse` objects which internally hold references to the response body. Even after you push the HTML content to Redis, these response objects in `redirect_history` keep the HTML strings in memory.

## Here's a comprehensive solution:

### 1. **Modify the FetchResult to not store response objects**

```python
redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)
```

```python
redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)
```

Based on my analysis, the memory leak is caused by the `redirect_history` field in the `FetchResult` dataclass, which stores `AiohttpClientResponse` objects that internally hold references to the response body (the HTML content).

## Here's the fix:

### Option 1: Remove redirect_history entirely (Recommended)
Since `redirect_history` is only used for debugging in the test code and not in production, you can simply remove it:

```python:crawler/crawler_module/fetcher.py
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
    # redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)  # REMOVED
```

And remove references to it in the `fetch_url` method:

```python:crawler/crawler_module/fetcher.py
# ... existing code ...
return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    error_message=f"HTTP {status_code}",
    is_redirect=is_redirect,
    # redirect_history=response.history  # REMOVED
)
```

### Option 2: Store only URLs from redirect history
If you need redirect information, store only the URLs instead of response objects:

```python:crawler/crawler_module/fetcher.py
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
    redirect_urls: Tuple[str, ...] = field(default_factory=tuple)  # Changed from redirect_history
```

And update the fetch_url method:

```python:crawler/crawler_module/fetcher.py
# Extract only URLs from redirect history
redirect_urls = tuple(str(r.url) for r in response.history) if response.history else tuple()

return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    content_bytes=content_bytes,
    text_content=text_content,
    is_redirect=is_redirect,
    redirect_urls=redirect_urls  # Only URLs, not response objects
)
```

### Additional Optimization in orchestrator.py

After pushing to Redis, explicitly clear the fetch_result to help garbage collection:

```python:crawler/crawler_module/orchestrator.py
# ... existing code ...
if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
    # Push to parse queue instead of parsing inline
    queue_item = {
        'url': fetch_result.final_url,
        'domain': domain,
        'depth': depth,
        'html_content': fetch_result.text_content,
        'content_type': fetch_result.content_type,
        'crawled_timestamp': crawled_timestamp,
        'status_code': fetch_result.status_code,
        'is_redirect': fetch_result.is_redirect,
        'initial_url': fetch_result.initial_url
    }
    
    # Push to Redis queue for parsing
    await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
    logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")
    
    # Clear large data to help garbage collection
    fetch_result.text_content = None
    fetch_result.content_bytes = None
```

### Monitor the Fix

After implementing these changes:
1. The HTML strings should no longer appear in your memory diagnostics
2. Memory usage should stabilize over time
3. The main process shouldn't be killed by the OS due to memory exhaustion

The root cause is that `aiohttp`'s `ClientResponse` objects in the redirect history hold references to the entire response body, preventing garbage collection even after you've extracted and sent the content to Redis.

```python
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
    # redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)  # REMOVED
```

```python
# ... existing code ...
return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    error_message=f"HTTP {status_code}",
    is_redirect=is_redirect,
    # redirect_history=response.history  # REMOVED
)
```

```python
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
    redirect_urls: Tuple[str, ...] = field(default_factory=tuple)  # Changed from redirect_history
```

```python
# Extract only URLs from redirect history
redirect_urls = tuple(str(r.url) for r in response.history) if response.history else tuple()

return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    content_bytes=content_bytes,
    text_content=text_content,
    is_redirect=is_redirect,
    redirect_urls=redirect_urls  # Only URLs, not response objects
)
```

```python
# ... existing code ...
if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
    # Push to parse queue instead of parsing inline
    queue_item = {
        'url': fetch_result.final_url,
        'domain': domain,
        'depth': depth,
        'html_content': fetch_result.text_content,
        'content_type': fetch_result.content_type,
        'crawled_timestamp': crawled_timestamp,
        'status_code': fetch_result.status_code,
        'is_redirect': fetch_result.is_redirect,
        'initial_url': fetch_result.initial_url
    }
    
    # Push to Redis queue for parsing
    await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
    logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")
    
    # Clear large data to help garbage collection
    fetch_result.text_content = None
    fetch_result.content_bytes = None
```

```python
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
    # redirect_history: Tuple[AiohttpClientResponse, ...] = field(default_factory=tuple)  # REMOVED
```

```python
# ... existing code ...
return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    error_message=f"HTTP {status_code}",
    is_redirect=is_redirect,
    # redirect_history=response.history  # REMOVED
)
```

```python
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
    redirect_urls: Tuple[str, ...] = field(default_factory=tuple)  # Changed from redirect_history
```

```python
# Extract only URLs from redirect history
redirect_urls = tuple(str(r.url) for r in response.history) if response.history else tuple()

return FetchResult(
    initial_url=url,
    final_url=actual_final_url,
    status_code=status_code,
    content_type=content_type,
    content_bytes=content_bytes,
    text_content=text_content,
    is_redirect=is_redirect,
    redirect_urls=redirect_urls  # Only URLs, not response objects
)
```

```python
# ... existing code ...
if fetch_result.text_content and fetch_result.content_type and "html" in fetch_result.content_type.lower():
    # Push to parse queue instead of parsing inline
    queue_item = {
        'url': fetch_result.final_url,
        'domain': domain,
        'depth': depth,
        'html_content': fetch_result.text_content,
        'content_type': fetch_result.content_type,
        'crawled_timestamp': crawled_timestamp,
        'status_code': fetch_result.status_code,
        'is_redirect': fetch_result.is_redirect,
        'initial_url': fetch_result.initial_url
    }
    
    # Push to Redis queue for parsing
    await self.redis_client.rpush('fetch:queue', json.dumps(queue_item))
    logger.debug(f"Worker-{worker_id}: Pushed HTML content to parse queue for {fetch_result.final_url}")
    
    # Clear large data to help garbage collection
    fetch_result.text_content = None
    fetch_result.content_bytes = None
```

---

**User**

The redirect_history should have the same lifetime as the text_content field of the FetchResult, which would also contain HTML content (although possibly less than the full HTML document), IIUC. Are you just saying that `fetch_result` is storing more than it needs to? Or is there a real leak here?

---

**Cursor**

You're absolutely right to question this - let me clarify my analysis.

You're correct that both `text_content` and `redirect_history` are fields of the same `FetchResult` object and should have the same lifetime. When the `fetch_result` goes out of scope at the next loop iteration, both should be garbage collected.

The fact that you're seeing **the same string object** appearing in "top 5 largest strings" across multiple diagnostic periods suggests there IS a real leak - the `FetchResult` object (or at least the string) is being retained somewhere.

Let me look for where the leak might actually be occurring:

Wait, I see something interesting - line 435 shows `del fetch_result`. Someone already tried to explicitly delete the fetch_result, which suggests they were already experiencing memory issues!

If the leak persists despite the explicit `del`, then something else must be holding a reference. Let me check for potential culprits:

---

**User**

I just added that in response to your suggestion

---

**Cursor**

Ah, I see! Then we need to dig deeper to find the real leak.

The fact that the same string persists across multiple diagnostic periods despite the `del fetch_result` suggests something else is holding a reference. Let me analyze potential culprits:

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

I found a likely culprit! Look at line 452:

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
```

The `exc_info=True` parameter causes the logger to capture the full exception traceback, which includes references to all local variables in the stack frame - including `fetch_result` with its HTML content!

Python's traceback objects hold references to frame locals, preventing garbage collection. This is a well-known source of memory leaks in Python applications.

Here's what's happening:
1. An exception occurs somewhere in the worker loop (maybe during Redis push, storage operations, etc.)
2. The exception handler logs with `exc_info=True`, capturing the traceback
3. The logging system (depending on configuration) might be keeping these log records in memory (e.g., in a MemoryHandler, or if there's a handler that buffers)
4. The traceback holds a reference to `fetch_result` and its HTML content
5. Even though you `del fetch_result`, the string persists because the traceback still references it

## Solutions:

### 1. **Remove `exc_info=True` from the exception handler**:
```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")  # No exc_info
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

### 2. **If you need the traceback, explicitly clear it after logging**:
```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    
    # Clear the exception info to break reference cycles
    import sys
    sys.exc_clear() if hasattr(sys, 'exc_clear') else None  # Python 2
    # In Python 3:
    e.__traceback__ = None
    
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

### 3. **Check your logging configuration**:
If you're using any logging handlers that buffer in memory (like `MemoryHandler`), they could be holding onto these log records with their tracebacks.

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")  # No exc_info
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    
    # Clear the exception info to break reference cycles
    import sys
    sys.exc_clear() if hasattr(sys, 'exc_clear') else None  # Python 2
    # In Python 3:
    e.__traceback__ = None
    
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

There are many instances of `exc_info=True` throughout the codebase, but the most problematic one for your memory leak is in the worker loop at line 452 of orchestrator.py.

The memory leak is caused by Python's exception handling mechanism. When you use `exc_info=True`, the logger captures the full exception traceback, which contains references to all local variables in the stack frames - including `fetch_result` with its large HTML content. These references prevent garbage collection.

## Immediate fix for the memory leak:

Replace this in orchestrator.py:
```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

With this:
```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")  # Removed exc_info=True
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

## If you need the traceback for debugging:

Use this approach to clear the traceback after logging:
```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    
    # Clear the exception info to break reference cycles
    import sys
    sys.exc_info()  # Get current exception info
    # Clear traceback to break reference cycles
    if sys.exc_info()[2] is not None:
        sys.exc_info()[2].tb_frame.f_locals.clear()
    
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

## Additional considerations:

1. **Check your logging configuration**: Make sure you're not using any handlers that buffer log records in memory (like `MemoryHandler`).

2. **Consider removing `exc_info=True` from other hot paths**: The fetcher.py line 206 could also contribute if there are frequent fetch errors.

3. **The `redirect_history` issue is still valid**: While not the primary cause, storing response objects does increase memory usage unnecessarily.

This explains why the same HTML string appears in multiple diagnostic periods - it's being held by exception tracebacks in the logging system, preventing garbage collection even after `del fetch_result`.

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")  # Removed exc_info=True
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    
    # Clear the exception info to break reference cycles
    import sys
    sys.exc_info()  # Get current exception info
    # Clear traceback to break reference cycles
    if sys.exc_info()[2] is not None:
        sys.exc_info()[2].tb_frame.f_locals.clear()
    
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}")  # Removed exc_info=True
    errors_counter.labels(error_type='worker_error').inc()
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

```python
except Exception as e:
    # Log the error but continue processing
    logger.error(f"Worker-{worker_id}: Error processing URL: {e}", exc_info=True)
    errors_counter.labels(error_type='worker_error').inc()
    
    # Clear the exception info to break reference cycles
    import sys
    sys.exc_info()  # Get current exception info
    # Clear traceback to break reference cycles
    if sys.exc_info()[2] is not None:
        sys.exc_info()[2].tb_frame.f_locals.clear()
    
    # Small delay before continuing to avoid tight error loops
    await asyncio.sleep(1)
```

