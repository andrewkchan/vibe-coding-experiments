# Crawler Coroutine Error Troubleshooting
_Exported on 5/28/2025 at 00:18:20 PDT from Cursor (0.50.5)_

---

**User**

Any idea how I could be getting this error when running the crawler?
```
2025-05-28 07:02:44 - CRITICAL - crawler_module.orchestrator [7866] - Orchestrator critical error: expected string or bytes-like object, got 'coroutine'
Traceback (most recent call last):
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/orchestrator.py", line 150, in run_crawl
    await self.frontier.initialize_frontier()
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 61, in initialize_frontier
    await self._load_seeds() # add_url within is now thread-safe for DB ops
    ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 87, in _load_seeds
    await self.add_url(url)
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/frontier.py", line 151, in add_url
    if not await self.politeness.is_url_allowed(normalized_url):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/politeness.py", line 220, in is_url_allowed
    rerp = await self._get_robots_for_domain(domain)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/vibe-coding-experiments/crawler/crawler_module/politeness.py", line 184, in _get_robots_for_domain
    rerp.parse(robots_content)
  File "/home/ubuntu/vibe-coding-experiments/crawler/.venv/lib/python3.12/site-packages/robotexclusionrulesparser.py", line 533, in parse
    s = _end_of_line_regex.sub("\n", s)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: expected string or bytes-like object, got 'coroutine'
```

I'm confused because it looks like `robot_content` is only ever a string or None

---

**Cursor**

Looking at the error trace, I can see the issue. The problem is occurring in the `_get_robots_for_domain` method when trying to parse `robots_content`.

Let me search for where `robots_content` might be getting set to a coroutine:

Now let me check the `fetcher.py` file to see what `fetch_url` returns:

Now I need to look more carefully at the error. The issue seems to be that `robots_content` is somehow becoming a coroutine. Let me check if there's any place where we might be missing an `await`:

Wait, I think I found the issue! Let me check if there's any place where `asyncio.to_thread` might be returning a coroutine instead of the actual result. The error trace shows it's happening at line 184 when we call `rerp.parse(robots_content)`. 

Let me look more carefully at the code flow around there:

I think I see a potential issue! Let me check the fetcher's `text` method:

Ah! There's the issue! Look at the last line of the grep results - there's a RuntimeWarning about `coroutine 'ClientResponse.text' was never awaited`. This confirms the problem.

Let me check the fetcher code more carefully:

I found the bug! In the `fetcher.py` file, lines 76 and 81, `response.text()` is being called but it's a coroutine that needs to be awaited. Since we've already read the content as bytes with `await response.read()`, we can't call `response.text()` after that in aiohttp.

The issue is that when the content detection fails or returns None, the code tries to use `response.text()` as a fallback, but:
1. `response.text()` is a coroutine that needs to be awaited
2. After calling `response.read()`, the response body has already been consumed, so `response.text()` won't work anyway

Here's the fix for the fetcher:

```python:crawler/crawler_module/fetcher.py
// ... existing code ...
                text_content: Optional[str] = None
                if content_bytes:
                    # Try to decode using cchardet for speed and accuracy
                    try:
                        detected_encoding = cchardet.detect(content_bytes)['encoding']
                        if detected_encoding:
                            text_content = content_bytes.decode(detected_encoding, errors='replace')
                        else:
                            # Fallback to utf-8 if no encoding detected
                            text_content = content_bytes.decode('utf-8', errors='replace')
                    except (UnicodeDecodeError, LookupError, TypeError) as e:
                        logger.warning(f"Encoding detection/decoding error for {actual_final_url}: {e}. Falling back.")
                        try:
                            # Fallback to utf-8 with error replacement
                            text_content = content_bytes.decode('utf-8', errors='replace') 
                        except Exception as ex_inner:
                             logger.error(f"Final fallback decoding error for {actual_final_url}: {ex_inner}")
                             text_content = "DECODING_ERROR"
// ... existing code ...
```

The key changes are:
1. Replace `response.text(errors='replace')` with `content_bytes.decode('utf-8', errors='replace')` on both lines 76 and 81
2. This uses the bytes we already have instead of trying to call the coroutine `response.text()`

This explains why `robots_content` was becoming a coroutine - when the fetcher tried to use `response.text()` as a fallback, it returned the unawaited coroutine object instead of the actual text content.

```python
// ... existing code ...
                text_content: Optional[str] = None
                if content_bytes:
                    # Try to decode using cchardet for speed and accuracy
                    try:
                        detected_encoding = cchardet.detect(content_bytes)['encoding']
                        if detected_encoding:
                            text_content = content_bytes.decode(detected_encoding, errors='replace')
                        else:
                            # Fallback to utf-8 if no encoding detected
                            text_content = content_bytes.decode('utf-8', errors='replace')
                    except (UnicodeDecodeError, LookupError, TypeError) as e:
                        logger.warning(f"Encoding detection/decoding error for {actual_final_url}: {e}. Falling back.")
                        try:
                            # Fallback to utf-8 with error replacement
                            text_content = content_bytes.decode('utf-8', errors='replace') 
                        except Exception as ex_inner:
                             logger.error(f"Final fallback decoding error for {actual_final_url}: {ex_inner}")
                             text_content = "DECODING_ERROR"
// ... existing code ...
```

