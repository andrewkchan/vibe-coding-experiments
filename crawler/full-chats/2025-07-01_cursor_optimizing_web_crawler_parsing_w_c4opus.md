# Optimizing web crawler parsing with selectolax
_Exported on 7/2/2025 at 02:07:18 PDT from Cursor (1.0.0)_

---

**User**

Parsing is a huge bottleneck in my web crawler. I previously needed to have 2 parser processes just to keep up with 1 fetcher. I recently made a lot of optimizations to the parser and thought I could move to 2 fetchers x 1 parser but it is struggling. It's bottlenecked on CPU and the biggest component is the parsing itself which is done with lxml. I heard selectolax is the state-of-the-art nowadays for Python. Does this sound right to you? If so, can you help me replace my lxml code with selectolax?

---

**Cursor**

I'll help you replace lxml with selectolax. Let me first verify that selectolax is indeed a good choice for your use case.

Based on my research, you're absolutely right! Selectolax is indeed considered one of the fastest HTML parsing libraries for Python and would be an excellent replacement for lxml in your web crawler. Let me help you replace your lxml code with selectolax.

First, let me search for the best way to implement selectolax for your specific use case:

Now let me install selectolax and implement the replacement:

Now let me create a new version of the parser using selectolax:

```python
import logging
from dataclasses import dataclass, field
from typing import List, Set, Optional
from .utils import normalize_and_join_url, normalize_url_parts
from urllib.parse import urlparse

from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

@dataclass
class ParseResult:
    extracted_links: Set[str] = field(default_factory=set)
    text_content: Optional[str] = None
    title: Optional[str] = None

class PageParser:
    def __init__(self):
        # No longer need the Cleaner since selectolax handles HTML differently
        pass

    def parse_html_content(
        self, 
        html_string: str, 
        base_url: str
    ) -> ParseResult:
        """Parses HTML content to extract links and text."""
        if not html_string:
            logger.warning(f"Attempted to parse empty HTML string for base_url: {base_url}")
            return ParseResult()

        try:
            # Use selectolax HTMLParser for faster parsing
            tree = HTMLParser(html_string)
        except Exception as e:
            logger.error(f"Error parsing HTML for {base_url}: {e}. Content preview: {html_string[:200]}")
            return ParseResult()

        # 1. Extract and resolve links
        extracted_links: Set[str] = set()
        try:
            base_parsed = normalize_url_parts(urlparse(base_url.strip()))
            
            # In selectolax, we use css() to find all anchor tags
            for link_tag in tree.css('a'):
                href = link_tag.attributes.get('href')
                if href:
                    u = normalize_and_join_url(base_parsed, href.strip())
                    if u:
                        extracted_links.add(u)
        except Exception as e:
            logger.error(f"Error during link extraction for {base_url}: {e}")

        # 2. Extract text content
        # For now, still returning the raw HTML string to maintain compatibility
        text_content = html_string

        # 3. Extract title
        title: Optional[str] = None
        try:
            title_tags = tree.css('title')
            if title_tags and len(title_tags) > 0:
                title_text = title_tags[0].text(strip=True)
                if title_text:
                    title = title_text
        except Exception as e:
            logger.warning(f"Error extracting title for {base_url}: {e}")

        return ParseResult(
            extracted_links=extracted_links,
            text_content=text_content,
            title=title
        )
```

Now let's run the tests to make sure our selectolax implementation works correctly:

I see. The test failure is due to a difference in how selectolax handles base href resolution compared to lxml. Let me fix this by implementing base href resolution manually:

```python
import logging
from dataclasses import dataclass, field
from typing import List, Set, Optional
from .utils import normalize_and_join_url, normalize_url_parts
from urllib.parse import urlparse

from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)

@dataclass
class ParseResult:
    extracted_links: Set[str] = field(default_factory=set)
    text_content: Optional[str] = None
    title: Optional[str] = None

class PageParser:
    def __init__(self):
        # No longer need the Cleaner since selectolax handles HTML differently
        pass

    def parse_html_content(
        self, 
        html_string: str, 
        base_url: str
    ) -> ParseResult:
        """Parses HTML content to extract links and text."""
        if not html_string:
            logger.warning(f"Attempted to parse empty HTML string for base_url: {base_url}")
            return ParseResult()

        try:
            # Use selectolax HTMLParser for faster parsing
            tree = HTMLParser(html_string)
        except Exception as e:
            logger.error(f"Error parsing HTML for {base_url}: {e}. Content preview: {html_string[:200]}")
            return ParseResult()

        # Check for base href in the document
        effective_base_url = base_url
        try:
            base_tags = tree.css('base[href]')
            if base_tags:
                base_href = base_tags[0].attributes.get('href')
                if base_href:
                    # If base href is absolute, use it directly
                    # Otherwise, resolve it relative to the document URL
                    if base_href.startswith(('http://', 'https://', '//')):
                        effective_base_url = base_href
                    else:
                        # Resolve relative base href against document URL
                        base_parsed = normalize_url_parts(urlparse(base_url.strip()))
                        resolved_base = normalize_and_join_url(base_parsed, base_href)
                        if resolved_base:
                            effective_base_url = resolved_base
        except Exception as e:
            logger.warning(f"Error processing base href for {base_url}: {e}")

        # 1. Extract and resolve links
        extracted_links: Set[str] = set()
        try:
            base_parsed = normalize_url_parts(urlparse(effective_base_url.strip()))
            
            # In selectolax, we use css() to find all anchor tags
            for link_tag in tree.css('a'):
                href = link_tag.attributes.get('href')
                if href:
                    u = normalize_and_join_url(base_parsed, href.strip())
                    if u:
                        extracted_links.add(u)
        except Exception as e:
            logger.error(f"Error during link extraction for {base_url}: {e}")

        # 2. Extract text content
        # For now, still returning the raw HTML string to maintain compatibility
        text_content = html_string

        # 3. Extract title
        title: Optional[str] = None
        try:
            title_tags = tree.css('title')
            if title_tags and len(title_tags) > 0:
                title_text = title_tags[0].text(strip=True)
                if title_text:
                    title = title_text
        except Exception as e:
            logger.warning(f"Error extracting title for {base_url}: {e}")

        return ParseResult(
            extracted_links=extracted_links,
            text_content=text_content,
            title=title
        )
```

Let me debug this by checking what the base href actually resolves to:

```python
// ... existing code ...

def test_base_href_resolution(page_parser: PageParser):
    html_with_base_href = """
    <html><head><base href="http://different.com/basepath/"><title>Base Test</title></head>
    <body><a href="relative.html">Link</a></body></html>
    """
    # The page itself is at page_url, but it declares a different base href
    page_url = "http://actual.com/page.html"
    result = page_parser.parse_html_content(html_with_base_href, page_url)
    print(f"Extracted links: {result.extracted_links}")
    assert result.extracted_links == {"http://different.com/basepath/relative.html"}

// ... existing code ...
```

I see the issue. The base href is `http://different.com/basepath/` (with a trailing slash), but it's being resolved to `http://different.com/relative.html` instead of `http://different.com/basepath/relative.html`. This is a URL normalization issue. Let me check the utils module to see how normalize_and_join_url works:

I see the issue now. In the `normalize_and_join_url` function, when the base path has a trailing slash, it's removing the last part (which is an empty string after split). Let me create a simple test to verify this:

I see the issue now. The `normalize_url_parts` function is stripping the trailing slash from `/basepath/` making it `/basepath`, which causes the URL joining to treat `basepath` as a file instead of a directory. Let me fix this by not passing the base URL through `normalize_url_parts` when we get it from the base tag:

```python
// ... existing code ...

        # Check for base href in the document
        effective_base_url = base_url
        try:
            base_tags = tree.css('base[href]')
            if base_tags:
                base_href = base_tags[0].attributes.get('href')
                if base_href:
                    # If base href is absolute, use it directly
                    # Otherwise, resolve it relative to the document URL
                    if base_href.startswith(('http://', 'https://', '//')):
                        effective_base_url = base_href
                    else:
                        # Resolve relative base href against document URL
                        base_parsed = normalize_url_parts(urlparse(base_url.strip()))
                        resolved_base = normalize_and_join_url(base_parsed, base_href)
                        if resolved_base:
                            effective_base_url = resolved_base
        except Exception as e:
            logger.warning(f"Error processing base href for {base_url}: {e}")

        # 1. Extract and resolve links
        extracted_links: Set[str] = set()
        try:
            # Don't normalize the effective_base_url to preserve trailing slashes
            base_parsed = urlparse(effective_base_url.strip())
            
            # In selectolax, we use css() to find all anchor tags
            for link_tag in tree.css('a'):
                href = link_tag.attributes.get('href')
                if href:
                    u = normalize_and_join_url(base_parsed, href.strip())
                    if u:
                        extracted_links.add(u)
        except Exception as e:
            logger.error(f"Error during link extraction for {base_url}: {e}")

// ... existing code ...
```

Great! Now let's run all the tests to make sure everything is working:

Excellent! All tests are passing. Now let's create a simple benchmark script to compare the performance of selectolax vs lxml:

