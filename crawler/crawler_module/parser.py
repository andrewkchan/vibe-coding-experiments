import logging
from dataclasses import dataclass, field
from typing import List, Set, Optional
from .utils import normalize_and_join_url, normalize_url_parts
from urllib.parse import urlparse

from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger(__name__)

@dataclass
class ParseResult:
    extracted_links: Set[str] = field(default_factory=set)
    text_content: Optional[str] = None
    title: Optional[str] = None

class PageParser:
    def __init__(self):
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
            tree = LexborHTMLParser(html_string)
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

        # 2. Extract text content
        # Currently not using any text extraction logic, just returning the raw HTML string
        # If we want to clean the HTML or extract specific elements we would do so here
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