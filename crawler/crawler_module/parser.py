import logging
from dataclasses import dataclass, field
from typing import List, Set, Optional
from urllib.parse import urljoin, urlparse

from lxml import html, etree # For parsing HTML
from lxml.html.clean import Cleaner # For basic text extraction

from .utils import normalize_url, extract_domain # For normalizing and validating URLs

logger = logging.getLogger(__name__)

@dataclass
class ParseResult:
    extracted_links: Set[str] = field(default_factory=set)
    text_content: Optional[str] = None
    title: Optional[str] = None

class PageParser:
    def __init__(self):
        # Initialize a cleaner object. Configure as needed.
        self.cleaner = Cleaner(
            scripts=True, # Remove script tags
            javascript=True, # Remove inline JS
            comments=True, # Remove comments
            style=True, # Remove style tags and attributes
            links=False, # Keep <link> tags if they might be useful, e.g. canonical, but usually for crawler, no
            meta=False, # Keep <meta> tags for now, could remove if noisy
            page_structure=False, # Keep page structure (divs, p, etc.)
            processing_instructions=True, # Remove PIs
            frames=True, # Remove frames
            forms=False, # Keep forms if their text content is desired, else True
            annoying_tags=True, # Remove blink, marquee
            remove_unknown_tags=False, # Keep unknown tags
            safe_attrs_only=False, # Keep all attributes for now
        )

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
            # Use lxml.html.fromstring which is more robust for potentially broken HTML
            doc = html.fromstring(html_string)
        except etree.ParserError as e:
            logger.error(f"lxml ParserError for {base_url}: {e}. Content preview: {html_string[:200]}")
            return ParseResult() # Return empty result on severe parsing error
        except Exception as e:
            logger.error(f"Unexpected error parsing HTML for {base_url}: {e}. Content preview: {html_string[:200]}")
            return ParseResult()

        # 1. Extract and resolve links
        extracted_links: Set[str] = set()
        try:
            # Make sure the base_url is properly set for link resolution
            doc.make_links_absolute(base_url, resolve_base_href=True)
            
            for element, attribute, link, pos in doc.iterlinks():
                if attribute == 'href':
                    try:
                        parsed_link = urlparse(link)
                        if parsed_link.scheme not in ['http', 'https']:
                            continue 
                    except Exception as e:
                        continue 

                    normalized = normalize_url(link) 
                    if normalized:
                        extracted_links.add(normalized)
        except Exception as e:
            logger.error(f"Error during link extraction for {base_url}: {e}")

        # 2. Extract text content
        # Currently not using any text extraction logic, just returning the raw HTML string
        # If we want to clean the HTML or extract specific elements we would do so here
        text_content = html_string

        # 3. Extract title
        title: Optional[str] = None
        try:
            title_element = doc.find('.//title')
            if title_element is not None and title_element.text:
                title = title_element.text.strip()
        except Exception as e:
            logger.warning(f"Error extracting title for {base_url}: {e}")

        return ParseResult(
            extracted_links=extracted_links,
            text_content=text_content,
            title=title
        ) 