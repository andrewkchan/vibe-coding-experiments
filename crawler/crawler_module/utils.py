from urllib.parse import urlparse, urlunparse, urldefrag
import tldextract # type: ignore
from collections import OrderedDict
from typing import Generic, TypeVar, Optional

def normalize_url(url: str) -> str:
    """Normalize a URL to a canonical form."""
    url = url.strip()
    
    # Prepend http:// if no scheme is present to allow urlparse to work correctly
    # for inputs like "example.com/path"
    # A more robust scheme check involves parsing first, but for typical inputs:
    original_url_had_scheme = "://" in url.split("?", 1)[0].split("#", 1)[0]
    
    temp_url_for_parsing = url
    if not original_url_had_scheme:
        # Avoid prepending to things that might be like mailto: or have other non-http context
        # This is a simple heuristic.
        if "@" not in url.split("/",1)[0]: 
             temp_url_for_parsing = "http://" + url

    parsed = urlparse(temp_url_for_parsing)
    
    scheme = parsed.scheme.lower()
    # If we prepended http but urlparse still found a different scheme (e.g. "ftp:example.com")
    # we should respect the original scheme if one was truly there, or stick to http if not.
    # This logic is tricky. The goal is to normalize http/https and schemeless common inputs.
    if original_url_had_scheme:
        # If original had a scheme, use its parsed version (lowercased)
        parsed_original_scheme = urlparse(url).scheme.lower()
        if parsed_original_scheme:
            scheme = parsed_original_scheme
    elif not scheme: # If no scheme found even after potential prepend (e.g. just "example")
        scheme = "http"

    netloc = parsed.netloc.lower() # domain & port
    path = parsed.path
    query = parsed.query
    # fragment is explicitly removed by passing '' to urlunparse for the fragment part

    # Remove default port (80 for http, 443 for https)
    if (scheme == "http" and netloc.endswith(":80")) or \
       (scheme == "https" and netloc.endswith(":443")):
        netloc = netloc.rsplit(":", 1)[0]
        
    # Path normalization for trailing slashes
    if path == "/":
        path = "" 
    elif path.endswith("/") and len(path) > 1:
        path = path.rstrip("/")

    # Reconstruct, explicitly removing fragment by passing empty string for fragment component
    normalized = urlunparse((scheme, netloc, path, '', query, ''))
        
    return normalized

def extract_domain(url: str) -> str | None:
    """Extracts the registered domain from a URL."""
    try:
        url_to_extract = url.strip()
        parsed_check = urlparse(url_to_extract)

        if not parsed_check.scheme: # If no scheme like "example.com", prepend http for tldextract
            url_to_extract = "http://" + url_to_extract
            
        extracted = tldextract.extract(url_to_extract)
        if extracted.top_domain_under_public_suffix:
            return extracted.top_domain_under_public_suffix
        return None
    except Exception:
        # tldextract can sometimes fail on very malformed inputs
        return None 

K = TypeVar('K')
V = TypeVar('V')

class LRUCache(Generic[K, V]):
    """A thread-safe LRU (Least Recently Used) cache implementation.
    
    Uses an OrderedDict to maintain insertion order. When the cache reaches
    its maximum size, the least recently used item is evicted.
    """
    
    def __init__(self, max_size: int):
        """Initialize the LRU cache with a maximum size.
        
        Args:
            max_size: Maximum number of items to store in the cache.
        """
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self.max_size = max_size
        self._cache: OrderedDict[K, V] = OrderedDict()
    
    def get(self, key: K) -> Optional[V]:
        """Get a value from the cache.
        
        If the key exists, it's moved to the end (most recently used).
        
        Args:
            key: The key to look up.
            
        Returns:
            The value if found, None otherwise.
        """
        if key not in self._cache:
            return None
        
        # Move to end (mark as recently used)
        self._cache.move_to_end(key)
        return self._cache[key]
    
    def put(self, key: K, value: V) -> None:
        """Put a key-value pair in the cache.
        
        If the key already exists, it's updated and moved to the end.
        If the cache is full, the least recently used item is evicted.
        
        Args:
            key: The key to store.
            value: The value to store.
        """
        if key in self._cache:
            # Update existing key and move to end
            self._cache.move_to_end(key)
            self._cache[key] = value
        else:
            # Add new key
            self._cache[key] = value
            
            # Evict LRU item if necessary
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
    
    def __contains__(self, key: K) -> bool:
        """Check if a key exists in the cache.
        
        Note: This doesn't update the access order.
        
        Args:
            key: The key to check.
            
        Returns:
            True if the key exists, False otherwise.
        """
        return key in self._cache
    
    def __len__(self) -> int:
        """Get the current number of items in the cache."""
        return len(self._cache)
    
    def clear(self) -> None:
        """Clear all items from the cache."""
        self._cache.clear() 