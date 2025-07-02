from urllib.parse import urlparse, urlunparse, uses_relative, uses_netloc, ParseResult
import tldextract # type: ignore
from collections import OrderedDict
from typing import Generic, TypeVar, Optional, List

def extract_domain(url: str) -> str | None:
    """Extracts the registered domain from a URL."""
    try:
        extracted = tldextract.extract(url.strip())
        if extracted.top_domain_under_public_suffix:
            return extracted.top_domain_under_public_suffix
        return None
    except Exception:
        # tldextract can sometimes fail on very malformed inputs
        return None 

def normalize_url_parts(parsed: ParseResult) -> ParseResult:
    """Normalize a URL to a canonical form."""
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower() # domain & port
    path = parsed.path
    if not scheme:
        # Handle common case: 'example.com' -> 'http://example.com'
        scheme = "http"
        if not netloc:
            path_parts = path.split("/", 1)
            netloc = path_parts[0]
            path = ("/" + path_parts[1]) if len(path_parts) > 1 else ""

    query = parsed.query
    # fragment and params are explicitly removed
    fragment = ""
    params = ""

    # Remove default port (80 for http, 443 for https)
    if (scheme == "http" and netloc.endswith(":80")) or \
       (scheme == "https" and netloc.endswith(":443")):
        netloc = netloc.rsplit(":", 1)[0]
        
    # Path normalization for trailing slashes
    if path == "/":
        path = "" 
    elif path.endswith("/") and len(path) > 1:
        path = path.rstrip("/")

    return ParseResult(scheme, netloc, path, params, query, fragment)

# Adapted from urllib.parse.urljoin with modifications to filter out non http/https urls
def normalize_and_join_url(base_parsed: ParseResult, url: str) -> str | None:
    """Join a base URL and a possibly relative URL to form an absolute
    interpretation of the latter."""
    bscheme, bnetloc, bpath, bparams, bquery, bfragment = base_parsed
    scheme, netloc, path, params, query, fragment = \
            normalize_url_parts(urlparse(url, bscheme, allow_fragments=True))
    
    if scheme not in ["http", "https"]:
        return None
    if scheme != bscheme or scheme not in uses_relative:
        return url
    if scheme in uses_netloc:
        if netloc:
            return urlunparse((scheme, netloc, path,
                               params, query, fragment))
        netloc = bnetloc

    if not path and not params:
        path = bpath
        params = bparams
        if not query:
            query = bquery
        return urlunparse((scheme, netloc, path,
                           params, query, fragment))

    base_parts = bpath.split('/')
    if base_parts[-1] != '':
        # the last item is not a directory, so will not be taken into account
        # in resolving the relative path
        del base_parts[-1]

    # for rfc3986, ignore all base path should the first character be root.
    if path[:1] == '/':
        segments = path.split('/')
    else:
        segments = base_parts + path.split('/')
        # filter out elements that would cause redundant slashes on re-joining
        # the resolved_path
        segments[1:-1] = filter(None, segments[1:-1])

    resolved_path: List[str] = []

    for seg in segments:
        if seg == '..':
            try:
                resolved_path.pop()
            except IndexError:
                # ignore any .. segments that would otherwise cause an IndexError
                # when popped from resolved_path if resolving for rfc3986
                pass
        elif seg == '.':
            continue
        else:
            resolved_path.append(seg)

    if segments[-1] in ('.', '..'):
        # do some post-processing here. if the last segment was a relative dir,
        # then we need to append the trailing '/'
        resolved_path.append('')

    return urlunparse((scheme, netloc, '/'.join(
        resolved_path) or '/', params, query, fragment))

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