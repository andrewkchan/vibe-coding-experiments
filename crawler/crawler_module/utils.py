from urllib.parse import urlparse, urlunparse, urldefrag
import tldextract # type: ignore

def normalize_url(url: str) -> str:
    """Normalize a URL to a canonical form."""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "http://" + url # Assume http if no scheme, can be refined
    
    parsed = urlparse(url)
    
    # Lowercase scheme and hostname
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    
    # Remove default port (80 for http, 443 for https)
    if (scheme == "http" and netloc.endswith(":80")) or \
       (scheme == "https" and netloc.endswith(":443")):
        netloc = netloc.rsplit(":", 1)[0]
        
    # Remove fragment
    path = urldefrag(parsed.path).url
    if parsed.query:
        path += "?" + parsed.query

    # Reconstruct URL
    # Note: params, query, fragment are handled by urldefrag for path or explicitly added
    normalized = urlunparse((scheme, netloc, path, '', '', ''))
    
    # Remove trailing slash unless it's the only thing in the path
    if normalized.endswith("/") and len(urlparse(normalized).path) > 1:
        normalized = normalized.rstrip("/")
        
    return normalized

def extract_domain(url: str) -> str | None:
    """Extracts the registered domain from a URL."""
    try:
        # Normalize first to handle URLs like "example.com" without scheme
        normalized_url = url
        if not normalized_url.startswith(("http://", "https://")):
            normalized_url = "http://" + normalized_url
            
        extracted = tldextract.extract(normalized_url)
        if extracted.registered_domain:
            return extracted.registered_domain
        return None
    except Exception:
        # tldextract can sometimes fail on very malformed inputs
        return None 