from urllib.parse import urlparse, urlunparse, urldefrag
import tldextract # type: ignore

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