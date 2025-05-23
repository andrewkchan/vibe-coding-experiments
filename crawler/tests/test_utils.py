import pytest
from pathlib import Path
from urllib.parse import urlparse

from crawler_module.utils import normalize_url, extract_domain

# Test cases for normalize_url
# (original_url, expected_normalized_url)
normalize_test_cases = [
    ("http://Example.com/Path/to/Page?Query=Value#Fragment", "http://example.com/Path/to/Page?Query=Value"),
    ("https://example.com:80/", "https://example.com:80"), # HTTPS on port 80 is unusual but valid
    ("http://example.com:443/", "http://example.com:443"), # HTTP on port 443 is unusual but valid
    ("https://example.com:443/path/", "https://example.com/path"),
    ("https://example.com/path/", "https://example.com/path"),
    ("example.com", "http://example.com"),
    ("example.com/path", "http://example.com/path"),
    ("http://www.example.com/path/", "http://www.example.com/path"),
    ("HTTP://WWW.EXAMPLE.COM/another_path?param=1&PARAM=2", "http://www.example.com/another_path?param=1&PARAM=2"),
    ("http://example.com/trailing/", "http://example.com/trailing"),
    ("http://example.com/", "http://example.com"), # Root path with slash
    ("http://example.com", "http://example.com"),   # Root path without slash
    ("  http://example.com/padded  ", "http://example.com/padded"),
    ("http://example.com/a//b//c", "http://example.com/a//b//c"), # Path normalization is not part of this simple normalizer
]

@pytest.mark.parametrize("original,expected", normalize_test_cases)
def test_normalize_url(original, expected):
    assert normalize_url(original) == expected

# Test cases for extract_domain
# (url, expected_domain)
extract_domain_test_cases = [
    ("http://example.com/path", "example.com"),
    ("https://www.example.co.uk/another", "example.co.uk"),
    ("http://sub.domain.example.com?q=1", "example.com"),
    ("example.com", "example.com"), # No scheme
    ("ftp://another.example.org", "example.org"), # Different scheme (though we normalize to http/s first in util)
    ("http://localhost/test", None), # localhost usually not a registered domain by tldextract
    ("http://127.0.0.1/page", None), # IP addresses
    ("very.long.subdomain.example.com", "example.com"),
    ("http://example.com.", "example.com"), # Trailing dot on FQDN
    ("malformed-url", None) # Completely malformed
]

@pytest.mark.parametrize("url,expected", extract_domain_test_cases)
def test_extract_domain(url, expected):
    # extract_domain itself normalizes to http if no scheme, so we pass raw url
    assert extract_domain(url) == expected


def test_normalize_url_idempotency():
    url = "http://Example.com/Some_Path/?Query=Test#Fragment"
    normalized1 = normalize_url(url)
    normalized2 = normalize_url(normalized1)
    assert normalized1 == normalized2, "Normalizing an already normalized URL should yield the same URL"

# Clean up the original utils.py by removing its __main__ block 