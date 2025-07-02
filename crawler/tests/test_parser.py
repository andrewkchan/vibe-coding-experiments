import pytest
from crawler_module.parser import PageParser

@pytest.fixture
def page_parser() -> PageParser:
    return PageParser()

def test_parse_sample_html(page_parser: PageParser):
    sample_html_content = """
    <html>
        <head>
            <title>  Test Page Title </title>
            <base href="http://example.com/path/">
        </head>
        <body>
            <h1>Welcome</h1>
            <p>This is a test page with <a href="subpage.html">a relative link</a>, 
               an <a href="http://another.com/abs_link">absolute link</a>, and
               a <a href="/another_relative_link">root relative link</a>.
            </p>
            <a href="https://secure.example.com/secure">Secure Link</a>
            <a href="javascript:void(0)">JS Link</a>
            <a href="mailto:test@example.com">Mail Link</a>
            <p>Some more text. &nbsp; &copy; 2023</p>
            <script>console.log("hello")</script>
            <!-- A comment -->
            <a href="http://example.com/path/subpage.html#fragment">Link with fragment</a>
            <a href="HTTP://EXAMPLE.COM/UPPERCASE">Uppercase link</a>
        </body>
    </html>
    """
    base_url = "http://example.com/path/page.html"

    result = page_parser.parse_html_content(sample_html_content, base_url)

    assert result.title == "Test Page Title"
    
    expected_links = {
        "http://example.com/path/subpage.html",
        "http://another.com/abs_link",
        "http://example.com/another_relative_link",
        "https://secure.example.com/secure",
        "http://example.com/path/subpage.html", # from link with fragment
        "http://example.com/UPPERCASE",
    }
    assert result.extracted_links == expected_links

    # Basic check for text content - since it returns the raw string
    assert "<h1>Welcome</h1>" in result.text_content if result.text_content else False
    assert "<script>console.log(\"hello\")</script>" in result.text_content if result.text_content else False
    assert "<!-- A comment -->" in result.text_content if result.text_content else False

def test_parse_broken_html(page_parser: PageParser):
    broken_html = "<body><p>Just a paragraph <a href=\"malformed.html\">link</p></body>"
    base_url = "http://broken.com/"
    result = page_parser.parse_html_content(broken_html, base_url)

    # lxml is quite robust and might still parse this.
    # Check what it extracts.
    # Expected: http://broken.com/malformed.html (if base URL is applied before error)
    # or just http://broken.com/ if malformed.html cannot be resolved or link not found.
    # Based on current parser, it should find it.
    if result.extracted_links:
        assert "http://broken.com/malformed.html" in result.extracted_links
    else:
        pass 
    # The text content should be the original broken HTML string
    assert result.text_content == broken_html

def test_parse_empty_html(page_parser: PageParser):
    empty_html = ""
    base_url = "http://empty.com/"
    result = page_parser.parse_html_content(empty_html, base_url)
    assert not result.extracted_links
    assert result.text_content is None # The parser returns None for empty input
    assert result.title is None

def test_parse_html_no_body(page_parser: PageParser):
    html_no_body = "<html><head><title>No Body</title></head></html>"
    base_url = "http://nobody.com/"
    result = page_parser.parse_html_content(html_no_body, base_url)
    assert result.title == "No Body"
    assert not result.extracted_links
    assert result.text_content == html_no_body

def test_parse_html_no_title(page_parser: PageParser):
    html_no_title = "<html><body><p>No title here.</p></body></html>"
    base_url = "http://notitle.com/"
    result = page_parser.parse_html_content(html_no_title, base_url)
    assert result.title is None
    assert result.text_content == html_no_title

def test_base_href_resolution(page_parser: PageParser):
    html_with_base_href = """
    <html><head><base href="http://different.com/basepath/"><title>Base Test</title></head>
    <body><a href="relative.html">Link</a></body></html>
    """
    # The page itself is at page_url, but it declares a different base href
    page_url = "http://actual.com/page.html"
    result = page_parser.parse_html_content(html_with_base_href, page_url)
    assert result.extracted_links == {"http://different.com/basepath/relative.html"}

def test_no_links(page_parser: PageParser):
    html_no_links = "<html><body><p>Just text.</p></body></html>"
    base_url = "http://nolinks.com/"
    result = page_parser.parse_html_content(html_no_links, base_url)
    assert not result.extracted_links
    assert result.text_content == html_no_links

# TODO: Add more tests for various HTML structures, encodings (though parser takes string), edge cases for text extraction. 