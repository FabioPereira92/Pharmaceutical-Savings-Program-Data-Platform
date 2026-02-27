"""
crawl4ai-based HTML fetching module for robust page extraction.
Falls back to requests + BeautifulSoup if crawl4ai is not available.
"""

import logging
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from threading import Lock

# Try to import crawl4ai
try:
    from crawl4ai import WebCrawler
    from crawl4ai.extraction_strategy import LLMExtractionStrategy  # noqa: F401 (kept for compatibility)
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False
    WebCrawler = None

# Fallback imports
try:
    import requests
    from bs4 import BeautifulSoup
    FALLBACK_AVAILABLE = True
except ImportError:
    FALLBACK_AVAILABLE = False
    requests = None
    BeautifulSoup = None


# Simple LRU cache with thread safety
class SimpleLRUCache:
    def __init__(self, max_size: int = 100):
        self.cache: Dict[str, Dict] = {}
        self.max_size = max_size
        self.lock = Lock()
        self.access_order: List[str] = []

    def get(self, key: str) -> Optional[Dict]:
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                try:
                    self.access_order.remove(key)
                except ValueError:
                    # Access order got out of sync; continue safely
                    pass
                self.access_order.append(key)
                return self.cache[key]
            return None

    def set(self, key: str, value: Dict):
        with self.lock:
            if key in self.cache:
                try:
                    self.access_order.remove(key)
                except ValueError:
                    pass
            elif len(self.cache) >= self.max_size:
                # Remove least recently used
                if self.access_order:
                    lru_key = self.access_order.pop(0)
                    self.cache.pop(lru_key, None)
                else:
                    # Safety fallback if access_order is empty for some reason
                    self.cache.clear()

            self.cache[key] = value
            self.access_order.append(key)


# Global cache instance
_fetch_cache = SimpleLRUCache(max_size=100)


def _normalize_url_for_cache(url: str) -> str:
    """Normalize URL for caching (strip UTM params, etc.)"""
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    if not url:
        return ""
    try:
        p = urlparse(url)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path.endswith("/") and path != "/":
            path = path[:-1]

        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = (k or "").lower()
            # Skip tracking params
            if kl in {
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "gclid",
                "fbclid",
                "msclkid",
                "_ga",
                "_gid",
                "ref",
                "referrer",
            }:
                continue
            if "utm_" in kl:
                continue
            q.append((k, v))

        query = urlencode(q, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return url


def _is_valid_http_url(url: str) -> bool:
    """Check if URL is http/https and has netloc"""
    if not url:
        return False
    try:
        p = urlparse(url)
        return (p.scheme or "").lower() in {"http", "https"} and bool(p.netloc)
    except Exception:
        return False


def _clean_url(url: str, base_url: str = "") -> str:
    """Convert to absolute URL and validate"""
    if not url:
        return ""

    # Skip non-http protocols
    if url.startswith(("tel:", "mailto:", "javascript:", "data:", "#")):
        return ""

    # Make absolute
    if base_url and not url.startswith(("http://", "https://")):
        url = urljoin(base_url, url)

    # Validate
    if not _is_valid_http_url(url):
        return ""

    return url


def _detect_block(title: str, text: str, final_url: str) -> tuple:
    """Detect if page is blocked/captcha. Returns (blocked: bool, reason: str)"""
    title_lower = (title or "").lower()
    text_lower = (text or "")[:5000].lower()

    block_signals = [
        ("access denied", "access_denied"),
        ("forbidden", "forbidden"),
        ("403 forbidden", "403"),
        ("too many requests", "rate_limited"),
        ("429", "rate_limited"),
        ("attention required", "cloudflare_attention"),
        ("are you a robot", "bot_check"),
        ("captcha", "captcha"),
        ("cloudflare", "cloudflare"),
        ("just a moment", "cloudflare_wait"),
        ("security check", "security_check"),
        ("unusual traffic", "unusual_traffic"),
        ("verify you are human", "verification"),
        ("perimeterx", "perimeterx"),
        ("datadome", "datadome"),
        ("incapsula", "imperva"),
        ("imperva", "imperva"),
        ("akamai", "akamai_bot"),
        ("bot manager", "akamai_bot"),
    ]

    for signal, reason in block_signals:
        if signal in title_lower or signal in text_lower:
            return True, reason

    # Check for about:blank or data URLs
    if (final_url or "").startswith(("about:", "data:")):
        return True, "invalid_url"

    return False, ""


def _extract_text_fallback(html: str) -> str:
    """Extract visible text from HTML using BeautifulSoup"""
    if not BeautifulSoup:
        return html

    try:
        soup = BeautifulSoup(html, "html.parser")

        # Remove script and style elements
        for script in soup(["script", "style", "noscript"]):
            script.decompose()

        # Get text
        text = soup.get_text(separator="\n", strip=True)

        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = "\n".join(chunk for chunk in chunks if chunk)

        return text
    except Exception as e:
        logging.warning(f"BeautifulSoup text extraction failed: {e}")
        return html


def _extract_links_fallback(html: str, base_url: str) -> List[Dict[str, str]]:
    """Extract links from HTML using BeautifulSoup"""
    if not BeautifulSoup:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
        links: List[Dict[str, str]] = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href:
                continue

            # Clean and make absolute
            href = _clean_url(href, base_url)
            if not href:
                continue

            # Normalize for deduplication
            norm_href = _normalize_url_for_cache(href)
            if not norm_href or norm_href in seen:
                continue
            seen.add(norm_href)

            # Extract label
            label = (
                a.get_text(strip=True)
                or a.get("aria-label", "").strip()
                or a.get("title", "").strip()
            )
            if not label:
                label = "(no visible text)"
            if len(label) > 140:
                label = label[:140] + "…"

            links.append({"href": href, "label": label})

            if len(links) >= 350:
                break

        return links
    except Exception as e:
        logging.warning(f"BeautifulSoup link extraction failed: {e}")
        return []


def _extract_forms_fallback(html: str, base_url: str) -> List[Dict[str, str]]:
    """Extract forms from HTML using BeautifulSoup"""
    if not BeautifulSoup:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
        forms: List[Dict[str, str]] = []
        seen = set()

        for form in soup.find_all("form"):
            action = form.get("action", "").strip()
            if not action:
                continue

            # Clean and make absolute
            action = _clean_url(action, base_url)
            if not action or action in seen:
                continue
            seen.add(action)

            method = (form.get("method") or "GET").upper()
            forms.append({"action": action, "method": method})

            if len(forms) >= 50:
                break

        return forms
    except Exception as e:
        logging.warning(f"BeautifulSoup form extraction failed: {e}")
        return []


def crawl4ai_fetch(url: str, timeout_s: int = 30) -> dict:
    """
    Fetch and extract content from a URL using crawl4ai (or fallback).

    Returns:
        {
            "final_url": str,              # after redirects
            "title": str,
            "text": str,                   # visible text extracted from HTML where possible
            "links": List[{"href": str, "label": str}],  # absolute URLs only
            "forms": List[{"action": str, "method": str}],
            "blocked": bool,               # True if bot/captcha/forbidden detected
            "block_reason": str,           # short reason if blocked
            "content_type": str            # "text/html" (default) or "pdf"
        }
    """
    # Check cache first
    cache_key = _normalize_url_for_cache(url)
    cached = _fetch_cache.get(cache_key)
    if cached:
        logging.info(f"crawl4ai_fetch cache hit: {url}")
        return cached

    result = {
        "final_url": url,
        "title": "",
        "text": "",
        "links": [],
        "forms": [],
        "blocked": False,
        "block_reason": "",
        "content_type": "text/html",
    }

    # Validate URL
    if not _is_valid_http_url(url):
        result["blocked"] = True
        result["block_reason"] = "invalid_url"
        _fetch_cache.set(cache_key, result)
        return result

    # Try crawl4ai first
    if CRAWL4AI_AVAILABLE:
        try:
            logging.info(f"crawl4ai_fetch using crawl4ai: {url}")
            crawler = WebCrawler()
            crawler.warmup()

            crawl_result = crawler.run(
                url=url,
                bypass_cache=False,
                timeout=timeout_s,
            )

            if crawl_result.success:
                result["final_url"] = crawl_result.url or url
                result["title"] = (
                    crawl_result.metadata.get("title", "") if crawl_result.metadata else ""
                )

                # Prefer actual HTML for parsing; fall back gracefully
                html_for_parse = (crawl_result.html or crawl_result.cleaned_html or "") or ""

                # IMPORTANT: "text" should be visible text, not raw HTML
                if html_for_parse:
                    result["text"] = _extract_text_fallback(html_for_parse)
                    result["links"] = _extract_links_fallback(
                        html_for_parse, result["final_url"]
                    )
                    result["forms"] = _extract_forms_fallback(
                        html_for_parse, result["final_url"]
                    )
                elif getattr(crawl_result, "markdown", None):
                    # Not ideal, but better than empty
                    result["text"] = crawl_result.markdown or ""
                else:
                    result["text"] = ""

                # Check for blocks
                blocked, reason = _detect_block(result["title"], result["text"], result["final_url"])
                result["blocked"] = blocked
                result["block_reason"] = reason

                # Cache and return
                _fetch_cache.set(cache_key, result)
                return result
            else:
                logging.warning(f"crawl4ai failed for {url}, falling back to requests")
        except Exception as e:
            logging.warning(f"crawl4ai exception for {url}: {e}, falling back to requests")

    # Fallback to requests + BeautifulSoup
    if FALLBACK_AVAILABLE:
        try:
            logging.info(f"crawl4ai_fetch using requests fallback: {url}")
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }

            response = requests.get(
                url, timeout=timeout_s, headers=headers, allow_redirects=True
            )

            # Detect block via status codes early
            if response.status_code in (401, 403, 429):
                result["final_url"] = response.url or url
                result["blocked"] = True
                result["block_reason"] = f"http_{response.status_code}"
                _fetch_cache.set(cache_key, result)
                return result

            # Detect PDF via content-type even if URL doesn't end with .pdf
            ct = (response.headers.get("Content-Type") or "").lower()
            if "application/pdf" in ct or ct.startswith("application/pdf"):
                result["final_url"] = response.url or url
                result["content_type"] = "pdf"
                # Leave text empty; caller can route to PDF extractor
                result["text"] = ""
                result["links"] = []
                result["forms"] = []
                result["blocked"] = False
                result["block_reason"] = ""
                _fetch_cache.set(cache_key, result)
                return result

            response.raise_for_status()

            result["final_url"] = response.url or url
            html = response.text

            # Extract title
            if BeautifulSoup:
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    title_tag = soup.find("title")
                    result["title"] = title_tag.get_text(strip=True) if title_tag else ""
                except Exception:
                    pass

            # Extract visible text
            result["text"] = _extract_text_fallback(html)

            # Extract links and forms
            result["links"] = _extract_links_fallback(html, result["final_url"])
            result["forms"] = _extract_forms_fallback(html, result["final_url"])

            # Check for blocks
            blocked, reason = _detect_block(result["title"], result["text"], result["final_url"])
            result["blocked"] = blocked
            result["block_reason"] = reason

            # Cache and return
            _fetch_cache.set(cache_key, result)
            return result

        except Exception as e:
            logging.error(f"Requests fallback failed for {url}: {e}")
            result["blocked"] = True
            result["block_reason"] = f"fetch_error:{str(e)[:50]}"
            _fetch_cache.set(cache_key, result)
            return result

    # No fetch method available
    logging.error("No fetch method available (crawl4ai and requests both unavailable)")
    result["blocked"] = True
    result["block_reason"] = "no_fetch_library"
    _fetch_cache.set(cache_key, result)
    return result


# Smoke test function
def _smoke_test():
    """Smoke test for crawl4ai_fetch"""
    print("=== crawl4ai_fetch smoke test ===")
    print(f"crawl4ai available: {CRAWL4AI_AVAILABLE}")
    print(f"fallback available: {FALLBACK_AVAILABLE}")
    print()

    # Test 1: Simple HTML page
    print("Test 1: example.com")
    result1 = crawl4ai_fetch("https://example.com", timeout_s=15)
    print(f"  final_url: {result1['final_url']}")
    print(f"  title: {result1['title']}")
    print(f"  text_length: {len(result1['text'])}")
    print(f"  links_count: {len(result1['links'])}")
    print(f"  forms_count: {len(result1['forms'])}")
    print(f"  blocked: {result1['blocked']} ({result1['block_reason']})")
    print(f"  content_type: {result1.get('content_type')}")
    print()

    # Test 2: PDF URL (should not fetch in normal flow)
    print("Test 2: PDF URL detection")
    pdf_url = "https://example.com/document.pdf"
    result2 = crawl4ai_fetch(pdf_url, timeout_s=15)
    print(f"  final_url: {result2['final_url']}")
    print(f"  text_length: {len(result2['text'])}")
    print(f"  blocked: {result2['blocked']} ({result2['block_reason']})")
    print(f"  content_type: {result2.get('content_type')}")
    print()

    print("=== Smoke test complete ===")


if __name__ == "__main__":
    _smoke_test()