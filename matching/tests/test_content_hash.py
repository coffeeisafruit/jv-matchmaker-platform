"""
Tests for matching/enrichment/flows/content_hash_check.py

Verification item #17: Content hash detection — modify 1 test profile's
website content, run change detection, verify hash change flagged.

Covers pure functions (_clean_html, _hash_text, _normalise_base_url) without
Prefect or network access, plus mocked integration tests for check_content_hash.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import hashlib
import logging
from unittest.mock import patch, MagicMock

import pytest
import requests

from matching.enrichment.flows.content_hash_check import (
    _clean_html,
    _hash_text,
    _normalise_base_url,
    check_content_hash,
    HashCheckResult,
)


# =============================================================================
# Fixtures
# =============================================================================

BUSINESS_HTML = """
<!DOCTYPE html>
<html>
<head><title>Acme Coaching</title></head>
<body>
  <nav><a href="/">Home</a><a href="/about">About</a></nav>
  <header><div class="site-branding">Acme Logo</div></header>
  <main>
    <h1>Welcome to Acme Coaching</h1>
    <p>We help executives unlock their potential through proven frameworks.</p>
    <ul>
      <li>Leadership development</li>
      <li>Strategic planning</li>
      <li>Team alignment</li>
    </ul>
  </main>
  <aside><p>Recent tweets feed widget</p></aside>
  <footer><p>Copyright 2025 Acme Inc.</p></footer>
  <script>console.log("analytics");</script>
  <style>.hidden { display: none; }</style>
  <iframe src="https://ads.example.com/banner"></iframe>
</body>
</html>
"""

MINIMAL_HTML = """
<html><body>
<p>Core content only.</p>
</body></html>
"""


# =============================================================================
# 1. _clean_html strips nav, footer, header, script, style, aside, iframe tags
# =============================================================================

class TestCleanHtmlStripsStructuralTags:
    """Verify that _clean_html removes all non-content structural tags."""

    @pytest.mark.parametrize("tag", ["nav", "footer", "header", "script", "style", "aside", "iframe"])
    def test_strip_tag(self, tag):
        html = f"<html><body><{tag}>REMOVE ME</{tag}><p>Keep this</p></body></html>"
        result = _clean_html(html)
        assert "REMOVE ME" not in result
        assert "Keep this" in result

    def test_strips_all_structural_tags_from_full_page(self):
        result = _clean_html(BUSINESS_HTML)
        # Content from stripped tags should not appear
        assert "analytics" not in result       # <script>
        assert "display: none" not in result   # <style>
        assert "ads.example.com" not in result  # <iframe>
        assert "Recent tweets" not in result   # <aside>


# =============================================================================
# 2. _clean_html strips elements with ad-related CSS classes
# =============================================================================

class TestCleanHtmlStripsAdElements:
    """Verify elements with ad-indicator class/id substrings are removed."""

    @pytest.mark.parametrize("indicator", [
        "ad-wrapper", "advert-box", "banner-top", "cookie-notice",
        "popup-overlay", "counter-widget", "countdown-timer",
    ])
    def test_strip_ad_class(self, indicator):
        html = f'<html><body><div class="{indicator}">AD CONTENT</div><p>Real content</p></body></html>'
        result = _clean_html(html)
        assert "AD CONTENT" not in result
        assert "Real content" in result

    @pytest.mark.parametrize("indicator", [
        "ad-sidebar", "advert-footer", "banner-slot",
    ])
    def test_strip_ad_id(self, indicator):
        html = f'<html><body><div id="{indicator}">AD CONTENT</div><p>Real content</p></body></html>'
        result = _clean_html(html)
        assert "AD CONTENT" not in result
        assert "Real content" in result


# =============================================================================
# 3. _clean_html strips date patterns
# =============================================================================

class TestCleanHtmlStripsDatePatterns:
    """Verify that date patterns are stripped from the cleaned text."""

    def test_strip_iso_date(self):
        html = "<html><body><p>Updated on 2025-01-15 by admin.</p></body></html>"
        result = _clean_html(html)
        assert "2025-01-15" not in result
        assert "Updated on" in result

    def test_strip_us_date(self):
        html = "<html><body><p>Published January 15, 2025</p></body></html>"
        result = _clean_html(html)
        assert "January 15, 2025" not in result

    def test_strip_us_date_abbreviated(self):
        html = "<html><body><p>Published Jan 15, 2025</p></body></html>"
        result = _clean_html(html)
        assert "Jan 15, 2025" not in result

    def test_strip_copyright_year(self):
        html = "<html><body><p>Copyright 2025 Acme Corp</p></body></html>"
        result = _clean_html(html)
        assert "2025" not in result

    def test_strip_copyright_range(self):
        html = "<html><body><p>\u00a9 2020\u20132025 Acme Corp</p></body></html>"
        result = _clean_html(html)
        assert "2020" not in result
        assert "2025" not in result

    def test_strip_bare_year(self):
        html = "<html><body><p>Established 2019</p></body></html>"
        result = _clean_html(html)
        assert "2019" not in result


# =============================================================================
# 4. _clean_html preserves actual business content
# =============================================================================

class TestCleanHtmlPreservesContent:
    """Verify that meaningful business content survives cleaning."""

    def test_preserves_paragraphs(self):
        result = _clean_html(BUSINESS_HTML)
        assert "proven frameworks" in result

    def test_preserves_headings(self):
        result = _clean_html(BUSINESS_HTML)
        assert "Welcome to Acme Coaching" in result

    def test_preserves_list_items(self):
        result = _clean_html(BUSINESS_HTML)
        assert "Leadership development" in result
        assert "Strategic planning" in result
        assert "Team alignment" in result


# =============================================================================
# 5. _hash_text returns "sha256:" prefixed hex digest
# =============================================================================

class TestHashTextFormat:
    """Verify hash output format."""

    def test_sha256_prefix(self):
        result = _hash_text("hello world")
        assert result.startswith("sha256:")

    def test_hex_digest_after_prefix(self):
        result = _hash_text("hello world")
        hex_part = result[len("sha256:"):]
        # Valid hex string of 64 chars (SHA-256 = 256 bits = 64 hex chars)
        assert len(hex_part) == 64
        int(hex_part, 16)  # Should not raise

    def test_matches_manual_sha256(self):
        text = "test content for hashing"
        expected = "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
        assert _hash_text(text) == expected


# =============================================================================
# 6. _hash_text is deterministic
# =============================================================================

class TestHashTextDeterministic:
    """Same input must always produce same hash."""

    def test_same_input_same_hash(self):
        text = "The quick brown fox jumps over the lazy dog"
        assert _hash_text(text) == _hash_text(text)

    def test_repeated_calls_stable(self):
        text = "Consistent content"
        hashes = [_hash_text(text) for _ in range(100)]
        assert len(set(hashes)) == 1


# =============================================================================
# 7. _hash_text differs for different input
# =============================================================================

class TestHashTextDiffers:
    """Different inputs must produce different hashes."""

    def test_different_content_different_hash(self):
        assert _hash_text("content A") != _hash_text("content B")

    def test_whitespace_difference(self):
        assert _hash_text("hello world") != _hash_text("helloworld")

    def test_case_sensitivity(self):
        assert _hash_text("Hello") != _hash_text("hello")


# =============================================================================
# 8. _normalise_base_url adds https:// if missing
# =============================================================================

class TestNormaliseBaseUrlScheme:
    """Verify scheme handling."""

    def test_adds_https_when_missing(self):
        result = _normalise_base_url("example.com")
        assert result.startswith("https://")

    def test_preserves_existing_https(self):
        result = _normalise_base_url("https://example.com")
        assert result == "https://example.com"

    def test_preserves_existing_http(self):
        result = _normalise_base_url("http://example.com")
        assert result == "http://example.com"

    def test_adds_https_with_path(self):
        result = _normalise_base_url("example.com/page")
        assert result == "https://example.com/page"


# =============================================================================
# 9. _normalise_base_url strips trailing slash
# =============================================================================

class TestNormaliseBaseUrlTrailingSlash:
    """Verify trailing slash removal."""

    def test_strips_trailing_slash(self):
        result = _normalise_base_url("https://example.com/")
        assert not result.endswith("/")
        assert result == "https://example.com"

    def test_strips_trailing_slash_bare_domain(self):
        result = _normalise_base_url("example.com/")
        assert result == "https://example.com"

    def test_handles_whitespace(self):
        result = _normalise_base_url("  example.com/  ")
        assert result == "https://example.com"


# =============================================================================
# Helpers for mocked check_content_hash tests
# =============================================================================

def _mock_logger():
    """Return a standard logger that stands in for prefect's run logger."""
    return logging.getLogger("test_content_hash")


def _make_profile(
    pid="prof-001",
    name="Acme Coaching",
    website="https://acme-coaching.com",
    content_hashes=None,
):
    """Build a minimal profile dict suitable for check_content_hash."""
    em = {}
    if content_hashes is not None:
        em["content_hashes"] = content_hashes
    return {
        "id": pid,
        "name": name,
        "website": website,
        "enrichment_metadata": em,
    }


def _mock_requests_get(html_map):
    """Return a side_effect function for requests.get that serves html_map.

    *html_map* maps URL substrings to HTML strings.  If a URL matches none
    of the keys, a ConnectionError is raised (simulates 404/timeout).
    """
    def _side_effect(url, **kwargs):
        for key, html in html_map.items():
            if key in url:
                resp = MagicMock()
                resp.status_code = 200
                resp.text = html
                resp.raise_for_status = MagicMock()
                return resp
        raise requests.ConnectionError(f"No mock for {url}")
    return _side_effect


def _mock_requests_head_all_404(url, **kwargs):
    """HEAD always returns 404 — so no subpages are discovered."""
    resp = MagicMock()
    resp.status_code = 404
    return resp


# =============================================================================
# 10. Change detection: stored hash differs from new content
# =============================================================================

class TestCheckContentHashChangeDetected:
    """Mock network to return DIFFERENT HTML than what was previously hashed."""

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    @patch("matching.enrichment.flows.content_hash_check.requests.head")
    @patch("matching.enrichment.flows.content_hash_check.requests.get")
    def test_changed_true_when_content_differs(self, mock_get, mock_head, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()
        mock_head.side_effect = _mock_requests_head_all_404

        old_html = "<html><body><p>Old content about leadership.</p></body></html>"
        new_html = "<html><body><p>Completely new content about strategy.</p></body></html>"

        old_cleaned = _clean_html(old_html)
        old_hash = _hash_text(old_cleaned)

        # Profile has stored hash for homepage
        profile = _make_profile(
            content_hashes={"homepage": old_hash},
        )

        # Network returns new (different) HTML
        mock_get.side_effect = _mock_requests_get({"acme-coaching.com": new_html})

        result = check_content_hash.fn(profile)

        assert result.changed is True
        assert "homepage" in result.pages_changed
        assert result.new_hashes["homepage"] != old_hash

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    @patch("matching.enrichment.flows.content_hash_check.requests.head")
    @patch("matching.enrichment.flows.content_hash_check.requests.get")
    def test_pages_changed_populated(self, mock_get, mock_head, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()
        mock_head.side_effect = _mock_requests_head_all_404

        profile = _make_profile(
            content_hashes={"homepage": "sha256:0000000000000000000000000000000000000000000000000000000000000000"},
        )

        new_html = "<html><body><p>Some real content here.</p></body></html>"
        mock_get.side_effect = _mock_requests_get({"acme-coaching.com": new_html})

        result = check_content_hash.fn(profile)

        assert result.changed is True
        assert len(result.pages_changed) >= 1
        assert "homepage" in result.pages_changed


# =============================================================================
# 11. No change: mock returns SAME HTML → changed=False
# =============================================================================

class TestCheckContentHashNoChange:
    """Mock network to return identical HTML → hashes match."""

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    @patch("matching.enrichment.flows.content_hash_check.requests.head")
    @patch("matching.enrichment.flows.content_hash_check.requests.get")
    def test_changed_false_when_content_identical(self, mock_get, mock_head, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()
        mock_head.side_effect = _mock_requests_head_all_404

        html = "<html><body><p>Stable content that does not change.</p></body></html>"
        cleaned = _clean_html(html)
        stored_hash = _hash_text(cleaned)

        profile = _make_profile(
            content_hashes={"homepage": stored_hash},
        )

        mock_get.side_effect = _mock_requests_get({"acme-coaching.com": html})

        result = check_content_hash.fn(profile)

        assert result.changed is False
        assert result.pages_changed == []
        assert result.pages_checked >= 1


# =============================================================================
# 12. No website: profile without website → error='no_website'
# =============================================================================

class TestCheckContentHashNoWebsite:
    """Profile with no website should return early with error."""

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    def test_no_website_empty_string(self, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()

        profile = _make_profile(website="")
        result = check_content_hash.fn(profile)

        assert result.error == "no_website"
        assert result.changed is False
        assert result.pages_checked == 0

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    def test_no_website_none(self, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()

        profile = {
            "id": "prof-002",
            "name": "No Site Inc",
            "website": None,
            "enrichment_metadata": {},
        }
        result = check_content_hash.fn(profile)

        assert result.error == "no_website"

    @patch("matching.enrichment.flows.content_hash_check.get_run_logger")
    def test_no_website_whitespace_only(self, mock_logger_fn):
        mock_logger_fn.return_value = _mock_logger()

        profile = _make_profile(website="   ")
        result = check_content_hash.fn(profile)

        assert result.error == "no_website"


# =============================================================================
# 13. Date stripping prevents false positive: two HTML docs identical except
#     for copyright year → hashes match after cleaning
# =============================================================================

class TestDateStrippingPreventsFalsePositive:
    """Changing only dates/copyright years should NOT trigger a hash change."""

    def test_copyright_year_change_same_hash(self):
        html_2024 = """
        <html><body>
          <h1>Acme Coaching</h1>
          <p>We provide executive coaching services.</p>
          <footer><p>Copyright 2024 Acme Inc. All rights reserved.</p></footer>
        </body></html>
        """
        html_2025 = """
        <html><body>
          <h1>Acme Coaching</h1>
          <p>We provide executive coaching services.</p>
          <footer><p>Copyright 2025 Acme Inc. All rights reserved.</p></footer>
        </body></html>
        """
        cleaned_2024 = _clean_html(html_2024)
        cleaned_2025 = _clean_html(html_2025)

        assert _hash_text(cleaned_2024) == _hash_text(cleaned_2025)

    def test_iso_date_change_same_hash(self):
        html_a = "<html><body><p>Report generated 2024-06-15. Our mission is excellence.</p></body></html>"
        html_b = "<html><body><p>Report generated 2025-01-20. Our mission is excellence.</p></body></html>"

        assert _hash_text(_clean_html(html_a)) == _hash_text(_clean_html(html_b))

    def test_us_date_change_same_hash(self):
        html_a = "<html><body><p>Last updated March 1, 2024. We value integrity.</p></body></html>"
        html_b = "<html><body><p>Last updated November 22, 2025. We value integrity.</p></body></html>"

        assert _hash_text(_clean_html(html_a)) == _hash_text(_clean_html(html_b))

    def test_real_content_change_different_hash(self):
        """Sanity check: actual content change IS detected despite same date."""
        html_a = "<html><body><p>We offer coaching. Updated 2024-01-01.</p></body></html>"
        html_b = "<html><body><p>We offer consulting. Updated 2024-01-01.</p></body></html>"

        assert _hash_text(_clean_html(html_a)) != _hash_text(_clean_html(html_b))
