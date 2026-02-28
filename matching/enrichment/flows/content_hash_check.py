"""
Prefect @task: Content hash checking for profile freshness monitoring.

Fetches key web pages for a profile, strips dynamic elements, and computes
SHA-256 content hashes.  Compares new hashes against stored hashes in
``enrichment_metadata.content_hashes`` to detect meaningful page changes.

Pages checked per profile:
  - Homepage (/)
  - About page (/about, /about-us, /about-me)
  - Services/programs page (/services, /programs)

Layer 1 of the change-detection pipeline -- FREE (hashlib + requests + BS4).
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from prefect import task, get_run_logger

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_REQUEST_TIMEOUT = 15  # seconds

# Elements stripped before hashing (dynamic / non-content)
_STRIP_TAGS = {"nav", "footer", "header", "script", "style", "aside", "iframe"}

# Regex patterns for dynamic content that should be stripped before hashing
_DATE_PATTERNS = [
    # ISO dates: 2025-01-15, 2025/01/15
    re.compile(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b"),
    # US dates: January 15, 2025 / Jan 15, 2025
    re.compile(
        r"\b(?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|"
        r"Nov|Dec)\s+\d{1,2},?\s+\d{4}\b",
        re.IGNORECASE,
    ),
    # Copyright years: (c) 2025, Copyright 2024-2025
    re.compile(r"(?:copyright|\u00a9|\(c\))\s*\d{4}(?:\s*[-\u2013]\s*\d{4})?", re.IGNORECASE),
    # Bare year references near copyright-like context
    re.compile(r"\b20\d{2}\b"),
]

# CSS class / id substrings that indicate ad or counter widgets
_AD_INDICATORS = {"ad-", "advert", "banner", "cookie", "popup", "counter", "countdown"}

# Candidate about-page paths (tried in order)
_ABOUT_PATHS = ["/about", "/about-us", "/about-me", "/our-story"]

# Candidate services-page paths (tried in order)
_SERVICES_PATHS = ["/services", "/programs", "/what-we-do", "/our-services"]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class HashCheckResult:
    """Result of checking content hashes for a single profile."""

    profile_id: str
    name: str
    website: str
    changed: bool = False
    pages_checked: int = 0
    pages_changed: list[str] = field(default_factory=list)
    new_hashes: dict[str, str] = field(default_factory=dict)   # page_key -> sha256:hex
    old_hashes: dict[str, str] = field(default_factory=dict)   # page_key -> sha256:hex
    error: str = ""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_page(url: str) -> Optional[str]:
    """Fetch a single URL and return raw HTML, or None on failure."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text
    except requests.RequestException:
        return None


def _clean_html(html: str) -> str:
    """Strip dynamic elements from HTML and return normalised plain text.

    Removes navigation, footer, header, scripts, styles, aside, ad divs,
    date strings, and counters so that only meaningful content remains.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove structural non-content tags
    for tag_name in _STRIP_TAGS:
        for el in soup.find_all(tag_name):
            el.decompose()

    # Remove elements whose class or id looks like ads / counters
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", []))
        el_id = el.get("id", "") or ""
        combined = f"{classes} {el_id}".lower()
        if any(indicator in combined for indicator in _AD_INDICATORS):
            el.decompose()

    # Extract text
    text = soup.get_text(separator="\n")

    # Strip date patterns to avoid false positives on copyright updates etc.
    for pattern in _DATE_PATTERNS:
        text = pattern.sub("", text)

    # Normalise whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = "\n".join(chunk for chunk in chunks if chunk)

    return text


def _hash_text(text: str) -> str:
    """Return a prefixed SHA-256 hex digest of *text*."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _resolve_subpage(base_url: str, candidates: list[str]) -> Optional[str]:
    """Try each candidate path against *base_url*; return the first that responds 200."""
    for path in candidates:
        url = urljoin(base_url, path)
        try:
            resp = requests.head(
                url,
                headers={"User-Agent": _USER_AGENT},
                timeout=8,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                return url
        except requests.RequestException:
            continue
    return None


def _normalise_base_url(website: str) -> str:
    """Ensure the website string has a scheme and trailing slash."""
    url = website.strip().rstrip("/")
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="check-content-hashes", retries=1, retry_delay_seconds=5)
def check_content_hash(profile: dict) -> HashCheckResult:
    """Fetch key pages for a profile and compare content hashes.

    Pages checked: homepage (/), about (/about, /about-us, /about-me),
    services/programs page.  Uses requests + BeautifulSoup to fetch and
    strip dynamic elements (nav, footer, scripts, ads, date counters)
    before hashing.

    Compares new hashes against stored hashes in
    ``enrichment_metadata["content_hashes"]``.

    Parameters
    ----------
    profile:
        Dict with at least ``id``, ``name``, ``website``, and
        ``enrichment_metadata`` keys.

    Returns
    -------
    HashCheckResult
    """
    log = get_run_logger()
    pid = str(profile.get("id", ""))
    name = profile.get("name", "")
    website = (profile.get("website") or "").strip()

    result = HashCheckResult(profile_id=pid, name=name, website=website)

    if not website:
        result.error = "no_website"
        return result

    base_url = _normalise_base_url(website)

    # Existing hashes from enrichment_metadata
    em = profile.get("enrichment_metadata") or {}
    old_hashes: dict[str, str] = em.get("content_hashes", {})
    result.old_hashes = dict(old_hashes)

    # --- Build page map: key -> URL ---
    pages: dict[str, str] = {"homepage": base_url}

    about_url = _resolve_subpage(base_url, _ABOUT_PATHS)
    if about_url:
        pages["about"] = about_url

    services_url = _resolve_subpage(base_url, _SERVICES_PATHS)
    if services_url:
        pages["services"] = services_url

    # --- Fetch, clean, hash each page ---
    for page_key, page_url in pages.items():
        html = _fetch_page(page_url)
        if html is None:
            log.debug("Could not fetch %s for %s (%s)", page_key, name, page_url)
            continue

        cleaned = _clean_html(html)
        if not cleaned.strip():
            log.debug("Empty content after cleaning %s for %s", page_key, name)
            continue

        new_hash = _hash_text(cleaned)
        result.new_hashes[page_key] = new_hash
        result.pages_checked += 1

        stored_hash = old_hashes.get(page_key)
        if stored_hash and stored_hash != new_hash:
            result.pages_changed.append(page_key)

    result.changed = len(result.pages_changed) > 0

    if result.changed:
        log.info(
            "Change detected for %s (%s): %s",
            name, pid, ", ".join(result.pages_changed),
        )
    else:
        log.debug("No change for %s (%s) — %d pages checked", name, pid, result.pages_checked)

    return result


@task(name="check-hashes-batch")
def check_hashes_batch(profiles: list[dict]) -> list[HashCheckResult]:
    """Batch hash check using parallel task submission.

    Submits ``check_content_hash`` for each profile, then collects
    results.  A small delay between submissions avoids hammering the
    same hosts.

    Parameters
    ----------
    profiles:
        List of profile dicts.

    Returns
    -------
    list[HashCheckResult]
    """
    log = get_run_logger()
    log.info("Starting batch hash check for %d profiles", len(profiles))

    futures = []
    for i, profile in enumerate(profiles):
        future = check_content_hash.submit(profile)
        futures.append(future)
        # Polite crawling: small stagger every 5 profiles
        if (i + 1) % 5 == 0:
            time.sleep(0.5)

    results: list[HashCheckResult] = []
    for future in futures:
        try:
            results.append(future.result())
        except Exception as exc:
            log.error("Hash check task failed: %s", exc)

    changed_count = sum(1 for r in results if r.changed)
    error_count = sum(1 for r in results if r.error)
    log.info(
        "Batch hash check complete: %d checked, %d changed, %d errors",
        len(results), changed_count, error_count,
    )

    return results
