"""
AI-Powered Profile Research Service

Fetches REAL data from partner websites and extracts accurate profile information.
This is NOT inference - it's research to find what they actually say about themselves.

Uses Crawl4AI for intelligent site crawling (BestFirst strategy with keyword scoring)
and Claude for structured data extraction.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

from django.conf import settings

from matching.enrichment.text_sanitizer import TextSanitizer

logger = logging.getLogger(__name__)


class ContentQualityChecker:
    """Pre-AI-call content quality gate. Skips parked domains and thin content."""

    PARKED_SIGNALS = [
        'domain for sale', 'buy this domain', 'this domain is parked',
        'parked by', 'domain parking', 'this page is under construction',
        'coming soon', 'website coming soon',
    ]
    MIN_CONTENT_LENGTH = 200

    @classmethod
    def check(cls, content: str, url: str = '') -> tuple[bool, str]:
        """Returns (should_proceed, reason). False = skip AI call."""
        if not content or not content.strip():
            return False, "empty content"

        text = content.strip().lower()

        if len(text) < cls.MIN_CONTENT_LENGTH:
            return False, f"thin content ({len(text)} chars < {cls.MIN_CONTENT_LENGTH})"

        for signal in cls.PARKED_SIGNALS:
            if signal in text[:500]:  # Only check first 500 chars
                return False, f"parked domain signal: '{signal}'"

        # Check for error pages
        error_signals = ['404 not found', '403 forbidden', 'page not found', 'access denied']
        for signal in error_signals:
            if signal in text[:300]:
                return False, f"error page signal: '{signal}'"

        return True, "ok"


class ExtractionValidator:
    """Post-AI-extraction field-level validation."""

    FIELD_MAX_LENGTHS = {
        'niche': 100,
        'company': 150,
        'phone': 30,
        'business_size': 30,
        'audience_type': 80,
        'business_focus': 150,
    }
    MAX_TAGS = 10
    MAX_LIST_SIZE = 50_000_000  # 50M is reasonable max for an email list

    @classmethod
    def validate(cls, extracted: dict) -> list[str]:
        """Validate extracted fields. Returns list of issues (empty = all good)."""
        issues = []

        # Field length checks
        for field, max_len in cls.FIELD_MAX_LENGTHS.items():
            value = extracted.get(field)
            if value and isinstance(value, str) and len(value) > max_len:
                extracted[field] = value[:max_len]
                issues.append(f"{field} truncated from {len(value)} to {max_len} chars")

        # Tags count check
        tags = extracted.get('tags')
        if tags and isinstance(tags, list) and len(tags) > cls.MAX_TAGS:
            extracted['tags'] = tags[:cls.MAX_TAGS]
            issues.append(f"tags truncated from {len(tags)} to {cls.MAX_TAGS}")

        # List size reasonableness
        list_size = extracted.get('list_size')
        if list_size is not None:
            try:
                ls = int(list_size)
                if ls > cls.MAX_LIST_SIZE:
                    extracted['list_size'] = None
                    issues.append(f"list_size {ls} exceeds max {cls.MAX_LIST_SIZE}, removed")
                elif ls < 0:
                    extracted['list_size'] = None
                    issues.append(f"list_size {ls} is negative, removed")
            except (ValueError, TypeError):
                pass

        return issues


# ---------------------------------------------------------------------------
# Social media regex patterns (deterministic, no AI needed)
# ---------------------------------------------------------------------------
SOCIAL_PLATFORM_PATTERNS = {
    'youtube': re.compile(r'youtube\.com/(?:c/|channel/|@)([\w-]+)', re.IGNORECASE),
    'instagram': re.compile(r'instagram\.com/([\w.]+)', re.IGNORECASE),
    'facebook': re.compile(r'facebook\.com/([\w.]+)', re.IGNORECASE),
    'tiktok': re.compile(r'tiktok\.com/@([\w.]+)', re.IGNORECASE),
    'twitter': re.compile(r'(?:twitter|x)\.com/([\w]+)', re.IGNORECASE),
    'linkedin': re.compile(r'linkedin\.com/(?:in|company)/([\w-]+)', re.IGNORECASE),
    'pinterest': re.compile(r'pinterest\.com/([\w]+)', re.IGNORECASE),
}

PODCAST_PLATFORM_PATTERNS = [
    re.compile(r'podcasts\.apple\.com/[\w/-]+', re.IGNORECASE),
    re.compile(r'open\.spotify\.com/show/[\w]+', re.IGNORECASE),
    re.compile(r'anchor\.fm/([\w-]+)', re.IGNORECASE),
    re.compile(r'podbean\.com/([\w-]+)', re.IGNORECASE),
]

BOOKING_LINK_PATTERNS = [
    re.compile(r'https?://(?:www\.)?calendly\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?acuityscheduling\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?savvycal\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?tidycal\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?hubspot\.com/meetings/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?zcal\.co/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
]

PRICE_PATTERNS = [
    re.compile(r'\$[\d,]+(?:\.\d{2})?'),           # $997, $25,000
    re.compile(r'starting at \$[\d,]+', re.IGNORECASE),
    re.compile(r'investment of \$[\d,]+', re.IGNORECASE),
    re.compile(r'(?:only|just) \$[\d,]+', re.IGNORECASE),
    re.compile(r'\$[\d,]+/(?:month|mo|year|yr)', re.IGNORECASE),
]

# Keywords for Crawl4AI BestFirst strategy - pages most likely to contain JV signals
CRAWL_KEYWORDS = [
    "about", "speaking", "partners", "affiliates", "collaborate", "jv",
    "podcast", "media", "press", "work-with-me", "services", "programs",
    "courses", "coaching", "pricing", "events", "testimonials", "team",
    "our-story", "results", "book", "schedule",
]

# URL path patterns to always skip
SKIP_URL_PATTERNS = [
    r'/blog/', r'/blog$', r'/tag/', r'/category/', r'/wp-content/',
    r'/privacy', r'/terms', r'/cart', r'/login', r'/signup',
    r'/wp-admin', r'/feed', r'\.xml$', r'\.pdf$', r'\.jpg$', r'\.png$',
]

# ---------------------------------------------------------------------------
# Proxy configuration (set CRAWLER_PROXY_URL env var when needed)
# Supports HTTP, HTTPS, and SOCKS5 proxies.
# Examples: "http://proxy:8080", "socks5://proxy:1080"
# ---------------------------------------------------------------------------
PROXY_URL = os.environ.get('CRAWLER_PROXY_URL', None)

# ---------------------------------------------------------------------------
# Social reach scraping – platform URL templates and follower-count regexes
# ---------------------------------------------------------------------------
SOCIAL_PLATFORM_URLS = {
    'youtube': 'https://www.youtube.com/@{handle}',
    'instagram': 'https://www.instagram.com/{handle}/',
    'twitter': 'https://x.com/{handle}',
    'facebook': 'https://www.facebook.com/{handle}',
    'tiktok': 'https://www.tiktok.com/@{handle}',
}

# Each platform gets a list of regexes tried in order; first match wins.
SOCIAL_FOLLOWER_PATTERNS = {
    'youtube': [
        re.compile(r'"subscriberCountText":\{"simpleText":"([\d.]+[KMB]?) subscribers"\}'),
        re.compile(r'([\d][\d,.]*[KMB]?)\s*subscribers', re.IGNORECASE),
    ],
    'instagram': [
        re.compile(r'"edge_followed_by":\{"count":(\d+)\}'),
        re.compile(r'([\d][\d,.]*[KMB]?)\s*[Ff]ollowers'),
    ],
    'twitter': [
        re.compile(r'([\d][\d,.]*[KMB]?)\s*[Ff]ollowers'),
    ],
    'facebook': [
        re.compile(r'([\d][\d,.]*[KMB]?)\s*(?:likes|followers)', re.IGNORECASE),
    ],
    'tiktok': [
        re.compile(r'"followerCount":(\d+)'),
        re.compile(r'([\d][\d,.]*[KMB]?)\s*[Ff]ollowers'),
    ],
}

_SOCIAL_SCRAPE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def _parse_follower_count(text: str) -> int:
    """
    Convert human-readable follower count strings to integers.

    Examples:
        "1.2M"   -> 1_200_000
        "50K"    -> 50_000
        "10,000" -> 10_000
        "500"    -> 500
        "2.5B"   -> 2_500_000_000

    Returns 0 if the string cannot be parsed.
    """
    if not text:
        return 0

    text = text.strip().replace(',', '')

    multipliers = {
        'B': 1_000_000_000,
        'M': 1_000_000,
        'K': 1_000,
    }

    suffix = text[-1].upper()
    if suffix in multipliers:
        try:
            return int(float(text[:-1]) * multipliers[suffix])
        except (ValueError, TypeError):
            return 0

    # Plain integer
    try:
        return int(float(text))
    except (ValueError, TypeError):
        return 0


def scrape_social_reach(social_links: Dict[str, str]) -> int:
    """
    Visit social media profile pages and extract follower/subscriber counts.

    This is a **best-effort** scraper.  Social platforms actively block automated
    requests, so individual platforms may return 0.  The function never raises --
    all exceptions are caught, logged, and treated as 0 for that platform.

    Args:
        social_links: Dict mapping platform name to handle/identifier,
                      e.g. ``{'youtube': 'ChannelName', 'instagram': 'handle'}``.

    Returns:
        Total followers across all platforms (int).  Returns 0 when every
        platform fails or the input is empty.
    """
    import requests as _requests  # imported here to keep module-level imports light

    if not social_links:
        return 0

    total = 0
    proxies = {'http': PROXY_URL, 'https': PROXY_URL} if PROXY_URL else None

    for platform, handle in social_links.items():
        if platform not in SOCIAL_PLATFORM_URLS:
            logger.debug(f"Social reach: skipping unsupported platform '{platform}'")
            continue

        profile_url = SOCIAL_PLATFORM_URLS[platform].format(handle=handle)

        try:
            resp = _requests.get(
                profile_url,
                headers=_SOCIAL_SCRAPE_HEADERS,
                timeout=3,
                proxies=proxies,
                allow_redirects=True,
            )

            if resp.status_code != 200:
                logger.warning(
                    f"Social reach: {platform} ({profile_url}) returned HTTP {resp.status_code} -- skipping"
                )
                continue

            html = resp.text
            patterns = SOCIAL_FOLLOWER_PATTERNS.get(platform, [])
            count = 0

            for pattern in patterns:
                match = pattern.search(html)
                if match:
                    count = _parse_follower_count(match.group(1))
                    if count > 0:
                        break

            if count > 0:
                logger.info(f"Social reach: {platform} @{handle} -> {count:,} followers")
                total += count
            else:
                logger.info(f"Social reach: {platform} @{handle} -> could not parse follower count")

        except _requests.exceptions.Timeout:
            logger.warning(f"Social reach: {platform} ({profile_url}) timed out (3s) -- skipping")
        except _requests.exceptions.RequestException as e:
            logger.warning(f"Social reach: {platform} ({profile_url}) request error: {e} -- skipping")
        except Exception as e:
            logger.warning(f"Social reach: {platform} ({profile_url}) unexpected error: {e} -- skipping")

    logger.info(f"Social reach total: {total:,} across {len(social_links)} platform(s)")
    return total


def extract_social_links(urls: List[str]) -> Dict[str, str]:
    """
    Extract structured social media handles/URLs from a list of URLs.
    Pure regex - no AI, no cost.
    """
    platforms = {}
    for url in urls:
        for platform, pattern in SOCIAL_PLATFORM_PATTERNS.items():
            match = pattern.search(url)
            if match:
                handle = match.group(1)
                # Skip generic handles
                if handle.lower() not in ('share', 'sharer', 'intent', 'dialog', 'login'):
                    platforms[platform] = handle
                    break

        # Check podcast platforms
        if 'podcast_url' not in platforms:
            for pattern in PODCAST_PLATFORM_PATTERNS:
                if pattern.search(url):
                    platforms['podcast_url'] = url
                    break

    return platforms


def extract_booking_links(urls: List[str]) -> Optional[str]:
    """
    Extract a booking/calendar URL from a list of URLs.
    Pure regex - no AI, no cost. Returns the first match found.
    """
    for url in urls:
        for pattern in BOOKING_LINK_PATTERNS:
            match = pattern.search(url)
            if match:
                return match.group(0)
    return None


def extract_price_signals(text: str) -> List[str]:
    """Extract price mentions from text for revenue tier hints."""
    prices = []
    for pattern in PRICE_PATTERNS:
        for match in pattern.finditer(text):
            prices.append(match.group(0))
    return list(set(prices))[:10]  # Dedupe, cap at 10


def calculate_engagement_score(content_platforms: Dict) -> float:
    """
    Compute audience engagement score (0.0-1.0) from detected platforms.

    Philosophy: A single platform with a massive audience beats five platforms
    with tiny followings. Since we can't reliably scrape follower counts, we:
    - Give a solid base (0.40) for ANY content platform presence
    - Weight deep-investment platforms higher (podcast, YouTube, newsletter)
    - Treat additional platforms as a small breadth bonus, not the main driver
    """
    has_podcast = bool(
        content_platforms.get('podcast_name') or content_platforms.get('podcast_url')
    )
    has_youtube = bool(
        content_platforms.get('youtube_channel') or content_platforms.get('youtube')
    )
    has_newsletter = bool(content_platforms.get('newsletter_name'))
    has_instagram = bool(
        content_platforms.get('instagram_handle') or content_platforms.get('instagram')
    )
    has_facebook = bool(
        content_platforms.get('facebook_group') or content_platforms.get('facebook')
    )
    has_tiktok = bool(
        content_platforms.get('tiktok_handle') or content_platforms.get('tiktok')
    )

    active = [has_podcast, has_youtube, has_newsletter, has_instagram, has_facebook, has_tiktok]
    active_count = sum(active)

    if active_count == 0:
        return 0.0

    # Base: any content presence means they're actively creating
    score = 0.40

    # Deep-investment platforms get additional weight
    if has_podcast:
        score += 0.15   # Podcasts require sustained effort, deep audience trust
    if has_youtube:
        score += 0.15   # YouTube = significant content investment
    if has_newsletter:
        score += 0.10   # Newsletter = owned audience (highest-value channel)

    # Small breadth bonus for additional platforms (not the primary driver)
    if active_count >= 2:
        score += min(0.15, (active_count - 1) * 0.05)

    return min(1.0, score)


class SmartCrawler:
    """
    Intelligent site crawler using Crawl4AI's BestFirstCrawlingStrategy.
    Discovers and fetches high-value pages (about, speaking, partners, etc.)
    while skipping low-value content (blog posts, privacy, terms).
    """

    MAX_PAGES = 12
    MAX_CONTENT_CHARS = 25000
    CRAWL_TIMEOUT = 60  # seconds

    def crawl_site(self, url: str) -> Tuple[str, List[str]]:
        """
        Crawl a site intelligently, returning combined markdown content
        and a list of all external URLs found (for social media extraction).

        Falls back to simple requests+BS4 if Crawl4AI fails.

        Returns:
            Tuple of (combined_markdown_content, external_urls)
        """
        try:
            content, urls = self._crawl_with_crawl4ai(url)
            if content:
                return content, urls
        except Exception as e:
            logger.warning(f"Crawl4AI failed for {url}, falling back to simple fetch: {e}")

        # Fallback: simple single-page fetch
        content = self._simple_fetch(url)
        return content or '', []

    def _crawl_with_crawl4ai(self, url: str) -> Tuple[str, List[str]]:
        """Use Crawl4AI BestFirstCrawlingStrategy for intelligent multi-page crawl."""
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
        from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
        from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer

        scorer = KeywordRelevanceScorer(
            keywords=CRAWL_KEYWORDS,
            weight=0.7,
        )

        crawl_config = CrawlerRunConfig(
            deep_crawl_strategy=BestFirstCrawlingStrategy(
                max_depth=1,
                max_pages=self.MAX_PAGES,
                url_scorer=scorer,
            ),
            exclude_external_links=False,  # We want to capture social media links
            verbose=False,
        )

        browser_config = BrowserConfig(
            headless=True,
            java_script_enabled=False,  # Most JV partner sites are static
            proxy=PROXY_URL,  # None when no proxy is configured
        )

        combined_content = []
        external_urls = []
        total_chars = 0

        async def _run():
            nonlocal total_chars
            async with AsyncWebCrawler(config=browser_config) as crawler:
                results = await crawler.arun(url=url, config=crawl_config)

                # results may be a single result or a list
                if not isinstance(results, list):
                    results = [results]

                for result in results:
                    if not result.success:
                        continue

                    page_content = result.markdown_v2.fit_markdown if hasattr(result, 'markdown_v2') and result.markdown_v2 else result.markdown
                    if not page_content:
                        continue

                    # Skip if this would push us over the content limit
                    if total_chars + len(page_content) > self.MAX_CONTENT_CHARS:
                        # Take what fits
                        remaining = self.MAX_CONTENT_CHARS - total_chars
                        if remaining > 500:
                            page_content = page_content[:remaining]
                        else:
                            break

                    # Tag content with source page
                    page_url = result.url or url
                    path = urlparse(page_url).path or '/'
                    combined_content.append(f"\n--- PAGE: {path} ---\n{page_content}")
                    total_chars += len(page_content)

                    # Collect external links for social media extraction
                    if hasattr(result, 'links') and result.links:
                        ext_links = result.links.get('external', [])
                        for link in ext_links:
                            link_url = link.get('href', '') if isinstance(link, dict) else str(link)
                            if link_url:
                                external_urls.append(link_url)

        # Run the async crawl
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, create a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    pool.submit(lambda: asyncio.run(_run())).result(timeout=self.CRAWL_TIMEOUT)
            else:
                asyncio.run(_run())
        except Exception as e:
            logger.warning(f"Async crawl error for {url}: {e}")
            # Try a simpler approach if the async run fails
            try:
                asyncio.run(_run())
            except Exception as e2:
                logger.warning(f"Async crawl retry also failed for {url}: {e2}")

        content = '\n'.join(combined_content)
        logger.info(f"Crawl4AI: {url} -> {len(combined_content)} pages, {total_chars} chars, {len(external_urls)} external links")
        return content, external_urls

    def _simple_fetch(self, url: str) -> Optional[str]:
        """Fallback: single-page fetch with requests + BeautifulSoup."""
        try:
            import requests
            from bs4 import BeautifulSoup

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }

            proxies = {'http': PROXY_URL, 'https': PROXY_URL} if PROXY_URL else None
            response = requests.get(url, headers=headers, timeout=25, proxies=proxies)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract external links before cleaning
            external_urls = []
            parsed_base = urlparse(url)
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if href.startswith('http') and urlparse(href).netloc != parsed_base.netloc:
                    external_urls.append(href)

            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()

            text = soup.get_text(separator='\n')
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            return text[:self.MAX_CONTENT_CHARS]

        except Exception as e:
            logger.error(f"Simple fetch failed for {url}: {e}")
            return None


class ProfileResearchService:
    """
    Researches sparse profiles by intelligently crawling their website and extracting
    accurate data about what they do, who they serve, and what they're seeking.

    Uses Crawl4AI for multi-page site discovery and Claude for data extraction.
    """

    def __init__(self, use_agents: bool = False):
        from .claude_client import ClaudeClient
        openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')
        self.client = ClaudeClient(
            max_tokens=2048,
            openrouter_key=openrouter_key,
            anthropic_key=anthropic_key,
        )
        self.api_key = self.client.api_key
        self.use_openrouter = self.client.use_openrouter
        self.model = self.client.model
        self.max_tokens = self.client.max_tokens
        self.use_agents = use_agents
        # Cache raw website content for downstream verification (Layer 2)
        self._content_cache: Dict[str, str] = {}
        self._crawler = SmartCrawler()

    def get_cached_content(self, key: str) -> Optional[str]:
        """Retrieve cached raw content for a profile (used by verification gate Layer 2)."""
        return self._content_cache.get(key)

    def research_profile(self, name: str, website: str, existing_data: Dict, skip_social_reach: bool = False) -> Dict:
        """
        Research a profile by intelligently crawling their website and extracting data.

        Performs:
        1. Smart crawl (multi-page with keyword-scored prioritization)
        2. Social media link extraction (free, regex-based)
        2a. Social reach scraping (follower/subscriber counts via HTTP + regex)
        2b. Booking link extraction (free, regex-based)
        3. Core profile extraction (Prompt 1 - what_you_do, who_you_serve, seeking,
           offering, social_proof, signature_programs, booking_link, niche, phone,
           current_projects, company, list_size, business_size)
        3b. Booking link merge (regex wins over AI for URL reliability)
        4. Extended signals extraction (Prompt 2 - revenue tier, JV history, content platforms)
        5. Social reach attachment (total followers integer)

        Args:
            name: Partner's name
            website: Partner's website URL
            existing_data: Any data we already have (to avoid overwriting good data)

        Returns:
            Dict with researched fields including new extended signals
        """
        if not website:
            logger.info(f"No website for {name}, skipping research")
            return {}

        if not self.api_key:
            logger.warning("No API key configured for research")
            return {}

        # Normalize website URL
        website = self._normalize_url(website)

        # Step 1: Smart crawl - fetch multiple high-value pages
        content, external_urls = self._crawler.crawl_site(website)
        if not content:
            logger.warning(f"Could not fetch website for {name}: {website}")
            return {}

        # Cache raw content for downstream verification (gate Layer 2)
        cache_key = name.lower().strip()
        self._content_cache[cache_key] = content

        # Content quality gate: skip AI calls for parked/thin/error pages
        proceed, quality_reason = ContentQualityChecker.check(content, website)
        if not proceed:
            logger.warning(f"Content quality gate BLOCKED {name} ({website}): {quality_reason}")
            return {}

        # Step 2: Extract social media links (free, deterministic)
        social_links = extract_social_links(external_urls)
        logger.info(f"  Social links for {name}: {social_links}")

        # Step 2a: Scrape social reach (follower/subscriber counts)
        social_reach = 0 if skip_social_reach else scrape_social_reach(social_links)

        # Step 2b: Extract booking links (free, deterministic)
        regex_booking_link = extract_booking_links(external_urls)
        if regex_booking_link:
            logger.info(f"  Booking link for {name}: {regex_booking_link}")

        # Step 3: Core profile extraction (Prompt 1)
        if getattr(self, 'use_agents', False):
            researched = self._extract_profile_data_agent(name, content, website, existing_data)
        else:
            researched = self._extract_profile_data(name, content, website, existing_data)

        # Step 3b: Merge booking link — regex wins over AI (more reliable for URLs)
        if regex_booking_link:
            researched['booking_link'] = regex_booking_link
            metadata = researched.get('_extraction_metadata', {})
            if 'booking_link' not in metadata.get('fields_updated', []):
                metadata.setdefault('fields_updated', []).append('booking_link')

        # Step 4: Extended signals extraction (Prompt 2)
        if getattr(self, 'use_agents', False):
            extended = self._extract_extended_signals_agent(name, content, website, social_links)
        else:
            extended = self._extract_extended_signals(name, content, website, social_links)
        if extended:
            researched['_extended_signals'] = extended
            # Merge social links with AI-extracted platform data
            content_platforms = extended.get('content_platforms', {})
            # Social regex is more reliable for handles; AI fills in names
            for platform, handle in social_links.items():
                if platform not in content_platforms or not content_platforms[platform]:
                    content_platforms[platform] = handle
            researched['_extended_signals']['content_platforms'] = content_platforms

        # Compute engagement score from combined platform data
        platforms = researched.get('_extended_signals', {}).get('content_platforms', social_links)
        researched['_audience_engagement_score'] = calculate_engagement_score(platforms)

        # Store discovered partnership page for intent signal
        researched['_has_partnership_page'] = any(
            kw in content.lower()
            for kw in ['partner with', 'affiliate program', 'collaborate with', 'jv partner', 'joint venture']
        )

        # Step 5: Attach social reach (total followers across all platforms)
        if social_reach > 0:
            researched['social_reach'] = social_reach
            metadata = researched.get('_extraction_metadata', {})
            if 'social_reach' not in metadata.get('fields_updated', []):
                metadata.setdefault('fields_updated', []).append('social_reach')

        return researched

    def _normalize_url(self, url: str) -> str:
        """Ensure URL has proper scheme."""
        url = url.strip()
        if not url.startswith('http'):
            url = 'https://' + url
        return url

    def _extract_profile_data(self, name: str, content: str, website: str, existing: Dict) -> Dict:
        """
        Prompt 1: Extract core profile fields from multi-page website content.
        Only extracts what they ACTUALLY say - no inference or fabrication.
        """
        # Truncate content for Prompt 1 if needed (keep within token limits)
        prompt_content = content[:15000]

        prompt = f"""You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated on the website.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Website: {website}

Website Content (from multiple pages):
<content>
{prompt_content}
</content>

Extract the following fields. Only include information that is DIRECTLY stated:

1. what_you_do: What is their primary business/service? (1-2 sentences max)
   - Look for: "I help...", "We provide...", "Our mission...", About section

2. who_you_serve: Who is their target audience? (1 sentence max)
   - Look for: "I work with...", "For...", "Serving...", client descriptions

3. seeking: What are they actively looking for? (partnerships, speaking, affiliates, etc.)
   - Look for: "Looking for...", "Seeking...", "Partner with us", JV/affiliate mentions
   - Also check for partnership/collaborate/affiliate pages
   - If nothing explicitly stated, leave EMPTY

4. offering: What do they offer to partners/collaborators?
   - Look for: Podcast, email list mentions, speaking platforms, courses, certifications
   - Only include if they explicitly mention offering it to others

5. social_proof: Any notable credentials (bestseller, certifications, audience size)
   - Only include verifiable claims they make

6. signature_programs: Named courses, books, frameworks, certifications, or signature methodologies
   - Look for: Program names, course titles, book titles, certification names, trademark/™ symbols
   - Only include named, specific programs (not generic "coaching" or "consulting")

7. booking_link: Calendar booking URL (Calendly, Acuity, SavvyCal, HubSpot meetings, etc.)
   - Look for: URLs containing calendly.com, acuityscheduling.com, savvycal.com, hubspot.com/meetings, tidycal.com
   - Also look for "Book a call", "Schedule a meeting" link destinations

8. niche: Their primary market niche or industry vertical (1-3 words)
   - Examples: "health coaching", "real estate investing", "B2B SaaS", "personal development"

9. phone: Business phone number if publicly displayed
   - Only include if explicitly shown on the website
   - Format: as displayed on the site

10. current_projects: Active launches, programs, or initiatives they're currently promoting
    - Look for: "Now enrolling", "Coming soon", "New!", "Currently", "Join our upcoming"
    - Only include if they're actively promoting it

11. company: Company or business name (may differ from personal name)
    - Look for: LLC, Inc, Co, company name in footer/header, "About [Company Name]"
    - Only include if clearly stated and different from their personal name

12. list_size: Email list or audience size as an integer
    - Look for: "Join X subscribers", "X+ community members", "audience of X"
    - Only include if a specific number is mentioned
    - Return as an integer (e.g., 50000 not "50K")

13. business_size: Business scale indicator
    - Look for: Team size mentions, revenue indicators, "solo", "team of X", employee count
    - Categories: "solo", "small_team" (2-10), "medium" (11-50), "large" (50+)

14. tags: 3-7 keyword tags describing their expertise, industry, and focus areas
    - Generate from what you've learned about them (not from existing site tags/categories)
    - Each tag should be 1-3 words, lowercase
    - Used for search and categorization

15. audience_type: The type of audience they serve (1-2 words)
    - Examples: "B2B", "B2C", "coaches", "entrepreneurs", "executives", "parents", "creatives"
    - Based on who_you_serve but more categorical

16. business_focus: Their primary business focus area in 1 sentence
    - Short summary combining what_you_do + niche
    - Example: "Executive leadership coaching for Fortune 500 companies"

17. service_provided: Specific services they offer (comma-separated list)
    - Examples: "1:1 coaching, group programs, online courses, speaking, consulting"
    - Only include services explicitly mentioned on the site

Return as JSON. Use empty string "" for fields without explicit information (use null for list_size if unknown):
{{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "social_proof": "",
    "signature_programs": "",
    "booking_link": "",
    "niche": "",
    "phone": "",
    "current_projects": "",
    "company": "",
    "list_size": null,
    "business_size": "",
    "tags": ["tag1", "tag2", "tag3"],
    "audience_type": "",
    "business_focus": "",
    "service_provided": "",
    "confidence": "high/medium/low",
    "source_quotes": ["quote1", "quote2"]
}}

IMPORTANT:
- "confidence" should be "high" only if you found clear, explicit statements
- Include "source_quotes" with 1-2 direct quotes from the content that support your extraction
- If you're unsure, set confidence to "low" and leave the field empty
- Business accuracy matters - do NOT fabricate or assume
- list_size MUST be an integer or null (never a string like "50K")"""

        response = self._call_claude(prompt)
        return self._parse_research_response(response, name, existing)

    def _extract_extended_signals(
        self, name: str, content: str, website: str, social_links: Dict
    ) -> Optional[Dict]:
        """
        Prompt 2: Extract extended signals - revenue tier, JV history, content platforms.
        Only runs when we have multipage content to work with.
        """
        # Pre-scan for price signals to give Claude structured hints
        price_signals = extract_price_signals(content)
        price_hint = f"\nDetected price mentions: {', '.join(price_signals)}" if price_signals else ""

        social_hint = ""
        if social_links:
            social_hint = f"\nDetected social media links: {json.dumps(social_links)}"

        prompt_content = content[:15000]

        prompt = f"""You are a business intelligence analyst extracting PARTNERSHIP and REVENUE signals.

CRITICAL: Only extract information that is EXPLICITLY stated or clearly demonstrated.
DO NOT fabricate partnerships, prices, or platform names.

Person: {name}
Website: {website}
{price_hint}
{social_hint}

Website Content (from multiple pages):
<content>
{prompt_content}
</content>

Extract the following:

1. revenue_tier: Classify their pricing level based on what you see:
   - "micro": Products under $100, free content only
   - "emerging": $100-$999 (courses, group programs)
   - "established": $1,000-$9,999 (masterminds, certifications)
   - "premium": $10,000-$50,000 (high-touch coaching, consulting)
   - "enterprise": $50,000+ (corporate contracts, licensing)
   - Indirect signals: "Book a call" / "Apply now" often means $3,000+; "Add to cart" with price means direct purchase
   - If unclear, use empty string ""

2. jv_history: List any partnerships, collaborations, or guest appearances you find:
   - "As seen on..." / "Featured in..." / "Podcast guest on..."
   - Speaking engagements, summit appearances
   - Co-authored content, bundle participations
   - ONLY include what is explicitly stated

3. content_platforms: What content platforms do they use?
   - Podcast name (not just a link - the actual show name)
   - YouTube channel name
   - Newsletter name
   - Any other named content properties

4. audience_engagement_signals: Evidence of active audience engagement
   - Subscriber/follower counts mentioned
   - Community mentions (Facebook group, membership site)
   - Download/listener counts

Return as JSON:
{{
    "revenue_tier": "",
    "revenue_signals": [],
    "jv_history": [
        {{"partner_name": "...", "format": "podcast_guest|summit_speaker|bundle|affiliate|co_author|webinar_guest|endorsement", "source_quote": "..."}}
    ],
    "content_platforms": {{
        "podcast_name": "",
        "youtube_channel": "",
        "instagram_handle": "",
        "facebook_group": "",
        "tiktok_handle": "",
        "newsletter_name": ""
    }},
    "audience_engagement_signals": "",
    "confidence": "high/medium/low",
    "source_quotes": []
}}

IMPORTANT:
- For jv_history, only include partnerships you can cite from the content
- For content_platforms, prefer actual names over generic "has a podcast"
- Revenue tier should be based on evidence, not assumptions
- If you cannot determine something, use empty string or empty array"""

        response = self._call_claude(prompt)
        if not response:
            return None

        return self._parse_json_response(response)

    def _call_claude(self, prompt: str) -> Optional[str]:
        """Call Claude via OpenRouter or Anthropic API."""
        if not hasattr(self, 'client') or not self.client:
            return None
        return self.client.call(prompt)

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """Parse a JSON response from Claude, handling markdown code blocks."""
        from .claude_client import ClaudeClient
        return ClaudeClient.parse_json(response)

    def _parse_research_response(self, response: str, name: str, existing: Dict) -> Dict:
        """Parse Prompt 1 AI response and merge with existing data, preserving extraction metadata."""
        result = {}

        data = self._parse_json_response(response)
        if not data:
            return result

        # Post-extraction field-level validation
        validation_issues = ExtractionValidator.validate(data)
        if validation_issues:
            logger.info(f"ExtractionValidator issues for {name}: {validation_issues}")

        confidence = data.get('confidence', 'low')
        source_quotes = data.get('source_quotes', [])

        logger.info(f"Researched {name}: confidence={confidence}")
        if source_quotes:
            for quote in source_quotes[:2]:
                logger.info(f"  Quote: '{quote[:80]}...'")

        extraction_metadata = {
            'source': 'website_research',
            'confidence': confidence,
            'source_quotes': source_quotes,
            'extracted_at': datetime.now().isoformat(),
            'fields_updated': [],
        }

        if confidence in ('high', 'medium'):
            for field in ['what_you_do', 'who_you_serve', 'seeking', 'offering']:
                new_value = data.get(field, '').strip()
                existing_value = existing.get(field, '').strip()

                if new_value and (not existing_value or len(existing_value) < 10):
                    # Clean list fields (offering, seeking) for leading commas
                    if field in ('offering', 'seeking'):
                        new_value = TextSanitizer.clean_list_field(new_value)
                    if new_value:
                        result[field] = new_value
                        extraction_metadata['fields_updated'].append(field)
                        logger.info(f"  Added {field}: {new_value[:60]}...")

            social_proof = data.get('social_proof', '').strip()
            if social_proof and not existing.get('bio'):
                # Validate bio for common AI errors (offering-as-role, etc.)
                validated_bio = TextSanitizer.validate_bio(social_proof, name)
                if validated_bio:
                    result['bio'] = validated_bio
                    extraction_metadata['fields_updated'].append('bio')

            # --- New optional fields (string-valued) ---
            for field in ['signature_programs', 'booking_link', 'niche', 'phone',
                          'current_projects', 'company', 'business_size']:
                new_value = (data.get(field) or '').strip() if isinstance(data.get(field), str) else ''
                if new_value:
                    # Validate company names against generic blocklist
                    if field == 'company':
                        new_value = TextSanitizer.validate_company(new_value, name)
                    if new_value:
                        result[field] = new_value
                        extraction_metadata['fields_updated'].append(field)
                        logger.info(f"  Added {field}: {new_value[:60]}...")

            # list_size: must be an integer or None
            raw_list_size = data.get('list_size')
            if raw_list_size is not None:
                try:
                    list_size_int = int(raw_list_size)
                    if list_size_int > 0:
                        result['list_size'] = list_size_int
                        extraction_metadata['fields_updated'].append('list_size')
                        logger.info(f"  Added list_size: {list_size_int}")
                except (ValueError, TypeError):
                    logger.warning(f"  Invalid list_size value '{raw_list_size}', skipping")

            # --- tags: list of strings, cap at 7 ---
            raw_tags = data.get('tags')
            if raw_tags:
                if isinstance(raw_tags, str):
                    # AI returned a comma-separated string instead of a list
                    raw_tags = [t.strip() for t in raw_tags.split(',') if t.strip()]
                if isinstance(raw_tags, list):
                    # Validate: each element must be a non-empty string
                    tags = [str(t).strip().lower() for t in raw_tags if t and str(t).strip()]
                    tags = tags[:7]  # Cap at 7
                    if tags:
                        result['tags'] = tags
                        extraction_metadata['fields_updated'].append('tags')
                        logger.info(f"  Added tags: {tags}")

            # --- audience_type, business_focus, service_provided: string fields ---
            for field in ['audience_type', 'business_focus', 'service_provided']:
                new_value = (data.get(field) or '').strip() if isinstance(data.get(field), str) else ''
                if new_value:
                    result[field] = new_value
                    extraction_metadata['fields_updated'].append(field)
                    logger.info(f"  Added {field}: {new_value[:60]}...")
        else:
            # Log which fields were extracted but dropped due to low confidence
            extracted_fields = [f for f in ['what_you_do', 'who_you_serve', 'seeking', 'offering',
                                            'social_proof', 'signature_programs', 'booking_link',
                                            'niche', 'phone', 'current_projects', 'company',
                                            'business_size', 'list_size', 'tags', 'audience_type',
                                            'business_focus', 'service_provided']
                                if data.get(f) and str(data.get(f)).strip()]
            logger.warning(f"  Low confidence for {name} — dropping {len(extracted_fields)} extracted fields: {extracted_fields}")

        result['_extraction_metadata'] = extraction_metadata
        return result

    # ── Pydantic AI agent-based extraction methods ──────────────────

    def _extract_profile_data_agent(
        self, name: str, content: str, website: str, existing: Dict
    ) -> Dict:
        """Prompt 1 via Pydantic AI agent: structured profile extraction."""
        from .claude_client import research_agent, get_pydantic_model
        from .schemas import Confidence

        model = get_pydantic_model()
        if not model:
            logger.warning("No Pydantic AI model available, falling back to raw call")
            return self._extract_profile_data(name, content, website, existing)

        prompt_content = content[:15000]
        prompt = (
            f"Person: {name}\nWebsite: {website}\n\n"
            f"Website Content (from multiple pages):\n<content>\n{prompt_content}\n</content>"
        )

        try:
            result_obj = research_agent.run_sync(prompt, model=model)
            extraction = result_obj.output
        except Exception as e:
            logger.error(f"Agent extraction failed for {name}: {e}")
            return self._extract_profile_data(name, content, website, existing)

        # Apply same confidence gating and merge logic as _parse_research_response
        result = {}
        confidence = extraction.confidence.value

        extraction_metadata = {
            'source': 'website_research',
            'confidence': confidence,
            'source_quotes': extraction.source_quotes,
            'extracted_at': datetime.now().isoformat(),
            'fields_updated': [],
        }

        if confidence in ('high', 'medium'):
            for field in ['what_you_do', 'who_you_serve', 'seeking', 'offering']:
                new_value = getattr(extraction, field).strip()
                existing_value = existing.get(field, '').strip()
                if new_value and (not existing_value or len(existing_value) < 10):
                    result[field] = new_value
                    extraction_metadata['fields_updated'].append(field)

            if extraction.social_proof and not existing.get('bio'):
                result['bio'] = extraction.social_proof
                extraction_metadata['fields_updated'].append('bio')

            for field in ['signature_programs', 'booking_link', 'niche', 'phone',
                          'current_projects', 'company']:
                new_value = getattr(extraction, field).strip()
                if new_value:
                    result[field] = new_value
                    extraction_metadata['fields_updated'].append(field)

            if extraction.business_size.value:
                result['business_size'] = extraction.business_size.value
                extraction_metadata['fields_updated'].append('business_size')

            if extraction.list_size is not None and extraction.list_size > 0:
                result['list_size'] = extraction.list_size
                extraction_metadata['fields_updated'].append('list_size')

            if extraction.tags:
                result['tags'] = extraction.tags
                extraction_metadata['fields_updated'].append('tags')

            for field in ['audience_type', 'business_focus', 'service_provided']:
                new_value = getattr(extraction, field).strip()
                if new_value:
                    result[field] = new_value
                    extraction_metadata['fields_updated'].append(field)
        else:
            logger.warning(f"  Low confidence for {name} (agent) — dropping extracted fields")

        result['_extraction_metadata'] = extraction_metadata
        return result

    def _extract_extended_signals_agent(
        self, name: str, content: str, website: str, social_links: Dict
    ) -> Optional[Dict]:
        """Prompt 2 via Pydantic AI agent: structured extended signals extraction."""
        from .claude_client import extended_signals_agent, get_pydantic_model

        model = get_pydantic_model()
        if not model:
            return self._extract_extended_signals(name, content, website, social_links)

        prompt_content = content[:15000]
        price_signals = extract_price_signals(content)
        price_hint = f"\nDetected price mentions: {', '.join(price_signals)}" if price_signals else ""
        social_hint = f"\nDetected social media links: {json.dumps(social_links)}" if social_links else ""

        prompt = (
            f"Person: {name}\nWebsite: {website}{price_hint}{social_hint}\n\n"
            f"Website Content (from multiple pages):\n<content>\n{prompt_content}\n</content>"
        )

        try:
            result_obj = extended_signals_agent.run_sync(prompt, model=model)
            extraction = result_obj.output
        except Exception as e:
            logger.error(f"Extended signals agent failed for {name}: {e}")
            return self._extract_extended_signals(name, content, website, social_links)

        # Convert typed output to dict format expected by the rest of the pipeline
        return {
            'revenue_tier': extraction.revenue_tier.value,
            'revenue_signals': extraction.revenue_signals,
            'jv_history': [p.model_dump() for p in extraction.jv_history],
            'content_platforms': extraction.content_platforms.model_dump(),
            'audience_engagement_signals': extraction.audience_engagement_signals,
            'confidence': extraction.confidence.value,
            'source_quotes': extraction.source_quotes,
        }


class ProfileResearchCache:
    """
    File-based cache for research results with TTL and schema versioning (P4).

    - Configurable path via ENRICHMENT_CACHE_DIR env var or constructor arg
    - Cache TTL: entries older than max_age_days are treated as expired
    - Schema versioning: bump SCHEMA_VERSION when PROFILE_SCHEMA changes to
      auto-invalidate stale cache entries
    """

    SCHEMA_VERSION = 2

    def __init__(self, cache_dir: str = None):
        from pathlib import Path

        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            env_path = os.environ.get('ENRICHMENT_CACHE_DIR')
            if env_path:
                self.cache_dir = Path(env_path)
            else:
                self.cache_dir = (
                    Path(__file__).resolve().parent.parent.parent
                    / 'Chelsea_clients' / 'research_cache'
                )

        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, name: str) -> str:
        """Generate cache key from name."""
        import hashlib
        return hashlib.md5(name.lower().encode()).hexdigest()[:12]

    def get(self, name: str, max_age_days: int = 30) -> Optional[Dict]:
        """Get cached research result if fresh and schema-compatible."""
        cache_file = self.cache_dir / f"{self._cache_key(name)}.json"

        if cache_file.exists():
            # Check TTL via file modification time
            age_days = (time.time() - cache_file.stat().st_mtime) / 86400
            if age_days > max_age_days:
                logger.info(f"Cache expired for {name} ({age_days:.0f} days old)")
                return None

            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                # Schema version check — invalidate outdated entries
                if data.get('_cache_schema_version') != self.SCHEMA_VERSION:
                    logger.info(f"Cache schema outdated for {name}")
                    return None
                return data
            except Exception as e:
                logger.warning(f"Failed to read cache for {name}: {e}")
                cache_file.unlink(missing_ok=True)
        return None

    def set(self, name: str, data: Dict) -> None:
        """Cache research result with schema version stamp."""
        cache_file = self.cache_dir / f"{self._cache_key(name)}.json"

        try:
            data['_cache_schema_version'] = self.SCHEMA_VERSION
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to cache research for {name}: {e}")


def research_and_enrich_profile(
    name: str,
    website: str,
    existing_data: Dict,
    use_cache: bool = True,
    force_research: bool = False,
    linkedin: Optional[str] = None,
    company: Optional[str] = None,
    fill_only: bool = False,
    skip_social_reach: bool = False,
    exa_only: bool = False,
) -> Tuple[Dict, bool]:
    """
    Main entry point: Research a profile and return enriched data.

    Strategy: Exa first (fast, cheap, structured), fallback to crawl4ai + Claude.
    - Exa handles most profiles in 1-2 API calls at ~$0.02/profile
    - If site is not in Exa's index, falls back to crawl4ai + Claude pipeline
    - Name-only profiles (no website) use Exa discovery to find website + LinkedIn

    Args:
        name: Partner name
        website: Partner website (may be empty for name-only profiles)
        existing_data: Current profile data
        use_cache: Whether to use cached results
        force_research: If True, always research (caller has already determined this is needed)
        linkedin: LinkedIn URL (optional, helps Exa extraction)
        company: Company name (optional, helps name-only discovery)
        fill_only: When True, only fill empty fields (never overwrite existing data)
        exa_only: When True, skip crawl4ai + Claude fallback if Exa can't process

    Returns:
        Tuple of (enriched_data, was_researched)
    """
    cache = ProfileResearchCache()

    # Always check cache first (saves Exa credits on re-runs)
    # But skip cache when force_research is set — caller wants fresh data
    if use_cache and not force_research:
        cached = cache.get(name)
        if cached:
            logger.info(f"Using cached research for {name}")
            return cached, False

    # Research if sparse or forced
    is_sparse = not existing_data.get('seeking') and not existing_data.get('who_you_serve')
    should_research = force_research or is_sparse

    if not should_research:
        return existing_data, False

    # === Try Exa first (fast, cheap, structured) ===
    try:
        from matching.enrichment.exa_research import exa_enrich_profile

        exa_merged, exa_enriched = exa_enrich_profile(
            name=name,
            website=website or None,
            linkedin=linkedin,
            company=company,
            existing_data=existing_data,
            fill_only=fill_only,
            skip_social_reach=skip_social_reach,
        )

        exa_meta = exa_merged.get('_extraction_metadata', {})
        exa_indexed = not exa_merged.get('_exa_indexed') is False

        if exa_enriched and exa_indexed:
            # Exa succeeded — use its results
            logger.info(
                f"  Exa enriched {name}: "
                f"{len(exa_meta.get('fields_updated', []))} fields, "
                f"${exa_meta.get('exa_cost', 0):.4f}"
            )

            # Cache the result (without internal metadata)
            if use_cache:
                cache_data = {k: v for k, v in exa_merged.items() if not k.startswith('_')}
                cache.set(name, cache_data)

            return exa_merged, True

        if not exa_indexed:
            logger.info(f"  Exa: {name} not indexed, falling back to crawl4ai + Claude")

    except ImportError:
        logger.info("  exa_py not installed, using crawl4ai + Claude pipeline")
    except Exception as e:
        logger.warning(f"  Exa research failed for {name}, falling back: {e}")

    # === Fallback: crawl4ai + Claude pipeline ===
    if exa_only:
        logger.info(f"  Exa-only mode: skipping crawl4ai + Claude fallback for {name}")
        return existing_data, False

    if website:
        service = ProfileResearchService()
        researched = service.research_profile(name, website, existing_data, skip_social_reach=skip_social_reach)

        if researched:
            # Extract internal metadata before merging
            extraction_metadata = researched.pop('_extraction_metadata', {})
            extended_signals = researched.pop('_extended_signals', {})
            engagement_score = researched.pop('_audience_engagement_score', None)
            has_partnership_page = researched.pop('_has_partnership_page', False)

            # Provenance-aware merge: only overwrite fields that research
            # explicitly updated (tracked in extraction_metadata)
            merged = dict(existing_data)
            updated_fields = extraction_metadata.get('fields_updated', [])

            for field in updated_fields:
                if field in researched:
                    merged[field] = researched[field]

            # Merge extended signals into result
            if extended_signals:
                confidence = extended_signals.get('confidence', 'low')
                if confidence in ('high', 'medium'):
                    if extended_signals.get('revenue_tier'):
                        merged['revenue_tier'] = extended_signals['revenue_tier']
                        extraction_metadata['fields_updated'].append('revenue_tier')
                    if extended_signals.get('jv_history'):
                        merged['jv_history'] = extended_signals['jv_history']
                        extraction_metadata['fields_updated'].append('jv_history')
                    if extended_signals.get('content_platforms'):
                        merged['content_platforms'] = extended_signals['content_platforms']
                        extraction_metadata['fields_updated'].append('content_platforms')
                else:
                    dropped = [f for f in ('revenue_tier', 'jv_history', 'content_platforms')
                               if extended_signals.get(f)]
                    if dropped:
                        logger.warning(
                            f"  Prompt 2 low confidence for {name} — dropping extended signals: {dropped}"
                        )

            if engagement_score is not None:
                merged['audience_engagement_score'] = engagement_score
                extraction_metadata['fields_updated'].append('audience_engagement_score')

            # Store partnership page discovery for intent scoring
            extraction_metadata['has_partnership_page'] = has_partnership_page
            extraction_metadata['extended_signals'] = extended_signals

            # Attach provenance metadata for downstream consumers (gate, Supabase write)
            merged['_extraction_metadata'] = extraction_metadata

            # Cache the result (without internal metadata)
            if use_cache:
                cache_data = {k: v for k, v in merged.items() if not k.startswith('_')}
                cache.set(name, cache_data)

            return merged, True

    return existing_data, False
