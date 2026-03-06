"""
Zero-cost affiliate link detection using BeautifulSoup.

Extracts all links from email HTML and flags affiliate/tracking URLs
based on known affiliate patterns, query parameters, and redirect domains.
"""

import re
from urllib.parse import urlparse, parse_qs

AFFILIATE_QUERY_PARAMS = frozenset([
    'ref', 'aff', 'hop', 'tid', 'affid', 'affiliate', 'partner',
    'utm_source', 'via', 'source', 'clickid', 'subid',
])

AFFILIATE_PATH_PATTERNS = re.compile(
    r'/(go|aff|refer|click|track|out|recommend|r)/',
    re.IGNORECASE,
)

AFFILIATE_DOMAINS = {
    'clickbank.net': 'ClickBank',
    'jvzoo.com': 'JVZoo',
    'warriorplus.com': 'WarriorPlus',
    'shareasale.com': 'ShareASale',
    'cj.com': 'CJ Affiliate',
    'commission-junction.com': 'CJ Affiliate',
    'impact.com': 'Impact',
    'impactradius.com': 'Impact',
    'partnerstack.com': 'PartnerStack',
    'digistore24.com': 'Digistore24',
    'paykickstart.com': 'PayKickstart',
    'thrivecart.com': 'ThriveCart',
    'infusionsoft.com': 'Keap',
    'samcart.com': 'SamCart',
}

LINK_SHORTENERS = frozenset([
    'bit.ly', 'tinyurl.com', 'ow.ly', 't.co', 'goo.gl',
    'buff.ly', 'dlvr.it', 'ift.tt', 'rb.gy', 'lnkd.in',
])


def _detect_affiliate(url: str) -> tuple[bool, str]:
    """
    Check if a URL is an affiliate link.

    Returns (is_affiliate, network_name).
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ''
        # Strip www.
        hostname = hostname.removeprefix('www.')

        # Known affiliate domains
        for domain, network in AFFILIATE_DOMAINS.items():
            if hostname == domain or hostname.endswith('.' + domain):
                return True, network

        # Known link shorteners (potential tracking)
        if hostname in LINK_SHORTENERS:
            return True, 'link_shortener'

        # Affiliate path patterns (/go/, /aff/, etc.)
        if AFFILIATE_PATH_PATTERNS.search(parsed.path):
            return True, 'path_pattern'

        # Affiliate query parameters
        params = parse_qs(parsed.query)
        for param in AFFILIATE_QUERY_PARAMS:
            if param in params:
                return True, f'query_param:{param}'

    except Exception:
        pass

    return False, ''


def extract_links(html_or_text: str) -> list[dict]:
    """
    Extract and classify all links from email HTML (or plain text).

    Returns a list of dicts:
    [{url, anchor_text, is_affiliate, affiliate_network}]
    """
    if not html_or_text:
        return []

    links: list[dict] = []
    seen_urls: set[str] = set()

    # Try HTML parsing first
    if '<a ' in html_or_text or '<A ' in html_or_text:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_or_text, 'html.parser')
            for tag in soup.find_all('a', href=True):
                url = tag['href'].strip()
                if not url or url.startswith('mailto:') or url.startswith('#'):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                is_affiliate, network = _detect_affiliate(url)
                links.append({
                    'url': url,
                    'anchor_text': tag.get_text(strip=True)[:200],
                    'is_affiliate': is_affiliate,
                    'affiliate_network': network,
                })
        except Exception:
            pass
    else:
        # Plain text: extract raw URLs
        for url in re.findall(r'https?://[^\s<>"\']+', html_or_text):
            url = url.rstrip('.,;)')
            if url in seen_urls:
                continue
            seen_urls.add(url)
            is_affiliate, network = _detect_affiliate(url)
            links.append({
                'url': url,
                'anchor_text': '',
                'is_affiliate': is_affiliate,
                'affiliate_network': network,
            })

    return links
