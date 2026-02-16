"""
Exa.ai-Powered Profile Research Service

Uses Exa's semantic search and structured extraction to enrich JV partner profiles.
Replaces Claude API calls for profile extraction with Exa's summary+schema feature,
reducing cost from ~$0.03/profile (crawl4ai + 2 Claude calls) to ~$0.02/profile.

Key advantages over crawl4ai + Claude:
- Structured JSON extraction via Exa summary+schema (no Claude call needed)
- LinkedIn access (Exa indexes LinkedIn; crawlers get blocked)
- Social media profile discovery across platforms
- JV partnership signal discovery across the web
- Name-only profile enrichment (discovers website + LinkedIn from just a name)

Cost per profile:
- Has website (Exa-indexed): ~$0.020 (3 API calls)
- Has website (not indexed): Falls back to crawl4ai + Claude pipeline
- Name-only: ~$0.025 (4 API calls)
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Social platform domains for discovery search
SOCIAL_DOMAINS = [
    "youtube.com", "instagram.com", "facebook.com", "linkedin.com",
    "twitter.com", "x.com", "open.spotify.com", "podcasts.apple.com",
    "tiktok.com",
]

# Domains to skip when discovering a profile's website
SKIP_DOMAINS = [
    'amazon.', 'wikipedia.', 'linkedin.', 'youtube.', 'facebook.',
    'instagram.', 'twitter.', 'x.com', 'reddit.', 'tiktok.',
    'spotify.', 'apple.com', 'goodreads.', 'imdb.', 'crunchbase.',
]

# Subpage targets most likely to contain JV-relevant data
JV_SUBPAGE_TARGETS = [
    "about", "services", "programs", "speaking", "partners",
    "work-with-me", "coaching", "courses", "affiliates", "collaborate",
    "podcast", "certifications", "pricing",
]

# The JSON schema Exa uses to extract structured profile data
# Exa summary+schema supports max 16 fields. We keep 16 here and derive
# booking_link (from page links), business_focus (what_you_do+niche),
# and audience_type (from who_you_serve) in the merge logic.
PROFILE_SCHEMA = {
    "type": "object",
    "properties": {
        "what_you_do": {
            "type": "string",
            "description": "Primary business or service in 1-2 sentences. What does this person/company do?"
        },
        "who_you_serve": {
            "type": "string",
            "description": "Target audience and audience type (B2B, B2C, coaches, entrepreneurs, etc.) in 1-2 sentences."
        },
        "seeking": {
            "type": "string",
            "description": "What partnerships or collaborations they are actively seeking. Empty string if not mentioned."
        },
        "offering": {
            "type": "string",
            "description": "What they offer partners (podcast, email list, speaking platform, courses, audience access)"
        },
        "niche": {
            "type": "string",
            "description": "Primary market niche in 1-3 words (e.g. 'life coaching', 'real estate investing', 'B2B SaaS')"
        },
        "signature_programs": {
            "type": "string",
            "description": "Named courses, books, frameworks, certifications, or signature methodologies."
        },
        "revenue_tier": {
            "type": "string",
            "enum": ["micro", "emerging", "established", "premium", "enterprise", "unknown"],
            "description": "Pricing level: micro(<$100), emerging($100-999), established($1K-9K), premium($10K-50K), enterprise($50K+)"
        },
        "company": {
            "type": "string",
            "description": "Business or company name if different from personal name"
        },
        "social_proof": {
            "type": "string",
            "description": "Notable credentials: bestseller status, certifications, audience size, awards, media features"
        },
        "service_provided": {
            "type": "string",
            "description": "Comma-separated list of services: 1:1 coaching, group programs, courses, speaking, consulting"
        },
        "phone": {
            "type": "string",
            "description": "Business phone number if publicly displayed on the website"
        },
        "current_projects": {
            "type": "string",
            "description": "Active launches, programs, or initiatives currently being promoted"
        },
        "business_size": {
            "type": "string",
            "enum": ["solo", "small_team", "medium", "large", "unknown"],
            "description": "Business scale: solo (1 person), small_team (2-10), medium (11-50), large (50+)"
        },
        "list_size": {
            "type": "integer",
            "description": "Email list or audience size as integer. Only if a specific number is mentioned."
        },
        "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "3-7 keyword tags for expertise, industry, focus areas. Each 1-3 words, lowercase."
        },
        "email": {
            "type": "string",
            "description": "Public business email address if displayed on the website."
        },
    },
    "required": ["what_you_do", "who_you_serve", "niche"]
}


class ExaResearchService:
    """
    Enriches JV partner profiles using Exa.ai's search and content extraction.

    Optimized strategy for profiles with a website (1-2 calls):
      Call 1 (always): get_contents(url) + summary schema + subpages + extras
      Call 2 (conditional): social + JV discovery search — only if Call 1
        didn't find enough social links (< 2) or JV partnership data

    Strategy for name-only profiles (2-3 calls):
      Call 1: search(name) to discover website + LinkedIn
      Call 2: get_contents(discovered_url) + summary schema
      Call 3 (conditional): social + JV discovery if still missing
    """

    def __init__(self):
        self.api_key = os.environ.get('EXA_API_KEY', '')
        if not self.api_key:
            # Fallback: try loading from .env if Django hasn't loaded it yet
            try:
                from dotenv import load_dotenv
                from pathlib import Path
                env_path = Path(__file__).resolve().parent.parent.parent / '.env'
                if env_path.exists():
                    load_dotenv(env_path)
                    self.api_key = os.environ.get('EXA_API_KEY', '')
            except ImportError:
                pass
        self._client = None

    @property
    def client(self):
        """Lazy-init Exa client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError("EXA_API_KEY not set")
            from exa_py import Exa
            self._client = Exa(api_key=self.api_key)
        return self._client

    @property
    def available(self) -> bool:
        """Check if Exa is configured."""
        return bool(self.api_key)

    def research_profile(
        self,
        name: str,
        website: Optional[str] = None,
        linkedin: Optional[str] = None,
        company: Optional[str] = None,
        existing_data: Optional[Dict] = None,
    ) -> Dict:
        """
        Research a profile using Exa.ai.

        Args:
            name: Partner's name
            website: Website URL (may be None for name-only profiles)
            linkedin: LinkedIn URL (may be None)
            company: Company name (helps name-only discovery)
            existing_data: Current profile data

        Returns:
            Dict with enriched fields + internal metadata (_exa_* keys)
        """
        if not self.available:
            logger.warning("Exa API key not configured, skipping Exa research")
            return {}

        existing = existing_data or {}
        result = {}
        total_cost = 0.0

        try:
            # Normalize website URL
            if website:
                website = self._normalize_url(website)
                # Skip non-website URLs (calendly, facebook, linkedin, etc.)
                if self._is_non_website_url(website):
                    logger.info(f"  Exa: {name} has non-website URL ({website}), treating as name-only")
                    website = None

            if website:
                # === Strategy A: Has website ===
                profile_data, links, cost = self._extract_from_website(name, website)
                total_cost += cost

                if profile_data:
                    result.update(profile_data)
                    result['_exa_source'] = 'website'
                else:
                    # Exa doesn't have this site indexed
                    logger.info(f"  Exa: {website} not indexed, returning empty for fallback")
                    result['_exa_indexed'] = False
                    return result

                # Extract social links from page links
                if links:
                    page_social = self._extract_social_from_links(links)
                    if page_social:
                        result['_exa_page_social'] = page_social

            else:
                # === Strategy B: Name-only ===
                discovered, cost = self._discover_profile(name, company)
                total_cost += cost

                if discovered.get('website'):
                    website = discovered['website']
                    result['_exa_discovered_website'] = website

                    # Extract from discovered website
                    profile_data, links, cost = self._extract_from_website(name, website)
                    total_cost += cost
                    if profile_data:
                        result.update(profile_data)
                        result['_exa_source'] = 'discovered_website'

                if discovered.get('linkedin') and not linkedin:
                    linkedin = discovered['linkedin']
                    result['_exa_discovered_linkedin'] = linkedin

                # If we still have nothing, try LinkedIn extraction
                if not result.get('what_you_do') and linkedin:
                    linkedin_data, cost = self._extract_from_linkedin(name, linkedin)
                    total_cost += cost
                    if linkedin_data:
                        for k, v in linkedin_data.items():
                            if v and not result.get(k):
                                result[k] = v
                        result['_exa_source'] = result.get('_exa_source', 'linkedin')

                # Name-only: if discovery found nothing useful, signal empty
                if not discovered and not result.get('_exa_source'):
                    result['_exa_indexed'] = False
                    return result

            # === Conditional: social + JV discovery ===
            # Skip if Call 1 already found enough social links and JV data
            page_social = result.get('_exa_page_social', {})
            has_enough_social = len(page_social) >= 2
            has_jv_data = bool(result.get('jv_partnerships') or result.get('_exa_jv_mentions'))

            if not has_enough_social or not has_jv_data:
                social_profiles, jv_data, cost = self._discover_social_and_jv(
                    name, result.get('company', company or '')
                )
                total_cost += cost
                if social_profiles:
                    result['_exa_social_profiles'] = social_profiles
                if jv_data:
                    result['_exa_jv_mentions'] = jv_data
                logger.info(
                    f"  Exa Call 2 for {name}: "
                    f"{len(social_profiles)} social, {len(jv_data)} JV mentions"
                )
            else:
                logger.info(f"  Exa Call 2 skipped for {name}: enough data from Call 1")

            result['_exa_cost'] = total_cost
            result['_exa_indexed'] = True

            fields_found = [k for k in result if not k.startswith('_') and result[k]]
            logger.info(
                f"  Exa research for {name}: {len(fields_found)} fields, "
                f"${total_cost:.4f} cost"
            )

        except Exception as e:
            logger.error(f"  Exa research failed for {name}: {e}", exc_info=True)
            result['_exa_error'] = str(e)

        return result

    def _extract_from_website(
        self, name: str, url: str
    ) -> Tuple[Dict, List[str], float]:
        """
        Call 1: Extract structured profile data from a website URL.
        Uses Exa get_contents with summary+schema for direct JSON extraction.

        Returns: (profile_dict, page_links, cost)
        """
        try:
            result = self.client.get_contents(
                urls=[url],
                summary={
                    "query": (
                        f"Extract complete business profile for {name}: "
                        "what they do, who they serve, their programs, "
                        "pricing tier, credentials, partnerships, services"
                    ),
                    "schema": PROFILE_SCHEMA,
                },
                subpages=3,
                subpage_target=JV_SUBPAGE_TARGETS,
                extras={"links": 25},
                livecrawl="fallback",
            )
        except Exception as e:
            logger.warning(f"  Exa get_contents failed for {url}: {e}")
            return {}, [], 0.0

        cost = result.cost_dollars.total if result.cost_dollars else 0.0
        profile = {}
        links = []

        if not result.results:
            return {}, [], cost

        r = result.results[0]

        # Parse structured summary
        if r.summary:
            try:
                data = json.loads(r.summary) if isinstance(r.summary, str) else r.summary
                for k, v in data.items():
                    if v and v not in ('unknown', '', 'Unknown'):
                        profile[k] = v
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"  Exa summary parse failed: {e}")

        # Collect links from extras
        if hasattr(r, 'extras') and r.extras:
            page_links = r.extras.get('links', [])
            links = [l for l in page_links if isinstance(l, str)]

        # Also extract from subpages if they have summaries
        if hasattr(r, 'subpages') and r.subpages:
            for sp in r.subpages:
                sp_summary = sp.get('summary') if isinstance(sp, dict) else getattr(sp, 'summary', None)
                if sp_summary:
                    try:
                        sp_data = json.loads(sp_summary) if isinstance(sp_summary, str) else sp_summary
                        # Merge subpage data, preferring non-empty values we don't already have
                        for k, v in sp_data.items():
                            if v and v not in ('unknown', '', 'Unknown') and not profile.get(k):
                                profile[k] = v
                    except (json.JSONDecodeError, TypeError):
                        pass

        return profile, links, cost

    def _extract_from_linkedin(
        self, name: str, linkedin_url: str
    ) -> Tuple[Dict, float]:
        """Extract profile data from a LinkedIn URL."""
        try:
            result = self.client.get_contents(
                urls=[linkedin_url],
                summary={
                    "query": (
                        f"Extract professional profile for {name}: "
                        "what they do, who they serve, credentials, experience, company"
                    ),
                    "schema": PROFILE_SCHEMA,
                },
                livecrawl="fallback",
            )
        except Exception as e:
            logger.warning(f"  Exa LinkedIn extraction failed for {linkedin_url}: {e}")
            return {}, 0.0

        cost = result.cost_dollars.total if result.cost_dollars else 0.0
        profile = {}

        if result.results and result.results[0].summary:
            try:
                data = json.loads(result.results[0].summary) if isinstance(result.results[0].summary, str) else result.results[0].summary
                for k, v in data.items():
                    if v and v not in ('unknown', '', 'Unknown'):
                        profile[k] = v
            except (json.JSONDecodeError, TypeError):
                pass

        return profile, cost

    def _discover_profile(
        self, name: str, company: Optional[str] = None
    ) -> Tuple[Dict, float]:
        """
        For name-only profiles: search to discover their website and LinkedIn.
        Returns: ({"website": url, "linkedin": url}, cost)
        """
        query_parts = [name]
        if company and company not in ('More Info', 'None', ''):
            query_parts.append(company)
        query_parts.extend(["coach", "author", "speaker", "entrepreneur"])
        query = " ".join(query_parts)

        try:
            result = self.client.search(
                query=query,
                type="auto",
                num_results=5,
                contents=False,
            )
        except Exception as e:
            logger.warning(f"  Exa discovery search failed for {name}: {e}")
            return {}, 0.0

        cost = result.cost_dollars.total if result.cost_dollars else 0.0
        discovered = {}

        for r in result.results:
            url = r.url
            # Find LinkedIn
            if 'linkedin.com/in/' in url and 'linkedin' not in discovered:
                discovered['linkedin'] = url
            # Find personal website (skip social, marketplace, wiki sites)
            elif not any(d in url for d in SKIP_DOMAINS):
                if 'website' not in discovered:
                    discovered['website'] = url

        if discovered:
            logger.info(
                f"  Exa discovered for {name}: "
                f"website={discovered.get('website', 'N/A')}, "
                f"linkedin={discovered.get('linkedin', 'N/A')}"
            )

        return discovered, cost

    def _discover_social_and_jv(
        self, name: str, company: str = ''
    ) -> Tuple[Dict[str, str], List[Dict], float]:
        """
        Combined Call 2: Discover social media profiles AND JV partnership signals
        in a single search using include_domains for social platforms.
        Highlights capture JV signals from social platform content.

        Returns: (social_profiles, jv_mentions, cost)
        """
        query = f"{name} {company}".strip()

        try:
            result = self.client.search(
                query=query,
                type="auto",
                num_results=10,
                include_domains=SOCIAL_DOMAINS,
                contents={
                    "highlights": {
                        "query": (
                            "partnership collaboration guest podcast speaker "
                            "summit affiliate interview featured"
                        ),
                        "num_sentences": 2,
                        "highlights_per_url": 1,
                    }
                },
            )
        except Exception as e:
            logger.warning(f"  Exa social+JV search failed for {name}: {e}")
            return {}, [], 0.0

        cost = result.cost_dollars.total if result.cost_dollars else 0.0

        platform_map = {
            'youtube': 'youtube',
            'instagram': 'instagram',
            'facebook': 'facebook',
            'linkedin': 'linkedin',
            'twitter': 'twitter',
            'x.com': 'twitter',
            'tiktok': 'tiktok',
            'open.spotify.com': 'spotify_podcast',
            'podcasts.apple.com': 'apple_podcast',
        }

        jv_keywords = [
            'partner', 'collaborat', 'guest', 'summit', 'affiliate',
            'joint', 'sponsor', 'bundle', 'co-', 'featured', 'keynote',
            'panelist', 'interview',
        ]

        profiles = {}
        mentions = []

        for r in result.results:
            # Extract social profile
            for domain, platform in platform_map.items():
                if domain in r.url and platform not in profiles:
                    profiles[platform] = r.url
                    break

            # Extract JV signals from highlights
            if r.highlights:
                for h in r.highlights:
                    if any(kw in h.lower() for kw in jv_keywords):
                        mentions.append({
                            "url": r.url,
                            "title": r.title or '',
                            "quote": h[:300],
                        })

        return profiles, mentions[:10], cost

    def _extract_social_from_links(self, links: List[str]) -> Dict[str, str]:
        """Extract social media handles/URLs from page links."""
        social = {}
        for link in links:
            if not isinstance(link, str):
                continue
            for domain, platform in [
                ('youtube.com', 'youtube'), ('instagram.com', 'instagram'),
                ('facebook.com', 'facebook'), ('linkedin.com', 'linkedin'),
                ('twitter.com', 'twitter'), ('x.com', 'twitter'),
                ('tiktok.com', 'tiktok'), ('calendly.com', 'booking_link'),
                ('acuityscheduling.com', 'booking_link'),
                ('savvycal.com', 'booking_link'),
                ('open.spotify.com', 'spotify_podcast'),
                ('podcasts.apple.com', 'apple_podcast'),
            ]:
                if domain in link and platform not in social:
                    social[platform] = link
                    break

        return social

    def _normalize_url(self, url: str) -> str:
        """Ensure URL has proper scheme."""
        url = url.strip()
        if not url.startswith('http'):
            url = 'https://' + url
        return url

    def _is_non_website_url(self, url: str) -> bool:
        """Check if URL is a social/booking link rather than a real website."""
        non_website_domains = [
            'calendly.com', 'acuityscheduling.com', 'savvycal.com',
            'tidycal.com', 'hubspot.com/meetings', 'zcal.co',
            'facebook.com', 'linkedin.com', 'instagram.com',
            'tinyurl.com', 'bit.ly', 'youtube.com', 'twitter.com',
            'x.com', 'tiktok.com',
        ]
        return any(d in url.lower() for d in non_website_domains)

    def check_indexed(self, url: str) -> bool:
        """Quick check if Exa has a URL indexed (no content retrieval)."""
        try:
            result = self.client.get_contents(
                urls=[url],
                text={"max_characters": 100},
            )
            return bool(result.results and result.results[0].text)
        except Exception:
            return False


def exa_enrich_profile(
    name: str,
    website: Optional[str] = None,
    linkedin: Optional[str] = None,
    company: Optional[str] = None,
    existing_data: Optional[Dict] = None,
    fill_only: bool = False,
    skip_social_reach: bool = False,
) -> Tuple[Dict, bool]:
    """
    Convenience function: enrich a profile using Exa, merging with existing data.

    Args:
        fill_only: When True, only fill empty/null fields — never overwrite existing
            data. Used for re-enrichment (Tier 0) to add new fields without
            touching existing good data.

    Returns:
        Tuple of (merged_data, was_enriched)
    """
    service = ExaResearchService()
    if not service.available:
        return existing_data or {}, False

    existing = existing_data or {}
    exa_result = service.research_profile(
        name=name,
        website=website,
        linkedin=linkedin,
        company=company,
        existing_data=existing,
    )

    if not exa_result or exa_result.get('_exa_indexed') is False:
        return existing, False

    # Build extraction metadata
    extraction_metadata = {
        'source': 'exa_research',
        'confidence': 'high',  # Exa summary+schema is reliable
        'extracted_at': datetime.now().isoformat(),
        'fields_updated': [],
        'exa_cost': exa_result.get('_exa_cost', 0),
    }

    # Merge profile fields (text fields — only update if new data and existing is empty/short)
    merged = dict(existing)
    text_fields = [
        'what_you_do', 'who_you_serve', 'seeking', 'offering', 'niche',
        'signature_programs', 'revenue_tier', 'company', 'service_provided',
        'phone', 'current_projects', 'business_size',
    ]

    for field in text_fields:
        new_value = exa_result.get(field, '')
        if isinstance(new_value, str):
            new_value = new_value.strip()
        if new_value and new_value != 'unknown':
            existing_value = existing.get(field, '')
            if isinstance(existing_value, str):
                existing_value = existing_value.strip()
            if fill_only:
                # Fill-only mode: only write if existing is truly empty
                if not existing_value:
                    merged[field] = new_value
                    extraction_metadata['fields_updated'].append(field)
            else:
                # Normal mode: write if existing is empty or very short
                if not existing_value or len(str(existing_value)) < 10:
                    merged[field] = new_value
                    extraction_metadata['fields_updated'].append(field)

    # list_size: integer, only update if new > existing
    raw_list_size = exa_result.get('list_size')
    if raw_list_size is not None:
        try:
            new_list_size = int(raw_list_size)
            existing_list_size = int(existing.get('list_size') or 0)
            if new_list_size > 0 and new_list_size > existing_list_size:
                merged['list_size'] = new_list_size
                merged['enriched_list_size'] = new_list_size  # for Supabase CASE logic
                extraction_metadata['fields_updated'].append('list_size')
        except (ValueError, TypeError):
            pass

    # tags: list of strings, cap at 7 — respect fill_only parameter (P3)
    raw_tags = exa_result.get('tags')
    if raw_tags:
        if isinstance(raw_tags, str):
            raw_tags = [t.strip() for t in raw_tags.split(',') if t.strip()]
        if isinstance(raw_tags, list):
            tags = [str(t).strip().lower() for t in raw_tags if t and str(t).strip()][:7]
            if tags:
                if fill_only and existing.get('tags'):
                    pass  # fill-only: don't overwrite existing tags
                else:
                    merged['tags'] = tags
                    extraction_metadata['fields_updated'].append('tags')

    # email: only if Exa found one and existing is empty (always fill-only)
    exa_email = exa_result.get('email', '').strip()
    if exa_email and '@' in exa_email and not existing.get('email'):
        merged['email'] = exa_email
        extraction_metadata['fields_updated'].append('email')

    # social_proof: preserve in its own field AND fill bio as fallback (P2)
    social_proof = exa_result.get('social_proof', '').strip()
    if social_proof:
        merged['social_proof'] = social_proof
        extraction_metadata['fields_updated'].append('social_proof')
        if not existing.get('bio'):
            merged['bio'] = social_proof
            extraction_metadata['fields_updated'].append('bio')

    # Derive business_focus from what_you_do + niche (removed from schema to stay <=16 fields)
    if not merged.get('business_focus') or len(str(merged.get('business_focus', ''))) < 10:
        what = merged.get('what_you_do', '')
        niche = merged.get('niche', '')
        if what and niche and niche.lower() not in what.lower():
            merged['business_focus'] = f"{what} ({niche})"
            extraction_metadata['fields_updated'].append('business_focus')
        elif what:
            merged['business_focus'] = what
            extraction_metadata['fields_updated'].append('business_focus')

    # Derive audience_type from who_you_serve (removed from schema to stay <=16 fields)
    if not merged.get('audience_type') or len(str(merged.get('audience_type', ''))) < 3:
        who = merged.get('who_you_serve', '')
        if who:
            # Extract audience type keywords
            b2b_signals = ['business', 'b2b', 'corporate', 'executive', 'ceo', 'founder', 'enterprise']
            b2c_signals = ['consumer', 'b2c', 'individual', 'personal', 'parent', 'family']
            if any(s in who.lower() for s in b2b_signals):
                merged['audience_type'] = 'B2B'
            elif any(s in who.lower() for s in b2c_signals):
                merged['audience_type'] = 'B2C'
            else:
                # Use who_you_serve directly as the audience type description
                merged['audience_type'] = who[:100]
            extraction_metadata['fields_updated'].append('audience_type')

    # Build content_platforms from Exa social discoveries
    content_platforms = {}
    social_profiles = exa_result.get('_exa_social_profiles', {})
    page_social = exa_result.get('_exa_page_social', {})

    # Merge page social links and discovered social profiles
    all_social = {**page_social, **social_profiles}
    for platform, url in all_social.items():
        if platform == 'booking_link':
            if not merged.get('booking_link'):
                merged['booking_link'] = url
                extraction_metadata['fields_updated'].append('booking_link')
        else:
            content_platforms[platform] = url

    if content_platforms:
        merged['content_platforms'] = content_platforms
        extraction_metadata['fields_updated'].append('content_platforms')

    # Build jv_history from Exa JV mentions
    jv_mentions = exa_result.get('_exa_jv_mentions', [])
    if jv_mentions:
        jv_history = []
        for mention in jv_mentions:
            # Infer format from keywords in the quote
            quote = mention.get('quote', '').lower()
            fmt = 'endorsement'
            if 'podcast' in quote or 'episode' in quote:
                fmt = 'podcast_guest'
            elif 'summit' in quote or 'keynote' in quote or 'speaker' in quote:
                fmt = 'summit_speaker'
            elif 'affiliate' in quote:
                fmt = 'affiliate'
            elif 'bundle' in quote:
                fmt = 'bundle'
            elif 'co-author' in quote or 'co-wrote' in quote:
                fmt = 'co_author'
            elif 'webinar' in quote:
                fmt = 'webinar_guest'

            jv_history.append({
                'partner_name': mention.get('title', '')[:100],
                'format': fmt,
                'source_quote': mention.get('quote', '')[:200],
                'source_url': mention.get('url', ''),
            })

        merged['jv_history'] = jv_history
        extraction_metadata['fields_updated'].append('jv_history')

    # Compute audience engagement score
    from matching.enrichment.ai_research import calculate_engagement_score
    engagement = calculate_engagement_score(content_platforms)
    if engagement > 0:
        merged['audience_engagement_score'] = engagement
        extraction_metadata['fields_updated'].append('audience_engagement_score')

    # Scrape social_reach (follower counts) from discovered social URLs
    # Best-effort: social platforms often block bots, so this may return 0
    if content_platforms and not existing.get('social_reach') and not skip_social_reach:
        try:
            from matching.enrichment.ai_research import extract_social_links, scrape_social_reach
            # Convert full URLs back to handles for the scraper
            social_urls = [url for p, url in content_platforms.items()
                           if p in ('youtube', 'instagram', 'facebook', 'twitter', 'tiktok')]
            if social_urls:
                handles = extract_social_links(social_urls)
                if handles:
                    reach = scrape_social_reach(handles)
                    if reach > 0:
                        merged['social_reach'] = reach
                        extraction_metadata['fields_updated'].append('social_reach')
        except Exception as e:
            logger.debug(f"Social reach scraping skipped: {e}")

    # Store discovered URLs
    if exa_result.get('_exa_discovered_website'):
        extraction_metadata['discovered_website'] = exa_result['_exa_discovered_website']
        if not merged.get('website'):
            merged['website'] = exa_result['_exa_discovered_website']
            extraction_metadata['fields_updated'].append('website')

    if exa_result.get('_exa_discovered_linkedin'):
        extraction_metadata['discovered_linkedin'] = exa_result['_exa_discovered_linkedin']
        if not merged.get('linkedin'):
            merged['linkedin'] = exa_result['_exa_discovered_linkedin']
            extraction_metadata['fields_updated'].append('linkedin')

    # Store partnership page signal for intent scoring
    extraction_metadata['has_partnership_page'] = bool(
        exa_result.get('seeking') or
        any('partner' in m.get('quote', '').lower() for m in jv_mentions)
    )

    merged['_extraction_metadata'] = extraction_metadata

    was_enriched = bool(extraction_metadata['fields_updated'])
    return merged, was_enriched
