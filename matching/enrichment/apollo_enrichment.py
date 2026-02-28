"""
Apollo.io Enrichment Service

Captures ALL data Apollo returns, mapped by tier:
- Tier 1: email, website, linkedin (strict validation)
- Tier 2: phone, company, business_size, revenue_tier (basic validation)
- Tier 3: service_provided, niche, avatar_url + ALL raw data → enrichment_metadata.apollo_data

Source priority: 'apollo' = 30. Never overwrites Exa (50), AI research (40), or client data (90-100).

Used by both:
- scripts/import_apollo_csv.py (CSV import path)
- scripts/run_apollo_sweep.py (API path)
- scripts/automated_enrichment_pipeline_safe.py (cascade pipeline)
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Apollo source priority (matches pipeline's SOURCE_PRIORITY)
APOLLO_SOURCE = 'apollo'
APOLLO_PRIORITY = 30

# Business size mapping: Apollo employee_count → our business_size categories
EMPLOYEE_TO_BUSINESS_SIZE = [
    (10, 'solopreneur'),
    (50, 'small'),
    (200, 'medium'),
    (1000, 'established'),
    (float('inf'), 'enterprise'),
]

# Revenue mapping: Apollo annual_revenue → our revenue_tier categories
REVENUE_TO_TIER = [
    (1_000_000, 'micro'),
    (5_000_000, 'emerging'),
    (25_000_000, 'established'),
    (100_000_000, 'premium'),
    (float('inf'), 'enterprise'),
]


def extract_domain(url: str) -> Optional[str]:
    """Extract clean domain from URL for Apollo matching."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        domain = parsed.hostname or ''
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def split_name(full_name: str) -> Tuple[str, str]:
    """Split full name into (first_name, last_name)."""
    parts = full_name.strip().split(' ', 1)
    first = parts[0] if parts else full_name
    last = parts[1] if len(parts) > 1 else ''
    return first, last


def map_employee_count(count) -> Optional[str]:
    """Map Apollo employee count to business_size category."""
    if count is None:
        return None
    try:
        count = int(count)
    except (ValueError, TypeError):
        return None
    for threshold, size in EMPLOYEE_TO_BUSINESS_SIZE:
        if count <= threshold:
            return size
    return None


def map_annual_revenue(revenue) -> Optional[str]:
    """Map Apollo annual revenue to revenue_tier category."""
    if revenue is None:
        return None
    try:
        revenue = float(revenue)
    except (ValueError, TypeError):
        return None
    for threshold, tier in REVENUE_TO_TIER:
        if revenue <= threshold:
            return tier
    return None


def validate_email(email: str) -> bool:
    """Basic email validation."""
    if not email or not isinstance(email, str):
        return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email.strip()))


def validate_url(url: str) -> bool:
    """Basic URL validation."""
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        return bool(parsed.hostname and '.' in parsed.hostname)
    except Exception:
        return False


class ApolloEnrichmentService:
    """
    Apollo.io enrichment service that captures ALL returned data.

    Maps Apollo response fields to SupabaseProfile columns by tier,
    with overflow stored in enrichment_metadata.apollo_data JSONB.
    """

    BASE_URL = "https://api.apollo.io/api/v1"
    MAX_BATCH_SIZE = 10
    SOURCE = APOLLO_SOURCE

    def __init__(self, api_key: Optional[str] = None, webhook_url: Optional[str] = None):
        self.api_key = api_key or os.environ.get('APOLLO_API_KEY', '')
        self.webhook_url = webhook_url
        self.session = requests.Session()
        self.session.headers.update({
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'accept': 'application/json',
            'x-api-key': self.api_key,
        })
        # Rate limit tracking
        self.daily_calls = 0
        self.daily_limit = 2000  # Paid plan default
        self.last_rate_limit_remaining = None

    def needs_enrichment(self, profile: Dict) -> bool:
        """Only call Apollo if profile has Tier 1-2 gaps Apollo can fill."""
        return (
            not profile.get('email')
            or not profile.get('phone')
            or not profile.get('linkedin')
        )

    def build_request(self, profile: Dict) -> Dict:
        """Build Apollo API request from profile data. More input data = better match rate."""
        first_name, last_name = split_name(profile.get('name', ''))

        request = {
            'first_name': first_name,
            'last_name': last_name,
            'reveal_personal_emails': True,
        }

        # Add optional fields for better matching
        if profile.get('company'):
            request['organization_name'] = profile['company']

        if profile.get('website'):
            domain = extract_domain(profile['website'])
            if domain:
                request['domain'] = domain

        if profile.get('linkedin'):
            request['linkedin_url'] = profile['linkedin']

        if profile.get('email'):
            request['email'] = profile['email']

        # Phone reveal requires webhook
        if self.webhook_url:
            request['reveal_phone_number'] = True

        return request

    def enrich_single(self, profile: Dict) -> Dict:
        """
        Enrich a single profile via Apollo people/match endpoint.

        Returns a dict with all extracted fields mapped by tier.
        """
        if self.daily_calls >= self.daily_limit:
            logger.warning("Apollo daily limit reached (%d calls)", self.daily_calls)
            return {'error': 'daily_limit_reached', '_profile_id': profile.get('id')}

        request_data = self.build_request(profile)

        try:
            response = self.session.post(
                f"{self.BASE_URL}/people/match",
                json=request_data,
                timeout=30,
            )

            # Track rate limits from headers
            self._track_rate_limits(response)

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning("Apollo rate limited. Retry after %ds", retry_after)
                return {'error': 'rate_limited', 'retry_after': retry_after}

            response.raise_for_status()
            data = response.json()
            self.daily_calls += 1

            person = data.get('person')
            if not person:
                return {'error': 'no_match', '_profile_id': profile.get('id')}

            return self.extract_all_fields(person, profile)

        except requests.exceptions.RequestException as e:
            logger.warning("Apollo API error for %s: %s", profile.get('name', ''), e)
            return {'error': str(e), '_profile_id': profile.get('id')}

    def enrich_batch(self, profiles: List[Dict]) -> List[Dict]:
        """
        Enrich up to 10 profiles via Apollo people/bulk_match endpoint.

        Returns list of dicts with all extracted fields mapped by tier.
        """
        if not profiles:
            return []

        batch = profiles[:self.MAX_BATCH_SIZE]

        if self.daily_calls >= self.daily_limit:
            logger.warning("Apollo daily limit reached (%d calls)", self.daily_calls)
            return [{'error': 'daily_limit_reached', '_profile_id': p.get('id')} for p in batch]

        details = [self.build_request(p) for p in batch]

        query_params = {
            'reveal_personal_emails': 'true',
        }
        if self.webhook_url:
            query_params['reveal_phone_number'] = 'true'
            query_params['webhook_url'] = self.webhook_url

        try:
            response = self.session.post(
                f"{self.BASE_URL}/people/bulk_match",
                json={'details': details},
                params=query_params,
                timeout=60,
            )

            self._track_rate_limits(response)

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning("Apollo rate limited. Retry after %ds", retry_after)
                return [{'error': 'rate_limited', 'retry_after': retry_after} for _ in batch]

            response.raise_for_status()
            data = response.json()
            self.daily_calls += len(batch)

            matches = data.get('matches', [])
            credits_consumed = data.get('credits_consumed', len(matches))

            results = []
            for i, profile in enumerate(batch):
                person = matches[i] if i < len(matches) else None
                if person:
                    result = self.extract_all_fields(person, profile)
                    result['_credits_consumed'] = 1
                    results.append(result)
                else:
                    results.append({'error': 'no_match', '_profile_id': profile.get('id')})

            return results

        except requests.exceptions.RequestException as e:
            logger.warning("Apollo bulk match failed: %s", e)
            return [{'error': str(e), '_profile_id': p.get('id')} for p in batch]

    def search_people(
        self,
        title: str = "",
        industry: str = "",
        company_size: str = "",
        location: str = "",
        max_results: int = 25,
    ) -> list[dict]:
        """Search Apollo for people matching criteria.

        Uses the Apollo /people/search endpoint for discovering
        new prospects by title, industry, and company attributes.

        Args:
            title: Job title keywords (e.g. "health coach", "CEO")
            industry: Industry filter
            company_size: Company size range (e.g. "1-10", "11-50")
            location: Location filter
            max_results: Max results to return (Apollo caps at 100/page)

        Returns:
            List of prospect dicts with name, email, linkedin, company, title.
        """
        if not self.api_key:
            logger.warning("Apollo API key not configured for people search")
            return []

        import httpx

        payload = {
            "api_key": self.api_key,
            "per_page": min(max_results, 100),
            "page": 1,
        }

        # Build person_titles filter
        if title:
            payload["person_titles"] = [t.strip() for t in title.split(",")]
        if industry:
            payload["person_industries"] = [industry]
        if company_size:
            payload["organization_num_employees_ranges"] = [company_size]
        if location:
            payload["person_locations"] = [location]

        try:
            resp = httpx.post(
                "https://api.apollo.io/api/v1/mixed_people/search",
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Apollo people search failed: %s", exc)
            return []

        prospects = []
        for person in data.get("people", [])[:max_results]:
            org = person.get("organization", {}) or {}
            prospects.append({
                "name": person.get("name", ""),
                "email": person.get("email", ""),
                "linkedin": person.get("linkedin_url", ""),
                "company": org.get("name", ""),
                "title": person.get("title", ""),
                "website": org.get("website_url", ""),
                "industry": org.get("industry", ""),
                "company_size": org.get("estimated_num_employees"),
                "source_tool": "apollo",
            })

        logger.info("Apollo people search: %d results for title='%s'", len(prospects), title[:40])
        return prospects

    def extract_all_fields(self, person: Dict, original_profile: Dict) -> Dict:
        """
        Extract ALL fields from Apollo person response, mapped by tier.

        Nothing is discarded — every field Apollo returns is captured.
        """
        org = person.get('organization') or {}

        result = {
            '_source': self.SOURCE,
            '_profile_id': original_profile.get('id'),
            '_extracted_at': datetime.now().isoformat(),
        }

        # === TIER 1: Core (strict validation) ===
        email = person.get('email', '')
        if email and validate_email(email):
            result['email'] = email.strip()
            result['email_status'] = person.get('email_status', 'unknown')

        linkedin = person.get('linkedin_url', '')
        if linkedin and validate_url(linkedin):
            result['linkedin'] = linkedin.strip()

        org_website = org.get('website_url', '')
        if org_website and validate_url(org_website):
            result['website'] = org_website.strip()

        # === TIER 2: Operational (basic validation) ===
        phone_numbers = person.get('phone_numbers') or []
        if phone_numbers:
            raw_phone = phone_numbers[0].get('raw_number', '')
            if raw_phone and len(raw_phone) >= 7:
                result['phone'] = raw_phone.strip()

        org_name = org.get('name', '')
        if org_name and len(org_name) >= 2:
            result['company'] = org_name.strip()

        # Business size from employee count
        employee_count = org.get('estimated_num_employees')
        business_size = map_employee_count(employee_count)
        if business_size:
            result['business_size'] = business_size

        # Revenue tier from annual revenue
        annual_revenue = org.get('annual_revenue')
        revenue_tier = map_annual_revenue(annual_revenue)
        if revenue_tier:
            result['revenue_tier'] = revenue_tier

        # === TIER 3: Enrichment (columns where available, else enrichment_metadata) ===
        title = person.get('title', '')
        if title and len(title) >= 3:
            result['service_provided'] = title.strip()

        industry = org.get('industry', '')
        if industry and len(industry) >= 3:
            result['niche'] = industry.strip()

        photo_url = person.get('photo_url', '')
        if photo_url and validate_url(photo_url):
            result['avatar_url'] = photo_url.strip()

        # Social URLs → content_platforms
        social_urls = {}
        if person.get('twitter_url'):
            social_urls['twitter'] = person['twitter_url']
        if person.get('github_url'):
            social_urls['github'] = person['github_url']
        if person.get('facebook_url'):
            social_urls['facebook'] = person['facebook_url']
        if social_urls:
            result['_social_urls'] = social_urls

        # === ALL raw Apollo data → enrichment_metadata.apollo_data ===
        apollo_data = {
            'apollo_id': person.get('id'),
            'email_status': person.get('email_status'),
            'email_confidence': person.get('email_confidence'),
            'headline': person.get('headline'),
            'seniority': person.get('seniority'),
            'city': person.get('city'),
            'state': person.get('state'),
            'country': person.get('country'),
            'departments': person.get('departments'),
            'subdepartments': person.get('subdepartments'),
            'functions': person.get('functions'),
            'is_likely_to_engage': person.get('is_likely_to_engage'),
            'show_intent': person.get('show_intent'),
            'phone_numbers': person.get('phone_numbers'),
            # Organization data
            'org_linkedin_url': org.get('linkedin_url'),
            'org_twitter_url': org.get('twitter_url'),
            'org_facebook_url': org.get('facebook_url'),
            'employee_count': org.get('estimated_num_employees'),
            'annual_revenue': org.get('annual_revenue'),
            'annual_revenue_printed': org.get('annual_revenue_printed'),
            'total_funding': org.get('total_funding'),
            'total_funding_printed': org.get('total_funding_printed'),
            'founded_year': org.get('founded_year'),
            'org_industry': org.get('industry'),
            'org_keywords': org.get('keywords'),
            'org_short_description': org.get('short_description'),
            'org_city': org.get('city'),
            'org_state': org.get('state'),
            'org_country': org.get('country'),
            # Employment history
            'employment_history': person.get('employment_history'),
            # Enrichment metadata
            'enriched_at': datetime.now().isoformat(),
        }
        # Remove None values to keep JSONB clean
        apollo_data = {k: v for k, v in apollo_data.items() if v is not None}
        result['_apollo_data'] = apollo_data

        return result

    def _track_rate_limits(self, response: requests.Response):
        """Track rate limit headers from Apollo responses."""
        remaining = response.headers.get('X-RateLimit-Remaining')
        if remaining is not None:
            self.last_rate_limit_remaining = int(remaining)
            if self.last_rate_limit_remaining < 10:
                logger.warning(
                    "Apollo rate limit low: %d remaining",
                    self.last_rate_limit_remaining,
                )


def process_apollo_result(
    result: Dict,
    profile: Dict,
    existing_meta: Optional[Dict] = None,
) -> Dict:
    """
    Process an Apollo enrichment result into a write-ready update dict.

    Combines extracted fields with provenance tracking and enrichment_metadata.
    Used by both CSV import and API sweep paths.

    Returns a dict with:
    - Profile column updates (email, phone, linkedin, etc.)
    - enrichment_metadata updates (apollo_data, field_meta)
    - _fields_written list for logging
    """
    if result.get('error'):
        return {'_error': result['error'], '_profile_id': result.get('_profile_id')}

    updates = {}
    fields_written = []
    now_iso = datetime.now().isoformat()

    # Get existing enrichment_metadata
    if existing_meta is None:
        existing_meta = profile.get('enrichment_metadata') or {}
    if isinstance(existing_meta, str):
        try:
            existing_meta = json.loads(existing_meta)
        except (json.JSONDecodeError, TypeError):
            existing_meta = {}

    # Import source priority check from pipeline
    from scripts.automated_enrichment_pipeline_safe import SOURCE_PRIORITY

    def should_write(field: str, value) -> bool:
        """Check if Apollo should write this field based on source priority."""
        if not value:
            return False
        field_info = (existing_meta or {}).get('field_meta', {}).get(field, {})
        existing_source = field_info.get('source', 'unknown')
        existing_priority = SOURCE_PRIORITY.get(existing_source, 0)
        apollo_priority = SOURCE_PRIORITY.get(APOLLO_SOURCE, 30)

        # Never overwrite higher-priority sources
        if apollo_priority < existing_priority:
            return False
        # Higher priority always wins
        if apollo_priority > existing_priority:
            return True
        # Equal priority — only overwrite if field is empty
        current_value = profile.get(field)
        return not current_value or (isinstance(current_value, str) and not current_value.strip())

    # Tier 1 writes
    if result.get('email') and should_write('email', result['email']):
        updates['email'] = result['email']
        fields_written.append('email')

    if result.get('linkedin') and should_write('linkedin', result['linkedin']):
        updates['linkedin'] = result['linkedin']
        fields_written.append('linkedin')

    if result.get('website') and should_write('website', result['website']):
        updates['website'] = result['website']
        fields_written.append('website')

    # Tier 2 writes
    if result.get('phone') and should_write('phone', result['phone']):
        updates['phone'] = result['phone']
        fields_written.append('phone')

    if result.get('company') and should_write('company', result['company']):
        updates['company'] = result['company']
        fields_written.append('company')

    if result.get('business_size') and should_write('business_size', result['business_size']):
        updates['business_size'] = result['business_size']
        fields_written.append('business_size')

    if result.get('revenue_tier') and should_write('revenue_tier', result['revenue_tier']):
        updates['revenue_tier'] = result['revenue_tier']
        fields_written.append('revenue_tier')

    # Tier 3 writes
    if result.get('service_provided') and should_write('service_provided', result['service_provided']):
        updates['service_provided'] = result['service_provided']
        fields_written.append('service_provided')

    if result.get('niche') and should_write('niche', result['niche']):
        updates['niche'] = result['niche']
        fields_written.append('niche')

    if result.get('avatar_url') and should_write('avatar_url', result['avatar_url']):
        updates['avatar_url'] = result['avatar_url']
        fields_written.append('avatar_url')

    # Build enrichment_metadata update
    field_meta_update = {}
    for f in fields_written:
        field_meta_update[f] = {
            'source': APOLLO_SOURCE,
            'updated_at': now_iso,
            'pipeline_version': 1,
        }

    # Merge with existing enrichment_metadata
    meta = dict(existing_meta)
    meta['apollo_data'] = result.get('_apollo_data', {})
    meta['last_apollo_enrichment'] = now_iso

    existing_field_meta = meta.get('field_meta', {})
    existing_field_meta.update(field_meta_update)
    meta['field_meta'] = existing_field_meta

    updates['enrichment_metadata'] = meta
    updates['_fields_written'] = fields_written
    updates['_profile_id'] = result.get('_profile_id')

    return updates
