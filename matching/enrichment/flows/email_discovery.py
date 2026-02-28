"""
Prefect tasks for email discovery.

Extracted from the monolithic automated_enrichment_pipeline_safe.py.
Provides three Prefect tasks:
  - discover_email()        : single-profile email discovery (website + LinkedIn)
  - discover_emails_batch() : batch wrapper over discover_email()
  - apollo_bulk_enrich()    : Apollo bulk-match API for profiles still missing emails

Original methods:
  - enrich_profile_async()
  - try_website_scraping_async()
  - try_linkedin_scraping_async()
  - enrich_with_apollo_bulk()
"""

import os
import re
from typing import Optional

import requests
from prefect import task, get_run_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
)

# Prefixes / domains to discard when scraping LinkedIn
LINKEDIN_EMAIL_BLACKLIST = ['noreply', 'spam', 'linkedin', 'example']


# ---------------------------------------------------------------------------
# Internal helpers (not tasks — plain functions)
# ---------------------------------------------------------------------------

def _try_website_scraping(
    website: str,
    name: str,
    logger,
) -> Optional[dict]:
    """
    Scrape *website* for contact info using ContactScraper (Playwright).

    Returns a dict with keys ``email``, ``secondary_emails``, ``phone``,
    ``booking_link`` — or ``None`` on failure.
    """
    try:
        from matching.enrichment.contact_scraper import ContactScraper

        scraper = ContactScraper(browse_timeout=45)
        result = scraper.scrape_contact_info(website, name)

        if result.get('email') or result.get('secondary_emails') or result.get('phone'):
            return result

    except Exception as exc:
        logger.warning("Website scraping failed for %s: %s", website, exc)

    return None


def _try_linkedin_scraping(
    linkedin_url: str,
    logger,
) -> Optional[str]:
    """
    Fetch the public LinkedIn page via ``requests`` and extract email
    addresses with a regex.  Filters out noreply/spam/linkedin/example
    addresses.

    Returns the first valid email found, or ``None``.
    """
    try:
        response = requests.get(
            linkedin_url,
            timeout=10,
            headers={'User-Agent': 'Mozilla/5.0'},
        )
        if response.status_code != 200:
            return None

        text = response.text
        emails = EMAIL_RE.findall(text)

        valid_emails = [
            e for e in emails
            if not any(bl in e.lower() for bl in LINKEDIN_EMAIL_BLACKLIST)
        ]
        if valid_emails:
            return valid_emails[0]

    except Exception as exc:
        logger.warning("LinkedIn scraping failed for %s: %s", linkedin_url, exc)

    return None


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="discover-email", retries=1, retry_delay_seconds=10)
def discover_email(profile: dict) -> dict:
    """Discover email for a single profile using website scraping, LinkedIn, or Apollo.

    Returns the profile dict augmented with:
    - 'email': found email or None
    - 'email_method': 'website_scrape', 'linkedin_scrape', or None
    - '_scraped_secondary_emails': list of secondary emails
    - '_scraped_phone': phone if found
    - '_scraped_booking_link': booking link if found
    """
    logger = get_run_logger()
    name = profile.get('name', '')

    # Initialise output fields so callers always see consistent keys
    profile.setdefault('email', None)
    profile.setdefault('email_method', None)
    profile.setdefault('_scraped_secondary_emails', [])
    profile.setdefault('_scraped_phone', None)
    profile.setdefault('_scraped_booking_link', None)

    # ------------------------------------------------------------------
    # METHOD 1 — Website scraping (Playwright via ContactScraper)
    # ------------------------------------------------------------------
    website = profile.get('website')
    if website:
        logger.info("Trying website scraping for %s (%s)", name, website)
        scrape_result = _try_website_scraping(website, name, logger)

        if scrape_result:
            # Always propagate secondary data regardless of primary email
            if scrape_result.get('secondary_emails'):
                profile['_scraped_secondary_emails'] = scrape_result['secondary_emails']
            if scrape_result.get('phone') and not profile.get('phone'):
                profile['_scraped_phone'] = scrape_result['phone']
            if scrape_result.get('booking_link') and not profile.get('booking_link'):
                profile['_scraped_booking_link'] = scrape_result['booking_link']

            email = scrape_result.get('email')
            if email:
                profile['email'] = email
                profile['email_method'] = 'website_scrape'
                logger.info(
                    "Email found via website scrape for %s: %s",
                    name, email,
                )
                return profile

    # ------------------------------------------------------------------
    # METHOD 2 — LinkedIn scraping (regex extraction from public page)
    # ------------------------------------------------------------------
    linkedin = profile.get('linkedin')
    if linkedin:
        logger.info("Trying LinkedIn scraping for %s (%s)", name, linkedin)
        email = _try_linkedin_scraping(linkedin, logger)
        if email:
            profile['email'] = email
            profile['email_method'] = 'linkedin_scrape'
            logger.info(
                "Email found via LinkedIn scrape for %s: %s",
                name, email,
            )
            return profile

    # ------------------------------------------------------------------
    # No email found via scraping
    # ------------------------------------------------------------------
    logger.info("No email discovered for %s via scraping methods", name)
    return profile


@task(name="discover-emails-batch")
def discover_emails_batch(profiles: list[dict]) -> list[dict]:
    """Run email discovery for a batch of profiles.

    Iterates sequentially; Prefect handles parallelism at the flow level via
    task mapping / concurrency.  A failure on one profile never crashes the
    entire batch.
    """
    logger = get_run_logger()
    results: list[dict] = []

    for i, profile in enumerate(profiles):
        name = profile.get('name', f'profile-{i}')
        try:
            enriched = discover_email.fn(profile)
            results.append(enriched)
        except Exception as exc:
            logger.error(
                "Email discovery failed for %s (index %d): %s",
                name, i, exc,
            )
            # Return the profile unchanged so downstream tasks still see it
            profile.setdefault('email', None)
            profile.setdefault('email_method', None)
            profile.setdefault('_scraped_secondary_emails', [])
            profile.setdefault('_scraped_phone', None)
            profile.setdefault('_scraped_booking_link', None)
            results.append(profile)

    found = sum(1 for p in results if p.get('email'))
    logger.info(
        "Batch email discovery complete: %d/%d profiles found emails",
        found, len(profiles),
    )
    return results


@task(name="apollo-bulk-enrich", retries=2, retry_delay_seconds=30)
def apollo_bulk_enrich(profiles: list[dict], max_credits: int = 0) -> list[dict]:
    """Use Apollo bulk API for email discovery on profiles missing emails.

    Two paths are supported:

    1. **Cascade path** — uses ``ApolloEnrichmentService.enrich_batch()`` which
       captures full Apollo response data (all tiers).
    2. **Legacy path** — falls back to a direct ``requests`` call against the
       ``/people/bulk_match`` endpoint when the service layer is unavailable.

    Parameters
    ----------
    profiles:
        List of profile dicts.  Only those *without* an ``email`` key (or
        with a falsy value) will consume Apollo credits.
    max_credits:
        Upper-bound on Apollo credits to consume in this call.  ``0`` means
        unlimited (i.e. process all profiles in the batch).

    Returns
    -------
    list[dict]
        The input profiles augmented with ``email``, ``email_method``, and
        optionally ``_apollo_data`` when the cascade path is used.
    """
    logger = get_run_logger()

    if not profiles:
        return []

    # Separate profiles that still need an email from those that already have one.
    needs_email = [p for p in profiles if not p.get('email')]
    already_have = [p for p in profiles if p.get('email')]

    if not needs_email:
        logger.info("All %d profiles already have emails — skipping Apollo", len(profiles))
        return profiles

    # Honour credit cap
    if max_credits > 0:
        needs_email = needs_email[:max_credits]

    logger.info(
        "Apollo bulk enrich: %d profiles need emails (cap=%s)",
        len(needs_email),
        max_credits or 'unlimited',
    )

    enriched = _apollo_cascade_path(needs_email, logger)
    if enriched is None:
        enriched = _apollo_legacy_path(needs_email, logger)

    # Merge enriched profiles back into the full list while preserving order.
    enriched_by_id = {p.get('id'): p for p in enriched if p.get('id')}
    merged: list[dict] = []
    for p in profiles:
        pid = p.get('id')
        if pid and pid in enriched_by_id:
            merged.append(enriched_by_id[pid])
        else:
            merged.append(p)

    found = sum(1 for p in merged if p.get('email'))
    logger.info(
        "Apollo bulk enrich complete: %d/%d profiles now have emails",
        found, len(merged),
    )
    return merged


# ---------------------------------------------------------------------------
# Apollo internal helpers
# ---------------------------------------------------------------------------

def _apollo_cascade_path(
    profiles: list[dict],
    logger,
) -> Optional[list[dict]]:
    """Try the ``ApolloEnrichmentService`` (full-data cascade path).

    Returns the augmented profiles list, or ``None`` if the service is not
    available (missing API key, import error, etc.).
    """
    try:
        from matching.enrichment.apollo_enrichment import ApolloEnrichmentService

        api_key = os.environ.get('APOLLO_API_KEY', '')
        if not api_key:
            logger.warning("APOLLO_API_KEY not set — cascade path unavailable")
            return None

        service = ApolloEnrichmentService(api_key=api_key)

        # ApolloEnrichmentService.enrich_batch processes up to 10 at a time.
        MAX_BATCH = service.MAX_BATCH_SIZE
        results: list[dict] = []

        for start in range(0, len(profiles), MAX_BATCH):
            batch = profiles[start : start + MAX_BATCH]
            try:
                batch_results = service.enrich_batch(batch)
            except Exception as exc:
                logger.error("Apollo cascade batch failed (offset %d): %s", start, exc)
                # Return the batch profiles unchanged
                for p in batch:
                    p.setdefault('email', None)
                    p.setdefault('email_method', None)
                    results.append(p)
                continue

            for profile, apollo_result in zip(batch, batch_results):
                if apollo_result.get('error'):
                    logger.warning(
                        "Apollo returned error for %s: %s",
                        profile.get('name', '?'),
                        apollo_result['error'],
                    )
                    profile.setdefault('email', None)
                    profile.setdefault('email_method', None)
                    results.append(profile)
                    continue

                email = apollo_result.get('email')
                if email:
                    profile['email'] = email
                    profile['email_method'] = 'apollo_api'
                    logger.info(
                        "Apollo found email for %s: %s",
                        profile.get('name', '?'),
                        email,
                    )
                else:
                    profile.setdefault('email', None)
                    profile.setdefault('email_method', None)

                # Attach full Apollo data for downstream consolidation
                if apollo_result.get('_apollo_data'):
                    profile['_apollo_data'] = apollo_result['_apollo_data']

                results.append(profile)

        return results

    except ImportError:
        logger.warning("ApolloEnrichmentService import failed — falling back to legacy path")
        return None
    except Exception as exc:
        logger.error("Apollo cascade path failed: %s", exc)
        return None


def _apollo_legacy_path(
    profiles: list[dict],
    logger,
) -> list[dict]:
    """Direct ``requests`` call to the Apollo ``/people/bulk_match`` endpoint.

    This mirrors the original ``enrich_with_apollo_bulk()`` from the monolith.
    """
    api_key = os.environ.get('APOLLO_API_KEY', '')
    if not api_key:
        logger.warning("APOLLO_API_KEY not set — Apollo enrichment skipped")
        for p in profiles:
            p.setdefault('email', None)
            p.setdefault('email_method', None)
        return profiles

    MAX_BATCH = 10
    all_results: list[dict] = []

    for start in range(0, len(profiles), MAX_BATCH):
        batch = profiles[start : start + MAX_BATCH]
        details = []

        for profile in batch:
            name_parts = profile.get('name', '').strip().split(' ', 1)
            first_name = name_parts[0] if name_parts else profile.get('name', '')
            last_name = name_parts[1] if len(name_parts) > 1 else ''

            detail: dict = {
                'first_name': first_name,
                'last_name': last_name,
                'organization_name': profile.get('company', ''),
            }

            website = profile.get('website')
            if website:
                domain = (
                    website
                    .replace('https://', '')
                    .replace('http://', '')
                    .split('/')[0]
                )
                if domain.startswith('www.'):
                    domain = domain[4:]
                detail['domain'] = domain

            details.append(detail)

        try:
            response = requests.post(
                'https://api.apollo.io/api/v1/people/bulk_match',
                json={
                    'details': details,
                    'reveal_personal_emails': True,
                },
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': api_key,
                },
                timeout=30,
            )

            if response.status_code != 200:
                logger.warning(
                    "Apollo legacy bulk_match returned status %d",
                    response.status_code,
                )
                for p in batch:
                    p.setdefault('email', None)
                    p.setdefault('email_method', None)
                    all_results.append(p)
                continue

            data = response.json()
            matches = data.get('matches', [])

            for profile, match in zip(batch, matches):
                if match and match.get('email'):
                    profile['email'] = match['email']
                    profile['email_method'] = 'apollo_api'
                    logger.info(
                        "Apollo (legacy) found email for %s: %s",
                        profile.get('name', '?'),
                        match['email'],
                    )
                else:
                    profile.setdefault('email', None)
                    profile.setdefault('email_method', None)
                all_results.append(profile)

        except Exception as exc:
            logger.error("Apollo legacy bulk_match failed (offset %d): %s", start, exc)
            for p in batch:
                p.setdefault('email', None)
                p.setdefault('email_method', None)
                all_results.append(p)

    return all_results
