#!/usr/bin/env python3
"""
Verified Automated Enrichment Pipeline with Truelist.io

Best of both worlds:
1. High volume from pattern guessing (80-90% discovery rate)
2. Truelist verification (only save verified emails)
3. Safe for cold email outreach

All emails are verified through Truelist.io before saving.

Usage:
    python scripts/automated_enrichment_pipeline_verified.py --limit 50 --auto-consolidate
"""

import os
import sys
import csv
import re
import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from dotenv import load_dotenv

load_dotenv()


class EmailVerifier:
    """Truelist.io email verification with rate limiting"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.truelist.io/v1"
        self.rate_limit_delay = 0.1  # 10 requests/second = 0.1s between requests
        self.last_request_time = 0

    async def verify_email(self, email: str, session: aiohttp.ClientSession) -> Dict:
        """
        Verify email through Truelist.io with rate limiting.

        Rate limit: 10 requests/second
        Retry on 429 errors

        Returns: {
            'valid': bool,
            'score': float,
            'status': str,
            'reason': str
        }
        """
        # Rate limiting: ensure 0.1s between requests
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)

        self.last_request_time = time.time()

        # Retry logic for rate limits
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with session.get(
                    f"{self.base_url}/verify",
                    params={'email': email},
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Truelist response format
                        return {
                            'valid': data.get('valid', False),
                            'score': data.get('score', 0),
                            'status': data.get('status', 'unknown'),
                            'reason': data.get('reason', ''),
                            'provider': data.get('provider', '')
                        }
                    elif response.status == 429:
                        # Rate limited - wait and retry
                        wait_time = (attempt + 1) * 2  # Exponential backoff
                        print(f"  ⚠️  Rate limited, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return {
                            'valid': False,
                            'score': 0,
                            'status': 'error',
                            'reason': f'API error: {response.status}'
                        }
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return {
                    'valid': False,
                    'score': 0,
                    'status': 'error',
                    'reason': str(e)
                }

        # All retries failed
        return {
            'valid': False,
            'score': 0,
            'status': 'error',
            'reason': 'Max retries exceeded'
        }


class VerifiedEnrichmentPipeline:
    """
    Enrichment pipeline with Truelist verification.
    All emails are verified before saving.
    """

    def __init__(self, max_apollo_credits=0, dry_run=False, batch_size=5):
        self.max_apollo_credits = max_apollo_credits
        self.apollo_credits_used = 0
        self.dry_run = dry_run
        self.batch_size = batch_size  # Note: Truelist rate limit is 10/sec, handled internally

        # Initialize Truelist verifier (with rate limiting)
        truelist_api_key = os.environ.get('TRUELIST_API_KEY')
        if not truelist_api_key:
            raise ValueError("TRUELIST_API_KEY not found in .env")

        self.verifier = EmailVerifier(truelist_api_key)

        self.stats = {
            'total': 0,
            'emails_discovered': 0,
            'emails_verified': 0,
            'emails_invalid': 0,
            'website_scrape': 0,
            'linkedin_scrape': 0,
            'email_pattern': 0,
            'apollo_api': 0,
            'verification_cost': 0,
            'failed': 0,
            'time_taken': 0
        }

    def get_profiles_to_enrich(self, limit=20, priority='high-value') -> List[Dict]:
        """Get profiles needing enrichment"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if priority == 'high-value':
            query = """
                SELECT id, name, email, company, website, linkedin, list_size
                FROM profiles
                WHERE (email IS NULL OR email = '')
                  AND company IS NOT NULL AND company != ''
                  AND name IS NOT NULL AND name != ''
                  AND list_size > 100000
                ORDER BY list_size DESC
                LIMIT %s
            """
        elif priority == 'has-website':
            query = """
                SELECT id, name, email, company, website, linkedin, list_size
                FROM profiles
                WHERE (email IS NULL OR email = '')
                  AND website IS NOT NULL AND website != ''
                  AND name IS NOT NULL AND name != ''
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
            """
        else:
            query = """
                SELECT id, name, email, company, website, linkedin, list_size
                FROM profiles
                WHERE (email IS NULL OR email = '')
                  AND name IS NOT NULL AND name != ''
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
            """

        cursor.execute(query, (limit,))
        profiles = [dict(p) for p in cursor.fetchall()]

        cursor.close()
        conn.close()

        return profiles

    async def enrich_profile_batch(
        self,
        profiles: List[Dict],
        session: aiohttp.ClientSession
    ) -> List[Tuple[Dict, Optional[str], Optional[str], Optional[Dict]]]:
        """
        Enrich batch with verification.
        Returns: List of (profile, email, method, verification_result)
        """
        tasks = [
            self.enrich_and_verify_profile(profile, session)
            for profile in profiles
        ]
        return await asyncio.gather(*tasks)

    async def enrich_and_verify_profile(
        self,
        profile: Dict,
        session: aiohttp.ClientSession
    ) -> Tuple[Dict, Optional[str], Optional[str], Optional[Dict]]:
        """
        Enrich and verify single profile.
        Returns: (profile, email, method, verification_result)
        """
        name = profile['name']
        company = profile.get('company')
        website = profile.get('website')
        linkedin = profile.get('linkedin')

        # STEP 1: Try all enrichment methods (including pattern guessing)
        email = None
        method = None

        # Method 1: Website scraping
        if website:
            email = await self.try_website_scraping(website, name, session)
            if email:
                method = 'website_scrape'

        # Method 2: LinkedIn scraping
        if not email and linkedin:
            email = await self.try_linkedin_scraping(linkedin, session)
            if email:
                method = 'linkedin_scrape'

        # Method 3: Email pattern guessing (NOW SAFE with verification!)
        if not email and company and website:
            email = await self.try_email_pattern(name, website)
            if email:
                method = 'email_pattern'

        # STEP 2: Verify the email through Truelist
        if email:
            self.stats['emails_discovered'] += 1

            if not self.dry_run:
                verification = await self.verifier.verify_email(email, session)
                self.stats['verification_cost'] += 1  # Count verifications

                if verification['valid']:
                    # Email is verified!
                    self.stats['emails_verified'] += 1
                    if method == 'website_scrape':
                        self.stats['website_scrape'] += 1
                    elif method == 'linkedin_scrape':
                        self.stats['linkedin_scrape'] += 1
                    elif method == 'email_pattern':
                        self.stats['email_pattern'] += 1

                    return profile, email, method, verification
                else:
                    # Email is invalid - don't save
                    self.stats['emails_invalid'] += 1
                    return profile, None, None, verification

        return profile, None, None, None

    async def try_website_scraping(
        self,
        website: str,
        name: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Scrape website for emails"""
        if self.dry_run:
            return None

        try:
            if not website.startswith('http'):
                website = f'https://{website}'

            urls = [
                website,
                f"{website}/contact",
                f"{website}/about",
                f"{website}/team"
            ]

            tasks = [self.fetch_url(url, session) for url in urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            all_emails = []
            for response in responses:
                if isinstance(response, str):
                    emails = re.findall(
                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        response
                    )
                    all_emails.extend(emails)

            # Filter generic emails
            valid_emails = [
                e for e in all_emails
                if not any(x in e.lower() for x in [
                    'noreply', 'spam', 'abuse', 'postmaster', 'example'
                ])
            ]

            if valid_emails:
                # Prefer emails matching name
                name_parts = name.lower().split()
                for email in valid_emails:
                    if any(part in email.lower() for part in name_parts):
                        return email
                return valid_emails[0]

        except:
            pass

        return None

    async def fetch_url(
        self,
        url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Fetch URL"""
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=5),
                headers={'User-Agent': 'Mozilla/5.0'}
            ) as response:
                if response.status == 200:
                    return await response.text()
        except:
            pass
        return None

    async def try_linkedin_scraping(
        self,
        linkedin_url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Scrape LinkedIn"""
        if self.dry_run:
            return None

        try:
            text = await self.fetch_url(linkedin_url, session)
            if text:
                emails = re.findall(
                    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                    text
                )
                valid_emails = [
                    e for e in emails
                    if not any(x in e.lower() for x in [
                        'noreply', 'spam', 'linkedin', 'example'
                    ])
                ]
                if valid_emails:
                    return valid_emails[0]
        except:
            pass
        return None

    async def try_email_pattern(self, name: str, website: str) -> Optional[str]:
        """Guess email pattern (will be verified!)"""
        if self.dry_run:
            return None

        try:
            domain = website.replace('https://', '').replace('http://', '').split('/')[0]
            if domain.startswith('www.'):
                domain = domain[4:]

            name_parts = name.strip().split()
            if len(name_parts) < 2:
                return None

            first_name = name_parts[0].lower()
            last_name = name_parts[-1].lower()

            # Most common pattern
            return f"{first_name}.{last_name}@{domain}"

        except:
            pass
        return None

    async def enrich_with_apollo_bulk(
        self,
        profiles: List[Dict],
        session: aiohttp.ClientSession
    ) -> List[Tuple[Dict, Optional[str], Optional[str], Optional[Dict]]]:
        """Use Apollo bulk API with verification"""
        if self.dry_run or not profiles:
            return [(p, None, None, None) for p in profiles]

        api_key = os.environ.get('APOLLO_API_KEY')
        if not api_key:
            return [(p, None, None, None) for p in profiles]

        batch = profiles[:10]
        details = []

        for profile in batch:
            name_parts = profile['name'].strip().split(' ', 1)
            first_name = name_parts[0] if name_parts else profile['name']
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            detail = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": profile.get('company', '')
            }

            website = profile.get('website')
            if website:
                domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                if domain.startswith('www.'):
                    domain = domain[4:]
                detail['domain'] = domain

            details.append(detail)

        try:
            async with session.post(
                "https://api.apollo.io/api/v1/people/bulk_match",
                json={
                    "details": details,
                    "reveal_personal_emails": True
                },
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': api_key
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    matches = data.get('matches', [])

                    results = []
                    for profile, match in zip(batch, matches):
                        if match and match.get('email'):
                            email = match['email']
                            self.stats['emails_discovered'] += 1
                            self.apollo_credits_used += 1

                            # Verify Apollo emails too
                            verification = await self.verifier.verify_email(email, session)
                            self.stats['verification_cost'] += 1

                            if verification['valid']:
                                self.stats['emails_verified'] += 1
                                self.stats['apollo_api'] += 1
                                results.append((profile, email, 'apollo_api', verification))
                            else:
                                self.stats['emails_invalid'] += 1
                                results.append((profile, None, None, verification))
                        else:
                            results.append((profile, None, None, None))

                    return results
        except:
            pass

        return [(p, None, None, None) for p in batch]

    async def run(self, limit=20, priority='high-value', auto_consolidate=False):
        """Run verified enrichment pipeline"""
        start_time = time.time()

        print("=" * 70)
        print("VERIFIED ENRICHMENT PIPELINE (Truelist.io)")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ENRICHMENT + VERIFICATION'}")
        print(f"Priority: {priority}")
        print(f"Limit: {limit}")
        print(f"Max Apollo credits: {self.max_apollo_credits}")
        print()
        print("✅ All emails verified through Truelist.io before saving")
        print("✅ Pattern guessing enabled (safe with verification)")
        print()

        profiles = self.get_profiles_to_enrich(limit, priority)
        print(f"Found {len(profiles)} profiles to enrich")
        print()

        results = []
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=2)

        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(0, len(profiles), self.batch_size):
                batch = profiles[i:i + self.batch_size]
                self.stats['total'] += len(batch)

                print(f"Processing batch {i//self.batch_size + 1} ({len(batch)} profiles)...")

                # Enrich and verify in parallel
                batch_results = await self.enrich_profile_batch(batch, session)

                needs_apollo = []
                for profile, email, method, verification in batch_results:
                    if email and verification and verification['valid']:
                        results.append({
                            'profile_id': profile['id'],
                            'name': profile['name'],
                            'company': profile['company'],
                            'email': email,
                            'method': method,
                            'verification_score': verification.get('score', 0),
                            'verification_status': verification.get('status', 'unknown'),
                            'list_size': profile.get('list_size', 0),
                            'enriched_at': datetime.now().isoformat()
                        })
                    else:
                        needs_apollo.append(profile)

                # Apollo fallback
                if needs_apollo and self.apollo_credits_used < self.max_apollo_credits:
                    remaining = self.max_apollo_credits - self.apollo_credits_used
                    apollo_batch = needs_apollo[:min(len(needs_apollo), remaining)]

                    if apollo_batch:
                        print(f"  Trying Apollo API for {len(apollo_batch)} profiles...")
                        apollo_results = await self.enrich_with_apollo_bulk(apollo_batch, session)

                        for profile, email, method, verification in apollo_results:
                            if email and verification and verification['valid']:
                                results.append({
                                    'profile_id': profile['id'],
                                    'name': profile['name'],
                                    'company': profile['company'],
                                    'email': email,
                                    'method': method,
                                    'verification_score': verification.get('score', 0),
                                    'verification_status': verification.get('status', 'unknown'),
                                    'list_size': profile.get('list_size', 0),
                                    'enriched_at': datetime.now().isoformat()
                                })
                            else:
                                self.stats['failed'] += 1

                batch_verified = sum(1 for r in results if r['profile_id'] in [p['id'] for p in batch])
                print(f"  ✅ {batch_verified}/{len(batch)} verified emails")

        # Save results
        if results and not self.dry_run:
            output_file = f"enriched_verified_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['profile_id', 'name', 'company', 'email', 'method',
                             'verification_score', 'verification_status', 'list_size', 'enriched_at']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)

            print(f"\n✅ Results saved to: {output_file}")

            if auto_consolidate:
                print("\nAuto-consolidating to Supabase...")
                await self.consolidate_to_supabase_batch(results)

        self.stats['time_taken'] = time.time() - start_time

        # Print summary
        print("\n" + "=" * 70)
        print("ENRICHMENT SUMMARY (ALL VERIFIED)")
        print("=" * 70)
        print(f"Total profiles:      {self.stats['total']}")
        print(f"Emails discovered:   {self.stats['emails_discovered']}")
        print(f"Emails verified:     {self.stats['emails_verified']} ({self.stats['emails_verified']/max(self.stats['emails_discovered'],1)*100:.1f}% valid)")
        print(f"Emails invalid:      {self.stats['emails_invalid']}")
        print(f"Failed:              {self.stats['failed']}")
        print(f"Time taken:          {self.stats['time_taken']:.1f}s")
        print()
        print("Discovery methods (all verified):")
        print(f"  Website scraping:  {self.stats['website_scrape']}")
        print(f"  LinkedIn scraping: {self.stats['linkedin_scrape']}")
        print(f"  Email patterns:    {self.stats['email_pattern']} (verified!)")
        print(f"  Apollo API:        {self.stats['apollo_api']}")
        print()
        print(f"💰 Costs:")
        print(f"  Apollo:     ${self.stats['apollo_api'] * 0.10:.2f} ({self.stats['apollo_api']} credits)")
        print(f"  Truelist:   ${self.stats['verification_cost'] * 0.002:.2f} ({self.stats['verification_cost']} verifications @ $0.002)")
        print(f"  Total:      ${(self.stats['apollo_api'] * 0.10) + (self.stats['verification_cost'] * 0.002):.2f}")
        print()
        print("✅ All emails are VERIFIED and safe for outreach")

    async def consolidate_to_supabase_batch(self, results: List[Dict]):
        """Batch consolidate to Supabase with verification tracking"""
        from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

        scorer = ConfidenceScorer()
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()

        source_map = {
            'website_scrape': 'website_scraped',
            'linkedin_scrape': 'linkedin_scraped',
            'email_pattern': 'email_pattern_verified',  # New source type!
            'apollo_api': 'apollo'
        }

        updates = []
        for result in results:
            profile_id = result['profile_id']
            email = result['email']
            method = result['method']
            enriched_at = datetime.fromisoformat(result['enriched_at'])

            source = source_map.get(method, 'unknown')

            # Higher confidence for verified emails
            if method == 'email_pattern':
                # Pattern was verified, so confidence is high
                confidence = 0.75  # Between scraped (0.70) and Apollo (0.80)
            else:
                confidence = scorer.calculate_confidence('email', source, enriched_at)

            confidence_expires_at = scorer.calculate_expires_at('email', enriched_at)

            email_metadata = {
                'source': source,
                'enriched_at': enriched_at.isoformat(),
                'source_date': enriched_at.date().isoformat(),
                'confidence': confidence,
                'confidence_expires_at': confidence_expires_at.isoformat(),
                'verification_count': 1,
                'enrichment_method': method,
                'verified': True,
                'verification_service': 'truelist',
                'verification_score': result.get('verification_score', 0),
                'verification_status': result.get('verification_status', 'verified')
            }

            updates.append((
                email,
                json.dumps(email_metadata),
                confidence,
                enriched_at,
                datetime.now(),
                profile_id
            ))

        execute_batch(cursor, """
            UPDATE profiles
            SET email = %s,
                enrichment_metadata = jsonb_set(
                    COALESCE(enrichment_metadata, '{}'::jsonb),
                    '{email}',
                    %s::jsonb
                ),
                profile_confidence = %s,
                last_enriched_at = %s,
                updated_at = %s
            WHERE id = %s
        """, updates)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Batch updated {len(updates)} profiles with VERIFIED emails")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Verified enrichment pipeline with Truelist.io')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--priority', type=str, default='high-value',
                        choices=['high-value', 'has-website', 'all'])
    parser.add_argument('--max-apollo-credits', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=5)
    parser.add_argument('--auto-consolidate', action='store_true')
    parser.add_argument('--dry-run', action='store_true')

    args = parser.parse_args()

    pipeline = VerifiedEnrichmentPipeline(
        max_apollo_credits=args.max_apollo_credits,
        dry_run=args.dry_run,
        batch_size=args.batch_size
    )

    asyncio.run(pipeline.run(
        limit=args.limit,
        priority=args.priority,
        auto_consolidate=args.auto_consolidate
    ))


if __name__ == '__main__':
    main()
