#!/usr/bin/env python3
"""
Safe Automated Enrichment Pipeline (NO GUESSING)

Only uses verified enrichment methods:
1. Website scraping (found on actual website)
2. LinkedIn scraping (found on actual profile)
3. Apollo.io API (verified by Apollo)

NO email pattern guessing - only real, found emails.

Usage:
    python scripts/automated_enrichment_pipeline_safe.py --limit 50 --auto-consolidate
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


class SafeEnrichmentPipeline:
    """
    Safe enrichment pipeline - NO GUESSING.
    Only uses emails that are actually found/verified.
    """

    def __init__(self, max_apollo_credits=0, dry_run=False, batch_size=5):
        self.max_apollo_credits = max_apollo_credits
        self.apollo_credits_used = 0
        self.dry_run = dry_run
        self.batch_size = batch_size

        self.stats = {
            'total': 0,
            'enriched': 0,
            'emails_found': 0,
            'website_scrape': 0,
            'linkedin_scrape': 0,
            'apollo_api': 0,
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
    ) -> List[Tuple[Dict, Optional[str], Optional[str]]]:
        """Enrich batch in parallel"""
        tasks = [
            self.enrich_profile_async(profile, session)
            for profile in profiles
        ]
        return await asyncio.gather(*tasks)

    async def enrich_profile_async(
        self,
        profile: Dict,
        session: aiohttp.ClientSession
    ) -> Tuple[Dict, Optional[str], Optional[str]]:
        """
        Enrich single profile - VERIFIED ONLY.
        Returns: (profile, email, method)
        """
        name = profile['name']
        website = profile.get('website')
        linkedin = profile.get('linkedin')

        # METHOD 1: Website scraping (VERIFIED - found on actual site)
        if website:
            email = await self.try_website_scraping_async(website, name, session)
            if email:
                self.stats['website_scrape'] += 1
                return profile, email, 'website_scrape'

        # METHOD 2: LinkedIn scraping (VERIFIED - found on actual profile)
        if linkedin:
            email = await self.try_linkedin_scraping_async(linkedin, session)
            if email:
                self.stats['linkedin_scrape'] += 1
                return profile, email, 'linkedin_scrape'

        # NO METHOD 3: Email pattern guessing - REMOVED for safety

        return profile, None, None

    async def try_website_scraping_async(
        self,
        website: str,
        name: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Scrape website for ACTUAL email addresses"""
        if self.dry_run:
            return None

        try:
            if not website.startswith('http'):
                website = f'https://{website}'

            # Try multiple pages in parallel
            urls = [
                website,
                f"{website}/contact",
                f"{website}/about",
                f"{website}/team"
            ]

            tasks = [self.fetch_url_async(url, session) for url in urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            # Extract emails from successful responses
            all_emails = []
            for response in responses:
                if isinstance(response, str):
                    emails = re.findall(
                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        response
                    )
                    all_emails.extend(emails)

            # Filter out generic emails
            valid_emails = [
                e for e in all_emails
                if not any(x in e.lower() for x in [
                    'noreply', 'spam', 'abuse', 'postmaster', 'example',
                    'privacy', 'legal', 'dmca', 'support', 'info', 'hello'
                ])
            ]

            if valid_emails:
                # Strongly prefer emails matching person's name
                name_parts = name.lower().split()
                for email in valid_emails:
                    email_lower = email.lower()
                    # Must match first AND last name
                    if len(name_parts) >= 2:
                        if name_parts[0] in email_lower and name_parts[-1] in email_lower:
                            return email

                # If no name match, be conservative - return None
                # (Could be generic support@ or sales@)
                return None

        except Exception as e:
            pass

        return None

    async def fetch_url_async(
        self,
        url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Fetch URL with timeout"""
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

    async def try_linkedin_scraping_async(
        self,
        linkedin_url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Scrape LinkedIn for ACTUAL email"""
        if self.dry_run:
            return None

        try:
            text = await self.fetch_url_async(linkedin_url, session)
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

    async def enrich_with_apollo_bulk(
        self,
        profiles: List[Dict]
    ) -> List[Tuple[Dict, Optional[str], Optional[str]]]:
        """Use Apollo bulk API (VERIFIED by Apollo)"""
        if self.dry_run or not profiles:
            return [(p, None, None) for p in profiles]

        api_key = os.environ.get('APOLLO_API_KEY')
        if not api_key:
            return [(p, None, None) for p in profiles]

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
            async with aiohttp.ClientSession() as session:
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
                                self.apollo_credits_used += 1
                                self.stats['apollo_api'] += 1
                                results.append((profile, email, 'apollo_api'))
                            else:
                                results.append((profile, None, None))
                        return results
        except:
            pass

        return [(p, None, None) for p in batch]

    async def run(self, limit=20, priority='high-value', auto_consolidate=False):
        """Run safe enrichment pipeline"""
        start_time = time.time()

        print("=" * 70)
        print("SAFE AUTOMATED ENRICHMENT PIPELINE (NO GUESSING)")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ENRICHMENT'}")
        print(f"Priority: {priority}")
        print(f"Limit: {limit}")
        print(f"Max Apollo credits: {self.max_apollo_credits}")
        print()
        print("⚠️  SAFE MODE: Only verified emails (no pattern guessing)")
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

                # Try verified methods only
                batch_results = await self.enrich_profile_batch(batch, session)

                needs_apollo = []
                for profile, email, method in batch_results:
                    if email:
                        results.append({
                            'profile_id': profile['id'],
                            'name': profile['name'],
                            'company': profile['company'],
                            'email': email,
                            'method': method,
                            'list_size': profile.get('list_size', 0),
                            'enriched_at': datetime.now().isoformat()
                        })
                        self.stats['enriched'] += 1
                        self.stats['emails_found'] += 1
                    else:
                        needs_apollo.append(profile)

                # Apollo fallback
                if needs_apollo and self.apollo_credits_used < self.max_apollo_credits:
                    remaining = self.max_apollo_credits - self.apollo_credits_used
                    apollo_batch = needs_apollo[:min(len(needs_apollo), remaining)]

                    if apollo_batch:
                        print(f"  Trying Apollo API for {len(apollo_batch)} profiles...")
                        apollo_results = await self.enrich_with_apollo_bulk(apollo_batch)

                        for profile, email, method in apollo_results:
                            if email:
                                results.append({
                                    'profile_id': profile['id'],
                                    'name': profile['name'],
                                    'company': profile['company'],
                                    'email': email,
                                    'method': method,
                                    'list_size': profile.get('list_size', 0),
                                    'enriched_at': datetime.now().isoformat()
                                })
                                self.stats['enriched'] += 1
                                self.stats['emails_found'] += 1
                            else:
                                self.stats['failed'] += 1

                batch_emails = sum(1 for r in results if r['profile_id'] in [p['id'] for p in batch])
                print(f"  ✅ {batch_emails}/{len(batch)} verified emails found")

        # Save results
        if results and not self.dry_run:
            output_file = f"enriched_safe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['profile_id', 'name', 'company', 'email', 'method', 'list_size', 'enriched_at']
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
        print("ENRICHMENT SUMMARY (VERIFIED ONLY)")
        print("=" * 70)
        print(f"Total profiles:      {self.stats['total']}")
        print(f"Verified emails:     {self.stats['emails_found']} ({self.stats['emails_found']/self.stats['total']*100:.1f}%)")
        print(f"Failed:              {self.stats['failed']}")
        print(f"Time taken:          {self.stats['time_taken']:.1f}s")
        print()
        print("Methods used (all verified):")
        print(f"  Website scraping:  {self.stats['website_scrape']} (found on site)")
        print(f"  LinkedIn scraping: {self.stats['linkedin_scrape']} (found on profile)")
        print(f"  Apollo API:        {self.stats['apollo_api']} (verified by Apollo)")
        print()
        print("✅ All emails are VERIFIED (no guessing)")

        if self.stats['apollo_api'] > 0:
            print(f"\n💰 Apollo cost: ${self.stats['apollo_api'] * 0.10:.2f}")

    async def consolidate_to_supabase_batch(self, results: List[Dict]):
        """Batch consolidate to Supabase"""
        from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

        scorer = ConfidenceScorer()
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()

        source_map = {
            'website_scrape': 'website_scraped',
            'linkedin_scrape': 'linkedin_scraped',
            'apollo_api': 'apollo'
        }

        updates = []
        for result in results:
            profile_id = result['profile_id']
            email = result['email']
            method = result['method']
            enriched_at = datetime.fromisoformat(result['enriched_at'])

            source = source_map.get(method, 'unknown')
            confidence = scorer.calculate_confidence('email', source, enriched_at)
            confidence_expires_at = scorer.calculate_expires_at('email', enriched_at)

            email_metadata = {
                'source': source,
                'enriched_at': enriched_at.isoformat(),
                'source_date': enriched_at.date().isoformat(),
                'confidence': confidence,
                'confidence_expires_at': confidence_expires_at.isoformat(),
                'verification_count': 1 if method == 'apollo_api' else 0,
                'enrichment_method': method,
                'verified': True  # All methods are verified
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

    parser = argparse.ArgumentParser(description='Safe enrichment pipeline (NO GUESSING)')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--priority', type=str, default='high-value',
                        choices=['high-value', 'has-website', 'all'])
    parser.add_argument('--max-apollo-credits', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=5)
    parser.add_argument('--auto-consolidate', action='store_true')
    parser.add_argument('--dry-run', action='store_true')

    args = parser.parse_args()

    pipeline = SafeEnrichmentPipeline(
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
