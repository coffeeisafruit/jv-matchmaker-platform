#!/usr/bin/env python3
"""
Automated Enrichment Pipeline

Progressive enrichment strategy:
1. FREE: Website scraping (company website contact pages)
2. FREE: LinkedIn scraping (public profile data)
3. LOW-COST: Email pattern guessing (firstname@company.com)
4. PAID: Apollo.io API (only when free methods fail)

Usage:
    python scripts/automated_enrichment_pipeline.py --limit 20 --auto-consolidate
    python scripts/automated_enrichment_pipeline.py --priority high-value --max-apollo-credits 10
"""

import os
import sys
import csv
import re
import asyncio
import requests
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse
import time

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


class EnrichmentMethod:
    """Track which enrichment methods were used"""
    WEBSITE_SCRAPE = "website_scrape"
    LINKEDIN_SCRAPE = "linkedin_scrape"
    EMAIL_PATTERN = "email_pattern"
    APOLLO_API = "apollo_api"


class ProgressiveEnrichmentPipeline:
    """Automated enrichment pipeline with progressive strategy"""

    def __init__(self, max_apollo_credits=0, dry_run=False):
        self.max_apollo_credits = max_apollo_credits
        self.apollo_credits_used = 0
        self.dry_run = dry_run

        self.stats = {
            'total': 0,
            'enriched': 0,
            'emails_found': 0,
            'website_scrape': 0,
            'linkedin_scrape': 0,
            'email_pattern': 0,
            'apollo_api': 0,
            'failed': 0
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
        elif priority == 'has-linkedin':
            query = """
                SELECT id, name, email, company, website, linkedin, list_size
                FROM profiles
                WHERE (email IS NULL OR email = '')
                  AND linkedin IS NOT NULL AND linkedin != ''
                  AND name IS NOT NULL AND name != ''
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
            """
        else:  # all
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

    async def enrich_profile(self, profile: Dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Enrich a single profile using progressive strategy.
        Returns: (email, method_used)
        """
        name = profile['name']
        company = profile.get('company')
        website = profile.get('website')
        linkedin = profile.get('linkedin')

        print(f"\n{'='*70}")
        print(f"Enriching: {name}")
        print(f"  Company: {company or 'N/A'}")
        print(f"  Website: {website or 'N/A'}")
        print(f"  LinkedIn: {linkedin or 'N/A'}")
        print(f"  List size: {profile.get('list_size', 0):,}")
        print(f"{'='*70}")

        # STRATEGY 1: Website scraping (FREE)
        if website:
            print("🔍 Trying: Website scraping (FREE)...")
            email = await self.try_website_scraping(website, name)
            if email:
                print(f"✅ Found via website: {email}")
                self.stats['website_scrape'] += 1
                return email, EnrichmentMethod.WEBSITE_SCRAPE

        # STRATEGY 2: LinkedIn scraping (FREE)
        if linkedin:
            print("🔍 Trying: LinkedIn scraping (FREE)...")
            email = await self.try_linkedin_scraping(linkedin)
            if email:
                print(f"✅ Found via LinkedIn: {email}")
                self.stats['linkedin_scrape'] += 1
                return email, EnrichmentMethod.LINKEDIN_SCRAPE

        # STRATEGY 3: Email pattern guessing (FREE)
        if company and website:
            print("🔍 Trying: Email pattern guessing (FREE)...")
            email = await self.try_email_pattern(name, website)
            if email:
                print(f"✅ Guessed email pattern: {email}")
                self.stats['email_pattern'] += 1
                return email, EnrichmentMethod.EMAIL_PATTERN

        # STRATEGY 4: Apollo.io API (PAID - last resort)
        if self.apollo_credits_used < self.max_apollo_credits and company:
            print(f"🔍 Trying: Apollo.io API (PAID - {self.apollo_credits_used}/{self.max_apollo_credits} credits used)...")
            email = await self.try_apollo_api(name, company, website)
            if email:
                print(f"✅ Found via Apollo: {email}")
                self.apollo_credits_used += 1
                self.stats['apollo_api'] += 1
                return email, EnrichmentMethod.APOLLO_API
        elif self.apollo_credits_used >= self.max_apollo_credits:
            print(f"⚠️  Apollo credits exhausted ({self.apollo_credits_used}/{self.max_apollo_credits})")

        print("❌ No email found with any method")
        return None, None

    async def try_website_scraping(self, website: str, name: str) -> Optional[str]:
        """Scrape company website for contact email (FREE)"""
        if self.dry_run:
            return None

        try:
            # Normalize URL
            if not website.startswith('http'):
                website = f'https://{website}'

            # Try common contact page patterns
            contact_urls = [
                website,
                f"{website}/contact",
                f"{website}/about",
                f"{website}/team"
            ]

            for url in contact_urls:
                try:
                    response = requests.get(url, timeout=5, headers={
                        'User-Agent': 'Mozilla/5.0 (compatible; EnrichmentBot/1.0)'
                    })

                    if response.status_code == 200:
                        # Look for email patterns
                        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', response.text)

                        # Filter out generic/spam emails
                        valid_emails = [
                            e for e in emails
                            if not any(x in e.lower() for x in ['noreply', 'spam', 'abuse', 'postmaster', 'example'])
                        ]

                        if valid_emails:
                            # Prefer emails matching the person's name
                            name_parts = name.lower().split()
                            for email in valid_emails:
                                if any(part in email.lower() for part in name_parts):
                                    return email

                            # Otherwise return first valid email
                            return valid_emails[0]

                except requests.RequestException:
                    continue

        except Exception as e:
            print(f"  ⚠️  Website scraping error: {str(e)}")

        return None

    async def try_linkedin_scraping(self, linkedin_url: str) -> Optional[str]:
        """Scrape LinkedIn profile for contact email (FREE but limited)"""
        if self.dry_run:
            return None

        try:
            # LinkedIn public profiles sometimes show contact info
            # This is a simplified approach - real scraping would need more sophistication
            response = requests.get(linkedin_url, timeout=5, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; EnrichmentBot/1.0)'
            })

            if response.status_code == 200:
                emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', response.text)
                valid_emails = [
                    e for e in emails
                    if not any(x in e.lower() for x in ['noreply', 'spam', 'linkedin', 'example'])
                ]

                if valid_emails:
                    return valid_emails[0]

        except Exception as e:
            print(f"  ⚠️  LinkedIn scraping error: {str(e)}")

        return None

    async def try_email_pattern(self, name: str, website: str) -> Optional[str]:
        """Guess email using common patterns (FREE)"""
        if self.dry_run:
            return None

        try:
            # Extract domain
            domain = website.replace('https://', '').replace('http://', '').split('/')[0]
            if domain.startswith('www.'):
                domain = domain[4:]

            # Split name
            name_parts = name.strip().split()
            if len(name_parts) < 2:
                return None

            first_name = name_parts[0].lower()
            last_name = name_parts[-1].lower()

            # Common email patterns
            patterns = [
                f"{first_name}@{domain}",
                f"{first_name}.{last_name}@{domain}",
                f"{first_name[0]}{last_name}@{domain}",
                f"{first_name}_{last_name}@{domain}",
            ]

            # Try to verify with a simple MX record check
            import dns.resolver

            try:
                dns.resolver.resolve(domain, 'MX')
                # Domain has valid MX records, patterns are plausible
                # Return most common pattern
                return patterns[1]  # firstname.lastname@domain
            except:
                pass

        except Exception as e:
            print(f"  ⚠️  Email pattern error: {str(e)}")

        return None

    async def try_apollo_api(self, name: str, company: str, website: Optional[str]) -> Optional[str]:
        """Use Apollo.io API (PAID - last resort)"""
        if self.dry_run:
            return None

        api_key = os.environ.get('APOLLO_API_KEY')
        if not api_key:
            return None

        try:
            # Split name
            name_parts = name.strip().split(' ', 1)
            first_name = name_parts[0] if name_parts else name
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            payload = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": company,
                "reveal_personal_emails": True
            }

            if website:
                domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                if domain.startswith('www.'):
                    domain = domain[4:]
                payload["domain"] = domain

            response = requests.post(
                "https://api.apollo.io/api/v1/people/match",
                json=payload,
                headers={
                    'Cache-Control': 'no-cache',
                    'Content-Type': 'application/json',
                    'x-api-key': api_key
                },
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                person = data.get('person', {})
                email = person.get('email')

                if email:
                    return email

        except Exception as e:
            print(f"  ⚠️  Apollo API error: {str(e)}")

        return None

    async def run(self, limit=20, priority='high-value', auto_consolidate=False):
        """Run the automated enrichment pipeline"""
        print("=" * 70)
        print("AUTOMATED ENRICHMENT PIPELINE")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ENRICHMENT'}")
        print(f"Priority: {priority}")
        print(f"Limit: {limit}")
        print(f"Max Apollo credits: {self.max_apollo_credits}")
        print(f"Auto-consolidate: {auto_consolidate}")
        print()

        # Get profiles to enrich
        print("Fetching profiles needing enrichment...")
        profiles = self.get_profiles_to_enrich(limit, priority)
        print(f"Found {len(profiles)} profiles to enrich")
        print()

        # Enrich each profile
        results = []

        for profile in profiles:
            self.stats['total'] += 1

            try:
                email, method = await self.enrich_profile(profile)

                if email:
                    self.stats['enriched'] += 1
                    self.stats['emails_found'] += 1

                    results.append({
                        'profile_id': profile['id'],
                        'name': profile['name'],
                        'company': profile['company'],
                        'email': email,
                        'method': method,
                        'list_size': profile.get('list_size', 0),
                        'enriched_at': datetime.now().isoformat()
                    })
                else:
                    self.stats['failed'] += 1

                # Small delay between profiles
                await asyncio.sleep(1)

            except Exception as e:
                print(f"❌ Error enriching {profile['name']}: {str(e)}")
                self.stats['failed'] += 1

        # Save results to CSV
        if results and not self.dry_run:
            output_file = f"enriched_automated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['profile_id', 'name', 'company', 'email', 'method', 'list_size', 'enriched_at']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)

            print(f"\n✅ Results saved to: {output_file}")

            # Auto-consolidate to Supabase if requested
            if auto_consolidate:
                print("\nAuto-consolidating to Supabase...")
                await self.consolidate_to_supabase(results)

        # Print summary
        print("\n" + "=" * 70)
        print("ENRICHMENT SUMMARY")
        print("=" * 70)
        print(f"Total profiles:      {self.stats['total']}")
        print(f"Emails found:        {self.stats['emails_found']} ({self.stats['emails_found']/self.stats['total']*100:.1f}%)")
        print(f"Failed:              {self.stats['failed']}")
        print()
        print("Methods used:")
        print(f"  Website scraping:  {self.stats['website_scrape']} (FREE)")
        print(f"  LinkedIn scraping: {self.stats['linkedin_scrape']} (FREE)")
        print(f"  Email patterns:    {self.stats['email_pattern']} (FREE)")
        print(f"  Apollo API:        {self.stats['apollo_api']} (${self.stats['apollo_api'] * 0.10:.2f})")
        print()

        if self.stats['apollo_api'] > 0:
            free_success_rate = (self.stats['website_scrape'] + self.stats['linkedin_scrape'] + self.stats['email_pattern']) / self.stats['emails_found'] * 100 if self.stats['emails_found'] > 0 else 0
            print(f"💰 Cost: ${self.stats['apollo_api'] * 0.10:.2f}")
            print(f"💡 Free methods: {free_success_rate:.1f}% success rate")

    async def consolidate_to_supabase(self, results: List[Dict]):
        """Consolidate enrichment results to Supabase with confidence tracking"""
        from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

        scorer = ConfidenceScorer()
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()

        updated = 0

        for result in results:
            profile_id = result['profile_id']
            email = result['email']
            method = result['method']
            enriched_at = datetime.fromisoformat(result['enriched_at'])

            # Map method to source for confidence scoring
            source_map = {
                EnrichmentMethod.WEBSITE_SCRAPE: 'website_scraped',
                EnrichmentMethod.LINKEDIN_SCRAPE: 'linkedin_scraped',
                EnrichmentMethod.EMAIL_PATTERN: 'email_domain_inferred',
                EnrichmentMethod.APOLLO_API: 'apollo'
            }
            source = source_map.get(method, 'unknown')

            # Calculate confidence
            confidence = scorer.calculate_confidence(
                field_name='email',
                source=source,
                enriched_at=enriched_at
            )

            confidence_expires_at = scorer.calculate_expires_at('email', enriched_at)

            # Create enrichment metadata
            email_metadata = {
                'source': source,
                'enriched_at': enriched_at.isoformat(),
                'source_date': enriched_at.date().isoformat(),
                'confidence': confidence,
                'confidence_expires_at': confidence_expires_at.isoformat(),
                'verification_count': 0,
                'enrichment_method': method
            }

            # Update profile
            cursor.execute("""
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
            """, (
                email,
                json.dumps(email_metadata),
                confidence,
                enriched_at,
                datetime.now(),
                profile_id
            ))

            updated += 1

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Updated {updated} profiles in Supabase with confidence tracking")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Automated enrichment pipeline with progressive strategy')
    parser.add_argument('--limit', type=int, default=20, help='Number of profiles to enrich')
    parser.add_argument('--priority', type=str, default='high-value',
                        choices=['high-value', 'has-website', 'has-linkedin', 'all'],
                        help='Enrichment priority')
    parser.add_argument('--max-apollo-credits', type=int, default=0,
                        help='Maximum Apollo API credits to use (0 = no Apollo)')
    parser.add_argument('--auto-consolidate', action='store_true',
                        help='Automatically consolidate results to Supabase')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without making API calls')

    args = parser.parse_args()

    pipeline = ProgressiveEnrichmentPipeline(
        max_apollo_credits=args.max_apollo_credits,
        dry_run=args.dry_run
    )

    asyncio.run(pipeline.run(
        limit=args.limit,
        priority=args.priority,
        auto_consolidate=args.auto_consolidate
    ))


if __name__ == '__main__':
    main()
