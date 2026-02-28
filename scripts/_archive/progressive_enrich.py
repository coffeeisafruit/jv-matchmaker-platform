#!/usr/bin/env python3
"""
Progressive enrichment using SmartEnrichmentService.

Enriches profiles using free methods first:
1. Website scraping (free)
2. LinkedIn scraping (free)
3. OWL deep research (free but API-intensive)
4. Only uses paid APIs when needed and authorized

Usage:
    python scripts/progressive_enrich.py --limit 20 --priority high-value
    python scripts/progressive_enrich.py --limit 50 --dry-run
"""

import os
import sys
import csv
import argparse
import asyncio
from datetime import datetime

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

from matching.enrichment.smart_enrichment_service import SmartEnrichmentService
from matching.models import SupabaseProfile
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


class ProgressiveEnricher:
    """Progressive enrichment using SmartEnrichmentService"""

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.service = SmartEnrichmentService()
        self.stats = {
            'processed': 0,
            'enriched': 0,
            'skipped': 0,
            'errors': 0,
            'fields_added': 0
        }

    def get_profiles_to_enrich(self, limit=20, priority='high-value'):
        """Get profiles that need enrichment"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if priority == 'high-value':
            # High list size, missing email
            query = """
                SELECT id, name, email, company, website, linkedin, phone,
                       list_size, niche
                FROM profiles
                WHERE (email IS NULL OR email = '')
                  AND list_size > 10000
                ORDER BY list_size DESC
                LIMIT %s
            """
        elif priority == 'missing-email':
            # Any profile missing email
            query = """
                SELECT id, name, email, company, website, linkedin, phone,
                       list_size, niche
                FROM profiles
                WHERE (email IS NULL OR email = '')
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
            """
        elif priority == 'low-confidence':
            # Profiles with low confidence or stale data
            query = """
                SELECT id, name, email, company, website, linkedin, phone,
                       list_size, niche, profile_confidence, last_enriched_at
                FROM profiles
                WHERE profile_confidence < 0.5
                   OR last_enriched_at < (NOW() - INTERVAL '90 days')
                ORDER BY profile_confidence ASC, last_enriched_at ASC NULLS FIRST
                LIMIT %s
            """
        else:
            # All profiles needing enrichment
            query = """
                SELECT id, name, email, company, website, linkedin, phone,
                       list_size, niche
                FROM profiles
                WHERE (email IS NULL OR email = '')
                   OR (website IS NULL OR website = '')
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
            """

        cursor.execute(query, (limit,))
        profiles = cursor.fetchall()

        cursor.close()
        conn.close()

        return [dict(p) for p in profiles]

    async def enrich_profile(self, profile):
        """Enrich a single profile using SmartEnrichmentService"""
        contact = {
            'name': profile['name'],
            'company': profile.get('company'),
            'website': profile.get('website'),
            'linkedin': profile.get('linkedin'),
            'email': profile.get('email'),
            'phone': profile.get('phone')
        }

        print(f"\nEnriching: {profile['name']}")
        print(f"  Company: {profile.get('company', 'N/A')}")
        print(f"  List size: {profile.get('list_size', 0):,}")
        print(f"  Current email: {profile.get('email', '(missing)')}")

        if self.dry_run:
            print("  [DRY RUN] Would enrich using SmartEnrichmentService")
            return None

        # Use SmartEnrichmentService with progressive strategy
        # This tries free methods first (website, LinkedIn, OWL)
        result = await self.service.enrich_contact(contact, priority='progressive')

        if result:
            fields_added = []
            if result.get('email') and not contact.get('email'):
                fields_added.append('email')
            if result.get('phone') and not contact.get('phone'):
                fields_added.append('phone')
            if result.get('linkedin') and not contact.get('linkedin'):
                fields_added.append('linkedin')

            if fields_added:
                print(f"  ✅ Found: {', '.join(fields_added)}")
                self.stats['fields_added'] += len(fields_added)
                self.stats['enriched'] += 1
                return result
            else:
                print(f"  ⚠️  No new data found")
                self.stats['skipped'] += 1
        else:
            print(f"  ⚠️  Enrichment returned no results")
            self.stats['skipped'] += 1

        return result

    def save_results(self, enriched_profiles, output_file='progressive_enriched.csv'):
        """Save enriched profiles to CSV"""
        if not enriched_profiles:
            print("\nNo profiles were enriched")
            return

        fieldnames = ['id', 'name', 'email', 'phone', 'linkedin', 'company',
                      'website', 'list_size', 'niche', 'enrichment_method']

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for profile_id, result in enriched_profiles.items():
                writer.writerow({
                    'id': profile_id,
                    'name': result.get('name'),
                    'email': result.get('email'),
                    'phone': result.get('phone'),
                    'linkedin': result.get('linkedin'),
                    'company': result.get('company'),
                    'website': result.get('website'),
                    'list_size': result.get('list_size'),
                    'niche': result.get('niche'),
                    'enrichment_method': result.get('enrichment_method', 'progressive')
                })

        print(f"\n✅ Saved {len(enriched_profiles)} enriched profiles to: {output_file}")

    async def run(self, limit=20, priority='high-value', output_file='progressive_enriched.csv'):
        """Run progressive enrichment"""
        print("=" * 70)
        print("PROGRESSIVE ENRICHMENT (SmartEnrichmentService)")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ENRICHMENT'}")
        print(f"Priority: {priority}")
        print(f"Limit: {limit}")
        print()

        # Get profiles to enrich
        profiles = self.get_profiles_to_enrich(limit, priority)
        print(f"Found {len(profiles)} profiles to enrich")
        print()

        # Enrich each profile
        enriched_profiles = {}

        for profile in profiles:
            self.stats['processed'] += 1

            try:
                result = await self.enrich_profile(profile)
                if result:
                    enriched_profiles[profile['id']] = {
                        **profile,
                        **result
                    }
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
                self.stats['errors'] += 1

        # Save results
        if not self.dry_run and enriched_profiles:
            self.save_results(enriched_profiles, output_file)

        # Print summary
        print()
        print("=" * 70)
        print("ENRICHMENT SUMMARY")
        print("=" * 70)
        print(f"Processed: {self.stats['processed']}")
        print(f"Enriched: {self.stats['enriched']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Fields added: {self.stats['fields_added']}")
        print()

        if self.stats['enriched'] > 0 and not self.dry_run:
            print("✅ Progressive enrichment complete!")
            print()
            print("Next steps:")
            print(f"  1. Review enriched profiles: {output_file}")
            print("  2. Consolidate to Supabase:")
            print(f"     python manage.py consolidate_enrichment --source progressive")
        elif self.dry_run:
            print("✅ Dry run complete!")
        else:
            print("⚠️  No profiles were enriched")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Progressive enrichment using SmartEnrichmentService')
    parser.add_argument('--limit', type=int, default=20, help='Number of profiles to enrich')
    parser.add_argument('--priority', type=str, default='high-value',
                        choices=['high-value', 'missing-email', 'low-confidence', 'all'],
                        help='Enrichment priority')
    parser.add_argument('--output', type=str, default='progressive_enriched.csv',
                        help='Output CSV file')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without enriching')

    args = parser.parse_args()

    enricher = ProgressiveEnricher(dry_run=args.dry_run)
    asyncio.run(enricher.run(limit=args.limit, priority=args.priority, output_file=args.output))
