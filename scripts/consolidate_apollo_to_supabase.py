#!/usr/bin/env python3
"""
Consolidate Apollo enrichment results to Supabase with confidence tracking.

Usage:
    python scripts/consolidate_apollo_to_supabase.py --input enriched_high_value.csv
"""

import os
import csv
import json
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import confidence scorer
from matching.enrichment.confidence import ConfidenceScorer

load_dotenv()


class ApolloConsolidator:
    """Consolidate Apollo enrichment to Supabase"""

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.scorer = ConfidenceScorer()
        self.stats = {
            'processed': 0,
            'updated': 0,
            'skipped': 0,
            'new_emails': 0
        }

    def consolidate(self, input_file):
        """Consolidate Apollo enrichment results"""
        print("=" * 70)
        print("CONSOLIDATING APOLLO ENRICHMENT TO SUPABASE")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        print(f"Input: {input_file}")
        print()

        # Load enrichment results
        enriched = self.load_enrichment_results(input_file)
        print(f"Loaded {len(enriched)} enriched profiles")
        print()

        # Connect to Supabase
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Process each enriched profile
        for profile in enriched:
            self.stats['processed'] += 1
            profile_id = profile.get('profile_id')

            if not profile_id:
                print(f"⚠️  Skipping profile without ID: {profile.get('name')}")
                self.stats['skipped'] += 1
                continue

            # Get existing profile
            cursor.execute("SELECT * FROM profiles WHERE id = %s", (profile_id,))
            existing = cursor.fetchone()

            if not existing:
                print(f"⚠️  Profile not found in Supabase: {profile.get('name')} ({profile_id})")
                self.stats['skipped'] += 1
                continue

            # Check if we have new data
            new_email = profile.get('email')
            has_updates = False

            if new_email and (not existing.get('email') or existing.get('email') != new_email):
                has_updates = True
                self.stats['new_emails'] += 1

            if not has_updates:
                print(f"⏭️  No new data for: {profile.get('name')}")
                self.stats['skipped'] += 1
                continue

            # Create enrichment metadata
            enrichment_metadata = existing.get('enrichment_metadata') or {}
            if isinstance(enrichment_metadata, str):
                enrichment_metadata = json.loads(enrichment_metadata)

            # Determine source confidence
            source = 'apollo_verified' if profile.get('confidence') == 'verified' else 'apollo'

            # Add email metadata
            if new_email:
                email_metadata = {
                    'source': source,
                    'enriched_at': datetime.now().isoformat(),
                    'source_date': datetime.now().date().isoformat(),
                    'confidence': self.scorer.calculate_confidence(
                        field_name='email',
                        source=source,
                        enriched_at=datetime.now()
                    ),
                    'confidence_expires_at': self.scorer.calculate_expires_at(
                        'email', datetime.now()
                    ).isoformat(),
                    'verification_count': 1 if source == 'apollo_verified' else 0,
                    'last_verification_method': 'apollo_api' if source == 'apollo_verified' else None
                }
                enrichment_metadata['email'] = email_metadata

            # Calculate profile confidence
            profile_confidence = self.scorer.calculate_profile_confidence(enrichment_metadata)

            # Update profile
            print(f"✅ Updating: {profile.get('name')}")
            print(f"   Email: {new_email}")
            print(f"   Confidence: {profile_confidence:.3f}")

            if not self.dry_run:
                cursor.execute("""
                    UPDATE profiles
                    SET email = %s,
                        enrichment_metadata = %s,
                        profile_confidence = %s,
                        last_enriched_at = %s,
                        updated_at = %s
                    WHERE id = %s
                """, (
                    new_email,
                    json.dumps(enrichment_metadata),
                    profile_confidence,
                    datetime.now(),
                    datetime.now(),
                    profile_id
                ))
                self.stats['updated'] += 1

        if not self.dry_run:
            conn.commit()

        cursor.close()
        conn.close()

        # Print summary
        print()
        print("=" * 70)
        print("CONSOLIDATION SUMMARY")
        print("=" * 70)
        print(f"Processed: {self.stats['processed']}")
        print(f"Updated: {self.stats['updated']}")
        print(f"New emails: {self.stats['new_emails']}")
        print(f"Skipped: {self.stats['skipped']}")
        print()

        if self.dry_run:
            print("✅ Dry run complete!")
        else:
            print("✅ Consolidation complete!")
            print()
            print("Updated profiles now have:")
            print("  - Email addresses")
            print("  - Enrichment metadata (source, confidence, dates)")
            print("  - Profile confidence scores")

    def load_enrichment_results(self, input_file):
        """Load Apollo enrichment results from CSV"""
        results = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(row)
        return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Consolidate Apollo enrichment to Supabase')
    parser.add_argument('--input', required=True, help='Input CSV file with Apollo enrichment results')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')

    args = parser.parse_args()

    consolidator = ApolloConsolidator(dry_run=args.dry_run)
    consolidator.consolidate(args.input)
