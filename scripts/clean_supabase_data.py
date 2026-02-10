#!/usr/bin/env python3
"""
Clean Supabase data based on quality assessment.
Merges duplicates, fixes formats, adds quality flags.

Usage:
    # Dry run (see what would happen)
    python scripts/clean_supabase_data.py --input data_quality_report.json --dry-run

    # Execute cleaning
    python scripts/clean_supabase_data.py --input data_quality_report.json
"""
import os
import sys
import re
import json
import argparse
from typing import Dict, List, Tuple
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DataCleaner:
    """Clean Supabase data using existing merge logic"""

    def __init__(self, database_url: str = None, dry_run: bool = False):
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment")

        self.dry_run = dry_run
        self.merged_count = 0
        self.normalized_count = 0
        self.flagged_count = 0

    def connect(self):
        """Establish database connection"""
        return psycopg2.connect(self.database_url)

    def merge_field_values(self, val1: str, val2: str) -> str:
        """
        Merge two field values using existing merge_duplicates.py logic.
        Keep the longer/more complete value.
        """
        val1 = (val1 or '').strip()
        val2 = (val2 or '').strip()

        # Keep the longer/more complete value
        if val1 and val2:
            return val1 if len(val1) >= len(val2) else val2
        else:
            return val1 or val2

    def merge_profiles(self, profile1: Dict, profile2: Dict) -> Dict:
        """
        Merge two profile dictionaries, keeping the best data from each.
        Uses existing merge logic from merge_duplicates.py.
        """
        merged = {}

        # Field list from profiles table (actual columns that exist)
        fields = ['name', 'email', 'company', 'website', 'linkedin', 'booking_link',
                  'phone', 'list_size', 'niche', 'seeking', 'who_you_serve',
                  'what_you_do', 'offering', 'status', 'notes']

        for field in fields:
            if field == 'list_size':
                # For list_size, take the maximum
                val1 = profile1.get(field) or 0
                val2 = profile2.get(field) or 0
                merged[field] = max(val1, val2)
            else:
                # For text fields, merge using existing logic
                merged[field] = self.merge_field_values(
                    profile1.get(field),
                    profile2.get(field)
                )

        # Keep the ID of the first profile
        merged['id'] = profile1['id']
        merged['duplicate_of_id'] = profile2['id']

        return merged

    def merge_duplicates(self, duplicate_groups: List[Dict]):
        """Merge duplicate profiles using existing merge_duplicates.py logic"""
        print("=" * 70)
        print("MERGING DUPLICATE PROFILES")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Only merge HIGH RISK duplicates (exact email matches)
        high_risk_groups = [g for g in duplicate_groups if g['risk'] == 'HIGH']

        print(f"Found {len(high_risk_groups)} HIGH-RISK duplicate groups to merge")
        print(f"(Skipping {len(duplicate_groups) - len(high_risk_groups)} MEDIUM/LOW-RISK groups - manual review needed)")
        print()

        for group in high_risk_groups:
            profiles = group['profiles']
            if len(profiles) != 2:
                print(f"⚠️  Skipping group with {len(profiles)} profiles (only handling pairs)")
                continue

            # Fetch full profile data
            profile1_id = profiles[0]['id']
            profile2_id = profiles[1]['id']

            cursor.execute("""
                SELECT * FROM profiles WHERE id = %s
            """, (profile1_id,))
            p1 = cursor.fetchone()

            cursor.execute("""
                SELECT * FROM profiles WHERE id = %s
            """, (profile2_id,))
            p2 = cursor.fetchone()

            if not p1 or not p2:
                print(f"⚠️  Could not fetch profiles {profile1_id}, {profile2_id}")
                continue

            # Merge the profiles
            merged = self.merge_profiles(dict(p1), dict(p2))

            print(f"Merging: {p1['name']} ({p1.get('company', 'N/A')})")
            print(f"   With: {p2['name']} ({p2.get('company', 'N/A')})")
            print(f" Reason: {group['reason']}")

            if self.dry_run:
                print("   [DRY RUN] Would update profile", merged['id'])
                print("   [DRY RUN] Would delete profile", merged['duplicate_of_id'])
            else:
                # Update first profile with merged data (actual columns that exist)
                update_fields = [
                    'name', 'email', 'company', 'website', 'linkedin', 'booking_link',
                    'phone', 'list_size', 'niche', 'seeking', 'who_you_serve',
                    'what_you_do', 'offering', 'notes'
                ]

                set_clause = ', '.join([f"{field} = %s" for field in update_fields])
                values = [merged.get(field) for field in update_fields]
                values.append(merged['id'])

                cursor.execute(f"""
                    UPDATE profiles
                    SET {set_clause},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, values)

                # Delete second profile
                cursor.execute("""
                    DELETE FROM profiles WHERE id = %s
                """, (merged['duplicate_of_id'],))

                print(f"   ✅ Merged into {merged['id']}, deleted {merged['duplicate_of_id']}")
                self.merged_count += 1

            print()

        if not self.dry_run:
            conn.commit()

        cursor.close()
        conn.close()

        print(f"Total profiles merged: {self.merged_count}")

    def normalize_urls(self):
        """Standardize URL formats"""
        print()
        print("=" * 70)
        print("NORMALIZING URLS")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get all profiles with URLs
        cursor.execute("""
            SELECT id, name, linkedin, website, booking_link
            FROM profiles
            WHERE linkedin IS NOT NULL OR website IS NOT NULL OR booking_link IS NOT NULL
        """)
        profiles = cursor.fetchall()

        print(f"Checking {len(profiles)} profiles for URL normalization...")
        print()

        for profile in profiles:
            normalized = {}
            changes = []

            # Normalize LinkedIn (remove query params, ensure https)
            if profile['linkedin']:
                original = profile['linkedin']
                normalized_url = original

                # Remove query params
                normalized_url = re.sub(r'\?.*$', '', normalized_url)
                # Remove trailing slash
                normalized_url = normalized_url.rstrip('/')
                # Ensure https
                if normalized_url.startswith('http://'):
                    normalized_url = normalized_url.replace('http://', 'https://', 1)

                if normalized_url != original:
                    normalized['linkedin'] = normalized_url
                    changes.append(f"LinkedIn: {original[:50]}... → {normalized_url[:50]}...")

            # Normalize website (ensure https where possible)
            if profile['website']:
                original = profile['website']
                normalized_url = original.strip()

                # Remove trailing slash
                normalized_url = normalized_url.rstrip('/')

                # Only upgrade to HTTPS if it's a simple http:// URL
                # (Don't upgrade if it might break - be conservative)
                if normalized_url.startswith('http://') and '?' not in original:
                    # Conservative: only suggest HTTPS, don't auto-convert
                    changes.append(f"Website could be HTTPS: {original}")
                    # normalized['website'] = normalized_url.replace('http://', 'https://', 1)

                elif normalized_url != original:
                    normalized['website'] = normalized_url
                    changes.append(f"Website: Removed trailing slash")

            # Normalize booking link
            if profile['booking_link']:
                original = profile['booking_link']
                normalized_url = original.strip()

                # Add https:// if missing
                if not normalized_url.startswith(('http://', 'https://')):
                    normalized_url = 'https://' + normalized_url
                    normalized['booking_link'] = normalized_url
                    changes.append(f"Booking: Added https://")

            # Apply changes
            if changes:
                print(f"{profile['name']}:")
                for change in changes:
                    print(f"  - {change}")

                if not self.dry_run and normalized:
                    # Build UPDATE query
                    set_parts = []
                    values = []
                    for field, value in normalized.items():
                        set_parts.append(f"{field} = %s")
                        values.append(value)

                    if set_parts:
                        values.append(profile['id'])
                        cursor.execute(f"""
                            UPDATE profiles
                            SET {', '.join(set_parts)},
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, values)
                        self.normalized_count += 1

        if not self.dry_run:
            conn.commit()

        cursor.close()
        conn.close()

        print()
        print(f"Total URLs normalized: {self.normalized_count}")

    def flag_invalid_emails(self, invalid_emails: List[Dict]):
        """Mark invalid emails, don't delete"""
        print()
        print("=" * 70)
        print("FLAGGING INVALID EMAILS")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        print(f"Found {len(invalid_emails)} invalid/suspicious emails to flag")
        print()

        for item in invalid_emails:
            profile = item['profile']
            issues = item['issues']

            print(f"{profile['name']}: {item['email']}")
            print(f"  Issues: {', '.join(issues)}")

            if not self.dry_run:
                # Add to notes field
                cursor.execute("""
                    UPDATE profiles
                    SET notes = COALESCE(notes, '') || %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (f"\n[AUTO-FLAG] Email validation issues: {', '.join(issues)}", profile['id']))

                self.flagged_count += 1

        if not self.dry_run:
            conn.commit()

        cursor.close()
        conn.close()

        print()
        print(f"Total emails flagged: {self.flagged_count}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clean Supabase data based on quality assessment')
    parser.add_argument('--input', required=True, help='JSON report from assess_data_quality.py')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')

    args = parser.parse_args()

    # Load assessment report
    with open(args.input, 'r') as f:
        report = json.load(f)

    print("=" * 70)
    print("SUPABASE DATA CLEANING")
    print("=" * 70)
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE EXECUTION'}")
    print(f"Report: {args.input}")
    print(f"Generated: {report['generated_at']}")
    print()
    print("Summary from assessment:")
    print(f"  - Total issues: {report['summary']['total_issues']}")
    print(f"  - High risk: {report['summary']['high_risk']}")
    print(f"  - Duplicates: {report['summary']['duplicates']}")
    print(f"  - Invalid emails: {report['summary']['invalid_emails']}")
    print()

    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
        print()

    # Create cleaner
    cleaner = DataCleaner(dry_run=args.dry_run)

    # 1. Merge HIGH-RISK duplicates
    cleaner.merge_duplicates(report['duplicates'])

    # 2. Normalize URLs
    cleaner.normalize_urls()

    # 3. Flag invalid emails
    cleaner.flag_invalid_emails(report['invalid_emails'])

    # Summary
    print()
    print("=" * 70)
    print("CLEANING SUMMARY")
    print("=" * 70)
    print(f"Profiles merged: {cleaner.merged_count}")
    print(f"URLs normalized: {cleaner.normalized_count}")
    print(f"Emails flagged: {cleaner.flagged_count}")
    print()

    if args.dry_run:
        print("✅ Dry run complete! Review the output above.")
        print("   To execute, run without --dry-run flag")
    else:
        print("✅ Data cleaning complete!")
        print()
        print("Next steps:")
        print("  1. Review the changes in Supabase")
        print("  2. Manually review MEDIUM-RISK duplicates if needed")
        print("  3. Proceed to Phase 1 (consolidation with confidence scoring)")
