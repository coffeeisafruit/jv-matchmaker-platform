#!/usr/bin/env python3
"""
Update Supabase Profiles with Newly Found Emails

Usage:
  python scripts/update_enriched_emails.py --input enriched_emails.csv

CSV Format:
  profile_id,name,email,source,notes

Example:
  706e20c9-93fb-4aa0-864e-0d11e82cd024,Michelle Tennant,michelle@wasabipublicity.com,Hunter.io,Verified via website
"""

import os
import sys
import django
import csv
from datetime import datetime

# Setup Django environment
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from matching.models import SupabaseProfile


def update_profiles_from_csv(input_file, dry_run=False):
    """
    Update Supabase profiles with newly found emails

    Args:
        input_file: Path to CSV with enriched data
        dry_run: If True, show what would be updated without making changes

    Returns:
        Dictionary with update statistics
    """
    stats = {
        'total_rows': 0,
        'found': 0,
        'updated': 0,
        'skipped': 0,
        'errors': []
    }

    print(f"\n{'='*70}")
    print(f"UPDATING SUPABASE PROFILES FROM CSV")
    print(f"{'='*70}\n")
    print(f"Input File: {input_file}")
    print(f"Dry Run: {'Yes (no changes will be made)' if dry_run else 'No (will update database)'}\n")

    # Read CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            stats['total_rows'] += 1

            profile_id = row.get('profile_id', '').strip()
            name = row.get('name', '').strip()
            email = row.get('email', '').strip()
            source = row.get('source', 'Manual').strip()
            notes = row.get('notes', '').strip()

            # Validate required fields
            if not profile_id:
                stats['errors'].append(f"Row {stats['total_rows']}: Missing profile_id")
                stats['skipped'] += 1
                continue

            if not email:
                stats['errors'].append(f"Row {stats['total_rows']}: Missing email for {name}")
                stats['skipped'] += 1
                continue

            # Find profile
            try:
                profile = SupabaseProfile.objects.get(id=profile_id)
                stats['found'] += 1

                # Check if profile already has email
                if profile.email and profile.email.strip():
                    print(f"⚠️  {name:30} | Already has email: {profile.email}")
                    stats['skipped'] += 1
                    continue

                # Update email
                if dry_run:
                    print(f"✓  {name:30} | Would add: {email:40} | Source: {source}")
                else:
                    # NOTE: Supabase models have managed=False, so .save() won't work!
                    # You need to update Supabase directly via API or SQL
                    print(f"⚠️  {name:30} | Found email: {email:40}")
                    print(f"    → MANUAL ACTION REQUIRED: Update Supabase directly")
                    print(f"    → Profile ID: {profile_id}")
                    print(f"    → SQL: UPDATE profiles SET email='{email}' WHERE id='{profile_id}';")

                stats['updated'] += 1

            except SupabaseProfile.DoesNotExist:
                stats['errors'].append(f"Profile not found: {profile_id} ({name})")
                stats['skipped'] += 1
            except Exception as e:
                stats['errors'].append(f"Error updating {name}: {str(e)}")
                stats['skipped'] += 1

    # Print summary
    print(f"\n{'='*70}")
    print("UPDATE SUMMARY")
    print(f"{'='*70}\n")

    print(f"Total Rows:          {stats['total_rows']}")
    print(f"Profiles Found:      {stats['found']}")
    print(f"Would Update:        {stats['updated']}")
    print(f"Skipped:             {stats['skipped']}")
    print(f"Errors:              {len(stats['errors'])}")

    if stats['errors']:
        print(f"\nErrors:")
        for error in stats['errors']:
            print(f"  - {error}")

    print(f"\n{'='*70}")
    print("⚠️  IMPORTANT: Supabase Profiles Cannot Be Updated from Django")
    print(f"{'='*70}\n")
    print("The SupabaseProfile model has `managed = False`, which means Django")
    print("cannot make changes to the Supabase database.\n")
    print("To update these profiles, you need to:")
    print("  1. Use Supabase Admin UI")
    print("  2. Use Supabase SQL editor")
    print("  3. Use Supabase Python client directly")
    print("  4. Create a management command with raw SQL\n")

    if stats['updated'] > 0:
        print("SQL Commands to update Supabase:")
        print("-" * 70)

        # Re-read CSV to generate SQL
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                profile_id = row.get('profile_id', '').strip()
                email = row.get('email', '').strip()

                if profile_id and email:
                    print(f"UPDATE profiles SET email = '{email}' WHERE id = '{profile_id}';")

    return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Update Supabase profiles with enriched emails')
    parser.add_argument('--input', required=True, help='Input CSV file with enriched data')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')

    args = parser.parse_args()

    try:
        stats = update_profiles_from_csv(args.input, dry_run=args.dry_run)

        print(f"\n{'='*70}")
        if args.dry_run:
            print("✅ DRY RUN COMPLETE - No changes made")
        else:
            print("✅ ANALYSIS COMPLETE")
            print("   Use SQL commands above to update Supabase")
        print(f"{'='*70}\n")

    except FileNotFoundError:
        print(f"\n❌ Error: File not found: {args.input}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
