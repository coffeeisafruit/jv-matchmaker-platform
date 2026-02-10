"""
Import scraped JV Directory data into Supabase.
Handles 10,000+ records with upsert logic.
"""

import csv
import re
from datetime import datetime
from pathlib import Path
from django.core.management.base import BaseCommand
from django.db import transaction
from matching.models import SupabaseProfile


class Command(BaseCommand):
    help = 'Import scraped JV Directory data into Supabase profiles table'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            type=str,
            default='/Users/josephtepe/Projects/jv-matchmaker-platform/jv_directory_full_with_contacts.csv',
            help='Path to the scraped CSV file'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview changes without saving'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Limit number of records to process (0 = all)'
        )

    def handle(self, *args, **options):
        csv_file = Path(options['file'])
        dry_run = options['dry_run']
        limit = options['limit']

        if not csv_file.exists():
            self.stdout.write(self.style.ERROR(f'File not found: {csv_file}'))
            return

        self.stdout.write(self.style.SUCCESS('\n' + '='*60))
        self.stdout.write(self.style.SUCCESS('IMPORTING SCRAPED JV DIRECTORY DATA'))
        self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be saved\n'))

        # Load existing profiles for matching
        self.stdout.write('Loading existing profiles...')
        existing_profiles = {
            p.name.lower().strip(): p
            for p in SupabaseProfile.objects.all()
        }
        self.stdout.write(f'Found {len(existing_profiles)} existing profiles\n')

        # Read CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        total_rows = len(rows)
        if limit > 0:
            rows = rows[:limit]
            self.stdout.write(f'Processing {len(rows)} of {total_rows} records (limited)\n')
        else:
            self.stdout.write(f'Processing {total_rows} records\n')

        # Stats
        created = 0
        updated = 0
        skipped = 0
        errors = 0

        # Process records individually (no batch transaction to avoid error cascades)
        for i, row in enumerate(rows):
            try:
                result = self._process_row(row, existing_profiles, dry_run)
                if result == 'created':
                    created += 1
                elif result == 'updated':
                    updated += 1
                elif result == 'skipped':
                    skipped += 1
            except Exception as e:
                errors += 1
                if errors <= 10:  # Only show first 10 errors
                    self.stdout.write(self.style.ERROR(f'Error: {row.get("name", "?")} - {str(e)[:100]}'))

            # Progress update every 500 records
            if (i + 1) % 500 == 0:
                self.stdout.write(f'  Processed {i+1}/{len(rows)} ({100*(i+1)//len(rows)}%)')

        # Summary
        self.stdout.write('\n' + '='*60)
        self.stdout.write('IMPORT SUMMARY')
        self.stdout.write('='*60)
        self.stdout.write(f'  Created: {created}')
        self.stdout.write(f'  Updated: {updated}')
        self.stdout.write(f'  Skipped: {skipped}')
        self.stdout.write(f'  Errors:  {errors}')
        self.stdout.write(f'  Total:   {len(rows)}')

        if dry_run:
            self.stdout.write(self.style.WARNING('\nDRY RUN - No changes were saved'))
        else:
            self.stdout.write(self.style.SUCCESS('\nImport complete!'))

    # Valid status values (from Supabase check constraint)
    VALID_STATUSES = {
        'Member', 'Non Member Resource', 'Pending', 'Active', 'Inactive',
        'Premium', 'Basic', 'Trial', 'Suspended'
    }

    def _process_row(self, row: dict, existing: dict, dry_run: bool) -> str:
        """Process a single row. Returns 'created', 'updated', or 'skipped'."""
        name = row.get('name', '').strip()
        if not name:
            return 'skipped'

        name_lower = name.lower()

        # Parse list_size (handle various formats, skip URLs/invalid values)
        list_size_raw = row.get('list_size', '')
        if list_size_raw and not list_size_raw.startswith('http') and list_size_raw != 'Update':
            list_size = self._parse_number(list_size_raw)
        else:
            list_size = 0

        social_reach_raw = row.get('social_reach', '')
        if social_reach_raw and not social_reach_raw.startswith('http'):
            social_reach = self._parse_number(social_reach_raw)
        else:
            social_reach = 0

        # Extract clean email
        email = self._extract_email(row.get('email', ''))

        # Extract website from the website field (may contain extra info)
        website = self._extract_url(row.get('website', ''))

        # Clean business_focus
        business_focus = row.get('business_focus', '').strip()

        # Status mapping - validate against allowed values
        raw_status = row.get('status', '').strip()
        if raw_status in self.VALID_STATUSES:
            status = raw_status
        elif 'non member' in raw_status.lower():
            status = 'Non Member Resource'
        elif 'pending' in raw_status.lower():
            status = 'Pending'
        else:
            status = 'Member'  # Default to Member for invalid values

        # Build notes from extra info
        notes_parts = []
        if row.get('best_way_to_contact'):
            notes_parts.append(f"Best contact: {row['best_way_to_contact']}")
        if row.get('calendar_link'):
            notes_parts.append(f"Calendar: {row['calendar_link']}")
        if row.get('url'):
            notes_parts.append(f"JV Directory: {row['url']}")
        notes = '\n'.join(notes_parts) if notes_parts else None

        # Check if profile exists
        existing_profile = existing.get(name_lower)

        if existing_profile:
            # Update existing - only update fields that are empty or have new data
            changed = False

            if email and not existing_profile.email:
                existing_profile.email = email
                changed = True

            if row.get('phone') and not existing_profile.phone:
                existing_profile.phone = row['phone'].strip()
                changed = True

            if website and not existing_profile.website:
                existing_profile.website = website
                changed = True

            if business_focus and not existing_profile.business_focus:
                existing_profile.business_focus = business_focus
                changed = True

            if list_size > 0 and (not existing_profile.list_size or existing_profile.list_size == 0):
                existing_profile.list_size = list_size
                changed = True

            if social_reach > 0 and (not existing_profile.social_reach or existing_profile.social_reach == 0):
                existing_profile.social_reach = social_reach
                changed = True

            if notes and not existing_profile.notes:
                existing_profile.notes = notes
                changed = True

            if changed:
                if not dry_run:
                    existing_profile.save()
                return 'updated'
            return 'skipped'
        else:
            # Create new profile
            if not dry_run:
                new_profile = SupabaseProfile.objects.create(
                    name=name,
                    email=email or None,
                    phone=row.get('phone', '').strip() or None,
                    company=row.get('company', '').strip() or None,
                    website=website or None,
                    business_focus=business_focus or None,
                    status=status,
                    list_size=list_size,
                    social_reach=social_reach,
                    notes=notes,
                )
                # Add to existing dict for dedup within batch
                existing[name_lower] = new_profile

            return 'created'

    def _parse_number(self, value: str) -> int:
        """Parse numbers like '10,000', '10K', '1M', etc."""
        if not value:
            return 0

        value = value.strip().upper().replace(',', '')

        # Handle K/M suffixes
        multiplier = 1
        if value.endswith('K'):
            multiplier = 1000
            value = value[:-1]
        elif value.endswith('M'):
            multiplier = 1000000
            value = value[:-1]

        try:
            return int(float(value) * multiplier)
        except (ValueError, TypeError):
            return 0

    def _extract_email(self, text: str) -> str:
        """Extract email from text that may contain other info."""
        if not text:
            return ''

        # Find email pattern
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        match = re.search(email_pattern, text)
        if match:
            return match.group(0).lower()
        return ''

    def _extract_url(self, text: str) -> str:
        """Extract URL from text that may contain other info."""
        if not text:
            return ''

        # Find URL pattern
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        match = re.search(url_pattern, text)
        if match:
            return match.group(0)

        # Check if it looks like a domain without protocol
        if '.' in text and ' ' not in text[:50]:
            domain = text.split()[0] if ' ' in text else text
            if re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', domain):
                return f'https://{domain}'

        return ''
