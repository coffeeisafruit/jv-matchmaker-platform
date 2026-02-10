"""
Simple Enrichment - Uses OpenRouter API for website analysis
No OWL Agent SDK - just direct Claude API calls via OpenRouter
"""

import csv
import asyncio
import logging
from django.core.management.base import BaseCommand
from matching.enrichment.ai_research import ProfileResearchService, ProfileResearchCache

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Simple enrichment using OpenRouter API for website analysis'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            required=True,
            help='Input CSV file'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='contacts_openrouter_enriched.csv',
            help='Output CSV file'
        )
        parser.add_argument(
            '--filter-unmatched',
            action='store_true',
            help='Only process unmatched contacts'
        )
        parser.add_argument(
            '--max-contacts',
            type=int,
            default=None,
            help='Max contacts to process'
        )

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']
        filter_unmatched = options['filter_unmatched']
        max_contacts = options['max_contacts']

        # Load contacts
        self.stdout.write(f'Reading contacts from: {input_file}')
        contacts = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if filter_unmatched and row.get('Match Status') != 'Not Matched':
                    continue
                contacts.append(dict(row))

        if max_contacts:
            contacts = contacts[:max_contacts]

        self.stdout.write(f'Loaded {len(contacts)} contacts\n')

        # Initialize service
        service = ProfileResearchService()
        cache = ProfileResearchCache()

        if not service.api_key:
            self.stdout.write(
                self.style.ERROR('ERROR: No API key found!')
            )
            self.stdout.write('Please set OPENROUTER_API_KEY or ANTHROPIC_API_KEY in your environment')
            return

        self.stdout.write(
            self.style.SUCCESS(f'✓ Using {"OpenRouter" if service.use_openrouter else "Anthropic"} API')
        )

        # Enrich contacts
        enriched_contacts = []
        success_count = 0
        failed_count = 0
        skipped_count = 0

        for i, contact in enumerate(contacts, 1):
            name = contact.get('Name') or contact.get('name', '')
            website = contact.get('Website') or contact.get('website', '')

            self.stdout.write(f'\n[{i}/{len(contacts)}] {name}')

            # Skip if no website
            if not website:
                self.stdout.write('  ⊘ No website - skipping')
                skipped_count += 1
                enriched_contacts.append(contact)
                continue

            # Skip if already has good data
            existing_seeking = contact.get('seeking', '').strip()
            existing_who = contact.get('who_you_serve', '').strip()
            existing_what = contact.get('what_you_do', '').strip()

            if len(existing_seeking) > 20 and len(existing_who) > 20:
                self.stdout.write('  ✓ Already has sufficient data - skipping')
                skipped_count += 1
                enriched_contacts.append(contact)
                continue

            # Check cache first
            cached = cache.get(name)
            if cached:
                self.stdout.write('  ✓ Using cached data')
                merged = {**contact, **cached}
                enriched_contacts.append(merged)
                success_count += 1
                continue

            # Research profile
            try:
                self.stdout.write(f'  Researching website: {website}...')
                researched = service.research_profile(
                    name=name,
                    website=website,
                    existing_data=contact
                )

                if researched:
                    # Merge with original contact
                    merged = {**contact, **researched}
                    enriched_contacts.append(merged)

                    # Cache the result
                    cache.set(name, researched)

                    # Show what was found
                    if researched.get('what_you_do'):
                        what = researched['what_you_do']
                        self.stdout.write(f'  ✓ What they do: {what[:70]}...' if len(what) > 70 else f'  ✓ What they do: {what}')
                    if researched.get('who_you_serve'):
                        who = researched['who_you_serve']
                        self.stdout.write(f'  ✓ Who they serve: {who[:70]}...' if len(who) > 70 else f'  ✓ Who they serve: {who}')
                    if researched.get('seeking'):
                        seek = researched['seeking']
                        self.stdout.write(f'  ✓ Seeking: {seek[:70]}...' if len(seek) > 70 else f'  ✓ Seeking: {seek}')

                    success_count += 1
                else:
                    self.stdout.write('  ⚠ No data extracted')
                    enriched_contacts.append(contact)
                    failed_count += 1

            except Exception as e:
                self.stdout.write(f'  ✗ Error: {str(e)[:60]}')
                enriched_contacts.append(contact)
                failed_count += 1

            # Rate limiting
            if i < len(contacts):
                asyncio.run(asyncio.sleep(2.0))

        # Save results
        self.stdout.write(f'\n\nSaving enriched data to: {output_file}')

        if enriched_contacts:
            fieldnames = list(enriched_contacts[0].keys())

            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(enriched_contacts)

        # Print summary
        summary = f"""
=== ENRICHMENT SUMMARY ===

Total Contacts: {len(contacts)}
  ✓ Successfully enriched: {success_count}
  ⊘ Skipped (no website or already complete): {skipped_count}
  ✗ Failed: {failed_count}

Output: {output_file}
"""
        self.stdout.write(self.style.SUCCESS(summary))

        # Show next steps
        if failed_count > 0:
            self.stdout.write('\n💡 TIP: Failed contacts may have:')
            self.stdout.write('  - Websites that are down or blocking scrapers')
            self.stdout.write('  - Very sparse website content')
            self.stdout.write('  - Non-standard website structures')
