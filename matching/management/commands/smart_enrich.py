"""
Smart Enrichment Command - Optimized for Minimal API Calls

Usage:
    python manage.py smart_enrich --input contacts_enriched.csv --tier1 "Joan Ranquet,Keren Killgore"

This uses a progressive enrichment strategy:
1. FREE: Website scraping for all contacts
2. PAID: Targeted searches for medium-priority contacts
3. PAID: Full OWL for high-priority contacts only

Result: 75% reduction in API calls vs traditional OWL
"""

import csv
from django.core.management.base import BaseCommand
from matching.enrichment.smart_enrichment_service import smart_enrich_batch_sync


class Command(BaseCommand):
    help = 'Smart enrichment with minimal API calls'

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
            default='contacts_smart_enriched.csv',
            help='Output CSV file'
        )
        parser.add_argument(
            '--tier1',
            type=str,
            default='',
            help='Comma-separated high-priority names (full OWL)'
        )
        parser.add_argument(
            '--tier2',
            type=str,
            default='',
            help='Comma-separated medium-priority names (targeted search)'
        )
        parser.add_argument(
            '--enable-owl',
            action='store_true',
            default=False,
            help='Enable paid OWL searches (default: website scraping only)'
        )
        parser.add_argument(
            '--max-contacts',
            type=int,
            default=None,
            help='Max contacts to process (for testing)'
        )
        parser.add_argument(
            '--filter-unmatched',
            action='store_true',
            default=False,
            help='Only process contacts with Match Status = Not Matched'
        )

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']
        enable_owl = options['enable_owl']
        max_contacts = options['max_contacts']
        filter_unmatched = options['filter_unmatched']

        # Parse priority tiers
        tier1 = [n.strip() for n in options['tier1'].split(',') if n.strip()]
        tier2 = [n.strip() for n in options['tier2'].split(',') if n.strip()]

        # Load contacts
        self.stdout.write(f'Reading contacts from: {input_file}')
        contacts = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filter if requested
                if filter_unmatched and row.get('Match Status') != 'Not Matched':
                    continue
                contacts.append(dict(row))

        self.stdout.write(f'Loaded {len(contacts)} contacts')

        # Show strategy
        self.stdout.write('\n=== ENRICHMENT STRATEGY ===')
        self.stdout.write(f'High Priority (full OWL): {len(tier1)} contacts')
        if tier1:
            for name in tier1[:5]:
                self.stdout.write(f'  - {name}')
            if len(tier1) > 5:
                self.stdout.write(f'  ... and {len(tier1) - 5} more')

        self.stdout.write(f'Medium Priority (targeted search): {len(tier2)} contacts')
        if tier2:
            for name in tier2[:5]:
                self.stdout.write(f'  - {name}')
            if len(tier2) > 5:
                self.stdout.write(f'  ... and {len(tier2) - 5} more')

        low_priority = len(contacts) - len(tier1) - len(tier2)
        self.stdout.write(f'Low Priority (website scraping only): {low_priority} contacts')

        if not enable_owl:
            self.stdout.write(
                self.style.WARNING('\n⚠️  OWL searches DISABLED - will only use FREE website scraping')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS('\n✓ OWL searches ENABLED for medium/high priority contacts')
            )

        # Run smart enrichment
        self.stdout.write('\n=== STARTING ENRICHMENT ===\n')

        enriched_contacts, stats = smart_enrich_batch_sync(
            contacts=contacts,
            priority_tier_1=tier1,
            priority_tier_2=tier2,
            enable_owl=enable_owl,
            max_contacts=max_contacts,
        )

        # Save results
        self.stdout.write(f'\nSaving enriched data to: {output_file}')

        if enriched_contacts:
            # Get fieldnames (exclude metadata for CSV)
            fieldnames = [k for k in enriched_contacts[0].keys() if k != '_metadata']

            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(enriched_contacts)

        # Print stats
        stats_report = f"""
=== SMART ENRICHMENT STATISTICS ===

Profiles Processed: {stats.profiles_processed}

FREE Methods Used:
  - Website scrapes: {stats.website_scrapes}
  - LinkedIn extractions: {stats.linkedin_scrapes}

PAID Methods Used:
  - Targeted searches: {stats.targeted_searches} (1-2 API calls each)
  - Full OWL searches: {stats.full_owl_searches} (4 API calls each)

Efficiency:
  - API calls saved: {stats.api_calls_saved}
  - Estimated cost: ${stats.get_estimated_cost():.3f}
  - Money saved: ${stats.get_savings():.3f}
"""
        self.stdout.write(self.style.SUCCESS(stats_report))

        # Recommendations
        if stats.full_owl_searches > 0:
            self.stdout.write('\n=== RECOMMENDATIONS ===')
            self.stdout.write(
                f'You used {stats.full_owl_searches} full OWL searches.\n'
                f'Consider moving some contacts to Tier 2 (targeted) to save costs.'
            )

        if not enable_owl and low_priority > 0:
            self.stdout.write(
                f'\n💡 TIP: {low_priority} contacts enriched with FREE methods only.\n'
                f'   If quality is low, enable --enable-owl and add high-value contacts to --tier1'
            )
