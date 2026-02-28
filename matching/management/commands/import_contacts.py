"""Import contacts from CSV and trigger enrichment + cross-client scoring."""
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Import contacts from a CSV file into the enrichment pipeline."

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, required=True, help='Path to CSV file.')
        parser.add_argument(
            '--source', type=str, default='csv_import',
            help='Ingestion source label (default: csv_import).',
        )
        parser.add_argument('--ingested-by', type=str, default='', help='Who is importing (email/name).')
        parser.add_argument('--skip-enrichment', action='store_true', help='Skip enrichment after ingestion.')
        parser.add_argument('--dry-run', action='store_true', help='Skip DB writes.')

    def handle(self, *args, **options):
        if not os.environ.get('DATABASE_URL'):
            self.stderr.write("ERROR: DATABASE_URL not set")
            return

        import csv
        from pathlib import Path

        filepath = Path(options['file'])
        if not filepath.exists():
            self.stderr.write(f"ERROR: File not found: {filepath}")
            return

        # Read CSV
        contacts = []
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                contact = {}
                for key, value in row.items():
                    clean_key = key.strip().lower().replace(' ', '_')
                    contact[clean_key] = (value or '').strip()
                contacts.append(contact)

        if not contacts:
            self.stderr.write("No contacts found in CSV")
            return

        self.stdout.write(f"Loaded {len(contacts)} contacts from {filepath.name}")

        from matching.enrichment.flows.new_contact_flow import new_contact_flow

        result = new_contact_flow(
            contacts=contacts,
            source=options['source'],
            ingested_by=options['ingested_by'],
            source_file=str(filepath),
            skip_enrichment=options['skip_enrichment'],
            dry_run=options['dry_run'],
        )
        self.stdout.write(self.style.SUCCESS(f"Contact import: {result}"))
