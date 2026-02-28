"""Deliver monthly reports to all active clients (1st of month)."""
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Deliver updated monthly reports with new access codes to all active clients."

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Skip actual email sending.')

    def handle(self, *args, **options):
        if not os.environ.get('DATABASE_URL'):
            self.stderr.write("ERROR: DATABASE_URL not set")
            return

        from matching.enrichment.flows.report_delivery import report_delivery_flow

        result = report_delivery_flow(dry_run=options['dry_run'])
        self.stdout.write(self.style.SUCCESS(f"Report delivery: {result}"))
