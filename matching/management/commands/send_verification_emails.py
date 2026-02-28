"""Send client verification emails (Week 3 Mon/Wed/Fri)."""
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Send client verification emails for the current day of week."

    def add_arguments(self, parser):
        parser.add_argument(
            '--day', type=str, default='',
            help='Override day of week (monday, wednesday, friday). Default: auto-detect.',
        )
        parser.add_argument('--dry-run', action='store_true', help='Skip actual email sending.')

    def handle(self, *args, **options):
        if not os.environ.get('DATABASE_URL'):
            self.stderr.write("ERROR: DATABASE_URL not set")
            return

        from matching.enrichment.flows.client_verification import client_verification_flow

        day = options['day'] or None
        kwargs = {'dry_run': options['dry_run']}
        if day:
            kwargs['day_of_week'] = day.lower()

        result = client_verification_flow(**kwargs)
        self.stdout.write(self.style.SUCCESS(f"Verification emails: {result}"))
