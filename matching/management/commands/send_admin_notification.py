"""Send admin notification with AI suggestions (Week 4 Tue)."""
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Generate and send admin notification with monthly processing summary."

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Skip actual email sending.')

    def handle(self, *args, **options):
        if not os.environ.get('DATABASE_URL'):
            self.stderr.write("ERROR: DATABASE_URL not set")
            return

        from matching.enrichment.flows.admin_notification import admin_notification_flow

        result = admin_notification_flow(dry_run=options['dry_run'])
        self.stdout.write(self.style.SUCCESS(f"Admin notification: {result}"))
