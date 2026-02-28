"""Run profile freshness change detection scan."""
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run content change detection scan on profiles (Layer 1 hash + Layer 2 triage)."

    def add_arguments(self, parser):
        parser.add_argument(
            '--tiers', type=str, default='A,B,C',
            help='Comma-separated tier list to check (default: A,B,C).',
        )
        parser.add_argument('--limit', type=int, default=0, help='Max profiles to check (0 = all due).')
        parser.add_argument('--skip-triage', action='store_true', help='Skip Layer 2 semantic triage.')
        parser.add_argument('--dry-run', action='store_true', help='Skip DB updates.')

    def handle(self, *args, **options):
        if not os.environ.get('DATABASE_URL'):
            self.stderr.write("ERROR: DATABASE_URL not set")
            return

        from matching.enrichment.flows.change_detection_flow import change_detection_flow

        result = change_detection_flow(
            tiers=options['tiers'],
            limit=options['limit'],
            skip_triage=options['skip_triage'],
            dry_run=options['dry_run'],
        )
        self.stdout.write(self.style.SUCCESS(f"Change detection: {result}"))
