"""
Django management command: trigger acquisition for a specific client.

Usage:
    python3 manage.py run_new_client_acquisition --client-id <uuid>
    python3 manage.py run_new_client_acquisition --client-id <uuid> --dry-run
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Trigger the acquisition pipeline for a specific client'

    def add_arguments(self, parser):
        parser.add_argument(
            '--client-id',
            type=str,
            required=True,
            help='UUID of the client profile',
        )
        parser.add_argument(
            '--target-score',
            type=int,
            default=64,
            help='Minimum harmonic_mean for quality match (default: 64)',
        )
        parser.add_argument(
            '--target-count',
            type=int,
            default=30,
            help='Target number of quality matches (default: 30)',
        )
        parser.add_argument(
            '--budget',
            type=float,
            default=2.00,
            help='Budget cap per client (default: 2.00)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview mode — no DB writes or API calls',
        )

    def handle(self, *args, **options):
        from matching.enrichment.flows.acquisition_flow import acquisition_flow

        client_id = options['client_id']

        self.stdout.write(self.style.SUCCESS(
            f'\nRunning acquisition for client: {client_id}\n'
            f'  Target score: {options["target_score"]}\n'
            f'  Target count: {options["target_count"]}\n'
            f'  Budget: ${options["budget"]:.2f}\n'
            f'  Dry run: {options["dry_run"]}\n'
        ))

        result = acquisition_flow(
            client_profile_id=client_id,
            target_score=options['target_score'],
            target_count=options['target_count'],
            budget=options['budget'],
            dry_run=options['dry_run'],
        )

        self.stdout.write(self.style.SUCCESS(
            f'\nAcquisition complete:\n'
            f'  Client: {result.client_name}\n'
            f'  Gap detected: {result.gap_detected}\n'
            f'  DB search: {result.db_search_count}\n'
            f'  Total discovered: {result.total_discovered}\n'
            f'  Above threshold: {result.above_threshold}\n'
            f'  Saved to DB: {result.saved_to_db}\n'
            f'  Duplicates: {result.duplicates}\n'
            f'  Enriched: {result.enriched}\n'
            f'  Cost: ${result.cost:.2f}\n'
            f'  Budget cap reached: {result.budget_cap_reached}\n'
            f'  Runtime: {result.runtime_seconds:.1f}s\n'
        ))
