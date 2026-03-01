"""Flag profiles with low confidence or stale data for priority re-enrichment.

Usage:
    python manage.py flag_low_confidence
    python manage.py flag_low_confidence --dry-run
    python manage.py flag_low_confidence --threshold 0.3 --stale-days 14
"""

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from matching.models import SupabaseProfile


class Command(BaseCommand):
    help = 'Flag profiles with low confidence or stale data for priority re-enrichment'

    def add_arguments(self, parser):
        parser.add_argument(
            '--threshold', type=float, default=0.5,
            help='Confidence threshold below which profiles need re-enrichment (default: 0.5)',
        )
        parser.add_argument(
            '--stale-days', type=int, default=29,
            help='Days after which enrichment data is considered stale (default: 29)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Report flagged profiles without taking action',
        )

    def handle(self, *args, **options):
        threshold = options['threshold']
        stale_days = options['stale_days']
        dry_run = options['dry_run']

        cutoff = timezone.now() - timedelta(days=stale_days)

        # Two independent triggers, combined with OR
        low_confidence = Q(profile_confidence__lt=threshold, profile_confidence__isnull=False)
        stale_data = Q(last_enriched_at__lt=cutoff, last_enriched_at__isnull=False)
        never_enriched = Q(last_enriched_at__isnull=True)

        flagged = SupabaseProfile.objects.filter(
            low_confidence | stale_data | never_enriched
        ).exclude(
            status='Inactive'
        ).order_by('profile_confidence')

        total = flagged.count()

        # Breakdown counts for reporting
        low_conf_count = SupabaseProfile.objects.filter(low_confidence).exclude(status='Inactive').count()
        stale_count = SupabaseProfile.objects.filter(stale_data).exclude(status='Inactive').count()
        never_count = SupabaseProfile.objects.filter(never_enriched).exclude(status='Inactive').count()

        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(f'RE-ENRICHMENT FLAG REPORT')
        self.stdout.write(f'{"=" * 60}')
        self.stdout.write(f'Threshold: confidence < {threshold}')
        self.stdout.write(f'Stale cutoff: > {stale_days} days since enrichment')
        self.stdout.write(f'{"─" * 60}')
        self.stdout.write(f'Low confidence (< {threshold}):  {low_conf_count}')
        self.stdout.write(f'Stale data (> {stale_days}d):        {stale_count}')
        self.stdout.write(f'Never enriched:                {never_count}')
        self.stdout.write(f'Total flagged (deduplicated):  {total}')
        self.stdout.write(f'{"─" * 60}')

        # Collect all flagged profile IDs (sorted by confidence, lowest first)
        ids = list(flagged.values_list('id', flat=True))

        if dry_run:
            self.stdout.write('\n[DRY RUN] Top flagged profiles:\n')
            for p in flagged[:20]:
                conf = f'{p.profile_confidence:.2f}' if p.profile_confidence is not None else 'None'
                enriched = p.last_enriched_at.strftime('%Y-%m-%d') if p.last_enriched_at else 'Never'
                self.stdout.write(
                    f'  {p.name[:30]:<30} conf={conf:<6} enriched={enriched} status={p.status}'
                )
            if total > 20:
                self.stdout.write(f'  ... and {total - 20} more')
            self.stdout.write(f'\n[DRY RUN] {len(ids)} profiles would be flagged for re-enrichment.')
        else:
            self.stdout.write(f'\nFlagged {len(ids)} profile IDs for re-enrichment.')

        # Always return IDs so the command can be called programmatically
        return ids
