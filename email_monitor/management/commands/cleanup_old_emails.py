"""
Management command: cleanup_old_emails

Purges raw body_text/body_html from InboundEmail records older than 90 days.
All JV-relevant extracted data (analysis JSON, links_extracted, EmailActivitySummary,
promotion_network) is retained permanently. Only raw HTML/text is cleared.

Usage:
    python3 manage.py cleanup_old_emails
    python3 manage.py cleanup_old_emails --dry-run
"""

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Purge raw email bodies older than 90 days (analysis data retained)'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='Count records without purging')
        parser.add_argument('--days', type=int, default=90,
                            help='Retention period in days (default: 90)')

    def handle(self, *args, **options):
        from email_monitor.models import InboundEmail

        dry_run = options['dry_run']
        days = options['days']
        cutoff = timezone.now() - timedelta(days=days)

        stale = InboundEmail.objects.filter(
            received_at__lt=cutoff,
        ).exclude(body_text='', body_html='')

        count = stale.count()
        self.stdout.write(f'Found {count} emails with body content older than {days} days')

        if dry_run:
            self.stdout.write(self.style.WARNING('  DRY RUN — no changes made'))
            return

        updated = stale.update(body_text='', body_html='')
        self.stdout.write(self.style.SUCCESS(f'  Purged bodies from {updated} emails'))
