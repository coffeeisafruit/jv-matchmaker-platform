"""
Management command: poll_gmail_inbox

Polls the monitor Gmail inbox for unread newsletter emails.
Creates InboundEmail records, auto-confirms double opt-in subscriptions,
and triggers AI analysis on newly received emails.

Usage:
    python3 manage.py poll_gmail_inbox
    python3 manage.py poll_gmail_inbox --daemon     # Loop every 15 min
    python3 manage.py poll_gmail_inbox --analyze    # Run AI analysis after polling
    python3 manage.py poll_gmail_inbox --max-results 200
"""

import time
import logging

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Poll Gmail monitor inbox for newsletter emails'

    def add_arguments(self, parser):
        parser.add_argument('--daemon', action='store_true',
                            help='Run continuously every 15 minutes')
        parser.add_argument('--analyze', action='store_true',
                            help='Run AI analysis on newly received emails')
        parser.add_argument('--max-results', type=int, default=100,
                            help='Max messages to fetch per run (default: 100)')
        parser.add_argument('--analyze-batch', type=int, default=50,
                            help='Max emails to analyze per run (default: 50)')

    def handle(self, *args, **options):
        daemon = options['daemon']
        run_analysis = options['analyze']
        max_results = options['max_results']
        analyze_batch = options['analyze_batch']

        if daemon:
            self.stdout.write('Starting Gmail monitor daemon (polling every 15 min)...')
            while True:
                self._run_once(run_analysis, max_results, analyze_batch)
                self.stdout.write('  Sleeping 15 min...')
                time.sleep(900)
        else:
            self._run_once(run_analysis, max_results, analyze_batch)

    def _run_once(self, run_analysis: bool, max_results: int, analyze_batch: int):
        from email_monitor.services.gmail_poller import poll_inbox

        self.stdout.write('Polling Gmail monitor inbox...')
        stats = poll_inbox(max_results=max_results)
        self.stdout.write(
            self.style.SUCCESS(
                f'  Processed: {stats["processed"]}, '
                f'Confirmed: {stats["confirmed"]}, '
                f'Skipped: {stats["skipped"]}, '
                f'Errors: {stats["errors"]}'
            )
        )

        if run_analysis and stats['processed'] > 0:
            self._run_analysis(analyze_batch)

    def _run_analysis(self, batch_size: int):
        from email_monitor.models import InboundEmail
        from email_monitor.services.email_analyzer import batch_analyze_emails

        pending_ids = list(
            InboundEmail.objects.filter(analyzed_at__isnull=True)
            .values_list('id', flat=True)[:batch_size]
        )
        if not pending_ids:
            return

        self.stdout.write(f'  Analyzing {len(pending_ids)} emails with Haiku...')
        results = batch_analyze_emails(pending_ids)
        success = sum(1 for v in results.values() if v is not None)
        self.stdout.write(f'  Analyzed: {success}/{len(pending_ids)} successful')
