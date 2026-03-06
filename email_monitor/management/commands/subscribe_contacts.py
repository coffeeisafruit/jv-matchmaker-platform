"""
Management command: subscribe_contacts

Subscribes to pending MonitoredSubscription records (status='pending').
Separate from discover_newsletters — allows discovering and subscribing as separate steps.

Usage:
    python3 manage.py subscribe_contacts --limit 50
    python3 manage.py subscribe_contacts --status pending
"""

import time
import random
import logging

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Subscribe to pending discovered newsletter forms'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=50,
                            help='Max subscriptions to attempt (default: 50)')
        parser.add_argument('--status', type=str, default='pending',
                            choices=['pending', 'failed'],
                            help='Which subscriptions to retry (default: pending)')

    def handle(self, *args, **options):
        from email_monitor.models import MonitoredSubscription
        from email_monitor.services.subscription_manager import subscribe_and_confirm

        limit = options['limit']
        status = options['status']

        subs = MonitoredSubscription.objects.filter(
            status=status
        ).select_related('profile')[:limit]

        total = subs.count()
        if total == 0:
            self.stdout.write(f'No {status} subscriptions found.')
            return

        self.stdout.write(f'Attempting {total} {status} subscriptions...')
        success = 0
        failed = 0

        for sub in subs:
            if not sub.signup_url:
                sub.status = 'failed'
                sub.save(update_fields=['status'])
                failed += 1
                continue

            result = subscribe_and_confirm(
                profile_id=str(sub.profile_id),
                monitor_address=sub.monitor_address,
                signup_url=sub.signup_url,
                form_action='',
                esp_detected=sub.esp_detected,
                profile_name=sub.profile.name,
            )

            sub.status = result.status
            if result.esp and not sub.esp_detected:
                sub.esp_detected = result.esp
            sub.save(update_fields=['status', 'esp_detected'])

            if result.status in ('active', 'pending_confirm'):
                success += 1
                self.stdout.write(f'  {sub.profile.name}: {result.status}')
            else:
                failed += 1
                self.stdout.write(
                    self.style.WARNING(f'  {sub.profile.name}: failed — {result.reason}')
                )

            # Randomized delay to avoid spam patterns
            time.sleep(random.uniform(30, 90))

        self.stdout.write(self.style.SUCCESS(
            f'\nSuccess: {success}, Failed: {failed}'
        ))
