"""
Management command: subscribe_contacts

Subscribes to pending MonitoredSubscription records (status='pending').
Uses parallel HTTP for both re-discovery and subscription POSTs.

Usage:
    python3 manage.py subscribe_contacts --limit 50
    python3 manage.py subscribe_contacts --limit 500 --workers 20
"""

import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

from email_monitor.constants import DISCOVERY_ERROR_CODES as ERROR_CODES


class Command(BaseCommand):
    help = 'Subscribe to pending discovered newsletter forms'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=50,
                            help='Max subscriptions to attempt (default: 50)')
        parser.add_argument('--status', type=str, default='pending',
                            choices=['pending', 'failed'],
                            help='Which subscriptions to retry (default: pending)')
        parser.add_argument('--workers', type=int, default=15,
                            help='Parallel HTTP workers (default: 15)')
        parser.add_argument('--batch-size', type=int, default=50,
                            help='Rows to claim per wave (default: 50)')

    def handle(self, *args, **options):
        from email_monitor.models import MonitoredSubscription
        from email_monitor.services.subscription_manager import subscribe_and_confirm
        from django.db import connection

        limit = options['limit']
        status = options['status']
        workers = options['workers']
        batch_size = options['batch_size']

        total_available = MonitoredSubscription.objects.filter(status=status).count()
        if total_available == 0:
            self.stdout.write(f'No {status} subscriptions found.')
            return

        self.stdout.write(f'{total_available} {status} available, processing up to {limit} '
                          f'({workers} workers, batch {batch_size})...')
        success = 0
        failed = 0
        processed = 0

        while processed < limit:
            # Claim a batch — two queries to avoid JOIN timeout on Supabase pgbouncer
            claim_size = min(batch_size, limit - processed)
            with connection.cursor() as cur:
                # Step 1: fast single-table scan
                # For failed retries: only pick rows with real URLs (not error codes)
                # and no failure_reason yet (haven't been tried with improved logic)
                if status == 'failed':
                    cur.execute("""
                        SELECT id, profile_id, monitor_address, signup_url,
                               form_action, esp_detected
                        FROM email_monitor_monitoredsubscription
                        WHERE status = 'failed'
                          AND signup_url LIKE 'http%%'
                          AND (failure_reason = '' OR failure_reason IS NULL)
                        ORDER BY id
                        LIMIT %s
                    """, [claim_size])
                else:
                    cur.execute("""
                        SELECT id, profile_id, monitor_address, signup_url,
                               form_action, esp_detected
                        FROM email_monitor_monitoredsubscription
                        WHERE status = %s
                        ORDER BY id
                        LIMIT %s
                    """, [status, claim_size])
                ms_rows = cur.fetchall()
            if not ms_rows:
                break

            # Step 2: fetch names separately (small IN query, fast)
            profile_ids = list({r[1] for r in ms_rows})
            names = {}
            with connection.cursor() as cur:
                cur.execute("SELECT id, name FROM profiles WHERE id = ANY(%s)", [profile_ids])
                names = {r[0]: r[1] or '' for r in cur.fetchall()}

            # Build lightweight sub objects
            subs = []
            sub_ids = []
            for row in ms_rows:
                sub_ids.append(row[0])
                subs.append({
                    'id': row[0], 'profile_id': row[1], 'monitor_address': row[2],
                    'signup_url': row[3], 'form_action': row[4] or '',
                    'esp_detected': row[5] or '', 'name': names.get(row[1], ''),
                })

            # Mark all claimed
            MonitoredSubscription.objects.filter(id__in=sub_ids, status=status).update(status='subscribing')

            # Filter out error-code entries
            valid_subs = []
            skip_ids = []
            for sub in subs:
                if not sub['signup_url'] or sub['signup_url'] in ERROR_CODES:
                    skip_ids.append(sub['id'])
                    failed += 1
                    processed += 1
                else:
                    valid_subs.append(sub)
            if skip_ids:
                MonitoredSubscription.objects.filter(id__in=skip_ids).update(status='failed')

            if not valid_subs:
                continue

            # Parallel subscribe: each worker handles one contact (different website each)
            def _do_subscribe(sub):
                try:
                    result = subscribe_and_confirm(
                        profile_id=str(sub['profile_id']),
                        monitor_address=sub['monitor_address'],
                        signup_url=sub['signup_url'],
                        form_action=sub['form_action'],
                        esp_detected=sub['esp_detected'],
                        profile_name=sub['name'],
                        confirm_timeout=0,
                    )
                    return sub, result
                except Exception as exc:
                    from email_monitor.services.subscription_manager import SubscriptionResult
                    return sub, SubscriptionResult(status='failed', reason=str(exc)[:200])

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_do_subscribe, s): s for s in valid_subs}
                for future in as_completed(futures):
                    sub, result = future.result()
                    update = {'status': result.status}
                    if result.esp and not sub['esp_detected']:
                        update['esp_detected'] = result.esp
                    if result.status == 'failed' and result.reason:
                        update['failure_reason'] = result.reason[:500]
                    MonitoredSubscription.objects.filter(id=sub['id']).update(**update)

                    processed += 1
                    name = sub['name'][:35]
                    if result.status in ('active', 'pending_confirm'):
                        success += 1
                        self.stdout.write(
                            f'  [{processed}] {name:<35} '
                            f'{self.style.SUCCESS(result.status)} ({result.esp or "form"})'
                        )
                    else:
                        failed += 1
                        self.stdout.write(
                            f'  [{processed}] {name:<35} '
                            + self.style.WARNING(f'failed — {result.reason[:60]}')
                        )

            # Brief pause between batches (not between individual subs)
            time.sleep(random.uniform(0.5, 1.0))

        self.stdout.write(self.style.SUCCESS(
            f'\nDone: {success} subscribed, {failed} failed, {processed} total'
        ))
