"""
Management command: discover_newsletters

Discovers newsletter signup forms on profile websites and optionally subscribes.
Processes profiles in descending jv_readiness_score order (highest value first).

Failed discoveries are logged as MonitoredSubscription(status='failed') with
the error code in signup_url so they can be retried later with a different approach.

Usage:
    python3 manage.py discover_newsletters --limit 200
    python3 manage.py discover_newsletters --limit 200 --subscribe
    python3 manage.py discover_newsletters --tier B --limit 50
    python3 manage.py discover_newsletters --retry-failed http_403 --limit 50
    python3 manage.py discover_newsletters --summary
"""

import logging
import time
import random
import uuid

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection

logger = logging.getLogger(__name__)

SUBSCRIBE_RATE_LIMIT = 20  # per hour
DISCOVERY_METHOD = 'requests_form'  # updated from crawl4ai_form


class Command(BaseCommand):
    help = 'Discover newsletter signup forms on profile websites'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=200,
                            help='Max profiles to process (default: 200)')
        parser.add_argument('--offset', type=int, default=0,
                            help='Skip first N candidates (for parallel batching, default: 0)')
        parser.add_argument('--subscribe', action='store_true',
                            help='Subscribe after discovering (default: discover only)')
        parser.add_argument('--tier', type=str, default='',
                            help='Filter by JV tier (A/B/C/D/E, default: all)')
        parser.add_argument('--retry-failed', type=str, default='',
                            metavar='ERROR_CODE',
                            help='Retry previously failed discoveries by error code '
                                 '(e.g. http_403, no_form, timeout, js_required)')
        parser.add_argument('--summary', action='store_true',
                            help='Show missed discovery summary by error code and exit')

    def handle(self, *args, **options):
        if options['summary']:
            self._print_summary()
            return

        limit = options['limit']
        offset = options['offset']
        subscribe = options['subscribe']
        tier_filter = options['tier'].upper() if options['tier'] else ''
        retry_code = options['retry_failed']

        if retry_code:
            profiles = self._fetch_retry_candidates(retry_code, limit, tier_filter)
            self.stdout.write(
                f'Retrying {len(profiles)} profiles with error={retry_code}'
            )
        else:
            profiles = self._fetch_candidates(limit, tier_filter, offset)
            self.stdout.write(f'Found {len(profiles)} profiles to process (offset={offset})')

        discovered = 0
        subscribed = 0
        failed_counts: dict[str, int] = {}
        hourly_sub_count = 0
        hour_start = time.time()

        from email_monitor.services.newsletter_discoverer import discover_newsletter
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Phase 1: parallel discovery (HTTP fetches — safe to parallelize)
        self.stdout.write(f'Discovering in parallel (20 workers)...')
        discovery_results = {}
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {
                pool.submit(discover_newsletter, str(p['id']), p['website']): p
                for p in profiles
            }
            for future in as_completed(futures):
                p = futures[future]
                try:
                    discovery_results[str(p['id'])] = (p, future.result())
                except Exception as exc:
                    logger.warning('Discovery thread error for %s: %s', p['name'], exc)

        # Phase 2: process results + subscribe (sequential, rate-limited)
        for profile_id, (profile, result) in discovery_results.items():
            name = profile.get('name', '')
            website = profile.get('website', '')

            if not result.success:
                code = result.error or 'unknown'
                failed_counts[code] = failed_counts.get(code, 0) + 1
                self.stdout.write(f'  {name[:35]:<35} ' + self.style.WARNING(f'✗ [{code}]'))
                self._record_failure(profile_id, website, code, retry_code)
                continue

            discovered += 1
            self.stdout.write(
                f'  {name[:35]:<35} ' +
                self.style.SUCCESS(f'✓ [{result.esp_detected or "form"}]')
            )

            if subscribe:
                now = time.time()
                if now - hour_start >= 3600:
                    hourly_sub_count = 0
                    hour_start = now
                if hourly_sub_count >= SUBSCRIBE_RATE_LIMIT:
                    self.stdout.write('  Rate limit reached (20/hr) — stopping subscriptions')
                    break

                ok = self._subscribe(profile_id, name, result, retry_code)
                if ok:
                    subscribed += 1
                    hourly_sub_count += 1
                    time.sleep(random.uniform(30, 90))
            else:
                self._record_discovery(profile_id, result, retry_code)

        total_failed = sum(failed_counts.values())
        self.stdout.write(self.style.SUCCESS(
            f'\nDiscovered: {discovered}  Subscribed: {subscribed}  Failed: {total_failed}'
        ))
        if failed_counts:
            self.stdout.write('Failed breakdown (retry with --retry-failed <code>):')
            for code, count in sorted(failed_counts.items(), key=lambda x: -x[1]):
                self.stdout.write(f'  {code:15s} {count:4d}')

    def _print_summary(self):
        """Show all failed discoveries grouped by error code."""
        from email_monitor.models import MonitoredSubscription
        from django.db.models import Count

        from email_monitor.constants import DISCOVERY_ERROR_CODES as ERROR_CODES
        rows = (
            MonitoredSubscription.objects
            .filter(status='failed', signup_url__in=ERROR_CODES)
            .values('signup_url')  # signup_url stores error code on failures
            .annotate(count=Count('id'))
            .order_by('-count')
        )
        self.stdout.write('\nMissed discovery summary (by error code):')
        self.stdout.write(f'{"Error code":<20} {"Count":>6}  Retry strategy')
        self.stdout.write('-' * 60)
        retry_hints = {
            'http_403':     'Crawl4AI with stealth headers',
            'http_404':     'Check/fix profile URL',
            'http_other':   'Investigate individually',
            'timeout':      'Crawl4AI or longer timeout',
            'js_required':  'Crawl4AI headless',
            'no_form':      'Manual subscribe or headless',
            'captcha':      'Headless browser with delay',
            'error':        'Investigate individually',
            'bad_discovery': 'Re-run discovery (junk URL detected)',
        }
        total = 0
        for row in rows:
            code = row['signup_url'] or 'unknown'
            count = row['count']
            total += count
            hint = retry_hints.get(code, 'Unknown')
            self.stdout.write(f'{code:<20} {count:>6}  {hint}')
        self.stdout.write(f'\nTotal logged failures: {total:,}')

    # Directory/social domains that are NOT personal websites
    DIRECTORY_DOMAINS = (
        'sessionize.com', 'speakerhub.com', 'psychologytoday.com',
        'coachingfederation.org', 'speaking.com', 'ted.com',
        'linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com',
        'youtube.com', 'tiktok.com', 'pinterest.com',
        'yelp.com', 'bbb.org', 'trustpilot.com',
        'clutch.co', 'g2.com', 'capterra.com',
        'icf.org', 'therapists.psychologytoday.com',
        # Podcast feed/hosting platforms (not personal websites)
        'podcasts.apple.com', 'feeds.captivate.fm', 'feeds.simplecast.com',
        'feeds.acast.com', 'api.riverside.fm', 'feeds.buzzsprout.com',
        'anchor.fm', 'podcasters.spotify.com', 'feed.pod.co',
        'redcircle.com', 'podomatic.com', 'podbean.com', 'buzzsprout.com',
        'feeds.libsyn.com', 'libsyn.com', 'rss.com/podcasts',
    )

    def _fetch_candidates(self, limit: int, tier: str, offset: int = 0) -> list[dict]:
        """Profiles with personal websites and no subscription attempt yet.
        Excludes directory/social domains that are not personal websites."""
        from email_monitor.models import MonitoredSubscription
        tier_clause = f"AND p.jv_tier = %s" if tier else ''
        # Build exclusion clause for known directory domains
        dir_exclusions = ' AND '.join(
            f"p.website NOT ILIKE '%%{d}%%'" for d in self.DIRECTORY_DOMAINS
        )
        params = [tier, limit, offset] if tier else [limit, offset]
        sql = f"""
            SELECT p.id, p.name, p.website, p.jv_tier, p.jv_readiness_score
            FROM profiles p
            LEFT JOIN {MonitoredSubscription._meta.db_table} ms ON ms.profile_id = p.id
            WHERE p.website IS NOT NULL AND p.website != ''
              AND ms.id IS NULL
              AND {dir_exclusions}
              {tier_clause}
            ORDER BY p.jv_readiness_score DESC NULLS LAST
            LIMIT %s OFFSET %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def _fetch_retry_candidates(self, error_code: str, limit: int, tier: str) -> list[dict]:
        """Previously failed discoveries with a specific error code."""
        from email_monitor.models import MonitoredSubscription
        tier_clause = "AND p.jv_tier = %s" if tier else ''
        params = [error_code]
        if tier:
            params.append(tier)
        params.append(limit)
        sql = f"""
            SELECT p.id, p.name, p.website, p.jv_tier, p.jv_readiness_score
            FROM profiles p
            INNER JOIN {MonitoredSubscription._meta.db_table} ms ON ms.profile_id = p.id
            WHERE ms.status = 'failed'
              AND ms.signup_url = %s
              AND p.website IS NOT NULL AND p.website != ''
              {tier_clause}
            ORDER BY p.jv_readiness_score DESC NULLS LAST
            LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def _make_monitor_address(self) -> str:
        prefix = str(uuid.uuid4())[:8]
        base = settings.GMAIL_MONITOR_ADDRESS or 'mail@jvmatches.com'
        local, domain = base.split('@', 1)
        return f'{local}+{prefix}@{domain}'

    def _subscribe(self, profile_id: str, name: str, result, retry_code: str) -> bool:
        from email_monitor.models import MonitoredSubscription
        from email_monitor.services.subscription_manager import subscribe_and_confirm

        monitor_address = self._make_monitor_address()

        sub_result = subscribe_and_confirm(
            profile_id=profile_id,
            monitor_address=monitor_address,
            signup_url=result.signup_url,
            form_action=result.form_action,
            esp_detected=result.esp_detected,
            profile_name=name,
        )

        # On retry, delete the old failed record first
        if retry_code:
            MonitoredSubscription.objects.filter(
                profile_id=profile_id, status='failed', signup_url=retry_code
            ).delete()

        MonitoredSubscription.objects.create(
            profile_id=profile_id,
            monitor_address=monitor_address,
            signup_url=result.signup_url,
            esp_detected=result.esp_detected,
            discovery_method=DISCOVERY_METHOD,
            status=sub_result.status,
        )
        self.stdout.write(f'    → {monitor_address} [{sub_result.status}]')
        return sub_result.status in ('active', 'pending_confirm')

    def _record_discovery(self, profile_id: str, result, retry_code: str) -> None:
        """Record discovered newsletter (pending subscription)."""
        from email_monitor.models import MonitoredSubscription

        if retry_code:
            MonitoredSubscription.objects.filter(
                profile_id=profile_id, status='failed', signup_url=retry_code
            ).delete()

        MonitoredSubscription.objects.create(
            profile_id=profile_id,
            monitor_address=self._make_monitor_address(),
            signup_url=result.signup_url,
            form_action=result.form_action or '',
            esp_detected=result.esp_detected,
            discovery_method=DISCOVERY_METHOD,
            status='pending',
        )

    def _record_failure(self, profile_id: str, website: str, error_code: str, retry_code: str) -> None:
        """Log a failed discovery so it can be retried later by error code."""
        from email_monitor.models import MonitoredSubscription

        # On retry, update the existing failed record rather than duplicating
        if retry_code:
            updated = MonitoredSubscription.objects.filter(
                profile_id=profile_id, status='failed', signup_url=retry_code
            ).update(signup_url=error_code)
            if updated:
                return

        # Skip if already logged with this error code
        if MonitoredSubscription.objects.filter(
            profile_id=profile_id, status='failed', signup_url=error_code
        ).exists():
            return

        MonitoredSubscription.objects.create(
            profile_id=profile_id,
            monitor_address=self._make_monitor_address(),
            signup_url=error_code,  # store error code here for queryability
            esp_detected='',
            discovery_method=DISCOVERY_METHOD,
            status='failed',
        )
