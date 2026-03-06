"""
Score profiles enriched since the last scoring run against all active clients.

For each profile enriched within the time window, runs full ISMC scoring
against all active clients and creates match_suggestions rows. High-quality
matches (harmonic_mean >= threshold) trigger client report regeneration.

Usage:
    python manage.py score_new_enrichments
    python manage.py score_new_enrichments --since 2026-03-01
    python manage.py score_new_enrichments --dry-run
    python manage.py score_new_enrichments --threshold 64
    python manage.py score_new_enrichments --limit 500
    python manage.py score_new_enrichments --tier A
    python manage.py score_new_enrichments --tier A --tier B
"""

import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from django.core.management.base import BaseCommand

from matching.models import MemberReport


class Command(BaseCommand):
    help = 'Score recently enriched profiles against all active clients'

    def add_arguments(self, parser):
        parser.add_argument(
            '--since', type=str, default=None,
            help='ISO date/datetime — score profiles enriched since this date '
                 '(default: 7 days ago). Example: 2026-03-01',
        )
        parser.add_argument(
            '--threshold', type=int, default=64,
            help='Minimum harmonic_mean to qualify as high-quality (default: 64)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Report pair count without scoring',
        )
        parser.add_argument(
            '--limit', type=int, default=0,
            help='Max profiles to score (0 = all)',
        )
        parser.add_argument(
            '--tier', type=str, action='append', dest='tiers',
            metavar='TIER',
            help='Only score profiles in this JV tier (A/B/C/D/E). '
                 'Can be repeated: --tier A --tier B. '
                 'Matches are still evaluated against ALL active clients.',
        )
        parser.add_argument(
            '--client-id', type=str, default=None,
            help='Only score against this specific client UUID (for parallel runs).',
        )

    def handle(self, *args, **options):
        since_str = options['since']
        threshold = options['threshold']
        dry_run = options['dry_run']
        limit = options['limit']
        tiers = options.get('tiers') or []
        client_id_filter = options.get('client_id')

        start_time = time.time()

        # ------------------------------------------------------------------
        # Step 1: Determine the since date
        # ------------------------------------------------------------------
        if since_str:
            try:
                since_dt = datetime.fromisoformat(since_str)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                self.stderr.write(f'Invalid --since date: {since_str!r}')
                raise SystemExit(1)
        else:
            since_dt = datetime.now(timezone.utc) - timedelta(days=7)

        self.stdout.write(
            f'Scoring profiles enriched since: {since_dt.isoformat()}'
        )

        # ------------------------------------------------------------------
        # Step 2: Query profiles enriched since that date
        # ------------------------------------------------------------------
        if tiers:
            self.stdout.write(
                f'Tier filter: {tiers} '
                f'(matches still scored against all active clients)'
            )

        dsn = os.environ.get('DATABASE_URL')
        if not dsn:
            self.stderr.write('ERROR: DATABASE_URL environment variable not set.')
            raise SystemExit(1)

        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if tiers:
                query = """
                    SELECT id
                    FROM profiles
                    WHERE last_enriched_at >= %s
                      AND jv_readiness_score >= 20
                      AND jv_tier = ANY(%s)
                    ORDER BY last_enriched_at DESC
                """
                params = [since_dt, tiers]
            else:
                query = """
                    SELECT id
                    FROM profiles
                    WHERE last_enriched_at >= %s
                      AND jv_readiness_score >= 20
                    ORDER BY last_enriched_at DESC
                """
                params = [since_dt]
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, params)
            rows = cur.fetchall()
        finally:
            conn.close()

        profile_ids = [str(r['id']) for r in rows]
        if not profile_ids:
            self.stdout.write('No profiles enriched in the given window — nothing to score.')
            return

        # ------------------------------------------------------------------
        # Step 3: Count active clients for pair estimate
        # ------------------------------------------------------------------
        active_client_count = (
            MemberReport.objects.filter(is_active=True)
            .exclude(supabase_profile__isnull=True)
            .values('supabase_profile_id')
            .distinct()
            .count()
        )
        pair_count = len(profile_ids) * active_client_count

        self.stdout.write(
            f'Found {len(profile_ids)} enriched profiles × '
            f'{active_client_count} active clients = {pair_count:,} pairs'
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f'DRY RUN — would score {pair_count:,} pairs. No DB writes.'
                )
            )
            return

        # ------------------------------------------------------------------
        # Step 4: Score via cross-client scoring task
        # ------------------------------------------------------------------
        from matching.enrichment.flows.cross_client_scoring import (
            score_against_all_clients,
            flag_reports_for_update,
        )

        self.stdout.write(f'Scoring {pair_count:,} pairs (threshold={threshold})...')

        try:
            high_quality = score_against_all_clients(
                profile_ids=profile_ids,
                score_threshold=threshold,
                client_id_filter=client_id_filter,
            )
        except Exception as exc:
            self.stderr.write(f'Scoring failed: {exc}')
            raise

        # ------------------------------------------------------------------
        # Step 5: Flag reports for regeneration
        # ------------------------------------------------------------------
        flagged = 0
        if high_quality:
            try:
                flagged = flag_reports_for_update(high_quality)
            except Exception as exc:
                self.stderr.write(f'Report flagging failed: {exc}')

        # ------------------------------------------------------------------
        # Step 6: Report results
        # ------------------------------------------------------------------
        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f'\nScoring complete in {elapsed:.1f}s:\n'
            f'  Profiles scored:       {len(profile_ids)}\n'
            f'  Pairs evaluated:       {pair_count:,}\n'
            f'  High-quality matches:  {len(high_quality)} (>= {threshold})\n'
            f'  Reports flagged:       {flagged}'
        ))
