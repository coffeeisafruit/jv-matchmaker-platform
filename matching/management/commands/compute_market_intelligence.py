"""
Compute market intelligence: supply-demand gaps, role gaps, niche health.

Loads enriched profiles from the Supabase ``profiles`` table, runs
MarketGapAnalyzer, persists a snapshot into ``niche_statistics_snapshots``,
and writes JSON + Markdown reports to disk.

Usage:
    python manage.py compute_market_intelligence --dry-run
    python manage.py compute_market_intelligence --output-dir reports/market_intelligence/
    python manage.py compute_market_intelligence --min-profiles 100
"""

import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import psycopg2
from psycopg2.extras import RealDictCursor

from django.core.management.base import BaseCommand

from matching.enrichment.market_gaps import MarketGapAnalyzer, generate_gap_report


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

PROFILES_QUERY = """
    SELECT id, name, niche, network_role, seeking, offering,
           what_you_do, who_you_serve, source
    FROM profiles
    WHERE (seeking IS NOT NULL AND seeking != '')
       OR (offering IS NOT NULL AND offering != '')
"""

CREATE_SNAPSHOT_TABLE = """
    CREATE TABLE IF NOT EXISTS niche_statistics_snapshots (
        id SERIAL PRIMARY KEY,
        computed_at TIMESTAMPTZ DEFAULT NOW(),
        snapshot_data JSONB NOT NULL,
        version INTEGER DEFAULT 1
    );
"""

INSERT_SNAPSHOT = """
    INSERT INTO niche_statistics_snapshots (snapshot_data)
    VALUES (%s)
    RETURNING id;
"""


class Command(BaseCommand):
    help = 'Compute market intelligence (supply-demand gaps, role gaps, niche health) from enriched profiles'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Log analysis results but do not persist to database',
        )
        parser.add_argument(
            '--output-dir', type=str,
            default='reports/market_intelligence/',
            help='Directory for gap_report.json and gap_report.md (default: reports/market_intelligence/)',
        )
        parser.add_argument(
            '--min-profiles', type=int, default=50,
            help='Minimum enriched profiles required to run analysis (default: 50)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        output_dir = options['output_dir']
        min_profiles = options['min_profiles']

        dsn = os.environ.get('DATABASE_URL')
        if not dsn:
            self.stderr.write(self.style.ERROR('DATABASE_URL environment variable is not set.'))
            return

        # ── Load enriched profiles ────────────────────────────────────
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(PROFILES_QUERY)
            profiles = cur.fetchall()
        finally:
            conn.close()

        profile_count = len(profiles)
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write('MARKET INTELLIGENCE ANALYSIS')
        self.stdout.write(f'{"=" * 60}')
        self.stdout.write(f'Enriched profiles loaded: {profile_count}')

        if profile_count < min_profiles:
            self.stdout.write(self.style.WARNING(
                f'\nInsufficient profiles: {profile_count} < {min_profiles} minimum. '
                f'Skipping analysis.'
            ))
            return

        if dry_run:
            self.stdout.write(self.style.WARNING('\n  ** DRY RUN -- no data will be persisted **\n'))

        # ── Run gap analysis ──────────────────────────────────────────
        analyzer = MarketGapAnalyzer(profiles)
        report = analyzer.analyze()
        report_dict = report.to_dict()

        # ── Write report files ────────────────────────────────────────
        json_path, md_path = generate_gap_report(report, output_dir)
        self.stdout.write(f'\nReports written:')
        self.stdout.write(f'  JSON: {json_path}')
        self.stdout.write(f'  Markdown: {md_path}')

        # ── Persist snapshot ──────────────────────────────────────────
        if not dry_run:
            conn = psycopg2.connect(dsn)
            try:
                cur = conn.cursor()
                cur.execute(CREATE_SNAPSHOT_TABLE)
                cur.execute(INSERT_SNAPSHOT, (json.dumps(report_dict),))
                snapshot_id = cur.fetchone()[0]
                conn.commit()
                self.stdout.write(f'\nSnapshot saved: niche_statistics_snapshots.id = {snapshot_id}')
            finally:
                conn.close()
        else:
            self.stdout.write('\n[DRY RUN] Would persist snapshot to niche_statistics_snapshots table.')

        # ── Print summary ─────────────────────────────────────────────
        gap_count = len(report.supply_demand_gaps)
        role_gap_count = len(report.role_gaps)
        high_demand = [g for g in report.supply_demand_gaps if g.gap_type == 'high_demand']

        self.stdout.write(f'\n{"─" * 60}')
        self.stdout.write('SUMMARY')
        self.stdout.write(f'{"─" * 60}')
        self.stdout.write(f'  Enriched profiles: {report.enriched_profile_count}')
        self.stdout.write(f'  Canonical niches:  {report.canonical_niche_count}')
        self.stdout.write(f'  Supply-demand gaps: {gap_count}')
        self.stdout.write(f'  Role gaps:         {role_gap_count}')

        # Top 5 supply-demand gaps
        if high_demand:
            self.stdout.write(f'\n  Top 5 unmet demand:')
            for g in high_demand[:5]:
                self.stdout.write(
                    f'    {g.keyword:<25s}  seeking={g.seeking_count}  '
                    f'offering={g.offering_count}  ratio={g.gap_ratio:.1f}x'
                )

        # Top 3 role gaps (niches with missing high-value roles)
        gaps_with_missing = [g for g in report.role_gaps if g.missing_high_value_roles]
        if gaps_with_missing:
            self.stdout.write(f'\n  Top 3 role gaps:')
            for g in gaps_with_missing[:3]:
                missing_str = ', '.join(g.missing_high_value_roles[:3])
                self.stdout.write(
                    f'    {g.canonical_niche:<25s}  profiles={g.total_profiles}  '
                    f'dominant={g.dominant_role} ({g.dominance_ratio:.0%})  '
                    f'missing=[{missing_str}]'
                )

        # Health score average
        if report.niche_health:
            avg_health = sum(h.health_score for h in report.niche_health) / len(report.niche_health)
            self.stdout.write(f'\n  Avg niche health score: {avg_health:.1f}/100')

        # Stability warnings
        if report.stability_warnings:
            self.stdout.write(f'\n  Warnings:')
            for w in report.stability_warnings:
                self.stdout.write(f'    - {w}')

        self.stdout.write(f'\n{"=" * 60}\n')
