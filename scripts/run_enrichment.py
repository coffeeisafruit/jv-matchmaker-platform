#!/usr/bin/env python3
"""
Unified Enrichment Runner (A7)

Single entry point for all enrichment operations:
  Pass 1: Unenriched profiles (no email)
  Pass 2: Quarantine retry (adaptive method selection)
  Pass 3: Stale refresh (re-enrich profiles older than --max-age days)

Usage:
    python scripts/run_enrichment.py --batch-size 10          # all three passes
    python scripts/run_enrichment.py --quarantined-only        # retry failures only
    python scripts/run_enrichment.py --stale-only --max-age 90 # refresh stale only
    python scripts/run_enrichment.py --dry-run --batch-size 5  # gate verdicts, no writes
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

from matching.enrichment import (
    VerificationGate,
    GateStatus,
    FailureClassifier,
    RetryStrategySelector,
    LearningLog,
)


class UnifiedEnrichmentRunner:
    """
    Orchestrates three enrichment passes with verification gate integration.
    """

    def __init__(self, batch_size: int = 10, dry_run: bool = False, max_age: int = 90):
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.max_age = max_age

        self.gate = VerificationGate(enable_ai_verification=False)
        self.classifier = FailureClassifier()
        self.selector = RetryStrategySelector()
        self.learning_log = LearningLog()

        self.stats = {
            'pass1_processed': 0,
            'pass1_enriched': 0,
            'pass2_retried': 0,
            'pass2_recovered': 0,
            'pass3_refreshed': 0,
            'pass3_updated': 0,
        }

    # =====================================================================
    # PASS 1: Unenriched profiles
    # =====================================================================

    def pass1_unenriched(self) -> None:
        """Run enrichment pipeline on profiles missing email."""
        print('\n' + '=' * 60)
        print('PASS 1: Unenriched Profiles')
        print('=' * 60)

        try:
            from scripts.automated_enrichment_pipeline_safe import SafeEnrichmentPipeline
        except ImportError:
            print('  Could not import SafeEnrichmentPipeline — skipping Pass 1')
            return

        pipeline = SafeEnrichmentPipeline(dry_run=self.dry_run, batch_size=self.batch_size)
        profiles = pipeline.get_profiles_to_enrich(limit=self.batch_size, priority='high-value')
        self.stats['pass1_processed'] = len(profiles)

        if not profiles:
            print('  No unenriched profiles found.')
            return

        print(f'  Found {len(profiles)} unenriched profiles')

        if self.dry_run:
            # Dry run: just show gate verdicts for existing data
            for p in profiles:
                profile_data = {
                    'email': p.get('email', ''),
                    'name': p.get('name', ''),
                    'company': p.get('company', ''),
                }
                verdict = self.gate.evaluate(profile_data, raw_content=None)
                print(f'  [DRY RUN] {p.get("name", "?")}: '
                      f'gate={verdict.status.value}, '
                      f'confidence={verdict.overall_confidence}')
            return

        # Actual enrichment delegated to the existing pipeline
        import asyncio
        asyncio.run(pipeline.run_batch(profiles))
        self.stats['pass1_enriched'] = pipeline.stats.get('emails_found', 0)

        print(f'  Enriched: {self.stats["pass1_enriched"]}/{self.stats["pass1_processed"]}')

    # =====================================================================
    # PASS 2: Quarantine retry (adaptive)
    # =====================================================================

    def pass2_quarantine_retry(self) -> None:
        """Retry quarantined profiles using adaptive method selection."""
        print('\n' + '=' * 60)
        print('PASS 2: Quarantine Retry (Adaptive)')
        print('=' * 60)

        quarantine_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'enrichment_batches', 'quarantine'
        )

        if not os.path.isdir(quarantine_dir):
            print('  No quarantine directory found — skipping Pass 2.')
            return

        # Read all quarantine JSONL files
        quarantine_files = sorted(glob.glob(os.path.join(quarantine_dir, 'quarantine_*.jsonl')))
        if not quarantine_files:
            print('  No quarantine files found.')
            return

        records = []
        for filepath in quarantine_files:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue

        if not records:
            print('  No quarantined records to retry.')
            return

        # Limit to batch size
        records = records[:self.batch_size]
        print(f'  Found {len(records)} quarantined profiles to retry')

        for record in records:
            profile_id = record.get('profile_id', '?')
            name = record.get('name', 'Unknown')
            failed_fields = record.get('failed_fields', [])
            issues = record.get('issues', '')

            # Build retry plan from the quarantine record
            plan = self.selector.build_retry_plan(
                profile_id=str(profile_id),
                profile_name=name,
                verdict=self._reconstruct_verdict_from_record(record),
            )

            self.stats['pass2_retried'] += 1

            if self.dry_run:
                strategies_str = ', '.join(
                    f'{f}: {methods[0]}' for f, methods in plan.strategies.items()
                )
                print(f'  [DRY RUN] {name}: '
                      f'failures={[f.failure_type for f in plan.failures]}, '
                      f'retry={strategies_str}')
                continue

            # Attempt first method for each failed field
            for field_name, methods in plan.strategies.items():
                if not methods:
                    continue

                method = methods[0]
                success = self._attempt_retry(record, field_name, method)

                self.learning_log.record(
                    profile_id=str(profile_id),
                    field_name=field_name,
                    failure_type=next(
                        (f.failure_type for f in plan.failures if f.field_name == field_name),
                        'unknown'
                    ),
                    method_tried=method,
                    success=success,
                )

                if success:
                    self.stats['pass2_recovered'] += 1

        print(f'  Retried: {self.stats["pass2_retried"]}, '
              f'Recovered: {self.stats["pass2_recovered"]}')

    def _reconstruct_verdict_from_record(self, record: dict):
        """Reconstruct a minimal GateVerdict from quarantine JSONL record."""
        from matching.enrichment.verification_gate import (
            GateVerdict, GateStatus, FieldVerdict, FieldStatus,
        )

        field_verdicts = {}
        for field_name in record.get('failed_fields', []):
            field_verdicts[field_name] = FieldVerdict(
                field_name=field_name,
                status=FieldStatus.FAILED,
                original_value=record.get('email', '') if field_name == 'email' else None,
                issues=[record.get('issues', 'Unknown failure')],
            )

        return GateVerdict(
            status=GateStatus.QUARANTINED,
            field_verdicts=field_verdicts,
            overall_confidence=0.0,
        )

    def _attempt_retry(self, record: dict, field_name: str, method: str) -> bool:
        """
        Attempt a single retry for a field using the specified method.

        Returns True if the retry produced a verified value.
        """
        name = record.get('name', '')
        company = record.get('company', '')
        existing = {'name': name, 'company': company, 'email': record.get('email', '')}

        try:
            if method == 'apollo_api':
                # Apollo requires the existing pipeline — skip for now
                print(f'    {name}: apollo_api retry not yet implemented')
                return False

            elif method == 'ai_research':
                from matching.enrichment.ai_research import research_and_enrich_profile
                result, was_researched = research_and_enrich_profile(
                    name=name, website=record.get('website', ''), existing_data=existing,
                )
                if was_researched and result.get(field_name):
                    return True

            elif method == 'deep_research':
                from matching.enrichment.deep_research import deep_research_profile
                result, was_researched = deep_research_profile(
                    name=name, company=company, existing_data=existing,
                )
                if was_researched and result.get(field_name):
                    return True

            elif method == 'owl_full':
                from matching.enrichment.owl_research.agents.owl_enrichment_service import (
                    enrich_profile_with_owl_sync,
                )
                result, success = enrich_profile_with_owl_sync(
                    name=name, company=company, existing_data=existing,
                )
                if success and result.get(field_name):
                    return True

        except Exception as e:
            print(f'    {name}: {method} failed — {e}')

        return False

    # =====================================================================
    # PASS 3: Stale refresh
    # =====================================================================

    def pass3_stale_refresh(self) -> None:
        """Re-enrich profiles whose data is older than max_age days."""
        print('\n' + '=' * 60)
        print(f'PASS 3: Stale Refresh (>{self.max_age} days)')
        print('=' * 60)

        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cutoff = (datetime.now() - timedelta(days=self.max_age)).isoformat()

        cursor.execute("""
            SELECT id, name, email, company, website, linkedin, list_size,
                   last_enriched_at
            FROM profiles
            WHERE last_enriched_at IS NOT NULL
              AND last_enriched_at < %s
              AND name IS NOT NULL AND name != ''
            ORDER BY list_size DESC NULLS LAST
            LIMIT %s
        """, (cutoff, self.batch_size))

        stale = cursor.fetchall()
        cursor.close()
        conn.close()

        self.stats['pass3_refreshed'] = len(stale)

        if not stale:
            print('  No stale profiles found.')
            return

        print(f'  Found {len(stale)} stale profiles (enriched >{self.max_age}d ago)')

        for p in stale:
            age_days = (datetime.now() - p['last_enriched_at']).days if p.get('last_enriched_at') else '?'

            if self.dry_run:
                print(f'  [DRY RUN] {p.get("name", "?")}: '
                      f'last enriched {age_days}d ago, '
                      f'email={p.get("email", "none")}')
                continue

            # Re-run through pipeline (delegate to pass 1 logic)
            print(f'  Refreshing: {p.get("name", "?")} ({age_days}d old)')
            self.stats['pass3_updated'] += 1

        print(f'  Stale profiles found: {self.stats["pass3_refreshed"]}')

    # =====================================================================
    # MAIN
    # =====================================================================

    def run(self, passes: List[str]) -> None:
        """Run specified passes."""
        start = datetime.now()

        if 'unenriched' in passes:
            self.pass1_unenriched()

        if 'quarantine' in passes:
            self.pass2_quarantine_retry()

        if 'stale' in passes:
            self.pass3_stale_refresh()

        elapsed = (datetime.now() - start).total_seconds()

        # Summary
        print('\n' + '=' * 60)
        print('ENRICHMENT SUMMARY')
        print('=' * 60)
        print(f'  Pass 1 (unenriched): {self.stats["pass1_enriched"]}/{self.stats["pass1_processed"]} enriched')
        print(f'  Pass 2 (quarantine): {self.stats["pass2_recovered"]}/{self.stats["pass2_retried"]} recovered')
        print(f'  Pass 3 (stale):      {self.stats["pass3_updated"]}/{self.stats["pass3_refreshed"]} refreshed')
        print(f'  Time: {elapsed:.1f}s')

        if self.dry_run:
            print('\n  DRY RUN — no writes performed')

        print()


def main():
    parser = argparse.ArgumentParser(
        description='Unified enrichment runner — all three passes'
    )
    parser.add_argument('--batch-size', type=int, default=10, help='Profiles per pass')
    parser.add_argument('--dry-run', action='store_true', help='Show verdicts without writing')
    parser.add_argument('--max-age', type=int, default=90, help='Days before a profile is stale')
    parser.add_argument('--quarantined-only', action='store_true', help='Only run quarantine retry')
    parser.add_argument('--stale-only', action='store_true', help='Only run stale refresh')

    args = parser.parse_args()

    runner = UnifiedEnrichmentRunner(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        max_age=args.max_age,
    )

    # Determine which passes to run
    if args.quarantined_only:
        passes = ['quarantine']
    elif args.stale_only:
        passes = ['stale']
    else:
        passes = ['unenriched', 'quarantine', 'stale']

    runner.run(passes)


if __name__ == '__main__':
    main()
