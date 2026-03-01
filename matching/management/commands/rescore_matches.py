"""
Re-score all SupabaseMatch records and produce a before/after impact report.

Captures current scores, re-scores using the updated ISMC engine (with
embedding-based synergy when available), and writes the production impact
report to validation_results/production_impact_report.txt.

PgBouncer-safe: loads match IDs upfront, processes in batches, and cycles
DB connections between batches to avoid idle-connection timeouts.

Usage:
    python manage.py rescore_matches
    python manage.py rescore_matches --resume             # only unscored matches (harmonic_mean IS NULL)
    python manage.py rescore_matches --dry-run             # report only, don't update DB
    python manage.py rescore_matches --limit 100           # rescore only first 100 matches
    python manage.py rescore_matches --batch-size 500      # batch size for DB writes
    python manage.py rescore_matches --snapshot-only       # save current scores without rescoring
"""

import json
import statistics
import time
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone

from matching.models import SupabaseProfile, SupabaseMatch
from matching.services import SupabaseMatchScoringService


class Command(BaseCommand):
    help = 'Re-score all matches and produce before/after impact report'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Compute new scores and report but do not update the database',
        )
        parser.add_argument(
            '--limit', type=int, default=0,
            help='Limit number of matches to rescore (0 = all)',
        )
        parser.add_argument(
            '--resume', action='store_true',
            help='Only rescore matches where harmonic_mean IS NULL',
        )
        parser.add_argument(
            '--batch-size', type=int, default=500,
            help='Number of matches per batch before cycling DB connection (default: 500)',
        )
        parser.add_argument(
            '--snapshot-only', action='store_true',
            help='Save current scores to snapshot file without rescoring',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        limit = options['limit']
        resume = options['resume']
        batch_size = options['batch_size']
        snapshot_only = options['snapshot_only']

        start_time = time.time()
        scorer = SupabaseMatchScoringService()

        # 1. Load match IDs + profile IDs upfront (lightweight queries)
        matches_qs = SupabaseMatch.objects.all()
        if resume:
            matches_qs = matches_qs.filter(harmonic_mean__isnull=True)
            self.stdout.write('Resume mode: filtering to harmonic_mean IS NULL')
        matches_qs = matches_qs.order_by('-match_score')
        if limit:
            matches_qs = matches_qs[:limit]

        match_rows = list(matches_qs.values_list('id', 'profile_id', 'suggested_profile_id'))
        match_ids = [row[0] for row in match_rows]
        total = len(match_ids)
        self.stdout.write(f'Found {total} matches to score')

        # 2. Pre-load ALL profiles once (fast — ~3,500 rows, stays in Python memory)
        all_profile_ids = set()
        for _, pid, sid in match_rows:
            all_profile_ids.add(pid)
            all_profile_ids.add(sid)
        profiles = {
            str(p.id): p for p in SupabaseProfile.objects.filter(id__in=all_profile_ids)
        }
        self.stdout.write(f'Loaded {len(profiles)} profiles into memory')

        # Pre-aggregate outcome track record from MatchLearningSignal
        from matching.models import MatchLearningSignal
        from django.db.models import Count
        raw_outcomes = MatchLearningSignal.objects.values(
            'match__partner_id', 'outcome'
        ).annotate(count=Count('id'))
        outcome_agg = {}
        for row in raw_outcomes:
            pid = str(row['match__partner_id'])
            outcome_agg.setdefault(pid, {})[row['outcome']] = row['count']
        self.stdout.write(f'Pre-aggregated outcomes for {len(outcome_agg)} partners')

        connection.close()

        if snapshot_only:
            matches = list(SupabaseMatch.objects.filter(id__in=match_ids))
            before_scores = []
            for m in matches:
                before_scores.append({
                    'match_id': str(m.id),
                    'profile_id': str(m.profile_id),
                    'suggested_id': str(m.suggested_profile_id),
                    'match_score': float(m.match_score) if m.match_score else None,
                })
            self._save_snapshot(before_scores)
            return

        # 3. Process in batches — score in memory, bulk-write once per batch
        before_scores = []
        after_scores = []
        rescored = 0
        failed = 0
        skipped = 0

        for batch_start in range(0, total, batch_size):
            batch_ids = match_ids[batch_start:batch_start + batch_size]

            # Load match objects for this batch only
            matches = list(SupabaseMatch.objects.filter(id__in=batch_ids))
            match_map = {str(m.id): m for m in matches}

            # Collect matches to bulk-update at end of batch
            to_update = []

            for mid in batch_ids:
                m = match_map.get(str(mid))
                if not m:
                    skipped += 1
                    continue

                before_scores.append({
                    'match_id': str(m.id),
                    'profile_id': str(m.profile_id),
                    'suggested_id': str(m.suggested_profile_id),
                    'match_score': float(m.match_score) if m.match_score else None,
                })

                profile_a = profiles.get(str(m.profile_id))
                profile_b = profiles.get(str(m.suggested_profile_id))

                if not profile_a or not profile_b:
                    skipped += 1
                    after_scores.append(before_scores[-1])
                    continue

                try:
                    result = scorer.score_pair(profile_a, profile_b, outcome_data=outcome_agg)

                    after_scores.append({
                        'match_id': str(m.id),
                        'profile_id': str(m.profile_id),
                        'suggested_id': str(m.suggested_profile_id),
                        'score_ab': result['score_ab'],
                        'score_ba': result['score_ba'],
                        'harmonic_mean': result['harmonic_mean'],
                        'breakdown_ab': result.get('breakdown_ab'),
                        'breakdown_ba': result.get('breakdown_ba'),
                    })

                    # Stage the update on the model instance (written in bulk below)
                    m.score_ab = result['score_ab']
                    m.score_ba = result['score_ba']
                    m.harmonic_mean = result['harmonic_mean']
                    m.match_reason = result.get('match_reason', '')
                    m.match_context = json.dumps({
                        'breakdown_ab': result['breakdown_ab'],
                        'breakdown_ba': result['breakdown_ba'],
                        'scored_at': timezone.now().isoformat(),
                        'scoring_version': 'ismc_v2_embeddings',
                    })
                    to_update.append(m)
                    rescored += 1

                except Exception as e:
                    failed += 1
                    after_scores.append(before_scores[-1])
                    self.stderr.write(f'  Failed match {m.id}: {e}')

            # Bulk write — 1 SQL statement instead of N individual UPDATEs
            if to_update and not dry_run:
                SupabaseMatch.objects.bulk_update(
                    to_update,
                    ['score_ab', 'score_ba', 'harmonic_mean', 'match_reason', 'match_context'],
                    batch_size=batch_size,
                )

            # Cycle DB connection between batches (PgBouncer safety)
            connection.close()

            elapsed = time.time() - start_time
            done = batch_start + len(batch_ids)
            rate = done / elapsed if elapsed > 0 else 0
            self.stdout.write(
                f'  Batch complete: {done}/{total} '
                f'({rescored} ok, {failed} failed, {skipped} skipped) '
                f'[{elapsed:.1f}s, {rate:.0f} matches/sec]'
            )

        elapsed = time.time() - start_time
        self.stdout.write(
            f'\nRescoring complete: {rescored} updated, {failed} failed, '
            f'{skipped} skipped in {elapsed:.1f}s'
            f'{" (DRY RUN — no DB writes)" if dry_run else ""}'
        )

        # Generate impact report (skip for resume mode — partial data skews comparison)
        if not resume:
            report = self._generate_report(before_scores, after_scores, profiles, elapsed, dry_run)
            self._save_report(report)
        else:
            self.stdout.write('Resume mode — skipping impact report (partial data).')

    def _extract_synergy(self, breakdown: dict) -> float | None:
        """Extract synergy score from a breakdown dict."""
        if not breakdown:
            return None
        synergy = breakdown.get('synergy', {})
        return synergy.get('score')

    def _generate_report(self, before: list, after: list, profiles: dict,
                         elapsed: float, dry_run: bool) -> str:
        """Generate the production impact report."""
        lines = []
        lines.append('=' * 80)
        lines.append('PRODUCTION IMPACT REPORT: Embedding-Based Synergy Scoring')
        lines.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        lines.append(f'Mode: {"DRY RUN" if dry_run else "LIVE"}')
        lines.append(f'Matches analyzed: {len(before)}')
        lines.append(f'Time elapsed: {elapsed:.1f}s')
        lines.append('=' * 80)

        # Filter to matches with valid scores in both before and after
        # Before uses match_score (original production score)
        # After uses harmonic_mean (new ISMC score with embedding synergy)
        valid = []
        for b, a in zip(before, after):
            if b['match_score'] is not None and a['harmonic_mean'] is not None:
                valid.append((b, a))

        if not valid:
            lines.append('\nNo valid match pairs to compare.')
            return '\n'.join(lines)

        before_scores_list = [b['match_score'] for b, _ in valid]
        after_hm = [a['harmonic_mean'] for _, a in valid]
        deltas = [a['harmonic_mean'] - b['match_score'] for b, a in valid]

        lines.append(f'\n{"─" * 80}')
        lines.append('1. SCORE DISTRIBUTION (0-100 scale)')
        lines.append(f'   Before = match_score (original), After = harmonic_mean (ISMC + embeddings)')
        lines.append(f'{"─" * 80}')
        lines.append(f'{"Metric":<30} {"Before":>12} {"After":>12} {"Delta":>12}')
        lines.append(f'{"─" * 30} {"─" * 12} {"─" * 12} {"─" * 12}')
        lines.append(f'{"Mean":<30} {statistics.mean(before_scores_list):>12.2f} {statistics.mean(after_hm):>12.2f} {statistics.mean(deltas):>+12.2f}')
        lines.append(f'{"Median":<30} {statistics.median(before_scores_list):>12.2f} {statistics.median(after_hm):>12.2f} {statistics.median(deltas):>+12.2f}')
        if len(before_scores_list) > 1:
            lines.append(f'{"Std dev":<30} {statistics.stdev(before_scores_list):>12.2f} {statistics.stdev(after_hm):>12.2f} {statistics.stdev(deltas):>12.2f}')
        lines.append(f'{"Min":<30} {min(before_scores_list):>12.2f} {min(after_hm):>12.2f} {min(deltas):>+12.2f}')
        lines.append(f'{"Max":<30} {max(before_scores_list):>12.2f} {max(after_hm):>12.2f} {max(deltas):>+12.2f}')

        # Tier changes
        lines.append(f'\n{"─" * 80}')
        lines.append('2. TIER CHANGES')
        lines.append(f'{"─" * 80}')

        def tier(score):
            if score >= 80: return (4, 'Excellent (80+)')
            if score >= 60: return (3, 'Good (60-80)')
            if score >= 40: return (2, 'Fair (40-60)')
            return (1, 'Poor (<40)')

        tier_changes = {'upgraded': 0, 'downgraded': 0, 'unchanged': 0}
        for b, a in valid:
            tb_rank, _ = tier(b['match_score'])
            ta_rank, _ = tier(a['harmonic_mean'])
            if ta_rank > tb_rank:
                tier_changes['upgraded'] += 1
            elif ta_rank < tb_rank:
                tier_changes['downgraded'] += 1
            else:
                tier_changes['unchanged'] += 1

        for k, v in tier_changes.items():
            lines.append(f'  {k}: {v} ({v / len(valid) * 100:.1f}%)')

        # Rescued matches (synergy went from <6 to >=6)
        lines.append(f'\n{"─" * 80}')
        lines.append('3. RESCUED MATCHES (score went from <6 to >=6 on 0-10 component scale)')
        lines.append(f'{"─" * 80}')

        rescued = [(b, a) for b, a in valid
                    if b['match_score'] < 60 and a['harmonic_mean'] >= 60]
        lines.append(f'  Matches rescued: {len(rescued)}')

        # Top 20 biggest positive changes
        lines.append(f'\n{"─" * 80}')
        lines.append('4. TOP 20 BIGGEST POSITIVE SCORE CHANGES')
        lines.append(f'{"─" * 80}')
        sorted_by_delta = sorted(valid, key=lambda x: x[1]['harmonic_mean'] - x[0]['match_score'], reverse=True)

        lines.append(f'  {"#":>3}  {"Before":>8}  {"After":>8}  {"Delta":>8}  Profile A → Profile B')
        lines.append(f'  {"─" * 3}  {"─" * 8}  {"─" * 8}  {"─" * 8}  {"─" * 40}')
        for i, (b, a) in enumerate(sorted_by_delta[:20]):
            delta = a['harmonic_mean'] - b['match_score']
            pa = profiles.get(b['profile_id'])
            pb = profiles.get(b['suggested_id'])
            name_a = pa.name[:20] if pa else '?'
            name_b = pb.name[:20] if pb else '?'
            lines.append(f'  {i+1:3d}  {b["match_score"]:8.2f}  {a["harmonic_mean"]:8.2f}  {delta:+8.2f}  {name_a} → {name_b}')

        # Top 20 biggest negative changes (watch for regressions)
        lines.append(f'\n{"─" * 80}')
        lines.append('5. TOP 20 BIGGEST NEGATIVE SCORE CHANGES (regressions)')
        lines.append(f'{"─" * 80}')

        lines.append(f'  {"#":>3}  {"Before":>8}  {"After":>8}  {"Delta":>8}  Profile A → Profile B')
        lines.append(f'  {"─" * 3}  {"─" * 8}  {"─" * 8}  {"─" * 8}  {"─" * 40}')
        for i, (b, a) in enumerate(sorted_by_delta[-20:]):
            delta = a['harmonic_mean'] - b['match_score']
            pa = profiles.get(b['profile_id'])
            pb = profiles.get(b['suggested_id'])
            name_a = pa.name[:20] if pa else '?'
            name_b = pb.name[:20] if pb else '?'
            lines.append(f'  {i+1:3d}  {b["match_score"]:8.2f}  {a["harmonic_mean"]:8.2f}  {delta:+8.2f}  {name_a} → {name_b}')

        # Scoring method breakdown
        lines.append(f'\n{"─" * 80}')
        lines.append('6. SCORING METHOD BREAKDOWN')
        lines.append(f'{"─" * 80}')

        semantic_count = 0
        fallback_count = 0
        for _, a in valid:
            ab_breakdown = a.get('breakdown_ab', {})
            if ab_breakdown:
                synergy_factors = ab_breakdown.get('synergy', {}).get('factors', [])
                for f in synergy_factors:
                    if f.get('method') == 'semantic':
                        semantic_count += 1
                    elif f.get('method') == 'word_overlap':
                        fallback_count += 1

        lines.append(f'  Semantic (embedding): {semantic_count} factor evaluations')
        lines.append(f'  Word overlap (fallback): {fallback_count} factor evaluations')

        lines.append(f'\n{"=" * 80}')
        lines.append('END OF REPORT')
        lines.append(f'{"=" * 80}')

        return '\n'.join(lines)

    def _save_report(self, report: str):
        """Save the report to validation_results/."""
        output_dir = Path(__file__).resolve().parent.parent.parent.parent / 'validation_results'
        output_dir.mkdir(exist_ok=True)
        path = output_dir / 'production_impact_report.txt'
        path.write_text(report)
        self.stdout.write(self.style.SUCCESS(f'\nReport saved: {path}'))
        self.stdout.write(report)

    def _save_snapshot(self, scores: list):
        """Save current scores snapshot."""
        output_dir = Path(__file__).resolve().parent.parent.parent.parent / 'validation_results'
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = output_dir / f'score_snapshot_{timestamp}.json'
        path.write_text(json.dumps(scores, indent=2))
        self.stdout.write(self.style.SUCCESS(f'Snapshot saved: {path} ({len(scores)} matches)'))
