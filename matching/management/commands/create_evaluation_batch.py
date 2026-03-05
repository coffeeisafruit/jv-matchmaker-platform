"""
Management command: create_evaluation_batch

Creates a calibration or coverage batch for the Match Evaluation System.

Stratified sampling strategy:
  - Low band  (score < 40):  5 "bad" matches to anchor the bottom of the scale
  - Mid band  (40-69):       quota samples the largest region
  - High band (score >= 70): strong matches for the top anchor

Usage:
    python manage.py create_evaluation_batch --phase calibration --size 45 --name "Calibration 1"
    python manage.py create_evaluation_batch --phase coverage --size 15 --name "Coverage Batch A" --reviewer alice
    python manage.py create_evaluation_batch --phase validation --size 48 --name "Holdout 1"
"""

import random
import logging

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from matching.models import (
    SupabaseProfile, ReportPartner, MemberReport,
    EvaluationReviewer, EvaluationBatch, EvaluationItem,
)
from matching.services import SupabaseMatchScoringService

logger = logging.getLogger(__name__)


# Score bands aligned with ISMC delivery threshold (64 = Premier tier cutoff)
# 30% high (≥64): matches we actually deliver to clients
# 40% mid  (55-63): borderline — below delivery threshold, useful anchors
# 30% low  (<55): weak matches — bottom-of-scale anchors
SCORE_BANDS = [
    ('low',   0,  55, None),
    ('mid',  55,  64, None),
    ('high', 64, 100, None),
]


class Command(BaseCommand):
    help = 'Create a stratified evaluation batch for human-in-the-loop ISMC calibration'

    def add_arguments(self, parser):
        parser.add_argument('--name', required=True, help='Batch name (e.g. "Calibration 1")')
        parser.add_argument(
            '--phase',
            choices=['calibration', 'coverage', 'validation'],
            default='calibration',
            help='Evaluation phase (default: calibration)',
        )
        parser.add_argument(
            '--size', type=int, default=45,
            help='Total number of items in the batch (default: 45)',
        )
        parser.add_argument(
            '--reviewer', action='append', dest='reviewers',
            help='Reviewer name to assign (repeat for multiple). Omit for unassigned.',
        )
        parser.add_argument(
            '--client', type=int,
            help='Restrict sampling to a single MemberReport client (report ID)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Print what would be created without writing to DB',
        )

    def handle(self, *args, **options):
        name = options['name']
        phase = options['phase']
        size = options['size']
        reviewer_names = options['reviewers'] or []
        client_report_id = options['client']
        dry_run = options['dry_run']

        self.stdout.write(f"\nCreating evaluation batch: {name!r} ({phase}, {size} items)")

        # --- 1. Collect candidate (client, partner) pairs from existing reports ---
        candidates = self._collect_candidates(client_report_id)
        if not candidates:
            raise CommandError("No ReportPartner records with source_profile found. Generate reports first.")

        self.stdout.write(f"  Found {len(candidates)} candidate pairs from delivered reports")

        # --- 2. Score all candidates and bucket by band ---
        scorer = SupabaseMatchScoringService()
        scored = self._score_candidates(scorer, candidates)
        self.stdout.write(f"  Scored {len(scored)} pairs (skipped ineligible)")

        # --- 3. Stratified sample ---
        selected = self._stratified_sample(scored, size)
        self.stdout.write(f"  Selected {len(selected)} pairs (stratified)")

        for band, score, client_p, partner_p, report_partner in selected:
            self.stdout.write(f"    [{band:4s}] score={score:.1f}  {client_p.name} ↔ {partner_p.name}")

        if dry_run:
            self.stdout.write(self.style.WARNING("\nDry run — nothing written."))
            return

        # --- 4. Look up reviewers ---
        reviewers = []
        for rname in reviewer_names:
            try:
                reviewers.append(EvaluationReviewer.objects.get(name__iexact=rname, is_active=True))
            except EvaluationReviewer.DoesNotExist:
                raise CommandError(f"Reviewer not found: {rname!r}. Create via Django admin first.")

        # --- 5. Write to DB ---
        with transaction.atomic():
            batch = EvaluationBatch.objects.create(
                name=name,
                phase=phase,
                status=EvaluationBatch.Status.ACTIVE,
                selection_criteria={
                    'strategy': 'stratified',
                    'score_bands': [{'label': b, 'min': mn, 'max': mx, 'target': t}
                                    for b, mn, mx, t in SCORE_BANDS],
                    'total_candidates': len(candidates),
                    'total_scored': len(scored),
                    'client_report_id': client_report_id,
                },
            )
            if reviewers:
                batch.assigned_reviewers.set(reviewers)

            items_created = 0
            for position, (band, score, client_p, partner_p, report_partner) in enumerate(selected):
                # Re-score to capture full breakdown for the snapshot
                result = scorer.score_pair(client_p, partner_p)
                EvaluationItem.objects.create(
                    batch=batch,
                    client_profile=client_p,
                    partner_profile=partner_p,
                    report_partner=report_partner,
                    algorithm_score=result['harmonic_mean'],
                    algorithm_breakdown={
                        'breakdown_ab': result.get('breakdown_ab', {}),
                        'breakdown_ba': result.get('breakdown_ba', {}),
                        'score_ab': result.get('score_ab', 0),
                        'score_ba': result.get('score_ba', 0),
                    },
                    why_fit_narrative=report_partner.why_fit if report_partner else '',
                    position=position,
                )
                items_created += 1

        reviewer_str = ', '.join(r.name for r in reviewers) or '(unassigned)'
        self.stdout.write(self.style.SUCCESS(
            f"\n✓ Batch created: {batch.id} — {items_created} items, assigned to {reviewer_str}"
        ))

    # -------------------------------------------------------------------------

    def _collect_candidates(self, client_report_id=None):
        """
        Return list of (client_profile, partner_profile, report_partner) tuples
        sourced from delivered ReportPartner records that have a linked SupabaseProfile.
        """
        qs = ReportPartner.objects.select_related(
            'report', 'source_profile',
        ).filter(
            source_profile__isnull=False,
        )

        if client_report_id:
            qs = qs.filter(report_id=client_report_id)

        # We need the MemberReport to have a client SupabaseProfile link
        # Fall back to the report's member_name lookup if direct FK not present
        candidates = []
        seen = set()
        for rp in qs:
            client_profile = self._get_client_profile(rp.report)
            if client_profile and rp.source_profile and client_profile.id != rp.source_profile.id:
                key = (client_profile.id, rp.source_profile.id)
                if key not in seen:
                    seen.add(key)
                    candidates.append((client_profile, rp.source_profile, rp))

        return candidates

    def _get_client_profile(self, report: MemberReport):
        """Resolve the MemberReport's client to a SupabaseProfile via email lookup."""
        if report.member_email:
            try:
                return SupabaseProfile.objects.get(email=report.member_email)
            except (SupabaseProfile.DoesNotExist, SupabaseProfile.MultipleObjectsReturned):
                pass
        return None

    def _score_candidates(self, scorer, candidates):
        """
        Score all (client, partner, rp) tuples.
        Returns list of (band, score, client, partner, rp) sorted by score.
        """
        scored = []
        for client_p, partner_p, rp in candidates:
            try:
                result = scorer.score_pair(client_p, partner_p)
                if result.get('ineligible'):
                    continue
                hm = result['harmonic_mean']
                band = self._get_band(hm)
                scored.append((band, hm, client_p, partner_p, rp))
            except Exception as e:
                logger.warning(f"Scoring failed for {client_p.name} ↔ {partner_p.name}: {e}")
                continue

        return sorted(scored, key=lambda x: x[1])

    def _get_band(self, score):
        for label, mn, mx, _ in SCORE_BANDS:
            if mn <= score < mx:
                return label
        return 'high'

    def _stratified_sample(self, scored, target_size):
        """
        Proportional stratified sampling aligned with ISMC delivery threshold:
          - High  (≥64): 30% — delivery range, primary calibration target
          - Mid (55-63): 40% — borderline anchors
          - Low   (<55): 30% — bottom-of-scale anchors

        If a band has fewer candidates than its target, the remainder is
        redistributed to the next band down (high → mid → low).
        """
        by_band = {'low': [], 'mid': [], 'high': []}
        for item in scored:
            band = item[0]
            if band in by_band:
                by_band[band].append(item)

        for band in by_band:
            random.shuffle(by_band[band])

        high_target = round(target_size * 0.30)
        mid_target  = round(target_size * 0.40)
        low_target  = target_size - high_target - mid_target

        high_count = min(high_target, len(by_band['high']))
        overflow   = high_target - high_count          # couldn't fill high → push to mid
        mid_count  = min(mid_target + overflow, len(by_band['mid']))
        overflow   = (mid_target + overflow) - mid_count  # couldn't fill mid → push to low
        low_count  = min(low_target + overflow, len(by_band['low']))

        selected = (
            by_band['high'][:high_count] +
            by_band['mid'][:mid_count] +
            by_band['low'][:low_count]
        )

        random.shuffle(selected)
        return selected[:target_size]
