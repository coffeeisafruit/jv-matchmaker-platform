"""
One-time backfill: convert nested field_meta entries into top-level
per-field confidence entries so ConfidenceScorer.calculate_profile_confidence()
can compute a real weighted average.

Background:
  - The Exa pipeline (Feb 17-27) wrote enrichment data into
    enrichment_metadata.field_meta (nested), but the confidence scorer
    reads top-level keys with {'confidence': float} structure.
  - 3,232 profiles have profile_confidence = 0.0 because the per-field
    confidence code didn't exist during the last enrichment run.
  - This command bridges the gap by reading field_meta, computing
    confidence via ConfidenceScorer, and writing results at top level.

Usage:
    python manage.py backfill_confidence --dry-run
    python manage.py backfill_confidence
"""

import json
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import transaction

from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer
from matching.models import SupabaseProfile


# Map field_meta source names → SOURCE_BASE_CONFIDENCE keys.
# field_meta uses pipeline-level names; the scorer uses data-quality names.
SOURCE_MAP = {
    'exa_pipeline': 'website_scraped',       # 0.70 — web-sourced extraction
    'exa_research': 'website_scraped',       # 0.70 — same pipeline, alt name
    'ai_research': 'website_scraped',        # 0.70 — AI-driven web research
    'ai_inference': 'email_domain_inferred', # 0.50 — AI guessed/inferred
    'apollo': 'apollo',                      # 0.80 — direct match
    'apollo_verified': 'apollo_verified',    # 0.95 — verified
    'website_scrape': 'website_scraped',     # 0.70 — scraped
    'web_discovery': 'website_scraped',      # 0.70 — web discovered
    'client_ingest': 'manual',               # 1.00 — client-provided data
    'client_confirmed': 'manual',            # 1.00 — client confirmed
    'owl': 'owl',                            # 0.85 — OWL deep research
    'manual': 'manual',                      # 1.00 — manual entry
}

# Fields in field_meta that are scorable (have weights in the scorer)
SCORABLE_FIELDS = {
    'email', 'phone', 'linkedin', 'website', 'seeking', 'offering',
    'who_you_serve', 'what_you_do', 'list_size', 'niche', 'company',
    'revenue_tier', 'jv_history', 'content_platforms',
    'audience_engagement_score',
}


class Command(BaseCommand):
    help = 'Backfill per-field confidence entries and profile_confidence from existing field_meta data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Compute and display stats without saving to database',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        scorer = ConfidenceScorer()

        # ── Before snapshot ──────────────────────────────────────────
        total_members = SupabaseProfile.objects.filter(status='Member').count()
        zero_before = SupabaseProfile.objects.filter(
            status='Member', profile_confidence=0.0,
        ).count()
        nonzero_before = total_members - zero_before

        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write('ENRICHMENT CONFIDENCE BACKFILL')
        self.stdout.write(f'{"=" * 60}')
        self.stdout.write(f'\nBefore:')
        self.stdout.write(f'  Total members: {total_members}')
        self.stdout.write(f'  profile_confidence = 0.0: {zero_before}')
        self.stdout.write(f'  profile_confidence > 0.0: {nonzero_before}')
        if dry_run:
            self.stdout.write(self.style.WARNING('\n  ** DRY RUN — no data will be saved **\n'))

        # ── Process all member profiles ──────────────────────────────
        profiles = SupabaseProfile.objects.filter(status='Member')
        updated = 0
        skipped_no_fm = 0
        skipped_already_complete = 0
        new_confidences = []
        spot_checks = []

        for sp in profiles.iterator():
            meta = sp.enrichment_metadata or {}
            field_meta = meta.get('field_meta', {})

            if not field_meta:
                skipped_no_fm += 1
                continue

            # Find scorable fields in field_meta that DON'T already
            # have a top-level confidence entry.
            fields_to_add = []
            for field_name, fm_entry in field_meta.items():
                if field_name not in SCORABLE_FIELDS:
                    continue
                if not isinstance(fm_entry, dict):
                    continue
                # Skip if top-level entry already exists with confidence
                existing = meta.get(field_name)
                if isinstance(existing, dict) and 'confidence' in existing:
                    continue
                fields_to_add.append(field_name)

            if not fields_to_add:
                skipped_already_complete += 1
                continue

            # ── Build top-level confidence entries ────────────────────
            before_meta = json.dumps(
                {k: v for k, v in meta.items()
                 if isinstance(v, dict) and 'confidence' in v},
                indent=2,
            ) if len(spot_checks) < 5 else None

            for field_name in fields_to_add:
                fm_entry = field_meta[field_name]
                raw_source = fm_entry.get('source', 'unknown')
                mapped_source = SOURCE_MAP.get(raw_source, 'unknown')

                # Parse enrichment date from field_meta
                date_str = fm_entry.get('updated_at') or fm_entry.get('enriched_at')
                if date_str:
                    try:
                        enriched_at = datetime.fromisoformat(date_str)
                    except (ValueError, TypeError):
                        enriched_at = datetime.now()
                else:
                    # Fallback to top-level enriched_at
                    top_date = meta.get('enriched_at', '')
                    try:
                        enriched_at = datetime.fromisoformat(top_date)
                    except (ValueError, TypeError):
                        enriched_at = datetime.now()
                # Strip timezone to match scorer's datetime.now()
                enriched_at = enriched_at.replace(tzinfo=None)

                confidence = scorer.calculate_confidence(
                    field_name, mapped_source, enriched_at,
                )

                meta[field_name] = {
                    'confidence': round(confidence, 4),
                    'source': mapped_source,
                    'enriched_at': enriched_at.isoformat(),
                }

            # ── Compute profile-level confidence ─────────────────────
            profile_conf = scorer.calculate_profile_confidence(meta)
            profile_conf = round(profile_conf, 4)
            new_confidences.append(profile_conf)

            # ── Spot-check collection ────────────────────────────────
            if len(spot_checks) < 5:
                after_meta = {
                    k: v for k, v in meta.items()
                    if isinstance(v, dict) and 'confidence' in v
                }
                spot_checks.append({
                    'name': sp.name,
                    'id': str(sp.id),
                    'old_confidence': sp.profile_confidence,
                    'new_confidence': profile_conf,
                    'fields_added': fields_to_add,
                    'before_entries': before_meta,
                    'after_entries': json.dumps(after_meta, indent=2),
                })

            # ── Save ─────────────────────────────────────────────────
            if not dry_run:
                sp.enrichment_metadata = meta
                sp.profile_confidence = profile_conf
                sp.save(update_fields=['enrichment_metadata', 'profile_confidence'])

            updated += 1

        # ── After snapshot ───────────────────────────────────────────
        if not dry_run:
            zero_after = SupabaseProfile.objects.filter(
                status='Member', profile_confidence=0.0,
            ).count()
        else:
            zero_after = zero_before - updated

        self.stdout.write(f'\n{"─" * 60}')
        self.stdout.write('Results:')
        self.stdout.write(f'  Profiles updated: {updated}')
        self.stdout.write(f'  Skipped (no field_meta): {skipped_no_fm}')
        self.stdout.write(f'  Skipped (already complete): {skipped_already_complete}')
        self.stdout.write(f'\nAfter:')
        self.stdout.write(f'  profile_confidence = 0.0: {zero_after}')
        self.stdout.write(f'  profile_confidence > 0.0: {total_members - zero_after}')

        # ── Distribution ─────────────────────────────────────────────
        if new_confidences:
            new_confidences.sort()
            n = len(new_confidences)
            mean_c = sum(new_confidences) / n
            median_c = new_confidences[n // 2]
            variance = sum((c - mean_c) ** 2 for c in new_confidences) / n
            std_c = variance ** 0.5

            self.stdout.write(f'\nNew confidence distribution (N={n}):')
            self.stdout.write(f'  Min:    {new_confidences[0]:.4f}')
            self.stdout.write(f'  P25:    {new_confidences[n // 4]:.4f}')
            self.stdout.write(f'  Median: {median_c:.4f}')
            self.stdout.write(f'  Mean:   {mean_c:.4f}')
            self.stdout.write(f'  P75:    {new_confidences[3 * n // 4]:.4f}')
            self.stdout.write(f'  Max:    {new_confidences[-1]:.4f}')
            self.stdout.write(f'  Stdev:  {std_c:.4f}')

        # ── Spot checks ─────────────────────────────────────────────
        if spot_checks:
            self.stdout.write(f'\n{"─" * 60}')
            self.stdout.write('Spot Checks:')
            for i, sc in enumerate(spot_checks, 1):
                self.stdout.write(f'\n  [{i}] {sc["name"]}')
                self.stdout.write(f'      Old confidence: {sc["old_confidence"]}')
                self.stdout.write(f'      New confidence: {sc["new_confidence"]}')
                self.stdout.write(f'      Fields added: {sc["fields_added"]}')
                self.stdout.write(f'      Before entries: {sc["before_entries"]}')
                self.stdout.write(f'      After entries:')
                for line in sc['after_entries'].split('\n'):
                    self.stdout.write(f'        {line}')

        self.stdout.write(f'\n{"=" * 60}\n')
