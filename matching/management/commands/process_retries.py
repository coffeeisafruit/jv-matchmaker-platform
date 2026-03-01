"""
Process the retry queue: re-run failed pipeline operations.

Reads pending retry items, filters by retry eligibility, groups by operation
type, and processes each group with the appropriate handler. Marks successful
items as resolved and increments retry counts for failures.

Usage:
    python manage.py process_retries
    python manage.py process_retries --dry-run
    python manage.py process_retries --operation embedding_failed
    python manage.py process_retries --max-items 20
    python manage.py process_retries --dry-run --operation quarantined
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand

from config.alerting import send_alert
from matching.enrichment.retry_queue import (
    RETRY_OPERATIONS,
    RetryItem,
    mark_resolved,
    read_pending,
    should_retry,
    update_retry_count,
)

logger = logging.getLogger(__name__)

MAX_RETRIES_FALLBACK = 4
BACKOFF_HOURS_FALLBACK = 1


def _should_retry_fallback(item: RetryItem) -> bool:
    """Fallback retry check when should_retry() is not yet implemented.

    Simple policy: retry up to 4 times with a 1-hour backoff between attempts.
    """
    if item.retry_count >= MAX_RETRIES_FALLBACK:
        return False
    if item.last_retry_at:
        try:
            last = datetime.fromisoformat(item.last_retry_at)
            if (datetime.now() - last) < timedelta(hours=BACKOFF_HOURS_FALLBACK):
                return False
        except (ValueError, TypeError):
            pass  # Malformed timestamp — allow retry
    return True


def _check_should_retry(item: RetryItem) -> bool:
    """Try the real should_retry(); fall back gracefully if unimplemented."""
    try:
        return should_retry(item)
    except NotImplementedError:
        return _should_retry_fallback(item)


class Command(BaseCommand):
    help = 'Process the retry queue: re-run failed pipeline operations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Show what would be processed without actually running anything',
        )
        parser.add_argument(
            '--operation', type=str, default='',
            help='Filter to a specific operation type (e.g. embedding_failed)',
        )
        parser.add_argument(
            '--max-items', type=int, default=50,
            help='Maximum number of items to process (default: 50)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        operation_filter = options['operation']
        max_items = options['max_items']

        start_time = time.time()

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — no operations will be executed'))

        # 1. Read all pending retry items
        self.stdout.write('Reading retry queue...')
        pending = read_pending()
        self.stdout.write(f'Found {len(pending)} pending retry items')

        if not pending:
            self.stdout.write(self.style.SUCCESS('Retry queue is empty. Nothing to do.'))
            return

        # 2. Filter by operation type if specified
        if operation_filter:
            if operation_filter not in RETRY_OPERATIONS:
                self.stderr.write(
                    self.style.ERROR(
                        f'Unknown operation: {operation_filter}. '
                        f'Valid operations: {", ".join(sorted(RETRY_OPERATIONS.keys()))}'
                    )
                )
                return
            pending = [item for item in pending if item.operation == operation_filter]
            self.stdout.write(f'Filtered to {len(pending)} items for operation: {operation_filter}')

        # 3. Filter by retry eligibility (should_retry / fallback)
        eligible = [item for item in pending if _check_should_retry(item)]
        skipped_backoff = len(pending) - len(eligible)
        if skipped_backoff:
            self.stdout.write(f'Skipped {skipped_backoff} items (backoff not elapsed or max retries hit)')

        # 4. Enforce max-items limit
        if len(eligible) > max_items:
            self.stdout.write(f'Limiting to {max_items} items (of {len(eligible)} eligible)')
            eligible = eligible[:max_items]

        if not eligible:
            self.stdout.write(self.style.SUCCESS('No eligible items to process right now.'))
            return

        # 5. Group by operation type
        grouped: dict[str, list[RetryItem]] = defaultdict(list)
        for item in eligible:
            grouped[item.operation].append(item)

        self.stdout.write(f'\nProcessing {len(eligible)} items across {len(grouped)} operation types:')
        for op, items in sorted(grouped.items()):
            self.stdout.write(f'  {op}: {len(items)} items')

        # 6. Process each group
        results = {
            'processed': 0,
            'succeeded': 0,
            'failed': 0,
            'skipped_dry_run': 0,
            'max_retries_hit': [],
        }

        for operation, items in sorted(grouped.items()):
            self.stdout.write(f'\n--- Processing: {operation} ({len(items)} items) ---')
            handler = self._get_handler(operation)

            for item in items:
                if dry_run:
                    self.stdout.write(
                        f'  [DRY RUN] Would process: profile={item.profile_id} '
                        f'reason="{item.reason}" retries={item.retry_count}'
                    )
                    results['skipped_dry_run'] += 1
                    continue

                results['processed'] += 1
                try:
                    success = handler(item)
                    if success:
                        mark_resolved(item.profile_id, item.operation)
                        results['succeeded'] += 1
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'  Resolved: profile={item.profile_id} ({item.operation})'
                            )
                        )
                    else:
                        update_retry_count(item)
                        results['failed'] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f'  Still failing: profile={item.profile_id} '
                                f'({item.operation}, retry #{item.retry_count})'
                            )
                        )
                        if item.retry_count >= MAX_RETRIES_FALLBACK:
                            results['max_retries_hit'].append(item)
                except Exception as exc:
                    update_retry_count(item)
                    results['failed'] += 1
                    self.stderr.write(
                        self.style.ERROR(
                            f'  Error: profile={item.profile_id} ({item.operation}): {exc}'
                        )
                    )
                    if item.retry_count >= MAX_RETRIES_FALLBACK:
                        results['max_retries_hit'].append(item)

        # 7. Summary
        elapsed = time.time() - start_time
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write('RETRY PROCESSING SUMMARY')
        self.stdout.write(f'{"=" * 60}')

        if dry_run:
            self.stdout.write(f'  Would process: {results["skipped_dry_run"]} items (DRY RUN)')
        else:
            self.stdout.write(f'  Processed:  {results["processed"]}')
            self.stdout.write(f'  Succeeded:  {results["succeeded"]}')
            self.stdout.write(f'  Failed:     {results["failed"]}')
        self.stdout.write(f'  Elapsed:    {elapsed:.1f}s')

        # 8. Alert if items have hit max retries
        if results['max_retries_hit'] and not dry_run:
            exhausted_count = len(results['max_retries_hit'])
            exhausted_detail = '\n'.join(
                f'  - {item.profile_id} ({item.operation}): {item.reason}'
                for item in results['max_retries_hit']
            )
            self.stdout.write(
                self.style.ERROR(
                    f'\n{exhausted_count} item(s) have hit max retries and need manual attention:\n'
                    f'{exhausted_detail}'
                )
            )
            send_alert(
                'warning',
                f'Retry queue: {exhausted_count} item(s) exhausted all retries',
                exhausted_detail,
            )

        if not dry_run and results['succeeded'] > 0:
            self.stdout.write(
                self.style.SUCCESS(f'\nDone. {results["succeeded"]} items resolved successfully.')
            )

    # =========================================================================
    # Operation handlers
    # =========================================================================

    def _get_handler(self, operation: str):
        """Return the handler function for a given operation type."""
        handlers = {
            'embedding_failed': self._handle_embedding_failed,
            'match_recalc_failed': self._handle_match_recalc,
            'score_stale': self._handle_match_recalc,
            'db_write_failed': self._handle_manual_intervention,
            'email_write_failed': self._handle_manual_intervention,
            'quarantined': self._handle_quarantined,
            'ai_research_failed': self._handle_ai_research_failed,
            'report_skipped': self._handle_report_skipped,
            'confidence_calc_failed': self._handle_confidence_calc_failed,
        }
        return handlers.get(operation, self._handle_unknown)

    def _handle_embedding_failed(self, item: RetryItem) -> bool:
        """Re-run embedding generation for a single profile."""
        from lib.enrichment.embeddings import ProfileEmbeddingService
        from lib.enrichment.hf_client import HFClient
        from matching.models import SupabaseProfile

        profile = SupabaseProfile.objects.get(id=item.profile_id)
        profile_dict = {
            'name': profile.name,
            'seeking': profile.seeking or '',
            'offering': profile.offering or '',
            'who_you_serve': profile.who_you_serve or '',
            'what_you_do': profile.what_you_do or '',
        }

        client = HFClient()
        service = ProfileEmbeddingService(client)
        embeddings = service.embed_profile(profile_dict)

        if not embeddings:
            logger.warning(f'No embeddings generated for profile {item.profile_id}')
            return False

        # Write embeddings to DB
        update_fields = []
        for field_key, vector in embeddings.items():
            # field_key is e.g. 'embedding_seeking'
            if hasattr(profile, field_key):
                setattr(profile, field_key, json.dumps(vector))
                update_fields.append(field_key)

        if update_fields:
            from django.utils import timezone
            profile.embeddings_updated_at = timezone.now()
            update_fields.append('embeddings_updated_at')
            profile.save(update_fields=update_fields)
            logger.info(
                f'Embeddings updated for profile {item.profile_id}: {", ".join(update_fields)}'
            )

        return True

    def _handle_match_recalc(self, item: RetryItem) -> bool:
        """Re-run match score recalculation for a profile."""
        from matching.tasks import recalculate_matches_for_profile

        result = recalculate_matches_for_profile(item.profile_id)

        if result.get('errors'):
            logger.warning(
                f'Match recalculation had errors for {item.profile_id}: {result["errors"]}'
            )
            # Treat as success if at least some matches were updated
            if result.get('matches_updated', 0) > 0:
                return True
            return False

        return True

    def _handle_manual_intervention(self, item: RetryItem) -> bool:
        """Log DB/email write failures as needing manual intervention.

        We cannot safely replay arbitrary DB writes, so we alert and
        return False to keep the item in the queue for manual review.
        """
        detail = (
            f'Profile: {item.profile_id}\n'
            f'Operation: {item.operation}\n'
            f'Reason: {item.reason}\n'
            f'Context: {json.dumps(item.context, default=str)}'
        )
        logger.warning(f'Manual intervention required for {item.operation}: {detail}')
        send_alert(
            'warning',
            f'Manual intervention needed: {item.operation} for profile {item.profile_id}',
            detail,
        )
        # Return False — this item stays in the queue for manual resolution
        return False

    def _handle_quarantined(self, item: RetryItem) -> bool:
        """Re-fetch profile and re-run through the verification gate."""
        from matching.enrichment.verification_gate import GateStatus, VerificationGate
        from matching.models import SupabaseProfile

        profile = SupabaseProfile.objects.get(id=item.profile_id)

        # Build profile data dict for the gate
        profile_data = {
            'name': profile.name,
            'email': profile.email or '',
            'website': profile.website or '',
            'linkedin': profile.linkedin or '',
            'seeking': profile.seeking or '',
            'offering': profile.offering or '',
            'who_you_serve': profile.who_you_serve or '',
            'what_you_do': profile.what_you_do or '',
            'bio': profile.bio or '',
        }

        # Retrieve raw content and extraction metadata from context if available
        raw_content = item.context.get('raw_content')
        extraction_metadata = item.context.get('extraction_metadata')

        gate = VerificationGate()
        verdict = gate.evaluate(profile_data, raw_content, extraction_metadata)

        if verdict.status == GateStatus.QUARANTINED:
            logger.info(
                f'Profile {item.profile_id} still quarantined: {verdict.issues_summary}'
            )
            return False

        # Profile passed verification — apply fixes and update DB
        fixed_data = VerificationGate.apply_fixes(profile_data, verdict)
        update_fields = []
        for field_name, value in fixed_data.items():
            if hasattr(profile, field_name) and getattr(profile, field_name) != value:
                setattr(profile, field_name, value)
                update_fields.append(field_name)

        if update_fields:
            profile.save(update_fields=update_fields)
            logger.info(
                f'Profile {item.profile_id} passed verification gate '
                f'(status={verdict.status.value}), updated: {", ".join(update_fields)}'
            )

        return True

    def _handle_ai_research_failed(self, item: RetryItem) -> bool:
        """Flag profile for next enrichment batch (just mark in DB).

        We don't re-run AI research here — that's expensive and belongs in
        the enrichment pipeline. Instead, we flag the profile so the next
        batch picks it up.
        """
        from matching.models import SupabaseProfile

        profile = SupabaseProfile.objects.get(id=item.profile_id)

        # Flag in enrichment_metadata for the pipeline to pick up
        metadata = profile.enrichment_metadata or {}
        metadata['needs_reresearch'] = True
        metadata['reresearch_reason'] = item.reason
        metadata['reresearch_queued_at'] = datetime.now().isoformat()
        profile.enrichment_metadata = metadata
        profile.save(update_fields=['enrichment_metadata'])

        logger.info(f'Profile {item.profile_id} flagged for re-research in next batch')
        return True

    def _handle_report_skipped(self, item: RetryItem) -> bool:
        """Re-check if data is now sufficient and regenerate the report."""
        from matching.models import SupabaseMatch, SupabaseProfile
        from scripts.generate_partner_page import ReportReadinessGate

        profile = SupabaseProfile.objects.get(id=item.profile_id)

        # Check profile readiness
        profile_dict = {
            'name': profile.name,
            'what_you_do': profile.what_you_do or '',
            'who_you_serve': profile.who_you_serve or '',
            'email': profile.email or '',
        }
        is_ready, issues = ReportReadinessGate.check_profile(profile_dict)
        if not is_ready:
            logger.info(f'Profile {item.profile_id} still not ready for report: {issues}')
            return False

        # Check match readiness
        matches = list(
            SupabaseMatch.objects.filter(profile_id=item.profile_id)
            .order_by('-harmonic_mean')[:10]
            .values('id', 'suggested_profile_id', 'harmonic_mean')
        )

        # Enrich match dicts with contact info for the readiness check
        match_dicts = []
        for m in matches:
            partner = SupabaseProfile.objects.filter(id=m['suggested_profile_id']).first()
            if partner:
                match_dicts.append({
                    'email': partner.email or '',
                    'linkedin': partner.linkedin or '',
                    'website': partner.website or '',
                })

        matches_ready, match_issues = ReportReadinessGate.check_matches(match_dicts)
        if not matches_ready:
            logger.info(
                f'Matches for {item.profile_id} still not ready for report: {match_issues}'
            )
            return False

        # Data is now sufficient — flag for report generation in metadata
        metadata = profile.enrichment_metadata or {}
        metadata['report_ready'] = True
        metadata['report_ready_at'] = datetime.now().isoformat()
        profile.enrichment_metadata = metadata
        profile.save(update_fields=['enrichment_metadata'])

        logger.info(
            f'Profile {item.profile_id} now passes report readiness gate, '
            f'flagged for generation'
        )
        return True

    def _handle_confidence_calc_failed(self, item: RetryItem) -> bool:
        """Re-calculate the profile confidence score."""
        from matching.enrichment.confidence import ConfidenceScorer
        from matching.models import SupabaseProfile

        profile = SupabaseProfile.objects.get(id=item.profile_id)

        scorer = ConfidenceScorer()
        metadata = profile.enrichment_metadata or {}

        # Calculate confidence for each enriched field
        field_confidences = []
        enriched_at_str = metadata.get('enriched_at') or (
            profile.last_enriched_at.isoformat() if profile.last_enriched_at else None
        )

        if not enriched_at_str:
            logger.warning(
                f'Profile {item.profile_id} has no enrichment timestamp, '
                f'using profile update time'
            )
            enriched_at_str = (
                profile.updated_at.isoformat() if profile.updated_at else
                datetime.now().isoformat()
            )

        enriched_at = datetime.fromisoformat(enriched_at_str)

        source = metadata.get('source', 'unknown')

        # Score key fields
        scorable_fields = [
            'email', 'website', 'linkedin', 'seeking', 'offering',
            'who_you_serve', 'what_you_do',
        ]

        for field_name in scorable_fields:
            value = getattr(profile, field_name, None)
            if value and str(value).strip():
                confidence = scorer.calculate_confidence(
                    field_name=field_name,
                    source=source,
                    enriched_at=enriched_at,
                )
                field_confidences.append(confidence)

        if not field_confidences:
            logger.warning(f'No scorable fields for profile {item.profile_id}')
            return False

        # Overall confidence = mean of field confidences
        overall = sum(field_confidences) / len(field_confidences)
        profile.profile_confidence = round(overall, 4)
        profile.save(update_fields=['profile_confidence'])

        logger.info(
            f'Confidence recalculated for {item.profile_id}: '
            f'{overall:.4f} (from {len(field_confidences)} fields)'
        )
        return True

    def _handle_unknown(self, item: RetryItem) -> bool:
        """Handle unknown operation types — log and skip."""
        logger.warning(
            f'Unknown retry operation "{item.operation}" for profile {item.profile_id}. '
            f'Skipping.'
        )
        self.stderr.write(
            self.style.WARNING(
                f'  Unknown operation: {item.operation} — skipping profile {item.profile_id}'
            )
        )
        return False
