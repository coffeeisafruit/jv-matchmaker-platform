"""
Verify analytics interventions after their measurement window has elapsed.

Compares baseline metrics to current engagement data and classifies impact
as positive, negative, neutral, or insufficient_data.

Usage:
    python manage.py verify_interventions
    python manage.py verify_interventions --intervention-id 7
"""

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.utils import timezone

from matching.models import (
    AnalyticsInsight,
    AnalyticsIntervention,
    EngagementSummary,
    MemberReport,
    ReportPartner,
)


def _capture_metrics(report: MemberReport) -> dict:
    """
    Capture a snapshot of engagement metrics for a report.

    Shared between the admin baseline capture and the verification command.
    Returns a dict with standardized metric keys for before/after comparison.
    """
    now = timezone.now()

    page_summary = EngagementSummary.objects.filter(
        report=report, partner_id=''
    ).first()

    partner_summaries = EngagementSummary.objects.filter(
        report=report,
    ).exclude(partner_id='')

    total = partner_summaries.count()
    contacted = partner_summaries.filter(any_contact_action=True).count()

    avg_dwell = partner_summaries.aggregate(
        avg=Sum('avg_card_dwell_ms')
    )['avg']
    if avg_dwell is not None and total > 0:
        avg_dwell = avg_dwell // total
    else:
        avg_dwell = 0

    # Build contact_rate_by_score_bucket
    partners = ReportPartner.objects.filter(report=report)
    buckets = {}  # label -> {contacted: int, total: int}

    for rp in partners:
        if rp.match_score is None or rp.source_profile_id is None:
            continue
        es = partner_summaries.filter(
            partner_id=str(rp.source_profile_id)
        ).first()
        if es is None:
            continue

        score = rp.match_score
        if score >= 80:
            label = '80+'
        elif score >= 60:
            label = '60-80'
        elif score >= 40:
            label = '40-60'
        else:
            label = '<40'

        if label not in buckets:
            buckets[label] = {'contacted': 0, 'total': 0}
        buckets[label]['total'] += 1
        if es.any_contact_action:
            buckets[label]['contacted'] += 1

    contact_rate_by_bucket = {}
    for label, counts in buckets.items():
        rate = round(counts['contacted'] / counts['total'], 3) if counts['total'] else 0
        contact_rate_by_bucket[label] = {
            'rate': rate,
            'contacted': counts['contacted'],
            'total': counts['total'],
        }

    return {
        'captured_at': now.isoformat(),
        'sessions_total': page_summary.total_sessions if page_summary else 0,
        'contact_rate_overall': round(contacted / total, 3) if total else 0,
        'avg_card_dwell_ms': avg_dwell,
        'template_copy_count': page_summary.template_copy_count if page_summary else 0,
        'scroll_depth_avg_pct': page_summary.avg_scroll_depth_pct if page_summary else 0,
        'partners_total': total,
        'partners_contacted': contacted,
        'contact_rate_by_score_bucket': contact_rate_by_bucket,
    }


def _classify_impact(baseline: dict, followup: dict) -> tuple[str, str]:
    """
    Compare followup vs baseline metrics and classify impact.

    Returns (assessment, details_text).
    Thresholds:
        - positive: target metric improved >10%
        - negative: target metric declined >10%
        - neutral: change within 10%
        - insufficient_data: no engagement data available
    """
    if not baseline or not followup:
        return 'insufficient_data', 'No baseline or followup metrics available.'

    b_contact_rate = baseline.get('contact_rate_overall', 0)
    f_contact_rate = followup.get('contact_rate_overall', 0)
    b_sessions = baseline.get('sessions_total', 0)
    f_sessions = followup.get('sessions_total', 0)
    b_partners_total = baseline.get('partners_total', 0)
    f_partners_total = followup.get('partners_total', 0)

    # If there are no partners in either snapshot, insufficient data
    if b_partners_total == 0 and f_partners_total == 0:
        return 'insufficient_data', 'No partner engagement data in baseline or followup.'

    # If baseline had zero sessions and followup still has zero, insufficient data
    if b_sessions == 0 and f_sessions == 0:
        return 'insufficient_data', 'No sessions recorded in baseline or followup period.'

    details_parts = []

    # Primary metric: contact_rate_overall
    if b_contact_rate > 0:
        change_pct = (f_contact_rate - b_contact_rate) / b_contact_rate
    elif f_contact_rate > 0:
        # Went from 0 to something — positive
        change_pct = 1.0
    else:
        change_pct = 0.0

    details_parts.append(
        f'Contact rate: {b_contact_rate:.1%} -> {f_contact_rate:.1%} '
        f'({change_pct:+.1%} change)'
    )

    # Secondary metrics for context
    if b_sessions > 0:
        session_change = (f_sessions - b_sessions) / b_sessions
        details_parts.append(
            f'Sessions: {b_sessions} -> {f_sessions} ({session_change:+.1%})'
        )
    else:
        details_parts.append(f'Sessions: {b_sessions} -> {f_sessions}')

    b_contacted = baseline.get('partners_contacted', 0)
    f_contacted = followup.get('partners_contacted', 0)
    details_parts.append(
        f'Partners contacted: {b_contacted} -> {f_contacted}'
    )

    b_scroll = baseline.get('scroll_depth_avg_pct', 0)
    f_scroll = followup.get('scroll_depth_avg_pct', 0)
    if b_scroll > 0 or f_scroll > 0:
        details_parts.append(f'Scroll depth: {b_scroll}% -> {f_scroll}%')

    b_dwell = baseline.get('avg_card_dwell_ms', 0)
    f_dwell = followup.get('avg_card_dwell_ms', 0)
    if b_dwell > 0 or f_dwell > 0:
        details_parts.append(f'Avg card dwell: {b_dwell}ms -> {f_dwell}ms')

    details = '. '.join(details_parts) + '.'

    # Classify based on primary metric (contact rate) change
    if change_pct > 0.10:
        return 'positive', details
    elif change_pct < -0.10:
        return 'negative', details
    else:
        return 'neutral', details


class Command(BaseCommand):
    help = 'Verify analytics interventions after their measurement window has elapsed'

    def add_arguments(self, parser):
        parser.add_argument(
            '--intervention-id', type=int, default=None,
            help='Verify a specific intervention by ID (default: all pending)',
        )

    def handle(self, *args, **options):
        intervention_id = options.get('intervention_id')
        now = timezone.now()

        if intervention_id:
            interventions = AnalyticsIntervention.objects.filter(
                id=intervention_id,
                verified_at__isnull=True,
            )
            if not interventions.exists():
                self.stdout.write(self.style.WARNING(
                    f'Intervention #{intervention_id} not found or already verified.'
                ))
                return
        else:
            interventions = AnalyticsIntervention.objects.filter(
                verified_at__isnull=True,
            )

        if not interventions.exists():
            self.stdout.write(self.style.SUCCESS('No pending interventions to verify.'))
            return

        verified_count = 0
        skipped_count = 0

        for intervention in interventions:
            window_end = intervention.created_at + timedelta(
                days=intervention.measurement_window_days
            )

            # Skip if measurement window hasn't elapsed yet
            if window_end > now:
                days_remaining = (window_end - now).days
                self.stdout.write(
                    f'  Skipping #{intervention.id}: {days_remaining} days remaining '
                    f'in measurement window'
                )
                skipped_count += 1
                continue

            if not intervention.report:
                self.stdout.write(self.style.WARNING(
                    f'  Skipping #{intervention.id}: no report linked'
                ))
                skipped_count += 1
                continue

            report = intervention.report
            self.stdout.write(
                f'\n--- Verifying intervention #{intervention.id} '
                f'({intervention.get_intervention_type_display()}) '
                f'for {report.member_name} ---'
            )

            # Step 1: Capture current metrics
            followup = _capture_metrics(report)

            # Step 2 & 3: Compare and classify
            baseline = intervention.baseline_metrics
            assessment, details = _classify_impact(baseline, followup)

            # Step 4: Update intervention record
            intervention.followup_metrics = followup
            intervention.verified_at = now
            intervention.impact_assessment = assessment
            intervention.impact_details = details
            intervention.save(update_fields=[
                'followup_metrics', 'verified_at',
                'impact_assessment', 'impact_details',
            ])

            # Step 5: Create a verified insight
            AnalyticsInsight.objects.create(
                report=report,
                severity='info',
                category='intervention_verified',
                title=(
                    f'Intervention #{intervention.id} verified: {assessment}'
                ),
                description=(
                    f'{intervention.get_intervention_type_display()} intervention '
                    f'verified after {intervention.measurement_window_days}-day window. '
                    f'Impact: {assessment}. {details}'
                ),
                data={
                    'intervention_id': intervention.id,
                    'intervention_type': intervention.intervention_type,
                    'impact_assessment': assessment,
                    'baseline_contact_rate': baseline.get('contact_rate_overall', 0),
                    'followup_contact_rate': followup.get('contact_rate_overall', 0),
                },
            )

            # Step 6: Print results
            if assessment == 'positive':
                style = self.style.SUCCESS
            elif assessment == 'negative':
                style = self.style.ERROR
            else:
                style = self.style.WARNING

            self.stdout.write(style(f'  Impact: {assessment.upper()}'))
            self.stdout.write(f'  {details}')
            verified_count += 1

        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(self.style.SUCCESS(
            f'Done. Verified {verified_count} interventions, '
            f'skipped {skipped_count}.'
        ))
