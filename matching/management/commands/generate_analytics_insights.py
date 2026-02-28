"""
Generate proactive analytics insights from engagement data.

Scans EngagementSummary records for patterns that indicate opportunities
or problems, and creates AnalyticsInsight records for operator review.

Usage:
    python manage.py generate_analytics_insights
    python manage.py generate_analytics_insights --report-id 42
"""

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Avg, Q, Sum
from django.utils import timezone

from matching.models import (
    AnalyticsInsight,
    AnalyticsIntervention,
    EngagementSummary,
    MemberReport,
    ReportPartner,
)


def _insight_exists(category: str, report=None) -> bool:
    """Check if an active, non-dismissed insight already exists for this category + report."""
    return AnalyticsInsight.objects.filter(
        category=category,
        report=report,
        is_active=True,
        is_dismissed=False,
    ).exists()


class Command(BaseCommand):
    help = 'Generate proactive analytics insights from engagement data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--report-id', type=int, default=None,
            help='Limit analysis to a single report ID',
        )

    def handle(self, *args, **options):
        report_id = options.get('report_id')
        if report_id:
            reports = MemberReport.objects.filter(id=report_id, is_active=True)
        else:
            reports = MemberReport.objects.filter(is_active=True)

        if not reports.exists():
            self.stdout.write(self.style.WARNING('No active reports found.'))
            return

        created_count = 0
        skipped_count = 0

        for report in reports:
            self.stdout.write(f'\n--- Report #{report.id}: {report.member_name} ---')

            page_summary = EngagementSummary.objects.filter(
                report=report, partner_id=''
            ).first()
            partner_summaries = EngagementSummary.objects.filter(
                report=report,
            ).exclude(partner_id='')

            # Rule 1 & 2: Idle report
            c, s = self._check_idle_report(report, page_summary)
            created_count += c
            skipped_count += s

            # Rule 3: Score calibration
            c, s = self._check_score_calibration(report, partner_summaries)
            created_count += c
            skipped_count += s

            # Rule 4: Template friction
            c, s = self._check_template_friction(report, page_summary, partner_summaries)
            created_count += c
            skipped_count += s

            # Rule 5: Enrichment quality
            c, s = self._check_enrichment_quality(report, partner_summaries)
            created_count += c
            skipped_count += s

            # Rule 6: Partner friction
            c, s = self._check_partner_friction(report, partner_summaries)
            created_count += c
            skipped_count += s

            # Rule 7 & 8: Engagement patterns
            c, s = self._check_engagement_patterns(report, partner_summaries)
            created_count += c
            skipped_count += s

            # Rule 9: Section attention
            c, s = self._check_section_attention(report, page_summary)
            created_count += c
            skipped_count += s

        # Rule 10: Intervention pending (global, not per-report)
        c, s = self._check_pending_interventions()
        created_count += c
        skipped_count += s

        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(self.style.SUCCESS(
            f'Done. Created {created_count} insights, skipped {skipped_count} duplicates.'
        ))

    # =========================================================================
    # RULE 1 & 2: Idle Report
    # =========================================================================

    def _check_idle_report(self, report, page_summary):
        created = 0
        skipped = 0

        if not page_summary or page_summary.days_since_last_visit is None:
            return created, skipped

        days = page_summary.days_since_last_visit

        # Rule 2: CRITICAL — 14+ days idle (check first so we don't also create WARNING)
        if days > 14:
            if _insight_exists('idle_report', report):
                skipped += 1
            else:
                insight = AnalyticsInsight.objects.create(
                    report=report,
                    severity='critical',
                    category='idle_report',
                    title=f'{report.member_name} has not visited in {days} days',
                    description=(
                        f'Report #{report.id} has had no visits for {days} days. '
                        f'Consider a nudge email or check-in to re-engage the member.'
                    ),
                    data={'days_since_last_visit': days},
                )
                self._print_insight(insight)
                created += 1
        # Rule 1: WARNING — 7-14 days idle
        elif days > 7:
            if _insight_exists('idle_report', report):
                skipped += 1
            else:
                insight = AnalyticsInsight.objects.create(
                    report=report,
                    severity='warning',
                    category='idle_report',
                    title=f'{report.member_name} idle for {days} days',
                    description=(
                        f'Report #{report.id} has not been visited in {days} days. '
                        f'The member may need a reminder or follow-up.'
                    ),
                    data={'days_since_last_visit': days},
                )
                self._print_insight(insight)
                created += 1

        return created, skipped

    # =========================================================================
    # RULE 3: Score Calibration
    # =========================================================================

    def _check_score_calibration(self, report, partner_summaries):
        created = 0
        skipped = 0

        # Build score buckets from ReportPartner match_score joined with engagement
        partners = ReportPartner.objects.filter(report=report)

        bucket_high = []   # score 80+
        bucket_mid = []    # score 60-80

        for rp in partners:
            if rp.match_score is None:
                continue
            # Find engagement for this partner
            es = partner_summaries.filter(partner_id=str(rp.source_profile_id)).first() if rp.source_profile_id else None
            if es is None:
                continue
            contacted = es.any_contact_action
            if rp.match_score >= 80:
                bucket_high.append(contacted)
            elif rp.match_score >= 60:
                bucket_mid.append(contacted)

        # Need at least 3 partners per bucket
        if len(bucket_high) >= 3 and len(bucket_mid) >= 3:
            rate_high = sum(bucket_high) / len(bucket_high)
            rate_mid = sum(bucket_mid) / len(bucket_mid)

            if rate_mid > rate_high:
                if _insight_exists('score_calibration', report):
                    skipped += 1
                else:
                    insight = AnalyticsInsight.objects.create(
                        report=report,
                        severity='warning',
                        category='score_calibration',
                        title=f'Score calibration issue: mid-score partners outperforming high-score',
                        description=(
                            f'Partners scored 60-80 have a {rate_mid:.0%} contact rate vs '
                            f'{rate_high:.0%} for 80+ partners. The scoring model may need '
                            f'recalibration for {report.member_name}.'
                        ),
                        data={
                            'rate_60_80': round(rate_mid, 3),
                            'rate_80_plus': round(rate_high, 3),
                            'count_60_80': len(bucket_mid),
                            'count_80_plus': len(bucket_high),
                        },
                    )
                    self._print_insight(insight)
                    created += 1

        return created, skipped

    # =========================================================================
    # RULE 4: Template Friction
    # =========================================================================

    def _check_template_friction(self, report, page_summary, partner_summaries):
        created = 0
        skipped = 0

        if not page_summary:
            return created, skipped

        copies = page_summary.template_copy_count
        total_email_clicks = partner_summaries.aggregate(
            total=Sum('email_click_count')
        )['total'] or 0

        if copies > 3 and total_email_clicks == 0:
            if _insight_exists('template_friction', report):
                skipped += 1
            else:
                insight = AnalyticsInsight.objects.create(
                    report=report,
                    severity='warning',
                    category='template_friction',
                    title=f'Template copied {copies}x but no email clicks',
                    description=(
                        f'{report.member_name} copied outreach templates {copies} times '
                        f'but has 0 email clicks across all partners. The template may not '
                        f'be converting to actual outreach — consider revising or adding '
                        f'a mailto: link.'
                    ),
                    data={
                        'template_copy_count': copies,
                        'total_email_clicks': total_email_clicks,
                    },
                )
                self._print_insight(insight)
                created += 1

        return created, skipped

    # =========================================================================
    # RULE 5: Enrichment Quality
    # =========================================================================

    def _check_enrichment_quality(self, report, partner_summaries):
        created = 0
        skipped = 0

        partners = ReportPartner.objects.filter(report=report)

        rich_contacted = 0
        rich_total = 0
        basic_contacted = 0
        basic_total = 0

        for rp in partners:
            es = partner_summaries.filter(
                partner_id=str(rp.source_profile_id)
            ).first() if rp.source_profile_id else None
            if es is None:
                continue

            why_fit_len = len(rp.why_fit) if rp.why_fit else 0
            if why_fit_len > 100:
                rich_total += 1
                if es.any_contact_action:
                    rich_contacted += 1
            else:
                basic_total += 1
                if es.any_contact_action:
                    basic_contacted += 1

        if rich_total >= 1 and basic_total >= 1:
            rate_rich = rich_contacted / rich_total
            rate_basic = basic_contacted / basic_total if basic_total else 0

            # Rich why_fit cards get 2x+ contact rate vs basic
            if rate_basic > 0 and rate_rich >= 2 * rate_basic:
                if _insight_exists('enrichment_quality', report):
                    skipped += 1
                else:
                    insight = AnalyticsInsight.objects.create(
                        report=report,
                        severity='info',
                        category='enrichment_quality',
                        title=f'Rich why-fit narratives driving {rate_rich / rate_basic:.1f}x more contacts',
                        description=(
                            f'Partners with detailed why-fit text (>100 chars) have a '
                            f'{rate_rich:.0%} contact rate vs {rate_basic:.0%} for basic cards. '
                            f'Consider upgrading remaining {basic_total} basic narratives '
                            f'for {report.member_name}.'
                        ),
                        data={
                            'rich_contact_rate': round(rate_rich, 3),
                            'basic_contact_rate': round(rate_basic, 3),
                            'rich_count': rich_total,
                            'basic_count': basic_total,
                            'multiplier': round(rate_rich / rate_basic, 2),
                        },
                    )
                    self._print_insight(insight)
                    created += 1

        return created, skipped

    # =========================================================================
    # RULE 6: Partner Friction
    # =========================================================================

    def _check_partner_friction(self, report, partner_summaries):
        created = 0
        skipped = 0

        # Partners with high interest (expand >= 5) but no contact action
        friction_partners = partner_summaries.filter(
            card_expand_count__gte=5,
            any_contact_action=False,
        ).exclude(partner_id='')

        for es in friction_partners:
            # Use partner_id in the dedup check via data field
            if _insight_exists('partner_friction', report):
                skipped += 1
                continue

            # Look up partner name for a better title
            rp = ReportPartner.objects.filter(
                report=report,
                source_profile_id=es.partner_id,
            ).first()
            partner_name = rp.name if rp else f'partner {es.partner_id[:8]}'

            insight = AnalyticsInsight.objects.create(
                report=report,
                severity='warning',
                category='partner_friction',
                title=f'High interest but no contact: {partner_name}',
                description=(
                    f'{report.member_name} expanded {partner_name}\'s card '
                    f'{es.card_expand_count} times but never took a contact action. '
                    f'There may be missing contact info or the CTA is unclear.'
                ),
                data={
                    'partner_id': es.partner_id,
                    'partner_name': partner_name,
                    'card_expand_count': es.card_expand_count,
                },
            )
            self._print_insight(insight)
            created += 1

        return created, skipped

    # =========================================================================
    # RULE 7 & 8: Engagement Patterns
    # =========================================================================

    def _check_engagement_patterns(self, report, partner_summaries):
        created = 0
        skipped = 0

        # Get partner-level summaries only (exclude page-level)
        partner_level = partner_summaries.exclude(partner_id='')
        total_partners = partner_level.count()

        if total_partners == 0:
            return created, skipped

        contacted = partner_level.filter(any_contact_action=True).count()

        # Rule 7: WARNING — Priority section partners all have no contact action
        priority_partners = ReportPartner.objects.filter(
            report=report, section='priority'
        )
        if priority_partners.exists():
            all_priority_idle = True
            for rp in priority_partners:
                if rp.source_profile_id:
                    es = partner_level.filter(
                        partner_id=str(rp.source_profile_id)
                    ).first()
                    if es and es.any_contact_action:
                        all_priority_idle = False
                        break

            if all_priority_idle:
                if _insight_exists('engagement_pattern', report):
                    skipped += 1
                else:
                    insight = AnalyticsInsight.objects.create(
                        report=report,
                        severity='warning',
                        category='engagement_pattern',
                        title=f'No priority partners contacted',
                        description=(
                            f'{report.member_name} has not taken any contact action on '
                            f'any of the {priority_partners.count()} priority section partners. '
                            f'Consider reordering, improving why-fit text, or sending a nudge.'
                        ),
                        data={
                            'priority_partner_count': priority_partners.count(),
                            'total_contacted': contacted,
                            'total_partners': total_partners,
                        },
                    )
                    self._print_insight(insight)
                    created += 1

        # Rule 8: INFO — >80% of partners have contact action (high engagement)
        if total_partners > 0:
            contact_rate = contacted / total_partners
            if contact_rate > 0.80:
                if _insight_exists('engagement_pattern', report):
                    skipped += 1
                else:
                    insight = AnalyticsInsight.objects.create(
                        report=report,
                        severity='info',
                        category='engagement_pattern',
                        title=f'High engagement: {contact_rate:.0%} contact rate',
                        description=(
                            f'{report.member_name} has contacted {contacted}/{total_partners} '
                            f'partners ({contact_rate:.0%}). This report is performing exceptionally '
                            f'well — consider this member as a case study or for upsell.'
                        ),
                        data={
                            'contact_rate': round(contact_rate, 3),
                            'contacted': contacted,
                            'total': total_partners,
                        },
                    )
                    self._print_insight(insight)
                    created += 1

        return created, skipped

    # =========================================================================
    # RULE 9: Section Attention
    # =========================================================================

    def _check_section_attention(self, report, page_summary):
        created = 0
        skipped = 0

        if not page_summary:
            return created, skipped

        scroll_depth = page_summary.avg_scroll_depth_pct

        if scroll_depth < 50:
            if _insight_exists('section_attention', report):
                skipped += 1
            else:
                insight = AnalyticsInsight.objects.create(
                    report=report,
                    severity='warning',
                    category='section_attention',
                    title=f'Low scroll depth: {scroll_depth}% average',
                    description=(
                        f'{report.member_name} only scrolls to {scroll_depth}% of the report '
                        f'on average. Partners in the lower sections may never be seen. '
                        f'Consider restructuring the report or promoting key partners higher.'
                    ),
                    data={
                        'avg_scroll_depth_pct': scroll_depth,
                    },
                )
                self._print_insight(insight)
                created += 1

        return created, skipped

    # =========================================================================
    # RULE 10: Pending Interventions
    # =========================================================================

    def _check_pending_interventions(self):
        created = 0
        skipped = 0
        now = timezone.now()

        pending = AnalyticsIntervention.objects.filter(
            verified_at__isnull=True,
        )

        for intervention in pending:
            window_end = intervention.created_at + timedelta(
                days=intervention.measurement_window_days
            )
            if window_end >= now:
                # Window hasn't elapsed yet — skip
                continue

            report = intervention.report
            if _insight_exists('intervention_pending', report):
                skipped += 1
                continue

            days_overdue = (now - window_end).days
            insight = AnalyticsInsight.objects.create(
                report=report,
                severity='info',
                category='intervention_pending',
                title=f'Intervention #{intervention.id} ready for verification',
                description=(
                    f'{intervention.get_intervention_type_display()} intervention '
                    f'(created {intervention.created_at.strftime("%Y-%m-%d")}) '
                    f'has passed its {intervention.measurement_window_days}-day window '
                    f'by {days_overdue} days. Run verify_interventions to assess impact.'
                ),
                data={
                    'intervention_id': intervention.id,
                    'intervention_type': intervention.intervention_type,
                    'days_overdue': days_overdue,
                    'measurement_window_days': intervention.measurement_window_days,
                },
            )
            self._print_insight(insight)
            created += 1

        return created, skipped

    # =========================================================================
    # OUTPUT HELPERS
    # =========================================================================

    def _print_insight(self, insight: AnalyticsInsight):
        """Print a formatted insight line with colored severity."""
        severity = insight.severity.upper()
        report_label = f' [Report #{insight.report_id}]' if insight.report_id else ''

        if insight.severity == 'critical':
            style = self.style.ERROR
        elif insight.severity == 'warning':
            style = self.style.WARNING
        else:
            style = self.style.SUCCESS

        self.stdout.write(
            style(f'  [{severity}]') + f' {insight.category}{report_label}: {insight.title}'
        )
