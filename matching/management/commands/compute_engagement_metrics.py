"""
Compute engagement metrics from raw OutreachEvent records.

Aggregates page-level and partner-level engagement data into
EngagementSummary records for each active report.

Usage:
    python manage.py compute_engagement_metrics
    python manage.py compute_engagement_metrics --report-id 42
"""

from collections import defaultdict
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from matching.models import (
    EngagementSummary,
    MemberReport,
    OutreachEvent,
    ReportPartner,
)


class Command(BaseCommand):
    help = 'Compute engagement metrics from OutreachEvent into EngagementSummary'

    def add_arguments(self, parser):
        parser.add_argument(
            '--report-id',
            type=int,
            default=None,
            help='Process a single report by ID (default: all active reports)',
        )

    def handle(self, *args, **options):
        report_id = options['report_id']
        now = timezone.now()

        # Determine which reports to process
        if report_id:
            reports = MemberReport.objects.filter(id=report_id)
            if not reports.exists():
                self.stderr.write(self.style.ERROR(
                    f'Report {report_id} not found.'
                ))
                return
        else:
            reports = MemberReport.objects.filter(is_active=True)

        total_reports = reports.count()
        self.stdout.write(self.style.NOTICE(
            f'Processing {total_reports} report(s)...'
        ))

        for report in reports:
            events = list(
                OutreachEvent.objects.filter(report_id=report.id)
            )
            if not events:
                self.stdout.write(self.style.WARNING(
                    f'  [{report.id}] {report.member_name}: 0 events, skipping.'
                ))
                continue

            self._compute_page_level(report, events, now)
            partner_ids = self._compute_partner_level(report, events, now)
            self._print_report_summary(report, events, partner_ids, now)

        self.stdout.write(self.style.SUCCESS(
            f'\nDone. Processed {total_reports} report(s).'
        ))

    # -----------------------------------------------------------------
    # Page-level aggregation (partner_id='')
    # -----------------------------------------------------------------

    def _compute_page_level(
        self,
        report: MemberReport,
        events: list,
        now,
    ) -> None:
        session_ids = {
            e.session_id for e in events
            if e.session_id
        }
        total_sessions = len(session_ids)

        page_loads = [e for e in events if e.event_type == 'page_load']
        total_page_views = len(page_loads)

        # Time on page from page_exit events
        page_exits = [e for e in events if e.event_type == 'page_exit']
        time_vals = []
        scroll_vals = []
        for e in page_exits:
            details = e.details or {}
            if 'time_on_page_secs' in details:
                try:
                    time_vals.append(int(details['time_on_page_secs']))
                except (TypeError, ValueError):
                    pass
            if 'scroll_depth_pct' in details:
                try:
                    scroll_vals.append(int(details['scroll_depth_pct']))
                except (TypeError, ValueError):
                    pass

        avg_time = int(sum(time_vals) / len(time_vals)) if time_vals else 0
        avg_scroll = int(sum(scroll_vals) / len(scroll_vals)) if scroll_vals else 0

        last_visit_at = None
        if page_loads:
            last_visit_at = max(e.created_at for e in page_loads)

        days_since = None
        if last_visit_at:
            days_since = (now - last_visit_at).days

        return_visit_count = max(total_sessions - 1, 0)

        template_opens = sum(
            1 for e in events if e.event_type == 'template_open'
        )
        template_copies = sum(
            1 for e in events if e.event_type == 'template_copy'
        )

        EngagementSummary.objects.update_or_create(
            report=report,
            partner_id='',
            defaults={
                'total_sessions': total_sessions,
                'total_page_views': total_page_views,
                'avg_time_on_page_secs': avg_time,
                'avg_scroll_depth_pct': avg_scroll,
                'last_visit_at': last_visit_at,
                'days_since_last_visit': days_since,
                'return_visit_count': return_visit_count,
                'template_open_count': template_opens,
                'template_copy_count': template_copies,
            },
        )

    # -----------------------------------------------------------------
    # Partner-level aggregation
    # -----------------------------------------------------------------

    def _compute_partner_level(
        self,
        report: MemberReport,
        events: list,
        now,
    ) -> set:
        """Compute per-partner metrics. Returns set of unique partner_ids processed."""
        # Group events by partner_id (skip empty/None)
        by_partner = defaultdict(list)
        for e in events:
            if e.partner_id:
                by_partner[e.partner_id].append(e)

        # Find earliest page_load for time_to_first_action calculation
        page_loads = [e for e in events if e.event_type == 'page_load']
        first_page_load_at = (
            min(e.created_at for e in page_loads) if page_loads else None
        )

        for partner_id, pevents in by_partner.items():
            card_expands = sum(
                1 for e in pevents if e.event_type == 'card_expand'
            )

            # Dwell time from card_collapse events
            dwell_vals = []
            for e in pevents:
                if e.event_type == 'card_collapse':
                    details = e.details or {}
                    if 'dwell_time_ms' in details:
                        try:
                            dwell_vals.append(int(details['dwell_time_ms']))
                        except (TypeError, ValueError):
                            pass
            avg_dwell = (
                int(sum(dwell_vals) / len(dwell_vals)) if dwell_vals else 0
            )

            was_checked = any(
                e.event_type == 'contact_done' for e in pevents
            )

            # Link click counts by type
            link_clicks = [
                e for e in pevents if e.event_type == 'link_click'
            ]
            email_clicks = sum(
                1 for e in link_clicks
                if (e.details or {}).get('link_type') == 'email'
            )
            linkedin_clicks = sum(
                1 for e in link_clicks
                if (e.details or {}).get('link_type') == 'linkedin'
            )
            schedule_clicks = sum(
                1 for e in link_clicks
                if (e.details or {}).get('link_type') == 'schedule'
            )
            apply_clicks = sum(
                1 for e in link_clicks
                if (e.details or {}).get('link_type') == 'apply'
            )

            total_contact = (
                email_clicks + linkedin_clicks + schedule_clicks + apply_clicks
            )
            any_contact = total_contact > 0

            # Contact action events for first_action_at
            contact_events = [
                e for e in link_clicks
                if (e.details or {}).get('link_type') in (
                    'email', 'linkedin', 'schedule', 'apply',
                )
            ]
            first_action_at = None
            time_to_first = None
            if contact_events:
                first_action_at = min(e.created_at for e in contact_events)
                if first_page_load_at:
                    delta = (first_action_at - first_page_load_at).total_seconds()
                    time_to_first = max(int(delta), 0)

            EngagementSummary.objects.update_or_create(
                report=report,
                partner_id=partner_id,
                defaults={
                    'card_expand_count': card_expands,
                    'avg_card_dwell_ms': avg_dwell,
                    'was_checked': was_checked,
                    'email_click_count': email_clicks,
                    'linkedin_click_count': linkedin_clicks,
                    'schedule_click_count': schedule_clicks,
                    'apply_click_count': apply_clicks,
                    'any_contact_action': any_contact,
                    'first_action_at': first_action_at,
                    'time_to_first_action_secs': time_to_first,
                },
            )

        return set(by_partner.keys())

    # -----------------------------------------------------------------
    # Summary output
    # -----------------------------------------------------------------

    def _print_report_summary(
        self,
        report: MemberReport,
        events: list,
        partner_ids: set,
        now,
    ) -> None:
        session_ids = {e.session_id for e in events if e.session_id}
        total_sessions = len(session_ids)

        # Count partners that had any contact action
        contacted = EngagementSummary.objects.filter(
            report=report,
            any_contact_action=True,
        ).exclude(partner_id='').count()

        total_partners = ReportPartner.objects.filter(report=report).count()

        self.stdout.write(self.style.SUCCESS(
            f'\n  [{report.id}] {report.member_name}'
        ))
        self.stdout.write(
            f'    Sessions: {total_sessions} | '
            f'Partners contacted: {contacted}/{total_partners}'
        )

        # ------- Score accuracy check -------
        self.stdout.write(self.style.NOTICE('    -- Score Accuracy Check --'))
        partners = ReportPartner.objects.filter(report=report)
        buckets = self._bucket_by_score(partners)
        for label, rp_list in buckets:
            if not rp_list:
                continue
            rp_partner_ids = set()
            for rp in rp_list:
                if rp.source_profile_id:
                    rp_partner_ids.add(str(rp.source_profile_id))
            engaged = EngagementSummary.objects.filter(
                report=report,
                partner_id__in=rp_partner_ids,
                any_contact_action=True,
            ).count()
            rate = (
                f'{engaged}/{len(rp_list)} '
                f'({100 * engaged // len(rp_list)}%)'
            )
            self.stdout.write(f'      {label}: {rate}')

        # ------- Enrichment quality check -------
        self.stdout.write(self.style.NOTICE('    -- Enrichment Quality Check --'))
        groups = self._bucket_by_why_fit(partners)
        for label, rp_list in groups:
            if not rp_list:
                continue
            rp_partner_ids = set()
            for rp in rp_list:
                if rp.source_profile_id:
                    rp_partner_ids.add(str(rp.source_profile_id))
            engaged_count = EngagementSummary.objects.filter(
                report=report,
                partner_id__in=rp_partner_ids,
            ).exclude(partner_id='')
            total_expands = sum(s.card_expand_count for s in engaged_count)
            total_contacts = engaged_count.filter(any_contact_action=True).count()
            self.stdout.write(
                f'      {label} ({len(rp_list)} partners): '
                f'expands={total_expands}, contacted={total_contacts}'
            )

    # -----------------------------------------------------------------
    # Bucketing helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _bucket_by_score(partners) -> list:
        """Group ReportPartners into score buckets."""
        buckets = {
            '90-100': [],
            '80-89': [],
            '70-79': [],
            '60-69': [],
            '<60': [],
            'unscored': [],
        }
        for rp in partners:
            score = rp.match_score
            if score is None:
                buckets['unscored'].append(rp)
            elif score >= 90:
                buckets['90-100'].append(rp)
            elif score >= 80:
                buckets['80-89'].append(rp)
            elif score >= 70:
                buckets['70-79'].append(rp)
            elif score >= 60:
                buckets['60-69'].append(rp)
            else:
                buckets['<60'].append(rp)
        return [
            ('90-100', buckets['90-100']),
            ('80-89', buckets['80-89']),
            ('70-79', buckets['70-79']),
            ('60-69', buckets['60-69']),
            ('<60', buckets['<60']),
            ('unscored', buckets['unscored']),
        ]

    @staticmethod
    def _bucket_by_why_fit(partners) -> list:
        """Group ReportPartners by why_fit text length as a quality proxy."""
        buckets = {
            'detailed (200+ chars)': [],
            'moderate (50-199 chars)': [],
            'minimal (<50 chars)': [],
            'empty': [],
        }
        for rp in partners:
            length = len(rp.why_fit or '')
            if length == 0:
                buckets['empty'].append(rp)
            elif length < 50:
                buckets['minimal (<50 chars)'].append(rp)
            elif length < 200:
                buckets['moderate (50-199 chars)'].append(rp)
            else:
                buckets['detailed (200+ chars)'].append(rp)
        return [
            ('detailed (200+ chars)', buckets['detailed (200+ chars)']),
            ('moderate (50-199 chars)', buckets['moderate (50-199 chars)']),
            ('minimal (<50 chars)', buckets['minimal (<50 chars)']),
            ('empty', buckets['empty']),
        ]
