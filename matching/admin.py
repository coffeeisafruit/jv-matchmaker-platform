from django.contrib import admin
from django.utils import timezone
from .models import (
    MemberReport, ReportPartner, OutreachEvent,
    EngagementSummary, AnalyticsInsight, AnalyticsIntervention,
)


class ReportPartnerInline(admin.TabularInline):
    model = ReportPartner
    extra = 0
    fields = ['rank', 'section', 'name', 'company', 'email', 'linkedin',
              'list_size', 'badge', 'match_score']
    readonly_fields = ['match_score']
    ordering = ['section', 'rank']


@admin.register(MemberReport)
class MemberReportAdmin(admin.ModelAdmin):
    list_display = ['member_name', 'company_name', 'access_code', 'month',
                    'is_active', 'is_expired_display', 'access_count']
    list_filter = ['is_active', 'month']
    search_fields = ['member_name', 'company_name', 'access_code', 'member_email']
    readonly_fields = ['created_at', 'last_accessed_at', 'access_count']
    inlines = [ReportPartnerInline]
    actions = ['generate_new_codes', 'deactivate_reports']

    def is_expired_display(self, obj):
        return obj.is_expired
    is_expired_display.boolean = True
    is_expired_display.short_description = 'Expired?'

    def generate_new_codes(self, request, queryset):
        import secrets
        for report in queryset:
            report.access_code = secrets.token_hex(4).upper()
            report.save(update_fields=['access_code'])
        self.message_user(request, f'Generated new codes for {queryset.count()} reports.')
    generate_new_codes.short_description = 'Generate new access codes'

    def deactivate_reports(self, request, queryset):
        queryset.update(is_active=False)
        self.message_user(request, f'Deactivated {queryset.count()} reports.')
    deactivate_reports.short_description = 'Deactivate selected reports'


@admin.register(ReportPartner)
class ReportPartnerAdmin(admin.ModelAdmin):
    list_display = ['name', 'company', 'report', 'section', 'rank', 'email', 'match_score']
    list_filter = ['section', 'report__month']
    search_fields = ['name', 'company']


# =============================================================================
# ENGAGEMENT ANALYTICS ADMIN
# =============================================================================

@admin.register(OutreachEvent)
class OutreachEventAdmin(admin.ModelAdmin):
    list_display = ['event_type', 'report_id', 'partner_id', 'session_id', 'created_at']
    list_filter = ['event_type']
    search_fields = ['session_id', 'partner_id']
    readonly_fields = [
        'report_id', 'access_code', 'event_type', 'partner_id',
        'details', 'session_id', 'created_at',
    ]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(EngagementSummary)
class EngagementSummaryAdmin(admin.ModelAdmin):
    list_display = [
        'report', 'partner_id', 'any_contact_action', 'card_expand_count',
        'email_click_count', 'linkedin_click_count', 'computed_at',
    ]
    list_filter = ['any_contact_action', 'was_checked']
    search_fields = ['partner_id']
    readonly_fields = ['computed_at']


@admin.register(AnalyticsInsight)
class AnalyticsInsightAdmin(admin.ModelAdmin):
    list_display = ['severity', 'category', 'title', 'report', 'is_active', 'is_dismissed', 'created_at']
    list_filter = ['severity', 'category', 'is_active', 'is_dismissed']
    search_fields = ['title', 'description']
    readonly_fields = ['created_at']
    actions = ['dismiss_insights', 'reactivate_insights']

    def dismiss_insights(self, request, queryset):
        queryset.update(is_dismissed=True, dismissed_at=timezone.now())
        self.message_user(request, f'Dismissed {queryset.count()} insights.')
    dismiss_insights.short_description = 'Dismiss selected insights'

    def reactivate_insights(self, request, queryset):
        queryset.update(is_dismissed=False, dismissed_at=None)
        self.message_user(request, f'Reactivated {queryset.count()} insights.')
    reactivate_insights.short_description = 'Reactivate selected insights'


@admin.register(AnalyticsIntervention)
class AnalyticsInterventionAdmin(admin.ModelAdmin):
    list_display = [
        'intervention_type', 'report', 'impact_assessment',
        'verified_at', 'created_at',
    ]
    list_filter = ['intervention_type', 'impact_assessment']
    search_fields = ['description']
    readonly_fields = [
        'baseline_metrics', 'followup_metrics', 'verified_at',
        'impact_assessment', 'impact_details', 'created_at',
    ]
    actions = ['capture_baseline', 'run_verification']

    def capture_baseline(self, request, queryset):
        """Snapshot current engagement metrics as baseline for selected interventions."""
        from .models import EngagementSummary as ES
        captured = 0
        for intervention in queryset.filter(baseline_metrics={}):
            if not intervention.report_id:
                continue
            page_summary = ES.objects.filter(
                report=intervention.report, partner_id=''
            ).first()
            partner_summaries = ES.objects.filter(
                report=intervention.report
            ).exclude(partner_id='')
            total = partner_summaries.count()
            contacted = partner_summaries.filter(any_contact_action=True).count()

            intervention.baseline_metrics = {
                'captured_at': timezone.now().isoformat(),
                'sessions_total': page_summary.total_sessions if page_summary else 0,
                'contact_rate_overall': round(contacted / total, 3) if total else 0,
                'avg_card_dwell_ms': page_summary.avg_card_dwell_ms if page_summary else 0,
                'template_copy_count': page_summary.template_copy_count if page_summary else 0,
                'scroll_depth_avg_pct': page_summary.avg_scroll_depth_pct if page_summary else 0,
                'partners_total': total,
                'partners_contacted': contacted,
            }
            intervention.save(update_fields=['baseline_metrics'])
            captured += 1
        self.message_user(request, f'Captured baseline for {captured} interventions.')
    capture_baseline.short_description = 'Capture baseline metrics now'

    def run_verification(self, request, queryset):
        """Trigger verification for selected interventions."""
        from django.core.management import call_command
        verified = 0
        for intervention in queryset.filter(verified_at__isnull=True):
            try:
                call_command('verify_interventions', intervention_id=intervention.id)
                verified += 1
            except Exception:
                pass
        self.message_user(request, f'Verified {verified} interventions.')
    run_verification.short_description = 'Run verification now'
