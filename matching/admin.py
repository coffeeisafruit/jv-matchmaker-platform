from django.contrib import admin
from django.utils import timezone
from .models import (
    SupabaseProfile, SupabaseMatch,
    Profile, Match, MatchFeedback, SavedCandidate,
    PartnerRecommendation, MatchLearningSignal,
    MemberReport, ReportPartner, OutreachEvent,
    EngagementSummary, AnalyticsInsight, AnalyticsIntervention,
    ClientVerification, MonthlyProcessingResult, SearchCostLog,
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


# =============================================================================
# DIRECTORY & MATCHING ADMIN
# =============================================================================

@admin.register(SupabaseProfile)
class SupabaseProfileAdmin(admin.ModelAdmin):
    list_display = ['name', 'company', 'email', 'status', 'revenue_tier',
                    'pagerank_score', 'profile_confidence', 'last_active_at']
    list_filter = ['status', 'revenue_tier']
    search_fields = ['name', 'company', 'email', 'niche']
    readonly_fields = [
        'id', 'pagerank_score', 'degree_centrality', 'betweenness_centrality',
        'network_role', 'centrality_updated_at', 'profile_confidence',
        'recommendation_pressure_30d', 'pressure_updated_at',
        'audience_engagement_score', 'social_reach', 'created_at', 'updated_at',
        'last_enriched_at', 'embeddings_updated_at',
    ]


@admin.register(SupabaseMatch)
class SupabaseMatchAdmin(admin.ModelAdmin):
    list_display = ['profile_id', 'suggested_profile_id', 'harmonic_mean',
                    'trust_level', 'status', 'suggested_at']
    list_filter = ['trust_level', 'status']
    search_fields = ['profile_id', 'suggested_profile_id']
    readonly_fields = [
        'id', 'match_score', 'score_ab', 'score_ba', 'harmonic_mean',
        'scale_symmetry_score', 'match_context', 'match_reason',
    ]


@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    list_display = ['name', 'company', 'email', 'industry', 'source', 'created_at']
    list_filter = ['source', 'industry']
    search_fields = ['name', 'company', 'email']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(Match)
class MatchAdmin(admin.ModelAdmin):
    list_display = ['user', 'profile', 'final_score', 'intent_score',
                    'synergy_score', 'momentum_score', 'context_score', 'status']
    list_filter = ['status']
    search_fields = ['user__email', 'profile__name']
    readonly_fields = ['final_score', 'score_breakdown', 'created_at']


@admin.register(MatchFeedback)
class MatchFeedbackAdmin(admin.ModelAdmin):
    list_display = ['match', 'rating', 'outcome', 'created_at']
    list_filter = ['rating', 'outcome']
    readonly_fields = ['created_at']


@admin.register(SavedCandidate)
class SavedCandidateAdmin(admin.ModelAdmin):
    list_display = ['name', 'company', 'niche', 'list_size', 'user', 'created_at']
    search_fields = ['name', 'company', 'niche']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(PartnerRecommendation)
class PartnerRecommendationAdmin(admin.ModelAdmin):
    list_display = ['partner', 'user', 'context', 'was_viewed', 'was_contacted',
                    'feedback_outcome', 'recommended_at']
    list_filter = ['context', 'was_viewed', 'was_contacted', 'feedback_outcome']
    search_fields = ['partner__name', 'user__email']
    readonly_fields = [
        'recommended_at', 'viewed_at', 'contacted_at', 'time_to_first_action',
        'feedback_recorded_at',
    ]


@admin.register(MatchLearningSignal)
class MatchLearningSignalAdmin(admin.ModelAdmin):
    list_display = ['signal_type', 'outcome', 'match_score',
                    'explanation_source', 'created_at']
    list_filter = ['signal_type', 'outcome', 'explanation_source']
    readonly_fields = [
        'match', 'signal_type', 'outcome', 'outcome_timestamp', 'match_score',
        'explanation_source', 'reciprocity_balance', 'confidence_at_generation',
        'signal_details', 'created_at',
    ]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(ClientVerification)
class ClientVerificationAdmin(admin.ModelAdmin):
    list_display = ['client', 'month', 'status', 'sent_at', 'confirmed_at',
                    'reminder_count']
    list_filter = ['status', 'month']
    search_fields = ['client__name', 'client__email']
    readonly_fields = ['verification_token', 'sent_at', 'opened_at', 'confirmed_at']


@admin.register(MonthlyProcessingResult)
class MonthlyProcessingResultAdmin(admin.ModelAdmin):
    list_display = ['client', 'month', 'profiles_enriched', 'profiles_rescored',
                    'matches_above_70', 'gap_detected', 'processing_cost', 'completed_at']
    list_filter = ['month', 'gap_detected', 'report_regenerated']
    search_fields = ['client__name']
    readonly_fields = ['completed_at']


@admin.register(SearchCostLog)
class SearchCostLogAdmin(admin.ModelAdmin):
    list_display = ['tool', 'cost_usd', 'results_returned', 'results_useful',
                    'context', 'created_at']
    list_filter = ['tool']
    search_fields = ['query', 'context', 'profile_id']
    readonly_fields = [
        'tool', 'query', 'cost_usd', 'results_returned', 'results_useful',
        'context', 'profile_id', 'created_at',
    ]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
