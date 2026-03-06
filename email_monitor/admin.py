from django.contrib import admin
from .models import MonitoredSubscription, InboundEmail, EmailActivitySummary


@admin.register(MonitoredSubscription)
class MonitoredSubscriptionAdmin(admin.ModelAdmin):
    list_display = ['profile', 'monitor_address', 'esp_detected', 'status',
                    'total_emails_received', 'subscribed_at', 'last_email_received_at']
    list_filter = ['status', 'esp_detected', 'discovery_method']
    search_fields = ['profile__name', 'monitor_address', 'signup_url']
    raw_id_fields = ['profile']
    ordering = ['-subscribed_at']
    readonly_fields = ['subscribed_at', 'total_emails_received']


@admin.register(InboundEmail)
class InboundEmailAdmin(admin.ModelAdmin):
    list_display = ['subscription', 'from_name', 'subject_truncated', 'received_at',
                    'analyzed_at', 'has_analysis']
    list_filter = ['analyzed_at']
    search_fields = ['from_address', 'from_name', 'subject']
    raw_id_fields = ['subscription']
    ordering = ['-received_at']
    readonly_fields = ['gmail_message_id', 'received_at', 'analyzed_at']

    def subject_truncated(self, obj):
        return obj.subject[:60]
    subject_truncated.short_description = 'Subject'

    def has_analysis(self, obj):
        return obj.analysis is not None
    has_analysis.boolean = True
    has_analysis.short_description = 'Analyzed'


@admin.register(EmailActivitySummary)
class EmailActivitySummaryAdmin(admin.ModelAdmin):
    list_display = ['profile', 'month', 'emails_sent', 'avg_emails_per_week',
                    'promotional_emails', 'unique_partners_promoted',
                    'mailing_activity_score', 'promotion_willingness_score', 'computed_at']
    list_filter = ['month']
    search_fields = ['profile__name']
    raw_id_fields = ['profile']
    ordering = ['-month', '-mailing_activity_score']
    readonly_fields = ['computed_at']
