from django.contrib import admin
from .models import MemberReport, ReportPartner


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
