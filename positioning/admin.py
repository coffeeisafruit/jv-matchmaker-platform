from django.contrib import admin
from .models import ICP, TransformationAnalysis, PainSignal, LeadMagnetConcept


@admin.register(ICP)
class ICPAdmin(admin.ModelAdmin):
    list_display = ['name', 'user', 'customer_type', 'industry', 'is_primary', 'created_at']
    list_filter = ['customer_type', 'is_primary', 'created_at']
    search_fields = ['name', 'industry', 'user__username', 'user__business_name']
    ordering = ['-is_primary', '-created_at']


@admin.register(TransformationAnalysis)
class TransformationAnalysisAdmin(admin.ModelAdmin):
    list_display = ['user', 'icp', 'get_summary_preview', 'ai_generated', 'created_at']
    list_filter = ['ai_generated', 'created_at']
    search_fields = ['user__username', 'transformation_summary']
    ordering = ['-created_at']

    def get_summary_preview(self, obj):
        return obj.transformation_summary[:50] + '...' if len(obj.transformation_summary) > 50 else obj.transformation_summary
    get_summary_preview.short_description = 'Summary'


@admin.register(PainSignal)
class PainSignalAdmin(admin.ModelAdmin):
    list_display = ['signal_type', 'user', 'description_preview', 'weight', 'is_active']
    list_filter = ['signal_type', 'is_active']
    search_fields = ['description', 'user__username']
    ordering = ['-weight', 'signal_type']

    def description_preview(self, obj):
        return obj.description[:50] + '...' if len(obj.description) > 50 else obj.description
    description_preview.short_description = 'Description'


@admin.register(LeadMagnetConcept)
class LeadMagnetConceptAdmin(admin.ModelAdmin):
    list_display = ['title', 'user', 'format_suggestion', 'is_selected', 'transformation', 'created_at']
    list_filter = ['format_suggestion', 'is_selected', 'created_at']
    search_fields = ['title', 'user__username', 'target_problem', 'what_description']
    ordering = ['-created_at']
    readonly_fields = ['created_at']

    fieldsets = (
        ('Basic Information', {
            'fields': ('user', 'transformation', 'title', 'format_suggestion')
        }),
        ('Content', {
            'fields': ('what_description', 'why_description', 'target_problem', 'hook')
        }),
        ('Metadata', {
            'fields': ('estimated_creation_time', 'is_selected', 'created_at')
        }),
    )
