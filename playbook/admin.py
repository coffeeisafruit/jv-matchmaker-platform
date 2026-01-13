from django.contrib import admin
from .models import LaunchPlay, GeneratedPlaybook, GeneratedPlay


@admin.register(LaunchPlay)
class LaunchPlayAdmin(admin.ModelAdmin):
    list_display = ['play_number', 'name', 'phase', 'included_in_small', 'included_in_medium', 'included_in_large']
    list_filter = ['phase', 'included_in_small', 'included_in_medium', 'included_in_large']
    search_fields = ['name', 'purpose', 'psychology', 'content_concept']
    ordering = ['play_number']


@admin.register(GeneratedPlaybook)
class GeneratedPlaybookAdmin(admin.ModelAdmin):
    list_display = ['name', 'user', 'size', 'launch_date', 'created_at']
    list_filter = ['size', 'created_at']
    search_fields = ['name', 'user__email', 'user__first_name', 'user__last_name']
    date_hierarchy = 'created_at'
    raw_id_fields = ['user', 'transformation']


@admin.register(GeneratedPlay)
class GeneratedPlayAdmin(admin.ModelAdmin):
    list_display = ['playbook', 'launch_play', 'scheduled_date', 'is_completed', 'completed_at']
    list_filter = ['is_completed', 'scheduled_date', 'launch_play__phase']
    search_fields = ['playbook__name', 'launch_play__name', 'custom_content']
    date_hierarchy = 'scheduled_date'
    raw_id_fields = ['playbook', 'launch_play']
