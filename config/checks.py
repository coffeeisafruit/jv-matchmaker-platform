"""
Django system checks for required configuration.

Runs automatically on `manage.py runserver`, `migrate`, and `check`.
"""
import os

from django.conf import settings
from django.core.checks import Error, Warning, register


@register()
def check_required_settings(app_configs, **kwargs):
    errors = []

    # E001: Must have at least one AI API key
    if not settings.OPENROUTER_API_KEY and not settings.ANTHROPIC_API_KEY:
        errors.append(Error(
            "No AI API key configured.",
            hint="Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY in .env",
            id="jv.E001",
        ))

    # E002: DATABASE_URL required in production
    if not settings.DEBUG and not os.environ.get("DATABASE_URL"):
        errors.append(Error(
            "DATABASE_URL not set in production.",
            hint="Set DATABASE_URL for PostgreSQL connection.",
            id="jv.E002",
        ))

    # E003: Insecure SECRET_KEY in production
    if not settings.DEBUG and "insecure" in settings.SECRET_KEY:
        errors.append(Error(
            "SECRET_KEY contains 'insecure' — not safe for production.",
            hint="Generate a secure SECRET_KEY.",
            id="jv.E003",
        ))

    # W001: Supabase credentials recommended
    if not getattr(settings, 'SUPABASE_URL', '') or not getattr(settings, 'SUPABASE_KEY', ''):
        errors.append(Warning(
            "Supabase credentials not configured.",
            hint="Set SUPABASE_URL and SUPABASE_KEY for tracking features.",
            id="jv.W001",
        ))

    return errors
