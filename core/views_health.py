"""
Health check endpoints for monitoring.

/health/       - Liveness check (always returns 200)
/health/ready/ - Readiness check (verifies DB, API keys, profile count)
"""
import logging

from django.db import connection
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

logger = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name='dispatch')
class HealthCheckView(View):
    """Liveness probe — always returns 200 if Django is running."""

    def get(self, request):
        return JsonResponse({"status": "ok"})


@method_decorator(csrf_exempt, name='dispatch')
class ReadinessCheckView(View):
    """Readiness probe — checks database and critical config."""

    def get(self, request):
        from django.conf import settings

        checks = {}

        # 1. Database connectivity
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            checks["database"] = "ok"
        except Exception as e:
            checks["database"] = f"error: {e}"

        # 2. Required API keys present
        checks["api_keys"] = {
            "ai_key": bool(settings.OPENROUTER_API_KEY or settings.ANTHROPIC_API_KEY),
            "supabase_url": bool(getattr(settings, 'SUPABASE_URL', '')),
            "supabase_key": bool(getattr(settings, 'SUPABASE_KEY', '')),
        }

        # 3. Profile count (basic data sanity)
        try:
            from matching.models import SupabaseProfile
            count = SupabaseProfile.objects.count()
            checks["profile_count"] = count
        except Exception as e:
            checks["profile_count"] = f"error: {e}"

        all_ok = (
            checks["database"] == "ok"
            and checks["api_keys"]["ai_key"]
            and isinstance(checks.get("profile_count"), int)
        )

        return JsonResponse(
            {"status": "ready" if all_ok else "degraded", "checks": checks},
            status=200 if all_ok else 503,
        )
