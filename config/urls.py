"""
URL configuration for GTM Engine Platform.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
"""

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

from core.views_health import HealthCheckView, ReadinessCheckView

urlpatterns = [
    # Admin
    path("admin/", admin.site.urls),

    # Health checks
    path('health/', HealthCheckView.as_view(), name='health'),
    path('health/ready/', ReadinessCheckView.as_view(), name='health-ready'),

    # Core app (authentication, dashboard, home)
    path("", include("core.urls")),

    # Feature apps
    path("positioning/", include("positioning.urls", namespace="positioning")),
    path("matching/", include("matching.urls")),
    path("outreach/", include("outreach.urls")),
    path("playbook/", include("playbook.urls")),
]

# Serve static and media files in development
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
