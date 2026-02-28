"""
URL patterns for the JV Matcher module.

Handles partner matching, scoring, and profile management.
"""

from django.urls import path
from django.views.generic import RedirectView

from . import views
from . import views_verification

app_name = 'matching'

urlpatterns = [
    # Index redirects to partner database (Supabase)
    path('', RedirectView.as_view(pattern_name='matching:partners', permanent=False), name='index'),

    # Supabase Partner Database (3,143+ profiles)
    path('partners/', views.SupabaseProfileListView.as_view(), name='partners'),
    path('partners/<uuid:pk>/', views.SupabaseProfileDetailView.as_view(), name='partner-detail'),

    # User's Own Profiles (legacy)
    path('profiles/', views.ProfileListView.as_view(), name='profile-list'),
    path('profiles/create/', views.ProfileCreateView.as_view(), name='profile-create'),
    path('profiles/import/', views.ProfileImportView.as_view(), name='profile-import'),
    path('profiles/<int:pk>/', views.ProfileDetailView.as_view(), name='profile-detail'),
    path('profiles/<int:pk>/edit/', views.ProfileUpdateView.as_view(), name='profile-edit'),
    path('profiles/<int:pk>/delete/', views.ProfileDeleteView.as_view(), name='profile-delete'),

    # Match URLs
    path('matches/', views.MatchListView.as_view(), name='match-list'),
    path('matches/<int:pk>/', views.MatchDetailView.as_view(), name='match-detail'),
    path('matches/<int:pk>/status/', views.MatchUpdateStatusView.as_view(), name='match-update-status'),

    # Score calculation URLs (HTMX endpoints)
    path('calculate/<int:pk>/', views.CalculateMatchView.as_view(), name='calculate-match'),
    path('calculate/bulk/', views.CalculateBulkMatchView.as_view(), name='calculate-bulk'),
    path('calculate/recalculate-all/', views.RecalculateAllMatchesView.as_view(), name='recalculate-all'),

    # Member Reports (code-gated, no login required)
    path('report/', views.ReportAccessView.as_view(), name='report-access'),
    path('report/<int:report_id>/', views.ReportHubView.as_view(), name='report-hub'),
    path('report/<int:report_id>/outreach/', views.ReportOutreachView.as_view(), name='report-outreach'),
    path('report/<int:report_id>/profile/', views.ReportProfileView.as_view(), name='report-profile'),
    path('report/<int:report_id>/profile/edit/', views.ReportProfileEditView.as_view(), name='report-profile-edit'),
    path('report/<int:report_id>/profile/confirm/', views.ReportProfileConfirmView.as_view(), name='report-profile-confirm'),

    # Demo (promotional mock, no login)
    path('demo/', views.DemoReportView.as_view(), name='demo-report'),
    path('demo/outreach/', views.DemoOutreachView.as_view(), name='demo-outreach'),
    path('demo/profile/', views.DemoProfileView.as_view(), name='demo-profile'),

    # Apollo.io webhook (receives async phone/email data)
    path('api/apollo/webhook/', views.ApolloWebhookView.as_view(), name='apollo-webhook'),

    # Contact ingestion webhook
    path('api/contacts/ingest/', views.ContactIngestionWebhookView.as_view(), name='contact-ingest-webhook'),

    # Analytics dashboard (login required)
    path('analytics/', views.AnalyticsDashboardView.as_view(), name='analytics-dashboard'),
    path('analytics/report/<int:report_id>/', views.AnalyticsReportDetailView.as_view(), name='analytics-report-detail'),
    path('analytics/insights/', views.AnalyticsInsightsView.as_view(), name='analytics-insights'),
    path('analytics/insights/<int:insight_id>/dismiss/', views.AnalyticsInsightDismissView.as_view(), name='analytics-insight-dismiss'),

    # Pipeline stats API (for architecture_diagram.html live data)
    path('api/pipeline-stats/', views.PipelineStatsAPIView.as_view(), name='pipeline-stats-api'),

    # Client profile verification (token-gated, no login required)
    path('verify/<uuid:token>/', views_verification.ProfileVerificationView.as_view(), name='verification-form'),
    path('verify/<uuid:token>/submit/', views_verification.ProfileVerificationSubmitView.as_view(), name='verification-submit'),
    path('verify/<uuid:token>/done/', views_verification.VerificationStatusView.as_view(), name='verification-done'),
]
