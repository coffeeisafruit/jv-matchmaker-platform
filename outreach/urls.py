"""
URL patterns for the Outreach module.

Includes routes for:
- PVP Generator and management
- Outreach templates (GEX-style)
- Campaign management
- Clay webhook integration
"""

from django.urls import path

from . import views

app_name = 'outreach'

urlpatterns = [
    # PVP Generator
    path('pvp/generate/', views.PVPGeneratorView.as_view(), name='pvp_generator'),
    path('pvp/', views.PVPListView.as_view(), name='pvp_list'),
    path('pvp/<int:pk>/', views.PVPDetailView.as_view(), name='pvp_detail'),

    # Outreach Templates (GEX-style)
    path('templates/', views.TemplateListView.as_view(), name='template_list'),
    path('templates/create/', views.TemplateCreateView.as_view(), name='template_create'),
    path('templates/<int:pk>/edit/', views.TemplateEditView.as_view(), name='template_edit'),

    # Campaigns
    path('campaigns/', views.CampaignListView.as_view(), name='campaign_list'),
    path('campaigns/create/', views.CampaignCreateView.as_view(), name='campaign_create'),
    path('campaigns/<int:pk>/', views.CampaignDetailView.as_view(), name='campaign_detail'),

    # Webhooks
    path('webhooks/clay/', views.ClayWebhookView.as_view(), name='clay_webhook'),
    path('webhooks/clay/supabase/', views.ClaySupabaseWebhookView.as_view(), name='clay_supabase_webhook'),

    # Outreach Sequences (GEX Email Sequences)
    path('sequences/', views.SequenceListView.as_view(), name='sequence_list'),
    path('sequences/create/', views.SequenceCreateView.as_view(), name='sequence_create'),
    path('sequences/<int:pk>/', views.SequenceDetailView.as_view(), name='sequence_detail'),
    path('sequences/<int:pk>/generate/', views.SequenceGenerateView.as_view(), name='sequence_generate'),

    # Email Integration
    path('email/settings/', views.EmailSettingsView.as_view(), name='email_settings'),
    path('email/oauth/google/', views.GoogleOAuthConnectView.as_view(), name='google_oauth_connect'),
    path('email/oauth/google/callback/', views.GoogleOAuthCallbackView.as_view(), name='google_oauth_callback'),
    path('email/oauth/microsoft/', views.MicrosoftOAuthConnectView.as_view(), name='microsoft_oauth_connect'),
    path('email/oauth/microsoft/callback/', views.MicrosoftOAuthCallbackView.as_view(), name='microsoft_oauth_callback'),
    path('email/disconnect/<int:pk>/', views.EmailDisconnectView.as_view(), name='email_disconnect'),
    path('email/send/', views.SendEmailView.as_view(), name='send_email'),
    path('email/sent/', views.SentEmailListView.as_view(), name='sent_email_list'),
]
