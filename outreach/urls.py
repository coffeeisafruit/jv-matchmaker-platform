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
]
