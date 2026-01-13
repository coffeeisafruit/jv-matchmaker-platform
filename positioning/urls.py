"""
URL configuration for the Positioning app.
"""

from django.urls import path
from . import views

app_name = 'positioning'

urlpatterns = [
    # Index (redirects to ICP list)
    path('', views.ICPListView.as_view(), name='index'),

    # ICP URLs
    path('icps/', views.ICPListView.as_view(), name='icp_list'),
    path('icps/create/', views.ICPCreateView.as_view(), name='icp_create'),
    path('icps/<int:pk>/', views.ICPDetailView.as_view(), name='icp_detail'),
    path('icps/<int:pk>/update/', views.ICPUpdateView.as_view(), name='icp_update'),
    path('icps/<int:pk>/delete/', views.ICPDeleteView.as_view(), name='icp_delete'),
    path('icps/<int:pk>/set-primary/', views.ICPSetPrimaryView.as_view(), name='icp_set_primary'),

    # AI Suggestion Generation (HTMX endpoint)
    path('icps/generate-suggestions/', views.GenerateAISuggestionsView.as_view(), name='generate_suggestions'),

    # Transformation URLs
    path('transformations/', views.TransformationListView.as_view(), name='transformation_list'),
    path('transformations/create/', views.TransformationCreateView.as_view(), name='transformation_create'),
    path('transformations/generate/', views.TransformationGenerateView.as_view(), name='transformation_generate'),
    path('transformations/generate-draft/', views.GenerateTransformationDraftView.as_view(), name='generate_transformation_draft'),
    path('transformations/<int:pk>/', views.TransformationDetailView.as_view(), name='transformation_detail'),
    path('transformations/<int:pk>/delete/', views.TransformationDeleteView.as_view(), name='transformation_delete'),

    # Lead Magnet URLs
    path('lead-magnets/', views.LeadMagnetListView.as_view(), name='lead_magnet_list'),
    path('lead-magnets/generate/', views.LeadMagnetGenerateView.as_view(), name='lead_magnet_generate'),
    path('lead-magnets/<int:pk>/', views.LeadMagnetDetailView.as_view(), name='lead_magnet_detail'),
]
