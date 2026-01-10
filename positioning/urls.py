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

    # Transformation URLs
    path('transformations/', views.TransformationListView.as_view(), name='transformation_list'),
    path('transformations/create/', views.TransformationCreateView.as_view(), name='transformation_create'),
    path('transformations/generate/', views.TransformationGenerateView.as_view(), name='transformation_generate'),
    path('transformations/<int:pk>/', views.TransformationDetailView.as_view(), name='transformation_detail'),
    path('transformations/<int:pk>/delete/', views.TransformationDeleteView.as_view(), name='transformation_delete'),
]
