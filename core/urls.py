"""
URL configuration for the core app.
Handles authentication routes and main navigation.
"""

from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    # Home and Dashboard
    path('', views.HomeView.as_view(), name='home'),
    path('dashboard/', views.DashboardView.as_view(), name='dashboard'),

    # Authentication
    path('login/', views.CustomLoginView.as_view(), name='login'),
    path('logout/', views.CustomLogoutView.as_view(), name='logout'),
    path('signup/', views.SignupView.as_view(), name='signup'),

    # HTMX endpoints
    path('dashboard/stats/', views.DashboardStatsView.as_view(), name='dashboard_stats'),
]
