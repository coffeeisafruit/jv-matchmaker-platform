"""
URL configuration for the Playbook app.
"""

from django.urls import path
from . import views

app_name = 'playbook'

urlpatterns = [
    path('', views.PlaybookListView.as_view(), name='list'),
    path('create/', views.PlaybookCreateView.as_view(), name='create'),
    path('<int:pk>/', views.PlaybookDetailView.as_view(), name='detail'),
    path('<int:pk>/generate/', views.PlaybookGenerateView.as_view(), name='generate'),
    path('play/<int:pk>/complete/', views.PlayMarkCompleteView.as_view(), name='mark_complete'),
]
