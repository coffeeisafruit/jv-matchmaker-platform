"""
Core views for GTM Engine Platform.
Handles authentication, dashboard, and home page.
"""

from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import LoginView as DjangoLoginView, LogoutView as DjangoLogoutView
from django.contrib import messages
from django.urls import reverse_lazy

from .forms import SignupForm
from .models import User


class HomeView(TemplateView):
    """Landing page for non-authenticated users."""
    template_name = 'core/home.html'

    def dispatch(self, request, *args, **kwargs):
        # Redirect authenticated users to dashboard
        if request.user.is_authenticated:
            return redirect('core:dashboard')
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Get actual counts from database
        from matching.models import SupabaseProfile
        partners_count = SupabaseProfile.objects.count()
        founders_count = User.objects.count()

        # Set minimum display values for marketing purposes
        # Show actual count if > minimum, otherwise show minimum
        context['partners_count'] = max(partners_count, 3143)
        context['founders_count'] = max(founders_count, 500)

        return context


class DashboardView(LoginRequiredMixin, TemplateView):
    """Main dashboard for authenticated users showing summary stats."""
    template_name = 'core/dashboard.html'
    login_url = reverse_lazy('core:login')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user = self.request.user

        # User stats
        context['matches_this_month'] = user.matches_this_month
        context['pvps_this_month'] = user.pvps_this_month
        context['tier'] = user.get_tier_display()
        context['tier_value'] = user.tier
        context['business_name'] = user.business_name

        # Calculate tier limits based on tier
        tier_limits = {
            'free': {'matches': 5, 'pvps': 10},
            'starter': {'matches': 20, 'pvps': 50},
            'growth': {'matches': 50, 'pvps': 150},
            'pro': {'matches': 200, 'pvps': 500},
        }
        limits = tier_limits.get(user.tier, tier_limits['free'])
        context['max_matches'] = limits['matches']
        context['max_pvps'] = limits['pvps']

        # Calculate progress percentages for progress bars
        context['matches_progress'] = min(100, (user.matches_this_month / limits['matches']) * 100) if limits['matches'] > 0 else 0
        context['pvps_progress'] = min(100, (user.pvps_this_month / limits['pvps']) * 100) if limits['pvps'] > 0 else 0

        # Check if onboarding is completed
        context['onboarding_completed'] = user.onboarding_completed

        # Check if ICP review is due (monthly check-in)
        context['icp_review_due'] = user.is_icp_review_due()

        # Get primary ICP for review prompt
        from positioning.models import ICP
        context['primary_icp'] = ICP.objects.filter(user=user, is_primary=True).first()

        return context


class CustomLoginView(DjangoLoginView):
    """Custom login view using Django's built-in authentication."""
    template_name = 'core/login.html'
    redirect_authenticated_user = True

    def get_success_url(self):
        return reverse_lazy('core:dashboard')

    def form_valid(self, form):
        messages.success(self.request, f'Welcome back, {form.get_user().business_name}!')
        return super().form_valid(form)


class CustomLogoutView(DjangoLogoutView):
    """Custom logout view."""
    next_page = reverse_lazy('core:home')

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            messages.info(request, 'You have been logged out successfully.')
        return super().dispatch(request, *args, **kwargs)


class SignupView(View):
    """Registration view with business_name field."""
    template_name = 'core/signup.html'

    def dispatch(self, request, *args, **kwargs):
        # Redirect authenticated users to dashboard
        if request.user.is_authenticated:
            return redirect('core:dashboard')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        form = SignupForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request):
        form = SignupForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, f'Welcome to JV Matchmaker, {user.business_name}! Let\'s get started.')
            return redirect('core:dashboard')
        return render(request, self.template_name, {'form': form})


class DashboardStatsView(LoginRequiredMixin, View):
    """HTMX endpoint for refreshing dashboard stats."""
    login_url = reverse_lazy('core:login')

    def get(self, request):
        user = request.user

        # Calculate tier limits based on tier
        tier_limits = {
            'free': {'matches': 5, 'pvps': 10},
            'starter': {'matches': 20, 'pvps': 50},
            'growth': {'matches': 50, 'pvps': 150},
            'pro': {'matches': 200, 'pvps': 500},
        }
        limits = tier_limits.get(user.tier, tier_limits['free'])

        context = {
            'matches_this_month': user.matches_this_month,
            'pvps_this_month': user.pvps_this_month,
            'tier': user.get_tier_display(),
            'tier_value': user.tier,
            'max_matches': limits['matches'],
            'max_pvps': limits['pvps'],
            'matches_progress': min(100, (user.matches_this_month / limits['matches']) * 100) if limits['matches'] > 0 else 0,
            'pvps_progress': min(100, (user.pvps_this_month / limits['pvps']) * 100) if limits['pvps'] > 0 else 0,
        }
        return render(request, 'core/partials/stats_cards.html', context)


class ICPReviewView(LoginRequiredMixin, View):
    """Handle ICP review confirmation."""
    login_url = reverse_lazy('core:login')

    def post(self, request):
        """Mark the ICP as reviewed."""
        user = request.user
        user.mark_icp_reviewed()
        messages.success(request, 'Great! Your ICP has been confirmed. We\'ll check in again next month.')
        return redirect('core:dashboard')
