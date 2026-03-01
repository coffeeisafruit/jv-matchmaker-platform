"""
Views for the JV Matcher module.

Provides profile management, match listing, and score calculation endpoints.
"""

import csv
import io
import json
import os
import re
import time

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import models
from django.db.models import Count, Max, Min, Q, Sum
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy
from django.views import View
from django.views.generic import (
    CreateView,
    DetailView,
    FormView,
    ListView,
    TemplateView,
    UpdateView,
)

from .forms import ProfileForm, ProfileImportForm, MatchStatusForm
from .models import Match, MemberReport, Profile, SupabaseProfile, SupabaseMatch
from .services import MatchScoringService, PartnershipAnalyzer
from positioning.models import ICP, TransformationAnalysis


# =============================================================================
# SUPABASE PROFILE VIEWS (Browse the 3,143+ partner database)
# =============================================================================

class SupabaseProfileListView(LoginRequiredMixin, ListView):
    """
    Browse all JV partner profiles from Supabase database.
    """
    model = SupabaseProfile
    template_name = 'matching/supabase_profile_list.html'
    context_object_name = 'profiles'
    paginate_by = 25

    def get_queryset(self):
        """Apply search and filters to Supabase profiles."""
        queryset = SupabaseProfile.objects.all()

        # Search filter
        search = self.request.GET.get('search', '').strip()
        if search:
            queryset = queryset.filter(
                Q(name__icontains=search) |
                Q(company__icontains=search) |
                Q(niche__icontains=search) |
                Q(business_focus__icontains=search) |
                Q(what_you_do__icontains=search) |
                Q(who_you_serve__icontains=search)
            )

        # Niche filter
        niche = self.request.GET.get('niche', '').strip()
        if niche:
            queryset = queryset.filter(niche__icontains=niche)

        # Status filter (Member, Non Member Resource, Pending)
        status = self.request.GET.get('status', '').strip()
        if status:
            queryset = queryset.filter(status=status)

        return queryset.order_by('-last_active_at')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['total_profiles'] = SupabaseProfile.objects.count()
        context['current_search'] = self.request.GET.get('search', '')
        context['current_niche'] = self.request.GET.get('niche', '')
        context['current_status'] = self.request.GET.get('status', '')

        # Get user's primary ICP for smart suggestions
        primary_icp = ICP.objects.filter(
            user=self.request.user,
            is_primary=True
        ).first()

        if not primary_icp:
            # Fall back to most recent ICP
            primary_icp = ICP.objects.filter(user=self.request.user).first()

        context['icp'] = primary_icp

        # Get user's transformation analysis for solution fit
        transformation = TransformationAnalysis.objects.filter(
            user=self.request.user
        ).first()

        # Find user's SupabaseProfile by email to get personalized matches
        user_supabase_profile = SupabaseProfile.objects.filter(
            email__iexact=self.request.user.email
        ).first()

        context['user_supabase_profile'] = user_supabase_profile

        # Get personalized recommendations from pre-computed matches
        if user_supabase_profile:
            recommended, matches_by_id = self._get_user_matches_with_data(
                user_supabase_profile
            )
            context['recommended_partners'] = recommended
        else:
            recommended = self._get_featured_partners(icp=primary_icp)
            matches_by_id = {}
            context['recommended_partners'] = recommended

        # Create PartnershipAnalyzer and enrich partner data with insights
        analyzer = PartnershipAnalyzer(
            user=self.request.user,
            user_supabase_profile=user_supabase_profile,
            icp=primary_icp,
            transformation=transformation,
        )

        # Analyze recommended partners with dynamic insights
        if recommended:
            partner_analyses = analyzer.analyze_batch(
                partners=recommended,
                matches_by_partner_id=matches_by_id
            )
            context['partner_analyses'] = partner_analyses
        else:
            context['partner_analyses'] = []

        return context

    def _get_user_matches(self, user_profile):
        """
        Get personalized partner recommendations from pre-computed SupabaseMatch.
        These matches are based on seeking→offering algorithm (you need what they offer).
        """
        partners, _ = self._get_user_matches_with_data(user_profile)
        return partners

    def _get_user_matches_with_data(self, user_profile):
        """
        Get personalized partner recommendations with match data.

        Returns:
            Tuple of (partners list, matches_by_partner_id dict)
        """
        # Get this user's top matches ordered by harmonic_mean score
        matches = SupabaseMatch.objects.filter(
            profile_id=user_profile.id,
            harmonic_mean__isnull=False,
            harmonic_mean__gt=0
        ).order_by('-harmonic_mean')[:8]

        if not matches:
            return self._get_featured_partners(), {}

        # Get the suggested profile IDs in score order
        profile_ids = [m.suggested_profile_id for m in matches]

        # Create lookup of matches by suggested_profile_id
        matches_by_id = {str(m.suggested_profile_id): m for m in matches}

        # Fetch the actual profiles
        profiles = list(SupabaseProfile.objects.filter(
            id__in=profile_ids,
            status='Member'
        ))

        # Re-order by match score order
        id_to_profile = {p.id: p for p in profiles}
        ordered = [id_to_profile[pid] for pid in profile_ids if pid in id_to_profile]
        return ordered[:4], matches_by_id

    def _get_featured_partners(self, icp=None):
        """
        Get featured partners when user has no SupabaseProfile.

        Prioritizes partners that match the user's ICP industry if available.
        """
        from django.db.models import Avg, Count

        # If user has an ICP, first try to find partners matching their industry
        if icp and icp.industry:
            icp_aligned = SupabaseProfile.objects.filter(
                status='Member',
                niche__icontains=icp.industry
            ).exclude(niche__isnull=True).order_by('-list_size')[:4]

            if icp_aligned.exists():
                return list(icp_aligned)

        # Fallback: Get profiles that are frequently recommended with high scores
        top_matched_profile_ids = (
            SupabaseMatch.objects
            .filter(harmonic_mean__isnull=False, harmonic_mean__gt=0)
            .values('suggested_profile_id')
            .annotate(
                avg_score=Avg('harmonic_mean'),
                match_count=Count('id')
            )
            .filter(match_count__gte=3)
            .order_by('-avg_score')[:8]
        )

        profile_ids = [m['suggested_profile_id'] for m in top_matched_profile_ids]

        if profile_ids:
            profiles = list(SupabaseProfile.objects.filter(
                id__in=profile_ids,
                status='Member'
            ).exclude(niche__isnull=True))

            id_to_profile = {p.id: p for p in profiles}
            ordered = [id_to_profile[pid] for pid in profile_ids if pid in id_to_profile]
            return ordered[:4]

        # Ultimate fallback: active members with largest lists
        return SupabaseProfile.objects.filter(
            status='Member',
            list_size__gt=1000
        ).exclude(
            niche__isnull=True
        ).order_by('-list_size')[:4]

    def render_to_response(self, context, **response_kwargs):
        """Return partial template for HTMX requests."""
        if self.request.htmx:
            self.template_name = 'matching/partials/supabase_profile_table.html'
        return super().render_to_response(context, **response_kwargs)


class SupabaseProfileDetailView(LoginRequiredMixin, DetailView):
    """View detailed information about a Supabase profile."""
    model = SupabaseProfile
    template_name = 'matching/supabase_profile_detail.html'
    context_object_name = 'profile'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Get any matches involving this profile
        context['matches_as_source'] = SupabaseMatch.objects.filter(
            profile_id=self.object.id
        ).order_by('-harmonic_mean')[:10]
        context['matches_as_target'] = SupabaseMatch.objects.filter(
            suggested_profile_id=self.object.id
        ).order_by('-harmonic_mean')[:10]

        # Get user context for partnership analysis
        primary_icp = ICP.objects.filter(
            user=self.request.user,
            is_primary=True
        ).first() or ICP.objects.filter(user=self.request.user).first()

        transformation = TransformationAnalysis.objects.filter(
            user=self.request.user
        ).first()

        user_supabase_profile = SupabaseProfile.objects.filter(
            email__iexact=self.request.user.email
        ).first()

        # Get match data for this specific partner
        supabase_match = None
        if user_supabase_profile:
            supabase_match = SupabaseMatch.objects.filter(
                profile_id=user_supabase_profile.id,
                suggested_profile_id=self.object.id
            ).first()

        # Analyze this partner
        analyzer = PartnershipAnalyzer(
            user=self.request.user,
            user_supabase_profile=user_supabase_profile,
            icp=primary_icp,
            transformation=transformation,
        )
        context['partnership_analysis'] = analyzer.analyze(self.object, supabase_match)
        context['icp'] = primary_icp

        return context


# =============================================================================
# DJANGO PROFILE VIEWS (User's own profiles)
# =============================================================================

class ProfileListView(LoginRequiredMixin, ListView):
    """
    List all profiles with search and filter capabilities.

    Supports HTMX for dynamic search and filtering.
    """
    model = Profile
    template_name = 'matching/profile_list.html'
    context_object_name = 'profiles'
    paginate_by = 20

    def get_queryset(self):
        """Filter profiles by current user and apply search/filters."""
        queryset = Profile.objects.filter(user=self.request.user)

        # Search filter
        search = self.request.GET.get('search', '').strip()
        if search:
            queryset = queryset.filter(
                Q(name__icontains=search) |
                Q(company__icontains=search) |
                Q(industry__icontains=search) |
                Q(email__icontains=search)
            )

        # Industry filter
        industry = self.request.GET.get('industry', '').strip()
        if industry:
            queryset = queryset.filter(industry__iexact=industry)

        # Audience size filter
        audience_size = self.request.GET.get('audience_size', '').strip()
        if audience_size:
            queryset = queryset.filter(audience_size=audience_size)

        # Source filter
        source = self.request.GET.get('source', '').strip()
        if source:
            queryset = queryset.filter(source=source)

        return queryset.order_by('-created_at')

    def get_context_data(self, **kwargs):
        """Add filter options to context."""
        context = super().get_context_data(**kwargs)

        # Get unique industries for filter dropdown
        context['industries'] = (
            Profile.objects.filter(user=self.request.user)
            .exclude(industry__isnull=True)
            .exclude(industry='')
            .values_list('industry', flat=True)
            .distinct()
            .order_by('industry')
        )

        # Audience size choices
        context['audience_sizes'] = Profile.AudienceSize.choices

        # Source choices
        context['sources'] = Profile.Source.choices

        # Current filter values
        context['current_search'] = self.request.GET.get('search', '')
        context['current_industry'] = self.request.GET.get('industry', '')
        context['current_audience_size'] = self.request.GET.get('audience_size', '')
        context['current_source'] = self.request.GET.get('source', '')

        return context

    def render_to_response(self, context, **response_kwargs):
        """Return partial template for HTMX requests."""
        if self.request.htmx:
            self.template_name = 'matching/partials/profile_table.html'
        return super().render_to_response(context, **response_kwargs)


class ProfileCreateView(LoginRequiredMixin, CreateView):
    """Create a new profile via manual entry."""
    model = Profile
    form_class = ProfileForm
    template_name = 'matching/profile_form.html'
    success_url = reverse_lazy('matching:profile-list')

    def form_valid(self, form):
        """Set the user and source before saving."""
        form.instance.user = self.request.user
        form.instance.source = Profile.Source.MANUAL
        messages.success(self.request, f'Profile "{form.instance.name}" created successfully.')
        return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'Add New Profile'
        context['submit_text'] = 'Create Profile'
        return context


class ProfileUpdateView(LoginRequiredMixin, UpdateView):
    """Update an existing profile."""
    model = Profile
    form_class = ProfileForm
    template_name = 'matching/profile_form.html'
    success_url = reverse_lazy('matching:profile-list')

    def get_queryset(self):
        """Ensure users can only edit their own profiles."""
        return Profile.objects.filter(user=self.request.user)

    def form_valid(self, form):
        messages.success(self.request, f'Profile "{form.instance.name}" updated successfully.')
        return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = f'Edit Profile: {self.object.name}'
        context['submit_text'] = 'Update Profile'
        return context


class ProfileDetailView(LoginRequiredMixin, DetailView):
    """View profile details with match score if available."""
    model = Profile
    template_name = 'matching/profile_detail.html'
    context_object_name = 'profile'

    def get_queryset(self):
        """Ensure users can only view their own profiles."""
        return Profile.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Get match for this profile if exists
        try:
            match = Match.objects.get(user=self.request.user, profile=self.object)
            context['match'] = match
            context['score_breakdown'] = match.score_breakdown
        except Match.DoesNotExist:
            context['match'] = None
            context['score_breakdown'] = None

        return context


class ProfileImportView(LoginRequiredMixin, FormView):
    """Bulk import profiles from CSV file."""
    template_name = 'matching/profile_import.html'
    form_class = ProfileImportForm
    success_url = reverse_lazy('matching:profile-list')

    def form_valid(self, form):
        """Process the uploaded CSV file."""
        csv_file = form.cleaned_data['csv_file']

        # Read CSV
        try:
            decoded_file = csv_file.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(decoded_file))

            created_count = 0
            error_count = 0
            errors = []

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                try:
                    # Map CSV columns to model fields
                    profile = Profile(
                        user=self.request.user,
                        name=row.get('name', '').strip(),
                        company=row.get('company', '').strip() or None,
                        linkedin_url=row.get('linkedin_url', '').strip() or None,
                        website_url=row.get('website_url', '').strip() or None,
                        email=row.get('email', '').strip() or None,
                        industry=row.get('industry', '').strip() or None,
                        audience_size=self._parse_audience_size(row.get('audience_size', '')),
                        audience_description=row.get('audience_description', '').strip() or None,
                        content_style=row.get('content_style', '').strip() or None,
                        source=Profile.Source.IMPORT,
                    )

                    # Validate name is provided
                    if not profile.name:
                        errors.append(f'Row {row_num}: Name is required')
                        error_count += 1
                        continue

                    profile.full_clean()
                    profile.save()
                    created_count += 1

                except Exception as e:
                    errors.append(f'Row {row_num}: {str(e)}')
                    error_count += 1

            # Set success/error messages
            if created_count > 0:
                messages.success(self.request, f'Successfully imported {created_count} profiles.')

            if error_count > 0:
                messages.warning(
                    self.request,
                    f'{error_count} rows had errors. First 5 errors: {"; ".join(errors[:5])}'
                )

        except Exception as e:
            messages.error(self.request, f'Error processing CSV file: {str(e)}')

        return super().form_valid(form)

    def _parse_audience_size(self, value: str) -> str | None:
        """Parse audience size from CSV value."""
        value = value.strip().lower()
        valid_sizes = {choice[0] for choice in Profile.AudienceSize.choices}
        return value if value in valid_sizes else None


class MatchListView(LoginRequiredMixin, ListView):
    """
    List all matches sorted by score.

    Color-coded:
    - Green: 8+ score
    - Yellow: 6-8 score
    - Red: <6 score
    """
    model = Match
    template_name = 'matching/match_list.html'
    context_object_name = 'matches'
    paginate_by = 20

    def get_queryset(self):
        """Filter matches by current user and apply filters."""
        queryset = Match.objects.filter(user=self.request.user).select_related('profile')

        # Status filter
        status = self.request.GET.get('status', '').strip()
        if status:
            queryset = queryset.filter(status=status)

        # Score range filter
        min_score = self.request.GET.get('min_score', '').strip()
        if min_score:
            try:
                queryset = queryset.filter(final_score__gte=float(min_score) / 10)
            except ValueError:
                pass

        return queryset.order_by('-final_score', '-created_at')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Status choices
        context['statuses'] = Match.Status.choices

        # Current filter values
        context['current_status'] = self.request.GET.get('status', '')
        context['current_min_score'] = self.request.GET.get('min_score', '')

        # Score statistics
        all_matches = Match.objects.filter(user=self.request.user)
        context['total_matches'] = all_matches.count()
        context['high_score_count'] = all_matches.filter(final_score__gte=0.8).count()
        context['medium_score_count'] = all_matches.filter(
            final_score__gte=0.6,
            final_score__lt=0.8
        ).count()
        context['low_score_count'] = all_matches.filter(final_score__lt=0.6).count()

        return context

    def render_to_response(self, context, **response_kwargs):
        """Return partial template for HTMX requests."""
        if self.request.htmx:
            self.template_name = 'matching/partials/match_cards.html'
        return super().render_to_response(context, **response_kwargs)


class MatchDetailView(LoginRequiredMixin, DetailView):
    """View match details with full score breakdown."""
    model = Match
    template_name = 'matching/match_detail.html'
    context_object_name = 'match'

    def get_queryset(self):
        """Ensure users can only view their own matches."""
        return Match.objects.filter(user=self.request.user).select_related('profile')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Status form for updating match status
        context['status_form'] = MatchStatusForm(instance=self.object)

        # Score components for visualization
        if self.object.score_breakdown:
            breakdown = self.object.score_breakdown
            context['score_components'] = [
                {
                    'name': 'Intent',
                    'score': breakdown.get('intent', {}).get('score', 0),
                    'weight': 45,
                    'explanation': breakdown.get('intent', {}).get('explanation', ''),
                    'factors': breakdown.get('intent', {}).get('factors', []),
                },
                {
                    'name': 'Synergy',
                    'score': breakdown.get('synergy', {}).get('score', 0),
                    'weight': 25,
                    'explanation': breakdown.get('synergy', {}).get('explanation', ''),
                    'factors': breakdown.get('synergy', {}).get('factors', []),
                },
                {
                    'name': 'Momentum',
                    'score': breakdown.get('momentum', {}).get('score', 0),
                    'weight': 20,
                    'explanation': breakdown.get('momentum', {}).get('explanation', ''),
                    'factors': breakdown.get('momentum', {}).get('factors', []),
                },
                {
                    'name': 'Context',
                    'score': breakdown.get('context', {}).get('score', 0),
                    'weight': 10,
                    'explanation': breakdown.get('context', {}).get('explanation', ''),
                    'factors': breakdown.get('context', {}).get('factors', []),
                },
            ]
            context['recommendation'] = breakdown.get('recommendation', '')
        else:
            context['score_components'] = []
            context['recommendation'] = ''

        return context


class MatchUpdateStatusView(LoginRequiredMixin, View):
    """HTMX endpoint to update match status."""

    def post(self, request, pk):
        match = get_object_or_404(Match, pk=pk, user=request.user)

        status = request.POST.get('status')
        notes = request.POST.get('notes', '')

        if status and status in dict(Match.Status.choices):
            match.status = status
            if notes:
                match.notes = notes
            match.save()

            if request.htmx:
                return render(request, 'matching/partials/match_status_badge.html', {
                    'match': match
                })

            messages.success(request, 'Match status updated.')
            return redirect('matching:match-detail', pk=pk)

        messages.error(request, 'Invalid status.')
        return redirect('matching:match-detail', pk=pk)


class CalculateMatchView(LoginRequiredMixin, View):
    """
    HTMX endpoint to calculate or recalculate match score for a profile.

    Can be called for a single profile or bulk calculation.
    """

    def post(self, request, pk=None):
        """Calculate match score for a single profile."""
        if pk:
            profile = get_object_or_404(Profile, pk=pk, user=request.user)
            profiles = [profile]
        else:
            # Bulk calculation for all profiles without matches
            profile_ids = request.POST.getlist('profile_ids')
            if profile_ids:
                profiles = Profile.objects.filter(
                    pk__in=profile_ids,
                    user=request.user
                )
            else:
                # Calculate for all profiles
                profiles = Profile.objects.filter(user=request.user)

        matches_created = 0
        for profile in profiles:
            service = MatchScoringService(profile, request.user)
            service.create_or_update_match()
            matches_created += 1

        if request.htmx:
            if pk:
                # Return updated match card for single profile
                match = Match.objects.get(user=request.user, profile_id=pk)
                return render(request, 'matching/partials/match_score_badge.html', {
                    'match': match
                })
            else:
                # Return success message for bulk
                return HttpResponse(
                    f'<div class="text-green-600">{matches_created} matches calculated</div>'
                )

        messages.success(request, f'Calculated {matches_created} match scores.')
        return redirect('matching:match-list')

    def get(self, request, pk):
        """Show calculate button/form for HTMX."""
        profile = get_object_or_404(Profile, pk=pk, user=request.user)

        # Check if match exists
        try:
            match = Match.objects.get(user=request.user, profile=profile)
            has_match = True
        except Match.DoesNotExist:
            match = None
            has_match = False

        return render(request, 'matching/partials/calculate_match_form.html', {
            'profile': profile,
            'match': match,
            'has_match': has_match,
        })


class CalculateBulkMatchView(LoginRequiredMixin, View):
    """Calculate matches for all profiles without existing matches."""

    def post(self, request):
        # Get profiles without matches
        profiles_with_matches = Match.objects.filter(
            user=request.user
        ).values_list('profile_id', flat=True)

        profiles_without_matches = Profile.objects.filter(
            user=request.user
        ).exclude(pk__in=profiles_with_matches)

        matches_created = 0
        for profile in profiles_without_matches:
            service = MatchScoringService(profile, request.user)
            service.create_or_update_match()
            matches_created += 1

        if request.htmx:
            return HttpResponse(
                f'<div class="p-4 bg-green-100 text-green-800 rounded">'
                f'Successfully calculated {matches_created} new match scores.</div>'
            )

        messages.success(request, f'Calculated {matches_created} new match scores.')
        return redirect('matching:match-list')


class RecalculateAllMatchesView(LoginRequiredMixin, View):
    """Recalculate all existing match scores."""

    def post(self, request):
        profiles = Profile.objects.filter(user=request.user)

        matches_updated = 0
        for profile in profiles:
            service = MatchScoringService(profile, request.user)
            service.create_or_update_match()
            matches_updated += 1

        if request.htmx:
            return HttpResponse(
                f'<div class="p-4 bg-green-100 text-green-800 rounded">'
                f'Successfully recalculated {matches_updated} match scores.</div>'
            )

        messages.success(request, f'Recalculated {matches_updated} match scores.')
        return redirect('matching:match-list')


class ProfileDeleteView(LoginRequiredMixin, View):
    """Delete a profile."""

    def post(self, request, pk):
        profile = get_object_or_404(Profile, pk=pk, user=request.user)
        name = profile.name
        profile.delete()

        if request.htmx:
            return HttpResponse('')  # Return empty response for HTMX to remove element

        messages.success(request, f'Profile "{name}" deleted.')
        return redirect('matching:profile-list')


# =============================================================================
# DEMO OUTREACH (promotional mock, no login required)
# =============================================================================

class DemoOutreachView(TemplateView):
    """
    Mock Partner Outreach report using fairy tale characters.
    Shareable at /matching/demo/outreach/ for prospects.
    """
    template_name = 'matching/demo_outreach.html'

    def get_context_data(self, **kwargs):
        from .demo_data import get_demo_outreach_data
        data = get_demo_outreach_data()
        context = super().get_context_data(**kwargs)
        context.update(data)
        return context


class DemoProfileView(TemplateView):
    """
    Mock Client Profile one-pager (fairy tale).
    Shareable at /matching/demo/profile/ for prospects.
    """
    template_name = 'matching/demo_profile.html'

    def get_context_data(self, **kwargs):
        from .demo_data import get_demo_profile_data
        data = get_demo_profile_data()
        context = super().get_context_data(**kwargs)
        context.update(data)
        return context


class DemoReportView(TemplateView):
    """
    Demo Partner Report hub (like Janet index).
    /matching/demo/ with links to Outreach and Client Profile.
    """
    template_name = 'matching/demo_report.html'


# =============================================================================
# MEMBER REPORTS (access-coded, no login required)
# =============================================================================

class ReportAccessMixin:
    """Mixin that verifies session-based report access."""

    def get_report_or_redirect(self, request, report_id):
        from .models import MemberReport
        report = get_object_or_404(MemberReport, id=report_id)
        if not request.session.get(f'report_access_{report.id}'):
            return None, redirect('matching:report-access')
        if not report.is_accessible:
            return None, redirect('matching:report-access')
        return report, None


class ReportAccessView(View):
    """
    Code-gated entry point for member reports.
    No login required -- members authenticate via access code.
    """

    def get(self, request):
        return render(request, 'matching/report_access.html')

    def post(self, request):
        code = request.POST.get('code', '').strip().upper()

        # --- Rate limiting: 5 attempts per 15 minutes via session ---
        now = time.time()
        attempts = request.session.get('report_access_attempts', [])
        # Prune attempts older than 15 minutes
        window = 15 * 60
        attempts = [t for t in attempts if now - t < window]

        if len(attempts) >= 5:
            messages.error(
                request,
                'Too many attempts. Please wait 15 minutes before trying again.',
            )
            return render(request, 'matching/report_access.html')

        attempts.append(now)
        request.session['report_access_attempts'] = attempts

        # --- Look up the report ---
        try:
            report = MemberReport.objects.get(access_code=code)
        except MemberReport.DoesNotExist:
            messages.error(request, 'Invalid access code.')
            return render(request, 'matching/report_access.html')

        if not report.is_accessible:
            messages.error(request, 'This report is no longer accessible.')
            return render(request, 'matching/report_access.html')

        # --- Grant access ---
        request.session[f'report_access_{report.id}'] = True

        # Update tracking fields
        from django.utils import timezone
        report.last_accessed_at = timezone.now()
        report.access_count += 1
        report.save(update_fields=['last_accessed_at', 'access_count'])

        return redirect('matching:report-hub', report_id=report.id)


class ReportHubView(ReportAccessMixin, View):
    """
    Report landing page showing navigation links for a member report.
    No login required -- access is verified via session.
    """

    def get(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect
        return render(request, 'matching/report_hub.html', {'report': report})


def _get_tracking_context(report) -> dict:
    """Build tracking config for analytics JS in report templates."""
    from django.conf import settings
    supabase_url = getattr(settings, 'SUPABASE_URL', '') or ''
    supabase_key = getattr(settings, 'SUPABASE_KEY', '') or ''
    ctx = {}
    if supabase_url and supabase_key:
        ctx['tracking_config'] = json.dumps({
            'supabase_url': supabase_url,
            'supabase_anon_key': supabase_key,
            'report_id': report.id,
            'access_code': report.access_code,
        })
    clarity_id = os.environ.get('CLARITY_PROJECT_ID', '')
    if clarity_id:
        ctx['clarity_project_id'] = clarity_id
    return ctx


def _outreach_clean_url(value: str) -> str:
    """Strip trailing commas and whitespace from URLs."""
    if not value:
        return ''
    return value.strip().rstrip(',')


def _outreach_extract_linkedin(sp: SupabaseProfile) -> str:
    """Get LinkedIn URL, checking both linkedin and website fields."""
    if sp.linkedin and 'linkedin.com' in sp.linkedin.lower():
        return _outreach_clean_url(sp.linkedin)
    if sp.website and 'linkedin.com' in sp.website.lower():
        return _outreach_clean_url(sp.website)
    return ''


def _outreach_extract_website(sp: SupabaseProfile) -> str:
    """Get actual website URL, excluding social/scheduling links."""
    if not sp.website:
        return ''
    url = _outreach_clean_url(sp.website)
    for domain in ('linkedin.com', 'facebook.com', 'calendly.com', 'cal.com', 'tinyurl.com'):
        if domain in url.lower():
            return ''
    return url


def _outreach_extract_schedule(sp: SupabaseProfile) -> str:
    """Get scheduling link from booking_link or website."""
    if sp.booking_link:
        return _outreach_clean_url(sp.booking_link)
    if sp.website and ('calendly.com' in sp.website.lower() or 'cal.com' in sp.website.lower()):
        return _outreach_clean_url(sp.website)
    return ''


def _outreach_format_list_size(size) -> str:
    if not size:
        return ''
    if size >= 1_000_000:
        return f'{size / 1_000_000:.0f}M+'
    if size >= 1000:
        return f'{size // 1000}K'
    return str(size)


def _outreach_build_tagline(sp: SupabaseProfile) -> str:
    return sp.what_you_do or sp.offering or sp.niche or sp.business_focus or ''


def _outreach_clean_company(sp: SupabaseProfile) -> str:
    """Extract a clean company name, filtering out niche/category text."""
    company = (sp.company or '').strip()
    if company.startswith((',', '.')):
        company = company.lstrip(',.').strip()
    category_words = {
        'business skills', 'self improvement', 'success', 'fitness',
        'lifestyle', 'mental health', 'health', 'personal finances',
        'relationships', 'spirituality', 'natural health', 'service provider',
    }
    segments = [s.strip().lower() for s in company.split(',')]
    if any(seg in category_words for seg in segments):
        real = [s.strip() for s in company.split(',') if s.strip().lower() not in category_words]
        company = real[0] if real else ''
    if company.count(',') >= 1:
        first = company.split(',')[0].strip()
        company = first if first and len(first) > 3 else ''
    if not company or company.lower() in ('more info', 'n/a', 'none', 'tbd'):
        return sp.name
    return company


def _outreach_assign_section(sp: SupabaseProfile, score: float) -> tuple:
    """Assign outreach section using tier thresholds.

    Returns (section_key, section_label, section_note).
    Uses tier-aligned thresholds: hand_picked (>=64), strong (>=55), wildcard (<55).
    """
    has_email = bool(sp.email)
    has_linkedin = bool(_outreach_extract_linkedin(sp))
    has_schedule = bool(_outreach_extract_schedule(sp))

    if sp.booking_link and sp.seeking and 'jv' in (sp.seeking or '').lower():
        return 'jv_programs', 'JV Programs', 'Apply directly via their partner page'

    if score >= 64 and has_email:
        return 'priority', 'Priority Contacts', 'Hand-picked matches — reach out this week'

    if score >= 55 and has_email:
        return 'this_week', 'This Week', 'Strong matches — email available'

    if has_schedule:
        return 'this_week', 'This Week', 'Schedule a call directly'

    if has_linkedin:
        return 'low_priority', 'LinkedIn Outreach', 'Connect on LinkedIn first'

    return 'low_priority', 'Research Needed', 'Find contact info before outreach'


def _outreach_assign_badge(sp: SupabaseProfile, score: float) -> str:
    if score >= 64:
        return 'Hand-Picked'
    if sp.seeking and 'jv' in (sp.seeking or '').lower():
        return 'Active JV'
    if score >= 55:
        return 'Strong Match'
    if (sp.list_size or 0) >= 100000:
        return f'{_outreach_format_list_size(sp.list_size)} Reach'
    return ''


def _outreach_assign_badge_style(score: float) -> str:
    if score >= 64:
        return 'priority'
    return 'fit'


def _outreach_build_tags(sp: SupabaseProfile, score: float) -> list:
    tags = []
    all_text = ' '.join(filter(None, [
        sp.niche or '', sp.who_you_serve or '',
        sp.what_you_do or '', sp.offering or '',
    ])).lower()

    if 'women' in all_text or 'female' in all_text:
        tags.append({'label': 'Women', 'style': 'fit'})
    if 'coach' in all_text:
        tags.append({'label': 'Coaches', 'style': 'fit'})
    if 'speaker' in all_text or 'speaking' in all_text:
        tags.append({'label': 'Speakers', 'style': 'fit'})
    if 'entrepreneur' in all_text:
        tags.append({'label': 'Entrepreneurs', 'style': 'fit'})
    if 'author' in all_text or 'book' in all_text:
        tags.append({'label': 'Author', 'style': 'fit'})
    if 'event' in all_text or 'summit' in all_text:
        tags.append({'label': 'Events', 'style': 'fit'})
    if 'podcast' in all_text:
        tags.append({'label': 'Podcast', 'style': 'fit'})

    if sp.seeking and 'jv' in (sp.seeking or '').lower():
        tags.append({'label': 'Active JV', 'style': 'priority'})
    if score >= 64:
        tags.append({'label': 'Hand-Picked', 'style': 'priority'})

    if (sp.list_size or 0) >= 50000:
        tags.append({'label': 'Large List', 'style': 'fit'})

    return tags[:4]


def _outreach_build_audience(sp: SupabaseProfile) -> str:
    parts = []
    if sp.list_size:
        parts.append(f'{sp.list_size:,} subscribers')
    if sp.who_you_serve:
        parts.append(sp.who_you_serve)
    elif sp.niche:
        parts.append(f'{sp.niche} audience')
    if sp.offering and sp.offering not in ' '.join(parts):
        parts.append(f'Offering: {sp.offering}')
    return '. '.join(parts) if parts else ''


def _outreach_build_detail_note(sp: SupabaseProfile) -> str:
    parts = []
    if sp.signature_programs:
        parts.append(f'Programs: {sp.signature_programs}')
    if sp.business_focus and sp.business_focus != sp.niche:
        parts.append(f'Focus: {sp.business_focus}')
    if sp.notes:
        for line in sp.notes.split('\n')[:2]:
            line = line.strip()
            if line and len(line) < 200:
                parts.append(line)
    return ' · '.join(parts) if parts else ''


def _outreach_build_why_fit(sp: SupabaseProfile, match_context: dict) -> str:
    """Build why-fit text from match_context JSON or profile fields."""
    if match_context:
        parts = []
        for dim_key in ('intent', 'synergy', 'momentum'):
            dim = match_context.get(dim_key, {})
            for factor in dim.get('factors', []):
                if factor.get('score', 0) >= 6.0:
                    detail = factor.get('detail', '')
                    if detail:
                        parts.append(detail)
        if parts:
            return '. '.join(parts[:3]) + '.'

    # Fallback to profile fields
    parts = []
    if sp.who_you_serve:
        parts.append(sp.who_you_serve)
    elif sp.niche:
        parts.append(f'{sp.niche} specialist')
    if sp.what_you_do:
        parts.append(sp.what_you_do)
    return '. '.join(parts) if parts else ''


def _outreach_assign_section_from_dict(pd: dict) -> tuple:
    """Assign section from a partner dict (used after building the dict)."""
    score = pd.get('match_score', 0)
    has_email = bool(pd.get('email'))
    has_linkedin = bool(pd.get('linkedin'))
    has_schedule = bool(pd.get('schedule'))
    is_jv_program = bool(pd.get('apply_url'))

    if is_jv_program:
        return 'jv_programs', 'JV Programs', 'Apply directly via their partner page'
    if score >= 64 and has_email:
        return 'priority', 'Priority Contacts', 'Hand-picked matches — reach out this week'
    if score >= 55 and has_email:
        return 'this_week', 'This Week', 'Strong matches — email available'
    if has_schedule:
        return 'this_week', 'This Week', 'Schedule a call directly'
    if has_linkedin:
        return 'low_priority', 'LinkedIn Outreach', 'Connect on LinkedIn first'
    return 'low_priority', 'Research Needed', 'Find contact info before outreach'


def _outreach_build_partner_dict(sp: SupabaseProfile, score: float, match_context: dict) -> dict:
    """Build a dict matching ReportPartner field names from live SupabaseProfile data."""
    return {
        'id': str(sp.id),
        'name': sp.name,
        'company': _outreach_clean_company(sp),
        'tagline': _outreach_build_tagline(sp),
        'email': sp.email or '',
        'website': _outreach_extract_website(sp),
        'phone': sp.phone or '',
        'linkedin': _outreach_extract_linkedin(sp),
        'apply_url': sp.booking_link or '',
        'schedule': _outreach_extract_schedule(sp),
        'badge': _outreach_assign_badge(sp, score),
        'badge_style': _outreach_assign_badge_style(score),
        'list_size': _outreach_format_list_size(sp.list_size),
        'audience': _outreach_build_audience(sp),
        'why_fit': _outreach_build_why_fit(sp, match_context),
        'detail_note': _outreach_build_detail_note(sp),
        'tags': _outreach_build_tags(sp, score),
        'match_score': score,
    }


def _snapshot_to_partner_dict(rp) -> dict:
    """Convert a ReportPartner model instance to the dict format the template expects."""
    return {
        'id': str(rp.source_profile_id or rp.pk),
        'name': rp.name,
        'company': rp.company,
        'tagline': rp.tagline,
        'email': rp.email,
        'website': rp.website,
        'phone': rp.phone,
        'linkedin': rp.linkedin,
        'apply_url': rp.apply_url,
        'schedule': rp.schedule,
        'badge': rp.badge,
        'badge_style': rp.badge_style,
        'list_size': rp.list_size,
        'audience': rp.audience,
        'why_fit': rp.why_fit,
        'detail_note': rp.detail_note,
        'tags': rp.tags or [],
        'match_score': rp.match_score or 0,
    }


class ReportOutreachView(ReportAccessMixin, View):
    """
    Partner outreach list from live SupabaseMatch + SupabaseProfile data.
    No login required -- access is verified via session.
    """

    def get(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect

        # If report has frozen partner data, use it (curated + algorithmic)
        if report.partners.exists():
            return self._fallback_to_snapshot(request, report)

        client_sp = report.supabase_profile
        if not client_sp:
            return self._fallback_to_snapshot(request, report)

        # Query live matches from SupabaseMatch, ordered by harmonic_mean
        matches = (
            SupabaseMatch.objects
            .filter(Q(profile_id=client_sp.id) | Q(suggested_profile_id=client_sp.id))
            .filter(harmonic_mean__isnull=False)
            .order_by('-harmonic_mean')[:15]
        )

        # Collect partner IDs and batch-load profiles (avoids N+1 queries)
        matches = list(matches)
        partner_ids = set()
        for match in matches:
            if str(match.profile_id) == str(client_sp.id):
                partner_ids.add(match.suggested_profile_id)
            else:
                partner_ids.add(match.profile_id)

        profiles_by_id = {
            sp.id: sp
            for sp in SupabaseProfile.objects.filter(id__in=partner_ids)
        }

        # Build partner dicts from live data
        partner_dicts = []
        for match in matches:
            if str(match.profile_id) == str(client_sp.id):
                partner_sp = profiles_by_id.get(match.suggested_profile_id)
            else:
                partner_sp = profiles_by_id.get(match.profile_id)

            if not partner_sp:
                continue

            score = float(match.harmonic_mean)
            match_context = match.match_context or {}
            partner_dicts.append(_outreach_build_partner_dict(partner_sp, score, match_context))

        # Inject curated partners from ReportPartner snapshot
        curated_partners = report.partners.filter(section='curated').order_by('rank')

        # Assign sections dynamically
        section_buckets = {}

        if curated_partners.exists():
            section_buckets['curated'] = {
                'key': 'curated', 'label': 'Personally Selected',
                'note': 'Hand-curated by your JV matching team',
                'partners': [_snapshot_to_partner_dict(cp) for cp in curated_partners],
            }

        for pd in partner_dicts:
            section_key, label, note = _outreach_assign_section_from_dict(pd)
            if section_key not in section_buckets:
                section_buckets[section_key] = {
                    'key': section_key, 'label': label, 'note': note, 'partners': [],
                }
            section_buckets[section_key]['partners'].append(pd)

        sections = []
        for key in ('curated', 'priority', 'this_week', 'low_priority', 'jv_programs'):
            if key in section_buckets:
                sections.append(section_buckets[key])

        curated_count = len(curated_partners)
        context = {
            'report': report,
            'sections': sections,
            'total_partners': len(partner_dicts) + curated_count,
            **_get_tracking_context(report),
        }
        return render(request, 'matching/report_outreach.html', context)

    def _fallback_to_snapshot(self, request, report):
        """Fall back to ReportPartner snapshot when no live profile is linked."""
        partners = report.partners.all()
        sections = []
        for section_key in ['curated', 'priority', 'this_week', 'low_priority', 'jv_programs']:
            section_partners = partners.filter(section=section_key)
            if section_partners.exists():
                first = section_partners.first()
                sections.append({
                    'key': section_key,
                    'label': first.section_label or section_key.replace('_', ' ').title(),
                    'note': first.section_note or '',
                    'partners': section_partners,
                })
        context = {
            'report': report,
            'sections': sections,
            'total_partners': partners.count(),
            **_get_tracking_context(report),
        }
        return render(request, 'matching/report_outreach.html', context)


class ReportProfileView(ReportAccessMixin, View):
    """
    Client profile one-pager derived from live SupabaseProfile data.
    Falls back to report.client_profile JSON if no linked profile.
    No login required -- access is verified via session.
    """

    def get(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect

        sp = report.supabase_profile
        if sp:
            context = _build_profile_context(sp, report)
        else:
            context = {**report.client_profile, 'report': report}
        context.update(_get_tracking_context(report))
        return render(request, 'matching/report_profile.html', context)


class ReportProfileEditView(ReportAccessMixin, View):
    """
    Edit form for client profile fields on SupabaseProfile.
    Stamps changed fields with client_ingest provenance.
    """

    def get(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect

        sp = report.supabase_profile
        if not sp:
            messages.info(request, 'No linked profile available to edit.')
            return redirect('matching:report-profile', report_id=report.id)

        from .forms import ClientProfileForm
        form = ClientProfileForm(instance=sp)
        return render(request, 'matching/report_profile_edit.html', {
            'report': report,
            'form': form,
        })

    def post(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect

        sp = report.supabase_profile
        if not sp:
            messages.info(request, 'No linked profile available to edit.')
            return redirect('matching:report-profile', report_id=report.id)

        from .forms import ClientProfileForm
        from django.utils import timezone

        form = ClientProfileForm(request.POST, instance=sp)
        if form.is_valid():
            sp = form.save(commit=False)

            # Stamp provenance on changed fields
            now = timezone.now()
            meta = sp.enrichment_metadata or {}
            field_meta = meta.get('field_meta', {})
            for field_name in form.changed_data:
                field_meta[field_name] = {
                    'source': 'client_ingest',
                    'updated_at': now.isoformat(),
                }
            meta['field_meta'] = field_meta
            sp.enrichment_metadata = meta
            sp.save()

            messages.success(request, 'Profile updated. Review and confirm your changes.')
            return redirect('matching:report-profile', report_id=report.id)

        return render(request, 'matching/report_profile_edit.html', {
            'report': report,
            'form': form,
        })


class ReportProfileConfirmView(ReportAccessMixin, View):
    """
    Upgrades client_ingest fields to client_confirmed (priority 100).
    After confirmation, no AI enrichment can overwrite these fields.
    """

    def post(self, request, report_id):
        report, error_redirect = self.get_report_or_redirect(request, report_id)
        if error_redirect:
            return error_redirect

        sp = report.supabase_profile
        if not sp:
            messages.info(request, 'No linked profile to confirm.')
            return redirect('matching:report-profile', report_id=report.id)

        from django.utils import timezone
        now = timezone.now()

        meta = sp.enrichment_metadata or {}
        field_meta = meta.get('field_meta', {})
        confirmed_count = 0
        for field_name, info in field_meta.items():
            if info.get('source') == 'client_ingest':
                info['source'] = 'client_confirmed'
                info['updated_at'] = now.isoformat()
                confirmed_count += 1

        meta['field_meta'] = field_meta
        sp.enrichment_metadata = meta
        sp.save(update_fields=['enrichment_metadata'])

        if confirmed_count:
            messages.success(request, f'{confirmed_count} field(s) confirmed. Your data is now protected from AI updates.')
        else:
            messages.info(request, 'No pending changes to confirm.')
        return redirect('matching:report-profile', report_id=report.id)


# ---------------------------------------------------------------------------
# Report profile helpers
# ---------------------------------------------------------------------------

def _build_seeking_goals(seeking_text):
    """Split seeking text into meaningful bullet points.

    Uses sentence boundaries or semicolons — never commas, which appear
    inside natural prose and produce fragments like "BizOpp" or "health".
    """
    if not seeking_text:
        return []
    text = seeking_text.strip()
    # Try sentence boundaries (period/exclamation followed by space + uppercase)
    sentences = re.split(r'(?<=[.!])\s+(?=[A-Z])', text)
    if len(sentences) >= 2:
        return [s.strip() for s in sentences if s.strip()][:5]
    # Try semicolons
    if ';' in text:
        return [s.strip() for s in text.split(';') if s.strip()][:5]
    # Single block — return as one item
    return [text]


def _build_profile_context(sp, report):
    """Build template context from live SupabaseProfile data."""
    name = sp.name or ''
    company = sp.company or ''
    first_name = name.split()[0] if name else ''

    seeking_goals = _build_seeking_goals(sp.seeking or '')

    credentials = []
    if company and company != name:
        credentials.append(f'Founder of {company}')
    if sp.signature_programs:
        credentials.append(sp.signature_programs)
    if getattr(sp, 'social_proof', None):
        credentials.append(sp.social_proof)

    bio_text = sp.bio or (f'{name} is the founder of {company}.' if company else '')
    who_text = (sp.who_you_serve or 'entrepreneurs and business owners').rstrip('.')

    meta = sp.enrichment_metadata or {}
    field_meta = meta.get('field_meta', {})
    has_unconfirmed = any(
        info.get('source') == 'client_ingest'
        for info in field_meta.values()
    )

    context = {
        'report': report,
        'contact_name': name,
        'avatar_initials': ''.join(w[0].upper() for w in name.split()[:2]) if name else '??',
        'title': f'{name} \u00b7 {company}' if company else name,
        'program_name': company or sp.signature_programs or name,
        'program_sub': sp.offering or '',
        'program_focus': sp.niche or 'Business Growth',
        'target_audience': sp.audience_type or sp.niche or 'Entrepreneurs & Business Owners',
        'target_audience_sub': sp.who_you_serve or '',
        'network_reach': _format_list_size(sp.list_size) if sp.list_size else 'Growing',
        'network_reach_sub': f"{first_name}'s subscriber network",
        'main_website': sp.website or '',
        'key_message': sp.offering or '',
        'about_story': bio_text,
        'about_story_paragraphs': [p.strip() for p in bio_text.split('\n\n') if p.strip()] if bio_text else [],
        'credentials': credentials,
        'ideal_partner_intro': f'Partners serving <strong>{who_text}</strong>.',
        'ideal_partner_sub': sp.what_you_do or '',
        'seeking_goals': seeking_goals,
        'contact_email': sp.email or '',
        'tiers': [],
        'offers_partners': [],
        'shared_stage': [],
        'perfect_for': [],
        'faqs': [],
        'resource_links': [],
        'partner_deliverables': [],
        'why_converts': [],
        'launch_stats': None,
        'has_unconfirmed_edits': has_unconfirmed,
        'has_supabase_profile': True,
    }

    # Overlay rich JV Brief data from client_profile (tiers, FAQs, etc.)
    cp = report.client_profile or {}
    for field in ('tiers', 'offers_partners', 'faqs', 'resource_links',
                  'partner_deliverables', 'why_converts', 'launch_stats',
                  'credentials', 'key_message_headline', 'key_message_points',
                  'program_name', 'program_sub', 'program_focus',
                  'about_story', 'about_story_paragraphs',
                  'seeking_goals', 'ideal_partner_intro', 'ideal_partner_sub'):
        if field in cp and cp[field]:
            context[field] = cp[field]

    return context


def _format_list_size(size):
    """Format an integer list size into a human-readable string."""
    if not size:
        return ''
    if size >= 1_000_000:
        return f'{size / 1_000_000:.1f}M'
    if size >= 1_000:
        return f'{size / 1_000:.0f}K'
    return str(size)


# =============================================================================
# APOLLO WEBHOOK (Receives async phone/email data from Apollo.io)
# =============================================================================

class ApolloWebhookView(View):
    """
    Receives async phone/waterfall enrichment data from Apollo.io.

    Apollo sends phone numbers and waterfall-enriched emails asynchronously
    via webhook after the initial API response. This view processes that
    payload and writes the data to the matching Supabase profile.
    """

    def post(self, request, *args, **kwargs):
        import json
        import logging
        from datetime import datetime

        logger = logging.getLogger(__name__)

        try:
            payload = json.loads(request.body)
        except (json.JSONDecodeError, ValueError):
            return JsonResponse({'error': 'Invalid JSON'}, status=400)

        # Apollo webhook payload structure varies — extract what we can
        matches = payload.get('matches', [payload]) if not isinstance(payload, list) else payload

        updated = 0
        for match in matches:
            person = match.get('person', match)
            apollo_id = person.get('id')

            if not apollo_id:
                continue

            # Find profile by apollo_id stored in enrichment_metadata
            from matching.models import SupabaseProfile
            import psycopg2
            from psycopg2 import sql as psql
            import os

            try:
                conn = psycopg2.connect(os.environ['DATABASE_URL'])
                cur = conn.cursor()

                # Find profile with this apollo_id
                cur.execute(
                    "SELECT id, phone, email, enrichment_metadata FROM profiles "
                    "WHERE enrichment_metadata->'apollo_data'->>'apollo_id' = %s",
                    (apollo_id,)
                )
                row = cur.fetchone()

                if not row:
                    logger.info("Apollo webhook: no profile for apollo_id %s", apollo_id)
                    continue

                profile_id, existing_phone, existing_email, existing_meta = row

                set_parts = []
                params = []

                # Write phone if we don't have one
                phone_numbers = person.get('phone_numbers') or []
                if phone_numbers and not existing_phone:
                    raw_phone = phone_numbers[0].get('raw_number', '')
                    if raw_phone and len(raw_phone) >= 7:
                        set_parts.append(psql.SQL("phone = %s"))
                        params.append(raw_phone.strip())

                # Write waterfall email if we don't have one
                waterfall_email = person.get('email')
                if waterfall_email and not existing_email:
                    set_parts.append(psql.SQL("email = %s"))
                    params.append(waterfall_email.strip())

                if set_parts:
                    # Update enrichment_metadata too
                    meta = existing_meta or {}
                    if isinstance(meta, str):
                        meta = json.loads(meta)
                    apollo_data = meta.get('apollo_data', {})
                    apollo_data['webhook_received_at'] = datetime.now().isoformat()
                    apollo_data['phone_numbers'] = phone_numbers
                    meta['apollo_data'] = apollo_data

                    set_parts.append(psql.SQL("enrichment_metadata = %s::jsonb"))
                    params.append(json.dumps(meta))
                    set_parts.append(psql.SQL("updated_at = %s"))
                    params.append(datetime.now())

                    update_q = psql.SQL("UPDATE profiles SET {} WHERE id = %s").format(
                        psql.SQL(", ").join(set_parts)
                    )
                    params.append(profile_id)
                    cur.execute(update_q, params)
                    conn.commit()
                    updated += 1

                cur.close()
                conn.close()

            except Exception as e:
                logger.error("Apollo webhook error: %s", e)
                continue

        return JsonResponse({'status': 'ok', 'updated': updated})


# =============================================================================
# ANALYTICS DASHBOARD VIEWS (admin-only, login required)
# =============================================================================

class AnalyticsDashboardView(LoginRequiredMixin, View):
    """
    System-level analytics overview showing all active reports,
    top alerts, and a global engagement funnel.
    """

    def get(self, request):
        from django.utils import timezone
        from .models import (
            EngagementSummary, AnalyticsInsight, ReportPartner,
        )

        now = timezone.now()

        # ---- Report cards with engagement stats ----
        active_reports = MemberReport.objects.filter(is_active=True).order_by('-month')
        report_stats = []

        for report in active_reports:
            # Page-level summary (partner_id='')
            page_summary = (
                EngagementSummary.objects
                .filter(report=report, partner_id='')
                .first()
            )

            # Partner-level summaries
            partner_summaries = EngagementSummary.objects.filter(
                report=report
            ).exclude(partner_id='')

            total_partners = partner_summaries.count() or ReportPartner.objects.filter(report=report).count()
            contacted = partner_summaries.filter(any_contact_action=True).count()

            total_sessions = page_summary.total_sessions if page_summary else 0
            template_opens = page_summary.template_open_count if page_summary else 0
            template_copies = page_summary.template_copy_count if page_summary else 0
            template_copy_rate = (
                round(template_copies * 100 / template_opens)
                if template_opens > 0 else 0
            )

            days_since_visit = None
            if page_summary and page_summary.last_visit_at:
                days_since_visit = (now - page_summary.last_visit_at).days
            elif report.last_accessed_at:
                days_since_visit = (now - report.last_accessed_at).days

            report_stats.append({
                'report': report,
                'total_sessions': total_sessions,
                'total_partners': total_partners,
                'contacted': contacted,
                'template_copy_rate': template_copy_rate,
                'days_since_visit': days_since_visit,
            })

        # ---- Active alerts (top 10, CRITICAL first) ----
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        alerts = list(
            AnalyticsInsight.objects
            .filter(is_active=True, is_dismissed=False)
            .select_related('report')
            .order_by('-created_at')[:50]
        )
        alerts.sort(key=lambda a: (severity_order.get(a.severity, 9), -a.created_at.timestamp()))
        alerts = alerts[:10]

        # ---- Global funnel ----
        all_partner_summaries = EngagementSummary.objects.exclude(partner_id='')
        shown = all_partner_summaries.count()
        expanded = all_partner_summaries.filter(card_expand_count__gt=0).count()
        contacted_global = all_partner_summaries.filter(any_contact_action=True).count()

        funnel = {
            'shown': shown,
            'expanded': expanded,
            'contacted': contacted_global,
            'expanded_pct': round(expanded * 100 / shown) if shown > 0 else 0,
            'contacted_pct': round(contacted_global * 100 / shown) if shown > 0 else 0,
        }

        return render(request, 'matching/analytics/dashboard.html', {
            'report_stats': report_stats,
            'alerts': alerts,
            'funnel': funnel,
        })


class AnalyticsReportDetailView(LoginRequiredMixin, View):
    """
    Per-report analytics deep dive: sessions, funnel, partner engagement,
    score-vs-engagement, template effectiveness, interventions, insights.
    """

    def get(self, request, report_id):
        from django.utils import timezone
        from .models import (
            MemberReport, EngagementSummary, OutreachEvent,
            ReportPartner, AnalyticsInsight, AnalyticsIntervention,
        )

        report = get_object_or_404(MemberReport, id=report_id)
        now = timezone.now()

        # ---- Session timeline ----
        sessions_raw = (
            OutreachEvent.objects
            .filter(report_id=report.id)
            .values('session_id')
            .annotate(
                event_count=Count('id'),
                started_at=Min('created_at'),
                ended_at=Max('created_at'),
            )
            .order_by('-started_at')[:20]
        )

        sessions = []
        for s in sessions_raw:
            if s['started_at'] and s['ended_at']:
                delta = s['ended_at'] - s['started_at']
                minutes = int(delta.total_seconds() // 60)
                secs = int(delta.total_seconds() % 60)
                duration = f"{minutes}m {secs}s" if minutes else f"{secs}s"
            else:
                duration = "--"
            sessions.append({
                'started_at': s['started_at'],
                'event_count': s['event_count'],
                'duration': duration,
            })

        # ---- Page-level summary ----
        page_summary = (
            EngagementSummary.objects
            .filter(report=report, partner_id='')
            .first()
        )

        # ---- Partner summaries + ReportPartner data ----
        partner_summaries = {
            ps.partner_id: ps
            for ps in EngagementSummary.objects.filter(report=report).exclude(partner_id='')
        }

        report_partners = ReportPartner.objects.filter(report=report).select_related('source_profile')

        partner_rows = []
        for rp in report_partners:
            pid = str(rp.source_profile_id) if rp.source_profile_id else ''
            ps = partner_summaries.get(pid)

            partner_rows.append({
                'name': rp.name,
                'section': rp.section_label or rp.section,
                'match_score': round(rp.match_score) if rp.match_score else None,
                'expand_count': ps.card_expand_count if ps else 0,
                'avg_dwell_s': round(ps.avg_card_dwell_ms / 1000, 1) if ps and ps.avg_card_dwell_ms else 0,
                'email_clicked': ps.email_click_count > 0 if ps else False,
                'linkedin_clicked': ps.linkedin_click_count > 0 if ps else False,
                'schedule_clicked': ps.schedule_click_count > 0 if ps else False,
                'apply_clicked': ps.apply_click_count > 0 if ps else False,
                'was_checked': ps.was_checked if ps else False,
                'first_action': (
                    ps.first_action_at.strftime('%b %d, %H:%M')
                    if ps and ps.first_action_at else None
                ),
                '_contacted': ps.any_contact_action if ps else False,
                '_score': rp.match_score or 0,
            })

        # ---- Engagement funnel ----
        shown = len(partner_rows)
        expanded = sum(1 for p in partner_rows if p['expand_count'] > 0)
        contacted = sum(1 for p in partner_rows if p['_contacted'])

        funnel = {
            'shown': shown,
            'expanded': expanded,
            'contacted': contacted,
            'expanded_pct': round(expanded * 100 / shown) if shown > 0 else 0,
            'contacted_pct': round(contacted * 100 / shown) if shown > 0 else 0,
        }

        # ---- Score vs engagement buckets ----
        buckets_def = [
            ('80+', 80, 200),
            ('60-80', 60, 80),
            ('40-60', 40, 60),
            ('<40', 0, 40),
        ]

        score_buckets = []
        for label, low, high in buckets_def:
            in_bucket = [p for p in partner_rows if low <= p['_score'] < high]
            total = len(in_bucket)
            bucket_contacted = sum(1 for p in in_bucket if p['_contacted'])
            score_buckets.append({
                'label': label,
                'total': total,
                'contacted': bucket_contacted,
                'contact_rate': round(bucket_contacted * 100 / total) if total > 0 else 0,
            })

        # ---- Template effectiveness ----
        template_stats = {
            'opens': page_summary.template_open_count if page_summary else 0,
            'copies': page_summary.template_copy_count if page_summary else 0,
            'copy_rate': (
                round(
                    page_summary.template_copy_count * 100
                    / page_summary.template_open_count
                )
                if page_summary and page_summary.template_open_count > 0 else 0
            ),
        }

        # ---- Interventions ----
        interventions = AnalyticsIntervention.objects.filter(report=report).order_by('-created_at')

        # ---- Insights ----
        insights = (
            AnalyticsInsight.objects
            .filter(report=report, is_active=True, is_dismissed=False)
            .order_by('-created_at')
        )

        return render(request, 'matching/analytics/report_detail.html', {
            'report': report,
            'sessions': sessions,
            'funnel': funnel,
            'partner_rows': partner_rows,
            'score_buckets': score_buckets,
            'template_stats': template_stats,
            'interventions': interventions,
            'insights': insights,
        })


class AnalyticsInsightsView(LoginRequiredMixin, View):
    """
    Filterable feed of all analytics insights across all reports.
    """

    def get(self, request):
        from .models import AnalyticsInsight

        severity = request.GET.get('severity', '').strip().lower()

        qs = AnalyticsInsight.objects.filter(
            is_active=True, is_dismissed=False
        ).select_related('report').order_by('-created_at')

        # Counts for filter buttons (before filtering)
        all_active = AnalyticsInsight.objects.filter(is_active=True, is_dismissed=False)
        counts = {
            'all': all_active.count(),
            'critical': all_active.filter(severity='critical').count(),
            'warning': all_active.filter(severity='warning').count(),
            'info': all_active.filter(severity='info').count(),
        }

        if severity in ('critical', 'warning', 'info'):
            qs = qs.filter(severity=severity)

        return render(request, 'matching/analytics/insights.html', {
            'insights': qs,
            'current_severity': severity if severity in ('critical', 'warning', 'info') else '',
            'counts': counts,
        })


class AnalyticsInsightDismissView(LoginRequiredMixin, View):
    """
    Dismiss an insight via POST. Redirects back to the referring page.
    """

    def post(self, request, insight_id):
        from django.utils import timezone
        from .models import AnalyticsInsight

        insight = get_object_or_404(AnalyticsInsight, id=insight_id)
        insight.is_dismissed = True
        insight.dismissed_at = timezone.now()
        insight.save(update_fields=['is_dismissed', 'dismissed_at'])

        # Redirect back to referrer or dashboard
        referer = request.META.get('HTTP_REFERER')
        if referer:
            return redirect(referer)
        return redirect('matching:analytics-dashboard')


# ─── PIPELINE STATS API (for architecture_diagram.html live data) ───

class PipelineStatsAPIView(LoginRequiredMixin, View):
    """
    JSON endpoint returning live pipeline health stats from Supabase.
    Used by architecture_diagram.html Data Health tab.
    """

    def get(self, request):
        from django.db.models import Count, Q
        from django.db.models.functions import Coalesce

        total = SupabaseProfile.objects.filter(
            name__isnull=False
        ).exclude(name='').count()

        # Field coverage counts
        fields_to_check = {
            'bio': Q(bio__isnull=False) & ~Q(bio=''),
            'niche': Q(niche__isnull=False) & ~Q(niche=''),
            'who_you_serve': Q(who_you_serve__isnull=False) & ~Q(who_you_serve=''),
            'seeking': Q(seeking__isnull=False) & ~Q(seeking=''),
            'offering': Q(offering__isnull=False) & ~Q(offering=''),
            'what_you_do': Q(what_you_do__isnull=False) & ~Q(what_you_do=''),
            'business_focus': Q(business_focus__isnull=False) & ~Q(business_focus=''),
            'service_provided': Q(service_provided__isnull=False) & ~Q(service_provided=''),
            'signature_programs': Q(signature_programs__isnull=False) & ~Q(signature_programs=''),
            'current_projects': Q(current_projects__isnull=False) & ~Q(current_projects=''),
            'name': Q(name__isnull=False) & ~Q(name=''),
            'company': Q(company__isnull=False) & ~Q(company=''),
            'website': Q(website__isnull=False) & ~Q(website=''),
            'linkedin': Q(linkedin__isnull=False) & ~Q(linkedin=''),
            'phone': Q(phone__isnull=False) & ~Q(phone=''),
            'email': Q(email__isnull=False) & ~Q(email=''),
            'booking_link': Q(booking_link__isnull=False) & ~Q(booking_link=''),
            'revenue_tier': Q(revenue_tier__isnull=False) & ~Q(revenue_tier=''),
            'avatar_url': Q(avatar_url__isnull=False) & ~Q(avatar_url=''),
        }

        base_qs = SupabaseProfile.objects.filter(
            name__isnull=False
        ).exclude(name='')

        field_coverage = {}
        for field_name, q_filter in fields_to_check.items():
            count = base_qs.filter(q_filter).count()
            field_coverage[field_name] = {
                'count': count,
                'pct': round(count / total * 100, 1) if total else 0,
            }

        # Pipeline stage counts
        enriched = base_qs.filter(last_enriched_at__isnull=False).count()
        embedded = base_qs.filter(
            embedding_seeking__isnull=False
        ).exclude(embedding_seeking='').count()
        scored = SupabaseMatch.objects.filter(
            harmonic_mean__isnull=False
        ).values('profile_id').distinct().count()
        reported = MemberReport.objects.filter(is_active=True).count()

        pipeline_stages = {
            'ingested': total,
            'enriched': enriched,
            'embedded': embedded,
            'scored': scored,
            'reported': reported,
        }

        # Source distribution from enrichment_metadata
        source_distribution = {}
        try:
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT sub.source_val, COUNT(*) AS field_count
                    FROM profiles,
                        LATERAL jsonb_each(enrichment_metadata->'field_meta')
                            AS fields(field_name, field_info),
                        LATERAL (
                            SELECT field_info->>'source' AS source_val
                        ) sub
                    WHERE enrichment_metadata IS NOT NULL
                      AND enrichment_metadata->'field_meta' IS NOT NULL
                      AND name IS NOT NULL AND name != ''
                    GROUP BY sub.source_val
                    ORDER BY field_count DESC
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        source_distribution[row[0]] = row[1]
        except Exception:
            source_distribution = {'error': 'Could not query source distribution'}

        return JsonResponse({
            'total_profiles': total,
            'field_coverage': field_coverage,
            'pipeline_stages': pipeline_stages,
            'source_distribution': source_distribution,
        })


class ContactIngestionWebhookView(View):
    """REST endpoint for ingesting new contacts via webhook/API.

    POST /api/contacts/ingest/
    Body: {"contacts": [...], "source": "api_webhook", "ingested_by": "..."}
    """

    def post(self, request, *args, **kwargs):
        import json
        from django.http import JsonResponse
        from django.views.decorators.csrf import csrf_exempt

        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)

        contacts = data.get("contacts", [])
        if not contacts:
            return JsonResponse({"error": "No contacts provided"}, status=400)

        source = data.get("source", "api_webhook")
        ingested_by = data.get("ingested_by", "")

        try:
            from matching.enrichment.flows.new_contact_flow import new_contact_flow

            result = new_contact_flow(
                contacts=contacts,
                source=source,
                ingested_by=ingested_by,
                source_file="",
                skip_enrichment=False,
                dry_run=False,
            )

            return JsonResponse({
                "status": "ok",
                "contacts_received": len(contacts),
                "result": str(result),
            })
        except Exception as exc:
            return JsonResponse({"error": str(exc)}, status=500)

    def dispatch(self, request, *args, **kwargs):
        # Skip CSRF for API endpoint
        from django.utils.decorators import method_decorator
        from django.views.decorators.csrf import csrf_exempt
        return super().dispatch(request, *args, **kwargs)
