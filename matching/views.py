"""
Views for the JV Matcher module.

Provides profile management, match listing, and score calculation endpoints.
"""

import csv
import io

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy
from django.views import View
from django.views.generic import (
    CreateView,
    DetailView,
    FormView,
    ListView,
    UpdateView,
)

from .forms import ProfileForm, ProfileImportForm, MatchStatusForm
from .models import Match, Profile, SupabaseProfile, SupabaseMatch
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
