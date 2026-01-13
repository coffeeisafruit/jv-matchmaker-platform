"""
Views for the PVP Generator and Outreach module.

Provides views for:
- PVP generation and management
- Outreach template management (GEX-style)
- Campaign management
- Clay webhook integration
"""

import json
import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import ListView, DetailView, CreateView, UpdateView, TemplateView
from django.conf import settings
from django.utils import timezone

from matching.models import Match, Profile, SupabaseProfile, SupabaseMatch
from positioning.models import ICP
from .models import PVP, OutreachTemplate, OutreachCampaign, OutreachSequence, OutreachEmail, EmailConnection, SentEmail
from .services import PVPGeneratorService, ClayWebhookService
from .email_service import EmailService, OAuthHelper, generate_mailto_link

logger = logging.getLogger(__name__)


class PVPGeneratorView(LoginRequiredMixin, View):
    """
    Main view for generating PVPs for matches.

    GET: Display the PVP generator interface with match selection
    POST: Generate a PVP for the selected match (HTMX compatible)
    """
    template_name = 'outreach/pvp_generator.html'

    def get(self, request):
        """Display the PVP generator interface."""
        # Get user's Supabase profile by email
        user_supabase_profile = SupabaseProfile.objects.filter(
            email__iexact=request.user.email
        ).first()

        # Get Supabase matches for this user
        supabase_matches = []
        featured_partners = []

        if user_supabase_profile:
            # Get matches where user is the source profile
            match_suggestions = SupabaseMatch.objects.filter(
                profile_id=user_supabase_profile.id
            ).order_by('-harmonic_mean')[:20]

            # Fetch the suggested profiles
            suggested_profile_ids = [m.suggested_profile_id for m in match_suggestions]
            profiles_by_id = {
                p.id: p for p in SupabaseProfile.objects.filter(id__in=suggested_profile_ids)
            }

            # Build match list with profile data
            for match in match_suggestions:
                profile = profiles_by_id.get(match.suggested_profile_id)
                if profile:
                    supabase_matches.append({
                        'match': match,
                        'profile': profile,
                        'score': float(match.harmonic_mean or 0) * 100,  # Convert to percentage
                    })

        # If no matches, get featured partners
        if not supabase_matches:
            # Get featured partners (same logic as Partners page)
            featured_partners = list(SupabaseProfile.objects.filter(
                status='Member'
            ).exclude(
                name__isnull=True
            ).exclude(
                name=''
            ).order_by('-list_size', '-social_reach')[:20])

        # Get user's primary ICP
        primary_icp = ICP.objects.filter(
            user=request.user,
            is_primary=True
        ).first()

        # Get all user ICPs for selection
        icps = ICP.objects.filter(user=request.user)

        # Get pattern type choices
        pattern_choices = PVP.PATTERN_TYPE_CHOICES

        # Get selected profile if provided
        selected_profile_id = request.GET.get('profile_id')
        selected_profile = None
        selected_match_data = None
        if selected_profile_id:
            selected_profile = SupabaseProfile.objects.filter(id=selected_profile_id).first()
            if user_supabase_profile and selected_profile:
                selected_match_data = SupabaseMatch.objects.filter(
                    profile_id=user_supabase_profile.id,
                    suggested_profile_id=selected_profile.id
                ).first()

        # Get recent PVPs for display
        recent_pvps = PVP.objects.filter(user=request.user).order_by('-created_at')[:5]

        context = {
            'supabase_matches': supabase_matches,
            'featured_partners': featured_partners,
            'user_supabase_profile': user_supabase_profile,
            'primary_icp': primary_icp,
            'icps': icps,
            'pattern_choices': pattern_choices,
            'selected_profile': selected_profile,
            'selected_match_data': selected_match_data,
            'recent_pvps': recent_pvps,
        }

        return render(request, self.template_name, context)

    def post(self, request):
        """Generate a PVP for the selected Supabase profile or general copy."""
        profile_id = request.POST.get('profile_id')
        pattern_type = request.POST.get('pattern_type', 'pain_solution')
        icp_id = request.POST.get('icp_id')

        # Get ICP if specified (needed for both general and specific cases)
        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, id=icp_id, user=request.user)
        else:
            # Use primary ICP if available
            icp = ICP.objects.filter(user=request.user, is_primary=True).first()

        # Handle general copy generation (no specific partner selected OR "general" chosen)
        if not profile_id or profile_id == 'general':
            try:
                # Get user's API key if available (BYOK)
                from core.models import APIKey
                api_key = None
                user_key = APIKey.objects.filter(
                    user=request.user,
                    provider='anthropic',
                    is_active=True
                ).first()
                if user_key:
                    api_key = user_key.encrypted_key

                # Generate general PVP
                service = PVPGeneratorService(api_key=api_key)
                result = service.generate_general_pvp(pattern_type, icp)

                # Save the PVP (without a specific profile)
                pvp = PVP.objects.create(
                    user=request.user,
                    supabase_profile_id=None,  # No specific partner
                    pattern_type=pattern_type,
                    pain_point_addressed=result.pain_point_addressed,
                    value_offered=result.value_offered,
                    call_to_action=result.call_to_action,
                    full_message=result.full_message,
                    personalization_data=result.personalization_data,
                    ai_model_used=service.model,
                    quality_score=result.quality_score,
                )

                # Update user's PVP count
                request.user.pvps_this_month += 1
                request.user.save(update_fields=['pvps_this_month'])

                # Return HTMX partial or JSON response
                if request.htmx:
                    return render(request, 'outreach/partials/pvp_result.html', {
                        'pvp': pvp,
                        'quality_breakdown': result.quality_breakdown,
                        'profile': None,
                        'is_general': True,
                    })

                return JsonResponse({
                    'success': True,
                    'pvp_id': pvp.id,
                    'quality_score': pvp.quality_score,
                    'full_message': pvp.full_message,
                    'is_general': True,
                })

            except Exception as e:
                logger.error(f"Error generating general PVP: {e}")

                if request.htmx:
                    return render(request, 'outreach/partials/pvp_error.html', {
                        'error': str(e)
                    })

                return JsonResponse({
                    'success': False,
                    'error': str(e)
                }, status=500)

        # Handle specific partner profile
        try:
            profile = SupabaseProfile.objects.get(id=profile_id)
        except (SupabaseProfile.DoesNotExist, ValueError, Exception):
            if request.htmx:
                return render(request, 'outreach/partials/pvp_error.html', {
                    'error': 'Please select a valid partner profile.'
                })
            return JsonResponse({'success': False, 'error': 'Invalid profile'}, status=400)

        # Get user's Supabase profile for match data
        user_supabase_profile = SupabaseProfile.objects.filter(
            email__iexact=request.user.email
        ).first()

        # Get match data if available
        match_data = None
        if user_supabase_profile:
            match_data = SupabaseMatch.objects.filter(
                profile_id=user_supabase_profile.id,
                suggested_profile_id=profile.id
            ).first()

        try:
            # Get user's API key if available (BYOK)
            from core.models import APIKey
            api_key = None
            user_key = APIKey.objects.filter(
                user=request.user,
                provider='anthropic',
                is_active=True
            ).first()
            if user_key:
                # In production, decrypt the key
                api_key = user_key.encrypted_key

            # Generate PVP using Supabase profile data
            service = PVPGeneratorService(api_key=api_key)
            result = service.generate_pvp_for_supabase(profile, match_data, pattern_type, icp)

            # Save the PVP
            pvp = PVP.objects.create(
                user=request.user,
                supabase_profile_id=profile.id,
                pattern_type=pattern_type,
                pain_point_addressed=result.pain_point_addressed,
                value_offered=result.value_offered,
                call_to_action=result.call_to_action,
                full_message=result.full_message,
                personalization_data=result.personalization_data,
                ai_model_used=service.model,
                quality_score=result.quality_score,
            )

            # Update user's PVP count
            request.user.pvps_this_month += 1
            request.user.save(update_fields=['pvps_this_month'])

            # Return HTMX partial or JSON response
            if request.htmx:
                return render(request, 'outreach/partials/pvp_result.html', {
                    'pvp': pvp,
                    'quality_breakdown': result.quality_breakdown,
                    'profile': profile,
                })

            return JsonResponse({
                'success': True,
                'pvp_id': pvp.id,
                'quality_score': pvp.quality_score,
                'full_message': pvp.full_message,
            })

        except Exception as e:
            logger.error(f"Error generating PVP: {e}")

            if request.htmx:
                return render(request, 'outreach/partials/pvp_error.html', {
                    'error': str(e)
                })

            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


class PVPListView(LoginRequiredMixin, ListView):
    """List all PVPs for the current user."""
    model = PVP
    template_name = 'outreach/pvp_list.html'
    context_object_name = 'pvps'
    paginate_by = 20

    def get_queryset(self):
        """Filter PVPs to current user only."""
        queryset = PVP.objects.filter(
            user=self.request.user
        ).select_related('match', 'match__profile')

        # Filter by pattern type if specified
        pattern_type = self.request.GET.get('pattern_type')
        if pattern_type:
            queryset = queryset.filter(pattern_type=pattern_type)

        # Filter by quality score range
        min_score = self.request.GET.get('min_score')
        if min_score:
            queryset = queryset.filter(quality_score__gte=float(min_score))

        return queryset

    def get_context_data(self, **kwargs):
        """Add pattern choices to context."""
        context = super().get_context_data(**kwargs)
        context['pattern_choices'] = PVP.PATTERN_TYPE_CHOICES
        context['current_pattern'] = self.request.GET.get('pattern_type', '')
        context['min_score'] = self.request.GET.get('min_score', '')
        return context


class PVPDetailView(LoginRequiredMixin, View):
    """View and edit a specific PVP."""
    template_name = 'outreach/pvp_detail.html'

    def get(self, request, pk):
        """Display PVP details."""
        pvp = get_object_or_404(PVP, pk=pk, user=request.user)

        context = {
            'pvp': pvp,
            'match': pvp.match,
            'profile': pvp.match.profile if pvp.match else None,
        }

        return render(request, self.template_name, context)

    def post(self, request, pk):
        """Update PVP content."""
        pvp = get_object_or_404(PVP, pk=pk, user=request.user)

        # Update editable fields
        full_message = request.POST.get('full_message')
        if full_message:
            pvp.full_message = full_message

        pain_point = request.POST.get('pain_point_addressed')
        if pain_point:
            pvp.pain_point_addressed = pain_point

        value_offered = request.POST.get('value_offered')
        if value_offered:
            pvp.value_offered = value_offered

        call_to_action = request.POST.get('call_to_action')
        if call_to_action:
            pvp.call_to_action = call_to_action

        pvp.save()

        messages.success(request, 'PVP updated successfully.')

        if request.htmx:
            return render(request, 'outreach/partials/pvp_content.html', {
                'pvp': pvp
            })

        return redirect('outreach:pvp_detail', pk=pvp.pk)


class TemplateListView(LoginRequiredMixin, ListView):
    """List all outreach templates for the current user."""
    model = OutreachTemplate
    template_name = 'outreach/template_list.html'
    context_object_name = 'templates'
    paginate_by = 20

    def get_queryset(self):
        """Filter templates to current user only."""
        queryset = OutreachTemplate.objects.filter(user=self.request.user)

        # Filter by category if specified
        category = self.request.GET.get('category')
        if category:
            queryset = queryset.filter(category=category)

        # Filter by sequence position
        position = self.request.GET.get('position')
        if position:
            queryset = queryset.filter(sequence_position=int(position))

        return queryset

    def get_context_data(self, **kwargs):
        """Add category choices to context."""
        context = super().get_context_data(**kwargs)
        context['category_choices'] = OutreachTemplate.CATEGORY_CHOICES
        context['current_category'] = self.request.GET.get('category', '')
        context['current_position'] = self.request.GET.get('position', '')
        context['positions'] = [1, 2, 3, 4]
        return context


class TemplateCreateView(LoginRequiredMixin, View):
    """Create a new GEX-style outreach template."""
    template_name = 'outreach/template_form.html'

    def get(self, request):
        """Display template creation form."""
        context = {
            'category_choices': OutreachTemplate.CATEGORY_CHOICES,
            'positions': [1, 2, 3, 4],
            'common_variables': [
                '{{first_name}}',
                '{{company}}',
                '{{pain_point}}',
                '{{value_prop}}',
                '{{cta}}',
                '{{industry}}',
                '{{recent_news}}',
            ],
            'is_edit': False,
        }
        return render(request, self.template_name, context)

    def post(self, request):
        """Create a new template."""
        name = request.POST.get('name')
        sequence_position = request.POST.get('sequence_position')
        subject_line = request.POST.get('subject_line')
        body_template = request.POST.get('body_template')
        category = request.POST.get('category')
        variables_raw = request.POST.get('variables', '')

        # Parse variables from comma-separated list
        variables = [v.strip() for v in variables_raw.split(',') if v.strip()]

        # Validate required fields
        if not all([name, sequence_position, subject_line, body_template, category]):
            messages.error(request, 'Please fill in all required fields.')
            return redirect('outreach:template_create')

        try:
            template = OutreachTemplate.objects.create(
                user=request.user,
                name=name,
                sequence_position=int(sequence_position),
                subject_line=subject_line,
                body_template=body_template,
                category=category,
                variables=variables,
            )

            messages.success(request, f'Template "{name}" created successfully.')
            return redirect('outreach:template_list')

        except Exception as e:
            logger.error(f"Error creating template: {e}")
            messages.error(request, f'Error creating template: {e}')
            return redirect('outreach:template_create')


class TemplateEditView(LoginRequiredMixin, View):
    """Edit an existing outreach template."""
    template_name = 'outreach/template_form.html'

    def get(self, request, pk):
        """Display template edit form."""
        template = get_object_or_404(OutreachTemplate, pk=pk, user=request.user)

        context = {
            'template': template,
            'category_choices': OutreachTemplate.CATEGORY_CHOICES,
            'positions': [1, 2, 3, 4],
            'common_variables': [
                '{{first_name}}',
                '{{company}}',
                '{{pain_point}}',
                '{{value_prop}}',
                '{{cta}}',
                '{{industry}}',
                '{{recent_news}}',
            ],
            'is_edit': True,
        }
        return render(request, self.template_name, context)

    def post(self, request, pk):
        """Update the template."""
        template = get_object_or_404(OutreachTemplate, pk=pk, user=request.user)

        template.name = request.POST.get('name', template.name)
        template.sequence_position = int(request.POST.get('sequence_position', template.sequence_position))
        template.subject_line = request.POST.get('subject_line', template.subject_line)
        template.body_template = request.POST.get('body_template', template.body_template)
        template.category = request.POST.get('category', template.category)

        variables_raw = request.POST.get('variables', '')
        template.variables = [v.strip() for v in variables_raw.split(',') if v.strip()]

        template.is_active = request.POST.get('is_active') == 'on'

        template.save()

        messages.success(request, f'Template "{template.name}" updated successfully.')
        return redirect('outreach:template_list')


class CampaignListView(LoginRequiredMixin, ListView):
    """List all outreach campaigns for the current user."""
    model = OutreachCampaign
    template_name = 'outreach/campaign_list.html'
    context_object_name = 'campaigns'
    paginate_by = 20

    def get_queryset(self):
        """Filter campaigns to current user only."""
        queryset = OutreachCampaign.objects.filter(
            user=self.request.user
        ).select_related('template')

        # Filter by status if specified
        status = self.request.GET.get('status')
        if status:
            queryset = queryset.filter(status=status)

        return queryset

    def get_context_data(self, **kwargs):
        """Add status choices to context."""
        context = super().get_context_data(**kwargs)
        context['status_choices'] = OutreachCampaign.STATUS_CHOICES
        context['current_status'] = self.request.GET.get('status', '')
        return context


class CampaignCreateView(LoginRequiredMixin, View):
    """Create a new outreach campaign."""
    template_name = 'outreach/campaign_form.html'

    def get(self, request):
        """Display campaign creation form."""
        templates = OutreachTemplate.objects.filter(
            user=request.user,
            is_active=True
        )
        profiles = Profile.objects.filter(user=request.user)[:100]

        # Get matches for easier profile selection
        matches = Match.objects.filter(
            user=request.user
        ).select_related('profile').order_by('-final_score')[:50]

        context = {
            'templates': templates,
            'profiles': profiles,
            'matches': matches,
            'status_choices': OutreachCampaign.STATUS_CHOICES,
        }
        return render(request, self.template_name, context)

    def post(self, request):
        """Create a new campaign."""
        name = request.POST.get('name')
        template_id = request.POST.get('template_id')
        profile_ids = request.POST.getlist('profile_ids')
        status = request.POST.get('status', 'draft')

        if not name:
            messages.error(request, 'Please provide a campaign name.')
            return redirect('outreach:campaign_create')

        try:
            template = None
            if template_id:
                template = OutreachTemplate.objects.get(
                    id=template_id,
                    user=request.user
                )

            campaign = OutreachCampaign.objects.create(
                user=request.user,
                name=name,
                template=template,
                status=status,
            )

            # Add target profiles
            if profile_ids:
                profiles = Profile.objects.filter(
                    id__in=profile_ids,
                    user=request.user
                )
                campaign.target_profiles.set(profiles)

            messages.success(request, f'Campaign "{name}" created successfully.')
            return redirect('outreach:campaign_list')

        except OutreachTemplate.DoesNotExist:
            messages.error(request, 'Selected template not found.')
            return redirect('outreach:campaign_create')
        except Exception as e:
            logger.error(f"Error creating campaign: {e}")
            messages.error(request, f'Error creating campaign: {e}')
            return redirect('outreach:campaign_create')


class CampaignDetailView(LoginRequiredMixin, View):
    """View and manage a specific campaign."""
    template_name = 'outreach/campaign_detail.html'

    def get(self, request, pk):
        """Display campaign details."""
        campaign = get_object_or_404(
            OutreachCampaign,
            pk=pk,
            user=request.user
        )

        context = {
            'campaign': campaign,
            'profiles': campaign.target_profiles.all(),
            'activities': campaign.activities.all()[:50],
        }
        return render(request, self.template_name, context)


@method_decorator(csrf_exempt, name='dispatch')
class ClayWebhookView(View):
    """
    Webhook endpoint for Clay enrichment data.

    POST: Receive and process enrichment data from Clay
    """

    def post(self, request):
        """Handle Clay webhook POST request."""
        # Get signature from header
        signature = request.headers.get('X-Clay-Signature', '')

        # Initialize service
        service = ClayWebhookService()

        # Validate signature
        if not service.validate_signature(request.body, signature):
            logger.warning("Invalid Clay webhook signature")
            return JsonResponse({
                'error': 'Invalid signature'
            }, status=401)

        try:
            # Parse JSON payload
            payload = json.loads(request.body)

            # Process the webhook
            results = service.process_webhook(payload)

            logger.info(
                f"Clay webhook processed: {results['processed']} success, "
                f"{results['failed']} failed"
            )

            return JsonResponse({
                'success': True,
                'processed': results['processed'],
                'failed': results['failed'],
                'errors': results['errors'][:10] if results['errors'] else []
            })

        except json.JSONDecodeError:
            logger.error("Invalid JSON in Clay webhook")
            return JsonResponse({
                'error': 'Invalid JSON payload'
            }, status=400)
        except Exception as e:
            logger.error(f"Error processing Clay webhook: {e}")
            return JsonResponse({
                'error': str(e)
            }, status=500)

    def get(self, request):
        """Health check endpoint for Clay webhook."""
        return JsonResponse({
            'status': 'ok',
            'endpoint': 'clay-webhook'
        })


@method_decorator(csrf_exempt, name='dispatch')
class ClaySupabaseWebhookView(View):
    """
    Webhook endpoint for Clay enrichment data targeting SupabaseProfile.

    This endpoint updates SupabaseProfile records (the main partner database)
    with enrichment data from Clay/Claygent.

    POST: Receive and process enrichment data from Clay
    GET: Health check
    """

    def post(self, request):
        """Handle Clay webhook POST request for SupabaseProfile updates."""
        # Get signature from header
        signature = request.headers.get('X-Clay-Signature', '')

        # Initialize service
        service = ClayWebhookService()

        # Validate signature
        if not service.validate_signature(request.body, signature):
            logger.warning("Invalid Clay webhook signature for Supabase endpoint")
            return JsonResponse({
                'error': 'Invalid signature'
            }, status=401)

        try:
            # Parse JSON payload
            payload = json.loads(request.body)

            # Process the webhook for SupabaseProfile
            results = service.process_supabase_webhook(payload)

            logger.info(
                f"Clay Supabase webhook processed: {results['processed']} success, "
                f"{results['failed']} failed"
            )

            return JsonResponse({
                'success': True,
                'processed': results['processed'],
                'failed': results['failed'],
                'updated_profiles': results['updated_profiles'][:10],  # Limit response size
                'errors': results['errors'][:10] if results['errors'] else []
            })

        except json.JSONDecodeError:
            logger.error("Invalid JSON in Clay Supabase webhook")
            return JsonResponse({
                'error': 'Invalid JSON payload'
            }, status=400)
        except Exception as e:
            logger.error(f"Error processing Clay Supabase webhook: {e}")
            return JsonResponse({
                'error': str(e)
            }, status=500)

    def get(self, request):
        """Health check endpoint for Clay Supabase webhook."""
        return JsonResponse({
            'status': 'ok',
            'endpoint': 'clay-supabase-webhook',
            'description': 'Updates SupabaseProfile records with Clay enrichment data'
        })


class SequenceListView(LoginRequiredMixin, ListView):
    """List user's outreach sequences."""
    model = OutreachSequence
    template_name = 'outreach/sequence_list.html'
    context_object_name = 'sequences'
    paginate_by = 20

    def get_queryset(self):
        """Filter sequences to current user only."""
        return OutreachSequence.objects.filter(
            user=self.request.user
        ).select_related('target_profile').prefetch_related('emails')


class SequenceCreateView(LoginRequiredMixin, CreateView):
    """Create a new outreach sequence."""
    model = OutreachSequence
    template_name = 'outreach/sequence_form.html'
    fields = ['name', 'sequence_type', 'target_profile']
    success_url = '/outreach/sequences/'

    def get_form(self, form_class=None):
        """Customize the form."""
        form = super().get_form(form_class)
        # Filter target profiles to Supabase profiles
        form.fields['target_profile'].queryset = SupabaseProfile.objects.filter(
            status='Member'
        ).exclude(name__isnull=True).exclude(name='')
        return form

    def form_valid(self, form):
        """Set user and generate emails."""
        form.instance.user = self.request.user
        response = super().form_valid(form)

        # TODO: Generate emails using SequenceGeneratorService
        # For now, create placeholder emails
        self._create_placeholder_emails(form.instance)

        messages.success(
            self.request,
            f'Sequence "{form.instance.name}" created successfully.'
        )
        return response

    def _create_placeholder_emails(self, sequence):
        """Create 4 placeholder emails for the sequence."""
        email_templates = [
            {
                'number': 1,
                'is_threaded': False,
                'subject': f"Quick question about {sequence.target_profile.name}",
                'body': "Email 1 content will be AI-generated based on sequence type..."
            },
            {
                'number': 2,
                'is_threaded': True,
                'subject': "Re: Quick question",
                'body': "Email 2 follow-up content..."
            },
            {
                'number': 3,
                'is_threaded': False,
                'subject': "Different approach",
                'body': "Email 3 with different value prop..."
            },
            {
                'number': 4,
                'is_threaded': True,
                'subject': "Re: Different approach",
                'body': "Email 4 breakup email..."
            }
        ]

        for template in email_templates:
            OutreachEmail.objects.create(
                sequence=sequence,
                email_number=template['number'],
                is_threaded=template['is_threaded'],
                subject_line=template['subject'],
                body=template['body']
            )


class SequenceDetailView(LoginRequiredMixin, DetailView):
    """View sequence with all emails."""
    model = OutreachSequence
    template_name = 'outreach/sequence_detail.html'
    context_object_name = 'sequence'

    def get_queryset(self):
        """Filter to user's sequences."""
        return OutreachSequence.objects.filter(
            user=self.request.user
        ).prefetch_related('emails')


class SequenceGenerateView(LoginRequiredMixin, View):
    """HTMX endpoint to generate/regenerate emails."""

    def post(self, request, pk):
        """Generate or regenerate sequence emails."""
        sequence = get_object_or_404(
            OutreachSequence,
            pk=pk,
            user=request.user
        )

        try:
            # TODO: Call SequenceGeneratorService here
            # For now, just return success
            messages.success(request, 'Email sequence generated successfully.')

            if request.htmx:
                # Return updated emails partial
                return render(request, 'outreach/partials/sequence_emails.html', {
                    'sequence': sequence,
                    'emails': sequence.emails.all()
                })

            return redirect('outreach:sequence_detail', pk=sequence.pk)

        except Exception as e:
            logger.error(f"Error generating sequence: {e}")
            messages.error(request, f'Error generating sequence: {e}')

            if request.htmx:
                return render(request, 'outreach/partials/error.html', {
                    'error': str(e)
                })

            return redirect('outreach:sequence_detail', pk=sequence.pk)


# =============================================================================
# Email Integration Views
# =============================================================================

class EmailSettingsView(LoginRequiredMixin, TemplateView):
    """View for managing connected email accounts."""
    template_name = 'outreach/email_settings.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['email_connections'] = EmailConnection.objects.filter(
            user=self.request.user,
            is_active=True
        )
        context['has_google_oauth'] = bool(settings.GOOGLE_OAUTH_CLIENT_ID)
        context['has_microsoft_oauth'] = bool(settings.MICROSOFT_OAUTH_CLIENT_ID)
        return context


class GoogleOAuthConnectView(LoginRequiredMixin, View):
    """Initiate Google OAuth flow."""

    def get(self, request):
        if not settings.GOOGLE_OAUTH_CLIENT_ID:
            messages.error(request, 'Google OAuth is not configured.')
            return redirect('outreach:email_settings')

        # Generate state token to prevent CSRF
        import secrets
        state = secrets.token_urlsafe(32)
        request.session['google_oauth_state'] = state

        auth_url = OAuthHelper.get_google_auth_url(state)
        return redirect(auth_url)


class GoogleOAuthCallbackView(LoginRequiredMixin, View):
    """Handle Google OAuth callback."""

    def get(self, request):
        error = request.GET.get('error')
        if error:
            messages.error(request, f'Google authorization failed: {error}')
            return redirect('outreach:email_settings')

        # Verify state
        state = request.GET.get('state')
        session_state = request.session.pop('google_oauth_state', None)
        if state != session_state:
            messages.error(request, 'Invalid OAuth state. Please try again.')
            return redirect('outreach:email_settings')

        code = request.GET.get('code')
        if not code:
            messages.error(request, 'No authorization code received.')
            return redirect('outreach:email_settings')

        try:
            # Exchange code for tokens
            token_data = OAuthHelper.exchange_google_code(code)

            # Get user's email from the token
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build

            creds = Credentials(token=token_data['access_token'])
            service = build('oauth2', 'v2', credentials=creds)
            user_info = service.userinfo().get().execute()
            email_address = user_info.get('email')

            # Create or update connection
            connection, created = EmailConnection.objects.update_or_create(
                user=request.user,
                email_address=email_address,
                defaults={
                    'provider': 'gmail',
                    'access_token': token_data['access_token'],
                    'refresh_token': token_data.get('refresh_token', ''),
                    'token_expires_at': token_data['expires_at'],
                    'scopes': settings.GOOGLE_OAUTH_SCOPES,
                    'is_active': True,
                }
            )

            # Set as primary if it's the first connection
            if created and not EmailConnection.objects.filter(
                user=request.user, is_primary=True
            ).exists():
                connection.mark_as_primary()

            messages.success(
                request,
                f'Successfully connected {email_address}!'
            )

        except Exception as e:
            logger.error(f"Google OAuth error: {e}")
            messages.error(request, f'Failed to connect Google account: {e}')

        return redirect('outreach:email_settings')


class MicrosoftOAuthConnectView(LoginRequiredMixin, View):
    """Initiate Microsoft OAuth flow."""

    def get(self, request):
        if not settings.MICROSOFT_OAUTH_CLIENT_ID:
            messages.error(request, 'Microsoft OAuth is not configured.')
            return redirect('outreach:email_settings')

        import secrets
        state = secrets.token_urlsafe(32)
        request.session['microsoft_oauth_state'] = state

        auth_url = OAuthHelper.get_microsoft_auth_url(state)
        return redirect(auth_url)


class MicrosoftOAuthCallbackView(LoginRequiredMixin, View):
    """Handle Microsoft OAuth callback."""

    def get(self, request):
        error = request.GET.get('error')
        if error:
            error_desc = request.GET.get('error_description', error)
            messages.error(request, f'Microsoft authorization failed: {error_desc}')
            return redirect('outreach:email_settings')

        # Verify state
        state = request.GET.get('state')
        session_state = request.session.pop('microsoft_oauth_state', None)
        if state != session_state:
            messages.error(request, 'Invalid OAuth state. Please try again.')
            return redirect('outreach:email_settings')

        code = request.GET.get('code')
        if not code:
            messages.error(request, 'No authorization code received.')
            return redirect('outreach:email_settings')

        try:
            # Exchange code for tokens
            token_data = OAuthHelper.exchange_microsoft_code(code)

            # Get user's email from Microsoft Graph
            import requests as http_requests
            headers = {'Authorization': f"Bearer {token_data['access_token']}"}
            response = http_requests.get(
                'https://graph.microsoft.com/v1.0/me',
                headers=headers
            )
            user_info = response.json()
            email_address = user_info.get('mail') or user_info.get('userPrincipalName')

            # Create or update connection
            connection, created = EmailConnection.objects.update_or_create(
                user=request.user,
                email_address=email_address,
                defaults={
                    'provider': 'outlook',
                    'access_token': token_data['access_token'],
                    'refresh_token': token_data.get('refresh_token', ''),
                    'token_expires_at': token_data['expires_at'],
                    'scopes': settings.MICROSOFT_OAUTH_SCOPES,
                    'is_active': True,
                }
            )

            # Set as primary if it's the first connection
            if created and not EmailConnection.objects.filter(
                user=request.user, is_primary=True
            ).exists():
                connection.mark_as_primary()

            messages.success(
                request,
                f'Successfully connected {email_address}!'
            )

        except Exception as e:
            logger.error(f"Microsoft OAuth error: {e}")
            messages.error(request, f'Failed to connect Microsoft account: {e}')

        return redirect('outreach:email_settings')


class EmailDisconnectView(LoginRequiredMixin, View):
    """Disconnect an email account."""

    def post(self, request, pk):
        connection = get_object_or_404(
            EmailConnection,
            pk=pk,
            user=request.user
        )

        email = connection.email_address
        connection.is_active = False
        connection.save()

        messages.success(request, f'Disconnected {email}')

        if request.htmx:
            # Return updated connections list
            connections = EmailConnection.objects.filter(
                user=request.user,
                is_active=True
            )
            return render(request, 'outreach/partials/email_connections_list.html', {
                'email_connections': connections
            })

        return redirect('outreach:email_settings')


class SendEmailView(LoginRequiredMixin, View):
    """Send an email via connected account or return mailto link."""

    def post(self, request):
        """Send email or return mailto link."""
        to_email = request.POST.get('to_email', '')
        to_name = request.POST.get('to_name', '')
        subject = request.POST.get('subject', '')
        body = request.POST.get('body', '')
        pvp_id = request.POST.get('pvp_id')

        if not to_email or not subject or not body:
            return JsonResponse({
                'success': False,
                'error': 'Missing required fields: to_email, subject, body'
            }, status=400)

        # Get user's primary email connection
        connection = EmailConnection.objects.filter(
            user=request.user,
            is_active=True,
            is_primary=True
        ).first()

        # If no primary, get any active connection
        if not connection:
            connection = EmailConnection.objects.filter(
                user=request.user,
                is_active=True
            ).first()

        # If no connection, return mailto link
        if not connection:
            mailto_link = generate_mailto_link(to_email, subject, body, to_name)
            return JsonResponse({
                'success': True,
                'method': 'mailto',
                'mailto_link': mailto_link,
                'message': 'No email account connected. Use the link to open your email client.'
            })

        try:
            # Send via connected account
            email_service = EmailService(connection)
            result = email_service.send_email(
                to_email=to_email,
                subject=subject,
                body=body,
                to_name=to_name
            )

            # Record the sent email
            sent_email = SentEmail.objects.create(
                user=request.user,
                email_connection=connection,
                pvp_id=pvp_id if pvp_id else None,
                recipient_email=to_email,
                recipient_name=to_name,
                subject=subject,
                body=body,
                provider_message_id=result.get('message_id', ''),
                thread_id=result.get('thread_id', ''),
                status='sent',
                sent_at=timezone.now()
            )

            return JsonResponse({
                'success': True,
                'method': 'oauth',
                'message': f'Email sent successfully via {connection.email_address}',
                'sent_email_id': sent_email.id
            })

        except Exception as e:
            logger.error(f"Error sending email: {e}")

            # Record failed attempt
            SentEmail.objects.create(
                user=request.user,
                email_connection=connection,
                pvp_id=pvp_id if pvp_id else None,
                recipient_email=to_email,
                recipient_name=to_name,
                subject=subject,
                body=body,
                status='failed',
                error_message=str(e)
            )

            # Fall back to mailto
            mailto_link = generate_mailto_link(to_email, subject, body, to_name)
            return JsonResponse({
                'success': False,
                'error': str(e),
                'method': 'mailto',
                'mailto_link': mailto_link,
                'message': 'Failed to send via connected account. Use the link to open your email client.'
            })


class SentEmailListView(LoginRequiredMixin, ListView):
    """View list of sent emails."""
    model = SentEmail
    template_name = 'outreach/sent_email_list.html'
    context_object_name = 'sent_emails'
    paginate_by = 20

    def get_queryset(self):
        return SentEmail.objects.filter(
            user=self.request.user
        ).select_related('email_connection', 'pvp')
