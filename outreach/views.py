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
from django.views.generic import ListView, DetailView, CreateView, UpdateView

from matching.models import Match, Profile
from positioning.models import ICP
from .models import PVP, OutreachTemplate, OutreachCampaign
from .services import PVPGeneratorService, ClayWebhookService

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
        # Get user's matches ordered by score
        matches = Match.objects.filter(
            user=request.user
        ).select_related('profile').order_by('-final_score')[:20]

        # Get user's primary ICP
        primary_icp = ICP.objects.filter(
            user=request.user,
            is_primary=True
        ).first()

        # Get all user ICPs for selection
        icps = ICP.objects.filter(user=request.user)

        # Get pattern type choices
        pattern_choices = PVP.PATTERN_TYPE_CHOICES

        # Get selected match if provided
        selected_match_id = request.GET.get('match_id')
        selected_match = None
        if selected_match_id:
            selected_match = Match.objects.filter(
                id=selected_match_id,
                user=request.user
            ).select_related('profile').first()

        context = {
            'matches': matches,
            'primary_icp': primary_icp,
            'icps': icps,
            'pattern_choices': pattern_choices,
            'selected_match': selected_match,
        }

        return render(request, self.template_name, context)

    def post(self, request):
        """Generate a PVP for the selected match."""
        match_id = request.POST.get('match_id')
        pattern_type = request.POST.get('pattern_type', 'pain_solution')
        icp_id = request.POST.get('icp_id')

        # Validate match
        match = get_object_or_404(Match, id=match_id, user=request.user)

        # Get ICP if specified
        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, id=icp_id, user=request.user)
        else:
            # Use primary ICP if available
            icp = ICP.objects.filter(user=request.user, is_primary=True).first()

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

            # Generate PVP
            service = PVPGeneratorService(api_key=api_key)
            result = service.generate_pvp(match, pattern_type, icp)

            # Save the PVP
            pvp = PVP.objects.create(
                user=request.user,
                match=match,
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
                    'match': match,
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
