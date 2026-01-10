"""
Views for the Positioning Wizard - ICP and Transformation Analysis.
"""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.views.generic import ListView, DetailView, CreateView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.urls import reverse_lazy
from django.contrib import messages
from django.template.loader import render_to_string

from .models import ICP, TransformationAnalysis
from .services import TransformationService


class ICPListView(LoginRequiredMixin, ListView):
    """List all ICPs for the current user."""

    model = ICP
    template_name = 'positioning/icp_list.html'
    context_object_name = 'icps'

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transformations'] = TransformationAnalysis.objects.filter(
            user=self.request.user
        )[:5]
        return context


class ICPCreateView(LoginRequiredMixin, View):
    """Multi-step wizard for creating an ICP."""

    template_name = 'positioning/icp_form.html'

    def get(self, request):
        """Display the ICP creation wizard."""
        step = request.GET.get('step', '1')

        # Get session data for the wizard
        wizard_data = request.session.get('icp_wizard', {})

        context = {
            'step': int(step),
            'wizard_data': wizard_data,
            'company_size_choices': ICP.COMPANY_SIZE_CHOICES,
        }

        # For HTMX requests, return only the step partial
        if request.htmx:
            return render(request, f'positioning/partials/icp_step_{step}.html', context)

        return render(request, self.template_name, context)

    def post(self, request):
        """Process wizard step submission."""
        step = request.POST.get('step', '1')
        action = request.POST.get('action', 'next')

        # Initialize or get session data
        wizard_data = request.session.get('icp_wizard', {})

        # Process based on current step
        if step == '1':
            wizard_data['name'] = request.POST.get('name', '')
            wizard_data['industry'] = request.POST.get('industry', '')
            wizard_data['company_size'] = request.POST.get('company_size', '')

        elif step == '2':
            pain_points = request.POST.getlist('pain_points[]')
            wizard_data['pain_points'] = [p for p in pain_points if p.strip()]

        elif step == '3':
            goals = request.POST.getlist('goals[]')
            wizard_data['goals'] = [g for g in goals if g.strip()]

        elif step == '4':
            wizard_data['budget_range'] = request.POST.get('budget_range', '')

            # Process decision makers as JSON
            decision_makers_raw = request.POST.get('decision_makers', '[]')
            try:
                wizard_data['decision_makers'] = json.loads(decision_makers_raw)
            except json.JSONDecodeError:
                wizard_data['decision_makers'] = []

        # Save to session
        request.session['icp_wizard'] = wizard_data
        request.session.modified = True

        # Handle navigation
        current_step = int(step)

        if action == 'back':
            next_step = max(1, current_step - 1)
        elif action == 'save':
            # Final save - create the ICP
            return self._create_icp(request, wizard_data)
        else:
            next_step = min(4, current_step + 1)

        context = {
            'step': next_step,
            'wizard_data': wizard_data,
            'company_size_choices': ICP.COMPANY_SIZE_CHOICES,
        }

        # For HTMX requests, return only the step partial
        if request.htmx:
            return render(request, f'positioning/partials/icp_step_{next_step}.html', context)

        return render(request, self.template_name, context)

    def _create_icp(self, request, wizard_data):
        """Create the ICP from wizard data."""
        try:
            # Check if this should be the primary ICP
            is_primary = not ICP.objects.filter(user=request.user).exists()

            icp = ICP.objects.create(
                user=request.user,
                name=wizard_data.get('name', ''),
                industry=wizard_data.get('industry', ''),
                company_size=wizard_data.get('company_size', 'small'),
                pain_points=wizard_data.get('pain_points', []),
                goals=wizard_data.get('goals', []),
                budget_range=wizard_data.get('budget_range', ''),
                decision_makers=wizard_data.get('decision_makers', []),
                is_primary=is_primary,
            )

            # Clear wizard session data
            if 'icp_wizard' in request.session:
                del request.session['icp_wizard']

            messages.success(request, f'ICP "{icp.name}" created successfully!')

            if request.htmx:
                response = HttpResponse()
                response['HX-Redirect'] = reverse_lazy('positioning:icp_detail', kwargs={'pk': icp.pk})
                return response

            return redirect('positioning:icp_detail', pk=icp.pk)

        except Exception as e:
            messages.error(request, f'Error creating ICP: {str(e)}')
            return redirect('positioning:icp_create')


class ICPDetailView(LoginRequiredMixin, DetailView):
    """View ICP details."""

    model = ICP
    template_name = 'positioning/icp_detail.html'
    context_object_name = 'icp'

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transformations'] = TransformationAnalysis.objects.filter(
            icp=self.object,
            user=self.request.user
        )
        return context


class ICPUpdateView(LoginRequiredMixin, UpdateView):
    """Edit an existing ICP."""

    model = ICP
    template_name = 'positioning/icp_update.html'
    fields = ['name', 'industry', 'company_size', 'pain_points', 'goals',
              'budget_range', 'decision_makers', 'is_primary']

    def get_queryset(self):
        return ICP.objects.filter(user=self.request.user)

    def get_success_url(self):
        return reverse_lazy('positioning:icp_detail', kwargs={'pk': self.object.pk})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['company_size_choices'] = ICP.COMPANY_SIZE_CHOICES
        return context

    def form_valid(self, form):
        messages.success(self.request, f'ICP "{form.instance.name}" updated successfully!')
        return super().form_valid(form)


class ICPDeleteView(LoginRequiredMixin, View):
    """Delete an ICP."""

    def post(self, request, pk):
        icp = get_object_or_404(ICP, pk=pk, user=request.user)
        name = icp.name
        icp.delete()

        messages.success(request, f'ICP "{name}" deleted successfully!')

        if request.htmx:
            response = HttpResponse()
            response['HX-Redirect'] = reverse_lazy('positioning:icp_list')
            return response

        return redirect('positioning:icp_list')


class ICPSetPrimaryView(LoginRequiredMixin, View):
    """Set an ICP as the primary ICP."""

    def post(self, request, pk):
        icp = get_object_or_404(ICP, pk=pk, user=request.user)

        # Remove primary from all other ICPs
        ICP.objects.filter(user=request.user).update(is_primary=False)

        # Set this one as primary
        icp.is_primary = True
        icp.save()

        messages.success(request, f'"{icp.name}" is now your primary ICP!')

        if request.htmx:
            return render(request, 'positioning/partials/icp_card.html', {'icp': icp})

        return redirect('positioning:icp_detail', pk=pk)


class TransformationCreateView(LoginRequiredMixin, View):
    """Create a new transformation analysis."""

    template_name = 'positioning/transformation_form.html'

    def get(self, request):
        """Display the transformation creation form."""
        icps = ICP.objects.filter(user=request.user)
        preselected_icp = request.GET.get('icp')

        context = {
            'icps': icps,
            'preselected_icp': preselected_icp,
        }

        return render(request, self.template_name, context)

    def post(self, request):
        """Process transformation creation."""
        icp_id = request.POST.get('icp')
        before_state = request.POST.get('before_state', '')
        after_state = request.POST.get('after_state', '')

        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, pk=icp_id, user=request.user)

        # Create the basic transformation
        transformation = TransformationAnalysis.objects.create(
            user=request.user,
            icp=icp,
            before_state=before_state,
            after_state=after_state,
            transformation_summary='',
            key_obstacles=[],
            value_drivers=[],
            ai_generated=False,
        )

        messages.success(request, 'Transformation analysis created!')
        return redirect('positioning:transformation_detail', pk=transformation.pk)


class TransformationGenerateView(LoginRequiredMixin, View):
    """Generate AI analysis for a transformation."""

    def post(self, request):
        """Generate transformation analysis using AI."""
        icp_id = request.POST.get('icp')
        before_state = request.POST.get('before_state', '')
        after_state = request.POST.get('after_state', '')

        icp = None
        if icp_id:
            icp = get_object_or_404(ICP, pk=icp_id, user=request.user)

        # Use the TransformationService
        service = TransformationService()

        try:
            analysis = service.analyze(
                icp=icp,
                before_state=before_state,
                after_state=after_state,
            )

            # Create the transformation with AI analysis
            transformation = TransformationAnalysis.objects.create(
                user=request.user,
                icp=icp,
                before_state=before_state,
                after_state=after_state,
                transformation_summary=analysis.get('transformation_summary', ''),
                key_obstacles=analysis.get('key_obstacles', []),
                value_drivers=analysis.get('value_drivers', []),
                ai_generated=True,
            )

            if request.htmx:
                # Return the analysis results partial
                return render(request, 'positioning/partials/transformation_results.html', {
                    'transformation': transformation,
                    'analysis': analysis,
                })

            return redirect('positioning:transformation_detail', pk=transformation.pk)

        except Exception as e:
            error_message = f'Error generating analysis: {str(e)}'

            if request.htmx:
                return render(request, 'positioning/partials/transformation_error.html', {
                    'error': error_message,
                })

            messages.error(request, error_message)
            return redirect('positioning:transformation_create')


class TransformationDetailView(LoginRequiredMixin, DetailView):
    """View transformation analysis details."""

    model = TransformationAnalysis
    template_name = 'positioning/transformation_detail.html'
    context_object_name = 'transformation'

    def get_queryset(self):
        return TransformationAnalysis.objects.filter(user=self.request.user)


class TransformationListView(LoginRequiredMixin, ListView):
    """List all transformations for the current user."""

    model = TransformationAnalysis
    template_name = 'positioning/transformation_list.html'
    context_object_name = 'transformations'

    def get_queryset(self):
        return TransformationAnalysis.objects.filter(user=self.request.user)


class TransformationDeleteView(LoginRequiredMixin, View):
    """Delete a transformation analysis."""

    def post(self, request, pk):
        transformation = get_object_or_404(
            TransformationAnalysis,
            pk=pk,
            user=request.user
        )
        transformation.delete()

        messages.success(request, 'Transformation analysis deleted!')

        if request.htmx:
            response = HttpResponse()
            response['HX-Redirect'] = reverse_lazy('positioning:transformation_list')
            return response

        return redirect('positioning:transformation_list')
