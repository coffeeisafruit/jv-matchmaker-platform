"""
Views for the Playbook app - Launch playbook generation and management.
"""

from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.views.generic import ListView, DetailView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponse
from django.urls import reverse_lazy
from django.contrib import messages
from django.utils import timezone

from .models import GeneratedPlaybook, GeneratedPlay, LaunchPlay
from .forms import PlaybookCreateForm


class PlaybookListView(LoginRequiredMixin, ListView):
    """List all playbooks for the current user."""

    model = GeneratedPlaybook
    template_name = 'playbook/playbook_list.html'
    context_object_name = 'playbooks'

    def get_queryset(self):
        return GeneratedPlaybook.objects.filter(user=self.request.user)


class PlaybookDetailView(LoginRequiredMixin, DetailView):
    """View a playbook with all its plays."""

    model = GeneratedPlaybook
    template_name = 'playbook/playbook_detail.html'
    context_object_name = 'playbook'

    def get_queryset(self):
        return GeneratedPlaybook.objects.filter(user=self.request.user)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Get all plays for this playbook
        context['plays'] = self.object.plays.select_related('launch_play').all()
        return context


class PlaybookCreateView(LoginRequiredMixin, CreateView):
    """Create a new playbook."""

    model = GeneratedPlaybook
    form_class = PlaybookCreateForm
    template_name = 'playbook/playbook_form.html'

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        form.instance.user = self.request.user
        response = super().form_valid(form)

        # Create GeneratedPlay instances based on size
        playbook = self.object
        size = playbook.size

        # Filter plays based on size
        if size == 'small':
            plays = LaunchPlay.objects.filter(included_in_small=True)
        elif size == 'medium':
            plays = LaunchPlay.objects.filter(included_in_medium=True)
        else:  # large
            plays = LaunchPlay.objects.all()

        # Create GeneratedPlay instances for each selected play
        for play in plays:
            GeneratedPlay.objects.create(
                playbook=playbook,
                launch_play=play,
            )

        messages.success(
            self.request,
            f'Playbook "{playbook.name}" created successfully with {plays.count()} plays!'
        )
        return response

    def get_success_url(self):
        return reverse_lazy('playbook:detail', kwargs={'pk': self.object.pk})


class PlaybookGenerateView(LoginRequiredMixin, View):
    """HTMX endpoint to generate AI content for plays in a playbook."""

    def post(self, request, pk):
        playbook = get_object_or_404(
            GeneratedPlaybook,
            pk=pk,
            user=request.user
        )

        # Get the transformation analysis if linked
        transformation = playbook.transformation

        if not transformation:
            return JsonResponse({
                'error': 'No transformation analysis linked to this playbook. Please link a transformation first.'
            }, status=400)

        # TODO: Implement AI generation using transformation context
        # For now, return a placeholder response

        # Get all plays that need generation
        plays_to_generate = playbook.plays.filter(custom_content='')

        if not plays_to_generate.exists():
            return JsonResponse({
                'success': True,
                'message': 'All plays already have content generated.'
            })

        # Placeholder: Mark plays as having some content
        # In production, this would call an AI service
        for play in plays_to_generate:
            play.custom_content = f"AI-generated content for {play.launch_play.name} based on transformation analysis."
            play.custom_hook = f"Hook for {play.launch_play.name}"
            play.custom_cta = f"CTA for {play.launch_play.name}"
            play.save()

        return JsonResponse({
            'success': True,
            'message': f'Generated content for {plays_to_generate.count()} plays.',
            'generated_count': plays_to_generate.count()
        })


class PlayMarkCompleteView(LoginRequiredMixin, View):
    """HTMX endpoint to mark a play as complete."""

    def post(self, request, pk):
        play = get_object_or_404(
            GeneratedPlay,
            pk=pk,
            playbook__user=request.user
        )

        # Toggle completion status
        play.is_completed = not play.is_completed
        if play.is_completed:
            play.completed_at = timezone.now()
        else:
            play.completed_at = None
        play.save()

        # For HTMX requests, return the updated play partial
        if request.htmx:
            return render(request, 'playbook/partials/play_row.html', {
                'play': play
            })

        return JsonResponse({
            'success': True,
            'is_completed': play.is_completed,
            'completed_at': play.completed_at.isoformat() if play.completed_at else None
        })
