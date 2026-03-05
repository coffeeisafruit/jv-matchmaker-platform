"""
Views for the Match Evaluation system.

Access-code authenticated (no Django login required).
Two-step evaluation flow:
  Step 1: Rate match quality from raw profiles (no narrative, no score)
  Step 2: Rate narrative quality (why_fit shown after Step 1)

Algorithm scores are stored internally but never shown to reviewers.
"""

import time
from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.utils import timezone
from django.http import Http404

from .models import (
    EvaluationReviewer, EvaluationBatch, EvaluationItem, MatchEvaluation,
)
from .forms_evaluation import MatchQualityForm, NarrativeQualityForm


# =============================================================================
# ACCESS CONTROL
# =============================================================================

class EvalAccessMixin:
    """Mixin that verifies session-based reviewer access."""

    def get_reviewer_or_redirect(self, request):
        reviewer_id = request.session.get('eval_reviewer_id')
        if not reviewer_id:
            return None, redirect('matching:eval-access')
        try:
            reviewer = EvaluationReviewer.objects.get(id=reviewer_id, is_active=True)
        except EvaluationReviewer.DoesNotExist:
            return None, redirect('matching:eval-access')
        return reviewer, None


class EvalAccessView(View):
    """
    Code-gated entry point for the evaluation system.
    No login required — reviewers authenticate via personal access code.
    """

    def get(self, request):
        return render(request, 'matching/evaluation/access.html')

    def post(self, request):
        code = request.POST.get('code', '').strip()

        # Rate limiting: 5 attempts per 15 minutes
        now = time.time()
        attempts = request.session.get('eval_access_attempts', [])
        window = 15 * 60
        attempts = [t for t in attempts if now - t < window]

        if len(attempts) >= 5:
            messages.error(
                request,
                'Too many attempts. Please wait 15 minutes before trying again.',
            )
            return render(request, 'matching/evaluation/access.html')

        attempts.append(now)
        request.session['eval_access_attempts'] = attempts

        try:
            reviewer = EvaluationReviewer.objects.get(access_code__iexact=code, is_active=True)
        except EvaluationReviewer.DoesNotExist:
            messages.error(request, 'Invalid access code.')
            return render(request, 'matching/evaluation/access.html')

        # Grant access
        request.session['eval_reviewer_id'] = reviewer.id
        return redirect('matching:eval-dashboard')


# =============================================================================
# DASHBOARD
# =============================================================================

class EvalDashboardView(EvalAccessMixin, View):
    """Reviewer's personal dashboard showing assigned batches and progress."""

    def get(self, request):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        batches = EvaluationBatch.objects.filter(
            assigned_reviewers=reviewer,
            status=EvaluationBatch.Status.ACTIVE,
        )

        batch_info = []
        for batch in batches:
            completed, total = batch.completion_for_reviewer(reviewer)
            batch_info.append({
                'batch': batch,
                'completed': completed,
                'total': total,
                'pct': round(completed / total * 100) if total else 0,
            })

        # Also show completed batches
        completed_batches = EvaluationBatch.objects.filter(
            assigned_reviewers=reviewer,
            status=EvaluationBatch.Status.COMPLETED,
        ).order_by('-completed_at')[:5]

        context = {
            'reviewer': reviewer,
            'batch_info': batch_info,
            'completed_batches': completed_batches,
        }
        return render(request, 'matching/evaluation/dashboard.html', context)


# =============================================================================
# BATCH OVERVIEW
# =============================================================================

class EvalBatchView(EvalAccessMixin, View):
    """Shows all items in a batch with completion status per item."""

    def get(self, request, batch_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        batch = get_object_or_404(
            EvaluationBatch, id=batch_id, assigned_reviewers=reviewer
        )
        items = batch.items.select_related(
            'client_profile', 'partner_profile'
        ).order_by('position')

        # Mark which items this reviewer has completed
        completed_item_ids = set(
            MatchEvaluation.objects.filter(
                item__batch=batch, rater=reviewer
            ).values_list('item_id', flat=True)
        )

        item_list = []
        next_item = None
        for item in items:
            is_done = item.id in completed_item_ids
            item_list.append({
                'item': item,
                'is_done': is_done,
            })
            if not is_done and next_item is None:
                next_item = item

        completed_count = len(completed_item_ids)
        total_count = items.count()

        context = {
            'reviewer': reviewer,
            'batch': batch,
            'item_list': item_list,
            'completed_count': completed_count,
            'total_count': total_count,
            'pct': round(completed_count / total_count * 100) if total_count else 0,
            'next_item': next_item,
        }
        return render(request, 'matching/evaluation/batch_overview.html', context)


# =============================================================================
# EVALUATION (two-step flow)
# =============================================================================

class EvalItemView(EvalAccessMixin, View):
    """
    Step 1: Show profiles side-by-side. No narrative, no algorithm score.
    Reviewer rates partnership potential + actionability + failure modes.
    """

    def get(self, request, batch_id, item_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        item = get_object_or_404(
            EvaluationItem,
            id=item_id, batch_id=batch_id, batch__assigned_reviewers=reviewer,
        )

        # Check if already evaluated
        existing = MatchEvaluation.objects.filter(item=item, rater=reviewer).first()
        if existing:
            # Already rated Step 1 — show Step 2 (narrative)
            return redirect(
                'matching:eval-narrative',
                batch_id=batch_id, item_id=item_id,
            )

        form = MatchQualityForm(initial={
            'started_at': timezone.now().isoformat(),
            'time_spent_seconds': 0,
        })

        # Determine if this is detailed mode (based on query param)
        detailed = request.GET.get('mode') == 'detailed'

        context = {
            'reviewer': reviewer,
            'item': item,
            'client': item.client_profile,
            'partner': item.partner_profile,
            'form': form,
            'detailed': detailed,
            'batch': item.batch,
            'current_position': item.position + 1,
            'total_items': item.batch.total_items,
        }
        return render(request, 'matching/evaluation/evaluate.html', context)

    def post(self, request, batch_id, item_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        item = get_object_or_404(
            EvaluationItem,
            id=item_id, batch_id=batch_id, batch__assigned_reviewers=reviewer,
        )

        # Prevent duplicate submissions
        if MatchEvaluation.objects.filter(item=item, rater=reviewer).exists():
            return redirect(
                'matching:eval-narrative',
                batch_id=batch_id, item_id=item_id,
            )

        form = MatchQualityForm(request.POST)
        if not form.is_valid():
            context = {
                'reviewer': reviewer,
                'item': item,
                'client': item.client_profile,
                'partner': item.partner_profile,
                'form': form,
                'detailed': request.GET.get('mode') == 'detailed',
                'batch': item.batch,
                'current_position': item.position + 1,
                'total_items': item.batch.total_items,
            }
            return render(request, 'matching/evaluation/evaluate.html', context)

        d = form.cleaned_data
        has_dimension_ratings = any([
            d.get('intent_rating'),
            d.get('synergy_rating'),
            d.get('momentum_rating'),
            d.get('context_rating'),
        ])

        # Save Step 1 data (narrative_quality set to 0 as placeholder for Step 2)
        MatchEvaluation.objects.create(
            item=item,
            rater=reviewer,
            mode=MatchEvaluation.Mode.DETAILED if has_dimension_ratings else MatchEvaluation.Mode.QUICK,
            partnership_potential=d['partnership_potential'],
            recommendation_actionability=d['recommendation_actionability'],
            narrative_quality=0,  # Placeholder — set in Step 2
            failure_wrong_audience=d.get('failure_wrong_audience', False),
            failure_one_sided_value=d.get('failure_one_sided_value', False),
            failure_stale_profile=d.get('failure_stale_profile', False),
            failure_missing_contact=d.get('failure_missing_contact', False),
            failure_scale_mismatch=d.get('failure_scale_mismatch', False),
            failure_same_niche_no_complement=d.get('failure_same_niche_no_complement', False),
            failure_data_quality=d.get('failure_data_quality', False),
            intent_rating=d.get('intent_rating'),
            synergy_rating=d.get('synergy_rating'),
            momentum_rating=d.get('momentum_rating'),
            context_rating=d.get('context_rating'),
            discovery_response=d.get('discovery_response', ''),
            notes=d.get('notes', ''),
            started_at=d.get('started_at') or timezone.now(),
            time_spent_seconds=d.get('time_spent_seconds') or 0,
        )

        return redirect(
            'matching:eval-narrative',
            batch_id=batch_id, item_id=item_id,
        )


class EvalNarrativeView(EvalAccessMixin, View):
    """
    Step 2: Show the why_fit narrative. Reviewer rates narrative quality.
    """

    def get(self, request, batch_id, item_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        item = get_object_or_404(
            EvaluationItem,
            id=item_id, batch_id=batch_id, batch__assigned_reviewers=reviewer,
        )

        evaluation = MatchEvaluation.objects.filter(item=item, rater=reviewer).first()
        if not evaluation:
            return redirect(
                'matching:eval-item',
                batch_id=batch_id, item_id=item_id,
            )

        # If narrative already rated, skip to next
        if evaluation.narrative_quality > 0:
            return self._redirect_to_next(item, reviewer)

        form = NarrativeQualityForm()

        context = {
            'reviewer': reviewer,
            'item': item,
            'evaluation': evaluation,
            'form': form,
            'batch': item.batch,
            'current_position': item.position + 1,
            'total_items': item.batch.total_items,
        }
        return render(request, 'matching/evaluation/narrative.html', context)

    def post(self, request, batch_id, item_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        item = get_object_or_404(
            EvaluationItem,
            id=item_id, batch_id=batch_id, batch__assigned_reviewers=reviewer,
        )

        evaluation = MatchEvaluation.objects.filter(item=item, rater=reviewer).first()
        if not evaluation:
            return redirect(
                'matching:eval-item',
                batch_id=batch_id, item_id=item_id,
            )

        form = NarrativeQualityForm(request.POST)
        if not form.is_valid():
            context = {
                'reviewer': reviewer,
                'item': item,
                'evaluation': evaluation,
                'form': form,
                'batch': item.batch,
                'current_position': item.position + 1,
                'total_items': item.batch.total_items,
            }
            return render(request, 'matching/evaluation/narrative.html', context)

        d = form.cleaned_data
        evaluation.narrative_quality = d['narrative_quality']
        evaluation.narrative_notes = d.get('narrative_notes', '')
        evaluation.save(update_fields=['narrative_quality', 'narrative_notes'])

        # Update reviewer stats
        reviewer.total_evaluations += 1
        reviewer.last_evaluation_at = timezone.now()
        reviewer.save(update_fields=['total_evaluations', 'last_evaluation_at'])

        return self._redirect_to_next(item, reviewer)

    def _redirect_to_next(self, current_item, reviewer):
        """Redirect to next unevaluated item or batch complete page."""
        batch = current_item.batch
        completed_ids = set(
            MatchEvaluation.objects.filter(
                item__batch=batch, rater=reviewer, narrative_quality__gt=0,
            ).values_list('item_id', flat=True)
        )

        next_item = batch.items.exclude(
            id__in=completed_ids
        ).order_by('position').first()

        if next_item:
            return redirect(
                'matching:eval-item',
                batch_id=batch.id, item_id=next_item.id,
            )
        return redirect('matching:eval-batch-complete', batch_id=batch.id)


# =============================================================================
# BATCH COMPLETE
# =============================================================================

class EvalBatchCompleteView(EvalAccessMixin, View):
    """Completion summary for a batch."""

    def get(self, request, batch_id):
        reviewer, error = self.get_reviewer_or_redirect(request)
        if error:
            return error

        batch = get_object_or_404(
            EvaluationBatch, id=batch_id, assigned_reviewers=reviewer
        )

        evaluations = MatchEvaluation.objects.filter(
            item__batch=batch, rater=reviewer, narrative_quality__gt=0,
        )

        # Basic stats
        total = evaluations.count()
        avg_potential = None
        avg_narrative = None
        if total > 0:
            from django.db.models import Avg
            aggs = evaluations.aggregate(
                avg_potential=Avg('partnership_potential'),
                avg_narrative=Avg('narrative_quality'),
                avg_actionability=Avg('recommendation_actionability'),
            )
            avg_potential = round(aggs['avg_potential'], 1) if aggs['avg_potential'] else None
            avg_narrative = round(aggs['avg_narrative'], 1) if aggs['avg_narrative'] else None
            avg_actionability = round(aggs['avg_actionability'], 1) if aggs['avg_actionability'] else None
        else:
            avg_actionability = None

        # Failure mode summary
        failure_counts = {}
        for label, field in [
            ('Wrong audience', 'failure_wrong_audience'),
            ('One-sided value', 'failure_one_sided_value'),
            ('Stale profile', 'failure_stale_profile'),
            ('Missing contact', 'failure_missing_contact'),
            ('Scale mismatch', 'failure_scale_mismatch'),
            ('Same niche', 'failure_same_niche_no_complement'),
            ('Data quality', 'failure_data_quality'),
        ]:
            count = evaluations.filter(**{field: True}).count()
            if count > 0:
                failure_counts[label] = count

        context = {
            'reviewer': reviewer,
            'batch': batch,
            'total': total,
            'avg_potential': avg_potential,
            'avg_narrative': avg_narrative,
            'avg_actionability': avg_actionability,
            'failure_counts': failure_counts,
        }
        return render(request, 'matching/evaluation/batch_complete.html', context)
