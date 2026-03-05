"""
Forms for the Match Evaluation system.
Two-step flow: Step 1 (match quality) → Step 2 (narrative quality).
"""

from django import forms
from .models import MatchEvaluation


class MatchQualityForm(forms.Form):
    """
    Step 1: Rate match quality from raw profiles (no narrative, no algorithm score).
    """
    partnership_potential = forms.IntegerField(
        min_value=1, max_value=7,
        widget=forms.RadioSelect(choices=[
            (1, '1 — Terrible'),
            (2, '2 — Poor'),
            (3, '3 — Below average'),
            (4, '4 — Acceptable'),
            (5, '5 — Good'),
            (6, '6 — Strong'),
            (7, '7 — Excellent'),
        ]),
        help_text='Ignoring contact info and data quality — how strong is this partnership?',
    )
    recommendation_actionability = forms.IntegerField(
        min_value=1, max_value=4,
        widget=forms.RadioSelect(choices=[
            (1, '1 — Not actionable'),
            (2, '2 — Needs work'),
            (3, '3 — Actionable'),
            (4, '4 — Ready to go'),
        ]),
        help_text='How actionable is this as a recommendation to the client?',
    )

    # Failure mode checkboxes
    failure_wrong_audience = forms.BooleanField(required=False, label='Wrong audience')
    failure_one_sided_value = forms.BooleanField(required=False, label='One-sided value')
    failure_stale_profile = forms.BooleanField(required=False, label='Stale profile')
    failure_missing_contact = forms.BooleanField(required=False, label='Missing contact info')
    failure_scale_mismatch = forms.BooleanField(required=False, label='Scale mismatch')
    failure_same_niche_no_complement = forms.BooleanField(required=False, label='Same niche, no complement')
    failure_data_quality = forms.BooleanField(required=False, label='Data quality issues')
    failure_timing_readiness = forms.BooleanField(required=False, label='Timing / launch readiness mismatch')

    # Open discovery question
    discovery_response = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'placeholder': 'What\'s the most important thing about this match the algorithm probably doesn\'t capture?',
        }),
    )

    notes = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'placeholder': 'Any other observations...',
        }),
    )

    # Detailed mode: ISMC dimension ratings (optional)
    intent_rating = forms.IntegerField(
        min_value=1, max_value=4, required=False,
        widget=forms.RadioSelect(choices=[
            (1, '1 — Wrong/Missing'),
            (2, '2 — Weak'),
            (3, '3 — Reasonable'),
            (4, '4 — Strong'),
        ]),
    )
    synergy_rating = forms.IntegerField(
        min_value=1, max_value=4, required=False,
        widget=forms.RadioSelect(choices=[(1, '1'), (2, '2'), (3, '3'), (4, '4')]),
    )
    momentum_rating = forms.IntegerField(
        min_value=1, max_value=4, required=False,
        widget=forms.RadioSelect(choices=[(1, '1'), (2, '2'), (3, '3'), (4, '4')]),
    )
    context_rating = forms.IntegerField(
        min_value=1, max_value=4, required=False,
        widget=forms.RadioSelect(choices=[(1, '1'), (2, '2'), (3, '3'), (4, '4')]),
    )

    # Hidden fields for timing
    started_at = forms.DateTimeField(widget=forms.HiddenInput(), required=False)
    time_spent_seconds = forms.IntegerField(widget=forms.HiddenInput(), initial=0, required=False)


class NarrativeQualityForm(forms.Form):
    """
    Step 2: Rate the why_fit narrative (shown after Step 1 submission).
    """
    narrative_quality = forms.IntegerField(
        min_value=1, max_value=7,
        widget=forms.RadioSelect(choices=[
            (1, '1 — Misleading'),
            (2, '2 — Very weak'),
            (3, '3 — Below average'),
            (4, '4 — Acceptable'),
            (5, '5 — Good'),
            (6, '6 — Strong'),
            (7, '7 — Excellent'),
        ]),
        help_text='How well does this explanation capture the value of this partnership?',
    )
    narrative_notes = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'placeholder': 'What did the narrative get wrong or miss?',
        }),
    )
