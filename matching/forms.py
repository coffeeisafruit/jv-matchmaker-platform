"""
Forms for the JV Matcher module.
"""

from django import forms
from django.core.validators import FileExtensionValidator

from .models import Match, Profile


class ProfileForm(forms.ModelForm):
    """Form for creating and editing profiles."""

    class Meta:
        model = Profile
        fields = [
            'name',
            'company',
            'linkedin_url',
            'website_url',
            'email',
            'industry',
            'audience_size',
            'audience_description',
            'content_style',
        ]
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'Full name',
            }),
            'company': forms.TextInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'Company name',
            }),
            'linkedin_url': forms.URLInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'https://linkedin.com/in/username',
            }),
            'website_url': forms.URLInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'https://example.com',
            }),
            'email': forms.EmailInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'email@example.com',
            }),
            'industry': forms.TextInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'placeholder': 'e.g., Technology, Marketing, Finance',
            }),
            'audience_size': forms.Select(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
            }),
            'audience_description': forms.Textarea(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'rows': 3,
                'placeholder': 'Describe the target audience...',
            }),
            'content_style': forms.Textarea(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'rows': 3,
                'placeholder': 'Describe their content style, tone, and topics...',
            }),
        }


class ProfileImportForm(forms.Form):
    """Form for bulk importing profiles from CSV."""

    csv_file = forms.FileField(
        validators=[FileExtensionValidator(allowed_extensions=['csv'])],
        widget=forms.FileInput(attrs={
            'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
            'accept': '.csv',
        }),
        help_text='Upload a CSV file with columns: name, company, linkedin_url, website_url, email, industry, audience_size, audience_description, content_style'
    )


class MatchStatusForm(forms.ModelForm):
    """Form for updating match status and notes."""

    class Meta:
        model = Match
        fields = ['status', 'notes']
        widgets = {
            'status': forms.Select(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'hx-post': '',  # Will be set in template
                'hx-trigger': 'change',
                'hx-swap': 'outerHTML',
            }),
            'notes': forms.Textarea(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
                'rows': 4,
                'placeholder': 'Add notes about this match...',
            }),
        }


class MatchFilterForm(forms.Form):
    """Form for filtering matches."""

    status = forms.ChoiceField(
        choices=[('', 'All Statuses')] + list(Match.Status.choices),
        required=False,
        widget=forms.Select(attrs={
            'class': 'px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
            'hx-get': '',
            'hx-trigger': 'change',
            'hx-target': '#match-list',
            'hx-swap': 'innerHTML',
        })
    )

    min_score = forms.ChoiceField(
        choices=[
            ('', 'All Scores'),
            ('8', '8+ (High)'),
            ('6', '6+ (Medium)'),
            ('4', '4+ (Low)'),
        ],
        required=False,
        widget=forms.Select(attrs={
            'class': 'px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
            'hx-get': '',
            'hx-trigger': 'change',
            'hx-target': '#match-list',
            'hx-swap': 'innerHTML',
        })
    )
