"""
Forms for the JV Matcher module.
"""

from django import forms
from django.core.validators import FileExtensionValidator

from .models import Match, Profile, SupabaseProfile


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


# ---------------------------------------------------------------------------
# Client Profile Edit (report system — uses report design CSS, not Tailwind)
# ---------------------------------------------------------------------------

_INPUT_STYLE = (
    'width: 100%; padding: 0.625rem 0.875rem; border: 1px solid #d4d0cb; '
    'border-radius: 6px; font-family: DM Sans, sans-serif; font-size: 0.875rem; '
    'background: #fff; color: #1a1a1a;'
)

_TEXTAREA_STYLE = _INPUT_STYLE


class ClientProfileForm(forms.ModelForm):
    """Client-facing profile edit form for SupabaseProfile."""

    class Meta:
        model = SupabaseProfile
        fields = [
            'name', 'company', 'email', 'phone', 'website', 'linkedin',
            'what_you_do', 'who_you_serve', 'seeking', 'offering', 'niche',
            'bio', 'signature_programs', 'booking_link',
            'list_size',
        ]
        labels = {
            'name': 'Full Name',
            'company': 'Company',
            'email': 'Email',
            'phone': 'Phone',
            'website': 'Website',
            'linkedin': 'LinkedIn URL',
            'what_you_do': 'What You Do',
            'who_you_serve': 'Who You Serve',
            'seeking': 'What You Are Seeking',
            'offering': 'What You Are Offering',
            'niche': 'Your Niche',
            'bio': 'About You',

            'signature_programs': 'Signature Programs',
            'booking_link': 'Booking / Calendar Link',
            'list_size': 'Email List Size',
        }
        widgets = {
            'name': forms.TextInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'Your full name'}),
            'company': forms.TextInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'Company name'}),
            'email': forms.EmailInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'you@company.com'}),
            'phone': forms.TextInput(attrs={'style': _INPUT_STYLE, 'placeholder': '+1 (555) 123-4567'}),
            'website': forms.URLInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'https://yoursite.com'}),
            'linkedin': forms.URLInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'https://linkedin.com/in/you'}),
            'what_you_do': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 3, 'placeholder': 'Describe your business or service...'}),
            'who_you_serve': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 3, 'placeholder': 'Who is your ideal client or audience?'}),
            'seeking': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 3, 'placeholder': 'What kind of JV partnerships are you looking for?'}),
            'offering': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 3, 'placeholder': 'What can you offer potential partners?'}),
            'niche': forms.TextInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'e.g., Health & Wellness, Business Coaching'}),
            'bio': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 4, 'placeholder': 'Tell your story...'}),

            'signature_programs': forms.Textarea(attrs={'style': _TEXTAREA_STYLE, 'rows': 2, 'placeholder': 'Named courses, books, or frameworks...'}),
            'booking_link': forms.URLInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'https://calendly.com/you'}),
            'list_size': forms.NumberInput(attrs={'style': _INPUT_STYLE, 'placeholder': 'e.g., 5000', 'min': '0'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # All fields optional for client edits
        for field in self.fields.values():
            field.required = False
