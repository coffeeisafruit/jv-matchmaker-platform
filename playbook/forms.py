"""
Forms for the Playbook app.
"""

from django import forms
from .models import GeneratedPlaybook


class PlaybookCreateForm(forms.ModelForm):
    """Form for creating a new playbook."""

    class Meta:
        model = GeneratedPlaybook
        fields = ['name', 'size', 'launch_date', 'transformation']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent',
                'placeholder': 'e.g., Q1 2024 Product Launch'
            }),
            'size': forms.Select(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent'
            }),
            'launch_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent'
            }),
            'transformation': forms.Select(attrs={
                'class': 'w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent'
            }),
        }
        help_texts = {
            'name': 'Give your playbook a descriptive name',
            'size': 'Choose the number of plays based on your launch timeline',
            'launch_date': 'When do you plan to launch?',
            'transformation': 'Link to a transformation analysis for AI customization',
        }

    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)

        # Filter transformation choices to only the user's transformations
        if self.user:
            self.fields['transformation'].queryset = self.user.transformation_analyses.all()

        # Make transformation optional
        self.fields['transformation'].required = False
