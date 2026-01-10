from django.db import models


class ICP(models.Model):
    """Ideal Customer Profile - defines the target customer characteristics."""

    COMPANY_SIZE_CHOICES = [
        ('solo', 'Solo/Freelancer'),
        ('small', 'Small (2-10 employees)'),
        ('medium', 'Medium (11-100 employees)'),
        ('enterprise', 'Enterprise (100+ employees)'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='icps'
    )
    name = models.CharField(max_length=255)
    industry = models.CharField(max_length=100)
    company_size = models.CharField(max_length=20, choices=COMPANY_SIZE_CHOICES)
    pain_points = models.JSONField(default=list, help_text='List of pain points')
    goals = models.JSONField(default=list, help_text='List of goals')
    budget_range = models.CharField(max_length=100, null=True, blank=True)
    decision_makers = models.JSONField(
        null=True,
        blank=True,
        help_text='Information about decision makers'
    )
    is_primary = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'ICP'
        verbose_name_plural = 'ICPs'
        ordering = ['-is_primary', '-created_at']

    def __str__(self):
        return f"{self.name} ({self.industry})"


class TransformationAnalysis(models.Model):
    """Analyzes the transformation journey from before to after state."""

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='transformation_analyses'
    )
    icp = models.ForeignKey(
        ICP,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='transformation_analyses'
    )
    before_state = models.TextField(help_text='Where the customer starts')
    after_state = models.TextField(help_text='Where the customer ends up')
    transformation_summary = models.TextField()
    key_obstacles = models.JSONField(default=list)
    value_drivers = models.JSONField(default=list)
    ai_generated = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Transformation Analysis'
        verbose_name_plural = 'Transformation Analyses'
        ordering = ['-created_at']

    def __str__(self):
        summary_preview = self.transformation_summary[:50] if self.transformation_summary else ''
        return f"Transformation: {summary_preview}..."


class PainSignal(models.Model):
    """Signals that indicate potential customer pain points to detect."""

    SIGNAL_TYPE_CHOICES = [
        ('hiring', 'Hiring'),
        ('tech_change', 'Technology Change'),
        ('funding', 'Funding'),
        ('expansion', 'Expansion'),
        ('complaint', 'Complaint'),
        ('question', 'Question'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='pain_signals'
    )
    signal_type = models.CharField(max_length=20, choices=SIGNAL_TYPE_CHOICES)
    description = models.TextField()
    weight = models.FloatField(default=1.0)
    keywords = models.JSONField(default=list, help_text='List of keywords to detect')
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = 'Pain Signal'
        verbose_name_plural = 'Pain Signals'
        ordering = ['-weight', 'signal_type']

    def __str__(self):
        return f"{self.get_signal_type_display()}: {self.description[:50]}"
