from django.db import models


class ICP(models.Model):
    """Ideal Customer Profile - defines the target customer characteristics."""

    CUSTOMER_TYPE_CHOICES = [
        ('b2b', 'Business (B2B)'),
        ('b2c', 'Consumer (B2C)'),
    ]

    COMPANY_SIZE_CHOICES = [
        ('solo', 'Solo/Freelancer'),
        ('small', 'Small (2-10 employees)'),
        ('medium', 'Medium (11-100 employees)'),
        ('enterprise', 'Enterprise (100+ employees)'),
    ]

    AGE_RANGE_CHOICES = [
        ('18-24', '18-24 years'),
        ('25-34', '25-34 years'),
        ('35-44', '35-44 years'),
        ('45-54', '45-54 years'),
        ('55-64', '55-64 years'),
        ('65+', '65+ years'),
    ]

    INCOME_LEVEL_CHOICES = [
        ('low', 'Under $30,000'),
        ('lower_middle', '$30,000 - $50,000'),
        ('middle', '$50,000 - $75,000'),
        ('upper_middle', '$75,000 - $100,000'),
        ('high', '$100,000 - $150,000'),
        ('affluent', '$150,000+'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='icps'
    )
    name = models.CharField(max_length=255)
    customer_type = models.CharField(
        max_length=10,
        choices=CUSTOMER_TYPE_CHOICES,
        default='b2b'
    )
    industry = models.CharField(max_length=100)  # Also used as "niche/market" for B2C

    # B2B fields
    company_size = models.CharField(
        max_length=20,
        choices=COMPANY_SIZE_CHOICES,
        null=True,
        blank=True
    )

    # B2C fields
    age_range = models.CharField(
        max_length=20,
        choices=AGE_RANGE_CHOICES,
        null=True,
        blank=True
    )
    income_level = models.CharField(
        max_length=20,
        choices=INCOME_LEVEL_CHOICES,
        null=True,
        blank=True
    )
    demographics = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='e.g., "Parents with young children", "Remote workers"'
    )

    # Common fields
    pain_points = models.JSONField(default=list, help_text='List of pain points')
    goals = models.JSONField(default=list, help_text='List of goals')
    budget_range = models.CharField(max_length=100, null=True, blank=True)
    decision_makers = models.JSONField(
        null=True,
        blank=True,
        help_text='Information about decision makers (B2B) or purchase influencers (B2C)'
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


class LeadMagnetConcept(models.Model):
    """AI-generated lead magnet concept based on transformation analysis."""

    class Format(models.TextChoices):
        PDF = 'pdf', 'PDF Guide'
        VIDEO = 'video', 'Short Video'
        CHECKLIST = 'checklist', 'Checklist'
        TEMPLATE = 'template', 'Template/Swipe File'
        QUIZ = 'quiz', 'Quiz/Assessment'
        CALCULATOR = 'calculator', 'Calculator/Tool'

    user = models.ForeignKey('core.User', on_delete=models.CASCADE, related_name='lead_magnets')
    transformation = models.ForeignKey(
        'TransformationAnalysis',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='lead_magnets'
    )

    title = models.CharField(max_length=200)
    what_description = models.TextField(help_text='What the audience will learn/get')
    why_description = models.TextField(help_text='Why they need this before buying')
    format_suggestion = models.CharField(max_length=20, choices=Format.choices)

    # AI generation metadata
    target_problem = models.TextField(help_text='The one problem this solves')
    hook = models.CharField(max_length=300, blank=True)
    estimated_creation_time = models.CharField(max_length=50, default='3-5 hours')

    is_selected = models.BooleanField(default=False, help_text='User selected this concept to build')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Lead Magnet Concept'
        verbose_name_plural = 'Lead Magnet Concepts'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.title} ({self.get_format_suggestion_display()})"
