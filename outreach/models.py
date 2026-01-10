from django.db import models


class PVP(models.Model):
    """
    Permissionless Value Proposition - a personalized outreach message
    that provides value upfront without asking for anything in return.
    """
    PATTERN_TYPE_CHOICES = [
        ('pain_solution', 'Pain Solution'),
        ('insight_share', 'Insight Share'),
        ('mutual_benefit', 'Mutual Benefit'),
        ('social_proof', 'Social Proof'),
        ('curiosity_hook', 'Curiosity Hook'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='pvps'
    )
    match = models.ForeignKey(
        'matching.Match',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='pvps'
    )
    pattern_type = models.CharField(
        max_length=20,
        choices=PATTERN_TYPE_CHOICES
    )
    pain_point_addressed = models.TextField()
    value_offered = models.TextField()
    call_to_action = models.TextField()
    full_message = models.TextField()
    personalization_data = models.JSONField(null=True, blank=True)
    ai_model_used = models.CharField(max_length=50)
    quality_score = models.FloatField(
        null=True,
        blank=True,
        help_text='Quality score based on 7-criterion rubric (0-100)'
    )
    is_template = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'PVP'
        verbose_name_plural = 'PVPs'
        ordering = ['-created_at']

    def __str__(self):
        return f"PVP ({self.get_pattern_type_display()}) - {self.user}"


class OutreachTemplate(models.Model):
    """
    GEX-style email template for outreach sequences.
    Supports a 4-email sequence with customizable placeholders.
    """
    CATEGORY_CHOICES = [
        ('cold_outreach', 'Cold Outreach'),
        ('jv_pitch', 'JV Pitch'),
        ('follow_up', 'Follow Up'),
        ('breakup', 'Breakup'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='outreach_templates'
    )
    name = models.CharField(max_length=255)
    sequence_position = models.IntegerField(
        help_text='Position in the GEX 4-email sequence (1-4)'
    )
    subject_line = models.CharField(max_length=255)
    body_template = models.TextField(
        help_text='Email body with {{placeholders}} for personalization'
    )
    variables = models.JSONField(
        default=list,
        help_text='List of placeholder variable names used in the template'
    )
    category = models.CharField(
        max_length=20,
        choices=CATEGORY_CHOICES
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Outreach Template'
        verbose_name_plural = 'Outreach Templates'
        ordering = ['sequence_position', '-created_at']

    def __str__(self):
        return f"{self.name} (Position {self.sequence_position})"


class OutreachCampaign(models.Model):
    """
    A campaign for reaching out to multiple target profiles
    using a specific template sequence.
    """
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('completed', 'Completed'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='outreach_campaigns'
    )
    name = models.CharField(max_length=255)
    template = models.ForeignKey(
        OutreachTemplate,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='campaigns'
    )
    target_profiles = models.ManyToManyField(
        'matching.Profile',
        related_name='outreach_campaigns',
        blank=True
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='draft'
    )
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Outreach Campaign'
        verbose_name_plural = 'Outreach Campaigns'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name} ({self.get_status_display()})"


class OutreachActivity(models.Model):
    """
    Tracks individual outreach actions and engagement metrics
    for a campaign.
    """
    ACTION_TYPE_CHOICES = [
        ('email_sent', 'Email Sent'),
        ('email_opened', 'Email Opened'),
        ('link_clicked', 'Link Clicked'),
        ('replied', 'Replied'),
        ('meeting_booked', 'Meeting Booked'),
    ]

    campaign = models.ForeignKey(
        OutreachCampaign,
        on_delete=models.CASCADE,
        related_name='activities'
    )
    profile = models.ForeignKey(
        'matching.Profile',
        on_delete=models.CASCADE,
        related_name='outreach_activities'
    )
    pvp = models.ForeignKey(
        PVP,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='activities'
    )
    action_type = models.CharField(
        max_length=20,
        choices=ACTION_TYPE_CHOICES
    )
    details = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Outreach Activity'
        verbose_name_plural = 'Outreach Activities'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.get_action_type_display()} - {self.profile} ({self.campaign})"
