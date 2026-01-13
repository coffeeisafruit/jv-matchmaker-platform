from django.db import models
from django.utils import timezone
from cryptography.fernet import Fernet
from django.conf import settings
import json


class EmailConnection(models.Model):
    """
    Stores OAuth tokens for connected email accounts (Gmail, Outlook).
    Allows users to send emails directly from the platform.
    """
    PROVIDER_CHOICES = [
        ('gmail', 'Gmail'),
        ('outlook', 'Outlook'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='email_connections'
    )
    provider = models.CharField(max_length=20, choices=PROVIDER_CHOICES)
    email_address = models.EmailField()

    # Encrypted OAuth tokens
    _access_token = models.TextField(db_column='access_token')
    _refresh_token = models.TextField(db_column='refresh_token')

    token_expires_at = models.DateTimeField()
    scopes = models.JSONField(default=list)

    is_active = models.BooleanField(default=True)
    is_primary = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = 'Email Connection'
        verbose_name_plural = 'Email Connections'
        unique_together = ['user', 'email_address']
        ordering = ['-is_primary', '-created_at']

    def __str__(self):
        return f"{self.email_address} ({self.get_provider_display()})"

    def _get_cipher(self):
        """Get Fernet cipher for token encryption."""
        key = settings.SECRET_KEY[:32].encode()
        key = key.ljust(32, b'=')
        import base64
        return Fernet(base64.urlsafe_b64encode(key))

    @property
    def access_token(self):
        """Decrypt and return access token."""
        if not self._access_token:
            return None
        try:
            cipher = self._get_cipher()
            return cipher.decrypt(self._access_token.encode()).decode()
        except Exception:
            return self._access_token

    @access_token.setter
    def access_token(self, value):
        """Encrypt and store access token."""
        if value:
            cipher = self._get_cipher()
            self._access_token = cipher.encrypt(value.encode()).decode()
        else:
            self._access_token = ''

    @property
    def refresh_token(self):
        """Decrypt and return refresh token."""
        if not self._refresh_token:
            return None
        try:
            cipher = self._get_cipher()
            return cipher.decrypt(self._refresh_token.encode()).decode()
        except Exception:
            return self._refresh_token

    @refresh_token.setter
    def refresh_token(self, value):
        """Encrypt and store refresh token."""
        if value:
            cipher = self._get_cipher()
            self._refresh_token = cipher.encrypt(value.encode()).decode()
        else:
            self._refresh_token = ''

    def is_token_expired(self):
        """Check if access token is expired."""
        return timezone.now() >= self.token_expires_at

    def mark_as_primary(self):
        """Set this connection as the primary email for the user."""
        EmailConnection.objects.filter(user=self.user, is_primary=True).update(is_primary=False)
        self.is_primary = True
        self.save(update_fields=['is_primary'])


class SentEmail(models.Model):
    """
    Tracks emails sent through the platform.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('sent', 'Sent'),
        ('failed', 'Failed'),
        ('bounced', 'Bounced'),
    ]

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='sent_emails'
    )
    email_connection = models.ForeignKey(
        EmailConnection,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='sent_emails'
    )
    pvp = models.ForeignKey(
        'outreach.PVP',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='sent_emails'
    )
    outreach_email = models.ForeignKey(
        'outreach.OutreachEmail',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='sent_records'
    )

    # Recipient info
    recipient_email = models.EmailField()
    recipient_name = models.CharField(max_length=255, blank=True)
    recipient_profile_id = models.UUIDField(null=True, blank=True)

    # Email content
    subject = models.CharField(max_length=500)
    body = models.TextField()

    # Provider message ID for tracking
    provider_message_id = models.CharField(max_length=255, blank=True)
    thread_id = models.CharField(max_length=255, blank=True)

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True)

    # Tracking
    sent_at = models.DateTimeField(null=True, blank=True)
    opened_at = models.DateTimeField(null=True, blank=True)
    clicked_at = models.DateTimeField(null=True, blank=True)
    replied_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Sent Email'
        verbose_name_plural = 'Sent Emails'
        ordering = ['-created_at']

    def __str__(self):
        return f"To: {self.recipient_email} - {self.subject[:50]}"


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
    supabase_profile_id = models.UUIDField(
        null=True,
        blank=True,
        help_text='UUID of the SupabaseProfile this PVP was generated for'
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


class OutreachSequence(models.Model):
    """A 4-email outreach sequence for a match."""

    class SequenceType(models.TextChoices):
        LOOKALIKE = 'lookalike', 'Lookalike Campaign'
        TRIGGER = 'trigger', 'Trigger-Based'
        CREATIVE_IDEAS = 'creative_ideas', 'Creative Ideas'
        POKE_BEAR = 'poke_bear', 'Poke the Bear'
        SUPER_SHORT = 'super_short', 'Super Short'

    class Status(models.TextChoices):
        DRAFT = 'draft', 'Draft'
        READY = 'ready', 'Ready to Send'
        IN_PROGRESS = 'in_progress', 'In Progress'
        COMPLETED = 'completed', 'Completed'
        PAUSED = 'paused', 'Paused'

    user = models.ForeignKey('core.User', on_delete=models.CASCADE, related_name='outreach_sequences')
    match = models.ForeignKey('matching.SupabaseMatch', on_delete=models.SET_NULL, null=True, blank=True)
    target_profile = models.ForeignKey('matching.SupabaseProfile', on_delete=models.SET_NULL, null=True, blank=True)

    name = models.CharField(max_length=200)
    sequence_type = models.CharField(max_length=20, choices=SequenceType.choices, default=SequenceType.LOOKALIKE)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.DRAFT)

    # Personalization data
    trigger_data = models.JSONField(default=dict, blank=True)
    ai_personalization = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Outreach Sequence'
        verbose_name_plural = 'Outreach Sequences'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name} ({self.get_status_display()})"


class OutreachEmail(models.Model):
    """Individual email in a sequence."""

    sequence = models.ForeignKey(OutreachSequence, on_delete=models.CASCADE, related_name='emails')

    email_number = models.IntegerField()  # 1-4
    is_threaded = models.BooleanField(default=False)

    subject_line = models.CharField(max_length=255)
    body = models.TextField()

    # Tracking
    scheduled_for = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    opened_at = models.DateTimeField(null=True, blank=True)
    replied_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['email_number']
        unique_together = ['sequence', 'email_number']
        verbose_name = 'Outreach Email'
        verbose_name_plural = 'Outreach Emails'

    def __str__(self):
        return f"Email {self.email_number} - {self.sequence.name}"
