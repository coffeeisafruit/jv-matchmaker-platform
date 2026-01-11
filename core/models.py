from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils import timezone
from datetime import timedelta


class User(AbstractUser):
    """Custom User model for JV Matchmaker Platform."""

    class Tier(models.TextChoices):
        FREE = 'free', 'Free'
        STARTER = 'starter', 'Starter'
        GROWTH = 'growth', 'Growth'
        PRO = 'pro', 'Pro'

    business_name = models.CharField(max_length=255)
    business_domain = models.URLField(blank=True, null=True)
    business_description = models.TextField(blank=True, null=True)
    tier = models.CharField(
        max_length=10,
        choices=Tier.choices,
        default=Tier.FREE,
    )
    matches_this_month = models.IntegerField(default=0)
    pvps_this_month = models.IntegerField(default=0)
    onboarding_completed = models.BooleanField(default=False)
    icp_last_reviewed = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Last time the user reviewed/confirmed their ICP'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'core_user'
        verbose_name = 'User'
        verbose_name_plural = 'Users'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.username} ({self.business_name})"

    def is_icp_review_due(self, days=30):
        """Check if the user's ICP review is due (30 days by default)."""
        if not self.icp_last_reviewed:
            # If never reviewed, check if they have an ICP and it's been 30 days since creation
            from positioning.models import ICP
            primary_icp = ICP.objects.filter(user=self, is_primary=True).first()
            if primary_icp:
                return timezone.now() > primary_icp.created_at + timedelta(days=days)
            return False
        return timezone.now() > self.icp_last_reviewed + timedelta(days=days)

    def mark_icp_reviewed(self):
        """Mark the user's ICP as reviewed."""
        self.icp_last_reviewed = timezone.now()
        self.save(update_fields=['icp_last_reviewed'])


class APIKey(models.Model):
    """API Key model for BYOK (Bring Your Own Key) functionality."""

    class Provider(models.TextChoices):
        ANTHROPIC = 'anthropic', 'Anthropic'
        OPENAI = 'openai', 'OpenAI'

    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='api_keys',
    )
    provider = models.CharField(
        max_length=20,
        choices=Provider.choices,
    )
    encrypted_key = models.TextField()
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'core_api_key'
        verbose_name = 'API Key'
        verbose_name_plural = 'API Keys'
        ordering = ['-created_at']
        unique_together = ['user', 'provider']

    def __str__(self):
        return f"{self.user.username} - {self.get_provider_display()}"
