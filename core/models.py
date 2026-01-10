from django.db import models
from django.contrib.auth.models import AbstractUser


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
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'core_user'
        verbose_name = 'User'
        verbose_name_plural = 'Users'
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.username} ({self.business_name})"


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
