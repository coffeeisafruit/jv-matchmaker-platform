from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.contrib.postgres.fields import ArrayField
import uuid


# =============================================================================
# SUPABASE MODELS (Read-only, maps to existing Supabase tables)
# =============================================================================

class SupabaseProfile(models.Model):
    """
    Maps to the existing 'profiles' table in Supabase.
    Contains 3,143+ JV partner profiles.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    auth_user_id = models.UUIDField(null=True, blank=True)
    name = models.TextField()
    email = models.TextField(null=True, blank=True)
    phone = models.TextField(null=True, blank=True)
    company = models.TextField(null=True, blank=True)
    website = models.TextField(null=True, blank=True)
    linkedin = models.TextField(null=True, blank=True)
    avatar_url = models.TextField(null=True, blank=True)
    business_focus = models.TextField(null=True, blank=True)
    status = models.TextField(default='Member')  # Member, Non Member Resource, Pending
    service_provided = models.TextField(null=True, blank=True)
    list_size = models.IntegerField(default=0)
    business_size = models.TextField(null=True, blank=True)
    social_reach = models.IntegerField(default=0)
    role = models.TextField(default='member')  # admin, member, viewer
    bio = models.TextField(null=True, blank=True)
    tags = ArrayField(models.TextField(), null=True, blank=True)
    notes = models.TextField(null=True, blank=True)
    what_you_do = models.TextField(null=True, blank=True)
    who_you_serve = models.TextField(null=True, blank=True)
    seeking = models.TextField(null=True, blank=True)
    offering = models.TextField(null=True, blank=True)
    current_projects = models.TextField(null=True, blank=True)
    niche = models.TextField(null=True, blank=True)
    audience_type = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    profile_updated_at = models.DateTimeField(null=True, blank=True)
    last_active_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False  # Django won't create/modify this table
        db_table = 'profiles'
        ordering = ['-last_active_at']
        verbose_name = 'Supabase Profile'
        verbose_name_plural = 'Supabase Profiles'

    def __str__(self):
        if self.company:
            return f"{self.name} ({self.company})"
        return self.name

    @property
    def audience_size_display(self):
        """Convert list_size to a display category."""
        if self.list_size >= 1000000:
            return 'Massive (1M+)'
        elif self.list_size >= 100000:
            return 'Large (100K - 1M)'
        elif self.list_size >= 10000:
            return 'Medium (10K - 100K)'
        elif self.list_size >= 1000:
            return 'Small (1K - 10K)'
        return 'Tiny (< 1K)'


class SupabaseMatch(models.Model):
    """
    Maps to the existing 'match_suggestions' table in Supabase.
    Contains pre-computed match suggestions.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    profile_id = models.UUIDField()
    suggested_profile_id = models.UUIDField()
    match_score = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    match_reason = models.TextField(null=True, blank=True)
    source = models.TextField(null=True, blank=True)
    status = models.TextField(default='pending')  # pending, viewed, contacted, connected, dismissed
    suggested_at = models.DateTimeField(auto_now_add=True)
    viewed_at = models.DateTimeField(null=True, blank=True)
    contacted_at = models.DateTimeField(null=True, blank=True)
    notes = models.TextField(null=True, blank=True)
    rich_analysis = models.TextField(null=True, blank=True)
    analysis_generated_at = models.DateTimeField(null=True, blank=True)
    email_sent_at = models.DateTimeField(null=True, blank=True)
    user_feedback = models.CharField(max_length=20, null=True, blank=True)
    feedback_at = models.DateTimeField(null=True, blank=True)
    match_context = models.JSONField(null=True, blank=True)
    score_ab = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    score_ba = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    harmonic_mean = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    scale_symmetry_score = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    trust_level = models.TextField(default='legacy')  # platinum, gold, bronze, legacy
    expires_at = models.DateTimeField(null=True, blank=True)
    draft_intro_clicked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False  # Django won't create/modify this table
        db_table = 'match_suggestions'
        ordering = ['-harmonic_mean']
        verbose_name = 'Supabase Match'
        verbose_name_plural = 'Supabase Matches'

    def __str__(self):
        return f"Match {self.profile_id} <-> {self.suggested_profile_id} ({self.harmonic_mean})"

    def get_profile(self):
        """Get the source profile."""
        return SupabaseProfile.objects.filter(id=self.profile_id).first()

    def get_suggested_profile(self):
        """Get the suggested match profile."""
        return SupabaseProfile.objects.filter(id=self.suggested_profile_id).first()


# =============================================================================
# DJANGO MODELS (Managed by Django, for app-specific data)
# =============================================================================

class Profile(models.Model):
    """
    Represents a JV partner or prospect profile with enrichment data.
    """

    class AudienceSize(models.TextChoices):
        TINY = 'tiny', 'Tiny (< 1K)'
        SMALL = 'small', 'Small (1K - 10K)'
        MEDIUM = 'medium', 'Medium (10K - 100K)'
        LARGE = 'large', 'Large (100K - 1M)'
        MASSIVE = 'massive', 'Massive (1M+)'

    class Source(models.TextChoices):
        MANUAL = 'manual', 'Manual Entry'
        CLAY = 'clay', 'Clay Import'
        LINKEDIN = 'linkedin', 'LinkedIn'
        IMPORT = 'import', 'Bulk Import'

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='profiles'
    )
    name = models.CharField(max_length=255)
    company = models.CharField(max_length=255, null=True, blank=True)
    linkedin_url = models.URLField(null=True, blank=True)
    website_url = models.URLField(null=True, blank=True)
    email = models.EmailField(null=True, blank=True)
    industry = models.CharField(max_length=100, null=True, blank=True)
    audience_size = models.CharField(
        max_length=20,
        choices=AudienceSize.choices,
        null=True,
        blank=True
    )
    audience_description = models.TextField(null=True, blank=True)
    content_style = models.TextField(null=True, blank=True)
    collaboration_history = models.JSONField(null=True, blank=True)
    enrichment_data = models.JSONField(
        null=True,
        blank=True,
        help_text='Data from Clay or other enrichment APIs'
    )
    source = models.CharField(
        max_length=20,
        choices=Source.choices,
        default=Source.MANUAL
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Profile'
        verbose_name_plural = 'Profiles'

    def __str__(self):
        if self.company:
            return f"{self.name} ({self.company})"
        return self.name


class Match(models.Model):
    """
    Represents a match between a user and a potential JV partner profile.
    Scores are weighted: Intent (45%), Synergy (25%), Momentum (20%), Context (10%)
    """

    class Status(models.TextChoices):
        NEW = 'new', 'New'
        CONTACTED = 'contacted', 'Contacted'
        IN_PROGRESS = 'in_progress', 'In Progress'
        CONVERTED = 'converted', 'Converted'
        DECLINED = 'declined', 'Declined'

    # Score weights
    INTENT_WEIGHT = 0.45
    SYNERGY_WEIGHT = 0.25
    MOMENTUM_WEIGHT = 0.20
    CONTEXT_WEIGHT = 0.10

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='matches'
    )
    profile = models.ForeignKey(
        Profile,
        on_delete=models.CASCADE,
        related_name='matches'
    )
    intent_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Intent signal score (45% weight)'
    )
    synergy_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Audience/content synergy score (25% weight)'
    )
    momentum_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Recent activity/momentum score (20% weight)'
    )
    context_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Contextual relevance score (10% weight)'
    )
    final_score = models.FloatField(
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text='Harmonic mean of weighted scores'
    )
    score_breakdown = models.JSONField(
        null=True,
        blank=True,
        help_text='Detailed explanation of score components'
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.NEW
    )
    notes = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-final_score', '-created_at']
        verbose_name = 'Match'
        verbose_name_plural = 'Matches'
        unique_together = ['user', 'profile']

    def __str__(self):
        return f"Match: {self.user} <-> {self.profile.name} ({self.final_score:.2f})"

    def calculate_final_score(self):
        """
        Calculate the final score as the harmonic mean of weighted scores.

        The harmonic mean is calculated as:
        H = n / (1/x1 + 1/x2 + ... + 1/xn)

        For weighted harmonic mean:
        H = sum(weights) / sum(weight_i / x_i)

        This penalizes low scores more heavily than arithmetic mean,
        ensuring balanced performance across all dimensions.
        """
        weighted_scores = [
            (self.intent_score, self.INTENT_WEIGHT),
            (self.synergy_score, self.SYNERGY_WEIGHT),
            (self.momentum_score, self.MOMENTUM_WEIGHT),
            (self.context_score, self.CONTEXT_WEIGHT),
        ]

        # Avoid division by zero - use small epsilon for zero scores
        epsilon = 1e-10

        total_weight = sum(weight for _, weight in weighted_scores)
        weighted_reciprocal_sum = sum(
            weight / max(score, epsilon)
            for score, weight in weighted_scores
        )

        if weighted_reciprocal_sum == 0:
            self.final_score = 0.0
        else:
            self.final_score = total_weight / weighted_reciprocal_sum

        return self.final_score

    def save(self, *args, **kwargs):
        # Auto-calculate final score if not set or if component scores changed
        if not self.final_score:
            self.calculate_final_score()
        super().save(*args, **kwargs)


class MatchFeedback(models.Model):
    """
    User feedback on match quality to improve future matching.
    """

    class Outcome(models.TextChoices):
        SUCCESSFUL = 'successful', 'Successful'
        UNSUCCESSFUL = 'unsuccessful', 'Unsuccessful'
        PENDING = 'pending', 'Pending'

    match = models.ForeignKey(
        Match,
        on_delete=models.CASCADE,
        related_name='feedback'
    )
    rating = models.IntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(5)],
        help_text='Rating from 1 to 5'
    )
    accuracy_feedback = models.TextField(
        null=True,
        blank=True,
        help_text='Feedback on match accuracy'
    )
    outcome = models.CharField(
        max_length=20,
        choices=Outcome.choices,
        null=True,
        blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Match Feedback'
        verbose_name_plural = 'Match Feedback'

    def __str__(self):
        return f"Feedback for {self.match}: {self.rating}/5"
