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
    secondary_emails = ArrayField(models.TextField(), default=list, blank=True)
    phone = models.TextField(null=True, blank=True)
    company = models.TextField(null=True, blank=True)
    website = models.TextField(null=True, blank=True)
    linkedin = models.TextField(null=True, blank=True)
    avatar_url = models.TextField(null=True, blank=True)
    business_focus = models.TextField(null=True, blank=True)
    status = models.TextField(default='Member', choices=[
        ('Member', 'Member'),
        ('Non Member Resource', 'Non Member Resource'),
        ('Pending', 'Pending'),
        ('Prospect', 'Prospect'),
        ('Qualified', 'Qualified'),
        ('Inactive', 'Inactive'),
    ])
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
    signature_programs = models.TextField(null=True, blank=True)  # Named courses, books, frameworks
    booking_link = models.TextField(null=True, blank=True)  # Calendly, Acuity, etc.
    current_projects = models.TextField(null=True, blank=True)
    niche = models.TextField(null=True, blank=True)
    audience_type = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    profile_updated_at = models.DateTimeField(null=True, blank=True)
    last_active_at = models.DateTimeField(null=True, blank=True)

    # Enrichment metadata (exist in Supabase, previously invisible to Django ORM)
    enrichment_metadata = models.JSONField(null=True, blank=True)
    profile_confidence = models.FloatField(null=True, blank=True)
    last_enriched_at = models.DateTimeField(null=True, blank=True)

    # Network centrality (computed by management commands)
    pagerank_score = models.FloatField(null=True, blank=True)
    degree_centrality = models.FloatField(null=True, blank=True)
    betweenness_centrality = models.FloatField(null=True, blank=True)
    network_role = models.CharField(max_length=50, null=True, blank=True)
    centrality_updated_at = models.DateTimeField(null=True, blank=True)

    # Enrichment: Revenue, JV history, content platforms, engagement
    revenue_tier = models.CharField(max_length=20, null=True, blank=True)  # micro, emerging, established, premium, enterprise
    jv_history = models.JSONField(null=True, blank=True)  # [{partner_name, format, source_quote}]
    content_platforms = models.JSONField(null=True, blank=True)  # {podcast_name, youtube_channel, instagram_handle, ...}
    audience_engagement_score = models.FloatField(null=True, blank=True)  # 0.0-1.0

    # Dedicated social media columns (extracted from content_platforms for direct access)
    facebook = models.TextField(null=True, blank=True)
    instagram = models.TextField(null=True, blank=True)
    youtube = models.TextField(null=True, blank=True)
    twitter = models.TextField(null=True, blank=True)

    # Embedding columns (vector(1024) in Postgres, read as text by Django ORM)
    embedding_seeking = models.TextField(null=True, blank=True)
    embedding_offering = models.TextField(null=True, blank=True)
    embedding_who_you_serve = models.TextField(null=True, blank=True)
    embedding_what_you_do = models.TextField(null=True, blank=True)
    embeddings_model = models.CharField(max_length=100, null=True, blank=True)
    embeddings_updated_at = models.DateTimeField(null=True, blank=True)

    # Recommendation pressure
    recommendation_pressure_30d = models.IntegerField(null=True, blank=True)
    pressure_updated_at = models.DateTimeField(null=True, blank=True)

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


class SavedCandidate(models.Model):
    """
    A candidate saved by a user (e.g., from a guest matching flow)
    before being added to the JV directory.
    """
    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='saved_candidates'
    )
    name = models.CharField(max_length=255)
    company = models.CharField(max_length=255, null=True, blank=True)
    seeking = models.TextField(null=True, blank=True)
    offering = models.TextField(null=True, blank=True)
    niche = models.CharField(max_length=255, null=True, blank=True)
    list_size = models.IntegerField(default=0)
    who_you_serve = models.TextField(null=True, blank=True)
    what_you_do = models.TextField(null=True, blank=True)
    added_to_directory = models.ForeignKey(
        SupabaseProfile,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='saved_candidate_source',
        help_text='Set when user adds this candidate to the JV directory'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Saved candidate'
        verbose_name_plural = 'Saved candidates'

    def __str__(self):
        return f"{self.name} ({self.company or 'No company'})"


class PartnerRecommendation(models.Model):
    """
    Tracks when a partner is recommended to a user, with behavioral signals.
    Used for recommendation pressure, feedback loops, and match quality analysis.
    """

    class Context(models.TextChoices):
        GUEST_MATCH = 'guest_match', 'Guest Candidate Match'
        DIRECTORY_MATCH = 'directory_match', 'Directory Match'
        PARTNER_DETAIL = 'partner_detail', 'Partner Detail View'
        SIMILAR_PARTNERS = 'similar_partners', 'Similar Partners'

    user = models.ForeignKey(
        'core.User',
        on_delete=models.CASCADE,
        related_name='recommendations_made'
    )
    partner = models.ForeignKey(
        SupabaseProfile,
        on_delete=models.CASCADE,
        related_name='recommendations_received'
    )
    candidate = models.ForeignKey(
        SavedCandidate,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='recommendations_made',
        help_text='The candidate being matched (if applicable)'
    )
    recommended_at = models.DateTimeField(auto_now_add=True)
    context = models.CharField(
        max_length=50,
        choices=Context.choices,
        default=Context.DIRECTORY_MATCH
    )

    # Existing behavioral signals
    was_viewed = models.BooleanField(default=False)
    viewed_at = models.DateTimeField(null=True, blank=True)
    was_contacted = models.BooleanField(default=False)
    contacted_at = models.DateTimeField(null=True, blank=True)

    # B3: New behavioral tracking fields
    outreach_message_used = models.BooleanField(default=False)
    time_to_first_action = models.DurationField(
        null=True, blank=True,
        help_text='Time between recommendation and first contact (conversion intent)'
    )
    view_count = models.IntegerField(default=0)
    explanation_source = models.CharField(
        max_length=20, null=True, blank=True,
        help_text='How the match explanation was generated: llm_verified, llm_partial, template_fallback'
    )

    # B4: Tier 2 prompted feedback
    class FeedbackOutcome(models.TextChoices):
        CONNECTED_PROMISING = 'connected_promising', 'Connected and promising'
        CONNECTED_NOT_FIT = 'connected_not_fit', 'Connected but not a fit'
        NO_RESPONSE = 'no_response', 'No response'
        DID_NOT_REACH_OUT = 'did_not_reach_out', 'Decided not to reach out'

    feedback_outcome = models.CharField(
        max_length=30, choices=FeedbackOutcome.choices,
        null=True, blank=True,
        help_text='Tier 2 follow-up feedback after 7-14 days'
    )
    feedback_notes = models.TextField(
        null=True, blank=True,
        help_text='Optional notes from the user about the outcome'
    )
    feedback_recorded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-recommended_at']
        verbose_name = 'Partner Recommendation'
        verbose_name_plural = 'Partner Recommendations'
        indexes = [
            models.Index(fields=['partner', 'recommended_at']),
            models.Index(fields=['partner', 'was_contacted']),
            models.Index(fields=['user', 'recommended_at']),
        ]

    def __str__(self):
        return f"Recommendation: {self.partner} → {self.user} ({self.context})"


class MatchLearningSignal(models.Model):
    """
    Captures learning signals from match outcomes (B5).

    Each record ties a match outcome to the conditions at generation time,
    enabling analysis of what factors predict successful partnerships.
    Designed for batch analysis once 200+ outcomes exist.
    """

    class SignalType(models.TextChoices):
        FEEDBACK_TIER2 = 'feedback_tier2', 'Tier 2 Prompted Feedback'
        CONTACT_MADE = 'contact_made', 'Contact Initiated'
        VIEW_PATTERN = 'view_pattern', 'View Pattern Signal'
        OUTREACH_USED = 'outreach_used', 'Outreach Message Used'

    match = models.ForeignKey(
        PartnerRecommendation,
        on_delete=models.CASCADE,
        related_name='learning_signals',
    )
    outcome = models.CharField(max_length=50)
    outcome_timestamp = models.DateTimeField()
    match_score = models.FloatField()
    explanation_source = models.CharField(
        max_length=20, blank=True, default='',
        help_text='llm_verified, llm_partial, or template_fallback at generation time'
    )
    reciprocity_balance = models.CharField(
        max_length=30, blank=True, default='',
        help_text='balanced, slightly_asymmetric, or significantly_asymmetric'
    )
    confidence_at_generation = models.JSONField(
        default=dict,
        help_text='Snapshot of confidence scores when the match was generated'
    )
    signal_type = models.CharField(max_length=50, choices=SignalType.choices)
    signal_details = models.JSONField(
        default=dict,
        help_text='Additional context: view_count, time_to_action, outreach_used, etc.'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Match Learning Signal'
        verbose_name_plural = 'Match Learning Signals'
        indexes = [
            models.Index(fields=['signal_type', 'outcome']),
            models.Index(fields=['explanation_source', 'outcome']),
        ]

    def __str__(self):
        return f"Signal: {self.signal_type} → {self.outcome} (match #{self.match_id})"


# =============================================================================
# MEMBER REPORT MODELS (Access-coded monthly partner reports)
# =============================================================================

class MemberReport(models.Model):
    """
    A monthly partner report for a paying member ($100/month tier).
    Accessed via unique code — no platform login required.
    """
    member_name = models.CharField(max_length=255)
    member_email = models.EmailField()
    company_name = models.CharField(max_length=255)

    # Access control
    access_code = models.CharField(max_length=20, unique=True, db_index=True)
    month = models.DateField(help_text='First day of the month this report covers')
    expires_at = models.DateTimeField(help_text='When this access code stops working')
    is_active = models.BooleanField(default=True)

    # Client profile data (rendered on the Client Profile page)
    client_profile = models.JSONField(
        default=dict,
        help_text='Client one-pager data matching DEMO_PROFILE structure'
    )

    # Optional FK to source profile in the directory
    supabase_profile = models.ForeignKey(
        SupabaseProfile, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='member_reports'
    )

    # Outreach email templates (rendered in the template modal)
    outreach_templates = models.JSONField(
        default=dict,
        help_text='{"initial": {"title": "...", "text": "..."}, "followup": {"title": "...", "text": "..."}}'
    )

    # Optional launch date for countdown timer
    launch_date = models.DateTimeField(null=True, blank=True)

    # Footer text (e.g., commission info)
    footer_text = models.CharField(max_length=500, blank=True)

    # Tracking
    last_accessed_at = models.DateTimeField(null=True, blank=True)
    access_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-month', '-created_at']
        verbose_name = 'Member Report'
        verbose_name_plural = 'Member Reports'

    def __str__(self):
        return f"{self.member_name} - {self.month.strftime('%B %Y')}"

    @property
    def is_expired(self):
        from django.utils import timezone
        return timezone.now() > self.expires_at

    @property
    def is_accessible(self):
        return self.is_active and not self.is_expired

    @property
    def is_stale(self):
        """Report is stale if older than 30 days."""
        from django.utils import timezone
        return (timezone.now() - self.created_at).days >= 30


class ReportPartner(models.Model):
    """
    A partner included in a member's monthly report.
    Stores a snapshot of partner data at report generation time.
    """
    report = models.ForeignKey(
        MemberReport, on_delete=models.CASCADE, related_name='partners'
    )

    # Display order and section
    rank = models.IntegerField(default=0)
    section = models.CharField(
        max_length=20,
        help_text='priority, this_week, low_priority, jv_programs'
    )
    section_label = models.CharField(max_length=100, blank=True)
    section_note = models.CharField(max_length=200, blank=True)

    # Partner identity
    name = models.CharField(max_length=255)
    company = models.CharField(max_length=255, blank=True)
    tagline = models.CharField(max_length=500, blank=True)

    # Contact info (TextField, not URLField — Supabase data is messy)
    email = models.TextField(blank=True)
    website = models.TextField(blank=True)
    phone = models.CharField(max_length=50, blank=True)
    linkedin = models.TextField(blank=True)
    apply_url = models.TextField(blank=True, help_text='For JV Programs with application links')
    schedule = models.TextField(blank=True)

    # Display fields
    badge = models.CharField(max_length=50, blank=True)
    badge_style = models.CharField(
        max_length=20, default='fit',
        help_text='priority, fit, or warn'
    )
    list_size = models.CharField(max_length=20, blank=True, help_text='Display format: 295K, 91K')
    audience = models.TextField(blank=True, help_text='Audience description for expanded view')
    why_fit = models.TextField(blank=True)
    detail_note = models.TextField(blank=True, help_text='Italic note below why-fit')
    tags = models.JSONField(
        default=list,
        help_text='[{"label": "Women", "style": "fit"}, {"label": "Active JV", "style": "priority"}]'
    )

    # Score and traceability
    match_score = models.FloatField(null=True, blank=True)
    source_profile = models.ForeignKey(
        SupabaseProfile, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='report_appearances'
    )

    class Meta:
        ordering = ['report', 'section', 'rank']
        verbose_name = 'Report Partner'
        verbose_name_plural = 'Report Partners'

    def __str__(self):
        return f"{self.name} (in {self.report})"


# =============================================================================
# ENGAGEMENT ANALYTICS MODELS
# =============================================================================

class OutreachEvent(models.Model):
    """
    Raw client-side event from outreach pages.
    Table managed by SQL migration (0014), not Django ORM.
    Populated directly by browser JS via Supabase REST API.
    Read by Django ORM for aggregation and admin display.
    """
    report_id = models.IntegerField()
    access_code = models.CharField(max_length=20)
    event_type = models.CharField(max_length=50)
    partner_id = models.CharField(max_length=255, null=True, blank=True)
    details = models.JSONField(default=dict)
    session_id = models.CharField(max_length=64, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'outreach_events'
        ordering = ['-created_at']
        verbose_name = 'Outreach Event'
        verbose_name_plural = 'Outreach Events'

    def __str__(self):
        return f"{self.event_type} | report={self.report_id} | {self.created_at}"


class EngagementSummary(models.Model):
    """
    Pre-computed engagement metrics per report + partner.
    Regenerated by the compute_engagement_metrics management command.
    Partner-level when partner_id is set, report-level when partner_id is empty.
    """
    report = models.ForeignKey(
        MemberReport, on_delete=models.CASCADE, related_name='engagement_summaries'
    )
    partner_id = models.CharField(
        max_length=40, blank=True, default='', db_index=True,
        help_text='SupabaseProfile UUID, or empty for report-level aggregates'
    )

    # Page-level metrics (used when partner_id='')
    total_sessions = models.IntegerField(default=0)
    total_page_views = models.IntegerField(default=0)
    avg_time_on_page_secs = models.IntegerField(default=0)
    avg_scroll_depth_pct = models.IntegerField(default=0)
    last_visit_at = models.DateTimeField(null=True, blank=True)
    days_since_last_visit = models.IntegerField(null=True, blank=True)
    return_visit_count = models.IntegerField(default=0)
    template_open_count = models.IntegerField(default=0)
    template_copy_count = models.IntegerField(default=0)

    # Partner-level metrics (used when partner_id is set)
    card_expand_count = models.IntegerField(default=0)
    avg_card_dwell_ms = models.IntegerField(default=0)
    was_checked = models.BooleanField(default=False)
    email_click_count = models.IntegerField(default=0)
    linkedin_click_count = models.IntegerField(default=0)
    schedule_click_count = models.IntegerField(default=0)
    apply_click_count = models.IntegerField(default=0)
    any_contact_action = models.BooleanField(default=False)
    first_action_at = models.DateTimeField(null=True, blank=True)
    time_to_first_action_secs = models.IntegerField(null=True, blank=True)

    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Engagement Summary'
        verbose_name_plural = 'Engagement Summaries'
        unique_together = ['report', 'partner_id']
        indexes = [
            models.Index(fields=['report', 'any_contact_action']),
        ]

    def __str__(self):
        if self.partner_id:
            return f"Summary: report={self.report_id} partner={self.partner_id[:8]}"
        return f"Summary: report={self.report_id} (page-level)"


class AnalyticsInsight(models.Model):
    """
    Proactive insight generated by the analytics engine.
    Developer-facing alerts based on engagement patterns.
    """
    class Severity(models.TextChoices):
        INFO = 'info', 'Info'
        WARNING = 'warning', 'Warning'
        CRITICAL = 'critical', 'Critical'

    class Category(models.TextChoices):
        IDLE_REPORT = 'idle_report', 'Idle Report'
        SCORE_CALIBRATION = 'score_calibration', 'Score Calibration'
        TEMPLATE_FRICTION = 'template_friction', 'Template Friction'
        ENRICHMENT_QUALITY = 'enrichment_quality', 'Enrichment Quality'
        PARTNER_FRICTION = 'partner_friction', 'Partner Friction'
        ENGAGEMENT_PATTERN = 'engagement_pattern', 'Engagement Pattern'
        SECTION_ATTENTION = 'section_attention', 'Section Attention'
        INTERVENTION_VERIFIED = 'intervention_verified', 'Intervention Verified'
        INTERVENTION_PENDING = 'intervention_pending', 'Intervention Pending'

    report = models.ForeignKey(
        MemberReport, on_delete=models.CASCADE,
        null=True, blank=True, related_name='insights'
    )
    severity = models.CharField(max_length=10, choices=Severity.choices)
    category = models.CharField(max_length=30, choices=Category.choices)
    title = models.CharField(max_length=200)
    description = models.TextField()
    data = models.JSONField(default=dict, help_text='Supporting data for the insight')
    is_active = models.BooleanField(default=True)
    is_dismissed = models.BooleanField(default=False)
    dismissed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Analytics Insight'
        verbose_name_plural = 'Analytics Insights'

    def __str__(self):
        return f"[{self.severity}] {self.title}"


class AnalyticsIntervention(models.Model):
    """
    Records an action taken in response to an insight.
    Links to a before/after measurement window for verification.
    """
    class InterventionType(models.TextChoices):
        WEIGHT_CHANGE = 'weight_change', 'ISMC Weight Adjustment'
        RE_ENRICHMENT = 're_enrichment', 'Profile Re-Enrichment'
        REPORT_RESTRUCTURE = 'report_restructure', 'Report Section/Order Change'
        TEMPLATE_UPDATE = 'template_update', 'Outreach Template Revision'
        WHY_FIT_UPGRADE = 'why_fit_upgrade', 'Why-Fit Narrative Improvement'
        PARTNER_SWAP = 'partner_swap', 'Partner Added/Removed from Report'
        NUDGE_SENT = 'nudge_sent', 'Client Nudge/Check-in'
        OTHER = 'other', 'Other'

    insight = models.ForeignKey(
        AnalyticsInsight, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='interventions',
        help_text='The insight that prompted this action (if any)'
    )
    report = models.ForeignKey(
        MemberReport, on_delete=models.CASCADE,
        null=True, blank=True, related_name='interventions'
    )
    intervention_type = models.CharField(max_length=30, choices=InterventionType.choices)
    description = models.TextField(help_text='What was changed and why')

    baseline_metrics = models.JSONField(
        default=dict,
        help_text='Engagement metrics snapshot before intervention'
    )
    followup_metrics = models.JSONField(
        default=dict,
        help_text='Engagement metrics snapshot after measurement window'
    )
    measurement_window_days = models.IntegerField(
        default=14,
        help_text='Days to wait before measuring impact'
    )

    verified_at = models.DateTimeField(null=True, blank=True)
    impact_assessment = models.CharField(
        max_length=20, blank=True,
        choices=[
            ('positive', 'Positive Impact'),
            ('neutral', 'No Measurable Change'),
            ('negative', 'Negative Impact'),
            ('insufficient_data', 'Insufficient Data'),
        ]
    )
    impact_details = models.TextField(blank=True, help_text='What changed and by how much')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Analytics Intervention'
        verbose_name_plural = 'Analytics Interventions'

    def __str__(self):
        return f"{self.get_intervention_type_display()} ({self.created_at:%Y-%m-%d})"


class ClientVerification(models.Model):
    """Tracks monthly client profile verification status.

    Each record represents a single verification request.  The
    ``verification_token`` is a UUID included in the emailed link so the
    client can access the confirmation form without logging in.
    """

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('confirmed', 'Confirmed'),
        ('expired', 'Expired'),
    ]

    client = models.ForeignKey(
        SupabaseProfile, on_delete=models.CASCADE,
        related_name='verifications',
    )
    verification_token = models.UUIDField(
        default=uuid.uuid4, unique=True, db_index=True,
        help_text='Token embedded in the verification email link',
    )
    month = models.DateField(help_text="First day of the verification month")
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default='pending',
    )
    sent_at = models.DateTimeField(null=True, blank=True)
    opened_at = models.DateTimeField(null=True, blank=True)
    confirmed_at = models.DateTimeField(null=True, blank=True)
    reminder_count = models.IntegerField(default=0)
    changes_made = models.JSONField(default=dict, blank=True)
    original_data = models.JSONField(
        default=dict, blank=True,
        help_text='Snapshot of profile data at the time the verification was created',
    )
    updated_data = models.JSONField(
        default=dict, blank=True,
        help_text='Data submitted by the client via the confirmation form',
    )
    fields_confirmed = models.JSONField(
        default=list, blank=True,
        help_text='List of field names the client explicitly confirmed or edited',
    )
    template_variant = models.CharField(
        max_length=30, default='initial',
        choices=[
            ('initial', 'Initial'),
            ('follow_up', 'Follow-up'),
            ('final_reminder', 'Final Reminder'),
        ],
    )

    class Meta:
        unique_together = ('client', 'month')
        ordering = ['-month']

    def __str__(self):
        return f"{self.client} - {self.month:%Y-%m} ({self.status})"


class MonthlyProcessingResult(models.Model):
    """Stores per-client monthly processing outcomes for admin review."""
    client = models.ForeignKey(
        SupabaseProfile, on_delete=models.CASCADE,
        related_name='processing_results',
    )
    month = models.DateField()
    profiles_enriched = models.IntegerField(default=0)
    profiles_rescored = models.IntegerField(default=0)
    matches_above_70 = models.IntegerField(default=0)
    gap_detected = models.BooleanField(default=False)
    prospects_discovered = models.IntegerField(default=0)
    prospects_enriched = models.IntegerField(default=0)
    report_regenerated = models.BooleanField(default=False)
    processing_cost = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    errors = models.JSONField(default=list, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ('client', 'month')
        ordering = ['-month']

    def __str__(self):
        return f"{self.client} - {self.month:%Y-%m}"


class SearchCostLog(models.Model):
    """Per-query cost tracking across all search tools."""
    tool = models.CharField(max_length=30, db_index=True)
    query = models.TextField()
    cost_usd = models.DecimalField(max_digits=8, decimal_places=6)
    results_returned = models.IntegerField(default=0)
    results_useful = models.IntegerField(default=0)
    context = models.CharField(max_length=100, blank=True, default='')
    profile_id = models.CharField(max_length=100, blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['tool', 'created_at']),
        ]

    def __str__(self):
        return f"{self.tool}: ${self.cost_usd} ({self.results_useful}/{self.results_returned} useful)"
