from django.db import models


class LaunchPlay(models.Model):
    """The 54 pre-defined plays in the Launch Library"""

    class Phase(models.TextChoices):
        PRE_LAUNCH = 'pre_launch', 'Pre-Launch'
        LAUNCH_ANNOUNCEMENT = 'launch_announcement', 'Launch Announcement'
        LAUNCH_NURTURE = 'launch_nurture', 'Launch Nurture'
        LAUNCH_URGENCY = 'launch_urgency', 'Urgency/Cart Close'
        POST_BUYERS = 'post_buyers', 'Post-Launch Buyers'
        POST_NON_BUYERS = 'post_non_buyers', 'Post-Launch Non-Buyers'

    play_number = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    phase = models.CharField(max_length=30, choices=Phase.choices)
    purpose = models.TextField()
    psychology = models.CharField(max_length=100)
    content_concept = models.TextField()
    aha_moment = models.TextField()
    vibe_authority = models.TextField()
    soft_cta = models.TextField()
    hook_inspirations = models.JSONField(default=list)  # Array of hook ideas

    # For size-based filtering
    included_in_small = models.BooleanField(default=False)  # 12 plays
    included_in_medium = models.BooleanField(default=False)  # 29 plays
    included_in_large = models.BooleanField(default=True)   # 54 plays (all)

    class Meta:
        ordering = ['play_number']
        verbose_name = 'Launch Play'
        verbose_name_plural = 'Launch Plays'

    def __str__(self):
        return f"Play {self.play_number}: {self.name}"


class GeneratedPlaybook(models.Model):
    """User's customized playbook"""

    class Size(models.TextChoices):
        SMALL = 'small', 'Small (12 plays, 2-3 weeks)'
        MEDIUM = 'medium', 'Medium (29 plays, 4-6 weeks)'
        LARGE = 'large', 'Large (54 plays, 60-90 days)'

    user = models.ForeignKey('core.User', on_delete=models.CASCADE, related_name='playbooks')
    transformation = models.ForeignKey('positioning.TransformationAnalysis', on_delete=models.SET_NULL, null=True, blank=True)
    name = models.CharField(max_length=200)
    size = models.CharField(max_length=20, choices=Size.choices, default=Size.MEDIUM)
    launch_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Generated Playbook'
        verbose_name_plural = 'Generated Playbooks'

    def __str__(self):
        return f"{self.name} ({self.get_size_display()})"


class GeneratedPlay(models.Model):
    """Individual customized play content"""

    playbook = models.ForeignKey(GeneratedPlaybook, on_delete=models.CASCADE, related_name='plays')
    launch_play = models.ForeignKey(LaunchPlay, on_delete=models.CASCADE)
    scheduled_date = models.DateField(null=True, blank=True)
    custom_content = models.TextField(blank=True)  # AI-generated content
    custom_hook = models.CharField(max_length=200, blank=True)
    custom_cta = models.TextField(blank=True)
    is_completed = models.BooleanField(default=False)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['scheduled_date', 'launch_play__play_number']
        verbose_name = 'Generated Play'
        verbose_name_plural = 'Generated Plays'

    def __str__(self):
        return f"{self.playbook.name} - {self.launch_play.name}"
