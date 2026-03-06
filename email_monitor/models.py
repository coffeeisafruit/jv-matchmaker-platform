from django.db import models
from matching.models import SupabaseProfile


class MonitoredSubscription(models.Model):
    """Tracks a single newsletter subscription we've made on behalf of a profile."""

    DISCOVERY_METHOD_CHOICES = [
        ('crawl4ai_form', 'Crawl4AI Form'),
        ('manual', 'Manual'),
        ('esp_api', 'ESP API'),
        ('headless', 'Headless Browser'),
    ]
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('active', 'Active'),
        ('pending_confirm', 'Pending Confirmation'),
        ('unsubscribed', 'Unsubscribed'),
        ('bouncing', 'Bouncing'),
        ('failed', 'Failed'),
    ]

    profile = models.ForeignKey(
        SupabaseProfile, on_delete=models.CASCADE,
        related_name='monitored_subscriptions',
    )
    monitor_address = models.EmailField(unique=True)  # jvmonitor+{uuid[:8]}@gmail.com
    signup_url = models.TextField(blank=True)
    form_action = models.TextField(blank=True, default='')  # POST target URL (stored at discovery time)
    esp_detected = models.CharField(max_length=50, blank=True)  # ConvertKit / Mailchimp / etc.
    discovery_method = models.CharField(
        max_length=20, choices=DISCOVERY_METHOD_CHOICES, default='crawl4ai_form'
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    failure_reason = models.TextField(blank=True, default='')  # why subscription failed (for retry logic)
    subscribed_at = models.DateTimeField(auto_now_add=True)
    last_email_received_at = models.DateTimeField(null=True, blank=True)
    total_emails_received = models.IntegerField(default=0)

    class Meta:
        ordering = ['-subscribed_at']
        verbose_name = 'Monitored Subscription'
        verbose_name_plural = 'Monitored Subscriptions'

    def __str__(self) -> str:
        return f'{self.profile.name} → {self.monitor_address} [{self.status}]'


class InboundEmail(models.Model):
    """A single email received from a monitored newsletter subscription."""

    subscription = models.ForeignKey(
        MonitoredSubscription, on_delete=models.CASCADE,
        related_name='emails',
    )
    gmail_message_id = models.CharField(max_length=100, unique=True)  # dedup key
    from_address = models.EmailField()
    from_name = models.CharField(max_length=255, blank=True)
    subject = models.TextField()
    received_at = models.DateTimeField(db_index=True)
    body_text = models.TextField(blank=True)  # purged after 90 days
    body_html = models.TextField(blank=True)  # purged after 90 days
    links_extracted = models.JSONField(default=list)  # [{url, anchor_text, is_affiliate, affiliate_network}]
    analysis = models.JSONField(null=True, blank=True)  # AI classification result
    analyzed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-received_at']
        verbose_name = 'Inbound Email'
        verbose_name_plural = 'Inbound Emails'
        indexes = [
            models.Index(fields=['subscription', 'received_at']),
        ]

    def __str__(self) -> str:
        return f'{self.from_name or self.from_address}: {self.subject[:60]}'


class EmailActivitySummary(models.Model):
    """Monthly aggregated email activity for a profile."""

    profile = models.ForeignKey(
        SupabaseProfile, on_delete=models.CASCADE,
        related_name='email_activity_summaries',
    )
    month = models.DateField()  # first day of month
    emails_sent = models.IntegerField(default=0)
    avg_emails_per_week = models.FloatField(default=0.0)
    promotional_emails = models.IntegerField(default=0)
    own_product_emails = models.IntegerField(default=0)
    content_only_emails = models.IntegerField(default=0)
    promotion_ratio = models.FloatField(default=0.0)
    unique_partners_promoted = models.IntegerField(default=0)
    partners_promoted = models.JSONField(default=list)  # [{name, url, count, affiliate_detected}]
    promotion_types = models.JSONField(default=dict)  # {webinar: 3, course_launch: 2}
    mailing_activity_score = models.FloatField(default=0.0)  # 0-1
    promotion_willingness_score = models.FloatField(default=0.0)  # 0-1
    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [['profile', 'month']]
        ordering = ['-month']
        verbose_name = 'Email Activity Summary'
        verbose_name_plural = 'Email Activity Summaries'

    def __str__(self) -> str:
        return f'{self.profile.name} — {self.month.strftime("%B %Y")}'
