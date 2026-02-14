"""Add Tier 2 feedback outcome fields to PartnerRecommendation (B4)."""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0005_add_behavioral_tracking_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='partnerrecommendation',
            name='feedback_outcome',
            field=models.CharField(
                blank=True,
                choices=[
                    ('connected_promising', 'Connected and promising'),
                    ('connected_not_fit', 'Connected but not a fit'),
                    ('no_response', 'No response'),
                    ('did_not_reach_out', 'Decided not to reach out'),
                ],
                help_text='Tier 2 follow-up feedback after 7-14 days',
                max_length=30,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='partnerrecommendation',
            name='feedback_notes',
            field=models.TextField(
                blank=True,
                help_text='Optional notes from the user about the outcome',
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='partnerrecommendation',
            name='feedback_recorded_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
