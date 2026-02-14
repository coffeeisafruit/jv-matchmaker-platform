"""Add MatchLearningSignal model for outcome analysis (B5)."""

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0006_add_feedback_outcome_fields'),
    ]

    operations = [
        migrations.CreateModel(
            name='MatchLearningSignal',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('outcome', models.CharField(max_length=50)),
                ('outcome_timestamp', models.DateTimeField()),
                ('match_score', models.FloatField()),
                ('explanation_source', models.CharField(blank=True, default='', help_text='llm_verified, llm_partial, or template_fallback at generation time', max_length=20)),
                ('reciprocity_balance', models.CharField(blank=True, default='', help_text='balanced, slightly_asymmetric, or significantly_asymmetric', max_length=30)),
                ('confidence_at_generation', models.JSONField(default=dict, help_text='Snapshot of confidence scores when the match was generated')),
                ('signal_type', models.CharField(choices=[('feedback_tier2', 'Tier 2 Prompted Feedback'), ('contact_made', 'Contact Initiated'), ('view_pattern', 'View Pattern Signal'), ('outreach_used', 'Outreach Message Used')], max_length=50)),
                ('signal_details', models.JSONField(default=dict, help_text='Additional context: view_count, time_to_action, outreach_used, etc.')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('match', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='learning_signals', to='matching.partnerrecommendation')),
            ],
            options={
                'verbose_name': 'Match Learning Signal',
                'verbose_name_plural': 'Match Learning Signals',
                'ordering': ['-created_at'],
                'indexes': [
                    models.Index(fields=['signal_type', 'outcome'], name='matching_mat_signal__cc48c6_idx'),
                    models.Index(fields=['explanation_source', 'outcome'], name='matching_mat_explana_6f2a1e_idx'),
                ],
            },
        ),
    ]
