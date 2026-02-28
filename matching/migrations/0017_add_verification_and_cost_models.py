import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0016_update_profile_status_choices'),
    ]

    operations = [
        migrations.CreateModel(
            name='ClientVerification',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('month', models.DateField(help_text='First day of the verification month')),
                ('sent_at', models.DateTimeField(blank=True, null=True)),
                ('opened_at', models.DateTimeField(blank=True, null=True)),
                ('confirmed_at', models.DateTimeField(blank=True, null=True)),
                ('reminder_count', models.IntegerField(default=0)),
                ('changes_made', models.JSONField(blank=True, default=dict)),
                ('template_variant', models.CharField(
                    choices=[('initial', 'Initial'), ('follow_up', 'Follow-up'), ('final_reminder', 'Final Reminder')],
                    default='initial', max_length=30,
                )),
                ('client', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='verifications',
                    to='matching.supabaseprofile',
                )),
            ],
            options={
                'ordering': ['-month'],
                'unique_together': {('client', 'month')},
            },
        ),
        migrations.CreateModel(
            name='MonthlyProcessingResult',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('month', models.DateField()),
                ('profiles_enriched', models.IntegerField(default=0)),
                ('profiles_rescored', models.IntegerField(default=0)),
                ('matches_above_70', models.IntegerField(default=0)),
                ('gap_detected', models.BooleanField(default=False)),
                ('prospects_discovered', models.IntegerField(default=0)),
                ('prospects_enriched', models.IntegerField(default=0)),
                ('report_regenerated', models.BooleanField(default=False)),
                ('processing_cost', models.DecimalField(decimal_places=4, default=0, max_digits=8)),
                ('errors', models.JSONField(blank=True, default=list)),
                ('completed_at', models.DateTimeField(blank=True, null=True)),
                ('client', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='processing_results',
                    to='matching.supabaseprofile',
                )),
            ],
            options={
                'ordering': ['-month'],
                'unique_together': {('client', 'month')},
            },
        ),
        migrations.CreateModel(
            name='SearchCostLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tool', models.CharField(db_index=True, max_length=30)),
                ('query', models.TextField()),
                ('cost_usd', models.DecimalField(decimal_places=6, max_digits=8)),
                ('results_returned', models.IntegerField(default=0)),
                ('results_useful', models.IntegerField(default=0)),
                ('context', models.CharField(blank=True, default='', max_length=100)),
                ('profile_id', models.CharField(blank=True, default='', max_length=100)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'ordering': ['-created_at'],
                'indexes': [
                    models.Index(fields=['tool', 'created_at'], name='matching_sea_tool_created_idx'),
                ],
            },
        ),
    ]
