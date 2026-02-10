# Generated manually for SavedCandidate model

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('matching', '0002_supabasematch_supabaseprofile'),
    ]

    operations = [
        migrations.CreateModel(
            name='SavedCandidate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
                ('company', models.CharField(blank=True, max_length=255, null=True)),
                ('seeking', models.TextField(blank=True, null=True)),
                ('offering', models.TextField(blank=True, null=True)),
                ('niche', models.CharField(blank=True, max_length=255, null=True)),
                ('list_size', models.IntegerField(default=0)),
                ('who_you_serve', models.TextField(blank=True, null=True)),
                ('what_you_do', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('added_to_directory', models.ForeignKey(blank=True, help_text='Set when user adds this candidate to the JV directory', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='saved_candidate_source', to='matching.supabaseprofile')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='saved_candidates', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'Saved candidate',
                'verbose_name_plural': 'Saved candidates',
                'ordering': ['-created_at'],
            },
        ),
    ]
