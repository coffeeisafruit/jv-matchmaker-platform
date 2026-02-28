"""Add dedicated social media columns and migrate data from content_platforms JSONB."""

from django.db import migrations, models


def migrate_social_from_content_platforms(apps, schema_editor):
    """Extract facebook/instagram/youtube/twitter from content_platforms JSONB
    into the new dedicated columns."""
    from django.db import connection

    with connection.cursor() as cursor:
        for platform in ('facebook', 'instagram', 'youtube', 'twitter'):
            cursor.execute(f"""
                UPDATE profiles
                SET {platform} = content_platforms ->> '{platform}'
                WHERE content_platforms ? '{platform}'
                  AND content_platforms ->> '{platform}' IS NOT NULL
                  AND content_platforms ->> '{platform}' != ''
                  AND ({platform} IS NULL OR {platform} = '')
            """)
            updated = cursor.rowcount
            if updated:
                print(f"  Migrated {updated} {platform} URLs from content_platforms")


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0015_add_engagement_summary_and_insights'),
    ]

    operations = [
        migrations.AddField(
            model_name='profile',
            name='facebook',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='profile',
            name='instagram',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='profile',
            name='youtube',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='profile',
            name='twitter',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.RunPython(
            migrate_social_from_content_platforms,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
