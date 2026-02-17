"""
Add secondary_emails column to the Supabase profiles table.

SupabaseProfile is managed=False, so Django won't auto-generate migrations
for it. We use RunSQL to keep schema changes version-controlled and
prevent drift between the Django model and the actual Supabase table.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0009_alter_reportpartner_apply_url_and_more'),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE profiles ADD COLUMN IF NOT EXISTS secondary_emails text[] DEFAULT '{}'",
            reverse_sql="ALTER TABLE profiles DROP COLUMN IF EXISTS secondary_emails",
        ),
    ]
