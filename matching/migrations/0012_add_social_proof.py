"""
Add social_proof column to preserve credentials/awards data separately from bio (P2).

Previously social_proof was silently remapped to bio and discarded if bio existed.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0011_add_enrichment_columns'),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE profiles ADD COLUMN IF NOT EXISTS social_proof text",
            reverse_sql="ALTER TABLE profiles DROP COLUMN IF EXISTS social_proof",
        ),
    ]
