from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0015_add_engagement_summary_and_insights'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'profiles_status_check'
                ) THEN
                    ALTER TABLE profiles
                    ADD CONSTRAINT profiles_status_check
                    CHECK (status IN ('Member', 'Non Member Resource', 'Pending', 'Prospect', 'Qualified', 'Inactive'));
                END IF;
            END $$;
            """,
            reverse_sql="ALTER TABLE profiles DROP CONSTRAINT IF EXISTS profiles_status_check;",
        ),
    ]
