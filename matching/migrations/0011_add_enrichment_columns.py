"""
Version-control enrichment columns that were added directly to Supabase (M5).

SupabaseProfile is managed=False, so Django won't auto-generate migrations.
Uses IF NOT EXISTS / IF EXISTS so the migration is idempotent.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('matching', '0010_add_secondary_emails'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS revenue_tier varchar(20);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS jv_history jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS content_platforms jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS audience_engagement_score double precision;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS enrichment_metadata jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS profile_confidence double precision;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS last_enriched_at timestamptz;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS pagerank_score double precision;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS degree_centrality double precision;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS betweenness_centrality double precision;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS network_role varchar(50);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS centrality_updated_at timestamptz;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS recommendation_pressure_30d integer;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS pressure_updated_at timestamptz;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS audience_type text;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS business_size text;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS current_projects text;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS signature_programs text;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS booking_link text;
            """,
            reverse_sql="""
                ALTER TABLE profiles DROP COLUMN IF EXISTS revenue_tier;
                ALTER TABLE profiles DROP COLUMN IF EXISTS jv_history;
                ALTER TABLE profiles DROP COLUMN IF EXISTS content_platforms;
                ALTER TABLE profiles DROP COLUMN IF EXISTS audience_engagement_score;
                ALTER TABLE profiles DROP COLUMN IF EXISTS enrichment_metadata;
                ALTER TABLE profiles DROP COLUMN IF EXISTS profile_confidence;
                ALTER TABLE profiles DROP COLUMN IF EXISTS last_enriched_at;
                ALTER TABLE profiles DROP COLUMN IF EXISTS pagerank_score;
                ALTER TABLE profiles DROP COLUMN IF EXISTS degree_centrality;
                ALTER TABLE profiles DROP COLUMN IF EXISTS betweenness_centrality;
                ALTER TABLE profiles DROP COLUMN IF EXISTS network_role;
                ALTER TABLE profiles DROP COLUMN IF EXISTS centrality_updated_at;
                ALTER TABLE profiles DROP COLUMN IF EXISTS recommendation_pressure_30d;
                ALTER TABLE profiles DROP COLUMN IF EXISTS pressure_updated_at;
                ALTER TABLE profiles DROP COLUMN IF EXISTS audience_type;
                ALTER TABLE profiles DROP COLUMN IF EXISTS business_size;
                ALTER TABLE profiles DROP COLUMN IF EXISTS current_projects;
                ALTER TABLE profiles DROP COLUMN IF EXISTS signature_programs;
                ALTER TABLE profiles DROP COLUMN IF EXISTS booking_link;
            """,
        ),
    ]
