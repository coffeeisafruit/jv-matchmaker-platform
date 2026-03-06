-- Email monitor columns for SupabaseProfile (profiles table)
-- Run against Supabase: psql $DATABASE_URL -f scripts/sql/add_email_monitor_columns.sql

ALTER TABLE profiles ADD COLUMN IF NOT EXISTS email_list_activity_score FLOAT;
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS promotion_willingness_score FLOAT;
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS last_email_list_check_at TIMESTAMPTZ;
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS promotion_network JSONB;

-- GIN index for promotion network queries ("who promotes offers like X")
CREATE INDEX IF NOT EXISTS idx_profiles_promotion_network ON profiles USING GIN (promotion_network);
