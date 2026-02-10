-- Add columns for OWL enrichment data
-- Run this in the Supabase SQL Editor

-- Signature programs: Named courses, books, certifications, frameworks
ALTER TABLE profiles
ADD COLUMN IF NOT EXISTS signature_programs TEXT;

-- Booking link: Calendly, Acuity, TidyCal, etc.
ALTER TABLE profiles
ADD COLUMN IF NOT EXISTS booking_link TEXT;

-- Optional: Add indexes for faster searching
CREATE INDEX IF NOT EXISTS idx_profiles_signature_programs
ON profiles USING gin(to_tsvector('english', coalesce(signature_programs, '')));

COMMENT ON COLUMN profiles.signature_programs IS 'Named programs, courses, books, certifications - high value for JV matching';
COMMENT ON COLUMN profiles.booking_link IS 'Calendar booking URL (Calendly, Acuity, etc.) - critical for outreach';
