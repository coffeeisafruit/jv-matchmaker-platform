-- Add enrichment metadata tracking to profiles table
-- Run this in Supabase SQL Editor

-- 1. Add enrichment_metadata JSONB column
-- This stores field-level enrichment metadata (source, confidence, dates, etc.)
ALTER TABLE profiles
ADD COLUMN IF NOT EXISTS enrichment_metadata JSONB DEFAULT '{}';

-- 2. Add GIN index for efficient JSONB queries
CREATE INDEX IF NOT EXISTS idx_profiles_enrichment_metadata_gin
ON profiles USING gin(enrichment_metadata);

-- 3. Add profile_confidence column
-- Overall confidence score (0.0-1.0) calculated from field-level scores
ALTER TABLE profiles
ADD COLUMN IF NOT EXISTS profile_confidence FLOAT DEFAULT 0.0;

-- 4. Add last_enriched_at timestamp
-- Tracks when profile was last enriched (any field updated)
ALTER TABLE profiles
ADD COLUMN IF NOT EXISTS last_enriched_at TIMESTAMP;

-- 5. Add index on last_enriched_at for finding stale profiles
CREATE INDEX IF NOT EXISTS idx_profiles_last_enriched_at
ON profiles(last_enriched_at);

-- 6. Add index on profile_confidence for sorting by quality
CREATE INDEX IF NOT EXISTS idx_profiles_confidence
ON profiles(profile_confidence);

-- Verify the changes
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'profiles'
  AND column_name IN ('enrichment_metadata', 'profile_confidence', 'last_enriched_at')
ORDER BY column_name;

-- Example of enrichment_metadata structure:
-- {
--   "email": {
--     "source": "apollo",
--     "enriched_at": "2026-02-09T10:00:00Z",
--     "verified_at": "2026-02-09T10:00:00Z",
--     "source_date": "2026-02-09",
--     "confidence": 0.95,
--     "confidence_expires_at": "2026-05-09T10:00:00Z",
--     "verification_count": 1,
--     "last_verification_method": "api_verified"
--   },
--   "seeking": {
--     "source": "owl",
--     "enriched_at": "2026-02-08T15:30:00Z",
--     "source_date": "2026-02-08",
--     "confidence": 0.75,
--     "confidence_expires_at": "2026-03-10T15:30:00Z",
--     "verification_count": 0,
--     "cross_validated": false
--   }
-- }

-- Example queries using enrichment_metadata:

-- Find profiles with high-confidence emails
SELECT name, email, enrichment_metadata->'email'->>'confidence' as email_confidence
FROM profiles
WHERE (enrichment_metadata->'email'->>'confidence')::float > 0.9
ORDER BY email_confidence DESC
LIMIT 10;

-- Find profiles with stale data (confidence expired)
SELECT name, email, last_enriched_at,
       enrichment_metadata->'email'->>'confidence_expires_at' as email_expires
FROM profiles
WHERE (enrichment_metadata->'email'->>'confidence_expires_at')::timestamp < NOW()
  AND email IS NOT NULL
ORDER BY last_enriched_at
LIMIT 10;

-- Find profiles needing re-enrichment (low confidence or stale)
SELECT name, email, profile_confidence, last_enriched_at
FROM profiles
WHERE profile_confidence < 0.5
   OR last_enriched_at < (NOW() - INTERVAL '90 days')
ORDER BY profile_confidence ASC, last_enriched_at ASC
LIMIT 20;
