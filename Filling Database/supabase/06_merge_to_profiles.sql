-- Merge staging contacts into profiles with dedup
--
-- Run after loading CSVs into staging_contacts table via COPY or bulk INSERT.
--
-- Deduplication strategy (matches contact_ingestion.py logic):
--   1. Email exact match (case-insensitive)
--   2. Name + company match (case-insensitive)
--   3. No match → insert new profile
--
-- Note: This SQL path does NOT check website domain or LinkedIn URL matches
-- like the Python ingestion pipeline does. For full dedup logic, use the
-- Python import_all.py script instead.
--
-- Usage:
--   psql $DATABASE_URL -f 06_merge_to_profiles.sql

-- Insert new profiles from staging that don't match existing profiles
INSERT INTO profiles (
    id,
    name,
    email,
    company,
    website,
    linkedin,
    phone,
    bio,
    status,
    enrichment_metadata,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid(),
    s.name,
    s.email,
    s.company,
    s.website,
    s.linkedin,
    s.phone,
    s.bio,
    'Prospect',
    jsonb_build_object(
        'ingestion_source', COALESCE(s.source, 'staging_import'),
        'ingested_at', NOW()::text,
        'source_priority', 60
    ),
    NOW(),
    NOW()
FROM staging_contacts s
WHERE
    -- Exclude if email matches existing profile
    NOT EXISTS (
        SELECT 1 FROM profiles p
        WHERE LOWER(p.email) = LOWER(s.email)
            AND s.email IS NOT NULL
            AND s.email != ''
    )
    -- Exclude if name+company matches existing profile
    AND NOT EXISTS (
        SELECT 1 FROM profiles p
        WHERE LOWER(p.name) = LOWER(s.name)
            AND LOWER(p.company) = LOWER(s.company)
            AND s.name IS NOT NULL
            AND s.company IS NOT NULL
            AND s.name != ''
            AND s.company != ''
    )
ON CONFLICT DO NOTHING;

-- Return summary stats
SELECT
    (SELECT COUNT(*) FROM staging_contacts) as staging_total,
    (SELECT COUNT(*) FROM profiles) as profiles_total,
    (SELECT COUNT(*) FROM staging_contacts s
     WHERE NOT EXISTS (
         SELECT 1 FROM profiles p
         WHERE LOWER(p.email) = LOWER(s.email) AND s.email IS NOT NULL AND s.email != ''
     )
     AND NOT EXISTS (
         SELECT 1 FROM profiles p
         WHERE LOWER(p.name) = LOWER(s.name) AND LOWER(p.company) = LOWER(s.company)
         AND s.name IS NOT NULL AND s.company IS NOT NULL
     )
    ) as would_insert;
