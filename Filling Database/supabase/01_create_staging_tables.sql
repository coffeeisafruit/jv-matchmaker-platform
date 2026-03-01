-- Staging table for bulk CSV loads (optional, for direct SQL import path)
--
-- This table provides an alternative bulk import path using COPY FROM or
-- bulk INSERT statements, bypassing the Django ORM for maximum throughput.
--
-- Usage:
--   1. Load CSVs into staging_contacts via COPY or bulk INSERT
--   2. Run 06_merge_to_profiles.sql to deduplicate and merge into profiles
--   3. Truncate staging_contacts when done
--
-- Note: The Python import_all.py script uses the Django ingestion pipeline
-- directly and does NOT require this staging table. This is provided as an
-- alternative path for very large imports (100K+ rows).

CREATE TABLE IF NOT EXISTS staging_contacts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    company TEXT,
    website TEXT,
    linkedin TEXT,
    phone TEXT,
    bio TEXT,
    source TEXT,
    source_url TEXT,
    imported_at TIMESTAMP DEFAULT NOW(),
    batch_id TEXT
);

-- Indexes for fast deduplication lookups
CREATE INDEX IF NOT EXISTS idx_staging_email ON staging_contacts(LOWER(email));
CREATE INDEX IF NOT EXISTS idx_staging_name ON staging_contacts(LOWER(name));
CREATE INDEX IF NOT EXISTS idx_staging_company ON staging_contacts(LOWER(company));
CREATE INDEX IF NOT EXISTS idx_staging_batch ON staging_contacts(batch_id);

COMMENT ON TABLE staging_contacts IS 'Staging table for bulk CSV imports before deduplication';
COMMENT ON COLUMN staging_contacts.batch_id IS 'Optional identifier for tracking import batches';
