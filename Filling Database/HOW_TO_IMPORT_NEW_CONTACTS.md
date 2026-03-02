# How to Import New Contacts into Supabase

> Reference guide for bulk-importing scraped contacts into the JV Matchmaker database.
> Last updated: 2026-03-01

## Overview

The import pipeline takes CSV contacts from scraping → staging table → deduplicated profiles → priority-tagged for enrichment. The key constraint: enriching contacts costs $0.01-0.10 each via API calls, so we import all cheaply via SQL, then enrich selectively by ROI.

**Architecture**: `CSV → transform → psql COPY → staging table → merge SQL → profiles → priority tagging → enrichment queue`

---

## Data Sources

We have two separate scraping pipelines that produce importable CSVs:

### Pipeline A: Partnership Scraper (`Filling Database/partners/`)

The main bulk scraping pipeline. Produces per-source CSVs (600+ files) and a `_master_contacts_v2.csv` (1.14M lines). Contains a mix of businesses, organizations, and individuals from BBB, IRS, Clutch, government databases, etc.

- **Master CSV**: `Filling Database/_master_contacts_v2.csv` or `MASTER_JV_CONTACTS.csv`
- **Merge script**: `Filling Database/merge_all_contacts.py`
- **Import path**: Use Phase 1-5 below (SQL COPY pipeline)

### Pipeline B: JV Sourcing Framework (`scripts/sourcing/`)

Purpose-built scraping framework targeting JV-relevant individuals (coaches, speakers, podcasters, authors, founders). Sources include Apple Podcasts, Sessionize, Psychology Today, Noomii, Open Library, Google Books, YouTube API, YC Companies, Wikidata, and more.

- **Raw output**: `scripts/sourcing/output/*.csv` (per-source CSVs)
- **Merged**: `scripts/sourcing/output/MERGED_ALL.csv` (122K+ contacts, includes academic noise)
- **JV-filtered**: `scripts/sourcing/output/JV_FILTERED.csv` (~35K contacts, JV-relevant only)

#### How the JV filter works

`scripts/sourcing/filter_jv.py` classifies each contact:

| Classification | Action | Sources |
|----------------|--------|---------|
| **Auto-include** | Keep all | Sessionize, Noomii, Speaking.com, Apple Podcasts, Psychology Today, YouTube API, Expertfile, TEDx, YC Companies, 500 Global, Clutch Agencies, Shopify Partners, Greylock, MassChallenge, Axial, OpenVC, VC Sheet, MunchEye |
| **Exclude entirely** | Drop all | Crossref (academic citation database — every entry is academic paper authors) |
| **Keyword filter** | Keep only JV-relevant | Open Library, Google Books, Wikidata — requires strong JV keywords (life coach, business coach, digital marketing, self-help, etc.) AND not from an academic publisher |

```bash
# Re-run the filter (reads MERGED_ALL.csv, writes JV_FILTERED.csv)
python3 scripts/sourcing/filter_jv.py

# Stats only (no file write)
python3 scripts/sourcing/filter_jv.py --stats
```

#### To add new scraper data and re-filter

```bash
# 1. Merge all per-source CSVs into MERGED_ALL.csv
python3 scripts/sourcing/dedup_merge.py

# 2. Re-run JV filter
python3 scripts/sourcing/filter_jv.py
```

#### To import JV_FILTERED.csv

Use the same Phase 1-5 SQL pipeline below, pointing at `JV_FILTERED.csv` instead of `MASTER_JV_CONTACTS.csv`:

```bash
# Transform (update source path in script or copy file first)
cp scripts/sourcing/output/JV_FILTERED.csv "Filling Database/MASTER_JV_CONTACTS.csv"
python3 "Filling Database/supabase/transform_for_import.py"

# Then continue with Phase 2-5 as normal
```

**Alternatively**, use the direct Python importer (bypasses SQL staging, uses psycopg2):

```bash
# Imports JV_FILTERED.csv directly to Supabase with name-based dedup
venv/bin/python scripts/sourcing/import_csv.py
```

> **Note**: `import_csv.py` defaults to reading `JV_FILTERED.csv`. It inserts with `status='Pending'` and deduplicates against existing profiles by email, website domain, LinkedIn, and name. It requires the Django venv (`venv/bin/python`) for psycopg2, but does NOT require Django itself.

---

## Prerequisites

- `MASTER_JV_CONTACTS.csv` in `Filling Database/` (or any CSV with the standard 9-column header)
- Access to Supabase PostgreSQL via `$DATABASE_URL` (in `.env`)
- `psql` CLI installed
- Python 3 with `psycopg2` for priority tagging

**Standard CSV Header**: `name,email,company,website,linkedin,phone,bio,source,source_url`

---

## Phase 1: CSV Transformation

**Why**: The master CSV has `source` values like `bbb_sitemap` that need mapping to `scraper_bbb_sitemap` format. Platform-domain websites (clutch.co/profile/..., bbb.org/...) should be stripped since they aren't real company websites.

**Script**: `Filling Database/supabase/transform_for_import.py`

Steps:
1. Read `MASTER_JV_CONTACTS.csv`
2. Map `source` → `scraper_{source}` format
3. Strip rows where name is empty or < 2 chars
4. Drop platform-domain websites (not real company sites)
5. Write `MASTER_JV_CONTACTS_IMPORT.csv`

```bash
python3 "Filling Database/supabase/transform_for_import.py"
```

---

## Phase 2: SQL Bulk Import

**Why SQL over Python**: 100x faster (2 min vs 4-8 hours), no Django dependency needed.

### Step 2a: Create/update staging table

```bash
psql $DATABASE_URL -f "Filling Database/supabase/01_create_staging_tables.sql"
```

The staging table has columns: `name, email, company, website, linkedin, phone, bio, source, source_url, batch_id`

### Step 2b: COPY CSV into staging

```bash
psql $DATABASE_URL -c "\COPY staging_contacts(name,email,company,website,linkedin,phone,bio,source,source_url) FROM 'Filling Database/MASTER_JV_CONTACTS_IMPORT.csv' CSV HEADER"
```

### Step 2c: Merge to profiles (with 4-stage dedup)

```bash
psql $DATABASE_URL -f "Filling Database/supabase/06_merge_to_profiles.sql"
```

The merge SQL performs 4-stage deduplication (matching the Python pipeline in `matching/enrichment/flows/contact_ingestion.py`):

1. **Email match** — skip if email already exists in profiles
2. **Website domain match** — normalize `https://www.example.com` → `example.com`
3. **LinkedIn match** — skip if LinkedIn URL already exists
4. **Name + Company match** — fuzzy match on UPPER(name) + UPPER(company)

Key settings in the merge:
- `source_priority = 20` (scraper tier — lowest, so enrichment can overwrite)
- `status = 'Prospect'` (consistent with Python ingestion pipeline)
- `enrichment_metadata` includes: `ingestion_source`, `source_url`, `ingested_at`, `source_priority`, `batch_id`

---

## Phase 3: Enrichment Priority Tagging

**Why**: Without tagging, all imported contacts look identical to the enrichment queue. We need to mark which ones are high-value so they get enriched first.

**Script**: `Filling Database/supabase/tag_enrichment_priority.py`

Uses direct psycopg2 to update `enrichment_metadata` with `jv_priority` and `enrichment_priority_score`.

```bash
python3 "Filling Database/supabase/tag_enrichment_priority.py"
```

### Priority Tiers by Source

| Tier | Sources | Approx Count | Score Range |
|------|---------|-------------|-------------|
| **High** | yc_companies, sessionize, shareasale, trustpilot, coaching_federation, techstars, producthunt, apple_podcasts, youtube_api, muncheye | ~21K | 80-100 |
| **Medium** | clutch_sitemap, bbb_sitemap, sec_edgar, fdic_banks, sam_awards, noomii | ~700K | 40-70 |
| **Low** | irs_exempt, irs_business_leagues, epa_echo, usaspending, census | ~900K | 10-30 |

**Bonus scoring**: +20 if has email, +10 if has LinkedIn, +5 if has phone

---

## Phase 4: Enrichment Queue Integration

The existing `scripts/automated_enrichment_pipeline_safe.py` selects profiles by tier (website presence, list_size, etc.), but imported contacts don't have list_size or seeking yet.

Add a **Tier 0.5** to `get_profiles_to_enrich()` that prioritizes high-value imports:

```python
# High-value imports that haven't been enriched yet
tier_0_5_query = """
  SELECT id, name, email, company, website, linkedin, bio
  FROM profiles
  WHERE status = 'Prospect'
    AND (enrichment_metadata->>'jv_priority') = 'high'
    AND website IS NOT NULL AND website != ''
    AND (seeking IS NULL OR seeking = '')
    AND last_enriched_at IS NULL
  ORDER BY (enrichment_metadata->>'enrichment_priority_score')::int DESC NULLS LAST
  LIMIT %s
"""
```

This ensures high-value scraped contacts get enriched before generic Tier 3-5 contacts.

**File**: `scripts/automated_enrichment_pipeline_safe.py`

---

## Phase 5: Validation

Run these checks after import:

```sql
-- 1. Total imported
SELECT COUNT(*) FROM profiles WHERE enrichment_metadata->>'batch_id' = 'bulk_import_2026_03';

-- 2. Source priority correct (should match count from #1)
SELECT COUNT(*) FROM profiles
WHERE (enrichment_metadata->>'source_priority')::int = 20
  AND enrichment_metadata->>'batch_id' = 'bulk_import_2026_03';

-- 3. Priority distribution
SELECT enrichment_metadata->>'jv_priority' AS tier, COUNT(*)
FROM profiles
WHERE enrichment_metadata->>'batch_id' = 'bulk_import_2026_03'
GROUP BY 1;

-- 4. No client profile overwrites (should be 0)
SELECT COUNT(*) FROM profiles
WHERE status IN ('Member', 'Qualified')
  AND (enrichment_metadata->>'source_priority')::int = 20;

-- 5. Spot-check 10 random high-priority contacts
SELECT name, company, website, email, enrichment_metadata->>'jv_priority'
FROM profiles
WHERE enrichment_metadata->>'jv_priority' = 'high'
ORDER BY RANDOM() LIMIT 10;
```

### Smoke test enrichment

```bash
python3 scripts/automated_enrichment_pipeline_safe.py --batch-size 10 --limit 10
```

Check that enriched contacts have `seeking`/`offering` fields populated.

---

## Key Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Import method | SQL COPY + merge | 100x faster than Python, no Django needed |
| Import scope | All contacts | Storage is cheap ($0.20/mo), data loss is permanent |
| Dedup strategy | 4-stage SQL (email + website + linkedin + name+company) | Matches Python pipeline logic in `contact_ingestion.py` |
| Initial status | Prospect | Consistent with existing ingestion pipeline |
| Source priority | 20 (scraper tier) | Prevents scraper data from overwriting enrichment |
| Enrichment approach | ROI-tiered, high-value first | 21K contacts @ ~$1K vs 1.6M @ ~$80K |

## What NOT to Do

- **Don't install Django** just for import — SQL path is faster and simpler
- **Don't enrich all contacts** — cost-prohibitive, most are low JV probability
- **Don't filter before import** — we already deduplicated, and "low value" contacts may match niche member needs
- **Don't set source_priority > 20** — scraper data must be overwritable by enrichment results
- **Don't skip the 4-stage dedup** — the 2-stage dedup (email + name only) misses website/LinkedIn duplicates

## Source Priority Reference

From `matching/enrichment/constants.py`:

| Source | Priority | Notes |
|--------|----------|-------|
| client_confirmed | 100 | Client-verified data — never overwrite |
| client_ingest | 90 | Initial client import |
| manual_edit | 80 | Admin manual edits |
| csv_import | 60 | Bulk CSV imports |
| exa_research | 50 | Exa API enrichment |
| ai_research | 40 | Claude/AI enrichment |
| apollo | 30 | Apollo API data |
| scraper_* | 20 | All scrapers (lowest) |
| unknown | 0 | Fallback |

## Files Reference

| File | Purpose |
|------|---------|
| **Pipeline A (SQL bulk import)** | |
| `Filling Database/supabase/transform_for_import.py` | CSV transformation |
| `Filling Database/supabase/01_create_staging_tables.sql` | Create staging table |
| `Filling Database/supabase/06_merge_to_profiles.sql` | Merge with 4-stage dedup |
| `Filling Database/supabase/tag_enrichment_priority.py` | Priority tagging |
| `Filling Database/merge_all_contacts.py` | Merge/dedup all source CSVs |
| **Pipeline B (JV sourcing framework)** | |
| `scripts/sourcing/dedup_merge.py` | Merge all scraper CSVs → MERGED_ALL.csv |
| `scripts/sourcing/filter_jv.py` | Filter to JV-relevant contacts → JV_FILTERED.csv |
| `scripts/sourcing/import_csv.py` | Direct psycopg2 import (bypasses SQL staging) |
| `scripts/sourcing/base.py` | BaseScraper ABC + ScrapedContact dataclass |
| `scripts/sourcing/runner.py` | CLI orchestrator for running scrapers |
| `scripts/sourcing/scrapers/*.py` | Individual scraper modules |
| **Shared** | |
| `matching/enrichment/flows/contact_ingestion.py` | Python ingestion (reference implementation) |
| `matching/enrichment/constants.py` | Source priorities |
| `scripts/automated_enrichment_pipeline_safe.py` | Enrichment pipeline |

## Quick Start (TL;DR)

### Option A: Import JV-filtered sourcing contacts (~35K JV-relevant)

```bash
# 1. Merge scraper outputs + filter for JV relevance
python3 scripts/sourcing/dedup_merge.py
python3 scripts/sourcing/filter_jv.py

# 2. Import directly to Supabase (deduplicates against existing profiles)
venv/bin/python scripts/sourcing/import_csv.py

# 3. Tag priorities
python3 "Filling Database/supabase/tag_enrichment_priority.py"

# 4. Run enrichment on high-value contacts
python3 scripts/automated_enrichment_pipeline_safe.py --batch-size 10 --limit 10
```

### Option B: Import partnership pipeline bulk contacts (1M+)

```bash
# 1. Transform CSV
python3 "Filling Database/supabase/transform_for_import.py"

# 2. Load into staging + merge to profiles
psql $DATABASE_URL -f "Filling Database/supabase/01_create_staging_tables.sql"
psql $DATABASE_URL -c "\COPY staging_contacts(name,email,company,website,linkedin,phone,bio,source,source_url) FROM 'Filling Database/MASTER_JV_CONTACTS_IMPORT.csv' CSV HEADER"
psql $DATABASE_URL -f "Filling Database/supabase/06_merge_to_profiles.sql"

# 3. Tag priorities
python3 "Filling Database/supabase/tag_enrichment_priority.py"

# 4. Run enrichment on high-value contacts
python3 scripts/automated_enrichment_pipeline_safe.py --batch-size 10 --limit 10

# 5. Verify
psql $DATABASE_URL -c "SELECT COUNT(*) FROM profiles"
```
