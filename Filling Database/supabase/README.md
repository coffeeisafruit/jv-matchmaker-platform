# Supabase Bulk Import Pipeline

This directory contains tools for bulk importing CSV contact data into the Supabase PostgreSQL database.

## Files

### Python Import Script

**`import_all.py`** - Main import pipeline using Django ORM with full deduplication

- Reads CSVs from `Filling Database/partners/` and `Filling Database/chambers/`
- Uses existing `matching/enrichment/flows/contact_ingestion.py` pipeline
- Full deduplication: email, website domain, LinkedIn URL, name+company
- Batch processing in chunks of 500 contacts
- Resume support via `.import_state.json`
- Progress tracking and error handling

### SQL Helper Files

**`01_create_staging_tables.sql`** - Creates staging table for direct SQL import path

- Optional alternative to Python script for very large imports (100K+ rows)
- Provides `staging_contacts` table with indexes
- Use with `COPY FROM` or bulk `INSERT` statements

**`06_merge_to_profiles.sql`** - Merges staging data into profiles table

- Deduplicates by email and name+company (simpler than Python pipeline)
- Run after loading data into `staging_contacts`
- Returns summary stats

## Usage

### Quick Validation (No Django Required)

For quick CSV validation without installing Django:

```bash
# Validate all CSVs
python3 "Filling Database/supabase/validate_csvs.py"

# Validate single file with details
python3 "Filling Database/supabase/validate_csvs.py" --file ../chambers/ak.csv -v
```

This lightweight script checks:
- File exists and is readable
- Required headers present (name)
- Row counts and empty rows
- Missing optional columns

### Python Import (Recommended)

#### Prerequisites

```bash
# Install dependencies
pip3 install django psycopg2-binary python-dotenv --break-system-packages

# Ensure .env has DATABASE_URL
cat .env | grep DATABASE_URL
```

#### Import All CSVs

```bash
python3 "Filling Database/supabase/import_all.py"
```

This will:
- Scan `Filling Database/partners/*.csv` and `Filling Database/chambers/*.csv`
- Import all contacts with full deduplication
- Save progress to `.import_state.json`
- Resume from last checkpoint on subsequent runs

#### Dry Run (Preview Only)

```bash
python3 "Filling Database/supabase/import_all.py" --dry-run
```

#### Import Single File

```bash
python3 "Filling Database/supabase/import_all.py" --file ../partners/sam_gov.csv
```

#### Validate CSVs Without Importing

```bash
python3 "Filling Database/supabase/import_all.py" --skip-ingestion
```

#### Reset Progress and Start Fresh

```bash
python3 "Filling Database/supabase/import_all.py" --reset-state
```

### SQL Import Path (Advanced)

For very large imports where Python overhead is too high:

```bash
# 1. Create staging table
psql $DATABASE_URL -f 01_create_staging_tables.sql

# 2. Load CSV into staging (example)
psql $DATABASE_URL -c "\COPY staging_contacts(name,email,company,website,linkedin,phone,bio,source) FROM 'data.csv' CSV HEADER"

# 3. Merge into profiles with dedup
psql $DATABASE_URL -f 06_merge_to_profiles.sql

# 4. Clean up staging
psql $DATABASE_URL -c "TRUNCATE staging_contacts"
```

## CSV Format

Expected headers (only `name` is required, others are optional):

```
name,email,company,website,linkedin,phone,bio,source,source_url
```

Example:

```csv
name,email,company,website,linkedin,phone,bio,source,source_url
John Doe,john@example.com,ACME Corp,https://acme.com,https://linkedin.com/in/johndoe,555-1234,CEO of ACME,scraper_linkedin,https://linkedin.com/in/johndoe
```

### Handling Missing Columns

The import script gracefully handles missing columns by filling them with empty strings. For example, chamber CSVs may not have `email` or `linkedin` columns:

```csv
name,company,website,phone,city,state,bio,source_url
Cooper Landing Chamber,Cooper Landing Chamber,http://example.com,907-555-1234,Anchorage,AK,"Local chamber",http://example.com
```

This will import successfully with empty `email` and `linkedin` fields.

## Source Names

The import script automatically derives source names from CSV filenames:

- `sam_gov.csv` → source = `scraper_sam_gov`
- `chamber_data.csv` → source = `scraper_chamber_data`
- `Filling Database/partners/example.csv` → source = `scraper_example`

## Deduplication Logic

The Python import uses the full deduplication logic from `contact_ingestion.py`:

1. **Email exact match** (case-insensitive)
2. **Website domain match** (strips protocol, www.)
3. **LinkedIn URL match** (compares path only)
4. **Name + company match** (case-insensitive)

The SQL import path only checks email and name+company for performance.

## Progress Tracking

The script saves progress to `.import_state.json` after each file:

```json
{
  "sam_gov.csv": {
    "total_rows": 1000,
    "new_count": 850,
    "dup_count": 150,
    "error_count": 0,
    "completed": true,
    "errors": []
  }
}
```

If a file is marked `completed: true`, it will be skipped on subsequent runs. Use `--reset-state` to clear this.

## Error Handling

- Batch failures are logged but don't stop the import
- Failed batches are counted in `error_count`
- Error messages are saved to state file
- Subsequent batches continue processing

## Performance

- Batch size: 500 contacts per transaction
- Typical throughput: 50-100 contacts/second (depends on dedup complexity)
- Large files (10K+ rows) take 2-5 minutes each

## Output

Example output:

```
Found 25 CSV file(s) to process

[PROCESSING] sam_gov.csv
  Read 5000 rows
  Batch 1/10 (500 contacts)... ✓ (478 new, 22 dup)
  Batch 2/10 (500 contacts)... ✓ (495 new, 5 dup)
  ...
  Total: 4750 new, 250 duplicates, 0 errors

[SKIP] chamber_ak.csv - already completed

======================================================================
IMPORT SUMMARY
======================================================================
Total files:       25
Files completed:   25
Total rows:        50,000
New profiles:      45,500
Duplicates:        4,500
Errors:            0
======================================================================
```

## Troubleshooting

### ModuleNotFoundError: No module named 'django'

Install Django:

```bash
pip3 install django psycopg2-binary python-dotenv --break-system-packages
```

### Database connection error

Ensure `.env` has valid `DATABASE_URL`:

```bash
echo $DATABASE_URL  # or check .env file
```

### CSV parsing errors

Validate CSV format:

```bash
python3 "Filling Database/supabase/import_all.py" --file yourfile.csv --skip-ingestion
```

### Resume from failure

The script automatically resumes from the last successful file. To force re-import:

```bash
python3 "Filling Database/supabase/import_all.py" --reset-state
```

## Integration with Enrichment Pipeline

After importing contacts:

1. Contacts are created with `status = 'Prospect'`
2. Run enrichment pipeline to research and qualify contacts:

```bash
python3 scripts/automated_enrichment_pipeline_safe.py --batch-size 10
```

3. Enriched contacts move to `Researched` or `Qualified` status
