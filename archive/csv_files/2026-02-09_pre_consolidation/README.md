# CSV Archive - Pre-Consolidation (2026-02-09)

This archive contains CSV files from the enrichment process **before** the confidence scoring consolidation was implemented.

## Archive Date
2026-02-09

## What Was Archived
These files were intermediate/temporary outputs from the initial enrichment phases:
- Matched coach lists (various versions)
- LinkedIn matches
- JV Directory exports
- Conference enrichment outputs
- Client-specific matches

## Why Archived
Phase 2 of the consolidation pipeline:
- These files served their purpose during initial enrichment
- All data has been consolidated into Supabase with confidence tracking
- Keeping them for historical reference and potential audit needs

## What Changed After This Archive
✅ **Phase 0**: Cleaned Supabase database (merged 3 duplicates, normalized 330 URLs)
✅ **Phase 1**: Added robust confidence scoring with age decay
✅ **Consolidation**: 731 profiles updated with enrichment metadata

## Production Database State
After consolidation:
- **enrichment_metadata**: Field-level tracking (source, confidence, dates)
- **profile_confidence**: Overall quality scores (0.0-1.0)
- **last_enriched_at**: Freshness timestamps
- **Age decay**: Confidence decreases over time per field type

## If You Need This Data
All enriched data is now in Supabase with better tracking. Query examples:

```sql
-- Find high-confidence profiles
SELECT name, email, profile_confidence
FROM profiles
WHERE profile_confidence > 0.8
ORDER BY profile_confidence DESC;

-- Find profiles needing re-enrichment
SELECT name, email, last_enriched_at
FROM profiles
WHERE last_enriched_at < (NOW() - INTERVAL '90 days')
  OR profile_confidence < 0.5;
```

## Files Archived

### Matched Coaches (101M total)
- `matched_coaches_full.csv` (34M) - Full coach matches
- `matched_coaches_enhanced.csv` (32M) - Enhanced version
- `matched_coaches_v2.csv` (27M) - Version 2
- `matched_linkedin_contacts.csv` (9.7M) - LinkedIn matches
- `matched_linkedin_v2.csv` (8.6M) - LinkedIn v2

### JV Directory (1.4M total)
- `jv_directory_full_with_contacts.csv` (1.0M)
- `jv_directory_with_emails.csv` (436K)

### OWL Enrichment (3.6M total)
- `owl_enrichment_output/owl_enriched_profiles.csv` (3.0M) - **PRIMARY OWL DATA**
- `Chelsea_clients/conference_enriched/owl_enriched_profiles.csv` (288K)

### Client Files (500K total)
- `Chelsea_clients/conference_attendees.csv` (228K)
- `Chelsea_clients/janet_final.csv` (122K)
- `Chelsea_clients/penelope_matches*.csv` (~76K)

### Other (700K total)
- `publisher_audiobook_opportunities*.csv` (~210K)
- `contacts_complete_v*.csv` (~220K) - Multiple versions

## DO NOT DELETE
Keep this archive for:
- Historical reference
- Audit compliance
- Rollback capability (if needed)
- Understanding enrichment evolution

## Next Steps
If you need to work with enrichment data, use:
- Supabase database (production data with confidence tracking)
- `python manage.py consolidate_enrichment` for new enrichments
- SQL queries for high-confidence profiles
