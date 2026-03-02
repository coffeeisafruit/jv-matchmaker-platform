# Enrichment Pipeline Fix: Per-Field Confidence Writes

**Date:** 2026-03-01
**Scope:** Structural fix so future Exa enrichment runs write confidence data correctly

---

## Problem

Two enrichment pipelines evolved independently and wrote different `enrichment_metadata` shapes:

- **OWL pipeline** → top-level: `meta['seeking'] = {'confidence': 0.85, 'source': 'owl', ...}`
- **Exa pipeline** → nested: `meta['field_meta']['seeking'] = {'source': 'exa_research', ...}`
- **Scorer** → reads only top-level format

Result: 3,232 Exa-enriched profiles got a 0/10 penalty on Enrichment Confidence (Factor 5)
despite having valid enrichment data.

---

## Files Changed

### 1. `matching/enrichment/confidence/confidence_scorer.py`

**Change:** Added pipeline source names to `SOURCE_BASE_CONFIDENCE`

**Before:**
```python
SOURCE_BASE_CONFIDENCE = {
    'manual': 1.0,
    'apollo_verified': 0.95,
    'owl': 0.85,
    'apollo': 0.80,
    'website_scraped': 0.70,
    'linkedin_scraped': 0.65,
    'email_domain_inferred': 0.50,
    'unknown': 0.30,
}
```

**After:**
```python
SOURCE_BASE_CONFIDENCE = {
    'manual': 1.0,
    'client_ingest': 1.0,
    'client_confirmed': 1.0,
    'apollo_verified': 0.95,
    'owl': 0.85,
    'apollo': 0.80,
    'exa_research': 0.75,
    'exa_pipeline': 0.75,
    'ai_research': 0.75,
    'website_scraped': 0.70,
    'website_scrape': 0.70,
    'web_discovery': 0.70,
    'linkedin_scraped': 0.65,
    'ai_inference': 0.50,
    'email_domain_inferred': 0.50,
    'unknown': 0.30,
}
```

**Why:** Previously, pipeline source names like `exa_research` fell through to `unknown` (0.30),
drastically undervaluing real enrichment data. The new entries give correct base confidence
levels: Exa/AI research at 0.75 (between Apollo's 0.80 and website scraping's 0.70).

---

### 2. `scripts/automated_enrichment_pipeline_safe.py`

**Change:** Added per-field confidence writes and `profile_confidence` calculation after
the Apollo cascade override block (after line 2027).

**Added code (after Apollo cascade, before JSONB merge):**
```python
# -- Per-field confidence scores --
for f in fields_written:
    f_source = field_meta_update.get(f, {}).get('source', enrichment_source)
    field_conf = scorer.calculate_confidence(f, f_source, enriched_at)
    meta_payload[f] = {
        'confidence': round(field_conf, 4),
        'source': f_source,
        'enriched_at': enriched_at.isoformat(),
    }

# ... (existing JSONB merge) ...

# Update profile_confidence from per-field weighted average
profile_conf = scorer.calculate_profile_confidence(meta_payload)
if profile_conf > 0:
    set_parts.append(sql.SQL("profile_confidence = %s"))
    params.append(round(profile_conf, 4))
```

**Why:** The `scorer` (`ConfidenceScorer`) was already instantiated at line 1673 for
email confidence. This extends its use to ALL enriched fields, writing top-level entries
that `calculate_profile_confidence()` can aggregate.

The `field_meta` writes are preserved — both formats now coexist:
- `meta_payload['seeking']` = `{'confidence': 0.75, 'source': 'exa_research', ...}` (scorer reads this)
- `meta_payload['field_meta']['seeking']` = `{'source': 'exa_research', 'updated_at': ...}` (provenance)

---

### 3. `matching/enrichment/apollo_enrichment.py`

**Change:** Added per-field confidence writes in `build_apollo_update()` function,
after `field_meta` is updated (after line 693).

**Added code:**
```python
# Per-field confidence entries at top level
from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer
scorer = ConfidenceScorer()
enriched_at = datetime.fromisoformat(now_iso)
for f in fields_written:
    if f not in meta or not isinstance(meta.get(f), dict) or 'confidence' not in meta.get(f, {}):
        field_conf = scorer.calculate_confidence(f, APOLLO_SOURCE, enriched_at)
        meta[f] = {
            'confidence': round(field_conf, 4),
            'source': APOLLO_SOURCE,
            'enriched_at': now_iso,
        }
```

**Why:** Apollo enrichment is called as a cascade from the main Exa pipeline and also
independently. This ensures Apollo-sourced fields get confidence entries regardless of
the entry point.

Note: existing top-level confidence entries are NOT overwritten (the `if` guard checks
for existing entries). This preserves OWL or client-provided confidence data.

---

### 4. `matching/enrichment/flows/consolidation_task.py` — No changes needed

This file already had per-field confidence code (lines 648-661) and `profile_confidence`
calculation (lines 762-810), added in commit `90c8d09` on Feb 28. However, it was using
source names like `exa_research` which previously fell through to `unknown` (0.30).

**Fixed by:** The `SOURCE_BASE_CONFIDENCE` update in `confidence_scorer.py` — `exa_research`
now resolves to 0.75 instead of 0.30.

---

## Pipeline Test: 1-Profile Simulation

**Profile:** Mental Health, Natural Health

**Result:** Both formats coexist correctly:

```
Top-level:    meta["bio"] = {"confidence": 0.75, "source": "ai_research", "enriched_at": "2026-03-01T..."}
Nested:       meta["field_meta"]["bio"] = {"source": "ai_research", "updated_at": "2026-03-01T...", "pipeline_version": 2}
```

- `profile_confidence`: 0.75 (fresh data, no age decay)
- 18 top-level confidence entries written
- `field_meta` preserved with all 18 entries

**Future enrichment confidence vs backfilled:**
- Fresh enrichment: ~0.75 (no age decay)
- Backfilled (11-day-old data): ~0.60 (with age decay)

---

## Confirmation

The NEXT time the Exa pipeline runs, it will:

1. Write per-field confidence entries at top level (via new code in `automated_enrichment_pipeline_safe.py`)
2. Use correct base confidence of 0.75 for `exa_research` source (via updated `SOURCE_BASE_CONFIDENCE`)
3. Compute and save `profile_confidence` as a weighted average of all fields
4. Preserve `field_meta` for backward compatibility

No additional backfill will be needed after future runs.

---

## Files Not Changed (and Why)

| File | Reason |
|------|--------|
| `scripts/run_apollo_sweep.py` | Uses `apollo_enrichment.py` which is now fixed |
| `scripts/import_apollo_csv.py` | Archive/import script, runs infrequently; can be updated if needed |
| `matching/services.py` | Scoring code (read-only for this fix); no changes needed |
| `matching/models.py` | `profile_confidence` field already exists |
