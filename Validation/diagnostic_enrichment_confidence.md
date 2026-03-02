# Diagnostic: Why 3,232 Profiles Have profile_confidence = 0.0

**Generated:** 2026-02-28
**Scope:** Research only — no code changes made

---

## Executive Summary

**Root Cause:** The per-field confidence scoring code was added to `consolidation_task.py` on
**Feb 28, 2026** (commit `90c8d09`), but the last Exa enrichment run completed on **Feb 27**.
The code literally did not exist when the enrichment pipeline last ran.

The 345 profiles with non-zero confidence were enriched by the **OWL deep-research pipeline**
on Feb 10, 2026 — a separate, older code path that already had confidence scoring.

---

## 1. The Write/Read Architecture

### What `_score_context()` reads (Factor 5)

```
services.py:2400  →  target.profile_confidence  →  FloatField on SupabaseProfile
```

This is a single float (0.0–1.0) representing overall data quality.

### What should write it

```
consolidation_task.py:762-810  →  ConfidenceScorer.calculate_profile_confidence(meta)
                                  →  Weighted average of per-field confidence entries
                                  →  Written via SQL: SET profile_confidence = %s
```

### The per-field confidence entries (prerequisite)

`calculate_profile_confidence()` reads **top-level keys** in `enrichment_metadata` that
have a `{'confidence': float}` structure:

```json
{
  "offering": {"confidence": 0.85, "source": "owl", "enriched_at": "2026-02-10T..."},
  "seeking":  {"confidence": 0.85, "source": "owl", "enriched_at": "2026-02-10T..."},
  "field_meta": { ... }   ← ignored (no 'confidence' key)
}
```

These per-field entries are written by `consolidation_task.py:648-661`, which was also
added on Feb 28.

---

## 2. The Timing Mismatch

| Event | Date | Confidence Code? |
|-------|------|-------------------|
| OWL enrichment + `consolidate_enrichment` command | Feb 10 | YES — the command called `calculate_profile_confidence()` and wrote per-field entries |
| Exa enrichment run (automated pipeline) | Feb 17-27 | NO — old code only set `profile_confidence` in the email batch |
| Per-field confidence code added to `consolidation_task.py` | Feb 28 | YES — but no enrichment run yet |

### What the old code did (pre-Feb 28)

The **email batch update** in `automated_enrichment_pipeline_safe.py:2045-2057` was the
*only* path that wrote `profile_confidence` during Exa enrichment:

```sql
UPDATE profiles
SET email = %s,
    enrichment_metadata = jsonb_set(..., '{email}', %s::jsonb),
    profile_confidence = %s,   ← email_confidence only
    last_enriched_at = %s
WHERE id = %s
```

This set `profile_confidence = email_confidence` (source base score for the email source,
e.g. 0.70 for `website_scraped`). But only for profiles that received an email update.

### What the Exa pipeline did NOT do

- Did NOT write per-field confidence entries for non-email fields
- Did NOT compute weighted-average `profile_confidence` from all enriched fields
- Did NOT call `ConfidenceScorer.calculate_profile_confidence()`

### Why 3,232 profiles have 0.0

These profiles were enriched by the Exa pipeline (Feb 17-27) using the old code.
The old code either:
- Set `profile_confidence` via the email batch (if the profile got an email), or
- Left `profile_confidence` at 0.0 (the DB column default)

Since the Exa pipeline wrote enrichment data via JSONB `||` merge
(`enrichment_metadata = COALESCE(...) || %s::jsonb`), it overwrote top-level keys but
preserved any OWL-sourced per-field confidence entries at different top-level keys.

---

## 3. The 345 Non-Zero Profiles

### Breakdown

| Group | Count | `profile_confidence` | Source | Mechanism |
|-------|-------|---------------------|--------|-----------|
| OWL-enriched, with email | 206 | 0.85 | `owl` | `consolidate_enrichment` command (Feb 10) computed weighted avg |
| OWL-enriched, no email | 135 | 0.85 | `owl` | Same — OWL wrote per-field entries, command computed avg |
| Website-scraped email only | 4 | 0.70 | `website_scraped` | Email batch set `profile_confidence = email_confidence` |

### Why all OWL profiles show exactly 0.85

The `consolidate_enrichment` command (archived at
`matching/management/commands/_archive/consolidate_enrichment.py`) ran on Feb 10 after
OWL research completed. It:

1. Wrote per-field confidence entries with `source: 'owl'` and `confidence: 0.85`
   (OWL base confidence from `SOURCE_BASE_CONFIDENCE`)
2. Called `scorer.calculate_profile_confidence(merged_metadata)` → weighted average
3. Saved the result to `profile_confidence`

Since all OWL entries have the same base confidence (0.85) and were freshly enriched
(0 days age decay = 1.0 factor), the weighted average comes out to exactly 0.85.

### Per-field entries survived Exa overwrites

The Exa pipeline uses JSONB `||` (shallow top-level merge). OWL per-field entries like
`meta['offering'] = {'confidence': 0.85, ...}` live at different top-level keys than Exa's
`meta['field_meta'] = {...}`, so they survived the merge.

---

## 4. The Disconnect Map

```
                          ┌──────────────────────────┐
                          │    _score_context()       │
                          │    reads: target.         │
                          │    profile_confidence     │
                          └──────────┬───────────────┘
                                     │
                          ┌──────────▼───────────────┐
                          │  profile_confidence       │
                          │  (float on DB)            │
                          └──────────┬───────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
    ┌─────────▼──────────┐ ┌────────▼─────────┐ ┌─────────▼──────────┐
    │ consolidate_        │ │ consolidation_   │ │ email batch        │
    │ enrichment          │ │ task.py          │ │ (pipeline_safe.py) │
    │ (archived, Feb 10)  │ │ (Feb 28 code)   │ │                    │
    │                     │ │                  │ │ Writes: single     │
    │ ✅ Wrote per-field  │ │ ✅ Writes per-   │ │ email confidence   │
    │ entries + weighted  │ │ field entries +  │ │ to profile_conf.   │
    │ average             │ │ weighted avg     │ │                    │
    │                     │ │                  │ │ ⚠️ No weighted avg │
    │ 345 profiles        │ │ 0 profiles       │ │ of all fields      │
    │ (ran once)          │ │ (never ran)      │ │                    │
    └────────────────────┘ └──────────────────┘ └────────────────────┘
```

---

## 5. Enrichment Metadata Structure (Exa vs OWL)

### Exa-enriched profile (typical, confidence = 0.0)

```json
{
  "tier": 0,
  "field_meta": {
    "niche": {"source": "exa_research", "enriched_at": "2026-02-25"},
    "seeking": {"source": "exa_research", "enriched_at": "2026-02-25"},
    "offering": {"source": "exa_research", "enriched_at": "2026-02-25"}
  },
  "enriched_at": "2026-02-25T...",
  "last_enrichment": "exa_pipeline"
}
```

Note: Per-field entries are nested inside `field_meta`, NOT at top level.
`calculate_profile_confidence()` only reads **top-level** keys with `{'confidence': ...}`.

### OWL-enriched profile (confidence = 0.85)

```json
{
  "tier": 0,
  "field_meta": { ... },
  "enriched_at": "2026-02-10T...",
  "last_enrichment": "exa_pipeline",
  "offering": {
    "confidence": 0.85,
    "source": "owl",
    "enriched_at": "2026-02-10T..."
  },
  "seeking": {
    "confidence": 0.85,
    "source": "owl",
    "enriched_at": "2026-02-10T..."
  }
}
```

Note: Per-field confidence entries are at **top level** — exactly where
`calculate_profile_confidence()` looks.

---

## 6. Bridge Recommendations

### Option A: Re-run enrichment pipeline (comprehensive)

Run the enrichment pipeline with the new code (Feb 28+). The updated
`consolidation_task.py` will:
1. Write per-field confidence entries at top level (lines 648-661)
2. Compute `profile_confidence` as weighted average (lines 762-810)

**Pro:** Full data refresh with latest Exa results.
**Con:** API costs, time for 3,232 profiles.

### Option B: Backfill confidence from existing data (fast)

Write a one-time management command that:
1. Reads each profile's `enrichment_metadata.field_meta`
2. Creates top-level per-field confidence entries from the existing `field_meta` data
   (using `ConfidenceScorer.calculate_confidence()` with source and enriched_at from field_meta)
3. Computes `profile_confidence` via `calculate_profile_confidence()`
4. Saves both to DB

**Pro:** No API calls, runs in seconds, uses data already in the DB.
**Con:** Confidence scores will be based on Exa enrichment dates (Feb 17-27).

### Option C: Batch-update profile_confidence only (minimal)

Run `calculate_profile_confidence()` against existing `enrichment_metadata` for all
profiles. For Exa profiles this will return 0.0 (no top-level confidence entries),
but at least validates the pipeline end-to-end.

Then run the pipeline on 1 profile to confirm new code writes per-field entries correctly.

**Pro:** Validates the pipeline; lowest risk.
**Con:** Doesn't fix the 3,232 profiles until a full re-enrichment.

### Recommended: Option B

The `field_meta` data is already in the DB with source and enrichment dates. A simple
backfill converts it into the format `calculate_profile_confidence()` expects.

---

## 7. Impact on ISMC Scoring

### Current state (3,232 profiles with confidence = 0.0)

Factor 5 is **null-aware** — it only fires when `isinstance(confidence, (int, float))`
is true. Since `0.0` IS a valid float, Factor 5 fires with `conf_score = 0.0`,
creating a **0/10 penalty** for these profiles.

However, this penalty is diluted by the weighted average with Factors 1-4 and 6.
Factor 5 has weight 2.0 out of a theoretical max weight of 13.5, so its impact is ~15%.

### After backfill

With realistic confidence scores (likely 0.65-0.85 based on Exa source + age decay),
Factor 5 would contribute 6.5-8.5 points. This would increase Context dimension scores
by approximately 1.0-1.3 points, which flows through to the harmonic mean.

### Scoring asymmetry

Since `_score_context(target)` only evaluates the target profile, backfilling creates
symmetric improvement: every profile benefits equally whether it's the source or target
in a match pair.
