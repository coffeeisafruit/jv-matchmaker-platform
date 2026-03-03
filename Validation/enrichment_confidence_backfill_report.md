# Enrichment Confidence Backfill Report

**Executed:** 2026-03-01
**Command:** `python manage.py backfill_confidence`
**Script:** `matching/management/commands/backfill_confidence.py`

---

## What Was Done

Converted nested `field_meta` entries from the Exa enrichment pipeline into
top-level per-field confidence entries that `ConfidenceScorer.calculate_profile_confidence()`
can read. Then computed and saved the weighted-average `profile_confidence` for each profile.

See `Validation/diagnostic_enrichment_confidence.md` for full root-cause analysis.

---

## Before / After

| Metric | Before | After |
|--------|--------|-------|
| Total members | 3,359 | 3,359 |
| profile_confidence = 0.0 | 3,016 | 514 |
| profile_confidence > 0.0 | 343 | 2,845 |
| Profiles updated | — | 2,845 |
| Skipped (no field_meta) | — | 514 |

### Remaining 514 profiles with 0.0

These profiles have no `field_meta` at all — they were never enriched by any pipeline
that writes provenance data. They need a fresh enrichment pass.

---

## New Confidence Distribution (N=2,845)

| Statistic | Value |
|-----------|-------|
| Min | 0.4105 |
| P25 | 0.5877 |
| Median | 0.6200 |
| Mean | 0.6061 |
| P75 | 0.6524 |
| Max | 0.9668 |
| Stdev | 0.0671 |

### Why scores center around 0.60

The enrichment data is 11 days old (Feb 17-27), and the `website_scraped` base confidence
is 0.70. With exponential age decay:

- `niche` (decay=180d): 0.70 * e^(-11/180) = 0.66
- `offering` (decay=60d): 0.70 * e^(-11/60) = 0.58
- `seeking` (decay=30d): 0.70 * e^(-11/30) = 0.49

The weighted average of these decayed field confidences produces the ~0.60 profile confidence.

---

## Source Mapping

The backfill mapped `field_meta` source names to the closest `SOURCE_BASE_CONFIDENCE` entry:

| field_meta source | Mapped to | Base confidence |
|-------------------|-----------|-----------------|
| exa_pipeline | website_scraped | 0.70 |
| ai_research | website_scraped | 0.70 |
| apollo | apollo | 0.80 |
| ai_inference | email_domain_inferred | 0.50 |
| website_scrape | website_scraped | 0.70 |
| client_ingest | manual | 1.00 |
| web_discovery | website_scraped | 0.70 |
| client_confirmed | manual | 1.00 |

**Note:** After this backfill, we also added the pipeline source names directly to
`ConfidenceScorer.SOURCE_BASE_CONFIDENCE` (see pipeline fix report). Future enrichment
runs will use the native source names without mapping.

---

## Spot Checks

### [1] The Kevin David Experience

| | Before | After |
|---|--------|-------|
| profile_confidence | 0.0 | 0.6054 |
| Top-level confidence entries | none | 9 fields |

Fields added: niche (0.66), company (0.66), seeking (0.49), website (0.66),
linkedin (0.66), offering (0.58), what_you_do (0.62), who_you_serve (0.58),
content_platforms (0.64)

### [2] Accounting Apps | Cloud Accounting

| | Before | After |
|---|--------|-------|
| profile_confidence | 0.0 | 0.6054 |
| Top-level confidence entries | none | 9 fields |

Same field set as [1] — typical Exa-enriched profile.

### [3] The Stock Trading Reality

| | Before | After |
|---|--------|-------|
| profile_confidence | 0.0 | 0.6009 |
| Top-level confidence entries | none | 9 fields |

Includes `revenue_tier` (0.64) — slightly different field mix.

### [4] The Recipe For SEO Success Show

| | Before | After |
|---|--------|-------|
| profile_confidence | 0.0 | 0.6058 |
| Top-level confidence entries | none | 10 fields |

Includes `email` (0.63) — profile had email in `field_meta`.

### [5] Podcast Host, P

| | Before | After |
|---|--------|-------|
| profile_confidence | 0.0 | 0.6088 |
| Top-level confidence entries | none | 10 fields |

Includes both `linkedin` (0.66) and `revenue_tier` (0.64).

---

## Scoring Impact Assessment

### Isolated confidence backfill effect on Context dimension

| Metric | Value |
|--------|-------|
| Mean Context delta | -0.13 |
| Median Context delta | -0.13 |
| Context scores UP | 19.4% |
| Context scores SAME | 2.1% |
| Context scores DOWN | 78.4% |

**Why slightly negative:** Adding Factor 5 with ~6.0/10 score dilutes the existing
Context average (~6.9/10). The factor was previously NULL-skipped; now it fires and
pulls the average down slightly.

### Estimated harmonic mean impact

~-0.013 HM points per match (Context is 10% weight, Factor 5 is ~15% of Context).

**Effectively zero.** The backfill correctly populates the data infrastructure without
meaningfully disrupting existing scores.

### Full rescore (includes ALL changes, not just confidence)

A full rescore of 3,000 random matches showed +2.75 HM mean delta and 24% tier churn.
However, this reflects ALL accumulated profile changes since original scoring (new
enrichment data, updated fields, embedding changes), not just the confidence backfill.

The confidence backfill's isolated contribution is ~0.01 HM points — negligible.

### Tier churn from full rescore

| Transition | Count |
|------------|-------|
| aligned -> strong | 641 |
| strong -> premier | 60 |
| premier -> strong | 17 |
| strong -> aligned | 2 |

**Net direction:** Strongly upward. 701 matches moved up, 19 moved down.
This is primarily from enrichment data improvements, not the confidence backfill.

---

## Data Preservation

- Existing OWL per-field confidence entries were preserved (not overwritten)
- The 343 profiles with existing non-zero confidence were also processed —
  any `field_meta` fields not already at top level were added
- No data was deleted or overwritten
