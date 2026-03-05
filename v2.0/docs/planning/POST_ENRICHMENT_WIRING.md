# Post-Enrichment Wiring: Match Creation, Seeking Inference, Scale Preparation

**Created:** 2026-03-04
**Status:** Phase 1 implemented (Changes 1, 3, 4, 5, 6). Change 2 (seeking inference) deferred — API cost gate.

## Problem

Four gaps between enrichment and match delivery:

1. **No match creation**: Enriched prospects never become `match_suggestions` — `bulk_recalculate_matches()` only updates EXISTING rows. `score_against_all_clients()` in `cross_client_scoring.py` does exactly what we need but was never called from the acquisition pipeline.

2. **Seeking field at 11.5%**: Only 8,191 of 71,072 enriched profiles have `seeking` populated. Inference from `offering`/`what_you_do`/`who_you_serve` would fill most gaps. **Deferred — API cost ~$0.002/profile × 63K profiles = ~$125.**

3. **Enrichment source indistinguishable**: `enrichment_metadata.last_enrichment` always = `"exa_pipeline"`. No way to tell acquisition-triggered enrichment from batch enrichment.

4. **Scale preparation for 5K members**: At 5K clients × 5K enriched profiles = 25M pairs → ~70 hours. Vector pre-filter interface added as stub; activation deferred until after Tier B+C enrichment completes (to avoid duplicate embedding work).

---

## Changes Implemented

### Change 1: Wire match creation into acquisition pipeline
**File:** `matching/enrichment/flows/acquisition_flow.py`

After Step 6 (enrichment), added Step 7: call `score_against_all_clients()` on the enriched profile IDs. High-quality matches (≥64) are flagged for report regeneration via `flag_reports_for_update()`. Added `new_matches_created: int = 0` to `AcquisitionResult`.

### Change 3: Post-enrichment scoring in enrichment_flow
**File:** `matching/enrichment/flows/enrichment_flow.py`

Added `score_against_clients: bool = False` and `enrichment_context: str = "batch"` params. When `score_against_clients=True`, runs `score_against_all_clients()` after consolidation. The acquisition pipeline passes `score_against_clients=True, enrichment_context="acquisition"`.

### Change 4: Enrichment source traceability
**File:** `matching/enrichment/flows/consolidation_task.py`

Added `enrichment_context: str = "batch"` param to `consolidate_to_db()`. The value is stored in `enrichment_metadata.enrichment_context` so every enrichment is traceable. Values: `"batch"`, `"acquisition"`, `"manual"`.

### Change 5: Weekly scoring management command
**File:** `matching/management/commands/score_new_enrichments.py`

New Django management command that scores profiles enriched since a given date against all active clients. Default window: 7 days. Supports `--dry-run` for pair-count estimation.

### Change 6: Vector pre-filter interface (scale preparation)
**File:** `matching/enrichment/flows/cross_client_scoring.py`

Added `pre_filter: str = "none"` and `pre_filter_top_k: int = 200` params. Added `_vector_pre_filter()` stub that raises `NotImplementedError` with a helpful message about running `backfill_embeddings` first. Added pair-count log before scoring.

---

## Deferred Work

### Change 2: Seeking field inference (deferred — API cost gate)
Would infer `seeking` from `offering` / `what_you_do` / `who_you_serve` using Claude.
- New file: `matching/enrichment/flows/seeking_inference.py`
- Add `'ai_inference': 35` to `SOURCE_PRIORITY` in `constants.py`
- Confidence: 0.50 (vs 0.75 for exa_research) — marked as inferred
- Cost: ~$0.002/profile × ~63K profiles without seeking = ~$125
- **Trigger:** When seeking gap becomes a match quality blocker (currently only affects 11.5% coverage)

### Vector pre-filter activation (deferred — after Tier B+C enrichment)
1. Run `python3 manage.py backfill_embeddings` on all enriched profiles
2. Implement `_vector_pre_filter()` in `cross_client_scoring.py` using pgvector `<=>` operator
3. Tune IVFFlat indexes (lists=50 → 100-200 at scale)
4. Flip `pre_filter="vector"` in `score_new_enrichments` and monthly orchestrator

---

## Scale Math

| Members | Enriched/batch | Pairs (no filter) | Time  | Pairs (vector) | Time |
|---------|---------------|-------------------|-------|----------------|------|
| 2,000   | 500           | 1M                | ~3h   | 400K           | ~1h  |
| 2,000   | 5,000         | 10M               | ~28h  | 400K           | ~1h  |
| 5,000   | 5,000         | 25M               | ~70h  | 1M             | ~3h  |
| 5,000   | 50,000        | 250M              | weeks | 1M             | ~3h  |

---

## Key Functions

- `score_against_all_clients()` — `matching/enrichment/flows/cross_client_scoring.py:134`
- `flag_reports_for_update()` — `matching/enrichment/flows/cross_client_scoring.py:264`
- `consolidate_to_db()` — `matching/enrichment/flows/consolidation_task.py:224`
- `enrichment_flow()` — `matching/enrichment/flows/enrichment_flow.py:81`
- `acquisition_flow()` — `matching/enrichment/flows/acquisition_flow.py:178`
- `score_new_enrichments` — `matching/management/commands/score_new_enrichments.py`
