# HANDOFF: Pre-Supabase Verification Gate + Adaptive Re-Enrichment

> **For a new context window.** This document is self-contained — it has all the codebase context, design decisions, and implementation steps needed to execute without the original conversation.

---

## What We're Building

A centralized verification gate that sits between the enrichment pipeline and Supabase, plus an adaptive retry system that learns from failures. Three new files, five modified files.

**Problem:** The enrichment pipeline writes data to Supabase with minimal validation. AI-extracted `source_quotes` are never checked against actual content. The verification layer silently passes bad data when the API is down (`passed=True, score=100`). There's no retry logic for failed records. 77% of profiles (2,412 of 3,143) still need enrichment.

**Solution:**
1. A 3-layer verification gate (deterministic → source grounding → AI) that catches bad data before DB writes
2. An adaptive retry system that classifies *why* data failed, picks the best alternative enrichment method, and logs outcomes for continuous improvement
3. A unified `run_enrichment.py` command to process everything

---

## Codebase Context (READ THESE FILES FIRST)

### Critical files to understand before coding:

**Enrichment pipeline (the write path):**
- `scripts/automated_enrichment_pipeline_safe.py` — main pipeline, `consolidate_to_supabase_batch()` at **line 441** is where data writes to Supabase via `psycopg2.execute_batch()`
- `scripts/automated_enrichment_pipeline_optimized.py` — same pattern, `consolidate_to_supabase_batch()` at **line 527**
- Both scripts write to `profiles` table: `SET email, enrichment_metadata (JSONB), profile_confidence, last_enriched_at`

**AI extraction (where hallucinations originate):**
- `matching/enrichment/ai_research.py` — `ProfileResearchService._extract_profile_data()` at **line 125** prompts Claude to extract fields with `source_quotes`. Response parsed at **line 232** (`_parse_research_response`) — quotes are logged but never verified. Merge at **line 383** blindly overwrites: `merged = {**existing_data, **researched}`
- `matching/enrichment/ai_verification.py` — `ClaudeVerificationService` with 4 AI checks. **CRITICAL BUG** at **lines 276-284 and 310-318**: `_parse_response()` returns `passed=True, score=100` when API fails or JSON parsing fails.

**Existing utilities to REUSE (do NOT rewrite):**
- `matching/enrichment/match_enrichment.py` **lines 24-160**: `TextSanitizer` class — Unicode fixing, safe truncation, capitalization
- `matching/enrichment/match_enrichment.py` **lines 547-1107**: 6 deterministic verification agents (encoding, formatting, content, capitalization, truncation, data quality)
- `matching/enrichment/confidence/confidence_scorer.py`: `ConfidenceScorer` with source-based confidence, exponential age decay, cross-validation support (built but unused)
- `scripts/assess_data_quality.py` **line 228**: email regex `r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'`
- `scripts/assess_data_quality.py` **lines 231-234**: suspicious email patterns (noreply@, test@, admin@, etc.)
- `scripts/assess_data_quality.py` **line 290**: LinkedIn URL regex
- `scripts/assess_data_quality.py` **line 291**: website URL regex
- `matching/enrichment/__init__.py`: current exports (add new ones here)

**Enrichment methods available for retry (each has different strengths):**
- `website_scrape` — fetches 4 pages (homepage, /contact, /about, /team), regex email extraction. FREE. Email only. 15-25% success.
- `apollo_api` — Apollo.io bulk lookup, verified emails. $0.10/credit. 40-60% success.
- `owl_full` — OWL browser automation, visits 11+ contact page paths, regex + Claude extraction. $0.005. Finds everything. 70-85% success.
- `deep_research` — SimpleDeepResearch, 5 targeted web searches (interviews, podcasts, partnerships). $0.008. Good for seeking/offering. 60-70% success.
- `ai_research` — homepage scrape + Claude extraction. $0.002. Business info only (not email). 60-75% success. Currently fetches homepage only — could be extended to multi-page.
- `linkedin_scrape` — LinkedIn profile fetch. FREE. ~5% success (most profiles block).

---

## Design Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Primary defense layer? | **Layer 1 (deterministic)** | Safe pipeline mostly writes emails from scraping/Apollo — no AI extraction involved. Regex checks are the main defense. |
| AI verification unavailable? | **Fail-cautious** — write with `unverified` status + reduced confidence | Don't quarantine good Apollo emails just because Claude API is down. Reserve quarantine for *detected* problems. |
| Layer 3 (AI verification)? | **Supplementary only**, skipped if L1 or L2 fails | No point paying for AI verification on data we already know is bad. Gate must work with just Layers 1+2. |
| Quarantined records? | **Adaptive retry: Observe → Think → Plan → Execute → Verify → Learn** | Classify why each field failed, select best alternative method, log outcome. Strategy rankings auto-adjust over time. |

---

## Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `matching/enrichment/verification_gate.py` | **CREATE** | Core gate: 3 verification layers + quarantine logic |
| `matching/enrichment/retry_strategy.py` | **CREATE** | Adaptive retry: failure classification, method selection, learning log |
| `scripts/run_enrichment.py` | **CREATE** | Unified runner: enrich new + retry quarantined + refresh stale |
| `matching/enrichment/ai_verification.py` | MODIFY | Fix fail-open → fail-cautious (lines 276-284, 310-318); add `is_available()` |
| `matching/enrichment/ai_research.py` | MODIFY | Return `source_quotes` in metadata (line 257); provenance-tracked merge (line 381) |
| `scripts/automated_enrichment_pipeline_safe.py` | MODIFY | Wire gate into `consolidate_to_supabase_batch()` (line 441); cache raw content |
| `scripts/automated_enrichment_pipeline_optimized.py` | MODIFY | Wire gate into `consolidate_to_supabase_batch()` (line 527); cache raw content |
| `matching/enrichment/__init__.py` | MODIFY | Export new classes |

---

## Architecture

```
                    ┌─────────────────────────┐
                    │  run_enrichment.py       │
                    │  (unified pipeline)      │
                    │                          │
                    │  1. Unenriched profiles  │
                    │  2. Quarantined retries  │
                    │  3. Stale data refresh   │
                    └────────┬────────────────┘
                             │
               Extraction (website / LinkedIn / Apollo)
                             │
                    ┌────────▼────────────────┐
                    │   VERIFICATION GATE      │
                    │                          │
                    │  Layer 1: Deterministic   │  free, instant
                    │  - Email/URL/LinkedIn     │
                    │    regex validation       │
                    │  - Placeholder detection  │
                    │  - Field-swap detection   │
                    │  - Text sanitization      │
                    │  → Auto-fixes applied     │
                    │                          │
                    │  Layer 2: Source Grounding │  free, string match
                    │  - Verify source_quotes   │
                    │    against raw content    │
                    │  - Only for AI-extracted  │
                    │    fields (not emails)    │
                    │                          │
                    │  Layer 3: AI (optional)   │  paid, supplementary
                    │  - Existing Claude verify │
                    │  - Skipped if L1 or L2   │
                    │    fails (save money)     │
                    │  - Fail-cautious on error │
                    └──┬──────────┬────────┬───┘
                       │          │        │
                  ┌────▼──┐ ┌────▼───┐ ┌──▼──────────┐
                  │VERIFIED│ │UNVERIF.│ │QUARANTINED  │
                  │Write + │ │Write + │ │Don't write  │
                  │full    │ │reduced │ │→ Adaptive   │
                  │confid. │ │confid. │ │  retry      │
                  └────────┘ └────────┘ │  (max 2x)   │
                                        └─────────────┘
```

**Three outcomes:**
- **verified** → write to Supabase with full confidence
- **unverified** → write to Supabase with reduced confidence + `verification_status: 'unverified'` flag
- **quarantined** → don't write; adaptive retry classifies failure, selects best alt method, re-verifies, logs outcome; permanently quarantine after 2 retries

---

## Implementation Steps

### Step 1: Create `matching/enrichment/verification_gate.py` — dataclasses + Layer 1

**Dataclasses:**
- `FieldVerdict` — per-field: status (`passed`/`auto_fixed`/`failed`), original_value, fixed_value, issues list, source_verified bool
- `GateVerdict` — overall: status (`verified`/`unverified`/`quarantined`), dict of FieldVerdicts, overall_confidence, provenance dict
- `QuarantineRecord` — original_data, verdict, reason, retry_count, failures list

**`DeterministicChecker` class (Layer 1):**

Reuses existing patterns (copy the regex, don't import — `assess_data_quality.py` is a CLI script, not a library):

| Check | Pattern Source |
|-------|----------------|
| Email format regex | `assess_data_quality.py` line 228 |
| Suspicious email patterns (noreply@, test@, etc.) | `assess_data_quality.py` lines 231-234 |
| LinkedIn URL regex | `assess_data_quality.py` line 290 |
| Website URL regex | `assess_data_quality.py` line 291 |
| LinkedIn-in-website-field swap | `match_enrichment.py` lines 968-977 |
| URL-in-email-field swap | `match_enrichment.py` lines 1022-1030 |
| Placeholder detection ("N/A", "Update", "-") | `match_enrichment.py` lines 1011-1019 |
| Unicode/encoding sanitization | `match_enrichment.py` `TextSanitizer` class lines 24-160 (import this) |
| Truncation detection | `match_enrichment.py` `TruncationVerificationAgent` lines 868-929 |

Auto-fix rules:
- Invalid email → clear to empty string (triggers re-enrichment)
- Missing `https://` on URL → prepend it
- LinkedIn URL in website field → move to linkedin field
- URL in email field → move to website field
- Placeholder values → clear to empty string
- Bad Unicode/encoding → `TextSanitizer.sanitize()`
- Truncated text → `TextSanitizer.truncate_safe()`

### Step 2: Add Layer 2 — Source Quote Verification

**`SourceQuoteVerifier` class.** Only applies to AI-extracted fields (seeking, offering, who_you_serve, what_you_do) — NOT emails from scraping/Apollo.

Takes extracted data + raw website content. For each field with an associated `source_quote`:

1. Normalize both quote and raw content (lowercase, collapse whitespace)
2. Substring match first (fast path)
3. If no exact match, `difflib.SequenceMatcher` with threshold ≥ 0.75
4. Skip quotes under 20 characters (too short)

Outcomes:
- Quote found → `source_verified=True`, confidence unchanged
- Quote not found but field value itself found in raw content → `source_verified=True`, confidence -0.10
- Neither found → `source_verified=False`, confidence -0.30, field flagged

### Step 3: Fix fail-open in `ai_verification.py`

**`_parse_response()` line 276-284** — when `response` is None:
```python
# FROM: passed=True, score=100
# TO:   passed=False, score=0
reasoning="AI verification unavailable — marked unverified (fail-cautious)"
```

**`_parse_response()` lines 310-318** — same change for JSON parse errors.

**Add** `is_available()` method → `return self.api_key is not None`

### Step 4: Wire Layer 3 into the gate + evaluation logic

`VerificationGate.evaluate()`:
1. Run Layer 1 (always)
2. Run Layer 2 if raw_content and extraction_metadata available
3. Run Layer 3 **only if both L1 AND L2 passed** and `ai_verifier.is_available()`
4. Determine status:
   - Critical field (email) has detected problem → `quarantined`
   - All checks passed → `verified`
   - Verification couldn't run but no problems detected → `unverified`

### Step 5: Add provenance tracking to `ai_research.py`

**`_parse_research_response()` (line 257):** Include `source_quotes` and `confidence` in returned dict under `_extraction_metadata` key (currently only logged, never returned).

**`research_and_enrich_profile()` (line 381):** Replace `merged = {**existing_data, **researched}` with provenance-tracked merge. Build `_provenance_log` list: `{field, previous_value, new_value, source, confidence}` for each changed field.

### Step 6: Integrate gate into pipeline scripts

**Both `automated_enrichment_pipeline_safe.py` (line 441) and `automated_enrichment_pipeline_optimized.py` (line 527):**

Modify `consolidate_to_supabase_batch()`:
1. Import `VerificationGate`
2. Run each result through `gate.evaluate()` before `execute_batch`
3. Split: `verified` (full write), `unverified` (write with reduced confidence), `quarantined` (don't write)
4. Write verified + unverified to Supabase. Add `verification_status` to `enrichment_metadata` JSONB
5. Write quarantined to `enrichment_batches/quarantine/quarantine_YYYYMMDD.jsonl`
6. Log: "Gate: X verified, Y unverified, Z quarantined"

**Raw content caching:** Add `self._content_cache = {}` populated during website scraping, keyed by profile_id. Pass to `gate.evaluate()`.

### Step 7: Create `matching/enrichment/retry_strategy.py` — adaptive retry

**`FailureClassifier` class:** Takes a `GateVerdict`, classifies each failed field:
- `source_verified=False` → `hallucination`
- field empty after extraction → `missing_data`
- field has value but wrong format → `format_error`
- email failed regex → `email_invalid`

**`RetryStrategySelector` class:** Maps (failure_type, field) → ordered list of methods:

```python
DEFAULT_STRATEGIES = {
    ('email', 'email_invalid'):    ['apollo_api', 'owl_full', 'deep_research'],
    ('email', 'missing_data'):     ['apollo_api', 'owl_full', 'deep_research'],
    ('seeking', 'hallucination'):  ['deep_research', 'owl_full'],
    ('seeking', 'missing_data'):   ['owl_full', 'deep_research'],
    ('who_you_serve', 'hallucination'): ['ai_research_multipage', 'owl_full'],
    ('who_you_serve', 'missing_data'):  ['owl_full'],
    ('offering', 'missing_data'):  ['owl_full', 'deep_research'],
    ('*', '*'):                    ['owl_full'],  # fallback
}
```

Reads from `LearningLog` to adjust rankings based on accumulated success rates.

**`LearningLog` class:** Append-only JSONL at `enrichment_batches/learning_log.jsonl`:
- `record(failure_type, field, original_method, retry_method, outcome, confidence)`
- `success_rate(field, failure_type, method) → float` — what % of the time does this method resolve this failure?

If log shows `deep_research` resolves `seeking` hallucinations 90% of the time but `owl_full` only 60%, `deep_research` moves to first position for that combination.

### Step 8: Create `scripts/run_enrichment.py` — unified pipeline runner

```bash
python scripts/run_enrichment.py --batch-size 10          # all three passes
python scripts/run_enrichment.py --quarantined-only        # retry failures only
python scripts/run_enrichment.py --stale-only --max-age 90 # refresh stale only
python scripts/run_enrichment.py --dry-run --batch-size 5  # gate verdicts, no writes
```

**Three passes:**

1. **Unenriched profiles** — query Supabase for profiles with no email and no `enrichment_metadata`. Run through safe pipeline. All results go through verification gate.

2. **Quarantined retries (adaptive)** — Observe → Think → Plan → Execute → Verify → Learn:

   **Observe:** Read `GateVerdict` from quarantine record — which fields failed, what method produced bad data.

   **Think:** `FailureClassifier` classifies: `hallucination`, `missing_data`, `format_error`, `stale_content`, `email_invalid`.

   **Plan:** `RetryStrategySelector` picks best untried method for each (field, failure_type). Strategy table:

   | Failed Field | Failure Type | Retry Strategy |
   |---|---|---|
   | email | invalid/missing | Apollo API → OWL (contact pages) → SimpleDeepResearch |
   | seeking | hallucination | SimpleDeepResearch (web searches for interviews/podcasts) |
   | seeking | missing_data | OWL full (browses actual pages) |
   | who_you_serve | hallucination | ai_research multi-page (/about, /services) |
   | who_you_serve | missing_data | OWL full research |
   | offering | missing | OWL (course/podcast/program pages) |
   | multiple fields | any | OWL full ($0.005, most comprehensive) |

   **Execute:** Run the targeted method (not the full pipeline).

   **Verify:** Result goes through verification gate again.

   **Learn:** Record outcome in `learning_log.jsonl`. Over time, strategy rankings auto-adjust.

   **Limits:** Max 2 retry cycles. After 2 → permanently quarantined for manual review.

3. **Stale data refresh** — query Supabase for `confidence_expires_at < now()` or `profile_confidence < 0.5`. Uses existing `ConfidenceScorer.calculate_expires_at()`.

**Stats output:**
```
Enrichment run complete:
  New profiles enriched: 45/50
  Quarantine retries: 8/12 resolved
  Stale refreshed: 15/20
  Currently quarantined: 9
  Gate stats: 68 verified, 5 unverified, 9 quarantined
```

### Step 9: Update `matching/enrichment/__init__.py`

Add `VerificationGate`, `GateVerdict`, `QuarantineRecord`, `RetryStrategySelector`, `LearningLog` to imports and `__all__`.

---

## Quarantine Record Format

`enrichment_batches/quarantine/quarantine_YYYYMMDD.jsonl`:

```json
{
  "profile_id": "abc-123",
  "profile_name": "Jane Smith",
  "quarantined_at": "2026-02-13T14:30:00",
  "reason": "Email failed format validation; source quote not found for 'seeking'",
  "retry_count": 0,
  "max_retries": 2,
  "failures": [
    {"field": "email", "type": "email_invalid", "original_method": "website_scrape"},
    {"field": "seeking", "type": "hallucination", "original_method": "ai_research"}
  ],
  "next_strategies": {
    "email": "apollo_api",
    "seeking": "deep_research"
  },
  "original_data": {"email": "not-an-email", "seeking": "..."},
  "overall_confidence": 0.25
}
```

- `retry_count=0` → `RetryStrategySelector` picks targeted methods, executes, increments to 1
- `retry_count=1` → picks next-best method, increments to 2
- `retry_count=2` → permanently quarantined for manual review
- On success → removed from quarantine, outcome logged, data written as `verified`

---

## Verification

1. **DeterministicChecker tests:** URL in email field, placeholders, bad Unicode → verify auto-fixes
2. **SourceQuoteVerifier tests:** Real quotes vs fabricated quotes against raw content
3. **Fail-cautious test:** Mock API down → verify `passed=False` but record still written as `unverified`
4. **FailureClassifier test:** Various GateVerdicts → verify correct classification
5. **RetryStrategySelector test:** Classified failures → verify method selection; verify learning log adjusts rankings
6. **Gate integration:** `python scripts/run_enrichment.py --dry-run --batch-size 5`
7. **Adaptive retry:** Bad data → quarantine → `--quarantined-only` → verify targeted retry → verify learning_log.jsonl
8. **End-to-end:** `run_enrichment.py` on 10 profiles → inspect Supabase `verification_status` + `learning_log.jsonl`
