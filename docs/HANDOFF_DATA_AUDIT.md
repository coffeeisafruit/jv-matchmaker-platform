# Handoff: Data Utilization Audit — Ready for Implementation

## What Was Done

A comprehensive data utilization audit was completed across the entire JV Matchmaker Platform codebase (5 Django apps, 22 models, 200+ fields). The audit traced every field from where it's written to where it's read, identified what the ISMC scoring engine actually uses vs ignores, and produced a prioritized list of improvements.

**The full audit document is at:** `docs/DATA_UTILIZATION_AUDIT.md`

Read that file first — it's the source of truth. Everything below is a summary to orient you.

---

## Critical Findings to Verify

Before implementing anything, verify these claims by reading the referenced files:

### 1. Enrichment data is disconnected from scoring

- **Enrichment pipeline** (`matching/enrichment/ai_research.py`, `smart_enrichment_service.py`) collects: `seeking`, `offering`, `who_you_serve`, `what_you_do`, `niche`
- **ISMC scoring engine** (`matching/services.py`, lines 91-500) never reads any of those fields. It only checks:
  - Does `linkedin_url` exist? (line 123)
  - Does `email` exist? (line 147)
  - Does `website_url` exist? (line 135)
  - What's the `audience_size` category? (line 213)
  - How many days since `updated_at`? (line 314)
- **Verify by:** Reading `matching/services.py` methods `calculate_intent_score()`, `calculate_synergy_score()`, `calculate_momentum_score()`, `calculate_context_score()` — search for any reference to `seeking`, `offering`, `who_you_serve`, `what_you_do`, or `niche`. You won't find any.

### 2. Ghost fields exist in Supabase but not in the Django model

- `matching/management/commands/compute_network_centrality.py` writes `pagerank_score`, `degree_centrality`, `betweenness_centrality`, `network_role`, `centrality_updated_at` to SupabaseProfile via `save(update_fields=[...])`
- `matching/management/commands/compute_recommendation_pressure.py` writes `recommendation_pressure_30d`, `pressure_updated_at`
- `matching/management/commands/consolidate_enrichment.py` writes `enrichment_metadata`, `profile_confidence`, `last_enriched_at` via raw SQL
- **None of these fields are declared in** `matching/models.py` SupabaseProfile (lines 11-54)
- Since `managed = False`, Django doesn't error — but the ORM can't read these fields back
- **Verify by:** Comparing the field list in `SupabaseProfile` (models.py:11-54) against the `save(update_fields=[...])` calls in the commands above

### 3. Confidence scoring is built but unused

- `matching/enrichment/confidence/confidence_scorer.py` implements source reliability (0.3-1.0), exponential age decay, verification boost, cross-validation boost
- This feeds into `consolidate_enrichment.py` which writes `profile_confidence` and `enrichment_metadata` to Supabase
- But nothing in `matching/services.py` reads `profile_confidence` or `enrichment_metadata`
- **Verify by:** Grep for `profile_confidence` or `enrichment_metadata` in `matching/services.py` — not found

### 4. MatchFeedback is collected but never feeds back

- `matching/models.py:304-342` — `MatchFeedback` has `rating` (1-5) and `outcome` (successful/unsuccessful)
- Nothing in the codebase reads these to adjust future scoring
- **Verify by:** Grep for `MatchFeedback` outside of models.py and migrations — it's only referenced in model definition

---

## How This Improves the Codebase

The audit identified 8 prioritized improvements. Here's the recommended implementation order:

### Priority 1 (Quick Win): Add ghost fields to SupabaseProfile model
- **File:** `matching/models.py` — add 10 field declarations to SupabaseProfile
- **Why:** Since `managed = False`, no migrations are needed. Just declaring the fields lets the ORM read network metrics, confidence data, and enrichment metadata that already exist in the database.
- **Fields to add:** `pagerank_score` (FloatField), `degree_centrality` (FloatField), `betweenness_centrality` (FloatField), `network_role` (TextField), `centrality_updated_at` (DateTimeField), `recommendation_pressure_30d` (IntegerField), `pressure_updated_at` (DateTimeField), `enrichment_metadata` (JSONField), `profile_confidence` (FloatField), `last_enriched_at` (DateTimeField)
- **Risk:** Low. Unmanaged model — Django won't touch the table schema. If a field doesn't exist in the actual DB, queries filtering on it will error, but `.only()` and `.defer()` can handle that.

### Priority 2 (Highest ROI): Wire enrichment data into ISMC scoring
- **File:** `matching/services.py` — modify all 4 `calculate_*_score()` methods
- **Why:** This is the core finding. The platform spends money on AI enrichment but doesn't use the results for ranking.
- **What to change:**
  - `calculate_intent_score()`: Add factor for `seeking` content analysis (does it mention partnerships/JVs/affiliates?)
  - `calculate_synergy_score()`: Add NLP/keyword overlap between partner's `who_you_serve`/`offering`/`niche` and the user's ICP `industry`/`pain_points`
  - `calculate_momentum_score()`: Use `last_active_at`, `social_reach`, `current_projects`
  - `calculate_context_score()`: Use `profile_confidence`, `trust_level`, network centrality
- **Consideration:** The scoring service currently takes a `Profile` (Django-managed), not a `SupabaseProfile`. You may need to either: (a) adapt scoring to accept SupabaseProfile, (b) bridge Profile ↔ SupabaseProfile with a FK, or (c) pass enrichment data via the existing `enrichment_data` JSON field on Profile.

### Priority 3: Confidence-weighted scoring
- **File:** `matching/services.py` — after computing final ISMC score, apply confidence modifier
- **Depends on:** Priority 1 (ghost fields accessible)
- **Logic:** `adjusted_score = final_score * confidence_weight(profile_confidence)` where low-confidence profiles get penalized

### Priority 4: Feedback loop
- **Files:** `matching/services.py`, potentially a new management command
- **What:** Use `MatchFeedback.rating` and `outcome` to adjust scoring weights over time
- **Long-term:** This is the path to ML-based scoring calibration

### Priority 5: Network centrality in scoring
- **File:** `matching/services.py` — add network position to Context scoring
- **Depends on:** Priority 1 (ghost fields accessible)
- **Logic:** Partners with high `pagerank_score` or `network_role = 'hub'` get a Context boost

---

## Key Files Map

| File | What It Does | Lines of Interest |
|------|-------------|-------------------|
| `matching/models.py` | All matching models | 11-54 (SupabaseProfile), 75-121 (SupabaseMatch), 128-187 (Profile), 190-301 (Match), 304-342 (MatchFeedback) |
| `matching/services.py` | ISMC scoring + PartnershipAnalyzer | 91-171 (Intent), 195-274 (Synergy), 297-374 (Momentum), 397-478 (Context), 672-1019 (PartnershipAnalyzer) |
| `matching/enrichment/ai_research.py` | Website scraping + AI extraction | 47-79 (research_profile), 125-183 (extraction prompt) |
| `matching/enrichment/smart_enrichment_service.py` | Progressive enrichment orchestrator | 85-231 (enrich_contact), 330-366 (missing field detection) |
| `matching/enrichment/confidence/confidence_scorer.py` | Confidence scoring with decay | 28-37 (source base confidence), 41-54 (field decay rates), 56-110 (calculate_confidence) |
| `matching/enrichment/match_enrichment.py` | Match enrichment + 6 verification agents | 211-310 (MatchEnrichmentService), 1035-1107 (MatchVerificationAgent) |
| `matching/management/commands/consolidate_enrichment.py` | Writes enrichment to Supabase | 216-278 (merge logic), 312-354 (SQL generation) |
| `matching/management/commands/compute_network_centrality.py` | Graph metrics via NetworkX | 54-71 (graph building), 154-188 (profile updating) |
| `matching/management/commands/compute_recommendation_pressure.py` | Partner fatigue prevention | 48-52 (pressure aggregation), 114-135 (profile updating) |
| `positioning/models.py` | ICP, Transformation, PainSignal | 4-97 (ICP), 133-162 (PainSignal — island table) |
| `outreach/models.py` | Email, PVP, campaigns, sequences | 107-175 (SentEmail — has unused engagement signals) |
| `docs/DATA_UTILIZATION_AUDIT.md` | The full audit document | All 7 sections with field-by-field tables |

---

## What NOT to Do

- **Don't restructure models.py broadly** — the `managed = False` pattern for Supabase tables is correct and should stay
- **Don't add Django migrations for SupabaseProfile** — it's unmanaged; just add field declarations
- **Don't remove the PartnershipAnalyzer** — it correctly uses enrichment data for display-time insights; the problem is that this same logic doesn't inform *ranking*
- **Don't break the existing enrichment pipeline** — it works well; the issue is downstream consumption, not data collection
- **Don't modify confidence_scorer.py** — it's well-built; it just needs to be *called* from the scoring service

---

## Architecture Context

- **Django 5.x** with a single PostgreSQL database (Supabase-hosted)
- **No database routers** — all models hit the same DB
- **Two parallel partner representations:** `Profile` (Django-managed, used by ISMC scoring) and `SupabaseProfile` (unmanaged, used by PartnershipAnalyzer and enrichment). They are NOT linked.
- **AI via OpenRouter/Anthropic API** (Claude Sonnet) for enrichment extraction
- **HTMX + Alpine.js + Tailwind** frontend
- See `CLAUDE.md` for full project conventions
