# JV Matchmaker Platform

## Project Overview

Django-based platform for matching joint venture partners. Uses AI enrichment pipelines to research and qualify potential partners from various data sources.

## Key Directories

- `matching/` - Core matching engine and enrichment pipelines
- `matching/enrichment/` - AI-powered research and verification
- `owl_framework/` - External framework for browser automation
- `v2.0/` - Future planning and expansion documentation
- `scripts/` - Automation scripts for batch processing

## Development Commands

```bash
# Run Django server
python manage.py runserver

# Run enrichment pipeline
python scripts/automated_enrichment_pipeline_safe.py --batch-size 10

# Run tests
pytest
```

## AI/LLM Configuration

Currently uses Claude Sonnet via OpenRouter or Anthropic API. Configuration in:
- `matching/enrichment/ai_research.py` - Profile extraction
- `matching/enrichment/ai_verification.py` - Content verification
- `matching/enrichment/deep_research.py` - Multi-source research

Environment variables:
- `OPENROUTER_API_KEY` - Primary (preferred)
- `ANTHROPIC_API_KEY` - Fallback
- `TAVILY_API_KEY` - Web search (paid, limited)

---

## Scaling Triggers

### TRIGGER: Enrichment Volume > 500 profiles/month

**Action Required:** Consider Vast.ai integration for 90%+ cost reduction.

**When to evaluate:**
- Monthly enrichment exceeds 500 profiles
- AI costs exceed $150/month
- Need higher throughput (16+ req/sec)

**Implementation plan:** See `v2.0/docs/planning/VAST_AI_INTEGRATION.md`

**Quick summary:**
- Rent GPU on Vast.ai (~$0.75/hour)
- Run OSS 120B with OpenAI-compatible API
- Drop-in replacement for Claude calls
- Saves $150-400/month at scale

**GPU selection rule (cost-optimized):**
- **Always use RTX 4090 (~$0.30/hr)** for enrichment jobs — not H200/H200 NVL
- Qwen3-30B-A3B fits in 24GB VRAM; 4090 is ~8x cheaper than H200 for same output
- Require: `gpu_ram>=24 disk_space>=80 inet_down>=500 dph_total<0.5`
- H200 ($2.57/hr) confirmed overkill — only use if 4090 unavailable and time-critical

---

## Scoring & Matching Pipeline

Key architectural rules:
- `score_against_all_clients()` in `matching/enrichment/flows/cross_client_scoring.py` is the canonical function for creating `match_suggestions` rows
- `bulk_recalculate_matches()` ONLY updates existing rows — never creates new matches
- `enrichment_metadata.enrichment_context` tracks why a profile was enriched: `"batch"`, `"acquisition"`, `"manual"`
- Weekly scoring job: `python3 manage.py score_new_enrichments`
- Vector pre-filter stub ready in `score_against_all_clients(pre_filter=...)` — activate after embedding backfill

See `v2.0/docs/planning/POST_ENRICHMENT_WIRING.md` for full plan.

---

## Code Style

- Use type hints for function signatures
- Docstrings for public methods
- JSON responses from AI must be validated
- Source citations required for extracted data (anti-hallucination)

## Testing

- Enrichment tests: `pytest matching/tests/`
- Integration tests require API keys in environment

---

## Tier B Mass Enrichment Pipeline — Resume Status

**Last updated:** 2026-03-04

### What This Pipeline Does
Reads batch files from `tmp/enrichment_batches/batch_XXXX.json` (each has 5 profiles with `scraped_text`), extracts structured fields via AI, writes results to `tmp/enrichment_results/batch_XXXX.json`, then pushes to DB via `cat result_file | python3 scripts/enrich_tier_b.py update`.

### Total Batches
- **3,830 batches** (batch_0000 through batch_3829), each with 5 profiles = **19,150 profiles**

### Completed Ranges
- Batches **0000–0232**: Done in prior sessions (~1,165 profiles)
- Batches **2900–3511**: Done or actively processing as of 2026-03-04
  - **511+ result files** confirmed in 2900-3511 range (~2,555 profiles)
  - Subagents were still running at session end — some gaps may have filled

### Still Needs Processing
- **Batches 0233–2899**: Not yet started (13,335 profiles)
- **Batches 3512–3829**: Not yet started (1,590 profiles)
- **Known gaps in 2900-3511** (check if subagents completed these):
  `3073, 3117, 3125-3127, 3136-3137, 3145-3147, 3154-3157, 3176-3177, 3184-3187, 3195-3197, 3206-3207, 3216-3217, 3224-3227, 3246-3247, 3265-3267, 3287, 3295-3297, 3315-3317, 3324-3327, 3343-3347, 3365-3367, 3399-3417, 3420-3427, 3479-3497`

### How to Resume
```bash
# Check current coverage
ls tmp/enrichment_results/batch_*.json | wc -l

# Find gaps in any range (e.g. 3512-3829)
for i in $(seq 3512 3829); do f=$(printf "tmp/enrichment_results/batch_%04d.json" $i); [ ! -f "$f" ] && echo $i; done | head -20

# Push a single result to DB
cat tmp/enrichment_results/batch_XXXX.json | python3 scripts/enrich_tier_b.py update
```

### Enrichment Output Format
Each result file is a JSON array of 5 objects with fields:
`id` (exact from input), `what_you_do`, `who_you_serve`, `seeking`, `offering`, `niche`, `tags` (JSON array like `["a","b"]`), `signature_programs`, `phone`, `company`, `social_proof`, `booking_link`, `revenue_tier` (solo/small_biz/mid_market/high/""), `service_provided`

### Critical Rules
- **tags MUST be JSON arrays** like `["tag1","tag2"]` — comma-separated strings cause Postgres "malformed array literal" errors
- **id must be copied exactly** from the input batch file — never fabricate UUIDs
- Use `""` for empty/missing fields, never null
- `revenue_tier` values: `"solo"`, `"small_biz"`, `"mid_market"`, `"high"`, or `""`

### Parallelization Strategy
Launch 10 background subagents per wave (each handles 10 batches) + process 4 batches directly. Use sonnet model, bypassPermissions mode, run_in_background. Each subagent IS the AI extraction engine — it reads scraped_text and extracts structured fields directly, no external API call needed.

---

## Team Match Evaluation System — Phase 1 In Progress

**Full plan file**: `/Users/josephtepe/.claude/plans/purrfect-bubbling-firefly.md`

**Live URL**: https://jv-matchmaker-production.up.railway.app/matching/eval/

**Purpose**: 4 team members (Joe, David, Chelsea, Ken) rate match quality to calibrate the ISMC scoring algorithm. Human-in-the-loop calibration, not a user-facing feature.

### Current Status (as of 2026-03-04)
- **Calibration 1 batch**: 48 items — 8 high (≥68 ISMC), 35 mid (55-67), 5 low (<55)
- **Reviewers**: Joe Tepe, David Baer, Chelsea Frederick, Ken Cook — all have access codes
- **Ratings collected**: Joe Tepe has started; others invited via Slack

### What's Built
- `matching/models.py` — 5 models: `EvaluationReviewer`, `EvaluationBatch`, `EvaluationItem`, `MatchEvaluation`, `WeightExperiment`
- `matching/views_evaluation.py` — Full view set with session-based access-code auth
- `matching/forms_evaluation.py` — `MatchQualityForm` (Step 1) + `NarrativeQualityForm` (Step 2)
- `matching/management/commands/create_evaluation_batch.py` — Stratified sampling with ISMC-aligned bands
- All templates: `access.html`, `dashboard.html`, `batch_overview.html`, `evaluate.html`, `narrative.html`, `batch_complete.html`
- Migrations 0021–0023 applied

### Key Design Decisions (locked in)
- **Algorithm-blind**: Reviewers NEVER see algorithm scores. Scores stored on `EvaluationItem` for analysis only.
- **Two-step flow**: Step 1 = rate match from raw profiles only. Step 2 = rate the why_fit narrative quality.
- **Score bands**: Low (<55) / Mid (55–64) / High (≥64) — aligned with ISMC delivery threshold (64 = Premier tier cutoff)
- **Failure modes**: Wrong audience, one-sided value, stale profile, missing contact, scale mismatch, same niche no complement, data quality, **timing/launch readiness mismatch**
- **Retrospective norming**: 3 reflection questions on batch_complete page (saved to `EvaluationBatch.completion_notes`)

### Next Steps — When Team Finishes Rating

#### Step 1: Check inter-rater reliability
Build and run `compute_evaluation_reliability`:
```bash
python manage.py compute_evaluation_reliability --batch <batch_id>
```
- Uses `krippendorff` package (ordinal alpha on 7-point scale)
- **Target**: α ≥ 0.80 to proceed. If α < 0.667 → hold norming session + re-run 15 more matches.
- Also check per-reviewer variance (flag SD < 0.8 — satisficing detector)

#### Step 2: Phase 2 — Coverage batches
```bash
python manage.py create_evaluation_batch --phase coverage --size 12 --name "Coverage A" --reviewer david --reviewer chelsea
```
- 10 batches of 12 matches, 2-3 raters each
- Pace: 1-2 batches/week (~5 min/person/batch)
- After batch 5: active learning — prioritize borderline (55–67) and high-divergence matches

#### Step 3: Phase 3 — Weight regression (needs ~120 rated matches)
Build `analyze_evaluation_weights`:
- Ordinal regression: `human_rating ~ intent + synergy + momentum + context`
- Sub-factor analysis within each ISMC dimension
- Bayesian approach via PyMC (priors centered on current weights)
- Failure mode correlation analysis

#### Step 4: Phase 4 — Holdout validation
Build `ShadowScoringService` test:
```bash
python manage.py validate_weight_experiment --experiment <id>
```
- 48-match holdout, Wilcoxon signed-rank test
- Accept new weights if p < 0.05 with positive effect

### Management Commands Still to Build
| Command | When | Purpose |
|---------|------|---------|
| `compute_evaluation_reliability` | After Phase 1 complete | Krippendorff's alpha + satisficing detection |
| `analyze_evaluation_weights` | After ~120 ratings | Ordinal regression → weight proposals |
| `export_evaluation_data` | Any time | CSV export for external analysis |
| `validate_weight_experiment` | After Phase 3 | Holdout test via ShadowScoringService |

### Existing Patterns to Follow
- Access-code auth: `ReportAccessMixin` in `matching/views.py` (~line 836)
- CSS design system: cream/forest/gold in `templates/matching/report_access.html`
- Algorithm scores: `SupabaseMatchScoringService.score_pair()` in `matching/services.py` (~line 1631)
- Shadow scoring: `ShadowScoringService` in `matching/services.py` (~line 2592)
- ISMC weights: `matching/services.py` (~line 1418)
