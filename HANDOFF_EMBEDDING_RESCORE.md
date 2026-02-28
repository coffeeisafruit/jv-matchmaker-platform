# Handoff: Embedding-Based Synergy Scoring — Production Integration

## What Was Done (Complete)

### 1. Model Swap
- `lib/enrichment/hf_client.py`: DEFAULT_EMBEDDING_MODEL → `BAAI/bge-large-en-v1.5` (1024-dim)
- `lib/enrichment/embeddings.py`: Updated default model name and pgvector SQL from 384→1024 dim

### 2. Database Migration
- `matching/migrations/0013_add_embedding_columns.py`: pgvector extension, 4 vector(1024) columns, ivfflat indexes, metadata columns. **Migration applied successfully to Supabase.**

### 3. Calibrated Scoring in _score_synergy()
- `matching/services.py` (lines ~1260-1505): Added `EMBEDDING_SCORE_THRESHOLDS`, `_parse_pgvector()`, `_cosine_similarity()`, `_embedding_to_score()`. Rewrote `_score_synergy()` to use embedding cosine similarity when available, word_overlap fallback otherwise. Thresholds calibrated from validation data (synonym mean=0.75, noise floor=0.53).

### 4. Backfill Embeddings — COMPLETE
- `matching/management/commands/backfill_embeddings.py`: All 3,060 profiles embedded (517 skipped for no text, 0 failures). Uses `BAAI/bge-large-en-v1.5` locally via sentence-transformers.

### 5. Django Model — Embedding Fields Added
- `matching/models.py` SupabaseProfile: Added `embedding_seeking`, `embedding_offering`, `embedding_who_you_serve`, `embedding_what_you_do` as TextField (pgvector returns strings that `_parse_pgvector()` converts to float lists).

### 6. Rescore Management Command
- `matching/management/commands/rescore_matches.py`: Scores all matches using ISMC engine, writes `score_ab`, `score_ba`, `harmonic_mean`, `match_context` (does NOT overwrite `match_score`).

### 7. Diagnostic Scripts Written
- `scripts/embedding_isolation_test.py`: Compares ISMC word-overlap-only vs ISMC+embeddings on same matches
- `scripts/momentum_diagnostic.py`: Analyzes Momentum bottleneck, data coverage, aggregation method comparison

---

## What's Partially Done

### Live Rescore — 25,619 of 29,863 matches written
The live rescore (`python manage.py rescore_matches`) wrote ISMC scores to 25,619 matches before PgBouncer killed the connection ("SSL connection closed unexpectedly"). **4,244 matches still have harmonic_mean=NULL.**

**To finish the rescore**, the command needs a batching fix to avoid PgBouncer timeouts. The pattern (same fix used in backfill_embeddings.py):
1. Load match IDs where `harmonic_mean IS NULL` upfront (lightweight query)
2. Process in batches of ~500
3. Call `django.db.connection.close()` between batches to force fresh connections

The current command at `matching/management/commands/rescore_matches.py` does `list(SupabaseMatch.objects.all())` which holds the connection open during the full load, then does individual UPDATE statements that eventually time out.

**Quick fix approach**: Add a `--resume` flag that filters to `harmonic_mean__isnull=True`, and batch the writes with `connection.close()` every 500 matches.

---

## Key Diagnostic Results

### Embedding Impact (Isolated)
From `scripts/embedding_isolation_test.py` on 50 matches:
- **ISMC word-overlap mean: 50.48** vs **ISMC+embeddings mean: 52.98** → **+2.50 points**
- Offering↔Seeking sub-factor: word_overlap 4.43 → embedding 5.37 (+0.94)
- Audience Alignment sub-factor: word_overlap 4.72 → embedding 6.69 (+1.97)
- 58,796 semantic evaluations, 930 word_overlap fallbacks across full dry-run

### Momentum Bottleneck (from momentum_diagnostic.py on 200 matches)
- **Momentum is the bottleneck in 57% of matches** (mean score 4.38/10)
- Root cause: **DATA COVERAGE problem, not calibration**
  - `audience_engagement_score`: 55.9% have data
  - `social_reach`: 62.4% have data
  - `current_projects`: **19.4% have data** (worst)
  - `list_size`: 54.8% have data
- When all Momentum fields are null, floor score = **3.55/10**
- This 3.55 floor drags the harmonic mean down because harmonic mean is punitive to weak dimensions

### Aggregation Method Comparison (200 matches)
| Method | Mean | Median | StdDev |
|--------|------|--------|--------|
| Harmonic (current) | 53.96 | 53.85 | 5.23 |
| Geometric | 55.13 | 55.35 | 5.24 |
| Weighted Average | 56.26 | 56.68 | 5.34 |

Tier distribution: Harmonic puts 171/200 in "Fair", Weighted Avg puts 150/200 in "Fair" and 50 in "Good".

### match_score Usage Map
`match_score` is the old hybrid_matcher score. The rescore does NOT overwrite it. It's read by:
- `matching/admin.py` (admin display, read-only)
- `matching/tasks.py:219` (writing new matches)
- `matching/management/commands/generate_member_report.py:242` (writing reports)
- `scripts/export_top_matches.py` (reads harmonic_mean, names it match_score in export)
- No HTML templates reference it directly

---

## Immediate Next Steps

### Step 1: Fix and finish the rescore
The rescore needs a PgBouncer-safe batching approach. 4,244 matches remain with `harmonic_mean IS NULL`. Options:
- A) Add `--resume` flag + batch writes with `connection.close()` every 500
- B) Write a one-off script that queries unscored match IDs, scores in batches

### Step 2: Decide on Momentum
The user identified Momentum as the main bottleneck. Two paths discussed:
1. **Fix data coverage** — enrich more profiles with engagement/reach/project data
2. **Switch aggregation method** — geometric mean or weighted average would be less punitive to thin Momentum data
3. **Both** — fix data AND switch method

The user explicitly asked to investigate these options. The diagnostic data is ready in `scripts/momentum_diagnostic.py` output.

### Step 3: DO NOT list (still applies)
- No offer_type_compat changes
- No ISMC weight changes
- No new enrichment fields
- No enrichment pipeline trigger changes

---

## Key Files
```
matching/services.py              — Core ISMC scoring engine (embedding synergy at lines ~1429-1505)
matching/models.py                — SupabaseProfile with embedding TextFields, SupabaseMatch
matching/management/commands/
  rescore_matches.py              — Rescore command (NEEDS BATCH FIX for remaining 4,244)
  backfill_embeddings.py          — Embedding backfill (COMPLETE)
lib/enrichment/hf_client.py      — HF embedding client (bge-large-en-v1.5)
lib/enrichment/embeddings.py     — ProfileEmbeddingService
scripts/embedding_isolation_test.py  — Isolation test script
scripts/momentum_diagnostic.py       — Momentum bottleneck diagnostic
validation_results/production_impact_report.txt — Latest dry-run report
```

## Environment
```bash
# Virtualenv
/Users/josephtepe/Projects/jv-matchmaker-platform/venv/bin/python

# Run Django commands
/Users/josephtepe/Projects/jv-matchmaker-platform/venv/bin/python manage.py <command>

# Settings module
DJANGO_SETTINGS_MODULE=config.settings
```

## Database State
- 3,578 profiles total, 3,060 have embeddings
- 29,863 matches total, **25,619 have ISMC scores**, 4,244 need rescoring
- `match_score` column preserved (original hybrid_matcher scores, untouched)
- `harmonic_mean` column has new ISMC scores for 25,619 matches
