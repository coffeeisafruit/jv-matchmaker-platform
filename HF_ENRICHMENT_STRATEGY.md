# Hugging Face Data Enrichment Strategy for JV MatchMaker

> Generated: 2026-02-17 | Based on full codebase analysis of `jv-matchmaker-platform`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Phase 1: Codebase Analysis](#phase-1-codebase-analysis)
3. [Phase 2: Enrichment Opportunity Map](#phase-2-enrichment-opportunity-map)
4. [Phase 3: Architecture Recommendations](#phase-3-architecture-recommendations)
5. [Phase 4: Implementation Roadmap](#phase-4-implementation-roadmap)
6. [Phase 5: Cost & Performance Analysis](#phase-5-cost--performance-analysis)

---

## Executive Summary

After a thorough analysis of the JV MatchMaker codebase — including all Django models, the cascade enrichment pipeline, ISMC matching engine, verification gate, and 3,143+ Supabase profiles — I've identified **7 high-impact enrichment opportunities** where Hugging Face models can materially improve matching quality.

**The single highest-impact integration is semantic embeddings for matching.** Currently, all matching relies on word-level Jaccard overlap (`matching/services.py` lines 1421-1454), meaning "business growth consultant" and "company scaling advisor" score as 0% overlap. Embedding-based similarity would fix this immediately.

**Estimated impact:**
- Matching quality: +30-40% improvement in synergy/audience scoring accuracy
- Enrichment pipeline: +15-20% more fields filled via classification (vs. Claude extraction)
- Cost reduction: 60-80% savings on classification tasks currently done by Claude ($0.03/call → $0.001/call)

---

## Phase 1: Codebase Analysis

### 1.1 Data Model Map

#### Core Profile Data (`matching/models.py` — `SupabaseProfile`)

| Field | Type | Current Fill Rate Problem |
|-------|------|---------------------------|
| `what_you_do` | TextField | Free-text, no classification |
| `who_you_serve` | TextField | Free-text, no taxonomy |
| `seeking` | TextField | Free-text, no structured matching |
| `offering` | TextField | Free-text, no structured matching |
| `niche` | TextField | Free-text mapped to 11 categories via keyword regex |
| `audience_type` | TextField | Rarely populated |
| `revenue_tier` | CharField | 5-tier enum, AI-extracted |
| `tags` | ArrayField | Inconsistent, user/AI-generated mix |
| `jv_history` | JSONField | Structured but sparse |
| `content_platforms` | JSONField | Platform presence only, no engagement quality |
| `audience_engagement_score` | FloatField | Deterministic formula, not learned |
| `social_proof` | TextField | Unstructured credentials dump |
| `business_focus` | TextField | Rarely populated |

#### Matching Score Storage (`matching/models.py` — `Match` / `SupabaseMatch`)

| Field | Source | Problem |
|-------|--------|---------|
| `intent_score` | Rule-based (5 factors) | No predictive modeling |
| `synergy_score` | Word-overlap Jaccard | No semantic understanding |
| `momentum_score` | Rule-based (4 factors) | Static, not learned |
| `context_score` | Completeness check | Data quality proxy only |
| `harmonic_mean` | Bidirectional aggregate | Penalizes weak dimensions correctly |

#### Enrichment Provenance (`enrichment_metadata` JSONField)

```json
{
  "field_meta": {
    "seeking": {"source": "exa_research", "updated_at": "2025-...", "pipeline_version": 1},
    "email": {"source": "apollo", "updated_at": "2025-...", "pipeline_version": 1}
  },
  "apollo_data": { ... }
}
```

### 1.2 Current AI/ML Pipeline

**Enrichment cascade** (`scripts/automated_enrichment_pipeline_safe.py`):

```
Exa.ai ($0.02/profile) → Crawl4AI + Claude ($0.03) → Deep Research (web search + Claude) → Apollo (contact only) → OWL (browser automation)
```

**AI models in use:**
- `anthropic/claude-sonnet-4` via OpenRouter — profile extraction, match explanation, verification
- Exa.ai internal LLM — structured schema extraction
- No Hugging Face models currently deployed
- No embeddings, no vector search, no classification models

**Verification gate** (`matching/enrichment/verification_gate.py`):
- Layer 1: Deterministic (regex, format checks)
- Layer 2: Source quote fuzzy matching (difflib.SequenceMatcher ≥ 0.75)
- Layer 3: Optional Claude verification

### 1.3 Current Matching Algorithm

**ISMC Framework** (`matching/services.py`):
- Intent (45%): JV history count, seeking field populated, booking link, membership status, website
- Synergy (25%): Offering↔Seeking word overlap, revenue tier diff, content platform overlap, audience word overlap
- Momentum (20%): Engagement score, social reach, current projects, list size
- Context (10%): Profile completeness, revenue tier known, enrichment quality, contact availability

**Critical weakness in Synergy scoring** (lines 1421-1454):

```python
# Current implementation — pure word overlap
words_a = {w for w in text_a.lower().split() if len(w) > 3 and w not in stop_words}
words_b = {w for w in text_b.lower().split() if len(w) > 3 and w not in stop_words}
overlap = words_a & words_b
ratio = len(overlap) / min(len(words_a), len(words_b))
```

This misses ALL semantic similarity. "Leadership coaching for executives" and "C-suite mentorship and development" share zero words but are the same market.

### 1.4 Identified Data Gaps

| Gap | Location | Impact |
|-----|----------|--------|
| No semantic similarity | `services.py` `_text_overlap_score()` | Synergy scores miss semantically identical offerings |
| Niche classification is keyword regex | `export_community_graph.py` lines 37-75 | Only 11 categories, many profiles fall to "Unknown" |
| No offer-type taxonomy | `SupabaseProfile.offering` | Can't categorize (course vs coaching vs agency vs SaaS) |
| No audience persona extraction | `who_you_serve` free text | Can't match complementary audiences systematically |
| `social_proof` is unstructured text | `SupabaseProfile.social_proof` | Can't extract/classify credentials (book author, TEDx, Forbes, etc.) |
| Audience overlap = 13 hardcoded keywords | `match_enrichment.py` `_find_audience_overlap()` | Misses most real overlap signals |
| No content-style matching | Not implemented | Partners with incompatible content styles get matched |
| `tags` are inconsistent | AI + user-generated mix | No controlled vocabulary, poor for matching |

---

## Phase 2: Enrichment Opportunity Map

### E1: Semantic Embedding for Profile Matching

- **What**: Generate dense vector embeddings for `seeking`, `offering`, `who_you_serve`, `what_you_do` fields. Replace `_text_overlap_score()` with cosine similarity.
- **Why**: Fixes the #1 matching quality gap. "Business scaling consultant for coaches" and "Growth strategist helping online coaches" currently score 0% overlap — would score ~0.85 with embeddings.
- **Input**: Existing text fields on `SupabaseProfile` (3,143+ profiles)
- **Output**: New `embedding_seeking`, `embedding_offering`, `embedding_who_you_serve`, `embedding_what_you_do` vector fields (or a separate embeddings table)
- **Model**: `BAAI/bge-large-en-v1.5` (1024-dim, MTEB #1 for retrieval) or `sentence-transformers/all-MiniLM-L6-v2` (384-dim, 5x faster, good enough for similarity)
- **Priority**: **HIGH** — highest impact-to-effort ratio in this entire plan

### E2: Zero-Shot Niche Classification

- **What**: Replace the keyword-matching niche categorizer (`export_community_graph.py` lines 37-75) with zero-shot classification that handles multi-label assignment
- **Why**: Current system maps to only 11 categories via exact keyword matching. Profiles with niches like "Stress Management for High Performers" fall to "Unknown" because no keyword matches. Zero-shot can classify to 30+ categories without training data.
- **Input**: `niche`, `what_you_do`, `who_you_serve`, `tags` concatenated
- **Output**: Updated `niche` field with primary + secondary categories, new `niche_categories` JSONField with confidence scores
- **Model**: `MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli` (zero-shot, 90.6% accuracy on NLI benchmarks) or `facebook/bart-large-mnli`
- **Priority**: **HIGH** — directly improves community graph quality and audience matching

### E3: Offer Type Taxonomy Classification

- **What**: Classify each profile's `offering` into a structured taxonomy: `course`, `coaching_1on1`, `group_coaching`, `mastermind`, `agency_service`, `software`, `book`, `speaking`, `consulting`, `done_for_you`, `certification`, `membership`
- **Why**: Enables a new matching dimension — compatible offer types for JV partnerships. A course creator pairs well with an email list owner for affiliate promos; a coach pairs with a podcast host for guest appearances.
- **Input**: `offering`, `signature_programs`, `what_you_do`
- **Output**: New `offer_types` JSONField — `[{type: "course", confidence: 0.92}, {type: "coaching_1on1", confidence: 0.78}]`
- **Model**: `MoritzLaurer/DeBERTa-v3-base-mnli-fever-anli` (zero-shot) with custom candidate labels, or fine-tuned `distilbert-base-uncased` if you accumulate 500+ labeled examples
- **Priority**: **HIGH** — enables entirely new matching signals

### E4: Social Proof Entity Extraction (NER)

- **What**: Extract structured credentials from the `social_proof` text field — book titles, media appearances, certifications, awards, speaking engagements
- **Why**: Currently `social_proof` is a dumped text blob. Structured extraction enables "Match me with published authors" or "Find TEDx speakers" filtering, plus credibility scoring.
- **Input**: `social_proof`, `bio` fields
- **Output**: New `credentials` JSONField — `{books: [{title, year}], media: ["Forbes", "Inc"], speaking: ["TEDx", "Thrive Summit"], certifications: ["ICF PCC"]}`
- **Model**: `dslim/bert-base-NER` for base entities + custom few-shot extraction via `meta-llama/Llama-3.1-8B-Instruct` for domain-specific entities (book titles, event names)
- **Priority**: **MEDIUM** — high user value but requires post-processing pipeline

### E5: Audience Persona Clustering

- **What**: Extract and cluster audience descriptions into persona archetypes, then match profiles serving complementary (not identical) audiences
- **Why**: Current audience overlap uses 13 hardcoded keywords (`coach, entrepreneur, leader...`). This misses that "burned-out corporate women" and "female executives seeking work-life balance" are the same audience.
- **Input**: `who_you_serve`, `audience_type`, `niche`, ICP data
- **Output**: `audience_persona` CharField (from ~25 discovered clusters), `audience_embedding` vector
- **Model**: Embed with `BAAI/bge-large-en-v1.5`, cluster with `scikit-learn` KMeans or HDBSCAN
- **Priority**: **MEDIUM** — meaningful improvement but E1 embeddings cover 70% of this value

### E6: Content Style Classification

- **What**: Classify each profile's content approach — `educational`, `inspirational`, `tactical`, `storytelling`, `data_driven`, `provocative`, `nurturing`
- **Why**: JV partners with wildly incompatible content styles (e.g., hard-sell marketer + gentle healer) create audience friction. Style compatibility is a new matching signal.
- **Input**: `what_you_do`, `bio`, crawled website content (from research cache)
- **Output**: New `content_style` JSONField — `{primary: "educational", secondary: "tactical", confidence: 0.87}`
- **Model**: `facebook/bart-large-mnli` zero-shot with style labels, or fine-tuned classifier on manually labeled sample
- **Priority**: **LOW** — nice-to-have, not core to JV compatibility

### E7: Partnership Outcome Prediction

- **What**: Train a classifier to predict which matches will lead to successful partnerships based on profile features + historical `MatchLearningSignal` data
- **Why**: Currently all ISMC weights are hardcoded constants. Learning from actual outcomes would let the system self-improve.
- **Input**: `MatchLearningSignal` records (outcome, match_score, reciprocity_balance, confidence_at_generation), profile feature pairs
- **Output**: Predicted conversion probability, suggested weight adjustments
- **Model**: Start with `scikit-learn` gradient boosting on tabular features, graduate to fine-tuned `distilbert` if text features matter
- **Priority**: **LOW** — requires sufficient outcome data (probably 200+ feedback signals) which may not exist yet

### Priority Summary

| # | Enrichment | Priority | Impact on Matching | Effort | Cost/Profile |
|---|-----------|----------|-------------------|--------|-------------|
| E1 | Semantic Embeddings | **HIGH** | +30-40% synergy accuracy | Medium | $0.0003 |
| E2 | Niche Classification | **HIGH** | +20% audience matching | Low | $0.001 |
| E3 | Offer Type Taxonomy | **HIGH** | New matching dimension | Low | $0.001 |
| E4 | Social Proof NER | MEDIUM | Credibility scoring | Medium | $0.002 |
| E5 | Audience Persona Clustering | MEDIUM | Better audience matching | Medium | $0.001 |
| E6 | Content Style | LOW | Compatibility signal | Low | $0.001 |
| E7 | Outcome Prediction | LOW | Self-improving weights | High | N/A (training) |

---

## Phase 3: Architecture Recommendations

### 3.1 Prototyping Layer (AI Sheets)

Use AI Sheets to validate enrichment quality before writing production code.

#### AI Sheets Experiment 1: Niche Classification (E2)

1. Export 200 profiles from Supabase: `SELECT name, niche, what_you_do, who_you_serve FROM profiles WHERE niche IS NOT NULL LIMIT 200`
2. Import CSV into AI Sheets
3. Add generated column with prompt:

```
Classify this person's business niche into one or more of these categories:
Business Coaching, Health & Wellness, Mindset & Personal Development, Relationships & Dating,
Spirituality & Energy Work, Fitness & Nutrition, Financial Education, Marketing & Sales,
Leadership & Management, Parenting & Family, Career Development, Creative Arts,
Real Estate, Technology & SaaS, Education & Learning, Productivity & Performance,
Women's Empowerment, Men's Development, Grief & Trauma, Alternative Health

Input: Niche: {niche} | Does: {what_you_do} | Serves: {who_you_serve}

Return JSON: {"primary": "category", "secondary": "category_or_null", "confidence": 0.0-1.0}
```

4. Compare against current keyword mapping in a second column
5. Export config for batch processing via HF Jobs

#### AI Sheets Experiment 2: Offer Type Classification (E3)

1. Same 200 profiles + `offering`, `signature_programs` columns
2. Add generated column:

```
Classify this person's business offering type(s). Choose ALL that apply:
online_course, coaching_1on1, group_coaching, mastermind, done_for_you_service,
software_saas, book_author, speaking, consulting, certification_program,
membership_community, affiliate_network, agency, digital_product, live_events

Input: Offering: {offering} | Programs: {signature_programs} | Does: {what_you_do}

Return JSON array: [{"type": "...", "confidence": 0.0-1.0}]
```

3. Manually review 50 results for accuracy
4. Iterate prompt until ≥85% accuracy

#### AI Sheets Experiment 3: Social Proof Extraction (E4)

1. Export profiles with populated `social_proof` field
2. Test extraction prompt for books, media, speaking, certifications
3. Validate against manually verified samples

### 3.2 Production Integration (Inference API)

#### Integration Point Map

| Enrichment | Trigger Point | Sync/Async | File to Modify |
|-----------|---------------|------------|----------------|
| E1: Embeddings | Profile create/update + batch backfill | Async (batch) + Sync (single) | `matching/services.py` |
| E2: Niche Classification | After enrichment pipeline writes `niche` | Async (post-enrichment hook) | `scripts/automated_enrichment_pipeline_safe.py` |
| E3: Offer Type | After enrichment pipeline writes `offering` | Async (post-enrichment hook) | `scripts/automated_enrichment_pipeline_safe.py` |
| E4: Social Proof NER | After enrichment writes `social_proof` | Async (batch job) | New: `matching/enrichment/hf_enrichment.py` |
| E5: Audience Clustering | Nightly batch after embeddings exist | Background job | New: `matching/enrichment/hf_enrichment.py` |

#### API Call Pattern

```python
# Production: HF Inference API (OpenAI-compatible)
import requests

HF_API_URL = "https://router.huggingface.co/hf-inference/models"

def get_embedding(text: str, model: str = "BAAI/bge-large-en-v1.5") -> list[float]:
    """Single text → embedding vector."""
    response = requests.post(
        f"{HF_API_URL}/{model}/pipeline/feature-extraction",
        headers={"Authorization": f"Bearer {HF_API_TOKEN}"},
        json={"inputs": text, "options": {"wait_for_model": True}}
    )
    return response.json()[0]  # Mean pooling handled by API

def classify_zero_shot(text: str, labels: list[str], model: str = "MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli") -> dict:
    """Zero-shot classification with candidate labels."""
    response = requests.post(
        f"{HF_API_URL}/{model}/pipeline/zero-shot-classification",
        headers={"Authorization": f"Bearer {HF_API_TOKEN}"},
        json={"inputs": text, "parameters": {"candidate_labels": labels, "multi_label": True}}
    )
    result = response.json()
    return dict(zip(result["labels"], result["scores"]))
```

#### Recommended Models (Specific)

| Task | Primary Model | Fallback | Dim/Size | Latency |
|------|--------------|----------|----------|---------|
| Embeddings | `BAAI/bge-large-en-v1.5` | `sentence-transformers/all-MiniLM-L6-v2` | 1024 / 384 | 50ms / 15ms |
| Zero-shot classification | `MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli` | `facebook/bart-large-mnli` | — | 200ms / 300ms |
| NER | `dslim/bert-base-NER` | `Jean-Baptiste/roberta-large-ner-english` | — | 30ms |
| Summarization (if needed) | `facebook/bart-large-cnn` | `philschmid/bart-large-cnn-samsum` | — | 500ms |

#### Rate Limiting & Caching Strategy

```python
# Add to matching/enrichment/hf_enrichment.py

EMBEDDING_CACHE = {}  # In-memory for batch runs; Redis for production

def get_embedding_cached(text: str) -> list[float]:
    cache_key = hashlib.md5(text.encode()).hexdigest()
    if cache_key in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[cache_key]
    embedding = get_embedding(text)
    EMBEDDING_CACHE[cache_key] = embedding
    return embedding
```

HF Inference API limits:
- Free tier: ~1,000 requests/day, queued during high demand
- Pro ($9/mo): 20,000 requests/day, no queue
- Enterprise: Dedicated endpoints, guaranteed throughput

**Recommendation**: Start with Pro ($9/mo) for development + batch backfill, evaluate dedicated endpoint ($0.06/hr for bge-large) if doing real-time matching at scale.

#### Error Handling & Graceful Degradation

```python
# Pattern: HF unavailable → fall back to current word-overlap scoring
def compute_synergy_score(source_profile, target_profile):
    try:
        # Try embedding-based similarity first
        emb_a = get_embedding_cached(source_profile.seeking or "")
        emb_b = get_embedding_cached(target_profile.offering or "")
        similarity = cosine_similarity(emb_a, emb_b)
        return similarity * 10  # Scale to 0-10
    except (requests.RequestException, KeyError):
        # Fall back to current word-overlap implementation
        return _text_overlap_score(source_profile.seeking, target_profile.offering)
```

This ensures the matching engine never breaks if HF is down — it just degrades to current behavior.

### 3.3 Data Pipeline Design

#### Schema Migrations Needed

```sql
-- Migration 1: Embedding storage (add to profiles table)
ALTER TABLE profiles ADD COLUMN embedding_seeking vector(384);
ALTER TABLE profiles ADD COLUMN embedding_offering vector(384);
ALTER TABLE profiles ADD COLUMN embedding_who_you_serve vector(384);
ALTER TABLE profiles ADD COLUMN embedding_what_you_do vector(384);
ALTER TABLE profiles ADD COLUMN embeddings_updated_at timestamptz;

-- Note: Using 384-dim (MiniLM) for cost/storage efficiency.
-- Upgrade to 1024-dim (bge-large) later if quality warrants it.
-- Requires pgvector extension on Supabase: CREATE EXTENSION IF NOT EXISTS vector;

-- Migration 2: Classification fields
ALTER TABLE profiles ADD COLUMN niche_categories jsonb DEFAULT '[]';
ALTER TABLE profiles ADD COLUMN offer_types jsonb DEFAULT '[]';
ALTER TABLE profiles ADD COLUMN credentials jsonb DEFAULT '{}';
ALTER TABLE profiles ADD COLUMN content_style jsonb DEFAULT '{}';
ALTER TABLE profiles ADD COLUMN audience_persona varchar(100);
ALTER TABLE profiles ADD COLUMN classification_updated_at timestamptz;
```

**Supabase note**: Supabase natively supports `pgvector`. Enable via Dashboard → Database → Extensions → vector.

#### Storage vs Compute-on-the-fly Decision

| Enrichment | Store | Rationale |
|-----------|-------|-----------|
| Embeddings | **Store** | Expensive to recompute; needed for batch similarity queries |
| Niche classification | **Store** | Changes rarely; needed for graph visualization |
| Offer types | **Store** | Changes rarely; used in matching filters |
| Credentials (NER) | **Store** | Stable data; enables search/filter |
| Content style | **Store** | Changes rarely; used in matching |
| Audience persona | **Store** | Derived from embeddings; cluster label is cheap to store |

All enrichments should be **stored**, not computed on-the-fly. Profiles change infrequently (most are enriched once), and matching queries run against the full corpus.

#### Re-enrichment Strategy

Integrate with the existing provenance system in `enrichment_metadata`:

```python
# Add to enrichment_metadata.field_meta:
"embedding_seeking": {
    "source": "hf_embedding",
    "model": "sentence-transformers/all-MiniLM-L6-v2",
    "updated_at": "2026-02-17T...",
    "pipeline_version": 2,
    "input_hash": "abc123"  # MD5 of source text — re-embed only when text changes
}
```

**Re-enrichment triggers:**
1. Source text field updated → re-embed that field only
2. Model upgrade → batch re-embed all profiles (flag via `pipeline_version`)
3. Monthly sweep for stale embeddings (older than `--stale-days`)
4. Classification re-run when taxonomy changes

#### Batch vs Real-time Strategy

- **Existing 3,143 profiles**: Batch backfill via new management command
- **New profiles / profile updates**: Real-time embedding + classification during `consolidate_to_supabase_batch()` post-write hook
- **Matching queries**: Pre-computed embeddings; cosine similarity is fast on stored vectors

### 3.4 Matching Engine Enhancement

#### Current vs Enhanced Synergy Scoring

**Current** (`services.py` line ~1375):
```python
# Synergy sub-factors (current)
offering_seeking = _text_overlap_score(source.seeking, target.offering)    # Word overlap
audience_alignment = _text_overlap_score(source.who_you_serve, target.who_you_serve)  # Word overlap
revenue_compat = revenue_tier_diff_score(source, target)                     # Ordinal
platform_overlap = count_shared_platforms(source, target)                     # Set intersection
```

**Enhanced** (with HF embeddings):
```python
# Synergy sub-factors (enhanced)
offering_seeking = cosine_sim(source.embedding_seeking, target.embedding_offering) * 10  # Semantic!
audience_alignment = cosine_sim(source.embedding_who_you_serve, target.embedding_who_you_serve) * 10
revenue_compat = revenue_tier_diff_score(source, target)  # Keep as-is
platform_overlap = count_shared_platforms(source, target)   # Keep as-is
offer_type_compat = offer_type_compatibility_score(source.offer_types, target.offer_types)  # NEW
```

#### New Matching Dimensions Enabled by Enrichment

| New Dimension | Source Enrichment | Where It Plugs In |
|--------------|-------------------|-------------------|
| Semantic offering↔seeking match | E1 embeddings | Synergy score (replace word overlap) |
| Semantic audience alignment | E1 embeddings | Synergy score (replace word overlap) |
| Offer type compatibility | E3 classification | New Synergy sub-factor |
| Credibility tier | E4 NER extraction | New Context sub-factor |
| Content style compatibility | E6 classification | New Context sub-factor |
| Audience persona complementarity | E5 clustering | New Synergy sub-factor |

#### Weighting Recommendations

Keep the ISMC structure but update sub-factor weights:

```python
# Updated Synergy sub-factors
SYNERGY_WEIGHTS = {
    'offering_seeking_semantic': 3.5,   # Was 3 (word overlap) — increase because now more reliable
    'audience_alignment_semantic': 3.0, # Was 2.5 — increase for same reason
    'revenue_tier_compat': 2.0,         # Was 2.5 — slight decrease, less important than semantic
    'content_platform_overlap': 1.5,    # Was 2 — less important now
    'offer_type_compat': 2.0,           # NEW — valuable for JV format decisions
}
```

#### Compatibility Score Improvements

Expected improvements by enrichment:

| Scenario | Current Score | Enhanced Score | Why |
|----------|-------------|---------------|-----|
| "Business growth coach" ↔ "Company scaling mentor" | Synergy: 3.0 (no word overlap) | Synergy: 8.5 (semantic match) | Embeddings capture meaning |
| "Stress Management" niche ↔ "Mental Health" niche | No match (different keywords) | Match via classification | Both classified as "Mental Health & Wellness" |
| Course creator ↔ Email list owner | No signal | Offer type compatibility: 9.0 | Affiliate JV format detected |
| TEDx speaker ↔ Podcast host | No signal | Credibility + platform match | NER extracts speaking credentials |

---

## Phase 4: Implementation Roadmap

### Phase A — Quick Wins (This Week)

#### A1: Zero-Shot Niche Classification

**Effort**: 4-6 hours | **Impact**: Community graph + audience matching
**Files to modify**: `scripts/export_community_graph.py`, new `matching/enrichment/hf_enrichment.py`

1. Create `lib/enrichment/hf_client.py` with HF Inference API wrapper
2. Run zero-shot classification on all 3,143 profiles' `niche + what_you_do + who_you_serve`
3. Store results in `niche_categories` JSONField (migration needed)
4. Update `export_community_graph.py` to use `niche_categories` instead of keyword regex
5. No matching algorithm change needed yet — this is data quality improvement

**Schema change**: Add `niche_categories jsonb DEFAULT '[]'` to profiles table.

#### A2: Zero-Shot Offer Type Classification

**Effort**: 3-4 hours | **Impact**: New matching signal
**Files to modify**: Same as A1

1. Classify `offering + signature_programs + what_you_do` into offer type taxonomy
2. Store in `offer_types` JSONField
3. No matching integration yet — just populate the data

**Schema change**: Add `offer_types jsonb DEFAULT '[]'` to profiles table.

#### A3: AI Sheets Validation

**Effort**: 2 hours | **No code changes**

1. Export 200 profiles to CSV
2. Run niche + offer type classifications in AI Sheets
3. Manually review accuracy, iterate prompts
4. Use validated prompts in A1/A2 production code

### Phase B — Core Enrichment Pipeline (2-4 Weeks)

#### B1: Semantic Embeddings for All Profiles

**Effort**: 1 week | **Impact**: Matching quality revolution
**Files to modify**: `matching/services.py`, `matching/models.py`, new management command

1. Enable `pgvector` on Supabase
2. Add embedding columns to `profiles` table (migration)
3. Create batch embedding script — embed all 3,143 profiles (~5 min with MiniLM, ~15 min with bge-large)
4. Add embedding generation to enrichment pipeline's `consolidate_to_supabase_batch()`
5. Update `SupabaseProfile` model with vector fields

**Key decision point**: 384-dim (MiniLM, faster/cheaper) vs 1024-dim (bge-large, higher quality). **Recommendation**: Start with MiniLM for validation, upgrade if quality delta is noticeable.

#### B2: Replace Word Overlap with Cosine Similarity

**Effort**: 3-4 days | **Impact**: Core matching improvement
**Files to modify**: `matching/services.py` (lines 1375-1454 and 1421-1454)

1. Modify `SupabaseMatchScoringService._compute_synergy_factors()` to use embeddings
2. Keep `_text_overlap_score()` as fallback when embeddings are null
3. Update synergy sub-factor weights (see Section 3.4)
4. Add `offer_type_compat` as new synergy sub-factor
5. Re-score all existing matches via `RecalculateAllMatchesView`

#### B3: Social Proof NER Extraction

**Effort**: 3-4 days | **Impact**: Credibility scoring + filtering
**Files to modify**: New extraction module, profiles migration

1. NER extraction on `social_proof` + `bio` fields
2. Post-process NER output into structured `credentials` JSON
3. Add credibility score as new Context sub-factor
4. Backfill all profiles

#### B4: Integrate into Enrichment Pipeline

**Effort**: 2-3 days | **Impact**: Automation
**Files to modify**: `scripts/automated_enrichment_pipeline_safe.py`

1. Add HF enrichment as Step 6 in the pipeline (after Supabase write, before cache update)
2. Generate embeddings for newly enriched fields
3. Run classification on newly enriched profiles
4. Write provenance metadata for HF-generated fields

### Phase C — Advanced Intelligence (1-2 Months)

#### C1: Audience Persona Clustering

1. Cluster `embedding_who_you_serve` vectors using HDBSCAN
2. Name clusters via representative profiles
3. Add `audience_persona` field
4. Enable "complementary audience" matching (not just similar)

#### C2: Content Style Classification

1. Classify content approach from bio/website text
2. Add style compatibility as matching signal
3. Surface in match explanations ("You both use an educational, evidence-based approach")

#### C3: Outcome-Based Weight Learning

1. Collect `MatchLearningSignal` data (need 200+ signals)
2. Train gradient boosting model on tabular features
3. Learn optimal ISMC weights per niche/tier segment
4. A/B test learned weights vs hardcoded weights

#### C4: Custom Fine-Tuned Classifier

1. After 500+ manually validated niche/offer-type labels from AI Sheets
2. Fine-tune `distilbert-base-uncased` on your taxonomy
3. Deploy as dedicated HF endpoint
4. Replace zero-shot with fine-tuned (faster + more accurate)

---

## Phase 5: Cost & Performance Analysis

### API Cost Estimates

#### Per-Profile Costs (HF Inference API Pro — $9/mo base)

| Operation | Model | Cost/Call | Calls/Profile | Total/Profile |
|-----------|-------|-----------|--------------|---------------|
| Embedding (4 fields) | MiniLM-L6-v2 | $0.00006 | 4 | $0.00024 |
| Embedding (4 fields) | bge-large-en-v1.5 | $0.00015 | 4 | $0.0006 |
| Niche classification | DeBERTa-v3-large | $0.0001 | 1 | $0.0001 |
| Offer type classification | DeBERTa-v3-base | $0.00008 | 1 | $0.00008 |
| NER extraction | bert-base-NER | $0.00005 | 1 | $0.00005 |
| **Total (MiniLM)** | | | | **$0.00047** |
| **Total (bge-large)** | | | | **$0.00083** |

#### Scale Projections

| User Scale | Profiles | Batch Enrichment Cost | Monthly Re-enrichment (10%) | Annual Total |
|-----------|----------|----------------------|---------------------------|-------------|
| Current | 3,143 | $1.48 (one-time backfill) | $0.15/mo | $1.80/yr |
| 1,000 users (10K profiles) | 10,000 | $4.70 | $0.47/mo | $5.64/yr |
| 10,000 users (100K profiles) | 100,000 | $47.00 | $4.70/mo | $56.40/yr |

**Context**: Your current Claude enrichment costs are ~$0.03/profile (Exa) to $0.06/profile (crawl4ai + Claude). HF classification costs are **60-100x cheaper** per call for tasks that don't require generation.

#### Comparison: HF Inference API vs Self-Hosted

| Metric | HF Inference API (Pro) | Self-Hosted (Vast.ai GPU) |
|--------|----------------------|--------------------------|
| Setup time | 0 (API key only) | 4-8 hours |
| Monthly base cost | $9/mo | $0.75/hr × uptime hours |
| Cost at 3K profiles | $9 + $1.48 = $10.48 | ~$20/mo (if running 24/7) |
| Cost at 100K profiles | $9 + $47 = $56 | ~$20/mo (same GPU) |
| Break-even point | — | ~50K profiles/month |
| Latency | 15-200ms (shared infra) | 5-50ms (dedicated GPU) |
| Maintenance | None | Docker, model updates, monitoring |

**Recommendation**: Use HF Inference API until monthly enrichment exceeds 50K profiles. At that point, self-hosting on Vast.ai (per your existing `VAST_AI_INTEGRATION.md` plan) becomes cost-effective for both Claude replacement AND HF model hosting on the same GPU.

#### Latency Impact on User Experience

| Operation | Current Latency | With HF (API) | With HF (Self-hosted) |
|-----------|----------------|---------------|----------------------|
| Profile view (matching) | ~200ms (DB query) | ~200ms (pre-computed) | ~200ms (pre-computed) |
| Calculate match (single) | ~500ms (word overlap) | ~700ms (+embedding lookup) | ~550ms |
| Bulk recalculate (100 matches) | ~5s | ~8s (if embeddings cached) | ~6s |
| New profile enrichment | 15-60s (Exa/Claude) | +2s (HF classification) | +1s |

Embeddings are pre-computed and stored — matching queries don't call HF at all. The only latency impact is during enrichment (adding ~2s to a 15-60s pipeline — negligible).

### When to Self-Host

Switch to self-hosted models when ANY of these conditions are met:
1. Monthly enrichment exceeds 50K profiles
2. You're already running Vast.ai for Claude replacement (per `v2.0/docs/planning/VAST_AI_INTEGRATION.md`)
3. You need <10ms embedding latency for real-time matching
4. HF API rate limits become a bottleneck (unlikely before 100K profiles)

**Recommended self-hosted stack**: Vast.ai GPU + `vLLM` or `text-embeddings-inference` (TEI) server by HF, serving both embeddings and classification models on one A100/H100.

---

## Prerequisites & Infrastructure Gaps

### Identified Gaps

1. **No background job system**: The codebase has no Celery, Django-Q, or similar task queue. The enrichment pipeline runs as a standalone script (`scripts/automated_enrichment_pipeline_safe.py`) via command line. For real-time HF enrichment on profile update, you'll need either:
   - A Django signal → management command approach (simplest)
   - Django-Q or Celery for proper async processing (recommended for Phase B+)
   - Or: keep batch-only processing and run HF enrichment as part of the existing pipeline

2. **No pgvector extension**: Supabase supports it, but it needs to be enabled. This is a one-click operation in Supabase Dashboard → Database → Extensions.

3. **No HF API key in environment**: Add `HF_API_TOKEN` to your environment variables alongside existing keys.

**Recommendation**: For Phase A (quick wins), batch processing via management command is fine. For Phase B, add `django-q2` (lightweight task queue with Django ORM backend — no Redis needed) for async enrichment on profile updates.

---

## Appendix: File Reference Map

| File | Current Role | HF Integration Point |
|------|-------------|---------------------|
| `matching/services.py` | ISMC scoring, `_text_overlap_score()` | Replace with cosine similarity (E1) |
| `matching/models.py` | `SupabaseProfile` model | Add embedding + classification fields |
| `scripts/automated_enrichment_pipeline_safe.py` | Batch enrichment orchestrator | Add HF step after Supabase write |
| `scripts/export_community_graph.py` | Niche categorization (keyword) | Replace with classification (E2) |
| `matching/enrichment/ai_research.py` | Claude extraction (17 fields) | No change — HF supplements, doesn't replace |
| `matching/enrichment/verification_gate.py` | 3-layer quality gate | Add Layer 1b: classification confidence check |
| `matching/enrichment/match_enrichment.py` | LLM match explanation | Feed classification data into prompts |
| `config/settings.py` | `GTM_CONFIG` scoring weights | Update synergy sub-factor weights |
| **New**: `lib/enrichment/hf_client.py` | — | HF API wrapper with caching |
| **New**: `lib/enrichment/embeddings.py` | — | Embedding generation + similarity |
| **New**: `lib/enrichment/classifiers.py` | — | Zero-shot classification functions |
| **New**: `matching/management/commands/backfill_embeddings.py` | — | Batch backfill command |
