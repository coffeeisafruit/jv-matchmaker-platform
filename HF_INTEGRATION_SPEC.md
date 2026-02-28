# Hugging Face Integration — Technical Specification

> Companion document to `HF_ENRICHMENT_STRATEGY.md`
> Generated: 2026-02-17

---

## 1. Environment Setup

### New Environment Variables

```bash
# Add to .env / environment
HF_API_TOKEN=hf_xxxxxxxxxxxxxxxxxxxx        # Hugging Face API token (Pro: $9/mo)
HF_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2  # Default embedding model
HF_CLASSIFICATION_MODEL=MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli  # Default classifier
```

### Python Dependencies

```bash
# Add to requirements.txt
huggingface_hub>=0.23.0       # HF Inference client (official)
numpy>=1.24.0                 # Vector operations (likely already installed)
# Optional for local embedding (Phase C):
# sentence-transformers>=2.7.0
# torch>=2.0
```

### Supabase: Enable pgvector

```sql
-- Run once in Supabase SQL Editor
CREATE EXTENSION IF NOT EXISTS vector;
```

---

## 2. Database Migrations

### Migration: Add HF enrichment columns to profiles

**File**: `matching/migrations/0013_add_hf_enrichment_columns.py`

```python
from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0012_add_social_proof'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                -- Embedding vectors (384-dim for MiniLM-L6-v2)
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_seeking vector(384);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_offering vector(384);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_who_you_serve vector(384);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_what_you_do vector(384);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embeddings_model varchar(100);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embeddings_updated_at timestamptz;

                -- Classification fields
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS niche_categories jsonb DEFAULT '[]'::jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS offer_types jsonb DEFAULT '[]'::jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS credentials jsonb DEFAULT '{}'::jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS content_style jsonb DEFAULT '{}'::jsonb;
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS audience_persona varchar(100);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS classification_updated_at timestamptz;

                -- Indexes for vector similarity search
                CREATE INDEX IF NOT EXISTS idx_profiles_embedding_seeking
                    ON profiles USING ivfflat (embedding_seeking vector_cosine_ops) WITH (lists = 50);
                CREATE INDEX IF NOT EXISTS idx_profiles_embedding_offering
                    ON profiles USING ivfflat (embedding_offering vector_cosine_ops) WITH (lists = 50);

                -- GIN index for classification JSON queries
                CREATE INDEX IF NOT EXISTS idx_profiles_offer_types
                    ON profiles USING gin (offer_types);
                CREATE INDEX IF NOT EXISTS idx_profiles_niche_categories
                    ON profiles USING gin (niche_categories);
            """,
            reverse_sql="""
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_seeking;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_offering;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_who_you_serve;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_what_you_do;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embeddings_model;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embeddings_updated_at;
                ALTER TABLE profiles DROP COLUMN IF EXISTS niche_categories;
                ALTER TABLE profiles DROP COLUMN IF EXISTS offer_types;
                ALTER TABLE profiles DROP COLUMN IF EXISTS credentials;
                ALTER TABLE profiles DROP COLUMN IF EXISTS content_style;
                ALTER TABLE profiles DROP COLUMN IF EXISTS audience_persona;
                ALTER TABLE profiles DROP COLUMN IF EXISTS classification_updated_at;
            """,
        ),
    ]
```

### Django Model Update

**File**: `matching/models.py` — Add to `SupabaseProfile` class:

```python
# HF Enrichment Fields (added migration 0013)
# Note: vector fields are managed via raw SQL; Django accesses them as text
niche_categories = models.JSONField(default=list, blank=True)
offer_types = models.JSONField(default=list, blank=True)
credentials = models.JSONField(default=dict, blank=True)
content_style = models.JSONField(default=dict, blank=True)
audience_persona = models.CharField(max_length=100, blank=True, null=True)
classification_updated_at = models.DateTimeField(blank=True, null=True)
embeddings_model = models.CharField(max_length=100, blank=True, null=True)
embeddings_updated_at = models.DateTimeField(blank=True, null=True)
```

---

## 3. Core Module Specifications

### 3.1 `lib/enrichment/hf_client.py` — HF API Client

**Purpose**: Centralized HF Inference API client with retry, caching, and rate limiting.

**Interface**:
```python
class HFClient:
    def __init__(self, api_token: str = None):
        """Initialize with HF_API_TOKEN from env if not provided."""

    def embed(self, text: str, model: str = None) -> list[float]:
        """Generate embedding vector for a single text string.
        Returns: list of floats (384-dim for MiniLM, 1024-dim for bge-large)
        """

    def embed_batch(self, texts: list[str], model: str = None, batch_size: int = 32) -> list[list[float]]:
        """Batch embedding for multiple texts.
        Returns: list of embedding vectors
        """

    def classify_zero_shot(self, text: str, labels: list[str], multi_label: bool = True, model: str = None) -> dict[str, float]:
        """Zero-shot classification with candidate labels.
        Returns: {label: confidence_score} dict, sorted by score descending
        """

    def extract_entities(self, text: str, model: str = "dslim/bert-base-NER") -> list[dict]:
        """Named entity recognition.
        Returns: [{entity_group: "PER", word: "...", score: 0.99, start: 0, end: 5}]
        """
```

**Retry strategy**: Align with existing `matching/enrichment/retry_strategy.py` patterns — 3 retries, exponential backoff, 503/429 handling.

**Caching**: MD5 hash of `(model_name, input_text)` as cache key. File-based cache at `Chelsea_clients/hf_cache/` (same pattern as research cache).

### 3.2 `lib/enrichment/embeddings.py` — Embedding Service

**Purpose**: Generate, store, and query profile embeddings.

**Interface**:
```python
class ProfileEmbeddingService:
    EMBEDDING_FIELDS = ['seeking', 'offering', 'who_you_serve', 'what_you_do']

    def embed_profile(self, profile: dict) -> dict[str, list[float]]:
        """Generate embeddings for all text fields of a single profile.
        Returns: {field_name: embedding_vector}
        Skips fields that are None/empty.
        """

    def embed_profiles_batch(self, profiles: list[dict], batch_size: int = 50) -> list[dict]:
        """Batch embed multiple profiles.
        Returns: list of {field_name: embedding_vector} dicts
        """

    def store_embeddings(self, profile_id: str, embeddings: dict[str, list[float]]) -> None:
        """Write embeddings to Supabase profiles table via UPDATE."""

    def cosine_similarity(self, vec_a: list[float], vec_b: list[float]) -> float:
        """Compute cosine similarity between two vectors. Returns 0.0-1.0."""

    def find_similar_profiles(self, profile_id: str, field: str, limit: int = 20) -> list[tuple[str, float]]:
        """Use pgvector to find most similar profiles by a specific field.
        Returns: [(profile_id, similarity_score), ...]
        Uses SQL: SELECT id, 1 - (embedding_seeking <=> target_vector) AS similarity
        """
```

**Supabase write pattern** (aligns with existing `consolidate_to_supabase_batch()`):
```sql
UPDATE profiles SET
    embedding_seeking = $1::vector,
    embedding_offering = $2::vector,
    embedding_who_you_serve = $3::vector,
    embedding_what_you_do = $4::vector,
    embeddings_model = $5,
    embeddings_updated_at = NOW()
WHERE id = $6;
```

### 3.3 `lib/enrichment/classifiers.py` — Classification Service

**Purpose**: Zero-shot classification for niche, offer type, content style.

**Interface**:
```python
class ProfileClassificationService:

    NICHE_LABELS = [
        "Business Coaching & Consulting", "Health & Wellness", "Mindset & Personal Development",
        "Relationships & Dating", "Spirituality & Energy Work", "Fitness & Nutrition",
        "Financial Education & Wealth", "Marketing & Sales", "Leadership & Management",
        "Parenting & Family", "Career Development", "Creative Arts & Expression",
        "Real Estate & Investing", "Technology & SaaS", "Education & Online Learning",
        "Productivity & Performance", "Women's Empowerment", "Mental Health & Therapy",
        "Alternative & Holistic Health", "Grief & Trauma Recovery",
        "Communication & Public Speaking", "Entrepreneurship & Startups",
        "Corporate Training & HR", "Life Coaching (General)", "Service Provider / Agency"
    ]

    OFFER_TYPE_LABELS = [
        "online_course", "coaching_1on1", "group_coaching", "mastermind",
        "done_for_you_service", "software_saas", "book_author", "speaking",
        "consulting", "certification_program", "membership_community",
        "affiliate_network", "agency", "digital_product", "live_events",
        "podcast", "retreat_workshop"
    ]

    def classify_niche(self, profile: dict) -> list[dict]:
        """Classify profile into niche categories.
        Input: concatenated niche + what_you_do + who_you_serve
        Returns: [{"category": "...", "confidence": 0.92}, ...] (top 3, threshold >= 0.3)
        """

    def classify_offer_types(self, profile: dict) -> list[dict]:
        """Classify offering into structured taxonomy.
        Input: concatenated offering + signature_programs + what_you_do
        Returns: [{"type": "online_course", "confidence": 0.88}, ...]
        """

    def classify_content_style(self, profile: dict) -> dict:
        """Classify content/communication style. (Phase C)
        Returns: {"primary": "educational", "secondary": "tactical", "confidence": 0.85}
        """

    def classify_profile_batch(self, profiles: list[dict]) -> list[dict]:
        """Run all classifications on a batch of profiles.
        Returns: list of {niche_categories, offer_types, content_style} dicts
        """
```

### 3.4 `lib/enrichment/credential_extractor.py` — NER-Based Credential Extraction

**Purpose**: Extract structured credentials from `social_proof` and `bio` text.

**Interface**:
```python
class CredentialExtractor:

    def extract_credentials(self, social_proof: str, bio: str = "") -> dict:
        """Extract structured credentials from free text.
        Returns: {
            "books": [{"title": "...", "year": 2023}],
            "media_appearances": ["Forbes", "Inc Magazine", "Entrepreneur"],
            "speaking_events": ["TEDx", "Thrive Summit"],
            "certifications": ["ICF PCC", "NLP Practitioner"],
            "awards": ["Inc 5000"],
            "podcast_appearances": ["The Tim Ferriss Show"],
            "credibility_score": 0.0-1.0
        }
        """

    def compute_credibility_score(self, credentials: dict) -> float:
        """Score based on credential quantity and quality.
        Books: 0.15 each (max 0.3)
        Major media: 0.2 each (max 0.4) — Forbes, Inc, Entrepreneur, NYT, WSJ
        TEDx/TED: 0.25
        Certifications: 0.1 each (max 0.2)
        Base if any credentials: 0.1
        Capped at 1.0
        """
```

---

## 4. Matching Engine Modifications

### 4.1 Modified Synergy Scoring

**File**: `matching/services.py` — `SupabaseMatchScoringService`

**Current** (lines ~1375-1460):
```python
def _compute_synergy_factors(self, source, target):
    factors = []
    factors.append(('offering_seeking', 3, self._text_overlap_score(source.seeking, target.offering)))
    factors.append(('audience_alignment', 2.5, self._text_overlap_score(source.who_you_serve, target.who_you_serve)))
    factors.append(('revenue_compat', 2, self._revenue_tier_score(source, target)))
    factors.append(('platform_overlap', 2, self._platform_overlap_score(source, target)))
    return factors
```

**Modified**:
```python
def _compute_synergy_factors(self, source, target):
    factors = []

    # E1: Semantic similarity (with word-overlap fallback)
    if source.embedding_seeking and target.embedding_offering:
        sim = self._cosine_similarity(source.embedding_seeking, target.embedding_offering)
        factors.append(('offering_seeking_semantic', 3.5, sim * 10))
    else:
        factors.append(('offering_seeking_overlap', 3, self._text_overlap_score(source.seeking, target.offering)))

    if source.embedding_who_you_serve and target.embedding_who_you_serve:
        sim = self._cosine_similarity(source.embedding_who_you_serve, target.embedding_who_you_serve)
        factors.append(('audience_alignment_semantic', 3.0, sim * 10))
    else:
        factors.append(('audience_alignment_overlap', 2.5, self._text_overlap_score(source.who_you_serve, target.who_you_serve)))

    # Keep existing rule-based factors
    factors.append(('revenue_compat', 2.0, self._revenue_tier_score(source, target)))
    factors.append(('platform_overlap', 1.5, self._platform_overlap_score(source, target)))

    # E3: New — offer type compatibility
    if source.offer_types and target.offer_types:
        factors.append(('offer_type_compat', 2.0, self._offer_type_compatibility(source, target)))

    return factors
```

### 4.2 New Synergy Sub-Factor: Offer Type Compatibility

```python
# JV-compatible offer type pairs (bidirectional)
OFFER_TYPE_COMPATIBILITY = {
    ('online_course', 'membership_community'): 9.0,   # Cross-promote
    ('online_course', 'podcast'): 8.5,                 # Guest teaching
    ('coaching_1on1', 'group_coaching'): 8.0,          # Referral pipeline
    ('coaching_1on1', 'online_course'): 7.5,           # Upsell/downsell
    ('book_author', 'podcast'): 9.0,                   # Interview circuit
    ('book_author', 'speaking'): 8.5,                  # Event pipeline
    ('speaking', 'live_events'): 9.0,                  # Stage sharing
    ('consulting', 'done_for_you_service'): 7.0,       # Referral
    ('membership_community', 'podcast'): 8.0,          # Content pipeline
    ('digital_product', 'affiliate_network'): 9.0,     # Affiliate JV
    ('mastermind', 'coaching_1on1'): 7.5,              # Tier progression
    ('certification_program', 'coaching_1on1'): 8.0,   # Graduate pipeline
    ('retreat_workshop', 'coaching_1on1'): 7.5,        # Upsell path
}

def _offer_type_compatibility(self, source, target) -> float:
    """Score compatibility based on offer type pairs."""
    source_types = {ot['type'] for ot in (source.offer_types or []) if ot.get('confidence', 0) >= 0.5}
    target_types = {ot['type'] for ot in (target.offer_types or []) if ot.get('confidence', 0) >= 0.5}

    if not source_types or not target_types:
        return 5.0  # Neutral when data is missing

    best_score = 0
    for s_type in source_types:
        for t_type in target_types:
            pair = tuple(sorted([s_type, t_type]))
            score = OFFER_TYPE_COMPATIBILITY.get(pair, 4.0)
            best_score = max(best_score, score)

    # Bonus for shared offer types (can do reciprocal JVs)
    shared = source_types & target_types
    if shared:
        best_score = max(best_score, 7.0)

    return best_score
```

### 4.3 New Context Sub-Factor: Credibility Score

```python
# Add to _compute_context_factors()
if hasattr(target, 'credentials') and target.credentials:
    cred_score = target.credentials.get('credibility_score', 0) * 10
    factors.append(('credibility', 2.0, cred_score))
```

---

## 5. Pipeline Integration

### 5.1 Integration into `automated_enrichment_pipeline_safe.py`

Add as Step 6, after `consolidate_to_supabase_batch()`:

```python
# After existing Step 5 (Supabase write)
async def _run_hf_enrichment(self, profiles_written: list[dict]):
    """Step 6: HF classification + embedding generation."""
    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.embeddings import ProfileEmbeddingService
    from lib.enrichment.classifiers import ProfileClassificationService

    hf = HFClient()
    embedder = ProfileEmbeddingService(hf)
    classifier = ProfileClassificationService(hf)

    for profile in profiles_written:
        # Generate embeddings
        embeddings = embedder.embed_profile(profile)
        embedder.store_embeddings(profile['id'], embeddings)

        # Run classifications
        classifications = classifier.classify_niche(profile)
        offer_types = classifier.classify_offer_types(profile)

        # Write to Supabase
        update_data = {
            'niche_categories': classifications,
            'offer_types': offer_types,
            'classification_updated_at': datetime.utcnow().isoformat(),
        }
        supabase.table('profiles').update(update_data).eq('id', profile['id']).execute()

        # Update provenance
        # ... (follow existing enrichment_metadata.field_meta pattern)
```

### 5.2 Backfill Management Command

**File**: `matching/management/commands/backfill_hf_enrichment.py`

```bash
# Usage
python manage.py backfill_hf_enrichment --type embeddings --batch-size 50
python manage.py backfill_hf_enrichment --type classifications --batch-size 100
python manage.py backfill_hf_enrichment --type all --batch-size 50
python manage.py backfill_hf_enrichment --type all --dry-run  # Preview without writing
```

---

## 6. Verification Gate Update

### Add Classification Confidence Check to Layer 1

**File**: `matching/enrichment/verification_gate.py` — `DeterministicChecker`

```python
# Add new check method
def check_classification_confidence(self, classifications: list[dict], field_name: str) -> list:
    """Verify classification results meet minimum confidence threshold."""
    issues = []
    if not classifications:
        return issues  # Empty is fine — just means not yet classified

    for item in classifications:
        if item.get('confidence', 0) < 0.3:
            issues.append({
                'field': field_name,
                'severity': 'info',
                'message': f"Low confidence classification: {item.get('category', item.get('type'))} ({item['confidence']:.2f})"
            })

    # Check for suspiciously uniform distribution (model uncertainty)
    if len(classifications) >= 3:
        scores = [c['confidence'] for c in classifications]
        score_range = max(scores) - min(scores)
        if score_range < 0.1:
            issues.append({
                'field': field_name,
                'severity': 'warning',
                'message': f"Classification confidence scores are suspiciously uniform (range: {score_range:.2f}) — model may be uncertain"
            })

    return issues
```

---

## 7. Source Priority Integration

Add HF enrichment to the existing source priority hierarchy in `scripts/automated_enrichment_pipeline_safe.py`:

```python
SOURCE_PRIORITY = {
    'client_confirmed': 100,
    'client_ingest': 90,
    'manual_edit': 80,
    'exa_research': 50,
    'hf_classification': 45,   # NEW — between Exa and AI research
    'hf_embedding': 45,        # NEW — computed, not extracted
    'ai_research': 40,
    'apollo': 30,
    'unknown': 0,
}
```

Note: HF classifications at priority 45 means they won't overwrite Exa-extracted niche data, but will overwrite keyword-regex-assigned categories (which have no source priority — treated as 'unknown'/0).

---

## 8. Testing Strategy

### Unit Tests

```python
# matching/tests/test_hf_enrichment.py

class TestEmbeddingService:
    def test_embed_profile_returns_correct_dimensions(self):
        """Embedding vectors should be 384-dim for MiniLM."""

    def test_cosine_similarity_identical_texts(self):
        """Identical texts should have similarity ~1.0."""

    def test_cosine_similarity_unrelated_texts(self):
        """Unrelated texts should have similarity < 0.3."""

    def test_semantic_similarity_synonyms(self):
        """'business growth coach' and 'company scaling mentor' should score > 0.7."""

    def test_fallback_when_hf_unavailable(self):
        """Should return None (not raise) when HF API is down."""

class TestClassificationService:
    def test_niche_classification_known_categories(self):
        """'Leadership coaching for Fortune 500 executives' → 'Leadership & Management'."""

    def test_offer_type_multi_label(self):
        """Profile with courses AND coaching should return both labels."""

    def test_low_confidence_filtering(self):
        """Results below 0.3 confidence should be excluded."""

class TestMatchScoringWithEmbeddings:
    def test_semantic_synergy_beats_word_overlap(self):
        """Semantic scoring should rank synonym-pairs higher than word overlap does."""

    def test_graceful_fallback_without_embeddings(self):
        """Scoring should work with word overlap when embeddings are null."""

    def test_offer_type_compatibility_scoring(self):
        """Course creator + podcast host should score high compatibility."""
```

### Integration Tests (require HF_API_TOKEN)

```python
class TestHFIntegration:
    @pytest.mark.skipif(not os.environ.get('HF_API_TOKEN'), reason="No HF token")
    def test_live_embedding_generation(self):
        """End-to-end: text → HF API → embedding vector."""

    @pytest.mark.skipif(not os.environ.get('HF_API_TOKEN'), reason="No HF token")
    def test_live_classification(self):
        """End-to-end: profile text → HF API → niche categories."""
```

---

## 9. Monitoring & Observability

### Metrics to Track

```python
# Add to HFClient
HF_METRICS = {
    'hf_api_calls_total': 0,
    'hf_api_errors_total': 0,
    'hf_api_latency_ms': [],
    'hf_cache_hits': 0,
    'hf_cache_misses': 0,
    'embeddings_generated': 0,
    'classifications_run': 0,
}
```

### Logging Pattern (align with existing pipeline logging)

```python
import logging
logger = logging.getLogger('enrichment.hf')

# Per-profile
logger.info(f"HF enrichment complete for {profile['name']}: "
            f"embeddings={len(embeddings)}, "
            f"niche_categories={len(niche_cats)}, "
            f"offer_types={len(offer_types)}, "
            f"cost=${cost:.5f}")

# Per-batch summary
logger.info(f"HF batch complete: {count} profiles, "
            f"{HF_METRICS['hf_api_calls_total']} API calls, "
            f"{HF_METRICS['hf_cache_hits']} cache hits, "
            f"total_cost=${total_cost:.4f}")
```
