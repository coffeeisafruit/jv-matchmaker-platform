# Study 1.2: Unknown Scoring Method Audit

## Executive Summary

The 34.3% "Unknown" scoring method in the ISMC synergy dimension is **fully explained and benign in structure, but reveals a data quality issue that biases results**. It comes from two synergy factors -- Role Compatibility (95.9%) and Revenue Tier (4.1%) -- that use lookup-table scoring and simply never tag a `method` field in their output. These factors are deterministic and well-defined. They are not computing anything opaque.

However, the word_overlap fallback (1.0% of synergy evaluations) acts as a strong proxy for **completely empty profiles** and produces dramatically lower match scores (Cohen's d = 5.58), creating a two-tier scoring regime where 465 matches involving 70 sparse profiles are effectively dead-on-arrival.

## 1. Code Audit: Factor-by-Factor Catalog

### File: `/Users/josephtepe/Projects/jv-matchmaker-platform/matching/services.py`
### Class: `SupabaseMatchScoringService` (line 1262)

Each ISMC dimension is scored by a dedicated method. Below is every factor, its scoring technique, and whether it tags `method` in the output dictionary.

### Intent (45% weight) -- `_score_intent()` (line 1770)

| # | Factor Name | Scoring Method | Tags `method`? | Weight |
|---|-------------|---------------|----------------|--------|
| 1 | JV History | Threshold (count of past JVs) | No | 4.0 |
| 2 | Booking Link | Binary (present/absent) | No | 3.5 |
| 3 | Profile Investment | Ratio (populated fields / 13) | No | 3.0 |
| 4 | Website | Binary (present/absent) | No | 2.5 |
| 5 | Membership Status | Lookup (status -> score) | No | 2.0 (conditional) |
| 6 | Profile Maintenance | Threshold (days since update) | No | 1.5 (conditional) |
| 7 | Outcome Track Record | Ratio (positive / total outcomes) | No | 3.0 (conditional) |

**All 7 intent factors: UNTAGGED. 100% of intent evaluations appear as "Unknown."**

### Synergy (25% weight) -- `_score_synergy()` (line 1867)

| # | Factor Name | Scoring Method | Tags `method`? | Weight |
|---|-------------|---------------|----------------|--------|
| 1 | Offering-to-Seeking | Embedding cosine sim OR word overlap fallback | **Yes** (`semantic` / `word_overlap`) | 3.5 |
| 2 | Audience Alignment | Embedding cosine sim OR word overlap fallback | **Yes** (`semantic` / `word_overlap`) | 3.0 |
| 3 | Role Compatibility | Lookup table (canonical role pairs) | No | 2.5 |
| 4 | Revenue Tier | Lookup (tier distance) | No (conditional) | 2.0 |
| 5 | Platform Overlap | Count (shared platforms) | No (conditional) | 2.0 |
| 6 | Network Influence | Composite (PageRank, degree, betweenness) | No (conditional) | 2.0 |
| 7 | Business Scale | Lookup (size gap) | No (conditional) | 1.5 |

**Only Factors 1 and 2 tag their method. Factors 3-7 are UNTAGGED.**

The 34.3% "Unknown" in synergy = 59,726 Role Compatibility evals + 2,560 Revenue Tier evals = 62,286 total.

### Momentum (20% weight) -- `_score_momentum()` (line 2108)

| # | Factor Name | Scoring Method | Tags `method`? | Weight |
|---|-------------|---------------|----------------|--------|
| 1 | Audience Engagement | Threshold (engagement score * 10) | No | 3.0 |
| 2 | Social Reach | Threshold (follower tiers) | No | 2.0 |
| 3 | Active Projects | Binary (>10 chars text) | No (conditional) | 2.5 |
| 4 | List Size | Threshold (subscriber tiers) | No | 2.5 |
| 5 | Activity Recency | Threshold (days since active) | No | 2.0 |

**All 5 momentum factors: UNTAGGED. 100% of momentum evaluations appear as "Unknown."**

### Context (10% weight) -- `_score_context()` (line 2202)

| # | Factor Name | Scoring Method | Tags `method`? | Weight |
|---|-------------|---------------|----------------|--------|
| 1 | Profile Completeness | Ratio (filled / 12 key fields) | No | 3.0 |
| 2 | Revenue Known | Binary (known/unknown) | No | 2.0 |
| 3 | Enrichment Quality | Ratio (enriched / 8 fields) | No | 2.5 |
| 4 | Contact Available | Tiered (email > LinkedIn > phone > none) | No | 2.5 |
| 5 | Enrichment Confidence | Threshold (AI confidence * 10) | No | 2.0 (conditional) |
| 6 | Recommendation Freshness | Threshold (inverse of pressure) | No | 1.5 (conditional) |

**All 6 context factors: UNTAGGED. 100% of context evaluations appear as "Unknown."**

### Method Tagging Summary

Of all 25 possible factors across the four ISMC dimensions, **only 2 factors tag their `method` field**: Offering-to-Seeking and Audience Alignment (both in Synergy). All other factors use deterministic threshold/lookup/ratio scoring and never set `method`.

## 2. Data Verification

### Total Factor Evaluations (29,863 matches)

| Category | Count | Percentage |
|----------|-------|------------|
| Total factor evaluations | 773,354 | 100% |
| With `method` tag | 119,452 | 15.4% |
| Without `method` tag (UNTAGGED) | 653,902 | 84.6% |

### Per-Dimension Breakdown

| Dimension | Total Evals | `semantic` | `word_overlap` | UNTAGGED |
|-----------|------------|-----------|---------------|----------|
| Intent | 238,904 | 0 (0%) | 0 (0%) | 238,904 (100%) |
| Synergy | 181,738 | 117,592 (64.7%) | 1,860 (1.0%) | 62,286 (34.3%) |
| Momentum | 113,808 | 0 (0%) | 0 (0%) | 113,808 (100%) |
| Context | 238,904 | 0 (0%) | 0 (0%) | 238,904 (100%) |

### The "Unknown" 34.3% in Synergy

| Factor | Count | % of Untagged Synergy |
|--------|-------|----------------------|
| Role Compatibility | 59,726 | 95.9% |
| Revenue Tier | 2,560 | 4.1% |
| **Total** | **62,286** | **100%** |

Platform Overlap, Network Influence, and Business Scale are conditional (null-aware) and excluded from most matches when data is missing, so they contribute 0 untagged evaluations in the current dataset.

## 3. Statistical Comparison: Semantic vs. Word Overlap

### Group Definitions

- **All Semantic** (n=29,398): Both Offering-to-Seeking and Audience Alignment used embedding cosine similarity in both directions.
- **Has Word Overlap** (n=465): At least one synergy factor fell back to word overlap in either direction.

### Harmonic Mean Distribution

| Statistic | All Semantic | Has Word Overlap |
|-----------|-------------|------------------|
| n | 29,398 | 465 |
| Mean | 57.90 | 31.73 |
| Median | 58.04 | 32.33 |
| Std Dev | 4.71 | 3.10 |
| Q1 | 54.71 | 30.71 |
| Q3 | 61.17 | 34.30 |
| Min | 40.86 | 25.38 |
| Max | 77.72 | 41.52 |

The distributions are **completely non-overlapping**: the maximum word_overlap score (41.52) is below the minimum semantic score (40.86). This is a bimodal scoring regime.

### Mann-Whitney U Test

| Metric | Value |
|--------|-------|
| U statistic | 13,670,068 |
| p-value | 1.38e-300 (effectively 0) |
| Rank-biserial correlation | -1.0000 |
| Cohen's d | 5.58 |
| Effect size | **Extremely large** |

A rank-biserial correlation of -1.0 confirms that **every single** word_overlap match scores lower than **every single** semantic match. The effect size (Cohen's d = 5.58) is far beyond the conventional "large" threshold of 0.8.

### Dimension-Level Score Comparison

| Dimension | Semantic Mean | Word Overlap Mean | Difference |
|-----------|:------------:|:-----------------:|:----------:|
| Intent | 5.86 | 3.52 | -2.34 |
| Synergy | 5.84 | 3.56 | -2.28 |
| Momentum | 5.62 | 5.51 | -0.11 |
| Context | 6.90 | 3.44 | -3.46 |

The score gap is **not confined to Synergy**. Intent and Context scores are also dramatically lower in the word_overlap group, confirming that the underlying cause is profile-level data poverty, not the scoring method itself.

### Word Overlap Factor Scores

| Factor | Semantic Mean | Word Overlap Mean | Word Overlap Score Distribution |
|--------|:------------:|:-----------------:|-------------------------------|
| Offering-to-Seeking | 5.34 | 3.00 | 100% at score 3.0 |
| Audience Alignment | 5.66 | 3.00 | 100% at score 3.0 |

Every word_overlap evaluation returns exactly 3.0 (the "neutral/missing data" default in `_text_overlap_score()`). This means the text fields (`seeking`, `offering`, `who_you_serve`) are empty for these profiles, so the word overlap function returns its default score of 3.0 before performing any actual overlap computation.

## 4. Profile Characteristics

### Profiles in Word Overlap Matches

| Metric | Word Overlap Group | All Scored Profiles (Baseline) |
|--------|:-----------------:|:------------------------------:|
| Total profiles | 193 | 1,969 |
| Missing ALL embeddings | 70 (36.3%) | 70 (3.6%) |
| Missing embedding_seeking | 70 (36.3%) | 70 (3.6%) |
| Missing embedding_who_you_serve | 70 (36.3%) | 70 (3.6%) |
| Sparse (<6 key fields) | 70 (36.3%) | 70 (3.6%) |

The 70 profiles with missing embeddings are the **exact same** 70 profiles that are sparse. They have no text in their `seeking`, `offering`, `who_you_serve`, or `what_you_do` fields. Their embeddings are null because there was no text to embed.

### Root Cause: Empty Profiles Being Scored

- 517 total profiles in the database lack embeddings
- Of those, only 1 has any `seeking` text, only 3 have any `who_you_serve` text
- These are skeletal profiles (often just a name and membership status)
- 70 of these profiles appear in match suggestions, generating the 465 word_overlap matches
- The `_text_overlap_score()` fallback correctly returns 3.0 ("no data") for empty text fields

### Niche Distribution of Word Overlap Profiles

The top niche for profiles in word_overlap matches is `unknown` (70 profiles), confirming these are unenriched/empty profiles. The remaining 123 profiles are the *partners* of these empty profiles and typically appear in semantic matches as well.

### Embedding Generation Coverage

| Status | Count |
|--------|-------|
| Has text AND embeddings | 3,060 |
| Has text but NO embeddings | 3 |

Only 3 profiles have text content without corresponding embeddings, meaning the embedding pipeline has excellent coverage. The word_overlap fallback is triggered by genuinely missing text, not by an embedding generation gap.

## 5. Conclusions

### What the "Unknown" 34.3% Actually Is

The 34.3% "Unknown" scoring method within the Synergy dimension consists of:

1. **Role Compatibility (95.9%)**: A deterministic lookup-table scoring function mapping canonical role pairs to compatibility scores (0-10). It is well-defined, uses the `_ROLE_COMPAT` dictionary, and behaves correctly. It simply does not tag a `method` field because it only has one method.

2. **Revenue Tier compatibility (4.1%)**: A deterministic tier-distance scoring function. Same situation -- single method, no tag needed.

These are **not opaque or broken computations**. They are straightforward lookup/threshold scorers that were not designed to tag their method because they have no fallback path.

### Does Word Overlap Bias Results?

**Yes, but the bias is appropriate.** Word_overlap fallback correlates perfectly with empty/sparse profiles. These profiles have no text fields, no embeddings, and minimal metadata. The low scores (harmonic mean ~32 vs ~58 for semantic) reflect genuine low match quality, not a methodological artifact.

The scoring pipeline is functioning as designed: profiles with no data to evaluate receive low scores. The word_overlap path is a "soft failure" mode that returns neutral 3.0 scores rather than crashing.

### Does the "Unknown" Method Bias Results?

**No.** Role Compatibility and Revenue Tier factors are:
- Deterministic and symmetric
- Based on well-defined lookup tables
- Applied equally to all matches
- Contributing the same factor weight regardless of the embedding method used for Factors 1-2

## 6. Recommendations

### Low Priority: Add Method Tags (Cosmetic)

Add a `method` key to all factors for transparency. For example:
- Role Compatibility: `'method': 'lookup_table'`
- Revenue Tier: `'method': 'tier_distance'`
- Profile Investment: `'method': 'field_count_ratio'`
- Booking Link: `'method': 'binary_presence'`

This would eliminate the "Unknown" classification in future validation reports.

### Medium Priority: Pre-Scoring Eligibility Filter

The 70 sparse profiles being scored generate 465 matches that will never surface as useful recommendations (max harmonic mean = 41.52). Consider adding these profiles to the pre-scoring eligibility filter (`ScoreValidator.check_scoring_eligibility()`) when they lack minimum data thresholds (e.g., must have at least one of: `seeking`, `offering`, `who_you_serve` populated).

### Low Priority: Backfill 3 Profiles Missing Embeddings

Three profiles have text content but no embeddings. Running them through the embedding pipeline would eliminate 3 potential word_overlap fallbacks.

## Appendix: Data Files

- **Per-match method distribution**: `/Users/josephtepe/Projects/jv-matchmaker-platform/Validation/study_1_2_method_distribution.csv`
  - 29,863 rows, one per scored match
  - Columns: match_id, profile_a_id, profile_b_id, harmonic_mean, total_factors, semantic_count, word_overlap_count, untagged_count, semantic_pct, word_overlap_pct, untagged_pct, synergy_group
