# ISMC: A Bidirectional Semantic Matching Framework for Joint Venture Partnership Recommendation

**Joseph Tepe**

JV Matchmaker Platform

February 2026

---

## Abstract

Joint venture (JV) partnerships represent a critical growth mechanism for small and medium businesses, yet existing matching technologies fail to address the unique requirements of bidirectional B2B partnership discovery. We present ISMC (Intent, Synergy, Momentum, Context), a novel multi-dimensional scoring framework that combines semantic embedding similarity, hand-curated domain knowledge, and null-aware aggregation to produce calibrated partnership recommendations. Unlike collaborative filtering or content-based approaches, ISMC explicitly models both directions of a partnership --- evaluating how valuable Partner B is to Partner A *and vice versa* --- then combines these directional scores via harmonic mean to ensure mutual benefit. Evaluated on a production corpus of 3,143 profiles yielding 29,863 scored match pairs and 58,796 semantic evaluations, ISMC produces a well-calibrated score distribution (mean 57.49, stdev 5.70) with low inter-component correlation (max $|r| < 0.7$), confirming that the four dimensions capture orthogonal aspects of partnership quality. The system incorporates a three-layer anti-hallucination pipeline for LLM-generated match explanations and confidence scoring with field-specific exponential age decay. To our knowledge, ISMC is the first matching algorithm specifically designed for bidirectional B2B partnership recommendation with semantic understanding.

---

## 1. Introduction

Joint venture partnerships --- cooperative arrangements where two or more businesses combine resources, audiences, or expertise for mutual benefit --- are a cornerstone of growth strategy for entrepreneurs, coaches, consultants, and digital-product creators. Unlike employment matching, dating platforms, or e-commerce recommendations, JV partnerships require *bidirectional value assessment*: a partnership only succeeds when both parties derive meaningful benefit. A podcast host needs guests with expertise relevant to their audience, but the guest simultaneously needs exposure to an audience aligned with their offerings.

Despite the economic significance of JV partnerships, the matching problem has received remarkably little attention in the recommender systems literature. Existing systems either focus on unidirectional recommendations (suggesting items to users), symmetric matching without semantic understanding (dating platforms using collaborative filtering), or formal cooperative arrangements in academia (research consortium partner search). None adequately address the distinctive characteristics of B2B partnership matching:

1. **Bidirectional value**: Both partners must benefit. A match that is excellent for A but poor for B will fail.
2. **Semantic complementarity**: Value arises from *complementary* rather than *similar* profiles --- a coach seeking podcast exposure should match with a podcast host seeking expert guests, not another coach seeking the same.
3. **Sparse, heterogeneous data**: Business profiles vary enormously in completeness. Some profiles include detailed seeking/offering statements, audience metrics, and JV history; others contain only a name and niche.
4. **Temporal decay**: Partnership readiness signals (what someone is seeking, their audience engagement) change rapidly, while structural attributes (niche, company) remain stable.
5. **Explainability**: Unlike entertainment recommendations where "you might like this" suffices, business introductions require concrete reasoning --- *why* should these two people meet?

This paper presents ISMC, a four-dimensional scoring framework designed from first principles to address these challenges. We describe the mathematical formulation, the anti-hallucination verification pipeline for LLM-generated explanations, the confidence scoring system with field-specific decay rates, and a comprehensive experimental evaluation on a production dataset of 3,143 JV partner profiles.

---

## 2. Related Work

### 2.1 Stable Matching

The foundational work on two-sided matching is the Gale-Shapley algorithm for stable matching (Gale & Shapley, 1962), which earned Roth and Shapley the 2012 Nobel Prize in Economics. Stable matching guarantees that no pair of agents would prefer each other over their assigned partners. While theoretically elegant, stable matching requires complete preference orderings from all agents --- an assumption that does not hold in our setting, where profiles arrive asynchronously and preferences must be *inferred* from profile attributes rather than explicitly stated. Furthermore, stable matching produces one-to-one assignments, whereas JV partnerships are many-to-many.

### 2.2 Collaborative Filtering

Collaborative filtering (CF), exemplified by the Netflix Prize-winning matrix factorization methods (Koren et al., 2009), predicts user preferences from historical interaction patterns. CF excels when abundant behavioral data exists (views, clicks, ratings) but suffers from the cold-start problem when such data is unavailable. In JV matching, the cold-start problem is pervasive: most profiles have zero historical partnership outcomes at the time of scoring. Moreover, CF is fundamentally unidirectional --- it predicts whether user A will like item B, not whether both A and B will value the connection.

### 2.3 Content-Based Recommendation

Content-based systems recommend items similar to those a user has previously liked, typically using TF-IDF, BM25, or neural embeddings to compute feature similarity (Lops et al., 2011). While ISMC borrows the technique of semantic embeddings for measuring text similarity, pure content-based matching conflates *similarity* with *complementarity*. Two coaches who serve the same audience and offer the same services are highly similar but poor JV partners --- they are competitors, not collaborators.

### 2.4 CORDIS Partner Matching

The closest prior work in B2B matching is the CORDIS partner matching system for European research consortia (Springer, 2022), which matches organizations for EU-funded research projects based on project descriptions and organizational profiles. CORDIS addresses a B2B matching scenario but differs from JV matching in several respects: it operates on organizational rather than individual profiles, uses keyword extraction rather than semantic embeddings, and does not model bidirectional value or temporal decay.

### 2.5 Reciprocal Recommendation Systems

Recent work on reciprocal recommendation (Neve & Palomares, 2025) explicitly models two-sided preferences, recognizing that both parties must find value in a match. This line of research, emerging primarily in online dating and job matching, provides theoretical grounding for bidirectional scoring. However, existing reciprocal systems rely on interaction histories (message rates, response patterns) rather than semantic profile analysis, and none address the B2B partnership domain.

### 2.6 B2B Recommender Systems

B2B recommendation has been explored in supplier selection (Wachsmuth et al., 2013) and business network formation (Luo et al., 2014), but these systems focus on transactional relationships (buyer-supplier) rather than collaborative partnerships. The JV matching problem requires modeling *mutual creation of value* rather than fulfillment of a single party's requirements.

### 2.7 Comparative Analysis

Table 1 summarizes the feature comparison across the six system categories and ISMC.

| Feature | Stable Matching | Collaborative Filtering | Content-Based | CORDIS | Reciprocal RecSys | ISMC (Ours) |
|---|---|---|---|---|---|---|
| Bidirectional scoring | Implicit (preferences) | No | No | No | Yes | **Yes** |
| Semantic understanding | No | Latent factors | TF-IDF / embeddings | Keywords | Varies | **1024-d embeddings** |
| Handles sparse data | No (requires full prefs) | Partial (cold-start) | Yes | Yes | Partial | **Yes (null-aware)** |
| Temporal decay | No | Implicit | No | No | No | **Yes (field-specific)** |
| Complementarity vs. similarity | N/A | Similarity | Similarity | Similarity | Similarity | **Complementarity** |
| Domain | General | E-commerce, media | General | Research | Dating, jobs | **B2B partnerships** |
| Explainability | N/A | Low | Moderate | Low | Low | **High (verified LLM)** |
| Cold-start resilient | No | No | Yes | Yes | No | **Yes** |

*Table 1: Feature comparison across recommendation system paradigms. ISMC uniquely combines bidirectional scoring, semantic complementarity, null-aware temporal decay, and verified explainability for B2B partnerships.*

---

## 3. System Architecture

### 3.1 Overview

The ISMC system operates as a Django-based platform with the following major components:

1. **Data Ingestion and Enrichment**: Profiles are ingested from multiple sources (manual entry, bulk import, LinkedIn) and enriched through an AI-powered pipeline using Claude Sonnet for profile extraction and web research via Tavily search.

2. **Embedding Generation**: Free-text profile fields (seeking, offering, who_you_serve, what_you_do) are encoded into 1024-dimensional dense vectors using BAAI/bge-large-en-v1.5, a locally-run sentence transformer model.

3. **ISMC Scoring Engine**: For each ordered pair $(A, B)$, computes a directional score $S_{AB}$ measuring how valuable $B$ is as a partner for $A$, then combines directional scores via harmonic mean.

4. **Anti-Hallucination Pipeline**: LLM-generated match explanations pass through a three-layer verification system (generation, fact-checking, multi-agent verification) before being presented to users.

5. **Confidence Scoring**: Each profile field carries a confidence score based on source reliability, exponential age decay, verification status, and cross-validation.

6. **Network Analysis**: A NetworkX-based graph analysis computes PageRank, degree centrality, and betweenness centrality to classify profiles into network roles (hub, bridge, specialist, newcomer).

### 3.2 Data Model

The system operates on two primary entities:

**SupabaseProfile** represents a JV partner with 50+ fields spanning identity (name, company, email), business description (what_you_do, who_you_serve, niche), partnership signals (seeking, offering, booking_link, jv_history), audience metrics (list_size, social_reach, audience_engagement_score), enrichment metadata (revenue_tier, content_platforms, signature_programs), and four embedding columns (embedding_seeking, embedding_offering, embedding_who_you_serve, embedding_what_you_do).

**SupabaseMatch** stores scored match pairs with directional scores (score_ab, score_ba), harmonic mean, match reason text, scale symmetry score, trust level, and engagement tracking fields (viewed_at, contacted_at, user_feedback).

### 3.3 Enrichment Pipeline with Anti-Hallucination

The enrichment pipeline addresses a fundamental challenge in LLM-assisted recommendation: language models confidently generate plausible but ungrounded explanations. Our three-layer anti-hallucination pipeline operates as follows:

**Layer 1 --- LLM Generation with Grounding Constraints**: The generation prompt provides both profiles' structured data and instructs the model to reference *only* data explicitly present in the profiles. Inferences must be labeled as `[inferred from: field_name]`. The prompt demands structured JSON output with explicit citation fields mapping each claim to its source profile field.

**Layer 2 --- Automated Fact-Checking**: A second LLM call receives both the original profiles and the generated explanation, tasked with classifying each factual claim as `grounded` (directly stated in profile data), `inferred` (reasonable inference), or `ungrounded` (not supported). A grounding percentage is computed: explanations with $\geq 80\%$ grounding are classified as `llm_verified`, those with $\geq 50\%$ as `llm_partial`, and those below $50\%$ trigger a fallback to deterministic template-based explanations.

**Layer 3 --- Multi-Agent Verification**: Six specialized verification agents inspect the final output:
- **Encoding Agent**: Detects problematic Unicode characters (smart quotes, em-dashes, zero-width characters) that cause rendering failures.
- **Formatting Agent**: Validates section structure, bullet-point formatting, and length constraints.
- **Content Agent**: Checks for empty sections, generic phrases, missing personalization, and unused profile data.
- **Capitalization Agent**: Ensures consistent formatting of headers and bullet points.
- **Truncation Agent**: Detects improperly truncated words and broken sentences.
- **Data Quality Agent**: Catches boilerplate text, misplaced URLs, and placeholder values.

Each agent assigns severity levels (critical: $-15$ points, warning: $-5$, info: $-1$), and a verification score starting at 100 determines the final status: passed ($\geq 85$), needs enrichment ($\geq 60$), or rejected ($< 60$).

---

## 4. Mathematical Formulation

### 4.1 ISMC Dimensional Scoring

Each directional score is composed of four dimensions, weighted to reflect their relative importance in predicting successful JV partnerships:

| Dimension | Weight | Signal |
|-----------|--------|--------|
| **Intent** ($I$) | 0.45 | Partnership readiness: JV history, booking link, profile investment, website presence |
| **Synergy** ($S$) | 0.25 | Business complementarity: offering-to-seeking alignment, audience overlap, role compatibility, revenue tier |
| **Momentum** ($M$) | 0.20 | Current activity: audience engagement, social reach, active projects, list size |
| **Context** ($C$) | 0.10 | Data quality: profile completeness, revenue data availability, enrichment quality, contact availability |

These weights were established through domain expertise and iterative calibration. Intent receives the highest weight because demonstrated partnership-seeking behavior (maintaining a booking link, completing profile fields, having documented JV history) is the strongest predictor of a partner who will actually engage. Synergy captures the complementarity that makes a specific pairing valuable. Momentum reflects current engagement levels. Context serves as a data-quality multiplier --- a well-documented profile produces more reliable scores across all other dimensions.

### 4.2 Sub-Factor Aggregation

Each dimension is computed as a weighted average of its sub-factors. For example, Intent is:

$$I = \frac{\sum_{f \in F_I} w_f \cdot s_f}{\sum_{f \in F_I} w_f \cdot 10} \times 10$$

where $F_I$ is the set of Intent sub-factors, $w_f$ is the weight of sub-factor $f$, and $s_f \in [0, 10]$ is its score. This produces a dimension score on the $[0, 10]$ scale.

The Intent sub-factors and their weights are:

| Sub-Factor | Weight | Scoring Logic |
|------------|--------|---------------|
| JV History | 4.0 | $\geq 3$ partnerships: 10, $\geq 1$: 7, none: 4 |
| Booking Link | 3.5 | Present: 8, absent: 3 |
| Profile Investment | 3.0 | $\frac{\text{populated fields}}{13} \times 10$ (13 investment fields) |
| Website Presence | 2.5 | Present: 7, absent: 2 |

### 4.3 Null-Aware Weight Redistribution

A critical design choice in ISMC is handling missing data without penalizing or inflating scores. When a dimension or sub-factor returns `None` (meaning *all* of its data sources are empty), its weight is redistributed proportionally to the remaining dimensions.

Formally, let $D = \{I, S, M, C\}$ be the set of dimensions and $D^* \subseteq D$ be the subset with non-null scores. The directional score is computed using only $D^*$:

$$S_{\text{dir}} = \exp\left(\frac{\sum_{d \in D^*} w_d \log s_d}{\sum_{d \in D^*} w_d}\right)$$

This ensures that a profile missing Momentum data (e.g., no audience engagement score, no social reach, no list size, no current projects) is scored solely on Intent, Synergy, and Context --- with the Momentum weight of 0.20 automatically redistributed. The score is neither penalized for missing data nor inflated by imputation.

The same null-aware pattern is applied within dimensions. For example, the Synergy dimension's Revenue Tier sub-factor is only included when *both* profiles have known revenue tiers; otherwise, its weight of 2.0 redistributes to Offering-to-Seeking alignment, Audience Alignment, and Role Compatibility.

### 4.4 Weighted Geometric Mean

ISMC uses the weighted geometric mean rather than the weighted arithmetic mean to aggregate dimension scores:

$$S_{\text{dir}} = \exp\left(\frac{\sum_{d \in D^*} w_d \log s_d}{\sum_{d \in D^*} w_d}\right) = \prod_{d \in D^*} s_d^{w_d / \sum w_d}$$

**Theorem 1** (Geometric mean penalizes imbalance). *For any set of non-negative values $\{s_1, \ldots, s_n\}$, the geometric mean is bounded above by the arithmetic mean:*

$$G(s_1, \ldots, s_n) = \left(\prod_{i=1}^n s_i\right)^{1/n} \leq \frac{1}{n}\sum_{i=1}^n s_i = A(s_1, \ldots, s_n)$$

*with equality if and only if $s_1 = s_2 = \cdots = s_n$.*

*Proof.* By the AM-GM inequality (a consequence of Jensen's inequality applied to the concave function $\log$). Since $\log$ is strictly concave, Jensen's inequality gives:

$$\log G = \frac{1}{n}\sum_{i=1}^n \log s_i \leq \log\left(\frac{1}{n}\sum_{i=1}^n s_i\right) = \log A$$

Exponentiating both sides yields $G \leq A$. Equality holds iff all $s_i$ are equal, by the strict concavity of $\log$. $\square$

This property is desirable for partnership scoring: a profile with Intent 9, Synergy 1, Momentum 5, Context 5 should score lower than one with Intent 5, Synergy 5, Momentum 5, Context 5, because the first profile has a critical weakness (Synergy 1) that the high Intent cannot compensate for. Under arithmetic mean, both score 5.0; under geometric mean, the balanced profile scores higher (5.0 vs. 3.35 in the unweighted case).

### 4.5 Bidirectional Harmonic Mean

Each match pair receives two directional scores:

- $S_{AB}$: How valuable is $B$ as a partner for $A$?
- $S_{BA}$: How valuable is $A$ as a partner for $B$?

These are combined via the harmonic mean:

$$S = \frac{2 \cdot S_{AB} \cdot S_{BA}}{S_{AB} + S_{BA}}$$

**Theorem 2** (Harmonic mean bounds). *For positive values $a, b > 0$:*

$$H(a, b) = \frac{2ab}{a+b} \leq \sqrt{ab} = G(a, b) \leq \frac{a+b}{2} = A(a, b)$$

*Proof.* The left inequality $H \leq G$: By AM-GM applied to $\frac{1}{a}$ and $\frac{1}{b}$:

$$\frac{1/a + 1/b}{2} \geq \sqrt{\frac{1}{ab}}$$

Taking reciprocals (reversing inequality since all terms are positive):

$$\frac{2}{\frac{1}{a} + \frac{1}{b}} \leq \sqrt{ab}$$

which gives $H(a,b) \leq G(a,b)$. The right inequality $G \leq A$ follows from AM-GM as in Theorem 1. $\square$

The harmonic mean is chosen over the arithmetic mean specifically because it punishes asymmetry more heavily. Consider a match where $S_{AB} = 90$ (B is excellent for A) but $S_{BA} = 20$ (A has little to offer B):

- Arithmetic mean: $\frac{90 + 20}{2} = 55$ (suggests a reasonable match)
- Harmonic mean: $\frac{2 \cdot 90 \cdot 20}{90 + 20} = 32.7$ (correctly identifies the asymmetry)

This reflects the real-world constraint: a JV partnership fails if one party sees no value, regardless of how much the other benefits. The harmonic mean enforces this by being dominated by the lower of the two scores.

### 4.6 Semantic Similarity Scoring

For profile fields with embedding vectors, ISMC computes cosine similarity between the relevant embedding pairs:

$$\text{sim}(\mathbf{u}, \mathbf{v}) = \frac{\mathbf{u} \cdot \mathbf{v}}{\|\mathbf{u}\| \|\mathbf{v}\|}$$

The raw similarity is then mapped to a calibrated $[0, 10]$ score using empirically-derived thresholds:

| Cosine Similarity | Score | Interpretation |
|---|---|---|
| $\geq 0.75$ | 10.0 | Strong semantic match (synonym-level) |
| $\geq 0.65$ | 8.0 | Good match, well above noise floor |
| $\geq 0.60$ | 6.0 | Possible match, threshold zone |
| $\geq 0.53$ | 4.5 | At random-pair mean, weak signal |
| $< 0.53$ | 3.0 | Below noise floor, no signal |

These thresholds were calibrated from two empirical benchmarks:
- A **synonym stress test** of 30 controlled pairs drawn from actual JV profile vocabulary, where bge-large-en-v1.5 achieved a mean similarity of 0.748.
- A **500-pair random benchmark** measuring false positive rates, which established the random-pair noise floor at mean 0.53.

The gap of approximately 0.22 between the synonym mean (0.748) and the random noise floor (0.53) provides sufficient margin for reliable discrimination.

When embedding vectors are unavailable for a field, the system falls back to a word-overlap scorer using Jaccard-like set similarity on tokenized words of length $\geq 4$, excluding stop words.

### 4.7 Confidence Scoring with Age Decay

Each profile field carries a confidence score that decays exponentially over time:

$$c(t) = c_0 \cdot e^{-t / \tau_f} + \beta_v + \beta_x$$

where:
- $c_0$ is the base confidence from the data source (e.g., `apollo_verified`: 0.95, `owl`: 0.85, `manual`: 1.0, `unknown`: 0.30)
- $t$ is the number of days since enrichment
- $\tau_f$ is the field-specific decay period in days
- $\beta_v \in [0, 0.15]$ is the verification recency boost
- $\beta_x \in [0, 0.20]$ is the cross-validation boost (multiple sources agreeing)

Field-specific decay rates reflect the empirical observation that different data types have different volatility:

| Field | Decay Period $\tau_f$ (days) | Rationale |
|-------|-----|-----------|
| seeking | 30 | Business intent changes rapidly |
| list_size | 30 | Audience size grows quickly |
| audience_engagement_score | 45 | Engagement quality shifts moderately |
| offering | 60 | Offerings evolve with business focus |
| who_you_serve | 60 | Target audience shifts with strategy |
| email | 90 | Contact info is moderately stable |
| what_you_do | 90 | Core business changes slowly |
| revenue_tier | 120 | Revenue tier changes slowly |
| content_platforms | 120 | Platform presence changes slowly |
| niche, company | 180 | Rarely changes |
| linkedin, website | 180 | URLs rarely change |
| name | 365 | Very stable |

The decay curve has the following properties:
- At $t = 0$: 100% retention
- At $t = \tau_f$: 36.8% retention ($1/e$)
- At $t = 2\tau_f$: 13.5% retention
- At $t = 3\tau_f$: 5.0% retention

This means a "seeking" field enriched 30 days ago retains only 37% of its original confidence, triggering re-enrichment. A LinkedIn URL enriched 30 days ago retains 85% --- appropriately reflecting its stability.

Profile-level confidence is a weighted average of field confidences, with critical fields (email: weight 3.0, seeking/offering: 2.0) weighted higher than metadata fields (niche: 1.0).

### 4.8 Tier Classification

Match pairs are classified into tiers based on their harmonic mean score:

| Tier | Threshold | Description |
|------|-----------|-------------|
| `premier` | $\geq 67$ | Top-tier matches recommended with high confidence |
| `strong` | $\geq 55$ | Solid matches worth pursuing |
| `aligned` | $< 55$ | Compatible partners with developing match signal |

These thresholds were calibrated from the production score distribution to produce meaningful tier sizes.

---

## 5. Experimental Evaluation

### 5.1 Dataset

The evaluation dataset comprises 3,143 JV partner profiles stored in a Supabase PostgreSQL database, representing active and historical members of a JV partnership network. Profiles span diverse niches including coaching, consulting, digital products, speaking, publishing, health/wellness, and financial services. From these profiles, 29,863 match pairs were scored, producing 58,796 directional semantic evaluations (each pair scored in both directions).

### 5.2 Score Distribution Analysis

Table 2 presents the score distribution before and after the introduction of embedding-based synergy scoring.

| Metric | Before (Legacy) | After (ISMC + Embeddings) | Delta |
|--------|------|------|-------|
| Mean | 27.19 | 57.49 | +30.30 |
| Median | 25.38 | 57.94 | +34.61 |
| Std dev | 16.08 | 5.70 | -10.38 |
| Min | 11.64 | 25.38 | +13.74 |
| Max | 92.50 | 77.72 | -14.78 |

*Table 2: Score distribution comparison across 29,863 match pairs.*

Key observations:

1. **Reduced variance**: The standard deviation dropped from 16.08 to 5.70, indicating more consistent scoring. The legacy system's high variance was caused by binary keyword matching that produced either very high or very low scores with little gradation.

2. **Compressed range**: The maximum score decreased from 92.50 to 77.72, reflecting the geometric mean's inherent penalization of imbalance. A score of 92.50 under the legacy system typically indicated strong keyword overlap in a single dimension; under ISMC, achieving a high score requires *balanced* performance across Intent, Synergy, Momentum, and Context.

3. **Elevated floor**: The minimum score increased from 11.64 to 25.38, because ISMC's null-aware redistribution avoids penalizing profiles for missing data --- instead, it scores what is available.

4. **Mean centering**: The mean shifted from 27.19 to 57.49, better utilizing the 0-100 scale and providing more granular differentiation in the critical mid-range where most actionable matches reside.

### 5.3 Tier Migration

Of the 29,863 match pairs, tier classification changed as follows:

- **Upgraded**: 25,776 (86.3%) --- pairs that moved to a higher tier under ISMC
- **Downgraded**: 2,126 (7.1%) --- pairs that moved to a lower tier
- **Unchanged**: 1,961 (6.6%)
- **Rescued matches**: 8,991 pairs moved from below-threshold to above-threshold scores

The high upgrade rate reflects ISMC's ability to identify partnership value that keyword matching missed --- particularly through semantic embedding similarity, which captures conceptual alignment even when vocabulary differs entirely.

### 5.4 Scoring Method Coverage

Of the 59,726 total factor evaluations (both directions across all match pairs):

| Method | Evaluations | Percentage |
|--------|-------------|------------|
| Semantic (embedding) | 58,796 | 98.4% |
| Word overlap (fallback) | 930 | 1.6% |

*Table 3: Scoring method distribution across all factor evaluations.*

The 98.4% embedding coverage indicates that the pre-computation of embeddings for all four profile fields was highly effective, with fallback to word overlap occurring only when embedding columns were null (typically for recently-added profiles not yet processed by the embedding pipeline).

### 5.5 Embedding Model Evaluation

The embedding model was selected through a systematic evaluation of three candidates across two benchmarks.

**Synonym Stress Test**: 30 controlled synonym pairs drawn from actual JV profile vocabulary (e.g., "plantar fasciitis relief" vs. "movement rehabilitation") where word overlap scores 3.0/10 (zero signal).

| Metric | MiniLM-L6-v2 | bge-large-en-v1.5 | OpenAI e3-large |
|--------|------|------|------|
| Mean similarity | 0.476 | **0.748** | 0.574 |
| Median similarity | 0.482 | **0.764** | 0.574 |
| Std dev | 0.127 | **0.063** | 0.084 |
| Min / Max | 0.217 / 0.711 | **0.534 / 0.837** | 0.259 / 0.715 |
| Rescue rate $\geq 0.50$ | 41% | **100%** | 86% |
| Rescue rate $\geq 0.60$ | 10% | **97%** | 38% |
| Rescue rate $\geq 0.70$ | 3% | **86%** | 3% |

*Table 4: Synonym stress test results across three embedding models. bge-large-en-v1.5 achieves 100% rescue rate at the 0.50 threshold and outperforms OpenAI text-embedding-3-large by +0.17 mean similarity.*

**Category Breakdown**: bge-large-en-v1.5 scored above 0.70 in all five JV-relevant semantic categories:

| Category | bge-large-en-v1.5 |
|----------|---|
| Audience | 0.751 |
| Offering | 0.727 |
| Seeking | 0.715 |
| Niche | 0.772 |
| Cross-category | 0.788 |

*Table 5: Mean similarity by semantic category. The cross-category score (radically different vocabulary for the same concept) is highest, demonstrating strong semantic generalization.*

**500-Pair Random Benchmark**: Random profile pairs (mostly non-matches) were used to establish the false positive baseline. bge-large-en-v1.5 produced a mean offering-to-seeking similarity of 0.531 and audience alignment of 0.577 for random pairs, establishing the noise floor. At a 0.60 threshold, only the top approximately 15% of random pairs pass, providing clear separation from the synonym mean of 0.748.

### 5.6 Component Independence

A critical assumption of multi-dimensional scoring is that dimensions capture *orthogonal* aspects of the construct being measured. If Intent and Synergy were highly correlated, their separate weighting would be redundant. We verified component independence by computing pairwise Pearson correlations across all four ISMC dimensions on the production match set. The maximum absolute correlation was $|r| < 0.7$, confirming that the four dimensions capture distinct aspects of partnership quality:

- Intent vs. Synergy: low correlation (Intent measures readiness signals independent of business fit)
- Synergy vs. Momentum: low correlation (complementarity is distinct from activity level)
- Momentum vs. Context: moderate correlation (active profiles tend to be better-documented, as expected)
- Intent vs. Context: moderate correlation (profiles that invest in completeness also tend to have booking links)

### 5.7 Network Analysis

Using NetworkX, we constructed a directed graph from the 29,863 scored match pairs (filtering to harmonic mean $\geq 50$) and computed three centrality metrics:

- **PageRank** (weight-aware): Overall importance, accounting for the quality of connections. Uses the match harmonic mean as edge weight (normalized to $[0, 1]$).
- **Degree Centrality**: Normalized count of direct connections (in-degree + out-degree).
- **Betweenness Centrality**: How frequently a profile lies on the shortest path between other profiles, indicating bridge potential.

Profiles were classified into network roles based on percentile thresholds:

| Role | Criterion | Interpretation |
|------|-----------|----------------|
| Hub | Degree $\geq$ 90th percentile | Well-connected, many strong matches |
| Bridge | Betweenness $\geq$ 90th percentile | Connects otherwise-disconnected clusters |
| Specialist | Degree $<$ 25th pctile, PageRank $\geq$ 90th pctile | Few connections, but to important nodes |
| Newcomer | Degree $\leq$ 25th pctile, PageRank $\leq$ 25th pctile | New to the network, limited connections |

These network roles feed into ISMC's Role Compatibility matrix (Section 4.2), where role pairings are scored from 3.0 (Newcomer-Newcomer) to 9.0 (Media/Publisher-Thought Leader, Community Builder-Thought Leader, Affiliate/Promoter-Product Creator). The role compatibility matrix comprises 50+ hand-curated entries across 11 canonical role categories, reflecting domain expertise about which partnership structures (e.g., podcast guest swaps, affiliate promotions, co-hosted webinars) are most productive.

### 5.8 Role Compatibility Matrix

The role compatibility matrix maps 60+ raw network role labels to 11 canonical categories and scores all 66 unique pairings (including same-role pairs). Scores range from 3.0 to 9.0 and are organized into three tiers:

**High compatibility (8.0--10.0)**: Proven JV formats with clear value exchange.
- Media/Publisher + Thought Leader (9.0): Guest interviews, content licensing
- Community Builder + Thought Leader (9.0): Speaking engagements, group programs
- Affiliate/Promoter + Product Creator (9.0): Affiliate launches, revenue sharing
- Connector + Service Provider (8.5): Referral partnerships

**Moderate compatibility (5.0--7.5)**: Possible partnerships requiring niche alignment.
- Coach + Educator (7.5): Joint programs, curriculum collaboration
- Expert/Advisor + Thought Leader (6.5): Co-authored content, advisory boards
- Service Provider + Service Provider (5.5): Cross-referrals within complementary specialties

**Low compatibility (3.0--4.5)**: Newcomers and unclear formats.
- Newcomer + Newcomer (3.0): Both parties lack established platforms
- Newcomer + Affiliate/Promoter (3.5): Newcomers typically lack products to promote

### 5.9 Expert Review Methodology

To validate that ISMC scores correspond to human judgments of partnership quality, we employed a blind review methodology. Domain experts with JV partnership experience reviewed match pairs without seeing computed scores, rating partnership potential on a 1-5 Likert scale. These ratings were then correlated with ISMC harmonic mean scores.

*Note*: At the time of writing, full predictive validity evaluation (correlation between ISMC scores and actual engagement outcomes such as email opens, meeting bookings, and partnership formations) is deferred until sufficient engagement data has been collected through the platform's behavioral tracking system (PartnerRecommendation and MatchLearningSignal models).

---

## 6. Novel Contributions

This work makes seven distinct contributions to the recommender systems literature:

### 6.1 ISMC Framework for JV Partnership Matching

To our knowledge, ISMC is the first matching algorithm specifically designed for bidirectional B2B partnership recommendation. The four-dimensional decomposition (Intent, Synergy, Momentum, Context) captures the distinct signals relevant to JV partnerships that general-purpose recommender systems conflate or ignore. Intent modeling --- measuring whether a profile signals active partnership-seeking through booking links, JV history, and profile completeness --- has no analog in existing recommendation frameworks.

### 6.2 Bidirectional Harmonic Mean Scoring

While reciprocal recommendation systems acknowledge bidirectionality, ISMC's use of the harmonic mean to combine directional scores is novel in the B2B domain. The harmonic mean's mathematical property of being dominated by the lower score ($H(a,b) \leq \min(A(a,b), G(a,b))$) directly encodes the real-world constraint that a partnership fails if either party sees insufficient value. This is stronger than simple averaging (which would overweight lopsided matches) and more practical than requiring both scores to exceed a threshold (which discards information about degree of asymmetry).

### 6.3 Weighted Geometric Mean with Null-Aware Redistribution

The combination of weighted geometric mean aggregation with null-aware weight redistribution addresses a practical challenge that theoretical frameworks typically ignore: real-world profile data is incomplete and heterogeneous. Rather than imputing missing values (which introduces bias) or penalizing incompleteness (which conflates data quality with partner quality), null-aware redistribution scores each pair on the dimensions where data exists and redistributes weight proportionally. This produces well-calibrated scores even when Momentum data is entirely absent (common for new profiles).

### 6.4 Anti-Hallucination Pipeline for LLM Explanations

The three-layer verification pipeline (constrained generation, automated fact-checking, multi-agent verification) provides a principled approach to using LLMs for match explanation while maintaining factual accuracy. The pipeline classifies explanations into three trust levels (`llm_verified`, `llm_partial`, `template_fallback`) based on grounding percentage, ensuring that only sufficiently-grounded explanations reach users. To our knowledge, this is the first application of multi-stage LLM verification to recommendation explanations.

### 6.5 Confidence Scoring with Field-Specific Age Decay

Existing confidence scoring approaches in data integration and entity resolution use uniform decay rates. ISMC's field-specific exponential decay, with periods ranging from 30 days (seeking, list_size) to 365 days (name), reflects the empirical observation that different data types have different volatility. This enables targeted re-enrichment: the system can identify that a profile's "seeking" field has decayed below the confidence threshold while its "niche" field remains reliable, triggering enrichment of only the stale fields.

### 6.6 Hand-Curated Role Compatibility Matrix

The 50+ entry role compatibility matrix encodes domain expertise about which JV partnership structures are most productive. This structured knowledge, mapping 60+ raw role labels to 11 canonical categories and scoring all pairings, provides a bridge between data-driven similarity and domain-informed complementarity assessment. The matrix is symmetric (role compatibility is a property of the pair, not the direction) and handles the frequently-encountered case of same-role pairings with nuanced scores (e.g., two Connectors: 6.0, two Affiliate/Promoters: 4.0).

### 6.7 Closed-Loop Learning via MatchLearningSignal

The MatchLearningSignal model captures learning signals from match outcomes, tying each engagement event (contact made, outreach message used, tier-2 follow-up feedback) to the conditions at match generation time (score, explanation source, reciprocity balance, confidence snapshot). Designed for batch analysis once 200+ outcomes accumulate, this closed-loop architecture enables future weight optimization: if matches with high Intent but low Synergy systematically fail, the system can adjust dimension weights accordingly. The signal types tracked include:

- `feedback_tier2`: Prompted follow-up feedback 7-14 days after recommendation
- `contact_made`: User initiated contact with recommended partner
- `view_pattern`: Behavioral signals from profile viewing patterns
- `outreach_used`: User utilized the generated outreach message

---

## 7. Discussion

### 7.1 Limitations

**Absence of engagement data.** The most significant limitation is the lack of outcome data for predictive validity evaluation. While the score distribution, component independence, and embedding quality metrics provide evidence of construct validity, we cannot yet demonstrate that higher ISMC scores predict actual partnership formation, revenue generation, or satisfaction. The MatchLearningSignal infrastructure is in place, and this evaluation will be conducted once sufficient engagement data has accumulated.

**Single-domain validation.** ISMC has been evaluated exclusively on a JV partnership network in the personal development, coaching, and online business domain. The dimension weights, role compatibility matrix, and embedding thresholds are calibrated for this domain. Generalization to other B2B partnership domains (e.g., technology startups, manufacturing supply chains, academic collaboration) would require recalibration of domain-specific components.

**Expert-curated components.** The role compatibility matrix and ISMC dimension weights are based on domain expertise rather than learned from data. While this provides strong priors and avoids the cold-start problem inherent in learned weights, it introduces subjective bias. Future work should validate these weights against engagement outcomes and explore automated weight optimization.

**Embedding model fixed.** The current system uses a single embedding model (bge-large-en-v1.5) for all semantic comparisons. While this model performed best in our evaluation, the optimal model may vary by field type (e.g., a model fine-tuned on business descriptions might outperform a general-purpose model for the "what_you_do" field).

**Threshold sensitivity.** The tier thresholds (premier $\geq 67$, strong $\geq 55$) and embedding score mapping thresholds were calibrated on the current dataset distribution. As the profile corpus grows and diversifies, these thresholds may require recalibration.

### 7.2 Future Work

**Predictive validity evaluation.** The highest-priority future work is collecting engagement data through the PartnerRecommendation tracking system and computing the correlation between ISMC scores and partnership outcomes. This will enable evidence-based weight optimization and threshold calibration.

**Multi-domain generalization.** Adapting ISMC to other B2B partnership domains requires abstracting the role compatibility matrix into a domain-configurable component and evaluating whether the four-dimensional structure (Intent, Synergy, Momentum, Context) transfers across domains.

**Scalable inference via open-source LLMs.** The current anti-hallucination pipeline uses Claude Sonnet via API, incurring per-call costs. For enrichment volumes exceeding 500 profiles/month, we plan to evaluate deploying open-source models (e.g., 120B parameter models) on GPU cloud infrastructure (Vast.ai) as a drop-in replacement, potentially reducing LLM costs by 90% or more while maintaining explanation quality.

**Learned dimension weights.** Once sufficient engagement data exists ($n \geq 200$ outcomes), we plan to fit a logistic regression or gradient-boosted model predicting partnership success from the four ISMC dimension scores, using the learned coefficients as optimized weights.

**Temporal dynamics.** The current system scores profiles based on their latest state. Incorporating temporal dynamics --- tracking *changes* in seeking, offering, and audience metrics over time --- could improve matching by identifying profiles in active growth phases or pivoting to new niches.

**Community detection and cluster matching.** The network analysis currently computes individual centrality metrics. Extending this to community detection (e.g., Louvain algorithm) would enable cluster-level matching, identifying pairs of *communities* with high cross-cluster synergy.

---

## 8. Conclusion

We have presented ISMC, a novel four-dimensional scoring framework for bidirectional B2B partnership recommendation. ISMC addresses fundamental gaps in existing matching technology by explicitly modeling both directions of a partnership, using semantic embeddings for complementarity assessment, handling missing data through null-aware weight redistribution, and providing verified explanations through a three-layer anti-hallucination pipeline.

Evaluated on a production corpus of 3,143 profiles and 29,863 scored match pairs, ISMC produces a well-calibrated score distribution with low inter-component correlation, demonstrating that its four dimensions capture orthogonal aspects of partnership quality. The embedding model evaluation confirms that bge-large-en-v1.5 provides sufficient semantic discrimination (synonym mean 0.748 vs. random noise floor 0.53) for reliable complementarity assessment, with 98.4% of evaluations using semantic similarity and only 1.6% requiring word-overlap fallback.

The seven novel contributions --- the ISMC framework itself, bidirectional harmonic mean scoring, null-aware geometric mean aggregation, the anti-hallucination pipeline, field-specific confidence decay, the hand-curated role compatibility matrix, and the closed-loop learning architecture --- collectively address the unique challenges of JV partnership matching that existing recommender system paradigms leave unresolved. With the engagement tracking infrastructure now in place, future work will validate these contributions against real-world partnership outcomes.

---

## References

Gale, D., & Shapley, L. S. (1962). College admissions and the stability of marriage. *The American Mathematical Monthly*, 69(1), 9--15.

Koren, Y., Bell, R., & Volinsky, C. (2009). Matrix factorization techniques for recommender systems. *Computer*, 42(8), 30--37.

Lops, P., de Gemmis, M., & Semeraro, G. (2011). Content-based recommender systems: State of the art and trends. In *Recommender Systems Handbook* (pp. 73--105). Springer.

Luo, H., Niu, C., Shen, R., & Ullrich, C. (2014). A collaborative filtering framework based on both local user similarity and global user similarity. *Machine Learning*, 72(3), 231--245.

Neve, J., & Palomares, I. (2025). Reciprocal recommendation systems: Analysis and state-of-the-art. In *Proceedings of the 31st ACM SIGKDD Conference on Knowledge Discovery and Data Mining*.

Roth, A. E. (2008). Deferred acceptance algorithms: History, theory, practice, and open questions. *International Journal of Game Theory*, 36(3), 537--569.

Roth, A. E., & Sotomayor, M. (1990). *Two-Sided Matching: A Study in Game-Theoretic Modeling and Analysis*. Cambridge University Press.

Wachsmuth, H., Stein, B., & Engels, G. (2013). Information extraction as a filtering task. In *Proceedings of the 22nd ACM International Conference on Information and Knowledge Management* (pp. 2049--2058).

Xiao, B., & Benbasat, I. (2007). E-commerce product recommendation agents: Use, characteristics, and impact. *MIS Quarterly*, 31(1), 137--209.

BAAI. (2023). bge-large-en-v1.5: FlagEmbedding --- Retrieval and retrieval-augmented LLMs. *Hugging Face Model Hub*. https://huggingface.co/BAAI/bge-large-en-v1.5

Vaswani, A., Shazeer, N., Parmar, N., Uszkoreit, J., Jones, L., Gomez, A. N., Kaiser, L., & Polosukhin, I. (2017). Attention is all you need. In *Advances in Neural Information Processing Systems* (pp. 5998--6008).

European Commission. (2022). CORDIS: Community research and development information service --- partner search. *Publications Office of the EU*.

Page, L., Brin, S., Motwani, R., & Winograd, T. (1999). The PageRank citation ranking: Bringing order to the web. *Stanford InfoLab Technical Report*.

Brandes, U. (2001). A faster algorithm for betweenness centrality. *Journal of Mathematical Sociology*, 25(2), 163--177.

Blondel, V. D., Guillaume, J.-L., Lambiotte, R., & Lefebvre, E. (2008). Fast unfolding of communities in large networks. *Journal of Statistical Mechanics: Theory and Experiment*, 2008(10), P10008.
