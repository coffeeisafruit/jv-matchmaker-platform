# Study 2.3: Context Dimension Directionality Audit

**Date:** 2026-02-28
**Analyst:** Automated (Claude Code)
**Scope:** Deep audit of `_score_context()` in `matching/services.py` to determine whether the Context dimension's directional asymmetry is a bug or legitimate design behavior.

---

## Executive Summary

| Factor | Weight | Mean Abs Diff | % Symmetric | Classification | Verdict |
|---|---|---|---|---|---|
| Profile Completeness | 3.0 | 1.2876 | 22.4% | ASYMMETRIC-INTENTIONAL | LEGITIMATE |
| Revenue Known | 2.0 | 1.6398 | 67.2% | ASYMMETRIC-INTENTIONAL | LEGITIMATE |
| Enrichment Quality | 2.5 | 1.3987 | 28.1% | ASYMMETRIC-INTENTIONAL | LEGITIMATE |
| Contact Available | 2.5 | 2.6182 | 38.9% | ASYMMETRIC-INTENTIONAL | LEGITIMATE |
| Enrichment Confidence | 2.0 | N/A | N/A | NULL (never populated) | N/A |
| Recommendation Freshness | 1.5 | N/A | N/A | NULL (never populated) | N/A |

**Overall Context asymmetry:** mean=1.5696, median=1.3200 (N=29,863)

**Verdict: NO BUGS FOUND.** The Context dimension's asymmetry is the highest among all four ISMC dimensions (mean abs diff 1.57 vs Intent 1.09, Momentum 1.03, Synergy 0.69), but this is entirely a consequence of its intentional design: Context scores the *target* profile's data quality, and different profiles naturally have different data completeness. The asymmetry is not a coding error.

---

## 1. Code Audit of `_score_context()`

**Location:** `matching/services.py`, line 2342

### Method Signature

```python
def _score_context(self, target: SupabaseProfile) -> dict:
    """Score data quality and profile completeness."""
```

**Critical observation:** The method takes only `target` -- it does NOT receive `source`. This is the root cause of all asymmetry: when scoring the A->B direction, `_score_context(profile_b)` evaluates B's data quality; when scoring B->A, `_score_context(profile_a)` evaluates A's data quality. If A and B have different data completeness, the scores will differ.

### Calling Context

In `_score_directional()` (line 1849):

```python
def _score_directional(self, source, target, outcome_data=None):
    intent = self._score_intent(target, outcome_data=outcome_data)      # target-only
    synergy = self._score_synergy(source, target)                        # both
    momentum = self._score_momentum(target)                              # target-only
    context = self._score_context(target)                                # target-only
```

And in `score_pair()` (line 1491):

```python
score_ab, breakdown_ab = self._score_directional(profile_a, profile_b)   # target=B
score_ba, breakdown_ba = self._score_directional(profile_b, profile_a)   # target=A
```

This means:
- **A->B Context score** = data quality of **profile B**
- **B->A Context score** = data quality of **profile A**

### Semantic Interpretation

The Context dimension answers: "How much can we trust the data we have about this target partner?" This is a per-profile property, not a pair property. It is conceptually correct that a well-documented profile scores higher regardless of who it's being matched with.

---

## 2. Factor-by-Factor Analysis

### Factor 1: Profile Completeness (weight=3.0)

**Code (line 2348-2360):**
```python
completeness_fields = [
    target.name, target.email, target.company, target.website,
    target.linkedin, target.niche, target.what_you_do,
    target.who_you_serve, target.seeking, target.offering,
    target.booking_link, target.revenue_tier,
]
filled = sum(1 for f in completeness_fields if f and str(f).strip())
completeness_score = (filled / len(completeness_fields)) * 10
```

- **Inputs:** 12 fields from `target` only
- **Direction-specific?** Yes -- scores target's field count
- **Empirical:** mean_diff=1.2876, 22.4% symmetric, N=29,863
- **Classification:** ASYMMETRIC-INTENTIONAL
- **Reasoning:** Profile completeness is inherently a per-profile property. If partner A has 11/12 fields and partner B has 8/12 fields, the asymmetry is real and meaningful -- we have more data about A.

### Factor 2: Revenue Known (weight=2.0)

**Code (line 2362-2368):**
```python
has_rev = bool(target.revenue_tier and target.revenue_tier != 'unknown')
rev_score = 8.0 if has_rev else 3.0
```

- **Inputs:** `target.revenue_tier` only
- **Direction-specific?** Yes -- binary check on target
- **Empirical:** mean_diff=1.6398, 67.2% symmetric, median_diff=0.0, N=29,863
- **Classification:** ASYMMETRIC-INTENTIONAL
- **Reasoning:** This is a binary factor (8.0 vs 3.0, a 5-point gap). The high median-symmetry (67.2%) but non-trivial mean diff (1.64) indicates most pairs have matching revenue status, but the 32.8% that don't create a large 5-point swing. This is correct behavior: knowing a partner's revenue tier is materially important for match quality.

### Factor 3: Enrichment Quality (weight=2.5)

**Code (line 2370-2381):**
```python
enrichment_fields = [
    target.what_you_do, target.who_you_serve, target.niche,
    target.offering, target.seeking, target.signature_programs,
    target.revenue_tier, target.content_platforms,
]
enriched = sum(1 for f in enrichment_fields if f)
enrich_score = min(10.0, (enriched / len(enrichment_fields)) * 10)
```

- **Inputs:** 8 enrichment fields from `target` only
- **Direction-specific?** Yes -- counts target's enrichment depth
- **Empirical:** mean_diff=1.3987, 28.1% symmetric, N=29,863
- **Classification:** ASYMMETRIC-INTENTIONAL
- **Reasoning:** Enrichment quality measures how thoroughly the AI pipeline has researched this profile. Partially-enriched profiles legitimately produce lower scores.

### Factor 4: Contact Available (weight=2.5)

**Code (line 2383-2398):**
```python
has_email = bool(target.email and target.email.strip())
has_phone = bool(target.phone and target.phone.strip())
has_linkedin = bool(target.linkedin and target.linkedin.strip())
if has_email:
    contact_score = 9.0
elif has_linkedin:
    contact_score = 7.0
elif has_phone:
    contact_score = 6.0
else:
    contact_score = 2.0
```

- **Inputs:** `target.email`, `target.phone`, `target.linkedin` only
- **Direction-specific?** Yes -- checks target's contact channels
- **Empirical:** mean_diff=2.6182, 38.9% symmetric, N=29,863
- **Classification:** ASYMMETRIC-INTENTIONAL
- **Reasoning:** This is the **largest contributor to asymmetry** (38.1% of weighted asymmetry). The tiered scoring creates discrete jumps (2.0 -> 6.0 -> 7.0 -> 9.0). Contact availability is inherently per-profile: if partner A has email but partner B only has LinkedIn, the 2-point gap (9.0 vs 7.0) correctly reflects that A is easier to reach.

**Score distribution:**

| Score | A->B Count | A->B % | B->A Count | B->A % |
|---|---|---|---|---|
| 2.0 (no contact) | 5,784 | 19.4% | 6,799 | 22.8% |
| 6.0 (phone only) | 435 | 1.5% | 579 | 1.9% |
| 7.0 (LinkedIn) | 7,260 | 24.3% | 7,810 | 26.2% |
| 9.0 (email) | 16,384 | 54.9% | 14,675 | 49.1% |

### Factor 5: Enrichment Confidence (weight=2.0, null-aware)

**Code (line 2400-2407):**
```python
confidence = target.profile_confidence
if isinstance(confidence, (int, float)):
    conf_score = min(10.0, confidence * 10)
    factors.append(...)
    total += conf_score * 2.0
    max_total += 10 * 2.0
```

- **Inputs:** `target.profile_confidence` only
- **Direction-specific?** Yes, but moot -- field is never populated
- **Empirical:** Present in 0/29,863 matches (0.0%)
- **Classification:** NULL -- never fires, does not affect scoring
- **Note:** This factor is null-aware: when absent, both `total` and `max_total` skip this term, so the overall Context score is computed from the remaining 4 factors only. No normalization bias is introduced.

### Factor 6: Recommendation Freshness (weight=1.5, null-aware)

**Code (line 2409-2423):**
```python
pressure = target.recommendation_pressure_30d
if isinstance(pressure, int):
    ...
```

- **Inputs:** `target.recommendation_pressure_30d` only
- **Direction-specific?** Yes, but moot -- field is never populated
- **Empirical:** Present in 0/29,863 matches (0.0%)
- **Classification:** NULL -- never fires, does not affect scoring
- **Note:** Same null-aware pattern as Factor 5. No impact on current scores.

---

## 3. Empirical Asymmetry Measurements

### 3.1 Cross-Dimension Comparison

| Dimension | Mean Abs Diff | Median Abs Diff | % Symmetric | N |
|---|---|---|---|---|
| Synergy | 0.6854 | 0.5800 | 31.2% | 29,863 |
| Momentum | 1.0312 | 0.8500 | 2.6% | 19,293 |
| Intent | 1.0866 | 0.9300 | 18.7% | 29,863 |
| **Context** | **1.5696** | **1.3200** | **5.8%** | **29,863** |

Context has the highest asymmetry among all dimensions. However, this is expected because:

1. **Context is target-only.** Unlike Synergy (which uses both source and target), Context evaluates only the target profile's data quality. Two different profiles will almost always have different data completeness.
2. **Discrete scoring tiers.** Contact Available and Revenue Known use binary/tiered scoring with large jumps (2.0/6.0/7.0/9.0 for contact; 3.0/8.0 for revenue), which amplifies differences.
3. **No shared component.** Synergy has the lowest asymmetry because its Offering<->Seeking comparison is inherently symmetric in many cases. Context has zero shared components between directions.

### 3.2 Context Score Distribution

| Statistic | A->B | B->A |
|---|---|---|
| Mean | 6.9442 | 6.7539 |

The slight A->B bias (0.19 higher) reflects that `profile_id` (the "source" profile) tends to be the client profile with slightly better enrichment, while `suggested_profile_id` tends to be the prospect.

### 3.3 Asymmetry Distribution (Percentiles)

| Percentile | Abs Diff |
|---|---|
| P10 | 0.24 |
| P25 | 0.50 |
| P50 (median) | 1.32 |
| P75 | 2.32 |
| P90 | 3.19 |
| P95 | 4.13 |
| P99 | 4.94 |
| Max | 7.75 |

### 3.4 Weighted Contribution to Asymmetry

Each factor's contribution to the overall Context asymmetry, weighted by its factor weight:

| Factor | Weight | Mean Weighted Diff | % of Total Asymmetry |
|---|---|---|---|
| Contact Available | 2.5 | 6.5455 | 38.1% |
| Profile Completeness | 3.0 | 3.8629 | 22.5% |
| Enrichment Quality | 2.5 | 3.4968 | 20.3% |
| Revenue Known | 2.0 | 3.2796 | 19.1% |

Contact Available dominates the asymmetry budget due to its large tier jumps (2.0 to 9.0 range = 7 points).

### 3.5 Per-Profile Consistency Check

To confirm that asymmetry comes from inter-profile differences (not from randomness or bugs), we checked whether the same profile gets the same Context score across different matches:

- **Profiles appearing 3+ times as target:** 1,955
- **Mean variance of Context score per profile:** 0.000622
- **Median variance:** 0.000000
- **% with zero variance (perfectly consistent):** 45.0%

The near-zero variance confirms that Context scores are **deterministic per profile**. The asymmetry is entirely explained by different profiles having different data quality -- not by any stochastic or buggy behavior.

---

## 4. Classification Summary

| Factor | Classification | Rationale |
|---|---|---|
| Profile Completeness | **ASYMMETRIC-INTENTIONAL** | Per-profile property; different profiles have different field counts |
| Revenue Known | **ASYMMETRIC-INTENTIONAL** | Binary per-profile property; knowing revenue tier is materially important |
| Enrichment Quality | **ASYMMETRIC-INTENTIONAL** | Per-profile property; enrichment depth varies by profile |
| Contact Available | **ASYMMETRIC-INTENTIONAL** | Per-profile property; contact channel availability varies |
| Enrichment Confidence | **NOT APPLICABLE** | Never populated (0/29,863 matches) |
| Recommendation Freshness | **NOT APPLICABLE** | Never populated (0/29,863 matches) |

**No factors classified as ASYMMETRIC-BUG.**

---

## 5. Design Analysis

### Why Context Scores Only the Target

The `_score_context(target)` design makes semantic sense when interpreted as: *"How reliable is our assessment of this potential partner?"*

In the directional scoring model:
- **A->B score** answers: "How valuable would B be as a partner for A?"
- **Context(B)** answers: "How much can we trust our data about B?"

If B's profile is incomplete or lacks contact info, our confidence in the A->B recommendation should be lower -- regardless of how complete A's profile is. This is the correct behavior.

### Comparison to Other Dimensions

| Dimension | Uses Source? | Uses Target? | Asymmetry Source |
|---|---|---|---|
| Intent | No | Yes | Target's partnership signals differ |
| Synergy | Yes | Yes | Offering<->Seeking cross-comparison |
| Momentum | No | Yes | Target's activity metrics differ |
| Context | No | Yes | Target's data quality differs |

Context follows the same pattern as Intent and Momentum (target-only). It is not an outlier in design -- only in magnitude, due to its discrete scoring tiers.

### Why Context Has the Highest Asymmetry

1. **No smoothing:** Profile Completeness and Enrichment Quality use ratios (e.g., 8/12 = 6.67), but Contact Available and Revenue Known use hard tiers with 5-7 point jumps.
2. **No pair-level component:** Synergy naturally dampens asymmetry because Offering<->Seeking similarity is somewhat symmetric. Context has no pair-level dampening.
3. **Binary dominance:** Revenue Known is effectively binary (8.0 vs 3.0), and Contact Available has only 4 tiers. These coarse-grained factors amplify inter-profile differences.

---

## 6. Recommendations

### No Bug Fixes Required

All Context factors behave as designed. The asymmetry reflects genuine differences in data quality between profiles.

### Optional Improvements (Design Choices, Not Bug Fixes)

1. **Soften Contact Available tiers.** The current 4-tier system (2/6/7/9) creates large jumps. A more graduated scale (e.g., incorporating number of valid contact channels additively) would reduce asymmetry without losing signal. This would reduce Context's contribution to the harmonic mean penalty.

2. **Activate dormant factors.** Enrichment Confidence and Recommendation Freshness are coded but never populated (0% prevalence). If `profile_confidence` and `recommendation_pressure_30d` were populated, they would add two more null-aware factors, potentially smoothing the overall Context score by increasing `max_total` and adding more data points.

3. **Consider a symmetric Context floor.** Since Context only weighs 10% of the total score, extreme asymmetry (up to 7.75 points) can cause the harmonic mean to penalize an otherwise good match. A design option would be to use `max(context_a, context_b)` or `mean(context_a, context_b)` as the Context score for both directions, reflecting that a match is only as uncertain as the least-known partner.

### Priority

These are all **low priority** -- Context is only 10% of the final score, and the harmonic mean already dampens unilateral asymmetry. The current behavior is defensible and not causing scoring errors.

---

## Appendix: Raw Data Collection Code

```python
import os, sys, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, '/Users/josephtepe/Projects/jv-matchmaker-platform')
django.setup()

from matching.models import SupabaseMatch
import json, numpy as np

matches = SupabaseMatch.objects.exclude(match_context__isnull=True).exclude(match_context={})

context_diffs = []
factor_ab = {}
factor_ba = {}

for m in matches.iterator():
    ctx = m.match_context
    if isinstance(ctx, str):
        ctx = json.loads(ctx)
    ab = ctx.get('breakdown_ab', {}).get('context', {})
    ba = ctx.get('breakdown_ba', {}).get('context', {})

    ab_total = ab.get('score')
    ba_total = ba.get('score')
    if ab_total is not None and ba_total is not None:
        context_diffs.append(abs(float(ab_total) - float(ba_total)))

    ab_factors = {f['name']: f for f in ab.get('factors', []) if isinstance(f, dict)}
    ba_factors = {f['name']: f for f in ba.get('factors', []) if isinstance(f, dict)}

    for fname in set(ab_factors) | set(ba_factors):
        ab_val = ab_factors.get(fname, {}).get('score')
        ba_val = ba_factors.get(fname, {}).get('score')
        if ab_val is not None and ba_val is not None:
            factor_ab.setdefault(fname, []).append(float(ab_val))
            factor_ba.setdefault(fname, []).append(float(ba_val))

print(f"Context asymmetry: mean={np.mean(context_diffs):.4f}, N={len(context_diffs)}")
for fname in sorted(factor_ab):
    diffs = np.abs(np.array(factor_ab[fname]) - np.array(factor_ba[fname]))
    print(f"{fname}: mean_diff={np.mean(diffs):.4f}, pct_sym={np.sum(diffs==0)/len(diffs)*100:.1f}%")
```
