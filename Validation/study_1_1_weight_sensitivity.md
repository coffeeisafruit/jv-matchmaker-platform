# Study 1.1: ISMC Weight Sensitivity Analysis

**Generated:** 2026-03-01 04:05:40
**Total pairs analyzed:** 29,863
**Scoring formula:** Weighted geometric mean per direction, harmonic mean across directions
**Tier thresholds:** premier >= 67, strong >= 55, aligned < 55

## Weight Configurations

| Config | Intent | Synergy | Momentum | Context |
|--------|--------|---------|----------|---------|
| Current | 45 | 25 | 20 | 10 |
| Equal | 25 | 25 | 25 | 25 |
| Synergy-heavy | 20 | 40 | 20 | 20 |
| Intent-light | 25 | 30 | 25 | 20 |
| No-Context | 45 | 25 | 30 | 0 |
| Momentum-heavy | 20 | 20 | 40 | 20 |

## 1. Tier Distribution

| Config | Premier | % | Strong | % | Aligned | % |
|--------|---------|---|--------|---|---------|---|
| Current | 671 | 2.2% | 20,841 | 69.8% | 8,351 | 28.0% |
| Equal | 1,440 | 4.8% | 22,147 | 74.2% | 6,276 | 21.0% |
| Synergy-heavy | 1,845 | 6.2% | 20,806 | 69.7% | 7,212 | 24.2% |
| Intent-light | 1,212 | 4.1% | 21,830 | 73.1% | 6,821 | 22.8% |
| No-Context | 323 | 1.1% | 18,619 | 62.3% | 10,921 | 36.6% |
| Momentum-heavy | 968 | 3.2% | 21,407 | 71.7% | 7,488 | 25.1% |

## 2. Tier Changes vs. Current Weights

| Config | Pairs Changed | % of Total |
|--------|---------------|------------|
| Current | -- (baseline) | -- |
| Equal | 3,444 | 11.5% |
| Synergy-heavy | 5,447 | 18.2% |
| Intent-light | 3,331 | 11.2% |
| No-Context | 3,186 | 10.7% |
| Momentum-heavy | 3,584 | 12.0% |

## 3. Jaccard Similarity vs. Current Weights

| Config | Premier | Strong | Aligned |
|--------|---------|--------|---------|
| Current | 1.0000 | 1.0000 | 1.0000 |
| Equal | 0.3943 | 0.8517 | 0.7054 |
| Synergy-heavy | 0.2739 | 0.7687 | 0.5900 |
| Intent-light | 0.4211 | 0.8552 | 0.7109 |
| No-Context | 0.4770 | 0.8506 | 0.7436 |
| Momentum-heavy | 0.3304 | 0.8436 | 0.7033 |

## 4. Top 10 Pairs with Largest Score Swing

| Rank | Name A | Name B | Swing | Min HM | Max HM | Worst Config | Best Config |
|------|--------|--------|-------|--------|--------|--------------|-------------|
| 1 | Arthur GISER | Art Giser | 11.1 | 65.2 | 76.3 | No-Context | Synergy-heavy |
| 2 | Joanna Taylor | Nathan Segal | 11.0 | 53.1 | 64.0 | No-Context | Synergy-heavy |
| 3 | Laurie Moser | Jadranka Bozja | 10.8 | 54.1 | 65.0 | No-Context | Synergy-heavy |
| 4 | Judy Britton | Gerald Klingerman | 10.8 | 60.3 | 71.2 | No-Context | Synergy-heavy |
| 5 | Gerald Klingerman | Judy Britton | 10.8 | 60.3 | 71.2 | No-Context | Synergy-heavy |
| 6 | Angela Jackson | Danielle Felicissimo | 10.8 | 58.8 | 69.6 | No-Context | Synergy-heavy |
| 7 | Rudy Bartolome | Robert Smith | 10.7 | 56.6 | 67.3 | Current | Momentum-heavy |
| 8 | Laura Tolosi | Jean Widner | 10.7 | 53.8 | 64.5 | No-Context | Synergy-heavy |
| 9 | Jean Widner | Abiola Oladoke | 10.6 | 53.1 | 63.7 | No-Context | Synergy-heavy |
| 10 | Abiola Oladoke | Jean Widner | 10.6 | 53.1 | 63.7 | No-Context | Synergy-heavy |

### Dimension Scores for Top-Swing Pairs

**1. Arthur GISER <-> Art Giser** (swing: 11.1)
- A->B: Intent=5.51, Synergy=8.84, Momentum=6.5, Context=8.47
- B->A: Intent=5.51, Synergy=8.84, Momentum=None, Context=8.47
- Scores by config: Current=67.2, Equal=73.2, Synergy-heavy=76.3, Intent-light=73.4, No-Context=65.2, Momentum-heavy=72.4

**2. Joanna Taylor <-> Nathan Segal** (swing: 11.0)
- A->B: Intent=4.38, Synergy=8.14, Momentum=4.5, Context=6.97
- B->A: Intent=4.38, Synergy=8.14, Momentum=None, Context=6.66
- Scores by config: Current=54.9, Equal=59.8, Synergy-heavy=64.0, Intent-light=60.4, No-Context=53.1, Momentum-heavy=58.3

**3. Laurie Moser <-> Jadranka Bozja** (swing: 10.8)
- A->B: Intent=4.2, Synergy=8.11, Momentum=None, Context=6.41
- B->A: Intent=5.51, Synergy=8.11, Momentum=4.0, Context=8.04
- Scores by config: Current=56.6, Equal=60.9, Synergy-heavy=65.0, Intent-light=61.4, No-Context=54.1, Momentum-heavy=58.3

**4. Judy Britton <-> Gerald Klingerman** (swing: 10.8)
- A->B: Intent=4.38, Synergy=8.81, Momentum=None, Context=5.91
- B->A: Intent=5.51, Synergy=8.81, Momentum=6.5, Context=9.1
- Scores by config: Current=61.5, Equal=66.6, Synergy-heavy=71.2, Intent-light=67.5, No-Context=60.3, Momentum-heavy=65.9

**5. Gerald Klingerman <-> Judy Britton** (swing: 10.8)
- A->B: Intent=5.51, Synergy=8.81, Momentum=6.5, Context=9.1
- B->A: Intent=4.38, Synergy=8.81, Momentum=None, Context=5.91
- Scores by config: Current=61.5, Equal=66.6, Synergy-heavy=71.2, Intent-light=67.5, No-Context=60.3, Momentum-heavy=65.9

**6. Angela Jackson <-> Danielle Felicissimo** (swing: 10.8)
- A->B: Intent=5.51, Synergy=8.05, Momentum=4.0, Context=8.47
- B->A: Intent=5.51, Synergy=8.05, Momentum=None, Context=8.47
- Scores by config: Current=62.2, Equal=66.8, Synergy-heavy=69.6, Intent-light=66.7, No-Context=58.8, Momentum-heavy=63.7

**7. Rudy Bartolome <-> Robert Smith** (swing: 10.7)
- A->B: Intent=4.2, Synergy=4.42, Momentum=10.0, Context=6.41
- B->A: Intent=5.51, Synergy=5.0, Momentum=8.0, Context=9.1
- Scores by config: Current=56.6, Equal=62.6, Synergy-heavy=59.1, Intent-light=61.1, No-Context=57.6, Momentum-heavy=67.3

**8. Laura Tolosi <-> Jean Widner** (swing: 10.7)
- A->B: Intent=4.2, Synergy=8.11, Momentum=None, Context=6.41
- B->A: Intent=4.38, Synergy=8.11, Momentum=None, Context=5.91
- Scores by config: Current=54.8, Equal=59.8, Synergy-heavy=64.5, Intent-light=60.9, No-Context=53.8, Momentum-heavy=59.8

**9. Jean Widner <-> Abiola Oladoke** (swing: 10.6)
- A->B: Intent=4.38, Synergy=8.11, Momentum=5.0, Context=6.66
- B->A: Intent=4.2, Synergy=8.11, Momentum=None, Context=6.41
- Scores by config: Current=54.5, Equal=59.4, Synergy-heavy=63.7, Intent-light=60.2, No-Context=53.1, Momentum-heavy=58.5

**10. Abiola Oladoke <-> Jean Widner** (swing: 10.6)
- A->B: Intent=4.2, Synergy=8.11, Momentum=None, Context=6.41
- B->A: Intent=4.38, Synergy=8.11, Momentum=5.0, Context=6.66
- Scores by config: Current=54.5, Equal=59.4, Synergy-heavy=63.7, Intent-light=60.2, No-Context=53.1, Momentum-heavy=58.5

## 5. Score Distribution Statistics

| Config | Mean HM | Median HM | Std Dev | P10 | P90 |
|--------|---------|-----------|---------|-----|-----|
| Current | 57.5 | 57.9 | 5.7 | 51.4 | 63.8 |
| Equal | 58.8 | 59.3 | 6.1 | 52.3 | 65.3 |
| Synergy-heavy | 58.5 | 58.9 | 6.3 | 51.7 | 65.7 |
| Intent-light | 58.3 | 58.7 | 5.9 | 52.1 | 64.8 |
| No-Context | 56.3 | 56.6 | 5.3 | 50.5 | 62.3 |
| Momentum-heavy | 57.9 | 58.4 | 5.9 | 51.7 | 64.3 |

## 6. Verdict

**MODERATELY SENSITIVE**

The current weights are MODERATELY SENSITIVE. The worst-case config ('Synergy-heavy') changes 18.2% of tier assignments. Average tier change across all alternative configs is 12.7%. Consider whether the tier boundary thresholds (67/55) need recalibration.

## Methodology

1. Extracted per-dimension ISMC scores (0-10) from `match_context` JSON for all 29,863 scored pairs
2. For each weight configuration, recomputed directional scores using:
   ```
   S_dir = exp(sum(w_d * log(s_d)) / sum(w_d)) * 10
   ```
   where only dimensions with non-null scores participate (weight is redistributed)
3. Combined directional scores via harmonic mean: `H = 2*S_ab*S_ba / (S_ab + S_ba)`
4. Assigned tiers: premier >= 67, strong >= 55, aligned < 55
5. Compared tier assignments, Jaccard similarity, and score distributions

---
*Data source: `match_suggestions` table, 29,863 pairs with harmonic_mean > 0*
