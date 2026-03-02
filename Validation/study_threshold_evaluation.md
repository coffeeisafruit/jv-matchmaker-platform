# Threshold Evaluation: Impact of Revised Embedding Thresholds

**Generated:** 2026-03-01 05:05:53
**Matches analyzed:** 29863
**Matches skipped (no valid context):** 0

## Background

Study 1.3 found optimal F1 at cosine similarity 0.64. This evaluation simulates
the impact of tightening the top threshold to 0.84 (true synonym territory) and
adjusting intermediate thresholds to align with the empirical discrimination data.

## Threshold Comparison

| Bucket | Current Threshold | Current Score | Proposed Threshold | Proposed Score |
|--------|-------------------|---------------|-------------------|----------------|
| Strong | 0.75 | 10.0 | 0.84 | 10.0 |
| Good | 0.65 | 8.0 | 0.64 | 8.0 |
| Possible | 0.60 | 6.0 | 0.62 | 6.0 |
| Noise | 0.53 | 4.5 | 0.60 | 4.5 |
| Default | <0.53 | 3.0 | <0.60 | 3.0 |

## Score Mapping Comparison

| Similarity | Current Score | Proposed Score | Delta |
|------------|--------------|----------------|-------|
| 0.90 | 10.0 | 10.0 | +0.0 |
| 0.84 | 10.0 | 10.0 | +0.0 |
| 0.80 | 10.0 | 8.0 | -2.0 |
| 0.75 | 10.0 | 8.0 | -2.0 |
| 0.70 | 8.0 | 8.0 | +0.0 |
| 0.65 | 8.0 | 8.0 | +0.0 |
| 0.64 | 6.0 | 8.0 | +2.0 |
| 0.63 | 6.0 | 6.0 | +0.0 |
| 0.62 | 6.0 | 6.0 | +0.0 |
| 0.61 | 6.0 | 4.5 | -1.5 |
| 0.60 | 6.0 | 4.5 | -1.5 |
| 0.55 | 4.5 | 3.0 | -1.5 |
| 0.53 | 4.5 | 3.0 | -1.5 |
| 0.50 | 3.0 | 3.0 | +0.0 |

### Key Changes by Region

1. **0.75-0.83 (currently Strong/10.0):** Drops to Good/8.0 under proposed. This is the largest impact zone.
2. **0.65-0.74 (currently Good/8.0):** Stays at 8.0 (proposed threshold is 0.64, so 0.64-0.83 all map to 8.0).
3. **0.62-0.64 (currently Possible/6.0):** Some move UP to Good/8.0 (specifically 0.64), others stay at 6.0.
4. **0.60-0.61 (currently Possible/6.0):** Drops to Noise/4.5.
5. **0.53-0.59 (currently Noise/4.5):** Drops to Default/3.0.

## Raw Cosine Similarity Distribution

Total similarity values extracted: 117592
- Offering-to-Seeking: 58796
- Audience Alignment: 58796

**Overall:** mean=0.5951, median=0.5950, stdev=0.0917
**Offering-to-Seeking:** mean=0.5869, median=0.5910
**Audience Alignment:** mean=0.6032, median=0.6000

| Bucket | Count | Percentage |
|--------|-------|------------|
| 0.90+ | 96 | 0.1% |
| 0.84-0.89 | 583 | 0.5% |
| 0.75-0.83 | 4595 | 3.9% |
| 0.65-0.74 | 26796 | 22.8% |
| 0.64 | 4602 | 3.9% |
| 0.62-0.63 | 9709 | 8.3% |
| 0.60-0.61 | 10265 | 8.7% |
| 0.53-0.59 | 33654 | 28.6% |
| <0.53 | 27292 | 23.2% |

## Tier Change Analysis

| Metric | Value |
|--------|-------|
| Total matches analyzed | 29863 |
| Matches that change tier | 3184 (10.7%) |
| Moved UP in tier | 136 |
| Moved DOWN in tier | 3048 |

### Tier Transitions

| Transition | Count |
|------------|-------|
| strong -> wildcard | 2791 |
| hand_picked -> strong | 257 |
| wildcard -> strong | 102 |
| strong -> hand_picked | 34 |

### Tier Distribution (Before vs After)

| Tier | Before | After | Delta |
|------|--------|-------|-------|
| hand_picked | 673 | 450 | -223 |
| strong | 20854 | 18388 | -2466 |
| wildcard | 8336 | 11025 | +2689 |

### Harmonic Mean Score Impact

| Metric | Value |
|--------|-------|
| Mean delta | -1.1275 |
| Median delta | -1.0500 |
| Stdev delta | 1.1300 |
| Min delta | -4.66 |
| Max delta | 3.35 |
| Matches scoring LOWER | 21871 (73.2%) |
| Matches scoring SAME | 5671 (19.0%) |
| Matches scoring HIGHER | 2321 (7.8%) |

## Spot-Check: Matches Near Threshold Boundaries

These matches were selected because they are near tier boundaries or experienced
the largest score shifts under the proposed thresholds.

### [1] Robin Sherman <-> Dave Feldman

- **A seeking:** Mental health professionals for holistic wellbeing programs, spiritual coaches for intuition-based t
- **B offering:** Podcast appearances and speaking on sustainability/wellbeing topics
- **A serves:** I'm passionate about sharing this journey with others, helping them rediscover their inner strength 
- **B serves:** They serve educators, youth, communities, and workplaces, aiming to promote wellbeing and positive t
- **Raw cosine similarities:** {'ab_Offering↔Seeking': 0.559, 'ab_Audience Alignment': 0.537, 'ba_Offering↔Seeking': 0.532, 'ba_Audience Alignment': 0.537}
- **Current:** HM=63.13, tier=strong
- **Proposed:** HM=58.47, tier=strong
- **Delta:** -4.66

### [2] Luke Zahradka <-> Martha Alexander

- **A seeking:** Marketing agencies and growth-focused businesses for joint venture broker partnerships. Affiliate pr
- **B offering:** Leadership and management services within the organization.
- **A serves:** Marketing agencies, growth-focused businesses, and affiliate program managers seeking JV launch expe
- **B serves:** Those who aspire to live their best life, grow into their purpose, maximize their potential, and flo
- **Raw cosine similarities:** {'ab_Offering↔Seeking': 0.567, 'ab_Audience Alignment': 0.551, 'ba_Offering↔Seeking': 0.569, 'ba_Audience Alignment': 0.551}
- **Current:** HM=59.25, tier=strong
- **Proposed:** HM=54.95, tier=wildcard
- **Delta:** -4.30

### [3] Etta Hornsteiner <-> Kara James

- **A seeking:** Spiritual wellness practitioners and holistic health coaches for content creation and online course 
- **B offering:** Elevate Growth Consulting
- **A serves:** Individuals seeking integrative health, spiritual wellness, and holistic mind-body-spirit transforma
- **B serves:** Service-based business owners
- **Raw cosine similarities:** {'ab_Offering↔Seeking': 0.564, 'ab_Audience Alignment': 0.566, 'ba_Offering↔Seeking': 0.564, 'ba_Audience Alignment': 0.566}
- **Current:** HM=59.04, tier=strong
- **Proposed:** HM=54.75, tier=wildcard
- **Delta:** -4.29

### [4] Donna Price <-> Jennifer Glass

- **A seeking:** Partnerships with speaker bureaus and event planners, podcast guest swaps with marketing and social 
- **B offering:** Podcast Host | Interviewing Top Business Leaders on Success, Innovation, and Growth Strategies focus
- **A serves:** The platform serves event planners and organizations looking for speakers, as well as speakers seeki
- **B serves:** Small and mid-sized businesses
- **Raw cosine similarities:** {'ab_Offering↔Seeking': 0.555, 'ab_Audience Alignment': 0.575, 'ba_Offering↔Seeking': 0.582, 'ba_Audience Alignment': 0.575}
- **Current:** HM=61.62, tier=strong
- **Proposed:** HM=57.39, tier=strong
- **Delta:** -4.23

### [5] Dr Dolores Fazzino, DNP, Nurse <-> Carla Salteris

- **A seeking:** Medical intuitive networks and holistic health practitioners for membership platform and group coach
- **B offering:** Love Odyssey Coaching, LLC
- **A serves:** Holistic health seekers, medical intuitive clients, and spiritual wellness professionals.
- **B serves:** This masterclass is for women who want to feel well and grounded again, are spiritually curious and 
- **Raw cosine similarities:** {'ab_Offering↔Seeking': 0.576, 'ab_Audience Alignment': 0.569, 'ba_Offering↔Seeking': 0.571, 'ba_Audience Alignment': 0.569}
- **Current:** HM=57.06, tier=strong
- **Proposed:** HM=52.84, tier=wildcard
- **Delta:** -4.22

## Verification

To validate the simulation, we recomputed current scores using the existing
thresholds and compared against stored harmonic means.

- **Average deviation from stored HM:** 0.0170
- **Max deviation from stored HM:** 2.6900

NOTE: Max deviation of 2.69 suggests some matches may have
other scoring factors that differ. This is expected for matches scored with
different code versions or with null momentum dimensions.

## Recommendation

**ADJUST**

The proposed thresholds would change tiers for 10.7% of matches. While the optimal F1 at 0.64 is well-supported by Study 1.3, the combined effect of raising the Strong threshold to 0.84 and compressing the middle range may be too aggressive. Consider deploying the 0.64 Good threshold independently first, then evaluating the Strong threshold separately.

### Risk Factors

- 3048 matches move to a lower tier, which could affect user trust if
  previously-seen hand_picked matches suddenly appear as strong or wildcard.
- The 0.75-0.83 similarity range is the most impacted zone; these pairs
  were previously scored as Strong (10.0) and would become Good (8.0).

### Benefits

- 136 matches move to a higher tier, suggesting previously under-valued
  pairs near the 0.64 boundary are being correctly recognized.
- Better alignment with empirical F1 data from Study 1.3.
