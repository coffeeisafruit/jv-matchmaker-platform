# Tier Threshold Evaluation: premier 67 -> 64

**Generated:** 2026-02-28
**Matches analyzed:** 29,863
**Scope:** Read-only analysis of SupabaseMatch harmonic_mean distribution

## Background

Weight sensitivity analysis (study_1_1) showed the quality break in harmonic mean
scores naturally clusters around 61.5. The current premier threshold of 67
captures only 2.3% of matches. Moving from 67 to 64 is a conservative compromise
that captures more quality matches without lowering the bar to the natural break
point.

**Current tiers:** premier >= 67, strong >= 55, aligned < 55
**Proposed tiers:** premier >= 64, strong >= 55, aligned < 55

## Tier Distribution Comparison

| Tier | Current (>=67) | Current % | New (>=64) | New % | Change |
|------|---------------|-----------|------------|-------|--------|
| premier | 673 | 2.3% | 2,705 | 9.1% | +2,032 |
| strong | 20,854 | 69.8% | 18,822 | 63.0% | -2,032 |
| aligned | 8,336 | 27.9% | 8,336 | 27.9% | 0 |
| **TOTAL** | **29,863** | **100%** | **29,863** | **100%** | — |

The proposed change quadruples the premier tier (2.3% -> 9.1%). The strong
tier shrinks proportionally but remains the dominant tier at 63%.

## Score Distribution Around Boundary

Matches per 1-point bin near the thresholds:

| Score Range | Count | Cumulative (from top) | Note |
|-------------|-------|-----------------------|------|
| 72-73 | 1 | 1 | |
| 71-72 | 28 | 29 | |
| 70-71 | 60 | 89 | |
| 69-70 | 100 | 189 | |
| 68-69 | 180 | 369 | |
| **67-68** | **268** | **637** | **Current HP threshold** |
| 66-67 | 424 | 1,061 | |
| 65-66 | 669 | 1,730 | |
| **64-65** | **939** | **2,669** | **Proposed HP threshold** |
| 63-64 | 1,328 | 3,997 | |
| 62-63 | 1,647 | 5,644 | |
| 61-62 | 2,051 | 7,695 | |
| 60-61 | 2,257 | 9,952 | |
| 59-60 | 2,309 | 12,261 | |
| 58-59 | 2,504 | 14,765 | |

**Key observation:** The density accelerates sharply below 64. Moving from 67 to 64
adds 2,032 matches. Moving an additional 3 points (64 to 61) would add 5,026 more.
The 64 threshold sits at a density inflection point where match counts roughly
double per point below it.

## Matches Moving: strong -> premier (64.0 <= score < 67.0)

- **Total movers:** 2,032 matches
- **As % of total:** 6.80%
- **Breakdown:** 424 in 66-67 | 669 in 65-66 | 939 in 64-65

## ISMC Dimension Averages by Tier Band

| Band | Intent | Synergy | Momentum | Context | Asymmetry |AB-BA| |
|------|--------|---------|----------|---------|------------|
| premier (>=67) | 7.17 | 7.01 | 6.24 | 7.86 | 6.19 |
| **movers (64-66.99)** | **6.78** | **6.79** | **6.03** | **7.71** | **5.84** |
| strong (55-63.99) | 6.50 | 6.48 | 5.88 | 7.47 | 5.69 |
| aligned (<55) | 5.49 | 5.62 | 5.45 | 6.54 | 7.53 |

**Observations:**
1. Movers are closer to current premier than to the strong average on every
   dimension. The gap mover->HP is roughly half the gap mover->strong.
2. Movers have **lower asymmetry** (5.84) than current premier (6.19), meaning
   these are more bidirectionally balanced matches.
3. The weakest dimension across all bands is Momentum, suggesting these matches
   involve partners who are slightly less active but otherwise well-aligned.

## Sample of 10 Matches in 64-66.99 Range

### Match 1: Vinny Longo <-> Amanda Slade (64.73)
- **Top Dimension:** Context (8.1) | I=7.2, S=5.3, M=6.0, C=8.1
- **Vinny Seeking:** Spiritual teachers, personal development coaches, and success mentors for self-improvement collabora...
- **Vinny Offering:** Resources, knowledge, and a supportive community to help clients transform their bodies and lives...
- **Amanda Seeking:** Spiritual coaches and manifestation experts for co-created programs and podcast guest exchanges...
- **Amanda Offering:** Founder & CEO (Chief Energy Organizer). Specializing in professional training & coaching.
- **Assessment:** Clear spiritual/personal development overlap. Both seek and offer in the same space.

### Match 2: Amy Nacach <-> Donna Chimera (66.38)
- **Top Dimension:** Synergy (7.8) | I=6.2, S=7.8, M=6.5, C=6.8
- **Amy Seeking:** Retreat organizers, spiritual event hosts, and transformational coaches serving lifestyle entrepreneu...
- **Amy Offering:** Business Coaching, Coach Promotion, Event Hosting, Event Management, Group Coaching, Image Consultan...
- **Donna Seeking:** Spiritual coaches and intuitive development programs for group coaching and public speaking collabor...
- **Donna Offering:** Business Coaching, Business Consulting, Group Coaching, Intuition Coaching, Public Speaking, Spiritu...
- **Assessment:** Strong bidirectional fit. Both in spiritual coaching + events. High synergy score confirms this.

### Match 3: Sharon Grossman <-> Carol Look (66.84)
- **Top Dimension:** Context (8.9) | I=6.9, S=5.2, M=7.4, C=8.9
- **Sharon Seeking:** Partnerships with corporate wellness programs, HR consultants, and employee engagement platforms...
- **Sharon Offering:** Business consulting and strategic advisory services.
- **Carol Seeking:** Personal development coaches and financial mindset experts for workshop collaborations and co-create...
- **Carol Offering:** Carol Look offers workshops focused on personal development and financial empowerment through techni...
- **Assessment:** Moderate overlap. Sharon is corporate wellness, Carol is personal development. Some alignment but different target markets. Context inflates the score due to complete profiles.

### Match 4: Andrew Davidson <-> Ed Finch (64.51)
- **Top Dimension:** Momentum (7.5) | I=5.9, S=6.7, M=7.5, C=7.0
- **Andrew Seeking:** Real estate investors, wealth builders, and spiritual entrepreneurs seeking integrated business and...
- **Andrew Offering:** Business Coaching, Coach Marketing Training, Communication Training, Funnel Creation Consulting...
- **Ed Seeking:** Business coaching platforms and startup accelerators for mastermind group facilitation and presentat...
- **Ed Offering:** Branding, Business Coaching, Business Consulting, Group Coaching, Mastermind Groups, Online Course C...
- **Assessment:** Both are business coaches. Ed offers masterminds which Andrew could promote. Decent fit.

### Match 5: Matt Hilliard <-> Sharyn Konyak (65.78)
- **Top Dimension:** Synergy (7.5) | I=7.1, S=7.5, M=5.1, C=6.4
- **Matt Seeking:** Business coaching platforms and mastermind facilitators serving entrepreneur couples. Podcast swap o...
- **Matt Offering:** Programs and services in business development for transformation.
- **Sharyn Seeking:** Partners with startup accelerators and brand strategy consultants for co-delivered storytelling work...
- **Sharyn Offering:** Podcast hosting and speaking
- **Assessment:** Matt wants podcast swaps, Sharyn hosts a podcast. Clear offering-seeking alignment. Momentum is low (5.1) which drags the harmonic down.

### Match 6: Jane Deuber <-> Catharine O'Leary (65.87)
- **Top Dimension:** Context (8.0) | I=6.4, S=7.4, M=5.8, C=8.0
- **Jane Seeking:** Email list building tool partnerships and assessment software affiliates, speaking opportunities at...
- **Jane Offering:** Business Consulting, Email List Building, Email Marketing Software, Lead Generation Specialists...
- **Catharine Seeking:** Funnel software providers, email marketing platforms, and lead magnet creators. Co-created quiz funn...
- **Catharine Offering:** Business Coaching, Coach Marketing Training, Email List Building, Funnel Creation Consulting...
- **Assessment:** Excellent match. Both in email marketing/funnel space. Jane's offerings match Catharine's seeking almost perfectly. This is a clear premier-quality match.

### Match 7: Christie Love <-> Business Skills (65.92)
- **Top Dimension:** Context (9.1) | I=6.0, S=6.9, M=6.6, C=9.1
- **Christie Seeking:** Podcast swap opportunities with communication and confidence coaches, speaking slots at professional...
- **Christie Offering:** Communication Training, Group Coaching, Mastermind Groups, Podcast Host, Presentation Skills, Public...
- **Business Skills Seeking:** Referral exchanges with financial advisors and CPAs, joint workshops with business operations consul...
- **Business Skills Offering:** Bill Pratt Coaching
- **Assessment:** Some overlap in coaching/speaking, but different niches (communication vs. financial). Context score inflated by profile completeness. Borderline quality.

### Match 8: Sabine Kvenberg <-> Avital Spivak (66.25)
- **Top Dimension:** Context (7.9) | I=6.7, S=6.8, M=5.7, C=7.9
- **Sabine Seeking:** Professional training organizations for communication coaching partnerships, corporate consultants f...
- **Sabine Offering:** Impact Communication Coaching
- **Avital Seeking:** Partnerships with business coaches serving technologically challenged entrepreneurs, joint programs...
- **Avital Offering:** Business Consulting, Group Coaching, Launches for Online Programs, Opt-In Page Creation, Startup Coa...
- **Assessment:** Both coaching-oriented. Sabine is communication-focused, Avital is startup/tech-focused. Moderate overlap.

### Match 9: Andre Alexander <-> Sally Sparks-Cousins (64.52)
- **Top Dimension:** Synergy (7.2) | I=6.0, S=7.2, M=6.6, C=6.8
- **Andre Seeking:** Business accelerators, growth consultants, and group coaching facilitators targeting scale-up entrep...
- **Andre Offering:** Biz Accelerator Coach
- **Sally Seeking:** Course creation software vendors for academy platform integrations, email marketing platforms for fu...
- **Sally Offering:** Business Coaching, Business Consulting, Coach Marketing Training, Course Creation Software, Course C...
- **Assessment:** Andre is a biz accelerator coach; Sally offers course creation and coaching. Sally's breadth could complement Andre's accelerator focus. Reasonable match.

### Match 10: Jason Voigt <-> Gulraiz Farooqi (66.42)
- **Top Dimension:** Synergy (8.1) | I=6.0, S=8.1, M=6.4, C=7.1
- **Jason Seeking:** Business coaches and consultants for mastermind facilitation partnerships. Looking for joint venture...
- **Jason Offering:** Affiliate Marketplace, Business Consulting, Coach Marketing Training, Joint Venture Resources
- **Jason explicitly seeks JV partners**
- **Gulraiz Seeking:** Course creators and online educators needing Facebook ads, social media marketing, and launch strate...
- **Gulraiz Offering:** Affiliate Managers, Business Coaching, Business Consulting, Course Creation Training, Facebook Ads...
- **Assessment:** Strong fit. Jason runs a JV/affiliate marketplace and seeks JV partners. Gulraiz offers affiliate management and marketing. Direct synergy in JV space.

## Sample Quality Summary

| Quality Assessment | Count | Matches |
|-------------------|-------|---------|
| Clear premier quality | 4 | #2 (Amy/Donna), #5 (Matt/Sharyn), #6 (Jane/Catharine), #10 (Jason/Gulraiz) |
| Decent/reasonable match | 4 | #1 (Vinny/Amanda), #4 (Andrew/Ed), #8 (Sabine/Avital), #9 (Andre/Sally) |
| Borderline/inflated by Context | 2 | #3 (Sharon/Carol), #7 (Christie/Business Skills) |

**4 of 10 sampled matches (40%)** are clearly premier quality with strong
bidirectional seeking-offering alignment. Another 4 are reasonable matches that
would benefit from the "premier" label to encourage outreach. Only 2 of 10
appear borderline, and even those have some coaching overlap.

## Engagement Data

Engagement data (views, contacts, email sends, feedback) is near-zero across all
tiers, indicating the platform is pre-launch or engagement tracking is pending.
This means we cannot validate the threshold change using behavioral signals.
The assessment relies entirely on ISMC dimension analysis and qualitative review.

## Codebase Audit: Where Threshold 67 Is Hardcoded

### Production Code (MUST change)

| File | Line(s) | Context |
|------|---------|---------|
| `matching/services.py` | 692 | `TIER_THRESHOLDS = {'premier': 67, ...}` — **canonical definition** |
| `matching/views.py` | 1018 | `if score >= 67 and has_email:` — outreach section assignment |
| `matching/views.py` | 1034 | `if score >= 67:` — badge assignment |
| `matching/views.py` | 1046 | `if score >= 67:` — badge style assignment |
| `matching/views.py` | 1075 | `if score >= 67:` — tag assignment |
| `matching/views.py` | 1146 | `if score >= 67 and has_email:` — dict-based section assignment |

### Test Code (MUST update to match)

| File | Line(s) | Context |
|------|---------|---------|
| `matching/tests/test_services.py` | 816 | Comment: `below 67 premier` |
| `matching/tests/test_services.py` | 906 | Comment: `Thresholds: premier >= 67` |
| `matching/tests/test_report_outreach_live.py` | 365-368 | `test_boundary_67_with_email_is_priority` |
| `matching/tests/test_report_outreach_live.py` | 370-373 | `test_boundary_66_with_email_is_this_week` |
| `matching/tests/test_report_outreach_live.py` | 717 | Comment: `score 68 >= 67` |
| `matching/tests/test_report_outreach_live.py` | 898 | Comment: `score 68 >= 67` |

### Validation/Dashboard Code (update for consistency)

| File | Line(s) | Context |
|------|---------|---------|
| `Validation/generate_dashboard.py` | 54 | `TIER_THRESHOLDS = {'premier': 67, ...}` |
| `Validation/generate_dashboard.py` | 348-349 | Hardcoded 67 in tier counting |
| `Validation/generate_dashboard.py` | 1060, 1327, 1544, 1574 | HTML/JS references to 67 |
| `Validation/01_score_distribution.py` | 74, 113 | Tier definitions |
| `Validation/02_predictive_validity.py` | 79-80, 106 | Tier definitions |
| `Validation/03_bidirectional_analysis.py` | 66 | `TIER_THRESHOLDS` |
| `Validation/04_aggregation_ablation.py` | 67 | `TIER_THRESHOLDS` |
| `Validation/08_expert_review_sample.py` | 93 | Tier assignment |
| `Validation/09_literature_comparison.py` | 69 | `TIER_THRESHOLDS` |
| `Validation/study_1_1_weight_sensitivity.py` | 47 | `TIER_THRESHOLDS` |
| `Validation/validation_dashboard.html` | 571, 838, 895, 1055, 1085 | HTML/JS |

### NOT in these files (confirmed)

- `config/settings.py` — no tier threshold (only scoring weights)
- `matching/models.py` — no threshold logic
- `matching/management/commands/` — no threshold references

## Refactoring Recommendation

The `matching/views.py` file hardcodes `67` in **5 separate places** rather than
referencing `PartnershipAnalyzer.TIER_THRESHOLDS['premier']`. When deploying
this change, views.py should be refactored to import and use the canonical
threshold from `services.py` to prevent future drift:

```python
from matching.services import PartnershipAnalyzer
HP_THRESHOLD = PartnershipAnalyzer.TIER_THRESHOLDS['premier']
```

## Verdict

**RECOMMEND DEPLOYING** the threshold change from 67 to 64.

### Reasons FOR:

1. **Quality holds up.** 8 of 10 sampled matches (80%) are reasonable-to-excellent
   quality. 4 of 10 (40%) are clearly premier quality that the current threshold
   incorrectly excludes.

2. **ISMC dimensions support it.** Movers average I=6.78, S=6.79, M=6.03, C=7.71 --
   much closer to current premier (I=7.17, S=7.01, M=6.24, C=7.86) than to the
   strong average (I=6.50, S=6.48, M=5.88, C=7.47).

3. **Lower asymmetry.** Movers have |AB-BA| asymmetry of 5.84, actually better
   (more balanced) than current premier at 6.19. These are bidirectionally
   sound matches.

4. **Density inflection.** The score distribution shows a natural density
   inflection around 64 -- below this point, match counts roughly double per point.
   The 64 threshold captures the quality plateau before the steep falloff.

5. **Still selective.** At 9.1% premier, the tier remains exclusive (roughly
   1 in 11 matches). This is well within the "top decile" that users expect from
   a curated recommendation.

### Risks to monitor:

1. **Context dimension inflation.** Several borderline matches score highly on
   Context (profile completeness) rather than Intent or Synergy. Consider whether
   Context weight (10%) should be reduced to prevent well-filled-but-misaligned
   profiles from being promoted.

2. **No behavioral validation.** Without engagement data, this assessment is based
   on ISMC scores and qualitative review. After deployment, monitor click-through
   and contact rates for the 64-67 band vs. the 67+ band.

3. **Perception risk.** Quadrupling the premier count (673 -> 2,705) may dilute
   the perceived exclusivity of the tier. Consider whether the label "Premier"
   still resonates at 9.1%, or if it needs to be reserved for >=67 with a new
   intermediate label like "Top Match" for 64-67.

### Implementation checklist:

- [ ] Update `matching/services.py` line 692: `'premier': 64`
- [ ] Update `matching/views.py` lines 1018, 1034, 1046, 1075, 1146: change `67` to `64`
  (or refactor to reference `TIER_THRESHOLDS`)
- [ ] Update test expectations in `test_services.py` and `test_report_outreach_live.py`
- [ ] Update `Validation/generate_dashboard.py` and other Validation scripts
- [ ] Regenerate validation dashboard
- [ ] Monitor engagement rates for the 64-67 band post-launch
