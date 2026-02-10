# Matching Elements Documentation

This document identifies all the elements used to make matches in the JV Matchmaker Platform.

## Overview

The platform uses **two complementary matching systems**:

1. **Django Match Model** - Real-time scoring using ISMC framework (Intent, Synergy, Momentum, Context)
2. **SupabaseMatch** - Pre-computed matches based on seeking→offering alignment

---

## System 1: Django Match Model (ISMC Framework)

### Scoring Weights
- **Intent**: 45% weight
- **Synergy**: 25% weight
- **Momentum**: 20% weight
- **Context**: 10% weight

Final score uses **weighted harmonic mean** to penalize weak areas.

### 1. Intent Score (45% weight)

Measures signals indicating partnership interest:

#### Elements Used:
- **Collaboration History** (weight: 3)
  - `profile.collaboration_history` - JSON field with previous collaborations
  - Score: Based on number of collaborations (up to 10 points)

- **LinkedIn Presence** (weight: 2.5)
  - `profile.linkedin_url` - Presence of LinkedIn profile
  - Score: 8.0 if present, 0.0 if absent

- **Website Presence** (weight: 2)
  - `profile.website_url` - Presence of website
  - Score: 7.0 if present, 0.0 if absent

- **Contact Availability** (weight: 1.5)
  - `profile.email` - Direct email availability
  - Score: 9.0 if email present, 5.0 if indirect contact only

- **Data Enrichment** (weight: 1)
  - `profile.enrichment_data` - JSON field with Clay/enrichment data
  - Score: Based on data completeness (up to 10 points)

### 2. Synergy Score (25% weight)

Measures audience and content alignment:

#### Elements Used:
- **Audience Size Alignment** (weight: 3)
  - `profile.audience_size` - Profile's audience size (tiny/small/medium/large/massive)
  - `user.target_audience_size` - User's target audience size from ICP
  - Score: Based on size difference (10 - (diff * 2.5))

- **Industry Alignment** (weight: 3)
  - `profile.industry` - Profile's industry
  - `user.target_industries` - User's target industries from ICP
  - Score: 10.0 if exact match, 6.0 if industry specified but not in target list, 5.0 default

- **Content Style Compatibility** (weight: 2)
  - `profile.content_style` - Profile's content style description
  - Score: 7.0 if content style exists, 5.0 default

- **Audience Description Quality** (weight: 2)
  - `profile.audience_description` - Text description of audience
  - Score: Based on word count (50 words = full score)

### 3. Momentum Score (20% weight)

Measures recent activity and growth:

#### Elements Used:
- **Profile Freshness** (weight: 3)
  - `profile.updated_at` - Last profile update timestamp
  - Score: Decreases by 1 point per 3 days since update (max 10)

- **Data Recency** (weight: 2.5)
  - `profile.enrichment_data` - Timestamp of enrichment data
  - Score: 7.0 if recent enrichment data exists, 5.0 default

- **Activity Level** (weight: 2.5)
  - `profile.linkedin_url` - LinkedIn presence
  - `profile.website_url` - Website presence
  - Score: 8.0 if both present, 6.0 if one present, 3.0 if neither

- **Growth Potential** (weight: 2)
  - `profile.audience_size` - Current audience size
  - Score: 7.0 for small/medium (higher growth potential), 6.0 for large, 5.0 default

### 4. Context Score (10% weight)

Measures contextual relevance:

#### Elements Used:
- **Profile Completeness** (weight: 3)
  - Checks 8 fields: `name`, `company`, `industry`, `audience_size`, `audience_description`, `linkedin_url`, `website_url`, `email`
  - Score: (filled_count / 8) * 10

- **Data Source Quality** (weight: 2.5)
  - `profile.source` - Source of profile data (clay/linkedin/manual/import)
  - Score: 9.0 for Clay, 8.0 for LinkedIn, 6.0 for manual, 5.0 for import

- **Domain Relevance** (weight: 2.5)
  - `profile.website_url` - Website domain
  - `user.business_domain` - User's business domain from ICP
  - Score: 6.0 if domain analysis available, 5.0 default

- **Network Proximity** (weight: 2)
  - Mutual connections (placeholder - not fully implemented)
  - Score: 5.0 default (neutral)

---

## System 2: SupabaseMatch (Pre-computed Matches)

### Core Matching Algorithm

Pre-computed matches stored in `match_suggestions` table, based on **seeking→offering alignment**.

### Elements Used:

#### From SupabaseProfile (User's Profile):
- `seeking` - What the user is seeking/needs
- `offering` - What the user offers
- `who_you_serve` - Target audience description
- `niche` - Business niche
- `list_size` - Email list size
- `what_you_do` - Business description

#### From SupabaseProfile (Partner Profile):
- `seeking` - What the partner is seeking/needs
- `offering` - What the partner offers
- `who_you_serve` - Partner's target audience
- `niche` - Partner's business niche
- `list_size` - Partner's email list size
- `what_you_do` - Partner's business description

#### Match Scores:
- `score_ab` - Score from user's perspective (user needs → partner offers)
- `score_ba` - Score from partner's perspective (partner needs → user offers)
- `harmonic_mean` - Combined score using harmonic mean
- `scale_symmetry_score` - Compatibility based on list size ratios
- `match_reason` - Text explanation of why it's a match

#### Match Context:
- `match_context` - JSON field with additional matching context
- `trust_level` - Match quality tier (platinum, gold, bronze, legacy)

---

## System 3: PartnershipAnalyzer (Dynamic Insights)

Generates real-time partnership insights by combining multiple data sources.

### Elements Used:

#### 1. Seeking→Offering Alignment
- `user_profile.seeking` - What user needs
- `partner.offering` - What partner offers
- `supabase_match.match_reason` - Pre-computed match explanation
- `supabase_match.harmonic_mean` - Pre-computed match score

#### 2. Audience Overlap (ICP Integration)
- `icp.industry` - User's target industry from ICP
- `partner.who_you_serve` - Partner's target audience
- `partner.niche` - Partner's niche
- Keyword matching between ICP industry and partner's audience

#### 3. Solution Fit (Transformation Integration)
- `transformation.key_obstacles` - Customer pain points
- `transformation.value_drivers` - Value drivers
- `partner.offering` - Partner's services
- Overlap between partner offerings and ICP pain points

#### 4. Scale Compatibility
- `user_profile.list_size` - User's email list size
- `partner.list_size` - Partner's email list size
- Ratio calculation to determine compatibility:
  - Ratio ≤ 2: Equal list swap potential
  - Ratio 2-5: Growth/mentor opportunity
  - Ratio > 5: Significant scale difference

---

## Structured Intake Form (Proposed Enhancement)

The current `seeking` and `offering` fields are free text. Adding structured categories enables precise algorithmic matching.

### Step 1: What You Offer (select all that apply)

| Category | Field Value | Matches Seeking... |
|----------|-------------|-------------------|
| 📧 Email List / Audience | `email_list` | audience, exposure, list_swap |
| 🎙️ Podcast / Media | `podcast_media` | podcast_guest, visibility, PR |
| 💼 Coaching / Training | `coaching_training` | coaching, mentoring, guidance |
| 📚 Courses / Programs | `courses_programs` | education, curriculum, learning |
| 🛠️ Software / Tech | `software_tech` | tech, development, automation |
| 🤝 Referrals / Introductions | `referrals_intros` | referrals, connections, network |
| ✍️ Content / Copywriting | `content_copy` | content, copywriting, marketing |
| 🎤 Events / Speaking | `events_speaking` | speaking, stages, summits |

**Plus:** `offering_details` (text) - e.g., "50K email list of health-focused entrepreneurs"

### Step 2: What You Need (select all that apply)

| Category | Field Value | Matches Offering... |
|----------|-------------|---------------------|
| 👥 Audience / Exposure | `audience_exposure` | email_list, reach, followers |
| 🎙️ Podcast Guest Spots | `podcast_guest` | podcast_media, show, interview |
| 🚀 JV Launch Partners | `jv_launch` | launch, promotion, affiliates |
| 💰 Affiliates / Promoters | `affiliates_promoters` | affiliate, promotion, sales |
| 🔧 Tech / Development | `tech_dev` | software_tech, automation |
| 🎯 Coaching / Mentoring | `coaching_mentoring` | coaching_training, consulting |
| 📈 Leads / Referrals | `leads_referrals` | referrals_intros, clients |
| 🏪 Service Providers | `service_providers` | services, agency, vendor |

**Plus:** `seeking_details` (text) - e.g., "Looking for podcasts in health/wellness with 10K+ downloads"

### Step 3: Partnership Types (select 2-3)

| Type | Field Value | Matching Logic |
|------|-------------|----------------|
| 🤝 Peer / Bundle | `peer_bundle` | Similar `list_size` (±2x), same `niche`, cross-promotion |
| ⬅️ Referral (Before) | `referral_before` | Partner serves clients BEFORE you in journey |
| ➡️ Referral (After) | `referral_after` | Partner serves clients AFTER you in journey |
| 🛒 Service Provider | `service_provider` | Vendor/service needed for business operations |

### Proposed Schema Changes

```python
# New fields for SupabaseProfile model
offering_categories = ArrayField(CharField)  # ['email_list', 'podcast_media']
offering_details = TextField()               # Free text description
seeking_categories = ArrayField(CharField)   # ['jv_launch', 'affiliates_promoters']
seeking_details = TextField()                # Free text description
partnership_types = ArrayField(CharField)    # ['peer_bundle', 'referral_after']
```

### Enhanced Matching Algorithm

**Bidirectional Match Score:**
```
match_score = harmonic_mean(
    offer_need_score,    # User.offering ∩ Partner.seeking
    need_offer_score,    # User.seeking ∩ Partner.offering
    partnership_compat,  # Overlapping partnership_types
    scale_compatibility  # list_size ratio check
)
```

**Partnership Type Filters:**
- `peer_bundle` → Require `list_size` ratio ≤ 2x
- `referral_before` → Partner's `who_you_serve` = earlier customer journey stage
- `referral_after` → Partner's `who_you_serve` = later customer journey stage
- `service_provider` → Match `seeking_categories` to `offering_categories` (one-directional)

---

## Profile Data Sources

### SupabaseProfile Fields (3,143+ profiles):
- Basic Info: `name`, `email`, `phone`, `company`, `website`, `linkedin`
- Business: `business_focus`, `service_provided`, `niche`, `what_you_do`, `who_you_serve`
- Partnership: `seeking`, `offering`, `current_projects`
- Metrics: `list_size`, `social_reach`, `business_size`
- Metadata: `status` (Member/Non Member Resource/Pending), `tags`, `bio`, `notes`
- Activity: `last_active_at`, `profile_updated_at`

### Django Profile Fields (User's own profiles):
- Basic: `name`, `company`, `linkedin_url`, `website_url`, `email`
- Classification: `industry`, `audience_size`, `audience_description`
- Content: `content_style`
- History: `collaboration_history` (JSON)
- Enrichment: `enrichment_data` (JSON from Clay/APIs)
- Source: `source` (manual/clay/linkedin/import)

### ICP Fields (User's target customer profile):
- `industry` - Target industry/niche
- `customer_type` - B2B or B2C
- `company_size` - For B2B (solo/small/medium/enterprise)
- `age_range`, `income_level`, `demographics` - For B2C
- `pain_points` - JSON array of pain points
- `goals` - JSON array of goals
- `budget_range` - Budget information
- `decision_makers` - JSON with decision maker info

### TransformationAnalysis Fields:
- `before_state` - Where customer starts
- `after_state` - Where customer ends up
- `transformation_summary` - Summary of transformation
- `key_obstacles` - JSON array of obstacles
- `value_drivers` - JSON array of value drivers

---

## Match Outputs

### Django Match Output:
- `intent_score` - 0-1 scale
- `synergy_score` - 0-1 scale
- `momentum_score` - 0-1 scale
- `context_score` - 0-1 scale
- `final_score` - 0-1 scale (harmonic mean)
- `score_breakdown` - JSON with detailed factors and explanations
- `status` - new/contacted/in_progress/converted/declined

### SupabaseMatch Output:
- `harmonic_mean` - 0-100 scale (primary score)
- `score_ab` - User → Partner score
- `score_ba` - Partner → User score
- `scale_symmetry_score` - Scale compatibility
- `match_reason` - Text explanation
- `trust_level` - Quality tier
- `status` - pending/viewed/contacted/connected/dismissed

### PartnershipAnalysis Output:
- `tier` - hand_picked (80%+), strong (60-80%), wildcard (<60%)
- `score` - Harmonic mean from SupabaseMatch
- `insights` - Array of PartnershipInsight objects:
  - `seeking_offering` - Service match
  - `audience_overlap` - Shared audience
  - `solution_fit` - Solution provider match
  - `scale_match` - Scale compatibility
- `suggested_action` - Recommended next step
- `conversation_starter` - Suggested conversation opener

---

## Summary

**Total Matching Elements: ~40+ fields** across:
- Profile data (basic info, business focus, metrics)
- Partnership signals (seeking, offering, collaboration history)
- Audience alignment (industry, niche, who_you_serve)
- Activity signals (freshness, recency, presence)
- Scale metrics (list_size, social_reach)
- ICP alignment (industry, pain_points, goals)
- Transformation fit (obstacles, value_drivers)

The system combines **pre-computed matches** (SupabaseMatch) with **real-time analysis** (PartnershipAnalyzer) and **scoring** (MatchScoringService) to provide comprehensive partnership recommendations.

---

## Proposed Enhancement: Structured Intake

**Current State:** `seeking` and `offering` are free text fields requiring keyword matching.

**Proposed State:** Add structured categories via 3-step intake form (~60 seconds):
1. **What You Offer** - 8 categories + details text
2. **What You Need** - 8 categories + details text
3. **Partnership Types** - 4 types (peer, before, after, vendor)

**Benefits:**
- Precise category-to-category matching (not fuzzy keyword search)
- Bidirectional matching (both parties must benefit)
- Partnership type filtering (list swaps vs referrals vs vendors)
- Customer journey positioning (before/after referral chains)

**Implementation:** Keap intake form → sync to Supabase `offering_categories`, `seeking_categories`, `partnership_types` fields.
