# Data Utilization Audit

**Date:** 2026-02-14
**Scope:** All 22 Django models across 5 apps (core, matching, positioning, outreach, playbook)

---

## 1. Executive Summary

The JV Matchmaker Platform has a sophisticated enrichment pipeline that spends real money (AI calls, web scraping, Apollo.io) to collect rich business intelligence about partners — their offerings, who they serve, what they're seeking, and their niche. However, **the core ISMC scoring algorithm uses almost none of this data**. Instead, it scores matches based on field *presence* (does a LinkedIn URL exist?) rather than field *content* (does their offering complement your needs?). Meanwhile, a separate `PartnershipAnalyzer` does use this content for display-time insights, but its output doesn't feed back into match ranking.

**Key stats:**
- **200+ total fields** across 22 models
- **~15 fields** actively used in ISMC match scoring (mostly presence checks)
- **11 SupabaseProfile fields** completely unused by any matching or scoring logic
- **8+ Supabase columns** exist in the database but aren't defined in the Django model (schema drift)
- **Enrichment confidence metadata** is tracked per-field but never influences match quality

**Top 3 recommendations (by ROI):**
1. **Wire enrichment content into ISMC scoring** — Use `seeking`, `offering`, `who_you_serve`, `niche` for semantic matching instead of just checking they exist
2. **Fix schema drift** — Add the 8+ Supabase columns missing from the Django model (`pagerank_score`, `enrichment_metadata`, `recommendation_pressure_30d`, etc.)
3. **Use confidence scores in match ranking** — Penalize matches built on low-confidence or expired data

---

## 2. Field-by-Field Audit Tables

### 2.1 SupabaseProfile (matching/models.py:11-54)

Unmanaged model mapping to Supabase `profiles` table. 3,143+ records.

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `id` | UUID | Supabase | Everywhere | N/A (PK) | — |
| `auth_user_id` | UUID | Supabase | Nothing | No | Could link to User for auth context |
| `name` | Text | Supabase | PartnershipAnalyzer:972, MatchEnrichment:241, Context score:413 | Completeness check only | — |
| `email` | Text | Supabase, enrichment | Intent score:147, Context score:420 | Presence check only (services.py:147) | — |
| `phone` | Text | Supabase, enrichment | Nothing in scoring/matching | No | Could add to completeness |
| `company` | Text | Supabase | MatchEnrichment:242, Context score:414 | Completeness check only | — |
| `website` | Text | Supabase, enrichment | Intent score:135, Momentum:343, Context score:419 | Presence check only | — |
| `linkedin` | Text | Supabase, enrichment | Intent score:123, Momentum:343, Context score:418 | Presence check only | — |
| `avatar_url` | Text | Supabase | Nothing | No | Display only, fine as-is |
| `business_focus` | Text | Supabase | MatchEnrichment:251 (fallback) | No | Merge with `niche` or use in Synergy scoring |
| `status` | Text | Supabase | recommendation_pressure:91 (filter) | No | Used for membership filtering, fine |
| `service_provided` | Text | Supabase | Nothing | No | Redundant with `what_you_do`; consider dropping |
| `list_size` | Int | Supabase, enrichment | PartnershipAnalyzer:872-873 (scale insight) | No (only PartnershipAnalyzer) | **Should be in Synergy scoring** |
| `business_size` | Text | Supabase | Nothing | No | Could inform scale compatibility |
| `social_reach` | Int | Supabase | MatchEnrichment:293 (display) | No | **Should factor into Momentum scoring** |
| `role` | Text | Supabase | Nothing in matching | No | Access control only |
| `bio` | Text | Supabase, ai_research:283 | Nothing in scoring | No | Could feed content analysis |
| `tags` | Array | Supabase | Nothing | No | **Should be used for niche/keyword matching** |
| `notes` | Text | Supabase | MatchEnrichment:299 (display) | No | Operational notes, fine as-is |
| `what_you_do` | Text | Enrichment pipeline | PartnershipAnalyzer:840 (solution insight), MatchEnrichment:246 | **No — not in ISMC** | **Critical: should be in Synergy scoring** |
| `who_you_serve` | Text | Enrichment pipeline | PartnershipAnalyzer:813 (audience insight), MatchEnrichment:245 | **No — not in ISMC** | **Critical: should be in Synergy scoring** |
| `seeking` | Text | Enrichment pipeline | PartnershipAnalyzer:783 (seeking insight), MatchEnrichment:248 | **No — not in ISMC** | **Critical: should be in Intent scoring** |
| `offering` | Text | Enrichment pipeline | PartnershipAnalyzer:840,844 (solution insight), MatchEnrichment:249 | **No — not in ISMC** | **Critical: should be in Synergy scoring** |
| `signature_programs` | Text | Enrichment, smart_enrichment:338 | Nothing | No | Could be strong differentiator signal |
| `booking_link` | Text | Enrichment | Nothing | No | Indicates accessibility (intent signal) |
| `current_projects` | Text | Supabase | Nothing | No | Could inform Momentum scoring |
| `niche` | Text | Enrichment pipeline | PartnershipAnalyzer:814 (audience insight) | **No — not in ISMC** | **Should be in Synergy scoring** |
| `audience_type` | Text | Supabase | Nothing | No | Should complement `who_you_serve` |
| `created_at` | DateTime | Auto | Nothing in scoring | No | — |
| `updated_at` | DateTime | Auto | Momentum score:314 | Freshness calculation | — |
| `profile_updated_at` | DateTime | Supabase | Nothing | No | Redundant with `updated_at` |
| `last_active_at` | DateTime | Supabase | Default ordering | No | **Should be in Momentum scoring** |

**Ghost fields** (exist in Supabase DB but NOT in Django model):

| Field | Written By | Read By | Notes |
|-------|-----------|---------|-------|
| `pagerank_score` | compute_network_centrality:180 | Unknown (not in model) | Cannot be queried via ORM |
| `degree_centrality` | compute_network_centrality:181 | Unknown | Cannot be queried via ORM |
| `betweenness_centrality` | compute_network_centrality:182 | Unknown | Cannot be queried via ORM |
| `network_role` | compute_network_centrality:183 | Unknown | Cannot be queried via ORM |
| `centrality_updated_at` | compute_network_centrality:184 | Unknown | Cannot be queried via ORM |
| `recommendation_pressure_30d` | compute_recommendation_pressure:122,129 | Unknown | Cannot be queried via ORM |
| `pressure_updated_at` | compute_recommendation_pressure:123,130 | Unknown | Cannot be queried via ORM |
| `enrichment_metadata` | consolidate_enrichment:329 | consolidate_enrichment:194 (raw SQL) | JSONB — rich confidence data, invisible to ORM |
| `profile_confidence` | consolidate_enrichment:332 | consolidate_enrichment:194 (raw SQL) | Float 0-1, invisible to ORM |
| `last_enriched_at` | consolidate_enrichment:336 | consolidate_enrichment:194 (raw SQL) | Timestamp, invisible to ORM |

### 2.2 SupabaseMatch (matching/models.py:75-121)

Unmanaged model mapping to Supabase `match_suggestions` table.

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `id` | UUID | Supabase/jv-matcher | Everywhere | N/A (PK) | — |
| `profile_id` | UUID | jv-matcher | PartnershipAnalyzer:115-117, network_centrality:46 | Used as graph edge | Not a ForeignKey — consider adding |
| `suggested_profile_id` | UUID | jv-matcher | PartnershipAnalyzer:119-121, network_centrality:46 | Used as graph edge | Not a ForeignKey — consider adding |
| `match_score` | Decimal | jv-matcher | Nothing directly | Superseded by harmonic_mean | Legacy — could drop |
| `match_reason` | Text | jv-matcher | PartnershipAnalyzer:781,798 | Display only | — |
| `source` | Text | jv-matcher | Nothing in scoring | No | — |
| `status` | Text | jv-matcher, UI | Nothing in scoring | No | Could filter stale matches |
| `suggested_at` | DateTime | Auto | Nothing | No | — |
| `viewed_at` | DateTime | UI | Nothing in scoring | No | Could inform engagement metrics |
| `contacted_at` | DateTime | UI | Nothing in scoring | No | Could inform engagement metrics |
| `notes` | Text | User | Nothing in scoring | No | — |
| `rich_analysis` | Text | jv-matcher | Display only | No | — |
| `analysis_generated_at` | DateTime | jv-matcher | Nothing | No | — |
| `email_sent_at` | DateTime | Outreach | Nothing in scoring | No | Could inform engagement |
| `user_feedback` | CharField | User | Nothing in scoring | No | **Should feed into outcome learning** |
| `feedback_at` | DateTime | User | Nothing | No | — |
| `match_context` | JSON | jv-matcher | Nothing | No | Rich context data, unused |
| `score_ab` | Decimal | jv-matcher | Nothing directly | No | Bidirectional score, unused |
| `score_ba` | Decimal | jv-matcher | Nothing directly | No | Bidirectional score, unused |
| `harmonic_mean` | Decimal | jv-matcher | PartnershipAnalyzer:731, network_centrality:46 | Tier classification + graph weight | Primary ranking metric |
| `scale_symmetry_score` | Decimal | jv-matcher | Nothing | No | Could enhance scale matching |
| `trust_level` | Text | jv-matcher | Nothing in scoring | No | Could weight match confidence |
| `expires_at` | DateTime | jv-matcher | Nothing | No | Could trigger re-scoring |
| `draft_intro_clicked_at` | DateTime | UI | Nothing | No | Engagement signal |

### 2.3 Profile (matching/models.py:128-187)

Django-managed partner profiles with enrichment data.

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | Creation | All scoring | Ownership | — |
| `name` | Char | Import/manual | Context score:413 | Completeness check | — |
| `company` | Char | Import/manual | Context score:414 | Completeness check | — |
| `linkedin_url` | URL | Import/manual | Intent:123, Momentum:343, Context:418 | Presence check | — |
| `website_url` | URL | Import/manual | Intent:135, Momentum:343, Context:419,454 | Presence check | — |
| `email` | Email | Import/manual | Intent:147, Context:420 | Presence check | — |
| `industry` | Char | Import/manual | Synergy:229-234 | String match vs ICP | — |
| `audience_size` | Char | Import/manual | Synergy:213, Momentum:362, Context:416 | Category comparison | — |
| `audience_description` | Text | Import/manual | Synergy:262-264, Context:417 | Word count only | **Should use NLP content matching** |
| `content_style` | Text | Import/manual | Synergy:247-249 | Boolean check only | **Should use NLP comparison** |
| `collaboration_history` | JSON | Import/manual | Intent:107-111 | Count of entries | — |
| `enrichment_data` | JSON | Enrichment | Intent:160-162 | Field count only | **Should use actual content** |
| `source` | Char | Import | Context:441 | Source quality score | — |
| `created_at` | DateTime | Auto | Default ordering | No | — |
| `updated_at` | DateTime | Auto | Momentum:314 | Freshness calc | — |

### 2.4 Match (matching/models.py:190-301)

Django-managed match records with ISMC scores.

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | MatchScoringService:632 | Ownership | — | — |
| `profile` | FK→Profile | MatchScoringService:632 | All scoring | — | — |
| `intent_score` | Float | MatchScoringService:636 | Output | ISMC component | — |
| `synergy_score` | Float | MatchScoringService:637 | Output | ISMC component | — |
| `momentum_score` | Float | MatchScoringService:638 | Output | ISMC component | — |
| `context_score` | Float | MatchScoringService:639 | Output | ISMC component | — |
| `final_score` | Float | MatchScoringService:640 | Ranking | Harmonic mean | — |
| `score_breakdown` | JSON | MatchScoringService:641 | Display | Full explanation | — |
| `status` | Char | User/system | Nothing in re-scoring | No | — |
| `notes` | Text | User | Nothing | No | — |
| `created_at` | DateTime | Auto | Ordering | No | — |

### 2.5 MatchFeedback (matching/models.py:304-342)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `match` | FK→Match | User feedback | Nothing in scoring | No | **Should feed into outcome learning** |
| `rating` | Int(1-5) | User feedback | Nothing | No | **Critical: this is your ground truth for ML** |
| `accuracy_feedback` | Text | User feedback | Nothing | No | Could inform scoring calibration |
| `outcome` | Char | User feedback | Nothing | No | **Should weight future similar matches** |
| `created_at` | DateTime | Auto | Ordering | No | — |

### 2.6 SavedCandidate (matching/migrations/0003)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | UI | Ownership | N/A | — |
| `name` | Char | UI | Display | No | — |
| `company` | Char | UI | Display | No | — |
| `seeking` | Text | UI | Unknown | No | — |
| `offering` | Text | UI | Unknown | No | — |
| `niche` | Char | UI | Unknown | No | — |
| `list_size` | Int | UI | Unknown | No | — |
| `who_you_serve` | Text | UI | Unknown | No | — |
| `what_you_do` | Text | UI | Unknown | No | — |
| `added_to_directory` | FK→SupabaseProfile | UI | Bridge table | No | Bridges Supabase ↔ Django |

### 2.7 PartnerRecommendation (matching/migrations/0004)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | System | Ownership | N/A | — |
| `partner` | FK→SupabaseProfile | System | recommendation_pressure:48-52 | Pressure calc | — |
| `candidate` | FK→SavedCandidate | System | Unknown | No | — |
| `context` | Char | System | Unknown | No | — |
| `was_viewed` | Bool | UI | Unknown | No | Engagement signal |
| `viewed_at` | DateTime | UI | Unknown | No | — |
| `was_contacted` | Bool | UI | Unknown | No | Conversion signal |
| `contacted_at` | DateTime | UI | Unknown | No | — |

### 2.8 core.User (core/models.py:7-58)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `business_name` | Char | Onboarding | Display | No | — |
| `business_domain` | URL | Onboarding | MatchScoringService:88 (ICP) | Passed but placeholder logic | Should do real domain comparison |
| `business_description` | Text | Onboarding | MatchScoringService:87 (ICP) | Passed but placeholder logic | Should inform matching |
| `tier` | Char | Billing | Nothing in scoring | No | Could limit features |
| `matches_this_month` | Int | System | Rate limiting | No | — |
| `pvps_this_month` | Int | System | Rate limiting | No | — |
| `onboarding_completed` | Bool | Onboarding | Flow control | No | — |
| `icp_last_reviewed` | DateTime | User | Staleness check | No | — |

### 2.9 core.APIKey (core/models.py:61-89)

All fields operational (user, provider, encrypted_key, is_active). No relevance to scoring.

### 2.10 positioning.ICP (positioning/models.py:4-97)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | Onboarding | Ownership | — | — |
| `name` | Char | User | Display | No | — |
| `customer_type` | Char | User | Nothing in scoring | No | Could inform B2B vs B2C matching |
| `industry` | Char | User | PartnershipAnalyzer:810, Synergy:230-231 | Yes — keyword match vs partner niche | — |
| `company_size` | Char | User | Nothing in scoring | No | Could inform scale matching |
| `age_range` | Char | User | Nothing | No | B2C demographic matching |
| `income_level` | Char | User | Nothing | No | Could match partner's audience economics |
| `demographics` | Char | User | Nothing | No | — |
| `pain_points` | JSON | User | PartnershipAnalyzer:845-853 (solution fit) | Display-time insight only | **Should be in Synergy scoring** |
| `goals` | JSON | User | Nothing | No | Should match against partner offerings |
| `budget_range` | Char | User | Nothing | No | — |
| `decision_makers` | JSON | User | Nothing | No | — |

### 2.11 positioning.TransformationAnalysis (positioning/models.py:100-130)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | AI generation | Ownership | — | — |
| `icp` | FK→ICP | AI generation | Nothing in scoring | No | Links context |
| `before_state` | Text | AI generation | Nothing in scoring | No | Could inform solution fit matching |
| `after_state` | Text | AI generation | Nothing in scoring | No | Could inform solution fit matching |
| `transformation_summary` | Text | AI generation | Nothing in scoring | No | Could match against partner offerings |
| `key_obstacles` | JSON | AI generation | Nothing | No | — |
| `value_drivers` | JSON | AI generation | Nothing | No | — |

### 2.12 positioning.PainSignal (positioning/models.py:133-162)

| Field | Type | Written By | Read By | Used in Scoring? | Recommendation |
|-------|------|-----------|---------|-----------------|----------------|
| `user` | FK→User | User | Ownership | — | **Island table** |
| `signal_type` | Char | User | Nothing | No | No other table reads this |
| `description` | Text | User | Nothing | No | No other table reads this |
| `weight` | Float | User | Nothing | No | No other table reads this |
| `keywords` | JSON | User | Nothing | No | **Could detect partner signals** |
| `is_active` | Bool | User | Nothing | No | — |

**Verdict:** PainSignal is fully disconnected from matching. Could be valuable if its keywords were matched against partner activity signals.

### 2.13 positioning.LeadMagnetConcept (positioning/models.py:165-204)

All fields are functional for the lead magnet generation workflow. No direct relevance to matching.

### 2.14-2.21 Outreach Models (outreach/models.py)

**EmailConnection, SentEmail, PVP, OutreachTemplate, OutreachCampaign, OutreachActivity, OutreachSequence, OutreachEmail** — These are operational models for the outreach pipeline. They consume match results but don't feed back into scoring.

Notable gap: `SentEmail` tracks `opened_at`, `clicked_at`, `replied_at` — **these engagement signals are never fed back into matching quality assessment.** A partner who actually replies to outreach should boost future match confidence.

### 2.22-2.24 Playbook Models (playbook/models.py)

**LaunchPlay** (54 pre-defined plays) — standalone reference table, no FKs.
**GeneratedPlaybook** → User, TransformationAnalysis (cross-app link).
**GeneratedPlay** → GeneratedPlaybook, LaunchPlay.

These are correctly scoped for the playbook feature. No matching relevance.

---

## 3. Enrichment → Scoring Gap Analysis

### What the enrichment pipeline collects

The platform runs a progressive enrichment strategy (smart_enrichment_service.py:57-67):

1. **FREE:** Website scraping → extracts `what_you_do`, `who_you_serve`, `seeking`, `offering` (ai_research.py:148-163)
2. **FREE:** LinkedIn scraping → validates profile
3. **FREE:** Email domain → company research
4. **PAID:** Targeted OWL search → fills missing fields (smart_enrichment_service.py:293-328)
5. **PAID:** Full OWL deep research → comprehensive profile building

**Output stored in Supabase:** `seeking`, `who_you_serve`, `what_you_do`, `offering`, `niche` + confidence metadata (consolidate_enrichment.py:234)

### What ISMC scoring actually reads

| ISMC Component | What it checks | What it SHOULD check |
|----------------|----------------|----------------------|
| **Intent (45%)** | Has LinkedIn? Has email? Has website? Has collaboration history? enrichment_data field count (services.py:91-171) | Does their `seeking` match what you offer? Do they have a `booking_link` (accessibility)? Have they been recently active (`last_active_at`)? |
| **Synergy (25%)** | audience_size category, industry string match, content_style exists, audience_description word count (services.py:195-274) | NLP similarity between `who_you_serve` and ICP target. Semantic match of `offering` vs ICP `pain_points`. `niche` alignment. `tags` keyword overlap. |
| **Momentum (20%)** | Days since `updated_at`, enrichment_data exists (boolean), LinkedIn AND website exist, audience_size category (services.py:297-374) | `social_reach` trends, `last_active_at`, `current_projects` populated, confidence freshness from `enrichment_metadata` |
| **Context (10%)** | 8-field completeness count, source quality, website domain exists (services.py:397-478) | `profile_confidence` score, `trust_level` from SupabaseMatch, network centrality metrics, recommendation pressure |

### The disconnect visualized

```
ENRICHMENT PIPELINE                    ISMC SCORING ENGINE
========================              ========================
seeking ──────────────────┐
offering ─────────────────┤
who_you_serve ────────────┤  (never    ┌── linkedin_url exists?
what_you_do ──────────────┤   read)    ├── email exists?
niche ────────────────────┘           ├── website_url exists?
                                       ├── audience_size category
confidence metadata ──────── (never    ├── industry string
enrichment_metadata ──────── read)     ├── updated_at freshness
profile_confidence ───────── (ghost)   └── enrichment_data count
```

### Where content IS used (but not in ranking)

The `PartnershipAnalyzer` (services.py:672-1019) reads `seeking`, `offering`, `who_you_serve`, `niche`, and `list_size` to generate display-time insights. But this happens **after** matches are already ranked — it's presentation, not ranking.

---

## 4. Unused Data Inventory

### 4.1 Completely unused SupabaseProfile fields (11)

| Field | Potential Value | Recommendation |
|-------|----------------|----------------|
| `avatar_url` | Display only | Fine as-is |
| `auth_user_id` | Could link Supabase auth → Django User | Low priority |
| `business_size` | Scale compatibility signal | Add to Synergy scoring |
| `service_provided` | Redundant with what_you_do | Consider dropping |
| `role` | Access control only | Fine as-is |
| `tags` | **Rich keyword data for matching** | **High value — use in Synergy** |
| `booking_link` | Accessibility/intent signal | Add to Intent scoring |
| `signature_programs` | Differentiator signal (named products) | Could improve match specificity |
| `current_projects` | Momentum indicator | Add to Momentum scoring |
| `audience_type` | Audience characterization | Complements who_you_serve |
| `profile_updated_at` | Redundant with updated_at | Fine as-is |

### 4.2 Ghost fields (in Supabase, not in Django model)

These fields are written by management commands via `save(update_fields=[...])` or raw SQL, but since they're not defined on the Django model, **they cannot be read via the ORM**:

- `pagerank_score` — network importance
- `degree_centrality` — connection count
- `betweenness_centrality` — bridge position
- `network_role` — hub/bridge/specialist/newcomer
- `centrality_updated_at`
- `recommendation_pressure_30d` — fatigue prevention
- `pressure_updated_at`
- `enrichment_metadata` — per-field confidence JSONB
- `profile_confidence` — overall confidence (0-1)
- `last_enriched_at`

**Impact:** Network centrality and recommendation pressure are computed but can't be read back by the Django app unless accessed via raw SQL. This data could dramatically improve match ranking if it were accessible.

### 4.3 Confidence system: tracked but not leveraged

The `ConfidenceScorer` (confidence/confidence_scorer.py) implements:
- Source-based base confidence (manual: 1.0, apollo_verified: 0.95, owl: 0.85, website_scraped: 0.70)
- Exponential age decay per field type (seeking: 30 days, email: 90 days, niche: 180 days)
- Verification boost (+0.15 max)
- Cross-validation boost (+0.20 max)
- Expiry date calculation

**None of this is used in scoring.** A match based on 6-month-old scraped data scores the same as one based on yesterday's verified data.

### 4.4 Feedback loop: absent

`MatchFeedback.rating` (1-5) and `MatchFeedback.outcome` (successful/unsuccessful/pending) are collected but never feed back into the scoring algorithm. This is the ground truth for match quality — the most valuable signal for improving future matches.

Similarly, `SentEmail.replied_at` and `PartnerRecommendation.was_contacted` track real engagement but don't inform future recommendations.

---

## 5. Cross-App Relationship Gaps

### 5.1 Two parallel partner representations (no bridge)

| Django `Profile` | Supabase `SupabaseProfile` |
|------------------|---------------------------|
| `managed = True` | `managed = False` |
| Created by imports/manual | Pre-existing in Supabase |
| `user` FK to User | No user FK |
| ISMC scoring runs on this | PartnershipAnalyzer reads this |
| `enrichment_data` (JSON blob) | Individual enriched fields + metadata |

**Problem:** There is no FK or mapping between `Profile` and `SupabaseProfile`. They represent the same concept (a JV partner) but the ISMC scoring engine operates on `Profile` while the enrichment pipeline writes to `SupabaseProfile`. The two worlds don't inform each other.

### 5.2 SupabaseMatch UUID fields (not real ForeignKeys)

`SupabaseMatch.profile_id` and `suggested_profile_id` are plain `UUIDField`s with manual lookup methods (`get_profile()`, `get_suggested_profile()`). Django doesn't enforce referential integrity or enable reverse lookups (`profile.supabasematch_set`).

### 5.3 PainSignal: island table

`PainSignal` has keywords and signal types designed to detect partner activity — but nothing in the codebase reads them. Could be valuable for matching if connected to the enrichment pipeline's web scraping output.

### 5.4 Cross-app bridges that work well

- `OutreachSequence` → `SupabaseMatch`, `SupabaseProfile` (connects outreach to Supabase)
- `SavedCandidate` → `SupabaseProfile` (connects external candidates to directory)
- `GeneratedPlaybook` → `TransformationAnalysis` (connects playbook to positioning)
- `PVP` → `Match` (connects outreach to scoring)

---

## 6. Best Practices Comparison

### What partner matching platforms typically do

| Practice | Industry Standard | JV Matchmaker Status |
|----------|------------------|---------------------|
| **Content-based matching** | NLP/embedding similarity on offerings vs needs | Not implemented — uses field presence checks |
| **Outcome-based learning** | Feed match results back to improve scoring | Feedback collected but not used |
| **Confidence-weighted scoring** | Penalize matches built on stale/low-quality data | Confidence tracked but not used in scoring |
| **Bidirectional scoring** | Score A→B and B→A, show both | score_ab/score_ba exist in SupabaseMatch but unused |
| **Temporal signals** | Track changes over time, weight recent changes | Only updated_at freshness check |
| **Network effects** | Use graph position to find non-obvious matches | Centrality computed but not integrated |
| **Engagement signals** | Use email opens, clicks, replies to improve recs | SentEmail tracks these but doesn't feed back |
| **Semantic search** | Embeddings for "similar partner" discovery | Not implemented |
| **Decay/re-enrichment** | Auto-trigger when confidence drops below threshold | Decay model exists but no automation |
| **A/B testing** | Compare scoring algorithms on real outcomes | Not implemented |

### Key gap: scoring operates on metadata, not content

The biggest divergence from best practice is that ISMC scoring treats partner profiles as checklists ("do they have a LinkedIn?") rather than documents with meaning ("does their offering address my customer's pain?"). The enrichment pipeline collects the *right* data — it just never reaches the scoring engine.

---

## 7. Prioritized Recommendations

### Priority 1: Wire enrichment content into ISMC scoring

**Impact:** High — transforms matching from presence-based to content-based
**Complexity:** M
**Files to modify:**
- `matching/services.py` — MatchScoringService (all 4 score methods)
- `matching/models.py` — May need to add SupabaseProfile lookup to Profile or adapt scoring to work with SupabaseProfile directly

**What changes:**
- **Intent score:** If `seeking` mentions partnerships/JVs/affiliates, boost intent. If `booking_link` exists, boost accessibility.
- **Synergy score:** NLP keyword overlap between partner's `who_you_serve` and ICP `industry`/`pain_points`. Check `offering` vs ICP `goals`. Use `niche` alignment. Use `tags` for keyword matching.
- **Momentum score:** Use `last_active_at` instead of (or in addition to) `updated_at`. Factor in `social_reach`. Use `current_projects` as activity signal.
- **Context score:** Use `profile_confidence` (ghost field — need to add to model first). Weight by `trust_level` from SupabaseMatch.

### Priority 2: Fix schema drift — add ghost fields to Django model

**Impact:** High — unlocks network metrics and confidence data for ORM queries
**Complexity:** S
**Files to modify:**
- `matching/models.py` — Add 10 fields to SupabaseProfile

**What changes:**
Add to SupabaseProfile: `pagerank_score`, `degree_centrality`, `betweenness_centrality`, `network_role`, `centrality_updated_at`, `recommendation_pressure_30d`, `pressure_updated_at`, `enrichment_metadata`, `profile_confidence`, `last_enriched_at`

Since `managed = False`, no migrations needed — just declare the fields so the ORM can read them.

### Priority 3: Use confidence scores in match ranking

**Impact:** Medium — ensures match quality reflects data quality
**Complexity:** S (after Priority 2)
**Files to modify:**
- `matching/services.py` — Add confidence weighting to final score
- `matching/enrichment/confidence/confidence_scorer.py` — Already built, just needs integration

**What changes:**
After computing ISMC score, multiply by `profile_confidence` (or apply a penalty curve for low-confidence profiles). A match with 0.95 confidence keeps its score; a match with 0.3 confidence gets significantly penalized.

### Priority 4: Close the feedback loop

**Impact:** Medium-High (long-term) — enables continuous improvement
**Complexity:** M
**Files to modify:**
- `matching/services.py` — Add feedback-based weight adjustment
- `matching/models.py` — May need to add aggregate feedback stats

**What changes:**
When `MatchFeedback` records accumulate, use `rating` and `outcome` to adjust scoring weights. If matches in a certain `niche` consistently get low ratings, reduce synergy scores for that niche. If high `list_size` matches get better outcomes, adjust scale weighting.

### Priority 5: Use network centrality in scoring

**Impact:** Medium — surfaces non-obvious high-value matches
**Complexity:** S (after Priority 2)
**Files to modify:**
- `matching/services.py` — Add network position to Context or Intent scoring

**What changes:**
Partners classified as "hub" or "bridge" in the network graph are likely higher-value connections. Factor `pagerank_score` and `network_role` into scoring.

### Priority 6: Connect PainSignal to enrichment

**Impact:** Low-Medium — enables proactive matching based on signals
**Complexity:** M
**Files to modify:**
- `matching/enrichment/ai_research.py` — Check scraped content against PainSignal keywords
- `positioning/models.py` — Add matching integration

**What changes:**
When the enrichment pipeline scrapes a partner's website, check extracted text against the user's PainSignal keywords. If matches are found, boost the partner's Intent score.

### Priority 7: Bridge Profile ↔ SupabaseProfile

**Impact:** Medium — unifies the two partner representations
**Complexity:** M-L
**Files to modify:**
- `matching/models.py` — Add FK or UUID link
- `matching/services.py` — Adapt MatchScoringService to use unified data

**What changes:**
Add a `supabase_profile` FK (or UUID field) to `Profile` so Django-managed profiles can reference their Supabase counterpart. This lets ISMC scoring access enrichment data directly.

### Priority 8: Schema drift detection command

**Impact:** Low (preventive) — catches future drift automatically
**Complexity:** S
**Files to create:**
- `matching/management/commands/check_schema_drift.py`

**What changes:**
A management command that uses `connection.cursor()` to query `information_schema.columns` for the `profiles` table, compares against `SupabaseProfile._meta.get_fields()`, and reports any mismatches.
