# JV Matchmaker - Product Requirements Document

**Version:** 1.5
**Last Updated:** January 2025
**Status:** v1.0 Complete, v1.5 In Development

---

## Version Roadmap

| Version | Focus | Status |
|---------|-------|--------|
| **v1.0** | JV Partner Discovery & Matching | ✅ Complete |
| **v1.5** | JV Pipeline Management (6-stage workflow) | 🔄 In Development |
| **v2.0** | Co-Sell Execution Integration | 📋 Planned |

---

## 1. Problem Statement

### The Pain
Online course creators, coaches, and info-product sellers struggle to grow through joint venture partnerships because:

1. **Discovery is manual** - Finding compatible partners takes 10-20+ hours/month of research
2. **Outreach is generic** - Cold pitches feel spammy and get ignored (2-5% response rate)
3. **No system exists** - Most do JV partnerships inconsistently or not at all
4. **Asymmetric value** - Hard to find partners where BOTH sides benefit equally
5. **No relationship tracking** - Partnerships fall through cracks without a pipeline

### The Cost of Inaction
- Reliance on expensive paid ads ($50-200+ CAC)
- Revenue plateaus without new audience exposure
- Missed launch opportunities (no promotional partners)
- Competitor partnerships that could have been yours

### Current Alternatives
| Alternative | Problem |
|-------------|---------|
| Manual LinkedIn/Google research | Time-consuming, inconsistent |
| Affiliate networks (ClickBank, JVZoo) | Low-quality, transactional relationships |
| Networking events | Expensive, random connections |
| Hiring a BD person | $60K+/year, still needs tools |
| Spreadsheets | No workflow, easy to lose track |

---

## 2. Target Users

### Primary Persona: "Launch Lisa"

| Attribute | Description |
|-----------|-------------|
| **Role** | Course creator, coach, or consultant |
| **Revenue** | $100K - $2M annually |
| **Audience** | 1K - 100K email list |
| **Business Model** | Sells courses, coaching, memberships, digital products |
| **Tech Savvy** | Comfortable with SaaS tools, uses Kajabi/Teachable/etc. |
| **Growth Goal** | Wants to double revenue via partnerships, not ads |

### Jobs to Be Done
1. "Help me find partners who actually WANT what I offer"
2. "Tell me exactly what to say in my outreach"
3. "Give me a system I can repeat every launch"
4. "Show me who's most likely to say yes"
5. "Track my partner relationships through the whole workflow" *(v1.5)*

### Secondary Users
- **Affiliate Managers** - Finding affiliates for client launches
- **Launch Strategists** - Systematizing partner recruitment
- **Podcast Hosts** - Finding guests who promote episodes
- **Event Organizers** - Recruiting speakers with audiences
- **B2B Partnership Teams** - Co-sell execution *(v2.0)*

### Non-Users (Out of Scope)
- Beginners with no offer yet
- Enterprise companies with BD teams
- E-commerce/physical product brands
- People who won't do any outreach

---

## 3. Product Vision

### One-Liner
> "JV Matchmaker is the AI-powered partner discovery and relationship platform that helps course creators find, pitch, and close JV partnerships in hours instead of weeks."

### Vision Statement
Replace the 20-hour manual partner research process with an intelligent matching system that:
- Scores partners for **mutual fit** (not just one-sided)
- Generates **personalized pitches** (not templates)
- Provides **execution playbooks** (not just introductions)
- Tracks relationships through a **6-stage workflow** *(v1.5)*
- Enables **co-sell execution** with B2B partners *(v2.0)*

### Success Metrics (North Star)
| Metric | Target | Why It Matters |
|--------|--------|----------------|
| **Partnerships Closed** | 3+ per user per quarter | Ultimate value delivery |
| **Time Saved** | 15+ hours/month | Core pain relief |
| **Response Rate** | 15%+ (vs 2-5% baseline) | Pitch quality indicator |
| **Pipeline Velocity** | 30-day avg stage progression | Relationship momentum |
| **NPS** | 50+ | Product-market fit |

---

## 4. Core Features

### v1.0: JV Partner Discovery (✅ Complete)

#### 4.1 Partner Discovery
**What:** Searchable database of 3,000+ JV-ready partners
**Why:** Eliminates manual research
**Status:** ✅ Built
- [x] Search by niche, audience size, offering type
- [x] Filter by collaboration readiness
- [x] View partner profiles with key metrics
- [x] Supabase integration for profile storage

#### 4.2 Match Scoring
**What:** AI-powered compatibility scoring
**Why:** Surface best-fit partners, not just any partners
**Status:** ✅ Built
- [x] Bidirectional scoring (A→B and B→A)
- [x] Harmonic mean to penalize lopsided matches
- [x] Score breakdown explanation
- [x] Scores: Intent (45%), Synergy (25%), Momentum (20%), Context (10%)

#### 4.3 ICP Definition
**What:** Define your Ideal Customer Profile for partners
**Why:** Personalize matching algorithm
**Status:** ✅ Built
- [x] Multi-step ICP wizard
- [x] Monthly review reminders
- [x] ICP influences match scoring
- [x] Transformation analysis integration

#### 4.4 PVP Generator
**What:** AI-generated Partner Value Propositions
**Why:** Personalized pitches > generic templates
**Status:** ✅ Built
- [x] Generate pitch for any match
- [x] Multiple pitch styles (lookalike, trigger-based, creative)
- [x] One-click copy
- [x] Quality scoring (8.0+ threshold)

#### 4.5 Clay Enrichment Integration
**What:** Auto-enrich profiles with Clay.com data
**Why:** Better data = better matches
**Status:** ✅ Built
- [x] Webhook receives Clay data
- [x] Maps to SupabaseProfile fields
- [x] Triggers match recalculation
- [x] 9 AutoClaygent patterns documented

---

### v1.5: JV Pipeline Management (🔄 In Development)

#### 4.6 JV Relationship Tracking
**What:** Track JV partner relationships through 6-stage workflow
**Why:** Partnerships need nurturing, not just discovery
**Status:** 🔄 Planned
- [ ] `JVRelationship` model linking user to Supabase profiles
- [ ] 6-stage workflow: Match → Pitch → Close → Execute → Convert → Reciprocate
- [ ] Sub-status tracking within each stage
- [ ] Activity timeline per relationship

#### 4.7 JV Pipeline Board
**What:** Kanban-style view of JV pipeline by stage
**Why:** Visual pipeline management like a CRM
**Status:** 🔄 Planned
- [ ] 6 columns (one per stage)
- [ ] Drag-and-drop stage progression
- [ ] Deal value tracking per column
- [ ] Quick-view activity timeline

#### 4.8 JV Activity Log
**What:** Track all interactions with JV partners
**Why:** Never lose context on a relationship
**Status:** 🔄 Planned
- [ ] Activity types: notes, emails, calls, meetings, stage changes
- [ ] Automatic activity logging on stage/status changes
- [ ] Manual note addition
- [ ] Activity timeline view on partner detail

#### 4.9 54-Play Launch Library
**What:** AI-generated launch content sequences
**Why:** Partners need content to promote
**Status:** 🔄 Planned
- [ ] 54 plays across 6 launch phases
- [ ] Customized to user's transformation
- [ ] Exportable content calendar
- [ ] Aligned with BUILD_PLAN.md methodology

#### 4.10 GEX Email Sequences
**What:** 4-email outreach sequences
**Why:** Multi-touch > single email
**Status:** 🔄 Planned
- [ ] 4 emails: intro, context, alt-angle, hail-mary
- [ ] AI personalization per recipient
- [ ] Threading support

---

### v2.0: Co-Sell Execution (📋 Planned)

> **Full details:** See `v2.0/docs/planning/INTEGRATION_PLAN.md`

#### 4.11 Multi-Tenant Foundation
**What:** Organization-based multi-tenancy
**Why:** Support teams and B2B use cases
**Status:** 📋 Planned (v2.0 Phase 1)
- [ ] Tenant model with membership roles
- [ ] TenantMiddleware for request scoping
- [ ] Data migration for existing users

#### 4.12 Co-Sell Overlap Detection
**What:** Detect account overlaps with B2B partners
**Why:** Enable co-sell workflows beyond JV partnerships
**Status:** 📋 Planned (v2.0 Phase 3)
- [ ] CSV import from Crossbeam/Reveal
- [ ] Overlap inbox with filtering
- [ ] Domain-based matching

#### 4.13 Intro Request Workflow
**What:** Slack-native intro request workflow
**Why:** Execute co-sell intros within existing tools
**Status:** 📋 Planned (v2.0 Phase 4)
- [ ] Slack OAuth integration
- [ ] Block Kit messages with approve/deny buttons
- [ ] Outcome tracking and logging

#### 4.14 CRM Integration
**What:** Salesforce/HubSpot sync
**Why:** Pull CRM accounts for overlap detection
**Status:** 📋 Planned (v2.0 Phase 6)
- [ ] Salesforce Bulk API 2.0
- [ ] HubSpot API with rate limiting
- [ ] Incremental sync with deletion handling

#### 4.15 Internal Data Hygiene (Wedge 1)
**What:** Detect internal CRM disconnects
**Why:** Single-player value before partner features
**Status:** 📋 Planned (v2.0 Phase 7)
- [ ] Compare HubSpot vs Salesforce accounts
- [ ] Identify lifecycle stage conflicts
- [ ] Export disconnects report

---

## 5. User Flows

### v1.0 Flow: Partner Discovery
```
Sign Up → Define ICP (4 steps) → See Match Dashboard → Generate First PVP → Copy & Send
```

### v1.5 Flow: Pipeline Management
```
Find Match → Add to Pipeline (Match stage) → Pitch → Track Response →
Close Deal → Execute Promotion → Track Conversions → Reciprocate
```

### v2.0 Flow: Co-Sell Execution
```
Connect CRM → Upload Partner Overlaps → Review Inbox →
Request Intro (Slack) → Track Outcome → Log Revenue
```

---

## 6. Technical Requirements

### Stack
- **Backend:** Django 5.x
- **Frontend:** HTMX + Alpine.js + Tailwind CSS (HAT stack)
- **Database:** Supabase (PostgreSQL)
- **AI:** OpenRouter (GPT-4o-mini, Claude)
- **Enrichment:** Clay.com webhooks
- **Hosting:** DigitalOcean App Platform

### Integrations
| Integration | Purpose | Priority | Status |
|-------------|---------|----------|--------|
| Supabase | Database + Profiles | P0 | ✅ Built |
| OpenRouter | AI generation | P0 | ✅ Built |
| Clay.com | Profile enrichment | P0 | ✅ Built |
| Stripe | Payments | P1 | 🔄 Planned |
| Slack | Co-sell workflow | P2 | 📋 v2.0 |
| Salesforce | CRM sync | P2 | 📋 v2.0 |
| HubSpot | CRM sync | P2 | 📋 v2.0 |
| Instantly/Smartlead | Email sending | P3 | 📋 Future |

### Performance Requirements
- Page load: < 2 seconds
- PVP generation: < 10 seconds
- Match calculation: < 5 seconds per profile
- Database: Handle 10K+ profiles

### Security Requirements
- [x] HTTPS only
- [x] Secure webhook signatures
- [x] No plaintext passwords
- [x] Rate limiting on AI endpoints
- [ ] GDPR-compliant data handling
- [ ] RLS policies (v2.0)
- [ ] Double-blind hashing (v2.0)

---

## 7. Revenue Model

### Pricing Tiers
| Tier | Price | Includes |
|------|-------|----------|
| **Free** | $0 | Browse directory, see scores (limited) |
| **Playbook** | $297 one-time | 54-step execution system PDF |
| **Starter** | $97/month | 10 matches/month, AI pitches |
| **Pro** | $297/month | Unlimited matches, pipeline management |
| **Team** | $597/month | Multi-user, co-sell features (v2.0) |
| **DFY** | $1,500-4,000/month | Done-for-you JV management |

### Revenue Targets
- Year 1: $100K ARR (100 Pro subscribers)
- Year 2: $500K ARR (scaled tiers + DFY + Team)

---

## 8. Out of Scope

### v1.0/v1.5 - NOT building:
- Mobile app
- Real-time chat between partners
- Payment processing for JV deals
- Contract/agreement generation
- Affiliate tracking/attribution
- Video call integration
- Community/forum features

### v2.0 - Explicitly deferred:
- Real-time 2-way API sync with Crossbeam/Reveal
- Complex entity resolution (probabilistic/fuzzy matching)
- Data escrow architecture
- Payouts/commissions
- Multi-partner cluster graphs

---

## 9. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data quality issues | High | High | Clay enrichment, manual curation |
| Low response rates | Medium | High | Better PVP quality, A/B testing |
| Partner churn | Medium | Medium | Pipeline CRM, ongoing value |
| AI hallucinations | Medium | Medium | Quality scoring, human review |
| Competitor copy | Low | Medium | Execution speed, data moat |
| Multi-tenant complexity | Medium | High | Phased rollout, backward compat |

---

## 10. Success Criteria

### v1.0 Launch (✅ Complete)
- [x] 3,000+ partners in database
- [x] Match scoring working
- [x] PVP generator with 8.0+ quality
- [x] ICP wizard complete
- [x] User authentication
- [x] Basic dashboard

### v1.5 Launch Criteria
- [ ] JVRelationship model implemented
- [ ] Pipeline board view working
- [ ] Activity logging functional
- [ ] Stage progression tracked
- [ ] 54-play library integrated

### v2.0 Launch Criteria
- [ ] Multi-tenant foundation complete
- [ ] CSV overlap import working
- [ ] Slack intro workflow functional
- [ ] At least one CRM integration live
- [ ] Internal hygiene dashboard available

---

## 11. Appendix

### Related Documents

**v1.0 Research:**
- `docs/research/BUILD_PLAN.md` - Technical implementation plan & 6-stage workflow
- `docs/research/GEX_COLD_OUTREACH.md` - Email methodology
- `docs/research/54_PLAY_LIBRARY.md` - Launch content framework
- `docs/research/TRANSFORMATION_FINDER.md` - AI transformation analysis
- `docs/research/BLUEPRINT_FRAMEWORK.md` - GTM AI methodology
- `docs/research/MP3_FRAMEWORK.md` - Marketing playbook

**v2.0 Planning:**
- `v2.0/README.md` - v2.0 package overview
- `v2.0/docs/planning/INTEGRATION_PLAN.md` - Comprehensive 11-phase roadmap
- `v2.0/docs/planning/CURRENT_CODEBASE_ANALYSIS.md` - How v2.0 fits existing code
- `v2.0/docs/planning/QUICK_START.md` - Quick reference guide
- `v2.0/docs/implementation/PHASE_1_FOUNDATION.md` - Phase 1 step-by-step

### Glossary
| Term | Definition |
|------|------------|
| **JV (Joint Venture)** | Partnership where two businesses promote each other |
| **PVP** | Partner Value Proposition - personalized pitch |
| **ICP** | Ideal Customer Profile - who you want to partner with |
| **GEX** | Cold outreach methodology (Eric Nowoslawski) |
| **Harmonic Mean** | Scoring method that penalizes lopsided matches |
| **Clay** | Data enrichment platform (clay.com) |
| **Claygent** | AI agent in Clay for custom enrichment |
| **HAT Stack** | HTMX + Alpine.js + Tailwind CSS |
| **JVRelationship** | Model tracking user's relationship with JV partners (v1.5) |
| **6-Stage Workflow** | Match → Pitch → Close → Execute → Convert → Reciprocate |
| **Co-Sell** | B2B partner collaboration on shared accounts (v2.0) |
| **Overlap** | Account that exists in both your CRM and partner's CRM (v2.0) |
| **Tenant** | Multi-tenant workspace/organization (v2.0) |
| **RLS** | Row-Level Security - PostgreSQL data isolation (v2.0) |

---

## 12. Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Jan 2025 | Initial PRD - partner discovery features |
| 1.5 | Jan 2025 | Added JV Pipeline Management, v2.0 roadmap |
