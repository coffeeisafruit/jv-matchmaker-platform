# Co-Sell Platform Research Integration: Comprehensive Evaluation & Plan

> **Status**: Planning complete. Ready for implementation.
> **Last Updated**: January 2025

## Executive Summary

**Current State**: JV Matchmaker Platform (single-user, JV partner matching, enrichment, outreach, Supabase integration)

**Research Document 1 (Gemini "Sidecar")**: Co-Sell "Sidecar" Execution Engine - Context file for AI assistants, focuses on RLS, multi-tenancy, Bulk API, workflow automation

**Research Document 2 (Gemini "Project Bible")**: Detailed MVP scope, data models, Slack flow sequence, 90-day build plan, and Cursor rules

**Research Document 3 (Original "Execution Gap")**: Comprehensive strategic blueprint for Challenger Co-Sell Platform with detailed market analysis, GTM strategy, entity resolution algorithms, Chrome extension, and security architecture

**Research Document 4 (ChatGPT "Co-Sell Execution OS")**: Complete Django skeleton with models, app structure, CSV-first approach, simpler tenant model, and ready-to-run code

**Goal**: Integrate all research visions into a unified platform that supports:

1. **JV Partner Matching** (existing feature set)
2. **Co-Sell Execution** (new feature set from research)
3. **Internal Data Hygiene** (Wedge Strategy entry point)

## Research Document Comparison

### Document 1 (Gemini "Sidecar") - Strengths

- ✅ Concise context file format (PROJECT_CONTEXT.md)
- ✅ Clear RLS implementation rules
- ✅ Specific performance constraints (composite indexes)
- ✅ Phase-based roadmap (Day 0, Day 30, Day 60)
- ✅ Explicit "What We Are NOT Building" constraints

### Document 2 (Original "Execution Gap") - Strengths

- ✅ Comprehensive market analysis and competitive positioning
- ✅ Detailed entity resolution algorithms (trigrams, blocking strategy)
- ✅ Materialized views and performance optimization
- ✅ Chrome extension strategy (viral distribution)
- ✅ Security architecture (double-blind hashing)
- ✅ GTM strategy (pricing, distribution, attribution)
- ✅ Service layer architecture patterns

### Document 3 (Gemini "Project Bible") - Additional Details

- ✅ **MVP Scope Clarification**: Explicit in-scope vs out-of-scope items
- ✅ **Detailed Data Model**: Specific field names and types (UUID, Enums, etc.)
- ✅ **Slack Flow Sequence**: Step-by-step "Killer Feature" workflow
- ✅ **90-Day Build Plan**: Day 0-30, 31-60, 61-90 phases
- ✅ **Cursor Rules**: `.cursorrules` file with coding constraints

### Document 4 (ChatGPT "Co-Sell Execution OS") - Strengths

- ✅ **Complete Django Skeleton**: Ready-to-run code with all models defined
- ✅ **App Structure**: Clean separation (overlaps, intros, partners, audit)
- ✅ **CSV-First Approach**: Idempotent CSV import with normalization
- ✅ **Simpler Tenant Model**: tenant_id approach (no RLS initially, easier to start)
- ✅ **Complete Model Definitions**: All fields, indexes, and relationships specified
- ✅ **Slack Integration**: Installation, delivery tracking, interactive actions
- ✅ **Audit Log**: Immutable event log for compliance
- ✅ **Outcome Tracking**: Detailed outcome types and ROI fields
- ✅ **Documentation Structure**: `/docs/` folder with context briefs

### Combined Value

All four documents complement each other:

- **Gemini Context**: Operational constraints and implementation guardrails
- **Gemini Bible**: MVP scope, data models, Slack flow, build timeline
- **Original**: Strategic vision, detailed algorithms, GTM strategy
- **ChatGPT**: Complete implementation skeleton, app structure, ready-to-run code
- **Together**: Complete blueprint from strategy → implementation → execution → code

## Alignment Analysis

### ✅ Strong Alignment (Adopt As-Is)

1. **Tech Stack**: All three (Current + Both Research) use Django + HTMX + Alpine.js + TailwindCSS (HAT Stack)

   - Current project already uses this stack
   - No changes needed

2. **Database**: All target PostgreSQL

   - Current: Supabase (PostgreSQL) with SQLite fallback
   - Both Research: PostgreSQL 16+ with RLS
   - **Action**: Standardize on PostgreSQL, remove SQLite fallback

3. **Server-Side Rendering**: All explicitly avoid React/SPA

   - Current: Already using HTMX for interactivity
   - Original research provides detailed HTMX patterns
   - No changes needed

4. **Architecture Patterns**: Service Layer + Selectors

   - Original research emphasizes Clean Architecture
   - Current project has `services.py` files but could be more structured
   - **Action**: Refactor to explicit `services/` and `selectors/` directories

### 🔄 Needs Adaptation (Merge Concepts)

1. **Multi-Tenancy Architecture** (Key Decision Point)

   - **Current**: Single-user model (`User` with `business_name`, `tier`)
   - **Gemini**: Organization-based multi-tenancy with RLS (mandatory)
   - **ChatGPT**: Simpler tenant_id approach (no RLS initially, easier to start)
   - **Integration Strategy - Phased Approach**:
     - **Phase 1 (MVP)**: Use ChatGPT's simpler `Tenant` model with `tenant_id` FK (faster to ship)
     - **Phase 2 (Scale)**: Add RLS policies and composite indexes (Gemini requirement)
     - **Migration Path**: Start with tenant_id, add RLS later without breaking changes
     - Add `Tenant` model to `core` app (ChatGPT structure)
     - Migrate existing `User.business_name` → `Tenant.name`
     - Add `tenant` FK to all data models
     - Implement `TenantMiddleware` (ChatGPT approach) first, then RLS middleware (Gemini) later

2. **Data Models - App Structure Decision**

   - **Current**: `Profile` (JV partners), `Match` (JV matches) in `matching` app
   - **ChatGPT**: Separate apps: `overlaps`, `intros`, `partners`, `audit`
   - **Gemini**: `Account`, `Overlap`, `Action` models
   - **Integration Strategy - Best of Both**:
     - **Adopt ChatGPT's App Structure**: Clean separation of concerns
       - `overlaps/` app: `Overlap`, `OverlapImportRun` models (ChatGPT)
       - `intros/` app: `IntroRequest`, `Outcome` models (ChatGPT)
       - `partners/` app: `Partner`, `PartnerConnection` models (ChatGPT)
       - `audit/` app: `AuditEvent` model (ChatGPT)
     - **Keep Existing**: `matching/` app for JV partner features (`Profile`, `Match`)
     - **Add Later**: `data/` app for CRM accounts (`Account` model from Gemini) when adding CRM sync
     - **Link Models**: `Overlap` can reference both `Partner` (ChatGPT) and `Account` (Gemini future)

3. **Matching Algorithms - Two Different Systems**

   - **Current**: AI-powered scoring (Intent, Synergy, Momentum, Context) for JV partners
   - **Original Research**: Deterministic (exact domain) + Probabilistic (trigrams) for CRM accounts
   - **Integration Strategy**: 
     - `JVMatchService`: AI scoring for JV partner discovery (keep existing)
     - `EntityResolutionService`: Domain + trigram matching for CRM accounts (new)
     - `InternalMatchService`: Finds internal disconnects (HubSpot vs Salesforce) - Wedge 1

### ⚠️ Missing Infrastructure (Add New)

1. **Docker Setup**

   - **Current**: No Docker configuration
   - **Both Research**: Require Docker + docker-compose.yml
   - **Action**: Create Docker setup for development/production

2. **Async Task Queue**

   - **Current**: No Celery/Redis
   - **Both Research**: Require Celery + Redis for workflows
   - **Action**: Add Celery + Redis for:
     - CRM data ingestion (Bulk API jobs)
     - Entity resolution (trigram matching)
     - Materialized view refresh
     - Slack notifications
     - Workflow automation

3. **RLS (Row-Level Security)**

   - **Current**: No RLS implementation
   - **Both Research**: Mandatory RLS with composite indexes
   - **Action**: 
     - Create `RLSMiddleware` to set `app.current_tenant`
     - Add PostgreSQL RLS policies on all tenant-scoped tables
     - Update all indexes to composite: `(tenant_id, field_name)`
     - Original research emphasizes: "Defense in Depth" - even if developer forgets filter, DB blocks access

4. **CRM Integrations**

   - **Current**: No Salesforce/HubSpot integration
   - **Both Research**: OAuth flow + Bulk API sync
   - **Action**: Create `integrations` app for:
     - OAuth tokens (encrypted storage)
     - Salesforce Bulk API 2.0 integration
     - HubSpot API with burst limit handling (100/10sec)
     - Incremental sync with deletion handling

5. **Entity Resolution Engine** (Original Research Detail)

   - **Current**: No entity resolution
   - **Original Research**: Detailed algorithm with trigrams + blocking
   - **Action**: Create `data/services/entity_resolution.py`:
     - Stage 1: Exact domain matching (deterministic, O(1))
     - Stage 2: Fuzzy name matching with pg_trgm (probabilistic)
     - Blocking strategy to reduce O(N²) to manageable chunks
     - GIN indexes on normalized_name

6. **Materialized Views** (Original Research)

   - **Current**: No materialized views
   - **Original Research**: Incremental view maintenance for performance
   - **Action**: Create materialized views for:
     - Overlap dashboard queries
     - Partner account counts
     - Refresh triggers on account updates

7. **Chrome Extension** (Original Research - Wedge 3)

   - **Current**: No browser extension
   - **Original Research**: Viral distribution mechanism
   - **Action**: Create Chrome extension:
     - Manifest V3 compliant
     - Overlay on Salesforce/HubSpot account pages
     - Shows partner overlap count
     - Drives platform signups

8. **Security: Double-Blind Hashing** (Original Research)

   - **Current**: No blind hashing
   - **Original Research**: SOC 2 Type II requirement
   - **Action**: Implement:
     - Salted SHA-256 hashing for emails/domains
     - Separate index for hashed values
     - Match on hashes, reveal metadata only on match

### 🚫 Conflicts to Resolve

1. **Product Vision - Three Use Cases**

   - **Current**: JV partner matching (ecosystem of partners)
   - **Gemini**: Co-sell execution (internal CRM hygiene + partner overlap)
   - **Original**: Challenger platform to Crossbeam/Reveal
   - **Resolution**: Support all three:
     - **JV Matching**: Existing `Profile`/`Match` models (keep)
     - **Co-Sell Sidecar**: New `Account`/`Overlap` models (add)
     - **Internal Hygiene**: Same `Account` model, different matching logic (Wedge 1)

2. **Matching Logic - Two Systems**

   - **Current**: AI-powered scoring for JV partners
   - **Research**: Exact domain + trigram matching for CRM accounts
   - **Resolution**: 
     - Keep AI matching for JV partners (`JVMatchService`)
     - Use deterministic + probabilistic matching for CRM (`EntityResolutionService`)
     - Different services, different use cases

3. **Data Sources - Multiple Sources**

   - **Current**: Supabase profiles (read-only), Clay enrichment
   - **Research**: Salesforce/HubSpot CSVs + APIs
   - **Resolution**: Support all:
     - Supabase profiles remain for JV directory (read-only)
     - Add Salesforce/HubSpot as CRM data sources
     - CSV import for "Overlap CSVs" from Crossbeam/Reveal (Phase 2)
     - Clay enrichment continues for JV profiles

## Gemini Project Bible: Key Specifications

### MVP Scope (From Gemini Bible)

**In Scope (MVP)**:

- ✅ Ingestion: Salesforce Bulk API 2.0, HubSpot API, CSV Import (Universal Mapper)
- ✅ Matching: Exact Domain Matching (Normalized) - **Note**: No fuzzy matching in MVP
- ✅ Identity: Internal "Golden Record" (merging SFDC + HubSpot)
- ✅ Action: Slack Notifications (Block Kit), Email Drafts
- ✅ Writeback: Update SFDC Opportunity (Partner_Status field)

**Out of Scope (Non-Goals)**:

- ❌ Real-time 2-way API Sync with Crossbeam/Reveal (Phase 2)
- ❌ Complex Entity Resolution: No probabilistic/fuzzy matching yet (defer to Phase 3)
- ❌ Data Escrow: Rely on user's rights to uploaded data
- ❌ Payouts/Commissions: No partner payments
- ❌ Multi-Partner Graphs: No "Cluster" mapping

**Decision**: Start with exact domain matching only. Add trigram fuzzy matching later (Original Research) after MVP proves value.

### Detailed Data Model (From Gemini Bible)

**Organization (Tenant)**:

- `id` (UUID primary key)
- `name` (Text)
- `domain` (Text)
- `plan` (Enum: Starter/Growth/Pro/Enterprise)

**Integration**:

- `provider` (Enum: SFDC, HUBSPOT, SLACK)
- `credentials` (Encrypted JSON)
- `status` (Enum: Active/Error)
- `organization` (FK → Organization)

**Account (RLS Protected)**:

- `tenant_id` (FK → Organization)
- `domain_hash` (Indexed for fast lookups)
- `clean_domain` (Text, normalized)
- `name` (Text)
- `owner_email` (Email)
- `lifecycle_stage` (Enum: Prospect, Customer, Churned, etc.)
- `source_system` (Enum: SFDC, HubSpot, CSV)

**Overlap (The Core Unit)**:

- `tenant_id` (FK → Organization)
- `internal_account` (FK → Account)
- `partner_account_name` (Text - from CSV)
- `partner_name` (Text - e.g., "AWS")
- `overlap_status` (Enum: Open, Ignored, Actioned)

**Action (The Workflow)**:

- `tenant_id` (FK → Organization)
- `overlap` (FK → Overlap)
- `type` (Enum: SLACK_ALERT, INTRO_REQUEST)
- `state` (Enum: PENDING, SENT, ACCEPTED, REJECTED)
- `payload` (JSON - message content, etc.)

### Slack Flow Sequence (Gemini "Killer Feature")

**Trigger**: `overlap.detected` (High Value: My "Open Opp" maps to Partner "Customer")

**System Flow**:

1. Check `AutomationRules` for this overlap type
2. Format Slack Block Kit payload
3. Send Slack DM to Account Owner:

   - Text: "🎯 New Partner Intel: AWS has Acme Corp as a Customer."
   - Button 1: "Request Intro" (`action_id: request_intro`)
   - Button 2: "Ignore"

**User Interaction**:

1. User clicks "Request Intro"
2. Sidecar Modal opens in Slack (using `views.open`)
3. User types: "Hey [Partner], can you help me break into Acme?"

**Execution**:

1. System sends email to Partner Manager
2. System updates Salesforce Opportunity → `Partner_Ask_Date = Today`
3. Action state changes: `PENDING` → `SENT`

### Key Events (Django Signals)

- `ingestion.complete`: Triggered when CSV or API sync finishes
- `overlap.detected`: Triggered when Account matches Partner Record
- `action.requested`: Triggered when user clicks "Request Intro"
- `action.outcome`: Triggered when partner clicks "Accept" in email/portal

### 90-Day Build Plan Alignment (Gemini)

**Phase 1: The Foundation (Days 0-30)**

- Setup Django + Docker + Postgres
- Implement RLS Middleware (Critical security step)
- Build Salesforce Ingest Service (Bulk API 2.0)
- Build Internal Matcher (HubSpot vs. SFDC)
- **Deliverable**: "Hygiene Dashboard" (Show internal data disconnects)

**Phase 2: The Sidecar (Days 31-60)**

- Build Universal CSV Importer (Pandas)
- Build Slack OAuth & Block Kit Service
- Implement "Request Intro" Workflow
- **Deliverable**: User uploads Crossbeam CSV → Gets Slack Alerts

**Phase 3: The Closed Loop (Days 61-90)**

- Build CRM Writeback (Sync status back to SFDC)
- Build Chrome Extension (Lightweight domain lookup)
- **Deliverable**: Full attribution loop proved

## Synthesis: Unified Approach

### Decision: Start Simple, Scale Smart

**MVP Approach (ChatGPT + Gemini MVP Scope)**:

- Use ChatGPT's simpler tenant_id model (no RLS initially)
- Adopt ChatGPT's app structure (overlaps, intros, partners, audit)
- Use ChatGPT's complete model definitions as starting point
- Focus on CSV import + Slack workflow (proven value first)

**Scale Approach (Gemini + Original)**:

- Add RLS policies after MVP proves value
- Add CRM sync (Salesforce Bulk API, HubSpot) in Phase 2
- Add entity resolution (trigrams) in Phase 3
- Add materialized views for performance

### App Structure (Final Decision)

```
jv-matchmaker-platform/
├── core/                    # Tenant, User, base models
├── matching/                # EXISTING: JV partner matching (Profile, Match)
├── positioning/             # EXISTING: ICP, Transformation
├── outreach/               # EXISTING: PVP, Campaigns
├── overlaps/               # NEW (ChatGPT): Overlap, OverlapImportRun
├── intros/                 # NEW (ChatGPT): IntroRequest, Outcome
├── partners/               # NEW (ChatGPT): Partner, PartnerConnection
├── audit/                  # NEW (ChatGPT): AuditEvent
├── integrations/           # NEW: Slack, Salesforce, HubSpot
└── data/                   # FUTURE: Account model (CRM sync)
```

## Integration Roadmap

### Phase 1: Foundation & Multi-Tenancy (Days 0-30 / Week 1-2)

**Goal**: Add multi-tenancy infrastructure without breaking existing features

1. **Create `Organization` Model**

   - File: `core/models.py`
   - Fields: `name`, `domain`, `plan` (Starter/Growth/Pro/Enterprise)
   - Migration: Add `organization` FK to `User` (nullable initially, default org for existing users)

2. **Add Tenant Middleware** (Start Simple, Add RLS Later)

   - File: `core/middleware.py` (new)
   - Use ChatGPT's `TenantMiddleware` approach first (simpler, faster)
   - Sets `request.tenant` from session or single-tenant fallback
   - **Phase 2**: Add RLS middleware that sets `SET app.current_tenant = '...'` (Gemini requirement)
   - Handles subdomain-based tenant resolution (future)

3. **Docker Setup**

   - Files: `docker-compose.yml`, `Dockerfile`
   - Services: Django, PostgreSQL 16+, Redis, PgBouncer (optional for now)
   - Environment: `.env.example` with all required variables

4. **Celery + Redis**

   - File: `config/celery.py` (new)
   - Update `settings.py` for Celery config
   - Add `celery[redis] `and `redis` to `requirements.txt`
   - Basic task structure for future workflows

5. **Service Layer Refactoring**

   - Create `services/` and `selectors/` directories in each app
   - Move business logic from views to services
   - Create selector functions for read-heavy queries (return dicts, not models)

6. **Create Documentation Structure** (ChatGPT + Gemini)

   - Create `/docs/` folder structure (ChatGPT approach)
   - File: `/docs/00_CONTEXT_BRIEF.md` (ChatGPT format)
   - File: `/docs/01_VALUE_PROP_AND_FEATURES.md` (ChatGPT)
   - File: `/docs/07_CURSOR_PROJECT_PROMPT.md` (ChatGPT - pin this)
   - File: `PROJECT_CONTEXT.md` (Gemini format - root level)
   - File: `.cursorrules` (Gemini Project Bible - root level)
   - Copy both ChatGPT and Gemini prompts (complementary)

### Phase 2: Data Model Migration & RLS (Week 2-3)

**Goal**: Add tenant_id to all models, implement RLS policies

1. **Add `tenant_id` to All Models**

   - Update: `matching/models.py` (Profile, Match)
   - Update: `positioning/models.py` (ICP, TransformationAnalysis)
   - Update: `outreach/models.py` (PVP, Campaign, Template)
   - Create data migration: Assign existing records to default organizations

2. **PostgreSQL RLS Policies**

   - Create migration: `core/migrations/XXXX_enable_rls.py`
   - Enable RLS on all tenant-scoped tables
   - Create policies: `tenant_id = current_setting('app.current_tenant')::uuid`
   - Test with multiple tenants to verify isolation

3. **Update Indexes to Composite**

   - Modify all indexes: `(tenant_id, field_name)`
   - Examples:
     - `idx_tenant_domain` on `accounts(tenant_id, domain)`
     - `idx_tenant_user` on `matches(tenant_id, user_id)`
   - Create migration for index changes

4. **Enable pg_trgm Extension**

   - Migration: Enable `pg_trgm` extension for fuzzy matching
   - Prepare for entity resolution in Phase 3

### Phase 3: Overlaps & CSV Import (Week 3-4) - ChatGPT MVP

**Goal**: Build CSV-first overlap ingestion (proven value before CRM sync)

1. **Create `overlaps` App** (ChatGPT Structure)

   - File: `overlaps/models.py`
   - Model: `OverlapImportRun` (tracks CSV import jobs)
   - Model: `Overlap` (overlapped accounts)
   - Use ChatGPT's complete model definitions
   - Domain normalization utility (`normalize_domain`)

2. **CSV Import Service** (ChatGPT + Gemini)

   - File: `overlaps/services/csv_import.py` (new)
   - Use Pandas for parsing (Gemini requirement)
   - Normalize column headers to snake_case (Gemini)
   - Validate domains with regex (Gemini)
   - Idempotency: `source_run_id + partner_name + account_domain` (ChatGPT)
   - Update `last_seen_at` on duplicates, keep `created_at` (ChatGPT)
   - Trigger `overlap.imported` event (ChatGPT)

3. **Overlaps Inbox UI** (HTMX)

   - File: `templates/overlaps/inbox.html` (new)
   - List view with HTMX filtering (status, partner, segment)
   - Detail drawer (HTMX swap)
   - Upload CSV form (HTMX submit)

### Phase 4: Intro Requests & Slack Integration (Week 4-5) - ChatGPT MVP

**Goal**: Build Slack-native intro request workflow

1. **Create `intros` App** (ChatGPT Structure)

   - File: `intros/models.py`
   - Model: `IntroRequest` (workflow object)
   - Model: `Outcome` (logged results)
   - Use ChatGPT's complete model definitions
   - Status transitions: draft → sent → approved/denied → completed

2. **Create `partners` App** (ChatGPT Structure)

   - File: `partners/models.py`
   - Model: `Partner` (external org)
   - Model: `PartnerConnection` (tenant ↔ partner relationship)
   - Use ChatGPT's complete model definitions

3. **Slack Integration** (ChatGPT + Gemini)

   - File: `integrations/slack/models.py` (ChatGPT structure)
   - Model: `SlackInstallation` (OAuth tokens)
   - Model: `SlackMessageDelivery` (delivery tracking)
   - OAuth flow: Install app per tenant
   - Block Kit messages: Intro request with Approve/Deny buttons
   - Interactive actions: Handle button clicks
   - Signature verification (Gemini security requirement)
   - Idempotency on interactive payloads (ChatGPT)

4. **Slack Flow Implementation** (Gemini "Killer Feature")

   - Trigger: `overlap.detected` or user clicks "Request Intro"
   - Check `AutomationRules` (future) or hard-coded handler
   - Format Block Kit payload
   - Send DM to partner contact
   - Handle Approve/Deny button clicks
   - Update `IntroRequest.status`
   - Notify requester on approval
   - Create task "schedule meeting" (future)

### Phase 5: Outcomes & Audit Log (Week 5-6) - ChatGPT MVP

**Goal**: Track outcomes and maintain audit trail

1. **Create `audit` App** (ChatGPT Structure)

   - File: `audit/models.py`
   - Model: `AuditEvent` (immutable event log)
   - Append-only design (no updates after creation)
   - Event types: `overlap.imported`, `intro_request.approved`, etc.

2. **Outcome Logging UI**

   - File: `templates/intros/outcome_form.html` (new)
   - HTMX form for logging outcomes
   - Outcome types: meeting_booked, opp_created, closed_won, etc.
   - Optional ROI fields (estimated_pipeline, estimated_revenue)

3. **Basic Dashboard** (ChatGPT MVP)

   - File: `templates/core/dashboard.html` (update)
   - KPIs: Volume, acceptance rate, response SLA, cycle time
   - HTMX polling for real-time updates

### Phase 6: CRM Integration & Entity Resolution (Week 6-8) - Gemini Phase 2

**Goal**: Add Salesforce/HubSpot sync and entity resolution engine

1. **Create `integrations` App**

   - File: `integrations/models.py`
   - Model: `Integration` (organization, provider, encrypted tokens, instance_url)
   - OAuth flow views for Salesforce/HubSpot
   - Token refresh logic

2. **Salesforce Bulk API 2.0 Integration**

   - File: `integrations/services/salesforce_bulk.py` (new)
   - Use Bulk API 2.0 for initial syncs (avoids 15k/day REST limit)
   - Celery task: `sync_salesforce_accounts(integration_id)`
   - Handle job status polling, CSV download, streaming to DB

3. **HubSpot API Integration**

   - File: `integrations/services/hubspot_sync.py` (new)
   - Use `crm.companies.get_all` with pagination
   - Respect burst limits (100 requests/10 seconds)
   - Celery task: `sync_hubspot_companies(integration_id)`

4. **Incremental Sync & Deletion Handling**

   - Delta sync using `SystemModstamp` (Salesforce) / `hs_lastmodifieddate` (HubSpot)
   - Handle hard deletes: `getDeleted()` endpoint (Salesforce) or ID comparison (HubSpot)
   - Prevent "Ghost Overlaps"

5. **Entity Resolution Service** (MVP: Exact Domain Only)

   - File: `data/services/entity_resolution.py` (new)
   - **MVP (Phase 3)**: Exact domain matching only
     - Normalize domains (strip www., http://, lowercase)
     - O(1) lookup using `domain_hash` index
   - **Future (Phase 4)**: Add fuzzy name matching with `pg_trgm`
     - Stage 2: Fuzzy name matching (defer to after MVP)
     - Create GIN index on `normalized_name`
     - Similarity threshold: 0.8
     - Blocking strategy: Group by first 2 letters + state
   - Celery task: `calculate_overlaps(tenant_a_id, tenant_b_id)`

6. **Create `data` App with Account/Overlap Models** (Gemini Spec)

   - File: `data/models.py` (new app)
   - Model: `Account`:
     - `tenant_id` (FK → Organization)
     - `domain_hash` (Indexed for fast lookups)
     - `clean_domain` (Text, normalized)
     - `name`, `owner_email`, `lifecycle_stage`
     - `source_system` (Enum: SFDC, HubSpot, CSV)
   - Model: `Overlap`:
     - `tenant_id` (FK → Organization)
     - `internal_account` (FK → Account)
     - `partner_account_name` (Text - from CSV)
     - `partner_name` (Text - e.g., "AWS")
     - `overlap_status` (Enum: Open, Ignored, Actioned)
   - Indexes: `(tenant_id, domain_hash)`, `(tenant_id, source_system, external_id)`

### Phase 7: Internal Data Hygiene (Wedge 1) (Week 8-9) - Gemini Phase 1

**Goal**: Deliver "Single-Player Value" - internal CRM disconnect detection

1. **Internal Match Service**

   - File: `data/services/internal_match_service.py` (new)
   - Compares HubSpot accounts vs Salesforce accounts
   - Identifies: "Active Prospect in HubSpot" is "Closed Customer in Salesforce"
   - Returns list of disconnects with conflict details

2. **Internal Hygiene Dashboard**

   - File: `templates/data/internal_hygiene.html` (new)
   - Shows internal disconnects in table format
   - HTMX filtering and sorting
   - Export to CSV functionality

3. **CSV Import for Overlap Data** (Phase 2 from Gemini - "Universal Mapper")

   - File: `data/services/csv_ingestion.py` (new)
   - Use Pandas for parsing (Gemini requirement)
   - Normalize column headers to snake_case before processing
   - Validate domain fields using rigorous regex before saving
   - Parse "Overlap CSVs" from Crossbeam/Reveal
   - Create `Account` and `Overlap` records from CSV
   - Celery task: `ingest_overlap_csv(organization_id, csv_file)`
   - Trigger `ingestion.complete` signal when done

### Phase 8: Materialized Views & Performance (Week 9-10) - Original Research

**Goal**: Optimize dashboard queries with materialized views

1. **Create Materialized Views**

   - Migration: Create materialized views for:
     - `overlap_dashboard` (partner_name, account_count, revenue_sum)
     - `account_summary` (tenant_id, source, lifecycle_stage, count)
   - Refresh strategy: Incremental updates via triggers

2. **Incremental View Maintenance**

   - Trigger: On `Account` update, recalculate affected overlaps
   - Only refresh rows that changed, not full view
   - Celery task: `refresh_overlap_view(account_ids)`

3. **Selector Layer Optimization**

   - File: `data/selectors/dashboard.py` (new)
   - Return dictionaries instead of model instances
   - Use `.values()` for large queries
   - Benchmark: 10k dicts vs 10k models

### Phase 9: Automation & Workflows (Week 10-11) - Gemini + Original

**Goal**: Add workflow automation for co-sell execution

1. **Create `automation` App**

   - File: `automation/models.py` (new app)
   - Model: `Action` (tenant_id, trigger, type, status, payload)
   - Types: Slack_DM, SFDC_Task, Email, Webhook

2. **Slack Integration** (Gemini "Killer Feature")

   - OAuth flow in `integrations` app
   - Celery task: `send_slack_dm(overlap_id, message_template)`
   - Use Slack Block Kit for rich messages
   - **Slack Flow Sequence**:

     1. Trigger: `overlap.detected` signal
     2. Check `AutomationRules` for this overlap type
     3. Format Block Kit payload with buttons
     4. Send DM to Account Owner
     5. Handle button interactions (`request_intro`, `ignore`)
     6. Open modal (`views.open`) for intro request
     7. Update Salesforce Opportunity → `Partner_Ask_Date = Today`

3. **Workflow Builder UI**

   - File: `templates/automation/workflow_builder.html` (new)
   - HTMX-based workflow creation
   - Trigger: "New Overlap" → Action: "Slack Alert"
   - Preview and test workflows

4. **Chrome Extension** (Wedge 3 - Future)

   - Directory: `chrome_extension/` (new)
   - Manifest V3 compliant
   - Overlay on Salesforce/HubSpot account pages
   - Shows: "You have 3 partners who can help with this account"
   - API endpoint: `/api/extension/overlap-check?domain=acme.com`

### Phase 10: Security & Compliance (Week 11-12) - Original Research

**Goal**: Implement double-blind hashing for SOC 2 compliance

1. **Double-Blind Hashing Service**

   - File: `data/services/hashing.py` (new)
   - Salted SHA-256 for emails/domains
   - Separate `hashed_identifier` table
   - Match on hashes, reveal metadata only on match

2. **Data Escrow Architecture**

   - Encrypt raw data in tenant RLS partition
   - Store hashed values in separate index
   - Matching engine compares hashes only
   - Retrieve metadata only when match occurs

3. **Chrome Extension Security**

   - Extension sends hash of domain, not raw domain
   - API checks RLS permissions
   - Returns simple status object (no full partner list)
   - Prevents scraping attacks

### Phase 11: UI Integration & Polish (Week 12-13) - All Sources

**Goal**: Integrate co-sell features into existing UI

1. **Dashboard Updates**

   - File: `templates/core/dashboard.html` (update)
   - Add "Co-Sell" section alongside "JV Matches"
   - Tabs: "JV Partners" | "Co-Sell Overlaps" | "Internal Hygiene"
   - HTMX tab switching

2. **Overlap List View**

   - File: `templates/data/overlap_list.html` (new)
   - Table with HTMX filtering (partner, status, revenue)
   - Alpine.js for multi-select filters
   - "Send Intro" button triggers workflow

3. **Real-Time Sync Progress**

   - File: `templates/integrations/sync_progress.html` (new)
   - HTMX polling: `hx-trigger="every 2s"`
   - Progress bar updates from Celery task status
   - Auto-refresh when complete

## File Structure Changes

```
jv-matchmaker-platform/
├── core/                      # EXISTING + Updates
│   ├── models.py              # Add Tenant model (ChatGPT)
│   ├── middleware.py          # NEW: TenantMiddleware (ChatGPT), then RLSMiddleware (Gemini)
│   ├── services/              # NEW: Service layer (Original)
│   └── selectors/             # NEW: Selector layer (Original)
├── matching/                  # EXISTING: JV partner matching
│   ├── models.py              # Profile, Match (keep existing)
│   └── ...
├── positioning/               # EXISTING: ICP, Transformation
├── outreach/                 # EXISTING: PVP, Campaigns
├── overlaps/                 # NEW APP (ChatGPT)
│   ├── models.py              # Overlap, OverlapImportRun
│   ├── services/
│   │   └── csv_import.py       # CSV ingestion (ChatGPT + Gemini)
│   └── admin.py
├── intros/                   # NEW APP (ChatGPT)
│   ├── models.py              # IntroRequest, Outcome
│   └── admin.py
├── partners/                 # NEW APP (ChatGPT)
│   ├── models.py              # Partner, PartnerConnection
│   └── admin.py
├── audit/                    # NEW APP (ChatGPT)
│   ├── models.py              # AuditEvent
│   └── admin.py
├── integrations/             # NEW APP
│   ├── slack/
│   │   ├── models.py          # SlackInstallation, SlackMessageDelivery (ChatGPT)
│   │   └── views.py           # OAuth, interactive actions
│   ├── services/
│   │   ├── salesforce_bulk.py  # FUTURE (Gemini Phase 2)
│   │   └── hubspot_sync.py    # FUTURE (Gemini Phase 2)
│   └── views.py
├── data/                     # FUTURE APP (Gemini Phase 2)
│   ├── models.py              # Account (CRM sync)
│   ├── services/
│   │   ├── entity_resolution.py  # FUTURE (Original Research)
│   │   ├── internal_match_service.py
│   │   └── hashing.py
│   └── selectors/
│       └── dashboard.py
├── automation/               # FUTURE APP (Gemini Phase 3)
│   ├── models.py              # Action, AutomationRule
│   └── tasks.py
├── chrome_extension/         # FUTURE (Original Research)
│   ├── manifest.json
│   └── content.js
├── docs/                     # NEW (ChatGPT structure)
│   ├── 00_CONTEXT_BRIEF.md
│   ├── 01_VALUE_PROP_AND_FEATURES.md
│   ├── 07_CURSOR_PROJECT_PROMPT.md
│   └── ...
├── config/
│   ├── celery.py              # NEW
│   └── settings.py            # Add Celery, tenant config
├── docker-compose.yml         # NEW
├── Dockerfile                 # NEW
├── PROJECT_CONTEXT.md          # NEW: Gemini's context file
└── .cursorrules                # NEW: Cursor system prompt rules
```

## Key Decisions Needed

1. **Migration Strategy**: 

   - **Decision**: Use ChatGPT's `Tenant` + `Membership` model approach
   - Auto-create default `Tenant` for each existing `User` (smoother migration)
   - Create `Membership` record linking User to Tenant
   - New signups: Create Tenant during onboarding (Flow A from ChatGPT)
   - **Future**: Add `Organization` model (Gemini) if needed, but `Tenant` works for MVP

2. **RLS Performance**: 

   - Start with composite indexes (required by both research docs)
   - Add PgBouncer later if connection pooling needed
   - Monitor query performance from day 1

3. **Feature Priority - Which Wedge First?**

   - **Wedge 1 (Internal Hygiene)**: Fastest to implement, immediate value
   - **Wedge 2 (Vertical Compliance)**: Requires industry-specific schemas (MedTech NPI, etc.)
   - **Wedge 3 (Chrome Extension)**: Viral distribution but requires API infrastructure
   - **Recommendation**: Start with Wedge 1, then Wedge 3, defer Wedge 2

4. **Data Model Overlap**:

   - Keep `Account` and `Profile` separate (different sources, different purposes)
   - `Profile`: JV partner directory (Supabase, read-only)
   - `Account`: CRM-synced accounts (Salesforce/HubSpot, writable)
   - Link them via domain matching if needed

5. **Pricing Strategy** (Original Research):

   - Freemium: Internal Hygiene tool free forever (Wedge 1)
   - Entry tier: $199/month with full connector access (undercut Crossbeam)
   - Seat-based: Charge per "Active Co-Seller" (aligns with value)
   - Agency Edition: Multi-tenant admin for consultants

6. **Entity Resolution Algorithm**:

   - **MVP (Gemini)**: Exact domain matching only (normalized domains)
   - **Phase 3 (Original Research)**: Add two-stage approach (exact domain → trigrams)
   - Blocking strategy to reduce O(N²) complexity
   - GIN indexes on normalized_name for performance
   - **Decision**: Start simple (exact match), add fuzzy later

## Risks & Mitigations

1. **Risk**: RLS performance overhead

   - **Mitigation**: Composite indexes from day 1, monitor query performance, add PgBouncer if needed

2. **Risk**: Breaking existing single-user functionality

   - **Mitigation**: Make `organization` nullable initially, migrate gradually, create default orgs for existing users

3. **Risk**: Two different matching systems causing confusion

   - **Mitigation**: Clear UI separation: "JV Matches" (AI-powered) vs "Co-Sell Overlaps" (deterministic)

4. **Risk**: Entity resolution performance on large datasets

   - **Mitigation**: Blocking strategy reduces search space, materialized views cache results, incremental refresh

5. **Risk**: Chrome extension security vulnerabilities

   - **Mitigation**: Manifest V3 compliance, no sensitive logic in client, hash-based API calls

6. **Risk**: Salesforce/HubSpot API rate limits

   - **Mitigation**: Bulk API 2.0 for initial syncs, incremental syncs for updates, respect burst limits

## Success Metrics (Original Research)

1. **Partner Attribution**: Track "Partner Sourced" vs "Partner Influenced" revenue

   - Time decay model: Activity within X days of deal closing
   - Write-back to Salesforce Opportunity object
   - VP of Sales sees "Partner Ecosystem" in revenue reports

2. **Activation**: % of users who complete first overlap detection

   - Internal Hygiene: 80%+ should see value immediately
   - Co-Sell Overlaps: 50%+ should create first workflow

3. **Retention**: Monthly active users who use co-sell features

   - Target: 60%+ of paid users active monthly
   - Chrome extension: 40%+ of users install extension

## Next Steps

1. ✅ Review both research documents (completed)
2. ⏳ Review this comprehensive plan
3. ⏳ Decide on migration strategy (default org vs manual)
4. ⏳ Prioritize which wedge to build first (recommend Wedge 1: Internal Hygiene)
5. ⏳ Create `/docs/` folder structure (ChatGPT format)
6. ⏳ Create PROJECT_CONTEXT.md file (Gemini's format)
7. ⏳ Create .cursorrules file (Gemini's Cursor system prompt)
8. ⏳ Review ChatGPT's Django skeleton and adapt to existing project structure
9. ⏳ Start Phase 1: Foundation (Docker + Tenant model + TenantMiddleware)