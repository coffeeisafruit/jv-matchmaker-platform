# Quick Start Guide - When Ready to Build

## Overview

This integration adds co-sell execution features alongside your existing JV matching platform. The approach is **non-breaking** - all existing features continue to work.

## What You're Building

**Three feature sets in one platform:**
1. **JV Partner Matching** (existing) - Find and match with JV partners from Supabase directory
2. **JV Relationship Management** (v1.5 - added) - Track JV partner relationships through 6-stage workflow
3. **Co-Sell Execution** (v2.0 - new) - Execute co-sell workflows with B2B partners

---

## Pre-Existing Work (v1.5)

Before starting v2.0, the `matching/` app was extended with JV relationship tracking:

### Models Added to `matching/models.py`:
- **JVRelationship** - Tracks user's relationship with JV partners through 6-stage workflow (Match → Pitch → Close → Execute → Convert → Reciprocate)
- **JVActivity** - Activity timeline for each relationship

### Why This Matters for v2.0:
- `JVRelationship` and `JVActivity` need `tenant_id` added in Phase 2
- These are **separate** from Co-Sell `partners/` app - no naming conflict
- JV = promotional partnerships, Co-Sell = B2B account overlaps

### Distinction Between "Partners":
| Feature | App | Model | Purpose |
|---------|-----|-------|---------|
| JV Partner Tracking | `matching/` | `JVRelationship` | Your relationship with JV partners (6-stage workflow) |
| Co-Sell Partners | `partners/` | `Partner` | External orgs (AWS, etc.) for overlap detection |

---

## Before You Start

1. ✅ Review [`/docs/PRD.md`](../../../docs/PRD.md) - Product Requirements Document (v1.0 → v1.5 → v2.0)
2. ✅ Review [`INTEGRATION_PLAN.md`](./INTEGRATION_PLAN.md) - Full v2.0 roadmap
3. ✅ Review [`CURRENT_CODEBASE_ANALYSIS.md`](./CURRENT_CODEBASE_ANALYSIS.md) - How it fits
4. ✅ Review [`RESEARCH_SUMMARY.md`](../research/RESEARCH_SUMMARY.md) - All research documents
5. ✅ Backup your database
6. ✅ Create a feature branch: `git checkout -b feature/cosell-integration`

## Implementation Order

### Phase 1: Foundation (Week 1-2)
- Add Tenant model
- Add TenantMiddleware
- Create default tenants for existing users
- **See**: `/docs/implementation/PHASE_1_FOUNDATION.md`

### Phase 2: Data Model Migration (Week 2-3)
- Add tenant FK to existing models (nullable)
- Populate tenant from user memberships
- **See**: Integration plan for details

### Phase 3: New Co-Sell Apps (Week 3-5)
- Create `overlaps/` app
- Create `intros/` app
- Create `partners/` app
- Create `audit/` app
- **See**: ChatGPT research document for model definitions

### Phase 4-11: Continue with Roadmap
- Follow phases in `/docs/planning/INTEGRATION_PLAN.md`

## Key Principles

1. **Non-Breaking**: Existing code continues to work
2. **Phased**: Build incrementally, test at each phase
3. **Backward Compatible**: Old data accessible, new data separate
4. **Gradual Migration**: Move to tenant-based queries over time

## Testing Strategy

After each phase:
1. Verify existing features still work
2. Test new features in isolation
3. Test integration points
4. Run full test suite

## Rollback Plan

If you need to rollback:
- Phase 1: Remove middleware, tenants remain (harmless)
- Phase 2: Make tenant FK nullable again
- Phase 3+: New apps can be disabled via INSTALLED_APPS

## Questions?

- Review `/docs/planning/INTEGRATION_PLAN.md` for detailed answers
- Review `/docs/planning/CURRENT_CODEBASE_ANALYSIS.md` for code-level details

## Ready?

Start with Phase 1: `/docs/implementation/PHASE_1_FOUNDATION.md`
