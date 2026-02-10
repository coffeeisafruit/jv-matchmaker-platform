# JV Management Automation (DFY Backend)

**Status:** Requirements Gathering
**Origin:** Chelsea's manual workflow (Jan 2025)
**Priority:** High — enables scale for $1,500–4,000/mo DFY tier

---

## The Opportunity

Chelsea is currently doing JV management manually:
- Reviewing client offers
- Collecting information via WhatsApp conversations
- Writing letters and gathering facts/figures
- Each conversation takes ~30 minutes

**The problem:** Manual process caps capacity at ~16 clients/day.

**The opportunity:** Automate 80% of the intake and content generation. Scale to 50–100+ clients with the same effort.

---

## Why This Matters

From the PRD, the DFY tier is priced at **$1,500–4,000/month**. The margin on this tier depends entirely on operational efficiency.

| Scenario | Clients/Day | Revenue Potential |
|----------|-------------|-------------------|
| Manual (current) | 16 max | Capped by Chelsea's time |
| Semi-automated | 50+ | 3x capacity, same labor |
| Fully automated | 100+ | 6x capacity, minimal oversight |

**Build it for ourselves first, then sell it as a product.**

---

## Current Manual Workflow (To Be Automated)

```
1. Client reaches out (WhatsApp)
2. Chelsea asks questions to understand their offer
3. Client provides details (audience, product, pricing, etc.)
4. Chelsea reviews and synthesizes
5. Chelsea writes outreach letters / materials
6. Back-and-forth until finalized
```

**Pain points:**
- WhatsApp is not automatable
- Questions are repeated for every client
- Synthesis and writing is manual

---

## Proposed Automated Workflow

```
1. Client receives intake form link (Typeform, custom form, etc.)
2. Form collects structured data:
   - Offer details (product, price, audience)
   - Transformation (before → after)
   - Past partnerships
   - Goals for JV outreach
3. AI reviews submission and flags gaps
4. AI generates draft materials:
   - Partner Value Proposition (PVP)
   - Outreach sequences
   - One-sheet / media kit
5. Chelsea reviews and approves (human QC)
6. Client receives deliverables
```

**Time savings:** 30 min → 5 min per client (review only)

---

## What We'd Need to Build

| Component | Status | Notes |
|-----------|--------|-------|
| Intake form | 📋 Build | Structured questions, not WhatsApp |
| Data storage | ✅ Built | Supabase profiles |
| AI review/gap detection | 📋 Build | Flag incomplete submissions |
| PVP generation | ✅ Built | Existing PVP generator |
| Outreach sequence generation | 🔄 Adapt | Extend GEX email logic |
| One-sheet generator | 📋 Build | PDF/doc output |
| Client dashboard | 📋 Build | View their materials, status |
| Admin dashboard | 📋 Build | Chelsea reviews queue |

**Estimated lift:** Medium. Core AI is built; need intake and delivery layers.

---

## Revenue Implications

### Internal Use
- Increases margin on DFY tier
- Enables Chelsea to handle more clients
- Reduces burnout / operational bottleneck

### Productized (Future)
- Sell the system to other JV managers / agencies
- "JV Management in a Box" — white-label to consultants
- SaaS pricing: $297–597/mo for access to the automation

---

## Open Questions (For Chelsea)

1. What are the exact questions you ask every client?
2. What are the most common gaps / missing info?
3. What does the final deliverable look like? (Email copy, one-sheet, pitch deck?)
4. Where do clients get stuck in the current process?
5. What's the QC checklist before sending materials?

---

## Next Steps

- [ ] Chelsea documents her current question flow
- [ ] Map questions to structured intake form fields
- [ ] Build v1 intake form (Typeform or custom)
- [ ] Connect form submissions to Supabase
- [ ] Extend PVP generator to accept form data
- [ ] Build simple admin review queue
- [ ] Pilot with 5 clients, gather feedback

---

## Related Documents

- `docs/PRD.md` — DFY tier pricing, PVP generator
- `docs/research/GEX_COLD_OUTREACH.md` — Email methodology
- `v2.0/docs/planning/EXPANSION_IDEAS.md` — Broader automation opportunities
