# Future Opportunities

This folder contains one-pagers for expansion opportunities beyond the core JV Matchmaker platform. Each document captures the opportunity, requirements, revenue model, and next steps.

---

## The Core Insight

JV Matchmaker is a **matching + outreach engine**. The same technology can serve multiple verticals:

1. Ingest a database of entities
2. Score them for fit
3. Generate personalized outreach
4. Track the pipeline

**Different data sets, same motion.**

---

## Opportunity Index

| Document | Opportunity | Status | Priority |
|----------|-------------|--------|----------|
| [COMPETITIVE_ENHANCEMENTS.md](./COMPETITIVE_ENHANCEMENTS.md) | Platform improvements from ListMatch analysis | Strategic Recommendations | High |
| [CHAMBER_OF_COMMERCE_MATCHING.md](./CHAMBER_OF_COMMERCE_MATCHING.md) | B2B matching for chamber members | Meeting scheduled | High |
| [FOIA_LEAD_GENERATION.md](./FOIA_LEAD_GENERATION.md) | New business leads via public records | Spec complete | Medium |
| [JV_MANAGEMENT_AUTOMATION.md](./JV_MANAGEMENT_AUTOMATION.md) | Automate Chelsea's DFY workflow | Requirements gathering | High |
| [SPEAKING_PODCAST_AUTOMATION.md](./SPEAKING_PODCAST_AUTOMATION.md) | Auto-find and apply for appearances | Concept | Medium-High |

---

## How These Relate

```
                    ┌─────────────────────────────────────┐
                    │      JV Matchmaker Core Engine      │
                    │  (Matching + Pitch + Pipeline)      │
                    └─────────────────────────────────────┘
                                     │
       ┌─────────────────────────────┼─────────────────────────────┐
       │                             │                             │
       ▼                             ▼                             ▼
┌──────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  JV Partners     │      │ Chamber Members │      │ Podcasts/Events │
│   (Current)      │      │   (B2B Sale)    │      │  (New Vertical) │
└──────────────────┘      └─────────────────┘      └─────────────────┘
       │
       ├──────────────────────────────────────┐
       │                                      │
       ▼                                      ▼
┌──────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│ JV Management    │      │ FOIA Lead Gen   │      │ Competitive     │
│  Automation      │      │   (Upsell)      │      │ Enhancements    │
│   (DFY Tier)     │      │                 │      │ (ListMatch)     │
└──────────────────┘      └─────────────────┘      └─────────────────┘
```

---

## Prioritization Framework

When evaluating which opportunity to pursue:

| Factor | Question |
|--------|----------|
| **Effort** | How much new tech do we need to build? |
| **Revenue** | What's the pricing and market size? |
| **Strategic fit** | Does this serve our existing audience? |
| **Timing** | Is there a forcing function (meeting, pilot, etc.)? |
| **Learning** | Does this teach us something we can reuse? |

---

## Related Documents

- `v2.0/docs/planning/EXPANSION_IDEAS.md` — Broader list of 14 adjacent use cases
- `docs/PRD.md` — Core product requirements
