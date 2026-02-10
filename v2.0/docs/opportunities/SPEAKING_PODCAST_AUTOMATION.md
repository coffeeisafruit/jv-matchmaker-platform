# Speaking & Podcast Booking Automation

**Status:** Opportunity Identified
**Origin:** JVD conversation + internal need (Jan 2025)
**Priority:** Medium-High — solves our own problem AND is sellable

---

## The Opportunity

Finding and applying for speaking engagements and podcast appearances is time-consuming:
- Research podcasts/events that fit your niche
- Evaluate if they're worth pursuing
- Craft personalized pitches
- Submit applications (often 30–45 min each)
- Follow up

**Current pain:** Joe spends significant time on this. It's a bottleneck for thought leadership and lead gen.

**The opportunity:** Build automation that finds, scores, and applies for opportunities automatically.

---

## Why It Fits Our Platform

This is the **same matching + outreach engine** we've built for JV partnerships:

| JV Matchmaker | Speaking/Podcast Automation |
|---------------|----------------------------|
| Database of JV partners | Database of podcasts/events |
| Match scoring (fit) | Score podcast/event relevance |
| PVP generator (pitch) | Booking pitch generator |
| Pipeline tracking | Application tracking |
| GEX follow-ups | Follow-up sequences |

**Key insight:** We're not building new tech. We're applying existing tech to a new data set.

---

## What We'd Need to Build

| Component | Status | Notes |
|-----------|--------|-------|
| Podcast/event database | 📋 Build | Scrape or purchase; enrich with Clay/Exa |
| Speaker profile schema | 🔄 Adapt | Bio, topics, past appearances, target audience |
| Fit scoring algorithm | 🔄 Adapt | Topic match, audience size, format fit |
| Pitch generator | 🔄 Adapt | Booking pitch vs. JV pitch |
| Application automation | 📋 Build | Auto-fill forms, submit via API/browser |
| Pipeline tracking | ✅ Built | Same as JV pipeline (v1.5) |
| Follow-up sequences | 🔄 Adapt | GEX methodology for booking context |

**Estimated lift:** Medium. Scoring and pitch gen are adaptations; application automation is new.

---

## Data Sources for Podcasts/Events

| Source | Type | Notes |
|--------|------|-------|
| ListenNotes | Podcasts | API available, paid |
| Podchaser | Podcasts | Database + contact info |
| Rephonic | Podcasts | Audience analytics |
| Eventbrite | Events | API available |
| Meetup | Events | API available |
| Luma | Events | Growing platform |
| Conference websites | Events | Manual scrape or Clay |
| PodMatch | Podcasts | Existing marketplace (competitor) |
| MatchMaker.fm | Podcasts | Existing marketplace (competitor) |

**Strategy:** Start with one source, prove the model, expand.

---

## Workflow: Automated Booking

```
1. Define speaker profile
   - Topics you speak on
   - Target audience
   - Past appearances
   - Preferred formats (interview, solo, panel)

2. System finds opportunities
   - Scrapes/queries podcast/event databases
   - Scores each for relevance
   - Filters by minimum threshold

3. System generates pitches
   - Personalized to each podcast/event
   - References specific episodes or past speakers
   - Explains value to their audience

4. System submits applications
   - Auto-fills application forms
   - Sends cold outreach emails
   - Logs in pipeline

5. System follows up
   - Automated follow-up sequences
   - Escalates hot leads for human touch

6. Human reviews and schedules
   - Accept/decline bookings
   - Coordinate logistics
```

---

## Revenue Model

### Internal Use
- Saves Joe 10+ hours/month
- Generates speaking/podcast appearances
- Drives lead gen and authority building

### Productized (Future)

| Tier | Price | Features |
|------|-------|----------|
| DIY | $97/mo | Database access + pitch generator |
| Assisted | $297/mo | + automated applications |
| DFY | $1,500–3,000/mo | We run the whole system for you |

**Market comp:** DFY podcast booking agencies charge $1,500–5,000/mo.

---

## Build vs. Buy

| Option | Pros | Cons |
|--------|------|------|
| Build internally | Full control, integrates with platform | Dev time |
| Use PodMatch/MatchMaker | Already built | No automation, manual pitching |
| Hybrid | Use their data, add our automation | Depends on API access |

**Recommendation:** Build. Our differentiation is the automation layer, not the database.

---

## Open Questions

1. Which podcast databases have APIs we can use?
2. What's the application format? (Forms, emails, DMs?)
3. Can we auto-submit, or do we need browser automation (Playwright)?
4. What's the legal/ToS situation for automated submissions?
5. Do we start with podcasts or events? (Podcasts likely easier.)

---

## Next Steps

- [ ] Joe documents his current podcast/speaking research process
- [ ] Identify top 3 data sources for podcasts
- [ ] Build speaker profile schema (adapt ICP model)
- [ ] Test pitch generator with podcast context
- [ ] Build v1 pipeline for manual tracking
- [ ] Explore application automation (forms vs. email)

---

## Related Documents

- `v2.0/docs/planning/EXPANSION_IDEAS.md` — Speaker/podcast booking overview
- `docs/PRD.md` — Mentions podcast hosts and event organizers as secondary users
- `docs/research/GEX_COLD_OUTREACH.md` — Email methodology for follow-ups
