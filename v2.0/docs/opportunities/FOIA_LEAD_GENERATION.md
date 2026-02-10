# FOIA Data Pipeline (New Business Licenses)

**Status:** Spec Complete
**Origin:** Partner conversation (Jan 2025)
**Owner:** Joe
**Validation:** Previously done manually — this is proven, not theory

---

## Project Overview

A backend data pipeline to automate the acquisition, parsing, and enrichment of "New Business License" data via **Freedom of Information Act (FOIA)** requests.

**The Goal:** Generate a monthly supply of "fresh local leads" (Entities, DBAs, Licenses) to be sold as an upsell package to Chambers of Commerce.

---

## Context & Business Logic

| Problem | Solution |
|---------|----------|
| Chambers need to prove value to members | We provide proactive recruitment data |
| They lack "proactive" recruitment data | Monthly list of businesses formed in last 30 days |
| Current approach is reactive | We make them proactive with scored, relevant leads |

**The pitch to chambers:** "Every month, we deliver you the 50–100 most relevant NEW businesses in your area that aren't chamber members yet. These are warm leads for membership recruitment."

---

## Revenue Model

### Cost Basis
- FOIA requests are generally free/low cost
- Enrichment API costs per lead (variable)

### Retail Pricing
- **Effective rate:** ~$10 per lead

### Package Options

| Package | Leads/Month | Price |
|---------|-------------|-------|
| Starter | 25 | $250/month |
| Growth | 50 | $500/month |
| Scale | 100 | $1,000/month |

### Bundle Pricing (with Chamber Matching)
- Chamber Matching + 50 leads: $1,250/month (vs. $1,500 standalone)
- Creates stickiness and higher ARPU

---

## User Stories

1. **As a System:** I need to ingest data from various municipal formats (PDF, Excel, Email Body) delivered via FOIA requests.
2. **As a System:** I need to "clean" the data to remove non-commercial entities (e.g., home renovations) if mixed with business data.
3. **As a User (Chamber):** I want to see the Business Name, Owner Name, and Contact Info for every new business in my zip code range.
4. **As an Admin:** I need to automatically "enrich" the raw data (which may lack emails/phones) using third-party APIs.

---

## Technical Specifications

### Phase 1: Data Acquisition (Ingestion)

**Source:** City/State Clerks & Business Licensing Bureaus

**Trigger:** Monthly scheduled task (Cron job or Manual Trigger)

**Methods:**
- *Direct:* Web scraping public "New Business" portals (where available)
- *Indirect:* Automating email requests to clerks for CSV/Excel exports

**Target Data Fields:**
| Field | Required | Notes |
|-------|----------|-------|
| Entity Name | Yes | DBA / LLC |
| Owner Name | Yes | Individual filer |
| Registration Date | Yes | Must be < 30 days |
| Mailing Address | Yes | For enrichment matching |
| Business Type | Preferred | NAICS code if available |

### Phase 2: Enrichment Pipeline

**Logic:** Raw government data rarely has direct emails or cell phones. We must bridge this gap.

**Process:**
1. Take `Owner Name` + `Mailing Address`
2. Ping Enrichment API (e.g., PeopleDataLabs, Clearbit, or similar)
3. Return `Email Address`, `LinkedIn Profile`, `Phone Number`

**Constraint:** If enrichment fails, flag lead as "Offline Only" (lower tier value)

**Vendor Considerations:**
- Compare API costs vs. the $10/lead sale price
- Must maintain margin after enrichment costs

### Phase 3: Delivery Format

**Output:** JSON object or CSV export ready for the JV Matchmaker dashboard

**Filtering Capabilities:**
- Segment by `Zip Code`
- Segment by `City`
- Segment by `Industry`

---

## Data Dictionary & Schema

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `lead_id` | UUID | Unique internal ID | System Gen |
| `business_name` | String | The DBA or Legal Entity | **FOIA** |
| `owner_name` | String | Individual filer | **FOIA** |
| `license_date` | Date | Date filed | **FOIA** |
| `raw_address` | String | Address on file | **FOIA** |
| `contact_email` | String | Validated email | **Enrichment** |
| `contact_phone` | String | Validated phone | **Enrichment** |
| `linkedin_url` | String | Owner's LinkedIn | **Enrichment** |
| `chamber_region` | Link | Which Chamber buys this? | Internal Logic |
| `enrichment_status` | Enum | `enriched` / `offline_only` / `pending` | System |

---

## Risks & Mitigations

### Data Consistency
**Risk:** Every city returns FOIA data differently. Some send PDFs (requires OCR/LLM parsing), others send CSVs.

**Mitigation:** Build a "Parser Router" where we can map specific cities to specific parsing scripts:
- `parse_portland_pdf`
- `parse_austin_csv`
- `parse_hillsboro_excel`

### Privacy & Legal
**Risk:** Potential violation of local laws regarding resale of government data.

**Mitigation:**
- Generally public record is fair game
- Strict adherence to "Commercial Use" clauses in FOIA requests
- Review each jurisdiction's specific rules

### Enrichment Costs
**Risk:** API costs could erode margin on $10/lead pricing.

**Mitigation:**
- Batch enrichment requests
- Cache results to avoid duplicate lookups
- Tier pricing based on enrichment success rate

---

## Implementation Phases

### Phase 1: Pilot (1 City)
- [ ] **Joe** | Prototype a scraper for **1 Pilot City** (e.g., Hillsboro) to validate data fields
- [ ] **Joe** | Select an Enrichment Vendor (compare API costs vs. the $10/lead sale price)
- [ ] **Partner** | Provide a list of target cities to prioritize based on initial Chamber sales calls

### Phase 2: Parser Framework
- [ ] Build generic parser interface
- [ ] Implement PDF parser (OCR + LLM extraction)
- [ ] Implement CSV/Excel parser
- [ ] Create city-to-parser mapping configuration

### Phase 3: Enrichment Integration
- [ ] Integrate with selected enrichment vendor API
- [ ] Build retry/fallback logic for failed enrichments
- [ ] Implement "Offline Only" flagging

### Phase 4: Dashboard Integration
- [ ] Add FOIA leads table to Supabase schema
- [ ] Build chamber admin view for lead browsing
- [ ] Implement filtering by zip/city/industry
- [ ] Add export functionality (CSV download)

---

## Integration with Platform

This connects to the Chamber of Commerce offering:

```
┌─────────────────────────────────────┐
│     Chamber of Commerce Package     │
├─────────────────────────────────────┤
│  Base: Member Matching ($500-1K/mo) │
│  + Upsell: FOIA Leads ($250-1K/mo)  │
│  = Total ARPU: $750-2K/mo           │
└─────────────────────────────────────┘
```

---

## Related Documents

- [CHAMBER_OF_COMMERCE_MATCHING.md](./CHAMBER_OF_COMMERCE_MATCHING.md) — Base offering this upsells from
- [../planning/EXPANSION_IDEAS.md](../planning/EXPANSION_IDEAS.md) — Other adjacent opportunities
- `docs/PRD.md` — Core platform requirements
