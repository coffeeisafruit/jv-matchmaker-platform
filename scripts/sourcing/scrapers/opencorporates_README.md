# OpenCorporates Scraper

## Overview

Scrapes company data from OpenCorporates, the largest open database of companies in the world with 200M+ companies.

**Source:** `opencorporates`  
**Category:** `business_registrations`  
**API:** https://api.opencorporates.com/v0.4/companies/search

## Setup

1. Get your free API key from https://opencorporates.com/api_accounts/new
2. Add to `.env`:
   ```
   OPENCORPORATES_API_KEY=your_key_here
   ```

## Rate Limits

- **Free tier:** ~500 requests/day
- **Scraper setting:** 10 requests/minute (conservative)
- **Coverage:** 35 queries × 10 jurisdictions × 2 pages = ~700 URLs per run

## Search Strategy

### Queries (35 JV-relevant industries)
- Consulting, coaching, training, marketing agency
- Advertising, PR, management consulting
- Business development, strategic advisory, venture
- Joint venture, partnership, investment group
- Accelerator, incubator, venture capital
- Digital marketing, brand agency, media company
- Technology/IT consulting, software development
- Financial advisory, wealth management, insurance
- Real estate investment, property management
- Franchise, staffing, recruiting
- Health coaching, wellness, fitness
- Legal services, accounting, CPA

### Jurisdictions (Top 10 US states)
- California (us_ca)
- New York (us_ny)
- Texas (us_tx)
- Florida (us_fl)
- Illinois (us_il)
- Pennsylvania (us_pa)
- Ohio (us_oh)
- Georgia (us_ga)
- North Carolina (us_nc)
- Michigan (us_mi)

## Usage

```bash
# Test with dry-run (no database writes)
python3 -m scripts.sourcing.runner --source opencorporates --dry-run --max-pages 10

# Full run (writes to database)
python3 -m scripts.sourcing.runner --source opencorporates --max-pages 100

# Check status
python3 -m scripts.sourcing.runner --source opencorporates --status
```

## Data Extracted

Each company becomes a `ScrapedContact` with:
- **name**: Cleaned company name (entity suffixes removed)
- **company**: Full legal name
- **website**: State registry URL (if available)
- **bio**: `{name} | {address} | {company_type} | Inc: {date}`
- **source_category**: `business_registrations`

## Deduplication

- By `company_number + jurisdiction_code`
- Skips inactive companies
- Removes generic/invalid names

## Expected Volume

- **Per run:** ~700-1,400 companies (2 pages per query×jurisdiction)
- **Per day:** Limited to 500 API requests (free tier)

## Expansion Options

To scrape all 50 US states (not just top 10):
1. Edit `generate_urls()` method
2. Change `self.US_JURISDICTIONS` to `self.ALL_US_JURISDICTIONS`
3. Reduce `max_pages_per_query` to stay within daily limit

## Notes

- OpenCorporates API now requires authentication (as of 2026)
- Free tier provides basic company data (name, address, incorporation date, status)
- Premium tiers offer additional data (officers, financial statements, etc.)
- Scraper focuses on Active companies only
