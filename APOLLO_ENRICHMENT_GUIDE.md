# Apollo.io Enrichment Guide

## Quick Start (5 minutes to enrich 100 profiles!)

### 1. Get Your Apollo.io API Key

1. Go to https://app.apollo.io/#/settings/integrations/api
2. Copy your API key
3. Free tier: **60 credits/month** (enough for 60 emails)
4. Paid tier: 10,000+ credits/month

### 2. Test with Dry Run (No Credits Used)

```bash
python3 scripts/enrich_with_apollo.py \
  --api-key "YOUR_API_KEY_HERE" \
  --batch enrichment_batches/batch3_has_company.csv \
  --limit 5 \
  --dry-run
```

This shows what would be enriched without using credits.

### 3. Enrich Top 20 High-Value Targets

**Cost:** ~$2-4 (20 credits × $0.10-0.20 each)
**Time:** 30 seconds (with bulk API)
**Value:** 20.8M combined reach!

```bash
python3 scripts/enrich_with_apollo.py \
  --api-key "YOUR_API_KEY_HERE" \
  --batch enrichment_batches/batch3_has_company.csv \
  --limit 20 \
  --output enriched_top20.csv
```

**Top 20 includes:**
- Michelle Tennant (7.4M list) @ Wasabi Publicity
- John Assaraf (1.1M list) @ NeuroGym
- Rebecca Murtagh (1.0M list) @ Human AI Institute
- Kim Serafini (1.0M list) @ Positive Prime
- And 16 more with 200K-600K lists each!

### 4. Enrich All 50 from Batch 3

**Cost:** ~$5-10 (50 credits)
**Time:** 1-2 minutes
**Value:** 25M+ combined reach

```bash
python3 scripts/enrich_with_apollo.py \
  --api-key "YOUR_API_KEY_HERE" \
  --batch enrichment_batches/batch3_has_company.csv \
  --limit 50 \
  --output enriched_batch3.csv
```

### 5. Review Results

```bash
# Check the CSV
cat enriched_batch3.csv | head -10

# Or open in Excel/Google Sheets
open enriched_batch3.csv
```

### 6. Update Supabase

```bash
# Generate SQL update commands
python3 scripts/update_enriched_emails.py \
  --input enriched_batch3.csv \
  --dry-run

# Copy the SQL output and run in Supabase SQL editor
```

## Command Options

```bash
--api-key API_KEY       Your Apollo.io API key (required)
--batch BATCH_FILE      Input CSV from enrichment_batches/ (required)
--output OUTPUT_FILE    Where to save results (default: enriched_apollo.csv)
--limit N              Only enrich first N profiles
--delay SECONDS        Delay between API calls (default: 0.5)
--dry-run              Test without using credits
--no-bulk              Process one at a time (slower, for debugging)
```

## Expected Results

### Success Rates (Based on Apollo.io Data Quality):

- **Work Emails:** 70-80% found
- **Personal Emails:** 40-50% found (if enabled)
- **Phone Numbers:** 30-40% found (costs extra)
- **LinkedIn URLs:** 90%+ found

### Sample Output:

```
======================================================================
APOLLO.IO ENRICHMENT
======================================================================

Input:  enrichment_batches/batch3_has_company.csv
Output: enriched_batch3.csv
Limit:  20

Found 50 profiles in batch

======================================================================
ENRICHMENT PROGRESS
======================================================================

  1. 🔍 Michelle Tennant            @ Wasabi Publicity, Inc.   | List:  7,458,483 | Querying... ✅ michelle@wasabipublicity.com          | verified
  2. 🔍 John Assaraf                @ NeuroGym                 | List:  1,154,000 | Querying... ✅ john@neurogym.com                     | verified
  3. 🔍 Rebecca Murtagh             @ Human AI Institute       | List:  1,027,500 | Querying... ✅ rebecca@humanaiinstitute.com          | verified
  ...

======================================================================
ENRICHMENT SUMMARY
======================================================================

Total Profiles:      20
Enriched:            20
Emails Found:        16 (80.0%)
Phones Found:        0
LinkedIn Found:      18
Errors:              0
Skipped:             0
Credits Used:        20

💰 Cost: ~$2.00 (estimated at $0.10/credit)
```

## Rate Limits

- **600 requests per hour** (single match API)
- **300 requests per hour** (bulk match API - but 10 people per request!)
- With bulk API: Can enrich **3,000 people/hour** (vs 600 with single API)

## Cost Estimates

### Free Tier (60 credits/month):
- ✅ Top 60 high-value targets = **FREE**
- Best strategy: Prioritize by list size

### Growth Plan ($49/month, 1,500 credits):
- ✅ All 100 batch targets + validation
- ✅ Monthly ongoing enrichment
- Cost per email: ~$0.03

### Professional Plan ($99/month, 10,000+ credits):
- ✅ Enrich all 3,581 Supabase profiles
- ✅ Continuous enrichment
- Cost per email: ~$0.01

## Troubleshooting

### "Invalid API Key"
- Check you copied the full key from https://app.apollo.io/#/settings/integrations/api
- Make sure to use `x-api-key` header (script handles this)

### "Rate Limit Exceeded"
- Wait an hour, or
- Use `--delay 2.0` to slow down requests, or
- Use `--limit 50` to process in smaller batches

### "No Email Found"
- Some profiles may not have public emails
- Try enabling personal email search (costs more):
  - Modify script to set `reveal_personal_emails=True`

### "Company Not Found"
- Company name might be misspelled in CSV
- Try adding domain manually to CSV
- Check batch CSV for data quality

## Next Steps After Enrichment

1. **Validate emails** (optional but recommended):
   ```bash
   # Use ZeroBounce or similar
   # Cost: ~$0.005 per email
   ```

2. **Update Supabase** with SQL:
   ```sql
   UPDATE profiles SET email = 'found@email.com' WHERE id = 'uuid';
   ```

3. **Re-export matches**:
   ```bash
   python3 scripts/export_top_matches.py --limit 100 --output new_matches.csv
   ```

4. **Start test outreach** to top 10-20 matches

## Tips for Best Results

1. **Start small:** Test with `--limit 5` first
2. **Use bulk API:** 10x faster than single (enabled by default)
3. **Prioritize by list size:** Top 20 = 20M+ reach for ~$2
4. **Check credit balance:** Monitor at https://app.apollo.io/#/settings/credits
5. **Validate before outreach:** Bad emails hurt sender reputation

## ROI Calculation

**Top 20 Enrichment:**
- Cost: $2-4 (20 credits)
- Reach unlocked: 20.8M email subscribers
- Value if purchased: $1,000+ (at $0.05/contact)
- **ROI: 250-500x**

**All 100 High-Value Targets:**
- Cost: $10-20 (100 credits)
- Reach unlocked: 37.5M email subscribers
- Value if purchased: $5,000+
- **ROI: 250-500x**

This is why Apollo.io API is worth it! 🚀
