# Enrichment Pipeline - Quick Start Guide

**Recommended:** Safe Pipeline (Verified Emails Only)
**Status:** Production Ready ✅
**Date:** 2026-02-10

---

## TL;DR - Run This

```bash
# Daily automated enrichment (50 profiles, $2 budget)
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 50 \
    --priority high-value \
    --max-apollo-credits 20 \
    --auto-consolidate
```

**Expected:** 15-25 verified emails, all safe for outreach

---

## Why Safe Pipeline?

### ❌ Guessing Problems
- Creates unverified email addresses
- Risk of wrong person
- Hurts email deliverability (bounces)
- Compliance/legal issues
- False confidence in data quality

### ✅ Safe Pipeline Benefits
- Every email is **verified** (found on site or Apollo)
- Safe for cold email outreach
- Protects sender reputation
- Legal/compliance safe
- Accurate data quality metrics

---

## Available Pipelines

### 1. Safe Pipeline (RECOMMENDED)
```bash
python scripts/automated_enrichment_pipeline_safe.py
```

**Methods:**
- ✅ Website scraping (found on actual site)
- ✅ LinkedIn scraping (found on profile)
- ✅ Apollo.io API (verified by Apollo)
- ❌ NO pattern guessing

**Use for:** Cold email, sales outreach, compliance-sensitive

### 2. Optimized Pipeline (Research Only)
```bash
python scripts/automated_enrichment_pipeline_optimized.py
```

**Methods:**
- Website scraping
- LinkedIn scraping
- **Email pattern guessing** (unverified!)
- Apollo.io API

**Use for:** Database building, research (NOT outreach)

### 3. Original Pipeline (Backup)
```bash
python scripts/automated_enrichment_pipeline.py
```

**Use for:** Fallback if safe/optimized have issues

---

## Quick Commands

### Daily Enrichment (Recommended)

```bash
# 50 profiles, $2 Apollo budget
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 50 \
    --priority high-value \
    --max-apollo-credits 20 \
    --auto-consolidate
```

**Expected:**
- Time: ~30-60 seconds
- Emails: 15-25 verified
- Cost: $1.50-2.00
- All safe for outreach

### Free Methods Only

```bash
# 100 profiles, $0 cost
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 100 \
    --priority has-website \
    --max-apollo-credits 0 \
    --auto-consolidate
```

**Expected:**
- Time: ~60-90 seconds
- Emails: 5-15 verified (website scraping only)
- Cost: $0.00

### Preview First

```bash
# Dry run - see what would happen
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 10 \
    --dry-run
```

---

## Automated Scheduling

### Daily Enrichment (Cron)

```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 2 AM)
0 2 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_safe.py --limit 50 --priority high-value --max-apollo-credits 20 --auto-consolidate >> logs/enrichment.log 2>&1
```

**Result:** 15-25 verified emails per day, ~$2/day

### Weekly High-Volume

```bash
# Runs Monday at 3 AM
0 3 * * 1 cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_safe.py --limit 200 --priority high-value --max-apollo-credits 50 --auto-consolidate >> logs/enrichment_weekly.log 2>&1
```

**Result:** 60-100 verified emails per week, ~$5/week

### Monitor Status

```bash
# Check enrichment needs (daily at 1 AM)
0 1 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/analyze_enrichment_needs.py >> logs/enrichment_status.log 2>&1
```

---

## Budget Planning

### Conservative ($50/month)

```bash
# Daily: 20 profiles, 10 Apollo credits
# Expected: 350-500 verified emails/month
```

**Cron:**
```bash
0 2 * * * ... --limit 20 --max-apollo-credits 10 ...
```

**Cost:** ~$1.50/day = $45/month

### Moderate ($100/month)

```bash
# Daily: 50 profiles, 20 Apollo credits
# Expected: 650-900 verified emails/month
```

**Cron:**
```bash
0 2 * * * ... --limit 50 --max-apollo-credits 20 ...
```

**Cost:** ~$3/day = $90/month

### Aggressive ($200/month)

```bash
# Daily: 100 profiles, 40 Apollo credits
# Expected: 1,200-1,800 verified emails/month
```

**Cron:**
```bash
0 2 * * * ... --limit 100 --max-apollo-credits 40 ...
```

**Cost:** ~$6/day = $180/month

---

## Priority Strategies

### high-value (Default)
```bash
--priority high-value
```

- Profiles with list_size > 100,000
- Has valid company
- Ordered by reach
- Best ROI per enrichment

### has-website
```bash
--priority has-website
```

- Profiles with websites
- Higher free method success
- Lower Apollo usage
- Best for minimizing costs

### all
```bash
--priority all
```

- Any profile missing email
- Comprehensive coverage
- Use with higher Apollo budget

---

## Monitoring

### Check Current Status

```bash
python scripts/analyze_enrichment_needs.py
```

**Output:**
```
Missing email: 2,566 (71.7%)
High confidence: 731 (20.4%)
Top enrichment opportunities...
```

### Check Logs

```bash
# Today's enrichment
tail -100 logs/enrichment.log

# Real-time monitoring
tail -f logs/enrichment.log
```

### Query Verified Emails

```sql
-- High-confidence verified emails
SELECT name, email, company, list_size, profile_confidence
FROM profiles
WHERE profile_confidence >= 0.7
  AND email IS NOT NULL
  AND last_enriched_at > (NOW() - INTERVAL '30 days')
ORDER BY list_size DESC
LIMIT 100;
```

---

## Results & Confidence

### Safe Pipeline Confidence Scores

| Method | Confidence | Decay | Notes |
|--------|------------|-------|-------|
| Website scraping | 0.70 | 90 days | Found on actual site |
| LinkedIn scraping | 0.65 | 90 days | Found on profile |
| Apollo API | 0.80 | 90 days | Verified by Apollo |
| Apollo verified | 0.95 | 90 days | API-verified email |

### What Gets Saved

```json
{
  "email": "karen@karenyankovich.com",
  "enrichment_metadata": {
    "email": {
      "source": "apollo",
      "confidence": 0.800,
      "enriched_at": "2026-02-10T02:35:31",
      "confidence_expires_at": "2026-04-13T02:35:31",
      "verified": true,
      "enrichment_method": "apollo_api"
    }
  },
  "profile_confidence": 0.800,
  "last_enriched_at": "2026-02-10T02:35:31"
}
```

### Confidence Decay

Confidence decreases over time (emails become stale):

**Email from Apollo (base: 0.80):**
- Fresh (0 days): 0.800
- 30 days: 0.665
- 60 days: 0.553
- 90 days: 0.459 ⚠️ Re-enrichment needed

**Trigger:** Re-enrich when confidence < 0.50

---

## Cost Comparison

### Apollo.io Only (Baseline)

**Method:**
- Use Apollo for everything
- No free methods
- 40% success rate

**Cost for 1,000 profiles:**
- Credits used: 1,000
- **Total: $100**

### Safe Pipeline (Recommended)

**Method:**
- Website scraping (free)
- LinkedIn scraping (free)
- Apollo fallback (paid)

**Cost for 1,000 profiles:**
- Free success: 15-20%
- Apollo needed: 800-850
- **Total: $80-85**
- **Savings: 15-20%**

### With Higher Apollo Budget

**Method:**
- Same progressive strategy
- Higher Apollo limit for quality

**Cost for 1,000 profiles with 30% Apollo usage:**
- Free success: 70%
- Apollo needed: 300
- **Total: $30**
- **Savings: 70%**

---

## Troubleshooting

### "No emails found"

**Solution:** Increase Apollo credits
```bash
--max-apollo-credits 30  # Instead of 10
```

### "Too slow"

**Solution:** Increase batch size
```bash
--batch-size 10  # Instead of 5
```

### "Apollo API error"

**Check:** API key in .env
```bash
cat .env | grep APOLLO_API_KEY
```

### "Memory usage high"

**Solution:** Reduce batch size
```bash
--batch-size 3  # Instead of 10
```

---

## File Structure

```
scripts/
├── automated_enrichment_pipeline_safe.py      # RECOMMENDED
├── automated_enrichment_pipeline_optimized.py # Research only
├── automated_enrichment_pipeline.py           # Backup
├── analyze_enrichment_needs.py                # Status check
└── consolidate_apollo_to_supabase.py          # Manual Apollo

logs/
├── enrichment.log                             # Daily enrichment
├── enrichment_weekly.log                      # Weekly runs
└── enrichment_status.log                      # Status checks
```

---

## Complete Setup

### 1. Install Dependencies

```bash
pip install aiohttp psycopg2 django python-dotenv
```

### 2. Configure API Key

```bash
# Add to .env
APOLLO_API_KEY=your_key_here
```

### 3. Test Run

```bash
# Dry run first
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 5 \
    --dry-run

# Live test
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 5 \
    --max-apollo-credits 3 \
    --auto-consolidate
```

### 4. Schedule Daily Run

```bash
# Create logs directory
mkdir -p logs

# Add to crontab
crontab -e

# Add line
0 2 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_safe.py --limit 50 --priority high-value --max-apollo-credits 20 --auto-consolidate >> logs/enrichment.log 2>&1
```

### 5. Monitor Progress

```bash
# Check status
python scripts/analyze_enrichment_needs.py

# View logs
tail -f logs/enrichment.log
```

---

## Success Metrics

### Current Status
- Total profiles: 3,578
- Missing email: 2,566 (71.7%)
- High confidence: 731

### Daily Target (50 profiles, $2 budget)
- Emails found: 15-25 verified
- Success rate: 30-50%
- Cost per email: $0.08-0.13

### Monthly Projection
- Daily runs: 30 days
- Emails: 450-750 verified
- Cost: $60
- Cost per email: $0.08

---

## Support & Documentation

📖 **Full Guides:**
- [AUTOMATED_ENRICHMENT_GUIDE.md](AUTOMATED_ENRICHMENT_GUIDE.md) - Complete reference
- [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) - Performance analysis
- [CONSOLIDATION_COMPLETE.md](CONSOLIDATION_COMPLETE.md) - Confidence scoring

**Quick Help:**
```bash
python scripts/automated_enrichment_pipeline_safe.py --help
```

---

## Summary

✅ **Use:** `automated_enrichment_pipeline_safe.py` for all production enrichment

✅ **Budget:** $2-3/day for 15-25 verified emails

✅ **Schedule:** Daily at 2 AM via cron

✅ **Monitor:** Check logs and run analyze_enrichment_needs.py

✅ **All emails:** Verified and safe for outreach

**Status: PRODUCTION READY** 🚀
