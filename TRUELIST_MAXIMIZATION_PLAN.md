# Truelist Maximization Plan (Feb 10 - Feb 27)

**Urgency:** HIGH
**Timeline:** 17 days of Truelist access
**Goal:** Verify as many emails as possible before access expires

---

## Current Situation

**Today:** February 10, 2026
**Truelist expires:** February 27, 2026
**Time remaining:** 17 days

**Database status:**
- Total profiles: 3,578
- Missing email: 2,566 (71.7%)
- **Opportunity:** Enrich 2,500+ profiles with verification

**Truelist rate limit:** 10 requests/second
- Built-in rate limiting with retry logic
- Automatic handling of 429 errors
- No manual throttling needed

---

## Aggressive Enrichment Strategy

### Option 1: Maximum Volume (Recommended)

**Daily enrichment:** 150 profiles/day for 17 days

```bash
# Schedule this NOW
crontab -e

# Add this line
0 2 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_verified.py --limit 150 --priority has-website --max-apollo-credits 30 --auto-consolidate >> logs/enrichment_verified.log 2>&1
```

**Expected results over 17 days:**
- Profiles processed: 2,550
- Emails discovered: 2,000-2,300 (80-90%)
- Emails verified: 1,000-1,600 (50-70% valid)
- **Verified emails added:** 1,000-1,600 ✅

**Costs:**
- Apollo: $150-250 (500-800 credits @ $0.10)
- Truelist: $4-4.50 (2,000-2,300 verifications @ $0.002)
- **Total: $154-255 for 1,000-1,600 verified emails**
- **Cost per email: $0.10-0.16**

### Option 2: Moderate Volume

**Daily enrichment:** 100 profiles/day

**Expected over 17 days:**
- Verified emails: 650-1,000
- Cost: $100-170

### Option 3: Conservative

**Daily enrichment:** 50 profiles/day

**Expected over 17 days:**
- Verified emails: 350-500
- Cost: $50-85

---

## Immediate Action Plan

### Day 1 (Today - Feb 10)

**1. Start large test run NOW**
```bash
python scripts/automated_enrichment_pipeline_verified.py \
    --limit 200 \
    --priority has-website \
    --max-apollo-credits 40 \
    --auto-consolidate
```

**Expected:** 80-140 verified emails, $4-6

**2. Schedule daily runs**
```bash
crontab -e

# Add aggressive daily enrichment
0 2 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_verified.py --limit 150 --priority has-website --max-apollo-credits 30 --auto-consolidate >> logs/enrichment_verified.log 2>&1

# Add midday run for double throughput
0 14 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_verified.py --limit 100 --priority high-value --max-apollo-credits 20 --auto-consolidate >> logs/enrichment_afternoon.log 2>&1
```

**Two runs per day = 250 profiles/day!**

### Days 2-16 (Feb 11-26)

**Monitor daily:**
```bash
# Check logs
tail -f logs/enrichment_verified.log

# Check status
python scripts/analyze_enrichment_needs.py

# Check verified count
psql $DATABASE_URL -c "SELECT COUNT(*) FROM profiles WHERE email IS NOT NULL AND enrichment_metadata->'email'->>'verified' = 'true';"
```

**Adjust limits based on:**
- Success rate (if high, increase limits)
- Apollo budget (track daily spend)
- Verification rate (if low, reduce pattern guessing)

### Day 17 (Feb 27)

**Final push:**
```bash
# One last big run before access expires
python scripts/automated_enrichment_pipeline_verified.py \
    --limit 300 \
    --priority all \
    --max-apollo-credits 60 \
    --auto-consolidate
```

---

## After Feb 27: Transition Plan

### Option 1: Switch to Safe Pipeline

**No verification, only found emails:**
```bash
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 100 \
    --max-apollo-credits 30 \
    --auto-consolidate
```

**Cost:** $3-5/day
**Volume:** 30-50 verified emails/day

### Option 2: Pay for Truelist

**Truelist pricing:**
- $16/1,000 verifications (ZeroBounce alternative)
- Or other services: Hunter.io, NeverBounce

**Decision:** After seeing 17-day results, decide if worth paying

### Option 3: Hybrid Approach

**Weekdays:** Safe pipeline (no guessing)
**Weekends:** Batch verify with paid service

---

## Tracking & Optimization

### Daily Metrics to Track

```sql
-- Daily verified email count
SELECT
    DATE(last_enriched_at) as date,
    COUNT(*) as verified_emails,
    AVG(profile_confidence) as avg_confidence
FROM profiles
WHERE enrichment_metadata->'email'->>'verified' = 'true'
  AND last_enriched_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(last_enriched_at)
ORDER BY date DESC;
```

### Weekly Goals

| Week | Target | Cost | Status |
|------|--------|------|--------|
| Week 1 (Feb 10-16) | 500-700 emails | $50-90 | |
| Week 2 (Feb 17-23) | 500-700 emails | $50-90 | |
| Week 3 (Feb 24-27) | 200-300 emails | $20-40 | |
| **Total** | **1,200-1,700** | **$120-220** | |

### Success Criteria

✅ **Minimum goal:** 1,000 verified emails
✅ **Target goal:** 1,500 verified emails
✅ **Stretch goal:** 2,000 verified emails

---

## Budget Scenarios

### Aggressive ($250 for 17 days)

**Daily:**
- 150 profiles (morning)
- 100 profiles (afternoon)
- 250 profiles/day total

**Results:**
- 4,250 profiles processed
- ~1,800-2,400 verified emails
- Database 66-95% complete!

### Moderate ($150 for 17 days)

**Daily:**
- 150 profiles (morning only)

**Results:**
- 2,550 profiles processed
- ~1,000-1,600 verified emails
- Database 40-65% complete

### Conservative ($75 for 17 days)

**Daily:**
- 75 profiles

**Results:**
- 1,275 profiles processed
- ~500-800 verified emails
- Database 20-30% complete

---

## Recommended: AGGRESSIVE Plan

### Why Aggressive?

1. **One-time opportunity** - Truelist access expires
2. **High ROI** - $0.10-0.15 per verified email (cheap!)
3. **Future savings** - Won't need to re-verify these later
4. **Complete database** - Get 60-90% coverage now

### Execute Now

```bash
# 1. Test run (do this RIGHT NOW)
python scripts/automated_enrichment_pipeline_verified.py \
    --limit 200 \
    --priority has-website \
    --max-apollo-credits 40 \
    --auto-consolidate

# 2. Schedule daily (if test looks good)
crontab -e

# Morning run (2 AM)
0 2 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_verified.py --limit 150 --priority has-website --max-apollo-credits 30 --auto-consolidate >> logs/enrichment_verified.log 2>&1

# Afternoon run (2 PM) - OPTIONAL for double throughput
0 14 * * * cd /Users/josephtepe/Projects/jv-matchmaker-platform && python3 scripts/automated_enrichment_pipeline_verified.py --limit 100 --priority high-value --max-apollo-credits 20 --auto-consolidate >> logs/enrichment_afternoon.log 2>&1

# 3. Monitor daily
tail -f logs/enrichment_verified.log
```

---

## Post-Truelist Strategy (After Feb 27)

### Evaluation Criteria

After 17 days, evaluate:

**If verification rate was HIGH (70%+):**
- ✅ Consider paying for Truelist ($16/1k)
- Pattern guessing is worth it with verification

**If verification rate was LOW (30-50%):**
- ✅ Switch to Safe pipeline (no guessing)
- Apollo-only is more cost-effective

**If budget is tight:**
- ✅ Use Safe pipeline
- ✅ Batch verify quarterly with paid service

---

## Summary

### Timeline

**Feb 10 (TODAY):** Start aggressive enrichment
**Feb 11-26:** Run 150-250 profiles/day
**Feb 27:** Final push before expiration
**Feb 28+:** Switch to Safe pipeline or pay for verification

### Expected Results (Aggressive Plan)

**Input:** 2,500-4,000 profiles
**Output:** 1,200-2,400 verified emails
**Cost:** $150-250
**Per email:** $0.10-0.15

### Action Items

- [ ] Run test with 200 profiles NOW
- [ ] Schedule daily cron jobs (150/day minimum)
- [ ] Monitor logs daily
- [ ] Track verification rates
- [ ] Decide on post-Feb-27 strategy by Feb 20

**Status: URGENT - START TODAY** 🚨

Run this NOW:
```bash
python scripts/automated_enrichment_pipeline_verified.py \
    --limit 200 \
    --priority has-website \
    --max-apollo-credits 40 \
    --auto-consolidate
```
