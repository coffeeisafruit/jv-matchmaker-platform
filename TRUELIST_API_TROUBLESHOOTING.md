# Truelist API Endpoint Troubleshooting

**Date:** 2026-02-10
**Status:** BLOCKED - Cannot find email validation endpoint

---

## Problem

Successfully authenticated with Truelist API, but cannot locate the email validation/verification endpoint.

## What Works ✅

**Endpoint:** `GET https://api.truelist.io/me`
**Status:** 200 OK
**Response:**
```json
{
  "email": "tepetrain@gmail.com",
  "name": "Joe Tepe",
  "account": {
    "payment_plan": "growth_v4"
  }
}
```

**Authentication:** Bearer token works correctly
- API Key: `eyJhbGciOiJIUzI1NiJ9...` (stored in .env as TRUELIST_API_KEY)
- Header: `Authorization: Bearer {token}`

---

## What Doesn't Work ❌

### All Attempted Endpoints (ALL return 404)

#### Simple Paths
- `/verify`
- `/validate`
- `/check`
- `/email`
- `/emails`
- `/single`
- `/test`
- `/batch`
- `/bulk`
- `/docs`

#### With Query Parameters (GET)
- `/?email=test@gmail.com`
- `/verify?email=test@gmail.com`
- `/validate?email=test@gmail.com`
- `/check?email=test@gmail.com`
- `/email?email=test@gmail.com`
- `/emails?email=test@gmail.com`
- `/single?email=test@gmail.com`

#### With JSON Body (POST)
- `POST /emails` with `{"email":"test@gmail.com"}`
- `POST /` with `{"emails":["test@gmail.com"]}`
- `POST /batch` with `{"emails":["test@gmail.com"]}`
- `POST /bulk` with `{"emails":["test@gmail.com"]}`

#### Versioned Paths
- `/v1/verify?email=test@gmail.com` (original attempt)
- `/v2/verify?email=test@gmail.com`
- `/api/emails?email=test@gmail.com`
- `/api/v1/emails?email=test@gmail.com`

#### API Specs
- `/openapi.json`
- `/swagger.json`

---

## Test Results from verify_existing_emails.py

**Attempted:** Verify 1,022 existing emails in database
**Result:** ALL returned "API error: 404"

**Examples of emails that returned 404** (these are likely valid):
- cindyj@cindyjholbrook.com
- carol@carollook.com
- robyn@alifeofchoice.ca

**Issue:** Not that emails are invalid - the API endpoint itself returns 404

---

## Documentation Issues

### Truelist Docs (https://truelist.io/docs/api)
- Shows "Loading API documentation..."
- Interactive docs don't load (JavaScript-rendered)
- Cannot access full endpoint specifications
- Base URL confirmed: `https://api.truelist.io`
- Rate limit confirmed: 10 requests/second

### Documentation URL Pattern
- `/me` endpoint works: `https://truelist.io/docs/api#tag/me/GET/me`
- URL pattern: `#tag/{section}/{method}/{path}`
- But cannot determine validation endpoint from this

---

## Next Steps (User Action Required)

### Option 1: Check Truelist Dashboard
1. Login to https://truelist.io
2. Navigate to API documentation or settings
3. Look for:
   - Code examples showing actual API calls
   - Endpoint paths for email validation
   - Sample curl commands
   - Integration guides

### Option 2: Contact Truelist Support
- Email: support@truelist.io (or check their website)
- Ask for: "Email validation API endpoint path for single email verification"
- Mention: API key works for `/me` but cannot find validation endpoint

### Option 3: Network Inspection
1. Open https://truelist.io in browser
2. Open Developer Tools (F12)
3. Go to Network tab
4. Use their web interface to validate an email
5. Inspect the API call made - this will show the actual endpoint

### Option 4: Check Email/Docs
- Check confirmation email from Truelist signup
- May contain API documentation links or quick start guide
- Integration guides often show actual endpoint paths

---

## Code Impact

### Files Blocked by This Issue

1. **scripts/automated_enrichment_pipeline_verified.py**
   - Line 61: `f"{self.base_url}/verify"` - WRONG
   - Needs correct endpoint path

2. **scripts/verify_existing_emails.py**
   - Same issue - using `/verify` endpoint
   - Line 61: Wrong base URL and path

### When Fixed

Once correct endpoint is found:
1. Update `EmailVerifier.base_url` and endpoint path
2. Test with single email first
3. Run verify_existing_emails.py on all 1,022 emails
4. Enable verified pipeline for daily enrichment
5. Execute aggressive 17-day enrichment plan

---

## Temporary Workaround

**Until Truelist endpoint is found, use Safe Pipeline:**

```bash
# Use safe pipeline (no verification needed)
python scripts/automated_enrichment_pipeline_safe.py \
    --limit 50 \
    --priority high-value \
    --max-apollo-credits 20 \
    --auto-consolidate
```

**Trade-offs:**
- Lower discovery rate (30-50% vs 80-90%)
- Higher cost per email ($0.10-0.16 vs $0.03-0.06)
- But: all emails are verified by source
- Safe for outreach

---

## Summary

**Problem:** Cannot find Truelist email validation API endpoint
**Authentication:** ✅ Works (API key valid)
**Base URL:** ✅ Correct (`https://api.truelist.io`)
**Endpoint Path:** ❌ Unknown

**Critical:** Need correct API endpoint path to enable:
- Verified enrichment pipeline
- Existing email verification
- Aggressive 17-day enrichment plan
- Cost-effective verified emails ($0.03-0.06 each)

**Status:** User action required to obtain endpoint path from Truelist
