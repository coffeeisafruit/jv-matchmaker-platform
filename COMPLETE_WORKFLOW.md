# Complete OWL Workflow Guide

## What You Need to Do (X) → So OWL Can Work (Y) → So You Get Results (Z)

### Step X: Setup (What YOU Do)

1. **Ensure API keys are set** ✅ (Already done!)
   ```bash
   # Check your keys are there
   cat .env | grep OPENROUTER_API_KEY
   cat owl_framework/owl/.env | grep OPENROUTER_MODEL
   ```

2. **Activate your virtual environment**
   ```bash
   cd /Users/josephtepe/Projects/jv-matchmaker-platform
   source venv/bin/activate
   ```

3. **Run the test script**
   ```bash
   python test_owl_simple.py
   ```

---

### Step Y: What OWL Does (Automatically)

When you run the script, OWL will:

1. **Load your API keys** from `.env` files
2. **Create a research task** for the profile
3. **Use AI agents** to:
   - Search the web (Google, DuckDuckGo, Wikipedia)
   - Browse the person's website
   - Check their LinkedIn
   - Find interviews, articles, podcasts
4. **Extract structured data**:
   - Name, title, company
   - What they offer
   - Who they serve
   - Partnership interests
   - Specific programs/courses
5. **Return JSON results** with sources

---

### Step Z: What You Get (Results)

You'll receive a JSON object with:

```json
{
  "name": "Janet Bray Attwood",
  "enriched_data": {
    "full_name": "...",
    "title": "...",
    "company_name": "...",
    "offerings": [...],
    "signature_programs": [...],
    "who_they_serve": "...",
    "seeking": "...",
    "linkedin_url": "...",
    "sources": ["url1", "url2", ...]
  },
  "success": true
}
```

---

## Quick Start Commands

### For Testing (Single Profile)
```bash
cd /Users/josephtepe/Projects/jv-matchmaker-platform
source venv/bin/activate
python test_owl_simple.py
```

### For Batch Processing (Multiple Profiles)
```bash
# From CSV
python manage.py owl_enrich_batch --input profiles.csv --max 10

# From Supabase database
python manage.py owl_enrich_batch --from-supabase --max 10 --save-to-supabase
```

---

## Troubleshooting Chain

**If you get rate limit errors:**
- ✅ Already fixed - using `openrouter/free` auto-select

**If you get JSON parsing errors:**
- This is normal for free models
- **Solution:** Just retry - `python test_owl_simple.py`
- Free models are inconsistent (~50-70% success rate)

**If you want reliability:**
- Add credits to Anthropic (~$5-10)
- System will auto-use Claude if available

---

## The Complete Flow

```
YOU RUN COMMAND
    ↓
OWL LOADS API KEYS
    ↓
OWL CREATES RESEARCH TASK
    ↓
AI AGENTS SEARCH & BROWS
    ↓
AI EXTRACTS STRUCTURED DATA
    ↓
YOU GET JSON RESULTS
```

---

## Next Steps

1. **Test it now:**
   ```bash
   python test_owl_simple.py
   ```

2. **If it works:** Great! You can now process profiles

3. **If it fails:** Retry (free models are inconsistent)

4. **For production:** Consider adding Anthropic credits for reliability

---

## Summary

- **X (You):** Run `python test_owl_simple.py`
- **Y (OWL):** Researches profile automatically
- **Z (Result):** Get structured JSON data with sources

That's it! The system handles everything in between.
