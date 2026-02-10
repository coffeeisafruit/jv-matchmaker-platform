# Rate Limit Solutions for OWL

## The Problem

You're hitting rate limits on the free model `meta-llama/llama-3.2-3b-instruct:free`. This happens because:
- Free models are shared by many users
- OpenRouter limits: 20 requests/minute for free models
- Popular free models get rate-limited frequently

## Solutions (Pick One)

### Solution 1: Use Auto-Select Model (RECOMMENDED - Easiest)

This automatically picks the best available free model, avoiding rate limits:

```bash
# Edit owl_framework/owl/.env
OPENROUTER_MODEL=openrouter/free
```

Then run again:
```bash
python test_owl_simple.py
```

**Why this works:** `openrouter/free` automatically routes to whichever free model has capacity, avoiding rate limits.

---

### Solution 2: Wait and Retry

Free model rate limits reset quickly. Just wait 1-2 minutes and try again:

```bash
# Wait 1-2 minutes, then:
python test_owl_simple.py
```

---

### Solution 3: Use Anthropic Claude (If You Have Credits)

You have an Anthropic API key configured. If you have credits, it will automatically fall back to Claude:

```bash
# Check if Anthropic key is set
cat .env | grep ANTHROPIC_API_KEY

# If set, OWL will automatically use it if OpenRouter fails
python test_owl_simple.py
```

**Note:** Anthropic Claude is a paid model (~$0.003 per 1K tokens), but it's more reliable and doesn't have rate limits if you have credits.

---

### Solution 4: Try Different Free Models

Check current free models: https://openrouter.ai/models?order=free

Then update `owl_framework/owl/.env`:
```
OPENROUTER_MODEL=google/gemini-2.0-flash-001:free
# or
OPENROUTER_MODEL=meta-llama/llama-3.1-8b-instruct:free
```

---

### Solution 5: Add Credits to OpenRouter

If you add $10+ credits to OpenRouter, you get:
- 1,000 free requests/day (instead of 50)
- Better rate limits
- More reliable access

Visit: https://openrouter.ai/settings/integrations

---

## Quick Fix (Do This Now)

1. **Update the model to auto-select:**
   ```bash
   echo 'OPENROUTER_MODEL=openrouter/free' >> owl_framework/owl/.env
   ```

2. **Or edit the file directly:**
   ```bash
   # Find the line with OPENROUTER_MODEL and change it to:
   OPENROUTER_MODEL=openrouter/free
   ```

3. **Run again:**
   ```bash
   python test_owl_simple.py
   ```

---

## Understanding Rate Limits

- **Free models:** Shared by everyone → frequent rate limits
- **Auto-select (`openrouter/free`):** Routes to available models → fewer rate limits
- **Paid models:** Your own credits → no rate limits (if you have credits)

The code is working correctly - this is just a limitation of free shared models. Using `openrouter/free` is the best solution for free usage.
