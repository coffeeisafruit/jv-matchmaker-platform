# OWL Troubleshooting Guide

## Common Issues and Solutions

### Error: "No API key found"

**Solution:**
1. Check that `.env` file exists: `cat .env | grep OPENROUTER`
2. Verify `owl_framework/owl/.env` exists and has the key
3. Make sure you activated your virtual environment: `source venv/bin/activate`

---

### Error: "429 - Rate-limited" or "Provider returned error"

**What it means:** The free model you're using is temporarily rate-limited (too many people using it).

**Solutions:**

1. **Wait and retry** (easiest)
   - Free models have rate limits
   - Wait 1-2 minutes and try again

2. **Switch to auto-select model** (recommended)
   ```bash
   # Edit owl_framework/owl/.env
   OPENROUTER_MODEL=openrouter/free
   ```
   This automatically picks the best available free model.

3. **Try a different free model**
   ```bash
   # Edit owl_framework/owl/.env
   OPENROUTER_MODEL=google/gemini-2.0-flash-001:free
   ```
   (Check OpenRouter for current free models: https://openrouter.ai/models?order=free)

4. **Use your own API key** (if you have credits)
   - Your OpenRouter key is already configured
   - Free models are shared, but your own key helps with rate limits
   - You can add credits to OpenRouter for better limits

---

### Error: "Timeout" or "Research timed out"

**Solution:**
- Increase timeout in your script:
  ```python
  agent = OWLEnrichmentAgent(timeout=300)  # 5 minutes instead of 2
  ```

---

### Error: "OWL_VENV not found" or "Python not found"

**Solution:**
1. Check OWL venv exists: `ls owl_framework/.venv/bin/python`
2. If missing, you may need to set up the OWL framework:
   ```bash
   cd owl_framework
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # if requirements.txt exists
   ```

---

### Error: "Django settings not configured"

**Solution:**
- This is normal when running standalone scripts
- The fix has been applied - OWL now reads `.env` files directly
- Make sure your `.env` files exist and have the API keys

---

### Slow Performance

**Solutions:**
1. Use parallel processing for batches:
   ```bash
   python manage.py owl_enrich_batch --workers 3 --max 10
   ```

2. Reduce delay between profiles:
   ```bash
   python manage.py owl_enrich_batch --delay 1.0 --max 10
   ```

3. Process fewer profiles at once:
   ```bash
   python manage.py owl_enrich_batch --max 5
   ```

---

## Getting Help

If you're still stuck:
1. Check the error message carefully
2. Verify your `.env` files have the correct API keys
3. Try waiting a few minutes if it's a rate limit error
4. Check OpenRouter status: https://openrouter.ai/models
