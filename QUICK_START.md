# Quick Start: Running OWL

## Step-by-Step Instructions

### 1. Open Your Terminal

Open Terminal (Mac) or Command Prompt/PowerShell (Windows).

### 2. Navigate to Your Project

```bash
cd /Users/josephtepe/Projects/jv-matchmaker-platform
```

### 3. Activate Your Virtual Environment

```bash
source venv/bin/activate
```

(If that doesn't work, try: `source .venv/bin/activate`)

You should see `(venv)` or `(.venv)` appear in your terminal prompt.

### 4. Run OWL

**Option A: Simple Test (Easiest)**
```bash
python test_owl_simple.py
```

**Option B: Built-in Test**
```bash
python matching/enrichment/owl_research/agents/owl_agent.py
```

### 5. Wait for Results

OWL will:
- Research the profile (takes 1-2 minutes)
- Print results to your terminal
- Show you the enriched profile data

---

## That's It!

You run it in YOUR terminal, not mine. Just copy-paste the commands above.

The script will use your free OpenRouter API key automatically.
