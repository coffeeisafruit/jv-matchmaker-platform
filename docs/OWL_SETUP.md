# OWL Framework Setup Guide

## Overview

The OWL (Open-World Learning) framework is configured to use **free LLM models** via OpenRouter. This guide will help you set up and use OWL for profile enrichment.

## Quick Start

### 1. Get Your Free OpenRouter API Key

1. Visit [https://openrouter.ai/keys](https://openrouter.ai/keys)
2. Sign up for a free account (no credit card required)
3. Create a new API key
4. Copy your API key

### 2. Configure Your Environment

Your API key is already configured in the root `.env` file. The OWL framework will automatically use it.

**Root `.env` file location:** `/Users/josephtepe/Projects/jv-matchmaker-platform/.env`

**OWL Framework `.env` file:** `/Users/josephtepe/Projects/jv-matchmaker-platform/owl_framework/owl/.env`

Both files are configured and will work together.

### 3. Free Models Available

The system is configured to use these free models (in order of preference):

1. **`meta-llama/llama-3.2-3b-instruct:free`** (default) - Fast, efficient, good for research tasks
2. **`openrouter/free`** - Auto-selects the best available free model

To change the model, set `OPENROUTER_MODEL` in your `.env` file:
```bash
OPENROUTER_MODEL=openrouter/free  # Auto-select best free model
```

### 4. How It Works

The OWL agent will:
1. Check for `OPENROUTER_API_KEY` first (uses free models)
2. Fall back to `ANTHROPIC_API_KEY` if OpenRouter is not available (requires paid API)
3. Raise an error if neither is found

## Testing the Setup

You can test the OWL setup by running:

```python
from matching.enrichment.owl_research.agents.owl_agent import OWLEnrichmentAgent

agent = OWLEnrichmentAgent(timeout=120)
result = agent.enrich_profile(
    name="Test Person",
    company="Test Company",
    website="https://example.com",
)
print(result)
```

## Cost Information

### Free Models (OpenRouter)
- **Cost:** $0.00 per request
- **Limits:** Rate limits apply, but no cost
- **Best for:** Development, testing, small-scale operations

### Paid Models (Anthropic Claude)
- **Cost:** ~$0.003 per 1K input tokens, $0.015 per 1K output tokens
- **Best for:** Production, high-quality research

## Troubleshooting

### Error: "No API key found"
- Check that `OPENROUTER_API_KEY` is set in your `.env` file
- Verify the key is valid at [https://openrouter.ai/keys](https://openrouter.ai/keys)
- Ensure the `.env` file is in the correct location

### Error: "Model not found"
- Check that `OPENROUTER_MODEL` is set to a valid free model name
- Try using `openrouter/free` for auto-selection

### OWL Framework Not Found
- Ensure the `owl_framework` directory exists
- Check that the virtual environment is set up: `owl_framework/.venv`

## Next Steps

1. ✅ API key configured
2. ✅ Free model selected (`meta-llama/llama-3.2-3b-instruct:free`)
3. ✅ OWL agent updated to use OpenRouter
4. 🚀 Ready to use!

You can now run OWL enrichment tasks using free models!
