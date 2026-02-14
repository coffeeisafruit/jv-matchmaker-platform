# JV Matchmaker Platform

## Project Overview

Django-based platform for matching joint venture partners. Uses AI enrichment pipelines to research and qualify potential partners from various data sources.

## Key Directories

- `matching/` - Core matching engine and enrichment pipelines
- `matching/enrichment/` - AI-powered research and verification
- `owl_framework/` - External framework for browser automation
- `v2.0/` - Future planning and expansion documentation
- `scripts/` - Automation scripts for batch processing

## Development Commands

```bash
# Run Django server
python manage.py runserver

# Run enrichment pipeline
python scripts/automated_enrichment_pipeline_safe.py --batch-size 10

# Run tests
pytest
```

## AI/LLM Configuration

Currently uses Claude Sonnet via OpenRouter or Anthropic API. Configuration in:
- `matching/enrichment/ai_research.py` - Profile extraction
- `matching/enrichment/ai_verification.py` - Content verification
- `matching/enrichment/deep_research.py` - Multi-source research

Environment variables:
- `OPENROUTER_API_KEY` - Primary (preferred)
- `ANTHROPIC_API_KEY` - Fallback
- `TAVILY_API_KEY` - Web search (paid, limited)

---

## Scaling Triggers

### TRIGGER: Enrichment Volume > 500 profiles/month

**Action Required:** Consider Vast.ai integration for 90%+ cost reduction.

**When to evaluate:**
- Monthly enrichment exceeds 500 profiles
- AI costs exceed $150/month
- Need higher throughput (16+ req/sec)

**Implementation plan:** See `v2.0/docs/planning/VAST_AI_INTEGRATION.md`

**Quick summary:**
- Rent GPU on Vast.ai (~$0.75/hour)
- Run OSS 120B with OpenAI-compatible API
- Drop-in replacement for Claude calls
- Saves $150-400/month at scale

---

## Code Style

- Use type hints for function signatures
- Docstrings for public methods
- JSON responses from AI must be validated
- Source citations required for extracted data (anti-hallucination)

## Testing

- Enrichment tests: `pytest matching/tests/`
- Integration tests require API keys in environment
