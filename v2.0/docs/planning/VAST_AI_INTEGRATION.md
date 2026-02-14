# Vast.ai Self-Hosted Model Integration

**Status:** Future consideration (trigger at 500+ profiles/month)
**Estimated Savings:** 90-99% reduction in AI costs
**Integration Effort:** ~3 hours

---

## Overview

When enrichment volume exceeds 500 profiles/month, integrate Vast.ai with OSS 120B/20B models to replace Claude API calls. This approach uses rented GPUs (~$0.75/hour) running open-source models with OpenAI-compatible API endpoints.

### Use Case Mapping

| Platform Feature | Vast.ai Equivalent | File to Modify |
|------------------|-------------------|----------------|
| Website → profile extraction | "Find business owner" | `ai_research.py` |
| Content verification (4-5 calls/match) | ICP classification | `ai_verification.py` |
| Deep research synthesis | Content extraction | `deep_research.py` |

---

## Implementation Steps

### 1. Environment Configuration

Add to `.env`:
```bash
# Vast.ai Configuration
VAST_API_ENDPOINT=https://your-vast-instance.vast.ai/v1
VAST_MODEL=oss-120b  # or oss-20b for faster/cheaper
LLM_PROVIDER=vast    # Options: vast, openrouter, anthropic
```

### 2. Create Unified LLM Client

Create `matching/enrichment/llm_client.py`:

```python
"""
Unified LLM client supporting Claude, OpenRouter, and Vast.ai
"""
import os
import openai
from typing import Optional

class LLMClient:
    """Drop-in replacement for Claude API calls."""

    def __init__(self, provider: Optional[str] = None):
        self.provider = provider or os.environ.get('LLM_PROVIDER', 'openrouter')

        if self.provider == 'vast':
            self.base_url = os.environ.get('VAST_API_ENDPOINT')
            self.api_key = os.environ.get('VAST_API_KEY', 'not-needed')
            self.model = os.environ.get('VAST_MODEL', 'oss-120b')
        elif self.provider == 'openrouter':
            self.base_url = 'https://openrouter.ai/api/v1'
            self.api_key = os.environ.get('OPENROUTER_API_KEY')
            self.model = 'anthropic/claude-sonnet-4'
        else:  # anthropic
            self.base_url = None  # Use native client
            self.api_key = os.environ.get('ANTHROPIC_API_KEY')
            self.model = 'claude-sonnet-4-20250514'

    def complete(self, prompt: str, max_tokens: int = 2048, temperature: float = 0) -> Optional[str]:
        """Call LLM and return response text."""
        if self.provider in ('vast', 'openrouter'):
            client = openai.OpenAI(
                base_url=self.base_url,
                api_key=self.api_key,
            )
            response = client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        else:
            import anthropic
            client = anthropic.Anthropic(api_key=self.api_key)
            message = client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
```

### 3. Update Existing Files

Replace Claude client instantiation in:
- `matching/enrichment/ai_research.py` (lines 35-51)
- `matching/enrichment/ai_verification.py` (lines 35-51)
- `matching/enrichment/deep_research.py`

---

## Ready-to-Use Prompts

These are the existing prompts that work with OSS 120B. Test with 20 profiles before full migration.

### Profile Research Extraction Prompt

```
You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated on the website.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Website: {website}

Website Content:
<content>
{content}
</content>

Extract the following fields. Only include information that is DIRECTLY stated:

1. what_you_do: What is their primary business/service? (1-2 sentences max)
   - Look for: "I help...", "We provide...", "Our mission...", About section

2. who_you_serve: Who is their target audience? (1 sentence max)
   - Look for: "I work with...", "For...", "Serving...", client descriptions

3. seeking: What are they actively looking for? (partnerships, speaking, affiliates, etc.)
   - Look for: "Looking for...", "Seeking...", "Partner with us", JV/affiliate mentions
   - If nothing explicitly stated, leave EMPTY

4. offering: What do they offer to partners/collaborators?
   - Look for: Podcast, email list mentions, speaking platforms, courses, certifications
   - Only include if they explicitly mention offering it to others

5. social_proof: Any notable credentials (bestseller, certifications, audience size)
   - Only include verifiable claims they make

Return as JSON. Use empty string "" for fields without explicit information:
{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "social_proof": "",
    "confidence": "high/medium/low",
    "source_quotes": ["quote1", "quote2"]
}

IMPORTANT:
- "confidence" should be "high" only if you found clear, explicit statements
- Include "source_quotes" with 1-2 direct quotes from the content that support your extraction
- If you're unsure, set confidence to "low" and leave the field empty
- Business accuracy matters - do NOT fabricate or assume
```

### Formatting Verification Prompt

```
You are a content verification agent for a professional PDF report.

Evaluate the following {field_name} content for formatting quality:

<content>
{content}
</content>

Check for:
1. COMPLETE SENTENCES - Does each sentence end properly? Any cut-off text?
2. STRUCTURE - Are there clear sections with headers?
3. READABILITY - Is it easy to scan and understand?
4. LENGTH - Content should fit within {max_length} characters without truncation. Current length: {len(content)} chars.
5. BOTH PARTIES - If this is mutual benefit content, are BOTH parties' benefits clearly listed?

Respond in JSON format:
{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}
```

### Content Quality Verification Prompt

```
You are a content quality agent for a JV partnership matching system.

Evaluate this content for {partner_name}:

<content>
{content}
</content>

Partner data available:
{partner_data}

Check for:
1. SPECIFICITY - Does it use actual partner data or generic phrases?
2. PERSONALIZATION - Is the partner's name and business correctly referenced?
3. VALUE PROPOSITION - Is the mutual benefit clear and specific?
4. NO PLACEHOLDERS - Check for [brackets], {braces}, or "INSERT HERE" patterns

Respond in JSON format:
{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}
```

---

## Testing Protocol

Before migrating:

1. **Setup Test Harness**
   ```bash
   python scripts/test_vast_vs_claude.py --profiles 20
   ```

2. **Compare Outputs**
   - Field accuracy (exact match %)
   - JSON formatting success rate
   - Source citation compliance
   - Confidence scoring alignment

3. **Acceptance Criteria**
   - >90% field accuracy match
   - >95% valid JSON responses
   - No hallucinated content

4. **Gradual Rollout**
   - Week 1: Verification pipeline only
   - Week 2: Add profile research
   - Week 3: Full migration if metrics hold

---

## Cost Comparison

| Volume | Claude (current) | Vast.ai | Savings |
|--------|-----------------|---------|---------|
| 100/mo | $30-60 | ~$2-5 | 90%+ |
| 500/mo | $150-300 | ~$10-25 | 93%+ |
| 1000/mo | $300-600 | ~$20-50 | 95%+ |

**Break-even:** ~50 profiles/month (integration effort pays off immediately above this)

---

## Vast.ai Setup Reference

Quick setup (via ChatGPT screenshots approach):

1. Create account at vast.ai
2. Search for GPU with OSS 120B or OSS 20B
3. Rent instance (~$0.75/hour)
4. Note API endpoint URL
5. Test with curl:
   ```bash
   curl $VAST_API_ENDPOINT/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d '{"model": "oss-120b", "messages": [{"role": "user", "content": "Hello"}]}'
   ```

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-10 | Created future plan | Volume at 100-500/mo, savings ~$50-100/mo not urgent |
| - | Trigger: 500+ profiles/mo | At this volume, savings exceed $150/mo, worth integration effort |
