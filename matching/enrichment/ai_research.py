"""
AI-Powered Profile Research Service

Fetches REAL data from partner websites and extracts accurate profile information.
This is NOT inference - it's research to find what they actually say about themselves.
"""

import logging
import os
import re
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from django.conf import settings

logger = logging.getLogger(__name__)


class ProfileResearchService:
    """
    Researches sparse profiles by fetching their website and extracting
    accurate data about what they do, who they serve, and what they're seeking.

    Uses Claude to intelligently extract and verify profile data.
    """

    def __init__(self):
        # Try OpenRouter first, then Anthropic
        self.openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        self.anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')

        if self.openrouter_key:
            self.use_openrouter = True
            self.api_key = self.openrouter_key
            self.model = "anthropic/claude-sonnet-4"
        elif self.anthropic_key:
            self.use_openrouter = False
            self.api_key = self.anthropic_key
            self.model = "claude-sonnet-4-20250514"
        else:
            self.use_openrouter = False
            self.api_key = None
            self.model = None

        self.max_tokens = 2048

    def research_profile(self, name: str, website: str, existing_data: Dict) -> Dict:
        """
        Research a profile by fetching their website and extracting accurate data.

        Args:
            name: Partner's name
            website: Partner's website URL
            existing_data: Any data we already have (to avoid overwriting good data)

        Returns:
            Dict with researched fields: who_you_serve, what_you_do, seeking, offering
        """
        if not website:
            logger.info(f"No website for {name}, skipping research")
            return {}

        if not self.api_key:
            logger.warning("No API key configured for research")
            return {}

        # Normalize website URL
        website = self._normalize_url(website)

        # Fetch website content
        content = self._fetch_website(website)
        if not content:
            logger.warning(f"Could not fetch website for {name}: {website}")
            return {}

        # Extract profile data using AI
        researched = self._extract_profile_data(name, content, website, existing_data)

        return researched

    def _normalize_url(self, url: str) -> str:
        """Ensure URL has proper scheme."""
        url = url.strip()
        if not url.startswith('http'):
            url = 'https://' + url
        return url

    def _fetch_website(self, url: str) -> Optional[str]:
        """Fetch website content."""
        try:
            import requests
            from bs4 import BeautifulSoup

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }

            response = requests.get(url, headers=headers, timeout=25)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()

            # Get text content
            text = soup.get_text(separator='\n')

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            # Limit to first 8000 chars (enough for extraction)
            return text[:8000]

        except ImportError:
            logger.warning("requests or beautifulsoup4 not installed")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _extract_profile_data(self, name: str, content: str, website: str, existing: Dict) -> Dict:
        """
        Use AI to extract accurate profile data from website content.

        IMPORTANT: Only extract what they ACTUALLY say - no inference or fabrication.
        """
        first_name = name.split()[0] if name else 'Partner'

        prompt = f"""You are a business research assistant extracting FACTUAL profile data.

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
{{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "social_proof": "",
    "confidence": "high/medium/low",
    "source_quotes": ["quote1", "quote2"]
}}

IMPORTANT:
- "confidence" should be "high" only if you found clear, explicit statements
- Include "source_quotes" with 1-2 direct quotes from the content that support your extraction
- If you're unsure, set confidence to "low" and leave the field empty
- Business accuracy matters - do NOT fabricate or assume"""

        response = self._call_claude(prompt)
        return self._parse_research_response(response, name, existing)

    def _call_claude(self, prompt: str) -> Optional[str]:
        """Call Claude via OpenRouter or Anthropic API."""
        if not self.api_key:
            return None

        try:
            if self.use_openrouter:
                import openai

                client = openai.OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=self.api_key,
                )

                response = client.chat.completions.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=0,  # Deterministic for accuracy
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                return response.choices[0].message.content
            else:
                import anthropic

                client = anthropic.Anthropic(api_key=self.api_key)

                message = client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=0,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                return message.content[0].text

        except ImportError as e:
            logger.warning(f"Required package not installed: {e}")
            return None
        except Exception as e:
            logger.error(f"Error calling AI API: {e}")
            return None

    def _parse_research_response(self, response: str, name: str, existing: Dict) -> Dict:
        """Parse AI response and merge with existing data."""
        import json

        result = {}

        if not response:
            return result

        try:
            # Extract JSON from response
            text = response.strip()

            # Handle markdown code blocks
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            data = json.loads(text)

            confidence = data.get('confidence', 'low')
            source_quotes = data.get('source_quotes', [])

            # Log research results
            logger.info(f"Researched {name}: confidence={confidence}")
            if source_quotes:
                for quote in source_quotes[:2]:
                    logger.info(f"  Quote: '{quote[:80]}...'")

            # Only use high/medium confidence results
            if confidence in ('high', 'medium'):
                # Only add fields that are non-empty AND not already well-populated
                for field in ['what_you_do', 'who_you_serve', 'seeking', 'offering']:
                    new_value = data.get(field, '').strip()
                    existing_value = existing.get(field, '').strip()

                    # Only update if:
                    # 1. New value exists
                    # 2. Existing value is empty or very short
                    if new_value and (not existing_value or len(existing_value) < 10):
                        result[field] = new_value
                        logger.info(f"  Added {field}: {new_value[:60]}...")

                # Add social proof if not already present
                social_proof = data.get('social_proof', '').strip()
                if social_proof and not existing.get('bio'):
                    result['bio'] = social_proof

            else:
                logger.info(f"  Low confidence, skipping update for {name}")

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse research response for {name}: {e}")

        return result


class ProfileResearchCache:
    """
    Simple file-based cache for research results.
    Prevents re-fetching websites we've already researched.
    """

    def __init__(self, cache_dir: str = None):
        from pathlib import Path

        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path('/Users/josephtepe/Projects/jv-matchmaker-platform/Chelsea_clients/research_cache')

        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, name: str) -> str:
        """Generate cache key from name."""
        import hashlib
        return hashlib.md5(name.lower().encode()).hexdigest()[:12]

    def get(self, name: str) -> Optional[Dict]:
        """Get cached research result."""
        import json

        cache_file = self.cache_dir / f"{self._cache_key(name)}.json"

        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return None

    def set(self, name: str, data: Dict) -> None:
        """Cache research result."""
        import json

        cache_file = self.cache_dir / f"{self._cache_key(name)}.json"

        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to cache research for {name}: {e}")


def research_and_enrich_profile(
    name: str,
    website: str,
    existing_data: Dict,
    use_cache: bool = True,
    force_research: bool = False
) -> Tuple[Dict, bool]:
    """
    Main entry point: Research a profile and return enriched data.

    Args:
        name: Partner name
        website: Partner website
        existing_data: Current profile data
        use_cache: Whether to use cached results
        force_research: If True, always research (caller has already determined this is needed)

    Returns:
        Tuple of (enriched_data, was_researched)
    """
    cache = ProfileResearchCache()

    # Check cache first (unless force_research)
    if use_cache and not force_research:
        cached = cache.get(name)
        if cached:
            logger.info(f"Using cached research for {name}")
            return cached, False

    # Research if we have a website
    # Let the caller decide when research is needed (via force_research)
    # or check if data is actually sparse
    is_sparse = not existing_data.get('seeking') and not existing_data.get('who_you_serve')
    should_research = force_research or is_sparse

    if website and should_research:
        service = ProfileResearchService()
        researched = service.research_profile(name, website, existing_data)

        if researched:
            # Merge with existing data
            merged = {**existing_data, **researched}

            # Cache the result
            if use_cache:
                cache.set(name, merged)

            return merged, True

    return existing_data, False
