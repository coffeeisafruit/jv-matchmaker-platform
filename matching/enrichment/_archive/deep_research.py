"""
Deep Research Service using GPT Researcher

For profiles that simple website scraping can't fully enrich, this service
performs comprehensive multi-source research to find accurate data about
what they're seeking, who they serve, and what they offer.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Tuple

from django.conf import settings

logger = logging.getLogger(__name__)


class DeepResearchService:
    """
    Deep research service using GPT Researcher for comprehensive profile enrichment.

    Uses multi-source search to find:
    - What partners are actively seeking (JV partners, speaking, affiliates)
    - Who they serve (target audience)
    - What they offer (expertise, platforms, reach)
    - Social proof and credentials
    """

    def __init__(self):
        # Get API keys
        self.openai_key = getattr(settings, 'OPENAI_API_KEY', '') or os.environ.get('OPENAI_API_KEY', '')
        self.tavily_key = getattr(settings, 'TAVILY_API_KEY', '') or os.environ.get('TAVILY_API_KEY', '')

        # GPT Researcher requires these environment variables
        if self.openai_key:
            os.environ['OPENAI_API_KEY'] = self.openai_key
        if self.tavily_key:
            os.environ['TAVILY_API_KEY'] = self.tavily_key

    async def research_profile_async(self, name: str, company: str, existing_data: Dict) -> Dict:
        """
        Perform deep research on a profile using GPT Researcher.

        Args:
            name: Person's full name
            company: Company name
            existing_data: Any data we already have

        Returns:
            Dict with researched fields: seeking, who_you_serve, what_you_do, offering
        """
        try:
            from gpt_researcher import GPTResearcher
        except ImportError:
            logger.error("gpt-researcher not installed")
            return {}

        if not self.openai_key:
            logger.warning("No OPENAI_API_KEY configured for deep research")
            return {}

        # Build focused research query
        website = existing_data.get('website', '')
        business_focus = existing_data.get('business_focus', '')

        query = f"""Research {name} from {company} for JV partnership opportunities.

Find:
1. What partnerships or collaborations they are actively seeking (JV partners, speaking opportunities, affiliates, cross-promotions)
2. Who is their target audience / who do they serve
3. What services, products, or expertise do they offer
4. Any notable credentials, bestseller status, audience size, or social proof

Focus on recent information from their website ({website}), LinkedIn, interviews, podcasts, and press releases.
Only include factual, verifiable information."""

        try:
            # Initialize researcher with focused config
            researcher = GPTResearcher(
                query=query,
                report_type="research_report",
                config_path=None
            )

            # Conduct research
            research_result = await researcher.conduct_research()

            # Get the report
            report = await researcher.write_report()

            # Extract structured data from the report
            return self._extract_profile_data(name, report, existing_data)

        except Exception as e:
            logger.error(f"Deep research failed for {name}: {e}")
            return {}

    def research_profile(self, name: str, company: str, existing_data: Dict) -> Dict:
        """Sync wrapper for research_profile_async."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(
            self.research_profile_async(name, company, existing_data)
        )

    def _extract_profile_data(self, name: str, report: str, existing: Dict) -> Dict:
        """
        Use Claude to extract structured profile data from the research report.
        """
        from .ai_research import ProfileResearchService

        # Use Claude to extract structured data
        service = ProfileResearchService()

        prompt = f"""Extract JV partnership profile data from this research report about {name}.

Research Report:
{report[:4000]}  # Limit to prevent token overflow

Extract the following fields. Only include information that is EXPLICITLY stated:

1. seeking: What partnerships/collaborations are they actively looking for?
2. who_you_serve: Who is their target audience?
3. what_you_do: What is their primary business/service?
4. offering: What can they offer to JV partners?
5. social_proof: Notable credentials, audience size, achievements

Return as JSON:
{{
    "seeking": "",
    "who_you_serve": "",
    "what_you_do": "",
    "offering": "",
    "social_proof": "",
    "confidence": "high/medium/low",
    "sources": ["source1", "source2"]
}}

IMPORTANT: Only include factual, verifiable information from the report."""

        response = service._call_claude(prompt)

        if not response:
            return {}

        try:
            # Extract JSON from response
            text = response.strip()
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

            result = {}
            if confidence in ('high', 'medium'):
                for field in ['seeking', 'who_you_serve', 'what_you_do', 'offering']:
                    new_value = data.get(field, '').strip()
                    existing_value = existing.get(field, '').strip()

                    if new_value and (not existing_value or len(existing_value) < 10):
                        result[field] = new_value
                        logger.info(f"  Deep research found {field}: {new_value[:50]}...")

                if data.get('social_proof') and not existing.get('bio'):
                    result['bio'] = data['social_proof']

            return result

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse deep research response: {e}")
            return {}


class SimpleDeepResearch:
    """
    Simpler deep research using direct web search + Claude extraction.
    Doesn't require GPT Researcher's full setup.
    """

    def __init__(self):
        self.openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        self.serper_key = getattr(settings, 'SERPER_API_KEY', '') or os.environ.get('SERPER_API_KEY', '')

    def research_profile(self, name: str, company: str, existing_data: Dict) -> Dict:
        """
        Research a profile using web search + Claude extraction + verification.

        This approach:
        1. Searches multiple queries for comprehensive coverage
        2. Extracts data with source citations
        3. Verifies extracted data against sources
        4. Returns only verified, high-confidence data
        """
        if not self.openrouter_key:
            logger.warning("No OPENROUTER_API_KEY for deep research")
            return {}

        # Expanded search queries for more comprehensive coverage
        queries = [
            # Partnership/JV seeking
            f'"{name}" "{company}" seeking partnerships OR JV partner OR joint venture OR affiliate',
            # Interviews and media (often reveal what they're looking for)
            f'"{name}" interview podcast guest speaker',
            # Services and audience
            f'"{name}" "{company}" services clients audience',
            # Social proof and credentials
            f'"{name}" bestseller author speaker credentials award',
            # Contact and booking info
            f'"{name}" "{company}" contact book speaking calendar',
        ]

        # Collect search results with source URLs
        all_results = []
        source_urls = set()

        for query in queries:
            results = self._web_search(query)
            if results:
                for r in results:
                    all_results.append(r)
                    if r.get('url'):
                        source_urls.add(r['url'])

        if not all_results:
            logger.info(f"No search results found for {name}")
            return {}

        # Extract and verify profile data
        return self._extract_and_verify(name, company, all_results, list(source_urls), existing_data)

    def _web_search(self, query: str) -> List[Dict]:
        """Perform web search and return results with source URLs."""
        import requests

        if self.serper_key:
            # Use Serper API
            try:
                response = requests.post(
                    'https://google.serper.dev/search',
                    headers={'X-API-KEY': self.serper_key},
                    json={'q': query, 'num': 5}
                )
                data = response.json()

                results = []
                for item in data.get('organic', [])[:5]:
                    snippet = item.get('snippet', '')
                    title = item.get('title', '')
                    url = item.get('link', '')
                    if snippet:
                        results.append({
                            'text': f"{title}: {snippet}",
                            'title': title,
                            'url': url,
                            'snippet': snippet
                        })
                return results

            except Exception as e:
                logger.warning(f"Serper search failed: {e}")
                return []
        else:
            # Fallback: Use DuckDuckGo (no API key needed)
            try:
                from duckduckgo_search import DDGS

                with DDGS() as ddgs:
                    results = list(ddgs.text(query, max_results=5))
                    return [{
                        'text': f"{r['title']}: {r['body']}",
                        'title': r['title'],
                        'url': r.get('href', ''),
                        'snippet': r['body']
                    } for r in results]

            except ImportError:
                logger.warning("duckduckgo-search not installed")
                return []
            except Exception as e:
                logger.warning(f"DuckDuckGo search failed: {e}")
                return []

    def _extract_and_verify(self, name: str, company: str, results: List[Dict],
                            source_urls: List[str], existing: Dict) -> Dict:
        """
        Extract profile data from search results, then verify against sources.

        Two-step process:
        1. Extract data with source citations
        2. Verify each extracted field has supporting evidence
        """
        from .ai_research import ProfileResearchService

        service = ProfileResearchService()

        # Combine results for extraction
        combined_results = "\n\n".join([r['text'] for r in results[:15]])

        # Step 1: Extract with source citations
        extraction_prompt = f"""Extract JV partnership profile data for {name} from {company}.

Search Results:
{combined_results}

Extract the following fields. For each field, include the SOURCE (quote the text that supports it):

1. seeking: What partnerships/collaborations are they actively looking for?
2. who_you_serve: Who is their target audience?
3. what_you_do: What is their primary business/service?
4. offering: What can they offer to JV partners? (podcasts, courses, certifications, audience access)
5. social_proof: Notable credentials (bestseller, certifications, audience size, awards)
6. contact_info: Any email, phone, or calendar booking link found

Return as JSON:
{{
    "seeking": {{"value": "", "source_quote": ""}},
    "who_you_serve": {{"value": "", "source_quote": ""}},
    "what_you_do": {{"value": "", "source_quote": ""}},
    "offering": {{"value": "", "source_quote": ""}},
    "social_proof": {{"value": "", "source_quote": ""}},
    "contact_info": {{"value": "", "source_quote": ""}},
    "confidence": "high/medium/low",
    "verification_notes": "Brief explanation of data quality"
}}

CRITICAL RULES:
- Only include data that is EXPLICITLY stated in the search results
- source_quote MUST be a direct quote from the search results
- If no evidence found for a field, leave value AND source_quote empty
- "high" confidence = multiple sources confirm the data
- "medium" confidence = single source clearly states the data
- "low" confidence = data is inferred or unclear"""

        response = service._call_claude(extraction_prompt)

        if not response:
            return {}

        try:
            text = response.strip()
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
            verification_notes = data.get('verification_notes', '')

            logger.info(f"  Extraction confidence: {confidence}")
            if verification_notes:
                logger.info(f"  Verification: {verification_notes[:80]}...")

            result = {}

            # Only use high/medium confidence results
            if confidence in ('high', 'medium'):
                # Process each field
                field_mappings = [
                    ('seeking', 'seeking'),
                    ('who_you_serve', 'who_you_serve'),
                    ('what_you_do', 'what_you_do'),
                    ('offering', 'offering'),
                    ('social_proof', 'bio'),  # Map social_proof to bio field
                ]

                for src_field, dest_field in field_mappings:
                    field_data = data.get(src_field, {})
                    if isinstance(field_data, dict):
                        new_value = field_data.get('value', '').strip()
                        source_quote = field_data.get('source_quote', '').strip()
                    else:
                        new_value = str(field_data).strip() if field_data else ''
                        source_quote = ''

                    existing_value = existing.get(dest_field, '').strip()

                    # Only use if we have a value AND a source quote (verified)
                    if new_value and source_quote and (not existing_value or len(existing_value) < 10):
                        result[dest_field] = new_value
                        logger.info(f"    Verified {dest_field}: {new_value[:50]}...")
                        logger.info(f"      Source: \"{source_quote[:60]}...\"")

                # Handle contact info separately
                contact_data = data.get('contact_info', {})
                if isinstance(contact_data, dict):
                    contact_value = contact_data.get('value', '').strip()
                    if contact_value and '@' in contact_value and not existing.get('email'):
                        # Extract email from contact info
                        import re
                        email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_value)
                        if email_match:
                            result['email'] = email_match.group(0)
                            logger.info(f"    Found email: {result['email']}")

            # Add source URLs to result for reference
            if result and source_urls:
                result['_research_sources'] = source_urls[:5]  # Store top 5 sources

            return result

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse extraction response: {e}")
            return {}


def deep_research_profile(
    name: str,
    company: str,
    existing_data: Dict,
    use_gpt_researcher: bool = False
) -> Tuple[Dict, bool]:
    """
    Main entry point for deep research.

    Args:
        name: Person's name
        company: Company name
        existing_data: Current profile data
        use_gpt_researcher: Use full GPT Researcher (requires more setup)

    Returns:
        Tuple of (enriched_data, was_researched)
    """
    if use_gpt_researcher:
        service = DeepResearchService()
        result = service.research_profile(name, company, existing_data)
    else:
        service = SimpleDeepResearch()
        result = service.research_profile(name, company, existing_data)

    if result:
        merged = {**existing_data, **result}
        return merged, True

    return existing_data, False
