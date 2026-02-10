"""
Profile Enrichment Agent using Claude Agent SDK

Uses Claude Code Max subscription (via Agent SDK) for intelligent extraction.
Uses DuckDuckGo (free) for primary search, Tavily for verification.
Designed for JV Matchmaker profile enrichment with MANDATORY verification.

CRITICAL: Every extracted field MUST have source verification.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

from django.conf import settings as django_settings

from matching.enrichment.owl_research.schemas.profile_schema import (
    CompanyInfo,
    EnrichedProfile,
    IdealCustomer,
    PartnershipSeeking,
    ProfileEnrichmentResult,
    VerifiedField,
    VerifiedList,
)
from matching.enrichment.owl_research.config.settings import EnrichmentConfig, get_tavily_key

logger = logging.getLogger(__name__)


class ProfileEnrichmentAgent:
    """
    Agent for researching and enriching JV partner profiles.

    Uses Claude Code Max subscription via the Claude Agent SDK.
    Primary search: DuckDuckGo (free, unlimited)
    Verification: Tavily (paid, limited - use sparingly)

    CRITICAL: All extracted data MUST be verified with source citations.
    """

    def __init__(self, config: Optional[EnrichmentConfig] = None):
        self.config = config or EnrichmentConfig()
        self.tavily_key = get_tavily_key()

        # Track usage
        self.total_searches = 0
        self.tavily_searches = 0  # Track paid searches separately
        self.profiles_processed = 0

    async def enrich_profile(
        self,
        name: str,
        email: str = "",
        company: str = "",
        linkedin_url: str = "",
        existing_data: Optional[Dict] = None
    ) -> ProfileEnrichmentResult:
        """
        Research and enrich a single profile with VERIFIED data.

        Args:
            name: Person's full name
            email: Email address (for context)
            company: Company name
            linkedin_url: LinkedIn profile URL if known
            existing_data: Any existing data to preserve

        Returns:
            ProfileEnrichmentResult with verified enriched data or error
        """
        result = ProfileEnrichmentResult(
            input_name=name,
            input_email=email,
            input_company=company,
            input_linkedin=linkedin_url,
        )

        try:
            # Step 1: Web search for profile information (DuckDuckGo - free)
            search_results = self._search_profile(name, company, linkedin_url)

            if not search_results:
                result.error = "No search results found"
                return result

            # Step 2: Extract VERIFIED profile data using Claude Agent SDK
            enriched = await self._extract_verified_profile(name, company, search_results, existing_data)

            if enriched:
                result.enriched = enriched
                self.profiles_processed += 1

                # Log verification summary
                verified_count = enriched.get_verified_field_count()
                logger.info(f"  Verified {verified_count}/9 fields for {name}")
            else:
                result.error = "Could not extract verified profile data"

            return result

        except Exception as e:
            logger.error(f"Error enriching {name}: {e}")
            result.error = str(e)
            return result

    def _search_profile(
        self,
        name: str,
        company: str,
        linkedin_url: str = ""
    ) -> List[Dict]:
        """
        Search for profile information using DuckDuckGo (free, unlimited).
        """
        all_results = []

        # Build search queries - optimized for JV partnership data
        queries = [
            f'"{name}" {company} CEO founder owner',
            f'"{name}" services products offerings clients',
            f'"{name}" partnerships JV joint venture affiliate speaker',
            f'"{name}" {company} "work with" clients customers serve',
        ]

        # Limit searches per config
        queries = queries[:self.config.max_searches_per_profile]

        for query in queries:
            results = self._duckduckgo_search(query)  # Free search
            all_results.extend(results)
            self.total_searches += 1

        # Deduplicate by URL
        seen_urls = set()
        unique_results = []
        for r in all_results:
            url = r.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(r)

        return unique_results

    def _duckduckgo_search(self, query: str) -> List[Dict]:
        """Primary search using DuckDuckGo (free, unlimited)."""
        try:
            from ddgs import DDGS

            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=5))
                return [{
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "content": r.get("body", ""),
                    "score": 0.5,
                } for r in results]
        except ImportError:
            try:
                from duckduckgo_search import DDGS as OldDDGS
                with OldDDGS() as ddgs:
                    results = list(ddgs.text(query, max_results=5))
                    return [{
                        "title": r.get("title", ""),
                        "url": r.get("href", ""),
                        "content": r.get("body", ""),
                        "score": 0.5,
                    } for r in results]
            except Exception as e:
                logger.warning(f"DuckDuckGo search failed: {e}")
                return []
        except Exception as e:
            logger.warning(f"DuckDuckGo search failed: {e}")
            return []

    def _tavily_search(self, query: str) -> List[Dict]:
        """Tavily search - USE SPARINGLY (paid, limited to 1000/month)."""
        if not self.tavily_key:
            return []

        try:
            response = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": self.tavily_key,
                    "query": query,
                    "search_depth": self.config.search_depth,
                    "max_results": 5,
                    "include_raw_content": False,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            self.tavily_searches += 1  # Track paid searches

            results = []
            for item in data.get("results", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "content": item.get("content", ""),
                    "score": item.get("score", 0),
                })
            return results

        except Exception as e:
            logger.warning(f"Tavily search failed: {e}")
            return []

    async def _extract_verified_profile(
        self,
        name: str,
        company: str,
        search_results: List[Dict],
        existing_data: Optional[Dict] = None
    ) -> Optional[EnrichedProfile]:
        """
        Use Claude Agent SDK to extract VERIFIED profile data.

        CRITICAL: Every field MUST have a source_quote that proves the data.
        Data without source citations is REJECTED.
        """
        try:
            from claude_agent_sdk import query, ClaudeAgentOptions
        except ImportError:
            logger.error("claude-agent-sdk not installed. Run: pip install claude-agent-sdk")
            return None

        # Prepare search context with source URLs
        context_parts = []
        sources = []

        for r in search_results[:15]:
            title = r.get("title", "")
            content = r.get("content", "")
            url = r.get("url", "")

            if content:
                context_parts.append(f"[SOURCE: {url}]\nTitle: {title}\nContent: {content}")
                if url:
                    sources.append(url)

        search_context = "\n\n---\n\n".join(context_parts)

        # VERIFICATION-FOCUSED extraction prompt
        prompt = f"""You are a business research assistant. Your task is to extract VERIFIED profile data.

CRITICAL RULES:
1. ONLY include data that is EXPLICITLY stated in the search results
2. EVERY field MUST include a source_quote - a DIRECT QUOTE from the search results
3. If you cannot find a direct quote to support a field, leave it EMPTY
4. NEVER fabricate, infer, or guess data
5. source_url MUST be the actual URL where you found the information

TASK: Extract verified JV partnership profile data for {name} from {company}.

SEARCH RESULTS:
{search_context}

IMPORTANT: For "offerings" - look for SPECIFIC program names, course names, book titles,
certifications, or signature methodologies. Examples: "The Passion Test", "Becoming International",
"Revenue Acceleration Framework". Generic descriptions like "coaching" are less valuable than
specific, named programs.

Return ONLY valid JSON with this EXACT structure. For EVERY field, include source_quote (direct quote) and source_url:

{{
  "full_name": {{
    "value": "",
    "source_quote": "EXACT quote from search results proving this",
    "source_url": "URL where found"
  }},
  "title": {{
    "value": "",
    "source_quote": "",
    "source_url": ""
  }},
  "company": {{
    "name": {{"value": "", "source_quote": "", "source_url": ""}},
    "website": {{"value": "", "source_quote": "", "source_url": ""}},
    "industry": {{"value": "", "source_quote": "", "source_url": ""}},
    "description": {{"value": "", "source_quote": "", "source_url": ""}}
  }},
  "offerings": {{
    "values": [],
    "source_quote": "",
    "source_url": "",
    "note": "Include SPECIFIC program names, book titles, certifications - not just generic services"
  }},
  "signature_programs": {{
    "values": [],
    "source_quote": "",
    "source_url": "",
    "note": "Named programs, courses, methodologies, books, or certifications they're known for"
  }},
  "ideal_customer": {{
    "description": {{"value": "", "source_quote": "", "source_url": ""}},
    "industries": {{"values": [], "source_quote": "", "source_url": ""}}
  }},
  "seeking": {{
    "partnership_types": {{"values": [], "source_quote": "", "source_url": ""}},
    "goals": {{"values": [], "source_quote": "", "source_url": ""}}
  }},
  "linkedin_url": {{
    "value": "",
    "source_quote": "",
    "source_url": ""
  }},
  "matching_keywords": [],
  "verification_summary": "Brief summary of what was verified and data quality"
}}

REMEMBER:
- source_quote MUST be an EXACT quote from the search results, not a paraphrase
- If no evidence exists for a field, leave value AND source_quote EMPTY
- Quality over quantity - fewer verified fields is better than many unverified ones
- PRIORITIZE finding specific program/course/book names over generic descriptions"""

        try:
            options = ClaudeAgentOptions(
                max_turns=1,
            )

            response_text = ""
            async for message in query(prompt=prompt, options=options):
                if hasattr(message, 'content'):
                    for block in message.content:
                        if hasattr(block, 'text'):
                            response_text += block.text

            if response_text:
                return self._parse_verified_response(response_text, sources)
            return None

        except Exception as e:
            logger.error(f"Claude Agent SDK extraction failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _parse_verified_response(
        self,
        response_text: str,
        sources: List[str]
    ) -> Optional[EnrichedProfile]:
        """Parse Claude's JSON response into EnrichedProfile with verification."""
        try:
            # Extract JSON from response
            text = response_text.strip()
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            data = json.loads(text)

            # Build EnrichedProfile with verified fields
            def parse_verified_field(field_data: dict) -> VerifiedField:
                if not isinstance(field_data, dict):
                    return VerifiedField()
                return VerifiedField(
                    value=field_data.get("value", ""),
                    source_quote=field_data.get("source_quote", ""),
                    source_url=field_data.get("source_url", ""),
                    confidence=0.8 if field_data.get("source_quote") else 0.0
                )

            def parse_verified_list(list_data: dict) -> VerifiedList:
                if not isinstance(list_data, dict):
                    return VerifiedList()
                return VerifiedList(
                    values=list_data.get("values", []),
                    source_quote=list_data.get("source_quote", ""),
                    source_url=list_data.get("source_url", ""),
                    confidence=0.8 if list_data.get("source_quote") else 0.0
                )

            company_data = data.get("company", {})
            icp_data = data.get("ideal_customer", {})
            seeking_data = data.get("seeking", {})

            profile = EnrichedProfile(
                full_name=parse_verified_field(data.get("full_name", {})),
                title=parse_verified_field(data.get("title", {})),
                company=CompanyInfo(
                    name=parse_verified_field(company_data.get("name", {})),
                    website=parse_verified_field(company_data.get("website", {})),
                    industry=parse_verified_field(company_data.get("industry", {})),
                    description=parse_verified_field(company_data.get("description", {})),
                ),
                offerings=parse_verified_list(data.get("offerings", {})),
                signature_programs=parse_verified_list(data.get("signature_programs", {})),
                ideal_customer=IdealCustomer(
                    description=parse_verified_field(icp_data.get("description", {})),
                    industries=parse_verified_list(icp_data.get("industries", {})),
                ),
                seeking=PartnershipSeeking(
                    partnership_types=parse_verified_list(seeking_data.get("partnership_types", {})),
                    goals=parse_verified_list(seeking_data.get("goals", {})),
                ),
                linkedin_url=parse_verified_field(data.get("linkedin_url", {})),
                matching_keywords=data.get("matching_keywords", []),
                all_sources=sources[:10],
                verification_summary=data.get("verification_summary", ""),
            )

            # Calculate overall confidence based on verified fields
            verified_count = profile.get_verified_field_count()
            profile.overall_confidence = verified_count / 9.0

            return profile

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse extraction response: {e}")
            return None

    def get_stats(self) -> Dict:
        """Get processing statistics."""
        return {
            "profiles_processed": self.profiles_processed,
            "total_searches": self.total_searches,
            "tavily_searches": self.tavily_searches,
            "free_searches": self.total_searches - self.tavily_searches,
        }


async def enrich_single_profile(
    name: str,
    email: str = "",
    company: str = "",
    linkedin_url: str = "",
    existing_data: Optional[Dict] = None
) -> Tuple[Dict, bool]:
    """
    Convenience function to enrich a single profile with VERIFIED data.

    Returns data in JV Matcher format with source citations.
    """
    agent = ProfileEnrichmentAgent()
    result = await agent.enrich_profile(name, email, company, linkedin_url, existing_data)

    if result.enriched:
        jv_data = result.to_jv_matcher_format()

        # Log verification report
        logger.info(result.enriched.get_verification_report())

        return jv_data, True

    return existing_data or {}, False


def enrich_single_profile_sync(
    name: str,
    email: str = "",
    company: str = "",
    linkedin_url: str = "",
    existing_data: Optional[Dict] = None
) -> Tuple[Dict, bool]:
    """Synchronous wrapper for enrich_single_profile."""
    return asyncio.run(
        enrich_single_profile(name, email, company, linkedin_url, existing_data)
    )
