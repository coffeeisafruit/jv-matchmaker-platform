"""
OWL-Powered Profile Enrichment using Claude Agent SDK

Combines:
- Claude Agent SDK (uses your Max subscription - no API key needed)
- OWL's toolkits (BrowserToolkit, SearchToolkit) for deep research

This gives you OWL's research power without needing separate API keys.
"""

import asyncio
import json
import logging
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Email regex pattern - catches most valid emails
EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
)

# Booking link patterns
BOOKING_PATTERNS = [
    r'calendly\.com/[^\s"\'<>]+',
    r'acuityscheduling\.com/[^\s"\'<>]+',
    r'tidycal\.com/[^\s"\'<>]+',
    r'cal\.com/[^\s"\'<>]+',
    r'hubspot\.com/meetings/[^\s"\'<>]+',
    r'bookme\.name/[^\s"\'<>]+',
]


def extract_emails_from_text(text: str) -> List[str]:
    """Extract email addresses directly from text using regex."""
    if not text:
        return []

    emails = EMAIL_PATTERN.findall(text)

    # Filter out common false positives
    filtered = []
    skip_domains = ['example.com', 'email.com', 'domain.com', 'yoursite.com', 'company.com']
    skip_prefixes = ['noreply', 'no-reply', 'donotreply', 'mailer-daemon']

    for email in emails:
        email_lower = email.lower()
        # Skip example/placeholder emails
        if any(d in email_lower for d in skip_domains):
            continue
        # Skip system emails
        if any(email_lower.startswith(p) for p in skip_prefixes):
            continue
        # Skip image/file extensions that look like emails
        if any(email_lower.endswith(ext) for ext in ['.png', '.jpg', '.gif', '.svg']):
            continue
        filtered.append(email)

    return list(set(filtered))  # Dedupe


def extract_booking_links_from_text(text: str) -> List[str]:
    """Extract booking/scheduling links from text."""
    if not text:
        return []

    links = []
    for pattern in BOOKING_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            # Ensure it's a full URL
            if not match.startswith('http'):
                match = 'https://' + match
            links.append(match)

    return list(set(links))


def extract_website_from_search_results(results: List[Dict], name: str, company: str) -> Optional[str]:
    """
    Extract the most likely official website from search results.

    Filters out social media, directories, and other non-official sites.
    """
    if not results:
        return None

    # Domains to skip (not their official website)
    skip_domains = [
        'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
        'youtube.com', 'tiktok.com', 'pinterest.com',
        'amazon.com', 'goodreads.com', 'wikipedia.org',
        'yelp.com', 'bbb.org', 'glassdoor.com',
        'jvdirectory.com', 'podcasts.apple.com', 'spotify.com',
        'podchaser.com', 'listennotes.com',
    ]

    name_lower = name.lower()
    company_lower = company.lower() if company else ""

    for result in results:
        url = result.get('href', result.get('url', ''))
        if not url:
            continue

        url_lower = url.lower()

        # Skip social media and directories
        if any(domain in url_lower for domain in skip_domains):
            continue

        # Skip if it's clearly not their site (e.g., news article about them)
        if '/news/' in url_lower or '/article/' in url_lower:
            continue

        # Prefer URLs that might contain their name or company
        # But accept the first non-social result as fallback
        return url

    return None

# Path to OWL's venv for using its toolkits
OWL_DIR = Path(__file__).parent.parent.parent.parent.parent / "owl_framework"
OWL_VENV = OWL_DIR / ".venv"
OWL_PYTHON = OWL_VENV / "bin" / "python"


class OWLToolkitRunner:
    """
    Runs OWL toolkit operations in OWL's Python 3.12 venv.

    This lets us use OWL's browser automation and search
    while keeping the main Django app in Python 3.13.
    """

    @staticmethod
    def search_duckduckgo(query: str, max_results: int = 10) -> List[Dict]:
        """Run DuckDuckGo search using ddgs package (in main venv)."""
        try:
            from ddgs import DDGS
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=max_results))
                return [{
                    "title": r.get("title", ""),
                    "href": r.get("href", ""),
                    "body": r.get("body", ""),
                } for r in results]
        except ImportError:
            # Fallback to OWL's toolkit
            script = f'''
import json
from camel.toolkits import SearchToolkit
toolkit = SearchToolkit()
results = toolkit.search_duckduckgo("{query.replace('"', '\\"')}", max_results={max_results})
print(json.dumps(results if isinstance(results, list) else [results]))
'''
            return OWLToolkitRunner._run_owl_script(script)
        except Exception as e:
            return [{"error": str(e)}]

    @staticmethod
    def search_google(query: str, max_results: int = 10) -> List[Dict]:
        """Run Google search using OWL's SearchToolkit."""
        script = f'''
import json
from camel.toolkits import SearchToolkit
toolkit = SearchToolkit()
try:
    results = toolkit.search_google("{query.replace('"', '\\"')}", max_results={max_results})
    print(json.dumps(results if isinstance(results, list) else [results]))
except Exception as e:
    print(json.dumps({{"error": str(e)}}))
'''
        return OWLToolkitRunner._run_owl_script(script)

    @staticmethod
    def browse_website(url: str, extract_text: bool = True) -> Dict:
        """
        Browse a website using OWL's BrowserToolkit with Playwright.

        This actually visits the page and extracts content.
        """
        script = f'''
import json
import asyncio
from playwright.async_api import async_playwright

async def fetch_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto("{url}", timeout=30000)
            await page.wait_for_load_state("networkidle", timeout=10000)

            # Extract text content
            text = await page.evaluate("() => document.body.innerText")
            title = await page.title()

            # Get meta description
            meta_desc = await page.evaluate("""
                () => {{
                    const meta = document.querySelector('meta[name="description"]');
                    return meta ? meta.content : '';
                }}
            """)

            result = {{
                "url": "{url}",
                "title": title,
                "meta_description": meta_desc,
                "text_content": text[:15000],  # Limit to 15k chars
                "success": True
            }}
        except Exception as e:
            result = {{"url": "{url}", "error": str(e), "success": False}}
        finally:
            await browser.close()
        return result

result = asyncio.run(fetch_page())
print(json.dumps(result))
'''
        return OWLToolkitRunner._run_owl_script(script, timeout=60)

    @staticmethod
    def _run_owl_script(script: str, timeout: int = 30) -> any:
        """Run a Python script in OWL's venv."""
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.py', dir=str(OWL_DIR), delete=False
        ) as f:
            f.write(script)
            script_path = f.name

        try:
            result = subprocess.run(
                [str(OWL_PYTHON), script_path],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(OWL_DIR),
            )

            if result.returncode == 0 and result.stdout.strip():
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return {"raw": result.stdout, "error": "JSON parse failed"}
            else:
                return {"error": result.stderr or "No output", "stdout": result.stdout}

        except subprocess.TimeoutExpired:
            return {"error": f"Timeout after {timeout}s"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            Path(script_path).unlink(missing_ok=True)


class OWLProfileEnricher:
    """
    Profile enrichment using OWL toolkits + Claude Agent SDK.

    Research flow:
    1. Search DuckDuckGo for the person
    2. Browse their website + CONTACT PAGES (critical for outreach)
    3. Browse their LinkedIn (if provided)
    4. Use Claude Agent SDK to synthesize findings into structured profile
    """

    # Common contact page paths to check
    CONTACT_PATHS = [
        "/contact",
        "/contact-us",
        "/book",
        "/book-a-call",
        "/schedule",
        "/booking",
        "/work-with-me",
        "/hire-me",
        "/get-started",
        "/connect",
        "/about",  # Often has contact info
    ]

    def __init__(self):
        self.toolkit = OWLToolkitRunner()
        self.profiles_processed = 0
        self.total_searches = 0

    async def enrich_profile(
        self,
        name: str,
        company: str = "",
        website: str = "",
        linkedin: str = "",
        existing_data: Optional[Dict] = None,
    ) -> Dict:
        """
        Research and enrich a single profile.

        Uses OWL toolkits for research, Claude Agent SDK for synthesis.
        """
        logger.info(f"Researching: {name} ({company})")

        # Step 1: Gather raw research data
        research_data = await self._gather_research(name, company, website, linkedin)

        # Step 2: Use Claude Agent SDK to synthesize into structured profile
        enriched = await self._synthesize_with_claude(name, company, research_data)

        self.profiles_processed += 1

        return {
            "input_name": name,
            "input_company": company,
            "enriched": enriched,
            "sources": research_data.get("sources", []),
            "research_timestamp": datetime.now().isoformat(),
        }

    async def _gather_research(
        self,
        name: str,
        company: str,
        website: str,
        linkedin: str,
    ) -> Dict:
        """Gather research from multiple sources using OWL toolkits."""

        research = {
            "search_results": [],
            "website_content": None,
            "contact_pages": [],  # NEW: Store contact page content
            "linkedin_content": None,
            "sources": [],
            # Direct extraction (regex) - catches what Claude might miss
            "extracted_emails": [],
            "extracted_booking_links": [],
        }

        # STEP 1: If no website provided, try to discover it via search
        discovered_website = None
        if not website or not website.startswith("http"):
            logger.info(f"  🔍 No website - searching to discover...")
            discovery_query = f'"{name}" {company} official website'
            discovery_results = self.toolkit.search_duckduckgo(discovery_query, max_results=8)
            self.total_searches += 1

            if isinstance(discovery_results, list):
                discovered_website = extract_website_from_search_results(
                    discovery_results, name, company
                )
                if discovered_website:
                    website = discovered_website
                    logger.info(f"  🌐 Discovered website: {website}")
                    research["sources"].append(website)
                # Add discovery results to search results too
                research["search_results"].extend(discovery_results)

            time.sleep(0.5)  # Rate limiting

        # Search queries optimized for JV partnership data
        # Strategy: Fewer, more targeted queries to avoid rate limits
        queries = [
            f'"{name}" {company} founder CEO about',                  # Identity & role
            f'"{name}" programs courses coaching offerings',          # Signature programs
            f'"{name}" contact email calendly "book a call"',        # Contact info (CRITICAL)
            f'"{name}" site:linkedin.com OR site:twitter.com',       # Social profiles (via search, not scraping)
        ]

        # Run searches with rate limiting
        for i, query in enumerate(queries):
            logger.info(f"  Searching: {query[:50]}...")
            results = self.toolkit.search_duckduckgo(query, max_results=5)
            self.total_searches += 1

            if isinstance(results, list):
                research["search_results"].extend(results)
                for r in results:
                    if isinstance(r, dict) and r.get("href"):
                        research["sources"].append(r["href"])

            # Rate limiting: small delay between searches to avoid blocks
            if i < len(queries) - 1:
                time.sleep(0.5)

        # Browse website (either provided or discovered)
        if website and website.startswith("http"):
            logger.info(f"  Browsing website: {website}")
            web_content = self.toolkit.browse_website(website)
            if web_content.get("success"):
                research["website_content"] = web_content
                research["sources"].append(website)

            # Extract emails/booking from homepage
            if web_content.get("text_content"):
                research["extracted_emails"].extend(
                    extract_emails_from_text(web_content["text_content"])
                )
                research["extracted_booking_links"].extend(
                    extract_booking_links_from_text(web_content["text_content"])
                )

            # CRITICAL: Browse contact pages for email/booking info
            base_url = website.rstrip("/")
            contact_pages_found = 0
            for path in self.CONTACT_PATHS:
                if contact_pages_found >= 3:  # Limit to 3 contact pages to save time
                    break
                contact_url = f"{base_url}{path}"
                logger.info(f"  Checking contact page: {contact_url}")
                contact_content = self.toolkit.browse_website(contact_url)
                if contact_content.get("success"):
                    research["contact_pages"].append(contact_content)
                    research["sources"].append(contact_url)
                    contact_pages_found += 1
                    logger.info(f"    ✓ Found contact page: {path}")

                    # Extract emails/booking from contact pages
                    if contact_content.get("text_content"):
                        research["extracted_emails"].extend(
                            extract_emails_from_text(contact_content["text_content"])
                        )
                        research["extracted_booking_links"].extend(
                            extract_booking_links_from_text(contact_content["text_content"])
                        )

        # Browse LinkedIn if provided (may be blocked)
        if linkedin and "linkedin.com" in linkedin:
            logger.info(f"  Attempting LinkedIn: {linkedin}")
            # LinkedIn often blocks scraping, so we note but don't fail
            li_content = self.toolkit.browse_website(linkedin)
            if li_content.get("success"):
                research["linkedin_content"] = li_content
                research["sources"].append(linkedin)

        # Dedupe extracted contact info
        research["extracted_emails"] = list(set(research["extracted_emails"]))
        research["extracted_booking_links"] = list(set(research["extracted_booking_links"]))

        # Log what we found directly
        if research["extracted_emails"]:
            logger.info(f"  📧 Direct extraction found emails: {research['extracted_emails']}")
        if research["extracted_booking_links"]:
            logger.info(f"  📅 Direct extraction found booking links: {research['extracted_booking_links']}")

        return research

    async def _synthesize_with_claude(
        self,
        name: str,
        company: str,
        research_data: Dict,
    ) -> Dict:
        """Use Claude Agent SDK to synthesize research into structured profile."""

        try:
            from claude_agent_sdk import query, ClaudeAgentOptions
        except ImportError:
            logger.error("claude-agent-sdk not installed")
            return self._fallback_synthesis(name, company, research_data)

        # Build context from research
        context_parts = []

        # Add search results
        for result in research_data.get("search_results", [])[:15]:
            if isinstance(result, dict):
                title = result.get("title", "")
                body = result.get("body", result.get("snippet", ""))
                url = result.get("href", result.get("url", ""))
                if body:
                    context_parts.append(f"[SOURCE: {url}]\n{title}\n{body}")

        # Add website content
        if research_data.get("website_content"):
            wc = research_data["website_content"]
            context_parts.append(f"[WEBSITE: {wc.get('url')}]\nTitle: {wc.get('title')}\n{wc.get('text_content', '')[:5000]}")

        # CRITICAL: Add contact page content (where emails/booking links live)
        for contact_page in research_data.get("contact_pages", []):
            if contact_page.get("success"):
                context_parts.append(
                    f"[CONTACT PAGE: {contact_page.get('url')}]\n"
                    f"Title: {contact_page.get('title')}\n"
                    f"{contact_page.get('text_content', '')[:3000]}"
                )

        # Add LinkedIn content
        if research_data.get("linkedin_content"):
            lc = research_data["linkedin_content"]
            context_parts.append(f"[LINKEDIN: {lc.get('url')}]\n{lc.get('text_content', '')[:3000]}")

        # Add directly extracted contact info (regex-based, high confidence)
        if research_data.get("extracted_emails"):
            context_parts.append(
                f"[DIRECTLY EXTRACTED EMAILS - HIGH CONFIDENCE]\n"
                f"Found these email addresses on their website:\n"
                + "\n".join(f"- {email}" for email in research_data["extracted_emails"][:5])
            )
        if research_data.get("extracted_booking_links"):
            context_parts.append(
                f"[DIRECTLY EXTRACTED BOOKING LINKS - HIGH CONFIDENCE]\n"
                f"Found these booking/scheduling links:\n"
                + "\n".join(f"- {link}" for link in research_data["extracted_booking_links"][:3])
            )

        research_context = "\n\n---\n\n".join(context_parts)

        prompt = f"""You are extracting verified profile data for JV partnership matching.

PERSON: {name}
COMPANY: {company}

RESEARCH DATA:
{research_context}

TASK: Extract ONLY information that is EXPLICITLY stated in the research data above.
For each field, include a source_quote (direct quote proving the data).
PRIORITY: Contact information (email, phone, booking links) is CRITICAL for outreach.

Return JSON with this structure:
{{
    "full_name": {{"value": "", "source_quote": "", "source_url": ""}},
    "title": {{"value": "", "source_quote": "", "source_url": ""}},
    "company": {{
        "name": {{"value": "", "source_quote": "", "source_url": ""}},
        "description": {{"value": "", "source_quote": "", "source_url": ""}}
    }},
    "email": {{"value": "", "source_quote": "", "source_url": ""}},
    "phone": {{"value": "", "source_quote": "", "source_url": ""}},
    "booking_link": {{"value": "", "source_quote": "", "source_url": ""}},
    "offerings": {{"values": [], "source_quote": "", "source_url": ""}},
    "signature_programs": {{"values": [], "source_quote": "", "source_url": ""}},
    "who_they_serve": {{"value": "", "source_quote": "", "source_url": ""}},
    "seeking": {{"values": [], "source_quote": "", "source_url": ""}},
    "linkedin_url": {{"value": "", "source_quote": "", "source_url": ""}},
    "verification_summary": ""
}}

RULES:
- ONLY include data with direct quotes from sources
- Leave fields empty if no evidence exists
- signature_programs should be SPECIFIC named programs, courses, books
- seeking should be partnership goals/interests

CONTACT INFO EXTRACTION (CRITICAL - prioritize finding these):
- EMAIL: Look for patterns like name@domain.com, info@, hello@, contact@
- BOOKING LINK: Look for calendly.com, acuityscheduling.com, "book a call", "schedule", "work with me"
- PHONE: Look for phone numbers, especially on contact pages
- If you find ANY email or booking link, include it even with lower confidence"""

        try:
            options = ClaudeAgentOptions(max_turns=1)

            response_text = ""
            async for message in query(prompt=prompt, options=options):
                if hasattr(message, 'content'):
                    for block in message.content:
                        if hasattr(block, 'text'):
                            response_text += block.text

            if response_text:
                return self._parse_claude_response(response_text)

        except Exception as e:
            logger.error(f"Claude Agent SDK error: {e}")

        return self._fallback_synthesis(name, company, research_data)

    def _parse_claude_response(self, response_text: str) -> Dict:
        """Parse Claude's JSON response."""
        try:
            text = response_text.strip()
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            return json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse response: {e}")
            return {"raw_response": response_text, "parse_error": str(e)}

    def _fallback_synthesis(self, name: str, company: str, research_data: Dict) -> Dict:
        """Fallback when Claude SDK isn't available."""
        return {
            "full_name": {"value": name, "source_quote": "", "source_url": ""},
            "company": {"name": {"value": company, "source_quote": "", "source_url": ""}},
            "raw_research": research_data,
            "fallback": True,
        }

    def get_stats(self) -> Dict:
        """Get processing statistics."""
        return {
            "profiles_processed": self.profiles_processed,
            "total_searches": self.total_searches,
        }


async def enrich_single_profile(
    name: str,
    company: str = "",
    website: str = "",
    linkedin: str = "",
) -> Tuple[Dict, bool]:
    """Convenience function to enrich a single profile."""
    enricher = OWLProfileEnricher()
    result = await enricher.enrich_profile(name, company, website, linkedin)

    success = bool(result.get("enriched") and not result["enriched"].get("fallback"))
    return result, success


def enrich_single_profile_sync(
    name: str,
    company: str = "",
    website: str = "",
    linkedin: str = "",
) -> Tuple[Dict, bool]:
    """Synchronous wrapper."""
    return asyncio.run(enrich_single_profile(name, company, website, linkedin))


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result, success = enrich_single_profile_sync(
        name="David Riklan",
        company="SelfGrowth.com",
        website="https://selfgrowth.com",
    )

    print(f"\nSuccess: {success}")
    print(json.dumps(result, indent=2, default=str))
