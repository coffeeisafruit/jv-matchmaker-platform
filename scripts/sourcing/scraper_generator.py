"""
Scraper generation system for uncovered market gaps.

When the gap-driven sourcing pipeline identifies needs that no existing scraper
covers, this module can:

1. Identify which gaps are "uncovered" (no scraper has matching metadata)
2. Recommend potential data sources from a curated registry
3. Generate a scaffold scraper module ready for review and customization

Usage:
    # CLI: Generate a scraper for a specific source
    python3 -m scripts.sourcing.scraper_generator --source stitcher_podcasts --preview
    python3 -m scripts.sourcing.scraper_generator --source stitcher_podcasts --generate

    # CLI: Analyze gaps and recommend new scrapers
    python3 -m scripts.sourcing.scraper_generator --analyze-gaps

    # Programmatic: from gap-driven sourcing task
    from scripts.sourcing.scraper_generator import (
        find_uncovered_gaps, recommend_sources, generate_scraper_module,
    )
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Potential Sources Registry
#
# Curated knowledge base of data sources we know exist but haven't built
# scrapers for yet. Each entry maps to niches, roles, and offerings so the
# gap matcher can recommend them. Entries include enough metadata (API type,
# URL pattern, auth needs) to generate a working scaffold.
# ---------------------------------------------------------------------------

@dataclass
class PotentialSource:
    """A known data source we could scrape but haven't yet."""
    name: str                          # Unique slug (becomes filename)
    display_name: str                  # Human-friendly name
    base_url: str                      # Root URL or API endpoint
    description: str                   # What it is
    api_type: str                      # "json_api", "html_scrape", "sitemap", "graphql", "rss"
    auth_type: str = "none"            # "none", "api_key", "oauth", "hmac"
    typical_roles: list[str] = field(default_factory=list)
    typical_niches: list[str] = field(default_factory=list)
    typical_offerings: list[str] = field(default_factory=list)
    estimated_yield: str = "unknown"   # e.g. "5,000-10,000"
    rate_limit: int = 10               # Suggested requests/min
    notes: str = ""                    # Implementation tips

    def matches_gap(self, gap_keywords: set[str], missing_roles: set[str],
                    underserved_niches: set[str]) -> float:
        """Score how well this source matches identified gaps. Higher = better."""
        score = 0.0

        offerings_lower = {o.lower() for o in self.typical_offerings}
        niches_lower = {n.lower() for n in self.typical_niches}
        roles_lower = {r.lower() for r in self.typical_roles}

        for kw in gap_keywords:
            for offering in offerings_lower:
                if kw in offering or offering in kw:
                    score += 3.0
                    break
            for niche in niches_lower:
                if kw in niche or niche in kw:
                    score += 2.0
                    break

        for role in missing_roles:
            for tr in roles_lower:
                if role in tr or tr in role:
                    score += 2.5
                    break

        for niche in underserved_niches:
            for tn in niches_lower:
                if niche in tn or tn in niche:
                    score += 1.5
                    break

        return score


# ---------------------------------------------------------------------------
# Registry of potential sources (expand over time)
# ---------------------------------------------------------------------------

POTENTIAL_SOURCES: list[PotentialSource] = [
    # --- Podcast / Media directories ---
    PotentialSource(
        name="listennotes",
        display_name="Listen Notes",
        base_url="https://www.listennotes.com",
        description="Podcast search engine with 3M+ podcasts. HTML scrape with structured data.",
        api_type="html_scrape",
        typical_roles=["Media/Publisher", "Thought Leader"],
        typical_niches=["podcasting", "content_marketing"],
        typical_offerings=["podcast", "interviews", "audience"],
        estimated_yield="50,000-100,000",
        rate_limit=8,
        notes="Category pages at /best-podcasts/. Structured data in JSON-LD. "
              "Paid API exists but scraping category pages is free.",
    ),
    PotentialSource(
        name="goodpods",
        display_name="Goodpods",
        base_url="https://goodpods.com",
        description="Curated podcast discovery with social features and host profiles.",
        api_type="html_scrape",
        typical_roles=["Media/Publisher", "Thought Leader"],
        typical_niches=["podcasting", "health_wellness", "business_coaching"],
        typical_offerings=["podcast", "interviews", "community"],
        estimated_yield="10,000-30,000",
        rate_limit=10,
    ),

    # --- Speaker directories ---
    PotentialSource(
        name="speakerhub",
        display_name="SpeakerHub",
        base_url="https://speakerhub.com",
        description="Global speaker marketplace with 25,000+ speakers. Rich profile data.",
        api_type="html_scrape",
        typical_roles=["Thought Leader", "Educator"],
        typical_niches=["speaking", "corporate_training", "executive_coaching"],
        typical_offerings=["speaking", "keynote", "workshops"],
        estimated_yield="15,000-25,000",
        rate_limit=8,
        notes="Profile pages at /speaker/slug. Search API may be available.",
    ),
    PotentialSource(
        name="speakerflow",
        display_name="SpeakerFlow CRM Directory",
        base_url="https://www.speakerflow.com",
        description="Speaker business management with public directory of coaches/speakers.",
        api_type="html_scrape",
        typical_roles=["Thought Leader", "Coach/Consultant"],
        typical_niches=["speaking", "business_coaching"],
        typical_offerings=["speaking", "coaching", "consulting"],
        estimated_yield="3,000-8,000",
        rate_limit=10,
    ),

    # --- Course creator platforms ---
    PotentialSource(
        name="teachable_discover",
        display_name="Teachable Discover",
        base_url="https://teachable.com/discover",
        description="Teachable's public course marketplace. Course creators with landing pages.",
        api_type="html_scrape",
        typical_roles=["Educator", "Product Creator"],
        typical_niches=["online_education", "digital_marketing", "health_wellness"],
        typical_offerings=["courses", "training", "education"],
        estimated_yield="10,000-30,000",
        rate_limit=8,
        notes="Category browsing at /discover/category. Each course has creator profile.",
    ),
    PotentialSource(
        name="thinkific_marketplace",
        display_name="Thinkific Marketplace",
        base_url="https://www.thinkific.com",
        description="Thinkific's partner/expert directory. Course creators and consultants.",
        api_type="html_scrape",
        typical_roles=["Educator", "Product Creator"],
        typical_niches=["online_education", "saas_software"],
        typical_offerings=["courses", "platform", "education"],
        estimated_yield="5,000-15,000",
        rate_limit=10,
    ),
    PotentialSource(
        name="skillshare_teachers",
        display_name="Skillshare Teachers",
        base_url="https://www.skillshare.com",
        description="Skillshare teacher profiles with class counts and student metrics.",
        api_type="html_scrape",
        typical_roles=["Educator", "Product Creator"],
        typical_niches=["online_education", "web_design", "photography"],
        typical_offerings=["courses", "tutorials", "creative education"],
        estimated_yield="20,000-50,000",
        rate_limit=6,
    ),

    # --- Coaching directories ---
    PotentialSource(
        name="noomii",
        display_name="Noomii Coach Directory",
        base_url="https://www.noomii.com",
        description="Large coaching directory with specialty search and full profiles.",
        api_type="html_scrape",
        typical_roles=["Coach/Consultant"],
        typical_niches=["life_coaching", "business_coaching", "career_coaching",
                        "health_wellness", "executive_coaching"],
        typical_offerings=["coaching", "consulting", "mentoring"],
        estimated_yield="10,000-20,000",
        rate_limit=8,
        notes="Category pages at /find-a-coach/specialty. Rich profile data with bios.",
    ),
    PotentialSource(
        name="psychology_today_coaches",
        display_name="Psychology Today Coach Directory",
        base_url="https://www.psychologytoday.com/us/coaches",
        description="Psychology Today's life/business coach directory. Trusted platform.",
        api_type="html_scrape",
        typical_roles=["Coach/Consultant"],
        typical_niches=["life_coaching", "mental_health", "relationship_coaching"],
        typical_offerings=["coaching", "therapy", "counseling"],
        estimated_yield="8,000-15,000",
        rate_limit=6,
        notes="State-based browsing. JSON-LD structured data on profile pages.",
    ),

    # --- Affiliate / JV specific ---
    PotentialSource(
        name="clickbank_marketplace",
        display_name="ClickBank Marketplace",
        base_url="https://www.clickbank.com/marketplace",
        description="Digital product marketplace with affiliate metrics. Product creators with JV history.",
        api_type="html_scrape",
        typical_roles=["Product Creator", "Affiliate/JV Partner"],
        typical_niches=["digital_marketing", "health_wellness", "financial_services"],
        typical_offerings=["digital products", "affiliate programs", "courses"],
        estimated_yield="20,000-50,000",
        rate_limit=8,
        notes="Category browsing. Gravity score indicates JV/affiliate activity.",
    ),
    PotentialSource(
        name="jvzoo_marketplace",
        display_name="JVZoo Marketplace",
        base_url="https://www.jvzoo.com",
        description="JV-focused digital product marketplace. Direct JV partner data.",
        api_type="html_scrape",
        typical_roles=["Product Creator", "Affiliate/JV Partner"],
        typical_niches=["digital_marketing", "saas_software", "online_education"],
        typical_offerings=["digital products", "software", "JV launches"],
        estimated_yield="10,000-30,000",
        rate_limit=6,
        notes="Product pages show vendor info and JV page links. High-value JV signals.",
    ),
    PotentialSource(
        name="warriorplus_marketplace",
        display_name="WarriorPlus Marketplace",
        base_url="https://warriorplus.com",
        description="Digital marketing product marketplace with vendor profiles.",
        api_type="html_scrape",
        typical_roles=["Product Creator", "Affiliate/JV Partner"],
        typical_niches=["digital_marketing", "seo", "email_marketing"],
        typical_offerings=["software tools", "courses", "marketing services"],
        estimated_yield="5,000-15,000",
        rate_limit=8,
    ),

    # --- Consulting / professional directories ---
    PotentialSource(
        name="growth_mentors",
        display_name="GrowthMentor",
        base_url="https://www.growthmentor.com",
        description="Startup mentor marketplace. Experienced business mentors with profiles.",
        api_type="html_scrape",
        typical_roles=["Coach/Consultant", "Thought Leader"],
        typical_niches=["business_coaching", "saas_software", "digital_marketing"],
        typical_offerings=["mentoring", "consulting", "strategy"],
        estimated_yield="3,000-8,000",
        rate_limit=10,
    ),
    PotentialSource(
        name="toptal_experts",
        display_name="Toptal Expert Directory",
        base_url="https://www.toptal.com",
        description="Top 3% freelance talent. Expert profiles with specializations.",
        api_type="html_scrape",
        typical_roles=["Service Provider", "Coach/Consultant"],
        typical_niches=["saas_software", "web_design", "ai_machine_learning"],
        typical_offerings=["development", "design", "consulting"],
        estimated_yield="10,000-20,000",
        rate_limit=6,
    ),

    # --- Real estate specific ---
    PotentialSource(
        name="bigger_pockets_members",
        display_name="BiggerPockets Members",
        base_url="https://www.biggerpockets.com",
        description="Real estate investing community. Public member profiles.",
        api_type="html_scrape",
        typical_roles=["Educator", "Coach/Consultant", "Community Builder"],
        typical_niches=["real_estate", "financial_services"],
        typical_offerings=["investing education", "deal analysis", "mentoring"],
        estimated_yield="20,000-50,000",
        rate_limit=6,
        notes="Member profiles at /users/. Forum data shows expertise and activity.",
    ),

    # --- Newsletter / Substack ---
    PotentialSource(
        name="substack_directory",
        display_name="Substack Directory",
        base_url="https://substack.com",
        description="Newsletter platform with public discovery. Writers with audiences.",
        api_type="json_api",
        typical_roles=["Media/Publisher", "Thought Leader"],
        typical_niches=["content_marketing", "digital_marketing", "financial_services"],
        typical_offerings=["newsletter", "content", "audience"],
        estimated_yield="30,000-80,000",
        rate_limit=10,
        notes="Discovery API at /api/v1/category. Each publication has subscriber signals.",
    ),

    # --- Health & Wellness ---
    PotentialSource(
        name="mindbody_instructors",
        display_name="Mindbody Instructor Directory",
        base_url="https://www.mindbody.io",
        description="Wellness business platform. Fitness/yoga/wellness instructors.",
        api_type="html_scrape",
        typical_roles=["Service Provider", "Educator"],
        typical_niches=["fitness", "nutrition", "health_wellness"],
        typical_offerings=["fitness classes", "wellness programs", "training"],
        estimated_yield="15,000-40,000",
        rate_limit=8,
    ),

    # --- Author / Book ---
    PotentialSource(
        name="amazon_author_central",
        display_name="Amazon Author Pages",
        base_url="https://www.amazon.com/stores",
        description="Amazon author pages with bios, book lists, and social links.",
        api_type="html_scrape",
        typical_roles=["Thought Leader", "Educator"],
        typical_niches=["author_publishing", "business_coaching", "life_coaching"],
        typical_offerings=["books", "speaking", "courses"],
        estimated_yield="50,000-100,000",
        rate_limit=4,
        notes="Author pages at /stores/author/ASIN. Rate limit carefully.",
    ),

    # --- Events / Summits ---
    PotentialSource(
        name="eventbrite_organizers",
        display_name="Eventbrite Organizers",
        base_url="https://www.eventbrite.com",
        description="Event platform with organizer profiles. People running workshops/summits.",
        api_type="json_api",
        auth_type="api_key",
        typical_roles=["Community Builder", "Educator"],
        typical_niches=["speaking", "corporate_training", "music_entertainment"],
        typical_offerings=["events", "workshops", "summits", "conferences"],
        estimated_yield="30,000-80,000",
        rate_limit=10,
        notes="API key needed. Organizer search by category and location.",
    ),

    # --- Community / Membership ---
    PotentialSource(
        name="circle_communities",
        display_name="Circle.so Communities",
        base_url="https://circle.so",
        description="Community platform with public discovery. Community leaders.",
        api_type="html_scrape",
        typical_roles=["Community Builder", "Educator"],
        typical_niches=["online_education", "saas_software", "digital_marketing"],
        typical_offerings=["community", "membership", "education"],
        estimated_yield="5,000-15,000",
        rate_limit=10,
    ),
    PotentialSource(
        name="mighty_networks_discover",
        display_name="Mighty Networks Discovery",
        base_url="https://www.mightynetworks.com",
        description="Community platform with public discovery. Community hosts with member counts.",
        api_type="html_scrape",
        typical_roles=["Community Builder", "Educator"],
        typical_niches=["online_education", "health_wellness", "business_coaching"],
        typical_offerings=["community", "courses", "membership"],
        estimated_yield="8,000-20,000",
        rate_limit=10,
    ),
]

# Build lookup for quick access
_SOURCE_REGISTRY: dict[str, PotentialSource] = {s.name: s for s in POTENTIAL_SOURCES}


# ---------------------------------------------------------------------------
# Gap analysis: find uncovered gaps
# ---------------------------------------------------------------------------

def find_uncovered_gaps(
    gap_data: dict,
    scraper_metadata: dict[str, dict],
) -> dict:
    """Identify gaps that no existing scraper covers.

    Parameters
    ----------
    gap_data : dict
        Snapshot data from market intelligence (supply_demand_gaps, role_gaps, niche_health).
    scraper_metadata : dict
        From _load_scraper_metadata() — existing scraper capabilities.

    Returns
    -------
    dict with:
        uncovered_keywords: keywords no scraper targets
        uncovered_roles: roles no scraper covers
        uncovered_niches: niches below health threshold with no scraper
    """
    # Collect all keywords/roles/niches covered by existing scrapers
    covered_offerings: set[str] = set()
    covered_roles: set[str] = set()
    covered_niches: set[str] = set()

    for name, meta in scraper_metadata.items():
        for o in meta.get("typical_offerings", []):
            covered_offerings.add(o.lower())
        for r in meta.get("typical_roles", []):
            covered_roles.add(r.lower())
        for n in meta.get("typical_niches", []):
            covered_niches.add(n.lower())

    # Find gap keywords not covered
    uncovered_keywords: list[dict] = []
    for gap in gap_data.get("supply_demand_gaps", []):
        if gap.get("gap_type") != "high_demand":
            continue
        kw = gap["keyword"].lower()
        is_covered = any(kw in o or o in kw for o in covered_offerings)
        if not is_covered:
            uncovered_keywords.append(gap)

    # Find missing roles not covered
    uncovered_roles: list[str] = []
    for rg in gap_data.get("role_gaps", []):
        for role in rg.get("missing_high_value_roles", []):
            role_lower = role.lower()
            is_covered = any(role_lower in r or r in role_lower for r in covered_roles)
            if not is_covered and role not in uncovered_roles:
                uncovered_roles.append(role)

    # Find underserved niches with no scraper
    uncovered_niches: list[dict] = []
    for nh in gap_data.get("niche_health", []):
        if nh.get("health_score", 100) >= 40:
            continue
        niche_lower = nh["niche"].lower()
        is_covered = any(niche_lower in n or n in niche_lower for n in covered_niches)
        if not is_covered:
            uncovered_niches.append(nh)

    return {
        "uncovered_keywords": uncovered_keywords,
        "uncovered_roles": uncovered_roles,
        "uncovered_niches": uncovered_niches,
    }


def recommend_sources(
    uncovered: dict,
    exclude_existing: set[str] = None,
) -> list[tuple[PotentialSource, float, list[str]]]:
    """Recommend potential sources that could fill uncovered gaps.

    Returns list of (source, score, gaps_targeted) sorted by score descending.
    """
    exclude = exclude_existing or set()

    gap_keywords = {g["keyword"].lower() for g in uncovered.get("uncovered_keywords", [])}
    missing_roles = {r.lower() for r in uncovered.get("uncovered_roles", [])}
    underserved = {n["niche"].lower() for n in uncovered.get("uncovered_niches", [])}

    recommendations: list[tuple[PotentialSource, float, list[str]]] = []

    for source in POTENTIAL_SOURCES:
        if source.name in exclude:
            continue

        score = source.matches_gap(gap_keywords, missing_roles, underserved)
        if score <= 0:
            continue

        # Track which gaps this source targets
        targeted: list[str] = []
        for kw in gap_keywords:
            for o in source.typical_offerings:
                if kw in o.lower() or o.lower() in kw:
                    targeted.append(f"demand:{kw}")
                    break
        for role in missing_roles:
            for r in source.typical_roles:
                if role in r.lower() or r.lower() in role:
                    targeted.append(f"role:{role}")
                    break
        for niche in underserved:
            for n in source.typical_niches:
                if niche in n.lower() or n.lower() in niche:
                    targeted.append(f"niche:{niche}")
                    break

        recommendations.append((source, score, sorted(set(targeted))))

    recommendations.sort(key=lambda x: x[1], reverse=True)
    return recommendations


# ---------------------------------------------------------------------------
# Scraper module generator
# ---------------------------------------------------------------------------

_SCRAPER_TEMPLATE = '''\
"""
{display_name} scraper — auto-generated scaffold.

{description}

Generated: {generated_at}
Estimated yield: {estimated_yield}
API type: {api_type}
Auth: {auth_type}

TODO: Review and customize this scaffold before running.
      Search patterns, parsing logic, and field mapping need
      manual verification against the actual site structure.
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


{search_queries_block}

class Scraper(BaseScraper):
    SOURCE_NAME = "{source_name}"
    BASE_URL = "{base_url}"
    REQUESTS_PER_MINUTE = {rate_limit}
    TYPICAL_ROLES = {typical_roles}
    TYPICAL_NICHES = {typical_niches}
    TYPICAL_OFFERINGS = {typical_offerings}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
{auth_init_block}
    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield URLs to scrape.

        TODO: Implement URL generation for {display_name}.
        Common patterns:
          - Category/search pages: for query in SEARCH_QUERIES: yield f"{{base}}/search?q={{query}}"
          - Sitemap: fetch sitemap.xml, yield profile URLs
          - ID range: for i in range(START, END): yield f"{{base}}/profile/{{i}}"
          - API pagination: for page in range(1, MAX): yield f"{{base}}/api?page={{page}}"
        """
{generate_urls_body}

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a page and return extracted contacts.

        TODO: Implement parsing for {display_name}.
        The html parameter contains the raw response text.
        For JSON APIs, use: data = json.loads(html)
        For HTML pages, use: soup = self.parse_html(html)
        """
{scrape_page_body}
'''


def generate_scraper_module(
    source_name: str,
    output_dir: Optional[str] = None,
    preview: bool = False,
) -> str:
    """Generate a scraper module file from the potential sources registry.

    Parameters
    ----------
    source_name : str
        Name from POTENTIAL_SOURCES registry.
    output_dir : str, optional
        Directory to write the file. Defaults to scripts/sourcing/scrapers/.
    preview : bool
        If True, return the code without writing to disk.

    Returns
    -------
    str : The generated Python source code.
    """
    if source_name not in _SOURCE_REGISTRY:
        available = sorted(_SOURCE_REGISTRY.keys())
        raise ValueError(
            f"Unknown source '{source_name}'. Available: {available}"
        )

    source = _SOURCE_REGISTRY[source_name]

    # Build search queries block (for HTML/API scrapers)
    search_queries_block = ""
    if source.api_type in ("html_scrape", "json_api"):
        queries = _generate_search_queries(source)
        if queries:
            lines = [f'    "{q}",' for q in queries]
            search_queries_block = (
                "# JV-relevant search queries\n"
                "SEARCH_QUERIES = [\n"
                + "\n".join(lines)
                + "\n]\n"
            )

    # Build auth init block
    auth_init_block = ""
    if source.auth_type == "api_key":
        env_var = f"{source_name.upper()}_API_KEY"
        auth_init_block = f"""\
        import os
        self._api_key = os.environ.get("{env_var}", "")
        if not self._api_key:
            self.logger.warning("{env_var} not set")
"""
    elif source.auth_type == "hmac":
        auth_init_block = """\
        self._api_key = None
        self._api_secret = None
"""

    # Build generate_urls body
    if source.api_type == "json_api":
        generate_urls_body = f"""\
        for query in SEARCH_QUERIES:
            # TODO: Adjust pagination and URL format
            for page in range(1, 50):
                yield f"{{self.BASE_URL}}/api/search?q={{query}}&page={{page}}"
"""
    elif source.api_type == "sitemap":
        generate_urls_body = f"""\
        # Fetch and parse sitemap
        sitemap_url = f"{{self.BASE_URL}}/sitemap.xml"
        html = self.fetch_page(sitemap_url)
        if html:
            # TODO: Parse sitemap XML for profile URLs
            import xml.etree.ElementTree as ET
            root = ET.fromstring(html)
            ns = {{"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}}
            for url_elem in root.findall(".//sm:url/sm:loc", ns):
                if url_elem.text and "/profile" in url_elem.text:
                    yield url_elem.text
"""
    else:  # html_scrape default
        generate_urls_body = f"""\
        for query in SEARCH_QUERIES:
            # TODO: Adjust URL format and pagination
            for page in range(1, 20):
                yield f"{{self.BASE_URL}}/search?q={{query}}&page={{page}}"
"""

    # Build scrape_page body
    if source.api_type == "json_api":
        scrape_page_body = _build_json_scrape_body(source)
    else:
        scrape_page_body = _build_html_scrape_body(source)

    code = _SCRAPER_TEMPLATE.format(
        display_name=source.display_name,
        description=source.description,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        estimated_yield=source.estimated_yield,
        api_type=source.api_type,
        auth_type=source.auth_type,
        search_queries_block=search_queries_block,
        source_name=source.name,
        base_url=source.base_url,
        rate_limit=source.rate_limit,
        typical_roles=repr(source.typical_roles),
        typical_niches=repr(source.typical_niches),
        typical_offerings=repr(source.typical_offerings),
        auth_init_block=auth_init_block,
        generate_urls_body=generate_urls_body,
        scrape_page_body=scrape_page_body,
    )

    if preview:
        return code

    # Write to file
    if output_dir is None:
        output_dir = str(Path(__file__).parent / "scrapers")

    filepath = os.path.join(output_dir, f"{source_name}.py")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(code)

    return code


def _generate_search_queries(source: PotentialSource) -> list[str]:
    """Generate JV-relevant search queries based on source niches/offerings."""
    queries = []

    niche_queries = {
        "podcasting": ["business podcast", "coaching podcast", "entrepreneurship podcast"],
        "speaking": ["keynote speaker", "motivational speaker", "business speaker"],
        "life_coaching": ["life coach", "personal development", "transformation"],
        "business_coaching": ["business coach", "business consultant", "startup advisor"],
        "executive_coaching": ["executive coach", "leadership development"],
        "health_wellness": ["health coach", "wellness coach", "holistic health"],
        "digital_marketing": ["digital marketing", "marketing strategy", "growth marketing"],
        "online_education": ["online course", "course creator", "teaching online"],
        "real_estate": ["real estate investing", "property management"],
        "financial_services": ["financial advisor", "wealth management", "investing"],
        "saas_software": ["SaaS", "software startup", "tech company"],
        "content_marketing": ["content creator", "copywriting", "blogging"],
        "fitness": ["fitness coach", "personal trainer", "yoga instructor"],
        "nutrition": ["nutritionist", "dietitian", "meal planning"],
        "career_coaching": ["career coach", "career development", "job search"],
        "relationship_coaching": ["relationship coach", "dating coach", "couples coaching"],
        "author_publishing": ["author", "book launch", "self-publishing"],
        "ai_machine_learning": ["AI startup", "machine learning", "artificial intelligence"],
    }

    for niche in source.typical_niches:
        queries.extend(niche_queries.get(niche, [niche.replace("_", " ")]))

    return queries[:30]  # Cap at 30 queries


def _build_json_scrape_body(source: PotentialSource) -> str:
    """Generate scrape_page body for JSON API sources."""
    return '''\
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        # TODO: Adjust the data path based on actual API response structure
        items = data.get("results", data.get("data", data.get("items", [])))
        if isinstance(items, dict):
            items = items.get("hits", items.get("results", []))

        for item in items:
            # TODO: Map fields to actual API response keys
            name = (item.get("name") or item.get("title") or "").strip()
            if not name or len(name) < 2:
                continue

            item_id = str(item.get("id", ""))
            if item_id and item_id in self._seen_ids:
                continue
            if item_id:
                self._seen_ids.add(item_id)

            email = (item.get("email") or "").strip()
            website = (item.get("website") or item.get("url") or "").strip()
            company = (item.get("company") or item.get("organization") or "").strip()
            bio = (item.get("bio") or item.get("description") or "").strip()

            contacts.append(ScrapedContact(
                name=name,
                email=email,
                company=company,
                website=website,
                bio=bio[:2000] if bio else "",
                source_category="{category}",
                raw_data=item,
            ))

        return contacts
'''.format(category=source.typical_niches[0] if source.typical_niches else "general")


def _build_html_scrape_body(source: PotentialSource) -> str:
    """Generate scrape_page body for HTML scraping sources."""
    return '''\
        soup = self.parse_html(html)
        contacts = []

        # TODO: Adjust selector based on actual page structure
        # Common patterns:
        #   - Card grid: soup.select(".profile-card")
        #   - List items: soup.select("li.result-item")
        #   - Table rows: soup.select("table.results tr")
        items = soup.select(".profile-card, .result-item, .listing")

        for item in items:
            # TODO: Map selectors to actual HTML structure
            name_el = item.select_one("h2, h3, .name, .title")
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or len(name) < 2:
                continue

            # Extract link
            link_el = item.select_one("a[href]")
            profile_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    profile_url = f"{{self.BASE_URL}}{{href}}"
                elif href.startswith("http"):
                    profile_url = href

            # Extract other fields
            bio_el = item.select_one(".bio, .description, .summary, p")
            bio = bio_el.get_text(strip=True) if bio_el else ""

            website = ""
            email = ""
            # Check for email in text
            text = item.get_text()
            emails = self.extract_emails(text)
            if emails:
                email = emails[0]

            contacts.append(ScrapedContact(
                name=name,
                email=email,
                website=website or profile_url,
                bio=bio[:2000] if bio else "",
                source_category="{category}",
            ))

        return contacts
'''.format(category=source.typical_niches[0] if source.typical_niches else "general")


# ---------------------------------------------------------------------------
# Report integration: generate recommendations for gap report
# ---------------------------------------------------------------------------

def generate_scraper_recommendations(
    gap_data: dict,
    scraper_metadata: dict[str, dict],
    existing_scraper_names: set[str],
    max_recommendations: int = 5,
) -> list[dict]:
    """Generate scraper recommendations for inclusion in gap reports.

    Called by market_gaps.py to add a "recommended_new_scrapers" section
    to the gap analysis report.

    Returns list of dicts with source info and gap coverage.
    """
    uncovered = find_uncovered_gaps(gap_data, scraper_metadata)
    recommendations = recommend_sources(uncovered, exclude_existing=existing_scraper_names)

    result = []
    for source, score, gaps_targeted in recommendations[:max_recommendations]:
        result.append({
            "source_name": source.name,
            "display_name": source.display_name,
            "base_url": source.base_url,
            "api_type": source.api_type,
            "auth_type": source.auth_type,
            "estimated_yield": source.estimated_yield,
            "gap_score": score,
            "gaps_targeted": gaps_targeted,
            "typical_roles": source.typical_roles,
            "typical_niches": source.typical_niches,
            "generate_command": (
                f"python3 -m scripts.sourcing.scraper_generator "
                f"--source {source.name} --generate"
            ),
            "ai_create_command": (
                f"python3 -m scripts.sourcing.scraper_generator "
                f"--source {source.name} --ai-create"
            ),
        })

    return result


# ---------------------------------------------------------------------------
# AI-powered scraper creation
# ---------------------------------------------------------------------------

# Reference scraper shown to the LLM as a pattern example
_REFERENCE_SCRAPER = '''\
"""Example scraper following BaseScraper pattern."""
from __future__ import annotations
import json, re
from typing import Iterator
from scripts.sourcing.base import BaseScraper, ScrapedContact

SEARCH_QUERIES = ["business coaching", "marketing consultant"]

class Scraper(BaseScraper):
    SOURCE_NAME = "example_directory"
    BASE_URL = "https://example.com"
    REQUESTS_PER_MINUTE = 8
    TYPICAL_ROLES = ["Coach/Consultant"]
    TYPICAL_NICHES = ["business_coaching"]
    TYPICAL_OFFERINGS = ["coaching", "consulting"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        for query in SEARCH_QUERIES:
            for page in range(1, 50):
                yield f"{self.BASE_URL}/search?q={query}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        soup = self.parse_html(html)
        contacts = []
        for card in soup.select(".profile-card"):
            name = card.select_one("h3").get_text(strip=True) if card.select_one("h3") else ""
            if not name or len(name) < 2:
                continue
            link = card.select_one("a[href]")
            website = link["href"] if link else ""
            bio_el = card.select_one(".description")
            bio = bio_el.get_text(strip=True)[:2000] if bio_el else ""
            email = ""
            emails = self.extract_emails(card.get_text())
            if emails:
                email = emails[0]
            contacts.append(ScrapedContact(
                name=name, email=email, website=website, bio=bio,
                source_category="business_coaching",
            ))
        return contacts
'''


def ai_create_scraper(
    source_name: str,
    output_dir: Optional[str] = None,
    dry_run: bool = False,
    max_sample_pages: int = 3,
) -> dict:
    """Use AI to create a fully working scraper by analyzing a target site.

    Process:
      1. Fetch the target site's listing/search pages
      2. Send the HTML + reference scraper to Claude
      3. Claude generates a complete scraper with real selectors
      4. Validate by running a quick smoke test (3 pages)
      5. Write to disk if validation passes

    Parameters
    ----------
    source_name : str
        Name from POTENTIAL_SOURCES registry.
    output_dir : str, optional
        Defaults to scripts/sourcing/scrapers/.
    dry_run : bool
        If True, generate but don't write to disk.
    max_sample_pages : int
        Number of pages to fetch for analysis (default 3).

    Returns
    -------
    dict with: success, scraper_path, contacts_found, errors
    """
    import logging
    logger = logging.getLogger("scraper_generator.ai")

    if source_name not in _SOURCE_REGISTRY:
        return {"success": False, "error": f"Unknown source: {source_name}"}

    source = _SOURCE_REGISTRY[source_name]

    # ── Step 1: Fetch sample pages from the target site ───────────────
    logger.info("Fetching sample pages from %s", source.base_url)
    sample_html = _fetch_sample_pages(source, max_pages=max_sample_pages)

    if not sample_html:
        return {
            "success": False,
            "error": f"Could not fetch any pages from {source.base_url}",
        }

    logger.info("Fetched %d sample pages (%d chars total)",
                len(sample_html),
                sum(len(h) for h in sample_html.values()))

    # ── Step 2: Send to Claude for scraper generation ─────────────────
    logger.info("Sending to Claude for scraper generation...")
    generated_code = _call_ai_for_scraper(source, sample_html)

    if not generated_code:
        # Fall back to template generation
        logger.warning("AI generation failed, falling back to template scaffold")
        generated_code = generate_scraper_module(source_name, preview=True)
        return {
            "success": True,
            "method": "template_fallback",
            "scraper_code": generated_code,
            "contacts_found": 0,
            "error": "AI generation failed; template scaffold used instead",
        }

    # ── Step 3: Validate the generated code ───────────────────────────
    validation = _validate_generated_scraper(generated_code, source_name)

    if not validation["valid"]:
        logger.warning("Generated scraper failed validation: %s", validation["error"])
        # Try to fix common issues and regenerate
        generated_code = _fix_common_issues(generated_code, source)
        validation = _validate_generated_scraper(generated_code, source_name)

    # ── Step 4: Write to disk ─────────────────────────────────────────
    if output_dir is None:
        output_dir = str(Path(__file__).parent / "scrapers")

    filepath = os.path.join(output_dir, f"{source_name}.py")

    if not dry_run and validation["valid"]:
        os.makedirs(output_dir, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(generated_code)
        logger.info("Wrote scraper to %s", filepath)

    # ── Step 5: Smoke test (optional, only if written) ────────────────
    contacts_found = 0
    smoke_test_error = None
    if not dry_run and validation["valid"]:
        try:
            contacts_found = _smoke_test_scraper(source_name, max_pages=2)
            logger.info("Smoke test: found %d contacts", contacts_found)
        except Exception as exc:
            smoke_test_error = str(exc)
            logger.warning("Smoke test failed: %s", exc)

    return {
        "success": validation["valid"],
        "method": "ai_generated",
        "scraper_path": filepath if not dry_run else None,
        "contacts_found": contacts_found,
        "validation": validation,
        "smoke_test_error": smoke_test_error,
        "source": {
            "name": source.name,
            "display_name": source.display_name,
            "base_url": source.base_url,
        },
    }


def _fetch_sample_pages(source: PotentialSource, max_pages: int = 3) -> dict[str, str]:
    """Fetch a few pages from the target site for analysis.

    Returns dict of url -> html content.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    pages: dict[str, str] = {}

    # URLs to try: homepage, search page, category listing
    urls_to_try = [source.base_url]

    # Add likely search/listing URLs based on api_type
    base = source.base_url.rstrip("/")
    if source.api_type == "html_scrape":
        urls_to_try.extend([
            f"{base}/search?q={source.typical_niches[0].replace('_', '+')}" if source.typical_niches else base,
            f"{base}/directory",
            f"{base}/browse",
            f"{base}/explore",
        ])
    elif source.api_type == "json_api":
        urls_to_try.extend([
            f"{base}/api/search",
            f"{base}/api/v1/search",
        ])

    for url in urls_to_try[:max_pages + 2]:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 500:
                # Truncate very large pages to save tokens
                content = resp.text[:15000]
                pages[url] = content
                if len(pages) >= max_pages:
                    break
        except Exception:
            continue

    return pages


def _call_ai_for_scraper(
    source: PotentialSource,
    sample_html: dict[str, str],
) -> Optional[str]:
    """Call Claude to generate a scraper module from sample HTML."""
    try:
        from matching.enrichment.claude_client import ClaudeClient
    except ImportError:
        # Try direct API call without Django
        return _call_ai_direct(source, sample_html)

    client = ClaudeClient(max_tokens=4096)
    if not client.is_available():
        return _call_ai_direct(source, sample_html)

    prompt = _build_ai_prompt(source, sample_html)

    try:
        response = client.call(prompt)
        if response:
            return _extract_code_from_response(response)
    except Exception:
        pass

    return None


def _call_ai_direct(
    source: PotentialSource,
    sample_html: dict[str, str],
) -> Optional[str]:
    """Direct API call without Django/ClaudeClient."""
    api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    prompt = _build_ai_prompt(source, sample_html)

    try:
        import httpx

        use_openrouter = bool(os.environ.get("OPENROUTER_API_KEY"))

        if use_openrouter:
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "anthropic/claude-sonnet-4",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4096,
                "temperature": 0,
            }
        else:
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}],
            }

        resp = httpx.post(url, json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        if use_openrouter:
            text = data["choices"][0]["message"]["content"]
        else:
            text = data["content"][0]["text"]

        return _extract_code_from_response(text)

    except Exception:
        return None


def _build_ai_prompt(
    source: PotentialSource,
    sample_html: dict[str, str],
) -> str:
    """Build the prompt for Claude to generate a scraper."""
    # Prepare truncated HTML samples
    html_sections = []
    for url, html in list(sample_html.items())[:3]:
        # Further truncate for prompt size
        truncated = html[:8000]
        html_sections.append(f"### URL: {url}\n```html\n{truncated}\n```")

    html_block = "\n\n".join(html_sections)

    return f"""You are an expert Python developer building a web scraper.

## Task
Generate a complete, working Python scraper module for **{source.display_name}** ({source.base_url}).

The scraper MUST follow the BaseScraper pattern exactly. Here is a reference example:

```python
{_REFERENCE_SCRAPER}
```

## Target Site Info
- **Name:** {source.display_name}
- **URL:** {source.base_url}
- **API type:** {source.api_type}
- **Auth:** {source.auth_type}
- **Description:** {source.description}
- **Notes:** {source.notes}

## Sample HTML from the site
{html_block}

## Requirements
1. The module MUST define a `class Scraper(BaseScraper)` with these exact attributes:
   - SOURCE_NAME = "{source.name}"
   - BASE_URL = "{source.base_url}"
   - REQUESTS_PER_MINUTE = {source.rate_limit}
   - TYPICAL_ROLES = {repr(source.typical_roles)}
   - TYPICAL_NICHES = {repr(source.typical_niches)}
   - TYPICAL_OFFERINGS = {repr(source.typical_offerings)}

2. `__init__` MUST accept `**kwargs` and call `super().__init__(**kwargs)`
3. `generate_urls` MUST yield real URLs based on the site structure you see in the HTML
4. `scrape_page` MUST parse the HTML and return `list[ScrapedContact]`
5. ScrapedContact fields are strings — NEVER pass None, use "" instead
6. Use `self.parse_html(html)` for BeautifulSoup, `self.extract_emails(text)` for emails
7. Include deduplication via `self._seen_ids`
8. For JSON APIs: parse with `json.loads(html)` since fetch_page returns text
9. Import only from: `scripts.sourcing.base` (BaseScraper, ScrapedContact), stdlib (json, re, etc.)

## Output
Return ONLY the complete Python module code. No explanation, no markdown fences — just the raw Python code starting with the docstring.
"""


def _extract_code_from_response(response: str) -> Optional[str]:
    """Extract Python code from an AI response, handling markdown fences."""
    if not response:
        return None

    # Try to extract from markdown code block
    code_match = re.search(r'```python\s*\n(.*?)```', response, re.DOTALL)
    if code_match:
        return code_match.group(1).strip()

    # Try generic code block
    code_match = re.search(r'```\s*\n(.*?)```', response, re.DOTALL)
    if code_match:
        return code_match.group(1).strip()

    # If response starts with docstring or import, it's likely raw code
    stripped = response.strip()
    if stripped.startswith('"""') or stripped.startswith("from ") or stripped.startswith("import "):
        return stripped

    return None


def _validate_generated_scraper(code: str, source_name: str) -> dict:
    """Validate that generated code is syntactically valid and follows the pattern."""
    result = {"valid": False, "error": None, "warnings": []}

    # Syntax check
    try:
        compile(code, f"{source_name}.py", "exec")
    except SyntaxError as e:
        result["error"] = f"Syntax error: {e}"
        return result

    # Pattern checks
    if "class Scraper(BaseScraper)" not in code:
        result["error"] = "Missing 'class Scraper(BaseScraper)'"
        return result

    if "def generate_urls" not in code:
        result["error"] = "Missing generate_urls method"
        return result

    if "def scrape_page" not in code:
        result["error"] = "Missing scrape_page method"
        return result

    if "ScrapedContact(" not in code:
        result["error"] = "Missing ScrapedContact instantiation"
        return result

    if "def __init__" not in code:
        result["warnings"].append("Missing __init__ (will use BaseScraper default)")

    if f'SOURCE_NAME = "{source_name}"' not in code:
        result["warnings"].append(f"SOURCE_NAME doesn't match '{source_name}'")

    result["valid"] = True
    return result


def _fix_common_issues(code: str, source: PotentialSource) -> str:
    """Attempt to fix common AI generation issues."""
    # Fix missing imports
    if "from scripts.sourcing.base import" not in code:
        code = "from scripts.sourcing.base import BaseScraper, ScrapedContact\n\n" + code

    # Fix None being passed to ScrapedContact string fields
    code = code.replace("=None,", '="",')
    code = code.replace("= None,", '= "",')

    return code


def _smoke_test_scraper(source_name: str, max_pages: int = 2) -> int:
    """Run the scraper for a few pages to verify it works.

    Returns number of valid contacts found.
    """
    from scripts.sourcing.runner import _register_scrapers, SCRAPER_REGISTRY
    from scripts.sourcing.rate_limiter import RateLimiter

    # Re-register to pick up the new scraper
    _register_scrapers()

    if source_name not in SCRAPER_REGISTRY:
        raise RuntimeError(f"Scraper '{source_name}' not found in registry after generation")

    rate_limiter = RateLimiter()
    scraper_cls = SCRAPER_REGISTRY[source_name]
    scraper = scraper_cls(rate_limiter=rate_limiter)

    contacts_found = 0
    for contact in scraper.run(max_pages=max_pages, max_contacts=50):
        contacts_found += 1

    return contacts_found


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate scraper modules for uncovered market gaps"
    )
    parser.add_argument(
        "--source", help="Source name to generate scraper for"
    )
    parser.add_argument(
        "--generate", action="store_true",
        help="Write the scraper file to disk"
    )
    parser.add_argument(
        "--preview", action="store_true",
        help="Print the generated code without writing"
    )
    parser.add_argument(
        "--list-sources", action="store_true",
        help="List all potential sources in the registry"
    )
    parser.add_argument(
        "--analyze-gaps", action="store_true",
        help="Analyze latest gap data and recommend new scrapers"
    )
    parser.add_argument(
        "--ai-create", action="store_true",
        help="Use AI to generate a fully working scraper (requires --source)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate but don't write to disk (used with --ai-create)"
    )

    args = parser.parse_args()

    if args.list_sources:
        print(f"\n{'=' * 70}")
        print("POTENTIAL DATA SOURCES REGISTRY")
        print(f"{'=' * 70}\n")
        for source in POTENTIAL_SOURCES:
            print(f"  {source.name:30s}  {source.api_type:12s}  est: {source.estimated_yield}")
            print(f"    {source.display_name} — {source.description[:80]}")
            print(f"    Roles: {', '.join(source.typical_roles)}")
            print(f"    Niches: {', '.join(source.typical_niches[:4])}")
            print()
        print(f"Total: {len(POTENTIAL_SOURCES)} potential sources\n")
        return

    if args.source and args.ai_create:
        # AI-powered scraper creation
        import logging
        logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
        print(f"\nAI-generating scraper for: {args.source}")
        print(f"{'=' * 50}")
        result = ai_create_scraper(
            args.source,
            dry_run=args.dry_run,
        )
        if result.get("success"):
            print(f"\nSuccess! Method: {result.get('method', 'unknown')}")
            if result.get("scraper_path"):
                print(f"Written to: {result['scraper_path']}")
            if result.get("contacts_found"):
                print(f"Smoke test: {result['contacts_found']} contacts found")
            if result.get("smoke_test_error"):
                print(f"Smoke test warning: {result['smoke_test_error']}")
            if result.get("validation", {}).get("warnings"):
                for w in result["validation"]["warnings"]:
                    print(f"  Warning: {w}")
            if not args.dry_run:
                print(f"\nNext steps:")
                print(f"  1. Review: cat scripts/sourcing/scrapers/{args.source}.py")
                print(f"  2. Test: python3 -m scripts.sourcing.runner --source {args.source} --max-pages 5")
        else:
            print(f"\nFailed: {result.get('error', 'unknown error')}")
        return

    if args.source:
        try:
            code = generate_scraper_module(
                args.source,
                preview=not args.generate,
            )
            if args.generate:
                scraper_dir = Path(__file__).parent / "scrapers"
                filepath = scraper_dir / f"{args.source}.py"
                print(f"Generated: {filepath}")
                print(f"Next steps:")
                print(f"  1. Review and customize the generated scraper")
                print(f"  2. Test: python3 -m scripts.sourcing.runner --source {args.source} --dry-run --max-pages 3")
                print(f"  3. Run: python3 -m scripts.sourcing.runner --source {args.source} --export-csv ...")
            else:
                print(code)
        except ValueError as e:
            print(f"ERROR: {e}")
        return

    if args.analyze_gaps:
        _analyze_and_recommend()
        return

    parser.print_help()


def _analyze_and_recommend():
    """Load latest gap data and recommend new scrapers."""
    import json as json_mod

    # Try to load latest gap report
    report_dir = Path(__file__).parent.parent.parent / "reports" / "market_intelligence"
    report_file = report_dir / "gap_report.json"

    if not report_file.exists():
        print("No gap report found. Run: python3 manage.py compute_market_intelligence")
        print("Or provide gap data manually.")
        return

    with open(report_file) as f:
        gap_data = json_mod.load(f)

    # Load existing scraper metadata
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from scripts.sourcing.runner import _register_scrapers, SCRAPER_REGISTRY

    _register_scrapers()
    existing_names = set(SCRAPER_REGISTRY.keys())

    scraper_metadata = {}
    for name, cls in SCRAPER_REGISTRY.items():
        scraper_metadata[name] = {
            "typical_roles": getattr(cls, "TYPICAL_ROLES", []),
            "typical_niches": getattr(cls, "TYPICAL_NICHES", []),
            "typical_offerings": getattr(cls, "TYPICAL_OFFERINGS", []),
        }

    # Find uncovered gaps
    uncovered = find_uncovered_gaps(gap_data, scraper_metadata)
    recommendations = recommend_sources(uncovered, exclude_existing=existing_names)

    print(f"\n{'=' * 70}")
    print("UNCOVERED GAP ANALYSIS")
    print(f"{'=' * 70}\n")

    if uncovered["uncovered_keywords"]:
        print(f"  Demand keywords with no scraper coverage:")
        for g in uncovered["uncovered_keywords"][:10]:
            print(f"    - \"{g['keyword']}\" (seeking={g.get('seeking_count', '?')}, "
                  f"offering={g.get('offering_count', '?')})")
    else:
        print("  All demand keywords have scraper coverage.")

    if uncovered["uncovered_roles"]:
        print(f"\n  Missing roles with no scraper:")
        for r in uncovered["uncovered_roles"][:10]:
            print(f"    - {r}")

    if uncovered["uncovered_niches"]:
        print(f"\n  Underserved niches (health < 40) with no scraper:")
        for n in uncovered["uncovered_niches"][:10]:
            print(f"    - {n['niche']} (health={n.get('health_score', '?')})")

    if recommendations:
        print(f"\n{'=' * 70}")
        print("RECOMMENDED NEW SCRAPERS")
        print(f"{'=' * 70}\n")
        for source, score, gaps in recommendations[:8]:
            print(f"  {source.name:30s}  score={score:.1f}")
            print(f"    {source.display_name} — {source.description[:70]}")
            print(f"    Estimated yield: {source.estimated_yield}")
            print(f"    Gaps targeted: {', '.join(gaps[:5])}")
            print(f"    Scaffold: python3 -m scripts.sourcing.scraper_generator "
                  f"--source {source.name} --generate")
            print(f"    AI-build: python3 -m scripts.sourcing.scraper_generator "
                  f"--source {source.name} --ai-create")
            print()
    else:
        print("\n  No recommended sources match the uncovered gaps.")

    print()


if __name__ == "__main__":
    main()
