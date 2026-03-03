"""
Translate client profiles + gap analysis into Apollo People Search queries.

Takes the rich text fields from a client profile (seeking, niche, who_you_serve,
revenue_tier) and the structured output from gap detection (niche_gaps, top_niches)
and produces 2-3 complementary Apollo search parameter dicts that can be passed
directly to ``ApolloEnrichmentService.search_people(**query)``.

Usage:
    from matching.enrichment.apollo_query_builder import build_apollo_queries

    queries = build_apollo_queries(client_profile, gap_analysis)
    for q in queries:
        results = apollo_svc.search_people(**q, max_results=25)
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Niche → Apollo industry mapping
# Apollo uses LinkedIn-style industry tags. We map our top canonical niches.
# ---------------------------------------------------------------------------

NICHE_TO_APOLLO_INDUSTRY: dict[str, list[str]] = {
    # Coaching & Personal Development
    "life_coaching": ["Professional Training & Coaching"],
    "business_coaching": ["Professional Training & Coaching", "Management Consulting"],
    "executive_coaching": ["Professional Training & Coaching", "Executive Office"],
    "health_wellness": ["Health, Wellness & Fitness", "Alternative Medicine"],
    "fitness": ["Health, Wellness & Fitness", "Sports"],
    "nutrition": ["Health, Wellness & Fitness", "Food & Beverages"],
    "mental_health": ["Mental Health Care", "Hospital & Health Care"],
    "relationship_coaching": ["Professional Training & Coaching"],
    "career_coaching": ["Professional Training & Coaching", "Staffing & Recruiting"],
    "spiritual": ["Religious Institutions", "Alternative Medicine"],
    # Marketing & Sales
    "digital_marketing": ["Marketing & Advertising", "Online Media"],
    "social_media": ["Marketing & Advertising", "Online Media"],
    "content_marketing": ["Marketing & Advertising", "Writing & Editing"],
    "email_marketing": ["Marketing & Advertising"],
    "seo": ["Marketing & Advertising", "Internet"],
    "sales": ["Professional Training & Coaching", "Management Consulting"],
    "branding": ["Marketing & Advertising", "Design"],
    "advertising": ["Marketing & Advertising"],
    "public_relations": ["Public Relations & Communications"],
    "affiliate_marketing": ["Marketing & Advertising"],
    # Technology & Software
    "saas_software": ["Computer Software", "Internet", "Information Technology & Services"],
    "ai_machine_learning": ["Computer Software", "Information Technology & Services"],
    "cybersecurity": ["Computer & Network Security", "Information Technology & Services"],
    "cloud_infrastructure": ["Information Technology & Services", "Computer Software"],
    "web_design": ["Design", "Internet"],
    "ecommerce": ["Retail", "Internet", "Consumer Goods"],
    # Finance & Business
    "financial_services": ["Financial Services", "Investment Management"],
    "accounting": ["Accounting", "Financial Services"],
    "insurance": ["Insurance"],
    "real_estate": ["Real Estate", "Commercial Real Estate"],
    "cryptocurrency": ["Financial Services", "Internet"],
    # Education & Training
    "online_education": ["E-Learning", "Education Management"],
    "corporate_training": ["Professional Training & Coaching", "Human Resources"],
    "higher_education": ["Higher Education", "Education Management"],
    "speaking": ["Professional Training & Coaching", "Events Services"],
    "author_publishing": ["Publishing", "Writing & Editing"],
    # Media & Content
    "podcasting": ["Online Media", "Broadcast Media"],
    "video_production": ["Online Media", "Media Production"],
    # Healthcare
    "healthcare": ["Hospital & Health Care", "Medical Practice"],
    "alternative_medicine": ["Alternative Medicine", "Health, Wellness & Fitness"],
    # Legal & Consulting
    "legal": ["Law Practice", "Legal Services"],
    "management_consulting": ["Management Consulting"],
    "hr_consulting": ["Human Resources", "Management Consulting"],
    # Non-profit
    "nonprofit": ["Nonprofit Organization Management", "Philanthropy"],
}

# ---------------------------------------------------------------------------
# Revenue tier → Apollo employee range mapping (reverse of apollo_enrichment.py)
# ---------------------------------------------------------------------------

REVENUE_TIER_TO_EMPLOYEE_RANGES: dict[str, list[str]] = {
    "micro": ["1,10", "11,20"],
    "emerging": ["11,20", "21,50"],
    "established": ["51,200", "201,500"],
    "premium": ["201,500", "501,1000"],
    "enterprise": ["1001,5000", "5001,10000"],
}

# ---------------------------------------------------------------------------
# Keywords that signal JV-relevant roles when found in ``seeking`` text
# ---------------------------------------------------------------------------

_ROLE_KEYWORDS: dict[str, list[str]] = {
    "Podcast Host": [
        "podcast", "podcaster", "show host", "audio",
    ],
    "Coach": [
        "coach", "coaching", "mentor", "mentoring",
    ],
    "Course Creator": [
        "course", "program creator", "online program", "membership",
    ],
    "Speaker": [
        "speaker", "keynote", "speaking", "stage",
    ],
    "Author": [
        "author", "book", "writer", "publishing",
    ],
    "Consultant": [
        "consultant", "consulting", "advisor", "advisory",
    ],
    "Agency Owner": [
        "agency", "agency owner", "firm",
    ],
    "Influencer": [
        "influencer", "content creator", "social media",
    ],
    "Community Leader": [
        "community", "community builder", "group leader", "network",
    ],
    "Affiliate Manager": [
        "affiliate", "jv partner", "joint venture", "referral",
    ],
}

# Default seniorities for JV discovery — people who can say yes to a partnership
_JV_SENIORITIES = ["owner", "founder", "c_suite", "director"]


def _extract_titles_from_seeking(seeking: str) -> list[str]:
    """Extract Apollo-compatible job titles from a client's ``seeking`` field.

    Scans the free-text seeking field for keywords that map to specific
    professional roles suitable for Apollo's person_titles filter.
    """
    if not seeking:
        return []

    seeking_lower = seeking.lower()
    titles: list[str] = []

    for title, keywords in _ROLE_KEYWORDS.items():
        for kw in keywords:
            if kw in seeking_lower:
                titles.append(title)
                break  # one match per title is enough

    return titles


def _extract_keywords_from_text(text: str, max_keywords: int = 3) -> str:
    """Extract meaningful keywords from free text for Apollo q_keywords.

    Strips common filler words and returns the most distinctive terms.
    """
    if not text:
        return ""

    stop_words = {
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "can", "i", "we", "you", "they",
        "my", "our", "your", "their", "who", "that", "this", "these", "those",
        "looking", "seeking", "want", "need", "find", "help", "people",
        "partners", "partner", "someone", "anyone", "work", "working",
    }

    words = re.findall(r"[a-zA-Z]+", text.lower())
    meaningful = [w for w in words if w not in stop_words and len(w) > 2]

    # Return top keywords by position (earlier = more important)
    unique: list[str] = []
    for w in meaningful:
        if w not in unique:
            unique.append(w)
        if len(unique) >= max_keywords:
            break

    return " ".join(unique)


def build_apollo_queries(
    client_profile: dict,
    gap_analysis: dict,
    max_queries: int = 3,
) -> list[dict]:
    """Build 2-3 complementary Apollo People Search query parameter dicts.

    Each returned dict can be unpacked into ``search_people(**query)``.

    Query strategy:
      1. **Primary** — titles + industry from client's ``seeking`` + ``niche``
      2. **Audience match** — keywords from ``who_you_serve`` to find people
         targeting the same audience
      3. **Gap fill** — target underrepresented niches from ``niche_gaps``

    Parameters
    ----------
    client_profile:
        Profile dict with niche, seeking, who_you_serve, revenue_tier, etc.
    gap_analysis:
        Output from detect_match_gaps — needs niche_gaps, top_niches.
    max_queries:
        Cap on number of queries to generate (default 3).

    Returns
    -------
    list[dict]
        Each dict has keys matching ``search_people()`` parameters:
        title, industry, person_seniorities, q_keywords, company_size, etc.
    """
    queries: list[dict] = []

    niche = (client_profile.get("niche") or "").strip()
    seeking = (client_profile.get("seeking") or "").strip()
    who_you_serve = (client_profile.get("who_you_serve") or "").strip()
    revenue_tier = (client_profile.get("revenue_tier") or "").strip().lower()
    niche_gaps = gap_analysis.get("niche_gaps") or []

    # Resolve industry from niche
    industries = NICHE_TO_APOLLO_INDUSTRY.get(niche, [])
    industry = industries[0] if industries else ""

    # Resolve company size from revenue tier
    employee_ranges = REVENUE_TIER_TO_EMPLOYEE_RANGES.get(revenue_tier, [])
    company_size = employee_ranges[0] if employee_ranges else ""

    # ---------------------------------------------------------------
    # Query 1: Primary — titles from seeking + industry from niche
    # ---------------------------------------------------------------
    titles = _extract_titles_from_seeking(seeking)
    if titles or industry or niche:
        q1: dict = {
            "person_seniorities": _JV_SENIORITIES,
        }
        if titles:
            q1["title"] = ", ".join(titles)
        if industry:
            q1["industry"] = industry
        if company_size:
            q1["company_size"] = company_size
        # Add niche as keyword fallback if no titles extracted
        if not titles and niche:
            q1["q_keywords"] = niche.replace("_", " ")

        queries.append(q1)
        logger.info(
            "Apollo query 1 (primary): titles=%s industry=%s",
            titles, industry,
        )

    # ---------------------------------------------------------------
    # Query 2: Audience match — find people serving the same audience
    # ---------------------------------------------------------------
    if who_you_serve and len(queries) < max_queries:
        audience_keywords = _extract_keywords_from_text(who_you_serve, max_keywords=4)
        if audience_keywords:
            q2: dict = {
                "q_keywords": audience_keywords,
                "person_seniorities": _JV_SENIORITIES,
            }
            if company_size:
                q2["company_size"] = company_size
            queries.append(q2)
            logger.info(
                "Apollo query 2 (audience): keywords='%s'", audience_keywords,
            )

    # ---------------------------------------------------------------
    # Query 3: Gap fill — target underrepresented niches
    # ---------------------------------------------------------------
    if niche_gaps and len(queries) < max_queries:
        # Pick the first gap niche that maps to an Apollo industry
        for gap_niche in niche_gaps[:3]:
            gap_industries = NICHE_TO_APOLLO_INDUSTRY.get(gap_niche, [])
            if gap_industries:
                q3: dict = {
                    "industry": gap_industries[0],
                    "person_seniorities": _JV_SENIORITIES,
                    "q_keywords": gap_niche.replace("_", " "),
                }
                if company_size:
                    q3["company_size"] = company_size
                queries.append(q3)
                logger.info(
                    "Apollo query 3 (gap fill): niche=%s industry=%s",
                    gap_niche, gap_industries[0],
                )
                break

    # Fallback: if no queries could be built, try a keyword-only search
    if not queries:
        fallback_text = seeking or who_you_serve or niche
        if fallback_text:
            queries.append({
                "q_keywords": _extract_keywords_from_text(fallback_text, max_keywords=5),
                "person_seniorities": _JV_SENIORITIES,
            })
            logger.info("Apollo fallback query: keywords from profile text")

    logger.info("Built %d Apollo discovery queries for client '%s'",
                len(queries), client_profile.get("name", "?"))
    return queries[:max_queries]
