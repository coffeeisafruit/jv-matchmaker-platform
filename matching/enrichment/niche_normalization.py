"""
Niche normalization: maps free-text niche values to canonical categories.

The ``niche`` field in profiles is free text, producing thousands of unique
values ("life coaching", "life coach", "coaching for life transitions").
This module normalizes them to a controlled vocabulary of ~120 canonical
categories so frequency analysis is meaningful.

Approach:
  1. Exact alias lookup (fast, like _ROLE_ALIASES in services.py)
  2. Substring containment check against canonical keywords
  3. Fallback to "other" for truly unmappable values

Usage:
    from matching.enrichment.niche_normalization import normalize_niche

    normalize_niche("life coaching")         # → "life_coaching"
    normalize_niche("Executive Life Coach")  # → "life_coaching"
    normalize_niche("B2B SaaS Marketing")    # → "saas_software"
"""

from __future__ import annotations

import re
from typing import Optional


# ---------------------------------------------------------------------------
# Canonical niche vocabulary (~120 categories)
# Organized by broad sector for maintainability
# ---------------------------------------------------------------------------

CANONICAL_NICHES: dict[str, list[str]] = {
    # --- Coaching & Personal Development ---
    "life_coaching": [
        "life coach", "life coaching", "personal coaching",
        "transformation coach", "transformational coaching",
        "mindset coach", "mindset coaching", "personal growth",
        "personal transformation", "self improvement",
        "self-improvement", "self development", "self-development",
    ],
    "business_coaching": [
        "business coach", "business coaching", "business strategy coach",
        "business growth", "business consultant", "business consulting",
        "business development", "small business coaching",
        "entrepreneurship coaching", "startup coaching",
    ],
    "executive_coaching": [
        "executive coach", "executive coaching", "leadership coaching",
        "leadership coach", "leadership development", "c-suite coaching",
        "executive development", "leadership training",
    ],
    "health_wellness": [
        "health coach", "health coaching", "wellness coach",
        "wellness coaching", "health and wellness", "holistic health",
        "holistic coaching", "integrative health", "wellbeing",
        "well-being", "functional health", "health optimization",
        "biohacking",
    ],
    "fitness": [
        "fitness", "fitness coach", "fitness coaching",
        "personal trainer", "personal training", "gym",
        "strength training", "bodybuilding", "crossfit",
        "yoga", "pilates", "sports training",
    ],
    "nutrition": [
        "nutrition", "nutritionist", "nutrition coach",
        "dietitian", "diet", "meal planning", "weight loss",
        "weight management", "keto", "plant-based",
    ],
    "mental_health": [
        "mental health", "therapy", "therapist", "counseling",
        "psychology", "psychologist", "anxiety", "depression",
        "mindfulness", "meditation", "stress management",
        "emotional intelligence", "emotional wellness",
    ],
    "relationship_coaching": [
        "relationship coach", "relationship coaching", "dating coach",
        "marriage coaching", "couples coaching", "love coach",
        "relationship advice",
    ],
    "career_coaching": [
        "career coach", "career coaching", "career development",
        "career transition", "job search", "resume",
        "career change", "professional development",
    ],
    "spiritual": [
        "spiritual", "spirituality", "spiritual coaching",
        "faith-based", "ministry", "church", "christian coaching",
        "energy healing", "reiki", "chakra",
    ],

    # --- Marketing & Sales ---
    "digital_marketing": [
        "digital marketing", "online marketing", "internet marketing",
        "marketing agency", "marketing consultant",
        "marketing strategy", "growth marketing",
        "performance marketing", "marketing automation",
    ],
    "social_media": [
        "social media", "social media marketing", "social media management",
        "instagram", "tiktok", "facebook marketing",
        "influencer marketing", "content creator",
    ],
    "content_marketing": [
        "content marketing", "content strategy", "content creation",
        "blogging", "copywriting", "copywriter",
    ],
    "email_marketing": [
        "email marketing", "email list", "newsletter",
        "email automation", "drip campaigns",
    ],
    "seo": [
        "seo", "search engine optimization", "search marketing",
        "sem", "google ads", "ppc", "paid search", "organic search",
    ],
    "sales": [
        "sales", "sales training", "sales coaching", "sales strategy",
        "b2b sales", "closing", "sales funnel", "pipeline",
    ],
    "branding": [
        "branding", "brand strategy", "personal branding",
        "brand identity", "brand consulting", "visual identity",
    ],
    "advertising": [
        "advertising", "ad agency", "media buying",
        "facebook ads", "google ads agency", "paid media",
    ],
    "public_relations": [
        "public relations", "pr", "press", "media relations",
        "publicity", "media coverage", "press release",
    ],
    "affiliate_marketing": [
        "affiliate marketing", "affiliate", "affiliate program",
        "referral marketing", "partner marketing",
    ],

    # --- Technology & Software ---
    "saas_software": [
        "saas", "software", "software development", "app development",
        "mobile app", "web development", "web app",
        "software company", "tech startup", "b2b saas",
    ],
    "ai_machine_learning": [
        "artificial intelligence", "ai", "machine learning",
        "deep learning", "data science", "ai consulting",
        "ai automation", "chatbot", "generative ai",
    ],
    "cybersecurity": [
        "cybersecurity", "cyber security", "information security",
        "infosec", "network security", "data security",
    ],
    "cloud_infrastructure": [
        "cloud", "cloud computing", "aws", "azure",
        "devops", "infrastructure", "hosting",
    ],
    "web_design": [
        "web design", "website design", "ux design", "ui design",
        "ux/ui", "user experience", "graphic design",
        "website development",
    ],
    "ecommerce": [
        "ecommerce", "e-commerce", "online store", "shopify",
        "amazon fba", "dropshipping", "online retail",
        "woocommerce", "etsy",
    ],

    # --- Finance & Business ---
    "financial_services": [
        "financial", "finance", "financial planning",
        "financial advisor", "wealth management", "investment",
        "investing", "financial coaching", "money coach",
    ],
    "accounting": [
        "accounting", "accountant", "bookkeeping", "cpa",
        "tax", "tax preparation", "tax planning",
    ],
    "insurance": [
        "insurance", "insurance agent", "insurance broker",
        "life insurance", "health insurance",
    ],
    "real_estate": [
        "real estate", "realtor", "real estate investing",
        "property", "property management", "real estate agent",
        "mortgage", "home buying",
    ],
    "cryptocurrency": [
        "cryptocurrency", "crypto", "bitcoin", "blockchain",
        "defi", "web3", "nft",
    ],

    # --- Education & Training ---
    "online_education": [
        "online education", "online course", "course creator",
        "e-learning", "edtech", "online training",
        "course creation", "online school",
    ],
    "corporate_training": [
        "corporate training", "corporate education",
        "employee training", "workforce development",
        "organizational development", "team building",
    ],
    "higher_education": [
        "higher education", "university", "college",
        "academic", "professor", "research",
    ],
    "speaking": [
        "public speaking", "keynote speaker", "motivational speaker",
        "speaker", "speaking", "conference speaker",
        "professional speaker", "speaking coach",
    ],
    "author_publishing": [
        "author", "publishing", "book", "self-publishing",
        "writer", "writing", "book publishing",
        "bestselling author",
    ],

    # --- Media & Content ---
    "podcasting": [
        "podcast", "podcasting", "podcast host", "podcaster",
        "podcast production", "audio content",
    ],
    "video_production": [
        "video", "video production", "youtube", "youtuber",
        "video marketing", "video editing", "filmmaker",
    ],
    "photography": [
        "photography", "photographer", "photo", "headshots",
        "portrait photography", "commercial photography",
    ],
    "music_entertainment": [
        "music", "musician", "entertainment", "events",
        "event planning", "event management", "dj",
    ],

    # --- Professional Services ---
    "legal": [
        "legal", "lawyer", "attorney", "law firm",
        "legal services", "legal consulting",
    ],
    "hr_recruiting": [
        "human resources", "hr", "recruiting", "recruitment",
        "talent acquisition", "staffing", "headhunter",
    ],
    "consulting": [
        "consulting", "management consulting", "strategy consulting",
        "consultant", "advisory",
    ],
    "project_management": [
        "project management", "program management",
        "agile", "scrum", "pmp",
    ],

    # --- Industry Verticals ---
    "healthcare": [
        "healthcare", "medical", "hospital", "clinic",
        "telehealth", "health tech", "pharma", "pharmaceutical",
        "biotech", "medical device",
    ],
    "construction": [
        "construction", "contractor", "building", "architecture",
        "architect", "engineering", "civil engineering",
    ],
    "manufacturing": [
        "manufacturing", "factory", "industrial", "supply chain",
        "logistics", "warehouse", "distribution",
    ],
    "food_beverage": [
        "food", "restaurant", "catering", "food service",
        "beverage", "bakery", "chef", "culinary",
    ],
    "automotive": [
        "automotive", "auto", "car", "vehicle",
        "auto repair", "dealership",
    ],
    "agriculture": [
        "agriculture", "farming", "farm", "agribusiness",
        "organic farming",
    ],
    "energy_environment": [
        "energy", "renewable energy", "solar", "wind",
        "environmental", "sustainability", "green energy",
        "clean energy", "climate",
    ],
    "nonprofit": [
        "nonprofit", "non-profit", "ngo", "charity",
        "philanthropic", "social impact", "social enterprise",
        "community organization",
    ],
    "government_defense": [
        "government", "federal", "defense", "military",
        "public sector", "civic", "municipal",
    ],
    "travel_hospitality": [
        "travel", "tourism", "hospitality", "hotel",
        "vacation", "adventure", "retreat",
    ],
    "beauty_fashion": [
        "beauty", "fashion", "skincare", "cosmetics",
        "hair", "salon", "spa", "aesthetics",
    ],
    "parenting_family": [
        "parenting", "family", "motherhood", "fatherhood",
        "child development", "homeschool", "kids",
    ],
    "pet_animal": [
        "pet", "veterinary", "animal", "dog training",
        "pet care", "equine",
    ],
}


# ---------------------------------------------------------------------------
# Build reverse lookup: alias → canonical key
# ---------------------------------------------------------------------------

_ALIAS_TO_CANONICAL: dict[str, str] = {}
for canonical, aliases in CANONICAL_NICHES.items():
    _ALIAS_TO_CANONICAL[canonical] = canonical  # Self-reference
    for alias in aliases:
        _ALIAS_TO_CANONICAL[alias.lower().strip()] = canonical

# Niche values that should be treated as "no niche" (not mapped)
NICHE_BLOCKLIST = frozenset({
    "", "host", "other", "misc", "none", "n/a", "unknown",
    "general", "various", "multiple", "all", "na", "null",
    "not specified", "unspecified", "tbd",
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_niche(raw_niche: str) -> Optional[str]:
    """Normalize a free-text niche to a canonical category.

    Returns the canonical key (e.g., "life_coaching") or None if the niche
    is in the blocklist or truly unmappable.

    Matching strategy:
      1. Exact alias lookup (case-insensitive)
      2. Substring containment (longest match wins)
      3. None for unmappable values
    """
    if not raw_niche or not isinstance(raw_niche, str):
        return None

    cleaned = raw_niche.lower().strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)

    if cleaned in NICHE_BLOCKLIST:
        return None

    # Strategy 1: Exact alias match
    if cleaned in _ALIAS_TO_CANONICAL:
        return _ALIAS_TO_CANONICAL[cleaned]

    # Strategy 2: Substring containment (try longer aliases first for precision)
    best_match: Optional[str] = None
    best_len = 0
    for alias, canonical in _ALIAS_TO_CANONICAL.items():
        if len(alias) > 3 and alias in cleaned and len(alias) > best_len:
            best_match = canonical
            best_len = len(alias)

    if best_match:
        return best_match

    # Strategy 3: Check if any canonical keyword appears in the raw text
    for canonical, aliases in CANONICAL_NICHES.items():
        # Check the canonical key itself (e.g., "real_estate" → "real estate")
        key_words = canonical.replace("_", " ")
        if key_words in cleaned:
            return canonical

    return None


def get_canonical_vocabulary() -> list[str]:
    """Return sorted list of all canonical niche categories."""
    return sorted(CANONICAL_NICHES.keys())


def get_unmapped_niches(niche_values: list[str]) -> list[tuple[str, int]]:
    """Find niche values that don't map to any canonical category.

    Returns (niche_text, count) tuples sorted by count descending.
    Useful for expanding the vocabulary over time.
    """
    from collections import Counter

    unmapped: list[str] = []
    for niche in niche_values:
        if niche and normalize_niche(niche) is None:
            cleaned = niche.lower().strip()
            if cleaned and cleaned not in NICHE_BLOCKLIST:
                unmapped.append(cleaned)

    return Counter(unmapped).most_common()
