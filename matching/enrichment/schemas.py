"""
Pydantic output schemas for the enrichment pipeline.

Used by Pydantic AI agents as structured output types, replacing
manual JSON parsing from ai_research.py and ai_verification.py.

Schema groups:
  - Profile extraction (CoreProfileExtraction, ExtendedSignalsExtraction)
  - Verification (VerificationResult, AIVerificationResult, ClaimVerification)
  - Match enrichment (EnrichedMatch, LLMExplanation)
  - Acquisition pipeline (ProspectQualification, DiscoveryResult, MatchGap)
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ────────────────────────────────────────────────────────────────
# Shared enums and sub-models
# ────────────────────────────────────────────────────────────────

class Confidence(str, enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class RevenueTier(str, enum.Enum):
    MICRO = "micro"
    EMERGING = "emerging"
    ESTABLISHED = "established"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"
    UNKNOWN = ""


class BusinessSize(str, enum.Enum):
    SOLO = "solo"
    SMALL_TEAM = "small_team"
    MEDIUM = "medium"
    LARGE = "large"
    UNKNOWN = ""


class JVPartnership(BaseModel):
    """A single JV partnership or collaboration entry."""
    partner_name: str = Field(description="Name of the partner or organization")
    format: str = Field(
        description="Type: podcast_guest, summit_speaker, bundle, affiliate, "
                     "co_author, webinar_guest, or endorsement"
    )
    source_quote: str = Field(default="", description="Direct quote from the website supporting this")


class ContentPlatforms(BaseModel):
    """Named content platforms the person operates."""
    podcast_name: str = Field(default="", description="Name of their podcast show")
    youtube_channel: str = Field(default="", description="YouTube channel name")
    instagram_handle: str = Field(default="", description="Instagram handle")
    facebook_group: str = Field(default="", description="Facebook group name")
    tiktok_handle: str = Field(default="", description="TikTok handle")
    newsletter_name: str = Field(default="", description="Newsletter name")


# ────────────────────────────────────────────────────────────────
# Profile extraction schemas (Prompt 1 + 2)
# ────────────────────────────────────────────────────────────────

class CoreProfileExtraction(BaseModel):
    """Output schema for the core profile research agent (Prompt 1).

    Maps 1:1 to the 17 fields extracted from website content.
    """
    what_you_do: str = Field(default="", description="Primary business/service, 1-2 sentences")
    who_you_serve: str = Field(default="", description="Target audience, 1 sentence")
    seeking: str = Field(default="", description="What they're actively looking for (partnerships, speaking, etc.)")
    offering: str = Field(default="", description="What they offer to partners/collaborators")
    social_proof: str = Field(default="", description="Notable credentials, bestseller status, certifications")
    signature_programs: str = Field(default="", description="Named courses, books, frameworks, certifications")
    booking_link: str = Field(default="", description="Calendar booking URL (Calendly, Acuity, etc.)")
    niche: str = Field(default="", description="Primary market niche, 1-3 words")
    phone: str = Field(default="", description="Business phone number if publicly displayed")
    current_projects: str = Field(default="", description="Active launches or programs being promoted")
    company: str = Field(default="", description="Company or business name")
    list_size: Optional[int] = Field(default=None, description="Email list or audience size as integer")
    business_size: BusinessSize = Field(default=BusinessSize.UNKNOWN, description="Business scale: solo, small_team, medium, large")
    tags: List[str] = Field(default_factory=list, description="3-7 keyword tags, lowercase")
    audience_type: str = Field(default="", description="Audience category: B2B, B2C, coaches, entrepreneurs, etc.")
    business_focus: str = Field(default="", description="Primary business focus in 1 sentence")
    service_provided: str = Field(default="", description="Comma-separated list of services offered")
    confidence: Confidence = Field(default=Confidence.LOW, description="Extraction confidence: high, medium, low")
    source_quotes: List[str] = Field(default_factory=list, description="1-2 direct quotes supporting the extraction")

    @field_validator("tags")
    @classmethod
    def normalize_tags(cls, v: List[str]) -> List[str]:
        return [t.lower().strip() for t in v[:7] if t.strip()]

    @field_validator("list_size", mode="before")
    @classmethod
    def coerce_list_size(cls, v):
        if v is None or v == "" or v == "null":
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None


class ExtendedSignalsExtraction(BaseModel):
    """Output schema for the extended signals agent (Prompt 2).

    Revenue tier, JV history, content platforms, engagement signals.
    """
    revenue_tier: RevenueTier = Field(default=RevenueTier.UNKNOWN, description="Pricing level classification")
    revenue_signals: List[str] = Field(default_factory=list, description="Price mentions found on the site")
    jv_history: List[JVPartnership] = Field(default_factory=list, description="Known partnerships and collaborations")
    content_platforms: ContentPlatforms = Field(default_factory=ContentPlatforms, description="Named content platforms")
    audience_engagement_signals: str = Field(default="", description="Evidence of active audience engagement")
    confidence: Confidence = Field(default=Confidence.LOW)
    source_quotes: List[str] = Field(default_factory=list)


# ────────────────────────────────────────────────────────────────
# Verification schemas
# ────────────────────────────────────────────────────────────────

class VerificationStatus(str, enum.Enum):
    PASSED = "passed"
    NEEDS_ENRICHMENT = "needs_enrichment"
    REJECTED = "rejected"


class AIVerificationResult(BaseModel):
    """Result from a single AI-powered verification check."""
    passed: bool
    score: float = Field(ge=0, le=100, description="Quality score 0-100")
    issues: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    reasoning: str = Field(default="")


class VerificationResult(BaseModel):
    """Aggregate verification result across all checks."""
    status: VerificationStatus
    score: float = Field(ge=0, le=100)
    issues: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)


class VerificationIssue(BaseModel):
    """A single issue found by a verification agent."""
    agent: str = Field(description="Which verification agent found this")
    severity: str = Field(description="critical, warning, or info")
    issue: str
    suggestion: str = Field(default="")
    location: str = Field(default="", description="Field name where issue was found")


class ClaimCheck(BaseModel):
    """A single claim verification entry."""
    claim: str
    status: str = Field(description="grounded, inferred, or ungrounded")
    source_field: str = Field(default="")
    note: str = Field(default="")


class ClaimVerification(BaseModel):
    """Output from the LLM explanation grounding check."""
    claims: List[ClaimCheck] = Field(default_factory=list)
    grounded_percentage: float = Field(ge=0, le=1, description="0.0-1.0 fraction of grounded claims")
    recommendation: str = Field(
        description="use_as_is, remove_ungrounded, or fall_back_to_template"
    )


# ────────────────────────────────────────────────────────────────
# Match enrichment schemas
# ────────────────────────────────────────────────────────────────

class PartnerValue(BaseModel):
    """What one partner brings to the other."""
    summary: str = Field(description="2-3 sentence summary")
    key_points: List[str] = Field(default_factory=list)


class ConnectionInsight(BaseModel):
    """A synergy insight between two partners."""
    type: str = Field(description="obvious or non_obvious")
    insight: str


class ReciprocityAssessment(BaseModel):
    """Balance assessment of a partnership."""
    balance: str = Field(description="balanced, slightly_asymmetric, or significantly_asymmetric")
    stronger_side: str = Field(description="partner_a, partner_b, or neither")
    explanation: str
    gap: Optional[str] = Field(default=None, description="What's missing if asymmetric")


class LLMExplanation(BaseModel):
    """Output from the match explanation generation agent."""
    what_partner_b_brings_to_a: PartnerValue
    what_partner_a_brings_to_b: PartnerValue
    connection_insights: List[ConnectionInsight] = Field(default_factory=list)
    reciprocity_assessment: ReciprocityAssessment
    citations: Dict[str, str] = Field(default_factory=dict, description="claim_text → source_field_name")
    confidence: Dict[str, str] = Field(
        default_factory=dict,
        description="data_richness and explanation_confidence, each high/medium/low"
    )


class EnrichedMatch(BaseModel):
    """A match with full profile data and compelling reasoning.

    Replaces the dataclass in match_enrichment.py.
    """
    # Identity
    name: str
    company: str = ""
    email: str = ""
    linkedin: str = ""
    website: str = ""
    niche: str = ""

    # Metrics
    list_size: int = 0
    social_reach: int = 0
    score: float = 0.0

    # Rich profile data
    who_they_serve: str = ""
    what_they_do: str = ""
    seeking: str = ""
    offering: str = ""
    notes: str = ""

    # Generated content
    why_fit: str = ""
    mutual_benefit: str = ""
    outreach_message: str = ""

    # Verification
    verification_score: float = 0.0
    verification_passed: bool = False

    # Data quality tracking
    data_quality: str = "unknown"
    has_explicit_seeking: bool = False
    has_explicit_offering: bool = False
    explanation_source: str = "template_fallback"


# ────────────────────────────────────────────────────────────────
# Acquisition pipeline schemas
# ────────────────────────────────────────────────────────────────

class ProspectQualification(BaseModel):
    """Result of qualifying a discovered prospect for JV potential."""
    is_jv_candidate: bool = Field(description="Whether this prospect is worth pursuing")
    confidence: Confidence = Field(default=Confidence.LOW)
    reasoning: str = Field(default="", description="Why this prospect qualifies or doesn't")
    estimated_match_quality: float = Field(
        default=0.0, ge=0, le=100,
        description="Estimated ISMC score before full enrichment"
    )


class DiscoveredProspect(BaseModel):
    """A single prospect found during acquisition."""
    name: str
    website: str = ""
    email: str = ""
    linkedin: str = ""
    niche: str = ""
    what_you_do: str = ""
    source_tool: str = Field(description="exa_websets, exa_search, serper, apollo, duckduckgo")
    source_query: str = Field(default="", description="The query that found this prospect")
    pre_score: Optional[float] = Field(default=None, description="ISMC pre-filter score if computed")


class DiscoveryResult(BaseModel):
    """Output from the acquisition discovery agent."""
    prospects: List[DiscoveredProspect] = Field(default_factory=list)
    queries_used: List[str] = Field(default_factory=list, description="Search queries executed")
    tools_used: List[str] = Field(default_factory=list, description="Which search tools were used")
    total_cost: float = Field(default=0.0, description="Estimated cost of this discovery run")


class MatchGap(BaseModel):
    """Gap analysis for a client's match quality."""
    client_id: str
    client_name: str = ""
    current_match_count: int = Field(default=0, description="Number of matches scoring 70+")
    target_match_count: int = Field(default=10)
    gap: int = Field(default=0, description="How many more 70+ matches are needed")
    best_scores: List[float] = Field(default_factory=list, description="Top match scores currently")
    lowest_qualifying_score: Optional[float] = Field(
        default=None, description="Lowest score in the current top 10"
    )
    suggested_query: str = Field(
        default="",
        description="Suggested search query for acquisition based on client needs"
    )
