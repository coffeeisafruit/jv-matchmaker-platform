"""
Tests for Pydantic enrichment schemas.

Verification item #2: Schema tests -- Pydantic models validate against
saved LLM responses.  Uses simulated realistic data (the kind of JSON
Claude would actually return from enrichment prompts).
"""

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.test_settings")

import pytest
from pydantic import ValidationError

from matching.enrichment.schemas import (
    AIVerificationResult,
    BusinessSize,
    ClaimCheck,
    ClaimVerification,
    Confidence,
    ConnectionInsight,
    ContentPlatforms,
    CoreProfileExtraction,
    DiscoveredProspect,
    DiscoveryResult,
    ExtendedSignalsExtraction,
    JVPartnership,
    LLMExplanation,
    MatchGap,
    PartnerValue,
    ProspectQualification,
    ReciprocityAssessment,
    RevenueTier,
    VerificationResult,
    VerificationStatus,
)


# ────────────────────────────────────────────────────────────────
# CoreProfileExtraction
# ────────────────────────────────────────────────────────────────


class TestCoreProfileExtraction:
    """Validates the primary profile extraction schema used by Prompt 1."""

    def test_full_realistic_llm_response(self):
        """A fully-populated response that mirrors real Claude output."""
        data = {
            "what_you_do": "Sarah runs a boutique leadership coaching firm helping mid-career women executives break through to the C-suite.",
            "who_you_serve": "Women in senior management roles (VP/Director level) at Fortune 500 companies looking to advance to C-suite positions.",
            "seeking": "Speaking engagements at women's leadership conferences, podcast guest spots, JV partnerships with executive recruiters.",
            "offering": "Audience of 12,000 ambitious executive women, proven conversion for high-ticket offers, bestselling book credibility.",
            "social_proof": "Wall Street Journal bestselling author, former McKinsey partner, 200+ executive clients placed in C-suite roles.",
            "signature_programs": "The Executive Edge Accelerator (12-week program), Boardroom Ready Mastermind, 'Lead Without Limits' (book).",
            "booking_link": "https://calendly.com/sarah-chen-coaching/strategy",
            "niche": "executive women leadership",
            "phone": "(415) 555-0192",
            "current_projects": "Launching 'Boardroom Ready 2.0' mastermind in Q3; promoting keynote tour for 'Lead Without Limits'.",
            "company": "Sarah Chen Leadership Group",
            "list_size": 12000,
            "business_size": "small_team",
            "tags": ["Leadership", "Executive Coaching", "Women in Business", "C-Suite", "Keynote Speaker"],
            "audience_type": "B2B, corporate executives, ambitious women leaders",
            "business_focus": "High-ticket executive coaching and leadership development for women breaking into C-suite.",
            "service_provided": "1:1 executive coaching, group mastermind, keynote speaking, corporate workshops",
            "confidence": "high",
            "source_quotes": [
                "I help ambitious women leaders shatter the glass ceiling and claim their seat at the table.",
                "Join 200+ executives who've made the leap to the C-suite with our proven framework.",
            ],
        }
        profile = CoreProfileExtraction(**data)
        assert profile.what_you_do.startswith("Sarah runs")
        assert profile.list_size == 12000
        assert profile.business_size == BusinessSize.SMALL_TEAM
        assert profile.confidence == Confidence.HIGH
        assert len(profile.tags) == 5
        assert all(t == t.lower() for t in profile.tags)  # normalised
        assert len(profile.source_quotes) == 2

    def test_minimal_fields_with_defaults(self):
        """LLM sometimes returns mostly empty strings when a site is thin."""
        profile = CoreProfileExtraction()
        assert profile.what_you_do == ""
        assert profile.who_you_serve == ""
        assert profile.list_size is None
        assert profile.tags == []
        assert profile.business_size == BusinessSize.UNKNOWN
        assert profile.confidence == Confidence.LOW
        assert profile.source_quotes == []

    def test_normalize_tags_lowercases_and_limits(self):
        """normalize_tags must lowercase, strip, and cap at 7 entries."""
        data = {
            "tags": [
                "  Leadership ",
                "COACHING",
                "Women",
                "Keynote",
                "Podcasting",
                "Masterminds",
                "Books",
                "This-Should-Be-Dropped",
                "Also-Dropped",
            ]
        }
        profile = CoreProfileExtraction(**data)
        assert len(profile.tags) == 7
        assert profile.tags[0] == "leadership"
        assert profile.tags[1] == "coaching"
        assert "this-should-be-dropped" not in profile.tags

    def test_normalize_tags_strips_blank_entries(self):
        """Whitespace-only tags should be filtered out."""
        profile = CoreProfileExtraction(tags=["valid", "  ", "", "also_valid"])
        assert profile.tags == ["valid", "also_valid"]

    def test_coerce_list_size_from_int(self):
        """Pass-through when the LLM already returns a proper integer."""
        profile = CoreProfileExtraction(list_size=5000)
        assert profile.list_size == 5000

    def test_coerce_list_size_from_string(self):
        """LLMs sometimes quote numbers as strings like '5000'."""
        profile = CoreProfileExtraction(list_size="5000")
        assert profile.list_size == 5000

    def test_coerce_list_size_none(self):
        """Explicit None should stay None."""
        profile = CoreProfileExtraction(list_size=None)
        assert profile.list_size is None

    def test_coerce_list_size_empty_string(self):
        """Empty string (common LLM default) coerces to None."""
        profile = CoreProfileExtraction(list_size="")
        assert profile.list_size is None

    def test_coerce_list_size_null_string(self):
        """The literal string 'null' coerces to None."""
        profile = CoreProfileExtraction(list_size="null")
        assert profile.list_size is None

    def test_coerce_list_size_unparseable(self):
        """Non-numeric strings (e.g. 'unknown') coerce to None."""
        profile = CoreProfileExtraction(list_size="large audience")
        assert profile.list_size is None


# ────────────────────────────────────────────────────────────────
# ExtendedSignalsExtraction
# ────────────────────────────────────────────────────────────────


class TestExtendedSignalsExtraction:
    """Validates the extended signals schema used by Prompt 2."""

    def test_full_response_with_jv_partnerships(self):
        """Realistic Prompt 2 output with JV history and revenue tier."""
        data = {
            "revenue_tier": "established",
            "revenue_signals": [
                "$997 for Executive Edge Accelerator",
                "$5,000/month mastermind membership",
                "Corporate workshops starting at $15,000",
            ],
            "jv_history": [
                {
                    "partner_name": "John Maxwell Team",
                    "format": "summit_speaker",
                    "source_quote": "Sarah was our featured speaker at the 2024 Global Leadership Summit.",
                },
                {
                    "partner_name": "Amy Porterfield",
                    "format": "podcast_guest",
                    "source_quote": "Catch my interview on Online Marketing Made Easy, episode 412.",
                },
            ],
            "content_platforms": {
                "podcast_name": "The Executive Edge Podcast",
                "youtube_channel": "Sarah Chen Leadership",
                "instagram_handle": "@sarahchenleads",
                "facebook_group": "Women Leading Forward",
                "tiktok_handle": "",
                "newsletter_name": "The Leadership Briefing",
            },
            "audience_engagement_signals": "Active Facebook group with 8,500 members; weekly live Q&A sessions; 45% email open rate mentioned in media kit.",
            "confidence": "high",
            "source_quotes": [
                "Join our community of 8,500+ women leaders in the Women Leading Forward group.",
            ],
        }
        ext = ExtendedSignalsExtraction(**data)
        assert ext.revenue_tier == RevenueTier.ESTABLISHED
        assert len(ext.jv_history) == 2
        assert ext.jv_history[0].partner_name == "John Maxwell Team"
        assert ext.jv_history[1].format == "podcast_guest"
        assert ext.content_platforms.podcast_name == "The Executive Edge Podcast"
        assert ext.content_platforms.tiktok_handle == ""
        assert ext.confidence == Confidence.HIGH
        assert len(ext.revenue_signals) == 3

    def test_empty_defaults(self):
        """LLM returns an empty object when no signals are found."""
        ext = ExtendedSignalsExtraction()
        assert ext.revenue_tier == RevenueTier.UNKNOWN
        assert ext.jv_history == []
        assert ext.content_platforms.podcast_name == ""
        assert ext.audience_engagement_signals == ""
        assert ext.confidence == Confidence.LOW
        assert ext.source_quotes == []
        assert ext.revenue_signals == []


# ────────────────────────────────────────────────────────────────
# AIVerificationResult
# ────────────────────────────────────────────────────────────────


class TestAIVerificationResult:
    """Validates the verification score schema."""

    def test_valid_score_boundaries(self):
        """Scores at 0 and 100 are both valid."""
        low = AIVerificationResult(passed=False, score=0)
        assert low.score == 0
        high = AIVerificationResult(passed=True, score=100)
        assert high.score == 100

    def test_realistic_verification(self):
        """A typical mid-range verification result."""
        result = AIVerificationResult(
            passed=True,
            score=78.5,
            issues=["Booking link returns 404"],
            suggestions=["Verify booking link is still active"],
            reasoning="Profile data is well-supported but booking link is stale.",
        )
        assert result.passed is True
        assert result.score == 78.5
        assert len(result.issues) == 1
        assert result.reasoning.startswith("Profile data")

    def test_rejects_score_above_100(self):
        """Score > 100 must raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            AIVerificationResult(passed=True, score=101)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("score",) for e in errors)

    def test_rejects_score_below_0(self):
        """Score < 0 must raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            AIVerificationResult(passed=False, score=-1)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("score",) for e in errors)

    def test_defaults_for_optional_fields(self):
        """Only passed and score are required; rest should default."""
        result = AIVerificationResult(passed=True, score=85)
        assert result.issues == []
        assert result.suggestions == []
        assert result.reasoning == ""


# ────────────────────────────────────────────────────────────────
# LLMExplanation
# ────────────────────────────────────────────────────────────────


class TestLLMExplanation:
    """Validates the match explanation generation schema."""

    def test_full_partner_value_and_reciprocity(self):
        """Realistic LLM explanation with full partner value assessment."""
        data = {
            "what_partner_b_brings_to_a": {
                "summary": "Amy's massive podcast audience (20M+ downloads) gives Sarah direct access to entrepreneurial women who are ready to invest in executive coaching.",
                "key_points": [
                    "20M+ podcast downloads with overlapping audience demographics",
                    "Proven affiliate conversion rates above 3%",
                    "Credibility transfer from a trusted online marketing educator",
                ],
            },
            "what_partner_a_brings_to_b": {
                "summary": "Sarah's Fortune 500 network and Wall Street Journal bestseller credibility elevates Amy's brand into the corporate leadership space.",
                "key_points": [
                    "Access to corporate training budgets via established enterprise relationships",
                    "WSJ bestseller status provides prestige for summit positioning",
                ],
            },
            "connection_insights": [
                {
                    "type": "obvious",
                    "insight": "Both serve ambitious women entrepreneurs at different stages of their career journey.",
                },
                {
                    "type": "non_obvious",
                    "insight": "Sarah's executive clients are potential corporate sponsors for Amy's annual event, creating a three-way value loop.",
                },
            ],
            "reciprocity_assessment": {
                "balance": "slightly_asymmetric",
                "stronger_side": "partner_b",
                "explanation": "Amy's audience reach significantly exceeds Sarah's, but Sarah's corporate access and prestige partially compensate.",
                "gap": "Sarah could strengthen reciprocity by offering a corporate workshop discount exclusively for Amy's audience.",
            },
            "citations": {
                "20M+ podcast downloads": "social_proof",
                "Wall Street Journal bestseller": "social_proof",
                "Fortune 500 network": "who_you_serve",
            },
            "confidence": {
                "data_richness": "high",
                "explanation_confidence": "medium",
            },
        }
        explanation = LLMExplanation(**data)
        assert explanation.what_partner_b_brings_to_a.summary.startswith("Amy's massive")
        assert len(explanation.what_partner_b_brings_to_a.key_points) == 3
        assert len(explanation.what_partner_a_brings_to_b.key_points) == 2
        assert len(explanation.connection_insights) == 2
        assert explanation.connection_insights[1].type == "non_obvious"
        assert explanation.reciprocity_assessment.balance == "slightly_asymmetric"
        assert explanation.reciprocity_assessment.stronger_side == "partner_b"
        assert explanation.reciprocity_assessment.gap is not None
        assert len(explanation.citations) == 3
        assert explanation.confidence["data_richness"] == "high"


# ────────────────────────────────────────────────────────────────
# ProspectQualification
# ────────────────────────────────────────────────────────────────


class TestProspectQualification:
    """Validates the prospect qualification schema."""

    def test_valid_qualification(self):
        """A prospect passing qualification with a high estimated score."""
        pq = ProspectQualification(
            is_jv_candidate=True,
            confidence="high",
            reasoning="Strong alignment: serves same audience, has complementary offerings, active podcast with 50+ episodes.",
            estimated_match_quality=82.5,
        )
        assert pq.is_jv_candidate is True
        assert pq.confidence == Confidence.HIGH
        assert pq.estimated_match_quality == 82.5

    def test_estimated_match_quality_at_boundaries(self):
        """Boundary values 0 and 100 are valid."""
        low = ProspectQualification(is_jv_candidate=False, estimated_match_quality=0)
        assert low.estimated_match_quality == 0
        high = ProspectQualification(is_jv_candidate=True, estimated_match_quality=100)
        assert high.estimated_match_quality == 100

    def test_rejects_match_quality_above_100(self):
        """estimated_match_quality > 100 must fail."""
        with pytest.raises(ValidationError) as exc_info:
            ProspectQualification(is_jv_candidate=True, estimated_match_quality=101)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("estimated_match_quality",) for e in errors)

    def test_rejects_match_quality_below_0(self):
        """estimated_match_quality < 0 must fail."""
        with pytest.raises(ValidationError) as exc_info:
            ProspectQualification(is_jv_candidate=True, estimated_match_quality=-5)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("estimated_match_quality",) for e in errors)

    def test_defaults(self):
        """Only is_jv_candidate is truly required."""
        pq = ProspectQualification(is_jv_candidate=False)
        assert pq.confidence == Confidence.LOW
        assert pq.reasoning == ""
        assert pq.estimated_match_quality == 0.0


# ────────────────────────────────────────────────────────────────
# DiscoveryResult
# ────────────────────────────────────────────────────────────────


class TestDiscoveryResult:
    """Validates the acquisition discovery output schema."""

    def test_mixed_data(self):
        """Discovery result with prospects from multiple tools."""
        data = {
            "prospects": [
                {
                    "name": "Lisa Martinez",
                    "website": "https://lisamartinez.com",
                    "email": "lisa@lisamartinez.com",
                    "linkedin": "https://linkedin.com/in/lisamartinez",
                    "niche": "sales coaching",
                    "what_you_do": "Teaches B2B sales teams to close enterprise deals using consultative selling.",
                    "source_tool": "exa_search",
                    "source_query": "women sales coaches with podcasts",
                    "pre_score": 74.2,
                },
                {
                    "name": "David Park",
                    "website": "https://davidpark.co",
                    "niche": "leadership development",
                    "what_you_do": "Executive leadership programs for tech companies.",
                    "source_tool": "apollo",
                    "source_query": "leadership coaches tech industry",
                    "pre_score": None,
                },
            ],
            "queries_used": [
                "women sales coaches with podcasts",
                "leadership coaches tech industry",
            ],
            "tools_used": ["exa_search", "apollo"],
            "total_cost": 0.045,
        }
        result = DiscoveryResult(**data)
        assert len(result.prospects) == 2
        assert result.prospects[0].name == "Lisa Martinez"
        assert result.prospects[0].pre_score == 74.2
        assert result.prospects[1].pre_score is None
        assert result.prospects[1].email == ""
        assert len(result.queries_used) == 2
        assert "exa_search" in result.tools_used
        assert result.total_cost == pytest.approx(0.045)

    def test_empty_discovery(self):
        """No prospects found -- valid but empty."""
        result = DiscoveryResult()
        assert result.prospects == []
        assert result.queries_used == []
        assert result.tools_used == []
        assert result.total_cost == 0.0


# ────────────────────────────────────────────────────────────────
# MatchGap
# ────────────────────────────────────────────────────────────────


class TestMatchGap:
    """Validates the gap analysis schema."""

    def test_gap_calculation_fields(self):
        """Full gap analysis with scores and suggested query."""
        data = {
            "client_id": "clnt_abc123",
            "client_name": "Transformative Leadership Co.",
            "current_match_count": 4,
            "target_match_count": 10,
            "gap": 6,
            "best_scores": [92.1, 88.7, 85.3, 71.2],
            "lowest_qualifying_score": 71.2,
            "suggested_query": "leadership coaches serving women entrepreneurs with active podcast and 5000+ email list",
        }
        mg = MatchGap(**data)
        assert mg.client_id == "clnt_abc123"
        assert mg.gap == 6
        assert len(mg.best_scores) == 4
        assert mg.lowest_qualifying_score == 71.2
        assert mg.suggested_query != ""

    def test_defaults(self):
        """Only client_id is required."""
        mg = MatchGap(client_id="clnt_xyz")
        assert mg.client_name == ""
        assert mg.current_match_count == 0
        assert mg.target_match_count == 10
        assert mg.gap == 0
        assert mg.best_scores == []
        assert mg.lowest_qualifying_score is None
        assert mg.suggested_query == ""


# ────────────────────────────────────────────────────────────────
# ClaimVerification
# ────────────────────────────────────────────────────────────────


class TestClaimVerification:
    """Validates the claim grounding check schema."""

    def test_valid_grounded_percentage(self):
        """Fully populated claim verification result."""
        data = {
            "claims": [
                {
                    "claim": "She is a Wall Street Journal bestselling author.",
                    "status": "grounded",
                    "source_field": "social_proof",
                    "note": "Exact text found on homepage hero section.",
                },
                {
                    "claim": "She has coached 500+ executives.",
                    "status": "inferred",
                    "source_field": "social_proof",
                    "note": "Site says '200+ executives'; 500 is not substantiated.",
                },
                {
                    "claim": "She partners with Fortune 100 companies.",
                    "status": "ungrounded",
                    "source_field": "",
                    "note": "No evidence found on the site for Fortune 100 claim.",
                },
            ],
            "grounded_percentage": 0.33,
            "recommendation": "remove_ungrounded",
        }
        cv = ClaimVerification(**data)
        assert len(cv.claims) == 3
        assert cv.claims[0].status == "grounded"
        assert cv.claims[1].status == "inferred"
        assert cv.claims[2].status == "ungrounded"
        assert cv.grounded_percentage == pytest.approx(0.33)
        assert cv.recommendation == "remove_ungrounded"

    def test_grounded_percentage_boundaries(self):
        """0.0 and 1.0 are both valid boundary values."""
        zero = ClaimVerification(
            grounded_percentage=0.0, recommendation="fall_back_to_template"
        )
        assert zero.grounded_percentage == 0.0
        one = ClaimVerification(
            grounded_percentage=1.0, recommendation="use_as_is"
        )
        assert one.grounded_percentage == 1.0

    def test_rejects_grounded_percentage_above_1(self):
        """grounded_percentage > 1.0 must fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimVerification(
                grounded_percentage=1.1, recommendation="use_as_is"
            )
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("grounded_percentage",) for e in errors)

    def test_rejects_grounded_percentage_below_0(self):
        """grounded_percentage < 0.0 must fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ClaimVerification(
                grounded_percentage=-0.1, recommendation="use_as_is"
            )
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("grounded_percentage",) for e in errors)
