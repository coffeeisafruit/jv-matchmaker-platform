"""
Tests for the owl_research package.

Covers:
- matching/enrichment/owl_research/schemas/profile_schema.py
- matching/enrichment/owl_research/agents/owl_enrichment_service.py
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from matching.enrichment.owl_research.schemas.profile_schema import (
    BatchProgress,
    CompanyInfo,
    EnrichedProfile,
    IdealCustomer,
    PartnershipSeeking,
    ProfileEnrichmentResult,
    VerifiedField,
    VerifiedList,
)
from matching.enrichment.owl_research.agents.owl_enrichment_service import (
    OWLEnrichmentService,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_verified_field(value="", source_quote="", source_url="", confidence=0.0):
    """Shortcut to build a VerifiedField with given values."""
    return VerifiedField(
        value=value,
        source_quote=source_quote,
        source_url=source_url,
        confidence=confidence,
    )


def _make_verified_list(values=None, source_quote="", source_url="", confidence=0.0):
    """Shortcut to build a VerifiedList with given values."""
    return VerifiedList(
        values=values or [],
        source_quote=source_quote,
        source_url=source_url,
        confidence=confidence,
    )


def _verified(value="Test", quote="From source"):
    """Return a VerifiedField that passes is_verified()."""
    return _make_verified_field(value=value, source_quote=quote, confidence=0.85)


def _verified_list(values=None, quote="From source"):
    """Return a VerifiedList that passes is_verified()."""
    return _make_verified_list(values=values or ["item"], source_quote=quote, confidence=0.85)


def _build_fully_verified_profile() -> EnrichedProfile:
    """Build an EnrichedProfile with all 12 verifiable fields verified."""
    return EnrichedProfile(
        full_name=_verified("Jane Doe", "Jane Doe is the CEO"),
        title=_verified("CEO", "CEO of Acme Corp"),
        company=CompanyInfo(
            name=_verified("Acme Corp", "Acme Corp is a..."),
            website=_verified("https://acme.com", "Visit acme.com"),
            industry=_make_verified_field(),
            size=_make_verified_field(),
            description=_verified("A consulting firm", "Acme Corp is a consulting firm"),
        ),
        email=_verified("jane@acme.com", "Contact jane@acme.com"),
        phone=_verified("555-0100", "Call 555-0100"),
        booking_link=_verified("https://acme.com/book", "Book at acme.com/book"),
        offerings=_verified_list(["Coaching", "Consulting"], "Offers coaching and consulting"),
        signature_programs=_verified_list(["Acme Leadership Academy"]),
        ideal_customer=IdealCustomer(
            description=_verified("Small business owners", "Serves small business owners"),
            industries=_verified_list(["Tech"]),
            company_size=_make_verified_field(),
            pain_points_solved=_verified_list(["Growth"]),
        ),
        seeking=PartnershipSeeking(
            partnership_types=_verified_list(["JV", "Affiliate"], "Seeking JV and affiliate"),
            ideal_partner_profile=_make_verified_field(),
            goals=_verified_list(["Scale reach"], "Goals: scale reach"),
        ),
        matching_keywords=["coaching", "consulting", "leadership"],
        linkedin_url=_verified("https://linkedin.com/in/janedoe", "LinkedIn profile"),
        overall_confidence=1.0,
        all_sources=["https://acme.com", "https://linkedin.com/in/janedoe"],
        verification_summary="All fields verified.",
    )


def _build_empty_profile() -> EnrichedProfile:
    """Build an EnrichedProfile with all defaults (nothing verified)."""
    return EnrichedProfile()


def _build_partial_profile(verified_count: int = 5) -> EnrichedProfile:
    """Build an EnrichedProfile with exactly `verified_count` verified fields.

    The 12 verifiable fields checked in get_verified_field_count() are:
        full_name, title, company.name, company.website, company.description,
        email, phone, booking_link, offerings, ideal_customer.description,
        seeking.partnership_types, linkedin_url
    """
    fields_in_order = [
        "full_name", "title", "company_name", "company_website",
        "company_description", "email", "phone", "booking_link",
        "offerings", "ideal_customer_description",
        "seeking_partnership_types", "linkedin_url",
    ]

    # Determine which fields to verify
    verified_set = set(fields_in_order[:verified_count])

    def vf(key):
        if key in verified_set:
            return _verified()
        return _make_verified_field()

    def vl(key):
        if key in verified_set:
            return _verified_list()
        return _make_verified_list()

    return EnrichedProfile(
        full_name=vf("full_name"),
        title=vf("title"),
        company=CompanyInfo(
            name=vf("company_name"),
            website=vf("company_website"),
            description=vf("company_description"),
        ),
        email=vf("email"),
        phone=vf("phone"),
        booking_link=vf("booking_link"),
        offerings=vl("offerings"),
        ideal_customer=IdealCustomer(
            description=vf("ideal_customer_description"),
        ),
        seeking=PartnershipSeeking(
            partnership_types=vl("seeking_partnership_types"),
        ),
        linkedin_url=vf("linkedin_url"),
    )


def _create_service_without_init():
    """Create an OWLEnrichmentService without invoking __init__
    (avoids importing OWLProfileEnricher)."""
    with patch.object(OWLEnrichmentService, '__init__', lambda self: None):
        svc = OWLEnrichmentService.__new__(OWLEnrichmentService)
        svc.enricher = MagicMock()
        svc.profiles_processed = 0
        return svc


# ===========================================================================
# VerifiedField tests
# ===========================================================================

class TestVerifiedField:

    def test_verified_field_defaults(self):
        """Empty by default — all strings empty, confidence 0."""
        field = VerifiedField()
        assert field.value == ""
        assert field.source_quote == ""
        assert field.source_url == ""
        assert field.confidence == 0.0

    def test_verified_field_is_verified_true(self):
        """value + source_quote → True."""
        field = _make_verified_field(value="John", source_quote="From LinkedIn")
        assert field.is_verified() is True

    def test_verified_field_is_verified_no_quote(self):
        """value only, no source_quote → False."""
        field = _make_verified_field(value="John")
        assert field.is_verified() is False

    def test_verified_field_is_verified_no_value(self):
        """source_quote only, no value → False."""
        field = _make_verified_field(source_quote="From LinkedIn")
        assert field.is_verified() is False

    def test_verified_field_whitespace_only(self):
        """Whitespace-only value and/or quote should not count as verified."""
        field = _make_verified_field(value="   ", source_quote="   ")
        assert field.is_verified() is False

        field2 = _make_verified_field(value="Real", source_quote="   ")
        assert field2.is_verified() is False

        field3 = _make_verified_field(value="   ", source_quote="Real")
        assert field3.is_verified() is False

    def test_verified_field_str_returns_value(self):
        """str(field) should return the value string."""
        field = _make_verified_field(value="Hello World")
        assert str(field) == "Hello World"

        empty = VerifiedField()
        assert str(empty) == ""


# ===========================================================================
# VerifiedList tests
# ===========================================================================

class TestVerifiedList:

    def test_verified_list_defaults(self):
        """Empty by default — empty list, empty strings, confidence 0."""
        vl = VerifiedList()
        assert vl.values == []
        assert vl.source_quote == ""
        assert vl.source_url == ""
        assert vl.confidence == 0.0

    def test_verified_list_is_verified_true(self):
        """Non-empty values + non-empty source_quote → True."""
        vl = _make_verified_list(values=["Coaching"], source_quote="Offers coaching")
        assert vl.is_verified() is True

    def test_verified_list_is_verified_no_values(self):
        """Empty values list → False, even with source_quote."""
        vl = _make_verified_list(values=[], source_quote="Offers coaching")
        assert vl.is_verified() is False

    def test_verified_list_is_verified_no_quote(self):
        """Values but no source_quote → False."""
        vl = _make_verified_list(values=["Coaching", "Consulting"])
        assert vl.is_verified() is False


# ===========================================================================
# EnrichedProfile tests
# ===========================================================================

class TestEnrichedProfile:

    def test_enriched_profile_defaults(self):
        """All fields should be empty/default when constructed with no args."""
        profile = _build_empty_profile()
        assert profile.full_name.value == ""
        assert profile.title.value == ""
        assert profile.company.name.value == ""
        assert profile.email.value == ""
        assert profile.matching_keywords == []
        assert profile.all_sources == []
        assert profile.overall_confidence == 0.0

    def test_verified_field_count_zero(self):
        """Empty profile → 0 verified fields."""
        profile = _build_empty_profile()
        assert profile.get_verified_field_count() == 0

    def test_verified_field_count_full(self):
        """All 12 verifiable fields verified → 12."""
        profile = _build_fully_verified_profile()
        assert profile.get_verified_field_count() == 12

    def test_verified_field_count_partial(self):
        """Exactly 5 verified fields → 5."""
        profile = _build_partial_profile(verified_count=5)
        assert profile.get_verified_field_count() == 5

    def test_verification_report_format(self):
        """Report string contains header, field count, and confidence."""
        profile = _build_fully_verified_profile()
        report = profile.get_verification_report()

        assert "VERIFICATION REPORT" in report
        assert "Verified Fields: 12/12" in report
        assert "Overall Confidence:" in report

        # Also check a partially verified profile
        partial = _build_partial_profile(verified_count=3)
        partial_report = partial.get_verification_report()
        assert "Verified Fields: 3/12" in partial_report


# ===========================================================================
# ProfileEnrichmentResult tests
# ===========================================================================

class TestProfileEnrichmentResult:

    def test_to_jv_matcher_format_empty(self):
        """No enriched data → empty dict."""
        result = ProfileEnrichmentResult(input_name="Test User")
        assert result.to_jv_matcher_format() == {}

    def test_to_jv_matcher_format_with_data(self):
        """Verified fields should appear in the output dict."""
        profile = _build_fully_verified_profile()
        result = ProfileEnrichmentResult(
            input_name="Jane Doe",
            enriched=profile,
        )
        fmt = result.to_jv_matcher_format()

        assert fmt.get("bio") == "Jane Doe, CEO"
        assert fmt.get("website") == "https://acme.com"
        assert fmt.get("offering") == "Coaching, Consulting"
        assert "seeking" in fmt
        assert fmt.get("email") == "jane@acme.com"
        assert fmt.get("phone") == "555-0100"
        assert fmt.get("booking_link") == "https://acme.com/book"

    def test_to_jv_matcher_format_only_verified(self):
        """Unverified fields should NOT appear in the flat dict
        (except metadata keys)."""
        # Profile with only full_name and title verified
        profile = EnrichedProfile(
            full_name=_verified("John", "John from site"),
            title=_verified("CTO", "CTO from site"),
        )
        result = ProfileEnrichmentResult(
            input_name="John",
            enriched=profile,
        )
        fmt = result.to_jv_matcher_format()

        # Bio should be present (both full_name and title verified)
        assert "bio" in fmt
        # These should NOT be present (unverified)
        assert "seeking" not in fmt
        assert "who_you_serve" not in fmt
        assert "what_you_do" not in fmt
        assert "offering" not in fmt
        assert "website" not in fmt
        assert "linkedin" not in fmt
        assert "email" not in fmt
        assert "phone" not in fmt
        assert "booking_link" not in fmt
        assert "signature_programs" not in fmt

    def test_to_jv_matcher_format_metadata(self):
        """Output should always include _confidence, _verified_fields,
        _all_sources, _verification_summary, _keywords."""
        profile = _build_fully_verified_profile()
        result = ProfileEnrichmentResult(
            input_name="Jane Doe",
            enriched=profile,
        )
        fmt = result.to_jv_matcher_format()

        assert "_confidence" in fmt
        assert "_verified_fields" in fmt
        assert "_all_sources" in fmt
        assert "_verification_summary" in fmt
        assert "_keywords" in fmt

        assert fmt["_confidence"] == profile.overall_confidence
        assert fmt["_verified_fields"] == 12
        assert fmt["_all_sources"] == profile.all_sources
        assert fmt["_keywords"] == profile.matching_keywords

    def test_to_jv_matcher_format_seeking(self):
        """Verified partnership_types + goals → combined seeking string."""
        profile = EnrichedProfile(
            seeking=PartnershipSeeking(
                partnership_types=_verified_list(["JV", "Affiliate"], "Seeking JV"),
                goals=_verified_list(["Scale reach", "New markets"], "Goals stated"),
            ),
        )
        result = ProfileEnrichmentResult(
            input_name="Test",
            enriched=profile,
        )
        fmt = result.to_jv_matcher_format()

        assert "seeking" in fmt
        seeking_val = fmt["seeking"]
        # partnership_types joined by ", " then goals joined by "; ", combined with ". "
        assert "JV, Affiliate" in seeking_val
        assert "Scale reach; New markets" in seeking_val
        assert "seeking_source" in fmt

    def test_to_jv_matcher_format_bio(self):
        """Verified full_name + title → 'Name, Title' bio string."""
        profile = EnrichedProfile(
            full_name=_verified("Alice Smith", "Alice Smith is..."),
            title=_verified("VP Sales", "VP Sales at..."),
        )
        result = ProfileEnrichmentResult(
            input_name="Alice Smith",
            enriched=profile,
        )
        fmt = result.to_jv_matcher_format()

        assert fmt.get("bio") == "Alice Smith, VP Sales"
        assert "bio_source" in fmt


# ===========================================================================
# BatchProgress tests
# ===========================================================================

class TestBatchProgress:

    def test_batch_progress_defaults(self):
        """All counters should be 0 by default."""
        bp = BatchProgress()
        assert bp.total_profiles == 0
        assert bp.completed == 0
        assert bp.failed == 0
        assert bp.skipped == 0
        assert bp.total_cost_usd == 0.0
        assert bp.avg_confidence == 0.0
        assert bp.avg_verified_fields == 0.0
        assert bp.last_processed_index == -1
        assert bp.started_at is None
        assert bp.last_updated is None

    def test_batch_progress_tracks_count(self):
        """Can set completed and failed directly."""
        bp = BatchProgress(total_profiles=10, completed=5, failed=2)
        assert bp.total_profiles == 10
        assert bp.completed == 5
        assert bp.failed == 2


# ===========================================================================
# OWLEnrichmentService._convert_to_verified_profile tests
# ===========================================================================

class TestConvertToVerifiedProfile:

    def _get_service(self):
        return _create_service_without_init()

    def test_convert_full_owl_data(self):
        """Complete OWL dict → populated EnrichedProfile with correct fields."""
        svc = self._get_service()
        owl_data = {
            "full_name": {"value": "Jane Doe", "source_quote": "Jane Doe, CEO", "source_url": "https://example.com"},
            "title": {"value": "CEO", "source_quote": "CEO of Acme", "source_url": "https://example.com"},
            "company": {
                "name": {"value": "Acme Corp", "source_quote": "Acme Corp is...", "source_url": "https://acme.com"},
                "website": {"value": "https://acme.com", "source_quote": "Visit acme.com"},
                "industry": {"value": "Consulting", "source_quote": "Consulting firm"},
                "description": {"value": "A consulting firm", "source_quote": "Acme is a consulting firm"},
            },
            "email": {"value": "jane@acme.com", "source_quote": "Email: jane@acme.com"},
            "phone": {"value": "555-0100", "source_quote": "Phone: 555-0100"},
            "booking_link": {"value": "https://acme.com/book", "source_quote": "Book at acme.com/book"},
            "offerings": {"values": ["Coaching", "Consulting"], "source_quote": "Offers coaching and consulting"},
            "signature_programs": {"values": ["Leadership Academy"], "source_quote": "Her Leadership Academy"},
            "who_they_serve": {"value": "Small businesses", "source_quote": "Serves small businesses"},
            "ideal_customer": {
                "description": {"value": "Small biz", "source_quote": "Ideal customer is small biz"},
                "industries": {"values": ["Tech"], "source_quote": "Tech industry"},
            },
            "seeking": {
                "partnership_types": {"values": ["JV"], "source_quote": "Seeking JV"},
                "goals": {"values": ["Scale"], "source_quote": "Goal: scale"},
            },
            "linkedin_url": {"value": "https://linkedin.com/in/jane", "source_quote": "LinkedIn page"},
            "matching_keywords": ["coaching", "consulting"],
            "verification_summary": "Mostly verified",
        }
        sources = ["https://acme.com", "https://linkedin.com/in/jane"]

        profile = svc._convert_to_verified_profile(owl_data, sources)

        assert profile.full_name.value == "Jane Doe"
        assert profile.full_name.is_verified()
        assert profile.title.value == "CEO"
        assert profile.company.name.value == "Acme Corp"
        assert profile.email.value == "jane@acme.com"
        assert profile.offerings.values == ["Coaching", "Consulting"]
        assert profile.linkedin_url.value == "https://linkedin.com/in/jane"
        assert profile.matching_keywords == ["coaching", "consulting"]
        assert profile.all_sources == sources
        assert profile.verification_summary == "Mostly verified"
        assert profile.overall_confidence > 0

    def test_convert_confidence_with_quote(self):
        """value + source_quote → confidence 0.85."""
        svc = self._get_service()
        owl_data = {
            "full_name": {"value": "John", "source_quote": "John is..."},
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        assert profile.full_name.confidence == 0.85

    def test_convert_confidence_url_only(self):
        """value + source_url (but no source_quote) → confidence 0.7."""
        svc = self._get_service()
        owl_data = {
            "full_name": {"value": "John", "source_url": "https://example.com"},
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        assert profile.full_name.confidence == 0.7

    def test_convert_confidence_value_only(self):
        """value only (no source_quote, no source_url) → confidence 0.5."""
        svc = self._get_service()
        owl_data = {
            "full_name": {"value": "John"},
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        assert profile.full_name.confidence == 0.5

    def test_convert_company_as_string(self):
        """company='Acme' (string) → wrapped in dict with name field."""
        svc = self._get_service()
        owl_data = {
            "company": "Acme",
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        # The string "Acme" should become company.name.value
        assert profile.company.name.value == "Acme"

    def test_convert_seeking_flat_format(self):
        """seeking with 'values' key → flat format, partnership_types populated, goals empty."""
        svc = self._get_service()
        owl_data = {
            "seeking": {
                "values": ["JV Partners", "Affiliates"],
                "source_quote": "Looking for JV partners",
            },
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        assert profile.seeking.partnership_types.values == ["JV Partners", "Affiliates"]
        assert profile.seeking.partnership_types.is_verified()
        # Goals should be an empty VerifiedList in flat format
        assert profile.seeking.goals.values == []

    def test_convert_seeking_nested_format(self):
        """seeking with 'partnership_types' and 'goals' nested keys."""
        svc = self._get_service()
        owl_data = {
            "seeking": {
                "partnership_types": {
                    "values": ["JV"],
                    "source_quote": "Seeks JV",
                },
                "goals": {
                    "values": ["Revenue growth"],
                    "source_quote": "Goal is revenue growth",
                },
            },
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        assert profile.seeking.partnership_types.values == ["JV"]
        assert profile.seeking.partnership_types.is_verified()
        assert profile.seeking.goals.values == ["Revenue growth"]
        assert profile.seeking.goals.is_verified()

    def test_convert_overall_confidence(self):
        """overall_confidence = get_verified_field_count() / 12.0."""
        svc = self._get_service()
        # Create data with exactly 3 verifiable fields that will count
        # (full_name, title, company.name — all with value + source_quote)
        owl_data = {
            "full_name": {"value": "Jane", "source_quote": "Jane is..."},
            "title": {"value": "CEO", "source_quote": "She is the CEO"},
            "company": {
                "name": {"value": "Acme", "source_quote": "Acme Corp"},
            },
        }
        profile = svc._convert_to_verified_profile(owl_data, [])
        expected_count = profile.get_verified_field_count()
        assert profile.overall_confidence == pytest.approx(expected_count / 12.0)
        assert expected_count == 3


# ===========================================================================
# OWLEnrichmentService.enrich_profile (mocked) tests
# ===========================================================================

class TestEnrichProfileAsync:

    def test_enrich_profile_success(self):
        """Mock enricher returns valid data → result.enriched is populated."""
        with patch(
            'matching.enrichment.owl_research.agents.owl_enrichment_service.OWLProfileEnricher'
        ) as MockEnricher:
            mock_enricher = MockEnricher.return_value
            mock_enricher.enrich_profile = AsyncMock(return_value={
                "enriched": {
                    "full_name": {"value": "Test User", "source_quote": "Test User is..."},
                    "title": {"value": "CTO", "source_quote": "CTO of TestCo"},
                    "company": {
                        "name": {"value": "TestCo", "source_quote": "TestCo is..."},
                    },
                },
                "sources": ["https://testco.com"],
            })

            svc = OWLEnrichmentService()
            result = asyncio.run(svc.enrich_profile(name="Test User"))

            assert result.enriched is not None
            assert result.enriched.full_name.value == "Test User"
            assert result.error is None

    def test_enrich_profile_fallback(self):
        """Mock returns {'fallback': True} → error is set, enriched is None."""
        with patch(
            'matching.enrichment.owl_research.agents.owl_enrichment_service.OWLProfileEnricher'
        ) as MockEnricher:
            mock_enricher = MockEnricher.return_value
            mock_enricher.enrich_profile = AsyncMock(return_value={
                "enriched": {
                    "fallback": True,
                    "verification_summary": "Could not find data",
                },
                "sources": [],
            })

            svc = OWLEnrichmentService()
            result = asyncio.run(svc.enrich_profile(name="Unknown Person"))

            assert result.enriched is None
            assert result.error is not None
            assert "Could not find data" in result.error

    def test_enrich_profile_exception(self):
        """Mock raises exception → error contains exception message."""
        with patch(
            'matching.enrichment.owl_research.agents.owl_enrichment_service.OWLProfileEnricher'
        ) as MockEnricher:
            mock_enricher = MockEnricher.return_value
            mock_enricher.enrich_profile = AsyncMock(
                side_effect=RuntimeError("API limit exceeded")
            )

            svc = OWLEnrichmentService()
            result = asyncio.run(svc.enrich_profile(name="Test User"))

            assert result.enriched is None
            assert result.error is not None
            assert "API limit exceeded" in result.error

    def test_enrich_profile_increments_counter(self):
        """Successful enrichment → profiles_processed incremented by 1."""
        with patch(
            'matching.enrichment.owl_research.agents.owl_enrichment_service.OWLProfileEnricher'
        ) as MockEnricher:
            mock_enricher = MockEnricher.return_value
            mock_enricher.enrich_profile = AsyncMock(return_value={
                "enriched": {
                    "full_name": {"value": "User", "source_quote": "User is..."},
                },
                "sources": [],
            })

            svc = OWLEnrichmentService()
            assert svc.profiles_processed == 0

            asyncio.run(svc.enrich_profile(name="User"))
            assert svc.profiles_processed == 1

            asyncio.run(svc.enrich_profile(name="User 2"))
            assert svc.profiles_processed == 2
