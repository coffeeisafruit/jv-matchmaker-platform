"""
Tests for matching/enrichment/smart_enrichment_service.py

Covers:
- EnrichmentStats: dataclass defaults, cost calculations, savings
- SmartEnrichmentService: helper methods (_extract_domain, _extract_linkedin_basics,
  _identify_missing_fields, _is_sufficiently_enriched, _calculate_confidence)
- SmartEnrichmentService.enrich_contact: progressive enrichment with mocked deps
- get_stats_report: formatted report output

All tests are pure Python with mocked objects -- no database access required.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from matching.enrichment.smart_enrichment_service import (
    EnrichmentStats,
    SmartEnrichmentService,
)


# =============================================================================
# HELPER: Build a SmartEnrichmentService with __init__ bypassed
# =============================================================================

def make_service(**overrides):
    """
    Create a SmartEnrichmentService without triggering __init__
    (avoids importing external dependencies like OWL and ProfileResearchCache).
    """
    with patch.object(SmartEnrichmentService, '__init__', lambda self, **kw: None):
        svc = SmartEnrichmentService.__new__(SmartEnrichmentService)
        svc.stats = EnrichmentStats()
        svc.cache = None
        svc.owl_service = None
        svc.company_cache = {}
        svc.use_cache = False
        svc.enable_owl = False
        svc.max_searches = 2
        svc.website_scraper = MagicMock()

        for key, value in overrides.items():
            setattr(svc, key, value)

        return svc


# =============================================================================
# EnrichmentStats -- defaults
# =============================================================================

class TestEnrichmentStatsDefaults:
    def test_stats_defaults(self):
        """All fields should initialize to zero."""
        stats = EnrichmentStats()
        assert stats.profiles_processed == 0
        assert stats.website_scrapes == 0
        assert stats.linkedin_scrapes == 0
        assert stats.targeted_searches == 0
        assert stats.full_owl_searches == 0
        assert stats.api_calls_saved == 0


# =============================================================================
# EnrichmentStats -- get_estimated_cost
# =============================================================================

class TestEnrichmentStatsEstimatedCost:
    def test_get_estimated_cost(self):
        """targeted=2, full_owl=1 => (2 * 1.5 * 0.004) + (1 * 4 * 0.004) = 0.028."""
        stats = EnrichmentStats(targeted_searches=2, full_owl_searches=1)
        expected = (2 * 1.5 * 0.004) + (1 * 4 * 0.004)
        assert stats.get_estimated_cost() == pytest.approx(expected)
        assert stats.get_estimated_cost() == pytest.approx(0.028)

    def test_get_estimated_cost_zero(self):
        """No paid searches => cost is 0.0."""
        stats = EnrichmentStats()
        assert stats.get_estimated_cost() == 0.0


# =============================================================================
# EnrichmentStats -- get_savings
# =============================================================================

class TestEnrichmentStatsSavings:
    def test_get_savings(self):
        """profiles_processed=5, targeted=1 => savings = would_have - actual."""
        stats = EnrichmentStats(profiles_processed=5, targeted_searches=1)
        would_have = 5 * 4 * 0.004  # 0.08
        actual = 1 * 1.5 * 0.004  # 0.006
        assert stats.get_savings() == pytest.approx(would_have - actual)

    def test_get_savings_no_paid(self):
        """All free methods => savings equals the full would-have-cost."""
        stats = EnrichmentStats(profiles_processed=10)
        would_have = 10 * 4 * 0.004  # 0.16
        assert stats.get_savings() == pytest.approx(would_have)

    def test_get_savings_zero_profiles(self):
        """Zero profiles processed => zero savings."""
        stats = EnrichmentStats()
        assert stats.get_savings() == pytest.approx(0.0)


# =============================================================================
# SmartEnrichmentService._extract_domain
# =============================================================================

class TestExtractDomain:
    def test_extract_domain_valid(self):
        """Valid business email => domain extracted."""
        svc = make_service()
        assert svc._extract_domain('jane@acmecorp.com') == 'acmecorp.com'

    def test_extract_domain_generic_gmail(self):
        """Gmail address => None (generic domain)."""
        svc = make_service()
        assert svc._extract_domain('jane@gmail.com') is None

    def test_extract_domain_generic_yahoo(self):
        """Yahoo address => None (generic domain)."""
        svc = make_service()
        assert svc._extract_domain('jane@yahoo.com') is None

    def test_extract_domain_generic_hotmail(self):
        """Hotmail address => None (generic domain)."""
        svc = make_service()
        assert svc._extract_domain('jane@hotmail.com') is None

    def test_extract_domain_no_at(self):
        """String without @ => None."""
        svc = make_service()
        assert svc._extract_domain('not-an-email') is None

    def test_extract_domain_empty(self):
        """Empty string => None."""
        svc = make_service()
        assert svc._extract_domain('') is None

    def test_extract_domain_case_insensitive(self):
        """Domain should be lowercased."""
        svc = make_service()
        assert svc._extract_domain('Jane@AcmeCorp.COM') == 'acmecorp.com'


# =============================================================================
# SmartEnrichmentService._extract_linkedin_basics
# =============================================================================

class TestExtractLinkedinBasics:
    def test_extract_linkedin_basics_valid(self):
        """Valid LinkedIn /in/ URL => linkedin_verified = True."""
        svc = make_service()
        result = svc._extract_linkedin_basics('https://linkedin.com/in/jane', 'Jane')
        assert result == {'linkedin_verified': True}

    def test_extract_linkedin_basics_invalid(self):
        """Non-LinkedIn URL => empty dict."""
        svc = make_service()
        result = svc._extract_linkedin_basics('https://twitter.com/jane', 'Jane')
        assert result == {}

    def test_extract_linkedin_basics_empty(self):
        """Empty URL => empty dict."""
        svc = make_service()
        result = svc._extract_linkedin_basics('', 'Jane')
        assert result == {}


# =============================================================================
# SmartEnrichmentService._identify_missing_fields
# =============================================================================

class TestIdentifyMissingFields:
    def test_identify_missing_fields_all_missing(self):
        """Empty dict => all 5 critical fields reported missing."""
        svc = make_service()
        missing = svc._identify_missing_fields({})
        assert len(missing) == 5
        assert set(missing) == {
            'seeking', 'who_you_serve', 'what_you_do',
            'offering', 'signature_programs',
        }

    def test_identify_missing_fields_none_missing(self):
        """All 5 critical fields with long values => no missing fields."""
        svc = make_service()
        data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching',
            'offering': 'Full-service marketing strategy packages',
            'signature_programs': 'The Growth Accelerator 12-week program',
        }
        missing = svc._identify_missing_fields(data)
        assert missing == []

    def test_identify_missing_fields_short_values(self):
        """Values shorter than 10 characters are still considered missing."""
        svc = make_service()
        data = {
            'seeking': 'short',          # < 10 chars
            'who_you_serve': 'brief',    # < 10 chars
            'what_you_do': 'Marketing consulting and coaching services',
            'offering': '',
            'signature_programs': 'x',
        }
        missing = svc._identify_missing_fields(data)
        assert 'seeking' in missing
        assert 'who_you_serve' in missing
        assert 'offering' in missing
        assert 'signature_programs' in missing
        assert 'what_you_do' not in missing


# =============================================================================
# SmartEnrichmentService._is_sufficiently_enriched
# =============================================================================

class TestIsSufficientlyEnriched:
    def test_is_sufficiently_enriched_true(self):
        """3 of 4 key fields filled (>= 10 chars) => True."""
        svc = make_service()
        data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching',
            'offering': '',
        }
        assert svc._is_sufficiently_enriched(data) is True

    def test_is_sufficiently_enriched_all_four(self):
        """All 4 key fields filled => True."""
        svc = make_service()
        data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching',
            'offering': 'Full-service marketing strategy packages',
        }
        assert svc._is_sufficiently_enriched(data) is True

    def test_is_sufficiently_enriched_false(self):
        """Only 1 of 4 key fields filled => False."""
        svc = make_service()
        data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': '',
            'what_you_do': '',
            'offering': '',
        }
        assert svc._is_sufficiently_enriched(data) is False

    def test_is_sufficiently_enriched_short_values(self):
        """Values shorter than 10 chars are not counted as filled."""
        svc = make_service()
        data = {
            'seeking': 'short',
            'who_you_serve': 'tiny',
            'what_you_do': 'x',
            'offering': 'y',
        }
        assert svc._is_sufficiently_enriched(data) is False

    def test_is_sufficiently_enriched_empty_dict(self):
        """Empty dict => False."""
        svc = make_service()
        assert svc._is_sufficiently_enriched({}) is False


# =============================================================================
# SmartEnrichmentService._calculate_confidence
# =============================================================================

class TestCalculateConfidence:
    def test_calculate_confidence_full(self):
        """All 12 important fields filled (> 5 chars) => 1.0."""
        svc = make_service()
        data = {
            'email': 'jane@acmecorp.com',
            'phone': '555-123-4567',
            'website': 'https://acme.com',
            'linkedin': 'https://linkedin.com/in/jane',
            'booking_link': 'https://calendly.com/jane',
            'seeking': 'Looking for strategic partners',
            'who_you_serve': 'Small business owners',
            'what_you_do': 'Marketing consulting',
            'offering': 'Strategy packages',
            'signature_programs': 'Growth Accelerator',
            'company': 'Acme Corporation',
            'bio': 'Seasoned marketing professional',
        }
        assert svc._calculate_confidence(data) == pytest.approx(1.0)

    def test_calculate_confidence_empty(self):
        """No fields filled => 0.0."""
        svc = make_service()
        assert svc._calculate_confidence({}) == pytest.approx(0.0)

    def test_calculate_confidence_partial(self):
        """6 of 12 fields filled => 0.5."""
        svc = make_service()
        data = {
            'email': 'jane@acmecorp.com',
            'phone': '555-123-4567',
            'website': 'https://acme.com',
            'linkedin': 'https://linkedin.com/in/jane',
            'booking_link': 'https://calendly.com/jane',
            'seeking': 'Looking for strategic partners',
        }
        assert svc._calculate_confidence(data) == pytest.approx(6.0 / 12.0)

    def test_calculate_confidence_short_values_ignored(self):
        """Values with 5 or fewer chars are not counted."""
        svc = make_service()
        data = {
            'email': 'a@b.c',     # exactly 5 chars => not counted (need > 5)
            'phone': '12345',     # exactly 5 chars => not counted
            'website': 'https://acme.com',  # long enough
        }
        assert svc._calculate_confidence(data) == pytest.approx(1.0 / 12.0)


# =============================================================================
# SmartEnrichmentService.enrich_contact -- cache hit
# =============================================================================

class TestEnrichContactCacheHit:
    def test_enrich_cache_hit(self):
        """Cached data that is sufficiently enriched => return cached, no API calls."""
        cached_data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching',
            'offering': 'Full-service marketing strategy packages',
        }

        mock_cache = MagicMock()
        mock_cache.get.return_value = cached_data

        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache', return_value=mock_cache), \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            svc = SmartEnrichmentService(use_cache=True, enable_owl=False)
            result_data, result_meta = asyncio.run(
                svc.enrich_contact(name='Jane Doe')
            )

        assert result_data == cached_data
        assert 'cache' in result_meta['methods_used']
        mock_research.assert_not_called()


# =============================================================================
# SmartEnrichmentService.enrich_contact -- website scrape
# =============================================================================

class TestEnrichContactWebsiteScrape:
    def test_enrich_website_scrape(self):
        """Website provided => research_and_enrich_profile is called."""
        scraped_data = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching services',
            'offering': 'Full-service marketing strategy packages',
        }

        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            mock_research.return_value = (scraped_data, True)
            MockCache.return_value.get.return_value = None

            svc = SmartEnrichmentService(use_cache=True, enable_owl=False)
            result_data, result_meta = asyncio.run(
                svc.enrich_contact(name='Jane Doe', website='https://acme.com')
            )

        mock_research.assert_called_once()
        assert 'website_scrape' in result_meta['methods_used']
        assert result_data['seeking'] == scraped_data['seeking']


# =============================================================================
# SmartEnrichmentService.enrich_contact -- low priority => no OWL
# =============================================================================

class TestEnrichContactLowPriority:
    def test_enrich_low_priority_no_owl(self):
        """priority='low' => no OWL calls even when data is insufficient."""
        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            mock_research.return_value = ({}, False)
            MockCache.return_value.get.return_value = None

            mock_owl_instance = MockOWL.return_value
            mock_owl_instance.enrich_profile = AsyncMock()

            svc = SmartEnrichmentService(use_cache=True, enable_owl=True)
            result_data, result_meta = asyncio.run(
                svc.enrich_contact(
                    name='Jane Doe',
                    website='https://acme.com',
                    priority='low',
                )
            )

        # OWL should NOT have been invoked
        mock_owl_instance.enrich_profile.assert_not_called()
        assert 'targeted_search' not in result_meta['methods_used']
        assert 'full_owl' not in result_meta['methods_used']


# =============================================================================
# SmartEnrichmentService.enrich_contact -- high priority => full OWL
# =============================================================================

class TestEnrichContactHighPriority:
    def test_enrich_high_priority_full_owl(self):
        """priority='high' with insufficient data => full OWL is called.

        The targeted search (Step 4) returns only 1 field so the data stays
        insufficient, which triggers full OWL (Step 5).
        """
        # First call result (targeted search) -- returns only 1 useful field
        targeted_result = MagicMock()
        targeted_result.enriched = True
        targeted_result.to_jv_matcher_format.return_value = {
            'seeking': 'Looking for strategic partners in B2B',
        }

        # Second call result (full OWL) -- returns all fields
        full_owl_result = MagicMock()
        full_owl_result.enriched = True
        full_owl_result.to_jv_matcher_format.return_value = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching',
            'offering': 'Full-service marketing strategy packages',
            '_confidence': 0.85,
        }

        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            mock_research.return_value = ({}, False)
            MockCache.return_value.get.return_value = None

            mock_owl_instance = MockOWL.return_value
            # First call (targeted) returns sparse data, second (full OWL) returns full data
            mock_owl_instance.enrich_profile = AsyncMock(
                side_effect=[targeted_result, full_owl_result]
            )

            svc = SmartEnrichmentService(use_cache=True, enable_owl=True)
            result_data, result_meta = asyncio.run(
                svc.enrich_contact(
                    name='Jane Doe',
                    website='https://acme.com',
                    priority='high',
                )
            )

        assert 'full_owl' in result_meta['methods_used']
        assert svc.stats.full_owl_searches == 1
        assert result_data.get('seeking') == 'Looking for strategic partners in B2B'


# =============================================================================
# SmartEnrichmentService.enrich_contact -- stats increment
# =============================================================================

class TestEnrichContactStatsIncrement:
    def test_enrich_stats_increment(self):
        """After enrichment, profiles_processed is incremented."""
        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            mock_research.return_value = ({}, False)
            MockCache.return_value.get.return_value = None

            svc = SmartEnrichmentService(use_cache=True, enable_owl=False)

            assert svc.stats.profiles_processed == 0

            asyncio.run(svc.enrich_contact(name='Jane Doe'))
            assert svc.stats.profiles_processed == 1

            asyncio.run(svc.enrich_contact(name='John Smith'))
            assert svc.stats.profiles_processed == 2

    def test_enrich_website_scrape_increments_scrapes(self):
        """Successful website scrape increments website_scrapes and api_calls_saved."""
        scraped = {
            'seeking': 'Looking for strategic partners in B2B',
            'who_you_serve': 'Small business owners and entrepreneurs',
            'what_you_do': 'Marketing consulting and coaching services',
        }

        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile') as mock_research, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService') as MockScraper, \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService') as MockOWL:

            mock_research.return_value = (scraped, True)
            MockCache.return_value.get.return_value = None

            svc = SmartEnrichmentService(use_cache=True, enable_owl=False)
            asyncio.run(
                svc.enrich_contact(name='Jane Doe', website='https://acme.com')
            )

        assert svc.stats.website_scrapes == 1
        assert svc.stats.api_calls_saved == 3


# =============================================================================
# SmartEnrichmentService.get_stats_report
# =============================================================================

class TestGetStatsReport:
    def test_stats_report_contains_sections(self):
        """Report string includes expected section headers."""
        with patch('matching.enrichment.smart_enrichment_service.research_and_enrich_profile'), \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchCache'), \
             patch('matching.enrichment.smart_enrichment_service.ProfileResearchService'), \
             patch('matching.enrichment.smart_enrichment_service.OWLEnrichmentService'):

            svc = SmartEnrichmentService(use_cache=False, enable_owl=False)
            svc.stats.profiles_processed = 5
            svc.stats.website_scrapes = 3
            svc.stats.targeted_searches = 1

            report = svc.get_stats_report()

        assert 'SMART ENRICHMENT STATISTICS' in report
        assert 'FREE Methods' in report
        assert 'PAID Methods' in report
        assert 'Efficiency' in report
        assert 'Website scrapes: 3' in report

    def test_stats_report_zero_profiles(self):
        """Report handles zero profiles without division error."""
        svc = make_service()
        report = svc.get_stats_report()

        assert 'Profiles Processed: 0' in report
        assert 'SMART ENRICHMENT STATISTICS' in report
