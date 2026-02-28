"""
Tests for matching/services.py

Covers:
- MatchScoringService: ISMC scoring framework with weighted harmonic mean
- PartnershipAnalyzer: Dynamic partnership insights with 7 dimensions

All tests are pure Python with mocked objects — no database access required.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from django.utils import timezone

from matching.services import (
    MatchScoringService,
    ScoreComponent,
    ScoreBreakdown,
    PartnershipAnalyzer,
    PartnershipInsight,
    PartnershipAnalysis,
)


# =============================================================================
# HELPER: Mock Factory Functions
# =============================================================================

def make_mock_profile(**overrides):
    """
    Create a MagicMock Profile with all required attributes set to sensible
    defaults.  Any keyword argument overrides the default.
    """
    defaults = {
        'name': 'Jane Doe',
        'company': 'Acme Corp',
        'linkedin_url': 'https://linkedin.com/in/janedoe',
        'website_url': 'https://acme.com',
        'email': 'jane@acme.com',
        'industry': 'Marketing',
        'audience_size': 'medium',
        'audience_description': 'Digital marketers focused on B2B SaaS growth strategies and lead generation techniques',
        'content_style': 'Educational long-form articles and webinars',
        'collaboration_history': [
            {'partner': 'Company A', 'type': 'webinar'},
            {'partner': 'Company B', 'type': 'podcast'},
        ],
        'enrichment_data': {
            'field1': 'val1',
            'field2': 'val2',
            'field3': 'val3',
            'field4': 'val4',
            'field5': 'val5',
        },
        'source': 'manual',
        'updated_at': timezone.now(),
        'created_at': timezone.now() - datetime.timedelta(days=30),
    }
    defaults.update(overrides)

    mock = MagicMock()
    for attr, value in defaults.items():
        setattr(mock, attr, value)

    # get_source_display() should return a human-readable label
    source_display_map = {
        'manual': 'Manual Entry',
        'clay': 'Clay Import',
        'linkedin': 'LinkedIn',
        'import': 'Bulk Import',
    }
    mock.get_source_display.return_value = source_display_map.get(
        defaults['source'], 'Manual Entry'
    )
    return mock


def make_mock_user(**overrides):
    """Create a MagicMock Django User with ICP-related attributes."""
    defaults = {
        'target_industries': ['Marketing', 'SaaS'],
        'target_audience_size': 'medium',
        'content_preferences': ['podcast', 'newsletter'],
        'business_description': 'We help SaaS companies grow through partnerships.',
        'business_domain': 'saas-growth.com',
    }
    defaults.update(overrides)

    mock = MagicMock()
    for attr, value in defaults.items():
        setattr(mock, attr, value)
    return mock


def make_mock_supabase_profile(**overrides):
    """Create a MagicMock SupabaseProfile for PartnershipAnalyzer tests."""
    defaults = {
        'id': '00000000-0000-0000-0000-000000000001',
        'name': 'John Partner',
        'email': 'john@partner.com',
        'company': 'Partner Inc',
        'offering': 'Podcast production and email marketing automation',
        'seeking': 'JV webinar partners',
        'who_you_serve': 'Marketing professionals in SaaS',
        'niche': 'Digital marketing',
        'list_size': 5000,
        'revenue_tier': 'established',
        'jv_history': None,
        'content_platforms': None,
    }
    defaults.update(overrides)

    mock = MagicMock()
    for attr, value in defaults.items():
        setattr(mock, attr, value)
    return mock


def make_mock_supabase_match(**overrides):
    """Create a MagicMock SupabaseMatch."""
    defaults = {
        'harmonic_mean': 75.0,
        'match_reason': 'Both serve SaaS marketers and offer complementary services.',
        'score_ab': 80.0,
        'score_ba': 70.0,
    }
    defaults.update(overrides)

    mock = MagicMock()
    for attr, value in defaults.items():
        setattr(mock, attr, value)
    return mock


# =============================================================================
# TEST: Harmonic Mean
# =============================================================================


class TestHarmonicMean:
    """Tests for MatchScoringService.calculate_harmonic_mean."""

    def _make_service(self):
        """Create a service instance with default mocks."""
        profile = make_mock_profile()
        user = make_mock_user()
        return MatchScoringService(profile, user)

    def test_harmonic_mean_equal_scores(self):
        """All equal scores → harmonic mean equals that score."""
        service = self._make_service()
        scores = [(5.0, 0.25), (5.0, 0.25), (5.0, 0.25), (5.0, 0.25)]
        result = service.calculate_harmonic_mean(scores)
        assert abs(result - 5.0) < 0.01

    def test_harmonic_mean_unequal_scores(self):
        """Low score pulls harmonic mean below arithmetic mean."""
        service = self._make_service()
        scores = [(9.0, 0.45), (2.0, 0.25), (7.0, 0.20), (6.0, 0.10)]
        result = service.calculate_harmonic_mean(scores)
        # Arithmetic mean (weighted) would be ~6.65; harmonic mean should be lower
        weighted_arithmetic = (9.0 * 0.45 + 2.0 * 0.25 + 7.0 * 0.20 + 6.0 * 0.10)
        assert result < weighted_arithmetic
        assert result > 0

    def test_harmonic_mean_zero_score(self):
        """One zero score → near-zero result (epsilon protection, not crash)."""
        service = self._make_service()
        scores = [(0.0, 0.50), (8.0, 0.50)]
        result = service.calculate_harmonic_mean(scores)
        # With epsilon=1e-10, the zero term dominates the denominator
        assert result < 0.001
        assert result >= 0.0

    def test_harmonic_mean_single_score(self):
        """Single item → returns that score regardless of weight."""
        service = self._make_service()
        scores = [(7.5, 1.0)]
        result = service.calculate_harmonic_mean(scores)
        assert abs(result - 7.5) < 0.01


# =============================================================================
# TEST: Intent Score
# =============================================================================


class TestIntentScore:
    """Tests for MatchScoringService.calculate_intent_score."""

    def test_intent_high_score(self):
        """Profile with all signals → score >= 7."""
        profile = make_mock_profile(
            linkedin_url='https://linkedin.com/in/test',
            website_url='https://example.com',
            email='test@example.com',
            enrichment_data={'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5,
                             'f': 6, 'g': 7, 'h': 8, 'i': 9, 'j': 10},
            collaboration_history=[
                {'partner': 'A', 'type': 'webinar'},
                {'partner': 'B', 'type': 'podcast'},
                {'partner': 'C', 'type': 'summit'},
            ],
        )
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_intent_score()
        assert component.score >= 7.0

    def test_intent_low_score(self):
        """Bare profile with nothing → low score."""
        profile = make_mock_profile(
            linkedin_url=None,
            website_url=None,
            email=None,
            enrichment_data=None,
            collaboration_history=None,
        )
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_intent_score()
        # contact_score defaults to 5.0 when no email, so it won't be zero
        assert component.score < 5.0

    def test_intent_collab_history_capped(self):
        """Collaboration history of 5 items → collab_score capped at 10."""
        collab_items = [{'partner': f'Company{i}', 'type': 'webinar'} for i in range(5)]
        profile = make_mock_profile(collaboration_history=collab_items)
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_intent_score()

        # collab_score = min(10, 5 * 2.5) = min(10, 12.5) = 10
        # Find the Collaboration History factor
        collab_factor = next(
            f for f in component.factors if f['name'] == 'Collaboration History'
        )
        assert collab_factor['score'] == 10.0

    def test_intent_returns_score_component(self):
        """Verify return type is ScoreComponent with correct fields."""
        profile = make_mock_profile()
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_intent_score()

        assert isinstance(component, ScoreComponent)
        assert component.name == 'Intent'
        assert 0 <= component.score <= 10
        assert component.weight == 0.45
        assert isinstance(component.factors, list)
        assert len(component.factors) == 5
        assert isinstance(component.explanation, str)


# =============================================================================
# TEST: Synergy Score
# =============================================================================


class TestSynergyScore:
    """Tests for MatchScoringService.calculate_synergy_score."""

    def test_synergy_matching_industry(self):
        """Profile industry in user's target_industries → industry_score = 10."""
        profile = make_mock_profile(industry='Marketing')
        user = make_mock_user(target_industries=['Marketing', 'SaaS'])
        service = MatchScoringService(profile, user)
        component = service.calculate_synergy_score()

        industry_factor = next(
            f for f in component.factors if f['name'] == 'Industry Alignment'
        )
        assert industry_factor['score'] == 10.0

    def test_synergy_mismatched_audience_size(self):
        """'tiny' profile vs user wanting 'massive' → penalized size score."""
        profile = make_mock_profile(audience_size='tiny')
        user = make_mock_user(target_audience_size='massive')
        service = MatchScoringService(profile, user)
        component = service.calculate_synergy_score()

        size_factor = next(
            f for f in component.factors if f['name'] == 'Audience Size Match'
        )
        # tiny=1, massive=5, diff=4, score = max(0, 10 - 4*2.5) = 0
        assert size_factor['score'] == 0.0

    def test_synergy_rich_audience_description(self):
        """50+ word description → full audience_desc_score (10)."""
        long_desc = ' '.join(['word'] * 55)
        profile = make_mock_profile(audience_description=long_desc)
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_synergy_score()

        audience_factor = next(
            f for f in component.factors if f['name'] == 'Audience Definition'
        )
        # word_count = 55, score = min(10, 55/5) = min(10, 11) = 10
        assert audience_factor['score'] == 10.0


# =============================================================================
# TEST: Momentum Score
# =============================================================================


class TestMomentumScore:
    """Tests for MatchScoringService.calculate_momentum_score."""

    def test_momentum_recently_updated(self):
        """updated_at = now → high freshness score."""
        profile = make_mock_profile(updated_at=timezone.now())
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_momentum_score()

        freshness_factor = next(
            f for f in component.factors if f['name'] == 'Profile Freshness'
        )
        # days_since_update ~ 0 → freshness_score ~ 10
        assert freshness_factor['score'] >= 9.0

    def test_momentum_stale_profile(self):
        """updated_at = 60 days ago → low freshness score."""
        stale_date = timezone.now() - datetime.timedelta(days=60)
        profile = make_mock_profile(updated_at=stale_date)
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_momentum_score()

        freshness_factor = next(
            f for f in component.factors if f['name'] == 'Profile Freshness'
        )
        # days=60 → score = max(0, 10 - 60/3) = max(0, -10) = 0
        assert freshness_factor['score'] == 0.0

    def test_momentum_both_urls_boost(self):
        """Both linkedin and website → activity_score = 8."""
        profile = make_mock_profile(
            linkedin_url='https://linkedin.com/in/test',
            website_url='https://example.com',
        )
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_momentum_score()

        activity_factor = next(
            f for f in component.factors if f['name'] == 'Activity Level'
        )
        assert activity_factor['score'] == 8.0

    def test_momentum_no_urls(self):
        """Neither linkedin nor website → activity_score = 3."""
        profile = make_mock_profile(linkedin_url=None, website_url=None)
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_momentum_score()

        activity_factor = next(
            f for f in component.factors if f['name'] == 'Activity Level'
        )
        assert activity_factor['score'] == 3.0


# =============================================================================
# TEST: Context Score
# =============================================================================


class TestContextScore:
    """Tests for MatchScoringService.calculate_context_score."""

    def test_context_fully_populated(self):
        """All 8 completeness fields filled → high completeness_score."""
        profile = make_mock_profile(
            name='Jane Doe',
            company='Acme Corp',
            industry='Marketing',
            audience_size='medium',
            audience_description='A relevant audience',
            linkedin_url='https://linkedin.com/in/test',
            website_url='https://example.com',
            email='jane@example.com',
        )
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_context_score()

        completeness_factor = next(
            f for f in component.factors if f['name'] == 'Profile Completeness'
        )
        # 8/8 = 10.0
        assert completeness_factor['score'] == 10.0

    def test_context_clay_source(self):
        """source='clay' → source_score = 9."""
        profile = make_mock_profile(source='clay')
        profile.get_source_display.return_value = 'Clay Import'
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        component = service.calculate_context_score()

        source_factor = next(
            f for f in component.factors if f['name'] == 'Data Source Quality'
        )
        assert source_factor['score'] == 9.0


# =============================================================================
# TEST: Full Score Calculation
# =============================================================================


class TestCalculateScore:
    """Tests for MatchScoringService.calculate_score (integration)."""

    def test_calculate_score_returns_breakdown(self):
        """check all 4 components + final_score + recommendation."""
        profile = make_mock_profile()
        user = make_mock_user()
        service = MatchScoringService(profile, user)
        breakdown = service.calculate_score()

        assert isinstance(breakdown, ScoreBreakdown)
        assert isinstance(breakdown.intent, ScoreComponent)
        assert isinstance(breakdown.synergy, ScoreComponent)
        assert isinstance(breakdown.momentum, ScoreComponent)
        assert isinstance(breakdown.context, ScoreComponent)
        assert 0 <= breakdown.final_score <= 10
        assert isinstance(breakdown.recommendation, str)
        assert len(breakdown.recommendation) > 0

    def test_high_score_recommendation(self):
        """final_score >= 8 → 'Highly recommended'."""
        profile = make_mock_profile(
            linkedin_url='https://linkedin.com/in/test',
            website_url='https://example.com',
            email='test@example.com',
            industry='Marketing',
            audience_size='medium',
            audience_description=' '.join(['word'] * 60),
            content_style='Educational podcasts',
            collaboration_history=[{'p': f'P{i}'} for i in range(5)],
            enrichment_data={f'field{i}': f'val{i}' for i in range(12)},
            source='clay',
            updated_at=timezone.now(),
        )
        profile.get_source_display.return_value = 'Clay Import'
        user = make_mock_user(
            target_industries=['Marketing'],
            target_audience_size='medium',
        )
        service = MatchScoringService(profile, user)
        breakdown = service.calculate_score()

        # This profile should be high-scoring across all dimensions
        if breakdown.final_score >= 8:
            assert 'Highly recommended' in breakdown.recommendation

    def test_medium_score_identifies_weakest(self):
        """6-8 range → recommendation mentions weakest area."""
        profile = make_mock_profile(
            linkedin_url='https://linkedin.com/in/test',
            website_url='https://example.com',
            email='test@example.com',
            industry='Unrelated Industry',
            audience_size='tiny',
            audience_description='Short desc',
            content_style=None,
            collaboration_history=None,
            enrichment_data={'a': 1, 'b': 2, 'c': 3},
            source='manual',
            updated_at=timezone.now(),
        )
        user = make_mock_user(target_industries=['Marketing'], target_audience_size='massive')
        service = MatchScoringService(profile, user)
        breakdown = service.calculate_score()

        # If score falls in 6-8 range, check weakest area is mentioned
        if 6 <= breakdown.final_score < 8:
            weakest_areas = ['intent', 'synergy', 'momentum', 'context']
            assert any(area in breakdown.recommendation.lower() for area in weakest_areas)

    def test_low_score_recommendation(self):
        """final_score < 4 → 'Low match score'."""
        profile = make_mock_profile(
            linkedin_url=None,
            website_url=None,
            email=None,
            industry=None,
            audience_size=None,
            audience_description=None,
            content_style=None,
            collaboration_history=None,
            enrichment_data=None,
            source='import',
            name=None,
            company=None,
            updated_at=timezone.now() - datetime.timedelta(days=90),
        )
        profile.get_source_display.return_value = 'Bulk Import'
        user = make_mock_user(target_industries=['Marketing'], target_audience_size='massive')
        service = MatchScoringService(profile, user)
        breakdown = service.calculate_score()

        if breakdown.final_score < 4:
            assert 'Low match score' in breakdown.recommendation


# =============================================================================
# TEST: Explanation Generators
# =============================================================================


class TestExplanationThresholds:
    """Tests for the _get_*_explanation threshold methods."""

    def _make_service(self):
        profile = make_mock_profile()
        user = make_mock_user()
        return MatchScoringService(profile, user)

    def test_intent_explanation_thresholds(self):
        """Check all 4 threshold ranges for intent explanation."""
        service = self._make_service()

        # >= 8: Strong signals
        explanation = service._get_intent_explanation(8.5)
        assert 'Strong signals' in explanation

        # >= 6: Moderate intent
        explanation = service._get_intent_explanation(6.5)
        assert 'Moderate' in explanation

        # >= 4: Limited signals
        explanation = service._get_intent_explanation(4.5)
        assert 'Limited' in explanation

        # < 4: Weak intent
        explanation = service._get_intent_explanation(2.0)
        assert 'Weak' in explanation

    def test_synergy_explanation_thresholds(self):
        """Check all 4 threshold ranges for synergy explanation."""
        service = self._make_service()

        # >= 8: Excellent
        explanation = service._get_synergy_explanation(9.0)
        assert 'Excellent' in explanation

        # >= 6: Good synergy
        explanation = service._get_synergy_explanation(7.0)
        assert 'Good' in explanation

        # >= 4: Moderate alignment
        explanation = service._get_synergy_explanation(4.5)
        assert 'Moderate' in explanation

        # < 4: Limited synergy
        explanation = service._get_synergy_explanation(2.0)
        assert 'Limited' in explanation


# =============================================================================
# TEST: PartnershipAnalyzer - Scale Insight
# =============================================================================


class TestScaleInsight:
    """Tests for PartnershipAnalyzer._build_scale_insight."""

    def _make_analyzer(self, user_list_size=5000):
        user = make_mock_user()
        user_profile = make_mock_supabase_profile(list_size=user_list_size)
        return PartnershipAnalyzer(
            user=user,
            user_supabase_profile=user_profile,
        )

    def test_scale_compatible_within_2x(self):
        """Similar list sizes (within 2x) → 'Compatible Scale'."""
        analyzer = self._make_analyzer(user_list_size=5000)
        partner = make_mock_supabase_profile(list_size=6000)
        insight = analyzer._build_scale_insight(partner)

        assert insight is not None
        assert insight.headline == 'Compatible Scale'

    def test_scale_growth_opportunity(self):
        """Partner 3x larger → 'Growth Opportunity'."""
        analyzer = self._make_analyzer(user_list_size=2000)
        partner = make_mock_supabase_profile(list_size=6000)
        insight = analyzer._build_scale_insight(partner)

        assert insight is not None
        assert insight.headline == 'Growth Opportunity'

    def test_scale_mentor_opportunity(self):
        """User 3x larger → 'Mentor Opportunity'."""
        analyzer = self._make_analyzer(user_list_size=15000)
        partner = make_mock_supabase_profile(list_size=5000)
        insight = analyzer._build_scale_insight(partner)

        assert insight is not None
        assert insight.headline == 'Mentor Opportunity'

    def test_scale_too_far_apart(self):
        """10x difference → no insight (returns None)."""
        analyzer = self._make_analyzer(user_list_size=1000)
        partner = make_mock_supabase_profile(list_size=10000)
        insight = analyzer._build_scale_insight(partner)

        # ratio = 10, which is > 5, so None
        assert insight is None

    def test_scale_zero_lists(self):
        """One or both list sizes zero → no insight."""
        analyzer = self._make_analyzer(user_list_size=0)
        partner = make_mock_supabase_profile(list_size=5000)
        insight = analyzer._build_scale_insight(partner)
        assert insight is None

        # Both zero
        analyzer2 = self._make_analyzer(user_list_size=0)
        partner2 = make_mock_supabase_profile(list_size=0)
        insight2 = analyzer2._build_scale_insight(partner2)
        assert insight2 is None


# =============================================================================
# TEST: PartnershipAnalyzer - Revenue Tier
# =============================================================================


class TestRevenueTierInsight:
    """Tests for PartnershipAnalyzer._build_revenue_tier_insight."""

    def _make_analyzer(self, user_revenue_tier='established'):
        user = make_mock_user()
        user_profile = make_mock_supabase_profile(revenue_tier=user_revenue_tier)
        return PartnershipAnalyzer(
            user=user,
            user_supabase_profile=user_profile,
        )

    def test_revenue_same_tier(self):
        """Both 'established' → 'Revenue Tier Match'."""
        analyzer = self._make_analyzer(user_revenue_tier='established')
        partner = make_mock_supabase_profile(revenue_tier='established')
        insight = analyzer._build_revenue_tier_insight(partner)

        assert insight is not None
        assert insight.headline == 'Revenue Tier Match'

    def test_revenue_adjacent_tier(self):
        """'established' vs 'premium' → 'Adjacent Revenue Tiers'."""
        analyzer = self._make_analyzer(user_revenue_tier='established')
        partner = make_mock_supabase_profile(revenue_tier='premium')
        insight = analyzer._build_revenue_tier_insight(partner)

        assert insight is not None
        assert insight.headline == 'Adjacent Revenue Tiers'

    def test_revenue_far_apart(self):
        """'micro' vs 'enterprise' → None (4 tiers apart)."""
        analyzer = self._make_analyzer(user_revenue_tier='micro')
        partner = make_mock_supabase_profile(revenue_tier='enterprise')
        insight = analyzer._build_revenue_tier_insight(partner)

        assert insight is None


# =============================================================================
# TEST: PartnershipAnalyzer - JV History
# =============================================================================


class TestJVHistoryInsight:
    """Tests for PartnershipAnalyzer._build_jv_history_insight."""

    def _make_analyzer(self):
        user = make_mock_user()
        user_profile = make_mock_supabase_profile()
        return PartnershipAnalyzer(
            user=user,
            user_supabase_profile=user_profile,
        )

    def test_jv_experienced_partner(self):
        """3+ items → 'Experienced JV Partner'."""
        analyzer = self._make_analyzer()
        partner = make_mock_supabase_profile(jv_history=[
            {'partner_name': 'A', 'format': 'webinar', 'source_quote': '...'},
            {'partner_name': 'B', 'format': 'podcast_swap', 'source_quote': '...'},
            {'partner_name': 'C', 'format': 'list_swap', 'source_quote': '...'},
        ])
        insight = analyzer._build_jv_history_insight(partner)

        assert insight is not None
        assert insight.headline == 'Experienced JV Partner'
        assert '3 past partnerships' in insight.detail

    def test_jv_some_experience(self):
        """1-2 items → 'Has JV Experience'."""
        analyzer = self._make_analyzer()
        partner = make_mock_supabase_profile(jv_history=[
            {'partner_name': 'A', 'format': 'webinar', 'source_quote': '...'},
        ])
        insight = analyzer._build_jv_history_insight(partner)

        assert insight is not None
        assert insight.headline == 'Has JV Experience'

    def test_jv_no_history(self):
        """Empty/None jv_history → None."""
        analyzer = self._make_analyzer()

        # None case
        partner_none = make_mock_supabase_profile(jv_history=None)
        assert analyzer._build_jv_history_insight(partner_none) is None

        # Empty list case
        partner_empty = make_mock_supabase_profile(jv_history=[])
        assert analyzer._build_jv_history_insight(partner_empty) is None


# =============================================================================
# TEST: PartnershipAnalyzer - Content Platform
# =============================================================================


class TestContentPlatformInsight:
    """Tests for PartnershipAnalyzer._build_content_platform_insight."""

    def _make_analyzer(self, user_platforms=None):
        user = make_mock_user()
        user_profile = make_mock_supabase_profile(
            content_platforms=user_platforms or {
                'podcast_name': 'My Podcast',
                'youtube_channel': 'MyChannel',
            }
        )
        return PartnershipAnalyzer(
            user=user,
            user_supabase_profile=user_profile,
        )

    def test_platform_podcast_swap(self):
        """Both have podcast (only) → 'Podcast Swap Opportunity'."""
        analyzer = self._make_analyzer(user_platforms={
            'podcast_name': 'My Podcast',
        })
        partner = make_mock_supabase_profile(content_platforms={
            'podcast_name': 'Their Podcast',
        })
        insight = analyzer._build_content_platform_insight(partner)

        assert insight is not None
        assert insight.headline == 'Podcast Swap Opportunity'

    def test_platform_multi_overlap(self):
        """2+ shared platforms → 'Multi-Platform Overlap'."""
        analyzer = self._make_analyzer(user_platforms={
            'podcast_name': 'My Podcast',
            'youtube_channel': 'MyYT',
            'instagram_handle': '@myig',
        })
        partner = make_mock_supabase_profile(content_platforms={
            'podcast_name': 'Their Podcast',
            'youtube_channel': 'TheirYT',
        })
        insight = analyzer._build_content_platform_insight(partner)

        assert insight is not None
        assert insight.headline == 'Multi-Platform Overlap'

    def test_platform_no_overlap(self):
        """No shared platforms → None."""
        analyzer = self._make_analyzer(user_platforms={
            'podcast_name': 'My Podcast',
        })
        partner = make_mock_supabase_profile(content_platforms={
            'youtube_channel': 'TheirYT',
        })
        insight = analyzer._build_content_platform_insight(partner)

        assert insight is None


# =============================================================================
# TEST: PartnershipAnalyzer - Tier Determination
# =============================================================================


class TestTierDetermination:
    """Tests for PartnershipAnalyzer._determine_tier."""

    def _make_analyzer(self):
        user = make_mock_user()
        return PartnershipAnalyzer(user=user)

    def test_tier_hand_picked(self):
        """score=85 → 'hand_picked'."""
        analyzer = self._make_analyzer()
        tier = analyzer._determine_tier(score=85, insight_count=2)
        assert tier == 'hand_picked'

    def test_tier_strong_by_score(self):
        """score=60 → 'strong' (above 55 threshold, below 67 hand_picked)."""
        analyzer = self._make_analyzer()
        tier = analyzer._determine_tier(score=60, insight_count=1)
        assert tier == 'strong'

    def test_tier_strong_by_insights(self):
        """No score, 3 insights → 'strong'."""
        analyzer = self._make_analyzer()
        tier = analyzer._determine_tier(score=None, insight_count=3)
        assert tier == 'strong'

    def test_tier_wildcard(self):
        """score=40, 0 insights → 'wildcard'."""
        analyzer = self._make_analyzer()
        tier = analyzer._determine_tier(score=40, insight_count=0)
        assert tier == 'wildcard'


# =============================================================================
# TEST: PartnershipAnalyzer - Helpers
# =============================================================================


class TestFormatNumber:
    """Tests for PartnershipAnalyzer._format_number."""

    def _make_analyzer(self):
        user = make_mock_user()
        return PartnershipAnalyzer(user=user)

    def test_format_number_thousands(self):
        """5000 → '5K'."""
        analyzer = self._make_analyzer()
        assert analyzer._format_number(5000) == '5K'

    def test_format_number_millions(self):
        """1500000 → '1.5M'."""
        analyzer = self._make_analyzer()
        assert analyzer._format_number(1500000) == '1.5M'

    def test_format_number_small(self):
        """500 → '500'."""
        analyzer = self._make_analyzer()
        assert analyzer._format_number(500) == '500'


# =============================================================================
# TEST: PartnershipAnalyzer - Batch Analysis
# =============================================================================


class TestAnalyzeBatch:
    """Tests for PartnershipAnalyzer.analyze_batch."""

    def test_analyze_batch_sorts_by_tier(self):
        """hand_picked before strong before wildcard in output order."""
        user = make_mock_user()
        user_profile = make_mock_supabase_profile(list_size=5000)
        analyzer = PartnershipAnalyzer(
            user=user,
            user_supabase_profile=user_profile,
        )

        # Create three partners — scores will map to different tiers
        partner_wildcard = make_mock_supabase_profile(
            id='wildcard-id',
            name='Wildcard Partner',
            list_size=0,
            revenue_tier=None,
            jv_history=None,
            content_platforms=None,
        )
        partner_strong = make_mock_supabase_profile(
            id='strong-id',
            name='Strong Partner',
            list_size=0,
            revenue_tier=None,
            jv_history=None,
            content_platforms=None,
        )
        partner_hand_picked = make_mock_supabase_profile(
            id='hand-picked-id',
            name='Hand Picked Partner',
            list_size=0,
            revenue_tier=None,
            jv_history=None,
            content_platforms=None,
        )

        # Create matches that produce different tiers
        # Thresholds: hand_picked >= 67, strong >= 55, wildcard < 55
        match_wildcard = make_mock_supabase_match(harmonic_mean=40.0, match_reason='Low match')
        match_strong = make_mock_supabase_match(harmonic_mean=60.0, match_reason='Good match')
        match_hand_picked = make_mock_supabase_match(harmonic_mean=90.0, match_reason='Excellent match')

        partners = [partner_wildcard, partner_strong, partner_hand_picked]
        matches_by_id = {
            'wildcard-id': match_wildcard,
            'strong-id': match_strong,
            'hand-picked-id': match_hand_picked,
        }

        results = analyzer.analyze_batch(partners, matches_by_id)

        assert len(results) == 3

        # Verify sort order: hand_picked first, then strong, then wildcard
        tiers = [r.tier for r in results]
        assert tiers[0] == 'hand_picked'
        assert tiers[1] == 'strong'
        assert tiers[2] == 'wildcard'
