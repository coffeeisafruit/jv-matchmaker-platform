"""
Tests for SupabaseMatchScoringService.score_pair_lightweight

Verification item #4: "Pre-scoring accuracy — score 50 prospects with
ISMC lightweight vs full, measure correlation."

Covers:
- Dict shape and field presence
- Score range (0-100)
- Zero result when both profiles lack names
- Good-match vs bad-match directional scoring
- booking_link influence on intent score
- Profile completeness influence on context score
- is_lightweight flag always True
- Bulk scoring (20+ pairs) without errors
- Correlation between lightweight and full scoring rankings

All tests are pure Python with mocked objects — no database access required.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import random
import statistics
from unittest.mock import MagicMock

import pytest

from matching.services import SupabaseMatchScoringService


# =============================================================================
# HELPER: Mock SupabaseProfile Factory
# =============================================================================

# All fields accessed by score_pair_lightweight AND score_pair (full)
_ALL_FIELDS = [
    'name', 'booking_link', 'seeking', 'offering', 'what_you_do',
    'who_you_serve', 'email', 'company', 'website', 'linkedin',
    'niche', 'signature_programs', 'revenue_tier', 'list_size',
    'social_reach', 'bio', 'tags', 'jv_history', 'content_platforms',
    'audience_type', 'current_projects',
    # Additional fields accessed by full scoring
    'network_role', 'phone', 'audience_engagement_score',
    # Embedding fields (None by default so full scoring falls back to text)
    'embedding_seeking', 'embedding_offering',
    'embedding_what_you_do', 'embedding_who_you_serve',
]


def make_supabase_profile(**overrides):
    """
    Create a MagicMock SupabaseProfile with all required attributes.

    Defaults to a moderately-filled profile.  Pass keyword arguments to
    override any field.
    """
    defaults = {
        'name': 'Test Partner',
        'booking_link': None,
        'seeking': 'JV webinar partners and podcast swaps',
        'offering': 'Email marketing automation and list building',
        'what_you_do': 'Help coaches grow their email lists through automation',
        'who_you_serve': 'Online coaches and course creators',
        'email': 'test@example.com',
        'company': 'Test Inc',
        'website': 'https://test.com',
        'linkedin': 'https://linkedin.com/in/test',
        'niche': 'Digital marketing',
        'signature_programs': 'List Building Masterclass',
        'revenue_tier': 'established',
        'list_size': 5000,
        'social_reach': 10000,
        'bio': 'Marketing expert helping coaches grow online.',
        'tags': ['marketing', 'automation', 'coaching'],
        'jv_history': [
            {'partner_name': 'Partner A', 'format': 'webinar'},
            {'partner_name': 'Partner B', 'format': 'podcast_swap'},
        ],
        'content_platforms': {
            'podcast_name': 'The Marketing Show',
            'youtube_channel': 'MarketingTV',
        },
        'audience_type': 'coaches',
        'current_projects': 'Launching a new course on email automation',
        'network_role': 'educator',
        'phone': None,
        'audience_engagement_score': 0.65,
        'embedding_seeking': None,
        'embedding_offering': None,
        'embedding_what_you_do': None,
        'embedding_who_you_serve': None,
    }
    defaults.update(overrides)

    mock = MagicMock()
    for attr, value in defaults.items():
        setattr(mock, attr, value)
    return mock


def make_bare_profile(**overrides):
    """Create a minimal/empty profile — all fields None or empty."""
    bare = {field: None for field in _ALL_FIELDS}
    bare['tags'] = []
    bare['jv_history'] = []
    bare.update(overrides)

    mock = MagicMock()
    for attr, value in bare.items():
        setattr(mock, attr, value)
    return mock


# =============================================================================
# TEST 1: Dict shape
# =============================================================================


class TestLightweightDictShape:
    """score_pair_lightweight returns the expected dict keys."""

    def test_returns_expected_keys(self):
        """Result dict must contain score_ab, score_ba, harmonic_mean, is_lightweight."""
        service = SupabaseMatchScoringService()
        a = make_supabase_profile(name='Alice')
        b = make_supabase_profile(name='Bob')
        result = service.score_pair_lightweight(a, b)

        assert isinstance(result, dict)
        expected_keys = {'score_ab', 'score_ba', 'harmonic_mean', 'is_lightweight'}
        assert expected_keys == set(result.keys())

    def test_values_are_numeric(self):
        """score_ab, score_ba, harmonic_mean must be int or float."""
        service = SupabaseMatchScoringService()
        a = make_supabase_profile(name='Alice')
        b = make_supabase_profile(name='Bob')
        result = service.score_pair_lightweight(a, b)

        for key in ('score_ab', 'score_ba', 'harmonic_mean'):
            assert isinstance(result[key], (int, float)), f"{key} is not numeric"


# =============================================================================
# TEST 2: Score range 0-100
# =============================================================================


class TestLightweightScoreRange:
    """All scores must be in 0-100."""

    def test_scores_in_range(self):
        service = SupabaseMatchScoringService()
        a = make_supabase_profile()
        b = make_supabase_profile()
        result = service.score_pair_lightweight(a, b)

        for key in ('score_ab', 'score_ba', 'harmonic_mean'):
            assert 0 <= result[key] <= 100, f"{key}={result[key]} out of range"

    def test_bare_profiles_scores_in_range(self):
        """Even minimal profiles should produce in-range scores."""
        service = SupabaseMatchScoringService()
        a = make_bare_profile(name='A')
        b = make_bare_profile(name='B')
        result = service.score_pair_lightweight(a, b)

        for key in ('score_ab', 'score_ba', 'harmonic_mean'):
            assert 0 <= result[key] <= 100, f"{key}={result[key]} out of range"


# =============================================================================
# TEST 3: Zero result when both profiles lack names
# =============================================================================


class TestLightweightZeroOnMissingNames:
    """Both profiles lacking names -> zero result."""

    def test_both_names_none(self):
        service = SupabaseMatchScoringService()
        a = make_bare_profile(name=None)
        b = make_bare_profile(name=None)
        result = service.score_pair_lightweight(a, b)

        assert result['score_ab'] == 0
        assert result['score_ba'] == 0
        assert result['harmonic_mean'] == 0
        assert result['is_lightweight'] is True

    def test_both_names_empty_string(self):
        service = SupabaseMatchScoringService()
        a = make_bare_profile(name='')
        b = make_bare_profile(name='  ')
        result = service.score_pair_lightweight(a, b)

        assert result['score_ab'] == 0
        assert result['score_ba'] == 0
        assert result['harmonic_mean'] == 0

    def test_one_name_present_does_not_zero(self):
        """If at least one profile has a name, scoring proceeds normally."""
        service = SupabaseMatchScoringService()
        a = make_supabase_profile(name='Alice')
        b = make_bare_profile(name=None)
        result = service.score_pair_lightweight(a, b)

        # Should NOT be zero — at least one name is present
        assert result['is_lightweight'] is True
        # score_ab or score_ba should be > 0 since Alice has data
        assert result['score_ab'] > 0 or result['score_ba'] > 0


# =============================================================================
# TEST 4: Good match scores higher than bad match
# =============================================================================


class TestLightweightGoodVsBadMatch:
    """Complementary seeking/offering should score higher than mismatched."""

    def test_complementary_pair_beats_mismatched(self):
        service = SupabaseMatchScoringService()

        # Good match: A seeks what B offers
        good_a = make_supabase_profile(
            name='Alice',
            seeking='podcast guest swaps and email list building partnerships',
            offering='Facebook ads management and funnel optimization',
        )
        good_b = make_supabase_profile(
            name='Bob',
            seeking='Facebook advertising and funnel building help',
            offering='Podcast guest swaps and email list building partnerships',
        )

        # Bad match: completely unrelated domains
        bad_a = make_supabase_profile(
            name='Carol',
            seeking='industrial welding equipment suppliers',
            offering='Commercial real estate brokerage services',
        )
        bad_b = make_supabase_profile(
            name='Dave',
            seeking='Quantum computing research collaborators',
            offering='Deep sea fishing charter boat rentals',
        )

        good_result = service.score_pair_lightweight(good_a, good_b)
        bad_result = service.score_pair_lightweight(bad_a, bad_b)

        assert good_result['harmonic_mean'] > bad_result['harmonic_mean'], (
            f"Good match ({good_result['harmonic_mean']}) should beat "
            f"bad match ({bad_result['harmonic_mean']})"
        )

    def test_symmetric_complement_scores_well(self):
        """When A.seeking matches B.offering AND vice versa, harmonic mean is high."""
        service = SupabaseMatchScoringService()
        a = make_supabase_profile(
            name='Alpha',
            seeking='content creation and podcast guest opportunities',
            offering='Marketing automation and email list building',
        )
        b = make_supabase_profile(
            name='Beta',
            seeking='marketing automation and email list building partners',
            offering='Content creation and podcast guest opportunities',
        )
        result = service.score_pair_lightweight(a, b)

        # Both directional scores should be relatively balanced
        diff = abs(result['score_ab'] - result['score_ba'])
        assert diff < 30, f"Symmetric pair should be balanced, got diff={diff}"


# =============================================================================
# TEST 5: booking_link increases intent score
# =============================================================================


class TestLightweightBookingLink:
    """Presence of booking_link should increase the score."""

    def test_booking_link_boosts_score(self):
        service = SupabaseMatchScoringService()

        with_booking = make_supabase_profile(
            name='Booked',
            booking_link='https://calendly.com/booked',
        )
        without_booking = make_supabase_profile(
            name='Unbooked',
            booking_link=None,
        )
        # Use the same counterpart for both
        counterpart = make_supabase_profile(name='Counter')

        result_with = service.score_pair_lightweight(counterpart, with_booking)
        result_without = service.score_pair_lightweight(counterpart, without_booking)

        # score_ab measures "how valuable is B for A" — B is the target
        # so the booking_link on B should boost score_ab
        assert result_with['score_ab'] > result_without['score_ab'], (
            f"Booking link should boost score: "
            f"with={result_with['score_ab']}, without={result_without['score_ab']}"
        )

    def test_empty_booking_link_treated_as_absent(self):
        """An empty/whitespace booking_link should not boost the score."""
        service = SupabaseMatchScoringService()

        empty_booking = make_supabase_profile(
            name='Empty',
            booking_link='   ',
        )
        no_booking = make_supabase_profile(
            name='None',
            booking_link=None,
        )
        counterpart = make_supabase_profile(name='Counter')

        result_empty = service.score_pair_lightweight(counterpart, empty_booking)
        result_none = service.score_pair_lightweight(counterpart, no_booking)

        # Both should produce similar scores
        diff = abs(result_empty['score_ab'] - result_none['score_ab'])
        assert diff < 1.0, f"Empty booking_link should equal None, diff={diff}"


# =============================================================================
# TEST 6: Profile completeness affects context score
# =============================================================================


class TestLightweightProfileCompleteness:
    """More complete profiles should score higher via the context dimension."""

    def test_complete_profile_beats_sparse(self):
        service = SupabaseMatchScoringService()

        complete = make_supabase_profile(
            name='Complete',
            email='complete@example.com',
            company='Full Corp',
            website='https://full.com',
            linkedin='https://linkedin.com/in/full',
            niche='Marketing',
            what_you_do='Everything marketing related',
            who_you_serve='All marketers everywhere',
            seeking='JV partners for webinars',
            offering='Email list building services',
            booking_link='https://calendly.com/full',
            revenue_tier='established',
        )
        sparse = make_supabase_profile(
            name='Sparse',
            email=None,
            company=None,
            website=None,
            linkedin=None,
            niche=None,
            what_you_do=None,
            who_you_serve=None,
            seeking='JV partners',
            offering='services',
            booking_link=None,
            revenue_tier=None,
        )

        # Score them against the same counterpart
        counterpart = make_supabase_profile(
            name='Counter',
            seeking='Marketing help and email list building',
        )

        result_complete = service.score_pair_lightweight(counterpart, complete)
        result_sparse = service.score_pair_lightweight(counterpart, sparse)

        assert result_complete['score_ab'] > result_sparse['score_ab'], (
            f"Complete profile ({result_complete['score_ab']}) should outscore "
            f"sparse profile ({result_sparse['score_ab']})"
        )


# =============================================================================
# TEST 7: is_lightweight is always True
# =============================================================================


class TestLightweightFlag:
    """is_lightweight must be True in all cases."""

    def test_flag_on_normal_result(self):
        service = SupabaseMatchScoringService()
        a = make_supabase_profile(name='A')
        b = make_supabase_profile(name='B')
        result = service.score_pair_lightweight(a, b)
        assert result['is_lightweight'] is True

    def test_flag_on_zero_result(self):
        service = SupabaseMatchScoringService()
        a = make_bare_profile(name=None)
        b = make_bare_profile(name=None)
        result = service.score_pair_lightweight(a, b)
        assert result['is_lightweight'] is True

    def test_flag_on_bare_profiles(self):
        service = SupabaseMatchScoringService()
        a = make_bare_profile(name='Bare A')
        b = make_bare_profile(name='Bare B')
        result = service.score_pair_lightweight(a, b)
        assert result['is_lightweight'] is True


# =============================================================================
# TEST 8: Bulk scoring (20+ mock profile pairs)
# =============================================================================

# Profile templates with varying completeness levels
_COMPLETENESS_LEVELS = {
    'full': dict(
        email='full@test.com',
        company='Full Corp',
        website='https://full.com',
        linkedin='https://linkedin.com/in/full',
        niche='Digital marketing',
        what_you_do='Help coaches build email lists',
        who_you_serve='Online coaches and consultants',
        seeking='JV webinar partners and list swaps',
        offering='Email marketing automation services',
        booking_link='https://calendly.com/full',
        revenue_tier='established',
        bio='Experienced marketer with 10 years in the field.',
        tags=['marketing', 'coaching', 'automation'],
        jv_history=[
            {'partner_name': 'P1', 'format': 'webinar'},
            {'partner_name': 'P2', 'format': 'podcast_swap'},
            {'partner_name': 'P3', 'format': 'list_swap'},
        ],
        content_platforms={'podcast_name': 'The Show', 'youtube_channel': 'TheYT'},
        audience_type='coaches',
        current_projects='Building a new funnel system',
        network_role='educator',
        social_reach=25000,
        list_size=10000,
        audience_engagement_score=0.7,
        signature_programs='List Building Masterclass',
        phone='555-1234',
    ),
    'moderate': dict(
        email='mod@test.com',
        company='Mod LLC',
        website='https://mod.com',
        linkedin=None,
        niche='Business coaching',
        what_you_do='Business coaching for startups',
        who_you_serve='Startup founders',
        seeking='Speaking opportunities',
        offering='Business strategy consulting',
        booking_link=None,
        revenue_tier='emerging',
        bio='Coach and consultant.',
        tags=['coaching'],
        jv_history=[{'partner_name': 'P1', 'format': 'webinar'}],
        content_platforms=None,
        audience_type='founders',
        current_projects=None,
        network_role='coach',
        social_reach=2000,
        list_size=1500,
        audience_engagement_score=0.4,
        signature_programs=None,
        phone=None,
    ),
    'sparse': dict(
        email=None,
        company=None,
        website=None,
        linkedin=None,
        niche=None,
        what_you_do=None,
        who_you_serve=None,
        seeking='Looking for partners',
        offering='Services',
        booking_link=None,
        revenue_tier=None,
        bio=None,
        tags=[],
        jv_history=[],
        content_platforms=None,
        audience_type=None,
        current_projects=None,
        network_role=None,
        social_reach=None,
        list_size=None,
        audience_engagement_score=None,
        signature_programs=None,
        phone=None,
    ),
}


def _generate_bulk_pairs(n=25):
    """Generate n profile pairs with varying completeness."""
    levels = list(_COMPLETENESS_LEVELS.keys())
    random.seed(42)  # Reproducible
    pairs = []
    for i in range(n):
        level_a = random.choice(levels)
        level_b = random.choice(levels)
        a = make_supabase_profile(
            name=f'Profile_A_{i}',
            **_COMPLETENESS_LEVELS[level_a],
        )
        b = make_supabase_profile(
            name=f'Profile_B_{i}',
            **_COMPLETENESS_LEVELS[level_b],
        )
        pairs.append((a, b))
    return pairs


class TestLightweightBulkScoring:
    """Run 25 profile pairs through both lightweight and full scoring."""

    @pytest.fixture(scope='class')
    def bulk_pairs(self):
        return _generate_bulk_pairs(25)

    def test_lightweight_does_not_error(self, bulk_pairs):
        """All 25 pairs score without exceptions."""
        service = SupabaseMatchScoringService()
        for a, b in bulk_pairs:
            result = service.score_pair_lightweight(a, b)
            assert isinstance(result, dict)
            assert 'score_ab' in result

    def test_full_does_not_error(self, bulk_pairs):
        """All 25 pairs score with full method without exceptions."""
        service = SupabaseMatchScoringService()
        for a, b in bulk_pairs:
            result = service.score_pair(a, b)
            assert isinstance(result, dict)
            assert 'score_ab' in result

    def test_all_lightweight_scores_in_range(self, bulk_pairs):
        """Every lightweight score in 0-100."""
        service = SupabaseMatchScoringService()
        for a, b in bulk_pairs:
            result = service.score_pair_lightweight(a, b)
            for key in ('score_ab', 'score_ba', 'harmonic_mean'):
                assert 0 <= result[key] <= 100, (
                    f"{a.name} vs {b.name}: {key}={result[key]}"
                )

    def test_all_full_scores_in_range(self, bulk_pairs):
        """Every full score in 0-100."""
        service = SupabaseMatchScoringService()
        for a, b in bulk_pairs:
            result = service.score_pair(a, b)
            for key in ('score_ab', 'score_ba', 'harmonic_mean'):
                assert 0 <= result[key] <= 100, (
                    f"{a.name} vs {b.name}: {key}={result[key]}"
                )

    def test_all_lightweight_have_flag(self, bulk_pairs):
        """Every lightweight result has is_lightweight=True."""
        service = SupabaseMatchScoringService()
        for a, b in bulk_pairs:
            result = service.score_pair_lightweight(a, b)
            assert result['is_lightweight'] is True


# =============================================================================
# TEST 9: Lightweight vs Full correlation (KEY TEST)
# =============================================================================

# Intentionally designed profile pairs:
# - Clearly GOOD matches (complementary seeking/offering, complete profiles)
# - Clearly BAD matches (unrelated domains, sparse profiles)
# Both methods should rank them similarly.

_CORRELATION_PROFILES = {
    # --- GOOD matches ---
    'good_1': {
        'a': dict(
            name='Email Expert Alice',
            seeking='Podcast interview opportunities and audience access',
            offering='Email copywriting and automation funnels',
            what_you_do='Write high-converting email sequences for coaches',
            who_you_serve='Health and wellness coaches',
            email='alice@emailpro.com',
            company='EmailPro',
            website='https://emailpro.com',
            linkedin='https://linkedin.com/in/alice',
            niche='Email marketing',
            booking_link='https://calendly.com/alice',
            revenue_tier='established',
            bio='10 years email marketing.',
            tags=['email', 'marketing'],
            jv_history=[{'partner_name': 'X', 'format': 'webinar'}],
            content_platforms={'podcast_name': 'Email Tips'},
            audience_type='coaches',
            current_projects='New email course launch',
            network_role='educator',
            social_reach=15000,
            list_size=8000,
            audience_engagement_score=0.6,
            signature_programs='Email Mastery',
            phone='555-0001',
        ),
        'b': dict(
            name='Podcast Host Bob',
            seeking='Email marketing experts for interviews and list building',
            offering='Podcast interview spots and audience access to coaches',
            what_you_do='Host top podcast for health and wellness coaches',
            who_you_serve='Health and wellness coaches',
            email='bob@podcastpro.com',
            company='PodcastPro',
            website='https://podcastpro.com',
            linkedin='https://linkedin.com/in/bob',
            niche='Podcast production',
            booking_link='https://calendly.com/bob',
            revenue_tier='established',
            bio='Leading podcaster in coaching niche.',
            tags=['podcast', 'coaching'],
            jv_history=[{'partner_name': 'Y', 'format': 'podcast_swap'}],
            content_platforms={'podcast_name': 'Coach Cast'},
            audience_type='coaches',
            current_projects='Season 5 launch',
            network_role='media_content_creator',
            social_reach=20000,
            list_size=12000,
            audience_engagement_score=0.7,
            signature_programs='Podcast Launchpad',
            phone='555-0002',
        ),
    },
    'good_2': {
        'a': dict(
            name='Funnel Builder Fran',
            seeking='Course creators who need funnel optimization help',
            offering='Sales funnel design and optimization',
            what_you_do='Build high-converting sales funnels for course creators',
            who_you_serve='Digital course creators and info-product sellers',
            email='fran@funnels.com',
            company='FunnelWorks',
            website='https://funnelworks.com',
            linkedin='https://linkedin.com/in/fran',
            niche='Sales funnels',
            booking_link='https://calendly.com/fran',
            revenue_tier='premium',
            bio='Funnel optimization specialist.',
            tags=['funnels', 'courses'],
            jv_history=[
                {'partner_name': 'Z', 'format': 'webinar'},
                {'partner_name': 'W', 'format': 'list_swap'},
            ],
            content_platforms={'youtube_channel': 'FunnelTips'},
            audience_type='course_creators',
            current_projects='Funnel template library',
            network_role='service_provider',
            social_reach=8000,
            list_size=6000,
            audience_engagement_score=0.55,
            signature_programs='Funnel in a Day',
            phone='555-0003',
        ),
        'b': dict(
            name='Course Creator Chris',
            seeking='Sales funnel design help and funnel optimization partners',
            offering='Online courses on digital marketing and course creation',
            what_you_do='Teach digital marketing and course creation',
            who_you_serve='Aspiring course creators and digital entrepreneurs',
            email='chris@courses.com',
            company='CourseHQ',
            website='https://coursehq.com',
            linkedin='https://linkedin.com/in/chris',
            niche='Course creation',
            booking_link='https://calendly.com/chris',
            revenue_tier='premium',
            bio='Helped 500+ people launch courses.',
            tags=['courses', 'marketing'],
            jv_history=[{'partner_name': 'V', 'format': 'webinar'}],
            content_platforms={'youtube_channel': 'CourseBuilder'},
            audience_type='entrepreneurs',
            current_projects='New masterclass series',
            network_role='educator',
            social_reach=30000,
            list_size=15000,
            audience_engagement_score=0.65,
            signature_programs='Course Launch Formula',
            phone='555-0004',
        ),
    },
    'good_3': {
        'a': dict(
            name='Community Builder Claire',
            seeking='Workshop facilitators and group coaching experts',
            offering='Access to active community of 5000 coaches',
            what_you_do='Run the largest online community for life coaches',
            who_you_serve='Life coaches building online practices',
            email='claire@community.com',
            company='CoachCommunity',
            website='https://coachcommunity.com',
            linkedin='https://linkedin.com/in/claire',
            niche='Community building',
            booking_link='https://calendly.com/claire',
            revenue_tier='established',
            bio='Community builder extraordinaire.',
            tags=['community', 'coaching'],
            jv_history=[
                {'partner_name': 'Q', 'format': 'webinar'},
                {'partner_name': 'R', 'format': 'list_swap'},
                {'partner_name': 'S', 'format': 'summit'},
            ],
            content_platforms={'facebook_group': 'CoachConnect'},
            audience_type='coaches',
            current_projects='Virtual summit planning',
            network_role='community builder',
            social_reach=18000,
            list_size=9000,
            audience_engagement_score=0.75,
            signature_programs='Community Mastery',
            phone='555-0005',
        ),
        'b': dict(
            name='Workshop Expert Will',
            seeking='Communities where I can run workshops for coaches',
            offering='Group coaching workshops and facilitation for coaches',
            what_you_do='Design and facilitate group coaching workshops',
            who_you_serve='Coaches who want to add group programs',
            email='will@workshops.com',
            company='WorkshopPro',
            website='https://workshoppro.com',
            linkedin='https://linkedin.com/in/will',
            niche='Workshop facilitation',
            booking_link='https://calendly.com/will',
            revenue_tier='established',
            bio='Facilitation expert with 15 years experience.',
            tags=['workshops', 'coaching', 'facilitation'],
            jv_history=[{'partner_name': 'T', 'format': 'webinar'}],
            content_platforms={'podcast_name': 'Workshop Wisdom'},
            audience_type='coaches',
            current_projects='New workshop curriculum',
            network_role='educator',
            social_reach=7000,
            list_size=4000,
            audience_engagement_score=0.6,
            signature_programs='Workshop Blueprint',
            phone='555-0006',
        ),
    },
    # --- BAD matches ---
    'bad_1': {
        'a': dict(
            name='Welder Wayne',
            seeking='Industrial equipment distributors for metalwork supplies',
            offering='Structural steel welding and fabrication services',
            what_you_do='Industrial welding and steel fabrication',
            who_you_serve='Construction companies and manufacturers',
            email='wayne@welding.com',
            company='WayneWeld',
            website='https://wayneweld.com',
            linkedin=None,
            niche='Industrial welding',
            booking_link=None,
            revenue_tier='micro',
            bio=None,
            tags=['welding'],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
        'b': dict(
            name='Yoga Yolanda',
            seeking='Retreat center partnerships in tropical locations',
            offering='Prenatal yoga teacher training certification programs',
            what_you_do='Train prenatal yoga teachers internationally',
            who_you_serve='Aspiring prenatal yoga instructors',
            email='yolanda@yoga.com',
            company='YogaMama',
            website='https://yogamama.com',
            linkedin=None,
            niche='Prenatal yoga',
            booking_link=None,
            revenue_tier='micro',
            bio=None,
            tags=['yoga'],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
    },
    'bad_2': {
        'a': dict(
            name='Sparse Sam',
            seeking=None,
            offering=None,
            what_you_do=None,
            who_you_serve=None,
            email=None,
            company=None,
            website=None,
            linkedin=None,
            niche=None,
            booking_link=None,
            revenue_tier=None,
            bio=None,
            tags=[],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
        'b': dict(
            name='Sparse Sally',
            seeking=None,
            offering=None,
            what_you_do=None,
            who_you_serve=None,
            email=None,
            company=None,
            website=None,
            linkedin=None,
            niche=None,
            booking_link=None,
            revenue_tier=None,
            bio=None,
            tags=[],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
    },
    'bad_3': {
        'a': dict(
            name='Plumber Pete',
            seeking='Wholesale plumbing supply chain distributors',
            offering='Commercial plumbing installation and maintenance',
            what_you_do='Fix commercial plumbing systems',
            who_you_serve='Property management companies',
            email=None,
            company='PetePlumbing',
            website=None,
            linkedin=None,
            niche='Plumbing',
            booking_link=None,
            revenue_tier=None,
            bio=None,
            tags=[],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
        'b': dict(
            name='Astrologer Ana',
            seeking='Crystal healing practitioners for joint readings',
            offering='Astrology chart readings and horoscope publishing',
            what_you_do='Publish horoscopes and provide astrology readings',
            who_you_serve='Spiritual seekers and new age enthusiasts',
            email=None,
            company='StarSigns',
            website=None,
            linkedin=None,
            niche='Astrology',
            booking_link=None,
            revenue_tier=None,
            bio=None,
            tags=[],
            jv_history=[],
            content_platforms=None,
            audience_type=None,
            current_projects=None,
            network_role=None,
            social_reach=None,
            list_size=None,
            audience_engagement_score=None,
            signature_programs=None,
            phone=None,
        ),
    },
}


class TestLightweightVsFullCorrelation:
    """
    KEY TEST: Both scoring methods should agree on which pairs are
    high-quality vs low-quality matches.

    We use intentionally designed good and bad pairs. The correlation
    test verifies ranking agreement, not exact score equality.
    """

    @pytest.fixture(scope='class')
    def scored_pairs(self):
        """Score all correlation profiles with both methods."""
        service = SupabaseMatchScoringService()
        results = {}
        for pair_name, pair_data in _CORRELATION_PROFILES.items():
            a = make_supabase_profile(**pair_data['a'])
            b = make_supabase_profile(**pair_data['b'])

            lw = service.score_pair_lightweight(a, b)
            full = service.score_pair(a, b)
            results[pair_name] = {
                'lightweight': lw,
                'full': full,
                'is_good': pair_name.startswith('good_'),
            }
        return results

    def test_good_pairs_score_higher_lightweight(self, scored_pairs):
        """Good pairs should have higher lightweight harmonic_mean than bad pairs."""
        good_scores = [
            v['lightweight']['harmonic_mean']
            for v in scored_pairs.values() if v['is_good']
        ]
        bad_scores = [
            v['lightweight']['harmonic_mean']
            for v in scored_pairs.values() if not v['is_good']
        ]

        avg_good = statistics.mean(good_scores)
        avg_bad = statistics.mean(bad_scores)
        assert avg_good > avg_bad, (
            f"Lightweight: avg good ({avg_good:.2f}) should beat avg bad ({avg_bad:.2f})"
        )

    def test_good_pairs_score_higher_full(self, scored_pairs):
        """Good pairs should have higher full harmonic_mean than bad pairs."""
        good_scores = [
            v['full']['harmonic_mean']
            for v in scored_pairs.values() if v['is_good']
        ]
        bad_scores = [
            v['full']['harmonic_mean']
            for v in scored_pairs.values() if not v['is_good']
        ]

        avg_good = statistics.mean(good_scores)
        avg_bad = statistics.mean(bad_scores)
        assert avg_good > avg_bad, (
            f"Full: avg good ({avg_good:.2f}) should beat avg bad ({avg_bad:.2f})"
        )

    def test_ranking_agreement(self, scored_pairs):
        """
        Both methods should rank good pairs above bad pairs.

        Specifically: the lowest-scoring good pair should score higher
        than the highest-scoring bad pair, for BOTH methods.
        """
        good_lw = [
            v['lightweight']['harmonic_mean']
            for v in scored_pairs.values() if v['is_good']
        ]
        bad_lw = [
            v['lightweight']['harmonic_mean']
            for v in scored_pairs.values() if not v['is_good']
        ]
        good_full = [
            v['full']['harmonic_mean']
            for v in scored_pairs.values() if v['is_good']
        ]
        bad_full = [
            v['full']['harmonic_mean']
            for v in scored_pairs.values() if not v['is_good']
        ]

        # Lightweight: min good > max bad
        assert min(good_lw) > max(bad_lw), (
            f"Lightweight ranking broken: min good ({min(good_lw):.2f}) "
            f"<= max bad ({max(bad_lw):.2f})"
        )
        # Full: min good > max bad
        assert min(good_full) > max(bad_full), (
            f"Full ranking broken: min good ({min(good_full):.2f}) "
            f"<= max bad ({max(bad_full):.2f})"
        )

    def test_spearman_rank_correlation(self, scored_pairs):
        """
        Spearman rank-order correlation between lightweight and full
        harmonic_mean scores should be positive and strong (>= 0.7).

        This is the core correlation metric — it measures whether the
        two methods AGREE on the ORDERING of pairs, not exact values.
        """
        pair_names = sorted(scored_pairs.keys())
        lw_scores = [scored_pairs[p]['lightweight']['harmonic_mean'] for p in pair_names]
        full_scores = [scored_pairs[p]['full']['harmonic_mean'] for p in pair_names]

        # Compute Spearman correlation manually (no scipy dependency)
        n = len(pair_names)
        lw_ranks = _rank_scores(lw_scores)
        full_ranks = _rank_scores(full_scores)

        d_sq_sum = sum((lw_ranks[i] - full_ranks[i]) ** 2 for i in range(n))
        rho = 1 - (6 * d_sq_sum) / (n * (n ** 2 - 1))

        assert rho >= 0.7, (
            f"Spearman correlation too low: rho={rho:.3f} (need >= 0.7). "
            f"Lightweight and full scoring disagree on ranking.\n"
            f"LW scores:   {[f'{s:.1f}' for s in lw_scores]}\n"
            f"Full scores: {[f'{s:.1f}' for s in full_scores]}\n"
            f"LW ranks:    {lw_ranks}\n"
            f"Full ranks:  {full_ranks}"
        )

    def test_no_score_inversion_on_extremes(self, scored_pairs):
        """
        The best good pair and worst bad pair should never swap positions
        between lightweight and full scoring.
        """
        pairs_lw = {
            k: v['lightweight']['harmonic_mean'] for k, v in scored_pairs.items()
        }
        pairs_full = {
            k: v['full']['harmonic_mean'] for k, v in scored_pairs.items()
        }

        # Best good pair by lightweight
        best_good_lw = max(
            (k for k in pairs_lw if scored_pairs[k]['is_good']),
            key=lambda k: pairs_lw[k],
        )
        # Worst bad pair by lightweight
        worst_bad_lw = min(
            (k for k in pairs_lw if not scored_pairs[k]['is_good']),
            key=lambda k: pairs_lw[k],
        )

        # These same pairs should maintain ordering in full scoring
        assert pairs_full[best_good_lw] > pairs_full[worst_bad_lw], (
            f"Score inversion: {best_good_lw} beats {worst_bad_lw} in "
            f"lightweight but not in full scoring"
        )


def _rank_scores(scores):
    """
    Return ranks for a list of scores (1 = highest).
    Ties get average rank.
    """
    n = len(scores)
    indexed = sorted(enumerate(scores), key=lambda x: -x[1])
    ranks = [0.0] * n

    i = 0
    while i < n:
        j = i
        while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1  # 1-indexed
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1

    return ranks
