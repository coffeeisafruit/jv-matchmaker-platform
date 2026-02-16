import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import pytest
from matching.enrichment.match_enrichment import MatchEnrichmentService


CLIENT_PROFILE = {
    'name': 'Janet Bray Attwood',
    'company': 'Enlightened Alliance',
    'what_you_do': 'Helping people discover their passions and purpose',
    'who_you_serve': 'Transformational leaders and coaches',
    'seeking': 'JV partners for summit and course launches',
    'offering': 'Access to 200K+ subscriber list',
    'signature_programs': 'The Passion Test, Passion Test Certification',
    'audience_description': 'engaged audience of transformational seekers',
    'bio': 'NYT bestselling author of The Passion Test',
}


@pytest.fixture
def service():
    return MatchEnrichmentService(CLIENT_PROFILE)


# ---------------------------------------------------------------------------
# _find_audience_overlap tests
# ---------------------------------------------------------------------------

class TestFindAudienceOverlap:
    def test_overlap_with_coaches_and_entrepreneurs(self, service):
        result = service._find_audience_overlap(
            'coaches and entrepreneurs looking for growth',
            'transformational leaders and coaches',
        )
        assert result is True

    def test_no_overlap_with_software_devs(self, service):
        result = service._find_audience_overlap(
            'software developers and data engineers',
            'accountants and financial advisors',
        )
        assert result is False

    def test_one_keyword_not_enough(self, service):
        result = service._find_audience_overlap(
            'yoga practitioners and coach',
            'accountants and lawyers',
        )
        assert result is False


# ---------------------------------------------------------------------------
# _generate_why_fit tests
# ---------------------------------------------------------------------------

class TestGenerateWhyFit:
    def test_audience_alignment_section_when_who_they_serve_present(self, service):
        result = service._generate_why_fit(
            name='Mike Smith',
            company='Peak Performance Inc',
            who_they_serve='entrepreneurs and business coaches',
            what_they_do='Executive coaching',
            seeking='partnerships',
            offering='speaking',
            match_data={},
        )
        assert 'AUDIENCE' in result.upper()
        assert 'Mike' in result or 'Smith' in result

    def test_they_want_this_for_cross_promotion_seeking(self, service):
        result = service._generate_why_fit(
            name='Lisa Chen',
            company='GrowthLab',
            who_they_serve='online entrepreneurs',
            what_they_do='Digital marketing',
            seeking='cross-promotion opportunities',
            offering='email list access',
            match_data={},
        )
        upper = result.upper()
        assert 'THEY WANT THIS' in upper or 'CROSS-PROMOTION' in upper

    def test_they_want_this_for_speaking_seeking(self, service):
        result = service._generate_why_fit(
            name='Tom Harris',
            company='Speaker Academy',
            who_they_serve='aspiring speakers',
            what_they_do='Speaker training',
            seeking='speaking and podcast guest spots',
            offering='stage time',
            match_data={},
        )
        upper = result.upper()
        assert 'THEY WANT THIS' in upper or 'SPEAKING' in upper

    def test_scale_section_for_large_list(self, service):
        result = service._generate_why_fit(
            name='Rachel Green',
            company='Big Reach Media',
            who_they_serve='coaches and consultants',
            what_they_do='List building',
            seeking='affiliate partners',
            offering='massive audience',
            match_data={'list_size': 75000},
        )
        upper = result.upper()
        assert 'SCALE' in upper or '75,000' in result or '75000' in result

    def test_sparse_profile_uses_expertise_fallback(self, service):
        result = service._generate_why_fit(
            name='Unknown Person',
            company='',
            who_they_serve='',
            what_they_do='Leadership development programs',
            seeking='',
            offering='',
            match_data={},
        )
        assert len(result) > 0
        upper = result.upper()
        assert 'EXPERTISE' in upper or 'LEADERSHIP' in upper

    def test_max_520_chars(self, service):
        result = service._generate_why_fit(
            name='Rachel Green',
            company='Big Reach Media',
            who_they_serve='coaches, consultants, entrepreneurs, trainers, speakers, authors',
            what_they_do='List building, affiliate management, cross-promotion, speaking engagements',
            seeking='cross-promotion, affiliate partners, speaking opportunities',
            offering='massive audience, premium stage time, high-converting funnels',
            match_data={'list_size': 75000},
        )
        assert len(result) <= 520


# ---------------------------------------------------------------------------
# _generate_mutual_benefit tests
# ---------------------------------------------------------------------------

class TestGenerateMutualBenefit:
    def test_both_gets_sections_present(self, service):
        result = service._generate_mutual_benefit(
            name='David Kim',
            company='Innovation Labs',
            who_they_serve='tech entrepreneurs',
            seeking='partnerships',
            offering='mentorship',
            match_data={},
        )
        upper = result.upper()
        assert 'WHAT' in upper and 'GETS' in upper
        # Should have sections for both the partner and the client
        assert 'DAVID' in upper or 'KIM' in upper
        assert 'JANET' in upper or 'ATTWOOD' in upper or 'CLIENT' in upper

    def test_bullet_points_present(self, service):
        result = service._generate_mutual_benefit(
            name='Sarah Lopez',
            company='Coaching Circle',
            who_they_serve='life coaches',
            seeking='affiliate opportunities',
            offering='coaching framework',
            match_data={},
        )
        assert '* ' in result or '- ' in result

    def test_cross_promotion_seeking_gives_exposure_bullet(self, service):
        result = service._generate_mutual_benefit(
            name='Amy Turner',
            company='Visibility Co',
            who_they_serve='entrepreneurs',
            seeking='cross-promotion and list sharing',
            offering='social media reach',
            match_data={},
        )
        lower = result.lower()
        assert 'exposure' in lower or 'cross-promotion' in lower or 'audience' in lower or 'list' in lower


# ---------------------------------------------------------------------------
# _generate_outreach tests
# ---------------------------------------------------------------------------

class TestGenerateOutreach:
    def test_subject_line_present(self, service):
        result = service._generate_outreach(
            name='Brian Clark',
            company='Copyblogger',
            who_they_serve='content creators',
            seeking='partnerships',
            offering='platform access',
            match_data={},
        )
        assert 'Subject:' in result

    def test_partner_name_in_greeting(self, service):
        result = service._generate_outreach(
            name='Brian Clark',
            company='Copyblogger',
            who_they_serve='content creators',
            seeking='partnerships',
            offering='platform access',
            match_data={},
        )
        assert 'Hi Brian' in result or 'Dear Brian' in result or 'Hello Brian' in result

    def test_dr_prefix_handled(self, service):
        result = service._generate_outreach(
            name='Dr. Sarah Jones',
            company='Wellness Institute',
            who_they_serve='health professionals',
            seeking='speaking engagements',
            offering='research platform',
            match_data={},
        )
        # Should use "Sarah" not "Dr." in the greeting
        assert 'Hi Sarah' in result or 'Dear Sarah' in result or 'Hello Sarah' in result or 'Dr. Sarah' in result
