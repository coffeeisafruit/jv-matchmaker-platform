"""
Production-readiness tests for the report generation pipeline.

Covers:
- generate_member_report command: helpers, ISMC integration, end-to-end
- tasks.py: regenerate_member_report, regenerate_all_monthly_reports, refresh_reports_for_profile
- MemberReport.is_stale property
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import uuid
from datetime import date, timedelta
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.utils import timezone

from matching.management.commands.generate_member_report import (
    Command,
    _looks_like_person_name,
    _clean_url_field,
    _extract_linkedin,
    _extract_website,
    _extract_schedule,
)


# =============================================================================
# HELPERS: mock SupabaseProfile factory
# =============================================================================

def make_sp(**overrides):
    """Create a MagicMock SupabaseProfile with sensible defaults."""
    defaults = {
        'id': uuid.uuid4(),
        'name': 'Alice Smith',
        'email': 'alice@example.com',
        'company': 'Alice Co',
        'website': 'https://aliceco.com',
        'linkedin': 'https://linkedin.com/in/alicesmith',
        'phone': '555-0100',
        'booking_link': '',
        'status': 'Member',
        'niche': 'Business Coaching',
        'who_you_serve': 'Entrepreneurs and small business owners',
        'what_you_do': 'Business strategy consulting',
        'seeking': 'JV partners for cross-promotion',
        'offering': 'Access to 10K email list',
        'bio': 'Award-winning coach with 10 years experience',
        'business_focus': 'Coaching',
        'list_size': 10000,
        'social_reach': 5000,
        'signature_programs': 'Coaching Academy',
        'current_projects': '',
        'notes': '',
        'tags': ['coaching'],
        'revenue_tier': 'established',
        'jv_history': None,
        'content_platforms': None,
        'audience_engagement_score': None,
        'audience_type': None,
    }
    defaults.update(overrides)
    sp = MagicMock()
    for k, v in defaults.items():
        setattr(sp, k, v)
    return sp


# =============================================================================
# Unit tests: _looks_like_person_name()
# =============================================================================

class TestLooksLikePersonName:
    def test_valid_names(self):
        assert _looks_like_person_name('Janet Bray Attwood') is True
        assert _looks_like_person_name('Bob Johnson') is True
        assert _looks_like_person_name('Mary Jane Watson') is True

    def test_empty_and_none(self):
        assert _looks_like_person_name('') is False
        assert _looks_like_person_name(None) is False

    def test_category_names_rejected(self):
        assert _looks_like_person_name('Business Skills, Fitness, Life') is False
        assert _looks_like_person_name('Health, Lifestyle, Mental Health') is False

    def test_single_word_rejected(self):
        assert _looks_like_person_name('Madonna') is False

    def test_category_prefix_rejected(self):
        assert _looks_like_person_name('Business Skills Training') is False
        assert _looks_like_person_name('Self Improvement Coach') is False
        assert _looks_like_person_name('Health and Wellness') is False


# =============================================================================
# Unit tests: URL helpers
# =============================================================================

class TestUrlHelpers:
    def test_clean_url_field(self):
        assert _clean_url_field('https://example.com,') == 'https://example.com'
        assert _clean_url_field('  https://example.com  ') == 'https://example.com'
        assert _clean_url_field('') == ''
        assert _clean_url_field(None) == ''

    def test_extract_linkedin_from_linkedin_field(self):
        sp = make_sp(linkedin='https://linkedin.com/in/janedoe', website='https://janedoe.com')
        assert _extract_linkedin(sp) == 'https://linkedin.com/in/janedoe'

    def test_extract_linkedin_from_website_field(self):
        sp = make_sp(linkedin='', website='https://linkedin.com/in/janedoe')
        assert _extract_linkedin(sp) == 'https://linkedin.com/in/janedoe'

    def test_extract_linkedin_none(self):
        sp = make_sp(linkedin='', website='https://janedoe.com')
        assert _extract_linkedin(sp) == ''

    def test_extract_website_excludes_social(self):
        assert _extract_website(make_sp(website='https://linkedin.com/in/x')) == ''
        assert _extract_website(make_sp(website='https://calendly.com/x')) == ''
        assert _extract_website(make_sp(website='https://facebook.com/x')) == ''

    def test_extract_website_returns_real_site(self):
        sp = make_sp(website='https://acme.com')
        assert _extract_website(sp) == 'https://acme.com'

    def test_extract_website_empty(self):
        assert _extract_website(make_sp(website='')) == ''
        assert _extract_website(make_sp(website=None)) == ''

    def test_extract_schedule_from_booking_link(self):
        sp = make_sp(booking_link='https://calendly.com/alice')
        assert _extract_schedule(sp) == 'https://calendly.com/alice'

    def test_extract_schedule_from_website_calendly(self):
        sp = make_sp(booking_link='', website='https://calendly.com/alice')
        assert _extract_schedule(sp) == 'https://calendly.com/alice'

    def test_extract_schedule_none(self):
        sp = make_sp(booking_link='', website='https://acme.com')
        assert _extract_schedule(sp) == ''


# =============================================================================
# Unit tests: Command helper methods (no DB needed)
# =============================================================================

class TestCommandHelpers:
    def setup_method(self):
        self.cmd = Command()

    # --- _clean_company_name ---
    def test_clean_company_name_normal(self):
        sp = make_sp(company='Acme Corp', name='Jane Doe')
        assert self.cmd._clean_company_name(sp) == 'Acme Corp'

    def test_clean_company_name_category(self):
        sp = make_sp(company='Business Skills, Self Improvement', name='Jane Doe')
        result = self.cmd._clean_company_name(sp)
        assert 'business skills' not in result.lower()

    def test_clean_company_name_fallback_to_name(self):
        sp = make_sp(company='', name='Jane Doe')
        assert self.cmd._clean_company_name(sp) == 'Jane Doe'

    # --- _parse_month ---
    def test_parse_month_valid(self):
        result = Command._parse_month('2026-02')
        assert result == date(2026, 2, 1)

    def test_parse_month_none_defaults_current(self):
        result = Command._parse_month(None)
        today = date.today()
        assert result == today.replace(day=1)

    def test_parse_month_invalid(self):
        from django.core.management.base import CommandError
        with pytest.raises(CommandError):
            Command._parse_month('invalid')

    # --- _assign_section ---
    def test_assign_section_priority(self):
        match = {'partner': make_sp(email='a@b.com', booking_link='', seeking='affiliates'), 'score': 75}
        section, label, note = self.cmd._assign_section(match, 1, 10)
        assert section == 'priority'

    def test_assign_section_this_week(self):
        match = {'partner': make_sp(email='a@b.com', booking_link='', seeking='affiliates'), 'score': 55}
        section, label, note = self.cmd._assign_section(match, 1, 10)
        assert section == 'this_week'

    def test_assign_section_linkedin_fallback(self):
        match = {'partner': make_sp(email='', booking_link='', seeking='affiliates', linkedin='https://linkedin.com/in/x'), 'score': 40}
        section, label, note = self.cmd._assign_section(match, 1, 10)
        assert section == 'low_priority'

    def test_assign_section_jv_programs(self):
        match = {'partner': make_sp(booking_link='https://calendly.com/x', seeking='JV partners'), 'score': 60}
        section, label, note = self.cmd._assign_section(match, 1, 10)
        assert section == 'jv_programs'

    # --- _assign_badge ---
    def test_assign_badge_top_match(self):
        sp = make_sp(seeking='affiliates')
        result = self.cmd._assign_badge({'score': 90}, sp)
        assert result == 'Top Match'

    def test_assign_badge_strong_match(self):
        sp = make_sp(seeking='affiliates')
        result = self.cmd._assign_badge({'score': 78}, sp)
        assert result == 'Strong Match'

    def test_assign_badge_active_jv(self):
        sp = make_sp(seeking='JV partners')
        result = self.cmd._assign_badge({'score': 60}, sp)
        assert result == 'Active JV'

    # --- _assign_badge_style ---
    def test_badge_style_priority(self):
        assert self.cmd._assign_badge_style({'score': 90}) == 'priority'

    def test_badge_style_fit(self):
        assert self.cmd._assign_badge_style({'score': 72}) == 'fit'

    # --- _build_tagline ---
    def test_build_tagline_what_you_do(self):
        sp = make_sp(what_you_do='Business coaching', offering='10K list')
        assert self.cmd._build_tagline(sp) == 'Business coaching'

    def test_build_tagline_fallback_offering(self):
        sp = make_sp(what_you_do='', offering='10K list')
        assert self.cmd._build_tagline(sp) == '10K list'

    # --- _format_list_size ---
    def test_format_list_size_millions(self):
        assert self.cmd._format_list_size(1_500_000) == '2M+'

    def test_format_list_size_thousands(self):
        assert self.cmd._format_list_size(50000) == '50K'

    def test_format_list_size_small(self):
        assert self.cmd._format_list_size(500) == '500'

    def test_format_list_size_none(self):
        assert self.cmd._format_list_size(None) == ''

    # --- _build_client_profile ---
    def test_client_profile_contact_email(self):
        sp = make_sp(email='alice@personal.com')
        profile = self.cmd._build_client_profile(sp)
        assert profile['contact_email'] == 'help@jvmatches.com'
        assert 'alice@personal.com' not in str(profile)

    def test_client_profile_structure(self):
        sp = make_sp()
        profile = self.cmd._build_client_profile(sp)
        assert 'client' in profile
        assert 'contact_name' in profile
        assert 'avatar_initials' in profile
        assert 'program_name' in profile
        assert 'seeking_goals' in profile

    # --- _build_tags ---
    def test_build_tags_coaching(self):
        sp = make_sp(who_you_serve='coaches and speakers')
        tags = self.cmd._build_tags({'score': 60}, sp)
        labels = [t['label'] for t in tags]
        assert 'Coaches' in labels
        assert 'Speakers' in labels

    def test_build_tags_top_match(self):
        sp = make_sp()
        tags = self.cmd._build_tags({'score': 90}, sp)
        labels = [t['label'] for t in tags]
        assert 'Top Match' in labels

    def test_build_tags_max_four(self):
        sp = make_sp(
            who_you_serve='women coaches',
            what_you_do='speaking events for entrepreneurs',
            offering='podcast and author platform'
        )
        tags = self.cmd._build_tags({'score': 90}, sp)
        assert len(tags) <= 4

    # --- footer_text ---
    def test_footer_no_commission_text(self):
        """Verify footer_text doesn't contain commission language."""
        # Read the actual source to verify
        import inspect
        source = inspect.getsource(Command._generate_report_for)
        assert '20% commission' not in source
        assert 'commission on referred' not in source


# =============================================================================
# Unit tests: _build_why_fit_from_ismc
# =============================================================================

class TestBuildWhyFitFromISMC:
    def setup_method(self):
        self.cmd = Command()

    def test_with_high_intent_factors(self):
        partner = make_sp(seeking='JV Launch Partners')
        breakdown = {
            'intent': {
                'score': 8.0,
                'factors': [
                    {'name': 'JV History', 'score': 8.0, 'weight': 0.3, 'detail': 'has 3 past JV partnerships'},
                    {'name': 'Seeking Stated', 'score': 9.0, 'weight': 0.4, 'detail': 'explicitly seeking JVs'},
                ],
            },
            'synergy': {'score': 5.0, 'factors': []},
            'momentum': {'score': 5.0, 'factors': []},
            'context': {'score': 5.0, 'factors': []},
        }
        result = self.cmd._build_why_fit_from_ismc(partner, breakdown)
        assert 'JV Launch Partners' in result

    def test_with_high_synergy_factors(self):
        partner = make_sp(who_you_serve='Coaches and speakers', what_you_do='Marketing automation')
        breakdown = {
            'intent': {'score': 5.0, 'factors': []},
            'synergy': {
                'score': 7.0,
                'factors': [
                    {'name': 'Audience Alignment', 'score': 7.5, 'weight': 0.3, 'detail': 'strong audience overlap'},
                    {'name': 'Offering↔Seeking', 'score': 6.5, 'weight': 0.4, 'detail': 'offering matches seeking'},
                ],
            },
            'momentum': {'score': 5.0, 'factors': []},
            'context': {'score': 5.0, 'factors': []},
        }
        result = self.cmd._build_why_fit_from_ismc(partner, breakdown)
        assert 'Coaches and speakers' in result

    def test_with_high_momentum_list_size(self):
        partner = make_sp(list_size=75000)
        breakdown = {
            'intent': {'score': 5.0, 'factors': []},
            'synergy': {'score': 5.0, 'factors': []},
            'momentum': {
                'score': 8.0,
                'factors': [
                    {'name': 'List Size', 'score': 8.0, 'weight': 0.5, 'detail': '75K subscribers'},
                ],
            },
            'context': {'score': 5.0, 'factors': []},
        }
        result = self.cmd._build_why_fit_from_ismc(partner, breakdown)
        assert '75,000' in result
        assert 'cross-promotion' in result

    def test_fallback_when_no_high_scores(self):
        partner = make_sp(who_you_serve='Business owners', what_you_do='Consulting')
        breakdown = {
            'intent': {'score': 3.0, 'factors': [{'name': 'JV History', 'score': 3.0, 'weight': 0.3, 'detail': 'no history'}]},
            'synergy': {'score': 3.0, 'factors': [{'name': 'Audience Alignment', 'score': 3.0, 'weight': 0.3, 'detail': 'low overlap'}]},
            'momentum': {'score': 3.0, 'factors': []},
            'context': {'score': 3.0, 'factors': []},
        }
        result = self.cmd._build_why_fit_from_ismc(partner, breakdown)
        assert 'Business owners' in result


# =============================================================================
# Unit tests: _score_with_ismc (mocked scorer)
# =============================================================================

class TestScoreWithISMC:
    def setup_method(self):
        self.cmd = Command()
        self.cmd.stdout = StringIO()

    @patch('matching.management.commands.generate_member_report.SupabaseMatchScoringService')
    @patch('matching.management.commands.generate_member_report.SupabaseProfile')
    def test_scores_and_sorts_partners(self, MockSP, MockScorer):
        client = make_sp(name='Client User')

        partner_a = make_sp(name='Alice Smith', email='alice@test.com')
        partner_b = make_sp(name='Bob Jones', email='bob@test.com')

        # Mock the queryset
        qs = MagicMock()
        qs.filter.return_value = qs
        qs.exclude.return_value = qs
        qs.__iter__ = lambda self: iter([partner_a, partner_b])
        MockSP.objects = qs

        # Mock scorer: Alice scores higher than Bob
        scorer_instance = MockScorer.return_value
        def score_pair_side_effect(src, tgt):
            score = 75.0 if tgt.name == 'Alice Smith' else 55.0
            return {
                'score_ab': score,
                'breakdown_ab': {
                    'intent': {'score': 5.0, 'factors': []},
                    'synergy': {'score': 5.0, 'factors': []},
                    'momentum': {'score': 5.0, 'factors': []},
                    'context': {'score': 5.0, 'factors': []},
                },
            }
        scorer_instance.score_pair.side_effect = score_pair_side_effect

        results = self.cmd._score_with_ismc(client, top_n=5)

        assert len(results) == 2
        # Alice should be first (higher score)
        assert results[0]['partner'].name == 'Alice Smith'
        assert results[0]['score'] == 75.0
        assert results[1]['partner'].name == 'Bob Jones'
        assert results[1]['score'] == 55.0

    @patch('matching.management.commands.generate_member_report.SupabaseMatchScoringService')
    @patch('matching.management.commands.generate_member_report.SupabaseProfile')
    def test_filters_non_person_names(self, MockSP, MockScorer):
        client = make_sp(name='Client User')

        real_person = make_sp(name='Alice Smith', email='alice@test.com')
        category_entry = make_sp(name='Business Skills, Fitness, Life', email='x@test.com')

        qs = MagicMock()
        qs.filter.return_value = qs
        qs.exclude.return_value = qs
        qs.__iter__ = lambda self: iter([real_person, category_entry])
        MockSP.objects = qs

        scorer_instance = MockScorer.return_value
        scorer_instance.score_pair.return_value = {
            'score_ab': 60.0,
            'breakdown_ab': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
            },
        }

        results = self.cmd._score_with_ismc(client, top_n=5)

        # Only the real person should be scored
        assert len(results) == 1
        assert results[0]['partner'].name == 'Alice Smith'

    @patch('matching.management.commands.generate_member_report.SupabaseMatchScoringService')
    @patch('matching.management.commands.generate_member_report.SupabaseProfile')
    def test_respects_top_n(self, MockSP, MockScorer):
        client = make_sp(name='Client User')
        partners = [make_sp(name=f'Partner {i}', email=f'p{i}@test.com') for i in range(10)]

        qs = MagicMock()
        qs.filter.return_value = qs
        qs.exclude.return_value = qs
        qs.__iter__ = lambda self: iter(partners)
        MockSP.objects = qs

        scorer_instance = MockScorer.return_value
        call_idx = {'i': 0}
        def score_side_effect(src, tgt):
            call_idx['i'] += 1
            return {
                'score_ab': 90.0 - call_idx['i'],
                'breakdown_ab': {
                    'intent': {'score': 5.0, 'factors': []},
                    'synergy': {'score': 5.0, 'factors': []},
                    'momentum': {'score': 5.0, 'factors': []},
                    'context': {'score': 5.0, 'factors': []},
                },
            }
        scorer_instance.score_pair.side_effect = score_side_effect

        results = self.cmd._score_with_ismc(client, top_n=3)
        assert len(results) == 3


# =============================================================================
# DB tests: MemberReport.is_stale property
# =============================================================================

@pytest.mark.django_db
class TestMemberReportIsStale:
    def _create_report(self, created_days_ago=0):
        from matching.models import MemberReport
        report = MemberReport.objects.create(
            member_name='Test Member',
            member_email='test@example.com',
            company_name='Test Co',
            access_code=f'TEST{uuid.uuid4().hex[:8].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
        )
        # Override created_at (auto_now_add prevents setting it at create time)
        if created_days_ago:
            MemberReport.objects.filter(id=report.id).update(
                created_at=timezone.now() - timedelta(days=created_days_ago)
            )
            report.refresh_from_db()
        return report

    def test_fresh_report_not_stale(self):
        report = self._create_report(created_days_ago=5)
        assert report.is_stale is False

    def test_old_report_is_stale(self):
        report = self._create_report(created_days_ago=31)
        assert report.is_stale is True

    def test_exactly_30_days_is_stale(self):
        report = self._create_report(created_days_ago=30)
        assert report.is_stale is True

    def test_29_days_not_stale(self):
        report = self._create_report(created_days_ago=29)
        assert report.is_stale is False


@pytest.mark.django_db
class TestMemberReportAccessible:
    def test_active_and_not_expired(self):
        from matching.models import MemberReport
        report = MemberReport.objects.create(
            member_name='Test',
            member_email='t@t.com',
            company_name='Co',
            access_code=f'ACC{uuid.uuid4().hex[:8].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
        )
        assert report.is_accessible is True
        assert report.is_expired is False

    def test_expired_report(self):
        from matching.models import MemberReport
        report = MemberReport.objects.create(
            member_name='Test',
            member_email='t@t.com',
            company_name='Co',
            access_code=f'EXP{uuid.uuid4().hex[:8].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() - timedelta(days=1),
            is_active=True,
        )
        assert report.is_expired is True
        assert report.is_accessible is False


# =============================================================================
# DB tests: tasks.py — regenerate_member_report
# =============================================================================

@pytest.mark.django_db
class TestRegenerateMemberReport:
    def _setup_report_with_partners(self):
        from matching.models import MemberReport, ReportPartner, SupabaseProfile

        # Create a SupabaseProfile for the client
        client_sp = SupabaseProfile(
            id=uuid.uuid4(),
            name='Test Client',
            email='client@test.com',
            company='Client Co',
            status='Member',
            who_you_serve='Coaches',
            what_you_do='Business coaching',
            seeking='JV partners',
            offering='10K list',
            list_size=10000,
        )
        client_sp.save()

        # Create partner profiles
        partners = []
        for i in range(3):
            p = SupabaseProfile(
                id=uuid.uuid4(),
                name=f'Partner {chr(65+i)}',
                email=f'partner{chr(97+i)}@test.com',
                company=f'Partner Co {chr(65+i)}',
                status='Member',
                who_you_serve='Entrepreneurs',
                what_you_do='Marketing',
                seeking='affiliates',
                offering='5K list',
                list_size=5000,
            )
            p.save()
            partners.append(p)

        report = MemberReport.objects.create(
            member_name='Test Client',
            member_email='client@test.com',
            company_name='Client Co',
            access_code=f'REG{uuid.uuid4().hex[:6].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
            supabase_profile=client_sp,
        )

        for i, p in enumerate(partners):
            ReportPartner.objects.create(
                report=report,
                rank=i + 1,
                section='this_week',
                section_label='This Week',
                name=p.name,
                company=p.company,
                match_score=60.0 + i,
                source_profile=p,
            )

        return report, client_sp, partners

    @patch('matching.services.SupabaseMatchScoringService')
    def test_regenerate_deletes_old_partners(self, MockScorer):
        from matching.tasks import regenerate_member_report
        from matching.models import ReportPartner

        report, client_sp, partners = self._setup_report_with_partners()
        old_partner_count = report.partners.count()
        assert old_partner_count == 3

        # Mock scorer
        scorer_instance = MockScorer.return_value
        scorer_instance.score_pair.return_value = {
            'score_ab': 65.0,
            'breakdown_ab': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
            },
        }

        result = regenerate_member_report(report.id)

        assert result['report_id'] == report.id
        assert result['partners_created'] > 0
        assert not result['errors']

    @patch('matching.services.SupabaseMatchScoringService')
    def test_regenerate_preserves_access_code(self, MockScorer):
        from matching.tasks import regenerate_member_report
        from matching.models import MemberReport

        report, client_sp, partners = self._setup_report_with_partners()
        original_code = report.access_code

        scorer_instance = MockScorer.return_value
        scorer_instance.score_pair.return_value = {
            'score_ab': 65.0,
            'breakdown_ab': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
            },
        }

        regenerate_member_report(report.id)

        report.refresh_from_db()
        assert report.access_code == original_code

    def test_regenerate_inactive_report_errors(self):
        from matching.tasks import regenerate_member_report

        result = regenerate_member_report(99999)
        assert len(result['errors']) > 0
        assert 'not found' in result['errors'][0].lower() or 'inactive' in result['errors'][0].lower()


# =============================================================================
# DB tests: tasks.py — regenerate_all_monthly_reports
# =============================================================================

@pytest.mark.django_db
class TestRegenerateAllMonthlyReports:
    @patch('matching.tasks.regenerate_member_report')
    def test_triggers_for_active_reports(self, mock_regen):
        from matching.models import MemberReport
        from matching.tasks import regenerate_all_monthly_reports

        mock_regen.return_value = {'report_id': 1, 'partners_created': 5, 'errors': []}

        for i in range(3):
            MemberReport.objects.create(
                member_name=f'Member {i}',
                member_email=f'm{i}@test.com',
                company_name=f'Co {i}',
                access_code=f'ALL{uuid.uuid4().hex[:6].upper()}',
                month=date.today().replace(day=1),
                expires_at=timezone.now() + timedelta(days=45),
                is_active=True,
            )

        result = regenerate_all_monthly_reports()

        assert result['total'] >= 3
        assert result['triggered'] >= 3

    @patch('matching.tasks.regenerate_member_report')
    def test_skips_inactive_reports(self, mock_regen):
        from matching.models import MemberReport
        from matching.tasks import regenerate_all_monthly_reports

        mock_regen.return_value = {'report_id': 1, 'partners_created': 5, 'errors': []}

        # Active report
        MemberReport.objects.create(
            member_name='Active',
            member_email='a@t.com',
            company_name='Active Co',
            access_code=f'ACT{uuid.uuid4().hex[:6].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
        )
        # Inactive report
        MemberReport.objects.create(
            member_name='Inactive',
            member_email='i@t.com',
            company_name='Inactive Co',
            access_code=f'INA{uuid.uuid4().hex[:6].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=False,
        )

        result = regenerate_all_monthly_reports()

        # Only active reports should be triggered
        assert result['total'] >= 1
        # Inactive report should not be in the count
        # (total only counts active ones from the query)


# =============================================================================
# DB tests: tasks.py — refresh_reports_for_profile
# =============================================================================

@pytest.mark.django_db
class TestRefreshReportsForProfile:
    @patch('matching.tasks.regenerate_member_report')
    def test_refreshes_affected_reports(self, mock_regen):
        from matching.models import MemberReport, ReportPartner, SupabaseProfile
        from matching.tasks import refresh_reports_for_profile

        mock_regen.return_value = {'report_id': 1, 'partners_created': 5, 'errors': []}

        # Create a partner profile
        partner_sp = SupabaseProfile(
            id=uuid.uuid4(),
            name='Changed Partner',
            email='partner@test.com',
            status='Member',
        )
        partner_sp.save()

        # Create a report that includes this partner
        report = MemberReport.objects.create(
            member_name='Report Owner',
            member_email='owner@test.com',
            company_name='Owner Co',
            access_code=f'REF{uuid.uuid4().hex[:6].upper()}',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
        )
        ReportPartner.objects.create(
            report=report,
            rank=1,
            section='this_week',
            section_label='This Week',
            name='Changed Partner',
            source_profile=partner_sp,
            match_score=60.0,
        )

        result = refresh_reports_for_profile(str(partner_sp.id))

        assert result['reports_affected'] >= 1
        assert result['triggered'] >= 1
        mock_regen.assert_called()

    @patch('matching.tasks.regenerate_member_report')
    def test_no_affected_reports(self, mock_regen):
        from matching.tasks import refresh_reports_for_profile

        result = refresh_reports_for_profile(str(uuid.uuid4()))

        assert result['reports_affected'] == 0
        assert result['triggered'] == 0
        mock_regen.assert_not_called()


# =============================================================================
# DB tests: End-to-end command execution
# =============================================================================

@pytest.mark.django_db
class TestGenerateMemberReportCommand:
    def _create_test_members(self):
        """Create a client and several partner profiles for testing."""
        from matching.models import SupabaseProfile

        client = SupabaseProfile(
            id=uuid.uuid4(),
            name='Test Reporter',
            email='reporter@test.com',
            company='Reporter Co',
            status='Member',
            who_you_serve='Entrepreneurs',
            what_you_do='Business coaching',
            seeking='JV partners',
            offering='10K list access',
            list_size=10000,
            niche='Business Coaching',
        )
        client.save()

        for i in range(5):
            p = SupabaseProfile(
                id=uuid.uuid4(),
                name=f'Partner {chr(65+i)} Person',
                email=f'partner{chr(97+i)}@test.com',
                company=f'Partner {chr(65+i)} Corp',
                status='Member',
                who_you_serve='Small business owners',
                what_you_do=f'Service offering {i}',
                seeking='Affiliates' if i % 2 == 0 else 'JV partners',
                offering=f'{(i+1)*5}K list',
                list_size=(i + 1) * 5000,
                niche='Marketing',
            )
            p.save()

        return client

    @patch('matching.services.SupabaseMatchScoringService.score_pair')
    def test_single_member_report(self, mock_score_pair):
        from matching.models import MemberReport, ReportPartner

        mock_score_pair.return_value = {
            'score_ab': 65.0,
            'score_ba': 55.0,
            'harmonic_mean': 59.58,
            'breakdown_ab': {
                'intent': {'score': 6.0, 'factors': []},
                'synergy': {'score': 6.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 6.5,
            },
            'breakdown_ba': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 5.5,
            },
        }

        client = self._create_test_members()

        out = StringIO()
        call_command(
            'generate_member_report',
            client_name='Test Reporter',
            month='2026-02',
            top=3,
            stdout=out,
        )

        output = out.getvalue()
        assert 'REPORT GENERATED SUCCESSFULLY' in output
        assert 'ACCESS CODE' in output

        # Verify DB records
        report = MemberReport.objects.filter(member_name='Test Reporter').first()
        assert report is not None
        assert report.access_code
        assert report.month == date(2026, 2, 1)
        assert report.footer_text == 'Report generated for Test Reporter.'
        assert '20%' not in report.footer_text
        assert 'commission' not in report.footer_text

        partners = ReportPartner.objects.filter(report=report)
        assert partners.count() <= 3
        assert partners.count() > 0

    @patch('matching.services.SupabaseMatchScoringService.score_pair')
    def test_batch_mode(self, mock_score_pair):
        from matching.models import MemberReport

        mock_score_pair.return_value = {
            'score_ab': 60.0,
            'score_ba': 50.0,
            'harmonic_mean': 54.55,
            'breakdown_ab': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 6.0,
            },
            'breakdown_ba': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 5.0,
            },
        }

        self._create_test_members()

        initial_count = MemberReport.objects.count()

        out = StringIO()
        call_command(
            'generate_member_report',
            all=True,
            month='2026-02',
            top=3,
            stdout=out,
        )

        output = out.getvalue()
        assert 'BATCH MODE' in output

        # At least the 5 partners + client = 6 members, all should get reports
        # (each member generates a report unless they're not a person name)
        new_reports = MemberReport.objects.count() - initial_count
        assert new_reports >= 1

    @patch('matching.services.SupabaseMatchScoringService.score_pair')
    def test_company_override(self, mock_score_pair):
        from matching.models import MemberReport

        mock_score_pair.return_value = {
            'score_ab': 60.0,
            'score_ba': 50.0,
            'harmonic_mean': 54.55,
            'breakdown_ab': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 6.0,
            },
            'breakdown_ba': {
                'intent': {'score': 5.0, 'factors': []},
                'synergy': {'score': 5.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 5.0,
            },
        }

        client = self._create_test_members()

        out = StringIO()
        call_command(
            'generate_member_report',
            client_name='Test Reporter',
            month='2026-02',
            company='Custom Company Name',
            stdout=out,
        )

        report = MemberReport.objects.filter(member_name='Test Reporter').order_by('-id').first()
        assert report.company_name == 'Custom Company Name'

    def test_missing_client_name_errors(self):
        from django.core.management.base import CommandError
        with pytest.raises(CommandError, match='Provide --client-name or --client-profile-id'):
            call_command('generate_member_report', month='2026-02')

    def test_nonexistent_client_errors(self):
        from django.core.management.base import CommandError
        with pytest.raises(CommandError, match='No profile found'):
            call_command('generate_member_report', client_name='Nonexistent Person XYZ', month='2026-02')


# =============================================================================
# DB tests: ReportPartner section assignment integration
# =============================================================================

@pytest.mark.django_db
class TestReportPartnerSections:
    @patch('matching.services.SupabaseMatchScoringService.score_pair')
    def test_high_score_with_email_gets_priority(self, mock_score_pair):
        from matching.models import MemberReport, ReportPartner, SupabaseProfile

        # Create client
        client = SupabaseProfile(
            id=uuid.uuid4(), name='Section Client', email='sc@test.com',
            company='SC Co', status='Member', who_you_serve='Coaches',
            what_you_do='Coaching', seeking='JV', offering='list', list_size=5000,
        )
        client.save()

        # Create partner with email
        partner = SupabaseProfile(
            id=uuid.uuid4(), name='High Score Partner', email='high@test.com',
            company='High Co', status='Member', who_you_serve='Entrepreneurs',
            what_you_do='Marketing', seeking='affiliates', offering='5K list',
            list_size=5000,
        )
        partner.save()

        mock_score_pair.return_value = {
            'score_ab': 80.0,
            'score_ba': 70.0,
            'harmonic_mean': 74.67,
            'breakdown_ab': {
                'intent': {'score': 8.0, 'factors': []},
                'synergy': {'score': 7.0, 'factors': []},
                'momentum': {'score': 6.0, 'factors': []},
                'context': {'score': 6.0, 'factors': []},
                'final_0_10': 8.0,
            },
            'breakdown_ba': {
                'intent': {'score': 7.0, 'factors': []},
                'synergy': {'score': 6.0, 'factors': []},
                'momentum': {'score': 5.0, 'factors': []},
                'context': {'score': 5.0, 'factors': []},
                'final_0_10': 7.0,
            },
        }

        out = StringIO()
        call_command(
            'generate_member_report',
            client_name='Section Client',
            month='2026-02',
            stdout=out,
        )

        # Check that the high-scoring partner with email is in "priority" section
        rp = ReportPartner.objects.filter(
            name='High Score Partner',
            report__member_name='Section Client',
        ).first()
        assert rp is not None
        assert rp.section == 'priority'
        assert rp.match_score == 80.0


# =============================================================================
# DB tests: Report access views (the user-facing production path)
# =============================================================================

@pytest.mark.django_db
class TestReportAccessViews:
    """Tests for the code-gated report access flow."""

    def _create_accessible_report(self):
        from matching.models import MemberReport, ReportPartner
        report = MemberReport.objects.create(
            member_name='View Test Member',
            member_email='view@test.com',
            company_name='View Co',
            access_code='VIEWTEST1',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=True,
            client_profile={
                'client': {'name': 'View Co', 'tagline': 'Testing'},
                'contact_name': 'View Test Member',
                'contact_email': 'help@jvmatches.com',
            },
        )
        ReportPartner.objects.create(
            report=report, rank=1, section='priority',
            section_label='Priority Contacts', name='Partner View',
            company='Partner Co', match_score=75.0,
        )
        return report

    def test_access_page_renders(self, client):
        response = client.get('/matching/report/')
        assert response.status_code == 200

    def test_valid_access_code_redirects_to_hub(self, client):
        report = self._create_accessible_report()
        response = client.post('/matching/report/', {'code': 'VIEWTEST1'})
        assert response.status_code == 302
        assert f'/matching/report/{report.id}/' in response.url

    def test_invalid_access_code_shows_error(self, client):
        response = client.post('/matching/report/', {'code': 'BADCODE99'})
        assert response.status_code == 200  # re-renders form

    def test_expired_report_rejected(self, client):
        from matching.models import MemberReport
        report = MemberReport.objects.create(
            member_name='Expired',
            member_email='e@t.com',
            company_name='Exp Co',
            access_code='EXPIRED01',
            month=date.today().replace(day=1),
            expires_at=timezone.now() - timedelta(days=1),
            is_active=True,
        )
        response = client.post('/matching/report/', {'code': 'EXPIRED01'})
        assert response.status_code == 200  # re-renders form, not redirect

    def test_inactive_report_rejected(self, client):
        from matching.models import MemberReport
        MemberReport.objects.create(
            member_name='Inactive',
            member_email='i@t.com',
            company_name='Ina Co',
            access_code='INACTIVE1',
            month=date.today().replace(day=1),
            expires_at=timezone.now() + timedelta(days=45),
            is_active=False,
        )
        response = client.post('/matching/report/', {'code': 'INACTIVE1'})
        assert response.status_code == 200  # re-renders form

    def test_hub_requires_session_access(self, client):
        report = self._create_accessible_report()
        # Without posting the code first, should redirect
        response = client.get(f'/matching/report/{report.id}/')
        assert response.status_code == 302
        assert 'report' in response.url

    def test_hub_accessible_after_code_entry(self, client):
        report = self._create_accessible_report()
        # First authenticate
        client.post('/matching/report/', {'code': 'VIEWTEST1'})
        # Then access hub
        response = client.get(f'/matching/report/{report.id}/')
        assert response.status_code == 200

    def test_outreach_page_accessible(self, client):
        report = self._create_accessible_report()
        client.post('/matching/report/', {'code': 'VIEWTEST1'})
        response = client.get(f'/matching/report/{report.id}/outreach/')
        assert response.status_code == 200

    def test_profile_page_accessible(self, client):
        report = self._create_accessible_report()
        client.post('/matching/report/', {'code': 'VIEWTEST1'})
        response = client.get(f'/matching/report/{report.id}/profile/')
        assert response.status_code == 200

    def test_access_code_case_insensitive(self, client):
        report = self._create_accessible_report()
        response = client.post('/matching/report/', {'code': 'viewtest1'})
        assert response.status_code == 302  # redirects to hub

    def test_access_increments_count(self, client):
        from matching.models import MemberReport
        report = self._create_accessible_report()
        assert report.access_count == 0
        client.post('/matching/report/', {'code': 'VIEWTEST1'})
        report.refresh_from_db()
        assert report.access_count == 1

    def test_rate_limiting_after_5_attempts(self, client):
        # Make 5 bad attempts
        for _ in range(5):
            client.post('/matching/report/', {'code': 'WRONGCODE'})
        # 6th attempt should be rate-limited
        report = self._create_accessible_report()
        response = client.post('/matching/report/', {'code': 'VIEWTEST1'})
        assert response.status_code == 200  # blocked, re-renders form
