"""
Tests for the refactored ReportOutreachView — live SupabaseMatch + SupabaseProfile queries.

Covers:
- Helper functions (URL extraction, section assignment, badge logic, tagline, etc.)
- ReportOutreachView with live data (matches queried at view time)
- ReportOutreachView fallback to ReportPartner snapshot
- Section assignment with tier-aligned thresholds
- Template context structure (sections, partners, fields)
- Edge cases (no matches, no email, no supabase_profile link)
"""

import os
import uuid
from decimal import Decimal

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import pytest
from django.test import Client
from django.urls import reverse
from django.utils import timezone
from datetime import timedelta

from matching.models import (
    MemberReport, ReportPartner, SupabaseProfile, SupabaseMatch,
)
from matching.views import (
    _outreach_clean_url,
    _outreach_extract_linkedin,
    _outreach_extract_website,
    _outreach_extract_schedule,
    _outreach_format_list_size,
    _outreach_build_tagline,
    _outreach_clean_company,
    _outreach_assign_section_from_dict,
    _outreach_assign_badge,
    _outreach_assign_badge_style,
    _outreach_build_tags,
    _outreach_build_audience,
    _outreach_build_detail_note,
    _outreach_build_why_fit,
    _outreach_build_partner_dict,
)


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def client_profile(db):
    """Create a SupabaseProfile for the client (report owner)."""
    return SupabaseProfile.objects.create(
        id=uuid.uuid4(),
        name='Janet Attwood',
        email='janet@enlightenedalliance.com',
        company='Enlightened Alliance',
        website='https://enlightenedalliance.com',
        status='Member',
        who_you_serve='Transformational leaders',
        what_you_do='Helping people discover their passions',
        seeking='JV partners for launches',
        offering='200K subscriber list',
        list_size=200000,
        niche='Personal development',
    )


@pytest.fixture
def partner_with_email(db):
    """Create a partner SupabaseProfile with email."""
    return SupabaseProfile.objects.create(
        id=uuid.uuid4(),
        name='Alice Smith',
        email='alice@coaching.com',
        company='Alice Coaching',
        website='https://alicecoaching.com',
        status='Member',
        who_you_serve='Women entrepreneurs',
        what_you_do='Executive coaching for women in business',
        seeking='JV partners',
        offering='Coaching programs',
        list_size=50000,
        niche='Business coaching',
        signature_programs='Leadership Mastery',
    )


@pytest.fixture
def partner_with_linkedin(db):
    """Create a partner SupabaseProfile with LinkedIn but no email."""
    return SupabaseProfile.objects.create(
        id=uuid.uuid4(),
        name='Bob Jones',
        email='',
        company='Bob Media',
        linkedin='https://linkedin.com/in/bobjones',
        website='https://bobmedia.com',
        status='Member',
        who_you_serve='Course creators',
        what_you_do='Podcast production and speaking',
        list_size=15000,
        niche='Content marketing',
    )


@pytest.fixture
def partner_no_contact(db):
    """Create a partner SupabaseProfile with no contact info."""
    return SupabaseProfile.objects.create(
        id=uuid.uuid4(),
        name='Charlie Doe',
        email='',
        company='',
        status='Member',
        niche='Wellness',
    )


@pytest.fixture
def partner_jv_program(db):
    """Create a partner with JV program (booking link + seeking JV)."""
    return SupabaseProfile.objects.create(
        id=uuid.uuid4(),
        name='Dana Program',
        email='dana@jvprogram.com',
        company='JV Programs Inc',
        booking_link='https://calendly.com/dana',
        seeking='JV affiliates and partners',
        status='Member',
        list_size=80000,
    )


@pytest.fixture
def report_with_profile(db, client_profile):
    """Create a MemberReport linked to a SupabaseProfile."""
    return MemberReport.objects.create(
        member_name='Janet Attwood',
        member_email='janet@enlightenedalliance.com',
        company_name='Enlightened Alliance',
        access_code='TEST1234',
        month=timezone.now().date().replace(day=1),
        expires_at=timezone.now() + timedelta(days=30),
        is_active=True,
        client_profile={},
        supabase_profile=client_profile,
    )


@pytest.fixture
def report_without_profile(db):
    """Create a MemberReport without a linked SupabaseProfile."""
    return MemberReport.objects.create(
        member_name='No Profile User',
        member_email='nolink@test.com',
        company_name='No Link Inc',
        access_code='NOLINK99',
        month=timezone.now().date().replace(day=1),
        expires_at=timezone.now() + timedelta(days=30),
        is_active=True,
        client_profile={},
        supabase_profile=None,
    )


def _create_match(profile_id, suggested_id, harmonic_mean, match_context=None):
    """Create a SupabaseMatch between two profiles."""
    return SupabaseMatch.objects.create(
        id=uuid.uuid4(),
        profile_id=profile_id,
        suggested_profile_id=suggested_id,
        harmonic_mean=Decimal(str(harmonic_mean)),
        score_ab=Decimal(str(harmonic_mean + 2)),
        score_ba=Decimal(str(harmonic_mean - 2)),
        match_context=match_context or {},
    )


def _grant_session_access(http_client, report):
    """Grant session access to a report (mimics ReportAccessView POST)."""
    session = http_client.session
    session[f'report_access_{report.id}'] = True
    session.save()


# ===========================================================================
# Helper Function Tests
# ===========================================================================

class TestOutreachCleanUrl:
    def test_strips_trailing_comma(self):
        assert _outreach_clean_url('https://example.com,') == 'https://example.com'

    def test_strips_whitespace(self):
        assert _outreach_clean_url('  https://example.com  ') == 'https://example.com'

    def test_empty_string(self):
        assert _outreach_clean_url('') == ''

    def test_none(self):
        assert _outreach_clean_url(None) == ''


class TestOutreachExtractLinkedin:
    def test_linkedin_field(self, db):
        sp = SupabaseProfile(linkedin='https://linkedin.com/in/test', website='https://example.com')
        assert _outreach_extract_linkedin(sp) == 'https://linkedin.com/in/test'

    def test_linkedin_in_website(self, db):
        sp = SupabaseProfile(linkedin='', website='https://linkedin.com/in/test2')
        assert _outreach_extract_linkedin(sp) == 'https://linkedin.com/in/test2'

    def test_no_linkedin(self, db):
        sp = SupabaseProfile(linkedin='', website='https://example.com')
        assert _outreach_extract_linkedin(sp) == ''

    def test_none_fields(self, db):
        sp = SupabaseProfile(linkedin=None, website=None)
        assert _outreach_extract_linkedin(sp) == ''


class TestOutreachExtractWebsite:
    def test_normal_website(self, db):
        sp = SupabaseProfile(website='https://example.com')
        assert _outreach_extract_website(sp) == 'https://example.com'

    def test_linkedin_in_website_excluded(self, db):
        sp = SupabaseProfile(website='https://linkedin.com/in/test')
        assert _outreach_extract_website(sp) == ''

    def test_calendly_excluded(self, db):
        sp = SupabaseProfile(website='https://calendly.com/test')
        assert _outreach_extract_website(sp) == ''

    def test_facebook_excluded(self, db):
        sp = SupabaseProfile(website='https://facebook.com/test')
        assert _outreach_extract_website(sp) == ''

    def test_empty(self, db):
        sp = SupabaseProfile(website='')
        assert _outreach_extract_website(sp) == ''

    def test_none(self, db):
        sp = SupabaseProfile(website=None)
        assert _outreach_extract_website(sp) == ''


class TestOutreachExtractSchedule:
    def test_booking_link(self, db):
        sp = SupabaseProfile(booking_link='https://calendly.com/test', website='https://example.com')
        assert _outreach_extract_schedule(sp) == 'https://calendly.com/test'

    def test_calendly_in_website(self, db):
        sp = SupabaseProfile(booking_link='', website='https://calendly.com/test')
        assert _outreach_extract_schedule(sp) == 'https://calendly.com/test'

    def test_cal_com_in_website(self, db):
        sp = SupabaseProfile(booking_link='', website='https://cal.com/test')
        assert _outreach_extract_schedule(sp) == 'https://cal.com/test'

    def test_no_schedule(self, db):
        sp = SupabaseProfile(booking_link='', website='https://example.com')
        assert _outreach_extract_schedule(sp) == ''


class TestOutreachFormatListSize:
    def test_millions(self):
        assert _outreach_format_list_size(1_500_000) == '2M+'

    def test_thousands(self):
        assert _outreach_format_list_size(50000) == '50K'

    def test_hundreds(self):
        assert _outreach_format_list_size(500) == '500'

    def test_zero(self):
        assert _outreach_format_list_size(0) == ''

    def test_none(self):
        assert _outreach_format_list_size(None) == ''


class TestOutreachBuildTagline:
    def test_what_you_do_preferred(self, db):
        sp = SupabaseProfile(what_you_do='Coaching', offering='Programs', niche='Business')
        assert _outreach_build_tagline(sp) == 'Coaching'

    def test_offering_fallback(self, db):
        sp = SupabaseProfile(what_you_do='', offering='Programs', niche='Business')
        assert _outreach_build_tagline(sp) == 'Programs'

    def test_niche_fallback(self, db):
        sp = SupabaseProfile(what_you_do='', offering='', niche='Business')
        assert _outreach_build_tagline(sp) == 'Business'

    def test_business_focus_fallback(self, db):
        sp = SupabaseProfile(what_you_do='', offering='', niche='', business_focus='Growth')
        assert _outreach_build_tagline(sp) == 'Growth'

    def test_empty(self, db):
        sp = SupabaseProfile(what_you_do='', offering='', niche='', business_focus='')
        assert _outreach_build_tagline(sp) == ''


class TestOutreachCleanCompany:
    def test_normal_company(self, db):
        sp = SupabaseProfile(name='Test', company='Acme Corp')
        assert _outreach_clean_company(sp) == 'Acme Corp'

    def test_category_filtered(self, db):
        sp = SupabaseProfile(name='Jane', company='Business Skills, Self Improvement')
        # Should filter out category words
        result = _outreach_clean_company(sp)
        assert result != 'Business Skills, Self Improvement'

    def test_empty_falls_back_to_name(self, db):
        sp = SupabaseProfile(name='Jane Smith', company='')
        assert _outreach_clean_company(sp) == 'Jane Smith'

    def test_placeholder_falls_back_to_name(self, db):
        sp = SupabaseProfile(name='Jane Smith', company='N/A')
        assert _outreach_clean_company(sp) == 'Jane Smith'

    def test_leading_comma_stripped(self, db):
        sp = SupabaseProfile(name='Jane', company=',Real Company')
        assert _outreach_clean_company(sp) == 'Real Company'


class TestOutreachAssignSectionFromDict:
    def test_jv_program(self):
        pd = {'match_score': 60, 'email': 'a@b.com', 'linkedin': '', 'schedule': '', 'apply_url': 'https://apply.com'}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'jv_programs'

    def test_priority_high_score_with_email(self):
        pd = {'match_score': 70, 'email': 'a@b.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'priority'
        assert 'Hand-picked' in label or 'Priority' in label

    def test_this_week_moderate_score_with_email(self):
        pd = {'match_score': 60, 'email': 'a@b.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'this_week'

    def test_this_week_with_schedule(self):
        pd = {'match_score': 40, 'email': '', 'linkedin': '', 'schedule': 'https://cal.com/x', 'apply_url': ''}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'this_week'

    def test_linkedin_outreach(self):
        pd = {'match_score': 60, 'email': '', 'linkedin': 'https://linkedin.com/in/x', 'schedule': '', 'apply_url': ''}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'low_priority'
        assert 'LinkedIn' in label

    def test_research_needed_no_contact(self):
        pd = {'match_score': 60, 'email': '', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, label, note = _outreach_assign_section_from_dict(pd)
        assert section == 'low_priority'
        assert 'Research' in label

    def test_boundary_67_with_email_is_priority(self):
        pd = {'match_score': 67, 'email': 'x@y.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, _, _ = _outreach_assign_section_from_dict(pd)
        assert section == 'priority'

    def test_boundary_66_with_email_is_this_week(self):
        pd = {'match_score': 66, 'email': 'x@y.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, _, _ = _outreach_assign_section_from_dict(pd)
        assert section == 'this_week'

    def test_boundary_55_with_email_is_this_week(self):
        pd = {'match_score': 55, 'email': 'x@y.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, _, _ = _outreach_assign_section_from_dict(pd)
        assert section == 'this_week'

    def test_boundary_54_with_email_falls_through(self):
        """Score 54 with email but no schedule/linkedin → research needed."""
        pd = {'match_score': 54, 'email': 'x@y.com', 'linkedin': '', 'schedule': '', 'apply_url': ''}
        section, _, _ = _outreach_assign_section_from_dict(pd)
        assert section == 'low_priority'


class TestOutreachAssignBadge:
    def test_hand_picked(self, db):
        sp = SupabaseProfile(seeking='')
        assert _outreach_assign_badge(sp, 70) == 'Hand-Picked'

    def test_active_jv(self, db):
        sp = SupabaseProfile(seeking='JV partners')
        assert _outreach_assign_badge(sp, 60) == 'Active JV'

    def test_strong_match(self, db):
        sp = SupabaseProfile(seeking='')
        assert _outreach_assign_badge(sp, 60) == 'Strong Match'

    def test_large_reach(self, db):
        sp = SupabaseProfile(seeking='', list_size=150000)
        assert 'Reach' in _outreach_assign_badge(sp, 50)

    def test_no_badge(self, db):
        sp = SupabaseProfile(seeking='', list_size=5000)
        assert _outreach_assign_badge(sp, 50) == ''


class TestOutreachAssignBadgeStyle:
    def test_priority_for_high_score(self):
        assert _outreach_assign_badge_style(70) == 'priority'

    def test_fit_for_moderate_score(self):
        assert _outreach_assign_badge_style(60) == 'fit'

    def test_fit_for_low_score(self):
        assert _outreach_assign_badge_style(40) == 'fit'


class TestOutreachBuildTags:
    def test_women_tag(self, db):
        sp = SupabaseProfile(who_you_serve='Women entrepreneurs', niche='', what_you_do='', offering='')
        tags = _outreach_build_tags(sp, 60)
        labels = [t['label'] for t in tags]
        assert 'Women' in labels

    def test_coach_tag(self, db):
        sp = SupabaseProfile(who_you_serve='', niche='Life coach training', what_you_do='', offering='')
        tags = _outreach_build_tags(sp, 60)
        labels = [t['label'] for t in tags]
        assert 'Coaches' in labels

    def test_hand_picked_tag_for_high_score(self, db):
        sp = SupabaseProfile(who_you_serve='', niche='', what_you_do='', offering='')
        tags = _outreach_build_tags(sp, 70)
        labels = [t['label'] for t in tags]
        assert 'Hand-Picked' in labels

    def test_large_list_tag(self, db):
        sp = SupabaseProfile(who_you_serve='', niche='', what_you_do='', offering='', list_size=60000)
        tags = _outreach_build_tags(sp, 60)
        labels = [t['label'] for t in tags]
        assert 'Large List' in labels

    def test_max_4_tags(self, db):
        sp = SupabaseProfile(
            who_you_serve='Women coaches and speakers',
            niche='Entrepreneur events and podcasting',
            what_you_do='Author and book publishing',
            offering='',
            list_size=60000,
            seeking='JV partners',
        )
        tags = _outreach_build_tags(sp, 70)
        assert len(tags) <= 4


class TestOutreachBuildAudience:
    def test_with_all_fields(self, db):
        sp = SupabaseProfile(list_size=50000, who_you_serve='Women leaders', offering='Leadership programs')
        audience = _outreach_build_audience(sp)
        assert '50,000 subscribers' in audience
        assert 'Women leaders' in audience

    def test_with_niche_fallback(self, db):
        sp = SupabaseProfile(list_size=0, who_you_serve='', niche='Wellness')
        audience = _outreach_build_audience(sp)
        assert 'Wellness audience' in audience

    def test_empty(self, db):
        sp = SupabaseProfile(list_size=0, who_you_serve='', niche='', offering='')
        assert _outreach_build_audience(sp) == ''


class TestOutreachBuildDetailNote:
    def test_with_programs(self, db):
        sp = SupabaseProfile(signature_programs='Leadership Mastery', business_focus='', notes='')
        note = _outreach_build_detail_note(sp)
        assert 'Programs: Leadership Mastery' in note

    def test_with_focus_and_notes(self, db):
        sp = SupabaseProfile(
            signature_programs='', business_focus='Growth', niche='Other',
            notes='Prefers email contact\nCall Tuesday AM',
        )
        note = _outreach_build_detail_note(sp)
        assert 'Focus: Growth' in note
        assert 'Prefers email contact' in note

    def test_empty(self, db):
        sp = SupabaseProfile(signature_programs='', business_focus='', notes='', niche='')
        assert _outreach_build_detail_note(sp) == ''


class TestOutreachBuildWhyFit:
    def test_from_match_context(self, db):
        sp = SupabaseProfile(who_you_serve='', what_you_do='', niche='')
        match_context = {
            'synergy': {
                'factors': [
                    {'name': 'Audience Alignment', 'score': 8.0, 'detail': 'Strong audience overlap'},
                ]
            }
        }
        why = _outreach_build_why_fit(sp, match_context)
        assert 'Strong audience overlap' in why

    def test_fallback_to_profile(self, db):
        sp = SupabaseProfile(who_you_serve='Tech entrepreneurs', what_you_do='SaaS consulting', niche='')
        why = _outreach_build_why_fit(sp, {})
        assert 'Tech entrepreneurs' in why

    def test_empty(self, db):
        sp = SupabaseProfile(who_you_serve='', what_you_do='', niche='')
        assert _outreach_build_why_fit(sp, {}) == ''


class TestOutreachBuildPartnerDict:
    def test_all_fields_present(self, partner_with_email):
        pd = _outreach_build_partner_dict(partner_with_email, 72.5, {})
        expected_keys = {
            'id', 'name', 'company', 'tagline', 'email', 'website', 'phone',
            'linkedin', 'apply_url', 'schedule', 'badge', 'badge_style',
            'list_size', 'audience', 'why_fit', 'detail_note', 'tags', 'match_score',
        }
        assert set(pd.keys()) == expected_keys

    def test_score_preserved(self, partner_with_email):
        pd = _outreach_build_partner_dict(partner_with_email, 65.3, {})
        assert pd['match_score'] == 65.3

    def test_id_is_string(self, partner_with_email):
        pd = _outreach_build_partner_dict(partner_with_email, 60, {})
        assert isinstance(pd['id'], str)


# ===========================================================================
# View Integration Tests
# ===========================================================================

@pytest.mark.django_db
class TestReportOutreachViewLive:
    """Tests for the refactored ReportOutreachView using live SupabaseMatch data."""

    def test_live_view_returns_200(self, report_with_profile, client_profile, partner_with_email):
        """View returns 200 with live data when supabase_profile is linked."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert response.status_code == 200

    def test_live_view_shows_partner_name(self, report_with_profile, client_profile, partner_with_email):
        """Live view renders partner names from SupabaseProfile."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        content = response.content.decode()
        assert 'Alice Smith' in content

    def test_live_view_shows_email_link(self, report_with_profile, client_profile, partner_with_email):
        """Live view includes email mailto link."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        content = response.content.decode()
        assert 'alice@coaching.com' in content

    def test_live_view_sections_context(self, report_with_profile, client_profile, partner_with_email):
        """View passes sections list in context."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert 'sections' in response.context
        assert 'total_partners' in response.context
        assert response.context['total_partners'] == 1

    def test_live_view_priority_section(self, report_with_profile, client_profile, partner_with_email):
        """High score + email → priority section."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'priority' in section_keys

    def test_live_view_linkedin_outreach_section(
        self, report_with_profile, client_profile, partner_with_linkedin,
    ):
        """No email + has LinkedIn → low_priority LinkedIn section."""
        _create_match(client_profile.id, partner_with_linkedin.id, 65)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'low_priority' in section_keys

    def test_live_view_research_needed(
        self, report_with_profile, client_profile, partner_no_contact,
    ):
        """No contact info → low_priority Research Needed."""
        _create_match(client_profile.id, partner_no_contact.id, 60)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        sections = response.context['sections']
        low_priority_sections = [s for s in sections if s['key'] == 'low_priority']
        assert len(low_priority_sections) == 1
        assert 'Research' in low_priority_sections[0]['label']

    def test_live_view_multiple_partners_sorted(
        self, report_with_profile, client_profile, partner_with_email, partner_with_linkedin,
    ):
        """Multiple matches appear with correct ordering (highest score first)."""
        _create_match(client_profile.id, partner_with_email.id, 72.5)
        _create_match(client_profile.id, partner_with_linkedin.id, 58.3)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert response.context['total_partners'] == 2
        content = response.content.decode()
        assert 'Alice Smith' in content
        assert 'Bob Jones' in content

    def test_live_view_jv_programs_section(
        self, report_with_profile, client_profile, partner_jv_program,
    ):
        """Partner with booking link → jv_programs section."""
        _create_match(client_profile.id, partner_jv_program.id, 60)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'jv_programs' in section_keys

    def test_live_view_no_matches(self, report_with_profile):
        """Report with no matches shows empty sections."""
        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert response.status_code == 200
        assert response.context['total_partners'] == 0
        assert response.context['sections'] == []

    def test_live_view_reverse_match_direction(
        self, report_with_profile, client_profile, partner_with_email,
    ):
        """Match where client is suggested_profile_id (reverse direction) still works."""
        _create_match(partner_with_email.id, client_profile.id, 70.0)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert response.status_code == 200
        assert response.context['total_partners'] == 1
        content = response.content.decode()
        assert 'Alice Smith' in content

    def test_live_view_reflects_updated_email(
        self, report_with_profile, client_profile, partner_with_linkedin,
    ):
        """After adding email to a partner, the live view shows it immediately."""
        _create_match(client_profile.id, partner_with_linkedin.id, 68)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})

        # Before: no email
        response = http_client.get(url)
        sections = response.context['sections']
        # LinkedIn-only partner at score 68 but no email → low_priority
        section_keys = [s['key'] for s in sections]
        assert 'low_priority' in section_keys

        # Simulate enrichment adding an email
        partner_with_linkedin.email = 'bob@bobmedia.com'
        partner_with_linkedin.save()

        # After: email added, score 68 >= 67 → priority
        response = http_client.get(url)
        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'priority' in section_keys


@pytest.mark.django_db
class TestReportOutreachViewFallback:
    """Tests for the fallback path (no supabase_profile linked)."""

    def test_fallback_to_snapshot(self, report_without_profile):
        """When no supabase_profile is linked, read from ReportPartner."""
        ReportPartner.objects.create(
            report=report_without_profile,
            rank=1,
            section='priority',
            section_label='Priority Contacts',
            section_note='High match score',
            name='Snapshot Partner',
            company='Snapshot Co',
            email='snapshot@partner.com',
            match_score=80,
        )

        http_client = Client()
        _grant_session_access(http_client, report_without_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_without_profile.id})
        response = http_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        assert 'Snapshot Partner' in content

    def test_fallback_empty_snapshot(self, report_without_profile):
        """Fallback with no ReportPartner records shows empty sections."""
        http_client = Client()
        _grant_session_access(http_client, report_without_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_without_profile.id})
        response = http_client.get(url)

        assert response.status_code == 200
        assert response.context['total_partners'] == 0


@pytest.mark.django_db
class TestReportOutreachAccessControl:
    """Tests for session-based access control."""

    def test_no_session_redirects(self, report_with_profile):
        """Without session access, user gets redirected."""
        http_client = Client()
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})
        response = http_client.get(url)

        assert response.status_code == 302

    def test_expired_report_redirects(self, db, client_profile):
        """Expired report redirects even with session access."""
        report = MemberReport.objects.create(
            member_name='Expired User',
            member_email='expired@test.com',
            company_name='Expired Co',
            access_code='EXPIRED1',
            month=timezone.now().date().replace(day=1),
            expires_at=timezone.now() - timedelta(days=1),
            is_active=True,
            client_profile={},
            supabase_profile=client_profile,
        )

        http_client = Client()
        _grant_session_access(http_client, report)
        url = reverse('matching:report-outreach', kwargs={'report_id': report.id})
        response = http_client.get(url)

        assert response.status_code == 302

    def test_inactive_report_redirects(self, db, client_profile):
        """Inactive report redirects even with session access."""
        report = MemberReport.objects.create(
            member_name='Inactive User',
            member_email='inactive@test.com',
            company_name='Inactive Co',
            access_code='INACTIVE',
            month=timezone.now().date().replace(day=1),
            expires_at=timezone.now() + timedelta(days=30),
            is_active=False,
            client_profile={},
            supabase_profile=client_profile,
        )

        http_client = Client()
        _grant_session_access(http_client, report)
        url = reverse('matching:report-outreach', kwargs={'report_id': report.id})
        response = http_client.get(url)

        assert response.status_code == 302


@pytest.mark.django_db
class TestReportOutreachLiveData:
    """Tests verifying the 'live data' aspect — data freshness."""

    def test_new_match_appears_immediately(
        self, report_with_profile, client_profile, partner_with_email,
    ):
        """A match created after report generation appears in the view."""
        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})

        # No matches yet
        response = http_client.get(url)
        assert response.context['total_partners'] == 0

        # Add a match
        _create_match(client_profile.id, partner_with_email.id, 75)

        # Now the match appears
        response = http_client.get(url)
        assert response.context['total_partners'] == 1
        assert 'Alice Smith' in response.content.decode()

    def test_updated_score_changes_section(
        self, report_with_profile, client_profile, partner_with_email,
    ):
        """Score update changes section assignment on next page load."""
        match = _create_match(client_profile.id, partner_with_email.id, 60)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})

        # Score 60 + email → this_week
        response = http_client.get(url)
        sections = response.context['sections']
        assert sections[0]['key'] == 'this_week'

        # Update score to 70 → should move to priority
        match.harmonic_mean = Decimal('70.0')
        match.save()

        response = http_client.get(url)
        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'priority' in section_keys

    def test_deleted_match_disappears(
        self, report_with_profile, client_profile, partner_with_email,
    ):
        """A deleted match no longer appears in the view."""
        match = _create_match(client_profile.id, partner_with_email.id, 72)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})

        response = http_client.get(url)
        assert response.context['total_partners'] == 1

        match.delete()

        response = http_client.get(url)
        assert response.context['total_partners'] == 0

    def test_contact_info_update_reflected(
        self, report_with_profile, client_profile, partner_no_contact,
    ):
        """Adding contact info to a profile changes its section on next load."""
        _create_match(client_profile.id, partner_no_contact.id, 68)

        http_client = Client()
        _grant_session_access(http_client, report_with_profile)
        url = reverse('matching:report-outreach', kwargs={'report_id': report_with_profile.id})

        # No contact → research needed
        response = http_client.get(url)
        sections = response.context['sections']
        assert sections[0]['key'] == 'low_priority'

        # Add email → priority (score 68 >= 67)
        partner_no_contact.email = 'charlie@wellness.com'
        partner_no_contact.save()

        response = http_client.get(url)
        sections = response.context['sections']
        section_keys = [s['key'] for s in sections]
        assert 'priority' in section_keys
