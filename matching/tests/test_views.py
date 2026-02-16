"""
Tests for matching/views.py — Django class-based views for the JV Matcher module.

Covers:
- Demo views (no auth required)
- Auth redirect enforcement
- Profile CRUD (list, create, update, detail, delete)
- CSV import
- Match views (list, detail, status update)
- Score calculation endpoints
- Index redirect
"""

import os
import io

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import pytest
from unittest.mock import patch, MagicMock

from django.test import Client
from django.urls import reverse
from django.contrib.auth import get_user_model

from matching.models import Profile, Match

User = get_user_model()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_user(username='testuser', email='test@example.com', password='testpass'):
    """Create and return a test user."""
    return User.objects.create_user(
        username=username,
        password=password,
        email=email,
    )


def _create_profile(user, name='Test Partner', **kwargs):
    """Create and return a Profile for the given user."""
    defaults = {
        'company': 'Test Co',
        'email': 'partner@test.com',
        'industry': 'Technology',
        'audience_size': 'medium',
        'source': Profile.Source.MANUAL,
    }
    defaults.update(kwargs)
    return Profile.objects.create(user=user, name=name, **defaults)


def _create_match(user, profile, **kwargs):
    """Create and return a Match for the given user and profile."""
    defaults = {
        'intent_score': 0.7,
        'synergy_score': 0.6,
        'momentum_score': 0.5,
        'context_score': 0.4,
        'final_score': 0.55,
        'status': Match.Status.NEW,
        'score_breakdown': {
            'intent': {'score': 7.0, 'explanation': 'Good intent signals'},
            'synergy': {'score': 6.0, 'explanation': 'Moderate synergy'},
            'momentum': {'score': 5.0, 'explanation': 'Average momentum'},
            'context': {'score': 4.0, 'explanation': 'Some context'},
            'recommendation': 'Good potential partner.',
        },
    }
    defaults.update(kwargs)
    return Match.objects.create(user=user, profile=profile, **defaults)


# ===========================================================================
# Demo Views (no authentication required)
# ===========================================================================

class TestDemoViews:
    """Demo views should be accessible without login."""

    def test_demo_report_accessible(self, client):
        """GET /matching/demo/ returns 200."""
        url = reverse('matching:demo-report')
        response = client.get(url)
        assert response.status_code == 200

    def test_demo_outreach_accessible(self, client):
        """GET /matching/demo/outreach/ returns 200."""
        url = reverse('matching:demo-outreach')
        response = client.get(url)
        assert response.status_code == 200

    def test_demo_profile_accessible(self, client):
        """GET /matching/demo/profile/ returns 200."""
        url = reverse('matching:demo-profile')
        response = client.get(url)
        assert response.status_code == 200


# ===========================================================================
# Authentication Redirect
# ===========================================================================

class TestAuthRedirects:
    """Protected views must redirect unauthenticated users to login."""

    def test_profile_list_requires_login(self, client):
        """GET /matching/profiles/ without login redirects to login page."""
        url = reverse('matching:profile-list')
        response = client.get(url)
        assert response.status_code == 302
        assert '/login/' in response.url

    def test_match_list_requires_login(self, client):
        """GET /matching/matches/ without login redirects to login page."""
        url = reverse('matching:match-list')
        response = client.get(url)
        assert response.status_code == 302
        assert '/login/' in response.url

    def test_profile_create_requires_login(self, client):
        """GET /matching/profiles/create/ without login redirects to login page."""
        url = reverse('matching:profile-create')
        response = client.get(url)
        assert response.status_code == 302
        assert '/login/' in response.url

    def test_profile_import_requires_login(self, client):
        """GET /matching/profiles/import/ without login redirects to login page."""
        url = reverse('matching:profile-import')
        response = client.get(url)
        assert response.status_code == 302
        assert '/login/' in response.url


# ===========================================================================
# Profile CRUD
# ===========================================================================

@pytest.mark.django_db
class TestProfileListView:
    """Tests for the ProfileListView."""

    def test_profile_list_shows_user_profiles(self):
        """Create 2 profiles; GET should return 200 with both names."""
        user = _create_user()
        p1 = _create_profile(user, name='Alice Smith', company='Acme')
        p2 = _create_profile(user, name='Bob Jones', company='Beta', email='bob@beta.com')

        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-list')
        response = client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        assert 'Alice Smith' in content
        assert 'Bob Jones' in content

    def test_profile_list_search_filters(self):
        """Search by company name filters to matching profiles only."""
        user = _create_user()
        p1 = _create_profile(user, name='Alice Smith', company='Acme Corp')
        p2 = _create_profile(user, name='Bob Jones', company='Beta Inc', email='bob@beta.com')

        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-list')
        response = client.get(url, {'search': 'Acme'})

        assert response.status_code == 200
        profiles = response.context['profiles']
        profile_names = [p.name for p in profiles]
        assert 'Alice Smith' in profile_names
        assert 'Bob Jones' not in profile_names

    def test_profile_list_only_shows_own_profiles(self):
        """User should only see their own profiles, not another user's."""
        user1 = _create_user(username='user1', email='user1@test.com')
        user2 = _create_user(username='user2', email='user2@test.com')
        _create_profile(user1, name='User1 Profile')
        _create_profile(user2, name='User2 Profile', email='user2partner@test.com')

        client = Client()
        client.force_login(user1)
        url = reverse('matching:profile-list')
        response = client.get(url)

        profiles = response.context['profiles']
        profile_names = [p.name for p in profiles]
        assert 'User1 Profile' in profile_names
        assert 'User2 Profile' not in profile_names


@pytest.mark.django_db
class TestProfileCreateView:
    """Tests for the ProfileCreateView."""

    def test_profile_create_get_form(self):
        """GET should return 200 with a form."""
        user = _create_user()
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-create')
        response = client.get(url)

        assert response.status_code == 200
        assert 'form' in response.context

    def test_profile_create_success(self):
        """POST with valid data creates a Profile and redirects."""
        user = _create_user()
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-create')

        data = {
            'name': 'New Partner',
            'company': 'New Co',
            'email': 'new@partner.com',
            'industry': 'Marketing',
        }
        response = client.post(url, data)

        assert response.status_code == 302
        assert Profile.objects.filter(user=user, name='New Partner').exists()

    def test_profile_create_sets_source_manual(self):
        """Source is automatically set to 'manual' on creation."""
        user = _create_user()
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-create')

        data = {
            'name': 'Manual Partner',
            'company': 'Manual Co',
        }
        response = client.post(url, data)

        profile = Profile.objects.get(user=user, name='Manual Partner')
        assert profile.source == Profile.Source.MANUAL

    def test_profile_create_sets_user(self):
        """The created profile should belong to the logged-in user."""
        user = _create_user()
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-create')

        data = {'name': 'Owned Partner'}
        response = client.post(url, data)

        profile = Profile.objects.get(name='Owned Partner')
        assert profile.user == user


@pytest.mark.django_db
class TestProfileUpdateView:
    """Tests for the ProfileUpdateView."""

    def test_profile_update_success(self):
        """POST with updated name changes the profile name."""
        user = _create_user()
        profile = _create_profile(user, name='Old Name')

        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-edit', kwargs={'pk': profile.pk})

        data = {
            'name': 'New Name',
            'company': profile.company or '',
            'email': profile.email or '',
            'industry': profile.industry or '',
        }
        response = client.post(url, data)

        assert response.status_code == 302
        profile.refresh_from_db()
        assert profile.name == 'New Name'

    def test_profile_update_only_own(self):
        """User2 cannot edit user1's profile; should get 404."""
        user1 = _create_user(username='owner', email='owner@test.com')
        user2 = _create_user(username='intruder', email='intruder@test.com')
        profile = _create_profile(user1, name='Owner Profile')

        client = Client()
        client.force_login(user2)
        url = reverse('matching:profile-edit', kwargs={'pk': profile.pk})

        response = client.get(url)
        assert response.status_code == 404


@pytest.mark.django_db
class TestProfileDetailView:
    """Tests for the ProfileDetailView."""

    def test_profile_detail_success(self):
        """GET for own profile returns 200 with profile in context."""
        user = _create_user()
        profile = _create_profile(user, name='Detail Profile')

        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-detail', kwargs={'pk': profile.pk})
        response = client.get(url)

        assert response.status_code == 200
        assert response.context['profile'] == profile

    def test_profile_detail_only_own(self):
        """User cannot view another user's profile detail."""
        user1 = _create_user(username='viewer_owner', email='vo@test.com')
        user2 = _create_user(username='viewer_other', email='vother@test.com')
        profile = _create_profile(user1, name='Private Profile')

        client = Client()
        client.force_login(user2)
        url = reverse('matching:profile-detail', kwargs={'pk': profile.pk})

        response = client.get(url)
        assert response.status_code == 404


@pytest.mark.django_db
class TestProfileDeleteView:
    """Tests for the ProfileDeleteView."""

    def test_profile_delete_success(self):
        """POST deletes the profile and it no longer exists."""
        user = _create_user()
        profile = _create_profile(user, name='Doomed Profile')
        profile_pk = profile.pk

        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-delete', kwargs={'pk': profile.pk})
        response = client.post(url)

        assert response.status_code == 302
        assert not Profile.objects.filter(pk=profile_pk).exists()

    def test_profile_delete_only_own(self):
        """User cannot delete another user's profile."""
        user1 = _create_user(username='del_owner', email='delo@test.com')
        user2 = _create_user(username='del_other', email='delo2@test.com')
        profile = _create_profile(user1, name='Protected Profile')

        client = Client()
        client.force_login(user2)
        url = reverse('matching:profile-delete', kwargs={'pk': profile.pk})
        response = client.post(url)

        assert response.status_code == 404
        assert Profile.objects.filter(pk=profile.pk).exists()


# ===========================================================================
# CSV Import
# ===========================================================================

@pytest.mark.django_db
class TestProfileImportView:
    """Tests for the ProfileImportView CSV import."""

    def test_import_valid_csv(self):
        """Upload a CSV with 2 valid rows; 2 profiles should be created."""
        user = _create_user(username='importer', email='importer@test.com')
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-import')

        csv_content = "name,company,email\nAlice Smith,Acme,alice@acme.com\nBob Jones,Beta,bob@beta.com"
        csv_file = io.BytesIO(csv_content.encode('utf-8'))
        csv_file.name = 'test.csv'

        response = client.post(url, {'csv_file': csv_file})

        assert response.status_code == 302
        assert Profile.objects.filter(user=user, name='Alice Smith').exists()
        assert Profile.objects.filter(user=user, name='Bob Jones').exists()
        # Imported profiles should have source='import'
        assert Profile.objects.get(user=user, name='Alice Smith').source == Profile.Source.IMPORT

    def test_import_missing_name(self):
        """CSV row without a name should produce an error; no profile created for that row."""
        user = _create_user(username='importer2', email='importer2@test.com')
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-import')

        csv_content = "name,company,email\n,Acme,noname@acme.com\nBob Jones,Beta,bob2@beta.com"
        csv_file = io.BytesIO(csv_content.encode('utf-8'))
        csv_file.name = 'test.csv'

        response = client.post(url, {'csv_file': csv_file})

        assert response.status_code == 302
        # The row without a name should NOT create a profile
        assert not Profile.objects.filter(user=user, email='noname@acme.com').exists()
        # The valid row should still create a profile
        assert Profile.objects.filter(user=user, name='Bob Jones').exists()

    def test_import_sets_source_import(self):
        """Imported profiles should have source='import'."""
        user = _create_user(username='importer3', email='importer3@test.com')
        client = Client()
        client.force_login(user)
        url = reverse('matching:profile-import')

        csv_content = "name,company\nImported User,Import Co"
        csv_file = io.BytesIO(csv_content.encode('utf-8'))
        csv_file.name = 'test.csv'

        response = client.post(url, {'csv_file': csv_file})

        profile = Profile.objects.get(user=user, name='Imported User')
        assert profile.source == Profile.Source.IMPORT


# ===========================================================================
# Match Views
# ===========================================================================

@pytest.mark.django_db
class TestMatchListView:
    """Tests for the MatchListView."""

    def test_match_list_shows_matches(self):
        """Create a Match; GET should return 200 with match in context."""
        user = _create_user(username='matchuser', email='matchuser@test.com')
        profile = _create_profile(user, name='Matched Partner')
        match = _create_match(user, profile)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-list')
        response = client.get(url)

        assert response.status_code == 200
        matches = list(response.context['matches'])
        assert match in matches

    def test_match_list_status_filter(self):
        """Filter by status=contacted returns only matching matches."""
        user = _create_user(username='filteruser', email='filteruser@test.com')
        p1 = _create_profile(user, name='Partner A')
        p2 = _create_profile(user, name='Partner B', email='partnerb@test.com')
        m1 = _create_match(user, p1, status=Match.Status.NEW)
        m2 = _create_match(user, p2, status=Match.Status.CONTACTED)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-list')
        response = client.get(url, {'status': 'contacted'})

        assert response.status_code == 200
        matches = list(response.context['matches'])
        match_ids = [m.pk for m in matches]
        assert m2.pk in match_ids
        assert m1.pk not in match_ids


@pytest.mark.django_db
class TestMatchDetailView:
    """Tests for the MatchDetailView."""

    def test_match_detail_shows_breakdown(self):
        """GET returns 200 with score_breakdown data in context."""
        user = _create_user(username='detailuser', email='detailuser@test.com')
        profile = _create_profile(user, name='Detail Partner')
        match = _create_match(user, profile)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-detail', kwargs={'pk': match.pk})
        response = client.get(url)

        assert response.status_code == 200
        assert response.context['match'] == match
        # The view populates score_components from score_breakdown
        assert 'score_components' in response.context

    def test_match_detail_only_own(self):
        """User cannot view another user's match detail."""
        user1 = _create_user(username='match_owner', email='mowner@test.com')
        user2 = _create_user(username='match_other', email='mother@test.com')
        profile = _create_profile(user1, name='Owner Match Partner')
        match = _create_match(user1, profile)

        client = Client()
        client.force_login(user2)
        url = reverse('matching:match-detail', kwargs={'pk': match.pk})
        response = client.get(url)

        assert response.status_code == 404


@pytest.mark.django_db
class TestMatchUpdateStatusView:
    """Tests for the MatchUpdateStatusView."""

    def test_match_update_status(self):
        """POST with a valid status changes the match status."""
        user = _create_user(username='statususer', email='statususer@test.com')
        profile = _create_profile(user, name='Status Partner')
        match = _create_match(user, profile, status=Match.Status.NEW)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-update-status', kwargs={'pk': match.pk})
        response = client.post(url, {'status': 'contacted'})

        assert response.status_code == 302
        match.refresh_from_db()
        assert match.status == Match.Status.CONTACTED

    def test_match_update_status_invalid(self):
        """POST with an invalid status does not change the match."""
        user = _create_user(username='badstatus', email='badstatus@test.com')
        profile = _create_profile(user, name='Bad Status Partner')
        match = _create_match(user, profile, status=Match.Status.NEW)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-update-status', kwargs={'pk': match.pk})
        response = client.post(url, {'status': 'nonexistent_status'})

        match.refresh_from_db()
        assert match.status == Match.Status.NEW

    def test_match_update_status_with_notes(self):
        """POST with status and notes updates both fields."""
        user = _create_user(username='noteuser', email='noteuser@test.com')
        profile = _create_profile(user, name='Note Partner')
        match = _create_match(user, profile, status=Match.Status.NEW)

        client = Client()
        client.force_login(user)
        url = reverse('matching:match-update-status', kwargs={'pk': match.pk})
        response = client.post(url, {'status': 'in_progress', 'notes': 'Following up next week'})

        match.refresh_from_db()
        assert match.status == Match.Status.IN_PROGRESS
        assert match.notes == 'Following up next week'


# ===========================================================================
# Score Calculation
# ===========================================================================

@pytest.mark.django_db
class TestCalculateMatchView:
    """Tests for the CalculateMatchView."""

    def test_calculate_match_creates_match(self):
        """POST to calculate-match creates a Match with scores."""
        user = _create_user(username='calcuser', email='calcuser@test.com')
        profile = _create_profile(
            user,
            name='Calc Partner',
            email='calc@partner.com',
            linkedin_url='https://linkedin.com/in/calcpartner',
            website_url='https://calcpartner.com',
        )

        client = Client()
        client.force_login(user)
        url = reverse('matching:calculate-match', kwargs={'pk': profile.pk})
        response = client.post(url)

        assert response.status_code == 302
        assert Match.objects.filter(user=user, profile=profile).exists()
        match = Match.objects.get(user=user, profile=profile)
        assert match.final_score > 0
        assert match.score_breakdown is not None


@pytest.mark.django_db
class TestCalculateBulkMatchView:
    """Tests for the CalculateBulkMatchView."""

    def test_calculate_bulk_creates_matches(self):
        """POST to calculate-bulk creates matches for profiles without existing matches."""
        user = _create_user(username='bulkuser', email='bulkuser@test.com')
        p1 = _create_profile(user, name='Bulk Partner 1')
        p2 = _create_profile(user, name='Bulk Partner 2', email='bulk2@test.com')
        # p1 already has a match, p2 does not
        _create_match(user, p1)

        client = Client()
        client.force_login(user)
        url = reverse('matching:calculate-bulk')
        response = client.post(url)

        assert response.status_code == 302
        # p2 should now have a match
        assert Match.objects.filter(user=user, profile=p2).exists()


@pytest.mark.django_db
class TestRecalculateAllMatchesView:
    """Tests for the RecalculateAllMatchesView."""

    def test_recalculate_all_updates_scores(self):
        """POST to recalculate-all processes all profiles."""
        user = _create_user(username='recalcuser', email='recalcuser@test.com')
        p1 = _create_profile(user, name='Recalc Partner 1')
        p2 = _create_profile(user, name='Recalc Partner 2', email='recalc2@test.com')

        client = Client()
        client.force_login(user)
        url = reverse('matching:recalculate-all')
        response = client.post(url)

        assert response.status_code == 302
        # Both profiles should now have matches
        assert Match.objects.filter(user=user, profile=p1).exists()
        assert Match.objects.filter(user=user, profile=p2).exists()


# ===========================================================================
# Index Redirect
# ===========================================================================

@pytest.mark.django_db
class TestIndexRedirect:
    """Tests for the index redirect."""

    def test_index_redirects(self):
        """GET /matching/ redirects (302) to the partners view."""
        user = _create_user(username='indexuser', email='indexuser@test.com')
        client = Client()
        client.force_login(user)
        url = reverse('matching:index')
        response = client.get(url)

        assert response.status_code == 302
        # Should redirect to the partners URL
        partners_url = reverse('matching:partners')
        assert partners_url in response.url
