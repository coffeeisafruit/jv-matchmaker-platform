"""
Tests for matching/views_verification.py — Client profile verification views.

Verification item #12: "Profile confirmation: Client submits confirmation
form -> verify profile updated + client_confirmed source priority".

Covers:
 1. GET  /matching/verify/<token>/        — 200 with form fields (pending)
 2. GET  /matching/verify/<token>/        — sets opened_at on first visit
 3. GET  /matching/verify/<invalid>/      — 404
 4. GET  /matching/verify/<token>/        — redirects to /done/ if confirmed
 5. GET  /matching/verify/<token>/        — redirects to /done/ if expired
 6. POST /matching/verify/<token>/submit/ — sets status to confirmed
 7. POST submit                           — records fields_confirmed list
 8. POST submit                           — records changes_made for modified fields
 9. POST submit                           — calls _write_confirmed_data_to_profile
10. POST submit on already-confirmed      — redirects without reprocessing
11. GET  /matching/verify/<token>/done/   — 200 status page
"""

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import uuid
from datetime import date
from unittest.mock import MagicMock, patch

from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from matching.views_verification import VERIFICATION_FIELDS


# ---------------------------------------------------------------------------
# Helpers — concrete mock classes
# ---------------------------------------------------------------------------
# Django's template engine resolves ``{{ obj.attr }}`` by trying dict-style
# access ``obj['attr']`` first.  MagicMock silently handles __getitem__,
# returning another MagicMock — which breaks Django template filters like
# ``|date``.  Using plain objects avoids this; ``save`` is still a MagicMock
# so call-assertion patterns (``save.assert_called()``) keep working.
# ---------------------------------------------------------------------------

class _FakeProfile:
    """Concrete stand-in for SupabaseProfile."""

    def __init__(self, **overrides):
        self.id = uuid.uuid4()
        self.name = 'Jane Doe'
        self.company = 'Acme Inc'
        self.email = 'jane@acme.com'
        self.website = 'https://acme.com'
        self.linkedin = 'https://linkedin.com/in/janedoe'
        self.what_you_do = 'Business coaching'
        self.who_you_serve = 'Female entrepreneurs'
        self.seeking = 'JV partners with large lists'
        self.offering = 'High-ticket coaching program'
        self.niche = 'Executive coaching'
        self.signature_programs = 'Leadership Accelerator'
        self.bio = 'Award-winning coach.'
        for key, value in overrides.items():
            setattr(self, key, value)


class _FakeVerification:
    """Concrete stand-in for ClientVerification.

    Supports attribute assignment (e.g. ``cv.status = 'confirmed'``)
    and ``save()`` assertion via MagicMock.
    """

    _STATUS_DISPLAY = {
        'pending': 'Pending',
        'confirmed': 'Confirmed',
        'expired': 'Expired',
    }

    def __init__(self, token=None, status='pending', opened_at=None,
                 profile=None, **overrides):
        self.verification_token = token or uuid.uuid4()
        self.status = status
        self.opened_at = opened_at
        self.confirmed_at = None
        self.client = profile or _FakeProfile()
        self.month = date(2026, 2, 1)
        self.original_data = {}
        self.updated_data = {}
        self.fields_confirmed = []
        self.changes_made = {}
        self.save = MagicMock()
        for key, value in overrides.items():
            setattr(self, key, value)

    def get_status_display(self):
        return self._STATUS_DISPLAY.get(self.status, self.status)


def _make_mock_profile(**overrides):
    """Return a _FakeProfile that looks like a SupabaseProfile."""
    return _FakeProfile(**overrides)


def _make_mock_cv(token=None, status='pending', opened_at=None,
                  profile=None, **overrides):
    """Return a _FakeVerification that looks like a ClientVerification."""
    return _FakeVerification(
        token=token, status=status, opened_at=opened_at,
        profile=profile, **overrides,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProfileVerificationGet(TestCase):
    """GET /matching/verify/<token>/ — the confirmation form page."""

    @patch('matching.views_verification._get_verification_or_404')
    def test_pending_returns_200_with_form(self, mock_get):
        """#1: A pending verification returns 200 with field data."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending')
        mock_get.return_value = mock_cv

        url = reverse('matching:verification-form', kwargs={'token': token})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        # The context should contain the field list.
        self.assertIn('fields', response.context)
        field_names = [f['name'] for f in response.context['fields']]
        for expected in ('name', 'company', 'email', 'seeking'):
            self.assertIn(expected, field_names)

    @patch('matching.views_verification._get_verification_or_404')
    def test_first_visit_sets_opened_at(self, mock_get):
        """#2: First GET sets opened_at on the verification record."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending', opened_at=None)
        mock_get.return_value = mock_cv

        url = reverse('matching:verification-form', kwargs={'token': token})
        self.client.get(url)

        # opened_at should have been set and save() called.
        self.assertIsNotNone(mock_cv.opened_at)
        mock_cv.save.assert_called()

    @patch('matching.views_verification._get_verification_or_404')
    def test_invalid_token_returns_404(self, mock_get):
        """#3: An invalid token raises Http404 -> 404 response."""
        from django.http import Http404
        mock_get.side_effect = Http404('Verification link is invalid or has expired.')

        bad_token = uuid.uuid4()
        url = reverse('matching:verification-form', kwargs={'token': bad_token})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 404)

    @patch('matching.views_verification._get_verification_or_404')
    def test_confirmed_redirects_to_done(self, mock_get):
        """#4: A confirmed verification redirects to the done page."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='confirmed',
                                opened_at=timezone.now())
        mock_get.return_value = mock_cv

        url = reverse('matching:verification-form', kwargs={'token': token})
        response = self.client.get(url)

        expected_url = reverse('matching:verification-done',
                               kwargs={'token': token})
        self.assertRedirects(response, expected_url, fetch_redirect_response=False)

    @patch('matching.views_verification._get_verification_or_404')
    def test_expired_redirects_to_done(self, mock_get):
        """#5: An expired verification redirects to the done page."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='expired',
                                opened_at=timezone.now())
        mock_get.return_value = mock_cv

        url = reverse('matching:verification-form', kwargs={'token': token})
        response = self.client.get(url)

        expected_url = reverse('matching:verification-done',
                               kwargs={'token': token})
        self.assertRedirects(response, expected_url, fetch_redirect_response=False)


class TestProfileVerificationSubmit(TestCase):
    """POST /matching/verify/<token>/submit/ — form submission."""

    def _post_form(self, token, mock_cv, form_data=None,
                   mock_write=None):
        """Helper: POST the submit endpoint with patched helpers."""
        if form_data is None:
            form_data = {}

        with patch('matching.views_verification._get_verification_or_404',
                   return_value=mock_cv), \
             patch('matching.views_verification._write_confirmed_data_to_profile') as mw:
            if mock_write is not None:
                mw.side_effect = mock_write
            url = reverse('matching:verification-submit',
                          kwargs={'token': token})
            response = self.client.post(url, data=form_data)
            return response, mw

    def test_submit_sets_status_confirmed(self):
        """#6: A valid POST changes status to 'confirmed'."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending')

        form_data = {
            'name': 'Jane Doe',
            'confirm_name': 'on',
            'company': 'Acme Inc',
            'confirm_company': 'on',
        }
        # Fill remaining fields to avoid KeyError.
        for field in VERIFICATION_FIELDS:
            form_data.setdefault(field, getattr(mock_cv.client, field, ''))
            form_data.setdefault(f'confirm_{field}', 'on')

        response, _ = self._post_form(token, mock_cv, form_data)

        self.assertEqual(mock_cv.status, 'confirmed')
        self.assertIsNotNone(mock_cv.confirmed_at)
        mock_cv.save.assert_called()
        # Should redirect to done page.
        expected_url = reverse('matching:verification-done',
                               kwargs={'token': token})
        self.assertRedirects(response, expected_url,
                             fetch_redirect_response=False)

    def test_submit_records_fields_confirmed(self):
        """#7: fields_confirmed contains the list of confirmed field names."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending')

        form_data = {}
        for field in VERIFICATION_FIELDS:
            form_data[field] = getattr(mock_cv.client, field, '')
            form_data[f'confirm_{field}'] = 'on'

        self._post_form(token, mock_cv, form_data)

        # Every field was confirmed.
        self.assertIsInstance(mock_cv.fields_confirmed, list)
        for field in VERIFICATION_FIELDS:
            self.assertIn(field, mock_cv.fields_confirmed)

    def test_submit_records_changes_made(self):
        """#8: changes_made records old/new for modified fields."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending')

        # Submit with a changed company name.
        form_data = {}
        for field in VERIFICATION_FIELDS:
            form_data[field] = getattr(mock_cv.client, field, '')
            form_data[f'confirm_{field}'] = 'on'

        form_data['company'] = 'New Company Name'

        self._post_form(token, mock_cv, form_data)

        self.assertIn('company', mock_cv.changes_made)
        self.assertEqual(mock_cv.changes_made['company']['old'], 'Acme Inc')
        self.assertEqual(mock_cv.changes_made['company']['new'], 'New Company Name')

    def test_submit_calls_write_with_client_confirmed(self):
        """#9: _write_confirmed_data_to_profile is called with confirmed fields."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='pending')
        profile_id = str(mock_cv.client.id)

        form_data = {}
        for field in VERIFICATION_FIELDS:
            form_data[field] = getattr(mock_cv.client, field, '')
            form_data[f'confirm_{field}'] = 'on'

        _, mock_write = self._post_form(token, mock_cv, form_data)

        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args.kwargs
        # profile_id must match the mock profile.
        self.assertEqual(call_kwargs['profile_id'], profile_id)
        # updated_fields is a dict of confirmed field values.
        updated_fields = call_kwargs['updated_fields']
        self.assertIsInstance(updated_fields, dict)
        # All confirmed fields should be present.
        for field in VERIFICATION_FIELDS:
            self.assertIn(field, updated_fields)

    def test_already_confirmed_redirects_without_reprocessing(self):
        """#10: POST on already-confirmed verification redirects, does not reprocess."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='confirmed',
                                opened_at=timezone.now(),
                                confirmed_at=timezone.now())

        form_data = {'name': 'Ignored'}

        with patch('matching.views_verification._get_verification_or_404',
                   return_value=mock_cv), \
             patch('matching.views_verification._write_confirmed_data_to_profile') as mw:
            url = reverse('matching:verification-submit',
                          kwargs={'token': token})
            response = self.client.post(url, data=form_data)

            # Should redirect to done without calling the write function.
            expected_url = reverse('matching:verification-done',
                                   kwargs={'token': token})
            self.assertRedirects(response, expected_url,
                                 fetch_redirect_response=False)
            mw.assert_not_called()


class TestVerificationDone(TestCase):
    """GET /matching/verify/<token>/done/ — the status/thank-you page."""

    @patch('matching.views_verification._get_verification_or_404')
    def test_done_returns_200(self, mock_get):
        """#11: The done page returns 200 with verification context."""
        token = uuid.uuid4()
        mock_cv = _make_mock_cv(token=token, status='confirmed',
                                opened_at=timezone.now(),
                                confirmed_at=timezone.now(),
                                changes_made={'company': {'old': 'A', 'new': 'B'}})
        mock_get.return_value = mock_cv

        url = reverse('matching:verification-done', kwargs={'token': token})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertIn('verification', response.context)
        self.assertIn('changes_count', response.context)
        self.assertEqual(response.context['changes_count'], 1)
