"""
Tests for B4 management command: send_match_followups.

The command queries PartnerRecommendation records to identify those eligible
for Tier 2 follow-up feedback and reports them. Key eligibility criteria:
- was_contacted=True, contacted_at in 7-14 day window, feedback_outcome is NULL

Covers:
- Eligible window filtering (too recent, in window, too old)
- Feedback already recorded exclusion
- was_contacted=False exclusion (unless --include-uncontacted)
- --user-email filter
- --dry-run flag behavior
- Custom --days-min / --days-max window
- Output text verification
"""

import pytest
from datetime import timedelta
from io import StringIO

from django.core.management import call_command
from django.utils import timezone

from core.models import User
from matching.models import PartnerRecommendation, SupabaseProfile


@pytest.fixture
def user_a(db):
    """Primary test user."""
    return User.objects.create_user(
        username='user_a',
        password='pass',
        business_name='Company A',
        email='user_a@example.com',
    )


@pytest.fixture
def user_b(db):
    """Secondary test user for filtering tests."""
    return User.objects.create_user(
        username='user_b',
        password='pass',
        business_name='Company B',
        email='user_b@example.com',
    )


@pytest.fixture
def partner_alpha(db):
    """A test partner profile."""
    return SupabaseProfile.objects.create(
        name='Alpha Partner',
        email='alpha@partner.com',
    )


@pytest.fixture
def partner_beta(db):
    """Another test partner profile."""
    return SupabaseProfile.objects.create(
        name='Beta Partner',
        email='beta@partner.com',
    )


def _create_recommendation(user, partner, **overrides):
    """Helper to create a PartnerRecommendation with sensible defaults."""
    defaults = {
        'user': user,
        'partner': partner,
        'context': PartnerRecommendation.Context.DIRECTORY_MATCH,
        'was_contacted': False,
        'was_viewed': False,
    }
    defaults.update(overrides)
    return PartnerRecommendation.objects.create(**defaults)


def _run_command(**kwargs):
    """Run the management command and return stdout text."""
    out = StringIO()
    call_command('send_match_followups', stdout=out, **kwargs)
    return out.getvalue()


# =========================================================================
# Eligibility window tests
# =========================================================================


@pytest.mark.django_db
class TestFollowupEligibility:
    """Tests for the follow-up eligibility window."""

    def test_contacted_10_days_ago_is_eligible(self, user_a, partner_alpha):
        """Recommendation contacted 10 days ago (no feedback) is eligible."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
        )
        output = _run_command()

        assert 'Alpha Partner' in output
        assert 'Tier 2 Feedback Eligible: 1' in output

    def test_contacted_3_days_ago_excluded(self, user_a, partner_alpha):
        """Recommendation contacted 3 days ago is too recent and excluded."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=3),
        )
        output = _run_command()

        assert 'Alpha Partner' not in output
        assert 'No recommendations eligible for Tier 2 feedback' in output

    def test_contacted_20_days_ago_excluded(self, user_a, partner_alpha):
        """Recommendation contacted 20 days ago is too old and excluded."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=20),
        )
        output = _run_command()

        assert 'Alpha Partner' not in output
        assert 'No recommendations eligible for Tier 2 feedback' in output

    def test_already_has_feedback_excluded(self, user_a, partner_alpha):
        """Recommendation with feedback_outcome already set is excluded."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
            feedback_outcome='connected_promising',
            feedback_recorded_at=timezone.now() - timedelta(days=1),
        )
        output = _run_command()

        assert 'Alpha Partner' not in output
        assert 'No recommendations eligible for Tier 2 feedback' in output


# =========================================================================
# Contact status and flag tests
# =========================================================================


@pytest.mark.django_db
class TestFollowupFiltering:
    """Tests for filtering by contact status and flags."""

    def test_not_contacted_excluded_by_default(self, user_a, partner_alpha):
        """was_contacted=False is excluded without --include-uncontacted."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=False,
            was_viewed=True,
            # recommended_at is auto-set by auto_now_add
        )
        output = _run_command()

        assert 'Alpha Partner' not in output

    def test_not_contacted_included_with_flag(self, user_a, partner_alpha):
        """was_contacted=False, was_viewed=True is included with --include-uncontacted."""
        rec = _create_recommendation(
            user_a, partner_alpha,
            was_contacted=False,
            was_viewed=True,
        )
        # Override recommended_at to be within the 7-14 day window
        PartnerRecommendation.objects.filter(pk=rec.pk).update(
            recommended_at=timezone.now() - timedelta(days=10),
        )
        output = _run_command(include_uncontacted=True)

        assert 'Alpha Partner' in output
        assert 'nudge candidates' in output.lower() or 'Viewed but not contacted' in output

    def test_user_email_filters_to_one_user(self, user_a, user_b, partner_alpha, partner_beta):
        """--user-email filters results to only that user's recommendations."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
        )
        _create_recommendation(
            user_b, partner_beta,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
        )
        output = _run_command(user_email='user_a@example.com')

        assert 'Alpha Partner' in output
        assert 'Beta Partner' not in output

    def test_custom_days_window(self, user_a, partner_alpha):
        """--days-min and --days-max adjust the eligibility window."""
        # Contacted 20 days ago -- normally excluded with default 7-14 window
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=20),
        )
        # Expand window to 15-25 days
        output = _run_command(days_min=15, days_max=25)

        assert 'Alpha Partner' in output
        assert 'Tier 2 Feedback Eligible: 1' in output


# =========================================================================
# Dry-run and output verification
# =========================================================================


@pytest.mark.django_db
class TestFollowupOutput:
    """Tests for command output formatting."""

    def test_dry_run_shows_warning(self, user_a, partner_alpha):
        """--dry-run includes 'DRY RUN' in the output."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
        )
        output = _run_command(dry_run=True)

        assert 'DRY RUN' in output

    def test_output_includes_report_header(self, user_a, partner_alpha):
        """Output includes the MATCH FOLLOW-UP REPORT header."""
        _create_recommendation(
            user_a, partner_alpha,
            was_contacted=True,
            contacted_at=timezone.now() - timedelta(days=10),
        )
        output = _run_command()

        assert 'MATCH FOLLOW-UP REPORT' in output
        assert 'Window:' in output
