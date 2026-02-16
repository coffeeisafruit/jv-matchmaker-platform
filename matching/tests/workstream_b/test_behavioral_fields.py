"""Tests for B3 behavioral tracking fields on PartnerRecommendation model."""

import pytest
from datetime import timedelta
from django.utils import timezone

from core.models import User
from matching.models import SupabaseProfile, PartnerRecommendation


@pytest.fixture
def test_user(db):
    return User.objects.create_user(username='behavuser', password='pass123', business_name='Behav Co', email='behav@test.com')


@pytest.fixture
def test_partner(db):
    return SupabaseProfile.objects.create(name='Behav Partner', email='behavpartner@test.com', company='BP Inc')


@pytest.fixture
def recommendation(test_user, test_partner):
    return PartnerRecommendation.objects.create(
        user=test_user,
        partner=test_partner,
        context=PartnerRecommendation.Context.DIRECTORY_MATCH,
    )


@pytest.mark.django_db
class TestBehavioralFieldDefaults:
    def test_was_viewed_defaults_to_false(self, recommendation):
        assert recommendation.was_viewed is False

    def test_was_contacted_defaults_to_false(self, recommendation):
        assert recommendation.was_contacted is False

    def test_view_count_defaults_to_zero(self, recommendation):
        assert recommendation.view_count == 0

    def test_contacted_at_defaults_to_none(self, recommendation):
        assert recommendation.contacted_at is None

    def test_outreach_message_used_defaults_to_false(self, recommendation):
        assert recommendation.outreach_message_used is False

    def test_explanation_source_defaults_to_empty(self, recommendation):
        assert recommendation.explanation_source is None


@pytest.mark.django_db
class TestBehavioralFieldUpdates:
    def test_can_update_behavioral_fields(self, recommendation):
        now = timezone.now()
        recommendation.was_viewed = True
        recommendation.was_contacted = True
        recommendation.view_count = 5
        recommendation.contacted_at = now
        recommendation.outreach_message_used = True
        recommendation.explanation_source = 'ai_generated'
        recommendation.time_to_first_action = timedelta(minutes=12)
        recommendation.save()

        recommendation.refresh_from_db()

        assert recommendation.was_viewed is True
        assert recommendation.was_contacted is True
        assert recommendation.view_count == 5
        assert recommendation.contacted_at is not None
        assert recommendation.outreach_message_used is True
        assert recommendation.explanation_source == 'ai_generated'
        assert recommendation.time_to_first_action == timedelta(minutes=12)

    def test_time_to_first_action_accepts_timedelta(self, recommendation):
        delta = timedelta(hours=2, minutes=30, seconds=15)
        recommendation.time_to_first_action = delta
        recommendation.save()

        recommendation.refresh_from_db()

        assert recommendation.time_to_first_action == delta
