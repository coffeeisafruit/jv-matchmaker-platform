"""
Tests for B4 PartnerRecommendation feedback fields and B5 MatchLearningSignal model.

B4 Tests:
- FeedbackOutcome choices validation
- Nullable feedback fields (feedback_outcome, feedback_notes, feedback_recorded_at)
- FeedbackOutcome values match expected set

B5 Tests:
- MatchLearningSignal creation with all required fields
- SignalType choices validation
- FK to PartnerRecommendation
- JSON fields accept dicts
- Default ordering is -created_at
- Meta has 2 indexes
"""

import pytest
from django.utils import timezone

from matching.models import PartnerRecommendation, MatchLearningSignal, SupabaseProfile
from core.models import User


@pytest.fixture
def test_user(db):
    """Create a test user for FK relationships."""
    return User.objects.create_user(
        username='testuser',
        password='testpass123',
        business_name='Test Co',
        email='testuser@example.com',
    )


@pytest.fixture
def test_partner(db):
    """Create a test SupabaseProfile for FK relationships."""
    return SupabaseProfile.objects.create(
        name='Test Partner',
        email='partner@example.com',
        company='Partner Inc',
    )


@pytest.fixture
def test_recommendation(test_user, test_partner):
    """Create a base PartnerRecommendation for testing."""
    return PartnerRecommendation.objects.create(
        user=test_user,
        partner=test_partner,
        context=PartnerRecommendation.Context.DIRECTORY_MATCH,
    )


# =========================================================================
# B4: PartnerRecommendation feedback fields
# =========================================================================


@pytest.mark.django_db
class TestFeedbackOutcomeChoices:
    """Tests for FeedbackOutcome TextChoices on PartnerRecommendation."""

    def test_feedback_outcome_has_four_choices(self):
        """FeedbackOutcome.choices should have exactly 4 entries."""
        choices = PartnerRecommendation.FeedbackOutcome.choices
        assert len(choices) == 4

    def test_feedback_outcome_values(self):
        """FeedbackOutcome contains the expected value set."""
        values = {choice[0] for choice in PartnerRecommendation.FeedbackOutcome.choices}
        expected = {
            'connected_promising',
            'connected_not_fit',
            'no_response',
            'did_not_reach_out',
        }
        assert values == expected

    def test_feedback_outcome_is_nullable(self, test_recommendation):
        """feedback_outcome defaults to None (nullable)."""
        assert test_recommendation.feedback_outcome is None

    def test_feedback_notes_is_nullable(self, test_recommendation):
        """feedback_notes defaults to None (nullable)."""
        assert test_recommendation.feedback_notes is None

    def test_feedback_recorded_at_is_nullable(self, test_recommendation):
        """feedback_recorded_at defaults to None (nullable)."""
        assert test_recommendation.feedback_recorded_at is None

    def test_can_set_feedback_outcome(self, test_recommendation):
        """feedback_outcome can be set to any valid choice and saved."""
        test_recommendation.feedback_outcome = 'connected_promising'
        test_recommendation.feedback_notes = 'Great conversation, planning a webinar together.'
        test_recommendation.feedback_recorded_at = timezone.now()
        test_recommendation.save()

        refreshed = PartnerRecommendation.objects.get(pk=test_recommendation.pk)
        assert refreshed.feedback_outcome == 'connected_promising'
        assert 'Great conversation' in refreshed.feedback_notes
        assert refreshed.feedback_recorded_at is not None


# =========================================================================
# B5: MatchLearningSignal model
# =========================================================================


@pytest.mark.django_db
class TestMatchLearningSignal:
    """Tests for the MatchLearningSignal model."""

    def test_create_with_required_fields(self, test_recommendation):
        """Can create a signal with all required fields."""
        now = timezone.now()
        signal = MatchLearningSignal.objects.create(
            match=test_recommendation,
            outcome='connected_promising',
            outcome_timestamp=now,
            match_score=0.85,
            signal_type=MatchLearningSignal.SignalType.FEEDBACK_TIER2,
        )
        assert signal.pk is not None
        assert signal.outcome == 'connected_promising'
        assert signal.match_score == 0.85

    def test_signal_type_has_four_choices(self):
        """SignalType.choices should have exactly 4 entries."""
        choices = MatchLearningSignal.SignalType.choices
        assert len(choices) == 4

    def test_signal_type_values(self):
        """SignalType contains the expected value set."""
        values = {choice[0] for choice in MatchLearningSignal.SignalType.choices}
        expected = {'feedback_tier2', 'contact_made', 'view_pattern', 'outreach_used'}
        assert values == expected

    def test_fk_to_partner_recommendation(self, test_recommendation):
        """FK to PartnerRecommendation works and related_name is 'learning_signals'."""
        now = timezone.now()
        signal = MatchLearningSignal.objects.create(
            match=test_recommendation,
            outcome='contact_made',
            outcome_timestamp=now,
            match_score=0.72,
            signal_type=MatchLearningSignal.SignalType.CONTACT_MADE,
        )
        assert signal.match_id == test_recommendation.pk
        assert test_recommendation.learning_signals.count() == 1
        assert test_recommendation.learning_signals.first().pk == signal.pk

    def test_json_fields_accept_dicts(self, test_recommendation):
        """confidence_at_generation and signal_details accept dict values."""
        now = timezone.now()
        signal = MatchLearningSignal.objects.create(
            match=test_recommendation,
            outcome='view_pattern',
            outcome_timestamp=now,
            match_score=0.60,
            signal_type=MatchLearningSignal.SignalType.VIEW_PATTERN,
            confidence_at_generation={'data_richness': 'high', 'explanation_confidence': 'medium'},
            signal_details={'view_count': 5, 'time_to_action': '2h30m'},
        )

        refreshed = MatchLearningSignal.objects.get(pk=signal.pk)
        assert refreshed.confidence_at_generation['data_richness'] == 'high'
        assert refreshed.signal_details['view_count'] == 5

    def test_default_ordering_is_negative_created_at(self):
        """Meta.ordering is ['-created_at']."""
        assert MatchLearningSignal._meta.ordering == ['-created_at']

    def test_meta_has_two_indexes(self):
        """Meta should define 2 indexes."""
        indexes = MatchLearningSignal._meta.indexes
        assert len(indexes) == 2

        # Verify index field compositions
        index_fields = [tuple(idx.fields) for idx in indexes]
        assert ('signal_type', 'outcome') in index_fields
        assert ('explanation_source', 'outcome') in index_fields
