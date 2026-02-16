"""
Tests for ConfidenceScorer — pure Python, no Django ORM needed.

Covers age decay, verification boost, cross-validation boost,
full confidence calculation, expiry calculation, and profile-level confidence.
"""

import math
from datetime import datetime, timedelta

import pytest

from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer


@pytest.fixture
def scorer():
    return ConfidenceScorer()


# ---------------------------------------------------------------------------
# _calculate_age_decay
# ---------------------------------------------------------------------------

class TestCalculateAgeDecay:

    def test_zero_days_returns_one(self, scorer):
        result = scorer._calculate_age_decay(0, 90)
        assert result == pytest.approx(1.0, abs=0.02)

    def test_one_decay_period_returns_approx_0_37(self, scorer):
        result = scorer._calculate_age_decay(90, 90)
        assert result == pytest.approx(1 / math.e, abs=0.02)

    def test_two_decay_periods(self, scorer):
        result = scorer._calculate_age_decay(180, 90)
        expected = math.exp(-2)  # ~0.1353
        assert result == pytest.approx(expected, abs=0.02)

    def test_negative_days_clamped_to_zero(self, scorer):
        result = scorer._calculate_age_decay(-10, 90)
        assert result == pytest.approx(1.0, abs=0.02)


# ---------------------------------------------------------------------------
# _calculate_verification_boost
# ---------------------------------------------------------------------------

class TestCalculateVerificationBoost:

    def test_no_verified_at_returns_zero(self, scorer):
        result = scorer._calculate_verification_boost(None, 0)
        assert result == pytest.approx(0.0, abs=0.02)

    def test_recent_verification_gives_015(self, scorer):
        verified_at = datetime.now() - timedelta(days=1)
        result = scorer._calculate_verification_boost(verified_at, 0)
        assert result == pytest.approx(0.15, abs=0.02)

    def test_30_day_verification_gives_010(self, scorer):
        verified_at = datetime.now() - timedelta(days=15)
        result = scorer._calculate_verification_boost(verified_at, 0)
        assert result == pytest.approx(0.10, abs=0.02)

    def test_old_verification_gives_zero(self, scorer):
        verified_at = datetime.now() - timedelta(days=100)
        result = scorer._calculate_verification_boost(verified_at, 0)
        assert result == pytest.approx(0.0, abs=0.02)

    def test_multiple_verifications_multiply(self, scorer):
        verified_at = datetime.now() - timedelta(days=1)
        result = scorer._calculate_verification_boost(verified_at, 5)
        # base_boost=0.15, multiplier=min(1.0 + 5*0.1, 1.5) = 1.5
        assert result == pytest.approx(0.225, abs=0.02)


# ---------------------------------------------------------------------------
# _calculate_cross_validation_boost
# ---------------------------------------------------------------------------

class TestCalculateCrossValidationBoost:

    def test_no_sources_zero(self, scorer):
        result = scorer._calculate_cross_validation_boost([])
        assert result == pytest.approx(0.0, abs=0.02)

    def test_one_source_zero(self, scorer):
        result = scorer._calculate_cross_validation_boost(['apollo'])
        assert result == pytest.approx(0.0, abs=0.02)

    def test_two_sources_010(self, scorer):
        result = scorer._calculate_cross_validation_boost(['a', 'b'])
        assert result == pytest.approx(0.10, abs=0.02)

    def test_three_sources_020(self, scorer):
        result = scorer._calculate_cross_validation_boost(['a', 'b', 'c'])
        assert result == pytest.approx(0.20, abs=0.02)


# ---------------------------------------------------------------------------
# calculate_confidence (integration)
# ---------------------------------------------------------------------------

class TestCalculateConfidence:

    def test_fresh_apollo_verified_email(self, scorer):
        result = scorer.calculate_confidence('email', 'apollo_verified', datetime.now())
        assert result == pytest.approx(0.95, abs=0.02)

    def test_old_seeking_decayed(self, scorer):
        enriched_at = datetime.now() - timedelta(days=60)
        result = scorer.calculate_confidence('seeking', 'owl', enriched_at)
        # 0.85 * exp(-60/30) = 0.85 * exp(-2) ≈ 0.85 * 0.1353 ≈ 0.115
        assert result < 0.50

    def test_unknown_source_base(self, scorer):
        result = scorer.calculate_confidence('email', 'unknown', datetime.now())
        assert result == pytest.approx(0.30, abs=0.02)

    def test_clamped_to_one(self, scorer):
        # Fresh manual data (1.0) + recent verification (0.15) + 3-source cross-val (0.20)
        result = scorer.calculate_confidence(
            'email', 'manual',
            enriched_at=datetime.now(),
            verified_at=datetime.now() - timedelta(days=1),
            verification_count=0,
            cross_validated_by=['a', 'b', 'c'],
        )
        assert result <= 1.0


# ---------------------------------------------------------------------------
# calculate_expires_at
# ---------------------------------------------------------------------------

class TestCalculateExpiresAt:

    def test_email_expires_in_62_days(self, scorer):
        now = datetime.now()
        expires = scorer.calculate_expires_at('email', now, confidence_threshold=0.5)
        # days = -90 * ln(0.5) ≈ 62.38 → int → 62
        days_until = (expires - now).days
        assert days_until == pytest.approx(62, abs=2)


# ---------------------------------------------------------------------------
# calculate_profile_confidence
# ---------------------------------------------------------------------------

class TestCalculateProfileConfidence:

    def test_weighted_average(self, scorer):
        metadata = {
            'email': {'confidence': 0.95},    # weight 3.0
            'seeking': {'confidence': 0.75},  # weight 2.0
        }
        # weighted avg = (0.95*3.0 + 0.75*2.0) / (3.0+2.0) = (2.85+1.50)/5.0 = 0.87
        result = scorer.calculate_profile_confidence(metadata)
        assert result == pytest.approx(0.87, abs=0.02)

    def test_empty_metadata_returns_zero(self, scorer):
        result = scorer.calculate_profile_confidence({})
        assert result == pytest.approx(0.0, abs=0.02)
