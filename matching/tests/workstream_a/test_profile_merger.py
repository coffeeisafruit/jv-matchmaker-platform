"""
Tests for ProfileMerger — confidence-aware profile field merging.

Covers _simple_merge fallback, merge_field with and without metadata,
_get_current_confidence calculation, and merge_profile_metadata.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from matching.enrichment.consolidation.profile_merger import ProfileMerger


@pytest.fixture
def merger():
    return ProfileMerger()


# ---------------------------------------------------------------------------
# _simple_merge
# ---------------------------------------------------------------------------

class TestSimpleMerge:

    def test_keeps_longer_value(self, merger):
        result = merger._simple_merge('short', 'much longer value')
        assert result == 'much longer value'

    def test_one_none_returns_other(self, merger):
        result = merger._simple_merge(None, 'value')
        assert result == 'value'

    def test_both_none_returns_empty(self, merger):
        result = merger._simple_merge(None, None)
        assert result == ''

    def test_equal_length_keeps_first(self, merger):
        # Both are 5 characters; >= comparison means first wins
        result = merger._simple_merge('alpha', 'bravo')
        assert result == 'alpha'


# ---------------------------------------------------------------------------
# merge_field — no metadata (falls back to simple merge)
# ---------------------------------------------------------------------------

class TestMergeFieldNoMetadata:

    def test_no_metadata_uses_simple_merge(self, merger):
        val, meta = merger.merge_field('email', 'a@b.co', None, 'longer@example.com', None)
        assert val == 'longer@example.com'
        assert meta == {}


# ---------------------------------------------------------------------------
# merge_field — with metadata (confidence-based)
# ---------------------------------------------------------------------------

class TestMergeFieldWithMetadata:

    def test_higher_confidence_wins(self, merger):
        meta1 = {'source': 'apollo', 'confidence': 0.3}
        meta2 = {'source': 'apollo_verified', 'confidence': 0.95}
        val, meta = merger.merge_field('email', 'old@example.com', meta1, 'new@example.com', meta2)
        assert val == 'new@example.com'
        assert meta is meta2

    def test_equal_confidence_keeps_first(self, merger):
        meta1 = {'source': 'apollo', 'confidence': 0.80}
        meta2 = {'source': 'owl', 'confidence': 0.80}
        val, meta = merger.merge_field('email', 'first@example.com', meta1, 'second@example.com', meta2)
        assert val == 'first@example.com'
        assert meta is meta1

    def test_one_has_metadata_other_doesnt(self, merger):
        meta2 = {'source': 'apollo', 'confidence': 0.70}
        val, meta = merger.merge_field('email', 'no_meta@example.com', None, 'has_meta@example.com', meta2)
        # metadata1 is None so conf1 = 0.0; conf2 = 0.70 > 0.0 => value2 wins
        assert val == 'has_meta@example.com'
        assert meta is meta2


# ---------------------------------------------------------------------------
# _get_current_confidence
# ---------------------------------------------------------------------------

class TestGetCurrentConfidence:

    def test_no_metadata_returns_zero(self, merger):
        result = merger._get_current_confidence('email', 'test@example.com', None)
        assert result == 0.0

    def test_no_value_returns_zero(self, merger):
        result = merger._get_current_confidence('email', None, {'confidence': 0.9})
        assert result == 0.0

    def test_stored_confidence_used_when_no_enriched_at(self, merger):
        metadata = {'confidence': 0.75}
        result = merger._get_current_confidence('email', 'test@example.com', metadata)
        assert result == 0.75

    def test_recalculates_from_enriched_at(self, merger):
        enriched_at = datetime.now() - timedelta(hours=1)
        metadata = {
            'source': 'apollo_verified',
            'enriched_at': enriched_at.isoformat(),
            'confidence': 0.50,  # stale stored value — should be recalculated
        }
        result = merger._get_current_confidence('email', 'test@example.com', metadata)
        # Fresh apollo_verified email => base 0.95, almost no decay
        assert result == pytest.approx(0.95, abs=0.05)


# ---------------------------------------------------------------------------
# merge_profile_metadata
# ---------------------------------------------------------------------------

class TestMergeProfileMetadata:

    def test_merges_unique_fields(self, merger):
        metadata1 = {
            'email': {'source': 'apollo', 'confidence': 0.80},
        }
        metadata2 = {
            'seeking': {'source': 'owl', 'confidence': 0.75},
        }
        result = merger.merge_profile_metadata(metadata1, metadata2)
        assert 'email' in result
        assert 'seeking' in result
        assert result['email']['confidence'] == 0.80
        assert result['seeking']['confidence'] == 0.75

    def test_higher_confidence_field_wins(self, merger):
        metadata1 = {
            'email': {'source': 'apollo', 'confidence': 0.60},
        }
        metadata2 = {
            'email': {'source': 'apollo_verified', 'confidence': 0.95},
        }
        result = merger.merge_profile_metadata(metadata1, metadata2)
        assert result['email']['source'] == 'apollo_verified'
        assert result['email']['confidence'] == 0.95

    def test_one_empty_field_uses_other(self, merger):
        metadata1 = {
            'email': {},
        }
        metadata2 = {
            'email': {'source': 'apollo', 'confidence': 0.80},
        }
        result = merger.merge_profile_metadata(metadata1, metadata2)
        assert result['email']['confidence'] == 0.80
