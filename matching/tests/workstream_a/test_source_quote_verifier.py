"""
Tests for SourceQuoteVerifier (Layer 2 of the Verification Gate).

Validates that AI-extracted field values are grounded in raw website content
via substring matching, fuzzy matching, and key-phrase fallback.
"""

import pytest

from matching.enrichment.verification_gate import (
    FieldStatus,
    FieldVerdict,
    SourceQuoteVerifier,
)


@pytest.fixture
def verifier():
    return SourceQuoteVerifier()


# =========================================================================
# Guard clauses — early returns
# =========================================================================


def test_returns_empty_when_raw_content_is_none(verifier):
    """Verification is skipped entirely when raw_content is None."""
    data = {'seeking': 'Growth partners'}
    metadata = {'fields_updated': ['seeking'], 'source_quotes': []}
    result = verifier.verify(data, raw_content=None, extraction_metadata=metadata)
    assert result == {}


def test_returns_empty_when_extraction_metadata_is_none(verifier):
    """Verification is skipped entirely when extraction_metadata is None."""
    data = {'seeking': 'Growth partners'}
    result = verifier.verify(data, raw_content='some content', extraction_metadata=None)
    assert result == {}


def test_returns_empty_when_no_fields_updated(verifier):
    """If fields_updated is empty, there is nothing to verify."""
    data = {'seeking': 'Growth partners'}
    metadata = {'fields_updated': [], 'source_quotes': []}
    result = verifier.verify(data, raw_content='some content', extraction_metadata=metadata)
    assert result == {}


def test_skips_non_ai_extracted_fields(verifier):
    """Fields not in AI_EXTRACTED_FIELDS (like 'email') are never checked."""
    data = {'email': 'test@example.com'}
    metadata = {'fields_updated': ['email'], 'source_quotes': ['test@example.com is a test']}
    result = verifier.verify(data, raw_content='test@example.com is a test', extraction_metadata=metadata)
    assert 'email' not in result


# =========================================================================
# Source quote matching
# =========================================================================


def test_exact_substring_match_passes(verifier):
    """When the source quote appears verbatim in raw content, the field PASSES."""
    raw = 'We specialize in helping small business owners grow their revenue through strategic partnerships.'
    quote = 'helping small business owners grow their revenue'
    data = {'seeking': 'Strategic growth partners for small business owners'}
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': [quote],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['seeking'].status == FieldStatus.PASSED
    assert result['seeking'].source_verified is True


def test_short_quote_skipped(verifier):
    """Quotes shorter than MIN_QUOTE_LENGTH (20 chars) are skipped."""
    raw = 'We do consulting.'
    # The only quote is short — should be skipped, then fallback applies
    data = {'offering': 'We do consulting and advisory services for enterprise clients worldwide'}
    metadata = {
        'fields_updated': ['offering'],
        'source_quotes': ['short quote'],  # 11 chars, below 20
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    # With the short quote skipped, fallback key-phrase check runs.
    # The value is long but largely not in raw_content, so it should FAIL.
    assert result['offering'].status == FieldStatus.FAILED
    assert result['offering'].source_verified is False


def test_fuzzy_match_above_threshold_passes(verifier):
    """A quote that fuzzy-matches the raw content above 0.75 should PASS."""
    # Nearly identical text — high fuzzy ratio
    raw = 'Our mission is to empower entrepreneurs with capital and mentorship'
    quote = 'Our mission is to empower entrepreneurs with capital and mentoring'  # 'mentoring' vs 'mentorship'
    data = {'what_you_do': 'Empowering entrepreneurs with capital and mentorship'}
    metadata = {
        'fields_updated': ['what_you_do'],
        'source_quotes': [quote],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['what_you_do'].status == FieldStatus.PASSED
    assert result['what_you_do'].source_verified is True


def test_no_match_fails(verifier):
    """When neither quote nor value matches raw content, the field FAILS."""
    raw = 'This company sells industrial equipment and heavy machinery.'
    data = {'seeking': 'Looking for SaaS partners in the healthcare vertical'}
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': ['SaaS healthcare partnership opportunities are abundant'],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['seeking'].status == FieldStatus.FAILED
    assert result['seeking'].source_verified is False
    assert any('not verified' in i for i in result['seeking'].issues)


def test_key_phrase_fallback_passes(verifier):
    """When no source quotes match but key phrases from the value appear in raw
    content (>=50%), the field should PASS via fallback."""
    raw = (
        'We provide management consulting and leadership development programs '
        'for Fortune 500 companies across North America.'
    )
    # No source quotes at all
    data = {'offering': 'management consulting, leadership development programs, Fortune 500 companies'}
    metadata = {
        'fields_updated': ['offering'],
        'source_quotes': [],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['offering'].status == FieldStatus.PASSED
    assert result['offering'].source_verified is True


def test_key_phrase_fallback_fails_when_few_phrases_match(verifier):
    """When fewer than 50% of key phrases appear in raw content, the fallback
    should not rescue the field — it should FAIL."""
    raw = 'We build custom software for logistics companies.'
    data = {
        'offering': 'AI-powered analytics, blockchain integration, quantum computing solutions'
    }
    metadata = {
        'fields_updated': ['offering'],
        'source_quotes': [],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['offering'].status == FieldStatus.FAILED
    assert result['offering'].source_verified is False


def test_multiple_ai_fields_verified_independently(verifier):
    """Each AI-extracted field should be verified independently: one grounded
    in raw content via key-phrase fallback, the other completely fabricated."""
    raw = (
        'We sell industrial plumbing equipment, serve contractors in the Midwest, '
        'and provide installation services.'
    )
    data = {
        # Comma-separated phrases that split into substrings found in raw
        'seeking': 'industrial plumbing equipment, contractors in the Midwest',
        # Completely unrelated — no overlap with raw content at all
        'offering': 'quantum blockchain tokenization, interplanetary logistics orchestration',
    }
    metadata = {
        'fields_updated': ['seeking', 'offering'],
        'source_quotes': [],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert result['seeking'].status == FieldStatus.PASSED
    assert result['offering'].status == FieldStatus.FAILED


def test_empty_field_value_skipped(verifier):
    """If the field value is empty/blank, it should be skipped entirely."""
    raw = 'Some raw website content here with enough length to test.'
    data = {'seeking': ''}
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': ['Some raw website content here'],
    }
    result = verifier.verify(data, raw_content=raw, extraction_metadata=metadata)
    assert 'seeking' not in result
