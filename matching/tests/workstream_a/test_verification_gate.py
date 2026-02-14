"""
Tests for VerificationGate orchestrator (all layers of the Verification Gate).

Validates the full evaluation pipeline: Layer 1 deterministic checks,
Layer 2 source quote verification, gate status determination, confidence
scoring, and the apply_fixes utility.
"""

import pytest

from matching.enrichment.verification_gate import (
    FieldStatus,
    GateStatus,
    GateVerdict,
    VerificationGate,
)


@pytest.fixture
def gate():
    return VerificationGate(enable_ai_verification=False)


# =========================================================================
# Gate status: VERIFIED
# =========================================================================


def test_all_fields_pass_verified(gate):
    """When all fields pass Layer 1 and no AI metadata exists, status is VERIFIED
    with confidence 1.0."""
    data = {
        'email': 'jane@realcompany.com',
        'website': 'https://realcompany.com',
        'seeking': 'Growth partners',
    }
    verdict = gate.evaluate(data)
    assert verdict.status == GateStatus.VERIFIED
    assert verdict.overall_confidence == 1.0


def test_empty_data_verified(gate):
    """Empty data with no fields to check should result in VERIFIED."""
    verdict = gate.evaluate({})
    assert verdict.status == GateStatus.VERIFIED
    assert verdict.overall_confidence == 1.0


def test_verified_with_source_verification_passing(gate):
    """When Layer 1 passes and Layer 2 source verification also passes,
    the gate should be VERIFIED with confidence 1.0."""
    raw = 'We connect entrepreneurs with growth capital and strategic advisory services.'
    data = {
        'email': 'ceo@goodfirm.com',
        'seeking': 'Growth capital and strategic advisory',
    }
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': ['connect entrepreneurs with growth capital and strategic advisory services'],
        'source': 'website',
    }
    verdict = gate.evaluate(data, raw_content=raw, extraction_metadata=metadata)
    assert verdict.status == GateStatus.VERIFIED
    assert verdict.overall_confidence == 1.0


# =========================================================================
# Gate status: QUARANTINED
# =========================================================================


def test_critical_email_failure_quarantined(gate):
    """A failed email (critical field) at Layer 1 should immediately QUARANTINE
    with confidence 0.0 and layer_stopped_at 1."""
    data = {'email': 'not-valid'}
    verdict = gate.evaluate(data)
    assert verdict.status == GateStatus.QUARANTINED
    assert verdict.overall_confidence == 0.0
    assert verdict.provenance.get('layer_stopped_at') == 1


def test_email_url_swap_quarantined(gate):
    """A URL in the email field is a critical failure -> QUARANTINED."""
    data = {'email': 'https://example.com', 'website': 'https://example.com'}
    verdict = gate.evaluate(data)
    assert verdict.status == GateStatus.QUARANTINED
    assert verdict.overall_confidence == 0.0


# =========================================================================
# Gate status: UNVERIFIED
# =========================================================================


def test_non_critical_failure_unverified_0_6(gate):
    """A non-critical failure (e.g. bad website) with a valid email should
    produce UNVERIFIED status with confidence 0.6."""
    data = {
        'email': 'real@company.com',
        'website': 'not a url',
    }
    verdict = gate.evaluate(data)
    assert verdict.status == GateStatus.UNVERIFIED
    assert verdict.overall_confidence == 0.6


def test_source_verification_failure_unverified_0_5(gate):
    """When source verification (Layer 2) fails, the status should be
    UNVERIFIED with confidence 0.5."""
    raw = 'This company sells industrial equipment exclusively.'
    data = {
        'email': 'sales@industrial.com',
        'seeking': 'SaaS partners for healthcare vertical',
    }
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': ['SaaS partners for healthcare vertical expansion in Asia'],
        'source': 'website',
    }
    verdict = gate.evaluate(data, raw_content=raw, extraction_metadata=metadata)
    assert verdict.status == GateStatus.UNVERIFIED
    assert verdict.overall_confidence == 0.5


def test_extraction_metadata_without_raw_content_unverified_0_7(gate):
    """AI-extracted data present (extraction_metadata) but no raw_content to
    verify against should produce UNVERIFIED with confidence 0.7."""
    data = {
        'email': 'ceo@startup.io',
        'seeking': 'Investor introductions',
    }
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': [],
        'source': 'website',
    }
    verdict = gate.evaluate(data, raw_content=None, extraction_metadata=metadata)
    assert verdict.status == GateStatus.UNVERIFIED
    assert verdict.overall_confidence == 0.7


# =========================================================================
# Layer interaction: L2 does not overwrite L1 failures
# =========================================================================


def test_layer2_does_not_overwrite_layer1_failures(gate):
    """Layer 2 verdicts should only supplement PASSED fields, never
    overwrite a Layer 1 failure for the same field."""
    # Use a non-critical field that fails L1 (e.g. placeholder in 'seeking')
    raw = 'TBD is our seeking value.'
    data = {
        'email': 'real@company.com',
        'seeking': 'TBD',  # will be AUTO_FIXED by L1
    }
    metadata = {
        'fields_updated': ['seeking'],
        'source_quotes': ['TBD is our seeking value'],
        'source': 'website',
    }
    verdict = gate.evaluate(data, raw_content=raw, extraction_metadata=metadata)
    # L1 already set seeking to AUTO_FIXED; L2 should NOT overwrite it
    assert verdict.field_verdicts['seeking'].status == FieldStatus.AUTO_FIXED


# =========================================================================
# apply_fixes
# =========================================================================


def test_apply_fixes_replaces_auto_fixed_values(gate):
    """apply_fixes should return a new dict with auto-fixed values applied."""
    data = {
        'email': 'n/a',
        'website': 'www.company.com',
        'seeking': 'Growth partners',
    }
    verdict = gate.evaluate(data)
    fixed = VerificationGate.apply_fixes(data, verdict)

    # email placeholder cleared
    assert fixed['email'] == ''
    # website got https:// prefix
    assert fixed['website'] == 'https://www.company.com'
    # seeking untouched
    assert fixed['seeking'] == 'Growth partners'


def test_apply_fixes_does_not_mutate_original(gate):
    """apply_fixes should not modify the original data dict."""
    data = {'email': 'n/a', 'website': 'www.example.com'}
    verdict = gate.evaluate(data)
    fixed = VerificationGate.apply_fixes(data, verdict)

    assert data['email'] == 'n/a'
    assert data['website'] == 'www.example.com'
    assert fixed is not data
