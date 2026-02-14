"""
Tests for matching/enrichment/retry_strategy.py (Workstream A6)

Covers:
    - FailureClassifier: mapping GateVerdict field verdicts to FailureType constants
    - RetryStrategySelector: mapping (field, failure_type) to ordered enrichment methods
    - LearningLog: append-only JSONL recording, read-back, and success_rate calculation

No Django ORM needed. Pure pytest style.
"""

import json
import os

import pytest

from matching.enrichment.verification_gate import (
    FieldStatus,
    FieldVerdict,
    GateStatus,
    GateVerdict,
)
from matching.enrichment.retry_strategy import (
    FailureClassifier,
    FailureType,
    FieldFailure,
    LearningLog,
    RetryPlan,
    RetryStrategySelector,
)


# =========================================================================
# Helpers
# =========================================================================

def _make_verdict(field_verdicts: dict) -> GateVerdict:
    """Convenience: build a GateVerdict from a dict of FieldVerdict objects."""
    return GateVerdict(
        status=GateStatus.QUARANTINED,
        field_verdicts=field_verdicts,
        overall_confidence=0.0,
    )


def _fv(name, status, original_value=None, issues=None, source_verified=None):
    """Shorthand FieldVerdict builder."""
    return FieldVerdict(
        field_name=name,
        status=status,
        original_value=original_value,
        issues=issues or [],
        source_verified=source_verified,
    )


# =========================================================================
# FailureClassifier tests
# =========================================================================

class TestFailureClassifierSkipsPassedFields:
    """FailureClassifier.classify should skip PASSED fields entirely."""

    def test_passed_field_is_skipped(self):
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.PASSED, original_value='ok@example.com'),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures == []

    def test_mixed_passed_and_failed(self):
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.PASSED, original_value='ok@example.com'),
            'website': _fv('website', FieldStatus.FAILED, original_value='not-a-url',
                           issues=['Invalid website URL format: not-a-url']),
        })
        failures = FailureClassifier().classify(verdict)
        assert len(failures) == 1
        assert failures[0].field_name == 'website'


class TestFailureClassifierMissingData:
    """AUTO_FIXED + placeholder, or empty original_value -> missing_data."""

    def test_auto_fixed_placeholder(self):
        verdict = _make_verdict({
            'seeking': _fv('seeking', FieldStatus.AUTO_FIXED,
                           original_value='n/a',
                           issues=['Placeholder value cleared']),
        })
        failures = FailureClassifier().classify(verdict)
        assert len(failures) == 1
        assert failures[0].failure_type == FailureType.MISSING_DATA

    def test_empty_original_value(self):
        verdict = _make_verdict({
            'offering': _fv('offering', FieldStatus.FAILED,
                            original_value='',
                            issues=['Some issue']),
        })
        failures = FailureClassifier().classify(verdict)
        assert len(failures) == 1
        assert failures[0].failure_type == FailureType.MISSING_DATA

    def test_none_original_value(self):
        verdict = _make_verdict({
            'offering': _fv('offering', FieldStatus.FAILED,
                            original_value=None,
                            issues=['Some issue']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.MISSING_DATA


class TestFailureClassifierHallucination:
    """source_verified=False -> hallucination."""

    def test_source_verified_false(self):
        verdict = _make_verdict({
            'seeking': _fv('seeking', FieldStatus.FAILED,
                           original_value='Some real text here',
                           issues=['Source quote not verified for seeking'],
                           source_verified=False),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.HALLUCINATION


class TestFailureClassifierEmailSpecific:
    """Email field with specific issue strings."""

    def test_url_in_email_field_swap(self):
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.FAILED,
                         original_value='https://example.com',
                         issues=['URL found in email field']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.FIELD_SWAP

    def test_invalid_email_format(self):
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.FAILED,
                         original_value='not-an-email',
                         issues=['Invalid email format: not-an-email']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.EMAIL_INVALID

    def test_suspicious_email_pattern(self):
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.FAILED,
                         original_value='info@company.com',
                         issues=['Suspicious email pattern: info@company.com']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.EMAIL_SUSPICIOUS


class TestFailureClassifierURLAndLinkedIn:
    """URL/LinkedIn-specific failure types."""

    def test_invalid_website_url(self):
        verdict = _make_verdict({
            'website': _fv('website', FieldStatus.FAILED,
                           original_value='ftp://badsite',
                           issues=['Invalid website URL format: ftp://badsite']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.URL_INVALID

    def test_invalid_linkedin_url(self):
        verdict = _make_verdict({
            'linkedin': _fv('linkedin', FieldStatus.FAILED,
                            original_value='not-a-linkedin',
                            issues=['Invalid LinkedIn URL: not-a-linkedin']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.LINKEDIN_INVALID


class TestFailureClassifierGenericFallbacks:
    """Generic FAILED -> validation_failed; other AUTO_FIXED -> format_error."""

    def test_generic_failed(self):
        verdict = _make_verdict({
            'bio': _fv('bio', FieldStatus.FAILED,
                       original_value='Some bio text',
                       issues=['Some unknown issue']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.VALIDATION_FAILED

    def test_auto_fixed_without_placeholder_is_format_error(self):
        verdict = _make_verdict({
            'bio': _fv('bio', FieldStatus.AUTO_FIXED,
                       original_value='Some bio text',
                       issues=['Text sanitized (encoding/whitespace)']),
        })
        failures = FailureClassifier().classify(verdict)
        assert failures[0].failure_type == FailureType.FORMAT_ERROR


# =========================================================================
# RetryStrategySelector tests
# =========================================================================

class TestRetryStrategySelectorDefaults:
    """Default strategy lookup and wildcard fallback."""

    def test_known_email_invalid_strategy(self):
        selector = RetryStrategySelector()
        failures = [FieldFailure('email', FailureType.EMAIL_INVALID, 'bad')]
        result = selector.select(failures)
        assert result['email'] == ['apollo_api', 'owl_full', 'deep_research']

    def test_known_seeking_hallucination_strategy(self):
        selector = RetryStrategySelector()
        failures = [FieldFailure('seeking', FailureType.HALLUCINATION, 'hal')]
        result = selector.select(failures)
        assert result['seeking'] == ['deep_research', 'owl_full']

    def test_unknown_combo_falls_back_to_wildcard(self):
        selector = RetryStrategySelector()
        failures = [FieldFailure('custom_field', 'totally_unknown_type')]
        result = selector.select(failures)
        assert result['custom_field'] == ['owl_full']

    def test_custom_strategies_override(self):
        custom = {('email', 'email_invalid'): ['my_custom_method']}
        selector = RetryStrategySelector(custom_strategies=custom)
        failures = [FieldFailure('email', FailureType.EMAIL_INVALID)]
        result = selector.select(failures)
        assert result['email'] == ['my_custom_method']

    def test_fourteen_default_strategies_exist(self):
        assert len(RetryStrategySelector.DEFAULT_STRATEGIES) == 14


class TestRetryStrategySelectorBuildPlan:
    """build_retry_plan integrates classifier + selector."""

    def test_build_retry_plan_returns_retry_plan(self):
        selector = RetryStrategySelector()
        verdict = _make_verdict({
            'email': _fv('email', FieldStatus.FAILED,
                         original_value='bad-email',
                         issues=['Invalid email format: bad-email']),
        })
        plan = selector.build_retry_plan(
            profile_id='prof-123',
            profile_name='Test User',
            verdict=verdict,
            attempt_number=2,
        )
        assert isinstance(plan, RetryPlan)
        assert plan.profile_id == 'prof-123'
        assert plan.profile_name == 'Test User'
        assert plan.attempt_number == 2
        assert len(plan.failures) == 1
        assert plan.failures[0].failure_type == FailureType.EMAIL_INVALID
        assert 'email' in plan.strategies
        assert plan.strategies['email'] == ['apollo_api', 'owl_full', 'deep_research']


# =========================================================================
# LearningLog tests
# =========================================================================

class TestLearningLogRecord:
    """LearningLog.record appends JSON lines."""

    def test_record_creates_file_and_appends(self, tmp_path):
        log_path = str(tmp_path / 'sub' / 'learning_log.jsonl')
        log = LearningLog(log_path=log_path)
        log.record(
            profile_id='p1',
            field_name='email',
            failure_type='email_invalid',
            method_tried='apollo_api',
            success=True,
            new_value='found@example.com',
        )
        assert os.path.exists(log_path)
        with open(log_path, 'r') as f:
            lines = f.readlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry['profile_id'] == 'p1'
        assert entry['success'] is True
        assert entry['new_value'] == 'found@example.com'

    def test_record_appends_multiple_entries(self, tmp_path):
        log_path = str(tmp_path / 'learning_log.jsonl')
        log = LearningLog(log_path=log_path)
        for i in range(3):
            log.record(
                profile_id=f'p{i}',
                field_name='email',
                failure_type='missing_data',
                method_tried='owl_full',
                success=(i % 2 == 0),
            )
        entries = log.read_all()
        assert len(entries) == 3


class TestLearningLogReadAll:
    """LearningLog.read_all reads entries and handles corrupt lines."""

    def test_read_all_returns_empty_for_missing_file(self, tmp_path):
        log = LearningLog(log_path=str(tmp_path / 'nonexistent.jsonl'))
        assert log.read_all() == []

    def test_read_all_skips_corrupt_lines(self, tmp_path):
        log_path = tmp_path / 'learning_log.jsonl'
        log_path.write_text(
            '{"profile_id": "p1", "success": true}\n'
            'NOT JSON AT ALL\n'
            '{"profile_id": "p2", "success": false}\n'
        )
        log = LearningLog(log_path=str(log_path))
        entries = log.read_all()
        assert len(entries) == 2
        assert entries[0]['profile_id'] == 'p1'
        assert entries[1]['profile_id'] == 'p2'


class TestLearningLogSuccessRate:
    """LearningLog.success_rate returns None if <5 records, else a float."""

    def test_returns_none_with_fewer_than_five(self, tmp_path):
        log_path = str(tmp_path / 'learning_log.jsonl')
        log = LearningLog(log_path=log_path)
        for i in range(4):
            log.record(
                profile_id=f'p{i}',
                field_name='email',
                failure_type='email_invalid',
                method_tried='apollo_api',
                success=True,
            )
        rate = log.success_rate('email', 'email_invalid', 'apollo_api')
        assert rate is None

    def test_returns_correct_rate_with_five_records(self, tmp_path):
        log_path = str(tmp_path / 'learning_log.jsonl')
        log = LearningLog(log_path=log_path)
        # 3 successes out of 5
        for i in range(5):
            log.record(
                profile_id=f'p{i}',
                field_name='email',
                failure_type='email_invalid',
                method_tried='apollo_api',
                success=(i < 3),
            )
        rate = log.success_rate('email', 'email_invalid', 'apollo_api')
        assert rate == pytest.approx(0.6)

    def test_filters_by_field_failure_type_and_method(self, tmp_path):
        log_path = str(tmp_path / 'learning_log.jsonl')
        log = LearningLog(log_path=log_path)

        # 5 records for email/email_invalid/apollo_api — all success
        for i in range(5):
            log.record(
                profile_id=f'p{i}',
                field_name='email',
                failure_type='email_invalid',
                method_tried='apollo_api',
                success=True,
            )

        # 5 records for email/email_invalid/owl_full — all failure
        for i in range(5):
            log.record(
                profile_id=f'q{i}',
                field_name='email',
                failure_type='email_invalid',
                method_tried='owl_full',
                success=False,
            )

        assert log.success_rate('email', 'email_invalid', 'apollo_api') == pytest.approx(1.0)
        assert log.success_rate('email', 'email_invalid', 'owl_full') == pytest.approx(0.0)

    def test_creates_directory_if_missing(self, tmp_path):
        deep_path = str(tmp_path / 'a' / 'b' / 'c' / 'learning_log.jsonl')
        log = LearningLog(log_path=deep_path)
        assert os.path.isdir(os.path.dirname(deep_path))
