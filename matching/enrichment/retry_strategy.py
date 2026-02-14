"""
Quarantine Retry Strategy (A6)

Classifies verification failures, selects adaptive retry methods,
and records outcomes for learning-based strategy adjustment.

Classes:
    FailureClassifier: GateVerdict → failure type per field
    RetryStrategySelector: (field, failure_type) → ordered enrichment methods
    LearningLog: Append-only JSONL outcome recording
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from matching.enrichment.verification_gate import (
    FieldStatus,
    FieldVerdict,
    GateVerdict,
)

logger = logging.getLogger(__name__)


# =========================================================================
# FAILURE CLASSIFICATION
# =========================================================================

class FailureType:
    """Failure type constants for retry strategy selection."""
    HALLUCINATION = 'hallucination'
    MISSING_DATA = 'missing_data'
    FORMAT_ERROR = 'format_error'
    EMAIL_INVALID = 'email_invalid'
    EMAIL_SUSPICIOUS = 'email_suspicious'
    FIELD_SWAP = 'field_swap'
    URL_INVALID = 'url_invalid'
    LINKEDIN_INVALID = 'linkedin_invalid'
    VALIDATION_FAILED = 'validation_failed'


@dataclass
class FieldFailure:
    """A classified failure for a single field."""
    field_name: str
    failure_type: str
    original_value: Optional[str] = None
    issues: List[str] = field(default_factory=list)


class FailureClassifier:
    """
    Classifies GateVerdict field failures into actionable failure types.

    Maps DeterministicChecker issue strings → failure categories
    that RetryStrategySelector can use to pick enrichment methods.
    """

    def classify(self, verdict: GateVerdict) -> List[FieldFailure]:
        """
        Classify all failed fields in a gate verdict.

        Args:
            verdict: GateVerdict from VerificationGate.evaluate()

        Returns:
            List of FieldFailure objects for each failed/problematic field
        """
        failures = []

        for field_name, fv in verdict.field_verdicts.items():
            if fv.status == FieldStatus.PASSED:
                continue

            failure_type = self._classify_field(fv)
            failures.append(FieldFailure(
                field_name=field_name,
                failure_type=failure_type,
                original_value=fv.original_value,
                issues=list(fv.issues),
            ))

        return failures

    def _classify_field(self, fv: FieldVerdict) -> str:
        """Classify a single FieldVerdict into a failure type."""
        issues_text = ' '.join(fv.issues).lower()

        # Check for empty/missing data (auto-fixed placeholders or empty values)
        if fv.status == FieldStatus.AUTO_FIXED and 'placeholder' in issues_text:
            return FailureType.MISSING_DATA
        if not fv.original_value or fv.original_value.strip() == '':
            return FailureType.MISSING_DATA

        # Source verification failure → hallucination
        if fv.source_verified is False:
            return FailureType.HALLUCINATION

        # Email-specific failures
        if fv.field_name == 'email':
            if 'url found in email' in issues_text:
                return FailureType.FIELD_SWAP
            if 'invalid email format' in issues_text:
                return FailureType.EMAIL_INVALID
            if 'suspicious email pattern' in issues_text:
                return FailureType.EMAIL_SUSPICIOUS

        # URL/LinkedIn failures
        if 'invalid website url' in issues_text:
            return FailureType.URL_INVALID
        if 'invalid linkedin url' in issues_text:
            return FailureType.LINKEDIN_INVALID

        # Generic format/validation errors
        if fv.status == FieldStatus.FAILED:
            return FailureType.VALIDATION_FAILED

        return FailureType.FORMAT_ERROR


# =========================================================================
# RETRY STRATEGY SELECTION
# =========================================================================

@dataclass
class RetryPlan:
    """A plan for retrying enrichment on a quarantined profile."""
    profile_id: str
    profile_name: str
    failures: List[FieldFailure]
    strategies: Dict[str, List[str]]  # field_name → ordered methods
    attempt_number: int = 1
    max_attempts: int = 3


class RetryStrategySelector:
    """
    Maps (field_name, failure_type) → ordered list of enrichment methods.

    Methods are tried in order; first success wins. Strategy rankings
    can be adjusted over time by LearningLog analysis.
    """

    DEFAULT_STRATEGIES: Dict[Tuple[str, str], List[str]] = {
        ('email', 'email_invalid'):     ['apollo_api', 'owl_full', 'deep_research'],
        ('email', 'missing_data'):      ['apollo_api', 'owl_full', 'deep_research'],
        ('email', 'email_suspicious'):  ['apollo_api', 'owl_full'],
        ('email', 'field_swap'):        ['apollo_api', 'owl_full'],
        ('seeking', 'hallucination'):   ['deep_research', 'owl_full'],
        ('seeking', 'missing_data'):    ['owl_full', 'deep_research'],
        ('who_you_serve', 'hallucination'): ['ai_research', 'owl_full'],
        ('who_you_serve', 'missing_data'):  ['owl_full'],
        ('offering', 'missing_data'):   ['owl_full', 'deep_research'],
        ('offering', 'hallucination'):  ['deep_research', 'owl_full'],
        ('what_you_do', 'missing_data'): ['ai_research', 'owl_full'],
        ('what_you_do', 'hallucination'): ['ai_research', 'owl_full'],
        ('website', 'url_invalid'):     ['owl_full', 'deep_research'],
        ('linkedin', 'linkedin_invalid'): ['owl_full', 'deep_research'],
    }

    WILDCARD_STRATEGY: List[str] = ['owl_full']

    def __init__(self, custom_strategies: Optional[Dict] = None):
        self.strategies = dict(self.DEFAULT_STRATEGIES)
        if custom_strategies:
            self.strategies.update(custom_strategies)

    def select(self, failures: List[FieldFailure]) -> Dict[str, List[str]]:
        """
        Select retry methods for each failed field.

        Args:
            failures: List of FieldFailure from FailureClassifier

        Returns:
            Dict mapping field_name → ordered list of enrichment methods
        """
        result = {}
        for failure in failures:
            key = (failure.field_name, failure.failure_type)

            if key in self.strategies:
                methods = self.strategies[key]
            else:
                # Try wildcard for the failure type
                wildcard_key = ('*', failure.failure_type)
                if wildcard_key in self.strategies:
                    methods = self.strategies[wildcard_key]
                else:
                    methods = list(self.WILDCARD_STRATEGY)

            result[failure.field_name] = methods

        return result

    def build_retry_plan(
        self,
        profile_id: str,
        profile_name: str,
        verdict: GateVerdict,
        attempt_number: int = 1,
    ) -> RetryPlan:
        """
        Build a complete retry plan from a gate verdict.

        Args:
            profile_id: Profile database ID
            profile_name: Profile name for logging
            verdict: GateVerdict from failed verification
            attempt_number: Which retry attempt this is

        Returns:
            RetryPlan with classified failures and selected strategies
        """
        classifier = FailureClassifier()
        failures = classifier.classify(verdict)
        strategies = self.select(failures)

        return RetryPlan(
            profile_id=profile_id,
            profile_name=profile_name,
            failures=failures,
            strategies=strategies,
            attempt_number=attempt_number,
        )


# =========================================================================
# LEARNING LOG
# =========================================================================

class LearningLog:
    """
    Append-only JSONL log for retry outcomes.

    Records (failure_type, field, method, outcome) tuples so
    strategy rankings can be adjusted over time based on actual
    success rates per method per failure type.
    """

    def __init__(self, log_path: Optional[str] = None):
        if log_path is None:
            base = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                'scripts', 'enrichment_batches'
            )
            log_path = os.path.join(base, 'learning_log.jsonl')

        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def record(
        self,
        profile_id: str,
        field_name: str,
        failure_type: str,
        method_tried: str,
        success: bool,
        new_value: Optional[str] = None,
        gate_status_after: Optional[str] = None,
        attempt_number: int = 1,
        notes: str = '',
    ) -> None:
        """
        Record a retry outcome for learning.

        Args:
            profile_id: Profile that was retried
            field_name: Which field was retried
            failure_type: Original failure classification
            method_tried: Enrichment method used
            success: Whether the retry produced a verified value
            new_value: The new value (if successful)
            gate_status_after: Gate verdict after re-verification
            attempt_number: Which attempt this was
            notes: Any additional context
        """
        entry = {
            'profile_id': profile_id,
            'field_name': field_name,
            'failure_type': failure_type,
            'method_tried': method_tried,
            'success': success,
            'new_value': new_value,
            'gate_status_after': gate_status_after,
            'attempt_number': attempt_number,
            'notes': notes,
            'timestamp': datetime.now().isoformat(),
        }

        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
        except OSError as e:
            logger.error(f"Failed to write learning log: {e}")

    def read_all(self) -> List[Dict]:
        """Read all log entries for analysis."""
        entries = []
        if not os.path.exists(self.log_path):
            return entries

        with open(self.log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        return entries

    def success_rate(self, field_name: str, failure_type: str, method: str) -> Optional[float]:
        """
        Calculate success rate for a specific (field, failure_type, method) combo.

        Returns None if fewer than 5 records exist (insufficient data).
        """
        entries = self.read_all()
        matching = [
            e for e in entries
            if e.get('field_name') == field_name
            and e.get('failure_type') == failure_type
            and e.get('method_tried') == method
        ]

        if len(matching) < 5:
            return None

        successes = sum(1 for e in matching if e.get('success'))
        return successes / len(matching)
