"""
Tests for B2 LLM Explanation System in MatchEnrichmentService.

Covers:
- _build_enriched_context: context assembly from profile fields
- _generate_llm_explanation: LLM call + JSON parsing + validation
- _verify_explanation: grounding verification call
- generate_llm_explanation: orchestrator (generate -> verify -> classify)
- _format_llm_why_fit: formatting for PDF WHY FIT section
- _format_llm_mutual_benefit: formatting for MUTUAL BENEFIT section
- enrich_match: integration of LLM path vs template fallback
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from matching.enrichment.match_enrichment import (
    MatchEnrichmentService,
    EnrichedMatch,
    TextSanitizer,
)


# =========================================================================
# _build_enriched_context
# =========================================================================


class TestBuildEnrichedContext:
    """Tests for _build_enriched_context()."""

    def test_full_profile_returns_all_sections(self, sample_client_profile, sample_partner_profile):
        """Profile with bio, signature_programs, current_projects, tags returns all."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._build_enriched_context(sample_partner_profile)

            assert 'Credentials/social proof:' in result
            assert 'Former Google engineer' in result
            assert 'Signature programs:' in result
            assert 'LaunchPad Academy' in result
            assert 'Current projects:' in result
            assert 'AI-powered email sequence builder' in result
            assert 'Keywords/tags:' in result
            assert 'marketing' in result
            assert 'Additional Context (from enrichment)' in result

    def test_empty_profile_returns_fallback(self, sample_client_profile):
        """Profile with no enrichment fields returns fallback message."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._build_enriched_context({'name': 'Empty Profile'})

            assert 'No enriched data available' in result

    def test_tags_as_non_list_treated_as_empty(self, sample_client_profile):
        """Tags that are not a list should be treated as empty (no tags section)."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            profile = {'name': 'Test', 'bio': 'Has bio', 'tags': 'not-a-list'}
            result = svc._build_enriched_context(profile)

            assert 'Keywords/tags:' not in result
            assert 'Credentials/social proof:' in result

    def test_partial_profile_includes_available_fields(self, sample_client_profile):
        """Profile with only some enrichment fields shows those fields."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            profile = {'name': 'Partial', 'bio': 'Some bio text'}
            result = svc._build_enriched_context(profile)

            assert 'Credentials/social proof: Some bio text' in result
            assert 'Signature programs:' not in result
            assert 'No enriched data available' not in result


# =========================================================================
# _generate_llm_explanation
# =========================================================================


class TestGenerateLlmExplanation:
    """Tests for _generate_llm_explanation() (Call 1: Generation)."""

    def test_valid_json_response_returns_dict(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """Valid JSON response is parsed and returned."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = json.dumps(sample_llm_explanation)

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is not None
            assert 'what_partner_b_brings_to_a' in result
            assert 'what_partner_a_brings_to_b' in result

    def test_strips_json_code_fences(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """Response wrapped in ```json fences is handled correctly."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            fenced = f"```json\n{json.dumps(sample_llm_explanation)}\n```"
            mock_service._call_claude.return_value = fenced

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is not None
            assert 'what_partner_b_brings_to_a' in result

    def test_strips_plain_code_fences(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """Response wrapped in plain ``` fences is handled correctly."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            fenced = f"```\n{json.dumps(sample_llm_explanation)}\n```"
            mock_service._call_claude.return_value = fenced

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is not None

    def test_missing_required_keys_returns_none(
        self, sample_client_profile, sample_partner_profile
    ):
        """Response missing required keys returns None."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            # Missing what_partner_a_brings_to_b
            incomplete = json.dumps({'what_partner_b_brings_to_a': {'summary': 'test'}})
            mock_service._call_claude.return_value = incomplete

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is None

    def test_invalid_json_returns_none(
        self, sample_client_profile, sample_partner_profile
    ):
        """Invalid JSON response returns None."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = 'not valid json {{'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is None

    def test_empty_response_returns_none(
        self, sample_client_profile, sample_partner_profile
    ):
        """Empty/None response from API returns None."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = None

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is None

    def test_api_exception_returns_none(
        self, sample_client_profile, sample_partner_profile
    ):
        """Exception during API call returns None."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.side_effect = RuntimeError('API timeout')

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is None

    def test_unavailable_service_returns_none(
        self, sample_client_profile, sample_partner_profile
    ):
        """If service is not available, returns None immediately."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = None

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._generate_llm_explanation(sample_partner_profile)

            assert result is None
            mock_service._call_claude.assert_not_called()


# =========================================================================
# _verify_explanation
# =========================================================================


class TestVerifyExplanation:
    """Tests for _verify_explanation() (Call 2: Verification)."""

    def test_valid_verification_returns_dict(
        self, sample_client_profile, sample_partner_profile,
        sample_llm_explanation, sample_verification_response
    ):
        """Valid verification response is returned as dict."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = json.dumps(sample_verification_response)

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._verify_explanation(sample_llm_explanation, sample_partner_profile)

            assert 'grounded_percentage' in result
            assert result['grounded_percentage'] == 0.85
            assert 'claims' in result
            assert len(result['claims']) == 3

    def test_verification_failure_returns_fallback(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """API failure returns the standard fallback dict."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = None

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._verify_explanation(sample_llm_explanation, sample_partner_profile)

            assert result['claims'] == []
            assert result['grounded_percentage'] == 0.0
            assert result['recommendation'] == 'fall_back_to_template'

    def test_invalid_json_returns_fallback(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """Invalid JSON in verification returns fallback."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = 'totally broken {'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._verify_explanation(sample_llm_explanation, sample_partner_profile)

            assert result['grounded_percentage'] == 0.0
            assert result['recommendation'] == 'fall_back_to_template'

    def test_missing_grounded_percentage_returns_fallback(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """Response without grounded_percentage key returns fallback."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = json.dumps({'claims': [], 'recommendation': 'use_as_is'})

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._verify_explanation(sample_llm_explanation, sample_partner_profile)

            assert result['grounded_percentage'] == 0.0
            assert result['recommendation'] == 'fall_back_to_template'


# =========================================================================
# generate_llm_explanation (public orchestrator)
# =========================================================================


class TestGenerateLlmExplanationOrchestrator:
    """Tests for generate_llm_explanation() orchestration."""

    def test_high_grounding_returns_llm_verified(
        self, sample_client_profile, sample_partner_profile,
        sample_llm_explanation, sample_verification_response
    ):
        """grounded_percentage >= 0.8 returns (explanation, 'llm_verified')."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'

            # Call 1 returns explanation, Call 2 returns verification at 0.85
            mock_service._call_claude.side_effect = [
                json.dumps(sample_llm_explanation),
                json.dumps(sample_verification_response),
            ]

            svc = MatchEnrichmentService(sample_client_profile)
            explanation, source = svc.generate_llm_explanation(sample_partner_profile)

            assert explanation is not None
            assert source == 'llm_verified'

    def test_medium_grounding_returns_llm_partial(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """grounded_percentage >= 0.5 but < 0.8 returns 'llm_partial'."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'

            partial_verification = {
                'claims': [{'claim': 'test', 'status': 'inferred'}],
                'grounded_percentage': 0.65,
                'recommendation': 'remove_ungrounded',
            }
            mock_service._call_claude.side_effect = [
                json.dumps(sample_llm_explanation),
                json.dumps(partial_verification),
            ]

            svc = MatchEnrichmentService(sample_client_profile)
            explanation, source = svc.generate_llm_explanation(sample_partner_profile)

            assert explanation is not None
            assert source == 'llm_partial'

    def test_low_grounding_returns_template_fallback(
        self, sample_client_profile, sample_partner_profile, sample_llm_explanation
    ):
        """grounded_percentage < 0.5 returns (None, 'template_fallback')."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'

            low_verification = {
                'claims': [],
                'grounded_percentage': 0.3,
                'recommendation': 'fall_back_to_template',
            }
            mock_service._call_claude.side_effect = [
                json.dumps(sample_llm_explanation),
                json.dumps(low_verification),
            ]

            svc = MatchEnrichmentService(sample_client_profile)
            explanation, source = svc.generate_llm_explanation(sample_partner_profile)

            assert explanation is None
            assert source == 'template_fallback'

    def test_generation_failure_returns_template_fallback(
        self, sample_client_profile, sample_partner_profile
    ):
        """If _generate_llm_explanation returns None, returns template_fallback."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = None  # generation fails

            svc = MatchEnrichmentService(sample_client_profile)
            explanation, source = svc.generate_llm_explanation(sample_partner_profile)

            assert explanation is None
            assert source == 'template_fallback'


# =========================================================================
# _format_llm_why_fit
# =========================================================================


class TestFormatLlmWhyFit:
    """Tests for _format_llm_why_fit()."""

    def test_formats_summary_and_insights(
        self, sample_client_profile, sample_llm_explanation
    ):
        """Formats summary + connection_insights into multi-paragraph text."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._format_llm_why_fit(sample_llm_explanation, 'Bob Johnson')

            assert 'Bob brings marketing automation' in result
            assert 'Connection:' in result or 'Key insight:' in result

    def test_missing_data_returns_default(self, sample_client_profile):
        """Missing summary and insights returns '[FirstName] is a strong potential JV partner.'"""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            empty_explanation = {
                'what_partner_b_brings_to_a': {},
                'what_partner_a_brings_to_b': {},
            }
            result = svc._format_llm_why_fit(empty_explanation, 'Alice Walker')

            assert 'Alice is a strong potential JV partner.' == result

    def test_truncates_to_520_chars(self, sample_client_profile):
        """Result is truncated to 520 characters max."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            long_explanation = {
                'what_partner_b_brings_to_a': {
                    'summary': 'A' * 300,
                },
                'connection_insights': [
                    {'type': 'obvious', 'insight': 'B' * 200},
                    {'type': 'non_obvious', 'insight': 'C' * 200},
                ],
            }
            result = svc._format_llm_why_fit(long_explanation, 'Test Partner')

            assert len(result) <= 520

    def test_non_obvious_insight_labeled_key_insight(
        self, sample_client_profile, sample_llm_explanation
    ):
        """non_obvious insights get 'Key insight' label."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._format_llm_why_fit(sample_llm_explanation, 'Bob Johnson')

            assert 'Key insight:' in result


# =========================================================================
# _format_llm_mutual_benefit
# =========================================================================


class TestFormatLlmMutualBenefit:
    """Tests for _format_llm_mutual_benefit()."""

    def test_formats_both_sides_as_bullets(
        self, sample_client_profile, sample_llm_explanation
    ):
        """Both sides' key_points are formatted as bullet lists."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._format_llm_mutual_benefit(sample_llm_explanation, 'Bob Johnson')

            assert 'WHAT JANET GETS:' in result
            assert 'WHAT BOB GETS:' in result
            assert '*' in result  # bullet points

    def test_includes_reciprocity_assessment(
        self, sample_client_profile, sample_llm_explanation
    ):
        """Includes reciprocity_assessment explanation when present."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            result = svc._format_llm_mutual_benefit(sample_llm_explanation, 'Bob Johnson')

            assert 'Balance:' in result

    def test_truncates_to_450_chars(self, sample_client_profile):
        """Result is truncated to 450 characters max."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            long_explanation = {
                'what_partner_b_brings_to_a': {
                    'key_points': ['Point ' * 30, 'Point ' * 30, 'Point ' * 30],
                },
                'what_partner_a_brings_to_b': {
                    'key_points': ['Long ' * 30, 'Long ' * 30, 'Long ' * 30],
                },
                'reciprocity_assessment': {
                    'explanation': 'Explanation ' * 20,
                },
            }
            result = svc._format_llm_mutual_benefit(long_explanation, 'Test Partner')

            assert len(result) <= 450

    def test_empty_explanation_returns_fallback(self, sample_client_profile):
        """Empty explanation returns default mutual benefit text."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = 'test-model'

            svc = MatchEnrichmentService(sample_client_profile)
            empty = {
                'what_partner_b_brings_to_a': {},
                'what_partner_a_brings_to_b': {},
            }
            result = svc._format_llm_mutual_benefit(empty, 'Bob Johnson')

            assert 'Mutual cross-promotion and audience sharing opportunity.' == result


# =========================================================================
# enrich_match integration
# =========================================================================


class TestEnrichMatchIntegration:
    """Tests for enrich_match() integration with LLM path."""

    def test_llm_verified_uses_llm_explanations(
        self, sample_client_profile, sample_partner_profile,
        sample_match_data, sample_llm_explanation, sample_verification_response
    ):
        """If LLM succeeds with 'llm_verified', uses LLM-generated explanations."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'

            mock_service._call_claude.side_effect = [
                json.dumps(sample_llm_explanation),
                json.dumps(sample_verification_response),
            ]

            svc = MatchEnrichmentService(sample_client_profile)
            enriched = svc.enrich_match(sample_match_data, sample_partner_profile)

            assert enriched.explanation_source == 'llm_verified'
            # LLM explanation content should be present (summary from explanation)
            assert 'Bob brings marketing automation' in enriched.why_fit

    def test_ai_unavailable_uses_template_fallback(
        self, sample_client_profile, sample_partner_profile, sample_match_data
    ):
        """If ai_service is not available, uses template fallback."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = False
            mock_service.model = None

            svc = MatchEnrichmentService(sample_client_profile)
            enriched = svc.enrich_match(sample_match_data, sample_partner_profile)

            assert enriched.explanation_source == 'template_fallback'
            mock_service._call_claude.assert_not_called()

    def test_llm_failure_falls_back_to_template(
        self, sample_client_profile, sample_partner_profile, sample_match_data
    ):
        """If LLM generation fails entirely, uses template fallback."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'
            mock_service._call_claude.return_value = None  # generation fails

            svc = MatchEnrichmentService(sample_client_profile)
            enriched = svc.enrich_match(sample_match_data, sample_partner_profile)

            assert enriched.explanation_source == 'template_fallback'

    def test_enriched_match_sets_explanation_source(
        self, sample_client_profile, sample_partner_profile,
        sample_match_data, sample_llm_explanation
    ):
        """explanation_source is set correctly on the returned EnrichedMatch."""
        with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
            mock_service = MockCls.return_value
            mock_service.is_available.return_value = True
            mock_service.model = 'test-model'

            partial_verification = {
                'claims': [{'claim': 'test', 'status': 'inferred'}],
                'grounded_percentage': 0.65,
                'recommendation': 'remove_ungrounded',
            }
            mock_service._call_claude.side_effect = [
                json.dumps(sample_llm_explanation),
                json.dumps(partial_verification),
            ]

            svc = MatchEnrichmentService(sample_client_profile)
            enriched = svc.enrich_match(sample_match_data, sample_partner_profile)

            assert enriched.explanation_source == 'llm_partial'
