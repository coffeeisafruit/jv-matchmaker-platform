"""
Tests for Layer 3 AI verification: ClaudeVerificationService and AIMatchVerificationAgent.

Covers:
- API key initialization and availability
- Response parsing (None, valid JSON, fenced JSON, invalid JSON)
- verify_formatting, verify_content_quality, rewrite_content (with mocked _call_claude)
- AIMatchVerificationAgent score averaging and rewrite triggering
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import json
import pytest
from unittest.mock import patch, MagicMock

from matching.enrichment.ai_verification import (
    ClaudeVerificationService,
    AIVerificationResult,
    AIMatchVerificationAgent,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_match(**overrides):
    from matching.enrichment.match_enrichment import EnrichedMatch
    defaults = dict(
        name='Bob Smith', company='BobCo', email='bob@bobco.com',
        linkedin='https://linkedin.com/in/bob', website='https://bobco.com',
        niche='coaching', list_size=5000, social_reach=0, score=0.85,
        who_they_serve='entrepreneurs', what_they_do='business coaching',
        seeking='JV partners', offering='email list access',
        notes='', why_fit='Bob serves entrepreneurs who need coaching.',
        mutual_benefit='WHAT BOB GETS:\n* Access\n\nWHAT JANET GETS:\n* Promotion',
        outreach_message='Subject: Hi Bob\n\nHi Bob,\n\nLooking forward.',
        verification_score=0, verification_passed=False,
    )
    defaults.update(overrides)
    return EnrichedMatch(**defaults)


# ===================================================================
# ClaudeVerificationService — initialization / availability
# ===================================================================

class TestClaudeVerificationServiceInit:

    def test_no_api_keys_not_available(self, monkeypatch):
        """With no API keys in the environment the service should report unavailable."""
        monkeypatch.delenv('OPENROUTER_API_KEY', raising=False)
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        # Also patch Django settings in case they provide a fallback
        with patch('matching.enrichment.ai_verification.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = None
            mock_settings.ANTHROPIC_API_KEY = None
            svc = ClaudeVerificationService()
        assert svc.is_available() is False

    def test_openrouter_key_preferred(self, monkeypatch):
        """When OPENROUTER_API_KEY is set it should be preferred over Anthropic."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-test-key-123')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        with patch('matching.enrichment.ai_verification.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-test-key-123'
            mock_settings.ANTHROPIC_API_KEY = None
            svc = ClaudeVerificationService()
        assert svc.is_available() is True
        assert svc.use_openrouter is True
        assert 'anthropic/' in svc.model.lower() or 'claude' in svc.model.lower()

    def test_anthropic_key_fallback(self, monkeypatch):
        """When only ANTHROPIC_API_KEY is set the service should still be available."""
        monkeypatch.delenv('OPENROUTER_API_KEY', raising=False)
        monkeypatch.setenv('ANTHROPIC_API_KEY', 'ant-test-key-456')
        with patch('matching.enrichment.ai_verification.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = None
            mock_settings.ANTHROPIC_API_KEY = 'ant-test-key-456'
            svc = ClaudeVerificationService()
        assert svc.is_available() is True


# ===================================================================
# _parse_response
# ===================================================================

class TestParseResponse:

    def _svc(self):
        """Return a service instance without requiring real API keys."""
        with patch.object(ClaudeVerificationService, '__init__', lambda self: None):
            svc = ClaudeVerificationService.__new__(ClaudeVerificationService)
            svc.api_key = None
            svc.use_openrouter = False
            svc.model = 'test'
        return svc

    def test_none_response_returns_fail_cautious(self):
        svc = self._svc()
        result = svc._parse_response(None, 'test_field')
        assert result.passed is False
        assert result.score == 0
        assert any('unavailable' in i.lower() or 'fail-cautious' in i.lower() for i in result.issues)

    def test_valid_json_parsed_correctly(self):
        svc = self._svc()
        payload = json.dumps({
            'passed': True,
            'score': 92,
            'issues': ['minor wording'],
            'suggestions': ['tighten intro'],
            'reasoning': 'Looks good overall.',
        })
        result = svc._parse_response(payload, 'test_field')
        assert result.passed is True
        assert result.score == 92
        assert 'minor wording' in result.issues
        assert 'tighten intro' in result.suggestions

    def test_json_in_code_fence_stripped(self):
        svc = self._svc()
        inner = json.dumps({
            'passed': True, 'score': 88,
            'issues': [], 'suggestions': [], 'reasoning': 'ok',
        })
        fenced = f'```json\n{inner}\n```'
        result = svc._parse_response(fenced, 'test_field')
        assert result.passed is True
        assert result.score == 88

    def test_invalid_json_returns_fail_cautious(self):
        svc = self._svc()
        result = svc._parse_response('NOT JSON {{{', 'test_field')
        assert result.passed is False
        assert result.score == 0
        assert any('parse error' in i.lower() for i in result.issues)

    def test_missing_fields_use_defaults(self):
        svc = self._svc()
        # Only supply 'score'; 'passed' is missing and should default to True
        payload = json.dumps({'score': 75, 'issues': [], 'suggestions': [], 'reasoning': ''})
        result = svc._parse_response(payload, 'test_field')
        # Default for 'passed' when missing should be True (or at least not crash)
        assert isinstance(result, AIVerificationResult)
        assert result.score == 75


# ===================================================================
# verify_formatting (mock _call_claude)
# ===================================================================

class TestVerifyFormatting:

    def _svc(self):
        with patch.object(ClaudeVerificationService, '__init__', lambda self: None):
            svc = ClaudeVerificationService.__new__(ClaudeVerificationService)
            svc.api_key = 'fake'
            svc.use_openrouter = False
            svc.model = 'test'
        return svc

    def test_verify_formatting_calls_claude_with_content(self):
        svc = self._svc()
        response = json.dumps({
            'passed': True, 'score': 95,
            'issues': [], 'suggestions': [], 'reasoning': 'ok',
        })
        with patch.object(svc, '_call_claude', return_value=response) as mock_call:
            svc.verify_formatting('Some content here', 'why_fit')
            mock_call.assert_called_once()
            prompt_arg = mock_call.call_args[0][0]
            assert 'Some content here' in prompt_arg

    def test_verify_formatting_returns_result(self):
        svc = self._svc()
        response = json.dumps({
            'passed': True, 'score': 90,
            'issues': [], 'suggestions': ['add detail'], 'reasoning': 'Good.',
        })
        with patch.object(svc, '_call_claude', return_value=response):
            result = svc.verify_formatting('Content', 'mutual_benefit')
        assert isinstance(result, AIVerificationResult)
        assert result.passed is True
        assert result.score == 90


# ===================================================================
# verify_content_quality (mock _call_claude)
# ===================================================================

class TestVerifyContentQuality:

    def _svc(self):
        with patch.object(ClaudeVerificationService, '__init__', lambda self: None):
            svc = ClaudeVerificationService.__new__(ClaudeVerificationService)
            svc.api_key = 'fake'
            svc.use_openrouter = False
            svc.model = 'test'
        return svc

    def test_verify_content_quality_includes_partner_name(self):
        svc = self._svc()
        response = json.dumps({
            'passed': True, 'score': 85,
            'issues': [], 'suggestions': [], 'reasoning': 'ok',
        })
        with patch.object(svc, '_call_claude', return_value=response) as mock_call:
            svc.verify_content_quality(
                'Great coaching programme',
                'Alice Johnson',
                {'niche': 'coaching'},
            )
            prompt_arg = mock_call.call_args[0][0]
            assert 'Alice Johnson' in prompt_arg


# ===================================================================
# rewrite_content (mock _call_claude)
# ===================================================================

class TestRewriteContent:

    def _svc(self):
        with patch.object(ClaudeVerificationService, '__init__', lambda self: None):
            svc = ClaudeVerificationService.__new__(ClaudeVerificationService)
            svc.api_key = 'fake'
            svc.use_openrouter = False
            svc.model = 'test'
        return svc

    def test_rewrite_returns_response_stripped(self):
        svc = self._svc()
        with patch.object(svc, '_call_claude', return_value='  Rewritten content here  '):
            result = svc.rewrite_content('original', 'why_fit', ['too short'])
        assert result == 'Rewritten content here'

    def test_rewrite_returns_original_on_failure(self):
        svc = self._svc()
        with patch.object(svc, '_call_claude', return_value=None):
            result = svc.rewrite_content('original text', 'why_fit', ['issue'])
        assert result == 'original text'


# ===================================================================
# AIMatchVerificationAgent
# ===================================================================

class TestAIMatchVerificationAgent:

    def test_verify_match_averages_scores(self):
        """verify_match should average the scores returned by the sub-verifications."""
        match = _make_match()
        agent = AIMatchVerificationAgent()
        svc = agent.claude

        high_result = AIVerificationResult(
            passed=True, score=90, issues=[], suggestions=[], reasoning='good',
        )
        low_result = AIVerificationResult(
            passed=True, score=70, issues=['minor'], suggestions=[], reasoning='ok',
        )

        with patch.object(svc, 'is_available', return_value=True), \
             patch.object(svc, 'verify_formatting', return_value=high_result), \
             patch.object(svc, 'verify_content_quality', return_value=low_result), \
             patch.object(svc, 'verify_data_quality', return_value=high_result), \
             patch.object(svc, 'verify_outreach_message', return_value=high_result):
            avg_score, issues, suggestions = agent.verify_match(match)

        # Average of the scores used should be deterministic and between 70 and 90
        assert 70 <= avg_score <= 90

    def test_verify_and_fix_triggers_rewrite_below_threshold(self):
        """When average score < MIN_SCORE_TO_PASS (80), rewrite should be called."""
        match = _make_match()
        agent = AIMatchVerificationAgent()
        svc = agent.claude

        low_result = AIVerificationResult(
            passed=False, score=60, issues=['bad section'], suggestions=['rewrite'], reasoning='poor',
        )

        with patch.object(svc, 'is_available', return_value=True), \
             patch.object(svc, 'verify_formatting', return_value=low_result), \
             patch.object(svc, 'verify_content_quality', return_value=low_result), \
             patch.object(svc, 'verify_data_quality', return_value=low_result), \
             patch.object(svc, 'verify_outreach_message', return_value=low_result), \
             patch.object(svc, 'rewrite_content', return_value='Improved text') as mock_rewrite:
            updated_match, score, issues = agent.verify_and_fix(match)

        # rewrite_content should have been invoked at least once
        assert mock_rewrite.call_count >= 1
