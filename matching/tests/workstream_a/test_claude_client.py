"""
Tests for ClaudeClient shared API client.

Covers:
- Initialization with explicit keys (OpenRouter preferred, Anthropic fallback, no keys)
- Auto-detection from Django settings
- call() via OpenRouter (mocked openai) and Anthropic (mocked anthropic)
- call() with no API key, import errors, API errors
- parse_json() with clean JSON, code fences, bare fences, None, empty, invalid
- is_available() check
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from matching.enrichment.claude_client import ClaudeClient


# ===================================================================
# Initialization — explicit keys
# ===================================================================

class TestClaudeClientInitExplicit:

    def test_openrouter_preferred(self):
        """When both keys are provided, OpenRouter should be preferred."""
        client = ClaudeClient(openrouter_key='or-key', anthropic_key='ant-key')
        assert client.use_openrouter is True
        assert client.api_key == 'or-key'
        assert 'anthropic/' in client.model

    def test_anthropic_fallback(self):
        """When only Anthropic key is provided, it should be used."""
        client = ClaudeClient(openrouter_key='', anthropic_key='ant-key')
        assert client.use_openrouter is False
        assert client.api_key == 'ant-key'
        assert client.model is not None
        assert 'anthropic/' not in client.model

    def test_no_keys(self):
        """With no keys, api_key should be None."""
        client = ClaudeClient(openrouter_key='', anthropic_key='')
        assert client.api_key is None
        assert client.model is None

    def test_custom_max_tokens(self):
        """max_tokens should be configurable."""
        client = ClaudeClient(max_tokens=1024, openrouter_key='', anthropic_key='')
        assert client.max_tokens == 1024


# ===================================================================
# Initialization — auto-detection from Django settings
# ===================================================================

class TestClaudeClientInitAutoDetect:

    def test_auto_detect_openrouter(self, monkeypatch):
        """Auto-detect should pick up OPENROUTER_API_KEY from Django settings."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-env-key')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        with patch('matching.enrichment.claude_client.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-env-key'
            mock_settings.ANTHROPIC_API_KEY = ''
            # Force re-import to trigger auto-detection
            client = ClaudeClient.__new__(ClaudeClient)
            client.__init__()  # No explicit keys → auto-detect
        assert client.api_key is not None

    def test_auto_detect_from_environ(self, monkeypatch):
        """Auto-detect should fall back to environment variables."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-env-key')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        # Don't set Django settings — let environ provide the key
        client = ClaudeClient()
        assert client.api_key is not None


# ===================================================================
# is_available
# ===================================================================

class TestIsAvailable:

    def test_available_with_key(self):
        client = ClaudeClient(openrouter_key='key')
        assert client.is_available() is True

    def test_unavailable_without_key(self):
        client = ClaudeClient(openrouter_key='', anthropic_key='')
        assert client.is_available() is False


# ===================================================================
# call() — mocked API calls
# ===================================================================

class TestClaudeClientCall:

    def test_no_key_returns_none(self):
        """call() should return None when no API key is configured."""
        client = ClaudeClient(openrouter_key='', anthropic_key='')
        assert client.call("test prompt") is None

    def test_openrouter_call(self):
        """call() via OpenRouter should use openai.OpenAI with base_url."""
        client = ClaudeClient(openrouter_key='or-key')

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "AI response text"

        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.return_value = mock_response

        with patch('openai.OpenAI', return_value=mock_openai_client) as mock_cls:
            result = client.call("test prompt")

        mock_cls.assert_called_once_with(
            base_url="https://openrouter.ai/api/v1",
            api_key='or-key',
        )
        assert result == "AI response text"

    def test_anthropic_call(self):
        """call() via Anthropic should use anthropic.Anthropic."""
        client = ClaudeClient(openrouter_key='', anthropic_key='ant-key')

        mock_message = MagicMock()
        mock_message.content = [MagicMock()]
        mock_message.content[0].text = "Anthropic response"

        mock_anthropic_client = MagicMock()
        mock_anthropic_client.messages.create.return_value = mock_message

        with patch('anthropic.Anthropic', return_value=mock_anthropic_client) as mock_cls:
            result = client.call("test prompt")

        mock_cls.assert_called_once_with(api_key='ant-key')
        assert result == "Anthropic response"

    def test_import_error_returns_none(self):
        """call() should return None when required package is not installed."""
        client = ClaudeClient(openrouter_key='or-key')

        with patch.dict('sys.modules', {'openai': None}):
            with patch('builtins.__import__', side_effect=ImportError("No openai")):
                result = client.call("test prompt")

        assert result is None

    def test_api_error_returns_none(self):
        """call() should return None on API errors."""
        client = ClaudeClient(openrouter_key='or-key')

        with patch('openai.OpenAI', side_effect=Exception("API error")):
            result = client.call("test prompt")

        assert result is None


# ===================================================================
# parse_json() — static method
# ===================================================================

class TestParseJson:

    def test_clean_json(self):
        result = ClaudeClient.parse_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_json_in_code_fence(self):
        raw = '```json\n{"key": "value"}\n```'
        result = ClaudeClient.parse_json(raw)
        assert result == {"key": "value"}

    def test_json_in_bare_fence(self):
        raw = '```\n{"key": "value"}\n```'
        result = ClaudeClient.parse_json(raw)
        assert result == {"key": "value"}

    def test_none_returns_none(self):
        assert ClaudeClient.parse_json(None) is None

    def test_empty_string_returns_none(self):
        assert ClaudeClient.parse_json("") is None

    def test_invalid_json_returns_none(self):
        assert ClaudeClient.parse_json("not json at all") is None

    def test_nested_json(self):
        data = {"fields": {"name": "Alice", "tags": ["a", "b"]}}
        raw = f'```json\n{json.dumps(data)}\n```'
        result = ClaudeClient.parse_json(raw)
        assert result == data


# ===================================================================
# Contract tests — services expose the same public interface
# ===================================================================

class TestServiceContracts:

    def test_profile_research_service_has_expected_attrs(self):
        """ProfileResearchService should expose api_key, use_openrouter, model."""
        with patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = ''
            mock_settings.ANTHROPIC_API_KEY = ''
            from matching.enrichment.ai_research import ProfileResearchService
            svc = ProfileResearchService()
        assert hasattr(svc, 'api_key')
        assert hasattr(svc, 'use_openrouter')
        assert hasattr(svc, 'model')
        assert hasattr(svc, 'max_tokens')
        assert hasattr(svc, '_call_claude')
        assert hasattr(svc, '_parse_json_response')

    def test_claude_verification_service_has_expected_attrs(self):
        """ClaudeVerificationService should expose api_key, use_openrouter, model."""
        with patch('matching.enrichment.ai_verification.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = ''
            mock_settings.ANTHROPIC_API_KEY = ''
            from matching.enrichment.ai_verification import ClaudeVerificationService
            svc = ClaudeVerificationService()
        assert hasattr(svc, 'api_key')
        assert hasattr(svc, 'use_openrouter')
        assert hasattr(svc, 'model')
        assert hasattr(svc, '_call_claude')
        assert hasattr(svc, 'is_available')
