"""
Tests for DeepResearchService, SimpleDeepResearch, and deep_research_profile.

Covers:
- DeepResearchService: missing API key, import errors, JSON extraction from Claude,
  low-confidence filtering
- SimpleDeepResearch: missing API key, Serper web search, DuckDuckGo fallback,
  all-search-fail, field extraction with source quotes, source-quote filtering,
  email extraction from contact_info, empty search results
- deep_research_profile entry point: default mode selection, result merging,
  no-results passthrough
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from matching.enrichment.deep_research import (
    DeepResearchService,
    SimpleDeepResearch,
    deep_research_profile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bare_deep_service(**attrs):
    """Return a DeepResearchService with __init__ bypassed and attrs set."""
    with patch.object(DeepResearchService, '__init__', lambda self: None):
        svc = DeepResearchService.__new__(DeepResearchService)
        svc.openai_key = ''
        svc.tavily_key = ''
        for k, v in attrs.items():
            setattr(svc, k, v)
    return svc


def _bare_simple_service(**attrs):
    """Return a SimpleDeepResearch with __init__ bypassed and attrs set."""
    with patch.object(SimpleDeepResearch, '__init__', lambda self: None):
        svc = SimpleDeepResearch.__new__(SimpleDeepResearch)
        svc.openrouter_key = ''
        svc.serper_key = ''
        for k, v in attrs.items():
            setattr(svc, k, v)
    return svc


def _claude_json_response(data: dict, fence: bool = False) -> str:
    """Return a JSON string optionally wrapped in ```json fences."""
    raw = json.dumps(data)
    if fence:
        return f"```json\n{raw}\n```"
    return raw


# ===================================================================
# DeepResearchService
# ===================================================================

class TestDeepResearchServiceNoKey:

    def test_no_openai_key_returns_empty(self, monkeypatch):
        """research_profile_async should return {} when no OPENAI_API_KEY is set."""
        monkeypatch.delenv('OPENAI_API_KEY', raising=False)
        with patch('matching.enrichment.deep_research.settings', create=True) as mock_settings:
            mock_settings.OPENAI_API_KEY = ''
            mock_settings.TAVILY_API_KEY = ''
            svc = DeepResearchService()

        # Mock the gpt_researcher import so we can isolate the key check
        mock_gpt = MagicMock()
        with patch.dict('sys.modules', {'gpt_researcher': mock_gpt}):
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    svc.research_profile_async('Alice', 'Acme', {})
                )
            finally:
                loop.close()
        assert result == {}


class TestDeepResearchServiceImportError:

    def test_gpt_researcher_import_error(self):
        """research_profile_async should return {} when gpt_researcher cannot be imported."""
        import asyncio
        import builtins

        svc = _bare_deep_service(openai_key='fake-key')

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == 'gpt_researcher':
                raise ImportError("No module named 'gpt_researcher'")
            return original_import(name, *args, **kwargs)

        with patch('builtins.__import__', side_effect=mock_import):
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    svc.research_profile_async('Alice', 'Acme', {})
                )
            finally:
                loop.close()
        assert result == {}


class TestExtractProfileData:

    def test_extract_profile_data_parses_json(self):
        """Valid high-confidence JSON from Claude should yield extracted fields."""
        svc = _bare_deep_service()
        claude_response = _claude_json_response({
            'seeking': 'JV partners for product launch',
            'who_you_serve': 'Small business owners',
            'what_you_do': 'Business coaching',
            'offering': 'Email list of 50k subscribers',
            'social_proof': 'NYT bestselling author',
            'confidence': 'high',
            'sources': ['interview on Forbes'],
        }, fence=True)

        mock_service = MagicMock()
        mock_service._call_claude.return_value = claude_response

        with patch('matching.enrichment.ai_research.ProfileResearchService', return_value=mock_service):
            result = svc._extract_profile_data('Alice', 'Some long report text', {})

        assert result['seeking'] == 'JV partners for product launch'
        assert result['who_you_serve'] == 'Small business owners'
        assert result['what_you_do'] == 'Business coaching'
        assert result['offering'] == 'Email list of 50k subscribers'
        assert result['bio'] == 'NYT bestselling author'

    def test_extract_low_confidence_skipped(self):
        """Low-confidence response should yield empty dict."""
        svc = _bare_deep_service()
        claude_response = _claude_json_response({
            'seeking': 'Something vague',
            'who_you_serve': 'People',
            'what_you_do': 'Stuff',
            'offering': 'Things',
            'social_proof': 'Unknown',
            'confidence': 'low',
            'sources': [],
        })

        mock_service = MagicMock()
        mock_service._call_claude.return_value = claude_response

        with patch('matching.enrichment.ai_research.ProfileResearchService', return_value=mock_service):
            result = svc._extract_profile_data('Bob', 'Report text', {})

        assert result == {}


# ===================================================================
# SimpleDeepResearch
# ===================================================================

class TestSimpleDeepResearchNoKey:

    def test_no_openrouter_key_returns_empty(self, monkeypatch):
        """research_profile should return {} when no OPENROUTER_API_KEY is available."""
        monkeypatch.delenv('OPENROUTER_API_KEY', raising=False)
        with patch('matching.enrichment.deep_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = ''
            mock_settings.SERPER_API_KEY = ''
            svc = SimpleDeepResearch()

        result = svc.research_profile('Alice', 'Acme', {})
        assert result == {}


class TestWebSearch:

    def test_web_search_serper(self):
        """When serper_key is set, _web_search should call Serper API and return results."""
        svc = _bare_simple_service(serper_key='serper-key-123')

        mock_response = MagicMock()
        mock_response.json.return_value = {
            'organic': [
                {
                    'title': 'Alice at Acme',
                    'snippet': 'Alice is the CEO of Acme Corp.',
                    'link': 'https://example.com/alice',
                },
                {
                    'title': 'Acme Partners',
                    'snippet': 'Acme seeks JV partners.',
                    'link': 'https://example.com/acme',
                },
            ]
        }

        # requests is imported locally inside _web_search, patch it at the
        # requests module level so the local import picks up our mock.
        with patch('requests.post', return_value=mock_response) as mock_post:
            results = svc._web_search('Alice Acme JV')

        mock_post.assert_called_once()
        assert len(results) == 2
        assert results[0]['title'] == 'Alice at Acme'
        assert results[0]['url'] == 'https://example.com/alice'
        assert 'CEO of Acme' in results[0]['text']
        assert results[0]['snippet'] == 'Alice is the CEO of Acme Corp.'

    def test_web_search_no_keys_duckduckgo(self):
        """Without serper_key, _web_search should fall back to DuckDuckGo."""
        svc = _bare_simple_service(serper_key='')

        mock_ddgs_instance = MagicMock()
        mock_ddgs_instance.__enter__ = MagicMock(return_value=mock_ddgs_instance)
        mock_ddgs_instance.__exit__ = MagicMock(return_value=False)
        mock_ddgs_instance.text.return_value = [
            {
                'title': 'DDG Result',
                'body': 'Alice runs a coaching business.',
                'href': 'https://ddg.example.com/alice',
            }
        ]

        # DDGS is imported locally via "from duckduckgo_search import DDGS".
        # Patch the DDGS class in the duckduckgo_search module so the local
        # import resolves to our mock.
        mock_ddgs_module = MagicMock()
        mock_ddgs_module.DDGS = MagicMock(return_value=mock_ddgs_instance)

        with patch.dict('sys.modules', {'duckduckgo_search': mock_ddgs_module}):
            results = svc._web_search('Alice coaching')

        assert len(results) == 1
        assert results[0]['title'] == 'DDG Result'
        assert results[0]['url'] == 'https://ddg.example.com/alice'
        assert 'coaching business' in results[0]['snippet']

    def test_web_search_all_fail(self):
        """When both Serper and DuckDuckGo fail, _web_search should return []."""
        svc = _bare_simple_service(serper_key='')

        # DuckDuckGo import fails
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == 'duckduckgo_search':
                raise ImportError("No module named 'duckduckgo_search'")
            return original_import(name, *args, **kwargs)

        with patch('builtins.__import__', side_effect=mock_import):
            results = svc._web_search('Alice anything')

        assert results == []


class TestExtractAndVerify:

    def _make_extraction_response(self, **overrides):
        """Build a typical Claude extraction response dict."""
        data = {
            'seeking': {'value': 'JV partners for course launch', 'source_quote': 'Looking for JV partners'},
            'who_you_serve': {'value': 'Online entrepreneurs', 'source_quote': 'We serve online entrepreneurs'},
            'what_you_do': {'value': 'Digital marketing training', 'source_quote': 'We provide digital marketing training'},
            'offering': {'value': 'Access to 100k email list', 'source_quote': 'Our list of 100k subscribers'},
            'social_proof': {'value': 'Inc 500 company', 'source_quote': 'Named to Inc 500 list'},
            'contact_info': {'value': '', 'source_quote': ''},
            'confidence': 'high',
            'verification_notes': 'Multiple sources confirm data.',
        }
        data.update(overrides)
        return data

    def test_extract_and_verify_parses_fields(self):
        """Valid JSON with source_quotes should populate all fields."""
        svc = _bare_simple_service()
        response_data = self._make_extraction_response()
        claude_response = _claude_json_response(response_data, fence=True)

        search_results = [
            {'text': 'Alice runs a digital marketing company', 'title': 'About Alice', 'url': 'https://example.com', 'snippet': 'Digital marketing'},
        ]

        mock_service = MagicMock()
        mock_service._call_claude.return_value = claude_response

        with patch('matching.enrichment.ai_research.ProfileResearchService', return_value=mock_service):
            result = svc._extract_and_verify('Alice', 'Acme', search_results, ['https://example.com'], {})

        assert result['seeking'] == 'JV partners for course launch'
        assert result['who_you_serve'] == 'Online entrepreneurs'
        assert result['what_you_do'] == 'Digital marketing training'
        assert result['offering'] == 'Access to 100k email list'
        assert result['bio'] == 'Inc 500 company'
        assert '_research_sources' in result

    def test_extract_and_verify_no_source_quote_excluded(self):
        """Fields with empty source_quote should be excluded from results."""
        svc = _bare_simple_service()
        response_data = self._make_extraction_response(
            seeking={'value': 'JV partners', 'source_quote': ''},  # No source quote
            who_you_serve={'value': 'Entrepreneurs', 'source_quote': 'We serve entrepreneurs'},
        )
        claude_response = _claude_json_response(response_data)

        search_results = [
            {'text': 'Some search text', 'title': 'Title', 'url': 'https://example.com', 'snippet': 'Snippet'},
        ]

        mock_service = MagicMock()
        mock_service._call_claude.return_value = claude_response

        with patch('matching.enrichment.ai_research.ProfileResearchService', return_value=mock_service):
            result = svc._extract_and_verify('Alice', 'Acme', search_results, ['https://example.com'], {})

        # seeking should be excluded (no source_quote)
        assert 'seeking' not in result
        # who_you_serve should be included (has source_quote)
        assert result['who_you_serve'] == 'Entrepreneurs'

    def test_extract_and_verify_extracts_email(self):
        """contact_info containing an email address should populate the email field."""
        svc = _bare_simple_service()
        response_data = self._make_extraction_response(
            contact_info={'value': 'Contact Alice at alice@acme.com for partnerships', 'source_quote': 'email alice@acme.com'},
        )
        claude_response = _claude_json_response(response_data)

        search_results = [
            {'text': 'Alice contact info', 'title': 'Contact', 'url': 'https://example.com', 'snippet': 'Contact'},
        ]

        mock_service = MagicMock()
        mock_service._call_claude.return_value = claude_response

        with patch('matching.enrichment.ai_research.ProfileResearchService', return_value=mock_service):
            result = svc._extract_and_verify('Alice', 'Acme', search_results, ['https://example.com'], {})

        assert result['email'] == 'alice@acme.com'

    def test_no_search_results_returns_empty(self):
        """When _web_search returns [] for all queries, research_profile should return {}."""
        svc = _bare_simple_service(openrouter_key='or-key-123')

        with patch.object(svc, '_web_search', return_value=[]):
            result = svc.research_profile('Alice', 'Acme', {})

        assert result == {}


# ===================================================================
# deep_research_profile (entry point)
# ===================================================================

class TestDeepResearchProfile:

    def test_simple_mode_default(self):
        """use_gpt_researcher=False should use SimpleDeepResearch, not DeepResearchService."""
        mock_result = {'seeking': 'JV partners', 'who_you_serve': 'Entrepreneurs'}

        with patch.object(SimpleDeepResearch, 'research_profile', return_value=mock_result) as mock_research, \
             patch('matching.enrichment.deep_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-key'
            mock_settings.SERPER_API_KEY = ''

            merged, was_researched = deep_research_profile(
                'Alice', 'Acme', {'what_you_do': 'Coaching'}, use_gpt_researcher=False
            )

        mock_research.assert_called_once_with('Alice', 'Acme', {'what_you_do': 'Coaching'})
        assert was_researched is True
        assert merged['seeking'] == 'JV partners'

    def test_result_merged_with_existing(self):
        """Researched fields should be merged on top of existing data."""
        existing = {'what_you_do': 'Existing coaching biz', 'website': 'https://alice.com'}
        researched = {'seeking': 'Affiliate partners', 'who_you_serve': 'Small biz owners'}

        with patch.object(SimpleDeepResearch, 'research_profile', return_value=researched), \
             patch('matching.enrichment.deep_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-key'
            mock_settings.SERPER_API_KEY = ''

            merged, was_researched = deep_research_profile(
                'Alice', 'Acme', existing, use_gpt_researcher=False
            )

        assert was_researched is True
        # Existing fields preserved
        assert merged['what_you_do'] == 'Existing coaching biz'
        assert merged['website'] == 'https://alice.com'
        # New fields merged in
        assert merged['seeking'] == 'Affiliate partners'
        assert merged['who_you_serve'] == 'Small biz owners'

    def test_no_results_returns_existing(self):
        """When research returns {}, deep_research_profile should return (existing, False)."""
        existing = {'what_you_do': 'Known business', 'website': 'https://known.com'}

        with patch.object(SimpleDeepResearch, 'research_profile', return_value={}), \
             patch('matching.enrichment.deep_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-key'
            mock_settings.SERPER_API_KEY = ''

            result, was_researched = deep_research_profile(
                'Bob', 'BobCo', existing, use_gpt_researcher=False
            )

        assert was_researched is False
        assert result == existing
