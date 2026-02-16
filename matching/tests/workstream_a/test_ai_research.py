"""
Tests for ProfileResearchService, ProfileResearchCache, and research_and_enrich_profile.

Covers:
- API key initialization (OpenRouter preferred, Anthropic fallback, no keys)
- URL normalization (_normalize_url)
- AI response parsing (_parse_research_response) with valid JSON, code fences,
  low confidence, invalid JSON, None response, existing-data protection, and metadata
- research_profile behavior (no website, no API key, content caching)
- ProfileResearchCache set/get/miss/key determinism
- research_and_enrich_profile entry point (cache hit, sparse trigger, force_research)
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import json
import pytest
from unittest.mock import patch, MagicMock

from matching.enrichment.ai_research import (
    ProfileResearchService,
    ProfileResearchCache,
    research_and_enrich_profile,
)


# ---------------------------------------------------------------------------
# Helper: build a ProfileResearchService without real API keys
# ---------------------------------------------------------------------------

def _bare_service(**attrs):
    """Return a ProfileResearchService with __init__ bypassed and attrs set."""
    from matching.enrichment.ai_research import SmartCrawler
    with patch.object(ProfileResearchService, '__init__', lambda self: None):
        svc = ProfileResearchService.__new__(ProfileResearchService)
        svc.api_key = None
        svc.use_openrouter = False
        svc.model = None
        svc.max_tokens = 2048
        svc._content_cache = {}
        svc._crawler = SmartCrawler()
        for k, v in attrs.items():
            setattr(svc, k, v)
    return svc


# ===================================================================
# ProfileResearchService.__init__ — API key selection
# ===================================================================

class TestProfileResearchServiceInit:

    def test_openrouter_preferred(self, monkeypatch):
        """When OPENROUTER_API_KEY is set it should be preferred; model contains 'anthropic/'."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-key-111')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        with patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-key-111'
            mock_settings.ANTHROPIC_API_KEY = ''
            svc = ProfileResearchService()
        assert svc.use_openrouter is True
        assert svc.api_key == 'or-key-111'
        assert 'anthropic/' in svc.model

    def test_anthropic_fallback(self, monkeypatch):
        """When only ANTHROPIC_API_KEY is set, use_openrouter should be False."""
        monkeypatch.delenv('OPENROUTER_API_KEY', raising=False)
        monkeypatch.setenv('ANTHROPIC_API_KEY', 'ant-key-222')
        with patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = ''
            mock_settings.ANTHROPIC_API_KEY = 'ant-key-222'
            svc = ProfileResearchService()
        assert svc.use_openrouter is False
        assert svc.api_key == 'ant-key-222'
        assert svc.model is not None

    def test_no_keys_unavailable(self, monkeypatch):
        """With no API keys configured, api_key should be None."""
        monkeypatch.delenv('OPENROUTER_API_KEY', raising=False)
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
        with patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = ''
            mock_settings.ANTHROPIC_API_KEY = ''
            svc = ProfileResearchService()
        assert svc.api_key is None
        assert svc.model is None


# ===================================================================
# _normalize_url
# ===================================================================

class TestNormalizeUrl:

    def test_adds_https_to_bare_url(self):
        svc = _bare_service()
        assert svc._normalize_url('acme.com') == 'https://acme.com'

    def test_keeps_existing_https(self):
        svc = _bare_service()
        assert svc._normalize_url('https://acme.com') == 'https://acme.com'

    def test_strips_whitespace(self):
        svc = _bare_service()
        assert svc._normalize_url('  acme.com  ') == 'https://acme.com'


# ===================================================================
# _parse_research_response
# ===================================================================

class TestParseResearchResponse:

    def _make_json(self, **overrides):
        data = {
            'what_you_do': 'Business coaching and consulting services',
            'who_you_serve': 'Entrepreneurs and small-business owners',
            'seeking': 'JV partners for launches',
            'offering': 'Email list of 20k subscribers',
            'social_proof': 'NYT bestselling author',
            'confidence': 'high',
            'source_quotes': ['We help entrepreneurs grow.'],
        }
        data.update(overrides)
        return data

    def test_valid_json_parsed(self):
        svc = _bare_service()
        raw = json.dumps(self._make_json())
        result = svc._parse_research_response(raw, 'Alice', {})
        assert result['what_you_do'] == 'Business coaching and consulting services'
        assert result['who_you_serve'] == 'Entrepreneurs and small-business owners'
        assert result['seeking'] == 'JV partners for launches'
        assert result['offering'] == 'Email list of 20k subscribers'

    def test_json_in_code_fence(self):
        svc = _bare_service()
        inner = json.dumps(self._make_json())
        fenced = f'```json\n{inner}\n```'
        result = svc._parse_research_response(fenced, 'Alice', {})
        assert result['what_you_do'] == 'Business coaching and consulting services'

    def test_low_confidence_skipped(self):
        svc = _bare_service()
        raw = json.dumps(self._make_json(confidence='low'))
        result = svc._parse_research_response(raw, 'Alice', {})
        # With low confidence, profile fields should NOT be populated
        assert 'what_you_do' not in result
        assert 'who_you_serve' not in result
        # But extraction metadata must still be present
        assert '_extraction_metadata' in result
        assert result['_extraction_metadata']['confidence'] == 'low'

    def test_invalid_json_returns_empty(self):
        svc = _bare_service()
        result = svc._parse_research_response('NOT JSON {{{', 'Alice', {})
        assert result == {}

    def test_none_response_returns_empty(self):
        svc = _bare_service()
        result = svc._parse_research_response(None, 'Alice', {})
        assert result == {}

    def test_existing_data_not_overwritten(self):
        """When an existing field has length >= 10, it should not be replaced."""
        svc = _bare_service()
        existing = {'what_you_do': 'Already has a perfectly fine description here'}
        raw = json.dumps(self._make_json())
        result = svc._parse_research_response(raw, 'Alice', existing)
        # what_you_do should NOT appear in result because existing value is long enough
        assert 'what_you_do' not in result

    def test_metadata_always_attached(self):
        """_extraction_metadata should be present for any successfully parsed response."""
        svc = _bare_service()
        raw = json.dumps(self._make_json())
        result = svc._parse_research_response(raw, 'Alice', {})
        assert '_extraction_metadata' in result
        meta = result['_extraction_metadata']
        assert meta['source'] == 'website_research'
        assert meta['confidence'] == 'high'
        assert 'extracted_at' in meta
        assert isinstance(meta['fields_updated'], list)
        assert len(meta['fields_updated']) > 0


# ===================================================================
# research_profile
# ===================================================================

class TestResearchProfile:

    def test_no_website_returns_empty(self):
        svc = _bare_service(api_key='fake-key')
        result = svc.research_profile('Alice', '', {})
        assert result == {}

    def test_no_api_key_returns_empty(self):
        svc = _bare_service(api_key=None)
        result = svc.research_profile('Alice', 'https://acme.com', {})
        assert result == {}

    def test_caches_raw_content(self):
        """After a successful research call, get_cached_content should return content."""
        svc = _bare_service(api_key='fake-key')
        website_text = '<html>We help entrepreneurs succeed.</html>'
        ai_response = json.dumps({
            'what_you_do': 'Helping entrepreneurs',
            'who_you_serve': 'Entrepreneurs',
            'seeking': '', 'offering': '', 'social_proof': '',
            'confidence': 'high', 'source_quotes': [],
        })

        with patch.object(svc._crawler, 'crawl_site', return_value=(website_text, [])), \
             patch.object(svc, '_call_claude', return_value=ai_response):
            svc.research_profile('Alice Smith', 'https://acme.com', {})

        cached = svc.get_cached_content('alice smith')
        assert cached == website_text

    def test_fetch_failure_returns_empty(self):
        """When crawl_site returns empty content, research_profile should return {}."""
        svc = _bare_service(api_key='fake-key')
        with patch.object(svc._crawler, 'crawl_site', return_value=('', [])):
            result = svc.research_profile('Alice', 'https://acme.com', {})
        assert result == {}


# ===================================================================
# ProfileResearchCache
# ===================================================================

class TestProfileResearchCache:

    def test_set_and_get(self, tmp_path):
        cache = ProfileResearchCache(cache_dir=str(tmp_path))
        data = {'what_you_do': 'coaching', 'who_you_serve': 'entrepreneurs'}
        cache.set('Alice Smith', data)
        retrieved = cache.get('Alice Smith')
        assert retrieved == data

    def test_get_missing_returns_none(self, tmp_path):
        cache = ProfileResearchCache(cache_dir=str(tmp_path))
        assert cache.get('Nonexistent Person') is None

    def test_cache_key_deterministic(self, tmp_path):
        cache = ProfileResearchCache(cache_dir=str(tmp_path))
        key1 = cache._cache_key('Alice Smith')
        key2 = cache._cache_key('Alice Smith')
        assert key1 == key2

    def test_cache_key_case_insensitive(self, tmp_path):
        cache = ProfileResearchCache(cache_dir=str(tmp_path))
        assert cache._cache_key('Alice Smith') == cache._cache_key('alice smith')


# ===================================================================
# research_and_enrich_profile (entry point)
# ===================================================================

class TestResearchAndEnrichProfile:

    def test_uses_cache_when_available(self, tmp_path, monkeypatch):
        """Cached data should be returned without triggering fresh research."""
        cached_data = {'what_you_do': 'cached coaching', 'who_you_serve': 'cached audience'}

        with patch('matching.enrichment.ai_research.ProfileResearchCache') as MockCache:
            instance = MockCache.return_value
            instance.get.return_value = cached_data

            result, was_researched = research_and_enrich_profile(
                name='Alice',
                website='https://acme.com',
                existing_data={},
                use_cache=True,
                force_research=False,
            )

        assert result == cached_data
        assert was_researched is False

    def test_sparse_profile_triggers_research(self, monkeypatch):
        """A profile missing seeking and who_you_serve should trigger research."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-fake')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)

        ai_response = json.dumps({
            'what_you_do': 'Life coaching',
            'who_you_serve': 'Stressed professionals',
            'seeking': 'Affiliate partners',
            'offering': 'Coaching sessions',
            'social_proof': '',
            'confidence': 'high',
            'source_quotes': ['We serve stressed professionals.'],
        })

        with patch('matching.enrichment.ai_research.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-fake'
            mock_settings.ANTHROPIC_API_KEY = ''
            instance = MockCache.return_value
            instance.get.return_value = None  # No cache hit

            with patch('matching.enrichment.ai_research.SmartCrawler.crawl_site', return_value=('Website text here', [])), \
                 patch.object(ProfileResearchService, '_call_claude', return_value=ai_response):
                existing = {'what_you_do': 'short'}  # sparse: no seeking, no who_you_serve
                result, was_researched = research_and_enrich_profile(
                    name='Bob',
                    website='https://bobco.com',
                    existing_data=existing,
                    use_cache=True,
                    force_research=False,
                )

        assert was_researched is True
        assert result.get('who_you_serve') == 'Stressed professionals'

    def test_force_research_bypasses_cache(self, monkeypatch):
        """force_research=True should research even when cache has data."""
        monkeypatch.setenv('OPENROUTER_API_KEY', 'or-fake')
        monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)

        ai_response = json.dumps({
            'what_you_do': 'Fresh research result',
            'who_you_serve': 'New audience',
            'seeking': '', 'offering': '', 'social_proof': '',
            'confidence': 'high',
            'source_quotes': [],
        })

        with patch('matching.enrichment.ai_research.ProfileResearchCache') as MockCache, \
             patch('matching.enrichment.ai_research.settings', create=True) as mock_settings:
            mock_settings.OPENROUTER_API_KEY = 'or-fake'
            mock_settings.ANTHROPIC_API_KEY = ''
            instance = MockCache.return_value
            # Cache has data, but force_research should bypass it
            instance.get.return_value = {'what_you_do': 'stale data'}

            with patch('matching.enrichment.ai_research.SmartCrawler.crawl_site', return_value=('Website text', [])), \
                 patch.object(ProfileResearchService, '_call_claude', return_value=ai_response):
                result, was_researched = research_and_enrich_profile(
                    name='Carol',
                    website='https://carol.com',
                    existing_data={},
                    use_cache=True,
                    force_research=True,
                )

        assert was_researched is True
        # The cache.get should never have been consulted
        instance.get.assert_not_called()

    def test_no_website_returns_existing_data(self):
        """Without a website the function should return existing_data unchanged."""
        existing = {'what_you_do': 'Already known'}

        with patch('matching.enrichment.ai_research.ProfileResearchCache') as MockCache:
            instance = MockCache.return_value
            instance.get.return_value = None

            # Mock Exa to avoid real API calls
            with patch('matching.enrichment.exa_research.ExaResearchService') as MockExa:
                mock_svc = MockExa.return_value
                mock_svc.available = True
                mock_svc.research_profile.return_value = {'_exa_indexed': False}

                result, was_researched = research_and_enrich_profile(
                    name='Dave',
                    website='',
                    existing_data=existing,
                    use_cache=False,
                    force_research=True,
                )

        assert was_researched is False
        assert result == existing
