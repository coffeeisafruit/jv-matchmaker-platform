"""
Unit tests for scripts/_common.py shared utilities.

Tests all extracted functions to verify they behave identically
to the original inline implementations they replaced.
"""

import json
import hashlib
import os
import pytest
from unittest.mock import patch, MagicMock


# ── cache_key ─────────────────────────────────────────────────────

class TestCacheKey:
    def test_deterministic(self):
        """Same name always produces same key."""
        from scripts._common import cache_key
        assert cache_key("Jane Smith") == cache_key("Jane Smith")

    def test_case_insensitive(self):
        from scripts._common import cache_key
        assert cache_key("Jane Smith") == cache_key("jane smith")

    def test_length_12(self):
        from scripts._common import cache_key
        assert len(cache_key("Test Name")) == 12

    def test_matches_original_implementation(self):
        """Must produce identical output to the original inline version."""
        from scripts._common import cache_key
        name = "Alice Johnson"
        expected = hashlib.md5(name.lower().encode()).hexdigest()[:12]
        assert cache_key(name) == expected


# ── extract_json_from_claude ──────────────────────────────────────

class TestExtractJsonFromClaude:
    def test_clean_json(self):
        from scripts._common import extract_json_from_claude
        raw = '{"what_you_do": "coaching"}'
        assert extract_json_from_claude(raw) == {"what_you_do": "coaching"}

    def test_json_in_markdown_fence(self):
        from scripts._common import extract_json_from_claude
        raw = '```json\n{"what_you_do": "coaching"}\n```'
        assert extract_json_from_claude(raw) == {"what_you_do": "coaching"}

    def test_json_in_bare_fence(self):
        from scripts._common import extract_json_from_claude
        raw = '```\n{"what_you_do": "coaching"}\n```'
        assert extract_json_from_claude(raw) == {"what_you_do": "coaching"}

    def test_json_with_surrounding_text(self):
        from scripts._common import extract_json_from_claude
        raw = 'Here is the result:\n{"what_you_do": "coaching"}\nDone.'
        result = extract_json_from_claude(raw)
        assert result["what_you_do"] == "coaching"

    def test_no_json_returns_none(self):
        from scripts._common import extract_json_from_claude
        assert extract_json_from_claude("No JSON here at all") is None

    def test_none_input_returns_none(self):
        from scripts._common import extract_json_from_claude
        assert extract_json_from_claude(None) is None

    def test_empty_string_returns_none(self):
        from scripts._common import extract_json_from_claude
        assert extract_json_from_claude("") is None


# ── call_claude_cli ───────────────────────────────────────────────

class TestCallClaudeCli:
    @patch('scripts._common.subprocess.run')
    def test_successful_call_returns_parsed_json(self, mock_run):
        from scripts._common import call_claude_cli
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"what_you_do": "coaching"}',
            stderr=''
        )
        result = call_claude_cli("test prompt")
        assert result == {"what_you_do": "coaching"}

    @patch('scripts._common.subprocess.run')
    def test_nonzero_returncode_returns_none(self, mock_run):
        from scripts._common import call_claude_cli
        mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='error')
        assert call_claude_cli("test prompt") is None

    @patch('scripts._common.subprocess.run')
    def test_timeout_returns_none(self, mock_run):
        import subprocess
        from scripts._common import call_claude_cli
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='claude', timeout=90)
        assert call_claude_cli("test prompt") is None

    @patch('scripts._common.subprocess.run')
    def test_strips_markdown_fences(self, mock_run):
        from scripts._common import call_claude_cli
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='```json\n{"niche": "health"}\n```',
            stderr=''
        )
        result = call_claude_cli("test prompt")
        assert result == {"niche": "health"}

    @patch('scripts._common.subprocess.run')
    def test_respects_model_parameter(self, mock_run):
        from scripts._common import call_claude_cli
        mock_run.return_value = MagicMock(returncode=0, stdout='{}', stderr='')
        call_claude_cli("prompt", model='opus')
        args = mock_run.call_args[0][0]
        assert '--model' in args
        assert 'opus' in args

    @patch('scripts._common.subprocess.run')
    def test_respects_timeout_parameter(self, mock_run):
        from scripts._common import call_claude_cli
        mock_run.return_value = MagicMock(returncode=0, stdout='{}', stderr='')
        call_claude_cli("prompt", timeout=180)
        assert mock_run.call_args[1]['timeout'] == 180


# ── save_to_research_cache ────────────────────────────────────────

class TestSaveToResearchCache:
    def test_creates_new_cache_file(self, tmp_path):
        from scripts._common import save_to_research_cache
        result = save_to_research_cache(
            "Jane Smith", {"what_you_do": "coaching"}, "_test_enriched",
            cache_dir=str(tmp_path)
        )
        assert result is True
        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        with open(files[0]) as f:
            data = json.load(f)
        assert data["what_you_do"] == "coaching"
        assert data["_test_enriched"] is True
        assert data["name"] == "Jane Smith"
        assert "_cache_schema_version" in data

    def test_merges_with_existing_cache(self, tmp_path):
        from scripts._common import cache_key, save_to_research_cache
        # Create existing cache entry
        key = cache_key("Jane Smith")
        existing = {"name": "Jane Smith", "email": "jane@test.com"}
        cache_file = tmp_path / f"{key}.json"
        with open(cache_file, 'w') as f:
            json.dump(existing, f)
        # Save new data — should merge, not overwrite
        save_to_research_cache(
            "Jane Smith", {"what_you_do": "coaching"}, "_test_enriched",
            cache_dir=str(tmp_path)
        )
        with open(cache_file) as f:
            data = json.load(f)
        assert data["email"] == "jane@test.com"  # Preserved
        assert data["what_you_do"] == "coaching"  # Added

    def test_no_overwrite_by_default(self, tmp_path):
        from scripts._common import cache_key, save_to_research_cache
        key = cache_key("Jane Smith")
        existing = {"name": "Jane Smith", "what_you_do": "existing value"}
        cache_file = tmp_path / f"{key}.json"
        with open(cache_file, 'w') as f:
            json.dump(existing, f)
        save_to_research_cache(
            "Jane Smith", {"what_you_do": "new value"}, "_test_enriched",
            cache_dir=str(tmp_path)
        )
        with open(cache_file) as f:
            data = json.load(f)
        assert data["what_you_do"] == "existing value"  # NOT overwritten

    def test_overwrite_when_requested(self, tmp_path):
        from scripts._common import cache_key, save_to_research_cache
        key = cache_key("Jane Smith")
        existing = {"name": "Jane Smith", "what_you_do": "existing value"}
        cache_file = tmp_path / f"{key}.json"
        with open(cache_file, 'w') as f:
            json.dump(existing, f)
        save_to_research_cache(
            "Jane Smith", {"what_you_do": "new value"}, "_test_enriched",
            cache_dir=str(tmp_path), overwrite=True
        )
        with open(cache_file) as f:
            data = json.load(f)
        assert data["what_you_do"] == "new value"  # Overwritten

    def test_skips_confidence_field(self, tmp_path):
        from scripts._common import save_to_research_cache
        save_to_research_cache(
            "Jane Smith",
            {"what_you_do": "coaching", "confidence": "high"},
            "_test_enriched",
            cache_dir=str(tmp_path),
        )
        files = list(tmp_path.glob("*.json"))
        with open(files[0]) as f:
            data = json.load(f)
        assert "confidence" not in data


# ── get_db_connection ─────────────────────────────────────────────

class TestGetDbConnection:
    @patch('psycopg2.connect')
    def test_returns_connection_and_cursor(self, mock_connect, monkeypatch):
        monkeypatch.setenv('DATABASE_URL', 'postgresql://fake:fake@localhost/fakedb')
        from scripts._common import get_db_connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        conn, cur = get_db_connection()
        assert conn is mock_conn
        mock_conn.cursor.assert_called_once()


# ── SKIP_DOMAINS ──────────────────────────────────────────────────

class TestSkipDomains:
    def test_contains_known_domains(self):
        from scripts._common import SKIP_DOMAINS
        assert 'calendly.com' in SKIP_DOMAINS
        assert 'linkedin.com' in SKIP_DOMAINS
        assert 'linktr.ee' in SKIP_DOMAINS

    def test_is_list(self):
        from scripts._common import SKIP_DOMAINS
        assert isinstance(SKIP_DOMAINS, list)


# ── setup_django ──────────────────────────────────────────────────

class TestSetupDjango:
    def test_project_root_on_path(self):
        from scripts._common import setup_django, PROJECT_ROOT
        setup_django()
        assert PROJECT_ROOT in sys.path

    def test_settings_module_set(self):
        """setup_django uses setdefault, so it won't override pytest.ini's setting."""
        from scripts._common import setup_django
        setup_django()
        # In test context, pytest.ini already sets config.test_settings
        assert 'DJANGO_SETTINGS_MODULE' in os.environ


import sys
