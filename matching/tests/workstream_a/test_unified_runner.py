"""
Tests for A7 UnifiedEnrichmentRunner from scripts/run_enrichment.py

Covers:
    - run() dispatches to correct pass combinations
    - dry_run mode prints without mutations
    - pass2 reads quarantine JSONL and builds retry plans
    - pass3 queries stale profiles via psycopg2
    - Stats are accumulated correctly

The runner module does django.setup() at module level, so Django and
psycopg2 must be available. We mock SafeEnrichmentPipeline and psycopg2
to isolate the runner logic.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    """Ensure required env vars exist so module-level imports succeed."""
    monkeypatch.setenv('DATABASE_URL', 'postgresql://fake:fake@localhost/fakedb')
    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'config.settings')


@pytest.fixture
def runner_cls():
    """Import and return the UnifiedEnrichmentRunner class."""
    from scripts.run_enrichment import UnifiedEnrichmentRunner
    return UnifiedEnrichmentRunner


@pytest.fixture
def runner(runner_cls):
    """Create a runner instance with dry_run=True and small batch."""
    return runner_cls(batch_size=5, dry_run=True, max_age=90)


@pytest.fixture
def quarantine_dir(tmp_path):
    """Create a quarantine directory with a test JSONL file."""
    qdir = tmp_path / 'enrichment_batches' / 'quarantine'
    qdir.mkdir(parents=True)

    records = [
        {
            'profile_id': 'q1',
            'name': 'Quarantined User',
            'email': 'bad-email',
            'method': 'website_scrape',
            'issues': 'Invalid email format: bad-email',
            'failed_fields': ['email'],
            'timestamp': datetime.now().isoformat(),
        },
        {
            'profile_id': 'q2',
            'name': 'Another Quarantined',
            'email': 'http://not-email.com',
            'method': 'linkedin_scrape',
            'issues': 'URL found in email field',
            'failed_fields': ['email'],
            'timestamp': datetime.now().isoformat(),
        },
    ]

    jsonl_path = qdir / 'quarantine_20260214.jsonl'
    with open(jsonl_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')

    return str(tmp_path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRunPassDispatch:
    """run() should dispatch to the correct passes based on the list."""

    def test_run_all_three_passes(self, runner):
        with patch.object(runner, 'pass1_unenriched') as p1, \
             patch.object(runner, 'pass2_quarantine_retry') as p2, \
             patch.object(runner, 'pass3_stale_refresh') as p3:

            runner.run(['unenriched', 'quarantine', 'stale'])

            p1.assert_called_once()
            p2.assert_called_once()
            p3.assert_called_once()

    def test_run_quarantine_only(self, runner):
        with patch.object(runner, 'pass1_unenriched') as p1, \
             patch.object(runner, 'pass2_quarantine_retry') as p2, \
             patch.object(runner, 'pass3_stale_refresh') as p3:

            runner.run(['quarantine'])

            p1.assert_not_called()
            p2.assert_called_once()
            p3.assert_not_called()

    def test_run_stale_only(self, runner):
        with patch.object(runner, 'pass1_unenriched') as p1, \
             patch.object(runner, 'pass2_quarantine_retry') as p2, \
             patch.object(runner, 'pass3_stale_refresh') as p3:

            runner.run(['stale'])

            p1.assert_not_called()
            p2.assert_not_called()
            p3.assert_called_once()


class TestPass1Delegation:
    """pass1_unenriched delegates to SafeEnrichmentPipeline."""

    def test_pass1_creates_pipeline(self, runner):
        with patch('scripts.automated_enrichment_pipeline_safe.SafeEnrichmentPipeline') as MockPipeline:
            mock_instance = MockPipeline.return_value
            mock_instance.get_profiles_to_enrich.return_value = []

            runner.pass1_unenriched()

            # In dry_run mode with no profiles, should just print and return
            assert runner.stats['pass1_processed'] == 0


class TestPass2QuarantineRetry:
    """pass2 reads quarantine JSONL and builds retry plans."""

    def test_pass2_reads_quarantine_files(self, runner, quarantine_dir, monkeypatch):
        # Point the runner's quarantine dir lookup to our temp dir
        monkeypatch.setattr(
            os.path, 'abspath',
            lambda p: os.path.join(quarantine_dir, os.path.basename(p))
            if 'run_enrichment' in p else os.path.realpath(p)
        )

        # Directly patch the quarantine directory resolution within pass2
        script_dir = os.path.join(quarantine_dir)
        qdir = os.path.join(quarantine_dir, 'enrichment_batches', 'quarantine')

        with patch('scripts.run_enrichment.os.path.abspath', return_value=script_dir):
            with patch('scripts.run_enrichment.os.path.isdir', return_value=True):
                with patch('scripts.run_enrichment.glob.glob') as mock_glob:
                    jsonl_file = os.path.join(qdir, 'quarantine_20260214.jsonl')
                    mock_glob.return_value = [jsonl_file]

                    runner.pass2_quarantine_retry()

        # In dry_run mode, retries are printed but not executed
        assert runner.stats['pass2_retried'] == 2

    def test_pass2_no_quarantine_dir(self, runner):
        with patch('scripts.run_enrichment.os.path.isdir', return_value=False):
            with patch('scripts.run_enrichment.os.path.abspath', return_value='/fake'):
                runner.pass2_quarantine_retry()

        assert runner.stats['pass2_retried'] == 0


class TestPass3StaleRefresh:
    """pass3 queries stale profiles via psycopg2."""

    def test_pass3_queries_stale_profiles(self, runner):
        stale_profiles = [
            {
                'id': 's1', 'name': 'Stale User', 'email': 'stale@co.com',
                'company': 'OldCo', 'website': 'https://old.co',
                'linkedin': None, 'list_size': 50000,
                'last_enriched_at': datetime.now() - timedelta(days=120),
            },
        ]

        with patch('scripts.run_enrichment.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = stale_profiles

            runner.pass3_stale_refresh()

        assert runner.stats['pass3_refreshed'] == 1

    def test_pass3_no_stale_profiles(self, runner):
        with patch('scripts.run_enrichment.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = []

            runner.pass3_stale_refresh()

        assert runner.stats['pass3_refreshed'] == 0


class TestDryRunMode:
    """dry_run mode should not perform mutations."""

    def test_dry_run_pass3_no_updates(self, runner):
        """In dry_run, pass3 should not increment pass3_updated."""
        stale_profiles = [
            {
                'id': 's2', 'name': 'Stale Dry', 'email': 'dry@co.com',
                'company': 'DryCo', 'website': None,
                'linkedin': None, 'list_size': 10000,
                'last_enriched_at': datetime.now() - timedelta(days=100),
            },
        ]

        with patch('scripts.run_enrichment.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = stale_profiles

            runner.pass3_stale_refresh()

        # dry_run is True, so pass3_updated should remain 0
        assert runner.stats['pass3_updated'] == 0


class TestStatsAccumulation:
    """Stats should accumulate correctly across passes."""

    def test_stats_accumulate_across_all_passes(self, runner):
        """Verify that stats from multiple passes are all tracked."""
        # Start with all zeros
        assert runner.stats['pass1_processed'] == 0
        assert runner.stats['pass2_retried'] == 0
        assert runner.stats['pass3_refreshed'] == 0

        # Run pass1 (no profiles found)
        with patch('scripts.automated_enrichment_pipeline_safe.SafeEnrichmentPipeline') as MockPipeline:
            mock_inst = MockPipeline.return_value
            mock_inst.get_profiles_to_enrich.return_value = []

            runner.pass1_unenriched()

        # Run pass3 (1 stale profile found)
        with patch('scripts.run_enrichment.psycopg2') as mock_pg:
            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [
                {
                    'id': 'sa1', 'name': 'Acc User', 'email': 'acc@co.com',
                    'company': 'AccCo', 'website': None,
                    'linkedin': None, 'list_size': 1000,
                    'last_enriched_at': datetime.now() - timedelta(days=200),
                }
            ]

            runner.pass3_stale_refresh()

        assert runner.stats['pass1_processed'] == 0
        assert runner.stats['pass3_refreshed'] == 1
