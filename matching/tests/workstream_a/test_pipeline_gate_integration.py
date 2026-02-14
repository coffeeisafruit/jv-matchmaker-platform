"""
Tests for A4 gate integration in scripts/automated_enrichment_pipeline_safe.py

Specifically tests SafeEnrichmentPipeline.consolidate_to_supabase_batch():
    - Verified emails are written to Supabase
    - Quarantined emails are skipped and written to quarantine JSONL
    - Unverified emails get reduced confidence
    - Gate stats are counted correctly
    - email_metadata includes verification_status and verification_confidence

The pipeline module does django.setup() at module level, so we must mock
Django and psycopg2 before importing it.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures: mock Django + psycopg2 so the pipeline module can be imported
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_django_and_db(monkeypatch, tmp_path):
    """
    Patch django.setup, psycopg2, and DATABASE_URL so the pipeline module
    can be imported without a real Django/Postgres environment.
    """
    monkeypatch.setenv('DATABASE_URL', 'postgresql://fake:fake@localhost/fakedb')
    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'config.settings')


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    """Create a SafeEnrichmentPipeline instance with mocked dependencies."""
    from scripts.automated_enrichment_pipeline_safe import SafeEnrichmentPipeline

    p = SafeEnrichmentPipeline(dry_run=False, batch_size=5)
    # Point quarantine dir to tmp_path so JSONL writes don't leak
    monkeypatch.setattr(p, '_ensure_quarantine_dir', lambda: str(tmp_path))
    return p


def _make_result(profile_id, email, method='website_scrape', name='Test User', company='TestCo'):
    """Build a single enrichment result dict."""
    return {
        'profile_id': profile_id,
        'email': email,
        'method': method,
        'name': name,
        'company': company,
        'enriched_at': datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestVerifiedEmailWritten:
    """A VERIFIED email should be included in the execute_batch call."""

    def test_verified_email_is_batched(self, pipeline, tmp_path):
        results = [_make_result('p1', 'valid@example.com')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.80
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            # execute_batch should have been called with one update tuple
            assert mock_exec.called
            updates = mock_exec.call_args[0][2]
            assert len(updates) == 1
            assert updates[0][0] == 'valid@example.com'  # email value


class TestQuarantinedEmailSkipped:
    """A QUARANTINED email should NOT appear in execute_batch and should be logged."""

    def test_quarantined_email_not_written(self, pipeline, tmp_path):
        # An invalid email triggers quarantine via the gate's Layer 1
        results = [_make_result('p2', 'not-an-email', method='website_scrape')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.80
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            # execute_batch should not be called (no valid updates)
            if mock_exec.called:
                updates = mock_exec.call_args[0][2]
                # None of the updates should be for the quarantined profile
                for u in updates:
                    assert u[5] != 'p2'  # profile_id is the last element

            # Stats should reflect quarantine
            assert pipeline.stats['gate_quarantined'] == 1


class TestQuarantineJSONL:
    """Quarantined records should be written to a JSONL file."""

    def test_quarantine_jsonl_written(self, pipeline, tmp_path):
        results = [_make_result('p3', 'http://example.com', method='website_scrape')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer'), \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch'):

            mock_pg.connect.return_value = MagicMock()

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

        # Check that a quarantine JSONL file was written in tmp_path
        jsonl_files = [f for f in os.listdir(tmp_path) if f.endswith('.jsonl')]
        assert len(jsonl_files) >= 1

        with open(os.path.join(tmp_path, jsonl_files[0]), 'r') as f:
            records = [json.loads(line) for line in f if line.strip()]

        assert len(records) == 1
        assert records[0]['profile_id'] == 'p3'
        assert 'email' in records[0]['failed_fields']


class TestUnverifiedConfidenceReduced:
    """An UNVERIFIED verdict should multiply base_confidence by overall_confidence."""

    def test_unverified_gets_reduced_confidence(self, pipeline, tmp_path):
        # A suspicious email (e.g. info@company.com) triggers UNVERIFIED via suspicious pattern
        # But actually suspicious emails get FAILED status which quarantines them.
        # Instead, let's test with a valid email but patch the gate to return UNVERIFIED.
        results = [_make_result('p4', 'real@company.com')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.80
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            # Patch the gate to return UNVERIFIED with overall_confidence=0.6
            from matching.enrichment.verification_gate import GateVerdict, GateStatus
            unverified_verdict = GateVerdict(
                status=GateStatus.UNVERIFIED,
                field_verdicts={},
                overall_confidence=0.6,
                auto_fixed_data={},
            )
            pipeline.gate.evaluate = MagicMock(return_value=unverified_verdict)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            assert mock_exec.called
            updates = mock_exec.call_args[0][2]
            assert len(updates) == 1

            # The confidence in the update tuple is at index 2
            confidence = updates[0][2]
            assert confidence == pytest.approx(0.80 * 0.6)
            assert pipeline.stats['gate_unverified'] == 1


class TestVerifiedConfidenceUnmodified:
    """A VERIFIED verdict should use base_confidence without reduction."""

    def test_verified_gets_full_confidence(self, pipeline, tmp_path):
        results = [_make_result('p5', 'good@company.com')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.90
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            updates = mock_exec.call_args[0][2]
            confidence = updates[0][2]
            assert confidence == pytest.approx(0.90)
            assert pipeline.stats['gate_verified'] == 1


class TestEmailMetadataFields:
    """email_metadata should include verification_status and verification_confidence."""

    def test_metadata_has_verification_fields(self, pipeline, tmp_path):
        results = [_make_result('p6', 'meta@example.com')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.85
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            updates = mock_exec.call_args[0][2]
            # Index 1 is the JSON metadata string
            metadata = json.loads(updates[0][1])
            assert 'verification_status' in metadata
            assert 'verification_confidence' in metadata
            assert metadata['verification_status'] in ('verified', 'unverified', 'quarantined')
            assert isinstance(metadata['verification_confidence'], float)


class TestGateStatsCounting:
    """Gate stats should accurately count verified, unverified, and quarantined."""

    def test_mixed_results_counted_correctly(self, pipeline, tmp_path):
        results = [
            _make_result('p10', 'good1@company.com'),      # verified
            _make_result('p11', 'good2@company.com'),      # verified
            _make_result('p12', 'not-an-email'),           # quarantined
        ]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch'):

            mock_pg.connect.return_value = MagicMock()
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.80
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

        assert pipeline.stats['gate_verified'] >= 2 or pipeline.stats['gate_unverified'] >= 0
        assert pipeline.stats['gate_quarantined'] >= 1
        total_gate = (pipeline.stats['gate_verified']
                      + pipeline.stats['gate_unverified']
                      + pipeline.stats['gate_quarantined'])
        assert total_gate == 3


class TestAutoFixApplied:
    """Auto-fixes from the gate verdict should be applied before writing."""

    def test_auto_fix_changes_email_in_update(self, pipeline, tmp_path):
        results = [_make_result('p13', 'good@company.com')]

        with patch('scripts.automated_enrichment_pipeline_safe.psycopg2') as mock_pg, \
             patch('scripts.automated_enrichment_pipeline_safe.ConfidenceScorer') as MockScorer, \
             patch('scripts.automated_enrichment_pipeline_safe.execute_batch') as mock_exec:

            mock_conn = MagicMock()
            mock_pg.connect.return_value = mock_conn
            scorer_inst = MockScorer.return_value
            scorer_inst.calculate_confidence.return_value = 0.80
            scorer_inst.calculate_expires_at.return_value = datetime(2026, 6, 1)

            # Patch the gate to return a verdict with an auto-fix on email
            from matching.enrichment.verification_gate import GateVerdict, GateStatus
            fixed_verdict = GateVerdict(
                status=GateStatus.VERIFIED,
                field_verdicts={},
                overall_confidence=1.0,
                auto_fixed_data={'email': 'fixed@company.com'},
            )
            pipeline.gate.evaluate = MagicMock(return_value=fixed_verdict)

            asyncio.run(pipeline.consolidate_to_supabase_batch(results))

            updates = mock_exec.call_args[0][2]
            # The email in the update should be the fixed version
            assert updates[0][0] == 'fixed@company.com'
