"""
Root conftest for JV Matchmaker Platform test suite.

Handles:
- Django settings configuration (PostgreSQL via DATABASE_URL)
- Unmanaged model (SupabaseProfile, SupabaseMatch) table creation via pre_migrate signal
- Shared fixtures for sample data and mocked services
"""

import os
import json
import pytest
from unittest.mock import MagicMock, patch

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')


# ---------------------------------------------------------------------------
# Hooks: create unmanaged model tables before migrations need them
# ---------------------------------------------------------------------------

_signal_connected = False


def pytest_configure(config):
    """Connect pre_migrate signal to create unmanaged model tables.

    The unmanaged models (SupabaseProfile, SupabaseMatch) have managed=False,
    so Django migrations skip CREATE TABLE for them. But managed models like
    SavedCandidate have FK constraints pointing to 'profiles', which fails
    if the table doesn't exist during migration.

    Solution: use Django's pre_migrate signal to create the tables (with the
    full current model schema) right before the matching app's migrations run.
    """
    global _signal_connected
    if _signal_connected:
        return

    from django.apps import apps
    from django.db.models.signals import pre_migrate

    def create_unmanaged_tables(sender, **kwargs):
        """Create tables for unmanaged models before matching migrations run."""
        if sender.label != 'matching':
            return

        from django.db import connection

        for model_name in ('SupabaseProfile', 'SupabaseMatch'):
            try:
                model = apps.get_model('matching', model_name)
                old_managed = model._meta.managed
                model._meta.managed = True
                try:
                    with connection.schema_editor() as editor:
                        editor.create_model(model)
                except Exception:
                    pass  # Table may already exist (keepdb mode)
                finally:
                    model._meta.managed = old_managed
            except LookupError:
                pass

    pre_migrate.connect(create_unmanaged_tables)
    _signal_connected = True


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

@pytest.fixture(scope='session')
def django_db_setup(django_test_environment, django_db_blocker):
    """Create (or reuse) test DB and run migrations.

    Uses keepdb=True so a stale test database from a previous run is
    reused rather than dropped and recreated. This avoids PostgreSQL's
    "database is being accessed by other users" errors entirely.

    The pre_migrate signal handler (above) ensures unmanaged model tables
    exist before any migration tries to create FK constraints to them.
    """
    from django.test.utils import setup_databases, teardown_databases

    with django_db_blocker.unblock():
        db_cfg = setup_databases(
            verbosity=0,
            interactive=False,
            keepdb=True,
            aliases=['default'],
        )

    yield

    # Don't tear down — keepdb mode reuses the database across runs.
    # This is safe because migrations are idempotent.


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_profile_data():
    """Valid profile data dict for gate/enrichment testing."""
    return {
        'name': 'Jane Smith',
        'email': 'jane@acmecorp.com',
        'company': 'Acme Corp',
        'website': 'https://acmecorp.com',
        'linkedin': 'https://linkedin.com/in/janesmith',
        'who_you_serve': 'Entrepreneurs and small business owners',
        'what_you_do': 'Business coaching and strategic planning',
        'seeking': 'JV partners for cross-promotion',
        'offering': 'Access to 10K email list of engaged entrepreneurs',
        'bio': 'Award-winning business coach with 15 years experience',
        'list_size': 10000,
    }


@pytest.fixture
def sample_partner_profile():
    """Partner profile dict for match enrichment testing."""
    return {
        'name': 'Bob Johnson',
        'email': 'bob@partnerco.com',
        'company': 'Partner Co',
        'website': 'https://partnerco.com',
        'who_you_serve': 'Online course creators and coaches',
        'what_you_do': 'Marketing automation for digital educators',
        'seeking': 'Joint ventures with established coaches',
        'offering': 'Free webinar co-hosting and tech support',
        'bio': 'Former Google engineer turned marketing automation expert',
        'signature_programs': 'LaunchPad Academy, Automation Mastery',
        'current_projects': 'AI-powered email sequence builder',
        'tags': ['marketing', 'automation', 'courses'],
        'list_size': 5000,
        'niche': 'Digital marketing',
    }


@pytest.fixture
def sample_client_profile():
    """Client profile dict for MatchEnrichmentService."""
    return {
        'name': 'Janet Bray Attwood',
        'company': 'Enlightened Alliance',
        'who_you_serve': 'Transformational leaders and coaches',
        'what_you_do': 'Helping people discover their passions and purpose',
        'seeking': 'JV partners for summit and course launches',
        'offering': 'Access to 200K+ subscriber list',
        'list_size': 200000,
        'bio': 'NYT bestselling author of The Passion Test',
        'signature_programs': 'The Passion Test, Passion Test Certification',
    }


@pytest.fixture
def sample_match_data():
    """Basic match data dict for enrich_match()."""
    return {
        'name': 'Bob Johnson',
        'company': 'Partner Co',
        'email': 'bob@partnerco.com',
        'linkedin': 'https://linkedin.com/in/bobjohnson',
        'website': 'https://partnerco.com',
        'niche': 'Digital marketing',
        'list_size': 5000,
        'score': 0.85,
    }


@pytest.fixture
def mock_claude_service():
    """Mock ClaudeVerificationService for LLM tests."""
    with patch('matching.enrichment.match_enrichment.ClaudeVerificationService') as MockCls:
        instance = MockCls.return_value
        instance.is_available.return_value = True
        instance.model = 'anthropic/claude-sonnet-4-20250514'
        instance.api_key = 'test-key'
        yield instance


@pytest.fixture
def sample_llm_explanation():
    """Sample LLM-generated explanation dict."""
    return {
        'what_partner_b_brings_to_a': {
            'summary': 'Bob brings marketing automation expertise and a 5K list of course creators.',
            'key_points': [
                'Marketing automation tools for launch support',
                'Direct access to digital educator audience',
            ],
        },
        'what_partner_a_brings_to_b': {
            'summary': 'Janet brings a 200K subscriber list and brand authority in personal development.',
            'key_points': [
                '200K+ engaged subscriber list',
                'Bestselling author credibility for co-branding',
            ],
        },
        'connection_insights': [
            {'type': 'obvious', 'insight': "Bob's course creators need Janet's audience for launches."},
            {'type': 'non_obvious', 'insight': "Janet's Passion Test graduates are ideal customers for Bob's automation tools."},
        ],
        'reciprocity_assessment': {
            'balance': 'slightly_asymmetric',
            'stronger_side': 'partner_a',
            'explanation': 'Janet brings a larger audience, but Bob offers specialized tech value.',
            'gap': None,
        },
        'citations': {
            "Bob's course creators": 'partner_b.who_you_serve',
            "Janet's 200K list": 'partner_a.list_size',
        },
        'confidence': {
            'data_richness': 'high',
            'explanation_confidence': 'high',
        },
    }


@pytest.fixture
def sample_verification_response():
    """Sample verification response (Call 2) dict."""
    return {
        'claims': [
            {'claim': 'Bob has 5K list', 'status': 'grounded', 'source_field': 'partner_b.list_size'},
            {'claim': 'Janet has 200K list', 'status': 'grounded', 'source_field': 'partner_a.list_size'},
            {'claim': 'Automation tools useful for coaches', 'status': 'inferred', 'source_field': 'partner_b.what_you_do'},
        ],
        'grounded_percentage': 0.85,
        'recommendation': 'use_as_is',
    }
