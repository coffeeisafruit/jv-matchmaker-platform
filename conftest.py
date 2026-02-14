"""
Root conftest for JV Matchmaker Platform test suite.

Handles:
- Django settings configuration
- Unmanaged model (SupabaseProfile, SupabaseMatch) table creation for SQLite
- ArrayField → JSONField shim for SQLite compatibility
- Shared fixtures for sample data and mocked services
"""

import os
import json
import pytest
from unittest.mock import MagicMock, patch

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')


# ---------------------------------------------------------------------------
# Unmanaged model handling: make SupabaseProfile / SupabaseMatch creatable
# in the test database by temporarily setting managed = True
# ---------------------------------------------------------------------------

@pytest.fixture(scope='session')
def django_db_modify_db_settings():
    """Ensure tests use SQLite (no DATABASE_URL)."""
    os.environ.pop('DATABASE_URL', None)


def _patch_unmanaged_models():
    """Set managed=True on unmanaged models so Django creates their tables."""
    from django.apps import apps
    unmanaged = [
        ('matching', 'SupabaseProfile'),
        ('matching', 'SupabaseMatch'),
    ]
    originals = {}
    for app_label, model_name in unmanaged:
        try:
            model = apps.get_model(app_label, model_name)
            originals[(app_label, model_name)] = model._meta.managed
            model._meta.managed = True
        except LookupError:
            pass
    return originals


def _restore_unmanaged_models(originals):
    """Restore original managed state."""
    from django.apps import apps
    for (app_label, model_name), managed in originals.items():
        try:
            model = apps.get_model(app_label, model_name)
            model._meta.managed = managed
        except LookupError:
            pass


@pytest.fixture(scope='session')
def django_db_setup(django_test_environment, django_db_modify_db_settings):
    """Create all tables including unmanaged models."""
    from django.test.utils import setup_databases, teardown_databases
    from django.conf import settings

    # Patch ArrayField for SQLite: replace with JSONField
    _patch_arrayfield_for_sqlite()

    originals = _patch_unmanaged_models()

    db_cfg = setup_databases(
        verbosity=0,
        interactive=False,
        aliases=['default'],
    )

    yield

    _restore_unmanaged_models(originals)
    teardown_databases(db_cfg, verbosity=0)


def _patch_arrayfield_for_sqlite():
    """Replace ArrayField with JSONField for SQLite compatibility."""
    from django.conf import settings
    db_engine = settings.DATABASES.get('default', {}).get('ENGINE', '')
    if 'sqlite' in db_engine:
        from django.db import models as django_models
        from django.contrib.postgres import fields as pg_fields

        class FakeArrayField(django_models.JSONField):
            """Drop-in replacement for ArrayField on SQLite."""
            def __init__(self, base_field=None, size=None, **kwargs):
                kwargs.pop('base_field', None)
                kwargs.pop('size', None)
                super().__init__(**kwargs)

            def deconstruct(self):
                name, path, args, kwargs = super().deconstruct()
                return name, path, args, kwargs

        pg_fields.ArrayField = FakeArrayField


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
