"""
Shared constants for the enrichment pipeline.

Centralizes values that are referenced across multiple modules
(consolidation_task, automated_enrichment_pipeline_safe, flows, etc.).
"""

SOURCE_PRIORITY: dict[str, int] = {
    'client_confirmed': 100,
    'client_ingest': 90,
    'manual_edit': 80,
    'csv_import': 60,
    'exa_research': 50,
    'ai_research': 40,
    'apollo': 30,
    'unknown': 0,
}

PROFILE_STATUS_CHOICES = [
    ('Member', 'Member'),
    ('Non Member Resource', 'Non Member Resource'),
    ('Pending', 'Pending'),
    ('Prospect', 'Prospect'),
    ('Qualified', 'Qualified'),
    ('Inactive', 'Inactive'),
]

VALID_STATUSES = {choice[0] for choice in PROFILE_STATUS_CHOICES}
