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
    # Directory scrapers — lowest priority so enrichment overwrites
    'scraper_youtube_api': 20,
    'scraper_speakerhub': 20,
    'scraper_muncheye': 20,
    'scraper_noomii': 20,
    'scraper_udemy': 20,
    'scraper_podchaser': 20,
    'scraper_gumroad': 20,
    'scraper_substack': 20,
    'scraper_icf_coaching': 20,
    'scraper_clickbank': 20,
    'scraper_jvzoo': 20,
    'scraper_warriorplus': 20,
    'scraper_apple_podcasts': 20,
    'scraper_eventbrite': 20,
    'scraper_medium': 20,
    'scraper_summit_speakers': 20,
    'scraper_sam_gov': 20,
    'scraper_sbir': 20,
    'scraper_sec_edgar': 20,
    'scraper_chambers': 20,
    'scraper_opencorporates': 20,
    'scraper_irs_exempt': 20,
    'scraper_sbir_gov': 20,
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
