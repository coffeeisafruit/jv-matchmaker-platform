"""
Prefect flow decomposition of the enrichment pipeline.

This package decomposes the monolithic automated_enrichment_pipeline_safe.py
into composable Prefect @flow and @task functions across five pipeline systems.

Enrichment pipeline:
    profile_selection      - @task: tiered SQL queries for profile selection
    email_discovery        - @task: website/LinkedIn/Apollo email finding
    ai_research_task       - @task: Pydantic AI research agent per profile
    validation_task        - @task: L1+L2 deterministic verification checks
    ai_verification_task   - @task: L3 optional AI verification
    consolidation_task     - @task: batch DB write with source priority
    retry_subflow          - @flow: auto-retry quarantined profiles
    enrichment_flow        - @flow: main enrichment pipeline orchestrator

Acquisition pipeline:
    gap_detection          - @task: detect match quality gaps per client
    prospect_discovery     - @task: Exa-powered prospect discovery
    prospect_prescoring    - @task: ISMC pre-filter scoring on partial data
    prospect_ingestion     - @task: save prospects to DB with dedup
    acquisition_flow       - @flow: full acquisition pipeline orchestrator

Contact ingestion:
    contact_ingestion      - @task: ingest contacts from CSV/API with dedup
    cross_client_scoring   - @task: score new profiles against all clients
    new_contact_flow       - @flow: ingest → enrich → score → flag reports

Profile freshness:
    content_hash_check     - @task: SHA-256 change detection (Layer 1)
    semantic_triage        - @task: Claude semantic triage (Layer 2)
    change_detection_flow  - @flow: hash → triage → queue re-enrichment

Monthly automation:
    client_verification    - @flow: Week 3 client verification emails
    monthly_processing     - @flow: Week 4 Mon full processing pipeline
    admin_notification     - @flow: Week 4 Tue admin report with AI suggestions
    report_delivery        - @flow: 1st of month report delivery
    report_regeneration    - @task: regenerate flagged member reports
    monthly_orchestrator   - @flow: top-level monthly cycle coordinator

Shared:
    cost_tracking          - @task: per-query cost logging and monthly reports
"""
