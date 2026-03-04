"""
Enrichment cascade — 6-layer self-healing pipeline.

Layers:
    1. Free HTTP extraction (email, phone, social from websites)
    2. Rescore + filter (jv_triage re-scoring, threshold 50+)
    3. GPU AI enrichment (configurable LLM endpoint via LLM_BASE_URL)
    4. Claude conflict resolution (when AI ≠ existing data)
    5. Cross-client scoring (score enriched profiles against all clients)
    6. Gap detection + targeted acquisition (ensure 30+ matches per client)

Usage:
    from matching.enrichment.cascade import (
        Layer1FreeExtraction,
        Layer2RescoreFilter,
        Layer3GpuEnrichment,
        Layer4ClaudeJudge,
        CascadeCheckpoint,
        CascadeLearningLog,
        PartnerPipeline,
    )
"""

from matching.enrichment.cascade.checkpoint import CascadeCheckpoint
