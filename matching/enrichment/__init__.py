"""
Match Enrichment and Multi-Agent Verification Package

Provides:
- TextSanitizer: Fix encoding, truncation, capitalization
- MatchEnrichmentService: Enrich matches with full profile data
- MatchVerificationAgent: Coordinate multiple verification agents
- Specialized agents: Encoding, Formatting, Content, Capitalization, Truncation
"""

from .match_enrichment import (
    # Core text utilities
    TextSanitizer,

    # Data classes
    EnrichedMatch,
    VerificationResult,
    VerificationStatus,
    VerificationIssue,

    # Enrichment service
    MatchEnrichmentService,

    # Verification agents
    MatchVerificationAgent,
    EncodingVerificationAgent,
    FormattingVerificationAgent,
    ContentVerificationAgent,
    CapitalizationVerificationAgent,
    TruncationVerificationAgent,

    # Main entry point
    enrich_and_verify_matches,
)

__all__ = [
    'TextSanitizer',
    'EnrichedMatch',
    'VerificationResult',
    'VerificationStatus',
    'VerificationIssue',
    'MatchEnrichmentService',
    'MatchVerificationAgent',
    'EncodingVerificationAgent',
    'FormattingVerificationAgent',
    'ContentVerificationAgent',
    'CapitalizationVerificationAgent',
    'TruncationVerificationAgent',
    'enrich_and_verify_matches',
]
