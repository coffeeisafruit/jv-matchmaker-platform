"""
Extend existing merge_duplicates.py with confidence scoring.
Backward compatible with existing simple merge.

This module provides confidence-aware merging while falling back to the
existing "keep longer/more complete value" logic when metadata is unavailable.
"""

from datetime import datetime
from typing import Dict, Tuple, Optional
from matching.enrichment.confidence import ConfidenceScorer


class ProfileMerger:
    """
    Extends existing merge logic with confidence scoring.

    The original merge_duplicates.py uses simple logic:
    - Keep the longer/more complete value between two fields

    This extends it with confidence-based selection:
    - If metadata available: pick field with higher current confidence
    - If no metadata: fall back to original simple merge logic
    """

    def __init__(self):
        self.confidence_scorer = ConfidenceScorer()

    def merge_field(
        self,
        field_name: str,
        value1: Optional[str],
        metadata1: Optional[Dict],
        value2: Optional[str],
        metadata2: Optional[Dict]
    ) -> Tuple[Optional[str], Dict]:
        """
        Merge two values for same field using confidence.

        Falls back to existing simple logic if no metadata available.

        Args:
            field_name: Name of the field being merged
            value1: First value
            metadata1: Enrichment metadata for first value (or None)
            value2: Second value
            metadata2: Enrichment metadata for second value (or None)

        Returns:
            Tuple of (merged_value, merged_metadata)

        Examples:
            # With metadata - use confidence
            >>> merger = ProfileMerger()
            >>> val, meta = merger.merge_field(
            ...     'email',
            ...     'old@example.com',
            ...     {'source': 'apollo', 'enriched_at': '2025-01-01T00:00:00', 'confidence': 0.3},
            ...     'new@example.com',
            ...     {'source': 'apollo_verified', 'enriched_at': '2026-02-01T00:00:00', 'confidence': 0.95}
            ... )
            >>> val
            'new@example.com'  # Higher confidence wins

            # Without metadata - use simple merge
            >>> val, meta = merger.merge_field('email', 'short@e.co', None, 'longer@example.com', None)
            >>> val
            'longer@example.com'  # Longer value wins
        """
        # If no metadata for either, use existing simple logic
        if not metadata1 and not metadata2:
            return self._simple_merge(value1, value2), {}

        # Calculate current confidence for both (if metadata exists)
        conf1 = self._get_current_confidence(field_name, value1, metadata1) if metadata1 else 0.0
        conf2 = self._get_current_confidence(field_name, value2, metadata2) if metadata2 else 0.0

        # Pick higher confidence
        if conf1 >= conf2:
            return value1, metadata1 or {}
        else:
            return value2, metadata2 or {}

    def merge_profile_metadata(
        self,
        metadata1: Dict[str, Dict],
        metadata2: Dict[str, Dict]
    ) -> Dict[str, Dict]:
        """
        Merge enrichment_metadata from two profiles.

        For each field, keep the metadata with higher confidence.

        Args:
            metadata1: First profile's enrichment_metadata
            metadata2: Second profile's enrichment_metadata

        Returns:
            Dict: Merged enrichment metadata

        Example:
            {
                'email': {'source': 'apollo', 'confidence': 0.95, ...},
                'seeking': {'source': 'owl', 'confidence': 0.75, ...}
            }
        """
        merged = {}

        # Get all unique fields
        all_fields = set(metadata1.keys()) | set(metadata2.keys())

        for field in all_fields:
            field_meta1 = metadata1.get(field, {})
            field_meta2 = metadata2.get(field, {})

            # If only one has metadata, use that
            if not field_meta1:
                merged[field] = field_meta2
                continue
            if not field_meta2:
                merged[field] = field_meta1
                continue

            # Both have metadata - compare confidence
            conf1 = field_meta1.get('confidence', 0.0)
            conf2 = field_meta2.get('confidence', 0.0)

            merged[field] = field_meta1 if conf1 >= conf2 else field_meta2

        return merged

    def _simple_merge(self, val1: Optional[str], val2: Optional[str]) -> Optional[str]:
        """
        Existing logic from merge_duplicates.py: keep longer/more complete value.

        This is the fallback when no metadata is available.

        Args:
            val1: First value
            val2: Second value

        Returns:
            The longer/more complete value

        Examples:
            >>> merger = ProfileMerger()
            >>> merger._simple_merge('short', 'much longer value')
            'much longer value'
            >>> merger._simple_merge('only one', None)
            'only one'
            >>> merger._simple_merge(None, 'only one')
            'only one'
        """
        # Strip and normalize
        val1 = (val1 or '').strip()
        val2 = (val2 or '').strip()

        # If both exist, keep longer
        if val1 and val2:
            return val1 if len(val1) >= len(val2) else val2

        # Otherwise return whichever exists
        return val1 or val2

    def _get_current_confidence(
        self,
        field_name: str,
        value: Optional[str],
        metadata: Optional[Dict]
    ) -> float:
        """
        Calculate current confidence from metadata.

        If metadata has pre-calculated confidence, recalculate it based on current time
        to account for age decay.

        Args:
            field_name: Name of the field
            value: Current field value
            metadata: Enrichment metadata

        Returns:
            float: Current confidence score (0.0-1.0)
        """
        if not metadata or not value:
            return 0.0

        # If metadata has pre-calculated confidence but is old, recalculate
        if 'enriched_at' in metadata:
            enriched_at = datetime.fromisoformat(metadata['enriched_at'])

            # Parse verified_at if present
            verified_at = None
            if 'verified_at' in metadata and metadata['verified_at']:
                verified_at = datetime.fromisoformat(metadata['verified_at'])

            # Calculate fresh confidence
            return self.confidence_scorer.calculate_confidence(
                field_name=field_name,
                source=metadata.get('source', 'unknown'),
                enriched_at=enriched_at,
                verified_at=verified_at,
                verification_count=metadata.get('verification_count', 0),
                cross_validated_by=metadata.get('cross_validated_by')
            )

        # Fall back to stored confidence if no enriched_at
        return metadata.get('confidence', 0.0)


# Example usage and tests
if __name__ == '__main__':
    merger = ProfileMerger()

    print("=" * 70)
    print("PROFILE MERGER EXAMPLES")
    print("=" * 70)
    print()

    # Example 1: Simple merge (no metadata)
    print("1. Simple merge (no metadata) - falls back to 'keep longer':")
    val, meta = merger.merge_field('email', 'short@e.co', None, 'longer@example.com', None)
    print(f"   Result: {val}")
    print(f"   Reason: Longer value wins (existing merge_duplicates.py logic)")
    print()

    # Example 2: Confidence-based merge
    print("2. Confidence-based merge:")
    val, meta = merger.merge_field(
        'email',
        'old@example.com',
        {
            'source': 'apollo',
            'enriched_at': '2025-01-01T00:00:00',
            'confidence': 0.30  # Old, decayed
        },
        'new@example.com',
        {
            'source': 'apollo_verified',
            'enriched_at': '2026-02-09T00:00:00',
            'confidence': 0.95  # Fresh, verified
        }
    )
    print(f"   Result: {val}")
    print(f"   Reason: Higher confidence (0.95 > 0.30)")
    print()

    # Example 3: One has metadata, one doesn't
    print("3. One has metadata, one doesn't:")
    val, meta = merger.merge_field(
        'email',
        'verified@example.com',
        {
            'source': 'manual',
            'enriched_at': '2026-01-01T00:00:00',
            'confidence': 0.98
        },
        'unverified@example.com',
        None
    )
    print(f"   Result: {val}")
    print(f"   Reason: Value with metadata wins")
    print()

    # Example 4: Merge profile metadata
    print("4. Merge full profile metadata:")
    metadata1 = {
        'email': {
            'source': 'apollo',
            'confidence': 0.80,
            'enriched_at': '2026-01-01T00:00:00'
        },
        'seeking': {
            'source': 'owl',
            'confidence': 0.85,
            'enriched_at': '2026-02-01T00:00:00'
        }
    }
    metadata2 = {
        'email': {
            'source': 'apollo_verified',
            'confidence': 0.95,
            'enriched_at': '2026-02-09T00:00:00'
        },
        'offering': {
            'source': 'manual',
            'confidence': 1.0,
            'enriched_at': '2026-02-09T00:00:00'
        }
    }
    merged = merger.merge_profile_metadata(metadata1, metadata2)
    print(f"   Email: {merged['email']['source']} (conf: {merged['email']['confidence']})")
    print(f"   Seeking: {merged['seeking']['source']} (conf: {merged['seeking']['confidence']})")
    print(f"   Offering: {merged['offering']['source']} (conf: {merged['offering']['confidence']})")
    print(f"   Reason: For each field, keep higher confidence source")
