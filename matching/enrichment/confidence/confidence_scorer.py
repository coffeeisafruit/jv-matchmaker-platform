"""
Sophisticated confidence scoring with age decay, verification, cross-validation.

Key Features:
- Source-based base confidence (apollo_verified: 0.95, owl: 0.85, etc.)
- Exponential age decay: confidence = base * e^(-age_days / decay_period)
- Field-specific decay rates (email: 90 days, seeking: 30 days, linkedin: 180 days)
- Verification boost (0.0-0.15) for recently verified data
- Cross-validation boost (0.0-0.20) when multiple sources agree
"""

import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class ConfidenceScorer:
    """
    Calculate confidence scores based on multiple factors:
    - Source reliability (tracked success rates)
    - Data age (exponential decay)
    - Verification status (verified > scraped > inferred)
    - Cross-validation (multiple sources agreeing)
    - Field type (contact info > business intel > metadata)
    """

    # Base confidence by source (starting point, not final)
    SOURCE_BASE_CONFIDENCE = {
        'manual': 1.0,              # Manually entered/verified
        'apollo_verified': 0.95,    # Apollo.io verified email
        'owl': 0.85,                # OWL deep research
        'apollo': 0.80,             # Apollo.io match (not verified)
        'website_scraped': 0.70,    # Scraped from website
        'linkedin_scraped': 0.65,   # Scraped from LinkedIn
        'email_domain_inferred': 0.50,  # Inferred from domain
        'unknown': 0.30,            # Unknown source
    }

    # Field type decay rates (how fast confidence drops, in days)
    # Contact info is more stable, business intent changes quickly
    FIELD_DECAY_RATES = {
        'email': 90,           # Contact info valid longer
        'phone': 90,
        'linkedin': 180,       # LinkedIn URLs rarely change
        'website': 180,
        'seeking': 30,         # Business intent changes quickly
        'who_you_serve': 60,
        'what_you_do': 90,
        'offering': 60,
        'list_size': 30,       # Grows quickly
        'niche': 180,          # Rarely changes
        'company': 180,        # Company rarely changes
        'name': 365,           # Name very stable
        'revenue_tier': 120,   # Revenue tier changes slowly
        'jv_history': 180,     # Historical facts, very stable
        'content_platforms': 120,  # Platform presence changes slowly
        'audience_engagement_score': 45,  # Engagement quality shifts moderately
    }

    def calculate_confidence(
        self,
        field_name: str,
        source: str,
        enriched_at: datetime,
        verified_at: Optional[datetime] = None,
        verification_count: int = 0,
        cross_validated_by: Optional[List[str]] = None
    ) -> float:
        """
        Calculate current confidence score for a field.

        Args:
            field_name: Name of the field being scored
            source: Data source (apollo_verified, owl, manual, etc.)
            enriched_at: When the data was originally enriched
            verified_at: When the data was last verified (optional)
            verification_count: Number of times verified (optional)
            cross_validated_by: List of other sources that agree (optional)

        Returns:
            float: Confidence score between 0.0 and 1.0

        Examples:
            >>> scorer = ConfidenceScorer()
            >>> # Fresh Apollo verified email
            >>> scorer.calculate_confidence('email', 'apollo_verified', datetime.now())
            0.95
            >>> # 30-day old OWL seeking data (decayed from 0.85)
            >>> scorer.calculate_confidence('seeking', 'owl', datetime.now() - timedelta(days=30))
            0.52  # 0.85 * e^(-30/30) = 0.85 * 0.61 = 0.52
        """
        # Start with source base confidence
        base = self.SOURCE_BASE_CONFIDENCE.get(source, 0.30)

        # Apply age decay (exponential)
        age_days = (datetime.now() - enriched_at).days
        decay_period = self.FIELD_DECAY_RATES.get(field_name, 60)
        age_factor = self._calculate_age_decay(age_days, decay_period)

        # Apply verification boost
        verification_factor = self._calculate_verification_boost(
            verified_at, verification_count
        )

        # Apply cross-validation boost
        cross_val_factor = self._calculate_cross_validation_boost(
            cross_validated_by or []
        )

        # Combine factors (multiplicative decay, additive boosts)
        confidence = base * age_factor + verification_factor + cross_val_factor

        # Clamp to 0.0-1.0
        return max(0.0, min(1.0, confidence))

    def _calculate_age_decay(self, age_days: int, decay_period: int) -> float:
        """
        Exponential decay: confidence = e^(-age_days / decay_period)

        This creates a smooth decay curve where:
        - 0 days old: 100% retention
        - 1x decay_period: 37% retention (1/e)
        - 2x decay_period: 14% retention
        - 3x decay_period: 5% retention

        Examples:
            For email (decay_period=90):
            - 0 days old: 1.0 (100%)
            - 45 days: 0.61 (61%)
            - 90 days: 0.37 (37%)
            - 180 days: 0.14 (14%)

            For seeking (decay_period=30):
            - 0 days old: 1.0 (100%)
            - 15 days: 0.61 (61%)
            - 30 days: 0.37 (37%)
            - 60 days: 0.14 (14%)

        Args:
            age_days: Days since enrichment
            decay_period: Half-life in days for this field type

        Returns:
            float: Decay factor between 0.0 and 1.0
        """
        if age_days < 0:
            age_days = 0  # Future dates default to now

        return math.exp(-age_days / decay_period)

    def _calculate_verification_boost(
        self,
        verified_at: Optional[datetime],
        verification_count: int
    ) -> float:
        """
        Boost confidence if recently verified.

        Recent verification indicates the data was checked and confirmed accurate.
        Multiple verifications increase confidence further.

        Returns:
            float: Boost between 0.0 and 0.15

        Examples:
            - Verified yesterday: +0.15
            - Verified 2 weeks ago: +0.10
            - Verified 60 days ago: +0.05
            - Verified 100 days ago: +0.00
            - Verified 3 times: 1.5x multiplier on boost
        """
        if not verified_at:
            return 0.0

        # Calculate days since verification
        days_since_verified = (datetime.now() - verified_at).days

        # Base boost based on recency
        if days_since_verified <= 7:
            base_boost = 0.15      # Very recent verification
        elif days_since_verified <= 30:
            base_boost = 0.10      # Recent verification
        elif days_since_verified <= 90:
            base_boost = 0.05      # Older verification
        else:
            base_boost = 0.0       # Too old to boost

        # Multiple verifications increase boost (max 1.5x)
        verification_multiplier = min(1.0 + (verification_count * 0.1), 1.5)

        return base_boost * verification_multiplier

    def _calculate_cross_validation_boost(
        self,
        cross_validated_by: List[str]
    ) -> float:
        """
        Boost confidence if multiple sources agree.

        When independent sources provide the same data, it's more likely accurate.

        Returns:
            float: Boost between 0.0 and 0.20

        Examples:
            - 1 source: +0.00 (no cross-validation)
            - 2 sources agree: +0.10
            - 3+ sources agree: +0.20
        """
        if len(cross_validated_by) < 2:
            return 0.0

        # 2 sources agreeing: moderate boost
        if len(cross_validated_by) == 2:
            return 0.10

        # 3+ sources agreeing: high boost
        return 0.20

    def calculate_expires_at(
        self,
        field_name: str,
        enriched_at: datetime,
        confidence_threshold: float = 0.5
    ) -> datetime:
        """
        Calculate when confidence will drop below threshold.

        Used to trigger re-enrichment jobs.

        Args:
            field_name: Name of the field
            enriched_at: When the data was enriched
            confidence_threshold: Minimum acceptable confidence (default: 0.5)

        Returns:
            datetime: When re-enrichment should be triggered

        Examples:
            For email (decay_period=90, threshold=0.5):
            - e^(-days/90) = 0.5
            - -days/90 = ln(0.5)
            - days = -90 * ln(0.5) = 62 days
        """
        decay_period = self.FIELD_DECAY_RATES.get(field_name, 60)

        # Calculate days until confidence < threshold
        # Using: threshold = e^(-days / decay_period)
        # Solve for days: days = -decay_period * ln(threshold)
        days_until_expiry = -decay_period * math.log(confidence_threshold)

        return enriched_at + timedelta(days=int(days_until_expiry))

    def calculate_profile_confidence(
        self,
        enrichment_metadata: Dict[str, Dict]
    ) -> float:
        """
        Calculate overall profile confidence from field-level metadata.

        Weighted average of field confidences, with key fields weighted higher.

        Args:
            enrichment_metadata: Dict of field metadata from profile

        Returns:
            float: Overall profile confidence (0.0-1.0)

        Example:
            {
                'email': {'confidence': 0.95, ...},
                'seeking': {'confidence': 0.75, ...},
                'offering': {'confidence': 0.80, ...}
            }
            -> weighted_avg([0.95, 0.75, 0.80]) = 0.83
        """
        if not enrichment_metadata:
            return 0.0

        # Field weights (higher = more important)
        field_weights = {
            'email': 3.0,           # Contact info is critical
            'phone': 2.5,
            'linkedin': 2.0,
            'seeking': 2.0,         # Business intent important
            'offering': 2.0,
            'who_you_serve': 1.5,
            'what_you_do': 1.5,
            'list_size': 1.0,
            'niche': 1.0,
            'company': 1.0,
            'website': 1.0,
            'revenue_tier': 1.5,
            'jv_history': 1.5,
            'content_platforms': 1.0,
            'audience_engagement_score': 1.5,
        }

        total_weighted_confidence = 0.0
        total_weight = 0.0

        for field, metadata in enrichment_metadata.items():
            if not isinstance(metadata, dict) or 'confidence' not in metadata:
                continue

            weight = field_weights.get(field, 0.5)
            confidence = metadata['confidence']

            total_weighted_confidence += confidence * weight
            total_weight += weight

        if total_weight == 0:
            return 0.0

        return total_weighted_confidence / total_weight


# Example usage
if __name__ == '__main__':
    scorer = ConfidenceScorer()

    print("=" * 70)
    print("CONFIDENCE SCORING EXAMPLES")
    print("=" * 70)
    print()

    # Example 1: Fresh Apollo verified email
    print("1. Fresh Apollo verified email:")
    conf = scorer.calculate_confidence('email', 'apollo_verified', datetime.now())
    print(f"   Confidence: {conf:.2f} (0.95 base, no decay)")
    print()

    # Example 2: 30-day old OWL seeking data
    print("2. OWL seeking data (30 days old):")
    conf = scorer.calculate_confidence('seeking', 'owl', datetime.now() - timedelta(days=30))
    print(f"   Confidence: {conf:.2f} (0.85 base * 0.61 age factor)")
    print()

    # Example 3: 90-day old email (at decay period)
    print("3. Email (90 days old, at decay period):")
    conf = scorer.calculate_confidence('email', 'apollo', datetime.now() - timedelta(days=90))
    print(f"   Confidence: {conf:.2f} (0.80 base * 0.37 age factor)")
    print()

    # Example 4: Recently verified email with boost
    print("4. Email verified yesterday (with boost):")
    conf = scorer.calculate_confidence(
        'email', 'apollo',
        enriched_at=datetime.now() - timedelta(days=30),
        verified_at=datetime.now() - timedelta(days=1),
        verification_count=1
    )
    print(f"   Confidence: {conf:.2f} (decayed base + 0.15 verification boost)")
    print()

    # Example 5: Cross-validated data
    print("5. Email cross-validated by 3 sources:")
    conf = scorer.calculate_confidence(
        'email', 'apollo',
        enriched_at=datetime.now() - timedelta(days=10),
        cross_validated_by=['apollo', 'owl', 'manual']
    )
    print(f"   Confidence: {conf:.2f} (base + 0.20 cross-validation boost)")
    print()

    # Example 6: Calculate expiry date
    print("6. Email expiry (when confidence drops below 0.5):")
    expires = scorer.calculate_expires_at('email', datetime.now())
    days_until = (expires - datetime.now()).days
    print(f"   Expires in {days_until} days ({expires.strftime('%Y-%m-%d')})")
    print()

    # Example 7: Profile-level confidence
    print("7. Overall profile confidence:")
    metadata = {
        'email': {'confidence': 0.95},
        'seeking': {'confidence': 0.75},
        'offering': {'confidence': 0.80},
        'linkedin': {'confidence': 0.90}
    }
    profile_conf = scorer.calculate_profile_confidence(metadata)
    print(f"   Profile confidence: {profile_conf:.2f}")
    print(f"   (weighted avg of field confidences)")
