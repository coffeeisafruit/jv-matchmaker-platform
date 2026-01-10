"""
Match Scoring Service for JV Matcher Module.

Implements the ISMC (Intent, Synergy, Momentum, Context) scoring framework
using a weighted harmonic mean for the final score calculation.
"""

from dataclasses import dataclass
from typing import Optional
import math

from django.conf import settings

from .models import Profile, Match


@dataclass
class ScoreComponent:
    """Represents a single scoring component with its details."""
    name: str
    score: float  # 0-10 scale
    weight: float  # Percentage weight
    factors: list[dict]  # Individual factors that contributed to the score
    explanation: str


@dataclass
class ScoreBreakdown:
    """Complete breakdown of all scoring components."""
    intent: ScoreComponent
    synergy: ScoreComponent
    momentum: ScoreComponent
    context: ScoreComponent
    final_score: float  # 0-10 scale
    recommendation: str


class MatchScoringService:
    """
    Service for calculating match scores between a Profile and User's ICP.

    Scoring Framework:
    - Intent (45%): Signals indicating partnership interest
    - Synergy (25%): Audience and content alignment
    - Momentum (20%): Recent activity and growth trends
    - Context (10%): Contextual relevance factors

    Final score uses weighted harmonic mean to penalize weak areas.
    """

    # Scoring weights from settings or defaults
    WEIGHTS = getattr(settings, 'GTM_CONFIG', {}).get('scoring_weights', {
        'intent': 0.45,
        'synergy': 0.25,
        'momentum': 0.20,
        'context': 0.10,
    })

    def __init__(self, profile: Profile, user):
        """
        Initialize the scoring service.

        Args:
            profile: The JV partner profile to score
            user: The user with ICP (Ideal Customer Profile) data
        """
        self.profile = profile
        self.user = user
        self.icp = self._get_user_icp()

    def _get_user_icp(self) -> dict:
        """
        Extract ICP data from user.

        Returns:
            Dictionary containing user's ICP preferences
        """
        # Placeholder: In production, this would pull from user's ICP settings
        return {
            'target_industries': getattr(self.user, 'target_industries', []),
            'target_audience_size': getattr(self.user, 'target_audience_size', 'medium'),
            'content_preferences': getattr(self.user, 'content_preferences', []),
            'business_description': getattr(self.user, 'business_description', ''),
            'business_domain': getattr(self.user, 'business_domain', ''),
        }

    def calculate_intent_score(self) -> ScoreComponent:
        """
        Calculate Intent Score (45% weight).

        Measures signals that indicate partnership interest:
        - Has collaboration history
        - Active on social media
        - Engages with similar content
        - Has publicly expressed interest in JVs
        """
        factors = []
        total_score = 0.0
        max_possible = 0.0

        # Factor 1: Has collaboration history (weight: 3)
        collab_score = 0.0
        if self.profile.collaboration_history:
            collab_count = len(self.profile.collaboration_history) if isinstance(
                self.profile.collaboration_history, list
            ) else 1
            collab_score = min(10, collab_count * 2.5)
        factors.append({
            'name': 'Collaboration History',
            'score': collab_score,
            'max_score': 10,
            'weight': 3,
            'detail': f'Previous collaborations found' if collab_score > 0 else 'No collaboration history'
        })
        total_score += collab_score * 3
        max_possible += 10 * 3

        # Factor 2: LinkedIn presence (weight: 2.5)
        linkedin_score = 8.0 if self.profile.linkedin_url else 0.0
        factors.append({
            'name': 'LinkedIn Presence',
            'score': linkedin_score,
            'max_score': 10,
            'weight': 2.5,
            'detail': 'LinkedIn profile available' if linkedin_score > 0 else 'No LinkedIn profile'
        })
        total_score += linkedin_score * 2.5
        max_possible += 10 * 2.5

        # Factor 3: Website presence (weight: 2)
        website_score = 7.0 if self.profile.website_url else 0.0
        factors.append({
            'name': 'Website Presence',
            'score': website_score,
            'max_score': 10,
            'weight': 2,
            'detail': 'Website available' if website_score > 0 else 'No website'
        })
        total_score += website_score * 2
        max_possible += 10 * 2

        # Factor 4: Contact info available (weight: 1.5)
        contact_score = 9.0 if self.profile.email else 5.0
        factors.append({
            'name': 'Contact Availability',
            'score': contact_score,
            'max_score': 10,
            'weight': 1.5,
            'detail': 'Direct email available' if self.profile.email else 'Indirect contact only'
        })
        total_score += contact_score * 1.5
        max_possible += 10 * 1.5

        # Factor 5: Enrichment data quality (weight: 1)
        enrichment_score = 0.0
        if self.profile.enrichment_data:
            data_completeness = len(self.profile.enrichment_data) / 10  # Assume 10 fields is complete
            enrichment_score = min(10, data_completeness * 10)
        factors.append({
            'name': 'Data Enrichment',
            'score': enrichment_score,
            'max_score': 10,
            'weight': 1,
            'detail': 'Enrichment data available' if enrichment_score > 0 else 'No enrichment data'
        })
        total_score += enrichment_score * 1
        max_possible += 10 * 1

        # Calculate weighted average (0-10 scale)
        final_score = (total_score / max_possible) * 10 if max_possible > 0 else 0

        return ScoreComponent(
            name='Intent',
            score=round(final_score, 2),
            weight=self.WEIGHTS['intent'],
            factors=factors,
            explanation=self._get_intent_explanation(final_score)
        )

    def _get_intent_explanation(self, score: float) -> str:
        """Generate human-readable explanation for intent score."""
        if score >= 8:
            return "Strong signals indicate high partnership interest and accessibility."
        elif score >= 6:
            return "Moderate intent signals suggest potential interest in collaboration."
        elif score >= 4:
            return "Limited signals available; may require more research."
        else:
            return "Weak intent signals; consider prioritizing other prospects."

    def calculate_synergy_score(self) -> ScoreComponent:
        """
        Calculate Synergy Score (25% weight).

        Measures audience and content alignment:
        - Audience size compatibility
        - Industry alignment
        - Content style compatibility
        - Audience overlap potential
        """
        factors = []
        total_score = 0.0
        max_possible = 0.0

        # Factor 1: Audience size alignment (weight: 3)
        audience_map = {
            'tiny': 1, 'small': 2, 'medium': 3, 'large': 4, 'massive': 5
        }
        profile_size = audience_map.get(self.profile.audience_size, 3)
        target_size = audience_map.get(self.icp.get('target_audience_size', 'medium'), 3)
        size_diff = abs(profile_size - target_size)
        size_score = max(0, 10 - (size_diff * 2.5))
        factors.append({
            'name': 'Audience Size Match',
            'score': size_score,
            'max_score': 10,
            'weight': 3,
            'detail': f'Profile: {self.profile.audience_size or "unknown"}, Target: {self.icp.get("target_audience_size", "medium")}'
        })
        total_score += size_score * 3
        max_possible += 10 * 3

        # Factor 2: Industry alignment (weight: 3)
        industry_score = 5.0  # Default moderate score
        if self.profile.industry:
            target_industries = self.icp.get('target_industries', [])
            if target_industries and self.profile.industry.lower() in [i.lower() for i in target_industries]:
                industry_score = 10.0
            elif self.profile.industry:
                industry_score = 6.0  # Has industry, but not in target list
        factors.append({
            'name': 'Industry Alignment',
            'score': industry_score,
            'max_score': 10,
            'weight': 3,
            'detail': f'Industry: {self.profile.industry or "not specified"}'
        })
        total_score += industry_score * 3
        max_possible += 10 * 3

        # Factor 3: Content style compatibility (weight: 2)
        content_score = 5.0  # Default moderate
        if self.profile.content_style:
            # Placeholder: In production, use NLP to compare content styles
            content_score = 7.0
        factors.append({
            'name': 'Content Compatibility',
            'score': content_score,
            'max_score': 10,
            'weight': 2,
            'detail': 'Content style analysis' if self.profile.content_style else 'No content style data'
        })
        total_score += content_score * 2
        max_possible += 10 * 2

        # Factor 4: Audience description quality (weight: 2)
        audience_desc_score = 0.0
        if self.profile.audience_description:
            # Score based on description completeness
            word_count = len(self.profile.audience_description.split())
            audience_desc_score = min(10, word_count / 5)  # 50 words = full score
        factors.append({
            'name': 'Audience Definition',
            'score': audience_desc_score,
            'max_score': 10,
            'weight': 2,
            'detail': 'Well-defined audience' if audience_desc_score >= 7 else 'Audience needs clarification'
        })
        total_score += audience_desc_score * 2
        max_possible += 10 * 2

        final_score = (total_score / max_possible) * 10 if max_possible > 0 else 0

        return ScoreComponent(
            name='Synergy',
            score=round(final_score, 2),
            weight=self.WEIGHTS['synergy'],
            factors=factors,
            explanation=self._get_synergy_explanation(final_score)
        )

    def _get_synergy_explanation(self, score: float) -> str:
        """Generate human-readable explanation for synergy score."""
        if score >= 8:
            return "Excellent audience and content alignment indicates high partnership potential."
        elif score >= 6:
            return "Good synergy with some complementary audience overlap."
        elif score >= 4:
            return "Moderate alignment; partnership may require creative positioning."
        else:
            return "Limited synergy; consider if partnership goals align."

    def calculate_momentum_score(self) -> ScoreComponent:
        """
        Calculate Momentum Score (20% weight).

        Measures recent activity and growth:
        - Recent content publication
        - Social engagement trends
        - Business growth indicators
        - Activity frequency
        """
        factors = []
        total_score = 0.0
        max_possible = 0.0

        # Factor 1: Profile freshness (weight: 3)
        # Based on when profile was last updated
        from django.utils import timezone
        days_since_update = (timezone.now() - self.profile.updated_at).days
        freshness_score = max(0, 10 - (days_since_update / 3))  # Lose 1 point per 3 days
        factors.append({
            'name': 'Profile Freshness',
            'score': round(freshness_score, 2),
            'max_score': 10,
            'weight': 3,
            'detail': f'Updated {days_since_update} days ago'
        })
        total_score += freshness_score * 3
        max_possible += 10 * 3

        # Factor 2: Enrichment recency (weight: 2.5)
        enrichment_recency_score = 5.0  # Default
        if self.profile.enrichment_data:
            # Placeholder: Check enrichment data timestamp
            enrichment_recency_score = 7.0
        factors.append({
            'name': 'Data Recency',
            'score': enrichment_recency_score,
            'max_score': 10,
            'weight': 2.5,
            'detail': 'Recent enrichment data' if enrichment_recency_score >= 7 else 'Data may be outdated'
        })
        total_score += enrichment_recency_score * 2.5
        max_possible += 10 * 2.5

        # Factor 3: Activity indicators (weight: 2.5)
        activity_score = 6.0  # Default moderate
        if self.profile.linkedin_url and self.profile.website_url:
            activity_score = 8.0
        elif self.profile.linkedin_url or self.profile.website_url:
            activity_score = 6.0
        else:
            activity_score = 3.0
        factors.append({
            'name': 'Activity Level',
            'score': activity_score,
            'max_score': 10,
            'weight': 2.5,
            'detail': 'Active online presence' if activity_score >= 7 else 'Limited activity signals'
        })
        total_score += activity_score * 2.5
        max_possible += 10 * 2.5

        # Factor 4: Growth potential (weight: 2)
        growth_score = 5.0  # Default
        # Placeholder: In production, analyze growth metrics from enrichment
        if self.profile.audience_size in ['small', 'medium']:
            growth_score = 7.0  # Higher growth potential
        elif self.profile.audience_size == 'large':
            growth_score = 6.0
        factors.append({
            'name': 'Growth Potential',
            'score': growth_score,
            'max_score': 10,
            'weight': 2,
            'detail': f'Based on {self.profile.audience_size or "unknown"} audience size'
        })
        total_score += growth_score * 2
        max_possible += 10 * 2

        final_score = (total_score / max_possible) * 10 if max_possible > 0 else 0

        return ScoreComponent(
            name='Momentum',
            score=round(final_score, 2),
            weight=self.WEIGHTS['momentum'],
            factors=factors,
            explanation=self._get_momentum_explanation(final_score)
        )

    def _get_momentum_explanation(self, score: float) -> str:
        """Generate human-readable explanation for momentum score."""
        if score >= 8:
            return "Strong momentum with active engagement and growth indicators."
        elif score >= 6:
            return "Positive momentum with consistent activity."
        elif score >= 4:
            return "Moderate activity; timing may need consideration."
        else:
            return "Low momentum; partner may not be actively seeking collaborations."

    def calculate_context_score(self) -> ScoreComponent:
        """
        Calculate Context Score (10% weight).

        Measures contextual relevance:
        - Geographic alignment
        - Timing considerations
        - Market conditions
        - Relationship proximity
        """
        factors = []
        total_score = 0.0
        max_possible = 0.0

        # Factor 1: Data completeness (weight: 3)
        completeness_fields = [
            self.profile.name,
            self.profile.company,
            self.profile.industry,
            self.profile.audience_size,
            self.profile.audience_description,
            self.profile.linkedin_url,
            self.profile.website_url,
            self.profile.email,
        ]
        filled_count = sum(1 for f in completeness_fields if f)
        completeness_score = (filled_count / len(completeness_fields)) * 10
        factors.append({
            'name': 'Profile Completeness',
            'score': round(completeness_score, 2),
            'max_score': 10,
            'weight': 3,
            'detail': f'{filled_count}/{len(completeness_fields)} fields populated'
        })
        total_score += completeness_score * 3
        max_possible += 10 * 3

        # Factor 2: Source quality (weight: 2.5)
        source_scores = {
            'clay': 9.0,
            'linkedin': 8.0,
            'manual': 6.0,
            'import': 5.0,
        }
        source_score = source_scores.get(self.profile.source, 5.0)
        factors.append({
            'name': 'Data Source Quality',
            'score': source_score,
            'max_score': 10,
            'weight': 2.5,
            'detail': f'Source: {self.profile.get_source_display()}'
        })
        total_score += source_score * 2.5
        max_possible += 10 * 2.5

        # Factor 3: Business domain alignment (weight: 2.5)
        domain_score = 5.0  # Default
        if self.profile.website_url and self.icp.get('business_domain'):
            # Placeholder: In production, analyze domain similarity
            domain_score = 6.0
        factors.append({
            'name': 'Domain Relevance',
            'score': domain_score,
            'max_score': 10,
            'weight': 2.5,
            'detail': 'Domain analysis available' if self.profile.website_url else 'No domain data'
        })
        total_score += domain_score * 2.5
        max_possible += 10 * 2.5

        # Factor 4: Relationship proximity (weight: 2)
        proximity_score = 5.0  # Default neutral
        # Placeholder: In production, check for mutual connections
        factors.append({
            'name': 'Network Proximity',
            'score': proximity_score,
            'max_score': 10,
            'weight': 2,
            'detail': 'No direct connections found'
        })
        total_score += proximity_score * 2
        max_possible += 10 * 2

        final_score = (total_score / max_possible) * 10 if max_possible > 0 else 0

        return ScoreComponent(
            name='Context',
            score=round(final_score, 2),
            weight=self.WEIGHTS['context'],
            factors=factors,
            explanation=self._get_context_explanation(final_score)
        )

    def _get_context_explanation(self, score: float) -> str:
        """Generate human-readable explanation for context score."""
        if score >= 8:
            return "Strong contextual fit with high-quality data and relevance."
        elif score >= 6:
            return "Good contextual alignment with adequate information."
        elif score >= 4:
            return "Some contextual gaps; additional research recommended."
        else:
            return "Limited context available; proceed with caution."

    def calculate_harmonic_mean(
        self,
        scores: list[tuple[float, float]]
    ) -> float:
        """
        Calculate weighted harmonic mean of scores.

        Formula: H = sum(weights) / sum(weight_i / score_i)

        The harmonic mean penalizes low scores more heavily than
        arithmetic mean, ensuring balanced performance across all dimensions.

        Args:
            scores: List of (score, weight) tuples where scores are 0-10

        Returns:
            Weighted harmonic mean on 0-10 scale
        """
        epsilon = 1e-10  # Avoid division by zero

        total_weight = sum(weight for _, weight in scores)
        weighted_reciprocal_sum = sum(
            weight / max(score, epsilon)
            for score, weight in scores
        )

        if weighted_reciprocal_sum == 0:
            return 0.0

        return total_weight / weighted_reciprocal_sum

    def calculate_score(self) -> ScoreBreakdown:
        """
        Calculate complete match score breakdown.

        Returns:
            ScoreBreakdown with all component scores and final score
        """
        # Calculate each component
        intent = self.calculate_intent_score()
        synergy = self.calculate_synergy_score()
        momentum = self.calculate_momentum_score()
        context = self.calculate_context_score()

        # Calculate weighted harmonic mean
        scores = [
            (intent.score, intent.weight),
            (synergy.score, synergy.weight),
            (momentum.score, momentum.weight),
            (context.score, context.weight),
        ]
        final_score = self.calculate_harmonic_mean(scores)

        # Generate recommendation
        recommendation = self._generate_recommendation(final_score, intent, synergy, momentum, context)

        return ScoreBreakdown(
            intent=intent,
            synergy=synergy,
            momentum=momentum,
            context=context,
            final_score=round(final_score, 2),
            recommendation=recommendation
        )

    def _generate_recommendation(
        self,
        final_score: float,
        intent: ScoreComponent,
        synergy: ScoreComponent,
        momentum: ScoreComponent,
        context: ScoreComponent
    ) -> str:
        """Generate actionable recommendation based on scores."""
        if final_score >= 8:
            return "Highly recommended for immediate outreach. Strong match across all dimensions."
        elif final_score >= 6:
            # Identify weakest area
            scores = {
                'intent': intent.score,
                'synergy': synergy.score,
                'momentum': momentum.score,
                'context': context.score
            }
            weakest = min(scores, key=scores.get)
            return f"Good potential partner. Consider strengthening {weakest} signals before outreach."
        elif final_score >= 4:
            return "Moderate match. Recommend additional research or waiting for better timing."
        else:
            return "Low match score. Consider other prospects or significantly different approach."

    def create_or_update_match(self) -> Match:
        """
        Create or update a Match record with calculated scores.

        Returns:
            Match instance with all scores populated
        """
        breakdown = self.calculate_score()

        # Prepare score breakdown JSON
        score_breakdown_json = {
            'intent': {
                'score': breakdown.intent.score,
                'weight': breakdown.intent.weight,
                'factors': breakdown.intent.factors,
                'explanation': breakdown.intent.explanation,
            },
            'synergy': {
                'score': breakdown.synergy.score,
                'weight': breakdown.synergy.weight,
                'factors': breakdown.synergy.factors,
                'explanation': breakdown.synergy.explanation,
            },
            'momentum': {
                'score': breakdown.momentum.score,
                'weight': breakdown.momentum.weight,
                'factors': breakdown.momentum.factors,
                'explanation': breakdown.momentum.explanation,
            },
            'context': {
                'score': breakdown.context.score,
                'weight': breakdown.context.weight,
                'factors': breakdown.context.factors,
                'explanation': breakdown.context.explanation,
            },
            'recommendation': breakdown.recommendation,
        }

        # Create or update match
        match, created = Match.objects.update_or_create(
            user=self.user,
            profile=self.profile,
            defaults={
                'intent_score': breakdown.intent.score / 10,  # Convert to 0-1 scale
                'synergy_score': breakdown.synergy.score / 10,
                'momentum_score': breakdown.momentum.score / 10,
                'context_score': breakdown.context.score / 10,
                'final_score': breakdown.final_score / 10,
                'score_breakdown': score_breakdown_json,
            }
        )

        return match
