"""
Match Scoring Service for JV Matcher Module.

Implements the ISMC (Intent, Synergy, Momentum, Context) scoring framework
using a weighted geometric mean for the final score calculation.

Also includes PartnershipAnalyzer for dynamic partnership insights that
integrates ICP and Transformation data with pre-computed SupabaseMatch data.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import logging
import math
import re

from django.conf import settings

from .models import Profile, Match, SupabaseProfile, SupabaseMatch
from .enrichment.text_sanitizer import TextSanitizer

logger = logging.getLogger('matching.services')


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


# =============================================================================
# PARTNERSHIP ANALYZER - Dynamic insights for SupabaseProfile partners
# =============================================================================

@dataclass
class PartnershipInsight:
    """A single insight about why a partnership could work."""
    type: str  # 'seeking_offering', 'audience_overlap', 'solution_fit', 'scale_match'
    icon: str  # CSS class or emoji for display
    headline: str  # Short summary
    detail: str  # Explanation
    action: Optional[str] = None  # Suggested action


@dataclass
class PartnershipAnalysis:
    """Complete analysis of a partner with dynamic insights."""
    partner: SupabaseProfile
    tier: str  # 'hand_picked', 'strong', 'wildcard'
    score: Optional[float]  # harmonic_mean from SupabaseMatch if available
    insights: List[PartnershipInsight] = field(default_factory=list)
    suggested_action: Optional[str] = None
    conversation_starter: Optional[str] = None


class PartnershipAnalyzer:
    """
    Analyzes partnerships at display time, combining:
    - Pre-computed SupabaseMatch data (seeking→offering, harmonic_mean)
    - User's ICP (audience overlap)
    - User's Transformation (solution fit)
    - Scale compatibility (list sizes)

    Generates dynamic "Why You Two" narratives and suggested actions.
    """

    # Tier thresholds based on harmonic_mean (0-100 scale)
    # Calibrated for ISMC distribution: mean ~57.5, stdev ~5.7, range 25-78
    TIER_THRESHOLDS = {
        'hand_picked': 67,  # ~Top 5%, no weak dimensions (geometric mean enforces this)
        'strong': 55,       # Above mean, solid signal across most dimensions
        'wildcard': 0,      # Below average, algorithm doesn't see strong fit
    }

    def __init__(
        self,
        user,
        user_supabase_profile: Optional[SupabaseProfile] = None,
        icp=None,  # positioning.models.ICP
        transformation=None,  # positioning.models.TransformationAnalysis
    ):
        """
        Initialize analyzer with user context.

        Args:
            user: Django User instance
            user_supabase_profile: User's own SupabaseProfile (if linked)
            icp: User's primary ICP (for audience matching)
            transformation: User's transformation analysis (for solution fit)
        """
        self.user = user
        self.user_profile = user_supabase_profile
        self.icp = icp
        self.transformation = transformation

    def analyze(
        self,
        partner: SupabaseProfile,
        supabase_match: Optional[SupabaseMatch] = None
    ) -> PartnershipAnalysis:
        """
        Analyze a partner and generate dynamic insights.

        Args:
            partner: The SupabaseProfile to analyze
            supabase_match: Pre-computed match data (if available)

        Returns:
            PartnershipAnalysis with tier, insights, and suggested action
        """
        insights = []
        score = None

        # Dimension 1: Seeking→Offering (from pre-computed SupabaseMatch)
        if supabase_match and supabase_match.harmonic_mean:
            score = float(supabase_match.harmonic_mean)
            seeking_insight = self._build_seeking_offering_insight(
                partner, supabase_match
            )
            if seeking_insight:
                insights.append(seeking_insight)

        # Dimension 2: Audience Overlap (ICP integration)
        if self.icp:
            audience_insight = self._build_audience_insight(partner)
            if audience_insight:
                insights.append(audience_insight)

        # Dimension 3: Solution Fit (Transformation integration)
        if self.transformation:
            solution_insight = self._build_solution_insight(partner)
            if solution_insight:
                insights.append(solution_insight)

        # Dimension 4: Scale Compatibility
        scale_insight = self._build_scale_insight(partner)
        if scale_insight:
            insights.append(scale_insight)

        # Dimension 5: Revenue Tier Compatibility
        revenue_insight = self._build_revenue_tier_insight(partner)
        if revenue_insight:
            insights.append(revenue_insight)

        # Dimension 6: JV History (experienced partner signal)
        jv_insight = self._build_jv_history_insight(partner)
        if jv_insight:
            insights.append(jv_insight)

        # Dimension 7: Content Platform Overlap
        platform_insight = self._build_content_platform_insight(partner)
        if platform_insight:
            insights.append(platform_insight)

        # Determine tier
        tier = self._determine_tier(score, len(insights))

        # Generate suggested action
        suggested_action = self._generate_suggested_action(insights, tier)

        # Generate conversation starter
        conversation_starter = self._generate_conversation_starter(
            partner, insights
        )

        return PartnershipAnalysis(
            partner=partner,
            tier=tier,
            score=score,
            insights=insights,
            suggested_action=suggested_action,
            conversation_starter=conversation_starter,
        )

    def _build_seeking_offering_insight(
        self,
        partner: SupabaseProfile,
        match: SupabaseMatch
    ) -> Optional[PartnershipInsight]:
        """Build insight from seeking→offering alignment."""
        if not match.match_reason:
            # Generate from raw data if no pre-computed reason
            if self.user_profile and self.user_profile.seeking and partner.offering:
                return PartnershipInsight(
                    type='seeking_offering',
                    icon='exchange',
                    headline='Service Match',
                    detail=f"You're seeking help with your goals — they offer relevant services",
                    action='Explore their offering'
                )
            return None

        # Use pre-computed match_reason from jv-matcher
        return PartnershipInsight(
            type='seeking_offering',
            icon='exchange',
            headline='Complementary Skills',
            detail=match.match_reason[:150] if match.match_reason else 'Skills alignment detected',
            action='Request an introduction'
        )

    def _build_audience_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from ICP audience overlap."""
        if not self.icp or not self.icp.industry:
            return None

        icp_industry = self.icp.industry.lower()

        # Check partner's who_you_serve
        partner_serves = (partner.who_you_serve or '').lower()
        partner_niche = (partner.niche or '').lower()

        # Simple keyword matching (could be enhanced with NLP)
        has_overlap = (
            icp_industry in partner_serves or
            icp_industry in partner_niche or
            any(word in partner_serves for word in icp_industry.split()) or
            any(word in partner_niche for word in icp_industry.split())
        )

        if has_overlap:
            return PartnershipInsight(
                type='audience_overlap',
                icon='users',
                headline='Shared Audience',
                detail=f"You both serve {self.icp.industry} — ideal for cross-promotion",
                action='Propose a list swap or co-promotion'
            )

        return None

    def _build_solution_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from transformation/solution fit."""
        if not self.transformation or not partner.offering:
            return None

        # Check if partner's offering addresses ICP pain points
        partner_offering = (partner.offering or '').lower()
        pain_points = self.icp.pain_points if self.icp else []

        if not pain_points:
            return None

        # Check for overlap between offering and pain points
        for pain in pain_points[:3]:  # Check top 3 pain points
            pain_lower = pain.lower() if isinstance(pain, str) else ''
            if any(word in partner_offering for word in pain_lower.split() if len(word) > 3):
                return PartnershipInsight(
                    type='solution_fit',
                    icon='lightbulb',
                    headline='Solution Provider',
                    detail=f"Their offering may help your customers with: {pain[:50]}",
                    action='Explore affiliate or referral partnership'
                )

        return None

    def _build_scale_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from list size compatibility."""
        if not self.user_profile:
            return None

        user_list = self.user_profile.list_size or 0
        partner_list = partner.list_size or 0

        if user_list == 0 or partner_list == 0:
            return None

        # Calculate ratio
        if user_list > partner_list:
            ratio = user_list / partner_list if partner_list > 0 else float('inf')
        else:
            ratio = partner_list / user_list if user_list > 0 else float('inf')

        if ratio <= 2:
            # Within 2x = good for list swap
            return PartnershipInsight(
                type='scale_match',
                icon='balance',
                headline='Compatible Scale',
                detail=f"Similar list sizes (~{self._format_number(partner_list)}) — fair exchange potential",
                action='Propose an equal list swap'
            )
        elif ratio <= 5:
            # 2-5x difference
            if partner_list > user_list:
                return PartnershipInsight(
                    type='scale_match',
                    icon='trending-up',
                    headline='Growth Opportunity',
                    detail=f"Their larger audience ({self._format_number(partner_list)}) could accelerate your reach",
                    action='Offer value beyond list size (expertise, content)'
                )
            else:
                return PartnershipInsight(
                    type='scale_match',
                    icon='gift',
                    headline='Mentor Opportunity',
                    detail=f"Your larger audience could help them grow",
                    action='Consider a mentorship or promotional deal'
                )

        return None

    # Revenue tier ordering for compatibility comparison
    REVENUE_TIER_ORDER = {
        'micro': 0,
        'emerging': 1,
        'established': 2,
        'premium': 3,
        'enterprise': 4,
    }

    def _build_revenue_tier_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from revenue tier compatibility.

        Same or adjacent tiers indicate audiences that spend similarly,
        making cross-promotions more effective.
        """
        if not self.user_profile:
            return None

        user_tier = getattr(self.user_profile, 'revenue_tier', None)
        partner_tier = getattr(partner, 'revenue_tier', None)

        if not user_tier or not partner_tier:
            return None

        user_rank = self.REVENUE_TIER_ORDER.get(user_tier)
        partner_rank = self.REVENUE_TIER_ORDER.get(partner_tier)

        if user_rank is None or partner_rank is None:
            return None

        diff = abs(user_rank - partner_rank)

        if diff == 0:
            return PartnershipInsight(
                type='revenue_alignment',
                icon='dollar-sign',
                headline='Revenue Tier Match',
                detail=f"Both operate at the {partner_tier} tier — audiences spend similarly",
                action='Propose a co-promotion to each other\'s buyers'
            )
        elif diff == 1:
            return PartnershipInsight(
                type='revenue_alignment',
                icon='dollar-sign',
                headline='Adjacent Revenue Tiers',
                detail=f"Your {user_tier} tier pairs well with their {partner_tier} tier for upsell/downsell",
                action='Explore a referral funnel between your offers'
            )

        # 2+ tiers apart — no insight (not necessarily bad, just not a signal)
        return None

    def _build_jv_history_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from partner's past JV experience.

        Partners with JV history are lower-risk — they understand the format
        and are more likely to follow through.
        """
        jv_history = getattr(partner, 'jv_history', None)

        if not jv_history or not isinstance(jv_history, list):
            return None

        jv_count = len(jv_history)

        if jv_count == 0:
            return None

        # Extract JV formats for detail
        formats = set()
        for jv in jv_history[:5]:
            if isinstance(jv, dict) and jv.get('format'):
                formats.add(jv['format'].replace('_', ' '))

        format_str = ', '.join(sorted(formats)[:3]) if formats else 'partnerships'

        if jv_count >= 3:
            return PartnershipInsight(
                type='jv_experience',
                icon='award',
                headline='Experienced JV Partner',
                detail=f"{jv_count} past partnerships ({format_str}) — proven collaborator",
                action='Reference their JV experience when reaching out'
            )
        else:
            return PartnershipInsight(
                type='jv_experience',
                icon='handshake',
                headline='Has JV Experience',
                detail=f"{jv_count} past partnership(s) ({format_str})",
                action='Ask about their partnership experience'
            )

    def _build_content_platform_insight(
        self,
        partner: SupabaseProfile
    ) -> Optional[PartnershipInsight]:
        """Build insight from content platform overlap.

        Shared platforms mean easier cross-promotion (e.g., podcast guest
        swaps, Instagram collaborations, YouTube features).
        """
        if not self.user_profile:
            return None

        user_platforms = getattr(self.user_profile, 'content_platforms', None)
        partner_platforms = getattr(partner, 'content_platforms', None)

        if not user_platforms or not isinstance(user_platforms, dict):
            return None
        if not partner_platforms or not isinstance(partner_platforms, dict):
            return None

        # Find platforms both have (non-empty values)
        shared = []
        platform_labels = {
            'podcast_name': 'Podcast',
            'youtube_channel': 'YouTube',
            'instagram_handle': 'Instagram',
            'facebook_group': 'Facebook Group',
            'tiktok_handle': 'TikTok',
            'newsletter_name': 'Newsletter',
        }

        for key, label in platform_labels.items():
            user_val = user_platforms.get(key)
            partner_val = partner_platforms.get(key)
            if user_val and partner_val:
                shared.append(label)

        if not shared:
            return None

        if len(shared) >= 2:
            platforms_str = ', '.join(shared[:3])
            return PartnershipInsight(
                type='platform_overlap',
                icon='share-2',
                headline='Multi-Platform Overlap',
                detail=f"You're both active on {platforms_str} — multiple cross-promo channels",
                action='Propose a multi-platform collaboration'
            )
        else:
            platform = shared[0]
            if platform == 'Podcast':
                return PartnershipInsight(
                    type='platform_overlap',
                    icon='mic',
                    headline='Podcast Swap Opportunity',
                    detail="You both have podcasts — guest swap is a quick win",
                    action='Propose a mutual podcast guest appearance'
                )
            elif platform == 'YouTube':
                return PartnershipInsight(
                    type='platform_overlap',
                    icon='video',
                    headline='YouTube Collaboration',
                    detail="You're both on YouTube — feature or collab opportunity",
                    action='Propose a joint video or feature'
                )
            else:
                return PartnershipInsight(
                    type='platform_overlap',
                    icon='share-2',
                    headline=f'{platform} Overlap',
                    detail=f"You're both active on {platform}",
                    action=f'Explore cross-promotion on {platform}'
                )

    def _determine_tier(
        self,
        score: Optional[float],
        insight_count: int
    ) -> str:
        """Determine confidence tier based on score and insights."""
        if score is not None:
            if score >= self.TIER_THRESHOLDS['hand_picked']:
                return 'hand_picked'
            elif score >= self.TIER_THRESHOLDS['strong']:
                return 'strong'

        # If no score but multiple insights, still consider it valuable
        if insight_count >= 3:
            return 'strong'
        elif insight_count >= 1:
            return 'wildcard'

        return 'wildcard'

    def _generate_suggested_action(
        self,
        insights: List[PartnershipInsight],
        tier: str
    ) -> str:
        """Generate a suggested next action based on insights."""
        if not insights:
            return "Explore their profile to find common ground"

        # Combine actions from insights
        actions = [i.action for i in insights if i.action]

        if tier == 'hand_picked':
            if 'audience_overlap' in [i.type for i in insights]:
                return "High-priority: Propose a co-marketing campaign or list swap"
            return "High-priority: Request an introduction call"

        if 'seeking_offering' in [i.type for i in insights]:
            return "Reach out to discuss how you can help each other"

        if 'audience_overlap' in [i.type for i in insights]:
            return "Explore cross-promotion opportunities"

        return actions[0] if actions else "Review their profile and find alignment"

    def _generate_conversation_starter(
        self,
        partner: SupabaseProfile,
        insights: List[PartnershipInsight]
    ) -> Optional[str]:
        """Generate a conversation starter for the intro."""
        if not insights:
            return None

        # Prioritize insights for conversation starters
        for insight in insights:
            if insight.type == 'seeking_offering':
                if partner.offering:
                    return f"Ask {partner.name.split()[0] if partner.name else 'them'} about their approach to {partner.offering[:30]}..."
            elif insight.type == 'audience_overlap':
                if self.icp:
                    return f"Discuss how you both serve {self.icp.industry} and explore synergies"
            elif insight.type == 'scale_match':
                return "Compare audience engagement strategies and list-building approaches"

        return None

    def _format_number(self, num: int) -> str:
        """Format number for display (e.g., 5000 -> 5K)."""
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.0f}K"
        return str(num)

    def analyze_batch(
        self,
        partners: List[SupabaseProfile],
        matches_by_partner_id: Optional[Dict[str, SupabaseMatch]] = None
    ) -> List[PartnershipAnalysis]:
        """
        Analyze multiple partners efficiently.

        Args:
            partners: List of SupabaseProfile instances
            matches_by_partner_id: Dict mapping partner.id to SupabaseMatch

        Returns:
            List of PartnershipAnalysis, sorted by tier/score
        """
        analyses = []
        matches_by_partner_id = matches_by_partner_id or {}

        for partner in partners:
            match = matches_by_partner_id.get(str(partner.id))
            analysis = self.analyze(partner, match)
            analyses.append(analysis)

        # Sort by tier (hand_picked first) then by score
        tier_order = {'hand_picked': 0, 'strong': 1, 'wildcard': 2}
        analyses.sort(
            key=lambda a: (tier_order.get(a.tier, 3), -(a.score or 0))
        )

        return analyses


# =============================================================================
# SUPABASE MATCH SCORING SERVICE — ISMC for SupabaseProfile pairs
# =============================================================================


class ScoreValidator:
    """Pre-scoring and post-scoring validation."""

    SCORING_FIELDS = ('seeking', 'offering', 'who_you_serve', 'what_you_do')
    MIN_FIELDS_FOR_SCORING = 2

    @classmethod
    def check_scoring_eligibility(cls, profile_a, profile_b) -> tuple[bool, str]:
        """Check if a pair has enough data for meaningful scoring."""
        def count_fields(profile):
            return sum(
                1 for f in cls.SCORING_FIELDS
                if getattr(profile, f, None) and len(str(getattr(profile, f, '')).strip()) >= 5
            )

        a_count = count_fields(profile_a)
        b_count = count_fields(profile_b)

        if a_count < cls.MIN_FIELDS_FOR_SCORING and b_count < cls.MIN_FIELDS_FOR_SCORING:
            return False, f"Insufficient data: {profile_a.name} has {a_count} fields, {profile_b.name} has {b_count} fields (need {cls.MIN_FIELDS_FOR_SCORING} each)"

        return True, "ok"

    @classmethod
    def validate_scores(cls, score_ab: float, score_ba: float, harmonic: float) -> list[str]:
        """Post-scoring sanity checks."""
        issues = []

        for name, val in [('score_ab', score_ab), ('score_ba', score_ba), ('harmonic_mean', harmonic)]:
            if val is None:
                continue
            if math.isnan(val) or math.isinf(val):
                issues.append(f"{name} is {val}")
            elif val < 0 or val > 100:
                issues.append(f"{name} out of bounds: {val}")

        return issues


class SupabaseMatchScoringService:
    """
    Scores a pair of SupabaseProfiles using the ISMC framework.

    Computes directional scores:
    - score_ab: How valuable is B as a partner for A?
    - score_ba: How valuable is A as a partner for B?
    - harmonic_mean: Balanced pair score

    ISMC weights:
    - Intent (45%): Partnership readiness signals
    - Synergy (25%): Business complementarity
    - Momentum (20%): Activity and engagement
    - Context (10%): Data quality and reliability
    """

    WEIGHTS = {
        'intent': 0.45,
        'synergy': 0.25,
        'momentum': 0.20,
        'context': 0.10,
    }

    REVENUE_TIER_ORDER = {
        'micro': 0,
        'emerging': 1,
        'established': 2,
        'premium': 3,
        'enterprise': 4,
    }

    # Calibrated embedding similarity → 0-10 score mapping.
    # Derived from validation data: synonym mean=0.75, random noise mean=0.53.
    EMBEDDING_SCORE_THRESHOLDS = [
        (0.75, 10.0),  # Strong semantic match (synonym-level)
        (0.65,  8.0),  # Good match, well above noise floor
        (0.60,  6.0),  # Possible match, threshold zone
        (0.53,  4.5),  # At random-pair mean, weak signal
    ]
    EMBEDDING_SCORE_DEFAULT = 3.0  # Below noise floor — no signal

    # --- Role compatibility ---
    # Maps raw network_role values to canonical categories.
    _ROLE_NORMALIZE = {
        'service provider': 'Service Provider',
        'service_provider': 'Service Provider',
        'generalist': 'Service Provider',
        'practitioner': 'Service Provider',
        'specialist': 'Service Provider',
        'general business professional': 'Service Provider',
        'operator': 'Service Provider',
        'thought leader': 'Thought Leader',
        'thought_leader': 'Thought Leader',
        'speaker': 'Thought Leader',
        'speaker / author': 'Thought Leader',
        'connector': 'Connector',
        'hub': 'Connector',
        'bridge': 'Connector',
        'connector / network builder': 'Connector',
        'connector / community builder': 'Connector',
        'connector_partner_seeker': 'Connector',
        'newcomer': 'Newcomer',
        'product creator': 'Product Creator',
        'creator': 'Product Creator',
        'media/publisher': 'Media/Publisher',
        'content creator': 'Media/Publisher',
        'content_creator': 'Media/Publisher',
        'content creator / educator': 'Media/Publisher',
        'content_creator_influencer': 'Media/Publisher',
        'broadcaster': 'Media/Publisher',
        'media / content creator': 'Media/Publisher',
        'media_content_creator': 'Media/Publisher',
        'amplifier / media': 'Media/Publisher',
        'amplifier': 'Media/Publisher',
        'influencer': 'Media/Publisher',
        'community builder': 'Community Builder',
        'affiliate/promoter': 'Affiliate/Promoter',
        'audience builder': 'Affiliate/Promoter',
        'referral_partner': 'Affiliate/Promoter',
        'educator': 'Educator',
        'educator_trainer': 'Educator',
        'educator / coach': 'Educator',
        'educator/trainer': 'Educator',
        'speaker / educator': 'Educator',
        'speaker/educator': 'Educator',
        'thought leader / educator': 'Educator',
        'expert, educator': 'Educator',
        'coach': 'Coach',
        'coach / mentor': 'Coach',
        'coach / consultant': 'Coach',
        'coach / practitioner': 'Coach',
        'coach/speaker': 'Coach',
        'expert/advisor': 'Expert/Advisor',
        'expert': 'Expert/Advisor',
        'advisor': 'Expert/Advisor',
        'consultant': 'Expert/Advisor',
        'consultant / advisor': 'Expert/Advisor',
        'strategic advisor': 'Expert/Advisor',
        'industry expert': 'Expert/Advisor',
    }

    # Symmetric role-pair compatibility scores (0-10).
    # Keyed by frozenset so (A,B) == (B,A).
    _ROLE_COMPAT = {
        # --- HIGH (8-10): clear, proven JV format ---
        frozenset(['Media/Publisher', 'Thought Leader']): 9.0,
        frozenset(['Media/Publisher', 'Coach']): 8.5,
        frozenset(['Media/Publisher', 'Educator']): 8.5,
        frozenset(['Media/Publisher', 'Expert/Advisor']): 8.5,
        frozenset(['Media/Publisher', 'Product Creator']): 8.0,
        frozenset(['Connector', 'Service Provider']): 8.5,
        frozenset(['Connector', 'Thought Leader']): 8.5,
        frozenset(['Connector', 'Media/Publisher']): 8.5,
        frozenset(['Connector', 'Coach']): 8.0,
        frozenset(['Connector', 'Product Creator']): 8.0,
        frozenset(['Connector', 'Educator']): 8.0,
        frozenset(['Connector', 'Expert/Advisor']): 8.0,
        frozenset(['Community Builder', 'Thought Leader']): 9.0,
        frozenset(['Community Builder', 'Educator']): 8.5,
        frozenset(['Community Builder', 'Coach']): 8.0,
        frozenset(['Community Builder', 'Media/Publisher']): 8.0,
        frozenset(['Affiliate/Promoter', 'Product Creator']): 9.0,
        frozenset(['Affiliate/Promoter', 'Coach']): 8.0,
        frozenset(['Coach', 'Product Creator']): 8.0,
        # --- MODERATE (5-7): possible, needs niche alignment ---
        frozenset(['Community Builder', 'Service Provider']): 7.5,
        frozenset(['Community Builder', 'Product Creator']): 7.0,
        frozenset(['Community Builder', 'Connector']): 7.0,
        frozenset(['Community Builder', 'Expert/Advisor']): 7.0,
        frozenset(['Thought Leader', 'Product Creator']): 7.0,
        frozenset(['Thought Leader', 'Educator']): 7.0,
        frozenset(['Coach', 'Educator']): 7.5,
        frozenset(['Expert/Advisor', 'Thought Leader']): 6.5,
        frozenset(['Expert/Advisor', 'Educator']): 6.5,
        frozenset(['Expert/Advisor', 'Product Creator']): 6.5,
        frozenset(['Thought Leader', 'Coach']): 6.5,
        frozenset(['Affiliate/Promoter', 'Service Provider']): 7.5,
        frozenset(['Affiliate/Promoter', 'Educator']): 7.5,
        frozenset(['Affiliate/Promoter', 'Media/Publisher']): 6.5,
        frozenset(['Affiliate/Promoter', 'Connector']): 6.0,
        frozenset(['Affiliate/Promoter', 'Thought Leader']): 6.0,
        frozenset(['Affiliate/Promoter', 'Community Builder']): 6.0,
        frozenset(['Affiliate/Promoter', 'Expert/Advisor']): 5.5,
        frozenset(['Service Provider', 'Thought Leader']): 6.0,
        frozenset(['Service Provider', 'Educator']): 6.0,
        frozenset(['Service Provider', 'Coach']): 6.0,
        frozenset(['Service Provider', 'Product Creator']): 6.0,
        frozenset(['Service Provider', 'Media/Publisher']): 6.0,
        frozenset(['Service Provider', 'Affiliate/Promoter']): 6.5,
        frozenset(['Service Provider', 'Community Builder']): 6.5,
        frozenset(['Service Provider', 'Expert/Advisor']): 5.5,
        # Same-role pairings
        frozenset(['Connector']): 6.0,
        frozenset(['Community Builder']): 6.0,
        frozenset(['Service Provider']): 5.5,
        frozenset(['Coach']): 5.5,
        frozenset(['Educator']): 5.5,
        frozenset(['Thought Leader']): 5.0,
        frozenset(['Product Creator']): 5.0,
        frozenset(['Media/Publisher']): 5.0,
        frozenset(['Expert/Advisor']): 5.0,
        frozenset(['Affiliate/Promoter']): 4.0,
        # --- LOW (3-4.5): newcomers, unclear format ---
        frozenset(['Newcomer']): 3.0,
        frozenset(['Newcomer', 'Connector']): 5.0,
        frozenset(['Newcomer', 'Community Builder']): 5.0,
        frozenset(['Newcomer', 'Thought Leader']): 4.5,
        frozenset(['Newcomer', 'Coach']): 4.5,
        frozenset(['Newcomer', 'Educator']): 4.5,
        frozenset(['Newcomer', 'Media/Publisher']): 4.5,
        frozenset(['Newcomer', 'Service Provider']): 4.0,
        frozenset(['Newcomer', 'Product Creator']): 4.0,
        frozenset(['Newcomer', 'Expert/Advisor']): 4.0,
        frozenset(['Newcomer', 'Affiliate/Promoter']): 3.5,
    }

    def _normalize_role(self, raw: str | None) -> str | None:
        """Normalize a network_role value to a canonical category."""
        if not raw:
            return None
        return self._ROLE_NORMALIZE.get(raw.lower().strip())

    def _role_compat_score(self, role_a: str | None, role_b: str | None) -> float:
        """Score structural compatibility between two network roles (0-10)."""
        if not role_a or not role_b:
            return 5.0  # Neutral when role data is missing
        key = frozenset([role_a, role_b])
        return self._ROLE_COMPAT.get(key, 5.0)

    @staticmethod
    def _parse_pgvector(value) -> list[float] | None:
        """Parse a pgvector column value into a list of floats.

        pgvector returns values as strings like '[0.1,0.2,...]'.
        Returns None if the value is empty or unparseable.
        """
        if value is None:
            return None
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            s = value.strip()
            if not s or s == '':
                return None
            try:
                return json.loads(s)
            except (json.JSONDecodeError, ValueError):
                return None
        return None

    @staticmethod
    def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
        """Cosine similarity between two vectors, clamped to [0, 1]."""
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0
        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return max(0.0, min(1.0, dot / (norm_a * norm_b)))

    def _embedding_to_score(self, similarity: float) -> float:
        """Convert cosine similarity to a calibrated 0-10 score."""
        for threshold, score in self.EMBEDDING_SCORE_THRESHOLDS:
            if similarity >= threshold:
                return score
        return self.EMBEDDING_SCORE_DEFAULT

    def score_pair(
        self,
        profile_a: SupabaseProfile,
        profile_b: SupabaseProfile,
        outcome_data=None
    ) -> dict:
        """
        Score a pair of profiles directionally.

        Returns:
            dict with score_ab, score_ba, harmonic_mean (all 0-100 scale),
            plus component breakdowns for each direction.
        """
        # Pre-scoring eligibility check
        eligible, reason = ScoreValidator.check_scoring_eligibility(profile_a, profile_b)
        if not eligible:
            logger.debug(f"Pair ineligible for scoring: {reason}")
            return {
                'score_ab': 0, 'score_ba': 0, 'harmonic_mean': 0,
                'match_reason': '', 'ineligible': True, 'ineligible_reason': reason,
            }

        score_ab, breakdown_ab = self._score_directional(profile_a, profile_b, outcome_data=outcome_data)
        score_ba, breakdown_ba = self._score_directional(profile_b, profile_a, outcome_data=outcome_data)

        # Harmonic mean of the two directional scores
        epsilon = 1e-10
        if score_ab > epsilon and score_ba > epsilon:
            hm = 2 / (1 / max(score_ab, epsilon) + 1 / max(score_ba, epsilon))
        else:
            hm = 0.0

        # Post-scoring sanity validation
        score_issues = ScoreValidator.validate_scores(score_ab, score_ba, hm)
        if score_issues:
            logger.warning(f"Score validation issues for {profile_a.name} <-> {profile_b.name}: {score_issues}")

        match_reason = self._generate_match_reason(
            profile_a, profile_b, breakdown_ab, breakdown_ba, hm
        )

        return {
            'score_ab': round(score_ab, 2),
            'score_ba': round(score_ba, 2),
            'harmonic_mean': round(hm, 2),
            'breakdown_ab': breakdown_ab,
            'breakdown_ba': breakdown_ba,
            'match_reason': match_reason,
        }

    # Niches that are too generic or are data artifacts
    NICHE_BLOCKLIST = {'host', 'other', 'misc', 'none', 'n/a', 'unknown', ''}

    def _generate_match_reason(
        self,
        profile_a: SupabaseProfile,
        profile_b: SupabaseProfile,
        breakdown_ab: dict,
        breakdown_ba: dict,
        harmonic_mean: float,
    ) -> str:
        """Generate an insightful, specific match reason for a partner pair.

        Builds from 5 signal layers: audience overlap, offering↔seeking
        alignment, collaboration format, scale context, and niche fit.
        Output reads like a thoughtful recommendation, not an algorithm.
        """
        name_b = (profile_b.name or '').split()[0]  # First name for natural tone
        parts = []

        # --- Layer 1: Audience overlap (strongest signal) ---
        who_a = (profile_a.who_you_serve or '').lower()
        who_b = (profile_b.who_you_serve or '').lower()
        name_b_lower = (profile_b.name or '').lower()
        if who_a and who_b:
            # Find shared audience keywords (4+ char words, skip stop/generic words)
            stop = {'and', 'the', 'for', 'who', 'are', 'that', 'with', 'their',
                    'seeking', 'looking', 'need', 'want', 'people', 'those',
                    'individuals', 'help', 'from', 'them', 'they', 'have',
                    'been', 'into', 'more', 'also', 'like', 'just', 'about'}
            # Also exclude the partner's own name words
            name_words = set(name_b_lower.split())
            words_a = {w for w in re.split(r'[,\s]+', who_a)
                       if len(w) > 4 and w not in stop and w not in name_words}
            words_b = {w for w in re.split(r'[,\s]+', who_b)
                       if len(w) > 4 and w not in stop and w not in name_words}
            shared = words_a & words_b
            if shared:
                # Use up to 2 shared audience terms
                terms = sorted(shared)[:2]
                parts.append(
                    f'Both serve {" and ".join(terms)}, '
                    f'creating a natural audience bridge for cross-promotion'
                )
            elif who_b and name_b_lower not in who_b[:30]:
                # Complementary audiences — truncate at word boundary
                audience_summary = self._truncate_at_boundary(who_b, 80)
                parts.append(f'{name_b} reaches {audience_summary}')

        # --- Layer 2: Offering↔Seeking alignment ---
        seeking_a = (profile_a.seeking or '').lower()
        offering_b = (profile_b.offering or '').lower()
        if seeking_a and offering_b:
            seek_items = {s.strip() for s in seeking_a.split(',') if s.strip()}
            offer_items = {o.strip() for o in offering_b.split(',') if o.strip()}
            # Find items where seeking keyword appears in an offering
            matched = set()
            for seek in seek_items:
                seek_words = {w for w in seek.split() if len(w) > 3}
                for offer in offer_items:
                    if any(sw in offer.lower() for sw in seek_words):
                        matched.add(offer.strip().title())
                        break
            if matched:
                match_text = ', '.join(sorted(matched)[:2])
                parts.append(f'{name_b} offers {match_text} — aligning with your partnership goals')

        # --- Layer 3: Concrete collaboration format ---
        platforms_b = profile_b.content_platforms or {}
        list_b = profile_b.list_size or 0
        list_a = profile_a.list_size or 0
        sig_programs = (profile_b.signature_programs or '').strip()

        podcast_name = ''
        if isinstance(platforms_b, dict):
            podcast_name = (platforms_b.get('podcast_name') or '').strip()

        if podcast_name:
            reach = f' to their {self._format_reach(list_b)} audience' if list_b > 1000 else ''
            parts.append(
                f'Suggested next step: A guest appearance on {podcast_name}{reach}'
            )
        elif 'podcast' in offering_b:
            parts.append(
                f'Suggested next step: A podcast guest swap to cross-pollinate audiences'
            )
        elif ('speaking' in offering_b or 'event' in offering_b
              or 'webinar' in offering_b):
            niche_topic = self._clean_niche(profile_b.niche) or 'your shared area of focus'
            parts.append(
                f'Suggested next step: A co-hosted webinar on {niche_topic}'
            )
        elif 'course' in offering_b or 'program' in offering_b:
            if sig_programs:
                program = sig_programs.split(',')[0].strip()[:50]
                parts.append(
                    f'Suggested next step: Explore a joint program building on {program}'
                )
            else:
                parts.append(
                    f'Suggested next step: A joint program combining your complementary expertise'
                )
        elif list_a > 5000 and list_b > 5000:
            combined = self._format_reach(list_a + list_b)
            parts.append(
                f'Suggested next step: A co-promoted email campaign to your combined {combined} subscribers'
            )
        else:
            parts.append(
                f'Suggested next step: An introductory call to explore mutual referral opportunities'
            )

        # --- Layer 4: Niche context (only when meaningful) ---
        niche_a = (profile_a.niche or '').lower().strip()
        niche_b = (profile_b.niche or '').lower().strip()
        if (niche_a and niche_b
                and niche_a not in self.NICHE_BLOCKLIST
                and niche_b not in self.NICHE_BLOCKLIST):
            if niche_a == niche_b and not parts:
                # Only add niche if we don't have richer signals
                parts.insert(0, f'Shared focus on {profile_b.niche.lower()}')

        reason = '. '.join(parts) if parts else 'Complementary business profiles worth a conversation'
        return TextSanitizer.validate_match_reason(reason)

    @staticmethod
    def _format_reach(size: int) -> str:
        """Format audience size for display: 295000 → '295K'."""
        if not size:
            return ''
        if size >= 1_000_000:
            return f'{size / 1_000_000:.1f}M'
        if size >= 1_000:
            return f'{size // 1_000}K'
        return str(size)

    def _clean_niche(self, niche: str | None) -> str:
        """Return niche if it's meaningful, empty string otherwise."""
        if not niche:
            return ''
        cleaned = niche.strip().lower()
        if cleaned in self.NICHE_BLOCKLIST or len(cleaned) < 4:
            return ''
        return niche.strip().lower()

    # Words that shouldn't end a truncated phrase
    _TRAILING_FILLER = {
        'and', 'or', 'but', 'the', 'a', 'an', 'in', 'on', 'at', 'to',
        'for', 'of', 'as', 'by', 'with', 'from', 'who', 'that', 'their',
        'are', 'is', 'be', 'its', 'particularly', 'including', 'such',
    }

    @classmethod
    def _truncate_at_boundary(cls, text: str, max_length: int) -> str:
        """Truncate text at a word boundary, stripping trailing filler words."""
        if not text or len(text) <= max_length:
            return text
        # Find the last space before the limit
        cut = text.rfind(' ', 0, max_length)
        if cut == -1:
            cut = max_length
        truncated = text[:cut].rstrip(' ,;')
        # Strip trailing prepositions/conjunctions/articles
        words = truncated.split()
        while words and words[-1].lower() in cls._TRAILING_FILLER:
            words.pop()
        return ' '.join(words) if words else truncated

    def _score_directional(
        self,
        source: SupabaseProfile,
        target: SupabaseProfile,
        outcome_data=None
    ) -> tuple[float, dict]:
        """
        Score how valuable target is as a partner for source.

        Returns (score_0_to_100, breakdown_dict).
        """
        intent = self._score_intent(target, outcome_data=outcome_data)
        synergy = self._score_synergy(source, target)
        momentum = self._score_momentum(target)
        context = self._score_context(target)

        # Build components, excluding dimensions with None scores
        # (Momentum returns None when all sub-factors lack data)
        dimension_scores = {
            'intent': intent['score'],
            'synergy': synergy['score'],
            'momentum': momentum['score'],
            'context': context['score'],
        }

        components = []
        for dim, weight in self.WEIGHTS.items():
            score = dimension_scores[dim]
            if score is not None:
                components.append((score, weight))

        # Weighted geometric mean (redistributes weight proportionally)
        epsilon = 1e-10
        total_weight = sum(w for _, w in components)
        if total_weight > 0 and components:
            log_sum = sum(w * math.log(max(s, epsilon)) for s, w in components)
            final_0_10 = math.exp(log_sum / total_weight)
        else:
            final_0_10 = 0.0

        # Convert to 0-100 scale
        final_score = final_0_10 * 10

        breakdown = {
            'intent': intent,
            'synergy': synergy,
            'momentum': momentum,
            'context': context,
            'final_0_10': round(final_0_10, 2),
        }
        return final_score, breakdown

    # ----- Intent (45%) — Does this partner want to collaborate? -----

    # Fields checked by Profile Investment score
    _INVESTMENT_FIELDS = [
        'bio', 'what_you_do', 'who_you_serve', 'offering', 'seeking',
        'niche', 'tags', 'company', 'website', 'booking_link',
        'linkedin', 'audience_type', 'network_role',
    ]

    def _score_intent(self, target: SupabaseProfile, outcome_data=None) -> dict:
        """Score partnership readiness signals for target profile."""
        factors = []
        total = 0.0
        max_total = 0.0

        # Factor 1: JV History (weight 4.0) — strongest intent signal
        jv_list = target.jv_history if isinstance(target.jv_history, list) else []
        jv_count = len(jv_list)
        if jv_count >= 3:
            jv_score = 10.0
        elif jv_count >= 1:
            jv_score = 7.0
        else:
            jv_score = 4.0  # No data ≠ no history
        factors.append({'name': 'JV History', 'score': jv_score, 'weight': 4,
                        'detail': f'{jv_count} past partnerships'})
        total += jv_score * 4
        max_total += 10 * 4

        # Factor 2: Booking link (weight 3.5) — "I'm taking meetings"
        has_booking = bool(target.booking_link and target.booking_link.strip())
        booking_score = 8.0 if has_booking else 3.0
        factors.append({'name': 'Booking Link', 'score': booking_score, 'weight': 3.5,
                        'detail': 'Ready for meetings' if has_booking else 'No booking link'})
        total += booking_score * 3.5
        max_total += 10 * 3.5

        # Factor 3: Profile Investment (weight 3.0) — gradient effort signal
        populated = 0
        for field_name in self._INVESTMENT_FIELDS:
            val = getattr(target, field_name, None)
            if val is not None:
                if isinstance(val, str) and val.strip():
                    populated += 1
                elif isinstance(val, list) and len(val) > 0:
                    populated += 1
        invest_score = (populated / len(self._INVESTMENT_FIELDS)) * 10
        factors.append({'name': 'Profile Investment', 'score': round(invest_score, 1), 'weight': 3,
                        'detail': f'{populated}/{len(self._INVESTMENT_FIELDS)} fields populated'})
        total += invest_score * 3
        max_total += 10 * 3

        # Factor 4: Website presence (weight 2.5) — professional seriousness
        has_website = bool(target.website and target.website.strip())
        website_score = 7.0 if has_website else 2.0
        factors.append({'name': 'Website', 'score': website_score, 'weight': 2.5,
                        'detail': 'Website available' if has_website else 'No website'})
        total += website_score * 2.5
        max_total += 10 * 2.5

        # Factor 5: Membership status (weight 2.0, bonus-only)
        status = (target.status or '').strip()
        _BONUS_STATUSES = {'Qualified': 9.0, 'Member': 8.0, 'Non Member Resource': 6.5}
        if status in _BONUS_STATUSES:
            status_score = _BONUS_STATUSES[status]
            factors.append({'name': 'Membership Status', 'score': status_score, 'weight': 2.0,
                            'detail': f'Status: {status}'})
            total += status_score * 2.0
            max_total += 10 * 2.0

        # Factor 6: Profile maintenance (weight 1.5, null-aware)
        profile_updated = target.profile_updated_at
        if isinstance(profile_updated, datetime):
            from django.utils import timezone as tz
            days_since_update = (tz.now() - profile_updated).days
            if days_since_update <= 30:
                maint_score = 9.0
            elif days_since_update <= 90:
                maint_score = 7.0
            elif days_since_update <= 180:
                maint_score = 5.0
            else:
                maint_score = 3.0
            factors.append({'name': 'Profile Maintenance', 'score': maint_score, 'weight': 1.5,
                            'detail': f'Profile updated {days_since_update}d ago'})
            total += maint_score * 1.5
            max_total += 10 * 1.5

        # Factor 7: Outcome track record (weight 3.0, null-aware, needs ≥3 outcomes)
        if outcome_data:
            outcomes = outcome_data.get(str(target.id), {})
            total_outcomes = sum(outcomes.values())
            if total_outcomes >= 3:
                positive = outcomes.get('connected_promising', 0)
                success_rate = positive / total_outcomes
                track_score = min(10.0, success_rate * 12)
                factors.append({'name': 'Outcome Track Record', 'score': round(track_score, 1), 'weight': 3.0,
                                'detail': f'{positive}/{total_outcomes} positive ({success_rate:.0%})'})
                total += track_score * 3.0
                max_total += 10 * 3.0

        score = (total / max_total) * 10 if max_total > 0 else 0
        return {'score': round(score, 2), 'factors': factors}

    # ----- Synergy (25%) — How well do their businesses complement? -----

    def _score_synergy(
        self, source: SupabaseProfile, target: SupabaseProfile
    ) -> dict:
        """Score business complementarity between source and target.

        Uses embedding cosine similarity when available (calibrated thresholds
        from validation data), falls back to word overlap when embeddings are null.
        """
        factors = []
        total = 0.0
        max_total = 0.0

        # Factor 1: Offering-to-seeking alignment (weight 3.5)
        src_seeking_emb = self._parse_pgvector(getattr(source, 'embedding_seeking', None))
        # Prefer embedding_offering, fall back to embedding_what_you_do
        tgt_offering_emb = (
            self._parse_pgvector(getattr(target, 'embedding_offering', None))
            or self._parse_pgvector(getattr(target, 'embedding_what_you_do', None))
        )

        if src_seeking_emb and tgt_offering_emb:
            sim = self._cosine_similarity(src_seeking_emb, tgt_offering_emb)
            alignment_score = self._embedding_to_score(sim)
            method = 'semantic'
            detail = f'Cosine similarity: {sim:.3f} → {alignment_score:.1f}/10'
        else:
            alignment_score = self._text_overlap_score(
                source.seeking or '', target.offering or target.what_you_do or ''
            )
            method = 'word_overlap'
            detail = f'Word overlap: {alignment_score:.1f}/10'

        factors.append({'name': 'Offering↔Seeking', 'score': alignment_score, 'weight': 3.5,
                        'detail': detail, 'method': method})
        total += alignment_score * 3.5
        max_total += 10 * 3.5

        # Factor 2: Audience alignment (weight 3.0)
        src_serve_emb = self._parse_pgvector(getattr(source, 'embedding_who_you_serve', None))
        tgt_serve_emb = self._parse_pgvector(getattr(target, 'embedding_who_you_serve', None))

        if src_serve_emb and tgt_serve_emb:
            sim = self._cosine_similarity(src_serve_emb, tgt_serve_emb)
            audience_score = self._embedding_to_score(sim)
            method = 'semantic'
            detail = f'Cosine similarity: {sim:.3f} → {audience_score:.1f}/10'
        else:
            audience_score = self._text_overlap_score(
                source.who_you_serve or '', target.who_you_serve or ''
            )
            method = 'word_overlap'
            detail = f'Word overlap: {audience_score:.1f}/10'

        factors.append({'name': 'Audience Alignment', 'score': audience_score, 'weight': 3.0,
                        'detail': detail, 'method': method})
        total += audience_score * 3.0
        max_total += 10 * 3.0

        # Factor 3: Role compatibility (weight 2.5) — structural JV format fit
        role_a = self._normalize_role(source.network_role)
        role_b = self._normalize_role(target.network_role)
        compat_score = self._role_compat_score(role_a, role_b)
        factors.append({'name': 'Role Compatibility', 'score': compat_score, 'weight': 2.5,
                        'detail': f'{role_a or "?"} ↔ {role_b or "?"}'})
        total += compat_score * 2.5
        max_total += 10 * 2.5

        # Factor 4: Revenue tier compatibility (weight 2.0, null-aware)
        # Only scored when BOTH profiles have real revenue data;
        # excluded otherwise so weight redistributes to other factors.
        src_rev = source.revenue_tier
        tgt_rev = target.revenue_tier
        has_rev_data = (
            src_rev and src_rev.strip() and src_rev != 'unknown'
            and tgt_rev and tgt_rev.strip() and tgt_rev != 'unknown'
        )
        if has_rev_data:
            rev_score = self._revenue_tier_compat(src_rev, tgt_rev)
            factors.append({'name': 'Revenue Tier', 'score': rev_score, 'weight': 2.0,
                            'detail': f'{src_rev} ↔ {tgt_rev}'})
            total += rev_score * 2.0
            max_total += 10 * 2.0

        # Factor 5: Content platform overlap (weight 2.0, null-aware)
        platform_score, shared_platforms = self._platform_overlap(source, target)
        if shared_platforms:
            factors.append({'name': 'Platform Overlap', 'score': platform_score, 'weight': 2.0,
                            'detail': f'Shared: {", ".join(shared_platforms)}'})
            total += platform_score * 2.0
            max_total += 10 * 2.0

        # Factor 6: Network influence (weight 2.0, null-aware, bilateral)
        src_net = self._network_influence_score(source)
        tgt_net = self._network_influence_score(target)
        if src_net is not None and tgt_net is not None:
            combined = (src_net + tgt_net) / 2
            factors.append({'name': 'Network Influence', 'score': round(combined, 1), 'weight': 2.0,
                            'detail': 'Both well-connected (composite network avg)'})
            total += combined * 2.0
            max_total += 10 * 2.0

        # Factor 7: Business scale compatibility (weight 1.5, null-aware, bilateral)
        _SIZE_ORDER = {'small': 1, 'medium': 2, 'large': 3}
        src_bs = source.business_size if isinstance(source.business_size, str) else ''
        tgt_bs = target.business_size if isinstance(target.business_size, str) else ''
        src_size = _SIZE_ORDER.get(src_bs.lower().strip())
        tgt_size = _SIZE_ORDER.get(tgt_bs.lower().strip())
        if src_size is not None and tgt_size is not None:
            gap = abs(src_size - tgt_size)
            if gap == 0:
                scale_score = 9.0
            elif gap == 1:
                scale_score = 7.0
            else:
                scale_score = 4.0
            factors.append({'name': 'Business Scale', 'score': scale_score, 'weight': 1.5,
                            'detail': f'{source.business_size} ↔ {target.business_size}'})
            total += scale_score * 1.5
            max_total += 10 * 1.5

        score = (total / max_total) * 10 if max_total > 0 else 0
        return {'score': round(score, 2), 'factors': factors}

    def _text_overlap_score(self, text_a: str, text_b: str) -> float:
        """Score keyword overlap between two text fields (0-10)."""
        if not text_a.strip() or not text_b.strip():
            return 3.0  # Neutral when data is missing

        words_a = set(re.findall(r'\b\w{4,}\b', text_a.lower()))
        words_b = set(re.findall(r'\b\w{4,}\b', text_b.lower()))

        # Remove common stop words
        stop = {'that', 'this', 'with', 'from', 'they', 'them', 'their',
                'have', 'been', 'were', 'will', 'would', 'could', 'should',
                'about', 'more', 'also', 'just', 'some', 'like', 'into',
                'other', 'what', 'your', 'help'}
        words_a -= stop
        words_b -= stop

        if not words_a or not words_b:
            return 3.0

        overlap = words_a & words_b
        # Jaccard-ish: overlap relative to smaller set
        smaller = min(len(words_a), len(words_b))
        ratio = len(overlap) / smaller if smaller > 0 else 0

        if ratio >= 0.4:
            return 10.0
        elif ratio >= 0.25:
            return 8.0
        elif ratio >= 0.15:
            return 6.0
        elif ratio >= 0.05:
            return 4.5
        else:
            return 3.0

    def _revenue_tier_compat(
        self, tier_a: Optional[str], tier_b: Optional[str]
    ) -> float:
        """Score revenue tier compatibility (0-10)."""
        if not tier_a or not tier_b:
            return 5.0  # Neutral when data missing
        if tier_a == 'unknown' or tier_b == 'unknown':
            return 5.0

        rank_a = self.REVENUE_TIER_ORDER.get(tier_a)
        rank_b = self.REVENUE_TIER_ORDER.get(tier_b)
        if rank_a is None or rank_b is None:
            return 5.0

        diff = abs(rank_a - rank_b)
        if diff == 0:
            return 9.0
        elif diff == 1:
            return 7.0
        elif diff == 2:
            return 5.0
        else:
            return 2.0

    def _platform_overlap(
        self, source: SupabaseProfile, target: SupabaseProfile
    ) -> tuple[float, list[str]]:
        """Score content platform overlap (0-10) and return shared platforms."""
        src_platforms = source.content_platforms if isinstance(source.content_platforms, dict) else {}
        tgt_platforms = target.content_platforms if isinstance(target.content_platforms, dict) else {}

        if not src_platforms or not tgt_platforms:
            return 3.0, []

        platform_labels = {
            'podcast_name': 'Podcast',
            'youtube_channel': 'YouTube',
            'instagram_handle': 'Instagram',
            'facebook_group': 'Facebook',
            'tiktok_handle': 'TikTok',
            'newsletter_name': 'Newsletter',
        }

        shared = []
        for key, label in platform_labels.items():
            if src_platforms.get(key) and tgt_platforms.get(key):
                shared.append(label)

        count = len(shared)
        if count >= 3:
            return 10.0, shared
        elif count == 2:
            return 8.0, shared
        elif count == 1:
            return 6.0, shared
        else:
            return 3.0, shared

    def _network_influence_score(self, profile) -> float | None:
        """Composite network score from 3 centrality metrics. Returns None if no data."""
        scores = []
        pr = profile.pagerank_score
        if isinstance(pr, (int, float)) and pr > 0:
            if pr >= 0.005:
                scores.extend([10.0, 10.0])
            elif pr >= 0.002:
                scores.extend([8.0, 8.0])
            elif pr >= 0.001:
                scores.extend([6.5, 6.5])
            elif pr >= 0.0005:
                scores.extend([5.0, 5.0])
            else:
                scores.extend([4.0, 4.0])
        dc = profile.degree_centrality
        if isinstance(dc, (int, float)) and dc > 0:
            scores.append(min(10.0, dc * 100))
        bc = profile.betweenness_centrality
        if isinstance(bc, (int, float)) and bc > 0:
            scores.append(min(10.0, bc * 200))
        return sum(scores) / len(scores) if scores else None

    # ----- Momentum (20%) — How active/engaged is this partner? -----

    def _score_momentum(self, target: SupabaseProfile) -> dict:
        """Score activity and engagement signals.

        Null-aware: only scores sub-factors that have real data.
        If ALL sub-factors are null, returns score=None so the caller
        can redistribute Momentum's weight to other dimensions.
        """
        factors = []
        total = 0.0
        max_total = 0.0

        # Factor 1: Audience engagement score (weight 3)
        engagement = target.audience_engagement_score
        if engagement is not None:
            eng_score = min(10.0, engagement * 10)
            factors.append({'name': 'Audience Engagement', 'score': round(eng_score, 1), 'weight': 3,
                            'detail': f'Engagement: {engagement:.2f}'})
            total += eng_score * 3
            max_total += 10 * 3

        # Factor 2: Social reach (weight 2)
        reach = target.social_reach
        if reach is not None and reach > 0:
            if reach >= 100000:
                reach_score = 10.0
            elif reach >= 50000:
                reach_score = 8.0
            elif reach >= 10000:
                reach_score = 6.5
            elif reach >= 1000:
                reach_score = 5.0
            else:
                reach_score = 4.0
            factors.append({'name': 'Social Reach', 'score': reach_score, 'weight': 2,
                            'detail': f'{reach:,} followers'})
            total += reach_score * 2
            max_total += 10 * 2

        # Factor 3: Current projects (weight 2.5)
        has_projects = bool(target.current_projects and len(target.current_projects.strip()) > 10)
        if has_projects:
            proj_score = 8.0
            factors.append({'name': 'Active Projects', 'score': proj_score, 'weight': 2.5,
                            'detail': 'Active projects noted'})
            total += proj_score * 2.5
            max_total += 10 * 2.5

        # Factor 4: List size as growth indicator (weight 2.5)
        list_size = target.list_size
        if list_size is not None and list_size > 0:
            if list_size >= 100000:
                list_score = 9.0
            elif list_size >= 50000:
                list_score = 8.0
            elif list_size >= 10000:
                list_score = 7.0
            elif list_size >= 1000:
                list_score = 5.5
            else:
                list_score = 4.0
            factors.append({'name': 'List Size', 'score': list_score, 'weight': 2.5,
                            'detail': f'{list_size:,}'})
            total += list_score * 2.5
            max_total += 10 * 2.5

        # Factor 5: Activity recency (weight 2.0, null-aware)
        last_active = target.last_active_at
        if isinstance(last_active, datetime):
            from django.utils import timezone as tz
            days_since = (tz.now() - last_active).days
            if days_since <= 7:
                active_score = 10.0
            elif days_since <= 30:
                active_score = 8.0
            elif days_since <= 90:
                active_score = 6.0
            elif days_since <= 180:
                active_score = 4.0
            else:
                active_score = 2.0
            factors.append({'name': 'Activity Recency', 'score': active_score, 'weight': 2.0,
                            'detail': f'Last active {days_since}d ago'})
            total += active_score * 2.0
            max_total += 10 * 2.0

        # All fields null → return None score so caller skips this dimension
        if max_total == 0:
            return {'score': None, 'factors': [], 'detail': 'No momentum data available'}

        score = (total / max_total) * 10
        return {'score': round(score, 2), 'factors': factors}

    # ----- Context (10%) — Data quality and reliability -----

    def _score_context(self, target: SupabaseProfile) -> dict:
        """Score data quality and profile completeness."""
        factors = []
        total = 0.0
        max_total = 0.0

        # Factor 1: Profile completeness (weight 3)
        completeness_fields = [
            target.name, target.email, target.company, target.website,
            target.linkedin, target.niche, target.what_you_do,
            target.who_you_serve, target.seeking, target.offering,
            target.booking_link, target.revenue_tier,
        ]
        filled = sum(1 for f in completeness_fields if f and str(f).strip())
        completeness_score = (filled / len(completeness_fields)) * 10
        factors.append({'name': 'Profile Completeness', 'score': round(completeness_score, 1), 'weight': 3,
                        'detail': f'{filled}/{len(completeness_fields)} key fields'})
        total += completeness_score * 3
        max_total += 10 * 3

        # Factor 2: Revenue tier known (weight 2)
        has_rev = bool(target.revenue_tier and target.revenue_tier != 'unknown')
        rev_score = 8.0 if has_rev else 3.0
        factors.append({'name': 'Revenue Known', 'score': rev_score, 'weight': 2,
                        'detail': target.revenue_tier or 'Unknown'})
        total += rev_score * 2
        max_total += 10 * 2

        # Factor 3: Enrichment quality (weight 2.5)
        enrichment_fields = [
            target.what_you_do, target.who_you_serve, target.niche,
            target.offering, target.seeking, target.signature_programs,
            target.revenue_tier, target.content_platforms,
        ]
        enriched = sum(1 for f in enrichment_fields if f)
        enrich_score = min(10.0, (enriched / len(enrichment_fields)) * 10)
        factors.append({'name': 'Enrichment Quality', 'score': round(enrich_score, 1), 'weight': 2.5,
                        'detail': f'{enriched}/{len(enrichment_fields)} enrichment fields'})
        total += enrich_score * 2.5
        max_total += 10 * 2.5

        # Factor 4: Contact availability (weight 2.5)
        has_email = bool(target.email and target.email.strip())
        has_phone = bool(target.phone and target.phone.strip())
        has_linkedin = bool(target.linkedin and target.linkedin.strip())
        if has_email:
            contact_score = 9.0
        elif has_linkedin:
            contact_score = 7.0
        elif has_phone:
            contact_score = 6.0
        else:
            contact_score = 2.0
        factors.append({'name': 'Contact Available', 'score': contact_score, 'weight': 2.5,
                        'detail': 'Email' if has_email else ('LinkedIn' if has_linkedin else 'Limited')})
        total += contact_score * 2.5
        max_total += 10 * 2.5

        # Factor 5: Enrichment confidence (weight 2.0, null-aware)
        confidence = target.profile_confidence
        if isinstance(confidence, (int, float)):
            conf_score = min(10.0, confidence * 10)
            factors.append({'name': 'Enrichment Confidence', 'score': round(conf_score, 1), 'weight': 2.0,
                            'detail': f'AI confidence: {confidence:.2f}'})
            total += conf_score * 2.0
            max_total += 10 * 2.0

        # Factor 6: Recommendation freshness (weight 1.5, null-aware — inverse scoring)
        pressure = target.recommendation_pressure_30d
        if isinstance(pressure, int):
            if pressure == 0:
                fresh_score = 9.0
            elif pressure <= 3:
                fresh_score = 7.0
            elif pressure <= 10:
                fresh_score = 5.0
            else:
                fresh_score = 3.0
            factors.append({'name': 'Recommendation Freshness', 'score': fresh_score, 'weight': 1.5,
                            'detail': f'Recommended {pressure}x in 30d'})
            total += fresh_score * 1.5
            max_total += 10 * 1.5

        score = (total / max_total) * 10 if max_total > 0 else 0
        return {'score': round(score, 2), 'factors': factors}

    # =================================================================
    # Lightweight pre-scoring for prospect acquisition flows
    # =================================================================

    # Redistributed weights (Momentum excluded):
    # Intent 50%, Synergy 35%, Context 15%
    LIGHTWEIGHT_WEIGHTS = {
        'intent': 0.50,
        'synergy': 0.35,
        'context': 0.15,
    }

    def score_pair_lightweight(
        self,
        profile_a: SupabaseProfile,
        profile_b: SupabaseProfile,
    ) -> dict:
        """Fast pre-scoring for partially-enriched prospect profiles.

        Uses a simplified ISMC variant that skips Momentum entirely and
        reduces each remaining dimension to the sub-factors most likely to
        be populated during early acquisition:

        - Intent (50%): booking_link + profile investment only
        - Synergy (35%): offering-to-seeking text overlap only
        - Context (15%): profile completeness only

        Args:
            profile_a: First SupabaseProfile (the "source" for score_ab).
            profile_b: Second SupabaseProfile (the "source" for score_ba).

        Returns:
            dict with score_ab, score_ba, harmonic_mean (0-100), and
            is_lightweight=True.
        """
        zero_result = {
            'score_ab': 0,
            'score_ba': 0,
            'harmonic_mean': 0,
            'is_lightweight': True,
        }

        # Early return when both profiles lack a name — not enough data
        name_a = getattr(profile_a, 'name', None)
        name_b = getattr(profile_b, 'name', None)
        if not (name_a and str(name_a).strip()) and not (name_b and str(name_b).strip()):
            return zero_result

        score_ab = self._score_directional_lightweight(profile_a, profile_b)
        score_ba = self._score_directional_lightweight(profile_b, profile_a)

        # Harmonic mean of the two directional scores
        epsilon = 1e-10
        if score_ab > epsilon and score_ba > epsilon:
            hm = 2 / (1 / max(score_ab, epsilon) + 1 / max(score_ba, epsilon))
        else:
            hm = 0.0

        return {
            'score_ab': round(score_ab, 2),
            'score_ba': round(score_ba, 2),
            'harmonic_mean': round(hm, 2),
            'is_lightweight': True,
        }

    def _score_directional_lightweight(
        self,
        source: SupabaseProfile,
        target: SupabaseProfile,
    ) -> float:
        """Compute a single directional lightweight score (0-100).

        Mirrors the geometric-mean pattern of ``_score_directional`` but
        only evaluates the simplified Intent, Synergy, and Context
        dimensions.
        """
        intent = self._score_intent_lightweight(target)
        synergy = self._score_synergy_lightweight(source, target)
        context = self._score_context_lightweight(target)

        components = [
            (intent, self.LIGHTWEIGHT_WEIGHTS['intent']),
            (synergy, self.LIGHTWEIGHT_WEIGHTS['synergy']),
            (context, self.LIGHTWEIGHT_WEIGHTS['context']),
        ]

        # Weighted geometric mean (same formula as _score_directional)
        epsilon = 1e-10
        total_weight = sum(w for _, w in components)
        if total_weight > 0:
            log_sum = sum(
                w * math.log(max(s, epsilon)) for s, w in components
            )
            final_0_10 = math.exp(log_sum / total_weight)
        else:
            final_0_10 = 0.0

        return final_0_10 * 10  # Convert to 0-100

    # --- Lightweight sub-scorers ---

    def _score_intent_lightweight(self, target: SupabaseProfile) -> float:
        """Simplified intent score (0-10): booking_link + profile investment.

        Skips JV history and website presence to stay fast on partial data.
        """
        total = 0.0
        max_total = 0.0

        # Factor 1: Booking link (weight 3.5)
        has_booking = bool(
            getattr(target, 'booking_link', None)
            and target.booking_link.strip()
        )
        booking_score = 8.0 if has_booking else 3.0
        total += booking_score * 3.5
        max_total += 10 * 3.5

        # Factor 2: Profile investment (weight 3.0) — same field list
        populated = 0
        for field_name in self._INVESTMENT_FIELDS:
            val = getattr(target, field_name, None)
            if val is not None:
                if isinstance(val, str) and val.strip():
                    populated += 1
                elif isinstance(val, list) and len(val) > 0:
                    populated += 1
        invest_score = (populated / len(self._INVESTMENT_FIELDS)) * 10
        total += invest_score * 3.0
        max_total += 10 * 3.0

        return (total / max_total) * 10 if max_total > 0 else 0.0

    def _score_synergy_lightweight(
        self,
        source: SupabaseProfile,
        target: SupabaseProfile,
    ) -> float:
        """Simplified synergy score (0-10): offering-to-seeking text overlap.

        Skips embeddings, role compatibility, and revenue tier to avoid
        dependencies on enrichment data that may not yet exist.
        """
        return self._text_overlap_score(
            source.seeking or '',
            target.offering or target.what_you_do or '',
        )

    def _score_context_lightweight(self, target: SupabaseProfile) -> float:
        """Simplified context score (0-10): profile completeness only.

        Skips revenue-known, enrichment quality, and contact availability
        since those depend on full enrichment.
        """
        completeness_fields = [
            target.name, target.email, target.company, target.website,
            target.linkedin, target.niche, target.what_you_do,
            target.who_you_serve, target.seeking, target.offering,
            target.booking_link, target.revenue_tier,
        ]
        filled = sum(1 for f in completeness_fields if f and str(f).strip())
        return (filled / len(completeness_fields)) * 10
