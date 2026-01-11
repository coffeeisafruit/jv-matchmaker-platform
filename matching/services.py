"""
Match Scoring Service for JV Matcher Module.

Implements the ISMC (Intent, Synergy, Momentum, Context) scoring framework
using a weighted harmonic mean for the final score calculation.

Also includes PartnershipAnalyzer for dynamic partnership insights that
integrates ICP and Transformation data with pre-computed SupabaseMatch data.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import math
import re

from django.conf import settings

from .models import Profile, Match, SupabaseProfile, SupabaseMatch


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
    TIER_THRESHOLDS = {
        'hand_picked': 80,  # 80%+ = rare, exceptional match
        'strong': 60,       # 60-80% = solid potential
        'wildcard': 0,      # Below 60% or no score = discovery
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
