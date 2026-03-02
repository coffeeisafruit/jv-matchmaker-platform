"""
Market gap analysis: identifies supply-demand imbalances across the JV population.

Analyzes ENRICHED profiles only (those with seeking/offering populated) to find:
  1. Supply-demand gaps by keyword (what people seek vs. what's offered)
  2. Structural role gaps by niche (which roles are underrepresented)
  3. Niche ecosystem health scores (balanced vs. lopsided)

The analysis drives sourcing priorities (which scrapers to run next) and
feeds into the admin notification email for monthly reporting.

Usage:
    from matching.enrichment.market_gaps import MarketGapAnalyzer

    analyzer = MarketGapAnalyzer(profiles)
    report = analyzer.analyze()
    analyzer.generate_report(report, output_dir="reports/")
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from matching.enrichment.niche_normalization import (
    normalize_niche,
    get_unmapped_niches,
    NICHE_BLOCKLIST,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SupplyDemandGap:
    """A single supply-demand gap for a keyword."""
    keyword: str
    seeking_count: int      # How many profiles seek this
    offering_count: int     # How many profiles offer this
    gap_ratio: float        # seeking / max(offering, 1) — higher = bigger gap

    @property
    def gap_type(self) -> str:
        if self.gap_ratio > 3.0:
            return "high_demand"
        elif self.gap_ratio < 0.3:
            return "oversupplied"
        else:
            return "balanced"


@dataclass
class RoleGap:
    """Structural role gap within a niche."""
    canonical_niche: str
    total_profiles: int
    role_distribution: dict[str, int]   # role → count
    missing_high_value_roles: list[str]  # Roles with <3 profiles that have high compat
    dominant_role: str
    dominance_ratio: float              # dominant_count / total


@dataclass
class NicheHealth:
    """Ecosystem health score for a niche."""
    canonical_niche: str
    total_profiles: int
    role_diversity: float       # 0-1, higher = more diverse (Shannon entropy / max)
    supply_demand_balance: float  # 0-1, higher = better balanced
    health_score: float         # Combined 0-100


@dataclass
class MarketIntelligenceReport:
    """Full market intelligence report."""
    computed_at: str
    enriched_profile_count: int
    canonical_niche_count: int
    supply_demand_gaps: list[SupplyDemandGap]
    role_gaps: list[RoleGap]
    niche_health: list[NicheHealth]
    unmapped_niches: list[tuple[str, int]]
    stability_warnings: list[str]
    recommended_new_scrapers: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "computed_at": self.computed_at,
            "enriched_profile_count": self.enriched_profile_count,
            "canonical_niche_count": self.canonical_niche_count,
            "supply_demand_gaps": [
                {
                    "keyword": g.keyword,
                    "seeking_count": g.seeking_count,
                    "offering_count": g.offering_count,
                    "gap_ratio": round(g.gap_ratio, 2),
                    "gap_type": g.gap_type,
                }
                for g in self.supply_demand_gaps
            ],
            "role_gaps": [
                {
                    "niche": g.canonical_niche,
                    "total_profiles": g.total_profiles,
                    "role_distribution": g.role_distribution,
                    "missing_high_value_roles": g.missing_high_value_roles,
                    "dominant_role": g.dominant_role,
                    "dominance_ratio": round(g.dominance_ratio, 2),
                }
                for g in self.role_gaps
            ],
            "niche_health": [
                {
                    "niche": h.canonical_niche,
                    "total_profiles": h.total_profiles,
                    "role_diversity": round(h.role_diversity, 3),
                    "supply_demand_balance": round(h.supply_demand_balance, 3),
                    "health_score": round(h.health_score, 1),
                }
                for h in self.niche_health
            ],
            "unmapped_niches": [
                {"niche": n, "count": c} for n, c in self.unmapped_niches[:20]
            ],
            "stability_warnings": self.stability_warnings,
            "recommended_new_scrapers": self.recommended_new_scrapers,
        }


# ---------------------------------------------------------------------------
# High-compatibility roles (from services.py _ROLE_COMPAT, scores >= 8.0)
# ---------------------------------------------------------------------------

HIGH_VALUE_ROLE_PAIRS: dict[str, list[str]] = {
    "Coach": ["Media/Publisher", "Connector", "Affiliate/Promoter"],
    "Educator": ["Media/Publisher", "Connector", "Product Creator"],
    "Media/Publisher": [
        "Thought Leader", "Coach", "Educator", "Expert/Advisor",
        "Product Creator", "Connector",
    ],
    "Connector": [
        "Service Provider", "Thought Leader", "Media/Publisher",
        "Coach", "Product Creator",
    ],
    "Thought Leader": ["Media/Publisher", "Community Builder", "Connector"],
    "Affiliate/Promoter": ["Product Creator", "Coach", "Educator"],
    "Product Creator": [
        "Affiliate/Promoter", "Media/Publisher", "Connector", "Educator",
    ],
    "Service Provider": ["Connector", "Thought Leader"],
    "Community Builder": ["Thought Leader", "Educator", "Coach"],
    "Expert/Advisor": ["Media/Publisher", "Connector"],
}


# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------

class MarketGapAnalyzer:
    """Analyzes enriched profiles to find market gaps and opportunities.

    Parameters
    ----------
    profiles : list[dict]
        Each dict must have at minimum: seeking, offering, niche, network_role.
        Typically loaded from Supabase ``profiles`` table.
    source_field : str
        Optional field name to check for stability analysis.
    """

    # Minimum keyword length for frequency analysis
    MIN_KEYWORD_LEN = 4

    # Stop words to exclude from keyword frequency
    STOP_WORDS = frozenset({
        "and", "the", "for", "who", "are", "that", "with", "their",
        "seeking", "looking", "need", "want", "people", "those",
        "individuals", "help", "from", "them", "they", "have",
        "been", "into", "more", "also", "like", "just", "about",
        "other", "would", "will", "each", "some", "this", "what",
        "make", "find", "know", "take", "come", "could", "than",
        "your", "services", "service", "business", "company",
        "partners", "partner", "opportunities", "opportunity",
    })

    def __init__(
        self,
        profiles: list[dict],
        source_field: str = "source",
    ):
        self.profiles = profiles
        self.source_field = source_field

    def analyze(self) -> MarketIntelligenceReport:
        """Run full market intelligence analysis."""
        # Extract keywords from seeking and offering fields
        seeking_keywords = Counter()
        offering_keywords = Counter()
        niche_role_map: dict[str, Counter] = defaultdict(Counter)
        niche_profile_count: dict[str, int] = defaultdict(int)
        raw_niches: list[str] = []

        for profile in self.profiles:
            seeking = profile.get("seeking") or ""
            offering = profile.get("offering") or profile.get("what_you_do") or ""
            raw_niche = profile.get("niche") or ""
            role = profile.get("network_role") or ""

            # Keyword extraction
            for kw in self._extract_keywords(seeking):
                seeking_keywords[kw] += 1
            for kw in self._extract_keywords(offering):
                offering_keywords[kw] += 1

            # Niche normalization
            if raw_niche:
                raw_niches.append(raw_niche)
            canonical = normalize_niche(raw_niche)
            if canonical:
                niche_profile_count[canonical] += 1
                if role:
                    normalized_role = self._normalize_role(role)
                    if normalized_role:
                        niche_role_map[canonical][normalized_role] += 1

        # 1. Supply-demand gaps
        all_keywords = set(seeking_keywords.keys()) | set(offering_keywords.keys())
        gaps = []
        for kw in all_keywords:
            seek_count = seeking_keywords.get(kw, 0)
            offer_count = offering_keywords.get(kw, 0)
            ratio = seek_count / max(offer_count, 1)
            # Only include keywords with meaningful volume
            if seek_count >= 3 or offer_count >= 3:
                gaps.append(SupplyDemandGap(
                    keyword=kw,
                    seeking_count=seek_count,
                    offering_count=offer_count,
                    gap_ratio=ratio,
                ))
        # Sort by gap ratio descending (biggest unmet demand first)
        gaps.sort(key=lambda g: g.gap_ratio, reverse=True)

        # 2. Structural role gaps by niche
        role_gaps = []
        for niche, role_counter in niche_role_map.items():
            total = niche_profile_count.get(niche, 0)
            if total < 5:
                continue  # Skip very small niches

            role_dist = dict(role_counter)
            dominant_role = role_counter.most_common(1)[0][0] if role_counter else ""
            dominant_count = role_counter.most_common(1)[0][1] if role_counter else 0
            dominance = dominant_count / max(total, 1)

            # Find missing high-value roles
            missing = self._find_missing_high_value_roles(
                dominant_role, role_dist, total
            )

            role_gaps.append(RoleGap(
                canonical_niche=niche,
                total_profiles=total,
                role_distribution=role_dist,
                missing_high_value_roles=missing,
                dominant_role=dominant_role,
                dominance_ratio=dominance,
            ))
        # Sort by number of missing roles descending
        role_gaps.sort(key=lambda g: len(g.missing_high_value_roles), reverse=True)

        # 3. Niche ecosystem health
        health_scores = []
        for niche, total in niche_profile_count.items():
            if total < 5:
                continue

            role_counter = niche_role_map.get(niche, Counter())
            diversity = self._shannon_diversity(role_counter)

            # Supply-demand balance for this niche's keywords
            balance = self._niche_supply_demand_balance(
                niche, seeking_keywords, offering_keywords
            )

            # Combined score (0-100): 60% diversity + 40% balance
            health = (diversity * 60) + (balance * 40)

            health_scores.append(NicheHealth(
                canonical_niche=niche,
                total_profiles=total,
                role_diversity=diversity,
                supply_demand_balance=balance,
                health_score=health,
            ))
        health_scores.sort(key=lambda h: h.health_score, reverse=True)

        # 4. Unmapped niches (for vocabulary expansion)
        unmapped = get_unmapped_niches(raw_niches)

        # 5. Stability analysis
        warnings = self._stability_check(niche_profile_count)

        # 6. Scraper recommendations for uncovered gaps
        recommended_scrapers = self._recommend_new_scrapers(
            gaps, role_gaps, health_scores,
        )

        return MarketIntelligenceReport(
            computed_at=datetime.utcnow().isoformat(),
            enriched_profile_count=len(self.profiles),
            canonical_niche_count=len(niche_profile_count),
            supply_demand_gaps=gaps,
            role_gaps=role_gaps,
            niche_health=health_scores,
            unmapped_niches=unmapped,
            stability_warnings=warnings,
            recommended_new_scrapers=recommended_scrapers,
        )

    # -------------------------------------------------------------------
    # Keyword extraction
    # -------------------------------------------------------------------

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract meaningful keywords from text (4+ chars, no stop words)."""
        if not text:
            return []
        words = re.findall(r'\b\w{4,}\b', text.lower())
        return [w for w in words if w not in self.STOP_WORDS]

    # -------------------------------------------------------------------
    # Role normalization (mirrors services.py _ROLE_ALIASES)
    # -------------------------------------------------------------------

    _ROLE_MAP = {
        "service provider": "Service Provider",
        "thought leader": "Thought Leader",
        "connector": "Connector",
        "community builder": "Community Builder",
        "affiliate/promoter": "Affiliate/Promoter",
        "educator": "Educator",
        "coach": "Coach",
        "expert/advisor": "Expert/Advisor",
        "media/publisher": "Media/Publisher",
        "product creator": "Product Creator",
        "newcomer": "Newcomer",
    }

    def _normalize_role(self, role: str) -> Optional[str]:
        """Normalize a role string to a canonical form."""
        if not role:
            return None
        cleaned = role.lower().strip()
        # Direct match
        if cleaned in self._ROLE_MAP:
            return self._ROLE_MAP[cleaned]
        # Substring match
        for key, canonical in self._ROLE_MAP.items():
            if key in cleaned:
                return canonical
        return None

    # -------------------------------------------------------------------
    # Gap analysis helpers
    # -------------------------------------------------------------------

    def _find_missing_high_value_roles(
        self,
        dominant_role: str,
        role_dist: dict[str, int],
        total: int,
    ) -> list[str]:
        """Find high-compatibility roles that are underrepresented."""
        missing = []
        # Get high-value partners for the dominant role
        high_value = HIGH_VALUE_ROLE_PAIRS.get(dominant_role, [])
        for partner_role in high_value:
            count = role_dist.get(partner_role, 0)
            # "Missing" = fewer than 3 profiles OR less than 5% of niche
            if count < 3 or (count / max(total, 1)) < 0.05:
                missing.append(partner_role)
        return missing

    def _shannon_diversity(self, counter: Counter) -> float:
        """Compute normalized Shannon diversity index (0-1)."""
        total = sum(counter.values())
        if total == 0:
            return 0.0
        n_categories = len(counter)
        if n_categories <= 1:
            return 0.0
        entropy = 0.0
        for count in counter.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        max_entropy = math.log2(n_categories)
        return entropy / max_entropy if max_entropy > 0 else 0.0

    def _niche_supply_demand_balance(
        self,
        niche: str,
        seeking_keywords: Counter,
        offering_keywords: Counter,
    ) -> float:
        """Compute supply-demand balance score for a niche (0-1).

        1.0 = perfectly balanced, 0.0 = completely one-sided.
        """
        # Use niche name words as proxy for niche-relevant keywords
        niche_words = set(niche.replace("_", " ").split())
        if not niche_words:
            return 0.5  # Neutral

        total_seek = 0
        total_offer = 0
        for word in niche_words:
            if len(word) >= self.MIN_KEYWORD_LEN:
                total_seek += seeking_keywords.get(word, 0)
                total_offer += offering_keywords.get(word, 0)

        if total_seek == 0 and total_offer == 0:
            return 0.5
        total = total_seek + total_offer
        # Balance = 1 - |seek_proportion - 0.5| * 2
        seek_prop = total_seek / total
        return 1.0 - abs(seek_prop - 0.5) * 2

    # -------------------------------------------------------------------
    # Stability analysis
    # -------------------------------------------------------------------

    def _stability_check(self, niche_counts: dict[str, int]) -> list[str]:
        """Check if niche distribution is dominated by a single source."""
        warnings = []
        total = sum(niche_counts.values())
        if total == 0:
            return ["No enriched profiles found for analysis."]

        # Check for source concentration
        source_counts = Counter()
        for profile in self.profiles:
            source = (profile.get(self.source_field) or "unknown").strip()
            source_counts[source] += 1

        for source, count in source_counts.most_common(3):
            proportion = count / total
            if proportion > 0.5:
                warnings.append(
                    f"Source '{source}' contributes {proportion:.0%} of enriched "
                    f"profiles ({count}/{total}). Gap analysis may reflect this "
                    f"source's bias rather than true market demand."
                )

        # Check for niche concentration
        if niche_counts:
            top_niche, top_count = max(niche_counts.items(), key=lambda x: x[1])
            proportion = top_count / total
            if proportion > 0.3:
                warnings.append(
                    f"Niche '{top_niche}' has {proportion:.0%} of profiles "
                    f"({top_count}/{total}). Consider whether this reflects "
                    f"market reality or collection bias."
                )

        return warnings

    def _recommend_new_scrapers(
        self,
        gaps: list[SupplyDemandGap],
        role_gaps: list[RoleGap],
        health_scores: list[NicheHealth],
    ) -> list[dict]:
        """Recommend new scraper sources for gaps that existing scrapers can't fill.

        Uses the scraper_generator module to match uncovered gaps against a
        curated registry of potential data sources.
        """
        try:
            from scripts.sourcing.scraper_generator import generate_scraper_recommendations
            from scripts.sourcing.runner import _register_scrapers, SCRAPER_REGISTRY

            _register_scrapers()
            existing_names = set(SCRAPER_REGISTRY.keys())

            scraper_metadata = {}
            for name, cls in SCRAPER_REGISTRY.items():
                scraper_metadata[name] = {
                    "typical_roles": getattr(cls, "TYPICAL_ROLES", []),
                    "typical_niches": getattr(cls, "TYPICAL_NICHES", []),
                    "typical_offerings": getattr(cls, "TYPICAL_OFFERINGS", []),
                }

            # Build gap data dict from the computed gaps
            gap_data = {
                "supply_demand_gaps": [
                    {
                        "keyword": g.keyword,
                        "seeking_count": g.seeking_count,
                        "offering_count": g.offering_count,
                        "gap_ratio": g.gap_ratio,
                        "gap_type": g.gap_type,
                    }
                    for g in gaps
                ],
                "role_gaps": [
                    {
                        "niche": g.canonical_niche,
                        "missing_high_value_roles": g.missing_high_value_roles,
                    }
                    for g in role_gaps
                ],
                "niche_health": [
                    {
                        "niche": h.canonical_niche,
                        "health_score": h.health_score,
                    }
                    for h in health_scores
                ],
            }

            return generate_scraper_recommendations(
                gap_data, scraper_metadata, existing_names, max_recommendations=5,
            )

        except ImportError:
            # Scraper generator not available (e.g., in test environments)
            return []
        except Exception:
            # Don't let recommendations crash the analysis
            return []


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_gap_report(
    report: MarketIntelligenceReport,
    output_dir: str,
) -> tuple[str, str]:
    """Write gap analysis to JSON and Markdown files.

    Returns (json_path, md_path).
    """
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, "gap_report.json")
    md_path = os.path.join(output_dir, "gap_report.md")

    # JSON
    with open(json_path, "w") as f:
        json.dump(report.to_dict(), f, indent=2, default=str)

    # Markdown
    lines = [
        f"# Market Intelligence Report",
        f"",
        f"**Generated:** {report.computed_at}",
        f"**Enriched profiles analyzed:** {report.enriched_profile_count:,}",
        f"**Canonical niches identified:** {report.canonical_niche_count}",
        f"",
    ]

    # Warnings
    if report.stability_warnings:
        lines.append("## Stability Warnings")
        lines.append("")
        for w in report.stability_warnings:
            lines.append(f"- {w}")
        lines.append("")

    # Top supply-demand gaps
    high_demand = [g for g in report.supply_demand_gaps if g.gap_type == "high_demand"]
    if high_demand:
        lines.append("## Top Unmet Demand (gap ratio > 3.0)")
        lines.append("")
        lines.append("| Keyword | Seeking | Offering | Gap Ratio |")
        lines.append("|---------|---------|----------|-----------|")
        for g in high_demand[:20]:
            lines.append(
                f"| {g.keyword} | {g.seeking_count} | {g.offering_count} | "
                f"{g.gap_ratio:.1f}x |"
            )
        lines.append("")

    # Oversupplied keywords
    oversupplied = [g for g in report.supply_demand_gaps if g.gap_type == "oversupplied"]
    if oversupplied:
        lines.append("## Oversupplied (gap ratio < 0.3)")
        lines.append("")
        lines.append("| Keyword | Seeking | Offering | Gap Ratio |")
        lines.append("|---------|---------|----------|-----------|")
        for g in oversupplied[:10]:
            lines.append(
                f"| {g.keyword} | {g.seeking_count} | {g.offering_count} | "
                f"{g.gap_ratio:.1f}x |"
            )
        lines.append("")

    # Structural role gaps
    gaps_with_missing = [g for g in report.role_gaps if g.missing_high_value_roles]
    if gaps_with_missing:
        lines.append("## Structural Role Gaps by Niche")
        lines.append("")
        for g in gaps_with_missing[:15]:
            roles_str = ", ".join(
                f"{r}: {c}" for r, c in sorted(
                    g.role_distribution.items(), key=lambda x: -x[1]
                )
            )
            missing_str = ", ".join(g.missing_high_value_roles)
            lines.append(f"### {g.canonical_niche} ({g.total_profiles} profiles)")
            lines.append(f"- **Roles:** {roles_str}")
            lines.append(
                f"- **Dominant:** {g.dominant_role} ({g.dominance_ratio:.0%})"
            )
            lines.append(f"- **Missing high-value partners:** {missing_str}")
            lines.append("")

    # Niche health
    if report.niche_health:
        lines.append("## Niche Ecosystem Health")
        lines.append("")
        lines.append("| Niche | Profiles | Role Diversity | Balance | Health |")
        lines.append("|-------|----------|---------------|---------|--------|")
        for h in report.niche_health[:20]:
            lines.append(
                f"| {h.canonical_niche} | {h.total_profiles} | "
                f"{h.role_diversity:.2f} | {h.supply_demand_balance:.2f} | "
                f"{h.health_score:.0f}/100 |"
            )
        lines.append("")

    # Unmapped niches
    if report.unmapped_niches:
        lines.append("## Unmapped Niches (vocabulary expansion candidates)")
        lines.append("")
        for niche, count in report.unmapped_niches[:15]:
            lines.append(f"- \"{niche}\" ({count} profiles)")
        lines.append("")

    # Recommended new scrapers
    if report.recommended_new_scrapers:
        lines.append("## Recommended New Scrapers")
        lines.append("")
        lines.append("These data sources could help fill gaps that no existing scraper covers:")
        lines.append("")
        for rec in report.recommended_new_scrapers:
            lines.append(f"### {rec['display_name']}")
            lines.append(f"- **Source:** {rec['base_url']}")
            lines.append(f"- **API type:** {rec['api_type']} | Auth: {rec['auth_type']}")
            lines.append(f"- **Estimated yield:** {rec['estimated_yield']}")
            lines.append(f"- **Gap score:** {rec['gap_score']}")
            lines.append(f"- **Gaps targeted:** {', '.join(rec['gaps_targeted'])}")
            lines.append(f"- **Scaffold:** `{rec['generate_command']}`")
            if rec.get("ai_create_command"):
                lines.append(f"- **AI-build:** `{rec['ai_create_command']}`")
            lines.append("")

    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    return json_path, md_path
