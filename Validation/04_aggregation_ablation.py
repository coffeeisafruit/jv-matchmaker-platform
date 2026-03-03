#!/usr/bin/env python3
"""
04_aggregation_ablation.py — Ablation Study: Geometric vs Arithmetic Mean Aggregation

Compares the production weighted geometric mean against a weighted arithmetic mean
alternative for ISMC scoring dimensions. The geometric mean penalizes pairs with
one very weak dimension more aggressively; this script quantifies the impact.

Analyses performed:
  1. Re-compute directional scores (geometric + arithmetic) with sanity check
  2. Score divergence distribution
  3. Top 100 most divergent pairs
  4. "Weak link" analysis (geometric < arithmetic by >5 pts)
  5. Tier reclassification impact
  6. Spearman rank correlation
  7. Controlled sensitivity scenarios

Usage:
  python scripts/validation/04_aggregation_ablation.py          # Live database
  python scripts/validation/04_aggregation_ablation.py --test   # Synthetic data
"""

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ── Django bootstrap ────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

from scipy import stats as scipy_stats

from matching.models import SupabaseMatch

# ── Constants ───────────────────────────────────────────────────────
WEIGHTS = {
    'intent': 0.45,
    'synergy': 0.25,
    'momentum': 0.20,
    'context': 0.10,
}
DIMENSIONS = ['intent', 'synergy', 'momentum', 'context']

TIER_THRESHOLDS = {
    'premier': 67,
    'strong': 55,
    'aligned': 0,
}

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'validation_results')
PLOTS_DIR = os.path.join(RESULTS_DIR, 'plots')

EPSILON = 1e-10  # Avoid log(0)


# ── Aggregation functions ──────────────────────────────────────────

def _normalize_weights(available_dims: List[str]) -> Dict[str, float]:
    """Return normalized weights for available (non-null) dimensions."""
    raw = {d: WEIGHTS[d] for d in available_dims}
    total = sum(raw.values())
    if total == 0:
        return {}
    return {d: w / total for d, w in raw.items()}


def weighted_geometric_mean(scores: Dict[str, Optional[float]]) -> Optional[float]:
    """
    Compute weighted geometric mean:  exp( sum(w_i * log(s_i)) / sum(w_i) )

    Skips null dimensions and redistributes weights.
    Scores on 0-10 scale; result on 0-10 scale.
    """
    available = {d: s for d, s in scores.items() if s is not None and d in WEIGHTS}
    if not available:
        return None
    nw = _normalize_weights(list(available.keys()))
    log_sum = sum(nw[d] * math.log(max(available[d], EPSILON)) for d in available)
    return math.exp(log_sum)


def weighted_arithmetic_mean(scores: Dict[str, Optional[float]]) -> Optional[float]:
    """
    Compute weighted arithmetic mean:  sum(w_i * s_i) / sum(w_i)

    Skips null dimensions and redistributes weights.
    Scores on 0-10 scale; result on 0-10 scale.
    """
    available = {d: s for d, s in scores.items() if s is not None and d in WEIGHTS}
    if not available:
        return None
    nw = _normalize_weights(list(available.keys()))
    return sum(nw[d] * available[d] for d in available)


def harmonic_mean_of_two(a: float, b: float) -> float:
    """Harmonic mean of two directional scores: 2/(1/a + 1/b)."""
    if a <= 0 or b <= 0:
        return 0.0
    return 2.0 / (1.0 / a + 1.0 / b)


def classify_tier(score: float) -> str:
    """Classify a harmonic_mean score into a tier."""
    if score >= TIER_THRESHOLDS['premier']:
        return 'premier'
    elif score >= TIER_THRESHOLDS['strong']:
        return 'strong'
    else:
        return 'aligned'


# ── Data extraction ────────────────────────────────────────────────

def extract_dimension_scores(breakdown: dict) -> Dict[str, Optional[float]]:
    """Extract per-dimension scores from a breakdown_ab or breakdown_ba dict."""
    scores = {}
    for dim in DIMENSIONS:
        entry = breakdown.get(dim)
        if entry is None:
            scores[dim] = None
        elif isinstance(entry, dict):
            raw = entry.get('score')
            scores[dim] = float(raw) if raw is not None else None
        else:
            # Sometimes stored as bare number
            try:
                scores[dim] = float(entry)
            except (TypeError, ValueError):
                scores[dim] = None
    return scores


def load_match_data() -> List[dict]:
    """Load all matches with valid match_context from the database."""
    matches = SupabaseMatch.objects.filter(
        match_context__isnull=False,
        score_ab__isnull=False,
        score_ba__isnull=False,
        harmonic_mean__isnull=False,
    ).values('id', 'score_ab', 'score_ba', 'harmonic_mean', 'match_context')

    results = []
    for m in matches:
        ctx = m['match_context']
        # match_context may be stored as a JSON string or as a dict
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(ctx, dict):
            continue
        breakdown_ab = ctx.get('breakdown_ab')
        breakdown_ba = ctx.get('breakdown_ba')
        if not breakdown_ab or not breakdown_ba:
            continue

        dim_ab = extract_dimension_scores(breakdown_ab)
        dim_ba = extract_dimension_scores(breakdown_ba)

        # Need at least 2 non-null dimensions in each direction
        if sum(1 for v in dim_ab.values() if v is not None) < 2:
            continue
        if sum(1 for v in dim_ba.values() if v is not None) < 2:
            continue

        results.append({
            'id': str(m['id']),
            'score_ab_stored': float(m['score_ab']),
            'score_ba_stored': float(m['score_ba']),
            'harmonic_mean_stored': float(m['harmonic_mean']),
            'dim_ab': dim_ab,
            'dim_ba': dim_ba,
        })

    return results


def generate_synthetic_data(n: int = 500) -> List[dict]:
    """Generate synthetic match data for --test mode."""
    random.seed(42)
    results = []
    for i in range(n):
        dim_ab = {}
        dim_ba = {}
        for dim in DIMENSIONS:
            # Occasionally make a dimension null (~5% chance)
            if random.random() < 0.05:
                dim_ab[dim] = None
            else:
                dim_ab[dim] = round(random.uniform(1.0, 10.0), 2)

            if random.random() < 0.05:
                dim_ba[dim] = None
            else:
                dim_ba[dim] = round(random.uniform(1.0, 10.0), 2)

        # Ensure at least 2 non-null dimensions
        for d in [dim_ab, dim_ba]:
            non_null = [k for k, v in d.items() if v is not None]
            while len(non_null) < 2:
                fix_dim = random.choice([k for k in DIMENSIONS if d[k] is None])
                d[fix_dim] = round(random.uniform(1.0, 10.0), 2)
                non_null.append(fix_dim)

        # Compute "stored" scores as geometric mean * 10
        geo_ab = weighted_geometric_mean(dim_ab)
        geo_ba = weighted_geometric_mean(dim_ba)
        score_ab = (geo_ab * 10) if geo_ab else 0
        score_ba = (geo_ba * 10) if geo_ba else 0
        hm = harmonic_mean_of_two(score_ab, score_ba) if (score_ab > 0 and score_ba > 0) else 0

        # Introduce some "imbalanced" profiles for interesting divergence
        if i < 50:
            # One very weak dimension
            weak_dim = random.choice(DIMENSIONS)
            dim_ab[weak_dim] = round(random.uniform(0.5, 2.0), 2)
            geo_ab = weighted_geometric_mean(dim_ab)
            score_ab = (geo_ab * 10) if geo_ab else 0
            hm = harmonic_mean_of_two(score_ab, score_ba) if (score_ab > 0 and score_ba > 0) else 0

        results.append({
            'id': f'synthetic-{i:04d}',
            'score_ab_stored': round(score_ab, 2),
            'score_ba_stored': round(score_ba, 2),
            'harmonic_mean_stored': round(hm, 2),
            'dim_ab': dim_ab,
            'dim_ba': dim_ba,
        })

    return results


# ── Analysis engine ────────────────────────────────────────────────

class AblationAnalysis:
    """Runs all ablation analyses and collects results."""

    def __init__(self, data: List[dict], is_test: bool = False):
        self.data = data
        self.is_test = is_test
        self.results: List[dict] = []  # Per-match computed results
        self.report_lines: List[str] = []

    def run_all(self) -> None:
        """Execute all analyses in sequence."""
        self._recompute_scores()
        self._score_divergence_analysis()
        self._top_divergent_pairs()
        self._weak_link_analysis()
        self._tier_reclassification()
        self._rank_correlation()
        self._sensitivity_scenarios()

    # ── 1. Re-compute directional scores ───────────────────────────

    def _recompute_scores(self) -> None:
        self._section("1. DIRECTIONAL SCORE RE-COMPUTATION")

        sanity_match = 0
        sanity_close = 0
        sanity_mismatch = 0
        total = 0

        for m in self.data:
            row = {'id': m['id']}

            # AB direction
            geo_ab_raw = weighted_geometric_mean(m['dim_ab'])
            arith_ab_raw = weighted_arithmetic_mean(m['dim_ab'])
            row['geo_ab'] = (geo_ab_raw * 10) if geo_ab_raw else None
            row['arith_ab'] = (arith_ab_raw * 10) if arith_ab_raw else None

            # BA direction
            geo_ba_raw = weighted_geometric_mean(m['dim_ba'])
            arith_ba_raw = weighted_arithmetic_mean(m['dim_ba'])
            row['geo_ba'] = (geo_ba_raw * 10) if geo_ba_raw else None
            row['arith_ba'] = (arith_ba_raw * 10) if arith_ba_raw else None

            # Harmonic means
            if row['geo_ab'] and row['geo_ba'] and row['geo_ab'] > 0 and row['geo_ba'] > 0:
                row['hm_geo'] = harmonic_mean_of_two(row['geo_ab'], row['geo_ba'])
            else:
                row['hm_geo'] = None

            if row['arith_ab'] and row['arith_ba'] and row['arith_ab'] > 0 and row['arith_ba'] > 0:
                row['hm_arith'] = harmonic_mean_of_two(row['arith_ab'], row['arith_ba'])
            else:
                row['hm_arith'] = None

            # Sanity check: geometric recomputation vs stored
            row['score_ab_stored'] = m['score_ab_stored']
            row['score_ba_stored'] = m['score_ba_stored']
            row['hm_stored'] = m['harmonic_mean_stored']

            if row['geo_ab'] is not None:
                diff_ab = abs(row['geo_ab'] - m['score_ab_stored'])
                if diff_ab < 0.01:
                    sanity_match += 1
                elif diff_ab < 1.0:
                    sanity_close += 1
                else:
                    sanity_mismatch += 1
                total += 1

            # Store dimension scores for later analysis
            row['dim_ab'] = m['dim_ab']
            row['dim_ba'] = m['dim_ba']

            # Divergences (arithmetic - geometric, per direction)
            if row['arith_ab'] is not None and row['geo_ab'] is not None:
                row['div_ab'] = row['arith_ab'] - row['geo_ab']
            else:
                row['div_ab'] = None

            if row['arith_ba'] is not None and row['geo_ba'] is not None:
                row['div_ba'] = row['arith_ba'] - row['geo_ba']
            else:
                row['div_ba'] = None

            # Harmonic mean divergence
            if row['hm_arith'] is not None and row['hm_geo'] is not None:
                row['div_hm'] = row['hm_arith'] - row['hm_geo']
            else:
                row['div_hm'] = None

            self.results.append(row)

        self._line(f"Total matches analyzed: {len(self.results)}")
        self._line(f"Sanity check (geometric recomputation vs stored score_ab):")
        self._line(f"  Exact match (<0.01):     {sanity_match:>6d}  ({sanity_match/max(total,1)*100:.1f}%)")
        self._line(f"  Close match (<1.0):      {sanity_close:>6d}  ({sanity_close/max(total,1)*100:.1f}%)")
        self._line(f"  Mismatch (>=1.0):        {sanity_mismatch:>6d}  ({sanity_mismatch/max(total,1)*100:.1f}%)")
        self._line(f"  Total checked:           {total:>6d}")
        self._line("")

    # ── 2. Score divergence analysis ───────────────────────────────

    def _score_divergence_analysis(self) -> None:
        self._section("2. SCORE DIVERGENCE ANALYSIS (arithmetic - geometric)")

        div_ab_vals = [r['div_ab'] for r in self.results if r['div_ab'] is not None]
        div_ba_vals = [r['div_ba'] for r in self.results if r['div_ba'] is not None]
        div_hm_vals = [r['div_hm'] for r in self.results if r['div_hm'] is not None]
        all_divs = div_ab_vals + div_ba_vals

        for label, vals in [("AB direction", div_ab_vals),
                            ("BA direction", div_ba_vals),
                            ("All directions", all_divs),
                            ("Harmonic mean", div_hm_vals)]:
            if not vals:
                self._line(f"  {label}: no data")
                continue
            self._line(f"  {label} (n={len(vals)}):")
            self._line(f"    Mean:   {statistics.mean(vals):>8.3f}")
            self._line(f"    Median: {statistics.median(vals):>8.3f}")
            self._line(f"    Stdev:  {statistics.stdev(vals) if len(vals) > 1 else 0:>8.3f}")
            self._line(f"    Min:    {min(vals):>8.3f}")
            self._line(f"    Max:    {max(vals):>8.3f}")
            neg_count = sum(1 for v in vals if v < -0.001)
            self._line(f"    Negative (arith < geo): {neg_count} "
                       f"({'UNEXPECTED' if neg_count > 0 else 'expected: 0'})")
            self._line("")

        # Property verification
        non_negative = sum(1 for v in all_divs if v >= -0.001)
        self._line(f"  AM-GM inequality verification:")
        self._line(f"    Divergence >= 0: {non_negative}/{len(all_divs)} "
                   f"({non_negative/max(len(all_divs),1)*100:.1f}%)")
        self._line(f"    (Arithmetic mean >= Geometric mean should hold for all non-negative inputs)")
        self._line("")

    # ── 3. Top 100 most divergent pairs ────────────────────────────

    def _top_divergent_pairs(self) -> None:
        self._section("3. TOP 100 MOST DIVERGENT PAIRS")

        # Sort by max divergence across both directions
        scored = []
        for r in self.results:
            max_div = max(
                r['div_ab'] if r['div_ab'] is not None else 0,
                r['div_ba'] if r['div_ba'] is not None else 0,
            )
            scored.append((max_div, r))
        scored.sort(key=lambda x: x[0], reverse=True)

        top_100 = scored[:100]

        self._line(f"{'Rank':>4}  {'ID':>16}  {'Geo_AB':>7}  {'Arith_AB':>8}  "
                   f"{'Geo_BA':>7}  {'Arith_BA':>8}  {'MaxDiv':>7}  {'Weakest Dim (AB)':>18}")
        self._line("-" * 105)

        for i, (max_div, r) in enumerate(top_100, 1):
            # Find weakest dimension in AB direction
            ab_scores = {d: s for d, s in r['dim_ab'].items() if s is not None}
            weakest_ab = min(ab_scores, key=ab_scores.get) if ab_scores else 'N/A'
            weakest_val = ab_scores.get(weakest_ab, 0)

            self._line(
                f"{i:>4}  {r['id'][:16]:>16}  "
                f"{r['geo_ab'] or 0:>7.2f}  {r['arith_ab'] or 0:>8.2f}  "
                f"{r['geo_ba'] or 0:>7.2f}  {r['arith_ba'] or 0:>8.2f}  "
                f"{max_div:>7.2f}  {weakest_ab:>10} ({weakest_val:.1f})"
            )

        self._line("")

        # Summary of weakest dimensions in top 100
        weakest_counts = Counter()
        for _, r in top_100:
            ab_scores = {d: s for d, s in r['dim_ab'].items() if s is not None}
            if ab_scores:
                weakest_counts[min(ab_scores, key=ab_scores.get)] += 1
            ba_scores = {d: s for d, s in r['dim_ba'].items() if s is not None}
            if ba_scores:
                weakest_counts[min(ba_scores, key=ba_scores.get)] += 1

        self._line("  Weakest dimension frequency (top 100 divergent, both directions):")
        for dim, count in weakest_counts.most_common():
            self._line(f"    {dim:>12}: {count:>4} times")
        self._line("")

    # ── 4. Weak link analysis ──────────────────────────────────────

    def _weak_link_analysis(self) -> None:
        self._section("4. WEAK LINK ANALYSIS (geometric < arithmetic by >5 points)")

        threshold = 5.0
        weak_link_rows = []

        for r in self.results:
            for direction, div_key, dim_key in [
                ('AB', 'div_ab', 'dim_ab'),
                ('BA', 'div_ba', 'dim_ba'),
            ]:
                div = r.get(div_key)
                if div is None or div <= threshold:
                    continue

                dim_scores = {d: s for d, s in r[dim_key].items() if s is not None}
                if not dim_scores:
                    continue

                weakest_dim = min(dim_scores, key=dim_scores.get)
                strongest_dim = max(dim_scores, key=dim_scores.get)
                weakest_val = dim_scores[weakest_dim]
                strongest_val = dim_scores[strongest_dim]
                ratio = strongest_val / max(weakest_val, EPSILON)

                weak_link_rows.append({
                    'id': r['id'],
                    'direction': direction,
                    'divergence': div,
                    'weakest_dim': weakest_dim,
                    'weakest_val': weakest_val,
                    'strongest_dim': strongest_dim,
                    'strongest_val': strongest_val,
                    'ratio': ratio,
                    'dim_scores': dim_scores,
                })

        total_directions = sum(
            2 for r in self.results
            if r.get('div_ab') is not None or r.get('div_ba') is not None
        )
        self._line(f"  Pairs with divergence > {threshold} points: {len(weak_link_rows)} "
                   f"out of {total_directions} direction-pairs "
                   f"({len(weak_link_rows)/max(total_directions,1)*100:.1f}%)")
        self._line("")

        if not weak_link_rows:
            self._line("  No pairs exceed the threshold.")
            self._line("")
            return

        # Weakest dimension distribution
        weak_dim_counts = Counter(r['weakest_dim'] for r in weak_link_rows)
        self._line("  Weakest dimension distribution:")
        for dim, count in weak_dim_counts.most_common():
            self._line(f"    {dim:>12}: {count:>4} ({count/len(weak_link_rows)*100:.1f}%)")
        self._line("")

        # Strongest-to-weakest ratio
        ratios = [r['ratio'] for r in weak_link_rows]
        self._line("  Strongest-to-weakest ratio:")
        self._line(f"    Mean:   {statistics.mean(ratios):.2f}x")
        self._line(f"    Median: {statistics.median(ratios):.2f}x")
        self._line(f"    Max:    {max(ratios):.2f}x")
        self._line("")

        # Show top 10 most extreme weak-link cases
        weak_link_rows.sort(key=lambda x: x['divergence'], reverse=True)
        self._line("  Top 10 most extreme weak-link cases:")
        self._line(f"  {'ID':>16}  {'Dir':>3}  {'Div':>6}  {'Weakest':>10}  "
                   f"{'WVal':>5}  {'Strongest':>10}  {'SVal':>5}  {'Ratio':>6}")
        self._line("  " + "-" * 85)
        for r in weak_link_rows[:10]:
            self._line(
                f"  {r['id'][:16]:>16}  {r['direction']:>3}  {r['divergence']:>6.2f}  "
                f"{r['weakest_dim']:>10}  {r['weakest_val']:>5.1f}  "
                f"{r['strongest_dim']:>10}  {r['strongest_val']:>5.1f}  "
                f"{r['ratio']:>6.1f}x"
            )
        self._line("")

        # Store for plotting
        self._weak_link_data = weak_link_rows

    # ── 5. Tier reclassification ───────────────────────────────────

    def _tier_reclassification(self) -> None:
        self._section("5. TIER RECLASSIFICATION IMPACT")

        geo_tiers = Counter()
        arith_tiers = Counter()
        transitions = Counter()  # (from_tier, to_tier)
        rescued = 0
        penalized = 0
        same = 0

        tier_order = {'aligned': 0, 'strong': 1, 'premier': 2}

        for r in self.results:
            hm_geo = r.get('hm_geo')
            hm_arith = r.get('hm_arith')
            if hm_geo is None or hm_arith is None:
                continue

            tier_geo = classify_tier(hm_geo)
            tier_arith = classify_tier(hm_arith)

            geo_tiers[tier_geo] += 1
            arith_tiers[tier_arith] += 1
            transitions[(tier_geo, tier_arith)] += 1

            if tier_order[tier_arith] > tier_order[tier_geo]:
                rescued += 1
            elif tier_order[tier_arith] < tier_order[tier_geo]:
                penalized += 1
            else:
                same += 1

        total = rescued + penalized + same
        self._line(f"  Total matches with both harmonic means: {total}")
        self._line("")

        # Tier distributions
        self._line("  Tier distribution:")
        self._line(f"    {'Tier':<15}  {'Geometric':>10}  {'Arithmetic':>10}  {'Change':>10}")
        self._line("    " + "-" * 50)
        for tier in ['premier', 'strong', 'aligned']:
            g = geo_tiers.get(tier, 0)
            a = arith_tiers.get(tier, 0)
            diff = a - g
            sign = '+' if diff > 0 else ''
            self._line(f"    {tier:<15}  {g:>10d}  {a:>10d}  {sign}{diff:>9d}")
        self._line("")

        # Reclassification summary
        self._line(f"  Reclassification summary:")
        self._line(f"    Same tier:          {same:>6d}  ({same/max(total,1)*100:.1f}%)")
        self._line(f"    Rescued (tier up):  {rescued:>6d}  ({rescued/max(total,1)*100:.1f}%)")
        self._line(f"    Penalized (down):   {penalized:>6d}  ({penalized/max(total,1)*100:.1f}%)")
        self._line("")

        # Transition matrix
        self._line("  Transition matrix (rows=geometric tier, cols=arithmetic tier):")
        tiers_list = ['aligned', 'strong', 'premier']
        header = f"    {'':>15}" + "".join(f"  {t:>12}" for t in tiers_list)
        self._line(header)
        self._line("    " + "-" * (15 + 14 * len(tiers_list)))
        for from_t in tiers_list:
            row_vals = "".join(f"  {transitions.get((from_t, to_t), 0):>12d}" for to_t in tiers_list)
            self._line(f"    {from_t:>15}{row_vals}")
        self._line("")

        # Specific transitions of interest
        self._line("  Notable transitions:")
        for from_t, to_t, label in [
            ('aligned', 'strong', 'aligned -> strong (rescued)'),
            ('aligned', 'premier', 'aligned -> premier (rescued)'),
            ('strong', 'premier', 'strong -> premier (rescued)'),
            ('premier', 'strong', 'premier -> strong (penalized)'),
            ('strong', 'aligned', 'strong -> aligned (penalized)'),
        ]:
            count = transitions.get((from_t, to_t), 0)
            self._line(f"    {label:<45}: {count:>6d}")
        self._line("")

        # Store for plotting
        self._tier_transitions = transitions
        self._geo_tiers = geo_tiers
        self._arith_tiers = arith_tiers

    # ── 6. Rank correlation ────────────────────────────────────────

    def _rank_correlation(self) -> None:
        self._section("6. SPEARMAN RANK CORRELATION")

        pairs = []
        for r in self.results:
            if r.get('hm_geo') is not None and r.get('hm_arith') is not None:
                pairs.append((r['hm_geo'], r['hm_arith']))

        if len(pairs) < 3:
            self._line("  Insufficient data for rank correlation (need >= 3 pairs).")
            self._line("")
            return

        geo_scores = [p[0] for p in pairs]
        arith_scores = [p[1] for p in pairs]

        rho, p_value = scipy_stats.spearmanr(geo_scores, arith_scores)

        self._line(f"  Pairs compared: {len(pairs)}")
        self._line(f"  Spearman rho:   {rho:.6f}")
        self._line(f"  P-value:        {p_value:.2e}")
        self._line("")

        if rho > 0.99:
            interpretation = "Very high agreement — aggregation method barely affects ranking."
        elif rho > 0.95:
            interpretation = "High agreement — minor ranking differences."
        elif rho > 0.90:
            interpretation = "Moderate agreement — noticeable ranking shifts for some pairs."
        else:
            interpretation = "Low agreement — aggregation method significantly impacts recommendations."

        self._line(f"  Interpretation: {interpretation}")
        self._line("")

        # Also compute Pearson for comparison
        pearson_r, pearson_p = scipy_stats.pearsonr(geo_scores, arith_scores)
        self._line(f"  Pearson r (linear correlation): {pearson_r:.6f}  (p={pearson_p:.2e})")
        self._line("")

    # ── 7. Sensitivity to single weak dimension ────────────────────

    def _sensitivity_scenarios(self) -> None:
        self._section("7. SENSITIVITY TO SINGLE WEAK DIMENSION (Controlled Scenarios)")

        scenarios = [
            ("One weak (9,9,9,2)", {'intent': 9, 'synergy': 9, 'momentum': 9, 'context': 2}),
            ("Moderate weak (9,9,9,5)", {'intent': 9, 'synergy': 9, 'momentum': 9, 'context': 5}),
            ("Balanced (7,7,7,7)", {'intent': 7, 'synergy': 7, 'momentum': 7, 'context': 7}),
            ("Two strong two weak (10,10,1,1)", {'intent': 10, 'synergy': 10, 'momentum': 1, 'context': 1}),
            ("Extreme imbalance (10,10,10,1)", {'intent': 10, 'synergy': 10, 'momentum': 10, 'context': 1}),
            ("All high (10,10,10,10)", {'intent': 10, 'synergy': 10, 'momentum': 10, 'context': 10}),
            ("All low (2,2,2,2)", {'intent': 2, 'synergy': 2, 'momentum': 2, 'context': 2}),
            ("Weak intent (2,9,9,9)", {'intent': 2, 'synergy': 9, 'momentum': 9, 'context': 9}),
        ]

        self._line(f"  {'Scenario':<30}  {'I':>4} {'S':>4} {'M':>4} {'C':>4}  "
                   f"{'Geo (0-100)':>11}  {'Arith (0-100)':>13}  {'Diff':>6}  {'Diff%':>6}")
        self._line("  " + "-" * 105)

        self._sensitivity_data = []

        for label, scores in scenarios:
            geo = weighted_geometric_mean(scores)
            arith = weighted_arithmetic_mean(scores)
            geo_100 = (geo * 10) if geo else 0
            arith_100 = (arith * 10) if arith else 0
            diff = arith_100 - geo_100
            diff_pct = (diff / max(geo_100, EPSILON)) * 100

            self._line(
                f"  {label:<30}  {scores['intent']:>4.0f} {scores['synergy']:>4.0f} "
                f"{scores['momentum']:>4.0f} {scores['context']:>4.0f}  "
                f"{geo_100:>11.2f}  {arith_100:>13.2f}  {diff:>6.2f}  {diff_pct:>5.1f}%"
            )

            self._sensitivity_data.append({
                'label': label,
                'scores': scores,
                'geo': geo_100,
                'arith': arith_100,
                'diff': diff,
            })

        self._line("")
        self._line("  Key observations:")
        self._line("  - Balanced scores: geometric == arithmetic (no penalty)")
        self._line("  - One weak dimension: geometric penalizes proportional to weakness severity")
        self._line("  - Weak high-weight dim (intent): larger penalty than weak low-weight dim (context)")
        self._line("  - Extreme imbalance: geometric can be dramatically lower than arithmetic")
        self._line("")

    # ── Report writing helpers ─────────────────────────────────────

    def _section(self, title: str) -> None:
        self._line("")
        self._line("=" * 80)
        self._line(f"  {title}")
        self._line("=" * 80)
        self._line("")

    def _line(self, text: str = "") -> None:
        self.report_lines.append(text)

    def get_report(self) -> str:
        header = [
            "=" * 80,
            "  AGGREGATION ABLATION STUDY",
            "  Weighted Geometric Mean vs Weighted Arithmetic Mean",
            "=" * 80,
            "",
            f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Data source: {'SYNTHETIC (--test mode)' if self.is_test else 'Production database'}",
            f"  Matches analyzed: {len(self.results)}",
            f"  Weights: intent={WEIGHTS['intent']}, synergy={WEIGHTS['synergy']}, "
            f"momentum={WEIGHTS['momentum']}, context={WEIGHTS['context']}",
            "",
        ]
        return "\n".join(header + self.report_lines)


# ── Visualization ──────────────────────────────────────────────────

class AblationPlotter:
    """Generate all visualization plots for the ablation study."""

    def __init__(self, analysis: AblationAnalysis):
        self.analysis = analysis
        self.results = analysis.results
        plt.style.use('seaborn-v0_8-whitegrid')
        if HAS_SEABORN:
            sns.set_palette('deep')

    def generate_all(self) -> None:
        os.makedirs(PLOTS_DIR, exist_ok=True)
        self._geometric_vs_arithmetic_scatter()
        self._divergence_distribution()
        self._weak_link_heatmap()
        self._tier_reclassification_bar()
        self._sensitivity_comparison()

    def _geometric_vs_arithmetic_scatter(self) -> None:
        """Scatter plot of geometric vs arithmetic scores, colored by divergence."""
        geo_vals = []
        arith_vals = []
        div_vals = []

        for r in self.results:
            for geo_key, arith_key, div_key in [
                ('geo_ab', 'arith_ab', 'div_ab'),
                ('geo_ba', 'arith_ba', 'div_ba'),
            ]:
                g = r.get(geo_key)
                a = r.get(arith_key)
                d = r.get(div_key)
                if g is not None and a is not None and d is not None:
                    geo_vals.append(g)
                    arith_vals.append(a)
                    div_vals.append(d)

        if not geo_vals:
            return

        fig, ax = plt.subplots(figsize=(10, 8))

        scatter = ax.scatter(
            geo_vals, arith_vals,
            c=div_vals, cmap='RdYlGn_r', alpha=0.5, s=15, edgecolors='none',
        )
        cbar = plt.colorbar(scatter, ax=ax, label='Divergence (arith - geo)')

        # Identity line
        lims = [0, max(max(geo_vals), max(arith_vals)) * 1.05]
        ax.plot(lims, lims, 'k--', alpha=0.3, linewidth=1, label='y = x (no divergence)')

        ax.set_xlabel('Geometric Mean Score (0-100)', fontsize=12)
        ax.set_ylabel('Arithmetic Mean Score (0-100)', fontsize=12)
        ax.set_title('Geometric vs Arithmetic Mean: Directional Scores', fontsize=14)
        ax.legend(loc='upper left')
        ax.set_xlim(lims)
        ax.set_ylim(lims)

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'geometric_vs_arithmetic_scatter.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    def _divergence_distribution(self) -> None:
        """Histogram of (arithmetic - geometric) divergence values."""
        div_all = []
        for r in self.results:
            for key in ('div_ab', 'div_ba'):
                v = r.get(key)
                if v is not None:
                    div_all.append(v)

        if not div_all:
            return

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Left: histogram
        ax = axes[0]
        bins = min(80, max(20, len(div_all) // 20))
        ax.hist(div_all, bins=bins, color='steelblue', edgecolor='white', alpha=0.8)
        ax.axvline(x=0, color='red', linestyle='--', alpha=0.5, label='Zero divergence')
        mean_div = statistics.mean(div_all)
        ax.axvline(x=mean_div, color='orange', linestyle='-', alpha=0.7,
                   label=f'Mean = {mean_div:.2f}')
        ax.set_xlabel('Divergence (arithmetic - geometric)', fontsize=11)
        ax.set_ylabel('Count', fontsize=11)
        ax.set_title('Distribution of Score Divergence', fontsize=13)
        ax.legend()

        # Right: CDF
        ax2 = axes[1]
        sorted_div = sorted(div_all)
        cdf_y = np.arange(1, len(sorted_div) + 1) / len(sorted_div)
        ax2.plot(sorted_div, cdf_y, color='steelblue', linewidth=2)
        ax2.axvline(x=5.0, color='red', linestyle='--', alpha=0.5,
                    label='5-point threshold')
        ax2.set_xlabel('Divergence (arithmetic - geometric)', fontsize=11)
        ax2.set_ylabel('Cumulative Proportion', fontsize=11)
        ax2.set_title('CDF of Score Divergence', fontsize=13)
        ax2.legend()

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'divergence_distribution.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    def _weak_link_heatmap(self) -> None:
        """Heatmap showing how often each dimension is the weakest link."""
        # Count weakest dimension per direction across all matches
        weakest_counts = {d: Counter() for d in DIMENSIONS}  # dim -> {other_dim: count}
        total_weakest = Counter()

        for r in self.results:
            for dim_key in ('dim_ab', 'dim_ba'):
                dim_scores = {d: s for d, s in r[dim_key].items() if s is not None}
                if not dim_scores:
                    continue
                weakest = min(dim_scores, key=dim_scores.get)
                total_weakest[weakest] += 1

                # Also track what the strongest dimension was when this was weakest
                strongest = max(dim_scores, key=dim_scores.get)
                weakest_counts[weakest][strongest] += 1

        # Build matrix: rows=weakest dim, cols=strongest dim when that was weakest
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Left: bar chart of weakest dimension frequency
        ax = axes[0]
        dims_sorted = sorted(DIMENSIONS, key=lambda d: total_weakest.get(d, 0), reverse=True)
        counts = [total_weakest.get(d, 0) for d in dims_sorted]
        colors = ['#e74c3c', '#e67e22', '#f1c40f', '#2ecc71']
        bars = ax.bar(dims_sorted, counts, color=colors[:len(dims_sorted)], edgecolor='white')
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    str(count), ha='center', va='bottom', fontsize=10)
        ax.set_xlabel('Dimension', fontsize=11)
        ax.set_ylabel('Times as Weakest Link', fontsize=11)
        ax.set_title('Weakest Link Frequency by Dimension', fontsize=13)

        # Right: heatmap matrix (weakest x strongest)
        ax2 = axes[1]
        matrix = np.zeros((len(DIMENSIONS), len(DIMENSIONS)))
        for i, weak in enumerate(DIMENSIONS):
            for j, strong in enumerate(DIMENSIONS):
                matrix[i, j] = weakest_counts[weak].get(strong, 0)

        if HAS_SEABORN:
            sns.heatmap(matrix, ax=ax2, annot=True, fmt='.0f', cmap='YlOrRd',
                       xticklabels=[d.capitalize() for d in DIMENSIONS],
                       yticklabels=[d.capitalize() for d in DIMENSIONS])
        else:
            im = ax2.imshow(matrix, cmap='YlOrRd', aspect='auto')
            ax2.set_xticks(range(len(DIMENSIONS)))
            ax2.set_xticklabels([d.capitalize() for d in DIMENSIONS])
            ax2.set_yticks(range(len(DIMENSIONS)))
            ax2.set_yticklabels([d.capitalize() for d in DIMENSIONS])
            for i in range(len(DIMENSIONS)):
                for j in range(len(DIMENSIONS)):
                    ax2.text(j, i, f'{matrix[i, j]:.0f}', ha='center', va='center', fontsize=10)
            plt.colorbar(im, ax=ax2)

        ax2.set_xlabel('Strongest Dimension', fontsize=11)
        ax2.set_ylabel('Weakest Dimension', fontsize=11)
        ax2.set_title('Weakest vs Strongest Dimension Co-occurrence', fontsize=13)

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'weak_link_heatmap.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    def _tier_reclassification_bar(self) -> None:
        """Grouped bar chart showing tier changes between methods."""
        transitions = getattr(self.analysis, '_tier_transitions', Counter())
        if not transitions:
            return

        tiers = ['aligned', 'strong', 'premier']
        tier_labels = ['Aligned (<55)', 'Strong (55-66)', 'Premier (67+)']

        fig, ax = plt.subplots(figsize=(12, 6))

        x = np.arange(len(tiers))
        width = 0.22

        for i, to_tier in enumerate(tiers):
            counts = [transitions.get((from_t, to_tier), 0) for from_t in tiers]
            offset = (i - 1) * width
            bars = ax.bar(x + offset, counts, width, label=f'-> {tier_labels[i]}',
                         alpha=0.85)
            for bar, count in zip(bars, counts):
                if count > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                            str(count), ha='center', va='bottom', fontsize=9)

        ax.set_xlabel('Geometric Mean Tier (Original)', fontsize=12)
        ax.set_ylabel('Number of Matches', fontsize=12)
        ax.set_title('Tier Reclassification: Geometric -> Arithmetic', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(tier_labels)
        ax.legend(title='Arithmetic Tier (New)')

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'tier_reclassification_bar.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    def _sensitivity_comparison(self) -> None:
        """Bar chart comparing geometric vs arithmetic for controlled scenarios."""
        sens_data = getattr(self.analysis, '_sensitivity_data', [])
        if not sens_data:
            return

        fig, ax = plt.subplots(figsize=(14, 7))

        labels = [d['label'] for d in sens_data]
        geo_scores = [d['geo'] for d in sens_data]
        arith_scores = [d['arith'] for d in sens_data]

        x = np.arange(len(labels))
        width = 0.35

        bars_geo = ax.bar(x - width / 2, geo_scores, width, label='Geometric Mean',
                          color='#3498db', edgecolor='white')
        bars_arith = ax.bar(x + width / 2, arith_scores, width, label='Arithmetic Mean',
                            color='#e74c3c', edgecolor='white')

        # Add value labels
        for bar in bars_geo:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=8)
        for bar in bars_arith:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=8)

        ax.set_xlabel('Scenario', fontsize=12)
        ax.set_ylabel('Score (0-100 scale)', fontsize=12)
        ax.set_title('Sensitivity Analysis: Geometric vs Arithmetic Mean', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=30, ha='right', fontsize=9)
        ax.legend()
        ax.set_ylim(0, max(max(geo_scores), max(arith_scores)) * 1.15)

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'sensitivity_comparison.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")


# ── CSV export ─────────────────────────────────────────────────────

def export_csv(results: List[dict], path: str) -> None:
    """Export per-match results to CSV."""
    if not results:
        return

    fieldnames = [
        'id',
        'intent_ab', 'synergy_ab', 'momentum_ab', 'context_ab',
        'intent_ba', 'synergy_ba', 'momentum_ba', 'context_ba',
        'geo_ab', 'arith_ab', 'div_ab',
        'geo_ba', 'arith_ba', 'div_ba',
        'hm_geo', 'hm_arith', 'div_hm',
        'score_ab_stored', 'score_ba_stored', 'hm_stored',
        'tier_geo', 'tier_arith',
    ]

    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in results:
            row = {
                'id': r['id'],
                'geo_ab': _fmt(r.get('geo_ab')),
                'arith_ab': _fmt(r.get('arith_ab')),
                'div_ab': _fmt(r.get('div_ab')),
                'geo_ba': _fmt(r.get('geo_ba')),
                'arith_ba': _fmt(r.get('arith_ba')),
                'div_ba': _fmt(r.get('div_ba')),
                'hm_geo': _fmt(r.get('hm_geo')),
                'hm_arith': _fmt(r.get('hm_arith')),
                'div_hm': _fmt(r.get('div_hm')),
                'score_ab_stored': _fmt(r.get('score_ab_stored')),
                'score_ba_stored': _fmt(r.get('score_ba_stored')),
                'hm_stored': _fmt(r.get('hm_stored')),
                'tier_geo': classify_tier(r['hm_geo']) if r.get('hm_geo') is not None else '',
                'tier_arith': classify_tier(r['hm_arith']) if r.get('hm_arith') is not None else '',
            }
            # Dimension scores
            for dim in DIMENSIONS:
                row[f'{dim}_ab'] = _fmt(r.get('dim_ab', {}).get(dim))
                row[f'{dim}_ba'] = _fmt(r.get('dim_ba', {}).get(dim))
            writer.writerow(row)


def _fmt(val) -> str:
    """Format a value for CSV output."""
    if val is None:
        return ''
    if isinstance(val, float):
        return f'{val:.4f}'
    return str(val)


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ablation study: geometric vs arithmetic mean aggregation'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Use synthetic data instead of database'
    )
    args = parser.parse_args()

    random.seed(42)

    print("=" * 60)
    print("  Aggregation Ablation Study")
    print("  Geometric Mean vs Arithmetic Mean")
    print("=" * 60)
    print()

    # Load data
    if args.test:
        print("Using SYNTHETIC data (--test mode)")
        data = generate_synthetic_data(n=500)
    else:
        print("Loading match data from database...")
        data = load_match_data()

    print(f"Loaded {len(data)} matches with valid dimension scores.")
    print()

    if not data:
        print("ERROR: No matches found with valid match_context data.")
        print("  Try running with --test for synthetic data.")
        sys.exit(1)

    # Run analyses
    analysis = AblationAnalysis(data, is_test=args.test)
    analysis.run_all()

    # Create output directories
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)

    # Write report
    report_path = os.path.join(RESULTS_DIR, 'aggregation_ablation_report.txt')
    report_text = analysis.get_report()
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"\nReport saved: {report_path}")

    # Export CSV
    csv_path = os.path.join(RESULTS_DIR, 'aggregation_ablation_data.csv')
    export_csv(analysis.results, csv_path)
    print(f"Data saved:   {csv_path}")

    # Generate plots
    print("\nGenerating plots...")
    plotter = AblationPlotter(analysis)
    plotter.generate_all()

    # Print summary to stdout
    print("\n" + report_text)
    print("\nDone.")


if __name__ == '__main__':
    main()
