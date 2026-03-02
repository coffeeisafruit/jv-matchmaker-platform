#!/usr/bin/env python3
"""
03_bidirectional_analysis.py — Bidirectional Harmonic Mean Validation

Validates the bidirectional harmonic mean design choice in the ISMC matching
algorithm by analyzing asymmetry patterns and comparing aggregation methods.

Analyses performed:
  1. Asymmetry distribution of |score_ab - score_ba|
  2. Aggregation method comparison (harmonic, arithmetic, geometric, min)
  3. Harmonic mean penalty quantification for highly asymmetric pairs
  4. Penalty by asymmetry bucket
  5. Rank correlation between aggregation methods (Spearman rho)
  6. Tier impact analysis across aggregation methods
  7. Per-dimension directionality source analysis
  8. Mathematical properties documentation

Usage:
  python scripts/validation/03_bidirectional_analysis.py          # production data
  python scripts/validation/03_bidirectional_analysis.py --test   # synthetic data
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django  # noqa: E402

django.setup()

import numpy as np  # noqa: E402
import matplotlib  # noqa: E402

matplotlib.use('Agg')  # non-interactive backend for headless environments

import matplotlib.pyplot as plt  # noqa: E402
import seaborn as sns  # noqa: E402
from scipy import stats as sp_stats  # noqa: E402

from matching.models import SupabaseMatch  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RESULTS_DIR = Path(__file__).resolve().parent / 'validation_results'
PLOTS_DIR = RESULTS_DIR / 'plots'

TIER_THRESHOLDS = {
    'hand_picked': 67,
    'strong': 55,
    'wildcard': 0,
}

ISMC_DIMENSIONS = ['intent', 'synergy', 'momentum', 'context']

AGGREGATION_METHODS = ['harmonic', 'arithmetic', 'geometric', 'min']

ASYMMETRY_BUCKETS = [
    (0, 5, '0-5'),
    (5, 10, '5-10'),
    (10, 15, '10-15'),
    (15, 20, '15-20'),
    (20, float('inf'), '20+'),
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class MatchPair(NamedTuple):
    """Flattened representation of a scored match pair."""
    score_ab: float
    score_ba: float
    harmonic_mean: float
    match_context: dict | None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_production_data() -> list[MatchPair]:
    """Load match pairs from the SupabaseMatch table via Django ORM."""
    qs = SupabaseMatch.objects.filter(
        score_ab__isnull=False,
        score_ba__isnull=False,
    ).values_list('score_ab', 'score_ba', 'harmonic_mean', 'match_context')

    pairs: list[MatchPair] = []
    for score_ab, score_ba, harmonic_mean, match_context in qs.iterator():
        ab = float(score_ab) if score_ab is not None else None
        ba = float(score_ba) if score_ba is not None else None
        hm = float(harmonic_mean) if harmonic_mean is not None else None

        if ab is None or ba is None:
            continue

        # Parse match_context — may be a dict, a JSON string, or None
        ctx = match_context
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                ctx = None

        pairs.append(MatchPair(
            score_ab=ab,
            score_ba=ba,
            harmonic_mean=hm if hm is not None else 0.0,
            match_context=ctx if isinstance(ctx, dict) else None,
        ))

    return pairs


def generate_synthetic_data(n: int = 500, seed: int = 42) -> list[MatchPair]:
    """Generate synthetic match pairs with realistic asymmetry patterns.

    Simulates the ISMC scoring distribution:
      - Scores roughly centered around 55-60 with stdev ~8
      - Moderate directional asymmetry (mean ~10 points)
      - A tail of highly asymmetric pairs
    """
    random.seed(seed)
    np.random.seed(seed)

    pairs: list[MatchPair] = []
    for _ in range(n):
        # Base quality — shared pair baseline
        base = np.clip(np.random.normal(57, 8), 10, 95)

        # Directional offsets — asymmetry comes from offering/seeking mismatch
        offset_ab = np.random.normal(0, 7)
        offset_ba = np.random.normal(0, 7)

        ab = float(np.clip(base + offset_ab, 1, 100))
        ba = float(np.clip(base + offset_ba, 1, 100))

        epsilon = 1e-10
        if ab > epsilon and ba > epsilon:
            hm = 2.0 / (1.0 / ab + 1.0 / ba)
        else:
            hm = 0.0

        # Build synthetic match_context with per-dimension breakdown
        ctx = _build_synthetic_context(ab, ba)
        pairs.append(MatchPair(score_ab=ab, score_ba=ba, harmonic_mean=hm, match_context=ctx))

    return pairs


def _build_synthetic_context(score_ab: float, score_ba: float) -> dict:
    """Build a plausible match_context dict with ISMC dimension breakdowns.

    Synergy is made deliberately more asymmetric (offering/seeking is
    directional), while Intent and Context are more symmetric.
    """
    def dim_score(base_10: float, asymmetry_scale: float) -> tuple[float, float]:
        """Return (score_for_ab, score_for_ba) on 0-10 scale."""
        noise_a = np.random.normal(0, asymmetry_scale)
        noise_b = np.random.normal(0, asymmetry_scale)
        return (
            float(np.clip(base_10 + noise_a, 0, 10)),
            float(np.clip(base_10 + noise_b, 0, 10)),
        )

    base = (score_ab + score_ba) / 20.0  # rough 0-10 baseline

    # Intent — relatively symmetric (both sides show intent independently)
    intent_ab, intent_ba = dim_score(base * 1.05, 0.5)
    # Synergy — deliberately asymmetric (offering-seeking mismatch)
    synergy_ab, synergy_ba = dim_score(base * 0.95, 1.8)
    # Momentum — moderate asymmetry
    momentum_ab, momentum_ba = dim_score(base * 0.90, 1.0)
    # Context — symmetric (market conditions affect both sides equally)
    context_ab, context_ba = dim_score(base * 1.0, 0.4)

    return {
        'breakdown_ab': {
            'intent': {'score': round(intent_ab, 2)},
            'synergy': {'score': round(synergy_ab, 2)},
            'momentum': {'score': round(momentum_ab, 2)},
            'context': {'score': round(context_ab, 2)},
        },
        'breakdown_ba': {
            'intent': {'score': round(intent_ba, 2)},
            'synergy': {'score': round(synergy_ba, 2)},
            'momentum': {'score': round(momentum_ba, 2)},
            'context': {'score': round(context_ba, 2)},
        },
    }


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def harmonic_mean(a: float, b: float) -> float:
    """Harmonic mean of two positive values. Returns 0 if either is <= 0."""
    if a <= 0 or b <= 0:
        return 0.0
    return 2.0 / (1.0 / a + 1.0 / b)


def arithmetic_mean(a: float, b: float) -> float:
    return (a + b) / 2.0


def geometric_mean(a: float, b: float) -> float:
    if a <= 0 or b <= 0:
        return 0.0
    return math.sqrt(a * b)


def minimum(a: float, b: float) -> float:
    return min(a, b)


AGGREGATORS = {
    'harmonic': harmonic_mean,
    'arithmetic': arithmetic_mean,
    'geometric': geometric_mean,
    'min': minimum,
}


def compute_all_aggregations(pairs: list[MatchPair]) -> dict[str, list[float]]:
    """Compute all aggregation method scores for every pair."""
    results: dict[str, list[float]] = {name: [] for name in AGGREGATION_METHODS}
    for p in pairs:
        for name, fn in AGGREGATORS.items():
            results[name].append(fn(p.score_ab, p.score_ba))
    return results


# ---------------------------------------------------------------------------
# Analysis 1: Asymmetry distribution
# ---------------------------------------------------------------------------

def analyze_asymmetry(pairs: list[MatchPair]) -> dict[str, Any]:
    """Compute asymmetry = |score_ab - score_ba| distribution statistics."""
    asymmetries = [abs(p.score_ab - p.score_ba) for p in pairs]

    if not asymmetries:
        return {'error': 'No pairs available'}

    percentiles = {
        'p10': float(np.percentile(asymmetries, 10)),
        'p25': float(np.percentile(asymmetries, 25)),
        'p50': float(np.percentile(asymmetries, 50)),
        'p75': float(np.percentile(asymmetries, 75)),
        'p90': float(np.percentile(asymmetries, 90)),
        'p95': float(np.percentile(asymmetries, 95)),
        'p99': float(np.percentile(asymmetries, 99)),
    }

    return {
        'n': len(asymmetries),
        'mean': statistics.mean(asymmetries),
        'median': statistics.median(asymmetries),
        'stdev': statistics.stdev(asymmetries) if len(asymmetries) > 1 else 0.0,
        'min': min(asymmetries),
        'max': max(asymmetries),
        'percentiles': percentiles,
        'raw': asymmetries,
    }


# ---------------------------------------------------------------------------
# Analysis 2: Aggregation method comparison
# ---------------------------------------------------------------------------

def compare_aggregations(agg_scores: dict[str, list[float]]) -> dict[str, dict[str, float]]:
    """Summary statistics for each aggregation method."""
    summaries: dict[str, dict[str, float]] = {}
    for name, scores in agg_scores.items():
        if not scores:
            summaries[name] = {'error': 'No data'}
            continue
        summaries[name] = {
            'mean': statistics.mean(scores),
            'median': statistics.median(scores),
            'stdev': statistics.stdev(scores) if len(scores) > 1 else 0.0,
            'min': min(scores),
            'max': max(scores),
        }
    return summaries


# ---------------------------------------------------------------------------
# Analysis 3: Harmonic mean penalty quantification
# ---------------------------------------------------------------------------

def quantify_penalty(pairs: list[MatchPair], agg_scores: dict[str, list[float]],
                     asymmetry_threshold: float = 20.0) -> dict[str, Any]:
    """For highly asymmetric pairs, compute avg(arithmetic - harmonic)."""
    penalties = []
    for i, p in enumerate(pairs):
        asym = abs(p.score_ab - p.score_ba)
        if asym > asymmetry_threshold:
            arith = agg_scores['arithmetic'][i]
            harm = agg_scores['harmonic'][i]
            penalties.append(arith - harm)

    if not penalties:
        return {
            'n_pairs': 0,
            'message': f'No pairs with asymmetry > {asymmetry_threshold}',
        }

    return {
        'n_pairs': len(penalties),
        'asymmetry_threshold': asymmetry_threshold,
        'mean_penalty': statistics.mean(penalties),
        'median_penalty': statistics.median(penalties),
        'max_penalty': max(penalties),
        'stdev_penalty': statistics.stdev(penalties) if len(penalties) > 1 else 0.0,
    }


# ---------------------------------------------------------------------------
# Analysis 4: Penalty by asymmetry bucket
# ---------------------------------------------------------------------------

def penalty_by_bucket(pairs: list[MatchPair],
                      agg_scores: dict[str, list[float]]) -> list[dict[str, Any]]:
    """Group pairs into asymmetry buckets and compute mean penalty per bucket."""
    buckets: list[dict[str, Any]] = []

    for low, high, label in ASYMMETRY_BUCKETS:
        penalties = []
        for i, p in enumerate(pairs):
            asym = abs(p.score_ab - p.score_ba)
            if low <= asym < high:
                arith = agg_scores['arithmetic'][i]
                harm = agg_scores['harmonic'][i]
                penalties.append(arith - harm)

        bucket_info: dict[str, Any] = {
            'bucket': label,
            'n_pairs': len(penalties),
        }
        if penalties:
            bucket_info['mean_penalty'] = statistics.mean(penalties)
            bucket_info['median_penalty'] = statistics.median(penalties)
            bucket_info['stdev_penalty'] = statistics.stdev(penalties) if len(penalties) > 1 else 0.0
        else:
            bucket_info['mean_penalty'] = 0.0
            bucket_info['median_penalty'] = 0.0
            bucket_info['stdev_penalty'] = 0.0

        buckets.append(bucket_info)

    return buckets


# ---------------------------------------------------------------------------
# Analysis 5: Rank correlation between methods
# ---------------------------------------------------------------------------

def rank_correlations(agg_scores: dict[str, list[float]]) -> dict[str, dict[str, float]]:
    """Spearman rho between harmonic mean rankings and each alternative."""
    if not agg_scores['harmonic']:
        return {}

    harm = agg_scores['harmonic']
    results: dict[str, dict[str, float]] = {}

    for name in ['arithmetic', 'geometric', 'min']:
        other = agg_scores[name]
        if len(harm) < 3:
            results[name] = {'rho': float('nan'), 'p_value': float('nan')}
            continue
        rho, p_value = sp_stats.spearmanr(harm, other)
        results[name] = {
            'rho': float(rho),
            'p_value': float(p_value),
        }

    return results


# ---------------------------------------------------------------------------
# Analysis 6: Tier impact analysis
# ---------------------------------------------------------------------------

def tier_impact(agg_scores: dict[str, list[float]]) -> dict[str, dict[str, int]]:
    """Count how many pairs fall into each tier under each aggregation method."""
    def classify(score: float) -> str:
        if score >= TIER_THRESHOLDS['hand_picked']:
            return 'hand_picked'
        elif score >= TIER_THRESHOLDS['strong']:
            return 'strong'
        else:
            return 'wildcard'

    results: dict[str, dict[str, int]] = {}
    for name, scores in agg_scores.items():
        counts = defaultdict(int)
        for s in scores:
            counts[classify(s)] += 1
        results[name] = {
            'hand_picked': counts['hand_picked'],
            'strong': counts['strong'],
            'wildcard': counts['wildcard'],
            'total': len(scores),
        }
    return results


# ---------------------------------------------------------------------------
# Analysis 7: Directionality source analysis
# ---------------------------------------------------------------------------

def directionality_analysis(pairs: list[MatchPair]) -> dict[str, dict[str, Any]]:
    """Parse match_context to compute per-dimension asymmetry.

    Extracts per-dimension scores from breakdown_ab and breakdown_ba.
    Expects dimension scores on a 0-10 scale.
    """
    dimension_asymmetries: dict[str, list[float]] = {d: [] for d in ISMC_DIMENSIONS}
    parsed_count = 0

    for p in pairs:
        ctx = p.match_context
        if not ctx:
            continue

        breakdown_ab = ctx.get('breakdown_ab')
        breakdown_ba = ctx.get('breakdown_ba')

        if not isinstance(breakdown_ab, dict) or not isinstance(breakdown_ba, dict):
            continue

        parsed_count += 1

        for dim in ISMC_DIMENSIONS:
            ab_dim = breakdown_ab.get(dim)
            ba_dim = breakdown_ba.get(dim)

            if not isinstance(ab_dim, dict) or not isinstance(ba_dim, dict):
                continue

            score_ab = ab_dim.get('score')
            score_ba = ba_dim.get('score')

            if score_ab is not None and score_ba is not None:
                try:
                    asymmetry = abs(float(score_ab) - float(score_ba))
                    dimension_asymmetries[dim].append(asymmetry)
                except (ValueError, TypeError):
                    continue

    results: dict[str, dict[str, Any]] = {}
    for dim in ISMC_DIMENSIONS:
        vals = dimension_asymmetries[dim]
        if vals:
            results[dim] = {
                'n': len(vals),
                'mean_asymmetry': statistics.mean(vals),
                'median_asymmetry': statistics.median(vals),
                'stdev_asymmetry': statistics.stdev(vals) if len(vals) > 1 else 0.0,
                'max_asymmetry': max(vals),
            }
        else:
            results[dim] = {
                'n': 0,
                'mean_asymmetry': 0.0,
                'median_asymmetry': 0.0,
                'stdev_asymmetry': 0.0,
                'max_asymmetry': 0.0,
            }

    results['_meta'] = {  # type: ignore[assignment]
        'pairs_with_context': parsed_count,
        'total_pairs': len(pairs),
    }

    return results


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------

def setup_plot_style() -> None:
    """Apply consistent plot styling."""
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams.update({
        'figure.figsize': (10, 6),
        'figure.dpi': 150,
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.labelsize': 12,
    })


def plot_asymmetry_distribution(asymmetries: list[float], output_path: Path) -> None:
    """Histogram of |score_ab - score_ba|."""
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.hist(asymmetries, bins=30, edgecolor='white', color='#4C72B0', alpha=0.85)
    ax.axvline(statistics.mean(asymmetries), color='#C44E52', linestyle='--',
               linewidth=2, label=f'Mean = {statistics.mean(asymmetries):.1f}')
    ax.axvline(statistics.median(asymmetries), color='#55A868', linestyle=':',
               linewidth=2, label=f'Median = {statistics.median(asymmetries):.1f}')

    ax.set_xlabel('Asymmetry |score_ab - score_ba|')
    ax.set_ylabel('Count')
    ax.set_title('Distribution of Directional Score Asymmetry')
    ax.legend(frameon=True, fancybox=True)

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {output_path}')


def plot_score_scatter(pairs: list[MatchPair], output_path: Path) -> None:
    """Scatter plot of score_ab vs score_ba, colored by harmonic_mean."""
    fig, ax = plt.subplots(figsize=(10, 10))

    ab = [p.score_ab for p in pairs]
    ba = [p.score_ba for p in pairs]
    hm = [p.harmonic_mean for p in pairs]

    scatter = ax.scatter(ab, ba, c=hm, cmap='RdYlGn', s=20, alpha=0.7,
                         edgecolors='none')
    cbar = fig.colorbar(scatter, ax=ax, label='Harmonic Mean')
    cbar.ax.tick_params(labelsize=10)

    # Diagonal line (perfect symmetry)
    lims = [0, 100]
    ax.plot(lims, lims, 'k--', alpha=0.4, linewidth=1, label='y = x (perfect symmetry)')

    # Penalty zones: asymmetry > 15
    ax.fill_between(
        np.linspace(0, 100, 200),
        np.linspace(0, 100, 200) + 15,
        100,
        alpha=0.08, color='red', label='Asymmetry > 15 zone',
    )
    ax.fill_between(
        np.linspace(0, 100, 200),
        0,
        np.clip(np.linspace(0, 100, 200) - 15, 0, 100),
        alpha=0.08, color='red',
    )

    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_xlabel('score_ab (A sees value in B)')
    ax.set_ylabel('score_ba (B sees value in A)')
    ax.set_title('Bidirectional Score Scatter with Harmonic Mean Coloring')
    ax.legend(loc='upper left', frameon=True, fancybox=True)
    ax.set_aspect('equal')

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {output_path}')


def plot_aggregation_comparison(agg_scores: dict[str, list[float]],
                                output_path: Path) -> None:
    """Overlaid density plots of all 4 aggregation methods."""
    fig, ax = plt.subplots(figsize=(12, 6))

    colors = {
        'harmonic': '#C44E52',
        'arithmetic': '#4C72B0',
        'geometric': '#55A868',
        'min': '#8172B2',
    }
    labels = {
        'harmonic': 'Harmonic Mean (production)',
        'arithmetic': 'Arithmetic Mean',
        'geometric': 'Geometric Mean',
        'min': 'Min',
    }

    for name in AGGREGATION_METHODS:
        scores = agg_scores[name]
        if not scores:
            continue
        sns.kdeplot(scores, ax=ax, color=colors[name], label=labels[name],
                    linewidth=2, fill=True, alpha=0.15)

    ax.set_xlabel('Aggregated Score (0-100)')
    ax.set_ylabel('Density')
    ax.set_title('Score Distribution by Aggregation Method')
    ax.legend(frameon=True, fancybox=True)
    ax.set_xlim(0, 100)

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {output_path}')


def plot_penalty_by_asymmetry(buckets: list[dict[str, Any]], output_path: Path) -> None:
    """Bar chart of mean (arithmetic - harmonic) penalty per asymmetry bucket."""
    fig, ax = plt.subplots(figsize=(10, 6))

    labels = [b['bucket'] for b in buckets]
    penalties = [b['mean_penalty'] for b in buckets]
    counts = [b['n_pairs'] for b in buckets]

    bars = ax.bar(labels, penalties, color='#DD8452', edgecolor='white', width=0.6)

    # Annotate bars with pair counts
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.annotate(
            f'n={count}',
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 5),
            textcoords='offset points',
            ha='center', va='bottom',
            fontsize=10, color='#555555',
        )

    ax.set_xlabel('Asymmetry Bucket (|score_ab - score_ba|)')
    ax.set_ylabel('Mean Penalty (Arithmetic - Harmonic)')
    ax.set_title('Harmonic Mean Penalty by Asymmetry Bucket')

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {output_path}')


def plot_dimension_asymmetry(dim_results: dict[str, dict[str, Any]],
                             output_path: Path) -> None:
    """Grouped bar chart of mean asymmetry per ISMC dimension."""
    fig, ax = plt.subplots(figsize=(10, 6))

    dims = [d for d in ISMC_DIMENSIONS if d in dim_results and dim_results[d]['n'] > 0]
    if not dims:
        # Nothing to plot — write a placeholder
        ax.text(0.5, 0.5, 'No per-dimension data available in match_context',
                ha='center', va='center', transform=ax.transAxes, fontsize=14,
                color='#888888')
        ax.set_title('Per-Dimension Asymmetry (ISMC)')
        fig.tight_layout()
        fig.savefig(output_path, bbox_inches='tight')
        plt.close(fig)
        print(f'  Saved: {output_path} (no data)')
        return

    means = [dim_results[d]['mean_asymmetry'] for d in dims]
    medians = [dim_results[d]['median_asymmetry'] for d in dims]

    x = np.arange(len(dims))
    width = 0.35

    bars_mean = ax.bar(x - width / 2, means, width, label='Mean Asymmetry',
                       color='#4C72B0', edgecolor='white')
    bars_median = ax.bar(x + width / 2, medians, width, label='Median Asymmetry',
                         color='#55A868', edgecolor='white')

    # Annotate with sample counts
    for i, d in enumerate(dims):
        n = dim_results[d]['n']
        ax.annotate(f'n={n}', xy=(x[i], max(means[i], medians[i])),
                    xytext=(0, 5), textcoords='offset points',
                    ha='center', va='bottom', fontsize=9, color='#555555')

    ax.set_xlabel('ISMC Dimension')
    ax.set_ylabel('Asymmetry (0-10 scale)')
    ax.set_title('Per-Dimension Directional Asymmetry')
    ax.set_xticks(x)
    ax.set_xticklabels([d.capitalize() for d in dims])
    ax.legend(frameon=True, fancybox=True)

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {output_path}')


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def format_report(
    n_pairs: int,
    is_test: bool,
    asymmetry: dict[str, Any],
    agg_summaries: dict[str, dict[str, float]],
    penalty: dict[str, Any],
    buckets: list[dict[str, Any]],
    correlations: dict[str, dict[str, float]],
    tiers: dict[str, dict[str, int]],
    dim_results: dict[str, dict[str, Any]],
) -> str:
    """Build the full text report."""
    lines: list[str] = []
    sep = '=' * 72
    subsep = '-' * 72

    lines.append(sep)
    lines.append('  BIDIRECTIONAL HARMONIC MEAN VALIDATION REPORT')
    lines.append(f'  Data source: {"SYNTHETIC (--test)" if is_test else "Production (SupabaseMatch)"}')
    lines.append(f'  Total pairs analyzed: {n_pairs}')
    lines.append(sep)
    lines.append('')

    # --- 1. Asymmetry Distribution ---
    lines.append('1. ASYMMETRY DISTRIBUTION')
    lines.append(subsep)
    lines.append(f'  Metric: |score_ab - score_ba| across all {asymmetry["n"]} pairs')
    lines.append(f'  Mean:   {asymmetry["mean"]:.2f}')
    lines.append(f'  Median: {asymmetry["median"]:.2f}')
    lines.append(f'  Stdev:  {asymmetry["stdev"]:.2f}')
    lines.append(f'  Min:    {asymmetry["min"]:.2f}')
    lines.append(f'  Max:    {asymmetry["max"]:.2f}')
    lines.append('')
    lines.append('  Percentiles:')
    for pct, val in asymmetry['percentiles'].items():
        lines.append(f'    {pct}: {val:.2f}')
    lines.append('')

    expected_range = '5-15 points'
    actual_mean = asymmetry['mean']
    if 5 <= actual_mean <= 15:
        verdict = f'WITHIN expected range ({expected_range})'
    else:
        verdict = f'OUTSIDE expected range ({expected_range})'
    lines.append(f'  Assessment: Mean asymmetry {actual_mean:.1f} is {verdict}.')
    lines.append('')

    # --- 2. Aggregation Method Comparison ---
    lines.append('2. AGGREGATION METHOD COMPARISON')
    lines.append(subsep)
    lines.append(f'  {"Method":<15} {"Mean":>8} {"Median":>8} {"Stdev":>8} {"Min":>8} {"Max":>8}')
    lines.append(f'  {"-"*15} {"-"*8} {"-"*8} {"-"*8} {"-"*8} {"-"*8}')
    for name in AGGREGATION_METHODS:
        s = agg_summaries[name]
        tag = ' (prod)' if name == 'harmonic' else ''
        lines.append(
            f'  {name + tag:<15} {s["mean"]:8.2f} {s["median"]:8.2f} '
            f'{s["stdev"]:8.2f} {s["min"]:8.2f} {s["max"]:8.2f}'
        )
    lines.append('')

    # --- 3. Harmonic Mean Penalty Quantification ---
    lines.append('3. HARMONIC MEAN PENALTY (asymmetry > 20 pts)')
    lines.append(subsep)
    if penalty.get('n_pairs', 0) == 0:
        lines.append(f'  {penalty.get("message", "No highly asymmetric pairs found.")}')
    else:
        lines.append(f'  Pairs with asymmetry > {penalty["asymmetry_threshold"]:.0f}: {penalty["n_pairs"]}')
        lines.append(f'  Mean penalty (arith - harmonic):   {penalty["mean_penalty"]:.2f}')
        lines.append(f'  Median penalty:                    {penalty["median_penalty"]:.2f}')
        lines.append(f'  Max penalty:                       {penalty["max_penalty"]:.2f}')
        lines.append(f'  Stdev:                             {penalty["stdev_penalty"]:.2f}')
    lines.append('')
    lines.append('  Interpretation: This is how much the harmonic mean "punishes"')
    lines.append('  one-sided matches compared to a naive arithmetic average.')
    lines.append('')

    # --- 4. Penalty by Asymmetry Bucket ---
    lines.append('4. PENALTY BY ASYMMETRY BUCKET')
    lines.append(subsep)
    lines.append(f'  {"Bucket":<10} {"N Pairs":>8} {"Mean Pen":>10} {"Med Pen":>10} {"Stdev":>8}')
    lines.append(f'  {"-"*10} {"-"*8} {"-"*10} {"-"*10} {"-"*8}')
    for b in buckets:
        lines.append(
            f'  {b["bucket"]:<10} {b["n_pairs"]:8d} {b["mean_penalty"]:10.3f} '
            f'{b["median_penalty"]:10.3f} {b["stdev_penalty"]:8.3f}'
        )
    lines.append('')
    lines.append('  Note: Penalty = arithmetic_mean - harmonic_mean.')
    lines.append('  Higher penalty means the harmonic mean is more "conservative".')
    lines.append('')

    # --- 5. Rank Correlation ---
    lines.append('5. RANK CORRELATION (Spearman rho vs. Harmonic Mean)')
    lines.append(subsep)
    lines.append(f'  {"Method":<15} {"rho":>8} {"p-value":>12}')
    lines.append(f'  {"-"*15} {"-"*8} {"-"*12}')
    for name, vals in correlations.items():
        rho_str = f'{vals["rho"]:.4f}' if not math.isnan(vals['rho']) else 'N/A'
        p_str = f'{vals["p_value"]:.2e}' if not math.isnan(vals['p_value']) else 'N/A'
        lines.append(f'  {name:<15} {rho_str:>8} {p_str:>12}')
    lines.append('')
    lines.append('  Interpretation: rho close to 1.0 means the ranking order is')
    lines.append('  nearly identical regardless of aggregation method. Lower rho')
    lines.append('  means the harmonic mean substantially reshuffles the rankings.')
    lines.append('')

    # --- 6. Tier Impact Analysis ---
    lines.append('6. TIER IMPACT ANALYSIS')
    lines.append(subsep)
    lines.append(f'  Tier thresholds: hand_picked >= {TIER_THRESHOLDS["hand_picked"]}, '
                 f'strong >= {TIER_THRESHOLDS["strong"]}, wildcard < {TIER_THRESHOLDS["strong"]}')
    lines.append('')
    lines.append(f'  {"Method":<15} {"Hand-Picked":>12} {"Strong":>8} {"Wildcard":>10} {"Total":>8}')
    lines.append(f'  {"-"*15} {"-"*12} {"-"*8} {"-"*10} {"-"*8}')
    for name in AGGREGATION_METHODS:
        t = tiers[name]
        lines.append(
            f'  {name:<15} {t["hand_picked"]:12d} {t["strong"]:8d} '
            f'{t["wildcard"]:10d} {t["total"]:8d}'
        )
    lines.append('')

    # Compute tier drift for each method vs harmonic
    if tiers['harmonic']['total'] > 0:
        lines.append('  Tier drift vs. harmonic (production):')
        harm_hp = tiers['harmonic']['hand_picked']
        for name in ['arithmetic', 'geometric', 'min']:
            diff = tiers[name]['hand_picked'] - harm_hp
            direction = '+' if diff >= 0 else ''
            lines.append(
                f'    {name:<15} hand_picked delta: {direction}{diff} '
                f'({diff / max(tiers[name]["total"], 1) * 100:+.1f}% of total)'
            )
        lines.append('')

    # --- 7. Directionality Source Analysis ---
    lines.append('7. DIRECTIONALITY SOURCE ANALYSIS (per-dimension)')
    lines.append(subsep)
    meta = dim_results.get('_meta', {})
    lines.append(f'  Pairs with match_context breakdowns: {meta.get("pairs_with_context", 0)} / {meta.get("total_pairs", n_pairs)}')
    lines.append('')

    dims_with_data = [d for d in ISMC_DIMENSIONS if d in dim_results and dim_results[d]['n'] > 0]
    if dims_with_data:
        lines.append(f'  {"Dimension":<12} {"N":>6} {"Mean Asym":>10} {"Med Asym":>10} {"Stdev":>8} {"Max":>8}')
        lines.append(f'  {"-"*12} {"-"*6} {"-"*10} {"-"*10} {"-"*8} {"-"*8}')
        for dim in ISMC_DIMENSIONS:
            d = dim_results.get(dim)
            if not d or d['n'] == 0:
                lines.append(f'  {dim:<12} {"(no data)":>6}')
                continue
            lines.append(
                f'  {dim:<12} {d["n"]:6d} {d["mean_asymmetry"]:10.3f} '
                f'{d["median_asymmetry"]:10.3f} {d["stdev_asymmetry"]:8.3f} {d["max_asymmetry"]:8.3f}'
            )
        lines.append('')

        # Identify the most asymmetric dimension
        ranked = sorted(dims_with_data, key=lambda d: dim_results[d]['mean_asymmetry'], reverse=True)
        most_asym = ranked[0]
        least_asym = ranked[-1]
        lines.append(f'  Most asymmetric dimension:  {most_asym.upper()} (mean {dim_results[most_asym]["mean_asymmetry"]:.3f})')
        lines.append(f'  Least asymmetric dimension: {least_asym.upper()} (mean {dim_results[least_asym]["mean_asymmetry"]:.3f})')
        lines.append('')
        lines.append('  Expected pattern: Synergy should show the most directionality')
        lines.append('  (offering->seeking is inherently asymmetric), while Intent and')
        lines.append('  Context should be more symmetric.')
        if most_asym == 'synergy':
            lines.append('  Result: CONFIRMED - Synergy is the most directional dimension.')
        else:
            lines.append(f'  Result: UNEXPECTED - {most_asym.capitalize()} is the most directional, not Synergy.')
    else:
        lines.append('  No per-dimension breakdown data available in match_context.')
        lines.append('  This analysis requires matches scored with scoring_version >= ismc_v2_embeddings.')
    lines.append('')

    # --- 8. Mathematical Properties ---
    lines.append('8. MATHEMATICAL PROPERTIES OF THE HARMONIC MEAN')
    lines.append(subsep)
    lines.append('')
    lines.append('  The harmonic mean was chosen as the aggregation method for')
    lines.append('  bidirectional match scoring because of these key properties:')
    lines.append('')
    lines.append('  (a) Inequality chain: H(a,b) <= G(a,b) <= A(a,b)  for all a,b > 0')
    lines.append('      Harmonic <= Geometric <= Arithmetic')
    lines.append('      This means H is the most conservative of the three Pythagorean')
    lines.append('      means, producing the lowest score when asymmetry exists.')
    lines.append('')
    lines.append('  (b) Symmetry maximization: H(a,b) is maximized when a = b.')
    lines.append('      For fixed a + b, the harmonic mean peaks at a = b = (a+b)/2.')
    lines.append('      This directly rewards balanced, mutual partnerships.')
    lines.append('')
    lines.append('  (c) Zero sensitivity: H(a,b) -> 0 as either a or b -> 0.')
    lines.append('      If one direction sees zero value, the pair scores zero.')
    lines.append('      This prevents completely one-sided matches from surfacing.')
    lines.append('')
    lines.append('  (d) Penalty formula: A(a,b) - H(a,b) = (a-b)^2 / (2(a+b))')
    lines.append('      The penalty grows quadratically with the gap, making the')
    lines.append('      harmonic mean disproportionately harsher on large asymmetries.')
    lines.append('')
    lines.append('  These properties make the harmonic mean the most "fairness-oriented"')
    lines.append('  aggregation for partner matching, where mutual value is essential.')
    lines.append('')

    lines.append(sep)
    lines.append('  END OF REPORT')
    lines.append(sep)

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_csv(pairs: list[MatchPair], agg_scores: dict[str, list[float]],
               output_path: Path) -> None:
    """Export per-pair data to CSV for downstream analysis."""
    fieldnames = [
        'score_ab', 'score_ba', 'asymmetry',
        'harmonic', 'arithmetic', 'geometric', 'min',
        'penalty_arith_minus_harm',
        'has_context',
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, p in enumerate(pairs):
            asym = abs(p.score_ab - p.score_ba)
            writer.writerow({
                'score_ab': round(p.score_ab, 4),
                'score_ba': round(p.score_ba, 4),
                'asymmetry': round(asym, 4),
                'harmonic': round(agg_scores['harmonic'][i], 4),
                'arithmetic': round(agg_scores['arithmetic'][i], 4),
                'geometric': round(agg_scores['geometric'][i], 4),
                'min': round(agg_scores['min'][i], 4),
                'penalty_arith_minus_harm': round(
                    agg_scores['arithmetic'][i] - agg_scores['harmonic'][i], 4
                ),
                'has_context': 1 if p.match_context else 0,
            })

    print(f'  Saved: {output_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Validate the bidirectional harmonic mean design choice in ISMC scoring.'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Use synthetic data instead of production database.',
    )
    parser.add_argument(
        '--n-synthetic', type=int, default=500,
        help='Number of synthetic pairs to generate (only with --test).',
    )
    args = parser.parse_args()

    # Ensure output directories exist
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load data ----
    print()
    if args.test:
        print('Generating synthetic data...')
        pairs = generate_synthetic_data(n=args.n_synthetic)
        print(f'  Generated {len(pairs)} synthetic match pairs.')
    else:
        print('Loading production data from SupabaseMatch...')
        pairs = load_production_data()
        print(f'  Loaded {len(pairs)} match pairs with bidirectional scores.')

    if not pairs:
        print('ERROR: No data available. Use --test for synthetic data.')
        sys.exit(1)

    # Filter out pairs where either score is 0 or negative (avoid division by zero)
    valid_pairs = [p for p in pairs if p.score_ab > 0 and p.score_ba > 0]
    skipped = len(pairs) - len(valid_pairs)
    if skipped > 0:
        print(f'  Skipped {skipped} pairs with zero/negative scores.')
    pairs = valid_pairs

    if not pairs:
        print('ERROR: No valid pairs remaining after filtering.')
        sys.exit(1)

    print(f'  Analyzing {len(pairs)} valid pairs.\n')

    # ---- Run analyses ----
    print('Running analyses...')

    # Compute aggregation scores once
    agg_scores = compute_all_aggregations(pairs)

    # 1. Asymmetry distribution
    print('  [1/7] Asymmetry distribution')
    asymmetry = analyze_asymmetry(pairs)

    # 2. Aggregation method comparison
    print('  [2/7] Aggregation method comparison')
    agg_summaries = compare_aggregations(agg_scores)

    # 3. Harmonic mean penalty quantification
    print('  [3/7] Harmonic mean penalty quantification')
    penalty = quantify_penalty(pairs, agg_scores, asymmetry_threshold=20.0)

    # 4. Penalty by asymmetry bucket
    print('  [4/7] Penalty by asymmetry bucket')
    buckets = penalty_by_bucket(pairs, agg_scores)

    # 5. Rank correlation
    print('  [5/7] Rank correlation (Spearman)')
    correlations = rank_correlations(agg_scores)

    # 6. Tier impact analysis
    print('  [6/7] Tier impact analysis')
    tiers = tier_impact(agg_scores)

    # 7. Directionality source analysis
    print('  [7/7] Directionality source analysis')
    dim_results = directionality_analysis(pairs)

    print('  All analyses complete.\n')

    # ---- Generate report ----
    print('Generating report...')
    report = format_report(
        n_pairs=len(pairs),
        is_test=args.test,
        asymmetry=asymmetry,
        agg_summaries=agg_summaries,
        penalty=penalty,
        buckets=buckets,
        correlations=correlations,
        tiers=tiers,
        dim_results=dim_results,
    )

    report_path = RESULTS_DIR / 'bidirectional_analysis_report.txt'
    with open(report_path, 'w') as f:
        f.write(report)
    print(f'  Saved: {report_path}')

    # Print report to console
    print('\n' + report)

    # ---- Export CSV ----
    print('\nExporting CSV data...')
    csv_path = RESULTS_DIR / 'bidirectional_analysis_data.csv'
    export_csv(pairs, agg_scores, csv_path)

    # ---- Generate plots ----
    print('\nGenerating visualizations...')
    setup_plot_style()

    plot_asymmetry_distribution(
        asymmetry['raw'],
        PLOTS_DIR / 'asymmetry_distribution.png',
    )
    plot_score_scatter(
        pairs,
        PLOTS_DIR / 'score_ab_vs_ba_scatter.png',
    )
    plot_aggregation_comparison(
        agg_scores,
        PLOTS_DIR / 'aggregation_comparison.png',
    )
    plot_penalty_by_asymmetry(
        buckets,
        PLOTS_DIR / 'penalty_by_asymmetry.png',
    )
    plot_dimension_asymmetry(
        dim_results,
        PLOTS_DIR / 'dimension_asymmetry.png',
    )

    print(f'\nDone. All outputs written to: {RESULTS_DIR}/')


if __name__ == '__main__':
    main()
