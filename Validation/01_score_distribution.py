#!/usr/bin/env python3
"""
01_score_distribution.py - ISMC Score Distribution Analysis
============================================================

Phase 1A of the matching algorithm validation plan. Analyzes the statistical
properties of harmonic_mean scores and the four ISMC dimension sub-scores
to verify the scoring engine produces a well-behaved, discriminating
distribution suitable for tiered partner matching.

Analyses:
    1. Score distribution statistics (normality, skewness, kurtosis, entropy, Gini)
    2. Per-dimension ISMC distributions (Intent, Synergy, Momentum, Context)
    3. Component independence (Pearson correlation matrix, verify max |r| < 0.7)
    4. Tier distribution (hand_picked >= 67, strong >= 55, wildcard < 55)
    5. Sanity checks against known population values (mean ~57.49, stdev ~5.70)

Usage:
    python scripts/validation/01_score_distribution.py
    python scripts/validation/01_score_distribution.py --test

Flags:
    --test    Generate synthetic data (500 matches, seed=42) instead of querying DB
"""

import os
import sys
import argparse
import random
import warnings
from datetime import datetime
from pathlib import Path
from collections import OrderedDict

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from matching.models import SupabaseMatch

# ---------------------------------------------------------------------------
# Scientific stack (imported after Django to keep bootstrap errors separate)
# ---------------------------------------------------------------------------
from scipy import stats as sp_stats
import matplotlib
matplotlib.use('Agg')  # non-interactive backend for server/CI
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
BOOTSTRAP_ITERATIONS = 10_000
SHAPIRO_MAX_N = 5000

TIER_BOUNDARIES = {
    'hand_picked': (67, 100),
    'strong':      (55, 67),
    'wildcard':    (0,  55),
}

PERCENTILES = [1, 5, 10, 25, 50, 75, 90, 95, 99]

ISMC_DIMENSION_KEYS = OrderedDict([
    ('Intent',   'intent_breakdown'),
    ('Synergy',  'synergy_breakdown'),
    ('Momentum', 'momentum_breakdown'),
    ('Context',  'context_breakdown'),
])

# Known population values for sanity checks
KNOWN_MEAN = 57.49
KNOWN_STDEV = 5.70
SANITY_TOLERANCE = 2.0  # absolute tolerance for mean/stdev check

# Output paths (relative to the validation script directory)
PROJECT_ROOT_PATH = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(__file__).resolve().parent / 'validation_results'
PLOTS_DIR = RESULTS_DIR / 'plots'
REPORT_PATH = RESULTS_DIR / 'score_distribution_report.txt'
DATA_PATH = RESULTS_DIR / 'score_distribution_data.csv'


# ============================================================================
# Helpers
# ============================================================================

def ensure_output_dirs() -> None:
    """Create output directories if they do not exist."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def assign_tier(score: float) -> str:
    """Map a harmonic_mean score to its tier label."""
    if score >= 67:
        return 'hand_picked'
    elif score >= 55:
        return 'strong'
    else:
        return 'wildcard'


def _sig_label(p: float) -> str:
    """Return a human-readable significance label."""
    if p < 0.001:
        return '***'
    elif p < 0.01:
        return '**'
    elif p < 0.05:
        return '*'
    else:
        return 'n.s.'


def gini_coefficient(values: np.ndarray) -> float:
    """
    Compute the Gini coefficient of an array of values.

    Uses the relative mean absolute difference formula:
        G = (2 * sum(i * x_i) - (n+1) * sum(x)) / (n * sum(x))
    where x is sorted ascending.
    """
    values = np.sort(values)
    n = len(values)
    if n == 0 or np.sum(values) == 0:
        return 0.0
    index = np.arange(1, n + 1)
    return float((2 * np.sum(index * values) - (n + 1) * np.sum(values)) / (n * np.sum(values)))


def shannon_entropy(values: np.ndarray, bins: int = 50) -> float:
    """
    Compute Shannon entropy (in bits) of a continuous distribution
    by binning into a histogram.
    """
    counts, _ = np.histogram(values, bins=bins)
    probs = counts / counts.sum()
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))


def bootstrap_ci(values: np.ndarray, stat_func, n_bootstrap: int = BOOTSTRAP_ITERATIONS,
                 ci: float = 0.95, seed: int = RANDOM_SEED) -> tuple:
    """
    Compute a bootstrapped confidence interval for a given statistic.

    Returns:
        (point_estimate, ci_lower, ci_upper)
    """
    rng = np.random.RandomState(seed)
    point = stat_func(values)
    boot_stats = np.empty(n_bootstrap)
    n = len(values)
    for i in range(n_bootstrap):
        sample = rng.choice(values, size=n, replace=True)
        boot_stats[i] = stat_func(sample)
    alpha = (1 - ci) / 2
    ci_lower = float(np.percentile(boot_stats, alpha * 100))
    ci_upper = float(np.percentile(boot_stats, (1 - alpha) * 100))
    return point, ci_lower, ci_upper


# ============================================================================
# Data Loading
# ============================================================================

def load_real_data() -> pd.DataFrame:
    """
    Query SupabaseMatch for all matches with harmonic_mean scores.
    Extract ISMC dimension sub-scores from match_context JSON.

    Returns:
        DataFrame with columns: harmonic_mean, score_ab, score_ba, tier,
                                 Intent, Synergy, Momentum, Context
    """
    qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False
    ).values(
        'harmonic_mean', 'score_ab', 'score_ba', 'match_context', 'trust_level'
    )

    records = []
    for row in qs.iterator(chunk_size=2000):
        hm = float(row['harmonic_mean'])
        score_ab = float(row['score_ab']) if row['score_ab'] is not None else None
        score_ba = float(row['score_ba']) if row['score_ba'] is not None else None

        rec = {
            'harmonic_mean': hm,
            'score_ab': score_ab,
            'score_ba': score_ba,
            'trust_level': row['trust_level'] or 'legacy',
            'tier': assign_tier(hm),
        }

        # Extract ISMC dimension scores from match_context JSON
        ctx = row.get('match_context') or {}
        if isinstance(ctx, str):
            import json
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                ctx = {}

        for dim_name, json_key in ISMC_DIMENSION_KEYS.items():
            breakdown = ctx.get(json_key, {})
            if isinstance(breakdown, dict):
                rec[dim_name] = breakdown.get('score')
            else:
                rec[dim_name] = None

        records.append(rec)

    df = pd.DataFrame(records)
    print(f"  Loaded {len(df):,} matches from database")
    return df


def generate_synthetic_data(n: int = 500, seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    Generate synthetic ISMC match data for testing the pipeline.

    Produces 500 matches with realistic score distributions:
    - Intent: ~60 mean, moderate spread
    - Synergy: ~55 mean, wider spread
    - Momentum: ~58 mean, moderate spread
    - Context: ~56 mean, narrow spread
    - harmonic_mean: derived from dimensions with noise

    Uses random.seed(42) and np.random.seed(42) for reproducibility.
    """
    random.seed(seed)
    np.random.seed(seed)

    records = []
    for _ in range(n):
        intent = np.clip(np.random.normal(60, 8), 10, 95)
        synergy = np.clip(np.random.normal(55, 10), 10, 95)
        momentum = np.clip(np.random.normal(58, 9), 10, 95)
        context = np.clip(np.random.normal(56, 7), 10, 95)

        # Approximate harmonic mean with noise (realistic simulation)
        dims = np.array([intent, synergy, momentum, context])
        weights = np.array([0.45, 0.25, 0.20, 0.10])
        epsilon = 1e-10
        weighted_hm = np.sum(weights) / np.sum(weights / np.maximum(dims, epsilon))
        hm = np.clip(weighted_hm + np.random.normal(0, 2), 20, 95)

        # score_ab and score_ba centered around harmonic_mean
        score_ab = np.clip(hm + np.random.normal(0, 3), 20, 95)
        score_ba = np.clip(hm + np.random.normal(0, 3), 20, 95)

        tier = assign_tier(hm)
        trust_choices = ['platinum', 'gold', 'bronze', 'legacy']
        trust_level = random.choice(trust_choices)

        records.append({
            'harmonic_mean': round(float(hm), 2),
            'score_ab': round(float(score_ab), 2),
            'score_ba': round(float(score_ba), 2),
            'trust_level': trust_level,
            'tier': tier,
            'Intent': round(float(intent), 2),
            'Synergy': round(float(synergy), 2),
            'Momentum': round(float(momentum), 2),
            'Context': round(float(context), 2),
        })

    df = pd.DataFrame(records)
    print(f"  Generated {len(df):,} synthetic matches (seed={seed})")
    return df


# ============================================================================
# Analysis Functions
# ============================================================================

def analyze_score_distribution(scores: np.ndarray) -> dict:
    """
    Compute comprehensive distribution statistics for harmonic_mean scores.

    Returns dict with:
        - descriptive stats (mean, stdev, min, max)
        - normality tests (Shapiro-Wilk, Anderson-Darling)
        - skewness with bootstrapped 95% CI
        - kurtosis with bootstrapped 95% CI
        - Shannon entropy
        - Gini coefficient
        - percentile table
    """
    results = {}

    # --- Descriptive statistics ---
    results['n'] = len(scores)
    results['mean'] = float(np.mean(scores))
    results['stdev'] = float(np.std(scores, ddof=1))
    results['min'] = float(np.min(scores))
    results['max'] = float(np.max(scores))
    results['range'] = results['max'] - results['min']
    results['median'] = float(np.median(scores))
    results['iqr'] = float(np.percentile(scores, 75) - np.percentile(scores, 25))

    # --- Normality tests ---
    # Shapiro-Wilk (sample if n > 5000)
    if len(scores) > SHAPIRO_MAX_N:
        rng = np.random.RandomState(RANDOM_SEED)
        sample = rng.choice(scores, size=SHAPIRO_MAX_N, replace=False)
        sw_stat, sw_p = sp_stats.shapiro(sample)
        results['shapiro_note'] = f'Sampled {SHAPIRO_MAX_N:,} of {len(scores):,}'
    else:
        sw_stat, sw_p = sp_stats.shapiro(scores)
        results['shapiro_note'] = f'Full sample (n={len(scores):,})'
    results['shapiro_stat'] = float(sw_stat)
    results['shapiro_p'] = float(sw_p)

    # Anderson-Darling
    ad_result = sp_stats.anderson(scores, dist='norm')
    results['anderson_stat'] = float(ad_result.statistic)
    results['anderson_critical_5pct'] = float(ad_result.critical_values[2])  # 5% level
    results['anderson_reject_5pct'] = bool(ad_result.statistic > ad_result.critical_values[2])

    # --- Skewness with bootstrapped 95% CI ---
    skew_point, skew_lo, skew_hi = bootstrap_ci(
        scores, lambda x: float(sp_stats.skew(x))
    )
    results['skewness'] = skew_point
    results['skewness_ci_lower'] = skew_lo
    results['skewness_ci_upper'] = skew_hi

    # --- Kurtosis with bootstrapped 95% CI ---
    kurt_point, kurt_lo, kurt_hi = bootstrap_ci(
        scores, lambda x: float(sp_stats.kurtosis(x))
    )
    results['kurtosis'] = kurt_point
    results['kurtosis_ci_lower'] = kurt_lo
    results['kurtosis_ci_upper'] = kurt_hi

    # --- Shannon entropy ---
    results['shannon_entropy'] = shannon_entropy(scores, bins=50)
    results['entropy_good'] = results['shannon_entropy'] > 3.0

    # --- Gini coefficient ---
    results['gini'] = gini_coefficient(scores)

    # --- Percentile table ---
    results['percentiles'] = {}
    for p in PERCENTILES:
        results['percentiles'][p] = float(np.percentile(scores, p))

    return results


def analyze_dimension_distributions(df: pd.DataFrame) -> dict:
    """
    Compute distribution statistics for each ISMC dimension.

    Returns:
        dict keyed by dimension name, each containing basic stats.
    """
    dim_stats = {}
    for dim_name in ISMC_DIMENSION_KEYS.keys():
        vals = df[dim_name].dropna().values.astype(float)
        if len(vals) == 0:
            dim_stats[dim_name] = {'n': 0, 'note': 'No data available'}
            continue

        stats = {
            'n': len(vals),
            'mean': float(np.mean(vals)),
            'stdev': float(np.std(vals, ddof=1)),
            'min': float(np.min(vals)),
            'max': float(np.max(vals)),
            'median': float(np.median(vals)),
            'iqr': float(np.percentile(vals, 75) - np.percentile(vals, 25)),
            'skewness': float(sp_stats.skew(vals)),
            'kurtosis': float(sp_stats.kurtosis(vals)),
        }

        # Shannon entropy for this dimension
        stats['shannon_entropy'] = shannon_entropy(vals, bins=50)

        # Percentiles
        stats['percentiles'] = {}
        for p in PERCENTILES:
            stats['percentiles'][p] = float(np.percentile(vals, p))

        dim_stats[dim_name] = stats

    return dim_stats


def analyze_component_independence(df: pd.DataFrame) -> dict:
    """
    Compute Pearson correlation matrix for the 4 ISMC dimensions.
    Verifies that max |r| < 0.7 (dimensions are reasonably independent).

    Returns:
        dict with correlation_matrix, max_abs_r, independence_ok, pvalue_matrix
    """
    dim_names = list(ISMC_DIMENSION_KEYS.keys())

    # Build a sub-DataFrame with only rows that have all 4 dimensions
    sub = df[dim_names].dropna()
    n_complete = len(sub)

    if n_complete < 10:
        return {
            'n_complete': n_complete,
            'note': 'Insufficient data for correlation analysis (need >= 10 complete rows)',
            'independence_ok': None,
        }

    corr_matrix = np.zeros((4, 4))
    pval_matrix = np.zeros((4, 4))

    for i, d1 in enumerate(dim_names):
        for j, d2 in enumerate(dim_names):
            if i == j:
                corr_matrix[i, j] = 1.0
                pval_matrix[i, j] = 0.0
            else:
                r, p = sp_stats.pearsonr(sub[d1].values, sub[d2].values)
                corr_matrix[i, j] = r
                pval_matrix[i, j] = p

    # Find max absolute correlation (off-diagonal)
    mask = ~np.eye(4, dtype=bool)
    max_abs_r = float(np.max(np.abs(corr_matrix[mask])))

    return {
        'n_complete': n_complete,
        'dim_names': dim_names,
        'correlation_matrix': corr_matrix,
        'pvalue_matrix': pval_matrix,
        'max_abs_r': max_abs_r,
        'independence_ok': max_abs_r < 0.7,
    }


def analyze_tier_distribution(df: pd.DataFrame) -> dict:
    """
    Count matches in each tier and compute percentages.
    """
    total = len(df)
    tier_counts = df['tier'].value_counts().to_dict()

    tiers = {}
    for tier_name in ['hand_picked', 'strong', 'wildcard']:
        count = tier_counts.get(tier_name, 0)
        pct = (count / total * 100) if total > 0 else 0.0
        lo, hi = TIER_BOUNDARIES[tier_name]
        tiers[tier_name] = {
            'count': count,
            'pct': pct,
            'range': f'{lo}-{hi}',
        }

    return {
        'total': total,
        'tiers': tiers,
    }


def run_sanity_checks(dist_stats: dict, is_test: bool) -> list:
    """
    Verify that distribution statistics match expected known values.

    For test mode, we skip the checks against production values since
    synthetic data will have different parameters.

    Returns:
        list of (check_name, passed, message) tuples
    """
    checks = []

    if is_test:
        checks.append((
            'Test Mode',
            True,
            'Sanity checks relaxed in test mode (synthetic data has different parameters)',
        ))
        return checks

    # Check mean ~57.49
    mean = dist_stats['mean']
    mean_ok = abs(mean - KNOWN_MEAN) < SANITY_TOLERANCE
    checks.append((
        'Mean check',
        mean_ok,
        f'mean={mean:.2f}, expected~{KNOWN_MEAN} (tolerance +/-{SANITY_TOLERANCE})'
        + (' PASS' if mean_ok else ' FAIL'),
    ))

    # Check stdev ~5.70
    stdev = dist_stats['stdev']
    stdev_ok = abs(stdev - KNOWN_STDEV) < SANITY_TOLERANCE
    checks.append((
        'Stdev check',
        stdev_ok,
        f'stdev={stdev:.2f}, expected~{KNOWN_STDEV} (tolerance +/-{SANITY_TOLERANCE})'
        + (' PASS' if stdev_ok else ' FAIL'),
    ))

    # Check score range is reasonable (at least 20 points of spread)
    range_val = dist_stats['range']
    range_ok = range_val >= 20
    checks.append((
        'Range check',
        range_ok,
        f'range={range_val:.2f} (need >= 20 for meaningful tiers)'
        + (' PASS' if range_ok else ' FAIL'),
    ))

    # Check entropy > 3.0 bits
    entropy = dist_stats['shannon_entropy']
    entropy_ok = entropy > 3.0
    checks.append((
        'Entropy check',
        entropy_ok,
        f'entropy={entropy:.3f} bits (target > 3.0 for good discrimination)'
        + (' PASS' if entropy_ok else ' FAIL'),
    ))

    return checks


# ============================================================================
# Plotting
# ============================================================================

def plot_score_histogram(scores: np.ndarray, dist_stats: dict) -> None:
    """
    Histogram of harmonic_mean scores with tier boundary lines at 55 and 67.
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    # Histogram
    n_bins = min(50, max(20, len(scores) // 20))
    ax.hist(scores, bins=n_bins, color='#4A90D9', edgecolor='white',
            alpha=0.85, density=False, label='Score distribution')

    # Tier boundaries
    ax.axvline(x=55, color='#E67E22', linewidth=2, linestyle='--',
               label='Strong threshold (55)')
    ax.axvline(x=67, color='#27AE60', linewidth=2, linestyle='--',
               label='Hand-picked threshold (67)')

    # Mean line
    ax.axvline(x=dist_stats['mean'], color='#E74C3C', linewidth=1.5,
               linestyle=':', label=f'Mean ({dist_stats["mean"]:.1f})')

    # Shade tier regions
    ax.axvspan(0, 55, alpha=0.05, color='#E74C3C', label='Wildcard zone')
    ax.axvspan(55, 67, alpha=0.05, color='#E67E22', label='Strong zone')
    ax.axvspan(67, 100, alpha=0.05, color='#27AE60', label='Hand-picked zone')

    ax.set_xlabel('Harmonic Mean Score', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_title('ISMC Score Distribution with Tier Boundaries', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(axis='y', alpha=0.3)

    # Annotation box with key stats
    stats_text = (
        f"n = {dist_stats['n']:,}\n"
        f"mean = {dist_stats['mean']:.2f}\n"
        f"stdev = {dist_stats['stdev']:.2f}\n"
        f"skew = {dist_stats['skewness']:.3f}\n"
        f"entropy = {dist_stats['shannon_entropy']:.2f} bits"
    )
    ax.text(0.97, 0.95, stats_text, transform=ax.transAxes,
            fontsize=9, verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.85))

    plt.tight_layout()
    path = PLOTS_DIR / 'score_distribution_histogram.png'
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {path}")


def plot_correlation_heatmap(corr_results: dict) -> None:
    """
    4x4 Pearson correlation matrix heatmap for ISMC dimensions.
    Graceful fallback: uses matplotlib imshow if seaborn unavailable.
    """
    if corr_results.get('n_complete', 0) < 10:
        print("  Skipping correlation heatmap (insufficient data)")
        return

    corr_matrix = corr_results['correlation_matrix']
    dim_names = corr_results['dim_names']

    fig, ax = plt.subplots(figsize=(8, 7))

    if HAS_SEABORN:
        corr_df = pd.DataFrame(corr_matrix, index=dim_names, columns=dim_names)
        sns.heatmap(corr_df, annot=True, fmt='.3f', cmap='RdBu_r',
                    center=0, vmin=-1, vmax=1, square=True,
                    linewidths=1, linecolor='white', ax=ax,
                    cbar_kws={'label': 'Pearson r'})
    else:
        # Matplotlib-only fallback
        im = ax.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1, aspect='equal')
        ax.set_xticks(range(4))
        ax.set_yticks(range(4))
        ax.set_xticklabels(dim_names, fontsize=11)
        ax.set_yticklabels(dim_names, fontsize=11)
        # Annotate cells
        for i in range(4):
            for j in range(4):
                color = 'white' if abs(corr_matrix[i, j]) > 0.5 else 'black'
                ax.text(j, i, f'{corr_matrix[i, j]:.3f}',
                        ha='center', va='center', fontsize=12, color=color)
        cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label('Pearson r', fontsize=11)

    max_r = corr_results['max_abs_r']
    ok_str = 'PASS' if corr_results['independence_ok'] else 'FAIL'
    ax.set_title(
        f'ISMC Dimension Correlation Matrix\n(max |r| = {max_r:.3f}, independence check: {ok_str})',
        fontsize=13, fontweight='bold'
    )

    plt.tight_layout()
    path = PLOTS_DIR / 'ismc_correlation_heatmap.png'
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {path}")


def plot_dimension_boxplot(df: pd.DataFrame) -> None:
    """
    Box plots of all 4 ISMC dimensions side by side.
    """
    dim_names = list(ISMC_DIMENSION_KEYS.keys())

    # Collect data for each dimension (drop NaN)
    box_data = []
    labels = []
    for dim in dim_names:
        vals = df[dim].dropna().values.astype(float)
        if len(vals) > 0:
            box_data.append(vals)
            labels.append(dim)

    if not box_data:
        print("  Skipping dimension boxplot (no dimension data available)")
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    bp = ax.boxplot(box_data, labels=labels, patch_artist=True,
                    showmeans=True, meanline=True,
                    meanprops=dict(color='red', linewidth=1.5, linestyle='--'),
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='o', markersize=3, alpha=0.4))

    colors = ['#3498DB', '#2ECC71', '#E67E22', '#9B59B6']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)

    # Tier boundary lines
    ax.axhline(y=55, color='#E67E22', linewidth=1, linestyle=':', alpha=0.6,
               label='Strong threshold (55)')
    ax.axhline(y=67, color='#27AE60', linewidth=1, linestyle=':', alpha=0.6,
               label='Hand-picked threshold (67)')

    ax.set_ylabel('Score', fontsize=12)
    ax.set_title('ISMC Dimension Score Distributions', fontsize=14, fontweight='bold')
    ax.legend(loc='lower right', fontsize=9)
    ax.grid(axis='y', alpha=0.3)

    # Add count annotations below each box
    for i, dim in enumerate(labels):
        n = len(box_data[i])
        ax.text(i + 1, ax.get_ylim()[0] + 1, f'n={n:,}',
                ha='center', va='bottom', fontsize=8, color='gray')

    plt.tight_layout()
    path = PLOTS_DIR / 'dimension_boxplot.png'
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {path}")


# ============================================================================
# Report Generation
# ============================================================================

def generate_report(dist_stats: dict, dim_stats: dict, corr_results: dict,
                    tier_results: dict, sanity_checks: list,
                    is_test: bool) -> str:
    """
    Generate the full text report.
    """
    lines = []
    W = 78  # report width

    def section(title):
        lines.append('')
        lines.append('=' * W)
        lines.append(title.upper())
        lines.append('=' * W)

    def subsection(title):
        lines.append('')
        lines.append(f'--- {title} ---')

    # Header
    lines.append('=' * W)
    lines.append('ISMC SCORE DISTRIBUTION ANALYSIS')
    lines.append('Phase 1A: Matching Algorithm Validation')
    lines.append('=' * W)
    lines.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append(f'Mode: {"SYNTHETIC TEST DATA" if is_test else "PRODUCTION DATA"}')
    lines.append(f'N matches: {dist_stats["n"]:,}')

    # -----------------------------------------------------------------------
    # Section 1: Score Distribution Statistics
    # -----------------------------------------------------------------------
    section('1. SCORE DISTRIBUTION STATISTICS (harmonic_mean)')

    subsection('Descriptive Statistics')
    lines.append(f'  N:       {dist_stats["n"]:,}')
    lines.append(f'  Mean:    {dist_stats["mean"]:.4f}')
    lines.append(f'  Stdev:   {dist_stats["stdev"]:.4f}')
    lines.append(f'  Median:  {dist_stats["median"]:.4f}')
    lines.append(f'  IQR:     {dist_stats["iqr"]:.4f}')
    lines.append(f'  Min:     {dist_stats["min"]:.4f}')
    lines.append(f'  Max:     {dist_stats["max"]:.4f}')
    lines.append(f'  Range:   {dist_stats["range"]:.4f}')

    subsection('Normality Tests')
    lines.append(f'  Shapiro-Wilk:')
    lines.append(f'    Statistic:  {dist_stats["shapiro_stat"]:.6f}')
    lines.append(f'    p-value:    {dist_stats["shapiro_p"]:.6e} {_sig_label(dist_stats["shapiro_p"])}')
    lines.append(f'    Note:       {dist_stats["shapiro_note"]}')
    lines.append(f'  Anderson-Darling:')
    lines.append(f'    Statistic:  {dist_stats["anderson_stat"]:.4f}')
    lines.append(f'    5% critical: {dist_stats["anderson_critical_5pct"]:.4f}')
    reject_str = 'REJECT normality' if dist_stats['anderson_reject_5pct'] else 'FAIL to reject normality'
    lines.append(f'    Result:     {reject_str}')

    subsection('Shape Statistics (Bootstrapped 95% CIs, 10,000 iterations)')
    lines.append(f'  Skewness:  {dist_stats["skewness"]:.4f}  '
                 f'[{dist_stats["skewness_ci_lower"]:.4f}, {dist_stats["skewness_ci_upper"]:.4f}]')
    lines.append(f'  Kurtosis:  {dist_stats["kurtosis"]:.4f}  '
                 f'[{dist_stats["kurtosis_ci_lower"]:.4f}, {dist_stats["kurtosis_ci_upper"]:.4f}]')
    lines.append(f'    (Excess kurtosis: 0 = normal, >0 = heavy-tailed, <0 = light-tailed)')

    subsection('Information & Inequality')
    ent = dist_stats['shannon_entropy']
    ent_verdict = 'GOOD (>3.0 bits)' if dist_stats['entropy_good'] else 'LOW (<3.0 bits, poor discrimination)'
    lines.append(f'  Shannon entropy: {ent:.4f} bits  [{ent_verdict}]')
    lines.append(f'  Gini coefficient: {dist_stats["gini"]:.4f}')

    subsection('Percentile Table')
    lines.append(f'  {"Percentile":>12}  {"Score":>10}')
    lines.append(f'  {"-"*12}  {"-"*10}')
    for p in PERCENTILES:
        lines.append(f'  {p:>11}th  {dist_stats["percentiles"][p]:>10.2f}')

    # -----------------------------------------------------------------------
    # Section 2: Per-Dimension ISMC Distributions
    # -----------------------------------------------------------------------
    section('2. PER-DIMENSION ISMC DISTRIBUTIONS')

    for dim_name, stats in dim_stats.items():
        subsection(f'{dim_name} (key: {ISMC_DIMENSION_KEYS[dim_name]})')
        if stats.get('n', 0) == 0:
            lines.append(f'  {stats.get("note", "No data")}')
            continue

        lines.append(f'  N:       {stats["n"]:,}')
        lines.append(f'  Mean:    {stats["mean"]:.4f}')
        lines.append(f'  Stdev:   {stats["stdev"]:.4f}')
        lines.append(f'  Median:  {stats["median"]:.4f}')
        lines.append(f'  IQR:     {stats["iqr"]:.4f}')
        lines.append(f'  Min:     {stats["min"]:.4f}')
        lines.append(f'  Max:     {stats["max"]:.4f}')
        lines.append(f'  Skew:    {stats["skewness"]:.4f}')
        lines.append(f'  Kurt:    {stats["kurtosis"]:.4f}')
        lines.append(f'  Entropy: {stats["shannon_entropy"]:.4f} bits')

        lines.append(f'  Percentiles:')
        pct_line = '    '
        for p in PERCENTILES:
            pct_line += f'P{p}={stats["percentiles"][p]:.1f}  '
        lines.append(pct_line.rstrip())

    # -----------------------------------------------------------------------
    # Section 3: Component Independence
    # -----------------------------------------------------------------------
    section('3. COMPONENT INDEPENDENCE (Pearson Correlation Matrix)')

    if corr_results.get('n_complete', 0) < 10:
        lines.append(f'  {corr_results.get("note", "Insufficient data")}')
    else:
        n_c = corr_results['n_complete']
        dim_names = corr_results['dim_names']
        corr_mat = corr_results['correlation_matrix']
        pval_mat = corr_results['pvalue_matrix']

        lines.append(f'  Complete cases: {n_c:,}')
        lines.append('')

        # Print correlation matrix
        header = f'  {"":>10}'
        for d in dim_names:
            header += f'  {d:>10}'
        lines.append(header)
        lines.append(f'  {"-"*10}' + f'  {"-"*10}' * 4)

        for i, d1 in enumerate(dim_names):
            row = f'  {d1:>10}'
            for j in range(4):
                row += f'  {corr_mat[i, j]:>10.4f}'
            lines.append(row)

        lines.append('')

        # Print p-value matrix
        lines.append('  P-values:')
        header = f'  {"":>10}'
        for d in dim_names:
            header += f'  {d:>10}'
        lines.append(header)
        lines.append(f'  {"-"*10}' + f'  {"-"*10}' * 4)

        for i, d1 in enumerate(dim_names):
            row = f'  {d1:>10}'
            for j in range(4):
                if i == j:
                    row += f'  {"---":>10}'
                else:
                    row += f'  {pval_mat[i, j]:>10.4e}'
            lines.append(row)

        lines.append('')
        max_r = corr_results['max_abs_r']
        ok = corr_results['independence_ok']
        verdict = 'PASS (dimensions sufficiently independent)' if ok else \
                  'FAIL (correlated dimensions detected, geometric mean may waste weight)'
        lines.append(f'  Max |r| = {max_r:.4f}  (threshold: 0.7)')
        lines.append(f'  Independence check: {verdict}')

    # -----------------------------------------------------------------------
    # Section 4: Tier Distribution
    # -----------------------------------------------------------------------
    section('4. TIER DISTRIBUTION')

    total = tier_results['total']
    lines.append(f'  Total matches: {total:,}')
    lines.append('')
    lines.append(f'  {"Tier":>15}  {"Range":>10}  {"Count":>8}  {"Pct":>8}')
    lines.append(f'  {"-"*15}  {"-"*10}  {"-"*8}  {"-"*8}')

    for tier_name in ['hand_picked', 'strong', 'wildcard']:
        t = tier_results['tiers'][tier_name]
        lines.append(f'  {tier_name:>15}  {t["range"]:>10}  {t["count"]:>8,}  {t["pct"]:>7.1f}%')

    # -----------------------------------------------------------------------
    # Section 5: Sanity Checks
    # -----------------------------------------------------------------------
    section('5. SANITY CHECKS')

    all_pass = True
    for name, passed, msg in sanity_checks:
        status = 'PASS' if passed else 'FAIL'
        if not passed:
            all_pass = False
        lines.append(f'  [{status}] {name}: {msg}')

    lines.append('')
    if all_pass:
        lines.append('  Overall: ALL CHECKS PASSED')
    else:
        lines.append('  Overall: SOME CHECKS FAILED (see above)')

    # Footer
    lines.append('')
    lines.append('=' * W)
    lines.append('END OF REPORT')
    lines.append('=' * W)

    return '\n'.join(lines)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='ISMC Score Distribution Analysis (Phase 1A Validation)'
    )
    parser.add_argument('--test', action='store_true',
                        help='Generate synthetic data (500 matches, seed=42)')
    args = parser.parse_args()

    print('\n' + '=' * 60)
    print('ISMC Score Distribution Analysis')
    print('=' * 60)

    # Ensure output directories exist
    ensure_output_dirs()

    # -----------------------------------------------------------------------
    # Load data
    # -----------------------------------------------------------------------
    print('\n[1/6] Loading data...')
    if args.test:
        df = generate_synthetic_data(n=500, seed=RANDOM_SEED)
    else:
        df = load_real_data()

    if len(df) == 0:
        print('ERROR: No data loaded. Exiting.')
        sys.exit(1)

    scores = df['harmonic_mean'].dropna().values.astype(float)
    print(f'  Scores available: {len(scores):,}')

    # -----------------------------------------------------------------------
    # Analyze score distribution
    # -----------------------------------------------------------------------
    print('\n[2/6] Analyzing score distribution...')
    dist_stats = analyze_score_distribution(scores)
    print(f'  Mean: {dist_stats["mean"]:.2f}, Stdev: {dist_stats["stdev"]:.2f}')
    print(f'  Skewness: {dist_stats["skewness"]:.4f}, Kurtosis: {dist_stats["kurtosis"]:.4f}')
    print(f'  Shannon entropy: {dist_stats["shannon_entropy"]:.3f} bits')
    print(f'  Gini coefficient: {dist_stats["gini"]:.4f}')

    # -----------------------------------------------------------------------
    # Analyze per-dimension distributions
    # -----------------------------------------------------------------------
    print('\n[3/6] Analyzing per-dimension ISMC distributions...')
    dim_stats = analyze_dimension_distributions(df)
    for dim_name, stats in dim_stats.items():
        if stats.get('n', 0) > 0:
            print(f'  {dim_name}: mean={stats["mean"]:.2f}, stdev={stats["stdev"]:.2f}, n={stats["n"]:,}')
        else:
            print(f'  {dim_name}: {stats.get("note", "No data")}')

    # -----------------------------------------------------------------------
    # Component independence
    # -----------------------------------------------------------------------
    print('\n[4/6] Analyzing component independence...')
    corr_results = analyze_component_independence(df)
    if corr_results.get('independence_ok') is not None:
        max_r = corr_results['max_abs_r']
        ok_str = 'PASS' if corr_results['independence_ok'] else 'FAIL'
        print(f'  Max |r| = {max_r:.4f} (threshold: 0.7) -> {ok_str}')
    else:
        print(f'  {corr_results.get("note", "Insufficient data")}')

    # -----------------------------------------------------------------------
    # Tier distribution
    # -----------------------------------------------------------------------
    print('\n[5/6] Analyzing tier distribution...')
    tier_results = analyze_tier_distribution(df)
    for tier_name in ['hand_picked', 'strong', 'wildcard']:
        t = tier_results['tiers'][tier_name]
        print(f'  {tier_name}: {t["count"]:,} ({t["pct"]:.1f}%)')

    # -----------------------------------------------------------------------
    # Sanity checks
    # -----------------------------------------------------------------------
    print('\n[5.5/6] Running sanity checks...')
    sanity_checks = run_sanity_checks(dist_stats, is_test=args.test)
    for name, passed, msg in sanity_checks:
        status = 'PASS' if passed else 'FAIL'
        print(f'  [{status}] {name}: {msg}')

    # -----------------------------------------------------------------------
    # Generate plots
    # -----------------------------------------------------------------------
    print('\n[6/6] Generating plots...')
    plot_score_histogram(scores, dist_stats)
    plot_correlation_heatmap(corr_results)
    plot_dimension_boxplot(df)

    # -----------------------------------------------------------------------
    # Save report
    # -----------------------------------------------------------------------
    print('\nSaving report and data...')
    report_text = generate_report(dist_stats, dim_stats, corr_results,
                                  tier_results, sanity_checks, is_test=args.test)
    with open(REPORT_PATH, 'w') as f:
        f.write(report_text)
    print(f'  Report: {REPORT_PATH}')

    # Save raw data CSV
    csv_cols = ['harmonic_mean', 'score_ab', 'score_ba', 'trust_level', 'tier']
    csv_cols += list(ISMC_DIMENSION_KEYS.keys())
    df_out = df[[c for c in csv_cols if c in df.columns]]
    df_out.to_csv(DATA_PATH, index=False)
    print(f'  Data:   {DATA_PATH}')

    # Print summary
    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    print(f'  Matches analyzed: {dist_stats["n"]:,}')
    print(f'  Mean score:       {dist_stats["mean"]:.2f}')
    print(f'  Stdev:            {dist_stats["stdev"]:.2f}')
    print(f'  Entropy:          {dist_stats["shannon_entropy"]:.3f} bits '
          f'({"GOOD" if dist_stats["entropy_good"] else "LOW"})')
    if corr_results.get('independence_ok') is not None:
        print(f'  Independence:     max|r|={corr_results["max_abs_r"]:.3f} '
              f'({"PASS" if corr_results["independence_ok"] else "FAIL"})')
    all_pass = all(p for _, p, _ in sanity_checks)
    print(f'  Sanity checks:    {"ALL PASS" if all_pass else "SOME FAILED"}')
    print(f'\nOutputs:')
    print(f'  {REPORT_PATH}')
    print(f'  {DATA_PATH}')
    print(f'  {PLOTS_DIR / "score_distribution_histogram.png"}')
    print(f'  {PLOTS_DIR / "ismc_correlation_heatmap.png"}')
    print(f'  {PLOTS_DIR / "dimension_boxplot.png"}')
    print()


if __name__ == '__main__':
    main()
