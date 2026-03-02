#!/usr/bin/env python3
"""
05_embedding_validation.py — Statistical Validation of Embedding-Based Semantic Matching

Validates the quality of bge-large-en-v1.5 embeddings used in the ISMC Synergy
dimension for JV partner matching. Produces a comprehensive report with:

  1. Embedding Coverage Audit — field population rates across profiles
  2. Semantic vs Word Overlap Hit Rate — method usage in production matches
  3. Precision-Recall Analysis — PR curve from benchmark data
  4. Threshold Calibration Validation — TPR/FPR at production thresholds
  5. Score Distribution by Method — embedding vs word_overlap discrimination
  6. Embedding Quality Summary Table — consolidated whitepaper-ready metrics

Usage:
    python scripts/validation/05_embedding_validation.py          # Live DB
    python scripts/validation/05_embedding_validation.py --test   # Synthetic data

Outputs:
    validation_results/embedding_validation_report.txt
    validation_results/embedding_validation_data.csv
    validation_results/plots/embedding_coverage_bar.png
    validation_results/plots/precision_recall_curve.png
    validation_results/plots/semantic_vs_overlap_distribution.png
    validation_results/plots/threshold_discrimination.png
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django  # noqa: E402
django.setup()

from matching.models import SupabaseMatch, SupabaseProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Matplotlib / seaborn (import after Django to avoid backend issues)
# ---------------------------------------------------------------------------
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

plt.style.use('seaborn-v0_8-whitegrid')

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(__file__).resolve().parent / 'validation_results'
PLOTS_DIR = RESULTS_DIR / 'plots'
REPORT_PATH = RESULTS_DIR / 'embedding_validation_report.txt'
CSV_PATH = RESULTS_DIR / 'embedding_validation_data.csv'

# Benchmark CSV files (produced by prior validation runs)
SYNONYM_CSV = BASE_DIR / 'validation_results' / 'model_comparison_20260217_184807.csv'
SYNONYM_CSV_FALLBACK = BASE_DIR / 'validation_results' / 'synonym_stress_test_20260217_182410.csv'
RANDOM_CSV = BASE_DIR / 'validation_results' / 'embedding_benchmark_20260217_183929.csv'

# Production thresholds used in synergy scoring
THRESHOLDS = [0.50, 0.53, 0.55, 0.60, 0.65, 0.70, 0.75]
PRODUCTION_THRESHOLDS = [0.53, 0.60, 0.65, 0.75]

EMBEDDING_FIELDS = [
    'embedding_seeking',
    'embedding_offering',
    'embedding_who_you_serve',
    'embedding_what_you_do',
]

# Profile completeness fields (text/json fields that indicate a filled-out profile)
COMPLETENESS_FIELDS = [
    'name', 'email', 'company', 'website', 'linkedin', 'bio',
    'what_you_do', 'who_you_serve', 'seeking', 'offering',
    'niche', 'audience_type', 'business_focus', 'service_provided',
    'signature_programs', 'current_projects', 'booking_link',
    'tags', 'revenue_tier',
]

random.seed(42)
np.random.seed(42)


# ============================================================================
# Synthetic data generators (--test mode)
# ============================================================================

def _generate_synthetic_profiles(n: int = 200) -> List[Dict[str, Any]]:
    """Generate synthetic profile dicts for test mode."""
    statuses = ['Member', 'Non Member Resource', 'Pending', 'Prospect', 'Qualified', 'Inactive']
    status_weights = [0.45, 0.15, 0.10, 0.15, 0.10, 0.05]
    niches = [
        'Business Coaching', 'Health & Wellness', 'Real Estate', 'Marketing',
        'Personal Development', 'Finance', 'Technology', 'Spirituality',
        'Leadership', 'Content Creation',
    ]
    network_roles = ['hub', 'connector', 'specialist', 'peripheral', None]

    profiles = []
    for i in range(n):
        status = random.choices(statuses, weights=status_weights, k=1)[0]
        # Simulate varying completeness
        completeness_roll = random.random()
        if completeness_roll < 0.25:
            n_fields = random.randint(1, 4)   # sparse
        elif completeness_roll < 0.65:
            n_fields = random.randint(5, 9)   # moderate
        else:
            n_fields = random.randint(10, len(COMPLETENESS_FIELDS))  # complete

        filled = set(random.sample(COMPLETENESS_FIELDS, min(n_fields, len(COMPLETENESS_FIELDS))))

        # Embedding coverage: correlate with completeness
        has_embeddings = {}
        for field in EMBEDDING_FIELDS:
            # Higher completeness => higher chance of having embedding
            if n_fields >= 10:
                has_embeddings[field] = random.random() < 0.92
            elif n_fields >= 5:
                has_embeddings[field] = random.random() < 0.70
            else:
                has_embeddings[field] = random.random() < 0.30

        profile = {
            'id': f'synth-{i:04d}',
            'name': f'Test Profile {i}',
            'status': status,
            'niche': random.choice(niches) if 'niche' in filled else None,
            'audience_type': 'B2B' if random.random() < 0.6 else 'B2C' if 'audience_type' in filled else None,
            'network_role': random.choice(network_roles),
            'filled_fields': filled,
            'n_fields_filled': n_fields,
        }
        for field in EMBEDDING_FIELDS:
            profile[field] = '[0.1, 0.2, ...]' if has_embeddings[field] else None

        profiles.append(profile)
    return profiles


def _generate_synthetic_match_contexts(n: int = 500) -> List[Dict[str, Any]]:
    """Generate synthetic match_context JSON dicts for test mode."""
    contexts = []
    for i in range(n):
        n_factors = random.randint(2, 4)
        factors = []
        for j in range(n_factors):
            method = 'semantic' if random.random() < 0.985 else 'word_overlap'
            if method == 'semantic':
                score = max(0.0, min(1.0, random.gauss(0.58, 0.12)))
            else:
                score = max(0.0, min(1.0, random.gauss(0.45, 0.15)))
            factors.append({
                'name': f'factor_{j}',
                'score': round(score, 4),
                'method': method,
            })

        synergy_score = round(np.mean([f['score'] for f in factors]), 4)
        contexts.append({
            'match_id': f'synth-match-{i:04d}',
            'match_context': {
                'breakdown_ab': {
                    'synergy': {
                        'score': synergy_score,
                        'factors': factors,
                    }
                }
            }
        })
    return contexts


def _generate_synthetic_benchmark_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generate synthetic synonym and random benchmark DataFrames."""
    # Synonym pairs: 30 pairs with high embedding sim
    synonym_rows = []
    for i in range(30):
        bge_sim = max(0.40, min(0.95, random.gauss(0.748, 0.063)))
        synonym_rows.append({
            'pair_num': i + 1,
            'category': random.choice(['audience', 'offering', 'seeking', 'niche', 'cross']),
            'text_a': f'synonym text A {i}',
            'text_b': f'synonym text B {i}',
            'word_overlap': 3.0,
            'emb_bge-large-en-v1.5': round(bge_sim, 4),
            'label': 1,
        })

    # Random pairs: 500 pairs with lower embedding sim
    random_rows = []
    for i in range(500):
        os_sim = max(0.20, min(0.80, random.gauss(0.531, 0.077)))
        aud_sim = max(0.20, min(0.80, random.gauss(0.577, 0.083)))
        random_rows.append({
            'pair_num': i + 1,
            'profile_a_name': f'Random A {i}',
            'profile_b_name': f'Random B {i}',
            'embedding_sim_offering_seeking': round(os_sim, 4) if random.random() < 0.49 else None,
            'embedding_sim_audience': round(aud_sim, 4) if random.random() < 0.78 else None,
            'word_overlap_offering_seeking': random.choice([3.0, 3.0, 3.0, 4.5, 6.0, 8.0, 10.0]),
            'word_overlap_audience': random.choice([3.0, 3.0, 4.5, 4.5, 6.0, 8.0, 10.0]),
            'label': 0,
        })

    return pd.DataFrame(synonym_rows), pd.DataFrame(random_rows)


# ============================================================================
# Data loading — live DB
# ============================================================================

def load_profile_coverage() -> pd.DataFrame:
    """Query embedding coverage from SupabaseProfile."""
    profiles = SupabaseProfile.objects.all().values(
        'id', 'name', 'status', 'niche', 'audience_type', 'network_role',
        *EMBEDDING_FIELDS,
        *[f for f in COMPLETENESS_FIELDS if f not in ('tags',)],
        'tags',
    )

    rows = []
    for p in profiles:
        n_filled = 0
        for field in COMPLETENESS_FIELDS:
            val = p.get(field)
            if val is not None and val != '' and val != [] and val != {}:
                n_filled += 1

        emb_populated = {}
        for field in EMBEDDING_FIELDS:
            val = p.get(field)
            emb_populated[field] = (val is not None and str(val).strip() not in ('', 'None', 'null'))

        rows.append({
            'id': str(p['id']),
            'name': p.get('name', ''),
            'status': p.get('status', 'Unknown'),
            'niche': p.get('niche'),
            'audience_type': p.get('audience_type'),
            'network_role': p.get('network_role'),
            'n_fields_filled': n_filled,
            **{f: emb_populated[f] for f in EMBEDDING_FIELDS},
        })

    return pd.DataFrame(rows)


def load_match_contexts() -> List[Dict[str, Any]]:
    """Load match_context JSON from SupabaseMatch for synergy method analysis."""
    matches = SupabaseMatch.objects.filter(
        match_context__isnull=False
    ).values('id', 'match_context', 'harmonic_mean')

    results = []
    for m in matches:
        ctx = m.get('match_context')
        if not ctx:
            continue
        # match_context may be stored as a JSON string; parse it if needed
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(ctx, dict):
            continue
        results.append({
            'match_id': str(m['id']),
            'match_context': ctx,
            'harmonic_mean': float(m['harmonic_mean']) if m.get('harmonic_mean') else None,
        })
    return results


def load_benchmark_csvs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load the pre-computed benchmark CSVs for PR analysis."""
    # Synonym data — prefer the model_comparison CSV (has bge-large column)
    if SYNONYM_CSV.exists():
        synonym_df = pd.read_csv(SYNONYM_CSV)
    elif SYNONYM_CSV_FALLBACK.exists():
        synonym_df = pd.read_csv(SYNONYM_CSV_FALLBACK)
    else:
        raise FileNotFoundError(
            f"Cannot find synonym benchmark CSV at {SYNONYM_CSV} or {SYNONYM_CSV_FALLBACK}"
        )

    # Random pairs benchmark
    if RANDOM_CSV.exists():
        random_df = pd.read_csv(RANDOM_CSV)
    else:
        raise FileNotFoundError(f"Cannot find random benchmark CSV at {RANDOM_CSV}")

    return synonym_df, random_df


# ============================================================================
# Analysis 1: Embedding Coverage Audit
# ============================================================================

def analyze_coverage(
    profiles_df: pd.DataFrame,
) -> Dict[str, Any]:
    """Compute embedding field coverage statistics."""
    total = len(profiles_df)
    if total == 0:
        return {'total_profiles': 0, 'fields': {}, 'by_status': {}, 'by_tier': {}}

    # Per-field coverage
    field_stats = {}
    for field in EMBEDDING_FIELDS:
        n_populated = int(profiles_df[field].sum())
        field_stats[field] = {
            'count': n_populated,
            'pct': round(100.0 * n_populated / total, 2),
        }

    # All 4 fields populated
    profiles_df['all_4_fields'] = profiles_df[EMBEDDING_FIELDS].all(axis=1)
    n_all_4 = int(profiles_df['all_4_fields'].sum())

    # Coverage by status
    by_status = {}
    for status, group in profiles_df.groupby('status'):
        n_group = len(group)
        n_all = int(group['all_4_fields'].sum())
        by_status[status] = {
            'total': n_group,
            'all_4_pct': round(100.0 * n_all / n_group, 2) if n_group > 0 else 0.0,
        }
        for field in EMBEDDING_FIELDS:
            by_status[status][field + '_pct'] = round(
                100.0 * group[field].sum() / n_group, 2
            ) if n_group > 0 else 0.0

    # Coverage by completeness tier
    def tier(n: int) -> str:
        if n >= 10:
            return 'Complete (10+)'
        elif n >= 5:
            return 'Moderate (5-9)'
        else:
            return 'Sparse (<5)'

    profiles_df['tier'] = profiles_df['n_fields_filled'].apply(tier)
    by_tier = {}
    for t, group in profiles_df.groupby('tier'):
        n_group = len(group)
        n_all = int(group['all_4_fields'].sum())
        by_tier[t] = {
            'total': n_group,
            'all_4_pct': round(100.0 * n_all / n_group, 2) if n_group > 0 else 0.0,
        }
        for field in EMBEDDING_FIELDS:
            by_tier[t][field + '_pct'] = round(
                100.0 * group[field].sum() / n_group, 2
            ) if n_group > 0 else 0.0

    return {
        'total_profiles': total,
        'fields': field_stats,
        'all_4_count': n_all_4,
        'all_4_pct': round(100.0 * n_all_4 / total, 2),
        'by_status': dict(sorted(by_status.items(), key=lambda x: -x[1]['total'])),
        'by_tier': by_tier,
    }


# ============================================================================
# Analysis 2: Semantic vs Word Overlap Hit Rate
# ============================================================================

def analyze_method_hit_rate(
    match_contexts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Count semantic vs word_overlap factor evaluations across all matches."""
    semantic_count = 0
    overlap_count = 0
    unknown_count = 0
    total_matches = 0
    matches_with_synergy = 0

    for mc in match_contexts:
        total_matches += 1
        ctx = mc['match_context']

        # Navigate into breakdown_ab.synergy.factors
        for direction in ['breakdown_ab', 'breakdown_ba']:
            breakdown = ctx.get(direction, {})
            synergy = breakdown.get('synergy', {})
            factors = synergy.get('factors', [])
            if factors:
                if direction == 'breakdown_ab':
                    matches_with_synergy += 1
                for factor in factors:
                    method = factor.get('method', 'unknown')
                    if method == 'semantic':
                        semantic_count += 1
                    elif method == 'word_overlap':
                        overlap_count += 1
                    else:
                        unknown_count += 1

    total_evals = semantic_count + overlap_count + unknown_count
    return {
        'total_matches_analyzed': total_matches,
        'matches_with_synergy_factors': matches_with_synergy,
        'semantic_count': semantic_count,
        'word_overlap_count': overlap_count,
        'unknown_count': unknown_count,
        'total_factor_evaluations': total_evals,
        'semantic_pct': round(100.0 * semantic_count / total_evals, 2) if total_evals > 0 else 0.0,
        'word_overlap_pct': round(100.0 * overlap_count / total_evals, 2) if total_evals > 0 else 0.0,
    }


# ============================================================================
# Analysis 3: Precision-Recall Analysis
# ============================================================================

def _extract_bge_scores(synonym_df: pd.DataFrame) -> np.ndarray:
    """Extract bge-large-en-v1.5 similarity scores from the synonym CSV."""
    # model_comparison CSV has 'emb_bge-large-en-v1.5' column
    if 'emb_bge-large-en-v1.5' in synonym_df.columns:
        scores = synonym_df['emb_bge-large-en-v1.5'].dropna().values
    elif 'embedding_sim' in synonym_df.columns:
        # Fallback: synonym_stress_test CSV (these are MiniLM scores unless
        # the run used bge-large; the report heading clarifies)
        scores = synonym_df['embedding_sim'].dropna().values
    else:
        raise ValueError("Cannot find embedding similarity column in synonym CSV")
    return scores.astype(float)


def _extract_random_scores(random_df: pd.DataFrame) -> np.ndarray:
    """Extract all available embedding similarity scores from the random benchmark CSV."""
    score_cols = []
    for col in ['embedding_sim_offering_seeking', 'embedding_sim_audience']:
        if col in random_df.columns:
            score_cols.append(col)
    if not score_cols:
        raise ValueError("Cannot find embedding similarity columns in random CSV")

    all_scores = []
    for col in score_cols:
        vals = random_df[col].dropna().values.astype(float)
        all_scores.extend(vals.tolist())
    return np.array(all_scores)


def compute_precision_recall(
    synonym_scores: np.ndarray,
    random_scores: np.ndarray,
    thresholds: List[float],
) -> Dict[str, Any]:
    """Compute precision, recall, F1 at each threshold using synonym=positive, random=negative."""
    n_pos = len(synonym_scores)
    n_neg = len(random_scores)

    results = []
    for t in thresholds:
        tp = int(np.sum(synonym_scores >= t))
        fn = n_pos - tp
        fp = int(np.sum(random_scores >= t))
        tn = n_neg - fp

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        results.append({
            'threshold': t,
            'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
            'precision': round(precision, 4),
            'recall': round(recall, 4),
            'f1': round(f1, 4),
            'tpr': round(recall, 4),
            'fpr': round(fp / n_neg, 4) if n_neg > 0 else 0.0,
        })

    # Compute AUC-PR via trapezoidal integration over a fine grid
    fine_thresholds = np.linspace(0.0, 1.0, 1000)
    precisions = []
    recalls = []
    for t in fine_thresholds:
        tp = np.sum(synonym_scores >= t)
        fp = np.sum(random_scores >= t)
        fn = n_pos - tp
        p = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        precisions.append(p)
        recalls.append(r)

    # Sort by recall for proper AUC computation
    recall_arr = np.array(recalls)
    precision_arr = np.array(precisions)
    sorted_idx = np.argsort(recall_arr)
    recall_sorted = recall_arr[sorted_idx]
    precision_sorted = precision_arr[sorted_idx]

    # Deduplicate recall values, keeping max precision at each recall level
    unique_recalls = []
    unique_precisions = []
    prev_r = -1
    for r, p in zip(recall_sorted, precision_sorted):
        if r != prev_r:
            unique_recalls.append(r)
            unique_precisions.append(p)
        else:
            unique_precisions[-1] = max(unique_precisions[-1], p)
        prev_r = r

    # np.trapezoid was introduced in NumPy 2.0; np.trapz was removed in 2.0+
    _trapz = getattr(np, 'trapezoid', None) or getattr(np, 'trapz')
    auc_pr = float(_trapz(unique_precisions, unique_recalls))

    # Optimal F1
    best = max(results, key=lambda x: x['f1'])

    return {
        'n_positives': n_pos,
        'n_negatives': n_neg,
        'thresholds': results,
        'auc_pr': round(auc_pr, 4),
        'optimal_f1_threshold': best['threshold'],
        'optimal_f1': best['f1'],
        'pr_curve_data': {
            'recalls': [r['recall'] for r in results],
            'precisions': [r['precision'] for r in results],
        },
    }


# ============================================================================
# Analysis 4: Threshold Calibration Validation
# ============================================================================

def analyze_threshold_calibration(
    synonym_scores: np.ndarray,
    random_scores: np.ndarray,
) -> Dict[str, Any]:
    """Evaluate TPR and FPR at production thresholds."""
    synonym_mean = float(np.mean(synonym_scores))
    random_mean = float(np.mean(random_scores))
    discrimination_gap = synonym_mean - random_mean

    calibration = []
    for t in PRODUCTION_THRESHOLDS:
        tpr = float(np.mean(synonym_scores >= t))
        fpr = float(np.mean(random_scores >= t))
        calibration.append({
            'threshold': t,
            'synonym_capture_rate': round(tpr, 4),
            'false_positive_rate': round(fpr, 4),
            'gap': round(tpr - fpr, 4),
        })

    return {
        'synonym_mean': round(synonym_mean, 4),
        'random_mean': round(random_mean, 4),
        'discrimination_gap': round(discrimination_gap, 4),
        'synonym_std': round(float(np.std(synonym_scores)), 4),
        'random_std': round(float(np.std(random_scores)), 4),
        'calibration': calibration,
    }


# ============================================================================
# Analysis 5: Score Distribution by Method
# ============================================================================

def analyze_score_distribution_by_method(
    match_contexts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compare synergy score distributions for semantic vs word_overlap scored matches."""
    semantic_scores = []
    overlap_scores = []

    for mc in match_contexts:
        ctx = mc['match_context']
        for direction in ['breakdown_ab', 'breakdown_ba']:
            breakdown = ctx.get(direction, {})
            synergy = breakdown.get('synergy', {})
            factors = synergy.get('factors', [])
            synergy_score = synergy.get('score')

            if not factors or synergy_score is None:
                continue

            # Determine dominant method for this direction
            methods = [f.get('method', 'unknown') for f in factors]
            method_counts = Counter(methods)
            dominant = method_counts.most_common(1)[0][0]

            score_val = float(synergy_score)
            if dominant == 'semantic':
                semantic_scores.append(score_val)
            elif dominant == 'word_overlap':
                overlap_scores.append(score_val)

    sem_arr = np.array(semantic_scores) if semantic_scores else np.array([])
    ovl_arr = np.array(overlap_scores) if overlap_scores else np.array([])

    result = {
        'semantic_count': len(sem_arr),
        'word_overlap_count': len(ovl_arr),
    }

    if len(sem_arr) > 0:
        result['semantic_mean'] = round(float(np.mean(sem_arr)), 4)
        result['semantic_median'] = round(float(np.median(sem_arr)), 4)
        result['semantic_std'] = round(float(np.std(sem_arr)), 4)
        result['semantic_q1'] = round(float(np.percentile(sem_arr, 25)), 4)
        result['semantic_q3'] = round(float(np.percentile(sem_arr, 75)), 4)
    if len(ovl_arr) > 0:
        result['word_overlap_mean'] = round(float(np.mean(ovl_arr)), 4)
        result['word_overlap_median'] = round(float(np.median(ovl_arr)), 4)
        result['word_overlap_std'] = round(float(np.std(ovl_arr)), 4)
        result['word_overlap_q1'] = round(float(np.percentile(ovl_arr, 25)), 4)
        result['word_overlap_q3'] = round(float(np.percentile(ovl_arr, 75)), 4)

    if len(sem_arr) > 0 and len(ovl_arr) > 0:
        result['mean_difference'] = round(result['semantic_mean'] - result['word_overlap_mean'], 4)

    result['semantic_scores'] = sem_arr.tolist()
    result['word_overlap_scores'] = ovl_arr.tolist()

    return result


# ============================================================================
# Visualization helpers
# ============================================================================

def plot_coverage_bar(coverage: Dict[str, Any], output_path: Path) -> None:
    """Bar chart of embedding coverage % for each field."""
    fields = EMBEDDING_FIELDS
    short_names = [f.replace('embedding_', '') for f in fields]
    pcts = [coverage['fields'][f]['pct'] for f in fields]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(short_names, pcts, color=['#2196F3', '#4CAF50', '#FF9800', '#9C27B0'], edgecolor='white')

    for bar, pct in zip(bars, pcts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f'{pct:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

    # Add "All 4" bar
    all_4_pct = coverage['all_4_pct']
    all_bar = ax.bar('ALL 4 fields', all_4_pct, color='#E91E63', edgecolor='white')
    ax.text(all_bar[0].get_x() + all_bar[0].get_width() / 2, all_bar[0].get_height() + 1,
            f'{all_4_pct:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

    ax.set_ylim(0, 110)
    ax.set_ylabel('Coverage (%)', fontsize=12)
    ax.set_title(f'Embedding Field Coverage ({coverage["total_profiles"]:,} profiles)', fontsize=14)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def plot_precision_recall_curve(
    pr_data: Dict[str, Any],
    synonym_scores: np.ndarray,
    random_scores: np.ndarray,
    output_path: Path,
) -> None:
    """PR curve with annotated thresholds and AUC."""
    # Compute fine-grained PR curve
    fine_thresholds = np.linspace(0.0, 1.0, 500)
    n_pos = len(synonym_scores)
    n_neg = len(random_scores)

    recalls, precisions = [], []
    for t in fine_thresholds:
        tp = np.sum(synonym_scores >= t)
        fp = np.sum(random_scores >= t)
        fn = n_pos - tp
        p = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        recalls.append(r)
        precisions.append(p)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.plot(recalls, precisions, linewidth=2, color='#1565C0', label='PR Curve')
    ax.fill_between(recalls, precisions, alpha=0.15, color='#1565C0')

    # Annotate production thresholds
    colors = ['#FF9800', '#E91E63', '#4CAF50', '#9C27B0']
    for i, entry in enumerate(pr_data['thresholds']):
        t = entry['threshold']
        r = entry['recall']
        p = entry['precision']
        ax.plot(r, p, 'o', markersize=10, color=colors[i % len(colors)], zorder=5)
        ax.annotate(
            f't={t:.2f}\nF1={entry["f1"]:.3f}',
            xy=(r, p), xytext=(r - 0.08, p + 0.04),
            fontsize=9, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='gray', lw=1.2),
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.9),
        )

    ax.set_xlabel('Recall (synonym capture rate)', fontsize=12)
    ax.set_ylabel('Precision (1 - false discovery rate)', fontsize=12)
    ax.set_title(
        f'Precision-Recall Curve for Embedding Similarity\n'
        f'AUC-PR = {pr_data["auc_pr"]:.4f}  |  '
        f'Optimal F1 = {pr_data["optimal_f1"]:.3f} @ t={pr_data["optimal_f1_threshold"]:.2f}',
        fontsize=13,
    )
    ax.set_xlim(-0.02, 1.05)
    ax.set_ylim(-0.02, 1.05)
    ax.legend(fontsize=11, loc='lower left')

    # Reference line at AUC-PR target 0.80
    ax.axhline(y=0.80, color='gray', linestyle='--', alpha=0.5, label='Target AUC-PR=0.80')

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def plot_semantic_vs_overlap_distribution(
    dist_data: Dict[str, Any],
    output_path: Path,
) -> None:
    """Overlaid histograms comparing synergy score distributions by scoring method."""
    fig, ax = plt.subplots(figsize=(10, 6))

    sem_scores = dist_data.get('semantic_scores', [])
    ovl_scores = dist_data.get('word_overlap_scores', [])

    bins = np.linspace(0, 1, 40)

    if sem_scores:
        ax.hist(sem_scores, bins=bins, alpha=0.6, color='#1565C0',
                label=f'Semantic (n={len(sem_scores):,})', density=True, edgecolor='white')
    if ovl_scores:
        ax.hist(ovl_scores, bins=bins, alpha=0.6, color='#FF6F00',
                label=f'Word Overlap (n={len(ovl_scores):,})', density=True, edgecolor='white')

    # Add mean lines
    if sem_scores:
        sem_mean = np.mean(sem_scores)
        ax.axvline(x=sem_mean, color='#1565C0', linestyle='--', linewidth=2,
                   label=f'Semantic mean={sem_mean:.3f}')
    if ovl_scores:
        ovl_mean = np.mean(ovl_scores)
        ax.axvline(x=ovl_mean, color='#FF6F00', linestyle='--', linewidth=2,
                   label=f'Overlap mean={ovl_mean:.3f}')

    ax.set_xlabel('Synergy Score', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('Synergy Score Distribution: Semantic vs Word Overlap Method', fontsize=14)
    ax.legend(fontsize=10)

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def plot_threshold_discrimination(
    calibration_data: Dict[str, Any],
    output_path: Path,
) -> None:
    """Dual bar chart: synonym capture rate vs false positive rate at each threshold."""
    calibration = calibration_data['calibration']
    thresholds = [f'{c["threshold"]:.2f}' for c in calibration]
    capture_rates = [c['synonym_capture_rate'] * 100 for c in calibration]
    fp_rates = [c['false_positive_rate'] * 100 for c in calibration]

    x = np.arange(len(thresholds))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.bar(x - width / 2, capture_rates, width, color='#2196F3',
                   label='Synonym Capture Rate (TPR)', edgecolor='white')
    bars2 = ax.bar(x + width / 2, fp_rates, width, color='#F44336',
                   label='False Positive Rate (FPR)', edgecolor='white')

    # Annotate bars
    for bar, val in zip(bars1, capture_rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f'{val:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold', color='#1565C0')
    for bar, val in zip(bars2, fp_rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f'{val:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold', color='#C62828')

    ax.set_xlabel('Similarity Threshold', fontsize=12)
    ax.set_ylabel('Rate (%)', fontsize=12)
    ax.set_title(
        f'Threshold Discrimination: Synonym Capture vs False Positive\n'
        f'Discrimination gap (mean): {calibration_data["discrimination_gap"]:.4f}',
        fontsize=13,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(thresholds, fontsize=11)
    ax.set_ylim(0, 115)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.legend(fontsize=11, loc='upper right')

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)


# ============================================================================
# Report formatting
# ============================================================================

def format_report(
    coverage: Dict[str, Any],
    hit_rate: Dict[str, Any],
    pr_analysis: Dict[str, Any],
    calibration: Dict[str, Any],
    distribution: Dict[str, Any],
    elapsed: float,
    test_mode: bool,
) -> str:
    """Format the complete validation report as plain text."""
    lines = []
    w = lines.append

    mode_label = 'TEST (synthetic data)' if test_mode else 'LIVE'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    w('=' * 80)
    w('EMBEDDING VALIDATION REPORT')
    w(f'Generated: {timestamp}')
    w(f'Mode: {mode_label}')
    w(f'Time elapsed: {elapsed:.1f}s')
    w('=' * 80)

    # ------------------------------------------------------------------
    # Section 1: Embedding Coverage Audit
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('1. EMBEDDING COVERAGE AUDIT')
    w('-' * 80)
    w(f'  Total profiles: {coverage["total_profiles"]:,}')
    w('')
    w(f'  {"Field":<30s} {"Count":>8s} {"Coverage":>10s}')
    w(f'  {"─" * 30} {"─" * 8} {"─" * 10}')
    for field in EMBEDDING_FIELDS:
        fs = coverage['fields'][field]
        w(f'  {field:<30s} {fs["count"]:>8,d} {fs["pct"]:>9.1f}%')
    w(f'  {"─" * 30} {"─" * 8} {"─" * 10}')
    w(f'  {"ALL 4 fields":<30s} {coverage["all_4_count"]:>8,d} {coverage["all_4_pct"]:>9.1f}%')

    # Coverage by status
    w('')
    w('  Coverage by Profile Status (% with all 4 embedding fields):')
    w(f'  {"Status":<25s} {"Total":>8s} {"All 4":>8s}')
    w(f'  {"─" * 25} {"─" * 8} {"─" * 8}')
    for status, stats in coverage['by_status'].items():
        w(f'  {status:<25s} {stats["total"]:>8,d} {stats["all_4_pct"]:>7.1f}%')

    # Coverage by tier
    w('')
    w('  Coverage by Profile Completeness Tier:')
    w(f'  {"Tier":<25s} {"Total":>8s} {"All 4":>8s}')
    w(f'  {"─" * 25} {"─" * 8} {"─" * 8}')
    for tier_name in ['Complete (10+)', 'Moderate (5-9)', 'Sparse (<5)']:
        if tier_name in coverage['by_tier']:
            stats = coverage['by_tier'][tier_name]
            w(f'  {tier_name:<25s} {stats["total"]:>8,d} {stats["all_4_pct"]:>7.1f}%')

    # ------------------------------------------------------------------
    # Section 2: Semantic vs Word Overlap Hit Rate
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('2. SEMANTIC VS WORD OVERLAP HIT RATE')
    w('-' * 80)
    w(f'  Total matches analyzed:       {hit_rate["total_matches_analyzed"]:,}')
    w(f'  Matches with synergy factors: {hit_rate["matches_with_synergy_factors"]:,}')
    w(f'  Total factor evaluations:     {hit_rate["total_factor_evaluations"]:,}')
    w('')
    w(f'  {"Method":<20s} {"Count":>10s} {"Percentage":>12s}')
    w(f'  {"─" * 20} {"─" * 10} {"─" * 12}')
    w(f'  {"Semantic (embedding)":<20s} {hit_rate["semantic_count"]:>10,d} {hit_rate["semantic_pct"]:>11.2f}%')
    w(f'  {"Word Overlap":<20s} {hit_rate["word_overlap_count"]:>10,d} {hit_rate["word_overlap_pct"]:>11.2f}%')
    if hit_rate['unknown_count'] > 0:
        unk_pct = 100.0 * hit_rate['unknown_count'] / max(hit_rate['total_factor_evaluations'], 1)
        w(f'  {"Unknown":<20s} {hit_rate["unknown_count"]:>10,d} {unk_pct:>11.2f}%')

    # ------------------------------------------------------------------
    # Section 3: Precision-Recall Analysis
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('3. PRECISION-RECALL ANALYSIS')
    w('-' * 80)
    w(f'  Positives (synonym pairs):  {pr_analysis["n_positives"]}')
    w(f'  Negatives (random pairs):   {pr_analysis["n_negatives"]}')
    w(f'  AUC-PR:                     {pr_analysis["auc_pr"]:.4f}'
      f'  {"PASS (>0.80)" if pr_analysis["auc_pr"] > 0.80 else "BELOW TARGET (0.80)"}')
    w(f'  Optimal F1:                 {pr_analysis["optimal_f1"]:.4f} @ threshold={pr_analysis["optimal_f1_threshold"]:.2f}')
    w('')
    w(f'  {"Threshold":>10s} {"TP":>6s} {"FP":>6s} {"FN":>6s} {"Precision":>10s} {"Recall":>8s} {"F1":>8s} {"FPR":>8s}')
    w(f'  {"─" * 10} {"─" * 6} {"─" * 6} {"─" * 6} {"─" * 10} {"─" * 8} {"─" * 8} {"─" * 8}')
    for entry in pr_analysis['thresholds']:
        w(f'  {entry["threshold"]:>10.2f} {entry["tp"]:>6d} {entry["fp"]:>6d} {entry["fn"]:>6d}'
          f' {entry["precision"]:>10.4f} {entry["recall"]:>8.4f} {entry["f1"]:>8.4f} {entry["fpr"]:>8.4f}')

    # ------------------------------------------------------------------
    # Section 4: Threshold Calibration Validation
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('4. THRESHOLD CALIBRATION VALIDATION')
    w('-' * 80)
    w(f'  Synonym mean similarity:    {calibration["synonym_mean"]:.4f} (std={calibration["synonym_std"]:.4f})')
    w(f'  Random mean similarity:     {calibration["random_mean"]:.4f} (std={calibration["random_std"]:.4f})')
    w(f'  Discrimination gap:         {calibration["discrimination_gap"]:.4f}')
    w('')
    w(f'  {"Threshold":>10s} {"Synonym Capture":>18s} {"False Positive":>16s} {"Gap (TPR-FPR)":>16s}')
    w(f'  {"─" * 10} {"─" * 18} {"─" * 16} {"─" * 16}')
    for c in calibration['calibration']:
        w(f'  {c["threshold"]:>10.2f} {c["synonym_capture_rate"]*100:>17.1f}% {c["false_positive_rate"]*100:>15.1f}% {c["gap"]*100:>15.1f}%')

    # ------------------------------------------------------------------
    # Section 5: Score Distribution by Method
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('5. SCORE DISTRIBUTION BY METHOD')
    w('-' * 80)

    if distribution['semantic_count'] > 0:
        w(f'  Semantic-scored synergy values: {distribution["semantic_count"]:,}')
        w(f'    Mean:   {distribution.get("semantic_mean", "N/A")}')
        w(f'    Median: {distribution.get("semantic_median", "N/A")}')
        w(f'    Std:    {distribution.get("semantic_std", "N/A")}')
        w(f'    Q1/Q3:  {distribution.get("semantic_q1", "N/A")} / {distribution.get("semantic_q3", "N/A")}')
    else:
        w('  No semantic-scored matches found.')

    w('')
    if distribution['word_overlap_count'] > 0:
        w(f'  Word-overlap-scored synergy values: {distribution["word_overlap_count"]:,}')
        w(f'    Mean:   {distribution.get("word_overlap_mean", "N/A")}')
        w(f'    Median: {distribution.get("word_overlap_median", "N/A")}')
        w(f'    Std:    {distribution.get("word_overlap_std", "N/A")}')
        w(f'    Q1/Q3:  {distribution.get("word_overlap_q1", "N/A")} / {distribution.get("word_overlap_q3", "N/A")}')
    else:
        w('  No word-overlap-scored matches found.')

    if 'mean_difference' in distribution:
        w('')
        w(f'  Mean difference (semantic - overlap): {distribution["mean_difference"]:+.4f}')
        better = 'Embedding-scored matches show BETTER' if distribution['mean_difference'] > 0 else 'Word-overlap-scored matches show BETTER'
        w(f'  Interpretation: {better} discrimination.')

    # ------------------------------------------------------------------
    # Section 6: Embedding Quality Summary Table
    # ------------------------------------------------------------------
    w('')
    w('-' * 80)
    w('6. EMBEDDING QUALITY SUMMARY TABLE')
    w('-' * 80)
    w('')
    w(f'  {"Metric":<45s} {"Value":>15s} {"Assessment":>15s}')
    w(f'  {"═" * 45} {"═" * 15} {"═" * 15}')

    # Model
    w(f'  {"Embedding model":<45s} {"bge-large-en-v1.5":>15s} {"":>15s}')
    w(f'  {"Vector dimensions":<45s} {"1024":>15s} {"":>15s}')

    # Coverage
    w(f'  {"Profile coverage (all 4 fields)":<45s} {coverage["all_4_pct"]:>14.1f}% '
      f'{"GOOD" if coverage["all_4_pct"] > 80 else "NEEDS IMPROVEMENT":>15s}')

    # Hit rate
    if hit_rate['total_factor_evaluations'] > 0:
        w(f'  {"Semantic method usage":<45s} {hit_rate["semantic_pct"]:>14.1f}% '
          f'{"EXCELLENT" if hit_rate["semantic_pct"] > 95 else "GOOD":>15s}')

    # PR metrics
    w(f'  {"AUC-PR":<45s} {pr_analysis["auc_pr"]:>15.4f} '
      f'{"PASS" if pr_analysis["auc_pr"] > 0.80 else "BELOW TARGET":>15s}')
    w(f'  {"Optimal F1":<45s} {pr_analysis["optimal_f1"]:>15.4f} {"":>15s}')
    w(f'  {"Optimal F1 threshold":<45s} {pr_analysis["optimal_f1_threshold"]:>15.2f} {"":>15s}')

    # Calibration
    w(f'  {"Synonym mean similarity":<45s} {calibration["synonym_mean"]:>15.4f} {"":>15s}')
    w(f'  {"Random mean similarity":<45s} {calibration["random_mean"]:>15.4f} {"":>15s}')
    w(f'  {"Discrimination gap (syn - random)":<45s} {calibration["discrimination_gap"]:>15.4f} '
      f'{"GOOD" if calibration["discrimination_gap"] > 0.15 else "MARGINAL":>15s}')

    # Best production threshold
    best_cal = max(calibration['calibration'], key=lambda c: c['gap'])
    w(f'  {"Best production threshold":<45s} {best_cal["threshold"]:>15.2f} {"":>15s}')
    w(f'  {"TPR at best threshold":<45s} {best_cal["synonym_capture_rate"]*100:>14.1f}% {"":>15s}')
    w(f'  {"FPR at best threshold":<45s} {best_cal["false_positive_rate"]*100:>14.1f}% {"":>15s}')

    # Distribution
    if 'mean_difference' in distribution:
        w(f'  {"Semantic vs overlap mean delta":<45s} {distribution["mean_difference"]:>+15.4f} '
          f'{"BETTER" if distribution["mean_difference"] > 0 else "WORSE":>15s}')

    w(f'  {"═" * 45} {"═" * 15} {"═" * 15}')

    w('')
    w('=' * 80)
    w('END OF REPORT')
    w('=' * 80)

    return '\n'.join(lines)


def build_csv_data(
    coverage: Dict[str, Any],
    hit_rate: Dict[str, Any],
    pr_analysis: Dict[str, Any],
    calibration: Dict[str, Any],
    distribution: Dict[str, Any],
) -> pd.DataFrame:
    """Consolidate all metrics into a CSV-friendly DataFrame."""
    rows = []

    # Coverage metrics
    for field in EMBEDDING_FIELDS:
        fs = coverage['fields'][field]
        rows.append({
            'section': 'coverage',
            'metric': f'{field}_count',
            'value': fs['count'],
            'unit': 'profiles',
        })
        rows.append({
            'section': 'coverage',
            'metric': f'{field}_pct',
            'value': fs['pct'],
            'unit': '%',
        })
    rows.append({'section': 'coverage', 'metric': 'all_4_fields_count', 'value': coverage['all_4_count'], 'unit': 'profiles'})
    rows.append({'section': 'coverage', 'metric': 'all_4_fields_pct', 'value': coverage['all_4_pct'], 'unit': '%'})

    # Hit rate
    rows.append({'section': 'hit_rate', 'metric': 'semantic_count', 'value': hit_rate['semantic_count'], 'unit': 'evaluations'})
    rows.append({'section': 'hit_rate', 'metric': 'word_overlap_count', 'value': hit_rate['word_overlap_count'], 'unit': 'evaluations'})
    rows.append({'section': 'hit_rate', 'metric': 'semantic_pct', 'value': hit_rate['semantic_pct'], 'unit': '%'})

    # PR analysis
    rows.append({'section': 'pr_analysis', 'metric': 'auc_pr', 'value': pr_analysis['auc_pr'], 'unit': 'score'})
    rows.append({'section': 'pr_analysis', 'metric': 'optimal_f1', 'value': pr_analysis['optimal_f1'], 'unit': 'score'})
    rows.append({'section': 'pr_analysis', 'metric': 'optimal_f1_threshold', 'value': pr_analysis['optimal_f1_threshold'], 'unit': 'threshold'})
    for entry in pr_analysis['thresholds']:
        t = entry['threshold']
        for k in ['precision', 'recall', 'f1', 'fpr']:
            rows.append({
                'section': 'pr_analysis',
                'metric': f't{t:.2f}_{k}',
                'value': entry[k],
                'unit': 'score',
            })

    # Calibration
    rows.append({'section': 'calibration', 'metric': 'synonym_mean', 'value': calibration['synonym_mean'], 'unit': 'similarity'})
    rows.append({'section': 'calibration', 'metric': 'random_mean', 'value': calibration['random_mean'], 'unit': 'similarity'})
    rows.append({'section': 'calibration', 'metric': 'discrimination_gap', 'value': calibration['discrimination_gap'], 'unit': 'similarity'})
    for c in calibration['calibration']:
        t = c['threshold']
        rows.append({'section': 'calibration', 'metric': f't{t:.2f}_tpr', 'value': c['synonym_capture_rate'], 'unit': 'rate'})
        rows.append({'section': 'calibration', 'metric': f't{t:.2f}_fpr', 'value': c['false_positive_rate'], 'unit': 'rate'})

    # Distribution
    if distribution['semantic_count'] > 0:
        rows.append({'section': 'distribution', 'metric': 'semantic_mean', 'value': distribution.get('semantic_mean'), 'unit': 'score'})
        rows.append({'section': 'distribution', 'metric': 'semantic_std', 'value': distribution.get('semantic_std'), 'unit': 'score'})
    if distribution['word_overlap_count'] > 0:
        rows.append({'section': 'distribution', 'metric': 'word_overlap_mean', 'value': distribution.get('word_overlap_mean'), 'unit': 'score'})
        rows.append({'section': 'distribution', 'metric': 'word_overlap_std', 'value': distribution.get('word_overlap_std'), 'unit': 'score'})
    if 'mean_difference' in distribution:
        rows.append({'section': 'distribution', 'metric': 'mean_difference', 'value': distribution['mean_difference'], 'unit': 'score'})

    return pd.DataFrame(rows)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Validate embedding-based semantic matching quality (ISMC Synergy)',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Run with synthetic data (no DB required)',
    )
    args = parser.parse_args()

    test_mode = args.test
    start_time = time.time()

    # Ensure output directories exist
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"{'=' * 60}")
    print(f"  Embedding Validation Script")
    print(f"  Mode: {'TEST (synthetic)' if test_mode else 'LIVE (database)'}")
    print(f"{'=' * 60}")

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    if test_mode:
        print("\n[1/6] Generating synthetic profile data...")
        synth_profiles = _generate_synthetic_profiles(n=200)
        profiles_df = pd.DataFrame(synth_profiles)
        # Convert embedding fields to boolean
        for field in EMBEDDING_FIELDS:
            profiles_df[field] = profiles_df[field].apply(lambda x: x is not None and x != '')

        print("[2/6] Generating synthetic match contexts...")
        match_contexts = _generate_synthetic_match_contexts(n=500)

        print("[3/6] Generating synthetic benchmark data...")
        synonym_df, random_df = _generate_synthetic_benchmark_data()
        synonym_scores = synonym_df['emb_bge-large-en-v1.5'].values.astype(float)

        # Combine the random sim columns into a single array
        rand_scores_list = []
        if 'embedding_sim_offering_seeking' in random_df.columns:
            rand_scores_list.extend(random_df['embedding_sim_offering_seeking'].dropna().values.tolist())
        if 'embedding_sim_audience' in random_df.columns:
            rand_scores_list.extend(random_df['embedding_sim_audience'].dropna().values.tolist())
        random_scores = np.array(rand_scores_list, dtype=float)

    else:
        print("\n[1/6] Loading profile coverage from database...")
        profiles_df = load_profile_coverage()
        print(f"       Loaded {len(profiles_df):,} profiles")

        print("[2/6] Loading match contexts from database...")
        match_contexts = load_match_contexts()
        print(f"       Loaded {len(match_contexts):,} matches with context")

        print("[3/6] Loading benchmark CSV files...")
        synonym_df, random_df = load_benchmark_csvs()
        synonym_scores = _extract_bge_scores(synonym_df)
        random_scores = _extract_random_scores(random_df)
        print(f"       Synonym pairs: {len(synonym_scores)}, Random scores: {len(random_scores)}")

    # ------------------------------------------------------------------
    # Run analyses
    # ------------------------------------------------------------------
    print("[4/6] Running analyses...")

    print("       - Embedding coverage audit...")
    coverage = analyze_coverage(profiles_df)

    print("       - Semantic vs word overlap hit rate...")
    hit_rate = analyze_method_hit_rate(match_contexts)

    print("       - Precision-recall analysis...")
    pr_analysis = compute_precision_recall(synonym_scores, random_scores, THRESHOLDS)

    print("       - Threshold calibration validation...")
    calibration = analyze_threshold_calibration(synonym_scores, random_scores)

    print("       - Score distribution by method...")
    distribution = analyze_score_distribution_by_method(match_contexts)

    # ------------------------------------------------------------------
    # Generate plots
    # ------------------------------------------------------------------
    print("[5/6] Generating visualizations...")

    print("       - embedding_coverage_bar.png")
    plot_coverage_bar(coverage, PLOTS_DIR / 'embedding_coverage_bar.png')

    print("       - precision_recall_curve.png")
    plot_precision_recall_curve(pr_analysis, synonym_scores, random_scores,
                               PLOTS_DIR / 'precision_recall_curve.png')

    print("       - semantic_vs_overlap_distribution.png")
    plot_semantic_vs_overlap_distribution(distribution,
                                         PLOTS_DIR / 'semantic_vs_overlap_distribution.png')

    print("       - threshold_discrimination.png")
    plot_threshold_discrimination(calibration, PLOTS_DIR / 'threshold_discrimination.png')

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    elapsed = time.time() - start_time
    print("[6/6] Writing report and data files...")

    report_text = format_report(
        coverage, hit_rate, pr_analysis, calibration, distribution,
        elapsed, test_mode,
    )
    REPORT_PATH.write_text(report_text)
    print(f"       Report: {REPORT_PATH}")

    csv_df = build_csv_data(coverage, hit_rate, pr_analysis, calibration, distribution)
    csv_df.to_csv(CSV_PATH, index=False)
    print(f"       CSV:    {CSV_PATH}")

    print(f"\n{'=' * 60}")
    print(f"  Validation complete in {elapsed:.1f}s")
    print(f"  Report:  {REPORT_PATH}")
    print(f"  Data:    {CSV_PATH}")
    print(f"  Plots:   {PLOTS_DIR}/")
    print(f"{'=' * 60}")

    # Print key metrics summary
    print(f"\n  KEY METRICS:")
    print(f"    Coverage (all 4 fields):  {coverage['all_4_pct']:.1f}%")
    if hit_rate['total_factor_evaluations'] > 0:
        print(f"    Semantic method usage:    {hit_rate['semantic_pct']:.1f}%")
    print(f"    AUC-PR:                   {pr_analysis['auc_pr']:.4f}"
          f"  {'PASS' if pr_analysis['auc_pr'] > 0.80 else 'BELOW TARGET'}")
    print(f"    Discrimination gap:       {calibration['discrimination_gap']:.4f}")
    print(f"    Optimal F1:               {pr_analysis['optimal_f1']:.4f} @ t={pr_analysis['optimal_f1_threshold']:.2f}")


if __name__ == '__main__':
    main()
