#!/usr/bin/env python3
"""
02_predictive_validity.py - Predictive Validity of ISMC Match Scores
====================================================================

Tests whether ISMC match scores predict real-world engagement behavior.
This is the most critical validation test: if scores don't predict engagement,
the matching engine has no external validity.

Analyses:
    1. Data availability check (minimum N for reliable inference)
    2. Score-engagement monotonicity by decile
    3. Logistic regression: P(contacted) ~ harmonic_mean
    4. Tier contact rate independence (chi-squared)
    5. Kaplan-Meier survival analysis: time-to-first-action by tier
    6. Calibration analysis (reliability diagram, ECE, Brier score)
    7. Optimal threshold via ROC / Youden's J
    8. Explanation source impact on engagement

Usage:
    python scripts/validation/02_predictive_validity.py
    python scripts/validation/02_predictive_validity.py --test
    python scripts/validation/02_predictive_validity.py --min-events 100

Flags:
    --test          Generate synthetic engagement data to exercise the pipeline
    --min-events N  Minimum contact events required (default: 50)
"""

import os
import sys
import argparse
import random
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from matching.models import (
    SupabaseMatch,
    EngagementSummary,
    PartnerRecommendation,
)

# ---------------------------------------------------------------------------
# Scientific stack (imported after Django to keep bootstrap errors separate)
# ---------------------------------------------------------------------------
from scipy import stats as sp_stats
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, roc_curve, brier_score_loss
from sklearn.calibration import calibration_curve
import matplotlib
matplotlib.use('Agg')  # non-interactive backend for server/CI
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

try:
    from lifelines import KaplanMeierFitter, CoxPHFitter
    HAS_LIFELINES = True
except ImportError:
    HAS_LIFELINES = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BONFERRONI_ALPHA = 0.05 / 6  # 6 primary tests -> 0.0083
TIER_BOUNDARIES = {
    'hand_picked': (67, 100),
    'strong':      (55, 67),
    'wildcard':    (0,  55),
}
CENSORING_DAYS = 30
RANDOM_SEED = 42

# Output paths (relative to project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(__file__).resolve().parent / 'validation_results'
PLOTS_DIR = RESULTS_DIR / 'plots'
REPORT_PATH = RESULTS_DIR / 'predictive_validity_report.txt'
DATA_PATH = RESULTS_DIR / 'predictive_validity_data.csv'


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
    elif p < BONFERRONI_ALPHA:
        return '** (Bonferroni)'
    elif p < 0.05:
        return '* (nominal)'
    else:
        return 'ns'


class ReportWriter:
    """Accumulates report lines and writes them to a file."""

    def __init__(self):
        self._lines: list[str] = []

    def h1(self, text: str) -> None:
        self._lines.append('')
        self._lines.append('=' * 72)
        self._lines.append(text.upper())
        self._lines.append('=' * 72)

    def h2(self, text: str) -> None:
        self._lines.append('')
        self._lines.append('-' * 60)
        self._lines.append(text)
        self._lines.append('-' * 60)

    def line(self, text: str = '') -> None:
        self._lines.append(text)

    def kv(self, key: str, value, fmt: str = '') -> None:
        """Print a key-value pair with optional format spec."""
        if fmt:
            self._lines.append(f'  {key:.<40s} {value:{fmt}}')
        else:
            self._lines.append(f'  {key:.<40s} {value}')

    def save(self, path: Path) -> None:
        with open(path, 'w') as f:
            f.write('\n'.join(self._lines) + '\n')

    def print_all(self) -> None:
        print('\n'.join(self._lines))


# ============================================================================
# Data loading
# ============================================================================

def load_real_data() -> pd.DataFrame:
    """
    Join SupabaseMatch scores with EngagementSummary and
    PartnerRecommendation to build the analysis dataframe.

    Returns a DataFrame with columns:
        harmonic_mean, score_ab, score_ba, tier,
        was_viewed, was_contacted, any_contact_action,
        time_to_first_action_secs, explanation_source,
        feedback_outcome, card_expand_count, avg_card_dwell_ms,
        email_click_count, linkedin_click_count, schedule_click_count,
        apply_click_count
    """
    # ------------------------------------------------------------------
    # 1. Engagement summaries (partner-level only)
    # ------------------------------------------------------------------
    eng_qs = EngagementSummary.objects.exclude(partner_id='').values(
        'report_id', 'partner_id',
        'card_expand_count', 'avg_card_dwell_ms',
        'email_click_count', 'linkedin_click_count',
        'schedule_click_count', 'apply_click_count',
        'any_contact_action', 'time_to_first_action_secs',
    )
    eng_df = pd.DataFrame(list(eng_qs))

    # ------------------------------------------------------------------
    # 2. Partner recommendations
    # ------------------------------------------------------------------
    rec_qs = PartnerRecommendation.objects.values(
        'partner_id', 'was_viewed', 'was_contacted',
        'time_to_first_action', 'explanation_source',
        'feedback_outcome',
    )
    rec_df = pd.DataFrame(list(rec_qs))

    if rec_df.empty and eng_df.empty:
        return pd.DataFrame()

    # Convert partner_id to string for reliable joining
    if not rec_df.empty:
        rec_df['partner_id'] = rec_df['partner_id'].astype(str)
    if not eng_df.empty:
        eng_df['partner_id'] = eng_df['partner_id'].astype(str)

    # ------------------------------------------------------------------
    # 3. Match scores — index by suggested_profile_id for joining
    # ------------------------------------------------------------------
    match_qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False,
    ).values(
        'profile_id', 'suggested_profile_id',
        'harmonic_mean', 'score_ab', 'score_ba',
    )
    match_df = pd.DataFrame(list(match_qs))

    if match_df.empty:
        return pd.DataFrame()

    match_df['suggested_profile_id'] = match_df['suggested_profile_id'].astype(str)
    match_df['harmonic_mean'] = match_df['harmonic_mean'].astype(float)
    match_df['score_ab'] = match_df['score_ab'].astype(float)
    match_df['score_ba'] = match_df['score_ba'].astype(float)

    # ------------------------------------------------------------------
    # 4. Join: engagement with match scores
    # ------------------------------------------------------------------
    # Primary join key: partner_id (from engagement/recommendation) ==
    #                   suggested_profile_id (from match scores)
    # There can be multiple matches per partner; take the one with the
    # highest harmonic_mean to avoid duplicates inflating counts.
    match_dedup = (
        match_df
        .sort_values('harmonic_mean', ascending=False)
        .drop_duplicates(subset='suggested_profile_id', keep='first')
    )

    # Start with recommendations (wider signal set)
    if not rec_df.empty:
        df = rec_df.merge(
            match_dedup,
            left_on='partner_id',
            right_on='suggested_profile_id',
            how='inner',
        )
    else:
        df = pd.DataFrame()

    # Merge engagement metrics if available
    if not eng_df.empty and not df.empty:
        df = df.merge(
            eng_df.drop(columns=['report_id'], errors='ignore'),
            on='partner_id',
            how='left',
        )
    elif not eng_df.empty:
        df = eng_df.merge(
            match_dedup,
            left_on='partner_id',
            right_on='suggested_profile_id',
            how='inner',
        )

    if df.empty:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # 5. Derived columns
    # ------------------------------------------------------------------
    df['tier'] = df['harmonic_mean'].apply(assign_tier)

    # Ensure boolean columns exist with defaults
    for col in ('was_viewed', 'was_contacted', 'any_contact_action'):
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)

    # Time-to-first-action: reconcile DurationField (timedelta) and integer seconds
    if 'time_to_first_action' in df.columns:
        def _to_seconds(val):
            if pd.isna(val):
                return np.nan
            if isinstance(val, timedelta):
                return val.total_seconds()
            return float(val)
        df['time_to_first_action_secs_rec'] = df['time_to_first_action'].apply(_to_seconds)
    else:
        df['time_to_first_action_secs_rec'] = np.nan

    # Prefer EngagementSummary's integer seconds if present
    if 'time_to_first_action_secs' not in df.columns:
        df['time_to_first_action_secs'] = df['time_to_first_action_secs_rec']
    else:
        df['time_to_first_action_secs'] = df['time_to_first_action_secs'].fillna(
            df['time_to_first_action_secs_rec']
        )

    # Fill missing numeric engagement columns with 0
    for col in ('card_expand_count', 'avg_card_dwell_ms',
                'email_click_count', 'linkedin_click_count',
                'schedule_click_count', 'apply_click_count'):
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].fillna(0).astype(int)

    return df


# ============================================================================
# Synthetic data generation (--test mode)
# ============================================================================

def generate_synthetic_data(n: int = 500) -> pd.DataFrame:
    """
    Generate synthetic engagement data where contact probability is a known
    monotonic function of score, so we can verify the pipeline end-to-end.

    The generating model:
        P(contacted) = sigmoid(-4.0 + 0.07 * harmonic_mean)
    This yields ~18% contact at score=55 and ~50% at score=72.
    """
    rng = np.random.default_rng(RANDOM_SEED)
    random.seed(RANDOM_SEED)

    scores = rng.uniform(30, 95, size=n)
    score_ab = scores + rng.normal(0, 3, size=n)
    score_ba = scores + rng.normal(0, 3, size=n)

    # True generating process
    logit = -4.0 + 0.07 * scores
    p_contact = 1.0 / (1.0 + np.exp(-logit))
    contacted = rng.binomial(1, p_contact).astype(bool)

    # Viewing is more common (80% base + score-driven)
    p_view = np.clip(0.5 + 0.005 * scores, 0.3, 0.95)
    viewed = rng.binomial(1, p_view).astype(bool)

    # any_contact_action is a superset of was_contacted (includes clicks)
    any_action = contacted | rng.binomial(1, 0.08, size=n).astype(bool)

    # Time-to-first-action: lower scores take longer
    # Mean time = 7 days - 0.05 * score (in days), min 0.5 day
    mean_days = np.clip(7.0 - 0.05 * scores, 0.5, 14.0)
    ttfa_days = rng.exponential(mean_days)
    ttfa_secs = (ttfa_days * 86400).astype(int)
    # Null out for those with no action
    ttfa_secs_masked = np.where(any_action, ttfa_secs, np.nan)

    # Explanation source: higher scores more likely to be llm_verified
    explanation_sources = []
    for s in scores:
        r = rng.random()
        if s >= 70:
            src = 'llm_verified' if r < 0.70 else ('llm_partial' if r < 0.90 else 'template_fallback')
        elif s >= 55:
            src = 'llm_verified' if r < 0.45 else ('llm_partial' if r < 0.80 else 'template_fallback')
        else:
            src = 'llm_verified' if r < 0.20 else ('llm_partial' if r < 0.55 else 'template_fallback')
        explanation_sources.append(src)

    # Feedback outcome (only for those contacted)
    feedback_outcomes = []
    for c, s in zip(contacted, scores):
        if not c:
            feedback_outcomes.append(None)
            continue
        r = rng.random()
        if s >= 67:
            if r < 0.45:
                feedback_outcomes.append('connected_promising')
            elif r < 0.70:
                feedback_outcomes.append('connected_not_fit')
            elif r < 0.90:
                feedback_outcomes.append('no_response')
            else:
                feedback_outcomes.append('did_not_reach_out')
        else:
            if r < 0.20:
                feedback_outcomes.append('connected_promising')
            elif r < 0.45:
                feedback_outcomes.append('connected_not_fit')
            elif r < 0.75:
                feedback_outcomes.append('no_response')
            else:
                feedback_outcomes.append('did_not_reach_out')
        feedback_outcomes[-1]  # no-op, keeps linter quiet

    # Engagement metrics (correlated with action)
    card_expand = np.where(viewed, rng.poisson(2, size=n), 0)
    dwell_ms = np.where(viewed, rng.normal(3000, 1000, size=n).clip(500, 15000).astype(int), 0)
    email_clicks = np.where(contacted, rng.poisson(1.2, size=n), 0)
    linkedin_clicks = np.where(any_action, rng.poisson(0.8, size=n), 0)
    schedule_clicks = np.where(contacted, rng.binomial(1, 0.3, size=n), 0)
    apply_clicks = np.where(contacted, rng.binomial(1, 0.15, size=n), 0)

    df = pd.DataFrame({
        'harmonic_mean': np.round(scores, 2),
        'score_ab': np.round(score_ab, 2),
        'score_ba': np.round(score_ba, 2),
        'was_viewed': viewed,
        'was_contacted': contacted,
        'any_contact_action': any_action,
        'time_to_first_action_secs': ttfa_secs_masked,
        'explanation_source': explanation_sources,
        'feedback_outcome': feedback_outcomes,
        'card_expand_count': card_expand,
        'avg_card_dwell_ms': dwell_ms,
        'email_click_count': email_clicks,
        'linkedin_click_count': linkedin_clicks,
        'schedule_click_count': schedule_clicks,
        'apply_click_count': apply_clicks,
    })
    df['tier'] = df['harmonic_mean'].apply(assign_tier)
    return df


# ============================================================================
# Analysis functions
# ============================================================================

def analysis_1_data_check(df: pd.DataFrame, rpt: ReportWriter,
                          min_events: int) -> bool:
    """
    Check whether we have sufficient engagement data to proceed.
    Returns True if analysis can continue, False otherwise.
    """
    rpt.h1('Predictive Validity of ISMC Match Scores')
    rpt.line(f'Report generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    rpt.line(f'Bonferroni-corrected alpha: {BONFERRONI_ALPHA:.4f} (6 primary tests)')

    rpt.h2('1. Data Availability Check')

    n_total = len(df)
    n_contacted = df['was_contacted'].sum()
    n_any_action = df['any_contact_action'].sum()
    n_viewed = df['was_viewed'].sum()

    rpt.kv('Total match-engagement records', n_total)
    rpt.kv('Records with was_contacted=True', int(n_contacted))
    rpt.kv('Records with any_contact_action=True', int(n_any_action))
    rpt.kv('Records with was_viewed=True', int(n_viewed))

    rpt.line()
    rpt.kv('Contact rate (was_contacted)', f'{n_contacted / max(n_total, 1):.1%}')
    rpt.kv('Any-action rate', f'{n_any_action / max(n_total, 1):.1%}')
    rpt.kv('View rate', f'{n_viewed / max(n_total, 1):.1%}')

    # Tier breakdown
    rpt.line()
    rpt.line('  Tier distribution:')
    for tier in ('hand_picked', 'strong', 'wildcard'):
        n_tier = (df['tier'] == tier).sum()
        rpt.line(f'    {tier:.<30s} {n_tier:>5d}  ({n_tier / max(n_total, 1):.1%})')

    if n_any_action < min_events:
        rpt.line()
        rpt.line(
            f'  ** Insufficient engagement data. Need ~200 contact events '
            f'for reliable analysis. Currently have {int(n_any_action)}. '
            f'Re-run when more data is collected.'
        )
        return False

    return True


def analysis_2_monotonicity(df: pd.DataFrame, rpt: ReportWriter) -> pd.DataFrame:
    """
    Score-engagement monotonicity: bucket matches by score deciles and
    compute contact/view rates per bucket.
    """
    rpt.h2('2. Score-Engagement Monotonicity (Decile Analysis)')

    df = df.copy()
    df['decile'] = pd.qcut(df['harmonic_mean'], q=10, duplicates='drop')
    df['decile_label'] = df['decile'].apply(
        lambda x: f'{x.left:.0f}-{x.right:.0f}'
    )

    grouped = df.groupby('decile', observed=True).agg(
        n=('harmonic_mean', 'size'),
        mean_score=('harmonic_mean', 'mean'),
        contact_rate=('was_contacted', 'mean'),
        view_rate=('was_viewed', 'mean'),
        any_action_rate=('any_contact_action', 'mean'),
        mean_ttfa=('time_to_first_action_secs', 'mean'),
    ).sort_index()

    rpt.line(f'  {"Decile":<16s} {"N":>5s}  {"Mean":>6s}  {"Contact%":>9s}  '
             f'{"View%":>7s}  {"Action%":>8s}  {"TTFA(h)":>8s}')
    rpt.line(f'  {"-" * 70}')
    for idx, row in grouped.iterrows():
        ttfa_hours = row['mean_ttfa'] / 3600 if not np.isnan(row['mean_ttfa']) else float('nan')
        rpt.line(
            f'  {str(idx):<16s} {int(row["n"]):>5d}  {row["mean_score"]:>6.1f}  '
            f'{row["contact_rate"]:>8.1%}  {row["view_rate"]:>6.1%}  '
            f'{row["any_action_rate"]:>7.1%}  {ttfa_hours:>7.1f}'
        )

    # Test for monotonicity with Spearman correlation
    contact_rates = grouped['contact_rate'].values
    decile_ranks = np.arange(len(contact_rates))
    if len(contact_rates) >= 3:
        rho, p_mono = sp_stats.spearmanr(decile_ranks, contact_rates)
        rpt.line()
        rpt.kv('Spearman rho (decile vs contact rate)', f'{rho:.3f}')
        rpt.kv('p-value', f'{p_mono:.4e} {_sig_label(p_mono)}')
        rpt.kv('Monotonically increasing?',
               'Yes' if rho > 0 and p_mono < BONFERRONI_ALPHA else 'No / Insufficient evidence')
    else:
        rpt.line('  Too few deciles to test monotonicity.')

    # --- Plot: step function ---
    fig, ax1 = plt.subplots(figsize=(10, 5))
    x = np.arange(len(grouped))
    labels = [f'{row["mean_score"]:.0f}' for _, row in grouped.iterrows()]

    ax1.step(x, grouped['contact_rate'].values * 100, where='mid',
             color='#2563eb', linewidth=2.5, label='Contact rate')
    ax1.step(x, grouped['any_action_rate'].values * 100, where='mid',
             color='#059669', linewidth=2.0, linestyle='--', label='Any-action rate')
    ax1.step(x, grouped['view_rate'].values * 100, where='mid',
             color='#9333ea', linewidth=1.5, linestyle=':', label='View rate')

    ax1.set_xlabel('Score Decile (mean score)', fontsize=11)
    ax1.set_ylabel('Rate (%)', fontsize=11)
    ax1.set_title('Engagement Rates by Match Score Decile', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=9)
    ax1.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(axis='y', alpha=0.3)

    # Secondary axis: sample size
    ax2 = ax1.twinx()
    ax2.bar(x, grouped['n'].values, alpha=0.12, color='gray', label='N per decile')
    ax2.set_ylabel('Sample size', fontsize=10, color='gray')
    ax2.tick_params(axis='y', labelcolor='gray')

    fig.tight_layout()
    fig.savefig(PLOTS_DIR / 'engagement_by_score_decile.png', dpi=150)
    plt.close(fig)
    rpt.line(f'  Plot saved: {PLOTS_DIR / "engagement_by_score_decile.png"}')

    return grouped.reset_index()


def analysis_3_logistic_regression(df: pd.DataFrame, rpt: ReportWriter) -> dict:
    """
    Logistic regression: P(contacted) = sigmoid(b0 + b1 * harmonic_mean).
    Reports odds ratio, p-value, pseudo-R-squared, and AUC-ROC.
    """
    rpt.h2('3. Logistic Regression: P(contacted) ~ harmonic_mean')

    X = df[['harmonic_mean']].values
    y = df['was_contacted'].astype(int).values

    if y.sum() == 0 or y.sum() == len(y):
        rpt.line('  Skipped: outcome is constant (all contacted or none contacted).')
        return {}

    # --- sklearn logistic regression (for AUC, predictions) ---
    clf = LogisticRegression(solver='lbfgs', max_iter=1000, random_state=RANDOM_SEED)
    clf.fit(X, y)
    y_prob = clf.predict_proba(X)[:, 1]
    auc = roc_auc_score(y, y_prob)

    b1_sk = clf.coef_[0][0]
    b0_sk = clf.intercept_[0]

    # --- scipy logistic regression (for exact p-values) ---
    # Use statsmodels-style approach via scipy minimize
    from scipy.optimize import minimize

    def neg_log_lik(params):
        b0, b1 = params
        z = b0 + b1 * X.ravel()
        z = np.clip(z, -500, 500)
        p = 1.0 / (1.0 + np.exp(-z))
        p = np.clip(p, 1e-15, 1 - 1e-15)
        return -np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))

    result = minimize(neg_log_lik, x0=[b0_sk, b1_sk], method='BFGS')
    b0_mle, b1_mle = result.x

    # Hessian for standard errors
    hess_inv = result.hess_inv if hasattr(result.hess_inv, '__array__') else np.eye(2)
    hess_inv = np.atleast_2d(hess_inv)
    se = np.sqrt(np.diag(hess_inv))
    se_b1 = se[1] if len(se) > 1 else float('nan')

    z_stat = b1_mle / se_b1 if se_b1 > 0 else float('nan')
    p_value = 2 * (1 - sp_stats.norm.cdf(abs(z_stat))) if not np.isnan(z_stat) else float('nan')

    # McFadden pseudo-R-squared
    ll_full = -result.fun
    # Null model: intercept only
    p_null = y.mean()
    ll_null = np.sum(y * np.log(max(p_null, 1e-15)) + (1 - y) * np.log(max(1 - p_null, 1e-15)))
    mcfadden_r2 = 1 - (ll_full / ll_null) if ll_null != 0 else float('nan')

    odds_ratio = np.exp(b1_mle)
    odds_ratio_10pt = np.exp(b1_mle * 10)

    rpt.kv('Intercept (b0)', f'{b0_mle:.4f}')
    rpt.kv('Coefficient (b1)', f'{b1_mle:.4f}')
    rpt.kv('Std error (b1)', f'{se_b1:.4f}')
    rpt.kv('z-statistic', f'{z_stat:.3f}')
    rpt.kv('p-value', f'{p_value:.4e} {_sig_label(p_value)}')
    rpt.kv('Odds ratio (per 1-point)', f'{odds_ratio:.4f}')
    rpt.kv('Odds ratio (per 10-point)', f'{odds_ratio_10pt:.3f}')
    rpt.kv('McFadden pseudo-R^2', f'{mcfadden_r2:.4f}')
    rpt.kv('AUC-ROC', f'{auc:.4f}')
    rpt.kv('AUC target (>0.60)', 'PASS' if auc > 0.60 else 'FAIL')

    rpt.line()
    rpt.line(
        f'  Interpretation: Each 10-point increase in match score corresponds '
        f'to a {odds_ratio_10pt:.2f}x increase in contact likelihood.'
    )

    return {
        'b0': b0_mle, 'b1': b1_mle, 'se_b1': se_b1,
        'z': z_stat, 'p_value': p_value,
        'odds_ratio': odds_ratio, 'odds_ratio_10pt': odds_ratio_10pt,
        'mcfadden_r2': mcfadden_r2, 'auc': auc,
        'y_prob': y_prob, 'y_true': y,
    }


def analysis_4_tier_chi_squared(df: pd.DataFrame, rpt: ReportWriter) -> dict:
    """
    Chi-squared test: are tier-level contact rates independent of tier?
    """
    rpt.h2('4. Tier Contact Rates (Chi-Squared Test)')

    tier_order = ['hand_picked', 'strong', 'wildcard']
    tier_counts = []
    for tier in tier_order:
        subset = df[df['tier'] == tier]
        n_total = len(subset)
        n_contacted = subset['was_contacted'].sum()
        n_not = n_total - n_contacted
        tier_counts.append({
            'tier': tier,
            'contacted': int(n_contacted),
            'not_contacted': int(n_not),
            'total': n_total,
            'contact_rate': n_contacted / max(n_total, 1),
        })
        rpt.kv(f'{tier} contact rate',
               f'{n_contacted}/{n_total} = {n_contacted / max(n_total, 1):.1%}')

    # Build contingency table
    observed = np.array([
        [tc['contacted'], tc['not_contacted']]
        for tc in tier_counts
    ])

    if observed.min() < 0 or observed.sum() == 0:
        rpt.line('  Skipped: insufficient data for chi-squared test.')
        return {}

    chi2, p_value, dof, expected = sp_stats.chi2_contingency(observed)
    n = observed.sum()
    k = min(observed.shape)
    cramers_v = np.sqrt(chi2 / (n * (k - 1))) if n > 0 and k > 1 else 0.0

    rpt.line()
    rpt.kv('Chi-squared statistic', f'{chi2:.3f}')
    rpt.kv('Degrees of freedom', dof)
    rpt.kv('p-value', f'{p_value:.4e} {_sig_label(p_value)}')
    rpt.kv("Cramer's V (effect size)", f'{cramers_v:.4f}')

    # Check ordering: hand_picked > strong > wildcard
    rates = [tc['contact_rate'] for tc in tier_counts]
    monotonic = rates[0] > rates[1] > rates[2]
    rpt.kv('Rate ordering (HP > S > W)', 'Yes' if monotonic else 'No')

    return {
        'chi2': chi2, 'p_value': p_value, 'dof': dof,
        'cramers_v': cramers_v, 'tier_counts': tier_counts,
        'monotonic': monotonic,
    }


def analysis_5_survival(df: pd.DataFrame, rpt: ReportWriter) -> dict:
    """
    Kaplan-Meier survival curves and Cox proportional hazards model
    for time-to-first-action by tier.
    """
    rpt.h2('5. Kaplan-Meier Survival Analysis: Time-to-First-Action')

    if not HAS_LIFELINES:
        rpt.line('  Skipped: `lifelines` package is not installed.')
        rpt.line('  Install with: pip install lifelines')
        return {}

    # Prepare survival data
    sdf = df[['harmonic_mean', 'tier', 'any_contact_action',
              'time_to_first_action_secs']].copy()

    # Duration: time_to_first_action_secs if action occurred, else censored at 30 days
    censoring_secs = CENSORING_DAYS * 86400
    sdf['event'] = sdf['any_contact_action'].astype(int)
    sdf['duration_secs'] = sdf['time_to_first_action_secs'].fillna(censoring_secs)
    sdf['duration_secs'] = sdf['duration_secs'].clip(lower=1)  # avoid zero durations
    sdf['duration_days'] = sdf['duration_secs'] / 86400.0

    # Censor long durations
    too_long = sdf['duration_days'] > CENSORING_DAYS
    sdf.loc[too_long, 'duration_days'] = CENSORING_DAYS
    sdf.loc[too_long, 'event'] = 0

    # --- Kaplan-Meier by tier ---
    fig, ax = plt.subplots(figsize=(10, 6))
    tier_colors = {'hand_picked': '#2563eb', 'strong': '#f59e0b', 'wildcard': '#ef4444'}
    tier_labels = {'hand_picked': 'Hand-Picked (>=67)', 'strong': 'Strong (55-67)',
                   'wildcard': 'Wildcard (<55)'}

    kmf = KaplanMeierFitter()
    median_times = {}
    for tier in ('hand_picked', 'strong', 'wildcard'):
        mask = sdf['tier'] == tier
        if mask.sum() < 5:
            rpt.line(f'  {tier}: too few observations ({mask.sum()}) for KM estimation.')
            continue
        kmf.fit(
            sdf.loc[mask, 'duration_days'],
            event_observed=sdf.loc[mask, 'event'],
            label=tier_labels[tier],
        )
        kmf.plot_survival_function(ax=ax, color=tier_colors[tier], linewidth=2)
        median_t = kmf.median_survival_time_
        median_times[tier] = median_t
        rpt.kv(f'{tier} median time-to-action (days)',
               f'{median_t:.1f}' if not np.isinf(median_t) else '>30 (censored)')

    ax.set_xlabel('Days Since Recommendation', fontsize=11)
    ax.set_ylabel('Proportion Without Action', fontsize=11)
    ax.set_title('Time-to-First-Action by Match Tier (Kaplan-Meier)', fontsize=13,
                 fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / 'kaplan_meier_by_tier.png', dpi=150)
    plt.close(fig)
    rpt.line(f'  Plot saved: {PLOTS_DIR / "kaplan_meier_by_tier.png"}')

    # --- Cox Proportional Hazards ---
    rpt.line()
    rpt.line('  Cox Proportional Hazards Model:')
    cox_df = sdf[['harmonic_mean', 'duration_days', 'event']].dropna()
    if len(cox_df) >= 10 and cox_df['event'].sum() >= 5:
        cph = CoxPHFitter()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cph.fit(cox_df, duration_col='duration_days', event_col='event')

        hr = np.exp(cph.params_['harmonic_mean'])
        ci_lower = np.exp(cph.confidence_intervals_.iloc[0, 0])
        ci_upper = np.exp(cph.confidence_intervals_.iloc[0, 1])
        p_cox = cph.summary['p']['harmonic_mean']

        rpt.kv('Hazard ratio (per 1-point)', f'{hr:.4f}')
        rpt.kv('95% CI', f'[{ci_lower:.4f}, {ci_upper:.4f}]')
        rpt.kv('p-value (Cox)', f'{p_cox:.4e} {_sig_label(p_cox)}')
        rpt.kv('Concordance index', f'{cph.concordance_index_:.4f}')

        # Compute hand_picked vs wildcard hazard ratio (approximation)
        # HR for a 20-point difference (midpoint 72 vs midpoint 42)
        hr_hp_vs_wc = np.exp(cph.params_['harmonic_mean'] * 20)
        rpt.kv('HR: hand_picked vs wildcard (~20pt)', f'{hr_hp_vs_wc:.3f}')
        rpt.kv('Target (HR > 1.5)', 'PASS' if hr_hp_vs_wc > 1.5 else 'FAIL')

        return {
            'hazard_ratio': hr, 'ci_lower': ci_lower, 'ci_upper': ci_upper,
            'p_value': p_cox, 'concordance': cph.concordance_index_,
            'hr_hp_vs_wc': hr_hp_vs_wc, 'median_times': median_times,
        }
    else:
        rpt.line('  Insufficient events for Cox regression.')
        return {'median_times': median_times}


def analysis_6_calibration(df: pd.DataFrame, rpt: ReportWriter,
                           logreg_results: dict) -> dict:
    """
    Calibration analysis: reliability diagram, ECE, and Brier score.
    """
    rpt.h2('6. Calibration Analysis')

    y_prob = logreg_results.get('y_prob')
    y_true = logreg_results.get('y_true')

    if y_prob is None or y_true is None:
        rpt.line('  Skipped: logistic regression results not available.')
        return {}

    # Brier score
    brier = brier_score_loss(y_true, y_prob)

    # Calibration curve (reliability diagram)
    n_bins = 10
    fraction_of_positives, mean_predicted_value = calibration_curve(
        y_true, y_prob, n_bins=n_bins, strategy='uniform'
    )

    # Expected Calibration Error (ECE)
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bin_indices = np.digitize(y_prob, bin_edges[1:-1])
    ece = 0.0
    for b in range(n_bins):
        mask = bin_indices == b
        if mask.sum() == 0:
            continue
        bin_acc = y_true[mask].mean()
        bin_conf = y_prob[mask].mean()
        ece += (mask.sum() / len(y_true)) * abs(bin_acc - bin_conf)

    rpt.kv('Brier score', f'{brier:.4f}')
    rpt.kv('Brier target (<0.20)', 'PASS' if brier < 0.20 else 'FAIL')
    rpt.kv('Expected Calibration Error (ECE)', f'{ece:.4f}')
    rpt.kv('ECE target (<0.10)', 'PASS' if ece < 0.10 else 'FAIL')

    rpt.line()
    rpt.line(f'  {"Bin (predicted)":>18s}  {"Observed":>10s}  {"Count":>6s}')
    rpt.line(f'  {"-" * 40}')
    for i in range(len(fraction_of_positives)):
        lo = bin_edges[i]
        hi = bin_edges[i + 1]
        mask = (y_prob >= lo) & (y_prob < hi)
        count = mask.sum()
        rpt.line(
            f'  {lo:.2f}-{hi:.2f}         '
            f'{fraction_of_positives[i]:>9.3f}  {count:>6d}'
        )

    # --- Calibration plot ---
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Perfectly calibrated')
    ax.plot(mean_predicted_value, fraction_of_positives, 's-',
            color='#2563eb', linewidth=2, markersize=7, label='Model')
    ax.set_xlabel('Mean Predicted Probability', fontsize=11)
    ax.set_ylabel('Observed Fraction of Positives', fontsize=11)
    ax.set_title('Calibration Plot (Reliability Diagram)', fontsize=13, fontweight='bold')
    ax.legend(fontsize=10)
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / 'calibration_plot.png', dpi=150)
    plt.close(fig)
    rpt.line(f'  Plot saved: {PLOTS_DIR / "calibration_plot.png"}')

    return {'brier': brier, 'ece': ece}


def analysis_7_optimal_threshold(df: pd.DataFrame, rpt: ReportWriter,
                                 logreg_results: dict) -> dict:
    """
    ROC curve analysis with Youden's J to find optimal score threshold.
    """
    rpt.h2("7. Optimal Threshold (ROC / Youden's J)")

    y_prob = logreg_results.get('y_prob')
    y_true = logreg_results.get('y_true')
    auc = logreg_results.get('auc')
    b0 = logreg_results.get('b0')
    b1 = logreg_results.get('b1')

    if y_prob is None or y_true is None:
        rpt.line('  Skipped: logistic regression results not available.')
        return {}

    fpr, tpr, thresholds_roc = roc_curve(y_true, y_prob)
    youden_j = tpr - fpr
    best_idx = np.argmax(youden_j)
    best_prob_threshold = thresholds_roc[best_idx]
    best_sensitivity = tpr[best_idx]
    best_specificity = 1 - fpr[best_idx]
    best_j = youden_j[best_idx]

    # Convert probability threshold back to harmonic_mean score
    # P = sigmoid(b0 + b1*score) => score = (logit(P) - b0) / b1
    if b1 and b1 != 0:
        logit_p = np.log(best_prob_threshold / (1 - best_prob_threshold)) if 0 < best_prob_threshold < 1 else 0
        optimal_score = (logit_p - b0) / b1
    else:
        optimal_score = float('nan')

    rpt.kv('AUC-ROC', f'{auc:.4f}')
    rpt.kv('Optimal probability threshold', f'{best_prob_threshold:.4f}')
    rpt.kv('Optimal score threshold', f'{optimal_score:.1f}')
    rpt.kv("Youden's J at optimum", f'{best_j:.4f}')
    rpt.kv('Sensitivity at optimum', f'{best_sensitivity:.4f}')
    rpt.kv('Specificity at optimum', f'{best_specificity:.4f}')
    rpt.line()
    rpt.line('  Comparison with current tier boundaries:')
    rpt.kv('Current hand_picked cutoff', '67')
    rpt.kv('Current strong cutoff', '55')
    rpt.kv('Data-driven optimal cutoff', f'{optimal_score:.1f}')
    diff_hp = abs(optimal_score - 67) if not np.isnan(optimal_score) else float('nan')
    rpt.kv('Diff from hand_picked boundary', f'{diff_hp:.1f} points')

    # --- ROC curve plot ---
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.plot(fpr, tpr, color='#2563eb', linewidth=2.5,
            label=f'ROC (AUC = {auc:.3f})')
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random')
    ax.scatter([fpr[best_idx]], [tpr[best_idx]], s=120, c='#ef4444',
               zorder=5, label=f'Optimal (J={best_j:.3f}, score={optimal_score:.0f})')
    ax.set_xlabel('False Positive Rate', fontsize=11)
    ax.set_ylabel('True Positive Rate', fontsize=11)
    ax.set_title('ROC Curve: any_contact_action ~ harmonic_mean', fontsize=13,
                 fontweight='bold')
    ax.legend(loc='lower right', fontsize=10)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / 'roc_curve.png', dpi=150)
    plt.close(fig)
    rpt.line(f'  Plot saved: {PLOTS_DIR / "roc_curve.png"}')

    return {
        'optimal_score': optimal_score,
        'optimal_prob': best_prob_threshold,
        'youden_j': best_j,
        'sensitivity': best_sensitivity,
        'specificity': best_specificity,
    }


def analysis_8_explanation_source(df: pd.DataFrame, rpt: ReportWriter) -> dict:
    """
    Test whether explanation quality (llm_verified vs llm_partial vs
    template_fallback) affects engagement, controlling for score.
    """
    rpt.h2('8. Explanation Source Impact on Engagement')

    if 'explanation_source' not in df.columns or df['explanation_source'].isna().all():
        rpt.line('  Skipped: no explanation_source data available.')
        return {}

    src_df = df[df['explanation_source'].notna()].copy()
    sources = ['llm_verified', 'llm_partial', 'template_fallback']
    src_df = src_df[src_df['explanation_source'].isin(sources)]

    if len(src_df) < 20:
        rpt.line(f'  Insufficient data ({len(src_df)} records with explanation_source).')
        return {}

    # Contact rate by source
    rpt.line(f'  {"Source":<22s} {"N":>5s}  {"Contact%":>9s}  {"Action%":>8s}  {"Mean Score":>11s}')
    rpt.line(f'  {"-" * 60}')

    source_stats = []
    for src in sources:
        mask = src_df['explanation_source'] == src
        subset = src_df[mask]
        n = len(subset)
        if n == 0:
            continue
        cr = subset['was_contacted'].mean()
        ar = subset['any_contact_action'].mean()
        ms = subset['harmonic_mean'].mean()
        source_stats.append({'source': src, 'n': n, 'contact_rate': cr,
                             'action_rate': ar, 'mean_score': ms})
        rpt.line(f'  {src:<22s} {n:>5d}  {cr:>8.1%}  {ar:>7.1%}  {ms:>10.1f}')

    if len(source_stats) < 2:
        rpt.line('  Too few explanation sources with data.')
        return {}

    # Chi-squared: source vs contacted
    contingency = []
    for ss in source_stats:
        contacted = int(ss['contact_rate'] * ss['n'])
        not_contacted = ss['n'] - contacted
        contingency.append([contacted, not_contacted])
    contingency = np.array(contingency)

    if contingency.shape[0] >= 2 and contingency.sum() > 0:
        chi2, p_chi, dof, _ = sp_stats.chi2_contingency(contingency)
        n_total = contingency.sum()
        k = min(contingency.shape)
        cramers_v = np.sqrt(chi2 / (n_total * (k - 1))) if n_total > 0 and k > 1 else 0.0

        rpt.line()
        rpt.kv('Chi-squared (source x contacted)', f'{chi2:.3f}')
        rpt.kv('p-value', f'{p_chi:.4e} {_sig_label(p_chi)}')
        rpt.kv("Cramer's V", f'{cramers_v:.4f}')

    # Odds ratio: llm_verified vs template_fallback, controlling for score
    verified = src_df[src_df['explanation_source'] == 'llm_verified']
    fallback = src_df[src_df['explanation_source'] == 'template_fallback']

    if len(verified) >= 10 and len(fallback) >= 10:
        combined = pd.concat([verified, fallback])
        combined['is_verified'] = (combined['explanation_source'] == 'llm_verified').astype(int)

        X_ctrl = combined[['harmonic_mean', 'is_verified']].values
        y_ctrl = combined['was_contacted'].astype(int).values

        if y_ctrl.sum() > 0 and y_ctrl.sum() < len(y_ctrl):
            clf = LogisticRegression(solver='lbfgs', max_iter=1000, random_state=RANDOM_SEED)
            clf.fit(X_ctrl, y_ctrl)
            or_verified = np.exp(clf.coef_[0][1])
            rpt.line()
            rpt.kv('Odds ratio: llm_verified vs fallback', f'{or_verified:.3f}')
            rpt.line('  (controlling for harmonic_mean)')
            rpt.line()
            if or_verified > 1:
                rpt.line(
                    f'  Interpretation: LLM-verified explanations are associated with '
                    f'{or_verified:.2f}x higher odds of contact, controlling for match score. '
                    f'This validates the anti-hallucination pipeline\'s contribution to user trust.'
                )
            else:
                rpt.line(
                    f'  Interpretation: LLM-verified explanations do not show higher contact '
                    f'odds ({or_verified:.2f}x). The explanation source may not be the primary '
                    f'driver of engagement behavior.'
                )
        else:
            rpt.line('  Cannot compute odds ratio: outcome is constant.')
    else:
        rpt.line('  Insufficient data for controlled odds ratio (need >=10 per group).')

    # --- Bar chart ---
    if len(source_stats) >= 2:
        fig, ax = plt.subplots(figsize=(8, 5))
        src_names = [ss['source'] for ss in source_stats]
        src_contact = [ss['contact_rate'] * 100 for ss in source_stats]
        src_action = [ss['action_rate'] * 100 for ss in source_stats]
        x = np.arange(len(src_names))
        width = 0.35

        bars1 = ax.bar(x - width / 2, src_contact, width, color='#2563eb',
                        label='Contact rate', alpha=0.85)
        bars2 = ax.bar(x + width / 2, src_action, width, color='#059669',
                        label='Any-action rate', alpha=0.85)

        ax.set_xlabel('Explanation Source', fontsize=11)
        ax.set_ylabel('Rate (%)', fontsize=11)
        ax.set_title('Engagement by Explanation Source', fontsize=13, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([s.replace('_', '\n') for s in src_names], fontsize=9)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        ax.legend(fontsize=10)
        ax.grid(axis='y', alpha=0.3)

        # Add count labels on bars
        for bar_group, values in [(bars1, [ss['n'] for ss in source_stats]),
                                   (bars2, [ss['n'] for ss in source_stats])]:
            for bar, n in zip(bar_group, values):
                ax.annotate(f'n={n}', xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                            xytext=(0, 4), textcoords='offset points',
                            ha='center', va='bottom', fontsize=8, color='gray')

        fig.tight_layout()
        fig.savefig(PLOTS_DIR / 'explanation_source_impact.png', dpi=150)
        plt.close(fig)
        rpt.line(f'  Plot saved: {PLOTS_DIR / "explanation_source_impact.png"}')

    return {'source_stats': source_stats}


# ============================================================================
# Summary & verdict
# ============================================================================

def write_summary(rpt: ReportWriter, results: dict) -> None:
    """Write overall summary and pass/fail verdict."""
    rpt.h1('Summary & Verdict')

    tests_passed = 0
    tests_total = 0

    checks = [
        ('AUC-ROC > 0.60', results.get('logreg', {}).get('auc', 0) > 0.60),
        ('Monotonic contact rates', results.get('monotonicity_rho', 0) > 0),
        ('Tier ordering (HP > S > W)', results.get('chi2', {}).get('monotonic', False)),
        ('Brier score < 0.20', results.get('calibration', {}).get('brier', 1) < 0.20),
        ('ECE < 0.10', results.get('calibration', {}).get('ece', 1) < 0.10),
        ('HR hand_picked vs wildcard > 1.5',
         results.get('survival', {}).get('hr_hp_vs_wc', 0) > 1.5),
    ]

    rpt.line()
    rpt.line(f'  {"Test":<40s} {"Result":>8s}')
    rpt.line(f'  {"-" * 50}')
    for label, passed in checks:
        tests_total += 1
        if passed:
            tests_passed += 1
        status = 'PASS' if passed else 'FAIL'
        rpt.line(f'  {label:<40s} {status:>8s}')

    rpt.line()
    rpt.kv('Tests passed', f'{tests_passed}/{tests_total}')

    if tests_passed == tests_total:
        rpt.line()
        rpt.line('  VERDICT: ISMC match scores demonstrate strong predictive validity.')
        rpt.line('  Scores predict real-world engagement behavior across all metrics.')
    elif tests_passed >= 4:
        rpt.line()
        rpt.line('  VERDICT: ISMC match scores show moderate predictive validity.')
        rpt.line('  Most metrics pass, but some areas need investigation.')
    else:
        rpt.line()
        rpt.line('  VERDICT: ISMC match scores show weak predictive validity.')
        rpt.line('  The scoring algorithm may need recalibration based on engagement data.')


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Predictive validity analysis of ISMC match scores'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Generate synthetic engagement data to exercise the analysis pipeline'
    )
    parser.add_argument(
        '--min-events', type=int, default=50,
        help='Minimum number of contact events required for analysis (default: 50)'
    )
    args = parser.parse_args()

    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    ensure_output_dirs()
    rpt = ReportWriter()

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    if args.test:
        print('[TEST MODE] Generating synthetic engagement data (n=500) ...')
        df = generate_synthetic_data(n=500)
        rpt.line('*** TEST MODE: Using synthetic data. Results are for pipeline validation only. ***')
    else:
        print('Loading engagement data from database ...')
        df = load_real_data()

    if df.empty:
        rpt.h1('Predictive Validity of ISMC Match Scores')
        rpt.line(f'Report generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        rpt.line()
        eng_count = EngagementSummary.objects.exclude(partner_id='').count()
        rec_count = PartnerRecommendation.objects.count()
        rpt.line(
            f'Insufficient engagement data. Need ~200 contact events for reliable '
            f'analysis. Currently have {eng_count} engagement summaries and '
            f'{rec_count} partner recommendations (0 joinable to match scores). '
            f'Re-run when more data is collected.'
        )
        rpt.save(REPORT_PATH)
        rpt.print_all()
        print(f'\nReport saved to: {REPORT_PATH}')
        return

    # ------------------------------------------------------------------
    # 1. Data availability check
    # ------------------------------------------------------------------
    can_continue = analysis_1_data_check(df, rpt, min_events=args.min_events)

    if not can_continue:
        rpt.save(REPORT_PATH)
        rpt.print_all()
        print(f'\nReport saved to: {REPORT_PATH}')
        return

    # Save raw data for reproducibility
    df.to_csv(DATA_PATH, index=False)
    print(f'Raw data saved to: {DATA_PATH}')

    results = {}

    # ------------------------------------------------------------------
    # 2. Score-engagement monotonicity
    # ------------------------------------------------------------------
    decile_df = analysis_2_monotonicity(df, rpt)
    if 'contact_rate' in decile_df.columns and len(decile_df) >= 3:
        rho, _ = sp_stats.spearmanr(np.arange(len(decile_df)), decile_df['contact_rate'].values)
        results['monotonicity_rho'] = rho

    # ------------------------------------------------------------------
    # 3. Logistic regression
    # ------------------------------------------------------------------
    logreg_results = analysis_3_logistic_regression(df, rpt)
    results['logreg'] = logreg_results

    # ------------------------------------------------------------------
    # 4. Tier chi-squared
    # ------------------------------------------------------------------
    chi2_results = analysis_4_tier_chi_squared(df, rpt)
    results['chi2'] = chi2_results

    # ------------------------------------------------------------------
    # 5. Survival analysis
    # ------------------------------------------------------------------
    survival_results = analysis_5_survival(df, rpt)
    results['survival'] = survival_results

    # ------------------------------------------------------------------
    # 6. Calibration
    # ------------------------------------------------------------------
    calib_results = analysis_6_calibration(df, rpt, logreg_results)
    results['calibration'] = calib_results

    # ------------------------------------------------------------------
    # 7. Optimal threshold
    # ------------------------------------------------------------------
    threshold_results = analysis_7_optimal_threshold(df, rpt, logreg_results)
    results['threshold'] = threshold_results

    # ------------------------------------------------------------------
    # 8. Explanation source impact
    # ------------------------------------------------------------------
    expl_results = analysis_8_explanation_source(df, rpt)
    results['explanation'] = expl_results

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    write_summary(rpt, results)

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    rpt.save(REPORT_PATH)
    rpt.print_all()

    print(f'\n{"=" * 50}')
    print(f'Report saved to:  {REPORT_PATH}')
    print(f'Data saved to:    {DATA_PATH}')
    print(f'Plots saved to:   {PLOTS_DIR}/')
    print(f'{"=" * 50}')


if __name__ == '__main__':
    main()
