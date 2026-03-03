#!/usr/bin/env python3
"""
10_shadow_score_comparison.py — Shadow Scoring A/B Comparison

Compares production and experimental scores recorded by ShadowScoringService
during --shadow rescore runs. Experimental scores are stored in the
match_context JSON field under 'experimental_scores'.

Analyses performed:
  1. Score divergence distribution (histogram + statistics)
  2. Spearman rank correlation (PASS/FAIL gate)
  3. Tier reclassification impact (PASS/FAIL gate)
  4. Mean score shift (PASS/FAIL gate)
  5. Outcome-based comparison (if MatchFeedback data exists)

Pass/Fail criteria:
  - Spearman rho:       PASS >= 0.85,  HARD FAIL < 0.70
  - Tier reclassification: PASS < 15%, HARD FAIL > 25%
  - Mean score shift:    PASS < 3.0,   HARD FAIL > 5.0

Usage:
  python Validation/10_shadow_score_comparison.py           # Live database
  python Validation/10_shadow_score_comparison.py --test    # Synthetic data
"""

import argparse
import json
import math
import os
import random
import statistics
import sys
from collections import Counter
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
TIER_THRESHOLDS = {
    'premier': 67,       # Curator-quality: both sides benefit strongly
    'strong': 55,        # High-confidence: clear mutual value, worth introducing
    'aligned': 0,        # Speculative: possible fit, needs manual review
}

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'validation_results')
PLOTS_DIR = os.path.join(RESULTS_DIR, 'plots')

# Pass/fail thresholds
SPEARMAN_PASS = 0.85
SPEARMAN_HARD_FAIL = 0.70
TIER_RECLASS_PASS = 0.15       # < 15% change
TIER_RECLASS_HARD_FAIL = 0.25  # > 25% change
MEAN_SHIFT_PASS = 3.0          # abs(shift) < 3.0
MEAN_SHIFT_HARD_FAIL = 5.0     # abs(shift) > 5.0


# ── Tier classification ────────────────────────────────────────────

def classify_tier(score: float) -> str:
    """Classify a harmonic_mean score into a tier."""
    if score >= TIER_THRESHOLDS['premier']:
        return 'premier'
    elif score >= TIER_THRESHOLDS['strong']:
        return 'strong'
    else:
        return 'aligned'


# ── Data loading ────────────────────────────────────────────────────

def load_shadow_data() -> List[dict]:
    """Load match pairs that have experimental_scores in their match_context."""
    matches = SupabaseMatch.objects.filter(
        match_context__isnull=False,
        harmonic_mean__isnull=False,
    ).values('id', 'profile_id', 'suggested_profile_id',
             'harmonic_mean', 'score_ab', 'score_ba', 'match_context')

    results = []
    for m in matches:
        ctx = m['match_context']
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(ctx, dict):
            continue

        exp = ctx.get('experimental_scores')
        if not exp or not isinstance(exp, dict):
            continue

        exp_hm = exp.get('harmonic_mean')
        if exp_hm is None:
            continue

        results.append({
            'id': str(m['id']),
            'profile_id': str(m['profile_id']),
            'suggested_profile_id': str(m['suggested_profile_id']),
            'prod_harmonic_mean': float(m['harmonic_mean']),
            'prod_score_ab': float(m['score_ab']) if m['score_ab'] else 0,
            'prod_score_ba': float(m['score_ba']) if m['score_ba'] else 0,
            'exp_harmonic_mean': float(exp_hm),
            'exp_score_ab': float(exp.get('score_ab', 0)),
            'exp_score_ba': float(exp.get('score_ba', 0)),
        })

    return results


def generate_synthetic_shadow_data(n: int = 500) -> List[dict]:
    """Generate synthetic shadow comparison data for --test mode."""
    random.seed(42)
    results = []
    for i in range(n):
        # Generate production scores
        prod_ab = round(random.uniform(20, 95), 2)
        prod_ba = round(random.uniform(20, 95), 2)
        prod_hm = round(2.0 / (1.0 / max(prod_ab, 0.01) + 1.0 / max(prod_ba, 0.01)), 2)

        # Experimental scores: production + small perturbation
        # Most pairs get small changes; some get larger ones
        if i < 30:
            # Large divergence pairs
            noise = random.uniform(-8, 8)
        elif i < 80:
            # Medium divergence
            noise = random.uniform(-4, 4)
        else:
            # Small divergence (majority)
            noise = random.uniform(-2, 2)

        exp_ab = round(max(0, min(100, prod_ab + noise + random.uniform(-1, 1))), 2)
        exp_ba = round(max(0, min(100, prod_ba + noise + random.uniform(-1, 1))), 2)
        exp_hm = round(2.0 / (1.0 / max(exp_ab, 0.01) + 1.0 / max(exp_ba, 0.01)), 2)

        results.append({
            'id': f'synthetic-{i:04d}',
            'profile_id': f'profile-a-{i:04d}',
            'suggested_profile_id': f'profile-b-{i:04d}',
            'prod_harmonic_mean': prod_hm,
            'prod_score_ab': prod_ab,
            'prod_score_ba': prod_ba,
            'exp_harmonic_mean': exp_hm,
            'exp_score_ab': exp_ab,
            'exp_score_ba': exp_ba,
        })

    return results


# ── Analysis engine ────────────────────────────────────────────────

class ShadowAnalysis:
    """Runs all shadow comparison analyses and collects results."""

    def __init__(self, data: List[dict], is_test: bool = False):
        self.data = data
        self.is_test = is_test
        self.report_lines: List[str] = []
        self.verdicts: List[dict] = []  # {'name': ..., 'status': PASS/WARN/FAIL, 'detail': ...}

    def run_all(self) -> None:
        """Execute all analyses in sequence."""
        self._score_divergence_distribution()
        self._spearman_rank_correlation()
        self._tier_reclassification()
        self._mean_score_shift()
        self._outcome_based_comparison()
        self._summary_table()

    # ── 1. Score Divergence Distribution ────────────────────────────

    def _score_divergence_distribution(self) -> None:
        self._section("1. SCORE DIVERGENCE DISTRIBUTION")
        self._line("  How far experimental scores deviate from production scores.")
        self._line("  Small deltas mean the new scorer behaves similarly; large deltas")
        self._line("  suggest the experimental scorer is re-evaluating match quality.")
        self._line("")

        deltas = [d['exp_harmonic_mean'] - d['prod_harmonic_mean'] for d in self.data]

        if not deltas:
            self._line("  No data available.")
            return

        mean_d = statistics.mean(deltas)
        median_d = statistics.median(deltas)
        stdev_d = statistics.stdev(deltas) if len(deltas) > 1 else 0.0

        self._line(f"  Pairs analyzed:  {len(deltas)}")
        self._line(f"  Mean delta:      {mean_d:+.4f}")
        self._line(f"  Median delta:    {median_d:+.4f}")
        self._line(f"  Stdev delta:     {stdev_d:.4f}")
        self._line(f"  Min delta:       {min(deltas):+.4f}")
        self._line(f"  Max delta:       {max(deltas):+.4f}")
        self._line("")

        # Distribution buckets
        buckets = Counter()
        for d in deltas:
            if abs(d) < 1:
                buckets['< 1 point'] += 1
            elif abs(d) < 3:
                buckets['1-3 points'] += 1
            elif abs(d) < 5:
                buckets['3-5 points'] += 1
            else:
                buckets['>= 5 points'] += 1

        self._line("  Divergence distribution:")
        bucket_descriptions = {
            '< 1 point':   'Negligible — effectively identical scoring',
            '1-3 points':  'Minor — within normal scoring noise',
            '3-5 points':  'Moderate — may shift tier boundaries',
            '>= 5 points': 'Significant — likely changes match recommendations',
        }
        for label in ['< 1 point', '1-3 points', '3-5 points', '>= 5 points']:
            count = buckets.get(label, 0)
            pct = count / len(deltas) * 100
            desc = bucket_descriptions[label]
            self._line(f"    {label:>14}: {count:>6d}  ({pct:>5.1f}%)  {desc}")
        self._line("")

        # Generate histogram plot
        self._plot_divergence_histogram(deltas)

    def _plot_divergence_histogram(self, deltas: List[float]) -> None:
        """Plot histogram of score divergences."""
        os.makedirs(PLOTS_DIR, exist_ok=True)

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Left: histogram
        ax = axes[0]
        bins = min(80, max(20, len(deltas) // 15))
        ax.hist(deltas, bins=bins, color='steelblue', edgecolor='white', alpha=0.8)
        mean_d = statistics.mean(deltas)
        ax.axvline(x=0, color='red', linestyle='--', alpha=0.5, label='Zero divergence')
        ax.axvline(x=mean_d, color='orange', linestyle='-', alpha=0.7,
                   label=f'Mean = {mean_d:+.2f}')
        ax.set_xlabel('Delta (experimental - production)', fontsize=11)
        ax.set_ylabel('Count', fontsize=11)
        ax.set_title('Shadow Score Divergence Distribution', fontsize=13)
        ax.legend()

        # Right: scatter of prod vs exp
        ax2 = axes[1]
        prod_scores = [d['prod_harmonic_mean'] for d in self.data]
        exp_scores = [d['exp_harmonic_mean'] for d in self.data]
        ax2.scatter(prod_scores, exp_scores, alpha=0.3, s=10, color='steelblue')
        lims = [0, max(max(prod_scores), max(exp_scores)) * 1.05]
        ax2.plot(lims, lims, 'k--', alpha=0.3, linewidth=1, label='y = x')
        ax2.set_xlabel('Production harmonic_mean', fontsize=11)
        ax2.set_ylabel('Experimental harmonic_mean', fontsize=11)
        ax2.set_title('Production vs Experimental Scores', fontsize=13)
        ax2.legend(loc='upper left')
        ax2.set_xlim(lims)
        ax2.set_ylim(lims)

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'shadow_divergence_distribution.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    # ── 2. Spearman Rank Correlation ────────────────────────────────

    def _spearman_rank_correlation(self) -> None:
        self._section("2. SPEARMAN RANK CORRELATION")
        self._line("  Does the experimental scorer preserve the RANKING of matches?")
        self._line("  Spearman rho measures rank-order agreement (1.0 = identical ordering).")
        self._line("  This is the most critical test — even if absolute scores change,")
        self._line("  the 'best' matches should still rank highest.")
        self._line("")

        if len(self.data) < 3:
            self._line("  Insufficient data (need >= 3 pairs).")
            self.verdicts.append({
                'name': 'Spearman Rank Correlation',
                'status': 'SKIP',
                'detail': 'Insufficient data',
            })
            return

        prod_scores = [d['prod_harmonic_mean'] for d in self.data]
        exp_scores = [d['exp_harmonic_mean'] for d in self.data]

        rho, p_value = scipy_stats.spearmanr(prod_scores, exp_scores)

        self._line(f"  Pairs compared:  {len(self.data)}")
        self._line(f"  Spearman rho:    {rho:.6f}")
        self._line(f"  P-value:         {p_value:.2e}")
        self._line("")

        if rho >= SPEARMAN_PASS:
            status = 'PASS'
            interpretation = (f"rho >= {SPEARMAN_PASS} — rankings well-preserved. "
                            f"Users will see the same top matches in roughly the same order.")
        elif rho >= SPEARMAN_HARD_FAIL:
            status = 'WARN'
            interpretation = (f"rho between {SPEARMAN_HARD_FAIL} and {SPEARMAN_PASS} "
                            f"— moderate ranking shifts. Some matches will swap positions, "
                            f"but the general ordering is intact.")
        else:
            status = 'HARD FAIL'
            interpretation = (f"rho < {SPEARMAN_HARD_FAIL} — experimental scorer significantly "
                            f"reorders pairs. Users would see substantially different top matches. "
                            f"Do NOT promote without understanding why rankings diverged.")

        self._line(f"  Verdict: {status}")
        self._line(f"  Interpretation: {interpretation}")
        self._line("")

        self.verdicts.append({
            'name': 'Spearman Rank Correlation',
            'status': status,
            'detail': f'rho={rho:.4f}, p={p_value:.2e}',
        })

        # Also compute Pearson for reference
        pearson_r, pearson_p = scipy_stats.pearsonr(prod_scores, exp_scores)
        self._line(f"  Pearson r (linear): {pearson_r:.6f}  (p={pearson_p:.2e})")
        self._line("")

    # ── 3. Tier Reclassification ────────────────────────────────────

    def _tier_reclassification(self) -> None:
        self._section("3. TIER RECLASSIFICATION IMPACT")
        self._line("  How many matches would jump between tiers under the new scorer?")
        self._line("  Tier changes are user-visible — a match moving from 'Strong' to")
        self._line("  'Aligned' changes how it's presented and prioritized in the UI.")
        self._line("")
        self._line("  Tiers:")
        self._line("    Premier (67+)      — Curator-quality, both sides benefit strongly")
        self._line("    Strong (55-66)     — High-confidence, clear mutual value")
        self._line("    Aligned (<55)      — Speculative, possible fit, needs review")
        self._line("")

        tier_order = {'aligned': 0, 'strong': 1, 'premier': 2}
        transitions = Counter()
        changed = 0
        total = 0

        for d in self.data:
            prod_tier = classify_tier(d['prod_harmonic_mean'])
            exp_tier = classify_tier(d['exp_harmonic_mean'])
            transitions[(prod_tier, exp_tier)] += 1
            total += 1
            if prod_tier != exp_tier:
                changed += 1

        if total == 0:
            self._line("  No data available.")
            self.verdicts.append({
                'name': 'Tier Reclassification',
                'status': 'SKIP',
                'detail': 'No data',
            })
            return

        change_rate = changed / total
        self._line(f"  Total pairs:     {total}")
        self._line(f"  Changed tier:    {changed}  ({change_rate * 100:.1f}%)")
        self._line(f"  Unchanged tier:  {total - changed}  ({(1 - change_rate) * 100:.1f}%)")
        self._line("")

        # Transition matrix
        tiers_list = ['aligned', 'strong', 'premier']
        tier_labels = {'aligned': 'Aligned (<55)', 'strong': 'Strong (55-66)',
                       'premier': 'Premier (67+)'}

        self._line("  Transition matrix (rows=production tier, cols=experimental tier):")
        header = f"    {'':>15}" + "".join(f"  {t:>12}" for t in tiers_list)
        self._line(header)
        self._line("    " + "-" * (15 + 14 * len(tiers_list)))
        for from_t in tiers_list:
            row_vals = "".join(
                f"  {transitions.get((from_t, to_t), 0):>12d}" for to_t in tiers_list
            )
            self._line(f"    {from_t:>15}{row_vals}")
        self._line("")

        # Upgraded vs downgraded
        upgraded = sum(
            count for (from_t, to_t), count in transitions.items()
            if tier_order.get(to_t, 0) > tier_order.get(from_t, 0)
        )
        downgraded = sum(
            count for (from_t, to_t), count in transitions.items()
            if tier_order.get(to_t, 0) < tier_order.get(from_t, 0)
        )
        self._line(f"  Upgraded (experimental scores HIGHER tier):    {upgraded:>6d}  ({upgraded / total * 100:.1f}%)")
        self._line(f"  Downgraded (experimental scores LOWER tier):  {downgraded:>6d}  ({downgraded / total * 100:.1f}%)")
        self._line("")

        if change_rate < TIER_RECLASS_PASS:
            status = 'PASS'
            detail = f'{change_rate * 100:.1f}% changed, threshold < {TIER_RECLASS_PASS * 100:.0f}%'
        elif change_rate <= TIER_RECLASS_HARD_FAIL:
            status = 'WARN'
            detail = (f'{change_rate * 100:.1f}% changed, between '
                     f'{TIER_RECLASS_PASS * 100:.0f}% and {TIER_RECLASS_HARD_FAIL * 100:.0f}%')
        else:
            status = 'HARD FAIL'
            detail = f'{change_rate * 100:.1f}% changed, threshold > {TIER_RECLASS_HARD_FAIL * 100:.0f}%'

        self._line(f"  Verdict: {status}")
        self._line(f"  Detail:  {detail}")
        self._line("")

        self.verdicts.append({
            'name': 'Tier Reclassification',
            'status': status,
            'detail': detail,
        })

        # Generate tier reclassification plot
        self._plot_tier_reclassification(transitions)

    def _plot_tier_reclassification(self, transitions: Counter) -> None:
        """Bar chart of tier transitions."""
        os.makedirs(PLOTS_DIR, exist_ok=True)

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

        ax.set_xlabel('Production Tier', fontsize=12)
        ax.set_ylabel('Number of Matches', fontsize=12)
        ax.set_title('Shadow Scoring: Tier Reclassification Impact', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(tier_labels)
        ax.legend(title='Experimental Tier')

        fig.tight_layout()
        path = os.path.join(PLOTS_DIR, 'shadow_tier_reclassification.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {path}")

    # ── 4. Mean Score Shift ─────────────────────────────────────────

    def _mean_score_shift(self) -> None:
        self._section("4. MEAN SCORE SHIFT")
        self._line("  Is the experimental scorer systematically inflating or deflating scores?")
        self._line("  A positive shift means experimental scores are higher on average")
        self._line("  (more generous); negative means stricter. Large shifts indicate")
        self._line("  the scorer is recalibrated, not just reordering.")
        self._line("")

        if not self.data:
            self._line("  No data available.")
            self.verdicts.append({
                'name': 'Mean Score Shift',
                'status': 'SKIP',
                'detail': 'No data',
            })
            return

        prod_mean = statistics.mean(d['prod_harmonic_mean'] for d in self.data)
        exp_mean = statistics.mean(d['exp_harmonic_mean'] for d in self.data)
        shift = exp_mean - prod_mean
        abs_shift = abs(shift)

        self._line(f"  Production mean:    {prod_mean:.4f}")
        self._line(f"  Experimental mean:  {exp_mean:.4f}")
        self._line(f"  Shift:              {shift:+.4f}")
        self._line(f"  Absolute shift:     {abs_shift:.4f}")
        self._line("")

        if abs_shift < MEAN_SHIFT_PASS:
            status = 'PASS'
            detail = f'abs(shift)={abs_shift:.2f}, threshold < {MEAN_SHIFT_PASS}'
        elif abs_shift <= MEAN_SHIFT_HARD_FAIL:
            status = 'WARN'
            detail = f'abs(shift)={abs_shift:.2f}, between {MEAN_SHIFT_PASS} and {MEAN_SHIFT_HARD_FAIL}'
        else:
            status = 'HARD FAIL'
            detail = f'abs(shift)={abs_shift:.2f}, threshold > {MEAN_SHIFT_HARD_FAIL}'

        self._line(f"  Verdict: {status}")
        self._line(f"  Detail:  {detail}")
        self._line("")

        self.verdicts.append({
            'name': 'Mean Score Shift',
            'status': status,
            'detail': detail,
        })

    # ── 5. Outcome-Based Comparison ─────────────────────────────────

    def _outcome_based_comparison(self) -> None:
        self._section("5. OUTCOME-BASED COMPARISON (MatchFeedback)")
        self._line("  The ground-truth test: does the experimental scorer better predict")
        self._line("  which matches users actually accepted vs rejected? Requires real")
        self._line("  MatchFeedback data from users who acted on match suggestions.")
        self._line("  AUC > 0.5 means the scorer is better than random; higher is better.")
        self._line("")

        try:
            from matching.models import MatchFeedback
        except ImportError:
            self._line("  MatchFeedback model not available. Skipping.")
            self.verdicts.append({
                'name': 'Outcome-Based (Feedback)',
                'status': 'SKIP',
                'detail': 'MatchFeedback model not available',
            })
            return

        # Build lookup of match IDs in our shadow data
        shadow_ids = {d['id'] for d in self.data}
        shadow_lookup = {d['id']: d for d in self.data}

        # Load feedback records
        try:
            feedbacks = list(MatchFeedback.objects.filter(
                match_id__in=shadow_ids
            ).values('match_id', 'outcome', 'rating'))
        except Exception as e:
            self._line(f"  Could not load MatchFeedback: {e}")
            self.verdicts.append({
                'name': 'Outcome-Based (Feedback)',
                'status': 'SKIP',
                'detail': str(e),
            })
            return

        if not feedbacks:
            self._line("  No MatchFeedback records found for shadow-scored pairs.")
            self._line("  This analysis requires user feedback to evaluate which scorer")
            self._line("  better predicts positive outcomes.")
            self.verdicts.append({
                'name': 'Outcome-Based (Feedback)',
                'status': 'SKIP',
                'detail': 'No feedback data',
            })
            return

        # Classify feedback as positive/negative
        positive_outcomes = {'accepted', 'successful', 'positive', 'interested'}
        positive_matches = []
        negative_matches = []

        for fb in feedbacks:
            mid = str(fb['match_id'])
            shadow_row = shadow_lookup.get(mid)
            if not shadow_row:
                continue

            outcome = (fb.get('outcome') or '').lower().strip()
            rating = fb.get('rating')

            is_positive = outcome in positive_outcomes or (rating is not None and rating >= 4)

            if is_positive:
                positive_matches.append(shadow_row)
            else:
                negative_matches.append(shadow_row)

        total_feedback = len(positive_matches) + len(negative_matches)
        self._line(f"  Feedback records matched: {total_feedback}")
        self._line(f"  Positive outcomes:        {len(positive_matches)}")
        self._line(f"  Negative outcomes:        {len(negative_matches)}")
        self._line("")

        if positive_matches:
            # For positive matches: does experimental score better?
            prod_mean_pos = statistics.mean(d['prod_harmonic_mean'] for d in positive_matches)
            exp_mean_pos = statistics.mean(d['exp_harmonic_mean'] for d in positive_matches)
            exp_higher_count = sum(
                1 for d in positive_matches
                if d['exp_harmonic_mean'] > d['prod_harmonic_mean']
            )

            self._line(f"  Positive outcomes — mean production score:    {prod_mean_pos:.2f}")
            self._line(f"  Positive outcomes — mean experimental score:  {exp_mean_pos:.2f}")
            self._line(f"  Experimental higher for positive outcomes:    "
                       f"{exp_higher_count}/{len(positive_matches)} "
                       f"({exp_higher_count / len(positive_matches) * 100:.1f}%)")
            self._line("")

        # AUC comparison (need 50+ signals)
        if total_feedback >= 50 and positive_matches and negative_matches:
            self._line("  AUC comparison (production vs experimental):")
            try:
                from sklearn.metrics import roc_auc_score
                labels = [1] * len(positive_matches) + [0] * len(negative_matches)
                all_matches = positive_matches + negative_matches
                prod_preds = [d['prod_harmonic_mean'] for d in all_matches]
                exp_preds = [d['exp_harmonic_mean'] for d in all_matches]

                auc_prod = roc_auc_score(labels, prod_preds)
                auc_exp = roc_auc_score(labels, exp_preds)
                auc_diff = auc_exp - auc_prod

                self._line(f"    Production AUC:    {auc_prod:.4f}")
                self._line(f"    Experimental AUC:  {auc_exp:.4f}")
                self._line(f"    AUC improvement:   {auc_diff:+.4f}")
                self._line("")

                if auc_diff > 0.01:
                    detail = f'AUC improved by {auc_diff:+.4f} (prod={auc_prod:.4f}, exp={auc_exp:.4f})'
                    status = 'PASS'
                elif auc_diff > -0.01:
                    detail = f'AUC neutral ({auc_diff:+.4f})'
                    status = 'PASS'
                else:
                    detail = f'AUC decreased by {auc_diff:+.4f}'
                    status = 'WARN'

                self.verdicts.append({
                    'name': 'Outcome-Based (Feedback)',
                    'status': status,
                    'detail': detail,
                })
            except ImportError:
                self._line("    sklearn not available — skipping AUC computation.")
                self._line("    Install: pip install scikit-learn")
                self.verdicts.append({
                    'name': 'Outcome-Based (Feedback)',
                    'status': 'SKIP',
                    'detail': 'sklearn not installed',
                })
            except Exception as e:
                self._line(f"    AUC computation failed: {e}")
                self.verdicts.append({
                    'name': 'Outcome-Based (Feedback)',
                    'status': 'SKIP',
                    'detail': str(e),
                })
        else:
            needed = max(0, 50 - total_feedback)
            self._line(f"  AUC comparison requires 50+ feedback signals "
                       f"(have {total_feedback}, need {needed} more).")
            self.verdicts.append({
                'name': 'Outcome-Based (Feedback)',
                'status': 'SKIP',
                'detail': f'Need {needed} more feedback signals for AUC',
            })
        self._line("")

    # ── Summary table ───────────────────────────────────────────────

    def _summary_table(self) -> None:
        self._section("SUMMARY: PASS/FAIL CRITERIA")

        self._line(f"  {'#':>3}  {'Analysis':<30}  {'Status':<10}  {'Detail'}")
        self._line(f"  {'─' * 3}  {'─' * 30}  {'─' * 10}  {'─' * 50}")

        all_pass = True
        any_hard_fail = False

        for i, v in enumerate(self.verdicts, 1):
            status_display = v['status']
            if v['status'] == 'HARD FAIL':
                any_hard_fail = True
                all_pass = False
            elif v['status'] == 'WARN':
                all_pass = False

            self._line(f"  {i:3d}  {v['name']:<30}  {status_display:<10}  {v['detail']}")

        self._line("")
        if any_hard_fail:
            self._line("  OVERALL: HARD FAIL — Experimental scorer diverges dangerously from production.")
            self._line("")
            self._line("  What this means: Promoting this scorer would visibly change match rankings")
            self._line("  and tier assignments for users. Matches they previously saw as 'Premier'")
            self._line("  could drop to 'Aligned' or vice versa.")
            self._line("")
            self._line("  Next steps:")
            self._line("    1. Identify which scoring component(s) changed most (check score_ab vs score_ba)")
            self._line("    2. Inspect the highest-divergence pairs to understand what shifted")
            self._line("    3. Consider a more conservative weight adjustment and re-run --shadow")
        elif all_pass:
            self._line("  OVERALL: PASS — Experimental scorer is safe to promote to production.")
            self._line("")
            self._line("  What this means: Rankings are preserved, tier assignments are stable,")
            self._line("  and score magnitudes haven't shifted meaningfully. Users will not notice")
            self._line("  a difference in their match recommendations.")
            self._line("")
            self._line("  Next step: python manage.py rescore_matches --use-experimental")
        else:
            self._line("  OVERALL: WARN — Some criteria outside ideal range but no hard failures.")
            self._line("")
            self._line("  What this means: The experimental scorer is directionally safe but has")
            self._line("  measurable differences from production. Some users may notice changes")
            self._line("  in their match ordering or tier assignments.")
            self._line("")
            self._line("  Next steps:")
            self._line("    1. Review the WARN verdicts above for specific concerns")
            self._line("    2. If changes are intentional improvements, proceed with promotion")
            self._line("    3. If changes are unexpected, investigate before promoting")
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
            "  SHADOW SCORING A/B COMPARISON",
            "  Production vs Experimental Scorer",
            "=" * 80,
            "",
            "  Purpose: Validate that a new scoring formula produces safe, predictable",
            "  changes before promoting it to production. Shadow scoring runs BOTH scorers",
            "  on the same match pairs and compares outputs without affecting users.",
            "",
            f"  Generated:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Data source:     {'SYNTHETIC (--test mode)' if self.is_test else 'Production database (match_suggestions table)'}",
            f"  Pairs analyzed:  {len(self.data)}",
            "",
            f"  Tier thresholds:",
            f"    Premier:       >= {TIER_THRESHOLDS['premier']}  (curator-quality, both sides benefit strongly)",
            f"    Strong:        >= {TIER_THRESHOLDS['strong']}  (high-confidence, clear mutual value)",
            f"    Aligned:        < {TIER_THRESHOLDS['strong']}  (speculative, possible fit, needs review)",
            "",
            f"  Pass/fail gates:",
            f"    Spearman rho:           PASS >= {SPEARMAN_PASS}, HARD FAIL < {SPEARMAN_HARD_FAIL}",
            f"    Tier reclassification:  PASS < {TIER_RECLASS_PASS*100:.0f}%, HARD FAIL > {TIER_RECLASS_HARD_FAIL*100:.0f}%",
            f"    Mean score shift:       PASS < {MEAN_SHIFT_PASS}, HARD FAIL > {MEAN_SHIFT_HARD_FAIL}",
            "",
        ]
        return "\n".join(header + self.report_lines)


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Shadow scoring A/B comparison: production vs experimental'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Use synthetic data instead of database'
    )
    args = parser.parse_args()

    random.seed(42)

    print("=" * 60)
    print("  Shadow Scoring A/B Comparison")
    print("  Production vs Experimental Scorer")
    print("=" * 60)
    print()

    # Load data
    if args.test:
        print("Using SYNTHETIC data (--test mode)")
        data = generate_synthetic_shadow_data(n=500)
    else:
        print("Loading shadow-scored match data from database...")
        data = load_shadow_data()

    print(f"Loaded {len(data)} match pairs with experimental_scores.")
    print()

    if not data:
        print("ERROR: No matches found with experimental_scores in match_context.")
        print("  First run: python manage.py rescore_matches --shadow")
        print("  Then re-run this script.")
        print("  Or use --test for synthetic data.")
        sys.exit(1)

    # Run analyses
    analysis = ShadowAnalysis(data, is_test=args.test)
    analysis.run_all()

    # Create output directories
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)

    # Write report
    report_path = os.path.join(RESULTS_DIR, 'shadow_score_comparison_report.txt')
    report_text = analysis.get_report()
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"\nReport saved: {report_path}")

    # Print summary to stdout
    print("\n" + report_text)
    print("\nDone.")


if __name__ == '__main__':
    main()
