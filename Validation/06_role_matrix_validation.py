#!/usr/bin/env python3
"""
06_role_matrix_validation.py
============================
Statistical validation of the hand-curated role compatibility matrix
used in the ISMC Synergy dimension.

Analyses performed:
  1. Role distribution across profiles
  2. Role-pair frequency in scored matches
  3. Mean ISMC score by role pair
  4. Curated vs. empirical correlation (Pearson r, Spearman rho)
  5. Unlisted role-pair analysis (default-5.0 pairs)
  6. Top / bottom role pairs by empirical harmonic_mean

Outputs:
  - validation_results/role_matrix_report.txt
  - validation_results/role_matrix_data.csv
  - validation_results/plots/role_distribution.png
  - validation_results/plots/role_compatibility_heatmap.png
  - validation_results/plots/curated_vs_empirical_scatter.png

Usage:
  python scripts/validation/06_role_matrix_validation.py          # live DB
  python scripts/validation/06_role_matrix_validation.py --test    # synthetic data
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from io import StringIO
from typing import Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django  # noqa: E402
django.setup()

from matching.models import SupabaseMatch, SupabaseProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Matplotlib / Seaborn (imported after Django so settings don't conflict)
# ---------------------------------------------------------------------------
import matplotlib  # noqa: E402
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.colors as mcolors  # noqa: E402

try:
    import seaborn as sns  # noqa: E402
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

plt.style.use('seaborn-v0_8-whitegrid')

# ---------------------------------------------------------------------------
# Constants -- role normalisation & compatibility matrix
# Copied from matching/services.py (SupabaseMatchScoringService) so that
# the validation script stays self-contained and reproducible.
# ---------------------------------------------------------------------------

ROLE_NORMALIZE: Dict[str, str] = {
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

# Canonical roles in display order
CANONICAL_ROLES: List[str] = [
    'Service Provider',
    'Thought Leader',
    'Connector',
    'Educator',
    'Coach',
    'Expert/Advisor',
    'Media/Publisher',
    'Community Builder',
    'Affiliate/Promoter',
    'Product Creator',
    'Newcomer',
]

# Symmetric role-pair compatibility scores (0-10).
# Keyed by frozenset so (A,B) == (B,A).
ROLE_COMPAT: Dict[frozenset, float] = {
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

DEFAULT_COMPAT_SCORE = 5.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_role(raw: Optional[str]) -> Optional[str]:
    """Normalize a raw network_role string to its canonical form."""
    if not raw:
        return None
    return ROLE_NORMALIZE.get(raw.lower().strip())


def role_pair_key(role_a: str, role_b: str) -> Tuple[str, str]:
    """Return a deterministic (alphabetically sorted) tuple for a role pair."""
    return tuple(sorted([role_a, role_b]))


def curated_score(role_a: str, role_b: str) -> float:
    """Look up the curated compatibility score for two canonical roles."""
    key = frozenset([role_a, role_b])
    return ROLE_COMPAT.get(key, DEFAULT_COMPAT_SCORE)


def pearson_r(x: List[float], y: List[float]) -> float:
    """Compute Pearson correlation coefficient."""
    n = len(x)
    if n < 3:
        return float('nan')
    x_arr = np.array(x)
    y_arr = np.array(y)
    mx, my = x_arr.mean(), y_arr.mean()
    dx, dy = x_arr - mx, y_arr - my
    denom = math.sqrt(float(np.sum(dx ** 2) * np.sum(dy ** 2)))
    if denom == 0:
        return float('nan')
    return float(np.sum(dx * dy) / denom)


def spearman_rho(x: List[float], y: List[float]) -> float:
    """Compute Spearman rank correlation coefficient."""
    n = len(x)
    if n < 3:
        return float('nan')

    def _rank(vals: List[float]) -> List[float]:
        indexed = sorted(enumerate(vals), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = avg_rank
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)
    return pearson_r(rx, ry)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RolePairStats:
    """Accumulated statistics for a single role pair."""
    role_a: str
    role_b: str
    curated: float
    scores: List[float] = field(default_factory=list)

    @property
    def n(self) -> int:
        return len(self.scores)

    @property
    def mean(self) -> float:
        return float(np.mean(self.scores)) if self.scores else 0.0

    @property
    def stdev(self) -> float:
        return float(np.std(self.scores, ddof=1)) if len(self.scores) > 1 else 0.0

    @property
    def pair_label(self) -> str:
        return f"{self.role_a} + {self.role_b}"

    @property
    def is_in_matrix(self) -> bool:
        key = frozenset([self.role_a, self.role_b])
        return key in ROLE_COMPAT


# ---------------------------------------------------------------------------
# Synthetic test data generator
# ---------------------------------------------------------------------------

def generate_test_data() -> Tuple[Dict[str, str], List[Tuple[str, str, float]]]:
    """
    Generate synthetic profile roles and match records for testing.

    Returns:
        role_map: dict mapping fake UUID -> canonical role
        matches:  list of (profile_id, suggested_profile_id, harmonic_mean)
    """
    random.seed(42)
    np.random.seed(42)

    # Role distribution (roughly realistic)
    role_weights = {
        'Service Provider': 400,
        'Thought Leader': 150,
        'Connector': 200,
        'Educator': 180,
        'Coach': 250,
        'Expert/Advisor': 160,
        'Media/Publisher': 100,
        'Community Builder': 80,
        'Affiliate/Promoter': 60,
        'Product Creator': 70,
        'Newcomer': 40,
    }

    # Also include some unmapped raw values
    unmapped_raw = ['strategist', 'visionary', 'facilitator']

    role_map: Dict[str, str] = {}
    profile_ids: List[str] = []

    idx = 0
    for role, count in role_weights.items():
        for _ in range(count):
            pid = f"test-{idx:05d}"
            role_map[pid] = role
            profile_ids.append(pid)
            idx += 1

    # Add some profiles with unmapped roles (stored as None in canonical)
    for raw in unmapped_raw:
        for _ in range(15):
            pid = f"test-{idx:05d}"
            role_map[pid] = None  # unmapped
            profile_ids.append(pid)
            idx += 1

    # Generate matches
    matches: List[Tuple[str, str, float]] = []
    n_matches = 8000
    for _ in range(n_matches):
        p1 = random.choice(profile_ids)
        p2 = random.choice(profile_ids)
        while p2 == p1:
            p2 = random.choice(profile_ids)

        r1 = role_map[p1]
        r2 = role_map[p2]

        # Generate harmonic_mean influenced by curated score
        if r1 and r2:
            base = curated_score(r1, r2)
        else:
            base = 5.0
        # Scale to ~40-90 range with noise
        hm = base * 8.0 + random.gauss(0, 8)
        hm = max(20.0, min(99.0, hm))
        matches.append((p1, p2, round(hm, 2)))

    return role_map, matches


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_live_data() -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]], List[Tuple[str, str, float]]]:
    """
    Load data from the live Django database.

    Returns:
        canonical_role_map: profile_id (str) -> canonical role or None
        raw_role_map:       profile_id (str) -> raw network_role or None
        matches:            list of (profile_id, suggested_profile_id, harmonic_mean)
    """
    print("Loading profiles from database...")
    profiles = SupabaseProfile.objects.all().values_list('id', 'network_role')

    canonical_role_map: Dict[str, Optional[str]] = {}
    raw_role_map: Dict[str, Optional[str]] = {}
    for pid, raw_role in profiles:
        pid_str = str(pid)
        raw_role_map[pid_str] = raw_role
        canonical_role_map[pid_str] = normalize_role(raw_role)

    print(f"  Loaded {len(canonical_role_map)} profiles")

    print("Loading matches from database...")
    match_qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False,
    ).values_list('profile_id', 'suggested_profile_id', 'harmonic_mean')

    matches: List[Tuple[str, str, float]] = []
    for pid, sid, hm in match_qs:
        matches.append((str(pid), str(sid), float(hm)))

    print(f"  Loaded {len(matches)} scored matches")

    return canonical_role_map, raw_role_map, matches


def load_test_data() -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]], List[Tuple[str, str, float]]]:
    """
    Load synthetic test data.

    Returns same structure as load_live_data.
    """
    print("Generating synthetic test data (seed=42)...")
    role_map, matches = generate_test_data()

    # For test mode, raw == canonical (no unmapping needed)
    raw_role_map: Dict[str, Optional[str]] = {}
    canonical_role_map: Dict[str, Optional[str]] = {}
    for pid, canon in role_map.items():
        canonical_role_map[pid] = canon
        raw_role_map[pid] = canon  # in test mode, raw = canonical

    print(f"  Generated {len(canonical_role_map)} profiles, {len(matches)} matches")
    return canonical_role_map, raw_role_map, matches


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------

def analyze_role_distribution(
    canonical_role_map: Dict[str, Optional[str]],
    raw_role_map: Dict[str, Optional[str]],
) -> Tuple[Counter, List[str], Counter]:
    """
    Analyse the distribution of canonical roles across profiles.

    Returns:
        role_counts:    Counter of canonical role -> count
        unmapped_raws:  list of raw values that did not normalise
        raw_unmapped_counts: Counter of unmapped raw value -> count
    """
    role_counts: Counter = Counter()
    unmapped_raws: List[str] = []
    raw_unmapped_counts: Counter = Counter()

    for pid, canon in canonical_role_map.items():
        if canon:
            role_counts[canon] += 1
        else:
            raw = raw_role_map.get(pid)
            if raw:
                unmapped_raws.append(raw)
                raw_unmapped_counts[raw.lower().strip()] += 1

    return role_counts, unmapped_raws, raw_unmapped_counts


def analyze_role_pairs(
    canonical_role_map: Dict[str, Optional[str]],
    matches: List[Tuple[str, str, float]],
) -> Dict[Tuple[str, str], RolePairStats]:
    """
    For every scored match, look up both profiles' canonical roles and
    accumulate harmonic_mean statistics per role pair.

    Returns a dict keyed by (sorted) role pair tuple.
    """
    pair_stats: Dict[Tuple[str, str], RolePairStats] = {}

    skipped_missing_role = 0
    skipped_missing_profile = 0

    for profile_id, suggested_id, hm in matches:
        role_a = canonical_role_map.get(profile_id)
        role_b = canonical_role_map.get(suggested_id)

        if profile_id not in canonical_role_map or suggested_id not in canonical_role_map:
            skipped_missing_profile += 1
            continue

        if not role_a or not role_b:
            skipped_missing_role += 1
            continue

        key = role_pair_key(role_a, role_b)
        if key not in pair_stats:
            pair_stats[key] = RolePairStats(
                role_a=key[0],
                role_b=key[1],
                curated=curated_score(key[0], key[1]),
            )
        pair_stats[key].scores.append(hm)

    print(f"  Skipped {skipped_missing_profile} matches (profile not found)")
    print(f"  Skipped {skipped_missing_role} matches (role is None/unmapped)")
    print(f"  Accumulated stats for {len(pair_stats)} distinct role pairs")

    return pair_stats


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(
    role_counts: Counter,
    unmapped_raws: List[str],
    raw_unmapped_counts: Counter,
    pair_stats: Dict[Tuple[str, str], RolePairStats],
    total_profiles: int,
    total_matches: int,
) -> str:
    """Build the full text report."""
    buf = StringIO()
    w = buf.write

    w("=" * 72 + "\n")
    w("  ROLE COMPATIBILITY MATRIX VALIDATION REPORT\n")
    w("=" * 72 + "\n\n")

    # ------------------------------------------------------------------
    # 1. Role Distribution
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("1. ROLE DISTRIBUTION\n")
    w("-" * 72 + "\n\n")
    w(f"Total profiles: {total_profiles}\n")
    w(f"Profiles with a canonical role: {sum(role_counts.values())}\n")
    w(f"Profiles with unmapped/missing role: {total_profiles - sum(role_counts.values())}\n\n")

    w(f"{'Canonical Role':<25} {'Count':>8} {'Pct':>8}  Notes\n")
    w(f"{'-'*25} {'-'*8} {'-'*8}  {'-'*20}\n")
    for role in CANONICAL_ROLES:
        count = role_counts.get(role, 0)
        pct = 100.0 * count / total_profiles if total_profiles > 0 else 0
        note = "** LOW (< 50)" if count < 50 else ""
        w(f"{role:<25} {count:>8} {pct:>7.1f}%  {note}\n")

    # Any canonical roles not in our list?
    extra_roles = set(role_counts.keys()) - set(CANONICAL_ROLES)
    for role in sorted(extra_roles):
        count = role_counts[role]
        pct = 100.0 * count / total_profiles if total_profiles > 0 else 0
        w(f"{role:<25} {count:>8} {pct:>7.1f}%  (not in CANONICAL_ROLES list)\n")

    w("\n")
    if raw_unmapped_counts:
        w("Unmapped raw role values:\n")
        for raw_val, cnt in raw_unmapped_counts.most_common(30):
            w(f"  '{raw_val}' x {cnt}\n")
    else:
        w("No unmapped raw role values found.\n")
    w("\n")

    # ------------------------------------------------------------------
    # 2. Role-Pair Frequency
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("2. ROLE-PAIR FREQUENCY\n")
    w("-" * 72 + "\n\n")
    w(f"Distinct role pairs with at least 1 match: {len(pair_stats)}\n")

    sufficient = {k: v for k, v in pair_stats.items() if v.n >= 20}
    insufficient = {k: v for k, v in pair_stats.items() if v.n < 20}

    w(f"Pairs with n >= 20 (sufficient for analysis): {len(sufficient)}\n")
    w(f"Pairs with n < 20 (insufficient): {len(insufficient)}\n\n")

    w(f"{'Role Pair':<50} {'n':>6}  {'In Matrix':>10}\n")
    w(f"{'-'*50} {'-'*6}  {'-'*10}\n")
    for key in sorted(pair_stats.keys(), key=lambda k: pair_stats[k].n, reverse=True):
        ps = pair_stats[key]
        in_mat = "Yes" if ps.is_in_matrix else "No (5.0)"
        w(f"{ps.pair_label:<50} {ps.n:>6}  {in_mat:>10}\n")
    w("\n")

    # ------------------------------------------------------------------
    # 3. Mean ISMC Score by Role Pair (n >= 20)
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("3. MEAN HARMONIC_MEAN BY ROLE PAIR (n >= 20)\n")
    w("-" * 72 + "\n\n")

    w(f"{'Role Pair':<50} {'n':>5} {'Mean':>7} {'StDev':>7} {'Curated':>8} {'Delta':>7}\n")
    w(f"{'-'*50} {'-'*5} {'-'*7} {'-'*7} {'-'*8} {'-'*7}\n")
    for key in sorted(sufficient.keys(), key=lambda k: sufficient[k].mean, reverse=True):
        ps = sufficient[key]
        delta = ps.mean - ps.curated * 8.0  # rough scale comparison
        w(f"{ps.pair_label:<50} {ps.n:>5} {ps.mean:>7.2f} {ps.stdev:>7.2f} {ps.curated:>8.1f} {delta:>+7.2f}\n")
    w("\n")

    # ------------------------------------------------------------------
    # 4. Curated vs Empirical Correlation
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("4. CURATED vs. EMPIRICAL CORRELATION\n")
    w("-" * 72 + "\n\n")

    if len(sufficient) >= 3:
        curated_vals = [sufficient[k].curated for k in sufficient]
        empirical_vals = [sufficient[k].mean for k in sufficient]
        r_val = pearson_r(curated_vals, empirical_vals)
        rho_val = spearman_rho(curated_vals, empirical_vals)
        w(f"  Pairs used: {len(sufficient)} (all with n >= 20)\n")
        w(f"  Pearson  r  = {r_val:+.4f}\n")
        w(f"  Spearman rho = {rho_val:+.4f}\n\n")
        if not math.isnan(r_val):
            if abs(r_val) >= 0.7:
                w("  Interpretation: Strong correlation -- curated matrix aligns well\n")
                w("  with empirical ISMC scores.\n")
            elif abs(r_val) >= 0.4:
                w("  Interpretation: Moderate correlation -- matrix captures some\n")
                w("  real patterns but could be refined.\n")
            else:
                w("  Interpretation: Weak correlation -- curated intuitions may not\n")
                w("  reflect actual scoring dynamics. Review recommended.\n")
        else:
            w("  Interpretation: Could not compute (insufficient variance).\n")
    else:
        r_val = float('nan')
        rho_val = float('nan')
        w("  Insufficient data (fewer than 3 role pairs with n >= 20).\n")
    w("\n")

    # ------------------------------------------------------------------
    # 5. Unlisted Role Pairs (defaulting to 5.0)
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("5. UNLISTED ROLE PAIRS (default = 5.0)\n")
    w("-" * 72 + "\n\n")

    unlisted = {k: v for k, v in sufficient.items() if not v.is_in_matrix}
    if unlisted:
        w(f"{'Role Pair':<50} {'n':>5} {'Mean':>7} {'StDev':>7} {'Flag':>12}\n")
        w(f"{'-'*50} {'-'*5} {'-'*7} {'-'*7} {'-'*12}\n")
        for key in sorted(unlisted.keys(), key=lambda k: unlisted[k].mean, reverse=True):
            ps = unlisted[key]
            # Flag pairs that deviate significantly from 5.0 default
            # Scale: curated 5.0 corresponds roughly to mean ~40 in harmonic scores
            # but we compare curated-to-curated; the delta vs 5.0 is more useful
            deviation = abs(ps.mean - 40.0)  # rough expectation for 5.0 curated
            flag = ""
            if ps.mean > 55.0:
                flag = "HIGH (add?)"
            elif ps.mean < 30.0:
                flag = "LOW (add?)"
            w(f"{ps.pair_label:<50} {ps.n:>5} {ps.mean:>7.2f} {ps.stdev:>7.2f} {flag:>12}\n")
    else:
        w("  No unlisted role pairs with n >= 20.\n")
    w("\n")

    # ------------------------------------------------------------------
    # 6. Top and Bottom Role Pairs
    # ------------------------------------------------------------------
    w("-" * 72 + "\n")
    w("6. TOP 10 AND BOTTOM 10 ROLE PAIRS (by empirical mean, n >= 20)\n")
    w("-" * 72 + "\n\n")

    sorted_pairs = sorted(sufficient.keys(), key=lambda k: sufficient[k].mean, reverse=True)

    w("TOP 10 (highest empirical mean harmonic_mean):\n")
    w(f"{'#':>3} {'Role Pair':<50} {'n':>5} {'Mean':>7} {'Curated':>8}\n")
    w(f"{'-'*3} {'-'*50} {'-'*5} {'-'*7} {'-'*8}\n")
    for i, key in enumerate(sorted_pairs[:10], 1):
        ps = sufficient[key]
        w(f"{i:>3} {ps.pair_label:<50} {ps.n:>5} {ps.mean:>7.2f} {ps.curated:>8.1f}\n")
    w("\n")

    w("BOTTOM 10 (lowest empirical mean harmonic_mean):\n")
    w(f"{'#':>3} {'Role Pair':<50} {'n':>5} {'Mean':>7} {'Curated':>8}\n")
    w(f"{'-'*3} {'-'*50} {'-'*5} {'-'*7} {'-'*8}\n")
    for i, key in enumerate(reversed(sorted_pairs[-10:]), 1):
        ps = sufficient[key]
        w(f"{i:>3} {ps.pair_label:<50} {ps.n:>5} {ps.mean:>7.2f} {ps.curated:>8.1f}\n")
    w("\n")

    # ------------------------------------------------------------------
    # Compare curated ranking vs empirical ranking
    # ------------------------------------------------------------------
    if len(sufficient) >= 5:
        w("Ranking comparison (curated rank vs empirical rank):\n")
        curated_ranked = sorted(sufficient.keys(), key=lambda k: sufficient[k].curated, reverse=True)
        empirical_ranked = sorted(sufficient.keys(), key=lambda k: sufficient[k].mean, reverse=True)

        curated_rank_map = {k: i + 1 for i, k in enumerate(curated_ranked)}
        empirical_rank_map = {k: i + 1 for i, k in enumerate(empirical_ranked)}

        w(f"{'Role Pair':<50} {'Curated Rank':>13} {'Empirical Rank':>15} {'Diff':>6}\n")
        w(f"{'-'*50} {'-'*13} {'-'*15} {'-'*6}\n")
        for key in curated_ranked:
            ps = sufficient[key]
            c_rank = curated_rank_map[key]
            e_rank = empirical_rank_map[key]
            diff = c_rank - e_rank
            w(f"{ps.pair_label:<50} {c_rank:>13} {e_rank:>15} {diff:>+6}\n")
    w("\n")

    w("=" * 72 + "\n")
    w("  END OF REPORT\n")
    w("=" * 72 + "\n")

    return buf.getvalue()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_csv(
    pair_stats: Dict[Tuple[str, str], RolePairStats],
    csv_path: str,
) -> None:
    """Export role-pair statistics to CSV."""
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'role_a', 'role_b', 'pair_label', 'n',
            'mean_harmonic', 'stdev_harmonic',
            'curated_score', 'in_matrix', 'delta_vs_curated_scaled',
        ])
        for key in sorted(pair_stats.keys()):
            ps = pair_stats[key]
            delta = ps.mean - ps.curated * 8.0 if ps.n > 0 else ''
            writer.writerow([
                ps.role_a, ps.role_b, ps.pair_label, ps.n,
                f"{ps.mean:.2f}" if ps.n > 0 else '',
                f"{ps.stdev:.2f}" if ps.n > 1 else '',
                ps.curated,
                'Yes' if ps.is_in_matrix else 'No',
                f"{delta:+.2f}" if isinstance(delta, float) else '',
            ])


# ---------------------------------------------------------------------------
# Visualisations
# ---------------------------------------------------------------------------

def plot_role_distribution(
    role_counts: Counter,
    total_profiles: int,
    out_path: str,
) -> None:
    """Horizontal bar chart of profile count per canonical role."""
    roles = list(reversed(CANONICAL_ROLES))
    counts = [role_counts.get(r, 0) for r in roles]
    colors = ['#e74c3c' if c < 50 else '#3498db' for c in counts]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(roles, counts, color=colors, edgecolor='white', linewidth=0.5)

    # Annotate with count and percentage
    for bar, count in zip(bars, counts):
        pct = 100.0 * count / total_profiles if total_profiles > 0 else 0
        ax.text(
            bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2,
            f"{count}  ({pct:.1f}%)",
            va='center', fontsize=9,
        )

    ax.set_xlabel('Number of Profiles', fontsize=11)
    ax.set_title('Profile Distribution by Canonical Network Role', fontsize=13, fontweight='bold')
    ax.axvline(x=50, color='red', linestyle='--', alpha=0.5, label='n=50 threshold')
    ax.legend(loc='lower right', fontsize=9)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_compatibility_heatmap(
    pair_stats: Dict[Tuple[str, str], RolePairStats],
    out_path: str,
) -> None:
    """
    NxN heatmap of curated compatibility scores, annotated with
    match counts and empirical mean scores.
    """
    roles = CANONICAL_ROLES
    n = len(roles)
    role_idx = {r: i for i, r in enumerate(roles)}

    # Build matrices
    curated_matrix = np.full((n, n), DEFAULT_COMPAT_SCORE)
    count_matrix = np.zeros((n, n), dtype=int)
    empirical_matrix = np.full((n, n), np.nan)

    for i, ra in enumerate(roles):
        for j, rb in enumerate(roles):
            curated_matrix[i, j] = curated_score(ra, rb)
            key = role_pair_key(ra, rb)
            if key in pair_stats:
                ps = pair_stats[key]
                count_matrix[i, j] = ps.n
                if ps.n >= 5:
                    empirical_matrix[i, j] = ps.mean

    fig, ax = plt.subplots(figsize=(14, 11))

    # Use seaborn if available, otherwise plain imshow
    if HAS_SEABORN:
        sns.heatmap(
            curated_matrix, ax=ax,
            xticklabels=roles, yticklabels=roles,
            cmap='RdYlGn', vmin=3.0, vmax=9.5,
            annot=False, linewidths=0.5, linecolor='white',
            cbar_kws={'label': 'Curated Compatibility Score'},
        )
    else:
        im = ax.imshow(curated_matrix, cmap='RdYlGn', vmin=3.0, vmax=9.5, aspect='auto')
        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels(roles, rotation=45, ha='right')
        ax.set_yticklabels(roles)
        plt.colorbar(im, ax=ax, label='Curated Compatibility Score')

    # Annotate cells with curated score, count, and empirical mean
    for i in range(n):
        for j in range(n):
            cur = curated_matrix[i, j]
            cnt = count_matrix[i, j]
            emp = empirical_matrix[i, j]

            # Build annotation text
            lines = [f"{cur:.1f}"]
            if cnt > 0:
                lines.append(f"n={cnt}")
            if not np.isnan(emp):
                lines.append(f"m={emp:.0f}")

            text = "\n".join(lines)

            # Choose text color based on background brightness
            bg_val = (cur - 3.0) / 6.5  # normalize to 0-1
            text_color = 'white' if bg_val < 0.35 else 'black'

            ax.text(
                j, i, text,
                ha='center', va='center',
                fontsize=7, color=text_color,
                fontweight='bold' if cnt >= 20 else 'normal',
            )

    ax.set_title(
        'Role Compatibility Matrix\n(curated score / match count / empirical mean)',
        fontsize=13, fontweight='bold',
    )
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_curated_vs_empirical_scatter(
    pair_stats: Dict[Tuple[str, str], RolePairStats],
    out_path: str,
) -> None:
    """
    Scatter plot of curated compatibility score vs mean harmonic_mean,
    with regression line and r-value annotation.
    """
    sufficient = {k: v for k, v in pair_stats.items() if v.n >= 20}
    if len(sufficient) < 3:
        print("  Skipping scatter plot: fewer than 3 pairs with n >= 20")
        return

    curated_vals = [sufficient[k].curated for k in sufficient]
    empirical_vals = [sufficient[k].mean for k in sufficient]
    counts = [sufficient[k].n for k in sufficient]
    labels = [sufficient[k].pair_label for k in sufficient]

    r_val = pearson_r(curated_vals, empirical_vals)
    rho_val = spearman_rho(curated_vals, empirical_vals)

    fig, ax = plt.subplots(figsize=(10, 8))

    # Scale point size by count
    max_count = max(counts) if counts else 1
    sizes = [30 + 200 * (c / max_count) for c in counts]

    scatter = ax.scatter(
        curated_vals, empirical_vals,
        s=sizes, alpha=0.7, c=curated_vals, cmap='RdYlGn',
        edgecolors='black', linewidths=0.5,
        vmin=3.0, vmax=9.5,
    )

    # Regression line
    if not math.isnan(r_val):
        x_arr = np.array(curated_vals)
        y_arr = np.array(empirical_vals)
        coeffs = np.polyfit(x_arr, y_arr, 1)
        x_line = np.linspace(min(curated_vals) - 0.5, max(curated_vals) + 0.5, 100)
        y_line = np.polyval(coeffs, x_line)
        ax.plot(x_line, y_line, 'r--', alpha=0.8, linewidth=2, label='Regression line')

    # Label a selection of points (avoid overlaps by labelling extremes)
    if labels:
        # Label top-5 highest and bottom-5 lowest empirical
        sorted_idx = sorted(range(len(empirical_vals)), key=lambda i: empirical_vals[i])
        to_label = set(sorted_idx[:5] + sorted_idx[-5:])
        for idx in to_label:
            ax.annotate(
                labels[idx],
                (curated_vals[idx], empirical_vals[idx]),
                textcoords='offset points', xytext=(5, 5),
                fontsize=7, alpha=0.8,
            )

    # Annotation box with correlation values
    r_text = f"Pearson r = {r_val:+.3f}\nSpearman rho = {rho_val:+.3f}\nn pairs = {len(sufficient)}"
    ax.text(
        0.05, 0.95, r_text,
        transform=ax.transAxes, fontsize=10,
        verticalalignment='top',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.8),
    )

    ax.set_xlabel('Curated Compatibility Score (0-10)', fontsize=11)
    ax.set_ylabel('Mean Harmonic Mean (empirical)', fontsize=11)
    ax.set_title(
        'Curated Role Compatibility vs. Empirical Match Quality',
        fontsize=13, fontweight='bold',
    )
    ax.legend(loc='lower right', fontsize=9)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate the hand-curated role compatibility matrix against empirical data.",
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Use synthetic test data instead of live database',
    )
    args = parser.parse_args()

    random.seed(42)
    np.random.seed(42)

    # ── Output directories ────────────────────────────────────────────
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(script_dir, 'validation_results')
    plots_dir = os.path.join(results_dir, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    report_path = os.path.join(results_dir, 'role_matrix_report.txt')
    csv_path = os.path.join(results_dir, 'role_matrix_data.csv')
    dist_plot = os.path.join(plots_dir, 'role_distribution.png')
    heatmap_plot = os.path.join(plots_dir, 'role_compatibility_heatmap.png')
    scatter_plot = os.path.join(plots_dir, 'curated_vs_empirical_scatter.png')

    # ── Load data ─────────────────────────────────────────────────────
    if args.test:
        canonical_role_map, raw_role_map, matches = load_test_data()
    else:
        canonical_role_map, raw_role_map, matches = load_live_data()

    total_profiles = len(canonical_role_map)
    total_matches = len(matches)

    # ── 1. Role Distribution ──────────────────────────────────────────
    print("\n[1/6] Analysing role distribution...")
    role_counts, unmapped_raws, raw_unmapped_counts = analyze_role_distribution(
        canonical_role_map, raw_role_map,
    )

    # ── 2-3. Role-Pair Frequency & Mean Scores ───────────────────────
    print("[2/6] Analysing role-pair frequencies...")
    pair_stats = analyze_role_pairs(canonical_role_map, matches)

    # ── 4-6. Generate report ──────────────────────────────────────────
    print("[3/6] Generating text report...")
    report_text = generate_report(
        role_counts, unmapped_raws, raw_unmapped_counts,
        pair_stats, total_profiles, total_matches,
    )
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"  Saved: {report_path}")

    # ── CSV ───────────────────────────────────────────────────────────
    print("[4/6] Exporting CSV data...")
    export_csv(pair_stats, csv_path)
    print(f"  Saved: {csv_path}")

    # ── Plots ─────────────────────────────────────────────────────────
    print("[5/6] Generating visualisations...")
    plot_role_distribution(role_counts, total_profiles, dist_plot)
    plot_compatibility_heatmap(pair_stats, heatmap_plot)
    plot_curated_vs_empirical_scatter(pair_stats, scatter_plot)

    # ── Summary ───────────────────────────────────────────────────────
    print("\n[6/6] Done!")
    print(f"\n  Report:   {report_path}")
    print(f"  CSV:      {csv_path}")
    print(f"  Plots:    {plots_dir}/")

    # Print correlation summary to stdout
    sufficient = {k: v for k, v in pair_stats.items() if v.n >= 20}
    if len(sufficient) >= 3:
        curated_vals = [sufficient[k].curated for k in sufficient]
        empirical_vals = [sufficient[k].mean for k in sufficient]
        r_val = pearson_r(curated_vals, empirical_vals)
        rho_val = spearman_rho(curated_vals, empirical_vals)
        print(f"\n  Correlation summary (n={len(sufficient)} pairs with >= 20 matches):")
        print(f"    Pearson  r  = {r_val:+.4f}")
        print(f"    Spearman rho = {rho_val:+.4f}")
    else:
        print(f"\n  Only {len(sufficient)} pairs with >= 20 matches; correlation not computed.")

    print()


if __name__ == '__main__':
    main()
