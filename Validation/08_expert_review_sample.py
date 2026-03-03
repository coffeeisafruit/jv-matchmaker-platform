#!/usr/bin/env python3
"""
08 - Expert Review Sample Generator
====================================

Generates a stratified sample of 100 match pairs for blind expert review,
the gold standard for validating any matching algorithm.

Strata:
  - 25 premier      (harmonic_mean >= 67)
  - 25 strong       (55 <= harmonic_mean < 67)
  - 25 aligned      (harmonic_mean < 55)
  - 25 asymmetric   (|score_ab - score_ba| > 15)

Outputs:
  validation_results/expert_review_cards.txt          -- blind review cards
  validation_results/expert_review_answer_key.csv     -- scores & strata
  validation_results/expert_review_ratings_template.csv -- blank template
  validation_results/expert_review_sample_report.txt  -- summary stats
  validation_results/sample_distribution.png          -- score overlay plot

Usage:
  python scripts/validation/08_expert_review_sample.py
  python scripts/validation/08_expert_review_sample.py --test
  python scripts/validation/08_expert_review_sample.py --compute-kappa validation_results/expert_review_ratings.csv
"""

import os
import sys
import csv
import random
import argparse
import textwrap
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from matching.models import SupabaseMatch, SupabaseProfile

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SEED = 42
SAMPLE_SIZE = 100
STRATUM_SIZE = 25
OUTPUT_DIR = os.path.join(
    os.path.dirname(__file__), 'validation_results'
)

REVIEW_FIELDS = [
    'name', 'company', 'niche', 'what_you_do', 'who_you_serve',
    'seeking', 'offering', 'audience_type', 'revenue_tier', 'list_size',
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_str(value: object) -> str:
    """Return a clean display string; handle None, Decimal, and empty."""
    if value is None:
        return '(not provided)'
    s = str(value).strip()
    return s if s else '(not provided)'


def to_float(d: object) -> float:
    """Convert Decimal / None to float safely."""
    if d is None:
        return 0.0
    return float(d)


def get_profile_map(profile_ids: list) -> Dict[str, SupabaseProfile]:
    """Bulk-fetch profiles by UUID, return dict keyed by str(uuid)."""
    profiles = SupabaseProfile.objects.filter(id__in=profile_ids)
    return {str(p.id): p for p in profiles}


def classify_stratum_from_harmonic(harmonic: float) -> str:
    """Map a harmonic_mean value to the algorithmic tier label."""
    if harmonic >= 67:
        return 'premier'
    elif harmonic >= 55:
        return 'strong'
    else:
        return 'aligned'


def rating_to_tier(rating: int) -> str:
    """Map a 1-5 expert rating to a tier label for kappa comparison."""
    if rating >= 4:
        return 'premier'
    elif rating == 3:
        return 'strong'
    else:
        return 'aligned'


# ---------------------------------------------------------------------------
# Stratified sampling
# ---------------------------------------------------------------------------

def build_strata(test_mode: bool = False) -> Dict[str, list]:
    """
    Query all matches with harmonic_mean, score_ab, score_ba not null,
    and partition them into four strata.

    Returns dict mapping stratum name -> list of SupabaseMatch instances.
    """
    base_qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False,
        score_ab__isnull=False,
        score_ba__isnull=False,
    )

    if test_mode:
        # Limit query size for quick testing
        base_qs = base_qs[:500]
        matches = list(base_qs)
    else:
        matches = list(base_qs)

    premier = []
    strong = []
    aligned = []
    asymmetric = []

    for m in matches:
        hm = to_float(m.harmonic_mean)
        ab = to_float(m.score_ab)
        ba = to_float(m.score_ba)
        diff = abs(ab - ba)

        # A match can qualify for asymmetric AND a tier; we assign to
        # asymmetric bucket separately so the same match could appear
        # in both pools.  During sampling we deduplicate.
        if diff > 15:
            asymmetric.append(m)

        if hm >= 67:
            premier.append(m)
        elif hm >= 55:
            strong.append(m)
        else:
            aligned.append(m)

    return {
        'premier': premier,
        'strong': strong,
        'aligned': aligned,
        'asymmetric': asymmetric,
    }


def sample_pairs(
    strata: Dict[str, list],
    test_mode: bool = False,
) -> List[Tuple[SupabaseMatch, str]]:
    """
    Draw a stratified random sample.  Returns list of (match, stratum_label)
    tuples of length SAMPLE_SIZE (or fewer if insufficient data).

    Matches selected for the asymmetric stratum are excluded from
    the other three strata to prevent duplicates.
    """
    random.seed(SEED)

    target = 5 if test_mode else STRATUM_SIZE
    selected_ids = set()
    result: List[Tuple[SupabaseMatch, str]] = []

    # Order: asymmetric first (so we can remove them from tier pools)
    order = ['asymmetric', 'premier', 'strong', 'aligned']

    for stratum_name in order:
        pool = [m for m in strata[stratum_name] if m.id not in selected_ids]
        random.shuffle(pool)
        chosen = pool[:target]
        for m in chosen:
            selected_ids.add(m.id)
            result.append((m, stratum_name))

    # Shuffle the final list so strata are interleaved (blind ordering)
    random.shuffle(result)
    return result


def resolve_profiles(
    pairs: List[Tuple[SupabaseMatch, str]],
) -> List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]]:
    """
    Look up both profiles for every match pair.
    Drops pairs where either profile is missing.
    """
    all_profile_ids = set()
    for m, _ in pairs:
        all_profile_ids.add(m.profile_id)
        all_profile_ids.add(m.suggested_profile_id)

    profile_map = get_profile_map(list(all_profile_ids))

    resolved = []
    skipped = 0
    for m, stratum in pairs:
        pa = profile_map.get(str(m.profile_id))
        pb = profile_map.get(str(m.suggested_profile_id))
        if pa is None or pb is None:
            skipped += 1
            continue
        resolved.append((m, stratum, pa, pb))

    if skipped:
        print(f"  Skipped {skipped} pairs with missing profile(s).")

    return resolved


# ---------------------------------------------------------------------------
# Output: blind review cards
# ---------------------------------------------------------------------------

def format_profile_card(profile: SupabaseProfile, label: str) -> str:
    """Format a single profile's fields for the review card."""
    lines = [f"  {label}:"]
    for field in REVIEW_FIELDS:
        value = getattr(profile, field, None)
        display = safe_str(value)
        # Wrap long values for readability
        if len(display) > 80:
            wrapped = textwrap.fill(display, width=76, initial_indent='      ',
                                    subsequent_indent='      ')
            lines.append(f"    {field}: ")
            lines.append(wrapped)
        else:
            lines.append(f"    {field}: {display}")
    return '\n'.join(lines)


def write_review_cards(
    resolved: List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]],
    output_path: str,
) -> None:
    """Write the blind review cards file."""
    with open(output_path, 'w') as f:
        # Header / instructions
        f.write("=" * 78 + "\n")
        f.write("  EXPERT REVIEW: MATCH QUALITY ASSESSMENT\n")
        f.write("=" * 78 + "\n\n")
        f.write("INSTRUCTIONS\n")
        f.write("-" * 78 + "\n")
        f.write("You are evaluating the quality of algorithmically generated\n")
        f.write("joint-venture match pairs. For each pair, review both profiles\n")
        f.write("and rate how well they would work together as JV partners.\n\n")
        f.write("Rating Scale:\n")
        f.write("  1 = Poor        - No meaningful overlap or complementarity\n")
        f.write("  2 = Below Avg   - Minor overlap but significant gaps\n")
        f.write("  3 = Average     - Reasonable fit with some alignment\n")
        f.write("  4 = Good        - Strong complementarity and audience fit\n")
        f.write("  5 = Excellent   - Ideal partners with clear mutual benefit\n\n")
        f.write("IMPORTANT: Rate based ONLY on the profile information shown.\n")
        f.write("Do not look up additional information about these people.\n")
        f.write("Record your ratings in the accompanying CSV template.\n\n")
        f.write(f"Total pairs to review: {len(resolved)}\n")
        f.write("=" * 78 + "\n\n")

        for idx, (match, stratum, pa, pb) in enumerate(resolved, start=1):
            f.write(f"{'=' * 78}\n")
            f.write(f"  PAIR #{idx:03d}\n")
            f.write(f"{'=' * 78}\n\n")

            f.write(format_profile_card(pa, "Profile A") + "\n\n")
            f.write(format_profile_card(pb, "Profile B") + "\n\n")

            f.write("  Rate this match quality 1-5:\n")
            f.write("  (1=Poor, 2=Below Average, 3=Average, 4=Good, 5=Excellent)\n\n")
            f.write("  Rating: ____\n\n")
            f.write("  Notes:\n")
            f.write("  " + "_" * 70 + "\n")
            f.write("  " + "_" * 70 + "\n")
            f.write("  " + "_" * 70 + "\n\n")

    print(f"  Review cards written to {output_path}")


# ---------------------------------------------------------------------------
# Output: answer key CSV
# ---------------------------------------------------------------------------

def write_answer_key(
    resolved: List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]],
    output_path: str,
) -> None:
    """Write the answer key mapping pair IDs to algorithmic data."""
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'pair_id', 'stratum', 'harmonic_mean', 'score_ab', 'score_ba',
            'score_diff', 'match_reason', 'profile_a_name', 'profile_b_name',
        ])
        for idx, (match, stratum, pa, pb) in enumerate(resolved, start=1):
            hm = to_float(match.harmonic_mean)
            ab = to_float(match.score_ab)
            ba = to_float(match.score_ba)
            diff = abs(ab - ba)
            reason = safe_str(match.match_reason)
            writer.writerow([
                idx, stratum, f"{hm:.2f}", f"{ab:.2f}", f"{ba:.2f}",
                f"{diff:.2f}", reason, pa.name, pb.name,
            ])
    print(f"  Answer key written to {output_path}")


# ---------------------------------------------------------------------------
# Output: blank ratings template
# ---------------------------------------------------------------------------

def write_ratings_template(
    resolved: List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]],
    output_path: str,
) -> None:
    """Write a blank CSV template for the expert to fill in."""
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['pair_id', 'rating', 'notes'])
        for idx in range(1, len(resolved) + 1):
            writer.writerow([idx, '', ''])
    print(f"  Ratings template written to {output_path}")


# ---------------------------------------------------------------------------
# Output: sample report
# ---------------------------------------------------------------------------

def write_sample_report(
    strata: Dict[str, list],
    resolved: List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]],
    output_path: str,
) -> None:
    """Write summary statistics about the sample."""
    stratum_counts = Counter(stratum for _, stratum, _, _ in resolved)
    scores = [to_float(m.harmonic_mean) for m, _, _, _ in resolved]
    diffs = [abs(to_float(m.score_ab) - to_float(m.score_ba))
             for m, _, _, _ in resolved]

    with open(output_path, 'w') as f:
        f.write("Expert Review Sample Report\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Generated with random seed: {SEED}\n")
        f.write(f"Total pairs sampled: {len(resolved)}\n\n")

        f.write("Population sizes (full dataset):\n")
        for name in ['premier', 'strong', 'aligned', 'asymmetric']:
            f.write(f"  {name:15s}: {len(strata[name]):,} matches\n")
        f.write(f"  {'TOTAL':15s}: {sum(len(v) for v in strata.values()):,} "
                f"(note: asymmetric overlaps with tiers)\n\n")

        f.write("Sample composition:\n")
        for name in ['premier', 'strong', 'aligned', 'asymmetric']:
            count = stratum_counts.get(name, 0)
            f.write(f"  {name:15s}: {count} pairs\n")
        f.write(f"  {'TOTAL':15s}: {len(resolved)} pairs\n\n")

        if scores:
            f.write("Harmonic mean (sampled pairs):\n")
            f.write(f"  Min:    {min(scores):.2f}\n")
            f.write(f"  Max:    {max(scores):.2f}\n")
            f.write(f"  Mean:   {sum(scores) / len(scores):.2f}\n")
            sorted_scores = sorted(scores)
            median = sorted_scores[len(sorted_scores) // 2]
            f.write(f"  Median: {median:.2f}\n\n")

        if diffs:
            f.write("Score asymmetry |score_ab - score_ba| (sampled pairs):\n")
            f.write(f"  Min:    {min(diffs):.2f}\n")
            f.write(f"  Max:    {max(diffs):.2f}\n")
            f.write(f"  Mean:   {sum(diffs) / len(diffs):.2f}\n\n")

        f.write("Next steps:\n")
        f.write("  1. Distribute expert_review_cards.txt to reviewer(s)\n")
        f.write("  2. Reviewer fills in expert_review_ratings_template.csv\n")
        f.write("  3. Run: python scripts/validation/08_expert_review_sample.py "
                "--compute-kappa validation_results/expert_review_ratings.csv\n")
        f.write("  4. Kappa > 0.60 indicates substantial algorithm-expert agreement\n")

    print(f"  Sample report written to {output_path}")


# ---------------------------------------------------------------------------
# Output: score distribution plot
# ---------------------------------------------------------------------------

def write_distribution_plot(
    resolved: List[Tuple[SupabaseMatch, str, SupabaseProfile, SupabaseProfile]],
    output_path: str,
) -> None:
    """
    Overlay the sampled pair score distribution on the full population
    distribution using matplotlib/seaborn.
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("  [SKIP] matplotlib not installed -- skipping distribution plot.")
        return

    try:
        import seaborn as sns
        has_seaborn = True
    except ImportError:
        has_seaborn = False

    # Full population scores
    all_scores_qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False,
    ).values_list('harmonic_mean', flat=True)
    all_scores = [float(s) for s in all_scores_qs]

    # Sampled scores
    sample_scores = [to_float(m.harmonic_mean) for m, _, _, _ in resolved]

    if not all_scores:
        print("  [SKIP] No scores found in database -- skipping plot.")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    bins = np.arange(0, 105, 5)

    if has_seaborn:
        sns.histplot(all_scores, bins=bins, stat='density', alpha=0.4,
                     color='steelblue', label='Full population', ax=ax)
        sns.histplot(sample_scores, bins=bins, stat='density', alpha=0.6,
                     color='coral', label='Expert review sample', ax=ax)
    else:
        ax.hist(all_scores, bins=bins, density=True, alpha=0.4,
                color='steelblue', label='Full population')
        ax.hist(sample_scores, bins=bins, density=True, alpha=0.6,
                color='coral', label='Expert review sample')

    ax.set_xlabel('Harmonic Mean Score')
    ax.set_ylabel('Density')
    ax.set_title('Score Distribution: Full Population vs. Expert Review Sample')
    ax.legend()
    ax.set_xlim(0, 100)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Distribution plot saved to {output_path}")


# ---------------------------------------------------------------------------
# Cohen's Kappa computation
# ---------------------------------------------------------------------------

def compute_kappa(ratings_file: str) -> None:
    """
    Read a completed expert ratings CSV and compute Cohen's kappa
    between the expert tier assignments and the algorithmic tier assignments.

    The ratings CSV must have columns: pair_id, rating
    The answer key CSV is loaded automatically from the standard location.

    Tier mapping:
      Expert rating 4-5 -> premier
      Expert rating 3   -> strong
      Expert rating 1-2 -> aligned

    Algorithmic tier is derived from the harmonic_mean in the answer key:
      harmonic_mean >= 67 -> premier
      55 <= harmonic_mean < 67 -> strong
      harmonic_mean < 55 -> aligned
    """
    answer_key_path = os.path.join(OUTPUT_DIR, 'expert_review_answer_key.csv')

    if not os.path.exists(ratings_file):
        print(f"ERROR: Ratings file not found: {ratings_file}")
        sys.exit(1)
    if not os.path.exists(answer_key_path):
        print(f"ERROR: Answer key not found: {answer_key_path}")
        print("  Run the sampling script first to generate it.")
        sys.exit(1)

    # Load answer key
    algo_tiers: Dict[int, str] = {}
    with open(answer_key_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pair_id = int(row['pair_id'])
            hm = float(row['harmonic_mean'])
            algo_tiers[pair_id] = classify_stratum_from_harmonic(hm)

    # Load expert ratings
    expert_tiers: Dict[int, str] = {}
    with open(ratings_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pair_id = int(row['pair_id'])
            rating_str = row.get('rating', '').strip()
            if not rating_str:
                continue
            try:
                rating = int(rating_str)
            except ValueError:
                print(f"  WARNING: Non-integer rating for pair {pair_id}: "
                      f"'{rating_str}' -- skipping.")
                continue
            if rating < 1 or rating > 5:
                print(f"  WARNING: Rating out of range for pair {pair_id}: "
                      f"{rating} -- skipping.")
                continue
            expert_tiers[pair_id] = rating_to_tier(rating)

    # Find overlapping pair IDs
    common_ids = sorted(set(algo_tiers.keys()) & set(expert_tiers.keys()))
    if len(common_ids) < 5:
        print(f"ERROR: Only {len(common_ids)} rated pairs found. "
              f"Need at least 5 for meaningful kappa.")
        sys.exit(1)

    algo_labels = [algo_tiers[pid] for pid in common_ids]
    expert_labels = [expert_tiers[pid] for pid in common_ids]

    # Compute Cohen's kappa manually (avoid sklearn dependency)
    categories = ['premier', 'strong', 'aligned']
    cat_to_idx = {c: i for i, c in enumerate(categories)}
    n = len(common_ids)
    k = len(categories)

    # Build confusion matrix
    confusion = [[0] * k for _ in range(k)]
    for a, e in zip(algo_labels, expert_labels):
        confusion[cat_to_idx[a]][cat_to_idx[e]] += 1

    # Observed agreement
    p_o = sum(confusion[i][i] for i in range(k)) / n

    # Expected agreement (by chance)
    row_sums = [sum(confusion[i]) for i in range(k)]
    col_sums = [sum(confusion[i][j] for i in range(k)) for j in range(k)]
    p_e = sum(row_sums[i] * col_sums[i] for i in range(k)) / (n * n)

    # Kappa
    if p_e == 1.0:
        kappa = 1.0
    else:
        kappa = (p_o - p_e) / (1.0 - p_e)

    # Interpretation
    if kappa >= 0.81:
        interpretation = "Almost perfect agreement"
    elif kappa >= 0.61:
        interpretation = "Substantial agreement"
    elif kappa >= 0.41:
        interpretation = "Moderate agreement"
    elif kappa >= 0.21:
        interpretation = "Fair agreement"
    elif kappa >= 0.0:
        interpretation = "Slight agreement"
    else:
        interpretation = "Poor agreement (worse than chance)"

    # Print results
    print("\n" + "=" * 60)
    print("  COHEN'S KAPPA: Algorithm vs. Expert Agreement")
    print("=" * 60)
    print(f"\n  Pairs rated:        {n}")
    print(f"  Observed agreement: {p_o:.3f}")
    print(f"  Expected agreement: {p_e:.3f}")
    print(f"  Cohen's kappa:      {kappa:.3f}")
    print(f"  Interpretation:     {interpretation}")

    target_met = kappa >= 0.60
    print(f"\n  Target (kappa >= 0.60): {'MET' if target_met else 'NOT MET'}")

    # Print confusion matrix
    print(f"\n  Confusion Matrix (rows=algorithm, cols=expert):")
    print(f"  {'':15s}", end='')
    for c in categories:
        print(f"  {c:>12s}", end='')
    print()
    for i, row_label in enumerate(categories):
        print(f"  {row_label:15s}", end='')
        for j in range(k):
            print(f"  {confusion[i][j]:>12d}", end='')
        print()

    # Per-category agreement
    print(f"\n  Per-category accuracy:")
    for i, cat in enumerate(categories):
        total_algo = row_sums[i]
        correct = confusion[i][i]
        if total_algo > 0:
            acc = correct / total_algo
            print(f"    {cat:15s}: {correct}/{total_algo} = {acc:.1%}")
        else:
            print(f"    {cat:15s}: (no algorithm matches in this tier)")

    print()

    if not target_met:
        print("  RECOMMENDATION: Kappa < 0.60 suggests the algorithm's tier")
        print("  boundaries may not align with expert judgment. Consider:")
        print("    - Adjusting tier thresholds")
        print("    - Reviewing low-agreement categories in the confusion matrix")
        print("    - Examining specific pairs where algorithm and expert disagree")
    else:
        print("  RESULT: Substantial agreement between algorithm and expert.")
        print("  The matching algorithm's tier assignments are validated.")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate stratified sample for blind expert review of match quality."
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Run in test mode with smaller sample (5 per stratum instead of 25).'
    )
    parser.add_argument(
        '--compute-kappa', metavar='RATINGS_CSV', type=str, default=None,
        help='Compute Cohen\'s kappa from a completed ratings CSV file.'
    )
    args = parser.parse_args()

    # If --compute-kappa is provided, run kappa computation and exit
    if args.compute_kappa:
        compute_kappa(args.compute_kappa)
        return

    test_mode = args.test

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    mode_label = "TEST MODE (5 per stratum)" if test_mode else "PRODUCTION (25 per stratum)"
    print(f"\n{'=' * 60}")
    print(f"  Expert Review Sample Generator -- {mode_label}")
    print(f"{'=' * 60}\n")
    print(f"  Random seed: {SEED}")
    print(f"  Output dir:  {OUTPUT_DIR}\n")

    # Step 1: Build strata
    print("[1/6] Building strata from match database...")
    strata = build_strata(test_mode=test_mode)
    for name in ['premier', 'strong', 'aligned', 'asymmetric']:
        print(f"  {name:15s}: {len(strata[name]):,} matches in pool")

    # Step 2: Sample
    print(f"\n[2/6] Drawing stratified sample...")
    pairs = sample_pairs(strata, test_mode=test_mode)
    print(f"  {len(pairs)} pairs selected")

    # Step 3: Resolve profiles
    print(f"\n[3/6] Resolving profile data...")
    resolved = resolve_profiles(pairs)
    print(f"  {len(resolved)} pairs with complete profiles")

    if not resolved:
        print("\nERROR: No valid pairs could be resolved. Check database connectivity.")
        sys.exit(1)

    # Step 4: Write outputs
    print(f"\n[4/6] Writing review cards (blind)...")
    cards_path = os.path.join(OUTPUT_DIR, 'expert_review_cards.txt')
    write_review_cards(resolved, cards_path)

    print(f"\n[5/6] Writing answer key, ratings template, and sample report...")
    key_path = os.path.join(OUTPUT_DIR, 'expert_review_answer_key.csv')
    write_answer_key(resolved, key_path)

    template_path = os.path.join(OUTPUT_DIR, 'expert_review_ratings_template.csv')
    write_ratings_template(resolved, template_path)

    report_path = os.path.join(OUTPUT_DIR, 'expert_review_sample_report.txt')
    write_sample_report(strata, resolved, report_path)

    # Step 6: Distribution plot
    print(f"\n[6/6] Generating score distribution plot...")
    plot_path = os.path.join(OUTPUT_DIR, 'sample_distribution.png')
    write_distribution_plot(resolved, plot_path)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  DONE -- Expert review sample generated")
    print(f"{'=' * 60}")
    print(f"\n  Files created:")
    print(f"    {cards_path}")
    print(f"    {key_path}")
    print(f"    {template_path}")
    print(f"    {report_path}")
    print(f"    {plot_path}")
    print(f"\n  Next steps:")
    print(f"    1. Send expert_review_cards.txt to reviewer(s)")
    print(f"    2. They fill in expert_review_ratings_template.csv")
    print(f"    3. Run: python scripts/validation/08_expert_review_sample.py \\")
    print(f"         --compute-kappa validation_results/expert_review_ratings.csv")
    print(f"    4. Target: kappa >= 0.60 (substantial agreement)\n")


if __name__ == '__main__':
    main()
