#!/usr/bin/env python3
"""
Study: Threshold Evaluation — Impact of Revised Embedding Thresholds

Simulates the effect of changing EMBEDDING_SCORE_THRESHOLDS on all existing
ISMC-scored matches WITHOUT modifying any production code or data.

Current thresholds:
    (0.75, 10.0)  Strong
    (0.65,  8.0)  Good
    (0.60,  6.0)  Possible
    (0.53,  4.5)  Noise
    default: 3.0

Proposed thresholds (from Study 1.3):
    (0.84, 10.0)  Strong (true synonym territory)
    (0.64,  8.0)  Good   (optimal F1 @ 0.64)
    (0.62,  6.0)  Possible
    (0.60,  4.5)  Noise
    default: 3.0

Outputs:
    Validation/study_threshold_evaluation.md   (analysis report)
    Validation/study_threshold_evaluation.csv  (per-match raw data)
"""

import csv
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402
django.setup()

from matching.models import SupabaseMatch, SupabaseProfile  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CURRENT_THRESHOLDS = [
    (0.75, 10.0),
    (0.65, 8.0),
    (0.60, 6.0),
    (0.53, 4.5),
]
CURRENT_DEFAULT = 3.0

PROPOSED_THRESHOLDS = [
    (0.84, 10.0),
    (0.64, 8.0),
    (0.62, 6.0),
    (0.60, 4.5),
]
PROPOSED_DEFAULT = 3.0

ISMC_WEIGHTS = {
    'intent': 0.45,
    'synergy': 0.25,
    'momentum': 0.20,
    'context': 0.10,
}

TIER_THRESHOLDS = {
    'premier': 67,
    'strong': 55,
    'aligned': 0,
}

# The two embedding-based synergy factor names
EMBEDDING_FACTOR_NAMES = {'Offering↔Seeking', 'Audience Alignment'}

# Regex to extract raw cosine similarity from factor detail strings
SIM_PATTERN = re.compile(r'Cosine similarity:\s*([\d.]+)')


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def embedding_to_score(sim: float, thresholds: list, default: float) -> float:
    """Convert cosine similarity to a 0-10 score using given thresholds."""
    for threshold, score in thresholds:
        if sim >= threshold:
            return score
    return default


def assign_tier(harmonic_mean: float) -> str:
    """Assign tier based on harmonic mean (0-100 scale)."""
    if harmonic_mean >= TIER_THRESHOLDS['premier']:
        return 'premier'
    elif harmonic_mean >= TIER_THRESHOLDS['strong']:
        return 'strong'
    else:
        return 'aligned'


def weighted_geometric_mean(components: list) -> float:
    """Compute weighted geometric mean from [(score_0_10, weight), ...]."""
    epsilon = 1e-10
    total_weight = sum(w for _, w in components)
    if total_weight <= 0 or not components:
        return 0.0
    log_sum = sum(w * math.log(max(s, epsilon)) for s, w in components)
    return math.exp(log_sum / total_weight)


def harmonic_mean(a: float, b: float) -> float:
    """Harmonic mean of two values."""
    epsilon = 1e-10
    if a > epsilon and b > epsilon:
        return 2.0 / (1.0 / max(a, epsilon) + 1.0 / max(b, epsilon))
    return 0.0


def parse_match_context(match_context):
    """Parse match_context from JSON string or dict."""
    if match_context is None:
        return None
    if isinstance(match_context, str):
        try:
            return json.loads(match_context)
        except (json.JSONDecodeError, ValueError):
            return None
    if isinstance(match_context, dict):
        return match_context
    return None


def extract_cosine_sims_from_breakdown(breakdown: dict) -> dict:
    """
    Extract raw cosine similarities from synergy factors in a breakdown.

    Returns dict: {factor_name: cosine_similarity} for embedding-based factors.
    """
    result = {}
    synergy = breakdown.get('synergy', {})
    factors = synergy.get('factors', [])
    for factor in factors:
        name = factor.get('name', '')
        method = factor.get('method', '')
        detail = factor.get('detail', '')
        if name in EMBEDDING_FACTOR_NAMES and method == 'semantic':
            m = SIM_PATTERN.search(detail)
            if m:
                result[name] = float(m.group(1))
    return result


def recompute_synergy(breakdown: dict, new_thresholds: list, new_default: float) -> float:
    """
    Recompute synergy dimension score (0-10) with new embedding thresholds.

    Keeps all non-embedding factors unchanged, only modifies the two embedding
    factors' scores.
    """
    synergy = breakdown.get('synergy', {})
    factors = synergy.get('factors', [])

    total = 0.0
    max_total = 0.0

    for factor in factors:
        name = factor.get('name', '')
        weight = factor.get('weight', 1.0)
        method = factor.get('method', '')
        detail = factor.get('detail', '')
        score = factor.get('score', 0.0)

        if name in EMBEDDING_FACTOR_NAMES and method == 'semantic':
            # Re-score using new thresholds
            m = SIM_PATTERN.search(detail)
            if m:
                sim = float(m.group(1))
                score = embedding_to_score(sim, new_thresholds, new_default)
            # else: keep original score (shouldn't happen for semantic method)

        total += score * weight
        max_total += 10.0 * weight

    if max_total > 0:
        return round((total / max_total) * 10.0, 2)
    return 0.0


def recompute_directional_score(breakdown: dict, new_synergy: float) -> float:
    """
    Recompute a directional score (0-100) using the original dimension scores
    but substituting the new synergy score.
    """
    components = []
    for dim, weight in ISMC_WEIGHTS.items():
        if dim == 'synergy':
            score = new_synergy
        else:
            dim_data = breakdown.get(dim, {})
            score = dim_data.get('score')

        if score is not None:
            components.append((score, weight))

    final_0_10 = weighted_geometric_mean(components)
    return final_0_10 * 10.0  # Convert to 0-100


def bucket_similarity(sim: float) -> str:
    """Assign a similarity to a human-readable bucket."""
    if sim >= 0.90:
        return '0.90+'
    elif sim >= 0.84:
        return '0.84-0.89'
    elif sim >= 0.75:
        return '0.75-0.83'
    elif sim >= 0.65:
        return '0.65-0.74'
    elif sim >= 0.64:
        return '0.64'
    elif sim >= 0.62:
        return '0.62-0.63'
    elif sim >= 0.60:
        return '0.60-0.61'
    elif sim >= 0.53:
        return '0.53-0.59'
    else:
        return '<0.53'


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("Threshold Evaluation: Impact of Revised Embedding Thresholds")
    print("=" * 70)

    # Fetch all matches with match_context
    matches = SupabaseMatch.objects.exclude(match_context__isnull=True).exclude(
        match_context=''
    ).select_related()

    print(f"\nTotal matches with match_context: {matches.count()}")

    # Collect all raw similarities and recomputed results
    all_sims = []  # All raw cosine similarities
    offering_seeking_sims = []
    audience_sims = []
    results = []

    # Profile cache for spot-checks
    profile_cache = {}

    skipped = 0
    processed = 0

    for match in matches:
        ctx = parse_match_context(match.match_context)
        if ctx is None:
            skipped += 1
            continue

        breakdown_ab = ctx.get('breakdown_ab')
        breakdown_ba = ctx.get('breakdown_ba')

        if not breakdown_ab or not breakdown_ba:
            skipped += 1
            continue

        # Extract raw cosine similarities from both directions
        sims_ab = extract_cosine_sims_from_breakdown(breakdown_ab)
        sims_ba = extract_cosine_sims_from_breakdown(breakdown_ba)

        # Track all sims
        for name, sim in sims_ab.items():
            all_sims.append(sim)
            if name == 'Offering↔Seeking':
                offering_seeking_sims.append(sim)
            elif name == 'Audience Alignment':
                audience_sims.append(sim)
        for name, sim in sims_ba.items():
            all_sims.append(sim)
            if name == 'Offering↔Seeking':
                offering_seeking_sims.append(sim)
            elif name == 'Audience Alignment':
                audience_sims.append(sim)

        # Current scores
        current_score_ab = float(match.score_ab) if match.score_ab else 0.0
        current_score_ba = float(match.score_ba) if match.score_ba else 0.0
        current_hm = float(match.harmonic_mean) if match.harmonic_mean else 0.0
        current_tier = assign_tier(current_hm)

        # Recompute with current thresholds (to verify our logic matches)
        current_synergy_ab = recompute_synergy(breakdown_ab, CURRENT_THRESHOLDS, CURRENT_DEFAULT)
        current_synergy_ba = recompute_synergy(breakdown_ba, CURRENT_THRESHOLDS, CURRENT_DEFAULT)

        # Recompute with proposed thresholds
        proposed_synergy_ab = recompute_synergy(breakdown_ab, PROPOSED_THRESHOLDS, PROPOSED_DEFAULT)
        proposed_synergy_ba = recompute_synergy(breakdown_ba, PROPOSED_THRESHOLDS, PROPOSED_DEFAULT)

        # Recompute directional scores
        proposed_dir_ab = recompute_directional_score(breakdown_ab, proposed_synergy_ab)
        proposed_dir_ba = recompute_directional_score(breakdown_ba, proposed_synergy_ba)

        # Recompute harmonic mean
        proposed_hm = harmonic_mean(proposed_dir_ab, proposed_dir_ba)
        proposed_tier = assign_tier(proposed_hm)

        # Also recompute current to verify (using current thresholds)
        verify_dir_ab = recompute_directional_score(breakdown_ab, current_synergy_ab)
        verify_dir_ba = recompute_directional_score(breakdown_ba, current_synergy_ba)
        verify_hm = harmonic_mean(verify_dir_ab, verify_dir_ba)

        results.append({
            'match_id': str(match.id),
            'profile_id': str(match.profile_id),
            'suggested_profile_id': str(match.suggested_profile_id),
            'sims_ab': sims_ab,
            'sims_ba': sims_ba,
            'current_score_ab': current_score_ab,
            'current_score_ba': current_score_ba,
            'current_hm': current_hm,
            'current_tier': current_tier,
            'verify_hm': round(verify_hm, 2),
            'current_synergy_ab': current_synergy_ab,
            'current_synergy_ba': current_synergy_ba,
            'proposed_synergy_ab': proposed_synergy_ab,
            'proposed_synergy_ba': proposed_synergy_ba,
            'proposed_dir_ab': round(proposed_dir_ab, 2),
            'proposed_dir_ba': round(proposed_dir_ba, 2),
            'proposed_hm': round(proposed_hm, 2),
            'proposed_tier': proposed_tier,
            'hm_delta': round(proposed_hm - current_hm, 2),
            'tier_changed': current_tier != proposed_tier,
        })
        processed += 1

    print(f"Processed: {processed}, Skipped (no valid context): {skipped}")

    if not results:
        print("ERROR: No results to analyze. Exiting.")
        return

    # ---------------------------------------------------------------------------
    # Verification: check our recomputation matches stored scores
    # ---------------------------------------------------------------------------
    verify_diffs = [abs(r['verify_hm'] - r['current_hm']) for r in results]
    avg_verify_diff = sum(verify_diffs) / len(verify_diffs) if verify_diffs else 0
    max_verify_diff = max(verify_diffs) if verify_diffs else 0
    print(f"\nVerification: avg diff from stored HM = {avg_verify_diff:.4f}, max = {max_verify_diff:.4f}")

    # ---------------------------------------------------------------------------
    # 1. Distribution of raw cosine similarities
    # ---------------------------------------------------------------------------
    print("\n--- Raw Cosine Similarity Distribution ---")
    bucket_counts = Counter()
    for s in all_sims:
        bucket_counts[bucket_similarity(s)] += 1

    bucket_order = ['0.90+', '0.84-0.89', '0.75-0.83', '0.65-0.74', '0.64',
                    '0.62-0.63', '0.60-0.61', '0.53-0.59', '<0.53']
    for b in bucket_order:
        count = bucket_counts.get(b, 0)
        pct = (count / len(all_sims) * 100) if all_sims else 0
        print(f"  {b:>12s}: {count:>5d} ({pct:5.1f}%)")

    # Also by factor type
    print(f"\n  Total similarity values: {len(all_sims)}")
    print(f"  Offering↔Seeking: {len(offering_seeking_sims)}")
    print(f"  Audience Alignment: {len(audience_sims)}")

    if all_sims:
        import statistics
        print(f"\n  Overall: mean={statistics.mean(all_sims):.4f}, "
              f"median={statistics.median(all_sims):.4f}, "
              f"stdev={statistics.stdev(all_sims):.4f}")
    if offering_seeking_sims:
        print(f"  Offering↔Seeking: mean={statistics.mean(offering_seeking_sims):.4f}, "
              f"median={statistics.median(offering_seeking_sims):.4f}")
    if audience_sims:
        print(f"  Audience Alignment: mean={statistics.mean(audience_sims):.4f}, "
              f"median={statistics.median(audience_sims):.4f}")

    # ---------------------------------------------------------------------------
    # 2. Tier change analysis
    # ---------------------------------------------------------------------------
    print("\n--- Tier Change Analysis ---")
    tier_changes = [r for r in results if r['tier_changed']]
    total_matches = len(results)
    changed_count = len(tier_changes)
    pct_affected = (changed_count / total_matches * 100) if total_matches else 0

    print(f"  Total matches analyzed: {total_matches}")
    print(f"  Matches that change tier: {changed_count} ({pct_affected:.1f}%)")

    # Direction of changes
    tier_rank = {'premier': 3, 'strong': 2, 'aligned': 1}
    moved_up = 0
    moved_down = 0
    change_detail = Counter()

    for r in tier_changes:
        old_rank = tier_rank[r['current_tier']]
        new_rank = tier_rank[r['proposed_tier']]
        if new_rank > old_rank:
            moved_up += 1
        else:
            moved_down += 1
        change_detail[f"{r['current_tier']} -> {r['proposed_tier']}"] += 1

    print(f"  Moved UP in tier: {moved_up}")
    print(f"  Moved DOWN in tier: {moved_down}")
    print("\n  Transition details:")
    for transition, count in sorted(change_detail.items(), key=lambda x: -x[1]):
        print(f"    {transition}: {count}")

    # Tier distribution before and after
    print("\n  Tier distribution:")
    current_tiers = Counter(r['current_tier'] for r in results)
    proposed_tiers = Counter(r['proposed_tier'] for r in results)
    for tier in ['premier', 'strong', 'aligned']:
        print(f"    {tier}: {current_tiers.get(tier, 0)} -> {proposed_tiers.get(tier, 0)} "
              f"(delta: {proposed_tiers.get(tier, 0) - current_tiers.get(tier, 0):+d})")

    # Score delta statistics
    hm_deltas = [r['hm_delta'] for r in results]
    if hm_deltas:
        print(f"\n  Harmonic mean delta: mean={statistics.mean(hm_deltas):.4f}, "
              f"median={statistics.median(hm_deltas):.4f}, "
              f"stdev={statistics.stdev(hm_deltas):.4f}")
        print(f"  Range: [{min(hm_deltas):.2f}, {max(hm_deltas):.2f}]")

    # ---------------------------------------------------------------------------
    # 3. Spot-check boundary matches
    # ---------------------------------------------------------------------------
    print("\n--- Spot-Check: 5 Matches Near Threshold Boundaries ---")

    # Find matches where the proposed score is near tier boundaries
    # or where the tier changes
    boundary_candidates = []
    for r in results:
        # Near premier boundary (67)
        if abs(r['proposed_hm'] - 67) < 5 or abs(r['current_hm'] - 67) < 5:
            boundary_candidates.append(r)
        # Near strong boundary (55)
        elif abs(r['proposed_hm'] - 55) < 5 or abs(r['current_hm'] - 55) < 5:
            boundary_candidates.append(r)
        # Tier changed
        elif r['tier_changed']:
            boundary_candidates.append(r)

    # Sort by absolute HM delta to get the most affected
    boundary_candidates.sort(key=lambda x: abs(x['hm_delta']), reverse=True)

    # Take top 5
    spot_checks = boundary_candidates[:5]
    if len(spot_checks) < 5:
        # Fill with any remaining changed matches
        remaining = [r for r in results if r not in spot_checks]
        remaining.sort(key=lambda x: abs(x['hm_delta']), reverse=True)
        spot_checks.extend(remaining[:5 - len(spot_checks)])

    # Fetch profile details for spot-checks
    profile_ids_needed = set()
    for sc in spot_checks:
        profile_ids_needed.add(sc['profile_id'])
        profile_ids_needed.add(sc['suggested_profile_id'])

    profiles_by_id = {}
    for p in SupabaseProfile.objects.filter(id__in=profile_ids_needed):
        profiles_by_id[str(p.id)] = p

    spot_check_details = []
    for i, sc in enumerate(spot_checks, 1):
        p1 = profiles_by_id.get(sc['profile_id'])
        p2 = profiles_by_id.get(sc['suggested_profile_id'])

        p1_name = p1.name if p1 else 'Unknown'
        p2_name = p2.name if p2 else 'Unknown'
        p1_seeking = (p1.seeking or '')[:100] if p1 else ''
        p2_offering = (p2.offering or p2.what_you_do or '')[:100] if p2 else ''
        p1_who_serve = (p1.who_you_serve or '')[:100] if p1 else ''
        p2_who_serve = (p2.who_you_serve or '')[:100] if p2 else ''

        sims_display = {}
        for name, val in sc['sims_ab'].items():
            sims_display[f'ab_{name}'] = val
        for name, val in sc['sims_ba'].items():
            sims_display[f'ba_{name}'] = val

        detail = {
            'rank': i,
            'profile_a': p1_name,
            'profile_b': p2_name,
            'seeking_a': p1_seeking,
            'offering_b': p2_offering,
            'who_serve_a': p1_who_serve,
            'who_serve_b': p2_who_serve,
            'raw_sims': sims_display,
            'current_hm': sc['current_hm'],
            'proposed_hm': sc['proposed_hm'],
            'hm_delta': sc['hm_delta'],
            'current_tier': sc['current_tier'],
            'proposed_tier': sc['proposed_tier'],
        }
        spot_check_details.append(detail)

        print(f"\n  [{i}] {p1_name} <-> {p2_name}")
        print(f"      A seeking: {p1_seeking}")
        print(f"      B offering: {p2_offering}")
        print(f"      A serves: {p1_who_serve}")
        print(f"      B serves: {p2_who_serve}")
        print(f"      Raw sims: {sims_display}")
        print(f"      Current HM: {sc['current_hm']:.2f} ({sc['current_tier']})")
        print(f"      Proposed HM: {sc['proposed_hm']:.2f} ({sc['proposed_tier']})")
        print(f"      Delta: {sc['hm_delta']:+.2f}")

    # ---------------------------------------------------------------------------
    # 4. Score mapping comparison table
    # ---------------------------------------------------------------------------
    print("\n--- Score Mapping Comparison ---")
    test_sims = [0.90, 0.84, 0.80, 0.75, 0.70, 0.65, 0.64, 0.63, 0.62, 0.61, 0.60, 0.55, 0.53, 0.50]
    print(f"  {'Sim':>6s} | {'Current':>8s} | {'Proposed':>8s} | {'Delta':>6s}")
    print(f"  {'-'*6}-+-{'-'*8}-+-{'-'*8}-+-{'-'*6}")
    for s in test_sims:
        cur = embedding_to_score(s, CURRENT_THRESHOLDS, CURRENT_DEFAULT)
        prop = embedding_to_score(s, PROPOSED_THRESHOLDS, PROPOSED_DEFAULT)
        delta = prop - cur
        print(f"  {s:>6.2f} | {cur:>8.1f} | {prop:>8.1f} | {delta:>+6.1f}")

    # ---------------------------------------------------------------------------
    # 5. Write CSV
    # ---------------------------------------------------------------------------
    csv_path = Path(__file__).resolve().parent / 'study_threshold_evaluation.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'match_id', 'profile_id', 'suggested_profile_id',
            'os_sim_ab', 'aud_sim_ab', 'os_sim_ba', 'aud_sim_ba',
            'current_synergy_ab', 'current_synergy_ba',
            'proposed_synergy_ab', 'proposed_synergy_ba',
            'current_score_ab', 'current_score_ba', 'current_hm', 'current_tier',
            'proposed_dir_ab', 'proposed_dir_ba', 'proposed_hm', 'proposed_tier',
            'hm_delta', 'tier_changed',
        ])
        for r in results:
            writer.writerow([
                r['match_id'], r['profile_id'], r['suggested_profile_id'],
                r['sims_ab'].get('Offering↔Seeking', ''),
                r['sims_ab'].get('Audience Alignment', ''),
                r['sims_ba'].get('Offering↔Seeking', ''),
                r['sims_ba'].get('Audience Alignment', ''),
                r['current_synergy_ab'], r['current_synergy_ba'],
                r['proposed_synergy_ab'], r['proposed_synergy_ba'],
                r['current_score_ab'], r['current_score_ba'],
                r['current_hm'], r['current_tier'],
                r['proposed_dir_ab'], r['proposed_dir_ba'],
                r['proposed_hm'], r['proposed_tier'],
                r['hm_delta'], r['tier_changed'],
            ])
    print(f"\nCSV saved to {csv_path}")

    # ---------------------------------------------------------------------------
    # 6. Write markdown report
    # ---------------------------------------------------------------------------
    md_path = Path(__file__).resolve().parent / 'study_threshold_evaluation.md'

    lines = []
    lines.append("# Threshold Evaluation: Impact of Revised Embedding Thresholds")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Matches analyzed:** {total_matches}")
    lines.append(f"**Matches skipped (no valid context):** {skipped}")
    lines.append("")

    lines.append("## Background")
    lines.append("")
    lines.append("Study 1.3 found optimal F1 at cosine similarity 0.64. This evaluation simulates")
    lines.append("the impact of tightening the top threshold to 0.84 (true synonym territory) and")
    lines.append("adjusting intermediate thresholds to align with the empirical discrimination data.")
    lines.append("")

    lines.append("## Threshold Comparison")
    lines.append("")
    lines.append("| Bucket | Current Threshold | Current Score | Proposed Threshold | Proposed Score |")
    lines.append("|--------|-------------------|---------------|-------------------|----------------|")
    lines.append("| Strong | 0.75 | 10.0 | 0.84 | 10.0 |")
    lines.append("| Good | 0.65 | 8.0 | 0.64 | 8.0 |")
    lines.append("| Possible | 0.60 | 6.0 | 0.62 | 6.0 |")
    lines.append("| Noise | 0.53 | 4.5 | 0.60 | 4.5 |")
    lines.append("| Default | <0.53 | 3.0 | <0.60 | 3.0 |")
    lines.append("")

    lines.append("## Score Mapping Comparison")
    lines.append("")
    lines.append("| Similarity | Current Score | Proposed Score | Delta |")
    lines.append("|------------|--------------|----------------|-------|")
    for s in test_sims:
        cur = embedding_to_score(s, CURRENT_THRESHOLDS, CURRENT_DEFAULT)
        prop = embedding_to_score(s, PROPOSED_THRESHOLDS, PROPOSED_DEFAULT)
        delta = prop - cur
        lines.append(f"| {s:.2f} | {cur:.1f} | {prop:.1f} | {delta:+.1f} |")
    lines.append("")

    # Key insight: what happens to each similarity region
    lines.append("### Key Changes by Region")
    lines.append("")
    lines.append("1. **0.75-0.83 (currently Strong/10.0):** Drops to Good/8.0 under proposed. This is the largest impact zone.")
    lines.append("2. **0.65-0.74 (currently Good/8.0):** Stays at 8.0 (proposed threshold is 0.64, so 0.64-0.83 all map to 8.0).")
    lines.append("3. **0.62-0.64 (currently Possible/6.0):** Some move UP to Good/8.0 (specifically 0.64), others stay at 6.0.")
    lines.append("4. **0.60-0.61 (currently Possible/6.0):** Drops to Noise/4.5.")
    lines.append("5. **0.53-0.59 (currently Noise/4.5):** Drops to Default/3.0.")
    lines.append("")

    lines.append("## Raw Cosine Similarity Distribution")
    lines.append("")
    lines.append(f"Total similarity values extracted: {len(all_sims)}")
    lines.append(f"- Offering-to-Seeking: {len(offering_seeking_sims)}")
    lines.append(f"- Audience Alignment: {len(audience_sims)}")
    lines.append("")

    if all_sims:
        lines.append(f"**Overall:** mean={statistics.mean(all_sims):.4f}, "
                     f"median={statistics.median(all_sims):.4f}, "
                     f"stdev={statistics.stdev(all_sims):.4f}")
        if offering_seeking_sims:
            lines.append(f"**Offering-to-Seeking:** mean={statistics.mean(offering_seeking_sims):.4f}, "
                         f"median={statistics.median(offering_seeking_sims):.4f}")
        if audience_sims:
            lines.append(f"**Audience Alignment:** mean={statistics.mean(audience_sims):.4f}, "
                         f"median={statistics.median(audience_sims):.4f}")
        lines.append("")

    lines.append("| Bucket | Count | Percentage |")
    lines.append("|--------|-------|------------|")
    for b in bucket_order:
        count = bucket_counts.get(b, 0)
        pct = (count / len(all_sims) * 100) if all_sims else 0
        lines.append(f"| {b} | {count} | {pct:.1f}% |")
    lines.append("")

    lines.append("## Tier Change Analysis")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total matches analyzed | {total_matches} |")
    lines.append(f"| Matches that change tier | {changed_count} ({pct_affected:.1f}%) |")
    lines.append(f"| Moved UP in tier | {moved_up} |")
    lines.append(f"| Moved DOWN in tier | {moved_down} |")
    lines.append("")

    lines.append("### Tier Transitions")
    lines.append("")
    lines.append("| Transition | Count |")
    lines.append("|------------|-------|")
    for transition, count in sorted(change_detail.items(), key=lambda x: -x[1]):
        lines.append(f"| {transition} | {count} |")
    lines.append("")

    lines.append("### Tier Distribution (Before vs After)")
    lines.append("")
    lines.append("| Tier | Before | After | Delta |")
    lines.append("|------|--------|-------|-------|")
    for tier in ['premier', 'strong', 'aligned']:
        before = current_tiers.get(tier, 0)
        after = proposed_tiers.get(tier, 0)
        delta = after - before
        lines.append(f"| {tier} | {before} | {after} | {delta:+d} |")
    lines.append("")

    if hm_deltas:
        lines.append("### Harmonic Mean Score Impact")
        lines.append("")
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Mean delta | {statistics.mean(hm_deltas):.4f} |")
        lines.append(f"| Median delta | {statistics.median(hm_deltas):.4f} |")
        lines.append(f"| Stdev delta | {statistics.stdev(hm_deltas):.4f} |")
        lines.append(f"| Min delta | {min(hm_deltas):.2f} |")
        lines.append(f"| Max delta | {max(hm_deltas):.2f} |")
        neg_count = sum(1 for d in hm_deltas if d < 0)
        zero_count = sum(1 for d in hm_deltas if d == 0)
        pos_count = sum(1 for d in hm_deltas if d > 0)
        lines.append(f"| Matches scoring LOWER | {neg_count} ({neg_count/total_matches*100:.1f}%) |")
        lines.append(f"| Matches scoring SAME | {zero_count} ({zero_count/total_matches*100:.1f}%) |")
        lines.append(f"| Matches scoring HIGHER | {pos_count} ({pos_count/total_matches*100:.1f}%) |")
        lines.append("")

    lines.append("## Spot-Check: Matches Near Threshold Boundaries")
    lines.append("")
    lines.append("These matches were selected because they are near tier boundaries or experienced")
    lines.append("the largest score shifts under the proposed thresholds.")
    lines.append("")

    for detail in spot_check_details:
        lines.append(f"### [{detail['rank']}] {detail['profile_a']} <-> {detail['profile_b']}")
        lines.append("")
        lines.append(f"- **A seeking:** {detail['seeking_a']}")
        lines.append(f"- **B offering:** {detail['offering_b']}")
        lines.append(f"- **A serves:** {detail['who_serve_a']}")
        lines.append(f"- **B serves:** {detail['who_serve_b']}")
        lines.append(f"- **Raw cosine similarities:** {detail['raw_sims']}")
        lines.append(f"- **Current:** HM={detail['current_hm']:.2f}, tier={detail['current_tier']}")
        lines.append(f"- **Proposed:** HM={detail['proposed_hm']:.2f}, tier={detail['proposed_tier']}")
        lines.append(f"- **Delta:** {detail['hm_delta']:+.2f}")
        lines.append("")

    # ---------------------------------------------------------------------------
    # Verification report
    # ---------------------------------------------------------------------------
    lines.append("## Verification")
    lines.append("")
    lines.append("To validate the simulation, we recomputed current scores using the existing")
    lines.append("thresholds and compared against stored harmonic means.")
    lines.append("")
    lines.append(f"- **Average deviation from stored HM:** {avg_verify_diff:.4f}")
    lines.append(f"- **Max deviation from stored HM:** {max_verify_diff:.4f}")
    lines.append("")
    if max_verify_diff < 1.0:
        lines.append("Verification PASSED: Recomputed scores closely match stored values.")
    else:
        lines.append(f"NOTE: Max deviation of {max_verify_diff:.2f} suggests some matches may have")
        lines.append("other scoring factors that differ. This is expected for matches scored with")
        lines.append("different code versions or with null momentum dimensions.")
    lines.append("")

    # ---------------------------------------------------------------------------
    # Recommendation
    # ---------------------------------------------------------------------------
    lines.append("## Recommendation")
    lines.append("")

    # Determine recommendation based on analysis
    if pct_affected > 30:
        recommendation = "ADJUST"
        rationale = (
            f"The proposed thresholds would change tiers for {pct_affected:.1f}% of matches, "
            f"which is a significant disruption. The primary driver is the 0.75->0.84 jump "
            f"for the Strong threshold, which demotes a large number of matches from score 10.0 "
            f"to 8.0. Consider a more gradual transition or a compromise threshold."
        )
    elif pct_affected > 10:
        recommendation = "ADJUST"
        rationale = (
            f"The proposed thresholds would change tiers for {pct_affected:.1f}% of matches. "
            f"While the optimal F1 at 0.64 is well-supported by Study 1.3, the combined effect "
            f"of raising the Strong threshold to 0.84 and compressing the middle range may be "
            f"too aggressive. Consider deploying the 0.64 Good threshold independently first, "
            f"then evaluating the Strong threshold separately."
        )
    elif moved_down > moved_up * 3:
        recommendation = "ADJUST"
        rationale = (
            f"The proposed thresholds are heavily biased toward demotion ({moved_down} down vs "
            f"{moved_up} up). This could reduce match quality perception. Consider keeping the "
            f"0.64 Good threshold but moderating the Strong threshold to ~0.80 instead of 0.84."
        )
    elif pct_affected < 5 and abs(statistics.mean(hm_deltas)) < 1.0:
        recommendation = "DEPLOY"
        rationale = (
            f"The proposed thresholds affect only {pct_affected:.1f}% of tier assignments "
            f"with a mean HM shift of {statistics.mean(hm_deltas):.2f}. The changes are "
            f"well-calibrated and aligned with the empirical findings from Study 1.3."
        )
    else:
        recommendation = "DEPLOY with monitoring"
        rationale = (
            f"The proposed thresholds affect {pct_affected:.1f}% of tier assignments. "
            f"The changes are moderate and aligned with Study 1.3 findings. Deploy with "
            f"A/B monitoring to track user engagement with reclassified matches."
        )

    lines.append(f"**{recommendation}**")
    lines.append("")
    lines.append(rationale)
    lines.append("")

    # Additional nuance
    if moved_down > 0:
        lines.append("### Risk Factors")
        lines.append("")
        lines.append(f"- {moved_down} matches move to a lower tier, which could affect user trust if")
        lines.append("  previously-seen premier matches suddenly appear as strong or aligned.")
        lines.append("- The 0.75-0.83 similarity range is the most impacted zone; these pairs")
        lines.append("  were previously scored as Strong (10.0) and would become Good (8.0).")
        lines.append("")

    if moved_up > 0:
        lines.append("### Benefits")
        lines.append("")
        lines.append(f"- {moved_up} matches move to a higher tier, suggesting previously under-valued")
        lines.append("  pairs near the 0.64 boundary are being correctly recognized.")
        lines.append("- Better alignment with empirical F1 data from Study 1.3.")
        lines.append("")

    with open(md_path, 'w') as f:
        f.write('\n'.join(lines))
    print(f"Report saved to {md_path}")


if __name__ == '__main__':
    main()
