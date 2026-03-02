#!/usr/bin/env python
"""
Study 1.1: ISMC Weight Sensitivity Analysis

Recomputes ISMC scores under 6 different weight configurations using
per-dimension scores stored in match_context, then compares tier
distributions and identifies fragile vs. robust weight regimes.

Algorithm:
  1. Directional score: weighted geometric mean of dimension scores (0-10)
     S_dir = exp(sum(w_d * log(s_d)) / sum(w_d))  (only for non-None dims)
     Final directional = S_dir * 10  (0-100 scale)
  2. Pair score: harmonic mean of both directions
     H = 2 * S_ab * S_ba / (S_ab + S_ba)
  3. Tiers: hand_picked >= 67, strong >= 55, wildcard < 55
"""

import csv
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime

# Django setup
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, "/Users/josephtepe/Projects/jv-matchmaker-platform")

import django
django.setup()

from matching.models import SupabaseMatch, SupabaseProfile

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WEIGHT_CONFIGS = {
    "Current":         {"intent": 45, "synergy": 25, "momentum": 20, "context": 10},
    "Equal":           {"intent": 25, "synergy": 25, "momentum": 25, "context": 25},
    "Synergy-heavy":   {"intent": 20, "synergy": 40, "momentum": 20, "context": 20},
    "Intent-light":    {"intent": 25, "synergy": 30, "momentum": 25, "context": 20},
    "No-Context":      {"intent": 45, "synergy": 25, "momentum": 30, "context": 0},
    "Momentum-heavy":  {"intent": 20, "synergy": 20, "momentum": 40, "context": 20},
}

TIER_THRESHOLDS = {"hand_picked": 67, "strong": 55}  # wildcard < 55

EPSILON = 1e-10

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def tier_label(score: float) -> str:
    if score >= TIER_THRESHOLDS["hand_picked"]:
        return "hand_picked"
    elif score >= TIER_THRESHOLDS["strong"]:
        return "strong"
    else:
        return "wildcard"


def weighted_geometric_mean(dim_scores: dict, weights: dict) -> float:
    """
    Compute the weighted geometric mean of dimension scores.
    dim_scores: {intent: float|None, synergy: float|None, ...}  (0-10 scale)
    weights: {intent: int, synergy: int, ...}
    Returns score on 0-100 scale.
    """
    components = []
    for dim in ["intent", "synergy", "momentum", "context"]:
        s = dim_scores.get(dim)
        w = weights.get(dim, 0)
        if s is not None and w > 0:
            components.append((s, w))

    if not components:
        return 0.0

    total_weight = sum(w for _, w in components)
    if total_weight == 0:
        return 0.0

    log_sum = sum(w * math.log(max(s, EPSILON)) for s, w in components)
    final_0_10 = math.exp(log_sum / total_weight)
    return final_0_10 * 10  # convert to 0-100


def harmonic_mean(a: float, b: float) -> float:
    if a > EPSILON and b > EPSILON:
        return 2.0 * a * b / (a + b)
    return 0.0


def jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 1.0
    return len(set_a & set_b) / len(union)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
print("Loading match data from database...")
matches = SupabaseMatch.objects.filter(
    harmonic_mean__gt=0,
    match_context__isnull=False,
).exclude(match_context={}).values_list(
    "id", "profile_id", "suggested_profile_id", "match_context",
    "score_ab", "score_ba", "harmonic_mean",
)

# Build a name lookup for the top-swinging pairs
print("Loading profile names...")
profile_names = dict(
    SupabaseProfile.objects.values_list("id", "name")
)

# Parse match_context and extract per-dimension scores
print("Parsing match contexts...")
rows = []
skipped = 0

for match_id, pid_a, pid_b, mc_raw, orig_ab, orig_ba, orig_hm in matches:
    # match_context is stored as a JSON string inside a JSONField
    if isinstance(mc_raw, str):
        try:
            mc = json.loads(mc_raw)
        except (json.JSONDecodeError, TypeError):
            skipped += 1
            continue
    elif isinstance(mc_raw, dict):
        mc = mc_raw
    else:
        skipped += 1
        continue

    try:
        bd_ab = mc["breakdown_ab"]
        bd_ba = mc["breakdown_ba"]

        dims_ab = {
            "intent": bd_ab["intent"]["score"],
            "synergy": bd_ab["synergy"]["score"],
            "momentum": bd_ab["momentum"]["score"],
            "context": bd_ab["context"]["score"],
        }
        dims_ba = {
            "intent": bd_ba["intent"]["score"],
            "synergy": bd_ba["synergy"]["score"],
            "momentum": bd_ba["momentum"]["score"],
            "context": bd_ba["context"]["score"],
        }
    except (KeyError, TypeError):
        skipped += 1
        continue

    pair_key = f"{pid_a}|{pid_b}"

    rows.append({
        "match_id": str(match_id),
        "profile_a": str(pid_a),
        "profile_b": str(pid_b),
        "pair_key": pair_key,
        "name_a": profile_names.get(pid_a, "Unknown"),
        "name_b": profile_names.get(pid_b, "Unknown"),
        "dims_ab": dims_ab,
        "dims_ba": dims_ba,
        "orig_ab": float(orig_ab) if orig_ab else 0.0,
        "orig_ba": float(orig_ba) if orig_ba else 0.0,
        "orig_hm": float(orig_hm) if orig_hm else 0.0,
    })

print(f"Loaded {len(rows)} matches ({skipped} skipped due to parse errors)")

# ---------------------------------------------------------------------------
# Recompute scores under each weight config
# ---------------------------------------------------------------------------
print("\nRecomputing scores under each weight configuration...")
# results[config_name] = list of {pair_key, hm, tier, score_ab, score_ba}
results = {}

for config_name, weights in WEIGHT_CONFIGS.items():
    config_results = []
    for row in rows:
        s_ab = weighted_geometric_mean(row["dims_ab"], weights)
        s_ba = weighted_geometric_mean(row["dims_ba"], weights)
        hm = harmonic_mean(s_ab, s_ba)
        t = tier_label(hm)
        config_results.append({
            "pair_key": row["pair_key"],
            "score_ab": round(s_ab, 2),
            "score_ba": round(s_ba, 2),
            "hm": round(hm, 2),
            "tier": t,
        })
    results[config_name] = config_results
    print(f"  {config_name}: done")

# Also attach per-row results
for i, row in enumerate(rows):
    for config_name in WEIGHT_CONFIGS:
        r = results[config_name][i]
        row[f"hm_{config_name}"] = r["hm"]
        row[f"tier_{config_name}"] = r["tier"]

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
print("\n=== ANALYSIS ===\n")

# Current config as baseline
baseline = "Current"
baseline_results = results[baseline]
baseline_tiers = {r["pair_key"]: r["tier"] for r in baseline_results}

total_pairs = len(rows)

# 1. Tier distribution per config
print("1. Tier Distribution")
print("-" * 80)
tier_distributions = {}
for config_name in WEIGHT_CONFIGS:
    counts = defaultdict(int)
    for r in results[config_name]:
        counts[r["tier"]] += 1
    tier_distributions[config_name] = counts
    hp = counts["hand_picked"]
    st = counts["strong"]
    wc = counts["wildcard"]
    print(f"  {config_name:20s}  hand_picked: {hp:5d} ({hp/total_pairs*100:5.1f}%)  "
          f"strong: {st:5d} ({st/total_pairs*100:5.1f}%)  "
          f"wildcard: {wc:5d} ({wc/total_pairs*100:5.1f}%)")

# 2. Tier changes vs current
print(f"\n2. Tier Changes vs. '{baseline}'")
print("-" * 80)
tier_change_counts = {}
for config_name in WEIGHT_CONFIGS:
    if config_name == baseline:
        tier_change_counts[config_name] = 0
        continue
    changes = 0
    for r in results[config_name]:
        if r["tier"] != baseline_tiers[r["pair_key"]]:
            changes += 1
    tier_change_counts[config_name] = changes
    print(f"  {config_name:20s}  {changes:5d} pairs changed tier ({changes/total_pairs*100:5.1f}%)")

# 3. Jaccard similarity per tier vs current
print(f"\n3. Jaccard Similarity vs. '{baseline}'")
print("-" * 80)
jaccard_results = {}
for config_name in WEIGHT_CONFIGS:
    jaccards = {}
    for tier_name in ["hand_picked", "strong", "wildcard"]:
        set_baseline = {r["pair_key"] for r in baseline_results if r["tier"] == tier_name}
        set_config = {r["pair_key"] for r in results[config_name] if r["tier"] == tier_name}
        j = jaccard(set_baseline, set_config)
        jaccards[tier_name] = j
    jaccard_results[config_name] = jaccards
    hp_j = jaccards["hand_picked"]
    st_j = jaccards["strong"]
    wc_j = jaccards["wildcard"]
    print(f"  {config_name:20s}  hand_picked: {hp_j:.4f}  strong: {st_j:.4f}  wildcard: {wc_j:.4f}")

# 4. Top 10 pairs with biggest score swing
print(f"\n4. Top 10 Pairs with Biggest Score Swing (max absolute delta across all configs)")
print("-" * 120)

# For each pair, find max - min harmonic mean across all configs
swing_data = []
for i, row in enumerate(rows):
    all_hm = [results[cn][i]["hm"] for cn in WEIGHT_CONFIGS]
    swing = max(all_hm) - min(all_hm)
    best_config = list(WEIGHT_CONFIGS.keys())[all_hm.index(max(all_hm))]
    worst_config = list(WEIGHT_CONFIGS.keys())[all_hm.index(min(all_hm))]
    swing_data.append({
        "pair_key": row["pair_key"],
        "name_a": row["name_a"],
        "name_b": row["name_b"],
        "swing": round(swing, 2),
        "min_hm": round(min(all_hm), 2),
        "max_hm": round(max(all_hm), 2),
        "best_config": best_config,
        "worst_config": worst_config,
        "all_hm": {cn: results[cn][i]["hm"] for cn in WEIGHT_CONFIGS},
        "dims_ab": row["dims_ab"],
        "dims_ba": row["dims_ba"],
    })

swing_data.sort(key=lambda x: x["swing"], reverse=True)
top_10 = swing_data[:10]

print(f"  {'Rank':>4}  {'Name A':30s}  {'Name B':30s}  {'Swing':>6}  {'Min':>6}  {'Max':>6}  {'Worst Config':>18}  {'Best Config':>18}")
for rank, sd in enumerate(top_10, 1):
    name_a = sd["name_a"][:28]
    name_b = sd["name_b"][:28]
    print(f"  {rank:4d}  {name_a:30s}  {name_b:30s}  {sd['swing']:6.1f}  {sd['min_hm']:6.1f}  {sd['max_hm']:6.1f}  {sd['worst_config']:>18}  {sd['best_config']:>18}")

# 5. Verdict
print(f"\n5. Verdict")
print("-" * 80)
max_change_pct = 0
max_change_config = ""
for cn, changes in tier_change_counts.items():
    if cn == baseline:
        continue
    pct = changes / total_pairs * 100
    if pct > max_change_pct:
        max_change_pct = pct
        max_change_config = cn

avg_change_pct = sum(
    v / total_pairs * 100 for k, v in tier_change_counts.items() if k != baseline
) / (len(WEIGHT_CONFIGS) - 1)

if max_change_pct < 10:
    verdict = "ROBUST"
    verdict_detail = (
        f"The current weights are ROBUST. The worst-case config ('{max_change_config}') "
        f"changes only {max_change_pct:.1f}% of tier assignments. "
        f"Average tier change across all alternative configs is {avg_change_pct:.1f}%."
    )
elif max_change_pct < 20:
    verdict = "MODERATELY SENSITIVE"
    verdict_detail = (
        f"The current weights are MODERATELY SENSITIVE. The worst-case config "
        f"('{max_change_config}') changes {max_change_pct:.1f}% of tier assignments. "
        f"Average tier change across all alternative configs is {avg_change_pct:.1f}%. "
        f"Consider whether the tier boundary thresholds (67/55) need recalibration."
    )
else:
    verdict = "FRAGILE"
    verdict_detail = (
        f"The current weights are FRAGILE. The worst-case config "
        f"('{max_change_config}') changes {max_change_pct:.1f}% of tier assignments. "
        f"Average tier change across all alternative configs is {avg_change_pct:.1f}%. "
        f"Weight selection has outsized impact on recommendations. "
        f"Recommend validation against user feedback or outcome data before changing weights."
    )

print(f"  Verdict: {verdict}")
print(f"  {verdict_detail}")
print()

# ---------------------------------------------------------------------------
# Save CSV
# ---------------------------------------------------------------------------
csv_path = "/Users/josephtepe/Projects/jv-matchmaker-platform/Validation/study_1_1_weight_sensitivity.csv"
print(f"Saving CSV to {csv_path}...")

fieldnames = [
    "match_id", "profile_a", "profile_b", "name_a", "name_b",
    "intent_ab", "synergy_ab", "momentum_ab", "context_ab",
    "intent_ba", "synergy_ba", "momentum_ba", "context_ba",
]
for cn in WEIGHT_CONFIGS:
    fieldnames.extend([f"hm_{cn}", f"tier_{cn}"])
fieldnames.append("swing")

with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for i, row in enumerate(rows):
        all_hm = [results[cn][i]["hm"] for cn in WEIGHT_CONFIGS]
        swing = max(all_hm) - min(all_hm)
        csv_row = {
            "match_id": row["match_id"],
            "profile_a": row["profile_a"],
            "profile_b": row["profile_b"],
            "name_a": row["name_a"],
            "name_b": row["name_b"],
            "intent_ab": row["dims_ab"].get("intent"),
            "synergy_ab": row["dims_ab"].get("synergy"),
            "momentum_ab": row["dims_ab"].get("momentum"),
            "context_ab": row["dims_ab"].get("context"),
            "intent_ba": row["dims_ba"].get("intent"),
            "synergy_ba": row["dims_ba"].get("synergy"),
            "momentum_ba": row["dims_ba"].get("momentum"),
            "context_ba": row["dims_ba"].get("context"),
            "swing": round(swing, 2),
        }
        for cn in WEIGHT_CONFIGS:
            csv_row[f"hm_{cn}"] = row[f"hm_{cn}"]
            csv_row[f"tier_{cn}"] = row[f"tier_{cn}"]
        writer.writerow(csv_row)

print(f"  Wrote {len(rows)} rows.")

# ---------------------------------------------------------------------------
# Save Markdown summary
# ---------------------------------------------------------------------------
md_path = "/Users/josephtepe/Projects/jv-matchmaker-platform/Validation/study_1_1_weight_sensitivity.md"
print(f"Saving Markdown to {md_path}...")

lines = []
lines.append("# Study 1.1: ISMC Weight Sensitivity Analysis")
lines.append("")
lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
lines.append(f"**Total pairs analyzed:** {total_pairs:,}")
lines.append(f"**Scoring formula:** Weighted geometric mean per direction, harmonic mean across directions")
lines.append(f"**Tier thresholds:** hand_picked >= 67, strong >= 55, wildcard < 55")
lines.append("")

# Weight configs table
lines.append("## Weight Configurations")
lines.append("")
lines.append("| Config | Intent | Synergy | Momentum | Context |")
lines.append("|--------|--------|---------|----------|---------|")
for cn, w in WEIGHT_CONFIGS.items():
    lines.append(f"| {cn} | {w['intent']} | {w['synergy']} | {w['momentum']} | {w['context']} |")
lines.append("")

# Tier distribution
lines.append("## 1. Tier Distribution")
lines.append("")
lines.append("| Config | Hand-Picked | % | Strong | % | Wildcard | % |")
lines.append("|--------|-------------|---|--------|---|----------|---|")
for cn in WEIGHT_CONFIGS:
    c = tier_distributions[cn]
    hp, st, wc = c["hand_picked"], c["strong"], c["wildcard"]
    lines.append(
        f"| {cn} | {hp:,} | {hp/total_pairs*100:.1f}% "
        f"| {st:,} | {st/total_pairs*100:.1f}% "
        f"| {wc:,} | {wc/total_pairs*100:.1f}% |"
    )
lines.append("")

# Tier changes
lines.append("## 2. Tier Changes vs. Current Weights")
lines.append("")
lines.append("| Config | Pairs Changed | % of Total |")
lines.append("|--------|---------------|------------|")
for cn in WEIGHT_CONFIGS:
    if cn == baseline:
        lines.append(f"| {cn} | -- (baseline) | -- |")
        continue
    changes = tier_change_counts[cn]
    lines.append(f"| {cn} | {changes:,} | {changes/total_pairs*100:.1f}% |")
lines.append("")

# Jaccard similarity
lines.append("## 3. Jaccard Similarity vs. Current Weights")
lines.append("")
lines.append("| Config | Hand-Picked | Strong | Wildcard |")
lines.append("|--------|-------------|--------|----------|")
for cn in WEIGHT_CONFIGS:
    j = jaccard_results[cn]
    lines.append(f"| {cn} | {j['hand_picked']:.4f} | {j['strong']:.4f} | {j['wildcard']:.4f} |")
lines.append("")

# Top 10 swing
lines.append("## 4. Top 10 Pairs with Largest Score Swing")
lines.append("")
lines.append("| Rank | Name A | Name B | Swing | Min HM | Max HM | Worst Config | Best Config |")
lines.append("|------|--------|--------|-------|--------|--------|--------------|-------------|")
for rank, sd in enumerate(top_10, 1):
    lines.append(
        f"| {rank} | {sd['name_a'][:30]} | {sd['name_b'][:30]} | {sd['swing']:.1f} "
        f"| {sd['min_hm']:.1f} | {sd['max_hm']:.1f} "
        f"| {sd['worst_config']} | {sd['best_config']} |"
    )
lines.append("")

# Dimension scores for top-swing pairs
lines.append("### Dimension Scores for Top-Swing Pairs")
lines.append("")
for rank, sd in enumerate(top_10, 1):
    d_ab = sd["dims_ab"]
    d_ba = sd["dims_ba"]
    lines.append(f"**{rank}. {sd['name_a']} <-> {sd['name_b']}** (swing: {sd['swing']:.1f})")
    lines.append(f"- A->B: Intent={d_ab.get('intent')}, Synergy={d_ab.get('synergy')}, "
                 f"Momentum={d_ab.get('momentum')}, Context={d_ab.get('context')}")
    lines.append(f"- B->A: Intent={d_ba.get('intent')}, Synergy={d_ba.get('synergy')}, "
                 f"Momentum={d_ba.get('momentum')}, Context={d_ba.get('context')}")
    lines.append(f"- Scores by config: {', '.join(f'{cn}={hm:.1f}' for cn, hm in sd['all_hm'].items())}")
    lines.append("")

# Score distribution stats
lines.append("## 5. Score Distribution Statistics")
lines.append("")
lines.append("| Config | Mean HM | Median HM | Std Dev | P10 | P90 |")
lines.append("|--------|---------|-----------|---------|-----|-----|")
for cn in WEIGHT_CONFIGS:
    hm_values = sorted([r["hm"] for r in results[cn]])
    n = len(hm_values)
    mean_hm = sum(hm_values) / n
    median_hm = hm_values[n // 2]
    variance = sum((x - mean_hm) ** 2 for x in hm_values) / n
    std_hm = math.sqrt(variance)
    p10 = hm_values[int(n * 0.10)]
    p90 = hm_values[int(n * 0.90)]
    lines.append(f"| {cn} | {mean_hm:.1f} | {median_hm:.1f} | {std_hm:.1f} | {p10:.1f} | {p90:.1f} |")
lines.append("")

# Verdict
lines.append("## 6. Verdict")
lines.append("")
lines.append(f"**{verdict}**")
lines.append("")
lines.append(verdict_detail)
lines.append("")

# Methodology
lines.append("## Methodology")
lines.append("")
lines.append("1. Extracted per-dimension ISMC scores (0-10) from `match_context` JSON for all "
             f"{total_pairs:,} scored pairs")
lines.append("2. For each weight configuration, recomputed directional scores using:")
lines.append("   ```")
lines.append("   S_dir = exp(sum(w_d * log(s_d)) / sum(w_d)) * 10")
lines.append("   ```")
lines.append("   where only dimensions with non-null scores participate (weight is redistributed)")
lines.append("3. Combined directional scores via harmonic mean: `H = 2*S_ab*S_ba / (S_ab + S_ba)`")
lines.append("4. Assigned tiers: hand_picked >= 67, strong >= 55, wildcard < 55")
lines.append("5. Compared tier assignments, Jaccard similarity, and score distributions")
lines.append("")
lines.append("---")
lines.append(f"*Data source: `match_suggestions` table, {total_pairs:,} pairs with harmonic_mean > 0*")
lines.append("")

with open(md_path, "w") as f:
    f.write("\n".join(lines))

print(f"  Done.")
print(f"\nOutput files:")
print(f"  CSV:      {csv_path}")
print(f"  Markdown: {md_path}")
print("\nStudy 1.1 complete.")
