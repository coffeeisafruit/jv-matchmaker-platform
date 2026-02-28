"""
Synergy Recalibration Diagnostic — Measures impact of:
  1. Removing Platform Overlap (stdev 0.00, dead weight)
  2. Making Revenue Tier null-aware (14% fill rate)
  3. Reweighting: Offering↔Seeking 3.5, Audience 3.0, Role 2.5, Revenue 2.0

Compares OLD (pre-edit production scores) vs NEW (current engine) on 200 matches.
Does NOT write anything to the database.
"""
import os
import sys
import math
import statistics
from collections import Counter

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from matching.models import SupabaseProfile, SupabaseMatch
from matching.services import SupabaseMatchScoringService


def tier_label(s):
    if s >= 80:
        return 'Excellent'
    if s >= 60:
        return 'Good'
    if s >= 40:
        return 'Fair'
    return 'Poor'


def run():
    scorer = SupabaseMatchScoringService()

    # 200-match sample (top by old match_score, same as momentum diagnostic)
    matches = list(
        SupabaseMatch.objects.filter(harmonic_mean__isnull=False)
        .order_by('-match_score')[:200]
    )

    profile_ids = set()
    for m in matches:
        profile_ids.add(m.profile_id)
        profile_ids.add(m.suggested_profile_id)

    profiles = {
        str(p.id): p for p in SupabaseProfile.objects.filter(id__in=profile_ids)
    }
    print(f'Loaded {len(matches)} matches, {len(profiles)} profiles')
    print()

    # ------------------------------------------------------------------
    # Score all 200 matches with the CURRENT (modified) engine
    # ------------------------------------------------------------------
    new_results = []
    for m in matches:
        pa = profiles.get(str(m.profile_id))
        pb = profiles.get(str(m.suggested_profile_id))
        if not pa or not pb:
            continue
        result = scorer.score_pair(pa, pb)
        new_results.append({
            'match': m,
            'pa': pa,
            'pb': pb,
            'result': result,
            'old_hm': float(m.harmonic_mean) if m.harmonic_mean is not None else None,
        })

    print(f'Scored {len(new_results)} matches')
    print()

    # ------------------------------------------------------------------
    # 1. SYNERGY SUB-FACTOR ANALYSIS (new engine)
    # ------------------------------------------------------------------
    print('=' * 90)
    print('1. NEW SYNERGY SUB-FACTOR DISTRIBUTION')
    print('=' * 90)

    factor_scores = {}  # name -> list of scores
    revenue_included = 0
    revenue_excluded = 0

    for r in new_results:
        for direction in ['breakdown_ab', 'breakdown_ba']:
            bd = r['result'][direction]
            for f in bd['synergy']['factors']:
                factor_scores.setdefault(f['name'], []).append(f['score'])
            # Track Revenue Tier inclusion
            factor_names = [f['name'] for f in bd['synergy']['factors']]
            if 'Revenue Tier' in factor_names:
                revenue_included += 1
            else:
                revenue_excluded += 1

    print(f'\n  {"Factor":<25} {"Mean":>8} {"StdDev":>8} {"Min":>8} {"Max":>8} {"Count":>8}')
    print(f'  {"─" * 25} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8}')
    for name in ['Offering↔Seeking', 'Audience Alignment', 'Role Compatibility', 'Revenue Tier']:
        scores = factor_scores.get(name, [])
        if not scores:
            print(f'  {name:<25} {"N/A":>8}')
            continue
        print(f'  {name:<25} {statistics.mean(scores):8.2f} '
              f'{statistics.stdev(scores) if len(scores) > 1 else 0:8.2f} '
              f'{min(scores):8.2f} {max(scores):8.2f} {len(scores):8d}')

    total_dir = revenue_included + revenue_excluded
    print(f'\n  Revenue Tier inclusion: {revenue_included}/{total_dir} directions '
          f'({revenue_included/total_dir*100:.1f}%)')
    print(f'  Revenue Tier excluded (null-aware): {revenue_excluded}/{total_dir} directions '
          f'({revenue_excluded/total_dir*100:.1f}%)')

    # Score value distribution for each factor
    print(f'\n  SCORE VALUE DISTRIBUTION:')
    for name in ['Offering↔Seeking', 'Audience Alignment', 'Role Compatibility', 'Revenue Tier']:
        scores = factor_scores.get(name, [])
        if not scores:
            continue
        counts = Counter(scores)
        distinct = len(counts)
        top3 = counts.most_common(3)
        top3_str = ', '.join(f'{s:.1f}→{c}' for s, c in top3)
        print(f'  {name}: {distinct} distinct values, top 3: {top3_str}')

    # ------------------------------------------------------------------
    # 2. SYNERGY DIMENSION COMPARISON (old vs new)
    # ------------------------------------------------------------------
    print()
    print('=' * 90)
    print('2. SYNERGY SCORE: OLD vs NEW')
    print('=' * 90)

    old_synergy_scores = []
    new_synergy_scores = []

    for r in new_results:
        # New synergy from current engine
        new_syn_ab = r['result']['breakdown_ab']['synergy']['score']
        new_syn_ba = r['result']['breakdown_ba']['synergy']['score']
        new_synergy_scores.extend([new_syn_ab, new_syn_ba])

    # We don't have old synergy sub-scores stored, but we can see overall shift
    # via the harmonic_mean (overall ISMC) comparison
    print(f'\n  NEW Synergy (0-10):')
    print(f'    Mean:   {statistics.mean(new_synergy_scores):.2f}')
    print(f'    StdDev: {statistics.stdev(new_synergy_scores):.2f}')
    print(f'    Min:    {min(new_synergy_scores):.2f}')
    print(f'    Max:    {max(new_synergy_scores):.2f}')

    # ------------------------------------------------------------------
    # 3. OVERALL ISMC COMPARISON (old production vs new)
    # ------------------------------------------------------------------
    print()
    print('=' * 90)
    print('3. OVERALL ISMC SCORE: OLD (production) vs NEW (recalibrated)')
    print('=' * 90)

    old_hm_scores = []
    new_hm_scores = []

    for r in new_results:
        if r['old_hm'] is not None:
            old_hm_scores.append(r['old_hm'])
        new_hm_scores.append(r['result']['harmonic_mean'])

    print(f'\n  {"Metric":<20} {"OLD (prod)":>12} {"NEW":>12} {"Delta":>12}')
    print(f'  {"─" * 20} {"─" * 12} {"─" * 12} {"─" * 12}')
    for label, old_fn, new_fn in [
        ('Mean', statistics.mean, statistics.mean),
        ('Median', statistics.median, statistics.median),
        ('StdDev', statistics.stdev, statistics.stdev),
        ('Min', min, min),
        ('Max', max, max),
    ]:
        old_val = old_fn(old_hm_scores) if old_hm_scores else 0
        new_val = new_fn(new_hm_scores)
        delta = new_val - old_val
        print(f'  {label:<20} {old_val:12.2f} {new_val:12.2f} {delta:+12.2f}')

    # ------------------------------------------------------------------
    # 4. TIER DISTRIBUTION COMPARISON
    # ------------------------------------------------------------------
    print()
    print('=' * 90)
    print('4. TIER DISTRIBUTION: OLD vs NEW')
    print('=' * 90)

    old_tiers = Counter(tier_label(s) for s in old_hm_scores)
    new_tiers = Counter(tier_label(s) for s in new_hm_scores)

    print(f'\n  {"Tier":<12} {"OLD":>8} {"NEW":>8} {"Delta":>8}')
    print(f'  {"─" * 12} {"─" * 8} {"─" * 8} {"─" * 8}')
    for tier in ['Excellent', 'Good', 'Fair', 'Poor']:
        o = old_tiers.get(tier, 0)
        n = new_tiers.get(tier, 0)
        print(f'  {tier:<12} {o:8d} {n:8d} {n - o:+8d}')

    # ------------------------------------------------------------------
    # 5. BOTTLENECK FREQUENCY (new engine)
    # ------------------------------------------------------------------
    print()
    print('=' * 90)
    print('5. BOTTLENECK FREQUENCY (NEW engine, 200 matches)')
    print('=' * 90)

    bottleneck = Counter()
    for r in new_results:
        for direction in ['breakdown_ab', 'breakdown_ba']:
            bd = r['result'][direction]
            dims = {}
            for dim in ['intent', 'synergy', 'momentum', 'context']:
                s = bd[dim]['score']
                if s is not None:
                    dims[dim] = s
            if dims:
                weakest = min(dims, key=dims.get)
                bottleneck[weakest] += 1

    total_dirs = sum(bottleneck.values())
    print(f'\n  {"Dimension":<12} {"Count":>8} {"Pct":>8}')
    print(f'  {"─" * 12} {"─" * 8} {"─" * 8}')
    for dim, count in bottleneck.most_common():
        print(f'  {dim:<12} {count:8d} {count/total_dirs*100:7.1f}%')

    # ------------------------------------------------------------------
    # 6. TOP 10 BIGGEST SCORE CHANGES
    # ------------------------------------------------------------------
    print()
    print('=' * 90)
    print('6. TOP 10 BIGGEST SCORE CHANGES (old HM → new HM)')
    print('=' * 90)

    changes = []
    for r in new_results:
        if r['old_hm'] is not None:
            delta = r['result']['harmonic_mean'] - r['old_hm']
            changes.append({
                'pa': r['pa'].name or '?',
                'pb': r['pb'].name or '?',
                'old': r['old_hm'],
                'new': r['result']['harmonic_mean'],
                'delta': delta,
            })

    changes.sort(key=lambda x: abs(x['delta']), reverse=True)
    print(f'\n  {"#":>4} {"Old":>8} {"New":>8} {"Delta":>8}  {"Match"}')
    print(f'  {"─" * 4} {"─" * 8} {"─" * 8} {"─" * 8}  {"─" * 40}')
    for i, c in enumerate(changes[:10], 1):
        print(f'  {i:4d} {c["old"]:8.2f} {c["new"]:8.2f} {c["delta"]:+8.2f}  '
              f'{c["pa"][:20]} → {c["pb"][:20]}')

    # Also show top 5 positive and top 5 negative
    pos = sorted(changes, key=lambda x: x['delta'], reverse=True)[:5]
    neg = sorted(changes, key=lambda x: x['delta'])[:5]

    print(f'\n  TOP 5 POSITIVE:')
    for c in pos:
        print(f'    {c["old"]:8.2f} → {c["new"]:8.2f} ({c["delta"]:+.2f})  '
              f'{c["pa"][:20]} → {c["pb"][:20]}')

    print(f'\n  TOP 5 NEGATIVE:')
    for c in neg:
        print(f'    {c["old"]:8.2f} → {c["new"]:8.2f} ({c["delta"]:+.2f})  '
              f'{c["pa"][:20]} → {c["pb"][:20]}')

    print()
    print('=' * 90)
    print('DONE — diagnostic only, no database writes')
    print('=' * 90)


if __name__ == '__main__':
    run()
