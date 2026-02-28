"""
Momentum Diagnostic — Answers why Momentum is the bottleneck in 66% of matches.

Q2: Sub-factor distribution for matches where Momentum is weakest
Q3: Data coverage analysis — how many profiles have meaningful Momentum data
Q4: Aggregation method comparison — harmonic vs geometric vs weighted average
"""
import os
import sys
import math
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

import statistics
from matching.models import SupabaseProfile, SupabaseMatch
from matching.services import SupabaseMatchScoringService


def run():
    scorer = SupabaseMatchScoringService()

    # Use 200 matches for a more representative sample
    matches = list(
        SupabaseMatch.objects.filter(match_score__isnull=False)
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

    # Score all matches and collect dimension breakdowns
    all_results = []
    for m in matches:
        pa = profiles.get(str(m.profile_id))
        pb = profiles.get(str(m.suggested_profile_id))
        if not pa or not pb:
            continue
        result = scorer.score_pair(pa, pb)
        all_results.append({
            'pa': pa, 'pb': pb,
            'result': result,
            'bd_ab': result['breakdown_ab'],
            'bd_ba': result['breakdown_ba'],
        })

    print(f'Scored {len(all_results)} match pairs')
    print()

    # ================================================================
    # Q1: MOMENTUM SCORING BREAKDOWN
    # ================================================================
    print('=' * 90)
    print('Q1: MOMENTUM SCORING FACTORS')
    print('=' * 90)
    print('''
  Factor                Weight    Score logic
  ────────────────────  ──────    ──────────────────────────────────
  Audience Engagement   3.0       engagement_score * 10, or 4.0 if null
  Social Reach          2.0       >=100K→10, >=50K→8, >=10K→6.5, >=1K→5, >0→4, 0→3
  Active Projects       2.5       8.0 if has text >10 chars, else 4.0
  List Size             2.5       >=100K→9, >=50K→8, >=10K→7, >=1K→5.5, >0→4, 0→3
''')

    # ================================================================
    # Q2: SUB-FACTOR DISTRIBUTION (for bottleneck matches)
    # ================================================================
    print('=' * 90)
    print('Q2: MOMENTUM SUB-FACTOR DISTRIBUTION')
    print('=' * 90)

    # Collect momentum factors from ab direction
    momentum_is_bottleneck = []
    momentum_not_bottleneck = []

    all_eng_scores = []
    all_reach_scores = []
    all_proj_scores = []
    all_list_scores = []

    for r in all_results:
        bd = r['bd_ab']
        dims = {
            'intent': bd['intent']['score'],
            'synergy': bd['synergy']['score'],
            'momentum': bd['momentum']['score'],
            'context': bd['context']['score'],
        }
        weakest = min(dims, key=dims.get)

        mom_factors = {f['name']: f['score'] for f in bd['momentum']['factors']}
        eng = mom_factors.get('Audience Engagement', 0)
        reach = mom_factors.get('Social Reach', 0)
        proj = mom_factors.get('Active Projects', 0)
        lst = mom_factors.get('List Size', 0)

        all_eng_scores.append(eng)
        all_reach_scores.append(reach)
        all_proj_scores.append(proj)
        all_list_scores.append(lst)

        if weakest == 'momentum':
            momentum_is_bottleneck.append(mom_factors)
        else:
            momentum_not_bottleneck.append(mom_factors)

    total = len(all_results)
    bn = len(momentum_is_bottleneck)
    print(f'\n  Momentum is bottleneck in {bn}/{total} ({bn/total*100:.0f}%) of scored directions')
    print()

    print(f'  {"Factor":<25} {"All matches":>12} {"When bottleneck":>16} {"When NOT bn":>14}')
    print(f'  {"─" * 25} {"─" * 12} {"─" * 16} {"─" * 14}')

    for name, all_scores in [
        ('Audience Engagement', all_eng_scores),
        ('Social Reach', all_reach_scores),
        ('Active Projects', all_proj_scores),
        ('List Size', all_list_scores),
    ]:
        bn_scores = [d[name] for d in momentum_is_bottleneck if name in d]
        nbn_scores = [d[name] for d in momentum_not_bottleneck if name in d]
        print(f'  {name:<25} {statistics.mean(all_scores):12.2f} '
              f'{statistics.mean(bn_scores):16.2f} '
              f'{statistics.mean(nbn_scores) if nbn_scores else 0:14.2f}')

    # Show the score value distribution for each factor
    print()
    print('  SCORE VALUE DISTRIBUTION (how many matches get each score):')
    for name, all_scores in [
        ('Audience Engagement', all_eng_scores),
        ('Social Reach', all_reach_scores),
        ('Active Projects', all_proj_scores),
        ('List Size', all_list_scores),
    ]:
        # Count distinct values
        from collections import Counter
        counts = Counter(all_scores)
        dist = sorted(counts.items())
        dist_str = ', '.join(f'{score:.1f}→{count}' for score, count in dist)
        print(f'  {name}: {dist_str}')

    # ================================================================
    # Q3: DATA COVERAGE — How many profiles have Momentum data?
    # ================================================================
    print()
    print('=' * 90)
    print('Q3: MOMENTUM DATA COVERAGE ACROSS ALL PROFILES IN SAMPLE')
    print('=' * 90)

    unique_profiles = list(profiles.values())
    total_p = len(unique_profiles)

    has_engagement = sum(1 for p in unique_profiles if p.audience_engagement_score is not None)
    has_reach = sum(1 for p in unique_profiles if p.social_reach and p.social_reach > 0)
    has_projects = sum(1 for p in unique_profiles
                       if p.current_projects and len(str(p.current_projects).strip()) > 10)
    has_list = sum(1 for p in unique_profiles if p.list_size and p.list_size > 0)

    print(f'\n  Total unique profiles in sample: {total_p}')
    print(f'  {"Field":<30} {"Has data":>10} {"Empty/null":>12} {"Coverage":>10}')
    print(f'  {"─" * 30} {"─" * 10} {"─" * 12} {"─" * 10}')
    print(f'  {"audience_engagement_score":<30} {has_engagement:10d} {total_p - has_engagement:12d} {has_engagement/total_p*100:9.1f}%')
    print(f'  {"social_reach (>0)":<30} {has_reach:10d} {total_p - has_reach:12d} {has_reach/total_p*100:9.1f}%')
    print(f'  {"current_projects (>10 chars)":<30} {has_projects:10d} {total_p - has_projects:12d} {has_projects/total_p*100:9.1f}%')
    print(f'  {"list_size (>0)":<30} {has_list:10d} {total_p - has_list:12d} {has_list/total_p*100:9.1f}%')

    # Show what scores profiles get when data is missing
    print()
    print('  DEFAULT SCORES WHEN DATA IS MISSING:')
    print('    audience_engagement_score = null → 4.0/10')
    print('    social_reach = 0 or null          → 3.0/10')
    print('    current_projects = empty           → 4.0/10')
    print('    list_size = 0 or null              → 3.0/10')
    print()

    # What's the theoretical max momentum score with all nulls?
    # (4.0*3 + 3.0*2 + 4.0*2.5 + 3.0*2.5) / (10*3 + 10*2 + 10*2.5 + 10*2.5) * 10
    null_total = 4.0*3 + 3.0*2 + 4.0*2.5 + 3.0*2.5
    null_max = 10*3 + 10*2 + 10*2.5 + 10*2.5
    null_score = null_total / null_max * 10
    print(f'  MOMENTUM SCORE WITH ALL NULL DATA: {null_score:.2f}/10')
    print(f'  This is the floor — {bn/total*100:.0f}% of matches hit near this floor.')

    # ================================================================
    # Q4: AGGREGATION METHOD COMPARISON
    # ================================================================
    print()
    print('=' * 90)
    print('Q4: AGGREGATION METHOD COMPARISON (200 matches)')
    print('=' * 90)

    weights = {'intent': 0.45, 'synergy': 0.25, 'momentum': 0.20, 'context': 0.10}

    harmonic_scores = []
    geometric_scores = []
    weighted_avg_scores = []

    for r in all_results:
        bd = r['bd_ab']
        dims = {
            'intent': bd['intent']['score'],
            'synergy': bd['synergy']['score'],
            'momentum': bd['momentum']['score'],
            'context': bd['context']['score'],
        }

        # Harmonic mean (current)
        epsilon = 1e-10
        total_w = sum(weights.values())
        recip_sum = sum(w / max(dims[d], epsilon) for d, w in weights.items())
        hm = (total_w / recip_sum) * 10 if recip_sum > 0 else 0

        # Geometric mean
        log_sum = sum(w * math.log(max(dims[d], epsilon)) for d, w in weights.items())
        gm = math.exp(log_sum / total_w) * 10

        # Weighted average
        wa = sum(dims[d] * w for d, w in weights.items()) / total_w * 10

        harmonic_scores.append(hm)
        geometric_scores.append(gm)
        weighted_avg_scores.append(wa)

    print(f'\n  {"Method":<25} {"Mean":>8} {"Median":>8} {"StdDev":>8} {"Min":>8} {"Max":>8}')
    print(f'  {"─" * 25} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8}')
    for name, scores in [
        ('Harmonic (current)', harmonic_scores),
        ('Geometric', geometric_scores),
        ('Weighted Average', weighted_avg_scores),
    ]:
        print(f'  {name:<25} {statistics.mean(scores):8.2f} {statistics.median(scores):8.2f} '
              f'{statistics.stdev(scores):8.2f} {min(scores):8.2f} {max(scores):8.2f}')

    # Show how many matches change tier with each method
    print()
    print('  TIER DISTRIBUTION BY METHOD:')

    def tier_label(s):
        if s >= 80: return 'Excellent'
        if s >= 60: return 'Good'
        if s >= 40: return 'Fair'
        return 'Poor'

    for name, scores in [
        ('Harmonic (current)', harmonic_scores),
        ('Geometric', geometric_scores),
        ('Weighted Average', weighted_avg_scores),
    ]:
        from collections import Counter
        tiers = Counter(tier_label(s) for s in scores)
        print(f'  {name:<25}  '
              f'Excellent: {tiers.get("Excellent", 0):>4}  '
              f'Good: {tiers.get("Good", 0):>4}  '
              f'Fair: {tiers.get("Fair", 0):>4}  '
              f'Poor: {tiers.get("Poor", 0):>4}')

    # Show which dimension is bottleneck under each method
    print()
    print('  BOTTLENECK FREQUENCY BY METHOD:')
    print('  (Harmonic penalizes weak dimensions most; weighted avg penalizes least)')
    print()

    # For harmonic, we already know — recalculate to verify
    # For geometric and weighted avg, the concept of "bottleneck" changes
    # but we can still show which dimension contributes least
    for name, method in [('Harmonic', 'harmonic'), ('Geometric', 'geometric'), ('Weighted Avg', 'weighted_avg')]:
        bottleneck = {'intent': 0, 'synergy': 0, 'momentum': 0, 'context': 0}
        for r in all_results:
            bd = r['bd_ab']
            dims = {
                'intent': bd['intent']['score'],
                'synergy': bd['synergy']['score'],
                'momentum': bd['momentum']['score'],
                'context': bd['context']['score'],
            }

            if method == 'harmonic':
                # Bottleneck = dimension with lowest score (harmonic is most sensitive to low values)
                weakest = min(dims, key=dims.get)
            elif method == 'geometric':
                # Impact ∝ weight * log(score) — lowest log-contribution
                contributions = {d: weights[d] * math.log(max(dims[d], 1e-10)) for d in dims}
                weakest = min(contributions, key=contributions.get)
            else:
                # Impact ∝ weight * score — lowest weighted contribution
                contributions = {d: weights[d] * dims[d] for d in dims}
                weakest = min(contributions, key=contributions.get)

            bottleneck[weakest] += 1

        total_r = len(all_results)
        parts = [f'{d}: {c}/{total_r} ({c/total_r*100:.0f}%)' for d, c in sorted(bottleneck.items(), key=lambda x: -x[1])]
        print(f'  {name:<15} {", ".join(parts)}')

    print()
    print('=' * 90)
    print('DONE')
    print('=' * 90)


if __name__ == '__main__':
    run()
