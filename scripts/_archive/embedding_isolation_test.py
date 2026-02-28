"""
Embedding Isolation Test — Answers pre-rescore diagnostic questions.

Runs ISMC scoring on 50 matches two ways:
  A) Force word-overlap only (ignore embeddings)
  B) Use embeddings when available (production path)

Reports:
  1. Synergy sub-factor scores side-by-side (isolation)
  2. Full ISMC dimension breakdown (Intent, Synergy, Momentum, Context)
  3. Three hand-picked semantic pairs with raw cosine + calibrated scores
  4. Which ISMC dimension is the bottleneck
"""
import os
import sys
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

import json
import statistics
from matching.models import SupabaseProfile, SupabaseMatch
from matching.services import SupabaseMatchScoringService


def run_test():
    scorer = SupabaseMatchScoringService()

    # Load 50 matches that have match_score (top-scored ones for variety)
    matches = list(
        SupabaseMatch.objects.filter(match_score__isnull=False)
        .order_by('-match_score')[:50]
    )

    # Pre-load profiles
    profile_ids = set()
    for m in matches:
        profile_ids.add(m.profile_id)
        profile_ids.add(m.suggested_profile_id)

    profiles = {
        str(p.id): p for p in SupabaseProfile.objects.filter(id__in=profile_ids)
    }

    print(f'Loaded {len(matches)} matches, {len(profiles)} profiles')
    print()

    # ================================================================
    # PASS A: Word overlap only (null out embedding attrs temporarily)
    # PASS B: With embeddings (production path)
    # ================================================================

    results_a = []  # word overlap
    results_b = []  # embeddings

    embedding_fields = [
        'embedding_seeking', 'embedding_offering',
        'embedding_who_you_serve', 'embedding_what_you_do'
    ]

    for m in matches:
        pa = profiles.get(str(m.profile_id))
        pb = profiles.get(str(m.suggested_profile_id))
        if not pa or not pb:
            continue

        # --- PASS A: Force word overlap by temporarily nulling embeddings ---
        saved_embs = {}
        for prof in (pa, pb):
            saved_embs[str(prof.id)] = {}
            for field in embedding_fields:
                saved_embs[str(prof.id)][field] = getattr(prof, field, None)
                setattr(prof, field, None)

        result_a = scorer.score_pair(pa, pb)

        # --- Restore embeddings ---
        for prof in (pa, pb):
            for field in embedding_fields:
                setattr(prof, field, saved_embs[str(prof.id)][field])

        # --- PASS B: With embeddings (production) ---
        result_b = scorer.score_pair(pa, pb)

        results_a.append({
            'match_id': str(m.id),
            'profile_a': pa.name,
            'profile_b': pb.name,
            'result': result_a,
        })
        results_b.append({
            'match_id': str(m.id),
            'profile_a': pa.name,
            'profile_b': pb.name,
            'result': result_b,
        })

    # ================================================================
    # REPORT: Q2 — Synergy sub-factor side-by-side
    # ================================================================
    print('=' * 90)
    print('Q2: SYNERGY SUB-FACTOR ISOLATION (same 50 matches, word_overlap vs embedding)')
    print('=' * 90)
    print()
    print(f'{"#":>3}  {"Profile A":<20}  {"Profile B":<20}  '
          f'{"WO_Offer":>8}  {"EM_Offer":>8}  {"WO_Aud":>7}  {"EM_Aud":>7}  '
          f'{"WO_Syn":>7}  {"EM_Syn":>7}  {"Delta":>7}')
    print(f'{"─" * 3}  {"─" * 20}  {"─" * 20}  '
          f'{"─" * 8}  {"─" * 8}  {"─" * 7}  {"─" * 7}  '
          f'{"─" * 7}  {"─" * 7}  {"─" * 7}')

    synergy_deltas = []
    offer_wo_scores = []
    offer_em_scores = []
    aud_wo_scores = []
    aud_em_scores = []

    for i, (ra, rb) in enumerate(zip(results_a, results_b)):
        bd_a_ab = ra['result']['breakdown_ab']
        bd_b_ab = rb['result']['breakdown_ab']

        syn_a = bd_a_ab['synergy']
        syn_b = bd_b_ab['synergy']

        # Extract offering↔seeking and audience factors
        offer_wo = next((f['score'] for f in syn_a['factors'] if f['name'] == 'Offering↔Seeking'), None)
        offer_em = next((f['score'] for f in syn_b['factors'] if f['name'] == 'Offering↔Seeking'), None)
        aud_wo = next((f['score'] for f in syn_a['factors'] if f['name'] == 'Audience Alignment'), None)
        aud_em = next((f['score'] for f in syn_b['factors'] if f['name'] == 'Audience Alignment'), None)

        syn_wo = syn_a['score']
        syn_em = syn_b['score']
        delta = syn_em - syn_wo

        synergy_deltas.append(delta)
        if offer_wo is not None: offer_wo_scores.append(offer_wo)
        if offer_em is not None: offer_em_scores.append(offer_em)
        if aud_wo is not None: aud_wo_scores.append(aud_wo)
        if aud_em is not None: aud_em_scores.append(aud_em)

        print(f'{i+1:3d}  {ra["profile_a"][:20]:<20}  {ra["profile_b"][:20]:<20}  '
              f'{offer_wo:8.1f}  {offer_em:8.1f}  {aud_wo:7.1f}  {aud_em:7.1f}  '
              f'{syn_wo:7.2f}  {syn_em:7.2f}  {delta:+7.2f}')

    print()
    print('SYNERGY SUMMARY (embedding impact on synergy dimension only):')
    print(f'  Offering↔Seeking  word_overlap mean: {statistics.mean(offer_wo_scores):.2f}  '
          f'embedding mean: {statistics.mean(offer_em_scores):.2f}  '
          f'delta: {statistics.mean(offer_em_scores) - statistics.mean(offer_wo_scores):+.2f}')
    print(f'  Audience Alignment  word_overlap mean: {statistics.mean(aud_wo_scores):.2f}  '
          f'embedding mean: {statistics.mean(aud_em_scores):.2f}  '
          f'delta: {statistics.mean(aud_em_scores) - statistics.mean(aud_wo_scores):+.2f}')
    print(f'  Overall synergy  word_overlap mean: {statistics.mean([ra["result"]["breakdown_ab"]["synergy"]["score"] for ra in results_a]):.2f}  '
          f'embedding mean: {statistics.mean([rb["result"]["breakdown_ab"]["synergy"]["score"] for rb in results_b]):.2f}  '
          f'delta: {statistics.mean(synergy_deltas):+.2f}')
    print(f'  Synergy delta range: [{min(synergy_deltas):+.2f}, {max(synergy_deltas):+.2f}]')

    # ================================================================
    # REPORT: Q1 — ISMC scores side-by-side (full harmonic mean)
    # ================================================================
    print()
    print('=' * 90)
    print('Q1: FULL ISMC SCORE COMPARISON (word_overlap vs embedding)')
    print('=' * 90)

    hm_wo = [ra['result']['harmonic_mean'] for ra in results_a]
    hm_em = [rb['result']['harmonic_mean'] for rb in results_b]
    hm_deltas = [e - w for w, e in zip(hm_wo, hm_em)]

    print(f'  Word overlap ISMC mean:  {statistics.mean(hm_wo):.2f}')
    print(f'  Embedding ISMC mean:     {statistics.mean(hm_em):.2f}')
    print(f'  Delta (embedding - WO):  {statistics.mean(hm_deltas):+.2f}')
    print(f'  Delta median:            {statistics.median(hm_deltas):+.2f}')
    print(f'  Delta stdev:             {statistics.stdev(hm_deltas):.2f}')
    print(f'  Delta range:             [{min(hm_deltas):+.2f}, {max(hm_deltas):+.2f}]')

    # ================================================================
    # REPORT: Q4 — ISMC dimension breakdown (which is the bottleneck?)
    # ================================================================
    print()
    print('=' * 90)
    print('Q4: ISMC DIMENSION BREAKDOWN (with embeddings — production path)')
    print('=' * 90)

    # Collect all dimension scores from pass B (embedding path)
    intent_scores = []
    synergy_scores = []
    momentum_scores = []
    context_scores = []

    for rb in results_b:
        bd = rb['result']['breakdown_ab']
        intent_scores.append(bd['intent']['score'])
        synergy_scores.append(bd['synergy']['score'])
        momentum_scores.append(bd['momentum']['score'])
        context_scores.append(bd['context']['score'])

    print(f'  {"Dimension":<20} {"Weight":>8} {"Mean":>8} {"Median":>8} {"Min":>8} {"Max":>8} {"StdDev":>8}')
    print(f'  {"─" * 20} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8} {"─" * 8}')
    for name, w, scores in [
        ('Intent', 0.45, intent_scores),
        ('Synergy', 0.25, synergy_scores),
        ('Momentum', 0.20, momentum_scores),
        ('Context', 0.10, context_scores),
    ]:
        print(f'  {name:<20} {w:8.2f} {statistics.mean(scores):8.2f} '
              f'{statistics.median(scores):8.2f} {min(scores):8.2f} {max(scores):8.2f} '
              f'{statistics.stdev(scores):8.2f}')

    # Also show the harmonic mean effect
    print()
    print('  Note: Final score uses WEIGHTED HARMONIC MEAN — the lowest dimension')
    print('  drags the score down disproportionately. A single weak dimension')
    print('  constrains the overall score.')

    # Show which dimension is most often the lowest
    bottleneck_count = {'intent': 0, 'synergy': 0, 'momentum': 0, 'context': 0}
    for rb in results_b:
        bd = rb['result']['breakdown_ab']
        dims = {
            'intent': bd['intent']['score'],
            'synergy': bd['synergy']['score'],
            'momentum': bd['momentum']['score'],
            'context': bd['context']['score'],
        }
        weakest = min(dims, key=dims.get)
        bottleneck_count[weakest] += 1

    print()
    print('  BOTTLENECK FREQUENCY (how often each dimension is the lowest scorer):')
    for dim, count in sorted(bottleneck_count.items(), key=lambda x: -x[1]):
        print(f'    {dim}: {count}/{len(results_b)} ({count/len(results_b)*100:.0f}%)')

    # ================================================================
    # REPORT: Q3 — Embedding verification on specific pairs
    # ================================================================
    print()
    print('=' * 90)
    print('Q3: EMBEDDING VERIFICATION — 3 specific pairs with semantic match potential')
    print('=' * 90)

    # Find pairs where seeking/offering SHOULD match semantically
    # Look for pairs with high embedding synergy but low word overlap
    best_uplift_pairs = []
    for ra, rb in zip(results_a, results_b):
        bd_a = ra['result']['breakdown_ab']
        bd_b = rb['result']['breakdown_ab']
        syn_wo = bd_a['synergy']['score']
        syn_em = bd_b['synergy']['score']
        uplift = syn_em - syn_wo
        if uplift > 0.5:  # meaningful difference
            best_uplift_pairs.append((ra, rb, uplift))

    best_uplift_pairs.sort(key=lambda x: -x[2])

    for idx, (ra, rb, uplift) in enumerate(best_uplift_pairs[:3]):
        pa = profiles.get(str(matches[results_a.index(ra)].profile_id))
        pb = profiles.get(str(matches[results_a.index(ra)].suggested_profile_id))
        if not pa or not pb:
            continue

        print(f'\n  --- Pair {idx+1}: {pa.name} → {pb.name} (synergy uplift: {uplift:+.2f}) ---')
        print(f'  Source seeking:     "{(pa.seeking or "")[:80]}"')
        print(f'  Target offering:    "{(pb.offering or "")[:80]}"')
        print(f'  Source who_serve:   "{(pa.who_you_serve or "")[:80]}"')
        print(f'  Target who_serve:   "{(pb.who_you_serve or "")[:80]}"')

        # Show word overlap details
        syn_a = ra['result']['breakdown_ab']['synergy']
        syn_b = rb['result']['breakdown_ab']['synergy']

        for f_wo, f_em in zip(syn_a['factors'], syn_b['factors']):
            if f_wo['name'] in ('Offering↔Seeking', 'Audience Alignment'):
                print(f'  {f_wo["name"]}:')
                print(f'    Word overlap: {f_wo["score"]:.1f}/10 ({f_wo["detail"]})')
                print(f'    Embedding:    {f_em["score"]:.1f}/10 ({f_em["detail"]})')

    if not best_uplift_pairs:
        print('  No pairs found with meaningful embedding uplift in this sample.')
        print('  Trying to show any 3 pairs with their embedding details...')
        for idx in range(min(3, len(results_b))):
            rb = results_b[idx]
            pa = profiles.get(str(matches[idx].profile_id))
            pb = profiles.get(str(matches[idx].suggested_profile_id))
            if not pa or not pb:
                continue
            print(f'\n  --- Pair {idx+1}: {pa.name} → {pb.name} ---')
            print(f'  Source seeking:     "{(pa.seeking or "")[:80]}"')
            print(f'  Target offering:    "{(pb.offering or "")[:80]}"')
            syn_b = rb['result']['breakdown_ab']['synergy']
            for f in syn_b['factors']:
                if f['name'] in ('Offering↔Seeking', 'Audience Alignment'):
                    print(f'  {f["name"]}: {f["score"]:.1f}/10 ({f["detail"]})')

    print()
    print('=' * 90)
    print('DONE')
    print('=' * 90)


if __name__ == '__main__':
    run_test()
