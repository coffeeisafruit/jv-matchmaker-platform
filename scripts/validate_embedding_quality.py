#!/usr/bin/env python3
"""
Embedding Quality Validation Benchmark

Pulls 500 random profile pairs from Supabase, scores each pair two ways:
  1. Current: _text_overlap_score() (word-level Jaccard from matching/services.py)
  2. Proposed: cosine similarity via sentence-transformers/all-MiniLM-L6-v2

Outputs:
  - CSV with per-pair scores for both methods
  - Summary report with distributions, rescued matches, correlation, top divergences

Usage:
    python scripts/validate_embedding_quality.py
    python scripts/validate_embedding_quality.py --pairs 200  # smaller run
    python scripts/validate_embedding_quality.py --dry-run     # use research cache, skip DB
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

# Django setup (same pattern as automated_enrichment_pipeline_safe.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Replicated _text_overlap_score() — exact copy from matching/services.py
#    lines 1421-1454, so benchmark uses the identical algorithm.
# ---------------------------------------------------------------------------

def text_overlap_score(text_a: str, text_b: str) -> float:
    """Score keyword overlap between two text fields (0-10).

    Exact replica of SupabaseMatchScoringService._text_overlap_score().
    """
    if not text_a or not text_b:
        return 3.0
    if not text_a.strip() or not text_b.strip():
        return 3.0

    words_a = set(re.findall(r'\b\w{4,}\b', text_a.lower()))
    words_b = set(re.findall(r'\b\w{4,}\b', text_b.lower()))

    stop = {'that', 'this', 'with', 'from', 'they', 'them', 'their',
            'have', 'been', 'were', 'will', 'would', 'could', 'should',
            'about', 'more', 'also', 'just', 'some', 'like', 'into',
            'other', 'what', 'your', 'help'}
    words_a -= stop
    words_b -= stop

    if not words_a or not words_b:
        return 3.0

    overlap = words_a & words_b
    smaller = min(len(words_a), len(words_b))
    ratio = len(overlap) / smaller if smaller > 0 else 0

    if ratio >= 0.4:
        return 10.0
    elif ratio >= 0.25:
        return 8.0
    elif ratio >= 0.15:
        return 6.0
    elif ratio >= 0.05:
        return 4.5
    else:
        return 3.0


# ---------------------------------------------------------------------------
# 2. Profile loading
# ---------------------------------------------------------------------------

def load_profiles_from_db(limit: int = 1000) -> list[dict]:
    """Load profiles from Supabase via psycopg2 (same pattern as pipeline)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        logger.error("DATABASE_URL not set. Use --dry-run to use research cache instead.")
        sys.exit(1)

    logger.info(f"Connecting to Supabase to fetch up to {limit} profiles...")
    conn = psycopg2.connect(dsn=db_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, company,
                       seeking, offering, who_you_serve, what_you_do,
                       niche, revenue_tier
                FROM profiles
                WHERE (seeking IS NOT NULL AND length(seeking) > 10)
                   OR (offering IS NOT NULL AND length(offering) > 10)
                   OR (who_you_serve IS NOT NULL AND length(who_you_serve) > 10)
                   OR (what_you_do IS NOT NULL AND length(what_you_do) > 10)
                ORDER BY random()
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            logger.info(f"Loaded {len(rows)} profiles with text fields")
            return [dict(r) for r in rows]
    finally:
        conn.close()


def load_profiles_from_cache(limit: int = 1000) -> list[dict]:
    """Load profiles from research cache JSON files (for --dry-run)."""
    cache_dir = Path(__file__).resolve().parent.parent / 'Chelsea_clients' / 'research_cache'
    if not cache_dir.exists():
        logger.error(f"Research cache not found at {cache_dir}")
        sys.exit(1)

    files = list(cache_dir.glob('*.json'))
    random.shuffle(files)
    profiles = []

    for f in files[:limit * 2]:  # Read extra to filter for sufficient text
        try:
            with open(f) as fp:
                data = json.load(fp)
            # Need at least one substantive text field
            has_text = any(
                isinstance(data.get(field), str) and len(data.get(field, '')) > 10
                for field in ('seeking', 'offering', 'who_you_serve', 'what_you_do')
            )
            if has_text:
                data.setdefault('id', f.stem)
                data.setdefault('name', data.get('name', f.stem))
                profiles.append(data)
                if len(profiles) >= limit:
                    break
        except (json.JSONDecodeError, OSError):
            continue

    logger.info(f"Loaded {len(profiles)} profiles from research cache")
    return profiles


# ---------------------------------------------------------------------------
# 3. Embedding similarity (using lib/enrichment/)
# ---------------------------------------------------------------------------

def init_embedding_service():
    """Initialize the HF embedding client."""
    # Add project root to path for lib/ import
    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root))

    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.embeddings import ProfileEmbeddingService

    client = HFClient()
    if not client.api_token:
        logger.info("HF_API_TOKEN not set — using local sentence-transformers model.")

    service = ProfileEmbeddingService(client)
    return client, service


def get_embedding_cached(hf_client, text: str, model: str = None) -> list[float] | None:
    """Get embedding with HFClient's built-in cache."""
    if not text or not text.strip() or len(text.strip()) < 5:
        return None
    return hf_client.embed(text.strip(), model=model)


# ---------------------------------------------------------------------------
# 4. Pair generation and scoring
# ---------------------------------------------------------------------------

def generate_pairs(profiles: list[dict], num_pairs: int) -> list[tuple[dict, dict]]:
    """Generate random profile pairs for comparison.

    Ensures each pair has at least one scorable field combination:
    - (seeking_a, offering_b) for offering↔seeking alignment
    - (who_you_serve_a, who_you_serve_b) for audience alignment
    """
    valid_pairs = []
    attempts = 0
    max_attempts = num_pairs * 10

    while len(valid_pairs) < num_pairs and attempts < max_attempts:
        a, b = random.sample(profiles, 2)

        # At least one scorable combination
        has_offering_seeking = (
            (a.get('seeking') and len(str(a['seeking'])) > 10) and
            (b.get('offering') or b.get('what_you_do'))
        )
        has_audience = (
            (a.get('who_you_serve') and len(str(a['who_you_serve'])) > 10) and
            (b.get('who_you_serve') and len(str(b['who_you_serve'])) > 10)
        )

        if has_offering_seeking or has_audience:
            valid_pairs.append((a, b))

        attempts += 1

    logger.info(f"Generated {len(valid_pairs)} valid pairs from {len(profiles)} profiles "
                f"({attempts} attempts)")
    return valid_pairs


def score_pair(hf_client, pair: tuple[dict, dict], model: str = None) -> dict:
    """Score a single pair using both methods."""
    a, b = pair

    seeking_a = str(a.get('seeking') or '')
    offering_b = str(b.get('offering') or b.get('what_you_do') or '')
    serve_a = str(a.get('who_you_serve') or '')
    serve_b = str(b.get('who_you_serve') or '')

    # Word overlap scores (current method)
    wo_offering_seeking = text_overlap_score(seeking_a, offering_b)
    wo_audience = text_overlap_score(serve_a, serve_b)

    # Embedding scores (proposed method)
    emb_seeking_a = get_embedding_cached(hf_client, seeking_a, model=model)
    emb_offering_b = get_embedding_cached(hf_client, offering_b, model=model)
    emb_serve_a = get_embedding_cached(hf_client, serve_a, model=model)
    emb_serve_b = get_embedding_cached(hf_client, serve_b, model=model)

    from lib.enrichment.embeddings import ProfileEmbeddingService
    cos_sim = ProfileEmbeddingService.cosine_similarity

    emb_offering_seeking = cos_sim(emb_seeking_a, emb_offering_b) if (emb_seeking_a and emb_offering_b) else None
    emb_audience = cos_sim(emb_serve_a, emb_serve_b) if (emb_serve_a and emb_serve_b) else None

    return {
        'profile_a_id': str(a.get('id', '')),
        'profile_a_name': a.get('name', ''),
        'profile_b_id': str(b.get('id', '')),
        'profile_b_name': b.get('name', ''),
        'seeking_a': seeking_a[:200],
        'offering_b': offering_b[:200],
        'who_you_serve_a': serve_a[:200],
        'who_you_serve_b': serve_b[:200],
        'word_overlap_offering_seeking': wo_offering_seeking,
        'embedding_sim_offering_seeking': round(emb_offering_seeking, 4) if emb_offering_seeking is not None else '',
        'word_overlap_audience': wo_audience,
        'embedding_sim_audience': round(emb_audience, 4) if emb_audience is not None else '',
    }


# ---------------------------------------------------------------------------
# 5. Analysis and reporting
# ---------------------------------------------------------------------------

def analyze_results(results: list[dict]) -> str:
    """Generate the summary report."""
    lines = []
    lines.append("=" * 72)
    lines.append("EMBEDDING QUALITY VALIDATION REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Total pairs scored: {len(results)}")
    lines.append("=" * 72)

    # --- Offering↔Seeking analysis ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 1: Offering↔Seeking Alignment")
    lines.append("-" * 72)

    wo_os = [r['word_overlap_offering_seeking'] for r in results]
    emb_os = [r['embedding_sim_offering_seeking'] for r in results
              if r['embedding_sim_offering_seeking'] != '']

    if wo_os:
        lines.append(f"\n  Word Overlap Scores (0-10 scale):")
        lines.append(f"    Mean:   {statistics.mean(wo_os):.2f}")
        lines.append(f"    Median: {statistics.median(wo_os):.2f}")
        lines.append(f"    StdDev: {statistics.stdev(wo_os):.2f}" if len(wo_os) > 1 else "    StdDev: N/A")
        q = statistics.quantiles(wo_os, n=4) if len(wo_os) >= 4 else []
        if q:
            lines.append(f"    Q1/Q2/Q3: {q[0]:.2f} / {q[1]:.2f} / {q[2]:.2f}")

        # Score distribution
        score_buckets = {3.0: 0, 4.5: 0, 6.0: 0, 8.0: 0, 10.0: 0}
        for s in wo_os:
            score_buckets[s] = score_buckets.get(s, 0) + 1
        lines.append(f"    Distribution: {dict(score_buckets)}")

    if emb_os:
        lines.append(f"\n  Embedding Cosine Similarity (0-1 scale):")
        lines.append(f"    Mean:   {statistics.mean(emb_os):.4f}")
        lines.append(f"    Median: {statistics.median(emb_os):.4f}")
        lines.append(f"    StdDev: {statistics.stdev(emb_os):.4f}" if len(emb_os) > 1 else "    StdDev: N/A")
        q = statistics.quantiles(emb_os, n=4) if len(emb_os) >= 4 else []
        if q:
            lines.append(f"    Q1/Q2/Q3: {q[0]:.4f} / {q[1]:.4f} / {q[2]:.4f}")

    # --- Audience analysis ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 2: Audience Alignment")
    lines.append("-" * 72)

    wo_aud = [r['word_overlap_audience'] for r in results]
    emb_aud = [r['embedding_sim_audience'] for r in results
               if r['embedding_sim_audience'] != '']

    if wo_aud:
        lines.append(f"\n  Word Overlap Scores (0-10 scale):")
        lines.append(f"    Mean:   {statistics.mean(wo_aud):.2f}")
        lines.append(f"    Median: {statistics.median(wo_aud):.2f}")
        lines.append(f"    StdDev: {statistics.stdev(wo_aud):.2f}" if len(wo_aud) > 1 else "    StdDev: N/A")
        q = statistics.quantiles(wo_aud, n=4) if len(wo_aud) >= 4 else []
        if q:
            lines.append(f"    Q1/Q2/Q3: {q[0]:.2f} / {q[1]:.2f} / {q[2]:.2f}")

        score_buckets = {3.0: 0, 4.5: 0, 6.0: 0, 8.0: 0, 10.0: 0}
        for s in wo_aud:
            score_buckets[s] = score_buckets.get(s, 0) + 1
        lines.append(f"    Distribution: {dict(score_buckets)}")

    if emb_aud:
        lines.append(f"\n  Embedding Cosine Similarity (0-1 scale):")
        lines.append(f"    Mean:   {statistics.mean(emb_aud):.4f}")
        lines.append(f"    Median: {statistics.median(emb_aud):.4f}")
        lines.append(f"    StdDev: {statistics.stdev(emb_aud):.4f}" if len(emb_aud) > 1 else "    StdDev: N/A")
        q = statistics.quantiles(emb_aud, n=4) if len(emb_aud) >= 4 else []
        if q:
            lines.append(f"    Q1/Q2/Q3: {q[0]:.4f} / {q[1]:.4f} / {q[2]:.4f}")

    # --- Rescued matches ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 3: 'Rescued' Matches")
    lines.append("  (Word overlap = 3.0 [no signal] but embedding sim > 0.5)")
    lines.append("-" * 72)

    rescued_os = [
        r for r in results
        if r['word_overlap_offering_seeking'] == 3.0
        and r['embedding_sim_offering_seeking'] != ''
        and r['embedding_sim_offering_seeking'] > 0.5
    ]
    rescued_aud = [
        r for r in results
        if r['word_overlap_audience'] == 3.0
        and r['embedding_sim_audience'] != ''
        and r['embedding_sim_audience'] > 0.5
    ]

    total_wo3_os = sum(1 for r in results if r['word_overlap_offering_seeking'] == 3.0)
    total_wo3_aud = sum(1 for r in results if r['word_overlap_audience'] == 3.0)

    lines.append(f"\n  Offering↔Seeking:")
    lines.append(f"    Pairs scoring 3.0 (no signal) in word overlap: {total_wo3_os}")
    lines.append(f"    Of those, embedding finds similarity > 0.5: {len(rescued_os)}")
    if total_wo3_os > 0:
        lines.append(f"    Rescue rate: {len(rescued_os)/total_wo3_os*100:.1f}%")

    lines.append(f"\n  Audience Alignment:")
    lines.append(f"    Pairs scoring 3.0 (no signal) in word overlap: {total_wo3_aud}")
    lines.append(f"    Of those, embedding finds similarity > 0.5: {len(rescued_aud)}")
    if total_wo3_aud > 0:
        lines.append(f"    Rescue rate: {len(rescued_aud)/total_wo3_aud*100:.1f}%")

    # --- Correlation ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 4: Correlation Between Methods")
    lines.append("-" * 72)

    # Offering↔Seeking correlation
    paired_os = [
        (r['word_overlap_offering_seeking'], r['embedding_sim_offering_seeking'])
        for r in results
        if r['embedding_sim_offering_seeking'] != ''
    ]
    if len(paired_os) >= 10:
        corr = _pearson_correlation(
            [p[0] for p in paired_os],
            [p[1] for p in paired_os],
        )
        lines.append(f"\n  Offering↔Seeking: Pearson r = {corr:.4f}  (n={len(paired_os)})")
        if corr > 0.7:
            lines.append("    Interpretation: Strong positive — methods largely agree")
        elif corr > 0.4:
            lines.append("    Interpretation: Moderate — embeddings capture additional signal")
        else:
            lines.append("    Interpretation: Weak/no correlation — methods measure different things")

    paired_aud = [
        (r['word_overlap_audience'], r['embedding_sim_audience'])
        for r in results
        if r['embedding_sim_audience'] != ''
    ]
    if len(paired_aud) >= 10:
        corr = _pearson_correlation(
            [p[0] for p in paired_aud],
            [p[1] for p in paired_aud],
        )
        lines.append(f"  Audience Alignment: Pearson r = {corr:.4f}  (n={len(paired_aud)})")

    # --- Top 20: High embedding, low word overlap (strongest evidence) ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 5: Top 20 — High Embedding Sim, Low Word Overlap")
    lines.append("  (Strongest evidence: pairs embeddings find that word overlap misses)")
    lines.append("-" * 72)

    # Compute divergence score: embedding_sim - normalized_word_overlap
    divergent = []
    for r in results:
        emb_val = r['embedding_sim_offering_seeking']
        if emb_val == '':
            continue
        wo_normalized = r['word_overlap_offering_seeking'] / 10.0
        divergence = emb_val - wo_normalized
        divergent.append((divergence, r))

    divergent.sort(key=lambda x: x[0], reverse=True)
    for i, (div, r) in enumerate(divergent[:20]):
        lines.append(f"\n  #{i+1} (divergence: {div:+.3f})")
        lines.append(f"    A: {r['profile_a_name']}")
        lines.append(f"    B: {r['profile_b_name']}")
        lines.append(f"    Seeking A: {r['seeking_a'][:120]}...")
        lines.append(f"    Offering B: {r['offering_b'][:120]}...")
        lines.append(f"    Word overlap: {r['word_overlap_offering_seeking']:.1f}/10  |  "
                      f"Embedding sim: {r['embedding_sim_offering_seeking']:.4f}")

    # --- Top 20: Both methods agree on high score (sanity check) ---
    lines.append("\n" + "-" * 72)
    lines.append("SECTION 6: Top 20 — Both Methods Agree (Sanity Check)")
    lines.append("  (Pairs where word overlap >= 6.0 AND embedding sim >= 0.6)")
    lines.append("-" * 72)

    agreed = [
        r for r in results
        if r['word_overlap_offering_seeking'] >= 6.0
        and r['embedding_sim_offering_seeking'] != ''
        and r['embedding_sim_offering_seeking'] >= 0.6
    ]
    agreed.sort(key=lambda r: r['embedding_sim_offering_seeking'], reverse=True)
    for i, r in enumerate(agreed[:20]):
        lines.append(f"\n  #{i+1}")
        lines.append(f"    A: {r['profile_a_name']}  |  B: {r['profile_b_name']}")
        lines.append(f"    Seeking A: {r['seeking_a'][:100]}...")
        lines.append(f"    Offering B: {r['offering_b'][:100]}...")
        lines.append(f"    Word overlap: {r['word_overlap_offering_seeking']:.1f}/10  |  "
                      f"Embedding sim: {r['embedding_sim_offering_seeking']:.4f}")

    if not agreed:
        lines.append("\n  No pairs found where both methods agree on high scores.")

    lines.append("\n" + "=" * 72)
    lines.append("END OF REPORT")
    lines.append("=" * 72)

    return '\n'.join(lines)


def _pearson_correlation(x: list[float], y: list[float]) -> float:
    """Compute Pearson correlation coefficient (no numpy dependency)."""
    n = len(x)
    if n < 2:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    std_x = (sum((xi - mean_x) ** 2 for xi in x)) ** 0.5
    std_y = (sum((yi - mean_y) ** 2 for yi in y)) ** 0.5
    if std_x == 0 or std_y == 0:
        return 0.0
    return cov / (std_x * std_y)


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Validate embedding quality vs word overlap')
    parser.add_argument('--pairs', type=int, default=500, help='Number of profile pairs to score')
    parser.add_argument('--dry-run', action='store_true', help='Use research cache instead of DB')
    parser.add_argument('--profile-limit', type=int, default=1000,
                        help='Max profiles to load (pairs sampled from these)')
    parser.add_argument('--model', type=str, default=None,
                        help='Embedding model (default: sentence-transformers/all-MiniLM-L6-v2)')
    args = parser.parse_args()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(__file__).resolve().parent.parent / 'validation_results'
    output_dir.mkdir(exist_ok=True)

    # Load profiles
    if args.dry_run:
        profiles = load_profiles_from_cache(limit=args.profile_limit)
    else:
        profiles = load_profiles_from_db(limit=args.profile_limit)

    if len(profiles) < 20:
        logger.error(f"Only {len(profiles)} profiles found — need at least 20 for meaningful pairs")
        sys.exit(1)

    # Initialize embedding service
    hf_client, emb_service = init_embedding_service()

    # Generate pairs
    pairs = generate_pairs(profiles, num_pairs=args.pairs)

    # Score all pairs
    model_label = args.model or 'sentence-transformers/all-MiniLM-L6-v2'
    logger.info(f"Scoring {len(pairs)} pairs with model: {model_label}")
    results = []
    for i, pair in enumerate(pairs):
        result = score_pair(hf_client, pair, model=args.model)
        results.append(result)

        if (i + 1) % 50 == 0:
            from lib.enrichment.hf_client import get_metrics
            m = get_metrics()
            logger.info(f"Progress: {i+1}/{len(pairs)} pairs scored  "
                         f"[API calls: {m['api_calls']}, cache hits: {m['cache_hits']}]")

    # Export CSV
    csv_path = output_dir / f'embedding_benchmark_{timestamp}.csv'
    fieldnames = [
        'profile_a_id', 'profile_a_name', 'profile_b_id', 'profile_b_name',
        'seeking_a', 'offering_b', 'who_you_serve_a', 'who_you_serve_b',
        'word_overlap_offering_seeking', 'embedding_sim_offering_seeking',
        'word_overlap_audience', 'embedding_sim_audience',
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    logger.info(f"CSV exported: {csv_path}")

    # Generate and save report
    report = analyze_results(results)
    report_path = output_dir / f'embedding_benchmark_report_{timestamp}.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"Report saved: {report_path}")

    # Print report to stdout
    print("\n")
    print(report)

    # Print HF metrics
    from lib.enrichment.hf_client import get_metrics
    m = get_metrics()
    print(f"\nHF API Metrics: {m['api_calls']} calls, {m['cache_hits']} cache hits, "
          f"{m['api_errors']} errors, {m['total_latency_ms']:.0f}ms total latency")


if __name__ == '__main__':
    main()
