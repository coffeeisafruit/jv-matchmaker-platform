#!/usr/bin/env python3
"""
Synonym Stress Test for Embedding vs Word Overlap

Tests 30 hardcoded synonym pairs representing the same business concept
in different vocabulary. Pairs are sourced from actual JV MatchMaker
profile data (Chelsea_clients/research_cache/).

Measures what percentage of synonym pairs embeddings correctly identify
as similar (>0.6) that word overlap misses (<5.0 score).

Usage:
    python scripts/synonym_stress_test.py
    python scripts/synonym_stress_test.py --threshold 0.55  # adjust embedding threshold
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from dotenv import load_dotenv
load_dotenv()

# Project root for lib/ imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))


# ---------------------------------------------------------------------------
# Replicated _text_overlap_score() — exact copy from matching/services.py
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
# Synonym pairs — sourced from actual profile vocabulary in research_cache/
#
# Each pair represents the SAME business concept, audience, or service
# described using DIFFERENT vocabulary. These are the cases where word
# overlap fails because it looks for shared 4+ letter words and finds none.
#
# Format: (category, text_a, text_b, notes)
# ---------------------------------------------------------------------------

SYNONYM_PAIRS = [
    # --- Audience: Entrepreneurs / Business Owners ---
    (
        "audience",
        "entrepreneurs and business owners globally",
        "visionary founders, creatives, and startup leaders",
        "From Jane Warr vs Ashley Dyer profiles — same market, zero word overlap"
    ),
    (
        "audience",
        "coaches, consultants, and experts looking to increase visibility",
        "online thought leaders seeking to grow their authority and reach",
        "Henri Schauffler style vs generic authority-building framing"
    ),
    (
        "audience",
        "six-figure-plus business owners seeking better revenue systems",
        "established companies looking to scale from mid-six to seven figures",
        "Claudia Brown vocabulary vs generic scaling language"
    ),
    (
        "audience",
        "women entrepreneurs and business owners",
        "female founders building purpose-driven companies",
        "eWomenNetwork literal vs aspirational framing"
    ),

    # --- Audience: Personal Development Seekers ---
    (
        "audience",
        "individuals seeking personal growth and spiritual guidance",
        "people on a journey of self-discovery and inner transformation",
        "Kelle Sparta / Irina Portnova type — seeker market, different words"
    ),
    (
        "audience",
        "leaders and changemakers looking to align with purpose",
        "high achievers seeking clarity, meaning, and fulfillment",
        "Baylan Megino vs generic executive coaching audience"
    ),
    (
        "audience",
        "couples seeking to deepen connection and address conflicts",
        "partners struggling with communication who want lasting intimacy",
        "Transformative Loving vs Mary Kerwin couples vocabulary"
    ),

    # --- Offering: Coaching Types ---
    (
        "offering",
        "transformational coaching and shamanic healing",
        "deep personal change work through energy and consciousness practices",
        "Kelle Sparta exact offering vs reframed description"
    ),
    (
        "offering",
        "executive coaching for senior leaders",
        "C-suite mentorship and leadership development",
        "Helena Demuynck style — same service, prestige register shift"
    ),
    (
        "offering",
        "business mastery coaching to scale from six to seven figures",
        "revenue growth strategy and operational scaling for online businesses",
        "Claudia Brown coaching vs consulting framing"
    ),
    (
        "offering",
        "manifestation coaching and goal achievement",
        "mindset reprogramming to attract desired outcomes",
        "Irina Portnova vs NLP-style language for same transformation"
    ),
    (
        "offering",
        "Neuro Change Method certification and brain-based coaching",
        "neuroscience-backed techniques for subconscious transformation",
        "Pamela Cowan exact vs generic description"
    ),

    # --- Offering: Speaking / Podcasting ---
    (
        "offering",
        "speaker training for entrepreneurs and content creators",
        "public speaking coaching and stage presence development",
        "Danielle Benzon vs generic speaking trainer"
    ),
    (
        "offering",
        "podcast guesting services to grow your business",
        "getting featured on shows to attract clients and build authority",
        "Henri Schauffler offering vs outcome-based framing"
    ),
    (
        "offering",
        "helping clients launch profitable podcasts",
        "audio content creation and show production for thought leaders",
        "Christine Blosdale vs media production framing"
    ),

    # --- Offering: Health / Wellness ---
    (
        "offering",
        "health coaching and holistic wellness programs",
        "integrative wellness consulting for mind-body balance",
        "Susan Nordemo vs functional medicine language"
    ),
    (
        "offering",
        "plantar fasciitis relief and foot pain solutions",
        "movement rehabilitation to restore active lifestyle",
        "Heather Muhlhauser specific vs rehab generalization"
    ),

    # --- Seeking: JV Partnerships ---
    (
        "seeking",
        "guest speakers for our writing mastery summit",
        "presenters and subject matter experts for our virtual event series",
        "Paula Judith Johnson seeking vs generic event organizer language"
    ),
    (
        "seeking",
        "authors, speakers, and coaches for referral partnerships",
        "thought leaders interested in mutual cross-promotion opportunities",
        "Jennifer Taylor explicit vs JV community jargon"
    ),
    (
        "seeking",
        "podcast guests who are public figures and industry leaders",
        "accomplished professionals to interview on our show",
        "STRIVECast vs generic podcast booker language"
    ),

    # --- Niche: Spiritual / Transformation ---
    (
        "niche",
        "spiritual awakening and energy work",
        "consciousness expansion and holistic healing",
        "Kelle Sparta niche vs new-age umbrella terms"
    ),
    (
        "niche",
        "manifestation and self-discovery",
        "law of attraction and personal empowerment",
        "Irina Portnova vs classic LOA community vocabulary"
    ),

    # --- Niche: Business / Leadership ---
    (
        "niche",
        "leadership development and executive coaching",
        "organizational effectiveness and talent cultivation",
        "Remarkable Leadership Podcast vs corporate HR vocabulary"
    ),
    (
        "niche",
        "closing the gender leadership gap",
        "women's advancement in corporate and professional settings",
        "Bossed Up mission framing vs DEI language"
    ),

    # --- Niche: Finance ---
    (
        "niche",
        "passive income through rental property investing",
        "building long-term wealth with real estate portfolios",
        "Rental Income Podcast vs wealth advisor language"
    ),
    (
        "niche",
        "legacy planning and conscious wealth transfer",
        "estate strategy and generational financial stewardship",
        "Glenn Head spiritual-financial vs traditional estate planning"
    ),

    # --- Cross-category: Same concept, radically different words ---
    (
        "cross",
        "helping churches with financial management and stewardship",
        "bookkeeping and accounting services for nonprofit organizations",
        "Same service market, completely different vocabulary"
    ),
    (
        "cross",
        "grief support and end-of-life coaching",
        "bereavement counseling and palliative care guidance",
        "Lee Atherton vocabulary vs clinical/hospice vocabulary"
    ),
    (
        "cross",
        "communication training and sales enablement",
        "persuasion skills and revenue conversation coaching",
        "Katherine Minett style corporate vs sales trainer framing"
    ),
    (
        "cross",
        "branding design and high-converting sales funnels",
        "visual identity and digital marketing systems for businesses",
        "Ashley Dyer exact vs marketing agency generic"
    ),
]


# ---------------------------------------------------------------------------
# Scoring and reporting
# ---------------------------------------------------------------------------

def run_stress_test(embedding_threshold: float = 0.6, word_overlap_miss_threshold: float = 5.0):
    """Run the synonym stress test and return results."""
    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.embeddings import ProfileEmbeddingService

    client = HFClient()
    if not client.api_token:
        print("NOTE: HF_API_TOKEN not set — using local sentence-transformers model.")

    cos_sim = ProfileEmbeddingService.cosine_similarity
    results = []

    print(f"\nScoring {len(SYNONYM_PAIRS)} synonym pairs...\n")

    for i, (category, text_a, text_b, notes) in enumerate(SYNONYM_PAIRS):
        # Word overlap
        wo_score = text_overlap_score(text_a, text_b)

        # Embedding similarity
        emb_a = client.embed(text_a)
        emb_b = client.embed(text_b)
        emb_sim = cos_sim(emb_a, emb_b) if (emb_a and emb_b) else 0.0

        # Did embedding "rescue" this pair?
        wo_misses = wo_score < word_overlap_miss_threshold
        emb_catches = emb_sim >= embedding_threshold
        rescued = wo_misses and emb_catches

        results.append({
            'pair_num': i + 1,
            'category': category,
            'text_a': text_a,
            'text_b': text_b,
            'notes': notes,
            'word_overlap': wo_score,
            'embedding_sim': round(emb_sim, 4),
            'wo_misses': wo_misses,
            'emb_catches': emb_catches,
            'rescued': rescued,
        })

        # Show progress with inline result
        status = "RESCUED" if rescued else ("BOTH OK" if not wo_misses else "BOTH MISS")
        print(f"  [{i+1:2d}/{len(SYNONYM_PAIRS)}] WO={wo_score:4.1f}  EMB={emb_sim:.4f}  "
              f"{status:10s}  {category}")

    return results


def generate_report(results: list[dict], emb_threshold: float, wo_threshold: float) -> str:
    """Generate the summary report."""
    lines = []
    lines.append("=" * 76)
    lines.append("SYNONYM STRESS TEST REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Pairs tested: {len(results)}")
    lines.append(f"Embedding similarity threshold: {emb_threshold}")
    lines.append(f"Word overlap 'miss' threshold: < {wo_threshold}")
    lines.append("=" * 76)

    # --- Overall stats ---
    rescued = [r for r in results if r['rescued']]
    wo_misses = [r for r in results if r['wo_misses']]
    emb_catches = [r for r in results if r['emb_catches']]
    both_miss = [r for r in results if r['wo_misses'] and not r['emb_catches']]

    lines.append(f"\n  SUMMARY")
    lines.append(f"  -------")
    lines.append(f"  Word overlap misses (score < {wo_threshold}):  {len(wo_misses)}/{len(results)}  "
                  f"({len(wo_misses)/len(results)*100:.0f}%)")
    lines.append(f"  Embedding catches (sim >= {emb_threshold}):    {len(emb_catches)}/{len(results)}  "
                  f"({len(emb_catches)/len(results)*100:.0f}%)")
    lines.append(f"  RESCUED by embeddings:                {len(rescued)}/{len(results)}  "
                  f"({len(rescued)/len(results)*100:.0f}%)")
    lines.append(f"  Both methods miss:                    {len(both_miss)}/{len(results)}  "
                  f"({len(both_miss)/len(results)*100:.0f}%)")

    if wo_misses:
        rescue_rate = len(rescued) / len(wo_misses) * 100
        lines.append(f"\n  >>> Of pairs word overlap MISSES, embeddings rescue {rescue_rate:.0f}% <<<")

    # --- Comparison table ---
    lines.append(f"\n{'─' * 76}")
    lines.append(f"  FULL COMPARISON TABLE")
    lines.append(f"{'─' * 76}")
    lines.append(f"  {'#':>3}  {'Cat':8}  {'WO':>5}  {'EMB':>7}  {'Status':10}  Text A (truncated)")
    lines.append(f"  {'─'*3}  {'─'*8}  {'─'*5}  {'─'*7}  {'─'*10}  {'─'*30}")

    for r in results:
        status = "RESCUED" if r['rescued'] else ("BOTH OK" if not r['wo_misses'] else "BOTH MISS")
        lines.append(
            f"  {r['pair_num']:3d}  {r['category']:8s}  "
            f"{r['word_overlap']:5.1f}  {r['embedding_sim']:7.4f}  "
            f"{status:10s}  {r['text_a'][:40]}..."
        )

    # --- Rescued pairs (detailed) ---
    lines.append(f"\n{'─' * 76}")
    lines.append(f"  RESCUED PAIRS — Full Detail")
    lines.append(f"  (Word overlap missed, embeddings caught)")
    lines.append(f"{'─' * 76}")

    if not rescued:
        lines.append("\n  No pairs were rescued. Embedding threshold may be too high.")
    else:
        for r in rescued:
            lines.append(f"\n  Pair #{r['pair_num']} [{r['category']}]")
            lines.append(f"    A: \"{r['text_a']}\"")
            lines.append(f"    B: \"{r['text_b']}\"")
            lines.append(f"    Word overlap: {r['word_overlap']:.1f}/10  |  Embedding: {r['embedding_sim']:.4f}")
            lines.append(f"    Notes: {r['notes']}")

    # --- Both miss (concerning) ---
    if both_miss:
        lines.append(f"\n{'─' * 76}")
        lines.append(f"  BOTH METHODS MISS — Potential Issues")
        lines.append(f"{'─' * 76}")
        for r in both_miss:
            lines.append(f"\n  Pair #{r['pair_num']} [{r['category']}]")
            lines.append(f"    A: \"{r['text_a']}\"")
            lines.append(f"    B: \"{r['text_b']}\"")
            lines.append(f"    Word overlap: {r['word_overlap']:.1f}  |  Embedding: {r['embedding_sim']:.4f}")
            lines.append(f"    Notes: {r['notes']}")

    # --- Score distributions ---
    lines.append(f"\n{'─' * 76}")
    lines.append(f"  SCORE DISTRIBUTIONS")
    lines.append(f"{'─' * 76}")

    import statistics
    wo_scores = [r['word_overlap'] for r in results]
    emb_scores = [r['embedding_sim'] for r in results]

    lines.append(f"\n  Word Overlap (0-10 scale):")
    lines.append(f"    Mean: {statistics.mean(wo_scores):.2f}  |  Median: {statistics.median(wo_scores):.2f}")
    wo_buckets = {}
    for s in wo_scores:
        wo_buckets[s] = wo_buckets.get(s, 0) + 1
    lines.append(f"    Distribution: {dict(sorted(wo_buckets.items()))}")

    lines.append(f"\n  Embedding Similarity (0-1 scale):")
    lines.append(f"    Mean: {statistics.mean(emb_scores):.4f}  |  Median: {statistics.median(emb_scores):.4f}")
    lines.append(f"    Min: {min(emb_scores):.4f}  |  Max: {max(emb_scores):.4f}")

    lines.append(f"\n{'=' * 76}")
    lines.append(f"END OF REPORT")
    lines.append(f"{'=' * 76}")

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Synonym stress test: embedding vs word overlap')
    parser.add_argument('--threshold', type=float, default=0.6,
                        help='Embedding similarity threshold to count as "catch" (default: 0.6)')
    parser.add_argument('--wo-threshold', type=float, default=5.0,
                        help='Word overlap score below which is a "miss" (default: 5.0)')
    args = parser.parse_args()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = project_root / 'validation_results'
    output_dir.mkdir(exist_ok=True)

    # Run the test
    results = run_stress_test(
        embedding_threshold=args.threshold,
        word_overlap_miss_threshold=args.wo_threshold,
    )

    # Generate report
    report = generate_report(results, args.threshold, args.wo_threshold)

    # Save CSV
    csv_path = output_dir / f'synonym_stress_test_{timestamp}.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'pair_num', 'category', 'text_a', 'text_b', 'notes',
            'word_overlap', 'embedding_sim', 'wo_misses', 'emb_catches', 'rescued',
        ])
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV saved: {csv_path}")

    # Save report
    report_path = output_dir / f'synonym_stress_test_report_{timestamp}.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"Report saved: {report_path}")

    # Print report
    print("\n")
    print(report)

    # Final HF metrics
    from lib.enrichment.hf_client import get_metrics
    m = get_metrics()
    print(f"\nHF API: {m['api_calls']} calls, {m['cache_hits']} cache hits, "
          f"{m['api_errors']} errors")


if __name__ == '__main__':
    main()
