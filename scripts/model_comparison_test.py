#!/usr/bin/env python3
"""
Multi-Model Embedding Comparison on Synonym Pairs

Runs the same 30 synonym pairs through three embedding models and compares
rescue rates, mean similarity, and score distributions.

Models tested:
  1. sentence-transformers/all-MiniLM-L6-v2 (384-dim, fast, lightweight)
  2. BAAI/bge-large-en-v1.5 (1024-dim, top MTEB retrieval model)
  3. OpenAI text-embedding-3-large (3072-dim, commercial API)

Usage:
    python scripts/model_comparison_test.py
"""

import csv
import math
import os
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

# Django/project setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from dotenv import load_dotenv
load_dotenv()

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Import synonym pairs from existing test
from scripts.synonym_stress_test import SYNONYM_PAIRS, text_overlap_score


# ---------------------------------------------------------------------------
# Embedding backends
# ---------------------------------------------------------------------------

def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Cosine similarity, clamped to [0, 1]."""
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return max(0.0, min(1.0, dot / (norm_a * norm_b)))


def embed_with_hf_local(texts: list[str], model_name: str) -> list[list[float]]:
    """Embed using local sentence-transformers."""
    from sentence_transformers import SentenceTransformer
    print(f"  Loading {model_name}...")
    model = SentenceTransformer(model_name)
    print(f"  Encoding {len(texts)} texts...")
    embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
    return [e.tolist() for e in embeddings]


def embed_with_openai(texts: list[str], model_name: str = "text-embedding-3-large") -> list[list[float]]:
    """Embed using OpenAI API."""
    from openai import OpenAI

    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    client = OpenAI(api_key=api_key)
    print(f"  Calling OpenAI {model_name} for {len(texts)} texts...")
    response = client.embeddings.create(input=texts, model=model_name)
    return [item.embedding for item in response.data]


# ---------------------------------------------------------------------------
# Model definitions
# ---------------------------------------------------------------------------

MODELS = [
    {
        'name': 'MiniLM-L6-v2',
        'full_name': 'sentence-transformers/all-MiniLM-L6-v2',
        'dim': 384,
        'backend': 'local',
        'embed_fn': lambda texts: embed_with_hf_local(texts, 'sentence-transformers/all-MiniLM-L6-v2'),
    },
    {
        'name': 'bge-large-en-v1.5',
        'full_name': 'BAAI/bge-large-en-v1.5',
        'dim': 1024,
        'backend': 'local',
        'embed_fn': lambda texts: embed_with_hf_local(texts, 'BAAI/bge-large-en-v1.5'),
    },
    {
        'name': 'text-embedding-3-large',
        'full_name': 'openai/text-embedding-3-large',
        'dim': 3072,
        'backend': 'openai',
        'embed_fn': lambda texts: embed_with_openai(texts, 'text-embedding-3-large'),
    },
]


# ---------------------------------------------------------------------------
# Run comparison
# ---------------------------------------------------------------------------

def run_comparison():
    """Run all models on all synonym pairs and collect results."""
    # Collect all unique texts for batch embedding
    all_texts = []
    for _, text_a, text_b, _ in SYNONYM_PAIRS:
        all_texts.append(text_a)
        all_texts.append(text_b)

    # Pre-compute word overlap (same for all models)
    wo_scores = []
    for _, text_a, text_b, _ in SYNONYM_PAIRS:
        wo_scores.append(text_overlap_score(text_a, text_b))

    model_results = {}

    for model_info in MODELS:
        name = model_info['name']
        print(f"\n{'='*60}")
        print(f"Model: {name} ({model_info['dim']}-dim, {model_info['backend']})")
        print(f"{'='*60}")

        try:
            embeddings = model_info['embed_fn'](all_texts)

            # Score each pair
            similarities = []
            for i in range(len(SYNONYM_PAIRS)):
                emb_a = embeddings[i * 2]
                emb_b = embeddings[i * 2 + 1]
                sim = cosine_similarity(emb_a, emb_b)
                similarities.append(round(sim, 4))

            model_results[name] = {
                'similarities': similarities,
                'dim': model_info['dim'],
                'backend': model_info['backend'],
            }
            print(f"  Done. Mean similarity: {statistics.mean(similarities):.4f}")

        except Exception as e:
            print(f"  FAILED: {e}")
            model_results[name] = None

    return wo_scores, model_results


def print_comparison_table(wo_scores: list[float], model_results: dict):
    """Print side-by-side comparison."""
    active_models = {k: v for k, v in model_results.items() if v is not None}

    print(f"\n\n{'='*90}")
    print("MULTI-MODEL EMBEDDING COMPARISON — SYNONYM STRESS TEST")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Pairs tested: {len(SYNONYM_PAIRS)}")
    print(f"{'='*90}")

    # --- Summary statistics ---
    print(f"\n{'─'*90}")
    print(f"  MODEL SUMMARY")
    print(f"{'─'*90}")
    header = f"  {'Metric':<35}"
    for name in active_models:
        header += f"  {name:>20}"
    print(header)
    print(f"  {'─'*35}" + f"  {'─'*20}" * len(active_models))

    # Mean
    row = f"  {'Mean similarity':<35}"
    for name, data in active_models.items():
        row += f"  {statistics.mean(data['similarities']):>20.4f}"
    print(row)

    # Median
    row = f"  {'Median similarity':<35}"
    for name, data in active_models.items():
        row += f"  {statistics.median(data['similarities']):>20.4f}"
    print(row)

    # Std dev
    row = f"  {'Std dev':<35}"
    for name, data in active_models.items():
        row += f"  {statistics.stdev(data['similarities']):>20.4f}"
    print(row)

    # Min/Max
    row = f"  {'Min':<35}"
    for name, data in active_models.items():
        row += f"  {min(data['similarities']):>20.4f}"
    print(row)
    row = f"  {'Max':<35}"
    for name, data in active_models.items():
        row += f"  {max(data['similarities']):>20.4f}"
    print(row)

    # Rescue rates at different thresholds
    for threshold in [0.5, 0.55, 0.6, 0.65, 0.7]:
        row = f"  {'Rescued at >=' + str(threshold):<35}"
        for name, data in active_models.items():
            count = sum(1 for i, s in enumerate(data['similarities'])
                        if wo_scores[i] < 5.0 and s >= threshold)
            total_misses = sum(1 for s in wo_scores if s < 5.0)
            pct = count / total_misses * 100 if total_misses > 0 else 0
            row += f"  {f'{count}/{total_misses} ({pct:.0f}%)':>20}"
        print(row)

    # --- Per-pair detail ---
    print(f"\n{'─'*90}")
    print(f"  PER-PAIR SCORES")
    print(f"{'─'*90}")

    # Header
    header = f"  {'#':>3}  {'Cat':<8}  {'WO':>4}"
    for name in active_models:
        header += f"  {name:>12}"
    header += f"  {'Best':>12}  Text A (truncated)"
    print(header)
    print(f"  {'─'*3}  {'─'*8}  {'─'*4}" + f"  {'─'*12}" * len(active_models) + f"  {'─'*12}  {'─'*30}")

    for i, (category, text_a, text_b, notes) in enumerate(SYNONYM_PAIRS):
        wo = wo_scores[i]

        # Collect scores for this pair
        pair_scores = {}
        for name, data in active_models.items():
            pair_scores[name] = data['similarities'][i]

        # Find best model for this pair
        best_name = max(pair_scores, key=pair_scores.get) if pair_scores else ''
        best_score = pair_scores.get(best_name, 0)

        row = f"  {i+1:3d}  {category:<8}  {wo:4.1f}"
        for name in active_models:
            score = pair_scores.get(name, 0)
            marker = ' *' if name == best_name else '  '
            row += f"  {score:10.4f}{marker}"
        row += f"  {best_name:>12}  {text_a[:35]}..."
        print(row)

    # --- Category breakdown ---
    print(f"\n{'─'*90}")
    print(f"  MEAN SIMILARITY BY CATEGORY")
    print(f"{'─'*90}")

    categories = sorted(set(cat for cat, _, _, _ in SYNONYM_PAIRS))
    header = f"  {'Category':<12}"
    for name in active_models:
        header += f"  {name:>20}"
    print(header)
    print(f"  {'─'*12}" + f"  {'─'*20}" * len(active_models))

    for cat in categories:
        indices = [i for i, (c, _, _, _) in enumerate(SYNONYM_PAIRS) if c == cat]
        row = f"  {cat:<12}"
        for name, data in active_models.items():
            cat_sims = [data['similarities'][i] for i in indices]
            row += f"  {statistics.mean(cat_sims):>20.4f}"
        print(row)

    print(f"\n{'='*90}")
    print(f"END OF COMPARISON")
    print(f"{'='*90}")


def save_csv(wo_scores: list[float], model_results: dict):
    """Save detailed CSV."""
    active_models = {k: v for k, v in model_results.items() if v is not None}
    output_dir = project_root / 'validation_results'
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = output_dir / f'model_comparison_{timestamp}.csv'

    fieldnames = ['pair_num', 'category', 'text_a', 'text_b', 'notes', 'word_overlap']
    for name in active_models:
        fieldnames.append(f'emb_{name}')

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, (category, text_a, text_b, notes) in enumerate(SYNONYM_PAIRS):
            row = {
                'pair_num': i + 1,
                'category': category,
                'text_a': text_a,
                'text_b': text_b,
                'notes': notes,
                'word_overlap': wo_scores[i],
            }
            for name, data in active_models.items():
                row[f'emb_{name}'] = data['similarities'][i]
            writer.writerow(row)

    print(f"\nCSV saved: {csv_path}")
    return csv_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    wo_scores, model_results = run_comparison()
    print_comparison_table(wo_scores, model_results)
    save_csv(wo_scores, model_results)
