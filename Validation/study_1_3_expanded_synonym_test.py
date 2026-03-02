#!/usr/bin/env python3
"""
Study 1.3: Expanded Synonym / Embedding Discrimination Test

Expands the ISMC embedding validation from 30 synonym pairs to 200+
and tests whether bge-large-en-v1.5 adequately discriminates
JV-relevant semantic similarity.

Steps:
  1. Extract production vocabulary from SupabaseProfile
  2. Compute fresh embeddings via sentence-transformers
  3. Automatically discover synonym pairs via clustering
  4. Curate adversarial pairs (similar words, different JV meaning)
  5. Generate random negative pairs
  6. Compute AUC-PR, optimal F1, discrimination gap, per-category breakdown

Outputs:
  Validation/study_1_3_expanded_synonym_test.md   (summary report)
  Validation/study_1_3_test_pairs.csv              (all pairs + scores + labels)
"""

import csv
import json
import math
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

from matching.models import SupabaseProfile  # noqa: E402

# ---------------------------------------------------------------------------
# sentence-transformers for fresh embeddings
# ---------------------------------------------------------------------------
from sentence_transformers import SentenceTransformer  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MODEL_NAME = "BAAI/bge-large-en-v1.5"
FIELDS = ["seeking", "offering", "who_you_serve", "what_you_do"]
EMBEDDING_FIELDS = [
    "embedding_seeking",
    "embedding_offering",
    "embedding_who_you_serve",
    "embedding_what_you_do",
]

# Production thresholds from SupabaseMatchScoringService
EMBEDDING_SCORE_THRESHOLDS = [
    (0.75, 10.0),
    (0.65, 8.0),
    (0.60, 6.0),
    (0.53, 4.5),
]
EMBEDDING_SCORE_DEFAULT = 3.0

OUTPUT_DIR = Path(__file__).resolve().parent
REPORT_PATH = OUTPUT_DIR / "study_1_3_expanded_synonym_test.md"
CSV_PATH = OUTPUT_DIR / "study_1_3_test_pairs.csv"

random.seed(42)
np.random.seed(42)


# ============================================================================
# Utility functions
# ============================================================================

def cosine_similarity(vec_a, vec_b):
    """Cosine similarity between two numpy vectors."""
    if vec_a is None or vec_b is None:
        return 0.0
    dot = np.dot(vec_a, vec_b)
    norm_a = np.linalg.norm(vec_a)
    norm_b = np.linalg.norm(vec_b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(max(0.0, min(1.0, dot / (norm_a * norm_b))))


def embedding_to_score(similarity: float) -> float:
    """Map cosine similarity to production 0-10 score bucket."""
    for threshold, score in EMBEDDING_SCORE_THRESHOLDS:
        if similarity >= threshold:
            return score
    return EMBEDDING_SCORE_DEFAULT


def score_bucket_label(similarity: float) -> str:
    """Human-readable score bucket label."""
    if similarity >= 0.75:
        return "Strong (10.0)"
    elif similarity >= 0.65:
        return "Good (8.0)"
    elif similarity >= 0.60:
        return "Possible (6.0)"
    elif similarity >= 0.53:
        return "Noise (4.5)"
    else:
        return "Below noise (3.0)"


# ============================================================================
# Step 1: Extract production vocabulary
# ============================================================================

def extract_vocabulary() -> Dict[str, List[str]]:
    """Extract unique non-empty values from the four profile text fields."""
    print("[Step 1] Extracting production vocabulary...")
    vocab = {}
    for field in FIELDS:
        values = (
            SupabaseProfile.objects.exclude(**{field: None})
            .exclude(**{field: ""})
            .values_list(field, flat=True)
        )
        unique = set()
        for v in values:
            if v and v.strip():
                unique.add(v.strip())
        vocab[field] = sorted(unique)
        print(f"  {field}: {len(unique)} unique values")
    return vocab


# ============================================================================
# Step 2-3: Generate synonym pairs from production data
# ============================================================================

# Manually curated synonym clusters from the JV domain.
# Each cluster contains texts that are semantically equivalent for JV matching.
# We generate pairs from within each cluster.

SYNONYM_CLUSTERS = {
    "audience": [
        # Business owners / entrepreneurs cluster
        [
            "business owners",
            "entrepreneurs",
            "small business owners",
            "startup founders",
            "business founders",
            "solopreneurs",
            "online business owners",
            "digital entrepreneurs",
            "aspiring entrepreneurs",
            "early-stage entrepreneurs",
        ],
        # Coaches / consultants cluster
        [
            "coaches and consultants",
            "service-based professionals",
            "coaches, consultants, and healers",
            "service providers",
            "professional coaches",
            "independent consultants",
            "solo practitioners",
            "coaching professionals",
        ],
        # Women entrepreneurs
        [
            "women entrepreneurs",
            "female business owners",
            "women in business",
            "women-owned businesses",
            "female founders",
            "women professionals",
            "businesswomen",
        ],
        # Health / wellness professionals
        [
            "health and wellness professionals",
            "wellness practitioners",
            "holistic health practitioners",
            "health coaches",
            "wellness coaches",
            "holistic practitioners",
            "natural health professionals",
            "integrative health professionals",
        ],
        # High-level executives
        [
            "executives",
            "C-suite leaders",
            "senior leaders",
            "corporate leaders",
            "business leaders",
            "high-level executives",
            "organizational leaders",
            "corporate executives",
        ],
        # Real estate professionals
        [
            "real estate investors",
            "real estate professionals",
            "property investors",
            "real estate agents and investors",
            "real estate entrepreneurs",
        ],
        # Authors/speakers
        [
            "authors and speakers",
            "speakers and authors",
            "thought leaders",
            "influencers and thought leaders",
            "industry experts",
            "subject matter experts",
        ],
        # Online course creators
        [
            "course creators",
            "online educators",
            "digital course creators",
            "e-learning professionals",
            "online course developers",
            "knowledge entrepreneurs",
        ],
        # Network marketers
        [
            "network marketers",
            "MLM professionals",
            "direct sales professionals",
            "network marketing professionals",
            "direct sellers",
        ],
        # Parents / families
        [
            "parents",
            "families",
            "mothers",
            "working moms",
            "working parents",
            "busy parents",
            "stay-at-home parents",
        ],
        # Nonprofit leaders
        [
            "nonprofit leaders",
            "nonprofit organizations",
            "social impact leaders",
            "mission-driven organizations",
            "purpose-driven leaders",
            "nonprofit executives",
        ],
        # Midlife / retirement
        [
            "midlife professionals",
            "professionals in career transition",
            "career changers",
            "people in midlife transition",
            "second-career seekers",
        ],
    ],
    "offering": [
        # Lead generation
        [
            "lead generation",
            "client acquisition",
            "customer acquisition",
            "lead gen services",
            "prospect generation",
            "sales pipeline building",
            "client attraction",
        ],
        # Business coaching
        [
            "business coaching",
            "executive coaching",
            "business mentoring",
            "executive mentoring",
            "leadership coaching",
            "business strategy coaching",
            "entrepreneur coaching",
        ],
        # Social media marketing
        [
            "social media marketing",
            "social media management",
            "digital marketing",
            "online marketing",
            "internet marketing",
            "social media strategy",
            "digital advertising",
        ],
        # Speaking / presentations
        [
            "keynote speaking",
            "public speaking",
            "motivational speaking",
            "inspirational speaking",
            "professional speaking",
            "conference presentations",
            "speaking engagements",
        ],
        # Copywriting / content
        [
            "copywriting",
            "content creation",
            "content writing",
            "content marketing",
            "copy and content services",
            "brand messaging",
            "content strategy",
        ],
        # Website / funnel building
        [
            "website design",
            "web development",
            "funnel building",
            "landing page design",
            "sales funnel creation",
            "website creation",
            "funnel design",
        ],
        # Mindset / transformation coaching
        [
            "mindset coaching",
            "transformational coaching",
            "personal development coaching",
            "life transformation",
            "personal growth coaching",
            "breakthrough coaching",
            "mindset training",
        ],
        # Podcast hosting
        [
            "podcast hosting",
            "podcast production",
            "podcast guesting",
            "podcast management",
            "podcast creation",
            "podcast development",
        ],
        # Book publishing
        [
            "book publishing",
            "self-publishing",
            "book writing coaching",
            "author coaching",
            "book launch services",
            "publishing services",
            "bestseller launch",
        ],
        # Financial coaching
        [
            "financial coaching",
            "money coaching",
            "financial planning",
            "wealth building coaching",
            "financial literacy education",
            "investment education",
            "financial empowerment",
        ],
        # Email marketing
        [
            "email marketing",
            "email list building",
            "newsletter marketing",
            "email campaigns",
            "email automation",
            "email marketing strategy",
        ],
        # Video production
        [
            "video production",
            "video marketing",
            "video content creation",
            "video editing",
            "video strategy",
            "visual content creation",
        ],
        # Group programs
        [
            "group coaching",
            "group programs",
            "mastermind groups",
            "group mentoring",
            "mastermind facilitation",
            "cohort programs",
        ],
        # SEO
        [
            "SEO",
            "search engine optimization",
            "organic search marketing",
            "SEO consulting",
            "search marketing",
            "website optimization",
        ],
    ],
    "seeking": [
        # Podcast appearances
        [
            "podcast appearances",
            "speaking opportunities",
            "guest speaking",
            "podcast guesting opportunities",
            "media appearances",
            "interview opportunities",
            "guest expert opportunities",
        ],
        # JV partners
        [
            "JV partners",
            "strategic alliances",
            "joint venture partners",
            "strategic partners",
            "collaboration partners",
            "business alliance partners",
            "partnership opportunities",
        ],
        # Affiliate partners
        [
            "affiliate partners",
            "affiliate marketing partners",
            "referral partners",
            "commission-based partners",
            "affiliate relationships",
            "referral relationships",
        ],
        # Summit / event collaboration
        [
            "summit speakers",
            "virtual summit participants",
            "event collaboration",
            "conference speakers",
            "webinar partners",
            "online event collaborators",
        ],
        # List building
        [
            "list building partners",
            "audience growth partners",
            "email list growth",
            "subscriber growth",
            "list swap partners",
            "cross-promotion partners",
        ],
        # Visibility / exposure
        [
            "visibility opportunities",
            "brand exposure",
            "increased visibility",
            "media exposure",
            "brand awareness opportunities",
            "audience exposure",
        ],
        # Technology / tools
        [
            "technology partners",
            "tech integration partners",
            "software partners",
            "platform integration",
            "technology alliances",
        ],
        # Funding / investment
        [
            "investors",
            "funding opportunities",
            "investment partners",
            "angel investors",
            "capital partners",
            "financial backing",
        ],
        # Sponsorship
        [
            "sponsors",
            "sponsorship opportunities",
            "brand sponsors",
            "event sponsors",
            "corporate sponsors",
            "sponsorship deals",
        ],
    ],
    "niche": [
        # Health and wellness
        [
            "health and wellness coaching",
            "holistic wellness education",
            "wellness coaching and education",
            "integrative health coaching",
            "whole-body wellness",
            "mind-body wellness",
        ],
        # Personal development
        [
            "personal development",
            "personal growth",
            "self-improvement",
            "self-development",
            "personal transformation",
            "human potential development",
        ],
        # Marketing strategy
        [
            "marketing strategy",
            "business marketing",
            "strategic marketing",
            "marketing consulting",
            "marketing coaching",
            "growth marketing",
        ],
        # Spiritual coaching
        [
            "spiritual coaching",
            "spiritual development",
            "consciousness coaching",
            "spiritual growth",
            "spiritual mentoring",
            "spiritual transformation",
        ],
        # Relationship coaching
        [
            "relationship coaching",
            "couples coaching",
            "marriage coaching",
            "relationship counseling",
            "love coaching",
            "partnership coaching",
        ],
        # Sales training
        [
            "sales training",
            "sales coaching",
            "sales consulting",
            "sales enablement",
            "sales mastery",
            "revenue growth training",
        ],
        # Branding
        [
            "personal branding",
            "brand strategy",
            "brand building",
            "brand development",
            "brand identity",
            "brand positioning",
        ],
        # Fitness
        [
            "fitness coaching",
            "personal training",
            "health and fitness",
            "fitness and nutrition",
            "exercise coaching",
            "physical fitness training",
        ],
    ],
    "cross_category": [
        # Seeking <-> offering alignment
        [
            "looking for podcast interview opportunities",
            "offering podcast guest spots to experts",
        ],
        [
            "seeking JV partners who serve entrepreneurs",
            "serving coaches and business owners with marketing",
        ],
        [
            "looking for speakers for virtual summits",
            "offering keynote and summit speaking",
        ],
        [
            "seeking affiliate partners for online courses",
            "affiliate program for digital training products",
        ],
        [
            "looking for collaboration on wellness programs",
            "offering holistic wellness coaching partnerships",
        ],
        [
            "seeking partners in the personal development space",
            "providing personal growth and transformation coaching",
        ],
        [
            "seeking email list swap partners",
            "cross-promotion and list sharing opportunities",
        ],
        [
            "looking for social media marketing help",
            "offering social media management and strategy",
        ],
        [
            "seeking business coaching for entrepreneurs",
            "providing business mentoring for startup founders",
        ],
        [
            "looking for content creation partners",
            "content marketing and copywriting services",
        ],
    ],
}


def generate_synonym_pairs_from_clusters() -> List[Dict]:
    """Generate synonym pairs from curated clusters. Returns list of pair dicts."""
    pairs = []
    pair_id = 0

    for category, clusters in SYNONYM_CLUSTERS.items():
        for cluster in clusters:
            # Generate pairs from within the cluster
            for i in range(len(cluster)):
                for j in range(i + 1, len(cluster)):
                    pair_id += 1
                    pairs.append({
                        "pair_id": pair_id,
                        "text_a": cluster[i],
                        "text_b": cluster[j],
                        "category": category,
                        "label": "synonym",
                        "label_int": 1,
                    })

    return pairs


def find_production_synonym_pairs(
    vocab: Dict[str, List[str]],
    model: SentenceTransformer,
    n_candidates_per_field: int = 100,
) -> List[Dict]:
    """
    Find additional synonym pairs directly from production vocabulary
    by embedding all short texts and finding high-similarity pairs.
    """
    print("  Finding synonym pairs in production vocabulary...")
    production_pairs = []
    pair_id_offset = 10000  # Avoid collision with curated pair IDs

    for field in FIELDS:
        texts = [t for t in vocab[field] if 10 < len(t) < 120]
        if len(texts) < 20:
            continue

        # Sample to keep compute manageable
        if len(texts) > 500:
            sampled = random.sample(texts, 500)
        else:
            sampled = texts

        print(f"    {field}: encoding {len(sampled)} texts...")
        embeddings = model.encode(sampled, normalize_embeddings=True, show_progress_bar=False)

        # Compute pairwise similarities and find high-sim pairs
        # Only check a subset for efficiency
        sim_matrix = embeddings @ embeddings.T

        high_sim_pairs = []
        for i in range(len(sampled)):
            for j in range(i + 1, len(sampled)):
                sim = float(sim_matrix[i, j])
                if 0.78 <= sim < 0.99:  # High similarity but not identical
                    # Check they actually have different wording
                    if sampled[i].lower() != sampled[j].lower():
                        high_sim_pairs.append((i, j, sim))

        # Sort by similarity descending, take top candidates
        high_sim_pairs.sort(key=lambda x: -x[2])
        taken = 0
        for i, j, sim in high_sim_pairs:
            if taken >= n_candidates_per_field:
                break
            pair_id_offset += 1
            production_pairs.append({
                "pair_id": pair_id_offset,
                "text_a": sampled[i],
                "text_b": sampled[j],
                "category": f"production_{field}",
                "label": "synonym",
                "label_int": 1,
                "discovery_sim": round(sim, 4),
            })
            taken += 1

        print(f"    {field}: found {taken} production synonym pairs (threshold >= 0.78)")

    return production_pairs


# ============================================================================
# Step 3: Adversarial pairs
# ============================================================================

ADVERSARIAL_PAIRS = [
    # Similar words, different JV meaning
    ("life coaching", "life insurance"),
    ("health coaching", "healthcare administration"),
    ("financial planning", "financial marketing"),
    ("real estate investing", "real estate agent services"),
    ("business coaching", "business accounting"),
    ("leadership development", "leadership recruitment"),
    ("personal branding", "personal injury law"),
    ("wellness coaching", "wellness insurance"),
    ("executive coaching", "executive search"),
    ("career coaching", "career staffing agency"),
    ("sales coaching", "sales management software"),
    ("mindset coaching", "mindset assessment tools"),
    ("podcast hosting", "web hosting"),
    ("content creation", "content moderation"),
    ("email marketing", "email security"),
    ("social media marketing", "social media monitoring"),
    ("course creation", "course management system"),
    ("funnel building", "funnel analysis engineering"),
    ("brand strategy", "brand trademark law"),
    ("book publishing", "book distribution logistics"),
    ("video production", "video surveillance"),
    ("community building", "community planning (urban)"),
    ("network marketing", "network engineering"),
    ("affiliate marketing", "affiliate compliance"),
    ("coaching certification", "coaching hiring"),
    ("public speaking", "public relations"),
    ("webinar hosting", "webinar technology platform"),
    ("virtual events", "virtual reality development"),
    ("digital products", "digital forensics"),
    ("online courses", "online degree accreditation"),
    ("membership sites", "membership benefits administration"),
    ("group coaching", "group therapy"),
    ("relationship coaching", "relationship management software"),
    ("financial coaching", "financial auditing"),
    ("weight loss coaching", "weight loss surgery"),
    ("nutrition coaching", "nutritional science research"),
    ("yoga instruction", "yoga studio real estate"),
    ("meditation teaching", "meditation app development"),
    ("writing coaching", "technical writing services"),
    ("art coaching", "art authentication"),
    ("music coaching", "music licensing"),
    ("fitness coaching", "fitness equipment manufacturing"),
    ("parenting coaching", "parenting rights legal services"),
    ("grief coaching", "grief counseling clinical"),
    ("stress management coaching", "stress testing engineering"),
    ("time management coaching", "time tracking software"),
    ("productivity coaching", "productivity software development"),
    ("accountability coaching", "accountability auditing"),
    ("confidence coaching", "confidence interval statistics"),
    ("influence coaching", "influence operations security"),
    # Additional adversarial pairs with more nuance
    ("entrepreneurs", "enterprise software"),
    ("small business owners", "small business loans"),
    ("speakers bureau", "speaker manufacturer"),
    ("coaching program", "coding program"),
    ("mastermind group", "master's degree program"),
    ("summit speaking", "mountain summit expedition"),
    ("funnel optimization", "wind tunnel optimization"),
    ("lead magnet creation", "magnetic lead engineering"),
    ("list building", "building construction"),
    ("launch strategy", "rocket launch engineering"),
]


def generate_adversarial_pairs() -> List[Dict]:
    """Generate adversarial pair dicts."""
    pairs = []
    for i, (text_a, text_b) in enumerate(ADVERSARIAL_PAIRS):
        pairs.append({
            "pair_id": 20000 + i,
            "text_a": text_a,
            "text_b": text_b,
            "category": "adversarial",
            "label": "adversarial",
            "label_int": 0,  # Should NOT be matched
        })
    return pairs


# ============================================================================
# Step 4: Random negative pairs
# ============================================================================

def generate_random_negative_pairs(
    vocab: Dict[str, List[str]], n_pairs: int = 1200
) -> List[Dict]:
    """Generate random pairs from different profile field texts that should NOT be similar."""
    print(f"[Step 4] Generating {n_pairs} random negative pairs...")
    all_texts = []
    for field in FIELDS:
        for text in vocab[field]:
            if text.strip() and len(text.strip()) > 5:
                all_texts.append((field, text.strip()))

    pairs = []
    attempts = 0
    seen = set()

    while len(pairs) < n_pairs and attempts < n_pairs * 5:
        attempts += 1
        idx_a = random.randint(0, len(all_texts) - 1)
        idx_b = random.randint(0, len(all_texts) - 1)
        if idx_a == idx_b:
            continue

        field_a, text_a = all_texts[idx_a]
        field_b, text_b = all_texts[idx_b]

        # Skip if same text
        if text_a.lower() == text_b.lower():
            continue

        pair_key = tuple(sorted([text_a.lower(), text_b.lower()]))
        if pair_key in seen:
            continue
        seen.add(pair_key)

        pairs.append({
            "pair_id": 30000 + len(pairs),
            "text_a": text_a,
            "text_b": text_b,
            "category": f"random_{field_a}_vs_{field_b}",
            "label": "negative",
            "label_int": 0,
        })

    print(f"  Generated {len(pairs)} random negative pairs")
    return pairs


# ============================================================================
# Step 5: Compute embeddings and cosine similarity
# ============================================================================

def compute_all_similarities(
    pairs: List[Dict], model: SentenceTransformer
) -> List[Dict]:
    """Compute cosine similarity for all pairs using the model."""
    print(f"[Step 5] Computing embeddings for {len(pairs)} pairs...")

    # Collect all unique texts
    all_texts = set()
    for p in pairs:
        all_texts.add(p["text_a"])
        all_texts.add(p["text_b"])
    all_texts = sorted(all_texts)
    print(f"  {len(all_texts)} unique texts to embed")

    # Batch encode
    batch_size = 128
    embeddings = model.encode(
        all_texts, normalize_embeddings=True, batch_size=batch_size, show_progress_bar=True
    )

    # Build lookup
    text_to_emb = {text: embeddings[i] for i, text in enumerate(all_texts)}

    # Compute similarity for each pair
    for p in pairs:
        emb_a = text_to_emb[p["text_a"]]
        emb_b = text_to_emb[p["text_b"]]
        sim = cosine_similarity(emb_a, emb_b)
        p["cosine_similarity"] = round(sim, 6)
        p["score_bucket"] = embedding_to_score(sim)
        p["bucket_label"] = score_bucket_label(sim)

    return pairs


# ============================================================================
# Step 6: Compute metrics and generate report
# ============================================================================

def compute_metrics(pairs: List[Dict]) -> Dict:
    """Compute AUC-PR, F1, discrimination gap, per-category breakdown."""
    # Separate positive (synonym) and negative (adversarial + random) pairs
    synonym_pairs = [p for p in pairs if p["label"] == "synonym"]
    adversarial_pairs = [p for p in pairs if p["label"] == "adversarial"]
    negative_pairs = [p for p in pairs if p["label"] == "negative"]

    synonym_sims = np.array([p["cosine_similarity"] for p in synonym_pairs])
    adversarial_sims = np.array([p["cosine_similarity"] for p in adversarial_pairs])
    negative_sims = np.array([p["cosine_similarity"] for p in negative_pairs])
    all_negative_sims = np.concatenate([adversarial_sims, negative_sims])

    # Basic stats
    metrics = {
        "n_synonym_pairs": len(synonym_pairs),
        "n_adversarial_pairs": len(adversarial_pairs),
        "n_negative_pairs": len(negative_pairs),
        "n_total_pairs": len(pairs),
        "synonym_mean": round(float(np.mean(synonym_sims)), 4),
        "synonym_std": round(float(np.std(synonym_sims)), 4),
        "synonym_median": round(float(np.median(synonym_sims)), 4),
        "synonym_min": round(float(np.min(synonym_sims)), 4),
        "synonym_max": round(float(np.max(synonym_sims)), 4),
        "adversarial_mean": round(float(np.mean(adversarial_sims)), 4) if len(adversarial_sims) > 0 else None,
        "adversarial_std": round(float(np.std(adversarial_sims)), 4) if len(adversarial_sims) > 0 else None,
        "negative_mean": round(float(np.mean(negative_sims)), 4),
        "negative_std": round(float(np.std(negative_sims)), 4),
        "all_negative_mean": round(float(np.mean(all_negative_sims)), 4),
        "all_negative_std": round(float(np.std(all_negative_sims)), 4),
        "discrimination_gap": round(float(np.mean(synonym_sims) - np.mean(all_negative_sims)), 4),
        "discrimination_gap_vs_random": round(float(np.mean(synonym_sims) - np.mean(negative_sims)), 4),
    }

    # AUC-PR computation (synonym = positive, all_negative = negative)
    y_true = np.concatenate([
        np.ones(len(synonym_sims)),
        np.zeros(len(all_negative_sims)),
    ])
    y_scores = np.concatenate([synonym_sims, all_negative_sims])

    # Fine-grained PR curve
    fine_thresholds = np.linspace(0.0, 1.0, 2000)
    n_pos = len(synonym_sims)
    n_neg = len(all_negative_sims)

    precisions = []
    recalls = []
    f1s = []

    for t in fine_thresholds:
        tp = int(np.sum(synonym_sims >= t))
        fp = int(np.sum(all_negative_sims >= t))
        fn = n_pos - tp
        p = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
        precisions.append(p)
        recalls.append(r)
        f1s.append(f1)

    precisions = np.array(precisions)
    recalls = np.array(recalls)
    f1s = np.array(f1s)

    # Sort by recall for AUC computation
    sorted_idx = np.argsort(recalls)
    recall_sorted = recalls[sorted_idx]
    precision_sorted = precisions[sorted_idx]

    # Deduplicate
    unique_recalls = []
    unique_precisions = []
    prev_r = -1
    for r, p in zip(recall_sorted, precision_sorted):
        if r != prev_r:
            unique_recalls.append(r)
            unique_precisions.append(p)
        else:
            unique_precisions[-1] = max(unique_precisions[-1], p)
        prev_r = r

    _trapz = getattr(np, "trapezoid", None) or getattr(np, "trapz")
    auc_pr = float(_trapz(unique_precisions, unique_recalls))

    # Optimal F1
    best_f1_idx = int(np.argmax(f1s))
    optimal_f1 = float(f1s[best_f1_idx])
    optimal_threshold = float(fine_thresholds[best_f1_idx])

    metrics["auc_pr"] = round(auc_pr, 4)
    metrics["optimal_f1"] = round(optimal_f1, 4)
    metrics["optimal_f1_threshold"] = round(optimal_threshold, 4)
    metrics["optimal_f1_score_bucket"] = score_bucket_label(optimal_threshold)

    # Per production threshold analysis
    prod_thresholds = [0.53, 0.60, 0.65, 0.75]
    threshold_results = []
    for t in prod_thresholds:
        tp = int(np.sum(synonym_sims >= t))
        fp = int(np.sum(all_negative_sims >= t))
        fn = n_pos - tp
        tn = n_neg - fp
        p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
        fpr = fp / n_neg if n_neg > 0 else 0.0
        threshold_results.append({
            "threshold": t,
            "tp": tp, "fp": fp, "fn": fn,
            "precision": round(p, 4),
            "recall": round(r, 4),
            "f1": round(f1, 4),
            "fpr": round(fpr, 4),
        })
    metrics["threshold_results"] = threshold_results

    # Per-category breakdown
    categories = sorted(set(p["category"] for p in synonym_pairs))
    category_stats = {}
    for cat in categories:
        cat_sims = np.array([p["cosine_similarity"] for p in synonym_pairs if p["category"] == cat])
        category_stats[cat] = {
            "n_pairs": len(cat_sims),
            "mean": round(float(np.mean(cat_sims)), 4),
            "std": round(float(np.std(cat_sims)), 4),
            "median": round(float(np.median(cat_sims)), 4),
            "min": round(float(np.min(cat_sims)), 4),
            "max": round(float(np.max(cat_sims)), 4),
            "pct_above_065": round(100 * float(np.mean(cat_sims >= 0.65)), 1),
            "pct_above_075": round(100 * float(np.mean(cat_sims >= 0.75)), 1),
        }
    metrics["category_stats"] = category_stats

    # Adversarial analysis
    if len(adversarial_sims) > 0:
        metrics["adversarial_pct_above_065"] = round(
            100 * float(np.mean(adversarial_sims >= 0.65)), 1
        )
        metrics["adversarial_pct_above_075"] = round(
            100 * float(np.mean(adversarial_sims >= 0.75)), 1
        )
        # How many adversarial pairs score higher than synonym median?
        metrics["adversarial_above_synonym_median"] = round(
            100 * float(np.mean(adversarial_sims >= np.median(synonym_sims))), 1
        )

    return metrics


def format_report(metrics: Dict, elapsed: float) -> str:
    """Format the complete markdown report."""
    lines = []
    w = lines.append
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    w("# Study 1.3: Expanded Synonym / Embedding Discrimination Test")
    w("")
    w(f"**Generated:** {timestamp}")
    w(f"**Model:** BAAI/bge-large-en-v1.5 (1024 dimensions)")
    w(f"**Time elapsed:** {elapsed:.1f}s")
    w("")

    # Executive Summary
    w("## Executive Summary")
    w("")
    auc = metrics["auc_pr"]
    gap = metrics["discrimination_gap"]
    auc_pass = "PASS" if auc >= 0.80 else "BELOW TARGET"
    gap_pass = "PASS" if gap >= 0.15 else "BELOW TARGET"
    w(f"| Metric | Value | Target | Status |")
    w(f"|--------|-------|--------|--------|")
    w(f"| AUC-PR | {auc:.4f} | >= 0.80 | {auc_pass} |")
    w(f"| Discrimination Gap (syn - neg) | {gap:.4f} | >= 0.15 | {gap_pass} |")
    w(f"| Optimal F1 | {metrics['optimal_f1']:.4f} @ t={metrics['optimal_f1_threshold']:.2f} | - | {metrics['optimal_f1_score_bucket']} |")
    w(f"| Original 30-pair AUC-PR | 0.5607 | >= 0.80 | BELOW TARGET |")
    w(f"| Improvement | {'+' if auc > 0.5607 else ''}{(auc - 0.5607):.4f} | - | {'Improved' if auc > 0.5607 else 'Regression'} |")
    w("")

    # Dataset composition
    w("## Dataset Composition")
    w("")
    w(f"| Category | Count |")
    w(f"|----------|-------|")
    w(f"| Synonym pairs (positive) | {metrics['n_synonym_pairs']} |")
    w(f"| Adversarial pairs (negative) | {metrics['n_adversarial_pairs']} |")
    w(f"| Random negative pairs | {metrics['n_negative_pairs']} |")
    w(f"| **Total pairs** | **{metrics['n_total_pairs']}** |")
    w("")

    # Similarity distributions
    w("## Similarity Distributions")
    w("")
    w(f"| Group | N | Mean | Std | Median | Min | Max |")
    w(f"|-------|---|------|-----|--------|-----|-----|")
    w(f"| Synonym | {metrics['n_synonym_pairs']} | {metrics['synonym_mean']:.4f} | {metrics['synonym_std']:.4f} | {metrics['synonym_median']:.4f} | {metrics['synonym_min']:.4f} | {metrics['synonym_max']:.4f} |")
    if metrics["adversarial_mean"] is not None:
        w(f"| Adversarial | {metrics['n_adversarial_pairs']} | {metrics['adversarial_mean']:.4f} | {metrics['adversarial_std']:.4f} | - | - | - |")
    w(f"| Random Negative | {metrics['n_negative_pairs']} | {metrics['negative_mean']:.4f} | {metrics['negative_std']:.4f} | - | - | - |")
    w(f"| All Negative | {metrics['n_adversarial_pairs'] + metrics['n_negative_pairs']} | {metrics['all_negative_mean']:.4f} | {metrics['all_negative_std']:.4f} | - | - | - |")
    w("")
    w(f"**Discrimination gap (synonym mean - all negative mean):** {metrics['discrimination_gap']:.4f}")
    w(f"**Discrimination gap (synonym mean - random negative mean):** {metrics['discrimination_gap_vs_random']:.4f}")
    w("")

    # Production threshold analysis
    w("## Production Threshold Analysis")
    w("")
    w(f"| Threshold | Score Bucket | TP | FP | FN | Precision | Recall | F1 | FPR |")
    w(f"|-----------|-------------|----|----|-----|-----------|--------|-----|------|")
    for tr in metrics["threshold_results"]:
        bucket = score_bucket_label(tr["threshold"])
        w(f"| {tr['threshold']:.2f} | {bucket} | {tr['tp']} | {tr['fp']} | {tr['fn']} | {tr['precision']:.4f} | {tr['recall']:.4f} | {tr['f1']:.4f} | {tr['fpr']:.4f} |")
    w("")
    w(f"**Optimal F1 threshold:** {metrics['optimal_f1_threshold']:.2f} (score bucket: {metrics['optimal_f1_score_bucket']})")
    w("")

    # Per-category breakdown
    w("## Per-Category Breakdown (Synonym Pairs)")
    w("")
    w(f"| Category | N Pairs | Mean Sim | Std | Median | % >= 0.65 | % >= 0.75 |")
    w(f"|----------|---------|----------|-----|--------|-----------|-----------|")
    sorted_cats = sorted(
        metrics["category_stats"].items(),
        key=lambda x: -x[1]["mean"]
    )
    for cat, stats in sorted_cats:
        w(f"| {cat} | {stats['n_pairs']} | {stats['mean']:.4f} | {stats['std']:.4f} | {stats['median']:.4f} | {stats['pct_above_065']:.1f}% | {stats['pct_above_075']:.1f}% |")

    # Weakest categories
    weakest = [c for c, s in sorted_cats if s["mean"] < 0.65]
    if weakest:
        w("")
        w(f"**Weakest categories (mean < 0.65):** {', '.join(weakest)}")
    w("")

    # Adversarial analysis
    w("## Adversarial Pair Analysis")
    w("")
    if metrics["adversarial_mean"] is not None:
        w(f"- **Adversarial mean similarity:** {metrics['adversarial_mean']:.4f} (vs synonym mean {metrics['synonym_mean']:.4f})")
        w(f"- **% adversarial pairs scoring >= 0.65 (Good match):** {metrics['adversarial_pct_above_065']:.1f}%")
        w(f"- **% adversarial pairs scoring >= 0.75 (Strong match):** {metrics['adversarial_pct_above_075']:.1f}%")
        w(f"- **% adversarial above synonym median:** {metrics['adversarial_above_synonym_median']:.1f}%")
        w("")
        adv_gap = metrics["synonym_mean"] - metrics["adversarial_mean"]
        if adv_gap > 0.10:
            w(f"The model shows **{adv_gap:.4f}** separation between true synonyms and adversarial pairs, which is {'adequate' if adv_gap > 0.15 else 'marginal'}.")
        else:
            w(f"WARNING: The model shows only **{adv_gap:.4f}** separation between true synonyms and adversarial pairs. This is insufficient.")
    w("")

    # Comparison to original study
    w("## Comparison to Original 30-Pair Study")
    w("")
    w(f"| Metric | Original (30 pairs) | Expanded ({metrics['n_synonym_pairs']} positive pairs) | Change |")
    w(f"|--------|--------------------|-----------------------------------------|--------|")
    w(f"| AUC-PR | 0.5607 | {metrics['auc_pr']:.4f} | {metrics['auc_pr'] - 0.5607:+.4f} |")
    w(f"| Sample size (positive) | 30 | {metrics['n_synonym_pairs']} | {metrics['n_synonym_pairs'] - 30:+d} |")
    w(f"| Sample size (negative) | ~500 | {metrics['n_adversarial_pairs'] + metrics['n_negative_pairs']} | - |")
    w("")

    # Recommendation
    w("## Recommendation")
    w("")
    if auc >= 0.80 and gap >= 0.15:
        w("**bge-large-en-v1.5 is ADEQUATE** for JV partner matching at the current thresholds.")
        w("")
        w("The model demonstrates sufficient discrimination between semantically similar JV texts")
        w("and unrelated content. The production thresholds are well-calibrated.")
    elif auc >= 0.65 and gap >= 0.10:
        w("**bge-large-en-v1.5 is MARGINALLY ADEQUATE** for JV partner matching.")
        w("")
        w("The model shows some discrimination ability, but the thresholds may need adjustment.")
        w("Consider:")
        w(f"- Lowering the 'Strong match' threshold from 0.75 to ~{metrics['optimal_f1_threshold']:.2f}")
        w("- Adding domain-specific fine-tuning for JV vocabulary")
        w("- Supplementing embedding similarity with keyword/entity matching")
    else:
        w("**bge-large-en-v1.5 is INADEQUATE** for reliable JV partner matching.")
        w("")
        w("The model does not sufficiently discriminate between synonym-level matches and random noise.")
        w("Recommended actions:")
        w("1. **Threshold adjustment:** Consider the optimal F1 threshold identified above")
        w("2. **Model upgrade:** Evaluate domain-specific embedding models or fine-tuned variants")
        w("3. **Hybrid approach:** Combine embedding similarity with TF-IDF, keyword overlap, or LLM re-ranking")
        w("4. **Data cleaning:** Review production texts for quality; many may be too long/noisy for single-vector embedding")

    w("")
    w("### Suggested Threshold Adjustments")
    w("")
    opt_t = metrics["optimal_f1_threshold"]
    w(f"Based on the optimal F1 threshold of **{opt_t:.2f}**, consider:")
    w("")
    w("```python")
    w("# Current thresholds")
    w("EMBEDDING_SCORE_THRESHOLDS = [")
    w("    (0.75, 10.0),  # Strong semantic match")
    w("    (0.65,  8.0),  # Good match")
    w("    (0.60,  6.0),  # Possible match")
    w("    (0.53,  4.5),  # At random noise mean")
    w("]")
    w("")

    # Suggest revised thresholds based on data
    # Strong: synonym mean + 0.5 std (well above synonym center)
    strong_t = round(metrics["synonym_mean"] + metrics["synonym_std"] * 0.5, 2)
    strong_t = min(0.85, max(0.70, strong_t))
    # Good: optimal F1 threshold (best precision-recall tradeoff)
    good_t = round(opt_t, 2)
    good_t = max(good_t, 0.60)
    # Possible: halfway between optimal F1 and negative ceiling
    neg_ceiling = metrics["all_negative_mean"] + metrics["all_negative_std"]
    possible_t = round((good_t + neg_ceiling) / 2, 2)
    possible_t = max(0.55, min(possible_t, good_t - 0.02))
    # Noise: negative ceiling (mean + 1 std of negatives)
    noise_t = round(neg_ceiling, 2)
    noise_t = max(0.50, min(noise_t, possible_t - 0.02))

    # Ensure strictly descending
    if good_t >= strong_t:
        good_t = strong_t - 0.03
    if possible_t >= good_t:
        possible_t = good_t - 0.03
    if noise_t >= possible_t:
        noise_t = possible_t - 0.03

    w("# Suggested revised thresholds (based on expanded test)")
    w("EMBEDDING_SCORE_THRESHOLDS = [")
    w(f"    ({strong_t:.2f}, 10.0),  # Strong match (synonym mean + 0.5 std)")
    w(f"    ({good_t:.2f},  8.0),  # Good match (optimal F1 threshold)")
    w(f"    ({possible_t:.2f},  6.0),  # Possible match (midpoint)")
    w(f"    ({noise_t:.2f},  4.5),  # At noise ceiling (neg mean + 1 std)")
    w("]")
    w("```")
    w("")
    w("### Adversarial Vulnerability Note")
    w("")
    if metrics["adversarial_mean"] is not None:
        adv_gap = metrics["synonym_mean"] - metrics["adversarial_mean"]
        w(f"The adversarial gap ({adv_gap:.4f}) is notably smaller than the synonym-vs-random gap")
        w(f"({metrics['discrimination_gap_vs_random']:.4f}). This means the embedding model struggles")
        w(f"to distinguish between genuinely related JV concepts (e.g., 'life coaching' = semantically")
        w(f"similar) and superficially similar but JV-irrelevant concepts (e.g., 'life insurance').")
        w(f"This is a known limitation of general-purpose text embeddings and is best mitigated by:")
        w("")
        w("1. **JV-specific entity tagging** before embedding (e.g., tagging 'coaching' vs 'insurance' as different domains)")
        w("2. **Post-hoc re-ranking** with an LLM that understands business context")
        w("3. **Embedding fine-tuning** on JV-specific positive/negative pairs")
    w("")

    w("---")
    w(f"*Report generated by Study 1.3 expanded synonym test script.*")

    return "\n".join(lines)


# ============================================================================
# Main
# ============================================================================

def main():
    start_time = time.time()

    print("=" * 70)
    print("  Study 1.3: Expanded Synonym / Embedding Discrimination Test")
    print("  Model: BAAI/bge-large-en-v1.5")
    print("=" * 70)

    # Step 1: Extract vocabulary
    vocab = extract_vocabulary()

    # Load model
    print("\n[Model] Loading BAAI/bge-large-en-v1.5...")
    model = SentenceTransformer(MODEL_NAME)
    print(f"  Model loaded. Embedding dimension: {model.get_sentence_embedding_dimension()}")

    # Step 2: Generate synonym pairs from curated clusters
    print("\n[Step 2] Generating synonym pairs from curated clusters...")
    curated_pairs = generate_synonym_pairs_from_clusters()
    print(f"  Generated {len(curated_pairs)} curated synonym pairs")

    # Find additional synonym pairs from production data
    production_synonym_pairs = find_production_synonym_pairs(vocab, model, n_candidates_per_field=50)
    print(f"  Found {len(production_synonym_pairs)} production synonym pairs")

    all_synonym_pairs = curated_pairs + production_synonym_pairs
    print(f"  Total synonym pairs: {len(all_synonym_pairs)}")

    # Step 3: Generate adversarial pairs
    print("\n[Step 3] Generating adversarial pairs...")
    adversarial_pairs = generate_adversarial_pairs()
    print(f"  Generated {len(adversarial_pairs)} adversarial pairs")

    # Step 4: Generate random negative pairs
    negative_pairs = generate_random_negative_pairs(vocab, n_pairs=1200)

    # Combine all pairs
    all_pairs = all_synonym_pairs + adversarial_pairs + negative_pairs
    print(f"\n  Total test pairs: {len(all_pairs)}")
    print(f"    - Synonym (positive): {len(all_synonym_pairs)}")
    print(f"    - Adversarial (negative): {len(adversarial_pairs)}")
    print(f"    - Random negative: {len(negative_pairs)}")

    # Step 5: Compute cosine similarity
    print("")
    all_pairs = compute_all_similarities(all_pairs, model)

    # Step 6: Compute metrics and generate report
    print("\n[Step 6] Computing metrics...")
    metrics = compute_metrics(all_pairs)

    elapsed = time.time() - start_time

    print(f"\n  KEY RESULTS:")
    print(f"    AUC-PR:              {metrics['auc_pr']:.4f} (target >= 0.80)")
    print(f"    Optimal F1:          {metrics['optimal_f1']:.4f} @ t={metrics['optimal_f1_threshold']:.2f}")
    print(f"    Discrimination gap:  {metrics['discrimination_gap']:.4f} (target >= 0.15)")
    print(f"    Synonym mean:        {metrics['synonym_mean']:.4f}")
    print(f"    All negative mean:   {metrics['all_negative_mean']:.4f}")
    if metrics["adversarial_mean"] is not None:
        print(f"    Adversarial mean:    {metrics['adversarial_mean']:.4f}")

    # Save CSV
    print(f"\n[Output] Saving test pairs CSV to {CSV_PATH}...")
    df = pd.DataFrame(all_pairs)
    # Reorder columns
    col_order = [
        "pair_id", "text_a", "text_b", "category", "label", "label_int",
        "cosine_similarity", "score_bucket", "bucket_label",
    ]
    extra_cols = [c for c in df.columns if c not in col_order]
    df = df[col_order + extra_cols]
    df.to_csv(CSV_PATH, index=False)
    print(f"  Saved {len(df)} rows")

    # Save report
    print(f"\n[Output] Saving report to {REPORT_PATH}...")
    report = format_report(metrics, elapsed)
    REPORT_PATH.write_text(report)
    print(f"  Report saved")

    print(f"\n{'=' * 70}")
    print(f"  Study 1.3 complete in {elapsed:.1f}s")
    print(f"  Report: {REPORT_PATH}")
    print(f"  CSV:    {CSV_PATH}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
