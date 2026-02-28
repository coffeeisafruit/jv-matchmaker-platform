"""
Hugging Face Inference API Client

Centralized client for HF model inference with retry, caching, and rate tracking.
Follows the same patterns as matching/enrichment/retry_strategy.py and
matching/enrichment/ai_research.py for error handling and logging.

Usage:
    client = HFClient()
    embedding = client.embed("business growth coaching for entrepreneurs")
    labels = client.classify_zero_shot("Leadership training for executives", [
        "Business Coaching", "Health & Wellness", "Leadership & Management"
    ])
"""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import requests

# Lazy-loaded local models keyed by model name (avoids import cost when API works)
_local_models: dict = {}

logger = logging.getLogger('enrichment.hf')

# Default models — can be overridden via env or constructor
DEFAULT_EMBEDDING_MODEL = os.environ.get(
    'HF_EMBEDDING_MODEL', 'BAAI/bge-large-en-v1.5'
)
DEFAULT_CLASSIFICATION_MODEL = os.environ.get(
    'HF_CLASSIFICATION_MODEL', 'MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli'
)
DEFAULT_NER_MODEL = 'dslim/bert-base-NER'

HF_INFERENCE_URL = "https://router.huggingface.co/hf-inference/models"

# Metrics tracking (same pattern as pipeline's cost tracking)
_metrics = {
    'api_calls': 0,
    'api_errors': 0,
    'cache_hits': 0,
    'cache_misses': 0,
    'total_latency_ms': 0,
}


def get_metrics() -> dict:
    """Return current session metrics."""
    return dict(_metrics)


class HFClient:
    """
    Hugging Face Inference API client with file-based caching and retry.

    Cache location: Chelsea_clients/hf_cache/ (same pattern as research_cache/)
    Retry: 3 attempts with exponential backoff (mirrors retry_strategy.py)
    """

    def __init__(self, api_token: Optional[str] = None, cache_dir: Optional[str] = None):
        self.api_token = api_token or os.environ.get('HF_API_TOKEN')
        self._api_disabled = False  # Circuit breaker: set True after 403/auth failure
        if not self.api_token:
            logger.warning("No HF_API_TOKEN found — will use local models.")

        if cache_dir is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            cache_dir = str(project_root / 'Chelsea_clients' / 'hf_cache')
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

        self.session = requests.Session()
        if self.api_token:
            self.session.headers['Authorization'] = f'Bearer {self.api_token}'

    # ------------------------------------------------------------------
    # Caching (mirrors ProfileResearchCache pattern in ai_research.py)
    # ------------------------------------------------------------------

    def _cache_key(self, model: str, text: str) -> str:
        """MD5 hash of model + text for cache filename."""
        content = f"{model}:{text}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def _cache_get(self, key: str) -> Optional[any]:
        """Read from file cache. Returns None on miss."""
        path = os.path.join(self.cache_dir, f"{key}.json")
        if not os.path.exists(path):
            return None

        # 30-day TTL (same as research cache)
        age_days = (time.time() - os.path.getmtime(path)) / 86400
        if age_days > 30:
            return None

        try:
            with open(path, 'r') as f:
                _metrics['cache_hits'] += 1
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def _cache_set(self, key: str, value: any) -> None:
        """Write to file cache."""
        path = os.path.join(self.cache_dir, f"{key}.json")
        try:
            with open(path, 'w') as f:
                json.dump(value, f)
        except OSError as e:
            logger.warning(f"Cache write failed: {e}")

    # ------------------------------------------------------------------
    # API calls with retry (mirrors ai_research.py _call_claude pattern)
    # ------------------------------------------------------------------

    def _call_api(self, url: str, payload: dict, max_retries: int = 3) -> dict:
        """
        Make an API call with exponential backoff retry.

        Handles 503 (model loading), 429 (rate limit), and transient errors.
        Circuit-breaks on 401/403 to avoid repeated auth failures.
        """
        if self._api_disabled:
            return {}

        for attempt in range(max_retries):
            try:
                _metrics['api_calls'] += 1
                start = time.time()

                response = self.session.post(url, json=payload, timeout=60)
                latency_ms = (time.time() - start) * 1000
                _metrics['total_latency_ms'] += latency_ms

                if response.status_code == 200:
                    return response.json()

                # Auth failure — disable API for rest of session
                if response.status_code in (401, 403):
                    _metrics['api_errors'] += 1
                    logger.warning(f"HF API auth failed ({response.status_code}), "
                                   "switching to local model for remaining calls.")
                    self._api_disabled = True
                    return {}

                # Model still loading — wait and retry
                if response.status_code == 503:
                    wait_time = response.json().get('estimated_time', 20)
                    logger.info(f"Model loading, waiting {wait_time:.0f}s "
                                f"(attempt {attempt + 1}/{max_retries})")
                    time.sleep(min(wait_time, 60))
                    continue

                # Rate limited
                if response.status_code == 429:
                    wait_time = 2 ** (attempt + 1) * 5  # 10, 20, 40s
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue

                # Other errors
                _metrics['api_errors'] += 1
                logger.error(f"HF API error {response.status_code}: {response.text[:200]}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {}

            except requests.RequestException as e:
                _metrics['api_errors'] += 1
                logger.error(f"HF API request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {}

        return {}

    # ------------------------------------------------------------------
    # Public API: Embeddings
    # ------------------------------------------------------------------

    def _embed_local(self, text: str, model: str) -> Optional[list[float]]:
        """
        Embed text using a local sentence-transformers model.

        Falls back to this when the HF Inference API is unavailable (403, no token, etc.).
        Downloads the model on first call (~80MB for MiniLM-L6-v2, ~1.3GB for bge-large),
        then runs on CPU. Supports multiple models concurrently.
        """
        global _local_models
        try:
            if model not in _local_models:
                from sentence_transformers import SentenceTransformer
                logger.info(f"Loading local embedding model: {model}")
                _local_models[model] = SentenceTransformer(model)
                logger.info(f"Local model loaded: {model}")

            embedding = _local_models[model].encode(text, convert_to_numpy=True)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Local embedding failed: {e}")
            return None

    def embed(self, text: str, model: str = None) -> Optional[list[float]]:
        """
        Generate embedding vector for a single text string.

        Tries the HF Inference API first; falls back to local sentence-transformers
        if the API is unavailable (no token, 403, network error, etc.).

        Args:
            text: Input text (will be truncated to 512 tokens by model)
            model: HF model ID (default: sentence-transformers/all-MiniLM-L6-v2)

        Returns:
            List of floats (384-dim for MiniLM, 1024-dim for bge-large), or None on failure.
        """
        if not text or not text.strip():
            return None

        model = model or DEFAULT_EMBEDDING_MODEL

        # Check cache
        key = self._cache_key(model, text)
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        _metrics['cache_misses'] += 1

        # Try API first, fall back to local
        embedding = None
        if self.api_token:
            url = f"{HF_INFERENCE_URL}/{model}/pipeline/feature-extraction"
            result = self._call_api(url, {
                "inputs": text,
                "options": {"wait_for_model": True}
            })
            if result:
                embedding = self._extract_embedding(result)

        # Fallback: local sentence-transformers
        if embedding is None:
            embedding = self._embed_local(text, model)

        if embedding:
            self._cache_set(key, embedding)

        return embedding

    def embed_batch(self, texts: list[str], model: str = None,
                    batch_size: int = 32) -> list[Optional[list[float]]]:
        """
        Batch embedding for multiple texts.

        Processes in chunks of batch_size to respect API limits.
        Returns list of embeddings (None for failed/empty texts).
        """
        model = model or DEFAULT_EMBEDDING_MODEL
        results = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_results = []

            for text in batch:
                embedding = self.embed(text, model=model)
                batch_results.append(embedding)

            results.extend(batch_results)

            # Rate limit courtesy pause between batches
            if i + batch_size < len(texts):
                time.sleep(0.5)

        return results

    def _extract_embedding(self, api_response) -> Optional[list[float]]:
        """
        Extract a single embedding vector from the HF API response.

        sentence-transformers models return [[token_embeddings]] where each
        token has a vector. We mean-pool across tokens.
        """
        try:
            if isinstance(api_response, list):
                if isinstance(api_response[0], list):
                    if isinstance(api_response[0][0], list):
                        # Shape: [[token_vectors]] → mean pool
                        import numpy as np
                        token_embeddings = np.array(api_response[0])
                        return np.mean(token_embeddings, axis=0).tolist()
                    else:
                        # Shape: [embedding_vector] — already pooled
                        return api_response[0]
                else:
                    # Direct vector
                    return api_response
            return None
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Failed to extract embedding: {e}")
            return None

    # ------------------------------------------------------------------
    # Public API: Zero-Shot Classification
    # ------------------------------------------------------------------

    def classify_zero_shot(
        self,
        text: str,
        labels: list[str],
        multi_label: bool = True,
        model: str = None,
    ) -> dict[str, float]:
        """
        Zero-shot classification with candidate labels.

        Args:
            text: Text to classify
            labels: Candidate label strings
            multi_label: If True, labels are independent (can assign multiple)
            model: HF model ID (default: DeBERTa-v3-large)

        Returns:
            Dict of {label: confidence_score}, sorted by score descending.
            Empty dict on failure.
        """
        if not text or not text.strip():
            return {}

        model = model or DEFAULT_CLASSIFICATION_MODEL

        # Check cache
        key = self._cache_key(model, f"{text}|{'|'.join(sorted(labels))}|{multi_label}")
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        _metrics['cache_misses'] += 1

        url = f"{HF_INFERENCE_URL}/{model}/pipeline/zero-shot-classification"
        result = self._call_api(url, {
            "inputs": text,
            "parameters": {
                "candidate_labels": labels,
                "multi_label": multi_label,
            }
        })

        if not result or 'labels' not in result:
            return {}

        scores = dict(zip(result['labels'], result['scores']))
        self._cache_set(key, scores)
        return scores

    # ------------------------------------------------------------------
    # Public API: Named Entity Recognition
    # ------------------------------------------------------------------

    def extract_entities(self, text: str, model: str = None) -> list[dict]:
        """
        Named entity recognition on input text.

        Args:
            text: Input text for NER
            model: HF model ID (default: dslim/bert-base-NER)

        Returns:
            List of entity dicts: [{entity_group, word, score, start, end}]
            Empty list on failure.
        """
        if not text or not text.strip():
            return []

        model = model or DEFAULT_NER_MODEL

        url = f"{HF_INFERENCE_URL}/{model}/pipeline/token-classification"
        result = self._call_api(url, {
            "inputs": text,
            "parameters": {"aggregation_strategy": "simple"}
        })

        if not result or not isinstance(result, list):
            return []

        return result
