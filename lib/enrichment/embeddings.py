"""
Profile Embedding Service

Generates, stores, and queries semantic embeddings for SupabaseProfile text fields.
Replaces the word-overlap Jaccard similarity in matching/services.py with cosine similarity.

Usage:
    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.embeddings import ProfileEmbeddingService

    service = ProfileEmbeddingService(HFClient())

    # Single profile
    embeddings = service.embed_profile(profile_dict)

    # Similarity between two profiles
    sim = service.cosine_similarity(emb_a, emb_b)  # 0.0 - 1.0

Integration points:
    - scripts/automated_enrichment_pipeline_safe.py: call after consolidate_to_supabase_batch()
    - matching/services.py: replace _text_overlap_score() with cosine similarity
    - matching/management/commands/backfill_hf_enrichment.py: batch backfill existing profiles
"""

import logging
import math
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger('enrichment.hf.embeddings')

# Fields to embed — these are the text fields used in synergy scoring
EMBEDDING_FIELDS = ['seeking', 'offering', 'who_you_serve', 'what_you_do']


class ProfileEmbeddingService:
    """
    Generates and manages semantic embeddings for profile text fields.

    Embeddings enable cosine similarity matching, replacing the word-level
    Jaccard overlap in SupabaseMatchScoringService._text_overlap_score().
    """

    def __init__(self, hf_client, model: str = None):
        """
        Args:
            hf_client: HFClient instance (from lib/enrichment/hf_client.py)
            model: Override embedding model (default from env/HFClient)
        """
        self.hf = hf_client
        self.model = model

    def embed_profile(self, profile: dict) -> dict[str, list[float]]:
        """
        Generate embeddings for all text fields of a single profile.

        Args:
            profile: Dict with at least some of EMBEDDING_FIELDS populated.
                     Can be a SupabaseProfile.__dict__ or a raw Supabase row.

        Returns:
            Dict mapping field_name → embedding_vector.
            Only includes fields that have non-empty text.
        """
        embeddings = {}
        for field in EMBEDDING_FIELDS:
            text = profile.get(field, '')
            if text and isinstance(text, str) and len(text.strip()) >= 5:
                emb = self.hf.embed(text.strip(), model=self.model)
                if emb:
                    embeddings[f'embedding_{field}'] = emb
                    logger.debug(f"Embedded {field} ({len(text)} chars) → {len(emb)}-dim vector")
                else:
                    logger.warning(f"Embedding failed for {field} on profile {profile.get('name', '?')}")
        return embeddings

    def embed_profiles_batch(self, profiles: list[dict]) -> list[dict]:
        """
        Batch embed multiple profiles.

        Returns list of embedding dicts, one per profile.
        Logs progress every 50 profiles.
        """
        results = []
        total = len(profiles)

        for i, profile in enumerate(profiles):
            emb = self.embed_profile(profile)
            results.append(emb)

            if (i + 1) % 50 == 0 or i == total - 1:
                logger.info(f"Embedding progress: {i + 1}/{total} profiles")

        return results

    def store_embeddings(self, supabase_client, profile_id: str,
                         embeddings: dict[str, list[float]]) -> bool:
        """
        Write embeddings to the Supabase profiles table.

        Uses the same Supabase client pattern as consolidate_to_supabase_batch()
        in automated_enrichment_pipeline_safe.py.

        Args:
            supabase_client: Initialized Supabase client
            profile_id: UUID of the profile row
            embeddings: Output of embed_profile()

        Returns:
            True on success, False on failure.
        """
        if not embeddings:
            return False

        update_data = {
            'embeddings_model': self.model or 'BAAI/bge-large-en-v1.5',
            'embeddings_updated_at': datetime.now(timezone.utc).isoformat(),
        }

        # Convert embedding lists to pgvector format: "[0.1,0.2,...]"
        for field_name, vector in embeddings.items():
            update_data[field_name] = f"[{','.join(str(v) for v in vector)}]"

        try:
            supabase_client.table('profiles').update(update_data).eq('id', profile_id).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to store embeddings for {profile_id}: {e}")
            return False

    @staticmethod
    def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
        """
        Compute cosine similarity between two vectors.

        Returns:
            Float between 0.0 and 1.0 (clamped — raw cosine can be negative
            but embedding models produce non-negative similarities for related texts).

        Note:
            Uses pure Python to avoid numpy dependency in matching hot path.
            For batch operations, use numpy instead.
        """
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0

        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        similarity = dot / (norm_a * norm_b)
        return max(0.0, min(1.0, similarity))

    def find_similar_profiles(
        self,
        supabase_client,
        profile_id: str,
        field: str = 'seeking',
        limit: int = 20,
    ) -> list[tuple[str, float]]:
        """
        Find most similar profiles using pgvector cosine distance.

        Uses Supabase's pgvector support for efficient nearest-neighbor search.
        Requires the ivfflat index created in migration 0013.

        Args:
            supabase_client: Initialized Supabase client
            profile_id: Source profile UUID
            field: Which embedding field to compare (default: 'seeking')
            limit: Max results to return

        Returns:
            List of (profile_id, similarity_score) tuples, sorted by similarity descending.
        """
        embedding_field = f'embedding_{field}'

        # Fetch the source profile's embedding
        # Note: pgvector similarity queries require raw SQL via Supabase RPC
        # This is a placeholder — actual implementation depends on your Supabase RPC setup
        try:
            result = supabase_client.rpc('find_similar_profiles', {
                'source_profile_id': profile_id,
                'embedding_field': embedding_field,
                'match_limit': limit,
            }).execute()

            if result.data:
                return [(row['id'], row['similarity']) for row in result.data]
            return []
        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return []


# SQL for Supabase RPC function (run once in SQL editor):
SIMILARITY_SEARCH_SQL = """
-- Create this function in Supabase SQL Editor for pgvector similarity search
CREATE OR REPLACE FUNCTION find_similar_profiles(
    source_profile_id uuid,
    embedding_field text,
    match_limit int DEFAULT 20
)
RETURNS TABLE(id uuid, similarity float)
LANGUAGE plpgsql AS $$
DECLARE
    source_embedding vector(1024);
BEGIN
    -- Dynamic SQL to handle different embedding fields
    EXECUTE format(
        'SELECT %I FROM profiles WHERE id = $1',
        embedding_field
    ) INTO source_embedding USING source_profile_id;

    IF source_embedding IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY EXECUTE format(
        'SELECT p.id, 1 - (%I <=> $1) AS similarity
         FROM profiles p
         WHERE p.id != $2
           AND %I IS NOT NULL
         ORDER BY %I <=> $1
         LIMIT $3',
        embedding_field, embedding_field, embedding_field
    ) USING source_embedding, source_profile_id, match_limit;
END;
$$;
"""
