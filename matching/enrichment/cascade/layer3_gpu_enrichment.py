"""
Layer 3: GPU AI Enrichment — configurable LLM endpoint.

Uses ClaudeClient with LLM_BASE_URL + LLM_MODEL env vars to route to
Vast.ai GPU (Qwen3-30B-A3B + vLLM) or OpenRouter.

Reuses research_and_enrich_profile() from ai_research.py.
Source tag: "ai_research", priority 40.
JSONL checkpoint per batch.

Skip list: profiles that failed verification 3+ times get cascade_skip flag.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras

from matching.enrichment.cascade.checkpoint import CascadeCheckpoint

logger = logging.getLogger(__name__)


# ---------- Result dataclass ----------

@dataclass
class Layer3Result:
    """Summary of a Layer 3 run."""

    profiles_attempted: int = 0
    profiles_enriched: int = 0
    profiles_skipped: int = 0
    profiles_error: int = 0
    fields_filled: dict = field(default_factory=dict)
    enriched_ids: list = field(default_factory=list)
    json_parse_success: int = 0
    json_parse_fail: int = 0
    total_cost: float = 0.0
    model_used: str = ""
    runtime_seconds: float = 0.0


# ---------- DB helpers ----------

def _get_conn():
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(db_url, options="-c statement_timeout=120000")


def _fetch_profiles_for_enrichment(
    profile_ids: list[str] | None = None,
    tier_filter: set[str] | None = None,
    min_score: float = 50.0,
    limit: int | None = None,
    exclude_ids: set[str] | None = None,
) -> list[dict]:
    """Fetch profiles that need AI enrichment."""
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    conditions = []
    params: list = []

    if profile_ids:
        conditions.append("id = ANY(%s::uuid[])")
        params.append(profile_ids)
    else:
        # Default: profiles without AI enrichment
        conditions.append("(seeking IS NULL OR seeking = '')")
        conditions.append("(who_you_serve IS NULL OR who_you_serve = '')")

    if tier_filter:
        conditions.append("jv_tier = ANY(%s)")
        params.append(list(tier_filter))

    if min_score > 0:
        conditions.append("jv_readiness_score >= %s")
        params.append(min_score)

    # Skip profiles flagged for cascade skip
    conditions.append(
        "(enrichment_metadata->>'cascade_skip' IS NULL "
        "OR enrichment_metadata->>'cascade_skip' = 'false')"
    )

    where = " AND ".join(conditions) if conditions else "TRUE"
    sql = f"""
        SELECT id, name, email, phone, website, linkedin, company,
               bio, niche, what_you_do, who_you_serve, seeking, offering,
               audience_type, tags, booking_link, revenue_tier,
               list_size, social_reach, content_platforms,
               jv_tier, jv_readiness_score, enrichment_metadata
        FROM profiles
        WHERE {where}
        ORDER BY jv_readiness_score DESC NULLS LAST
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    if exclude_ids:
        rows = [r for r in rows if str(r["id"]) not in exclude_ids]

    return rows


def _write_enrichment(
    profile_id: str,
    enriched_data: dict,
    existing: dict,
    dry_run: bool = False,
) -> list[str]:
    """Write AI-enriched fields to DB with source priority checking."""
    from matching.enrichment.constants import SOURCE_PRIORITY

    ai_priority = SOURCE_PRIORITY.get("ai_research", 40)

    fields_to_write = {}
    fields_written = []

    # Core AI fields
    ai_fields = [
        "seeking", "offering", "who_you_serve", "what_you_do",
        "niche", "audience_type", "bio", "business_focus",
        "jv_interest_type", "ideal_partner_description",
    ]

    for fld in ai_fields:
        new_val = (enriched_data.get(fld) or "").strip()
        if not new_val:
            continue

        existing_val = (existing.get(fld) or "").strip()

        # Check source priority — don't overwrite higher-priority data
        existing_meta = existing.get("enrichment_metadata") or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}

        field_meta = (existing_meta.get("field_meta") or {}).get(fld, {})
        existing_source = field_meta.get("source", "unknown")
        existing_priority = SOURCE_PRIORITY.get(existing_source, 0)

        if existing_val and existing_priority >= ai_priority:
            continue  # Don't overwrite higher-priority data

        fields_to_write[fld] = new_val
        fields_written.append(fld)

    # Extended signal fields (tags, revenue_tier, list_size)
    if enriched_data.get("tags") and not existing.get("tags"):
        tags = enriched_data["tags"]
        if isinstance(tags, list):
            fields_to_write["tags"] = tags
            fields_written.append("tags")

    if enriched_data.get("revenue_tier") and not existing.get("revenue_tier"):
        fields_to_write["revenue_tier"] = enriched_data["revenue_tier"]
        fields_written.append("revenue_tier")

    if not fields_to_write:
        return []

    if dry_run:
        return fields_written

    conn = _get_conn()
    cur = conn.cursor()
    try:
        set_parts = []
        params: list = []

        for fld, val in fields_to_write.items():
            if fld == "tags":
                set_parts.append(f"{fld} = %s::jsonb")
                params.append(json.dumps(val))
            else:
                set_parts.append(f"{fld} = %s")
                params.append(val)

        # Update enrichment_metadata
        existing_meta = existing.get("enrichment_metadata") or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}
        meta = dict(existing_meta)
        meta["last_ai_enrichment"] = datetime.now().isoformat()
        meta["ai_model"] = os.environ.get("LLM_MODEL", "claude-sonnet")
        field_meta = meta.get("field_meta", {})
        for f in fields_written:
            field_meta[f] = {
                "source": "ai_research",
                "updated_at": datetime.now().isoformat(),
            }
        meta["field_meta"] = field_meta

        set_parts.append("enrichment_metadata = %s::jsonb")
        params.append(json.dumps(meta, default=str))
        set_parts.append("updated_at = NOW()")

        params.append(profile_id)
        sql = f"UPDATE profiles SET {', '.join(set_parts)} WHERE id = %s::uuid"
        cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

    return fields_written


def _mark_cascade_skip(profile_id: str) -> None:
    """Flag a profile to be skipped in future cascade runs."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE profiles
            SET enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb)
                || '{"cascade_skip": true}'::jsonb,
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (profile_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


# ---------- Main Layer 3 entry point ----------

class Layer3GpuEnrichment:
    """Layer 3: AI enrichment using configurable LLM endpoint."""

    def __init__(
        self,
        tier_filter: set[str] | None = None,
        min_score: float = 50.0,
        batch_size: int = 50,
        limit: int | None = None,
        dry_run: bool = False,
        checkpoint: CascadeCheckpoint | None = None,
    ):
        self.tier_filter = tier_filter
        self.min_score = min_score
        self.batch_size = batch_size
        self.limit = limit
        self.dry_run = dry_run
        self.checkpoint = checkpoint or CascadeCheckpoint(layer=3)

    def run(
        self,
        profile_ids: list[str] | None = None,
    ) -> Layer3Result:
        """Execute Layer 3 AI enrichment.

        Parameters
        ----------
        profile_ids:
            Specific profile IDs from Layer 2.
            If None, fetches profiles matching tier/score filters.
        """
        start = time.time()
        result = Layer3Result()
        result.model_used = os.environ.get("LLM_MODEL", "claude-sonnet")

        already_done = self.checkpoint.get_processed_ids()
        logger.info("Layer 3: %d profiles already processed (checkpoint)", len(already_done))

        profiles = _fetch_profiles_for_enrichment(
            profile_ids=profile_ids,
            tier_filter=self.tier_filter,
            min_score=self.min_score,
            limit=self.limit,
            exclude_ids=already_done,
        )
        result.profiles_attempted = len(profiles)
        logger.info("Layer 3: %d profiles to enrich with %s", len(profiles), result.model_used)

        if not profiles:
            result.runtime_seconds = time.time() - start
            return result

        # Import the enrichment function
        from matching.enrichment.ai_research import research_and_enrich_profile

        for i, profile in enumerate(profiles, 1):
            pid = str(profile["id"])
            name = profile.get("name", "")
            website = profile.get("website", "")

            try:
                enriched_data, was_researched = research_and_enrich_profile(
                    name=name,
                    website=website,
                    existing_data=profile,
                    use_cache=True,
                    force_research=True,
                    linkedin=profile.get("linkedin"),
                    company=profile.get("company"),
                    fill_only=False,
                )

                if not was_researched and not enriched_data:
                    result.profiles_skipped += 1
                    self.checkpoint.mark_processed(pid, "skipped")
                    continue

                result.json_parse_success += 1

                # Write to DB
                written = _write_enrichment(pid, enriched_data, profile, dry_run=self.dry_run)

                if written:
                    result.profiles_enriched += 1
                    result.enriched_ids.append(pid)
                    for f in written:
                        result.fields_filled[f] = result.fields_filled.get(f, 0) + 1
                    self.checkpoint.mark_processed(pid, "success", written)
                else:
                    result.profiles_skipped += 1
                    self.checkpoint.mark_processed(pid, "skipped")

            except Exception as e:
                logger.error("Layer 3 enrichment error for %s (%s): %s", name, pid, e)
                result.profiles_error += 1

                # Check for JSON parse failures
                if "json" in str(e).lower() or "parse" in str(e).lower():
                    result.json_parse_fail += 1

                self.checkpoint.mark_processed(pid, "error", error=str(e))

                # Check if profile should be skip-listed (3+ failures)
                self._check_skip_threshold(pid)

            if i % 50 == 0:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                logger.info(
                    "Layer 3 progress: %d/%d (%.2f/sec) enriched=%d err=%d",
                    i, len(profiles), rate,
                    result.profiles_enriched, result.profiles_error,
                )

        # Estimate cost
        per_profile_cost = 0.015 if "claude" in result.model_used.lower() else 0.0005
        result.total_cost = result.profiles_enriched * per_profile_cost

        result.runtime_seconds = time.time() - start

        parse_rate = (
            result.json_parse_success / max(result.json_parse_success + result.json_parse_fail, 1)
            * 100
        )
        logger.info(
            "Layer 3 complete: %d attempted, %d enriched, %d skipped, %d errors, "
            "JSON parse %.1f%%, $%.2f cost, %.1fs",
            result.profiles_attempted,
            result.profiles_enriched,
            result.profiles_skipped,
            result.profiles_error,
            parse_rate,
            result.total_cost,
            result.runtime_seconds,
        )

        return result

    def _check_skip_threshold(self, profile_id: str) -> None:
        """Check if a profile has failed 3+ times and should be skip-listed."""
        # Count errors in checkpoint
        error_count = 0
        cp_dir = self.checkpoint.filepath.parent
        pattern = f"cascade_L3_*.jsonl"
        for cp_file in cp_dir.glob(pattern):
            try:
                with open(cp_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            if data.get("profile_id") == profile_id and data.get("status") == "error":
                                error_count += 1
                        except json.JSONDecodeError:
                            continue
            except Exception:
                continue

        if error_count >= 3:
            logger.warning("Profile %s failed 3+ times — marking cascade_skip", profile_id)
            _mark_cascade_skip(profile_id)
