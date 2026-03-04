"""
Layer 4: Claude Conflict Resolution — Claude-only, when AI ≠ existing data.

Detects conflicts where new AI value ≠ existing value AND both are non-empty.
Uses ClaudeClient with forced Claude Sonnet (ignores LLM_MODEL override)
to judge which value is correct.

Budget: ~$15-20/mo via cost_guard.
Expected: 5-10% of Layer 3 profiles have conflicts.
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

logger = logging.getLogger(__name__)

# Fields worth judging (high-signal fields only)
JUDGEABLE_FIELDS = [
    "seeking", "offering", "who_you_serve", "what_you_do",
    "niche", "audience_type", "business_focus",
]

JUDGE_PROMPT_TEMPLATE = """You are a data quality judge for a JV (Joint Venture) partner matching platform.

Two sources disagree about a profile field. Decide which value is more accurate for matching purposes.

Profile: {name}
Website: {website}
Field: {field_name}

Value A (existing data): {value_a}
Value B (new AI extraction): {value_b}

Rules:
- Choose the value that is more specific and actionable for JV partner matching.
- If both are roughly equivalent, prefer the existing data (Value A).
- If one is clearly more detailed or accurate, choose it regardless of source.
- If both are wrong or nonsensical, respond "NEITHER".

Respond with ONLY one of: "A", "B", or "NEITHER" followed by a brief reason.
Format: VERDICT: [A/B/NEITHER] | REASON: [one sentence]"""


# ---------- Result dataclass ----------

@dataclass
class Layer4Result:
    """Summary of a Layer 4 run."""

    profiles_checked: int = 0
    conflicts_found: int = 0
    conflicts_resolved: int = 0
    verdicts: dict = field(default_factory=dict)  # {"A": n, "B": n, "NEITHER": n}
    fields_updated: int = 0
    cost: float = 0.0
    runtime_seconds: float = 0.0


# ---------- DB helpers ----------

def _get_conn():
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(db_url, options="-c statement_timeout=120000")


def _fetch_enriched_profiles(
    profile_ids: list[str],
) -> list[dict]:
    """Fetch recently enriched profiles that may have conflicts."""
    if not profile_ids:
        return []

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT id, name, website, enrichment_metadata,
               seeking, offering, who_you_serve, what_you_do,
               niche, audience_type, business_focus
        FROM profiles
        WHERE id = ANY(%s::uuid[])
        """,
        (profile_ids,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def _apply_verdict(
    profile_id: str,
    field_name: str,
    chosen_value: str,
    verdict: str,
    reason: str,
) -> None:
    """Apply a judge verdict to the database."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        # Update the field
        cur.execute(
            f"UPDATE profiles SET {field_name} = %s, updated_at = NOW() WHERE id = %s::uuid",
            (chosen_value, profile_id),
        )

        # Log the verdict in enrichment_metadata
        cur.execute(
            """
            UPDATE profiles
            SET enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb)
                || jsonb_build_object(
                    'judge_verdicts',
                    COALESCE(enrichment_metadata->'judge_verdicts', '[]'::jsonb)
                    || %s::jsonb
                )
            WHERE id = %s::uuid
            """,
            (
                json.dumps([{
                    "field": field_name,
                    "verdict": verdict,
                    "reason": reason,
                    "judged_at": datetime.now().isoformat(),
                }]),
                profile_id,
            ),
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Failed to apply verdict for %s.%s: %s", profile_id, field_name, e)
    finally:
        cur.close()
        conn.close()


# ---------- Conflict detection ----------

def _detect_conflicts(profile: dict) -> list[dict]:
    """Detect fields where AI enrichment disagrees with existing data.

    A conflict exists when:
    - The field has a non-empty existing value
    - The field_meta shows it was recently updated by ai_research
    - The new value differs meaningfully from the old value
    """
    conflicts = []

    meta = profile.get("enrichment_metadata") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            meta = {}

    field_meta = meta.get("field_meta", {})

    for fld in JUDGEABLE_FIELDS:
        current_val = (profile.get(fld) or "").strip()
        if not current_val:
            continue

        fld_info = field_meta.get(fld, {})
        source = fld_info.get("source", "")

        # Only conflict if this field was recently written by AI
        if source != "ai_research":
            continue

        # Check if there was a pre-existing value that was overwritten
        # We detect this via the _pre_enrichment snapshot in metadata
        pre_enrichment = meta.get("pre_enrichment", {})
        old_val = (pre_enrichment.get(fld) or "").strip()

        if old_val and old_val != current_val:
            conflicts.append({
                "field": fld,
                "value_a": old_val,  # existing (pre-enrichment)
                "value_b": current_val,  # new AI value
            })

    return conflicts


# ---------- Main Layer 4 entry point ----------

class Layer4ClaudeJudge:
    """Layer 4: Claude-only conflict resolution for AI vs existing data."""

    def __init__(
        self,
        dry_run: bool = False,
    ):
        self.dry_run = dry_run

    def run(
        self,
        enriched_ids: list[str],
    ) -> Layer4Result:
        """Execute Layer 4 conflict resolution.

        Parameters
        ----------
        enriched_ids:
            Profile IDs from Layer 3 that were AI-enriched.
        """
        start = time.time()
        result = Layer4Result()
        result.verdicts = {"A": 0, "B": 0, "NEITHER": 0}

        if not enriched_ids:
            logger.info("Layer 4: no enriched IDs to check")
            result.runtime_seconds = time.time() - start
            return result

        profiles = _fetch_enriched_profiles(enriched_ids)
        result.profiles_checked = len(profiles)
        logger.info("Layer 4: checking %d enriched profiles for conflicts", len(profiles))

        # Get Claude client — force Claude Sonnet (ignore LLM_MODEL override)
        saved_model = os.environ.pop("LLM_MODEL", None)
        saved_base_url = os.environ.pop("LLM_BASE_URL", None)
        try:
            from matching.enrichment.claude_client import ClaudeClient
            client = ClaudeClient()
        finally:
            if saved_model is not None:
                os.environ["LLM_MODEL"] = saved_model
            if saved_base_url is not None:
                os.environ["LLM_BASE_URL"] = saved_base_url

        if not client.is_available():
            logger.warning("Layer 4: Claude client not available — skipping conflict resolution")
            result.runtime_seconds = time.time() - start
            return result

        for profile in profiles:
            pid = str(profile["id"])
            name = profile.get("name", "")
            website = profile.get("website", "")

            conflicts = _detect_conflicts(profile)
            if not conflicts:
                continue

            result.conflicts_found += len(conflicts)

            for conflict in conflicts:
                fld = conflict["field"]
                prompt = JUDGE_PROMPT_TEMPLATE.format(
                    name=name,
                    website=website,
                    field_name=fld,
                    value_a=conflict["value_a"],
                    value_b=conflict["value_b"],
                )

                try:
                    response = client.call(prompt)
                    if not response:
                        continue

                    verdict, reason = _parse_verdict(response)
                    result.verdicts[verdict] = result.verdicts.get(verdict, 0) + 1
                    result.conflicts_resolved += 1
                    result.cost += 0.005  # ~$0.005 per judge call

                    if verdict == "A":
                        # Revert to existing value
                        chosen = conflict["value_a"]
                    elif verdict == "B":
                        # Keep AI value (already in DB)
                        chosen = conflict["value_b"]
                    else:
                        # NEITHER — clear the field
                        chosen = ""

                    if not self.dry_run and verdict != "B":
                        _apply_verdict(pid, fld, chosen, verdict, reason)
                        result.fields_updated += 1
                    elif verdict != "B":
                        logger.info(
                            "DRY RUN: would update %s.%s to %s (%s)",
                            name, fld, verdict, reason,
                        )

                except Exception as e:
                    logger.error("Layer 4 judge error for %s.%s: %s", name, fld, e)

        result.runtime_seconds = time.time() - start

        logger.info(
            "Layer 4 complete: %d checked, %d conflicts found, %d resolved, "
            "verdicts=%s, %d fields updated, $%.3f cost, %.1fs",
            result.profiles_checked,
            result.conflicts_found,
            result.conflicts_resolved,
            result.verdicts,
            result.fields_updated,
            result.cost,
            result.runtime_seconds,
        )

        return result


def _parse_verdict(response: str) -> tuple[str, str]:
    """Parse a judge response into (verdict, reason)."""
    response = response.strip()

    verdict = "NEITHER"
    reason = response

    # Try to parse "VERDICT: X | REASON: Y" format
    if "VERDICT:" in response.upper():
        parts = response.split("|", 1)
        verdict_part = parts[0].upper()
        if "A" in verdict_part.split("VERDICT:")[-1].strip()[:5]:
            verdict = "A"
        elif "B" in verdict_part.split("VERDICT:")[-1].strip()[:5]:
            verdict = "B"
        else:
            verdict = "NEITHER"

        if len(parts) > 1 and "REASON:" in parts[1].upper():
            reason = parts[1].split(":", 1)[-1].strip()
        elif len(parts) > 1:
            reason = parts[1].strip()
    elif response.upper().startswith("A"):
        verdict = "A"
    elif response.upper().startswith("B"):
        verdict = "B"

    return verdict, reason
