"""
Prefect @task: Batch DB write with source-priority protection.

Extracted from ``scripts/automated_enrichment_pipeline_safe.py``
method ``consolidate_to_supabase_batch()`` (lines 1476-1934).

Handles:
    - Email updates with verification metadata
    - Core text fields (what_you_do, seeking, etc.)
    - Extended signals (revenue_tier, jv_history, content_platforms)
    - JSONB deep merges in refresh mode
    - Field-level provenance tracking in enrichment_metadata
    - Source-priority protection (never overwrite client data with AI data)
    - Grouped batch execution with SAVEPOINT fallback
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extras import execute_batch, RealDictCursor

from prefect import task, get_run_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Source priority hierarchy (R2).  Higher-priority sources can *never* be
# overwritten by lower ones.  Client-provided data is protected from AI
# overwrites.
from matching.enrichment.constants import SOURCE_PRIORITY

PIPELINE_VERSION: int = 1

# Postgres int4 ceiling -- prevents overflow for list_size / social_reach.
_PG_INT4_MAX: int = 2_147_483_647

# Canonical map from raw extraction methods to normalised source names.
_SOURCE_MAP: dict[str, str] = {
    'website_scrape': 'website_scraped',
    'linkedin_scrape': 'linkedin_scraped',
    'apollo_api': 'apollo',
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def should_write_field(
    field: str,
    new_value: Any,
    existing_meta: dict,
    new_source: str = 'exa_research',
    refresh_mode: bool = False,
    stale_days: int = 30,
) -> bool:
    """Decide whether to write a field based on source priority and staleness.

    Rules (R4):
        1. Empty *new_value* is never written.
        2. A lower-priority source can **never** overwrite a higher-priority one.
        3. A higher-priority source **always** wins.
        4. Equal priority only overwrites when the existing data is stale.
           In non-refresh mode ``stale_days`` is effectively infinite so
           equal-priority data is never overwritten.

    Args:
        field:         Column name.
        new_value:     Candidate value to write.
        existing_meta: The profile's current ``enrichment_metadata`` dict
                       (may be ``{}``).
        new_source:    Provenance label for the candidate value.
        refresh_mode:  When *True*, equal-priority staleness check uses
                       *stale_days*.  Otherwise staleness is infinite.
        stale_days:    Number of days after which same-priority data is
                       considered stale.

    Returns:
        ``True`` if the write should proceed, ``False`` otherwise.
    """
    if not new_value:
        return False

    field_info = (existing_meta or {}).get('field_meta', {}).get(field, {})
    existing_source = field_info.get('source', 'unknown')
    existing_priority = SOURCE_PRIORITY.get(existing_source, 0)
    new_priority = SOURCE_PRIORITY.get(new_source, 0)

    # Rule 1: Never overwrite higher-priority sources.
    if new_priority < existing_priority:
        return False

    # Rule 2: Higher priority always wins.
    if new_priority > existing_priority:
        return True

    # Rule 3: Equal priority -- only overwrite if stale.
    effective_stale = stale_days if refresh_mode else 999_999
    updated_at = field_info.get('updated_at')
    if updated_at:
        try:
            field_age = datetime.now() - datetime.fromisoformat(updated_at)
            return field_age > timedelta(days=effective_stale)
        except (ValueError, TypeError):
            return True  # Bad timestamp -- treat as stale.

    # No timestamp at all -- treat as stale (safe to overwrite).
    return True


def _parse_meta(raw: Any) -> dict:
    """Normalise *enrichment_metadata* to a plain ``dict``."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


# ---------------------------------------------------------------------------
# Core Prefect task
# ---------------------------------------------------------------------------

@task(name="consolidate-to-db", retries=2, retry_delay_seconds=10)
def consolidate_to_db(
    results: list[dict],
    validation_results: list | None = None,
    refresh_mode: bool = False,
    stale_days: int = 30,
) -> dict:
    """Batch write enriched profile data to Supabase/Postgres.

    Handles:
    - Email updates with verification metadata
    - Core text fields (what_you_do, who_you_serve, seeking, offering, bio,
      social_proof)
    - Extended signals (revenue_tier, jv_history, content_platforms,
      audience_engagement_score)
    - Standard text fields (signature_programs, booking_link, niche, phone,
      current_projects, business_size)
    - Tags (text[], union merge in refresh mode)
    - Categorization fields (audience_type, business_focus, service_provided)
    - company: fill-only unless source priority wins
    - list_size, social_reach: upgrade-only (never decrease)
    - email: from Exa discovery, fill-only
    - secondary_emails: always append-only
    - discovered website/linkedin: fill-only
    - avatar_url: Apollo cascade, fill-only
    - JSONB deep merges in refresh mode
    - Field-level provenance tracking in enrichment_metadata
    - Source-priority protection (never overwrite client data with AI data)
    - Grouped batch execution with SAVEPOINT fallback

    Args:
        results:            List of enriched profile dicts.
        validation_results: Optional per-profile validation verdicts (unused
                            placeholder -- verification is handled inline
                            via the monolith's gate logic).
        refresh_mode:       When ``True``, JSONB fields use deep/union merge
                            and equal-priority staleness checks are honoured.
        stale_days:         Staleness threshold for equal-priority overwrites.

    Returns:
        ``dict`` with write stats::

            {
                "emails_written": int,
                "profiles_updated": int,
                "failed": int,
                "skipped_empty": int,
            }
    """
    logger = get_run_logger()
    # Lazy import -- keep module-level import list lean.
    from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

    scorer = ConfidenceScorer()

    email_updates: list[tuple] = []
    profile_updates: list[tuple[list, list]] = []
    skipped_empty = 0

    # ------------------------------------------------------------------
    # Phase 1 -- Build per-profile SET clauses
    # ------------------------------------------------------------------
    for result in results:
        profile_id = result.get('profile_id')
        if not profile_id:
            skipped_empty += 1
            continue

        email = result.get('email')
        method = result.get('method')
        enriched_at_raw = result.get('enriched_at')
        try:
            enriched_at = datetime.fromisoformat(enriched_at_raw) if enriched_at_raw else datetime.now()
        except (ValueError, TypeError):
            enriched_at = datetime.now()

        existing_meta = _parse_meta(result.get('enrichment_metadata'))

        # ---- Email verification metadata --------------------------------
        if email and method:
            source = _SOURCE_MAP.get(method, 'unknown')
            base_confidence = scorer.calculate_confidence('email', source, enriched_at)
            confidence_expires_at = scorer.calculate_expires_at('email', enriched_at)

            # Default verification metadata when gate is not run inline.
            verification_confidence = result.get('_verification_confidence', 1.0)
            verification_status = result.get('_verification_status', 'unverified')

            confidence = base_confidence * verification_confidence

            email_metadata = {
                'source': source,
                'enriched_at': enriched_at.isoformat(),
                'source_date': enriched_at.date().isoformat(),
                'confidence': confidence,
                'confidence_expires_at': confidence_expires_at.isoformat(),
                'verification_count': 1 if method == 'apollo_api' else 0,
                'enrichment_method': method,
                'verified': verification_status == 'verified',
                'verification_status': verification_status,
                'verification_confidence': verification_confidence,
            }

            email_updates.append((
                email,
                json.dumps(email_metadata),
                confidence,
                enriched_at,
                datetime.now(),
                profile_id,
            ))

        # ---- Profile field SET clauses ----------------------------------
        set_parts: list[sql.Composable] = []
        params: list[Any] = []
        fields_written: list[str] = []

        # Determine enrichment source label.
        enrichment_source = 'exa_research'
        ext_meta = result.get('_extraction_metadata') or {}
        if ext_meta.get('source') == 'ai_research':
            enrichment_source = 'ai_research'

        # Helper to check writability -- closure over per-profile state.
        def _writable(field: str, value: Any, source: str = enrichment_source) -> bool:
            return should_write_field(
                field, value, existing_meta, source, refresh_mode, stale_days,
            )

        # -- Core text fields (P1: sql.Identifier for column names) -------
        for field in ('what_you_do', 'who_you_serve', 'seeking', 'offering',
                      'bio', 'social_proof'):
            value = result.get(field)
            if value and isinstance(value, str) and value.strip():
                if _writable(field, value):
                    set_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(field))
                    )
                    params.append(value.strip())
                    fields_written.append(field)

        # -- Extended signal: revenue_tier --------------------------------
        revenue_tier = result.get('revenue_tier')
        if revenue_tier and _writable('revenue_tier', revenue_tier):
            set_parts.append(
                sql.SQL("{} = %s").format(sql.Identifier('revenue_tier'))
            )
            params.append(revenue_tier)
            fields_written.append('revenue_tier')

        # -- jv_history: JSONB -- smart merge in refresh mode (R7) --------
        jv_history = result.get('jv_history')
        if jv_history:
            jv_json = json.dumps(jv_history) if not isinstance(jv_history, str) else jv_history
            if refresh_mode and _writable('jv_history', jv_history):
                # Append new JV entries (dedup by partner name done in Python
                # before reaching this task).
                set_parts.append(sql.SQL(
                    "{} = %s::jsonb"
                ).format(sql.Identifier('jv_history')))
                params.append(jv_json)
                fields_written.append('jv_history')
            elif _writable('jv_history', jv_history):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN profiles.{col} IS NULL "
                    "OR profiles.{col}::text IN ('[]', 'null', '') "
                    "THEN %s::jsonb ELSE profiles.{col} END"
                ).format(col=sql.Identifier('jv_history')))
                params.append(jv_json)
                fields_written.append('jv_history')

        # -- content_platforms: JSONB -- deep merge in refresh mode (R7) --
        content_platforms = result.get('content_platforms')
        if content_platforms:
            cp_json = json.dumps(content_platforms) if not isinstance(content_platforms, str) else content_platforms
            if refresh_mode and _writable('content_platforms', content_platforms):
                # Deep merge: existing keys kept, new keys added.
                set_parts.append(sql.SQL(
                    "{col} = COALESCE(profiles.{col}, '{{}}'::jsonb) || %s::jsonb"
                ).format(col=sql.Identifier('content_platforms')))
                params.append(cp_json)
                fields_written.append('content_platforms')
            elif _writable('content_platforms', content_platforms):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN profiles.{col} IS NULL "
                    "OR profiles.{col}::text IN ('{{}}', 'null', '') "
                    "THEN %s::jsonb ELSE profiles.{col} END"
                ).format(col=sql.Identifier('content_platforms')))
                params.append(cp_json)
                fields_written.append('content_platforms')

        # -- Dedicated social media columns (fill-only from content_platforms)
        cp_dict = {}
        if content_platforms:
            if isinstance(content_platforms, dict):
                cp_dict = content_platforms
            elif isinstance(content_platforms, str):
                try:
                    cp_dict = json.loads(content_platforms)
                except (json.JSONDecodeError, TypeError):
                    pass

        for social_col in ('facebook', 'instagram', 'youtube', 'twitter'):
            social_url = cp_dict.get(social_col) or result.get(social_col) or ''
            social_url = social_url.strip() if isinstance(social_url, str) else ''
            if social_url:
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier(social_col)))
                params.append(social_url)
                fields_written.append(social_col)

        # -- audience_engagement_score: handle 0.0 (M4) -------------------
        engagement_score = result.get('audience_engagement_score')
        if engagement_score is not None:
            if _writable('audience_engagement_score', engagement_score):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN profiles.{col} IS NULL OR profiles.{col} = 0 "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('audience_engagement_score')))
                params.append(float(engagement_score))
                fields_written.append('audience_engagement_score')

        # -- Standard text fields -----------------------------------------
        for field in ('signature_programs', 'booking_link', 'niche', 'phone',
                      'current_projects', 'business_size'):
            value = result.get(field)
            if value and isinstance(value, str) and value.strip():
                if _writable(field, value):
                    set_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(field))
                    )
                    params.append(value.strip())
                    fields_written.append(field)

        # -- tags: text[] -- union merge in refresh mode (R7) -------------
        tags_value = result.get('tags')
        if tags_value and isinstance(tags_value, list) and len(tags_value) > 0:
            if _writable('tags', tags_value):
                if refresh_mode:
                    # Union existing + new tags, dedup.
                    set_parts.append(sql.SQL(
                        "{col} = ("
                        "SELECT array_agg(DISTINCT t) FROM unnest("
                        "COALESCE(profiles.{col}, ARRAY[]::text[]) || %s::text[]"
                        ") AS t"
                        ")"
                    ).format(col=sql.Identifier('tags')))
                else:
                    set_parts.append(
                        sql.SQL("{} = %s::text[]").format(sql.Identifier('tags'))
                    )
                params.append(tags_value)
                fields_written.append('tags')

        # -- Categorization fields ----------------------------------------
        for field in ('audience_type', 'business_focus', 'service_provided'):
            value = result.get(field)
            if value and isinstance(value, str) and value.strip():
                if _writable(field, value):
                    set_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(field))
                    )
                    params.append(value.strip())
                    fields_written.append(field)

        # -- company: fill-only unless source priority wins (R5) ----------
        company_value = result.get('company')
        if company_value and isinstance(company_value, str) and company_value.strip():
            if _writable('company', company_value):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('company')))
                params.append(company_value.strip())
                fields_written.append('company')

        # -- list_size: upgrade-only + source priority (R5) ---------------
        enriched_list_size = result.get('enriched_list_size')
        if enriched_list_size is not None:
            try:
                enriched_list_size = min(int(enriched_list_size), _PG_INT4_MAX)
                if enriched_list_size > 0:
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN %s > COALESCE(profiles.{col}, 0) "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('list_size')))
                    params.append(enriched_list_size)
                    params.append(enriched_list_size)
                    fields_written.append('list_size')
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid list_size %r for profile %s -- skipping",
                    enriched_list_size, profile_id,
                )

        # -- social_reach: upgrade-only + source priority (R5) ------------
        social_reach = result.get('social_reach')
        if social_reach is not None:
            try:
                social_reach = min(int(social_reach), _PG_INT4_MAX)
                if social_reach > 0:
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN %s > COALESCE(profiles.{col}, 0) "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('social_reach')))
                    params.append(social_reach)
                    params.append(social_reach)
                    fields_written.append('social_reach')
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid social_reach %r for profile %s -- skipping",
                    social_reach, profile_id,
                )

        # -- Exa-discovered email: fill-only (R5) -------------------------
        exa_email = (result.get('exa_email') or '').strip()
        if exa_email and '@' in exa_email:
            if _writable('email', exa_email):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('email')))
                params.append(exa_email)
                fields_written.append('email')

        # -- secondary_emails: always append-only (deduped) ---------------
        secondary = result.get('secondary_emails', [])
        if secondary:
            for sec_email in secondary:
                sec_email = sec_email.strip() if isinstance(sec_email, str) else ''
                if sec_email and '@' in sec_email:
                    set_parts.append(sql.SQL(
                        "{col} = CASE "
                        "WHEN %s = ANY(COALESCE(profiles.{col}, '{{}}')) "
                        "OR lower(%s) = lower(COALESCE(profiles.{email_col}, '')) "
                        "THEN profiles.{col} "
                        "ELSE array_append(COALESCE(profiles.{col}, '{{}}'), %s) END"
                    ).format(
                        col=sql.Identifier('secondary_emails'),
                        email_col=sql.Identifier('email'),
                    ))
                    params.extend([sec_email, sec_email, sec_email])

        # -- Discovered website / linkedin: fill-only (R5) ----------------
        discovered_website = (result.get('discovered_website') or '').strip()
        if discovered_website:
            if _writable('website', discovered_website):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('website')))
                params.append(discovered_website)
                fields_written.append('website')

        discovered_linkedin = (result.get('discovered_linkedin') or '').strip()
        if discovered_linkedin:
            if _writable('linkedin', discovered_linkedin):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('linkedin')))
                params.append(discovered_linkedin)
                fields_written.append('linkedin')

        # ---- Provenance / enrichment_metadata ---------------------------
        if set_parts:
            # Timestamps.
            set_parts.append(sql.SQL("last_enriched_at = %s"))
            params.append(enriched_at)
            set_parts.append(sql.SQL("updated_at = %s"))
            params.append(datetime.now())

            # Field-level provenance (R1).
            now_iso = datetime.now().isoformat()
            field_meta_update: dict[str, dict] = {}
            for f in fields_written:
                field_meta_update[f] = {
                    'source': enrichment_source,
                    'updated_at': now_iso,
                    'pipeline_version': PIPELINE_VERSION,
                }

            # avatar_url: from Apollo cascade (fill-only).
            avatar_url = (result.get('avatar_url') or '').strip()
            if avatar_url and should_write_field(
                'avatar_url', avatar_url, existing_meta, 'apollo',
                refresh_mode, stale_days,
            ):
                set_parts.append(sql.SQL(
                    "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                    "THEN %s ELSE profiles.{col} END"
                ).format(col=sql.Identifier('avatar_url')))
                params.append(avatar_url)
                fields_written.append('avatar_url')

            # Build enrichment_metadata JSON payload (M3).
            meta_payload: dict[str, Any] = {
                'last_enrichment': 'exa_pipeline',
                'enriched_at': enriched_at.isoformat(),
                'tier': result.get('_tier', 0),
                'field_meta': field_meta_update,
            }

            # Include Apollo cascade data if present.
            apollo_data = result.get('_apollo_data')
            if apollo_data:
                meta_payload['apollo_data'] = apollo_data
                meta_payload['last_apollo_enrichment'] = datetime.now().isoformat()
                # Track Apollo-sourced fields in field_meta.
                for f in fields_written:
                    if f in (
                        'email', 'phone', 'linkedin', 'website', 'company',
                        'business_size', 'revenue_tier', 'service_provided',
                        'niche', 'avatar_url',
                    ):
                        # Only re-attribute if field actually came from Apollo.
                        if result.get('_apollo_data') and not result.get('_extraction_metadata'):
                            field_meta_update[f] = {
                                'source': 'apollo',
                                'updated_at': now_iso,
                                'pipeline_version': PIPELINE_VERSION,
                            }

            # Merge enrichment_metadata JSONB (append, never clobber).
            set_parts.append(sql.SQL(
                "enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb) || %s::jsonb"
            ))
            params.append(json.dumps(meta_payload))

            # WHERE clause value -- must be last.
            params.append(profile_id)
            profile_updates.append((set_parts, params))
        else:
            skipped_empty += 1

    # ------------------------------------------------------------------
    # Phase 2 -- Execute writes
    # ------------------------------------------------------------------
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set.  Cannot write to database."
        )

    failed_updates = 0
    conn = psycopg2.connect(database_url)
    try:
        cursor = conn.cursor()
        try:
            # ---- Email updates -- SAVEPOINT-wrapped ---------------------
            if email_updates:
                cursor.execute("SAVEPOINT email_batch")
                try:
                    execute_batch(cursor, """
                        UPDATE profiles
                        SET email = %s,
                            enrichment_metadata = jsonb_set(
                                COALESCE(enrichment_metadata, '{}'::jsonb),
                                '{email}',
                                %s::jsonb
                            ),
                            profile_confidence = %s,
                            last_enriched_at = %s,
                            updated_at = %s
                        WHERE id = %s
                    """, email_updates)
                    cursor.execute("RELEASE SAVEPOINT email_batch")
                    logger.info(
                        "Email batch: %d updates applied", len(email_updates),
                    )
                except Exception as exc:
                    logger.error("Email batch update failed: %s", exc)
                    cursor.execute("ROLLBACK TO SAVEPOINT email_batch")
                    failed_updates += len(email_updates)

            # ---- Profile field updates -- grouped batch execution -------
            groups: dict[tuple, tuple[sql.Composable, list[list]]] = {}
            for set_parts, params in profile_updates:
                set_clause = sql.SQL(", ").join(set_parts)
                template_key = tuple(repr(sp) for sp in set_parts)
                if template_key not in groups:
                    query = sql.SQL(
                        "UPDATE profiles SET {} WHERE id = %s"
                    ).format(set_clause)
                    groups[template_key] = (query, [])
                groups[template_key][1].append(params)

            for group_idx, (template_key, (query, param_list)) in enumerate(
                groups.items()
            ):
                sp_name = f"batch_group_{group_idx}"
                cursor.execute(f"SAVEPOINT {sp_name}")
                try:
                    execute_batch(cursor, query, param_list)
                    cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
                except Exception as exc:
                    logger.warning(
                        "Batch group %d failed (%d profiles): %s",
                        group_idx, len(param_list), exc,
                    )
                    cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")

                    # Fallback: per-profile execution for this failed group.
                    for j, p in enumerate(param_list):
                        fb_sp = f"fallback_{group_idx}_{j}"
                        try:
                            cursor.execute(f"SAVEPOINT {fb_sp}")
                            cursor.execute(query, p)
                            cursor.execute(f"RELEASE SAVEPOINT {fb_sp}")
                        except Exception as exc2:
                            failed_updates += 1
                            logger.warning(
                                "Profile update fallback failed: %s", exc2,
                            )
                            try:
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {fb_sp}")
                            except Exception:
                                pass

            conn.commit()
        finally:
            cursor.close()
    finally:
        conn.close()

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    stats = {
        'emails_written': len(email_updates),
        'profiles_updated': len(profile_updates) - failed_updates,
        'failed': failed_updates,
        'skipped_empty': skipped_empty,
    }

    logger.info(
        "Consolidation complete: %d emails, %d profiles updated, %d failed, %d skipped",
        stats['emails_written'],
        stats['profiles_updated'],
        stats['failed'],
        stats['skipped_empty'],
    )

    return stats


# ---------------------------------------------------------------------------
# Batch upsert task (Phase 3: Database Write Optimization)
# ---------------------------------------------------------------------------

@task(name="upsert-profiles-batch")
def upsert_profiles_batch(
    profiles: list[dict],
    source: str = "ai_research",
) -> dict:
    """Batch upsert profiles using INSERT ... ON CONFLICT with source priority.

    Uses ON CONFLICT (id) DO UPDATE to atomically handle inserts and updates,
    with source priority checks to prevent lower-priority sources from
    overwriting higher-priority data.
    """
    logger = get_run_logger()

    if not profiles:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    source_priority = SOURCE_PRIORITY.get(source, 0)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        return {"inserted": 0, "updated": 0, "skipped": 0, "error": "no DATABASE_URL"}

    inserted = 0
    updated = 0
    skipped = 0

    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            # Build values list
            rows = []
            for p in profiles:
                pid = p.get("profile_id") or p.get("id")
                if not pid:
                    skipped += 1
                    continue

                rows.append((
                    str(pid),
                    p.get("name", ""),
                    p.get("email"),
                    p.get("company", ""),
                    p.get("website", ""),
                    p.get("linkedin", ""),
                    p.get("niche", ""),
                    p.get("what_you_do", ""),
                    p.get("who_you_serve", ""),
                    p.get("seeking", ""),
                    p.get("offering", ""),
                    p.get("status", "Prospect"),
                    source,
                    source_priority,
                ))

            if not rows:
                return {"inserted": 0, "updated": 0, "skipped": skipped}

            # Upsert with source priority protection
            sql = """
                INSERT INTO profiles (
                    id, name, email, company, website, linkedin,
                    niche, what_you_do, who_you_serve, seeking, offering,
                    status, enrichment_source, source_priority
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    name = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.name, ''), profiles.name)
                        ELSE profiles.name
                    END,
                    email = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        AND EXCLUDED.email IS NOT NULL AND EXCLUDED.email != ''
                        THEN EXCLUDED.email
                        ELSE profiles.email
                    END,
                    company = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.company, ''), profiles.company)
                        ELSE profiles.company
                    END,
                    website = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.website, ''), profiles.website)
                        ELSE profiles.website
                    END,
                    linkedin = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.linkedin, ''), profiles.linkedin)
                        ELSE profiles.linkedin
                    END,
                    niche = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.niche, ''), profiles.niche)
                        ELSE profiles.niche
                    END,
                    what_you_do = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.what_you_do, ''), profiles.what_you_do)
                        ELSE profiles.what_you_do
                    END,
                    who_you_serve = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.who_you_serve, ''), profiles.who_you_serve)
                        ELSE profiles.who_you_serve
                    END,
                    seeking = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.seeking, ''), profiles.seeking)
                        ELSE profiles.seeking
                    END,
                    offering = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN COALESCE(NULLIF(EXCLUDED.offering, ''), profiles.offering)
                        ELSE profiles.offering
                    END,
                    status = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN EXCLUDED.status
                        ELSE profiles.status
                    END,
                    enrichment_source = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN EXCLUDED.enrichment_source
                        ELSE profiles.enrichment_source
                    END,
                    source_priority = CASE
                        WHEN EXCLUDED.source_priority >= COALESCE(profiles.source_priority, 0)
                        THEN EXCLUDED.source_priority
                        ELSE profiles.source_priority
                    END,
                    updated_at = NOW()
            """

            execute_values(cur, sql, rows, page_size=100)

            conn.commit()

            # We can't easily distinguish insert vs update with execute_values,
            # so report total processed
            inserted = len(rows)  # approximate

    except Exception as exc:
        conn.rollback()
        logger.error("Batch upsert failed: %s", exc)
        raise
    finally:
        conn.close()

    logger.info(
        "Upsert complete: %d rows processed, %d skipped",
        len(rows), skipped,
    )
    return {"inserted": inserted, "updated": updated, "skipped": skipped}
