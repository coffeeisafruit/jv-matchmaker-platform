#!/usr/bin/env python3
"""
One-time backfill: Promote Apollo fields from enrichment_metadata JSONB to columns.

Reads enrichment_metadata->'apollo_data' for all profiles that have it,
extracts seniority, email_confidence, engagement_likelihood, intent_signal,
and funding_stage, then writes them to the new columns.

Usage:
    python3 scripts/backfill_apollo_columns.py                # Full run
    python3 scripts/backfill_apollo_columns.py --dry-run      # Report only
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import psycopg2
import psycopg2.extras


def derive_funding_stage(total_funding) -> str:
    if not total_funding:
        return ""
    try:
        total_funding = int(total_funding)
    except (ValueError, TypeError):
        return ""
    if total_funding < 1_000_000:
        return "seed"
    if total_funding < 5_000_000:
        return "series_a"
    if total_funding < 20_000_000:
        return "series_b"
    if total_funding < 100_000_000:
        return "growth"
    return "profitable"


def main():
    parser = argparse.ArgumentParser(description="Backfill Apollo columns from JSONB")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set.")
        sys.exit(1)

    conn = psycopg2.connect(db_url)

    # Ensure columns exist first
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS seniority VARCHAR(30);")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS email_confidence FLOAT;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS engagement_likelihood BOOLEAN;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS intent_signal BOOLEAN;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS funding_stage VARCHAR(30);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_profiles_seniority "
            "ON profiles (seniority) WHERE seniority IS NOT NULL;"
        )
    conn.commit()
    print("Columns ensured.")

    # Fetch all profiles with apollo_data
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, enrichment_metadata
            FROM profiles
            WHERE enrichment_metadata->'apollo_data' IS NOT NULL
              AND seniority IS NULL
        """)
        rows = cur.fetchall()

    print(f"Found {len(rows)} profiles with apollo_data and no seniority column.")

    if not rows:
        print("Nothing to backfill.")
        conn.close()
        return

    updates = []
    stats = {"seniority": 0, "email_confidence": 0, "engagement": 0, "intent": 0, "funding": 0}

    for row in rows:
        meta = row["enrichment_metadata"] or {}
        apollo = meta.get("apollo_data") or {}

        seniority = (apollo.get("seniority") or "").strip() or None

        email_conf = apollo.get("email_confidence")
        if email_conf is not None:
            try:
                email_conf = float(email_conf)
            except (ValueError, TypeError):
                email_conf = None

        engagement = apollo.get("is_likely_to_engage")
        if engagement is not None:
            engagement = bool(engagement)

        intent = apollo.get("show_intent")
        if intent is not None:
            intent = bool(intent)

        funding = derive_funding_stage(apollo.get("total_funding")) or None

        if seniority:
            stats["seniority"] += 1
        if email_conf is not None:
            stats["email_confidence"] += 1
        if engagement is not None:
            stats["engagement"] += 1
        if intent is not None:
            stats["intent"] += 1
        if funding:
            stats["funding"] += 1

        updates.append((seniority, email_conf, engagement, intent, funding, str(row["id"])))

    print(f"\nField coverage in apollo_data:")
    for field, count in stats.items():
        print(f"  {field:25s} {count:>6,} / {len(rows):,}")

    if args.dry_run:
        print("\n[DRY RUN] No writes performed.")
        conn.close()
        return

    # Batch update
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """UPDATE profiles SET
                seniority = data.seniority,
                email_confidence = data.email_conf,
                engagement_likelihood = data.engagement,
                intent_signal = data.intent,
                funding_stage = data.funding
            FROM (VALUES %s) AS data(seniority, email_conf, engagement, intent, funding, id)
            WHERE profiles.id = data.id::uuid""",
            updates,
            template="(%s, %s, %s, %s, %s, %s)",
        )
    conn.commit()
    conn.close()
    print(f"\nBackfilled {len(updates):,} profiles.")


if __name__ == "__main__":
    main()
