"""
Week 4 Tuesday admin notification flow -- Prefect @flow.

Gathers processing results from Monday, verification statuses, and
acquisition outcomes, then uses Claude to generate actionable AI
suggestions.  Sends a comprehensive admin email digest.

Usage (CLI):
    python -m matching.enrichment.flows.admin_notification
    python -m matching.enrichment.flows.admin_notification --dry-run

Usage (Prefect):
    from matching.enrichment.flows.admin_notification import admin_notification_flow
    admin_notification_flow()
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, task, get_run_logger


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AdminNotification:
    """Admin notification content."""

    per_client_summary: list[dict] = field(default_factory=list)
    verification_status: list[dict] = field(default_factory=list)
    acquisition_results: list[dict] = field(default_factory=list)
    system_health: dict = field(default_factory=dict)
    ai_suggestions: list[str] = field(default_factory=list)
    cost_summary: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="gather-processing-results", retries=2, retry_delay_seconds=5)
def gather_processing_results() -> list[dict[str, Any]]:
    """Query recent processing data -- per-client match counts and scores.

    Joins ``matching_memberreport`` to ``match_suggestions`` to produce
    a summary of each client's current match pool quality.

    Returns
    -------
    list[dict] with per-client summaries including:
        client_id, client_name, total_matches, matches_above_70,
        avg_score, report_partner_count, last_enriched_at.
    """
    logger = get_run_logger()

    sql = """
        SELECT
            p.id::text                        AS client_id,
            p.name                            AS client_name,
            mr.member_email                   AS email,
            p.last_enriched_at,
            COUNT(ms.id)                      AS total_matches,
            COUNT(ms.id) FILTER (
                WHERE COALESCE(ms.harmonic_mean, 0) >= 70
            )                                 AS matches_above_70,
            ROUND(AVG(COALESCE(ms.harmonic_mean, 0))::numeric, 1)
                                              AS avg_score,
            (
                SELECT COUNT(*)
                FROM matching_reportpartner rp
                WHERE rp.report_id = mr.id
            )                                 AS report_partner_count
        FROM matching_memberreport mr
        JOIN profiles p ON p.id = mr.supabase_profile_id
        LEFT JOIN match_suggestions ms ON ms.profile_id = p.id
            AND ms.status NOT IN ('dismissed')
        WHERE mr.is_active = true
        GROUP BY p.id, p.name, mr.member_email, p.last_enriched_at, mr.id
        ORDER BY p.name
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        logger.info("Gathered processing results for %d clients", len(rows))
        return rows
    finally:
        conn.close()


@task(name="gather-verification-summary", retries=2, retry_delay_seconds=5)
def gather_verification_summary() -> list[dict[str, Any]]:
    """Summarise client verification status for the current month.

    Reads ``enrichment_metadata -> verification -> {current_month}`` for
    every active client.

    Returns
    -------
    list[dict] with: client_id, client_name, sent_at, confirmed_at,
    reminder_count, changes_made (bool).
    """
    logger = get_run_logger()
    month_key = datetime.utcnow().strftime("%Y-%m")

    sql = """
        SELECT
            p.id::text          AS client_id,
            p.name              AS client_name,
            p.enrichment_metadata -> 'verification' -> %s AS vstatus
        FROM profiles p
        JOIN matching_memberreport mr ON mr.supabase_profile_id = p.id
        WHERE mr.is_active = true
        GROUP BY p.id, p.name, p.enrichment_metadata
        ORDER BY p.name
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (month_key,))
        rows = cur.fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            vstatus = row.get("vstatus") or {}
            results.append({
                "client_id": row["client_id"],
                "client_name": row["client_name"],
                "sent_at": vstatus.get("sent_at", ""),
                "confirmed_at": vstatus.get("confirmed_at", ""),
                "reminder_count": vstatus.get("reminder_count", 0),
                "has_changes": bool(vstatus.get("changes_made")),
            })

        confirmed = sum(1 for r in results if r["confirmed_at"])
        logger.info(
            "Verification summary: %d/%d confirmed", confirmed, len(results),
        )
        return results
    finally:
        conn.close()


@task(name="generate-ai-suggestions", retries=1, retry_delay_seconds=10)
def generate_ai_suggestions(
    processing_data: list[dict[str, Any]],
    verification_data: list[dict[str, Any]],
) -> list[str]:
    """Use Claude to analyze processing + verification data and generate suggestions.

    The prompt is structured to produce short, actionable recommendations
    that the admin can act on immediately.

    Returns
    -------
    list[str] -- 3-8 actionable suggestion strings.
    """
    logger = get_run_logger()

    # Build a concise data summary for the prompt
    client_lines: list[str] = []
    for c in processing_data:
        client_lines.append(
            f"- {c['client_name']}: {c.get('matches_above_70', 0)} matches at 70+, "
            f"avg={c.get('avg_score', 0)}, report_partners={c.get('report_partner_count', 0)}"
        )

    verification_lines: list[str] = []
    for v in verification_data:
        status = "confirmed" if v["confirmed_at"] else f"reminders={v['reminder_count']}"
        verification_lines.append(f"- {v['client_name']}: {status}")

    prompt = f"""You are an operations analyst for a JV (joint venture) matchmaking platform.

Analyze the following monthly processing results and produce 3-8 short, actionable suggestions
for the admin. Focus on concrete next steps -- not generic advice.

## Client Match Data
{chr(10).join(client_lines)}

## Verification Status
{chr(10).join(verification_lines)}

## Rules
- If a client has fewer than 10 matches at 70+, suggest expanding their seeking criteria or
  adjusting niche parameters.
- If any prospects scored 68-69, flag them as "near-threshold" and suggest small adjustments.
- If acquisition costs are rising, suggest tightening the pre-filter.
- If a client hasn't confirmed their profile, flag the risk of stale data.
- Be specific: use client names and numbers.

Return a JSON array of strings, each string being one suggestion. Example:
["Client X has only 6 matches at 70+. Consider expanding seeking criteria.",
 "3 prospects for Client Y scored 68-69. Small niche adjustment could push them over 70."]
"""

    try:
        # Use OpenRouter (preferred) or Anthropic API
        api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning("No AI API key found; returning placeholder suggestions")
            return [
                "Unable to generate AI suggestions -- no API key configured.",
                "Review the client match data manually for gaps.",
            ]

        import httpx

        if os.environ.get("OPENROUTER_API_KEY"):
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "anthropic/claude-sonnet-4",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1024,
                "temperature": 0.3,
            }
        else:
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
            }

        resp = httpx.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # Extract text content
        if "choices" in data:
            # OpenRouter format
            text = data["choices"][0]["message"]["content"]
        else:
            # Anthropic format
            text = data["content"][0]["text"]

        # Parse JSON array from the response
        suggestions = _parse_json_array(text)
        logger.info("AI generated %d suggestions", len(suggestions))
        return suggestions

    except Exception as exc:
        logger.error("AI suggestion generation failed: %s", exc)
        return [
            f"AI suggestion generation failed: {exc}",
            "Review the client match data manually for gaps and opportunities.",
        ]


@task(name="send-admin-email", retries=2, retry_delay_seconds=10)
def send_admin_email(
    notification: AdminNotification,
    dry_run: bool = False,
) -> bool:
    """Format and send the admin notification email.

    Parameters
    ----------
    notification:
        Fully populated AdminNotification dataclass.
    dry_run:
        If True, log instead of sending.

    Returns
    -------
    True if sent (or would be sent in dry-run), False on failure.
    """
    logger = get_run_logger()
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@jvmatchmaker.com")

    # Build email body
    lines: list[str] = []
    lines.append("=== JV Matchmaker -- Monthly Admin Report ===")
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # Per-client summary
    lines.append("--- Client Summary ---")
    for c in notification.per_client_summary:
        lines.append(
            f"  {c.get('client_name', '?')}: "
            f"{c.get('matches_above_70', 0)} matches at 70+, "
            f"avg={c.get('avg_score', 0)}, "
            f"report_partners={c.get('report_partner_count', 0)}"
        )
    lines.append("")

    # Verification summary
    lines.append("--- Verification Status ---")
    for v in notification.verification_status:
        status = "CONFIRMED" if v.get("confirmed_at") else f"pending (reminders={v.get('reminder_count', 0)})"
        lines.append(f"  {v.get('client_name', '?')}: {status}")
    lines.append("")

    # AI suggestions
    lines.append("--- AI Suggestions ---")
    for i, suggestion in enumerate(notification.ai_suggestions, 1):
        lines.append(f"  {i}. {suggestion}")
    lines.append("")

    # System health
    if notification.system_health:
        lines.append("--- System Health ---")
        for k, v in notification.system_health.items():
            lines.append(f"  {k}: {v}")
        lines.append("")

    # Cost summary
    if notification.cost_summary:
        lines.append("--- Cost Summary ---")
        for k, v in notification.cost_summary.items():
            lines.append(f"  {k}: ${v:.2f}" if isinstance(v, float) else f"  {k}: {v}")
        lines.append("")

    body = "\n".join(lines)

    if dry_run:
        logger.info("[DRY RUN] Would send admin notification to %s", admin_email)
        logger.info("Email body preview:\n%s", body[:1000])
        return True

    try:
        from outreach.email_service import EmailService

        email_svc = EmailService()
        email_svc.send_email(
            to=admin_email,
            subject=f"[JV Matchmaker] Monthly Admin Report -- {datetime.utcnow().strftime('%B %Y')}",
            body=body,
        )
        logger.info("Admin notification sent to %s", admin_email)
        return True
    except Exception as exc:
        logger.error("Failed to send admin email: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="admin-notification",
    description="Week 4 Tue: Generate admin report with AI analysis and suggestions",
    retries=0,
)
def admin_notification_flow(
    dry_run: bool = False,
) -> AdminNotification:
    """Generate and send admin notification with AI-powered suggestions.

    Steps:
      1. Gather processing results from Monday
      2. Gather verification status for all clients
      3. AI agent analyzes data and generates suggestions
      4. Send admin email with full summary + suggestions

    Parameters
    ----------
    dry_run:
        If True, log actions without sending emails.

    Returns
    -------
    AdminNotification with all gathered data and suggestions.
    """
    logger = get_run_logger()
    notification = AdminNotification()

    logger.info("Admin notification flow started (dry_run=%s)", dry_run)

    # Step 1: Gather processing results
    logger.info("Step 1/4: Gathering processing results")
    processing_data = gather_processing_results()
    notification.per_client_summary = processing_data

    # Step 2: Gather verification summary
    logger.info("Step 2/4: Gathering verification status")
    verification_data = gather_verification_summary()
    notification.verification_status = verification_data

    # Step 3: Generate AI suggestions
    logger.info("Step 3/4: Generating AI suggestions")
    suggestions = generate_ai_suggestions(
        processing_data=processing_data,
        verification_data=verification_data,
    )
    notification.ai_suggestions = suggestions

    # Compute system health snapshot
    notification.system_health = _compute_system_health(processing_data)

    # Compute cost summary
    notification.cost_summary = _compute_cost_summary()

    # Step 4: Send admin email
    logger.info("Step 4/4: Sending admin email")
    send_admin_email(notification=notification, dry_run=dry_run)

    logger.info(
        "Admin notification complete: %d clients, %d suggestions",
        len(processing_data),
        len(suggestions),
    )

    return notification


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_json_array(text: str) -> list[str]:
    """Extract a JSON array of strings from LLM output.

    Handles cases where the model wraps the JSON in markdown code fences.
    """
    import re

    # Strip markdown code fences if present
    text = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)

    # Try parsing as JSON
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except json.JSONDecodeError:
        pass

    # Fallback: split on newlines and filter
    lines = [line.strip().lstrip("- ").lstrip("0123456789.").strip()
             for line in text.split("\n") if line.strip()]
    return [line for line in lines if len(line) > 10]


def _compute_system_health(processing_data: list[dict]) -> dict[str, Any]:
    """Compute basic system health indicators."""
    total_clients = len(processing_data)
    if total_clients == 0:
        return {"status": "no_clients", "clients": 0}

    avg_matches = sum(c.get("matches_above_70", 0) for c in processing_data) / total_clients
    clients_below_target = sum(
        1 for c in processing_data if (c.get("matches_above_70", 0) or 0) < 10
    )

    return {
        "status": "healthy" if clients_below_target == 0 else "needs_attention",
        "total_clients": total_clients,
        "avg_matches_above_70": round(avg_matches, 1),
        "clients_below_target": clients_below_target,
    }


def _compute_cost_summary() -> dict[str, Any]:
    """Placeholder cost summary -- reads from Prefect flow run artifacts if available."""
    # In production this would query Prefect's API or a cost-tracking table.
    # For now return an empty dict; the orchestrator can inject actual costs.
    return {
        "note": "Cost tracking will be populated by monthly_orchestrator",
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(description="Week 4 Tuesday admin notification")
    parser.add_argument("--dry-run", action="store_true", help="Log only, don't send")
    args = parser.parse_args()

    result = admin_notification_flow(dry_run=args.dry_run)
    print(f"\nNotification: {len(result.per_client_summary)} clients, "
          f"{len(result.ai_suggestions)} suggestions")
    for s in result.ai_suggestions:
        print(f"  - {s}")
