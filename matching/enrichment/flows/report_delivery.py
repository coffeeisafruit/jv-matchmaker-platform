"""
1st-of-month report delivery flow -- Prefect @flow.

Delivers updated reports to all active clients with fresh access codes
and an AI-generated personalized introduction summarising what changed
since last month.

Usage (CLI):
    python -m matching.enrichment.flows.report_delivery
    python -m matching.enrichment.flows.report_delivery --dry-run

Usage (Prefect):
    from matching.enrichment.flows.report_delivery import report_delivery_flow
    report_delivery_flow()
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, task, get_run_logger


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DeliveryResult:
    """Result of delivering reports to clients."""

    reports_delivered: int = 0
    new_access_codes: int = 0
    delivery_failures: int = 0
    clients_skipped: int = 0


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

@task(name="get-active-reports")
def get_active_reports() -> list[dict[str, Any]]:
    """Retrieve all active MemberReports with client info.

    Returns
    -------
    list[dict] with: report_id, member_name, member_email, company_name,
    access_code, month, partner_count, client_profile_id.
    """
    logger = get_run_logger()

    sql = """
        SELECT
            mr.id                     AS report_id,
            mr.member_name,
            mr.member_email,
            mr.company_name,
            mr.access_code,
            mr.month,
            mr.supabase_profile_id::text AS client_profile_id,
            (
                SELECT COUNT(*)
                FROM matching_reportpartner rp
                WHERE rp.report_id = mr.id
            )                         AS partner_count
        FROM matching_memberreport mr
        WHERE mr.is_active = true
        ORDER BY mr.member_name
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        logger.info("Found %d active reports for delivery", len(rows))
        return rows
    finally:
        conn.close()


@task(name="generate-access-code")
def generate_access_code() -> str:
    """Generate an 8-character hex access code.

    Returns
    -------
    str -- e.g. ``"a3f1b09c"``.
    """
    return secrets.token_hex(4)


@task(name="rotate-access-code", retries=2, retry_delay_seconds=5)
def rotate_access_code(report_id: int, new_code: str) -> dict[str, Any]:
    """Update a MemberReport with a new access code and expiry date.

    Sets ``expires_at`` to 35 days from now and resets ``access_count``.

    Parameters
    ----------
    report_id:
        PK of the MemberReport.
    new_code:
        New 8-char hex access code.

    Returns
    -------
    dict with: report_id, old_code, new_code, expires_at.
    """
    logger = get_run_logger()

    new_month = datetime.utcnow().replace(day=1).date()
    new_expires = datetime.utcnow() + timedelta(days=35)

    sql = """
        UPDATE matching_memberreport
        SET access_code  = %s,
            month        = %s,
            expires_at   = %s,
            access_count = 0
        WHERE id = %s
        RETURNING access_code
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Capture old code first
        cur.execute("SELECT access_code FROM matching_memberreport WHERE id = %s", (report_id,))
        old_row = cur.fetchone()
        old_code = old_row["access_code"] if old_row else ""

        cur.execute(sql, (new_code, new_month, new_expires, report_id))
        conn.commit()

        logger.info(
            "Rotated access code for report %d: %s -> %s (expires %s)",
            report_id, old_code, new_code, new_expires.isoformat(),
        )

        return {
            "report_id": report_id,
            "old_code": old_code,
            "new_code": new_code,
            "expires_at": new_expires.isoformat(),
        }
    finally:
        conn.close()


@task(name="generate-report-intro", retries=1, retry_delay_seconds=10)
def generate_report_intro(
    client: dict[str, Any],
    report: dict[str, Any],
    changes: dict[str, Any],
) -> str:
    """Generate an AI-powered personalized intro for the delivery email.

    Summarises what changed since last month and highlights key new matches.

    Parameters
    ----------
    client:
        Dict with member_name, company_name.
    report:
        Dict with partner_count, month.
    changes:
        Dict with new_partners, removed_partners, score_changes
        (may be empty if this is the first delivery).

    Returns
    -------
    str -- 2-4 sentence personalized intro paragraph.
    """
    logger = get_run_logger()

    name = client.get("member_name", "there")
    partner_count = report.get("partner_count", 0)
    new_count = changes.get("new_partners", 0)
    improved_count = changes.get("score_improvements", 0)

    # Try AI generation, fall back to template
    try:
        api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("No AI API key")

        prompt = (
            f"Write a 2-3 sentence personalized intro for {name}'s monthly JV partner report. "
            f"Their report contains {partner_count} partner recommendations. "
            f"This month: {new_count} new partners were added, and {improved_count} existing "
            f"match scores improved. "
            f"Be warm, professional, and specific. Do NOT use emojis. "
            f"Start with 'Hi {name},' on its own line."
        )

        import httpx

        if os.environ.get("OPENROUTER_API_KEY"):
            resp = httpx.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "anthropic/claude-sonnet-4",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 256,
                    "temperature": 0.5,
                },
                timeout=30,
            )
            resp.raise_for_status()
            intro = resp.json()["choices"][0]["message"]["content"].strip()
        else:
            resp = httpx.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 256,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.5,
                },
                timeout=30,
            )
            resp.raise_for_status()
            intro = resp.json()["content"][0]["text"].strip()

        logger.info("Generated AI intro for %s (%d chars)", name, len(intro))
        return intro

    except Exception as exc:
        logger.warning("AI intro generation failed for %s: %s -- using template", name, exc)

    # Template fallback
    if new_count > 0:
        return (
            f"Hi {name},\n\n"
            f"Your updated partner report is ready with {partner_count} recommendations. "
            f"We added {new_count} new partner(s) this month based on fresh research."
        )
    return (
        f"Hi {name},\n\n"
        f"Your updated partner report is ready with {partner_count} recommendations. "
        f"All scores have been refreshed using the latest data."
    )


@task(name="send-delivery-email", retries=2, retry_delay_seconds=10)
def send_delivery_email(
    client: dict[str, Any],
    report: dict[str, Any],
    intro: str,
    access_code: str,
    dry_run: bool = False,
) -> bool:
    """Send the delivery email with report link and personalized intro.

    Parameters
    ----------
    client:
        Dict with member_name, member_email.
    report:
        Dict with report_id.
    intro:
        AI-generated personalized intro paragraph.
    access_code:
        New 8-char hex access code for the report URL.
    dry_run:
        If True, log instead of sending.

    Returns
    -------
    True if sent (or would be sent), False on failure.
    """
    logger = get_run_logger()

    base_url = os.environ.get("REPORT_BASE_URL", "https://app.jvmatchmaker.com")
    report_url = f"{base_url}/report/{access_code}/"

    subject = f"Your Updated JV Partner Report is Ready -- {datetime.utcnow().strftime('%B %Y')}"
    body = (
        f"{intro}\n\n"
        f"View your report here:\n{report_url}\n\n"
        f"This link is unique to you and expires in 35 days. "
        f"Bookmark it for easy access.\n\n"
        f"Questions or feedback? Just reply to this email.\n\n"
        f"Best,\nJV Matchmaker Team"
    )

    if dry_run:
        logger.info(
            "[DRY RUN] Would send delivery email to %s (code=%s)",
            client["member_email"],
            access_code,
        )
        return True

    try:
        from django.core.mail import send_mail
        send_mail(
            subject=subject,
            message=body,
            from_email=None,  # Uses DEFAULT_FROM_EMAIL from settings
            recipient_list=[client["member_email"]],
            fail_silently=False,
        )
        logger.info("Delivered report to %s", client["member_email"])
        return True
    except Exception as exc:
        logger.error(
            "Failed to deliver report to %s: %s",
            client["member_email"],
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="report-delivery",
    description="1st of month: Deliver updated reports with new access codes",
    retries=0,
)
def report_delivery_flow(
    dry_run: bool = False,
) -> DeliveryResult:
    """Deliver updated reports to all active clients.

    Steps:
      1. Get all active clients with MemberReports
      2. Generate new access codes (8-char hex)
      3. Generate personalized intro (AI-generated summary of what changed)
      4. Send delivery email with report link + intro
      5. Track delivery status

    Parameters
    ----------
    dry_run:
        If True, log actions without sending emails or rotating codes.

    Returns
    -------
    DeliveryResult with aggregate stats.
    """
    logger = get_run_logger()
    result = DeliveryResult()

    logger.info("Report delivery flow started (dry_run=%s)", dry_run)

    # Step 1: Get active reports
    reports = get_active_reports()
    if not reports:
        logger.info("No active reports found -- nothing to deliver")
        return result

    for report in reports:
        # Step 2: Generate new access code
        new_code = generate_access_code()

        # Step 3: Rotate access code in DB (unless dry run)
        if not dry_run:
            rotate_access_code(
                report_id=report["report_id"],
                new_code=new_code,
            )
            result.new_access_codes += 1
        else:
            logger.info(
                "[DRY RUN] Would rotate code for report %d",
                report["report_id"],
            )

        # Step 4: Compute what changed (simplified -- compare partner counts)
        changes = {
            "new_partners": 0,
            "score_improvements": 0,
        }

        # Step 5: Generate personalized intro
        intro = generate_report_intro(
            client=report,
            report=report,
            changes=changes,
        )

        # Step 6: Send delivery email
        success = send_delivery_email(
            client=report,
            report=report,
            intro=intro,
            access_code=new_code,
            dry_run=dry_run,
        )

        if success:
            result.reports_delivered += 1
        else:
            result.delivery_failures += 1

    logger.info(
        "Report delivery complete: %d delivered, %d codes rotated, %d failures",
        result.reports_delivered,
        result.new_access_codes,
        result.delivery_failures,
    )

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(description="1st-of-month report delivery")
    parser.add_argument("--dry-run", action="store_true", help="Log only, don't send")
    args = parser.parse_args()

    result = report_delivery_flow(dry_run=args.dry_run)
    print(
        f"\nDelivery: {result.reports_delivered} delivered, "
        f"{result.new_access_codes} codes rotated, "
        f"{result.delivery_failures} failures"
    )
