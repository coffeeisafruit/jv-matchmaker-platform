"""
Week 3 client verification flow -- Prefect @flow.

Sends escalating verification emails to active clients so they can
confirm or update their profile data before the monthly processing run.

Schedule:
    Monday    - Initial verification email
    Wednesday - Follow-up if not yet confirmed
    Friday    - Final reminder with urgency

Uses the existing EmailService from ``outreach.email_service`` and tracks
verification status in the ``enrichment_metadata`` JSON column on the
client's SupabaseProfile.

Usage (CLI):
    python -m matching.enrichment.flows.client_verification --day monday
    python -m matching.enrichment.flows.client_verification --day friday --dry-run

Usage (Prefect):
    from matching.enrichment.flows.client_verification import client_verification_flow
    client_verification_flow(day_of_week="monday")
"""

from __future__ import annotations

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
class VerificationStatus:
    """Verification status for a single client."""

    client_id: str
    client_name: str
    email: str
    sent_at: str = ""
    opened_at: str = ""
    confirmed_at: str = ""
    changes_made: dict = field(default_factory=dict)
    reminder_count: int = 0


# ---------------------------------------------------------------------------
# Email templates
# ---------------------------------------------------------------------------

_TEMPLATES: dict[str, dict[str, str]] = {
    "initial": {
        "subject": "Monthly Profile Check -- Please Confirm Your Info",
        "body": (
            "Hi {name},\n\n"
            "It's time for your monthly profile review.  We want to make sure "
            "the partners we match you with reflect your *current* goals.\n\n"
            "Please take 2 minutes to review the details below and reply with "
            "any changes (or just reply 'confirmed' if everything looks good):\n\n"
            "  Name:    {name}\n"
            "  Company: {company}\n"
            "  Seeking: {seeking}\n"
            "  Offering: {offering}\n"
            "  Niche:   {niche}\n\n"
            "We'll process your updated report next week.\n\n"
            "Thanks,\nJV Matchmaker Team"
        ),
    },
    "follow_up": {
        "subject": "Reminder: Confirm Your Profile Before Processing",
        "body": (
            "Hi {name},\n\n"
            "We sent a profile verification request on Monday and haven't "
            "heard back yet.\n\n"
            "If your info is still accurate, just reply 'confirmed'.  "
            "Otherwise, send us any updates so we can include them in this "
            "month's matching run.\n\n"
            "Current profile:\n"
            "  Seeking: {seeking}\n"
            "  Offering: {offering}\n\n"
            "Thanks,\nJV Matchmaker Team"
        ),
    },
    "final_reminder": {
        "subject": "[Action Needed] Last Chance to Update Before Monthly Processing",
        "body": (
            "Hi {name},\n\n"
            "This is your final reminder -- we're processing reports on Monday.\n\n"
            "If you have any changes to your profile, please reply TODAY.  "
            "After this, your current info will be used as-is for matching "
            "and report generation.\n\n"
            "  Seeking: {seeking}\n"
            "  Offering: {offering}\n\n"
            "No changes?  No action needed -- we'll proceed with your "
            "current profile.\n\n"
            "Thanks,\nJV Matchmaker Team"
        ),
    },
}

_DAY_TO_TEMPLATE: dict[str, str] = {
    "monday": "initial",
    "wednesday": "follow_up",
    "friday": "final_reminder",
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="get-active-clients", retries=2, retry_delay_seconds=5)
def get_active_clients() -> list[dict[str, Any]]:
    """Return clients with active MemberReports.

    Joins ``matching_memberreport`` to ``profiles`` via
    ``supabase_profile_id`` to pull the client's name, email, and
    profile fields needed for the verification template.
    """
    logger = get_run_logger()

    sql = """
        SELECT DISTINCT ON (p.id)
            p.id::text          AS client_id,
            p.name              AS client_name,
            mr.member_email     AS email,
            p.company,
            p.seeking,
            p.offering,
            p.niche,
            p.enrichment_metadata
        FROM matching_memberreport mr
        JOIN profiles p ON p.id = mr.supabase_profile_id
        WHERE mr.is_active = true
          AND mr.expires_at > now()
        ORDER BY p.id, mr.month DESC
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        logger.info("Found %d active clients for verification", len(rows))
        return rows
    finally:
        conn.close()


@task(name="get-verification-status", retries=1, retry_delay_seconds=3)
def get_verification_status(client_id: str, month: str) -> dict[str, Any]:
    """Check whether a verification email was already sent/confirmed this month.

    Reads from the ``enrichment_metadata -> verification`` sub-key on
    the client's profile row.

    Parameters
    ----------
    client_id:
        UUID string of the client profile.
    month:
        Month key in ``YYYY-MM`` format.

    Returns
    -------
    dict with keys: sent_at, confirmed_at, reminder_count (all possibly empty/0).
    """
    logger = get_run_logger()

    sql = """
        SELECT enrichment_metadata -> 'verification' -> %s AS vstatus
        FROM profiles
        WHERE id = %s
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (month, client_id))
        row = cur.fetchone()
        if row and row.get("vstatus"):
            vstatus = row["vstatus"]
            logger.info(
                "Verification status for %s (%s): %s",
                client_id, month, vstatus,
            )
            return vstatus
        return {"sent_at": "", "confirmed_at": "", "reminder_count": 0}
    finally:
        conn.close()


@task(name="send-verification-email", retries=1, retry_delay_seconds=10)
def send_verification_email(
    client: dict[str, Any],
    template_key: str,
    verification: dict[str, Any],
    dry_run: bool = False,
) -> VerificationStatus:
    """Send a verification email to *client* using *template_key*.

    If the client has already confirmed this month (``confirmed_at`` is
    set), the email is skipped.

    Parameters
    ----------
    client:
        Dict with client_id, client_name, email, company, seeking,
        offering, niche.
    template_key:
        One of ``"initial"``, ``"follow_up"``, ``"final_reminder"``.
    verification:
        Current verification status dict for this month.
    dry_run:
        If True, log instead of sending.

    Returns
    -------
    VerificationStatus for this client.
    """
    logger = get_run_logger()

    status = VerificationStatus(
        client_id=client["client_id"],
        client_name=client["client_name"],
        email=client["email"],
        sent_at=verification.get("sent_at", ""),
        confirmed_at=verification.get("confirmed_at", ""),
        reminder_count=verification.get("reminder_count", 0),
    )

    # Skip if already confirmed
    if status.confirmed_at:
        logger.info(
            "Skipping %s -- already confirmed on %s",
            client["client_name"],
            status.confirmed_at,
        )
        return status

    # Skip follow-up/final if initial was never sent
    if template_key != "initial" and not status.sent_at:
        logger.info(
            "Skipping %s %s -- initial not yet sent",
            client["client_name"],
            template_key,
        )
        return status

    template = _TEMPLATES[template_key]
    subject = template["subject"]
    body = template["body"].format(
        name=client.get("client_name", ""),
        company=client.get("company", ""),
        seeking=client.get("seeking", "N/A"),
        offering=client.get("offering", "N/A"),
        niche=client.get("niche", "N/A"),
    )

    if dry_run:
        logger.info("[DRY RUN] Would send '%s' to %s", subject, client["email"])
    else:
        from outreach.email_service import EmailService

        email_svc = EmailService()
        email_svc.send_email(
            to=client["email"],
            subject=subject,
            body=body,
        )
        logger.info("Sent '%s' to %s", template_key, client["email"])

    # Update verification tracking
    now_iso = datetime.utcnow().isoformat()
    status.sent_at = status.sent_at or now_iso
    status.reminder_count += 1

    if not dry_run:
        _update_verification_tracking(
            client["client_id"],
            datetime.utcnow().strftime("%Y-%m"),
            {
                "sent_at": status.sent_at,
                "confirmed_at": status.confirmed_at,
                "reminder_count": status.reminder_count,
                "last_template": template_key,
                "last_sent_at": now_iso,
            },
        )

    return status


# ---------------------------------------------------------------------------
# Verification tracking persistence
# ---------------------------------------------------------------------------

def _update_verification_tracking(
    client_id: str,
    month: str,
    data: dict[str, Any],
) -> None:
    """Persist verification status into ``enrichment_metadata -> verification -> {month}``."""
    import json

    sql = """
        UPDATE profiles
        SET enrichment_metadata = jsonb_set(
            COALESCE(enrichment_metadata, '{}'::jsonb),
            ARRAY['verification', %s],
            %s::jsonb,
            true
        )
        WHERE id = %s
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (month, json.dumps(data), client_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="client-verification",
    description="Week 3: Send verification emails with escalating urgency",
    retries=0,
)
def client_verification_flow(
    day_of_week: str = "monday",
    dry_run: bool = False,
) -> list[VerificationStatus]:
    """Send verification emails to active clients.

    Monday: Initial verification email
    Wednesday: Follow-up if not confirmed
    Friday: Final reminder

    Uses existing EmailService for sending.
    Tracks status in enrichment_metadata -> verification.

    Parameters
    ----------
    day_of_week:
        One of ``"monday"``, ``"wednesday"``, ``"friday"``.
    dry_run:
        If True, log actions without sending emails.

    Returns
    -------
    list[VerificationStatus] -- one per active client.
    """
    logger = get_run_logger()
    day = day_of_week.lower()
    template_key = _DAY_TO_TEMPLATE.get(day)

    if template_key is None:
        logger.warning(
            "No template mapped for day_of_week='%s'; expected monday/wednesday/friday",
            day_of_week,
        )
        return []

    logger.info(
        "Client verification flow: day=%s, template=%s, dry_run=%s",
        day, template_key, dry_run,
    )

    # Step 1: Get all active clients
    clients = get_active_clients()
    if not clients:
        logger.info("No active clients found -- nothing to do")
        return []

    # Step 2: Send verification emails
    month_key = datetime.utcnow().strftime("%Y-%m")
    results: list[VerificationStatus] = []

    for client in clients:
        verification = get_verification_status(client["client_id"], month_key)
        status = send_verification_email(
            client=client,
            template_key=template_key,
            verification=verification,
            dry_run=dry_run,
        )
        results.append(status)

    sent_count = sum(1 for r in results if r.reminder_count > 0 and not r.confirmed_at)
    confirmed_count = sum(1 for r in results if r.confirmed_at)
    logger.info(
        "Verification complete: %d sent, %d already confirmed, %d total",
        sent_count, confirmed_count, len(results),
    )

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(description="Week 3 client verification emails")
    parser.add_argument(
        "--day",
        type=str,
        default="monday",
        choices=["monday", "wednesday", "friday"],
        help="Day of the week (determines template urgency)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log only, don't send")
    args = parser.parse_args()

    result = client_verification_flow(day_of_week=args.day, dry_run=args.dry_run)
    print(f"\nProcessed {len(result)} clients")
    for r in result:
        flag = "CONFIRMED" if r.confirmed_at else f"reminders={r.reminder_count}"
        print(f"  {r.client_name} <{r.email}> -- {flag}")
