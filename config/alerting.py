"""
Lightweight alerting module.

Routes alerts through logging + optional Slack webhook + optional email.
"""
import logging
import os

import requests

logger = logging.getLogger("alerting")


def send_alert(severity: str, title: str, detail: str = "") -> None:
    """
    Send alert through configured channels.

    Args:
        severity: "critical", "warning", or "info"
        title: Short alert title
        detail: Additional context
    """
    log_fn = {
        "critical": logger.critical,
        "warning": logger.warning,
    }.get(severity, logger.info)
    log_fn(f"ALERT [{severity.upper()}]: {title} -- {detail}")

    # Slack webhook
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        emoji = {
            "critical": ":red_circle:",
            "warning": ":warning:",
        }.get(severity, ":information_source:")
        try:
            requests.post(
                webhook,
                json={"text": f"{emoji} *{title}*\\n{detail}"},
                timeout=5,
            )
        except Exception:
            logger.exception("Failed to send Slack alert")

    # Email for critical alerts only
    alert_email = os.environ.get("ALERT_EMAIL")
    if alert_email and severity == "critical":
        try:
            from django.core.mail import send_mail
            send_mail(
                subject=f"[JV Platform CRITICAL] {title}",
                message=detail or title,
                from_email=None,
                recipient_list=[alert_email],
                fail_silently=True,
            )
        except Exception:
            logger.exception("Failed to send email alert")
