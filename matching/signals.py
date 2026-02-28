"""
Django signals for the matching app.

Connects model lifecycle events to Prefect flow triggers.
"""

import logging
import threading

from django.db.models.signals import post_save
from django.dispatch import receiver

from matching.models import SavedCandidate

logger = logging.getLogger(__name__)


@receiver(post_save, sender=SavedCandidate)
def trigger_new_contact_flow(sender, instance, created, **kwargs):
    """Fire the Prefect new_contact_flow when a SavedCandidate is created.

    Only triggers on initial creation (not updates).  The flow is dispatched
    in a daemon thread so the Django request/response cycle is never blocked.
    """
    if not created:
        return

    # Build the contact dict from the SavedCandidate instance fields.
    contact = {
        "name": instance.name,
    }
    if instance.company:
        contact["company"] = instance.company
    if instance.niche:
        contact["niche"] = instance.niche
    if instance.seeking:
        contact["seeking"] = instance.seeking
    if instance.offering:
        contact["offering"] = instance.offering
    if instance.who_you_serve:
        contact["who_you_serve"] = instance.who_you_serve
    if instance.what_you_do:
        contact["what_you_do"] = instance.what_you_do

    logger.info(
        "SavedCandidate created (id=%s, name=%s) -- dispatching new_contact_flow",
        instance.pk,
        instance.name,
    )

    def _run_flow():
        try:
            from matching.enrichment.flows.new_contact_flow import new_contact_flow

            new_contact_flow(
                contacts=[contact],
                source="saved_candidate",
            )
        except Exception:
            logger.exception(
                "new_contact_flow failed for SavedCandidate id=%s",
                instance.pk,
            )

    thread = threading.Thread(target=_run_flow, daemon=True)
    thread.start()

    logger.info(
        "new_contact_flow dispatched in background thread for SavedCandidate id=%s",
        instance.pk,
    )
