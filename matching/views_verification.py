"""
Client-facing profile verification views.

These views allow clients to confirm or update their profile data via a
unique verification token emailed to them.  No login is required -- the
UUID token in the URL serves as the credential.

Flow:
    1. Client clicks link in verification email  -->  ProfileVerificationView (GET)
    2. Client reviews / edits fields and submits  -->  ProfileVerificationSubmitView (POST)
    3. Client sees thank-you page                 -->  VerificationStatusView (GET)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from django.http import Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views import View

from .models import ClientVerification, SupabaseProfile

logger = logging.getLogger(__name__)

# Fields the client is allowed to review and edit via the verification form.
VERIFICATION_FIELDS = [
    'name',
    'company',
    'email',
    'website',
    'linkedin',
    'what_you_do',
    'who_you_serve',
    'seeking',
    'offering',
    'niche',
    'signature_programs',
    'bio',
]

# Human-readable labels for template rendering.
FIELD_LABELS = {
    'name': 'Full Name',
    'company': 'Company',
    'email': 'Email',
    'website': 'Website',
    'linkedin': 'LinkedIn URL',
    'what_you_do': 'What You Do',
    'who_you_serve': 'Who You Serve',
    'seeking': 'What You Are Seeking',
    'offering': 'What You Are Offering',
    'niche': 'Your Niche',
    'signature_programs': 'Signature Programs',
    'bio': 'About You',
}

# Fields that should render as <textarea> rather than <input>.
TEXTAREA_FIELDS = {
    'what_you_do',
    'who_you_serve',
    'seeking',
    'offering',
    'bio',
    'signature_programs',
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_verification_or_404(token) -> ClientVerification:
    """Look up a ClientVerification by its ``verification_token``.

    Returns the object or raises Http404.
    """
    try:
        return ClientVerification.objects.select_related('client').get(
            verification_token=token,
        )
    except ClientVerification.DoesNotExist:
        raise Http404('Verification link is invalid or has expired.')


def _is_verification_active(cv: ClientVerification) -> bool:
    """Return True if the verification is still actionable (pending)."""
    return cv.status == 'pending'


def _build_field_list(profile: SupabaseProfile) -> list[dict]:
    """Build the list of field dicts for the confirmation template.

    Each dict contains: name, label, value, is_textarea.
    """
    fields = []
    for field_name in VERIFICATION_FIELDS:
        value = getattr(profile, field_name, '') or ''
        fields.append({
            'name': field_name,
            'label': FIELD_LABELS.get(field_name, field_name.replace('_', ' ').title()),
            'value': value,
            'is_textarea': field_name in TEXTAREA_FIELDS,
        })
    return fields


def _snapshot_profile(profile: SupabaseProfile) -> dict:
    """Capture the current values of VERIFICATION_FIELDS as a plain dict."""
    return {
        f: getattr(profile, f, '') or ''
        for f in VERIFICATION_FIELDS
    }


def _write_confirmed_data_to_profile(
    profile_id: str,
    updated_fields: dict[str, str],
) -> None:
    """Write confirmed field values back to the Supabase ``profiles`` table.

    Uses psycopg2 because SupabaseProfile is ``managed = False`` and we
    need to stamp ``enrichment_metadata.field_meta`` with
    ``source = 'client_confirmed'`` (priority 100).
    """
    import psycopg2
    from psycopg2 import sql as psql

    if not updated_fields:
        return

    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        logger.error('DATABASE_URL not set -- cannot write confirmed data')
        return

    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()

        # Step 1: Read current enrichment_metadata so we can merge.
        cur.execute(
            'SELECT enrichment_metadata FROM profiles WHERE id = %s',
            (profile_id,),
        )
        row = cur.fetchone()
        meta = {}
        if row and row[0]:
            meta = row[0] if isinstance(row[0], dict) else json.loads(row[0])

        field_meta = meta.get('field_meta', {})
        now_iso = datetime.utcnow().isoformat()

        # Step 2: Build SET clauses for each changed field.
        set_parts = []
        params = []

        for field_name, new_value in updated_fields.items():
            set_parts.append(psql.SQL('{} = %s').format(psql.Identifier(field_name)))
            params.append(new_value)

            # Stamp provenance.
            field_meta[field_name] = {
                'source': 'client_confirmed',
                'updated_at': now_iso,
            }

        # Step 3: Write enrichment_metadata and updated_at.
        meta['field_meta'] = field_meta
        set_parts.append(psql.SQL('enrichment_metadata = %s::jsonb'))
        params.append(json.dumps(meta))
        set_parts.append(psql.SQL('updated_at = %s'))
        params.append(datetime.utcnow())

        params.append(profile_id)

        query = psql.SQL('UPDATE profiles SET {} WHERE id = %s').format(
            psql.SQL(', ').join(set_parts),
        )
        cur.execute(query, params)
        conn.commit()
        logger.info(
            'Wrote %d confirmed fields for profile %s',
            len(updated_fields),
            profile_id,
        )
    except Exception:
        conn.rollback()
        logger.exception('Error writing confirmed data for profile %s', profile_id)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class ProfileVerificationView(View):
    """GET: Display the profile confirmation form pre-filled with current data.

    URL: ``/matching/verify/<uuid:token>/``

    The verification token identifies the ClientVerification record which
    links to the SupabaseProfile.  No login is required.
    """

    def get(self, request, token):
        cv = _get_verification_or_404(token)
        profile = cv.client

        # Mark as opened if first visit.
        if not cv.opened_at:
            cv.opened_at = timezone.now()
            cv.save(update_fields=['opened_at'])

        # If already confirmed, redirect to the status page.
        if cv.status == 'confirmed':
            return redirect('matching:verification-done', token=token)

        # If expired, show a message on the status page.
        if cv.status == 'expired':
            return redirect('matching:verification-done', token=token)

        fields = _build_field_list(profile)

        return render(request, 'matching/verification/confirm_profile.html', {
            'verification': cv,
            'profile': profile,
            'fields': fields,
            'token': token,
        })


class ProfileVerificationSubmitView(View):
    """POST: Process the confirmation form submission.

    URL: ``/matching/verify/<uuid:token>/submit/``

    Reads the submitted field values, compares them to the original data,
    updates the ClientVerification record, and writes confirmed data back
    to the SupabaseProfile.
    """

    def post(self, request, token):
        cv = _get_verification_or_404(token)

        if not _is_verification_active(cv):
            return redirect('matching:verification-done', token=token)

        profile = cv.client

        # Capture original data before any changes.
        original_data = _snapshot_profile(profile)

        # Read submitted values.
        updated_data = {}
        fields_confirmed = []
        changes_made = {}

        for field_name in VERIFICATION_FIELDS:
            submitted_value = request.POST.get(field_name, '').strip()
            original_value = original_data.get(field_name, '')
            is_confirmed = request.POST.get(f'confirm_{field_name}') == 'on'

            if is_confirmed or submitted_value:
                fields_confirmed.append(field_name)

            updated_data[field_name] = submitted_value

            if submitted_value != original_value:
                changes_made[field_name] = {
                    'old': original_value,
                    'new': submitted_value,
                }

        # Update the ClientVerification record.
        now = timezone.now()
        cv.status = 'confirmed'
        cv.confirmed_at = now
        cv.original_data = original_data
        cv.updated_data = updated_data
        cv.fields_confirmed = fields_confirmed
        cv.changes_made = changes_made
        cv.save()

        # Write confirmed data back to the SupabaseProfile.
        # We write ALL fields (not just changed ones) with client_confirmed
        # provenance, since the client reviewed and approved them.
        confirmed_fields = {
            field_name: updated_data[field_name]
            for field_name in fields_confirmed
        }

        try:
            _write_confirmed_data_to_profile(
                profile_id=str(profile.id),
                updated_fields=confirmed_fields,
            )
        except Exception:
            logger.exception('Failed to write confirmed data for token %s', token)
            # The ClientVerification is still marked confirmed so data is not lost.

        return redirect('matching:verification-done', token=token)

    def get(self, request, token):
        """Redirect GET requests to the form page."""
        return redirect('matching:verification-form', token=token)


class VerificationStatusView(View):
    """GET: Show the thank-you / status page after confirmation.

    URL: ``/matching/verify/<uuid:token>/done/``
    """

    def get(self, request, token):
        cv = _get_verification_or_404(token)
        profile = cv.client

        return render(request, 'matching/verification/confirm_success.html', {
            'verification': cv,
            'profile': profile,
            'token': token,
            'changes_count': len(cv.changes_made) if cv.changes_made else 0,
        })
