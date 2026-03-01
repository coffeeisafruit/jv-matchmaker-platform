"""
Pre-ingest validation for all data entry paths.

Catches invalid data at the boundary before it flows into enrichment.
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Reuse the existing email pattern from the verification gate
EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
URL_RE = re.compile(r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')


@dataclass
class IngestVerdict:
    valid: bool = True
    issues: list[str] = field(default_factory=list)
    auto_fixed: dict = field(default_factory=dict)


class IngestValidator:
    """Validates profile data before pipeline entry."""

    def validate(self, data: dict) -> IngestVerdict:
        """Run all ingest checks on a profile dict."""
        verdict = IngestVerdict()

        # Name validation
        name = (data.get('name') or '').strip()
        if not name or len(name) < 2:
            verdict.valid = False
            verdict.issues.append("Name is empty or too short (< 2 chars)")
        elif name.isdigit():
            verdict.valid = False
            verdict.issues.append("Name is all digits")

        # Email format (if present)
        email = (data.get('email') or '').strip()
        if email and not EMAIL_RE.match(email):
            verdict.issues.append(f"Email format invalid: {email}")
            verdict.auto_fixed['email'] = ''  # Clear invalid email

        # Suspicious email patterns
        if email:
            local = email.split('@')[0].lower()
            suspicious = ('test', 'spam', 'noreply', 'no-reply', 'admin', 'info', 'support')
            if local in suspicious:
                verdict.issues.append(f"Email looks generic/suspicious: {email}")

        # Website URL format (if present)
        website = (data.get('website') or '').strip()
        if website and not URL_RE.match(website):
            verdict.issues.append(f"Website URL format invalid: {website}")
            verdict.auto_fixed['website'] = ''

        # LinkedIn URL format (if present)
        linkedin = (data.get('linkedin') or '').strip()
        if linkedin and 'linkedin.com/in/' not in linkedin and 'linkedin.com/company/' not in linkedin:
            verdict.issues.append(f"LinkedIn URL format unexpected: {linkedin}")

        return verdict

    def check_duplicate(self, name: str, email: str, website: str, conn) -> Optional[str]:
        """Check for duplicate profiles. Returns existing profile_id if found."""
        if not name:
            return None

        cursor = conn.cursor()
        try:
            # Check by email first (strongest signal)
            if email:
                cursor.execute(
                    "SELECT id FROM profiles WHERE LOWER(email) = LOWER(%s) LIMIT 1",
                    [email]
                )
                row = cursor.fetchone()
                if row:
                    return str(row[0])

            # Check by name similarity + website domain
            if website:
                from urllib.parse import urlparse
                domain = urlparse(website).netloc.replace('www.', '')
                cursor.execute(
                    "SELECT id FROM profiles WHERE LOWER(name) = LOWER(%s) "
                    "AND website ILIKE %s LIMIT 1",
                    [name.strip(), f'%{domain}%']
                )
                row = cursor.fetchone()
                if row:
                    return str(row[0])

            # Check by exact name match (weakest)
            cursor.execute(
                "SELECT id FROM profiles WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)) LIMIT 1",
                [name]
            )
            row = cursor.fetchone()
            if row:
                return str(row[0])

        finally:
            cursor.close()

        return None
