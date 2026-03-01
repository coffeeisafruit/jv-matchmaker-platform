"""
Post-discovery verification for contact information.

Adds MX record validation (free, catches ~40% of bad emails) and
phone number format standardization.
"""
import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


@dataclass
class EmailVerdict:
    email: str
    format_valid: bool = False
    mx_valid: bool = False
    mx_checked: bool = False
    reason: str = ""


@dataclass
class PhoneVerdict:
    original: str
    formatted: str = ""
    valid: bool = False
    reason: str = ""


class ContactVerifier:
    """Verify contact information after discovery."""

    def verify_email(self, email: str) -> EmailVerdict:
        """3-stage email verification: format -> MX lookup."""
        verdict = EmailVerdict(email=email)

        if not email or not email.strip():
            verdict.reason = "empty"
            return verdict

        email = email.strip().lower()
        verdict.email = email

        # Stage 1: Format check
        if not EMAIL_RE.match(email):
            verdict.reason = "invalid format"
            return verdict
        verdict.format_valid = True

        # Stage 2: MX record check (free)
        domain = email.split('@')[1]
        try:
            import dns.resolver
            verdict.mx_checked = True
            try:
                answers = dns.resolver.resolve(domain, 'MX')
                if answers:
                    verdict.mx_valid = True
                else:
                    verdict.reason = "no MX records"
            except dns.resolver.NXDOMAIN:
                verdict.reason = f"domain does not exist: {domain}"
            except dns.resolver.NoAnswer:
                verdict.reason = f"no MX records for: {domain}"
            except dns.resolver.NoNameservers:
                verdict.reason = f"no nameservers for: {domain}"
            except dns.resolver.Timeout:
                # DNS timeout -- don't fail, just note
                verdict.reason = "DNS timeout"
                verdict.mx_valid = True  # Assume valid on timeout
                logger.debug(f"DNS timeout for {domain}")
        except ImportError:
            # dnspython not installed -- skip MX check
            verdict.reason = "dnspython not installed, MX check skipped"
            verdict.mx_valid = True  # Don't block without the library
            logger.debug("dnspython not available, skipping MX check")

        return verdict

    def verify_phone(self, phone: str, country: str = 'US') -> PhoneVerdict:
        """Format and validate phone number."""
        verdict = PhoneVerdict(original=phone)

        if not phone or not phone.strip():
            verdict.reason = "empty"
            return verdict

        try:
            import phonenumbers
            parsed = phonenumbers.parse(phone, country)
            if phonenumbers.is_valid_number(parsed):
                verdict.valid = True
                verdict.formatted = phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
            else:
                verdict.reason = "invalid phone number"
        except ImportError:
            # phonenumbers not installed -- basic digit count check
            digits = re.sub(r'\D', '', phone)
            if 7 <= len(digits) <= 15:
                verdict.valid = True
                verdict.formatted = phone.strip()
            else:
                verdict.reason = f"unexpected digit count: {len(digits)}"
        except Exception as e:
            verdict.reason = f"parse error: {e}"

        return verdict

    def check_url_reachable(self, url: str, timeout: int = 10) -> bool:
        """Quick HEAD request to check if URL is live."""
        if not url:
            return False
        try:
            import requests
            resp = requests.head(url, timeout=timeout, allow_redirects=True)
            return resp.status_code < 400
        except Exception:
            return False
