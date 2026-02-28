"""
Tests for deduplication logic in the contact ingestion pipeline.

Verification item #10: "Dedup -- Verify ingestion skips existing profiles
(by email, website, linkedin, name)".

Covers two dedup implementations:
  1. contact_ingestion._find_duplicate()  -- in-memory dedup used by
     new_contact_flow (the primary ingestion path).
  2. contact_ingestion._normalize_domain() / _normalize_linkedin() --
     helper normalization functions.

All tests are pure Python with no database access required.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import pytest
from unittest.mock import patch, MagicMock

from matching.enrichment.flows.contact_ingestion import (
    _find_duplicate,
    _normalize_domain,
    _normalize_linkedin,
)


# =============================================================================
# FIXTURES: reusable existing-profile stores
# =============================================================================

def _existing_profile(
    pid: str = "existing-uuid-1",
    name: str = "Jane Smith",
    email: str = "jane@example.com",
    website: str = "https://www.example.com",
    linkedin: str = "https://www.linkedin.com/in/janesmith",
    company: str = "Acme Corp",
) -> dict:
    """Build an existing-profile row dict for the dedup lookup table."""
    return {
        "id": pid,
        "name": name,
        "email": email,
        "website": website,
        "linkedin": linkedin,
        "company": company,
    }


@pytest.fixture
def existing_profiles() -> dict[str, dict]:
    """A small lookup table of existing profiles keyed by profile ID."""
    return {
        "uuid-1": _existing_profile(
            pid="uuid-1",
            name="Jane Smith",
            email="jane@example.com",
            website="https://www.example.com",
            linkedin="https://www.linkedin.com/in/janesmith",
            company="Acme Corp",
        ),
        "uuid-2": _existing_profile(
            pid="uuid-2",
            name="Bob Johnson",
            email="bob@testsite.org",
            website="https://testsite.org",
            linkedin="https://linkedin.com/in/bobjohnson",
            company="Widgets Inc",
        ),
    }


# =============================================================================
# 1. Email duplicate detection
# =============================================================================

class TestDedupByEmail:
    """Contact with matching email is detected as duplicate."""

    def test_exact_email_match(self, existing_profiles):
        contact = {"name": "Someone Else", "email": "jane@example.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_email_case_insensitive(self, existing_profiles):
        """'John@Example.COM' and 'john@example.com' should match."""
        contact = {"name": "Whoever", "email": "Jane@Example.COM"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_email_with_leading_trailing_whitespace(self, existing_profiles):
        contact = {"name": "Whoever", "email": "  jane@example.com  "}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_different_email_no_match(self, existing_profiles):
        contact = {"name": "Jane Smith", "email": "different@email.com"}
        # Only email is checked here; name alone (without matching company)
        # is NOT enough for a duplicate.
        result = _find_duplicate(contact, existing_profiles)
        # Name matches uuid-1 but company is missing, so no dup
        assert result is None


# =============================================================================
# 2. Website duplicate detection (domain normalization)
# =============================================================================

class TestDedupByWebsite:
    """Contact with matching website (normalized) is detected as duplicate."""

    def test_exact_website_match(self, existing_profiles):
        contact = {"name": "New Person", "website": "https://www.example.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_website_without_protocol(self, existing_profiles):
        """'example.com' should match 'https://www.example.com'."""
        contact = {"name": "New Person", "website": "example.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_website_without_www(self, existing_profiles):
        contact = {"name": "New Person", "website": "https://example.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_website_http_protocol(self, existing_profiles):
        contact = {"name": "New Person", "website": "http://www.example.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_website_with_path_still_matches_domain(self, existing_profiles):
        """A URL with a path should still match on domain."""
        contact = {"name": "New Person", "website": "https://www.example.com/about"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_different_website_no_match(self, existing_profiles):
        contact = {"name": "New Person", "website": "https://different-site.com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result is None


# =============================================================================
# 3. LinkedIn duplicate detection
# =============================================================================

class TestDedupByLinkedin:
    """Contact with matching LinkedIn URL is detected as duplicate."""

    def test_exact_linkedin_match(self, existing_profiles):
        contact = {
            "name": "New Person",
            "linkedin": "https://www.linkedin.com/in/janesmith",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_linkedin_without_www(self, existing_profiles):
        contact = {
            "name": "New Person",
            "linkedin": "https://linkedin.com/in/janesmith",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_linkedin_with_trailing_slash(self, existing_profiles):
        contact = {
            "name": "New Person",
            "linkedin": "https://www.linkedin.com/in/janesmith/",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_linkedin_case_insensitive(self, existing_profiles):
        contact = {
            "name": "New Person",
            "linkedin": "https://www.LinkedIn.com/in/JaneSmith",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_different_linkedin_no_match(self, existing_profiles):
        contact = {
            "name": "New Person",
            "linkedin": "https://linkedin.com/in/someone-else",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result is None


# =============================================================================
# 4. Name + Company duplicate detection (case-insensitive)
# =============================================================================

class TestDedupByNameAndCompany:
    """Contact with matching name (case-insensitive) + company is detected
    as duplicate.

    NOTE: The contact_ingestion._find_duplicate implementation requires BOTH
    name AND company to match (case-insensitive exact match). Name alone is
    not sufficient -- this is by design to avoid false positives on common
    names.
    """

    def test_exact_name_and_company_match(self, existing_profiles):
        contact = {"name": "Jane Smith", "company": "Acme Corp"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_name_and_company_case_insensitive(self, existing_profiles):
        contact = {"name": "jane smith", "company": "acme corp"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_name_and_company_mixed_case(self, existing_profiles):
        contact = {"name": "JANE SMITH", "company": "ACME CORP"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_name_only_without_company_not_duplicate(self, existing_profiles):
        """Name alone (without company) should NOT trigger a duplicate.
        This is the key distinction from prospect_ingestion which checks
        name-only."""
        contact = {"name": "Jane Smith"}
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_name_with_different_company_not_duplicate(self, existing_profiles):
        """Same name but different company is NOT a duplicate."""
        contact = {"name": "Jane Smith", "company": "Different Company"}
        result = _find_duplicate(contact, existing_profiles)
        assert result is None


# =============================================================================
# 5. New contact passes through (no matching fields)
# =============================================================================

class TestNewContactPassesThrough:
    """Contact with no matching fields passes through as new."""

    def test_completely_new_contact(self, existing_profiles):
        contact = {
            "name": "Brand New Person",
            "email": "newperson@newdomain.com",
            "website": "https://newdomain.com",
            "linkedin": "https://linkedin.com/in/brandnewperson",
            "company": "New Company LLC",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_empty_contact(self, existing_profiles):
        """A contact with no fields at all is not a duplicate."""
        contact = {}
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_contact_with_only_name_no_company(self, existing_profiles):
        """Name without company cannot match on the name+company check."""
        contact = {"name": "Unique Name Here"}
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_no_existing_profiles(self):
        """When the existing-profiles store is empty, nothing is a dup."""
        contact = {
            "name": "Jane Smith",
            "email": "jane@example.com",
        }
        result = _find_duplicate(contact, {})
        assert result is None


# =============================================================================
# 6. Partial matches are NOT duplicates (fields checked independently)
# =============================================================================

class TestPartialMatchesNotDuplicates:
    """Partial matches (same name but different email) are NOT duplicates.
    Each field is checked independently."""

    def test_same_name_different_email_different_company(self, existing_profiles):
        """Same name, different email, different company -- not a dup."""
        contact = {
            "name": "Jane Smith",
            "email": "totally-different@other.com",
            "company": "Totally Different Corp",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_same_company_different_name(self, existing_profiles):
        """Same company but different name -- not a dup."""
        contact = {
            "name": "Different Person",
            "company": "Acme Corp",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result is None

    def test_same_name_different_everything_else(self, existing_profiles):
        """Same name but every other field is different -- not a dup
        (because company also differs)."""
        contact = {
            "name": "Jane Smith",
            "email": "jane@differentdomain.com",
            "website": "https://differentdomain.com",
            "linkedin": "https://linkedin.com/in/differentperson",
            "company": "Totally Different Inc",
        }
        result = _find_duplicate(contact, existing_profiles)
        assert result is None


# =============================================================================
# 7. URL normalization tests
# =============================================================================

class TestNormalizeDomain:
    """'https://www.example.com' and 'example.com' should produce the
    same normalized domain."""

    def test_full_url_to_domain(self):
        assert _normalize_domain("https://www.example.com") == "example.com"

    def test_bare_domain(self):
        assert _normalize_domain("example.com") == "example.com"

    def test_http_url(self):
        assert _normalize_domain("http://www.example.com") == "example.com"

    def test_url_with_path(self):
        assert _normalize_domain("https://www.example.com/about") == "example.com"

    def test_url_without_www(self):
        assert _normalize_domain("https://example.com") == "example.com"

    def test_www_only_domain(self):
        assert _normalize_domain("www.example.com") == "example.com"

    def test_empty_string(self):
        assert _normalize_domain("") == ""

    def test_none_value(self):
        assert _normalize_domain(None) == ""

    def test_whitespace(self):
        assert _normalize_domain("  https://www.example.com  ") == "example.com"

    def test_url_normalization_consistency(self):
        """All these URL forms should produce the same normalized domain."""
        variants = [
            "https://www.example.com",
            "http://www.example.com",
            "https://example.com",
            "http://example.com",
            "www.example.com",
            "example.com",
            "https://www.example.com/page",
            "  https://www.example.com  ",
        ]
        normalized = [_normalize_domain(v) for v in variants]
        assert all(n == "example.com" for n in normalized), (
            f"Not all normalized to 'example.com': {normalized}"
        )


class TestNormalizeLinkedin:
    """LinkedIn URL normalization extracts consistent path."""

    def test_full_linkedin_url(self):
        result = _normalize_linkedin("https://www.linkedin.com/in/janesmith")
        assert result == "/in/janesmith"

    def test_linkedin_without_www(self):
        result = _normalize_linkedin("https://linkedin.com/in/janesmith")
        assert result == "/in/janesmith"

    def test_linkedin_with_trailing_slash(self):
        result = _normalize_linkedin("https://www.linkedin.com/in/janesmith/")
        assert result == "/in/janesmith"

    def test_linkedin_case_insensitive(self):
        result = _normalize_linkedin("https://www.LinkedIn.com/in/JaneSmith")
        assert result == "/in/janesmith"

    def test_empty_string(self):
        assert _normalize_linkedin("") == ""

    def test_none_value(self):
        assert _normalize_linkedin(None) == ""

    def test_linkedin_normalization_consistency(self):
        """All these LinkedIn forms should produce the same normalized path."""
        variants = [
            "https://www.linkedin.com/in/janesmith",
            "https://linkedin.com/in/janesmith",
            "https://www.linkedin.com/in/janesmith/",
            "https://LinkedIn.com/in/JaneSmith",
            "http://www.linkedin.com/in/janesmith",
            "linkedin.com/in/janesmith",
        ]
        normalized = [_normalize_linkedin(v) for v in variants]
        assert all(n == "/in/janesmith" for n in normalized), (
            f"Not all normalized to '/in/janesmith': {normalized}"
        )


# =============================================================================
# 8. Email normalization (case-insensitive matching)
# =============================================================================

class TestEmailNormalization:
    """'John@Example.COM' and 'john@example.com' should match."""

    def test_uppercase_email_matches_lowercase(self, existing_profiles):
        contact = {"name": "Whoever", "email": "JANE@EXAMPLE.COM"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_mixed_case_email_matches(self, existing_profiles):
        contact = {"name": "Whoever", "email": "Jane@Example.Com"}
        result = _find_duplicate(contact, existing_profiles)
        assert result == "uuid-1"

    def test_existing_profile_with_uppercase_email(self):
        """If the EXISTING profile has uppercase email, it still matches."""
        profiles = {
            "uuid-upper": _existing_profile(
                pid="uuid-upper",
                email="JANE@EXAMPLE.COM",
            ),
        }
        contact = {"name": "Whoever", "email": "jane@example.com"}
        result = _find_duplicate(contact, profiles)
        assert result == "uuid-upper"


# =============================================================================
# 9. Priority order -- email wins over other fields
# =============================================================================

class TestDedupPriorityOrder:
    """Dedup checks are evaluated in order: email > website > linkedin >
    name+company. If multiple existing profiles could match on different
    fields, the first match (by iteration order) wins."""

    def test_email_match_takes_precedence_over_website(self):
        """When a contact matches one profile by email and another by
        website, email is checked first."""
        profiles = {
            "uuid-email": _existing_profile(
                pid="uuid-email",
                email="match@example.com",
                website="https://no-match.com",
            ),
            "uuid-website": _existing_profile(
                pid="uuid-website",
                email="no-match@different.com",
                website="https://match-site.com",
            ),
        }
        contact = {
            "name": "Someone",
            "email": "match@example.com",
            "website": "https://match-site.com",
        }
        result = _find_duplicate(contact, profiles)
        # Should match on email first (uuid-email) since email check
        # comes before website check in _find_duplicate loop iteration.
        # Note: dict iteration order in Python 3.7+ is insertion order,
        # so uuid-email is checked first and email matches.
        assert result == "uuid-email"

    def test_website_match_when_email_differs(self):
        """If email does not match but website does, website match is used."""
        profiles = {
            "uuid-web": _existing_profile(
                pid="uuid-web",
                email="other@somewhere.com",
                website="https://www.example.com",
            ),
        }
        contact = {
            "name": "Someone",
            "email": "nope@different.com",
            "website": "example.com",
        }
        result = _find_duplicate(contact, profiles)
        assert result == "uuid-web"


# =============================================================================
# 10. Batch dedup within the same ingestion run
# =============================================================================

class TestBatchDedup:
    """When multiple contacts in a single batch might duplicate each other,
    verify the logic handles it. The ingest_contacts task adds newly inserted
    profiles to the existing dict, so the second duplicate in a batch is
    caught.

    NOTE: These tests exercise _find_duplicate with a growing existing
    dict to simulate the batch behavior without needing a database.
    """

    def test_second_contact_with_same_email_is_dup(self):
        existing = {}

        # Simulate first contact being inserted
        existing["new-uuid-1"] = {
            "id": "new-uuid-1",
            "name": "Alice",
            "email": "alice@domain.com",
            "website": None,
            "linkedin": None,
            "company": None,
        }

        # Second contact with the same email
        contact2 = {"name": "Alice Again", "email": "alice@domain.com"}
        result = _find_duplicate(contact2, existing)
        assert result == "new-uuid-1"

    def test_second_contact_with_same_website_is_dup(self):
        existing = {}
        existing["new-uuid-1"] = {
            "id": "new-uuid-1",
            "name": "Alice",
            "email": None,
            "website": "https://alice-site.com",
            "linkedin": None,
            "company": None,
        }

        contact2 = {"name": "Different Alice", "website": "alice-site.com"}
        result = _find_duplicate(contact2, existing)
        assert result == "new-uuid-1"
