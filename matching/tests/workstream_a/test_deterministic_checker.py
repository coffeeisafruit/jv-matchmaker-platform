"""
Tests for DeterministicChecker (Layer 1 of the Verification Gate).

Validates regex-based email/URL checks, placeholder detection, field swap
detection, and text sanitization across all profile data fields.
"""

import pytest

from matching.enrichment.verification_gate import (
    DeterministicChecker,
    FieldStatus,
    FieldVerdict,
)


@pytest.fixture
def checker():
    return DeterministicChecker()


# =========================================================================
# Email checks
# =========================================================================


def test_email_valid(checker):
    """A well-formed, non-suspicious email should PASS."""
    verdicts = checker.check({'email': 'jane.doe@realcompany.com'})
    assert verdicts['email'].status == FieldStatus.PASSED


def test_email_url_in_field_fails(checker):
    """A URL placed in the email field is a field-swap and should FAIL."""
    verdicts = checker.check({'email': 'https://example.com/contact'})
    assert verdicts['email'].status == FieldStatus.FAILED
    assert any('URL found' in i for i in verdicts['email'].issues)


def test_email_url_dotcom_slash_in_field_fails(checker):
    """A URL without scheme but with .com/ in email field should FAIL."""
    verdicts = checker.check({'email': 'example.com/about'})
    assert verdicts['email'].status == FieldStatus.FAILED


def test_email_placeholder_auto_fixed(checker):
    """Placeholder values like 'n/a' should be AUTO_FIXED to empty string."""
    for placeholder in ['n/a', 'N/A', 'none', 'tbd', 'unknown', '-']:
        verdicts = checker.check({'email': placeholder})
        assert verdicts['email'].status == FieldStatus.AUTO_FIXED
        assert verdicts['email'].fixed_value == ''


def test_email_invalid_format_fails(checker):
    """An email missing the @ or domain should FAIL."""
    verdicts = checker.check({'email': 'not-an-email'})
    assert verdicts['email'].status == FieldStatus.FAILED
    assert any('Invalid email format' in i for i in verdicts['email'].issues)


def test_email_suspicious_test_at_fails(checker):
    """Emails matching suspicious patterns like test@ should FAIL."""
    verdicts = checker.check({'email': 'test@gmail.com'})
    assert verdicts['email'].status == FieldStatus.FAILED
    assert any('Suspicious' in i for i in verdicts['email'].issues)


def test_email_suspicious_noreply_fails(checker):
    """noreply@ emails should FAIL as suspicious."""
    verdicts = checker.check({'email': 'noreply@company.com'})
    assert verdicts['email'].status == FieldStatus.FAILED


def test_email_suspicious_at_example_domain_fails(checker):
    """Emails at @example. domains should FAIL as suspicious."""
    verdicts = checker.check({'email': 'user@example.com'})
    assert verdicts['email'].status == FieldStatus.FAILED


def test_email_suspicious_info_at_fails(checker):
    """info@ emails should FAIL as suspicious."""
    verdicts = checker.check({'email': 'info@company.org'})
    assert verdicts['email'].status == FieldStatus.FAILED


def test_email_empty_not_checked(checker):
    """Empty or None email should not produce a verdict."""
    verdicts = checker.check({'email': ''})
    assert 'email' not in verdicts

    verdicts = checker.check({'email': None})
    assert 'email' not in verdicts

    verdicts = checker.check({})
    assert 'email' not in verdicts


# =========================================================================
# Website checks
# =========================================================================


def test_website_valid(checker):
    """A properly formatted website URL should PASS."""
    verdicts = checker.check({'website': 'https://www.example-site.com'})
    assert verdicts['website'].status == FieldStatus.PASSED


def test_website_linkedin_url_auto_fixed(checker):
    """LinkedIn URL in the website field should be AUTO_FIXED (moved)."""
    verdicts = checker.check({'website': 'https://linkedin.com/in/janedoe'})
    assert verdicts['website'].status == FieldStatus.AUTO_FIXED
    assert verdicts['website'].fixed_value == ''
    assert any('LinkedIn' in i for i in verdicts['website'].issues)


def test_website_missing_scheme_auto_fixed(checker):
    """Website without http(s) prefix should be AUTO_FIXED with https://."""
    verdicts = checker.check({'website': 'www.example.com'})
    assert verdicts['website'].status == FieldStatus.AUTO_FIXED
    assert verdicts['website'].fixed_value == 'https://www.example.com'


def test_website_invalid_fails(checker):
    """A completely invalid website value should FAIL."""
    verdicts = checker.check({'website': 'not a url at all'})
    assert verdicts['website'].status == FieldStatus.FAILED
    assert any('Invalid website URL' in i for i in verdicts['website'].issues)


def test_website_empty_not_checked(checker):
    """Empty or None website should not produce a verdict."""
    verdicts = checker.check({'website': ''})
    assert 'website' not in verdicts


# =========================================================================
# LinkedIn checks
# =========================================================================


def test_linkedin_valid_standard(checker):
    """A standard LinkedIn /in/ URL should PASS."""
    verdicts = checker.check({'linkedin': 'https://www.linkedin.com/in/jane-doe'})
    assert verdicts['linkedin'].status == FieldStatus.PASSED


def test_linkedin_missing_scheme_auto_fixed(checker):
    """LinkedIn URL without http should be AUTO_FIXED with https://."""
    verdicts = checker.check({'linkedin': 'www.linkedin.com/in/janedoe'})
    assert verdicts['linkedin'].status == FieldStatus.AUTO_FIXED
    assert verdicts['linkedin'].fixed_value == 'https://www.linkedin.com/in/janedoe'


def test_linkedin_nonstandard_but_contains_linkedin_passes(checker):
    """Non-standard LinkedIn URL (e.g. /company/) that still contains
    linkedin.com should PASS with relaxed check."""
    verdicts = checker.check({'linkedin': 'https://linkedin.com/company/acme-corp'})
    assert verdicts['linkedin'].status == FieldStatus.PASSED
    assert any('Non-standard' in i for i in verdicts['linkedin'].issues)


def test_linkedin_invalid_no_linkedin_domain_fails(checker):
    """A URL that does not contain linkedin.com should FAIL."""
    verdicts = checker.check({'linkedin': 'https://twitter.com/janedoe'})
    assert verdicts['linkedin'].status == FieldStatus.FAILED
    assert any('Invalid LinkedIn URL' in i for i in verdicts['linkedin'].issues)


# =========================================================================
# Text field checks (seeking, offering, who_you_serve, what_you_do, bio)
# =========================================================================


def test_text_placeholder_auto_fixed(checker):
    """Placeholder text values should be AUTO_FIXED to empty string."""
    verdicts = checker.check({'seeking': 'TBD'})
    assert verdicts['seeking'].status == FieldStatus.AUTO_FIXED
    assert verdicts['seeking'].fixed_value == ''


def test_text_clean_passes(checker):
    """Normal clean text should PASS without modification."""
    verdicts = checker.check({'offering': 'Strategic consulting for mid-market firms'})
    assert verdicts['offering'].status == FieldStatus.PASSED


def test_text_sanitized_auto_fixed(checker):
    """Text with problematic Unicode should be AUTO_FIXED after sanitization."""
    # Use an em-dash (\u2014) which TextSanitizer replaces with a hyphen
    verdicts = checker.check({'bio': 'Building partnerships \u2014 one deal at a time'})
    assert verdicts['bio'].status == FieldStatus.AUTO_FIXED
    assert '\u2014' not in verdicts['bio'].fixed_value
    assert '-' in verdicts['bio'].fixed_value


def test_text_empty_not_checked(checker):
    """Empty or None text fields should not produce a verdict."""
    verdicts = checker.check({'seeking': '', 'offering': None})
    assert 'seeking' not in verdicts
    assert 'offering' not in verdicts


def test_no_fields_returns_empty(checker):
    """Providing no recognized fields should return an empty verdicts dict."""
    verdicts = checker.check({})
    assert verdicts == {}


def test_multiple_fields_checked_together(checker):
    """Multiple fields in a single data dict should each get their own verdict."""
    data = {
        'email': 'real@company.com',
        'website': 'https://company.com',
        'linkedin': 'https://www.linkedin.com/in/someone',
        'seeking': 'Growth partners',
    }
    verdicts = checker.check(data)
    assert len(verdicts) == 4
    assert all(v.status == FieldStatus.PASSED for v in verdicts.values())
