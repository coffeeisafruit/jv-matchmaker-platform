"""
Tests for the 6 rule-based verification sub-agents and the MatchVerificationAgent orchestrator.

Covers:
- EncodingVerificationAgent  (Unicode / non-ASCII detection)
- FormattingVerificationAgent (structure checks)
- ContentVerificationAgent   (empty/short/generic content)
- CapitalizationVerificationAgent (bullet capitalisation)
- TruncationVerificationAgent (word-dash truncation)
- DataQualityVerificationAgent (boilerplate / placeholder data)
- MatchVerificationAgent      (orchestration, scoring, status)
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import pytest

from matching.enrichment.match_enrichment import (
    EncodingVerificationAgent,
    FormattingVerificationAgent,
    ContentVerificationAgent,
    CapitalizationVerificationAgent,
    TruncationVerificationAgent,
    DataQualityVerificationAgent,
    MatchVerificationAgent,
    VerificationStatus,
    EnrichedMatch,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_match(**overrides):
    defaults = dict(
        name='Bob Smith', company='BobCo', email='bob@bobco.com',
        linkedin='https://linkedin.com/in/bob', website='https://bobco.com',
        niche='coaching', list_size=5000, social_reach=0, score=0.85,
        who_they_serve='entrepreneurs', what_they_do='business coaching',
        seeking='JV partners', offering='email list access',
        notes='', why_fit='Bob serves entrepreneurs who need coaching.',
        mutual_benefit='WHAT BOB GETS:\n* Access\n\nWHAT JANET GETS:\n* Promotion',
        outreach_message='Subject: Hi Bob\n\nHi Bob,\n\nLooking forward.',
        verification_score=0, verification_passed=False,
    )
    defaults.update(overrides)
    return EnrichedMatch(**defaults)


# ===================================================================
# EncodingVerificationAgent
# ===================================================================

class TestEncodingVerificationAgent:
    agent = EncodingVerificationAgent()

    def test_clean_ascii_text_no_issues(self):
        match = _make_match()
        issues = self.agent.verify(match)
        encoding_issues = [i for i in issues if i.agent == 'encoding']
        assert len(encoding_issues) == 0

    def test_em_dash_flagged_as_critical(self):
        match = _make_match(
            why_fit='Bob serves entrepreneurs \u2014 they need coaching.',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities

    def test_smart_quotes_flagged(self):
        match = _make_match(
            why_fit='\u201cBob serves entrepreneurs who need coaching.\u201d',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0


# ===================================================================
# FormattingVerificationAgent
# ===================================================================

class TestFormattingVerificationAgent:
    agent = FormattingVerificationAgent()

    def test_proper_mutual_benefit_no_issues(self):
        match = _make_match(
            mutual_benefit=(
                'WHAT BOB GETS:\n* Access to audience\n* Increased visibility\n\n'
                'WHAT JANET GETS:\n* Product promotion\n* New subscribers'
            ),
        )
        issues = self.agent.verify(match)
        fmt_issues = [i for i in issues if i.agent == 'formatting']
        assert len(fmt_issues) == 0

    def test_missing_gets_structure_critical(self):
        match = _make_match(
            mutual_benefit='Both parties will benefit from the partnership greatly.',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities

    def test_one_party_only_critical(self):
        match = _make_match(
            mutual_benefit='WHAT BOB GETS:\n* Access to audience',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities

    def test_outreach_missing_subject_warning(self):
        match = _make_match(
            outreach_message='Hi Bob,\n\nLooking forward to connecting.',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'warning' in severities or 'critical' in severities


# ===================================================================
# ContentVerificationAgent
# ===================================================================

class TestContentVerificationAgent:
    agent = ContentVerificationAgent()

    def test_short_why_fit_critical(self):
        match = _make_match(why_fit='Good fit.')
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities

    def test_generic_phrases_warning(self):
        match = _make_match(
            why_fit=(
                'Bob is a great synergy for us. A perfect fit for our programme. '
                'This is a win-win opportunity that offers amazing value.'
            ),
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0

    def test_name_not_mentioned_warning(self):
        match = _make_match(
            why_fit='This partner serves entrepreneurs who need coaching programmes.',
        )
        issues = self.agent.verify(match)
        # Should flag that the partner name is not mentioned
        assert len(issues) > 0


# ===================================================================
# CapitalizationVerificationAgent
# ===================================================================

class TestCapitalizationVerificationAgent:
    agent = CapitalizationVerificationAgent()

    def test_lowercase_after_bullet_warning(self):
        match = _make_match(
            mutual_benefit=(
                'WHAT BOB GETS:\n* access to audience\n* increased visibility\n\n'
                'WHAT JANET GETS:\n* promotion\n* new subscribers'
            ),
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'warning' in severities

    def test_capitalized_bullets_no_issues(self):
        match = _make_match(
            mutual_benefit=(
                'WHAT BOB GETS:\n* Access to audience\n* Increased visibility\n\n'
                'WHAT JANET GETS:\n* Promotion\n* New subscribers'
            ),
        )
        issues = self.agent.verify(match)
        cap_issues = [i for i in issues if i.agent == 'capitalization']
        assert len(cap_issues) == 0


# ===================================================================
# TruncationVerificationAgent
# ===================================================================

class TestTruncationVerificationAgent:
    agent = TruncationVerificationAgent()

    def test_dash_truncated_word_critical(self):
        match = _make_match(
            why_fit='Bob offers a great opportu\u2014',
        )
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities

    def test_clean_text_no_issues(self):
        match = _make_match()
        issues = self.agent.verify(match)
        trunc_issues = [i for i in issues if i.agent == 'truncation']
        assert len(trunc_issues) == 0


# ===================================================================
# DataQualityVerificationAgent
# ===================================================================

class TestDataQualityVerificationAgent:
    agent = DataQualityVerificationAgent()

    def test_linkedin_in_website_warning(self):
        match = _make_match(website='https://linkedin.com/in/bob')
        issues = self.agent.verify(match)
        assert len(issues) > 0

    def test_placeholder_email_warning(self):
        match = _make_match(email='Update')
        issues = self.agent.verify(match)
        assert len(issues) > 0

    def test_url_in_email_warning(self):
        match = _make_match(email='https://bobco.com')
        issues = self.agent.verify(match)
        assert len(issues) > 0

    def test_empty_name_critical(self):
        match = _make_match(name='')
        issues = self.agent.verify(match)
        assert len(issues) > 0
        severities = [i.severity for i in issues]
        assert 'critical' in severities


# ===================================================================
# MatchVerificationAgent (orchestrator)
# ===================================================================

class TestMatchVerificationAgent:
    agent = MatchVerificationAgent()

    def test_clean_match_passes(self):
        match = _make_match(
            why_fit='Bob Smith serves entrepreneurs who need business coaching and has a 5000-person email list.',
            mutual_benefit=(
                'WHAT BOB GETS:\n* Access to audience\n* Increased visibility\n\n'
                'WHAT JANET GETS:\n* Product promotion\n* New subscribers'
            ),
            outreach_message='Subject: Partnership with Bob\n\nHi Bob,\n\nLooking forward to connecting.',
        )
        result = self.agent.verify(match)
        assert result.score >= 85
        assert result.status == VerificationStatus.PASSED

    def test_critical_issues_reduce_score(self):
        # Force a critical issue via encoding (em-dash)
        match = _make_match(
            why_fit='Bob serves entrepreneurs \u2014 they need coaching programmes.',
            mutual_benefit=(
                'WHAT BOB GETS:\n* Access to audience\n* Increased visibility\n\n'
                'WHAT JANET GETS:\n* Product promotion\n* New subscribers'
            ),
            outreach_message='Subject: Hi Bob\n\nHi Bob,\n\nLooking forward.',
        )
        result = self.agent.verify(match)
        # At least one critical issue should subtract 15 points
        assert result.score <= 85

    def test_verify_and_fix_sanitizes_text(self):
        match = _make_match(
            why_fit='Bob serves entrepreneurs \u2014 they need coaching\u2019s best.',
            mutual_benefit=(
                'WHAT BOB GETS:\n* Access to audience\n* Increased visibility\n\n'
                'WHAT JANET GETS:\n* Product promotion\n* New subscribers'
            ),
            outreach_message='Subject: Hi Bob\n\nHi Bob,\n\nLooking forward.',
        )
        fixed_match, result = self.agent.verify_and_fix(match)
        # After fix, em-dash and smart apostrophe should be replaced
        assert '\u2014' not in fixed_match.why_fit
        assert '\u2019' not in fixed_match.why_fit
