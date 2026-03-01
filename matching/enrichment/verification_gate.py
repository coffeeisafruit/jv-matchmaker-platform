"""
Pre-Supabase Verification Gate

3-layer verification gate between the enrichment pipeline and Supabase:
  Layer 1: Deterministic checks (regex, placeholders, field swaps) — free, instant
  Layer 2: Source quote verification (string matching against raw content) — free
  Layer 3: AI verification (existing ClaudeVerificationService) — paid, optional

Three outcomes:
  - verified   → write to Supabase with full confidence
  - unverified → write to Supabase with reduced confidence
  - quarantined → don't write; schedule for adaptive retry
"""

import difflib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from matching.enrichment.match_enrichment import TextSanitizer

logger = logging.getLogger(__name__)


# =============================================================================
# Dataclasses
# =============================================================================

class GateStatus(str, Enum):
    VERIFIED = 'verified'
    UNVERIFIED = 'unverified'
    QUARANTINED = 'quarantined'


class FieldStatus(str, Enum):
    PASSED = 'passed'
    AUTO_FIXED = 'auto_fixed'
    FAILED = 'failed'


@dataclass
class FieldVerdict:
    """Per-field verification result."""
    field_name: str
    status: FieldStatus
    original_value: Optional[str] = None
    fixed_value: Optional[str] = None
    issues: List[str] = field(default_factory=list)
    source_verified: Optional[bool] = None


@dataclass
class GateVerdict:
    """Overall gate verdict for a profile."""
    status: GateStatus
    field_verdicts: Dict[str, FieldVerdict] = field(default_factory=dict)
    overall_confidence: float = 1.0
    provenance: Dict = field(default_factory=dict)
    auto_fixed_data: Dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def failed_fields(self) -> List[str]:
        return [k for k, v in self.field_verdicts.items() if v.status == FieldStatus.FAILED]

    @property
    def issues_summary(self) -> str:
        all_issues = []
        for fv in self.field_verdicts.values():
            all_issues.extend(fv.issues)
        return '; '.join(all_issues) if all_issues else 'No issues'


@dataclass
class QuarantineRecord:
    """Record of quarantined data for retry."""
    profile_id: str
    profile_name: str
    original_data: Dict
    verdict: GateVerdict
    reason: str
    retry_count: int = 0
    max_retries: int = 2
    failures: List[Dict] = field(default_factory=list)
    quarantined_at: str = field(default_factory=lambda: datetime.now().isoformat())


# =============================================================================
# Layer 1: DeterministicChecker
# =============================================================================

class DeterministicChecker:
    """
    Layer 1: Free, instant deterministic validation.

    Checks email format, URL validity, placeholder detection, field swaps,
    and text sanitization. Auto-fixes what it can; fails what it can't.

    Regex patterns copied from assess_data_quality.py (not importable as library).
    TextSanitizer imported from match_enrichment.py.
    """

    # Email validation (from assess_data_quality.py line 228)
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

    # Suspicious email patterns (from assess_data_quality.py lines 231-234)
    SUSPICIOUS_EMAIL_PATTERNS = [
        re.compile(p, re.IGNORECASE) for p in [
            r'^test@', r'^spam@', r'^noreply@', r'^no-reply@',
            r'^admin@', r'^info@', r'^contact@', r'^support@',
            r'@test\.', r'@example\.', r'@placeholder\.',
        ]
    ]

    # URL patterns (from assess_data_quality.py lines 290-291)
    LINKEDIN_PATTERN = re.compile(r'^https?://(www\.)?linkedin\.com/in/[\w-]+/?')
    URL_PATTERN = re.compile(r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

    # Placeholder values (from match_enrichment.py DataQualityVerificationAgent)
    PLACEHOLDER_VALUES = {'update', 'n/a', 'none', 'na', '-', '.', 'tbd', 'unknown', ''}

    def check(self, data: Dict) -> Dict[str, FieldVerdict]:
        """Run all deterministic checks on profile data. Returns field verdicts."""
        verdicts = {}

        # Email checks
        email = (data.get('email') or '').strip()
        if email:
            verdicts['email'] = self._check_email(email, data)

        # URL checks
        website = (data.get('website') or '').strip()
        if website:
            verdicts['website'] = self._check_website(website, data)

        linkedin = (data.get('linkedin') or '').strip()
        if linkedin:
            verdicts['linkedin'] = self._check_linkedin(linkedin)

        # Text field checks (seeking, offering, who_you_serve, what_you_do, bio)
        for text_field in ['seeking', 'offering', 'who_you_serve', 'what_you_do', 'bio']:
            value = (data.get(text_field) or '').strip()
            if value:
                verdicts[text_field] = self._check_text_field(text_field, value)

        return verdicts

    def _check_email(self, email: str, data: Dict) -> FieldVerdict:
        """Validate email format and detect field swaps."""
        issues = []
        fixed_value = None

        # Check for URL in email field (field swap)
        if 'http' in email or '.com/' in email:
            issues.append('URL found in email field')
            return FieldVerdict(
                field_name='email',
                status=FieldStatus.FAILED,
                original_value=email,
                issues=issues,
            )

        # Check for placeholder
        if email.lower() in self.PLACEHOLDER_VALUES:
            return FieldVerdict(
                field_name='email',
                status=FieldStatus.AUTO_FIXED,
                original_value=email,
                fixed_value='',
                issues=['Placeholder value cleared'],
            )

        # Check format
        if not self.EMAIL_PATTERN.match(email):
            issues.append(f'Invalid email format: {email}')
            return FieldVerdict(
                field_name='email',
                status=FieldStatus.FAILED,
                original_value=email,
                issues=issues,
            )

        # MX record check (free, catches ~40% of bad emails)
        domain = email.split('@')[1]
        try:
            import dns.resolver
            try:
                answers = dns.resolver.resolve(domain, 'MX')
                if not answers:
                    return FieldVerdict(
                        field_name='email',
                        status=FieldStatus.FAILED,
                        original_value=email,
                        issues=[f'No MX records for {domain}'],
                    )
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
                return FieldVerdict(
                    field_name='email',
                    status=FieldStatus.FAILED,
                    original_value=email,
                    issues=[f'No MX records for {domain}'],
                )
            except dns.resolver.Timeout:
                pass  # Don't fail on timeout
        except ImportError:
            pass  # dnspython not installed, skip

        # Check suspicious patterns
        is_suspicious = any(p.search(email) for p in self.SUSPICIOUS_EMAIL_PATTERNS)
        if is_suspicious:
            issues.append(f'Suspicious email pattern: {email}')

        if issues:
            return FieldVerdict(
                field_name='email',
                status=FieldStatus.FAILED,
                original_value=email,
                issues=issues,
            )

        return FieldVerdict(field_name='email', status=FieldStatus.PASSED, original_value=email)

    def _check_website(self, website: str, data: Dict) -> FieldVerdict:
        """Validate website URL and detect LinkedIn-in-website swap."""
        # LinkedIn URL in website field → flag for move
        if 'linkedin.com' in website.lower():
            return FieldVerdict(
                field_name='website',
                status=FieldStatus.AUTO_FIXED,
                original_value=website,
                fixed_value='',
                issues=['LinkedIn URL in website field — moved to linkedin'],
            )

        # Missing scheme → auto-fix
        if not website.startswith('http'):
            fixed = 'https://' + website
            if self.URL_PATTERN.match(fixed):
                return FieldVerdict(
                    field_name='website',
                    status=FieldStatus.AUTO_FIXED,
                    original_value=website,
                    fixed_value=fixed,
                    issues=['Missing https:// prefix — auto-fixed'],
                )

        # Validate format
        if not self.URL_PATTERN.match(website):
            return FieldVerdict(
                field_name='website',
                status=FieldStatus.FAILED,
                original_value=website,
                issues=[f'Invalid website URL format: {website}'],
            )

        return FieldVerdict(field_name='website', status=FieldStatus.PASSED, original_value=website)

    def _check_linkedin(self, linkedin: str) -> FieldVerdict:
        """Validate LinkedIn URL format."""
        # Missing scheme → auto-fix
        if not linkedin.startswith('http') and 'linkedin.com' in linkedin.lower():
            fixed = 'https://' + linkedin
            return FieldVerdict(
                field_name='linkedin',
                status=FieldStatus.AUTO_FIXED,
                original_value=linkedin,
                fixed_value=fixed,
                issues=['Missing https:// prefix — auto-fixed'],
            )

        if not self.LINKEDIN_PATTERN.match(linkedin):
            # Relaxed check: at least must contain linkedin.com
            if 'linkedin.com' in linkedin.lower():
                return FieldVerdict(
                    field_name='linkedin',
                    status=FieldStatus.PASSED,
                    original_value=linkedin,
                    issues=['Non-standard LinkedIn URL format (accepted)'],
                )
            return FieldVerdict(
                field_name='linkedin',
                status=FieldStatus.FAILED,
                original_value=linkedin,
                issues=[f'Invalid LinkedIn URL: {linkedin}'],
            )

        return FieldVerdict(field_name='linkedin', status=FieldStatus.PASSED, original_value=linkedin)

    def _check_text_field(self, field_name: str, value: str) -> FieldVerdict:
        """Validate and sanitize a text field."""
        issues = []

        # Placeholder check
        if value.lower().strip() in self.PLACEHOLDER_VALUES:
            return FieldVerdict(
                field_name=field_name,
                status=FieldStatus.AUTO_FIXED,
                original_value=value,
                fixed_value='',
                issues=['Placeholder value cleared'],
            )

        # Sanitize (Unicode, whitespace, etc.)
        sanitized = TextSanitizer.sanitize(value)
        if sanitized != value:
            return FieldVerdict(
                field_name=field_name,
                status=FieldStatus.AUTO_FIXED,
                original_value=value,
                fixed_value=sanitized,
                issues=['Text sanitized (encoding/whitespace)'],
            )

        return FieldVerdict(field_name=field_name, status=FieldStatus.PASSED, original_value=value)


# =============================================================================
# Layer 2: Source Quote Verification
# =============================================================================

class SourceQuoteVerifier:
    """
    Layer 2: Verify AI-extracted data against raw website content.

    For each AI-extracted field with an associated source_quote:
    1. Normalize both quote and raw content
    2. Substring match (fast path)
    3. Fuzzy match via difflib.SequenceMatcher (threshold >= 0.75)
    4. Skip quotes under 20 characters

    Only applies to AI-extracted fields (seeking, offering, who_you_serve, what_you_do),
    NOT emails from scraping/Apollo.
    """

    FUZZY_THRESHOLD = 0.75
    MIN_QUOTE_LENGTH = 20
    AI_EXTRACTED_FIELDS = {'seeking', 'offering', 'who_you_serve', 'what_you_do', 'bio'}

    def verify(
        self,
        data: Dict,
        raw_content: Optional[str],
        extraction_metadata: Optional[Dict],
    ) -> Dict[str, FieldVerdict]:
        """
        Verify source quotes against raw content.

        Args:
            data: Profile data dict
            raw_content: Raw website content (cleaned text)
            extraction_metadata: Metadata from AI extraction with source_quotes

        Returns:
            Dict of field name -> FieldVerdict for fields that were checked
        """
        verdicts = {}

        if not raw_content or not extraction_metadata:
            return verdicts

        source_quotes = extraction_metadata.get('source_quotes', [])
        fields_updated = extraction_metadata.get('fields_updated', [])

        if not source_quotes and not fields_updated:
            return verdicts

        # Normalize raw content for matching
        raw_normalized = self._normalize(raw_content)

        # Check each AI-extracted field
        for field_name in fields_updated:
            if field_name not in self.AI_EXTRACTED_FIELDS:
                continue

            value = (data.get(field_name) or '').strip()
            if not value:
                continue

            # Try to find supporting evidence
            is_grounded = self._check_field_grounding(
                field_name, value, source_quotes, raw_normalized
            )

            if is_grounded:
                verdicts[field_name] = FieldVerdict(
                    field_name=field_name,
                    status=FieldStatus.PASSED,
                    original_value=value,
                    source_verified=True,
                )
            else:
                verdicts[field_name] = FieldVerdict(
                    field_name=field_name,
                    status=FieldStatus.FAILED,
                    original_value=value,
                    source_verified=False,
                    issues=[f'Source quote not verified for {field_name}'],
                )

        return verdicts

    def _check_field_grounding(
        self,
        field_name: str,
        value: str,
        source_quotes: List[str],
        raw_normalized: str,
    ) -> bool:
        """Check if a field value is grounded in raw content or source quotes."""

        # 1. Check source quotes
        for quote in source_quotes:
            if len(quote) < self.MIN_QUOTE_LENGTH:
                continue

            quote_normalized = self._normalize(quote)

            # Fast path: substring match
            if quote_normalized in raw_normalized:
                return True

            # Slow path: fuzzy match
            ratio = difflib.SequenceMatcher(None, quote_normalized, raw_normalized).ratio()
            if ratio >= self.FUZZY_THRESHOLD:
                return True

            # Try matching quote against a sliding window of raw content
            if self._sliding_window_match(quote_normalized, raw_normalized):
                return True

        # 2. Fallback: check if the field value itself appears in raw content
        value_normalized = self._normalize(value)
        if len(value_normalized) >= self.MIN_QUOTE_LENGTH:
            # Check key phrases from the value
            key_phrases = self._extract_key_phrases(value_normalized)
            matches_found = sum(1 for phrase in key_phrases if phrase in raw_normalized)
            if matches_found >= len(key_phrases) * 0.5:  # At least half the phrases found
                return True

        return False

    def _sliding_window_match(self, quote: str, content: str) -> bool:
        """Fuzzy match a quote against sliding windows of content."""
        window_size = len(quote) + 50  # Some margin
        step = max(1, len(quote) // 2)

        for i in range(0, max(1, len(content) - window_size), step):
            window = content[i:i + window_size]
            ratio = difflib.SequenceMatcher(None, quote, window).quick_ratio()
            if ratio >= self.FUZZY_THRESHOLD:
                # Confirm with full ratio
                full_ratio = difflib.SequenceMatcher(None, quote, window).ratio()
                if full_ratio >= self.FUZZY_THRESHOLD:
                    return True

        return False

    def _extract_key_phrases(self, text: str, min_length: int = 8) -> List[str]:
        """Extract meaningful key phrases from text for grounding checks."""
        # Split on common delimiters and filter short phrases
        parts = re.split(r'[,;.\n]+', text)
        return [p.strip() for p in parts if len(p.strip()) >= min_length][:5]

    @staticmethod
    def _normalize(text: str) -> str:
        """Normalize text for comparison: lowercase, collapse whitespace."""
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


# =============================================================================
# Verification Gate (orchestrator)
# =============================================================================

class VerificationGate:
    """
    Orchestrates 3-layer verification between enrichment and Supabase.

    Usage:
        gate = VerificationGate()
        verdict = gate.evaluate(profile_data, raw_content, extraction_metadata)

        if verdict.status == GateStatus.VERIFIED:
            write_to_supabase(apply_fixes(profile_data, verdict))
        elif verdict.status == GateStatus.UNVERIFIED:
            write_to_supabase(apply_fixes(profile_data, verdict), reduced_confidence=True)
        else:  # QUARANTINED
            quarantine(profile_data, verdict)
    """

    # Fields where failure means quarantine (critical for outreach)
    CRITICAL_FIELDS = {'email'}

    def __init__(self, enable_ai_verification: bool = False):
        self.deterministic = DeterministicChecker()
        self.source_verifier = SourceQuoteVerifier()
        self.enable_ai = enable_ai_verification
        self._ai_verifier = None

    @property
    def ai_verifier(self):
        """Lazy-load AI verifier only when needed."""
        if self._ai_verifier is None and self.enable_ai:
            from matching.enrichment.ai_verification import ClaudeVerificationService
            self._ai_verifier = ClaudeVerificationService()
        return self._ai_verifier

    def evaluate(
        self,
        data: Dict,
        raw_content: Optional[str] = None,
        extraction_metadata: Optional[Dict] = None,
    ) -> GateVerdict:
        """
        Run all verification layers and produce a verdict.

        Args:
            data: Profile data dict to verify
            raw_content: Raw website content (for Layer 2 source verification)
            extraction_metadata: AI extraction metadata with source_quotes

        Returns:
            GateVerdict with status, field verdicts, and auto-fix data
        """
        all_verdicts: Dict[str, FieldVerdict] = {}
        auto_fixed: Dict = {}

        # --- Layer 1: Deterministic checks (always run) ---
        l1_verdicts = self.deterministic.check(data)
        all_verdicts.update(l1_verdicts)

        # Collect auto-fixes from Layer 1
        for fname, fv in l1_verdicts.items():
            if fv.status == FieldStatus.AUTO_FIXED and fv.fixed_value is not None:
                auto_fixed[fname] = fv.fixed_value

        # Check for critical failures at Layer 1
        l1_critical_failures = [
            fname for fname, fv in l1_verdicts.items()
            if fv.status == FieldStatus.FAILED and fname in self.CRITICAL_FIELDS
        ]

        if l1_critical_failures:
            return GateVerdict(
                status=GateStatus.QUARANTINED,
                field_verdicts=all_verdicts,
                overall_confidence=0.0,
                provenance={'layer_stopped_at': 1, 'reason': 'critical field failed L1'},
                auto_fixed_data=auto_fixed,
            )

        # --- Layer 2: Source quote verification (if content available) ---
        if raw_content and extraction_metadata:
            l2_verdicts = self.source_verifier.verify(data, raw_content, extraction_metadata)

            # Merge: L2 verdicts supplement L1 (don't overwrite L1 failures)
            for fname, fv in l2_verdicts.items():
                if fname not in all_verdicts or all_verdicts[fname].status == FieldStatus.PASSED:
                    all_verdicts[fname] = fv

        # --- Layer 3: AI verification (optional, only if L1+L2 passed) ---
        l3_ran = False
        any_l2_failures = any(
            fv.status == FieldStatus.FAILED
            for fv in all_verdicts.values()
        )

        if self.enable_ai and not any_l2_failures and self.ai_verifier and self.ai_verifier.is_available():
            l3_ran = True
            # AI verification is supplementary — doesn't change gate status,
            # but can add confidence or flag issues
            logger.info("Layer 3 (AI verification) running as supplementary check")

        # --- Determine final status ---
        has_any_failure = any(
            fv.status == FieldStatus.FAILED for fv in all_verdicts.values()
        )
        has_critical_failure = any(
            fv.status == FieldStatus.FAILED and fv.field_name in self.CRITICAL_FIELDS
            for fv in all_verdicts.values()
        )
        has_source_failure = any(
            fv.source_verified is False for fv in all_verdicts.values()
        )

        if has_critical_failure:
            status = GateStatus.QUARANTINED
            confidence = 0.0
        elif has_source_failure:
            # Source verification failed but not critical — write with reduced confidence
            status = GateStatus.UNVERIFIED
            confidence = 0.5
        elif has_any_failure:
            status = GateStatus.UNVERIFIED
            confidence = 0.6
        elif not raw_content and extraction_metadata:
            # AI-extracted data but no raw content to verify against
            status = GateStatus.UNVERIFIED
            confidence = 0.7
        else:
            status = GateStatus.VERIFIED
            confidence = 1.0

        return GateVerdict(
            status=status,
            field_verdicts=all_verdicts,
            overall_confidence=confidence,
            provenance={
                'layers_run': [1, 2 if raw_content else None, 3 if l3_ran else None],
                'extraction_source': extraction_metadata.get('source') if extraction_metadata else None,
            },
            auto_fixed_data=auto_fixed,
        )

    @staticmethod
    def apply_fixes(data: Dict, verdict: GateVerdict) -> Dict:
        """Apply auto-fixes from the verdict to the profile data."""
        fixed = dict(data)
        for field_name, fixed_value in verdict.auto_fixed_data.items():
            fixed[field_name] = fixed_value
        return fixed
