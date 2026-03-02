"""
Production-readiness standard for enriched client profiles.

Defines field requirements, deterministic validators, and scoring logic
for MemberReport.client_profile, MemberReport.outreach_templates, and
ReportPartner card data.  No Django imports -- works with plain dicts.

Scoring breakdown (0-100):
  - Required fields:   60 pts  (equal share per required field)
  - Recommended fields: 30 pts (weighted share per recommended field)
  - Partner card quality: 10 pts (% of cards passing card standard)

A profile passes only at 100/100 (zero issues).
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

class FieldRequirement(Enum):
    REQUIRED = 'required'          # Blocks production
    RECOMMENDED = 'recommended'    # Improves score, doesn't block


@dataclass
class FieldSpec:
    """Specification for a single profile field."""
    name: str
    requirement: FieldRequirement
    min_length: int = 0        # Min character count for string fields
    min_items: int = 0         # Min items for list fields
    validators: List[str] = field(default_factory=list)
    weight: float = 1.0        # Weight in score calculation
    description: str = ''      # Human-readable description of what's expected


@dataclass
class ValidationIssue:
    """A single validation problem found during profile checks."""
    field: str
    message: str
    requirement: FieldRequirement


@dataclass
class ProfileValidationResult:
    """Overall result of profile validation."""
    passed: bool               # True only when zero issues (100/100)
    score: float               # 0-100
    issues: List[ValidationIssue]
    missing_fields: List[str]  # Fields that are empty/missing


# =============================================================================
# Standard Definitions
# =============================================================================

PROFILE_STANDARD: dict[str, FieldSpec] = {
    # --- REQUIRED (blocks production) ---
    'contact_name': FieldSpec(
        name='contact_name',
        requirement=FieldRequirement.REQUIRED,
        min_length=3,
        validators=['not_category_data', 'is_person_name'],
        description='Full name of the client contact (e.g. "Janet Bray Attwood")',
    ),
    'program_name': FieldSpec(
        name='program_name',
        requirement=FieldRequirement.REQUIRED,
        min_length=3,
        validators=['not_person_name_only', 'not_category_data'],
        description='Name of the program or company (not just the person\'s name)',
    ),
    'program_focus': FieldSpec(
        name='program_focus',
        requirement=FieldRequirement.REQUIRED,
        min_length=3,
        validators=['not_generic_fallback'],
        description='Primary focus area (not the default "Business Growth")',
    ),
    'target_audience': FieldSpec(
        name='target_audience',
        requirement=FieldRequirement.REQUIRED,
        min_length=10,
        validators=['not_generic_fallback'],
        description='Specific target audience (not the default "Entrepreneurs & Business Owners")',
    ),
    'about_story': FieldSpec(
        name='about_story',
        requirement=FieldRequirement.REQUIRED,
        min_length=50,
        validators=['not_auto_generated_placeholder', 'not_truncated'],
        description='Substantive bio/story (50+ chars, not auto-generated placeholder)',
    ),
    'credentials': FieldSpec(
        name='credentials',
        requirement=FieldRequirement.REQUIRED,
        min_items=2,
        validators=['not_truncated'],
        description='At least 2 meaningful credentials',
    ),
    'seeking_goals': FieldSpec(
        name='seeking_goals',
        requirement=FieldRequirement.REQUIRED,
        min_items=1,
        description='At least 1 partnership goal',
    ),
    'contact_method': FieldSpec(
        name='contact_method',
        requirement=FieldRequirement.REQUIRED,
        description='At least one of contact_email or booking_link must exist',
    ),
    'partner_count': FieldSpec(
        name='partner_count',
        requirement=FieldRequirement.REQUIRED,
        description='At least 10 partner cards in the report',
    ),

    # --- RECOMMENDED (improve score) ---
    'offers_partners': FieldSpec(
        name='offers_partners',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=2,
        weight=0.8,
        description='At least 2 value-proposition items for partners',
    ),
    'tiers': FieldSpec(
        name='tiers',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=2,
        weight=0.6,
        description='At least 2 program tiers',
    ),
    'key_message_headline': FieldSpec(
        name='key_message_headline',
        requirement=FieldRequirement.RECOMMENDED,
        min_length=10,
        weight=0.8,
        description='Headline message (10+ chars)',
    ),
    'key_message_points': FieldSpec(
        name='key_message_points',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=3,
        weight=0.6,
        description='At least 3 key message bullet points',
    ),
    'partner_deliverables': FieldSpec(
        name='partner_deliverables',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=3,
        weight=0.7,
        description='At least 3 deliverables for partners',
    ),
    'why_converts': FieldSpec(
        name='why_converts',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=2,
        weight=0.7,
        description='At least 2 conversion reasons',
    ),
    'launch_stats': FieldSpec(
        name='launch_stats',
        requirement=FieldRequirement.RECOMMENDED,
        weight=0.5,
        description='Launch statistics (must not be None)',
    ),
    'faqs': FieldSpec(
        name='faqs',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=3,
        weight=0.7,
        description='At least 3 frequently asked questions',
    ),
    'resource_links': FieldSpec(
        name='resource_links',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=2,
        weight=0.6,
        description='At least 2 resource links',
    ),
    'perfect_for': FieldSpec(
        name='perfect_for',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=3,
        weight=0.6,
        description='At least 3 "perfect for" descriptions',
    ),
    'shared_stage': FieldSpec(
        name='shared_stage',
        requirement=FieldRequirement.RECOMMENDED,
        min_items=1,
        weight=0.4,
        description='At least 1 shared stage / event appearance',
    ),
}


OUTREACH_STANDARD = {
    'initial': {
        'min_words': 120,
        'max_words': 250,
        'description': 'Initial outreach email template',
    },
    'followup': {
        'min_words': 50,
        'max_words': 100,
        'description': 'Follow-up email template',
    },
}


PARTNER_CARD_STANDARD: dict[str, FieldSpec] = {
    'name': FieldSpec(
        name='name',
        requirement=FieldRequirement.REQUIRED,
        min_length=3,
        description='Partner name',
    ),
    'tagline_or_company': FieldSpec(
        name='tagline_or_company',
        requirement=FieldRequirement.REQUIRED,
        description='Either tagline or company must be non-empty',
    ),
    'audience': FieldSpec(
        name='audience',
        requirement=FieldRequirement.REQUIRED,
        min_length=20,
        description='Audience description (20+ chars)',
    ),
    'why_fit': FieldSpec(
        name='why_fit',
        requirement=FieldRequirement.REQUIRED,
        min_length=30,
        validators=['has_three_part_why_fit'],
        description='Why-fit narrative with shared connection + alignment + suggested next step',
    ),
    'contact_info': FieldSpec(
        name='contact_info',
        requirement=FieldRequirement.REQUIRED,
        description='At least 1 of: email, phone, linkedin',
    ),
    'detail_note': FieldSpec(
        name='detail_note',
        requirement=FieldRequirement.RECOMMENDED,
        description='Optional italicised note below why-fit',
    ),
}


# =============================================================================
# Known generic boilerplate patterns
# =============================================================================

_KNOWN_GENERIC_INITIAL_PATTERNS = [
    # The default template from _build_outreach_templates
    re.compile(
        r"I came across your work and love what you're doing for \[their audience\]",
        re.IGNORECASE,
    ),
    re.compile(
        r"Would you be open to a quick call to explore some partnership ideas\?",
        re.IGNORECASE,
    ),
]

_KNOWN_GENERIC_FOLLOWUP_PATTERNS = [
    re.compile(
        r"Just following up on my earlier message.*explore how we might support "
        r"each other's communities",
        re.IGNORECASE | re.DOTALL,
    ),
]

_PLACEHOLDER_RE = re.compile(
    r'\[(?:their audience|Insert booking link|Partner Name|partner name|'
    r'Partner\'s Name|their name|Company Name|company name|your offering|'
    r'specific detail|insert|YOUR)\b[^\]]*\]',
    re.IGNORECASE,
)

_VAGUE_REFERENCE_RE = re.compile(
    r'\b(?:your work|your audience|your community|your people)\b',
    re.IGNORECASE,
)


# =============================================================================
# Validator Functions
# =============================================================================

def is_person_name(value: str) -> list[str]:
    """Check value looks like a person name (2+ words, not all caps, not category data)."""
    issues: list[str] = []
    if not value or not value.strip():
        issues.append('Name is empty')
        return issues

    stripped = value.strip()

    # All caps (e.g. "JOHN SMITH" -- likely OCR or import artifact)
    if stripped == stripped.upper() and len(stripped) > 5:
        issues.append(f'Name appears to be ALL CAPS: "{stripped}"')

    # Must have at least 2 words
    words = stripped.split()
    if len(words) < 2:
        issues.append(f'Name has only {len(words)} word(s); expected at least 2')

    # Category-style names: "Business Skills, Fitness, Life"
    cat_issues = not_category_data(stripped)
    issues.extend(cat_issues)

    return issues


def not_category_data(value: str) -> list[str]:
    """Detect comma-separated category strings like 'Business Skills, Fitness, Life'."""
    issues: list[str] = []
    if not value:
        return issues

    # Two or more commas is a strong signal of category data
    if value.count(',') >= 2:
        issues.append(
            f'Value looks like comma-separated categories: "{value[:80]}"'
        )
        return issues

    # Known category prefixes
    category_prefixes = [
        'business skills', 'more info', 'health', 'fitness',
        'lifestyle', 'mental health', 'self improvement',
        'personal development', 'marketing', 'sales',
    ]
    lower = value.lower().strip()
    for prefix in category_prefixes:
        if lower == prefix or (lower.startswith(prefix) and len(lower) < len(prefix) + 5):
            issues.append(f'Value looks like a category label: "{value}"')
            break

    return issues


def not_person_name_only(value: str, contact_name: str = '') -> list[str]:
    """Check program_name isn't just the person's name."""
    issues: list[str] = []
    if not value or not contact_name:
        return issues

    # Normalize for comparison
    v_lower = value.strip().lower()
    c_lower = contact_name.strip().lower()

    if v_lower == c_lower:
        issues.append(
            f'Program name "{value}" is identical to the contact name'
        )
    elif v_lower in c_lower or c_lower in v_lower:
        # One is a substring of the other (e.g. "Janet" vs "Janet Bray Attwood")
        # Only flag if the program name has no extra business words
        business_words = {
            'inc', 'llc', 'group', 'partners', 'academy', 'institute',
            'consulting', 'coaching', 'media', 'international', 'global',
            'method', 'system', 'program', 'foundation', 'network',
        }
        has_business_word = any(
            bw in v_lower for bw in business_words
        )
        if not has_business_word:
            issues.append(
                f'Program name "{value}" appears to be just the person\'s name '
                f'without a business identifier'
            )

    return issues


def not_generic_fallback(value: str, field_name: str = '') -> list[str]:
    """Detect known generic fallback values."""
    issues: list[str] = []
    if not value:
        return issues

    KNOWN_FALLBACKS: dict[str, list[str]] = {
        'program_focus': ['Business Growth'],
        'target_audience': ['Entrepreneurs & Business Owners'],
    }

    fallbacks = KNOWN_FALLBACKS.get(field_name, [])
    stripped = value.strip()
    for fallback in fallbacks:
        if stripped == fallback:
            issues.append(
                f'"{stripped}" is a known generic fallback for {field_name}'
            )
            break

    return issues


def not_auto_generated_placeholder(value: str, contact_name: str = '') -> list[str]:
    """Detect auto-generated text like '[Name] is the founder of [Company].'."""
    issues: list[str] = []
    if not value:
        return issues

    stripped = value.strip()

    # Pattern: "<Name> is the founder of <Company>." with nothing else
    if contact_name:
        first_name = contact_name.split()[0] if contact_name.split() else ''
        # Exact match against the known auto-generated template
        pattern = re.compile(
            rf'^{re.escape(contact_name)}\s+is\s+the\s+founder\s+of\s+\S.*\.\s*$',
            re.IGNORECASE,
        )
        if pattern.match(stripped) and len(stripped) < 120:
            issues.append(
                'About story looks auto-generated '
                f'("{stripped[:60]}...")'
            )

    # Generic single-sentence bios that are clearly placeholder
    placeholder_patterns = [
        re.compile(r'^.{3,30}\s+is\s+(?:a|the|an)\s+.{3,40}\.\s*$', re.IGNORECASE),
    ]
    for pat in placeholder_patterns:
        if pat.match(stripped) and len(stripped) < 80:
            issues.append(
                f'About story is suspiciously short and formulaic '
                f'("{stripped[:60]}")'
            )
            break

    return issues


def not_truncated(value, **kwargs) -> list[str]:
    """Detect text that ends mid-word (truncation artifacts).

    Works for both str values and list values (checks each item).
    """
    issues: list[str] = []

    texts_to_check: list[str] = []
    if isinstance(value, str):
        texts_to_check = [value]
    elif isinstance(value, list):
        texts_to_check = [str(item) for item in value if item]

    for text in texts_to_check:
        stripped = text.strip()
        if not stripped or len(stripped) < 10:
            continue

        # Ends with a hyphen (word-break truncation)
        if stripped.endswith('-'):
            issues.append(f'Text appears truncated (ends with hyphen): "...{stripped[-30:]}"')
            continue

        # Ends mid-word: last char is alphanumeric and not preceded by
        # a sentence-ending pattern
        last_char = stripped[-1]
        if last_char.isalpha() and len(stripped) > 30:
            # Check it doesn't end with a normal word
            # Normal endings: period, question mark, exclamation, closing paren,
            # closing quote, or a short final word
            last_word = stripped.split()[-1] if stripped.split() else ''
            # If the last word is very long (12+ chars) and no punctuation,
            # it's more likely truncated.  We use 12 to avoid false positives
            # on legitimate long words like "methodology" or "transformation".
            if len(last_word) > 12 and not any(c in last_word for c in '.!?,;:)"\']'):
                issues.append(
                    f'Text may be truncated (ends mid-word): '
                    f'"...{stripped[-40:]}"'
                )

    return issues


def has_unfilled_placeholders(value: str) -> list[str]:
    """Detect [their audience], [Insert booking link], [Partner Name] etc."""
    issues: list[str] = []
    if not value:
        return issues

    matches = _PLACEHOLDER_RE.findall(value)
    if matches:
        issues.append(
            f'Contains unfilled placeholder(s): {", ".join(matches)}'
        )

    return issues


def has_three_part_why_fit(value: str) -> list[str]:
    """Check why_fit has: shared audience/connection + alignment + suggested next step.

    Looks for patterns like "Suggested next step:" or "Explore a joint" or
    similar language indicating a concrete next action.
    """
    issues: list[str] = []
    if not value:
        issues.append('why_fit is empty')
        return issues

    lower = value.lower()

    # Check for suggested next step patterns
    next_step_patterns = [
        'suggested next step',
        'next step:',
        'explore a joint',
        'explore a co-',
        'consider a joint',
        'consider a co-',
        'guest appearance',
        'guest spot',
        'co-hosted',
        'co-host',
        'cross-promot',
        'affiliate',
        'partnership:',
        'joint webinar',
        'joint workshop',
        'joint event',
        'summit speak',
        'podcast appear',
        'interview swap',
        'bundle',
        'referral',
    ]

    has_next_step = any(pat in lower for pat in next_step_patterns)
    if not has_next_step:
        issues.append(
            'why_fit is missing a suggested next step '
            '(e.g. "Suggested next step: Explore a joint webinar")'
        )

    return issues


# =============================================================================
# Internal Helpers
# =============================================================================

def _get_field_value(profile: dict, field_name: str):
    """Retrieve a field value from a profile dict, handling special composite fields."""
    if field_name == 'contact_method':
        email = (profile.get('contact_email') or '').strip()
        booking = (profile.get('booking_link') or '').strip()
        # Also check main_website as a last resort for booking
        if not booking:
            booking = (profile.get('main_website') or '').strip()
        return email or booking or ''

    if field_name == 'about_story':
        # Check both about_story and about_story_paragraphs
        story = profile.get('about_story', '')
        if not story:
            paragraphs = profile.get('about_story_paragraphs', [])
            if paragraphs:
                story = '\n\n'.join(paragraphs)
        return story or ''

    if field_name == 'key_message_headline':
        # Could be stored as key_message_headline or key_message
        return (
            profile.get('key_message_headline')
            or profile.get('key_message')
            or ''
        )

    return profile.get(field_name)


def _check_field_length(value, spec: FieldSpec) -> list[str]:
    """Check min_length for strings or min_items for lists."""
    issues: list[str] = []

    if spec.min_length > 0 and isinstance(value, str):
        if len(value.strip()) < spec.min_length:
            issues.append(
                f'{spec.name} is too short ({len(value.strip())} chars, '
                f'need {spec.min_length}+)'
            )

    if spec.min_items > 0 and isinstance(value, list):
        non_empty = [item for item in value if item]
        if len(non_empty) < spec.min_items:
            issues.append(
                f'{spec.name} has too few items ({len(non_empty)}, '
                f'need {spec.min_items}+)'
            )

    return issues


def _run_validator(
    validator_name: str,
    value,
    profile: dict,
) -> list[str]:
    """Dispatch to a named validator function."""
    contact_name = (profile.get('contact_name') or '').strip()

    if validator_name == 'is_person_name':
        return is_person_name(value if isinstance(value, str) else str(value))

    if validator_name == 'not_category_data':
        return not_category_data(value if isinstance(value, str) else str(value))

    if validator_name == 'not_person_name_only':
        return not_person_name_only(
            value if isinstance(value, str) else str(value),
            contact_name=contact_name,
        )

    if validator_name == 'not_generic_fallback':
        return not_generic_fallback(
            value if isinstance(value, str) else str(value),
            field_name=_find_field_name_for_validator(validator_name, profile),
        )

    if validator_name == 'not_auto_generated_placeholder':
        return not_auto_generated_placeholder(
            value if isinstance(value, str) else str(value),
            contact_name=contact_name,
        )

    if validator_name == 'not_truncated':
        return not_truncated(value)

    if validator_name == 'has_three_part_why_fit':
        return has_three_part_why_fit(
            value if isinstance(value, str) else str(value),
        )

    logger.warning("Unknown validator: %s", validator_name)
    return []


def _find_field_name_for_validator(validator_name: str, profile: dict) -> str:
    """Find which field name is calling this validator (for context-aware validators)."""
    # Walk through the standard to find which field references this validator
    # This is used by not_generic_fallback to know which fallbacks to check
    for fname, spec in PROFILE_STANDARD.items():
        if validator_name in spec.validators:
            value = _get_field_value(profile, fname)
            if value:
                return fname
    return ''


def _is_field_empty(value) -> bool:
    """Check if a field value should be considered empty/missing."""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    return False


def _word_count(text: str) -> int:
    """Count words in a string."""
    return len(text.split())


# =============================================================================
# Main Validation Function
# =============================================================================

def validate_profile(
    client_profile: dict,
    outreach_templates: dict,
    partner_count: int = 0,
    partner_cards: Optional[list[dict]] = None,
) -> ProfileValidationResult:
    """Validate a client profile against the production standard.

    Args:
        client_profile: The MemberReport.client_profile JSON dict.
        outreach_templates: The MemberReport.outreach_templates JSON dict.
        partner_count: Number of ReportPartner records for this report.
        partner_cards: Optional list of partner card dicts for card-level
            validation.

    Returns:
        ProfileValidationResult with pass/fail, score, and issues.
    """
    all_issues: list[ValidationIssue] = []
    missing_fields: list[str] = []

    # Separate required and recommended specs
    required_specs: list[FieldSpec] = []
    recommended_specs: list[FieldSpec] = []

    for fname, spec in PROFILE_STANDARD.items():
        if spec.requirement == FieldRequirement.REQUIRED:
            required_specs.append(spec)
        else:
            recommended_specs.append(spec)

    # --- Score required fields (60 points) ---
    required_total = 60.0
    points_per_required = required_total / len(required_specs) if required_specs else 0
    required_score = 0.0

    for spec in required_specs:
        field_passed = True

        # Special handling for partner_count
        if spec.name == 'partner_count':
            if partner_count < 10:
                field_passed = False
                all_issues.append(ValidationIssue(
                    field='partner_count',
                    message=f'Only {partner_count} partners (need 10+)',
                    requirement=FieldRequirement.REQUIRED,
                ))
                missing_fields.append('partner_count')
            if field_passed:
                required_score += points_per_required
            continue

        # Special handling for contact_method
        if spec.name == 'contact_method':
            email = (client_profile.get('contact_email') or '').strip()
            booking = (client_profile.get('booking_link') or '').strip()
            if not email and not booking:
                field_passed = False
                all_issues.append(ValidationIssue(
                    field='contact_method',
                    message='Neither contact_email nor booking_link is set',
                    requirement=FieldRequirement.REQUIRED,
                ))
                missing_fields.append('contact_method')
            if field_passed:
                required_score += points_per_required
            continue

        value = _get_field_value(client_profile, spec.name)

        # Check empty
        if _is_field_empty(value):
            field_passed = False
            all_issues.append(ValidationIssue(
                field=spec.name,
                message=f'{spec.name} is missing or empty',
                requirement=FieldRequirement.REQUIRED,
            ))
            missing_fields.append(spec.name)
            continue

        # Check length / item count
        length_issues = _check_field_length(value, spec)
        if length_issues:
            field_passed = False
            for msg in length_issues:
                all_issues.append(ValidationIssue(
                    field=spec.name,
                    message=msg,
                    requirement=FieldRequirement.REQUIRED,
                ))

        # Run validators
        for validator_name in spec.validators:
            validator_issues = _run_validator(validator_name, value, client_profile)
            if validator_issues:
                field_passed = False
                for msg in validator_issues:
                    all_issues.append(ValidationIssue(
                        field=spec.name,
                        message=msg,
                        requirement=FieldRequirement.REQUIRED,
                    ))

        if field_passed:
            required_score += points_per_required

    # --- Score recommended fields (30 points) ---
    recommended_total = 30.0
    total_weight = sum(spec.weight for spec in recommended_specs)
    recommended_score = 0.0

    for spec in recommended_specs:
        field_passed = True
        value = _get_field_value(client_profile, spec.name)

        # Special: launch_stats just needs to not be None
        if spec.name == 'launch_stats':
            if value is None:
                field_passed = False
                all_issues.append(ValidationIssue(
                    field=spec.name,
                    message='launch_stats is None',
                    requirement=FieldRequirement.RECOMMENDED,
                ))
        elif _is_field_empty(value):
            field_passed = False
            all_issues.append(ValidationIssue(
                field=spec.name,
                message=f'{spec.name} is missing or empty',
                requirement=FieldRequirement.RECOMMENDED,
            ))
        else:
            # Check length / item count
            length_issues = _check_field_length(value, spec)
            if length_issues:
                field_passed = False
                for msg in length_issues:
                    all_issues.append(ValidationIssue(
                        field=spec.name,
                        message=msg,
                        requirement=FieldRequirement.RECOMMENDED,
                    ))

            # Run validators
            for validator_name in spec.validators:
                validator_issues = _run_validator(
                    validator_name, value, client_profile,
                )
                if validator_issues:
                    field_passed = False
                    for msg in validator_issues:
                        all_issues.append(ValidationIssue(
                            field=spec.name,
                            message=msg,
                            requirement=FieldRequirement.RECOMMENDED,
                        ))

        if field_passed and total_weight > 0:
            recommended_score += (spec.weight / total_weight) * recommended_total

    # --- Outreach template validation (contributes issues, not separate score) ---
    contact_name = (client_profile.get('contact_name') or '').strip()
    company_name = (
        (client_profile.get('client') or {}).get('name', '')
        or client_profile.get('program_name', '')
    )
    outreach_issues = validate_outreach_templates(
        outreach_templates,
        contact_name=contact_name,
        company_name=company_name,
    )
    all_issues.extend(outreach_issues)

    # --- Partner card quality (10 points) ---
    partner_card_score = 0.0
    if partner_cards:
        card_quality, card_issues = validate_all_partner_cards(partner_cards)
        partner_card_score = card_quality
        all_issues.extend(card_issues)
    elif partner_count > 0:
        # No card data provided but partners exist -- give partial credit
        partner_card_score = 5.0

    # --- Final score ---
    total_score = required_score + recommended_score + partner_card_score
    total_score = max(0.0, min(100.0, total_score))

    # A profile passes ONLY when there are zero issues — required OR recommended.
    # Every section must be populated, every partner card compliant, every
    # outreach template passing.  This is a client-facing product; nothing
    # incomplete reaches production.
    passed = len(all_issues) == 0

    logger.debug(
        "Profile validation: score=%.1f (required=%.1f, recommended=%.1f, "
        "cards=%.1f), passed=%s, issues=%d",
        total_score, required_score, recommended_score,
        partner_card_score, passed, len(all_issues),
    )

    return ProfileValidationResult(
        passed=passed,
        score=round(total_score, 1),
        issues=all_issues,
        missing_fields=missing_fields,
    )


# =============================================================================
# Outreach Template Validation
# =============================================================================

def validate_outreach_templates(
    templates: dict,
    contact_name: str = '',
    company_name: str = '',
) -> list[ValidationIssue]:
    """Validate outreach email templates against the standard.

    Checks for:
    - Word count within range
    - No unfilled placeholder tokens
    - Not matching known generic boilerplate
    - References something specific (not just "your work" or "your audience")

    Args:
        templates: The MemberReport.outreach_templates JSON dict.
        contact_name: Client's name for context.
        company_name: Client's company for context.

    Returns:
        List of ValidationIssue objects.
    """
    issues: list[ValidationIssue] = []

    if not templates:
        issues.append(ValidationIssue(
            field='outreach_templates',
            message='Outreach templates dict is empty',
            requirement=FieldRequirement.REQUIRED,
        ))
        return issues

    for template_key, standard in OUTREACH_STANDARD.items():
        template = templates.get(template_key)
        if not template:
            issues.append(ValidationIssue(
                field=f'outreach_templates.{template_key}',
                message=f'Missing "{template_key}" template',
                requirement=FieldRequirement.REQUIRED,
            ))
            continue

        text = (template.get('text') or '').strip()
        if not text:
            issues.append(ValidationIssue(
                field=f'outreach_templates.{template_key}',
                message=f'"{template_key}" template text is empty',
                requirement=FieldRequirement.REQUIRED,
            ))
            continue

        # Word count check
        wc = _word_count(text)
        if wc < standard['min_words']:
            issues.append(ValidationIssue(
                field=f'outreach_templates.{template_key}',
                message=(
                    f'"{template_key}" template is too short '
                    f'({wc} words, need {standard["min_words"]}+)'
                ),
                requirement=FieldRequirement.REQUIRED,
            ))
        if wc > standard['max_words']:
            issues.append(ValidationIssue(
                field=f'outreach_templates.{template_key}',
                message=(
                    f'"{template_key}" template is too long '
                    f'({wc} words, max {standard["max_words"]})'
                ),
                requirement=FieldRequirement.RECOMMENDED,
            ))

        # Unfilled placeholders
        placeholder_issues = has_unfilled_placeholders(text)
        for msg in placeholder_issues:
            issues.append(ValidationIssue(
                field=f'outreach_templates.{template_key}',
                message=msg,
                requirement=FieldRequirement.REQUIRED,
            ))

        # Known generic boilerplate check
        patterns = (
            _KNOWN_GENERIC_INITIAL_PATTERNS
            if template_key == 'initial'
            else _KNOWN_GENERIC_FOLLOWUP_PATTERNS
        )
        for pattern in patterns:
            if pattern.search(text):
                issues.append(ValidationIssue(
                    field=f'outreach_templates.{template_key}',
                    message=(
                        f'"{template_key}" template matches known generic '
                        f'boilerplate pattern'
                    ),
                    requirement=FieldRequirement.REQUIRED,
                ))
                break

        # Vague reference check -- flag if ALL references are vague
        # (i.e. uses "your work" but never mentions anything specific)
        vague_matches = _VAGUE_REFERENCE_RE.findall(text)
        if vague_matches and company_name:
            # Check if the template references something specific
            has_specific = (
                company_name.lower() in text.lower()
                or contact_name.lower() in text.lower()
            )
            if not has_specific:
                # Check for other specific signals (numbers, program names, etc.)
                has_numbers = bool(re.search(r'\d+[KkMm+%]', text))
                if not has_numbers:
                    issues.append(ValidationIssue(
                        field=f'outreach_templates.{template_key}',
                        message=(
                            f'"{template_key}" template uses only vague '
                            f'references ({", ".join(vague_matches)}) without '
                            f'mentioning anything specific about the client'
                        ),
                        requirement=FieldRequirement.RECOMMENDED,
                    ))

    return issues


# =============================================================================
# Partner Card Validation
# =============================================================================

def validate_partner_card(card: dict) -> list[ValidationIssue]:
    """Validate a single partner card against the quality standard.

    Args:
        card: A dict with partner card fields (name, company, tagline,
            audience, why_fit, email, phone, linkedin, detail_note, etc.)

    Returns:
        List of ValidationIssue objects for this card.
    """
    issues: list[ValidationIssue] = []
    card_name = card.get('name', '<unnamed>')

    # name -- required, min 3 chars
    name = (card.get('name') or '').strip()
    if len(name) < 3:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].name',
            message=f'Partner name is too short or missing: "{name}"',
            requirement=FieldRequirement.REQUIRED,
        ))

    # tagline or company -- at least one must be non-empty
    tagline = (card.get('tagline') or '').strip()
    company = (card.get('company') or '').strip()
    if not tagline and not company:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].tagline_or_company',
            message='Neither tagline nor company is set',
            requirement=FieldRequirement.REQUIRED,
        ))

    # audience -- required, min 20 chars
    audience = (card.get('audience') or '').strip()
    if len(audience) < 20:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].audience',
            message=(
                f'Audience description is too short '
                f'({len(audience)} chars, need 20+)'
            ),
            requirement=FieldRequirement.REQUIRED,
        ))

    # why_fit -- required, min 30 chars, must contain next step pattern
    why_fit_raw = card.get('why_fit') or ''
    if isinstance(why_fit_raw, list):
        why_fit = ' '.join(str(item).strip() for item in why_fit_raw if item)
    else:
        why_fit = str(why_fit_raw).strip()
    if len(why_fit) < 30:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].why_fit',
            message=(
                f'why_fit is too short '
                f'({len(why_fit)} chars, need 30+)'
            ),
            requirement=FieldRequirement.REQUIRED,
        ))
    else:
        why_fit_issues = has_three_part_why_fit(why_fit)
        for msg in why_fit_issues:
            issues.append(ValidationIssue(
                field=f'partner_card[{card_name}].why_fit',
                message=msg,
                requirement=FieldRequirement.REQUIRED,
            ))

    # Contact info -- at least 1 of email, phone, linkedin
    email = (card.get('email') or '').strip()
    phone = (card.get('phone') or '').strip()
    linkedin = (card.get('linkedin') or '').strip()
    if not email and not phone and not linkedin:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].contact_info',
            message='No contact info (email, phone, or linkedin)',
            requirement=FieldRequirement.REQUIRED,
        ))

    # detail_note -- recommended
    detail_note = (card.get('detail_note') or '').strip()
    if not detail_note:
        issues.append(ValidationIssue(
            field=f'partner_card[{card_name}].detail_note',
            message='detail_note is empty (recommended for context)',
            requirement=FieldRequirement.RECOMMENDED,
        ))

    return issues


def validate_all_partner_cards(
    cards: list[dict],
) -> tuple[float, list[ValidationIssue]]:
    """Validate all partner cards. Returns (quality_score_0_to_10, issues).

    The quality score is 10 * (fraction of cards that pass all required checks).

    Args:
        cards: List of partner card dicts.

    Returns:
        Tuple of (quality_score, all_issues).
    """
    if not cards:
        return 0.0, []

    all_issues: list[ValidationIssue] = []
    passing_count = 0

    for card in cards:
        card_issues = validate_partner_card(card)
        all_issues.extend(card_issues)

        # A card passes if it has no REQUIRED-level issues
        has_required_issue = any(
            issue.requirement == FieldRequirement.REQUIRED
            for issue in card_issues
        )
        if not has_required_issue:
            passing_count += 1

    quality_pct = passing_count / len(cards)
    quality_score = round(quality_pct * 10.0, 1)

    return quality_score, all_issues
