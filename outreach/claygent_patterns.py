"""
AutoClaygent Prompt Patterns

Based on Jordan Crawford's AutoClaygent methodology for building AI agents in Clay.com.
These patterns follow the 5-part prompt anatomy:
1. Input - What data you're providing
2. Goal - What you want to achieve
3. Steps with Decision Trees - Logic flow
4. Fallback - What to do if uncertain
5. JSON Output - Clay-compatible schema

Reference: https://www.autoclaygent.com/
"""

# Quality threshold for Claygent outputs (from AutoClaygent methodology)
QUALITY_THRESHOLD = 8.0

# 7-Criterion Quality Scoring Weights
QUALITY_CRITERIA = {
    'accuracy': 0.25,           # Is the output factually correct?
    'completeness': 0.25,       # Are all required fields populated?
    'json_validity': 0.15,      # Is the JSON properly formatted?
    'source_quality': 0.15,     # Are sources reliable?
    'step_efficiency': 0.10,    # Did it follow the optimal path?
    'consistency': 0.05,        # Is the output consistent with patterns?
    'error_handling': 0.05,     # Did it handle edge cases well?
}


# =============================================================================
# AUTOCLAYGENT 9 PROMPT PATTERNS
# =============================================================================

CLAYGENT_PATTERNS = {

    # -------------------------------------------------------------------------
    # Pattern 1: Business Model Classification
    # -------------------------------------------------------------------------
    'business_model': {
        'name': 'Business Model Classification',
        'description': 'Classify business model as subscription, transactional, hybrid, etc.',
        'use_case': 'Understand revenue model for partnership alignment',
        'prompt_template': '''You are a business analyst. Analyze the company information provided.

INPUT:
- Company Name: {{company_name}}
- Website: {{website}}
- Description: {{description}}

GOAL:
Classify this company's primary business model.

STEPS:
1. Look for subscription/recurring revenue indicators:
   - Monthly/annual pricing
   - "Subscribe", "membership", "plan" language
   - SaaS indicators
2. If subscription indicators found → classify as "subscription"
3. Look for transactional indicators:
   - One-time purchases
   - E-commerce products
   - Pay-per-use language
4. If transactional indicators found → classify as "transactional"
5. If both present → classify as "hybrid"
6. If neither clear → classify as "unknown"

FALLBACK:
If unable to determine with confidence, return "unknown" with reasoning.

OUTPUT (JSON):
{
  "business_model": "subscription|transactional|hybrid|marketplace|agency|unknown",
  "confidence": "high|medium|low",
  "indicators": ["indicator1", "indicator2"],
  "reasoning": "Brief explanation"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'business_model': {
                    'type': 'string',
                    'enum': ['subscription', 'transactional', 'hybrid', 'marketplace', 'agency', 'unknown']
                },
                'confidence': {
                    'type': 'string',
                    'enum': ['high', 'medium', 'low']
                },
                'indicators': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'reasoning': {'type': 'string'}
            },
            'required': ['business_model', 'confidence'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 2: Platform Detection via URL
    # -------------------------------------------------------------------------
    'platform_detection': {
        'name': 'Platform Detection via URL',
        'description': 'Detect SaaS platforms/tools via portal URL patterns (95% accuracy)',
        'use_case': 'Identify tech stack for compatibility assessment',
        'prompt_template': '''You are a technical analyst specializing in SaaS platforms.

INPUT:
- Domain: {{domain}}
- Known URLs: {{urls}}

GOAL:
Identify what platforms/tools this company uses by analyzing URL patterns.

STEPS:
1. Check for common platform login/portal patterns:
   - app.platform.com → Platform name
   - company.platform.com → Platform name
   - platform.com/company → Platform name
2. Identify each platform detected
3. Categorize by function (CRM, Marketing, Support, etc.)
4. Note confidence based on URL clarity

COMMON PATTERNS TO DETECT:
- Kajabi: *.mykajabi.com, app.kajabi.com
- Teachable: *.teachable.com
- Thinkific: *.thinkific.com
- ClickFunnels: *.clickfunnels.com
- Kartra: *.kartra.com
- HubSpot: app.hubspot.com
- Salesforce: *.salesforce.com
- Intercom: *.intercom.io
- Zendesk: *.zendesk.com

FALLBACK:
If no platforms detected, return empty array.

OUTPUT (JSON):
{
  "platforms_detected": [
    {
      "name": "Platform Name",
      "category": "CRM|Marketing|LMS|Support|Analytics|Other",
      "url_pattern": "detected URL",
      "confidence": "high|medium|low"
    }
  ],
  "tech_stack_summary": "Brief summary of detected stack"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'platforms_detected': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': {'type': 'string'},
                            'category': {'type': 'string'},
                            'url_pattern': {'type': 'string'},
                            'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']}
                        },
                        'required': ['name', 'category'],
                        'additionalProperties': False
                    }
                },
                'tech_stack_summary': {'type': 'string'}
            },
            'required': ['platforms_detected'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 3: CRM Validation & Mismatch Detection
    # -------------------------------------------------------------------------
    'crm_validation': {
        'name': 'CRM Validation & Mismatch',
        'description': 'Validate and detect mismatches in CRM data',
        'use_case': 'Data quality checks before outreach',
        'prompt_template': '''You are a data quality analyst.

INPUT:
- Name: {{name}}
- Email: {{email}}
- Company (claimed): {{company}}
- LinkedIn: {{linkedin_url}}
- Website: {{website}}

GOAL:
Validate the data consistency and flag any mismatches.

STEPS:
1. Check email domain against claimed company:
   - If email domain matches company website → valid
   - If email is personal (gmail, yahoo, etc.) → flag "personal_email"
   - If email domain doesn't match → flag "domain_mismatch"
2. Validate name format:
   - If name appears to be a company name → flag "name_is_company"
   - If name has unusual characters → flag "name_quality"
3. Cross-reference LinkedIn (if available):
   - Check if company on LinkedIn matches claimed company
   - Check if title seems reasonable

FALLBACK:
If unable to validate, note which fields couldn't be verified.

OUTPUT (JSON):
{
  "is_valid": true|false,
  "validation_score": 0-100,
  "issues": [
    {
      "field": "email|name|company|linkedin",
      "issue_type": "mismatch|missing|suspicious|invalid",
      "details": "Description of issue"
    }
  ],
  "corrected_data": {
    "company": "Corrected company if found",
    "email_type": "corporate|personal|unknown"
  }
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'is_valid': {'type': 'boolean'},
                'validation_score': {'type': 'integer'},
                'issues': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'field': {'type': 'string'},
                            'issue_type': {'type': 'string'},
                            'details': {'type': 'string'}
                        },
                        'required': ['field', 'issue_type'],
                        'additionalProperties': False
                    }
                },
                'corrected_data': {
                    'type': 'object',
                    'additionalProperties': True
                }
            },
            'required': ['is_valid', 'validation_score'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 4: Digital Maturity Profiling
    # -------------------------------------------------------------------------
    'digital_maturity': {
        'name': 'Digital Maturity Profiling',
        'description': 'Score online sophistication and digital presence',
        'use_case': 'Assess partnership readiness and tech-savviness',
        'prompt_template': '''You are a digital marketing analyst.

INPUT:
- Website: {{website}}
- Social Profiles: {{social_profiles}}
- Content Found: {{content_summary}}

GOAL:
Assess the digital maturity and online sophistication of this business.

STEPS:
1. Website Analysis (0-25 points):
   - Professional design → +10
   - Mobile responsive → +5
   - Fast loading → +5
   - SSL certificate → +5
2. Content Marketing (0-25 points):
   - Active blog → +10
   - Lead magnets → +10
   - Video content → +5
3. Social Presence (0-25 points):
   - Active on 2+ platforms → +10
   - Consistent posting → +10
   - Engagement rate → +5
4. Marketing Stack (0-25 points):
   - Email marketing tool → +10
   - Analytics tracking → +10
   - Automation detected → +5

FALLBACK:
If unable to assess a category, give neutral score (12.5) for that category.

OUTPUT (JSON):
{
  "digital_maturity_score": 0-100,
  "maturity_level": "nascent|developing|established|advanced|sophisticated",
  "breakdown": {
    "website": 0-25,
    "content": 0-25,
    "social": 0-25,
    "marketing_stack": 0-25
  },
  "strengths": ["strength1", "strength2"],
  "gaps": ["gap1", "gap2"],
  "recommendations": ["rec1", "rec2"]
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'digital_maturity_score': {'type': 'integer'},
                'maturity_level': {
                    'type': 'string',
                    'enum': ['nascent', 'developing', 'established', 'advanced', 'sophisticated']
                },
                'breakdown': {
                    'type': 'object',
                    'properties': {
                        'website': {'type': 'integer'},
                        'content': {'type': 'integer'},
                        'social': {'type': 'integer'},
                        'marketing_stack': {'type': 'integer'}
                    },
                    'additionalProperties': False
                },
                'strengths': {'type': 'array', 'items': {'type': 'string'}},
                'gaps': {'type': 'array', 'items': {'type': 'string'}},
                'recommendations': {'type': 'array', 'items': {'type': 'string'}}
            },
            'required': ['digital_maturity_score', 'maturity_level'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 5: B2B vs B2C Detection
    # -------------------------------------------------------------------------
    'b2b_b2c_detection': {
        'name': 'B2B vs B2C Detection',
        'description': 'Classify target market as B2B, B2C, or both',
        'use_case': 'Partnership alignment and audience matching',
        'prompt_template': '''You are a market analyst.

INPUT:
- Company: {{company_name}}
- Website: {{website}}
- Description: {{description}}
- Products/Services: {{products}}

GOAL:
Determine if this company primarily serves businesses (B2B), consumers (B2C), or both.

STEPS:
1. Analyze language on website:
   - "Enterprise", "teams", "business" → B2B indicator
   - "Personal", "individual", "home" → B2C indicator
2. Check pricing structure:
   - Per-seat, volume pricing → B2B
   - Individual pricing, consumer-friendly → B2C
3. Examine product complexity:
   - Requires implementation/onboarding → B2B
   - Self-service, immediate use → B2C
4. Look at customer testimonials:
   - Company logos, case studies → B2B
   - Individual reviews, personal stories → B2C

FALLBACK:
If unclear, classify as "B2B2C" (serves both) with explanation.

OUTPUT (JSON):
{
  "market_type": "B2B|B2C|B2B2C",
  "confidence": "high|medium|low",
  "b2b_indicators": ["indicator1", "indicator2"],
  "b2c_indicators": ["indicator1", "indicator2"],
  "primary_audience": "Description of main target customer",
  "reasoning": "Brief explanation"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'market_type': {'type': 'string', 'enum': ['B2B', 'B2C', 'B2B2C']},
                'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
                'b2b_indicators': {'type': 'array', 'items': {'type': 'string'}},
                'b2c_indicators': {'type': 'array', 'items': {'type': 'string'}},
                'primary_audience': {'type': 'string'},
                'reasoning': {'type': 'string'}
            },
            'required': ['market_type', 'confidence'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 6: Corporate Structure Detection
    # -------------------------------------------------------------------------
    'corporate_structure': {
        'name': 'Corporate Structure Detection',
        'description': 'Identify parent companies, subsidiaries, and corporate relationships',
        'use_case': 'Understand decision-making hierarchy for outreach',
        'prompt_template': '''You are a business intelligence analyst.

INPUT:
- Company: {{company_name}}
- Domain: {{domain}}
- LinkedIn: {{linkedin_url}}

GOAL:
Determine the corporate structure - is this a parent company, subsidiary, or independent?

STEPS:
1. Check for parent company indicators:
   - "A [Company] Company" in branding
   - Different company name on LinkedIn vs website
   - Corporate disclaimers/footer references
2. Check for subsidiary indicators:
   - Part of a larger organization
   - Shared infrastructure/login
   - Cross-references to parent brand
3. Determine independence:
   - Unique branding throughout
   - Independent funding/ownership mentioned
   - No corporate parent references

FALLBACK:
If unable to determine, classify as "independent" with low confidence.

OUTPUT (JSON):
{
  "structure_type": "parent|subsidiary|independent|franchise",
  "parent_company": "Name if subsidiary, null otherwise",
  "confidence": "high|medium|low",
  "evidence": ["evidence1", "evidence2"],
  "decision_maker_level": "local|regional|corporate"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'structure_type': {
                    'type': 'string',
                    'enum': ['parent', 'subsidiary', 'independent', 'franchise']
                },
                'parent_company': {'type': ['string', 'null']},
                'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
                'evidence': {'type': 'array', 'items': {'type': 'string'}},
                'decision_maker_level': {
                    'type': 'string',
                    'enum': ['local', 'regional', 'corporate']
                }
            },
            'required': ['structure_type', 'confidence'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 7: Buying Intent Detection
    # -------------------------------------------------------------------------
    'buying_intent': {
        'name': 'Buying Intent Detection',
        'description': 'Identify signals that indicate readiness to buy/partner',
        'use_case': 'Prioritize outreach to high-intent prospects',
        'prompt_template': '''You are a sales intelligence analyst.

INPUT:
- Company: {{company_name}}
- Recent Activity: {{recent_activity}}
- Job Postings: {{job_postings}}
- News/PR: {{news}}
- Tech Changes: {{tech_changes}}

GOAL:
Assess buying intent signals for partnership/purchase readiness.

STEPS:
1. Check for expansion signals (high intent):
   - Recent funding announcement → +30
   - Hiring for relevant roles → +25
   - New market entry → +20
2. Check for problem signals (medium-high intent):
   - Job posting mentioning pain points → +20
   - Negative reviews of current solution → +15
   - Compliance requirements → +15
3. Check for research signals (medium intent):
   - Visiting competitor websites → +15
   - Downloading whitepapers → +10
   - Conference attendance → +10
4. Check for timing signals:
   - Contract renewal period → +20
   - Budget cycle timing → +15
   - Leadership change → +10

FALLBACK:
If no signals detected, return score of 20 (baseline curiosity assumed).

OUTPUT (JSON):
{
  "intent_score": 0-100,
  "intent_level": "hot|warm|neutral|cold",
  "signals_detected": [
    {
      "signal_type": "expansion|problem|research|timing",
      "signal": "Description",
      "weight": 0-30
    }
  ],
  "recommended_action": "immediate_outreach|nurture|monitor|deprioritize",
  "best_approach": "Suggested outreach angle"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'intent_score': {'type': 'integer'},
                'intent_level': {
                    'type': 'string',
                    'enum': ['hot', 'warm', 'neutral', 'cold']
                },
                'signals_detected': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'signal_type': {'type': 'string'},
                            'signal': {'type': 'string'},
                            'weight': {'type': 'integer'}
                        },
                        'required': ['signal_type', 'signal'],
                        'additionalProperties': False
                    }
                },
                'recommended_action': {
                    'type': 'string',
                    'enum': ['immediate_outreach', 'nurture', 'monitor', 'deprioritize']
                },
                'best_approach': {'type': 'string'}
            },
            'required': ['intent_score', 'intent_level'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 8: Multi-Axis Classification
    # -------------------------------------------------------------------------
    'multi_axis': {
        'name': 'Multi-Axis Classification',
        'description': '5-dimension classification for comprehensive profiling',
        'use_case': 'Complete prospect profiling for matching algorithm',
        'prompt_template': '''You are a business classification specialist.

INPUT:
- Company: {{company_name}}
- Website: {{website}}
- Description: {{description}}
- Industry: {{industry}}

GOAL:
Classify this company across 5 key dimensions for partnership matching.

CLASSIFY ACROSS THESE 5 AXES:

1. INDUSTRY VERTICAL:
   - coaching|consulting|saas|ecommerce|agency|education|health|finance|other

2. BUSINESS STAGE:
   - startup|growth|established|enterprise

3. AUDIENCE SIZE:
   - tiny(<1K)|small(1K-10K)|medium(10K-100K)|large(100K-1M)|massive(1M+)

4. CONTENT STYLE:
   - educational|entertaining|inspirational|technical|mixed

5. COLLABORATION READINESS:
   - active_seeking|open|selective|not_interested

FALLBACK:
For any axis where classification is unclear, use "unknown" with reasoning.

OUTPUT (JSON):
{
  "classifications": {
    "industry_vertical": "value",
    "business_stage": "value",
    "audience_size": "value",
    "content_style": "value",
    "collaboration_readiness": "value"
  },
  "confidence_by_axis": {
    "industry_vertical": "high|medium|low",
    "business_stage": "high|medium|low",
    "audience_size": "high|medium|low",
    "content_style": "high|medium|low",
    "collaboration_readiness": "high|medium|low"
  },
  "overall_confidence": "high|medium|low",
  "notes": "Any relevant observations"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'classifications': {
                    'type': 'object',
                    'properties': {
                        'industry_vertical': {'type': 'string'},
                        'business_stage': {'type': 'string'},
                        'audience_size': {'type': 'string'},
                        'content_style': {'type': 'string'},
                        'collaboration_readiness': {'type': 'string'}
                    },
                    'required': ['industry_vertical', 'business_stage', 'audience_size',
                                'content_style', 'collaboration_readiness'],
                    'additionalProperties': False
                },
                'confidence_by_axis': {
                    'type': 'object',
                    'additionalProperties': {'type': 'string'}
                },
                'overall_confidence': {
                    'type': 'string',
                    'enum': ['high', 'medium', 'low']
                },
                'notes': {'type': 'string'}
            },
            'required': ['classifications', 'overall_confidence'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },

    # -------------------------------------------------------------------------
    # Pattern 9: SMB Owner Discovery (5-Stage Waterfall)
    # -------------------------------------------------------------------------
    'smb_owner_discovery': {
        'name': 'SMB Owner Discovery',
        'description': '5-stage waterfall to find SMB owner/decision-maker',
        'use_case': 'Identify the right person to contact for partnerships',
        'prompt_template': '''You are a business development researcher.

INPUT:
- Company: {{company_name}}
- Website: {{website}}
- LinkedIn Company Page: {{linkedin_company}}

GOAL:
Find the owner or primary decision-maker for this small/medium business.

5-STAGE WATERFALL:

STAGE 1 - Website About/Team Page:
- Check /about, /team, /our-team pages
- Look for "Founder", "CEO", "Owner" titles
- If found → return with confidence "high"

STAGE 2 - LinkedIn Company Page:
- Check company's LinkedIn page
- Look at "People" section for leadership
- Filter by "Founder", "Owner", "CEO", "President"
- If found → return with confidence "high"

STAGE 3 - Domain WHOIS (if public):
- Check domain registration
- Look for registrant name
- If found → return with confidence "medium"

STAGE 4 - Social Media Cross-Reference:
- Check Twitter/X bio linking to company
- Check Instagram business account
- If found → return with confidence "medium"

STAGE 5 - General Search:
- Search "[company name] founder" or "[company name] owner"
- Look for press releases, interviews
- If found → return with confidence "low"

FALLBACK:
If no owner found after all stages, return null with stages_attempted.

OUTPUT (JSON):
{
  "owner_found": true|false,
  "owner_name": "Full Name or null",
  "owner_title": "Title or null",
  "owner_linkedin": "URL or null",
  "owner_email": "Email or null (if found publicly)",
  "discovery_stage": 1-5,
  "confidence": "high|medium|low",
  "stages_attempted": ["stage1", "stage2", ...],
  "verification_notes": "How the owner was verified"
}''',
        'json_schema': {
            'type': 'object',
            'properties': {
                'owner_found': {'type': 'boolean'},
                'owner_name': {'type': ['string', 'null']},
                'owner_title': {'type': ['string', 'null']},
                'owner_linkedin': {'type': ['string', 'null']},
                'owner_email': {'type': ['string', 'null']},
                'discovery_stage': {'type': ['integer', 'null']},
                'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
                'stages_attempted': {'type': 'array', 'items': {'type': 'string'}},
                'verification_notes': {'type': 'string'}
            },
            'required': ['owner_found', 'confidence', 'stages_attempted'],
            'additionalProperties': False
        },
        'quality_threshold': 8.0,
    },
}


# =============================================================================
# CLAY JSON SCHEMA RULES (12 Rules for Clay Compatibility)
# =============================================================================

CLAY_SCHEMA_RULES = """
12 Rules for Clay-Compatible JSON Schemas:

1. NO minLength constraints - Clay doesn't enforce them
2. NO maxLength constraints - Clay doesn't enforce them
3. NO format constraints (email, uri, etc.) - Use string type only
4. NO pattern constraints - Clay doesn't validate regex
5. ALWAYS use additionalProperties: false - Prevents unexpected fields
6. ALWAYS include required array - Be explicit about requirements
7. Use enum for constrained values - Clay handles enums well
8. Arrays should have items defined - Always specify item schema
9. Use type: ['string', 'null'] for nullable fields - Not required: false
10. Keep nesting shallow - Max 2 levels deep preferred
11. Use descriptive property names - Clay displays these in UI
12. Test with edge cases - Empty strings, nulls, special characters
"""


def get_pattern(pattern_name: str) -> dict:
    """
    Get a specific Claygent pattern by name.

    Args:
        pattern_name: Key from CLAYGENT_PATTERNS

    Returns:
        Pattern dictionary or None if not found
    """
    return CLAYGENT_PATTERNS.get(pattern_name)


def get_all_patterns() -> dict:
    """Get all available Claygent patterns."""
    return CLAYGENT_PATTERNS


def get_pattern_names() -> list:
    """Get list of all pattern names."""
    return list(CLAYGENT_PATTERNS.keys())


def validate_against_schema(data: dict, pattern_name: str) -> tuple:
    """
    Validate data against a pattern's JSON schema.

    Args:
        data: Data to validate
        pattern_name: Pattern to validate against

    Returns:
        Tuple of (is_valid: bool, errors: list)
    """
    pattern = get_pattern(pattern_name)
    if not pattern:
        return False, [f"Pattern '{pattern_name}' not found"]

    schema = pattern.get('json_schema', {})
    errors = []

    # Check required fields
    required = schema.get('required', [])
    for field in required:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    # Check enum constraints
    properties = schema.get('properties', {})
    for field, field_schema in properties.items():
        if field in data and 'enum' in field_schema:
            if data[field] not in field_schema['enum']:
                errors.append(
                    f"Invalid value for {field}: {data[field]}. "
                    f"Must be one of: {field_schema['enum']}"
                )

    return len(errors) == 0, errors
