# JV Matchmaker Platform - Full Build Plan

## Vision Summary

Build a comprehensive JV partnership platform that combines:
- **Core Engine**: AI-powered matching + 54-step execution playbook
- **GTM Intelligence**: Blueprint Framework (PQS/PVP, data moats, data recipes)
- **Launch System**: MP3 Framework with 54-play content playbook generator
- **Tech Stack**: Django + HTMX + Alpine.js + Tailwind CSS (HAT stack)

---

## What We're Building

### The Platform Has 4 Major Systems

```
┌─────────────────────────────────────────────────────────────────────┐
│                    JV MATCHMAKER PLATFORM                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. MATCH ENGINE          2. ENRICHMENT PIPELINE                   │
│  ┌─────────────────┐      ┌─────────────────┐                      │
│  │ V1 Scoring      │ ←──→ │ Data Recipes    │                      │
│  │ - Intent (45%)  │      │ - 7-criterion   │                      │
│  │ - Synergy (25%) │      │ - Clay Bridge   │                      │
│  │ - Momentum (20%)│      │ - Multi-source  │                      │
│  │ - Context (10%) │      │ - PQS Signals   │                      │
│  │ Harmonic Mean   │      └─────────────────┘                      │
│  └─────────────────┘                                               │
│                                                                     │
│  3. EXECUTION PLAYBOOK    4. LAUNCH CONTENT ENGINE                 │
│  ┌─────────────────┐      ┌─────────────────┐                      │
│  │ 54-Step System  │      │ MP3 Framework   │                      │
│  │ MATCH → PITCH → │      │ - Problem       │                      │
│  │ CLOSE → EXECUTE │      │ - Process       │                      │
│  │ → CONVERT →     │      │ - Proof         │                      │
│  │ RECIPROCATE     │      │ 54 Play Library │                      │
│  └─────────────────┘      └─────────────────┘                      │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  REVENUE TIERS                                                      │
│  Free → Playbook ($297) → Starter ($97/mo) → Pro ($297/mo) →       │
│  Enterprise ($597/mo) → DFY Services ($1,500-$4,000/mo)            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Foundation (Django + HAT Stack)

### 1.1 Project Setup
**Files to create:**
```
jv-matchmaker/
├── manage.py
├── jvmatchmaker/
│   ├── settings/
│   │   ├── base.py
│   │   ├── development.py
│   │   └── production.py
│   ├── urls.py
│   └── wsgi.py
├── apps/
│   └── accounts/
├── templates/
│   └── base.html          # HTMX + Alpine.js + Tailwind base
├── static/
│   └── css/tailwind.css
├── requirements/
│   ├── base.txt
│   └── development.txt
└── docker/
    └── docker-compose.yml
```

### 1.2 Database Decision
**Recommendation:** Keep Supabase initially
- Existing data (1000+ profiles) stays intact
- Use `supabase-py` client in Django
- Migrate to Django ORM later if needed

### 1.3 Authentication
**Port from:** `jv-matcher-temp/auth_service.py`
- Supabase auth integration
- Roles: admin, member, viewer
- Subscription tier tracking

---

## Phase 2: Directory Module

### 2.1 Profile Management
**Port from:** `jv-matcher-temp/directory_service.py` (2,217 lines)

**Key functionality:**
- Profile CRUD operations
- Search/filter with HTMX live updates
- CSV import/export
- Profile cards with membership badges

### 2.2 Data Model
```python
class Profile(models.Model):
    # Core fields (map to existing Supabase)
    name = models.CharField(max_length=255)
    company = models.CharField(max_length=255)
    email = models.EmailField(null=True)  # 0.1% coverage - needs enrichment

    # Business data
    business_focus = models.TextField()
    niche = models.CharField(max_length=100)  # 16 niches
    offering = models.TextField()  # 61.5% missing - needs enrichment
    seeking = models.TextField()

    # Metrics for matching
    list_size = models.IntegerField(default=0)
    social_reach = models.IntegerField(default=0)

    # Enrichment tracking
    enrichment_score = models.FloatField(null=True)
    last_enriched_at = models.DateTimeField(null=True)
```

---

## Phase 3: Matching Engine

### 3.1 V1 Scoring Algorithm
**Port from:** `jv-matcher-temp/match_generator.py` (lines 1824-2100)

```python
# Scoring weights
INTENT_WEIGHT = 0.45      # Does A need what B offers?
SYNERGY_WEIGHT = 0.25     # Niche compatibility
MOMENTUM_WEIGHT = 0.20    # Time-decay activity
CONTEXT_WEIGHT = 0.10     # Industry alignment

# Final score = HarmonicMean(Score_AB, Score_BA)
# Harmonic Mean penalizes lopsided matches (100/0 → 0, not 50)
```

### 3.2 Rich Analysis Service
**Port from:** `jv-matcher-temp/rich_match_service.py` (464 lines)

Generates via GPT-4o-mini:
- `fit`: Why this is a strategic match
- `opportunity`: Specific collaboration idea
- `benefits`: Value exchange
- `outreach_message`: Ready-to-send (50-75 words)
- `confidence_score`: 75-95

### 3.3 Confidence Tiers
```python
TIERS = {
    'gold': {'max_rank': 3, 'label': 'Top Pick', 'emoji': '🔥'},
    'silver': {'max_rank': 8, 'label': 'Strong Match', 'emoji': '✅'},
    'bronze': {'max_rank': 999, 'label': 'Discovery', 'emoji': '👀'}
}
```

---

## Phase 4: Enrichment Pipeline

### 4.1 Integration with Match Engine
**Port from:** `enrichment-engine/` directory

```
New Profile Signup
       │
       ▼
Enrichment Pipeline ──► Auto-populate fields
       │                 - business_focus
       │                 - offering (61.5% missing!)
       │                 - email (0.1% coverage!)
       │                 - social_reach
       ▼
7-Criterion Scoring ──► Quality gate (must be 8.0+)
       │
       ▼
JV Matcher ──► Higher confidence matches
```

### 4.2 Data Recipes (Blueprint Framework)
Build "PQS Discovery" capabilities:
- Monitor multiple data sources for trigger signals
- Combine 2-5 data points for competitive moat
- Identify "pain-qualified segments" for targeting

### 4.3 Clay Bridge
**Port from:** `enrichment-engine/clay_bridge/`
- Django webhook view (replace Flask)
- Prompt templates for scraping
- Response analyzer with 7-criterion scoring

---

## Phase 5: 54-Step Execution Playbook

### 5.1 Playbook Structure
```python
STAGES = [
    'MATCH',       # Find partners
    'PITCH',       # Reach out
    'CLOSE',       # Secure agreement
    'EXECUTE',     # Run the promotion
    'CONVERT',     # Track results
    'RECIPROCATE'  # Return the favor
]

class PlaybookStep(models.Model):
    step_number = models.IntegerField()  # 1-54
    stage = models.CharField(choices=STAGES)
    title = models.CharField(max_length=255)
    description = models.TextField()
    required_for_gate = models.BooleanField()
```

### 5.2 Deal Progress Tracking
```python
class DealProgress(models.Model):
    user = models.ForeignKey(User)
    match = models.ForeignKey(MatchSuggestion)
    current_step = models.IntegerField()
    current_stage = models.CharField()
    estimated_revenue = models.DecimalField()
    actual_revenue = models.DecimalField()
```

**Note:** Playbook is "partially documented" - needs completion before implementation.

---

## Phase 6: Launch Content Engine (MP3 Framework)

### 6.1 The Framework
```
PRE-LAUNCH          LAUNCH              POST-LAUNCH
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Problem     │     │ Announcement│     │ ✅ Buyers   │
│ Process     │ ──► │ Nurture     │ ──► │ 🚫 Non-Buy  │
│ Proof       │     │ Urgency     │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
    Build              Convert            Nurture
    Anticipation       Interest           Relationships
```

### 6.2 Play Library (54 Plays, 3 Sizes)

**Size Options:**
| Size | Total Plays | Duration | Use Case |
|------|-------------|----------|----------|
| Small | 12 plays | 2-3 weeks | Quick launches, limited bandwidth |
| Medium | 29 plays | 4-6 weeks | Standard launches |
| Large/XL | 54 plays | 60-90 days | Major launches, high-ticket offers, mega-launches |

**Distribution by Phase:**
| Phase | Small | Medium | Large | Duration (XL) |
|-------|-------|--------|-------|---------------|
| Pre-Launch | 4 | 12 | 23 | 40-75 days |
| Launch: Announcement | 2 | 2 | 2 | Days 1-2 |
| Launch: Nurture | 4 | 10 | 15 | 4-10 days |
| Launch: Urgency/Cart Close | 0 | 2 | 7 | Final 2-3 days |
| Post-Launch: Buyers | 1 | 2 | 5 | 7 days |
| Post-Launch: Non-Buyers | 1 | 1 | 2 | 7 days |

### 6.2.1 Extra Large Blueprint (54 Touchpoints) - Implementation Guide

**Target:** Infoproducts, coaching services, courses, digital offerings over 2-3 months.

**Customization Principles:**
1. Take inspiration from the Transformation Report (Product Clarity Report)
2. Bring in customer stories whenever possible
3. Tell your own story if there's no customer story available
4. Explore problems-behind-the-problem AND customer limiting beliefs
5. **The AHA Moment Formula:** Give clarity on WHAT to do and WHY → Offer is the HOW shortcut

**Email Frequency Guidelines:**
```
PRE-LAUNCH (40-75 days)
├─ Mega-launch: 3+ emails/week
├─ Standard: 2 emails/week
└─ Goal: Become constant presence in audience's life

LAUNCH (4-10 days)
├─ Most successful launches: 2-5 emails during buying window
├─ 4-day window: ~6 total emails (not 6/day!)
├─ 10-day window: Can spread across more touchpoints
└─ Yeah, it's a lot. Do less if needed.

POST-LAUNCH (7 days)
├─ Buyers: 3-5 touchpoints (onboarding + engagement)
└─ Non-buyers: 2-3 touchpoints (gratitude + future nurture)
```

**Pre-Launch Phase Mapping (23 touchpoints):**
```
WEEK 1-2: Problem Awareness
├─ 1. Something's Coming (mystery/curiosity)
├─ 2. The Problem Spotlight (make them feel seen)
├─ 3. The Sneak Peek (effort behind solution)
└─ 4. Belief Breakthrough (why they struggle)

WEEK 3-4: Misconceptions & Root Causes
├─ 5. The Big Myth (external false belief)
├─ 6. The Untold Story (hidden truth)
├─ 7. The Domino Effect (ripple benefits)
├─ 8. The Big Mistake (common error)
└─ 9. The Real Reason (root cause)

WEEK 5-6: Social Proof & Credibility
├─ 10. Behind the Scenes (quality/effort)
├─ 11. Success Story (transformation)
├─ 12. The Invisible Bridge (problem → outcome)
├─ 13. The Story Behind (origin)
└─ 14. Quick Win Quick Start (momentum)

WEEK 7-8: Personal Connection & Differentiation
├─ 15. Why I'm Solving This (personal)
├─ 16. Overwhelmed? Not Alone (empathy)
├─ 17. Why a Plan Is Key (structure)
└─ 18. Secret Sauce Reveal (differentiation)

WEEK 9-10: Engagement & Early Access
├─ 19. What Are You Hoping? (engagement)
├─ 20. Early Access/Pre-Sale (exclusivity)
├─ 21. Live Event Announcement (value event)
├─ 22. My Solution Story (emotional journey)
└─ 23. Development Updates (anticipation)
```

**Launch Phase Mapping (23 touchpoints):**
```
DAYS 1-2: Announcement
├─ 24. Launch Announcement (doors open)
└─ 25. Introducing [Product] + Urgency Bonus

DAYS 2-5: Value & Social Proof
├─ 26. Here's What You Get / Transform
├─ 27. Early Responses (initial proof)
├─ 28. Time Running Out (early bird)
├─ 29. Milestone Celebrations
├─ 30. Join the Party (happy customers)
├─ 31. Your First Win
├─ 32. The Bonus Reveal
├─ 33. The Audit (cost of waiting)
├─ 34. 3 Biggest Objections
├─ 35. FAQs
└─ 36. What's Waiting (other side of problem)

DAYS 6-10: Urgency & Cart Close
├─ 37. Testimonial Triumph (early feedback)
├─ 38. Overcoming Objections (specific)
├─ 39. FAQs + Guarantees (detailed)
├─ 40. Additional Bonuses
├─ 41. Social Proof Build-Up
├─ 42. Referral Incentive
├─ 43. Last Chance (before cart closes)
├─ 44. Only 24 Hours Left + Guarantee
├─ 45. Last Chance to Fix [Problem]
└─ 46. Last Call / Final Reminder
```

**Post-Launch Mapping (8 touchpoints):**
```
BUYERS (Days 1-7 post-purchase):
├─ 47. Welcome: How to Get Started
├─ 48. How's It Going? Need Anything?
├─ 49. Get the Most Value (action urge)
├─ 50. Share Your Wins
└─ 51. Upsell/Cross-Sell

NON-BUYERS (Days 1-7 post-launch):
├─ 52. Feedback Request
├─ 53. If You Missed It, That's Okay
└─ 54. So Much Gratitude
```

### 6.3 The Complete 54 Play Library

Each play includes: Content Concept, AHA Moment, Vibe Opportunity, Soft CTA, Hook Inspiration

**PRE-LAUNCH PHASE (23 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 1 | Something's Coming | Build mystery/curiosity | Curiosity Gap + FOMO |
| 2 | The Problem Spotlight | Make audience feel seen | Empathy + Pain Amplification |
| 3 | The Sneak Peek | Show effort behind solution | Effort Justification + Open Loop |
| 4 | Belief Breakthrough | Address limiting beliefs | Cognitive Dissonance Relief |
| 5 | The Big Myth | Expose external false belief | Breaking Misconceptions |
| 6 | The Untold Story | Reveal hidden truth | Transparency + Emotional Engagement |
| 7 | The Domino Effect | Show ripple benefits | Future Pacing + Perceived Value |
| 8 | The Big Mistake | Identify common error | Common Enemy + Problem Recognition |
| 9 | The Real Reason | Reveal root cause | Clarity Brings Relief |
| 10 | Behind the Scenes | Show quality/effort | Transparency Builds Trust |
| 11 | Success Story | Share transformation | Social Proof + Relatability |
| 12 | The Invisible Bridge | Connect problem to solution | Gap Awareness + Logical Flow |
| 13 | The Story Behind | Share origin/inspiration | Emotional Connection |
| 14 | Quick Win Quick Start | Offer immediate action | Progress Builds Momentum |
| 15 | Why I'm Solving This | Personal connection | Authenticity + Shared Values |
| 16 | Overwhelmed? Not Alone | Acknowledge struggle | Normalization + Empathy |
| 17 | Why a Plan Is Key | Emphasize structure | Clarity Builds Confidence |
| 18 | Secret Sauce Reveal | Show differentiator | Unique Value Proposition |
| 19 | What Are You Hoping? | Engage audience input | Interactive Engagement |
| 20 | Early Access/Pre-Sale | Offer exclusivity | Exclusivity + Ownership |
| 21 | Live Event Announcement | Announce value event | Live Connection + Value First |
| 22 | My Solution Story | Share emotional journey | Emotional Engagement |
| 23 | Development Updates | Regular progress sharing | Transparency + Anticipation |

**LAUNCH PHASE - ANNOUNCEMENT (2 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 24 | Launch Announcement | Doors are open | Urgency + Clear CTA |
| 25 | Introducing [Product] | Full reveal + bonus | Problem-Solution Fit + Bonus Urgency |

**LAUNCH PHASE - MID-LAUNCH NURTURE (11 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 26 | Here's What You Get | Detail transformation | Clear Expectations + Future Pacing |
| 27 | Early Responses | Share initial proof | Social Proof + Relatability |
| 28 | Time Running Out | Early bird deadline | Urgency + Scarcity |
| 29 | Milestone Celebrations | Celebrate progress | Social Proof + Community |
| 30 | Join the Party | Showcase happy customers | Social Proof + FOMO |
| 31 | Your First Win | Describe initial success | Immediate Gratification |
| 32 | The Bonus Reveal | Introduce new bonus | Perceived Value + Reciprocity |
| 33 | The Audit | Cost of waiting | Loss Aversion + Scarcity of Time |
| 34 | 3 Biggest Objections | Address concerns | Cognitive Dissonance Resolution |
| 35 | FAQs | Answer key questions | Information Gap + Reduced Friction |
| 36 | What's Waiting | Paint success + guarantee | Future Pacing + Risk Reversal |

**LAUNCH PHASE - URGENCY/CART CLOSE (10 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 37 | Testimonial Triumph | Share buyer feedback | Social Proof |
| 38 | Overcoming Objections | Deep objection handling | Transparency + Trust |
| 39 | FAQs + Guarantees | Detailed Q&A + safety | Risk Reduction |
| 40 | Additional Bonuses | Sweeten the deal | Value Stacking + Loss Aversion |
| 41 | Social Proof Build-Up | Continue sharing wins | Trust Building |
| 42 | Referral Incentive | Reward sharing | Leveraging Relationships |
| 43 | Last Chance | Final opportunity | FOMO + Scarcity |
| 44 | Only 24 Hours Left | Countdown + support | Urgency + Risk Reversal |
| 45 | Last Chance to Fix | Final problem reminder | Social Proof + FOMO |
| 46 | Last Call | Final notice | Scarcity + Loss Aversion |

**POST-LAUNCH PHASE - BUYERS (5 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 47 | Welcome/Get Started | Onboarding guide | Reduces Friction + Builds Confidence |
| 48 | How's It Going? | Proactive check-in | Builds Relationships + Reduces Churn |
| 49 | Get Most Value | Advanced tips | Increases Engagement + Advocacy |
| 50 | Share Your Wins | Request success stories | Social Proof + Community |
| 51 | Upsell/Cross-Sell | Complementary offers | Customer Lifetime Value |

**POST-LAUNCH PHASE - NON-BUYERS (3 plays)**

| # | Play Name | Purpose | Psychology |
|---|-----------|---------|------------|
| 52 | Feedback Request | Learn why they didn't buy | Reciprocity + Cognitive Dissonance |
| 53 | If You Missed It | Acknowledge kindly | Maintains Positive Relationships |
| 54 | So Much Gratitude | Thank everyone | Cultivates Positive Emotions |

### 6.4 Play Template Data Structure

```python
class LaunchPlay(models.Model):
    play_number = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    # Phase categorization
    phase = models.CharField(choices=[
        ('pre_launch', 'Pre-Launch'),
        ('launch_announcement', 'Launch: Announcement'),
        ('launch_nurture', 'Launch: Nurture'),
        ('launch_urgency', 'Launch: Urgency/Cart Close'),
        ('post_launch_buyers', 'Post-Launch: Buyers'),
        ('post_launch_nonbuyers', 'Post-Launch: Non-Buyers'),
    ])

    # Size inclusion flags
    included_in_small = models.BooleanField(default=False)   # 12 plays
    included_in_medium = models.BooleanField(default=False)  # 29 plays
    included_in_large = models.BooleanField(default=True)    # 54 plays

    # Psychology & purpose
    purpose = models.TextField()  # What this play accomplishes
    psychology = models.TextField()  # Why it works (cognitive principle)
    aha_moment_template = models.TextField()  # The insight audience gets

    # Content templates (with {{placeholders}})
    content_concept_template = models.TextField()
    vibe_opportunity_template = models.TextField()
    soft_cta_template = models.TextField()
    hook_templates = models.JSONField()  # List of 3 hook options

    # AI customization instructions
    ai_prompt_template = models.TextField()  # How to customize for specific business

class GeneratedPlaybook(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)

    # Business context for customization
    business_name = models.CharField(max_length=255)
    offer_name = models.CharField(max_length=255)
    target_audience = models.TextField()
    key_problem = models.TextField()
    key_transformation = models.TextField()
    case_studies = models.JSONField(default=list)

    # Generation settings
    size = models.CharField(choices=[
        ('small', 'Small - 12 plays'),
        ('medium', 'Medium - 29 plays'),
        ('large', 'Large - 54 plays'),
    ])

    created_at = models.DateTimeField(auto_now_add=True)

class GeneratedPlay(models.Model):
    playbook = models.ForeignKey(GeneratedPlaybook, on_delete=models.CASCADE)
    play = models.ForeignKey(LaunchPlay, on_delete=models.CASCADE)

    # AI-customized content
    customized_concept = models.TextField()
    customized_aha = models.TextField()
    customized_vibe = models.TextField()
    customized_cta = models.TextField()
    customized_hooks = models.JSONField()  # 3 customized hooks

    # Scheduling
    scheduled_date = models.DateField(null=True)
    status = models.CharField(choices=[
        ('draft', 'Draft'),
        ('scheduled', 'Scheduled'),
        ('published', 'Published'),
    ], default='draft')
```

### 6.4 Play Data Model
```python
class LaunchPlay(models.Model):
    play_number = models.IntegerField()
    name = models.CharField(max_length=100)
    phase = models.CharField(choices=[
        ('pre_launch', 'Pre-Launch'),
        ('launch_announcement', 'Launch: Announcement'),
        ('launch_nurture', 'Launch: Nurture'),
        ('launch_urgency', 'Launch: Urgency/Cart Close'),
        ('post_launch_buyers', 'Post-Launch: Buyers'),
        ('post_launch_nonbuyers', 'Post-Launch: Non-Buyers'),
    ])
    mp3_category = models.CharField(choices=[
        ('problem', 'Market the Problem'),
        ('process', 'Market the Process'),
        ('proof', 'Market the Proof'),
    ])
    included_in_small = models.BooleanField(default=False)
    included_in_medium = models.BooleanField(default=False)
    included_in_large = models.BooleanField(default=True)

    # Template fields
    content_concept_template = models.TextField()
    aha_moment_template = models.TextField()
    vibe_opportunity_template = models.TextField()
    soft_cta_template = models.TextField()
    hook_templates = models.JSONField()  # List of 3 hooks

class GeneratedPlaybook(models.Model):
    user = models.ForeignKey(User)
    business_name = models.CharField(max_length=255)
    offer_name = models.CharField(max_length=255)
    target_audience = models.TextField()
    industry_context = models.TextField()
    size = models.CharField(choices=[('small', 'Small'), ('medium', 'Medium'), ('large', 'Large')])
    created_at = models.DateTimeField(auto_now_add=True)

class GeneratedPlay(models.Model):
    playbook = models.ForeignKey(GeneratedPlaybook)
    play = models.ForeignKey(LaunchPlay)
    customized_concept = models.TextField()
    customized_aha = models.TextField()
    customized_vibe = models.TextField()
    customized_cta = models.TextField()
    customized_hooks = models.JSONField()
```

### 6.5 AI Playbook Generator
```python
class PlaybookGenerator:
    """
    Input: Business context, offer details, audience, launch size
    Output: Fully customized playbook with all plays filled in

    Process:
    1. User provides: business_name, offer_name, target_audience, industry_context
    2. System selects plays based on size (12/29/54)
    3. AI customizes each play template for the specific business
    4. Returns downloadable playbook (CSV/PDF)
    """

    def generate(self, context: dict, size: str) -> GeneratedPlaybook:
        plays = LaunchPlay.objects.filter(**size_filter(size))

        for play in plays:
            customized = self.ai_customize(play, context)
            GeneratedPlay.objects.create(
                playbook=playbook,
                play=play,
                customized_concept=customized['concept'],
                customized_aha=customized['aha'],
                customized_vibe=customized['vibe'],
                customized_cta=customized['cta'],
                customized_hooks=customized['hooks'],
            )

        return playbook
```

### 6.6 Transformation Finder (Pre-Generation Step)

Before generating a playbook, users must clarify their offer's core transformation. This AI-powered tool analyzes their input and produces master-level insights.

**Flow Integration:**
```
User Input (URL, notes, landing page, offer description)
      │
      ▼
Transformation Finder ──► Analyzes and produces:
      │                    - FROM/TO transformation statement
      │                    - Key benefits
      │                    - Non-obvious insights
      │                    - Limiting beliefs to address
      │                    - Expert framework alignment
      ▼
Transformation Report ──► User reviews/refines
      │
      ▼
Playbook Generator ──► Uses transformation as context for all 54 plays
```

**Transformation Analysis Output:**
```python
class TransformationAnalysis(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)

    # Input
    raw_input = models.TextField()  # URL, notes, landing page text
    input_type = models.CharField(choices=[
        ('url', 'URL'),
        ('landing_page', 'Landing Page'),
        ('notes', 'Notes/Description'),
        ('offer_doc', 'Offer Document'),
    ])

    # Core Transformation
    from_state = models.TextField()  # Current painful state
    to_state = models.TextField()    # Desired transformed state

    # Key Benefits (JSON list)
    key_benefits = models.JSONField()  # 5 numbered benefits

    # Deep Insights
    non_obvious_insights = models.JSONField()  # Master-level insights
    expert_frameworks = models.JSONField()  # Amy Porterfield, Hormozi, Brunson, Brian Clark

    # Challenges & Solutions
    limiting_beliefs = models.JSONField()  # Objections with solutions

    # Communication Strategy
    positioning_tips = models.JSONField()
    social_proof_strategy = models.TextField()
    value_articulation = models.TextField()

    created_at = models.DateTimeField(auto_now_add=True)
```

**AI Prompt Template (Transformation Finder):**
```
You are analyzing content/offer to identify the core transformation.

INPUT: {user_input}

STEPS:
1. List key themes, topics, and solutions presented
2. Identify the primary problem being solved
3. Brainstorm multiple possible transformations
4. Select the most impactful transformation

OUTPUT FORMAT:

## Transformation Statement
FROM: [3 bullet points describing current painful state]
TO: [3 bullet points describing desired transformed state]

## Key Benefits
1. **[Benefit Name]:** [Description]
2. **[Benefit Name]:** [Description]
3. **[Benefit Name]:** [Description]
4. **[Benefit Name]:** [Description]
5. **[Benefit Name]:** [Description]

## Non-Obvious Master-Level Insights
1. **[The Hidden X]** - [Deep insight about what's really happening]
2. **[The Y Principle]** - [Strategic insight about the mechanism]
3. **[The Z Paradox]** - [Counter-intuitive insight]

## Expert Framework Alignment
- **Amy Porterfield**: [How this aligns with her micro-transformation approach]
- **Alex Hormozi**: [How this fits his value equation]
- **Russell Brunson**: [How this uses his funnel psychology]
- **Brian Clark**: [How this attracts the right audience]

## Limiting Beliefs & Solutions
1. "[Objection]" → Solution: [How to address it]
2. "[Objection]" → Solution: [How to address it]
3. "[Objection]" → Solution: [How to address it]

## Communication Strategy
- **Positioning**: [How to frame the offer]
- **Social Proof**: [What proof to collect/showcase]
- **Value Articulation**: [How to communicate the transformation]

IMPORTANT: Aim for non-obvious, master-level insights. Avoid clichés.
```

**Integration with Playbook Generation:**

The TransformationAnalysis feeds directly into playbook customization:
- `from_state` → Powers "Problem" plays (1-9)
- `to_state` → Powers "Process" and "Proof" plays
- `limiting_beliefs` → Powers objection-handling plays (34, 38)
- `key_benefits` → Powers benefit-focused plays (7, 26, 36)
- `non_obvious_insights` → Powers differentiation plays (18, 6)

### 6.7 Lead Magnet Generator (AI-Powered)

Integrated lead magnet creation system based on Rob Lennon's automation stack. Users can generate high-converting lead magnets in under 5 hours.

**Core Philosophy:**
- Focus on **WHAT** needs to be done and **WHY** it matters
- Minimal **HOW** details → drives prospects to paid offer
- High perceived value, consumable in 5 minutes or less
- Solves ONE immediate problem that prevents buying

**Tech Stack Integration:**
```
User Input (business context, audience, offer)
      │
      ▼
Lead Magnet Generator ──► AI creates 3 lead magnet concepts:
      │                    - Title (curiosity-driving)
      │                    - WHAT (the insight/framework)
      │                    - WHY (urgency/importance)
      │                    - FORMAT (PDF, video, cheat sheet)
      ▼
Template Selection ──► User picks format
      │
      ▼
Content Generation ──► AI fills template with personalized content
      │
      ▼
Export Options:
├─ Google Docs (via Make.com webhook)
├─ PDF download
└─ Notion template
```

**Data Model:**
```python
class LeadMagnetConcept(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    transformation = models.ForeignKey(TransformationAnalysis, null=True)

    # Generated concepts (3 per generation)
    title = models.CharField(max_length=255)
    what_description = models.TextField()  # The insight/framework
    why_description = models.TextField()   # Urgency/importance
    format_suggestion = models.TextField()  # PDF, video, cheat sheet

    # Selection tracking
    selected = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

class GeneratedLeadMagnet(models.Model):
    concept = models.ForeignKey(LeadMagnetConcept, on_delete=models.CASCADE)

    # Full generated content
    content_json = models.JSONField()  # Structured content for template
    google_doc_url = models.URLField(null=True)
    pdf_url = models.URLField(null=True)

    # Automation tracking
    make_webhook_triggered = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
```

**AI Prompt Template (Lead Magnet Generator):**
```
# PRIME DIRECTIVE
Create 3 unique AI-powered lead magnets that address one problem the
AUDIENCE must solve before buying the product or service.

Focus on **WHAT** needs to be done and **WHY** it's important, offering
only minimal details on **HOW** to do it. The recipient should be left
deciding between figuring out HOW themselves or taking a shortcut via
the paid product/service.

## CONTEXT
Business: {business_name}
Offer: {offer_name}
Target Audience: {target_audience}
Core Transformation: FROM {from_state} TO {to_state}
Key Problem: {key_problem}

## TASK GUIDE
1. **Clarity:** Clear, actionable language. Immediate benefit understanding.
2. **High Value, Low Time:** Creatable in 5 hours, high-impact insights.
3. **Avoid Generic Phrasing:** Genuine and specific, not academic.
4. **Flesch-Kincaid 8:** 8th grade reading level. Minimal jargon.

## OUTPUT FORMAT
Output as valid JSON only with this structure:

{
  "lead_magnets": [
    {
      "title": "[Curiosity-driving title]",
      "what": "[The insight/framework - what they'll learn]",
      "why": "[Why this matters urgently - the cost of not solving]",
      "format": "[Suggested format: PDF, video, cheat sheet, etc.]"
    }
  ]
}
```

**Automation Integration (Make.com Webhook):**
```python
class MakeWebhookService:
    """
    Triggers Make.com scenario to:
    1. Create Google Doc from template
    2. Insert generated content via {{placeholders}}
    3. Set sharing to "Anyone with link"
    4. Email user the document link
    """

    def trigger_doc_creation(self, lead_magnet: GeneratedLeadMagnet):
        payload = {
            "email": lead_magnet.concept.user.email,
            "title": lead_magnet.concept.title,
            **lead_magnet.content_json  # All template fields
        }
        response = requests.post(
            settings.MAKE_WEBHOOK_URL,
            json=payload
        )
        return response.json()
```

**Template Library:**
Pre-built Google Doc templates for common formats:
- 1-Page Cheat Sheet
- 2-Page PDF Guide
- Framework Overview
- Checklist/Scorecard
- Mini-Report (Product Clarity Report style)

### 6.8 Real Launch Examples (Training Data)

Reference examples from Erica Schneider's "Long to Short" course launch. These demonstrate how each play maps to real email/social content.

**Play-to-Content Mapping (From Long to Short Launch):**

| Play # | Play Name | Real Example | Key Technique |
|--------|-----------|--------------|---------------|
| 1 | Something's Coming | "Turn Your One-Hit Wonders into Timeless Classics" | Problem metaphor + tease solution |
| 2 | Problem Spotlight | "You spend ~6 hours writing...Gone and forgotten" | Paint the pain vividly |
| 6 | Untold Story | "Cut the Fluff: Let's find your writing voice" | Personal origin story + vulnerability |
| 8 | Big Mistake | "Be an emotional detective" | Common misconception exposed |
| 9 | Real Reason | "What a narcissist expert taught me" | Unexpected analogy + insight |
| 10 | Behind the Scenes | "My sixth sense is tingling" | Show creative process |
| 13 | Story Behind | "The day I realized what I was sitting on" | Narrative discovery moment |
| 14 | Quick Win | "Struggling to write? Do this." | 4-step framework immediately usable |
| 18 | Secret Sauce | "Watch me get 9 social post ideas from one sentence" | Live demonstration of method |
| 23 | Development Updates | "Just a few more hours" | Palms sweaty, knees weak buildup |
| 24 | Launch Announcement | "It's here! Grab Long to Short" | Clear CTA + feature list |
| 27 | Early Responses | "$11.5k in 21 hours" | Honest transparency about success |
| 32 | Bonus Reveal | "1 day left: AI prompt I've been dreaming of" | New value introduced mid-launch |
| 33 | The Audit | "You've got pie on your face" | Cost of current approach |
| 46 | Last Call | "2.5 hours left: Last call for the sale" | Countdown + final summary |

**Email Structure Templates (From Real Launch):**

**Pre-Launch Story Email Template:**
```
{Engaging Opening: Commanding action statement}

Before we dive in today, I've got a quick announcement.

{Personal anecdote from childhood/past - 2-3 paragraphs}

But when I {entered new phase}, I {sacrificed authentic voice}.

I was fine with it because {initial positive outcome}.

This continued until {catalyst for change}...

{Detailed framework or steps - numbered list}

Step 1: {Title}
{Description with personal example}

Step 2: {Title}
{Description with actionable tool}

[Continue steps...]

This only scratches the surface, but it's a good start.

PS: {Soft tease of upcoming offer}
```

**Launch Day Email Template:**
```
🎉 {Excited announcement}!

The time has come...

For you to {main benefit}...

And in under {timeframe}...

Get {specific outcome} 🔥🔥🔥

​Get {Product Name}​

{Product Name} is a step-by-step system to {main function}.

{Option 1: Manual method}
OR
{Option 2: AI-powered method}

What's inside {Product Name}:
- {Feature 1 with benefit}
- {Feature 2 with benefit}
- {Feature 3 with benefit}
- {Bonus resources}

{CTA Button}

Cheers,
{Name}

PS: {Personal note or urgency}
```

**Mid-Launch Social Proof Email Template:**
```
{Achievement metric} {emoji}

Gosh, what a {occasion}.

MEANTIME, HOLY SHIT THANK YOU.

{X} people have grabbed {Product} so far. That's ${amount} in sales 🔥😭🙏

But I do have to say...

While the bros would gush over {achievement}, that's 100% bullshit.

Here's the truth:
- {Reality factor 1}
- {Reality factor 2}
- {Reality factor 3}
- {Reality factor 4}

I love our success, but I'm never gonna conflate it with "overnight."

{Tour/demo of product}

{CTA}
```

**Cart Close Email Template:**
```
👇 {Time remaining}: Last call for the sale

In a few short hours, doors close on {Product} sale.

Thanks for sticking with me the past {X} days.

{Countdown timer}

So, for the last time, here's what's inside:
- {Feature 1}
- {Feature 2}
- {Feature 3}
- {Bonus}

{CTA with urgency}

PS: After tonight, {what happens next}
```

**Voice & Tone Characteristics (Erica Schneider Style):**
- Conversational, like talking to a friend
- Uses parentheticals for asides: "(can you feel the sarcasm, cuz it's there)"
- Varied sentence length: Long → Short → Punchy
- Bold/italic for emphasis on key points
- Emojis sparingly but strategically
- Self-deprecating humor balanced with expertise
- "Cheers" as sign-off
- PS always includes value (tease, link, or clarification)

**Content-to-Play Mapping for AI:**
The playbook generator uses these patterns to customize content:
```python
PLAY_PATTERNS = {
    'untold_story': {
        'structure': 'personal_anecdote → struggle → realization → lesson',
        'hooks': ['I'll never forget...', 'I've been staring at...', 'Lately, I've been feeling...'],
        'voice': 'vulnerable, honest, relatable'
    },
    'problem_spotlight': {
        'structure': 'paint_pain → amplify_cost → hint_solution',
        'hooks': ['You spend X hours...', 'People think...', 'If you...'],
        'voice': 'empathetic, understanding, slightly provocative'
    },
    'quick_win': {
        'structure': 'promise → numbered_steps → each_step_detailed → takeaway',
        'hooks': ['Here\'s a simple X-step process...', 'Do this today...'],
        'voice': 'instructional, encouraging, actionable'
    },
    'launch_announcement': {
        'structure': 'excitement → outcome → product_name → features_list → CTA',
        'hooks': ['The time has come...', 'It\'s here!', '🎉'],
        'voice': 'energetic, confident, clear'
    },
    'last_call': {
        'structure': 'urgency → gratitude → recap_features → final_CTA',
        'hooks': ['Last chance...', 'X hours left...', 'Doors close...'],
        'voice': 'urgent but not desperate, appreciative'
    }
}
```

---

## Phase 7: Billing & Tiers

### 7.1 Revenue Model
| Tier | Price | Features |
|------|-------|----------|
| Free Preview | $0 | Top 3 matches (gated) |
| Playbook | $297 one-time | 54-step guide, templates |
| Starter | $97/mo | X matches/month |
| Pro | $297/mo | More matches, AI outreach |
| Enterprise | $597/mo | Unlimited, priority support |
| DFY Intro | $1,500 | Setup + first campaign |
| DFY Management | $2,500-$4,000/mo | Full service |

### 7.2 Stripe Integration
- Subscription management
- Tier-based feature gating
- Usage tracking per tier

---

## Phase 8: Admin / Mission Control

### 8.1 KPI Dashboard
- Match generation rates
- Conversion tracking (viewed → contacted → connected)
- Revenue per deal
- Data health metrics

### 8.2 Data Health Monitor
Current issues to track:
- Email coverage: 0.1% → target 50%+
- Offering data: 38.5% → target 90%+
- Platinum trust ratio: 0% → target 10%+

---

## Files to Port vs Rewrite

### PORT (Adapt existing code)
| Source | Destination | Notes |
|--------|-------------|-------|
| `jv-matcher-temp/match_generator.py` | `apps/matching/services/` | V1 algorithm intact |
| `jv-matcher-temp/rich_match_service.py` | `apps/matching/services/` | GPT prompts intact |
| `jv-matcher-temp/directory_service.py` | `apps/directory/services/` | Adapt for Django |
| `jv-matcher-temp/services/pdf_generator.py` | `apps/reports/services/` | Direct port |
| `jv-matcher-temp/config/tactics.json` | `config/` | Direct copy |
| `enrichment-engine/sources/*.py` | `apps/enrichment/services/` | Direct port |
| `enrichment-engine/clay_bridge/*.py` | `apps/enrichment/services/` | Flask → Django |

### REWRITE (New implementation)
| Original | New | Reason |
|----------|-----|--------|
| `app.py` (Streamlit) | Django views + templates | HAT stack |
| Flask webhook | Django view | Framework change |
| Inline CSS | Tailwind CSS | Design system |

---

## Implementation Order

1. **Week 1-2**: Django project setup, auth, base templates
2. **Week 3-4**: Directory module (profiles, search)
3. **Week 5-7**: Matching engine (scoring, rich analysis)
4. **Week 8-9**: Enrichment pipeline integration
5. **Week 10-11**: Billing & tier system
6. **Week 12-13**: Playbook module (requires documentation completion)
7. **Week 14-15**: Launch content engine
8. **Week 16-17**: Admin dashboard, testing, polish

---

## Critical Dependencies

1. **54-Step Playbook Documentation** - Currently partial, blocks Phase 6
2. **Data Health Fix** - Email/offering coverage critical for matching quality
3. **Supabase vs PostgreSQL** - Decision needed before Phase 1

---

## Blueprint Framework Integration (GTM AI Transformation)

The Blueprint GTM methodology (Jordan Crawford) integrates throughout the platform:

### Core Principles Applied

| Blueprint Principle | Platform Implementation |
|---------------------|------------------------|
| "The List IS the Message" | Enrichment pipeline creates hyper-targeted profiles |
| Data Moats | Proprietary data recipes from 2-5 combined signals |
| PQS (Pain-Qualified Segments) | Match scoring identifies acute partner needs |
| PVP (Permissionless Value Props) | Rich analysis generates value before asking |
| Role Decomposition | AI: research/matching, Humans: relationships/trust |

### PQS Discovery Agent (Built into Enrichment)
```python
class PQSDiscoveryAgent:
    """
    Monitors data sources to identify partners showing pain signals
    matching our target PQS definitions
    """
    pain_signals = [
        "expansion_pressure",    # New locations, hiring
        "competitive_pressure",  # Competitor moves
        "operational_pressure",  # Tech/process changes
        "financial_pressure",    # Funding, revenue shifts
    ]

    def score_prospect(self, profile) -> dict:
        return {
            "pqs_signals": [...],
            "composite_score": 0.85,
            "recommended_action": "immediate_outreach",
            "reasoning": "Multiple converging signals"
        }
```

### PVP Generation (Rich Match Service Enhancement)
```python
# Enhanced rich_match_service.py output
{
    "pvp_type": "revenue_recovery",
    "data_foundation": {
        "audience_overlap": "73%",
        "complementary_offerings": true,
        "list_size_symmetry": 0.8
    },
    "value_statement": "Your audience needs X, I deliver X",
    "supporting_evidence": [...],
    "confidence": 0.8
}
```

### Data Recipe Examples for JV Matching
1. **Partner Readiness**: list_size + recent_launch + engagement_rate → "ready to promote"
2. **Audience Fit**: niche_overlap + audience_demographics + past_promos → "alignment score"
3. **Timing Signal**: product_launch_date + promotional_calendar + competitor_moves → "optimal window"

### Task Taxonomy in Platform
| Task Type | Owner | Examples |
|-----------|-------|----------|
| Pure AI | System | Profile enrichment, match scoring, data monitoring |
| AI-Initiated, Human-Refined | Hybrid | Outreach drafts, partnership proposals |
| Human-Only | User | Relationship building, deal negotiation, trust |
| Workflow Automation | System | Triggers, routing, status updates |
| Meta-Learning | System | Score calibration, model improvement |

---

## Implementation Strategy

### User Decisions
- **Launch Content Engine**: Both integrated feature AND standalone product
- **54-Step Playbook**: Hybrid approach (user provides partial docs, we fill gaps)
- **MVP Priority**: Full platform - all capabilities needed

### Build Order (Parallel Tracks)

```
TRACK A: Core Platform          TRACK B: Content Engine
─────────────────────           ───────────────────────
Week 1-2: Django + Auth         Week 1-2: Play templates DB
Week 3-4: Directory/Profiles    Week 3-4: AI customization
Week 5-7: Matching Engine       Week 5-6: Playbook generator UI
Week 8-9: Enrichment Pipeline   Week 7: Standalone mode
Week 10-11: Billing/Tiers       Week 8: CSV/PDF export
Week 12-13: 54-Step Playbook
Week 14-15: Integration
Week 16-17: Testing/Polish
```

### Immediate Next Steps

1. **Share your 54-step playbook documentation** (whatever exists)
2. **Start Django project scaffolding** with HAT stack
3. **Port match_generator.py** V1 scoring algorithm
4. **Create LaunchPlay model** with 54 play templates
5. **Keep Supabase** initially (migrate later if needed)

### Critical Files to Create First

```
jv-matchmaker/
├── apps/
│   ├── accounts/           # Auth + subscriptions
│   ├── directory/          # Profiles (port directory_service.py)
│   ├── matching/           # V1 scoring (port match_generator.py)
│   ├── enrichment/         # Data pipeline (port enrichment-engine/)
│   ├── playbook/           # 54-step execution tracking
│   ├── launch_content/     # MP3 + 54-play generator (NEW)
│   └── billing/            # Stripe + tiers
├── templates/
│   └── base.html           # HTMX + Alpine.js + Tailwind
└── config/
    ├── tactics.json        # Port from jv-matcher
    └── plays.json          # 54 launch plays (NEW)
```

### Revenue Model Finalized

| Product | Price | Delivery |
|---------|-------|----------|
| **Standalone Playbook Generator** | $297 one-time | Web app, CSV/PDF export |
| **JV Matchmaker Free** | $0 | 3 match previews, email gate |
| **JV Matchmaker Starter** | $97/mo | Matching + basic playbook |
| **JV Matchmaker Pro** | $297/mo | Full matching + content engine |
| **JV Matchmaker Enterprise** | $597/mo | Unlimited + priority support |
| **DFY Setup** | $1,500 | Done-for-you onboarding |
| **DFY Management** | $2,500-$4,000/mo | Full service |

---

## Cold Outreach Engine (GEX Methodology Integration)

Based on Eric Nowoslawski's GEX Wrapped 2024 learnings, integrate cold email capabilities:

### Core Principles (From GEX)

1. **The List IS the Message** - Hyper-precise targeting makes messaging inherently relevant
2. **Data Moats** - Unique public data combinations > commodity data (Apollo/ZoomInfo)
3. **AI for Personalization, Not Whole Emails** - AI writes pieces, never entire emails
4. **Test Before Scale** - Validate offers with small batches first

### Email Infrastructure Guidelines

```
Per Inbox: 30 emails/day (can push to 50-60 if campaign performs well)
Inboxes per Domain: 2
Warmup Period: 3 weeks minimum
Sequence Length: 4 emails max
Batch Strategy: 4 batches of sending capacity (Google/Hypertide/Google/Hypertide)
```

### Outreach Sequence Structure (4 emails)
```
Email 1: Net new - Why you, why now + offer
Email 2: Threaded to E1 - More context, case study depth
Email 3: Net new - Different value prop, lower friction CTA
Email 4: Threaded to E3 - Hail mary / "right person?" check
```

### The 4 B2B Offers (What to Say)
Every B2B offer helps prospects:
1. **Save Time** - "No need to hire SDRs"
2. **Save Money** - "Half the cost of an in-house team"
3. **Make More Money** - "Generate $4.2M in pipeline"
4. **Reduce Risk** - "Free test, pay only for results"

### AI Email Writing Framework
1. Tell AI its job (NOT "write a cold email" - say "write a persuasive first sentence")
2. Give AI knowledge to make decisions from (scraped data, not assumptions)
3. Give specific direction (tone, length, constraints)
4. Give examples or prefixes ("I was researching your pricing page and saw...")

### Campaign Testing Batches (GEX Methodology)

**Batch 1: Lookalike + AI Messaging**
- Lookalike campaign (case study matching)
- Standard email sequence with AI personalization
- Creative ideas campaign (3 AI-generated ideas per prospect)

**Batch 2: Triggers + List Refinement**
- New in role
- Hiring for specific roles
- Tech installed on site
- LinkedIn profile keywords
- Past company at current customers
- New fundraise
- Headcount growth/decline

**Batch 3: Copywriting Formats**
- Poke the bear questions
- Super short emails (1-2 sentences)
- Priority asking ("Is {{problem}} a priority?")
- Case study overwhelm (P.S. with multiple proof points)

**Batch 4: Social + Validation**
- LinkedIn engagement scraping
- LinkedIn group scraping
- Feedback campaigns
- Pay-for-meeting campaigns (last resort validation)

### Success Metrics (GEX Benchmarks)

| Metric | Target | Notes |
|--------|--------|-------|
| Positive response rate | 1 per 250-350 contacts | Agency average |
| Max theoretical response | 30% | Even with perfect offer |
| Bounce rate | < 3% | Double-verify all emails |
| Reply rate (any) | > 1% | Confirms primary inbox landing |

### Integration with JV Matchmaker

```
Match Generated
      │
      ▼
Outreach Sequence Created ──► AI personalizes based on:
      │                        - Match fit analysis
      │                        - PVP data foundation
      │                        - Social signals (LinkedIn posts)
      ▼
4-Email Sequence
      │
      ├─ Email 1: "Saw your [trigger]. We've helped [similar partner] achieve [outcome]"
      ├─ Email 2: Deeper case study context
      ├─ Email 3: Different angle (save time vs make money)
      └─ Email 4: "Should I reach out to [other team member] instead?"
```

### Data Sources for Outreach

| Source | Use Case | Cost |
|--------|----------|------|
| Clay.com | List building, enrichment, AI | $$$|
| Apollo.io | Initial lists (export limits) | $$ |
| Prospeo/LeadMagic/TryKitt | Email finding | $ |
| Debounce/MillionVerifier | Email verification | $ |
| Trigify | LinkedIn engagement data | $$ |
| Ocean.io | Lookalike audiences | $$ |

### Key Templates to Build

1. **Lookalike Campaign** - "Saw you're in {{industry}} like our customer {{case_study}}..."
2. **Standard + AI** - "{{trigger_line}}. {{ai_personalization}}. If we could help {{outcome}}..."
3. **Creative Ideas** - "Saw how you {{mission}}. Had 3 ideas: {{idea1}}, {{idea2}}, {{idea3}}..."
4. **Poke the Bear** - "How do you know {{current_solution}} is {{optimal_outcome}}?"
5. **Super Short** - "Are you like other {{title}} who keep telling me they're {{problem}}?"
