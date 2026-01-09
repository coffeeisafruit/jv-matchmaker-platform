# GEX Cold Outreach Methodology

Cold email methodology based on Eric Nowoslawski's GEX Wrapped 2024 learnings.

## Core Principles

### 1. The List IS the Message
Hyper-precise targeting makes messaging inherently relevant. The better your list, the less work your copy has to do.

### 2. Data Moats
Unique public data combinations > commodity data (Apollo/ZoomInfo). Build proprietary data recipes from 2-5 combined signals.

### 3. AI for Personalization, Not Whole Emails
AI writes pieces, never entire emails. Tell AI to write "a persuasive first sentence" not "a cold email."

### 4. Test Before Scale
Validate offers with small batches first. Use the 4-batch testing methodology.

## Email Infrastructure Guidelines

```
Per Inbox: 30 emails/day (can push to 50-60 if campaign performs well)
Inboxes per Domain: 2
Warmup Period: 3 weeks minimum
Sequence Length: 4 emails max
Batch Strategy: 4 batches of sending capacity (Google/Hypertide/Google/Hypertide)
```

## Outreach Sequence Structure (4 emails)

```
Email 1: Net new - Why you, why now + offer
Email 2: Threaded to E1 - More context, case study depth
Email 3: Net new - Different value prop, lower friction CTA
Email 4: Threaded to E3 - Hail mary / "right person?" check
```

## The 4 B2B Offers

Every B2B offer helps prospects:

| Offer Type | Example |
|------------|---------|
| **Save Time** | "No need to hire SDRs" |
| **Save Money** | "Half the cost of an in-house team" |
| **Make More Money** | "Generate $4.2M in pipeline" |
| **Reduce Risk** | "Free test, pay only for results" |

## AI Email Writing Framework

1. **Tell AI its job** - NOT "write a cold email" - say "write a persuasive first sentence"
2. **Give AI knowledge** - Scraped data, not assumptions
3. **Give specific direction** - Tone, length, constraints
4. **Give examples or prefixes** - "I was researching your pricing page and saw..."

## Campaign Testing Batches

### Batch 1: Lookalike + AI Messaging
- Lookalike campaign (case study matching)
- Standard email sequence with AI personalization
- Creative ideas campaign (3 AI-generated ideas per prospect)

### Batch 2: Triggers + List Refinement
| Trigger | Description |
|---------|-------------|
| New in role | Recently changed positions |
| Hiring for specific roles | Job postings signal priorities |
| Tech installed on site | Technology stack signals |
| LinkedIn profile keywords | Self-described priorities |
| Past company at current customers | Relationship potential |
| New fundraise | Budget availability |
| Headcount growth/decline | Organizational change |

### Batch 3: Copywriting Formats
- **Poke the bear questions** - Challenge assumptions
- **Super short emails** - 1-2 sentences only
- **Priority asking** - "Is {{problem}} a priority?"
- **Case study overwhelm** - P.S. with multiple proof points

### Batch 4: Social + Validation
- LinkedIn engagement scraping
- LinkedIn group scraping
- Feedback campaigns
- Pay-for-meeting campaigns (last resort validation)

## Success Metrics (GEX Benchmarks)

| Metric | Target | Notes |
|--------|--------|-------|
| Positive response rate | 1 per 250-350 contacts | Agency average |
| Max theoretical response | 30% | Even with perfect offer |
| Bounce rate | < 3% | Double-verify all emails |
| Reply rate (any) | > 1% | Confirms primary inbox landing |

## Data Sources

| Source | Use Case | Cost |
|--------|----------|------|
| Clay.com | List building, enrichment, AI | $$$ |
| Apollo.io | Initial lists (export limits) | $$ |
| Prospeo/LeadMagic/TryKitt | Email finding | $ |
| Debounce/MillionVerifier | Email verification | $ |
| Trigify | LinkedIn engagement data | $$ |
| Ocean.io | Lookalike audiences | $$ |

## Key Templates

### 1. Lookalike Campaign
```
Saw you're in {{industry}} like our customer {{case_study}}...
```

### 2. Standard + AI
```
{{trigger_line}}. {{ai_personalization}}. If we could help {{outcome}}...
```

### 3. Creative Ideas
```
Saw how you {{mission}}. Had 3 ideas: {{idea1}}, {{idea2}}, {{idea3}}...
```

### 4. Poke the Bear
```
How do you know {{current_solution}} is {{optimal_outcome}}?
```

### 5. Super Short
```
Are you like other {{title}} who keep telling me they're {{problem}}?
```

## Integration with JV Matchmaker

```
Match Generated
      |
      v
Outreach Sequence Created --> AI personalizes based on:
      |                        - Match fit analysis
      |                        - PVP data foundation
      |                        - Social signals (LinkedIn posts)
      v
4-Email Sequence
      |
      +- Email 1: "Saw your [trigger]. We've helped [similar partner] achieve [outcome]"
      +- Email 2: Deeper case study context
      +- Email 3: Different angle (save time vs make money)
      +- Email 4: "Should I reach out to [other team member] instead?"
```

## Data Model

```python
class OutreachSequence(models.Model):
    match = models.ForeignKey(MatchSuggestion, on_delete=models.CASCADE)

    # Sequence configuration
    sequence_type = models.CharField(choices=[
        ('lookalike', 'Lookalike Campaign'),
        ('trigger', 'Trigger-Based'),
        ('creative_ideas', 'Creative Ideas'),
        ('poke_bear', 'Poke the Bear'),
        ('super_short', 'Super Short'),
    ])

    # Personalization data
    trigger_data = models.JSONField()  # Scraped signals
    ai_personalization = models.JSONField()  # AI-generated pieces

    created_at = models.DateTimeField(auto_now_add=True)

class OutreachEmail(models.Model):
    sequence = models.ForeignKey(OutreachSequence, on_delete=models.CASCADE)

    email_number = models.IntegerField()  # 1-4
    is_threaded = models.BooleanField(default=False)  # Threaded to previous

    subject_line = models.CharField(max_length=255)
    body = models.TextField()

    # Tracking
    sent_at = models.DateTimeField(null=True)
    opened_at = models.DateTimeField(null=True)
    replied_at = models.DateTimeField(null=True)
    reply_sentiment = models.CharField(choices=[
        ('positive', 'Positive'),
        ('negative', 'Negative'),
        ('neutral', 'Neutral'),
    ], null=True)
```

## Credits

Based on Eric Nowoslawski's GEX Wrapped 2024 methodology.
