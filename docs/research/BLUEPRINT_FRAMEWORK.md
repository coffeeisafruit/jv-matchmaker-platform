# Blueprint Framework (GTM AI Transformation)

Jordan Crawford's methodology for building GTM AI systems that create sustainable competitive advantages.

## Core Principles

### The List IS the Message
When your targeting is precise enough, the message becomes inherently relevant. The better your list, the less work your copy has to do.

### Data Moats
Build competitive advantages through unique data combinations:
- Commodity data (Apollo, ZoomInfo) = No advantage
- 2-5 unique signal combinations = Sustainable moat
- Proprietary data recipes = Defensible competitive advantage

### PQS (Pain-Qualified Segments)
Identify segments showing active pain signals:
- Not just "who might need this"
- But "who is actively experiencing the pain right now"

### PVP (Permissionless Value Props)
Create value propositions that don't require permission:
- Generate value BEFORE asking for anything
- Demonstrate expertise through actions, not claims

### Role Decomposition
Separate tasks by optimal owner:

| Task Type | Owner | Examples |
|-----------|-------|----------|
| Pure AI | System | Profile enrichment, match scoring, data monitoring |
| AI-Initiated, Human-Refined | Hybrid | Outreach drafts, partnership proposals |
| Human-Only | User | Relationship building, deal negotiation, trust |
| Workflow Automation | System | Triggers, routing, status updates |
| Meta-Learning | System | Score calibration, model improvement |

## Platform Integration

### Principles Applied

| Blueprint Principle | Platform Implementation |
|---------------------|------------------------|
| "The List IS the Message" | Enrichment pipeline creates hyper-targeted profiles |
| Data Moats | Proprietary data recipes from 2-5 combined signals |
| PQS | Match scoring identifies acute partner needs |
| PVP | Rich analysis generates value before asking |
| Role Decomposition | AI: research/matching, Humans: relationships/trust |

## PQS Discovery Agent

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

## PVP Generation

Enhanced rich_match_service.py output:

```python
{
    "pvp_type": "revenue_recovery",
    "data_foundation": {
        "audience_overlap": "73%",
        "complementary_offerings": True,
        "list_size_symmetry": 0.8
    },
    "value_statement": "Your audience needs X, I deliver X",
    "supporting_evidence": [...],
    "confidence": 0.8
}
```

## Data Recipe Examples

### Partner Readiness
```
list_size + recent_launch + engagement_rate -> "ready to promote"
```

### Audience Fit
```
niche_overlap + audience_demographics + past_promos -> "alignment score"
```

### Timing Signal
```
product_launch_date + promotional_calendar + competitor_moves -> "optimal window"
```

## Task Taxonomy

| Task Type | Owner | Examples |
|-----------|-------|----------|
| Pure AI | System | Profile enrichment, match scoring, data monitoring |
| AI-Initiated, Human-Refined | Hybrid | Outreach drafts, partnership proposals |
| Human-Only | User | Relationship building, deal negotiation, trust |
| Workflow Automation | System | Triggers, routing, status updates |
| Meta-Learning | System | Score calibration, model improvement |

## Credits

Based on Jordan Crawford's GTM AI Transformation methodology (Blueprint Framework).
