# Lead Magnet Generator

Build epic lead magnets in an afternoon using AI-powered automation.

Based on Rob Lennon's methodology from The Launch Content Playbook.

## Core Philosophy

- Focus on **WHAT** needs to be done and **WHY** it matters
- Minimal **HOW** details -> drives prospects to paid offer
- High perceived value, consumable in 5 minutes or less
- Solves ONE immediate problem that prevents buying
- Buildable in under 5 hours

## Automation Flow

```
1. Landing page (optional) clicks through to...

2. Tally form captures:
   - Email
   - Info to personalize AI query

3. Make.com scenario triggered by Tally form

4. Pass form inputs to AI (GPT-4o or Claude Sonnet)
   - Use JSON mode for structured output

5. Parse JSON to get individual values

6. Insert values into Google Template
   - Create new doc in shared folder

7. Auto-email person with link to their report

8. Test and promote!
```

## Tech Stack

| Tool | Purpose | Notes |
|------|---------|-------|
| Tally.so | Form builder | Better/cheaper than Typeform |
| Make.com | Automation | Better/cheaper than Zapier |
| OpenAI/Anthropic | AI generation | API key required |
| Google Docs | Template delivery | Set up sharing permissions |

## JSON Data Structure

Don't worry about JSON formatting - each line is just a label and content.

Ask AI to create your JSON template:

**User:**
```
Hey ChatGPT. Format this data as JSON please.

"The Content Overwhelm Cure: 3 AI-Powered Secrets to Cut Your Creation Time in Half"
WHAT: Discover three key areas where AI can save you hours every week...
WHY: You're spending too much time on the wrong things...
Format: A 2-page PDF or 5-minute video...
```

**AI Response:**
```json
{
  "title": "The Content Overwhelm Cure: 3 AI-Powered Secrets to Cut Your Creation Time in Half",
  "what": "Discover three key areas where AI can save you hours every week, allowing you to focus on growth instead of getting bogged down in content creation.",
  "why": "You're spending too much time on the wrong things—crafting content from scratch, manually editing, and overthinking. The audience is likely stuck doing repetitive tasks that could be automated, costing them money and time. AI is the fast track to reclaiming your day.",
  "format": "A 2-page PDF or 5-minute video highlighting the three highest-impact areas for AI use in content creation."
}
```

## AI Prompt (Lead Magnet Generator)

```markdown
# PRIME DIRECTIVE
Take action on the USER INPUT to create 3 unique AI-powered lead magnets that address one problem the AUDIENCE must solve before buying the product or service.

Focus on dreaming up lead magnets about **WHAT** needs to be done and **WHY** it's important, offering only minimal details on **HOW** to do it. This way the recipient is left deciding between figuring out the HOW for themselves, or taking a shortcut to their desired outcome via your paid product/service.

The goal is to help the audience solve one immediate issue that prevents them from succeeding.

These lead magnets should be able to be built in under 5 hours and be irresistible to the target audience. Their output should be specific, have a perception of high value, and be able to be consumed in 5 minutes or less.

## SKILLS
You will succeed because you are a lead generation expert with years of experience crafting high-conversion lead magnets. Each magnet you create is designed to solve one problem for the audience. Your lead magnets offer valuable insights that guide potential customers toward realizing they need the full service/product to fully solve their challenges.

Your specific focus today is to generate 3 creative lead magnets based on the CONTENT_BRIEF.

You've just enjoyed a warm cup of tea. You've meditated on the task at hand and are in a state of peak flow and creativity. Review the USER INPUT. Consider the audience's pain points and what's stopping them from success in things that matter to them.

Now, tap into the audience's mindset, struggles, and challenges. Consider their **WHY**—why they need to solve this problem before purchasing. The **WHAT** should show them the essential steps they must take. Keep any step-by-step **HOW** limited so they are driven to engage with the full offer for deeper solutions.

## TASK GUIDE
Follow these guidelines for lead magnet creation:
1. **Clarity:** Keep the language clear and actionable. The audience should immediately understand the benefit.
2. **High Value, Low Time:** Ensure each lead magnet can be created within 5 hours. Focus on high-impact insights that help the audience solve one critical issue quickly.
3. **Avoid Generic Phrasing:** Avoid overused or academic-sounding phrases. Keep it genuine and specific.
4. **Flesch-Kincaid 8**: Write at an 8th grade level or below. Avoid using jargon unless the specific lead magnet requires a business concept where the jargon makes things more clear.

# OUTPUT FORMAT
Output as valid, unescaped JSON only, with no pre-text or post text. Provide three lead_magnets, each with a title, what, why, and format suggestion(s). Your responses always begin with { and end with }.

# OUTPUT EXAMPLE
User:
audience of creators, solopreneurs, coaches and consultants who want to use AI to streamline their content and make more money

A:
{
  "lead_magnets": [
    {
      "title": "Content Overwhelm Cure: 3 AI-Powered Secrets to Cut Your Creation Time in Half",
      "what": "Discover three key areas where AI can save you hours every week, allowing you to focus on growth instead of getting bogged down in content creation.",
      "why": "You're spending too much time on the wrong things—crafting content from scratch, manually editing, and overthinking. The audience is likely stuck doing repetitive tasks that could be automated, costing them money and time. AI is the fast track to reclaiming your day.",
      "format": "A 2-page PDF or 5-minute video highlighting the three highest-impact areas for AI use in content creation. It briefly introduces AI tools but leaves them hungry to learn the full application, driving them toward your full course or consulting service for the 'HOW'."
    },
    {
      "title": "60 Minutes for 6 Months of Content: The Simple Framework to Plan Months of Content in One Sitting",
      "what": "A breakdown of an AI-enhanced content calendar framework that helps you organize months' worth of posts, articles, or videos in under 60 minutes.",
      "why": "Without a clear plan, most solopreneurs struggle with consistency, leading to burnout or giving up entirely. Having a solid content strategy is crucial to attract clients and grow your business, and AI can make this process painless.",
      "format": "A 1-page cheat sheet that introduces the core elements of a content calendar and explains why each piece is important. To learn the 'HOW' of setting it up using AI tools, the audience would need to enroll in your course or service."
    },
    {
      "title": "Content Repurposing Matrix: How to Repurpose Your Content to Maximize Reach and Revenue",
      "what": "Understand how AI can instantly repurpose one piece of content into multiple formats (blog, email, social media posts) without much extra effort, ensuring nothing goes to waste.",
      "why": "Creators and coaches often feel they need to constantly create new content to stay relevant, which leads to overwhelm and missed opportunities. AI can multiply their efforts, making every piece of content stretch further without burning them out.",
      "format": "A 2-page guide with a visual example of how one piece of content can turn into five different formats with the help of AI. This guide leaves them needing the 'HOW' (which AI tools and exact methods) to explore through your product/service."
    }
  ]
}
```

## Google Docs Template

To insert data into a Google Doc, define target areas with `{{double_curly_brackets_no_spaces}}`:

```
{{title}}

What You'll Learn
{{what}}

Why This Matters
{{why}}

Recommended Format
{{format}}
```

In Make.com, point your parsed JSON data at these `{{targets}}`.

## Template Library

Pre-built Google Doc templates for common formats:
- 1-Page Cheat Sheet
- 2-Page PDF Guide
- Framework Overview
- Checklist/Scorecard
- Mini-Report (Product Clarity Report style)

## Data Model

```python
class LeadMagnetConcept(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    transformation = models.ForeignKey(TransformationAnalysis, null=True)

    # Generated concepts (3 per generation)
    title = models.CharField(max_length=255)
    what_description = models.TextField()
    why_description = models.TextField()
    format_suggestion = models.TextField()

    # Selection tracking
    selected = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

class GeneratedLeadMagnet(models.Model):
    concept = models.ForeignKey(LeadMagnetConcept, on_delete=models.CASCADE)

    # Full generated content
    content_json = models.JSONField()
    google_doc_url = models.URLField(null=True)
    pdf_url = models.URLField(null=True)

    # Automation tracking
    make_webhook_triggered = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
```

## Make.com Webhook Integration

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
            **lead_magnet.content_json
        }
        response = requests.post(
            settings.MAKE_WEBHOOK_URL,
            json=payload
        )
        return response.json()
```

## Credits

Based on Rob Lennon's methodology from [Lennon Labs](https://lennonlabs.com) and The Launch Content Playbook (Rob Lennon & Erica Schneider).
