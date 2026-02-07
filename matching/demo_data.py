"""
Fairy tale mock data for the demo Partner Outreach page.

No database or migrations. Used by DemoOutreachView for promotional preview.
"""

# Demo client (fairy tale "business")
DEMO_CLIENT = {
    "name": "Once Upon a Time Partners",
    "tagline": "Premium events and transformations for leaders",
    "profile_url": "/matching/demo/profile/",
    "website": "https://example.com",
    "updated": "Updated Feb 2025",
}

# Demo partners: section is one of 'ready' | 'verify' | 'linkedin'
# Contact fields optional; use None or omit for "—" or hidden
DEMO_PARTNERS = [
    # --- Ready to Contact (direct email verified) ---
    {
        "name": "Little Red Riding Hood",
        "company": "Red's Forest Bakery & Guides",
        "tagline": "Forest safety tours and artisanal bakery",
        "badge": "Top Match",
        "list_size": "295K",
        "section": "ready",
        "email": "red@hood.com",
        "website": "https://example.com",
        "phone": "555-0101",
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": None,
        "niche": "Forest Safety, Bakery, Storytelling",
        "why_fit": [
            "Serves travelers and food enthusiasts. Large 295K list; weekly newsletter—ideal for Once Upon a Time's event and transformation message.",
            "Top priority. Verified seeking: JV partnerships, retreat hosts, and webinar presenters. One of the largest story-led experience platforms.",
        ],
    },
    {
        "name": "Fairy Godmother",
        "company": "Transformations & Events Co.",
        "tagline": "Transformations and premium events",
        "badge": "Active JV",
        "list_size": "91K",
        "section": "ready",
        "email": "fairy@transform.com",
        "website": "https://example.com",
        "phone": "555-0102",
        "linkedin": "https://example.com",
        "pr_contact": "Pumpkin Coach 555-0199 or coach@example.com",
        "schedule": None,
        "niche": "Events, Transformations, Coaching",
        "why_fit": [
            "Large engaged list of leaders and event planners. World-class expertise on transformations and events—strong fit for Once Upon a Time's audience.",
            "Known active JV partner. Regularly promotes complementary offers and co-hosted events.",
        ],
    },
    {
        "name": "Rapunzel",
        "company": "Tower Retreats",
        "tagline": "Long-form content and retreat experiences",
        "badge": None,
        "list_size": "171K",
        "section": "ready",
        "email": "rapunzel@towerretreats.com",
        "website": "https://example.com",
        "phone": "555-0103",
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": None,
        "niche": "Retreats, Long-form Content, Wellness",
        "why_fit": [
            "Retreat and long-form content focus; audience aligned with purpose-driven, transformational events (Once Upon a Time Partners).",
            "Speaker and host; strong reach. Who they serve: people seeking immersive experiences and mindset shifts.",
        ],
    },
    {
        "name": "Prince Charming",
        "company": "Kingdom Networking",
        "tagline": "Networking and high-level introductions",
        "badge": None,
        "list_size": "100K",
        "section": "ready",
        "email": "prince@kingdomnetworking.com",
        "website": "https://example.com",
        "phone": None,
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": None,
        "niche": "Networking, Introductions, Leadership",
        "why_fit": [
            "Networking platform for leaders and entrepreneurs. Strong fit for Once Upon a Time's visibility and partnership expansion.",
            "Best contact via website. Who they serve: executives and entrepreneurs seeking strategic connections.",
        ],
    },
    {
        "name": "Three Little Pigs",
        "company": "Build & Grow Construction",
        "tagline": "Sustainable building and business foundations",
        "badge": "Networking",
        "list_size": "22K",
        "section": "ready",
        "email": "pigs@buildandgrow.com",
        "website": "https://example.com",
        "phone": None,
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": None,
        "niche": "Business Foundations, Sustainability, Coaching",
        "why_fit": [
            "Entrepreneurs investing in solid foundations—ideal for Once Upon a Time's partnership focus. Strong fit for coaches and builders ready to scale.",
            "Who they serve: small business owners. Partnership: guest spot, workshop, affiliate on mastermind.",
        ],
    },
    # --- Needs Verification ---
    {
        "name": "Big Bad Wolf",
        "company": "Logistics & Supply Chain Solutions",
        "tagline": "Supply chain and delivery at scale",
        "badge": "Verify Email",
        "list_size": "163K",
        "section": "verify",
        "email": "wolf@logistics.example.com",
        "website": "https://example.com",
        "phone": "555-0201",
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": None,
        "niche": "Logistics, Supply Chain, Scale",
        "why_fit": [
            "Large list of operations and logistics leaders. Supply chain focus—audience fits Once Upon a Time's event and fulfillment needs.",
            "Verify email is current before reaching out.",
        ],
    },
    {
        "name": "Snow White",
        "company": "Seven Dwarfs Consulting",
        "tagline": "Team dynamics and leadership coaching",
        "badge": "Verify Email",
        "list_size": "63K",
        "section": "verify",
        "email": "snow@sevendwarfs.com",
        "website": "https://example.com",
        "phone": "555-0202",
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": "https://example.com/book",
        "niche": "Leadership, Team Building, Coaching",
        "why_fit": [
            "Team and leadership coaching; diversity and collaboration. Audience aligned with Once Upon a Time's leaders and facilitators.",
            "Strong culture fit. Verify contact before outreach.",
        ],
    },
    # --- LinkedIn Outreach ---
    {
        "name": "Jack",
        "company": "Beanstalk Growth Institute",
        "tagline": "High-growth business consulting",
        "badge": "1M+ Reach",
        "list_size": "1M+",
        "section": "linkedin",
        "email": None,
        "website": "https://example.com",
        "phone": None,
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": "https://example.com/beanstalk",
        "niche": "Business Growth, Consulting",
        "why_fit": [
            "Massive reach (1M+). Serves executives, entrepreneurs, and growth leaders—ideal for Once Upon a Time's expansion and facilitator network.",
            "Award-winning growth expert. Biggest potential reach. Worth the LinkedIn connection effort.",
        ],
    },
    {
        "name": "Gingerbread Person",
        "company": "Gingerbread Coaching",
        "tagline": "Coach marketing and visibility",
        "badge": "High Match",
        "list_size": "Coaches",
        "section": "linkedin",
        "email": None,
        "website": "https://example.com",
        "phone": None,
        "linkedin": "https://example.com",
        "pr_contact": None,
        "schedule": "https://example.com/ginger",
        "niche": "Coach Marketing, Visibility",
        "why_fit": [
            "Serves coaches and facilitators. Perfect for Once Upon a Time's certification and global network. Author of Coach Visibility Roadmap; 20+ years.",
            "High match score. Coaches are ideal for facilitator certification. Offering: Visibility Roadmap, 1-on-1 marketing coaching.",
        ],
    },
]


def get_demo_outreach_data():
    """Return client dict and partners list for the demo outreach template."""
    return {
        "client": DEMO_CLIENT,
        "partners": DEMO_PARTNERS,
        "partners_ready": [p for p in DEMO_PARTNERS if p["section"] == "ready"],
        "partners_verify": [p for p in DEMO_PARTNERS if p["section"] == "verify"],
        "partners_linkedin": [p for p in DEMO_PARTNERS if p["section"] == "linkedin"],
    }
