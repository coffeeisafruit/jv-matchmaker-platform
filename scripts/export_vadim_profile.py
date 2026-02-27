#!/usr/bin/env python
"""Export Vadim Voss's profile page with full JV Brief data.

Updates Supabase profile fields and renders static HTML.

Usage:
    python scripts/export_vadim_profile.py --output /tmp/vadim-voss-profile/profile.html
    python scripts/export_vadim_profile.py --output /tmp/vadim-voss-profile/profile.html --update-db
"""
import argparse
import os
import sys

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from matching.models import SupabaseProfile
from scripts.export_static_profile import export_profile


# ─── Vadim's updated content from email + JV Brief ───

VADIM_BIO = """Vadim Voss is a business professional and founder of Next Level DeFi, where he helps everyday people turn crypto from a confusing "casino" into a practical, repeatable passive-income system.

After more than a decade in the crypto markets\u2014and over 20 years as an entrepreneur\u2014Vadim noticed the same pattern over and over: most people don\u2019t fail because they\u2019re \u201cnot smart enough\u201d\u2026 they fail because the setup is stressful, the information is scattered, and one wrong click can cost real money. So he built a simpler path.

That path became Next Level DeFi: a beginner-friendly framework that starts with clear education, then moves into a guided strategy, and\u2014if someone wants it\u2014hands-on help to get fully set up and running.

Today, Vadim\u2019s mission is straightforward: make DeFi accessible, safe, and actionable for non-technical people, so they can start generating passive income without spending months on YouTube, chasing hype tokens, or guessing what to do next."""

VADIM_OFFERING = (
    'Proven DFY (Done-For-You) Crypto (DeFi) Passive Income '
    'White Glove coaching program'
)

VADIM_WHO_YOU_SERVE = (
    'Non-technical people who want passive crypto income: complete beginners, '
    'retirees and near-retirees wanting to maximize bull market gains and secure '
    'retirement, busy professionals who want more free time, and investors curious '
    'about DeFi but intimidated by complexity. Audiences in biz-op, investing, '
    'financial education, trading, and make-money-online spaces.'
)

VADIM_WHAT_YOU_DO = (
    'Teaches everyday people how to generate passive income through DeFi '
    '(Decentralized Finance) using a beginner-friendly framework: clear education '
    'on liquidity mining and yield farming, guided strategy, and optional '
    'Done-For-You setup via personal Zoom screen-share sessions.'
)

VADIM_NICHE = 'DeFi / Crypto Passive Income Education'

VADIM_SEEKING = (
    'JV partners with email lists, podcasts, or masterminds in financial marketing, '
    'biz-op, investing, trading, crypto, and make-money-online spaces. '
    'Partners who attend or speak at events like Financial Marketing Summit. '
    'Audiences of non-technical people interested in passive income, retirement '
    'planning, or financial freedom. Health, wellness, and prepper audiences '
    'also convert well.'
)


# ─── JV Brief data for template context ───

VADIM_CONTEXT_OVERRIDES = {
    # Client card overrides
    'company_name': 'Next Level DeFi',
    'title': 'Founder',

    # About section — Vadim's full bio from email
    'about_story': VADIM_BIO,
    'about_story_paragraphs': [p.strip() for p in VADIM_BIO.split('\n\n') if p.strip()],

    # Program name and description
    'program_name': 'DFY DeFi Setup Experience',
    'program_sub': (
        'DFY (Done-For-You) system where Vadim personally walks every client '
        'through setup and yield activation via Zoom screen-share. Once set up, '
        'nothing else needed \u2014 ongoing support included. High-ticket, '
        'low-refund, audience-friendly.'
    ),
    'program_focus': 'DeFi / Crypto Passive Income',
    'target_audience': 'Financial marketing, biz-op, investing, and crypto audiences',

    # Key positioning message — structured for headline + points layout
    'key_message_headline': '$163K from a single JV launch to a small health/wellness list',
    'key_message_points': [
        'DFY Crypto (DeFi) Passive Income White Glove coaching program ($2,997\u2013$4,997)',
        '40% commission \u00b7 $178 EPC \u00b7 Near-zero refund rate',
        'Swipe copy, tracking links, and replay pages provided',
        'No crypto ads or exchange compliance required',
    ],

    # Program tiers
    'tiers': [
        {
            'name': 'Standard \u2014 $2,997',
            'desc': (
                '4 DFY Zoom sessions: wallet + yield setup, strategy, '
                'automation (Beefy, 20 blockchains), Q&A. All sessions recorded.'
            ),
            'highlight': False,
        },
        {
            'name': 'Elite (VIP) \u2014 $4,997',
            'desc': (
                'Everything in Standard + full course, 12-month VIP Telegram '
                'support, bi-weekly Zoom Q&A clinics, 30-day priority DM '
                'access to Vadim.'
            ),
            'highlight': True,
        },
    ],

    # Commission grid
    'offers_partners': [
        {'value': '40%', 'label': 'JV Commission', 'highlight': True},
        {'value': '$178', 'label': 'EPC', 'highlight': False},
        {'value': '$546', 'label': 'Value / Lead', 'highlight': False},
        {'value': '~0%', 'label': 'Refund Rate', 'highlight': False},
    ],

    # What partners receive
    'partner_deliverables': [
        'Swipe emails + subject lines',
        'Affiliate tracking links',
        'Replay + opt-in pages ready to go',
        'Optional co-host webinar / live Q&A session',
        'No crypto ads or exchange compliance needed for promotion',
    ],

    # Why this converts
    'why_converts': [
        '100% Done-For-You \u2014 no tech barrier for your list',
        'Designed for non-crypto audiences who want safe passive yield',
        'Hand-held and supported \u2014 your audience feels guided, not sold',
        'High EPCs from a $3K\u2013$5K offer',
        'Works for biz-op, investing, health, or \u201cmake money while you sleep\u201d audiences',
        'Clients stay, get results, and send referrals \u2014 builds trust equity with your list',
    ],

    # Proven results
    'launch_stats': {
        'partner': 'Chris James DFY JV #1',
        'audience': 'Non-crypto health/wellness list',
        'metrics': [
            {'label': 'Registration Visitors', 'value': '922'},
            {'label': 'Registrants', 'value': '300 (32.5% opt-in)'},
            {'label': 'Live Attendees', 'value': '140 (46.7% attendance)'},
            {'label': 'Standard Sales', 'value': '13 \u00d7 $2,997 = $38,961'},
            {'label': 'Elite Sales', 'value': '25 \u00d7 $4,997 = $124,925'},
            {'label': 'Total Sales', 'value': '$163,886'},
            {'label': 'Value Per Lead', 'value': '$546'},
            {'label': 'Value Per Attendee', 'value': '$1,171'},
            {'label': 'EPC', 'value': '$177.75'},
        ],
    },

    # Credentials (structured)
    'credentials': [
        {
            'name': 'Next Level DeFi',
            'role': 'Founder',
            'desc': 'DeFi education + systems designed for total beginners who want real results with less stress.',
        },
        {
            'name': 'DFY DeFi Setup Experience',
            'role': '',
            'desc': 'A guided, white-glove setup process (done with you / for you) to get everything working correctly and avoid costly mistakes.',
        },
        {
            'name': 'DeFi Training Workshop',
            'role': '',
            'desc': 'A step-by-step training that explains the fundamentals in plain English and shows the exact path from \u201cconfused beginner\u201d to \u201cconfident, set up, and earning.\u201d',
        },
    ],

    # Resources & proof
    'resource_links': [
        {
            'label': 'Workshop Page',
            'url': 'https://www.nextleveldefi.com/workshop',
            'desc': 'The page partners should promote (after initial sign-up)',
        },
        {
            'label': 'Webinar Replay',
            'url': 'https://www.nextleveldefi.com/workshop-replay',
            'desc': 'The exact presentation that generated $163K',
        },
        {
            'label': 'Registration Page',
            'url': 'https://www.nextleveldefi.com/defi-workshop',
            'desc': 'See the funnel your audience will experience',
        },
        {
            'label': 'Student Testimonials',
            'url': 'https://www.nextleveldefi.com/testimonials',
            'desc': 'From the $997 DIY offer \u2014 course + group Telegram support',
        },
        {
            'label': 'Partner Testimonial \u2014 Professor Spira',
            'url': 'https://nextleveldefi.kartra.com/videopage/daXne7hB4Ua4',
            'desc': 'Health & Wellness audience',
        },
        {
            'label': 'Partner Testimonial \u2014 Caleb Jones',
            'url': 'https://nextleveldefi.kartra.com/videopage/dc19liXFVUYf',
            'desc': 'Business & Make Money Online audience',
        },
    ],

    # Partner FAQs
    'faqs': [
        {
            'q': 'How does the DFY setup work?',
            'a': (
                'Vadim personally walks each client through setup via Zoom '
                'screen-share on a trusted platform. The first yield-generating '
                'position is set up within 1 hour of the first session. All '
                'sessions are recorded so clients can follow along later.'
            ),
        },
        {
            'q': "What's the difference between Standard and Elite?",
            'a': (
                'Standard ($2,997): 4 DFY Zoom sessions covering wallet setup, '
                'yield strategy, automation across 20+ blockchains, and Q&A. '
                'Elite ($4,997): Everything in Standard plus the full course, '
                '12 months VIP Telegram support, bi-weekly live Zoom Q&A clinics, '
                'and 30-day priority DM access to Vadim.'
            ),
        },
        {
            'q': 'How does the refund policy work?',
            'a': (
                'Full refund if no yield-generating position within 14 days of '
                'purchase. Vadim sets it up in the first session, so success is '
                'guaranteed as long as they book within the 14-day window. Only '
                '1 refund request in the last JV \u2014 near-zero refund rate.'
            ),
        },
        {
            'q': 'When and how do I get paid?',
            'a': (
                '40% commission on all collected sales, paid a few days after '
                'the last sale. Higher splits available for volume partners. '
                'No payment plans \u2014 clients can use Klarna at checkout.'
            ),
        },
        {
            'q': 'What do I need to create for promotion?',
            'a': (
                'Nothing. We provide swipe emails with subject lines, affiliate '
                'tracking links, replay and opt-in pages. Optional: co-host '
                'the webinar or join for a live Q&A. No crypto ads or exchange '
                'compliance needed.'
            ),
        },
        {
            'q': 'Does this work for non-crypto audiences?',
            'a': (
                'Yes \u2014 it\'s designed for it. The $163K JV launch was to '
                'a health/wellness list with zero crypto experience. Works great '
                'for biz-op, investing, or \u201cmake money while you sleep\u201d '
                'audiences. Your audience will feel guided, not sold.'
            ),
        },
    ],

    # Seeking goals (properly split, not comma-split)
    'seeking_goals': [
        'JV partners with email lists, podcasts, or masterminds in financial marketing, biz-op, investing, trading, crypto, and MMO spaces.',
        'Partners who attend or speak at events like Financial Marketing Summit.',
        'Audiences of non-technical people interested in passive income, retirement planning, or financial freedom.',
        'Health, wellness, and prepper audiences also convert well.',
    ],

    # Ideal partner
    'ideal_partner_intro': (
        'Partners serving <strong>audiences in financial marketing, biz-op, '
        'investing, trading, and make-money-online spaces who want safe passive '
        'DeFi income without needing crypto experience</strong>.'
    ),
    'ideal_partner_sub': (
        'DFY (Done-For-You) system that helps anyone start earning passive '
        'DeFi income safely \u2014 without needing any tech skills or crypto experience.'
    ),
}


def update_supabase_profile(sp):
    """Update Vadim's Supabase fields with JV Brief + workshop data."""
    sp.bio = VADIM_BIO
    sp.offering = VADIM_OFFERING
    sp.who_you_serve = VADIM_WHO_YOU_SERVE
    sp.what_you_do = VADIM_WHAT_YOU_DO
    sp.niche = VADIM_NICHE
    sp.signature_programs = (
        'Next Level DeFi\nDFY DeFi Setup Experience\nDeFi Training Workshop'
    )
    sp.seeking = VADIM_SEEKING
    sp.audience_type = 'Non-technical investors & beginners'

    # Verify company is set
    if not sp.company or sp.company == sp.name:
        sp.company = 'Next Level DeFi'

    # Null out embeddings so backfill_embeddings will regenerate them
    sp.embeddings_updated_at = None

    # Mark fields with client_ingest provenance
    meta = sp.enrichment_metadata or {}
    field_meta = meta.get('field_meta', {})
    now = timezone.now().isoformat()
    for field in ['bio', 'offering', 'who_you_serve', 'what_you_do', 'niche',
                  'signature_programs', 'seeking', 'audience_type', 'company']:
        field_meta[field] = {
            'source': 'client_ingest',
            'updated_at': now,
            'confirmed': True,
        }
    meta['field_meta'] = field_meta
    sp.enrichment_metadata = meta
    sp.save()
    print(f'Updated Supabase profile: {sp.name} (id={sp.id})')
    print(f'  who_you_serve: {sp.who_you_serve[:60]}...')
    print(f'  what_you_do: {sp.what_you_do[:60]}...')
    print(f'  niche: {sp.niche}')
    print(f'  embeddings_updated_at: nulled (will re-embed on next backfill)')


def main():
    parser = argparse.ArgumentParser(
        description='Export Vadim Voss profile with JV Brief data'
    )
    parser.add_argument(
        '--output', required=True,
        help='Output HTML file path',
    )
    parser.add_argument(
        '--update-db', action='store_true',
        help='Also update Supabase profile fields',
    )
    args = parser.parse_args()

    sp = SupabaseProfile.objects.filter(name__icontains='Vadim Voss').first()
    if not sp:
        print('No Vadim Voss profile found in Supabase')
        sys.exit(1)

    print(f'Found profile: {sp.name} (id={sp.id})')
    print(f'  Company: {sp.company}')
    print(f'  Email: {sp.email}')

    if args.update_db:
        update_supabase_profile(sp)

    export_profile(sp, args.output, context_overrides=VADIM_CONTEXT_OVERRIDES)


if __name__ == '__main__':
    main()
