#!/usr/bin/env python3
"""Generate a 3-page shareable partner report (index, outreach, profile).

Usage:
    python scripts/generate_partner_page.py "David Riklan"
    python scripts/generate_partner_page.py --id 4a5f9f31-78a0-4205-b75d-2cd604413eac
    python scripts/generate_partner_page.py "David Riklan" --top 15

Output:
    pages/<slug>/index.html     — Hub page
    pages/<slug>/outreach.html  — Interactive partner outreach list
    pages/<slug>/profile.html   — Client profile page
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_list_size(size) -> str:
    if not size:
        return ''
    size = int(size)
    if size >= 1_000_000:
        return f'{size / 1_000_000:.1f}M'
    if size >= 1_000:
        return f'{size / 1_000:.0f}K'
    return str(size)


def _initials(name: str) -> str:
    return ''.join(w[0].upper() for w in name.split()[:2]) if name else '??'


def _slug(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')


def _esc(value) -> str:
    if value is None:
        return ''
    return html.escape(str(value))


def _score_tier(score: float) -> tuple[str, str]:
    if score >= 70:
        return 'Excellent', 'tag-priority'
    if score >= 60:
        return 'Strong', 'tag-fit'
    if score >= 50:
        return 'Good', 'tag-fit'
    return 'Potential', 'tag-fit'


def _clean_url(url: str) -> str:
    if not url:
        return ''
    return re.sub(r'^https?://(www\.)?', '', url).rstrip('/')


def _parse_tags(tags) -> list[str]:
    if not tags:
        return []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            return [tags]
    if isinstance(tags, list):
        return [str(t) for t in tags[:8]]
    return []


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_profile(conn, name: str | None = None, profile_id: str | None = None) -> dict:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if profile_id:
        cur.execute("SELECT * FROM profiles WHERE id = %s::uuid", (profile_id,))
    else:
        cur.execute("SELECT * FROM profiles WHERE name ILIKE %s LIMIT 1", (name,))
    row = cur.fetchone()
    if not row:
        sys.exit(f"Profile not found: {name or profile_id}")
    return dict(row)


def fetch_matches(conn, profile_id: str, top: int = 10) -> list[dict]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT ms.harmonic_mean, ms.score_ab, ms.score_ba, ms.match_reason,
               ms.match_context,
               p.id, p.name, p.company, p.email, p.website, p.linkedin,
               p.niche, p.list_size, p.audience_type, p.what_you_do,
               p.who_you_serve, p.seeking, p.offering, p.tags, p.bio,
               p.facebook, p.instagram, p.youtube, p.twitter,
               p.social_reach, p.phone
        FROM match_suggestions ms
        JOIN profiles p ON p.id = ms.suggested_profile_id
        WHERE ms.profile_id = %s::uuid
          AND ms.harmonic_mean IS NOT NULL
          AND (p.email IS NOT NULL OR p.linkedin IS NOT NULL OR p.website IS NOT NULL)
        ORDER BY ms.harmonic_mean DESC
        LIMIT %s
    """, (profile_id, top))
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Page 1: index.html — Hub
# ---------------------------------------------------------------------------

def generate_index(profile: dict) -> str:
    name = _esc(profile.get('name', ''))
    company = _esc(profile.get('company') or name)
    generated_at = datetime.now().strftime('%B %Y')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{company} &middot; Partner Report</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=Playfair+Display:wght@500;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --cream: #faf8f5; --ink: #1a1a1a; --forest: #1e3a2f; --gold: #c9a962; --gold-light: #e8d5a8; --muted: #8b8680; --blush: #f4e8e1; --card: #ffffff; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'DM Sans', -apple-system, sans-serif; background: var(--cream); color: var(--ink); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }}
        .container {{ max-width: 400px; width: 100%; }}
        h1 {{ font-family: 'Playfair Display', Georgia, serif; font-size: 28px; font-weight: 500; color: var(--forest); text-align: center; margin-bottom: 8px; }}
        .subtitle {{ text-align: center; color: var(--muted); font-size: 14px; margin-bottom: 32px; }}
        .nav-list {{ list-style: none; }}
        .nav-card {{ background: var(--card); border-radius: 8px; padding: 20px 24px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); transition: box-shadow 0.2s, transform 0.2s; }}
        .nav-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.08); transform: translateY(-2px); }}
        .nav-card a {{ text-decoration: none; color: inherit; display: block; }}
        .nav-card .name {{ font-size: 18px; font-weight: 600; color: var(--forest); margin-bottom: 4px; }}
        .nav-card .desc {{ font-size: 14px; color: var(--muted); }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{company}</h1>
        <p class="subtitle">{name} &middot; {generated_at} report</p>
        <ul class="nav-list">
            <li class="nav-card">
                <a href="outreach.html">
                    <div class="name">Partner Outreach</div>
                    <div class="desc">Top-ranked partners matched to your goals</div>
                </a>
            </li>
            <li class="nav-card">
                <a href="profile.html">
                    <div class="name">Client Profile</div>
                    <div class="desc">Your offer, audience, and partnership goals</div>
                </a>
            </li>
        </ul>
    </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Page 2: outreach.html — Interactive partner outreach
# ---------------------------------------------------------------------------

def _render_outreach_card(match: dict, idx: int, section_key: str) -> str:
    score = match.get('harmonic_mean', 0)
    name = _esc(match.get('name', ''))
    company = _esc(match.get('company') or '')
    what = _esc(match.get('what_you_do') or '')
    email = match.get('email') or ''
    website = match.get('website') or ''
    linkedin = match.get('linkedin') or ''
    phone = match.get('phone') or ''
    list_size = _format_list_size(match.get('list_size'))
    niche = match.get('niche') or ''
    reason = _esc(match.get('match_reason') or '')
    who = _esc(match.get('who_you_serve') or '')
    offering = _esc(match.get('offering') or '')
    tags_list = _parse_tags(match.get('tags'))

    company_line = company
    if what:
        company_line += f' &middot; {what[:120]}'
    elif niche:
        company_line += f' &middot; {_esc(niche)}'

    tags_html = ''
    tier_label, tier_class = _score_tier(score)
    tags_html += f'<span class="tag {tier_class}">{tier_label} ({score:.0f})</span>'
    if list_size:
        tags_html += f'<span class="tag tag-fit">{list_size} List</span>'
    for t in tags_list[:3]:
        tags_html += f'<span class="tag tag-fit">{_esc(t.title())}</span>'

    action_html = ''
    if list_size:
        action_html += f'<span class="card-reach">{list_size}</span>'
    if email:
        action_html += (
            f'<a href="mailto:{_esc(email)}" class="action-btn" '
            f'onclick="event.stopPropagation()" title="Email {name}">'
            f'<svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">'
            f'<path d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>'
            f'</svg></a>'
        )
    elif linkedin:
        action_html += (
            f'<a href="{_esc(linkedin)}" target="_blank" rel="noopener" '
            f'class="action-btn linkedin" onclick="event.stopPropagation()" title="LinkedIn">'
            f'<svg fill="currentColor" viewBox="0 0 24 24"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>'
            f'</a>'
        )

    detail_rows = ''
    if email:
        detail_rows += f'<div class="detail-row"><span class="detail-label">Email</span><span class="detail-value"><a href="mailto:{_esc(email)}">{_esc(email)}</a></span></div>'

    audience_parts = []
    if list_size:
        audience_parts.append(f'{_format_list_size(match.get("list_size"))} subscribers.')
    if who:
        audience_parts.append(who)
    if offering:
        audience_parts.append(f'Offering: {offering}')
    if audience_parts:
        detail_rows += f'<div class="detail-row"><span class="detail-label">Audience</span><span class="detail-value">{" ".join(audience_parts)}</span></div>'

    if reason:
        detail_rows += f'<div class="detail-row"><span class="detail-label">Why fit</span><span class="detail-value">{reason}</span></div>'

    if website:
        detail_rows += f'<div class="detail-row"><span class="detail-label">Website</span><span class="detail-value"><a href="{_esc(website)}" target="_blank" rel="noopener">{_esc(website)}</a></span></div>'

    if phone:
        detail_rows += f'<div class="detail-row"><span class="detail-label">Phone</span><span class="detail-value">{_esc(phone)}</span></div>'

    if linkedin:
        detail_rows += f'<div class="detail-row"><span class="detail-label">LinkedIn</span><span class="detail-value"><a href="{_esc(linkedin)}" target="_blank" rel="noopener">View Profile</a></span></div>'

    detail_note = ''
    bio = match.get('bio') or ''
    if bio and len(bio) > 20:
        detail_note = f'<div class="detail-note">{_esc(bio[:300])}</div>'

    return f"""
            <div class="card" data-partner-id="{idx}" data-section-key="{section_key}" data-match-score="{score:.1f}" data-partner-name="{name}">
                <div class="card-inner" onclick="toggleExpand(this.parentElement)">
                    <div class="card-check" onclick="event.stopPropagation(); toggleCard(this.closest('.card'))">
                        <div class="checkbox"></div>
                    </div>
                    <div class="card-content">
                        <div class="card-name">{name}</div>
                        <div class="card-company">{company_line}</div>
                        <div class="card-tags">{tags_html}</div>
                        <div class="expand-hint">tap for details</div>
                    </div>
                    <div class="card-action">{action_html}</div>
                </div>
                <div class="card-details">
                    <div class="card-details-inner">
                        {detail_rows}
                        {detail_note}
                    </div>
                </div>
            </div>"""


def generate_outreach(profile: dict, matches: list[dict]) -> str:
    name = _esc(profile.get('name', ''))
    company = _esc(profile.get('company') or name)

    excellent = [m for m in matches if (m.get('harmonic_mean') or 0) >= 70]
    strong = [m for m in matches if 60 <= (m.get('harmonic_mean') or 0) < 70]
    good = [m for m in matches if (m.get('harmonic_mean') or 0) < 60]

    sections_html = ''
    card_idx = 0

    if excellent:
        cards = ''
        for m in excellent:
            cards += _render_outreach_card(m, card_idx, 'priority')
            card_idx += 1
        sections_html += f"""
        <section class="section" data-section-key="priority">
            <div class="section-header">
                <span class="section-title">Priority Contacts</span>
                <span class="section-count">{len(excellent)}</span>
                <span class="section-note">Excellent match &mdash; reach out this week</span>
            </div>
            {cards}
        </section>"""

    if strong:
        cards = ''
        for m in strong:
            cards += _render_outreach_card(m, card_idx, 'strong')
            card_idx += 1
        sections_html += f"""
        <section class="section" data-section-key="strong">
            <div class="section-header">
                <span class="section-title">Strong Matches</span>
                <span class="section-count">{len(strong)}</span>
                <span class="section-note">High potential &mdash; worth reaching out</span>
            </div>
            {cards}
        </section>"""

    if good:
        cards = ''
        for m in good:
            cards += _render_outreach_card(m, card_idx, 'good')
            card_idx += 1
        sections_html += f"""
        <section class="section" data-section-key="good">
            <div class="section-header">
                <span class="section-title">Good Fit</span>
                <span class="section-count">{len(good)}</span>
                <span class="section-note">Solid alignment &mdash; follow up as capacity allows</span>
            </div>
            {cards}
        </section>"""

    total_cards = len(matches)
    storage_key = f'report_outreach_{_slug(profile.get("name", ""))}'

    templates_json = json.dumps({
        "initial": {
            "text": f"Hi [Partner Name],\n\nI came across your work and love what you're doing for [their audience]. I'm {profile.get('name', '')} with {profile.get('company', '')}, and I think our audiences could really benefit from knowing about each other.\n\nWould you be open to a quick call to explore some partnership ideas?\n\nBest,\n{profile.get('name', '')}",
            "title": "Initial Outreach"
        },
        "followup": {
            "text": f"Hi [Partner Name],\n\nJust following up on my earlier message. I'd love to connect and explore how we might support each other's communities.\n\nNo pressure at all \u2014 just thought there might be a great fit.\n\nWarmly,\n{profile.get('name', '')}",
            "title": "Follow-Up"
        }
    })

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{company} &middot; Partner Outreach</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=Playfair+Display:wght@500;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --cream: #faf8f5; --ink: #1a1a1a; --forest: #1e3a2f; --gold: #c9a962; --gold-light: #e8d5a8; --muted: #8b8680; --blush: #f4e8e1; --card: #ffffff; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'DM Sans', -apple-system, sans-serif; background: var(--cream); color: var(--ink); line-height: 1.5; -webkit-font-smoothing: antialiased; min-height: 100vh; }}
        body::before {{ content: ""; position: fixed; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; opacity: 0.03; background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.8' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E"); }}
        .container {{ max-width: 820px; margin: 0 auto; padding: 24px 20px 120px; position: relative; }}
        .hub-link {{ margin-bottom: 16px; font-size: 13px; }}
        .hub-link a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .header {{ text-align: center; padding: 32px 0 24px; border-bottom: 1px solid var(--gold-light); margin-bottom: 32px; }}
        .header-eyebrow {{ font-size: 11px; letter-spacing: 0.15em; text-transform: uppercase; color: var(--muted); margin-bottom: 8px; }}
        .header h1 {{ font-family: 'Playfair Display', Georgia, serif; font-size: 28px; font-weight: 500; color: var(--forest); letter-spacing: -0.02em; }}
        .header-meta {{ margin-top: 12px; font-size: 13px; color: var(--muted); }}
        .header-meta a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .progress {{ text-align: center; padding: 28px 24px; background: var(--card); border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); margin-bottom: 32px; position: relative; }}
        .progress::before {{ content: ""; position: absolute; top: 0; left: 50%; transform: translateX(-50%); width: 40px; height: 3px; background: var(--gold); border-radius: 0 0 2px 2px; }}
        .progress-number {{ font-family: 'Playfair Display', Georgia, serif; font-size: 48px; font-weight: 500; color: var(--forest); line-height: 1; }}
        .progress-number span {{ color: var(--muted); font-size: 32px; }}
        .progress-label {{ font-size: 13px; color: var(--muted); margin-top: 8px; letter-spacing: 0.02em; }}
        .progress-bar {{ height: 3px; background: var(--blush); border-radius: 2px; margin-top: 20px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, var(--forest), var(--gold)); border-radius: 2px; transition: width 0.4s ease; }}
        .reset-btn {{ margin-top: 16px; padding: 8px 16px; background: transparent; border: 1px solid var(--gold-light); border-radius: 4px; font-family: 'DM Sans', sans-serif; font-size: 11px; color: var(--muted); cursor: pointer; transition: all 0.15s; }}
        .reset-btn:hover {{ background: var(--blush); color: var(--forest); border-color: var(--gold); }}
        .section {{ margin-bottom: 36px; }}
        .section-header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid var(--gold-light); }}
        .section-title {{ font-family: 'Playfair Display', Georgia, serif; font-size: 18px; font-weight: 500; color: var(--forest); }}
        .section-count {{ font-size: 12px; color: var(--gold); font-weight: 600; }}
        .section-note {{ font-size: 11px; color: var(--muted); margin-left: auto; font-style: italic; }}
        .card {{ background: var(--card); border-radius: 8px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); overflow: hidden; transition: box-shadow 0.2s; }}
        .card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
        .card.done {{ opacity: 0.5; }}
        .card.done .card-name {{ text-decoration: line-through; text-decoration-color: var(--muted); }}
        .card-inner {{ display: flex; align-items: stretch; }}
        .card-check {{ width: 52px; display: flex; align-items: center; justify-content: center; cursor: pointer; border-right: 1px solid rgba(0,0,0,0.05); flex-shrink: 0; transition: background 0.15s; }}
        .card-check:hover {{ background: var(--blush); }}
        .checkbox {{ width: 22px; height: 22px; border: 2px solid var(--gold-light); border-radius: 50%; transition: all 0.2s; display: flex; align-items: center; justify-content: center; }}
        .card.done .checkbox {{ background: var(--forest); border-color: var(--forest); }}
        .card.done .checkbox::after {{ content: "\\2713"; color: white; font-size: 12px; font-weight: 600; }}
        .card-content {{ flex: 1; padding: 16px 20px; min-width: 0; }}
        .card-name {{ font-size: 16px; font-weight: 600; color: var(--ink); margin-bottom: 4px; }}
        .card-company {{ font-size: 13px; color: var(--muted); }}
        .card-tags {{ display: flex; gap: 6px; margin-top: 10px; flex-wrap: wrap; }}
        .tag {{ font-size: 10px; font-weight: 600; padding: 3px 8px; border-radius: 3px; letter-spacing: 0.03em; text-transform: uppercase; }}
        .tag-priority {{ background: var(--forest); color: white; }}
        .tag-fit {{ background: var(--blush); color: var(--forest); }}
        .tag-warn {{ background: #fef3c7; color: #92400e; }}
        .expand-hint {{ font-size: 10px; color: var(--gold); margin-top: 6px; opacity: 0.8; }}
        .card.expanded .expand-hint {{ display: none; }}
        .card-action {{ display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 12px 16px; gap: 6px; }}
        .card-reach {{ font-size: 11px; color: var(--muted); font-weight: 500; }}
        .action-btn {{ display: flex; align-items: center; justify-content: center; width: 44px; height: 44px; background: var(--forest); color: white; border-radius: 8px; text-decoration: none; transition: transform 0.15s, background 0.15s; }}
        .action-btn:hover {{ transform: scale(1.08); background: var(--gold); }}
        .action-btn:active {{ transform: scale(0.98); }}
        .action-btn svg {{ width: 20px; height: 20px; }}
        .action-btn.linkedin {{ background: #0077b5; }}
        .action-btn.linkedin:hover {{ background: #005885; }}
        .card-details {{ display: none; padding: 0 20px 16px 72px; font-size: 13px; color: var(--muted); border-top: 1px solid var(--blush); }}
        .card.expanded .card-details {{ display: block; }}
        .card-details-inner {{ padding-top: 12px; }}
        .detail-row {{ display: flex; gap: 8px; margin-bottom: 6px; }}
        .detail-label {{ font-weight: 600; color: var(--forest); min-width: 60px; flex-shrink: 0; }}
        .detail-value {{ color: var(--ink); }}
        .detail-value a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .detail-note {{ font-style: italic; color: var(--muted); margin-top: 8px; padding-top: 8px; border-top: 1px dashed var(--gold-light); }}
        .footer-note {{ text-align: center; padding: 24px 0 0; font-size: 12px; color: var(--muted); border-top: 1px solid var(--gold-light); margin-top: 8px; }}
        .footer-note a {{ color: var(--forest); }}
        .template-bar {{ position: fixed; bottom: 0; left: 0; right: 0; background: var(--card); border-top: 2px solid var(--gold); padding: 16px 20px; z-index: 100; }}
        .template-bar-inner {{ max-width: 820px; margin: 0 auto; display: flex; gap: 12px; }}
        .template-btn {{ flex: 1; padding: 14px 16px; background: var(--cream); border: 1px solid var(--gold-light); border-radius: 6px; font-family: 'DM Sans', sans-serif; font-size: 13px; font-weight: 600; color: var(--forest); cursor: pointer; transition: all 0.15s; }}
        .template-btn:hover {{ background: var(--blush); border-color: var(--gold); }}
        .modal-overlay {{ display: none; position: fixed; inset: 0; background: rgba(30, 58, 47, 0.6); backdrop-filter: blur(4px); z-index: 200; align-items: flex-end; justify-content: center; }}
        .modal-overlay.open {{ display: flex; }}
        .modal {{ background: var(--card); width: 100%; max-width: 820px; max-height: 85vh; border-radius: 16px 16px 0 0; overflow: hidden; animation: slideUp 0.25s ease; }}
        @keyframes slideUp {{ from {{ transform: translateY(100%); opacity: 0.8; }} to {{ transform: translateY(0); opacity: 1; }} }}
        .modal-header {{ display: flex; justify-content: space-between; align-items: center; padding: 20px 24px; border-bottom: 1px solid var(--gold-light); }}
        .modal-title {{ font-family: 'Playfair Display', Georgia, serif; font-size: 18px; font-weight: 500; color: var(--forest); }}
        .modal-close {{ width: 36px; height: 36px; border: none; background: var(--cream); border-radius: 50%; font-size: 20px; color: var(--muted); cursor: pointer; display: flex; align-items: center; justify-content: center; transition: background 0.15s; }}
        .modal-close:hover {{ background: var(--blush); }}
        .modal-body {{ padding: 24px; overflow-y: auto; max-height: 55vh; }}
        .template-text {{ font-size: 14px; line-height: 1.7; white-space: pre-wrap; background: var(--cream); padding: 20px; border-radius: 8px; border: 1px solid var(--gold-light); color: var(--ink); }}
        .modal-action {{ padding: 20px 24px; border-top: 1px solid var(--gold-light); }}
        .copy-btn {{ width: 100%; padding: 16px; background: var(--forest); color: white; border: none; border-radius: 8px; font-family: 'DM Sans', sans-serif; font-size: 15px; font-weight: 600; cursor: pointer; transition: background 0.15s; }}
        .copy-btn:hover {{ background: var(--gold); color: var(--ink); }}
        @media (max-width: 480px) {{ .container {{ padding: 16px 16px 120px; }} .header h1 {{ font-size: 24px; }} .progress-number {{ font-size: 40px; }} }}
    </style>
</head>
<body>
    <div class="container">
        <p class="hub-link"><a href="index.html">&larr; Partner report home</a></p>
        <header class="header">
            <div class="header-eyebrow">Partner Outreach</div>
            <h1>{company}</h1>
            <div class="header-meta">{name} &middot; <a href="profile.html">View Profile</a></div>
        </header>
        <div class="progress">
            <div class="progress-number"><span id="doneCount">0</span><span>/</span><span id="totalCount">{total_cards}</span></div>
            <div class="progress-label">partners contacted</div>
            <div class="progress-bar"><div class="progress-fill" id="progressFill" style="width: 0%"></div></div>
            <button class="reset-btn" type="button" onclick="resetProgress()">Reset Progress</button>
        </div>
        {sections_html}
        <div class="footer-note">Report generated for {name}.</div>
    </div>
    <div class="template-bar">
        <div class="template-bar-inner">
            <button class="template-btn" type="button" onclick="showTemplate('initial')">Copy Outreach Email</button>
            <button class="template-btn" type="button" onclick="showTemplate('followup')">Copy Follow-up</button>
        </div>
    </div>
    <div class="modal-overlay" id="modal" onclick="closeModal()">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <div class="modal-title" id="modalTitle">Email Template</div>
                <button class="modal-close" type="button" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body"><div class="template-text" id="modalContent"></div></div>
            <div class="modal-action"><button class="copy-btn" type="button" id="copyBtn" onclick="copyAndClose()">Copy to Clipboard</button></div>
        </div>
    </div>
    <script>
        var templates = {templates_json};
        var STORAGE_KEY = '{storage_key}';
        var activeTemplateId = null;
        function toggleCard(card) {{ card.classList.toggle('done'); updateProgress(); saveState(); }}
        function toggleExpand(card) {{ card.classList.toggle('expanded'); }}
        function updateProgress() {{
            var total = document.querySelectorAll('.card').length;
            var done = document.querySelectorAll('.card.done').length;
            document.getElementById('doneCount').textContent = done;
            document.getElementById('totalCount').textContent = total;
            document.getElementById('progressFill').style.width = (total ? (done / total * 100) : 0) + '%';
        }}
        function saveState() {{
            var state = [];
            document.querySelectorAll('.card').forEach(function(card, i) {{ if (card.classList.contains('done')) state.push(i); }});
            localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
        }}
        function loadState() {{
            var saved = localStorage.getItem(STORAGE_KEY);
            if (saved) {{ try {{ var state = JSON.parse(saved); var cards = document.querySelectorAll('.card'); state.forEach(function(i) {{ if (cards[i]) cards[i].classList.add('done'); }}); }} catch(e) {{}} }}
            updateProgress();
        }}
        function resetProgress() {{
            if (confirm('Clear all progress? This cannot be undone.')) {{
                localStorage.removeItem(STORAGE_KEY);
                document.querySelectorAll('.card.done').forEach(function(card) {{ card.classList.remove('done'); }});
                updateProgress();
            }}
        }}
        function showTemplate(id) {{
            activeTemplateId = id;
            var tmpl = templates[id]; if (!tmpl) return;
            document.getElementById('modalTitle').textContent = tmpl.title || 'Email Template';
            document.getElementById('modalContent').textContent = tmpl.text || '';
            document.getElementById('modal').classList.add('open');
        }}
        function closeModal() {{
            document.getElementById('modal').classList.remove('open');
            var btn = document.getElementById('copyBtn'); btn.textContent = 'Copy to Clipboard'; btn.style.background = ''; btn.style.color = '';
        }}
        function copyAndClose() {{
            var tmpl = templates[activeTemplateId]; if (!tmpl) return;
            navigator.clipboard.writeText(tmpl.text || '').then(function() {{
                var btn = document.getElementById('copyBtn'); btn.textContent = 'Copied!'; btn.style.background = 'var(--gold)'; btn.style.color = 'var(--ink)';
                setTimeout(function() {{ closeModal(); }}, 800);
            }});
        }}
        document.addEventListener('keydown', function(e) {{ if (e.key === 'Escape') closeModal(); }});
        loadState();
    </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Page 3: profile.html — Client profile
# ---------------------------------------------------------------------------

def _render_offering_list(profile: dict) -> str:
    offering = profile.get('offering') or ''
    if not offering:
        return ''
    items = [o.strip() for o in offering.split(',') if o.strip()]
    if not items:
        return ''
    li = ''.join(f'<li>{_esc(item)}</li>' for item in items)
    return f'<div class="text-block" style="margin-top: 1.25rem; padding-top: 1rem; border-top: 1px solid #f0f0f0;"><p><strong>What You Get:</strong></p><ul>{li}</ul></div>'


def generate_profile(profile: dict) -> str:
    name = _esc(profile.get('name', ''))
    company = _esc(profile.get('company') or '')
    niche = _esc(profile.get('niche') or '')
    bio = _esc(profile.get('bio') or '')
    what_you_do = _esc(profile.get('what_you_do') or '')
    who_you_serve = _esc(profile.get('who_you_serve') or '')
    seeking = _esc(profile.get('seeking') or '')
    offering = _esc(profile.get('offering') or '')
    email = profile.get('email') or ''
    website = profile.get('website') or ''
    linkedin = profile.get('linkedin') or ''
    list_size = _format_list_size(profile.get('list_size'))
    social_reach = _format_list_size(profile.get('social_reach'))
    tags = _parse_tags(profile.get('tags'))

    info_items = ''
    if profile.get('what_you_do'):
        info_items += f'<div class="info-item"><label>Current Focus</label><div class="value">{what_you_do[:150]}</div></div>'
    if niche:
        info_items += f'<div class="info-item"><label>Niche</label><div class="value highlight">{niche}</div></div>'
    if who_you_serve:
        info_items += f'<div class="info-item"><label>Target Audience</label><div class="value">{who_you_serve}</div></div>'
    if list_size:
        info_items += f'<div class="info-item"><label>Network Reach</label><div class="value">{list_size}</div><div class="sub">{name}&#x27;s subscriber network</div></div>'
    info_grid_html = f'<div class="info-grid">{info_items}</div>' if info_items else ''

    website_html = ''
    if website:
        website_html = f'<div style="margin-top: 1.25rem; padding-top: 1rem; border-top: 1px solid #f0f0f0;"><div class="info-item"><label>Main Website</label><a href="{_esc(website)}" target="_blank" rel="noopener">{_esc(website)}</a></div></div>'

    stat_boxes = ''
    if list_size:
        stat_boxes += f'<div class="commission-box highlight"><div class="commission-value">{list_size}</div><div class="commission-label">Email List</div></div>'
    if social_reach:
        stat_boxes += f'<div class="commission-box"><div class="commission-value">{social_reach}</div><div class="commission-label">Social Reach</div></div>'
    engagement = profile.get('audience_engagement_score')
    if engagement:
        stat_boxes += f'<div class="commission-box"><div class="commission-value">{engagement:.0%}</div><div class="commission-label">Engagement</div></div>'

    stats_html = ''
    if stat_boxes:
        stats_html = f"""
            <div class="section">
                <div class="section-header"><span class="section-title">What {name} Offers Partners</span></div>
                <div class="section-body"><div class="commission-grid">{stat_boxes}</div>{_render_offering_list(profile)}</div>
            </div>"""

    about_html = ''
    if bio:
        credentials = ''
        if offering:
            creds = [o.strip() for o in (profile.get('offering') or '').split(',') if o.strip()]
            if creds:
                cred_items = ''.join(f'<li><strong>{_esc(c)}</strong></li>' for c in creds[:6])
                credentials = f'<p><strong>Credentials / Core Offers:</strong></p><ul>{cred_items}</ul>'
        about_html = f"""
            <div class="section">
                <div class="section-header"><span class="section-title">About {name}</span></div>
                <div class="section-body text-block"><p>{bio}</p>{credentials}</div>
            </div>"""

    resource_links = ''
    if website:
        resource_links += f'<li><a href="{_esc(website)}" target="_blank" rel="noopener">{_esc(_clean_url(website))}</a> &mdash; Main website</li>'
    if linkedin:
        resource_links += f'<li><a href="{_esc(linkedin)}" target="_blank" rel="noopener">LinkedIn Profile</a></li>'
    for platform, label in [('youtube', 'YouTube'), ('facebook', 'Facebook'), ('instagram', 'Instagram'), ('twitter', 'Twitter/X')]:
        url = profile.get(platform) or ''
        if url:
            resource_links += f'<li><a href="{_esc(url)}" target="_blank" rel="noopener">{label}</a></li>'
    resources_html = f'<div class="section"><div class="section-header"><span class="section-title">Resources &amp; Links</span></div><div class="section-body text-block"><ul>{resource_links}</ul></div></div>' if resource_links else ''

    ideal_html = ''
    if who_you_serve:
        ideal_html = f'<div class="section"><div class="section-header"><span class="section-title">Ideal Partner Profile</span></div><div class="section-body text-block"><p>Partners serving <strong>{who_you_serve}</strong>.</p>{f"<p style=color:var(--muted)>{what_you_do}</p>" if what_you_do else ""}</div></div>'

    seeking_html = ''
    if seeking:
        seeking_items = ''.join(f'<li>{_esc(s.strip())}</li>' for s in (profile.get('seeking') or '').split(';') if s.strip())
        if not seeking_items:
            seeking_items = f'<li>{seeking}</li>'
        seeking_html = f'<div class="section"><div class="section-header"><span class="section-title">What {name} Is Seeking</span></div><div class="section-body text-block"><p><strong>JV Partnership Goals:</strong></p><ul>{seeking_items}</ul></div></div>'

    tags_html = ''
    if tags:
        tag_items = ''.join(f'<li>{_esc(t)}</li>' for t in tags)
        tags_html = f'<div class="section"><div class="section-header"><span class="section-title">Areas of Expertise</span></div><div class="section-body text-block"><ul>{tag_items}</ul></div></div>'

    contact_html = ''
    if email:
        contact_html = f'<div class="contact-footer"><div>Questions or updates?</div><a href="mailto:{_esc(email)}">{_esc(email)}</a></div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{_esc(profile.get('company') or name)} &middot; Client Profile</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=Playfair+Display:wght@500;600&display=swap" rel="stylesheet">
    <style>
        :root {{ --cream: #faf8f5; --ink: #1a1a1a; --forest: #1e3a2f; --gold: #c9a962; --gold-light: #e8d5a8; --muted: #8b8680; --blush: #f4e8e1; --card: #ffffff; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'DM Sans', -apple-system, sans-serif; background: var(--cream); color: var(--ink); line-height: 1.5; -webkit-font-smoothing: antialiased; min-height: 100vh; }}
        body::before {{ content: ""; position: fixed; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; opacity: 0.03; background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.8' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E"); }}
        .container {{ max-width: 820px; margin: 0 auto; padding: 24px 20px 80px; position: relative; }}
        .hub-link {{ margin-bottom: 16px; font-size: 13px; }}
        .hub-link a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .header {{ text-align: center; padding: 32px 0 24px; border-bottom: 1px solid var(--gold-light); margin-bottom: 32px; }}
        .header-eyebrow {{ font-size: 11px; letter-spacing: 0.15em; text-transform: uppercase; color: var(--muted); margin-bottom: 8px; }}
        .header h1 {{ font-family: 'Playfair Display', Georgia, serif; font-size: 28px; font-weight: 500; color: var(--forest); letter-spacing: -0.02em; }}
        .header-meta {{ margin-top: 12px; font-size: 13px; color: var(--muted); }}
        .header-meta a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .main {{ margin-bottom: 2rem; }}
        .client-card {{ background: var(--card); border-radius: 8px; padding: 1.25rem 1.5rem; margin-bottom: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.04); display: flex; gap: 1rem; align-items: flex-start; }}
        .client-avatar {{ width: 56px; height: 56px; background: var(--forest); border-radius: 8px; display: flex; align-items: center; justify-content: center; color: var(--gold); font-weight: 600; font-size: 1.25rem; flex-shrink: 0; }}
        .client-info h2 {{ font-family: 'Playfair Display', Georgia, serif; font-size: 1.125rem; font-weight: 500; color: var(--forest); }}
        .client-company {{ color: var(--muted); font-size: 0.875rem; }}
        .client-title {{ color: var(--ink); font-size: 0.8125rem; margin-top: 0.25rem; }}
        .section {{ background: var(--card); border-radius: 8px; margin-bottom: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.04); overflow: hidden; }}
        .section-header {{ padding: 0.875rem 1.25rem; border-bottom: 1px solid var(--gold-light); background: var(--cream); }}
        .section-title {{ font-family: 'Playfair Display', Georgia, serif; font-size: 16px; font-weight: 500; color: var(--forest); }}
        .section-body {{ padding: 1.25rem; }}
        .info-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
        @media (max-width: 480px) {{ .info-grid {{ grid-template-columns: 1fr; }} }}
        .info-item label {{ display: block; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin-bottom: 0.25rem; }}
        .info-item .value {{ font-weight: 600; font-size: 0.9375rem; color: var(--ink); }}
        .info-item .sub {{ font-size: 0.75rem; color: var(--muted); }}
        .info-item .highlight {{ color: var(--gold); }}
        .info-item a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .commission-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 0.75rem; margin-bottom: 1.25rem; }}
        .commission-box {{ text-align: center; padding: 1rem; background: var(--blush); border-radius: 6px; }}
        .commission-box.highlight {{ background: var(--forest); }}
        .commission-value {{ font-size: 1.25rem; font-weight: 700; color: var(--ink); }}
        .commission-box.highlight .commission-value {{ color: var(--gold); }}
        .commission-label {{ font-size: 0.6875rem; color: var(--muted); margin-top: 0.25rem; }}
        .commission-box.highlight .commission-label {{ color: var(--gold-light); }}
        .text-block {{ font-size: 0.875rem; color: var(--ink); }}
        .text-block p {{ margin-bottom: 1rem; }}
        .text-block p:last-child {{ margin-bottom: 0; }}
        .text-block strong {{ color: var(--forest); }}
        .text-block ul {{ list-style: none; margin: 0.5rem 0; }}
        .text-block li {{ padding: 0.25rem 0; padding-left: 1rem; position: relative; }}
        .text-block li::before {{ content: "\\2022"; position: absolute; left: 0; color: var(--gold); }}
        .contact-footer {{ background: var(--card); border-radius: 8px; padding: 1.25rem; text-align: center; font-size: 0.8125rem; color: var(--muted); margin-bottom: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
        .contact-footer a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
        .footer {{ border-top: 1px solid var(--gold-light); padding: 1.5rem; text-align: center; font-size: 0.75rem; color: var(--muted); }}
        .footer a {{ color: var(--forest); text-decoration: underline; text-underline-offset: 2px; }}
    </style>
</head>
<body>
    <div class="container">
        <p class="hub-link"><a href="index.html">&larr; Partner report home</a></p>
        <header class="header">
            <div class="header-eyebrow">Client Profile</div>
            <h1>{name}</h1>
            <div class="header-meta">{name}</div>
        </header>
        <main class="main">
            <div class="client-card">
                <div class="client-avatar">{_initials(profile.get('name', ''))}</div>
                <div class="client-info">
                    <h2>{name}</h2>
                    <div class="client-company">{company}</div>
                    {f'<div class="client-title">{niche}</div>' if niche else ''}
                </div>
            </div>
            <div class="section">
                <div class="section-header"><span class="section-title">The Profile</span></div>
                <div class="section-body">{info_grid_html}{website_html}</div>
            </div>
            {stats_html}
            {about_html}
            {resources_html}
            {ideal_html}
            {seeking_html}
            {tags_html}
            {contact_html}
        </main>
        <div class="footer">
            {f'{company} &middot; ' if company else ''}<a href="index.html">Partner report</a> &middot; Client Profile{f' &middot; <a href="{_esc(website)}" target="_blank" rel="noopener">{_esc(_clean_url(website))}</a>' if website else ''}
        </div>
    </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Generate a 3-page shareable partner report')
    parser.add_argument('name', nargs='?', help='Partner name to look up')
    parser.add_argument('--id', dest='profile_id', help='Profile UUID')
    parser.add_argument('--top', type=int, default=10, help='Number of top matches to show')
    parser.add_argument('--output-dir', help='Output directory (default: pages/<slug>/)')
    args = parser.parse_args()

    if not args.name and not args.profile_id:
        parser.error('Provide either a name or --id')

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        sys.exit('ERROR: DATABASE_URL not set')

    conn = psycopg2.connect(db_url)
    try:
        profile = fetch_profile(conn, name=args.name, profile_id=args.profile_id)
        matches = fetch_matches(conn, str(profile['id']), top=args.top)
    finally:
        conn.close()

    slug = _slug(profile['name'])
    output_dir = Path(args.output_dir) if args.output_dir else Path(__file__).resolve().parent.parent / 'pages' / slug
    output_dir.mkdir(parents=True, exist_ok=True)

    pages = {
        'index.html': generate_index(profile),
        'outreach.html': generate_outreach(profile, matches),
        'profile.html': generate_profile(profile),
    }

    for filename, content in pages.items():
        path = output_dir / filename
        with open(path, 'w') as f:
            f.write(content)

    print(f'Generated 3-page report in: {output_dir}/')
    print(f'  Profile: {profile["name"]} ({profile.get("company") or "N/A"})')
    print(f'  Matches: {len(matches)} partners')
    print(f'  Pages:')
    for filename in pages:
        print(f'    - {filename}')
    print(f'  Open in browser: file://{os.path.abspath(output_dir / "index.html")}')


if __name__ == '__main__':
    main()
