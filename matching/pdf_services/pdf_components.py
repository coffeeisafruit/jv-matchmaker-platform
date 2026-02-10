"""
Reusable PDF components for JV Matcher Reports
"""

from reportlab.platypus import (
    Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
)
from reportlab.lib.units import inch
from reportlab.lib.colors import white, HexColor
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.pdfgen import canvas

from .pdf_styles import COLORS


def detect_urgency(timing_text):
    """Detect urgency level from timing text"""
    if not timing_text:
        return 'Medium'

    timing_lower = str(timing_text).lower()

    high_keywords = ['immediate', 'urgent', 'asap', 'time-sensitive', 'this week', 'tomorrow', 'now']
    if any(k in timing_lower for k in high_keywords):
        return 'High'

    low_keywords = ['ongoing', 'no rush', 'long-term', 'whenever', 'flexible']
    if any(k in timing_lower for k in low_keywords):
        return 'Low'

    return 'Medium'


def detect_collaboration_type(opportunity_text):
    """Detect collaboration type from opportunity text"""
    if not opportunity_text:
        return 'Partnership'

    opp_lower = str(opportunity_text).lower()

    if 'joint venture' in opp_lower or 'jv' in opp_lower:
        return 'Joint Venture'
    elif 'cross-referral' in opp_lower or 'referral' in opp_lower:
        return 'Cross-Referral'
    elif 'publishing' in opp_lower or 'book' in opp_lower:
        return 'Publishing'
    elif 'speaking' in opp_lower or 'event' in opp_lower:
        return 'Speaking'
    elif 'coaching' in opp_lower or 'mentoring' in opp_lower:
        return 'Coaching'
    else:
        return 'Partnership'


def get_score_color(score):
    """Get color based on score value"""
    if score >= 90:
        return COLORS['score_excellent']
    elif score >= 75:
        return COLORS['score_good']
    else:
        return COLORS['score_fair']


def parse_score(score_str):
    """Parse score from string like '95/100' or '95' or float 63.3"""
    try:
        score_text = str(score_str)
        if '/' in score_text:
            return int(float(score_text.split('/')[0]))
        # Use float first to handle decimal numbers like 63.3
        return int(float(score_text))
    except (ValueError, TypeError):
        return 0


def safe_get(obj, key, default="[Not provided]"):
    """Safely get value with user-friendly default"""
    value = obj.get(key, default) if obj else default
    return value if value and str(value).strip() else default


def create_cover_page(data, styles):
    """Create cover page with participant profile"""
    elements = []

    # Title
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(Paragraph("JV MATCHER REPORT", styles['Hero']))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph("Your Personalized Partnership Opportunities", styles['HeroSubtitle']))
    elements.append(Spacer(1, 0.4 * inch))

    # Participant name
    participant = safe_get(data, 'participant', 'JV Member')
    elements.append(Paragraph(f"Prepared for: <b>{participant}</b>", styles['SectionHead']))

    # Date
    date_str = safe_get(data, 'date', '')
    if date_str:
        elements.append(Paragraph(f"Generated: {date_str}", styles['Body']))

    elements.append(Spacer(1, 0.3 * inch))

    # Profile section
    profile = data.get('profile', {})

    elements.append(Paragraph("YOUR PROFILE", styles['SectionHead']))
    elements.append(Spacer(1, 0.1 * inch))

    # Profile fields
    profile_fields = [
        ('What You Do', 'what_you_do'),
        ('Who You Serve', 'who_you_serve'),
        ('What You\'re Seeking', 'seeking'),
        ('What You\'re Offering', 'offering'),
        ('Current Projects', 'current_projects'),
    ]

    for label, key in profile_fields:
        value = safe_get(profile, key, '')
        if value and value != "[Not provided]":
            elements.append(Paragraph(f"<b>{label}:</b>", styles['ProfileLabel']))
            elements.append(Paragraph(value, styles['ProfileValue']))

    # Match summary
    matches = data.get('matches', [])
    if matches:
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(Paragraph("MATCH SUMMARY", styles['SectionHead']))

        # Calculate stats
        scores = [parse_score(m.get('score', 0)) for m in matches]
        avg_score = sum(scores) / len(scores) if scores else 0
        top_score = max(scores) if scores else 0

        summary_text = f"Total Matches: <b>{len(matches)}</b> | Average Score: <b>{avg_score:.0f}/100</b> | Top Score: <b>{top_score}/100</b>"
        elements.append(Paragraph(summary_text, styles['Body']))

    elements.append(PageBreak())

    return elements


def create_dashboard(matches, styles):
    """Create executive dashboard with all matches overview"""
    elements = []

    elements.append(Paragraph("EXECUTIVE DASHBOARD", styles['Hero']))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph("Quick Overview of All Your Matches", styles['HeroSubtitle']))
    elements.append(Spacer(1, 0.4 * inch))

    if not matches:
        elements.append(Paragraph("No matches available.", styles['Body']))
        elements.append(PageBreak())
        return elements

    # Create table data
    table_data = [
        ['#', 'Partner Name', 'Score', 'Type', 'Urgency']
    ]

    for i, match in enumerate(matches, 1):
        score = parse_score(match.get('score', 0))
        urgency = detect_urgency(match.get('timing', ''))

        table_data.append([
            str(i),
            safe_get(match, 'name', 'Unknown')[:30],
            f"{score}/100",
            safe_get(match, 'type', 'Partnership')[:15],
            urgency
        ])

    # Create table - widened columns for full content visibility
    # Total width ~6.5 inches (letter page 8.5" - 1.5" margins)
    col_widths = [0.35 * inch, 2.5 * inch, 0.7 * inch, 1.5 * inch, 1.45 * inch]
    table = Table(table_data, colWidths=col_widths)

    # Style the table
    table_style = TableStyle([
        # Header styling
        ('BACKGROUND', (0, 0), (-1, 0), COLORS['primary']),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),

        # Body styling
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # # column
        ('ALIGN', (2, 1), (2, -1), 'CENTER'),  # Score column
        ('ALIGN', (4, 1), (4, -1), 'CENTER'),  # Urgency column

        # Alternating row colors
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, COLORS['light_bg']]),

        # Grid
        ('GRID', (0, 0), (-1, -1), 0.5, COLORS['border']),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('TOPPADDING', (0, 1), (-1, -1), 6),
    ])

    # Add urgency color highlighting
    for i, match in enumerate(matches, 1):
        urgency = detect_urgency(match.get('timing', ''))
        if urgency == 'High':
            table_style.add('TEXTCOLOR', (4, i), (4, i), COLORS['urgency_high'])
            table_style.add('FONTNAME', (4, i), (4, i), 'Helvetica-Bold')
        elif urgency == 'Medium':
            table_style.add('TEXTCOLOR', (4, i), (4, i), COLORS['urgency_medium'])

        # Score color
        score = parse_score(match.get('score', 0))
        if score >= 90:
            table_style.add('TEXTCOLOR', (2, i), (2, i), COLORS['score_excellent'])
            table_style.add('FONTNAME', (2, i), (2, i), 'Helvetica-Bold')
        elif score >= 75:
            table_style.add('TEXTCOLOR', (2, i), (2, i), COLORS['score_good'])

    table.setStyle(table_style)
    elements.append(table)

    elements.append(Spacer(1, 0.3 * inch))

    # Legend
    legend_text = "<b>Urgency Legend:</b> <font color='#E74C3C'>High</font> = Act Now | <font color='#F39C12'>Medium</font> = This Quarter | <font color='#95A5A6'>Low</font> = Ongoing"
    elements.append(Paragraph(legend_text, styles['Small']))

    elements.append(PageBreak())

    return elements


def _create_single_match(match, styles, match_num, is_top_pick=False):
    """Create a single match detail card"""
    elements = []

    score = parse_score(match.get('score', 0))
    score_color = get_score_color(score)
    urgency = detect_urgency(match.get('timing', ''))
    collab_type = detect_collaboration_type(match.get('opportunity', ''))

    # Top Pick badge for top 3 matches
    top_pick_prefix = "TOP PICK - " if is_top_pick else ""

    # Header with name and score
    header_data = [[
        Paragraph(f"<b>{top_pick_prefix}MATCH #{match_num}: {safe_get(match, 'name', 'Unknown')}</b>", styles['MatchTitle']),
        Paragraph(f"<b>{score}/100</b>", styles['BigScore'])
    ]]

    header_table = Table(header_data, colWidths=[4.5 * inch, 1.5 * inch], rowHeights=[0.5 * inch])
    header_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), score_color),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'CENTER'),
        ('LEFTPADDING', (0, 0), (0, 0), 12),
        ('RIGHTPADDING', (1, 0), (1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    elements.append(header_table)

    # Match type and urgency badges
    badge_text = f"<b>Type:</b> {safe_get(match, 'type', collab_type)} | <b>Urgency:</b> {urgency}"
    elements.append(Spacer(1, 0.08 * inch))
    elements.append(Paragraph(badge_text, styles['Subhead']))
    elements.append(Spacer(1, 0.15 * inch))

    # Detail sections in a clean layout
    sections = [
        ('Why They\'re a Great Fit', 'fit'),
        ('Collaboration Opportunity', 'opportunity'),
        ('Mutual Benefits', 'benefits'),
        ('Timing & Next Steps', 'timing'),
    ]

    for label, key in sections:
        value = safe_get(match, key, '')
        if value and value != "[Not provided]":
            elements.append(Paragraph(f"<b>{label}</b>", styles['ProfileLabel']))
            elements.append(Spacer(1, 0.05 * inch))
            # Convert newlines to <br/> tags for proper PDF rendering
            formatted_value = value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            formatted_value = formatted_value.replace('\n', '<br/>')
            elements.append(Paragraph(formatted_value, styles['BodySmall']))
            elements.append(Spacer(1, 0.15 * inch))

    # Outreach message in a box
    message = safe_get(match, 'message', '')
    if message and message != "[Not provided]":
        elements.append(Spacer(1, 0.1 * inch))
        elements.append(Paragraph("<b>Ready-to-Send Outreach Message:</b>", styles['ProfileLabel']))

        # Convert newlines to <br/> tags for proper PDF rendering
        # Also escape any HTML special characters first
        formatted_message = message.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        formatted_message = formatted_message.replace('\n', '<br/>')

        # Message in a styled box
        msg_table = Table([[Paragraph(formatted_message, styles['MessageBox'])]], colWidths=[6 * inch])
        msg_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), COLORS['light_bg']),
            ('BOX', (0, 0), (-1, -1), 1, COLORS['border']),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('RIGHTPADDING', (0, 0), (-1, -1), 10),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(msg_table)

    # Contact info
    contact = safe_get(match, 'contact', '')
    if contact and contact != "[Not provided]":
        elements.append(Spacer(1, 0.1 * inch))
        elements.append(Paragraph(f"<b>Contact:</b> {contact}", styles['Small']))

    return elements


def _build_left_column(match, styles, match_num, is_top_pick=False):
    """Build the left column content: Profile info - 12pt legible text"""
    content = []

    score = parse_score(match.get('score', 0))
    name = safe_get(match, 'name', 'Unknown Partner')

    # Match badge + Score on same line
    badge_text = "★ TOP PICK" if is_top_pick else f"#{match_num}"
    content.append(Paragraph(
        f"<font color='#0071e3'><b>{badge_text}</b></font>  "
        f"<font color='#34c759' size='16'><b>{score}</b></font><font color='#86868b'>/100</font>",
        styles['Body']
    ))

    # Name (prominent)
    content.append(Paragraph(f"<b>{name}</b>", styles['SectionHead']))
    content.append(Spacer(1, 0.15 * inch))

    # Contact section - includes email, website, calendar, contact preference
    contact = safe_get(match, 'contact', '')
    website = match.get('website', '')
    calendar_link = match.get('calendar_link', '')
    best_contact = match.get('best_contact', '')

    if contact or website or calendar_link or best_contact:
        content.append(Paragraph("<font color='#0071e3'><b>CONTACT</b></font>", styles['Body']))
        content.append(Spacer(1, 0.08 * inch))

        # Email/LinkedIn
        if contact and contact != "[Not provided]":
            contact_parts = contact.split(' | ')
            for part in contact_parts[:2]:
                content.append(Paragraph(part.strip(), styles['BodySmall']))

        # Website
        if website:
            content.append(Paragraph(f"<font color='#0071e3'>{website}</font>", styles['BodySmall']))

        # Calendar link
        if calendar_link:
            content.append(Paragraph(f"<b>Calendar:</b> {calendar_link}", styles['BodySmall']))

        # Best contact method
        if best_contact:
            content.append(Paragraph(f"<b>Preferred:</b> {best_contact}", styles['BodySmall']))

        content.append(Spacer(1, 0.2 * inch))

    # Why Great Fit - show full content with tighter spacing
    fit = safe_get(match, 'fit', '')
    if fit and fit != "[Not provided]":
        content.append(Paragraph("<font color='#0071e3'><b>WHY GREAT FIT</b></font>", styles['Body']))
        content.append(Spacer(1, 0.06 * inch))
        # Increased limit to 550 chars - tight spacing allows more content
        MAX_FIT_CHARS = 550
        fit_text = fit[:MAX_FIT_CHARS] if len(fit) <= MAX_FIT_CHARS else fit[:fit.rfind('\n\n', 0, MAX_FIT_CHARS)] if fit.rfind('\n\n', 0, MAX_FIT_CHARS) > 100 else fit[:MAX_FIT_CHARS]
        formatted_fit = fit_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        # Preserve paragraph breaks (double newlines) as <br/><br/>
        # Convert single newlines to <br/> for proper line breaks
        formatted_fit = formatted_fit.replace('\n\n', '<br/><br/>')
        formatted_fit = formatted_fit.replace('\n', '<br/>')
        content.append(Paragraph(formatted_fit, styles['BodySmall']))
        content.append(Spacer(1, 0.2 * inch))

    # Strategy (mutual benefits) - increased limit with tight spacing
    opportunity = safe_get(match, 'opportunity', '')
    if opportunity and opportunity != "[Not provided]":
        content.append(Paragraph("<font color='#0071e3'><b>STRATEGY</b></font>", styles['Body']))
        content.append(Spacer(1, 0.06 * inch))
        # Increased to 420 chars - tight spacing allows more content
        MAX_STRATEGY_CHARS = 420
        opp_text = opportunity
        if len(opp_text) > MAX_STRATEGY_CHARS:
            # Find last complete bullet point before limit
            search_text = opp_text[:MAX_STRATEGY_CHARS]
            # Find the last newline (end of bullet) before limit
            last_newline = search_text.rfind('\n')
            if last_newline > 100:
                opp_text = opp_text[:last_newline].rstrip()
            else:
                # Fallback: end at word boundary
                truncate_at = search_text.rfind(' ')
                opp_text = opp_text[:truncate_at]
        formatted_opp = opp_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        formatted_opp = formatted_opp.replace('\n', '<br/>')
        content.append(Paragraph(formatted_opp, styles['BodySmall']))

    return content


def _build_right_column(match, styles):
    """Build the right column content: Email - 12pt legible text"""
    content = []

    # Header
    content.append(Paragraph("<font color='#0071e3'><b>OUTREACH EMAIL</b></font>", styles['Body']))
    content.append(Spacer(1, 0.25 * inch))

    message = safe_get(match, 'message', '')
    if message and message != "[Not provided]":
        # Parse subject line
        lines = message.split('\n')
        subject_line = ""
        email_body = message

        for i, line in enumerate(lines):
            if line.upper().startswith('SUBJECT:'):
                subject_line = line.replace('SUBJECT:', '').replace('Subject:', '').strip()
                email_body = '\n'.join(lines[i+1:]).strip()
                break

        # Subject in a subtle highlight
        if subject_line:
            subj_table = Table([[
                Paragraph(f"<b>Subject:</b> {subject_line}", styles['BodySmall'])
            ]], colWidths=[3.4 * inch])
            subj_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), COLORS['light_bg']),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            content.append(subj_table)
            content.append(Spacer(1, 0.25 * inch))

        # Email body - single spacing, paragraph breaks only on double newlines
        formatted_email = email_body.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        formatted_email = formatted_email.replace('\n\n', '<br/><br/>').replace('\n', '<br/>')
        content.append(Paragraph(formatted_email, styles['BodySmall']))

    return content


def _create_single_match_page(match, styles, match_num, is_top_pick=False):
    """
    Create a single-page match layout with two columns:
    - Left: Profile info (name, score, contact, fit, strategy)
    - Right: Ready-to-send email
    """
    elements = []

    # Build column content
    left_content = _build_left_column(match, styles, match_num, is_top_pick)
    right_content = _build_right_column(match, styles)

    # Wrap in tables for the columns
    left_cell = Table([[c] for c in left_content], colWidths=[3.0 * inch])
    left_cell.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))

    right_cell = Table([[c] for c in right_content], colWidths=[3.3 * inch])
    right_cell.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))

    # Main two-column table - fills most of the page height
    # Page is 11" tall, minus 0.75" top/bottom margins = ~9.5" usable
    # We want content to fill this space better
    main_table = Table(
        [[left_cell, right_cell]],
        colWidths=[3.0 * inch, 3.5 * inch],
        rowHeights=[8.5 * inch]  # Fill most of page
    )
    main_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        # Vertical divider between columns
        ('LINEAFTER', (0, 0), (0, -1), 0.5, COLORS['border']),
    ]))

    elements.append(main_table)
    return elements


def create_match_pages(matches, styles):
    """
    Create match pages - ONE page per match with two-column layout
    No wasted divider page - go straight to matches
    """
    elements = []

    for i, match in enumerate(matches, 1):
        # One page per match - content fills the page
        match_elements = _create_single_match_page(match, styles, i, is_top_pick=(i <= 3))
        elements.extend(match_elements)
        elements.append(PageBreak())

    return elements


def create_action_tracker(matches, styles):
    """Create action tracker page for follow-up"""
    elements = []

    elements.append(Paragraph("ACTION TRACKER", styles['Hero']))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph("Your Follow-Up Checklist", styles['HeroSubtitle']))
    elements.append(Spacer(1, 0.4 * inch))

    if not matches:
        elements.append(Paragraph("No matches to track.", styles['Body']))
        return elements

    # Create checklist table
    table_data = [
        ['', 'Partner', 'Action Item', 'Urgency', 'Status']
    ]

    for i, match in enumerate(matches, 1):
        urgency = detect_urgency(match.get('timing', ''))
        name = safe_get(match, 'name', 'Unknown')[:28]  # Increased from 20

        # Default action based on urgency
        if urgency == 'High':
            action = "Send outreach message TODAY"
        elif urgency == 'Medium':
            action = "Schedule outreach this week"
        else:
            action = "Add to follow-up list"

        table_data.append([
            str(i),
            name,
            action,
            urgency,
            '\u2610'  # Unicode ballot box instead of [ ]
        ])

    # Create table - widened for better readability
    col_widths = [0.35 * inch, 1.6 * inch, 2.3 * inch, 0.7 * inch, 0.5 * inch]
    table = Table(table_data, colWidths=col_widths)

    table_style = TableStyle([
        # Header
        ('BACKGROUND', (0, 0), (-1, 0), COLORS['dark']),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),

        # Body
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),
        ('ALIGN', (3, 1), (3, -1), 'CENTER'),
        ('ALIGN', (4, 1), (4, -1), 'CENTER'),

        # Alternating rows
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, COLORS['light_bg']]),

        # Grid
        ('GRID', (0, 0), (-1, -1), 0.5, COLORS['border']),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ('TOPPADDING', (0, 1), (-1, -1), 8),
    ])

    # Urgency highlighting
    for i, match in enumerate(matches, 1):
        urgency = detect_urgency(match.get('timing', ''))
        if urgency == 'High':
            table_style.add('TEXTCOLOR', (3, i), (3, i), COLORS['urgency_high'])
            table_style.add('FONTNAME', (3, i), (3, i), 'Helvetica-Bold')
        elif urgency == 'Medium':
            table_style.add('TEXTCOLOR', (3, i), (3, i), COLORS['urgency_medium'])

    table.setStyle(table_style)
    elements.append(table)

    # Tips section
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(Paragraph("<b>Follow-Up Tips:</b>", styles['Subhead']))

    tips = [
        "Personalize each message - reference specific points from their profile",
        "High urgency contacts should be reached within 24-48 hours",
        "Follow up if no response within 5-7 business days",
        "Track all communications in your CRM or spreadsheet",
    ]

    for tip in tips:
        elements.append(Paragraph(f"  {tip}", styles['BodySmall']))

    return elements


class FooterCanvas(canvas.Canvas):
    """Custom canvas with page numbers and footer"""

    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        self.setFont("Helvetica", 8)
        self.setFillColor(HexColor('#95A5A6'))

        # Page number on right
        page_num = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(7.5 * inch, 0.5 * inch, page_num)

        # Footer text on left
        self.drawString(0.75 * inch, 0.5 * inch, "JV Matcher Report | Confidential")
