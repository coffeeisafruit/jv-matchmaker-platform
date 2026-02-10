"""
PDF styling and layout configuration
"""

from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.colors import HexColor, white


def create_pdf_styles():
    """Create all PDF styles"""
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name='Hero',
        fontName='Helvetica-Bold',
        fontSize=24,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        alignment=TA_CENTER,
        spaceAfter=6
    ))

    styles.add(ParagraphStyle(
        name='HeroSubtitle',
        fontName='Helvetica',
        fontSize=14,
        textColor=HexColor('#86868b'),  # Apple Secondary Text
        alignment=TA_CENTER,
        spaceAfter=20
    ))

    styles.add(ParagraphStyle(
        name='SectionHead',
        fontName='Helvetica-Bold',
        fontSize=16,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        spaceAfter=12,
        spaceBefore=16
    ))

    styles.add(ParagraphStyle(
        name='Subhead',
        fontName='Helvetica-Bold',
        fontSize=12,
        textColor=HexColor('#0071e3'),  # Apple Blue
        spaceAfter=6
    ))

    styles.add(ParagraphStyle(
        name='Body',
        fontName='Helvetica',
        fontSize=12,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=20  # Generous line spacing
    ))

    styles.add(ParagraphStyle(
        name='BodySmall',
        fontName='Helvetica',
        fontSize=10,  # Reduced from 11
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=14  # Tighter line spacing to fit more content
    ))

    styles.add(ParagraphStyle(
        name='Small',
        fontName='Helvetica',
        fontSize=8,
        textColor=HexColor('#86868b'),  # Apple Secondary Text
        leading=11
    ))

    styles.add(ParagraphStyle(
        name='SmallBold',
        fontName='Helvetica-Bold',
        fontSize=8,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=11
    ))

    styles.add(ParagraphStyle(
        name='MatchTitle',
        fontName='Helvetica-Bold',
        fontSize=12,
        textColor=white,
        leading=14
    ))

    styles.add(ParagraphStyle(
        name='BigScore',
        fontName='Helvetica-Bold',
        fontSize=24,
        textColor=white,
        alignment=TA_CENTER
    ))

    styles.add(ParagraphStyle(
        name='ProfileLabel',
        fontName='Helvetica-Bold',
        fontSize=10,
        textColor=HexColor('#0071e3'),  # Apple Blue
        spaceAfter=2
    ))

    styles.add(ParagraphStyle(
        name='ProfileValue',
        fontName='Helvetica',
        fontSize=10,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=13,
        spaceAfter=8
    ))

    styles.add(ParagraphStyle(
        name='TableHeader',
        fontName='Helvetica-Bold',
        fontSize=9,
        textColor=white,
        alignment=TA_CENTER
    ))

    styles.add(ParagraphStyle(
        name='TableCell',
        fontName='Helvetica',
        fontSize=8,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=10
    ))

    styles.add(ParagraphStyle(
        name='MessageBox',
        fontName='Helvetica',
        fontSize=10,
        textColor=HexColor('#1d1d1f'),  # Apple Text
        leading=14,
        leftIndent=10,
        rightIndent=10
    ))

    return styles


# Color scheme - Apple Design System
COLORS = {
    'primary': HexColor('#0071e3'),      # Apple Blue
    'secondary': HexColor('#34c759'),    # Apple Green
    'dark': HexColor('#1d1d1f'),         # Apple Text
    'light_bg': HexColor('#f5f5f7'),     # Apple Gray
    'border': HexColor('#d1d1d6'),       # Apple Border
    'white': white,
    'urgency_high': HexColor('#ff3b30'), # Apple Red
    'urgency_medium': HexColor('#ff9500'), # Apple Orange
    'urgency_low': HexColor('#86868b'),  # Apple Gray
    'tier_top': HexColor('#34c759'),     # Apple Green
    'tier_high': HexColor('#0071e3'),    # Apple Blue
    'tier_good': HexColor('#86868b'),    # Apple Gray
    'score_excellent': HexColor('#34c759'), # Apple Green
    'score_good': HexColor('#0071e3'),   # Apple Blue
    'score_fair': HexColor('#ff9500'),   # Apple Orange
}
