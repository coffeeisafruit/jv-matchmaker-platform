#!/usr/bin/env python
"""Export a member's outreach page as static HTML for GitHub Pages.

Renders the outreach page from ReportPartner snapshot data (not live
SupabaseMatch queries), making it self-contained for static hosting.

Usage:
    python scripts/export_static_outreach.py --name "Vadim Voss" --output /tmp/vadim-voss-profile/outreach.html
    python scripts/export_static_outreach.py --report-id 11 --output /tmp/outreach.html
"""
import argparse
import json
import os
import sys

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.template.loader import render_to_string
from django.utils.safestring import mark_safe

from matching.models import MemberReport


class StaticReportWrapper:
    """Wraps a MemberReport so outreach_templates renders as valid JSON in templates.

    Django's JSONField returns Python dicts, but the template uses
    {{ report.outreach_templates|safe }} inside a <script> tag. Python's
    str() on dicts outputs single quotes — invalid JavaScript. This wrapper
    makes the attribute return a pre-serialized JSON string that Django's
    |safe filter passes through correctly.
    """

    def __init__(self, report):
        self._report = report

    def __getattr__(self, name):
        if name == 'outreach_templates':
            raw = self._report.outreach_templates
            if raw:
                return mark_safe(json.dumps(raw))
            return None
        return getattr(self._report, name)


def export_outreach(report, output_path):
    """Render a MemberReport's outreach page as static HTML."""
    partners = report.partners.all().order_by('rank')

    sections = []
    for section_key in ['curated', 'priority', 'this_week', 'low_priority', 'jv_programs']:
        section_partners = partners.filter(section=section_key)
        if section_partners.exists():
            first = section_partners.first()
            sections.append({
                'key': section_key,
                'label': first.section_label or section_key.replace('_', ' ').title(),
                'note': first.section_note or '',
                'partners': list(section_partners),
            })

    wrapped = StaticReportWrapper(report)

    tracking_config = {}
    supabase_url = os.environ.get('SUPABASE_URL', '')
    supabase_key = os.environ.get('SUPABASE_KEY', '')
    if supabase_url and supabase_key:
        tracking_config = {
            'supabase_url': supabase_url,
            'supabase_anon_key': supabase_key,
            'report_id': report.id,
            'access_code': report.access_code,
        }

    context = {
        'report': wrapped,
        'sections': sections,
        'total_partners': partners.count(),
        'static_mode': True,
        'tracking_config': json.dumps(tracking_config) if tracking_config else '',
        'clarity_project_id': os.environ.get('CLARITY_PROJECT_ID', ''),
    }

    html = render_to_string('matching/report_outreach.html', context)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    print(f'Exported outreach page to {output_path}')
    print(f'  Sections: {len(sections)}')
    print(f'  Partners: {partners.count()}')
    for s in sections:
        print(f'    {s["label"]}: {len(s["partners"])} partners')
    return html


def main():
    parser = argparse.ArgumentParser(description='Export static outreach page')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--name', help='Member name (finds latest active report)')
    group.add_argument('--report-id', type=int, help='Report ID to export')
    parser.add_argument('--output', required=True, help='Output HTML file path')
    parser.add_argument('--clarity-id', help='Microsoft Clarity project ID (overrides CLARITY_PROJECT_ID env)')
    args = parser.parse_args()

    if args.clarity_id:
        os.environ['CLARITY_PROJECT_ID'] = args.clarity_id

    if args.report_id:
        report = MemberReport.objects.filter(id=args.report_id, is_active=True).first()
        if not report:
            print(f'No active report found with ID: {args.report_id}')
            sys.exit(1)
    else:
        report = (
            MemberReport.objects
            .filter(member_name__icontains=args.name, is_active=True)
            .order_by('-created_at')
            .first()
        )
        if not report:
            print(f'No active report found for: {args.name}')
            sys.exit(1)

    print(f'Found report: {report.member_name} (id={report.id})')
    print(f'  Company: {report.company_name}')
    print(f'  Month: {report.month}')
    print(f'  Access code: {report.access_code}')

    export_outreach(report, args.output)


if __name__ == '__main__':
    main()
