#!/usr/bin/env python
"""Export a member's profile page as static HTML for GitHub Pages.

Usage:
    python scripts/export_static_profile.py --name "Vadim Voss" --output /tmp/vadim-voss-profile/profile.html
"""
import argparse
import os
import sys

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.template.loader import render_to_string

from matching.models import SupabaseProfile
from matching.views import _build_profile_context, _format_list_size


class MockReport:
    """Minimal report-like object for static rendering."""
    def __init__(self, name, company):
        self.id = None
        self.member_name = name
        self.company_name = company
        self.client_profile = None


def export_profile(sp, output_path, context_overrides=None):
    """Render a SupabaseProfile to static HTML."""
    report = MockReport(sp.name, sp.company or sp.name)
    context = _build_profile_context(sp, report)

    # Static mode: replace Django URL tags with relative links
    context['static_mode'] = True
    context['company_name'] = sp.company or sp.name
    context['has_supabase_profile'] = False
    context['has_unconfirmed_edits'] = False

    # Apply any overrides (e.g., JV Brief data)
    if context_overrides:
        context.update(context_overrides)

    html = render_to_string('matching/report_profile.html', context)

    with open(output_path, 'w') as f:
        f.write(html)

    print(f'Exported to {output_path}')
    return html


def main():
    parser = argparse.ArgumentParser(description='Export static profile page')
    parser.add_argument('--name', required=True, help='Profile name to export')
    parser.add_argument('--output', required=True, help='Output HTML file path')
    args = parser.parse_args()

    sp = SupabaseProfile.objects.filter(name__iexact=args.name).first()
    if not sp:
        sp = SupabaseProfile.objects.filter(name__icontains=args.name).first()
    if not sp:
        print(f'No profile found for: {args.name}')
        sys.exit(1)

    print(f'Found profile: {sp.name} (id={sp.id})')
    export_profile(sp, args.output)


if __name__ == '__main__':
    main()
