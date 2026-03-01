#!/usr/bin/env python
"""
Diagnostic report: Profile scoring eligibility assessment.

Runs ProfileEnrichmentFilter against all profiles and outputs a markdown
report showing eligible, enrichment candidate, and ineligible profiles.
Also counts existing garbage matches that would be eliminated.

Usage:
    python scripts/enrichment_filter_report.py
"""

import os
import sys

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from matching.models import SupabaseProfile, SupabaseMatch
from matching.services import ProfileEnrichmentFilter


def main():
    print("Loading all profiles...")
    profiles = list(SupabaseProfile.objects.all())
    print(f"Found {len(profiles)} profiles")

    print("Running enrichment filter assessment...")
    report = ProfileEnrichmentFilter.generate_diagnostic_report(
        profiles, match_model=SupabaseMatch
    )

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'Validation', 'enrichment_filter_report.md',
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w') as f:
        f.write(report)

    print(f"\nReport written to: {output_path}")
    print("\n--- Quick Summary ---")
    # Print first few lines of summary
    for line in report.split('\n')[:12]:
        if line.strip():
            print(line)


if __name__ == '__main__':
    main()
