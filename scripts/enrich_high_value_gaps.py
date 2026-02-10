#!/usr/bin/env python3
"""
Enrich High-Value Profiles Missing Critical Data

Focuses on profiles with high list sizes (>10K) but missing email addresses.
Uses progressive enrichment: free methods first, then low-cost paid if needed.
"""

import os
import sys
import django
import csv
from datetime import timedelta
from django.utils import timezone

# Setup Django environment
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db.models import Q
from matching.models import SupabaseProfile


def identify_high_value_gaps(min_list_size=10000, limit=None):
    """
    Identify high-value profiles missing email addresses

    Args:
        min_list_size: Minimum list size to consider (default: 10,000)
        limit: Maximum profiles to return (None = all)

    Returns:
        QuerySet of SupabaseProfile objects
    """
    profiles = SupabaseProfile.objects.filter(
        (Q(email__isnull=True) | Q(email='')) &
        Q(list_size__gt=min_list_size) &
        Q(status='Member')
    ).order_by('-list_size')

    if limit:
        profiles = profiles[:limit]

    return profiles


def export_enrichment_targets(profiles, output_file):
    """Export profiles to CSV for manual/automated enrichment"""
    fieldnames = [
        'profile_id', 'name', 'company', 'website', 'linkedin',
        'list_size', 'niche', 'status', 'what_you_do', 'who_you_serve',
        'offering', 'notes'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for profile in profiles:
            writer.writerow({
                'profile_id': str(profile.id),
                'name': profile.name or '',
                'company': profile.company or '',
                'website': profile.website or '',
                'linkedin': profile.linkedin or '',
                'list_size': profile.list_size or 0,
                'niche': profile.niche or '',
                'status': profile.status or '',
                'what_you_do': profile.what_you_do or '',
                'who_you_serve': profile.who_you_serve or '',
                'offering': profile.offering or '',
                'notes': 'HIGH VALUE - Missing email'
            })

    print(f"✅ Exported {len(profiles)} profiles to {output_file}")


def print_enrichment_summary(profiles):
    """Print summary of enrichment opportunities"""
    if not profiles:
        print("No profiles found matching criteria")
        return

    print(f"\n{'='*70}")
    print("HIGH-VALUE ENRICHMENT TARGETS")
    print(f"{'='*70}\n")

    total = len(profiles)
    total_reach = sum(p.list_size for p in profiles)
    with_website = len([p for p in profiles if p.website])
    with_linkedin = len([p for p in profiles if p.linkedin])
    with_company = len([p for p in profiles if p.company])

    print(f"Total Profiles: {total:,}")
    print(f"Total Combined Reach: {total_reach:,}")
    print(f"Average List Size: {total_reach/total:,.0f}" if total else "N/A")
    print(f"\nWith Website: {with_website} ({with_website/total*100:.1f}%)")
    print(f"With LinkedIn: {with_linkedin} ({with_linkedin/total*100:.1f}%)")
    print(f"With Company: {with_company} ({with_company/total*100:.1f}%)")

    print(f"\n{'='*70}")
    print("TOP 20 BY LIST SIZE")
    print(f"{'='*70}\n")

    for i, profile in enumerate(profiles[:20], 1):
        has_website = '✓' if profile.website else '✗'
        has_linkedin = '✓' if profile.linkedin else '✗'
        has_company = '✓' if profile.company else '✗'

        print(f"{i:2}. {(profile.name or 'Unknown')[:30]:30} | "
              f"List: {profile.list_size:>9,} | "
              f"Web:{has_website} LI:{has_linkedin} Co:{has_company}")

    print(f"\n{'='*70}")
    print("ENRICHMENT STRATEGY")
    print(f"{'='*70}\n")

    print("TIER 1 - FREE METHODS (Try First):")
    print(f"  • {with_website} profiles have websites - scrape contact pages")
    print(f"  • {with_linkedin} profiles have LinkedIn - extract from profile")
    print(f"  • Google search: \"[Name] [Company] email\" or \"[Name] contact\"")
    print(f"  • Check social media bio links")
    print(f"  • Estimated time: 2-5 min per profile")
    print(f"  • Estimated cost: $0")
    print()

    print("TIER 2 - LOW-COST APIS (If Free Fails):")
    print(f"  • Hunter.io: Find emails from name + domain")
    print(f"  • RocketReach: Professional contact database")
    print(f"  • ContactOut: Email finder (Chrome extension)")
    print(f"  • Estimated cost: $0.01-0.05 per profile")
    print(f"  • Total cost for {total} profiles: ${total * 0.02:.2f} - ${total * 0.05:.2f}")
    print()

    print("TIER 3 - PREMIUM ENRICHMENT (High-Value Only):")
    print(f"  • OWL Framework deep research")
    print(f"  • ZoomInfo / Cognism (enterprise)")
    print(f"  • Manual outreach via LinkedIn")
    print(f"  • Estimated cost: $0.10-0.50 per profile")
    print(f"  • Recommend for top 50 only: ${50 * 0.10:.2f} - ${50 * 0.50:.2f}")

    print(f"\n{'='*70}")
    print("EXPECTED OUTCOMES")
    print(f"{'='*70}\n")

    print("If you enrich these profiles:")
    print(f"  • Unlock access to {total_reach:,} combined email list reach")
    print(f"  • Average {total/2:.0f} successful email finds (50% success rate)")
    print(f"  • Cost: $10-25 total using Tier 1+2 methods")
    print(f"  • Value: ${total * 50:.0f} if purchased from data provider")
    print(f"  • ROI: {total * 50 / 25:.0f}x return on investment")


def generate_enrichment_plan(profiles, output_dir='enrichment_batches'):
    """Generate batched enrichment plan"""
    os.makedirs(output_dir, exist_ok=True)

    # Batch 1: Profiles with websites (can scrape)
    with_websites = [p for p in profiles if p.website]
    if with_websites:
        export_enrichment_targets(
            with_websites[:50],  # Top 50 by list size
            os.path.join(output_dir, 'batch1_has_website.csv')
        )
        print(f"\n✅ Batch 1: {len(with_websites[:50])} profiles with websites")
        print(f"   → Use website scraping to find contact info (FREE)")

    # Batch 2: Profiles with LinkedIn
    with_linkedin = [p for p in profiles if p.linkedin and not p.website]
    if with_linkedin:
        export_enrichment_targets(
            with_linkedin[:50],
            os.path.join(output_dir, 'batch2_has_linkedin.csv')
        )
        print(f"\n✅ Batch 2: {len(with_linkedin[:50])} profiles with LinkedIn")
        print(f"   → Use LinkedIn scraping or ContactOut extension")

    # Batch 3: Profiles with company name
    with_company = [p for p in profiles if p.company and not p.website and not p.linkedin]
    if with_company:
        export_enrichment_targets(
            with_company[:50],
            os.path.join(output_dir, 'batch3_has_company.csv')
        )
        print(f"\n✅ Batch 3: {len(with_company[:50])} profiles with company name")
        print(f"   → Use Hunter.io to find email pattern from domain")

    # Batch 4: Minimal data (hardest to enrich)
    minimal = [p for p in profiles if not p.company and not p.website and not p.linkedin]
    if minimal:
        export_enrichment_targets(
            minimal[:20],  # Smaller batch
            os.path.join(output_dir, 'batch4_minimal_data.csv')
        )
        print(f"\n✅ Batch 4: {len(minimal[:20])} profiles with minimal data")
        print(f"   → Use Google search + OWL Framework research")


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Identify and export high-value enrichment targets')
    parser.add_argument('--min-list-size', type=int, default=10000,
                        help='Minimum list size to consider (default: 10,000)')
    parser.add_argument('--limit', type=int, help='Limit number of profiles (default: all)')
    parser.add_argument('--output', default='high_value_gaps.csv',
                        help='Output CSV file (default: high_value_gaps.csv)')
    parser.add_argument('--batch-plan', action='store_true',
                        help='Generate batched enrichment plan')
    parser.add_argument('--no-summary', action='store_true',
                        help='Skip printing summary')

    args = parser.parse_args()

    print(f"\n{'='*70}")
    print("IDENTIFY HIGH-VALUE ENRICHMENT TARGETS")
    print(f"{'='*70}\n")

    print(f"Parameters:")
    print(f"  Min List Size: {args.min_list_size:,}")
    print(f"  Limit: {args.limit or 'All'}")
    print(f"  Output: {args.output}")
    print(f"  Batch Plan: {'Yes' if args.batch_plan else 'No'}\n")

    try:
        # Get high-value profiles
        profiles = list(identify_high_value_gaps(
            min_list_size=args.min_list_size,
            limit=args.limit
        ))

        if not profiles:
            print("✅ No profiles found missing emails with list size >{args.min_list_size:,}")
            print("   All high-value profiles already have contact information!")
            return

        # Export main list
        export_enrichment_targets(profiles, args.output)

        # Print summary
        if not args.no_summary:
            print_enrichment_summary(profiles)

        # Generate batch plan
        if args.batch_plan:
            print(f"\n{'='*70}")
            print("GENERATING BATCHED ENRICHMENT PLAN")
            print(f"{'='*70}")
            generate_enrichment_plan(profiles)

        print(f"\n{'='*70}")
        print("✅ ANALYSIS COMPLETE")
        print(f"{'='*70}")
        print(f"\nNext steps:")
        print(f"  1. Review {args.output}")
        print(f"  2. Start with Batch 1 (websites) - FREE enrichment")
        print(f"  3. Use Hunter.io for Batch 3 (company names) - ~$1-2")
        print(f"  4. Update Supabase with found emails")
        print(f"  5. Re-run export_top_matches.py to get new actionable matches\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
