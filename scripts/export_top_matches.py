#!/usr/bin/env python3
"""
Export Top Matches for Outreach Campaign
Generates a CSV of high-quality matches ready for outreach.
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

from matching.models import SupabaseProfile, SupabaseMatch


def get_top_matches(profile_id=None, min_score=0.7, limit=100, require_email=True, require_booking=False):
    """
    Get top matches for a profile (or all high-scoring matches if no profile specified)

    Args:
        profile_id: UUID of profile to get matches for (None = get all high matches)
        min_score: Minimum harmonic_mean score (default 0.7 = 70%)
        limit: Maximum number of matches to return
        require_email: Only include profiles with email addresses
        require_booking: Only include profiles with booking links

    Returns:
        List of match dictionaries with profile data
    """
    # Build query
    query = SupabaseMatch.objects.filter(
        harmonic_mean__gte=min_score,
        status='pending'  # Not yet contacted
    )

    if profile_id:
        query = query.filter(profile_id=profile_id)

    # Order by score descending
    matches = query.order_by('-harmonic_mean')[:limit * 2]  # Get 2x to account for filtering

    # Get all suggested profile IDs
    profile_ids = list(matches.values_list('suggested_profile_id', flat=True))

    # Fetch profiles in bulk
    profiles_query = SupabaseProfile.objects.filter(id__in=profile_ids)

    if require_email:
        profiles_query = profiles_query.filter(email__isnull=False).exclude(email='')

    if require_booking:
        profiles_query = profiles_query.filter(booking_link__isnull=False).exclude(booking_link='')

    profiles = {p.id: p for p in profiles_query}

    # Build results
    results = []
    for match in matches:
        profile = profiles.get(match.suggested_profile_id)
        if not profile:
            continue  # Filtered out

        results.append({
            'match_id': str(match.id),
            'profile_id': str(profile.id),
            'name': profile.name or '',
            'email': profile.email or '',
            'phone': profile.phone or '',
            'company': profile.company or '',
            'website': profile.website or '',
            'linkedin': profile.linkedin or '',
            'booking_link': profile.booking_link or '',
            'match_score': float(match.harmonic_mean),
            'score_ab': float(match.score_ab) if match.score_ab else 0.0,
            'score_ba': float(match.score_ba) if match.score_ba else 0.0,
            'match_reason': match.match_reason or '',
            'list_size': profile.list_size or 0,
            'niche': profile.niche or '',
            'what_you_do': profile.what_you_do or '',
            'who_you_serve': profile.who_you_serve or '',
            'seeking': profile.seeking or '',
            'offering': profile.offering or '',
            'signature_programs': profile.signature_programs or '',
            'status': profile.status or '',
            'last_active': profile.last_active_at.isoformat() if profile.last_active_at else '',
        })

        if len(results) >= limit:
            break

    return results


def export_to_csv(matches, output_file):
    """Export matches to CSV file"""
    if not matches:
        print(f"⚠️  No matches to export")
        return

    fieldnames = [
        'name', 'email', 'phone', 'company', 'website', 'linkedin', 'booking_link',
        'match_score', 'score_ab', 'score_ba', 'match_reason',
        'list_size', 'niche', 'what_you_do', 'who_you_serve',
        'seeking', 'offering', 'signature_programs',
        'status', 'last_active', 'match_id', 'profile_id'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matches)

    print(f"✅ Exported {len(matches)} matches to {output_file}")


def print_summary(matches):
    """Print summary statistics"""
    if not matches:
        print("No matches found")
        return

    print(f"\n{'='*70}")
    print(f"EXPORT SUMMARY")
    print(f"{'='*70}\n")

    total = len(matches)
    with_email = len([m for m in matches if m['email']])
    with_booking = len([m for m in matches if m['booking_link']])
    with_phone = len([m for m in matches if m['phone']])

    avg_score = sum(m['match_score'] for m in matches) / total if total else 0
    avg_list = sum(m['list_size'] for m in matches) / total if total else 0

    print(f"Total Matches: {total}")
    print(f"With Email: {with_email} ({with_email/total*100:.1f}%)")
    print(f"With Booking Link: {with_booking} ({with_booking/total*100:.1f}%)")
    print(f"With Phone: {with_phone} ({with_phone/total*100:.1f}%)")
    print(f"\nAverage Match Score: {avg_score:.1%}")
    print(f"Average List Size: {avg_list:,.0f}")

    # Top 10 by score
    print(f"\n{'='*70}")
    print("TOP 10 MATCHES BY SCORE")
    print(f"{'='*70}\n")

    for i, match in enumerate(matches[:10], 1):
        print(f"{i:2}. {match['name'][:35]:35} | Score: {match['match_score']:5.1%} | List: {match['list_size']:>8,}")

    # Top 10 by list size
    print(f"\n{'='*70}")
    print("TOP 10 MATCHES BY LIST SIZE")
    print(f"{'='*70}\n")

    sorted_by_list = sorted(matches, key=lambda x: x['list_size'], reverse=True)
    for i, match in enumerate(sorted_by_list[:10], 1):
        print(f"{i:2}. {match['name'][:35]:35} | List: {match['list_size']:>8,} | Score: {match['match_score']:5.1%}")


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Export top matches for outreach')
    parser.add_argument('--profile-id', help='Profile UUID to get matches for (optional)')
    parser.add_argument('--min-score', type=float, default=0.7, help='Minimum match score (default: 0.7)')
    parser.add_argument('--limit', type=int, default=100, help='Maximum matches to export (default: 100)')
    parser.add_argument('--require-booking', action='store_true', help='Only include profiles with booking links')
    parser.add_argument('--output', default='top_matches.csv', help='Output CSV file (default: top_matches.csv)')
    parser.add_argument('--no-summary', action='store_true', help='Skip printing summary')

    args = parser.parse_args()

    print(f"\n{'='*70}")
    print("EXPORT TOP MATCHES")
    print(f"{'='*70}\n")

    print(f"Parameters:")
    print(f"  Profile ID: {args.profile_id or 'All profiles'}")
    print(f"  Min Score: {args.min_score:.0%}")
    print(f"  Limit: {args.limit}")
    print(f"  Require Email: Yes (always)")
    print(f"  Require Booking: {'Yes' if args.require_booking else 'No'}")
    print(f"  Output: {args.output}\n")

    try:
        # Get matches
        matches = get_top_matches(
            profile_id=args.profile_id,
            min_score=args.min_score,
            limit=args.limit,
            require_email=True,
            require_booking=args.require_booking
        )

        if not matches:
            print("❌ No matches found with the specified criteria")
            print("\nTry:")
            print("  • Lowering --min-score (e.g., --min-score 0.6)")
            print("  • Removing --require-booking flag")
            print("  • Increasing --limit")
            return

        # Export to CSV
        export_to_csv(matches, args.output)

        # Print summary
        if not args.no_summary:
            print_summary(matches)

        print(f"\n{'='*70}")
        print("✅ EXPORT COMPLETE")
        print(f"{'='*70}")
        print(f"\nNext steps:")
        print(f"  1. Review {args.output}")
        print(f"  2. Validate emails using scripts/validate_emails.py")
        print(f"  3. Start test outreach to 10-20 contacts")
        print(f"  4. Track: deliverability, opens, responses, bookings\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
