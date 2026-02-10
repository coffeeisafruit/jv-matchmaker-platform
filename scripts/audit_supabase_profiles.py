#!/usr/bin/env python3
"""
Audit Supabase Profile Data Quality
Analyzes the 3,143+ SupabaseProfile records to understand completeness and usability.
"""

import os
import sys
import django
from datetime import timedelta
from django.utils import timezone
from django.db.models import Q, Count, Avg

# Setup Django environment
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from matching.models import SupabaseProfile, SupabaseMatch


def print_header(title):
    """Print a formatted section header"""
    print(f"\n{'='*70}")
    print(f"{title}")
    print(f"{'='*70}\n")


def analyze_data_completeness():
    """Analyze how complete the Supabase profile data is"""
    print_header("📊 DATA COMPLETENESS ANALYSIS")

    total = SupabaseProfile.objects.count()
    print(f"Total SupabaseProfile records: {total:,}\n")

    # Critical contact fields
    metrics = {
        'Email': SupabaseProfile.objects.filter(email__isnull=False).exclude(email='').count(),
        'Phone': SupabaseProfile.objects.filter(phone__isnull=False).exclude(phone='').count(),
        'LinkedIn': SupabaseProfile.objects.filter(linkedin__isnull=False).exclude(linkedin='').count(),
        'Website': SupabaseProfile.objects.filter(website__isnull=False).exclude(website='').count(),
        'Booking Link': SupabaseProfile.objects.filter(booking_link__isnull=False).exclude(booking_link='').count(),
    }

    print("CONTACT INFORMATION:")
    for field, count in metrics.items():
        percentage = (count / total * 100) if total > 0 else 0
        bar = '█' * int(percentage / 2) + '░' * (50 - int(percentage / 2))
        print(f"  {field:15} [{bar}] {count:4}/{total} ({percentage:5.1f}%)")

    # Business intelligence fields
    print("\nBUSINESS INTELLIGENCE:")
    intel_metrics = {
        'What You Do': SupabaseProfile.objects.filter(what_you_do__isnull=False).exclude(what_you_do='').count(),
        'Who You Serve': SupabaseProfile.objects.filter(who_you_serve__isnull=False).exclude(who_you_serve='').count(),
        'Seeking': SupabaseProfile.objects.filter(seeking__isnull=False).exclude(seeking='').count(),
        'Offering': SupabaseProfile.objects.filter(offering__isnull=False).exclude(offering='').count(),
        'Niche': SupabaseProfile.objects.filter(niche__isnull=False).exclude(niche='').count(),
        'Signature Programs': SupabaseProfile.objects.filter(signature_programs__isnull=False).exclude(signature_programs='').count(),
    }

    for field, count in intel_metrics.items():
        percentage = (count / total * 100) if total > 0 else 0
        bar = '█' * int(percentage / 2) + '░' * (50 - int(percentage / 2))
        print(f"  {field:20} [{bar}] {count:4}/{total} ({percentage:5.1f}%)")

    # Audience metrics
    print("\nAUDIENCE METRICS:")
    list_size_counts = {
        '0 (no list)': SupabaseProfile.objects.filter(list_size=0).count(),
        '1-1,000': SupabaseProfile.objects.filter(list_size__gt=0, list_size__lte=1000).count(),
        '1,001-10,000': SupabaseProfile.objects.filter(list_size__gt=1000, list_size__lte=10000).count(),
        '10,001-50,000': SupabaseProfile.objects.filter(list_size__gt=10000, list_size__lte=50000).count(),
        '50,001-100,000': SupabaseProfile.objects.filter(list_size__gt=50000, list_size__lte=100000).count(),
        '100,001+': SupabaseProfile.objects.filter(list_size__gt=100000).count(),
    }

    for range_label, count in list_size_counts.items():
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {range_label:20} {count:4} profiles ({percentage:5.1f}%)")

    avg_list_size = SupabaseProfile.objects.filter(list_size__gt=0).aggregate(Avg('list_size'))['list_size__avg']
    print(f"\n  Average list size (excluding 0): {avg_list_size:,.0f}" if avg_list_size else "\n  No list size data")


def analyze_quality_tiers():
    """Categorize profiles by quality/usability tier"""
    print_header("🎯 QUALITY TIER ANALYSIS")

    total = SupabaseProfile.objects.count()

    # Tier 1: High Quality (ready for immediate outreach)
    tier1_qs = SupabaseProfile.objects.filter(
        email__isnull=False,
        seeking__isnull=False,
        offering__isnull=False,
        list_size__gt=0
    ).exclude(email='').exclude(seeking='').exclude(offering='')
    tier1 = tier1_qs.count()

    # Tier 2: Good Quality (has email + basic info)
    tier2_qs = SupabaseProfile.objects.filter(
        Q(email__isnull=False) & (Q(seeking__isnull=False) | Q(offering__isnull=False))
    ).exclude(email='').exclude(id__in=tier1_qs.values_list('id', flat=True))
    tier2 = tier2_qs.count()

    # Tier 3: Has contact info but missing business intel
    tier3_qs = SupabaseProfile.objects.filter(
        Q(email__isnull=False) | Q(phone__isnull=False) | Q(linkedin__isnull=False)
    ).exclude(id__in=tier1_qs.values_list('id', flat=True)).exclude(id__in=tier2_qs.values_list('id', flat=True))
    tier3 = tier3_qs.count()

    # Tier 4: Incomplete (needs significant enrichment)
    tier4 = total - tier1 - tier2 - tier3

    print(f"TIER 1 - High Quality (email + seeking + offering + list):")
    print(f"  {tier1:4} profiles ({tier1/total*100:5.1f}%)")
    print(f"  ✓ Ready for immediate outreach")
    print(f"  ✓ Complete matching data")
    print(f"  ✓ Audience size known\n")

    print(f"TIER 2 - Good Quality (email + some business intel):")
    print(f"  {tier2:4} profiles ({tier2/total*100:5.1f}%)")
    print(f"  ✓ Can contact")
    print(f"  ⚠ Partial matching data")
    print(f"  ⚠ May need enrichment for seeking/offering\n")

    print(f"TIER 3 - Has Contact Info (email/phone/linkedin only):")
    print(f"  {tier3:4} profiles ({tier3/total*100:5.1f}%)")
    print(f"  ✓ Can reach out")
    print(f"  ❌ Missing business intelligence")
    print(f"  ⚠ Needs enrichment before matching\n")

    print(f"TIER 4 - Incomplete (missing contact info):")
    print(f"  {tier4:4} profiles ({tier4/total*100:5.1f}%)")
    print(f"  ❌ Cannot easily contact")
    print(f"  ❌ Needs significant enrichment\n")

    print(f"USABLE TODAY (Tier 1 + Tier 2): {tier1 + tier2:,} profiles ({(tier1+tier2)/total*100:.1f}%)")
    print(f"NEEDS ENRICHMENT (Tier 3 + Tier 4): {tier3 + tier4:,} profiles ({(tier3+tier4)/total*100:.1f}%)")


def analyze_match_quality():
    """Analyze the pre-computed SupabaseMatch data"""
    print_header("🔗 MATCH QUALITY ANALYSIS")

    total_matches = SupabaseMatch.objects.count()
    print(f"Total pre-computed matches: {total_matches:,}\n")

    if total_matches == 0:
        print("⚠ No SupabaseMatch records found. Matches may need to be computed.")
        return

    # Score distribution
    print("MATCH SCORE DISTRIBUTION:")
    score_ranges = {
        '90-100% (Excellent)': SupabaseMatch.objects.filter(harmonic_mean__gte=0.9).count(),
        '80-89% (Very Good)': SupabaseMatch.objects.filter(harmonic_mean__gte=0.8, harmonic_mean__lt=0.9).count(),
        '70-79% (Good)': SupabaseMatch.objects.filter(harmonic_mean__gte=0.7, harmonic_mean__lt=0.8).count(),
        '60-69% (Fair)': SupabaseMatch.objects.filter(harmonic_mean__gte=0.6, harmonic_mean__lt=0.7).count(),
        '50-59% (Weak)': SupabaseMatch.objects.filter(harmonic_mean__gte=0.5, harmonic_mean__lt=0.6).count(),
        'Below 50% (Poor)': SupabaseMatch.objects.filter(harmonic_mean__lt=0.5).count(),
    }

    for range_label, count in score_ranges.items():
        percentage = (count / total_matches * 100) if total_matches > 0 else 0
        print(f"  {range_label:25} {count:6} matches ({percentage:5.1f}%)")

    # Match status distribution
    print("\nMATCH STATUS DISTRIBUTION:")
    status_counts = SupabaseMatch.objects.values('status').annotate(count=Count('id')).order_by('-count')

    for item in status_counts:
        status = item['status'] or 'unknown'
        count = item['count']
        percentage = (count / total_matches * 100) if total_matches > 0 else 0
        print(f"  {status:15} {count:6} matches ({percentage:5.1f}%)")

    # High-quality actionable matches
    actionable = SupabaseMatch.objects.filter(
        harmonic_mean__gte=0.7,
        status='pending'
    ).count()

    print(f"\nACTIONABLE MATCHES (>70% score, pending status): {actionable:,}")


def analyze_activity():
    """Analyze recent activity in Supabase profiles"""
    print_header("📈 ACTIVITY ANALYSIS")

    total = SupabaseProfile.objects.count()

    # Activity in last X days
    now = timezone.now()
    activity_ranges = {
        'Last 7 days': SupabaseProfile.objects.filter(last_active_at__gte=now - timedelta(days=7)).count(),
        'Last 30 days': SupabaseProfile.objects.filter(last_active_at__gte=now - timedelta(days=30)).count(),
        'Last 90 days': SupabaseProfile.objects.filter(last_active_at__gte=now - timedelta(days=90)).count(),
        'Last 6 months': SupabaseProfile.objects.filter(last_active_at__gte=now - timedelta(days=180)).count(),
        'Last year': SupabaseProfile.objects.filter(last_active_at__gte=now - timedelta(days=365)).count(),
        'Over a year ago': SupabaseProfile.objects.filter(last_active_at__lt=now - timedelta(days=365)).count(),
        'No activity date': SupabaseProfile.objects.filter(last_active_at__isnull=True).count(),
    }

    print("RECENT ACTIVITY:")
    for period, count in activity_ranges.items():
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {period:20} {count:4} profiles ({percentage:5.1f}%)")

    # Status distribution
    print("\nSTATUS DISTRIBUTION:")
    status_counts = SupabaseProfile.objects.values('status').annotate(count=Count('id')).order_by('-count')

    for item in status_counts:
        status = item['status'] or 'unknown'
        count = item['count']
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {status:25} {count:4} profiles ({percentage:5.1f}%)")


def identify_enrichment_priorities():
    """Identify which profiles should be enriched first"""
    print_header("🎯 ENRICHMENT PRIORITIES")

    print("HIGH PRIORITY (enrich these first):\n")

    # Priority 1: High list size but missing email
    high_value_no_email = SupabaseProfile.objects.filter(
        (Q(email__isnull=True) | Q(email='')) &
        Q(list_size__gt=10000) &
        Q(status='Member')
    ).order_by('-list_size')[:20]

    print(f"1. High-value profiles missing email ({high_value_no_email.count()} total, showing top 20):")
    for i, profile in enumerate(high_value_no_email, 1):
        print(f"   {i:2}. {profile.name or 'Unknown':30} | List: {profile.list_size:>8,} | Company: {(profile.company or 'Unknown')[:30]}")

    # Priority 2: Has high match scores but missing seeking/offering
    matched_incomplete = SupabaseMatch.objects.filter(
        harmonic_mean__gte=0.7,
        status='pending'
    ).values_list('suggested_profile_id', flat=True)[:200]

    incomplete_profiles = SupabaseProfile.objects.filter(
        Q(id__in=matched_incomplete) &
        (Q(seeking__isnull=True) | Q(seeking='') | Q(offering__isnull=True) | Q(offering=''))
    )[:20]

    print(f"\n2. Profiles with high match scores but missing seeking/offering ({incomplete_profiles.count()} showing top 20):")
    for i, profile in enumerate(incomplete_profiles, 1):
        has_seeking = '✓' if profile.seeking else '✗'
        has_offering = '✓' if profile.offering else '✗'
        print(f"   {i:2}. {profile.name or 'Unknown':30} | Seeking: {has_seeking} | Offering: {has_offering}")

    # Priority 3: Recently active but missing contact info
    recently_active_incomplete = SupabaseProfile.objects.filter(
        Q(last_active_at__gte=timezone.now() - timedelta(days=90)) &
        (Q(email__isnull=True) | Q(email=''))
    )[:20]

    print(f"\n3. Recently active (90 days) but missing email ({recently_active_incomplete.count()} showing top 20):")
    for i, profile in enumerate(recently_active_incomplete, 1):
        days_ago = (timezone.now() - profile.last_active_at).days if profile.last_active_at else None
        activity = f"{days_ago} days ago" if days_ago is not None else "Unknown"
        print(f"   {i:2}. {profile.name or 'Unknown':30} | Last active: {activity:15} | Status: {profile.status or 'Unknown'}")


def generate_recommendations():
    """Generate actionable recommendations based on audit"""
    print_header("💡 RECOMMENDATIONS")

    total = SupabaseProfile.objects.count()
    tier1 = SupabaseProfile.objects.filter(
        email__isnull=False,
        seeking__isnull=False,
        offering__isnull=False,
        list_size__gt=0
    ).exclude(email='').exclude(seeking='').exclude(offering='').count()

    has_email = SupabaseProfile.objects.filter(email__isnull=False).exclude(email='').count()

    print("IMMEDIATE ACTIONS:\n")

    print(f"1. USE WHAT YOU HAVE ({tier1:,} profiles)")
    print(f"   → Export top 50-100 matches from Tier 1 profiles")
    print(f"   → Start test outreach to validate conversion rates")
    print(f"   → Track: deliverability, opens, responses, meetings")
    print(f"   → Expected: 20%+ open rate, 5%+ meeting booking\n")

    missing_email_high_value = SupabaseProfile.objects.filter(
        (Q(email__isnull=True) | Q(email='')) &
        Q(list_size__gt=10000)
    ).count()

    if missing_email_high_value > 0:
        print(f"2. ENRICH HIGH-VALUE GAPS ({missing_email_high_value} profiles)")
        print(f"   → Focus on list_size >10K missing email")
        print(f"   → Use Hunter.io or similar ($2-10 total)")
        print(f"   → Validate emails before outreach\n")

    missing_intel = SupabaseProfile.objects.filter(
        Q(email__isnull=False) &
        (Q(seeking__isnull=True) | Q(seeking='') | Q(offering__isnull=True) | Q(offering=''))
    ).exclude(email='').count()

    if missing_intel > 0:
        print(f"3. FILL BUSINESS INTELLIGENCE ({missing_intel} profiles)")
        print(f"   → Profiles with email but missing seeking/offering")
        print(f"   → Use website scraping (free) or targeted AI ($2-5)")
        print(f"   → Improves match quality significantly\n")

    print(f"4. VALIDATE BEFORE SCALING")
    print(f"   → Test outreach with 20 contacts first")
    print(f"   → Measure: deliverability (>90%), response rate (>8%)")
    print(f"   → Only add new contacts if Supabase data converts well")
    print(f"   → If metrics good → you have {has_email:,} contacts ready to use!\n")

    print("DON'T DO (yet):")
    print("  ❌ Don't buy bulk contact databases")
    print("  ❌ Don't enrich all profiles at once")
    print("  ❌ Don't add new contacts before testing existing ones")
    print(f"  ❌ Don't build complex infrastructure until conversion validated\n")

    estimated_cost = min(missing_email_high_value * 0.01, 10) + 5
    print(f"ESTIMATED COST TO GET STARTED: ${estimated_cost:.2f}")
    print(f"TIMELINE: 2-3 weeks to first outreach results")
    print(f"POTENTIAL VALUE: {has_email:,} enriched contacts worth $50K+ if purchased")


def main():
    """Run the complete audit"""
    print("\n" + "="*70)
    print("SUPABASE PROFILE AUDIT")
    print("Analyzing existing data before adding new contacts")
    print("="*70)

    try:
        analyze_data_completeness()
        analyze_quality_tiers()
        analyze_match_quality()
        analyze_activity()
        identify_enrichment_priorities()
        generate_recommendations()

        print("\n" + "="*70)
        print("✅ AUDIT COMPLETE")
        print("="*70)
        print("\nNext step: Review recommendations and decide:")
        print("  1. Start test outreach with existing high-quality profiles, OR")
        print("  2. Enrich priority gaps first, then test outreach")
        print("\n")

    except Exception as e:
        print(f"\n❌ Error during audit: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
