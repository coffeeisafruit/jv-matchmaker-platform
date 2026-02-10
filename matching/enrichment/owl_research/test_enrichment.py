"""
Test script for profile enrichment using Claude Agent SDK.

Tests VERIFIED data extraction with source citations.
"""

import asyncio
import logging
import os
import sys

# Add project root to path for Django settings
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s'
)

from matching.enrichment.owl_research.agents.enrichment_agent import ProfileEnrichmentAgent


async def test_verified_enrichment():
    """Test enrichment with VERIFICATION - every field needs source citation."""

    # Sample profile to enrich
    test_profile = {
        "name": "Tony Robbins",
        "company": "Robbins Research International",
        "email": "",
        "linkedin_url": "",
    }

    print("=" * 70)
    print("VERIFIED PROFILE ENRICHMENT TEST")
    print("=" * 70)
    print(f"\nInput: {test_profile['name']} from {test_profile['company']}")
    print("\nRunning enrichment with MANDATORY source verification...")
    print()

    agent = ProfileEnrichmentAgent()

    result = await agent.enrich_profile(
        name=test_profile["name"],
        company=test_profile["company"],
        email=test_profile.get("email", ""),
        linkedin_url=test_profile.get("linkedin_url", ""),
    )

    print("=" * 70)

    if result.error:
        print(f"ERROR: {result.error}")
        return False

    if not result.enriched:
        print("No enriched data returned")
        return False

    e = result.enriched

    # Show VERIFICATION REPORT
    print("\n" + e.get_verification_report())

    # Show verified fields with their sources
    print("\n" + "=" * 70)
    print("VERIFIED DATA WITH SOURCES:")
    print("=" * 70)

    def show_verified_field(name: str, field):
        if field.is_verified():
            print(f"\n✓ {name}: {field.value}")
            print(f"  SOURCE: \"{field.source_quote[:80]}...\"")
            print(f"  URL: {field.source_url}")
        else:
            print(f"\n✗ {name}: [NOT VERIFIED - no source citation]")

    def show_verified_list(name: str, field):
        if field.is_verified():
            print(f"\n✓ {name}: {', '.join(field.values)}")
            print(f"  SOURCE: \"{field.source_quote[:80]}...\"")
            print(f"  URL: {field.source_url}")
        else:
            print(f"\n✗ {name}: [NOT VERIFIED - no source citation]")

    show_verified_field("Full Name", e.full_name)
    show_verified_field("Title", e.title)
    show_verified_field("Company Name", e.company.name)
    show_verified_field("Company Website", e.company.website)
    show_verified_field("Company Description", e.company.description)
    show_verified_list("Offerings", e.offerings)
    show_verified_field("Ideal Customer", e.ideal_customer.description)
    show_verified_list("Partnership Types", e.seeking.partnership_types)
    show_verified_field("LinkedIn URL", e.linkedin_url)

    # Show JV Matcher format (only verified data)
    print("\n" + "=" * 70)
    print("JV MATCHER FORMAT (VERIFIED DATA ONLY):")
    print("=" * 70)

    jv_data = result.to_jv_matcher_format()
    for key, value in jv_data.items():
        if not key.startswith('_'):
            print(f"\n{key}:")
            if isinstance(value, str) and len(value) > 100:
                print(f"  {value[:100]}...")
            else:
                print(f"  {value}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY:")
    print("=" * 70)
    print(f"Verified Fields: {e.get_verified_field_count()}/9")
    print(f"Overall Confidence: {e.overall_confidence:.2%}")
    print(f"Verification Summary: {e.verification_summary}")
    print(f"Total Sources: {len(e.all_sources)}")

    stats = agent.get_stats()
    print(f"\nSearch Stats:")
    print(f"  Total Searches: {stats['total_searches']}")
    print(f"  Tavily (paid): {stats['tavily_searches']}")
    print(f"  DuckDuckGo (free): {stats['free_searches']}")

    return e.get_verified_field_count() >= 3  # Pass if at least 3 fields verified


if __name__ == "__main__":
    success = asyncio.run(test_verified_enrichment())
    print("\n" + "=" * 70)
    print(f"Test {'PASSED' if success else 'FAILED'}")
    print("=" * 70)
