#!/usr/bin/env python3
"""
Enrich Profiles Using Apollo.io API

Automatically finds emails and contact info for high-value targets.

Usage:
  python scripts/enrich_with_apollo.py --api-key YOUR_KEY --batch enrichment_batches/batch3_has_company.csv

Requirements:
  - Apollo.io API key (get from https://app.apollo.io/#/settings/integrations/api)
  - Free tier: 50 credits/month
  - Paid tier: 10,000+ credits/month
"""

import os
import sys
import django
import csv
import requests
import time
from typing import Dict, List, Optional

# Setup Django environment
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from matching.models import SupabaseProfile


class ApolloEnricher:
    """Apollo.io API client for contact enrichment"""

    BASE_URL = "https://api.apollo.io/api/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'accept': 'application/json',
            'x-api-key': api_key  # Correct header per Apollo docs
        })

    def match_person(
        self,
        name: str,
        company: str,
        domain: Optional[str] = None,
        reveal_email: bool = True,
        reveal_phone: bool = False
    ) -> Dict:
        """
        Find contact information using Apollo.io People Match API

        Args:
            name: Full name (e.g., "Michelle Tennant")
            company: Company name (e.g., "Wasabi Publicity")
            domain: Company domain (optional, e.g., "wasabipublicity.com")
            reveal_email: Whether to use credit to reveal email
            reveal_phone: Whether to use credit to reveal phone

        Returns:
            Dictionary with contact information
        """
        # Split name into first/last
        name_parts = name.strip().split(' ', 1)
        first_name = name_parts[0] if name_parts else name
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        payload = {
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": company,
            "reveal_personal_emails": reveal_email,
            "reveal_phone_number": reveal_phone
        }

        if domain:
            payload["domain"] = domain

        url = f"{self.BASE_URL}/people/match"

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                'error': str(e),
                'status_code': getattr(e.response, 'status_code', None)
            }

    def bulk_match_people(
        self,
        people: List[Dict],
        reveal_email: bool = True,
        reveal_phone: bool = False
    ) -> List[Dict]:
        """
        Enrich up to 10 people at once (10x faster than single match)

        Args:
            people: List of dicts with 'name' and 'company' keys
            reveal_email: Whether to use credits to reveal emails
            reveal_phone: Whether to use credits to reveal phones

        Returns:
            List of enriched contact information dicts
        """
        details = []
        for person in people[:10]:  # Max 10 per request
            name_parts = person['name'].strip().split(' ', 1)
            first_name = name_parts[0] if name_parts else person['name']
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            detail = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": person['company']
            }

            if person.get('domain'):
                detail['domain'] = person['domain']

            details.append(detail)

        payload = {
            "details": details,
            "reveal_personal_emails": reveal_email,
            "reveal_phone_number": reveal_phone
        }

        url = f"{self.BASE_URL}/people/bulk_match"

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            result = response.json()

            # Debug: print response structure
            print(f"\nDEBUG - API Response keys: {result.keys()}")

            # Extract matches from bulk response
            matches = result.get('matches', [])

            if not matches:
                print(f"DEBUG - No matches in response. Full response: {result}")
                # Return empty dicts for each person
                return [{'error': 'No match found', 'email': None, 'phone': None, 'linkedin': None} for _ in people]

            # Matches are already person objects, don't wrap them
            return [self.extract_contact_info(match) if match else {'error': 'Empty match'} for match in matches]

        except requests.exceptions.RequestException as e:
            print(f"DEBUG - Request exception: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"DEBUG - Response text: {e.response.text}")
            return [{'error': str(e)} for _ in people]

    def extract_contact_info(self, api_response: Dict) -> Dict:
        """Extract relevant contact info from Apollo API response"""
        # Handle None or empty response
        if not api_response or api_response is None:
            return {
                'email': None,
                'phone': None,
                'linkedin': None,
                'title': None,
                'confidence': 'not_found',
                'error': 'Empty response'
            }

        if 'error' in api_response:
            return {
                'email': None,
                'phone': None,
                'linkedin': None,
                'title': None,
                'confidence': 'error',
                'error': api_response['error']
            }

        # Handle both single match and bulk match response formats
        person = api_response.get('person') if 'person' in api_response else api_response

        if not person or person is None:
            return {
                'email': None,
                'phone': None,
                'linkedin': None,
                'title': None,
                'confidence': 'not_found',
                'error': 'No person data in response'
            }

        return {
            'email': person.get('email'),
            'phone': person.get('phone_numbers', [{}])[0].get('raw_number') if person.get('phone_numbers') else None,
            'linkedin': person.get('linkedin_url'),
            'title': person.get('title'),
            'city': person.get('city'),
            'state': person.get('state'),
            'confidence': person.get('email_status', 'unknown'),
            'apollo_id': person.get('id'),
            'organization': person.get('organization', {}).get('name'),
            'credits_used': 1  # Estimate 1 credit per person
        }


def enrich_batch_from_csv(
    input_file: str,
    output_file: str,
    api_key: str,
    max_profiles: Optional[int] = None,
    delay_seconds: float = 0.5,
    dry_run: bool = False,
    use_bulk: bool = True
) -> Dict:
    """
    Enrich profiles from batch CSV using Apollo.io

    Args:
        input_file: Path to batch CSV (e.g., batch3_has_company.csv)
        output_file: Path to save enriched results
        api_key: Apollo.io API key
        max_profiles: Maximum number to enrich (None = all)
        delay_seconds: Delay between API calls to respect rate limits
        dry_run: If True, show what would be done without making API calls

    Returns:
        Dictionary with enrichment statistics
    """
    enricher = ApolloEnricher(api_key)

    stats = {
        'total': 0,
        'enriched': 0,
        'email_found': 0,
        'phone_found': 0,
        'linkedin_found': 0,
        'errors': 0,
        'skipped': 0,
        'credits_used': 0
    }

    results = []

    print(f"\n{'='*70}")
    print(f"APOLLO.IO ENRICHMENT")
    print(f"{'='*70}\n")
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Limit:  {max_profiles or 'All'}")
    print(f"Dry Run: {'Yes (no API calls)' if dry_run else 'No (will use credits)'}\n")

    # Read batch CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        profiles = list(reader)

    # Limit profiles if specified
    if max_profiles:
        profiles = profiles[:max_profiles]

    print(f"Found {len(profiles)} profiles in batch")
    print(f"Mode: {'BULK (10 at a time)' if use_bulk else 'SINGLE (1 at a time)'}\n")
    print(f"{'='*70}")
    print("ENRICHMENT PROGRESS")
    print(f"{'='*70}\n")

    if use_bulk and not dry_run:
        # BULK PROCESSING (10 at a time, 10x faster!)
        batch_size = 10
        for batch_start in range(0, len(profiles), batch_size):
            batch_profiles = profiles[batch_start:batch_start + batch_size]
            batch_num = (batch_start // batch_size) + 1

            # Prepare batch for API
            prepared_batch = []
            for profile in batch_profiles:
                name = profile.get('name', '').strip()
                company = profile.get('company', '').strip()

                if not name or not company:
                    stats['skipped'] += 1
                    continue

                # Extract domain from website
                website = profile.get('website', '').strip()
                domain = None
                if website:
                    domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                    if domain.startswith('www.'):
                        domain = domain[4:]

                prepared_batch.append({
                    'name': name,
                    'company': company,
                    'domain': domain,
                    'profile': profile  # Keep original for reference
                })

            if not prepared_batch:
                continue

            stats['total'] += len(prepared_batch)

            # Make bulk API call
            print(f"Batch {batch_num} ({len(prepared_batch)} profiles): ", end='', flush=True)

            contact_infos = enricher.bulk_match_people(
                people=prepared_batch,
                reveal_email=True,
                reveal_phone=False
            )

            # Process results
            for i, (prep, contact_info) in enumerate(zip(prepared_batch, contact_infos)):
                profile = prep['profile']
                name = prep['name']
                company = prep['company']
                list_size = profile.get('list_size', 0)

                if contact_info.get('error'):
                    stats['errors'] += 1
                elif contact_info.get('email'):
                    stats['email_found'] += 1
                    if contact_info.get('phone'):
                        stats['phone_found'] += 1
                    if contact_info.get('linkedin'):
                        stats['linkedin_found'] += 1

                stats['enriched'] += 1
                stats['credits_used'] += contact_info.get('credits_used', 1)

                # Save result
                results.append({
                    'profile_id': profile.get('profile_id'),
                    'name': name,
                    'company': company,
                    'list_size': list_size,
                    'email': contact_info.get('email', ''),
                    'phone': contact_info.get('phone', ''),
                    'linkedin': contact_info.get('linkedin', ''),
                    'title': contact_info.get('title', ''),
                    'confidence': contact_info.get('confidence', ''),
                    'apollo_id': contact_info.get('apollo_id', ''),
                    'source': 'Apollo.io Bulk',
                    'notes': f"Batch {batch_num}"
                })

            # Show summary for batch
            emails_in_batch = sum(1 for c in contact_infos if c.get('email'))
            print(f"✅ {emails_in_batch}/{len(prepared_batch)} emails found")

            # Respect rate limits between batches
            if batch_start + batch_size < len(profiles):
                time.sleep(delay_seconds)

    else:
        # SINGLE PROCESSING (original, slower method) or DRY RUN
        for i, profile in enumerate(profiles, 1):
            stats['total'] += 1

            name = profile.get('name', '').strip()
            company = profile.get('company', '').strip()
            list_size = profile.get('list_size', 0)

            if not name or not company:
                print(f"{i:3}. ❌ {name or 'Unknown':30} | Missing name or company")
                stats['skipped'] += 1
                continue

            # Extract domain from website if available
            website = profile.get('website', '').strip()
            domain = None
            if website:
                # Extract domain from URL
                domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                if domain.startswith('www.'):
                    domain = domain[4:]

            if dry_run:
                print(f"{i:3}. 🔍 {name:30} @ {company:25} | List: {int(float(list_size or 0)):>8,} | Would query Apollo")
                stats['enriched'] += 1
            else:
                # Make API call
                print(f"{i:3}. 🔍 {name:30} @ {company:25} | List: {int(float(list_size or 0)):>8,} | Querying...", end='', flush=True)

                api_response = enricher.match_person(
                    name=name,
                    company=company,
                    domain=domain,
                    reveal_email=True,
                    reveal_phone=False  # Save credits - only get email
                )

                contact_info = enricher.extract_contact_info(api_response)

                if contact_info.get('error'):
                    print(f" ❌ Error: {contact_info['error']}")
                    stats['errors'] += 1
                elif contact_info.get('email'):
                    email = contact_info['email']
                    confidence = contact_info.get('confidence', 'unknown')
                    print(f" ✅ {email:35} | {confidence}")
                    stats['enriched'] += 1
                    stats['email_found'] += 1

                    if contact_info.get('phone'):
                        stats['phone_found'] += 1
                    if contact_info.get('linkedin'):
                        stats['linkedin_found'] += 1

                    stats['credits_used'] += contact_info.get('credits_used', 1)
                else:
                    print(f" ⚠️  No email found")
                    stats['enriched'] += 1

                # Save result
                results.append({
                    'profile_id': profile.get('profile_id'),
                    'name': name,
                    'company': company,
                    'list_size': list_size,
                    'email': contact_info.get('email', ''),
                    'phone': contact_info.get('phone', ''),
                    'linkedin': contact_info.get('linkedin', ''),
                    'title': contact_info.get('title', ''),
                    'confidence': contact_info.get('confidence', ''),
                    'apollo_id': contact_info.get('apollo_id', ''),
                    'source': 'Apollo.io',
                    'notes': f"Credits used: {contact_info.get('credits_used', 0)}"
                })

                # Respect rate limits
                if i < len(profiles):
                    time.sleep(delay_seconds)

    # Write results to CSV
    if results and not dry_run:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['profile_id', 'name', 'company', 'list_size', 'email', 'phone',
                         'linkedin', 'title', 'confidence', 'apollo_id', 'source', 'notes']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n✅ Results saved to {output_file}")

    # Print summary
    print(f"\n{'='*70}")
    print("ENRICHMENT SUMMARY")
    print(f"{'='*70}\n")

    print(f"Total Profiles:      {stats['total']}")
    print(f"Enriched:            {stats['enriched']}")
    print(f"Emails Found:        {stats['email_found']} ({stats['email_found']/stats['total']*100:.1f}%)" if stats['total'] > 0 else "Emails Found:        0")
    print(f"Phones Found:        {stats['phone_found']}")
    print(f"LinkedIn Found:      {stats['linkedin_found']}")
    print(f"Errors:              {stats['errors']}")
    print(f"Skipped:             {stats['skipped']}")
    print(f"Credits Used:        {stats['credits_used']}")

    if stats['credits_used'] > 0:
        print(f"\n💰 Cost: ~${stats['credits_used'] * 0.10:.2f} (estimated at $0.10/credit)")

    return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Enrich profiles using Apollo.io API')
    parser.add_argument('--api-key', required=True, help='Apollo.io API key')
    parser.add_argument('--batch', required=True, help='Batch CSV file to enrich')
    parser.add_argument('--output', help='Output CSV file (default: enriched_apollo.csv)')
    parser.add_argument('--limit', type=int, help='Maximum profiles to enrich')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between API calls (seconds)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without API calls')
    parser.add_argument('--no-bulk', action='store_true', help='Disable bulk API (process one at a time)')

    args = parser.parse_args()

    output_file = args.output or 'enriched_apollo.csv'

    try:
        stats = enrich_batch_from_csv(
            input_file=args.batch,
            output_file=output_file,
            api_key=args.api_key,
            max_profiles=args.limit,
            delay_seconds=args.delay,
            dry_run=args.dry_run,
            use_bulk=not args.no_bulk
        )

        print(f"\n{'='*70}")
        print("✅ ENRICHMENT COMPLETE")
        print(f"{'='*70}\n")

        if not args.dry_run and stats['email_found'] > 0:
            print("Next steps:")
            print(f"  1. Review {output_file}")
            print(f"  2. python scripts/update_enriched_emails.py --input {output_file} --dry-run")
            print(f"  3. Update Supabase with SQL or Admin UI")
            print(f"  4. Re-run export_top_matches.py to get new matches\n")

    except FileNotFoundError:
        print(f"\n❌ Error: Batch file not found: {args.batch}")
        print(f"\nAvailable batches:")
        print(f"  - enrichment_batches/batch1_has_website.csv")
        print(f"  - enrichment_batches/batch3_has_company.csv")
        print(f"  - enrichment_batches/batch4_minimal_data.csv")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
