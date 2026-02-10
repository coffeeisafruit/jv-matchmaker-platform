#!/usr/bin/env python3
"""
Verify ALL Existing Emails with Truelist.io

With unlimited Truelist access, verify every email currently in the database:
- Add verification metadata to profiles with emails
- Flag invalid emails for re-enrichment
- Calculate new confidence scores based on verification

Usage:
    python scripts/verify_existing_emails.py --dry-run  # Preview
    python scripts/verify_existing_emails.py            # Execute
"""

import os
import sys
import asyncio
import aiohttp
import json
import time
from typing import Dict, List
from datetime import datetime

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from dotenv import load_dotenv

load_dotenv()


class EmailVerifier:
    """Truelist.io email verification with rate limiting"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.truelist.io/v1"
        self.rate_limit_delay = 0.1  # 10 requests/second
        self.last_request_time = 0

    async def verify_email(self, email: str, session: aiohttp.ClientSession) -> Dict:
        """Verify email through Truelist.io with rate limiting"""
        # Rate limiting
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)

        self.last_request_time = time.time()

        # Retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with session.get(
                    f"{self.base_url}/verify",
                    params={'email': email},
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'valid': data.get('valid', False),
                            'score': data.get('score', 0),
                            'status': data.get('status', 'unknown'),
                            'reason': data.get('reason', ''),
                            'provider': data.get('provider', '')
                        }
                    elif response.status == 429:
                        wait_time = (attempt + 1) * 2
                        print(f"  ⚠️  Rate limited, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return {
                            'valid': False,
                            'score': 0,
                            'status': 'error',
                            'reason': f'API error: {response.status}'
                        }
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return {
                    'valid': False,
                    'score': 0,
                    'status': 'error',
                    'reason': str(e)
                }

        return {
            'valid': False,
            'score': 0,
            'status': 'error',
            'reason': 'Max retries exceeded'
        }


class ExistingEmailVerifier:
    """Verify all existing emails in database"""

    def __init__(self, dry_run=False, batch_size=50):
        self.dry_run = dry_run
        self.batch_size = batch_size

        truelist_api_key = os.environ.get('TRUELIST_API_KEY')
        if not truelist_api_key:
            raise ValueError("TRUELIST_API_KEY not found in .env")

        self.verifier = EmailVerifier(truelist_api_key)

        self.stats = {
            'total': 0,
            'verified_valid': 0,
            'verified_invalid': 0,
            'verification_errors': 0,
            'updated': 0,
            'time_taken': 0
        }

    def get_profiles_with_emails(self) -> List[Dict]:
        """Get all profiles that have emails"""
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT id, name, email, company, enrichment_metadata
            FROM profiles
            WHERE email IS NOT NULL
              AND email != ''
            ORDER BY list_size DESC NULLS LAST
        """

        cursor.execute(query)
        profiles = [dict(p) for p in cursor.fetchall()]

        cursor.close()
        conn.close()

        return profiles

    async def verify_batch(
        self,
        profiles: List[Dict],
        session: aiohttp.ClientSession
    ) -> List[tuple]:
        """Verify a batch of profiles"""
        tasks = [
            self.verify_profile(profile, session)
            for profile in profiles
        ]
        return await asyncio.gather(*tasks)

    async def verify_profile(
        self,
        profile: Dict,
        session: aiohttp.ClientSession
    ) -> tuple:
        """Verify single profile's email"""
        email = profile['email']
        profile_id = profile['id']

        if self.dry_run:
            return (profile_id, email, None)

        verification = await self.verifier.verify_email(email, session)
        return (profile_id, email, verification)

    async def run(self):
        """Run verification on all existing emails"""
        start_time = time.time()

        print("=" * 70)
        print("VERIFY ALL EXISTING EMAILS (Truelist.io)")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE VERIFICATION'}")
        print()

        # Get all profiles with emails
        print("Fetching profiles with emails...")
        profiles = self.get_profiles_with_emails()
        self.stats['total'] = len(profiles)

        print(f"Found {len(profiles)} profiles with emails to verify")
        print()

        if self.dry_run:
            print("DRY RUN - Would verify these emails:")
            for i, profile in enumerate(profiles[:10], 1):
                print(f"{i}. {profile['name']:40} | {profile['email']}")
            if len(profiles) > 10:
                print(f"... and {len(profiles) - 10} more")
            print()
            return

        # Verify in batches
        results = []
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=2)

        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(0, len(profiles), self.batch_size):
                batch = profiles[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(profiles) + self.batch_size - 1) // self.batch_size

                print(f"Batch {batch_num}/{total_batches} ({len(batch)} profiles)...", end=' ', flush=True)

                batch_results = await self.verify_batch(batch, session)

                valid_count = 0
                invalid_count = 0
                error_count = 0

                for profile_id, email, verification in batch_results:
                    if verification:
                        if verification['valid']:
                            self.stats['verified_valid'] += 1
                            valid_count += 1
                            results.append({
                                'profile_id': profile_id,
                                'email': email,
                                'verification': verification,
                                'action': 'mark_verified'
                            })
                        else:
                            self.stats['verified_invalid'] += 1
                            invalid_count += 1
                            results.append({
                                'profile_id': profile_id,
                                'email': email,
                                'verification': verification,
                                'action': 'mark_invalid'
                            })
                    else:
                        self.stats['verification_errors'] += 1
                        error_count += 1

                print(f"✅ {valid_count} valid | ❌ {invalid_count} invalid | ⚠️  {error_count} errors")

        # Update database
        if results:
            print("\nUpdating database...")
            await self.update_database(results)

        self.stats['time_taken'] = time.time() - start_time

        # Print summary
        print("\n" + "=" * 70)
        print("VERIFICATION SUMMARY")
        print("=" * 70)
        print(f"Total emails checked: {self.stats['total']}")
        print(f"Valid emails:         {self.stats['verified_valid']} ({self.stats['verified_valid']/self.stats['total']*100:.1f}%)")
        print(f"Invalid emails:       {self.stats['verified_invalid']} ({self.stats['verified_invalid']/self.stats['total']*100:.1f}%)")
        print(f"Verification errors:  {self.stats['verification_errors']}")
        print(f"Database updates:     {self.stats['updated']}")
        print(f"Time taken:           {self.stats['time_taken']:.1f}s ({self.stats['time_taken']/60:.1f} minutes)")
        print()
        print(f"💰 Truelist verifications: {self.stats['total']} (unlimited plan)")
        print()

        if self.stats['verified_invalid'] > 0:
            print(f"⚠️  Action needed: {self.stats['verified_invalid']} invalid emails found")
            print("   These profiles should be re-enriched")
            print()
            print("   Run this to re-enrich:")
            print(f"   python scripts/automated_enrichment_pipeline_verified.py \\")
            print(f"       --limit {self.stats['verified_invalid']} \\")
            print(f"       --priority has-website \\")
            print(f"       --max-apollo-credits {self.stats['verified_invalid'] // 3} \\")
            print(f"       --auto-consolidate")

    async def update_database(self, results: List[Dict]):
        """Update database with verification results"""
        from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

        scorer = ConfidenceScorer()
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cursor = conn.cursor()

        updates = []

        for result in results:
            profile_id = result['profile_id']
            email = result['email']
            verification = result['verification']
            action = result['action']

            if action == 'mark_verified':
                # Email is valid - add verification metadata
                email_metadata = {
                    'source': 'existing_verified',
                    'enriched_at': datetime.now().isoformat(),
                    'source_date': datetime.now().date().isoformat(),
                    'confidence': 0.85,  # High confidence for verified existing
                    'confidence_expires_at': scorer.calculate_expires_at('email', datetime.now()).isoformat(),
                    'verification_count': 1,
                    'verified': True,
                    'verification_service': 'truelist',
                    'verification_score': verification.get('score', 0),
                    'verification_status': verification.get('status', 'verified'),
                    'verified_at': datetime.now().isoformat()
                }

                updates.append((
                    email,  # Keep email
                    json.dumps({'email': email_metadata}),
                    0.85,
                    datetime.now(),
                    datetime.now(),
                    profile_id
                ))

            elif action == 'mark_invalid':
                # Email is invalid - clear it and flag for re-enrichment
                # Keep the old email in metadata for reference
                email_metadata = {
                    'source': 'existing_invalid',
                    'invalid_email': email,
                    'enriched_at': datetime.now().isoformat(),
                    'confidence': 0.0,
                    'verified': False,
                    'verification_service': 'truelist',
                    'verification_score': verification.get('score', 0),
                    'verification_status': verification.get('status', 'invalid'),
                    'verification_reason': verification.get('reason', ''),
                    'verified_at': datetime.now().isoformat(),
                    'needs_re_enrichment': True
                }

                updates.append((
                    None,  # Clear invalid email
                    json.dumps({'email': email_metadata}),
                    0.0,
                    datetime.now(),
                    datetime.now(),
                    profile_id
                ))

        # Batch execute
        execute_batch(cursor, """
            UPDATE profiles
            SET email = %s,
                enrichment_metadata = jsonb_set(
                    COALESCE(enrichment_metadata, '{}'::jsonb),
                    '{email}',
                    %s::jsonb
                ),
                profile_confidence = %s,
                last_enriched_at = %s,
                updated_at = %s
            WHERE id = %s
        """, updates)

        conn.commit()
        cursor.close()
        conn.close()

        self.stats['updated'] = len(updates)
        print(f"✅ Updated {len(updates)} profiles with verification data")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Verify all existing emails with Truelist.io')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for verification')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')

    args = parser.parse_args()

    verifier = ExistingEmailVerifier(
        dry_run=args.dry_run,
        batch_size=args.batch_size
    )

    asyncio.run(verifier.run())


if __name__ == '__main__':
    main()
