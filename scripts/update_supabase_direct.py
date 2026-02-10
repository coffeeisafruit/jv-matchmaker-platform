#!/usr/bin/env python3
"""
Update Supabase profiles directly using Supabase Python client
"""
import os
from supabase import create_client, Client

# Supabase credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ysvwwfqmbjbvqvxutvom.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ACCESS_TOKEN")

if not SUPABASE_KEY:
    print("❌ Error: SUPABASE_KEY or SUPABASE_ACCESS_TOKEN not found")
    exit(1)

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Email updates from Apollo.io enrichment
updates = [
    {
        'id': '706e20c9-93fb-4aa0-864e-0d11e82cd024',
        'name': 'Michelle Tennant',
        'email': 'michelle@wasabipublicity.com',
        'list_size': '7.4M'
    },
    {
        'id': '21456f26-d587-4e4a-a55c-0710ce3cb1d1',
        'name': 'Sharon Grossman',
        'email': 'sharon@piccallo.com',
        'list_size': '320K'
    },
    {
        'id': '82e422d6-151d-4d59-981a-4024834b6552',
        'name': 'Melisa Ruscsak',
        'email': 'mlruscsak.ceo@trientpress.com',
        'list_size': '258K'
    },
    {
        'id': '6e0c37c6-52f7-4de4-abbe-1c11cefd65d1',
        'name': 'Kimberly Crowe',
        'email': 'kimberly@speakersplayhouse.com',
        'list_size': '155K'
    }
]

print("=" * 70)
print("UPDATING SUPABASE PROFILES")
print("=" * 70)
print()

success_count = 0
error_count = 0

for update in updates:
    try:
        # Update the profile
        result = supabase.table('profiles').update({
            'email': update['email']
        }).eq('id', update['id']).execute()

        print(f"✅ {update['name']:30} | {update['email']:40} | {update['list_size']:>6}")
        success_count += 1

    except Exception as e:
        print(f"❌ {update['name']:30} | Error: {str(e)}")
        error_count += 1

print()
print("=" * 70)
print("UPDATE SUMMARY")
print("=" * 70)
print(f"Successful: {success_count}")
print(f"Errors:     {error_count}")
print()

if success_count > 0:
    print("✅ Supabase profiles updated successfully!")
    print()
    print("Next step: Re-run export_top_matches.py to get new actionable matches")
