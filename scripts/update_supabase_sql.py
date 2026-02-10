#!/usr/bin/env python3
"""
Update Supabase profiles using direct PostgreSQL connection
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Get database URL from environment
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("❌ Error: DATABASE_URL not found")
    exit(1)

# Email updates from Apollo.io enrichment
updates = [
    ('706e20c9-93fb-4aa0-864e-0d11e82cd024', 'Michelle Tennant', 'michelle@wasabipublicity.com', '7.4M'),
    ('21456f26-d587-4e4a-a55c-0710ce3cb1d1', 'Sharon Grossman', 'sharon@piccallo.com', '320K'),
    ('82e422d6-151d-4d59-981a-4024834b6552', 'Melisa Ruscsak', 'mlruscsak.ceo@trientpress.com', '258K'),
    ('6e0c37c6-52f7-4de4-abbe-1c11cefd65d1', 'Kimberly Crowe', 'kimberly@speakersplayhouse.com', '155K'),
]

print("=" * 70)
print("UPDATING SUPABASE PROFILES VIA PostgreSQL")
print("=" * 70)
print()

try:
    # Connect to database
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    success_count = 0
    error_count = 0

    for profile_id, name, email, list_size in updates:
        try:
            # Update the profile
            cursor.execute(
                "UPDATE profiles SET email = %s WHERE id = %s",
                (email, profile_id)
            )

            if cursor.rowcount > 0:
                print(f"✅ {name:30} | {email:40} | {list_size:>6}")
                success_count += 1
            else:
                print(f"⚠️  {name:30} | Profile not found in database")
                error_count += 1

        except Exception as e:
            print(f"❌ {name:30} | Error: {str(e)}")
            error_count += 1

    # Commit changes
    conn.commit()

    print()
    print("=" * 70)
    print("VERIFICATION - Updated Profiles:")
    print("=" * 70)
    print()

    # Verify updates
    profile_ids = [u[0] for u in updates]
    cursor.execute(
        """
        SELECT id, name, email, list_size
        FROM profiles
        WHERE id = ANY(%s)
        ORDER BY list_size DESC NULLS LAST
        """,
        (profile_ids,)
    )

    results = cursor.fetchall()
    for row in results:
        print(f"  {row['name']:30} | {row['email'] or '(no email)':40} | {row['list_size'] or 0:>10,}")

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
        print("Next steps:")
        print("  1. Re-run export_top_matches.py to get new actionable matches")
        print("  2. Continue enriching remaining profiles from Batch 3")

    # Close connection
    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ Database connection error: {str(e)}")
    exit(1)
