#!/usr/bin/env python3
"""Verify scraped data landed in Supabase."""
import os, psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

cur.execute("SELECT COUNT(*) AS n FROM profiles WHERE email IS NOT NULL AND email != ''")
print(f"Profiles with email:           {cur.fetchone()['n']}")

cur.execute("SELECT COUNT(*) AS n FROM profiles WHERE phone IS NOT NULL AND phone != ''")
print(f"Profiles with phone:           {cur.fetchone()['n']}")

cur.execute("SELECT COUNT(*) AS n FROM profiles WHERE secondary_emails IS NOT NULL AND array_length(secondary_emails, 1) > 0")
print(f"Profiles with secondary emails:{cur.fetchone()['n']}")

# Show recent website_scrape entries
cur.execute("""
    SELECT name, email, phone,
           enrichment_metadata->'field_meta'->'email'->>'source' AS email_source,
           updated_at
    FROM profiles
    WHERE enrichment_metadata->'field_meta'->'email'->>'source' = 'website_scrape'
    ORDER BY updated_at DESC
    LIMIT 10
""")
rows = cur.fetchall()
print(f"\nRecent website_scrape entries in Supabase ({len(rows)} shown):")
for r in rows:
    print(f"  {r['name']:35s} {(r['email'] or ''):30s} {(r['phone'] or ''):15s} {str(r['updated_at'])[:19]}")

# Count by source
cur.execute("""
    SELECT enrichment_metadata->'field_meta'->'email'->>'source' AS source, COUNT(*) AS n
    FROM profiles
    WHERE email IS NOT NULL AND email != ''
    GROUP BY source
    ORDER BY n DESC
""")
rows = cur.fetchall()
print(f"\nEmail sources breakdown:")
for r in rows:
    print(f"  {(r['source'] or 'unknown'):20s} {r['n']}")

conn.close()
