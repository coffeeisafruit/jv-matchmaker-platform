#!/usr/bin/env python3
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

# Profiles NOT yet Apollo-enriched
cur.execute("""
    SELECT name, email, company, website, linkedin,
           CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 'yes' ELSE 'no' END as has_wyd
    FROM profiles
    WHERE enrichment_metadata->'apollo_data' IS NULL
    ORDER BY name
    LIMIT 50
""")
rows = cur.fetchall()
print(f'Profiles NOT yet Apollo-enriched: {len(rows)}')
for r in rows:
    parts = [r['name']]
    if r.get('email'): parts.append(f"email={r['email'][:30]}")
    if r.get('company'): parts.append(f"co={r['company'][:30]}")
    if r.get('website'): parts.append('web')
    if r.get('linkedin'): parts.append('li')
    parts.append(f"enriched={r['has_wyd']}")
    print(f"  {' | '.join(parts)}")

# Profiles already Apollo-tried, missing email, but now have better anchor data
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE enrichment_metadata->'apollo_data' IS NOT NULL
      AND (email IS NULL OR email = '')
      AND company IS NOT NULL AND company != ''
      AND website IS NOT NULL AND website != ''
""")
print(f"\nAlready Apollo-tried, no email, have company+website: {cur.fetchone()['c']}")

cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE enrichment_metadata->'apollo_data' IS NOT NULL
      AND (email IS NULL OR email = '')
      AND linkedin IS NOT NULL AND linkedin != ''
""")
print(f"Already Apollo-tried, no email, have linkedin: {cur.fetchone()['c']}")

# How many credits might we use?
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE (email IS NULL OR email = '')
      OR (phone IS NULL OR phone = '')
      OR (linkedin IS NULL OR linkedin = '')
""")
print(f"\nProfiles with any Tier 1-2 gap: {cur.fetchone()['c']}")

cur.close()
conn.close()
