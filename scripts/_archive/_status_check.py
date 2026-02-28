#!/usr/bin/env python3
"""Quick status check on bare profiles and enrichment sources."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

# Bare profiles breakdown
cur.execute("""
    SELECT
        SUM(CASE WHEN (company IS NULL OR company = '') AND (website IS NULL OR website = '')
                  AND (linkedin IS NULL OR linkedin = '') AND (email IS NULL OR email = '')
            THEN 1 ELSE 0 END) as name_only,
        SUM(CASE WHEN website IS NOT NULL AND website != '' AND website LIKE 'http%%'
            THEN 1 ELSE 0 END) as has_website,
        SUM(CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) as has_company,
        SUM(CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) as has_linkedin,
        SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as has_email
    FROM profiles
    WHERE (what_you_do IS NULL OR what_you_do = '')
      AND (who_you_serve IS NULL OR who_you_serve = '')
      AND (niche IS NULL OR niche = '')
      AND (bio IS NULL OR bio = '')
      AND (offering IS NULL OR offering = '')
      AND (seeking IS NULL OR seeking = '')
""")
r = cur.fetchone()
print(f"517 Bare profiles breakdown:")
print(f"  Name only (no anchors): {r['name_only']}")
print(f"  Has website:            {r['has_website']}")
print(f"  Has company:            {r['has_company']}")
print(f"  Has linkedin:           {r['has_linkedin']}")
print(f"  Has email:              {r['has_email']}")

# Quality check on the re-inferred 130
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE enrichment_metadata->'field_meta'->'niche'->>'source' = 'ai_inference'
      AND enrichment_metadata->'field_meta'->'niche'->>'pipeline_version' = '3'
""")
print(f"\nAI-inferred (pipeline v3): {cur.fetchone()['c']}")

cur.close()
conn.close()
