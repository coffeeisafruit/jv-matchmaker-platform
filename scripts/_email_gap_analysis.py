#!/usr/bin/env python3
"""Analyze email gaps and available anchor data for email discovery."""
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

# Total missing email
cur.execute("SELECT COUNT(*) as c FROM profiles WHERE email IS NULL OR email = ''")
total_missing = cur.fetchone()['c']

cur.execute("SELECT COUNT(*) as c FROM profiles WHERE email IS NOT NULL AND email != ''")
total_has = cur.fetchone()['c']

cur.execute("SELECT COUNT(*) as c FROM profiles")
total = cur.fetchone()['c']

print(f"EMAIL GAP ANALYSIS")
print(f"{'='*60}")
print(f"Total profiles:    {total}")
print(f"Have email:        {total_has} ({total_has/total*100:.1f}%)")
print(f"Missing email:     {total_missing} ({total_missing/total*100:.1f}%)")

# Break down missing-email profiles by what anchors they have
cur.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN website IS NOT NULL AND website != '' AND website LIKE 'http%%' THEN 1 ELSE 0 END) as has_website,
        SUM(CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) as has_company,
        SUM(CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) as has_linkedin,
        SUM(CASE WHEN website IS NOT NULL AND website != '' AND website LIKE 'http%%'
                  AND company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) as has_website_and_company,
        SUM(CASE WHEN (website IS NULL OR website = '' OR website NOT LIKE 'http%%')
                  AND (company IS NULL OR company = '')
                  AND (linkedin IS NULL OR linkedin = '') THEN 1 ELSE 0 END) as name_only
    FROM profiles
    WHERE email IS NULL OR email = ''
""")
r = cur.fetchone()
print(f"\nMissing-email profiles by anchor data:")
print(f"  Has website:              {r['has_website']}")
print(f"  Has company:              {r['has_company']}")
print(f"  Has linkedin:             {r['has_linkedin']}")
print(f"  Has website + company:    {r['has_website_and_company']}")
print(f"  Name only (no anchors):   {r['name_only']}")

# Apollo status on missing-email profiles
cur.execute("""
    SELECT
        SUM(CASE WHEN enrichment_metadata->'apollo_data' IS NOT NULL THEN 1 ELSE 0 END) as apollo_tried,
        SUM(CASE WHEN enrichment_metadata->'apollo_data' IS NULL THEN 1 ELSE 0 END) as apollo_not_tried
    FROM profiles
    WHERE email IS NULL OR email = ''
""")
r = cur.fetchone()
print(f"\nApollo status (missing-email profiles):")
print(f"  Already tried Apollo:     {r['apollo_tried']}")
print(f"  Never tried Apollo:       {r['apollo_not_tried']}")

# Among Apollo-tried, no email, what anchors do they have now?
cur.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN website IS NOT NULL AND website != '' AND website LIKE 'http%%' THEN 1 ELSE 0 END) as has_website,
        SUM(CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) as has_company,
        SUM(CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) as has_linkedin
    FROM profiles
    WHERE (email IS NULL OR email = '')
      AND enrichment_metadata->'apollo_data' IS NOT NULL
""")
r = cur.fetchone()
print(f"\nApollo-tried, still no email ({r['total']} profiles):")
print(f"  Has website:    {r['has_website']}")
print(f"  Has company:    {r['has_company']}")
print(f"  Has linkedin:   {r['has_linkedin']}")

# What about profiles with websites - can we scrape contact pages?
cur.execute("""
    SELECT COUNT(*) as c
    FROM profiles
    WHERE (email IS NULL OR email = '')
      AND website IS NOT NULL AND website != '' AND website LIKE 'http%%'
""")
print(f"\nScrapeable (has website, no email): {cur.fetchone()['c']}")

# Sample of high-value targets (have website+company, no email)
cur.execute("""
    SELECT name, company, website, linkedin
    FROM profiles
    WHERE (email IS NULL OR email = '')
      AND website IS NOT NULL AND website != '' AND website LIKE 'http%%'
      AND company IS NOT NULL AND company != ''
    ORDER BY name
    LIMIT 15
""")
rows = cur.fetchall()
print(f"\nSample high-value targets (website + company, no email):")
for r in rows:
    li = ' | li' if r.get('linkedin') else ''
    print(f"  {r['name']:30s} {r['company'][:25]:25s} {r['website'][:40]}{li}")

# Domain extraction potential
cur.execute("""
    SELECT COUNT(*) as c
    FROM profiles
    WHERE (email IS NULL OR email = '')
      AND website IS NOT NULL AND website != ''
      AND website LIKE 'http%%'
      AND website NOT LIKE '%%linkedin.com%%'
      AND website NOT LIKE '%%facebook.com%%'
      AND website NOT LIKE '%%twitter.com%%'
      AND website NOT LIKE '%%instagram.com%%'
      AND website NOT LIKE '%%youtube.com%%'
      AND website NOT LIKE '%%speakerhub.com%%'
      AND website NOT LIKE '%%alignable.com%%'
      AND website NOT LIKE '%%medium.com%%'
      AND website NOT LIKE '%%udemy.com%%'
""")
print(f"\nOwn-domain websites (not social/3rd-party): {cur.fetchone()['c']}")

cur.close()
conn.close()
