#!/usr/bin/env python3
"""Analyze which profiles need enrichment"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cursor = conn.cursor(cursor_factory=RealDictCursor)

print('PROFILES NEEDING ENRICHMENT')
print('=' * 70)
print()

# Total profiles
cursor.execute('SELECT COUNT(*) as total FROM profiles')
total = cursor.fetchone()['total']
print(f'Total profiles: {total:,}')
print()

# Profiles missing email
cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE email IS NULL OR email = ''")
missing_email = cursor.fetchone()['count']
print(f'Missing email: {missing_email:,} ({missing_email/total*100:.1f}%)')

# Profiles with email
cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE email IS NOT NULL AND email != ''")
has_email = cursor.fetchone()['count']
print(f'Has email: {has_email:,} ({has_email/total*100:.1f}%)')
print()

# Profiles without enrichment metadata
cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE enrichment_metadata::text = '{}'")
no_metadata = cursor.fetchone()['count']
print(f'No enrichment metadata: {no_metadata:,} ({no_metadata/total*100:.1f}%)')

# Profiles with enrichment metadata
cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE enrichment_metadata::text != '{}'")
has_metadata = cursor.fetchone()['count']
print(f'Has enrichment metadata: {has_metadata:,} ({has_metadata/total*100:.1f}%)')
print()

# Low confidence profiles
cursor.execute('SELECT COUNT(*) as count FROM profiles WHERE profile_confidence < 0.5 AND profile_confidence > 0')
low_confidence = cursor.fetchone()['count']
print(f'Low confidence (< 0.5): {low_confidence:,}')

# High confidence profiles
cursor.execute('SELECT COUNT(*) as count FROM profiles WHERE profile_confidence >= 0.8')
high_confidence = cursor.fetchone()['count']
print(f'High confidence (>= 0.8): {high_confidence:,}')
print()

# Profiles missing key fields
print('Missing key fields:')
cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE seeking IS NULL OR seeking = ''")
print(f'  - Seeking: {cursor.fetchone()["count"]:,}')

cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE offering IS NULL OR offering = ''")
print(f'  - Offering: {cursor.fetchone()["count"]:,}')

cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE niche IS NULL OR niche = ''")
print(f'  - Niche: {cursor.fetchone()["count"]:,}')

cursor.execute("SELECT COUNT(*) as count FROM profiles WHERE linkedin IS NULL OR linkedin = ''")
print(f'  - LinkedIn: {cursor.fetchone()["count"]:,}')
print()

# Top enrichment opportunities (high list size, missing email)
print('TOP 10 ENRICHMENT OPPORTUNITIES:')
print('(High list size but missing email)')
print('-' * 70)
cursor.execute("""
    SELECT name, company, list_size, niche
    FROM profiles
    WHERE (email IS NULL OR email = '')
      AND list_size > 10000
    ORDER BY list_size DESC
    LIMIT 10
""")

for i, row in enumerate(cursor.fetchall(), 1):
    niche = row.get('niche', 'N/A')
    if niche and niche != 'N/A':
        niche = niche[:40] + '...' if len(niche) > 40 else niche
    print(f'{i}. {row["name"]} ({row.get("company", "N/A")})')
    print(f'   List: {row["list_size"]:,} | Niche: {niche}')

cursor.close()
conn.close()
