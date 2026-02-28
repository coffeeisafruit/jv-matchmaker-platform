#!/usr/bin/env python3
"""Check how many profiles have at least one contact method."""
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

cur.execute("SELECT COUNT(*) as c FROM profiles")
total = cur.fetchone()['c']

# Has at least one direct contact method (email, phone, booking_link)
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE (email IS NOT NULL AND email != '')
       OR (phone IS NOT NULL AND phone != '')
       OR (booking_link IS NOT NULL AND booking_link != '')
""")
has_direct = cur.fetchone()['c']

# Has at least one reachable channel (email, phone, linkedin, booking_link, website)
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE (email IS NOT NULL AND email != '')
       OR (phone IS NOT NULL AND phone != '')
       OR (booking_link IS NOT NULL AND booking_link != '')
       OR (linkedin IS NOT NULL AND linkedin != '')
       OR (website IS NOT NULL AND website != '' AND website LIKE 'http%%')
""")
has_any = cur.fetchone()['c']

# Completely unreachable
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE (email IS NULL OR email = '')
      AND (phone IS NULL OR phone = '')
      AND (booking_link IS NULL OR booking_link = '')
      AND (linkedin IS NULL OR linkedin = '')
      AND (website IS NULL OR website = '' OR website NOT LIKE 'http%%')
""")
unreachable = cur.fetchone()['c']

print(f"CONTACTABILITY REPORT")
print(f"{'='*60}")
print(f"Total profiles:                    {total}")
print(f"")
print(f"Has email:                         {858:5d} ({858/total*100:.1f}%)")

cur.execute("SELECT COUNT(*) as c FROM profiles WHERE phone IS NOT NULL AND phone != ''")
phones = cur.fetchone()['c']
print(f"Has phone:                         {phones:5d} ({phones/total*100:.1f}%)")

cur.execute("SELECT COUNT(*) as c FROM profiles WHERE linkedin IS NOT NULL AND linkedin != ''")
li = cur.fetchone()['c']
print(f"Has LinkedIn:                      {li:5d} ({li/total*100:.1f}%)")

cur.execute("SELECT COUNT(*) as c FROM profiles WHERE booking_link IS NOT NULL AND booking_link != ''")
bl = cur.fetchone()['c']
print(f"Has booking link:                  {bl:5d} ({bl/total*100:.1f}%)")

cur.execute("SELECT COUNT(*) as c FROM profiles WHERE website IS NOT NULL AND website != '' AND website LIKE 'http%%'")
ws = cur.fetchone()['c']
print(f"Has website:                       {ws:5d} ({ws/total*100:.1f}%)")

print(f"")
print(f"Direct contact (email/phone/book): {has_direct:5d} ({has_direct/total*100:.1f}%)")
print(f"Any channel (incl. LI/website):    {has_any:5d} ({has_any/total*100:.1f}%)")
print(f"Completely unreachable:             {unreachable:5d} ({unreachable/total*100:.1f}%)")

# Tier breakdown
print(f"\n{'='*60}")
print(f"CONTACT TIER BREAKDOWN")
print(f"{'='*60}")

cur.execute("""
    SELECT
        CASE
            WHEN email IS NOT NULL AND email != '' THEN 'T1: Has email'
            WHEN phone IS NOT NULL AND phone != '' THEN 'T2: Phone only (no email)'
            WHEN booking_link IS NOT NULL AND booking_link != '' THEN 'T3: Booking link only'
            WHEN linkedin IS NOT NULL AND linkedin != '' THEN 'T4: LinkedIn only'
            WHEN website IS NOT NULL AND website != '' AND website LIKE 'http%%' THEN 'T5: Website only'
            ELSE 'T6: Name only - unreachable'
        END as tier,
        COUNT(*) as cnt
    FROM profiles
    GROUP BY tier
    ORDER BY tier
""")
for r in cur.fetchall():
    pct = r['cnt'] / total * 100
    bar = '#' * int(pct / 2)
    print(f"  {r['tier']:35s} {r['cnt']:5d} ({pct:5.1f}%) {bar}")

cur.close()
conn.close()
