#!/usr/bin/env python3
"""Quick progress check for email scraping."""
import os, psycopg2
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM profiles WHERE email IS NOT NULL AND email != ''")
has_email = cur.fetchone()[0]

cur.execute("""SELECT COUNT(*) FROM profiles
    WHERE (email IS NULL OR email = '')
      AND website IS NOT NULL AND website != ''
      AND website LIKE 'http%%'
      AND name IS NOT NULL AND name != ''""")
remaining = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM profiles")
total = cur.fetchone()[0]

print(f"Emails: {has_email} (+{has_email - 902} new) | Remaining: {remaining} | {has_email*100/total:.1f}%")
conn.close()
