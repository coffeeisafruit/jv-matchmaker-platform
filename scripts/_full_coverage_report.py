#!/usr/bin/env python3
"""Full database coverage report."""
import os, psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

cur.execute("SELECT COUNT(*) AS n FROM profiles")
total = cur.fetchone()['n']

print("=" * 65)
print("DATABASE COVERAGE REPORT")
print("=" * 65)
print(f"Total profiles: {total}")
print()

# Contact fields
fields = [
    ("email",            "email IS NOT NULL AND email != ''"),
    ("phone",            "phone IS NOT NULL AND phone != ''"),
    ("linkedin",         "linkedin IS NOT NULL AND linkedin != ''"),
    ("website",          "website IS NOT NULL AND website != ''"),
    ("booking_link",     "booking_link IS NOT NULL AND booking_link != ''"),
    ("secondary_emails", "secondary_emails IS NOT NULL AND array_length(secondary_emails, 1) > 0"),
]

print("CONTACT FIELDS")
print("-" * 65)
for name, where in fields:
    cur.execute(f"SELECT COUNT(*) AS n FROM profiles WHERE {where}")
    n = cur.fetchone()['n']
    pct = n * 100 / total
    bar = '#' * int(pct / 2)
    print(f"  {name:22s} {n:5d} ({pct:5.1f}%) {bar}")

# Profile/business fields
print()
print("PROFILE FIELDS")
print("-" * 65)
profile_fields = [
    ("name",                "name IS NOT NULL AND name != ''"),
    ("company",             "company IS NOT NULL AND company != ''"),
    ("bio/about",           "bio IS NOT NULL AND bio != ''"),
    ("niche",               "niche IS NOT NULL AND niche != ''"),
    ("list_size",           "list_size IS NOT NULL AND list_size > 0"),
    ("revenue_tier",        "revenue_tier IS NOT NULL AND revenue_tier != ''"),
    ("who_you_serve",       "who_you_serve IS NOT NULL AND who_you_serve != ''"),
    ("seeking",             "seeking IS NOT NULL AND seeking != ''"),
    ("signature_programs",  "signature_programs IS NOT NULL AND signature_programs != ''"),
    ("tags",                "tags IS NOT NULL AND array_length(tags, 1) > 0"),
    ("avatar_url",          "avatar_url IS NOT NULL AND avatar_url != ''"),
]

for name, where in profile_fields:
    cur.execute(f"SELECT COUNT(*) AS n FROM profiles WHERE {where}")
    n = cur.fetchone()['n']
    pct = n * 100 / total
    bar = '#' * int(pct / 2)
    print(f"  {name:22s} {n:5d} ({pct:5.1f}%) {bar}")

# Enrichment coverage
print()
print("ENRICHMENT STATUS")
print("-" * 65)
cur.execute("SELECT COUNT(*) AS n FROM profiles WHERE enrichment_metadata IS NOT NULL")
n = cur.fetchone()['n']
print(f"  Has enrichment_metadata: {n} ({n*100/total:.1f}%)")

cur.execute("SELECT COUNT(*) AS n FROM profiles WHERE last_enriched_at IS NOT NULL")
n = cur.fetchone()['n']
print(f"  Has last_enriched_at:    {n} ({n*100/total:.1f}%)")

# Email sources
print()
print("EMAIL SOURCES")
print("-" * 65)
cur.execute("""
    SELECT COALESCE(enrichment_metadata->'field_meta'->'email'->>'source', 'unknown') AS source,
           COUNT(*) AS n
    FROM profiles
    WHERE email IS NOT NULL AND email != ''
    GROUP BY source
    ORDER BY n DESC
""")
for r in cur.fetchall():
    print(f"  {r['source']:22s} {r['n']:5d}")

# Contactability tiers
print()
print("CONTACTABILITY TIERS")
print("-" * 65)
tiers = [
    ("T1: Has email",           "email IS NOT NULL AND email != ''"),
    ("T2: Phone (no email)",    "(email IS NULL OR email = '') AND phone IS NOT NULL AND phone != ''"),
    ("T3: Booking (no email/phone)", "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND booking_link IS NOT NULL AND booking_link != ''"),
    ("T4: LinkedIn only",       "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND linkedin IS NOT NULL AND linkedin != ''"),
    ("T5: Website only",        "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND (linkedin IS NULL OR linkedin = '') AND website IS NOT NULL AND website != ''"),
    ("T6: Unreachable",         "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND (linkedin IS NULL OR linkedin = '') AND (website IS NULL OR website = '')"),
]
for name, where in tiers:
    cur.execute(f"SELECT COUNT(*) AS n FROM profiles WHERE {where}")
    n = cur.fetchone()['n']
    pct = n * 100 / total
    bar = '#' * int(pct / 2)
    print(f"  {name:30s} {n:5d} ({pct:5.1f}%) {bar}")

# Completeness score
print()
print("COMPLETENESS SCORE")
print("-" * 65)
cur.execute("""
    SELECT
        ROUND(AVG(
            (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
            (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
            (CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) +
            (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
            (CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN list_size IS NOT NULL AND list_size > 0 THEN 1 ELSE 0 END)
        )::numeric / 10 * 100, 1) AS avg_completeness
    FROM profiles
""")
avg = cur.fetchone()['avg_completeness']
print(f"  Average field completeness: {avg}% (out of 10 key fields)")

# Fully enriched profiles (8+ of 10 fields)
cur.execute("""
    SELECT COUNT(*) AS n FROM profiles WHERE
        (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
        (CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) +
        (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
        (CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) +
        (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
        (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
        (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
        (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
        (CASE WHEN list_size IS NOT NULL AND list_size > 0 THEN 1 ELSE 0 END)
        >= 8
""")
n = cur.fetchone()['n']
print(f"  Profiles with 8+/10 fields:  {n} ({n*100/total:.1f}%)")

cur.execute("""
    SELECT COUNT(*) AS n FROM profiles WHERE
        (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
        (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
        (CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) +
        (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
        (CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) +
        (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
        (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
        (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
        (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
        (CASE WHEN list_size IS NOT NULL AND list_size > 0 THEN 1 ELSE 0 END)
        >= 5
""")
n = cur.fetchone()['n']
print(f"  Profiles with 5+/10 fields:  {n} ({n*100/total:.1f}%)")

conn.close()
