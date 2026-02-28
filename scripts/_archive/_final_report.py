#!/usr/bin/env python3
"""Final fill rate report with before/after comparison."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

# Before values (from earlier analysis)
BEFORE = {
    'name': 3577, 'email': 852, 'phone': 1232, 'company': 2612,
    'website': 2375, 'linkedin': 1705,
    'what_you_do': 2869, 'who_you_serve': 2762, 'seeking': 2715,
    'offering': 2635, 'bio': 2834, 'niche': 2619,
    'service_provided': 2765, 'business_focus': 3035, 'network_role': 2784,
    'signature_programs': 951, 'current_projects': 309, 'booking_link': 340,
    'audience_type': 2891, 'business_size': 2069, 'revenue_tier': 502,
    'list_size': 1293, 'social_reach': 1000, 'tags': 3042,
    'content_platforms': 1998,
}

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

cur.execute('SELECT COUNT(*) as total FROM profiles')
total = cur.fetchone()['total']

fields = [
    'name', 'email', 'phone', 'company', 'website', 'linkedin',
    'what_you_do', 'who_you_serve', 'seeking', 'offering', 'bio',
    'niche', 'service_provided', 'business_focus', 'network_role',
    'signature_programs', 'current_projects', 'booking_link',
    'audience_type', 'business_size', 'revenue_tier',
    'list_size', 'social_reach', 'tags',
]

print(f"Total profiles: {total}")
print(f"\n{'='*75}")
print(f"{'FIELD':22s} {'BEFORE':>8s} {'AFTER':>8s} {'DELTA':>8s} {'FILL%':>7s}")
print(f"{'='*75}")

total_delta = 0
for f in fields:
    if f in ('list_size', 'social_reach'):
        cur.execute(f'SELECT COUNT(*) as c FROM profiles WHERE {f} IS NOT NULL AND {f} > 0')
    elif f == 'tags':
        cur.execute(f"SELECT COUNT(*) as c FROM profiles WHERE {f} IS NOT NULL AND array_length({f}, 1) > 0")
    else:
        cur.execute(f"SELECT COUNT(*) as c FROM profiles WHERE {f} IS NOT NULL AND {f} != ''")
    after = cur.fetchone()['c']
    before = BEFORE.get(f, 0)
    delta = after - before
    pct = after / total * 100
    delta_str = f"+{delta}" if delta > 0 else str(delta)
    marker = " **" if delta > 10 else ""
    print(f"  {f:20s} {before:7d} {after:7d} {delta_str:>7s} {pct:6.1f}%{marker}")
    total_delta += delta

# Content platforms
cur.execute("SELECT COUNT(*) as c FROM profiles WHERE content_platforms IS NOT NULL AND content_platforms != '{}'::jsonb")
cp_after = cur.fetchone()['c']
cp_before = BEFORE.get('content_platforms', 0)
delta = cp_after - cp_before
pct = cp_after / total * 100
print(f"  {'content_platforms':20s} {cp_before:7d} {cp_after:7d} {'+' + str(delta) if delta > 0 else str(delta):>7s} {pct:6.1f}%")

print(f"\n{'='*75}")
print(f"  Total new field values written: {total_delta}")

# Matching readiness
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE (
            (CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN offering IS NOT NULL AND offering != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN tags IS NOT NULL AND array_length(tags, 1) > 0 THEN 1 ELSE 0 END)
        ) >= 1) as at_least_1,
        COUNT(*) FILTER (WHERE (
            (CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN offering IS NOT NULL AND offering != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN tags IS NOT NULL AND array_length(tags, 1) > 0 THEN 1 ELSE 0 END)
        ) >= 3) as at_least_3,
        COUNT(*) FILTER (WHERE (
            (CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN offering IS NOT NULL AND offering != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN tags IS NOT NULL AND array_length(tags, 1) > 0 THEN 1 ELSE 0 END)
        ) >= 5) as at_least_5,
        COUNT(*) FILTER (WHERE (
            (CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN offering IS NOT NULL AND offering != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN tags IS NOT NULL AND array_length(tags, 1) > 0 THEN 1 ELSE 0 END)
        ) = 7) as all_7
    FROM profiles
""")
r = cur.fetchone()

print(f"\n{'='*75}")
print("MATCHING READINESS (7 key fields)")
print(f"{'='*75}")
readiness_before = {
    'at_least_1': 3049, 'at_least_3': 2924, 'at_least_5': 2700, 'all_7': 2346
}
for label, key in [('1+ key fields', 'at_least_1'), ('3+ key fields', 'at_least_3'),
                    ('5+ key fields', 'at_least_5'), ('All 7 key fields', 'all_7')]:
    before = readiness_before[key]
    after = r[key]
    delta = after - before
    pct = after / total * 100
    print(f"  {label:22s} {before:5d} → {after:5d}  (+{delta:3d})  {pct:5.1f}%")

# Bare profiles
cur.execute("""
    SELECT COUNT(*) as c FROM profiles
    WHERE (what_you_do IS NULL OR what_you_do = '')
      AND (who_you_serve IS NULL OR who_you_serve = '')
      AND (niche IS NULL OR niche = '')
      AND (bio IS NULL OR bio = '')
      AND (offering IS NULL OR offering = '')
      AND (seeking IS NULL OR seeking = '')
""")
bare = cur.fetchone()['c']
print(f"\n  Bare profiles (0 key fields): 530 → {bare}  (-{530 - bare})")
print(f"  Remaining bare: {bare} ({bare/total*100:.1f}%) — mostly name-only, no enrichment anchor")

cur.close()
conn.close()
