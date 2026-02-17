#!/usr/bin/env python3
"""
Query live Supabase fill rates for all SupabaseProfile fields.
Outputs JSON with count and percentage for each field.
"""
import os
import sys
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

# All data fields on SupabaseProfile (excluding id, timestamps, auth_user_id)
TEXT_FIELDS = [
    'name', 'email', 'phone', 'company', 'website', 'linkedin',
    'avatar_url', 'booking_link', 'what_you_do', 'who_you_serve',
    'seeking', 'offering', 'niche', 'business_focus', 'service_provided',
    'signature_programs', 'current_projects', 'bio', 'notes',
    'status', 'role', 'audience_type', 'business_size',
    'revenue_tier', 'network_role',
]

# Integer fields (non-null AND > 0 counts as filled)
INT_FIELDS = [
    'list_size', 'social_reach', 'recommendation_pressure_30d',
]

# Float fields (non-null counts as filled)
FLOAT_FIELDS = [
    'profile_confidence', 'audience_engagement_score',
    'pagerank_score', 'degree_centrality', 'betweenness_centrality',
]

# JSON/Array fields (non-null AND not empty)
JSON_FIELDS = [
    'enrichment_metadata', 'jv_history', 'content_platforms',
]

ARRAY_FIELDS = [
    'tags', 'secondary_emails',
]

# Timestamp fields
TS_FIELDS = [
    'last_enriched_at', 'centrality_updated_at',
]

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Total count
cur.execute("SELECT COUNT(*) FROM profiles")
total = cur.fetchone()[0]

results = {'total_profiles': total, 'fields': {}}

# Text fields: NOT NULL AND != ''
for field in TEXT_FIELDS:
    cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {field} IS NOT NULL AND {field} != ''")
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

# Integer fields: NOT NULL AND > 0
for field in INT_FIELDS:
    cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {field} IS NOT NULL AND {field} > 0")
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

# Float fields: NOT NULL
for field in FLOAT_FIELDS:
    cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {field} IS NOT NULL")
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

# JSON fields: NOT NULL AND != '{}'::jsonb AND != '[]'::jsonb AND != 'null'::jsonb
for field in JSON_FIELDS:
    cur.execute(f"""
        SELECT COUNT(*) FROM profiles
        WHERE {field} IS NOT NULL
          AND {field}::text != '{{}}'
          AND {field}::text != '[]'
          AND {field}::text != 'null'
    """)
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

# Array fields: NOT NULL AND array_length > 0
for field in ARRAY_FIELDS:
    cur.execute(f"""
        SELECT COUNT(*) FROM profiles
        WHERE {field} IS NOT NULL
          AND array_length({field}, 1) > 0
    """)
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

# Timestamp fields: NOT NULL
for field in TS_FIELDS:
    cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {field} IS NOT NULL")
    count = cur.fetchone()[0]
    pct = round(count / total * 100, 1) if total > 0 else 0
    results['fields'][field] = {'count': count, 'pct': pct}

cur.close()
conn.close()

print(json.dumps(results, indent=2))
