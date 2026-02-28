#!/usr/bin/env python3
"""Clear low-quality AI-inferred fields for re-enrichment."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

FIELDS_TO_CLEAR = [
    'what_you_do', 'who_you_serve', 'seeking', 'offering',
    'niche', 'bio', 'network_role', 'audience_type'
]

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Load the 130 profiles
with open('/tmp/low_quality_ids.json') as f:
    profiles = json.load(f)

ids = [p['id'] for p in profiles]
print(f"Clearing {len(ids)} profiles' inferred fields...")

for pid in ids:
    set_parts = [f"{f} = NULL" for f in FIELDS_TO_CLEAR]
    cur.execute(
        f"UPDATE profiles SET {', '.join(set_parts)} WHERE id = %s",
        (pid,)
    )

conn.commit()
print(f"Cleared {len(ids)} profiles.")
cur.close()
conn.close()
