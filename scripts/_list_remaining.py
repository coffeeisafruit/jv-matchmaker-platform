#!/usr/bin/env python3
"""Quick helper to list remaining un-enriched profiles."""
import os, sys, json, glob, hashlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Chelsea_clients', 'research_cache')

def cache_key(name):
    return hashlib.md5(name.lower().encode()).hexdigest()[:12]

cache = {}
for fp in glob.glob(os.path.join(CACHE_DIR, '*.json')):
    try:
        with open(fp) as fh:
            data = json.load(fh)
        name = data.get('name', '')
        if name:
            cache[cache_key(name)] = data
    except:
        continue

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("""
    SELECT id, name, email, company, website, linkedin
    FROM profiles
    WHERE name IS NOT NULL AND name != ''
      AND website IS NOT NULL AND website != ''
    ORDER BY name
""")
profiles = cur.fetchall()
cur.close()
conn.close()

skip_patterns = [
    'calendly.com', 'acuityscheduling.com', 'tidycal.com',
    'oncehub.com', 'youcanbook.me', 'bookme.',
    'linktr.ee', 'linktree.com',
    'facebook.com', 'instagram.com', 'twitter.com',
    'linkedin.com', 'tiktok.com',
]

remaining = []
for p in profiles:
    key = cache_key(p['name'])
    cached = cache.get(key)
    if not cached:
        continue
    if cached.get('_crawl4ai_enriched'):
        continue
    has_cp = bool(cached.get('content_platforms'))
    has_wyd = bool(cached.get('what_you_do'))
    if has_cp and has_wyd:
        continue
    website = (p.get('website') or '').strip()
    if not website or not website.startswith('http'):
        continue
    if any(pat in website.lower() for pat in skip_patterns):
        continue
    remaining.append(dict(p))

print(f"Remaining: {len(remaining)}")
for r in remaining:
    print(f"  {r['name']} | {r.get('company', '')} | {r['website']}")

# Save as JSON for the OWL script to consume
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_remaining_profiles.json')
with open(output_path, 'w') as f:
    json.dump(remaining, f, indent=2, default=str)
print(f"\nSaved to {output_path}")
