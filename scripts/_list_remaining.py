#!/usr/bin/env python3
"""Quick helper to list remaining un-enriched profiles."""
import os, sys, json, glob

from _common import setup_django, cache_key, get_db_connection, CACHE_DIR, SKIP_DOMAINS
setup_django()

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

conn, cur = get_db_connection()
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
    if any(pat in website.lower() for pat in SKIP_DOMAINS):
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
