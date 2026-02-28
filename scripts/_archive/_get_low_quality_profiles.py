#!/usr/bin/env python3
"""Get the 32 low-quality profiles and their available context."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=RealDictCursor)

# Get profiles that had ai_inference and match our vague patterns
cur.execute("""
    SELECT id, name, company, website, linkedin, email, phone,
           what_you_do, who_you_serve, niche, tags,
           enrichment_metadata->'field_meta' as field_meta
    FROM profiles
    WHERE (enrichment_metadata->'field_meta'->'niche'->>'source' = 'ai_inference'
        OR enrichment_metadata->'field_meta'->'what_you_do'->>'source' = 'ai_inference'
        OR enrichment_metadata->'field_meta'->'offering'->>'source' = 'ai_inference')
      AND (
        what_you_do ILIKE '%professional services and business development%'
        OR what_you_do ILIKE '%professional consulting and business%'
        OR what_you_do ILIKE '%professional consulting and advisory%'
        OR what_you_do ILIKE '%business consultant who provides professional%'
        OR what_you_do ILIKE '%specializing in professional services%'
        OR what_you_do ILIKE '%Provide specialized services for clients%'
        OR what_you_do ILIKE '%delivers expert consulting and pro%'
        OR niche ILIKE '%general business%'
        OR niche ILIKE '%general professional%'
        OR niche = 'Business development solutions'
        OR niche = 'Business development consulting'
        OR niche = 'Business consulting advisory'
        OR niche = 'Business consulting development'
        OR niche = 'Professional consulting business development'
        OR niche = 'Business advisory consulting'
        OR niche = 'professional consulting services'
        OR niche = 'general business consulting'
        OR niche = 'General business services'
        OR niche = 'Professional business development'
        OR niche = 'Professional business services'
      )
    ORDER BY name
""")
rows = cur.fetchall()
print(f"Low-quality profiles found: {len(rows)}\n")

has_website = 0
has_company = 0
has_linkedin = 0
has_email = 0
name_only = 0

for r in rows:
    has_w = bool(r.get('website') and r['website'].strip() and r['website'].startswith('http'))
    has_c = bool(r.get('company') and r['company'].strip())
    has_l = bool(r.get('linkedin') and r['linkedin'].strip())
    has_e = bool(r.get('email') and r['email'].strip())
    if has_w: has_website += 1
    if has_c: has_company += 1
    if has_l: has_linkedin += 1
    if has_e: has_email += 1
    if not has_w and not has_c and not has_l and not has_e:
        name_only += 1

    anchors = []
    if has_c: anchors.append(f"co={r['company'][:30]}")
    if has_w: anchors.append("web")
    if has_l: anchors.append("li")
    if has_e: anchors.append("email")
    if r.get('tags'): anchors.append(f"tags={r['tags'][:3]}")
    anchor_str = ' | '.join(anchors) if anchors else 'NAME ONLY'
    print(f"  {r['name']:30s} [{anchor_str}]")

print(f"\nSummary:")
print(f"  Has website: {has_website}")
print(f"  Has company: {has_company}")
print(f"  Has linkedin: {has_linkedin}")
print(f"  Has email: {has_email}")
print(f"  Name only: {name_only}")

# Save IDs for use by enrichment script
ids = [str(r['id']) for r in rows]
with open('/tmp/low_quality_ids.json', 'w') as f:
    json.dump([dict(id=str(r['id']), name=r['name'], company=r.get('company'),
                     website=r.get('website'), linkedin=r.get('linkedin'),
                     email=r.get('email')) for r in rows], f, indent=2)
print(f"\nSaved {len(ids)} profile details to /tmp/low_quality_ids.json")

cur.close()
conn.close()
