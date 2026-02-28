#!/usr/bin/env python3
"""Analyze database completeness and enrichment gaps."""
import os, sys, json, glob

from _common import setup_django, cache_key, get_db_connection, CACHE_DIR
setup_django()

import random

# Load cache keys
cache_keys = set()
for fp in glob.glob(os.path.join(CACHE_DIR, '*.json')):
    cache_keys.add(os.path.basename(fp).replace('.json', ''))

conn, cur = get_db_connection()

# === SECTION 1: What do the 530 bare profiles have? ===
bare_query = """
    SELECT id, name, email, phone, company, website, linkedin
    FROM profiles
    WHERE (what_you_do IS NULL OR what_you_do = '')
      AND (who_you_serve IS NULL OR who_you_serve = '')
      AND (niche IS NULL OR niche = '')
      AND (bio IS NULL OR bio = '')
      AND (offering IS NULL OR offering = '')
      AND (seeking IS NULL OR seeking = '')
"""
cur.execute(bare_query)
bare = cur.fetchall()

has_website = sum(1 for p in bare if p.get('website') and p['website'].strip())
has_linkedin = sum(1 for p in bare if p.get('linkedin') and p['linkedin'].strip())
has_company = sum(1 for p in bare if p.get('company') and p['company'].strip())
has_email = sum(1 for p in bare if p.get('email') and p['email'].strip())
has_phone = sum(1 for p in bare if p.get('phone') and p['phone'].strip())
name_only = sum(1 for p in bare
                if not (p.get('website') and p['website'].strip())
                and not (p.get('linkedin') and p['linkedin'].strip())
                and not (p.get('company') and p['company'].strip())
                and not (p.get('email') and p['email'].strip()))

print(f"=== {len(bare)} BARE PROFILES (0 key enrichment fields) ===")
print(f"  Has website:   {has_website}")
print(f"  Has linkedin:  {has_linkedin}")
print(f"  Has company:   {has_company}")
print(f"  Has email:     {has_email}")
print(f"  Has phone:     {has_phone}")
print(f"  Name only:     {name_only}")

# Cache coverage
has_cache = sum(1 for p in bare if cache_key(p['name']) in cache_keys)
no_cache_has_web = sum(1 for p in bare
                       if cache_key(p['name']) not in cache_keys
                       and p.get('website') and p['website'].strip()
                       and p['website'].startswith('http'))
no_cache_has_co_or_li = sum(1 for p in bare
                            if cache_key(p['name']) not in cache_keys
                            and not (p.get('website') and p['website'].strip() and p['website'].startswith('http'))
                            and ((p.get('company') and p['company'].strip())
                                 or (p.get('linkedin') and p['linkedin'].strip())))
no_cache_name_only = sum(1 for p in bare
                         if cache_key(p['name']) not in cache_keys
                         and not (p.get('website') and p['website'].strip() and p['website'].startswith('http'))
                         and not (p.get('company') and p['company'].strip())
                         and not (p.get('linkedin') and p['linkedin'].strip()))

print(f"\n  Cache coverage:")
print(f"    Have cache entry (but still bare): {has_cache}")
print(f"    No cache + has website (scrapeable): {no_cache_has_web}")
print(f"    No cache + has company/linkedin: {no_cache_has_co_or_li}")
print(f"    No cache + name only: {no_cache_name_only}")

# Sample
print(f"\n  Sample bare profiles:")
random.seed(42)
sample = random.sample(bare, min(15, len(bare)))
for p in sample:
    parts = [p['name']]
    if p.get('company') and p['company'].strip(): parts.append(f"co={p['company']}")
    if p.get('website') and p['website'].strip(): parts.append(f"web={p['website'][:50]}")
    if p.get('linkedin') and p['linkedin'].strip(): parts.append(f"li={p['linkedin']}")
    if p.get('email') and p['email'].strip(): parts.append(f"email={p['email']}")
    print(f"    {' | '.join(parts)}")

# === SECTION 2: Partially enriched profiles missing fields ===
print(f"\n{'='*60}")
print("PARTIALLY ENRICHED: INFERRABLE GAPS")
print(f"{'='*60}")

# Profiles that have what_you_do but missing other fields
cur.execute("""
    SELECT
        COUNT(*) as total_with_wyd,
        COUNT(*) FILTER (WHERE seeking IS NULL OR seeking = '') as missing_seeking,
        COUNT(*) FILTER (WHERE offering IS NULL OR offering = '') as missing_offering,
        COUNT(*) FILTER (WHERE niche IS NULL OR niche = '') as missing_niche,
        COUNT(*) FILTER (WHERE bio IS NULL OR bio = '') as missing_bio,
        COUNT(*) FILTER (WHERE who_you_serve IS NULL OR who_you_serve = '') as missing_wys,
        COUNT(*) FILTER (WHERE tags IS NULL OR array_length(tags, 1) IS NULL) as missing_tags,
        COUNT(*) FILTER (WHERE audience_type IS NULL OR audience_type = '') as missing_audience,
        COUNT(*) FILTER (WHERE service_provided IS NULL OR service_provided = '') as missing_service,
        COUNT(*) FILTER (WHERE business_focus IS NULL OR business_focus = '') as missing_bfocus,
        COUNT(*) FILTER (WHERE network_role IS NULL OR network_role = '') as missing_nrole
    FROM profiles
    WHERE what_you_do IS NOT NULL AND what_you_do != ''
""")
r = cur.fetchone()
print(f"\nProfiles WITH what_you_do: {r['total_with_wyd']}")
print(f"  Missing seeking:          {r['missing_seeking']}")
print(f"  Missing offering:         {r['missing_offering']}")
print(f"  Missing niche:            {r['missing_niche']}")
print(f"  Missing bio:              {r['missing_bio']}")
print(f"  Missing who_you_serve:    {r['missing_wys']}")
print(f"  Missing tags:             {r['missing_tags']}")
print(f"  Missing audience_type:    {r['missing_audience']}")
print(f"  Missing service_provided: {r['missing_service']}")
print(f"  Missing business_focus:   {r['missing_bfocus']}")
print(f"  Missing network_role:     {r['missing_nrole']}")

# Broader: profiles with ANY key field but missing others
cur.execute("""
    SELECT
        COUNT(*) as has_any_key_field,
        COUNT(*) FILTER (WHERE what_you_do IS NULL OR what_you_do = '') as missing_wyd,
        COUNT(*) FILTER (WHERE who_you_serve IS NULL OR who_you_serve = '') as missing_wys,
        COUNT(*) FILTER (WHERE seeking IS NULL OR seeking = '') as missing_seeking,
        COUNT(*) FILTER (WHERE offering IS NULL OR offering = '') as missing_offering,
        COUNT(*) FILTER (WHERE niche IS NULL OR niche = '') as missing_niche,
        COUNT(*) FILTER (WHERE bio IS NULL OR bio = '') as missing_bio,
        COUNT(*) FILTER (WHERE tags IS NULL OR array_length(tags, 1) IS NULL) as missing_tags
    FROM profiles
    WHERE (what_you_do IS NOT NULL AND what_you_do != '')
       OR (who_you_serve IS NOT NULL AND who_you_serve != '')
       OR (niche IS NOT NULL AND niche != '')
       OR (bio IS NOT NULL AND bio != '')
       OR (offering IS NOT NULL AND offering != '')
       OR (seeking IS NOT NULL AND seeking != '')
""")
r = cur.fetchone()
print(f"\n{'='*60}")
print(f"ALL PROFILES WITH 1+ KEY FIELDS: {r['has_any_key_field']}")
print(f"{'='*60}")
print(f"  Missing what_you_do:   {r['missing_wyd']}")
print(f"  Missing who_you_serve: {r['missing_wys']}")
print(f"  Missing seeking:       {r['missing_seeking']}")
print(f"  Missing offering:      {r['missing_offering']}")
print(f"  Missing niche:         {r['missing_niche']}")
print(f"  Missing bio:           {r['missing_bio']}")
print(f"  Missing tags:          {r['missing_tags']}")

cur.close()
conn.close()
