"""
Export unenriched Tier B profiles from DB into batch files for Vast.ai enrichment.

Each batch file: 5 profiles with id + bio as scraped_text.
Output: tmp/tier_b_remaining_batches/batch_XXXX.json
"""
import json
import os
import psycopg2
import psycopg2.extras

DSN = os.environ.get('DATABASE_URL', 'postgresql://postgres.ysvwwfqmbjbvqvxutvom:qytned-qovfiZ-bazpy3@aws-0-us-west-2.pooler.supabase.com:6543/postgres')
OUT_DIR = 'tmp/tier_b_remaining_batches'
BATCH_SIZE = 5

os.makedirs(OUT_DIR, exist_ok=True)

conn = psycopg2.connect(DSN)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT id, name, bio, website, company, email, phone,
           who_you_serve, seeking, offering, niche, tags,
           signature_programs, booking_link, revenue_tier, service_provided
    FROM profiles
    WHERE jv_tier = 'B'
      AND (what_you_do IS NULL OR what_you_do = '')
      AND bio IS NOT NULL AND bio <> ''
    ORDER BY jv_readiness_score DESC
""")

rows = cur.fetchall()
conn.close()

total = len(rows)
batch_num = 0

for i in range(0, total, BATCH_SIZE):
    chunk = rows[i:i + BATCH_SIZE]
    batch = []
    for r in chunk:
        batch.append({
            'id': str(r['id']),
            'name': r['name'] or '',
            'scraped_text': r['bio'] or '',
            'website': r['website'] or '',
            'company': r['company'] or '',
            'email': r['email'] or '',
            'phone': r['phone'] or '',
            # Pre-existing fields (don't overwrite if good)
            'who_you_serve': r['who_you_serve'] or '',
            'seeking': r['seeking'] or '',
            'offering': r['offering'] or '',
            'niche': r['niche'] or '',
        })

    out_path = os.path.join(OUT_DIR, f'batch_{batch_num:04d}.json')
    with open(out_path, 'w') as f:
        json.dump(batch, f, indent=2, default=str)

    batch_num += 1

print(f'Exported {total:,} profiles into {batch_num:,} batches → {OUT_DIR}/')
