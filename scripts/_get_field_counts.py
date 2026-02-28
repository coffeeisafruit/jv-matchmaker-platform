#!/usr/bin/env python3
"""Get current field counts for architecture diagram update."""
import os, json, psycopg2
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM profiles")
total = cur.fetchone()[0]

fields = {
    "name": "name IS NOT NULL AND name != ''",
    "company": "company IS NOT NULL AND company != ''",
    "website": "website IS NOT NULL AND website != ''",
    "linkedin": "linkedin IS NOT NULL AND linkedin != ''",
    "phone": "phone IS NOT NULL AND phone != ''",
    "email": "email IS NOT NULL AND email != ''",
    "booking_link": "booking_link IS NOT NULL AND booking_link != ''",
    "secondary_emails": "secondary_emails IS NOT NULL AND array_length(secondary_emails, 1) > 0",
    "avatar_url": "avatar_url IS NOT NULL AND avatar_url != ''",
    "bio": "bio IS NOT NULL AND bio != ''",
    "niche": "niche IS NOT NULL AND niche != ''",
    "who_you_serve": "who_you_serve IS NOT NULL AND who_you_serve != ''",
    "seeking": "seeking IS NOT NULL AND seeking != ''",
    "signature_programs": "signature_programs IS NOT NULL AND signature_programs != ''",
    "current_projects": "current_projects IS NOT NULL AND current_projects != ''",
    "business_focus": "business_focus IS NOT NULL AND business_focus != ''",
    "service_provided": "service_provided IS NOT NULL AND service_provided != ''",
    "offering": "offering IS NOT NULL AND offering != ''",
    "what_you_do": "what_you_do IS NOT NULL AND what_you_do != ''",
    "tags": "tags IS NOT NULL AND array_length(tags, 1) > 0",
    "revenue_tier": "revenue_tier IS NOT NULL AND revenue_tier != ''",
    "list_size": "list_size IS NOT NULL AND list_size > 0",
    "business_size": "business_size IS NOT NULL AND business_size != ''",
    "content_platforms": "content_platforms IS NOT NULL AND content_platforms::text != '' AND content_platforms::text != 'null' AND content_platforms::text != '[]'",
    "audience_engagement_score": "audience_engagement_score IS NOT NULL AND audience_engagement_score > 0",
    "jv_history": "jv_history IS NOT NULL AND jv_history::text != '' AND jv_history::text != 'null' AND jv_history::text != '[]'",
}

result = {"total": total}
for k, where in fields.items():
    cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {where}")
    n = cur.fetchone()[0]
    result[k] = {"count": n, "pct": round(n * 100 / total, 1)}

cur.execute("SELECT COUNT(*) FROM profiles WHERE last_enriched_at IS NOT NULL")
result["enriched"] = cur.fetchone()[0]

print(json.dumps(result, indent=2))
conn.close()
