#!/usr/bin/env python3
"""
Quality verification for AI-inferred profile fields.

Checks:
1. Specificity — flags vague/generic descriptions
2. Consistency — cross-checks niche/what_you_do/who_you_serve/offering alignment
3. Nonsense — detects circular references or name-stuffing
4. Reports per-profile quality score and flags low-quality inferences

Usage:
    python scripts/verify_inferred_fields.py                  # Full scan
    python scripts/verify_inferred_fields.py --fix            # Score + clear bad fields
    python scripts/verify_inferred_fields.py --sample 20      # Quick spot-check
"""
import os, sys, json, argparse, re
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.environ['DATABASE_URL']

# Vague/generic phrases that indicate thin inference
VAGUE_PHRASES = [
    'professional services',
    'business development solutions',
    'business advisory',
    'consulting and business',
    'business support services',
    'professional consulting',
    'strategic guidance',
    'general business',
    'service-oriented business professional',
    'business professionals seeking',
    'operational support solutions',
    'proven consulting',
    'expert guidance',
    'delivers specialized',
    'provides professional',
    'seeking strategic partnerships and collaborations',
    'open to strategic partnerships',
    'business professionals in need of',
    'general market',
    'general audience',
    'general businesses',
]

# Nonsense patterns
NONSENSE_PATTERNS = [
    r'specializes in \w+ solutions',  # "Specializes in Dordschi solutions"
    r'contributes .+ and .+ solutions to joint venture',
    r'brings proven .+ expertise to joint venture',
]


def get_inferred_profiles():
    """Get all profiles that have AI-inferred fields."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id, name, what_you_do, who_you_serve, seeking, offering,
               niche, bio, network_role, audience_type, tags,
               company, website,
               enrichment_metadata->'field_meta' as field_meta
        FROM profiles
        WHERE enrichment_metadata->'field_meta'->'niche'->>'source' = 'ai_inference'
           OR enrichment_metadata->'field_meta'->'what_you_do'->>'source' = 'ai_inference'
           OR enrichment_metadata->'field_meta'->'offering'->>'source' = 'ai_inference'
           OR enrichment_metadata->'field_meta'->'seeking'->>'source' = 'ai_inference'
           OR enrichment_metadata->'field_meta'->'who_you_serve'->>'source' = 'ai_inference'
           OR enrichment_metadata->'field_meta'->'bio'->>'source' = 'ai_inference'
        ORDER BY name
    """)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def check_vagueness(text):
    """Score how vague/generic a text field is. Returns (score 0-1, reasons)."""
    if not text:
        return 0, []

    text_lower = text.lower()
    reasons = []

    # Check for vague phrases
    vague_hits = sum(1 for p in VAGUE_PHRASES if p in text_lower)
    if vague_hits >= 2:
        reasons.append(f'{vague_hits} vague phrases')

    # Check for very short or very long
    if len(text) < 15:
        reasons.append('too short')
    if len(text) > 400:
        reasons.append('too long')

    # Check for nonsense patterns
    for pat in NONSENSE_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            reasons.append('nonsense pattern')

    # Penalty score: higher = more vague
    score = min(1.0, vague_hits * 0.3 + (0.3 if len(text) < 15 else 0) +
                (0.3 if any('nonsense' in r for r in reasons) else 0))
    return score, reasons


def check_name_stuffing(text, name):
    """Check if the person's name is used excessively in a field."""
    if not text or not name:
        return 0
    first_name = name.split()[0].lower() if name else ''
    full_name_lower = name.lower()

    count = text.lower().count(full_name_lower) + text.lower().count(first_name)
    # More than 2 mentions in a field is name-stuffing
    return min(1.0, max(0, (count - 1) * 0.3))


def check_circular(what_you_do, offering, seeking):
    """Check if offering/seeking just restates what_you_do."""
    if not what_you_do:
        return 0

    wyd_words = set(what_you_do.lower().split())
    score = 0

    if offering:
        off_words = set(offering.lower().split())
        overlap = len(wyd_words & off_words) / max(len(wyd_words), 1)
        if overlap > 0.7:
            score += 0.5

    if seeking:
        seek_words = set(seeking.lower().split())
        overlap = len(wyd_words & seek_words) / max(len(wyd_words), 1)
        if overlap > 0.6:
            score += 0.3

    return min(1.0, score)


def check_consistency(niche, what_you_do, who_you_serve):
    """Check if niche aligns with what_you_do and who_you_serve."""
    if not niche or not what_you_do:
        return 0

    niche_words = set(niche.lower().split())
    wyd_lower = what_you_do.lower()

    # At least one niche word should appear in what_you_do
    matches = sum(1 for w in niche_words if w in wyd_lower and len(w) > 3)
    if matches == 0 and len(niche_words) > 1:
        return 0.5  # niche doesn't match what_you_do

    return 0


def score_profile(profile):
    """Score a single profile's inferred data quality. Returns (score 0-100, issues)."""
    issues = []
    penalties = 0

    field_meta = profile.get('field_meta') or {}
    if isinstance(field_meta, str):
        try:
            field_meta = json.loads(field_meta)
        except:
            field_meta = {}

    # Check each inferred field
    inferred_fields = []
    for field in ['what_you_do', 'who_you_serve', 'seeking', 'offering', 'niche', 'bio']:
        meta = field_meta.get(field, {})
        if meta.get('source') == 'ai_inference':
            inferred_fields.append(field)

    if not inferred_fields:
        return 100, []

    # 1. Vagueness checks
    for field in inferred_fields:
        val = profile.get(field, '')
        vague_score, vague_reasons = check_vagueness(val)
        if vague_score > 0.3:
            issues.append(f'{field}: vague ({", ".join(vague_reasons)})')
            penalties += vague_score * 15

    # 2. Name stuffing
    name = profile.get('name', '')
    for field in inferred_fields:
        val = profile.get(field, '')
        stuff_score = check_name_stuffing(val, name)
        if stuff_score > 0.3:
            issues.append(f'{field}: name-stuffed')
            penalties += stuff_score * 10

    # 3. Circular references
    circ = check_circular(
        profile.get('what_you_do', ''),
        profile.get('offering', '') if 'offering' in inferred_fields else None,
        profile.get('seeking', '') if 'seeking' in inferred_fields else None,
    )
    if circ > 0.3:
        issues.append('offering/seeking restates what_you_do')
        penalties += circ * 15

    # 4. Consistency
    cons = check_consistency(
        profile.get('niche', ''),
        profile.get('what_you_do', ''),
        profile.get('who_you_serve', ''),
    )
    if cons > 0.3:
        issues.append('niche inconsistent with what_you_do')
        penalties += cons * 10

    # 5. Audience type check
    if 'audience_type' in [f for f in field_meta if field_meta.get(f, {}).get('source') == 'ai_inference']:
        at = profile.get('audience_type', '')
        if at and at.lower() in ('general market', 'general audience', 'general businesses'):
            issues.append('audience_type too generic')
            penalties += 10

    score = max(0, 100 - penalties)
    return round(score), issues


def clear_bad_fields(profile_id, fields_to_clear):
    """Clear low-quality inferred fields from a profile."""
    if not fields_to_clear:
        return
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    set_parts = [f"{f} = NULL" for f in fields_to_clear]
    cur.execute(
        f"UPDATE profiles SET {', '.join(set_parts)} WHERE id = %s",
        (profile_id,)
    )
    conn.commit()
    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Verify AI-inferred field quality')
    parser.add_argument('--fix', action='store_true', help='Clear fields scoring below threshold')
    parser.add_argument('--threshold', type=int, default=40, help='Min score to keep (default: 40)')
    parser.add_argument('--sample', type=int, default=0, help='Only check N random profiles')
    args = parser.parse_args()

    profiles = get_inferred_profiles()
    if args.sample:
        import random
        random.seed(42)
        profiles = random.sample(profiles, min(args.sample, len(profiles)))

    print(f"\n{'='*60}")
    print("AI INFERENCE QUALITY VERIFICATION")
    print(f"{'='*60}")
    print(f"Profiles to check: {len(profiles)}")
    print(f"Threshold: {args.threshold}")
    print(f"Mode: {'FIX (will clear bad fields)' if args.fix else 'AUDIT ONLY'}")
    print()

    scores = []
    low_quality = []
    cleared_count = 0

    for profile in profiles:
        score, issues = score_profile(profile)
        scores.append(score)

        if score < args.threshold:
            low_quality.append((profile, score, issues))

    # Distribution
    buckets = Counter()
    for s in scores:
        if s >= 80: buckets['80-100 (good)'] += 1
        elif s >= 60: buckets['60-79 (ok)'] += 1
        elif s >= 40: buckets['40-59 (weak)'] += 1
        else: buckets['0-39 (bad)'] += 1

    print("QUALITY DISTRIBUTION:")
    for bucket in ['80-100 (good)', '60-79 (ok)', '40-59 (weak)', '0-39 (bad)']:
        count = buckets.get(bucket, 0)
        pct = count / len(scores) * 100 if scores else 0
        bar = '#' * int(pct / 2)
        print(f"  {bucket:20s} {count:5d} ({pct:5.1f}%)  {bar}")

    avg = sum(scores) / len(scores) if scores else 0
    print(f"\n  Average score: {avg:.1f}/100")
    print(f"  Below threshold ({args.threshold}): {len(low_quality)}")

    # Show worst offenders
    if low_quality:
        print(f"\n{'='*60}")
        print(f"LOW QUALITY PROFILES (score < {args.threshold})")
        print(f"{'='*60}")
        low_quality.sort(key=lambda x: x[1])
        for profile, score, issues in low_quality[:20]:
            print(f"\n  {profile['name']} (score: {score})")
            for issue in issues:
                print(f"    - {issue}")
            if profile.get('what_you_do'):
                print(f"    wyd: {profile['what_you_do'][:80]}")
            if profile.get('niche'):
                print(f"    niche: {profile['niche']}")

    # Fix mode: clear bad inferred fields
    if args.fix and low_quality:
        print(f"\n{'='*60}")
        print("CLEARING LOW-QUALITY FIELDS")
        print(f"{'='*60}")
        for profile, score, issues in low_quality:
            field_meta = profile.get('field_meta') or {}
            if isinstance(field_meta, str):
                try: field_meta = json.loads(field_meta)
                except: field_meta = {}

            # Only clear fields that are ai_inference source AND appear in issues
            fields_to_clear = []
            for field in ['what_you_do', 'who_you_serve', 'seeking', 'offering', 'niche', 'bio']:
                meta = field_meta.get(field, {})
                if meta.get('source') == 'ai_inference':
                    # Check if this specific field had issues
                    if any(field in issue for issue in issues):
                        fields_to_clear.append(field)

            if fields_to_clear:
                clear_bad_fields(profile['id'], fields_to_clear)
                cleared_count += 1
                print(f"  Cleared {', '.join(fields_to_clear)} for {profile['name']}")

        print(f"\n  Total profiles cleaned: {cleared_count}")


if __name__ == '__main__':
    main()
