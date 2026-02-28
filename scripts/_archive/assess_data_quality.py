#!/usr/bin/env python3
"""
Comprehensive Supabase data quality assessment.
Identifies duplicates, validates formats, checks consistency.

Usage:
    python scripts/assess_data_quality.py --output data_quality_report.html
"""
import os
import sys
import re
import json
from typing import Dict, List, Tuple
from datetime import datetime
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor
from difflib import SequenceMatcher
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DataQualityAssessor:
    """Comprehensive data quality assessment for Supabase profiles"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment")

        self.duplicates = []
        self.invalid_emails = []
        self.invalid_urls = []
        self.inconsistencies = []

    def connect(self):
        """Establish database connection"""
        return psycopg2.connect(self.database_url)

    def _get_coverage_stats(self) -> Dict:
        """Query DB for coverage statistics used in the HTML report."""
        conn = self.connect()
        cur = conn.cursor()

        def count(where: str) -> int:
            cur.execute(f"SELECT COUNT(*) FROM profiles WHERE {where}")
            return cur.fetchone()[0]

        total = count("TRUE")

        contact_fields = []
        for name, where in [
            ("Email", "email IS NOT NULL AND email != ''"),
            ("Phone", "phone IS NOT NULL AND phone != ''"),
            ("LinkedIn", "linkedin IS NOT NULL AND linkedin != ''"),
            ("Website", "website IS NOT NULL AND website != ''"),
            ("Booking link", "booking_link IS NOT NULL AND booking_link != ''"),
            ("Secondary emails", "secondary_emails IS NOT NULL AND array_length(secondary_emails, 1) > 0"),
        ]:
            n = count(where)
            contact_fields.append((name, n, n * 100 / total))

        profile_fields = []
        for name, where in [
            ("Name", "name IS NOT NULL AND name != ''"),
            ("Company", "company IS NOT NULL AND company != ''"),
            ("Bio", "bio IS NOT NULL AND bio != ''"),
            ("Niche", "niche IS NOT NULL AND niche != ''"),
            ("List size", "list_size IS NOT NULL AND list_size > 0"),
            ("Revenue tier", "revenue_tier IS NOT NULL AND revenue_tier != ''"),
            ("Who you serve", "who_you_serve IS NOT NULL AND who_you_serve != ''"),
            ("Seeking", "seeking IS NOT NULL AND seeking != ''"),
            ("Signature programs", "signature_programs IS NOT NULL AND signature_programs != ''"),
            ("Tags", "tags IS NOT NULL AND array_length(tags, 1) > 0"),
        ]:
            n = count(where)
            profile_fields.append((name, n, n * 100 / total))

        tiers = []
        for label, where in [
            ("T1: Has email", "email IS NOT NULL AND email != ''"),
            ("T2: Phone only", "(email IS NULL OR email = '') AND phone IS NOT NULL AND phone != ''"),
            ("T3: Booking only", "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND booking_link IS NOT NULL AND booking_link != ''"),
            ("T4: LinkedIn only", "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND linkedin IS NOT NULL AND linkedin != ''"),
            ("T5: Website only", "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND (linkedin IS NULL OR linkedin = '') AND website IS NOT NULL AND website != ''"),
            ("T6: Unreachable", "(email IS NULL OR email = '') AND (phone IS NULL OR phone = '') AND (booking_link IS NULL OR booking_link = '') AND (linkedin IS NULL OR linkedin = '') AND (website IS NULL OR website = '')"),
        ]:
            n = count(where)
            tiers.append((label, n, n * 100 / total))

        # Email sources
        cur.execute("""
            SELECT COALESCE(enrichment_metadata->'field_meta'->'email'->>'source', 'unknown') AS source,
                   COUNT(*) AS n
            FROM profiles WHERE email IS NOT NULL AND email != ''
            GROUP BY source ORDER BY n DESC
        """)
        email_sources = [(r[0], r[1]) for r in cur.fetchall()]

        # Contactable = email OR phone OR booking
        contactable = count(
            "(email IS NOT NULL AND email != '') OR "
            "(phone IS NOT NULL AND phone != '') OR "
            "(booking_link IS NOT NULL AND booking_link != '')"
        )

        # Completeness
        completeness_sql = """
            (CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) +
            (CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) +
            (CASE WHEN linkedin IS NOT NULL AND linkedin != '' THEN 1 ELSE 0 END) +
            (CASE WHEN website IS NOT NULL AND website != '' THEN 1 ELSE 0 END) +
            (CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) +
            (CASE WHEN bio IS NOT NULL AND bio != '' THEN 1 ELSE 0 END) +
            (CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END) +
            (CASE WHEN seeking IS NOT NULL AND seeking != '' THEN 1 ELSE 0 END) +
            (CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END) +
            (CASE WHEN list_size IS NOT NULL AND list_size > 0 THEN 1 ELSE 0 END)
        """
        cur.execute(f"SELECT ROUND(AVG(({completeness_sql})::numeric / 10 * 100), 1) FROM profiles")
        avg_completeness = float(cur.fetchone()[0])

        cur.execute(f"SELECT COUNT(*) FROM profiles WHERE ({completeness_sql}) >= 8")
        rich_profiles = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM profiles WHERE ({completeness_sql}) >= 5")
        medium_profiles = cur.fetchone()[0]

        conn.close()

        return {
            'total': total,
            'contact_fields': contact_fields,
            'profile_fields': profile_fields,
            'tiers': tiers,
            'email_sources': email_sources,
            'contactable_pct': contactable * 100 / total,
            'avg_completeness': avg_completeness,
            'rich_profiles': rich_profiles,
            'rich_pct': rich_profiles * 100 / total,
            'medium_profiles': medium_profiles,
            'medium_pct': medium_profiles * 100 / total,
        }

    def assess_duplicates(self) -> List[Dict]:
        """Find potential duplicates using multiple strategies"""
        print("=" * 70)
        print("ASSESSING DUPLICATES")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get all profiles
        cursor.execute("""
            SELECT id, name, email, linkedin, company, website
            FROM profiles
            WHERE name IS NOT NULL
            ORDER BY name
        """)
        profiles = cursor.fetchall()
        print(f"Analyzing {len(profiles)} profiles...")
        print()

        duplicates_found = set()  # Track profile IDs we've already flagged

        # Strategy 1: Exact email match
        print("Strategy 1: Exact email matches")
        email_groups = defaultdict(list)
        for profile in profiles:
            if profile['email']:
                email_groups[profile['email'].lower()].append(profile)

        for email, group in email_groups.items():
            if len(group) > 1:
                self.duplicates.append({
                    'strategy': 'email_exact',
                    'risk': 'HIGH',
                    'profiles': group,
                    'reason': f"Same email: {email}"
                })
                for p in group:
                    duplicates_found.add(p['id'])
                print(f"  ⚠️  {len(group)} profiles share email: {email}")
                for p in group:
                    print(f"      - {p['name']} ({p['company']})")

        # Strategy 2: LinkedIn URL match (normalized)
        print()
        print("Strategy 2: LinkedIn URL matches")
        linkedin_groups = defaultdict(list)
        for profile in profiles:
            if profile['linkedin']:
                # Normalize: remove query params, trailing slash, http/https
                normalized = re.sub(r'\?.*$', '', profile['linkedin'])
                normalized = normalized.rstrip('/')
                normalized = re.sub(r'^https?://', '', normalized)
                normalized = normalized.lower()
                linkedin_groups[normalized].append(profile)

        for linkedin, group in linkedin_groups.items():
            if len(group) > 1:
                # Skip if we've already flagged these profiles
                if all(p['id'] in duplicates_found for p in group):
                    continue

                self.duplicates.append({
                    'strategy': 'linkedin_url',
                    'risk': 'HIGH',
                    'profiles': group,
                    'reason': f"Same LinkedIn: {linkedin}"
                })
                for p in group:
                    duplicates_found.add(p['id'])
                print(f"  ⚠️  {len(group)} profiles share LinkedIn: {linkedin}")
                for p in group:
                    print(f"      - {p['name']} ({p['company']})")

        # Strategy 3: Fuzzy name match (same last name + similar first name)
        print()
        print("Strategy 3: Fuzzy name matches")
        name_groups = defaultdict(list)
        for profile in profiles:
            if profile['name']:
                parts = profile['name'].strip().split()
                if len(parts) >= 2:
                    last_name = parts[-1].lower()
                    name_groups[last_name].append(profile)

        for last_name, group in name_groups.items():
            if len(group) > 1:
                # Check for similar first names
                for i in range(len(group)):
                    for j in range(i + 1, len(group)):
                        p1, p2 = group[i], group[j]

                        # Skip if already flagged
                        if p1['id'] in duplicates_found and p2['id'] in duplicates_found:
                            continue

                        # Calculate similarity
                        similarity = SequenceMatcher(None,
                            p1['name'].lower(),
                            p2['name'].lower()
                        ).ratio()

                        if similarity > 0.8:  # 80% similar
                            self.duplicates.append({
                                'strategy': 'fuzzy_name',
                                'risk': 'MEDIUM',
                                'profiles': [p1, p2],
                                'reason': f"Similar names ({similarity:.0%} match)",
                                'similarity': similarity
                            })
                            duplicates_found.add(p1['id'])
                            duplicates_found.add(p2['id'])
                            print(f"  ⚠️  Similar names ({similarity:.0%}):")
                            print(f"      - {p1['name']} ({p1['company']})")
                            print(f"      - {p2['name']} ({p2['company']})")

        # Strategy 4: Same company + similar name
        print()
        print("Strategy 4: Same company + similar name")
        company_groups = defaultdict(list)
        for profile in profiles:
            if profile['company']:
                company_groups[profile['company'].lower()].append(profile)

        for company, group in company_groups.items():
            if len(group) > 1:
                # Check for similar names within same company
                for i in range(len(group)):
                    for j in range(i + 1, len(group)):
                        p1, p2 = group[i], group[j]

                        # Skip if already flagged
                        if p1['id'] in duplicates_found and p2['id'] in duplicates_found:
                            continue

                        # Calculate name similarity
                        similarity = SequenceMatcher(None,
                            p1['name'].lower(),
                            p2['name'].lower()
                        ).ratio()

                        if similarity > 0.6:  # 60% similar (lower threshold since company matches)
                            self.duplicates.append({
                                'strategy': 'company_name',
                                'risk': 'MEDIUM',
                                'profiles': [p1, p2],
                                'reason': f"Same company + similar names ({similarity:.0%} match)",
                                'similarity': similarity
                            })
                            duplicates_found.add(p1['id'])
                            duplicates_found.add(p2['id'])
                            print(f"  ⚠️  {company}: Similar names ({similarity:.0%}):")
                            print(f"      - {p1['name']}")
                            print(f"      - {p2['name']}")

        cursor.close()
        conn.close()

        print()
        print(f"Total duplicate groups found: {len(self.duplicates)}")
        print(f"Total profiles affected: {len(duplicates_found)}")

        return self.duplicates

    def validate_emails(self) -> List[Dict]:
        """Check email format and flag suspicious"""
        print()
        print("=" * 70)
        print("VALIDATING EMAILS")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, name, email, company
            FROM profiles
            WHERE email IS NOT NULL AND email != ''
        """)
        profiles = cursor.fetchall()
        print(f"Validating {len(profiles)} email addresses...")
        print()

        # Email validation regex
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

        # Suspicious patterns
        suspicious_patterns = [
            r'^test@', r'^spam@', r'^noreply@', r'^no-reply@',
            r'^admin@', r'^info@', r'^contact@', r'^support@',
            r'@test\.', r'@example\.', r'@placeholder\.'
        ]

        for profile in profiles:
            email = profile['email'].strip()
            issues = []

            # Check format
            if not email_pattern.match(email):
                issues.append('Invalid format')

            # Check suspicious patterns
            for pattern in suspicious_patterns:
                if re.search(pattern, email, re.IGNORECASE):
                    issues.append(f'Suspicious pattern: {pattern}')

            if issues:
                self.invalid_emails.append({
                    'profile': profile,
                    'email': email,
                    'issues': issues,
                    'risk': 'HIGH' if 'Invalid format' in issues else 'MEDIUM'
                })
                print(f"  ⚠️  {profile['name']}: {email}")
                for issue in issues:
                    print(f"      - {issue}")

        cursor.close()
        conn.close()

        print()
        print(f"Invalid/suspicious emails found: {len(self.invalid_emails)}")

        return self.invalid_emails

    def validate_urls(self) -> List[Dict]:
        """Check URL formats"""
        print()
        print("=" * 70)
        print("VALIDATING URLS")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT id, name, linkedin, website, booking_link
            FROM profiles
            WHERE linkedin IS NOT NULL OR website IS NOT NULL OR booking_link IS NOT NULL
        """)
        profiles = cursor.fetchall()
        print(f"Validating URLs for {len(profiles)} profiles...")
        print()

        # URL validation patterns
        linkedin_pattern = re.compile(r'^https?://(www\.)?linkedin\.com/in/[\w-]+/?')
        url_pattern = re.compile(r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

        for profile in profiles:
            issues = []

            # Validate LinkedIn
            if profile['linkedin']:
                linkedin = profile['linkedin'].strip()
                if not linkedin_pattern.match(linkedin):
                    issues.append(f"Invalid LinkedIn URL: {linkedin}")

            # Validate website
            if profile['website']:
                website = profile['website'].strip()
                if not url_pattern.match(website):
                    issues.append(f"Invalid website URL: {website}")
                elif not website.startswith('https://'):
                    issues.append(f"Website not HTTPS: {website}")

            # Validate booking link
            if profile['booking_link']:
                booking = profile['booking_link'].strip()
                if not url_pattern.match(booking):
                    issues.append(f"Invalid booking URL: {booking}")

            if issues:
                self.invalid_urls.append({
                    'profile': profile,
                    'issues': issues,
                    'risk': 'LOW'
                })
                print(f"  ⚠️  {profile['name']}:")
                for issue in issues:
                    print(f"      - {issue}")

        cursor.close()
        conn.close()

        print()
        print(f"URL issues found: {len(self.invalid_urls)}")

        return self.invalid_urls

    def check_consistency(self) -> List[Dict]:
        """Find data inconsistencies"""
        print()
        print("=" * 70)
        print("CHECKING DATA CONSISTENCY")
        print("=" * 70)
        print()

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Check for invalid list_size values
        cursor.execute("""
            SELECT id, name, list_size
            FROM profiles
            WHERE list_size IS NOT NULL AND (list_size < 0 OR list_size > 1000000000)
        """)
        invalid_list_sizes = cursor.fetchall()

        if invalid_list_sizes:
            print("Invalid list_size values:")
            for profile in invalid_list_sizes:
                self.inconsistencies.append({
                    'profile': profile,
                    'issue': f"Invalid list_size: {profile['list_size']:,}",
                    'risk': 'MEDIUM'
                })
                print(f"  ⚠️  {profile['name']}: {profile['list_size']:,}")

        # Check for null critical fields
        cursor.execute("""
            SELECT id, name, company, email, list_size
            FROM profiles
            WHERE name IS NULL OR name = ''
        """)
        null_names = cursor.fetchall()

        if null_names:
            print()
            print("Profiles with null/empty name:")
            for profile in null_names:
                self.inconsistencies.append({
                    'profile': profile,
                    'issue': "Missing name",
                    'risk': 'HIGH'
                })
                print(f"  ⚠️  ID: {profile['id']}, Company: {profile.get('company', 'N/A')}")

        cursor.close()
        conn.close()

        print()
        print(f"Consistency issues found: {len(self.inconsistencies)}")

        return self.inconsistencies

    def generate_report(self, output_path: str):
        """Generate detailed quality report"""
        print()
        print("=" * 70)
        print("GENERATING REPORT")
        print("=" * 70)
        print()

        # Calculate summary stats
        total_issues = (
            len(self.duplicates) +
            len(self.invalid_emails) +
            len(self.invalid_urls) +
            len(self.inconsistencies)
        )

        high_risk = sum(1 for d in self.duplicates if d['risk'] == 'HIGH')
        high_risk += sum(1 for e in self.invalid_emails if e['risk'] == 'HIGH')
        high_risk += sum(1 for i in self.inconsistencies if i['risk'] == 'HIGH')

        medium_risk = sum(1 for d in self.duplicates if d['risk'] == 'MEDIUM')
        medium_risk += sum(1 for e in self.invalid_emails if e['risk'] == 'MEDIUM')
        medium_risk += sum(1 for i in self.inconsistencies if i['risk'] == 'MEDIUM')

        # ── Gather coverage stats from DB ──
        coverage = self._get_coverage_stats()

        # Generate HTML report
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Quality &amp; Coverage Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; margin: 40px; color: #1a1a1a; }}
        h1 {{ color: #1a1a1a; }}
        h2 {{ color: #2c3e50; margin-top: 40px; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .summary {{ background: #ecf0f1; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .stat {{ display: inline-block; margin-right: 30px; }}
        .stat-label {{ font-weight: bold; color: #7f8c8d; }}
        .stat-value {{ font-size: 24px; color: #2c3e50; }}
        .risk-high {{ color: #e74c3c; font-weight: bold; }}
        .risk-medium {{ color: #f39c12; font-weight: bold; }}
        .risk-low {{ color: #95a5a6; }}
        .duplicate-group {{ background: #fff; border-left: 4px solid #e74c3c; padding: 15px; margin: 10px 0; }}
        .issue {{ background: #fff; border-left: 4px solid #f39c12; padding: 15px; margin: 10px 0; }}
        .profile {{ margin: 5px 0; padding: 5px; background: #f8f9fa; }}
        .reason {{ color: #7f8c8d; font-style: italic; }}
        .coverage-section {{ margin: 20px 0; }}
        .coverage-table {{ border-collapse: collapse; width: 100%; max-width: 700px; }}
        .coverage-table th {{ text-align: left; padding: 10px 12px; background: #2c3e50; color: white; }}
        .coverage-table td {{ padding: 8px 12px; border-bottom: 1px solid #ecf0f1; }}
        .coverage-table tr:hover {{ background: #f8f9fa; }}
        .bar-cell {{ width: 200px; }}
        .bar {{ height: 20px; border-radius: 4px; display: inline-block; vertical-align: middle; }}
        .bar-contact {{ background: #3498db; }}
        .bar-profile {{ background: #2ecc71; }}
        .bar-tier {{ background: #9b59b6; }}
        .pct {{ font-weight: bold; color: #2c3e50; min-width: 50px; display: inline-block; }}
        .tier-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; max-width: 700px; margin: 15px 0; }}
        .tier-card {{ background: #f8f9fa; border-radius: 8px; padding: 16px; border-left: 4px solid #3498db; }}
        .tier-card.t1 {{ border-left-color: #2ecc71; }}
        .tier-card.t2 {{ border-left-color: #3498db; }}
        .tier-card.t3 {{ border-left-color: #f39c12; }}
        .tier-card.t4 {{ border-left-color: #e67e22; }}
        .tier-card.t5 {{ border-left-color: #e74c3c; }}
        .tier-card.t6 {{ border-left-color: #95a5a6; }}
        .tier-card .tier-label {{ font-size: 13px; color: #7f8c8d; }}
        .tier-card .tier-value {{ font-size: 22px; font-weight: bold; color: #2c3e50; }}
        .tier-card .tier-pct {{ font-size: 14px; color: #95a5a6; }}
        .headline-stats {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
        .headline-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 12px; padding: 20px 28px; min-width: 150px; }}
        .headline-card.green {{ background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }}
        .headline-card.orange {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }}
        .headline-card .hl-value {{ font-size: 36px; font-weight: bold; }}
        .headline-card .hl-label {{ font-size: 14px; opacity: 0.9; }}
        .source-table {{ border-collapse: collapse; margin: 15px 0; }}
        .source-table td, .source-table th {{ padding: 6px 16px; text-align: left; }}
        .source-table th {{ background: #ecf0f1; }}
        .completeness {{ font-size: 18px; margin: 10px 0; }}
    </style>
</head>
<body>
    <h1>Database Quality &amp; Coverage Report</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &mdash; {coverage['total']} profiles</p>

    <!-- ═══════════════ COVERAGE SECTION ═══════════════ -->

    <h2>Database Coverage</h2>

    <div class="headline-stats">
        <div class="headline-card">
            <div class="hl-value">{coverage['total']:,}</div>
            <div class="hl-label">Total Profiles</div>
        </div>
        <div class="headline-card green">
            <div class="hl-value">{coverage['contactable_pct']:.0f}%</div>
            <div class="hl-label">Directly Contactable</div>
        </div>
        <div class="headline-card orange">
            <div class="hl-value">{coverage['avg_completeness']}%</div>
            <div class="hl-label">Avg Completeness</div>
        </div>
    </div>

    <h3>Contact Fields</h3>
    <table class="coverage-table">
        <tr><th>Field</th><th>Count</th><th>Coverage</th><th class="bar-cell">Bar</th></tr>
"""
        for field_name, count, pct in coverage['contact_fields']:
            bar_width = int(pct * 2)
            html += f"""        <tr>
            <td>{field_name}</td>
            <td>{count:,}</td>
            <td><span class="pct">{pct:.1f}%</span></td>
            <td class="bar-cell"><span class="bar bar-contact" style="width:{bar_width}px"></span></td>
        </tr>
"""
        html += """    </table>

    <h3>Profile Fields</h3>
    <table class="coverage-table">
        <tr><th>Field</th><th>Count</th><th>Coverage</th><th class="bar-cell">Bar</th></tr>
"""
        for field_name, count, pct in coverage['profile_fields']:
            bar_width = int(pct * 2)
            html += f"""        <tr>
            <td>{field_name}</td>
            <td>{count:,}</td>
            <td><span class="pct">{pct:.1f}%</span></td>
            <td class="bar-cell"><span class="bar bar-profile" style="width:{bar_width}px"></span></td>
        </tr>
"""
        html += """    </table>

    <h3>Contactability Tiers</h3>
    <div class="tier-grid">
"""
        tier_classes = ['t1', 't2', 't3', 't4', 't5', 't6']
        for i, (label, count, pct) in enumerate(coverage['tiers']):
            cls = tier_classes[i] if i < len(tier_classes) else ''
            html += f"""        <div class="tier-card {cls}">
            <div class="tier-label">{label}</div>
            <div class="tier-value">{count:,}</div>
            <div class="tier-pct">{pct:.1f}%</div>
        </div>
"""
        html += """    </div>

    <h3>Email Sources</h3>
    <table class="source-table">
        <tr><th>Source</th><th>Count</th></tr>
"""
        for source, count in coverage['email_sources']:
            html += f"        <tr><td>{source}</td><td>{count:,}</td></tr>\n"

        html += f"""    </table>

    <div class="completeness">
        <strong>Profiles with 8+/10 key fields:</strong> {coverage['rich_profiles']:,} ({coverage['rich_pct']:.1f}%) &nbsp;|&nbsp;
        <strong>Profiles with 5+/10:</strong> {coverage['medium_profiles']:,} ({coverage['medium_pct']:.1f}%)
    </div>

    <!-- ═══════════════ QUALITY SECTION ═══════════════ -->

    <h2>Data Quality Issues</h2>

    <div class="summary">
        <div class="stat">
            <div class="stat-label">Total Issues</div>
            <div class="stat-value">{total_issues}</div>
        </div>
        <div class="stat">
            <div class="stat-label">High Risk</div>
            <div class="stat-value risk-high">{high_risk}</div>
        </div>
        <div class="stat">
            <div class="stat-label">Medium Risk</div>
            <div class="stat-value risk-medium">{medium_risk}</div>
        </div>
        <div class="stat">
            <div class="stat-label">Duplicates</div>
            <div class="stat-value">{len(self.duplicates)}</div>
        </div>
        <div class="stat">
            <div class="stat-label">Invalid Emails</div>
            <div class="stat-value">{len(self.invalid_emails)}</div>
        </div>
    </div>

    <h2>Duplicate Profiles ({len(self.duplicates)} groups)</h2>
"""

        for dup in self.duplicates:
            html += f"""
    <div class="duplicate-group">
        <strong class="risk-{dup['risk'].lower()}">[{dup['risk']}]</strong>
        <span class="reason">{dup['reason']}</span>
"""
            for profile in dup['profiles']:
                linkedin = profile.get('linkedin', 'N/A')
                if linkedin and linkedin != 'N/A':
                    linkedin_display = linkedin[:50] + '...'
                else:
                    linkedin_display = 'N/A'
                html += f"""
        <div class="profile">
            <strong>{profile['name']}</strong> - {profile.get('company', 'N/A')}<br>
            Email: {profile.get('email', 'N/A')} | LinkedIn: {linkedin_display}
        </div>
"""
            html += "    </div>\n"

        html += f"\n    <h2>Invalid Emails ({len(self.invalid_emails)})</h2>\n"
        for item in self.invalid_emails:
            html += f"""
    <div class="issue">
        <strong class="risk-{item['risk'].lower()}">[{item['risk']}]</strong>
        {item['profile']['name']} - {item['email']}<br>
        <span class="reason">Issues: {', '.join(item['issues'])}</span>
    </div>
"""

        html += f"\n    <h2>Invalid URLs ({len(self.invalid_urls)})</h2>\n"
        for item in self.invalid_urls:
            html += f"""
    <div class="issue">
        <strong>{item['profile']['name']}</strong><br>
        <span class="reason">{', '.join(item['issues'])}</span>
    </div>
"""

        html += f"\n    <h2>Consistency Issues ({len(self.inconsistencies)})</h2>\n"
        for item in self.inconsistencies:
            html += f"""
    <div class="issue">
        <strong class="risk-{item['risk'].lower()}">[{item['risk']}]</strong>
        {item['profile'].get('name', 'Unknown')}<br>
        <span class="reason">{item['issue']}</span>
    </div>
"""

        html += """
</body>
</html>
"""

        # Write HTML report
        with open(output_path, 'w') as f:
            f.write(html)

        # Also write JSON for programmatic access
        json_path = output_path.replace('.html', '.json')
        report_data = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_issues': total_issues,
                'high_risk': high_risk,
                'medium_risk': medium_risk,
                'duplicates': len(self.duplicates),
                'invalid_emails': len(self.invalid_emails),
                'invalid_urls': len(self.invalid_urls),
                'inconsistencies': len(self.inconsistencies),
            },
            'duplicates': self.duplicates,
            'invalid_emails': self.invalid_emails,
            'invalid_urls': self.invalid_urls,
            'inconsistencies': self.inconsistencies,
        }

        with open(json_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)

        print(f"HTML report written to: {output_path}")
        print(f"JSON data written to: {json_path}")
        print()
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Total issues found: {total_issues}")
        print(f"  - High risk: {high_risk}")
        print(f"  - Medium risk: {medium_risk}")
        print(f"  - Low risk: {total_issues - high_risk - medium_risk}")
        print()
        print(f"Duplicates: {len(self.duplicates)}")
        print(f"Invalid emails: {len(self.invalid_emails)}")
        print(f"Invalid URLs: {len(self.invalid_urls)}")
        print(f"Inconsistencies: {len(self.inconsistencies)}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Assess Supabase data quality')
    parser.add_argument('--output', default='data_quality_report.html',
                        help='Output HTML report path')
    parser.add_argument('--flag-duplicates', action='store_true',
                        help='Flag duplicate profiles')
    parser.add_argument('--validate-all', action='store_true',
                        help='Run all validation checks')

    args = parser.parse_args()

    assessor = DataQualityAssessor()

    if args.flag_duplicates or args.validate_all:
        assessor.assess_duplicates()

    if args.validate_all:
        assessor.validate_emails()
        assessor.validate_urls()
        assessor.check_consistency()

    assessor.generate_report(args.output)

    print()
    print("✅ Data quality assessment complete!")
    print(f"   View report: open {args.output}")
