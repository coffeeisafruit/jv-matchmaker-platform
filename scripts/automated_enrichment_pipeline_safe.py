#!/usr/bin/env python3
"""
Safe Automated Enrichment Pipeline (NO GUESSING)

Only uses verified enrichment methods:
1. Website scraping (found on actual website)
2. LinkedIn scraping (found on actual profile)
3. Apollo.io API (verified by Apollo)

NO email pattern guessing - only real, found emails.

Usage:
    python scripts/automated_enrichment_pipeline_safe.py --limit 50 --auto-consolidate
"""

import os
import sys
import csv
import re
import asyncio
import aiohttp
import json
import logging
import random
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import time

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv
from matching.enrichment import VerificationGate, GateStatus
from matching.enrichment.ai_research import research_and_enrich_profile
try:
    from matching.enrichment.deep_research import deep_research_profile
except ImportError:
    deep_research_profile = None  # Module archived; fallback handled at call site

load_dotenv()

logger = logging.getLogger(__name__)

# --- Re-enrichment: Source priority hierarchy (R2) ---
# Higher priority sources can never be overwritten by lower ones.
# Client-provided data is protected from AI overwrites.
from matching.enrichment.constants import SOURCE_PRIORITY

PIPELINE_VERSION = 1


class SafeEnrichmentPipeline:
    """
    Safe enrichment pipeline - NO GUESSING.
    Only uses emails that are actually found/verified.
    """

    def __init__(self, max_apollo_credits=0, dry_run=False, batch_size=5):
        self.max_apollo_credits = max_apollo_credits
        self.apollo_credits_used = 0
        self.dry_run = dry_run
        self.batch_size = batch_size

        # Connection pool (P5) — lazy init, created on first use
        self._pool = None

        # Thread-safe stats lock (H3) — protects stats dict in ThreadPoolExecutor
        self._stats_lock = threading.Lock()

        # Verification gate (Layer 1 only — no AI, no raw_content in this pipeline)
        self.gate = VerificationGate(enable_ai_verification=False)

        # Refresh mode state (R3) — set by run() when --refresh is used
        self.refresh_mode = False
        self.stale_days = 30

        self.stats = {
            'total': 0,
            'enriched': 0,
            'emails_found': 0,
            'website_scrape': 0,
            'linkedin_scrape': 0,
            'apollo_api': 0,
            'failed': 0,
            'time_taken': 0,
            'gate_verified': 0,
            'gate_unverified': 0,
            'gate_quarantined': 0,
            # Tier tracking
            'tier_counts': {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
            # AI research tracking
            'ai_research_attempted': 0,
            'ai_research_success': 0,
            'deep_research_attempted': 0,
            'deep_research_success': 0,
            'extended_signals_found': 0,
        }

    def _ensure_pool(self):
        """Lazily initialize the connection pool on first use."""
        if self._pool is None:
            self._pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=os.environ['DATABASE_URL']
            )

    @contextmanager
    def _get_conn(self):
        """Get a connection from the pool with guaranteed return (P5)."""
        self._ensure_pool()
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def _inc_stat(self, key, delta=1):
        """Thread-safe stat increment (H3)."""
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + delta

    def _should_write_field(self, field: str, new_value, existing_meta: dict,
                            new_source: str = 'exa_research') -> bool:
        """Decide whether to write a field based on source priority and staleness (R4).

        In non-refresh mode, stale_days is effectively infinite (never overwrite
        equal-priority data). In refresh mode, uses self.stale_days.
        """
        if not new_value:
            return False

        field_info = (existing_meta or {}).get('field_meta', {}).get(field, {})
        existing_source = field_info.get('source', 'unknown')
        existing_priority = SOURCE_PRIORITY.get(existing_source, 0)
        new_priority = SOURCE_PRIORITY.get(new_source, 0)

        # Rule 1: Never overwrite higher-priority sources
        if new_priority < existing_priority:
            return False

        # Rule 2: Higher priority always wins
        if new_priority > existing_priority:
            return True

        # Rule 3: Equal priority — only overwrite if stale
        stale_days = self.stale_days if self.refresh_mode else 999999
        updated_at = field_info.get('updated_at')
        if updated_at:
            try:
                field_age = datetime.now() - datetime.fromisoformat(updated_at)
                return field_age > timedelta(days=stale_days)
            except (ValueError, TypeError):
                return True  # Bad timestamp — treat as stale

        # No timestamp — treat as stale (overwrite)
        return True

    def cleanup(self):
        """Close the connection pool."""
        if hasattr(self, '_pool') and self._pool:
            self._pool.closeall()

    def _ensure_quarantine_dir(self) -> str:
        """Ensure quarantine directory exists and return path."""
        quarantine_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'enrichment_batches', 'quarantine'
        )
        os.makedirs(quarantine_dir, exist_ok=True)
        return quarantine_dir

    def _ensure_reports_dir(self) -> str:
        """Ensure reports directory exists and return path."""
        reports_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'reports'
        )
        reports_dir = os.path.abspath(reports_dir)
        os.makedirs(reports_dir, exist_ok=True)
        return reports_dir

    def _get_field_counts(self) -> Dict[str, Dict]:
        """Query Supabase for current field fill counts — single query (M1)."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE what_you_do IS NOT NULL AND what_you_do != '') AS what_you_do,
                        COUNT(*) FILTER (WHERE who_you_serve IS NOT NULL AND who_you_serve != '') AS who_you_serve,
                        COUNT(*) FILTER (WHERE seeking IS NOT NULL AND seeking != '') AS seeking,
                        COUNT(*) FILTER (WHERE offering IS NOT NULL AND offering != '') AS offering,
                        COUNT(*) FILTER (WHERE revenue_tier IS NOT NULL AND revenue_tier != '') AS revenue_tier,
                        COUNT(*) FILTER (WHERE content_platforms IS NOT NULL AND content_platforms::text NOT IN ('{}', 'null', '')) AS content_platforms,
                        COUNT(*) FILTER (WHERE jv_history IS NOT NULL AND jv_history::text NOT IN ('[]', 'null', '')) AS jv_history,
                        COUNT(*) FILTER (WHERE website IS NOT NULL AND website != '') AS website,
                        COUNT(*) FILTER (WHERE niche IS NOT NULL AND niche != '') AS niche,
                        COUNT(*) FILTER (WHERE booking_link IS NOT NULL AND booking_link != '') AS booking_link,
                        COUNT(*) FILTER (WHERE signature_programs IS NOT NULL AND signature_programs != '') AS signature_programs,
                        COUNT(*) FILTER (WHERE tags IS NOT NULL AND array_length(tags, 1) > 0) AS tags
                    FROM profiles
                    WHERE name IS NOT NULL AND name != ''
                """)
                row = cursor.fetchone()
                col_names = [desc[0] for desc in cursor.description]
                result = dict(zip(col_names, row))

                total = result['total']
                counts = {}
                for field in col_names:
                    if field == 'total':
                        continue
                    count = result[field]
                    pct = (count / total * 100) if total > 0 else 0
                    counts[field] = {'count': count, 'pct': round(pct, 1)}

                counts['_total'] = total
                return counts
            finally:
                cursor.close()

    def generate_phase_report(
        self,
        phase_number: int,
        tier_filter: Optional[Set[int]],
        before_counts: Dict,
        runtime_seconds: float,
        cost_dollars: float,
        results: List[Dict],
    ) -> Dict:
        """Generate and save a per-phase enrichment report."""
        reports_dir = self._ensure_reports_dir()

        # Get current (after) counts
        after_counts = self._get_field_counts()
        total = after_counts.get('_total', 1)

        # Build field comparison
        field_fill_rates = {}
        tracked_fields = [
            'what_you_do', 'who_you_serve', 'seeking', 'offering',
            'revenue_tier', 'content_platforms', 'jv_history', 'website',
            'niche', 'booking_link', 'signature_programs', 'tags',
        ]
        for field in tracked_fields:
            before = before_counts.get(field, {'count': 0, 'pct': 0})
            after = after_counts.get(field, {'count': 0, 'pct': 0})
            field_fill_rates[field] = {
                'before': before,
                'after': after,
                'delta_count': after['count'] - before['count'],
                'delta_pct': round(after['pct'] - before['pct'], 1),
            }

        # Count enriched vs failed
        profiles_enriched = sum(1 for r in results if r.get('_extraction_metadata') or r.get('what_you_do'))
        profiles_failed = len(results) - profiles_enriched

        # Sample profiles
        sample = []
        for r in results[:5]:
            fields_filled = sum(1 for f in tracked_fields if r.get(f))
            sample.append({
                'name': r.get('name', ''),
                'fields_filled': fields_filled,
                'revenue_tier': r.get('revenue_tier', ''),
            })

        report = {
            'phase': phase_number,
            'tiers': sorted(tier_filter) if tier_filter else list(range(0, 6)),
            'timestamp': datetime.now().isoformat(),
            'runtime_seconds': round(runtime_seconds, 1),
            'profiles_processed': len(results),
            'profiles_enriched': profiles_enriched,
            'profiles_failed': profiles_failed,
            'cost_dollars': round(cost_dollars, 2),
            'field_fill_rates': field_fill_rates,
            'exa_stats': {
                'research_attempted': self.stats.get('ai_research_attempted', 0),
                'research_success': self.stats.get('ai_research_success', 0),
                'deep_research_attempted': self.stats.get('deep_research_attempted', 0),
                'deep_research_success': self.stats.get('deep_research_success', 0),
                'retry_count': self.stats.get('_retry_count', 0),
                'permanent_failures': self.stats.get('_retry_permanent_failures', 0),
            },
            'verification_gate': {
                'verified': self.stats.get('gate_verified', 0),
                'unverified': self.stats.get('gate_unverified', 0),
                'quarantined': self.stats.get('gate_quarantined', 0),
            },
            'extended_signals_found': self.stats.get('extended_signals_found', 0),
            'sample_profiles': sample,
        }

        # Save JSON report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(reports_dir, f'enrichment_phase_{phase_number}_{timestamp}.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)

        # Append to cumulative report
        cumulative_file = os.path.join(reports_dir, 'enrichment_cumulative.json')
        cumulative = []
        if os.path.exists(cumulative_file):
            with open(cumulative_file, 'r', encoding='utf-8') as f:
                try:
                    cumulative = json.load(f)
                except json.JSONDecodeError:
                    cumulative = []
        cumulative.append(report)
        with open(cumulative_file, 'w', encoding='utf-8') as f:
            json.dump(cumulative, f, indent=2, default=str)

        # Print human-readable summary
        self._print_phase_report(report)

        print(f"\n  Report saved to: {report_file}")
        return report

    def _print_phase_report(self, report: Dict):
        """Print a formatted phase report to stdout."""
        phase = report['phase']
        tiers = report['tiers']
        tier_str = ','.join(str(t) for t in tiers)

        print()
        print("=" * 64)
        print(f"  PHASE {phase} REPORT — Tier(s) {tier_str}")
        print("=" * 64)
        print(
            f"  Processed: {report['profiles_processed']:,} | "
            f"Enriched: {report['profiles_enriched']:,} | "
            f"Failed: {report['profiles_failed']:,}"
        )
        print(
            f"  Cost: ${report['cost_dollars']:.2f} | "
            f"Runtime: {report['runtime_seconds']/60:.1f} min"
        )
        print("-" * 64)
        print("  Field Coverage Improvement:")
        print(f"  {'Field':<22} {'Before':>8} {'After':>8} {'Delta':>8}")
        print(f"  {'-'*22} {'-'*8} {'-'*8} {'-'*8}")

        for field, data in report['field_fill_rates'].items():
            before_pct = data['before']['pct']
            after_pct = data['after']['pct']
            delta = data['delta_pct']
            delta_str = f"+{delta:.1f}%" if delta >= 0 else f"{delta:.1f}%"
            print(f"  {field:<22} {before_pct:>7.1f}% {after_pct:>7.1f}% {delta_str:>8}")

        print("-" * 64)
        exa = report['exa_stats']
        print(
            f"  Exa: {exa['research_success']}/{exa['research_attempted']} enriched, "
            f"{exa['retry_count']} retries, "
            f"{exa['permanent_failures']} permanent failures"
        )
        gate = report['verification_gate']
        print(
            f"  Gate: {gate['verified']} verified, "
            f"{gate['unverified']} unverified, "
            f"{gate['quarantined']} quarantined"
        )
        print(f"  Extended signals: {report['extended_signals_found']} profiles")
        print("=" * 64)

    def print_cumulative_dashboard(self):
        """Print a final cumulative dashboard across all phases."""
        reports_dir = self._ensure_reports_dir()
        cumulative_file = os.path.join(reports_dir, 'enrichment_cumulative.json')

        if not os.path.exists(cumulative_file):
            print("No cumulative report found.")
            return

        with open(cumulative_file, 'r', encoding='utf-8') as f:
            phases = json.load(f)

        if not phases:
            return

        total_cost = sum(p.get('cost_dollars', 0) for p in phases)
        total_runtime = sum(p.get('runtime_seconds', 0) for p in phases)
        total_profiles = sum(p.get('profiles_processed', 0) for p in phases)

        # Get baseline (first phase's "before") and final (last phase's "after")
        first_phase = phases[0]
        last_phase = phases[-1]

        print()
        print("=" * 50)
        print("  ENRICHMENT COMPLETE — ALL PHASES")
        print("=" * 50)
        print(f"  Total cost:    ${total_cost:.2f}")
        print(f"  Total runtime: {total_runtime/60:.1f} min")
        print(f"  Profiles:      {total_profiles:,} processed")
        print()
        print("  Field Coverage (baseline -> final):")

        tracked_fields = [
            'what_you_do', 'who_you_serve', 'seeking', 'offering',
            'revenue_tier', 'content_platforms', 'jv_history', 'website',
        ]

        for field in tracked_fields:
            baseline = first_phase['field_fill_rates'].get(field, {}).get('before', {}).get('pct', 0)
            final = last_phase['field_fill_rates'].get(field, {}).get('after', {}).get('pct', 0)
            delta = final - baseline
            delta_str = f"+{delta:.1f}%" if delta >= 0 else f"{delta:.1f}%"
            print(f"    {field:<20} {baseline:>5.1f}% -> {final:>5.1f}%  ({delta_str})")

        print("=" * 50)

    def get_profiles_to_enrich(self, limit=20, priority='high-value', tier_filter=None) -> List[Dict]:
        """
        Get profiles needing enrichment, ordered by 5-tier priority.

        Tier 0: Re-enrich previously enriched profiles missing new fields
        Tier 1: Has website + missing `seeking` + list_size > 10K  (highest ROI)
        Tier 2: Has website + missing any key field + list_size > 1K
        Tier 3: Has website + missing any key field
        Tier 4: No website + has LinkedIn  (deep research via web search)
        Tier 5: No website + no LinkedIn   (name-based search only)

        When priority='tiered' (default), returns profiles sorted by tier with
        a '_tier' tag on each profile dict.

        In refresh mode (R3), selects stale profiles regardless of missing fields.
        """
        with self._get_conn() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                # --- Refresh mode (R3): select stale profiles ---
                if self.refresh_mode:
                    cursor.execute("""
                        SELECT id, name, email, company, website, linkedin,
                               list_size, seeking, who_you_serve, what_you_do, offering,
                               enrichment_metadata
                        FROM profiles
                        WHERE name IS NOT NULL AND name != ''
                          AND last_enriched_at IS NOT NULL
                          AND last_enriched_at < NOW() - INTERVAL '%s days'
                        ORDER BY list_size DESC NULLS LAST
                        LIMIT %s
                    """, (self.stale_days, limit))
                    rows = cursor.fetchall()
                    profiles = []
                    for row in rows:
                        profile = dict(row)
                        # Assign tier based on what signals they have
                        if profile.get('website'):
                            profile['_tier'] = 1
                        elif profile.get('linkedin'):
                            profile['_tier'] = 4
                        else:
                            profile['_tier'] = 5
                        # Filter by tier if specified
                        if tier_filter and profile['_tier'] not in tier_filter:
                            continue
                        profiles.append(profile)
                        if len(profiles) >= limit:
                            break
                    return profiles

                if priority == 'tiered':
                    key_fields_null = (
                        "(seeking IS NULL OR seeking = '' "
                        "OR who_you_serve IS NULL OR who_you_serve = '' "
                        "OR what_you_do IS NULL OR what_you_do = '' "
                        "OR offering IS NULL OR offering = '')"
                    )
                    seeking_null = "(seeking IS NULL OR seeking = '')"
                    new_fields_null = (
                        "(revenue_tier IS NULL OR revenue_tier = '' "
                        "OR content_platforms IS NULL OR content_platforms::text IN ('{}', 'null', '') "
                        "OR jv_history IS NULL OR jv_history::text IN ('[]', 'null', ''))"
                    )

                    tier_queries = [
                        (0, f"""
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND last_enriched_at IS NOT NULL
                              AND {new_fields_null}
                            ORDER BY list_size DESC NULLS LAST
                        """),
                        (1, f"""
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND website IS NOT NULL AND website != ''
                              AND {seeking_null}
                              AND list_size > 10000
                            ORDER BY list_size DESC NULLS LAST
                        """),
                        (2, f"""
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND website IS NOT NULL AND website != ''
                              AND {key_fields_null}
                              AND list_size > 1000
                            ORDER BY list_size DESC NULLS LAST
                        """),
                        (3, f"""
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND website IS NOT NULL AND website != ''
                              AND {key_fields_null}
                            ORDER BY list_size DESC NULLS LAST
                        """),
                        (4, """
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND (website IS NULL OR website = '')
                              AND linkedin IS NOT NULL AND linkedin != ''
                            ORDER BY list_size DESC NULLS LAST
                        """),
                        (5, """
                            SELECT id, name, email, company, website, linkedin,
                                   list_size, seeking, who_you_serve, what_you_do, offering,
                                   enrichment_metadata
                            FROM profiles
                            WHERE name IS NOT NULL AND name != ''
                              AND (website IS NULL OR website = '')
                              AND (linkedin IS NULL OR linkedin = '')
                            ORDER BY list_size DESC NULLS LAST
                        """),
                    ]

                    profiles = []
                    seen_ids = set()

                    for tier, query in tier_queries:
                        if tier_filter and tier not in tier_filter:
                            continue
                        if len(profiles) >= limit:
                            break

                        cursor.execute(query)
                        rows = cursor.fetchall()

                        for row in rows:
                            if len(profiles) >= limit:
                                break
                            profile = dict(row)
                            if profile['id'] not in seen_ids:
                                seen_ids.add(profile['id'])
                                profile['_tier'] = tier
                                profiles.append(profile)

                elif priority == 'high-value':
                    cursor.execute("""
                        SELECT id, name, email, company, website, linkedin, list_size
                        FROM profiles
                        WHERE (email IS NULL OR email = '')
                          AND company IS NOT NULL AND company != ''
                          AND name IS NOT NULL AND name != ''
                          AND list_size > 100000
                        ORDER BY list_size DESC
                        LIMIT %s
                    """, (limit,))
                    profiles = [dict(p) for p in cursor.fetchall()]

                elif priority == 'has-website':
                    cursor.execute("""
                        SELECT id, name, email, company, website, linkedin, list_size
                        FROM profiles
                        WHERE (email IS NULL OR email = '')
                          AND website IS NOT NULL AND website != ''
                          AND name IS NOT NULL AND name != ''
                        ORDER BY list_size DESC NULLS LAST
                        LIMIT %s
                    """, (limit,))
                    profiles = [dict(p) for p in cursor.fetchall()]

                else:
                    cursor.execute("""
                        SELECT id, name, email, company, website, linkedin, list_size
                        FROM profiles
                        WHERE (email IS NULL OR email = '')
                          AND name IS NOT NULL AND name != ''
                        ORDER BY list_size DESC NULLS LAST
                        LIMIT %s
                    """, (limit,))
                    profiles = [dict(p) for p in cursor.fetchall()]

                return profiles
            finally:
                cursor.close()

    async def enrich_profile_batch(
        self,
        profiles: List[Dict],
        session: aiohttp.ClientSession
    ) -> List[Tuple[Dict, Optional[str], Optional[str]]]:
        """Enrich batch in parallel"""
        tasks = [
            self.enrich_profile_async(profile, session)
            for profile in profiles
        ]
        return await asyncio.gather(*tasks)

    async def enrich_profile_async(
        self,
        profile: Dict,
        session: aiohttp.ClientSession
    ) -> Tuple[Dict, Optional[str], Optional[str]]:
        """
        Enrich single profile - VERIFIED ONLY.
        Returns: (profile, email, method)
        """
        name = profile['name']
        website = profile.get('website')
        linkedin = profile.get('linkedin')

        # METHOD 1: Website scraping (VERIFIED - found on actual site via Playwright)
        if website:
            scrape_result = await self.try_website_scraping_async(website, name, session)
            if scrape_result:
                email = scrape_result.get('email')
                # Attach secondary data to profile for downstream DB write
                if scrape_result.get('secondary_emails'):
                    profile['_scraped_secondary_emails'] = scrape_result['secondary_emails']
                if scrape_result.get('phone') and not profile.get('phone'):
                    profile['_scraped_phone'] = scrape_result['phone']
                if scrape_result.get('booking_link') and not profile.get('booking_link'):
                    profile['_scraped_booking_link'] = scrape_result['booking_link']
                if email:
                    self.stats['website_scrape'] += 1
                    return profile, email, 'website_scrape'

        # METHOD 2: LinkedIn scraping (VERIFIED - found on actual profile)
        if linkedin:
            email = await self.try_linkedin_scraping_async(linkedin, session)
            if email:
                self.stats['linkedin_scrape'] += 1
                return profile, email, 'linkedin_scrape'

        # NO METHOD 3: Email pattern guessing - REMOVED for safety

        return profile, None, None

    async def try_website_scraping_async(
        self,
        website: str,
        name: str,
        session: aiohttp.ClientSession
    ) -> Optional[Dict]:
        """
        Scrape website for contact info using ContactScraper (Playwright).

        Returns dict with {email, secondary_emails, phone, booking_link} or None.
        """
        if self.dry_run:
            return None

        try:
            from matching.enrichment.contact_scraper import ContactScraper
            scraper = ContactScraper(browse_timeout=45)
            result = await asyncio.to_thread(
                scraper.scrape_contact_info, website, name
            )

            if result.get('email') or result.get('secondary_emails') or result.get('phone'):
                return result

        except Exception as e:
            logger.warning(f"Website email scraping failed for {website}: {e}")

        return None

    async def fetch_url_async(
        self,
        url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Fetch URL with timeout"""
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=5),
                headers={'User-Agent': 'Mozilla/5.0'}
            ) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.warning(f"Async URL fetch failed for {url}: {e}")
        return None

    async def try_linkedin_scraping_async(
        self,
        linkedin_url: str,
        session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Scrape LinkedIn for ACTUAL email"""
        if self.dry_run:
            return None

        try:
            text = await self.fetch_url_async(linkedin_url, session)
            if text:
                emails = re.findall(
                    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                    text
                )
                valid_emails = [
                    e for e in emails
                    if not any(x in e.lower() for x in [
                        'noreply', 'spam', 'linkedin', 'example'
                    ])
                ]
                if valid_emails:
                    return valid_emails[0]
        except Exception as e:
            logger.warning(f"LinkedIn email scraping failed for {linkedin_url}: {e}")
        return None

    async def enrich_with_apollo_bulk(
        self,
        profiles: List[Dict]
    ) -> List[Tuple[Dict, Optional[str], Optional[str]]]:
        """Use Apollo bulk API (VERIFIED by Apollo) — email extraction for legacy flow."""
        if self.dry_run or not profiles:
            return [(p, None, None) for p in profiles]

        api_key = os.environ.get('APOLLO_API_KEY')
        if not api_key:
            return [(p, None, None) for p in profiles]

        batch = profiles[:10]
        details = []

        for profile in batch:
            name_parts = profile['name'].strip().split(' ', 1)
            first_name = name_parts[0] if name_parts else profile['name']
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            detail = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": profile.get('company', '')
            }

            website = profile.get('website')
            if website:
                domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                if domain.startswith('www.'):
                    domain = domain[4:]
                detail['domain'] = domain

            details.append(detail)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.apollo.io/api/v1/people/bulk_match",
                    json={
                        "details": details,
                        "reveal_personal_emails": True
                    },
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': api_key
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        matches = data.get('matches', [])

                        results = []
                        for profile, match in zip(batch, matches):
                            if match and match.get('email'):
                                email = match['email']
                                self.apollo_credits_used += 1
                                self.stats['apollo_api'] += 1
                                results.append((profile, email, 'apollo_api'))
                            else:
                                results.append((profile, None, None))
                        return results
        except Exception as e:
            logger.warning(f"Apollo bulk match API failed: {e}")

        return [(p, None, None) for p in batch]

    # --- CASCADE ENRICHMENT (Exa → Apollo → OWL) ---

    TIER1_FIELDS = ['email', 'website', 'linkedin', 'what_you_do', 'who_you_serve',
                    'niche', 'offering']

    def _has_tier1_gaps(self, result: Dict) -> bool:
        """Check if a profile result still has Tier 1 field gaps."""
        for field in self.TIER1_FIELDS:
            value = result.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                return True
        return False

    def _has_contact_gaps(self, result: Dict) -> bool:
        """Check if a profile result is missing contact info Apollo can fill."""
        return (
            not result.get('email')
            or not result.get('phone')
            or not result.get('linkedin')
        )

    def _run_cascade_apollo(self, results: List[Dict]) -> List[Dict]:
        """
        Cascade Step 2: Run full Apollo enrichment on profiles with gaps.

        Uses ApolloEnrichmentService to capture ALL Apollo data (not just email).
        Only runs for profiles that still have Tier 1-2 gaps after Exa.
        """
        from matching.enrichment.apollo_enrichment import ApolloEnrichmentService

        api_key = os.environ.get('APOLLO_API_KEY', '')
        if not api_key:
            logger.warning("CASCADE: APOLLO_API_KEY not set, skipping Apollo step")
            return results

        service = ApolloEnrichmentService(api_key=api_key)

        # Find profiles that need Apollo enrichment
        needs_apollo = []
        result_map = {}  # profile_id -> result index
        for idx, result in enumerate(results):
            if self._has_contact_gaps(result):
                needs_apollo.append(result)
                result_map[result['profile_id']] = idx

        if not needs_apollo:
            print("  CASCADE: All profiles have contact info, skipping Apollo")
            return results

        remaining_credits = self.max_apollo_credits - self.apollo_credits_used
        if remaining_credits <= 0:
            print(f"  CASCADE: Apollo credit limit reached ({self.apollo_credits_used})")
            return results

        # Limit to remaining credits
        needs_apollo = needs_apollo[:remaining_credits]
        print(f"  CASCADE: Running Apollo on {len(needs_apollo)} profiles with contact gaps...")

        # Process in batches of 10
        for batch_start in range(0, len(needs_apollo), 10):
            batch = needs_apollo[batch_start:batch_start + 10]

            # Convert results to profile-like dicts for Apollo
            profile_dicts = []
            for r in batch:
                profile_dicts.append({
                    'id': r['profile_id'],
                    'name': r.get('name', ''),
                    'company': r.get('company', ''),
                    'website': r.get('website', r.get('discovered_website', '')),
                    'linkedin': r.get('linkedin', r.get('discovered_linkedin', '')),
                    'email': r.get('email', ''),
                })

            apollo_results = service.enrich_batch(profile_dicts)

            for apollo_result, original_result in zip(apollo_results, batch):
                if apollo_result.get('error'):
                    continue

                idx = result_map.get(original_result['profile_id'])
                if idx is None:
                    continue

                # Merge Apollo fields into existing result
                for field in ('email', 'linkedin', 'website', 'phone', 'company',
                              'business_size', 'revenue_tier', 'service_provided',
                              'niche', 'avatar_url'):
                    if apollo_result.get(field) and not results[idx].get(field):
                        results[idx][field] = apollo_result[field]

                # Store apollo_data in result for consolidation
                results[idx]['_apollo_data'] = apollo_result.get('_apollo_data', {})

                self.apollo_credits_used += 1
                self.stats['apollo_api'] += 1

            time.sleep(1)  # Rate limit between batches

        print(f"  CASCADE: Apollo enriched {self.stats['apollo_api']} profiles")
        return results

    def _run_cascade_owl(self, results: List[Dict]) -> List[Dict]:
        """
        Cascade Step 3: Run OWL Deep Research on profiles still missing Tier 1 fields.

        Uses ProfileEnrichmentAgent (Claude SDK + DDG + Tavily) — essentially
        free on Claude Max plan.
        """
        # Find profiles still with Tier 1 gaps after Exa + Apollo
        needs_owl = []
        result_map = {}
        for idx, result in enumerate(results):
            if self._has_tier1_gaps(result):
                needs_owl.append(result)
                result_map[result['profile_id']] = idx

        if not needs_owl:
            print("  CASCADE: All Tier 1 fields filled, skipping OWL")
            return results

        print(f"  CASCADE: Running OWL Deep Research on {len(needs_owl)} profiles with Tier 1 gaps...")

        try:
            from matching.enrichment.owl_research.agents.enrichment_agent import ProfileEnrichmentAgent
            agent = ProfileEnrichmentAgent()
        except ImportError:
            logger.warning("CASCADE: OWL enrichment agent not available, skipping")
            return results

        owl_count = 0
        for result in needs_owl:
            try:
                enriched = agent.enrich_profile(
                    name=result.get('name', ''),
                    website=result.get('website', result.get('discovered_website', '')),
                    company=result.get('company', ''),
                    linkedin=result.get('linkedin', result.get('discovered_linkedin', '')),
                )

                if not enriched:
                    continue

                idx = result_map.get(result['profile_id'])
                if idx is None:
                    continue

                # Merge OWL fields (only fill gaps, don't overwrite Exa/Apollo data)
                for field in ('what_you_do', 'who_you_serve', 'seeking', 'offering',
                              'bio', 'niche'):
                    if enriched.get(field) and not results[idx].get(field):
                        results[idx][field] = enriched[field]

                owl_count += 1

            except Exception as e:
                logger.warning(f"OWL research failed for {result.get('name', '')}: {e}")
                continue

        print(f"  CASCADE: OWL enriched {owl_count} profiles")
        return results

    def _run_ai_research(self, profile: Dict) -> Optional[Dict]:
        """
        Run AI-powered profile research for a single profile.

        Uses Exa-first strategy: Exa.ai handles most profiles (1-2 API calls,
        ~$0.02/profile). Falls back to crawl4ai + Claude if site not indexed.

        Returns enriched data dict or None on failure.
        """
        name = profile['name']
        website = profile.get('website') or ''
        linkedin = profile.get('linkedin') or ''
        company = profile.get('company') or ''
        tier = profile.get('_tier', 0)

        # In refresh mode, never use fill_only (source priority handles protection)
        # In normal mode, Tier 0 = fill gaps only
        fill_only = (tier == 0) and not self.refresh_mode

        # Build existing_data from what we already have
        existing_data = {
            k: v for k, v in profile.items()
            if k not in ('id', '_tier', 'enrichment_metadata') and v is not None
        }

        max_retries = 3
        backoff_base = 2  # seconds

        for attempt in range(1, max_retries + 1):
            try:
                self._inc_stat('ai_research_attempted')
                enriched, was_researched = research_and_enrich_profile(
                    name=name,
                    website=website,
                    existing_data=existing_data,
                    use_cache=True,
                    force_research=not getattr(self, 'cache_only', False),
                    linkedin=linkedin,
                    company=company,
                    fill_only=fill_only,
                    skip_social_reach=getattr(self, 'skip_social_reach', False),
                    exa_only=getattr(self, 'exa_only', False),
                )
                if was_researched or getattr(self, 'cache_only', False):
                    self._inc_stat('ai_research_success')
                    return enriched

                # Fallback for tiers 4-5 if Exa didn't find anything
                if tier in (4, 5) and not was_researched and deep_research_profile is not None:
                    self._inc_stat('deep_research_attempted')
                    enriched, was_researched = deep_research_profile(
                        name=name,
                        company=company,
                        existing_data=existing_data,
                        use_gpt_researcher=False,
                    )
                    if was_researched:
                        self._inc_stat('deep_research_success')
                        return enriched

                # No enrichment found but no error — don't retry
                return None

            except Exception as e:
                error_str = str(e).lower()

                # Rate-limit detection with longer back-off (M2)
                is_rate_limit = any(s in error_str for s in [
                    'rate limit', '429', 'too many requests'
                ])

                # Expanded permanent failure list (M2)
                is_permanent = any(s in error_str for s in [
                    'not indexed', 'not found', 'invalid api key', 'unauthorized',
                    'forbidden', 'api key', 'invalid url', 'bad request',
                    'unprocessable',
                ])

                if is_rate_limit:
                    wait = 30 + random.uniform(0, 5)  # 30-35s cooldown
                    self._inc_stat('_rate_limit_retries')
                    logger.warning(
                        f"Rate limited for {name} (attempt {attempt}/{max_retries}), "
                        f"cooling down {wait:.0f}s"
                    )
                    time.sleep(wait)
                    continue

                if is_permanent or attempt == max_retries:
                    logger.error(
                        f"AI research failed for {name} (tier {tier}, attempt {attempt}/{max_retries}): {e}"
                    )
                    self._inc_stat('_retry_permanent_failures')
                    return None

                # Retryable error — exponential backoff with jitter
                wait = (backoff_base ** attempt) + random.uniform(0, 1)
                self._inc_stat('_retry_count')
                logger.warning(
                    f"Retryable error for {name} (attempt {attempt}/{max_retries}), "
                    f"waiting {wait:.1f}s: {e}"
                )
                time.sleep(wait)

        return None

    async def run(self, limit=20, priority='high-value', auto_consolidate=False,
                  concurrency=5, tier_filter=None, phase_number=None,
                  skip_social_reach=False, exa_only=False, cache_only=False,
                  cascade=False, owl_fallback=False):
        """Run safe enrichment pipeline with concurrent processing.

        Args:
            limit: Max profiles to process
            priority: Priority mode ('tiered', 'high-value', 'has-website', 'all')
            auto_consolidate: Write results to Supabase after processing
            concurrency: Number of profiles to process concurrently (default 5)
            tier_filter: Set of tier numbers to include (None = all tiers)
            phase_number: Phase number for reporting (1-4, or None)
            cache_only: If True, only use cached results (no API calls)
            cascade: Enable cascade enrichment (Exa → Apollo full → OWL)
            owl_fallback: Enable OWL deep research as final fallback
        """
        start_time = time.time()
        self.skip_social_reach = skip_social_reach
        self.exa_only = exa_only
        self.cache_only = cache_only
        self.cascade = cascade
        self.owl_fallback = owl_fallback

        print("=" * 70)
        print("SAFE AUTOMATED ENRICHMENT PIPELINE (NO GUESSING)")
        print("=" * 70)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE ENRICHMENT'}")
        if self.refresh_mode:
            print(f"REFRESH MODE: Re-enriching profiles stale > {self.stale_days} days")
        if cascade:
            print(f"CASCADE MODE: Exa → Apollo (full) → {'OWL' if owl_fallback else 'done'}")
        print(f"Priority: {priority}")
        print(f"Limit: {limit}")
        print(f"Concurrency: {concurrency}")
        if tier_filter:
            print(f"Tier filter: {sorted(tier_filter)}")
        if phase_number:
            print(f"Phase: {phase_number}")
        print(f"Max Apollo credits: {self.max_apollo_credits}")
        print()
        print("  SAFE MODE: Only verified emails (no pattern guessing)")
        print()

        profiles = self.get_profiles_to_enrich(limit, priority, tier_filter=tier_filter)

        print(f"Found {len(profiles)} profiles to enrich")

        # Snapshot field counts before processing (for reporting)
        before_counts = {}
        if phase_number and not self.dry_run:
            print("  Taking before-snapshot of field counts...")
            before_counts = self._get_field_counts()

        # Print tier breakdown when using tiered priority
        if priority == 'tiered':
            for profile in profiles:
                tier = profile.get('_tier', 0)
                if tier in self.stats['tier_counts']:
                    self.stats['tier_counts'][tier] += 1
            tier_counts = self.stats['tier_counts']
            print(f"  Tier 0 (re-enrich, missing new fields):  {tier_counts[0]}")
            print(f"  Tier 1 (website + no seeking + 10K+):    {tier_counts[1]}")
            print(f"  Tier 2 (website + missing fields + 1K+): {tier_counts[2]}")
            print(f"  Tier 3 (website + missing fields):       {tier_counts[3]}")
            print(f"  Tier 4 (no website + LinkedIn):          {tier_counts[4]}")
            print(f"  Tier 5 (no website + no LinkedIn):       {tier_counts[5]}")
        print()

        results = []
        semaphore = asyncio.Semaphore(concurrency)
        connector = aiohttp.TCPConnector(limit=max(concurrency * 3, 10), limit_per_host=3)
        processed_count = 0
        cost_total = 0.0

        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(0, len(profiles), self.batch_size):
                batch = profiles[i:i + self.batch_size]
                self.stats['total'] += len(batch)

                tier_label = ""
                if priority == 'tiered' and batch:
                    tiers_in_batch = sorted(set(p.get('_tier', 0) for p in batch))
                    tier_label = f" [tier(s) {','.join(str(t) for t in tiers_in_batch)}]"

                print(f"Processing batch {i//self.batch_size + 1} ({len(batch)} profiles){tier_label}...")

                # Step 1: Try verified email-finding methods (skip in cache-only mode)
                if cache_only:
                    batch_results = [(p, None, None) for p in batch]
                else:
                    batch_results = await self.enrich_profile_batch(batch, session)

                needs_apollo = []
                for profile, email, method in batch_results:
                    result_dict = {
                        'profile_id': profile['id'],
                        'name': profile['name'],
                        'company': profile.get('company', ''),
                        'email': email or '',
                        'method': method or '',
                        'list_size': profile.get('list_size', 0),
                        'enriched_at': datetime.now().isoformat(),
                        '_tier': profile.get('_tier', 0),
                        'enrichment_metadata': profile.get('enrichment_metadata'),
                    }
                    # Pass through scraped secondary data from ContactScraper
                    if profile.get('_scraped_secondary_emails'):
                        result_dict['secondary_emails'] = profile['_scraped_secondary_emails']
                    if profile.get('_scraped_phone'):
                        result_dict['phone'] = profile['_scraped_phone']
                    if profile.get('_scraped_booking_link'):
                        result_dict['booking_link'] = profile['_scraped_booking_link']

                    if email:
                        results.append(result_dict)
                        self.stats['enriched'] += 1
                        self.stats['emails_found'] += 1
                    else:
                        # Even without primary email, write secondary data if found
                        if result_dict.get('secondary_emails') or result_dict.get('phone'):
                            results.append(result_dict)
                        needs_apollo.append(profile)

                # Step 2: Apollo fallback for profiles without emails
                # In cascade mode, skip this — full Apollo runs after Exa research
                if needs_apollo and self.apollo_credits_used < self.max_apollo_credits and not self.cascade:
                    remaining = self.max_apollo_credits - self.apollo_credits_used
                    apollo_batch = needs_apollo[:min(len(needs_apollo), remaining)]

                    if apollo_batch:
                        print(f"  Trying Apollo API for {len(apollo_batch)} profiles...")
                        apollo_results = await self.enrich_with_apollo_bulk(apollo_batch)

                        for profile, email, method in apollo_results:
                            if email:
                                results.append({
                                    'profile_id': profile['id'],
                                    'name': profile['name'],
                                    'company': profile.get('company', ''),
                                    'email': email,
                                    'method': method,
                                    'list_size': profile.get('list_size', 0),
                                    'enriched_at': datetime.now().isoformat(),
                                    '_tier': profile.get('_tier', 0),
                                    'enrichment_metadata': profile.get('enrichment_metadata'),
                                })
                                self.stats['enriched'] += 1
                                self.stats['emails_found'] += 1
                            else:
                                self.stats['failed'] += 1

                batch_emails = sum(1 for r in results if r['profile_id'] in [p['id'] for p in batch])
                print(f"  {batch_emails}/{len(batch)} verified emails found")

                # Step 3: AI research for profile data (concurrent with rate limiting)
                # This runs for ALL profiles in the batch (regardless of email result)
                # because we want to fill seeking, who_you_serve, revenue_tier, etc.
                if not self.dry_run:
                    async def _process_profile(profile):
                        """Process a single profile with semaphore-based concurrency control."""
                        async with semaphore:
                            # Run CPU-bound Exa research in thread pool to not block event loop
                            loop = asyncio.get_event_loop()
                            enriched = await loop.run_in_executor(
                                None, self._run_ai_research, profile
                            )
                            return profile, enriched

                    # Process all profiles in batch concurrently (limited by semaphore)
                    # return_exceptions=True prevents one crash from killing the batch (H2)
                    tasks = [_process_profile(p) for p in batch]
                    ai_results = await asyncio.gather(*tasks, return_exceptions=True)

                    ai_count = 0
                    for result in ai_results:
                        # H2: Handle crashed tasks
                        if isinstance(result, Exception):
                            logger.error(f"AI research task crashed: {result}")
                            self._inc_stat('failed')
                            continue
                        profile, enriched = result
                        if not enriched:
                            self._inc_stat('failed')  # H7: track AI research failures
                            continue
                        ai_count += 1
                        processed_count += 1

                        # Track cost — use actual Exa cost when available (H6)
                        real_cost = (enriched.get('_extraction_metadata') or {}).get('exa_cost')
                        if real_cost is not None:
                            cost_total += float(real_cost)
                        else:
                            has_website = bool(profile.get('website'))
                            cost_total += 0.020 if has_website else 0.025

                        # Find or create the result entry for this profile
                        existing_result = next(
                            (r for r in results if r['profile_id'] == profile['id']),
                            None
                        )
                        if existing_result is None:
                            # No email was found, but we have AI-researched data
                            existing_result = {
                                'profile_id': profile['id'],
                                'name': profile['name'],
                                'company': profile.get('company', ''),
                                'email': None,
                                'method': None,
                                'list_size': profile.get('list_size', 0),
                                'enriched_at': datetime.now().isoformat(),
                                '_tier': profile.get('_tier', 0),
                                'enrichment_metadata': profile.get('enrichment_metadata'),
                            }
                            results.append(existing_result)

                        # Attach AI-researched fields to the result
                        for field in ('what_you_do', 'who_you_serve', 'seeking', 'offering',
                                      'bio', 'social_proof'):
                            if enriched.get(field):
                                existing_result[field] = enriched[field]

                        # Attach new profile fields from Prompt 1
                        for field in ('signature_programs', 'booking_link', 'niche', 'phone',
                                      'current_projects', 'company', 'business_size'):
                            if enriched.get(field):
                                existing_result[field] = enriched[field]

                        # Attach categorization fields from Prompt 1
                        # tags: list -> pass through as-is (serialized to Postgres text[] later)
                        if enriched.get('tags'):
                            existing_result['tags'] = enriched['tags']
                        for field in ('audience_type', 'business_focus', 'service_provided'):
                            if enriched.get(field):
                                existing_result[field] = enriched[field]

                        # list_size: integer, only pass through if > 0
                        enriched_list_size = enriched.get('list_size')
                        if enriched_list_size is not None:
                            try:
                                enriched_list_size = int(enriched_list_size)
                                if enriched_list_size > 0:
                                    existing_result['enriched_list_size'] = enriched_list_size
                            except (ValueError, TypeError):
                                logger.warning(f"  Invalid list_size '{enriched_list_size}' for {profile['name']}, skipping")

                        # Attach extended signals
                        if enriched.get('revenue_tier'):
                            existing_result['revenue_tier'] = enriched['revenue_tier']
                        if enriched.get('jv_history'):
                            existing_result['jv_history'] = enriched['jv_history']
                        if enriched.get('content_platforms'):
                            existing_result['content_platforms'] = enriched['content_platforms']
                        if enriched.get('audience_engagement_score') is not None:
                            existing_result['audience_engagement_score'] = enriched['audience_engagement_score']

                        # social_reach: integer, only pass through if > 0
                        enriched_social_reach = enriched.get('social_reach')
                        if enriched_social_reach is not None:
                            try:
                                enriched_social_reach = int(enriched_social_reach)
                                if enriched_social_reach > 0:
                                    existing_result['social_reach'] = enriched_social_reach
                            except (ValueError, TypeError):
                                logger.warning(f"  Invalid social_reach '{enriched_social_reach}' for {profile['name']}, skipping")

                        # Exa-discovered email: primary if empty, secondary if different
                        exa_email = enriched.get('email', '').strip()
                        if exa_email and '@' in exa_email:
                            current_email = (existing_result.get('email') or profile.get('email') or '').strip().lower()
                            if not current_email:
                                existing_result['exa_email'] = exa_email
                            elif exa_email.lower() != current_email:
                                existing_result.setdefault('secondary_emails', []).append(exa_email)

                        # Exa-discovered website/linkedin: fill only if profile has none
                        if enriched.get('website') and not profile.get('website'):
                            existing_result['discovered_website'] = enriched['website']
                        if enriched.get('linkedin') and not profile.get('linkedin'):
                            existing_result['discovered_linkedin'] = enriched['linkedin']

                        # Track extended signals
                        has_extended = any(
                            enriched.get(f)
                            for f in ('revenue_tier', 'jv_history', 'content_platforms')
                        )
                        if has_extended:
                            self.stats['extended_signals_found'] += 1

                        # Pass through extraction metadata for verification gate
                        if enriched.get('_extraction_metadata'):
                            existing_result['_extraction_metadata'] = enriched['_extraction_metadata']

                    if ai_count > 0:
                        print(f"  {ai_count}/{len(batch)} profiles AI-researched (Prompt 1+2)")

                    # Live progress (Change 5)
                    total_profiles = len(profiles)
                    done = i + len(batch)
                    elapsed = time.time() - start_time
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total_profiles - done) / rate if rate > 0 else 0
                    print(
                        f"  [{done}/{total_profiles}] "
                        f"${cost_total:.2f} spent | "
                        f"{rate:.1f} profiles/s | "
                        f"ETA {eta/60:.0f}min"
                    )

        # Save results (only those with emails for the CSV)
        # --- CASCADE STEPS (after Exa, before consolidation) ---
        if self.cascade and results and not self.dry_run:
            print(f"\n{'='*60}")
            print("CASCADE ENRICHMENT")
            print(f"{'='*60}")

            # Cascade Step 2: Full Apollo enrichment on profiles with contact gaps
            results = self._run_cascade_apollo(results)

            # Cascade Step 3: OWL Deep Research on remaining Tier 1 gaps
            if self.owl_fallback:
                results = self._run_cascade_owl(results)

            print(f"{'='*60}\n")

        email_results = [r for r in results if r.get('email')]
        if email_results and not self.dry_run:
            output_file = f"enriched_safe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['profile_id', 'name', 'company', 'email', 'method', 'list_size', 'enriched_at']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(email_results)

            print(f"\nResults saved to: {output_file}")

        if auto_consolidate and results and not self.dry_run:
            print("\nAuto-consolidating to Supabase...")
            await self.consolidate_to_supabase_batch(results)

        self.stats['time_taken'] = time.time() - start_time

        # Print summary
        total = self.stats['total'] or 1  # prevent division by zero
        print("\n" + "=" * 70)
        print("ENRICHMENT SUMMARY")
        print("=" * 70)
        print(f"Total profiles:      {self.stats['total']}")
        print(f"Verified emails:     {self.stats['emails_found']} ({self.stats['emails_found']/total*100:.1f}%)")
        print(f"Failed:              {self.stats['failed']}")
        print(f"Time taken:          {self.stats['time_taken']:.1f}s")
        print()
        print("Email methods (all verified):")
        print(f"  Website scraping:  {self.stats['website_scrape']} (found on site)")
        print(f"  LinkedIn scraping: {self.stats['linkedin_scrape']} (found on profile)")
        print(f"  Apollo API:        {self.stats['apollo_api']} (verified by Apollo)")
        print()
        print("AI research (profile data + extended signals):")
        print(f"  Website research:  {self.stats['ai_research_success']}/{self.stats['ai_research_attempted']} enriched")
        print(f"  Deep research:     {self.stats['deep_research_success']}/{self.stats['deep_research_attempted']} enriched")
        print(f"  Extended signals:  {self.stats['extended_signals_found']} profiles with revenue/JV/platform data")

        if priority == 'tiered':
            print()
            print("Tier breakdown:")
            for tier in range(0, 6):
                count = self.stats['tier_counts'].get(tier, 0)
                if count > 0:
                    print(f"  Tier {tier}: {count} profiles")

        print()
        print("Verification gate:")
        print(f"  Verified:     {self.stats['gate_verified']} (full confidence)")
        print(f"  Unverified:   {self.stats['gate_unverified']} (reduced confidence)")
        print(f"  Quarantined:  {self.stats['gate_quarantined']} (not written)")

        if self.stats['apollo_api'] > 0:
            print(f"\nApollo cost: ${self.stats['apollo_api'] * 0.10:.2f}")

        # Generate per-phase report (Changes 6-7)
        if phase_number and not self.dry_run and results:
            self.generate_phase_report(
                phase_number=phase_number,
                tier_filter=tier_filter,
                before_counts=before_counts,
                runtime_seconds=self.stats['time_taken'],
                cost_dollars=cost_total,
                results=results,
            )

    async def consolidate_to_supabase_batch(self, results: List[Dict]):
        """
        Batch consolidate to Supabase with verification gate.

        Uses source-aware merge (R4) to decide which fields to write:
        - In normal mode: fill-only for Tier 0, standard write for other tiers
        - In refresh mode: uses source priority + staleness checks
        - Client-sourced data is never overwritten by AI enrichment

        Tracks field-level provenance in enrichment_metadata (R1).
        """
        from matching.enrichment.confidence.confidence_scorer import ConfidenceScorer

        scorer = ConfidenceScorer()

        source_map = {
            'website_scrape': 'website_scraped',
            'linkedin_scrape': 'linkedin_scraped',
            'apollo_api': 'apollo'
        }

        email_updates = []
        profile_updates = []
        quarantined_records = []

        for result in results:
            profile_id = result['profile_id']
            email = result.get('email')
            method = result.get('method')
            enriched_at = datetime.fromisoformat(result['enriched_at'])

            # Get existing enrichment_metadata for source priority checks (R4)
            existing_meta = result.get('enrichment_metadata') or {}
            if isinstance(existing_meta, str):
                try:
                    existing_meta = json.loads(existing_meta)
                except (json.JSONDecodeError, TypeError):
                    existing_meta = {}

            # --- Email verification gate (only when we have an email) ---
            if email and method:
                profile_data = {
                    'email': email,
                    'name': result.get('name', ''),
                    'company': result.get('company', ''),
                }
                verdict = self.gate.evaluate(
                    data=profile_data,
                    raw_content=None,
                    extraction_metadata=result.get('_extraction_metadata'),
                )

                fixed_data = VerificationGate.apply_fixes(profile_data, verdict)
                email = fixed_data.get('email', email)

                if verdict.status == GateStatus.QUARANTINED:
                    quarantined_records.append({
                        'profile_id': profile_id,
                        'name': result.get('name', ''),
                        'email': result.get('email', ''),
                        'method': method,
                        'issues': verdict.issues_summary,
                        'failed_fields': verdict.failed_fields,
                        'timestamp': datetime.now().isoformat(),
                    })
                    self.stats['gate_quarantined'] += 1
                    email = None
                else:
                    source = source_map.get(method, 'unknown')
                    base_confidence = scorer.calculate_confidence('email', source, enriched_at)
                    confidence_expires_at = scorer.calculate_expires_at('email', enriched_at)

                    if verdict.status == GateStatus.VERIFIED:
                        confidence = base_confidence
                        self.stats['gate_verified'] += 1
                    else:
                        confidence = base_confidence * verdict.overall_confidence
                        self.stats['gate_unverified'] += 1

                    email_metadata = {
                        'source': source,
                        'enriched_at': enriched_at.isoformat(),
                        'source_date': enriched_at.date().isoformat(),
                        'confidence': confidence,
                        'confidence_expires_at': confidence_expires_at.isoformat(),
                        'verification_count': 1 if method == 'apollo_api' else 0,
                        'enrichment_method': method,
                        'verified': True,
                        'verification_status': verdict.status.value,
                        'verification_confidence': verdict.overall_confidence,
                    }

                    email_updates.append((
                        email,
                        json.dumps(email_metadata),
                        confidence,
                        enriched_at,
                        datetime.now(),
                        profile_id
                    ))

            # --- AI-researched profile fields + extended signals ---
            set_parts = []
            params = []
            fields_written = []  # Track for field-level provenance (R1)

            # Determine enrichment source
            enrichment_source = 'exa_research'
            ext_meta = result.get('_extraction_metadata') or {}
            if ext_meta.get('source') == 'ai_research':
                enrichment_source = 'ai_research'

            # --- Core text fields (P1: sql.Identifier for column names) ---
            for field in ('what_you_do', 'who_you_serve', 'seeking', 'offering',
                          'bio', 'social_proof'):
                value = result.get(field)
                if value and isinstance(value, str) and value.strip():
                    if self._should_write_field(field, value, existing_meta, enrichment_source):
                        set_parts.append(
                            sql.SQL("{} = %s").format(sql.Identifier(field))
                        )
                        params.append(value.strip())
                        fields_written.append(field)

            # --- Extended signal fields (R5: source-aware, no longer always-fill-only) ---
            revenue_tier = result.get('revenue_tier')
            if revenue_tier and self._should_write_field('revenue_tier', revenue_tier, existing_meta, enrichment_source):
                set_parts.append(sql.SQL("{} = %s").format(sql.Identifier('revenue_tier')))
                params.append(revenue_tier)
                fields_written.append('revenue_tier')

            # jv_history: JSONB — smart merge in refresh mode (R7)
            jv_history = result.get('jv_history')
            if jv_history:
                jv_json = json.dumps(jv_history) if not isinstance(jv_history, str) else jv_history
                if self.refresh_mode and self._should_write_field('jv_history', jv_history, existing_meta, enrichment_source):
                    # R7: Append new JV entries, dedup by partner name (done in Python)
                    set_parts.append(sql.SQL(
                        "{} = %s::jsonb"
                    ).format(sql.Identifier('jv_history')))
                    params.append(jv_json)
                    fields_written.append('jv_history')
                elif self._should_write_field('jv_history', jv_history, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN profiles.{col} IS NULL "
                        "OR profiles.{col}::text IN ('[]', 'null', '') "
                        "THEN %s::jsonb ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('jv_history')))
                    params.append(jv_json)
                    fields_written.append('jv_history')

            # content_platforms: JSONB — deep merge in refresh mode (R7)
            content_platforms = result.get('content_platforms')
            if content_platforms:
                cp_json = json.dumps(content_platforms) if not isinstance(content_platforms, str) else content_platforms
                if self.refresh_mode and self._should_write_field('content_platforms', content_platforms, existing_meta, enrichment_source):
                    # R7: Deep merge — existing keys kept, new keys added
                    set_parts.append(sql.SQL(
                        "{col} = COALESCE(profiles.{col}, '{{}}'::jsonb) || %s::jsonb"
                    ).format(col=sql.Identifier('content_platforms')))
                    params.append(cp_json)
                    fields_written.append('content_platforms')
                elif self._should_write_field('content_platforms', content_platforms, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN profiles.{col} IS NULL "
                        "OR profiles.{col}::text IN ('{{}}', 'null', '') "
                        "THEN %s::jsonb ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('content_platforms')))
                    params.append(cp_json)
                    fields_written.append('content_platforms')

            # audience_engagement_score: handle 0.0 (M4)
            engagement_score = result.get('audience_engagement_score')
            if engagement_score is not None:
                if self._should_write_field('audience_engagement_score', engagement_score, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN profiles.{col} IS NULL OR profiles.{col} = 0 "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('audience_engagement_score')))
                    params.append(float(engagement_score))
                    fields_written.append('audience_engagement_score')

            # --- Standard text fields ---
            for field in ('signature_programs', 'booking_link', 'niche', 'phone',
                          'current_projects', 'business_size'):
                value = result.get(field)
                if value and isinstance(value, str) and value.strip():
                    if self._should_write_field(field, value, existing_meta, enrichment_source):
                        set_parts.append(
                            sql.SQL("{} = %s").format(sql.Identifier(field))
                        )
                        params.append(value.strip())
                        fields_written.append(field)

            # --- tags: text[] — union merge in refresh mode (R7) ---
            tags_value = result.get('tags')
            if tags_value and isinstance(tags_value, list) and len(tags_value) > 0:
                if self._should_write_field('tags', tags_value, existing_meta, enrichment_source):
                    if self.refresh_mode:
                        # R7: Union existing + new tags, dedup
                        set_parts.append(sql.SQL(
                            "{col} = ("
                            "SELECT array_agg(DISTINCT t) FROM unnest("
                            "COALESCE(profiles.{col}, ARRAY[]::text[]) || %s::text[]"
                            ") AS t"
                            ")"
                        ).format(col=sql.Identifier('tags')))
                    else:
                        set_parts.append(sql.SQL("{} = %s::text[]").format(sql.Identifier('tags')))
                    params.append(tags_value)
                    fields_written.append('tags')

            # --- Categorization fields ---
            for field in ('audience_type', 'business_focus', 'service_provided'):
                value = result.get(field)
                if value and isinstance(value, str) and value.strip():
                    if self._should_write_field(field, value, existing_meta, enrichment_source):
                        set_parts.append(
                            sql.SQL("{} = %s").format(sql.Identifier(field))
                        )
                        params.append(value.strip())
                        fields_written.append(field)

            # company: source-aware (R5)
            company_value = result.get('company')
            if company_value and isinstance(company_value, str) and company_value.strip():
                if self._should_write_field('company', company_value, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('company')))
                    params.append(company_value.strip())
                    fields_written.append('company')

            # list_size: upgrade-only + source priority (R5)
            PG_INT4_MAX = 2147483647
            enriched_list_size = result.get('enriched_list_size')
            if enriched_list_size is not None:
                try:
                    enriched_list_size = min(int(enriched_list_size), PG_INT4_MAX)
                    if enriched_list_size > 0:
                        set_parts.append(sql.SQL(
                            "{col} = CASE WHEN %s > COALESCE(profiles.{col}, 0) "
                            "THEN %s ELSE profiles.{col} END"
                        ).format(col=sql.Identifier('list_size')))
                        params.append(enriched_list_size)
                        params.append(enriched_list_size)
                        fields_written.append('list_size')
                except (ValueError, TypeError):
                    logger.warning(f"  Supabase write: invalid list_size '{enriched_list_size}' for {profile_id}")

            # social_reach: upgrade-only + source priority (R5)
            social_reach = result.get('social_reach')
            if social_reach is not None:
                try:
                    social_reach = min(int(social_reach), PG_INT4_MAX)
                    if social_reach > 0:
                        set_parts.append(sql.SQL(
                            "{col} = CASE WHEN %s > COALESCE(profiles.{col}, 0) "
                            "THEN %s ELSE profiles.{col} END"
                        ).format(col=sql.Identifier('social_reach')))
                        params.append(social_reach)
                        params.append(social_reach)
                        fields_written.append('social_reach')
                except (ValueError, TypeError):
                    logger.warning(f"  Supabase write: invalid social_reach '{social_reach}' for {profile_id}")

            # Exa-discovered email: source-aware (R5)
            exa_email = result.get('exa_email', '').strip()
            if exa_email and '@' in exa_email:
                if self._should_write_field('email', exa_email, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('email')))
                    params.append(exa_email)
                    fields_written.append('email')

            # Append any secondary emails (deduped — always append-only)
            secondary = result.get('secondary_emails', [])
            if secondary:
                for sec_email in secondary:
                    sec_email = sec_email.strip()
                    if sec_email and '@' in sec_email:
                        set_parts.append(sql.SQL(
                            "{col} = CASE "
                            "WHEN %s = ANY(COALESCE(profiles.{col}, '{{}}')) "
                            "OR lower(%s) = lower(COALESCE(profiles.{email_col}, '')) "
                            "THEN profiles.{col} "
                            "ELSE array_append(COALESCE(profiles.{col}, '{{}}'), %s) END"
                        ).format(
                            col=sql.Identifier('secondary_emails'),
                            email_col=sql.Identifier('email'),
                        ))
                        params.extend([sec_email, sec_email, sec_email])

            # Exa-discovered website/linkedin: source-aware (R5)
            discovered_website = result.get('discovered_website', '').strip()
            if discovered_website:
                if self._should_write_field('website', discovered_website, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('website')))
                    params.append(discovered_website)
                    fields_written.append('website')

            discovered_linkedin = result.get('discovered_linkedin', '').strip()
            if discovered_linkedin:
                if self._should_write_field('linkedin', discovered_linkedin, existing_meta, enrichment_source):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('linkedin')))
                    params.append(discovered_linkedin)
                    fields_written.append('linkedin')

            # --- Field-level provenance tracking (R1) + enrichment metadata (M3) ---
            if set_parts:
                set_parts.append(sql.SQL("last_enriched_at = %s"))
                params.append(enriched_at)
                set_parts.append(sql.SQL("updated_at = %s"))
                params.append(datetime.now())

                # Build field_meta for provenance tracking (R1)
                field_meta_update = {}
                now_iso = datetime.now().isoformat()
                for f in fields_written:
                    field_meta_update[f] = {
                        'source': enrichment_source,
                        'updated_at': now_iso,
                        'pipeline_version': PIPELINE_VERSION,
                    }

                # avatar_url: from Apollo cascade (fill-only)
                avatar_url = result.get('avatar_url', '').strip()
                if avatar_url and self._should_write_field('avatar_url', avatar_url, existing_meta, 'apollo'):
                    set_parts.append(sql.SQL(
                        "{col} = CASE WHEN COALESCE(profiles.{col}, '') = '' "
                        "THEN %s ELSE profiles.{col} END"
                    ).format(col=sql.Identifier('avatar_url')))
                    params.append(avatar_url)
                    fields_written.append('avatar_url')

                meta_payload = {
                    'last_enrichment': 'exa_pipeline',
                    'enriched_at': enriched_at.isoformat(),
                    'tier': result.get('_tier', 0),
                    'field_meta': field_meta_update,
                }

                # Include apollo_data from cascade enrichment
                apollo_data = result.get('_apollo_data')
                if apollo_data:
                    meta_payload['apollo_data'] = apollo_data
                    meta_payload['last_apollo_enrichment'] = datetime.now().isoformat()
                    # Track Apollo-sourced fields in field_meta
                    for f in fields_written:
                        if f in ('email', 'phone', 'linkedin', 'website', 'company',
                                 'business_size', 'revenue_tier', 'service_provided',
                                 'niche', 'avatar_url'):
                            # Check if this field came from Apollo (not Exa)
                            if result.get('_apollo_data') and not result.get('_extraction_metadata'):
                                field_meta_update[f] = {
                                    'source': 'apollo',
                                    'updated_at': now_iso,
                                    'pipeline_version': PIPELINE_VERSION,
                                }

                set_parts.append(sql.SQL(
                    "enrichment_metadata = COALESCE(enrichment_metadata, '{{}}'::jsonb) || %s::jsonb"
                ))
                params.append(json.dumps(meta_payload))

                params.append(profile_id)
                profile_updates.append((set_parts, params))

        # --- Execute writes using connection pool (P5/H1) ---
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                # Email updates — SAVEPOINT-wrapped (H4)
                if email_updates:
                    cursor.execute("SAVEPOINT email_batch")
                    try:
                        execute_batch(cursor, """
                            UPDATE profiles
                            SET email = %s,
                                enrichment_metadata = jsonb_set(
                                    COALESCE(enrichment_metadata, '{}'::jsonb),
                                    '{email}',
                                    %s::jsonb
                                ),
                                profile_confidence = %s,
                                last_enriched_at = %s,
                                updated_at = %s
                            WHERE id = %s
                        """, email_updates)
                        cursor.execute("RELEASE SAVEPOINT email_batch")
                    except Exception as e:
                        logger.error(f"Email batch update failed: {e}")
                        cursor.execute("ROLLBACK TO SAVEPOINT email_batch")

                # Profile field updates — grouped batch execution
                failed_updates = 0
                groups = {}  # template_key → (query, [params_list])
                for set_parts, params in profile_updates:
                    set_clause = sql.SQL(", ").join(set_parts)
                    template_key = tuple(repr(sp) for sp in set_parts)
                    if template_key not in groups:
                        query = sql.SQL("UPDATE profiles SET {} WHERE id = %s").format(set_clause)
                        groups[template_key] = (query, [])
                    groups[template_key][1].append(params)

                for group_idx, (template_key, (query, param_list)) in enumerate(groups.items()):
                    cursor.execute(f"SAVEPOINT batch_group_{group_idx}")
                    try:
                        execute_batch(cursor, query, param_list)
                        cursor.execute(f"RELEASE SAVEPOINT batch_group_{group_idx}")
                    except Exception as e:
                        logger.warning(f"Batch group {group_idx} failed ({len(param_list)} profiles): {e}")
                        cursor.execute(f"ROLLBACK TO SAVEPOINT batch_group_{group_idx}")
                        # Fallback: per-profile execution for failed group
                        for j, p in enumerate(param_list):
                            try:
                                cursor.execute(f"SAVEPOINT fallback_{group_idx}_{j}")
                                cursor.execute(query, p)
                                cursor.execute(f"RELEASE SAVEPOINT fallback_{group_idx}_{j}")
                            except Exception as e2:
                                failed_updates += 1
                                logger.warning(f"Profile update fallback failed: {e2}")
                                try:
                                    cursor.execute(f"ROLLBACK TO SAVEPOINT fallback_{group_idx}_{j}")
                                except Exception:
                                    pass
                if failed_updates:
                    logger.warning(f"  {failed_updates}/{len(profile_updates)} profile updates failed")

                conn.commit()
            finally:
                cursor.close()

        # Write quarantined profiles to JSONL for later retry
        if quarantined_records and not self.dry_run:
            quarantine_dir = self._ensure_quarantine_dir()
            quarantine_file = os.path.join(
                quarantine_dir,
                f"quarantine_{datetime.now().strftime('%Y%m%d')}.jsonl"
            )
            with open(quarantine_file, 'a', encoding='utf-8') as f:
                for record in quarantined_records:
                    f.write(json.dumps(record) + '\n')

        print(f"\n  Gate: {self.stats['gate_verified']} verified, "
              f"{self.stats['gate_unverified']} unverified, "
              f"{self.stats['gate_quarantined']} quarantined")
        print(f"  Batch updated {len(email_updates)} email(s) + "
              f"{len(profile_updates)} profile field set(s) to Supabase")


def main():
    import argparse

    # P6: Validate DATABASE_URL before anything else
    if not os.environ.get('DATABASE_URL'):
        print("ERROR: DATABASE_URL environment variable is not set.")
        print("Set it in .env or export it: export DATABASE_URL='postgresql://...'")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='Safe enrichment pipeline (NO GUESSING)')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--priority', type=str, default='tiered',
                        choices=['tiered', 'high-value', 'has-website', 'all'])
    parser.add_argument('--max-apollo-credits', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=5)
    parser.add_argument('--auto-consolidate', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--concurrency', type=int, default=5,
                        help='Number of profiles to process concurrently (default 5)')
    parser.add_argument('--tier', type=int, default=None,
                        help='Run only a specific tier (0-5)')
    parser.add_argument('--tiers', type=str, default=None,
                        help='Comma-separated list of tiers to run (e.g. 1,2,3)')
    parser.add_argument('--phase', type=int, default=None,
                        help='Phase number for reporting (1-4)')
    parser.add_argument('--skip-social-reach', action='store_true', default=False,
                        help='Skip social reach scraping (Facebook/YouTube follower counts)')
    parser.add_argument('--exa-only', action='store_true', default=False,
                        help='Use only Exa.ai — skip crawl4ai + Claude fallback')
    parser.add_argument('--cache-only', action='store_true', default=False,
                        help='Only use cached results (no API calls) — for re-consolidation')
    # R3: Refresh mode for monthly re-enrichment
    parser.add_argument('--refresh', action='store_true', default=False,
                        help='Re-enrich stale profiles (uses source priority to protect client data)')
    parser.add_argument('--stale-days', type=int, default=30,
                        help='In refresh mode, re-enrich profiles older than N days (default 30)')
    # Cascade enrichment: Exa → Apollo (full) → OWL
    parser.add_argument('--cascade', action='store_true', default=False,
                        help='Enable cascade: Exa first, then full Apollo on gaps, then OWL')
    parser.add_argument('--owl-fallback', action='store_true', default=False,
                        help='Enable OWL deep research as final cascade fallback')

    args = parser.parse_args()

    # Parse tier filter
    tier_filter = None
    if args.tier is not None:
        tier_filter = {args.tier}
    elif args.tiers:
        tier_filter = {int(t.strip()) for t in args.tiers.split(',')}

    pipeline = SafeEnrichmentPipeline(
        max_apollo_credits=args.max_apollo_credits,
        dry_run=args.dry_run,
        batch_size=args.batch_size
    )

    # R3: Set refresh mode state
    if args.refresh:
        pipeline.refresh_mode = True
        pipeline.stale_days = args.stale_days

    try:
        asyncio.run(pipeline.run(
            limit=args.limit,
            priority=args.priority,
            auto_consolidate=args.auto_consolidate,
            concurrency=args.concurrency,
            tier_filter=tier_filter,
            phase_number=args.phase,
            skip_social_reach=args.skip_social_reach,
            exa_only=args.exa_only,
            cache_only=args.cache_only,
            cascade=args.cascade,
            owl_fallback=args.owl_fallback,
        ))
    finally:
        pipeline.cleanup()


if __name__ == '__main__':
    main()
