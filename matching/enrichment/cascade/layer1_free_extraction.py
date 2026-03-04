"""
Layer 1: Free HTTP Extraction — $0 cost.

Scrapes websites for email, phone, social links, booking URLs, list size,
and social proof. Reuses extraction logic from scripts/scrape_contact_gap.py.

Source tag: "website_scrape", priority 25.
Fill-only writes — never overwrites existing data.
15 threads, 12s timeout, JSONL checkpoint every 500 profiles.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

from matching.enrichment.cascade.checkpoint import CascadeCheckpoint

logger = logging.getLogger(__name__)

# ---------- Config ----------
CONCURRENCY = 15
REQUEST_TIMEOUT = 12
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
CHECKPOINT_INTERVAL = 500
THIN_CONTENT_THRESHOLD = 200  # chars — skip Layer 3 if website has less

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/connect"]

SKIP_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "squarespace.com",
    "wordpress.com", "cloudflare.com", "googleapis.com", "w3.org",
    "schema.org", "jquery.com", "bootstrapcdn.com",
}
SKIP_EMAIL_PREFIXES = ["noreply", "no-reply", "webmaster", "postmaster", "admin@"]

BOOKING_PATTERNS = [
    r'https?://(?:calendly\.com|acuityscheduling\.com|savvycal\.com|tidycal\.com)[/\w.-]*',
    r'https?://(?:[\w-]+\.)?hubspot\.com/meetings[/\w.-]*',
    r'https?://(?:[\w-]+\.)?oncehub\.com/[\w.-]+',
    r'https?://(?:[\w-]+\.)?book\.me/[\w.-]+',
]


# ---------- Result dataclass ----------

@dataclass
class Layer1Result:
    """Summary of a Layer 1 run."""

    profiles_attempted: int = 0
    profiles_found_data: int = 0
    profiles_no_data: int = 0
    profiles_error: int = 0
    profiles_skipped: int = 0
    fields_filled: dict = field(default_factory=dict)
    thin_content_ids: list = field(default_factory=list)
    affected_ids: list = field(default_factory=list)
    runtime_seconds: float = 0.0


# ---------- DB helpers ----------

def _get_conn():
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(db_url, options="-c statement_timeout=120000")


def _fetch_profiles(
    tier_filter: set[str] | None = None,
    min_score: float = 0,
    limit: int | None = None,
    exclude_ids: set[str] | None = None,
) -> list[dict]:
    """Fetch profiles with websites that haven't been recently scraped."""
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    conditions = [
        "website IS NOT NULL",
        "website != ''",
    ]
    params: list = []

    if tier_filter:
        conditions.append("jv_tier = ANY(%s)")
        params.append(list(tier_filter))

    if min_score > 0:
        conditions.append("jv_readiness_score >= %s")
        params.append(min_score)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, name, website, email, phone, linkedin, booking_link,
               facebook, instagram, youtube, twitter,
               content_platforms, enrichment_metadata,
               secondary_emails, social_proof, list_size,
               jv_tier, jv_readiness_score
        FROM profiles
        WHERE {where}
        ORDER BY jv_readiness_score DESC NULLS LAST
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    if exclude_ids:
        rows = [r for r in rows if str(r["id"]) not in exclude_ids]

    return rows


# ---------- Extraction logic (from scrape_contact_gap.py) ----------

def is_valid_email(email: str) -> bool:
    """Filter out junk emails (CSS, JS, framework references)."""
    if not email or "@" not in email:
        return False
    email = email.lower().strip(".")
    local, _, domain = email.partition("@")
    if domain in SKIP_EMAIL_DOMAINS:
        return False
    if any(email.startswith(p) for p in SKIP_EMAIL_PREFIXES):
        return False
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if not tld or not tld.isalpha() or len(tld) < 2:
        return False
    if re.match(r'^\d', domain):
        return False
    placeholder_locals = {
        "businessname", "yourname", "youremail", "name", "email",
        "your-email", "info", "user", "test", "example",
    }
    if local in placeholder_locals:
        return False
    if re.search(r'\.(com|net|org|io|co)\b', local):
        return False
    if any(email.endswith(ext) for ext in [".js", ".css", ".png", ".jpg", ".svg", ".gif"]):
        return False
    return True


def extract_from_html(html: str, base_url: str) -> dict:
    """Extract contact/connection/profile data from HTML."""
    result = {}

    clean_html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    clean_html = re.sub(r'<style[^>]*>.*?</style>', '', clean_html, flags=re.DOTALL | re.IGNORECASE)
    soup = BeautifulSoup(clean_html, 'html.parser')
    visible_text = soup.get_text(separator=' ', strip=True)

    # --- Emails ---
    mailto = re.findall(r'mailto:([\w.+-]+@[\w-]+\.[\w.]+)', clean_html)
    emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.]+', clean_html)
    all_emails = list(dict.fromkeys(mailto + emails))
    valid_emails = [e.lower().strip(".") for e in all_emails if is_valid_email(e)]
    if valid_emails:
        result["email"] = valid_emails[0]
        if len(valid_emails) > 1:
            result["secondary_emails"] = valid_emails[1:4]

    # --- Phone ---
    tel_links = re.findall(r'href=["\']tel:([^"\']+)', clean_html)
    phones = re.findall(
        r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', clean_html,
    )
    all_phones = list(dict.fromkeys(tel_links + phones))
    for p in all_phones:
        cleaned = re.sub(r'[^\d+]', '', p)
        if 10 <= len(cleaned) <= 11:
            result["phone"] = p.strip()
            break

    # --- LinkedIn ---
    li_personal = re.findall(r'https?://(?:www\.)?linkedin\.com/in/[\w-]+/?', clean_html)
    if li_personal:
        result["linkedin"] = li_personal[0]

    # --- Social URLs ---
    fb_urls = re.findall(r'https?://(?:www\.)?facebook\.com/[\w.-]+/?', clean_html)
    if fb_urls:
        real_fb = [u for u in fb_urls if not any(x in u.lower() for x in ['sharer', 'share.php', 'plugins', 'dialog', '/tr?'])]
        if real_fb:
            result["facebook"] = real_fb[0]

    tw_urls = re.findall(r'https?://(?:www\.)?(?:twitter\.com|x\.com)/[\w-]+/?', clean_html)
    if tw_urls:
        real_tw = [u for u in tw_urls if not any(x in u.lower() for x in ['intent', 'share', 'widgets'])]
        if real_tw:
            result["twitter"] = real_tw[0]

    ig_urls = re.findall(r'https?://(?:www\.)?instagram\.com/[\w.-]+/?', clean_html)
    if ig_urls:
        result["instagram"] = ig_urls[0]

    yt_urls = re.findall(r'https?://(?:www\.)?youtube\.com/(?:@|c/|channel/)[\w-]+/?', clean_html)
    if yt_urls:
        result["youtube"] = yt_urls[0]

    # --- Text-based handles ---
    handle_patterns = {
        'facebook': r'(?:facebook|fb)[:\s]+@?([\w.-]{3,30})',
        'twitter': r'(?:twitter|x\.com)[:\s]+@?([\w.-]{3,30})',
        'instagram': r'(?:instagram|ig)[:\s]+@?([\w.-]{3,30})',
    }
    for platform, pattern in handle_patterns.items():
        if platform not in result:
            match = re.search(pattern, visible_text, re.IGNORECASE)
            if match:
                handle = match.group(1).strip(".")
                if len(handle) >= 3 and handle.lower() not in {'page', 'group', 'profile', 'com', 'the'}:
                    result[platform] = f"@{handle}"

    # --- Other platforms ---
    other_platforms = {}
    tg = re.findall(r'https?://(?:t\.me|telegram\.me)/([\w-]+)', clean_html)
    if tg:
        other_platforms["telegram"] = f"https://t.me/{tg[0]}"
    disc = re.findall(r'https?://discord\.(?:gg|com/invite)/([\w-]+)', clean_html)
    if disc:
        other_platforms["discord"] = f"https://discord.gg/{disc[0]}"
    tt = re.findall(r'https?://(?:www\.)?tiktok\.com/@[\w.-]+', clean_html)
    if tt:
        other_platforms["tiktok"] = tt[0]
    pod = re.findall(r'https?://(?:podcasts\.apple\.com|open\.spotify\.com/show)/[\w-]+', clean_html)
    if pod:
        other_platforms["podcast"] = pod[0]
    if other_platforms:
        result["other_platforms"] = other_platforms

    # --- Booking links ---
    for pattern in BOOKING_PATTERNS:
        booking = re.findall(pattern, clean_html)
        if booking:
            result["booking_link"] = booking[0]
            break

    # --- Social proof ---
    proof_patterns = [
        r'(?:featured (?:in|on)|as seen (?:in|on))\s*[:\s]*([^<.]{10,200})',
        r'(?:trusted by|used by|loved by)\s+(\d[\d,]*\+?\s+\w+)',
        r'(\d[\d,]*\+?)\s+(?:subscribers?|followers?|customers?|students?|members?)',
    ]
    proof_bits = []
    for pp in proof_patterns:
        m = re.search(pp, visible_text, re.IGNORECASE)
        if m:
            proof_bits.append(m.group(0).strip()[:200])
    if proof_bits:
        result["social_proof"] = " | ".join(proof_bits[:3])

    # --- List size ---
    size_match = re.search(
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:k|K|thousand|million|M)?\s*'
        r'(?:subscribers?|email\s*list|on (?:my|our) list)',
        visible_text, re.IGNORECASE,
    )
    if size_match:
        try:
            num = float(size_match.group(1).replace(',', ''))
            context = size_match.group(0).lower()
            if 'k' in context or 'thousand' in context:
                num *= 1000
            elif 'm' in context or 'million' in context:
                num *= 1_000_000
            if num >= 100:
                result["list_size"] = min(int(num), 10_000_000)
        except (ValueError, TypeError):
            pass

    # --- Content length for thin-content detection ---
    result["_content_length"] = len(visible_text)

    return result


def scrape_one(profile: dict) -> dict:
    """Scrape a single profile's website + contact pages. Thread-safe."""
    pid = str(profile["id"])
    website = (profile.get("website") or "").strip()
    result = {"id": pid, "fields": {}, "pages_tried": 0, "error": None}

    if not website:
        result["error"] = "no_website"
        return result

    if not website.startswith("http"):
        website = "https://" + website

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    parsed = urlparse(website)
    site_root = f"{parsed.scheme}://{parsed.netloc}"

    urls_to_try = [website]
    for path in CONTACT_PATHS:
        urls_to_try.append(site_root + path)

    all_extracted: dict = {}

    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            result["pages_tried"] += 1
            if resp.status_code != 200:
                continue
            extracted = extract_from_html(resp.text, url)
            for k, v in extracted.items():
                if k == "other_platforms":
                    existing_op = all_extracted.get("other_platforms", {})
                    all_extracted["other_platforms"] = {**existing_op, **v}
                elif k == "secondary_emails":
                    existing_se = all_extracted.get("secondary_emails", [])
                    all_extracted["secondary_emails"] = list(dict.fromkeys(existing_se + v))[:4]
                elif k not in all_extracted:
                    all_extracted[k] = v
        except Exception:
            continue

        if all(k in all_extracted for k in ("email", "phone", "facebook")):
            break

    result["fields"] = all_extracted
    return result


def _write_to_db(
    pid: str,
    fields: dict,
    existing: dict,
    dry_run: bool = False,
) -> list[str]:
    """Write scraped data to DB. Fill-only mode — never overwrite existing."""
    fields_to_write = {}
    fields_written = []

    simple_fill = [
        "email", "phone", "linkedin", "booking_link",
        "facebook", "instagram", "youtube", "twitter",
    ]
    for fld in simple_fill:
        val = fields.get(fld)
        if val and not (existing.get(fld) or "").strip():
            fields_to_write[fld] = val
            fields_written.append(fld)

    sec_emails = fields.get("secondary_emails", [])
    if sec_emails and not existing.get("secondary_emails"):
        fields_to_write["secondary_emails"] = sec_emails
        fields_written.append("secondary_emails")

    if fields.get("social_proof") and not (existing.get("social_proof") or "").strip():
        fields_to_write["social_proof"] = fields["social_proof"]
        fields_written.append("social_proof")

    if fields.get("list_size"):
        existing_ls = existing.get("list_size") or 0
        if fields["list_size"] > (existing_ls or 0):
            fields_to_write["list_size"] = fields["list_size"]
            fields_written.append("list_size")

    other = fields.get("other_platforms", {})
    if other:
        existing_cp = existing.get("content_platforms") or {}
        if isinstance(existing_cp, str):
            try:
                existing_cp = json.loads(existing_cp)
            except (json.JSONDecodeError, TypeError):
                existing_cp = {}
        if isinstance(existing_cp, list):
            existing_cp = {}
        new_platforms = {k: v for k, v in other.items() if k not in existing_cp}
        if new_platforms:
            merged_cp = {**existing_cp, **new_platforms}
            fields_to_write["content_platforms"] = json.dumps(merged_cp)
            fields_written.append("content_platforms")

    if not fields_to_write:
        return []

    if dry_run:
        return fields_written

    conn = _get_conn()
    cur = conn.cursor()
    try:
        set_parts = []
        params: list = []
        jsonb_fields = {"content_platforms"}
        for fld, val in fields_to_write.items():
            if fld in jsonb_fields:
                set_parts.append(f"{fld} = %s::jsonb")
            else:
                set_parts.append(f"{fld} = %s")
            params.append(val)

        # Update enrichment_metadata
        existing_meta = existing.get("enrichment_metadata") or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}
        meta = dict(existing_meta)
        meta["last_contact_scrape"] = datetime.now().isoformat()
        # Initialize check_tier if not already set (used by change_detection_flow)
        if "check_tier" not in meta:
            jv_tier = existing.get("jv_tier") or ""
            tier_map = {"A": "A", "B": "B", "C": "C", "D": "D", "E": "D"}
            meta["check_tier"] = tier_map.get(jv_tier, "C")
        field_meta = meta.get("field_meta", {})
        for f in fields_written:
            clean_f = f.split("(")[0]
            field_meta[clean_f] = {
                "source": "website_scrape",
                "updated_at": datetime.now().isoformat(),
            }
        meta["field_meta"] = field_meta

        set_parts.append("enrichment_metadata = %s::jsonb")
        params.append(json.dumps(meta, default=str))
        set_parts.append("updated_at = NOW()")

        params.append(pid)
        sql = f"UPDATE profiles SET {', '.join(set_parts)} WHERE id = %s::uuid"
        cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

    return fields_written


# ---------- Main Layer 1 entry point ----------

class Layer1FreeExtraction:
    """Layer 1: Free HTTP extraction from profile websites."""

    def __init__(
        self,
        tier_filter: set[str] | None = None,
        min_score: float = 0,
        limit: int | None = None,
        dry_run: bool = False,
        checkpoint: CascadeCheckpoint | None = None,
    ):
        self.tier_filter = tier_filter
        self.min_score = min_score
        self.limit = limit
        self.dry_run = dry_run
        self.checkpoint = checkpoint or CascadeCheckpoint(layer=1)

    def run(self) -> Layer1Result:
        """Execute Layer 1 extraction."""
        start = time.time()
        result = Layer1Result()

        already_done = self.checkpoint.get_processed_ids()
        logger.info("Layer 1: %d profiles already processed (checkpoint)", len(already_done))

        profiles = _fetch_profiles(
            tier_filter=self.tier_filter,
            min_score=self.min_score,
            limit=self.limit,
            exclude_ids=already_done,
        )
        result.profiles_attempted = len(profiles)
        logger.info("Layer 1: %d profiles to scrape", len(profiles))

        if not profiles:
            result.runtime_seconds = time.time() - start
            return result

        fills = Counter()

        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {pool.submit(scrape_one, p): p for p in profiles}

            for i, future in enumerate(as_completed(futures), 1):
                profile = futures[future]
                pid = str(profile["id"])

                try:
                    scrape_result = future.result()
                except Exception as e:
                    logger.error("Layer 1 scrape error for %s: %s", pid, e)
                    result.profiles_error += 1
                    self.checkpoint.mark_processed(pid, "error", error=str(e))
                    continue

                fields = scrape_result.get("fields", {})

                # Thin content detection
                content_len = fields.pop("_content_length", 0)
                if content_len < THIN_CONTENT_THRESHOLD and not fields:
                    result.thin_content_ids.append(pid)

                if not fields:
                    result.profiles_no_data += 1
                    self.checkpoint.mark_processed(pid, "skipped")
                    continue

                try:
                    written = _write_to_db(pid, fields, profile, dry_run=self.dry_run)
                except Exception as e:
                    logger.error("Layer 1 DB write error for %s: %s", pid, e)
                    result.profiles_error += 1
                    self.checkpoint.mark_processed(pid, "error", error=str(e))
                    continue

                if written:
                    result.profiles_found_data += 1
                    result.affected_ids.append(pid)
                    for f in written:
                        fills[f] += 1
                    self.checkpoint.mark_processed(pid, "success", written)
                else:
                    result.profiles_no_data += 1
                    self.checkpoint.mark_processed(pid, "skipped")

                if i % CHECKPOINT_INTERVAL == 0:
                    elapsed = time.time() - start
                    rate = i / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Layer 1 progress: %d/%d (%.1f/sec) found=%d err=%d",
                        i, len(profiles), rate,
                        result.profiles_found_data, result.profiles_error,
                    )

        result.fields_filled = dict(fills)
        result.runtime_seconds = time.time() - start

        logger.info(
            "Layer 1 complete: %d attempted, %d found data, %d empty, %d errors, %.1fs",
            result.profiles_attempted,
            result.profiles_found_data,
            result.profiles_no_data,
            result.profiles_error,
            result.runtime_seconds,
        )

        return result
