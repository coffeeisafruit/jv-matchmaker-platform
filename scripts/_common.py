"""
Shared utilities for enrichment scripts.

Consolidates duplicated code (cache_key, db connection, JSON extraction,
Claude CLI calls, research cache saving) into a single importable module.
"""
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Project paths ─────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_ROOT, 'Chelsea_clients', 'research_cache')

# ── Domains to skip (booking links, social, linktree) ────────────
SKIP_DOMAINS = [
    'calendly.com', 'acuityscheduling.com', 'tidycal.com',
    'oncehub.com', 'youcanbook.me', 'bookme.',
    'linktr.ee', 'linktree.com',
    'facebook.com', 'instagram.com', 'twitter.com',
    'linkedin.com', 'tiktok.com',
]


def setup_django():
    """Bootstrap Django for standalone scripts."""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    import django
    django.setup()


def cache_key(name: str) -> str:
    """Generate a deterministic 12-char hex key from a name."""
    return hashlib.md5(name.lower().encode()).hexdigest()[:12]


def get_db_connection():
    """Get psycopg2 connection + RealDictCursor using DATABASE_URL from .env."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from dotenv import load_dotenv
    load_dotenv()
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def extract_json_from_claude(text: str) -> Optional[Dict]:
    """Strip markdown fences and extract JSON dict from Claude response text."""
    if not text:
        return None

    text = text.strip()

    # Strip markdown code fences
    if text.startswith('```'):
        lines = text.split('\n')
        text = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])
        text = text.strip()

    # Try direct parse
    if text.startswith('{'):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    # Regex fallback: find first JSON object
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def call_claude_cli(prompt: str, model: str = 'haiku', timeout: int = 120) -> Optional[Dict]:
    """Call Claude CLI and return parsed JSON dict, or None on failure."""
    try:
        result = subprocess.run(
            ['claude', '--print', '--model', model, '-p', prompt],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            logger.warning(f"Claude CLI exited with code {result.returncode}")
            return None
        return extract_json_from_claude(result.stdout)
    except subprocess.TimeoutExpired:
        logger.warning(f"Claude CLI timed out after {timeout}s")
        return None
    except Exception as e:
        logger.error(f"Claude CLI error: {e}")
        return None


def save_to_research_cache(
    name: str,
    data: Dict,
    flag_name: str,
    cache_dir: str = None,
    overwrite: bool = False,
) -> bool:
    """
    Save enrichment results to research cache JSON file.

    Loads existing cache entry, merges new data, sets flag, writes back.

    Args:
        name: Person's name (used to derive cache key)
        data: Extracted data dict to save
        flag_name: Flag to set (e.g. '_crawl4ai_enriched', '_owl_enriched')
        cache_dir: Override cache directory (defaults to CACHE_DIR)
        overwrite: If True, overwrite existing values. If False (default),
                   only fill empty fields.
    """
    cache_dir = cache_dir or CACHE_DIR
    key = cache_key(name)
    cache_path = os.path.join(cache_dir, f'{key}.json')

    existing = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                existing = json.load(f)
        except Exception:
            pass

    merged = dict(existing)
    merged['name'] = name
    merged[flag_name] = True

    # Derive timestamp key: _crawl4ai_enriched → _crawl4ai_timestamp
    if flag_name.endswith('_enriched'):
        ts_key = flag_name[:-len('_enriched')] + '_timestamp'
    else:
        ts_key = flag_name + '_timestamp'
    merged[ts_key] = datetime.now().isoformat()
    merged['_cache_schema_version'] = 2

    for field, value in data.items():
        if field in ('confidence', 'source_quotes'):
            continue
        if value and str(value) not in ('', '[]', '{}', 'null', 'None'):
            if overwrite:
                merged[field] = value
            else:
                existing_val = merged.get(field)
                if not existing_val or str(existing_val) in ('', '[]', '{}', 'None'):
                    merged[field] = value

    try:
        with open(cache_path, 'w') as f:
            json.dump(merged, f, indent=2, default=str)
        return True
    except Exception as e:
        logger.error(f"Failed to save cache for {name}: {e}")
        return False
