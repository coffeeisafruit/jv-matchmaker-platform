"""
Prefect @task: Semantic triage of detected content changes.

Layer 2 of the change-detection pipeline.  For each profile whose content
hashes have changed, sends old vs new text snippets to Claude and asks for
a MATERIAL vs COSMETIC classification.

Cost: ~$0.008 per call (Haiku-class prompt with short context).

MATERIAL changes (trigger re-enrichment):
  - New offering, pricing change, niche shift, new program
  - New partnership, changed target audience
  - New credentials / certifications

COSMETIC changes (log and skip):
  - Rewording, layout changes, minor edits, copyright year update
  - Testimonial additions, blog posts
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from prefect import task, get_run_logger

from matching.enrichment.flows.content_hash_check import (
    HashCheckResult,
    _fetch_page,
    _clean_html,
    _normalise_base_url,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_SNIPPET_CHARS = 2000  # max chars per old/new snippet sent to Claude

_TRIAGE_PROMPT_TEMPLATE = """\
You are a business profile change analyst.  Compare the OLD and NEW website \
text for a JV partner profile and classify the change.

**Profile:** {name} ({website})
**Pages changed:** {pages_changed}

--- OLD TEXT (before) ---
{old_text}

--- NEW TEXT (after) ---
{new_text}

Classify the change as one of:
- "material": New offering, pricing change, niche shift, new program, new \
partnership, changed target audience, new credentials/certifications, \
significant service change.
- "cosmetic": Rewording, layout change, minor edits, copyright year update, \
testimonial additions, blog posts, formatting tweaks.

Respond with ONLY a JSON object (no markdown fences):
{{"classification": "material" or "cosmetic", "confidence": 0.0-1.0, \
"summary": "one-sentence description of what changed", \
"affected_fields": ["seeking", "offering", "who_you_serve", ...]}}

The affected_fields list should contain only fields from this set:
seeking, offering, who_you_serve, what_you_do, niche, signature_programs, \
revenue_tier, jv_history, content_platforms, company, bio.
"""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class TriageResult:
    """Result of semantic triage for a changed profile."""

    profile_id: str
    name: str
    classification: str  # "material" or "cosmetic"
    confidence: float  # 0-1
    change_summary: str  # Human-readable summary of what changed
    affected_fields: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get_api_config() -> tuple[str, str, str]:
    """Return (base_url, api_key, model) from environment.

    Prefers OpenRouter; falls back to direct Anthropic.
    """
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if openrouter_key:
        return (
            "https://openrouter.ai/api/v1/chat/completions",
            openrouter_key,
            "anthropic/claude-sonnet-4",
        )
    elif anthropic_key:
        return (
            "https://api.anthropic.com/v1/messages",
            anthropic_key,
            "claude-sonnet-4-20250514",
        )
    else:
        raise RuntimeError(
            "No API key found. Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY."
        )


def _call_claude(prompt: str) -> Optional[str]:
    """Call Claude via OpenRouter (preferred) or Anthropic and return text."""
    base_url, api_key, model = _get_api_config()

    if "openrouter" in base_url:
        payload = {
            "model": model,
            "max_tokens": 512,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        resp = httpx.post(base_url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    else:
        # Direct Anthropic messages API
        payload = {
            "model": model,
            "max_tokens": 512,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        resp = httpx.post(base_url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]


def _parse_triage_json(text: str) -> Optional[dict]:
    """Parse Claude's JSON response, tolerating markdown fences."""
    if not text:
        return None
    cleaned = text.strip()
    if "```json" in cleaned:
        start = cleaned.find("```json") + 7
        end = cleaned.find("```", start)
        cleaned = cleaned[start:end].strip()
    elif "```" in cleaned:
        start = cleaned.find("```") + 3
        end = cleaned.find("```", start)
        cleaned = cleaned[start:end].strip()
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse triage JSON: %s", cleaned[:200])
        return None


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="semantic-triage", retries=1, retry_delay_seconds=10)
def semantic_triage(
    profile: dict,
    hash_result: HashCheckResult,
    old_content: str = "",
    new_content: str = "",
) -> TriageResult:
    """Classify a detected content change as MATERIAL or COSMETIC using Claude.

    If *old_content* / *new_content* are not provided, the task attempts to
    reconstruct snippets by re-fetching the changed pages (new) and using
    the profile's cached data as a proxy for the old content.

    Parameters
    ----------
    profile:
        Profile dict (id, name, website, enrichment_metadata).
    hash_result:
        HashCheckResult from Layer 1 showing which pages changed.
    old_content:
        Optional pre-fetched old text (e.g. from cache).
    new_content:
        Optional pre-fetched new text.

    Returns
    -------
    TriageResult
    """
    log = get_run_logger()
    pid = str(profile.get("id", ""))
    name = profile.get("name", "")
    website = (profile.get("website") or "").strip()

    # --- Build text snippets if not provided ---
    if not new_content and website:
        base_url = _normalise_base_url(website)
        snippets: list[str] = []
        for page_key in hash_result.pages_changed:
            # Reconstruct URL from page key
            if page_key == "homepage":
                url = base_url
            elif page_key == "about":
                url = base_url.rstrip("/") + "/about"
            elif page_key == "services":
                url = base_url.rstrip("/") + "/services"
            else:
                continue
            html = _fetch_page(url)
            if html:
                snippets.append(_clean_html(html))
        new_content = "\n---\n".join(snippets)

    if not old_content:
        # Use existing enrichment fields as a proxy for "old" content
        em = profile.get("enrichment_metadata") or {}
        parts: list[str] = []
        for fld in ("what_you_do", "who_you_serve", "seeking", "offering", "bio"):
            val = profile.get(fld) or em.get(fld, "")
            if val:
                parts.append(f"{fld}: {val}")
        old_content = "\n".join(parts)

    # Truncate to budget
    old_text = old_content[:_MAX_SNIPPET_CHARS]
    new_text = new_content[:_MAX_SNIPPET_CHARS]

    prompt = _TRIAGE_PROMPT_TEMPLATE.format(
        name=name,
        website=website,
        pages_changed=", ".join(hash_result.pages_changed),
        old_text=old_text or "(no prior content available)",
        new_text=new_text or "(could not fetch new content)",
    )

    # Call Claude
    try:
        raw_response = _call_claude(prompt)
    except Exception as exc:
        log.error("Claude API call failed for %s: %s", name, exc)
        return TriageResult(
            profile_id=pid,
            name=name,
            classification="cosmetic",
            confidence=0.0,
            change_summary=f"API error: {exc}",
        )

    parsed = _parse_triage_json(raw_response)
    if not parsed:
        log.warning("Could not parse triage response for %s, defaulting to cosmetic", name)
        return TriageResult(
            profile_id=pid,
            name=name,
            classification="cosmetic",
            confidence=0.0,
            change_summary="Unparseable API response",
        )

    classification = parsed.get("classification", "cosmetic").lower()
    if classification not in ("material", "cosmetic"):
        classification = "cosmetic"

    result = TriageResult(
        profile_id=pid,
        name=name,
        classification=classification,
        confidence=min(1.0, max(0.0, float(parsed.get("confidence", 0.5)))),
        change_summary=parsed.get("summary", ""),
        affected_fields=parsed.get("affected_fields", []),
    )

    log.info(
        "Triage for %s: %s (conf=%.2f) — %s",
        name, result.classification, result.confidence, result.change_summary,
    )
    return result


@task(name="triage-batch")
def triage_batch(
    profiles: list[dict],
    hash_results: list[HashCheckResult],
) -> list[TriageResult]:
    """Batch semantic triage for all profiles with detected changes.

    Pairs each profile with its corresponding hash result by profile_id,
    then runs ``semantic_triage`` sequentially (to stay within API rate
    limits and keep costs predictable).

    Parameters
    ----------
    profiles:
        List of profile dicts (only those with detected changes).
    hash_results:
        Corresponding HashCheckResults (same order/length as *profiles*).

    Returns
    -------
    list[TriageResult]
    """
    log = get_run_logger()
    log.info("Starting semantic triage for %d changed profiles", len(profiles))

    # Build lookup by profile_id for safety
    hr_by_id = {hr.profile_id: hr for hr in hash_results}

    results: list[TriageResult] = []
    for profile in profiles:
        pid = str(profile.get("id", ""))
        hr = hr_by_id.get(pid)
        if not hr:
            log.warning("No hash result for profile %s, skipping triage", pid)
            continue

        try:
            tr = semantic_triage.fn(profile=profile, hash_result=hr)
            results.append(tr)
        except Exception as exc:
            log.error("Triage failed for %s: %s", pid, exc)

        # Rate-limit: ~1 call/sec to stay well under API limits
        time.sleep(1.0)

    material = sum(1 for r in results if r.classification == "material")
    cosmetic = sum(1 for r in results if r.classification == "cosmetic")
    log.info(
        "Triage complete: %d material, %d cosmetic out of %d",
        material, cosmetic, len(results),
    )

    return results
