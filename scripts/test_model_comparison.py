#!/usr/bin/env python3
"""
A/B test: Compare Claude vs OSS model for profile extraction quality.

Picks random contacts with websites from the master CSV, crawls each site,
runs the same extraction prompt through both models (via OpenRouter), and
compares outputs field-by-field.

Usage:
    # Compare Claude Sonnet vs Qwen3-30B (default)
    python3 scripts/test_model_comparison.py --sample-size 10

    # Compare against a specific model
    python3 scripts/test_model_comparison.py --challenger qwen/qwen3-32b --sample-size 20

    # Skip crawling (use cached content from a prior run)
    python3 scripts/test_model_comparison.py --use-cache

Requires: OPENROUTER_API_KEY in environment.
"""

import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Lightweight fetch (no Crawl4AI dependency — uses requests + BS4)
# ---------------------------------------------------------------------------

def simple_fetch(url: str, max_chars: int = 15000) -> Optional[str]:
    """Fetch a single page and extract text. Returns None on failure."""
    import requests
    from bs4 import BeautifulSoup

    if not url.startswith("http"):
        url = "https://" + url

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"Fetch failed for {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    lines = (line.strip() for line in text.splitlines())
    text = "\n".join(line for line in lines if line)
    return text[:max_chars] if text else None


# ---------------------------------------------------------------------------
# Prompt (identical to ai_research.py Prompt 1)
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated on the website.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Website: {website}

Website Content (from multiple pages):
<content>
{content}
</content>

Extract the following fields. Only include information that is DIRECTLY stated:

1. what_you_do: What is their primary business/service? (1-2 sentences max)
2. who_you_serve: Who is their target audience? (1 sentence max)
3. seeking: What are they actively looking for? (partnerships, speaking, affiliates, etc.)
   - If nothing explicitly stated, leave EMPTY
4. offering: What do they offer to partners/collaborators?
5. social_proof: Any notable credentials (bestseller, certifications, audience size)
6. signature_programs: Named courses, books, frameworks, certifications
7. booking_link: Calendar booking URL (Calendly, Acuity, etc.)
8. niche: Their primary market niche (1-3 words)
9. phone: Business phone number if publicly displayed
10. current_projects: Active launches or programs being promoted
11. company: Company or business name
12. list_size: Email list or audience size as integer
13. business_size: Scale: "solo", "small_team", "medium", "large"
14. tags: 3-7 keyword tags, lowercase
15. audience_type: Audience category (B2B, B2C, coaches, entrepreneurs, etc.)
16. business_focus: Primary focus in 1 sentence
17. service_provided: Comma-separated list of services

Return as JSON. Use empty string "" for fields without explicit information (use null for list_size if unknown):
{{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "social_proof": "",
    "signature_programs": "",
    "booking_link": "",
    "niche": "",
    "phone": "",
    "current_projects": "",
    "company": "",
    "list_size": null,
    "business_size": "",
    "tags": [],
    "audience_type": "",
    "business_focus": "",
    "service_provided": "",
    "confidence": "high/medium/low",
    "source_quotes": []
}}

IMPORTANT:
- "confidence" should be "high" only if you found clear, explicit statements
- Include "source_quotes" with 1-2 direct quotes from the content
- If you're unsure, set confidence to "low" and leave the field empty
- Business accuracy matters - do NOT fabricate or assume
- list_size MUST be an integer or null"""


# ---------------------------------------------------------------------------
# LLM call (uses OpenRouter directly via openai SDK)
# ---------------------------------------------------------------------------

def call_model(prompt: str, model: str, api_key: str, max_tokens: int = 2048) -> Optional[str]:
    """Call a model via OpenRouter. Returns raw response text."""
    import openai

    client = openai.OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    try:
        response = client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"API call to {model} failed: {e}")
        return None


def parse_json_response(text: str) -> Optional[Dict]:
    """Parse JSON from LLM response, handling markdown fences."""
    if not text:
        return None
    text = text.strip()
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        text = text[start:end].strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------

TEXT_FIELDS = [
    "what_you_do", "who_you_serve", "seeking", "offering", "social_proof",
    "signature_programs", "booking_link", "niche", "phone", "current_projects",
    "company", "business_size", "audience_type", "business_focus", "service_provided",
]

def compare_extractions(baseline: Dict, challenger: Dict) -> Dict:
    """Compare two extraction dicts field-by-field. Returns metrics."""
    metrics = {
        "fields_compared": 0,
        "both_empty": 0,           # Both correctly left empty
        "both_filled": 0,          # Both extracted something
        "baseline_only": 0,        # Baseline filled, challenger empty
        "challenger_only": 0,      # Challenger filled, baseline empty (potential hallucination)
        "confidence_match": baseline.get("confidence") == challenger.get("confidence"),
        "baseline_confidence": baseline.get("confidence", "?"),
        "challenger_confidence": challenger.get("confidence", "?"),
        "field_details": {},
    }

    for field in TEXT_FIELDS:
        b_val = (baseline.get(field) or "").strip()
        c_val = (challenger.get(field) or "").strip()
        b_empty = b_val == ""
        c_empty = c_val == ""

        metrics["fields_compared"] += 1

        if b_empty and c_empty:
            metrics["both_empty"] += 1
            status = "both_empty"
        elif not b_empty and not c_empty:
            metrics["both_filled"] += 1
            status = "both_filled"
        elif not b_empty and c_empty:
            metrics["baseline_only"] += 1
            status = "baseline_only"
        else:
            metrics["challenger_only"] += 1
            status = "challenger_only"

        metrics["field_details"][field] = {
            "status": status,
            "baseline": b_val[:80] if b_val else "",
            "challenger": c_val[:80] if c_val else "",
        }

    return metrics


def print_report(results: List[Dict], baseline_model: str, challenger_model: str):
    """Print a summary comparison report."""
    total = len(results)
    if total == 0:
        print("No results to report.")
        return

    # Count successes
    baseline_json_ok = sum(1 for r in results if r.get("baseline_parsed"))
    challenger_json_ok = sum(1 for r in results if r.get("challenger_parsed"))
    both_ok = sum(1 for r in results if r.get("baseline_parsed") and r.get("challenger_parsed"))

    print("\n" + "=" * 70)
    print(f"  MODEL COMPARISON REPORT")
    print(f"  Baseline:   {baseline_model}")
    print(f"  Challenger: {challenger_model}")
    print(f"  Profiles tested: {total}")
    print("=" * 70)

    print(f"\n--- JSON Parse Success ---")
    print(f"  Baseline:   {baseline_json_ok}/{total} ({100*baseline_json_ok/total:.0f}%)")
    print(f"  Challenger: {challenger_json_ok}/{total} ({100*challenger_json_ok/total:.0f}%)")

    # Aggregate field-level stats from profiles where both parsed
    comparable = [r for r in results if r.get("metrics")]
    if not comparable:
        print("\nNo profiles where both models produced valid JSON.")
        return

    agg = {"both_empty": 0, "both_filled": 0, "baseline_only": 0, "challenger_only": 0, "total_fields": 0}
    confidence_matches = 0
    for r in comparable:
        m = r["metrics"]
        agg["both_empty"] += m["both_empty"]
        agg["both_filled"] += m["both_filled"]
        agg["baseline_only"] += m["baseline_only"]
        agg["challenger_only"] += m["challenger_only"]
        agg["total_fields"] += m["fields_compared"]
        if m["confidence_match"]:
            confidence_matches += 1

    n = agg["total_fields"]
    print(f"\n--- Field-Level Comparison ({len(comparable)} profiles, {n} field comparisons) ---")
    print(f"  Both filled (agreement):      {agg['both_filled']:4d}  ({100*agg['both_filled']/n:.1f}%)")
    print(f"  Both empty (agreement):       {agg['both_empty']:4d}  ({100*agg['both_empty']/n:.1f}%)")
    print(f"  Baseline only (challenger missed): {agg['baseline_only']:4d}  ({100*agg['baseline_only']/n:.1f}%)")
    print(f"  Challenger only (potential halluc): {agg['challenger_only']:4d}  ({100*agg['challenger_only']/n:.1f}%)")

    agreement = agg["both_filled"] + agg["both_empty"]
    print(f"\n  Overall agreement rate: {100*agreement/n:.1f}%")
    print(f"  Confidence score match: {confidence_matches}/{len(comparable)} ({100*confidence_matches/len(comparable):.0f}%)")

    # Anti-hallucination metric
    if agg["both_empty"] + agg["challenger_only"] > 0:
        halluc_rate = agg["challenger_only"] / (agg["both_empty"] + agg["challenger_only"])
        print(f"\n  ** Hallucination risk: {100*halluc_rate:.1f}% **")
        print(f"     (fields the challenger filled when baseline left empty)")
        if halluc_rate > 0.10:
            print(f"     WARNING: >10% hallucination rate — review challenger_only fields below")
    else:
        print(f"\n  ** Hallucination risk: 0% (no fields where challenger filled and baseline didn't)")

    # Show per-field breakdown
    field_stats = {f: {"both_filled": 0, "both_empty": 0, "baseline_only": 0, "challenger_only": 0}
                   for f in TEXT_FIELDS}
    for r in comparable:
        for field, detail in r["metrics"]["field_details"].items():
            field_stats[field][detail["status"]] += 1

    print(f"\n--- Per-Field Breakdown ---")
    print(f"  {'Field':<20s} {'Agree':>6s} {'B-only':>7s} {'C-only':>7s}")
    print(f"  {'-'*20} {'-'*6} {'-'*7} {'-'*7}")
    for field in TEXT_FIELDS:
        s = field_stats[field]
        agree = s["both_filled"] + s["both_empty"]
        print(f"  {field:<20s} {agree:>5d}  {s['baseline_only']:>6d}  {s['challenger_only']:>6d}")

    # Show example disagreements
    disagreements = []
    for r in comparable:
        for field, detail in r["metrics"]["field_details"].items():
            if detail["status"] == "challenger_only":
                disagreements.append({
                    "name": r["name"],
                    "field": field,
                    "challenger_value": detail["challenger"],
                })

    if disagreements:
        print(f"\n--- Challenger-Only Fields (potential hallucinations, up to 10) ---")
        for d in disagreements[:10]:
            print(f"  {d['name']}: {d['field']} = \"{d['challenger_value']}\"")

    print("\n" + "=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compare Claude vs OSS model for profile extraction")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of contacts to test")
    parser.add_argument("--baseline", default="anthropic/claude-sonnet-4", help="Baseline model (default: Claude Sonnet)")
    parser.add_argument("--challenger", default="qwen/qwen3-30b-a3b", help="Challenger model to test")
    parser.add_argument("--csv", default="Filling Database/MASTER_JV_CONTACTS.csv", help="Path to contacts CSV")
    parser.add_argument("--cache-dir", default="scripts/model_comparison_cache", help="Cache directory for crawled content")
    parser.add_argument("--use-cache", action="store_true", help="Skip crawling, use cached content only")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible sampling")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        print("ERROR: OPENROUTER_API_KEY not set in environment")
        sys.exit(1)

    # Load contacts with websites
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        sys.exit(1)

    print(f"Loading contacts from {csv_path}...")
    contacts = []
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            website = (row.get("website") or "").strip()
            name = (row.get("name") or "").strip()
            if website and name and "linkedin.com" not in website.lower():
                contacts.append({"name": name, "website": website})

    print(f"  {len(contacts)} contacts with websites")

    # Sample
    random.seed(args.seed)
    sample = random.sample(contacts, min(args.sample_size, len(contacts)))
    print(f"  Sampled {len(sample)} for testing\n")

    # Cache setup
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for i, contact in enumerate(sample, 1):
        name = contact["name"]
        website = contact["website"]
        print(f"[{i}/{len(sample)}] {name} ({website})")

        # Crawl or use cache
        cache_file = cache_dir / f"{name.replace('/', '_')[:60]}.txt"
        content = None

        if cache_file.exists():
            content = cache_file.read_text(encoding="utf-8")
            print(f"  Using cached content ({len(content)} chars)")
        elif not args.use_cache:
            content = simple_fetch(website)
            if content:
                cache_file.write_text(content, encoding="utf-8")
                print(f"  Crawled ({len(content)} chars)")
            else:
                print(f"  SKIP: fetch failed")
                continue
        else:
            print(f"  SKIP: no cache and --use-cache set")
            continue

        if not content or len(content) < 200:
            print(f"  SKIP: content too thin ({len(content) if content else 0} chars)")
            continue

        prompt = EXTRACTION_PROMPT.format(name=name, website=website, content=content[:15000])

        # Call both models
        print(f"  Calling baseline ({args.baseline})...", end="", flush=True)
        t0 = time.time()
        baseline_raw = call_model(prompt, args.baseline, api_key)
        t_baseline = time.time() - t0
        baseline_data = parse_json_response(baseline_raw)
        print(f" {t_baseline:.1f}s {'OK' if baseline_data else 'PARSE FAIL'}")

        print(f"  Calling challenger ({args.challenger})...", end="", flush=True)
        t0 = time.time()
        challenger_raw = call_model(prompt, args.challenger, api_key)
        t_challenger = time.time() - t0
        challenger_data = parse_json_response(challenger_raw)
        print(f" {t_challenger:.1f}s {'OK' if challenger_data else 'PARSE FAIL'}")

        result = {
            "name": name,
            "website": website,
            "baseline_parsed": baseline_data is not None,
            "challenger_parsed": challenger_data is not None,
            "baseline_time": t_baseline,
            "challenger_time": t_challenger,
        }

        if baseline_data and challenger_data:
            result["metrics"] = compare_extractions(baseline_data, challenger_data)

        results.append(result)

        # Brief pause to respect rate limits
        time.sleep(0.5)

    # Save raw results
    results_file = cache_dir / "comparison_results.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nRaw results saved to {results_file}")

    # Print report
    print_report(results, args.baseline, args.challenger)


if __name__ == "__main__":
    main()
