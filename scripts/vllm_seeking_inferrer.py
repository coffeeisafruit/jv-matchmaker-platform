#!/usr/bin/env python3
"""
Standalone seeking inference for vLLM on Vast.ai.
Reads batch files from --batches-dir, calls vLLM, writes results to --results-dir.

Usage:
    python3 vllm_seeking_inferrer.py --batches-dir /root/jobs/seeking_batches --results-dir /root/results/seeking_results
"""

import argparse
import json
import os
import threading
import time
import concurrent.futures
from pathlib import Path
from typing import Optional

import requests

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8000")
MODEL = None
_model_lock = threading.Lock()
WORKERS = 15

PROMPT_TEMPLATE = """Given this professional's profile, what types of JV/collaboration partners would they most likely be seeking? Be specific about the partner type, their audience, and the collaboration format.

Profile:
- What they do: {what_you_do}
- Who they serve: {who_you_serve}
- What they offer: {offering}
- Their niche: {niche}

Respond with ONLY the seeking text (1-2 sentences, no quotes). Example: "Podcast hosts serving entrepreneurs who could feature me as a guest to promote my leadership coaching programs"
"""

SYSTEM_MSG = "You are a JV partnership expert. Infer what collaboration partners this professional would seek. Be specific and concise. /no_think"


def get_model():
    """Auto-detect model from vLLM (thread-safe)."""
    if MODEL:
        return MODEL
    with _model_lock:
        global MODEL
        if MODEL:
            return MODEL
        resp = requests.get(f"{VLLM_URL}/v1/models", timeout=10)
        resp.raise_for_status()
        MODEL = resp.json()["data"][0]["id"]
        print(f"Using model: {MODEL}")
        return MODEL


def infer_seeking(profile: dict) -> Optional[str]:
    """Call vLLM to infer seeking for a single profile."""
    prompt = PROMPT_TEMPLATE.format(
        what_you_do=profile.get("what_you_do", "") or "",
        who_you_serve=profile.get("who_you_serve", "") or "",
        offering=profile.get("offering", "") or "",
        niche=profile.get("niche", "") or "",
    )

    try:
        resp = requests.post(
            f"{VLLM_URL}/v1/chat/completions",
            json={
                "model": get_model(),
                "messages": [
                    {"role": "system", "content": SYSTEM_MSG},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 150,
                "temperature": 0.3,
            },
            timeout=30,
        )
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"].strip()
        # Clean up common artifacts
        text = text.strip('"').strip("'")
        if text.startswith("Seeking: "):
            text = text[9:]
        return text
    except Exception as e:
        print(f"  Error inferring for {profile.get('id', '?')}: {e}")
        return None


def process_batch_file(batch_file: Path, results_dir: Path) -> int:
    """Process a single batch file, write results, return count."""
    result_file = results_dir / batch_file.name
    if result_file.exists():
        return 0  # Already processed (resume support)

    profiles = json.loads(batch_file.read_text())
    results = []

    for profile in profiles:
        seeking = infer_seeking(profile)
        if seeking:
            results.append({
                "id": profile["id"],
                "seeking": seeking,
            })

    if results:
        result_file.write_text(json.dumps(results))

    return len(results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches-dir", required=True, help="Directory with batch JSON files")
    parser.add_argument("--results-dir", required=True, help="Directory for result JSON files")
    parser.add_argument("--workers", type=int, default=WORKERS, help="Concurrent workers")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of batch files (0=all)")
    args = parser.parse_args()

    batches_dir = Path(args.batches_dir)
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Find batch files
    batch_files = sorted(batches_dir.glob("*.json"))
    if args.limit:
        batch_files = batch_files[:args.limit]

    # Skip already processed
    existing = set(f.name for f in results_dir.glob("*.json"))
    todo = [f for f in batch_files if f.name not in existing]

    print(f"Total batches: {len(batch_files)}")
    print(f"Already done: {len(existing)}")
    print(f"To process: {len(todo)}")
    print(f"Workers: {args.workers}")
    print()

    if not todo:
        print("Nothing to do!")
        return

    # Wait for vLLM
    print("Checking vLLM...")
    get_model()
    print()

    # Process with thread pool
    total_inferred = 0
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(process_batch_file, bf, results_dir) for bf in todo]

        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            done_count += 1
            count = future.result()
            total_inferred += count

            if done_count % 100 == 0 or done_count == len(todo):
                elapsed = time.time() - start_time
                rate = done_count / elapsed if elapsed > 0 else 0
                eta = (len(todo) - done_count) / rate if rate > 0 else 0
                print(f"  [{done_count}/{len(todo)}] {total_inferred} inferred, "
                      f"{rate:.1f} batches/s, ETA {eta/60:.1f}min")

    elapsed = time.time() - start_time
    print(f"\nDone! {total_inferred} seeking values inferred in {elapsed/60:.1f}min")
    print(f"Results in: {results_dir}")


if __name__ == "__main__":
    main()
