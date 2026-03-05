#!/usr/bin/env python3
"""Track enrichment progress and estimate completion time."""

import re
from pathlib import Path
from datetime import datetime, timedelta

LOG_FILE = Path("/private/tmp/claude-501/-Users-josephtepe-Projects-jv-matchmaker-platform/tasks/b09a6fc.output")

def parse_log():
    """Parse log file to extract progress metrics."""
    if not LOG_FILE.exists():
        return None

    with open(LOG_FILE) as f:
        content = f.read()

    # Extract batch completions
    batch_pattern = r"Extracted (\d+)/(\d+) profiles"
    batches = []
    for match in re.finditer(batch_pattern, content):
        extracted = int(match.group(1))
        total = int(match.group(2))
        batches.append((extracted, total))

    # Current batch (get the last occurrence)
    current_batch_matches = list(re.finditer(r"Processing batch (\d+)", content))
    current_batch = int(current_batch_matches[-1].group(1)) if current_batch_matches else None

    # Profile count
    profile_count = content.count("Profile")

    return {
        "completed_batches": len(batches),
        "current_batch": current_batch,
        "profile_count": profile_count,
        "batches": batches,
    }

def main():
    data = parse_log()
    if not data:
        print("No log file found")
        return

    total_batches = 15  # 420-434
    start_batch = 420
    end_batch = 434

    completed = data["completed_batches"]
    current = data["current_batch"]

    print("=" * 60)
    print("TIER B ENRICHMENT PROGRESS - Batches 420-434")
    print("=" * 60)
    print(f"\nCompleted batches: {completed}/{total_batches}")
    print(f"Current batch: {current}")
    print(f"Total API calls made: {data['profile_count']}")

    if data["batches"]:
        print(f"\nCompleted batch details:")
        for i, (extracted, total) in enumerate(data["batches"]):
            batch_num = start_batch + i
            pct = (extracted / total * 100) if total else 0
            print(f"  Batch {batch_num}: {extracted}/{total} profiles ({pct:.1f}%)")

        # Calculate totals
        total_extracted = sum(b[0] for b in data["batches"])
        total_possible = sum(b[1] for b in data["batches"])
        print(f"\n  TOTAL: {total_extracted}/{total_possible} profiles enriched")

    # Estimate completion time
    if completed > 0:
        # Assume log file creation time is start time
        start_time = datetime.fromtimestamp(LOG_FILE.stat().st_ctime)
        elapsed = datetime.now() - start_time
        avg_time_per_batch = elapsed / completed
        remaining_batches = total_batches - completed
        estimated_remaining = avg_time_per_batch * remaining_batches
        estimated_completion = datetime.now() + estimated_remaining

        print(f"\nTiming:")
        print(f"  Elapsed time: {elapsed}")
        print(f"  Average time per batch: {avg_time_per_batch}")
        print(f"  Estimated remaining: {estimated_remaining}")
        print(f"  Estimated completion: {estimated_completion.strftime('%H:%M:%S')}")

    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
