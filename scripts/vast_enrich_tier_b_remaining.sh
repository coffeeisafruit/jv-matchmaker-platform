#!/bin/bash
# Run on Vast.ai instance to enrich a range of Tier B remaining batches.
# Usage: bash vast_enrich_tier_b_remaining.sh START END
# Example: bash vast_enrich_tier_b_remaining.sh 0 3561

START=${1:-0}
END=${2:-3561}
BATCH_DIR="/root/tier_b_remaining_batches"
RESULT_DIR="/root/tier_b_remaining_results"
ENRICH_SCRIPT="/root/enrich_tier_b.py"

mkdir -p "$RESULT_DIR"

echo "Processing batches $START to $END"
total=$((END - START + 1))
done=0
failed=0

for i in $(seq $START $END); do
    batch_file=$(printf "%s/batch_%04d.json" "$BATCH_DIR" $i)
    result_file=$(printf "%s/batch_%04d.json" "$RESULT_DIR" $i)

    [ ! -f "$batch_file" ] && continue
    [ -f "$result_file" ] && { done=$((done+1)); continue; }

    python3 "$ENRICH_SCRIPT" extract "$batch_file" "$result_file" 2>/dev/null
    if [ $? -eq 0 ] && [ -f "$result_file" ]; then
        done=$((done+1))
    else
        failed=$((failed+1))
        echo "FAILED: batch_$(printf '%04d' $i)"
    fi

    if [ $((done % 100)) -eq 0 ]; then
        echo "Progress: $done/$total done, $failed failed"
    fi
done

echo "Done: $done/$total, failed: $failed"
