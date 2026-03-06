#!/bin/bash
# Master job script for Vast.ai RTX 4090 instance
# Runs: seeking inference + Tier C/D enrichment, then signals auto-shutdown
#
# Prerequisites:
#   - vLLM server running on localhost:8000 with Qwen3-30B-A3B
#   - Job files uploaded to /root/jobs/
#
# This script runs ON the Vast.ai instance.

set -euo pipefail

VLLM_URL="http://localhost:8000"
JOBS_DIR="/root/jobs"
RESULTS_DIR="/root/results"

mkdir -p "$RESULTS_DIR"

echo "============================================"
echo "Vast.ai Job Runner — $(date)"
echo "============================================"

# Wait for vLLM to be ready
echo "[1/5] Waiting for vLLM server..."
MAX_WAIT=300
WAITED=0
while ! curl -s "$VLLM_URL/v1/models" >/dev/null 2>&1; do
    sleep 5
    WAITED=$((WAITED + 5))
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        echo "ERROR: vLLM not ready after ${MAX_WAIT}s"
        exit 1
    fi
    echo "  Waiting... (${WAITED}s)"
done
echo "  vLLM ready! (${WAITED}s)"

# Install dependencies
echo "[2/5] Installing dependencies..."
pip install psycopg2-binary python-dotenv 2>/dev/null || true

# Job 1: Seeking inference
if [ -d "$JOBS_DIR/seeking_batches" ]; then
    echo "[3/5] Running seeking inference..."
    BATCH_COUNT=$(ls "$JOBS_DIR/seeking_batches/"*.json 2>/dev/null | wc -l)
    echo "  $BATCH_COUNT batch files found"

    VLLM_URL="$VLLM_URL" python3 "$JOBS_DIR/vllm_seeking_inferrer.py" \
        --batches-dir "$JOBS_DIR/seeking_batches" \
        --results-dir "$RESULTS_DIR/seeking_results" \
        --workers 15 \
        2>&1 | tee "$RESULTS_DIR/seeking_log.txt"

    echo "  Seeking inference complete!"
else
    echo "[3/5] No seeking batches found, skipping"
fi

# Job 2: Tier C enrichment
if [ -d "$JOBS_DIR/tier_c_batches" ]; then
    echo "[4a/5] Running Tier C enrichment..."
    python3 "$JOBS_DIR/vllm_batch_enricher.py" \
        --batches-dir "$JOBS_DIR/tier_c_batches" \
        --results-dir "$RESULTS_DIR/tier_c_results" \
        --vllm-url "$VLLM_URL" \
        --workers 15 \
        2>&1 | tee "$RESULTS_DIR/tier_c_log.txt"
else
    echo "[4a/5] No Tier C batches found, skipping"
fi

# Job 3: Tier D enrichment
if [ -d "$JOBS_DIR/tier_d_batches" ]; then
    echo "[4b/5] Running Tier D enrichment..."
    python3 "$JOBS_DIR/vllm_batch_enricher.py" \
        --batches-dir "$JOBS_DIR/tier_d_batches" \
        --results-dir "$RESULTS_DIR/tier_d_results" \
        --vllm-url "$VLLM_URL" \
        --workers 15 \
        2>&1 | tee "$RESULTS_DIR/tier_d_log.txt"
else
    echo "[4b/5] No Tier D batches found, skipping"
fi

# Job 4: Remaining JSONL batches
if [ -d "$JOBS_DIR/jsonl_batches" ]; then
    echo "[4c/5] Running remaining JSONL enrichment..."
    python3 "$JOBS_DIR/vllm_batch_enricher.py" \
        --batches-dir "$JOBS_DIR/jsonl_batches" \
        --results-dir "$RESULTS_DIR/jsonl_results" \
        --vllm-url "$VLLM_URL" \
        --workers 15 \
        2>&1 | tee "$RESULTS_DIR/jsonl_log.txt"
else
    echo "[4c/5] No JSONL batches found, skipping"
fi

# Signal completion
echo "[5/5] All jobs complete!"
echo "$(date)" > /tmp/ALL_JOBS_DONE
echo ""
echo "============================================"
echo "RESULTS SUMMARY"
echo "============================================"
echo "Seeking results: $(ls "$RESULTS_DIR/seeking_results/"*.json 2>/dev/null | wc -l) files"
echo "Tier C results:  $(ls "$RESULTS_DIR/tier_c_results/"*.json 2>/dev/null | wc -l) files"
echo "Tier D results:  $(ls "$RESULTS_DIR/tier_d_results/"*.json 2>/dev/null | wc -l) files"
echo "JSONL results:   $(ls "$RESULTS_DIR/jsonl_results/"*.json 2>/dev/null | wc -l) files"
echo ""
echo "Sentinel written. Instance will auto-destroy."
echo "Download results with: scp -P <port> -r root@<host>:/root/results/ ./vast_results/"
