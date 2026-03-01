#!/bin/bash
#
# Autonomous JV Candidate Sourcing — runs all scrapers in parallel
#
# Usage:
#   ./scripts/sourcing/run_all.sh              # Run now
#   ./scripts/sourcing/run_all.sh --delayed 90 # Run after 90 minutes
#   ./scripts/sourcing/run_all.sh --status     # Check progress
#
set -euo pipefail
cd "$(dirname "$0")/../.."

# Load environment
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export DJANGO_SETTINGS_MODULE=config.settings

LOGDIR="scripts/sourcing/logs"
OUTDIR="scripts/sourcing/output"
PYTHON="python3"

mkdir -p "$LOGDIR" "$OUTDIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ─────────────────────────────────────────────
# Status check
# ─────────────────────────────────────────────
if [ "${1:-}" = "--status" ]; then
    echo ""
    echo "=== Running Scrapers ==="
    ps aux | grep "scripts.sourcing.runner" | grep -v grep || echo "  None running"
    echo ""
    echo "=== CSV Contacts ==="
    total=0
    for f in "$OUTDIR"/*.csv; do
        [ -f "$f" ] || continue
        lines=$(($(wc -l < "$f") - 1))
        [ "$lines" -lt 0 ] && lines=0
        total=$((total + lines))
        printf "  %-25s %6d contacts\n" "$(basename "$f")" "$lines"
    done
    echo "  ─────────────────────────────────────"
    printf "  %-25s %6d contacts\n" "TOTAL" "$total"
    echo ""
    $PYTHON -m scripts.sourcing.runner --status 2>/dev/null || true
    exit 0
fi

# ─────────────────────────────────────────────
# Delayed start
# ─────────────────────────────────────────────
if [ "${1:-}" = "--delayed" ]; then
    DELAY_MINS="${2:-90}"
    echo "Scheduling scraping run in $DELAY_MINS minutes..."
    echo "Will start at: $(date -v+${DELAY_MINS}M '+%Y-%m-%d %H:%M:%S')"
    nohup bash -c "sleep $((DELAY_MINS * 60)) && $0" >> "$LOGDIR/delayed_${TIMESTAMP}.log" 2>&1 &
    echo "Background PID: $!"
    echo "Log: $LOGDIR/delayed_${TIMESTAMP}.log"
    exit 0
fi

# ─────────────────────────────────────────────
# Main: Launch all scrapers in parallel
# ─────────────────────────────────────────────
echo "============================================================"
echo "  JV CANDIDATE SOURCING — AUTONOMOUS RUN"
echo "  Started: $(date)"
echo "============================================================"
echo ""

# API-based scrapers (reliable, high volume)
SCRAPERS_API=(
    "apple_podcasts"
    "youtube_api"
)

# HTML scrapers (working with requests+bs4)
SCRAPERS_HTML=(
    "noomii"
    "muncheye"
    "summit_speakers"
)

# JS-rendered sites (need crawl4ai if available, skip otherwise)
SCRAPERS_JS=(
    "substack"
    "podchaser"
    "gumroad"
    "eventbrite"
    "medium"
    "warriorplus"
    "speakerhub"
    "udemy"
    "clickbank"
    "jvzoo"
    "icf_coaching"
)

PIDS=()

launch_scraper() {
    local source=$1
    local log="$LOGDIR/${source}_${TIMESTAMP}.log"
    echo "  Launching: $source (log: $log)"
    nohup $PYTHON -m scripts.sourcing.runner \
        --source "$source" \
        --batch-size 100 \
        --export-csv "$OUTDIR/${source}.csv" \
        >> "$log" 2>&1 &
    PIDS+=($!)
}

echo "── API Scrapers (highest yield) ──"
for s in "${SCRAPERS_API[@]}"; do
    launch_scraper "$s"
done

echo ""
echo "── HTML Scrapers ──"
for s in "${SCRAPERS_HTML[@]}"; do
    launch_scraper "$s"
done

echo ""
echo "── JS-Rendered Scrapers (may produce fewer results) ──"
for s in "${SCRAPERS_JS[@]}"; do
    launch_scraper "$s"
done

echo ""
echo "Launched ${#PIDS[@]} scrapers in parallel"
echo "PIDs: ${PIDS[*]}"
echo ""
echo "Monitor progress:"
echo "  $0 --status"
echo "  tail -f $LOGDIR/*_${TIMESTAMP}.log"
echo ""

# Wait for all to finish
echo "Waiting for all scrapers to complete..."
for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

# Final summary
echo ""
echo "============================================================"
echo "  ALL SCRAPERS COMPLETE — $(date)"
echo "============================================================"
$0 --status
