#!/bin/bash
# Run locally to set up a Vast.ai instance for Tier B remaining enrichment.
# Usage:
#   bash scripts/setup_vast_tier_b_remaining.sh A   # Instance A (batches 0-3561)
#   bash scripts/setup_vast_tier_b_remaining.sh B   # Instance B (batches 3562-7123)

INSTANCE=${1:-A}

if [ "$INSTANCE" = "A" ]; then
    SSH_HOST="ssh7.vast.ai"
    SSH_PORT=21722
    START=0
    END=3561
elif [ "$INSTANCE" = "B" ]; then
    SSH_HOST="ssh9.vast.ai"
    SSH_PORT=21724
    START=3562
    END=7123
else
    echo "Usage: $0 A|B"
    exit 1
fi

SSH="ssh -o StrictHostKeyChecking=no -p $SSH_PORT root@$SSH_HOST"
SCP_OPTS="-o StrictHostKeyChecking=no -P $SSH_PORT"
LOCAL_DIR="/Users/josephtepe/Projects/jv-matchmaker-platform"

echo "=== Setting up Instance $INSTANCE ($SSH_HOST:$SSH_PORT) batches $START-$END ==="

# 1. Copy enrich script
echo "Copying enrich script..."
scp $SCP_OPTS "$LOCAL_DIR/scripts/enrich_tier_b.py" root@$SSH_HOST:/root/enrich_tier_b.py

# 2. Install deps
echo "Installing deps..."
$SSH "pip install psycopg2-binary python-dotenv beautifulsoup4 aiohttp -q"

# 3. Rsync batch files for this range
echo "Rsyncing batch files $START-$END..."
# Generate list of files to sync
FILES=()
for i in $(seq $START $END); do
    f=$(printf "tmp/tier_b_remaining_batches/batch_%04d.json" $i)
    [ -f "$LOCAL_DIR/$f" ] && FILES+=("$f")
done

echo "Syncing ${#FILES[@]} batch files..."
rsync -az --progress -e "ssh $SCP_OPTS" \
    --files-from=<(printf '%s\n' "${FILES[@]#*/}") \
    "$LOCAL_DIR/tmp/tier_b_remaining_batches/" \
    root@$SSH_HOST:/root/tier_b_remaining_batches/ 2>/dev/null || \
rsync -az -e "ssh -o StrictHostKeyChecking=no -p $SSH_PORT" \
    "$LOCAL_DIR/tmp/tier_b_remaining_batches/" \
    root@$SSH_HOST:/root/tier_b_remaining_batches/

# 4. Copy the enrichment runner script
scp $SCP_OPTS "$LOCAL_DIR/scripts/vast_enrich_tier_b_remaining.sh" \
    root@$SSH_HOST:/root/run_enrichment.sh

# 5. Start enrichment in background
echo "Starting enrichment (batches $START-$END)..."
$SSH "mkdir -p /root/tier_b_remaining_results && \
    nohup bash /root/run_enrichment.sh $START $END \
    > /root/enrichment_${INSTANCE}.log 2>&1 &
    echo 'PID:' \$!"

echo "=== Instance $INSTANCE running. Monitor with:"
echo "    ssh $SCP_OPTS root@$SSH_HOST 'tail -f /root/enrichment_${INSTANCE}.log'"
