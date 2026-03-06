#!/bin/bash
# Auto-shutdown script for Vast.ai instance
# Monitors for a "DONE" sentinel file and destroys the instance when found
#
# Usage (on local machine):
#   bash scripts/vast_auto_shutdown.sh <instance_id> <ssh_host> <ssh_port>
#
# The remote job should create /tmp/ALL_JOBS_DONE when finished.

INSTANCE_ID="${1:?Usage: $0 <instance_id> <ssh_host> <ssh_port>}"
SSH_HOST="${2:?Usage: $0 <instance_id> <ssh_host> <ssh_port>}"
SSH_PORT="${3:?Usage: $0 <instance_id> <ssh_host> <ssh_port>}"
POLL_INTERVAL=60  # Check every 60 seconds

echo "[auto-shutdown] Monitoring instance $INSTANCE_ID ($SSH_HOST:$SSH_PORT)"
echo "[auto-shutdown] Will destroy when /tmp/ALL_JOBS_DONE appears on remote"

while true; do
    # Check if sentinel file exists
    if ssh -i ~/.ssh/vastai_key -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p "$SSH_PORT" root@"$SSH_HOST" \
        'test -f /tmp/ALL_JOBS_DONE' 2>/dev/null; then
        echo "[auto-shutdown] $(date): Jobs complete! Destroying instance $INSTANCE_ID..."
        vastai destroy instance "$INSTANCE_ID"
        echo "[auto-shutdown] Instance destroyed. Total cost saved by auto-shutdown."
        exit 0
    fi

    # Check if instance is still running
    STATUS=$(vastai show instance "$INSTANCE_ID" 2>/dev/null | tail -1 | awk '{print $3}')
    if [ "$STATUS" = "exited" ] || [ "$STATUS" = "destroyed" ]; then
        echo "[auto-shutdown] Instance already $STATUS. Exiting."
        exit 0
    fi

    sleep "$POLL_INTERVAL"
done
