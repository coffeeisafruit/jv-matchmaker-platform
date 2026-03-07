"""
One-time script to clean up zombie Prefect runs stuck in RUNNING state.

These are orphaned runs from CLI invocations where the Python process
exited but the local SQLite state was never updated.

Usage:
    python3 scripts/cleanup_zombie_runs.py
    python3 scripts/cleanup_zombie_runs.py --dry-run
"""

import argparse
import os
import sqlite3
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Clean up zombie Prefect runs")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be cleaned without modifying")
    args = parser.parse_args()

    db_path = os.path.expanduser("~/.prefect/prefect.db")
    if not os.path.exists(db_path):
        print(f"Prefect database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)

    # Find zombie flow runs
    flow_zombies = conn.execute(
        "SELECT fr.name, f.name, fr.start_time "
        "FROM flow_run fr LEFT JOIN flow f ON fr.flow_id = f.id "
        "WHERE fr.state_type = 'RUNNING'"
    ).fetchall()

    # Find zombie task runs
    task_zombies = conn.execute(
        "SELECT name, state_type, start_time "
        "FROM task_run "
        "WHERE state_type IN ('RUNNING', 'SCHEDULED')"
    ).fetchall()

    print(f"Found {len(flow_zombies)} zombie flow runs:")
    for name, flow_name, start in flow_zombies:
        print(f"  {name} ({flow_name}) started {start}")

    print(f"\nFound {len(task_zombies)} zombie task runs:")
    for name, state, start in task_zombies:
        print(f"  {name} [{state}] started {start}")

    if args.dry_run:
        print("\n[DRY RUN] No changes made.")
        conn.close()
        return

    if not flow_zombies and not task_zombies:
        print("\nNo zombies to clean up.")
        conn.close()
        return

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        "UPDATE flow_run SET state_type='CANCELLED', state_name='Cancelled', "
        f"end_time='{now}' WHERE state_type='RUNNING'"
    )
    conn.execute(
        "UPDATE task_run SET state_type='CANCELLED', state_name='Cancelled', "
        f"end_time='{now}' WHERE state_type IN ('RUNNING', 'SCHEDULED')"
    )

    # Also update corresponding state tables
    conn.execute(
        "UPDATE flow_run_state SET type='CANCELLED', name='Cancelled' "
        "WHERE id IN (SELECT state_id FROM flow_run WHERE state_name='Cancelled' AND end_time=?)",
        (now,),
    )
    conn.execute(
        "UPDATE task_run_state SET type='CANCELLED', name='Cancelled' "
        "WHERE id IN (SELECT state_id FROM task_run WHERE state_name='Cancelled' AND end_time=?)",
        (now,),
    )

    conn.commit()
    print(f"\nCleaned up {len(flow_zombies)} flow runs and {len(task_zombies)} task runs.")
    conn.close()


if __name__ == "__main__":
    main()
