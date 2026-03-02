#!/usr/bin/env python3
"""
run_all.py - ISMC Validation Suite Orchestrator
=================================================

Runs all (or a subset of) the ISMC matching algorithm validation scripts
and produces a consolidated summary report.

Scripts:
    01  Score Distribution Analysis
    02  Predictive Validity            (deferred by default)
    03  Bidirectional Symmetry Analysis
    04  Aggregation Ablation (Geometric vs Arithmetic)
    05  Embedding Quality Deep Dive
    06  Role Compatibility Matrix Validation
    07  Network Centrality Analysis
    08  Expert Blind Review Sample Generator
    09  Literature Comparison

Usage:
    python scripts/validation/run_all.py --test
    python scripts/validation/run_all.py --test --scripts 1,3,5
    python scripts/validation/run_all.py --test --parallel
    python scripts/validation/run_all.py --test --include-deferred
    python scripts/validation/run_all.py --test --stop-on-error
"""

import os
import sys
import argparse
import random
import subprocess
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
random.seed(42)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR / 'validation_results'

SCRIPTS = {
    1: {'file': '01_score_distribution.py',    'name': 'Score Distribution Analysis'},
    2: {'file': '02_predictive_validity.py',   'name': 'Predictive Validity'},
    3: {'file': '03_bidirectional_analysis.py', 'name': 'Bidirectional Symmetry Analysis'},
    4: {'file': '04_aggregation_ablation.py',  'name': 'Aggregation Ablation (Geometric vs Arithmetic)'},
    5: {'file': '05_embedding_validation.py',  'name': 'Embedding Quality Deep Dive'},
    6: {'file': '06_role_matrix_validation.py', 'name': 'Role Compatibility Matrix Validation'},
    7: {'file': '07_network_analysis.py',      'name': 'Network Centrality Analysis'},
    8: {'file': '08_expert_review_sample.py',  'name': 'Expert Blind Review Sample Generator'},
    9: {'file': '09_literature_comparison.py', 'name': 'Literature Comparison'},
}

DEFERRED_SCRIPTS = {2}

DEFERRED_REASON = (
    "Deferred -- requires engagement data (EngagementSummary table). "
    "Use --include-deferred to run."
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.1f}s"


def run_script(script_num: int, test_mode: bool) -> dict:
    """
    Run a single validation script as a subprocess.

    Returns a dict with keys: num, name, status, elapsed, stdout, stderr, returncode
    """
    info = SCRIPTS[script_num]
    script_path = SCRIPT_DIR / info['file']

    cmd = [sys.executable, str(script_path)]
    if test_mode:
        cmd.append('--test')

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout per script
        )
        elapsed = time.time() - start
        status = 'PASS' if result.returncode == 0 else 'FAIL'
        return {
            'num': script_num,
            'name': info['name'],
            'status': status,
            'elapsed': elapsed,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
        }
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        return {
            'num': script_num,
            'name': info['name'],
            'status': 'FAIL',
            'elapsed': elapsed,
            'stdout': '',
            'stderr': 'ERROR: Script timed out after 600 seconds.',
            'returncode': -1,
        }
    except Exception as exc:
        elapsed = time.time() - start
        return {
            'num': script_num,
            'name': info['name'],
            'status': 'FAIL',
            'elapsed': elapsed,
            'stdout': '',
            'stderr': f'ERROR: {exc}',
            'returncode': -1,
        }


def print_header(script_nums: list, test_mode: bool, parallel: bool) -> str:
    """Print and return the run header."""
    now = datetime.now()
    mode_str = "TEST (synthetic data)" if test_mode else "LIVE (production database)"
    exec_str = "parallel" if parallel else "sequential"
    script_list = ', '.join(f"{n:02d}" for n in sorted(script_nums))

    lines = [
        "=" * 72,
        "ISMC Validation Suite -- Orchestrator",
        "=" * 72,
        f"  Timestamp : {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Mode      : {mode_str}",
        f"  Execution : {exec_str}",
        f"  Scripts   : [{script_list}]",
        f"  Python    : {sys.executable}",
        "=" * 72,
        "",
    ]
    header = '\n'.join(lines)
    print(header)
    return header


def print_script_status(num: int, name: str, status: str, elapsed: float = 0.0) -> str:
    """Print and return a single-line status update."""
    tag = f"[{num:02d}]"
    if status == 'RUNNING':
        line = f"  {tag} {name:.<52s} RUNNING"
    elif status == 'SKIP':
        line = f"  {tag} {name:.<52s} SKIP"
    else:
        line = f"  {tag} {name:.<52s} {status:<4s}  ({format_elapsed(elapsed)})"
    print(line)
    return line


def print_summary(results: list, deferred_nums: list, total_elapsed: float) -> str:
    """Print and return the summary table."""
    lines = [
        "",
        "=" * 72,
        "SUMMARY",
        "=" * 72,
        f"  {'#':<4s} {'Script':<46s} {'Status':<6s} {'Time':>10s}",
        "  " + "-" * 68,
    ]

    pass_count = 0
    fail_count = 0
    skip_count = 0

    # Build a lookup from results
    result_map = {r['num']: r for r in results}

    for num in sorted(SCRIPTS.keys()):
        name = SCRIPTS[num]['name']
        if num in deferred_nums and num not in result_map:
            lines.append(f"  {num:02d}   {name:<46s} {'SKIP':<6s} {'--':>10s}")
            skip_count += 1
        elif num in result_map:
            r = result_map[num]
            elapsed_str = format_elapsed(r['elapsed'])
            lines.append(f"  {num:02d}   {r['name']:<46s} {r['status']:<6s} {elapsed_str:>10s}")
            if r['status'] == 'PASS':
                pass_count += 1
            else:
                fail_count += 1
        else:
            # Not selected to run
            lines.append(f"  {num:02d}   {name:<46s} {'--':<6s} {'--':>10s}")

    lines.append("  " + "-" * 68)
    lines.append(f"  Total elapsed: {format_elapsed(total_elapsed)}")
    lines.append(f"  Passed: {pass_count}   Failed: {fail_count}   Skipped: {skip_count}")
    lines.append("")

    if fail_count > 0:
        lines.append("  RESULT: SOME SCRIPTS FAILED")
    else:
        lines.append("  RESULT: ALL SCRIPTS PASSED")
    lines.append("=" * 72)

    summary = '\n'.join(lines)
    print(summary)
    return summary


def save_report(header: str, log_lines: list, summary: str, results: list) -> Path:
    """Save a consolidated report to validation_results/run_all_summary.txt."""
    report_path = RESULTS_DIR / 'run_all_summary.txt'
    with open(report_path, 'w') as f:
        f.write(header)
        f.write('\n'.join(log_lines))
        f.write('\n')
        f.write(summary)
        f.write('\n\n')

        # Append per-script output details
        for r in sorted(results, key=lambda x: x['num']):
            f.write(f"\n{'=' * 72}\n")
            f.write(f"Script {r['num']:02d}: {r['name']} -- {r['status']}\n")
            f.write(f"{'=' * 72}\n")
            if r['stdout'].strip():
                f.write("--- stdout ---\n")
                f.write(r['stdout'])
                f.write('\n')
            if r['stderr'].strip():
                f.write("--- stderr ---\n")
                f.write(r['stderr'])
                f.write('\n')

    return report_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description='ISMC Validation Suite -- run all validation scripts.',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Pass --test to all scripts (synthetic data mode).',
    )
    parser.add_argument(
        '--scripts', type=str, default=None,
        help='Comma-separated list of script numbers to run (e.g. 1,3,5). Default: all except 02.',
    )
    parser.add_argument(
        '--include-deferred', action='store_true',
        help='Also run deferred scripts (e.g. 02 predictive validity).',
    )
    parser.add_argument(
        '--parallel', action='store_true',
        help='Run scripts in parallel using subprocesses (default: sequential).',
    )
    parser.add_argument(
        '--stop-on-error', action='store_true',
        help='Stop execution if any script fails (default: continue).',
    )
    args = parser.parse_args()

    # Determine which scripts to run
    if args.scripts:
        try:
            selected = [int(x.strip()) for x in args.scripts.split(',')]
        except ValueError:
            print("ERROR: --scripts must be a comma-separated list of integers (e.g. 1,3,5)")
            return 1
        invalid = [s for s in selected if s not in SCRIPTS]
        if invalid:
            print(f"ERROR: Unknown script number(s): {invalid}. Valid: 1-9")
            return 1
    else:
        selected = sorted(SCRIPTS.keys())

    # Filter out deferred unless explicitly included
    deferred_nums = []
    run_nums = []
    for num in selected:
        if num in DEFERRED_SCRIPTS and not args.include_deferred:
            deferred_nums.append(num)
        else:
            run_nums.append(num)

    # Create output directory
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Print header
    header = print_header(run_nums, args.test, args.parallel)

    # Note deferred scripts
    log_lines = []
    for num in deferred_nums:
        info = SCRIPTS[num]
        line = f"  [{num:02d}] {info['name']:.<52s} SKIP"
        print(line)
        log_lines.append(line)
        print(f"        {DEFERRED_REASON}")
        log_lines.append(f"        {DEFERRED_REASON}")

    if deferred_nums and run_nums:
        print()
        log_lines.append("")

    # Run scripts
    results = []
    any_failed = False

    if args.parallel and len(run_nums) > 1:
        # Parallel execution
        print("Starting parallel execution...\n")
        log_lines.append("Starting parallel execution...\n")

        futures = {}
        with ProcessPoolExecutor(max_workers=min(len(run_nums), os.cpu_count() or 4)) as executor:
            total_start = time.time()
            for num in run_nums:
                future = executor.submit(run_script, num, args.test)
                futures[future] = num

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                line = print_script_status(result['num'], result['name'], result['status'], result['elapsed'])
                log_lines.append(line)

                if result['status'] == 'FAIL':
                    any_failed = True
                    if result['stderr'].strip():
                        err_line = f"        Error: {result['stderr'].strip()[:200]}"
                        print(err_line)
                        log_lines.append(err_line)

        total_elapsed = time.time() - total_start

    else:
        # Sequential execution
        total_start = time.time()
        for num in run_nums:
            line = print_script_status(num, SCRIPTS[num]['name'], 'RUNNING')
            log_lines.append(line)

            result = run_script(num, args.test)
            results.append(result)

            # Overwrite RUNNING line with final status
            status_line = print_script_status(result['num'], result['name'], result['status'], result['elapsed'])
            log_lines.append(status_line)

            if result['status'] == 'FAIL':
                any_failed = True
                if result['stderr'].strip():
                    err_line = f"        Error: {result['stderr'].strip()[:200]}"
                    print(err_line)
                    log_lines.append(err_line)
                if args.stop_on_error:
                    print("\n  --stop-on-error set. Halting execution.")
                    log_lines.append("\n  --stop-on-error set. Halting execution.")
                    break

        total_elapsed = time.time() - total_start

    # Summary
    summary = print_summary(results, deferred_nums, total_elapsed)

    # Save report
    report_path = save_report(header, log_lines, summary, results)
    print(f"\n  Report saved to: {report_path}")

    return 1 if any_failed else 0


if __name__ == '__main__':
    sys.exit(main())
