#!/usr/bin/env python3
"""
Lightweight CSV validation script (no Django required).

Quickly validate CSV files without importing to database.
Checks headers, counts rows, and identifies potential issues.

Usage:
    python3 "Filling Database/supabase/validate_csvs.py"
    python3 "Filling Database/supabase/validate_csvs.py" --file ../partners/sam_gov.csv
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CSV_DIRS = [
    Path(__file__).resolve().parent.parent / "partners",
    Path(__file__).resolve().parent.parent / "chambers",
]

EXPECTED_HEADERS = {"name", "email", "company", "website", "linkedin", "phone", "bio"}
REQUIRED_HEADERS = {"name"}  # Only name is mandatory


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Result of validating a single CSV file."""
    filename: str
    exists: bool = True
    readable: bool = True
    total_rows: int = 0
    empty_rows: int = 0
    valid_rows: int = 0
    headers: list[str] = None
    missing_headers: set[str] = None
    extra_headers: set[str] = None
    errors: list[str] = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = []
        if self.missing_headers is None:
            self.missing_headers = set()
        if self.extra_headers is None:
            self.extra_headers = set()
        if self.errors is None:
            self.errors = []

    @property
    def is_valid(self) -> bool:
        """Check if CSV is valid and importable."""
        return (
            self.exists
            and self.readable
            and "name" in self.headers
            and self.valid_rows > 0
            and len(self.errors) == 0
        )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_csv(csv_path: Path) -> ValidationResult:
    """Validate a single CSV file."""
    result = ValidationResult(filename=csv_path.name)

    # Check file exists
    if not csv_path.exists():
        result.exists = False
        result.errors.append(f"File not found: {csv_path}")
        return result

    # Try to read CSV
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate headers
            result.headers = list(reader.fieldnames or [])
            headers_set = set(result.headers)

            # Check for required headers
            if not REQUIRED_HEADERS.issubset(headers_set):
                missing = REQUIRED_HEADERS - headers_set
                result.errors.append(f"Missing required headers: {missing}")
                result.readable = False
                return result

            # Identify missing optional headers
            result.missing_headers = EXPECTED_HEADERS - headers_set

            # Identify extra headers not in expected set
            result.extra_headers = headers_set - EXPECTED_HEADERS

            # Count rows
            for row in reader:
                result.total_rows += 1

                # Check if row is completely empty
                if not any(row.values()):
                    result.empty_rows += 1
                    continue

                # Check if row has at least a name
                if not row.get("name", "").strip():
                    result.errors.append(f"Row {result.total_rows} missing name")
                    continue

                result.valid_rows += 1

        result.readable = True

    except Exception as e:
        result.readable = False
        result.errors.append(f"Failed to read CSV: {e}")

    return result


def print_result(result: ValidationResult, verbose: bool = False) -> None:
    """Print validation result for a single file."""
    status = "✓" if result.is_valid else "✗"
    print(f"\n{status} {result.filename}")

    if not result.exists:
        print(f"  ERROR: File not found")
        return

    if not result.readable:
        print(f"  ERROR: Cannot read file")
        for error in result.errors:
            print(f"    - {error}")
        return

    # Print stats
    print(f"  Total rows:    {result.total_rows}")
    print(f"  Valid rows:    {result.valid_rows}")
    if result.empty_rows > 0:
        print(f"  Empty rows:    {result.empty_rows}")

    # Print headers
    if verbose:
        print(f"  Headers:       {', '.join(result.headers)}")

    if result.missing_headers:
        print(f"  Missing cols:  {', '.join(sorted(result.missing_headers))}")

    if result.extra_headers and verbose:
        print(f"  Extra cols:    {', '.join(sorted(result.extra_headers))}")

    # Print errors
    if result.errors:
        print(f"  Errors:")
        for error in result.errors[:5]:  # Limit to first 5 errors
            print(f"    - {error}")
        if len(result.errors) > 5:
            print(f"    ... and {len(result.errors) - 5} more errors")


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def find_csv_files(single_file: str | None = None) -> list[Path]:
    """Find all CSV files to validate."""
    if single_file:
        file_path = Path(single_file)
        if not file_path.is_absolute():
            file_path = (Path(__file__).resolve().parent / single_file).resolve()
        return [file_path]

    # Scan all CSV directories
    csv_files = []
    for csv_dir in CSV_DIRS:
        if not csv_dir.exists():
            continue
        csv_files.extend(csv_dir.rglob("*.csv"))

    return sorted(csv_files)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate CSV files for import",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--file",
        type=str,
        help="Validate a single specific CSV file",
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed information",
    )

    args = parser.parse_args()

    # Find CSV files
    csv_files = find_csv_files(args.file)

    if not csv_files:
        print("No CSV files found")
        return 1

    print(f"Validating {len(csv_files)} CSV file(s)...")

    # Validate all files
    results = []
    for csv_path in csv_files:
        result = validate_csv(csv_path)
        results.append(result)
        print_result(result, verbose=args.verbose)

    # Print summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    total_files = len(results)
    valid_files = sum(1 for r in results if r.is_valid)
    invalid_files = total_files - valid_files

    total_rows = sum(r.total_rows for r in results)
    valid_rows = sum(r.valid_rows for r in results)
    empty_rows = sum(r.empty_rows for r in results)

    print(f"Total files:     {total_files}")
    print(f"Valid files:     {valid_files}")
    if invalid_files > 0:
        print(f"Invalid files:   {invalid_files}")

    print(f"\nTotal rows:      {total_rows:,}")
    print(f"Valid rows:      {valid_rows:,}")
    if empty_rows > 0:
        print(f"Empty rows:      {empty_rows:,}")

    print("=" * 70)

    if invalid_files > 0:
        print("\n⚠ Some files have validation errors. See details above.")
        return 1

    print("\n✓ All files are valid and ready for import")
    return 0


if __name__ == "__main__":
    exit(main())
