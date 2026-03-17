#!/usr/bin/env python3
"""Verify all compat test files are covered by exactly one CI shard.

Catches files that would fall through the cracks if new test files are added
with names outside the expected [a-z] range, or if a file should be in a
special job (chaos, cross-service) but isn't explicitly listed.

Usage:
    uv run python scripts/check_compat_sharding.py
"""

import fnmatch
import sys
from pathlib import Path

COMPAT_DIR = Path("tests/compatibility")

# CI shard globs (from .github/workflows/ci.yml)
SHARD_PATTERNS = [
    "test_[a-g]*.py",
    "test_[h-r]*.py",
    "test_[s-z]*.py",
]

# Files explicitly handled outside the shards
SPECIAL_FILES = {
    "test_cross_service_compat.py",  # --ignore'd from shards, runs in cross-service-tests job
}

# Subdirectories with their own CI routing (not matched by root-level globs)
SPECIAL_DIRS = {
    "chaos",  # tests/compatibility/chaos/ — runs in cross-service-tests job
}


def main() -> int:
    errors = []

    # Collect all test files in the root of tests/compatibility/
    root_test_files = sorted(
        f.name
        for f in COMPAT_DIR.iterdir()
        if f.is_file() and f.name.startswith("test_") and f.name.endswith(".py")
    )

    # Check each file is covered by exactly one shard or is special
    for filename in root_test_files:
        if filename in SPECIAL_FILES:
            continue

        matching_shards = [p for p in SHARD_PATTERNS if fnmatch.fnmatch(filename, p)]
        if len(matching_shards) == 0:
            errors.append(
                f"  {filename}: NOT covered by any shard pattern — add to a shard or SPECIAL_FILES"
            )
        elif len(matching_shards) > 1:
            errors.append(
                f"  {filename}: covered by MULTIPLE shards: {matching_shards} — "
                f"shards should be disjoint"
            )

    # Check subdirectories are accounted for
    subdirs = sorted(
        d.name for d in COMPAT_DIR.iterdir() if d.is_dir() and not d.name.startswith("__")
    )
    for subdir in subdirs:
        if subdir not in SPECIAL_DIRS:
            subdir_tests = list((COMPAT_DIR / subdir).glob("test_*.py"))
            if subdir_tests:
                errors.append(
                    f"  {subdir}/: subdirectory with {len(subdir_tests)} test file(s) "
                    f"not in SPECIAL_DIRS — these won't run in CI"
                )

    # Check special files actually exist
    for sf in SPECIAL_FILES:
        if not (COMPAT_DIR / sf).exists():
            errors.append(f"  {sf}: listed in SPECIAL_FILES but doesn't exist")

    for sd in SPECIAL_DIRS:
        if not (COMPAT_DIR / sd).is_dir():
            errors.append(f"  {sd}/: listed in SPECIAL_DIRS but doesn't exist")

    if errors:
        print("FAIL: compat test sharding issues found:")
        for e in errors:
            print(e)
        return 1

    print(
        f"OK: {len(root_test_files)} root files "
        f"({len(root_test_files) - len(SPECIAL_FILES)} sharded, "
        f"{len(SPECIAL_FILES)} special), "
        f"{len(subdirs)} subdirectories ({len(SPECIAL_DIRS)} special)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
