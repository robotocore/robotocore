#!/usr/bin/env python3
"""Generate the S3 launch-to-semantics audit report and connectivity matrix."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from robotocore.semantic.s3_audit import (
    DEFAULT_CATALOG_PATH,
    DEFAULT_MATRIX_PATH,
    DEFAULT_REPORT_PATH,
    audit_catalog,
    load_catalog,
    write_report_files,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the S3 semantic audit report")
    parser.add_argument(
        "--catalog", type=Path, default=DEFAULT_CATALOG_PATH, help="Catalog YAML path"
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_REPORT_PATH, help="JSON report output path"
    )
    parser.add_argument(
        "--matrix-output",
        type=Path,
        default=DEFAULT_MATRIX_PATH,
        help="Markdown connectivity matrix output path",
    )
    parser.add_argument("--json", action="store_true", help="Print report JSON to stdout")
    args = parser.parse_args()

    catalog = load_catalog(args.catalog)
    report = audit_catalog(catalog)
    write_report_files(report, report_path=args.output, matrix_path=args.matrix_output)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        counts = report["summary"]["status_counts"]
        print(
            "S3 semantic audit: "
            f"{counts['pass']} pass, "
            f"{counts['needs_manual_review']} needs_manual_review, "
            f"{counts['fail']} fail, "
            f"{counts['retired']} retired, "
            f"{counts['out_of_scope']} out_of_scope"
        )
        print(f"Report: {args.output}")
        print(f"Connectivity matrix: {args.matrix_output}")


if __name__ == "__main__":
    main()
