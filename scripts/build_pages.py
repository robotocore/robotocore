#!/usr/bin/env python3
"""Build the GitHub Pages site by injecting parity data into docs/index.html.

Usage:
    uv run python scripts/build_pages.py [--output-dir OUTPUT]

Reads: scripts/generate_parity_report.py output (runs it internally)
Writes: OUTPUT/index.html with embedded parity JSON, OUTPUT/coverage.svg

The gh-pages deployment workflow runs this script, then pushes the
OUTPUT directory to GitHub Pages.
"""

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
DOCS = ROOT / "docs"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build GitHub Pages site")
    parser.add_argument("--output-dir", default=str(ROOT / "site"), help="Output directory")
    parser.add_argument("--commit", default="", help="Git commit SHA to embed in page")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Generate parity report
    print("Generating parity report…", flush=True)
    parity_path = out / "parity-report.json"
    report_script = str(ROOT / "scripts" / "generate_parity_report.py")
    result = subprocess.run(
        [sys.executable, report_script, "--output", str(parity_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    print(f"  → {parity_path}", flush=True)

    # 2. Load and annotate with metadata
    data = json.loads(parity_path.read_text())
    data["generated_at"] = datetime.now(UTC).isoformat()
    data["commit"] = args.commit or _git_sha()
    parity_path.write_text(json.dumps(data, indent=2))

    # 3. Inject into HTML template
    template = (DOCS / "index.html").read_text()
    injected = template.replace("DATA_PLACEHOLDER", json.dumps(data), 1)
    (out / "index.html").write_text(injected)
    print(f"  → {out / 'index.html'}", flush=True)

    # 4. Copy static assets
    for asset in ["banner.svg", "coverage.svg", "logo.png"]:
        src = DOCS / asset
        if src.exists():
            shutil.copy2(src, out / asset)
            print(f"  → {out / asset}", flush=True)

    print(f"\nSite built in {out}/  ({len(list(out.iterdir()))} files)")


def _git_sha() -> str:
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
        return r.stdout.strip()
    except Exception:
        return ""


if __name__ == "__main__":
    main()
