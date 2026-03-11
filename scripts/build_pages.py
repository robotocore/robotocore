#!/usr/bin/env python3
"""Build the GitHub Pages site.

Usage:
    uv run python scripts/build_pages.py [--output-dir OUTPUT]

Builds two pages:
  - index.html       — project story page (static, copied as-is)
  - coverage.html    — interactive coverage dashboard (parity data injected)

Plus static assets: banner.svg, coverage.svg, logo.png, parity-report.json

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

    # 3. Compute fresh stats from parity data
    summary = data.get("summary", {})
    total_tests = _count_tests(ROOT)
    native_count = summary.get("native_services", 0)
    impl_count = summary.get("total_implemented", 0)
    total_ops = summary.get("total_aws_operations", 0)
    impl_pct = round(summary.get("impl_pct", 0))
    prompt_count = _count_prompts(ROOT)

    # 4. Copy story page with fresh stats injected
    story = (DOCS / "index.html").read_text()
    story = _inject_stats(story, total_tests, native_count, impl_pct, prompt_count)
    (out / "index.html").write_text(story)
    print(f"  → {out / 'index.html'} (story, stats injected)", flush=True)

    # 5. Inject parity data into coverage dashboard
    template = (DOCS / "coverage.html").read_text()
    injected = template.replace("DATA_PLACEHOLDER", json.dumps(data), 1)
    (out / "coverage.html").write_text(injected)
    print(f"  → {out / 'coverage.html'} (coverage dashboard)", flush=True)

    # 6. Copy static assets, injecting stats into SVGs
    for asset in ["banner.svg", "coverage.svg", "logo.png"]:
        src = DOCS / asset
        if src.exists():
            if asset.endswith(".svg"):
                svg = src.read_text()
                svg = _inject_svg_stats(svg, total_tests, impl_count, total_ops, impl_pct)
                (out / asset).write_text(svg)
            else:
                shutil.copy2(src, out / asset)
            print(f"  → {out / asset}", flush=True)

    print(f"\nSite built in {out}/  ({len(list(out.iterdir()))} files)")


def _count_tests(root: Path) -> int:
    """Count test functions across all test files."""
    import re

    count = 0
    for test_dir in ["tests/unit", "tests/compatibility", "tests/integration"]:
        for f in (root / test_dir).rglob("test_*.py"):
            count += len(re.findall(r"^\s+def test_", f.read_text(), re.MULTILINE))
    return count


def _count_prompts(root: Path) -> int:
    """Count prompt log files."""
    prompts_dir = root / "prompts"
    if prompts_dir.exists():
        return len([f for f in prompts_dir.glob("*.md") if f.name != "PROMPTLOG.md"])
    return 0


def _inject_stats(
    html: str, total_tests: int, native_count: int, impl_pct: int, prompt_count: int
) -> str:
    """Replace hardcoded stats in story page HTML with fresh values."""
    import re

    # Match stat-card patterns: <div class="stat-card-num">NUMBER</div>
    # Update specific labels
    def _replace_stat(label: str, value: str) -> str:
        pattern = (
            r'(<div class="stat-card-num">)[^<]*(</div>'
            r'<div class="stat-card-label">' + re.escape(label) + r")"
        )
        return re.sub(pattern, rf"\g<1>{value}\2", html)

    html = _replace_stat("Tests", f"{total_tests:,}")
    html = _replace_stat("Native providers", str(native_count))
    html = _replace_stat("Operations implemented", f"{impl_pct}%")
    html = _replace_stat("Prompt sessions logged", str(prompt_count))
    return html


def _inject_svg_stats(
    svg: str, total_tests: int, impl_count: int, total_ops: int, impl_pct: int
) -> str:
    """Replace hardcoded stats in SVG badges with fresh values."""
    import re

    # banner.svg: "NN,NNN+ tests"
    svg = re.sub(r"[\d,]+\+ tests", f"{total_tests:,}+ tests", svg)
    # coverage.svg header: "NN% of N,NNN operations"
    svg = re.sub(r"\d+% of [\d,]+ operations", f"{impl_pct}% of {total_ops:,} operations", svg)
    # coverage.svg subheader: "147 services  ·  N,NNN implemented  ·  N,NNN tests"
    svg = re.sub(
        r"(147 services\s+·\s+)[\d,]+( implemented\s+·\s+)[\d,]+( tests)",
        rf"\g<1>{impl_count:,}\g<2>{total_tests:,}\3",
        svg,
    )
    return svg


def _git_sha() -> str:
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
        return r.stdout.strip()
    except Exception:
        return ""


if __name__ == "__main__":
    main()
