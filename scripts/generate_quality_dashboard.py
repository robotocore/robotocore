#!/usr/bin/env python3
"""Generate an HTML quality dashboard from test quality data.

Creates an interactive HTML report with:
- Summary cards (total services, avg quality, test count)
- Service heatmap (color grid by quality percentage)
- Quality trend chart (over recent commits)
- Behavioral coverage radar chart
- Top issues list (lowest-quality services)

Usage:
    uv run python scripts/generate_quality_dashboard.py
    uv run python scripts/generate_quality_dashboard.py --output docs/quality/index.html
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Try to import validate_test_quality functions
sys.path.insert(0, str(Path(__file__).parent))

from validate_test_quality import TESTS_DIR, _get_behavioral_report, analyze_file


def get_service_quality_data() -> list[dict]:
    """Collect quality data for all services."""
    services = []

    for test_file in sorted(TESTS_DIR.glob("test_*_compat.py")):
        # Extract service name from filename
        service_name = test_file.stem.replace("test_", "").replace("_compat", "")

        tests = analyze_file(test_file)
        if not tests:
            continue

        total = len(tests)
        ok_count = sum(1 for t in tests if t["quality"] == "ok")
        no_contact = sum(1 for t in tests if t["quality"] == "no_server_contact")
        no_assert = sum(1 for t in tests if t["quality"] == "no_assertion")
        weak_assert = sum(1 for t in tests if t["quality"] == "weak_assertion")

        effective_pct = round(ok_count / total * 100, 1) if total else 0

        # Get behavioral data
        behavioral = _get_behavioral_report(tests, str(test_file))

        services.append(
            {
                "name": service_name,
                "total_tests": total,
                "effective_tests": ok_count,
                "effective_pct": effective_pct,
                "no_contact": no_contact,
                "no_assertion": no_assert,
                "weak_assertion": weak_assert,
                "behavioral_coverage_pct": behavioral["overall_behavioral_coverage_pct"],
                "pattern_coverage": behavioral["pattern_coverage"],
            }
        )

    return sorted(services, key=lambda s: s["effective_pct"])


def get_trend_data() -> list[dict]:
    """Get quality trend from recent git commits (if available)."""
    # This is a placeholder - in a real implementation, we would:
    # 1. Check out each recent commit
    # 2. Run quality analysis
    # 3. Store the results
    # For now, return empty list (dashboard will hide trend if no data)
    return []


def generate_html(services: list[dict], trends: list[dict], output_path: Path) -> None:
    """Generate the HTML dashboard."""

    # Calculate summary stats
    total_tests = sum(s["total_tests"] for s in services)
    total_effective = sum(s["effective_tests"] for s in services)
    avg_quality = (
        round(sum(s["effective_pct"] for s in services) / len(services), 1) if services else 0
    )
    avg_behavioral = (
        round(sum(s["behavioral_coverage_pct"] for s in services) / len(services), 1)
        if services
        else 0
    )

    # Calculate overall pattern coverage
    overall_patterns = {"CREATE": 0, "RETRIEVE": 0, "LIST": 0, "UPDATE": 0, "DELETE": 0, "ERROR": 0}
    for service in services:
        for pattern, data in service["pattern_coverage"].items():
            overall_patterns[pattern] += data["count"]

    total_pattern_tests = sum(s["total_tests"] for s in services)
    pattern_pcts = {
        p: round(c / total_pattern_tests * 100, 1) if total_pattern_tests else 0
        for p, c in overall_patterns.items()
    }

    # Generate service grid HTML
    service_grid_html = ""
    for service in reversed(services):  # Reversed so best are at top
        pct = service["effective_pct"]
        # Color gradient: red (0%) -> yellow (50%) -> green (100%)
        if pct < 50:
            r = 255
            g = int(pct * 5.1)  # 0-255 as pct goes 0-50
        else:
            r = int((100 - pct) * 5.1)  # 255-0 as pct goes 50-100
            g = 255
        color = f"rgb({r}, {g}, 50)"

        service_grid_html += f"""
        <div class="service-card" style="background-color: {color}">
            <div class="service-name">{service["name"]}</div>
            <div class="service-score">{pct}%</div>
            <div class="service-tests">{service["total_tests"]} tests</div>
        </div>"""

    # Generate issues list HTML
    issues_html = ""
    for service in services[:10]:  # Bottom 10
        issues_html += f"""
        <tr>
            <td>{service["name"]}</td>
            <td>{service["effective_pct"]}%</td>
            <td>{service["total_tests"]}</td>
            <td>{service["no_contact"]}</td>
            <td>{service["weak_assertion"]}</td>
            <td>{service["no_assertion"]}</td>
        </tr>"""

    # Generate the full HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Test Quality Dashboard</title>
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            padding: 20px;
            min-height: 100vh;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .header h1 {{
            font-size: 2rem;
            margin-bottom: 10px;
        }}
        .header .timestamp {{
            color: #888;
            font-size: 0.9rem;
        }}
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: #16213e;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .card-value {{
            font-size: 2.5rem;
            font-weight: bold;
            color: #00d4ff;
        }}
        .card-label {{
            font-size: 0.9rem;
            color: #888;
            margin-top: 5px;
        }}
        .section {{
            background: #16213e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .section h2 {{
            font-size: 1.2rem;
            margin-bottom: 15px;
            color: #00d4ff;
        }}
        .service-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
            gap: 10px;
        }}
        .service-card {{
            border-radius: 8px;
            padding: 10px;
            text-align: center;
            color: #000;
        }}
        .service-name {{
            font-weight: bold;
            font-size: 0.85rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        .service-score {{
            font-size: 1.5rem;
            font-weight: bold;
        }}
        .service-tests {{
            font-size: 0.75rem;
            opacity: 0.8;
        }}
        .radar-container {{
            display: flex;
            justify-content: center;
            align-items: center;
            height: 300px;
        }}
        .radar-chart {{
            position: relative;
            width: 280px;
            height: 280px;
        }}
        .radar-axis {{
            position: absolute;
            width: 2px;
            background: #333;
            transform-origin: bottom center;
        }}
        .radar-label {{
            position: absolute;
            font-size: 0.8rem;
            color: #888;
        }}
        .radar-value {{
            position: absolute;
            background: rgba(0, 212, 255, 0.3);
            border: 2px solid #00d4ff;
        }}
        .pattern-bars {{
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}
        .pattern-row {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .pattern-label {{
            width: 80px;
            font-size: 0.9rem;
        }}
        .pattern-bar-bg {{
            flex: 1;
            height: 24px;
            background: #0f3460;
            border-radius: 4px;
            overflow: hidden;
        }}
        .pattern-bar {{
            height: 100%;
            background: linear-gradient(90deg, #00d4ff, #0099cc);
            transition: width 0.5s ease;
        }}
        .pattern-value {{
            width: 50px;
            text-align: right;
            font-size: 0.9rem;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #333;
        }}
        th {{
            color: #888;
            font-weight: normal;
            font-size: 0.85rem;
        }}
        td {{
            font-size: 0.9rem;
        }}
        tr:hover {{
            background: #1f4068;
        }}
        .legend {{
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-top: 15px;
            font-size: 0.85rem;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .legend-color {{
            width: 16px;
            height: 16px;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Test Quality Dashboard</h1>
        <div class="timestamp">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
    </div>

    <div class="summary-cards">
        <div class="card">
            <div class="card-value">{len(services)}</div>
            <div class="card-label">Services</div>
        </div>
        <div class="card">
            <div class="card-value">{total_tests:,}</div>
            <div class="card-label">Total Tests</div>
        </div>
        <div class="card">
            <div class="card-value">{total_effective:,}</div>
            <div class="card-label">Effective Tests</div>
        </div>
        <div class="card">
            <div class="card-value">{avg_quality}%</div>
            <div class="card-label">Avg Quality</div>
        </div>
        <div class="card">
            <div class="card-value">{avg_behavioral}%</div>
            <div class="card-label">Avg Behavioral</div>
        </div>
    </div>

    <div class="section">
        <h2>Service Quality Heatmap</h2>
        <div class="service-grid">
            {service_grid_html}
        </div>
        <div class="legend">
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(255, 0, 50)"></div>
                <span>0-25%</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(255, 127, 50)"></div>
                <span>25-50%</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(255, 255, 50)"></div>
                <span>50-75%</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(127, 255, 50)"></div>
                <span>75-90%</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: rgb(0, 255, 50)"></div>
                <span>90-100%</span>
            </div>
        </div>
    </div>

    <div class="section">
        <h2>Behavioral Coverage Patterns</h2>
        <div class="pattern-bars">
            <div class="pattern-row">
                <span class="pattern-label">CREATE</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["CREATE"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["CREATE"]}%</span>
            </div>
            <div class="pattern-row">
                <span class="pattern-label">RETRIEVE</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["RETRIEVE"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["RETRIEVE"]}%</span>
            </div>
            <div class="pattern-row">
                <span class="pattern-label">LIST</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["LIST"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["LIST"]}%</span>
            </div>
            <div class="pattern-row">
                <span class="pattern-label">UPDATE</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["UPDATE"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["UPDATE"]}%</span>
            </div>
            <div class="pattern-row">
                <span class="pattern-label">DELETE</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["DELETE"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["DELETE"]}%</span>
            </div>
            <div class="pattern-row">
                <span class="pattern-label">ERROR</span>
                <div class="pattern-bar-bg">
                    <div class="pattern-bar" style="width: {pattern_pcts["ERROR"]}%"></div>
                </div>
                <span class="pattern-value">{pattern_pcts["ERROR"]}%</span>
            </div>
        </div>
    </div>

    <div class="section">
        <h2>Services Needing Improvement</h2>
        <table>
            <thead>
                <tr>
                    <th>Service</th>
                    <th>Quality</th>
                    <th>Tests</th>
                    <th>No Contact</th>
                    <th>Weak Assert</th>
                    <th>No Assert</th>
                </tr>
            </thead>
            <tbody>
                {issues_html}
            </tbody>
        </table>
    </div>

    <script>
        // Add interactivity if needed
        document.querySelectorAll('.service-card').forEach(card => {{
            card.style.cursor = 'pointer';
            card.addEventListener('click', () => {{
                const name = card.querySelector('.service-name').textContent;
                alert('Service: ' + name + '\\nClick behavior can be customized.');
            }});
        }});
    </script>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    print(f"Dashboard generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate test quality dashboard")
    parser.add_argument(
        "--output",
        default="docs/quality/index.html",
        help="Output path for HTML dashboard",
    )
    args = parser.parse_args()

    print("Collecting service quality data...")
    services = get_service_quality_data()
    print(f"Found {len(services)} services")

    print("Collecting trend data...")
    trends = get_trend_data()

    print("Generating dashboard...")
    generate_html(services, trends, Path(args.output))


if __name__ == "__main__":
    main()
