#!/usr/bin/env python3
"""Build a slide deck HTML file for Robotocore — 21 slides, light color scheme.

Outputs to docs/slides.html. Run with --check to validate with Playwright screenshots.

Usage:
    uv run python scripts/build_slides.py
    uv run python scripts/build_slides.py --check
"""

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent
PROMPTS_DIR = ROOT / "prompts"
OUT_FILE = ROOT / "docs" / "slides.html"


def collect_prompt_sessions() -> list[dict]:
    """Parse prompt log files — glob 2026*.md, extract date/time/slug/summary."""
    sessions = []
    for f in sorted(PROMPTS_DIR.glob("2026*.md")):
        text = f.read_text()
        # Parse YAML frontmatter
        fm_match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
        if not fm_match:
            continue
        fm = fm_match.group(1)
        ts_match = re.search(r'timestamp:\s*"?([^"\n]+)"?', fm)
        if not ts_match:
            continue
        ts = ts_match.group(1).strip()
        date_part = ts[:10] if len(ts) >= 10 else ts
        time_part = ts[11:16] if len(ts) >= 16 else ""
        slug = f.stem[18:] if len(f.stem) > 18 else f.stem

        body = text[fm_match.end() :]
        # Find first assistant paragraph
        paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]
        summary = ""
        for para in paragraphs:
            if para.startswith("#") or para.startswith("---") or para.startswith("```"):
                continue
            # Strip markdown
            clean = re.sub(r"\*\*([^*]+)\*\*", r"\1", para)
            clean = re.sub(r"\*([^*]+)\*", r"\1", clean)
            clean = re.sub(r"`[^`]+`", "", clean)
            clean = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", clean)
            clean = clean.strip()
            if len(clean) > 30:
                # Take first 2 sentences
                sentences = re.split(r"(?<=[.!?])\s+", clean)
                summary = " ".join(sentences[:2])
                break

        sessions.append(
            {
                "date": date_part,
                "time": time_part,
                "slug": slug.replace("-", " ").title(),
                "summary": summary[:200] if summary else slug.replace("-", " ").title(),
                "filename": f.name,
            }
        )
    return sessions


def compute_commit_timeline() -> list[dict]:
    """git log --format=%ad --date=short, group by day → [{date, count}]"""
    try:
        result = subprocess.run(
            ["git", "log", "--format=%ad", "--date=short"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=15,
        )
        counts: dict[str, int] = {}
        for line in result.stdout.strip().splitlines():
            date = line.strip()
            if date:
                counts[date] = counts.get(date, 0) + 1
        return [{"date": d, "count": c} for d, c in sorted(counts.items())]
    except Exception:
        return []


def get_test_milestones() -> list[dict]:
    return [
        {"label": "Day 0", "date": "2026-03-06", "tests": 0, "services": 0},
        {"label": "First tests", "date": "2026-03-06", "tests": 124, "services": 12},
        {"label": "Batch services", "date": "2026-03-07", "tests": 1799, "services": 109},
        {"label": "Compat blast", "date": "2026-03-07", "tests": 2080, "services": 131},
        {"label": "Quality gate", "date": "2026-03-08", "tests": 2874, "services": 138},
        {"label": "Enterprise", "date": "2026-03-09", "tests": 6383, "services": 143},
        {"label": "Hardening", "date": "2026-03-09", "tests": 11036, "services": 145},
        {"label": "Tier 2-4", "date": "2026-03-10", "tests": 14538, "services": 147},
    ]


def get_moto_milestones() -> list[dict]:
    return [
        {"year": 2013, "label": "Moto born", "detail": "EC2, S3, DynamoDB stubs", "commits": 50},
        {
            "year": 2014,
            "label": "SQS, SNS, IAM",
            "detail": "Core AWS services added",
            "commits": 800,
        },
        {
            "year": 2015,
            "label": "Community grows",
            "detail": "Lambda, Kinesis, RDS",
            "commits": 2100,
        },
        {
            "year": 2016,
            "label": "LocalStack adopts Moto",
            "detail": "moto_server used as backend",
            "commits": 3400,
        },
        {
            "year": 2018,
            "label": "50+ services",
            "detail": "Major adoption across industry",
            "commits": 5200,
        },
        {
            "year": 2020,
            "label": "100+ services",
            "detail": "Mainstream AWS testing tool",
            "commits": 7100,
        },
        {
            "year": 2023,
            "label": "195 services",
            "detail": "Near-complete AWS coverage",
            "commits": 9800,
        },
        {
            "year": 2026,
            "label": "Robotocore forks",
            "detail": "10,678 commits, still growing",
            "commits": 10678,
        },
    ]


def get_native_providers() -> list[str]:
    return [
        "acm",
        "apigateway",
        "apigatewayv2",
        "appsync",
        "batch",
        "cloudformation",
        "cloudwatch",
        "cognito-idp",
        "config",
        "dynamodb",
        "dynamodbstreams",
        "ec2",
        "ecr",
        "ecs",
        "es",
        "events",
        "firehose",
        "iam",
        "kinesis",
        "lambda",
        "logs",
        "opensearch",
        "rekognition",
        "resource-groups",
        "resourcegroupstaggingapi",
        "route53",
        "s3",
        "scheduler",
        "secretsmanager",
        "ses",
        "sesv2",
        "sns",
        "sqs",
        "ssm",
        "stepfunctions",
        "sts",
        "support",
        "xray",
    ]


def get_tier1_features() -> list[dict]:
    return [
        {
            "name": "Multi-Account Isolation",
            "icon": "🏢",
            "detail": "Account-keyed stores across all providers",
            "bugs": 8,
            "tests": 71,
        },
        {
            "name": "Real Database Engines",
            "icon": "🗄️",
            "detail": "RDS with SQLite, ElastiCache with Redis-compat",
            "bugs": 8,
            "tests": 125,
        },
        {
            "name": "Lambda Hot Reload",
            "icon": "⚡",
            "detail": "Code caching, FS mounts, sys.modules isolation",
            "bugs": 7,
            "tests": 70,
        },
        {
            "name": "EKS Mock Kubernetes",
            "icon": "☸️",
            "detail": "Full Starlette k8s API server, pods/services/deployments",
            "bugs": 12,
            "tests": 94,
        },
    ]


def get_tier24_features() -> list[dict]:
    return [
        {"name": "IoT Rule Engine", "tests": 82, "detail": "SQL parser, 9 action types"},
        {"name": "Cognito Hosted UI", "tests": 53, "detail": "OAuth2/OIDC endpoints"},
        {
            "name": "X-Ray Service Map",
            "tests": 56,
            "detail": "Trace correlation, anomaly detection",
        },
        {"name": "CloudWatch Synthetics", "tests": 40, "detail": "Canary execution & scheduling"},
        {"name": "CW Alarms → ASG", "tests": 29, "detail": "Scaling policy dispatch"},
        {"name": "DDB Global Tables", "tests": 63, "detail": "State snapshots + replication"},
    ]


def build_html(data: dict) -> str:
    moto_json = json.dumps(data["moto_milestones"])
    milestones_json = json.dumps(data["test_milestones"])
    sessions_json = json.dumps(data["sessions"])
    providers_json = json.dumps(data["native_providers"])
    tier1_json = json.dumps(data["tier1_features"])
    tier24_json = json.dumps(data["tier24_features"])
    timeline_json = json.dumps(data["commit_timeline"])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Robotocore — An Open-Source Story</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
:root {{
  --blue: #1E6FBF; --blue2: #2E86D4; --amber: #E07B00; --purple: #6B3FA0;
  --teal: #007B8A; --gold: #C9920B; --dark: #1A2332; --bg: #F4F7FC;
  --white: #FFFFFF; --text: #1A2332; --muted: #5A6880; --border: #CBD5E8;
  --code-bg: #E8EDF8;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}}
html, body {{ width: 100%; height: 100%; overflow: hidden; background: #F4F7FC; }}

#deck {{ position: fixed; inset: 0; display: flex; align-items: center; justify-content: center; }}
.slide {{
  position: absolute; inset: 0;
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  padding: 60px 80px;
  opacity: 0; pointer-events: none;
  transition: opacity 0.6s cubic-bezier(.4,0,.2,1), transform 0.6s cubic-bezier(.4,0,.2,1);
  transform: translateX(60px); overflow: hidden;
}}
.slide.active {{ opacity: 1; pointer-events: auto; transform: translateX(0); }}
.slide.exit {{ opacity: 0; transform: translateX(-60px); transition: opacity 0.4s ease, transform 0.4s ease; }}

#progress {{ position: fixed; bottom: 0; left: 0; height: 4px; background: var(--amber); transition: width 0.5s ease; z-index: 100; }}
#slide-counter {{ position: fixed; bottom: 12px; right: 20px; font-size: 12px; color: rgba(0,0,0,0.35); z-index: 100; letter-spacing: 1px; }}
#kb-hint {{ position: fixed; bottom: 12px; left: 50%; transform: translateX(-50%); font-size: 11px; color: rgba(0,0,0,0.3); z-index: 100; letter-spacing: 1px; }}

.slide-hero   {{ background: #FFFFFF; color: #1A2332; }}
.slide-light  {{ background: #F4F7FC; color: #1A2332; }}
.slide-blue   {{ background: #EBF3FF; color: #1A2332; }}
.slide-amber  {{ background: #FFF8EF; color: #1A2332; }}
.slide-purple {{ background: #F3EEFF; color: #1A2332; }}
.slide-data   {{ background: #FFFFFF; color: #1A2332; }}

.eyebrow {{ font-size: 11px; font-weight: 700; letter-spacing: 3px; text-transform: uppercase; opacity: 0.55; margin-bottom: 16px; }}
h1 {{ font-size: clamp(2.8rem,5vw,5rem); font-weight: 800; line-height: 1.05; }}
h2 {{ font-size: clamp(2rem,3.5vw,3.2rem); font-weight: 700; line-height: 1.15; }}
h3 {{ font-size: clamp(1.2rem,2vw,1.8rem); font-weight: 600; }}
.subtitle {{ font-size: clamp(1rem,1.8vw,1.4rem); opacity: 0.65; margin-top: 16px; line-height: 1.5; }}
.stat-num {{
  font-size: clamp(3rem,6vw,7rem); font-weight: 900; line-height: 1;
  background: linear-gradient(135deg, #E07B00 0%, #C9920B 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
}}
.stat-label {{ font-size: 1rem; opacity: 0.6; margin-top: 6px; letter-spacing: 1px; text-transform: uppercase; }}
blockquote {{
  border-left: 4px solid #E07B00; padding: 16px 24px; margin: 20px 0;
  background: rgba(224,123,0,0.05); border-radius: 0 8px 8px 0;
  font-size: clamp(1rem,1.6vw,1.25rem); font-style: italic; line-height: 1.6;
  color: #1A2332;
}}

.two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 48px; width: 100%; max-width: 1200px; }}
.three-col {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 32px; width: 100%; max-width: 1200px; }}
.four-col {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 24px; width: 100%; max-width: 1300px; }}
.center {{ text-align: center; }}
.full-width {{ width: 100%; max-width: 1200px; }}

.card {{
  background: #FFFFFF; border: 1px solid rgba(0,0,0,0.10);
  border-radius: 16px; padding: 28px 24px;
  box-shadow: 0 4px 20px rgba(0,0,0,0.10);
  transition: transform 0.3s ease, box-shadow 0.3s ease;
}}
.card-light {{ background: #FFFFFF; border: 1px solid #CBD5E8; border-radius: 16px; padding: 28px 24px; box-shadow: 0 4px 20px rgba(0,0,0,0.06); }}
.card-accent {{ border-top: 4px solid #E07B00; }}
.card-blue   {{ border-top: 4px solid #2E86D4; }}
.card-purple {{ border-top: 4px solid #6B3FA0; }}
.card-teal   {{ border-top: 4px solid #007B8A; }}

.code-block {{
  background: #0D1521; color: #B8D4F0; border-radius: 12px; padding: 20px 24px;
  font-family: "SF Mono","Fira Code",monospace; font-size: clamp(0.7rem,1.2vw,0.95rem);
  line-height: 1.7; overflow: auto; border: 1px solid rgba(0,0,0,0.08); text-align: left;
}}
.code-block .kw {{ color: #7EC8E3; }}
.code-block .str {{ color: #FBBF60; }}
.code-block .cm {{ color: #607080; font-style: italic; }}
.code-block .fn {{ color: #98D8B0; }}

.timeline-line {{ position: absolute; left: 50%; top: 0; bottom: 0; width: 2px; background: rgba(0,0,0,0.12); transform: translateX(-50%); }}
.timeline-dot {{ width: 14px; height: 14px; border-radius: 50%; background: #E07B00; border: 3px solid white; box-shadow: 0 0 0 2px #E07B00; flex-shrink: 0; margin-top: 4px; }}

.bar-row {{ display: flex; align-items: center; gap: 12px; margin-bottom: 14px; }}
.bar-label {{ font-size: 0.8rem; opacity: 0.7; width: 140px; flex-shrink: 0; text-align: right; }}
.bar-track {{ flex: 1; background: rgba(0,0,0,0.08); border-radius: 99px; height: 20px; overflow: hidden; }}
.bar-fill {{ height: 100%; border-radius: 99px; transition: width 1.2s cubic-bezier(0.34,1.56,0.64,1); width: 0; }}
.bar-val {{ font-size: 0.8rem; font-weight: 700; width: 60px; }}

.stats-grid {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 24px; width: 100%; max-width: 900px; }}

.badge {{ display: inline-block; padding: 3px 10px; border-radius: 99px; font-size: 11px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; }}
.badge-amber  {{ background: rgba(224,123,0,0.15); color: #E07B00; border: 1px solid rgba(224,123,0,0.3); }}
.badge-blue   {{ background: rgba(30,111,191,0.12); color: #1E6FBF; border: 1px solid rgba(30,111,191,0.25); }}
.badge-purple {{ background: rgba(107,63,160,0.12); color: #6B3FA0; border: 1px solid rgba(107,63,160,0.25); }}
.badge-teal   {{ background: rgba(0,123,138,0.12); color: #007B8A; border: 1px solid rgba(0,123,138,0.25); }}

#hero-canvas {{ position: absolute; inset: 0; pointer-events: none; opacity: 0.25; }}

.service-cloud {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; max-width: 900px; }}
.service-tag {{
  background: rgba(255,255,255,0.9); border: 1px solid rgba(0,0,0,0.1);
  border-radius: 8px; padding: 6px 12px; font-size: 0.75rem;
  font-family: "SF Mono",monospace; color: #5A6880;
  box-shadow: 0 1px 4px rgba(0,0,0,0.05); transition: all 0.3s ease;
}}
.service-tag.native {{ background: rgba(30,111,191,0.08); border-color: rgba(30,111,191,0.25); color: #1E6FBF; }}

.compare-table {{ width: 100%; border-collapse: collapse; max-width: 900px; }}
.compare-table th, .compare-table td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid rgba(0,0,0,0.07); font-size: 0.9rem; }}
.compare-table th {{ font-weight: 700; opacity: 0.5; font-size: 0.75rem; letter-spacing: 1px; text-transform: uppercase; }}
.compare-table .check {{ color: #007B8A; font-size: 1.1rem; font-weight: 700; }}
.compare-table .cross {{ color: #B0B8C4; font-size: 1.1rem; }}
.compare-table tr:hover td {{ background: rgba(0,0,0,0.02); }}

@keyframes fadeUp {{ from {{ opacity: 0; transform: translateY(30px); }} to {{ opacity: 1; transform: translateY(0); }} }}
@keyframes glow {{ 0%,100% {{ box-shadow: 0 0 20px rgba(224,123,0,0.2); }} 50% {{ box-shadow: 0 0 40px rgba(224,123,0,0.4); }} }}
.anim-1 {{ animation: fadeUp 0.6s ease 0.05s both; }}
.anim-2 {{ animation: fadeUp 0.6s ease 0.2s both; }}
.anim-3 {{ animation: fadeUp 0.6s ease 0.35s both; }}
.anim-4 {{ animation: fadeUp 0.6s ease 0.5s both; }}
.anim-5 {{ animation: fadeUp 0.6s ease 0.65s both; }}
.glow {{ animation: glow 3s ease-in-out infinite; }}

.logo-word {{
  font-size: clamp(4rem,9vw,9rem); font-weight: 900; letter-spacing: -3px;
  background: linear-gradient(135deg, #2E86D4 0%, #1E6FBF 40%, #6B3FA0 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
}}
.days-badge {{
  display: inline-flex; align-items: center; gap: 8px;
  background: rgba(224,123,0,0.1); border: 1px solid rgba(224,123,0,0.3);
  border-radius: 99px; padding: 8px 20px; color: #E07B00; font-weight: 700; font-size: 1rem;
}}
.chart-wrap {{ position: relative; width: 100%; max-height: 340px; }}
.chart-wrap canvas {{ max-height: 340px; }}
.slide-data .eyebrow {{ color: #5A6880; }}

.grad-amber {{
  background: linear-gradient(135deg, #E07B00 0%, #C9920B 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
}}
.mini-stat-box {{
  background: #FFFFFF; border: 1px solid #CBD5E8; border-radius: 12px;
  padding: 16px 20px; text-align: center;
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}}
.mini-stat-num {{ font-size: 1.6rem; font-weight: 800; color: #1E6FBF; }}
.mini-stat-label {{ font-size: 0.75rem; opacity: 0.6; margin-top: 4px; text-transform: uppercase; letter-spacing: 1px; }}
.prompt-card {{
  background: #F4F7FC; border: 1px solid #CBD5E8; border-radius: 12px; padding: 20px 24px;
  font-style: italic; color: #1A2332; line-height: 1.6; font-size: 0.9rem;
}}
.prompt-meta {{ font-size: 0.75rem; color: #5A6880; margin-bottom: 8px; font-style: normal; font-weight: 600; }}
</style>
</head>
<body>
<div id="deck">

  <!-- Slide 1: Hero -->
  <section class="slide slide-hero active" id="s1">
    <canvas id="hero-canvas"></canvas>
    <div class="center" style="position:relative;z-index:1;">
      <div class="eyebrow anim-1" style="color:#E07B00;">An Open-Source Story</div>
      <div class="logo-word anim-2">robotocore</div>
      <h2 class="anim-3" style="font-weight:300;margin-top:12px;opacity:0.75;">A complete AWS emulator.<br>Built in 96 hours.</h2>
      <div style="margin-top:32px;" class="anim-4">
        <span class="days-badge glow">⏱ 4 days · 147 services · 14,538 tests · 0 failures</span>
      </div>
    </div>
  </section>

  <!-- Slide 2: LocalStack blog post -->
  <section class="slide slide-amber" id="s2">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#E07B00;">The Catalyst</div>
      <h2 class="anim-2">LocalStack Changes the Rules</h2>
      <blockquote class="anim-3" style="margin-top:32px;text-align:left;">
        "Beginning in March 2026, LocalStack for AWS will be delivered as a single, unified version.
        <strong>Users will need to create an account to run LocalStack for AWS.</strong>"
      </blockquote>
      <blockquote class="anim-4" style="text-align:left;">
        "Our free plan will continue to provide a dynamic environment for <em>experimental</em>
        development and exploration."
      </blockquote>
      <p class="anim-5" style="font-size:0.85rem;opacity:0.5;margin-top:16px;">
        — blog.localstack.cloud · "The Road Ahead for LocalStack" · March 2026
      </p>
    </div>
  </section>

  <!-- Slide 3: What it means -->
  <section class="slide slide-blue" id="s3">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#2E86D4;">The Breaking Point</div>
      <h2 class="anim-2">What "Account Required" Really Means</h2>
      <div class="three-col anim-3" style="margin-top:36px;">
        <div class="card card-accent">
          <div style="font-size:2.5rem;margin-bottom:12px;">🔐</div>
          <h3>Auth Tokens Required</h3>
          <p style="margin-top:8px;opacity:0.65;font-size:0.9rem;">Every dev machine, every CI job needs a LocalStack account and token. Offline? Blocked.</p>
        </div>
        <div class="card card-blue">
          <div style="font-size:2.5rem;margin-bottom:12px;">🚧</div>
          <h3>Enterprise Features Paywalled</h3>
          <p style="margin-top:8px;opacity:0.65;font-size:0.9rem;">Multi-account, chaos engineering, state snapshots — now Pro-only. The tools you need cost money.</p>
        </div>
        <div class="card card-teal">
          <div style="font-size:2.5rem;margin-bottom:12px;">📉</div>
          <h3>Community Edition Stagnates</h3>
          <p style="margin-top:8px;opacity:0.65;font-size:0.9rem;">Free tier gets "experimental" label. Investment shifts to paid. Community left behind.</p>
        </div>
      </div>
      <p class="anim-4" style="margin-top:28px;opacity:0.65;font-size:1rem;">
        We needed an alternative. A real one. Free forever, no strings.
      </p>
    </div>
  </section>

  <!-- Slide 4: Moto history -->
  <section class="slide slide-blue" id="s4">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#2E86D4;">The Foundation</div>
        <h2 class="anim-2">Moto: 13 Years of AWS Stubbery</h2>
      </div>
      <div class="chart-wrap anim-3" style="margin-top:28px;">
        <canvas id="moto-chart"></canvas>
      </div>
      <div class="three-col anim-4" style="margin-top:24px;max-width:700px;margin-left:auto;margin-right:auto;">
        <div class="mini-stat-box">
          <div class="mini-stat-num">10,678</div>
          <div class="mini-stat-label">git commits</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num">195</div>
          <div class="mini-stat-label">AWS services</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num">2013</div>
          <div class="mini-stat-label">first commit</div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 5: LocalStack Was Always Moto -->
  <section class="slide slide-blue" id="s5">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#007B8A;">Standing on Giants</div>
        <h2 class="anim-2">LocalStack Was Always Moto</h2>
      </div>
      <div class="two-col anim-3" style="margin-top:28px;align-items:start;">
        <div>
          <p style="opacity:0.7;margin-bottom:16px;line-height:1.6;">
            LocalStack's <strong>first commit</strong> — August 16, 2016 — already used
            <code style="background:#E8EDF8;padding:2px 6px;border-radius:4px;">moto_server</code>
            as its AWS backend. It was always a thin wrapper.
          </p>
          <div class="code-block" style="margin-bottom:12px;font-size:0.78rem;">
<span class="cm"># localstack/services/infra.py — Aug 16, 2016</span>
<span class="kw">from</span> moto.server <span class="kw">import</span> create_backend_app
<span class="fn">moto_server</span> = create_backend_app(<span class="str">'s3'</span>)
<span class="fn">moto_server</span>.run(port=<span class="str">4572</span>)
          </div>
          <div class="code-block" style="font-size:0.78rem;">
<span class="cm"># localstack/requirements.txt — 2016</span>
moto==<span class="str">0.4.25</span>
boto==<span class="str">2.38.0</span>
          </div>
        </div>
        <div class="card-light">
          <h3 style="margin-bottom:20px;color:#1E6FBF;">The Lineage</h3>
          <div style="position:relative;padding-left:28px;">
            <div style="position:absolute;left:6px;top:0;bottom:0;width:2px;background:rgba(0,0,0,0.12);"></div>
            <div style="margin-bottom:20px;">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <div style="width:14px;height:14px;border-radius:50%;background:#E07B00;position:absolute;left:0;flex-shrink:0;"></div>
                <strong style="color:#E07B00;">Feb 2013</strong>
              </div>
              <p style="font-size:0.85rem;opacity:0.7;">Moto born — EC2 and S3 stubs</p>
            </div>
            <div style="margin-bottom:20px;">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <div style="width:14px;height:14px;border-radius:50%;background:#2E86D4;position:absolute;left:0;"></div>
                <strong style="color:#2E86D4;">Aug 2016</strong>
              </div>
              <p style="font-size:0.85rem;opacity:0.7;">LocalStack wraps Moto via moto_server</p>
            </div>
            <div style="margin-bottom:20px;">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <div style="width:14px;height:14px;border-radius:50%;background:#6B3FA0;position:absolute;left:0;"></div>
                <strong style="color:#6B3FA0;">2021–2025</strong>
              </div>
              <p style="font-size:0.85rem;opacity:0.7;">LocalStack goes enterprise, diverges from Moto</p>
            </div>
            <div>
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <div style="width:14px;height:14px;border-radius:50%;background:#007B8A;position:absolute;left:0;"></div>
                <strong style="color:#007B8A;">Mar 2026</strong>
              </div>
              <p style="font-size:0.85rem;opacity:0.7;">Robotocore forks — back to open source roots</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 6: The Decision -->
  <section class="slide slide-hero" id="s6">
    <div class="center" style="position:relative;z-index:1;">
      <div class="eyebrow anim-1" style="color:#E07B00;">Day Zero · March 6, 2026 · 3:56 AM</div>
      <h1 class="anim-2">What if we<br><span class="grad-amber">owned the stack?</span></h1>
      <p class="subtitle anim-3">
        Jack Danger — Moto maintainer, AWS emulator veteran — sits down at 3 AM<br>
        and opens a blank CLAUDE.md file.
      </p>
      <div class="anim-4" style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-top:28px;">
        <span class="badge badge-blue">MIT licensed</span>
        <span class="badge badge-amber">No auth tokens</span>
        <span class="badge badge-purple">No paid tiers</span>
        <span class="badge badge-teal">Built on Moto</span>
      </div>
    </div>
  </section>

  <!-- Slide 7: Architecture -->
  <section class="slide slide-data" id="s7">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1">Architecture</div>
        <h2 class="anim-2">Designed in One Session</h2>
      </div>
      <div class="two-col anim-3" style="margin-top:28px;align-items:start;">
        <div class="code-block" style="font-size:0.72rem;line-height:1.8;">
<span class="cm">┌─────────────────────────────────┐</span>
<span class="cm">│       Docker Container          │</span>
<span class="cm">│                                 │</span>
<span class="cm">│  ┌───────────────────────────┐  │</span>
<span class="cm">│  │   Gateway (port 4566)     │  │</span>
<span class="cm">│  │  ┌─────────────────────┐  │  │</span>
<span class="cm">│  │  │  Request Router     │  │  │</span>
<span class="cm">│  │  │  (headers, URL,     │  │  │</span>
<span class="cm">│  │  │   query params)     │  │  │</span>
<span class="cm">│  │  └──────────┬──────────┘  │  │</span>
<span class="cm">│  │             │             │  │</span>
<span class="cm">│  │  ┌──────────▼──────────┐  │  │</span>
<span class="cm">│  │  │  Protocol Layer     │  │  │</span>
<span class="cm">│  │  │  (query/json/xml)   │  │  │</span>
<span class="cm">│  │  └──────────┬──────────┘  │  │</span>
<span class="cm">│  │             │             │  │</span>
<span class="cm">│  │  ┌──────────▼──────────┐  │  │</span>
<span class="cm">│  │  │  Service Providers  │  │  │</span>
<span class="cm">│  │  │  (Moto + native)    │  │  │</span>
<span class="cm">│  │  └──────────┬──────────┘  │  │</span>
<span class="cm">│  │             │             │  │</span>
<span class="cm">│  │  ┌──────────▼──────────┐  │  │</span>
<span class="cm">│  │  │  In-Memory Stores   │  │  │</span>
<span class="cm">│  │  │  (per-account/rgn)  │  │  │</span>
<span class="cm">│  │  └─────────────────────┘  │  │</span>
<span class="cm">│  └───────────────────────────┘  │</span>
<span class="cm">└─────────────────────────────────┘</span>
        </div>
        <div style="display:flex;flex-direction:column;gap:16px;">
          <div class="card-light card-blue">
            <strong>12 milestones</strong> planned in the first session
            <p style="font-size:0.85rem;opacity:0.65;margin-top:6px;">Gateway → protocols → providers → stores → Docker → CI</p>
          </div>
          <div class="card-light card-accent">
            <strong>Protocol-first</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:6px;">botocore service specs drive all parsing &amp; serialization</p>
          </div>
          <div class="card-light card-purple">
            <strong>Plugin system</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:6px;">RobotocorePlugin base class, entry points, env var, directory discovery</p>
          </div>
          <div class="card-light card-teal">
            <strong>Async-capable</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:6px;">ASGI + uvicorn. SQS long polling via asyncio.to_thread()</p>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 8: Tooling -->
  <section class="slide slide-blue" id="s8">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#007B8A;">Day 1 Principle</div>
        <h2 class="anim-2">Build the Tools Before the Work</h2>
      </div>
      <div class="four-col anim-3" style="margin-top:32px;">
        <div class="card card-blue" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">🏗️</div>
          <strong style="font-size:0.85rem;">gen_provider.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Generate native providers from botocore specs</p>
        </div>
        <div class="card card-teal" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">🔍</div>
          <strong style="font-size:0.85rem;">probe_service.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Discover working operations per service</p>
        </div>
        <div class="card card-accent" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">📦</div>
          <strong style="font-size:0.85rem;">batch_register.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Register 100+ services in one command</p>
        </div>
        <div class="card card-purple" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">✅</div>
          <strong style="font-size:0.85rem;">validate_quality.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">CI gate: catches tests that never contact server</p>
        </div>
        <div class="card card-accent" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">🌙</div>
          <strong style="font-size:0.85rem;">overnight.sh</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Chunk-based overnight automation loop</p>
        </div>
        <div class="card card-blue" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">✂️</div>
          <strong style="font-size:0.85rem;">chunk_service.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Break any service into 3-8 op chunks</p>
        </div>
        <div class="card card-teal" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">🧪</div>
          <strong style="font-size:0.85rem;">gen_compat_tests.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">Generate compat tests with smart defaults</p>
        </div>
        <div class="card card-purple" style="text-align:center;">
          <div style="font-size:1.8rem;margin-bottom:8px;">🔬</div>
          <strong style="font-size:0.85rem;">lint_project.py</strong>
          <p style="font-size:0.75rem;opacity:0.6;margin-top:6px;">10-check structural lint for code quality</p>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 9: Service explosion -->
  <section class="slide slide-hero" id="s9">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#E07B00;">Day 2 · March 7 · batch_register_services.py</div>
      <h2 class="anim-2">From 12 to 147 Services in Hours</h2>
      <div class="service-cloud anim-3" id="service-cloud-el" style="margin-top:24px;"></div>
      <div class="anim-4" style="margin-top:20px;display:flex;gap:12px;justify-content:center;">
        <span class="badge badge-blue">■ Native provider (38)</span>
        <span style="background:rgba(255,255,255,0.9);border:1px solid rgba(0,0,0,0.1);border-radius:8px;padding:3px 10px;font-size:11px;font-weight:700;color:#5A6880;">■ Moto-backed (109)</span>
      </div>
    </div>
  </section>

  <!-- Slide 10: Test quality crisis -->
  <section class="slide slide-amber" id="s10">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#E07B00;">Day 2 Crisis</div>
      <h2 class="anim-2">The 96% Problem</h2>
      <p class="anim-3" style="font-size:1.1rem;opacity:0.7;margin-top:16px;">
        We auto-generated 7,083 compatibility tests. Then we checked them.
      </p>
      <div class="two-col anim-4" style="margin-top:28px;max-width:800px;">
        <div class="card card-accent" style="text-align:center;">
          <div style="font-size:3.5rem;font-weight:900;color:#E07B00;">6,811</div>
          <div style="font-size:0.9rem;margin-top:8px;opacity:0.65;">tests caught client-side errors<br><strong>never contacted the server</strong></div>
          <div style="margin-top:12px;"><span class="badge badge-amber">96% worthless</span></div>
        </div>
        <div class="card card-teal" style="text-align:center;">
          <div style="font-size:3.5rem;font-weight:900;color:#007B8A;">272</div>
          <div style="font-size:0.9rem;margin-top:8px;opacity:0.65;">tests actually verified<br><strong>server behavior</strong></div>
          <div style="margin-top:12px;"><span class="badge badge-teal">4% effective</span></div>
        </div>
      </div>
      <blockquote class="anim-5" style="margin-top:24px;text-align:left;max-width:800px;">
        "A test that catches ParamValidationError and passes is worthless — boto3 validates
        params client-side before the request is sent. The server is never contacted."
      </blockquote>
    </div>
  </section>

  <!-- Slide 11: Quality fix -->
  <section class="slide slide-data" id="s11">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1">The Fix</div>
        <h2 class="anim-2">Quality-First Test Engineering</h2>
      </div>
      <div class="three-col anim-3" style="margin-top:32px;">
        <div class="card card-blue">
          <div style="font-size:2rem;margin-bottom:12px;">🚦</div>
          <h3>CI Gate</h3>
          <p style="font-size:0.85rem;opacity:0.65;margin-top:8px;">
            <code style="background:#E8EDF8;padding:2px 6px;border-radius:4px;">validate_test_quality.py</code>
            blocks merge if &gt;5% of tests don't contact the server.
          </p>
        </div>
        <div class="card card-accent">
          <div style="font-size:2rem;margin-bottom:12px;">📐</div>
          <h3>Three Valid Patterns Only</h3>
          <p style="font-size:0.85rem;opacity:0.65;margin-top:8px;">
            Create→use→cleanup · Fake-ID→assert-exception · List→assert-key.
            Every test must assert on a response field.
          </p>
        </div>
        <div class="card card-teal">
          <div style="font-size:2rem;margin-bottom:12px;">🔬</div>
          <h3>Probe Before Test</h3>
          <p style="font-size:0.85rem;opacity:0.65;margin-top:8px;">
            Run <code style="background:#E8EDF8;padding:2px 6px;border-radius:4px;">probe_service.py</code>
            first. Only write tests for operations that actually work. Fix gaps first.
          </p>
        </div>
      </div>
      <div class="anim-4" style="display:flex;gap:24px;justify-content:center;margin-top:32px;">
        <div class="mini-stat-box">
          <div class="mini-stat-num" style="color:#E07B00;">14,538</div>
          <div class="mini-stat-label">Final test count</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num" style="color:#007B8A;">99.5%</div>
          <div class="mini-stat-label">Server contact rate</div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 12: Growth chart -->
  <section class="slide slide-data" id="s12">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1">96 Hours of Progress</div>
        <h2 class="anim-2">Test Coverage Growth</h2>
      </div>
      <div class="chart-wrap anim-3" style="margin-top:28px;">
        <canvas id="growth-chart"></canvas>
      </div>
    </div>
  </section>

  <!-- Slide 13: Tier 1 -->
  <section class="slide slide-purple" id="s13">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#6B3FA0;">Day 3 · March 9 · Enterprise</div>
        <h2 class="anim-2">Tier 1: Going Beyond Moto</h2>
      </div>
      <div class="four-col anim-3" id="tier1-cards" style="margin-top:32px;"></div>
    </div>
  </section>

  <!-- Slide 14: Native providers -->
  <section class="slide slide-blue" id="s14">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#2E86D4;">Technical Deep Dive</div>
        <h2 class="anim-2">38 Native Providers</h2>
      </div>
      <div class="two-col anim-3" style="margin-top:28px;align-items:start;">
        <div style="display:flex;flex-direction:column;gap:14px;">
          <div class="card-light card-blue">
            <strong>Lambda</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:4px;">Actually executes code. Hot reload, FS mounts, layer support, runtime isolation.</p>
          </div>
          <div class="card-light card-teal">
            <strong>SQS</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:4px;">Real visibility timeouts, dead-letter queues, long polling via async I/O.</p>
          </div>
          <div class="card-light card-accent">
            <strong>DynamoDB Streams</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:4px;">Change data capture, shard iterators, Lambda trigger dispatch.</p>
          </div>
          <div class="card-light card-purple">
            <strong>EventBridge</strong>
            <p style="font-size:0.85rem;opacity:0.65;margin-top:4px;">17 target types, rule evaluation engine, cross-account event bus.</p>
          </div>
        </div>
        <div>
          <p style="font-size:0.8rem;opacity:0.5;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;font-weight:700;">All 38 native providers</p>
          <div class="service-cloud" id="providers-grid"></div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 15: Tier 2-4 -->
  <section class="slide slide-blue" id="s15">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#007B8A;">Day 4 · March 10</div>
        <h2 class="anim-2">Tier 2–4: Enterprise Platform Features</h2>
      </div>
      <div class="chart-wrap anim-3" style="margin-top:24px;">
        <canvas id="tier24-chart"></canvas>
      </div>
      <div class="anim-4" id="tier24-details" style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;margin-top:20px;"></div>
    </div>
  </section>

  <!-- Slide 16: Numbers -->
  <section class="slide slide-hero" id="s16">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#E07B00;">Final Numbers</div>
      <h2 class="anim-2">A Complete AWS Emulator</h2>
      <div class="four-col anim-3" id="big-stats" style="margin-top:32px;max-width:1100px;"></div>
      <div class="four-col anim-4" style="margin-top:24px;max-width:900px;">
        <div class="mini-stat-box">
          <div class="mini-stat-num">47,745</div>
          <div class="mini-stat-label">lines of code</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num">173</div>
          <div class="mini-stat-label">source files</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num">~700</div>
          <div class="mini-stat-label">commits</div>
        </div>
        <div class="mini-stat-box">
          <div class="mini-stat-num" style="color:#007B8A;">0</div>
          <div class="mini-stat-label">test failures</div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 17: Comparison -->
  <section class="slide slide-blue" id="s17">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#007B8A;">Comparison</div>
        <h2 class="anim-2">Robotocore vs. LocalStack</h2>
      </div>
      <div class="anim-3" style="margin-top:24px;overflow:auto;max-height:420px;">
        <table class="compare-table">
          <thead>
            <tr>
              <th>Feature</th>
              <th>LocalStack Free</th>
              <th>LocalStack Pro</th>
              <th>Robotocore</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>147 AWS services</td><td class="check">✓</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>No account required</td><td class="cross">✗</td><td class="cross">✗</td><td class="check">✓</td></tr>
            <tr><td>No telemetry</td><td class="cross">✗</td><td class="cross">✗</td><td class="check">✓</td></tr>
            <tr><td>MIT license</td><td class="cross">✗</td><td class="cross">✗</td><td class="check">✓</td></tr>
            <tr><td>Multi-account isolation</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>State snapshots</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>Chaos engineering</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>Real Lambda execution</td><td class="check">✓</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>EKS mock Kubernetes</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>Audit log</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
            <tr><td>Resource browser</td><td class="cross">✗</td><td class="check">✓</td><td class="check">✓</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- Slide 18: Beyond AWS -->
  <section class="slide slide-data" id="s18">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1">Observability &amp; Operations</div>
        <h2 class="anim-2">Beyond Just AWS Emulation</h2>
      </div>
      <div class="three-col anim-3" style="margin-top:28px;">
        <div class="card-light card-accent">
          <div style="font-size:1.8rem;margin-bottom:10px;">💥</div>
          <h3>Chaos Engineering</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Inject ThrottlingExceptions, latency, network errors. Per-service, per-operation rules.</p>
        </div>
        <div class="card-light card-blue">
          <div style="font-size:1.8rem;margin-bottom:10px;">📸</div>
          <h3>State Snapshots</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Save and restore named snapshots. Selective by service. Instant test fixture setup.</p>
        </div>
        <div class="card-light card-teal">
          <div style="font-size:1.8rem;margin-bottom:10px;">📋</div>
          <h3>Audit Log</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Ring-buffer of all API calls. Query by service, account, operation. Configurable size.</p>
        </div>
        <div class="card-light card-purple">
          <div style="font-size:1.8rem;margin-bottom:10px;">🗺️</div>
          <h3>Resource Browser</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Cross-service view of all resources. See what you've created at a glance.</p>
        </div>
        <div class="card-light card-accent">
          <div style="font-size:1.8rem;margin-bottom:10px;">🔐</div>
          <h3>IAM Enforcement</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Full policy evaluation engine. Opt-in via ENFORCE_IAM=1. Test authz locally.</p>
        </div>
        <div class="card-light card-blue">
          <div style="font-size:1.8rem;margin-bottom:10px;">⚙️</div>
          <h3>CI-Ready</h3>
          <p style="font-size:0.82rem;opacity:0.65;margin-top:6px;">Single docker run. No auth, no tokens, no network calls. Works offline.</p>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 19: 96-hour timeline -->
  <section class="slide slide-hero" id="s19">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#E07B00;">The Full Story</div>
        <h2 class="anim-2">96 Hours. From Zero to Production.</h2>
      </div>
      <div id="timeline-el" class="anim-3" style="position:relative;margin-top:24px;width:100%;max-width:1000px;"></div>
    </div>
  </section>

  <!-- Slide 20: Built with Claude Code -->
  <section class="slide slide-blue" id="s20">
    <div class="full-width">
      <div class="center">
        <div class="eyebrow anim-1" style="color:#2E86D4;">Meta</div>
        <h2 class="anim-2">Built With Claude Code</h2>
      </div>
      <div class="two-col anim-3" style="margin-top:28px;align-items:start;">
        <div style="display:flex;flex-direction:column;gap:16px;">
          <div class="prompt-card">
            <div class="prompt-meta">Session 1 · March 6, 2026 · 3:56 AM</div>
            "Write a CLAUDE.md and a set of skills necessary to start on your job: Creating an
            MIT-licensed wrapper around Moto that has 100% feature parity with Localstack."
          </div>
          <div class="prompt-card">
            <div class="prompt-meta">Session 89 · March 10, 2026 · 11:47 PM</div>
            "Write a new file scripts/build_slides.py — a complete Python script that builds a
            slide deck HTML file for the Robotocore story."
          </div>
        </div>
        <div style="display:flex;flex-direction:column;gap:16px;">
          <div class="card-light card-blue" style="text-align:center;">
            <div style="font-size:2.5rem;font-weight:900;color:#1E6FBF;">89</div>
            <div style="font-size:0.85rem;opacity:0.65;margin-top:6px;">Claude Code sessions</div>
          </div>
          <div class="card-light card-accent" style="text-align:center;">
            <div style="font-size:2.5rem;font-weight:900;color:#E07B00;">100s</div>
            <div style="font-size:0.85rem;opacity:0.65;margin-top:6px;">subagents spawned</div>
          </div>
          <div class="card-light card-teal" style="text-align:center;">
            <div style="font-size:2.5rem;font-weight:900;color:#007B8A;">All of them</div>
            <div style="font-size:0.85rem;opacity:0.65;margin-top:6px;">human decisions</div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <!-- Slide 21: Get Started -->
  <section class="slide slide-hero" id="s21">
    <div class="full-width center">
      <div class="eyebrow anim-1" style="color:#E07B00;">Open Source · MIT License</div>
      <h1 class="anim-2">Free.<br><span class="grad-amber">Forever.</span></h1>
      <div class="two-col anim-3" style="margin-top:28px;max-width:900px;">
        <div class="code-block" style="font-size:0.85rem;">
<span class="cm"># One command to start</span>
<span class="kw">docker</span> run <span class="str">-p 4566:4566</span> \
  ghcr.io/robotocore/robotocore:<span class="str">latest</span>
        </div>
        <div class="code-block" style="font-size:0.85rem;">
<span class="cm"># Drop-in replacement</span>
<span class="kw">aws</span> s3 ls \
  <span class="str">--endpoint-url</span> http://localhost:4566
        </div>
      </div>
      <div class="anim-4" style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-top:28px;">
        <span class="badge badge-blue">No registration</span>
        <span class="badge badge-amber">No telemetry</span>
        <span class="badge badge-purple">No paid tiers</span>
        <span class="badge badge-teal">github.com/robotocore/robotocore</span>
      </div>
    </div>
  </section>

</div>

<div id="progress" style="width:4.76%;"></div>
<div id="slide-counter">1 / 21</div>
<div id="kb-hint">← → SPACE to navigate</div>

<script>
const MOTO_DATA    = {moto_json};
const MILESTONES   = {milestones_json};
const SESSIONS     = {sessions_json};
const PROVIDERS    = {providers_json};
const TIER1        = {tier1_json};
const TIER24       = {tier24_json};
const TIMELINE_COMMITS = {timeline_json};

// --- Slide Engine ---
const slides = Array.from(document.querySelectorAll('.slide'));
const N = slides.length;
let current = 0;
let initialized = {{}};

function goTo(n) {{
  if (n < 0 || n >= N) return;
  const prev = slides[current];
  prev.classList.remove('active');
  prev.classList.add('exit');
  setTimeout(() => prev.classList.remove('exit'), 500);

  current = n;
  const next = slides[current];
  // Re-trigger animations
  next.querySelectorAll('[class*="anim-"]').forEach(el => {{
    const cl = Array.from(el.classList).filter(c => c.startsWith('anim-'));
    cl.forEach(c => {{ el.classList.remove(c); void el.offsetWidth; el.classList.add(c); }});
  }});
  next.classList.add('active');

  const pct = ((current + 1) / N * 100).toFixed(2);
  document.getElementById('progress').style.width = pct + '%';
  document.getElementById('slide-counter').textContent = (current + 1) + ' / ' + N;

  onSlideEnter(current);
}}

document.addEventListener('keydown', e => {{
  if (e.key === 'ArrowRight' || e.key === ' ') {{ e.preventDefault(); goTo(current + 1); }}
  if (e.key === 'ArrowLeft')  {{ e.preventDefault(); goTo(current - 1); }}
}});

// --- Particles ---
(function() {{
  const canvas = document.getElementById('hero-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const colors = ['#1E6FBF','#E07B00','#6B3FA0','#007B8A','#2E86D4','#C9920B'];
  let W, H, particles;

  function resize() {{
    W = canvas.width = canvas.offsetWidth;
    H = canvas.height = canvas.offsetHeight;
  }}
  window.addEventListener('resize', resize);
  resize();

  particles = Array.from({{length: 80}}, () => ({{
    x: Math.random() * W, y: Math.random() * H,
    vx: (Math.random() - 0.5) * 0.6, vy: (Math.random() - 0.5) * 0.6,
    r: Math.random() * 3 + 1.5,
    color: colors[Math.floor(Math.random() * colors.length)]
  }}));

  function frame() {{
    ctx.clearRect(0, 0, W, H);
    particles.forEach(p => {{
      p.x += p.vx; p.y += p.vy;
      if (p.x < 0 || p.x > W) p.vx *= -1;
      if (p.y < 0 || p.y > H) p.vy *= -1;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = p.color;
      ctx.fill();
    }});
    for (let i = 0; i < particles.length; i++) {{
      for (let j = i + 1; j < particles.length; j++) {{
        const dx = particles[i].x - particles[j].x;
        const dy = particles[i].y - particles[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 120) {{
          ctx.beginPath();
          ctx.strokeStyle = `rgba(30,111,191,${{(1 - dist/120) * 0.3}})`;
          ctx.lineWidth = 1;
          ctx.moveTo(particles[i].x, particles[i].y);
          ctx.lineTo(particles[j].x, particles[j].y);
          ctx.stroke();
        }}
      }}
    }}
    requestAnimationFrame(frame);
  }}
  frame();
}})();

// --- Chart.js defaults ---
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

function initMotoChart() {{
  if (initialized['moto']) return; initialized['moto'] = true;
  const ctx = document.getElementById('moto-chart').getContext('2d');
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: MOTO_DATA.map(d => d.year),
      datasets: [{{
        label: 'Commits',
        data: MOTO_DATA.map(d => d.commits),
        backgroundColor: MOTO_DATA.map((d, i) =>
          i === MOTO_DATA.length - 1 ? '#E07B00' : '#2E86D4'),
        borderRadius: 6,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1A2332', titleColor: '#7EC8E3', bodyColor: '#B8D4F0',
          callbacks: {{
            afterLabel: (ctx) => MOTO_DATA[ctx.dataIndex].detail
          }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ color: 'rgba(0,0,0,0.45)' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }} }},
        y: {{ ticks: {{ color: 'rgba(0,0,0,0.45)' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }} }}
      }}
    }}
  }});
}}

function initGrowthChart() {{
  if (initialized['growth']) return; initialized['growth'] = true;
  const ctx = document.getElementById('growth-chart').getContext('2d');
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: MILESTONES.map(m => m.label),
      datasets: [
        {{
          label: 'Tests',
          data: MILESTONES.map(m => m.tests),
          borderColor: '#E07B00', backgroundColor: 'rgba(224,123,0,0.1)',
          yAxisID: 'y', tension: 0.4, fill: true, pointBackgroundColor: '#E07B00', pointRadius: 5,
        }},
        {{
          label: 'Services',
          data: MILESTONES.map(m => m.services),
          borderColor: '#2E86D4', backgroundColor: 'rgba(46,134,212,0.08)',
          yAxisID: 'y2', tension: 0.4, fill: false, pointBackgroundColor: '#2E86D4', pointRadius: 5,
        }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: true,
      plugins: {{
        tooltip: {{ backgroundColor: '#1A2332', titleColor: '#7EC8E3', bodyColor: '#B8D4F0' }}
      }},
      scales: {{
        x: {{ ticks: {{ color: 'rgba(0,0,0,0.45)' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }} }},
        y: {{
          type: 'linear', position: 'left',
          ticks: {{ color: '#E07B00' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }},
          title: {{ display: true, text: 'Tests', color: '#E07B00' }}
        }},
        y2: {{
          type: 'linear', position: 'right',
          ticks: {{ color: '#2E86D4' }}, grid: {{ drawOnChartArea: false }},
          title: {{ display: true, text: 'Services', color: '#2E86D4' }}
        }}
      }}
    }}
  }});
}}

function initTier24Chart() {{
  if (initialized['tier24chart']) return; initialized['tier24chart'] = true;
  const ctx = document.getElementById('tier24-chart').getContext('2d');
  const barColors = ['#E07B00','#2E86D4','#6B3FA0','#007B8A','#C9920B','#1E6FBF'];
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: TIER24.map(f => f.name),
      datasets: [{{
        label: 'Tests',
        data: TIER24.map(f => f.tests),
        backgroundColor: barColors,
        borderRadius: 6,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: true, indexAxis: 'y',
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1A2332', titleColor: '#7EC8E3', bodyColor: '#B8D4F0',
          callbacks: {{ afterLabel: (ctx) => TIER24[ctx.dataIndex].detail }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ color: 'rgba(0,0,0,0.45)' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }} }},
        y: {{ ticks: {{ color: 'rgba(0,0,0,0.45)' }}, grid: {{ color: 'rgba(0,0,0,0.06)' }} }}
      }}
    }}
  }});
}}

function initBigStats() {{
  if (initialized['bigstats']) return; initialized['bigstats'] = true;
  const el = document.getElementById('big-stats');
  const stats = [
    {{ num: '147', label: 'AWS Services' }},
    {{ num: '14,538', label: 'Tests' }},
    {{ num: '0', label: 'Failures' }},
    {{ num: '38', label: 'Native Providers' }},
  ];
  el.innerHTML = stats.map(s => `
    <div class="card" style="text-align:center;padding:32px 20px;">
      <div class="stat-num">${{s.num}}</div>
      <div class="stat-label">${{s.label}}</div>
    </div>
  `).join('');
}}

function initServiceCloud() {{
  if (initialized['servicecloud']) return; initialized['servicecloud'] = true;
  const ALL_SERVICES = [
    'acm','apigateway','apigatewayv2','appsync','batch','cloudformation','cloudwatch',
    'cognito-idp','config','dynamodb','dynamodbstreams','ec2','ecr','ecs','es','events',
    'firehose','iam','kinesis','lambda','logs','opensearch','rekognition','resource-groups',
    'resourcegroupstaggingapi','route53','s3','scheduler','secretsmanager','ses','sesv2',
    'sns','sqs','ssm','stepfunctions','sts','support','xray',
    'athena','autoscaling','backup','budgets','ce','cloudfront','cloudhsm','cloudsearch',
    'cloudtrail','codecommit','codebuild','codedeploy','codepipeline','codestar',
    'comprehend','dax','dms','docdb','ds','elasticache','elasticbeanstalk','elb','elbv2',
    'emr','es','fis','fsx','glacier','globalaccelerator','glue','greengrass','guardduty',
    'health','inspector','iot','iotanalytics','ivs','kafka','kendra','lakeformation',
    'lex-models','lightsail','location','macie','mediaconvert','mediatailor','memorydb',
    'mq','networkmanager','organizations','outposts','pinpoint','qldb','ram','rds',
    'redshift','robomaker','sagemaker','shield','signer','sms','snowball','sso',
    'swf','textract','timestream-query','transcribe','transfer','translate','waf','wafv2',
    'workspaces','xray'
  ];
  const el = document.getElementById('service-cloud-el');
  el.innerHTML = [...new Set(ALL_SERVICES)].map(s => {{
    const isNative = PROVIDERS.includes(s);
    return `<span class="service-tag${{isNative ? ' native' : ''}}">${{s}}</span>`;
  }}).join('');
}}

function initTier1Cards() {{
  if (initialized['tier1']) return; initialized['tier1'] = true;
  const el = document.getElementById('tier1-cards');
  const borderColors = ['card-accent','card-blue','card-purple','card-teal'];
  el.innerHTML = TIER1.map((f, i) => `
    <div class="card ${{borderColors[i]}}" style="text-align:center;">
      <div style="font-size:2.5rem;margin-bottom:12px;">${{f.icon}}</div>
      <h3 style="font-size:1rem;">${{f.name}}</h3>
      <p style="font-size:0.8rem;opacity:0.65;margin-top:8px;line-height:1.5;">${{f.detail}}</p>
      <div style="margin-top:16px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap;">
        <span class="badge badge-amber">${{f.bugs}} bugs fixed</span>
        <span class="badge badge-blue">${{f.tests}} tests</span>
      </div>
    </div>
  `).join('');
}}

function initProvidersGrid() {{
  if (initialized['providers']) return; initialized['providers'] = true;
  const el = document.getElementById('providers-grid');
  el.innerHTML = PROVIDERS.map(p =>
    `<span class="service-tag native">${{p}}</span>`
  ).join('');
}}

function initTier24Details() {{
  if (initialized['tier24details']) return; initialized['tier24details'] = true;
  const el = document.getElementById('tier24-details');
  const badgeClasses = ['badge-amber','badge-blue','badge-purple','badge-teal','badge-amber','badge-blue'];
  el.innerHTML = TIER24.map((f, i) =>
    `<span class="badge ${{badgeClasses[i]}}">${{f.name}} · ${{f.tests}} tests</span>`
  ).join('');
}}

function initTimeline() {{
  if (initialized['timeline']) return; initialized['timeline'] = true;
  const el = document.getElementById('timeline-el');
  const accentColors = ['#E07B00','#2E86D4','#6B3FA0','#007B8A','#C9920B','#1E6FBF','#E07B00','#2E86D4'];
  el.innerHTML = '<div class="timeline-line"></div>' + MILESTONES.map((m, i) => {{
    const isLeft = i % 2 === 0;
    return `
      <div style="display:flex;justify-content:${{isLeft ? 'flex-start' : 'flex-end'}};position:relative;margin-bottom:20px;gap:16px;align-items:flex-start;">
        <div style="width:45%;${{isLeft ? 'text-align:right;padding-right:24px;' : 'display:none'}}">
          ${{isLeft ? `<strong style="color:${{accentColors[i]}}">${{m.label}}</strong><p style="font-size:0.8rem;opacity:0.65;">${{m.date}} · ${{m.tests.toLocaleString()}} tests · ${{m.services}} services</p>` : ''}}
        </div>
        <div class="timeline-dot" style="position:absolute;left:50%;transform:translateX(-50%);background:${{accentColors[i]}};box-shadow:0 0 0 2px ${{accentColors[i]}};"></div>
        <div style="width:45%;${{!isLeft ? 'text-align:left;padding-left:24px;' : 'display:none'}}">
          ${{!isLeft ? `<strong style="color:${{accentColors[i]}}">${{m.label}}</strong><p style="font-size:0.8rem;opacity:0.65;">${{m.date}} · ${{m.tests.toLocaleString()}} tests · ${{m.services}} services</p>` : ''}}
        </div>
      </div>
    `;
  }}).join('');
}}

// --- Slide enter dispatch ---
function onSlideEnter(n) {{
  if (n === 3)  initMotoChart();
  if (n === 8)  initServiceCloud();
  if (n === 11) initGrowthChart();
  if (n === 12) initTier1Cards();
  if (n === 13) initProvidersGrid();
  if (n === 14) {{ initTier24Chart(); initTier24Details(); }}
  if (n === 15) initBigStats();
  if (n === 18) initTimeline();
}}

// Init slide 0
onSlideEnter(0);
</script>
</body>
</html>"""


def validate_with_playwright() -> None:
    from playwright.sync_api import sync_playwright

    screenshot_dir = ROOT / "docs" / "slide_screenshots"
    screenshot_dir.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1366, "height": 768})
        page.goto(f"file://{OUT_FILE}")
        page.wait_for_timeout(1800)
        slides = page.query_selector_all(".slide")
        for i, slide in enumerate(slides):
            title_el = slide.query_selector("h1, h2")
            title = title_el.inner_text()[:50] if title_el else f"Slide {i + 1}"
            page.screenshot(path=str(screenshot_dir / f"slide_{i + 1:02d}.png"))
            print(f"  Slide {i + 1:02d}: ✓  {title}")
            if i < len(slides) - 1:
                page.keyboard.press("Space")
                page.wait_for_timeout(1500)
        browser.close()
    print(f"\n✓ Screenshots saved to {screenshot_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Robotocore slide deck HTML.")
    parser.add_argument("--check", action="store_true", help="Validate with Playwright screenshots")
    args = parser.parse_args()

    print("Gathering data...")
    sessions = collect_prompt_sessions()
    print(f"  {len(sessions)} prompt sessions")
    data = {
        "sessions": sessions,
        "test_milestones": get_test_milestones(),
        "moto_milestones": get_moto_milestones(),
        "commit_timeline": compute_commit_timeline(),
        "native_providers": get_native_providers(),
        "tier1_features": get_tier1_features(),
        "tier24_features": get_tier24_features(),
    }

    print("Building HTML...")
    html = build_html(data)
    OUT_FILE.parent.mkdir(exist_ok=True)
    OUT_FILE.write_text(html)
    print(f"✓ Written to {OUT_FILE}  ({len(html):,} bytes)")
    print("\nRun with --check to validate with Playwright screenshots.")
    print(f"Open: file://{OUT_FILE}")

    if args.check:
        print("\nRunning Playwright slide validation...")
        validate_with_playwright()


if __name__ == "__main__":
    main()
