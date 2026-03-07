# Robotocore

An MIT-licensed, open-source AWS emulator built on top of Moto with 100% feature parity with LocalStack Community Edition. Runs as a single Docker container on ARM Mac.

## Project Philosophy

- **Free forever**: MIT license, no registration, no telemetry, no paid tiers
- **Drop-in LocalStack replacement**: Same port (4566), same request routing, same response format
- **Built on Moto**: Leverage Moto's ~195 service implementations as the foundation
- **Behavioral fidelity where it matters**: Lambda actually executes, SQS has real visibility timeouts, etc.
- **Single container**: One `docker run` command to get all of AWS locally

## Architecture

```
┌─────────────────────────────────────────────┐
│              Docker Container                │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │     Gateway (port 4566)                │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │  Request Router                  │  │  │
│  │  │  (service detection from headers,│  │  │
│  │  │   URL patterns, query params)    │  │  │
│  │  └──────────┬───────────────────────┘  │  │
│  │             │                          │  │
│  │  ┌──────────▼───────────────────────┐  │  │
│  │  │  Protocol Layer                  │  │  │
│  │  │  (query, json, rest-json,        │  │  │
│  │  │   rest-xml, ec2)                 │  │  │
│  │  └──────────┬───────────────────────┘  │  │
│  │             │                          │  │
│  │  ┌──────────▼───────────────────────┐  │  │
│  │  │  Service Providers               │  │  │
│  │  │  (Moto backends + extensions)    │  │  │
│  │  └──────────┬───────────────────────┘  │  │
│  │             │                          │  │
│  │  ┌──────────▼───────────────────────┐  │  │
│  │  │  In-Memory Stores                │  │  │
│  │  │  (per-account, per-region)       │  │  │
│  │  └─────────────────────────────────-┘  │  │
│  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

## Project Layout

```
robotocore/
├── CLAUDE.md                  # This file
├── pyproject.toml             # Project config (uv)
├── Dockerfile                 # Single-container build
├── docker-compose.yml         # Dev convenience
├── src/robotocore/
│   ├── __init__.py
│   ├── main.py                # Entrypoint
│   ├── gateway/               # HTTP gateway, request routing
│   │   ├── __init__.py
│   │   ├── app.py             # ASGI/WSGI app
│   │   ├── router.py          # AWS service detection & dispatch
│   │   └── handler_chain.py   # Request/response handler chain
│   ├── protocols/             # AWS protocol parsers/serializers
│   │   ├── __init__.py
│   │   ├── parser.py          # HTTP → Python objects
│   │   └── serializer.py      # Python objects → HTTP response
│   ├── providers/             # Service provider wrappers
│   │   ├── __init__.py
│   │   └── moto_bridge.py     # Bridge to forward requests to moto
│   ├── services/              # Service-specific extensions beyond moto
│   │   └── __init__.py
│   ├── stores/                # In-memory state stores
│   │   └── __init__.py
│   └── utils/                 # Shared utilities
│       └── __init__.py
├── tests/
│   ├── unit/                  # Fast, no-network tests
│   ├── integration/           # Tests against running container
│   └── compatibility/         # Tests that verify LocalStack parity
├── scripts/
│   ├── discover_services.py   # Enumerate LocalStack community services
│   ├── generate_coverage.py   # Compare robotocore vs localstack coverage
│   └── run_aws_tests.py       # Exercise AWS APIs against robotocore
├── vendor/
│   ├── moto/                  # Git submodule: getmoto/moto
│   └── localstack/            # Git submodule: localstack/localstack
└── docker/
    └── entrypoint.sh          # Container entrypoint
```

## Development

### Prerequisites

- Python 3.12+
- uv (Python package manager)
- Docker (with ARM/aarch64 support)

### Setup

```bash
git submodule update --init --recursive
uv sync
```

### Running locally (development)

```bash
uv run python -m robotocore.main
```

### Running in Docker

```bash
docker build -t robotocore .
docker run -p 4566:4566 robotocore
```

### Testing

```bash
# Unit tests
uv run pytest tests/unit/

# Integration tests (requires running container)
uv run pytest tests/integration/

# Compatibility tests (verifies LocalStack parity)
uv run pytest tests/compatibility/

# Full test suite
uv run pytest
```

## Key Technical Decisions

1. **Gateway on port 4566**: Matches LocalStack's default so existing `aws --endpoint-url` configs work unchanged
2. **Moto as the service layer**: Don't reimplement what Moto already does well. Wrap it, extend it, fix its gaps.
3. **Protocol handling via botocore specs**: Use botocore's own service JSON specs to parse/serialize requests, same as LocalStack's ASF does
4. **In-memory state by default**: No persistence layer needed for dev/test use. Optional persistence can come later.
5. **No plugin system initially**: Direct imports, not lazy loading. Simplicity over startup optimization.
6. **ASGI with uvicorn**: Modern async-capable HTTP server

## Coding Conventions

- Python 3.12+, type hints everywhere
- `ruff` for linting and formatting
- `pytest` for all tests
- No classes where functions suffice
- Imports: stdlib → third-party → local, separated by blank lines
- Service provider methods match AWS API operation names exactly (PascalCase)
- Test files mirror source structure: `src/robotocore/gateway/router.py` → `tests/unit/gateway/test_router.py`

## Reference Materials

- **vendor/moto/**: Moto source code. Key files:
  - `moto/core/botocore_stubber.py` — Request interception architecture
  - `moto/backends.py` — Backend registry
  - `moto/{service}/models.py` — Service implementations
  - `moto/{service}/responses.py` — Request/response handling
  - `moto/{service}/urls.py` — URL patterns

  - `localstack-core/localstack/aws/gateway.py` — Gateway implementation
  - `localstack-core/localstack/aws/protocol/` — Protocol parsers/serializers
  - `localstack-core/localstack/aws/handlers/` — Handler chain
  - `localstack-core/localstack/services/` — Service providers

## Target Services (LocalStack Community Parity)

Phase 1 - Core (most used):
- S3, S3 Control
- SQS, SNS
- DynamoDB, DynamoDB Streams
- Lambda
- IAM, STS
- CloudFormation
- CloudWatch (Logs + Metrics)

Phase 2 - Integration:
- API Gateway (REST v1)
- Step Functions
- EventBridge, EventBridge Scheduler
- Kinesis Streams, Kinesis Firehose
- KMS, Secrets Manager, SSM Parameter Store

Phase 3 - Remaining:
- EC2
- SES
- Route 53, Route 53 Resolver
- SWF
- ACM
- Config
- Resource Groups
- Elasticsearch, OpenSearch
- Redshift
- Transcribe
- Support API

## Working with Agents

When using Claude Code agents on this project:
- Always read vendor source before implementing a feature — understand how both Moto and LocalStack handle it
- Run tests after every change
- Keep compatibility tests as the north star — if it works the same as LocalStack, it's correct
- Prefer extending Moto over reimplementing from scratch

### Parallel work with git worktrees
- Use `isolation: "worktree"` when multiple agents need to edit files simultaneously — each gets its own repo copy, no conflicts
- Do NOT use worktrees for read-only research (Explore agents) — wasteful
- Fan out by file/module: give each agent a non-overlapping set of files
- After agents complete, review their worktree branches and merge/cherry-pick results

### Build CLI tools first
- Before doing the same thing to 5+ files, write a script in `scripts/` that automates it
- Tools should have `--dry-run` (default), `--write` (apply), and `--file` (target specific files) flags
- Run `uv run python scripts/<tool>.py` to analyze, then spawn agents to act on the results
- Existing tools: `gen_provider.py`, `gen_compat_tests.py`, `gen_unit_tests.py`, `coverage_gaps.py`, `analyze_localstack.py`

### Subagent patterns
- **Research first**: Use Explore agents (parallel, no worktree) to understand the problem, then code agents to implement
- **Build tool → fan out**: Create a script, then spawn parallel agents each running it on different targets
- **Verify always**: Every code agent prompt must include "run tests after changes"
- **Be specific**: Tell agents exactly which files to edit, what to change, and what NOT to change

### Test expansion rules (IMPORTANT — learned from experience)
- **Never write tests for unverified operations**. Before writing compat tests for a service, verify what actually works by running the operations against the live server. Use `scripts/probe_service.py` as a starting point.
- **No speculative xfails**. If an operation doesn't work, fix the server first, then write the test. An xfail is a TODO you'll forget about.
- **Agent prompts for test writing MUST include**: "Run each test against the running server (port 4566) before including it. If it fails because the operation isn't implemented, do NOT include that test — skip it entirely."
- **Fix-then-test > test-then-xfail**. The correct order is: (1) discover gap, (2) implement fix, (3) write test that proves fix works. Never: (1) write test, (2) discover it fails, (3) mark xfail.
- **Probe script**: `uv run python scripts/probe_service.py --service <name>` reports which operations work. Give agents this output as their allowlist.
