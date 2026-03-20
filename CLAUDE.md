# Robotocore

An MIT-licensed, open-source AWS emulator built on top of Moto. Runs as a single Docker container on ARM Mac.

## Project Philosophy

- **Free forever**: MIT license, no registration, no telemetry, no paid tiers
- **Drop-in replacement**: Same port (4566), same request routing, same response format as other AWS emulators
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
│   └── compatibility/         # Tests that verify AWS parity
├── scripts/
│   ├── dev.py                 # Dev server lifecycle & test runner
│   ├── smoke_test.py          # Cross-service smoke test
│   ├── probe_service.py       # Discover working operations per service
│   └── ...                    # gen_provider, gen_compat_tests, batch_register, etc.
├── vendor/
│   ├── moto/                  # Git submodule: getmoto/moto
│   └── localstack/            # Git submodule (reference implementation)
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
# Unit tests (parallel, no server needed)
make test                              # or: uv run pytest tests/unit/ -n12

# Compat tests (auto-starts/stops server)
make compat-test                       # or: uv run python scripts/dev.py test-compat

# Compat tests (server already running)
make compat-test-hot

# All tests: unit + compat + integration
make test-all

# Server lifecycle
make start                             # Start dev server in background
make stop                              # Stop dev server
make status                            # Check if server is running
```

## Key Technical Decisions

1. **Gateway on port 4566**: Standard AWS emulator port so existing `aws --endpoint-url` configs work unchanged
2. **Moto as the service layer**: Don't reimplement what Moto already does well. Wrap it, extend it, fix its gaps.
3. **Protocol handling via botocore specs**: Use botocore's own service JSON specs to parse/serialize requests
4. **In-memory state by default**: No persistence layer needed for dev/test use. Optional persistence can come later.
5. **Plugin system**: `RobotocorePlugin` base class with entry point, env var, and directory discovery.
6. **ASGI with uvicorn**: Modern async-capable HTTP server

## Coding Conventions

- Python 3.12+, type hints everywhere
- `ruff` for linting and formatting
- `pytest` for all tests
- No classes where functions suffice
- Imports: stdlib → third-party → local, separated by blank lines
- Service provider methods match AWS API operation names exactly (PascalCase)
- Test files mirror source structure: `src/robotocore/gateway/router.py` → `tests/unit/gateway/test_router.py`
- **Never write `except ...: pass`**. Always either (a) log with `logger.debug("context: %s", exc)`, (b) add an explanatory comment, or (c) handle meaningfully. For best-effort cleanup, use `logging.debug`. CodeQL flags empty except blocks and CI will fail.
- In tests, use `pytest.raises(SomeException)` instead of try/except/pass to assert expected errors.

## Reference Materials

- **vendor/moto/**: Local checkout of `github.com/JackDanger/moto` (`master` branch) for agent/dev convenience. **Not used at install time** — `uv sync` clones from the git remote. Key files:
  - `moto/core/botocore_stubber.py` — Request interception architecture
  - `moto/backends.py` — Backend registry
  - `moto/{service}/models.py` — Service implementations
  - `moto/{service}/responses.py` — Request/response handling
  - `moto/{service}/urls.py` — URL patterns


## Service Coverage (157 services registered)

157 AWS services are implemented. 46 have native providers with enhanced fidelity; 111 are Moto-backed.

**Native providers** (46): acm, apigateway, apigatewayv2, appsync, batch, cloudformation, cloudwatch, cognito-idp, config, dynamodb, dynamodbstreams, ec2, ecr, ecs, es, events, firehose, iam, kinesis, lambda, logs, opensearch, pipes, rekognition, resource-groups, resourcegroupstaggingapi, route53, s3, scheduler, secretsmanager, ses, sesv2, sns, sqs, ssm, stepfunctions, sts, support, xray

**Test coverage**: 20,000+ tests (8,570 unit + 11,900+ compat + 58 integration), 0 failures.

## Adding a New Moto-Backed Service (Checklist)

1. Verify Moto has the backend: `ls vendor/moto/moto/{service}/`
2. Find protocol: check botocore service-2.json metadata
3. Add to registry.py: `ServiceInfo` with `MOTO_BACKED` status
4. Add routing in router.py (`TARGET_PREFIX_MAP` / `PATH_PATTERNS` / `SERVICE_NAME_ALIASES`)
5. Run smoke_test.py to verify basic operation
6. Probe with `probe_service.py` to find working operations
7. Add compat tests for verified operations only

Or use the batch tool: `uv run python scripts/batch_register_services.py --service <name> --write`

## Infrastructure Features

- **Chaos Engineering**: `POST /_robotocore/chaos/rules` to inject faults (ThrottlingException, latency, etc.)
- **Resource Browser**: `GET /_robotocore/resources` for cross-service resource overview
- **Audit Log**: `GET /_robotocore/audit` for recent API call history (ring buffer, configurable via `AUDIT_LOG_SIZE`)
- **State Snapshots**: `POST /_robotocore/state/save {"name":"my-snap"}` / `POST /_robotocore/state/load {"name":"my-snap"}`
- **Selective Persistence**: `POST /_robotocore/state/save {"services":["s3","dynamodb"]}`

## Working with Agents

When using Claude Code agents on this project:
- Always read vendor source before implementing a feature — understand how Moto handles it
- Run tests after every change
- Keep compatibility tests as the north star — if it works the same as AWS, it's correct
- Prefer extending Moto over reimplementing from scratch

### Parallel work with git worktrees
- Use `isolation: "worktree"` when multiple agents need to edit files simultaneously — each gets its own repo copy, no conflicts
- Do NOT use worktrees for read-only research (Explore agents) — wasteful
- Fan out by file/module: give each agent a non-overlapping set of files
- After agents complete, review their worktree branches and merge/cherry-pick results

### Work sequencing
- **Do 100% of the work** — don't prioritize by impact. Everything gets done.
- **Sequence by dependencies** — do foundational pieces first that make later work easier. Build the tool before using it. Build the test harness before writing tests. Register the service before probing it.
- **Don't stop to ask** unless truly blocked. If the plan says to do steps 1-12, do all 12.

### Build CLI tools first
- Before doing the same thing to 5+ files, write a script in `scripts/` that automates it
- Tools should have `--dry-run` (default), `--write` (apply), and `--file` (target specific files) flags
- Run `uv run python scripts/<tool>.py` to analyze, then spawn agents to act on the results
- Existing tools: `gen_provider.py`, `gen_compat_tests.py`, `gen_unit_tests.py`, `gen_cfn_resource.py`, `gen_eventbridge_targets.py`, `gen_gap_tests.py`, `coverage_gaps.py`, `compat_coverage.py`, `analyze_localstack.py`, `batch_register_services.py`, `check_wire_format.py`, `probe_service.py`, `smoke_test.py`, `generate_parity_report.py`, `service_health_matrix.py`, `dev.py`, `validate_test_quality.py`, `validate_tests_runtime.py`, `lint_project.py`

### Subagent patterns
- **Research first**: Use Explore agents (parallel, no worktree) to understand the problem, then code agents to implement
- **Build tool → fan out**: Create a script, then spawn parallel agents each running it on different targets
- **Verify always**: Every code agent prompt must include "run tests after changes"
- **Be specific**: Tell agents exactly which files to edit, what to change, and what NOT to change

### Fixing gaps in Moto
When we discover a Moto bug or missing feature:
1. Create a feature branch in `vendor/moto/`: `cd vendor/moto && git checkout -b fix/<name>`
2. Implement the fix directly in Moto's source
3. Write a test for it in Moto's test suite
4. Merge the branch into `master`: `git checkout master && git merge fix/<name>`
5. **Push to the fork**: `git push jackdanger master` — this is the source of truth
6. **Update the lockfile in robotocore**: `cd ../.. && uv lock` to pick up the new commit
7. Do NOT open PRs to `getmoto/moto` yet — we'll batch upstream contributions later in a structured push
8. For gaps that CANNOT be fixed in Moto (e.g., behavioral fidelity that conflicts with Moto's design), implement a native provider in `src/robotocore/services/<service>/provider.py` instead

**How Moto is installed**: `pyproject.toml` has `moto = { git = "https://github.com/JackDanger/moto.git", branch = "master" }`. The `vendor/moto/` checkout is only for dev/agent convenience (reading source, making fixes). Docker and CI install from the git remote. After making Moto fixes locally, always push to the fork and run `uv lock` so the lockfile pins the new commit.

### Commit cadence (CRITICAL — do this proactively)
- **Commit after every logical phase** — don't accumulate a massive diff. After writing tests: commit. After fixing lint: commit. After changing CI: commit. Each commit should be self-contained and green.
- **Maximum ~200 lines between commits.** If you've written 200+ lines of new code, stop and commit before continuing.
- **Run tests before committing.** `uv run pytest <changed files> -q --tb=short` to verify, then `git add` + `git commit`.
- **Push after every commit.** Always `git push` immediately after committing. This keeps the remote up to date, triggers CI, and publishes a new Docker image automatically. Every push to main is a release — CI auto-tags with CalVer (`vYYYY.M.D`) and publishes versioned + `:latest` Docker images to Docker Hub and GHCR.
- **Never stop to summarize** — if the plan has more steps, keep executing. A summary is only appropriate when the plan is fully complete.
- **Prompt log**: Every commit includes a prompt log entry in `prompts/`. Follow the format in `prompts/PROMPTLOG.md` (the spec lives in this repo). One file per session phase, named `prompts/{timestamp}-{slug}.md` with YAML frontmatter. Include both human prompts and assistant reasoning for non-obvious decisions.

### Test expansion rules (IMPORTANT — learned from experience)
- **Never write tests for unverified operations**. Before writing compat tests for a service, verify what actually works by running the operations against the live server. Use `scripts/probe_service.py` as a starting point.
- **No speculative xfails**. If an operation doesn't work, fix the server first, then write the test. An xfail is a TODO you'll forget about.
- **Agent prompts for test writing MUST include**: "Run each test against the running server (port 4566) before including it. If it fails because the operation isn't implemented, do NOT include that test — skip it entirely."
- **Fix-then-test > test-then-xfail**. The correct order is: (1) discover gap, (2) implement fix, (3) write test that proves fix works. Never: (1) write test, (2) discover it fails, (3) mark xfail.
- **Probe script**: `uv run python scripts/probe_service.py --service <name>` reports which operations work. Give agents this output as their allowlist.
- **Every test MUST contact the server.** A test that catches `ParamValidationError` and passes is worthless — boto3 validates params client-side before the request is sent. The server is never contacted, so the test proves nothing about the server. For ops requiring params, either provide valid params or don't write the test.
- **Every test MUST assert something.** A test that calls an operation and doesn't check the response is barely better than a smoke test. At minimum assert on a response key.
- **Run `make test-quality` before committing tests.** The `validate_test_quality.py` script catches tests that never contact the server. CI enforces <5% no-contact rate.
- **Test count is not a metric.** Never report test count as progress without also reporting effective test rate from `validate_test_quality.py`. 100 tests that verify behavior > 10,000 tests that catch exceptions and pass.

### Headless overnight workflow (chunk-based with feedback)

The overnight script (`scripts/overnight.sh`) uses a tight feedback loop:

**Architecture**: service → probe → chunk by resource noun → per-chunk Claude session → verify → commit

**Chunking**: `scripts/chunk_service.py` breaks any service (even EC2 with 756 ops) into resource-group chunks of 3-8 operations each (e.g., "Vpc", "SecurityGroup", "Instance"). Each chunk is a single Claude session.

**Feedback loop** (runs after every chunk):
1. Each test is run immediately after being written (pass → keep, fail → fix or delete)
2. Quality gate after chunk: `validate_test_quality.py` (no junk tests)
3. Coverage delta after service: did `compat_coverage.py` numbers actually go up?
4. 3 consecutive failed chunks → move to next service (don't waste time)
5. Every 5 services: overall progress check

**Tools in the pipeline**:
- `probe_service.py --all --json` — auto-fills params from botocore, classifies ops as working/not_implemented/500_error/needs_params
- `chunk_service.py --with-probe --untested-only` — groups untested-but-working ops by resource noun
- `compat_coverage.py --service X --json` — before/after coverage comparison
- `validate_test_quality.py --file X` — ensures tests actually contact server

**Three test patterns that work**:
1. **Create → use → cleanup** (for CRUD ops needing a resource)
2. **Call with fake ID → assert ResourceNotFoundException** (proves implementation without setup)
3. **Call list/describe with no args → assert response key** (for list operations)

**Critical rules for headless mode:**
- NEVER catch ParamValidationError — that's client-side, proves nothing about the server
- Every test MUST assert on a response field
- Run each test RIGHT AFTER writing it — don't batch
- If stuck on params for >2 minutes, skip the operation
- If an operation returns 501, DELETE the test and move on
- The goal is reliable coverage, not speed

### Reviewing pull requests

Use `/review-pr <number>` to review and merge external PRs. Key principles:

- **The submitter's PR is their contribution.** Merge it in their shape. Fix only what blocks CI. Save improvements for a follow-up PR.
- **Never cherry-pick to another branch.** Merge the PR directly so the author gets git credit.
- **Welcome first-time contributors** with a warm comment.
- **Gate on prompt log.** If the PR touches `src/`, `tests/`, or `scripts/` but has no `prompts/` entry, request changes — don't approve.
- **Fix CI blockers yourself** by pushing fixup commits to the PR branch (bare except blocks, missing imports, lint).
- **Approve and auto-merge** once CI is green: `gh pr review --approve` + `gh pr merge --merge --auto`.
- **Give credit.** Every approval comment thanks the author by name
