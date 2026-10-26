# Robotocore

An MIT-licensed, open-source AWS emulator, implemented entirely in Rust. Runs as a single Docker container on ARM and x86.

## Project Philosophy

- **Free forever**: MIT license, no registration, no telemetry, no paid tiers
- **Drop-in replacement**: Same port (4566), same request routing, same response format as other AWS emulators
- **Pure Rust, zero Python in the product**: the old Python server and the vendored Moto backend are gone; `rotocore/` reimplements Moto's service layer in Rust
- **Behavioral fidelity where it matters**: Lambda actually executes, SQS has real visibility timeouts, etc.
- **Single container**: One `docker run` command to get all of AWS locally

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                 Rust server (port 4566)              │
│                                                      │
│  ┌───────────────────────────────────────────────┐   │
│  │  Request Router (src/router.rs)               │   │
│  │  service detection from headers, target,      │   │
│  │  URL patterns, sign scopes, host names        │   │
│  └────────────────────┬──────────────────────────┘   │
│                       │                              │
│  ┌────────────────────▼──────────────────────────┐   │
│  │  Gateway (src/core/) — axum + protocol layer  │   │
│  │  query / json / rest-json / rest-xml / ec2    │   │
│  └────────┬──────────────────────────┬───────────┘   │
│           │                          │               │
│  ┌────────▼───────────────┐  ┌───────▼───────────────┐
│  │  Core service handlers │  │  rotocore crate       │
│  │  (src/core/services/,  │  │  (rotocore/src/,      │
│  │   crates/*)            │  │   ported far service  │
│  │                        │  │   backends)           │
│  └────────┬───────────────┘  └───────┬───────────────┘
│           │                          │               │
│  ┌────────▼──────────────────────────▼───────────────┐
│  │  In-Memory Stores (per-account, per-region)       │
│  └───────────────────────────────────────────────────┘
└──────────────────────────────────────────────────────┘
```

Requests for a service with no backend get a generic `501 NotImplemented`
(that is the only shape that means "absent"; anything else is a wire bug).

## Project Layout

```
robotocore/
├── Cargo.toml                 # workspace: robotocore-rust, rotocore, crates/*
├── Dockerfile                 # pure-Rust single-container build
├── Makefile                   # dev entrypoints (make start / compat-test / parity)
├── src/
│   ├── bin/robotocore-rust.rs # server binary (clap flags, tracing)
│   ├── router.rs, s3_routing.rs, cors.rs   # routing + S3 vhost detection
│   └── core/                  # gateway core
│       ├── server.rs          # axum router, catch-all, admin endpoints
│       ├── protocol.rs        # wire protocol parsing/serialization
│       ├── signing.rs         # SigV4 validation
│       ├── service_registry.rs# service table (name, protocol, description)
│       ├── introspection.rs   # audit log, usage errors, counters
│       ├── state.rs           # in-memory state + save/load
│       └── services/          # native core handlers (sts, s3, sqs, ...)
├── rotocore/                  # "moto in Rust" — far-service backends (141 services)
├── crates/                    # per-service crates (sqs, s3, dynamodb, lambda, ...)
├── bindings/                  # language bindings: python/, typescript/, go/, rust/ + tools/
├── tests/
│   ├── apps/                  # application-level smoke flows
│   ├── compatibility/         # contract suites (boto3, the fidelity gold standard)
│   ├── iac/                   # terraform/cdk/pulumi wrapper tests
│   └── parity/                # parity-harness support files
├── scripts/                   # dev tooling (pytest rig): parity.py, dev.py, smoke_test.py ...
├── probes/                    # per-service probe results (operation -> status)
├── .parity/                   # golden parity baseline (expectations_current.json)
└── tooling/                   # ground-truth capture tools
```

## Development

### Prerequisites

- Rust toolchain (rustup; any recent stable)
- Docker (with ARM support)
- uv (for the python test rig — pytest/boto3 only, no runtime python)

### Running locally (development)

```bash
cargo build --release -p robotocore-rust
make start                # or run target/release/robotocore-rust --port 4566
make status               # health check
```

### Running in Docker

```bash
docker build -t robotocore .
docker run -p 4566:4566 robotocore
```

### Testing

```bash
make rust-test            # all Rust unit/crate tests
make compat-test          # compatibility suites (auto-starts the server)
make compat-test-hot      # same, server already running
make parity               # rust-only parity vs .parity/expectations_current.json
cargo clippy --workspace --all-targets -- -D warnings   # CI lint gate
cargo fmt                 # formatting
```

Test tier guidance: **cheap>expensive** — run workspace lib tests after small
changes; only run the full compatibility/parity suites after meaningful
service changes.

## Service Coverage

160 AWS services are registered in the gateway table. Availability is
derived from the real tables, not a static claim — ask the twin:

```bash
curl -s http://localhost:4566/_robotocore/services | python3 -c "
import json,sys,collections
s=json.load(sys.stdin)['services']
print(dict(collections.Counter(x['status'] for x in s)))"
```

`native` = a Rust backend answers in-process (gateway core + rotocore —
currently all 160). `absent` = requests answer 501 NotImplemented.

## Adding a New Native Service (Checklist)

1. Probe the surface: read the checked-in `probes/<name>.json` (operation -> status) and ask a running twin `/_robotocore/services` for its posture
2. Write the module in `rotocore/src/services/<service>/`
3. Add the dispatch arm + `handles_service` entry in `rotocore/src/services/mod.rs`
4. Verify wire names against botocore's service-2.json
5. Add compatibility tests for verified operations only
6. Run the gates (see Testing)

See `.claude/skills/implement-service.md` for the detailed rules.

## Infrastructure Features

- **Audit Log**: `GET /_robotocore/audit` for recent API call history (in-memory ring)
- **Usage errors**: `GET /_robotocore/usage/errors`
- **State Snapshots**: `POST /_robotocore/state/save {"name":"my-snap"}` / `POST /_robotocore/state/load {"name":"my-snap"}`, plus export/import and selective per-service persistence

## Working with Agents

- Read the surrounding rust module before implementing — match its idioms
- Run tests after every change
- Keep compatibility tests as the north star — if it works the same as AWS, it's correct

### Parallel work with git worktrees
- Use worktrees when multiple agents need to edit files simultaneously — each gets its own repo copy, no conflicts
- Do NOT use worktrees for read-only research — wasteful
- Fan out by file/module: give each agent a non-overlapping set of files
- After agents complete, review their worktree branches and merge/cherry-pick results
- Prune stale worktrees (`git worktree prune`); merged `port/*` branches are merged history, not live dirs

### Work sequencing
- **Do 100% of the work** — don't prioritize by impact. Everything gets done.
- **Sequence by dependencies** — do foundational pieces first that make later work easier.
- **Don't stop to ask** unless truly blocked.

### Subagent patterns
- **Research first**: Use Explore agents (parallel, no worktree) to understand the problem, then code agents to implement
- **Verify always**: Every code agent prompt must include "run tests after changes"
- **Be specific**: Tell agents exactly which files to edit, what to change, and what NOT to change

### Fixing fidelity gaps
When behavior diverges from real AWS (or the compat suite disagrees):
1. Reproduce against the running twin, inspect `/_robotocore/audit`
2. Write the failing compat test (it is the gold standard)
3. Fix in `rotocore/` or `src/core/services/` — never on the test side
4. Re-run the suite + parity baseline; promote the baseline only when the whole lane is green

### Commit cadence (CRITICAL — do this proactively)
- Work happens on the `rust` branch (this tree); never commit directly to `main`
- **Commit after every logical phase** — don't accumulate a massive diff
- **Run the cheap gates before committing** (workspace lib tests + clippy -D warnings); run compat suites only after meaningful changes
- **Push in batches** (the maintainer's call for pacing CI — currently ~100 commits per push)

### Changelog discipline
- **Update `CHANGELOG.md` in the same branch** as any user-visible change: new feature/endpoint/env var, behavior change, breaking change, fidelity fix
- Use the standard `### Added/Changed/Fixed/Removed` subsections, newest first
- Be specific: name the endpoint, env var, service

### Test expansion rules (IMPORTANT — learned from experience)
- **Never write tests for unverified operations.** Check `probes/<name>.json` first; write tests only for operations that actually work.
- **No speculative xfails.** If an operation doesn't work, fix the server first, then write the test.
- **Every test MUST contact the server.** A test that catches client-side `ParamValidationError` and passes is worthless.
- **Every test MUST assert something.** At minimum assert on a response key.
- **Fix-then-test > test-then-xfail.**
- If an operation returns 501, DELETE the test and move on — 501 means genuinely absent.

## Reference Materials

- `docs/coverage.html` — browsable per-service coverage view
- `probes/<service>.json` — checked-in probe results (operation -> status)
- `.parity/expectations_current.json` — golden parity baseline (180 suites, promoted from the rust lane)
