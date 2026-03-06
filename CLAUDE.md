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

- **vendor/localstack/**: LocalStack source code. Key files:
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
