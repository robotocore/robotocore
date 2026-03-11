---
session: "af72d904"
timestamp: "2026-03-11T05:19:05Z"
model: claude-opus-4-6
---

## Human

Implement configurable endpoint strategies for SQS and OpenSearch in robotocore,
matching LocalStack's URL format options. SQS supports standard/domain/path/dynamic
via SQS_ENDPOINT_STRATEGY env var; OpenSearch supports domain/path/port via
OPENSEARCH_ENDPOINT_STRATEGY env var. Add gateway routing for all formats, management
endpoint, and comprehensive unit tests.

## Assistant

## Key decisions

**Architecture**: Created dedicated `endpoint_strategy.py` modules in each service
directory rather than centralizing in the gateway. This keeps strategy logic close
to the service that uses it and follows the existing provider pattern.

**SQS model change**: Modified `StandardQueue.url` property to delegate to
`sqs_queue_url()` instead of hardcoding `http://localhost:4566/{account}/{name}`.
This is the minimal change needed -- all existing code that reads `queue.url` now
gets strategy-aware URLs automatically.

**OpenSearch port strategy**: Used a module-level dict with threading lock for port
allocation. The range 4510-4559 (50 ports) matches LocalStack's behavior. Added
`reset_port_allocations()` for test isolation.

**Gateway routing**: Added PATH_PATTERNS entries for `/queue/` and `/opensearch/`
path-style routes, plus Host header checks for domain-style routes in `route_to_service()`.
All formats are accepted regardless of which strategy is configured (matches LocalStack's
behavior where the strategy only affects outgoing URLs).

**Management endpoint**: Added `GET /_robotocore/endpoints/config` returning current
strategy values as JSON, following the pattern of existing management endpoints.

76 new unit tests covering strategy selection, URL generation for all formats,
URL parsing, gateway routing, and semantic integration (model returns correct URLs).
