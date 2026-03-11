---
session: "pipes-impl"
timestamp: "2026-03-10T12:00:00Z"
model: claude-opus-4-6
---

## Human

Implement EventBridge Pipes for robotocore -- the Source->Enrichment->Target
pipeline feature. Full CRUD API, background polling, SQS/Kinesis/DynamoDB
Streams sources, Lambda enrichment, six target types. Write tests first.

## Assistant

## Key decisions

**Architecture**: Followed the scheduler provider pattern -- module-level state
dicts with threading.Lock, REST-JSON path matching, async handler function.
Background polling uses daemon threads with stop events for clean lifecycle.

**Source polling**: Each RUNNING pipe gets a daemon thread that polls its source
at the configured interval (MaximumBatchingWindowInSeconds, default 1s). Sources
are detected by ARN pattern (`:sqs:`, `:kinesis:`, `:dynamodb:`+`/stream/`).

**Enrichment**: Lambda enrichment calls invoke_lambda_sync and uses the response
as the enriched payload. API Gateway enrichment POSTs to localhost:4566. If
enrichment fails, original records pass through (resilient by default).

**Targets**: Six target types (SQS, SNS, Lambda, EventBridge, Step Functions,
Kinesis) each implemented as a separate delivery function. Errors are caught
and logged, never propagated to crash the polling loop.

**Registry upgrade**: Changed pipes from MOTO_BACKED to NATIVE since Moto has
no pipes backend. Added path routing pattern for `/v1/pipes` before the batch
`/v1/*` patterns to avoid routing conflicts.

**Test strategy**: 39 unit tests across three files -- CRUD operations (14 tests),
pipeline execution with mocked backends (18 tests), and end-to-end integration
flows (7 tests). All tests use `reset_pipes_state()` for clean isolation.
