---
session: "kinesis-unit-tests"
timestamp: "2026-03-12T05:22:00Z"
model: claude-opus-4-6
---

## Human

Write comprehensive unit tests for the Kinesis native provider at
`src/robotocore/services/kinesis/provider.py`. Create the test file at
`tests/unit/services/kinesis/test_kinesis_provider.py`. Test each action
function directly, covering stream CRUD, shard operations, record put/get,
consumer management, encryption, enhanced monitoring, and edge cases.

## Assistant

## Key decisions

**Test approach**: Imported each action function directly (e.g., `_create_stream`,
`_put_record`) and called them with a fresh `KinesisStore` instance rather than
going through the HTTP handler. This makes tests fast, focused, and independent
of the ASGI layer (which is already tested in the existing test file at
`tests/unit/services/test_kinesis_provider.py`).

**Existing coverage**: Found an existing test file at the flat path
`tests/unit/services/test_kinesis_provider.py` with 28 tests covering the HTTP
handler layer (async `handle_kinesis_request` calls). The new file in the
subdirectory tests provider functions directly, covering all 27 action functions
in `_ACTION_MAP` plus helper utilities.

**115 tests organized by domain**: Stream CRUD (create/delete/describe/list/summary),
record operations (put/get single and batch), shard iterator types (all 5 types
plus invalid), shard operations (split/merge/update count/list with pagination),
retention period (increase/decrease with edge cases), tags (CRUD with pagination),
encryption (start/stop), consumers (register/deregister/describe/list), enhanced
monitoring (enable/disable specific and ALL), resource policies (CRUD with consumer
ARNs), ARN parsing, iterator encoding, and store isolation by region/account.

**Pagination fix**: The `_list_shards` pagination test initially failed because
`MaxResults` must be passed on each call (it is not remembered from the token).
Fixed the test to pass `MaxResults` with each NextToken-based request.
