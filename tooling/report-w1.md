# W1 Report: SQS Service Integration

## Changes

### 1. Root `Cargo.toml`
- Added `sqs = { path = "crates/sqs" }` to `[dependencies]`.

### 2. `src/core/protocol.rs`
- Added `raw: Option<String>` field to `ParsedResponse`.
- Added `ParsedResponse::raw(status, headers, body)` constructor for pre-serialized bodies.
- Updated existing constructors (`json_success`, `error`) to set `raw: None`.
- Fixed clippy: redundant closure in `value_to_xml_content`, unused params in `extract_operation`, `split.last()` → `rsplit.next()`.

### 3. `src/core/server.rs`
- **`extract_service()`**: Now builds a `crate::router::AwsRequest` from `(method, uri, headers)` and calls `crate::router::route_to_service(...)`. Falls back to the legacy X-Amz-Target / path heuristics only when the router returns `None`.
- **`ServiceRegistry::new()`**: Registers the SQS handler under `"sqs"` using a new `SqsServiceHandler` adapter that converts `ParsedRequest` → `sqs::protocol::AwsRequest` and `sqs::protocol::AwsResponse` → `ParsedResponse` (with `raw` body).
- **`response_from_parsed()`**: Uses `resp.raw` when present (bypasses XML/JSON encoding), keeping STS query-XML behavior intact.
- Removed unused imports; simplified `parse_query_protocol` match to `unwrap_or_default()`.

### 4. `src/core/services/sts.rs`
- Added `raw: None` to both `ParsedResponse` constructors.
- Removed unused `uuid::Uuid` import; moved `serde_json::Value` import into the test module.

### 5. `crates/sqs/src/models.rs`
- Added `last_modified: u64` field to `Queue` (set on creation and mutation).
- Added `Queue::change_message_visibility(receipt_handle, timeout_seconds) -> bool`.
- Added `Queue::purge()`.

### 6. `crates/sqs/src/handler.rs`
- **New operations**: `SetQueueAttributes`, `ChangeMessageVisibility`, `PurgeQueue`.
- **`CreateQueue`**: Now applies `Attributes` map (VisibilityTimeout, DelaySeconds, ReceiveMessageWaitTimeSeconds, MaximumMessageSize, MessageRetentionPeriod, Policy).
- **`ListQueues`**: Added `QueueNamePrefix` filter.
- **`GetQueueAttributes`**: Added `LastModifiedTimestamp`, `ApproximateNumberOfMessagesNotVisible`, `ApproximateNumberOfMessagesDelayed`.
- Fixed clippy: redundant closure in `delete_message`, `map_or` → `is_none_or` in list filter.

### 7. `crates/sqs/src/tests.rs`
- Added 9 new tests:
  - `test_create_queue_with_attributes`
  - `test_set_queue_attributes` (happy path)
  - `test_set_queue_attributes_nonexistent_queue` (error)
  - `test_change_message_visibility` (happy path)
  - `test_change_message_visibility_invalid_handle` (error)
  - `test_purge_queue` (happy path)
  - `test_purge_queue_nonexistent` (error)
  - `test_list_queues_prefix_filter`
  - `test_receive_message_max_count_validation`

## Test Results
- `cargo test`: **130 passed** (root crate, unchanged)
- `cargo test -p sqs`: **22 passed** (13 pre-existing + 9 new)
- **Total: 152 tests, 0 failures**

## Clippy
- `cargo clippy -- -D warnings` clean on all touched files.
- Pre-existing warnings in other files (signing.rs, spec.rs, account.rs) are untouched.

## Known Gaps
- SQS `receive_messages` in `models.rs` puts messages back in the queue after reading (they stay visible if `visibility_timeout` expires). This matches the Python reference behavior for in-memory SQS.
- No FIFO queue support (`.fifo` suffix, MessageGroupId, DeduplicationId).
- No batch operations (SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch).
- No DLQ (Dead Letter Queue) redrive logic.
- No message retention period enforcement (expired messages are not pruned).
- No WaitTimeSeconds (long polling) — always non-blocking.
- `ApproximateNumberOfMessagesDelayed` is always 0 (no delayed-message queue).
- The `extract_service` function in `server.rs` does not use the request body for service detection (the Python router does body-based Action detection for unsigned requests). The router handles this via query params and host headers, which covers the common cases.
