# Worker 3 — First Rust Service: SQS (end-to-end, against golden baseline)

You are W3 of 3 parallel workers porting robotocore (Python, src/robotocore/) to Rust.
Read /Users/jackdanger/www/robotocore-rust/tooling/briefs-golden-2026-08-19.md first.

## Context
Worker W2 is building the core crate (server skeleton, protocol layer, state) IN PARALLEL in the same src/ tree. You MUST NOT edit files W2 owns: Cargo.toml, src/lib.rs, src/core/**, src/bin/**. W2 will publish its API in /Users/jackdanger/www/robotocore-rust/tooling/report-w2.md when done.

## Your job: build `crates/sqs/` — a standalone SQS service crate
Create a NEW cargo workspace member (add to repo root Cargo.toml [workspace] members if W2 hasn't created one yet — check first; if the root Cargo.toml has no [workspace], create `crates/` with its own Cargo.toml and DO NOT touch the root crate; instead make `crates/sqs` a standalone crate with path deps to nothing — implement your own minimal request/response types mirroring what W2 will expose, and expose a clean trait + integration point documented in your report).

Wait — coordinate this way: if `tooling/report-w2.md` does not exist yet when you finish your design, write your code against a minimal stable interface you define in `crates/sqs/src/protocol.rs`:
```rust
pub struct AwsRequest { pub service: String, pub operation: String,
  pub account: u64, pub region: String,
  pub params: std::collections::HashMap<String, serde_json::Value>,
  pub body: bytes::Bytes }
pub struct AwsResponse { pub status: u16, pub headers: Vec<(String,String)>, pub body: String }
pub trait SqsHandler: Send + Sync { fn handle(&self, req: AwsRequest) -> AwsResponse }
```
Keep this interface in the crate root so W2 can plug it in with <= 30 lines of glue. Document the glue in your report.

## SQS implementation (read src/robotocore/services/sqs/*.py AND vendor/moto/moto/sqs for exact behavior):
State (in-memory, per account+region, `Arc<RwLock<...>>`):
- queues: name -> Queue { arn, url, created, attributes, messages: VecDeque<Message> }
- Message: body, receipt_handle, md5, sent_ts, visible_until, receipt_count
Operations (query protocol — XML envelope `<SendMessageResponse><SendMessageResult><MessageId/><MD5OfBody/></SendMessageResult></SendMessageResponse>`; match Python EXACTLY, check /tmp/golden/baseline.json):
- CreateQueue (name validation: alnum/_-/. only, 80 char max; QueueName vs QueueUrl semantics)
- GetQueueUrl, GetQueueAttributes (All + named), DeleteQueue
- SendMessage (MessageId uuid, MD5 of body, return MD5OfBody; DelaySeconds attr)
- ReceiveMessage (MaxNumberOfMessages, VisibilityTimeout default 30, ReceiptHandle, MD5OfBody, remove on receipt if ReceiveMessage attribute; empty -> just `<ReceiveMessageResult/>`)
- DeleteMessage (requires valid receipt handle; unknown handle -> `ReceiptHandleIsInvalid`)
- ListQueues (max 100, QueueUrl list)
- PureSQS errors: `AWS.SimpleQueueService.NonExistentQueue`, `AWS.SimpleQueueService.NotFound`, `InvalidAttributeName` — in query-protocol XML error format (check moto's response format)

## Tests (must pass, no network):
1. Unit tests for each op incl. error cases
2. **Golden replay test** (in `crates/sqs/tests/golden.rs`): read /tmp/golden/baseline.json, extract the 3 sqs ops (send_message, receive_message + the implicit create_queue before them — replay in order), drive them through your handler, compare status + response body structure (XML-normalized) against the golden. Mark as `#[ignore = "needs golden baseline"]`? NO — if /tmp/golden/baseline.json exists, run it; if not, #[ignore].
3. `cargo test -p sqs` + clippy + fmt clean.

Report: /Users/jackdanger/www/robotocore-rust/tooling/report-w3.md — what works, the glue interface, ops done, ops stubbed, exact golden-replay results.
