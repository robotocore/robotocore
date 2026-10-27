# Worker 2 — Core Rust Crate (server skeleton + wire protocol + state)

You are W2 of 3 parallel workers porting robotocore (Python, src/robotocore/) to Rust.
Read /Users/jackdanger/www/robotocore-rust/tooling/briefs-golden-2026-08-19.md first.

## Your job
Build the core of the Rust AWS twin in the EXISTING crate (Cargo.toml, src/). Do NOT modify src/s3_routing.rs, src/router.rs, src/cors.rs, or src/lib.rs's existing exports (you may ADD items to lib.rs). Python server runs on :4566 — if you need to run your Rust server, use port 4567.

## Dependencies to add (Cargo.toml):
axum, tower, hyper, tokio, http, bytes, serde_json, quick-xml (XML), tracing, tracing-subscriber, thiserror, uuid, chrono, base64, sha2, hmac, hex

## Modules to build (src/core/...):

### 1. `src/core/account.rs`
Account/region keying: `AccountRegion { account: u64 (12-digit), region: String }`. Account resolution from access key ID: 12-digit number = that account; anything else = default 123456789012. Mirror src/robotocore/state/manager.py semantics (read it first).

### 2. `src/core/state.rs`
Generic in-memory store registry: `StateStore` — per (account, region) map of named tables; table = `Arc<RwLock<HashMap<String, Value>>>` (serde_json::Value for now). Methods: get/put/delete/scan/list_tables. This is the substrate services will use. Keep it simple — no persistence yet.

### 3. `src/core/protocol.rs` — THE heart. AWS wire protocol layer:
- **Router**: from (method, path, host, query, headers) determine (service, operation)
  - X-Amz-Target header: `service.Operation` (JSON + EC2 protocols)
  - POST body `Action=` param (query/ec2 protocol)
  - Querystring `Action=` (query protocol)
  - S3: path-style /bucket/key and virtual-hosted (reuse existing s3_routing module); operation from method+subresources
  - For S3, parse the botocore spec (see data paths in shared brief) to map (method, path, subresource-params) -> operation
- **Parser**: HTTP request -> `ParsedRequest { service, operation, params: HashMap<String, Value>, body: Bytes }`
  - query protocol: form-encoded params -> typed via spec (string, integer, long, boolean, timestamp ISO8601, list, map, structure with flattened list params)
  - json protocol (1.0/1.1): JSON body -> typed via spec (target string in header)
  - ec2 protocol: like query but `Content-Type: application/x-www-form-urlencoded`, timestamps unix epoch
  - rest-*: params from path/query + JSON or XML body
- **Serializer**: `ParsedResponse { status, headers, body: Value }` -> HTTP response
  - json: `{"X-Amz-RequestId": ...}` + body per spec (timestamp ISO8601 or epoch, blob base64)
  - query: XML envelope `<ActionResponse><ActionResult>...` (read src/robotocore/protocols/ to see exactly what Python emits and match it)
  - rest-json: JSON body, status from op
  - rest-xml: XML body
  - error responses: correct HTTP status + protocol-correct error body. For json: `{"__type":"Code","message":"..."}`; query/ec2: `<ErrorResponse><Error><Code/><Message/></Error></ErrorResponse>` (ec2 wraps: `<Response><Errors><Error>...`); rest: `{"message":"..."}` / XML `<?xml...?><Error>...`

Spec loading: at startup load `service-2.json` files from a configurable dir (default: point at botocore data dir via env `ROBOTOCORE_SPECS_DIR`). Write `src/core/spec.rs` that loads + caches spec: service -> { protocol, targetPrefix, operations: { name -> { http, input shape, output shape, errors, endpoint } }, shapes }.

### 4. `src/core/signing.rs`
Validate AWS SigV4: extract credential scope from `Authorization: AWS4-HMAC-SHA256 ...`, verify signature math against the raw request (canonical request per AWS spec). On invalid -> 403 with `SignatureDoesNotMatch` in the protocol's error format. Also support `X-Amz-Security-Token` pass-through. (Read AWS SigV4 spec; you know it. The Python server does NOT validate signing — this is a RUST improvement, but it must accept valid signatures from botocore.)

### 5. `src/core/server.rs`
Axum app: catch-all route -> protocol layer -> service handler. Service handlers come from a `ServiceRegistry`: `HashMap<String, Arc<dyn ServiceHandler>>` where `ServiceHandler::handle(ParsedRequest) -> ParsedResponse`. For now, register one built-in service: **STS** (implement in `src/core/services/sts.rs`):
- GetCallerIdentity: returns UserId=access_key, Account=account, Arn=arn:aws:iam::{account}:root (match Python output exactly — check /tmp/golden/baseline.json)
- GetAccessKeyInfo: returns Account, plus `Arn`
- GetCallerIdentity variants per golden capture
- Unknown STS op -> correct `InvalidAction`/`UnrecognizedClientException`-style error per protocol

Also implement **health endpoints** (match Python exactly — read src/robotocore for the routes):
- GET /_robotocore/health
- GET /_robotocore/config
- GET /_robotocore/audit (can return empty audit list for now, correct shape)

### 6. `src/bin/robotocore-rust.rs`
CLI: `--port` (default 4567), `--account` (optional). Starts the axum server.

## Acceptance (must pass before you report done):
1. `cargo build && cargo test && cargo clippy -- -D warnings && cargo fmt` all clean
2. With your server on :4567 and botocore pointed there:
   ```
   sts.get_caller_identity() -> 200, Account == "123456789012"
   sts.get_access_key_info(Serial="AKIA123456789012") -> 200
   s3.get_bucket_location("anything") -> 404 NoSuchBucket (correct XML error format)  [S3 routing must at least NOT 500 — unknown S3 op may return 501 Not
Implemented per robotocore convention, that's fine]
   ```
   Verify by running: `.venv/bin/python -c` with boto3 endpoint_url http://localhost:4567
3. Unit tests for: spec loading, query/json/ec2 parse (each protocol), serializer (each protocol incl. errors), sigv4 validation (valid + tampered signature), account resolution
4. Report to /Users/jackdanger/www/robotocore-rust/tooling/report-w2.md: what works, what's stubbed, exact commands to run the server, any golden-capture ops you verified against /tmp/golden/baseline.json.

Constraints:
- Do NOT git commit/branch/stash.
- Keep existing 109 tests passing.
- If botocore spec files have quirks (e.g. `__type` errors, event streams), handle the common case and note the rest in your report.
