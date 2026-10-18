---
session: "rot-state-port"
timestamp: "2026-10-18T17:30:00Z"
model: kimi-code
---

## Human

Add real state save/load endpoints to the pure-Rust robotocore server
(src/core/**) so the three state/concurrency compat suites pass the same
count they achieve against the Python ground truth. Snapshot surface per
service crate; endpoints commit-per-endpoint; never push.[Lane:
LOG_PREFIX=rot10 PY_PORT=4890 RUST_PORT=4891 SIDECAR_PORT=4892]

## Assistant

## Key decisions

**Baseline counts recorded first** (rust branch, worktree
`.worktrees/rot-state`): the 3 suites on Python 4890 = 20 passed;
on Rust 4891 = 9 passed, 11 failed. Failures: all state save/load paths
(501/404 — the routes do not exist in the Rust server) plus the API
Gateway concurrency test (the moto sidecar only sees `POST /` from the
rust proxy because `MotoProxy::forward` dropped the request path, so
rest-json services 501).

**Snapshot shape**: every service exposes `snapshot_json()` /
`restore_json(&Value)` / `reset()` keyed by `"account|region"` at the top
level (IAM uses `"account"`; region is global). Save replaces state
(clears then applies), never merges — verified by round-trip unit tests
in each crate's `snapshot.rs` using isolated account/region pairs so the
process-global stores are not clobbered by parallel tests.

**Per-crate approach**: parking_lot `RwLock`/`Mutex` and `AtomicU64`
fields carry `#[serde(with = "crate::snapshot::rw")]`-style attributes;
serde `rc` feature enabled so nested `Arc<T>` map values serialize;
byte payloads (S3 objects, Kinesis records, lambda zips) are base64;
`u128` hash keys in Kinesis serialize as decimal strings; tuple-keyed
maps (SQS none, events rules, lambda aliases/provisioning) use joined
`"a::b"` keys. KMS skips the RSA/EC private keys (`#[serde(skip)]`) and
regenerates them on restore (documented in the module doc).

**Central registry**: `SnapshotRegistry` in src/core/state.rs holds
(name, closure triple) per service built from the SAME handler instances
the ServiceRegistry hands out (crates like dynamodb keep state on the
handler instance, not process globals) — so `ServiceRegistry::new`
factors into `new_with_handlers()`.

**Not touched**: existing wire behavior of any service crate (only added
modules/derives/visibility bumps `pub(crate)`), Python server, and the
checked-in compat suites.


## Phase 2: endpoints + apigateway execution (same session)

## Human

(continuation) implement the /_robotocore/state/* endpoints + proxy fix.

## Assistant

**Central registry** lives in src/core/state.rs:
`SnapshotRegistry` built from the SAME `NativeHandlers` the wire path
uses (`ServiceRegistry::new_with_handlers`); adapters are closures, so
no trait churn for foreign crate types. The binary passes the registry
into `build_router` as part of `AppState`, plus in-memory named and
versioned snapshot stores.

**Endpoint semantics chosen by the suites** (recorded from the python
ground truth, not invented): save with `path` echoes the given path
(`data["path"] == save_path` for the unnamed case), named saves write
`<path>/<name>.json`; load responds `loaded` / `no_state_found`; reset
responds `{status: "reset"}`; health keeps `running`.

**Proxy fix (root cause)**: `MotoProxy::forward` built its URL as
`{base}/` — the request path never reached the sidecar, so every
rest-json/rest-xml route 501'd (`routing_map` had no `/` rule). Now the
original path + query and `x-robotocore-service` are preserved. This
unlocks the whole deployed-API config flow (create/api, resources,
methods, integrations, deployments all 2xx in the audit log).

**Deployed-API invocation** (`_user_request_`): moto has no route for
it (checked `flask_paths`) — the ground truth executes it in
`src/robotocore/services/apigateway/executor.py`. The rust gateway
mirrors that with `src/core/apigw_exec.rs`: fetch stage/resources/
integration from the sidecar, match exact then templated paths, build
the proxies event, and invoke through the native lambda crate (with a
`/2015-03-31/functions/{fn}/invocations` lambda request so the
lambda crate's own operation resolver routes it).

**Result**: 20/20 on the three suites (identical to ground truth);
`cargo test --workspace` green; clippy `-D warnings` clean; fmt clean.
