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
