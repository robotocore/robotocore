//! README for rotocore — what it is and the porting loop.

# rotocore

The moto-equivalent AWS service engine in pure Rust. Replaces the Python
moto sidecar (`scripts/moto_sidecar.py` + vendored moto) that used to serve
non-native services through the robotocore gateway.

## Contract

- The pytest compatibility suites (`tests/compatibility/`) are the spec.
  A service port is "done" when its suite passes at the same count it did
  against the moto bridge.
- Request/response shapes must match the moto-backed bridge's wire format —
  query-XML for query-protocol services, `application/x-amz-json-1.1` for
  JSON services, REST-XML for S3-style (the S3 crate already owns that one).

## Layout

```
rotocore/
├── src/
│   ├── lib.rs       # Request/Response types + dispatch table
│   ├── proto.rs     # shared wire helpers (query XML, JSON, errors)
│   ├── store.rs     # per-(account, region) state registry
│   └── services/
│       ├── mod.rs   # service → backend wiring
│       └── <svc>/   # one module per AWS service (backend.rs, tests.rs)
```

## Porting loop (per service)

1. Read moto's implementation for the service (vendor/moto source at the
   pinned submodule commit, or the pinned cpython sidecar behavior) — the
   compat tests that exercise it are the acceptance bar.
2. Port the state model + operation semantics to `services/<svc>/`.
3. `cargo test -p rotocore <svc>` for unit coverage of the semantics.
4. Wire it into `services/mod.rs`, run its pytest suite against the server
   (`ENDPOINT_URL=http://127.0.0.1:<rust-port> uv run python -m pytest
   tests/compatibility/test_<svc>_compat.py -q`) and record the count in the
   commit message.
5. Commit early and often; the moderator merges from worktrees.
