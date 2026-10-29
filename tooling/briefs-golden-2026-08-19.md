# Rust Port — Shared Context (read fully before starting)

Repo: /Users/jackdanger/www/robotocore-rust
Python twin (reference impl, running on http://localhost:4566): src/robotocore/
Rust crate: Cargo.toml + src/*.rs (lib name robotocore_rust)
Golden baseline captures (requests+responses from the running Python server): /tmp/golden/baseline.json
  - shape: [ {op, http:{method,path,query,headers,body}, status, response, ms}, ... ]

Key facts:
- Python server IS running on :4566 (started with `uv run python -m robotocore.main`). Do NOT stop/restart it; if it's down, start it that way from repo root.
- botocore specs: .venv/lib/python3.14/site-packages/botocore/data/<service>/<version>/service-2.json
- probes/<service>.json: per-op status (working / needs_params / not_implemented / 500_error)
- Existing Rust modules: src/s3_routing.rs, src/router.rs, src/cors.rs (all with tests, 109 passing)
- Rust toolchain: cargo 1.98, rustc installed. Use `cargo build/test` from repo root.
- The Python side optionally imports `robotocore_rust` (PyO3 feature) — existing pattern in src/robotocore/gateway/{s3_routing,router,cors}.py

Definition of done for every task:
1. `cargo test` passes (all tests, including yours)
2. `cargo clippy -- -D warnings` clean on your new code
3. `cargo fmt` applied
4. Do NOT touch git (no commits, no branches, no stash)
5. Write a report to the file named in your brief, and send one reply to parent with the summary
