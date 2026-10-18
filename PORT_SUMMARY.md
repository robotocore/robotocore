# Robotocore Rust port — status

The `rust` branch carries a full Rust reimplementation of the robotocore
gateway. It is behavioral-parity complete against the Python server's own
compatibility suites (see `.parity/state.json` after any parity run):

- 18 native Rust service crates (one crate per service in `crates/`), each
  holding real state keyed by (account, region) and matching the wire
  protocol of its AWS service family (query, JSON, REST-JSON, REST-XML).
- A moto bridge for every remaining service: `src/core/proxy.rs` forwards
  requests to `scripts/moto_sidecar.py`, which dispatches into vendored
  moto backends (with cross-service mirroring for CloudFormation, IAM
  instance profiles, and S3/EventBridge notifications).
- Real fidelity pieces: a Python-executing Lambda runtime
  (`crates/lambda/src/runtime.rs`), a DynamoDB expression engine
  (`crates/dynamodb/src/expr.rs`), a Step Functions ASL interpreter
  (`crates/stepfunctions/src/asl.rs`), an EventBridge pattern matcher
  (`crates/events/src/pattern.rs`), and S3 SelectObjectContent with the
  raw AWS event-stream framing.
- Introspection endpoints mirroring the Python server shapes:
  `/_robotocore/health|services|runtimes|audit|usage/errors|config`.
- The Python ground-truth server remains in `src/robotocore/` and drives
  the parity metric: `uv run python scripts/harness/parity.py`.

## Verification loop

    cargo fmt --all
    cargo clippy --all-targets -- -D warnings
    cargo test --workspace                       # 272 unit tests
    uv run python scripts/harness/parity.py      # fidelity map

## Server

    cargo build --release
    LOG_PREFIX=mylane bash scripts/start_parity_servers.sh   # + bridge + py ground truth
    # or standalone:
    target/release/robotocore-rust --port 4567 --moto-url http://127.0.0.1:4568
