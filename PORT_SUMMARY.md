# Robotocore Rust port — status

The `rust` branch carries a pure-Rust implementation of the robotocore
gateway. The Python server, vendored moto, and the moto bridge sidecar are
gone; compatibility suites that exercised them were pruned with them.

- 18 core Rust service crates (one per service in `crates/`), each holding
  real state keyed by (account, region) and matching the wire protocol of
  its AWS service family (query, JSON, REST-JSON, REST-XML).
- The `rotocore` crate reimplements moto's far-service layer in Rust:
  74 services are armed natively (connect, glue, elb/elbv2, cloudfront,
  redshift, emr, iot, athena, api gateway, and more). Requests for
  unported services answer a generic `501 NotImplemented` — nothing is
  forwarded to a sidecar anymore.
- Real fidelity pieces: a Python-executing Lambda runtime
  (`crates/lambda/src/runtime.rs`), a DynamoDB expression engine
  (`crates/dynamodb/src/expr.rs`), a Step Functions ASL interpreter
  (`crates/stepfunctions/src/asl.rs`), an EventBridge pattern matcher
  (`crates/events/src/pattern.rs`), and S3 SelectObjectContent with the
  raw AWS event-stream framing.
- Introspection endpoints: `/_robotocore/health|services|runtimes|audit|
  usage/errors|config`. Availability labels are derived from the real
  handler tables (`native` / `absent`).
- `scripts/harness/parity.py --rust-only` measures fidelity against the
  golden baseline in `.parity/expectations_current.json`.

## Verification loop

    cargo fmt --all
    cargo clippy --all-targets -- -D warnings
    cargo test --workspace --lib
    make parity                                  # fidelity map

## Server

    cargo build --release
    make start
    # or standalone:
    target/release/robotocore-rust --port 4567
