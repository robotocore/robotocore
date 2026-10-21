# robotocore Rust bindings

Thin `aws-sdk` wrapper that aims clients at a running robotocore twin.
Mirrors `../python/robotocore_client` (endpoint resolution, per-account
credentials); the wire behavior is language-agnostic and lives
server-side.

## API

The standalone crate `robotocore_client/` (deliberately detached from
the repo workspace — same pattern as `docker-extension/vm`) exposes:

| Export | Purpose |
|---|---|
| `endpoint_url(Option<&str>)` | Twin URL: arg, `ROBOTOCORE_ENDPOINT`, then `http://127.0.0.1:4566`; trailing slashes trimmed |
| `credentials(Option<&str>)` | `(access_key, "test")` — the 12-digit access key IS the account id, zero-padded |
| `session_config(account, endpoint, region)` | An `aws_config::ConfigLoader` pre-wired for the twin (`BehaviorVersion::latest()`) |
| `config(account, endpoint, region)` | The loaded `SdkConfig` |
| `session(service, account, endpoint, region)` | Ready-made client: `Ok(Client::S3(_))` / `Ok(Client::Sqs(_))` for "s3"/"sqs", `Err(UnknownServiceError)` otherwise |
| `s3_client(..)` / `sqs_client(..)` | Pre-typed clients; the S3 client forces path-style addressing |

```rust
use robotocore_client::{session, Client};

let client = session("sqs", None, None, "us-east-1").await?;
let sqs = client.as_sqs().expect("sqs client");
```

## Smoke test

`examples/smoke.rs` creates/sends/receives/deletes an SQS queue and does
an S3 put/get/delete round-trip against a running twin (`cargo build`,
unlike the example, needs no twin):

```bash
cargo test                                # unit tests + doctests, no twin needed
ROBOTOCORE_ENDPOINT=http://127.0.0.1:5000 cargo run --example smoke
                                          # defaults to http://127.0.0.1:4566
```
