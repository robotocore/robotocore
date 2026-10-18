# robotocore Rust bindings

Use `aws-sdk-rs` against the endpoint like any AWS account:

```rust
use aws_config::Region;
let conf = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://127.0.0.1:4566")
    .region(Region::new("us-east-1"))
    .credentials_provider(...)
    .load().await;
```

The crates under `crates/` and `rotocore/` are server-side, not bindings.
