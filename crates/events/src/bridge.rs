//! Foreign-target dispatch registry.
//!
//! EventBridge rules can target services whose handlers live in other crates
//! (Lambda, SNS, Kinesis, Step Functions, CloudWatch Logs). Wiring those
//! crates directly would create dependency cycles (S3 -> events -> lambda ->
//! S3), so this module holds a small process-global registry the gateway
//! populates at startup; the events provider routes non-SQS (and non-bus)
//! target ARNs through it. Missing registration means "call / dispatch is
//! not possible in this process" — callers treat that as a simulated no-op,
//! exactly like the Python provider's log-only target types.

use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Hook signature: `(target_arn, payload, account, region)`.
///
/// `payload` is the (already transformed) string the target would receive
/// with `event` being the full EventBridge event it was built from. The
/// hook returns `Err` when the target itself could not be reached (missing
/// resource, failed start) — the dispatcher turns that into a DLQ record;
/// invocation-time handler errors happen inside the target and are not DLQ
/// conditions.
pub type HookFn = Arc<dyn Fn(&str, &str, u64, &str) -> Result<(), String> + Send + Sync>;

fn hooks() -> &'static Lazy<parking_lot::RwLock<HashMap<String, HookFn>>> {
    static HOOKS: Lazy<parking_lot::RwLock<HashMap<String, HookFn>>> =
        Lazy::new(|| parking_lot::RwLock::new(HashMap::new()));
    &HOOKS
}

/// Register the implementation for a target type ("lambda", "sns", ...).
pub fn register_hook(kind: &str, hook: HookFn) {
    hooks().write().insert(kind.to_string(), hook);
}

/// Invoke the registered hook for `kind`. `None` means no implementation is
/// registered (unit tests, or a target type the gateway does not wire).
pub fn hook(
    kind: &str,
    arn: &str,
    payload: &str,
    account: u64,
    region: &str,
) -> Option<Result<(), String>> {
    let hook = hooks().read().get(kind).cloned()?;
    Some(hook(arn, payload, account, region))
}

/// Convenience: build the full-EB-event payload for a Lambda target.
pub fn event_payload(event: &Value) -> String {
    serde_json::to_string(event).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hook_roundtrip_and_none_when_absent() {
        register_hook(
            "lambda",
            Arc::new(|arn, payload, _account, _region| {
                if payload == "boom" {
                    Err(format!("broken: {arn}"))
                } else {
                    Ok(())
                }
            }),
        );
        assert!(hook("lambda", "arn:aws:lambda:x", "ok", 1, "r")
            .unwrap()
            .is_ok());
        assert!(hook("lambda", "arn:aws:lambda:x", "boom", 1, "r")
            .unwrap()
            .is_err());
        assert!(hook("unknown", "arn:x", "", 1, "r").is_none());
    }

    #[test]
    fn event_payload_serialises_the_event() {
        let p = event_payload(&json!({"source": "t"}));
        assert!(p.contains("\"source\":\"t\""));
    }
}
