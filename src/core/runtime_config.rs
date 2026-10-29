//! Runtime configuration mirror of the python twin's
//! `robotocore/config/runtime.py`: a whitelisted set of settings that can
//! be updated through `POST /_robotocore/config` without restarting.
//!
//! Gated behind `ENABLE_CONFIG_UPDATES=1` at startup, exactly like the
//! python twin — a POST to this endpoint answers 403 unless the twin was
//! launched with that flag.
//!
//! Keys without a live consumer are rejected with 400 instead of being
//! accepted and silently ignored: "accepted but not enforced" is a silent
//! bypass (a client toggling ENFORCE_IAM would believe enforcement changed
//! when nothing happened). `CONSUMED_SETTINGS` lists the keys this rust
//! gateway actually honors; read `runtime_config::get(key)` when wiring a
//! subsystem, then add the key there so it becomes settable again.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;

/// Whitelist of settings that can be changed at runtime, mirroring the
/// python twin's `MUTABLE_SETTINGS` (name → description).
pub const MUTABLE_SETTINGS: &[(&str, &str)] = &[
    (
        "LOG_LEVEL",
        "Root log verbosity (DEBUG, INFO, WARNING, ERROR)",
    ),
    ("DEBUG", "Enable/disable debug mode (0 or 1)"),
    ("ENFORCE_IAM", "Toggle IAM policy enforcement (0 or 1)"),
    ("AUDIT_LOG_SIZE", "Maximum entries in the audit ring buffer"),
    (
        "SQS_DELAY_PURGE_RETRY",
        "Toggle SQS purge delay retry behavior (0 or 1)",
    ),
    (
        "SQS_DELAY_RECENTLY_DELETED",
        "Toggle SQS recently-deleted delay (0 or 1)",
    ),
    (
        "DYNAMODB_REMOVE_EXPIRED_ITEMS",
        "Toggle DynamoDB TTL item removal (0 or 1)",
    ),
    ("USAGE_ANALYTICS", "Toggle usage analytics (0 or 1)"),
];

/// Subset of `MUTABLE_SETTINGS` with a live consumer in the rust gateway.
///
/// Derived from actual `runtime_config::get(key)` call sites: today none of
/// the whitelisted settings are consumed anywhere in this codebase, so the
/// endpoint rejects every key until a consumer is wired. This check must
/// not be skipped when re-enabling a key — an accepted key MUST have a
/// reader, otherwise the config endpoint lies about taking effect.
pub const CONSUMED_SETTINGS: &[&str] = &[];

static UPDATES_ENABLED: Lazy<bool> =
    Lazy::new(|| std::env::var("ENABLE_CONFIG_UPDATES").as_deref() == Ok("1"));

static OVERRIDES: Lazy<Mutex<HashMap<String, String>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn updates_enabled() -> bool {
    *UPDATES_ENABLED
}

/// Priority mirrors the python twin: runtime override > env var.
pub fn get(key: &str) -> Option<String> {
    if let Some(v) = OVERRIDES.lock().unwrap_or_else(|p| p.into_inner()).get(key) {
        return Some(v.clone());
    }
    std::env::var(key).ok()
}

pub fn set(key: &str, value: String) {
    OVERRIDES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.to_string(), value);
}

/// Python `str(value)` for the config endpoint's echo of non-string values:
/// bools render `True`/`False`, null renders `None`, arrays and objects use
/// single-quoted separators — the wire shape the python twin produced.
pub fn python_repr(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(true) => "True".to_string(),
        serde_json::Value::Bool(false) => "False".to_string(),
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(items) => {
            // str(["a"]) == "['a']" — container members render repr(),
            // which quotes strings.
            let inner: Vec<String> = items.iter().map(python_repr_member).collect();
            format!("[{}]", inner.join(", "))
        }
        serde_json::Value::Object(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        python_repr_member(&Value::String(k.clone())),
                        python_repr_member(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
    }
}

/// Inside containers (list items, dict keys and values) python uses repr():
/// strings carry single quotes. The settings whitelist only ever carries
/// ASCII values, so the only escapes are backslash and quote.
fn python_repr_member(value: &serde_json::Value) -> String {
    if let serde_json::Value::String(s) = value {
        let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
        return format!("'{escaped}'");
    }
    python_repr(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_keys_are_rejectable() {
        assert!(!known_key("NOT_A_SETTING"));
        assert!(known_key("ENFORCE_IAM"));
    }

    #[test]
    fn consumed_keys_are_a_whitelisted_subset() {
        // Guard against the endpoint accepting (and lying about) any future
        // key that has no reader: CONSUMED_SETTINGS entries must exist in
        // MUTABLE_SETTINGS and must have a runtime_config::get call site.
        // An empty CONSUMED_SETTINGS is only valid while nothing consumes
        // these settings (see the module note).
        for &key in CONSUMED_SETTINGS {
            assert!(
                MUTABLE_SETTINGS.iter().any(|(k, _)| *k == key),
                "consumed key {key} must be whitelisted in MUTABLE_SETTINGS"
            );
        }
        assert!(!known_consumed("ENFORCE_IAM"));
        assert!(!known_consumed("NOT_A_SETTING"));
    }

    #[test]
    fn set_and_get_round_trip() {
        set("DYNAMODB_REMOVE_EXPIRED_ITEMS", "0".to_string());
        assert_eq!(get("DYNAMODB_REMOVE_EXPIRED_ITEMS"), Some("0".to_string()));
    }

    #[test]
    fn python_repr_matches_str() {
        assert_eq!(python_repr(&json!(true)), "True");
        assert_eq!(python_repr(&json!(null)), "None");
        assert_eq!(python_repr(&json!(3)), "3");
        assert_eq!(python_repr(&json!(["a"])), "['a']");
        assert_eq!(python_repr(&json!({"a": 1})), "{'a': 1}");
        assert_eq!(python_repr(&json!("en")), "en");
    }

    fn known_key(key: &str) -> bool {
        MUTABLE_SETTINGS.iter().any(|(k, _)| *k == key)
    }

    fn known_consumed(key: &str) -> bool {
        CONSUMED_SETTINGS.contains(&key)
    }
}
