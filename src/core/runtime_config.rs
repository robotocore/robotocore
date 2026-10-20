//! Runtime configuration mirror of the python twin's
//! `robotocore/config/runtime.py`: a whitelisted set of settings that can
//! be updated through `POST /_robotocore/config` without restarting.
//!
//! Gated behind `ENABLE_CONFIG_UPDATES=1` at startup, exactly like the
//! python twin — a POST to this endpoint answers 403 unless the twin was
//! launched with that flag.
//!
//! NOTE: values are stored and reflected back on GET-style reads; settings
//! whose consumers are not ported to the rust gateway yet (ENFORCE_IAM
//! middleware, audit-log resize, retention toggles) are accepted but have
//! no consumer. Read `runtime_config::current(key)` when wiring those
//! subsystems so they honor runtime overrides.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_keys_are_rejectable() {
        assert!(!known_key("NOT_A_SETTING"));
        assert!(known_key("ENFORCE_IAM"));
    }

    #[test]
    fn set_and_get_round_trip() {
        set("DYNAMODB_REMOVE_EXPIRED_ITEMS", "0".to_string());
        assert_eq!(get("DYNAMODB_REMOVE_EXPIRED_ITEMS"), Some("0".to_string()));
    }

    fn known_key(key: &str) -> bool {
        MUTABLE_SETTINGS.iter().any(|(k, _)| *k == key)
    }
}
