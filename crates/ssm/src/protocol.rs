//! SSM request/response types (json protocol).

use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    pub params: Value,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&strip_nulls(&body)).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// moto ssm's MODELED error codes (exceptions.py classes carrying
    /// their own shapes — the wire measured 2026-11-12: lowercase
    /// `message` AND capital `Message` both present). Everything OFF
    /// this list rides the unmodeled get_error_model shape — capital
    /// `Message` only. ValidationException and InvalidFilterKey are
    /// unmodeled in SSM (auto-created via get_error_model).
    const MODELED_CODES: [&str; 8] = [
        "ParameterNotFound",
        "ParameterVersionNotFound",
        "ParameterLimitExceeded",
        "ParameterMaxVersionLimitExceeded",
        "ParameterAlreadyExists",
        "InvalidFilterOption",
        // The label-limit model: measured both-member on the wire
        // (moto ssm/models.py:2146 — the review's BLOCKER row with an
        // 11-label drive).
        "ParameterVersionLabelLimitExceeded",
        "InvalidOptionException",
    ];

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let modeled = {
            use std::sync::OnceLock;
            static SET: OnceLock<std::collections::HashSet<&'static str>> = OnceLock::new();
            let set = SET.get_or_init(|| Self::MODELED_CODES.iter().copied().collect());
            set.contains(code)
        };
        Self::error_envelope(status, code, message, modeled)
    }

    /// Moto's JSON error serializer (moto/core/serialize.py :529-544 + the
    /// get_error_model empty-member shape at core/errors.py:145-155):
    /// MODELED codes carry the lowercase `message` member AND the generic
    /// capital `Message`; UNMODELED shapes (get_error_model fabricates
    /// the members from an empty shape — ValidationException in SSM is
    /// auto-created) carry capital `Message` ONLY — the measured
    /// envelope on the 2026-11-11 census rows.
    pub fn error_envelope(status: u16, code: &str, message: &str, modeled: bool) -> Self {
        let body = if modeled {
            serde_json::json!({ "__type": code, "message": message, "Message": message })
        } else {
            serde_json::json!({ "__type": code, "Message": message })
        };
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }
}

/// Remove `null` object members the way moto's JSON serializer does.
fn strip_nulls(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if v.is_null() {
                    continue;
                }
                out.insert(k.clone(), strip_nulls(v));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}
