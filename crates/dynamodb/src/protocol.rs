//! DynamoDB request/response types for the json protocol.

use bytes::Bytes;
use serde_json::Value;

/// A parsed DynamoDB request.
#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    /// The parsed JSON body as a Value.
    pub params: Value,
    /// The raw request body.
    pub body: Bytes,
}

/// A DynamoDB response to be serialized to HTTP.
#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    /// moto's ERROR_TYPE_PREFIX (dynamodb/exceptions.py:9).
    pub(crate) const ERROR_TYPE_PREFIX: &'static str = "com.amazonaws.dynamodb.v20120810#";

    /// The namespace rides every DDB error envelope's `__type` —
    /// modeled and unmodeled alike (the raw-byte census pin: the fork
    /// answers "com.amazonaws.dynamodb.v20120810#ResourceNotFound…" on
    /// describe_table of a ghost table where the twin answered the bare
    /// code).
    fn referenced_code(code: &str) -> String {
        if code.starts_with(Self::ERROR_TYPE_PREFIX) {
            code.to_string()
        } else {
            format!("{}{}", Self::ERROR_TYPE_PREFIX, code)
        }
    }

    /// moto's ERROR_TYPE_PREFIX (dynamodb/exceptions.py:7): rides the
    /// MOCK-family + the eight bare-literal Resource* classes — but NOT
    /// the bare `ValidationException`/`SerializationException`/`
    /// InvalidConversion` classes (exceptions.py:380-401, raised from
    /// table.py's N-conversion/type legs). The writer namespaces by the
    /// FORK's raise-class semantics: the bare families are pinned by
    /// their exact message texts.
    fn bare_family_message(message: &str) -> bool {
        const BARE_PREFIXES: [&str; 4] = [
            "Null attribute value types must have the value of true",
            "The parameter cannot be converted to a numeric value:",
            "Number overflow.",
            "Number underflow.",
        ];
        const BARE_EQUALS: [&str; 2] = [
            "NUMBER_VALUE cannot be converted to String",
            "Start of structure or map found where not expected",
        ];
        BARE_PREFIXES.iter().any(|p| message.starts_with(p)) || BARE_EQUALS.contains(&message)
    }

    /// Namespace any inline `__type` member a caller-built body carries
    /// (the TransactionCanceled arm already prefixes; this normalizes the
    /// callers that don't).
    fn namespaced_body(body: Value) -> Value {
        if let Some(name) = body.get("__type").and_then(Value::as_str) {
            let namespaced = Self::referenced_code(name);
            let mut out = body;
            if let Some(obj) = out.as_object_mut() {
                obj.insert("__type".into(), Value::String(namespaced));
            }
            return out;
        }
        body
    }

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
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        // The fork's raise-class split (Review: moto prefixes the mock
        // family + the eight Resource* literal classes; the N-conversion
        // and serialization legs ride BARE — exceptions.py:380-401,
        // table.py:550-611). The bare families are pinned by message.
        let bare = Self::bare_family_message(message);
        let body = serde_json::json!({
            "__type": if bare { code.to_string() } else { Self::referenced_code(code) },
            "message": message
        });
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
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// An error response carrying the caller-built JSON body — same headers
    /// as `error`, for codes that need extra members beyond `__type`/`message`
    /// (e.g. moto's TransactionCanceledException with per-row CancellationReasons).
    pub fn error_with_body(status: u16, body: Value) -> Self {
        let body = Self::namespaced_body(body);
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
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }
}
