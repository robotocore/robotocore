//! DynamoDB request/response types for the json protocol.

use bytes::Bytes;
use serde_json::Value;

/// The werkzeug/flask plain 500 page moto_server renders for an unhandled
/// Python exception, byte-for-byte (moto 5.2.4 werkzeug 3.1.9, live capture
/// 2026-10-26 — 265 bytes including the trailing newline).
pub const MOTO_SERVER_WERKZEUG_500: &str = "<!doctype html>\n\
<html lang=en>\n\
<title>500 Internal Server Error</title>\n\
<h1>Internal Server Error</h1>\n\
<p>The server encountered an internal error and was unable to complete \
your request. Either the server is overloaded or there is an error in \
the application.</p>\n";

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
        let body = serde_json::json!({
            "__type": code,
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

    /// moto's response layer crashes on some wire requests — e.g. a
    /// TransactWriteItems row that parses to no operation (moto's
    /// `BaseJSONParser._parse_structure` keeps only the modeled Put/Update/
    /// Delete/ConditionCheck members, so such a row arrives as `{}` and
    /// `item_values = list(item.values())[0]` raises IndexError,
    /// dynamodb/responses.py:1478, before the model's row loop can produce a
    /// cancellation). moto_server renders unhandled Python exceptions as
    /// werkzeug's plain 500 page rather than an AWS error envelope. This
    /// mirrors those bytes verbatim (live transcript, moto 5.2.4
    /// moto_server 2026-10-26): HTTP 500, `Content-Type: text/html;
    /// charset=utf-8`, and the werkzeug error page below.
    pub fn werkzeug_500() -> Self {
        Self {
            status: 500,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "text/html; charset=utf-8".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: MOTO_SERVER_WERKZEUG_500.to_string(),
        }
    }
}
