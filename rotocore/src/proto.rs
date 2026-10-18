//! Shared wire-protocol helpers for rotocore services.

use crate::Response;
use serde_json::Value;
use std::collections::BTreeMap;

/// XML-escape a value interpolated into a response document.
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
    out
}

/// The XML declaration + query-protocol response wrapper every query service
/// uses: `<{Root}Response>` wrapping the body with `<ResponseMetadata>` unless
/// the body already contains it.
pub fn query_response_xml(root: &str, xmlns: Option<&str>, body_xml: &str) -> String {
    let xmlns_attr = xmlns
        .map(|ns| format!(r#" xmlns="{ns}""#))
        .unwrap_or_default();
    let meta = if body_xml.contains("ResponseMetadata") {
        String::new()
    } else {
        request_id_xml(&uuid::Uuid::new_v4().to_string())
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{root}Response{xmlns_attr}>{body_xml}{meta}</{root}Response>"#
    )
}

/// Build `<ResponseMetadata><RequestId>..</RequestId></ResponseMetadata>`.
pub fn request_id_xml(request_id: &str) -> String {
    format!(
        r#"<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>"#,
        xml_escape(request_id)
    )
}

/// Build an AWS query-protocol error XML document.
pub fn error_xml(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Response><Errors><Error><Code>{}</Code><Message>{}</Message></Error></Errors><RequestID>{}</RequestID></Response>"#,
        xml_escape(code),
        xml_escape(message),
        uuid::Uuid::new_v4()
    )
}

/// Query-protocol error as a wire response (400 client errors).
pub fn error_response(status: u16, code: &str, message: &str) -> Response {
    Response::xml(status, error_xml(code, message).into_bytes())
}

/// JSON-protocol error response (JSON services use `__type` codes).
pub fn json_error(status: u16, code: &str, message: &str) -> Response {
    let body = serde_json::json!({ "__type": code, "message": message });
    let mut resp = Response::json_status(status, &body);
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

/// Collect a service's JSON body into structured params.
pub fn parse_json_body(body: &[u8]) -> Value {
    if body.is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_slice(body).unwrap_or_else(|_| serde_json::json!({}))
}

/// Percent-decode a path or query component (`%XX` and `+` → space).
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => match (
                bytes.get(i + 1).and_then(|c| (*c as char).to_digit(16)),
                bytes.get(i + 2).and_then(|c| (*c as char).to_digit(16)),
            ) {
                (Some(h), Some(l)) => {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                }
                _ => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Current UTC time as ISO-8601 with millisecond precision (e.g. `2026-01-01T00:00:00.123Z`).
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Current unix time in seconds (AWS numeric timestamp fields).
pub fn now_unix() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// Unix time `seconds` in the future (AWS expiry timestamps).
pub fn unix_in(seconds: f64) -> f64 {
    now_unix() + seconds
}

/// Parse a query string into repeated-key maps (`a=1&a=2` → `"a": ["1", "2"]`).
pub fn query_map(query_string: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pair in query_string.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.entry(url_decode(k)).or_default().push(url_decode(v));
    }
    map
}

/// First value for `key` in a parsed query map, if any.
pub fn query_first<'a>(map: &'a BTreeMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    map.get(key).and_then(|v| v.first()).map(String::as_str)
}
