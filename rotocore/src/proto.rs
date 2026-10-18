//! Shared wire-protocol helpers for rotocore services.

use crate::Response;
use serde_json::Value;

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
    Response::json_status(status, &body)
}

/// Collect a service's JSON body into structured params.
pub fn parse_json_body(body: &[u8]) -> Value {
    if body.is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_slice(body).unwrap_or_else(|_| serde_json::json!({}))
}
