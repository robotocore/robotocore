//! CloudFormation wire XML helpers.
//!
//! Reproduces the Python provider's `_xml_response` / `_xml` shapes
//! bug-for-bug: values are spliced raw (no XML escaping), lists render as
//! `<member>` elements, and scalars render like Python `str()`.

use crate::proto::xml_escape;
use serde_json::Value;
use uuid::Uuid;

pub const CFN_XMLNS: &str = "http://cloudformation.amazonaws.com/doc/2010-05-15/";

/// Render a JSON value with Python `dict_to_xml` semantics.
pub fn dict_to_xml(d: &Value) -> String {
    match d {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Object(_) => format!("<member>{}</member>", dict_to_xml(item)),
                other => format!("<member>{}</member>", xml_scalar_str(other)),
            })
            .collect(),
        Value::Object(map) => {
            let mut parts = String::new();
            for (k, v) in map {
                match v {
                    Value::Array(_) | Value::Object(_) => {
                        parts.push_str(&format!("<{k}>{}</{k}>", dict_to_xml(v)));
                    }
                    other => parts.push_str(&format!("<{k}>{}</{k}>", xml_scalar_str(other))),
                }
            }
            parts
        }
        other => xml_scalar_str(other),
    }
}

fn xml_scalar_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => {
            if *b {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

/// 200 response in the Python provider's XML envelope. `action` may carry a
/// trailing "Response" (callers vary); both suffixes are normalized.
pub fn ok(action: &str, body_xml: &str) -> crate::Response {
    let base = action.trim_end_matches("Response");
    let root = format!("{base}Response");
    let result_name = format!("{base}Result");
    let body = format!(
        "<?xml version=\"1.0\"?>\
         <{root} xmlns=\"{ns}\">\
         <{result_name}>{body_xml}</{result_name}>\
         <ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>\
         </{root}>",
        ns = CFN_XMLNS,
        rid = Uuid::new_v4(),
    );
    crate::Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "text/xml".to_string())],
        body: body.into_bytes(),
    }
}

/// Error response in the Python provider's envelope.
pub fn error(code: &str, message: &str, status: u16) -> crate::Response {
    let rid = Uuid::new_v4();
    let body = format!(
        "<?xml version=\"1.0\"?>\
         <ErrorResponse xmlns=\"{ns}\">\
         <Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>\
         <RequestId>{rid}</RequestId></ErrorResponse>",
        ns = CFN_XMLNS,
        // Some messages embed XML (e.g. validation with templates); the
        // Python provider spliced raw text, so escape only the code.
        code = xml_escape(code),
    );
    crate::Response {
        status,
        headers: vec![("Content-Type".to_string(), "text/xml".to_string())],
        body: body.into_bytes(),
    }
}

/// 200 response with an empty `<{action}Result/>` (py `EmptyResult`).
pub fn empty(action: &str) -> crate::Response {
    ok(action, "")
}

/// Timestamp in the wire format the Python provider uses.
pub fn timestamp_from(epoch_secs: i64) -> String {
    use chrono::TimeZone;
    match chrono::Utc.timestamp_opt(epoch_secs, 0) {
        chrono::LocalResult::Single(t) => t.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        _ => String::new(),
    }
}

pub fn now_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Extract the first `<tag>value</tag>` text from an XML body.
pub fn xml_text(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = body.find(&open)? + open.len();
    let close = format!("</{tag}>");
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}
