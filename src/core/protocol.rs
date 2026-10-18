//! AWS wire protocol parsing and serialization.
//!
//! Handles parsing HTTP requests into ParsedRequest structures,
//! and serializing ParsedResponse back to HTTP responses.
//! Supports query, json, ec2, rest-json, rest-xml protocols.

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str;

/// Parsed AWS request extracted from HTTP.
#[derive(Debug, Clone)]
pub struct ParsedRequest {
    pub service: String,
    pub operation: String,
    pub params: HashMap<String, Value>,
    pub body: Bytes,
    pub region: String,
    pub account: u64,
    /// HTTP method (GET, PUT, POST, DELETE, HEAD).
    pub method: String,
    /// Request path (e.g. /bucket/key).
    pub path: String,
    /// Raw query string (e.g. "list-type=2&prefix=a").
    pub query_string: String,
    /// Request headers (lowercase keys).
    pub headers: HashMap<String, String>,
}

/// Parsed AWS response to be serialized to HTTP.
#[derive(Debug, Clone)]
pub struct ParsedResponse {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
    pub body: Value,
    /// Optional pre-serialized body. When set, it is sent verbatim,
    /// bypassing query-XML/JSON encoding (used by JSON-protocol services).
    pub raw: Option<String>,
    /// Optional arbitrary-byte body (binary payloads, XML event streams).
    /// When set, takes precedence over `raw` and is sent verbatim.
    pub raw_bytes: Option<Vec<u8>>,
}

impl ParsedResponse {
    /// Create a successful response with JSON body.
    pub fn json_success(body: Value) -> Self {
        Self {
            status: StatusCode::OK,
            headers: {
                let mut h = HashMap::new();
                h.insert(
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.1".to_string(),
                );
                h
            },
            body,
            raw: None,
            raw_bytes: None,
        }
    }

    /// Create a response with a pre-serialized raw body (e.g. JSON-protocol
    /// service responses that control their own serialization and content type).
    pub fn raw(status: StatusCode, headers: HashMap<String, String>, body: String) -> Self {
        Self {
            status,
            headers,
            body: Value::Null,
            raw: Some(body),
            raw_bytes: None,
        }
    }

    /// Create an error response.
    pub fn error(status: StatusCode, code: String, message: String, protocol: &str) -> Self {
        let body = match protocol {
            "json" | "rest-json" => json!({
                "__type": code,
                "message": message
            }),
            "query" | "ec2" => {
                // XML format will be handled by the server
                json!({
                    "error_code": code,
                    "error_message": message
                })
            }
            _ => json!({
                "message": message
            }),
        };

        Self {
            status,
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/json".to_string());
                h
            },
            body,
            raw: None,
            raw_bytes: None,
        }
    }
}

/// Percent-decode one `application/x-www-form-urlencoded` token: `%XX`
/// escapes plus `+` meaning space, in a single pass so a literal plus
/// (encoded as `%2B`) survives and a raw `+` becomes a space.
fn decode_form_value(input: &str) -> Result<String, Box<dyn std::error::Error>> {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() + 1 && i + 2 < bytes.len() => {
                let hex = &input[i + 1..i + 3];
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|e| e.into())
}

/// Parse a query-protocol request (form-encoded parameters).
pub fn parse_query_protocol(
    body: &[u8],
) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let body_str = str::from_utf8(body)?;
    let mut raw_params: Vec<(String, String)> = Vec::new();

    for pair in body_str.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let key = decode_form_value(key)?;
            let value = decode_form_value(value)?;
            raw_params.push((key, value));
        }
    }

    // Build params, handling member indices (e.g., TagKeys.member.1=env)
    let mut params = HashMap::new();
    let mut lists: HashMap<String, Vec<(u32, String)>> = HashMap::new();

    for (key, value) in raw_params {
        // Check for member index pattern: Name.member.N=value
        if let Some(pos) = key.rfind(".member.") {
            let base = &key[..pos];
            let idx_str = &key[pos + 8..];
            if let Ok(idx) = idx_str.parse::<u32>() {
                lists
                    .entry(base.to_string())
                    .or_default()
                    .push((idx, value));
            } else {
                params.insert(key, Value::String(value));
            }
        } else if key.contains(".") {
            // Nested key (e.g., CreateDate=...) - store as-is
            params.insert(key, Value::String(value));
        } else {
            params.insert(key, Value::String(value));
        }
    }

    // Convert lists to JSON arrays
    for (base, mut entries) in lists {
        entries.sort_by_key(|(idx, _)| *idx);
        let arr: Vec<Value> = entries
            .iter()
            .map(|(_, v)| Value::String(v.clone()))
            .collect();
        params.insert(base, Value::Array(arr));
    }

    Ok(params)
}

/// Parse a JSON-protocol request.
pub fn parse_json_protocol(
    body: &[u8],
) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    if body.is_empty() {
        return Ok(HashMap::new());
    }

    let json: Value = serde_json::from_slice(body)?;
    let mut params = HashMap::new();

    if let Some(obj) = json.as_object() {
        for (key, value) in obj {
            params.insert(key.clone(), value.clone());
        }
    }

    Ok(params)
}

/// Serialize a response body to JSON.
pub fn serialize_json_response(value: &Value, _operation: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

/// Serialize a response body to XML (query protocol).
pub fn serialize_query_response(value: &Value, operation: &str, request_id: &str) -> String {
    // Build query protocol XML response
    let result_wrapper = format!("{}Result", operation);
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{operation}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <{result_wrapper}>"#,
        operation = operation,
        result_wrapper = result_wrapper
    );

    // Serialize value as XML elements
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            xml.push_str(&format!("        <{}>", key));
            xml.push_str(&value_to_xml_content(val));
            xml.push_str(&format!("</{}>\n", key));
        }
    }

    xml.push_str(&format!(
        "    </{}>
",
        result_wrapper
    ));
    xml.push_str(&format!(
        "    <ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>
",
        request_id
    ));
    xml.push_str(&format!("</{operation}Response>", operation = operation));

    xml
}

/// Serialize an error response to XML (query protocol).
pub fn serialize_query_error(code: &str, message: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <Error>
        <Type>Sender</Type>
        <Code>{code}</Code>
        <Message>{message}</Message>
    </Error>
    <RequestId>{request_id}</RequestId>
</ErrorResponse>"#,
        code = code,
        message = message,
        request_id = request_id
    )
}

fn value_to_xml_content(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        Value::Array(arr) => arr
            .iter()
            .map(value_to_xml_content)
            .collect::<Vec<_>>()
            .join(""),
        Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| {
                format!(
                    "<{key}>{val}</{key}>",
                    key = k,
                    val = value_to_xml_content(v)
                )
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

/// Determine operation name from headers and body.
pub fn extract_operation(
    method: &Method,
    headers: &HeaderMap,
    body: &[u8],
    service: &str,
    query_string: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Try X-Amz-Target header (JSON/EC2 protocols)
    if let Some(target) = headers.get("X-Amz-Target") {
        let target_str = target.to_str()?;
        if let Some(op_part) = target_str.rsplit('.').next() {
            return Ok(op_part.to_string());
        }
    }

    // Try form-encoded Action parameter (query/EC2 protocols)
    let body_str = str::from_utf8(body).unwrap_or("");
    if let Some(start) = body_str.find("Action=") {
        let after_action = &body_str[start + 7..];
        if let Some(end) = after_action.find('&') {
            return Ok(after_action[..end].to_string());
        } else {
            return Ok(after_action.to_string());
        }
    }

    // Try query string for Action parameter (GET requests with query params)
    if let Some(start) = query_string.find("Action=") {
        let after_action = &query_string[start + 7..];
        if let Some(end) = after_action.find('&') {
            return Ok(after_action[..end].to_string());
        } else {
            return Ok(after_action.to_string());
        }
    }

    // REST-JSON / REST-XML: derive from method + path
    // Extract path from the Host/URI (the server passes it via a header or
    // we check the x-robotocore-path header)
    if let Some(path) = headers.get("x-robotocore-path") {
        let path_str = path.to_str()?;
        // Spec-driven table resolver (lambda legacy ops, etc.)
        if let Some(op) = resolve_rest_op_from_spec(service, method.as_str(), path_str) {
            return Ok(op.to_string());
        }
    }

    Err("Could not determine operation".into())
}

/// Resolve a REST operation using the spec-driven op table.
///
/// Matches the request path against the botocore spec's requestUri templates.
/// The path is version-agnostic: the first segment (API version) is stripped.
fn resolve_rest_op_from_spec(service: &str, method: &str, path: &str) -> Option<&'static str> {
    let p = path.trim_start_matches('/').trim_end_matches('/');
    // Strip the version prefix (first segment)
    let rest = if let Some(idx) = p.find('/') {
        &p[idx + 1..]
    } else {
        p
    };

    match service {
        "lambda" => {
            for (spec_method, spec_uri, op) in crate::core::rest_ops::lambda_rest_ops() {
                if *spec_method != method {
                    continue;
                }
                let spec_rest = strip_version_prefix(spec_uri);
                if segments_match(spec_rest, rest) {
                    return Some(op);
                }
            }
            None
        }
        "s3" => {
            // S3 is handled specially - the operation is derived from
            // method + path structure, not from a spec table
            None
        }
        _ => None,
    }
}

/// Strip a leading version segment (e.g. "2015-03-31/") from a URI.
fn strip_version_prefix(uri: &str) -> &str {
    let trimmed = uri.trim_matches('/');
    match trimmed.find('/') {
        Some(idx) => &trimmed[idx + 1..],
        None => trimmed,
    }
}

/// Segment-wise REST path match: spec segments equal request segments,
/// except `{ParamName}` segments which match any single non-empty segment.
/// Segment counts must be equal; trailing `/` is ignored on both sides.
fn segments_match(spec: &str, req: &str) -> bool {
    let spec_segs: Vec<&str> = spec
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let req_segs: Vec<&str> = req
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    if spec_segs.len() != req_segs.len() {
        return false;
    }
    for (sp, rp) in spec_segs.iter().zip(req_segs.iter()) {
        let is_param = sp.len() > 2 && sp.starts_with('{') && sp.ends_with('}');
        if !is_param && sp != rp {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_protocol() {
        let body = b"Action=GetCallerIdentity&Version=2011-06-15";
        let result = parse_query_protocol(body).unwrap();
        assert_eq!(
            result.get("Action").and_then(|v| v.as_str()),
            Some("GetCallerIdentity")
        );
        assert_eq!(
            result.get("Version").and_then(|v| v.as_str()),
            Some("2011-06-15")
        );
    }

    #[test]
    fn test_parse_json_protocol() {
        let body = br#"{"foo": "bar", "num": 42}"#;
        let result = parse_json_protocol(body).unwrap();
        assert_eq!(result.get("foo").and_then(|v| v.as_str()), Some("bar"));
    }

    #[test]
    fn test_serialize_query_response() {
        let value = json!({
            "UserId": "AKIAIOSFODNN7EXAMPLE",
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:root"
        });
        let xml = serialize_query_response(&value, "GetCallerIdentity", "req-123");
        assert!(xml.contains("UserId"));
        assert!(xml.contains("123456789012"));
    }
}
