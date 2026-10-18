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
        }
    }
}

/// Parse a query-protocol request (form-encoded parameters).
pub fn parse_query_protocol(
    body: &[u8],
) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let body_str = str::from_utf8(body)?;
    let mut raw_params: Vec<(String, String)> = Vec::new();

    for pair in body_str.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let key = urlencoding::decode(key)?.into_owned();
            let value = urlencoding::decode(value)?.into_owned();
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
                lists.entry(base.to_string()).or_default().push((idx, value));
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
        let arr: Vec<Value> = entries.iter().map(|(_, v)| Value::String(v.clone())).collect();
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
        // Try hand-rolled resolver first
        if let Some(op) = resolve_rest_operation(service, method.as_str(), path_str) {
            return Ok(op.to_string());
        }
        // Try spec-driven resolver
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
fn resolve_rest_op_from_spec(
    service: &str,
    method: &str,
    path: &str,
) -> Option<&'static str> {
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
                // Strip version from spec URI
                let spec_rest = spec_uri.trim_start_matches('/');
                let spec_rest = if let Some(idx) = spec_rest.find('/') {
                    &spec_rest[idx + 1..]
                } else {
                    spec_rest
                };
                // Match: replace {ParamName} with .* and check
                let mut pattern = String::new();
                let mut i = 0;
                while i < spec_rest.len() {
                    if spec_rest[i..].starts_with('{') {
                        if let Some(end) = spec_rest[i..].find('}') {
                            pattern.push_str(".*");
                            i += end + 1;
                            continue;
                        }
                    }
                    // Escape regex special chars
                    i += 1;
                }
                // Strip trailing / from pattern
                let pattern = pattern.trim_end_matches('/');
                let rest_no_slash = rest.trim_end_matches('/');
                if rest_no_slash == pattern || (rest_no_slash.starts_with(pattern) && pattern.ends_with(".*")) {
                    return Some(op);
                }
                // Simple contains check for path params
                if rest_no_slash.contains('/') && pattern.contains(".*") {
                    // Check segment by segment
                    let spec_parts: Vec<&str> = spec_rest.split('/').filter(|s| !s.is_empty() && !s.starts_with('{')).collect();
                    let req_parts: Vec<&str> = rest_no_slash.split('/').filter(|s| !s.is_empty()).collect();
                    if spec_parts.len() <= req_parts.len() {
                        let mut match_ok = true;
                        for (i, sp) in spec_parts.iter().enumerate() {
                            if i < req_parts.len() && *sp != req_parts[i] {
                                match_ok = false;
                                break;
                            }
                        }
                        if match_ok {
                            return Some(op);
                        }
                    }
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

/// Resolve a REST operation from service + method + path.
fn resolve_rest_operation(service: &str, method: &str, path: &str) -> Option<&'static str> {
    match service {
        "lambda" => {
            // Version-agnostic: match /YYYY-MM-DD/... patterns
            let p = path.trim_start_matches('/').trim_end_matches('/');
            // The path is like "2015-03-31/functions" or "2015-03-31/functions/{name}"
            // Strip the version prefix (first segment if it looks like a date)
            let rest = if let Some(idx) = p.find('/') {
                &p[idx + 1..]
            } else {
                p
            };
            let m = method;
            if rest == "functions" || rest.starts_with("functions/") {
                let after = if rest.starts_with("functions/") {
                    rest.strip_prefix("functions/").unwrap_or("")
                } else {
                    ""
                };
                if after.is_empty() {
                    // /functions (no name)
                    if m == "GET" { Some("ListFunctions") }
                    else if m == "POST" { Some("CreateFunction") }
                    else { None }
                } else if after.contains('/') {
                    // /functions/{name}/... (sub-resource)
                    if after.ends_with("/configuration") {
                        if m == "GET" { Some("GetFunctionConfiguration") }
                        else if m == "PUT" { Some("UpdateFunctionConfiguration") }
                        else { None }
                    } else if after.contains("/invocations") || after.contains("/invoke") {
                        Some("Invoke")
                    } else if after.contains("/aliases") {
                        if m == "GET" { Some("ListAliases") }
                        else if m == "POST" { Some("CreateAlias") }
                        else { None }
                    } else if after.contains("/tags") {
                        if m == "GET" { Some("ListTags") }
                        else if m == "PUT" { Some("TagResource") }
                        else if m == "DELETE" { Some("UntagResource") }
                        else { None }
                    } else if after.contains("/code") {
                        Some("GetFunction")
                    } else if after.contains("/versions") {
                        if m == "POST" { Some("PublishVersion") }
                        else if m == "GET" { Some("ListVersionsByFunction") }
                        else { None }
                    } else {
                        None
                    }
                } else {
                    // /functions/{name} (single segment)
                    if m == "GET" { Some("GetFunction") }
                    else if m == "DELETE" { Some("DeleteFunction") }
                    else if m == "PUT" { Some("UploadFunction") }
                    else { None }
                }
            } else if let Some(after) = rest.strip_prefix("aliases/") {
                if after.is_empty() {
                    None
                } else if after.contains('/') {
                    if after.ends_with("/configuration") {
                        if m == "GET" { Some("GetAlias") }
                        else if m == "PUT" { Some("UpdateAlias") }
                        else { None }
                    } else if after.contains("/tags") {
                        if m == "GET" { Some("ListTags") }
                        else if m == "PUT" { Some("TagResource") }
                        else if m == "DELETE" { Some("UntagResource") }
                        else { None }
                    } else {
                        None
                    }
                } else {
                    if m == "GET" { Some("GetAlias") }
                    else if m == "PUT" { Some("UpdateAlias") }
                    else if m == "DELETE" { Some("DeleteAlias") }
                    else { None }
                }
            } else if let Some(after) = rest.strip_prefix("layers/") {
                if after.contains("/versions") {
                    if m == "POST" { Some("PublishLayerVersion") }
                    else if m == "GET" && after.contains("/versions/") {
                        Some("GetLayerVersion")
                    } else if m == "GET" { Some("ListLayerVersions") }
                    else if m == "DELETE" { Some("DeleteLayerVersion") }
                    else { None }
                } else if m == "GET" {
                    Some("ListLayers")
                } else {
                    None
                }
            } else if rest.starts_with("event-source-mappings") {
                if rest.ends_with("event-source-mappings") || rest == "event-source-mappings/" {
                    if m == "GET" { Some("ListEventSources") }
                    else if m == "POST" { Some("AddEventSource") }
                    else { None }
                } else {
                    // /event-source-mappings/{uuid}
                    if m == "GET" { Some("GetEventSource") }
                    else if m == "PUT" { Some("UpdateEventSource") }
                    else if m == "DELETE" { Some("RemoveEventSource") }
                    else { None }
                }
            } else if rest.starts_with("tags") {
                if m == "GET" { Some("ListTags") }
                else if m == "PUT" { Some("TagResource") }
                else if m == "DELETE" { Some("UntagResource") }
                else { None }
            } else if rest.starts_with("code-signing-configs") {
                if m == "POST" { Some("CreateCodeSigningConfig") }
                else if m == "GET" { Some("ListCodeSigningConfigs") }
                else if rest.contains('/') {
                    if m == "GET" { Some("GetCodeSigningConfig") }
                    else if m == "PUT" { Some("UpdateCodeSigningConfig") }
                    else if m == "DELETE" { Some("DeleteCodeSigningConfig") }
                    else { None }
                } else { None }
            } else if rest.starts_with("capacity-providers") {
                if m == "POST" { Some("CreateCapacityProvider") }
                else if m == "GET" { Some("ListCapacityProviders") }
                else if rest.contains('/') {
                    if m == "GET" { Some("GetCapacityProvider") }
                    else if m == "PUT" { Some("UpdateCapacityProvider") }
                    else if m == "DELETE" { Some("DeleteCapacityProvider") }
                    else { None }
                } else { None }
            } else if rest.starts_with("function-url") {
                if m == "POST" { Some("CreateFunctionUrlConfig") }
                else if m == "GET" {
                    if rest.contains('/') { Some("GetFunctionUrlConfig") }
                    else { Some("ListFunctionUrlConfigs") }
                }
                else if m == "PUT" { Some("UpdateFunctionUrlConfig") }
                else if m == "DELETE" { Some("DeleteFunctionUrlConfig") }
                else { None }
            } else if rest.starts_with("event-invoke-config") || rest.contains("/event-invoke-config") {
                if m == "PUT" { Some("PutFunctionEventInvokeConfig") }
                else if m == "GET" { Some("GetFunctionEventInvokeConfig") }
                else if m == "DELETE" { Some("DeleteFunctionEventInvokeConfig") }
                else { None }
            } else if rest.starts_with("account-settings") {
                if m == "GET" { Some("GetAccountSettings") }
                else if m == "PUT" { Some("PutAccountSettings") }
                else { None }
            } else if rest.starts_with("policy") || rest.contains("/policy") {
                if m == "POST" { Some("AddPermission") }
                else if m == "DELETE" { Some("RemovePermission") }
                else { None }
            } else if rest.starts_with("concurrency") || rest.contains("/concurrency") {
                if m == "PUT" { Some("PutFunctionConcurrency") }
                else if m == "GET" { Some("GetFunctionConcurrency") }
                else if m == "DELETE" { Some("DeleteFunctionConcurrency") }
                else { None }
            } else if rest.starts_with("provisioned-concurrency") || rest.contains("/provisioned-concurrency") {
                if m == "PUT" { Some("PutProvisionedConcurrencyConfig") }
                else if m == "GET" { Some("GetProvisionedConcurrencyConfig") }
                else if m == "DELETE" { Some("DeleteProvisionedConcurrencyConfig") }
                else if m == "GET" && rest.ends_with("/provisioned-concurrency") {
                    Some("ListProvisionedConcurrencyConfigs")
                }
                else { None }
            } else if rest.starts_with("scaling-config") || rest.contains("/scaling-config") {
                if m == "PUT" { Some("PutFunctionScalingConfig") }
                else if m == "GET" { Some("GetFunctionScalingConfig") }
                else if m == "DELETE" { Some("DeleteFunctionScalingConfig") }
                else { None }
            } else if rest.starts_with("runtime-management-config") || rest.contains("/runtime-management-config") {
                if m == "PUT" { Some("PutRuntimeManagementConfig") }
                else if m == "GET" { Some("GetRuntimeManagementConfig") }
                else { None }
            } else if rest.starts_with("recursion-config") || rest.contains("/recursion-config") {
                if m == "PUT" { Some("PutFunctionRecursionConfig") }
                else if m == "GET" { Some("GetFunctionRecursionConfig") }
                else if m == "DELETE" { Some("DeleteFunctionRecursionConfig") }
                else { None }
            } else if rest.starts_with("durable-executions") || rest.contains("/durable-executions") {
                if m == "GET" { Some("GetDurableExecution") }
                else if m == "POST" { Some("StopDurableExecution") }
                else if rest.contains("history") { Some("GetDurableExecutionHistory") }
                else if rest.contains("callback") { Some("SendDurableExecutionCallbackSuccess") }
                else if rest.contains("heartbeat") { Some("SendDurableExecutionCallbackHeartbeat") }
                else if rest.contains("failure") { Some("SendDurableExecutionCallbackFailure") }
                else if m == "GET" && rest.ends_with("durable-executions") {
                    Some("ListDurableExecutionsByFunction")
                }
                else { None }
            } else if rest.starts_with("invocations") || rest.contains("/invocations") {
                if m == "POST" {
                    if rest.contains("response-stream") || rest.contains("streaming") {
                        Some("InvokeWithResponseStream")
                    } else {
                        Some("Invoke")
                    }
                } else { None }
            } else if rest == "invocation" || rest.starts_with("invocation") {
                if m == "POST" { Some("Invoke") }
                else { None }
            } else {
                None
            }
        }
        _ => None,
    }
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
