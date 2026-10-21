//! Lambda Function URL HTTP surface.
//!
//! The ground truth serves `GET|POST /lambda-url/{url-id}/{path}` with its
//! own executor: the request becomes a Function-URL v2.0 event, the invoked
//! lambda's `{statusCode, headers, body, isBase64Encoded}` shapes the HTTP
//! response, and the URL config's Cors block overlays CORS headers.
//! (moto has no function-URL invocation surface; the recorded suite
//! `tests/compatibility/test_lambda_function_urls_compat.py` is the spec.)

use axum::http::{Method, StatusCode};
use serde_json::{json, Map, Value};

/// A parsed `/lambda-url/{url_id}/{path…}` request target.
pub struct FurlTarget<'a> {
    pub url_id: &'a str,
    /// Path after the url-id ("" for `/lambda-url/{id}/`).
    pub sub_path: &'a str,
}

pub fn match_url(path: &str) -> Option<FurlTarget<'_>> {
    let rest = path.strip_prefix("/lambda-url/")?;
    let (url_id, sub) = match rest.split_once('/') {
        Some((id, sub)) => (id, sub),
        // `/lambda-url/{id}` (no trailing slash) is still a valid invocation.
        None => (rest, ""),
    };
    if url_id.is_empty() {
        return None;
    }
    Some(FurlTarget {
        url_id,
        sub_path: sub,
    })
}

/// Execute one function URL request; returns the wire response.
pub fn execute(
    lambda: &crate::core::server::NativeHandlers,
    method: &Method,
    target: FurlTarget<'_>,
    account: u64,
    body_bytes: &[u8],
    headers: &std::collections::HashMap<String, String>,
    query_string: &str,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let Some((func_account, func_region, config)) = lambda.lambda.find_url_config(target.url_id)
    else {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(
                json!({"message": format!("function url {} does not exist", target.url_id)}),
            ),
        )
            .into_response();
    };
    let Some(function_name) = config
        .get("FunctionArn")
        .and_then(Value::as_str)
        .map(String::from)
    else {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"message": "function url config has no function"})),
        )
            .into_response();
    };

    // OPTIONS preflight answers with the CORS headers only (no invocation).
    if method == Method::OPTIONS {
        return options_response(&config, headers).unwrap_or_else(|| {
            // No matching CORS config: preflight is rejected without
            // invoking the function.
            (
                StatusCode::FORBIDDEN,
                axum::Json(json!({"message": "No Access-Control-Allow-Origin header set. Should match allowed origins set on function URL."})),
            )
                .into_response()
        });
    }

    // The Function-URL v2.0 event (the recorded echo suite reads method,
    // rawPath, rawQueryString, headers, queryStringParameters, body,
    // isBase64Encoded and version).
    let raw_path = format!("/{}", target.sub_path);
    let mut event_headers: Map<String, Value> = Map::new();
    for (k, v) in headers {
        event_headers.insert(k.clone(), json!(v.clone()));
    }
    let mut event = json!({
        "version": "2.0",
        "rawPath": raw_path,
        "rawQueryString": query_string,
        "headers": event_headers,
        "requestContext": {
            "accountId": account.to_string(),
            "requestId": uuid::Uuid::new_v4().to_string(),
            "http": {
                "method": method.as_str(),
                "path": raw_path,
                "sourceIp": "127.0.0.1",
            },
        },
        "isBase64Encoded": false,
    });
    if let Some(params) = single_value_query(query_string) {
        event["queryStringParameters"] = Value::Object(params);
    }
    if !body_bytes.is_empty() {
        event["body"] = json!(String::from_utf8_lossy(body_bytes));
    }

    let result =
        lambda
            .lambda
            .invoke_engine_event(func_account, &func_region, &function_name, &event);

    // Invoke errors surface as HTTP 500 payloads describing the failure.
    if result.function_error.is_some() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "application/json")],
            result.payload,
        )
            .into_response();
    }

    // Success: the lambda's response shapes the HTTP response.
    let parsed: Value = serde_json::from_str(&result.payload).unwrap_or(Value::Null);
    let mut status = 200u16;
    let mut resp_headers: Vec<(String, String)> = Vec::new();
    let mut resp_body = result.payload.clone();
    let mut is_base64 = false;

    // A handler may return an object (statusCode/headers/body) or a plain
    // JSON scalar; scalars pass through as-is.
    if let Value::Object(obj) = &parsed {
        if let Some(code) = obj.get("statusCode").and_then(Value::as_u64) {
            status = u16::try_from(code).unwrap_or(200);
        }
        if let Some(b) = obj.get("body") {
            resp_body = match b {
                Value::String(text) => text.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            };
        } else {
            resp_body = String::new();
        }
        if let Some(Value::Object(map)) = obj.get("headers") {
            for (k, v) in map {
                resp_headers.push((k.to_string(), value_to_header(v)));
            }
        }
        if let Some(b) = obj.get("isBase64Encoded").and_then(Value::as_bool) {
            is_base64 = b;
        }
    }
    if is_base64 {
        use base64::Engine;
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(resp_body.as_bytes()) {
            resp_body = String::from_utf8_lossy(&bytes).into_owned();
        }
    }

    // CORS from the URL config (origin-matched, like the ground truth).
    let cors_headers = cors_headers(&config, headers.get("origin").map(String::as_str));
    for (k, v) in cors_headers {
        upsert(&mut resp_headers, &k, &v);
    }

    if !resp_headers.iter().any(|(k, _)| k == "content-type") {
        resp_headers.push(("content-type".to_string(), "application/json".to_string()));
    }

    let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let mut response = axum::http::Response::builder()
        .status(status_code)
        .body(axum::body::Body::from(resp_body))
        .expect("valid response");
    for (k, v) in resp_headers {
        if let Ok(name) = axum::http::HeaderName::from_bytes(k.as_bytes()) {
            if let Ok(value) = axum::http::HeaderValue::from_str(&v) {
                response.headers_mut().insert(name, value);
            }
        }
    }
    response.into_response()
}

/// OPTIONS preflight answers with the CORS headers only (no invocation).
pub fn options_response(
    config: &Value,
    headers: &std::collections::HashMap<String, String>,
) -> Option<axum::response::Response> {
    use axum::response::IntoResponse;

    let cors_headers = cors_headers(config, headers.get("origin").map(String::as_str));
    if cors_headers.is_empty() {
        return None;
    }
    let mut response = axum::http::Response::builder()
        .status(StatusCode::OK)
        .body(axum::body::Body::empty())
        .expect("valid response");
    for (k, v) in cors_headers {
        if let Ok(name) = axum::http::HeaderName::from_bytes(k.as_bytes()) {
            if let Ok(value) = axum::http::HeaderValue::from_str(&v) {
                response.headers_mut().insert(name, value);
            }
        }
    }
    Some(response.into_response())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Encode one JSON header value the way the gateway does elsewhere: strings
/// pass through, arrays join (multi-value headers), anything else stringifies.
fn value_to_header(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(value_to_header)
            .collect::<Vec<String>>()
            .join(","),
        other => other.to_string(),
    }
}

/// Insert or replace a header case-insensitively.
fn upsert(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    match headers
        .iter_mut()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
    {
        Some((_, existing)) => *existing = value.to_string(),
        None => headers.push((name.to_string(), value.to_string())),
    }
}

/// CORS response headers for the request origin, or none when the config
/// disallows it (recorded behavior: exact-match lists echo the origin,
/// wildcard lists answer `*`, and only matching origins get CORS headers).
fn cors_headers(config: &Value, origin: Option<&str>) -> Vec<(String, String)> {
    let Some(cors) = config.get("Cors").filter(|c| c.is_object()) else {
        return Vec::new();
    };
    let Some(origin) = origin else {
        return Vec::new();
    };
    let origins: Vec<String> = cors
        .get("AllowOrigins")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    if origins.is_empty() {
        return Vec::new();
    }
    let allow_origin = if origins.iter().any(|o| o == origin) {
        origin.to_string()
    } else if origins.iter().any(|o| o == "*") {
        "*".to_string()
    } else {
        return Vec::new();
    };
    let mut out = vec![("access-control-allow-origin".into(), allow_origin)];
    for (response_name, config_name) in [
        ("access-control-allow-methods", "AllowMethods"),
        ("access-control-allow-headers", "AllowHeaders"),
        ("access-control-expose-headers", "ExposeHeaders"),
        ("access-control-allow-credentials", "AllowCredentials"),
    ] {
        if let Some(values) = list_of(cors.get(config_name)) {
            out.push((response_name.to_string(), values.join(",")));
        }
    }
    if let Some(max_age) = cors.get("MaxAge").and_then(Value::as_i64) {
        out.push(("access-control-max-age".into(), max_age.to_string()));
    }
    out
}

/// Convenience for string-list CORS knobs (a `Vec<Value>` of strings).
fn list_of(v: Option<&Value>) -> Option<Vec<String>> {
    match v {
        Some(Value::Array(arr)) => {
            let items: Vec<String> = arr
                .iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect();
            (!items.is_empty()).then_some(items)
        }
        _ => None,
    }
}

/// Parse the raw query string into the single-value map the v2.0 event uses
/// (`+` decodes to space, like the lambda crate's own parser). `None` when
/// there is no query at all (the event omits the key then).
fn single_value_query(query_string: &str) -> Option<Map<String, Value>> {
    if query_string.is_empty() {
        return None;
    }
    let mut out = Map::new();
    for pair in query_string.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = query_decode(k);
        if key.is_empty() {
            continue;
        }
        // First value wins for repeated keys (AWS v2.0 events use
        // singleValue versions; any value choice is unasserted).
        out.entry(key).or_insert(json!(query_decode(v)));
    }
    Some(out)
}

/// Query-string decode (spaces via `+` and `%20` alike).
fn query_decode(s: &str) -> String {
    http_decode(&s.replace('+', " "))
}

/// Percent-decode. Minimal local copy of the lambda crate's URL decoder so
/// the gateway module stays dependency-free.
fn http_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            _ => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_parse() {
        let params = single_value_query("foo=bar&baz=123").expect("params");
        assert_eq!(params["foo"], json!("bar"));
        assert_eq!(params["baz"], json!("123"));
        assert!(single_value_query("").is_none());
    }

    #[test]
    fn test_url_match() {
        let t = match_url("/lambda-url/abc123/").expect("target");
        assert_eq!(t.url_id, "abc123");
        assert_eq!(t.sub_path, "");
        let t = match_url("/lambda-url/abc123/api/items/1").expect("target");
        assert_eq!(t.sub_path, "api/items/1");
        assert!(match_url("/restapis/foo").is_none());
    }

    #[test]
    fn test_options_preflight_no_invocation() {
        // A CORS-configured URL answers its preflight with the CORS headers
        // and no function invocation.
        let config = json!({
            "Cors": {
                "AllowOrigins": ["https://example.com"],
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["X-Custom-Header"],
                "MaxAge": 3600,
            }
        });
        let headers: std::collections::HashMap<String, String> =
            [("origin", "https://example.com")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        let resp = options_response(&config, &headers).expect("preflight response");
        assert_eq!(resp.status(), StatusCode::OK);
        let origin = resp
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok())
            .expect("allow-origin header");
        assert_eq!(origin, "https://example.com");
        assert!(
            resp.headers().contains_key("access-control-allow-methods"),
            "preflight carries the allowed methods"
        );

        // Without a matching CORS config there is nothing to answer — the
        // route layer must reject instead of invoking the function.
        assert!(options_response(&json!({}), &headers).is_none());
        let no_origin: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        assert!(options_response(&config, &no_origin).is_none());
    }
}
