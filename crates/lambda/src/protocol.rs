//! Lambda request/response types (rest-json protocol).

use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    pub params: Value,
    pub body: bytes::Bytes,
    /// HTTP method.
    pub method: String,
    /// Request path.
    pub path: String,
    /// Query string.
    pub query_string: String,
    /// Headers (lowercase).
    pub headers: std::collections::HashMap<String, String>,
}

/// Path split into decoded segments with the API-version prefix removed.
///
/// `/2015-03-31/functions/foo/versions` → `["functions", "foo", "versions"]`.
pub fn path_segments(path: &str) -> Vec<String> {
    let secs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let is_versioned = secs
        .first()
        .map(|s| s.starts_with("201") || s.starts_with("202"))
        .unwrap_or(false);
    let segs = if is_versioned { &secs[1..] } else { &secs[..] };
    segs.iter().map(|s| url_decode(s)).collect()
}

impl AwsRequest {
    /// Raw HTTP header access (map keys are lowercase).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// Query parameters (first value wins; `+` decodes to space).
    pub fn query_params(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for pair in self.query_string.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            let key = query_decode(k);
            if !key.is_empty() {
                out.entry(key).or_insert_with(|| query_decode(v));
            }
        }
        out
    }

    /// All values for a repeated query parameter (e.g. `tagKeys`).
    pub fn query_list(&self, name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for pair in self.query_string.split('&') {
            let (k, v) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            if query_decode(k) == name {
                out.push(query_decode(v));
            }
        }
        out
    }

    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query_params().get(name).cloned()
    }

    pub fn param_str(&self, name: &str) -> Option<String> {
        self.params.get(name).and_then(Value::as_str).map(String::from)
    }

    pub fn param_u64(&self, name: &str) -> Option<u64> {
        self.params.get(name).and_then(Value::as_u64)
    }

    pub fn param_i64(&self, name: &str) -> Option<i64> {
        self.params.get(name).and_then(Value::as_i64)
    }

    /// Function name from `/functions/{FunctionName}[...]` paths.
    pub fn path_fn_name(&self) -> String {
        let segs = path_segments(&self.path);
        match segs.first().map(String::as_str) {
            Some("functions") => segs.get(1).cloned().unwrap_or_default(),
            _ => self.param_str("FunctionName").unwrap_or_default(),
        }
    }

    /// (function name, alias name) from `/functions/{fn}/aliases/{alias}`.
    pub fn path_fn_alias(&self) -> (String, String) {
        let segs = path_segments(&self.path);
        let name = segs.get(1).cloned().unwrap_or_default();
        let alias = segs.get(3).cloned().unwrap_or_default();
        (name, alias)
    }

    /// UUID from `/event-source-mappings/{UUID}` or layer name from
    /// `/layers/{name}/...`.
    pub fn path_fn_uuid(&self) -> String {
        let segs = path_segments(&self.path);
        match segs.first().map(String::as_str) {
            Some("event-source-mappings") | Some("layers") => {
                segs.get(1).cloned().unwrap_or_default()
            }
            _ => String::new(),
        }
    }

    /// Layer version number from `/layers/{name}/versions/{N}[...]`.
    pub fn path_layer_version(&self) -> Option<i64> {
        let segs = path_segments(&self.path);
        if segs.len() >= 4 && segs[0] == "layers" && segs[2] == "versions" {
            return segs[3].parse().ok();
        }
        None
    }

    /// Last path segment — the StatementId in policy subresource paths.
    pub fn path_statement_id(&self) -> String {
        let segs = path_segments(&self.path);
        segs.last().cloned().unwrap_or_default()
    }

    /// Resource ARN from `/tags/{arn}` (URL-decoded).
    pub fn tags_arn(&self) -> String {
        let segs = path_segments(&self.path);
        if segs.first().map(String::as_str) == Some("tags") {
            return segs[1..].join("/");
        }
        String::new()
    }

    /// Code signing config ARN from `/code-signing-configs/{Arn}` (without
    /// the trailing `/functions` segment used by ListFunctionsByCodeSigningConfig).
    pub fn path_code_signing_arn(&self) -> String {
        let segs = path_segments(&self.path);
        if segs.first().map(String::as_str) == Some("code-signing-configs") {
            let mut arn: Vec<String> = segs[1..].to_vec();
            if arn.last().map(String::as_str) == Some("functions") {
                arn.pop();
            }
            return arn.join("/");
        }
        String::new()
    }
}

/// Query-string decode (also treats `+` as space).
fn query_decode(s: &str) -> String {
    let spaced = s.replace('+', " ");
    url_decode(&spaced)
}

/// Percent-decode a path segment (does NOT treat `+` as space).
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1]);
                let lo = hex_val(bytes[i + 2]);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
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
                ("Content-Type".to_string(), "application/json".to_string()),
                ("x-amz-request-id".to_string(), uuid::Uuid::new_v4().to_string()),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// Success with an empty body (Delete* style responses).
    pub fn empty(status: u16) -> Self {
        Self::raw(status, "application/json", String::new())
    }

    pub fn raw(status: u16, content_type: &str, body: String) -> Self {
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), content_type.to_string()),
                ("x-amz-request-id".to_string(), uuid::Uuid::new_v4().to_string()),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body,
        }
    }

    /// Response with extra (name, value) headers — Invoke-style payloads.
    pub fn raw_with_headers(
        status: u16,
        content_type: &str,
        body: String,
        extra: &[(&str, String)],
    ) -> Self {
        let mut headers = vec![
            ("Content-Type".to_string(), content_type.to_string()),
            ("x-amz-request-id".to_string(), uuid::Uuid::new_v4().to_string()),
            ("server".to_string(), "robotocore".to_string()),
        ];
        headers.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
        Self { status, headers, body }
    }

    /// Error body follows the Python ground truth:
    /// `{"Type": "User", "Message": ..., "__type": code}`.
    /// botocore maps Error.Code from `__type` and Error.Message from `Message`.
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = serde_json::json!({
            "Type": "User",
            "Message": message,
            "__type": code,
            "message": message,
        });
        let serialized = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("x-amz-request-id".to_string(), uuid::Uuid::new_v4().to_string()),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serialized,
        }
    }
}

/// 404 for a function resource, mirroring moto's message shape.
pub fn function_not_found(name_or_arn: &str, region: &str, account: u64) -> AwsResponse {
    let arn = if name_or_arn.starts_with("arn:") {
        name_or_arn.to_string()
    } else {
        format!("arn:aws:lambda:{region}:{account}:function:{name_or_arn}")
    };
    AwsResponse::error(
        404,
        "ResourceNotFoundException",
        &format!("Function not found: {arn}"),
    )
}

/// Resolve a function by name / ARN / alias. Qualifier resolution order:
/// query param (`?Qualifier=`) then ARN/name suffix (`name:qualifier`).
/// Versions are checked first; if the qualifier names an alias, the alias's
/// target version is used. `Err` is a 404 error response on miss.
pub fn resolve_fn(
    state: &crate::models::LambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<std::sync::Arc<crate::models::LambdaFunction>, AwsResponse> {
    let from_arn = if let Some(rest) = name.strip_prefix("arn:aws:lambda:") {
        rest.find(":function:").and_then(|idx| {
            let tail = &rest[idx + ":function:".len()..];
            let mut parts = tail.split(':');
            parts.next();
            parts.next().filter(|q| !q.is_empty()).map(String::from)
        })
    } else if name.starts_with("arn:") {
        None
    } else if let Some(idx) = name.find(':') {
        let q = &name[idx + 1..];
        (!q.is_empty()).then(|| q.to_string())
    } else {
        None
    };
    let qualifier = req.query_param("Qualifier").or(from_arn);
    let found = state
        .get_function_with_qualifier(name, qualifier.as_deref())
        .or_else(|| {
            // Alias fallback: alias name → its target function version.
            let alias_qual = qualifier.clone()?;
            let alias =
                state.resolve_alias(&crate::models::LambdaState::norm_name(name), &alias_qual)?;
            let alias_version = alias.function_version.read().clone();
            state.get_function_with_qualifier(name, Some(alias_version.as_str()))
        });
    found.ok_or_else(|| function_not_found(name, &req.region, req.account))
}
