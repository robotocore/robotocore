//! API Gateway execution bridge for deployed rest APIs.
//!
//! The python ground truth serves `GET /restapis/{api}/{stage}/_user_request_/{path}`
//! with its own executor (`src/robotocore/services/apigateway/executor.py`): moto
//! only stores the API config. The sidecar's moto routing has no such route, so
//! the rust gateway mirrors the ground-truth behavior here: read the stored
//! integration from the bridge sidecar, then dispatch AWS_PROXY integrations to
//! the native lambda crate.

use axum::http::{HeaderMap, StatusCode};
use http::Method;
use serde_json::{json, Value};

use crate::core::server::AppState;

/// Parse `/restapis/{api}/{stage}/_user_request_/{path}`.
pub fn match_execute(path: &str) -> Option<(String, String, String)> {
    let rest = path.strip_prefix("/restapis/")?;
    let (api_id, rest) = rest.split_once('/')?;
    let (stage, proxy_path) = rest.split_once('/')?;
    let proxy_path = proxy_path.strip_prefix("_user_request_/")?;
    Some((
        api_id.to_string(),
        stage.to_string(),
        proxy_path.to_string(),
    ))
}

/// Execution targets parsed by [`match_execute`].
pub struct ExecuteTarget<'a> {
    pub api_id: &'a str,
    pub stage: &'a str,
    pub proxy_path: &'a str,
}

/// Execute one deployed API request; returns the wire response.
pub async fn execute(
    proxy: &crate::core::proxy::MotoProxy,
    lambda: &crate::core::server::NativeHandlers,
    method: &Method,
    target: ExecuteTarget<'_>,
    account: u64,
    region: &str,
) -> axum::response::Response {
    let ExecuteTarget {
        api_id,
        stage,
        proxy_path,
    } = target;
    use axum::response::IntoResponse;

    let json_response = |status: u16, message: &str| -> axum::response::Response {
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::NOT_FOUND),
            axum::Json(json!({"message": message})),
        )
            .into_response()
    };

    // The stage must exist (deployed); only then do we execute.
    if proxy
        .sidecar_get(&format!("/restapis/{api_id}/stages/{stage}"), region)
        .await
        .map(|r| r.status)
        .unwrap_or(StatusCode::BAD_REQUEST)
        != StatusCode::OK
    {
        return json_response(404, "Not Found");
    }

    // Locate the resource matching the proxy path (exact first, then templates).
    let resources: Vec<Value> = match proxy
        .sidecar_get(&format!("/restapis/{api_id}/resources"), region)
        .await
    {
        Ok(resp) if resp.status == StatusCode::OK => resp
            .body
            .get("item")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => return json_response(404, "Not Found"),
    };

    let want_path = format!("/{proxy_path}");
    let matched: Option<&Value> = resources
        .iter()
        .find(|r| r.get("path").and_then(Value::as_str) == Some(want_path.as_str()))
        .or_else(|| {
            resources.iter().find(|r| {
                let Some(pattern) = r.get("path").and_then(Value::as_str) else {
                    return false;
                };
                template_matches(pattern, &want_path)
            })
        });
    let Some(resource) = matched else {
        return json_response(404, "Not Found");
    };
    let resource_id = resource
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // Configured method on the resource?
    let integration = proxy
        .sidecar_get(
            &format!(
                "/restapis/{api_id}/resources/{resource_id}/methods/{}/integration",
                method.as_str()
            ),
            region,
        )
        .await;
    let Ok(integration) = integration else {
        return json_response(404, "Not Found");
    };
    if integration.status != StatusCode::OK {
        return json_response(403, "Missing Authentication Token or Method Not Defined");
    }
    let integration_type = integration
        .body
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("AWS_PROXY")
        .to_string();
    let uri = integration
        .body
        .get("uri")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    match integration_type.as_str() {
        "AWS_PROXY" | "AWS" | "LAMBDA_PROXY" => {
            let Some(function_name) = lambda_arn_from_integration(&uri) else {
                return json_response(500, "API Configuration Error");
            };
            let event = proxy_event(method.as_str(), &want_path, region, account, proxy_path);
            crate::core::server::NativeHandlers::invoke_lambda(
                lambda,
                account,
                region,
                &function_name,
                &event,
            )
        }
        "MOCK" => (StatusCode::OK, axum::Json(json!({}))).into_response(),
        _ => json_response(501, "Not Implemented"),
    }
}

/// True when a templated resource path (`/{param+}`, `/{param}`) covers `path`.
fn template_matches(pattern: &str, path: &str) -> bool {
    let mut p = pattern.split('/');
    let mut q = path.split('/');
    loop {
        match (p.next(), q.next()) {
            (None, None) => return true,
            (Some(seg), None) => return seg.contains('{'),
            (Some(seg), Some(actual)) => {
                if seg.starts_with('{') {
                    continue;
                }
                if seg != actual {
                    return false;
                }
            }
            (None, Some(_)) => return false,
        }
    }
}

/// Lambda function name from a proxy integration uri
/// (`...lambda:path/2015-03-31/functions/<function arn>/invocations`).
fn lambda_arn_from_integration(uri: &str) -> Option<String> {
    let marker = "/functions/";
    let start = uri.find(marker)? + marker.len();
    let rest = &uri[start..];
    let end = rest.find("/invocations")?;
    let fn_arn = &rest[..end];
    // arn:aws:lambda:<region>:<acct>:function:<name>
    let name = fn_arn.rsplit(':').next()?;
    Some(name.to_string())
}

/// Build the API Gateway proxy event passed to the function.
fn proxy_event(method: &str, path: &str, region: &str, account: u64, proxy_path: &str) -> Value {
    json!({
        "resource": proxy_path,
        "path": path,
        "httpMethod": method.to_uppercase(),
        "headers": {},
        "queryStringParameters": Value::Null,
        "pathParameters": Value::Null,
        "stageVariables": Value::Null,
        "requestContext": {
            "resourceId": "proxy",
            "resourcePath": path,
            "httpMethod": method.to_uppercase(),
            "path": path,
            "stage": "test",
            "identity": {"accountId": account.to_string()},
        },
        "isBase64Encoded": false,
        "_region": region,
    })
}

/// Adjusted `AppState` accessors live here to keep server.rs focused.
impl AppState {
    /// The moto sidecar config source, when wired.
    pub fn proxy_provider(&self) -> Option<&crate::core::proxy::MotoProxy> {
        self.moto_proxy.as_ref()
    }
}

/// Silence unused warnings until handlers route through (see server.rs).
#[allow(dead_code)]
fn ensure_imports(_: &HeaderMap) {}
