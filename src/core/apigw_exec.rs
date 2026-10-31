//! API Gateway execution bridge for deployed rest APIs.
//!
//! The python ground truth serves `GET /restapis/{api}/{stage}/_user_request_/{path}`
//! with its own executor (`src/robotocore/services/apigateway/executor.py`): moto
//! only stores the API config. The rust gateway answers these here by asking the
//! native apigateway backend (`rotocore::services::apigateway::resolve_execution`)
//! for a directive — lambda-proxy events go to the native lambda crate, mock
//! integrations answer `{}`, and everything else returns the executor's error
//! shapes (404/405/500/501).

use axum::http::{Method, StatusCode};
use serde_json::json;

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
///
/// Config comes from the in-process apigateway backend (the py executor's
/// semantics: ANY-method fallback, `{param+}` path parameters, clientId-less
/// authorizer pass-through), then AWS_PROXY integrations invoke the native
/// lambda crate and mock integrations answer `{}`. Synchronous by
/// construction (nothing inside awaits) — callers run it on the blocking
/// pool so the in-process lambda invoke never pins an async worker.
#[allow(clippy::too_many_arguments)]
pub fn execute(
    lambda: &crate::core::server::NativeHandlers,
    method: &Method,
    target: ExecuteTarget<'_>,
    account: u64,
    region: &str,
    query_string: &str,
    headers: &std::collections::HashMap<String, String>,
    body_bytes: &[u8],
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let json_response = |status: u16, message: &str| -> axum::response::Response {
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::NOT_FOUND),
            axum::Json(json!({"message": message})),
        )
            .into_response()
    };

    let directive = rotocore::services::apigateway::resolve_execution(
        account,
        region,
        target.api_id,
        target.stage,
        method.as_str(),
        target.proxy_path,
        query_string,
        headers,
        body_bytes,
    );

    if let Some((function_name, event)) = directive.lambda {
        return crate::core::server::NativeHandlers::invoke_lambda(
            lambda,
            account,
            region,
            &function_name,
            &event,
        );
    }
    if let Some(body) = directive.body {
        return (
            StatusCode::from_u16(directive.status).unwrap_or(StatusCode::OK),
            axum::Json(body),
        )
            .into_response();
    }
    match directive.message.as_deref() {
        Some(message) => json_response(directive.status, message),
        None => json_response(500, "API Configuration Error"),
    }
}
