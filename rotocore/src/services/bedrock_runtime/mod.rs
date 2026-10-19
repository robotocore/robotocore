//! AWS Bedrock Runtime backend — moto-equivalent (rest-json).
//!
//! Replaces the moto sidecar for `bedrock-runtime`. Request URIs are the
//! botocore rest-json templates (`POST /model/{modelId}/invoke`, …); the
//! gateway keeps no REST op table for this service, so — like module
//! `bedrock` — this module resolves the operation from the HTTP method +
//! requestUri.
//!
//! Routing note: every SDK call to bedrock-runtime is *signed* for the
//! `bedrock` credential scope (botocore's `signingName` for the service is
//! "bedrock" while its endpoint prefix is "bedrock-runtime"), so the gateway
//! names the service `bedrock` and lands on this module's dispatch chain
//! first. The runtime requestUris (`/model/…`, `/async-invoke…`,
//! `/guardrail/{id}/version/{v}/apply`) are disjoint from the
//! model-management paths of module `bedrock`, so a template hit here is
//! unambiguous and a miss falls through to the `bedrock` module.
//!
//! Contract: tests/compatibility/test_bedrock_runtime_compat.py, source
//! `moto/bedrockruntime/{models,responses}.py` (moto 5.1.23 is identical to
//! the deployed fork `2016563fd`). The fork implements exactly one handler:
//! **InvokeModel** — json-parse the request body when Content-Type is
//! exactly `application/json` (an unparseable body crashes moto, surfaced by
//! the sidecar as a 500 `InternalError`), run the stateless backend (which
//! returns an empty inference result), and serialize `InvokeModelOutput`
//! with `body` as the payload blob (`"{}"`), plus the `Content-Type`,
//! `X-Amzn-Bedrock-PerformanceConfig-Latency` (echo or `standard`) and
//! `X-Amzn-Bedrock-Service-Tier` (echo or `default`) headers. Every other
//! bedrock-runtime operation is outside the fork's response surface and
//! answers the sidecar-era 501 `NotImplemented`; paths matching no botocore
//! requestUri answer moto's 404 with an empty body. State: the fork's
//! `BedrockRuntimeBackend` is stateless, so this module keeps none.

use crate::proto::json_error;
use crate::{Request, Response};
use bytes::Bytes;
use serde_json::Value;

const APPLICATION_JSON: &str = "application/json";

// ---------------------------------------------------------------------------
// botocore rest-json requestUri table (method, template, operation)
// verified against the deployed fork via `_get_method_urls("bedrock-runtime")`
// ---------------------------------------------------------------------------

static RULES: &[(&str, &str, &str)] = &[
    (
        "POST",
        "/guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply",
        "ApplyGuardrail",
    ),
    ("POST", "/model/{modelId}/converse", "Converse"),
    ("POST", "/model/{modelId}/converse-stream", "ConverseStream"),
    ("POST", "/model/{modelId}/count-tokens", "CountTokens"),
    (
        "POST",
        "/model/{modelId}/invoke-with-bidirectional-stream",
        "InvokeModelWithBidirectionalStream",
    ),
    (
        "POST",
        "/model/{modelId}/invoke-with-response-stream",
        "InvokeModelWithResponseStream",
    ),
    ("POST", "/model/{modelId}/invoke", "InvokeModel"),
    ("GET", "/async-invoke", "ListAsyncInvokes"),
    ("POST", "/async-invoke", "StartAsyncInvoke"),
    ("GET", "/async-invoke/{invocationArn}", "GetAsyncInvoke"),
];

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    // Called from the `bedrock` arm (shared credential scope) only after the
    // caller confirmed the service name, and directly from the
    // `bedrock-runtime` arm for endpoint-prefix addressing.
    if req.service != "bedrock" && req.service != "bedrock-runtime" {
        return None;
    }
    Some(match wire_operation(req) {
        // POST /model/{modelId}/invoke — the fork's only implemented handler.
        Some("InvokeModel") => op_invoke_model(req),
        // Routed paths outside the fork's response surface: moto's
        // `NotImplementedError` → the sidecar's 501 NotImplemented.
        Some(op) => not_implemented(op),
        // No botocore requestUri matches: moto answers 404 with an empty
        // body (`if not action: return 404`). On the shared `bedrock` scope
        // that means the path belongs to the model-management API — defer.
        None if req.service == "bedrock-runtime" => unknown_path(),
        None => return None,
    })
}

fn wire_operation(req: &Request) -> Option<&'static str> {
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    let segs: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && template_matches(uri, &segs))
        .map(|(_, _, op)| *op)
}

fn template_matches(uri: &str, segs: &[&str]) -> bool {
    let tpl: Vec<&str> = uri
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    tpl.len() == segs.len()
        && tpl
            .iter()
            .zip(segs)
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}')) || *t == *s)
}

// ---------------------------------------------------------------------------
// wire helpers
// ---------------------------------------------------------------------------

/// moto's `camelcase_to_underscores` for the botocore operation names
/// (`GetAsyncInvoke` → `get_async_invoke`); every bedrock-runtime op is
/// plain PascalCase, so a cap boundary is always a word boundary.
fn snake_case(op: &str) -> String {
    let mut out = String::with_capacity(op.len() + 4);
    for (i, c) in op.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(c.to_lowercase());
    }
    out
}

/// Sidecar shape for moto's `NotImplementedError` (501).
fn not_implemented(op: &str) -> Response {
    json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake_case(op)),
    )
}

/// moto's `if not action: return 404, headers, ""`.
fn unknown_path() -> Response {
    Response::new(404, "text/plain", Bytes::new())
}

/// The fork's `BedrockRuntimeResponse.invoke_model` +
/// `BedrockRuntimeBackend.invoke_model` (which asserts its inputs and
/// returns an empty inference result — there is no inference simulation in
/// moto, so everyone receives `{}` as the model output).
fn op_invoke_model(req: &Request) -> Response {
    // `_get_param("contentType", APPLICATION_JSON)` — header members land in
    // the parsed params; an absent header falls back to json.
    let content_type = req
        .headers
        .get("content-type")
        .map(String::as_str)
        .unwrap_or(APPLICATION_JSON);
    // `_get_param("accept", APPLICATION_JSON)` decides json.dumps vs raw-dict
    // passthrough of the empty inference result in moto; both serialize to
    // the same `{}` here, keep both branches for wire-shape honesty.
    let accept = req
        .headers
        .get("accept")
        .map(String::as_str)
        .unwrap_or(APPLICATION_JSON);

    let perf_latency = req
        .headers
        .get("x-amzn-bedrock-performanceconfig-latency")
        .map(String::as_str)
        .unwrap_or("standard");
    let service_tier = req
        .headers
        .get("x-amzn-bedrock-service-tier")
        .map(String::as_str)
        .unwrap_or("default");

    if content_type == APPLICATION_JSON {
        // moto: payload = json.loads(payload); an unparseable body raises an
        // unhandled JSONDecodeError → moto_server 500 → the sidecar's
        // `InternalError` translation. A JSON `null` payload trips the
        // backend's `assert payload is not None` the same way.
        match serde_json::from_slice::<Value>(&req.body) {
            Ok(Value::Null) => {
                return json_error(500, "InternalError", "AssertionError: payload is None")
            }
            Err(e) => return json_error(500, "InternalError", &e.to_string()),
            Ok(_) => {}
        }
    }

    // backend returns `inference_result = {}`; accept==json sends
    // `json.dumps({})`, any other accept hands the (empty) dict to the rest
    // serializer which yields the same `{}`. The hardcoded
    // "application/json" contentType member wins the wire Content-Type
    // on both paths.
    let body: Vec<u8> = if accept == APPLICATION_JSON {
        b"{}".to_vec()
    } else {
        serde_json::to_vec(&Value::Object(Default::default())).unwrap_or_default()
    };
    Response {
        status: 200,
        headers: vec![
            ("Content-Type".to_string(), APPLICATION_JSON.to_string()),
            (
                "X-Amzn-Bedrock-PerformanceConfig-Latency".to_string(),
                perf_latency.to_string(),
            ),
            (
                "X-Amzn-Bedrock-Service-Tier".to_string(),
                service_tier.to_string(),
            ),
        ],
        body,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::url_decode;
    use std::collections::HashMap;

    fn request(service: &str, method: &str, path: &str, body: &[u8]) -> Request {
        Request {
            service: service.to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::copy_from_slice(body),
            account: 123456789012,
            region: "us-east-1".to_string(),
        }
    }

    fn header(req: &Request, name: &str, value: &str) -> Request {
        let mut r = req.clone();
        r.headers.insert(name.to_string(), value.to_string());
        r
    }

    fn status_of(req: &Request) -> Response {
        dispatch(req).expect("bedrock-runtime dispatch answered")
    }

    #[test]
    fn invoke_model_returns_empty_inference_json() {
        // The compat-suite happy path: 200, "{}" body, json content type and
        // moto's default perf/tier token echoes.
        let resp = status_of(&request(
            "bedrock",
            "POST",
            "/model/amazon.titan-text-lite-v1/invoke",
            br#"{"inputText": "Hello world"}"#,
        ));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"{}");
        assert_eq!(single(&resp, "content-type"), "application/json");
        assert_eq!(
            single(&resp, "x-amzn-bedrock-performanceconfig-latency"),
            "standard"
        );
        assert_eq!(single(&resp, "x-amzn-bedrock-service-tier"), "default");
    }

    #[test]
    fn invoke_model_echoes_performance_config_and_tier() {
        let base = request("bedrock", "POST", "/model/m1/invoke", b"{}");
        let resp = status_of(&header(
            &header(
                &base,
                "x-amzn-bedrock-performanceconfig-latency",
                "optimized",
            ),
            "x-amzn-bedrock-service-tier",
            "flex",
        ));
        assert_eq!(resp.status, 200);
        assert_eq!(
            single(&resp, "x-amzn-bedrock-performanceconfig-latency"),
            "optimized"
        );
        assert_eq!(single(&resp, "x-amzn-bedrock-service-tier"), "flex");
    }

    #[test]
    fn invoke_model_without_content_type_still_answers_json() {
        // botocore sends no Content-Type for a blob payload unless asked;
        // moto's default parses the body as JSON and answers "{}".
        let resp = status_of(&request("bedrock", "POST", "/model/m1/invoke", b"{}"));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"{}");
    }

    #[test]
    fn invoke_model_invalid_json_body_is_moto_500() {
        // moto's json.loads raises -> unhandled exception -> 500 InternalError.
        let resp = status_of(&header(
            &request("bedrock", "POST", "/model/m1/invoke", b"not json"),
            "content-type",
            "application/json",
        ));
        assert_eq!(resp.status, 500);
        assert_eq!(single(&resp, "x-amzn-errortype"), "InternalError:");
    }

    #[test]
    fn invoke_model_null_payload_trips_backend_assert() {
        let resp = status_of(&request("bedrock", "POST", "/model/m1/invoke", b"null"));
        assert_eq!(resp.status, 500);
    }

    #[test]
    fn other_routed_ops_answer_moto_501() {
        for (method, path, op) in [
            (
                "GET",
                "/async-invoke/arn%3Aaws%3Abedrock%3Aus-east-1%3A1%3Aasync-invoke%2Ffake",
                "get_async_invoke",
            ),
            ("POST", "/model/m1/converse", "converse"),
            ("POST", "/model/m1/converse-stream", "converse_stream"),
            (
                "POST",
                "/model/m1/invoke-with-response-stream",
                "invoke_model_with_response_stream",
            ),
            ("POST", "/async-invoke", "start_async_invoke"),
            ("GET", "/async-invoke", "list_async_invokes"),
            (
                "POST",
                "/guardrail/g1/version/DRAFT/apply",
                "apply_guardrail",
            ),
            ("POST", "/model/m1/count-tokens", "count_tokens"),
        ] {
            let resp = status_of(&request("bedrock", method, path, b"{}"));
            assert_eq!(resp.status, 501, "{method} {path}");
            let body = String::from_utf8_lossy(&resp.body);
            assert_eq!(
                body,
                format!(
                    r#"{{"__type":"NotImplemented","message":"The {op} action has not been implemented"}}"#
                ),
                "{path}"
            );
            assert_eq!(single(&resp, "x-amzn-errortype"), "NotImplemented:");
        }
    }

    #[test]
    fn unmatching_paths_fall_through_on_the_bedrock_scope() {
        // Non-runtime paths defer to the model-management module.
        assert!(dispatch(&request("bedrock", "GET", "/foundation-models", b"")).is_none());
        // ...and direct bedrock-runtime traffic answers moto's empty 404.
        let resp = status_of(&request(
            "bedrock-runtime",
            "POST",
            "/model/m1/not-an-operation",
            b"{}",
        ));
        assert_eq!(resp.status, 404);
        assert!(resp.body.is_empty());
    }

    #[test]
    fn method_mismatch_falls_through_or_404s() {
        // GET on a POST-only runtime path: no template → not ours on the
        // shared scope.
        assert!(dispatch(&request("bedrock", "GET", "/model/m1/invoke", b"")).is_none());
        let resp = status_of(&request("bedrock-runtime", "GET", "/model/m1/invoke", b""));
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn invocation_arn_path_resolves_single_encoded_segment() {
        // botocore percent-encodes the ARN into one segment; matching is on
        // the raw segment, decoding happens only for the extracted arg.
        let path = "/async-invoke/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Aasync-invoke%2Ffake-id-12345";
        assert_eq!(
            wire_operation(&request("bedrock", "GET", path, b"")),
            Some("GetAsyncInvoke")
        );
        assert_eq!(
            url_decode(path.split('/').nth_back(0).unwrap_or("")),
            "arn:aws:bedrock:us-east-1:123456789012:async-invoke/fake-id-12345"
        );
    }

    #[test]
    fn other_services_are_not_ours() {
        assert!(dispatch(&request("iam", "POST", "/model/m1/invoke", b"{}")).is_none());
        assert!(dispatch(&request("sqs", "GET", "/async-invoke", b"")).is_none());
    }

    fn single(resp: &Response, name: &str) -> String {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .expect("header present")
    }
}
