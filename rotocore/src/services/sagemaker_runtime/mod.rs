//! AWS SageMaker Runtime backend — moto-equivalent (rest-json).
//!
//! Replaces the moto sidecar for `sagemaker-runtime` (source:
//! `moto/sagemakerruntime/{models,responses,urls}.py` in the deployed fork
//! `2016563fd`, identical to moto 5.1.23). The fork implements exactly two
//! handlers over one URL base (`runtime.sagemaker.<region>`):
//!
//! - **InvokeEndpoint** — `POST /endpoints/{name}/invocations`: static
//!   default data (`"body"` body, `"content_type"` content type,
//!   `invoked_production_variant` production variant, `custom_attributes`
//!   custom attributes), cached per request identity (all
//!   `x-amzn-sagemaker*` headers + `Accept` + body) mirroring moto's
//!   keyed map. The results queue that key rides on is only fillable
//!   through moto's `/moto-api/static/…` admin surface, which this
//!   backend does not serve — without it every branch of moto's backend
//!   yields the same static constants.
//! - **InvokeEndpointAsync** — `POST /endpoints/{name}/async-invocations`:
//!   remembers `output_location`/`failure_location` per (endpoint, input
//!   location), creates the `sagemaker-output-{uuid}` S3 bucket and puts
//!   `response.json` with the static payload `{"default": "response"}`
//!   through moto's in-process `s3` backend — mirrored here as a loopback
//!   write to this server's own S3 wire handlers.
//!
//! **InvokeEndpointWithResponseStream** is IN the botocore model
//! (`POST /endpoints/{name}/invocations-response-stream`) but has no route
//! and no handler anywhere in the fork's sagemaker/sagemakerruntime
//! modules, so it answers the fork-era 501 `NotImplemented`. Paths
//! matching no botocore requestUri under `/endpoints/` answer moto's 404
//! with an empty body (the fork's `urls.py` routes only the op paths).
//! State is keyed per (account, region).
//!
//! Routing note: botocore signs sagemaker-runtime requests for the
//! `sagemaker` credential scope (the signing name is shared with the
//! SageMaker model-management API), so the gateway labels these requests
//! `sagemaker` and they reach this module from the `sagemaker` arm first;
//! the runtime `/endpoints/…` requestUris are disjoint from the JSON
//! control plane (whose operation URIs are all `/`). A miss on a runtime
//! path never falls through — control-plane traffic always carries an
//! `X-Amz-Target` and never a runtime path, so this module answers all
//! `/endpoints/…` traffic itself, exactly like moto's URL table. The
//! `sagemaker-runtime` arm covers direct-addressed traffic.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use uuid::Uuid;

use crate::proto::json_error;
use crate::store;
use crate::{Request, Response};

// ---------------------------------------------------------------------------
// botocore rest-json requestUri table (method, template, operation)
// ---------------------------------------------------------------------------

static RULES: &[(&str, &str, &str)] = &[
    (
        "POST",
        "/endpoints/{EndpointName}/async-invocations",
        "InvokeEndpointAsync",
    ),
    (
        "POST",
        "/endpoints/{EndpointName}/invocations-response-stream",
        "InvokeEndpointWithResponseStream",
    ),
    (
        "POST",
        "/endpoints/{EndpointName}/invocations",
        "InvokeEndpoint",
    ),
];

static SLOT: Lazy<store::Slot<SagemakerRuntimeState>> =
    Lazy::new(|| store::slot::<SagemakerRuntimeState>("sagemaker-runtime"));

/// One cached InvokeEndpoint result — moto's
/// `(body, content_type, invoked_production_variant, custom_attributes)`
/// default tuple.
#[derive(Clone)]
struct SyncResult {
    body: Vec<u8>,
    content_type: String,
    variant: Option<String>,
    attrs: Option<String>,
}

impl Default for SyncResult {
    fn default() -> Self {
        SyncResult {
            body: b"body".to_vec(),
            content_type: "content_type".to_string(),
            variant: Some("invoked_production_variant".to_string()),
            attrs: Some("custom_attributes".to_string()),
        }
    }
}

#[derive(Clone)]
struct AsyncResult {
    output_location: String,
    failure_location: String,
}

#[derive(Default)]
struct SagemakerRuntimeState {
    /// InvokeEndpoint result cache — (endpoint name, request identity).
    results: HashMap<String, HashMap<String, SyncResult>>,
    /// InvokeEndpointAsync location cache — (endpoint name, input location).
    async_results: HashMap<String, HashMap<Option<String>, AsyncResult>>,
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "sagemaker" && req.service != "sagemaker-runtime" {
        return None;
    }
    // `/endpoints/…` never belongs to the JSON control plane (whose op
    // URIs are all `/`), and moto routes `sagemaker-runtime` traffic by
    // URL first — under this prefix the runtime API is the only candidate.
    if !req.path.starts_with("/endpoints/") {
        return None;
    }
    Some(match wire_operation(req) {
        Some("InvokeEndpoint") => op_invoke_endpoint(req),
        Some("InvokeEndpointAsync") => op_invoke_endpoint_async(req),
        // Routed paths outside the fork's response surface: moto's
        // `NotImplementedError` → the sidecar's 501 NotImplemented.
        Some(op) => not_implemented(op),
        // No botocore requestUri matches: moto answers 404 with an empty
        // body (`if not action: return 404`).
        None => unknown_path(),
    })
}

fn wire_operation(req: &Request) -> Option<&'static str> {
    let segs: Vec<&str> = req
        .path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && template_matches(uri, &segs))
        .map(|(_, _, op)| *op)
}

/// moto's URL routing matches `[^/]+` for `{EndpointName}` and anchors the
/// op path at the end — an empty name segment misses the regex (404).
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
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}') && !s.is_empty()) || *t == *s)
}

/// The endpoint name, taken from the raw URL segment like the moto
/// responses do (`self.path.split("/")[2]` / the url regex group).
///
/// moto splits the *raw* path, whose leading "/" keeps index 2 on the
/// name segment — ["", "endpoints", name, op].
fn endpoint_name(req: &Request) -> String {
    req.path.split('/').nth(2).unwrap_or_default().to_string()
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

/// moto's `SageMakerRuntimeResponse.invoke_endpoint` response identity:
/// every `x-amzn-sagemaker*` header plus `Accept` and the body.
fn request_identity_key(req: &Request) -> String {
    let mut parts: Vec<String> = req
        .headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amzn-sagemaker"))
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    parts.sort();
    parts.insert(
        0,
        format!(
            "accept={}",
            req.headers.get("accept").map(String::as_str).unwrap_or("")
        ),
    );
    parts.push(format!("body={}", B64.encode(&req.body)));
    parts.join("\n")
}

fn op_invoke_endpoint(req: &Request) -> Response {
    let endpoint_name = endpoint_name(req);
    let key = request_identity_key(req);

    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    let cache = st.results.entry(endpoint_name).or_default();
    let value = match cache.get(&key) {
        Some(v) => v.clone(),
        None => {
            let v = SyncResult::default();
            cache.insert(key, v.clone());
            v
        }
    };
    drop(guard);

    let mut headers = vec![("Content-Type".to_string(), value.content_type)];
    if let Some(variant) = value.variant {
        headers.push(("x-Amzn-Invoked-Production-Variant".to_string(), variant));
    }
    if let Some(attrs) = value.attrs {
        headers.push(("X-Amzn-SageMaker-Custom-Attributes".to_string(), attrs));
    }
    Response {
        status: 200,
        headers,
        body: value.body,
    }
}

/// moto's `invoke_endpoint_async` response + backend. The `InferenceId`
/// body member is NOT part of the cached locations (moto rebuilds it per
/// call); locations are remembered per input location so a repeat hits
/// the no-mutation cache path, exactly like moto's keyed map.
fn op_invoke_endpoint_async(req: &Request) -> Response {
    let endpoint_name = endpoint_name(req);
    let input_location = req.headers.get("x-amzn-sagemaker-inputlocation").cloned();
    let inference_id = req
        .headers
        .get("x-amzn-sagemaker-inference-id")
        .filter(|v| !v.is_empty())
        .cloned();

    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    let cache = st.async_results.entry(endpoint_name).or_default();
    let results = match cache.get(&input_location) {
        Some(r) => r.clone(),
        None => {
            // moto stores the payload via its in-process s3 backend, with
            // `json.dumps({"default": "response"})` as the static data.
            // The queue-configurable variants are out of scope (see module
            // doc): without them this is the only reachable branch.
            let data = "{\"default\": \"response\"}";
            let bucket = format!("sagemaker-output-{}", Uuid::new_v4());
            // Bucket first, object second — both through the real local S3
            // wire handlers, the same store an SDK client sees afterwards.
            let _ = s3_put(req, &bucket, b"");
            let _ = s3_put(req, &format!("{bucket}/response.json"), data.as_bytes());
            let r = AsyncResult {
                output_location: format!("s3://{bucket}/response.json"),
                failure_location: format!("s3://{bucket}/failure.json"),
            };
            cache.insert(input_location, r.clone());
            r
        }
    };
    drop(guard);

    // `json.dumps({"InferenceId": id})` — json's `", "` separators verbatim.
    let id = inference_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let id_json = serde_json::to_string(&id).unwrap_or_else(|_| format!("\"{id}\""));
    Response {
        status: 200,
        headers: vec![
            (
                "X-Amzn-SageMaker-OutputLocation".to_string(),
                results.output_location,
            ),
            (
                "X-Amzn-SageMaker-FailureLocation".to_string(),
                results.failure_location,
            ),
        ],
        body: format!("{{\"InferenceId\": {id_json}}}").into_bytes(),
    }
}

// ---------------------------------------------------------------------------
// wire helpers
// ---------------------------------------------------------------------------

/// Loopback mirror of moto's cross-backend S3 write: PUT through this
/// server's own S3 wire handler so the bucket/object live in the same
/// store an SDK client reads. Credentials are engine-internal shape (the
/// wire handlers only inspect the credential scope and account headers).
fn s3_put(req: &Request, path: &str, body: &[u8]) -> bool {
    // Mirror to the sender's own host (the local server); unit tests send
    // no Host header, so the fallback is a guaranteed-closed port.
    let host = req
        .headers
        .get("host")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:1".to_string());
    let url = format!("http://{host}/{path}");
    let account = req.account.to_string();
    let scope = format!(
        "AWS4-HMAC-SHA256 Credential={account}/20260101/{region}/s3/aws4_request",
        region = req.region
    );
    let req = agent()
        .put(&url)
        .set("Authorization", &scope)
        .set("X-Amz-Date", "20260101T000000Z")
        .set("X-Robotocore-Account", &account)
        .set("X-Robotocore-Region", &req.region)
        // Loopback calls are one-shot; keep-alive pooling wedges axum's
        // blocking-thread interplay here.
        .set("Connection", "close");
    matches!(req.send_bytes(body), Ok(resp) if resp.status() == 200)
}

fn agent() -> ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT
        .get_or_init(|| {
            ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(5))
                .timeout_read(Duration::from_secs(20))
                .build()
        })
        .clone()
}

/// moto's `camelcase_to_underscores` for botocore operation names
/// (`InvokeEndpointWithResponseStream` → `invoke_endpoint_with_response_stream`).
fn snake_case(op: &str) -> String {
    let mut out = String::with_capacity(op.len() + 6);
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

/// moto's `if not action: return 404` with an empty body.
fn unknown_path() -> Response {
    Response::new(404, "text/plain", Bytes::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn request(service: &str, method: &str, path: &str, body: &[u8]) -> Request {
        Request {
            service: service.to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            // No Host → the s3_put loopback targets a closed port, so unit
            // tests run offline and never touch a live server.
            headers: HashMap::new(),
            body: Bytes::copy_from_slice(body),
            account: 123456789012,
            region: "us-east-1".to_string(),
        }
    }

    fn status_of(req: &Request) -> Response {
        dispatch(req).expect("sagemaker-runtime dispatch answered")
    }

    fn invoke(endpoint: &str) -> Response {
        status_of(&request(
            "sagemaker",
            "POST",
            &format!("/endpoints/{endpoint}/invocations"),
            b"{\"input\": \"x\"}",
        ))
    }

    fn get_header(resp: &Response, name: &str) -> String {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .expect("header present")
    }

    #[test]
    fn invoke_endpoint_returns_moto_static_default() {
        // Per-endpoint state: each fresh endpoint seeds its own slot with
        // moto's constants.
        let suffix = Uuid::new_v4().to_string();
        let resp = invoke(&format!("static-{suffix}"));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"body");
        assert_eq!(get_header(&resp, "content-type"), "content_type");
        assert_eq!(
            get_header(&resp, "x-Amzn-Invoked-Production-Variant"),
            "invoked_production_variant"
        );
        assert_eq!(
            get_header(&resp, "x-amzn-sagemaker-custom-attributes"),
            "custom_attributes"
        );
    }

    #[test]
    fn invoke_endpoint_same_identity_is_cached() {
        let endpoint = format!("cache-{}", Uuid::new_v4());
        let first = invoke(&endpoint);
        let second = invoke(&endpoint);
        assert_eq!(first.body, second.body);
        assert_eq!(first.headers, second.headers);
    }

    #[test]
    fn stream_op_answers_moto_not_implemented() {
        let resp = status_of(&request(
            "sagemaker",
            "POST",
            "/endpoints/ep/invocations-response-stream",
            b"{}",
        ));
        assert_eq!(resp.status, 501);
        let body = String::from_utf8_lossy(&resp.body);
        assert_eq!(
            body,
            r#"{"__type":"NotImplemented","message":"The invoke_endpoint_with_response_stream action has not been implemented"}"#
        );
    }

    #[test]
    fn unmatched_runtime_paths_answer_moto_404() {
        let resp = status_of(&request(
            "sagemaker",
            "POST",
            "/endpoints/ep/not-a-runtime-operation",
            b"{}",
        ));
        assert_eq!(resp.status, 404);
        assert!(resp.body.is_empty());
        // An empty name segment misses moto's `[^/]+` regex too.
        let resp = status_of(&request(
            "sagemaker",
            "POST",
            "/endpoints//invocations",
            b"{}",
        ));
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn control_plane_traffic_defers() {
        // Target requests and paths outside the runtime prefix belong to
        // the SageMaker model-management arm.
        let mut req = request("sagemaker", "POST", "/", b"{}");
        req.headers.insert(
            "x-amz-target".to_string(),
            "SageMaker.DescribeEndpoint".to_string(),
        );
        assert!(dispatch(&req).is_none());
        assert!(dispatch(&request("sagemaker", "POST", "/", b"{}")).is_none());
        assert!(dispatch(&request("iam", "POST", "/endpoints/ep/invocations", b"{}")).is_none());
    }

    #[test]
    fn async_locations_resolve_offline() {
        // Without a live S3 handler the mirror is a best-effort no-op; the
        // moto contract (locations + InferenceId JSON) still holds.
        let endpoint = format!("async-{}", Uuid::new_v4());
        let resp = status_of(&request(
            "sagemaker",
            "POST",
            &format!("/endpoints/{endpoint}/async-invocations"),
            b"",
        ));
        assert_eq!(resp.status, 200);
        let out = get_header(&resp, "x-amzn-sagemaker-outputlocation");
        let fail = get_header(&resp, "x-amzn-sagemaker-failurelocation");
        assert!(out.starts_with("s3://sagemaker-output-"));
        assert!(out.ends_with("/response.json"));
        assert!(fail.ends_with("/failure.json"));
        assert!(!String::from_utf8_lossy(&resp.body)
            .trim_start_matches("{\"InferenceId\": \"")
            .is_empty());
    }

    #[test]
    fn async_repeated_input_location_hits_cache() {
        let endpoint = format!("async-dedup-{}", Uuid::new_v4());
        let mut req = request(
            "sagemaker",
            "POST",
            &format!("/endpoints/{endpoint}/async-invocations"),
            b"",
        );
        req.headers.insert(
            "x-amzn-sagemaker-inputlocation".to_string(),
            "s3://in/data.json".to_string(),
        );
        let first = status_of(&req);
        let second = status_of(&req);
        assert_eq!(
            get_header(&first, "x-amzn-sagemaker-outputlocation"),
            get_header(&second, "x-amzn-sagemaker-outputlocation")
        );
        // A different endpoint owns a separate cache.
        let mut other = req.clone();
        other.path = other.path.replace(&endpoint, &format!("{endpoint}-2"));
        let other_resp = status_of(&other);
        assert_ne!(
            get_header(&first, "x-amzn-sagemaker-outputlocation"),
            get_header(&other_resp, "x-amzn-sagemaker-outputlocation")
        );
    }

    #[test]
    fn async_inference_id_echoes_when_present() {
        let endpoint = format!("async-id-{}", Uuid::new_v4());
        let mut req = request(
            "sagemaker",
            "POST",
            &format!("/endpoints/{endpoint}/async-invocations"),
            b"",
        );
        req.headers.insert(
            "x-amzn-sagemaker-inference-id".to_string(),
            "my-inference".to_string(),
        );
        let resp = status_of(&req);
        assert_eq!(
            String::from_utf8_lossy(&resp.body),
            "{\"InferenceId\": \"my-inference\"}"
        );
    }
}
