//! AWS Kinesis Video Streams Archived Media backend (REST-JSON, one URI
//! per operation).
//!
//! Port of the moto fork's `kinesisvideoarchivedmedia` backend: HLS/DASH
//! streaming session URLs and the fixed mp4 `GetClip` payload, resolved
//! against the streams stored in the `kinesisvideo` backend state. Wire
//! shapes carry the botocore member names verbatim (PascalCase keys).
//!
//! Archived-media requests sign with the same `kinesisvideo` credential
//! scope as the control plane (both botocore clients share the signing
//! name), so the gateway labels them `kinesisvideo` and the
//! `services::dispatch` `kinesisvideo` arm routes here first — the URI
//! families are disjoint, and this module declines anything else.
//!
//! Kept-faithful moto quirks:
//! - Streaming session URLs ride the stream's `b-` data endpoint plus a
//!   fixed relative path and a moto-shaped `SessionToken` (moto's
//!   `random_session_token`: fixed `FQoGZXIvYXdzEBYaD` prefix followed by
//!   the tail of a base64 blob).
//! - `GetClip` ignores every parameter and answers the fixed
//!   `sample-mp4-video` payload with `Content-Type: video/mp4` (the
//!   botocore model binds `ContentType` to the HTTP header).
//! - A missing stream renders moto's default `ResourceNotFoundException`
//!   (HTTP 404, fixed message).
//! - Three botocore operations (`GetImages`, `GetMediaForFragmentList`,
//!   `ListFragments`) have no method in moto's archived-media responses;
//!   the module's catch-all still routes them there, raising moto's
//!   `NotImplemented` template over the snake_cased action name. Mirrored
//!   verbatim (HTTP 501) below.

use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({"__type": code, "message": message});
    let mut resp = Response::json(status, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

/// moto's `ResourceNotFoundException` (HTTP 404) with the fork's default
/// message.
fn stream_not_found() -> Response {
    error(
        404,
        "ResourceNotFoundException",
        "The requested stream is not found or not active.",
    )
}

/// moto's `NotImplemented` template for a catch-all-routed operation with
/// no handler method: the action name is the snake_case conversion moto's
/// `call_action` derived from the botocore op name.
fn not_implemented(action: &str) -> Response {
    error(
        501,
        "NotImplemented",
        &format!("The {action} action has not been implemented"),
    )
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

/// moto's `random_session_token`: `SESSION_TOKEN_PREFIX` plus the tail of
/// a base64 blob encoded from 266 random bytes (the slice removes exactly
/// the prefix length, keeping the 356-character token).
fn session_token() -> String {
    const PREFIX: &str = "FQoGZXIvYXdzEBYaD";
    let mut raw = Vec::with_capacity(272);
    for _ in 0..17 {
        raw.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    raw.truncate(266);
    let encoded = B64.encode(&raw);
    format!("{PREFIX}{}", &encoded[PREFIX.len()..])
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Botocore operations moto's archived-media module routes (its catch-all
/// url) but never implements: `(requestUri segment, moto's snake_case
/// action name)`. Answered with moto's verbatim 501 template.
const ABSENT_ACTIONS: &[(&str, &str)] = &[
    ("getImages", "get_images"),
    ("getMediaForFragmentList", "get_media_for_fragment_list"),
    ("listFragments", "list_fragments"),
];

pub fn dispatch(req: &Request) -> Option<Response> {
    // The gateway labels these requests `kinesisvideo` (the shared signing
    // scope); the registry name routes too, for callers that already know
    // the archived-media service id.
    if !matches!(
        req.service.as_str(),
        "kinesisvideo" | "kinesis-video-archived-media"
    ) {
        return None;
    }
    // Every operation rides on POST to its own URI; anything else (GET,
    // unknown paths, the media-plane URIs the control plane's backend
    // declines) stays unowned.
    if req.method != "POST" {
        return None;
    }
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    match path {
        "getClip" => Some(get_clip(req)),
        "getHLSStreamingSessionURL" => Some(streaming_session_url(
            req,
            "GET_HLS_STREAMING_SESSION_URL",
            "/hls/v1/getHLSMasterPlaylist.m3u8",
            "HLSStreamingSessionURL",
        )),
        "getDASHStreamingSessionURL" => Some(streaming_session_url(
            req,
            "GET_DASH_STREAMING_SESSION_URL",
            "/dash/v1/getDASHManifest.mpd",
            "DASHStreamingSessionURL",
        )),
        uri => ABSENT_ACTIONS
            .iter()
            .find(|(absent, _)| *absent == uri)
            .map(|(_, action)| not_implemented(action)),
    }
}

fn streaming_session_url(
    req: &Request,
    api_name: &str,
    relative_path: &str,
    member: &str,
) -> Response {
    let p = crate::proto::parse_json_body(&req.body);
    let stream_name = param_str(&p, "StreamName").unwrap_or_default();
    let stream_arn = param_str(&p, "StreamARN").unwrap_or_default();
    let endpoint = crate::services::kinesisvideo::stream_data_endpoint(
        req.account,
        &req.region,
        stream_name,
        stream_arn,
        api_name,
    );
    let Some(endpoint) = endpoint else {
        return stream_not_found();
    };
    // moto ignores every option parameter — the URL shape depends only on
    // the endpoint, the fixed relative path and a fresh session token.
    let url = format!("{endpoint}{relative_path}?SessionToken={}", session_token());
    ok(&json!({member: url}))
}

fn get_clip(req: &Request) -> Response {
    let p = crate::proto::parse_json_body(&req.body);
    let stream_name = param_str(&p, "StreamName").unwrap_or_default();
    let stream_arn = param_str(&p, "StreamARN").unwrap_or_default();
    if crate::services::kinesisvideo::stream_data_endpoint(
        req.account,
        &req.region,
        stream_name,
        stream_arn,
        "GET_CLIP",
    )
    .is_none()
    {
        return stream_not_found();
    }
    // moto: fixed content type and payload — "it depends on input stream"
    // in real AWS, so the fork pins both.
    Response::new(200, "video/mp4", b"sample-mp4-video".to_vec())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(method: &str, uri: &str, body: Value) -> Request {
        Request {
            service: "kinesisvideo".into(),
            operation: String::new(),
            method: method.into(),
            path: format!("/{uri}"),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: 111000111101,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Creates a stream through the kinesisvideo control plane — the same
    /// per-(account, region) store the archived-media plane reads.
    fn create_stream(account: u64, name: &str) -> String {
        let mut req = mk("POST", "createStream", json!({"StreamName": name}));
        req.account = account;
        let resp = crate::services::kinesisvideo::dispatch(&req).expect("dispatch");
        assert_eq!(resp.status, 200);
        body(&resp)["StreamARN"].as_str().unwrap().to_string()
    }

    #[test]
    fn hls_url_rides_the_b_endpoint() {
        let account = 111000111102;
        let arn = create_stream(account, "kvam-hls");
        let mut req = mk(
            "POST",
            "getHLSStreamingSessionURL",
            json!({"StreamName": "kvam-hls"}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 200);
        let parsed = body(&resp);
        let url = parsed["HLSStreamingSessionURL"].as_str().unwrap();
        assert!(url.starts_with("https://b-"), "{url}");
        assert!(
            url.contains(".kinesisvideo.us-east-1.amazonaws.com/hls/v1/getHLSMasterPlaylist.m3u8?SessionToken="),
            "{url}"
        );
        let token = url.split("?SessionToken=").nth(1).unwrap();
        assert_eq!(token.len(), 356, "{url}");

        // Same URL shape by ARN.
        let mut req = mk(
            "POST",
            "getHLSStreamingSessionURL",
            json!({"StreamARN": arn}),
        );
        req.account = account;
        assert_eq!(dispatch(&req).unwrap().status, 200);
    }

    #[test]
    fn dash_url_uses_the_manifest_path() {
        let account = 111000111103;
        create_stream(account, "kvam-dash");
        let mut req = mk(
            "POST",
            "getDASHStreamingSessionURL",
            json!({"StreamName": "kvam-dash"}),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 200);
        let parsed = body(&resp);
        let url = parsed["DASHStreamingSessionURL"].as_str().unwrap();
        assert!(
            url.contains("/dash/v1/getDASHManifest.mpd?SessionToken="),
            "{url}"
        );
    }

    #[test]
    fn session_urls_match_the_data_endpoint_host() {
        let account = 111000111104;
        create_stream(account, "kvam-ep");
        let mut req = mk(
            "POST",
            "getDataEndpoint",
            json!({"StreamName": "kvam-ep", "APIName": "GET_HLS_STREAMING_SESSION_URL"}),
        );
        req.account = account;
        let endpoint = body(&crate::services::kinesisvideo::dispatch(&req).unwrap())
            ["DataEndpoint"]
            .as_str()
            .unwrap()
            .to_string();
        let mut req = mk(
            "POST",
            "getHLSStreamingSessionURL",
            json!({"StreamName": "kvam-ep"}),
        );
        req.account = account;
        let parsed = body(&dispatch(&req).unwrap());
        let url = parsed["HLSStreamingSessionURL"].as_str().unwrap();
        assert!(url.starts_with(&endpoint), "{url} vs {endpoint}");
    }

    #[test]
    fn clip_answers_the_fixed_mp4_payload() {
        let account = 111000111105;
        create_stream(account, "kvam-clip");
        let mut req = mk(
            "POST",
            "getClip",
            json!({
                "StreamName": "kvam-clip",
                "ClipFragmentSelector": {
                    "FragmentSelectorType": "SERVER_TIMESTAMP",
                    "TimestampRange": {"StartTimestamp": "x", "EndTimestamp": "y"},
                },
            }),
        );
        req.account = account;
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.headers[0], ("Content-Type".into(), "video/mp4".into()));
        assert_eq!(resp.body, b"sample-mp4-video".to_vec());
    }

    #[test]
    fn missing_stream_not_founds_every_operation() {
        let account = 111000111106;
        for uri in [
            "getHLSStreamingSessionURL",
            "getDASHStreamingSessionURL",
            "getClip",
        ] {
            let mut req = mk("POST", uri, json!({"StreamName": "no-such-kvam-stream"}));
            req.account = account;
            let resp = dispatch(&req).unwrap();
            assert_eq!(resp.status, 404, "/{uri}");
            assert_eq!(
                body(&resp)["__type"],
                json!("ResourceNotFoundException"),
                "/{uri}"
            );
            assert_eq!(
                body(&resp)["message"],
                json!("The requested stream is not found or not active."),
                "/{uri}"
            );
        }
    }

    #[test]
    fn absent_ops_answer_motos_501() {
        let account = 111000111107;
        for (uri, action) in [
            ("getImages", "get_images"),
            ("getMediaForFragmentList", "get_media_for_fragment_list"),
            ("listFragments", "list_fragments"),
        ] {
            let mut req = mk("POST", uri, json!({"StreamName": "kvam-absent"}));
            req.account = account;
            let resp = dispatch(&req).unwrap();
            assert_eq!(resp.status, 501, "/{uri}");
            assert_eq!(body(&resp)["__type"], json!("NotImplemented"), "/{uri}");
            assert_eq!(
                body(&resp)["message"],
                json!(format!("The {action} action has not been implemented")),
                "/{uri}"
            );
        }
    }

    #[test]
    fn other_uris_and_methods_stay_unowned() {
        // Media-plane and junk URIs sign with the same scope but belong to
        // no archived-media operation — dispatch declines them.
        for uri in ["getMedia", "putMedia", "createStream", "", "unknown"] {
            let req = mk("POST", uri, json!({}));
            assert!(dispatch(&req).is_none(), "/{uri}");
        }
        let tests = ["getClip", "getHLSStreamingSessionURL", "listFragments"];
        for uri in tests {
            assert!(dispatch(&mk("GET", uri, json!({}))).is_none(), "GET /{uri}");
        }
        for service in ["kinesis", "s3", "kinesisvideo-unknown"] {
            let mut req = mk("POST", "getClip", json!({}));
            req.service = service.into();
            assert!(dispatch(&req).is_none(), "service {service}");
        }
    }

    #[test]
    fn streams_are_per_account_and_region() {
        let account = 111000111108;
        create_stream(account, "kvam-isolated");
        // A different account must not see the stream.
        let mut req = mk(
            "POST",
            "getHLSStreamingSessionURL",
            json!({"StreamName": "kvam-isolated"}),
        );
        req.account = 111000111109;
        assert_eq!(dispatch(&req).unwrap().status, 404);
        // A different region must not see it either.
        let mut req = mk(
            "POST",
            "getHLSStreamingSessionURL",
            json!({"StreamName": "kvam-isolated"}),
        );
        req.account = account;
        req.region = "us-west-2".into();
        assert_eq!(dispatch(&req).unwrap().status, 404);
    }
}
