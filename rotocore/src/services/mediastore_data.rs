//! AWS Elemental MediaStore Data Plane backend — moto-equivalent (rest-json).
//!
//! Replaces the moto sidecar for `mediastore-data`: `GET /` plus
//! `GET|HEAD|PUT|DELETE /{Path+}`, signed under the `mediastore` signing
//! scope the control plane shares. State is per-(account, region), mirroring
//! moto's `MediaStoreDataBackend` (moto/mediastoredata): objects keyed by
//! path in one flat namespace.
//!
//! Routing mirrors moto's dispatcher exactly: the HTTP method plus the raw
//! request path select the operation (nothing else — the data plane never
//! carries `X-Amz-Target`). The `mediastore` arm in `services/mod.rs` hands
//! off to this backend whenever a signed request arrives without the target
//! header; the bare `mediastore-data` arm serves callers that route the
//! service name directly.
//!
//! Contract: tests/compatibility/test_mediastore_data_compat.py (restored
//! from 978dd8e6). moto quirks kept verbatim:
//! - `ETag` is moto's literal `"etag"` — the fork never computes one.
//! - `StorageClass` always reports moto's `TEMPORAL`; the put header is
//!   ignored (`x-amz-storage-class` is not read in the fork).
//! - `Range` on GetObject is ignored (moto: "not yet supported").
//! - `DeleteObject` answers `{}` and raises `ObjectNotFoundException` with
//!   moto's "Object with id=… not found" message when the path is absent.
//! - `ListItems` ignores `Path`/`MaxResults`/`NextToken` and never emits a
//!   `NextToken` (moto: parameters not yet supported).
//!
//! The vendored `robotocore/all-fixes` moto (906f9c1f) went beyond the cached
//! fork checkout in two ways; both follow the restored compat suite:
//! - `DescribeObject` (HEAD /{Path+}) is implemented: ETag, Content-Type,
//!   Content-Length and Last-Modified headers, `ObjectNotFoundException` on
//!   absent paths.
//! - Metadata answers the real object length and content type — moto's
//!   `ContentLength: 123` placeholder is not carried over.

use crate::proto::{json_error, url_decode};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, MediaStoreDataStore>>>;

fn stores() -> Stores {
    store::slot::<MediaStoreDataStore>("mediastore-data")
}

/// Per-(account, region) in-memory state for the MediaStore data plane.
#[derive(Default)]
struct MediaStoreDataStore {
    /// object path -> stored object
    objects: BTreeMap<String, DataObject>,
}

struct DataObject {
    path: String,
    body: Vec<u8>,
    content_sha256: String,
    etag: String,
    storage_class: String,
    content_type: String,
    cache_control: Option<String>,
    /// RFC 7231 date for the `Last-Modified` metadata header.
    last_modified_header: String,
    /// ISO-8601 timestamp for `Item.LastModified` in ListItems JSON.
    last_modified_iso: String,
}

impl DataObject {
    /// moto's `Object.to_dict()` — the shape PutObject answers — with the
    /// real content type and length instead of moto's placeholders.
    fn to_dict(&self) -> Value {
        json!({
            "ETag": self.etag,
            "Name": self.path,
            "Type": "FILE",
            "ContentLength": self.body.len(),
            "StorageClass": self.storage_class,
            "Path": self.path,
            "ContentSHA256": self.content_sha256,
            "ContentType": self.content_type,
            "LastModified": self.last_modified_iso,
        })
    }

    /// The `Item` member ListItems returns (botocore Item shape members).
    fn to_item(&self) -> Value {
        json!({
            "Name": self.path,
            "Type": "FILE",
            "ETag": self.etag,
            "LastModified": self.last_modified_iso,
            "ContentType": self.content_type,
            "ContentLength": self.body.len(),
        })
    }

    /// The metadata headers DescribeObject and GetObject answer.
    fn metadata_headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![
            ("ETag".to_string(), self.etag.clone()),
            ("Content-Type".to_string(), self.content_type.clone()),
            ("Content-Length".to_string(), self.body.len().to_string()),
            (
                "Last-Modified".to_string(),
                self.last_modified_header.clone(),
            ),
        ];
        if let Some(cache_control) = &self.cache_control {
            headers.push(("Cache-Control".to_string(), cache_control.clone()));
        }
        headers
    }
}

/// moto stores content SHA-256 digests (`hashlib.sha256(body).hexdigest()`).
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// `Last-Modified` in RFC 7231 IMF-fixdate form (`Wed, 01 Oct 2025 12:00:00 GMT`).
fn httpdate_now() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

fn with_store(req: &Request, f: impl FnOnce(&mut MediaStoreDataStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if !handles_request(req) {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

/// This backend serves the data-plane half of the shared `mediastore`
/// signing scope: service `mediastore-data` directly, or `mediastore`
/// requests that carry no `X-Amz-Target` — the json-protocol control plane
/// always sends the target header, the rest-json data plane never does.
fn handles_request(req: &Request) -> bool {
    matches!(req.service.as_str(), "mediastore" | "mediastore-data")
        && !req.headers.contains_key("x-amz-target")
}

fn route(req: &Request, st: &mut MediaStoreDataStore) -> Response {
    let path = object_path(&req.path);
    match (req.method.as_str(), path.as_str()) {
        ("GET", "") => list_items(st),
        ("GET", p) => get_object(st, p),
        ("HEAD", p) => describe_object(st, p),
        ("PUT", p) => put_object(req, st, p),
        ("DELETE", p) => delete_object(st, p),
        (method, p) => json_error(
            400,
            "ValidationException",
            &format!("Unsupported method {method} for path /{p}"),
        ),
    }
}

/// The `{Path+}` request-URI member: strip the leading slash and percent-
/// decode (botocore URL-encodes special characters).
fn object_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    url_decode(trimmed)
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

/// `GET /` — moto's `list_items`: the whole namespace, params ignored.
fn list_items(st: &mut MediaStoreDataStore) -> Response {
    let items: Vec<Value> = st.objects.values().map(DataObject::to_item).collect();
    Response::json(
        200,
        serde_json::to_vec(&json!({ "Items": items })).unwrap_or_default(),
    )
}

/// `GET /{Path+}` — the raw bytes (moto's `get_object` returns the body plus
/// a `Path` header; botocore ignores the extra `Path` header).
fn get_object(st: &mut MediaStoreDataStore, path: &str) -> Response {
    match st.objects.get(path) {
        Some(object) => {
            let headers = object.metadata_headers();
            let body = object.body.clone();
            Response {
                status: 200,
                headers,
                body,
            }
        }
        None => object_not_found(path),
    }
}

/// `HEAD /{Path+}` — metadata only, empty body.
fn describe_object(st: &mut MediaStoreDataStore, path: &str) -> Response {
    match st.objects.get(path) {
        Some(object) => {
            let headers = object.metadata_headers();
            Response {
                status: 200,
                headers,
                body: Vec::new(),
            }
        }
        None => object_not_found(path),
    }
}

/// `PUT /{Path+}` — store and answer moto's `to_dict` JSON.
fn put_object(req: &Request, st: &mut MediaStoreDataStore, path: &str) -> Response {
    if path.is_empty() {
        return json_error(400, "ValidationException", "A Path is required");
    }
    // moto ignores the StorageClass header and runs with default TEMPORAL.
    let body = req.body.to_vec();
    let object = DataObject {
        path: path.to_string(),
        content_sha256: sha256_hex(&body),
        etag: "etag".to_string(),
        storage_class: "TEMPORAL".to_string(),
        content_type: req
            .headers
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| "binary/octet-stream".to_string()),
        cache_control: req.headers.get("cache-control").cloned(),
        last_modified_header: httpdate_now(),
        last_modified_iso: crate::proto::now_iso(),
        body,
    };
    let dict = object.to_dict();
    st.objects.insert(path.to_string(), object);
    Response::json(200, serde_json::to_vec(&dict).unwrap_or_default())
}

/// `DELETE /{Path+}` — moto answers `{}` or raises ObjectNotFound.
fn delete_object(st: &mut MediaStoreDataStore, path: &str) -> Response {
    if st.objects.remove(path).is_some() {
        Response::json(200, b"{}".to_vec())
    } else {
        object_not_found(path)
    }
}

/// moto's `JsonRESTError` surfaces missing objects as a 400 with type and
/// the bare id in the message.
fn object_not_found(path: &str) -> Response {
    json_error(
        400,
        "ObjectNotFoundException",
        &format!("Object with id={path} not found"),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(300_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(
        account: u64,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Response {
        let request = Request {
            service: "mediastore-data".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: Bytes::copy_from_slice(body),
            account,
            region: "us-east-1".to_string(),
        };
        dispatch(&request).expect("mediastore-data dispatch")
    }

    fn put(account: u64, path: &str, body: &[u8], content_type: &str) -> (u16, Value) {
        let resp = send(
            account,
            "PUT",
            &format!("/{path}"),
            &[("content-type", content_type)],
            body,
        );
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    #[test]
    fn put_object_reports_moto_dict() {
        let account = next_account();
        let (status, body) = put(account, "obj-1", b"hello world", "text/plain");
        assert_eq!(status, 200);
        // moto's to_dict members, with the real metadata filled in.
        assert_eq!(body["ETag"], "etag");
        assert_eq!(body["StorageClass"], "TEMPORAL");
        assert_eq!(body["ContentSHA256"], sha256_hex(b"hello world"));
        assert_eq!(body["Name"], "obj-1");
        assert_eq!(body["Type"], "FILE");
        assert_eq!(body["Path"], "obj-1");
        assert_eq!(body["ContentLength"], 11);
        assert_eq!(body["ContentType"], "text/plain");
    }

    #[test]
    fn get_object_round_trips_bytes_and_metadata() {
        let account = next_account();
        put(account, "get/me.bin", b"body-bytes", "application/x-custom");
        let resp = send(account, "GET", "/get/me.bin", &[], b"");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"body-bytes".to_vec());
        let got = |name: &str| {
            resp.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        assert_eq!(got("ETag").as_deref(), Some("etag"));
        assert_eq!(got("Content-Type").as_deref(), Some("application/x-custom"));
        assert_eq!(got("Content-Length").as_deref(), Some("10"));
        assert!(got("Last-Modified").unwrap().ends_with("GMT"));
    }

    #[test]
    fn describe_object_answers_metadata_on_head() {
        let account = next_account();
        put(account, "describe.txt", b"describe-body", "text/plain");
        let resp = send(account, "HEAD", "/describe.txt", &[], b"");
        assert_eq!(resp.status, 200);
        assert!(resp.body.is_empty());
        let got = |name: &str| {
            resp.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        assert_eq!(got("ETag").as_deref(), Some("etag"));
        assert_eq!(got("Content-Type").as_deref(), Some("text/plain"));
        assert_eq!(got("Content-Length").as_deref(), Some("13"));
        assert!(got("Last-Modified").unwrap().ends_with("GMT"));
    }

    #[test]
    fn list_items_reports_stored_objects() {
        let account = next_account();
        put(account, "folder/listed.txt", b"xyz", "text/plain");
        let resp = send(account, "GET", "/", &[], b"");
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let items = body["Items"].as_array().unwrap();
        let names: Vec<&str> = items.iter().filter_map(|i| i["Name"].as_str()).collect();
        assert!(names.contains(&"folder/listed.txt"));
        let listed = items
            .iter()
            .find(|i| i["Name"] == "folder/listed.txt")
            .unwrap();
        assert_eq!(listed["Type"], "FILE");
        assert_eq!(listed["ETag"], "etag");
        assert_eq!(listed["ContentLength"], 3);
        assert_eq!(listed["ContentType"], "text/plain");
        assert!(listed["LastModified"].as_str().unwrap().contains('T'));
    }

    #[test]
    fn delete_object_removes_and_then_404s() {
        let account = next_account();
        put(account, "to-delete", b"b", "text/plain");
        let resp = send(account, "DELETE", "/to-delete", &[], b"");
        assert_eq!(resp.status, 200);
        let _body: Value = serde_json::from_slice(&resp.body).unwrap();
        // Second delete must raise moto's ObjectNotFoundException.
        let resp = send(account, "DELETE", "/to-delete", &[], b"");
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["__type"], "ObjectNotFoundException");
        assert_eq!(body["message"], "Object with id=to-delete not found");
        // And the object is gone from the namespace.
        let resp = send(account, "GET", "/", &[], b"");
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["Items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|i| i["Name"].as_str())
            .collect();
        assert!(!names.contains(&"to-delete"));
    }

    #[test]
    fn get_and_list_missing_raise_object_not_found() {
        let account = next_account();
        for (method, path) in [("GET", "/missing"), ("HEAD", "/missing")] {
            let resp = send(account, method, path, &[], b"");
            assert_eq!(resp.status, 400, "{method}");
            let body: Value = serde_json::from_slice(&resp.body).unwrap_or(json!({}));
            if body.is_null() || body.get("__type").is_none() {
                // HEAD may have no body — the error type still surfaces via
                // the x-amzn-ErrorType header.
                let error_type = resp
                    .headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("x-amzn-ErrorType"))
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
                assert!(
                    error_type.starts_with("ObjectNotFoundException"),
                    "{method}: no error surfaced ({error_type})"
                );
            } else {
                assert_eq!(body["__type"], "ObjectNotFoundException");
            }
        }
    }

    #[test]
    fn control_plane_traffic_hands_back_to_the_control_plane() {
        // X-Amz-Target on the shared scope means the json-protocol control
        // plane — this backend must decline (None) so services/mod.rs can
        // route to the mediastore control-plane backend.
        let request = Request {
            service: "mediastore".to_string(),
            operation: "CreateContainer".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::from([(
                "x-amz-target".to_string(),
                "MediaStore_20170901.CreateContainer".to_string(),
            )]),
            body: Bytes::new(),
            account: next_account(),
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());

        // Foreign services are untouched.
        let foreign = Request {
            service: "s3".to_string(),
            operation: "GetObject".to_string(),
            method: "GET".to_string(),
            path: "/bucket/key".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 1,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&foreign).is_none());
    }

    #[test]
    fn unsupported_methods_error_without_touching_state() {
        let account = next_account();
        let resp = send(account, "POST", "/obj", &[], b"");
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["__type"], "ValidationException");
        let resp = send(account, "GET", "/", &[], b"");
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Items"], json!([]));
    }

    #[test]
    fn state_is_isolated_per_account_and_region() {
        let account = next_account();
        put(account, "shared-path", b"mine", "text/plain");
        // Another region (same account) must not see the object.
        let request = Request {
            service: "mediastore-data".to_string(),
            operation: String::new(),
            method: "GET".to_string(),
            path: "/shared-path".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account,
            region: "eu-west-1".to_string(),
        };
        let resp = dispatch(&request).expect("dispatch");
        assert_eq!(resp.status, 400);
    }
}
