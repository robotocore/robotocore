//! S3 operation handler.
//!
//! REST-XML protocol: responses are raw XML; state is in-memory per
//! (account, region). Requests are routed via method + path + query params.

use base64::Engine;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{
    md5_hex, multipart_etag, Bucket, DeleteMarker, MultipartPart, NotificationConfig, ObjectEntry,
    ObjectVersion, S3State, TargetConfig, NULL_VERSION_ID,
};
use crate::protocol::{AwsRequest, AwsResponse};
use crate::xml::S3_NS;
use serde_json::json;

/// Owner identity used in listing / ACL responses (matches moto).
const OWNER_ID: &str = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
const OWNER_DISPLAY: &str = "webfile";

/// HTTP method enum for S3 operation detection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Method_ {
    Get,
    Put,
    Post,
    Delete,
    Head,
}

// ---------------------------------------------------------------------------
// Minimal XML helpers (requests arrive as compact XML documents)
// ---------------------------------------------------------------------------

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Inner text of the first `<tag>...</tag>` (or `<tag attr>...</tag>`) element.
fn xml_text(doc: &str, tag: &str) -> Option<String> {
    let open_plain = format!("<{}>", tag);
    let open_attr = format!("<{} ", tag);
    let start = match (doc.find(&open_plain), doc.find(&open_attr)) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }?;
    let inner_start = doc[start..].find('>')? + start + 1;
    let close = format!("</{}>", tag);
    let end = doc[inner_start..].find(&close)? + inner_start;
    Some(xml_unescape(doc[inner_start..end].trim()))
}

/// Inner content of every `<tag>...</tag>` element, in document order.
fn xml_sections(doc: &str, tag: &str) -> Vec<String> {
    let close = format!("</{}>", tag);
    let mut out = Vec::new();
    let mut rest = doc;
    loop {
        let a = match (
            rest.find(&format!("<{}>", tag)),
            rest.find(&format!("<{} ", tag)),
        ) {
            (Some(x), Some(y)) => x.min(y),
            (Some(x), None) => x,
            (None, Some(y)) => y,
            (None, None) => break,
        };
        let inner_start = match rest[a..].find('>') {
            Some(i) => a + i + 1,
            None => break,
        };
        let end = match rest[inner_start..].find(&close) {
            Some(i) => inner_start + i,
            None => break,
        };
        out.push(rest[inner_start..end].to_string());
        rest = &rest[end + close.len()..];
    }
    out
}

/// ISO8601 S3 timestamp (zero fractional seconds, `Z` suffix).
fn iso8601(unix: u64) -> String {
    chrono::DateTime::from_timestamp(unix as i64, 0)
        .map(|d| format!("{}.{:03}Z", d.format("%Y-%m-%dT%H:%M:%S"), 0))
        .unwrap_or_default()
}

/// RFC 7231 HTTP date, used in `Last-Modified` headers.
fn httpdate(unix: u64) -> String {
    chrono::DateTime::from_timestamp(unix as i64, 0)
        .map(|d| format!("{}", d.format("%a, %d %b %Y %H:%M:%S GMT")))
        .unwrap_or_default()
}

/// Parse `bytes=X-Y`, `bytes=X-`, `bytes=-N` range expressions.
fn parse_range(spec: &str, size: usize) -> Option<std::ops::Range<usize>> {
    let body = spec.strip_prefix("bytes=")?;
    let (start_s, end_s) = body.split_once('-')?;
    if start_s.is_empty() {
        let n: usize = end_s.parse().ok()?;
        if n == 0 || size == 0 {
            return None;
        }
        let len = n.min(size);
        return Some(size - len..size);
    }
    let start: usize = start_s.parse().ok()?;
    let end: usize = end_s.parse().ok().unwrap_or(size.saturating_sub(1));
    if start >= size || start > end {
        return None;
    }
    let end = end.min(size - 1);
    Some(start..end + 1)
}

/// Parse an `x-amz-copy-source` value into `(bucket, key, Option<versionId>)`.
fn parse_copy_source(value: &str) -> (String, String, Option<String>) {
    let (path, version) = match value.split_once('?') {
        Some((p, q)) => {
            let vid = q
                .split('&')
                .find_map(|kv| kv.strip_prefix("versionId=").map(String::from));
            (p.to_string(), vid)
        }
        None => (value.to_string(), None),
    };
    let trimmed = path.trim_start_matches('/');
    let decoded = urlencoding::decode(trimmed)
        .map(String::from)
        .unwrap_or_else(|_| trimmed.to_string());
    match decoded.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string(), version),
        None => (decoded, String::new(), version),
    }
}


/// A listing content row: `(key, etag, size, last_modified, storage_class)`.
pub type ListingRow = (String, String, usize, u64, String);

fn encode_token(key: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key)
}

fn decode_token(token: &str) -> Option<String> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}

/// A parsed replication rule used by the replication engine.
struct ReplicationRule {
    status: String,
    prefix: Option<String>,
    dest_bucket: Option<String>,
}

/// Parse a notification target section into a `TargetConfig`.
fn parse_target_section(section: &str, arn_tag: &str) -> Option<TargetConfig> {
    let arn = xml_text(section, arn_tag)?;
    if arn.is_empty() {
        return None;
    }
    let mut prefix = None;
    let mut suffix = None;
    for rule in xml_sections(section, "FilterRule") {
        let name = xml_text(&rule, "Name").unwrap_or_default();
        let value = xml_text(&rule, "Value").unwrap_or_default();
        match name.as_str() {
            "prefix" => prefix = Some(value),
            "suffix" => suffix = Some(value),
            _ => {}
        }
    }
    let events = xml_sections(section, "Event")
        .iter()
        .map(|s| xml_unescape(s))
        .collect();
    Some(TargetConfig {
        arn,
        events,
        prefix,
        suffix,
    })
}

fn event_matches(event_name: &str, target: &TargetConfig, key: &str) -> bool {
    let matched = target
        .events
        .iter()
        .any(|evt| evt == "s3:*" || event_name.starts_with(evt.strip_suffix('*').unwrap_or(evt)));
    if !matched {
        return false;
    }
    if let Some(prefix) = &target.prefix {
        if !key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(suffix) = &target.suffix {
        if !key.ends_with(suffix) {
            return false;
        }
    }
    true
}

fn eb_detail_type(event: &str) -> &'static str {
    if event.starts_with("s3:ObjectRemoved") {
        "Object Deleted"
    } else if event.starts_with("s3:ObjectRestore") {
        "Object Restore Initiated"
    } else {
        "Object Created"
    }
}

fn eb_reason(event: &str) -> &'static str {
    match event {
        "s3:ObjectCreated:Copy" => "CopyObject",
        "s3:ObjectCreated:Post" => "POST Object",
        "s3:ObjectCreated:CompleteMultipartUpload" => "CompleteMultipartUpload",
        "s3:ObjectRemoved:DeleteMarkerCreated" => "DeleteObject",
        _ => "PutObject",
    }
}

fn object_lock_requested(req: &AwsRequest) -> bool {
    req.headers
        .get("x-amz-bucket-object-lock-enabled")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// The ID-config kinds sharing the put/get/delete handlers.
#[derive(Clone, Copy)]
enum IdConfig {
    Analytics,
    Metrics,
    IntelligentTiering,
    Inventory,
}

impl IdConfig {
    fn list_root(&self) -> &'static str {
        match self {
            IdConfig::Analytics => "ListBucketAnalyticsConfigurationsOutput",
            IdConfig::Metrics => "ListBucketMetricsConfigurationsOutput",
            IdConfig::IntelligentTiering => "ListBucketIntelligentTieringConfigurationsOutput",
            IdConfig::Inventory => "ListInventoryConfigurationsResult",
        }
    }


    fn store_field<'a>(&self, bucket: &'a Bucket) -> &'a RwLock<HashMap<String, String>> {
        match self {
            IdConfig::Analytics => &bucket.analytics_config,
            IdConfig::Metrics => &bucket.metrics_config,
            IdConfig::IntelligentTiering => &bucket.intelligent_tiering,
            IdConfig::Inventory => &bucket.inventory_config,
        }
    }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

/// The S3 service handler.
pub struct S3Handler {
    /// Per (account, region) state.
    state: RwLock<HashMap<(u64, String), S3State>>,
}

impl S3Handler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> S3State {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    fn with_bucket(&self, req: &AwsRequest) -> Result<(S3State, Arc<Bucket>), AwsResponse> {
        let state = self.get_state(req.account, &req.region);
        let name = req.bucket.clone().unwrap_or_default();
        match state.get_bucket(&name) {
            Some(b) => Ok((state, b)),
            None => Err(AwsResponse::error(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist",
            )),
        }
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let operation = req.operation.as_str();
        match operation {
            "ListBuckets" => self.list_buckets(&req),
            "CreateBucket" => self.create_bucket(&req),
            "DeleteBucket" => self.delete_bucket(&req),
            "HeadBucket" => self.head_bucket(&req),
            "GetBucketLocation" => self.get_bucket_location(&req),
            "PutBucketPolicy" => self.put_bucket_policy(&req),
            "GetBucketPolicy" => self.get_bucket_policy(&req),
            "DeleteBucketPolicy" => self.delete_bucket_policy(&req),
            "PutObject" => self.put_object(&req),
            "GetObject" => self.get_object(&req),
            "HeadObject" => self.head_object(&req),
            "DeleteObject" => self.delete_object(&req),
            "ListObjects" => self.list_objects_v1(&req),
            "ListObjectsV2" => self.list_objects_v2(&req),
            "ListObjectVersions" => self.list_object_versions(&req),
            "CopyObject" => self.copy_object(&req),
            "DeleteObjects" => self.delete_objects(&req),
            "PutBucketCors" => self.put_bucket_cors(&req),
            "GetBucketCors" => self.get_bucket_cors(&req),
            "DeleteBucketCors" => self.delete_bucket_cors(&req),
            "PutBucketVersioning" => self.put_bucket_versioning(&req),
            "GetBucketVersioning" => self.get_bucket_versioning(&req),
            "PutBucketTagging" => self.put_bucket_tagging(&req),
            "GetBucketTagging" => self.get_bucket_tagging(&req),
            "DeleteBucketTagging" => self.delete_bucket_tagging(&req),
            "PutObjectTagging" => self.put_object_tagging(&req),
            "GetObjectTagging" => self.get_object_tagging(&req),
            "DeleteObjectTagging" => self.delete_object_tagging(&req),
            "CreateMultipartUpload" => self.create_multipart_upload(&req),
            "UploadPart" => self.upload_part(&req),
            "CompleteMultipartUpload" => self.complete_multipart_upload(&req),
            "AbortMultipartUpload" => self.abort_multipart_upload(&req),
            "ListMultipartUploads" => self.list_multipart_uploads(&req),
            "ListParts" => self.list_parts(&req),
            "GetBucketAcl" => self.get_bucket_acl(&req),
            "PutBucketAcl" => self.put_bucket_acl(&req),
            "GetObjectAcl" => self.get_object_acl(&req),
            "PutObjectAcl" => self.put_object_acl(&req),
            "RestoreObject" => self.restore_object(&req),
            "GetObjectTorrent" => self.get_object_torrent(&req),
            "GetObjectAttributes" => self.get_object_attributes(&req),
            "GetBucketLifecycle" | "GetBucketLifecycleConfiguration" => {
                self.get_bucket_lifecycle(&req)
            }
            "PutBucketLifecycle" | "PutBucketLifecycleConfiguration" => {
                self.put_bucket_lifecycle(&req)
            }
            "DeleteBucketLifecycle" => self.delete_bucket_lifecycle(&req),
            "GetBucketWebsite" => self.get_bucket_website(&req),
            "PutBucketWebsite" => self.put_bucket_website(&req),
            "DeleteBucketWebsite" => self.delete_bucket_website(&req),
            "GetBucketEncryption" => self.get_bucket_encryption(&req),
            "PutBucketEncryption" => self.put_bucket_encryption(&req),
            "DeleteBucketEncryption" => self.delete_bucket_encryption(&req),
            "GetBucketNotification" | "GetBucketNotificationConfiguration" => {
                self.get_bucket_notification(&req)
            }
            "PutBucketNotification" | "PutBucketNotificationConfiguration" => {
                self.put_bucket_notification(&req)
            }
            "GetBucketLogging" => self.get_bucket_logging(&req),
            "PutBucketLogging" => self.put_bucket_logging(&req),
            "GetBucketAccelerateConfiguration" => self.get_bucket_accelerate(&req),
            "PutBucketAccelerateConfiguration" => self.put_bucket_accelerate(&req),
            "GetBucketRequestPayment" => self.get_bucket_request_payment(&req),
            "PutBucketRequestPayment" => self.put_bucket_request_payment(&req),
            "GetBucketOwnershipControls" => self.get_bucket_ownership(&req),
            "PutBucketOwnershipControls" => self.put_bucket_ownership(&req),
            "DeleteBucketOwnershipControls" => self.delete_bucket_ownership(&req),
            "GetPublicAccessBlock" => self.get_public_access_block(&req),
            "PutPublicAccessBlock" => self.put_public_access_block(&req),
            "DeletePublicAccessBlock" => self.delete_public_access_block(&req),
            "GetObjectLockConfiguration" => self.get_object_lock_config(&req),
            "PutObjectLockConfiguration" => self.put_object_lock_config(&req),
            "GetObjectLegalHold" => self.get_object_legal_hold(&req),
            "PutObjectLegalHold" => self.put_object_legal_hold(&req),
            "GetObjectRetention" => self.get_object_retention(&req),
            "PutObjectRetention" => self.put_object_retention(&req),
            "GetBucketReplication" => self.get_bucket_replication(&req),
            "PutBucketReplication" => self.put_bucket_replication(&req),
            "DeleteBucketReplication" => self.delete_bucket_replication(&req),
            "PutBucketAnalyticsConfiguration" => self.put_bucket_id_config(&req, IdConfig::Analytics),
            "GetBucketAnalyticsConfiguration" => self.get_bucket_id_config(&req, IdConfig::Analytics),
            "DeleteBucketAnalyticsConfiguration" => {
                self.delete_bucket_id_config(&req, IdConfig::Analytics)
            }
            "PutBucketMetricsConfiguration" => self.put_bucket_id_config(&req, IdConfig::Metrics),
            "GetBucketMetricsConfiguration" => self.get_bucket_id_config(&req, IdConfig::Metrics),
            "DeleteBucketMetricsConfiguration" => {
                self.delete_bucket_id_config(&req, IdConfig::Metrics)
            }
            "PutBucketIntelligentTieringConfiguration" => {
                self.put_bucket_id_config(&req, IdConfig::IntelligentTiering)
            }
            "GetBucketIntelligentTieringConfiguration" => {
                self.get_bucket_id_config(&req, IdConfig::IntelligentTiering)
            }
            "DeleteBucketIntelligentTieringConfiguration" => {
                self.delete_bucket_id_config(&req, IdConfig::IntelligentTiering)
            }
            "PutBucketInventoryConfiguration" => self.put_bucket_inventory(&req),
            "GetBucketInventoryConfiguration" => self.get_bucket_inventory(&req),
            "DeleteBucketInventoryConfiguration" => self.delete_bucket_inventory(&req),
            "ListBucketInventoryConfigurations" => self.list_bucket_inventory(&req),
            "ListBucketAnalyticsConfigurations" => self.list_bucket_id_configs(&req, IdConfig::Analytics),
            "ListBucketMetricsConfigurations" => self.list_bucket_id_configs(&req, IdConfig::Metrics),
            "ListBucketIntelligentTieringConfigurations" => {
                self.list_bucket_id_configs(&req, IdConfig::IntelligentTiering)
            }
            "CreateBucketMetadataConfiguration" => self.metadata_config_response(&req),
            "GetBucketMetadataConfiguration" => self.metadata_config_response(&req),
            "DeleteBucketMetadataConfiguration" => self.metadata_config_response(&req),
            "CreateBucketMetadataTableConfiguration" => self.metadata_config_response(&req),
            "GetBucketMetadataTableConfiguration" => self.metadata_config_response(&req),
            "DeleteBucketMetadataTableConfiguration" => self.metadata_config_response(&req),
            "UpdateBucketMetadataInventoryTableConfiguration"
            | "UpdateBucketMetadataJournalTableConfiguration" => {
                self.metadata_config_response(&req)
            }
            "CreateSession" => self.create_session(&req),
            "RenameObject" => self.rename_object(&req),
            "UpdateObjectEncryption" => self.update_object_encryption(&req),
            "WriteGetObjectResponse" => AwsResponse::no_content(200),
            "SelectObjectContent" => AwsResponse::error(
                501,
                "NotImplemented",
                "The select_object_content action has not been implemented",
            ),
            other => AwsResponse::error(
                400,
                "NotImplemented",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    fn bucket_and_key(req: &AwsRequest) -> (String, String) {
        (
            req.bucket.clone().unwrap_or_default(),
            req.key.clone().unwrap_or_default(),
        )
    }

    fn bucket_or_404(state: &S3State, name: &str) -> Result<Arc<Bucket>, AwsResponse> {
        state.get_bucket(name).ok_or_else(|| {
            AwsResponse::error(404, "NoSuchBucket", "The specified bucket does not exist")
        })
    }

    fn valid_bucket_name(name: &str) -> bool {
        if name.len() < 3 || name.len() > 63 {
            return false;
        }
        if name.starts_with('.')
            || name.ends_with('.')
            || name.starts_with('-')
            || name.ends_with('-')
            || name.contains("..")
        {
            return false;
        }
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    }

    fn user_metadata(req: &AwsRequest) -> HashMap<String, String> {
        req.headers
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("x-amz-meta-")
                    .map(|rest| (rest.to_ascii_lowercase(), v.clone()))
            })
            .collect()
    }

    fn tags_from_header(header: &str) -> HashMap<String, String> {
        let decoded = urlencoding::decode(header).unwrap_or_default().into_owned();
        let mut tags = HashMap::new();
        for pair in decoded.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = match pair.split_once('=') {
                Some(pair) => pair,
                None => continue,
            };
            tags.insert(
                urlencoding::decode(k).unwrap_or_default().into_owned(),
                urlencoding::decode(v).unwrap_or_default().into_owned(),
            );
        }
        tags
    }

    /// Store object bytes, applying bucket versioning semantics.
    /// Returns the assigned `(version_id, etag)`.
    fn store_bytes(
        bucket: &Bucket,
        key: &str,
        data: Vec<u8>,
        content_type: String,
        metadata: HashMap<String, String>,
        tags: HashMap<String, String>,
        storage_class: String,
    ) -> (String, String) {
        let version_id = if bucket.is_versioned() {
            uuid::Uuid::new_v4().to_string()
        } else {
            NULL_VERSION_ID.to_string()
        };
        let mut version = ObjectVersion::null_version(data, content_type);
        version.version_id = version_id.clone();
        version.seq = bucket.next_seq();
        version.metadata = metadata;
        version.tags = tags;
        version.storage_class = storage_class;
        version.last_modified = chrono::Utc::now().timestamp() as u64;
        let etag = version.etag.clone();

        let mut objects = bucket.objects.write();
        let entry = objects.entry(key.to_string()).or_default();
        if bucket.is_versioned() {
            entry.versions.insert(0, version);
        } else if *bucket.versioning.read() == Some(false) {
            // Suspended: a new "null" version replaces the previous null version;
            // previously stored non-null versions are kept.
            entry.versions.retain(|v| v.version_id != NULL_VERSION_ID);
            entry.versions.insert(0, version);
        } else {
            entry.versions.clear();
            entry.versions.push(version);
        }
        (version_id, etag)
    }

    /// The tags map of the current (newest) version of a key.
    fn current_tags(bucket: &Bucket, key: &str) -> Option<HashMap<String, String>> {
        let objects = bucket.objects.read();
        let entry = objects.get(key)?;
        entry.versions.first().map(|v| v.tags.clone())
    }

    /// True when the key has at least one stored record.
    fn entry_exists(bucket: &Bucket, key: &str) -> bool {
        bucket.objects.read().contains_key(key)
    }

    // ------------------------------------------------------------------
    // Bucket CRUD
    // ------------------------------------------------------------------

    fn list_buckets(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let buckets = state.list_buckets();
        AwsResponse::xml(200, crate::xml::list_buckets(&buckets))
    }

    fn create_bucket(&self, req: &AwsRequest) -> AwsResponse {
        let bucket_name = match &req.bucket {
            Some(name) => name.clone(),
            None => {
                return AwsResponse::error(400, "InvalidBucketName", "The specified bucket is not valid")
            }
        };

        let body_str = String::from_utf8_lossy(&req.body);
        let directory =
            body_str.contains("<Type>Directory</Type>") || bucket_name.ends_with("--x-s3");
        if !directory && !Self::valid_bucket_name(&bucket_name) {
            return AwsResponse::error(400, "InvalidBucketName", "The specified bucket is not valid");
        }

        let state = self.get_state(req.account, &req.region);
        if let Some(bucket) = state.get_bucket(&bucket_name) {
            // Duplicate creation is idempotent; an object-lock flag can be upgraded.
            if object_lock_requested(req) {
                *bucket.object_lock_enabled.write() = true;
            }
            return AwsResponse::xml(200, crate::xml::create_bucket_result(&bucket.name));
        }

        let mut bucket = Bucket::new(bucket_name.clone(), req.region.clone());
        bucket.directory = directory;
        let lock = object_lock_requested(req);
        *bucket.object_lock_enabled.write() = lock;
        let name = bucket.name.clone();
        state.put_bucket(Arc::new(bucket));
        AwsResponse::xml(200, crate::xml::create_bucket_result(&name))
    }

    fn delete_bucket(&self, req: &AwsRequest) -> AwsResponse {
        let (state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        if bucket.has_content() || !bucket.multipart_uploads.read().is_empty() {
            return AwsResponse::error(
                409,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
            );
        }
        state.delete_bucket(&bucket.name);
        AwsResponse::no_content(204)
    }

    fn head_bucket(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = req.bucket.clone().unwrap_or_default();
        if state.get_bucket(&name).is_none() {
            return AwsResponse::error(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        AwsResponse::no_content(200)
    }

    fn get_bucket_location(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        AwsResponse::xml(200, crate::xml::get_bucket_location(&bucket.region))
    }

    // ------------------------------------------------------------------
    // Bucket policy (body is the raw policy JSON, like moto)
    // ------------------------------------------------------------------

    fn put_bucket_policy(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        *bucket.policy.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn get_bucket_policy(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        let stored = bucket.policy.read().clone();
        match stored {
            // The Policy member is the response payload (botocore reads the body).
            Some(p) => AwsResponse::binary(200, p.into_bytes(), "application/json"),
            None => AwsResponse::error(404, "NoSuchBucketPolicy", "The bucket policy does not exist"),
        }
    }

    fn delete_bucket_policy(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        *bucket.policy.write() = None;
        AwsResponse::no_content(204)
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    fn put_object(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        // PUT with x-amz-copy-source is CopyObject (or UploadPartCopy with uploadId).
        if let Some(src) = req.headers.get("x-amz-copy-source").cloned() {
            return if req.query_params.contains_key("uploadId") {
                self.do_upload_part_copy(req, &bucket, &src)
            } else {
                self.do_copy_object(req, &state, &bucket, &key, &src)
            };
        }

        let content_type = req
            .headers
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| "binary/octet-stream".to_string());
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .cloned()
            .unwrap_or_else(|| "STANDARD".to_string());
        let tags = req
            .headers
            .get("x-amz-tagging")
            .map(|h| Self::tags_from_header(h))
            .unwrap_or_default();

        let (version_id, etag) = Self::store_bytes(
            &bucket,
            &key,
            req.body.to_vec(),
            content_type,
            Self::user_metadata(req),
            tags,
            storage_class,
        );

        Self::fire_event(req, &bucket, &key, "s3:ObjectCreated:Put", req.body.len(), &etag);
        self.maybe_replicate(req, &state, &bucket, &key, req.body.to_vec());

        let mut resp = AwsResponse::xml(200, String::new());
        resp.headers.push(("ETag".to_string(), format!("\"{}\"", etag)));
        if version_id != NULL_VERSION_ID {
            resp.headers.push(("x-amz-version-id".to_string(), version_id));
        }
        resp
    }

    fn do_copy_object(
        &self,
        req: &AwsRequest,
        state: &S3State,
        dest: &Arc<Bucket>,
        dest_key: &str,
        copy_source: &str,
    ) -> AwsResponse {
        let (src_bucket_name, src_key, src_version_id) = parse_copy_source(copy_source);
        let src_bucket = match state.get_bucket(&src_bucket_name) {
            Some(b) => b,
            None => {
                return AwsResponse::error(
                    404,
                    "NoSuchBucket",
                    "The specified source bucket does not exist",
                )
            }
        };
        let source: ObjectVersion = {
            let objects = src_bucket.objects.read();
            match objects.get(&src_key) {
                Some(entry) => {
                    let found = match &src_version_id {
                        Some(vid) => entry
                            .versions
                            .iter()
                            .find(|v| v.version_id == *vid)
                            .cloned(),
                        None => entry.versions.first().cloned(),
                    };
                    match found {
                        Some(v) => v,
                        None => {
                            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                        }
                    }
                }
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            }
        };

        let directive = req
            .headers
            .get("x-amz-metadata-directive")
            .cloned()
            .unwrap_or_else(|| "COPY".to_string())
            .to_ascii_uppercase();
        let replace = directive == "REPLACE";
        let content_type = if replace {
            req.headers
                .get("content-type")
                .cloned()
                .unwrap_or_else(|| source.content_type.clone())
        } else {
            source.content_type.clone()
        };
        let tags = req
            .headers
            .get("x-amz-tagging")
            .map(|h| Self::tags_from_header(h))
            .unwrap_or_else(|| source.tags.clone());
        let metadata = if replace {
            Self::user_metadata(req)
        } else {
            source.metadata.clone()
        };
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .cloned()
            .unwrap_or_else(|| source.storage_class.clone());

        let (_version_id, etag) = Self::store_bytes(
            dest,
            dest_key,
            source.data.clone(),
            content_type,
            metadata,
            tags,
            storage_class,
        );

        Self::fire_event(req, dest, dest_key, "s3:ObjectCreated:Copy", source.size, &etag);

        AwsResponse::xml(
            200,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult xmlns=\"{ns}\">\n  <LastModified>{dt}</LastModified>\n  <ETag>&quot;{et}&quot;</ETag>\n</CopyObjectResult>",
                ns = S3_NS,
                dt = iso8601(chrono::Utc::now().timestamp() as u64),
                et = etag
            ),
        )
    }

    fn get_object(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let range_header = req.headers.get("range").cloned();
        let obj: ObjectVersion = {
            let objects = bucket.objects.read();
            let entry = match objects.get(&key) {
                Some(e) => e,
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            };
            let found = match req.query_params.get("versionId") {
                Some(vid) => entry.versions.iter().find(|v| v.version_id == *vid).cloned(),
                None => {
                    if !entry.is_live() {
                        None
                    } else {
                        entry.versions.first().cloned()
                    }
                }
            };
            match found {
                Some(v) => v,
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            }
        };

        // Range handling
        if let Some(range) = range_header.as_deref().and_then(|r| parse_range(r, obj.size)) {
            let body = obj.data[range.clone()].to_vec();
            let mut resp = AwsResponse::binary(206, body, &obj.content_type);
            resp.headers.push((
                "Content-Range".to_string(),
                format!("bytes {}-{}/{}", range.start, range.end - 1, obj.size),
            ));
            resp.headers
                .push(("Accept-Ranges".to_string(), "bytes".to_string()));
            resp.headers
                .push(("ETag".to_string(), format!("\"{}\"", obj.etag)));
            resp.headers
                .push(("Last-Modified".to_string(), httpdate(obj.last_modified)));
            if obj.version_id != NULL_VERSION_ID {
                resp.headers
                    .push(("x-amz-version-id".to_string(), obj.version_id.clone()));
            }
            for (k, v) in obj.metadata.iter() {
                resp.headers.push((format!("x-amz-meta-{}", k), v.clone()));
            }
            return resp;
        }

        let mut resp = AwsResponse::binary(200, obj.data.clone(), &obj.content_type);
        resp.headers
            .push(("ETag".to_string(), format!("\"{}\"", obj.etag)));
        resp.headers
            .push(("Last-Modified".to_string(), httpdate(obj.last_modified)));
        resp.headers
            .push(("Accept-Ranges".to_string(), "bytes".to_string()));
        for (k, v) in obj.metadata.iter() {
            resp.headers.push((format!("x-amz-meta-{}", k), v.clone()));
        }
        if !obj.tags.is_empty() {
            resp.headers
                .push(("x-amz-tag-count".to_string(), obj.tags.len().to_string()));
        }
        if obj.version_id != NULL_VERSION_ID {
            resp.headers
                .push(("x-amz-version-id".to_string(), obj.version_id.clone()));
        }
        resp
    }

    fn head_object(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let obj: ObjectVersion = {
            let objects = bucket.objects.read();
            let entry = match objects.get(&key) {
                Some(e) => e,
                None => return AwsResponse::error(404, "NotFound", "Not Found"),
            };
            let found = match req.query_params.get("versionId") {
                Some(vid) => entry.versions.iter().find(|v| v.version_id == *vid).cloned(),
                None => {
                    if !entry.is_live() {
                        None
                    } else {
                        entry.versions.first().cloned()
                    }
                }
            };
            match found {
                Some(v) => v,
                None => return AwsResponse::error(404, "NotFound", "Not Found"),
            }
        };

        let mut headers = vec![
            ("Content-Length".to_string(), obj.size.to_string()),
            ("ETag".to_string(), format!("\"{}\"", obj.etag)),
            ("Content-Type".to_string(), obj.content_type.clone()),
            ("Last-Modified".to_string(), httpdate(obj.last_modified)),
            ("Accept-Ranges".to_string(), "bytes".to_string()),
        ];
        for (k, v) in obj.metadata.iter() {
            headers.push((format!("x-amz-meta-{}", k), v.clone()));
        }
        if !obj.tags.is_empty() {
            headers.push(("x-amz-tag-count".to_string(), obj.tags.len().to_string()));
        }
        if obj.version_id != NULL_VERSION_ID && bucket.is_versioned() {
            headers.push(("x-amz-version-id".to_string(), obj.version_id.clone()));
        }
        AwsResponse {
            status: 200,
            headers,
            body: vec![],
        }
    }

    fn delete_object(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let requested_version = req.query_params.get("versionId").cloned();
        let mut marker_created = false;
        let mut version_id_out = None;
        {
            let mut objects = bucket.objects.write();
            let entry = objects.entry(key.clone()).or_default();
            match requested_version.as_deref() {
                Some(vid) => {
                    entry.versions.retain(|v| v.version_id != vid);
                    entry.markers.retain(|m| m.version_id != vid);
                }
                None => {
                    if bucket.is_versioned() {
                        let marker = DeleteMarker {
                            version_id: uuid::Uuid::new_v4().to_string(),
                            seq: bucket.next_seq(),
                            last_modified: chrono::Utc::now().timestamp() as u64,
                        };
                        version_id_out = Some(marker.version_id.clone());
                        entry.markers.insert(0, marker);
                        marker_created = true;
                    } else {
                        entry.versions.clear();
                    }
                }
            }
            if entry.is_empty() {
                objects.remove(&key);
            }
        }

        let mut resp = AwsResponse::no_content(204);
        if marker_created {
            resp.headers
                .push(("x-amz-delete-marker".to_string(), "true".to_string()));
            if let Some(vid) = version_id_out {
                resp.headers.push(("x-amz-version-id".to_string(), vid));
            }
        }
        let event = if marker_created {
            "s3:ObjectRemoved:DeleteMarkerCreated"
        } else {
            "s3:ObjectRemoved:Delete"
        };
        Self::fire_event(req, &bucket, &key, event, 0, "");
        resp
    }

    fn copy_object(&self, req: &AwsRequest) -> AwsResponse {
        self.put_object(req)
    }

    // ------------------------------------------------------------------
    // Listings
    // ------------------------------------------------------------------

    /// Merged listing engine shared by ListObjects (v1) and ListObjectsV2.
    ///
    /// Returns `(contents, common_prefixes)` plus the next-marker payload when
    /// the result is truncated.
    fn collect_listing(
        &self,
        state: &S3State,
        req: &AwsRequest,
    ) -> (Vec<ListingRow>, Vec<String>, Option<String>) {
        let bucket_name = req.bucket.clone().unwrap_or_default();
        let bucket = match Self::bucket_or_404(state, &bucket_name) {
            Ok(b) => b,
            Err(_) => return (Vec::new(), Vec::new(), None),
        };

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req
            .query_params
            .get("delimiter")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000)
            .clamp(0, 1000);

        // The opaque continuation token wins over start-after; v1 uses marker.
        let token_marker = req
            .query_params
            .get("continuation-token")
            .and_then(|t| decode_token(t))
            .unwrap_or_default();
        let start_after = if token_marker.is_empty() {
            req.query_params
                .get("start-after")
                .cloned()
                .unwrap_or_default()
        } else {
            String::new()
        };
        let marker = if token_marker.is_empty() && start_after.is_empty() {
            req.query_params.get("marker").cloned().unwrap_or_default()
        } else {
            String::new()
        };
        let after = token_marker
            .clone()
            .max(start_after.clone())
            .max(marker.clone());

        let objects = bucket.objects.read();
        let mut keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        keys.sort();

        let mut contents: Vec<ListingRow> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut seen_prefixes: Vec<String> = Vec::new();
        let mut last_emitted: Option<String> = None;

        if max_keys > 0 {
            for key in keys.iter() {
                if !after.is_empty() && key.as_str() <= after.as_str() {
                    continue;
                }
                if !delimiter.is_empty() {
                    let rest = &key[prefix.len()..];
                    if let Some(pos) = rest.find(delimiter.as_str()) {
                        let cp = key[..prefix.len() + pos + delimiter.len()].to_string();
                        if seen_prefixes.contains(&cp) {
                            continue;
                        }
                        if contents.len() + prefixes.len() >= max_keys {
                            return (contents, prefixes, last_emitted);
                        }
                        seen_prefixes.push(cp.clone());
                        prefixes.push(cp.clone());
                        last_emitted = Some(cp);
                        continue;
                    }
                }
                if contents.len() + prefixes.len() >= max_keys {
                    return (contents, prefixes, last_emitted);
                }
                let v = objects[key].versions.first();
                contents.push((
                    key.clone(),
                    v.map(|x| x.etag.clone()).unwrap_or_default(),
                    v.map(|x| x.size).unwrap_or(0),
                    v.map(|x| x.last_modified).unwrap_or(0),
                    v.map(|x| x.storage_class.clone())
                        .unwrap_or_else(|| "STANDARD".to_string()),
                ));
                last_emitted = Some(key.clone());
            }
        }
        drop(objects);

        (contents, prefixes, last_emitted)
    }

    fn list_objects_v1(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let (contents, prefixes, last_emitted) = self.collect_listing(&state, req);
        let bucket_name = req.bucket.clone().unwrap_or_default();
        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let marker = req.query_params.get("marker").cloned().unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let delimiter = req.query_params.get("delimiter").cloned().unwrap_or_default();
        let truncated = last_emitted.is_some();

        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{ns}\">\n  <Name>{name}</Name>\n  <Prefix>{prefix}</Prefix>\n  <Marker>{marker}</Marker>\n  <MaxKeys>{max_keys}</MaxKeys>\n  <IsTruncated>{trunc}</IsTruncated>\n",
            ns = S3_NS,
            name = xml_escape(&bucket_name),
            prefix = xml_escape(&prefix),
            marker = xml_escape(&marker),
            max_keys = max_keys,
            trunc = truncated
        );
        if !delimiter.is_empty() {
            if let Some(last) = &last_emitted {
                xml.push_str(&format!(
                    "  <NextMarker>{}</NextMarker>\n",
                    xml_escape(last)
                ));
            }
        }
        for (key, etag, size, last_modified, storage_class) in &contents {
            xml.push_str(&format!(
                "  <Contents>\n    <Key>{k}</Key>\n    <LastModified>{dt}</LastModified>\n    <ETag>&quot;{et}&quot;</ETag>\n    <Size>{sz}</Size>\n    <StorageClass>{cls}</StorageClass>\n    <Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner>\n  </Contents>\n",
                k = xml_escape(key),
                dt = iso8601(*last_modified),
                et = etag,
                sz = size,
                cls = storage_class,
                owner = OWNER_ID,
                disp = OWNER_DISPLAY
            ));
        }
        for cp in &prefixes {
            xml.push_str(&format!(
                "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
                xml_escape(cp)
            ));
        }
        xml.push_str("</ListBucketResult>");
        AwsResponse::xml(200, xml)
    }

    fn list_objects_v2(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let (contents, prefixes, last_emitted) = self.collect_listing(&state, req);
        let bucket_name = req.bucket.clone().unwrap_or_default();
        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000)
            .clamp(0, 1000);
        let token = req
            .query_params
            .get("continuation-token")
            .cloned()
            .unwrap_or_default();
        let start_after = req
            .query_params
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let truncated = last_emitted.is_some();
        let key_count = contents.len() + prefixes.len();

        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{ns}\">\n  <Name>{name}</Name>\n  <Prefix>{prefix}</Prefix>\n",
            ns = S3_NS,
            name = xml_escape(&bucket_name),
            prefix = xml_escape(&prefix)
        );
        if !token.is_empty() {
            xml.push_str(&format!(
                "  <ContinuationToken>{}</ContinuationToken>\n",
                xml_escape(&token)
            ));
        }
        if !start_after.is_empty() && token.is_empty() {
            xml.push_str(&format!(
                "  <StartAfter>{}</StartAfter>\n",
                xml_escape(&start_after)
            ));
        }
        if truncated {
            if let Some(last) = &last_emitted {
                xml.push_str(&format!(
                    "  <NextContinuationToken>{}</NextContinuationToken>\n",
                    encode_token(last)
                ));
            }
        }
        xml.push_str(&format!(
            "  <KeyCount>{kc}</KeyCount>\n  <MaxKeys>{mk}</MaxKeys>\n  <IsTruncated>{tr}</IsTruncated>\n",
            kc = key_count,
            mk = max_keys,
            tr = truncated
        ));
        for (key, etag, size, last_modified, storage_class) in &contents {
            xml.push_str(&format!(
                "  <Contents>\n    <Key>{k}</Key>\n    <LastModified>{dt}</LastModified>\n    <ETag>&quot;{et}&quot;</ETag>\n    <Size>{sz}</Size>\n    <StorageClass>{cls}</StorageClass>\n  </Contents>\n",
                k = xml_escape(key),
                dt = iso8601(*last_modified),
                et = etag,
                sz = size,
                cls = storage_class
            ));
        }
        for cp in &prefixes {
            xml.push_str(&format!(
                "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
                xml_escape(cp)
            ));
        }
        xml.push_str("</ListBucketResult>");
        AwsResponse::xml(200, xml)
    }

    fn list_object_versions(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let bucket_name = bucket.name.clone();

        let keys: Vec<(String, ObjectEntry)> = {
            let objects = bucket.objects.read();
            let mut v: Vec<(String, ObjectEntry)> = objects
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(k, e)| (k.clone(), ObjectEntry {
                    versions: e.versions.clone(),
                    markers: e.markers.clone(),
                }))
                .collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        };

        let mut body = String::new();
        for (key, entry) in keys.iter() {
            let mut records: Vec<(u64, bool, String)> = entry
                .versions
                .iter()
                .map(|v| (v.seq, true, v.version_id.clone()))
                .chain(
                    entry
                        .markers
                        .iter()
                        .map(|m| (m.seq, false, m.version_id.clone())),
                )
                .collect();
            records.sort_by_key(|r| std::cmp::Reverse(r.0));
            for (idx, (seq, is_version, version_id)) in records.iter().enumerate() {
                let is_latest = idx == 0;
                if *is_version {
                    if let Some(v) = entry.versions.iter().find(|v| v.version_id == *version_id) {
                        body.push_str(&format!(
                            "<Version><Key>{k}</Key><VersionId>{vid}</VersionId><IsLatest>{latest}</IsLatest><LastModified>{dt}</LastModified><ETag>&quot;{et}&quot;</ETag><Size>{sz}</Size><StorageClass>{cls}</StorageClass><Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner></Version>",
                            k = xml_escape(key),
                            vid = v.version_id,
                            latest = is_latest,
                            dt = iso8601(v.last_modified),
                            et = v.etag,
                            sz = v.size,
                            cls = v.storage_class,
                            owner = OWNER_ID,
                            disp = OWNER_DISPLAY
                        ));
                    }
                } else if let Some(m) = entry.markers.iter().find(|m| m.version_id == *version_id) {
                    body.push_str(&format!(
                        "<DeleteMarker><Key>{k}</Key><VersionId>{vid}</VersionId><IsLatest>{latest}</IsLatest><LastModified>{dt}</LastModified><Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner></DeleteMarker>",
                        k = xml_escape(key),
                        vid = m.version_id,
                        latest = is_latest,
                        dt = iso8601(m.last_modified),
                        owner = OWNER_ID,
                        disp = OWNER_DISPLAY
                    ));
                }
                let _ = seq;
            }
        }

        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"{ns}\"><Name>{name}</Name><Prefix>{prefix}</Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{body}</ListVersionsResult>",
            ns = S3_NS,
            name = xml_escape(&bucket_name),
            prefix = xml_escape(&prefix)
        );
        AwsResponse::xml(200, xml)
    }

    fn delete_objects(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        let body_str = String::from_utf8_lossy(&req.body).to_string();
        let quiet = body_str.contains("<Quiet>true</Quiet>");

        let mut deleted: Vec<String> = Vec::new();
        let mut rest: &str = &body_str;
        while let Some(start) = rest.find("<Key>") {
            let after = &rest[start + 5..];
            match after.find("</Key>") {
                Some(end) => {
                    let key = xml_unescape(&after[..end]);
                    if !key.is_empty() {
                        deleted.push(key);
                    }
                }
                None => break,
            }
            rest = after;
        }

        {
            let mut objects = bucket.objects.write();
            for key in &deleted {
                if let Some(entry) = objects.get_mut(key) {
                    entry.versions.clear();
                    entry.markers.clear();
                }
                objects.remove(key);
            }
        }

        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"{ns}\">",
            ns = S3_NS
        );
        if !quiet {
            for k in &deleted {
                xml.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(k)));
            }
        }
        xml.push_str("</DeleteResult>");
        AwsResponse::xml(200, xml)
    }

    // ------------------------------------------------------------------
    // Tagging
    // ------------------------------------------------------------------

    fn put_bucket_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.tags_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn get_bucket_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.tags_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(404, "NoSuchTagSet", "The TagSet does not exist"),
        }
    }

    fn delete_bucket_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.tags_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn put_object_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_bucket_name, key) = Self::bucket_and_key(req);
        if !Self::entry_exists(&bucket, &key) {
            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist");
        }
        let body = String::from_utf8_lossy(&req.body).to_string();
        let mut tags = HashMap::new();
        for tag_section in xml_sections(&body, "Tag") {
            let k = xml_text(&tag_section, "Key").unwrap_or_default();
            let v = xml_text(&tag_section, "Value").unwrap_or_default();
            if !k.is_empty() {
                tags.insert(k, v);
            }
        }
        {
            let mut objects = bucket.objects.write();
            if let Some(entry) = objects.get_mut(&key) {
                if let Some(v) = entry.versions.first_mut() {
                    v.tags = tags;
                }
            }
        }
        AwsResponse::no_content(200)
    }

    fn get_object_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_bucket_name, key) = Self::bucket_and_key(req);
        let tags = Self::current_tags(&bucket, &key).unwrap_or_default();
        let mut tag_xml = String::new();
        for (k, v) in tags.iter() {
            tag_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v)
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging xmlns=\"{ns}\"><TagSet>{tags}</TagSet></Tagging>",
            ns = S3_NS,
            tags = tag_xml
        );
        AwsResponse::xml(200, body)
    }

    fn delete_object_tagging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_bucket_name, key) = Self::bucket_and_key(req);
        if !Self::entry_exists(&bucket, &key) {
            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist");
        }
        {
            let mut objects = bucket.objects.write();
            if let Some(entry) = objects.get_mut(&key) {
                if let Some(v) = entry.versions.first_mut() {
                    v.tags = HashMap::new();
                }
            }
        }
        AwsResponse::no_content(204)
    }

    // ------------------------------------------------------------------
    // CORS / Versioning
    // ------------------------------------------------------------------

    fn put_bucket_cors(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.cors_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn get_bucket_cors(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.cors_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
            ),
        }
    }

    fn delete_bucket_cors(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.cors_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn put_bucket_versioning(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = String::from_utf8_lossy(&req.body).to_string();
        let status = xml_text(&body, "Status").unwrap_or_default();
        *bucket.versioning.write() = if status == "Enabled" {
            Some(true)
        } else if status == "Suspended" {
            Some(false)
        } else {
            None
        };
        AwsResponse::no_content(200)
    }

    fn get_bucket_versioning(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let status = match *bucket.versioning.read() {
            Some(true) => "Enabled",
            Some(false) => "Suspended",
            None => "",
        };
        let status_elem = if status.is_empty() {
            String::new()
        } else {
            format!("<Status>{}</Status>", status)
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"{ns}\">{elem}</VersioningConfiguration>",
            ns = S3_NS,
            elem = status_elem
        );
        AwsResponse::xml(200, body)
    }

    // ------------------------------------------------------------------
    // Multipart upload
    // ------------------------------------------------------------------

    fn create_multipart_upload(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let content_type = req
            .headers
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| "binary/octet-stream".to_string());

        let upload = crate::models::MultipartUpload::new(key.clone(), content_type);
        let upload_id = upload.upload_id.clone();
        bucket
            .multipart_uploads
            .write()
            .insert(upload_id.clone(), upload);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"{ns}\">\n  <Bucket>{bucket}</Bucket>\n  <Key>{k}</Key>\n  <UploadId>{upload_id}</UploadId>\n</InitiateMultipartUploadResult>",
            ns = S3_NS,
            bucket = xml_escape(&bucket_name),
            k = xml_escape(&key),
            upload_id = xml_escape(&upload_id)
        );
        AwsResponse::xml(200, body)
    }

    fn upload_part(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, _key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        // PUT with both uploadId and x-amz-copy-source is UploadPartCopy.
        if let Some(src) = req.headers.get("x-amz-copy-source").cloned() {
            return self.do_upload_part_copy(req, &bucket, &src);
        }

        let upload_id = req
            .query_params
            .get("uploadId")
            .cloned()
            .unwrap_or_default();
        let part_number: u32 = req
            .query_params
            .get("partNumber")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let exists = bucket.multipart_uploads.read().contains_key(&upload_id);
        if !exists {
            return AwsResponse::error(
                404,
                "NoSuchUpload",
                "The specified multipart upload does not exist (upload might have been aborted or completed)",
            );
        }

        let etag = md5_hex(&req.body);
        let part = MultipartPart {
            part_number,
            etag: etag.clone(),
            size: req.body.len(),
            data: req.body.to_vec(),
            last_modified: chrono::Utc::now().timestamp() as u64,
        };
        if let Some(upload) = bucket.multipart_uploads.read().get(&upload_id) {
            upload.parts.write().insert(part_number, part);
        }

        let mut resp = AwsResponse::xml(200, String::new());
        resp.headers.push(("ETag".to_string(), format!("\"{}\"", etag)));
        resp
    }

    fn do_upload_part_copy(
        &self,
        req: &AwsRequest,
        bucket: &Arc<Bucket>,
        copy_source: &str,
    ) -> AwsResponse {
        let upload_id = req
            .query_params
            .get("uploadId")
            .cloned()
            .unwrap_or_default();
        let part_number: u32 = req
            .query_params
            .get("partNumber")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if !bucket.multipart_uploads.read().contains_key(&upload_id) {
            return AwsResponse::error(
                404,
                "NoSuchUpload",
                "The specified multipart upload does not exist (upload might have been aborted or completed)",
            );
        }

        let (src_bucket_name, src_key, src_version_id) = parse_copy_source(copy_source);
        let state = self.get_state(req.account, &req.region);
        let src_bucket = match state.get_bucket(&src_bucket_name) {
            Some(b) => b,
            None => {
                return AwsResponse::error(
                    404,
                    "NoSuchBucket",
                    "The specified source bucket does not exist",
                )
            }
        };
        let source: ObjectVersion = {
            let objects = src_bucket.objects.read();
            match objects.get(&src_key) {
                Some(entry) => {
                    let found = match &src_version_id {
                        Some(vid) => entry
                            .versions
                            .iter()
                            .find(|v| v.version_id == *vid)
                            .cloned(),
                        None => entry.versions.first().cloned(),
                    };
                    match found {
                        Some(v) => v,
                        None => {
                            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                        }
                    }
                }
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            }
        };

        let etag = source.etag.clone();
        let part = MultipartPart {
            part_number,
            etag: etag.clone(),
            size: source.size,
            data: source.data.clone(),
            last_modified: chrono::Utc::now().timestamp() as u64,
        };
        if let Some(upload) = bucket.multipart_uploads.read().get(&upload_id) {
            upload.parts.write().insert(part_number, part);
        }

        AwsResponse::xml(
            200,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyPartResult xmlns=\"{ns}\">\n  <LastModified>{dt}</LastModified>\n  <ETag>&quot;{et}&quot;</ETag>\n</CopyPartResult>",
                ns = S3_NS,
                dt = iso8601(chrono::Utc::now().timestamp() as u64),
                et = etag
            ),
        )
    }

    fn complete_multipart_upload(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let upload_id = req
            .query_params
            .get("uploadId")
            .cloned()
            .unwrap_or_default();
        let content_type = {
            let uploads = bucket.multipart_uploads.read();
            match uploads.get(&upload_id) {
                Some(u) => u.content_type.clone(),
                None => {
                    return AwsResponse::error(
                        404,
                        "NoSuchUpload",
                        "The specified multipart upload does not exist (upload might have been aborted or completed)",
                    )
                }
            }
        };

        let parts: Vec<MultipartPart> = {
            let uploads = bucket.multipart_uploads.read();
            match uploads.get(&upload_id) {
                Some(u) => u.parts.read().values().cloned().collect(),
                None => Vec::new(),
            }
        };
        if parts.is_empty() {
            return AwsResponse::error(400, "InvalidPart", "At least one part must be uploaded");
        }
        let etag = {
            let part_etags: Vec<String> = parts.iter().map(|p| p.etag.clone()).collect();
            multipart_etag(&part_etags, parts.len())
        };
        let mut combined = Vec::new();
        for part in &parts {
            combined.extend_from_slice(&part.data);
        }
        let total_size = combined.len();

        let (version_id, _stored_etag) = Self::store_bytes(
            &bucket,
            &key,
            combined,
            content_type,
            Self::user_metadata(req),
            HashMap::new(),
            "STANDARD".to_string(),
        );
        // Override the stored etag with the AWS multipart composite format.
        if let Some(entry) = bucket.objects.write().get_mut(&key) {
            if let Some(v) = entry.versions.first_mut() {
                v.etag = etag.clone();
            }
        }
        bucket.multipart_uploads.write().remove(&upload_id);

        Self::fire_event(
            req,
            &bucket,
            &key,
            "s3:ObjectCreated:CompleteMultipartUpload",
            total_size,
            &etag,
        );

        let location = format!(
            "http://{}.s3.amazonaws.com/{}",
            bucket_name,
            xml_escape(&key)
        );
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"{ns}\">\n  <Location>{loc}</Location>\n  <Bucket>{b}</Bucket>\n  <Key>{k}</Key>\n  <ETag>&quot;{et}&quot;</ETag>\n</CompleteMultipartUploadResult>",
            ns = S3_NS,
            loc = xml_escape(&location),
            b = xml_escape(&bucket_name),
            k = xml_escape(&key),
            et = etag
        );
        let mut resp = AwsResponse::xml(200, body);
        if version_id != NULL_VERSION_ID {
            resp.headers
                .push(("x-amz-version-id".to_string(), version_id));
        }
        resp
    }

    fn abort_multipart_upload(&self, req: &AwsRequest) -> AwsResponse {
        let (_bucket_name, _key) = Self::bucket_and_key(req);
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let upload_id = req
            .query_params
            .get("uploadId")
            .cloned()
            .unwrap_or_default();
        bucket.multipart_uploads.write().remove(&upload_id);
        AwsResponse::no_content(204)
    }

    fn list_multipart_uploads(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let bucket_name = bucket.name.clone();

        let uploads: Vec<(String, String, u64)> = {
            let mp = bucket.multipart_uploads.read();
            let mut v: Vec<(String, String, u64)> = mp
                .values()
                .map(|u| (u.key.clone(), u.upload_id.clone(), u.initiated))
                .collect();
            v.sort();
            v
        };

        let mut uploads_xml = String::new();
        for (key, upload_id, initiated) in &uploads {
            uploads_xml.push_str(&format!(
                "<Upload><Key>{k}</Key><UploadId>{u}</UploadId><StorageClass>STANDARD</StorageClass><Initiated>{dt}</Initiated><Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner><Initiator><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Initiator></Upload>",
                k = xml_escape(key),
                u = xml_escape(upload_id),
                dt = iso8601(*initiated),
                owner = OWNER_ID,
                disp = OWNER_DISPLAY
            ));
        }
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"{ns}\"><Bucket>{b}</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><NextKeyMarker></NextKeyMarker><NextUploadIdMarker></NextUploadIdMarker><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>{uploads}</ListMultipartUploadsResult>",
            ns = S3_NS,
            b = xml_escape(&bucket_name),
            uploads = uploads_xml
        );
        AwsResponse::xml(200, xml)
    }

    fn list_parts(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let upload_id = req
            .query_params
            .get("uploadId")
            .cloned()
            .unwrap_or_default();
        let parts: Vec<MultipartPart> = {
            let uploads = bucket.multipart_uploads.read();
            match uploads.get(&upload_id) {
                Some(u) => u.parts.read().values().cloned().collect(),
                None => {
                    return AwsResponse::error(
                        404,
                        "NoSuchUpload",
                        "The specified multipart upload does not exist (upload might have been aborted or completed)",
                    )
                }
            }
        };

        let next_marker = parts.iter().map(|p| p.part_number).max().unwrap_or(0);
        let mut parts_xml = String::new();
        for p in &parts {
            parts_xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><LastModified>{dt}</LastModified><ETag>&quot;{et}&quot;</ETag><Size>{sz}</Size></Part>",
                n = p.part_number,
                dt = iso8601(p.last_modified),
                et = p.etag,
                sz = p.size
            ));
        }
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"{ns}\"><Bucket>{b}</Bucket><Key>{k}</Key><UploadId>{u}</UploadId><PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>{next}</NextPartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>{parts}<Initiator><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Initiator><Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner><StorageClass>STANDARD</StorageClass></ListPartsResult>",
            ns = S3_NS,
            b = xml_escape(&bucket_name),
            k = xml_escape(&key),
            u = xml_escape(&upload_id),
            next = next_marker,
            parts = parts_xml,
            owner = OWNER_ID,
            disp = OWNER_DISPLAY
        );
        AwsResponse::xml(200, xml)
    }

    // ------------------------------------------------------------------
    // ACL (canned responses)
    // ------------------------------------------------------------------

    fn get_bucket_acl(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, Self::acl_xml())
    }

    fn acl_xml() -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<AccessControlPolicy xmlns=\"{ns}\">\n  <Owner><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Owner>\n  <AccessControlList>\n    <Grant><Grantee xsi:type=\"CanonicalUser\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><ID>{owner}</ID><DisplayName>{disp}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>\n  </AccessControlList>\n</AccessControlPolicy>",
            ns = S3_NS,
            owner = OWNER_ID,
            disp = OWNER_DISPLAY
        )
    }

    fn put_bucket_acl(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::no_content(200)
    }

    fn get_object_acl(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(200, Self::acl_xml())
    }

    fn put_object_acl(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::no_content(200)
    }

    // ------------------------------------------------------------------
    // Restore / Torrent / Attributes
    // ------------------------------------------------------------------

    fn restore_object(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_bucket_name, key) = Self::bucket_and_key(req);
        let storage_class = {
            let objects = bucket.objects.read();
            match objects.get(&key).and_then(|e| e.versions.first()) {
                Some(v) => v.storage_class.clone(),
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            }
        };
        if storage_class == "GLACIER" || storage_class == "DEEP_ARCHIVE" {
            Self::fire_event(req, &bucket, &key, "s3:ObjectRestore:Post", 0, "");
            AwsResponse::no_content(202)
        } else {
            AwsResponse::error(
                403,
                "InvalidObjectState",
                "The operation is not valid for the object's storage class",
            )
        }
    }

    fn get_object_torrent(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, _bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        AwsResponse::binary(
            200,
            b"robotocore-torrent".to_vec(),
            "application/x-bittorrent",
        )
    }

    fn get_object_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (bucket_name, key) = Self::bucket_and_key(req);
        let _ = &bucket_name;
        let obj: Option<ObjectVersion> = {
            let objects = bucket.objects.read();
            let entry = objects.get(&key);
            match req.query_params.get("versionId") {
                Some(vid) => entry
                    .and_then(|e| e.versions.iter().find(|v| v.version_id == *vid).cloned()),
                None => entry
                    .filter(|e| e.is_live())
                    .and_then(|e| e.versions.first().cloned()),
            }
        };
        let obj = match obj {
            Some(o) => o,
            None => return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist"),
        };
        let mut body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesOutput xmlns=\"{ns}\"><ETag>{et}</ETag><ObjectSize>{sz}</ObjectSize></GetObjectAttributesOutput>",
            ns = S3_NS,
            et = obj.etag,
            sz = obj.size
        );
        if let Some(vid) = req.query_params.get("versionId") {
            if vid != NULL_VERSION_ID {
                body = body.replace(
                    "</GetObjectAttributesOutput>",
                    &format!("<VersionId>{}</VersionId></GetObjectAttributesOutput>", vid),
                );
            }
        }
        AwsResponse::xml(200, body)
    }

    // ------------------------------------------------------------------
    // Lifecycle / website / encryption / logging / etc.
    // ------------------------------------------------------------------

    fn get_bucket_lifecycle(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.lifecycle_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
            ),
        }
    }

    fn put_bucket_lifecycle(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.lifecycle_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn delete_bucket_lifecycle(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.lifecycle_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn get_bucket_website(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.website_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
            ),
        }
    }

    fn put_bucket_website(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.website_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn delete_bucket_website(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.website_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn get_bucket_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.encryption_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
            ),
        }
    }

    fn put_bucket_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.encryption_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn delete_bucket_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.encryption_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn get_bucket_notification(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = bucket
            .notification_xml
            .read()
            .clone()
            .unwrap_or_else(|| format!("<NotificationConfiguration xmlns=\"{}\"/>", S3_NS));
        AwsResponse::xml(200, body)
    }

    fn put_bucket_notification(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = String::from_utf8_lossy(&req.body).to_string();
        let config = Self::parse_notification_xml(&body);
        *bucket.notification_xml.write() = Some(body);
        *bucket.notification_config.write() = Some(config);
        AwsResponse::no_content(200)
    }

    fn get_bucket_logging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = bucket
            .logging_xml
            .read()
            .clone()
            .unwrap_or_else(|| format!("<BucketLoggingStatus xmlns=\"{}\"/>", S3_NS));
        AwsResponse::xml(200, body)
    }

    fn put_bucket_logging(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = String::from_utf8_lossy(&req.body).to_string();
        if body.contains("<LoggingEnabled>") || body.contains("<LoggingEnabled ") {
            *bucket.logging_xml.write() = Some(body);
        } else {
            *bucket.logging_xml.write() = None;
        }
        AwsResponse::no_content(200)
    }

    fn get_bucket_accelerate(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = bucket
            .accelerate_xml
            .read()
            .clone()
            .unwrap_or_else(|| format!("<AccelerateConfiguration xmlns=\"{}\"/>", S3_NS));
        AwsResponse::xml(200, body)
    }

    fn put_bucket_accelerate(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.accelerate_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn get_bucket_request_payment(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = bucket
            .request_payment_xml
            .read()
            .clone()
            .unwrap_or_else(|| format!("<RequestPaymentConfiguration xmlns=\"{}\"/>", S3_NS));
        AwsResponse::xml(200, body)
    }

    fn put_bucket_request_payment(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.request_payment_xml.write() =
            Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn get_bucket_ownership(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.ownership_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "OwnershipControlsNotFoundError",
                "The bucket ownership controls were not found",
            ),
        }
    }

    fn put_bucket_ownership(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.ownership_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn delete_bucket_ownership(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.ownership_xml.write() = None;
        AwsResponse::no_content(204)
    }

    fn get_public_access_block(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.public_access_block_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "NoSuchPublicAccessBlockConfiguration",
                "The public access block configuration was not found",
            ),
        }
    }

    fn put_public_access_block(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.public_access_block_xml.write() =
            Some(String::from_utf8_lossy(&req.body).to_string());
        AwsResponse::no_content(200)
    }

    fn delete_public_access_block(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.public_access_block_xml.write() = None;
        AwsResponse::no_content(204)
    }

    // ------------------------------------------------------------------
    // Object lock / legal hold / retention
    // ------------------------------------------------------------------

    fn get_object_lock_config(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.object_lock_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
            ),
        }
    }

    fn put_object_lock_config(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = String::from_utf8_lossy(&req.body).to_string();
        let enabled = body.contains("ObjectLockEnabled")
            && !body.contains("<ObjectLockEnabled>Disabled</ObjectLockEnabled>");
        if enabled {
            *bucket.object_lock_enabled.write() = true;
        }
        *bucket.object_lock_xml.write() = Some(body);
        AwsResponse::no_content(200)
    }

    fn current_version_of(bucket: &Bucket, key: &str) -> Option<ObjectVersion> {
        let objects = bucket.objects.read();
        objects.get(key).and_then(|e| e.versions.first().cloned())
    }

    fn update_current_version<F: FnOnce(&mut ObjectVersion)>(
        &self,
        bucket: &Bucket,
        key: &str,
        f: F,
    ) -> bool {
        let mut objects = bucket.objects.write();
        match objects.get_mut(key) {
            Some(entry) => match entry.versions.first_mut() {
                Some(v) => {
                    f(v);
                    true
                }
                None => false,
            },
            None => false,
        }
    }

    fn get_object_legal_hold(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_b, key) = Self::bucket_and_key(req);
        match Self::current_version_of(&bucket, &key).and_then(|v| v.legal_hold) {
            Some(status) => AwsResponse::xml(
                200,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LegalHold xmlns=\"{ns}\"><Status>{status}</Status></LegalHold>",
                    ns = S3_NS,
                    status = status
                ),
            ),
            None => AwsResponse::error(
                404,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            ),
        }
    }

    fn put_object_legal_hold(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_b, key) = Self::bucket_and_key(req);
        let body = String::from_utf8_lossy(&req.body).to_string();
        let status = xml_text(&body, "Status").unwrap_or_else(|| "OFF".to_string());
        if !Self::entry_exists(&bucket, &key) {
            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist");
        }
        let bucket_ref = &bucket;
        let key_ref = key.clone();
        self.update_current_version(bucket_ref, &key_ref, |v| {
            v.legal_hold = Some(status);
        });
        AwsResponse::no_content(200)
    }

    fn get_object_retention(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_b, key) = Self::bucket_and_key(req);
        match Self::current_version_of(&bucket, &key).and_then(|v| v.retention.clone()) {
            Some((mode, until)) => AwsResponse::xml(
                200,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Retention xmlns=\"{ns}\"><Mode>{mode}</Mode><RetainUntilDate>{until}</RetainUntilDate></Retention>",
                    ns = S3_NS,
                    mode = mode,
                    until = until
                ),
            ),
            None => AwsResponse::error(
                404,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            ),
        }
    }

    fn put_object_retention(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_b, key) = Self::bucket_and_key(req);
        let body = String::from_utf8_lossy(&req.body).to_string();
        let mode = xml_text(&body, "Mode").unwrap_or_default();
        let until = xml_text(&body, "RetainUntilDate").unwrap_or_default();
        if !Self::entry_exists(&bucket, &key) {
            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist");
        }
        if !*bucket.object_lock_enabled.read() {
            return AwsResponse::error(
                400,
                "InvalidRequest",
                "Bucket is missing Object Lock Configuration",
            );
        }
        self.update_current_version(&bucket, &key, |v| {
            v.retention = Some((mode.clone(), until.clone()));
        });
        AwsResponse::no_content(200)
    }

    // ------------------------------------------------------------------
    // Replication
    // ------------------------------------------------------------------

    fn get_bucket_replication(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let stored = bucket.replication_xml.read().clone();
        match stored {
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(
                404,
                "ReplicationConfigurationNotFoundError",
                "The replication configuration was not found",
            ),
        }
    }

    fn put_bucket_replication(&self, req: &AwsRequest) -> AwsResponse {
        let (state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let body = String::from_utf8_lossy(&req.body).to_string();
        let rules = Self::parse_replication_rules(&body);

        // Validation: source bucket must be versioned and destinations exist.
        if !bucket.is_versioned() {
            return AwsResponse::error(
                400,
                "InvalidRequest",
                "Versioning must be 'Enabled' on the source bucket",
            );
        }
        for rule in &rules {
            if let Some(dest) = &rule.dest_bucket {
                if state.get_bucket(dest).is_none() {
                    return AwsResponse::error(
                        400,
                        "InvalidRequest",
                        "The destination bucket does not exist",
                    );
                }
            }
        }
        *bucket.replication_xml.write() = Some(body);
        AwsResponse::no_content(200)
    }

    fn delete_bucket_replication(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        *bucket.replication_xml.write() = None;
        AwsResponse::no_content(204)
    }

    /// Copy newly-created objects into their replication-destination buckets.
    fn maybe_replicate(
        &self,
        _req: &AwsRequest,
        state: &S3State,
        bucket: &Bucket,
        key: &str,
        data: Vec<u8>,
    ) {
        let rules = match bucket.replication_xml.read().clone() {
            Some(body) => Self::parse_replication_rules(&body),
            None => return,
        };
        let dests: Vec<String> = rules
            .iter()
            .filter(|r| r.status.eq_ignore_ascii_case("Enabled"))
            .filter(|r| r.prefix.as_ref().map(|p| key.starts_with(p)).unwrap_or(true))
            .filter_map(|r| r.dest_bucket.clone())
            .collect();
        for dest_name in dests {
            if dest_name == bucket.name {
                continue;
            }
            let source = {
                let objects = bucket.objects.read();
                match objects.get(key).and_then(|e| e.versions.first()) {
                    Some(v) => v.clone(),
                    None => return,
                }
            };
            if let Some(dest) = state.get_bucket(&dest_name) {
                Self::store_bytes(
                    &dest,
                    key,
                    data.clone(),
                    source.content_type.clone(),
                    source.metadata.clone(),
                    source.tags.clone(),
                    source.storage_class.clone(),
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // Id-keyed configurations (analytics / metrics / intelligent tiering)
    // ------------------------------------------------------------------

    fn put_bucket_id_config(&self, req: &AwsRequest, kind: IdConfig) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let id = req.query_params.get("id").cloned().unwrap_or_default();
        if id.is_empty() {
            return AwsResponse::error(400, "InvalidArgument", "The Id query parameter is required");
        }
        let body = String::from_utf8_lossy(&req.body).to_string();
        kind.store_field(&bucket).write().insert(id, body);
        AwsResponse::no_content(200)
    }

    fn get_bucket_id_config(&self, req: &AwsRequest, kind: IdConfig) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let id = req.query_params.get("id").cloned().unwrap_or_default();
        let stored = kind.store_field(&bucket).read().get(&id).cloned();
        match stored {
            // The config member is the response payload: the body IS the config.
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(404, "NoSuchConfiguration", "The specified configuration does not exist"),
        }
    }

    fn delete_bucket_id_config(&self, req: &AwsRequest, kind: IdConfig) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let id = req.query_params.get("id").cloned().unwrap_or_default();
        kind.store_field(&bucket).write().remove(&id);
        AwsResponse::no_content(204)
    }

    /// List stored configurations of an id-keyed kind.
    fn list_bucket_id_configs(&self, req: &AwsRequest, kind: IdConfig) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        // Stored bodies already root at the member element name botocore maps
        // (e.g. <AnalyticsConfiguration>), so emit them directly.
        let entries: Vec<String> = kind.store_field(&bucket).read().values().cloned().collect();
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{ns}\"><IsTruncated>false</IsTruncated>",
            root = kind.list_root(),
            ns = S3_NS
        );
        for entry in entries {
            xml.push_str(&entry);
        }
        xml.push_str(&format!("</{root}>", root = kind.list_root()));
        AwsResponse::xml(200, xml)
    }

    // ------------------------------------------------------------------
    // Inventory
    // ------------------------------------------------------------------

    fn put_bucket_inventory(&self, req: &AwsRequest) -> AwsResponse {
        self.put_bucket_id_config(req, IdConfig::Inventory)
    }

    fn get_bucket_inventory(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let id = req.query_params.get("id").cloned().unwrap_or_default();
        let stored = bucket.inventory_config.read().get(&id).cloned();
        match stored {
            // The config member is the response payload: the body IS the config.
            Some(body) => AwsResponse::xml(200, body),
            None => AwsResponse::error(404, "NoSuchConfiguration", "The specified configuration does not exist"),
        }
    }

    fn delete_bucket_inventory(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let id = req.query_params.get("id").cloned().unwrap_or_default();
        bucket.inventory_config.write().remove(&id);
        AwsResponse::no_content(204)
    }

    fn list_bucket_inventory(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let entries: Vec<String> = bucket.inventory_config.read().values().cloned().collect();
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListInventoryConfigurationsResult xmlns=\"{ns}\"><IsTruncated>false</IsTruncated>",
            ns = S3_NS
        );
        for entry in entries {
            xml.push_str(&format!("<InventoryConfiguration>{}</InventoryConfiguration>", entry));
        }
        xml.push_str("</ListInventoryConfigurationsResult>");
        AwsResponse::xml(200, xml)
    }

    // ------------------------------------------------------------------
    // Metadata configuration / metadata table (S3 Metadata)
    // ------------------------------------------------------------------

    fn metadata_config_response(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, _bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        match req.method.as_str() {
            "DELETE" => AwsResponse::no_content(204),
            _ => AwsResponse::xml(
                200,
                format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<MetadataConfigurationResult xmlns=\"{}\"/>", S3_NS),
            ),
        }
    }

    // ------------------------------------------------------------------
    // S3 Express session / rename / encryption
    // ------------------------------------------------------------------

    fn create_session(&self, req: &AwsRequest) -> AwsResponse {
        // Session credentials are accepted for any path; matches the Python
        // provider's native CreateSession handling.
        let _ = req;
        let seed = format!("{}:{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0), uuid::Uuid::new_v4());
        let hex_digest = {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(seed.as_bytes()))
        };
        let expiry = (chrono::Utc::now() + chrono::Duration::hours(12))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CreateSessionResult>\n  <Credentials>\n    <SessionToken>FwoGZXIvYXdzE{hex}</SessionToken>\n    <SecretAccessKey>{hex}</SecretAccessKey>\n    <AccessKeyId>ASIA{ak}</AccessKeyId>\n    <Expiration>{expiry}</Expiration>\n  </Credentials>\n</CreateSessionResult>",
            hex = hex_digest,
            ak = hex_digest[..16].to_ascii_uppercase(),
            expiry = expiry
        );
        AwsResponse::xml(200, body)
    }

    fn rename_object(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_bucket_name, dest_key) = Self::bucket_and_key(req);
        let rename_source = req
            .headers
            .get("x-amz-rename-source")
            .cloned()
            .unwrap_or_default();
        if rename_source.is_empty() {
            return AwsResponse::error(400, "InvalidArgument", "x-amz-rename-source is required");
        }
        let decoded = urlencoding::decode(&rename_source)
            .map(String::from)
            .unwrap_or_else(|_| rename_source.clone());
        let trimmed = decoded.trim_start_matches('/');
        let src_key = match trimmed.split_once('/') {
            // Format "<bucket>/<key>" — the bucket prefix may equal this bucket.
            Some((_src_bucket, key)) => key.to_string(),
            None => trimmed.to_string(),
        };
        if src_key.is_empty() {
            return AwsResponse::error(400, "InvalidArgument", "x-amz-rename-source is invalid");
        }

        let source = {
            let objects = bucket.objects.write();
            match objects.get(&src_key) {
                Some(entry) => entry.versions.first().cloned(),
                None => None,
            }
        };
        let source = match source {
            Some(v) => v,
            None => return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist"),
        };
        {
            let mut objects = bucket.objects.write();
            objects.remove(&src_key);
        }
        Self::store_bytes(
            &bucket,
            &dest_key,
            source.data.clone(),
            source.content_type.clone(),
            source.metadata.clone(),
            source.tags.clone(),
            source.storage_class.clone(),
        );
        AwsResponse::no_content(200)
    }

    fn update_object_encryption(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (_b, key) = Self::bucket_and_key(req);
        if !Self::entry_exists(&bucket, &key) {
            return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist");
        }
        AwsResponse::no_content(200)
    }

    // ------------------------------------------------------------------
    // Notification event firing
    // ------------------------------------------------------------------

    fn fire_event(
        req: &AwsRequest,
        bucket: &Bucket,
        key: &str,
        event_name: &str, // e.g. "s3:ObjectCreated:Put"
        size: usize,
        etag: &str,
    ) {
        let config = bucket.notification_config.read().clone().unwrap_or_default();
        if config.queues.is_empty()
            && config.topics.is_empty()
            && config.lambdas.is_empty()
            && !config.eventbridge
        {
            return;
        }

        let record = json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": req.region,
            "eventTime": iso8601(chrono::Utc::now().timestamp() as u64),
            "eventName": event_name.trim_start_matches("s3:"),
            "userIdentity": {"principalId": req.account.to_string()},
            "requestParameters": {"sourceIPAddress": "127.0.0.1"},
            "responseElements": {},
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": uuid::Uuid::new_v4(),
                "bucket": {
                    "name": bucket.name,
                    "ownerIdentity": {"principalId": req.account.to_string()},
                    "arn": format!("arn:aws:s3:::{}", bucket.name),
                },
                "object": {
                    "key": key,
                    "size": size,
                    "eTag": etag,
                    "sequencer": format!("{:016X}", chrono::Utc::now().timestamp_millis()),
                },
            },
        });
        let message = serde_json::to_string(&json!({"Records": [record]})).unwrap_or_default();

        for target in config.queues.iter() {
            if event_matches(event_name, target, key) && target.arn.contains(":sqs:") {
                sqs::deliver_to_queue(&target.arn, &message);
            }
        }
        // Topic and Lambda targets are not wired cross-crate in the Rust port.

        if config.eventbridge {
            let event = json!({
                "version": "0",
                "id": uuid::Uuid::new_v4(),
                "source": "aws.s3",
                "account": req.account.to_string(),
                "time": iso8601(chrono::Utc::now().timestamp() as u64),
                "region": req.region,
                "resources": [format!("arn:aws:s3:::{}", bucket.name)],
                "detail-type": eb_detail_type(event_name),
                "detail": {
                    "version": "0",
                    "bucket": {"name": bucket.name},
                    "object": {"key": key, "size": size, "etag": etag},
                    "request-id": uuid::Uuid::new_v4(),
                    "requester": req.account.to_string(),
                    "reason": eb_reason(event_name),
                },
            });
            events::publish_event_to_bus(&event, req.account, &req.region);
        }
    }

    /// Parse bucket notification configuration XML into the structured form.
    fn parse_notification_xml(body: &str) -> NotificationConfig {
        let mut config = NotificationConfig::default();
        for section in xml_sections(body, "QueueConfiguration") {
            if let Some(t) = parse_target_section(&section, "Queue") {
                config.queues.push(t);
            }
        }
        for section in xml_sections(body, "TopicConfiguration") {
            if let Some(t) = parse_target_section(&section, "Topic") {
                config.topics.push(t);
            }
        }
        for section in xml_sections(body, "LambdaFunctionConfiguration") {
            let arn = xml_text(&section, "LambdaFunctionArn")
                .or_else(|| xml_text(&section, "CloudFunction"))
                .unwrap_or_default();
            if arn.is_empty() {
                continue;
            }
            config.lambdas.push(TargetConfig {
                arn,
                events: xml_sections(&section, "Event")
                    .iter()
                    .map(|s| xml_unescape(s))
                    .collect(),
                prefix: None,
                suffix: None,
            });
        }
        if body.contains("EventBridgeConfiguration") {
            config.eventbridge = true;
        }
        config
    }

    fn parse_replication_rules(body: &str) -> Vec<ReplicationRule> {
        let mut rules = Vec::new();
        for section in xml_sections(body, "Rule") {
            let status = xml_text(&section, "Status").unwrap_or_else(|| "Disabled".to_string());
            // Legacy rules carry <Prefix>; filter-based rules nest it under <Filter>.
            let prefix = xml_text(&section, "Prefix")
                .or_else(|| xml_text(&section, "Filter").and_then(|f| xml_text(&f, "Prefix")),
                );
            let dest = xml_text(&section, "Destination").and_then(|d| xml_text(&d, "Bucket"));
            rules.push(ReplicationRule {
                status,
                prefix,
                dest_bucket: dest.map(|b| b.trim_start_matches("arn:aws:s3:::").to_string()),
            });
        }
        rules
    }

    // ------------------------------------------------------------------
    // Operation detection
    // ------------------------------------------------------------------

    /// Detect the S3 operation from HTTP method, path, and query params.
    /// Mirrors the Python `_detect_rest_operation` logic.
    pub fn detect_s3_operation(
        method: &str,
        path: &str,
        query_params: &std::collections::HashMap<String, String>,
    ) -> Option<String> {
        let trimmed = path.trim_start_matches('/');
        let mut parts = trimmed.splitn(2, '/');
        let first = parts.next().unwrap_or("");
        let second = parts.next().unwrap_or("");
        let has_bucket = !first.is_empty();
        let has_key = !second.is_empty();

        if !has_bucket {
            return if method == "GET" {
                Some("ListBuckets".to_string())
            } else {
                None
            };
        }

        // S3 Object Lambda write-back path: POST /{RequestRoute}/WriteGetObjectResponse
        if has_key && second == "WriteGetObjectResponse" && method == "POST" {
            return Some("WriteGetObjectResponse".to_string());
        }

        let q = |k: &str| query_params.contains_key(k);

        if has_key {
            if q("uploadId") {
                return match method {
                    "PUT" => Some("UploadPart".to_string()),
                    "GET" => Some("ListParts".to_string()),
                    "POST" => Some("CompleteMultipartUpload".to_string()),
                    "DELETE" => Some("AbortMultipartUpload".to_string()),
                    _ => None,
                };
            }
            if q("uploads") && method == "POST" {
                return Some("CreateMultipartUpload".to_string());
            }
            if q("restore") && method == "POST" {
                return Some("RestoreObject".to_string());
            }
            if q("select") && method == "POST" {
                return Some("SelectObjectContent".to_string());
            }
            if q("tagging") {
                return match method {
                    "PUT" => Some("PutObjectTagging".to_string()),
                    "GET" => Some("GetObjectTagging".to_string()),
                    "DELETE" => Some("DeleteObjectTagging".to_string()),
                    _ => None,
                };
            }
            if q("acl") {
                return match method {
                    "PUT" => Some("PutObjectAcl".to_string()),
                    "GET" => Some("GetObjectAcl".to_string()),
                    _ => None,
                };
            }
            if q("legal-hold") {
                return match method {
                    "PUT" => Some("PutObjectLegalHold".to_string()),
                    "GET" => Some("GetObjectLegalHold".to_string()),
                    _ => None,
                };
            }
            if q("retention") {
                return match method {
                    "PUT" => Some("PutObjectRetention".to_string()),
                    "GET" => Some("GetObjectRetention".to_string()),
                    _ => None,
                };
            }
            if q("attributes") && method == "GET" {
                return Some("GetObjectAttributes".to_string());
            }
            if q("torrent") && method == "GET" {
                return Some("GetObjectTorrent".to_string());
            }
            if q("renameObject") && method == "PUT" {
                return Some("RenameObject".to_string());
            }
            if q("encryption") && method == "PUT" {
                return Some("UpdateObjectEncryption".to_string());
            }
            return match method {
                "GET" => Some("GetObject".to_string()),
                "PUT" => Some("PutObject".to_string()),
                "HEAD" => Some("HeadObject".to_string()),
                "DELETE" => Some("DeleteObject".to_string()),
                "POST" => Some("PutObject".to_string()),
                _ => None,
            };
        }

        // Bucket-level
        match method {
            "GET" => {
                if q("versions") {
                    Some("ListObjectVersions".to_string())
                } else if q("versioning") {
                    Some("GetBucketVersioning".to_string())
                } else if q("policy") {
                    Some("GetBucketPolicy".to_string())
                } else if q("cors") {
                    Some("GetBucketCors".to_string())
                } else if q("lifecycle") {
                    Some("GetBucketLifecycle".to_string())
                } else if q("location") {
                    Some("GetBucketLocation".to_string())
                } else if q("website") {
                    Some("GetBucketWebsite".to_string())
                } else if q("logging") {
                    Some("GetBucketLogging".to_string())
                } else if q("notification") {
                    Some("GetBucketNotificationConfiguration".to_string())
                } else if q("tagging") {
                    Some("GetBucketTagging".to_string())
                } else if q("acl") {
                    Some("GetBucketAcl".to_string())
                } else if q("encryption") {
                    Some("GetBucketEncryption".to_string())
                } else if q("replication") {
                    Some("GetBucketReplication".to_string())
                } else if q("accelerate") {
                    Some("GetBucketAccelerateConfiguration".to_string())
                } else if q("requestPayment") {
                    Some("GetBucketRequestPayment".to_string())
                } else if q("ownershipControls") {
                    Some("GetBucketOwnershipControls".to_string())
                } else if q("publicAccessBlock") {
                    Some("GetPublicAccessBlock".to_string())
                } else if q("object-lock") {
                    Some("GetObjectLockConfiguration".to_string())
                } else if q("session") {
                    Some("CreateSession".to_string())
                } else if q("metadataConfiguration") {
                    Some("GetBucketMetadataConfiguration".to_string())
                } else if q("metadataTable") {
                    Some("GetBucketMetadataTableConfiguration".to_string())
                } else if q("analytics") {
                    let has_id = query_params
                        .get("id")
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    if has_id {
                        Some("GetBucketAnalyticsConfiguration".to_string())
                    } else {
                        Some("ListBucketAnalyticsConfigurations".to_string())
                    }
                } else if q("metrics") {
                    let has_id = query_params
                        .get("id")
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    if has_id {
                        Some("GetBucketMetricsConfiguration".to_string())
                    } else {
                        Some("ListBucketMetricsConfigurations".to_string())
                    }
                } else if q("intelligent-tiering") {
                    let has_id = query_params
                        .get("id")
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    if has_id {
                        Some("GetBucketIntelligentTieringConfiguration".to_string())
                    } else {
                        Some("ListBucketIntelligentTieringConfigurations".to_string())
                    }
                } else if q("inventory") {
                    let has_id = query_params
                        .get("id")
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    if has_id {
                        Some("GetBucketInventoryConfiguration".to_string())
                    } else {
                        Some("ListBucketInventoryConfigurations".to_string())
                    }
                } else if q("uploads") {
                    Some("ListMultipartUploads".to_string())
                } else if query_params.get("list-type").map(String::as_str) == Some("2") {
                    Some("ListObjectsV2".to_string())
                } else {
                    Some("ListObjects".to_string())
                }
            }
            "PUT" => {
                if q("versioning") {
                    Some("PutBucketVersioning".to_string())
                } else if q("policy") {
                    Some("PutBucketPolicy".to_string())
                } else if q("cors") {
                    Some("PutBucketCors".to_string())
                } else if q("lifecycle") {
                    Some("PutBucketLifecycle".to_string())
                } else if q("website") {
                    Some("PutBucketWebsite".to_string())
                } else if q("logging") {
                    Some("PutBucketLogging".to_string())
                } else if q("notification") {
                    Some("PutBucketNotification".to_string())
                } else if q("tagging") {
                    Some("PutBucketTagging".to_string())
                } else if q("acl") {
                    Some("PutBucketAcl".to_string())
                } else if q("encryption") {
                    Some("PutBucketEncryption".to_string())
                } else if q("replication") {
                    Some("PutBucketReplication".to_string())
                } else if q("accelerate") {
                    Some("PutBucketAccelerateConfiguration".to_string())
                } else if q("requestPayment") {
                    Some("PutBucketRequestPayment".to_string())
                } else if q("ownershipControls") {
                    Some("PutBucketOwnershipControls".to_string())
                } else if q("publicAccessBlock") {
                    Some("PutPublicAccessBlock".to_string())
                } else if q("object-lock") {
                    Some("PutObjectLockConfiguration".to_string())
                } else if q("analytics") {
                    Some("PutBucketAnalyticsConfiguration".to_string())
                } else if q("metrics") {
                    Some("PutBucketMetricsConfiguration".to_string())
                } else if q("intelligent-tiering") {
                    Some("PutBucketIntelligentTieringConfiguration".to_string())
                } else if q("inventory") {
                    Some("PutBucketInventoryConfiguration".to_string())
                } else if q("metadataInventoryTable") {
                    Some("UpdateBucketMetadataInventoryTableConfiguration".to_string())
                } else if q("metadataJournalTable") {
                    Some("UpdateBucketMetadataJournalTableConfiguration".to_string())
                } else {
                    Some("CreateBucket".to_string())
                }
            }
            "DELETE" => {
                if q("policy") {
                    Some("DeleteBucketPolicy".to_string())
                } else if q("cors") {
                    Some("DeleteBucketCors".to_string())
                } else if q("lifecycle") {
                    Some("DeleteBucketLifecycle".to_string())
                } else if q("website") {
                    Some("DeleteBucketWebsite".to_string())
                } else if q("tagging") {
                    Some("DeleteBucketTagging".to_string())
                } else if q("encryption") {
                    Some("DeleteBucketEncryption".to_string())
                } else if q("replication") {
                    Some("DeleteBucketReplication".to_string())
                } else if q("ownershipControls") {
                    Some("DeleteBucketOwnershipControls".to_string())
                } else if q("publicAccessBlock") {
                    Some("DeletePublicAccessBlock".to_string())
                } else if q("metadataConfiguration") {
                    Some("DeleteBucketMetadataConfiguration".to_string())
                } else if q("metadataTable") {
                    Some("DeleteBucketMetadataTableConfiguration".to_string())
                } else if q("analytics") {
                    Some("DeleteBucketAnalyticsConfiguration".to_string())
                } else if q("metrics") {
                    Some("DeleteBucketMetricsConfiguration".to_string())
                } else if q("intelligent-tiering") {
                    Some("DeleteBucketIntelligentTieringConfiguration".to_string())
                } else if q("inventory") {
                    Some("DeleteBucketInventoryConfiguration".to_string())
                } else {
                    Some("DeleteBucket".to_string())
                }
            }
            "POST" => {
                if q("delete") {
                    Some("DeleteObjects".to_string())
                } else if q("notification") {
                    Some("PutBucketNotification".to_string())
                } else if q("uploads") {
                    Some("CreateMultipartUpload".to_string())
                } else if q("metadataConfiguration") {
                    Some("CreateBucketMetadataConfiguration".to_string())
                } else if q("metadataTable") {
                    Some("CreateBucketMetadataTableConfiguration".to_string())
                } else {
                    None
                }
            }
            "HEAD" => Some("HeadBucket".to_string()),
            _ => None,
        }
    }
}

impl Default for S3Handler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::Value;

    fn make_req(
        operation: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        body: &[u8],
    ) -> AwsRequest {
        AwsRequest {
            service: "s3".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: bucket.map(String::from),
            key: key.map(String::from),
            query_params: HashMap::new(),
            headers: HashMap::new(),
            method: "GET".to_string(),
            body: Bytes::copy_from_slice(body),
            params: Value::Null,
        }
    }

    #[test]
    fn test_create_and_list_buckets() {
        let handler = S3Handler::new();

        let req = make_req("CreateBucket", Some("my-bucket"), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(String::from_utf8_lossy(&resp.body).contains("my-bucket"));

        let req = make_req("ListBuckets", None, None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(String::from_utf8_lossy(&resp.body).contains("my-bucket"));
    }

    #[test]
    fn test_put_and_get_object() {
        let handler = S3Handler::new();

        handler.handle(make_req("CreateBucket", Some("test-bucket"), None, b""));

        let req = make_req(
            "PutObject",
            Some("test-bucket"),
            Some("hello.txt"),
            b"Hello, World!",
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);

        let req = make_req("GetObject", Some("test-bucket"), Some("hello.txt"), b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"Hello, World!");
    }

    #[test]
    fn test_put_get_object_metadata() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("test-bucket"), None, b""));

        let mut req = make_req(
            "PutObject",
            Some("test-bucket"),
            Some("meta.txt"),
            b"body",
        );
        req.headers
            .insert("x-amz-meta-author".to_string(), "X".to_string());
        handler.handle(req);

        let resp = handler.handle(make_req(
            "HeadObject",
            Some("test-bucket"),
            Some("meta.txt"),
            b"",
        ));
        assert_eq!(resp.status, 200);
        let headers: HashMap<String, String> = resp.headers.clone().into_iter().collect();
        assert_eq!(headers.get("x-amz-meta-author").map(String::as_str), Some("X"));
    }

    #[test]
    fn test_range_get() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("test-bucket"), None, b""));
        handler.handle(make_req(
            "PutObject",
            Some("test-bucket"),
            Some("r.txt"),
            b"0123456789",
        ));

        let mut req = make_req("GetObject", Some("test-bucket"), Some("r.txt"), b"");
        req.headers.insert("range".to_string(), "bytes=1-2".to_string());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 206);
        assert_eq!(resp.body, b"12");
        let headers: HashMap<String, String> = resp.headers.into_iter().collect();
        assert_eq!(
            headers.get("Content-Range").map(String::as_str),
            Some("bytes 1-2/10")
        );
    }

    #[test]
    fn test_versioning_flow() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("ver"), None, b""));
        // enable
        let mut vreq = make_req("PutBucketVersioning", Some("ver"), None, b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>");
        vreq.query_params.insert("versioning".to_string(), String::new());
        handler.handle(vreq);

        handler.handle(make_req("PutObject", Some("ver"), Some("k"), b"one"));
        handler.handle(make_req("PutObject", Some("ver"), Some("k"), b"two"));

        let mut listreq = make_req("ListObjectVersions", Some("ver"), None, b"");
        listreq.query_params.insert("prefix".to_string(), String::new());
        let resp = handler.handle(listreq);
        let body = String::from_utf8_lossy(&resp.body).to_string();
        assert_eq!(body.matches("<Version>").count(), 2);
    }

    #[test]
    fn test_delete_object() {
        let handler = S3Handler::new();

        handler.handle(make_req("CreateBucket", Some("test-bucket"), None, b""));
        handler.handle(make_req(
            "PutObject",
            Some("test-bucket"),
            Some("file.txt"),
            b"data",
        ));

        let req = make_req("DeleteObject", Some("test-bucket"), Some("file.txt"), b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 204);

        let req = make_req("GetObject", Some("test-bucket"), Some("file.txt"), b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_nonexistent_bucket() {
        let handler = S3Handler::new();
        let req = make_req("HeadBucket", Some("no-such-bucket"), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 404);
        assert!(String::from_utf8_lossy(&resp.body).contains("NoSuchBucket"));
    }

    #[test]
    fn test_invalid_bucket_name() {
        let handler = S3Handler::new();
        let req = make_req("CreateBucket", Some("a"), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn test_bucket_not_empty_delete() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("test-bucket"), None, b""));
        handler.handle(make_req("PutObject", Some("test-bucket"), Some("file"), b"data"));

        let req = make_req("DeleteBucket", Some("test-bucket"), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 409);
    }

    #[test]
    fn test_detection_object_and_bucket_ops() {
        let mut q: HashMap<String, String> = HashMap::new();
        q.insert("uploads".to_string(), String::new());
        assert_eq!(
            S3Handler::detect_s3_operation("GET", "/bucket", &q),
            Some("ListMultipartUploads".to_string())
        );
        q.clear();
        q.insert("uploadId".to_string(), "u".to_string());
        q.insert("partNumber".to_string(), "1".to_string());
        assert_eq!(
            S3Handler::detect_s3_operation("PUT", "/b/k", &q),
            Some("UploadPart".to_string())
        );
        q.clear();
        q.insert("notification".to_string(), String::new());
        assert_eq!(
            S3Handler::detect_s3_operation("GET", "/b", &q),
            Some("GetBucketNotificationConfiguration".to_string())
        );
        assert_eq!(
            S3Handler::detect_s3_operation("GET", "/", &HashMap::new()),
            Some("ListBuckets".to_string())
        );
    }
}
