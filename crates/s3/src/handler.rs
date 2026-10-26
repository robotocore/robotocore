//! S3 operation handler.
//!
//! REST-XML protocol: responses are raw XML; state is in-memory per
//! (account, region). Requests are routed via method + path + query params.

use base64::Engine;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use sha2::Sha256;
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

/// CRC-32 (IEEE), required for AWS event-stream framing.
pub fn crc32(data: &[u8]) -> u32 {
    const fn make_table() -> [u32; 256] {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    }
    static TABLE: [u32; 256] = make_table();
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Encode a single AWS event-stream message frame.
fn event_stream_frame(event_type: &str, content_type: Option<&str>, payload: &[u8]) -> Vec<u8> {
    let mut headers = Vec::new();
    let mut push_header = |name: &str, value: &[u8], vtype: u8| {
        headers.push(name.len() as u8);
        headers.extend_from_slice(name.as_bytes());
        headers.push(vtype);
        // STRING (0x07) and BYTE_ARRAY (0x06) values carry a 2-byte BE length.
        headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
        headers.extend_from_slice(value);
    };
    push_header(":message-type", b"event", 0x07);
    push_header(":event-type", event_type.as_bytes(), 0x07);
    if let Some(ct) = content_type {
        push_header(":content-type", ct.as_bytes(), 0x07);
    }

    let headers_len = headers.len() as u32;
    let total_len = (12 + headers.len() + payload.len() + 4) as u32;

    let mut prelude = Vec::with_capacity(8);
    prelude.extend_from_slice(&total_len.to_be_bytes());
    prelude.extend_from_slice(&headers_len.to_be_bytes());
    let prelude_crc = crc32(&prelude);

    let mut message = Vec::with_capacity(total_len as usize);
    message.extend_from_slice(&prelude);
    message.extend_from_slice(&prelude_crc.to_be_bytes());
    message.extend_from_slice(&headers);
    message.extend_from_slice(payload);
    let message_crc = crc32(&message);
    message.extend_from_slice(&message_crc.to_be_bytes());
    message
}

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
// POST object form parsing
// ---------------------------------------------------------------------------

/// A browser form upload body, parsed from `multipart/form-data`: the form's
/// named text fields in wire order, plus the `file` part's bytes.
struct PostForm {
    /// `name` → value, first occurrence wins, case-insensitive lookup.
    fields: Vec<(String, String)>,
    /// The raw payload of the `file` field (a file part's bytes, or the value
    /// of a plain `file` field when a client posts the content as a string).
    file: Option<Vec<u8>>,
    /// `filename` attribute of the file part (used for `${filename}`).
    file_filename: Option<String>,
    /// `Content-Type` header of the file part, if the client sent one.
    file_content_type: Option<String>,
}

impl PostForm {
    // POST field names are matched the way AWS does it (the docs list fields
    // by exact capitalization, but browsers and SDKs disagree in practice —
    // localstack scans them case-insensitively through a lowercase dict).
    fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn contains(&self, name: &str) -> bool {
        self.fields
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(name))
    }
}

/// Extract the `boundary` parameter of a `Content-Type: multipart/form-data`
/// header. Quoted boundaries are unquoted.
fn parse_boundary(content_type: &str) -> Option<String> {
    for param in content_type.split(';') {
        let param = param.trim();
        let Some((name, value)) = param.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("boundary") {
            let value = value.trim();
            let unquoted = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            return Some(unquoted.to_string());
        }
    }
    None
}

/// First occurrence of `needle` in `hay` at or after `from`.
fn mem_find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() || from >= hay.len() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// `Content-Disposition` parameters of one part: `name="key"`,
/// `filename="f.txt"` (quoted or quoted-fresh values both tolerated).
fn disposition_param(headers: &str, param: &str) -> Option<String> {
    for line in headers.split("\r\n") {
        let (name, value) = line.split_once(':')?;
        if !name.trim().eq_ignore_ascii_case("content-disposition") {
            continue;
        }
        for segment in value.split(';') {
            let segment = segment.trim();
            if segment.len() > param.len() && segment[..param.len()].eq_ignore_ascii_case(param) {
                let rest = segment[param.len()..].trim_start_matches('=').trim();
                let value = rest
                    .strip_prefix('"')
                    .and_then(|v| v.strip_suffix('"'))
                    .unwrap_or(rest);
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Value of a plain header line inside one part (`Content-Type: text/plain`).
fn part_header_value(headers: &str, name: &str) -> Option<String> {
    headers.split("\r\n").find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

// ---------------------------------------------------------------------------
// POST policy (SigV4 form upload) signature validation
// ---------------------------------------------------------------------------

/// The form fields a SigV4 pre-signed POST carries (localstack
/// presigned_url.py SIGNATURE_V4_POST_FIELDS) and the legacy SigV2 pair.
const POST_V4_FIELDS: [&str; 4] = [
    "x-amz-signature",
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
];
const POST_V2_FIELDS: [&str; 2] = ["signature", "awsaccesskeyid"];

/// The canned ACL names AWS documents (ACL overview table; moto's
/// get_canned_acl accepts the same set, raising for anything else). Used by
/// POST Object's `x-amz-acl`/`acl` form field — the twin stores no ACL
/// state, so a valid canned name passes like PUT's x-amz-acl header does
/// and only unknown names are rejected.
const CANNED_ACLS: [&str; 8] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
    "log-delivery-write",
];

/// The signing secret the twin underwrites for POST-upload verification.
///
/// The twin's documented credential model is `aws_access_key_id` =
/// any 12-digit number, `aws_secret_access_key` = `test` (AGENTS.md
/// "Endpoint & credentials"). A real AWS deployment verifies the POST
/// signature against the IAM secret of the credentials in the form; the twin
/// has no secret store (any non-empty secret is accepted on signed
/// requests), so POST uploads — the one S3 wire path where the twin checks
/// a signature — pin the verification secret to the documented pair.
/// Clients using the documented credentials behave exactly as on real AWS;
/// a signature produced with any other secret answers SignatureDoesNotMatch,
/// same as a real server would for a wrong key.
const POST_UPLOAD_SIGNING_SECRET: &str = "test";

/// Which signature version the form declares, detected the localstack way
/// (`_is_match_with_signature_fields`): knowing ANY field of a version's
/// field set requires ALL of them, else the missing field names an
/// InvalidArgument error from the caller.
enum PostSigVersion {
    /// Complete SigV4 field set; carries the hex signature form value.
    V4 { signature: String },
    /// Complete legacy SigV2 pair; carrying no verification (see the
    /// residual-gaps notes: the twin validates v4 signatures only).
    V2,
    /// Any subset of one field set, naming the first missing field.
    Incomplete(&'static str),
    /// Neither version's fields appear at all.
    Anonymous,
}

fn post_signature_version(form: &PostForm) -> PostSigVersion {
    let missing = |fields: &[&'static str]| -> Option<&'static str> {
        fields.iter().find(|f| !form.contains(f)).copied()
    };
    let any_v4 = POST_V4_FIELDS.iter().any(|f| form.contains(f));
    let any_v2 = POST_V2_FIELDS.iter().any(|f| form.contains(f));
    if any_v4 {
        return match missing(&POST_V4_FIELDS) {
            Some(field) => PostSigVersion::Incomplete(field),
            None => PostSigVersion::V4 {
                signature: form.get("x-amz-signature").unwrap_or_default().to_string(),
            },
        };
    }
    if any_v2 {
        return match missing(&POST_V2_FIELDS) {
            Some(field) => PostSigVersion::Incomplete(field),
            None => PostSigVersion::V2,
        };
    }
    PostSigVersion::Anonymous
}

/// The user-facing field name (localstack's capitalize helper + its
/// AWSAccessKeyId special case).
fn post_argument_display_name(field: &str) -> &str {
    match field {
        "x-amz-signature" => "X-Amz-Signature",
        "x-amz-algorithm" => "X-Amz-Algorithm",
        "x-amz-credential" => "X-Amz-Credential",
        "x-amz-date" => "X-Amz-Date",
        "signature" => "Signature",
        "awsaccesskeyid" => "AWSAccessKeyId",
        other => other,
    }
}

/// HMAC-SHA256 of `data` under `key`.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// The expected SigV4 POST-upload signature: `hex(HMAC(signing_key,
/// base64(policy)))` — the signature covers the base64 policy document and
/// nothing else (AWS "Constructing an AWS4-HMAC-SHA256 POST policy").
/// `None` when the credential scope is not a SigV4 string.
fn expected_post_signature(
    secret: &str,
    amz_date: &str,
    amz_credential: &str,
    string_to_sign: &str,
) -> Option<String> {
    // Scope: <access-key>/<YYYYMMDD>/<region>/s3/aws4_request
    let parts: Vec<&str> = amz_credential.split('/').collect();
    if parts.len() != 5 {
        return None;
    }
    let date = amz_date.get(..8)?;
    let region = parts[2];
    let key_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let key_region = hmac_sha256(&key_date, region.as_bytes());
    let key_service = hmac_sha256(&key_region, b"s3");
    let key_signing = hmac_sha256(&key_service, b"aws4_request");
    Some(hex::encode(hmac_sha256(
        &key_signing,
        string_to_sign.as_bytes(),
    )))
}

/// The base64-encoded policy document is the SigV4 POST string-to-sign;
/// a policy that does not decode into a JSON object means the request was
/// tampered with, which real AWS reports as a signature mismatch
/// (localstack validate_post_policy comment: "this means the policy has
/// been tampered with").
struct PostPolicy {
    expiration: Option<String>,
    conditions: Vec<serde_json::Value>,
}

/// The two expiration formats the AWS docs allow in a POST policy
/// (localstack POLICY_EXPIRATION_FORMAT1/2).
fn parse_policy_expiration(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(value, fmt) {
            return Some(naive.and_utc());
        }
    }
    None
}

/// A POST policy JSON value as an integer (localstack coerces
/// content-length-range bounds with int(), which reads both JSON numbers
/// and numeric strings).
fn as_policy_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
}

/// Percent-encode an object key for a Location header's path, preserving the
/// `/` separators (localstack's urlparser.quote default safe='/').
fn encoded_key(key: &str) -> String {
    key.split('/')
        .map(urlencoding::encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// Parse a `multipart/form-data` body the way the form upload wire path
/// needs it: named text fields plus the file part. Mirrors what werkzeug
/// (localstack's context.request.form/files) and moto's Flask request give
/// their handlers: field values decoded as text, the file part raw.
/// `None` → not a parseable multipart body (MalformedPOSTRequest).
fn parse_multipart_form(body: &[u8], content_type: &str) -> Option<PostForm> {
    let boundary = parse_boundary(content_type)?;
    if boundary.is_empty() || body.is_empty() {
        return None;
    }

    let mut part_delim = Vec::with_capacity(boundary.len() + 2);
    part_delim.extend_from_slice(b"--");
    part_delim.extend_from_slice(boundary.as_bytes());
    let mut next_delim = Vec::with_capacity(part_delim.len() + 2);
    next_delim.extend_from_slice(b"\r\n");
    next_delim.extend_from_slice(&part_delim);

    // Preamble (everything before the first --boundary) is discarded.
    let mut pos = mem_find(body, &part_delim, 0)? + part_delim.len();
    let mut saw_terminal = false;
    let mut form = PostForm {
        fields: Vec::new(),
        file: None,
        file_filename: None,
        file_content_type: None,
    };

    loop {
        if pos >= body.len() {
            break;
        }
        // `--boundary--` terminal marker (optional trailing transport padding
        // after it is ignored, matching real parsers).
        if body[pos..].starts_with(b"--") {
            saw_terminal = true;
            break;
        }
        // Each part's headers begin right after CRLF.
        if !body[pos..].starts_with(b"\r\n") {
            return None;
        }
        pos += 2;

        let headers_end = mem_find(body, b"\r\n\r\n", pos)? + 4;
        let header_block = String::from_utf8_lossy(&body[pos..headers_end - 4]).to_string();
        let (content_end, terminated) = match mem_find(body, &next_delim, headers_end) {
            Some(end) => (end, true),
            None => {
                // Tolerate a final part whose closing delimiter arrives as a
                // bare `--boundary` (no leading CRLF) — rare client quirk.
                let tail = mem_find(body, &part_delim, headers_end)? + part_delim.len();
                if !body[tail..].starts_with(b"--") {
                    return None;
                }
                (tail - part_delim.len(), false)
            }
        };
        if content_end < headers_end {
            return None;
        }
        let payload = &body[headers_end..content_end];
        let name = disposition_param(&header_block, "name").unwrap_or_default();

        if name.eq_ignore_ascii_case("file") {
            // The file payload: a real file part carries a filename
            // attribute; a name="file" text part also carries uploadable
            // content when the client posted it as a plain form string
            // (localstack's `if "file" in form` branch). First wins.
            if form.file.is_none() {
                form.file = Some(payload.to_vec());
            }
            if form.file_filename.is_none() {
                form.file_filename = disposition_param(&header_block, "filename");
            }
            if form.file_content_type.is_none() {
                form.file_content_type = part_header_value(&header_block, "content-type");
            }
        } else if !name.is_empty() && form.get(&name).is_none() {
            form.fields
                .push((name, String::from_utf8_lossy(payload).into_owned()));
        }

        if terminated {
            pos = content_end + next_delim.len();
        } else {
            pos = content_end + part_delim.len();
        }
    }

    // A body that simply runs out before the closing `--boundary--` marker
    // was never terminated: the multipart envelope is truncated (the Python
    // form parsers this mirror refuse it — the loop must not "succeed" at
    // end-of-body). MalformedPOSTRequest.
    if !saw_terminal {
        return None;
    }
    Some(form)
}

/// The S3 service handler.
pub struct S3Handler {
    /// Per (account, region) state.
    pub(crate) state: RwLock<HashMap<(u64, String), S3State>>,
}

impl S3Handler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> S3State {
        let mut states = self.state.write();
        let state = states
            .entry((account, region.to_string()))
            .or_default()
            .clone();
        // Expose the live state to other crates via the bridge.
        crate::bridge::register_state(account, region, &state);
        state
    }

    fn with_bucket(&self, req: &AwsRequest) -> Result<(S3State, Arc<Bucket>), AwsResponse> {
        let state = self.get_state(req.account, &req.region);
        let name = req.bucket.clone().unwrap_or_default();
        match state.get_bucket(&name) {
            Some(b) => Ok((state, b)),
            None => Err(AwsResponse::bucket_error(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist",
                &name,
            )),
        }
    }

    /// Bucket name carried in the Host header of a virtual-hosted-style
    /// request (`<bucket>.<endpoint-host>`).
    ///
    /// Botocore uses virtual-hosted addressing for S3 Express directory
    /// buckets even against a custom endpoint (endpoint ruleset rule
    /// `{Bucket}.{url#authority}`), so those requests arrive with an empty
    /// path-level bucket and the bucket only in `Host`. Mirrors the host
    /// shapes of the gateway's `s3_routing::parse_s3_vhost`: the
    /// `S3_HOSTNAME` custom base, `<bucket>.s3.<rest>`, and
    /// `<bucket>.localhost` (which is how botocore addresses a localhost
    /// custom endpoint).
    fn vhost_bucket_from_host(host: Option<&str>) -> Option<String> {
        let host = host?;
        let host = match host.rsplit_once(':') {
            Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h,
            _ => host,
        };
        if host.is_empty() {
            return None;
        }
        let hostname = std::env::var("S3_HOSTNAME")
            .unwrap_or_else(|_| "s3.localhost.robotocore.cloud".to_string());
        let custom_suffix = format!(".{}", hostname);
        if let Some(bucket) = host.strip_suffix(custom_suffix.as_str()) {
            if !bucket.is_empty() && !bucket.starts_with('.') {
                return Some(bucket.to_string());
            }
        }
        if let Some((bucket, _rest)) = host.split_once(".s3.") {
            if !bucket.is_empty() && !bucket.starts_with('.') {
                return Some(bucket.to_string());
            }
        }
        if let Some(label) = host.strip_suffix(".localhost") {
            if !label.is_empty() && !label.contains('.') {
                return Some(label.to_string());
            }
        }
        None
    }

    /// Rewrite a virtual-hosted-style request so bucket/key/operation match
    /// the path-style shapes the handler expects — the same transformation
    /// the gateway's `s3_routing::rewrite_vhost_to_path` performs for hosts
    /// it recognizes. For a virtual-hosted request the gateway pulled the
    /// key's first segment out of the path into `bucket`; move that segment
    /// under the Host-carried bucket and re-detect the operation from the
    /// equivalent path-style shape.
    ///
    /// Operation fusion: the operation arriving from the gateway was derived
    /// with the FULL query (its pre-dispatch detection sees value-less
    /// params); the re-detect here sees the crate's query_params, which the
    /// gateway's to_s3_request rebuilds keeping only k=v pairs — value-less
    /// params ("?session") are dropped. So an arriving operation wins unless
    /// it is one of the blind shape endpoints the lost-bucket path produces
    /// (ListBuckets/CreateBucket/...); the re-detect is authoritative for
    /// the reshaped shape in that case.
    ///
    /// Requests without a virtual-hosted Host (path-style callers) parse to
    /// None here and pass through untouched.
    fn normalize_virtual_host(req: &mut AwsRequest) {
        let host_bucket =
            match Self::vhost_bucket_from_host(req.headers.get("host").map(String::as_str)) {
                Some(b) => b,
                None => return,
            };
        // Rebuild the path-style shape: /{host-bucket}/{former-path}.
        let path_bucket = req.bucket.as_deref().unwrap_or("");
        let former_key = match (path_bucket, req.key.as_deref()) {
            ("", k) => k.filter(|s| !s.is_empty()).map(String::from),
            (b, Some(k)) if !k.is_empty() => Some(format!("{}/{}", b, k)),
            (b, _) => Some(b.to_string()),
        };
        let owned_path = match &former_key {
            Some(key) => format!("/{}/{}", host_bucket, key),
            None => format!("/{}", host_bucket),
        };
        // botocore percent-encodes special characters in path keys;
        // detect_s3_operation only inspects the path shape.
        let redetect = Self::detect_s3_operation(&req.method, &owned_path, &req.query_params);
        let arriving = req.operation.clone();
        let blind = |op: &str| {
            matches!(
                op,
                "" | "ListBuckets"
                    | "ListObjects"
                    | "ListObjectsV2"
                    | "CreateBucket"
                    | "HeadBucket"
                    | "DeleteBucket"
            )
        };
        req.operation = match redetect {
            Some(red) if !blind(&red) => red,
            _ if blind(&arriving) => redetect.unwrap_or(arriving),
            _ => arriving,
        };
        // botocore serializes RenameSource as the x-amz-rename-source
        // header, but the renameObject query param is value-less and the
        // gateway drops it from query_params — recover the op here.
        if req.method == "PUT"
            && req.operation == "PutObject"
            && req.headers.contains_key("x-amz-rename-source")
        {
            req.operation = "RenameObject".to_string();
        }
        req.bucket = Some(host_bucket);
        req.key = former_key;
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let mut req = req;
        Self::normalize_virtual_host(&mut req);
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
            // Browser-form POST upload (part of PutObject's wire surface):
            // multipart/form-data body carrying key/policy/signature/file.
            "PostObject" => self.post_object(&req),
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
            "PutBucketAnalyticsConfiguration" => {
                self.put_bucket_id_config(&req, IdConfig::Analytics)
            }
            "GetBucketAnalyticsConfiguration" => {
                self.get_bucket_id_config(&req, IdConfig::Analytics)
            }
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
            "ListBucketAnalyticsConfigurations" => {
                self.list_bucket_id_configs(&req, IdConfig::Analytics)
            }
            "ListBucketMetricsConfigurations" => {
                self.list_bucket_id_configs(&req, IdConfig::Metrics)
            }
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
            "SelectObjectContent" => self.select_object_content(&req),
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
            req.key
                .as_deref()
                .map(Self::decoded_key)
                .unwrap_or_default(),
        )
    }

    /// S3 object keys arrive from the gateway in the raw wire form: a
    /// percent-encoded path segment. Botocore quotes `%` and non-ASCII bytes;
    /// other sub-delims such as `=` may legally appear raw. Normalize to the
    /// decoded key once, here, so stored state, listing comparisons
    /// (prefix/marker/StartAfter), pagination tokens and lookups all use the
    /// decoded form — real AWS stores the decoded key (moto's S3 response
    /// layer decodes the same way; localstack discussion 54).
    ///
    /// A single decode keeps the round-trip faithful: a literal `%20` uploaded
    /// raw arrives as `%2520` and decodes exactly once to `%20`, never to a
    /// space. Parameters read from `query_params` are already decoded by the
    /// gateway, so `StartAfter`-style comparisons against state keys need no
    /// re-encoding either.
    fn decoded_key(key: &str) -> String {
        urlencoding::decode(key)
            .map(String::from)
            .unwrap_or_else(|_| key.to_string())
    }

    fn bucket_or_404(state: &S3State, name: &str) -> Result<Arc<Bucket>, AwsResponse> {
        state.get_bucket(name).ok_or_else(|| {
            AwsResponse::bucket_error(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist",
                name,
            )
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

    /// moto s3/models.py:1920: a directory bucket name must match
    /// r"--[a-z0-9]+-az\d+--x-s3$".
    fn directory_bucket_name_valid(name: &str) -> bool {
        let azid_azn = name
            .strip_suffix("--x-s3")
            .and_then(|rest| rest.rsplit_once("--"))
            .map(|(_, az)| az);
        match azid_azn {
            Some(az) => {
                // "<azid>-az<N>" — azid is lowercase alnum, N is digits.
                match az.rsplit_once("-az") {
                    Some((azid, n)) => {
                        !azid.is_empty()
                            && azid
                                .chars()
                                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                            && !n.is_empty()
                            && n.chars().all(|c| c.is_ascii_digit())
                    }
                    None => false,
                }
            }
            None => false,
        }
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
                return AwsResponse::error(
                    400,
                    "InvalidBucketName",
                    "The specified bucket is not valid",
                )
            }
        };

        // moto s3/responses.py _bucket_response_put → models.py create_bucket():
        // directory-ness comes from the parsed CreateBucketConfiguration/Bucket/Type
        // (is_directory_bucket == bucket_type == "Directory"); there is no
        // name-suffix inference. A directory bucket additionally must match
        // r"--[a-z0-9]+-az\d+--x-s3$" (InvalidBucketName otherwise).
        let body_str = String::from_utf8_lossy(&req.body);
        let bucket_type = xml_sections(&body_str, "Bucket")
            .first()
            .and_then(|section| xml_text(section, "Type"));
        let directory = bucket_type.as_deref() == Some("Directory");
        if !Self::valid_bucket_name(&bucket_name) {
            return AwsResponse::error(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid",
            );
        }
        if directory && !Self::directory_bucket_name_valid(&bucket_name) {
            return AwsResponse::error(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid",
            );
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
            return AwsResponse::bucket_error(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist",
                &name,
            );
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
            None => AwsResponse::error(
                404,
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
            ),
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

        Self::fire_event(
            req,
            &bucket,
            &key,
            "s3:ObjectCreated:Put",
            req.body.len(),
            &etag,
        );
        self.maybe_replicate(req, &state, &bucket, &key, req.body.to_vec());

        let mut resp = AwsResponse::xml(200, String::new());
        resp.headers
            .push(("ETag".to_string(), format!("\"{}\"", etag)));
        if version_id != NULL_VERSION_ID {
            resp.headers
                .push(("x-amz-version-id".to_string(), version_id));
        }
        resp
    }

    // ------------------------------------------------------------------
    // POST object — browser form upload (RESTObjectPOST)
    // ------------------------------------------------------------------

    /// Build an S3 error document with extra child elements, the shape moto's
    /// `S3ClientError` serialization emits for POST-policy failures
    /// (`Condition`, `AWSAccessKeyId`, `StringToSign`, `ProposedSize`, ...).
    /// Same wire shape as `AwsResponse::error`, plus the extensions.
    fn aws_error_with(
        status: u16,
        code: &str,
        message: &str,
        extra: &[(&str, &str)],
    ) -> AwsResponse {
        let mut body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>"#,
            xml_escape(code),
            xml_escape(message),
        );
        for (tag, value) in extra {
            body.push_str(&format!(
                "\n  <{}>{}</{}>",
                xml_escape(tag),
                xml_escape(value),
                xml_escape(tag)
            ));
        }
        body.push_str("\n</Error>");
        AwsResponse::xml(status, body)
    }

    /// The multipart/form-data browser upload (AWS "POST Object" wire path,
    /// exposed as `POST /{Bucket}` and `POST /{Bucket}/{Key}`; mirror of
    /// localstack provider.py `post_object` and moto
    /// `_bucket_response_post`). The form carries `key`, `policy` (base64
    /// JSON), the SigV4 signature fields, `file`, and the success controls.
    /// Validation order mirrors real AWS: parse the form, then policy +
    /// signature, then policy conditions, then store.
    fn post_object(&self, req: &AwsRequest) -> AwsResponse {
        let content_type = req.headers.get("content-type").cloned().unwrap_or_default();
        if !content_type.contains("multipart/form-data") {
            // localstack post_object: PreconditionFailed carrying the
            // Condition element real AWS includes on this failure.
            return Self::aws_error_with(
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
                &[(
                    "Condition",
                    "Bucket POST must be of the enclosure-type multipart/form-data",
                )],
            );
        }
        // moto _bucket_response_post 411s a POST without an explicit length;
        // the message text is real AWS's documented MissingContentLength
        // wording (moto's plain "Content-Length required" sentinel is not
        // what hits the wire on real AWS).
        if !req.headers.contains_key("content-length") {
            return AwsResponse::error(
                411,
                "MissingContentLength",
                "You must provide the content-length HTTP header.",
            );
        }

        let form = match parse_multipart_form(&req.body, &content_type) {
            Some(form) => form,
            None => {
                return AwsResponse::error(
                    400,
                    "MalformedPOSTRequest",
                    "The body of your POST request is not well-formed multipart/form-data.",
                )
            }
        };

        // moto resolves the account from the parsed form (its querystring IS
        // the form body), so an unsigned browser POST still isolates state
        // per access key; a real client never signs this wire path with an
        // Authorization header. The form's x-amz-credential/AWSAccessKeyId is
        // the only credential source here.
        let mut req = req.clone();
        Self::apply_post_form_account(&mut req, &form);

        let bucket_name = req.bucket.clone().unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        // The form's `key` field is authoritative on the wire and is stored
        // VERBATIM: a multipart field value is form payload, not a URI path
        // component, so percent sequences in it are literal. moto keys the
        // object from the parsed form value as-is (its form IS the
        // querystring; s3/responses.py `_bucket_response_post` reads
        // `self.querystring["key"][0]`), and real AWS does the same. Only the
        // URL key (POST /{Bucket}/{Key}), the fallback when the form omits
        // the field, is a path segment and keeps the crate's decoded_key.
        let key = match form.get("key") {
            Some(k) => {
                let mut k = k.to_string();
                // moto replaces ${filename} with the uploaded file's basename.
                if k.contains("${filename}") {
                    if let Some(name) = form.file_filename.as_deref() {
                        k = k.replace("${filename}", name);
                    }
                }
                k
            }
            None => Self::bucket_and_key(&req).1,
        };
        // localstack validate_post_policy raises InvalidArgument when the key
        // field is absent or empty.
        if key.is_empty() {
            return Self::aws_error_with(
                400,
                "InvalidArgument",
                "Bucket POST must contain a field named 'key'.  If it is specified, please check the order of the fields.",
                &[("ArgumentName", "key"), ("ArgumentValue", "")],
            );
        }

        let file = match form.file.as_deref() {
            Some(f) => f,
            // Real AWS answers MalformedPOSTRequest when the form carries no
            // file payload at all.
            None => {
                return AwsResponse::error(
                    400,
                    "MalformedPOSTRequest",
                    "The body of your POST request is not well-formed multipart/form-data.",
                )
            }
        };

        // Policy + signature validation, in the AWS wire order: decode the
        // policy → expiry → signature → conditions → covered-field rule. A
        // form without a policy document is the publicly-writable-door shape
        // moto and localstack both store (localstack validate_post_policy
        // early-returns when no policy field is present).
        let policy_b64 = form.get("policy").unwrap_or("");
        if !policy_b64.is_empty() {
            let policy = match Self::decode_post_policy(policy_b64, &form) {
                Ok(policy) => policy,
                Err(e) => return e,
            };
            if let Err(e) = Self::check_post_policy_expiration(&policy) {
                return e;
            }
            if let Err(e) = Self::verify_post_signature(policy_b64, &form) {
                return e;
            }
            // Conditions (localstack walk order), then the covered-field doc
            // rule, then store.
            if let Err(e) = Self::check_post_conditions(&policy, &form, &bucket_name, file.len()) {
                return e;
            }
            if let Err(e) = Self::check_post_fields_covered(&policy, &form) {
                return e;
            }
        }

        // System metadata the form may set (localstack
        // post_system_settable_headers): the Content-Type form field wins,
        // else the file part's declared type, else the crate default.
        let content_type = form
            .get("content-type")
            .map(String::from)
            .or_else(|| form.file_content_type.clone())
            .unwrap_or_else(|| "binary/octet-stream".to_string());
        // The canned-ACL POST field (AWS documents x-amz-acl as the header
        // the canned value arrives under; moto's form-as-querystring check
        // reads the same name bare, `acl`). Valid canned names pass the way
        // PUT's x-amz-acl header does — the twin stores no ACL state — and
        // an unknown name is a bad argument, the way real AWS rejects an
        // unsupported canned value (moto's oracle path
        // `get_canned_acl` raises on the same inputs).
        let acl = form
            .get("x-amz-acl")
            .or_else(|| form.get("acl"))
            .unwrap_or("");
        if !acl.is_empty() && !CANNED_ACLS.contains(&acl) {
            return Self::aws_error_with(
                400,
                "InvalidArgument",
                &format!("Invalid argument: {acl}"),
                &[("ArgumentName", "x-amz-acl"), ("ArgumentValue", acl)],
            );
        }
        let storage_class = form
            .get("x-amz-storage-class")
            .unwrap_or("STANDARD")
            .to_string();
        // x-amz-meta-* form fields become object user metadata (localstack
        // post_object user_metadata loop). Multipart field names arrive with
        // the client's own casing (unlike gateway headers, which are
        // lowercased), so the strip is case-insensitive — an X-Amz-Meta-...
        // field is the same metadata spelling.
        let metadata: HashMap<String, String> = form
            .fields
            .iter()
            .filter_map(|(k, v)| {
                k.to_ascii_lowercase()
                    .strip_prefix("x-amz-meta-")
                    .map(|rest| (rest.to_ascii_lowercase(), v.clone()))
            })
            .collect();

        let (version_id, etag) = Self::store_bytes(
            &bucket,
            &key,
            file.to_vec(),
            content_type,
            metadata,
            HashMap::new(),
            storage_class,
        );

        Self::fire_event(
            &req,
            &bucket,
            &key,
            "s3:ObjectCreated:Post",
            file.len(),
            &etag,
        );
        self.maybe_replicate(&req, &state, &bucket, &key, file.to_vec());

        // Location points at the object URL (localstack's
        // get_url_encoded_object_location): host-aware path-style so it is
        // directly refetchable, with the AWS vhost shape as the host-less
        // fallback.
        let upstream = Self::post_object_location(&req, &bucket_name, &encoded_key(&key));

        // Success mapping, moto order: success_action_redirect wins, then
        // success_action_status, else 204. The redirect URL gets
        // bucket/key/etag appended (localstack
        // create_redirect_for_post_request); if S3 cannot interpret the URL
        // it acts as if the field was not present.
        if let Some(redirect) = form.get("success_action_redirect") {
            if let Some(url) = Self::success_redirect_url(redirect, &bucket_name, &key, &etag) {
                return Self::post_success_response(303, &url, None, &etag, &version_id);
            }
        }
        let immediate = form.get("success_action_status").and_then(|s| match s {
            "200" => Some(200),
            "201" => Some(201),
            _ => None,
        });
        let status = immediate.unwrap_or(204);
        let body_201 = (status == 201).then(|| {
            format!(
                r#"<PostResponse>
  <Location>{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"{}"</ETag>
</PostResponse>"#,
                xml_escape(&upstream),
                xml_escape(&bucket_name),
                xml_escape(&key),
                xml_escape(&etag),
            )
        });
        Self::post_success_response(status, &upstream, body_201.as_deref(), &etag, &version_id)
    }

    /// Location/ETag/version headers shared by all POST upload successes.
    fn post_success_response(
        status: u16,
        location: &str,
        body_201: Option<&str>,
        etag: &str,
        version_id: &str,
    ) -> AwsResponse {
        let mut resp = match (status, body_201) {
            (201, Some(body)) => AwsResponse::xml(201, body.to_string()),
            (201, None) | (200, _) => AwsResponse::xml(status, String::new()),
            _ => AwsResponse::no_content(status),
        };
        resp.headers
            .push(("Location".to_string(), location.to_string()));
        resp.headers
            .push(("ETag".to_string(), format!("\"{etag}\"")));
        if version_id != NULL_VERSION_ID {
            resp.headers
                .push(("x-amz-version-id".to_string(), version_id.to_string()));
        }
        resp
    }

    /// The object URL a POST success Location header points at. The request
    /// arrives with normalize_virtual_host's path-style rewrite ALREADY
    /// applied, so vhost-ness is re-detected from the Host header this code
    /// never rewrote: a virtual-hosted caller (Host names the bucket) gets
    /// `http://{host}/{key}` — real S3's Location for a virtual-hosted
    /// POST — instead of the path-style shape that would double-book the
    /// bucket and not refetch. Path-style stays `http://{host}/{bucket}/{key}`;
    /// a request without any Host keeps the AWS canonical vhost shape.
    /// X-Forwarded-Proto (first hop, when the caller forwarded TLS) decides
    /// the scheme; plain HTTP otherwise.
    fn post_object_location(req: &AwsRequest, bucket: &str, encoded_key: &str) -> String {
        let scheme = req
            .headers
            .get("x-forwarded-proto")
            .and_then(|v| v.split(',').next())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "http".to_string());
        match req.headers.get("host").map(String::as_str) {
            Some(host)
                if !host.is_empty() && Self::vhost_bucket_from_host(Some(host)).is_some() =>
            {
                format!("{scheme}://{host}/{encoded_key}")
            }
            Some(host) if !host.is_empty() => {
                format!("{scheme}://{host}/{bucket}/{encoded_key}")
            }
            _ => format!("{scheme}://{bucket}.s3.amazonaws.com/{encoded_key}"),
        }
    }

    /// Append `key`/`bucket`/`etag` to a `success_action_redirect` URL
    /// (localstack create_redirect_for_post_request: parse, append, rejoin).
    /// `None` when the value is not an absolute URL — real AWS then treats
    /// the field as if it was not present.
    fn success_redirect_url(
        base_redirect: &str,
        bucket: &str,
        key: &str,
        etag: &str,
    ) -> Option<String> {
        let (scheme, rest) = base_redirect.split_once("://")?;
        if scheme.is_empty() || rest.is_empty() {
            return None;
        }
        let (authority, rest) = rest.split_once('/')?;
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, q),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return None;
        }
        let mut queryargs: Vec<String> = query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .collect();
        queryargs.push(format!("key={}", urlencoding::encode(key)));
        queryargs.push(format!("bucket={}", urlencoding::encode(bucket)));
        queryargs.push(format!("etag={}", urlencoding::encode(etag)));
        Some(format!(
            "{scheme}://{authority}/{path}?{}",
            queryargs.join("&"),
        ))
    }

    /// moto resolves the account from the form's access key the same way as
    /// for query-string credentials (form-fields-as-querystring in
    /// moto/core/responses.py get_access_key). Only reconsidered when the
    /// request kept none of the higher-priority sources (moto order:
    /// account header, Authorization, access key, default fallback).
    fn apply_post_form_account(req: &mut AwsRequest, form: &PostForm) {
        if req.headers.contains_key("x-robotocore-account")
            || req.headers.contains_key("authorization")
        {
            return;
        }
        if req.account != 123456789012 {
            return;
        }
        // SigV4: x-amz-credential is "<access-key>/<date>/<region>/s3/aws4_request".
        // SigV2: AWSAccessKeyId carries the key directly.
        let access_key = form
            .get("x-amz-credential")
            .and_then(|c| c.split('/').next())
            .or_else(|| form.get("awsaccesskeyid"))
            .unwrap_or("");
        if access_key.len() == 12 && access_key.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(account) = access_key.parse::<u64>() {
                req.account = account;
            }
        }
    }

    /// The `SignatureDoesNotMatch` document real AWS answers when the
    /// server-side check fails: the AWSAccessKeyId attribute the error carries
    /// plus the provided signature and the string that was signed.
    fn signature_does_not_match_error(
        provided: &str,
        string_to_sign: &str,
        access_key: &str,
    ) -> AwsResponse {
        Self::aws_error_with(
            403,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
            &[
                ("AWSAccessKeyId", access_key),
                ("SignatureProvided", provided),
                ("StringToSign", string_to_sign),
            ],
        )
    }

    /// Base64-decode + JSON-parse the form's policy document. Failures mean
    /// the request was tampered with → SignatureDoesNotMatch (real AWS
    /// cannot confirm a signature over an unreadable policy either).
    fn decode_post_policy(policy_b64: &str, form: &PostForm) -> Result<PostPolicy, AwsResponse> {
        let failed = || {
            let access_key = form
                .get("x-amz-credential")
                .and_then(|c| c.split('/').next())
                .or_else(|| form.get("awsaccesskeyid"))
                .unwrap_or("")
                .to_string();
            let provided = form
                .get("x-amz-signature")
                .or_else(|| form.get("signature"))
                .unwrap_or("");
            Self::signature_does_not_match_error(provided, policy_b64, &access_key)
        };
        // Python's base64 decoder (the moto/localstack signing path, and how
        // most real-world producers wrap PEM-style) ignores ASCII
        // whitespace, so a line-wrapped policy field is a valid POST there.
        // Strip the whitespace before decoding — the RAW string still acts
        // as the SigV4 string-to-sign (expected_post_signature), exactly as
        // moto signs it.
        let compact: String = policy_b64
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(compact.as_bytes())
            .map_err(|_| failed())?;
        let text = String::from_utf8(raw).map_err(|_| failed())?;
        let value: serde_json::Value = serde_json::from_str(&text).map_err(|_| failed())?;
        if !value.is_object() {
            return Err(failed());
        }
        let expiration = value
            .get("expiration")
            .and_then(|v| v.as_str())
            .map(String::from);
        let conditions = value
            .get("conditions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(PostPolicy {
            expiration,
            conditions,
        })
    }

    /// Policy expiry (localstack validate_post_policy order: right after
    /// decoding, before signature and conditions). A value in neither
    /// documented format (`2007-12-01T12:00:00Z` /
    /// `2007-12-01T12:00:00.000Z`) is an invalid policy date (AWS docs: 400
    /// InvalidRequest, "Invalid date"); a past expiry is 403 AccessDenied,
    /// "Invalid according to Policy: Policy expired."
    fn check_post_policy_expiration(policy: &PostPolicy) -> Result<(), AwsResponse> {
        let Some(expiry) = policy.expiration.as_deref() else {
            return Ok(());
        };
        let parsed = parse_policy_expiration(expiry);
        let Some(exp) = parsed else {
            return Err(AwsResponse::error(400, "InvalidRequest", "Invalid date"));
        };
        if exp < chrono::Utc::now() {
            return Err(Self::aws_error_with(
                403,
                "AccessDenied",
                "Invalid according to Policy: Policy expired.",
                &[],
            ));
        }
        Ok(())
    }

    /// The signature gate (localstack field-set detection + real AWS's
    /// SigV4 chain). V2-signed forms verify as v4-less here (no SHA-1 in
    /// this crate; see residual gaps) — they still pass through decode and
    /// expiry above.
    fn verify_post_signature(policy_b64: &str, form: &PostForm) -> Result<(), AwsResponse> {
        match post_signature_version(form) {
            PostSigVersion::V4 { signature } => {
                // The V4 field set is only valid for its single documented
                // algorithm: anything else (or an empty value) is rejected
                // the way real AWS rejects a POST declaring an
                // unsupported x-amz-algorithm — 400 InvalidArgument naming
                // the field.
                let algorithm = form.get("x-amz-algorithm").unwrap_or_default();
                if algorithm != "AWS4-HMAC-SHA256" {
                    return Err(Self::aws_error_with(
                        400,
                        "InvalidArgument",
                        "Unsupported Signing Algorithm",
                        &[
                            ("ArgumentName", "x-amz-algorithm"),
                            ("ArgumentValue", algorithm),
                        ],
                    ));
                }
                let credential = form.get("x-amz-credential").unwrap_or_default();
                let amz_date = form.get("x-amz-date").unwrap_or_default();
                let access_key = credential.split('/').next().unwrap_or_default();
                let expected = expected_post_signature(
                    POST_UPLOAD_SIGNING_SECRET,
                    amz_date,
                    credential,
                    policy_b64,
                );
                let matches = expected
                    .as_deref()
                    .is_some_and(|exp| exp.eq_ignore_ascii_case(&signature));
                if !matches {
                    return Err(Self::signature_does_not_match_error(
                        &signature, policy_b64, access_key,
                    ));
                }
                Ok(())
            }
            PostSigVersion::Incomplete(field) => {
                let name = post_argument_display_name(field);
                Err(Self::aws_error_with(
                    400,
                    "InvalidArgument",
                    &format!("Bucket POST must contain a field named '{name}'.  If it is specified, please check the order of the fields.",),
                    &[("ArgumentName", name), ("ArgumentValue", "")],
                ))
            }
            // A form with a policy document but no signature field family is
            // not a signed upload: AccessDenied, as localstack raises.
            PostSigVersion::Anonymous => Err(Self::aws_error_with(
                403,
                "AccessDenied",
                "Access Denied",
                &[],
            )),
            PostSigVersion::V2 => Ok(()),
        }
    }

    /// Evaluate every declared policy condition against the form, in order
    /// (localstack `_verify_condition`). The first failing condition answers
    /// 403 AccessDenied "Invalid according to Policy: Policy Condition
    /// failed: [...]"; content-length-range raises the typed
    /// EntityTooSmall/EntityTooLarge shapes instead.
    fn check_post_conditions(
        policy: &PostPolicy,
        form: &PostForm,
        bucket_name: &str,
        content_length: usize,
    ) -> Result<(), AwsResponse> {
        for condition in &policy.conditions {
            if !Self::evaluate_post_condition(condition, form, bucket_name, content_length)? {
                return Err(Self::aws_error_with(
                    403,
                    "AccessDenied",
                    &format!(
                        "Invalid according to Policy: Policy Condition failed: {}",
                        condition
                    ),
                    &[],
                ));
            }
        }
        Ok(())
    }

    /// One condition's truth against the parsed form. `Err` is for the typed
    /// shapes localstack raises mid-walk (EntityTooSmall, EntityTooLarge,
    /// InvalidPolicyDocument); `Ok(false)` is a plain mismatch.
    fn evaluate_post_condition(
        condition: &serde_json::Value,
        form: &PostForm,
        bucket_name: &str,
        content_length: usize,
    ) -> Result<bool, AwsResponse> {
        if let Some(dict) = condition.as_object() {
            // Real AWS/simple-conditions: exactly one property per dict.
            if dict.len() != 1 {
                return Err(Self::aws_error_with(
                    400,
                    "InvalidPolicyDocument",
                    "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified.",
                    &[],
                ));
            }
            let (key, value) = dict.iter().next().expect("checked len above");
            let matched = key.eq_ignore_ascii_case("bucket") && value.as_str() == Some(bucket_name)
                || !key.eq_ignore_ascii_case("bucket")
                    && form
                        .get(key)
                        .is_some_and(|v| value.as_str().is_some_and(|s| v == s));
            return Ok(matched);
        }
        let Some(parts) = condition.as_array() else {
            // Unsupported condition shape: treated as failed (localstack's
            // _verify_condition match falls off for unknown shapes).
            return Ok(false);
        };
        // ["content-length-range", min, max]: bounds arrive as JSON numbers
        // or numeric strings, coerced the way localstack's int() reads them.
        if parts.len() == 3
            && parts.first().and_then(|v| v.as_str()) == Some("content-length-range")
        {
            let (Some(min), Some(max)) = (
                as_policy_i64(parts.get(1).unwrap_or(&serde_json::Value::Null)),
                as_policy_i64(parts.get(2).unwrap_or(&serde_json::Value::Null)),
            ) else {
                // Non-numeric bounds: localstack's try/int ValueError branch
                // marks the condition failed.
                return Ok(false);
            };
            let size = content_length as i64;
            if size < min {
                return Err(Self::aws_error_with(
                    400,
                    "EntityTooSmall",
                    "Your proposed upload is smaller than the minimum allowed size",
                    &[
                        ("ProposedSize", &size.to_string()),
                        ("MinSizeAllowed", &min.to_string()),
                    ],
                ));
            }
            if size > max {
                return Err(Self::aws_error_with(
                    400,
                    "EntityTooLarge",
                    "Your proposed upload exceeds the maximum allowed size",
                    &[
                        ("ProposedSize", &size.to_string()),
                        ("MaxSizeAllowed", &max.to_string()),
                    ],
                ));
            }
            return Ok(true);
        }
        // eq / starts-with conditions: operator, `$field`, string value.
        let Some(op) = parts.first().and_then(|v| v.as_str()) else {
            return Ok(false);
        };
        let Some(field) = parts.get(1).and_then(|v| v.as_str()) else {
            return Ok(false);
        };
        let Some(bound) = parts.get(2).and_then(|v| v.as_str()) else {
            return Ok(false);
        };
        match (op, field, bound) {
            ("eq", k, bound) => {
                if k.eq_ignore_ascii_case("$bucket") {
                    return Ok(bound == bucket_name);
                }
                Ok(k.starts_with('$')
                    && form
                        .get(k.trim_start_matches('$'))
                        .is_some_and(|v| v == bound))
            }
            ("starts-with", k, bound) => {
                if !k.starts_with('$') {
                    return Ok(false);
                }
                // An empty starts-with value accepts anything the field
                // holds — but the field itself must exist: an absent field
                // is a policy condition failure (AWS/moto 403 AccessDenied
                // with the Policy-Condition-failed message).
                let Some(value) = form.get(k.trim_start_matches('$')) else {
                    return Ok(false);
                };
                Ok(bound.is_empty() || value.starts_with(bound))
            }
            _ => Ok(false),
        }
    }

    /// AWS doc rule (sigv4-HTTPPOSTConstructPolicy, enforced the way minio's
    /// checkPostPolicy cites it): "Each form field that you specify in a
    /// form (except x-amz-signature, file, policy, and field names that have
    /// an x-ignore- prefix) must appear in the list of conditions." The
    /// signature mechanics of both versions are exempt too (the twin
    /// verifies those, real AWS conditions nothing on them). Violations
    /// answer 403 AccessDenied.
    fn check_post_fields_covered(policy: &PostPolicy, form: &PostForm) -> Result<(), AwsResponse> {
        let mut covered: std::collections::HashSet<String> = std::collections::HashSet::new();
        for condition in &policy.conditions {
            if let Some(dict) = condition.as_object() {
                for key in dict.keys() {
                    let name = key.to_ascii_lowercase();
                    if name != "bucket" {
                        covered.insert(name.trim_start_matches('$').to_string());
                    }
                }
            }
            if let Some(parts) = condition.as_array() {
                if parts.len() == 3 {
                    if let Some(key) = parts.get(1).and_then(|v| v.as_str()) {
                        if !key.eq_ignore_ascii_case("$bucket") {
                            covered.insert(
                                key.to_ascii_lowercase().trim_start_matches('$').to_string(),
                            );
                        }
                    }
                }
                // content-length-range covers nothing: it constrains size.
            }
        }
        let exempt = |name: &str| -> bool {
            matches!(name, "file" | "policy")
                || POST_V4_FIELDS.contains(&name)
                || POST_V2_FIELDS.contains(&name)
                || name.starts_with("x-ignore-")
        };
        for (name, _) in &form.fields {
            let lowered = name.to_ascii_lowercase();
            if !exempt(&lowered) && !covered.contains(&lowered) {
                return Err(Self::aws_error_with(
                    403,
                    "AccessDenied",
                    &format!(
                        "Each form field that you specify in a form must appear in the list of policy conditions. \"{name}\" not specified in the policy."
                    ),
                    &[],
                ));
            }
        }
        Ok(())
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
                            return AwsResponse::error(
                                404,
                                "NoSuchKey",
                                "The specified key does not exist",
                            )
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

        Self::fire_event(
            req,
            dest,
            dest_key,
            "s3:ObjectCreated:Copy",
            source.size,
            &etag,
        );

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

    /// GET/HEAD on a delete-marker version id: 405 MethodNotAllowed (the AWS
    /// S3 treatment moto's `MethodNotAllowed(method, resource_type="DeleteMarker")`
    /// renders), carrying the delete-marker headers, `Allow: DELETE` and the
    /// marker's `Last-Modified` (moto PR #10428; head_object precedent).
    fn delete_marker_version_error(
        method: &str,
        version_id: &str,
        last_modified: u64,
    ) -> AwsResponse {
        let mut resp = AwsResponse::error(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        );
        resp.headers
            .push(("x-amz-delete-marker".to_string(), "true".to_string()));
        resp.headers
            .push(("x-amz-version-id".to_string(), version_id.to_string()));
        resp.headers
            .push(("Allow".to_string(), "DELETE".to_string()));
        resp.headers
            .push(("Last-Modified".to_string(), httpdate(last_modified)));
        // moto's method_resource_error extension adds the requested method
        // and resource type to the error document.
        resp.body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>MethodNotAllowed</Code>
  <Message>The specified method is not allowed against this resource.</Message>
  <Method>{method}</Method>
  <ResourceType>DeleteMarker</ResourceType>
</Error>"#
        )
        .into_bytes();
        resp
    }

    /// The `response-*` query-parameter overrides a successful GET/HEAD
    /// object answer carries (AWS "Get Object: response header overrides";
    /// moto `ALLOWED_HEADER_OVERRIDES`, s3/responses.py:180-187). Each of
    /// the six usable response headers is overridden ONLY by its own query
    /// parameter, when that parameter is present in the request — this is
    /// how a browser pulls an object with a different Content-Disposition
    /// etc. without a copy (moto applies the same map for HEAD requests
    /// whose presigned URL carries the params).
    fn apply_response_header_overrides(req: &AwsRequest, headers: &mut Vec<(String, String)>) {
        const OVERRIDES: [(&str, &str); 6] = [
            ("response-content-type", "Content-Type"),
            ("response-content-language", "Content-Language"),
            ("response-expires", "Expires"),
            ("response-cache-control", "Cache-Control"),
            ("response-content-disposition", "Content-Disposition"),
            ("response-content-encoding", "Content-Encoding"),
        ];
        for (param, header) in OVERRIDES {
            if let Some(value) = req.query_params.get(param) {
                headers.retain(|(k, _)| !k.eq_ignore_ascii_case(header));
                headers.push((header.to_string(), value.clone()));
            }
        }
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
                Some(vid) => {
                    if let Some(v) = entry.versions.iter().find(|v| v.version_id == *vid) {
                        v.clone()
                    } else {
                        // The requested VersionId is a delete marker: AWS
                        // answers 405 MethodNotAllowed with Allow: DELETE —
                        // the same error moto's MethodNotAllowed(method="GET",
                        // resource_type="DeleteMarker") renders (see also
                        // put_object_tagging's marker handling). Previously
                        // this fell through to NoSuchKey.
                        if let Some(m) = entry.markers.iter().find(|m| m.version_id == *vid) {
                            return Self::delete_marker_version_error(
                                &req.method,
                                &m.version_id,
                                m.last_modified,
                            );
                        }
                        return AwsResponse::error(
                            404,
                            "NoSuchKey",
                            "The specified key does not exist",
                        );
                    }
                }
                None => {
                    if !entry.is_live() {
                        // Latest record is a delete marker: 404 NoSuchKey,
                        // but Amazon S3 behaves as if the object was deleted
                        // and includes x-amz-delete-marker: true plus the
                        // marker's x-amz-version-id in the response
                        // (mirror of HeadObject's 404 shape).
                        let vid = entry
                            .markers
                            .first()
                            .map(|m| m.version_id.clone())
                            .unwrap_or_default();
                        let mut resp = AwsResponse::error(
                            404,
                            "NoSuchKey",
                            "The specified key does not exist",
                        );
                        resp.headers
                            .push(("x-amz-delete-marker".to_string(), "true".to_string()));
                        resp.headers.push(("x-amz-version-id".to_string(), vid));
                        return resp;
                    }
                    entry
                        .versions
                        .first()
                        .cloned()
                        .expect("is_live() guarantees a live version row")
                }
            };
            found
        };

        // Range handling
        if let Some(range) = range_header
            .as_deref()
            .and_then(|r| parse_range(r, obj.size))
        {
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
            Self::apply_response_header_overrides(req, &mut resp.headers);
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
        Self::apply_response_header_overrides(req, &mut resp.headers);
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
            match req.query_params.get("versionId") {
                Some(vid) => {
                    if let Some(v) = entry.versions.iter().find(|v| v.version_id == *vid) {
                        v.clone()
                    } else {
                        if let Some(m) = entry.markers.iter().find(|m| m.version_id == *vid) {
                            // HEAD on a delete-marker version: 405 with the
                            // delete-marker headers and Allow: DELETE (moto
                            // HeadOnDeleteMarker path).
                            return Self::delete_marker_version_error(
                                &req.method,
                                &m.version_id,
                                m.last_modified,
                            );
                        }
                        return AwsResponse::error(404, "NotFound", "Not Found");
                    }
                }
                None => {
                    if !entry.is_live() {
                        // Latest record is a delete marker: 404, as if the
                        // object was deleted, with the delete-marker headers
                        // real AWS and moto send.
                        let vid = entry
                            .markers
                            .first()
                            .map(|m| m.version_id.clone())
                            .unwrap_or_default();
                        let mut resp = AwsResponse::error(404, "NotFound", "Not Found");
                        resp.headers
                            .push(("x-amz-delete-marker".to_string(), "true".to_string()));
                        resp.headers.push(("x-amz-version-id".to_string(), vid));
                        return resp;
                    }
                    entry
                        .versions
                        .first()
                        .cloned()
                        .expect("is_live() guarantees a live version row")
                }
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
        let mut resp = AwsResponse {
            status: 200,
            headers,
            body: vec![],
        };
        Self::apply_response_header_overrides(req, &mut resp.headers);
        resp
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
        // Listing a missing bucket is NoSuchBucket (moto raises MissingBucket);
        // the listing engine otherwise returns an empty success.
        if let Err(resp) = Self::bucket_or_404(&state, &req.bucket.clone().unwrap_or_default()) {
            return resp;
        }
        let (contents, prefixes, last_emitted) = self.collect_listing(&state, req);
        let bucket_name = req.bucket.clone().unwrap_or_default();
        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let marker = req.query_params.get("marker").cloned().unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let delimiter = req
            .query_params
            .get("delimiter")
            .cloned()
            .unwrap_or_default();
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
        // Listing a missing bucket is NoSuchBucket (moto raises MissingBucket).
        if let Err(resp) = Self::bucket_or_404(&state, &req.bucket.clone().unwrap_or_default()) {
            return resp;
        }
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
                .map(|(k, e)| {
                    (
                        k.clone(),
                        ObjectEntry {
                            versions: e.versions.clone(),
                            markers: e.markers.clone(),
                        },
                    )
                })
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
        resp.headers
            .push(("ETag".to_string(), format!("\"{}\"", etag)));
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
                            return AwsResponse::error(
                                404,
                                "NoSuchKey",
                                "The specified key does not exist",
                            )
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
                Some(vid) => {
                    entry.and_then(|e| e.versions.iter().find(|v| v.version_id == *vid).cloned())
                }
                None => entry
                    .filter(|e| e.is_live())
                    .and_then(|e| e.versions.first().cloned()),
            }
        };
        let obj = match obj {
            Some(o) => o,
            None => {
                return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
            }
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
        let mut resp = AwsResponse::xml(200, body);
        // moto master 086af7ac (#10415): LastModified is rendered in the
        // Last-Modified response header (the shape binds it to that header)
        // in RFC 822 format — moto 5.2.4 put an ISO-8601 string into the
        // header, which other SDKs reject.
        resp.headers
            .push(("Last-Modified".to_string(), httpdate(obj.last_modified)));
        resp
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
        *bucket.request_payment_xml.write() = Some(String::from_utf8_lossy(&req.body).to_string());
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
            .filter(|r| {
                r.prefix
                    .as_ref()
                    .map(|p| key.starts_with(p))
                    .unwrap_or(true)
            })
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
            return AwsResponse::error(
                400,
                "InvalidArgument",
                "The Id query parameter is required",
            );
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
            None => AwsResponse::error(
                404,
                "NoSuchConfiguration",
                "The specified configuration does not exist",
            ),
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
            None => AwsResponse::error(
                404,
                "NoSuchConfiguration",
                "The specified configuration does not exist",
            ),
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
            xml.push_str(&format!(
                "<InventoryConfiguration>{}</InventoryConfiguration>",
                entry
            ));
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
        // moto s3/models.py:1974 create_session(): a bucket that exists and
        // is not directory-typed is InvalidRequest ("CreateSession is only
        // supported for directory buckets."). A missing bucket still answers:
        // botocore's S3 Express credential refresh runs CreateSession BEFORE
        // its own CreateBucket request lands (before-sign auth resolution of
        // the v4-s3express scheme), so permissing here is what the client
        // flow requires (matches the Python twin's native handling).
        if Self::bucket_param(req).is_none() {
            // moto answers GET / without a bucket from the bucket-level
            // dispatch (all_buckets / ListDirectoryBuckets), not from
            // create_session — mirror that via the list handler.
            return self.list_buckets(req);
        }
        let (state, _bucket_name) = (self.get_state(req.account, &req.region), ());
        if let Some(bucket) =
            state.get_bucket(Self::bucket_param(req).as_deref().unwrap_or_default())
        {
            if !bucket.directory {
                return AwsResponse::error(
                    400,
                    "InvalidRequest",
                    "CreateSession is only supported for directory buckets.",
                );
            }
        }
        // moto: seed = "{bucket_name}-{account_id}-{utc-now isoformat}" and
        // key_hash = sha256(seed)[:40]; AccessKeyId is "ASIA" + the first 16
        // characters upper-cased, the session token carries a literal
        // "FwoGZXIvYXdzE" prefix, expiration is now + 12 hours.
        let now = chrono::Utc::now();
        let bucket_name = Self::bucket_param(req).unwrap_or_default();
        let seed = format!(
            "{}-{}-{}",
            bucket_name,
            req.account,
            now.format("%Y-%m-%dT%H:%M:%S%.6f+00:00"),
        );
        let key_hash: String = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(seed.as_bytes());
            hex::encode(digest)
        }
        .chars()
        .take(40)
        .collect();
        let expiration = (now + chrono::Duration::hours(12))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CreateSessionResult>\n  <Credentials>\n    <SessionToken>FwoGZXIvYXdzE{key_hash}</SessionToken>\n    <SecretAccessKey>{key_hash}</SecretAccessKey>\n    <AccessKeyId>ASIA{access}</AccessKeyId>\n    <Expiration>{expiration}</Expiration>\n  </Credentials>\n</CreateSessionResult>",
            key_hash = key_hash,
            access = key_hash[..16].to_ascii_uppercase(),
            expiration = expiration,
        );
        // moto S3_CREATE_SESSION_RESPONSE template (responses.py:3769).
        AwsResponse::xml(200, body)
    }

    /// The request's bucket name, present and non-empty.
    fn bucket_param(req: &AwsRequest) -> Option<String> {
        req.bucket.clone().filter(|b| !b.is_empty())
    }

    fn rename_object(&self, req: &AwsRequest) -> AwsResponse {
        let (_state, bucket) = match self.with_bucket(req) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if !bucket.directory {
            return AwsResponse::error(
                400,
                "InvalidRequest",
                "RenameObject is only supported for directory buckets.",
            );
        }
        let (_bucket_name, dest_key) = Self::bucket_and_key(req);
        // moto s3/models.py:2004 rename_object(): the source strip takes the
        // "<bucket>/" prefix off the front of x-amz-rename-source (first
        // slash onward), without URL decoding; a missing header yields an
        // empty source which is simply NoSuchKey.
        let rename_source = req
            .headers
            .get("x-amz-rename-source")
            .cloned()
            .unwrap_or_default();
        let src_key = match rename_source.split_once('/') {
            Some((_src_bucket, key)) => key.to_string(),
            None => rename_source.clone(),
        };
        // moto: bucket.keys[new_key] = obj ... del bucket.keys[source_key] —
        // the insert happens before the delete, so overwriting an existing
        // destination works and a source==destination rename drops the entry
        // (matches moto's dict semantics).
        let mut objects = bucket.objects.write();
        let entry = match objects.get(&src_key).cloned() {
            Some(entry) => entry,
            None => {
                return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist.")
            }
        };
        objects.insert(dest_key.clone(), entry);
        objects.remove(&src_key);
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
        let config = bucket
            .notification_config
            .read()
            .clone()
            .unwrap_or_default();
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
                sqs::bridge::deliver_to_arn(&target.arn, &message);
            }
        }
        // Topic targets publish through the SNS provider's registered hook
        // (its own queue/lambda subscribers receive the notification); Lambda
        // targets invoke through the gateway-registered lambda hook.
        for target in config.topics.iter() {
            if event_matches(event_name, target, key) && target.arn.contains(":sns:") {
                // Fire-and-forget, like the Python engine's S3 topic delivery.
                let _ = events::hook("sns", &target.arn, &message, req.account, &req.region);
            }
        }
        for target in config.lambdas.iter() {
            if event_matches(event_name, target, key) && target.arn.contains(":lambda:") {
                let _ = events::hook("lambda", &target.arn, &message, req.account, &req.region);
            }
        }

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
        // boto3 emits LambdaFunctionConfiguration for the modern shape and
        // CloudFunctionConfiguration (legacy <CloudFunction> element) for
        // the v1 shape — both carry the same ARN/events/filter members.
        for section in xml_sections(body, "LambdaFunctionConfiguration")
            .into_iter()
            .chain(xml_sections(body, "CloudFunctionConfiguration"))
        {
            let t = parse_target_section(&section, "LambdaFunctionArn")
                .or_else(|| parse_target_section(&section, "CloudFunction"));
            if let Some(t) = t {
                config.lambdas.push(t);
            }
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
                .or_else(|| xml_text(&section, "Filter").and_then(|f| xml_text(&f, "Prefix")));
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
    // SelectObjectContent (AWS event-stream wire format)
    // ------------------------------------------------------------------

    fn select_object_content(&self, req: &AwsRequest) -> AwsResponse {
        let (bucket_name, key) = Self::bucket_and_key(req);
        let state = self.get_state(req.account, &req.region);
        let bucket = match Self::bucket_or_404(&state, &bucket_name) {
            Ok(b) => b,
            Err(e) => return e,
        };

        let body = String::from_utf8_lossy(&req.body).to_string();
        let expression = xml_text(&body, "Expression").unwrap_or_default();
        let expression_type = xml_text(&body, "ExpressionType").unwrap_or_default();
        if expression_type != "SQL" {
            return AwsResponse::error(400, "InvalidRequest", "Only SQL expressions are supported");
        }

        let file_header_info =
            xml_text(&body, "FileHeaderInfo").unwrap_or_else(|| "NONE".to_string());

        let records_payload = {
            let objects = bucket.objects.read();
            let obj = match objects.get(&key).and_then(|e| {
                if e.is_live() {
                    e.versions.first().cloned()
                } else {
                    None
                }
            }) {
                Some(v) => v,
                None => {
                    return AwsResponse::error(404, "NoSuchKey", "The specified key does not exist")
                }
            };
            match Self::run_sql_select(
                &String::from_utf8_lossy(&obj.data),
                &expression,
                &file_header_info,
            ) {
                Some(out) => out.into_bytes(),
                None => {
                    return AwsResponse::error(
                        400,
                        "UnsupportedSQLOperation",
                        "The provided SQL expression is not supported",
                    )
                }
            }
        };

        // Frame the response as an AWS event stream: a Records event followed
        // by an End event.
        let mut frames = event_stream_frame(
            "Records",
            Some("application/octet-stream"),
            &records_payload,
        );
        frames.extend_from_slice(&event_stream_frame("End", None, b""));

        AwsResponse::binary(200, frames, "application/vnd.amazon.eventstream")
    }

    /// Minimal CSV SELECT: `SELECT *|col[,col]* FROM s3object [anything]`.
    fn run_sql_select(data: &str, expression: &str, file_header_info: &str) -> Option<String> {
        let normalized = expression.split_whitespace().collect::<Vec<_>>().join(" ");
        let rest = normalized
            .strip_prefix("SELECT ")
            .or_else(|| normalized.strip_prefix("select "))?;
        let from_pos = rest.find(" FROM ")?;
        let columns_part = rest[..from_pos].trim();
        if !rest[from_pos + 6..].starts_with("s3object") {
            return None;
        }

        let lines: Vec<&str> = data.lines().collect();
        if lines.is_empty() {
            return Some(String::new());
        }

        let mut all: Vec<Vec<String>> = lines
            .iter()
            .map(|l| l.split(',').map(|c| c.trim().to_string()).collect())
            .collect();

        let header: Vec<String> = if file_header_info.eq_ignore_ascii_case("USE") {
            all.remove(0)
        } else {
            all.first()
                .map(|c| (0..c.len()).map(|i| format!("_{}", i + 1)).collect())
                .unwrap_or_default()
        };

        let select_all = columns_part == "*";
        let wanted: Vec<String> = if select_all {
            header.clone()
        } else {
            columns_part
                .split(',')
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect()
        };

        // Header names only exist with FileHeaderInfo=USE; otherwise fall
        // back to selecting all columns.
        let mut selected: Vec<Option<usize>> = Vec::new();
        for want in &wanted {
            let idx = header.iter().position(|h| h.eq_ignore_ascii_case(want));
            selected.push(idx);
        }
        if selected.iter().any(|v| v.is_none()) && !select_all {
            return None;
        }

        let mut out = String::new();
        if !select_all && file_header_info.eq_ignore_ascii_case("USE") {
            out.push_str(&wanted.join(","));
            out.push('\n');
        }
        for row in all {
            if select_all {
                out.push_str(&row.join(","));
                out.push('\n');
            } else {
                let cells: Vec<String> = selected
                    .iter()
                    .map(|idx| {
                        idx.map_or(String::new(), |i| row.get(i).cloned().unwrap_or_default())
                    })
                    .collect();
                out.push_str(&cells.join(","));
                out.push('\n');
            }
        }
        Some(out)
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
            // Virtual-hosted-style Session requests carry the bucket in the
            // Host header (botocore S3 Express endpoint rules), so don't
            // assume ListBuckets when the session queries are present —
            // normalize_virtual_host fills the bucket in afterwards, and
            // create_session falls back to ListBuckets when it stays empty.
            if method == "GET" {
                if query_params.contains_key("session") {
                    return Some("CreateSession".to_string());
                }
                return Some("ListBuckets".to_string());
            }
            if method == "POST" && query_params.contains_key("createSession") {
                return Some("CreateSession".to_string());
            }
            return None;
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
                // POST /{Bucket}/{Key+} with no query suffix is the browser
                // form upload (real AWS routes POST object uploads on both
                // bucket-only and bucket/object URLs; the SDK-side POST with
                // a key, CompleteMultipartUpload, is matched above by
                // ?uploadId). The form's `key` field names the object.
                "POST" => Some("PostObject".to_string()),
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
                } else if q("createSession") {
                    // Real S3 accepts POST /{Bucket}?createSession for S3
                    // Express (botocore's older model shadows it as
                    // GET ?session; answer both).
                    Some("CreateSession".to_string())
                } else if q("notification") {
                    Some("PutBucketNotification".to_string())
                } else if q("uploads") {
                    Some("CreateMultipartUpload".to_string())
                } else if q("metadataConfiguration") {
                    Some("CreateBucketMetadataConfiguration".to_string())
                } else if q("metadataTable") {
                    Some("CreateBucketMetadataTableConfiguration".to_string())
                } else {
                    // POST /{Bucket} without a recognized query suffix is the
                    // HTML form upload (RESTObjectPOST). moto's
                    // _bucket_response_post treats it the same way: everything
                    // that is not ?delete is a pre-signed POST.
                    Some("PostObject".to_string())
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

        let mut req = make_req("PutObject", Some("test-bucket"), Some("meta.txt"), b"body");
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
        assert_eq!(
            headers.get("x-amz-meta-author").map(String::as_str),
            Some("X")
        );
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
        req.headers
            .insert("range".to_string(), "bytes=1-2".to_string());
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
        vreq.query_params
            .insert("versioning".to_string(), String::new());
        handler.handle(vreq);

        handler.handle(make_req("PutObject", Some("ver"), Some("k"), b"one"));
        handler.handle(make_req("PutObject", Some("ver"), Some("k"), b"two"));

        let mut listreq = make_req("ListObjectVersions", Some("ver"), None, b"");
        listreq
            .query_params
            .insert("prefix".to_string(), String::new());
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
        handler.handle(make_req(
            "PutObject",
            Some("test-bucket"),
            Some("file"),
            b"data",
        ));

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

    // -----------------------------------------------------------------------
    // S3 Express: directory buckets / CreateSession / RenameObject
    // (moto evidence: model.py:1098 is_directory_bucket, 1974 create_session,
    // 2004 rename_object, responses.py:583 GET ?session)
    // -----------------------------------------------------------------------

    /// CreateBucketConfiguration body the botocore fixtures send for a
    /// directory bucket.
    const DIR_BUCKET_CONFIG: &str = r#"<CreateBucketConfiguration><Location><Type>AvailabilityZone</Type><Name>use1-az1</Name></Location><Bucket><Type>Directory</Type><DataRedundancy>SingleAvailabilityZone</DataRedundancy></Bucket></CreateBucketConfiguration>"#;

    /// A request as the gateway delivers it: bucket/key split from the path,
    /// decoded query params, the operation pre-detected from method+path+query
    /// (S3ServiceHandler::handle_sync), and a raw Host header.
    fn wire_req(
        method: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        query: &[(&str, &str)],
        host: &str,
    ) -> AwsRequest {
        let mut query_params = HashMap::new();
        for (k, v) in query {
            query_params.insert(k.to_string(), v.to_string());
        }
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), host.to_string());
        // Path equivalent of the gateway's bucket/key split, used for
        // operation detection exactly like S3ServiceHandler.
        let path = format!(
            "/{}{}",
            bucket.unwrap_or_default(),
            key.map(|k| format!("/{}", k)).unwrap_or_default()
        );
        let operation =
            S3Handler::detect_s3_operation(method, &path, &query_params).unwrap_or_default();
        AwsRequest {
            service: "s3".to_string(),
            operation,
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: bucket.map(String::from).filter(|s| !s.is_empty()),
            key: key.map(String::from).filter(|s| !s.is_empty()),
            query_params,
            headers,
            method: method.to_string(),
            body: Bytes::new(),
            params: Value::Null,
        }
    }

    /// The moto CreateSession XML: every credential member present.
    fn assert_session_credentials(body: &str) {
        assert!(body.contains("<CreateSessionResult>"), "body: {body}");
        assert!(body.contains("<Credentials>"), "body: {body}");
        let token = xml_text(body, "SessionToken").expect("SessionToken");
        assert!(token.starts_with("FwoGZXIvYXdzE"), "token: {token}"); // models.py:1990
        assert_eq!(token.len(), 13 + 40); // "FwoGZXIvYXdzE" + 40-char hash
        let secret = xml_text(body, "SecretAccessKey").expect("SecretAccessKey");
        assert_eq!(secret.len(), 40); // sha256 hexdigest truncated to 40
        let access = xml_text(body, "AccessKeyId").expect("AccessKeyId");
        assert!(access.starts_with("ASIA"), "access: {access}"); // models.py:1987
        assert_eq!(access.len(), 4 + 16);
        assert_eq!(
            access[4..],
            secret[..16].to_ascii_uppercase(),
            "AccessKeyId derives from the SecretAccessKey hash"
        );
        let expiration = xml_text(body, "Expiration").expect("Expiration");
        assert!(
            chrono::NaiveDateTime::parse_from_str(&expiration, "%Y-%m-%dT%H:%M:%SZ").is_ok(),
            "expiration format: {expiration}"
        );
    }

    // ------------------------------------------------------------------
    // POST object — browser form upload
    // ------------------------------------------------------------------

    /// Compose a multipart/form-data body the way real form-post clients
    /// (browsers, requests-posting-boto formulalar) write it: named text
    /// fields first, then the `file` part, terminated with the standard
    /// boundary. Returns the full Content-Type header value and the body.
    fn multipart_form_body(
        fields: &[(&str, &str)],
        file_part: Option<(&str, &str, &[u8])>,
    ) -> (String, Vec<u8>) {
        const BOUNDARY: &str = "----robotocore-post-form";
        let mut body: Vec<u8> = Vec::new();
        for (name, value) in fields {
            body.extend_from_slice(
                format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n")
                    .as_bytes(),
            );
            body.extend_from_slice(value.as_bytes());
            body.extend_from_slice(b"\r\n");
        }
        if let Some((filename, content_type, payload)) = file_part {
            body.extend_from_slice(
                format!(
                    "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(payload);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
        (format!("multipart/form-data; boundary={BOUNDARY}"), body)
    }

    /// A POST upload wire request the way the gateway delivers it: method
    /// POST, multipart content-type, operation detected from the method+path
    /// (the same detect the gateway runs).
    fn form_post_request(
        bucket: &str,
        key_in_url: Option<&str>,
        fields: &[(&str, &str)],
        file_part: Option<(&str, &str, &[u8])>,
    ) -> AwsRequest {
        let (content_type, body) = multipart_form_body(fields, file_part);
        let path = format!(
            "/{}{}",
            bucket,
            key_in_url.map(|k| format!("/{k}")).unwrap_or_default(),
        );
        let operation =
            S3Handler::detect_s3_operation("POST", &path, &HashMap::new()).unwrap_or_default();
        AwsRequest {
            service: "s3".to_string(),
            operation,
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some(bucket.to_string()),
            key: key_in_url.map(String::from),
            query_params: HashMap::new(),
            headers: form_post_headers(&content_type, body.len()),
            method: "POST".to_string(),
            body: Bytes::copy_from_slice(&body),
            params: Value::Null,
        }
    }

    /// The header set a real multipart POST carries: Host plus the body
    /// length (browsers/hyper always send Content-Length; the handler's 411
    /// gate keys off it).
    fn form_post_headers(content_type: &str, body_len: usize) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "localhost:4566".to_string());
        headers.insert("content-type".to_string(), content_type.to_string());
        headers.insert("content-length".to_string(), body_len.to_string());
        headers
    }

    /// A POST upload with owned (policy/signature) form fields, no URL key.
    fn form_post_request_owned(
        bucket: &str,
        fields: &[(String, String)],
        file_part: Option<(&str, &str, &[u8])>,
    ) -> AwsRequest {
        let owned: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (content_type, body) = multipart_form_body(&owned, file_part);
        let operation =
            S3Handler::detect_s3_operation("POST", &format!("/{bucket}"), &HashMap::new())
                .unwrap_or_default();
        AwsRequest {
            service: "s3".to_string(),
            operation,
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some(bucket.to_string()),
            key: None,
            query_params: HashMap::new(),
            headers: form_post_headers(&content_type, body.len()),
            method: "POST".to_string(),
            body: Bytes::copy_from_slice(&body),
            params: Value::Null,
        }
    }

    #[test]
    fn post_object_success_stores_object() {
        // Policy-less form on a writable bucket (moto/localstack both store
        // and answer 204 + Location + ETag).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let file_payload: &[u8] = b"form file contents";
        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[
                ("key", "uploads/a & b.txt"),
                ("x-amz-meta-owner", "me"),
                ("Content-Type", "text/x-form-upload"),
            ],
            Some(("note.txt", "application/octet-stream", file_payload)),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let headers = header_map(&resp);
        assert_eq!(headers.get("content-type").map(String::as_str), None); // 204: no content type
        let etag = headers.get("ETag").expect("ETag header").clone();
        assert!(etag.starts_with('"') && etag.ends_with('"'), "etag: {etag}");
        let location = headers.get("Location").expect("Location header").clone();
        assert_eq!(
            location,
            "http://localhost:4566/post-bkt/uploads/a%20%26%20b.txt"
        );

        // Stored through the decoded-key funnel; fetchable and typed.
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("uploads/a & b.txt"),
            b"",
        ));
        assert_eq!(get.status, 200);
        assert_eq!(get.body, file_payload);
        let got_headers = header_map(&get);
        assert_eq!(
            got_headers.get("Content-Type").map(String::as_str),
            Some("text/x-form-upload")
        );
        assert_eq!(
            got_headers.get("x-amz-meta-owner").map(String::as_str),
            Some("me")
        );
        assert_eq!(
            got_headers.get("ETag").map(String::as_str),
            Some(etag.as_str())
        );

        // moto keys a POST upload from the form's `key` field verbatim
        // (moto/s3/responses.py `_bucket_response_post`: `key =
        // self.querystring["key"][0]`; `moto/core/responses.py` feeds the
        // multipart form into that querystring without URL-decoding) — a
        // form value is payload, not a URI path segment. A literal `a%3Db`
        // uploaded via the form therefore stores as `a%3Db`, reachable only
        // through the once-decoded wire spelling `a%253Db`.
        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[("key", "a%3Db")],
            Some(("pct.txt", "application/octet-stream", b"verbatim")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let location = header_map(&resp).get("Location").expect("Location").clone();
        assert_eq!(location, "http://localhost:4566/post-bkt/a%253Db");
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("a%253Db"), // the literal `a%3Db` form key, wire spelling
            b"",
        ));
        assert_eq!(get.status, 200, "form key must be stored verbatim");
        assert_eq!(get.body, b"verbatim");
        // The path-decoded spelling is a different key: the form key was
        // never run through decoded_key.
        let decoded = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("a%3Db"), // decodes once to `a=b`, which was never uploaded
            b"",
        ));
        assert_eq!(decoded.status, 404, "form key must not be path-decoded");
    }

    #[test]
    fn post_object_filename_substitution() {
        // moto: ${filename} in the key field is replaced by the uploaded
        // file's name when a file part exists.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[("key", "docs/${filename}")],
            Some(("report.pdf", "application/pdf", b"%PDF-1.4 body")),
        ));
        assert_eq!(resp.status, 204);
        let location = header_map(&resp).get("Location").expect("Location").clone();
        assert!(
            location.ends_with("/docs/report.pdf"),
            "location: {location}"
        );

        let head = handler.handle(make_req(
            "HeadObject",
            Some("post-bkt"),
            Some("docs/report.pdf"),
            b"",
        ));
        assert_eq!(head.status, 200);
    }

    #[test]
    fn post_object_requires_multipart_content_type() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let mut req = form_post_request("post-bkt", None, &[("key", "k")], Some(("f", "", b"1")));
        req.headers
            .insert("content-type".to_string(), "text/plain".to_string());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 412);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("PreconditionFailed"), "body: {body}");
        assert!(
            body.contains("enclosure-type multipart/form-data"),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_requires_content_length() {
        // moto _bucket_response_post 411s a POST with no explicit length;
        // the message text is real AWS's MissingContentLength wording.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let mut req = form_post_request("post-bkt", None, &[("key", "k")], Some(("f", "", b"1")));
        req.headers.remove("content-length");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 411);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>MissingContentLength</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains("You must provide the content-length HTTP header."),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_acl_field_honored() {
        // The canned-ACL form field is honored like PUT's x-amz-acl header:
        // the AWS-documented name x-amz-acl (moto also reads the bare `acl`
        // name). A valid canned name stores the object; an unknown name is
        // a bad argument.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        for (field, value) in [
            ("x-amz-acl", "public-read"),
            ("acl", "bucket-owner-full-control"),
        ] {
            let resp = handler.handle(form_post_request(
                "post-bkt",
                None,
                &[("key", &format!("acl-{field}.txt")), (field, value)],
                Some(("f.txt", "application/octet-stream", b"body")),
            ));
            assert_eq!(
                resp.status,
                204,
                "body: {:?}",
                String::from_utf8_lossy(&resp.body)
            );
            let get = handler.handle(make_req(
                "GetObject",
                Some("post-bkt"),
                Some(&format!("acl-{field}.txt")),
                b"",
            ));
            assert_eq!(get.status, 200);
        }

        // Unknown canned value → 400 InvalidArgument naming the field.
        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[("key", "bad-acl.txt"), ("x-amz-acl", "everyone")],
            Some(("f.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(
            resp.status,
            400,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains("<ArgumentName>x-amz-acl</ArgumentName>"),
            "body: {body}"
        );
        assert!(
            body.contains("<ArgumentValue>everyone</ArgumentValue>"),
            "body: {body}"
        );
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("bad-acl.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "rejected uploads store nothing");
    }

    #[test]
    fn post_object_meta_fields_case_insensitive() {
        // Multipart field names carry the client's casing, unlike gateway
        // headers (lowercased top-side): the x-amz-meta- strip must be
        // case-insensitive so an `X-Amz-Meta-...` form field survives as
        // object user metadata just like its lowercase spelling.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[
                ("key", "cased-meta.txt"),
                ("X-Amz-Meta-Color", "red"),
                ("x-amz-meta-Mood", "happy"),
            ],
            Some(("f.txt", "application/octet-stream", b"body")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("cased-meta.txt"),
            b"",
        ));
        assert_eq!(get.status, 200);
        let headers = header_map(&get);
        assert_eq!(
            headers.get("x-amz-meta-color").map(String::as_str),
            Some("red"),
            "headers: {:?}",
            headers
        );
        assert_eq!(
            headers.get("x-amz-meta-mood").map(String::as_str),
            Some("happy")
        );
    }

    #[test]
    fn post_object_key_field_required() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        // No key field in the form and no key in the URL.
        let resp = handler.handle(form_post_request(
            "post-bkt",
            None,
            &[],
            Some(("f.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("InvalidArgument"), "body: {body}");
        assert!(
            body.contains("<ArgumentName>key</ArgumentName>"),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_url_key_used_when_form_omits_key() {
        // POST /{Bucket}/{Key}: the URL key names the object when the form
        // carries no key field (buckets publish the endpoint /bucket/key).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let resp = handler.handle(form_post_request(
            "post-bkt",
            Some("from-url.txt"),
            &[],
            Some(("whatever.txt", "application/octet-stream", b"url-key body")),
        ));
        assert_eq!(resp.status, 204);
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("from-url.txt"),
            b"",
        ));
        assert_eq!(get.status, 200);
        assert_eq!(get.body, b"url-key body");
    }

    #[test]
    fn post_object_truncated_form_rejected() {
        // A multipart body that ends before the closing `--boundary--`
        // terminal marker is truncated: parse_multipart_form must refuse it
        // (MalformedPOSTRequest) instead of accepting whatever parts fit —
        // the werkzeug/Flask form parsers the wire path mirrors only return
        // a form when the closing delimiter is present, and real AWS 400s
        // the same way. Nothing is stored.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let (content_type, full_body) = multipart_form_body(
            &[("key", "trunc.txt")],
            Some(("f.txt", "application/octet-stream", b"payload")),
        );
        // Drop the final `--\r\n` so the body ends at the bare `--boundary`
        // delimiter with no terminal `--boundary--` (and no trailing CRLF).
        let truncated = full_body[..full_body.len() - 4].to_vec();

        let path = "/post-bkt";
        let operation =
            S3Handler::detect_s3_operation("POST", path, &HashMap::new()).unwrap_or_default();
        let req = AwsRequest {
            service: "s3".to_string(),
            operation,
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some("post-bkt".to_string()),
            key: None,
            query_params: HashMap::new(),
            headers: form_post_headers(&content_type, truncated.len()),
            method: "POST".to_string(),
            body: Bytes::copy_from_slice(&truncated),
            params: Value::Null,
        };
        let resp = handler.handle(req);
        assert_eq!(
            resp.status,
            400,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>MalformedPOSTRequest</Code>"),
            "body: {body}"
        );

        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("trunc.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "truncated uploads store nothing");
    }

    /// A POST wire request with a caller-supplied Host header, delivered
    /// pre-normalization the way `normalize_virtual_host` expects a
    /// virtual-hosted caller to arrive (bucket = the first path segment).
    fn form_post_request_with_host(
        host: &str,
        proto: Option<&str>,
        path: &str,
        fields: &[(&str, &str)],
        file_part: Option<(&str, &str, &[u8])>,
    ) -> AwsRequest {
        let (content_type, body) = multipart_form_body(fields, file_part);
        let operation =
            S3Handler::detect_s3_operation("POST", path, &HashMap::new()).unwrap_or_default();
        // The gateway parses /{bucket}/{key}: the first path segment is the
        // arriving bucket.
        let mut bucket_key = path.trim_start_matches('/').splitn(2, '/');
        let parsed_bucket = bucket_key.next().unwrap_or_default();
        let parsed_key = bucket_key.next().unwrap_or_default();
        let mut headers = form_post_headers(&content_type, body.len());
        headers.insert("host".to_string(), host.to_string());
        if let Some(scheme) = proto {
            headers.insert("x-forwarded-proto".to_string(), scheme.to_string());
        }
        AwsRequest {
            service: "s3".to_string(),
            operation,
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some(parsed_bucket.to_string()).filter(|s| !s.is_empty()),
            key: Some(parsed_key.to_string()).filter(|s| !s.is_empty()),
            query_params: HashMap::new(),
            headers,
            method: "POST".to_string(),
            body: Bytes::copy_from_slice(&body),
            params: Value::Null,
        }
    }

    #[test]
    fn post_object_vhost_location_omits_bucket_path() {
        // A virtual-hosted POST (Host carries the bucket) keeps the vhost
        // shape in the success Location: `http://host/key`. The request is
        // path-normalized before the builder runs, so the vhost shape is
        // re-detected from the original Host — the old builder answered
        // `http://host/bucket/key`, which names the bucket twice and
        // 404s on refetch.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("vhost-bkt"), None, b""));

        // On the wire: POST http://vhost-bkt.localhost:4566/v.txt — the
        // gateway parses /v.txt into bucket=v.txt before normalization.
        let resp = handler.handle(form_post_request_with_host(
            "vhost-bkt.localhost:4566",
            None,
            "/v.txt",
            &[("key", "v.txt")],
            Some(("v.txt", "application/octet-stream", b"payload")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("Location").map(String::as_str),
            Some("http://vhost-bkt.localhost:4566/v.txt")
        );
        let get = handler.handle(make_req("GetObject", Some("vhost-bkt"), Some("v.txt"), b""));
        assert_eq!(get.status, 200);
        assert_eq!(get.body, b"payload");

        // X-Forwarded-Proto wins over http (first hop before a comma).
        let resp = handler.handle(form_post_request_with_host(
            "vhost-bkt.localhost:4566",
            Some("https"),
            "/v2.txt",
            &[("key", "v2.txt")],
            Some(("v.txt", "application/octet-stream", b"payload")),
        ));
        assert_eq!(resp.status, 204);
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("Location").map(String::as_str),
            Some("https://vhost-bkt.localhost:4566/v2.txt")
        );

        // Path-style callers stay under the bucket path, scheme honored.
        let resp = handler.handle(form_post_request_with_host(
            "localhost:4566",
            Some("https"),
            "/vhost-bkt/p.txt",
            &[("key", "p.txt")],
            Some(("p.txt", "application/octet-stream", b"payload")),
        ));
        assert_eq!(resp.status, 204);
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("Location").map(String::as_str),
            Some("https://localhost:4566/vhost-bkt/p.txt")
        );
        // No X-Forwarded-Proto → plain http (covered by the sibling tests).
    }

    // ------------------------------------------------------------------
    // POST object — policy document + SigV4 signature (auth stage)
    // ------------------------------------------------------------------

    /// Base64-encode a POST policy document with the caller's conditions.
    fn post_policy_body(conditions: serde_json::Value, expiration: Option<&str>) -> String {
        let mut doc = serde_json::Map::new();
        if let Some(exp) = expiration {
            doc.insert(
                "expiration".to_string(),
                serde_json::Value::String(exp.into()),
            );
        }
        doc.insert("conditions".to_string(), conditions);
        let text = serde_json::Value::Object(doc).to_string();
        base64::engine::general_purpose::STANDARD.encode(text)
    }

    /// The SigV4 POST field family for `policy_b64`, signed with `secret`
    /// (or corrupted when asked).
    fn post_v4_fields(
        policy_b64: &str,
        secret: &str,
        corrupt_signature: bool,
    ) -> Vec<(String, String)> {
        let amz_date = "20260226T000000Z";
        let credential = format!("123456789012/{}/us-east-1/s3/aws4_request", &amz_date[..8]);
        let mut signature =
            expected_post_signature(secret, amz_date, &credential, policy_b64).unwrap();
        if corrupt_signature {
            signature = format!("face{}", &signature[4..]);
        }
        vec![
            (
                "x-amz-algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("x-amz-credential".to_string(), credential),
            ("x-amz-date".to_string(), amz_date.to_string()),
            ("x-amz-signature".to_string(), signature),
        ]
    }

    #[test]
    fn post_object_signed_policy_success() {
        // The documented credential pair (`secret_access_key = test`) signs
        // a full v4 POST: 204 + the object stored.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "signed/obj.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("signed.txt", "application/octet-stream", b"signed payload")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("signed/obj.txt"),
            b"",
        ));
        assert_eq!(get.body, b"signed payload");
    }

    #[test]
    fn post_object_wrong_signature_rejected() {
        // A bogus x-amz-signature answers 403 SignatureDoesNotMatch carrying
        // AWSAccessKeyId + SignatureProvided + StringToSign, and stores
        // nothing.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "evil.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(&policy_b64, "not-the-twin-secret", false));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("evil.txt", "application/octet-stream", b"nope")),
        ));
        assert_eq!(
            resp.status,
            403,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>SignatureDoesNotMatch</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains("<AWSAccessKeyId>123456789012</AWSAccessKeyId>"),
            "body: {body}"
        );
        assert!(
            body.contains(&format!("<StringToSign>{policy_b64}</StringToSign>")),
            "body: {body}"
        );

        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("evil.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "tampered upload must not store");
    }

    #[test]
    fn post_object_corrupt_policy_document_rejected() {
        // A policy field that decodes but is not a JSON policy document →
        // SignatureDoesNotMatch (real AWS cannot verify a signature over an
        // unreadable policy; localstack: "the policy has been tampered with").
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let garbage = base64::engine::general_purpose::STANDARD.encode("plain text, not json");
        let mut fields = vec![
            ("key".to_string(), "corrupt.txt".to_string()),
            ("policy".to_string(), garbage.clone()),
        ];
        fields.extend(post_v4_fields(&garbage, POST_UPLOAD_SIGNING_SECRET, false));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("corrupt.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 403);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>SignatureDoesNotMatch</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains(&format!("<StringToSign>{garbage}</StringToSign>")),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_line_wrapped_policy_accepted() {
        // Real-world policy producers (and the Python decoders
        // moto/localstack rely on) tolerate line-wrapped base64: ASCII
        // whitespace inside the policy field is stripped before decoding
        // while the RAW string remains the SigV4 string-to-sign. A wrapped
        // policy signed as posted must store the upload, not 403.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let flat = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        // Wrap at 16 chars with CRLF, the PEM-style layout a client posts.
        let wrapped: String = flat
            .as_bytes()
            .chunks(16)
            .map(|c| String::from_utf8_lossy(c).to_string())
            .collect::<Vec<_>>()
            .join("\r\n");
        assert_ne!(wrapped, flat, "the test must actually wrap");

        let mut fields = vec![
            ("key".to_string(), "wrapped.txt".to_string()),
            ("policy".to_string(), wrapped.clone()),
        ];
        // The client signs the policy exactly as it posts it (wrapped).
        fields.extend(post_v4_fields(&wrapped, POST_UPLOAD_SIGNING_SECRET, false));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("wrapped.txt", "application/octet-stream", b"stored")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("wrapped.txt"),
            b"",
        ));
        assert_eq!(get.status, 200);
        assert_eq!(get.body, b"stored");
    }

    #[test]
    fn post_object_expired_policy_rejected() {
        // Expired policy with a correct signature → 403 AccessDenied,
        // "Invalid according to Policy: Policy expired." (localstack
        // validate_post_policy; the policy+signature stage precedes storing).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2001-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "late.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("late.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(
            resp.status,
            403,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(
            body.contains("Invalid according to Policy: Policy expired."),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_bad_expiration_format_400() {
        // A policy expiration in neither documented format → 400
        // InvalidRequest "Invalid date" (AWS docs; the parent lane pin).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2099-13-99T99:99:99Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "badexp.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("badexp.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>InvalidRequest</Code>"), "body: {body}");
        assert!(body.contains("Invalid date"), "body: {body}");
    }

    #[test]
    fn post_object_missing_signature_field_400() {
        // A partial v4 field family → InvalidArgument naming the first
        // missing field (localstack _is_match_with_signature_fields).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let fields = vec![
            ("key".to_string(), "half-signed.txt".to_string()),
            ("policy".to_string(), policy_b64),
            (
                "x-amz-algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            (
                "x-amz-credential".to_string(),
                "123456789012/20260226/us-east-1/s3/aws4_request".to_string(),
            ),
        ];
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("half-signed.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "body: {body}"
        );
        assert!(body.contains("X-Amz-Signature"), "body: {body}");
    }

    #[test]
    fn post_object_bad_algorithm_400() {
        // A complete V4 field set whose x-amz-algorithm is not
        // AWS4-HMAC-SHA256 fails before any signature comparison: real AWS
        // accepts exactly one algorithm in a signed POST, and rejects the
        // request with 400 InvalidArgument naming x-amz-algorithm.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        // Fields are signed correctly otherwise: the algorithm gate must
        // still fire (it precedes the signature comparison).
        let mut fields = vec![
            ("key".to_string(), "wrong-alg.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        if let Some(alg) = fields.iter_mut().find(|(k, _)| k == "x-amz-algorithm") {
            alg.1 = "AWS4-HMAC-SHA1".to_string();
        }
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("wrong-alg.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(
            resp.status,
            400,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains("<ArgumentName>x-amz-algorithm</ArgumentName>"),
            "body: {body}"
        );
        assert!(
            body.contains("<ArgumentValue>AWS4-HMAC-SHA1</ArgumentValue>"),
            "body: {body}"
        );

        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("wrong-alg.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "rejected uploads store nothing");
    }

    #[test]
    fn post_object_policy_without_signature_403() {
        // A policy document with no signature field family at all is not a
        // signed upload → 403 AccessDenied (localstack validate_post_policy).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let fields = vec![
            ("key".to_string(), "unsigned.txt".to_string()),
            ("policy".to_string(), policy_b64),
        ];
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("unsigned.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 403);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
    }

    #[test]
    fn post_object_eq_condition_mismatch_403() {
        // An eq condition the form fails → 403 AccessDenied with the AWS
        // policy-condition-failed message rendering the condition.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["eq", "$key", "expected.txt"]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "mismatched.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("m.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(
            resp.status,
            403,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(
            body.contains(r#"Invalid according to Policy: Policy Condition failed: ["eq","$key","expected.txt"]"#),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_empty_starts_with_requires_field() {
        // An empty starts-with bound accepts any VALUE of the field, but the
        // field itself must be present: a policy `["starts-with", "$key", ""]`
        // with a form that carries no key field (the agent posts through the
        // URL-key fallback) fails the condition with 403 AccessDenied like
        // real AWS/moto — the old shortcut returned Ok(true) for the empty
        // bound even when the field was absent.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut owned = vec![("policy".to_string(), policy_b64.clone())];
        owned.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        // No `key` form field: the object would come from the URL key.
        let fields: Vec<(&str, &str)> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let resp = handler.handle(form_post_request(
            "post-bkt",
            Some("from-url.txt"),
            &fields,
            Some(("f.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(
            resp.status,
            403,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(
            body.contains(
                r#"Invalid according to Policy: Policy Condition failed: ["starts-with","$key",""]"#
            ),
            "body: {body}"
        );

        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("from-url.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "failed conditions store nothing");

        // The same policy with an explicit (present) key field succeeds —
        // the empty bound is only about value freedom once the field exists.
        let mut with_key = vec![
            ("key".to_string(), "signed/any.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        with_key.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &with_key,
            Some(("g.txt", "application/octet-stream", b"2")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
    }

    #[test]
    fn post_object_uncovered_field_403() {
        // The AWS doc rule (quoted in minio's checkPostPolicy): every form
        // field outside the ignore list must appear in the policy's
        // conditions; posting an uncovered field → 403 AccessDenied.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "covered-key.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
            // Metadata is conditionable like any other field: uncovered.
            ("x-amz-meta-owner".to_string(), "me".to_string()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("m.txt", "application/octet-stream", b"body")),
        ));
        assert_eq!(
            resp.status,
            403,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(
            body.contains(r#""x-amz-meta-owner" not specified in the policy."#),
            "body: {body}"
        );

        // The same field WITH a matching condition passes and stores.
        let policy_b64 = post_policy_body(
            serde_json::json!([
                ["starts-with", "$key", ""],
                ["eq", "$x-amz-meta-owner", "me"]
            ]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "covered-key2.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
            ("x-amz-meta-owner".to_string(), "me".to_string()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("m.txt", "application/octet-stream", b"body")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
    }

    #[test]
    fn post_object_content_length_range_too_large_400() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([["starts-with", "$key", ""], ["content-length-range", 1, 10]]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "big.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let file_payload = vec![b'x'; 1000];
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("big.txt", "application/octet-stream", &file_payload)),
        ));
        assert_eq!(
            resp.status,
            400,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>EntityTooLarge</Code>"), "body: {body}");
        assert!(
            body.contains("Your proposed upload exceeds the maximum allowed size"),
            "body: {body}"
        );
        assert!(
            body.contains("<ProposedSize>1000</ProposedSize>"),
            "body: {body}"
        );
        assert!(
            body.contains("<MaxSizeAllowed>10</MaxSizeAllowed>"),
            "body: {body}"
        );

        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("big.txt"),
            b"",
        ));
        assert_eq!(get.status, 404, "rejected uploads store nothing");
    }

    #[test]
    fn post_object_content_length_range_too_small_400() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([
                ["starts-with", "$key", ""],
                ["content-length-range", 100, 99999]
            ]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "tiny.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("tiny.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>EntityTooSmall</Code>"), "body: {body}");
        assert!(
            body.contains("<MinSizeAllowed>100</MinSizeAllowed>"),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_bucket_condition_mismatch_403() {
        // {"bucket": name} conditions compare the request's bucket, so a
        // policy pinned to another bucket fails (localstack
        // additional_policy_metadata["bucket"]).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([
                {"bucket": "some-other-bucket"},
                ["starts-with", "$key", ""]
            ]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "cross-posted.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("m.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 403);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains(r#"Policy Condition failed: {"bucket":"some-other-bucket"}"#),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_invalid_simple_condition_400() {
        // A dict condition with more than one property → 400
        // InvalidPolicyDocument "Invalid Simple-Condition".
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([{"bucket": "post-bkt", "key": "k.txt"}]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "k.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("m.txt", "application/octet-stream", b"1")),
        ));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>InvalidPolicyDocument</Code>"),
            "body: {body}"
        );
        assert!(
            body.contains("Simple-Conditions must have exactly one property"),
            "body: {body}"
        );
    }

    #[test]
    fn post_object_bucket_and_key_conditions_success() {
        // A policy matching the request exactly: bucket dict condition, key
        // starts-with, and an in-range content-length-range → stored.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("post-bkt"), None, b""));

        let policy_b64 = post_policy_body(
            serde_json::json!([
                {"bucket": "post-bkt"},
                ["starts-with", "$key", "docs/"],
                ["content-length-range", 1, 1 << 20]
            ]),
            Some("2080-01-01T00:00:00Z"),
        );
        let mut fields = vec![
            ("key".to_string(), "docs/final.txt".to_string()),
            ("policy".to_string(), policy_b64.clone()),
        ];
        fields.extend(post_v4_fields(
            &policy_b64,
            POST_UPLOAD_SIGNING_SECRET,
            false,
        ));
        let resp = handler.handle(form_post_request_owned(
            "post-bkt",
            &fields,
            Some(("final.txt", "application/octet-stream", b"checkpointed")),
        ));
        assert_eq!(
            resp.status,
            204,
            "body: {:?}",
            String::from_utf8_lossy(&resp.body)
        );
        let get = handler.handle(make_req(
            "GetObject",
            Some("post-bkt"),
            Some("docs/final.txt"),
            b"",
        ));
        assert_eq!(get.body, b"checkpointed");
    }

    /// The stored object pair the response-override tests pull back.
    fn put_response_override_object(handler: &S3Handler) {
        handler.handle(make_req("CreateBucket", Some("ov-bkt"), None, b""));
        let mut req = make_req("PutObject", Some("ov-bkt"), Some("obj.txt"), b"body");
        req.headers
            .insert("content-type".to_string(), "text/x-stored".to_string());
        handler.handle(req);
    }

    #[test]
    fn response_overrides_get() {
        // Each of the six response-* query params overrides exactly its own
        // response header (AWS "Get Object" response header overrides; moto
        // ALLOWED_HEADER_OVERRIDES); headers without an override keep the
        // stored values and the body is untouched.
        let handler = S3Handler::new();
        put_response_override_object(&handler);

        let resp = handler.handle(make_req("GetObject", Some("ov-bkt"), Some("obj.txt"), b""));
        assert_eq!(
            header_map(&resp).get("Content-Type").map(String::as_str),
            Some("text/x-stored")
        );

        let mut req = make_req("GetObject", Some("ov-bkt"), Some("obj.txt"), b"");
        for (param, value) in [
            ("response-content-type", "application/json"),
            ("response-content-language", "pt-BR"),
            ("response-expires", "Wed, 21 Oct 2115 07:28:00 GMT"),
            ("response-cache-control", "no-cache"),
            (
                "response-content-disposition",
                r#"attachment; filename="x.bin""#,
            ),
            ("response-content-encoding", "gzip"),
        ] {
            req.query_params
                .insert(param.to_string(), value.to_string());
        }
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"body", "overrides are response headers only");
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("Content-Type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            headers.get("Content-Language").map(String::as_str),
            Some("pt-BR")
        );
        assert_eq!(
            headers.get("Expires").map(String::as_str),
            Some("Wed, 21 Oct 2115 07:28:00 GMT")
        );
        assert_eq!(
            headers.get("Cache-Control").map(String::as_str),
            Some("no-cache")
        );
        assert_eq!(
            headers.get("Content-Disposition").map(String::as_str),
            Some(r#"attachment; filename="x.bin""#)
        );
        assert_eq!(
            headers.get("Content-Encoding").map(String::as_str),
            Some("gzip")
        );
        // Only the six mapped headers change.
        assert!(headers.get("ETag").map(String::as_str).is_some());
    }

    #[test]
    fn response_overrides_head() {
        // HEAD honors the same six overrides as GET (moto applies
        // ALLOWED_HEADER_OVERRIDES to HEAD when the URL carries the params);
        // the answer stays body-less.
        let handler = S3Handler::new();
        put_response_override_object(&handler);

        let mut req = make_req("HeadObject", Some("ov-bkt"), Some("obj.txt"), b"");
        req.query_params.insert(
            "response-content-type".to_string(),
            "application/x-override".to_string(),
        );
        req.query_params.insert(
            "response-content-disposition".to_string(),
            r#"attachment; filename="h.bin""#.to_string(),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.is_empty(), "HEAD has no body");
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("Content-Type").map(String::as_str),
            Some("application/x-override")
        );
        assert_eq!(
            headers.get("Content-Disposition").map(String::as_str),
            Some(r#"attachment; filename="h.bin""#)
        );
        // Non-overridden headers keep their values.
        assert!(headers.get("ETag").map(String::as_str).is_some());
        assert!(headers.get("Last-Modified").map(String::as_str).is_some());
    }

    #[test]
    fn test_directory_bucket_type_from_create_config() {
        let handler = S3Handler::new();
        let name = "compat-dir-abc123--use1-az1--x-s3";

        // moto: Explicit Bucket{Type=Directory} config makes it a directory bucket.
        let req = make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        );
        assert_eq!(handler.handle(req).status, 200);
        let bucket = handler
            .get_state(123456789012, "us-east-1")
            .get_bucket(name)
            .unwrap();
        assert!(bucket.directory, "moto predicate: bucket_type == Directory");

        // No directory config → ordinary bucket (no --x-s3 name inference).
        let req = make_req("CreateBucket", Some("plain-bucket"), None, b"");
        assert_eq!(handler.handle(req).status, 200);
        let bucket = handler
            .get_state(123456789012, "us-east-1")
            .get_bucket("plain-bucket")
            .unwrap();
        assert!(!bucket.directory);
    }

    #[test]
    fn test_directory_bucket_name_validation() {
        let handler = S3Handler::new();
        // moto models.py:1920: Bucket{Type=Directory} with a name that lacks
        // the "--azid-azN--x-s3" suffix is InvalidBucketName.
        let req = make_req(
            "CreateBucket",
            Some("not-a-directory-bucket"),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 400);
        assert!(String::from_utf8_lossy(&resp.body).contains("InvalidBucketName"));

        // The az id must be lowercase alnum and the az zone numeric.
        let req = make_req(
            "CreateBucket",
            Some("compat-ren-global-s3--x-s3"),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        );
        assert_eq!(handler.handle(req).status, 400);

        let valid = S3Handler::directory_bucket_name_valid("compat-dir-abc123--use1-az1--x-s3");
        assert!(valid);
        assert!(!S3Handler::directory_bucket_name_valid("plain-bucket"));
        assert!(!S3Handler::directory_bucket_name_valid(
            "compat-ren-global-s3--x-s3"
        ));
    }

    #[test]
    fn test_create_session_moto_credentials() {
        let handler = S3Handler::new();
        let name = "compat-dir-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));

        let req = make_req("CreateSession", Some(name), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert_session_credentials(&body);
    }

    #[test]
    fn test_create_session_non_directory_is_invalid_request() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("plain-bucket"), None, b""));

        // moto models.py:1980: S3ClientError("InvalidRequest",
        // "CreateSession is only supported for directory buckets.")
        let req = make_req("CreateSession", Some("plain-bucket"), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>InvalidRequest</Code>"));
        assert!(body.contains("CreateSession is only supported for directory buckets."));
    }

    #[test]
    fn test_create_session_answers_before_bucket_exists() {
        let handler = S3Handler::new();
        // botocore's S3 Express refresh sends CreateSession BEFORE its own
        // CreateBucket lands (v4-s3express auth resolution), so a missing
        // bucket still gets credentials — this is what the compat fixture
        // flow requires.
        let name = "compat-dir-later--use1-az1--x-s3";
        let req = make_req("CreateSession", Some(name), None, b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_session_credentials(&String::from_utf8_lossy(&resp.body));
    }

    #[test]
    fn test_rename_object_moves_object() {
        let handler = S3Handler::new();
        let name = "compat-ren-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));
        handler.handle(make_req(
            "PutObject",
            Some(name),
            Some("source.txt"),
            b"hello rename",
        ));

        // moto responses.py:3769: PUT /{Bucket}/{Key}?renameObject with
        // x-amz-rename-source: "<bucket>/<key>".
        let mut req = make_req("RenameObject", Some(name), Some("destination.txt"), b"");
        req.headers.insert(
            "x-amz-rename-source".to_string(),
            format!("{name}/source.txt"),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);

        // The destination carries the moved object; the source is gone.
        let dest = handler.handle(make_req(
            "GetObject",
            Some(name),
            Some("destination.txt"),
            b"",
        ));
        assert_eq!(dest.status, 200);
        assert_eq!(dest.body, b"hello rename".to_vec());
        let src = handler.handle(make_req("GetObject", Some(name), Some("source.txt"), b""));
        assert_eq!(src.status, 404);
        assert!(String::from_utf8_lossy(&src.body).contains("NoSuchKey"));
    }

    #[test]
    fn test_rename_object_missing_source_is_no_such_key() {
        let handler = S3Handler::new();
        let name = "compat-ren-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));

        let mut req = make_req("RenameObject", Some(name), Some("dst"), b"");
        req.headers.insert(
            "x-amz-rename-source".to_string(),
            format!("{name}/missing.txt"),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 404);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>NoSuchKey</Code>"),
            "moto MissingKey: 404 NoSuchKey 'The specified key does not exist.' got: {body}"
        );

        // moto responses.py:1861: an empty/absent x-amz-rename-source also
        // resolves to NoSuchKey (no InvalidArgument shadowing).
        let req = make_req("RenameObject", Some(name), Some("dst"), b"");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_rename_object_moto_move_semantics() {
        let handler = S3Handler::new();
        let name = "compat-ren-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));
        handler.handle(make_req("PutObject", Some(name), Some("a.txt"), b"AAA"));
        handler.handle(make_req("PutObject", Some(name), Some("b.txt"), b"BBB"));

        // Existing destination is overwritten by the move.
        let mut req = make_req("RenameObject", Some(name), Some("b.txt"), b"");
        req.headers
            .insert("x-amz-rename-source".to_string(), format!("{name}/a.txt"));
        assert_eq!(handler.handle(req).status, 200);
        let dest = handler.handle(make_req("GetObject", Some(name), Some("b.txt"), b""));
        assert_eq!(dest.body, b"AAA".to_vec());

        // moto dict order (insert then delete): source == destination
        // renames drop the entry, exactly like bucket.keys[new] = obj; del
        // bucket.keys[source] deletes the key when both are equal.
        let mut req = make_req("RenameObject", Some(name), Some("b.txt"), b"");
        req.headers
            .insert("x-amz-rename-source".to_string(), format!("{name}/b.txt"));
        assert_eq!(handler.handle(req).status, 200);
        let gone = handler.handle(make_req("GetObject", Some(name), Some("b.txt"), b""));
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn test_rename_object_non_directory_is_invalid_request() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("plain-bucket"), None, b""));
        handler.handle(make_req(
            "PutObject",
            Some("plain-bucket"),
            Some("k"),
            b"data",
        ));

        let mut req = make_req("RenameObject", Some("plain-bucket"), Some("k2"), b"");
        req.headers.insert(
            "x-amz-rename-source".to_string(),
            "plain-bucket/k".to_string(),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 400);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>InvalidRequest</Code>"));
        assert!(body.contains("RenameObject is only supported for directory buckets."));
    }

    #[test]
    fn test_detect_session_operations() {
        let mut q = HashMap::new();
        q.insert("session".to_string(), String::new());
        assert_eq!(
            S3Handler::detect_s3_operation("GET", "/", &q),
            Some("CreateSession".to_string())
        );
        // moto responses.py:583 intercepts "session" in the bucket-level GET.
        assert_eq!(
            S3Handler::detect_s3_operation("GET", "/bucket", &q),
            Some("CreateSession".to_string())
        );
        q.insert("createSession".to_string(), String::new());
        assert_eq!(
            S3Handler::detect_s3_operation("POST", "/", &q),
            Some("CreateSession".to_string())
        );
        assert_eq!(
            S3Handler::detect_s3_operation("POST", "/bucket", &q),
            Some("CreateSession".to_string())
        );
    }

    #[test]
    fn test_vhost_bucket_from_host() {
        // Parity with the gateway's s3_routing::parse_s3_vhost shapes.
        assert_eq!(
            S3Handler::vhost_bucket_from_host(Some("mybucket--use1-az1--x-s3.localhost:4566")),
            Some("mybucket--use1-az1--x-s3".to_string())
        );
        assert_eq!(
            S3Handler::vhost_bucket_from_host(Some("mybucket.s3.us-east-1.amazonaws.com")),
            Some("mybucket".to_string())
        );
        assert_eq!(
            S3Handler::vhost_bucket_from_host(Some("mybucket.s3.localhost.robotocore.cloud")),
            Some("mybucket".to_string())
        );
        // Path-style hosts: no virtual host.
        assert_eq!(
            S3Handler::vhost_bucket_from_host(Some("localhost:4783")),
            None
        );
        assert_eq!(
            S3Handler::vhost_bucket_from_host(Some("s3.amazonaws.com")),
            None
        );
        assert_eq!(S3Handler::vhost_bucket_from_host(Some("example.com")), None);
        assert_eq!(S3Handler::vhost_bucket_from_host(None), None);
    }

    #[test]
    fn test_vhost_session_request_resolves_bucket() {
        // A botocore S3 Express refresh: GET /?session with the bucket in a
        // virtual-hosted Host header. The twin must resolve the bucket and
        // answer the moto CreateSession XML.
        let handler = S3Handler::new();
        let name = "compat-dir-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));

        let req = wire_req(
            "GET",
            None,
            None,
            &[("session", "")],
            &format!("{}.localhost:4783", name),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_session_credentials(&String::from_utf8_lossy(&resp.body));

        // POST /{Bucket}?createSession (the wire shape from newer S3
        // endpoint rules) answers the same credentials.
        let req = wire_req(
            "POST",
            Some(name),
            None,
            &[("createSession", "")],
            "localhost:4783",
        );
        assert_eq!(handler.handle(req).status, 200);
    }

    #[test]
    fn test_vhost_object_request_rewrites_to_path_style() {
        // Virtual-hosted S3 Express object traffic: PUT /destination.txt
        // with Host=<bucket>.localhost — the gateway split "destination.txt"
        // out as the bucket. The handler must rebuild bucket/key and re-detect
        // RenameObject.
        let handler = S3Handler::new();
        let name = "compat-ren-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));
        handler.handle(make_req(
            "PutObject",
            Some(name),
            Some("source.txt"),
            b"hello rename",
        ));

        let mut req = wire_req(
            "PUT",
            Some("destination.txt"),
            None,
            &[("renameObject", "")],
            &format!("{}.localhost:4783", name),
        );
        req.headers.insert(
            "x-amz-rename-source".to_string(),
            format!("{name}/source.txt"),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        let dest = handler.handle(make_req(
            "GetObject",
            Some(name),
            Some("destination.txt"),
            b"",
        ));
        assert_eq!(dest.status, 200);
        assert_eq!(dest.body, b"hello rename".to_vec());
    }

    #[test]
    fn test_vhost_rename_recovered_from_rename_source_header() {
        // On the wire the gateway's to_s3_request drops the value-less
        // renameObject query param, so the re-detect lands on PutObject;
        // the x-amz-rename-source header (moto s3/responses.py:1861) is the
        // operation's own marker and recovers RenameObject.
        let handler = S3Handler::new();
        let name = "compat-ren-abc123--use1-az1--x-s3";
        handler.handle(make_req(
            "CreateBucket",
            Some(name),
            None,
            DIR_BUCKET_CONFIG.as_bytes(),
        ));
        handler.handle(make_req(
            "PutObject",
            Some(name),
            Some("source.txt"),
            b"hello rename",
        ));

        // No query params survive the gateway; the arriving op is the blind
        // bucket-level PUT fallback, exactly like the real wire.
        let mut req = wire_req(
            "PUT",
            Some("destination.txt"),
            None,
            &[],
            &format!("{}.localhost:4783", name),
        );
        req.headers.insert(
            "x-amz-rename-source".to_string(),
            format!("{name}/source.txt"),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        let dest = handler.handle(make_req(
            "GetObject",
            Some(name),
            Some("destination.txt"),
            b"",
        ));
        assert_eq!(dest.status, 200);
        assert_eq!(dest.body, b"hello rename".to_vec());
        let src = handler.handle(make_req("GetObject", Some(name), Some("source.txt"), b""));
        assert_eq!(src.status, 404);
    }

    #[test]
    fn test_vhost_session_without_bucket_falls_back_to_list_buckets() {
        // moto answers GET / (path-style, no bucket) from the root dispatch
        // (all_buckets) even with a ?session query — no virtual host means
        // no bucket to establish a session for.
        let handler = S3Handler::new();
        let req = wire_req("GET", None, None, &[("session", "")], "localhost:4783");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(String::from_utf8_lossy(&resp.body).contains("ListAllMyBucketsResult"));
    }

    // -----------------------------------------------------------------------
    // Delete-marker GET semantics (moto PR #10428: get_object() on a delete
    // marker returns the x-amz-delete-marker header, and 405 for its version).
    // -----------------------------------------------------------------------

    /// Enable versioning, mirroring the PutBucketVersioning wire shape.
    fn enable_versioning(handler: &S3Handler, bucket: &str) {
        let mut vreq = make_req(
            "PutBucketVersioning",
            Some(bucket),
            None,
            b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>",
        );
        vreq.query_params
            .insert("versioning".to_string(), String::new());
        assert_eq!(handler.handle(vreq).status, 200);
    }

    fn header_map(resp: &AwsResponse) -> HashMap<String, String> {
        resp.headers.clone().into_iter().collect()
    }

    #[test]
    fn delete_marker_get_without_version_id_returns_marker_headers() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm1"), None, b""));
        enable_versioning(&handler, "dm1");
        handler.handle(make_req("PutObject", Some("dm1"), Some("the-key"), b"data"));
        let resp = handler.handle(make_req("DeleteObject", Some("dm1"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("delete_object returns the marker's version id");

        // The current version is a delete marker: 404 NoSuchKey, but the
        // delete-marker headers carry the marker's identity (moto
        // MissingKey + x-amz-delete-marker/x-amz-version-id).
        let resp = handler.handle(make_req("GetObject", Some("dm1"), Some("the-key"), b""));
        assert_eq!(resp.status, 404);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Code>NoSuchKey</Code>"), "body: {body}");
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("x-amz-delete-marker").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            headers.get("x-amz-version-id").map(String::as_str),
            Some(marker_id.as_str())
        );
    }

    #[test]
    fn delete_marker_get_with_marker_version_id_returns_405() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm2"), None, b""));
        enable_versioning(&handler, "dm2");
        handler.handle(make_req("PutObject", Some("dm2"), Some("the-key"), b"data"));
        let resp = handler.handle(make_req("DeleteObject", Some("dm2"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("marker version id");

        // The requested version is a delete marker: 405 MethodNotAllowed
        // with Allow: DELETE, not NoSuchVersion/NoSuchKey.
        let mut req = make_req("GetObject", Some("dm2"), Some("the-key"), b"");
        req.query_params
            .insert("versionId".to_string(), marker_id.clone());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 405);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("<Code>MethodNotAllowed</Code>"),
            "body: {body}"
        );
        assert!(body.contains("<Method>GET</Method>"), "body: {body}");
        assert!(
            body.contains("<ResourceType>DeleteMarker</ResourceType>"),
            "body: {body}"
        );
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("x-amz-delete-marker").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            headers.get("x-amz-version-id").map(String::as_str),
            Some(marker_id.as_str())
        );
        assert_eq!(headers.get("Allow").map(String::as_str), Some("DELETE"));
        assert!(headers.contains_key("Last-Modified"));
    }

    #[test]
    fn delete_marker_get_recreated_key_is_live_again() {
        // After a re-put the marker no longer shadows the key: plain 200
        // with no delete-marker headers.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm3"), None, b""));
        enable_versioning(&handler, "dm3");
        handler.handle(make_req("PutObject", Some("dm3"), Some("the-key"), b"v1"));
        handler.handle(make_req("DeleteObject", Some("dm3"), Some("the-key"), b""));
        handler.handle(make_req("PutObject", Some("dm3"), Some("the-key"), b"v2"));

        let resp = handler.handle(make_req("GetObject", Some("dm3"), Some("the-key"), b""));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"v2".to_vec());
        let headers = header_map(&resp);
        assert!(!headers.contains_key("x-amz-delete-marker"));

        // The delete-marker version itself still answers 405 after the re-put.
        let resp = handler.handle(make_req("DeleteObject", Some("dm3"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("second marker version id");
        let mut req = make_req("GetObject", Some("dm3"), Some("the-key"), b"");
        req.query_params.insert("versionId".to_string(), marker_id);
        assert_eq!(handler.handle(req).status, 405);
    }

    #[test]
    fn delete_marker_get_head_without_version_id_returns_marker_headers() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm4"), None, b""));
        enable_versioning(&handler, "dm4");
        handler.handle(make_req("PutObject", Some("dm4"), Some("the-key"), b"data"));
        let resp = handler.handle(make_req("DeleteObject", Some("dm4"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("marker version id");

        // head_object already routed markers to its 404; ensure the marker
        // headers ride on the HEAD 404 exactly like moto's head path.
        let resp = handler.handle(make_req("HeadObject", Some("dm4"), Some("the-key"), b""));
        assert_eq!(resp.status, 404);
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("x-amz-delete-marker").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            headers.get("x-amz-version-id").map(String::as_str),
            Some(marker_id.as_str())
        );
    }

    #[test]
    fn delete_marker_get_head_with_marker_version_id_returns_405() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm5"), None, b""));
        enable_versioning(&handler, "dm5");
        handler.handle(make_req("PutObject", Some("dm5"), Some("the-key"), b"data"));
        let resp = handler.handle(make_req("DeleteObject", Some("dm5"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("marker version id");

        let mut req = make_req("HeadObject", Some("dm5"), Some("the-key"), b"");
        req.query_params
            .insert("versionId".to_string(), marker_id.clone());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 405);
        let headers = header_map(&resp);
        assert_eq!(
            headers.get("x-amz-delete-marker").map(String::as_str),
            Some("true")
        );
        assert_eq!(headers.get("Allow").map(String::as_str), Some("DELETE"));
        assert_eq!(
            headers.get("x-amz-version-id").map(String::as_str),
            Some(marker_id.as_str())
        );
    }

    #[test]
    fn delete_marker_get_list_object_versions_placement_untouched() {
        // Both marker GET shapes must leave ListObjectVersions untouched:
        // the deleted key keeps its Version row and the DeleteMarker row.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm6"), None, b""));
        enable_versioning(&handler, "dm6");
        handler.handle(make_req("PutObject", Some("dm6"), Some("the-key"), b"data"));
        let resp = handler.handle(make_req("DeleteObject", Some("dm6"), Some("the-key"), b""));
        let marker_id = header_map(&resp)
            .get("x-amz-version-id")
            .cloned()
            .expect("marker version id");

        // Run both marker GET shapes first, like the moto test sequence.
        let resp = handler.handle(make_req("GetObject", Some("dm6"), Some("the-key"), b""));
        assert_eq!(resp.status, 404);
        let mut req = make_req("GetObject", Some("dm6"), Some("the-key"), b"");
        req.query_params.insert("versionId".to_string(), marker_id);
        assert_eq!(handler.handle(req).status, 405);

        let resp = handler.handle(make_req("ListObjectVersions", Some("dm6"), None, b""));
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body);
        assert_eq!(body.matches("<Version>").count(), 1);
        assert_eq!(body.matches("<DeleteMarker>").count(), 1);
        assert!(body.contains("<IsLatest>false</IsLatest>"));
    }

    #[test]
    fn delete_marker_get_missing_key_has_no_marker_headers() {
        // A never-existed key stays plain NoSuchKey: no marker headers can
        // leak onto the plain miss (moto only answers markers when the key
        // entry carries one).
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("dm7"), None, b""));
        enable_versioning(&handler, "dm7");

        let resp = handler.handle(make_req("GetObject", Some("dm7"), Some("no-such-key"), b""));
        assert_eq!(resp.status, 404);
        let headers = header_map(&resp);
        assert!(!headers.contains_key("x-amz-delete-marker"));
        assert!(!headers.contains_key("x-amz-version-id"));
    }

    // -----------------------------------------------------------------------
    // S3 key percent-encoding round-trip (localstack discussion 54, ex
    // localstack #13745): the wire path may percent-encode parts of a key;
    // stored state must be the DECODED key everywhere — object storage key,
    // StartAfter comparison, continuation tokens, prefix filtering, delete,
    // get, head and copy.
    // -----------------------------------------------------------------------

    /// The decoded set of `<Key>` entries in a ListObjectsV2 body.
    fn listed_keys(handler: &S3Handler, bucket: &str) -> Vec<String> {
        let resp = handler.handle(make_req("ListObjectsV2", Some(bucket), None, b""));
        assert_eq!(resp.status, 200);
        let body = String::from_utf8_lossy(&resp.body).to_string();
        xml_sections(&body, "Key")
            .iter()
            .map(|k| xml_unescape(k))
            .collect()
    }

    /// Query listing with pre-decoded params, exactly as the gateway
    /// delivers them (src/core/server.rs decodes query params once).
    fn list_query(handler: &S3Handler, bucket: &str, query: &[(&str, &str)]) -> String {
        let mut req = make_req("ListObjectsV2", Some(bucket), None, b"");
        for (k, v) in query {
            req.query_params.insert(k.to_string(), v.to_string());
        }
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        String::from_utf8_lossy(&resp.body).to_string()
    }

    #[test]
    fn key_decode_equals_key_round_trip_with_start_after() {
        // The observed bug: PUT wire key "data/NE%3DABC/quz" stored
        // "%3D" instead of "=", so the key never listed or compared
        // against its decoded StartAfter.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc1"), None, b""));

        // Wire form as botocore quotes it: "=" sent raw is legal, "%3D"
        // is its encoded spelling; both must land in the same entry.
        let resp = handler.handle(make_req(
            "PutObject",
            Some("enc1"),
            Some("data/NE=ABC/quz"),
            b"v1",
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "PutObject",
            Some("enc1"),
            Some("data/NE%3DABC/quz"),
            b"v2",
        ));
        assert_eq!(resp.status, 200);

        // One entry under the decoded key, holding the second write.
        assert_eq!(listed_keys(&handler, "enc1"), ["data/NE=ABC/quz"]);

        // Decoded and encoded GET spellings both resolve to it.
        let resp = handler.handle(make_req(
            "GetObject",
            Some("enc1"),
            Some("data/NE=ABC/quz"),
            b"",
        ));
        assert_eq!(resp.body, b"v2".to_vec());
        let resp = handler.handle(make_req(
            "GetObject",
            Some("enc1"),
            Some("data/NE%3DABC/quz"),
            b"",
        ));
        assert_eq!(resp.body, b"v2".to_vec());
        let resp = handler.handle(make_req(
            "HeadObject",
            Some("enc1"),
            Some("data/NE=ABC/quz"),
            b"",
        ));
        assert_eq!(resp.status, 200);
        let headers = header_map(&resp);
        assert_eq!(headers.get("Content-Length").map(String::as_str), Some("2"));

        // StartAfter with raw '=' against decoded state still paginates.
        let body = list_query(&handler, "enc1", &[("start-after", "data/NE=ABC/")]);
        assert!(
            xml_unescape(&xml_text(&body, "Key").expect("the key after the prefix"))
                .contains("data/NE=ABC/quz"),
            "body: {body}"
        );
    }

    #[test]
    fn key_decode_start_after_excludes_earlier_keys() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc2"), None, b""));
        for key in ["before", "data/NE=ABC/one", "data/NE=ABC/quz"] {
            handler.handle(make_req("PutObject", Some("enc2"), Some(key), b"x"));
        }

        // Real AWS returns the objects after the (decoded) StartAfter;
        // the stored-encoded-key bug returned an empty result.
        let body = list_query(&handler, "enc2", &[("start-after", "data/NE=ABC/")]);
        let keys = xml_sections(&body, "Key")
            .iter()
            .map(|k| xml_unescape(k))
            .collect::<Vec<_>>();
        assert_eq!(keys, ["data/NE=ABC/one", "data/NE=ABC/quz"]);

        // StartAfter itself is excluded; and a marker carrying '>' and '='
        // raw ("k>=1" on the wire) compares against the DECODED keys — the
        // stored-encoded-key bug would sort the keys below the marker.
        for key in ["ka", "k=a", "k<a"] {
            handler.handle(make_req("PutObject", Some("enc2"), Some(key), b"x"));
        }
        // Sorting: "k<a" < "k=a" < "ka" ('<' < '=' < 'a'); only "ka" sorts
        // above the marker.
        let body = list_query(&handler, "enc2", &[("start-after", "k>=1")]);
        let keys = xml_sections(&body, "Key")
            .iter()
            .map(|k| xml_unescape(k))
            .collect::<Vec<_>>();
        assert_eq!(keys, ["ka"]);
    }

    #[test]
    fn key_decode_literal_percent_is_decoded_once() {
        // A literal "%20" uploaded raw arrives as "%2520"; a single decode
        // recovers the literal characters, never a space. The client sends
        // the same "%2520" spelling back to GET it.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc3"), None, b""));
        let resp = handler.handle(make_req(
            "PutObject",
            Some("enc3"),
            Some("a%2520b.txt"),
            b"literal",
        ));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req(
            "GetObject",
            Some("enc3"),
            Some("a%2520b.txt"),
            b"",
        ));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"literal".to_vec());

        // A literal space and a literal percent sign coexist as distinct keys;
        // the DECODED spellings are what the state holds and what the listing
        // shows.
        let resp = handler.handle(make_req(
            "PutObject",
            Some("enc3"),
            Some("sp%20ace.txt"),
            b"spacey",
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req("GetObject", Some("enc3"), Some("sp ace.txt"), b""));
        assert_eq!(resp.body, b"spacey".to_vec());

        let keys = listed_keys(&handler, "enc3");
        assert_eq!(keys, ["a%20b.txt", "sp ace.txt"]);
    }

    #[test]
    fn key_decode_unicode_key_round_trip() {
        // botocore quotes UTF-8 as %E2%80%94 for the em dash.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc4"), None, b""));
        let resp = handler.handle(make_req(
            "PutObject",
            Some("enc4"),
            Some("uni%E2%80%94d.txt"),
            b"\xe2\x80\x94-body",
        ));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req(
            "GetObject",
            Some("enc4"),
            Some("uni\u{2014}d.txt"),
            b"",
        ));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "\u{2014}-body".as_bytes().to_vec());

        // The listing shows the decoded, xml-escaped key.
        let body = list_query(&handler, "enc4", &[]);
        assert!(body.contains("<Key>uni\u{2014}d.txt</Key>"), "body: {body}");
    }

    #[test]
    fn key_decode_delete_encoded_key() {
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc4b"), None, b""));
        handler.handle(make_req(
            "PutObject",
            Some("enc4b"),
            Some("del%20k.txt"),
            b"gone",
        ));
        // Delete sent in the encoded wire spelling removes the decoded key.
        let resp = handler.handle(make_req(
            "DeleteObject",
            Some("enc4b"),
            Some("del%20k.txt"),
            b"",
        ));
        assert_eq!(resp.status, 204);

        let resp = handler.handle(make_req("GetObject", Some("enc4b"), Some("del k.txt"), b""));
        assert_eq!(resp.status, 404);
        assert_eq!(listed_keys(&handler, "enc4b"), Vec::<String>::new());
    }

    #[test]
    fn key_decode_copy_from_encoded_source() {
        // x-amz-copy-source quotes the source key; copy must write the
        // destination under its decoded key.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc5"), None, b""));
        handler.handle(make_req("PutObject", Some("enc5"), Some("src%3D1"), b"z"));

        let mut req = make_req("CopyObject", Some("enc5"), Some("cp%20d.txt"), b"");
        req.headers
            .insert("x-amz-copy-source".to_string(), "/enc5/src%3D1".to_string());
        req.headers
            .insert("content-type".to_string(), "application/xml".to_string());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);

        let mut req = make_req("GetObject", Some("enc5"), Some("cp d.txt"), b"");
        req.headers.remove("x-amz-copy-source");
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"z".to_vec());
        assert_eq!(listed_keys(&handler, "enc5"), ["cp d.txt", "src=1"]);
    }

    #[test]
    fn key_decode_pagination_across_continuation_tokens() {
        // Continuation tokens are opaque base64 of the stored key; page
        // through '>'/'=' keys and confirm the decoded sequence.
        let handler = S3Handler::new();
        handler.handle(make_req("CreateBucket", Some("enc6"), None, b""));
        // String order: "k=1" < "k>=" < "z9" ('=' < '>' < 'z').
        for key in ["z9", "k=1", "k>="] {
            handler.handle(make_req("PutObject", Some("enc6"), Some(key), b"row"));
        }

        let mut keys = Vec::new();
        let mut token = String::new();
        loop {
            let query: Vec<(&str, &str)> = if token.is_empty() {
                vec![("max-keys", "2")]
            } else {
                vec![("max-keys", "2"), ("continuation-token", token.as_str())]
            };
            let body = list_query(&handler, "enc6", &query);
            keys.extend(xml_sections(&body, "Key").iter().map(|k| xml_unescape(k)));
            match xml_text(&body, "NextContinuationToken") {
                Some(next) => token = next,
                None => break,
            }
        }
        assert_eq!(keys, ["k=1", "k>=", "z9"]);

        // The token is derived from the decoded key, not the wire form.
        let body = list_query(&handler, "enc6", &[("max-keys", "2")]);
        let token = xml_text(&body, "NextContinuationToken").expect("token");
        let decoded = decode_token(&token).expect("base64 key token");
        assert_eq!(decoded, "k>=");
    }
}

/// moto master 086af7ac (#10415): LastModified is a header-bound member of
/// GetObjectAttributesOutput, so it renders in the `Last-Modified` response
/// header in RFC 822 format. Moto 5.2.4 wrote an ISO-8601 string into that
/// header, which non-python SDKs (that parse HTTP dates) reject.
#[cfg(test)]
mod catchup_fx5_object_attributes_tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::Value;

    fn make_req(operation: &str, bucket: Option<&str>, key: Option<&str>, body: &[u8]) -> AwsRequest {
        AwsRequest {
            service: "s3".to_string(),
            operation: operation.to_string(),
            account: 424242222222,
            region: "us-fx5-catchup".to_string(),
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
    fn get_object_attributes_last_modified_is_an_http_date_header() {
        let handler = S3Handler::new();
        let resp = handler.handle(make_req("CreateBucket", Some("fx5-attrs"), None, b""));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req("PutObject", Some("fx5-attrs"), Some("k1"), b"x"));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req(
            "GetObjectAttributes",
            Some("fx5-attrs"),
            Some("k1"),
            b"",
        ));
        assert_eq!(resp.status, 200, "got {:?}", resp.status);

        // The header must be parseable as an RFC 2822 / HTTP-date — moto's
        // test asserts the same through werkzeug.http.parse_date.
        let last_modified = resp
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("Last-Modified"))
            .map(|(_, v)| v.clone())
            .expect("Last-Modified header present");
        let parsed = chrono::DateTime::parse_from_rfc2822(&last_modified);
        assert!(
            parsed.is_ok(),
            "Last-Modified must be an HTTP date, got {last_modified:?}"
        );

        // The member is header-bound in the S3 model — moto master emits the
        // XML body without a LastModified element (ObjectSize only, alongside
        // the pre-existing ETag row).
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<ObjectSize>"), "got {body}");
        assert!(!body.contains("<LastModified>"), "got {body}");
    }
}
