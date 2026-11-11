//! ENFORCE_IAM request enforcement — port of the python gateway's
//! `gateway/iam_middleware.py` + `_detect_rest_operation` (deleted with
//! the python product).
//!
//! Opt-in at runtime: `POST /_robotocore/config {"ENFORCE_IAM": "1"}`,
//! which itself requires the twin to boot with `ENABLE_CONFIG_UPDATES=1`
//! (the runtime-config endpoint's gate). When on, every request for an
//! enforcing service is matched against the principal's identity policies
//! (user inline + attached, plus the inline + attached policies of every
//! group the user belongs to) through the same evaluator the IAM
//! Simulate* APIs use; anything but an explicit Allow answers 403
//! AccessDenied in the service's wire protocol.
//!
//! IAM and STS are exempt (bootstrap deadlock: creating users and keys
//! requires IAM calls, and credential issuance is STS).
//!
//! Principal scope: IAM users found by access key. Assumed-role session
//! credentials are DENIED, not evaluated — STS issues a canned session
//! key and keeps no per-session registry, so a temp key maps to no user
//! and takes the implicit-deny path (the python middleware evaluated
//! role policies through its STS session store; that registry is not
//! ported). Every `intercept` call deep-clones the account's IAM state —
//! O(IAM state size) per request while enforcement is on. Acceptable for
//! an in-memory emulator; revisit if enforcement stays on under load.
//!
//! Runs on the async worker BEFORE dispatch (called from
//! `catch_all_handler` after chaos injection): when off it is two string
//! compares and one uncontended mutex — no behavior change, no locks
//! held past the call.

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use iam::policy::{self, ContextValues, Decision};

/// Everything `intercept` needs about the in-flight request. Bundled so
/// the call site stays one expression as the port grows.
pub struct RequestContext<'a> {
    pub service: &'a str,
    pub operation: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    /// Lowercased header names, matching the gateway's ParsedRequest map.
    pub headers: &'a HashMap<String, String>,
    pub account: u64,
    pub region: &'a str,
}

/// service-name → IAM action prefix (python `_SERVICE_ACTION_PREFIX`).
fn action_prefix(service: &str) -> &str {
    match service {
        "dynamodbstreams" => "dynamodb",
        "monitoring" => "cloudwatch",
        "stepfunctions" => "states",
        "es" | "opensearch" => "es",
        _ => service,
    }
}

fn build_action(service: &str, operation: &str) -> String {
    if operation.is_empty() {
        format!("{}:*", action_prefix(service))
    } else {
        format!("{}:{}", action_prefix(service), operation)
    }
}

/// S3 operation name from method + path + query for REST enforcement
/// (python `_detect_rest_operation` in `gateway/handlers.py`). The
/// python twin enforced op-level `s3:PutObject`-style actions; without
/// this an `Allow s3:*` + `Deny s3:DeleteBucket` policy would silently
/// bypass the deny. Non-S3 REST services degrade to None (python parity).
fn detect_s3_operation(method: &str, path: &str, query: &str) -> Option<&'static str> {
    let trimmed = path.trim_matches('/');
    let mut parts = trimmed.splitn(2, '/');
    let bucket = parts.next().filter(|b| !b.is_empty());
    let key = parts.next().filter(|k| !k.is_empty());

    let query_param = |name: &str| -> bool {
        query.split('&').any(|kv| {
            let (k, _) = kv.split_once('=').unwrap_or((kv, ""));
            k == name
        })
    };
    let query_param_value = |name: &str| -> Option<&str> {
        query.split('&').find_map(|kv| {
            kv.split_once('=')
                .filter(|(k, _)| *k == name)
                .map(|(_, v)| v)
        })
    };

    // Sub-resource query params map to specific operations (python's
    // s3_query_ops table; the first matching param wins, iteration is
    // insertion-ordered there — here in the table's written order).
    const SUB_RESOURCES: &[(&str, &str, &str, &str)] = &[
        ("acl", "GetBucketAcl", "PutBucketAcl", ""),
        (
            "versioning",
            "GetBucketVersioning",
            "PutBucketVersioning",
            "",
        ),
        (
            "tagging",
            "GetBucketTagging",
            "PutBucketTagging",
            "DeleteBucketTagging",
        ),
        (
            "lifecycle",
            "GetBucketLifecycleConfiguration",
            "PutBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
        ),
        ("cors", "GetBucketCors", "PutBucketCors", "DeleteBucketCors"),
        (
            "policy",
            "GetBucketPolicy",
            "PutBucketPolicy",
            "DeleteBucketPolicy",
        ),
        (
            "encryption",
            "GetBucketEncryption",
            "PutBucketEncryption",
            "DeleteBucketEncryption",
        ),
        ("location", "GetBucketLocation", "", ""),
        ("logging", "GetBucketLogging", "PutBucketLogging", ""),
        (
            "notification",
            "GetBucketNotificationConfiguration",
            "PutBucketNotificationConfiguration",
            "",
        ),
        (
            "replication",
            "GetBucketReplication",
            "PutBucketReplication",
            "DeleteBucketReplication",
        ),
        (
            "website",
            "GetBucketWebsite",
            "PutBucketWebsite",
            "DeleteBucketWebsite",
        ),
        ("uploads", "ListMultipartUploads", "", ""),
    ];
    for (param, get_op, put_op, delete_op) in SUB_RESOURCES {
        if query_param(param) {
            return match method {
                "GET" if !get_op.is_empty() => Some(get_op),
                "PUT" if !put_op.is_empty() => Some(put_op),
                "DELETE" if !delete_op.is_empty() => Some(delete_op),
                _ => None,
            };
        }
    }
    // POST ?delete is the batch-delete sub-resource (method-specific).
    if query_param("delete") && method == "POST" {
        return Some("DeleteObjects");
    }

    let Some(_bucket) = bucket else {
        // Root path — ListBuckets on GET; anything else undetectable.
        return (method == "GET").then_some("ListBuckets");
    };

    if key.is_some() {
        // Object-level operations.
        return match method {
            "PUT" | "POST" => Some("PutObject"),
            "GET" => Some("GetObject"),
            "DELETE" => Some("DeleteObject"),
            "HEAD" => Some("HeadObject"),
            _ => None,
        };
    }

    // Bucket-level operations (no key).
    match method {
        "GET" => {
            if query_param_value("list-type") == Some("2") {
                Some("ListObjectsV2")
            } else {
                Some("ListObjects")
            }
        }
        "PUT" => Some("CreateBucket"),
        "DELETE" => Some("DeleteBucket"),
        "HEAD" => Some("HeadBucket"),
        _ => None,
    }
}

/// Extract the access key id from the SigV4 Authorization header or a
/// presigned URL's `X-Amz-Credential` query parameter (python
/// `extract_credentials`). Both must carry the full SigV4 shape —
/// `<key>/YYYYMMDD/region/service/aws4_request` — and anything malformed
/// is treated as absent (python's regex simply doesn't match). Returns
/// None when neither is present or neither is well-formed.
fn extract_access_key(headers: &HashMap<String, String>, query: &str) -> Option<String> {
    fn well_formed(key: &str, rest: &str) -> bool {
        // <key>/YYYYMMDD/region/service/aws4_request
        let parts: Vec<&str> = rest.split('/').collect();
        parts.len() == 4
            && parts[0].len() == 8
            && parts[0].chars().all(|c| c.is_ascii_digit())
            && !parts[1].is_empty()
            && !parts[2].is_empty()
            && parts[3] == "aws4_request"
            && key.chars().all(|c| c.is_ascii_alphanumeric())
            && !key.is_empty()
    }
    if let Some(auth) = headers.get("authorization") {
        // Python: first WELL-FORMED `Credential=` match anywhere in the
        // header (regex search). Scan every occurrence, not just the first.
        for cred in auth.split("Credential=").skip(1) {
            let cred = cred.trim_start_matches([',', ' ']);
            // The credential chunk runs to the next separator; its first
            // slash separates the key from date/region/service/terminator.
            let token = cred.split([',', ' ']).next().unwrap_or_default();
            let mut it = token.splitn(2, '/');
            let key = it.next().unwrap_or_default();
            let rest = it.next().unwrap_or_default();
            if well_formed(key, rest) {
                return Some(key.to_string());
            }
        }
    }
    let credential = query.split('&').find_map(|kv| {
        kv.split_once('=')
            .filter(|(k, _)| *k == "X-Amz-Credential")
            .map(|(_, v)| v)
    })?;
    let decoded = urlencoding::decode(credential).unwrap_or_default();
    let mut it = decoded.splitn(2, '/');
    let key = it.next().unwrap_or_default();
    let rest = it.next().unwrap_or_default();
    well_formed(key, rest).then(|| key.to_string())
}

/// Best-effort resource ARN for the target (python `build_resource_arn`).
fn build_resource_arn(
    service: &str,
    region: &str,
    account: u64,
    path: &str,
    query: &str,
) -> String {
    let query_val = |key: &str| -> Option<String> {
        query.split('&').find_map(|kv| {
            kv.split_once('=')
                .filter(|(k, _)| *k == key)
                .map(|(_, v)| urlencoding::decode(v).unwrap_or_default().into_owned())
        })
    };
    match service {
        "s3" => {
            // Path-style: /bucket/key. Virtual-hosted buckets arrive on the
            // Host header and are not resolved here (python parity).
            let trimmed = path.trim_matches('/');
            let mut parts = trimmed.splitn(2, '/');
            let bucket = parts.next().unwrap_or_default();
            let key = parts.next().unwrap_or_default();
            if bucket.is_empty() {
                "arn:aws:s3:::*".to_string()
            } else if key.is_empty() {
                format!("arn:aws:s3:::{bucket}")
            } else {
                format!("arn:aws:s3:::{bucket}/{key}")
            }
        }
        "sqs" => {
            if let Some(queue_url) = query_val("QueueUrl") {
                // mirror extract_queue_name_from_url (#517) LITERALLY — its
                // scheme branch requires a non-empty last segment and falls
                // through to the whole-string default otherwise. The ARNs
                // here are instrumentation (the engine answers
                // NonExistentQueue for names that can't exist); the
                // difference from moto's own _get_queue_name (empty ""
                // segment from a raw split) is engine-vs-lookup-OUTCOME,
                // recorded on the audit rows.
                if queue_url.starts_with("http://") || queue_url.starts_with("https://") {
                    if let Some(last) = queue_url.rsplit('/').next() {
                        if !last.is_empty() {
                            return format!("arn:aws:sqs:{region}:{account}:{last}");
                        }
                    }
                }
                return format!("arn:aws:sqs:{region}:{account}:{queue_url}");
            }
            let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
            if parts.len() >= 2 {
                format!("arn:aws:sqs:{region}:{account}:{}", parts[parts.len() - 1])
            } else {
                format!("arn:aws:sqs:{region}:{account}:*")
            }
        }
        "sns" => {
            if let Some(topic_arn) = query_val("TopicArn") {
                return topic_arn;
            }
            format!("arn:aws:sns:{region}:{account}:*")
        }
        "lambda" => {
            // /2015-03-31/functions/<name>/...
            match path.split("/functions/").nth(1) {
                Some(rest) => {
                    let name = rest.trim_start_matches('/').split('/').next().unwrap_or("");
                    format!("arn:aws:lambda:{region}:{account}:function:{name}")
                }
                None => format!("arn:aws:lambda:{region}:{account}:function:*"),
            }
        }
        "dynamodb" => format!("arn:aws:dynamodb:{region}:{account}:table/*"),
        other => format!("arn:aws:{other}:{region}:{account}:*"),
    }
}

fn xml_escape(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// 403 AccessDenied in the service's wire protocol (python
/// `_build_access_denied_response`, message text included — it always
/// cites the identity-policy deny, even on an implicit deny).
fn access_denied_response(service: &str, action: &str) -> Response {
    let message = format!(
        "User is not authorized to perform: {action} \
         with an explicit deny in an identity-based policy"
    );
    let protocol = crate::core::service_registry::SERVICE_REGISTRY
        .iter()
        .find(|(name, _, _, _)| *name == service)
        .map(|(_, _, protocol, _)| *protocol)
        .unwrap_or("");
    if protocol == "json" || protocol == "rest-json" {
        let body = serde_json::json!({
            "__type": "AccessDeniedException",
            "Message": message,
        })
        .to_string();
        (
            StatusCode::FORBIDDEN,
            [("content-type", "application/x-amz-json-1.1")],
            body,
        )
            .into_response()
    } else {
        let body = format!(
            "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>\
             <Message>{}</Message></Error></ErrorResponse>",
            xml_escape(&message)
        );
        (
            StatusCode::FORBIDDEN,
            [("content-type", "application/xml")],
            body,
        )
            .into_response()
    }
}

/// Evaluate the request; `Some(response)` replaces the dispatch (deny),
/// `None` lets it proceed. Called after chaos injection, before dispatch.
pub fn intercept(iam: &iam::DefaultIamHandler, ctx: &RequestContext<'_>) -> Option<Response> {
    // Bootstrap deadlock guard: credential creation is IAM, issuance is STS.
    if ctx.service == "iam" || ctx.service == "sts" {
        return None;
    }
    if crate::core::runtime_config::get("ENFORCE_IAM").as_deref() != Some("1") {
        return None;
    }
    // REST services resolve their operation here (python parity); named
    // services already carry one from extract_operation.
    let operation = if ctx.operation.is_empty() && ctx.service == "s3" {
        detect_s3_operation(ctx.method, ctx.path, ctx.query).unwrap_or_default()
    } else {
        ctx.operation
    };
    let Some(access_key) = extract_access_key(ctx.headers, ctx.query) else {
        // No (well-formed) credentials — python allows and records only.
        return None;
    };

    let action = build_action(ctx.service, operation);
    let resource = build_resource_arn(ctx.service, ctx.region, ctx.account, ctx.path, ctx.query);

    // No principal / no policy docs → implicit deny (python parity).
    let docs = iam
        .policy_docs_for_access_key(ctx.account, &access_key)
        .unwrap_or_default();
    let parsed: Vec<(String, serde_json::Value)> = docs
        .iter()
        .filter_map(|(id, doc)| {
            serde_json::from_str::<serde_json::Value>(doc)
                .ok()
                .map(|v| (id.clone(), v))
        })
        .collect();

    let allowed = if parsed.is_empty() {
        false
    } else {
        // python set aws:username to the access key id; SourceIp/CurrentTime
        // default the same way (loopback, epoch — conditions referencing
        // them match the same values the python middleware offered).
        let mut context = ContextValues::new();
        context.insert("aws:username".into(), vec![access_key.clone()]);
        context.insert("aws:SourceIp".into(), vec!["127.0.0.1".into()]);
        context.insert(
            "aws:CurrentTime".into(),
            vec!["2024-01-01T00:00:00Z".into()],
        );
        policy::evaluate_policy_docs(&parsed, &action, &resource, &context).decision
            == Decision::Allowed
    };

    (!allowed).then(|| access_denied_response(ctx.service, &action))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdrs(auth: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        if !auth.is_empty() {
            m.insert("authorization".to_string(), auth.to_string());
        }
        m
    }

    #[test]
    fn extracts_credential_from_sigv4_header() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20241201/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc";
        let key = extract_access_key(&hdrs(auth), "").unwrap();
        assert_eq!(key, "AKIAIOSFODNN7EXAMPLE");
    }

    #[test]
    fn malformed_credentials_are_treated_as_absent() {
        // Wrong credential date shape / missing aws4_request terminator:
        // the python regex doesn't match these and neither do we.
        for auth in [
            "AWS4-HMAC-SHA256 Credential=AKIA123/xx/us-east-1/s3/aws4_request",
            "AWS4-HMAC-SHA256 Credential=AKIA123/20241201/us-east-1/s3/",
        ] {
            assert!(extract_access_key(&hdrs(auth), "").is_none(), "{auth}");
        }
    }

    #[test]
    fn extracts_credential_from_presigned_query() {
        let q =
            "X-Amz-Credential=AKIA123%2F20241201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=x";
        let key = extract_access_key(&HashMap::new(), q).unwrap();
        assert_eq!(key, "AKIA123");
        // Not well-formed (4 slash parts, not 5): absent.
        let q = "X-Amz-Credential=AKIA123%2F20241201%2Fus-east-1%2Fs3";
        assert!(extract_access_key(&HashMap::new(), q).is_none());
    }

    #[test]
    fn action_uses_prefix_map_and_operation() {
        assert_eq!(
            build_action("dynamodbstreams", "ListStreams"),
            "dynamodb:ListStreams"
        );
        assert_eq!(build_action("monitoring", ""), "cloudwatch:*");
        assert_eq!(build_action("s3", "PutObject"), "s3:PutObject");
        assert_eq!(build_action("s3", ""), "s3:*");
    }

    #[test]
    fn s3_operation_detection_matches_the_python_table() {
        assert_eq!(
            detect_s3_operation("PUT", "/my-bucket/key.txt", ""),
            Some("PutObject")
        );
        assert_eq!(
            detect_s3_operation("DELETE", "/my-bucket", ""),
            Some("DeleteBucket")
        );
        assert_eq!(
            detect_s3_operation("HEAD", "/my-bucket", ""),
            Some("HeadBucket")
        );
        assert_eq!(
            detect_s3_operation("GET", "/my-bucket", "list-type=2"),
            Some("ListObjectsV2")
        );
        assert_eq!(detect_s3_operation("GET", "/", ""), Some("ListBuckets"));
        assert_eq!(
            detect_s3_operation("GET", "/my-bucket", "policy="),
            Some("GetBucketPolicy")
        );
        assert_eq!(
            detect_s3_operation("DELETE", "/my-bucket", "policy="),
            Some("DeleteBucketPolicy")
        );
        assert_eq!(
            detect_s3_operation("POST", "/my-bucket", "delete="),
            Some("DeleteObjects")
        );
        // The acl sub-resource resolves to *Bucket*Acl even on object
        // paths — python's table checks sub-resources before the key.
        assert_eq!(
            detect_s3_operation("GET", "/my-bucket/key.txt", "acl="),
            Some("GetBucketAcl")
        );
        assert_eq!(
            detect_s3_operation("PUT", "/my-bucket/key.txt", ""),
            Some("PutObject")
        );
    }

    #[test]
    fn resource_arn_shapes_match_the_python_middleware() {
        assert_eq!(
            build_resource_arn("s3", "us-east-1", 123456789012, "/my-bucket/key.txt", ""),
            "arn:aws:s3:::my-bucket/key.txt"
        );
        assert_eq!(
            build_resource_arn(
                "sqs",
                "us-east-1",
                123456789012,
                "/",
                "QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F123456789012%2Fmy-queue"
            ),
            "arn:aws:sqs:us-east-1:123456789012:my-queue"
        );
        // Mirrored LITERALLY from extract_queue_name_from_url (#517):
        // bare names verbatim; slash-shaped bare urls name the WHOLE
        // string (the engine's NonExistentQueue shape keeps policy ARNs
        // honest); a trailing-slash http url falls through to the
        // whole-string default — the engine's name there is the full url
        // (its moto empty-segment split happens in _get_queue_name, whose
        // outcome the engine mirrors through the missing-queue answer).
        assert_eq!(
            build_resource_arn("sqs", "us-east-1", 123456789012, "/", "QueueUrl=bare-name"),
            "arn:aws:sqs:us-east-1:123456789012:bare-name"
        );
        assert_eq!(
            build_resource_arn(
                "sqs",
                "us-east-1",
                123456789012,
                "/",
                "QueueUrl=AccountX%2Fmy-queue"
            ),
            "arn:aws:sqs:us-east-1:123456789012:AccountX/my-queue"
        );
        assert_eq!(
            build_resource_arn(
                "sqs",
                "us-east-1",
                123456789012,
                "/",
                "QueueUrl=http%3A%2F%2Flocalhost%3A4566%2F123456789012%2Fmy-queue%2F"
            ),
            "arn:aws:sqs:us-east-1:123456789012:http://localhost:4566/123456789012/my-queue/"
        );
        assert_eq!(
            build_resource_arn("dynamodb", "us-east-1", 123456789012, "/", ""),
            "arn:aws:dynamodb:us-east-1:123456789012:table/*"
        );
        assert_eq!(
            build_resource_arn("s3", "us-east-1", 123456789012, "/", ""),
            "arn:aws:s3:::*"
        );
    }

    #[test]
    fn json_services_get_the_json_access_denied_shape() {
        let resp = access_denied_response("dynamodb", "dynamodb:DeleteTable");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/x-amz-json-1.1"
        );
    }

    #[test]
    fn xml_services_get_the_xml_access_denied_shape() {
        let resp = access_denied_response("s3", "s3:*");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/xml"
        );
    }
}
