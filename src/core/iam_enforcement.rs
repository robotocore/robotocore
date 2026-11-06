//! ENFORCE_IAM request enforcement — port of the python gateway's
//! `gateway/iam_middleware.py` (deleted with the python product).
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
//! Scope (mirrors the python middleware): principals are IAM users found
//! by access key. Assumed-role session credentials (the STS session
//! registry the python middleware kept) are not re-registered here yet —
//! the compat suite exercises user-key enforcement.

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use iam::policy::{self, ContextValues, Decision};

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
        // REST services (S3) resolve to the service wildcard — python
        // parity: their ops are method+path shaped, not named.
        format!("{}:*", action_prefix(service))
    } else {
        format!("{}:{}", action_prefix(service), operation)
    }
}

/// Extract the access key id from the SigV4 Authorization header or a
/// presigned URL's `X-Amz-Credential` query parameter (python
/// `extract_credentials`). Returns None when neither is present.
fn extract_access_key(headers: &HashMap<String, String>, query: &str) -> Option<String> {
    if let Some(auth) = headers.get("authorization") {
        // Credential=<key>/YYYYMMDD/region/service/aws4_request
        if let Some(cred) = auth.split("Credential=").nth(1) {
            let cred = cred.split(',').next().unwrap_or(cred);
            let key = cred.split('/').next().unwrap_or_default();
            if !key.is_empty() {
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
    let key = decoded.split('/').next().unwrap_or_default();
    (!key.is_empty()).then(|| key.to_string())
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
            if key.is_empty() {
                format!("arn:aws:s3:::{bucket}")
            } else {
                format!("arn:aws:s3:::{bucket}/{key}")
            }
        }
        "sqs" => {
            if let Some(queue_url) = query_val("QueueUrl") {
                let queue_name = queue_url
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("");
                return format!("arn:aws:sqs:{region}:{account}:{queue_name}");
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
/// `_build_access_denied_response`): JSON services get
/// `AccessDeniedException`, everything else the XML ErrorResponse shape.
fn access_denied_response(service: &str, action: &str) -> Response {
    let message = format!("User is not authorized to perform: {action}");
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
        (StatusCode::FORBIDDEN, [("content-type", "text/xml")], body).into_response()
    }
}

/// Evaluate the request; `Some(response)` replaces the dispatch (deny),
/// `None` lets it proceed. Called after chaos injection, before dispatch.
pub fn intercept(
    iam: &iam::DefaultIamHandler,
    service: &str,
    operation: &str,
    path: &str,
    query: &str,
    headers: &HashMap<String, String>,
    account: u64,
    region: &str,
) -> Option<Response> {
    // Bootstrap deadlock guard: credential creation is IAM, issuance is STS.
    if service == "iam" || service == "sts" {
        return None;
    }
    if crate::core::runtime_config::get("ENFORCE_IAM").as_deref() != Some("1") {
        return None;
    }
    let Some(access_key) = extract_access_key(headers, query) else {
        // No credentials — the python middleware allows (records only).
        return None;
    };

    let action = build_action(service, operation);
    let resource = build_resource_arn(service, region, account, path, query);

    // No principal / no policy docs → implicit deny (python parity).
    let docs = iam
        .policy_docs_for_access_key(account, &access_key)
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

    (!allowed).then(|| access_denied_response(service, &action))
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
    fn extracts_credential_from_presigned_query() {
        let q =
            "X-Amz-Credential=AKIA123%2F20241201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=x";
        let key = extract_access_key(&HashMap::new(), q).unwrap();
        assert_eq!(key, "AKIA123");
    }

    #[test]
    fn action_uses_prefix_map_and_operation() {
        assert_eq!(
            build_action("dynamodbstreams", "ListStreams"),
            "dynamodb:ListStreams"
        );
        assert_eq!(build_action("monitoring", ""), "cloudwatch:*");
        assert_eq!(build_action("s3", ""), "s3:*");
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
        assert_eq!(
            build_resource_arn("dynamodb", "us-east-1", 123456789012, "/", ""),
            "arn:aws:dynamodb:us-east-1:123456789012:table/*"
        );
    }

    #[test]
    fn json_services_get_the_json_access_denied_shape() {
        let resp = access_denied_response("dynamodb", "dynamodb:DeleteTable");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn xml_services_get_the_xml_access_denied_shape() {
        let resp = access_denied_response("s3", "s3:*");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
}
