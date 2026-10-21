//! Resource mirror: when a stack deploys, its resources are created through
//! the robotocore server's own native service APIs over loopback HTTP — the
//! same wire handlers real AWS SDKs hit. This is the native equivalent of
//! what the Python moto sidecar did (resolve intrinsics, then mutate the
//! sibling providers' stores).
//!
//! Operation shapes verified against the running server:
//! - query-XML services (sqs/sns/iam): form POST to `/` with `Action=&Version=`
//!   and a credential scope naming the service.
//! - JSON services (dynamodb/ssm/events/kms/logs/secretsmanager/kinesis/
//!   stepfunctions/ecs): POST with an `X-Amz-Target` header and a JSON body.
//! - lambda: POST `/2015-03-31/functions` with `X-Amz-Target`.
//! - s3: PUT/DELETE directly on `/{bucket}`.

use serde_json::{json, Value};
use std::sync::OnceLock;
use uuid::Uuid;

/// Short uuid suffix for synthetic physical ids (matches ops::uuid_head).
fn uuid_head() -> String {
    String::from(&Uuid::new_v4().to_string()[..8])
}

#[derive(Debug)]
pub struct MirrorCall {
    pub status: u16,
    pub body: String,
}

impl MirrorCall {
    /// Anything but a transport failure; HTTP-level errors surface in body.
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status) && !(self.status == 0)
    }
}

fn strip_slashes(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

/// Base URL of the robotocore server used for the mirror.
pub fn callback_base() -> String {
    let base = std::env::var("ROBOTOCORE_CALLBACK_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            // Match the Python sidecar's fallback: RUST_PORT (the parity
            // lane), otherwise the default single-server endpoint.
            match std::env::var("RUST_PORT") {
                Ok(port) if !port.is_empty() => format!("http://127.0.0.1:{port}"),
                _ => "http://127.0.0.1:4566".to_string(),
            }
        });
    strip_slashes(base)
}

fn client() -> ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT
        .get_or_init(|| {
            ureq::AgentBuilder::new()
                .timeout_connect(std::time::Duration::from_secs(5))
                .timeout_read(std::time::Duration::from_secs(20))
                .build()
        })
        .clone()
}

fn base_headers(account: &str, region: &str, service: &str) -> Vec<(String, String)> {
    vec![
        (
            "Authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={account}/20260101/{region}/{service}/aws4_request"
            ),
        ),
        ("X-Amz-Date".to_string(), "20260101T000000Z".to_string()),
        ("X-Robotocore-Account".to_string(), account.to_string()),
        ("X-Robotocore-Region".to_string(), region.to_string()),
        // Loopback calls are one-shot; keep-alive pooling wedges axum's
        // blocking-thread interplay here, so close each connection.
        ("Connection".to_string(), "close".to_string()),
    ]
}

fn with_headers(mut req: ureq::Request, headers: &[(String, String)]) -> ureq::Request {
    for (k, v) in headers {
        req = req.set(k, v);
    }
    req
}

fn send(req: ureq::Request, body: &[u8]) -> MirrorCall {
    match req.send_bytes(body) {
        Ok(resp) => MirrorCall {
            status: resp.status(),
            body: resp.into_string().unwrap_or_default(),
        },
        Err(ureq::Error::Status(code, resp)) => MirrorCall {
            status: code,
            body: resp.into_string().unwrap_or_default(),
        },
        Err(e) => MirrorCall {
            status: 0,
            body: format!("mirror call failed: {e}"),
        },
    }
}

fn post(path: &str, headers: Vec<(String, String)>, body: Vec<u8>) -> MirrorCall {
    let url = format!("{}{}", callback_base(), path);
    let req = with_headers(client().post(&url), &headers);
    send(req, &body)
}

fn urlenc(pairs: &[(&str, String)]) -> Vec<u8> {
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(&urlencoding::encode(k));
        out.push('=');
        out.push_str(&urlencoding::encode(v));
    }
    out.into_bytes()
}

/// POST a form-encoded request to a query-protocol service.
pub fn form_call(
    service: &str,
    action: &str,
    version: &str,
    params: &[(&str, String)],
    account: &str,
    region: &str,
) -> MirrorCall {
    let mut pairs: Vec<(&str, String)> = vec![
        ("Action", action.to_string()),
        ("Version", version.to_string()),
    ];
    pairs.extend_from_slice(params);
    let mut headers = base_headers(account, region, service);
    headers.push((
        "Content-Type".to_string(),
        "application/x-www-form-urlencoded; charset=utf-8".to_string(),
    ));
    post("/", headers, urlenc(&pairs))
}

/// POST a JSON-protocol request with an `X-Amz-Target`.
pub fn json_call(
    target: &str,
    service: &str,
    path: &str,
    body: Value,
    account: &str,
    region: &str,
) -> MirrorCall {
    let mut headers = base_headers(account, region, service);
    headers.push(("X-Amz-Target".to_string(), target.to_string()));
    headers.push((
        "Content-Type".to_string(),
        "application/x-amz-json-1.0".to_string(),
    ));
    post(path, headers, body.to_string().into_bytes())
}

// S3-style direct-REST call (`PUT /bucket`, `DELETE /bucket`).
pub fn s3_call(method: &str, bucket: &str, account: &str, region: &str) -> MirrorCall {
    let url = format!("{}/{}", callback_base(), bucket);
    let headers = base_headers(account, region, "s3");
    let req = match method {
        "DELETE" => client().delete(&url),
        _ => client().put(&url),
    };
    let req = with_headers(req, &headers);
    send(req, b"")
}

// ---------------------------------------------------------------------------
// Per-resource-type create/delete mirroring. Each returns the physical id
// and attributes the Python provider sets; failed mirrors log and the
// resource stays absent (stack flows on, like the sidecar's best-effort).
// ---------------------------------------------------------------------------

pub fn create_sqs_queue(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    // AWS::SQS::Queue properties ride in as queue attributes (moto forwards
    // every property through to `sqs_backend.create_queue`). SQS is a JSON
    // protocol service on the wire today, and the form-encoded legacy path
    // drops Attribute.N.Name/Value pairs — so post JSON with X-Amz-Target.
    let mut body = json!({ "QueueName": name });
    let attrs = sqs_queue_attributes(props);
    if attrs.as_object().is_some_and(|m| !m.is_empty()) {
        body.as_object_mut()
            .unwrap()
            .insert("Attributes".to_string(), attrs);
    }
    if let Some(tags) = props
        .get("Tags")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
    {
        // CFN Tags [{Key, Value}] → SQS tags {name: value}.
        let mut tag_map = serde_json::Map::new();
        for t in tags {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                tag_map.insert(k.to_string(), json!(v));
            }
        }
        body.as_object_mut()
            .unwrap()
            .insert("tags".to_string(), Value::Object(tag_map));
    }
    let resp = json_call("AmazonSQS.CreateQueue", "sqs", "/", body, account, region);
    if !resp.ok() {
        return None;
    }
    let url = if let Ok(v) = serde_json::from_str::<Value>(&resp.body) {
        v.get("QueueUrl").and_then(Value::as_str).map(String::from)
    } else {
        text(&resp.body, "QueueUrl")
    };
    let url = url?;
    let attrs = json!({
        "Arn": format!("arn:aws:sqs:{region}:{account}:{name}"),
        "QueueName": name,
        "QueueUrl": url,
    });
    Some((url, attrs))
}

/// Queue attributes derivable from AWS::SQS::Queue properties, stringified
/// the way SQS serializes them (moto's `Queue._set_attributes` names).
fn sqs_queue_attributes(props: &Value) -> Value {
    let mut out = serde_json::Map::new();
    let Some(obj) = props.as_object() else {
        return Value::Object(out);
    };
    for (key, v) in obj {
        if v.is_null() {
            continue;
        }
        let scalar_names = [
            "VisibilityTimeout",
            "DelaySeconds",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "ReceiveMessageWaitTimeSeconds",
            "KmsDataKeyReusePeriodSeconds",
            "KmsMasterKeyId",
            "DeduplicationScope",
            "FifoThroughputLimit",
            "RedriveAllowPolicy",
        ];
        let bool_names = [
            "FifoQueue",
            "ContentBasedDeduplication",
            "SqsManagedSseEnabled",
        ];
        let policy_names = ["Policy", "RedrivePolicy"];
        let rendered = if scalar_names.contains(&key.as_str()) {
            match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        } else if bool_names.contains(&key.as_str()) {
            match v {
                Value::Bool(b) => {
                    if *b {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }
                }
                other => other.to_string(),
            }
        } else if policy_names.contains(&key.as_str()) {
            policy_json(v)
        } else {
            continue;
        };
        out.insert(key.clone(), Value::String(rendered));
    }
    Value::Object(out)
}

fn text(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = body.find(&open)? + open.len();
    let close = format!("</{tag}>");
    let rel = body[start..].find(&close)?;
    Some(body[start..start + rel].to_string())
}

pub fn delete_sqs_queue(name: &str, account: &str, region: &str) {
    let get = form_call(
        "sqs",
        "GetQueueUrl",
        "2012-11-05",
        &[("QueueName", name.to_string())],
        account,
        region,
    );
    let url = if let Ok(v) = serde_json::from_str::<Value>(&get.body) {
        v.get("QueueUrl").and_then(Value::as_str).map(String::from)
    } else {
        text(&get.body, "QueueUrl")
    };
    if let Some(url) = url {
        form_call(
            "sqs",
            "DeleteQueue",
            "2012-11-05",
            &[("QueueUrl", url)],
            account,
            region,
        );
    }
}

pub fn create_sns_topic(name: &str, account: &str, region: &str) -> Option<(String, Value)> {
    let resp = form_call(
        "sns",
        "CreateTopic",
        "2010-03-31",
        &[("Name", name.to_string())],
        account,
        region,
    );
    let arn = text(&resp.body, "TopicArn")?;
    Some((arn.clone(), json!({"TopicArn": arn, "TopicName": name})))
}

pub fn delete_sns_topic(arn: &str, account: &str, region: &str) {
    let _ = form_call(
        "sns",
        "DeleteTopic",
        "2010-03-31",
        &[("TopicArn", arn.to_string())],
        account,
        region,
    );
}

pub fn subscribe_sns(
    topic_arn: &str,
    protocol: &str,
    endpoint: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let resp = form_call(
        "sns",
        "Subscribe",
        "2010-03-31",
        &[
            ("TopicArn", topic_arn.to_string()),
            ("Protocol", protocol.to_string()),
            ("Endpoint", endpoint.to_string()),
        ],
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let sub_arn = text(&resp.body, "SubscriptionArn").unwrap_or_default();
    Some((sub_arn.clone(), json!({"Arn": sub_arn})))
}

pub fn unsubscribe_sns(sub_arn: &str, account: &str, region: &str) {
    let _ = form_call(
        "sns",
        "Unsubscribe",
        "2010-03-31",
        &[("SubscriptionArn", sub_arn.to_string())],
        account,
        region,
    );
}

/// IAM policy docs flow as strings; render sub-objects compactly.
fn policy_json(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "{}".to_string()),
    }
}

pub fn create_dynamodb_table(props: &Value, name: &str, account: &str, region: &str) -> bool {
    let mut body = json!({ "TableName": name });
    if let Some(obj) = props.as_object() {
        for key in [
            "KeySchema",
            "AttributeDefinitions",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
            "BillingMode",
            "ProvisionedThroughput",
            "TableClass",
            "DeletionProtectionEnabled",
            "SSESpecification",
        ] {
            if let Some(v) = obj.get(key).filter(|v| !v.is_null()) {
                body.as_object_mut()
                    .unwrap()
                    .insert(key.to_string(), v.clone());
            }
        }
        if let Some(view) = obj
            .get("StreamSpecification")
            .and_then(|s| s.get("StreamViewType"))
            .and_then(Value::as_str)
        {
            body.as_object_mut().unwrap().insert(
                "StreamSpecification".to_string(),
                json!({"StreamEnabled": true, "StreamViewType": view}),
            );
        }
    }
    if body.get("BillingMode").is_none() && body.get("ProvisionedThroughput").is_none() {
        body.as_object_mut().unwrap().insert(
            "ProvisionedThroughput".to_string(),
            json!({"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}),
        );
    }
    json_call(
        "DynamoDB_20120810.CreateTable",
        "dynamodb",
        "/",
        body,
        account,
        region,
    )
    .ok()
}

pub fn delete_dynamodb_table(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "DynamoDB_20120810.DeleteTable",
        "dynamodb",
        "/",
        json!({"TableName": name}),
        account,
        region,
    );
}

pub fn create_iam_role(
    name: &str,
    assume_policy: &Value,
    account: &str,
) -> Option<(String, Value)> {
    let resp = form_call(
        "iam",
        "CreateRole",
        "2010-05-08",
        &[
            ("RoleName", name.to_string()),
            ("AssumeRolePolicyDocument", policy_json(assume_policy)),
        ],
        account,
        "global",
    );
    if !resp.ok() {
        return None;
    }
    let role_id = text(&resp.body, "RoleId").unwrap_or_default();
    let arn = text(&resp.body, "Arn").unwrap_or_default();
    let attrs = json!({"Arn": arn, "RoleId": role_id});
    Some((role_id, attrs))
}

pub fn delete_iam_role(name: &str, account: &str) {
    let _ = form_call(
        "iam",
        "DeleteRole",
        "2010-05-08",
        &[("RoleName", name.to_string())],
        account,
        "global",
    );
}

pub fn put_role_policy(role: &str, policy_name: &str, doc: &Value, account: &str) -> bool {
    form_call(
        "iam",
        "PutRolePolicy",
        "2010-05-08",
        &[
            ("RoleName", role.to_string()),
            ("PolicyName", policy_name.to_string()),
            ("PolicyDocument", policy_json(doc)),
        ],
        account,
        "global",
    )
    .ok()
}

pub fn create_managed_policy(name: &str, doc: &Value, account: &str) -> Option<(String, Value)> {
    let resp = form_call(
        "iam",
        "CreatePolicy",
        "2010-05-08",
        &[
            ("PolicyName", name.to_string()),
            ("PolicyDocument", policy_json(doc)),
        ],
        account,
        "global",
    );
    let arn = text(&resp.body, "Arn")?;
    Some((arn.clone(), json!({"Arn": arn})))
}

pub fn delete_managed_policy(arn: &str, account: &str) {
    let _ = form_call(
        "iam",
        "DeletePolicyByArn",
        "2010-05-08",
        &[("PolicyArn", arn.to_string())],
        account,
        "global",
    );
}

pub fn create_instance_profile(
    name: &str,
    roles: &[String],
    account: &str,
) -> Option<(String, Value)> {
    let mut params: Vec<(String, String)> =
        vec![("InstanceProfileName".to_string(), name.to_string())];
    for (i, r) in roles.iter().enumerate() {
        params.push((format!("Roles.member.{}", i + 1), r.clone()));
    }
    let params_ref: Vec<(&str, String)> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    let resp = form_call(
        "iam",
        "CreateInstanceProfile",
        "2010-05-08",
        &params_ref,
        account,
        "global",
    );
    let arn = text(&resp.body, "Arn")
        .unwrap_or_else(|| format!("arn:aws:iam::{account}:instance-profile/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn create_iam_user(name: &str, account: &str) -> Option<(String, Value)> {
    let resp = form_call(
        "iam",
        "CreateUser",
        "2010-05-08",
        &[("UserName", name.to_string())],
        account,
        "global",
    );
    let arn =
        text(&resp.body, "Arn").unwrap_or_else(|| format!("arn:aws:iam::{account}:user/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn create_iam_group(name: &str, account: &str) -> Option<(String, Value)> {
    let resp = form_call(
        "iam",
        "CreateGroup",
        "2010-05-08",
        &[("GroupName", name.to_string())],
        account,
        "global",
    );
    let arn =
        text(&resp.body, "Arn").unwrap_or_else(|| format!("arn:aws:iam::{account}:group/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn put_ssm_parameter(props: &Value, name: &str, account: &str, region: &str) -> bool {
    let value = match props.get("Value").unwrap_or(&Value::Null) {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let ptype = props
        .get("Type")
        .and_then(Value::as_str)
        .unwrap_or("String");
    let mut body = json!({"Name": name, "Value": value, "Type": ptype, "Overwrite": true});
    if ptype == "SecureString" {
        if let Some(kid) = props.get("KeyId").and_then(Value::as_str) {
            body.as_object_mut()
                .unwrap()
                .insert("KeyId".to_string(), Value::String(kid.to_string()));
        }
    }
    json_call("AmazonSSM.PutParameter", "ssm", "/", body, account, region).ok()
}

pub fn delete_ssm_parameter(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "AmazonSSM.DeleteParameter",
        "ssm",
        "/",
        json!({"Name": name}),
        account,
        region,
    );
}

pub fn create_s3_bucket(name: &str, account: &str, region: &str) -> bool {
    s3_call("PUT", name, account, region).ok()
}

/// Parse the CFN `WebsiteConfiguration` property into the S3 REST XML body.
/// Accepts the string short-form (`IndexDocument: index.html`) and the
/// object form (`IndexDocument: {Suffix: index.html}`).
fn website_configuration_xml(props: &Value) -> Option<String> {
    let cfg = props.get("WebsiteConfiguration").filter(|v| !v.is_null())?;
    let mut out =
        String::from("<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for (key, child) in [("IndexDocument", "Suffix"), ("ErrorDocument", "Key")] {
        match cfg.get(key) {
            Some(Value::String(s)) => {
                out.push_str(&format!(
                    "<{key}><{child}>{}</{child}></{key}>",
                    xml_escape(s)
                ));
            }
            Some(obj @ Value::Object(_)) => {
                if let Some(v) = obj.get(child).and_then(Value::as_str) {
                    out.push_str(&format!(
                        "<{key}><{child}>{}</{child}></{key}>",
                        xml_escape(v)
                    ));
                }
            }
            _ => {}
        }
    }
    out.push_str("</WebsiteConfiguration>");
    Some(out)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// `VersioningConfiguration` → S3 REST XML for ?versioning.
fn versioning_xml(props: &Value) -> Option<String> {
    let status = props
        .get("VersioningConfiguration")
        .and_then(|v| v.get("Status"))
        .and_then(Value::as_str)?;
    Some(format!(
        "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
    ))
}

/// Create the bucket, then apply website configuration and versioning that
/// ride in on AWS::S3::Bucket Properties (moto's Bucket::create cloudformation
/// path passes the whole property bag to its S3 backend).
pub fn create_s3_bucket_full(props: &Value, name: &str, account: &str, region: &str) -> bool {
    if !create_s3_bucket(name, account, region) {
        return false;
    }
    for (query, xml) in [
        ("website", website_configuration_xml(props)),
        ("versioning", versioning_xml(props)),
    ] {
        let Some(xml) = xml else {
            continue;
        };
        let headers = {
            let mut h = base_headers(account, region, "s3");
            h.push(("Content-Type".to_string(), "application/xml".to_string()));
            h
        };
        let req = with_headers(
            client().put(&format!("{}/{}?{query}", callback_base(), name)),
            &headers,
        );
        send(req, xml.as_bytes());
    }
    true
}

pub fn delete_s3_bucket_policy(name: &str, account: &str, region: &str) {
    let headers = base_headers(account, region, "s3");
    let req = with_headers(
        client().delete(&format!("{}/{}?policy", callback_base(), name)),
        &headers,
    );
    send(req, b"");
}

pub fn delete_s3_bucket(name: &str, account: &str, region: &str) {
    let _ = s3_call("DELETE", name, account, region);
}

pub fn put_s3_bucket_policy(name: &str, policy: &Value, account: &str, region: &str) -> bool {
    let body = json!({"policy": policy_json(policy)});
    let headers = {
        let mut h = base_headers(account, region, "s3");
        h.push(("Content-Type".to_string(), "application/json".to_string()));
        h
    };
    let req = with_headers(
        client().put(&format!("{}/{}?policy", callback_base(), name)),
        &headers,
    );
    send(req, body.to_string().as_bytes()).ok()
}

pub fn create_log_group(name: &str, account: &str, region: &str) -> bool {
    json_call(
        "Logs_20140328.CreateLogGroup",
        "logs",
        "/",
        json!({"logGroupName": name}),
        account,
        region,
    )
    .ok()
}

pub fn put_log_retention(name: &str, days: u64, account: &str, region: &str) {
    let _ = json_call(
        "Logs_20140328.PutRetentionPolicy",
        "logs",
        "/",
        json!({"logGroupName": name, "retentionInDays": days}),
        account,
        region,
    );
}

pub fn delete_log_group(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "Logs_20140328.DeleteLogGroup",
        "logs",
        "/",
        json!({"logGroupName": name}),
        account,
        region,
    );
}

pub fn put_events_rule(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let mut body = json!({"Name": name});
    if let Some(obj) = props.as_object() {
        for (src, key) in [
            ("EventPattern", "EventPattern"),
            ("ScheduleExpression", "ScheduleExpression"),
            ("Description", "Description"),
        ] {
            if let Some(v) = obj.get(src) {
                if !v.is_null() {
                    if key == "EventPattern" {
                        body.as_object_mut()
                            .unwrap()
                            .insert(key.to_string(), Value::String(policy_json(v)));
                    } else {
                        body.as_object_mut()
                            .unwrap()
                            .insert(key.to_string(), v.clone());
                    }
                }
            }
        }
        let state = obj
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or("ENABLED");
        body.as_object_mut()
            .unwrap()
            .insert("State".to_string(), Value::String(state.to_string()));
        if let Some(bus) = obj.get("EventBusName").and_then(Value::as_str) {
            if bus != "default" {
                body.as_object_mut()
                    .unwrap()
                    .insert("EventBusName".to_string(), Value::String(bus.to_string()));
            }
        }
    }
    let resp = json_call(
        "CloudWatchEvents.PutRule",
        "events",
        "/",
        body,
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let arn = serde_json::from_str::<Value>(&resp.body)
        .ok()
        .and_then(|v| v.get("RuleArn").and_then(Value::as_str).map(String::from))
        .unwrap_or_else(|| format!("arn:aws:events:{region}:{account}:rule/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn delete_events_rule(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "CloudWatchEvents.DeleteRule",
        "events",
        "/",
        json!({"Name": name}),
        account,
        region,
    );
}

pub fn create_kms_key(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let desc = props
        .get("Description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let key_usage = props
        .get("KeyUsage")
        .and_then(Value::as_str)
        .unwrap_or("ENCRYPT_DECRYPT");
    let key_spec = props
        .get("KeySpec")
        .and_then(Value::as_str)
        .unwrap_or("SYMMETRIC_DEFAULT");
    let mut body = json!({
        "Description": desc,
        "KeyUsage": key_usage,
        "KeySpec": key_spec,
        "Origin": "AWS_KMS",
    });
    if let Some(p) = props.get("KeyPolicy").filter(|p| !p.is_null()) {
        body.as_object_mut()
            .unwrap()
            .insert("Policy".to_string(), Value::String(policy_json(p)));
    }
    let resp = json_call("TrentService.CreateKey", "kms", "/", body, account, region);
    let md = serde_json::from_str::<Value>(&resp.body)
        .ok()?
        .get("KeyMetadata")?
        .clone();
    let key_id = md.get("KeyId").and_then(Value::as_str)?.to_string();
    let arn = md
        .get("Arn")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("arn:aws:kms:{region}:{account}:key/{key_id}"));
    let attrs = json!({"Arn": arn, "KeyId": key_id});
    Some((key_id, attrs))
}

pub fn delete_kms_key(key_id: &str, account: &str, region: &str) {
    let _ = json_call(
        "TrentService.ScheduleKeyDeletion",
        "kms",
        "/",
        json!({"KeyId": key_id, "PendingWindowInDays": 7}),
        account,
        region,
    );
}

pub fn create_kms_alias(name: &str, target: &str, account: &str, region: &str) -> bool {
    json_call(
        "TrentService.CreateAlias",
        "kms",
        "/",
        json!({"AliasName": name, "TargetKeyId": target}),
        account,
        region,
    )
    .ok()
}

pub fn delete_kms_alias(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "TrentService.DeleteAlias",
        "kms",
        "/",
        json!({"AliasName": name}),
        account,
        region,
    );
}

pub fn create_lambda_function(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let runtime = props
        .get("Runtime")
        .and_then(Value::as_str)
        .unwrap_or("python3.12");
    let handler = props
        .get("Handler")
        .and_then(Value::as_str)
        .unwrap_or("index.handler");
    let role = props
        .get("Role")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("arn:aws:iam::{account}:role/cfn-role"));
    let mut fn_body = json!({
        "FunctionName": name,
        "Runtime": runtime,
        "Handler": handler,
        "Role": role,
        "Description": props.get("Description").and_then(Value::as_str).unwrap_or(""),
        "Timeout": props.get("Timeout").and_then(Value::as_i64).unwrap_or(3),
        "MemorySize": props.get("MemorySize").and_then(Value::as_i64).unwrap_or(128),
    });
    if let Some(env) = props.get("Environment").filter(|v| !v.is_null()) {
        fn_body
            .as_object_mut()
            .unwrap()
            .insert("Environment".to_string(), env.clone());
    }
    if let Some(layers) = props.get("Layers") {
        fn_body
            .as_object_mut()
            .unwrap()
            .insert("Layers".to_string(), layers.clone());
    }
    let code = props.get("Code");
    let code_value = match code {
        Some(Value::Object(code_obj)) => {
            if let Some(zip) = code_obj.get("ZipFile") {
                json!({"ZipFile": zip_source(zip)})
            } else if let Some(bucket) = code_obj.get("S3Bucket").and_then(Value::as_str) {
                let mut c = json!({"S3Bucket": bucket});
                if let Some(key) = code_obj.get("S3Key").and_then(Value::as_str) {
                    c.as_object_mut()
                        .unwrap()
                        .insert("S3Key".to_string(), Value::String(key.to_string()));
                }
                if let Some(ver) = code_obj.get("S3ObjectVersion") {
                    c.as_object_mut()
                        .unwrap()
                        .insert("S3ObjectVersion".to_string(), ver.clone());
                }
                c
            } else {
                json!({"ZipFile": ""})
            }
        }
        _ => json!({"ZipFile": ""}),
    };
    fn_body
        .as_object_mut()
        .unwrap()
        .insert("Code".to_string(), code_value);
    let resp = json_call(
        "Lambda_20150331.CreateFunction",
        "lambda",
        "/2015-03-31/functions",
        fn_body,
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let body: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let arn = body
        .get("FunctionArn")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("arn:aws:lambda:{region}:{account}:function:{name}"));
    Some((name.to_string(), json!({"Arn": arn, "FunctionName": name})))
}

pub fn delete_lambda_function(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "Lambda_20150331.DeleteFunction",
        "lambda",
        "/2015-03-31/functions",
        json!({"FunctionName": name}),
        account,
        region,
    );
}

/// Inline source handling: if the value is already base64 of a zip (PK
/// header), keep it; otherwise wrap the plaintext source in a real zip
/// archive with the module the CFN `Handler` points at, exactly as moto's
/// `_create_zipfile_from_plaintext_code` does (moto/awslambda/models.py).
fn zip_source(zip: &Value) -> String {
    match zip {
        Value::String(s) => {
            if is_zip_b64(s) {
                s.clone()
            } else {
                b64_encode(&zip_from_plaintext(s.as_bytes()))
            }
        }
        other => other.to_string(),
    }
}

fn is_zip_b64(s: &str) -> bool {
    // base64("PK\x03\x04") starts with "UEsD"; treat any value that decodes
    // to a zip header as done.
    if s.starts_with("UEsDBB") || s.starts_with("UEsDB") {
        return true;
    }
    false
}

/// Build a stored (uncompressed) zip with one `index.py` member, mirroring
/// moto's CFN inline-code path: `zipfile.ZipFile(...).writestr("index.py", code)`.
fn zip_from_plaintext(code: &[u8]) -> Vec<u8> {
    let crc = crc32(code);
    let name = b"index.py";
    let mut out = Vec::with_capacity(code.len() + 160);
    // Local file header
    out.extend_from_slice(&0x04034b50u32.to_le_bytes());
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // method: stored
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0x21u16.to_le_bytes()); // mod date (1980-01-01)
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&(code.len() as u32).to_le_bytes());
    out.extend_from_slice(&(code.len() as u32).to_le_bytes());
    out.extend_from_slice(&(name.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra len
    out.extend_from_slice(name);
    out.extend_from_slice(code);
    let local_offset = 0u32;
    let cd_offset = out.len() as u32;
    // Central directory record
    out.extend_from_slice(&0x02014b50u32.to_le_bytes());
    out.extend_from_slice(&20u16.to_le_bytes()); // version made by
    out.extend_from_slice(&20u16.to_le_bytes()); // version needed
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // method
    out.extend_from_slice(&0u16.to_le_bytes()); // mod time
    out.extend_from_slice(&0x21u16.to_le_bytes()); // mod date
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&(code.len() as u32).to_le_bytes());
    out.extend_from_slice(&(code.len() as u32).to_le_bytes());
    out.extend_from_slice(&(name.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // extra len
    out.extend_from_slice(&0u16.to_le_bytes()); // comment len
    out.extend_from_slice(&0u16.to_le_bytes()); // disk number
    out.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
    out.extend_from_slice(&0u32.to_le_bytes()); // external attrs
    out.extend_from_slice(&local_offset.to_le_bytes());
    out.extend_from_slice(name);
    let cd_size = out.len() as u32 - cd_offset;
    // End of central directory
    out.extend_from_slice(&0x06054b50u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // disk
    out.extend_from_slice(&0u16.to_le_bytes()); // cd disk
    out.extend_from_slice(&1u16.to_le_bytes()); // entries on disk
    out.extend_from_slice(&1u16.to_le_bytes()); // total entries
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // comment len
    out
}

/// CRC-32 (IEEE) for zip records — the checksummed byte range zipfile uses.
fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB88320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for b in data {
        crc = table[((crc ^ *b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Kept for parity with the Python provider's helper naming.
pub fn inline_code(source: &str) -> String {
    b64_encode(source.as_bytes())
}

pub fn add_lambda_permission(props: &Value, name: &str, account: &str, region: &str) {
    let mut body = json!({"FunctionName": name});
    for (src, dst) in [
        ("Action", "Action"),
        ("Principal", "Principal"),
        ("SourceArn", "SourceArn"),
        ("SourceAccount", "SourceAccount"),
        ("StatementId", "StatementId"),
    ] {
        if let Some(v) = props.get(src) {
            if !v.is_null() {
                body.as_object_mut()
                    .unwrap()
                    .insert(dst.to_string(), v.clone());
            }
        }
    }
    let _ = json_call(
        "Lambda_20150331.AddPermission",
        "lambda",
        "/2015-03-31/functions",
        body,
        account,
        region,
    );
}

pub fn create_secretsmanager_secret(
    props: &Value,
    fallback: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let name = props
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let mut body = json!({"Name": name});
    if let Some(s) = props.get("SecretString").filter(|v| !v.is_null()) {
        let secret_string = match s {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        body.as_object_mut()
            .unwrap()
            .insert("SecretString".to_string(), Value::String(secret_string));
    }
    if let Some(desc) = props.get("Description").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(desc.to_string()));
    }
    let resp = json_call(
        "secretsmanager.CreateSecret",
        "secretsmanager",
        "/",
        body,
        account,
        region,
    );
    let body: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let arn = body.get("ARN").and_then(Value::as_str).map(String::from)?;
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn delete_secretsmanager_secret(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "secretsmanager.DeleteSecret",
        "secretsmanager",
        "/",
        json!({"SecretId": name, "ForceDeleteWithoutRecovery": true}),
        account,
        region,
    );
}

pub fn create_kinesis_stream(
    props: &Value,
    fallback: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let name = props
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let shards = props.get("ShardCount").and_then(Value::as_i64).unwrap_or(1);
    let resp = json_call(
        "Kinesis_20131202.CreateStream",
        "kinesis",
        "/",
        json!({"StreamName": name, "ShardCount": shards}),
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let arn = format!("arn:aws:kinesis:{region}:{account}:stream/{name}");
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn delete_kinesis_stream(name: &str, account: &str, region: &str) {
    let _ = json_call(
        "Kinesis_20131202.DeleteStream",
        "kinesis",
        "/",
        json!({"StreamName": name}),
        account,
        region,
    );
}

pub fn create_sfn_state_machine(
    props: &Value,
    fallback: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let name = props
        .get("StateMachineName")
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let mut body = json!({"name": name});
    if let Some(def) = props.get("DefinitionString").filter(|v| !v.is_null()) {
        let def_string = match def {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        body.as_object_mut()
            .unwrap()
            .insert("definition".to_string(), Value::String(def_string));
    }
    if let Some(role) = props.get("RoleArn").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("roleArn".to_string(), Value::String(role.to_string()));
    }
    let resp = json_call(
        "AWSStepFunctions.CreateStateMachine",
        "stepfunctions",
        "/",
        body,
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let body: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let sm_arn = body
        .get("stateMachineArn")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("arn:aws:states:{region}:{account}:stateMachine:{name}"));
    Some((sm_arn.clone(), json!({"Arn": sm_arn})))
}

pub fn delete_sfn_state_machine(arn: &str, account: &str, region: &str) {
    let _ = json_call(
        "AWSStepFunctions.DeleteStateMachine",
        "stepfunctions",
        "/",
        json!({"stateMachineArn": arn}),
        account,
        region,
    );
}

pub fn create_ecs_cluster(name: &str, account: &str, region: &str) -> Option<(String, Value)> {
    let resp = json_call(
        "AmazonEC2ContainerServiceV20141113.CreateCluster",
        "ecs",
        "/",
        json!({"clusterName": name}),
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let body: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let arn = body
        .pointer("/cluster/clusterArn")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("arn:aws:ecs:{region}:{account}:cluster/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn delete_ecs_cluster(arn: &str, account: &str, region: &str) {
    let _ = json_call(
        "AmazonEC2ContainerServiceV20141113.DeleteCluster",
        "ecs",
        "/",
        json!({"cluster": arn}),
        account,
        region,
    );
}

/// Base64 encode (for lambda inline code).
pub fn b64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn put_user_policy(user: &str, policy_name: &str, doc: &Value, account: &str) -> bool {
    form_call(
        "iam",
        "PutUserPolicy",
        "2010-05-08",
        &[
            ("UserName", user.to_string()),
            ("PolicyName", policy_name.to_string()),
            ("PolicyDocument", policy_json(doc)),
        ],
        account,
        "global",
    )
    .ok()
}

// ---------------------------------------------------------------------------
// EC2 VPC family. moto's `create_from_cloudformation_json` calls the EC2
// backend directly with the template properties and applies tags after the
// create (`moto/ec2/models/vpcs.py`, `subnets.py`, `security_groups.py`);
// the mirror drives the same handler chain over loopback HTTP.
// ---------------------------------------------------------------------------

/// One legacy-query call to the EC2 service.
fn ec2_form(action: &str, params: &[(&str, String)], account: &str, region: &str) -> MirrorCall {
    form_call("ec2", action, "2016-11-15", params, account, region)
}

/// CFN `Tags: [{Key, Value}]` → a CreateTags form body fragment.
fn tags_pairs(props: &Value, resource_id: &str) -> Vec<(String, String)> {
    let mut out = vec![("ResourceId.1".to_string(), resource_id.to_string())];
    if let Some(tags) = props.get("Tags").and_then(Value::as_array) {
        for (i, tag) in tags.iter().enumerate() {
            let n = i + 1;
            if let Some(k) = tag.get("Key").and_then(Value::as_str) {
                out.push((format!("Tag.{n}.Key"), k.to_string()));
            }
            if let Some(v) = tag.get("Value").and_then(Value::as_str) {
                out.push((format!("Tag.{n}.Value"), v.to_string()));
            }
        }
    }
    out
}

fn ec2_add_tags(props: &Value, resource_id: &str, account: &str, region: &str) {
    let pairs = tags_pairs(props, resource_id);
    if pairs.len() <= 1 {
        return;
    }
    let refs: Vec<(&str, String)> = pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    let _ = ec2_form("CreateTags", &refs, account, region);
}

pub fn create_ec2_vpc(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let cidr = props.get("CidrBlock").and_then(Value::as_str).unwrap_or("");
    let resp = ec2_form(
        "CreateVpc",
        &[("CidrBlock", cidr.to_string())],
        account,
        region,
    );
    let vpc_id = text(&resp.body, "vpcId")?;
    ec2_add_tags(props, &vpc_id, account, region);
    Some((vpc_id.clone(), json!({"VpcId": vpc_id, "CidrBlock": cidr})))
}

pub fn delete_ec2_vpc(vpc_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DeleteVpc",
        &[("VpcId", vpc_id.to_string())],
        account,
        region,
    );
}

pub fn create_ec2_subnet(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let mut params: Vec<(&str, String)> = vec![
        ("VpcId", value_str(props, "VpcId")),
        ("CidrBlock", value_str(props, "CidrBlock")),
    ];
    if let Some(az) = props.get("AvailabilityZone").and_then(Value::as_str) {
        params.push(("AvailabilityZone", az.to_string()));
    }
    let resp = ec2_form("CreateSubnet", &params, account, region);
    let subnet_id = text(&resp.body, "subnetId")?;
    ec2_add_tags(props, &subnet_id, account, region);
    Some((subnet_id.clone(), json!({"SubnetId": subnet_id})))
}

pub fn delete_ec2_subnet(subnet_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DeleteSubnet",
        &[("SubnetId", subnet_id.to_string())],
        account,
        region,
    );
}

pub fn create_ec2_internet_gateway(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let resp = ec2_form("CreateInternetGateway", &[], account, region);
    let igw_id = text(&resp.body, "internetGatewayId")?;
    ec2_add_tags(props, &igw_id, account, region);
    Some((igw_id.clone(), json!({"InternetGatewayId": igw_id})))
}

pub fn delete_ec2_internet_gateway(igw_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DeleteInternetGateway",
        &[("InternetGatewayId", igw_id.to_string())],
        account,
        region,
    );
}

/// AWS::EC2::VPCGatewayAttachment — attach the gateway to the VPC.
pub fn create_ec2_vpc_gateway_attachment(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let vpc_id = value_str(props, "VpcId");
    let igw_id = value_str(props, "InternetGatewayId");
    if igw_id.is_empty() {
        // VGW attachments are out of mirror scope for the lanes.
        return None;
    }
    let resp = ec2_form(
        "AttachInternetGateway",
        &[("InternetGatewayId", igw_id), ("VpcId", vpc_id)],
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    let attach_id =
        text(&resp.body, "attachmentId").unwrap_or_else(|| format!("igw-attach-{}", uuid_head()));
    Some((attach_id, json!({})))
}

pub fn delete_ec2_vpc_gateway_attachment(props: &Value, account: &str, region: &str) {
    let vpc_id = value_str(props, "VpcId");
    let igw_id = value_str(props, "InternetGatewayId");
    if igw_id.is_empty() {
        return;
    }
    let _ = ec2_form(
        "DetachInternetGateway",
        &[("InternetGatewayId", igw_id), ("VpcId", vpc_id)],
        account,
        region,
    );
}

pub fn create_ec2_route_table(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let resp = ec2_form(
        "CreateRouteTable",
        &[("VpcId", value_str(props, "VpcId"))],
        account,
        region,
    );
    let rt_id = text(&resp.body, "routeTableId")?;
    ec2_add_tags(props, &rt_id, account, region);
    Some((rt_id.clone(), json!({"RouteTableId": rt_id})))
}

pub fn delete_ec2_route_table(rt_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DeleteRouteTable",
        &[("RouteTableId", rt_id.to_string())],
        account,
        region,
    );
}

pub fn create_ec2_route(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let mut params: Vec<(&str, String)> = vec![("RouteTableId", value_str(props, "RouteTableId"))];
    for (prop_key, form_key) in [
        ("DestinationCidrBlock", "DestinationCidrBlock"),
        ("DestinationIpv6CidrBlock", "DestinationIpv6CidrBlock"),
        ("GatewayId", "GatewayId"),
        ("NatGatewayId", "NatGatewayId"),
        ("TransitGatewayId", "TransitGatewayId"),
    ] {
        let v = value_str(props, prop_key);
        if !v.is_empty() {
            params.push((form_key, v));
        }
    }
    let resp = ec2_form("CreateRoute", &params, account, region);
    if !resp.ok() {
        return None;
    }
    Some((format!("route-{}", uuid_head()), json!({})))
}

pub fn delete_ec2_route(props: &Value, account: &str, region: &str) {
    let rt_id = value_str(props, "RouteTableId");
    if rt_id.is_empty() {
        return;
    }
    let cidr = value_str(props, "DestinationCidrBlock");
    if cidr.is_empty() {
        return;
    }
    let _ = ec2_form(
        "DeleteRoute",
        &[("RouteTableId", rt_id), ("DestinationCidrBlock", cidr)],
        account,
        region,
    );
}

pub fn create_ec2_subnet_rtb_association(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let resp = ec2_form(
        "AssociateRouteTable",
        &[
            ("RouteTableId", value_str(props, "RouteTableId")),
            ("SubnetId", value_str(props, "SubnetId")),
        ],
        account,
        region,
    );
    let assoc_id = text(&resp.body, "associationId")?;
    Some((assoc_id.clone(), json!({"AssociationId": assoc_id})))
}

pub fn delete_ec2_subnet_rtb_association(assoc_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DisassociateRouteTable",
        &[("AssociationId", assoc_id.to_string())],
        account,
        region,
    );
}

pub fn create_ec2_security_group(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let vpc_id = value_str(props, "VpcId");
    let mut params: Vec<(&str, String)> = vec![
        ("GroupName", name.to_string()),
        ("GroupDescription", value_str(props, "GroupDescription")),
    ];
    if !vpc_id.is_empty() {
        params.push(("VpcId", vpc_id));
    }
    let resp = ec2_form("CreateSecurityGroup", &params, account, region);
    let sg_id = text(&resp.body, "groupId")?;
    ec2_add_tags(props, &sg_id, account, region);
    for (i, rule) in props
        .get("SecurityGroupIngress")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
        .iter()
        .enumerate()
    {
        let mut pairs: Vec<(String, String)> = vec![("GroupId".to_string(), sg_id.clone())];
        for (prop_key, form_key) in [
            ("IpProtocol", "IpProtocol"),
            ("FromPort", "FromPort"),
            ("ToPort", "ToPort"),
        ] {
            let v = value_str(rule, prop_key);
            if !v.is_empty() {
                pairs.push((format!("IpPermissions.{}.{}", i + 1, form_key), v));
            }
        }
        if let Some(cidr) = rule.get("CidrIp").and_then(Value::as_str) {
            pairs.push((
                format!("IpPermissions.{}.IpRanges.1.CidrIp", i + 1),
                cidr.to_string(),
            ));
        }
        if let Some(sg_ref) = rule.get("SourceSecurityGroupId").and_then(Value::as_str) {
            pairs.push((
                format!("IpPermissions.{}.UserIdGroupPairs.1.GroupId", i + 1),
                sg_ref.to_string(),
            ));
        }
        let refs: Vec<(&str, String)> =
            pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let _ = ec2_form("AuthorizeSecurityGroupIngress", &refs, account, region);
    }
    Some((sg_id.clone(), json!({"GroupId": sg_id})))
}

pub fn delete_ec2_security_group(sg_id: &str, account: &str, region: &str) {
    let _ = ec2_form(
        "DeleteSecurityGroup",
        &[("GroupId", sg_id.to_string())],
        account,
        region,
    );
}

/// Read a scalar property as a string (numbers/bools stringify).
pub fn value_str(props: &Value, key: &str) -> String {
    match props.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

/// Fetch one S3 object over the loopback mirror (nested-stack templates).
pub fn s3_get_object(bucket: &str, key: &str, account: &str, region: &str) -> Option<String> {
    let headers = base_headers(account, region, "s3");
    let url = format!("{}/{}/{}", callback_base(), bucket, key);
    let req = with_headers(client().get(&url), &headers);
    match send(req, b"") {
        r if (200..300).contains(&r.status) && r.status != 0 && !r.body.is_empty() => Some(r.body),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Cognito User Pools. moto has no CFN hooks for cognitoidp (the fork skips
// these types), so follow real AWS semantics through the native JSON API.
// ---------------------------------------------------------------------------

fn cognito_json(target: &str, body: Value, account: &str, region: &str) -> MirrorCall {
    json_call(
        &format!("AWSCognitoIdentityProviderService.{target}"),
        "cognito-idp",
        "/",
        body,
        account,
        region,
    )
}

pub fn create_cognito_user_pool(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let mut body = json!({ "PoolName": name });
    let obj = props.as_object();
    for key in [
        "AutoVerifiedAttributes",
        "Policies",
        "UsernameAttributes",
        "MfaConfiguration",
    ] {
        if let Some(v) = obj.and_then(|o| o.get(key)).filter(|v| !v.is_null()) {
            body.as_object_mut()
                .unwrap()
                .insert(key.to_string(), v.clone());
        }
    }
    let resp = cognito_json("CreateUserPool", body, account, region);
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let pool = parsed.get("UserPool")?;
    let pool_id = pool.get("Id").and_then(Value::as_str)?.to_string();
    let arn = pool
        .get("Arn")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_default();
    Some((
        pool_id.clone(),
        json!({"Id": pool_id, "Arn": arn, "Name": name}),
    ))
}

pub fn delete_cognito_user_pool(pool_id: &str, account: &str, region: &str) {
    let _ = cognito_json(
        "DeleteUserPool",
        json!({"UserPoolId": pool_id}),
        account,
        region,
    );
}

pub fn create_cognito_user_pool_client(
    props: &Value,
    fallback: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let pool_id = value_str(props, "UserPoolId");
    if pool_id.is_empty() {
        return None;
    }
    let name = props
        .get("ClientName")
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let mut body = json!({"UserPoolId": pool_id, "ClientName": name});
    if let Some(flows) = props.get("ExplicitAuthFlows").filter(|v| !v.is_null()) {
        body.as_object_mut()
            .unwrap()
            .insert("ExplicitAuthFlows".to_string(), flows.clone());
    }
    let resp = cognito_json("CreateUserPoolClient", body, account, region);
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let client = parsed.get("UserPoolClient")?;
    let client_id = client.get("ClientId").and_then(Value::as_str)?.to_string();
    Some((
        client_id.clone(),
        json!({"ClientId": client_id, "ClientName": name, "UserPoolId": pool_id}),
    ))
}

pub fn delete_cognito_user_pool_client(
    client_id: &str,
    pool_id: &str,
    account: &str,
    region: &str,
) {
    let _ = cognito_json(
        "DeleteUserPoolClient",
        json!({"UserPoolId": pool_id, "ClientId": client_id}),
        account,
        region,
    );
}

// ---------------------------------------------------------------------------
// CloudWatch Alarms (JSON protocol; real SDK target prefix is
// GraniteServiceVersion20100801 — see src/router.rs TARGET_PREFIX_MAP).
// ---------------------------------------------------------------------------

fn cloudwatch_json(target: &str, body: Value, account: &str, region: &str) -> MirrorCall {
    json_call(
        &format!("GraniteServiceVersion20100801.{target}"),
        "cloudwatch",
        "/",
        body,
        account,
        region,
    )
}

pub fn create_cloudwatch_alarm(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let mut body = json!({"AlarmName": name});
    for key in [
        "AlarmDescription",
        "Namespace",
        "MetricName",
        "Statistic",
        "ExtendedStatistic",
        "Period",
        "EvaluationPeriods",
        "DatapointsToAlarm",
        "Threshold",
        "ComparisonOperator",
        "TreatMissingData",
        "Unit",
        "AlarmActions",
        "OKActions",
        "InsufficientDataActions",
        "Dimensions",
    ] {
        if let Some(v) = props.get(key).filter(|v| !v.is_null()) {
            body.as_object_mut()
                .unwrap()
                .insert(key.to_string(), v.clone());
        }
    }
    let resp = cloudwatch_json("PutMetricAlarm", body, account, region);
    if !resp.ok() {
        return None;
    }
    Some((
        name.to_string(),
        json!({"AlarmName": name, "AlarmArn": format!("arn:aws:cloudwatch:{region}:{account}:alarm:{name}")}),
    ))
}

pub fn delete_cloudwatch_alarm(name: &str, account: &str, region: &str) {
    let _ = cloudwatch_json(
        "DeleteAlarms",
        json!({"AlarmNames": [name]}),
        account,
        region,
    );
}

// ---------------------------------------------------------------------------
// API Gateway REST API family (rest-json over path routing; the twin's
// apigateway backend serves `/restapis/...`). moto's apigateway CFN hooks
// call the same backend methods (`moto/apigateway/models.py`).
// ---------------------------------------------------------------------------

fn apigw_rest(method: &str, path: &str, body: Value, account: &str, region: &str) -> MirrorCall {
    let mut headers = base_headers(account, region, "apigateway");
    headers.push(("Content-Type".to_string(), "application/json".to_string()));
    let path = if path.is_empty() { "/restapis" } else { path };
    let req = match method {
        "DELETE" => client().delete(&format!("{}{path}", callback_base())),
        "PUT" => client().put(&format!("{}{path}", callback_base())),
        _ => client().post(&format!("{}{path}", callback_base())),
    };
    with_send(req, &headers, body.to_string().as_bytes())
}

fn with_send(req: ureq::Request, headers: &[(String, String)], body: &[u8]) -> MirrorCall {
    send(with_headers(req, headers), body)
}

pub fn create_apigw_rest_api(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let mut body = json!({"name": name});
    if let Some(desc) = props.get("Description").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("description".to_string(), json!(desc));
    }
    let resp = apigw_rest("POST", "", body, account, region);
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let api_id = parsed.get("id").and_then(Value::as_str)?.to_string();
    let root = parsed
        .get("rootResourceId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let attrs = json!({"RootResourceId": root, "ApiId": api_id, "Name": name});
    Some((api_id, attrs))
}

pub fn delete_apigw_rest_api(api_id: &str, account: &str, region: &str) {
    let _ = apigw_rest(
        "DELETE",
        &format!("/restapis/{api_id}"),
        json!({}),
        account,
        region,
    );
}

pub fn create_apigw_resource(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let api_id = value_str(props, "RestApiId");
    let parent = value_str(props, "ParentId");
    let path_part = value_str(props, "PathPart");
    if api_id.is_empty() || parent.is_empty() || path_part.is_empty() {
        return None;
    }
    let resp = apigw_rest(
        "POST",
        &format!("/restapis/{api_id}/resources/{parent}"),
        json!({"pathPart": path_part}),
        account,
        region,
    );
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let id = parsed.get("id").and_then(Value::as_str)?.to_string();
    Some((
        id.clone(),
        json!({"ResourceId": id, "PathPart": path_part, "Path": parsed.get("path").and_then(Value::as_str).unwrap_or("")}),
    ))
}

pub fn delete_apigw_resource(api_id: &str, resource_id: &str, account: &str, region: &str) {
    let _ = apigw_rest(
        "DELETE",
        &format!("/restapis/{api_id}/resources/{resource_id}"),
        json!({}),
        account,
        region,
    );
}

pub fn create_apigw_method(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let api_id = value_str(props, "RestApiId");
    let resource_id = value_str(props, "ResourceId");
    let http_method = value_str(props, "HttpMethod").to_uppercase();
    if api_id.is_empty() || resource_id.is_empty() || http_method.is_empty() {
        return None;
    }
    let mut body = json!({});
    if let Some(auth) = props.get("AuthorizationType").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("authorizationType".to_string(), json!(auth));
    }
    if let Some(req_params) = props.get("RequestParameters").filter(|v| !v.is_null()) {
        body.as_object_mut()
            .unwrap()
            .insert("requestParameters".to_string(), req_params.clone());
    }
    let resp = apigw_rest(
        "PUT",
        &format!("/restapis/{api_id}/resources/{resource_id}/methods/{http_method}"),
        body,
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    // An inline `Integration` property deploys with the method (the CFN
    // AWS::ApiGateway::Method Integration semantics the templates use).
    // CFN property names are PascalCase; the backend's wire fields are
    // camelCase, so translate before the PUT.
    if let Some(integration) = props.get("Integration").filter(|v| !v.is_null()) {
        let mut int_body = integration.clone();
        if let Some(obj) = int_body.as_object_mut() {
            for (cfn_key, wire_key) in [
                ("Type", "type"),
                ("IntegrationHttpMethod", "httpMethod"),
                ("Uri", "uri"),
                ("PassthroughBehavior", "passthroughBehavior"),
                ("TimeoutInMillis", "timeoutInMillis"),
                ("ConnectionType", "connectionType"),
                ("Credentials", "credentials"),
                ("CacheNamespace", "cacheNamespace"),
                ("CacheKeyParameters", "cacheKeyParameters"),
                ("RequestTemplates", "requestTemplates"),
                ("RequestParameters", "requestParameters"),
            ] {
                if let Some(v) = obj.remove(cfn_key) {
                    obj.insert(wire_key.to_string(), v);
                }
            }
        }
        let int_resp = apigw_rest(
            "PUT",
            &format!(
                "/restapis/{api_id}/resources/{resource_id}/methods/{http_method}/integration"
            ),
            int_body,
            account,
            region,
        );
        if !int_resp.ok() {
            return None;
        }
    }
    Some((
        format!("{resource_id}-{http_method}"),
        json!({"HttpMethod": http_method}),
    ))
}

pub fn delete_apigw_method(
    api_id: &str,
    resource_id: &str,
    http_method: &str,
    account: &str,
    region: &str,
) {
    let _ = apigw_rest(
        "DELETE",
        &format!("/restapis/{api_id}/resources/{resource_id}/methods/{http_method}"),
        json!({}),
        account,
        region,
    );
}

pub fn create_apigw_deployment(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let api_id = value_str(props, "RestApiId");
    if api_id.is_empty() {
        return None;
    }
    let mut body = json!({});
    if let Some(desc) = props.get("Description").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("description".to_string(), json!(desc));
    }
    let resp = apigw_rest(
        "POST",
        &format!("/restapis/{api_id}/deployments"),
        body,
        account,
        region,
    );
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let id = parsed.get("id").and_then(Value::as_str)?.to_string();
    Some((id.clone(), json!({"DeploymentId": id})))
}

pub fn delete_apigw_deployment(api_id: &str, deployment_id: &str, account: &str, region: &str) {
    let _ = apigw_rest(
        "DELETE",
        &format!("/restapis/{api_id}/deployments/{deployment_id}"),
        json!({}),
        account,
        region,
    );
}

pub fn create_apigw_stage(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let api_id = value_str(props, "RestApiId");
    let stage_name = value_str(props, "StageName");
    if api_id.is_empty() || stage_name.is_empty() {
        return None;
    }
    let mut body =
        json!({"stageName": stage_name, "deploymentId": value_str(props, "DeploymentId")});
    if let Some(desc) = props.get("Description").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("description".to_string(), json!(desc));
    }
    let resp = apigw_rest(
        "POST",
        &format!("/restapis/{api_id}/stages"),
        body,
        account,
        region,
    );
    if !resp.ok() {
        return None;
    }
    Some((stage_name.clone(), json!({"StageName": stage_name})))
}

pub fn delete_apigw_stage(api_id: &str, stage_name: &str, account: &str, region: &str) {
    let _ = apigw_rest(
        "DELETE",
        &format!("/restapis/{api_id}/stages/{stage_name}"),
        json!({}),
        account,
        region,
    );
}

// ---------------------------------------------------------------------------
// Lambda EventSourceMapping (REST: POST /2015-03-31/event-source-mappings).
// ---------------------------------------------------------------------------

pub fn create_lambda_esm(props: &Value, account: &str, region: &str) -> Option<(String, Value)> {
    let mut body = json!({
        "EventSourceArn": value_str(props, "EventSourceArn"),
        "FunctionName": value_str(props, "FunctionName"),
        "BatchSize": props.get("BatchSize").and_then(Value::as_u64).unwrap_or(10),
    });
    if let Some(sp) = props.get("StartingPosition").and_then(Value::as_str) {
        body.as_object_mut()
            .unwrap()
            .insert("StartingPosition".to_string(), json!(sp));
    }
    if let Some(enabled) = props.get("Enabled").and_then(Value::as_bool) {
        body.as_object_mut()
            .unwrap()
            .insert("Enabled".to_string(), json!(enabled));
    }
    let mut headers = base_headers(account, region, "lambda");
    headers.push((
        "Content-Type".to_string(),
        "application/x-amz-json-1.1".to_string(),
    ));
    let req = client().post(&format!(
        "{}/2015-03-31/event-source-mappings",
        callback_base()
    ));
    let resp = send(with_headers(req, &headers), body.to_string().as_bytes());
    if !resp.ok() {
        return None;
    }
    let parsed: Value = serde_json::from_str(&resp.body).unwrap_or(json!({}));
    let uuid = parsed.get("UUID").and_then(Value::as_str)?.to_string();
    Some((uuid.clone(), json!({"UUID": uuid})))
}

pub fn delete_lambda_esm(uuid: &str, account: &str, region: &str) {
    if uuid.is_empty() {
        return;
    }
    let _ = apigw_rest(
        "DELETE",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        json!({}),
        account,
        region,
    );
}
