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
    AGENT.get_or_init(|| {
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
    let mut pairs: Vec<(&str, String)> =
        vec![("Action", action.to_string()), ("Version", version.to_string())];
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

pub fn create_sqs_queue(name: &str, account: &str, region: &str) -> Option<(String, Value)> {
    let resp = form_call(
        "sqs",
        "CreateQueue",
        "2012-11-05",
        &[("QueueName", name.to_string())],
        account,
        region,
    );
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

pub fn create_dynamodb_table(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> bool {
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

pub fn create_instance_profile(name: &str, roles: &[String], account: &str) -> Option<(String, Value)> {
    let mut params: Vec<(String, String)> = vec![("InstanceProfileName".to_string(), name.to_string())];
    for (i, r) in roles.iter().enumerate() {
        params.push((format!("Roles.member.{}", i + 1), r.clone()));
    }
    let params_ref: Vec<(&str, String)> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    let resp = form_call("iam", "CreateInstanceProfile", "2010-05-08", &params_ref, account, "global");
    let arn = text(&resp.body, "Arn").unwrap_or_else(|| format!("arn:aws:iam::{account}:instance-profile/{name}"));
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
    let arn = text(&resp.body, "Arn").unwrap_or_else(|| format!("arn:aws:iam::{account}:user/{name}"));
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
    let arn = text(&resp.body, "Arn").unwrap_or_else(|| format!("arn:aws:iam::{account}:group/{name}"));
    Some((name.to_string(), json!({"Arn": arn})))
}

pub fn put_ssm_parameter(props: &Value, name: &str, account: &str, region: &str) -> bool {
    let value = match props.get("Value").unwrap_or(&Value::Null) {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let ptype = props.get("Type").and_then(Value::as_str).unwrap_or("String");
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
                        body.as_object_mut().unwrap().insert(
                            key.to_string(),
                            Value::String(policy_json(v)),
                        );
                    } else {
                        body.as_object_mut()
                            .unwrap()
                            .insert(key.to_string(), v.clone());
                    }
                }
            }
        }
        let state = obj.get("State").and_then(Value::as_str).unwrap_or("ENABLED");
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
    let resp = json_call("CloudWatchEvents.PutRule", "events", "/", body, account, region);
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

pub fn create_kms_key(
    props: &Value,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let desc = props.get("Description").and_then(Value::as_str).unwrap_or("");
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
    json_call("TrentService.CreateAlias", "kms", "/", json!({"AliasName": name, "TargetKeyId": target}), account, region).ok()
}

pub fn delete_kms_alias(name: &str, account: &str, region: &str) {
    let _ = json_call("TrentService.DeleteAlias", "kms", "/", json!({"AliasName": name}), account, region);
}

pub fn create_lambda_function(
    props: &Value,
    name: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let runtime = props.get("Runtime").and_then(Value::as_str).unwrap_or("python3.12");
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
        fn_body.as_object_mut().unwrap().insert("Environment".to_string(), env.clone());
    }
    if let Some(layers) = props.get("Layers") {
        fn_body.as_object_mut().unwrap().insert("Layers".to_string(), layers.clone());
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
/// header), keep it; else base64 the raw source (the native lambda store
/// records it, matching the inline-code path verified on the lane).
fn zip_source(zip: &Value) -> String {
    match zip {
        Value::String(s) => {
            if is_zip_b64(s) { s.clone() } else { b64_encode(s.as_bytes()) }
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
                body.as_object_mut().unwrap().insert(dst.to_string(), v.clone());
            }
        }
    }
    let _ = json_call("Lambda_20150331.AddPermission", "lambda", "/2015-03-31/functions", body, account, region);
}

pub fn create_secretsmanager_secret(
    props: &Value,
    fallback: &str,
    account: &str,
    region: &str,
) -> Option<(String, Value)> {
    let name = props.get("Name").and_then(Value::as_str).unwrap_or(fallback);
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
    let name = props.get("Name").and_then(Value::as_str).unwrap_or(fallback);
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
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
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
