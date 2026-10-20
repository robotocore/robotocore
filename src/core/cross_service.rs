//! Gateway-registered cross-service dispatch.
//!
//! The events crate dispatches non-SQS/non-bus target ARNs through the
//! `events::bridge` hook registry registered here (S3 → Lambda notifications
//! use the same registry), and the lambda crate delivers its async-invocation
//! destination records through the same registry. This keeps the crates
//! acyclic: lambda, sns and their dependents cannot be direct dependencies of
//! the events crate while S3 publishes into events.
//!
//! Hook semantics follow the Python-era provider:
//! - "lambda": synchronous invoke; a missing function is `Err` (the events
//!   dispatcher turns that into a DLQ record). Handler-level errors are not
//!   DLQ conditions.
//! - "sns": a real `Publish` through the SNS handler so the topic's own
//!   queue/lambda subscribers receive the notification envelope.
//! - "sns-result" / "sns-dlq": subject-carrying destination deliveries for
//!   the lambda async-invocation engine ("Lambda Invocation Result" /
//!   "Lambda DLQ").
//! - "events-target": a PutEvents submission carrying the destination record
//!   as `Detail` (Python twin's `store.put_events`).
//! - "stepfunctions": `StartExecution` in Step Functions; failures are `Err`
//!   (Python raises toward the DLQ).
//! - "cloudwatch-logs": create-if-missing group/stream, then PutLogEvents
//!   with the payload as the message.
//!
//! Hook returns `Ok` only for SQS topics — sns missing topics are no-ops like
//! the Python provider's error-log-and-return behavior.

use serde_json::{json, Value};
use std::sync::Arc;

use crate::core::server::NativeHandlers;

/// Register all cross-service dispatch hooks for the process.
///
/// Idempotent on purpose: introspection paths construct throwaway
/// `ServiceRegistry` instances purely to list service names; their handlers
/// must never replace the hooks of the instances that actually serve the
/// wire (registered hooks bind to the first registry in the process).
pub fn register(handlers: &NativeHandlers) {
    static REGISTERED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    if REGISTERED.set(()).is_err() {
        return;
    }
    events::register_hook(
        "lambda",
        Arc::new(
            move |arn: &str, payload: &str, _account: u64, _region: &str| {
                let Ok(event) = serde_json::from_str::<Value>(payload) else {
                    return Err(format!("EventBridge target payload is not JSON: {payload}"));
                };
                match lambda::invoke_by_arn(arn, &event) {
                    Some(_) => Ok(()),
                    None => Err(format!("Lambda function not found: {arn}")),
                }
            },
        ),
    );

    for (hook_kind, subject) in [
        ("sns", ""),
        ("sns-result", "Lambda Invocation Result"),
        ("sns-dlq", "Lambda DLQ"),
    ] {
        events::register_hook(
            hook_kind,
            Arc::new({
                let sns = handlers.sns.clone();
                move |arn: &str, payload: &str, account: u64, region: &str| {
                    let mut params = json!({ "TopicArn": arn, "Message": payload });
                    if !subject.is_empty() {
                        params["Subject"] = json!(subject);
                    }
                    let resp = sns.handle(sns::AwsRequest {
                        service: "sns".into(),
                        operation: "Publish".into(),
                        account,
                        region: region.to_string(),
                        params,
                        body: bytes::Bytes::new(),
                    });
                    if resp.status >= 400 {
                        return Err(format!("SNS publish failed for {arn}: {}", resp.status));
                    }
                    Ok(())
                }
            }),
        );
    }

    // EventBridge bus destination: the record travels as the Detail of a
    // PutEvents submission (Python twin `destinations.py::_send_to_eventbridge`).
    events::register_hook(
        "events-target",
        Arc::new({
            let events_handler = handlers.events.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let condition = serde_json::from_str::<Value>(payload)
                    .ok()
                    .and_then(|r| {
                        r["requestContext"]["condition"].as_str().map(String::from)
                    })
                    .unwrap_or_default();
                // arn:aws:events:<region>:<account>:event-bus/<name>
                let bus_name = arn
                    .rsplit("event-bus/")
                    .next()
                    .unwrap_or("default")
                    .to_string();
                let resp = events_handler.handle(events::AwsRequest {
                    service: "events".into(),
                    operation: "PutEvents".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "Entries": [{
                            "Source": "lambda",
                            "DetailType": format!(
                                "Lambda Function Invocation Result - {condition}"
                            ),
                            "Detail": payload,
                            "EventBusName": bus_name,
                        }]
                    }),
                    body: bytes::Bytes::new(),
                });
                if resp.status >= 400 {
                    return Err(format!("EventBridge put failed for {arn}: {}", resp.status));
                }
                Ok(())
            }
        }),
    );

    events::register_hook(
        "stepfunctions",
        Arc::new({
            let sfn = handlers.stepfunctions.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let params = json!({
                    "stateMachineArn": arn,
                    "name": format!("eb-{}", uuid::Uuid::new_v4().simple()),
                    "input": payload,
                });
                let resp = sfn.handle(stepfunctions::AwsRequest {
                    service: "stepfunctions".into(),
                    operation: "StartExecution".into(),
                    account,
                    region: region.to_string(),
                    params,
                    body: bytes::Bytes::new(),
                });
                if resp.status >= 400 {
                    return Err(format!(
                        "Step Functions start failed for {arn}: {}",
                        resp.status
                    ));
                }
                Ok(())
            }
        }),
    );

    events::register_hook(
        "cloudwatch-logs",
        Arc::new({
            let logs = handlers.logs.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let group_name = log_group_of(arn);
                if group_name.is_empty() {
                    return Err(format!("Log group ARN unparseable: {arn}"));
                }
                // Create-if-missing, best effort (Python parity).
                let _ = logs.handle(cloudwatch_logs::AwsRequest {
                    service: "logs".into(),
                    operation: "CreateLogGroup".into(),
                    account,
                    region: region.to_string(),
                    params: json!({ "logGroupName": group_name }),
                    body: bytes::Bytes::new(),
                });
                let stream_name = format!("eventbridge-{}", uuid::Uuid::new_v4().simple());
                let _ = logs.handle(cloudwatch_logs::AwsRequest {
                    service: "logs".into(),
                    operation: "CreateLogStream".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "logGroupName": group_name,
                        "logStreamName": stream_name,
                    }),
                    body: bytes::Bytes::new(),
                });
                let resp = logs.handle(cloudwatch_logs::AwsRequest {
                    service: "logs".into(),
                    operation: "PutLogEvents".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "logGroupName": group_name,
                        "logStreamName": stream_name,
                        "logEvents": [{
                            "timestamp": chrono::Utc::now().timestamp_millis(),
                            "message": payload,
                        }],
                    }),
                    body: bytes::Bytes::new(),
                });
                if resp.status >= 400 {
                    return Err(format!(
                        "CloudWatch Logs put failed for {group_name}: {}",
                        resp.status
                    ));
                }
                Ok(())
            }
        }),
    );

    events::register_hook(
        "kinesis",
        Arc::new({
            let kinesis = handlers.kinesis.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                // arn:aws:kinesis:<region>:<account>:stream/<name>
                let stream_name = match arn.rsplit('/').next()
                    .filter(|s| !s.is_empty())
                {
                    Some(n) => n.to_string(),
                    None => return Err(format!("Kinesis stream ARN unparseable: {arn}")),
                };
                use base64::Engine;
                let data = base64::engine::general_purpose::STANDARD.encode(payload.as_bytes());
                let resp = kinesis.handle(kinesis::AwsRequest {
                    service: "kinesis".into(),
                    operation: "PutRecord".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "StreamName": stream_name,
                        "Data": data,
                        "PartitionKey": format!("eventbridge-{}", uuid::Uuid::new_v4().simple()),
                    }),
                    body: bytes::Bytes::new(),
                });
                if resp.status >= 400 {
                    // Missing stream / shard is a no-op like the Python engine.
                    return Ok(());
                }
                Ok(())
            }
        }),
    );

    // Event source mappings are driven by the gateway's own ESM engine
    // (src/core/esm_engine.rs), started from the binary entrypoint — no
    // additional poller is needed here.
}

/// `arn:aws:logs:<region>:<account>:log-group:<name>[:*]` → `<name>`.
fn log_group_of(arn: &str) -> String {
    let Some(rest) = arn.split("log-group:").nth(1) else {
        return String::new();
    };
    rest.trim_end_matches(":*").to_string()
}
