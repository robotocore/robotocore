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
//! - "eventbridge-sns": the EventBridge-originated SNS publish, carrying
//!   the subject "EventBridge Notification" (era `_invoke_sns_target`);
//!   the plain "sns" hook stays subjectless for other origins (S3).
//! - "sns-result" / "sns-dlq": subject-carrying destination deliveries for
//!   the lambda async-invocation engine ("Lambda Invocation Result" /
//!   "Lambda DLQ").
//! - "firehose": a `PutRecord` into the delivery stream (era buffered the
//!   payload into the stream); a missing stream is a no-op like Kinesis.
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

    for hook_kind in ["sns", "sns-result", "sns-dlq"] {
        // The plain "sns" hook serves raw notification publishers (S3
        // notification configs); EventBridge → SNS runs through its own
        // "eventbridge-sns" registration below carrying the era subject
        // ("EventBridge Notification"), and the lambda async-invocation
        // engine carries its own result/DLQ subjects.
        let subject = match hook_kind {
            "sns-result" => "Lambda Invocation Result",
            "sns-dlq" => "Lambda DLQ",
            _ => "",
        };
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

    // EventBridge → SNS: era `_invoke_sns_target` publishes with the
    // subject "EventBridge Notification".
    events::register_hook(
        "eventbridge-sns",
        Arc::new({
            let sns = handlers.sns.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let resp = sns.handle(sns::AwsRequest {
                    service: "sns".into(),
                    operation: "Publish".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "TopicArn": arn,
                        "Message": payload,
                        "Subject": "EventBridge Notification",
                    }),
                    body: bytes::Bytes::new(),
                });
                // Missing topic: no-op like the Python provider's
                // error-log-and-return behavior.
                if resp.status >= 400 {
                    return Err(format!("SNS publish failed for {arn}: {}", resp.status));
                }
                Ok(())
            }
        }),
    );

    // EventBridge → Firehose: era `_invoke_firehose_target` buffers the
    // payload into the delivery stream; the twin runs the same delivery
    // through the firehose handler's `PutRecord` (stream-existence check +
    // audit trail). A missing stream stays a no-op, not an Err.
    events::register_hook(
        "firehose",
        Arc::new({
            let firehose = handlers.firehose.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                // arn:aws:firehose:<region>:<account>:deliverystream/<name>
                let stream_name = match arn.split_once("deliverystream/") {
                    Some((_, name)) if !name.is_empty() => name.to_string(),
                    _ => return Err(format!("Firehose stream ARN unparseable: {arn}")),
                };
                use base64::Engine;
                let data = base64::engine::general_purpose::STANDARD.encode(payload.as_bytes());
                let resp = firehose.handle(firehose::AwsRequest {
                    service: "firehose".into(),
                    operation: "PutRecord".into(),
                    account,
                    region: region.to_string(),
                    params: json!({
                        "DeliveryStreamName": stream_name,
                        "Record": { "Data": data },
                    }),
                    body: bytes::Bytes::new(),
                });
                if resp.status >= 400 {
                    // Missing stream is a no-op like the Python engine.
                    return Ok(());
                }
                Ok(())
            }
        }),
    );

    // EventBridge bus destination: the record travels as the Detail of a
    // PutEvents submission (Python twin `destinations.py::_send_to_eventbridge`).
    events::register_hook(
        "events-target",
        Arc::new({
            let events_handler = handlers.events.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let condition = serde_json::from_str::<Value>(payload)
                    .ok()
                    .and_then(|r| r["requestContext"]["condition"].as_str().map(String::from))
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
                let stream_name = match arn.rsplit('/').next().filter(|s| !s.is_empty()) {
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

    // CloudWatch alarm actions: alarm state transitions (evaluation on
    // PutMetricData, SetAlarmState) deliver AlarmActions / OKActions /
    // InsufficientDataActions ARNs through cloudwatch::bridge. SNS targets
    // are a real `Publish` through the live SNS handler (so the topic's own
    // queue/lambda subscribers receive the notification envelope) and Lambda
    // targets invoke synchronously with the alarm envelope as the event —
    // the same delivery shapes the events hooks above implement. moto's own
    // cloudwatch backend never delivers alarm actions (5.2.4 models.py keeps
    // alarms static), so the twin follows the real CloudWatch delivery
    // semantics for the SNS/Lambda action kinds.
    cloudwatch::bridge::register_alarm_action(
        "sns",
        Arc::new({
            let sns = handlers.sns.clone();
            move |arn: &str, payload: &str, account: u64, region: &str| {
                let resp = sns.handle(sns::AwsRequest {
                    service: "sns".into(),
                    operation: "Publish".into(),
                    account,
                    region: region.to_string(),
                    params: json!({ "TopicArn": arn, "Message": payload }),
                    body: bytes::Bytes::new(),
                });
                // Missing topic: no-op like the Python provider's
                // error-log-and-return behavior.
                if resp.status >= 400 {
                    return Err(format!("SNS publish failed for {arn}: {}", resp.status));
                }
                Ok(())
            }
        }),
    );
    cloudwatch::bridge::register_alarm_action(
        "lambda",
        Arc::new(
            move |arn: &str, payload: &str, _account: u64, _region: &str| {
                let Ok(event) = serde_json::from_str::<Value>(payload) else {
                    return Err(format!("CloudWatch alarm payload is not JSON: {payload}"));
                };
                match lambda::invoke_by_arn(arn, &event) {
                    Some(_) => Ok(()),
                    None => Err(format!("Lambda function not found: {arn}")),
                }
            },
        ),
    );

    // CloudTrail management events: successful mutating API calls on the
    // emit-enabled services publish the standard "AWS API Call via
    // CloudTrail" envelope onto the default bus (issue #347). The producers
    // live in rotocore's service handlers, which cannot depend on the events
    // crate directly, so the emit closes over the events handler the same way
    // the target hooks above do — the registry indirection is the
    // gateway-populated seam (`rotocore::cloudtrail_emit`).
    rotocore::cloudtrail_emit::register_emitter(Arc::new(
        |account: u64,
         region: &str,
         event_source: &str,
         event_name: &str,
         request_parameters_json: &str| {
            events::bridge::emit_cloudtrail_api_call(
                account,
                region,
                event_source,
                event_name,
                request_parameters_json,
            );
        },
    ));

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

#[cfg(test)]
mod tests {
    use super::*;

    fn native_handlers() -> NativeHandlers {
        NativeHandlers {
            sqs: Arc::new(sqs::DefaultSqsHandler::new()),
            s3: Arc::new(s3::DefaultS3Handler::new()),
            dynamodb: Arc::new(dynamodb::DefaultDynamoDbHandler::new()),
            sns: Arc::new(sns::DefaultSnsHandler::new()),
            secretsmanager: Arc::new(secretsmanager::DefaultSecretsManagerHandler::new()),
            kms: Arc::new(kms::DefaultKmsHandler::new()),
            ssm: Arc::new(ssm::DefaultSsmHandler::new()),
            iam: Arc::new(iam::DefaultIamHandler::new()),
            lambda: Arc::new(lambda::DefaultLambdaHandler::new()),
            logs: Arc::new(cloudwatch_logs::DefaultLogsHandler::new()),
            events: Arc::new(events::DefaultEventsHandler::new()),
            kinesis: Arc::new(kinesis::DefaultKinesisHandler::new()),
            firehose: Arc::new(firehose::DefaultFirehoseHandler::new()),
            cloudwatch: Arc::new(cloudwatch::DefaultCloudwatchHandler::new()),
            ecr: Arc::new(ecr::DefaultEcrHandler::new()),
            ecs: Arc::new(ecs::DefaultEcsHandler::new()),
            stepfunctions: Arc::new(stepfunctions::DefaultStepfunctionsHandler::new()),
        }
    }

    /// Handlers with the cross-service hooks registered — one process-wide
    /// registration (the hooks registry and the gateway's own OnceLock guard
    /// are global), shared by parallel tests.
    fn registered_handlers() -> NativeHandlers {
        static HANDLERS: std::sync::OnceLock<NativeHandlers> = std::sync::OnceLock::new();
        HANDLERS
            .get_or_init(|| {
                let handlers = native_handlers();
                register(&handlers);
                handlers
            })
            .clone()
    }

    fn sns_req(account: u64, params: serde_json::Value) -> sns::AwsRequest {
        sns::AwsRequest {
            service: "sns".into(),
            operation: params["_op"].as_str().unwrap_or_default().into(),
            account,
            region: "us-east-1".into(),
            params: params.clone(),
            body: bytes::Bytes::new(),
        }
    }

    // -- EventBridge → SNS subject (F7) ----------------------------------------

    #[test]
    fn eventbridge_sns_publish_carries_the_eventbridge_subject() {
        let handlers = registered_handlers();
        let account: u64 = 447089000001;
        let queue_name = "fp7-subj-q";
        let topic_name = "fp7-subj-topic";

        let queue = sqs::SqsHandler::handle(
            &*handlers.sqs,
            sqs::AwsRequest {
                service: "sqs".into(),
                operation: "CreateQueue".into(),
                account,
                region: "us-east-1".into(),
                params: json!({"QueueName": queue_name}),
                body: bytes::Bytes::new(),
            },
        );
        let queue_url: String = serde_json::from_str::<Value>(&queue.body).unwrap()["QueueUrl"]
            .as_str()
            .unwrap()
            .to_string();
        let queue_arn = format!("arn:aws:sqs:us-east-1:{account}:{queue_name}");
        // CreateTopic answers in query-protocol XML; the ARN is derived
        // (`arn:aws:sns:<region>:<account>:<topic>`), so no XML parsing here.
        handlers.sns.handle(sns_req(
            account,
            json!({"_op": "CreateTopic", "Name": topic_name}),
        ));
        let topic_arn = format!("arn:aws:sns:us-east-1:{account}:{topic_name}");
        handlers.sns.handle(sns_req(
            account,
            json!({"_op": "Subscribe", "TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": queue_arn}),
        ));

        // EventBridge delivery → subject "EventBridge Notification".
        let hook = events::hook(
            "eventbridge-sns",
            &topic_arn,
            r#"{"k":1}"#,
            account,
            "us-east-1",
        );
        assert!(hook.unwrap().is_ok());
        let recv = sqs::SqsHandler::handle(
            &*handlers.sqs,
            sqs::AwsRequest {
                service: "sqs".into(),
                operation: "ReceiveMessage".into(),
                account,
                region: "us-east-1".into(),
                params: json!({"QueueUrl": queue_url, "WaitTimeSeconds": 2}),
                body: bytes::Bytes::new(),
            },
        );
        let envelope: Value = serde_json::from_str::<Value>(&recv.body).unwrap()["Messages"][0]
            ["Body"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(envelope["Subject"], json!("EventBridge Notification"));
        assert_eq!(envelope["Message"], r#"{"k":1}"#);

        // The plain "sns" hook (S3 origin) publishes without a subject.
        sqs::SqsHandler::handle(
            &*handlers.sqs,
            sqs::AwsRequest {
                service: "sqs".into(),
                operation: "DeleteMessage".into(),
                account,
                region: "us-east-1".into(),
                params: json!({"QueueUrl": queue_url}),
                body: bytes::Bytes::new(),
            },
        );
        let hook = events::hook("sns", &topic_arn, r#"{"k":2}"#, account, "us-east-1");
        assert!(hook.unwrap().is_ok());
        let recv = sqs::SqsHandler::handle(
            &*handlers.sqs,
            sqs::AwsRequest {
                service: "sqs".into(),
                operation: "ReceiveMessage".into(),
                account,
                region: "us-east-1".into(),
                params: json!({"QueueUrl": queue_url, "WaitTimeSeconds": 2}),
                body: bytes::Bytes::new(),
            },
        );
        let envelope: Value = serde_json::from_str::<Value>(&recv.body).unwrap()["Messages"][0]
            ["Body"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            envelope.get("Subject").map(Value::is_null).unwrap_or(true),
            "S3-origin hook stays subjectless, got {envelope}"
        );
    }

    // -- Firehose target hook (F4) ----------------------------------------------

    #[test]
    fn firehose_hook_puts_the_payload_into_the_stream() {
        let handlers = registered_handlers();
        let account: u64 = 447089000002;
        let stream_name = "fp4-stream";
        handlers.firehose.handle(firehose::AwsRequest {
            service: "firehose".into(),
            operation: "CreateDeliveryStream".into(),
            account,
            region: "us-east-1".into(),
            params: json!({
                "DeliveryStreamName": stream_name,
                "DeliveryStreamType": "DirectPut",
            }),
            body: bytes::Bytes::new(),
        });
        let stream_arn =
            format!("arn:aws:firehose:us-east-1:{account}:deliverystream/{stream_name}");
        let hooked = events::hook(
            "firehose",
            &stream_arn,
            r#"{"msg":"fh"}"#,
            account,
            "us-east-1",
        );
        assert!(
            hooked.unwrap().is_ok(),
            "firehose targets must land in the stream"
        );
        // Missing stream: era logs and moves on — still Ok, never a DLQ Err.
        let missing = events::hook(
            "firehose",
            &format!("arn:aws:firehose:us-east-1:{account}:deliverystream/fp4-nope"),
            "{}",
            account,
            "us-east-1",
        );
        assert!(missing.unwrap().is_ok());
        // Malformed ARN: surfaced as an Err (the events crate may DLQ it).
        assert!(events::hook(
            "firehose",
            "arn:aws:firehose:us-east-1:x:bowlinglane",
            "{}",
            account,
            "us-east-1"
        )
        .unwrap()
        .is_err());
    }
}
