//! Lambda async-invocation destinations.
//!
//! After an `InvocationType=Event` invoke completes, the configured
//! OnSuccess/OnFailure destination receives a destination record in AWS's
//! format (`requestContext` + `requestPayload` + `responseContext`), and a
//! failure additionally lands on the function's `DeadLetterConfig` target.
//! Mirrors the Python-era twin's `destinations.py::_build_destination_record`
//! and `provider.py::dispatch_to_dlq`.
//!
//! Delivery: SQS ARNs go into the SQS message bridge, SNS publishes through
//! the gateway-registered `events::bridge` hooks (`sns-result` with subject
//! "Lambda Invocation Result", `sns-dlq` with subject "Lambda DLQ"), Lambda
//! ARNs re-invoke via the lambda bridge, and EventBridge ARNs go through a
//! `PutEvents` hook (matching the Python twin's `put_events` shape).
//!
//! Python-era retry semantics (up to 2 re-executions with 1s/2s backoff) are
//! not re-executed here: the invocation has already failed once, so the
//! terminal delivery straight to OnFailure/DLQ preserves the pinned
//! observable behavior (record arrives shortly after the 202 with condition
//! "RetriesExhausted").

use crate::bridge;
use crate::models::LambdaFunction;
use crate::runtime;
use serde_json::{json, Value};

/// Build the AWS destination record for one invocation
/// (Python twin `destinations.py::_build_destination_record`).
fn build_record(
    function_arn: &str,
    request_payload: &Value,
    is_success: bool,
    result_payload: &Value,
    error: Option<&str>,
) -> Value {
    let mut response_context = json!({
        "statusCode": 200,
        "executedVersion": "$LATEST",
    });
    let response_payload = if is_success {
        result_json(result_payload)
    } else {
        response_context["functionError"] = json!(error.unwrap_or("Unhandled"));
        result_json(result_payload)
    };
    json!({
        "version": "1.0",
        "timestamp": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "requestContext": {
            "requestId": uuid::Uuid::new_v4().to_string(),
            "functionArn": function_arn,
            "condition": if is_success { "Success" } else { "RetriesExhausted" },
            "approximateInvokeCount": 1,
        },
        "requestPayload": request_payload,
        "responseContext": response_context,
        "responsePayload": response_payload,
    })
}

/// The result as JSON: a parseable non-null body is echoed; `null` (and
/// unparseable bodies) become `{"errorMessage": "Unknown"}` like the Python
/// twin's `result if result else {"errorMessage": error or "Unknown"}`.
fn result_json(result_payload: &Value) -> Value {
    match result_payload {
        Value::Null => json!({"errorMessage": "Unknown"}),
        other => other.clone(),
    }
}

/// The reduced DLQ record (Python twin `provider.py::dispatch_to_dlq`):
/// `requestContext` + `requestPayload` + top-level `errorMessage` — no
/// `version`/`timestamp`/`responseContext`.
fn build_dlq_record(function_arn: &str, request_payload: &Value, error: Option<&str>) -> Value {
    json!({
        "requestContext": {
            "requestId": uuid::Uuid::new_v4().to_string(),
            "functionArn": function_arn,
            "condition": "RetriesExhausted",
        },
        "requestPayload": request_payload,
        "errorMessage": error.unwrap_or("Unknown error"),
    })
}

/// Extract the error message the handler produced. The runtime already puts
/// the structured failure (`errorMessage` / `errorType`) into the payload
/// when the handler raised.
fn error_message_of(outcome: &runtime::ExecResult) -> String {
    let payload_error = serde_json::from_str::<Value>(&outcome.payload)
        .ok()
        .and_then(|v| {
            v.get("errorMessage")
                .and_then(Value::as_str)
                .map(String::from)
        });
    payload_error
        .or(outcome.function_error.clone())
        .unwrap_or_default()
}

/// Deliver a record to SQS / SNS / Lambda / EventBridge destinations.
pub fn dispatch_destination(dest_arn: &str, record: &Value, account: u64, region: &str) {
    let payload = serde_json::to_string(record).unwrap_or_default();
    if dest_arn.contains(":sqs:") {
        sqs::bridge::deliver_to_arn(dest_arn, &payload);
    } else if dest_arn.contains(":sns:") {
        let _ = events::hook("sns-result", dest_arn, &payload, account, region);
    } else if dest_arn.contains(":lambda:") {
        let _ = bridge::invoke_by_arn(dest_arn, record);
    } else if dest_arn.contains(":events:") || dest_arn.contains("event-bus") {
        let _ = events::hook("events-target", dest_arn, &payload, account, region);
    }
}

/// Deliver a reduced failure record to the function's DeadLetterConfig target
/// (Python twin `provider.py::dispatch_to_dlq`). A no-op when no target.
pub fn dispatch_to_dlq(
    func: &LambdaFunction,
    request_payload: &Value,
    outcome: &runtime::ExecResult,
    account: u64,
    region: &str,
) {
    let target_arn = func
        .dead_letter_config
        .read()
        .as_ref()
        .and_then(|d| d.get("TargetArn"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if target_arn.is_empty() {
        return;
    }
    let record =
        build_dlq_record(&func.function_arn, request_payload, Some(&dlq_error_message(outcome)));
    let payload = serde_json::to_string(&record).unwrap_or_default();
    if target_arn.contains(":sqs:") {
        sqs::bridge::deliver_to_arn(&target_arn, &payload);
    } else if target_arn.contains(":sns:") {
        let _ = events::hook("sns-dlq", &target_arn, &payload, account, region);
    }
}

/// The DLQ record's top-level `errorMessage` (`error or "Unknown error"`).
fn dlq_error_message(outcome: &runtime::ExecResult) -> String {
    let msg = error_message_of(outcome);
    if msg.is_empty() {
        "Unknown error".to_string()
    } else {
        msg
    }
}

/// After-async-completion fanout driven by the function's event invoke
/// config. On success the OnSuccess destination fires; on failure the
/// OnFailure destination and (unconditionally, like the Python twin) the DLQ.
pub fn dispatch_async_result(
    func: &LambdaFunction,
    request_payload: &Value,
    outcome: &runtime::ExecResult,
    account: u64,
    region: &str,
) {
    let is_success = outcome.function_error.is_none();
    let error_message = if is_success {
        None
    } else {
        let msg = error_message_of(outcome);
        Some(if msg.is_empty() { "Unhandled".to_string() } else { msg })
    };

    let dest = |key: &str| -> Option<String> {
        func.event_invoke_config
            .read()
            .as_ref()
            .and_then(|c| c.get("DestinationConfig"))
            .and_then(|d| d.get(key))
            .and_then(|k| k.get("Destination"))
            .and_then(|a| a.as_str())
            .filter(|a| !a.is_empty())
            .map(String::from)
    };

    if is_success {
        if let Some(dest_arn) = dest("OnSuccess") {
            let record = build_record(
                &func.function_arn,
                request_payload,
                true,
                &parse_payload(&outcome.payload),
                error_message.as_deref(),
            );
            dispatch_destination(&dest_arn, &record, account, region);
        }
        return;
    }

    if let Some(dest_arn) = dest("OnFailure") {
        let record = build_record(
            &func.function_arn,
            request_payload,
            false,
            &parse_payload(&outcome.payload),
            error_message.as_deref(),
        );
        dispatch_destination(&dest_arn, &record, account, region);
    }
    dispatch_to_dlq(func, request_payload, outcome, account, region);
}

/// The invocation result payload as a JSON value (`null` when unparseable).
fn parse_payload(payload: &str) -> Value {
    serde_json::from_str(payload).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn success_record_carries_aws_fields() {
        let record = build_record(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            &json!({"hello": "world", "num": 42}),
            true,
            &json!({"ok": true}),
            None,
        );
        assert_eq!(record["version"], "1.0");
        assert!(record["requestContext"]["functionArn"]
            .as_str()
            .unwrap()
            .ends_with(":function:fn"));
        assert_eq!(record["requestContext"]["condition"], "Success");
        assert_eq!(record["requestPayload"]["num"], 42);
        assert_eq!(record["responseContext"]["statusCode"], 200);
        assert_eq!(record["responseContext"]["executedVersion"], "$LATEST");
        assert_eq!(record["responsePayload"]["ok"], true);
    }

    #[test]
    fn failure_record_flags_the_error() {
        let record = build_record(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            &json!({}),
            false,
            &json!({"errorMessage": "value error"}),
            Some("value error"),
        );
        assert_eq!(record["requestContext"]["condition"], "RetriesExhausted");
        assert_eq!(record["responseContext"]["functionError"], "value error");
        assert_eq!(record["responsePayload"]["errorMessage"], "value error");
        assert_eq!(record["requestPayload"], json!({}));
    }

    #[test]
    fn failure_record_synthesizes_response_payload_when_none() {
        // Python twin: `result if result else {"errorMessage": ...}`.
        let with_error = build_record(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            &json!({}),
            false,
            &Value::Null,
            Some("Runtime.InvalidRuntime"),
        );
        assert_eq!(with_error["responsePayload"], json!({"errorMessage": "Unknown"}));
        let no_error = build_record(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            &json!({}),
            false,
            &Value::Null,
            None,
        );
        assert_eq!(no_error["responseContext"]["functionError"], "Unhandled");
    }

    #[test]
    fn dlq_record_is_reduced_with_top_level_error() {
        let record = build_dlq_record(
            "arn:aws:lambda:us-east-1:123456789012:function:dlq-fn",
            &json!({"trigger": "dlq-test"}),
            Some("intentional error"),
        );
        // Pinned by tests/compatibility/test_lambda_dlq_destinations_compat.py:
        // no version/timestamp/responseContext, condition always
        // RetriesExhausted, and the error sits at the top level.
        assert!(record.get("version").is_none());
        assert!(record.get("timestamp").is_none());
        assert!(record.get("responseContext").is_none());
        assert_eq!(record["requestContext"]["condition"], "RetriesExhausted");
        assert_eq!(
            record["requestContext"]["functionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:dlq-fn"
        );
        assert_eq!(record["requestPayload"], json!({"trigger": "dlq-test"}));
        assert_eq!(record["errorMessage"], "intentional error");
        assert!(record["errorMessage"].is_string());
    }

    #[test]
    fn dlq_without_error_uses_the_placeholder() {
        let record = build_dlq_record(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            &json!({}),
            None,
        );
        assert_eq!(record["errorMessage"], "Unknown error");
    }
}
