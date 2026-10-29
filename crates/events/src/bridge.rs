//! Foreign-target dispatch registry.
//!
//! EventBridge rules can target services whose handlers live in other crates
//! (Lambda, SNS, Kinesis, Step Functions, CloudWatch Logs). Wiring those
//! crates directly would create dependency cycles (S3 -> events -> lambda ->
//! S3), so this module holds a small process-global registry the gateway
//! populates at startup; the events provider routes non-SQS (and non-bus)
//! target ARNs through it. Missing registration means "call / dispatch is
//! not possible in this process" — callers treat that as a simulated no-op,
//! exactly like the Python provider's log-only target types.

use chrono::SecondsFormat;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Hook signature: `(target_arn, payload, account, region)`.
///
/// `payload` is the (already transformed) string the target would receive
/// with `event` being the full EventBridge event it was built from. The
/// hook returns `Err` when the target itself could not be reached (missing
/// resource, failed start) — the dispatcher turns that into a DLQ record;
/// invocation-time handler errors happen inside the target and are not DLQ
/// conditions.
pub type HookFn = Arc<dyn Fn(&str, &str, u64, &str) -> Result<(), String> + Send + Sync>;

fn hooks() -> &'static Lazy<parking_lot::RwLock<HashMap<String, HookFn>>> {
    static HOOKS: Lazy<parking_lot::RwLock<HashMap<String, HookFn>>> =
        Lazy::new(|| parking_lot::RwLock::new(HashMap::new()));
    &HOOKS
}

/// Register the implementation for a target type ("lambda", "sns", ...).
pub fn register_hook(kind: &str, hook: HookFn) {
    hooks().write().insert(kind.to_string(), hook);
}

/// Invoke the registered hook for `kind`. `None` means no implementation is
/// registered (unit tests, or a target type the gateway does not wire).
pub fn hook(
    kind: &str,
    arn: &str,
    payload: &str,
    account: u64,
    region: &str,
) -> Option<Result<(), String>> {
    let hook = hooks().read().get(kind).cloned()?;
    Some(hook(arn, payload, account, region))
}

/// Convenience: build the full-EB-event payload for a Lambda target.
pub fn event_payload(event: &Value) -> String {
    serde_json::to_string(event).unwrap_or_default()
}

/// Publish the standard CloudTrail management-event envelope for one
/// *succeeded* mutating API call onto the default bus of `(account, region)`,
/// so rules like `detail-type = "AWS API Call via CloudTrail"`,
/// `eventSource = elasticloadbalancing.amazonaws.com`,
/// `eventName = DeregisterTargets` fire the way they do on real AWS
/// (issue #347: the LaunchDarkly streamer drain flow). moto does not emit
/// these events at all (moto#8451 covers only S3 CreateBucket) — the twin
/// exceeds moto on purpose so real-SDK user flows run unmodified.
///
/// `request_parameters_json` is the call's actual request parameters,
/// serialised with the lower-camel member names real CloudTrail records use
/// (`targetGroupArn`, `targets[].id`, `minSize`, ...); each emit site
/// constructs it from the fields it actually parsed. Fire-and-forget: an
/// unparseable payload publishes an empty `requestParameters`, no matching
/// rule is a no-op, and nothing here can fail the API call.
pub fn emit_cloudtrail_api_call(
    account: u64,
    region: &str,
    event_source: &str,
    event_name: &str,
    request_parameters_json: &str,
) {
    let now = chrono::Utc::now();
    let time = now.to_rfc3339_opts(SecondsFormat::Secs, true);
    let account_id = account.to_string();
    let request_parameters =
        serde_json::from_str(request_parameters_json).unwrap_or_else(|_| json!({}));
    // The twin's documented credentials are the account id (access key =
    // 12-digit account), so the skeleton's accessKeyId mirrors the caller.
    let event = json!({
        "version": "0",
        "id": uuid::Uuid::new_v4().to_string(),
        "detail-type": "AWS API Call via CloudTrail",
        "source": event_source,
        "account": account_id.clone(),
        "time": time.clone(),
        "region": region,
        // CloudTrail API-call records carry an empty resource list; the
        // filter surface is `detail.requestParameters`.
        "resources": [],
        "detail": {
            "eventVersion": "1.08",
            // Tenant identity skeleton: one AssumedRole session per account,
            // stable across calls so `userIdentity` stays a usable filter key.
            "userIdentity": {
                "type": "AssumedRole",
                "principalId": "AROAROBOCOREEXAMP:robotocore",
                "arn": format!("arn:aws:sts::{account_id}:assumed-role/robotocore/robotocore"),
                "accountId": account_id.clone(),
                "accessKeyId": account_id.clone(),
                "sessionContext": {
                    "attributes": {
                        "mfaAuthenticated": "false",
                        "creationDate": time.clone(),
                    },
                    "sessionIssuer": {
                        "type": "Role",
                        "principalId": "AROAROBOCOREEXAMP",
                        "arn": format!("arn:aws:iam::{account_id}:role/robotocore"),
                        "accountId": account_id.clone(),
                        "userName": "robotocore",
                    },
                },
            },
            "eventTime": time,
            "eventSource": event_source,
            "eventName": event_name,
            "awsRegion": region,
            // The call's actual request parameters, reshaped by the emit site
            // into the camel-case shape AWS sends (nested objects and
            // arrays-of-objects so detail paths like
            // `requestParameters.targets[0].id` resolve like real events).
            "requestParameters": request_parameters,
            "eventID": uuid::Uuid::new_v4().to_string(),
            "eventType": "AwsApiCall",
            "recipientAccountId": account_id,
        },
    });
    crate::publish_event_to_bus(&event, account, region);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hook_roundtrip_and_none_when_absent() {
        register_hook(
            "lambda",
            Arc::new(|arn, payload, _account, _region| {
                if payload == "boom" {
                    Err(format!("broken: {arn}"))
                } else {
                    Ok(())
                }
            }),
        );
        assert!(hook("lambda", "arn:aws:lambda:x", "ok", 1, "r")
            .unwrap()
            .is_ok());
        assert!(hook("lambda", "arn:aws:lambda:x", "boom", 1, "r")
            .unwrap()
            .is_err());
        assert!(hook("unknown", "arn:x", "", 1, "r").is_none());
    }

    #[test]
    fn event_payload_serialises_the_event() {
        let p = event_payload(&json!({"source": "t"}));
        assert!(p.contains("\"source\":\"t\""));
    }

    // -- CloudTrail API-call emitter -------------------------------------------
    //
    // Isolation: the events state and the sqs bridge pending buffer are
    // process-global but keyed (account, region); every test below uses the
    // dedicated 447089063401 scope nobody else touches, and clears the queue
    // buffer before asserting.

    const CT_ACCOUNT: u64 = 447089063401;
    const CT_REGION: &str = "us-east-1";

    fn events_req(operation: &str, params: Value) -> crate::AwsRequest {
        crate::AwsRequest {
            service: "events".to_string(),
            operation: operation.to_string(),
            account: CT_ACCOUNT,
            region: CT_REGION.to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    fn drain_trail_queue(name: &str) -> Vec<Value> {
        let mut queue = sqs::models::Queue::new(
            name.to_string(),
            CT_REGION.to_string(),
            CT_ACCOUNT.to_string(),
            format!("http://localhost:4566/{CT_ACCOUNT}/{name}"),
            format!("arn:aws:sqs:{CT_REGION}:{CT_ACCOUNT}:{name}"),
        );
        sqs::bridge::drain_pending(CT_ACCOUNT, CT_REGION, name, &mut queue);
        queue
            .messages
            .iter()
            .map(|m| serde_json::from_str(&m.body).unwrap_or(Value::Null))
            .collect()
    }

    #[test]
    fn eventbridge_cloudtrail_rule_fires_on_deregister_targets() {
        // Walk the real pipeline: rule on the default bus → SQS target → the
        // bridge emitter → the rule fires and the target receives the
        // standard envelope (issue #347's DeregisterTargets drain rule).
        let handler = crate::handler::EventsHandler::new();
        let queue = "ct-dereg-q";
        // Isolation: the bridge pending buffer is process-global.
        sqs::bridge::clear_pending(CT_ACCOUNT, CT_REGION, queue);
        let rule = "ct-dereg-rule";
        handler.handle(events_req(
            "PutRule",
            json!({
                "Name": rule,
                // LaunchDarkly-style verifier pattern: detail-type + source +
                // eventName, plus a nested requestParameters member filter.
                "EventPattern": json!({
                    "detail-type": ["AWS API Call via CloudTrail"],
                    "source": ["elasticloadbalancing.amazonaws.com"],
                    "detail": {
                        "eventName": ["DeregisterTargets"],
                        "requestParameters": {"targetGroupArn": [
                            "arn:aws:elasticloadbalancing:us-east-1:447089063401:targetgroup/streamer-tg/abc"
                        ]}
                    },
                })
                .to_string(),
            }),
        ));
        handler.handle(events_req(
            "PutTargets",
            json!({
                "Rule": rule,
                "Targets": [{
                    "Id": "q",
                    "Arn": format!("arn:aws:sqs:{}:{}:{}", CT_REGION, CT_ACCOUNT, queue),
                }],
            }),
        ));

        emit_cloudtrail_api_call(
            CT_ACCOUNT,
            CT_REGION,
            "elasticloadbalancing.amazonaws.com",
            "DeregisterTargets",
            r#"{"targetGroupArn":"arn:aws:elasticloadbalancing:us-east-1:447089063401:targetgroup/streamer-tg/abc","targets":[{"id":"i-0abc"}]}"#,
        );
        let records = drain_trail_queue(queue);
        assert_eq!(records.len(), 1, "the rule fires for DeregisterTargets");
        let event = &records[0];
        assert_eq!(event["detail-type"], "AWS API Call via CloudTrail");
        assert_eq!(event["source"], "elasticloadbalancing.amazonaws.com");
        assert_eq!(event["account"], CT_ACCOUNT.to_string());
        assert_eq!(event["region"], CT_REGION);
        let detail = &event["detail"];
        assert_eq!(detail["eventVersion"], "1.08");
        assert_eq!(detail["eventName"], "DeregisterTargets");
        assert_eq!(detail["eventSource"], "elasticloadbalancing.amazonaws.com");
        assert_eq!(detail["awsRegion"], CT_REGION);
        assert_eq!(detail["eventType"], "AwsApiCall");
        assert_eq!(detail["recipientAccountId"], CT_ACCOUNT.to_string());
        assert_eq!(
            detail["requestParameters"]["targetGroupArn"],
            "arn:aws:elasticloadbalancing:us-east-1:447089063401:targetgroup/streamer-tg/abc"
        );
        assert_eq!(detail["requestParameters"]["targets"][0]["id"], "i-0abc");
        assert!(
            !detail["eventID"].as_str().unwrap_or_default().is_empty(),
            "eventID must be minted"
        );
        assert!(!event["time"].as_str().unwrap_or_default().is_empty());
        let identity = &detail["userIdentity"];
        assert_eq!(identity["type"], "AssumedRole");
        assert_eq!(identity["accountId"], CT_ACCOUNT.to_string());
        assert_eq!(identity["accessKeyId"], CT_ACCOUNT.to_string());
        assert_eq!(identity["sessionContext"]["sessionIssuer"]["type"], "Role");

        // A different event name on the same source must NOT fire the rule.
        emit_cloudtrail_api_call(
            CT_ACCOUNT,
            CT_REGION,
            "elasticloadbalancing.amazonaws.com",
            "RegisterTargets",
            r#"{"targetGroupArn":"arn:aws:elasticloadbalancing:us-east-1:447089063401:targetgroup/streamer-tg/abc","targets":[{"id":"i-0abc"}]}"#,
        );
        assert_eq!(
            drain_trail_queue(queue).len(),
            0,
            "RegisterTargets does not match the DeregisterTargets rule"
        );

        // Cleanup: remove the targets so nothing dangles in the shared state.
        handler.handle(events_req(
            "RemoveTargets",
            json!({"Rule": rule, "Ids": ["q"]}),
        ));
        handler.handle(events_req("DeleteRule", json!({"Name": rule})));
    }

    #[test]
    fn eventbridge_cloudtrail_rule_requirements_hold() {
        // The arrays-of-objects requestParameters path (rule filters
        // `targets[0].id`) and the malformed-payload no-crash guarantee.
        let handler = crate::handler::EventsHandler::new();
        let queue = "ct-tg-paths-q";
        sqs::bridge::clear_pending(CT_ACCOUNT, CT_REGION, queue);
        let rule = "ct-tg-paths";
        handler.handle(events_req(
            "PutRule",
            json!({
                "Name": rule,
                "EventPattern": json!({
                    "detail-type": ["AWS API Call via CloudTrail"],
                    "source": ["elasticloadbalancing.amazonaws.com"],
                    "detail": {"eventName": ["DeregisterTargets"],
                               "requestParameters": {"targets": {"id": ["i-drainme"]}}}
                })
                .to_string(),
            }),
        ));
        handler.handle(events_req(
            "PutTargets",
            json!({
                "Rule": rule,
                "Targets": [{
                    "Id": "q",
                    "Arn": format!("arn:aws:sqs:{}:{}:{}", CT_REGION, CT_ACCOUNT, queue),
                }],
            }),
        ));
        // Per-element match (targets[0].id) resolves like real events.
        emit_cloudtrail_api_call(
            CT_ACCOUNT,
            CT_REGION,
            "elasticloadbalancing.amazonaws.com",
            "DeregisterTargets",
            r#"{"targetGroupArn":"arn:aws:elasticloadbalancing:us-east-1:447089063401:targetgroup/tg/xyz","targets":[{"id":"i-drainme","port":80}]}"#,
        );
        let records = drain_trail_queue(queue);
        assert_eq!(
            records.len(),
            1,
            "the arrays-of-objects requestParameters member must resolve per element"
        );
        assert_eq!(
            records[0]["detail"]["requestParameters"]["targets"][0]["port"], 80,
            "numeric members stay numeric like real CloudTrail records"
        );

        // Unparseable request parameters must not panic the emit path and
        // still publish (empty requestParameters) — the envelope stays
        // fire-and-forget for its callers.
        emit_cloudtrail_api_call(
            CT_ACCOUNT,
            CT_REGION,
            "elasticloadbalancing.amazonaws.com",
            "DeregisterTargets",
            "not-json-at-all",
        );
        // (No assertion on queue growth: this emit does not match the rule —
        // it just must not crash.)

        handler.handle(events_req(
            "RemoveTargets",
            json!({"Rule": rule, "Ids": ["q"]}),
        ));
        handler.handle(events_req("DeleteRule", json!({"Name": rule})));
    }
}
