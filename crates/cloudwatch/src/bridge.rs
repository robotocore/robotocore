//! Cross-service metric ingest.
//!
//! moto's Kinesis backend pushes its stream metrics straight into the
//! CloudWatch backend of the same account/region (`moto/kinesis/models.py
//! ::KinesisBackend._send_stream_metrics` calls `cloudwatch_backends[..]
//! .put_metric_data(...)`). The twin's crates stay acyclic — the kinesis
//! crate cannot depend on this one — so the live handler registers here
//! when it is constructed (first registration wins, mirroring the
//! events-hook rule in `src/core/cross_service.rs`) and other crates push
//! datapoints through [`ingest_kinesis_put_metrics`]. Without a handler in
//! the process the ingest is a no-op (unit tests without CloudWatch).

use parking_lot::RwLock;
use serde_json::json;
use std::sync::{Arc, OnceLock};

use crate::handler::CloudwatchHandler;
use crate::protocol::{AwsRequest, AwsResponse};

fn registered() -> &'static RwLock<Option<Arc<CloudwatchHandler>>> {
    static REGISTRY: OnceLock<RwLock<Option<Arc<CloudwatchHandler>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(None))
}

/// Register the live handler for cross-service pushes. The first
/// registration binds: the gateway constructs its serving handler at
/// startup, and later throwaway instances must not redirect the ingest
/// (same rule as the events-hook registry).
pub fn register_handler(handler: &Arc<CloudwatchHandler>) {
    let mut reg = registered().write();
    if reg.is_none() {
        *reg = Some(Arc::clone(handler));
    }
}

/// Push one datapoint set into the live handler the way a real PutMetricData
/// call would, answering the handler's response (the ingest never fails).
fn put_metric_data(req: AwsRequest) -> AwsResponse {
    let handler = registered().read().clone();
    match handler {
        Some(handler) => handler.handle(req),
        None => AwsResponse::json(200, json!({})),
    }
}

/// Emit the Kinesis producer metrics for one put_record / put_records call,
/// mirroring moto's `_send_stream_metrics`: namespace `AWS/Kinesis`, one
/// `IncomingBytes` datapoint (Unit Bytes) and one `IncomingRecords` datapoint
/// (Unit Count), both dimensioned by StreamName only. moto sends one
/// aggregated datapoint pair per call — `put_records` does NOT fan out per
/// shard. Fire-and-forget: a missing/cold handler drops the datapoints and
/// never fails the Kinesis put.
pub fn ingest_kinesis_put_metrics(
    account: u64,
    region: &str,
    stream_name: &str,
    incoming_bytes: u64,
    incoming_records: u64,
) {
    let dimensions = json!([{ "Name": "StreamName", "Value": stream_name }]);
    let _ = put_metric_data(AwsRequest {
        service: "cloudwatch".to_string(),
        operation: "PutMetricData".to_string(),
        account,
        region: region.to_string(),
        params: json!({
            "Namespace": "AWS/Kinesis",
            "MetricData": [
                {
                    "MetricName": "IncomingBytes",
                    "Value": incoming_bytes,
                    "Unit": "Bytes",
                    "Dimensions": dimensions,
                },
                {
                    "MetricName": "IncomingRecords",
                    "Value": incoming_records,
                    "Unit": "Count",
                    "Dimensions": dimensions,
                },
            ],
        }),
        body: bytes::Bytes::new(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn cw_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "cloudwatch".to_string(),
            operation: operation.to_string(),
            account: 447089000551,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    fn parse(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    #[test]
    fn kinesis_ingest_lands_in_the_cloudwatch_store() {
        // The registration binds to the first handler in the process; using
        // the shared handler keeps the datapoints visible to this test.
        let handler = Arc::new(CloudwatchHandler::new());
        register_handler(&handler);

        // moto's exact datapoint pair: namespace AWS/Kinesis, Units
        // Bytes/Count, StreamName-only dimensions.
        ingest_kinesis_put_metrics(447089000551, "us-east-1", "cw-bridge-stream", 15, 2);

        let list =
            parse(&handler.handle(cw_req("ListMetrics", json!({"Namespace": "AWS/Kinesis"}))));
        let metrics = list["Metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        let names: Vec<&str> = metrics
            .iter()
            .map(|m| m["MetricName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"IncomingBytes"), "got {metrics:?}");
        assert!(names.contains(&"IncomingRecords"), "got {metrics:?}");
        for m in metrics {
            assert_eq!(m["Namespace"], json!("AWS/Kinesis"));
            let dims = m["Dimensions"].as_array().unwrap();
            assert_eq!(dims.len(), 1);
            assert_eq!(dims[0]["Name"], json!("StreamName"));
            assert_eq!(dims[0]["Value"], json!("cw-bridge-stream"));
        }

        // StartTime/EndTime are read as epoch milliseconds by the handler.
        let start = chrono::Utc::now().timestamp_millis() - 3_600_000;
        let end = chrono::Utc::now().timestamp_millis() + 3_600_000;
        let stats = parse(&handler.handle(cw_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "AWS/Kinesis",
                "MetricName": "IncomingBytes",
                "Statistics": ["Sum", "SampleCount"],
                "Period": 60,
                "StartTime": start,
                "EndTime": end,
            }),
        )));
        let datapoints = stats["Datapoints"].as_array().unwrap();
        assert_eq!(datapoints.len(), 1);
        assert_eq!(datapoints[0]["Sum"], json!(15.0));
        assert_eq!(datapoints[0]["SampleCount"], json!(1.0));
        assert_eq!(datapoints[0]["Unit"], json!("Bytes"));

        let stats = parse(&handler.handle(cw_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "AWS/Kinesis",
                "MetricName": "IncomingRecords",
                "Statistics": ["Sum", "SampleCount"],
                "Period": 60,
                "StartTime": start,
                "EndTime": end,
            }),
        )));
        let datapoints = stats["Datapoints"].as_array().unwrap();
        assert_eq!(datapoints[0]["Sum"], json!(2.0));
        assert_eq!(datapoints[0]["Unit"], json!("Count"));

        // Per-account scoping: another account sees none of it.
        let other = parse(&handler.handle(AwsRequest {
            account: 447089000552,
            ..cw_req(
                "GetMetricStatistics",
                json!({
                    "Namespace": "AWS/Kinesis",
                    "MetricName": "IncomingBytes",
                    "Statistics": ["Sum"],
                    "Period": 60,
                    "StartTime": start,
                    "EndTime": end,
                }),
            )
        }));
        assert_eq!(other["Datapoints"].as_array().unwrap().len(), 0);
    }
}
