//! Smithy RPC v2 CBOR wire layer (`smithy-protocol: rpc-v2-cbor`).
//!
//! aws-sdk-go-v2 (and so Terraform's AWS provider) and the AWS SDK for .NET
//! v4 call CloudWatch as `POST /service/GraniteServiceVersion20100801/
//! operation/{Operation}` with a CBOR body. The handler speaks AWS JSON 1.0,
//! so requests are decoded into the same params a JSON body yields and
//! responses are re-encoded from the handler's JSON.
//!
//! The two protocols differ on timestamps (JSON: epoch-seconds numbers or, as
//! the handler emits them, RFC 3339 strings; CBOR: tag 1 around an epoch),
//! blobs (base64 strings vs byte strings) and number types (SDKs decode CBOR
//! doubles and integers by major type). Requests convert generically; the
//! response side follows each operation's output shape from botocore's model,
//! generated into `cbor_shapes.json` by `tooling/gen_cloudwatch_cbor_shapes.py`.
//!
//! CloudWatch is `awsQueryCompatible`, so an error names its model shape in
//! `__type` (SDKs pick the exception class from it) and carries the query
//! error code the handler raised in `x-amzn-query-error`.

use std::collections::HashMap;
use std::sync::OnceLock;

use base64::Engine as _;
use ciborium::value::{Integer, Value as Cbor};
use serde::Deserialize;
use serde_json::{Map, Number, Value};

use crate::protocol::AwsResponse;

/// `smithy-protocol` header value for this protocol.
pub const SMITHY_PROTOCOL: &str = "rpc-v2-cbor";

/// CBOR tag for an epoch-based date/time (RFC 8949 §3.4.2).
const TAG_EPOCH: u64 = 1;

/// Smithy namespace of the CloudWatch error shapes (`__type` shape ids).
const SHAPE_NAMESPACE: &str = "com.amazonaws.cloudwatch";

/// One node of an operation's output shape (see the generator's docstring).
#[derive(Debug, Deserialize)]
struct Node {
    t: String,
    #[serde(default)]
    m: HashMap<String, Node>,
    e: Option<Box<Node>>,
    v: Option<Box<Node>>,
}

#[derive(Debug, Deserialize)]
struct ErrorShape {
    code: String,
    sender: bool,
}

#[derive(Debug, Deserialize)]
struct Shapes {
    ops: HashMap<String, Node>,
    errors: HashMap<String, ErrorShape>,
    op_errors: HashMap<String, Vec<String>>,
}

fn shapes() -> &'static Shapes {
    static SHAPES: OnceLock<Shapes> = OnceLock::new();
    SHAPES.get_or_init(|| {
        serde_json::from_str(include_str!("cbor_shapes.json"))
            .expect("cbor_shapes.json is generated and valid")
    })
}

fn output_shape(operation: &str) -> Option<&'static Node> {
    shapes().ops.get(operation)
}

/// The model error shape for a query error code: the one the operation
/// declares, else the only shape with that code. `None` for codes the model
/// doesn't have (the handler's `ValidationError`, ...).
fn error_shape(operation: &str, code: &str) -> Option<(&'static str, &'static ErrorShape)> {
    let s = shapes();
    let has_code = |name: &&String| s.errors.get(name.as_str()).is_some_and(|e| e.code == code);
    let declared = s
        .op_errors
        .get(operation)
        .and_then(|names| names.iter().find(has_code));
    let name = declared.or_else(|| {
        let mut all = s.errors.keys().filter(has_code);
        match (all.next(), all.next()) {
            (Some(only), None) => Some(only),
            _ => None,
        }
    })?;
    Some((name.as_str(), &s.errors[name.as_str()]))
}

/// Response re-encoded for the RPC v2 CBOR wire.
#[derive(Debug, Clone)]
pub struct CborResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// True when the request headers (any case) select RPC v2 CBOR.
pub fn is_rpcv2_cbor<'a>(mut headers: impl Iterator<Item = (&'a str, &'a str)>) -> bool {
    headers.any(|(k, v)| {
        k.eq_ignore_ascii_case("smithy-protocol") && v.trim().eq_ignore_ascii_case(SMITHY_PROTOCOL)
    })
}

/// Decode a CBOR request body into JSON-protocol params. An empty body is an
/// empty input structure; anything other than a top-level map is an error.
pub fn decode_request(body: &[u8]) -> Result<Value, String> {
    if body.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    let cbor: Cbor = ciborium::de::from_reader(body).map_err(|e| e.to_string())?;
    match cbor_to_json(cbor) {
        v @ Value::Object(_) => Ok(v),
        _ => Err("the request body is not a CBOR map".to_string()),
    }
}

fn cbor_to_json(v: Cbor) -> Value {
    match v {
        Cbor::Null => Value::Null,
        Cbor::Bool(b) => Value::Bool(b),
        Cbor::Integer(i) => {
            let i = i128::from(i);
            if let Ok(n) = i64::try_from(i) {
                Value::from(n)
            } else if let Ok(n) = u64::try_from(i) {
                Value::from(n)
            } else {
                Value::from(i as f64)
            }
        }
        Cbor::Float(f) => float_to_json(f),
        Cbor::Text(s) => Value::String(s),
        Cbor::Bytes(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        // Tag 1 wraps epoch seconds, which is what the JSON wire sends for a
        // timestamp; other tags carry no meaning for these shapes.
        Cbor::Tag(_, inner) => cbor_to_json(*inner),
        Cbor::Array(items) => Value::Array(items.into_iter().map(cbor_to_json).collect()),
        Cbor::Map(entries) => Value::Object(
            entries
                .into_iter()
                .filter_map(|(k, v)| match k {
                    Cbor::Text(k) => Some((k, cbor_to_json(v))),
                    _ => None,
                })
                .collect(),
        ),
        _ => Value::Null,
    }
}

/// JSON has no NaN or infinities; the AWS JSON protocols send them as strings.
fn float_to_json(f: f64) -> Value {
    match Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None if f.is_nan() => Value::String("NaN".to_string()),
        None if f > 0.0 => Value::String("Infinity".to_string()),
        None => Value::String("-Infinity".to_string()),
    }
}

/// Re-encode a handler response for the CBOR wire. Errors become
/// `{"__type", "message"}` maps (the RPC v2 error shape) plus
/// `x-amzn-query-error`; success bodies are converted against the operation's
/// output shape.
pub fn encode_response(operation: &str, resp: AwsResponse) -> CborResponse {
    let json: Value = if resp.body.trim().is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_str(&resp.body).unwrap_or_else(|_| Value::Object(Map::new()))
    };
    let mut headers: Vec<(String, String)> = resp
        .headers
        .into_iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("content-type"))
        .collect();
    let cbor = if resp.status >= 400 {
        let code = json
            .get("__type")
            .and_then(Value::as_str)
            .map(|t| t.rsplit('#').next().unwrap_or(t))
            .unwrap_or("InternalFailure");
        let message = json
            .get("message")
            .or_else(|| json.get("Message"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let (type_name, sender) = match error_shape(operation, code) {
            Some((shape, e)) => (format!("{SHAPE_NAMESPACE}#{shape}"), e.sender),
            None => (code.to_string(), resp.status < 500),
        };
        let fault = if sender { "Sender" } else { "Receiver" };
        headers.push(("x-amzn-query-error".to_string(), format!("{code};{fault}")));
        Cbor::Map(vec![
            (Cbor::Text("__type".into()), Cbor::Text(type_name)),
            (Cbor::Text("message".into()), Cbor::Text(message.into())),
        ])
    } else {
        json_to_cbor(&json, output_shape(operation))
    };
    let mut body = Vec::new();
    ciborium::ser::into_writer(&cbor, &mut body).expect("CBOR encoding into a Vec cannot fail");

    headers.push(("Content-Type".to_string(), "application/cbor".to_string()));
    headers.push(("smithy-protocol".to_string(), SMITHY_PROTOCOL.to_string()));
    CborResponse {
        status: resp.status,
        headers,
        body,
    }
}

fn json_to_cbor(v: &Value, shape: Option<&Node>) -> Cbor {
    let t = shape.map(|n| n.t.as_str()).unwrap_or("");
    match v {
        Value::Null => Cbor::Null,
        Value::Bool(b) => Cbor::Bool(*b),
        Value::String(s) => match t {
            "timestamp" => parse_timestamp(s)
                .map(epoch_tag)
                .unwrap_or_else(|| Cbor::Text(s.clone())),
            "blob" => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map(Cbor::Bytes)
                .unwrap_or_else(|_| Cbor::Text(s.clone())),
            "double" | "float" => match s.as_str() {
                "NaN" => Cbor::Float(f64::NAN),
                "Infinity" => Cbor::Float(f64::INFINITY),
                "-Infinity" => Cbor::Float(f64::NEG_INFINITY),
                _ => Cbor::Text(s.clone()),
            },
            _ => Cbor::Text(s.clone()),
        },
        Value::Number(n) => match t {
            "timestamp" => n.as_f64().map(epoch_tag).unwrap_or(Cbor::Null),
            "double" | "float" => Cbor::Float(n.as_f64().unwrap_or(0.0)),
            "integer" | "long" => match (n.as_i64(), n.as_u64(), n.as_f64()) {
                (Some(i), _, _) => Cbor::Integer(Integer::from(i)),
                (_, Some(u), _) => Cbor::Integer(Integer::from(u)),
                (_, _, Some(f)) if f.fract() == 0.0 && f.abs() < 9.0e18 => {
                    Cbor::Integer(Integer::from(f as i64))
                }
                (_, _, f) => Cbor::Float(f.unwrap_or(0.0)),
            },
            _ => number_to_cbor(n),
        },
        Value::Array(items) => {
            let elem = shape.filter(|n| n.t == "list").and_then(|n| n.e.as_deref());
            Cbor::Array(items.iter().map(|i| json_to_cbor(i, elem)).collect())
        }
        Value::Object(obj) => {
            let is_map = shape.is_some_and(|n| n.t == "map");
            let (members, value) = match shape {
                Some(n) if n.t == "structure" => (Some(&n.m), None),
                Some(n) if is_map => (None, n.v.as_deref()),
                _ => (None, None),
            };
            Cbor::Map(
                obj.iter()
                    // Objects outside a modelled map are structures, where a
                    // null member means "absent"; leave it out rather than
                    // send a CBOR null the SDK must special-case.
                    .filter(|(_, v)| is_map || !v.is_null())
                    .map(|(k, v)| {
                        let child = members.and_then(|m| m.get(k)).or(value);
                        (Cbor::Text(k.clone()), json_to_cbor(v, child))
                    })
                    .collect(),
            )
        }
    }
}

fn number_to_cbor(n: &Number) -> Cbor {
    if let Some(i) = n.as_i64() {
        Cbor::Integer(Integer::from(i))
    } else if let Some(u) = n.as_u64() {
        Cbor::Integer(Integer::from(u))
    } else {
        Cbor::Float(n.as_f64().unwrap_or(0.0))
    }
}

fn epoch_tag(seconds: f64) -> Cbor {
    Cbor::Tag(TAG_EPOCH, Box::new(Cbor::Float(seconds)))
}

/// RFC 3339 (what the handler emits) or a bare epoch-seconds string.
fn parse_timestamp(s: &str) -> Option<f64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis() as f64 / 1000.0);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_millis() as f64 / 1000.0);
    }
    s.parse::<f64>().ok().filter(|f| f.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &Cbor) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::ser::into_writer(v, &mut out).unwrap();
        out
    }

    fn decode(b: &[u8]) -> Cbor {
        ciborium::de::from_reader(b).unwrap()
    }

    fn text(s: &str) -> Cbor {
        Cbor::Text(s.to_string())
    }

    fn get<'a>(m: &'a Cbor, key: &str) -> &'a Cbor {
        match m {
            Cbor::Map(entries) => entries
                .iter()
                .find(|(k, _)| k == &text(key))
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("missing {key}")),
            _ => panic!("not a map: {m:?}"),
        }
    }

    fn ok(body: Value) -> AwsResponse {
        AwsResponse::json(200, body)
    }

    #[test]
    fn detects_protocol_header_any_case() {
        assert!(is_rpcv2_cbor(
            [("Smithy-Protocol", "rpc-v2-cbor")].into_iter()
        ));
        assert!(is_rpcv2_cbor(
            [("smithy-protocol", " RPC-V2-CBOR ")].into_iter()
        ));
        assert!(!is_rpcv2_cbor(
            [("content-type", "application/cbor")].into_iter()
        ));
    }

    #[test]
    fn decode_empty_body_and_empty_map() {
        assert_eq!(decode_request(b"").unwrap(), json!({}));
        assert_eq!(decode_request(&[0xa0]).unwrap(), json!({}));
    }

    #[test]
    fn decode_rejects_non_map_and_garbage() {
        assert!(decode_request(&encode(&Cbor::Array(vec![]))).is_err());
        assert!(decode_request(&[0xff, 0x00]).is_err());
    }

    #[test]
    fn decode_timestamps_blobs_and_numbers() {
        let body = Cbor::Map(vec![
            (text("Namespace"), text("App")),
            (
                text("MetricData"),
                Cbor::Array(vec![Cbor::Map(vec![
                    (text("MetricName"), text("Latency")),
                    (text("Value"), Cbor::Float(1.5)),
                    (text("StorageResolution"), Cbor::Integer(60.into())),
                    (
                        text("Timestamp"),
                        Cbor::Tag(1, Box::new(Cbor::Float(1_793_361_600.25))),
                    ),
                ])]),
            ),
            (text("Blob"), Cbor::Bytes(b"hi".to_vec())),
            (text("Inf"), Cbor::Float(f64::INFINITY)),
        ]);
        let v = decode_request(&encode(&body)).unwrap();
        assert_eq!(
            v,
            json!({
                "Namespace": "App",
                "MetricData": [{
                    "MetricName": "Latency",
                    "Value": 1.5,
                    "StorageResolution": 60,
                    "Timestamp": 1_793_361_600.25
                }],
                "Blob": "aGk=",
                "Inf": "Infinity"
            })
        );
    }

    #[test]
    fn decode_indefinite_length_map() {
        // {_ "Namespace": "A"} with an indefinite-length map header (0xbf).
        let mut body = vec![0xbf, 0x69];
        body.extend_from_slice(b"Namespace");
        body.extend_from_slice(&[0x61, b'A', 0xff]);
        assert_eq!(decode_request(&body).unwrap(), json!({ "Namespace": "A" }));
    }

    #[test]
    fn encode_sets_protocol_headers() {
        let r = encode_response("ListMetrics", ok(json!({ "Metrics": [] })));
        assert_eq!(r.status, 200);
        let has = |k: &str, v: &str| r.headers.iter().any(|(hk, hv)| hk == k && hv == v);
        assert!(has("Content-Type", "application/cbor"));
        assert!(has("smithy-protocol", "rpc-v2-cbor"));
        assert!(!r
            .headers
            .iter()
            .any(|(_, v)| v == "application/x-amz-json-1.0"));
        assert!(r.headers.iter().any(|(k, _)| k == "x-amzn-RequestId"));
    }

    #[test]
    fn encode_drops_null_structure_members() {
        let r = encode_response(
            "ListMetrics",
            ok(json!({ "Metrics": [], "NextToken": null })),
        );
        assert_eq!(
            decode(&r.body),
            Cbor::Map(vec![(text("Metrics"), Cbor::Array(vec![]))])
        );
    }

    #[test]
    fn encode_metric_data_timestamps_and_doubles() {
        let r = encode_response(
            "GetMetricData",
            ok(json!({
                "MetricDataResults": [{
                    "Id": "m1",
                    "Label": "Latency",
                    "Timestamps": ["2026-10-30T12:00:00+00:00"],
                    "Values": [3],
                    "StatusCode": "Complete"
                }],
                "Messages": []
            })),
        );
        let out = decode(&r.body);
        let result = match get(&out, "MetricDataResults") {
            Cbor::Array(a) => &a[0],
            other => panic!("{other:?}"),
        };
        assert_eq!(
            get(result, "Timestamps"),
            &Cbor::Array(vec![Cbor::Tag(1, Box::new(Cbor::Float(1_793_361_600.0)))])
        );
        // An integral JSON number in a double list still goes out as a float.
        assert_eq!(get(result, "Values"), &Cbor::Array(vec![Cbor::Float(3.0)]));
        assert_eq!(get(result, "Id"), &text("m1"));
    }

    #[test]
    fn encode_alarm_shape_types() {
        let r = encode_response(
            "DescribeAlarms",
            ok(json!({
                "MetricAlarms": [{
                    "AlarmName": "a",
                    "ActionsEnabled": true,
                    "Threshold": 90,
                    "Period": 60.0,
                    "EvaluationPeriods": 1,
                    "StateUpdatedTimestamp": "2026-10-30T12:00:00.500Z",
                    "Dimensions": [{ "Name": "d", "Value": "v" }]
                }],
                "CompositeAlarms": []
            })),
        );
        let out = decode(&r.body);
        let alarm = match get(&out, "MetricAlarms") {
            Cbor::Array(a) => &a[0],
            other => panic!("{other:?}"),
        };
        assert_eq!(get(alarm, "Threshold"), &Cbor::Float(90.0));
        assert_eq!(get(alarm, "Period"), &Cbor::Integer(60.into()));
        assert_eq!(get(alarm, "EvaluationPeriods"), &Cbor::Integer(1.into()));
        assert_eq!(get(alarm, "ActionsEnabled"), &Cbor::Bool(true));
        assert_eq!(
            get(alarm, "StateUpdatedTimestamp"),
            &Cbor::Tag(1, Box::new(Cbor::Float(1_793_361_600.5)))
        );
        assert_eq!(
            get(alarm, "Dimensions"),
            &Cbor::Array(vec![Cbor::Map(vec![
                (text("Name"), text("d")),
                (text("Value"), text("v")),
            ])])
        );
    }

    #[test]
    fn encode_blob_as_byte_string() {
        let r = encode_response(
            "GetMetricWidgetImage",
            ok(json!({ "MetricWidgetImage": "aGk=" })),
        );
        assert_eq!(
            decode(&r.body),
            Cbor::Map(vec![(
                text("MetricWidgetImage"),
                Cbor::Bytes(b"hi".to_vec())
            )])
        );
    }

    #[test]
    fn encode_unknown_operation_passes_through() {
        let r = encode_response(
            "PutDashboard",
            ok(json!({ "DashboardValidationMessages": [] })),
        );
        assert_eq!(
            decode(&r.body),
            Cbor::Map(vec![(
                text("DashboardValidationMessages"),
                Cbor::Array(vec![])
            )])
        );
    }

    #[test]
    fn encode_error_envelope() {
        let r = encode_response(
            "GetDashboard",
            AwsResponse::error(404, "ResourceNotFound", "Dashboard does not exist"),
        );
        assert_eq!(r.status, 404);
        assert_eq!(
            decode(&r.body),
            Cbor::Map(vec![
                (
                    text("__type"),
                    text("com.amazonaws.cloudwatch#DashboardNotFoundError")
                ),
                (text("message"), text("Dashboard does not exist")),
            ])
        );
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "x-amzn-query-error" && v == "ResourceNotFound;Sender"));
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "smithy-protocol" && v == SMITHY_PROTOCOL));
    }

    #[test]
    fn error_shape_follows_the_operation() {
        // `ResourceNotFound` is the code of two shapes; the operation decides.
        assert_eq!(
            error_shape("GetDashboard", "ResourceNotFound").map(|(n, _)| n),
            Some("DashboardNotFoundError")
        );
        assert_eq!(
            error_shape("DeleteAlarms", "ResourceNotFound").map(|(n, _)| n),
            Some("ResourceNotFound")
        );
        // Undeclared by the operation but unique in the model.
        assert_eq!(
            error_shape("ListMetrics", "InvalidParameterCombination").map(|(n, _)| n),
            Some("InvalidParameterCombinationException")
        );
        // Ambiguous and undeclared, or not modelled at all.
        assert!(error_shape("ListMetrics", "ResourceNotFound").is_none());
        assert!(error_shape("ListMetrics", "ValidationError").is_none());
    }

    #[test]
    fn unmodelled_error_code_keeps_the_code() {
        let r = encode_response(
            "ListMetrics",
            AwsResponse::error(400, "ValidationError", "bad"),
        );
        assert_eq!(get(&decode(&r.body), "__type"), &text("ValidationError"));
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "x-amzn-query-error" && v == "ValidationError;Sender"));
        let r = encode_response(
            "ListMetrics",
            AwsResponse::error(500, "InternalServiceError", "boom"),
        );
        assert_eq!(
            get(&decode(&r.body), "__type"),
            &text("com.amazonaws.cloudwatch#InternalServiceFault")
        );
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "x-amzn-query-error" && v == "InternalServiceError;Receiver"));
    }

    #[test]
    fn every_generated_shape_is_well_formed() {
        fn check(n: &Node) {
            match n.t.as_str() {
                "structure" => n.m.values().for_each(check),
                "list" => check(n.e.as_deref().expect("list has e")),
                "map" => check(n.v.as_deref().expect("map has v")),
                "timestamp" | "blob" | "double" | "float" | "integer" | "long" => {}
                other => panic!("unexpected node type {other}"),
            }
        }
        assert!(output_shape("GetMetricStatistics").is_some());
        for op in ["DescribeAlarms", "GetMetricData", "ListDashboards"] {
            check(output_shape(op).unwrap());
        }
    }

    #[test]
    fn an_unparseable_body_answers_the_stable_serialization_exception_message() {
        use crate::protocol::AwsRequest;
        use crate::DefaultCloudwatchHandler;
        // Constructed without `new()` on purpose: `new` auto-registers the
        // process-global kinesis-ingest handler, and a second registrant
        // here would race the bridge.rs sole-registrant test.
        let handler = DefaultCloudwatchHandler {
            inner: std::sync::Arc::new(crate::handler::CloudwatchHandler::new()),
        };
        let resp = handler.handle_cbor(AwsRequest {
            service: "cloudwatch".to_string(),
            operation: "ListMetrics".to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params: serde_json::Value::Null,
            // Valid CBOR, but not a map — decode_request rejects it.
            body: bytes::Bytes::from_static(&[0x03]),
        });
        assert_eq!(resp.status, 400);
        let envelope = decode(&resp.body);
        assert_eq!(
            get(&envelope, "__type"),
            // SerializationException is a protocol framework exception, not a
            // CloudWatch model shape: error_shape does not resolve it, so the
            // fallback emits the bare code.
            &text("SerializationException")
        );
        assert_eq!(
            get(&envelope, "message"),
            &text("Unable to parse the CBOR request body")
        );
    }
}
