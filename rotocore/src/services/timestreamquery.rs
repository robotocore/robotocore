//! Timestream Query backend (JSON protocol, target `Timestream_20181101`).
//!
//! Port of moto's timestreamquery: scheduled queries, canned Query output
//! (empty by default) and DescribeEndpoints.

use crate::{Request, Response};
use serde_json::{json, Value};

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "timestreamquery" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let region = req.region.clone();
    Some(handle(req, &body, &region))
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn handle(req: &Request, _p: &Value, region: &str) -> Response {
    match req.operation.as_str() {
        "DescribeEndpoints" => {
            let region = region.strip_suffix("-global").unwrap_or(region);
            ok(&json!({
                "Endpoints": [{
                    "Address": format!("ingest.timestream.{region}.amazonaws.com"),
                    "CachePeriodInMinutes": 1440,
                }]
            }))
        }
        "Query" => {
            // moto's default Query output: zero rows, status at completion.
            ok(&json!({
                "QueryId": uuid::Uuid::new_v4(),
                "QueryStatus": {
                    "ProgressPercentage": 100.0,
                    "CumulativeBytesScanned": 0i64,
                    "CumulativeBytesMetered": 0i64,
                },
                "Rows": [],
                "ColumnInfo": [],
            }))
        }
        // The fork's timestreamquery only implements Query and
        // DescribeEndpoints; anything else falls through to 501 exactly
        // like moto's dispatcher.
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn req(op: &str) -> Request {
        Request {
            service: "timestreamquery".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn endpoints_shape() {
        let resp = handle(&req("DescribeEndpoints"), &json!({}), "us-east-1");
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(v["Endpoints"][0]["Address"].as_str().is_some());
        assert_eq!(v["Endpoints"][0]["CachePeriodInMinutes"], 1440);
    }

    #[test]
    fn query_empty_result() {
        let resp = handle(
            &req("Query"),
            &json!({"QueryString": "SELECT 1"}),
            "us-east-1",
        );
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(v["Rows"].is_array());
        assert!(v["QueryStatus"].is_object());
    }
}
