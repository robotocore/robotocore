//! RDS Data API (`rdsdata`, rest-json protocol, `rds-data` signing scope).
//!
//! Port of moto's `moto/rdsdata`, which routes exactly one operation:
//! `POST /Execute` → `ExecuteStatement`. The handler performs no
//! validation on any input parameter (moto: "there is no validation yet")
//! and no SQL is ever executed — every statement returns the backend's
//! default `QueryResults(records=[])`, i.e. the five wire keys with
//! `records` empty and the other four null.
//!
//! moto's predefined-results queue (configured via the moto-api endpoint
//! `/moto-api/static/rds-data/statement-results`) is not portable —
//! rotocore has no moto-api surface (same call as `athena`/`ce`) — so the
//! queue is always empty and the default empty result always applies.
//!
//! Wire notes: botocore's `rds-data` model is rest-json, so the operation
//! comes from the request URI (`/Execute`, `/BatchExecute`,
//! `/BeginTransaction`, `/CommitTransaction`, `/RollbackTransaction`,
//! `/ExecuteSql`) rather than an `X-Amz-Target` header — the gateway
//! therefore hands us an empty `operation` and we derive it from the path.
//! moto routes none of those URIs except `/Execute` (double-evidenced in
//! both the published and fork moto checkouts), so the other five
//! operations answer the crate's verbatim `501 NotImplemented`.

use crate::{Request, Response};
use serde_json::{json, Value};

pub fn dispatch(req: &Request) -> Response {
    let op = derive_operation(req);
    match op.as_str() {
        "ExecuteStatement" => execute_statement(),
        // moto's rdsdata module routes only /Execute; these botocore
        // operations have no moto handler at all (no url_paths entry, no
        // response method) — answer NotImplemented verbatim.
        _ => not_implemented(&op),
    }
}

/// REST-JSON: the gateway's `operation` is empty for rds-data (no
/// X-Amz-Target, no Action); resolve from the request URI. The URI segment
/// for the two execute operations is shorter than the botocore op name.
fn derive_operation(req: &Request) -> String {
    if !req.operation.is_empty() {
        return req.operation.clone();
    }
    let segment = req
        .path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");
    match segment {
        "Execute" => "ExecuteStatement".to_string(),
        "BatchExecute" => "BatchExecuteStatement".to_string(),
        "" => "The rdsdata operation could not be determined".to_string(),
        other => other.to_string(),
    }
}

/// moto `RDSDataServiceBackend.execute_statement` with an empty results
/// queue: `QueryResults(records=[])` with every remaining field `None`.
fn execute_statement() -> Response {
    ok_response(json!({
        "records": [],
        "columnMetadata": null,
        "numberOfRecordsUpdated": null,
        "generatedFields": null,
        "formattedRecords": null,
    }))
}

fn ok_response(body: Value) -> Response {
    Response::json(200, body.to_string().into_bytes())
}

/// Matches the crate-wide NotImplemented shape (payload is the snake_case
/// action name, matching moto core's `NotImplementedError` message).
fn not_implemented(op: &str) -> Response {
    let message = format!("The {} action has not been implemented", snake(op));
    let body = json!({
        "__type": "NotImplemented",
        "message": message,
    });
    Response {
        status: 501,
        headers: vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            (
                "x-amzn-errortype".to_string(),
                "NotImplemented:".to_string(),
            ),
        ],
        body: body.to_string().into_bytes(),
    }
}

/// moto-style snake_case action name for NotImplemented messages.
fn snake(op: &str) -> String {
    let mut out = String::new();
    for (i, c) in op.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn r(op: &str, path: &str) -> (u16, Value) {
        let req = Request {
            service: "rdsdata".into(),
            operation: op.into(),
            method: "POST".into(),
            path: path.into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: crate::Bytes::from(
                serde_json::to_vec(&json!({
                    "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c1",
                    "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s",
                    "sql": "SELECT 1",
                }))
                .unwrap(),
            ),
            account: 123456789012,
            region: "us-east-1".into(),
        };
        let resp = dispatch(&req);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        (resp.status, body)
    }

    #[test]
    fn execute_statement_by_path() {
        let (status, body) = r("", "/Execute");
        assert_eq!(status, 200);
        assert_eq!(body["records"], serde_json::json!([]));
        assert_eq!(body["columnMetadata"], serde_json::Value::Null);
        assert_eq!(body["numberOfRecordsUpdated"], serde_json::Value::Null);
        assert_eq!(body["generatedFields"], serde_json::Value::Null);
        assert_eq!(body["formattedRecords"], serde_json::Value::Null);
        assert_eq!(
            body.as_object().unwrap().len(),
            5,
            "moto ships exactly these five keys"
        );
    }

    #[test]
    fn execute_statement_by_op_name() {
        let (status, body) = r("ExecuteStatement", "/");
        assert_eq!(status, 200);
        assert_eq!(body["records"], serde_json::json!([]));
    }

    #[test]
    fn execute_statement_ignores_input() {
        // moto validates nothing: an arbitrary/absent sql must still answer
        // the default empty result with 200.
        let req = Request {
            service: "rdsdata".into(),
            operation: String::new(),
            method: "POST".into(),
            path: "/Execute".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: crate::Bytes::from_static(b"{}"),
            account: 123456789012,
            region: "us-east-1".into(),
        };
        let resp = dispatch(&req);
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["records"], serde_json::json!([]));
    }

    #[test]
    fn unrouted_botocore_ops_answer_not_implemented() {
        for path in [
            "/BatchExecute",
            "/BeginTransaction",
            "/CommitTransaction",
            "/RollbackTransaction",
            "/ExecuteSql",
        ] {
            let (status, body) = r("", path);
            assert_eq!(status, 501, "{path}");
            assert_eq!(body["__type"], "NotImplemented", "{path}")
        }
    }

    #[test]
    fn unknown_path_answers_not_implemented() {
        let (status, body) = r("", "/NoSuchAction");
        assert_eq!(status, 501);
        assert_eq!(
            body["message"],
            format!(
                "The {} action has not been implemented",
                snake("NoSuchAction")
            )
        );
    }
}
