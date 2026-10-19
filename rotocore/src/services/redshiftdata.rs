//! Redshift Data API backend (`redshiftdata`, JSON REST protocol).
//!
//! Port of moto's `redshiftdata`: statements are tracked per
//! (account, region) with the same field defaults moto's `Statement` model
//! carries (status STARTED, RedshiftPid/RedshiftQueryId in 0..=99999,
//! ResultRows -1, ...). Wire shapes come straight from the moto response
//! dicts (`json.dumps`), so `DbUser`/`SecretArn` render as nulls and the
//! `GetStatementResult` payload is moto's static fixture.

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
struct Statement {
    id: String,
    cluster_identifier: String,
    created_at: String,
    database: String,
    db_user: Option<String>,
    duration: i64,
    has_result_set: bool,
    query_parameters: Value,
    query_string: String,
    redshift_pid: i64,
    redshift_query_id: i64,
    result_rows: i64,
    result_size: i64,
    secret_arn: Option<String>,
    status: String,
    sub_statements: Value,
    updated_at: String,
}

impl Statement {
    fn json(&self) -> Value {
        json!({
            "Id": self.id,
            "ClusterIdentifier": self.cluster_identifier,
            "CreatedAt": self.created_at,
            "Database": self.database,
            "DbUser": self.db_user,
            "Duration": self.duration,
            "HasResultSet": self.has_result_set,
            "QueryParameters": self.query_parameters,
            "QueryString": self.query_string,
            "RedshiftPid": self.redshift_pid,
            "RedshiftQueryId": self.redshift_query_id,
            "ResultRows": self.result_rows,
            "ResultSize": self.result_size,
            "SecretArn": self.secret_arn,
            "Status": self.status,
            "SubStatements": self.sub_statements,
            "UpdatedAt": self.updated_at,
        })
    }
}

#[derive(Default)]
struct RedshiftDataState {
    statements: BTreeMap<String, Statement>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut RedshiftDataState) -> T) -> T {
    let slot = store::slot::<RedshiftDataState>("redshiftdata");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

/// Wire seconds timestamp (moto `iso_8601_datetime_without_milliseconds`).
fn now_ts() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn error_response(status: u16, code: &str, message: &str) -> Response {
    let body = json!({ "__type": code, "message": message }).to_string();
    Response {
        status,
        headers: vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("x-amzn-errortype".to_string(), format!("{code}:")),
        ],
        body: body.into_bytes(),
    }
}

fn ok_response(body: Value) -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: body.to_string().into_bytes(),
    }
}

fn validation_error() -> Response {
    error_response(
        400,
        "ValidationException",
        "id must satisfy regex pattern: ^[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}(:\\d+)?$",
    )
}

/// moto `_validate_uuid` — ids must match the uuid4 wire pattern (an
/// optional `:pid` suffix is allowed).
fn id_valid(id: &str) -> bool {
    let base = id.split(':').next().unwrap_or(id);
    let bytes = base.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, c) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
        };
        if !ok {
            return false;
        }
    }
    true
}

pub fn dispatch(req: &Request) -> Response {
    let op = req.operation.as_str();
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
    };
    match op {
        "CancelStatement" => cancel_statement(req, &body),
        "DescribeStatement" => describe_statement(req, &body),
        "ExecuteStatement" => execute_statement(req, &body),
        "GetStatementResult" => get_statement_result(req, &body),
        _ => error_response(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", snake(op)),
        ),
    }
}

/// moto's NotImplementedError message uses the snake_case action name.
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

fn js(v: &Value, k: &str) -> Option<String> {
    v.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn execute_statement(req: &Request, body: &Value) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_ts();
    let statement = Statement {
        id: id.clone(),
        cluster_identifier: js(body, "ClusterIdentifier").unwrap_or_default(),
        created_at: now.clone(),
        database: js(body, "Database").unwrap_or_default(),
        db_user: js(body, "DbUser"),
        duration: 0,
        has_result_set: false,
        query_parameters: body.get("Parameters").cloned().unwrap_or(json!([])),
        query_string: js(body, "Sql").unwrap_or_default(),
        redshift_pid: rand_int(),
        redshift_query_id: rand_int(),
        result_rows: -1,
        result_size: -1,
        secret_arn: js(body, "SecretArn"),
        status: "STARTED".to_string(),
        sub_statements: json!([]),
        updated_at: now,
    };
    with_state(req.account, &req.region, |st| {
        st.statements.insert(id.clone(), statement.clone());
    });
    ok_response(json!({
        "ClusterIdentifier": statement.cluster_identifier,
        "CreatedAt": statement.created_at,
        "Database": statement.database,
        "DbUser": statement.db_user,
        "Id": statement.id,
        "SecretArn": statement.secret_arn,
    }))
}

fn rand_int() -> i64 {
    (uuid::Uuid::new_v4().as_u128() % 100_000) as i64
}

fn describe_statement(req: &Request, body: &Value) -> Response {
    let Some(id) = js(body, "Id") else {
        return validation_error();
    };
    if !id_valid(&id) {
        return validation_error();
    }
    with_state(req.account, &req.region, |st| {
        match st.statements.get(&id) {
            Some(statement) => ok_response(statement.json()),
            None => error_response(400, "ResourceNotFoundException", "Query does not exist."),
        }
    })
}

fn cancel_statement(req: &Request, body: &Value) -> Response {
    let Some(id) = js(body, "Id") else {
        return validation_error();
    };
    if !id_valid(&id) {
        return validation_error();
    }
    with_state(req.account, &req.region, |st| {
        match st.statements.get_mut(&id) {
            Some(statement) if statement.status == "STARTED" => {
                statement.status = "ABORTED".to_string();
                ok_response(json!({"Status": true}))
            }
            Some(statement) => error_response(
                400,
                "ValidationException",
                &format!(
                    "Could not cancel a query that is already in {} state with ID: {}",
                    statement.status, id
                ),
            ),
            None => error_response(400, "ResourceNotFoundException", "Query does not exist."),
        }
    })
}

/// moto's static statement-result fixture — the SQL result cannot be
/// mocked without a live redshift, so the fixture stands in.
fn get_statement_result(req: &Request, body: &Value) -> Response {
    let Some(id) = js(body, "Id") else {
        return validation_error();
    };
    if !id_valid(&id) {
        return validation_error();
    }
    let exists = with_state(req.account, &req.region, |st| {
        st.statements.contains_key(&id)
    });
    if !exists {
        return error_response(400, "ResourceNotFoundException", "Query does not exist.");
    }
    ok_response(json!({
        "ColumnMetadata": [
            {"columnDefault": null, "isCaseSensitive": false, "isSigned": true, "name": "Number", "nullable": false},
            {"columnDefault": null, "isCaseSensitive": true, "isSigned": false, "name": "Street", "nullable": false},
            {"columnDefault": null, "isCaseSensitive": true, "isSigned": false, "name": "City", "nullable": false},
        ],
        "Records": [
            [{"longValue": 10}, {"stringValue": "Alpha st"}, {"stringValue": "Vancouver"}],
            [{"longValue": 50}, {"stringValue": "Beta st"}, {"stringValue": "Toronto"}],
            [{"longValue": 100}, {"stringValue": "Gamma av"}, {"stringValue": "Seattle"}],
        ],
        "TotalNumberRows": 3,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;
    use std::collections::HashMap;

    fn fmt_req(op: &str, body: Value) -> Request {
        Request {
            service: "redshiftdata".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: crate::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 482409018203,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn execute_then_describe_round_trip() {
        let r = dispatch(&fmt_req(
            "ExecuteStatement",
            json!({"ClusterIdentifier": "c1", "Database": "dev", "Sql": "SELECT 1"}),
        ));
        assert_eq!(r.status, 200);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["Database"], "dev");
        assert_eq!(v["ClusterIdentifier"], "c1");
        assert!(v["Id"].as_str().is_some());
        assert!(v["DbUser"].is_null());

        let describe = dispatch(&fmt_req("DescribeStatement", json!({"Id": v["Id"]})));
        assert_eq!(describe.status, 200);
        let d: Value = serde_json::from_slice(&describe.body).unwrap();
        assert_eq!(d["Id"], v["Id"]);
        assert_eq!(d["Status"], "STARTED");
    }

    #[test]
    fn describe_statement_not_found() {
        let r = dispatch(&fmt_req(
            "DescribeStatement",
            json!({"Id": "deadbeef-0123-4567-8901-deadbeef4321"}),
        ));
        assert_eq!(r.status, 400);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn describe_statement_rejects_bad_uuid() {
        let r = dispatch(&fmt_req("DescribeStatement", json!({"Id": "not-a-uuid"})));
        assert_eq!(r.status, 400);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["__type"], "ValidationException");
    }

    #[test]
    fn cancel_statement_aborts_started() {
        let r = dispatch(&fmt_req(
            "ExecuteStatement",
            json!({"ClusterIdentifier": "c1", "Database": "dev", "Sql": "SELECT 2"}),
        ));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        let cancel = dispatch(&fmt_req("CancelStatement", json!({"Id": v["Id"]})));
        assert_eq!(cancel.status, 200);
        let c: Value = serde_json::from_slice(&cancel.body).unwrap();
        assert_eq!(c["Status"], true);

        let second = dispatch(&fmt_req("CancelStatement", json!({"Id": v["Id"]})));
        assert_eq!(second.status, 400);
        let s: Value = serde_json::from_slice(&second.body).unwrap();
        assert!(s["message"]
            .as_str()
            .unwrap()
            .contains("Could not cancel a query that is already in ABORTED state"));
    }

    #[test]
    fn get_statement_result_static_fixture() {
        let r = dispatch(&fmt_req(
            "ExecuteStatement",
            json!({"ClusterIdentifier": "c1", "Database": "dev", "Sql": "SELECT 3", "Parameters": [{"name": "a", "value": "b"}]}),
        ));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        let res = dispatch(&fmt_req("GetStatementResult", json!({"Id": v["Id"]})));
        assert_eq!(res.status, 200);
        let out: Value = serde_json::from_slice(&res.body).unwrap();
        assert_eq!(out["TotalNumberRows"], 3);
        assert_eq!(out["Records"][0][0]["longValue"], 10);
    }

    #[test]
    fn unknown_op_is_501() {
        let r = dispatch(&fmt_req("BatchExecuteStatement", json!({})));
        assert_eq!(r.status, 501);
    }
}
