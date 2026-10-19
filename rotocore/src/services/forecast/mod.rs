//! Amazon Forecast backend (`forecast`, JSON protocol, target
//! `AmazonForecast.<Op>`).
//!
//! Port of moto's `forecast`: DatasetGroup CRUD with moto's validation
//! quirks — the name regex is a prefix match (only the first character is
//! constrained), names must stay under 64 characters, the domain must be
//! one of seven enum values, and dataset arns can never resolve because
//! moto exposes no `CreateDataset` (so `DatasetArns` on create always
//! fails with `InvalidInputException`; on update the requested arns must
//! already belong to the group). Dataset, predictor and the other
//! operations moto does not implement answer 501 `NotImplemented` with
//! the snake_case action name.

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Value};

/// The moto DatasetGroup model.
struct DatasetGroup {
    arn: String,
    name: String,
    domain: String,
    dataset_arns: Vec<String>,
    /// moto stores Tags on the model but no handler ever reads them back
    /// (list/describe omit them), so that asymmetry is preserved.
    #[allow(dead_code)]
    tags: Value,
    /// ISO-8601 seconds timestamp. moto's describe/list always render
    /// creation time for `LastModificationTime` (its `update` bumps an
    /// attribute no handler reads), so the value never moves.
    creation_date: String,
}

impl DatasetGroup {
    /// moto `DatasetGroup._validate` — each failed constraint appends a
    /// message, then all messages are joined into one ValidationException.
    fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if !self
            .name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic())
        {
            errors.push(format!(
                "Value '{}' at 'datasetGroupName' failed to satisfy constraint: Member must satisfy regular expression pattern {}",
                self.name, NAME_PATTERN
            ));
        }
        if self.name.chars().count() >= 64 {
            errors.push(format!(
                "Value '{}' at 'datasetGroupName' failed to satisfy constraint: Member must have length less than or equal to 63",
                self.name
            ));
        }
        if !ACCEPTED_TYPES.contains(&self.domain.as_str()) {
            errors.push(format!(
                "Value '{}' at 'domain' failed to satisfy constraint: Member must satisfy enum value set {}",
                self.domain, ACCEPTED_TYPES_REPR
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// moto `accepted_dataset_group_name_format.pattern` (verbatim string used
/// in validation messages; a prefix match, so only the first character is
/// constrained).
const NAME_PATTERN: &str = "^[a-zA-Z][a-z-A-Z0-9_]*";
/// moto `accepted_dataset_types`.
const ACCEPTED_TYPES: [&str; 7] = [
    "INVENTORY_PLANNING",
    "METRICS",
    "RETAIL",
    "EC2_CAPACITY",
    "CUSTOM",
    "WEB_TRAFFIC",
    "WORK_FORCE",
];
/// The python `str(list)` rendering moto interpolates into messages.
const ACCEPTED_TYPES_REPR: &str =
    "['INVENTORY_PLANNING', 'METRICS', 'RETAIL', 'EC2_CAPACITY', 'CUSTOM', 'WEB_TRAFFIC', 'WORK_FORCE']";

type State = Vec<DatasetGroup>;

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut State) -> T) -> T {
    let slot = store::slot::<State>("forecast");
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

fn validation_exception(errors: Vec<String>) -> Response {
    let noun = if errors.len() > 1 { "errors" } else { "error" };
    error_response(
        400,
        "ValidationException",
        &format!(
            "{} validation {} detected: {}",
            errors.len(),
            noun,
            errors.join("; ")
        ),
    )
}

fn ok_response(body: Value) -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: body.to_string().into_bytes(),
    }
}

/// moto's Delete/Update responses carry an empty 200 body (`""`).
fn empty_response() -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: Vec::new(),
    }
}

/// moto's NotImplementedError message renders the snake_case action name
/// (moto `camelcase_to_underscores(action)`).
fn snake(op: &str) -> String {
    let mut out = String::new();
    let mut prev_title = true;
    let chars: Vec<char> = op.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        let title = c.is_ascii_uppercase();
        let next_title = chars.get(i + 1).is_some_and(char::is_ascii_uppercase);
        if i > 0 && ((title && !next_title) || (title && !prev_title)) {
            out.push('_');
        }
        prev_title = title;
        out.extend(c.to_lowercase());
    }
    out
}

fn dataset_group_arn(account: u64, region: &str, name: &str) -> String {
    let partition = partition(region);
    format!("arn:{partition}:forecast:{region}:{account}:dataset-group/{name}")
}

/// moto `get_partition`.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// First string in a JSON list member (moto reports the first unresolvable
/// dataset arn in the error).
fn first_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key)
        .and_then(Value::as_array)
        .and_then(|list| list.first())
        .and_then(Value::as_str)
}

fn as_string_list(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn param_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or_default()
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "forecast" {
        return None;
    }
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
    };
    let op = req.operation.as_str();
    Some(match op {
        "CreateDatasetGroup" => create_dataset_group(req, &body),
        "DescribeDatasetGroup" => describe_dataset_group(req, &body),
        "DeleteDatasetGroup" => delete_dataset_group(req, &body),
        "UpdateDatasetGroup" => update_dataset_group(req, &body),
        "ListDatasetGroups" => list_dataset_groups(req),
        _ => not_implemented(op),
    })
}

fn not_implemented(op: &str) -> Response {
    error_response(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake(op)),
    )
}

fn not_found(arn: &str) -> Response {
    error_response(
        400,
        "ResourceNotFoundException",
        &format!("No resource found {arn}"),
    )
}

fn create_dataset_group(req: &Request, body: &Value) -> Response {
    let group = DatasetGroup {
        arn: dataset_group_arn(
            req.account,
            &req.region,
            param_str(body, "DatasetGroupName"),
        ),
        name: param_str(body, "DatasetGroupName").to_string(),
        domain: param_str(body, "Domain").to_string(),
        dataset_arns: as_string_list(body, "DatasetArns"),
        tags: body.get("Tags").cloned().unwrap_or(Value::Null),
        creation_date: now_ts(),
    };
    // moto validates the model before touching the backend.
    if let Err(errors) = group.validate() {
        return validation_exception(errors);
    }

    with_state(req.account, &req.region, |st| {
        // moto checks dataset arns against the backend's `datasets` map;
        // no moto operation ever populates that map, so any non-empty
        // DatasetArns list fails on its first member.
        if let Some(arn) = first_str(body, "DatasetArns") {
            return error_response(
                400,
                "InvalidInputException",
                &format!("Dataset arns: [{arn}] are not found"),
            );
        }
        if st.iter().any(|d| d.arn == group.arn) {
            return error_response(
                400,
                "ResourceAlreadyExistsException",
                &format!("A dataset group already exists with the arn: {}", group.arn),
            );
        }
        let arn = group.arn.clone();
        st.push(group);
        ok_response(json!({ "DatasetGroupArn": arn }))
    })
}

fn describe_dataset_group(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "DatasetGroupArn").to_string();
    with_state(req.account, &req.region, |st| {
        match st.iter().find(|d| d.arn == arn) {
            Some(d) => Response {
                status: 200,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({
                    "CreationTime": d.creation_date,
                    "DatasetArns": d.dataset_arns,
                    "DatasetGroupArn": d.arn,
                    "DatasetGroupName": d.name,
                    "Domain": d.domain,
                    "LastModificationTime": d.creation_date,
                    "Status": "ACTIVE",
                })
                .to_string()
                .into_bytes(),
            },
            None => not_found(&arn),
        }
    })
}

fn delete_dataset_group(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "DatasetGroupArn").to_string();
    with_state(req.account, &req.region, |st| {
        let before = st.len();
        st.retain(|d| d.arn != arn);
        if st.len() == before {
            not_found(&arn)
        } else {
            empty_response()
        }
    })
}

fn update_dataset_group(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "DatasetGroupArn").to_string();
    let dataset_arns = as_string_list(body, "DatasetArns");
    with_state(req.account, &req.region, |st| {
        let group = match st.iter_mut().find(|d| d.arn == arn) {
            Some(d) => d,
            None => return not_found(&arn),
        };
        // moto checks the requested arns against the group's current
        // members, not the backend: updates may only shrink the list.
        for member in &dataset_arns {
            if !group.dataset_arns.iter().any(|known| known == member) {
                return error_response(
                    400,
                    "InvalidInputException",
                    &format!("Dataset arns: [{member}] are not found"),
                );
            }
        }
        group.dataset_arns = dataset_arns;
        empty_response()
    })
}

fn list_dataset_groups(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let mut groups: Vec<Value> = st
            .iter()
            .map(|d| {
                json!({
                    "DatasetGroupArn": d.arn,
                    "DatasetGroupName": d.name,
                    "CreationTime": d.creation_date,
                    "LastModificationTime": d.creation_date,
                })
            })
            .collect();
        // moto sorts by LastModificationTime descending.
        groups.sort_by(|a, b| {
            b["LastModificationTime"]
                .as_str()
                .cmp(&a["LastModificationTime"].as_str())
        });
        ok_response(json!({ "DatasetGroups": groups }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn fmt_req(op: &str, body: Value) -> Request {
        Request {
            service: "forecast".into(),
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

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Unwrapping view of the module dispatch — every op in this service
    /// always answers a `Response`.
    fn dispatch(req: &Request) -> Response {
        super::dispatch(req).unwrap()
    }

    #[test]
    fn create_describe_list_round_trip() {
        let r = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-rt-1", "Domain": "CUSTOM"}),
        ));
        assert_eq!(r.status, 200);
        let v = body_of(&r);
        assert_eq!(
            v["DatasetGroupArn"],
            "arn:aws:forecast:us-east-1:482409018203:dataset-group/dsg-rt-1"
        );

        let d = dispatch(&fmt_req(
            "DescribeDatasetGroup",
            json!({"DatasetGroupArn": v["DatasetGroupArn"]}),
        ));
        assert_eq!(d.status, 200);
        let dv = body_of(&d);
        assert_eq!(dv["DatasetGroupName"], "dsg-rt-1");
        assert_eq!(dv["Domain"], "CUSTOM");
        assert_eq!(dv["Status"], "ACTIVE");
        assert_eq!(dv["DatasetArns"], json!([]));
        assert_eq!(dv["LastModificationTime"], dv["CreationTime"]);

        let l = dispatch(&fmt_req("ListDatasetGroups", json!({})));
        let lv = body_of(&l);
        let found = lv["DatasetGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["DatasetGroupName"] == "dsg-rt-1")
            .cloned()
            .unwrap();
        assert_eq!(found["DatasetGroupArn"], v["DatasetGroupArn"]);
        assert!(found["LastModificationTime"].as_str().is_some());

        let del = dispatch(&fmt_req(
            "DeleteDatasetGroup",
            json!({"DatasetGroupArn": v["DatasetGroupArn"]}),
        ));
        assert_eq!(del.status, 200);
        assert!(del.body.is_empty());

        let gone = dispatch(&fmt_req(
            "DescribeDatasetGroup",
            json!({"DatasetGroupArn": v["DatasetGroupArn"]}),
        ));
        assert_eq!(gone.status, 400);
        assert_eq!(body_of(&gone)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn duplicate_and_validation_errors() {
        dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-dup", "Domain": "RETAIL"}),
        ));
        let dup = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-dup", "Domain": "RETAIL"}),
        ));
        assert_eq!(dup.status, 400);
        let v = body_of(&dup);
        assert_eq!(v["__type"], "ResourceAlreadyExistsException");
        assert_eq!(
            v["message"],
            "A dataset group already exists with the arn: arn:aws:forecast:us-east-1:482409018203:dataset-group/dsg-dup"
        );

        let bad_name = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "9bad", "Domain": "CUSTOM"}),
        ));
        assert_eq!(body_of(&bad_name)["__type"], "ValidationException");
        assert!(body_of(&bad_name)["message"]
            .as_str()
            .unwrap()
            .contains("Member must satisfy regular expression pattern"));

        let long_name = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "a".repeat(65), "Domain": "CUSTOM"}),
        ));
        assert!(body_of(&long_name)["message"]
            .as_str()
            .unwrap()
            .contains("less than or equal to 63"));

        let bad_domain = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-x", "Domain": "BOGUS"}),
        ));
        assert!(body_of(&bad_domain)["message"]
            .as_str()
            .unwrap()
            .contains("enum value set"));

        let both_bad = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "9bad", "Domain": "BOGUS"}),
        ));
        assert!(body_of(&both_bad)["message"]
            .as_str()
            .unwrap()
            .starts_with("2 validation errors detected: "));
    }

    #[test]
    fn update_dataset_group_rules() {
        let created = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-upd", "Domain": "WORK_FORCE"}),
        ));
        let arn = body_of(&created)["DatasetGroupArn"].clone();

        // moto requires requested arns to already belong to the group.
        let bad = dispatch(&fmt_req(
            "UpdateDatasetGroup",
            json!({"DatasetGroupArn": arn, "DatasetArns": ["arn:aws:forecast:us-east-1:482409018203:dataset/nope"]}),
        ));
        assert_eq!(body_of(&bad)["__type"], "InvalidInputException");
        assert_eq!(
            body_of(&bad)["message"],
            "Dataset arns: [arn:aws:forecast:us-east-1:482409018203:dataset/nope] are not found"
        );

        let upd = dispatch(&fmt_req(
            "UpdateDatasetGroup",
            json!({"DatasetGroupArn": arn, "DatasetArns": []}),
        ));
        assert_eq!(upd.status, 200);

        let d = dispatch(&fmt_req(
            "DescribeDatasetGroup",
            json!({"DatasetGroupArn": arn}),
        ));
        assert_eq!(body_of(&d)["DatasetArns"], json!([]));
    }

    #[test]
    fn describe_delete_unknown_arn() {
        let arn = "arn:aws:forecast:us-east-1:482409018203:dataset-group/missing";
        let d = dispatch(&fmt_req(
            "DescribeDatasetGroup",
            json!({"DatasetGroupArn": arn}),
        ));
        assert_eq!(body_of(&d)["message"], format!("No resource found {arn}"));
        let del = dispatch(&fmt_req(
            "DeleteDatasetGroup",
            json!({"DatasetGroupArn": arn}),
        ));
        assert_eq!(del.status, 400);
        assert_eq!(body_of(&del)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn unknown_op_is_501_with_snake_name() {
        let r = dispatch(&fmt_req("CreateDataset", json!({"DatasetName": "x"})));
        assert_eq!(r.status, 501);
        let v = body_of(&r);
        assert_eq!(v["__type"], "NotImplemented");
        assert_eq!(
            v["message"],
            "The create_dataset action has not been implemented"
        );
        let snake_case = dispatch(&fmt_req("DeleteWhatIfForecastExport", json!({})));
        assert_eq!(
            body_of(&snake_case)["message"],
            "The delete_what_if_forecast_export action has not been implemented"
        );
    }
}
