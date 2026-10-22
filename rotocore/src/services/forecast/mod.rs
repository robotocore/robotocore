//! Amazon Forecast backend (`forecast`, JSON protocol, target
//! `AmazonForecast.<Op>`).
//!
//! Port of moto's `forecast` dataset-group machinery, extended to the full
//! botocore surface (moto implements only the dataset-group family — every
//! other Create/Describe/Delete/List + the tag/stop/tree operations answer
//! modeled botocore envelopes here): Dataset, DatasetImportJob, Predictor,
//! AutoPredictor, Forecast, Monitor, Explainability, ExplainabilityExport,
//! ForecastExportJob, PredictorBacktestExportJob, WhatIf families, plus
//! GetAccuracyMetrics, StopResource, ResumeResource, DeleteResourceTree and
//! the tag store.
//!
//! moto's validation quirks are kept where moto exists: the DatasetGroup
//! name regex is a prefix match (only the first character is constrained),
//! names must stay under 64 characters, the domain must be one of seven
//! enum values, and dataset arns can never resolve because moto exposes no
//! `CreateDataset` (so `DatasetArns` on create always fails with
//! `InvalidInputException`; on update the requested arns must already belong
//! to the group). Delete/Update responses carry the empty 200 body and the
//! NotImplementedError message renders the snake_case action name — the
//! same as moto.

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

/// Per-(account, region) state: the typed DatasetGroup family (moto's
/// model) plus every modeled Value record family keyed by its botocore ARN
/// member, and the tag store.
#[derive(Default)]
struct State {
    groups: Vec<DatasetGroup>,
    datasets: Vec<Value>,
    import_jobs: Vec<Value>,
    predictors: Vec<Value>,
    auto_predictors: Vec<Value>,
    forecasts: Vec<Value>,
    monitors: Vec<Value>,
    explainabilities: Vec<Value>,
    explainability_exports: Vec<Value>,
    forecast_exports: Vec<Value>,
    backtest_exports: Vec<Value>,
    what_if_analyses: Vec<Value>,
    what_if_forecasts: Vec<Value>,
    what_if_forecast_exports: Vec<Value>,
    /// arn -> {Key: Value} tag rows for any forecast resource.
    tags: Vec<(String, Value)>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut State) -> T) -> T {
    let slot = store::slot::<State>("forecast");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

/// The family-agnostic record builder: identity keys + every listed member
/// echoed from the create body verbatim + Status/Created-at fields. Stored
/// records double as the Describe wire bodies (moto has no models here; the
/// botocore shape is the authority).
fn record(
    arn_key: &str,
    arn: &str,
    name_key: &str,
    name: &str,
    echo: &[&str],
    body: &Value,
    extra: &[(&str, Value)],
) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(arn_key.to_string(), json!(arn));
    map.insert(name_key.to_string(), json!(name));
    for key in echo {
        if let Some(v) = body.get(*key) {
            map.insert(key.to_string(), v.clone());
        }
    }
    map.insert("Status".to_string(), json!("ACTIVE"));
    map.insert("CreationTime".to_string(), json!(now_ts()));
    map.insert("LastModificationTime".to_string(), json!(now_ts()));
    for (key, v) in extra {
        map.insert(key.to_string(), v.clone());
    }
    Value::Object(map)
}

/// The List<Family> summary body: identity + status + timestamps + every
/// listed extra key copied from the stored record.
fn summary(row: &Value, arn_key: &str, name_key: &str, extra: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(arn_key.to_string(), row[arn_key].clone());
    map.insert(name_key.to_string(), row[name_key].clone());
    for key in extra {
        if let Some(v) = row.get(*key) {
            map.insert(key.to_string(), v.clone());
        }
    }
    map.insert("Status".to_string(), row["Status"].clone());
    map.insert("CreationTime".to_string(), row["CreationTime"].clone());
    map.insert(
        "LastModificationTime".to_string(),
        row["LastModificationTime"].clone(),
    );
    Value::Object(map)
}

fn family_arn(kind: &str, region: &str, account: u64, name: &str) -> String {
    format!(
        "arn:{}:forecast:{region}:{account}:{kind}/{name}",
        partition(region)
    )
}

fn list_response(
    rows: &[&Value],
    key: &str,
    arn_key: &str,
    name_key: &str,
    extra: &[&str],
) -> Response {
    let summaries: Vec<Value> = rows
        .iter()
        .map(|r| summary(r, arn_key, name_key, extra))
        .collect();
    ok_response(json!({ key: summaries, "NextToken": Value::Null }))
}

/// moto-style duplicate message (`A dataset group already exists with the
/// arn: ...`), generalized per family.
fn already(kind: &str, arn: &str) -> Response {
    error_response(
        400,
        "ResourceAlreadyExistsException",
        &format!("A {kind} already exists with the arn: {arn}"),
    )
}

impl State {
    fn find_dataset_group(&self, arn: &str) -> Option<&DatasetGroup> {
        self.groups.iter().find(|d| d.arn == arn)
    }

    /// Dataset groups and Datasets share the "arn resolved?" check input.
    fn dataset_arn_known(&self, arn: &str) -> bool {
        self.datasets
            .iter()
            .any(|r| r.get("DatasetArn").and_then(Value::as_str) == Some(arn))
    }

    /// A predictor ARN: legacy predictors and auto predictors.
    fn predictor_arn_known(&self, arn: &str) -> bool {
        self.predictors
            .iter()
            .any(|r| r.get("PredictorArn").and_then(Value::as_str) == Some(arn))
            || self
                .auto_predictors
                .iter()
                .any(|r| r.get("PredictorArn").and_then(Value::as_str) == Some(arn))
    }

    fn is_auto_predictor(&self, arn: &str) -> bool {
        self.auto_predictors
            .iter()
            .any(|r| r.get("PredictorArn").and_then(Value::as_str) == Some(arn))
    }

    fn forecast_arn_known(&self, arn: &str) -> bool {
        self.forecasts
            .iter()
            .any(|r| r.get("ForecastArn").and_then(Value::as_str) == Some(arn))
    }

    fn explainability_arn_known(&self, arn: &str) -> bool {
        self.explainabilities
            .iter()
            .any(|r| r.get("ExplainabilityArn").and_then(Value::as_str) == Some(arn))
    }

    fn what_if_analysis_arn_known(&self, arn: &str) -> bool {
        self.what_if_analyses
            .iter()
            .any(|r| r.get("WhatIfAnalysisArn").and_then(Value::as_str) == Some(arn))
    }

    fn what_if_forecast_arn_known(&self, arn: &str) -> bool {
        self.what_if_forecasts
            .iter()
            .any(|r| r.get("WhatIfForecastArn").and_then(Value::as_str) == Some(arn))
    }

    fn monitor_arn_known(&self, arn: &str) -> bool {
        self.monitors
            .iter()
            .any(|r| r.get("MonitorArn").and_then(Value::as_str) == Some(arn))
    }

    /// Any modeled record answers this ARN (StopResource / ResumeResource /
    /// DeleteResourceTree / the tag routes).
    fn any_arn_known(&self, arn: &str) -> bool {
        self.find_dataset_group(arn).is_some()
            || self.dataset_arn_known(arn)
            || self
                .import_jobs
                .iter()
                .any(|r| r.get("DatasetImportJobArn").and_then(Value::as_str) == Some(arn))
            || self.predictor_arn_known(arn)
            || self.forecast_arn_known(arn)
            || self.monitor_arn_known(arn)
            || self.explainability_arn_known(arn)
            || self
                .explainability_exports
                .iter()
                .any(|r| r.get("ExplainabilityExportArn").and_then(Value::as_str) == Some(arn))
            || self
                .forecast_exports
                .iter()
                .any(|r| r.get("ForecastExportJobArn").and_then(Value::as_str) == Some(arn))
            || self.backtest_exports.iter().any(|r| {
                r.get("PredictorBacktestExportJobArn")
                    .and_then(Value::as_str)
                    == Some(arn)
            })
            || self.what_if_analysis_arn_known(arn)
            || self.what_if_forecast_arn_known(arn)
            || self
                .what_if_forecast_exports
                .iter()
                .any(|r| r.get("WhatIfForecastExportArn").and_then(Value::as_str) == Some(arn))
    }

    /// Remove one record from whichever family owns the ARN. None of the
    /// modeled families track child links, so the tree op prunes the
    /// targeted record only.
    fn delete_any(&mut self, arn: &str) -> bool {
        let mut existed = false;
        let before = self.groups.len();
        self.groups.retain(|d| {
            if d.arn == arn {
                existed = true;
                false
            } else {
                true
            }
        });
        if self.groups.len() != before {
            return true;
        }
        let before = existed;
        for vectors in [&mut self.datasets, &mut self.import_jobs] {
            let n = vectors.len();
            vectors.retain(|r| {
                r.get("DatasetArn").and_then(Value::as_str) != Some(arn)
                    && r.get("DatasetImportJobArn").and_then(Value::as_str) != Some(arn)
            });
            if vectors.len() != n {
                return true;
            }
        }
        let _ = before;
        for (vectors, arn_key) in [
            (&mut self.predictors, "PredictorArn"),
            (&mut self.auto_predictors, "PredictorArn"),
            (&mut self.forecasts, "ForecastArn"),
            (&mut self.monitors, "MonitorArn"),
            (&mut self.explainabilities, "ExplainabilityArn"),
            (&mut self.explainability_exports, "ExplainabilityExportArn"),
            (&mut self.forecast_exports, "ForecastExportJobArn"),
            (&mut self.backtest_exports, "PredictorBacktestExportJobArn"),
            (&mut self.what_if_analyses, "WhatIfAnalysisArn"),
            (&mut self.what_if_forecasts, "WhatIfForecastArn"),
            (
                &mut self.what_if_forecast_exports,
                "WhatIfForecastExportArn",
            ),
        ] {
            let n = vectors.len();
            vectors.retain(|r| r.get(arn_key).and_then(Value::as_str) != Some(arn));
            if vectors.len() != n {
                return true;
            }
        }
        existed
    }

    fn set_tags(&mut self, arn: &str, tags: &Value) {
        let row = match self.tags.iter_mut().find(|(a, _)| a == arn) {
            Some((_, v)) => v,
            None => {
                self.tags.push((arn.to_string(), json!({})));
                let i = self.tags.len() - 1;
                &mut self.tags[i].1
            }
        };
        if let Some(pairs) = tags.as_array() {
            for tag in pairs {
                let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
                if key.is_empty() {
                    continue;
                }
                if let Some(v) = row.as_object_mut() {
                    v.insert(
                        key.to_string(),
                        tag.get("Value").cloned().unwrap_or(Value::Null),
                    );
                }
            }
        }
    }

    fn tag_list(&self, arn: &str) -> Value {
        self.tags
            .iter()
            .find(|(a, _)| a == arn)
            .map(|(_, v)| v)
            .unwrap_or(&json!({}))
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect::<Vec<Value>>()
            })
            .unwrap_or_default()
            .into()
    }

    fn untag(&mut self, arn: &str, keys: &[String]) {
        if let Some((_, v)) = self.tags.iter_mut().find(|(a, _)| a == arn) {
            if let Some(obj) = v.as_object_mut() {
                for key in keys {
                    obj.remove(key);
                }
            }
        }
    }
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
        "CreateDataset" => create_dataset(req, &body),
        "DescribeDataset" => describe_dataset(req, &body),
        "DeleteDataset" => simple_delete(req, &body, "DatasetArn"),
        "ListDatasets" => list_dataset_rows(req),
        "CreateDatasetImportJob" => create_dataset_import_job(req, &body),
        "DescribeDatasetImportJob" => describe_import_job(req, &body),
        "DeleteDatasetImportJob" => simple_delete(req, &body, "DatasetImportJobArn"),
        "ListDatasetImportJobs" => list_import_jobs(req),
        "CreatePredictor" => create_predictor(req, &body),
        "DescribePredictor" => generic_describe(req, &body, "PredictorArn"),
        "DeletePredictor" => simple_delete(req, &body, "PredictorArn"),
        "ListPredictors" => list_predictors(req),
        "CreateAutoPredictor" => create_auto_predictor(req, &body),
        "DescribeAutoPredictor" => generic_describe(req, &body, "PredictorArn"),
        "CreateForecast" => create_forecast(req, &body),
        "DescribeForecast" => generic_describe(req, &body, "ForecastArn"),
        "DeleteForecast" => simple_delete(req, &body, "ForecastArn"),
        "ListForecasts" => list_forecasts(req),
        "CreateMonitor" => create_monitor(req, &body),
        "DescribeMonitor" => generic_describe(req, &body, "MonitorArn"),
        "DeleteMonitor" => simple_delete(req, &body, "MonitorArn"),
        "ListMonitors" => list_monitors(req),
        "ListMonitorEvaluations" => list_monitor_evaluations(req, &body),
        "CreateExplainability" => create_explainability(req, &body),
        "DescribeExplainability" => generic_describe(req, &body, "ExplainabilityArn"),
        "DeleteExplainability" => simple_delete(req, &body, "ExplainabilityArn"),
        "ListExplainabilities" => list_explainabilities(req),
        "CreateExplainabilityExport" => create_explainability_export(req, &body),
        "DescribeExplainabilityExport" => generic_describe(req, &body, "ExplainabilityExportArn"),
        "DeleteExplainabilityExport" => simple_delete(req, &body, "ExplainabilityExportArn"),
        "ListExplainabilityExports" => list_explainability_exports(req),
        "CreateForecastExportJob" => create_forecast_export_job(req, &body),
        "DescribeForecastExportJob" => generic_describe(req, &body, "ForecastExportJobArn"),
        "DeleteForecastExportJob" => simple_delete(req, &body, "ForecastExportJobArn"),
        "ListForecastExportJobs" => list_forecast_export_jobs(req),
        "CreatePredictorBacktestExportJob" => create_backtest_export_job(req, &body),
        "DescribePredictorBacktestExportJob" => {
            generic_describe(req, &body, "PredictorBacktestExportJobArn")
        }
        "DeletePredictorBacktestExportJob" => {
            simple_delete(req, &body, "PredictorBacktestExportJobArn")
        }
        "ListPredictorBacktestExportJobs" => list_backtest_export_jobs(req),
        "CreateWhatIfAnalysis" => create_what_if_analysis(req, &body),
        "DescribeWhatIfAnalysis" => generic_describe(req, &body, "WhatIfAnalysisArn"),
        "DeleteWhatIfAnalysis" => simple_delete(req, &body, "WhatIfAnalysisArn"),
        "ListWhatIfAnalyses" => list_what_if_analyses(req),
        "CreateWhatIfForecast" => create_what_if_forecast(req, &body),
        "DescribeWhatIfForecast" => generic_describe(req, &body, "WhatIfForecastArn"),
        "DeleteWhatIfForecast" => simple_delete(req, &body, "WhatIfForecastArn"),
        "ListWhatIfForecasts" => list_what_if_forecasts(req),
        "CreateWhatIfForecastExport" => create_what_if_forecast_export(req, &body),
        "DescribeWhatIfForecastExport" => generic_describe(req, &body, "WhatIfForecastExportArn"),
        "DeleteWhatIfForecastExport" => simple_delete(req, &body, "WhatIfForecastExportArn"),
        "ListWhatIfForecastExports" => list_what_if_forecast_exports(req),
        "GetAccuracyMetrics" => get_accuracy_metrics(req, &body),
        "StopResource" => stop_resource(req, &body),
        "ResumeResource" => resume_resource(req, &body),
        "DeleteResourceTree" => delete_resource_tree(req, &body),
        "TagResource" => tag_resource(req, &body),
        "UntagResource" => untag_resource(req, &body),
        "ListTagsForResource" => list_tags_for_resource(req, &body),
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
        // with the modeled Dataset family in place the check resolves real
        // records, falling back to moto's "not found" verdict for unknown
        // arns.
        if let Some(arn) = first_str(body, "DatasetArns") {
            if !st.dataset_arn_known(arn) {
                return error_response(
                    400,
                    "InvalidInputException",
                    &format!("Dataset arns: [{arn}] are not found"),
                );
            }
        }
        if st.groups.iter().any(|d| d.arn == group.arn) {
            return error_response(
                400,
                "ResourceAlreadyExistsException",
                &format!("A dataset group already exists with the arn: {}", group.arn),
            );
        }
        let arn = group.arn.clone();
        st.groups.push(group);
        ok_response(json!({ "DatasetGroupArn": arn }))
    })
}

fn describe_dataset_group(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "DatasetGroupArn").to_string();
    with_state(req.account, &req.region, |st| {
        match st.groups.iter().find(|d| d.arn == arn) {
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
        let before = st.groups.len();
        st.groups.retain(|d| d.arn != arn);
        if st.groups.len() == before {
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
        let group = match st.groups.iter_mut().find(|d| d.arn == arn) {
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
            .groups
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

// ---------------------------------------------------------------------------
// modeled families (moto has no handler; the botocore shape is authority)
// ---------------------------------------------------------------------------

/// Common describe: the stored record IS the wire body.
fn generic_describe(req: &Request, body: &Value, arn_key: &str) -> Response {
    let arn = param_str(body, arn_key).to_string();
    with_state(req.account, &req.region, |st| {
        match find_row(st, arn_key, &arn) {
            Some(row) => ok_response(row.clone()),
            None => not_found(&arn),
        }
    })
}

/// Find a modeled record by family key.
fn find_row<'a>(st: &'a State, arn_key: &str, arn: &str) -> Option<&'a Value> {
    match arn_key {
        "DatasetArn" => st
            .datasets
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "DatasetImportJobArn" => st
            .import_jobs
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "PredictorArn" => st
            .predictors
            .iter()
            .chain(st.auto_predictors.iter())
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "ForecastArn" => st
            .forecasts
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "MonitorArn" => st
            .monitors
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "ExplainabilityArn" => st
            .explainabilities
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "ExplainabilityExportArn" => st
            .explainability_exports
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "ForecastExportJobArn" => st
            .forecast_exports
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "PredictorBacktestExportJobArn" => st
            .backtest_exports
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "WhatIfAnalysisArn" => st
            .what_if_analyses
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "WhatIfForecastArn" => st
            .what_if_forecasts
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        "WhatIfForecastExportArn" => st
            .what_if_forecast_exports
            .iter()
            .find(|r| r.get(arn_key).and_then(Value::as_str) == Some(arn)),
        _ => None,
    }
}

/// Modeled deletes: unknown ARN -> RFNF; known record -> empty 200 body.
fn simple_delete(req: &Request, body: &Value, arn_key: &str) -> Response {
    let arn = param_str(body, arn_key).to_string();
    with_state(req.account, &req.region, |st| {
        if find_row(st, arn_key, &arn).is_none() {
            return not_found(&arn);
        }
        st.delete_any(&arn);
        empty_response()
    })
}

// --- datasets ----------------------------------------------------------------

fn create_dataset(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "DatasetName");
    let arn = family_arn("dataset", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        if st.dataset_arn_known(&arn) {
            return already("dataset", &arn);
        }
        let row = record(
            "DatasetArn",
            &arn,
            "DatasetName",
            name,
            &[
                "Domain",
                "DatasetType",
                "DataFrequency",
                "Schema",
                "EncryptionConfig",
            ],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.datasets.push(row);
        ok_response(json!({ "DatasetArn": arn }))
    })
}

fn describe_dataset(req: &Request, body: &Value) -> Response {
    generic_describe(req, body, "DatasetArn")
}

fn list_dataset_rows(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.datasets.iter().collect();
        list_response(
            &rows,
            "Datasets",
            "DatasetArn",
            "DatasetName",
            &["Domain", "DatasetType"],
        )
    })
}

// --- dataset import jobs -----------------------------------------------------

fn create_dataset_import_job(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "DatasetImportJobName");
    let arn = family_arn("dataset-import-job", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let dataset_arn = param_str(body, "DatasetArn");
        if !st.dataset_arn_known(dataset_arn) {
            return not_found(dataset_arn);
        }
        if st
            .import_jobs
            .iter()
            .any(|r| r.get("DatasetImportJobArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("dataset import job", &arn);
        }
        let row = record(
            "DatasetImportJobArn",
            &arn,
            "DatasetImportJobName",
            name,
            &[
                "DatasetArn",
                "DataSource",
                "TimestampFormat",
                "TimeZone",
                "UseGeolocationForTimeZone",
                "GeolocationFormat",
                "Format",
                "ImportMode",
            ],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.import_jobs.push(row);
        ok_response(json!({ "DatasetImportJobArn": arn }))
    })
}

fn describe_import_job(req: &Request, body: &Value) -> Response {
    generic_describe(req, body, "DatasetImportJobArn")
}

fn list_import_jobs(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.import_jobs.iter().collect();
        list_response(
            &rows,
            "DatasetImportJobs",
            "DatasetImportJobArn",
            "DatasetImportJobName",
            &["DataSource", "ImportMode"],
        )
    })
}

// --- predictors --------------------------------------------------------------

fn create_predictor(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "PredictorName");
    let arn = family_arn("predictor", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        // CreatePredictor requires InputDataConfig.DatasetGroupArn.
        let group_arn = body
            .get("InputDataConfig")
            .and_then(|c| c.get("DatasetGroupArn"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if group_arn.is_empty() || st.find_dataset_group(group_arn).is_none() {
            return not_found(&format!("No resource found {group_arn}"));
        }
        if st
            .predictors
            .iter()
            .chain(st.auto_predictors.iter())
            .any(|r| r.get("PredictorArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("predictor", &arn);
        }
        let row = record(
            "PredictorArn",
            &arn,
            "PredictorName",
            name,
            &[
                "AlgorithmArn",
                "ForecastHorizon",
                "ForecastTypes",
                "PerformAutoML",
                "AutoMLOverrideStrategy",
                "PerformHPO",
                "TrainingParameters",
                "EvaluationParameters",
                "HPOConfig",
                "InputDataConfig",
                "FeaturizationConfig",
                "EncryptionConfig",
                "OptimizationMetric",
            ],
            body,
            &[("IsAutoPredictor", json!(false))],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.predictors.push(row);
        ok_response(json!({ "PredictorArn": arn }))
    })
}

fn create_auto_predictor(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "PredictorName");
    let arn = family_arn("automl-predictor", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        // DataConfig carries an optional DatasetGroupArn reference.
        if let Some(group_arn) = body
            .get("DataConfig")
            .and_then(|c| c.get("DatasetGroupArn"))
            .and_then(Value::as_str)
        {
            if !group_arn.is_empty() && st.find_dataset_group(group_arn).is_none() {
                return not_found(group_arn);
            }
        }
        if st
            .predictors
            .iter()
            .chain(st.auto_predictors.iter())
            .any(|r| r.get("PredictorArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("predictor", &arn);
        }
        let row = record(
            "PredictorArn",
            &arn,
            "PredictorName",
            name,
            &[
                "ForecastHorizon",
                "ForecastTypes",
                "ForecastFrequency",
                "ForecastDimensions",
                "DataConfig",
                "EncryptionConfig",
                "ReferencePredictorArn",
                "OptimizationMetric",
                "ExplainPredictor",
                "MonitorConfig",
                "TimeAlignmentBoundary",
            ],
            body,
            &[("IsAutoPredictor", json!(true))],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.auto_predictors.push(row);
        ok_response(json!({ "PredictorArn": arn }))
    })
}

fn list_predictors(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st
            .predictors
            .iter()
            .chain(st.auto_predictors.iter())
            .collect();
        list_response(
            &rows,
            "Predictors",
            "PredictorArn",
            "PredictorName",
            &["DatasetGroupArn", "IsAutoPredictor"],
        )
    })
}

// --- forecasts ---------------------------------------------------------------

fn create_forecast(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "ForecastName");
    let arn = family_arn("forecast", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let predictor_arn = param_str(body, "PredictorArn");
        if !st.predictor_arn_known(predictor_arn) {
            return not_found(predictor_arn);
        }
        if st
            .forecasts
            .iter()
            .any(|r| r.get("ForecastArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("forecast", &arn);
        }
        // The rendered DatasetGroupArn comes from the parent predictor's
        // InputDataConfig, when resolvable.
        let dataset_group_arn = find_row(st, "PredictorArn", predictor_arn)
            .and_then(|r| {
                r.get("InputDataConfig")
                    .and_then(|c| c.get("DatasetGroupArn"))
                    .cloned()
            })
            .unwrap_or(json!(null));
        let row = record(
            "ForecastArn",
            &arn,
            "ForecastName",
            name,
            &["PredictorArn", "ForecastTypes", "TimeSeriesSelector"],
            body,
            &[
                ("DatasetGroupArn", dataset_group_arn),
                (
                    "CreatedUsingAutoPredictor",
                    json!(st.is_auto_predictor(predictor_arn)),
                ),
            ],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.forecasts.push(row);
        ok_response(json!({ "ForecastArn": arn }))
    })
}

fn list_forecasts(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.forecasts.iter().collect();
        list_response(
            &rows,
            "Forecasts",
            "ForecastArn",
            "ForecastName",
            &[
                "PredictorArn",
                "DatasetGroupArn",
                "CreatedUsingAutoPredictor",
            ],
        )
    })
}

// --- monitors ----------------------------------------------------------------

fn create_monitor(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "MonitorName");
    let arn = family_arn("monitor", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "ResourceArn");
        if !st.predictor_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .monitors
            .iter()
            .any(|r| r.get("MonitorArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("monitor", &arn);
        }
        let row = record(
            "MonitorArn",
            &arn,
            "MonitorName",
            name,
            &["ResourceArn"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.monitors.push(row);
        ok_response(json!({ "MonitorArn": arn }))
    })
}

fn list_monitors(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.monitors.iter().collect();
        list_response(
            &rows,
            "Monitors",
            "MonitorArn",
            "MonitorName",
            &["ResourceArn"],
        )
    })
}

fn list_monitor_evaluations(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "MonitorArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.monitor_arn_known(&arn) {
            return not_found(&arn);
        }
        ok_response(json!({
            "PredictorMonitorEvaluations": [],
            "NextToken": Value::Null,
        }))
    })
}

// --- explainabilities --------------------------------------------------------

fn create_explainability(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "ExplainabilityName");
    let arn = family_arn("explainability", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "ResourceArn");
        if !st.predictor_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .explainabilities
            .iter()
            .any(|r| r.get("ExplainabilityArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("explainability", &arn);
        }
        let row = record(
            "ExplainabilityArn",
            &arn,
            "ExplainabilityName",
            name,
            &[
                "ResourceArn",
                "ExplainabilityConfig",
                "DataSource",
                "Schema",
                "EnableVisualization",
                "StartDateTime",
                "EndDateTime",
            ],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.explainabilities.push(row);
        ok_response(json!({ "ExplainabilityArn": arn }))
    })
}

fn list_explainabilities(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.explainabilities.iter().collect();
        list_response(
            &rows,
            "Explainabilities",
            "ExplainabilityArn",
            "ExplainabilityName",
            &["ResourceArn", "ExplainabilityConfig"],
        )
    })
}

// --- exports -----------------------------------------------------------------

fn create_explainability_export(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "ExplainabilityExportName");
    let arn = family_arn("explainability-export", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "ExplainabilityArn");
        if !st.explainability_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .explainability_exports
            .iter()
            .any(|r| r.get("ExplainabilityExportArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("explainability export", &arn);
        }
        let row = record(
            "ExplainabilityExportArn",
            &arn,
            "ExplainabilityExportName",
            name,
            &["ExplainabilityArn", "Destination", "Format"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.explainability_exports.push(row);
        ok_response(json!({ "ExplainabilityExportArn": arn }))
    })
}

fn list_explainability_exports(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.explainability_exports.iter().collect();
        list_response(
            &rows,
            "ExplainabilityExports",
            "ExplainabilityExportArn",
            "ExplainabilityExportName",
            &["Destination"],
        )
    })
}

fn create_forecast_export_job(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "ForecastExportJobName");
    let arn = family_arn("forecast-export-job", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "ForecastArn");
        if !st.forecast_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .forecast_exports
            .iter()
            .any(|r| r.get("ForecastExportJobArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("forecast export job", &arn);
        }
        let row = record(
            "ForecastExportJobArn",
            &arn,
            "ForecastExportJobName",
            name,
            &["ForecastArn", "Destination", "Format"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.forecast_exports.push(row);
        ok_response(json!({ "ForecastExportJobArn": arn }))
    })
}

fn list_forecast_export_jobs(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.forecast_exports.iter().collect();
        list_response(
            &rows,
            "ForecastExportJobs",
            "ForecastExportJobArn",
            "ForecastExportJobName",
            &["Destination"],
        )
    })
}

fn create_backtest_export_job(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "PredictorBacktestExportJobName");
    let arn = family_arn(
        "predictor-backtest-export-job",
        &req.region,
        req.account,
        name,
    );
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "PredictorArn");
        if !st.predictor_arn_known(parent) {
            return not_found(parent);
        }
        if st.backtest_exports.iter().any(|r| {
            r.get("PredictorBacktestExportJobArn")
                .and_then(Value::as_str)
                == Some(arn.as_str())
        }) {
            return already("predictor backtest export job", &arn);
        }
        let row = record(
            "PredictorBacktestExportJobArn",
            &arn,
            "PredictorBacktestExportJobName",
            name,
            &["PredictorArn", "Destination", "Format"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.backtest_exports.push(row);
        ok_response(json!({ "PredictorBacktestExportJobArn": arn }))
    })
}

fn list_backtest_export_jobs(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.backtest_exports.iter().collect();
        list_response(
            &rows,
            "PredictorBacktestExportJobs",
            "PredictorBacktestExportJobArn",
            "PredictorBacktestExportJobName",
            &["Destination"],
        )
    })
}

// --- what-if family ----------------------------------------------------------

fn create_what_if_analysis(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "WhatIfAnalysisName");
    let arn = family_arn("what-if-analysis", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "ForecastArn");
        if !st.forecast_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .what_if_analyses
            .iter()
            .any(|r| r.get("WhatIfAnalysisArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("what-if analysis", &arn);
        }
        let row = record(
            "WhatIfAnalysisArn",
            &arn,
            "WhatIfAnalysisName",
            name,
            &["ForecastArn", "TimeSeriesSelector"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.what_if_analyses.push(row);
        ok_response(json!({ "WhatIfAnalysisArn": arn }))
    })
}

fn list_what_if_analyses(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.what_if_analyses.iter().collect();
        list_response(
            &rows,
            "WhatIfAnalyses",
            "WhatIfAnalysisArn",
            "WhatIfAnalysisName",
            &["ForecastArn"],
        )
    })
}

fn create_what_if_forecast(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "WhatIfForecastName");
    let arn = family_arn("what-if-forecast", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        let parent = param_str(body, "WhatIfAnalysisArn");
        if !st.what_if_analysis_arn_known(parent) {
            return not_found(parent);
        }
        if st
            .what_if_forecasts
            .iter()
            .any(|r| r.get("WhatIfForecastArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("what-if forecast", &arn);
        }
        let row = record(
            "WhatIfForecastArn",
            &arn,
            "WhatIfForecastName",
            name,
            &[
                "WhatIfAnalysisArn",
                "TimeSeriesTransformations",
                "TimeSeriesReplacementsDataSource",
            ],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.what_if_forecasts.push(row);
        ok_response(json!({ "WhatIfForecastArn": arn }))
    })
}

fn list_what_if_forecasts(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.what_if_forecasts.iter().collect();
        list_response(
            &rows,
            "WhatIfForecasts",
            "WhatIfForecastArn",
            "WhatIfForecastName",
            &["WhatIfAnalysisArn"],
        )
    })
}

fn create_what_if_forecast_export(req: &Request, body: &Value) -> Response {
    let name = param_str(body, "WhatIfForecastExportName");
    let arn = family_arn("what-if-forecast-export", &req.region, req.account, name);
    with_state(req.account, &req.region, |st| {
        // The first requested arn must resolve.
        if let Some(parent) = first_str(body, "WhatIfForecastArns") {
            if !st.what_if_forecast_arn_known(parent) {
                return not_found(parent);
            }
        }
        if st
            .what_if_forecast_exports
            .iter()
            .any(|r| r.get("WhatIfForecastExportArn").and_then(Value::as_str) == Some(arn.as_str()))
        {
            return already("what-if forecast export", &arn);
        }
        let row = record(
            "WhatIfForecastExportArn",
            &arn,
            "WhatIfForecastExportName",
            name,
            &["WhatIfForecastArns", "Destination", "Format"],
            body,
            &[],
        );
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        st.what_if_forecast_exports.push(row);
        ok_response(json!({ "WhatIfForecastExportArn": arn }))
    })
}

fn list_what_if_forecast_exports(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let rows: Vec<&Value> = st.what_if_forecast_exports.iter().collect();
        list_response(
            &rows,
            "WhatIfForecastExports",
            "WhatIfForecastExportArn",
            "WhatIfForecastExportName",
            &["Destination"],
        )
    })
}

// --- metrics, stop/resume, tree, tags ---------------------------------------

fn get_accuracy_metrics(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "PredictorArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.predictor_arn_known(&arn) {
            return not_found(&arn);
        }
        ok_response(json!({
            "PredictorEvaluationResults": [],
            "IsAutoPredictor": st.is_auto_predictor(&arn),
        }))
    })
}

fn stop_resource(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        // Every modeled record sits in a settled state, so stopping is a
        // no-op that answers moto's empty 200 body.
        empty_response()
    })
}

fn resume_resource(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        empty_response()
    })
}

fn delete_resource_tree(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        st.delete_any(&arn);
        empty_response()
    })
}

fn tag_resource(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        if let Some(tags) = body.get("Tags") {
            st.set_tags(&arn, tags);
        }
        empty_response()
    })
}

fn untag_resource(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        st.untag(&arn, &keys);
        empty_response()
    })
}

fn list_tags_for_resource(req: &Request, body: &Value) -> Response {
    let arn = param_str(body, "ResourceArn").to_string();
    with_state(req.account, &req.region, |st| {
        if !st.any_arn_known(&arn) {
            return not_found(&arn);
        }
        ok_response(json!({ "Tags": st.tag_list(&arn) }))
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
    fn snake_cased_501_message() {
        // Every modeled op now answers its handler; the snake-case
        // NotImplementedError rendering remains the safety net for
        // operations botocore does not register for the twin.
        let r = dispatch(&fmt_req("BogusOperation", json!({})));
        assert_eq!(r.status, 501);
        let v = body_of(&r);
        assert_eq!(v["__type"], "NotImplemented");
        assert_eq!(
            v["message"],
            "The bogus_operation action has not been implemented"
        );
        // moto's camelcase_to_underscores rendering, verbatim.
        let r = not_implemented("DeleteWhatIfForecastExport");
        assert_eq!(r.status, 501);
        assert_eq!(
            body_of(&r)["message"],
            "The delete_what_if_forecast_export action has not been implemented"
        );
    }

    #[test]
    fn dataset_lifecycle_round_trip() {
        let created = dispatch(&fmt_req(
            "CreateDataset",
            json!({
                "DatasetName": "ds-rt",
                "Domain": "CUSTOM",
                "DatasetType": "TARGET_TIME_SERIES",
                "DataFrequency": "1D",
                "Schema": {"Attributes": [
                    {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                    {"AttributeName": "target_value", "AttributeType": "float"}
                ]},
            }),
        ));
        assert_eq!(created.status, 200);
        let arn = body_of(&created)["DatasetArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(arn, "arn:aws:forecast:us-east-1:482409018203:dataset/ds-rt");

        let described = dispatch(&fmt_req("DescribeDataset", json!({"DatasetArn": arn})));
        assert_eq!(described.status, 200);
        let dv = body_of(&described);
        assert_eq!(dv["DatasetName"], "ds-rt");
        assert_eq!(dv["Domain"], "CUSTOM");
        assert_eq!(dv["DatasetType"], "TARGET_TIME_SERIES");
        assert_eq!(dv["Status"], "ACTIVE");
        assert!(dv["Schema"]["Attributes"].is_array());

        // Duplicate create -> ResourceAlreadyExistsException (moto wording).
        let dup = dispatch(&fmt_req(
            "CreateDataset",
            json!({"DatasetName": "ds-rt", "Domain": "CUSTOM", "DatasetType": "TARGET_TIME_SERIES"}),
        ));
        assert_eq!(dup.status, 400);
        assert_eq!(body_of(&dup)["__type"], "ResourceAlreadyExistsException");

        let listed = dispatch(&fmt_req("ListDatasets", json!({})));
        let listed_v = body_of(&listed);
        let rows = listed_v["Datasets"].as_array().unwrap();
        assert!(rows.iter().any(|r| r["DatasetArn"] == json!(arn)));

        let deleted = dispatch(&fmt_req("DeleteDataset", json!({"DatasetArn": arn})));
        assert_eq!(deleted.status, 200);
        assert!(deleted.body.is_empty());
        let gone = dispatch(&fmt_req("DescribeDataset", json!({"DatasetArn": arn})));
        assert_eq!(body_of(&gone)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn dataset_group_accepts_modeled_dataset_arns() {
        // moto exposed no CreateDataset, so any dataset arn failed; with the
        // modeled family in place a real record resolves, and moto's exact
        // "Dataset arns: [...] are not found" verdict survives for unknown
        // arns.
        let ds = dispatch(&fmt_req(
            "CreateDataset",
            json!({"DatasetName": "dsg-link", "Domain": "CUSTOM", "DatasetType": "TARGET_TIME_SERIES"}),
        ));
        let ds_arn = body_of(&ds)["DatasetArn"].as_str().unwrap().to_string();
        let ok = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-with-ds", "Domain": "RETAIL", "DatasetArns": [ds_arn]}),
        ));
        assert_eq!(ok.status, 200);
        let described = dispatch(&fmt_req(
            "DescribeDatasetGroup",
            json!({"DatasetGroupArn": body_of(&ok)["DatasetGroupArn"]}),
        ));
        assert_eq!(
            body_of(&described)["DatasetArns"].as_array().unwrap().len(),
            1
        );

        let bad = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "dsg-bad-link", "Domain": "RETAIL",
                   "DatasetArns": ["arn:aws:forecast:us-east-1:482409018203:dataset/nope"]}),
        ));
        assert_eq!(body_of(&bad)["__type"], "InvalidInputException");
        assert_eq!(
            body_of(&bad)["message"],
            "Dataset arns: [arn:aws:forecast:us-east-1:482409018203:dataset/nope] are not found"
        );
    }

    #[test]
    fn import_job_needs_dataset_parent() {
        let bad = dispatch(&fmt_req(
            "CreateDatasetImportJob",
            json!({
                "DatasetImportJobName": "ij-bad",
                "DatasetArn": "arn:aws:forecast:us-east-1:482409018203:dataset/nope",
                "DataSource": {"S3Config": {
                    "Path": "s3://bucket/path", "RoleArn": "arn:aws:iam::482409018203:role/x"
                }},
            }),
        ));
        assert_eq!(bad.status, 400);
        assert_eq!(body_of(&bad)["__type"], "ResourceNotFoundException");

        let ds = dispatch(&fmt_req(
            "CreateDataset",
            json!({"DatasetName": "ds-ij", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES"}),
        ));
        let ds_arn = body_of(&ds)["DatasetArn"].as_str().unwrap().to_string();
        let created = dispatch(&fmt_req(
            "CreateDatasetImportJob",
            json!({
                "DatasetImportJobName": "ij-ok",
                "DatasetArn": ds_arn.clone(),
                "DataSource": {"S3Config": {
                    "Path": "s3://bucket/path", "RoleArn": "arn:aws:iam::482409018203:role/x"
                }},
                "ImportMode": "FULL",
            }),
        ));
        assert_eq!(created.status, 200);
        let job_arn = body_of(&created)["DatasetImportJobArn"]
            .as_str()
            .unwrap()
            .to_string();

        let described = dispatch(&fmt_req(
            "DescribeDatasetImportJob",
            json!({"DatasetImportJobArn": job_arn.clone()}),
        ));
        let dv = body_of(&described);
        assert_eq!(dv["DatasetArn"], ds_arn);
        assert_eq!(dv["ImportMode"], "FULL");
        assert!(dv["DataSource"]["S3Config"]["Path"]
            .as_str()
            .unwrap()
            .contains("bucket"));

        let listed = dispatch(&fmt_req("ListDatasetImportJobs", json!({})));
        assert!(!body_of(&listed)["DatasetImportJobs"]
            .as_array()
            .unwrap()
            .is_empty());

        let deleted = dispatch(&fmt_req(
            "DeleteDatasetImportJob",
            json!({"DatasetImportJobArn": job_arn}),
        ));
        assert_eq!(deleted.status, 200);
    }

    #[test]
    fn predictor_forecast_chain() {
        let group = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "pred-chain-dsg", "Domain": "CUSTOM"}),
        ));
        let group_arn = body_of(&group)["DatasetGroupArn"]
            .as_str()
            .unwrap()
            .to_string();

        // CreatePredictor with unknown group arn -> RFNF.
        let bad = dispatch(&fmt_req(
            "CreatePredictor",
            json!({
                "PredictorName": "pred-bad",
                "ForecastHorizon": 12,
                "InputDataConfig": {"DatasetGroupArn": "arn:aws:forecast:us-east-1:482409018203:dataset-group/missing"},
                "FeaturizationConfig": {"ForecastFrequency": "1D"},
            }),
        ));
        assert_eq!(bad.status, 400);

        let pred = dispatch(&fmt_req(
            "CreatePredictor",
            json!({
                "PredictorName": "pred-chain",
                "ForecastHorizon": 12,
                "InputDataConfig": {"DatasetGroupArn": group_arn.clone()},
                "FeaturizationConfig": {"ForecastFrequency": "1D"},
            }),
        ));
        assert_eq!(pred.status, 200);
        let pred_arn = body_of(&pred)["PredictorArn"].as_str().unwrap().to_string();

        let described = dispatch(&fmt_req(
            "DescribePredictor",
            json!({"PredictorArn": pred_arn.clone()}),
        ));
        let dv = body_of(&described);
        assert_eq!(dv["ForecastHorizon"], 12);
        assert_eq!(dv["InputDataConfig"]["DatasetGroupArn"], group_arn.clone());
        assert_eq!(dv["Status"], "ACTIVE");

        let fcst = dispatch(&fmt_req(
            "CreateForecast",
            json!({"ForecastName": "f-chain", "PredictorArn": pred_arn.clone()}),
        ));
        assert_eq!(fcst.status, 200);
        let f_arn = body_of(&fcst)["ForecastArn"].as_str().unwrap().to_string();

        let f_desc = dispatch(&fmt_req(
            "DescribeForecast",
            json!({"ForecastArn": f_arn.clone()}),
        ));
        assert_eq!(body_of(&f_desc)["DatasetGroupArn"], group_arn.clone());

        // CreateForecast with unknown predictor -> RFNF.
        let bad_f = dispatch(&fmt_req(
            "CreateForecast",
            json!({"ForecastName": "f-bad", "PredictorArn": "arn:aws:forecast:us-east-1:482409018203:predictor/nope"}),
        ));
        assert_eq!(bad_f.status, 400);

        let metrics = dispatch(&fmt_req(
            "GetAccuracyMetrics",
            json!({"PredictorArn": pred_arn.clone()}),
        ));
        assert_eq!(metrics.status, 200);
        assert_eq!(body_of(&metrics)["IsAutoPredictor"], false);
        assert!(body_of(&metrics)["PredictorEvaluationResults"].is_array());

        let preds = dispatch(&fmt_req("ListPredictors", json!({})));
        assert!(body_of(&preds)["Predictors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["IsAutoPredictor"] == json!(false)));

        // Delete forecast then predictor then group (tree path).
        assert_eq!(
            dispatch(&fmt_req(
                "DeleteForecast",
                json!({"ForecastArn": f_arn.clone()})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DeletePredictor",
                json!({"PredictorArn": pred_arn.clone()})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DescribePredictor",
                json!({"PredictorArn": pred_arn})
            ))
            .status,
            400
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DeleteDatasetGroup",
                json!({"DatasetGroupArn": group_arn})
            ))
            .status,
            200
        );
    }

    #[test]
    fn auto_predictor_and_forecasts() {
        let auto = dispatch(&fmt_req(
            "CreateAutoPredictor",
            json!({
                "PredictorName": "auto-pred",
                "ForecastHorizon": 6,
                "ForecastFrequency": "1D",
            }),
        ));
        assert_eq!(auto.status, 200);
        let auto_arn = body_of(&auto)["PredictorArn"].as_str().unwrap().to_string();

        let described = dispatch(&fmt_req(
            "DescribeAutoPredictor",
            json!({"PredictorArn": auto_arn.clone()}),
        ));
        assert_eq!(body_of(&described)["ForecastHorizon"], 6);

        let fcst = dispatch(&fmt_req(
            "CreateForecast",
            json!({"ForecastName": "f-auto", "PredictorArn": auto_arn.clone()}),
        ));
        let f_arn = body_of(&fcst)["ForecastArn"].as_str().unwrap().to_string();
        let f_desc = dispatch(&fmt_req(
            "DescribeForecast",
            json!({"ForecastArn": f_arn.clone()}),
        ));
        assert_eq!(body_of(&f_desc)["CreatedUsingAutoPredictor"], true);
        let metrics = dispatch(&fmt_req(
            "GetAccuracyMetrics",
            json!({"PredictorArn": auto_arn}),
        ));
        assert_eq!(body_of(&metrics)["IsAutoPredictor"], true);
        dispatch(&fmt_req("DeleteForecast", json!({"ForecastArn": f_arn})));

        // Auto predictors also prune via DeleteResourceTree.
        let result = dispatch(&fmt_req(
            "DeleteResourceTree",
            json!({"ResourceArn": auto_arn}),
        ));
        assert_eq!(result.status, 200);
        assert_eq!(
            dispatch(&fmt_req(
                "DescribeAutoPredictor",
                json!({"PredictorArn": auto_arn})
            ))
            .status,
            400
        );
    }

    #[test]
    fn monitor_explainability_and_exports() {
        let group = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "mon-dsg", "Domain": "CUSTOM"}),
        ));
        let group_arn = body_of(&group)["DatasetGroupArn"]
            .as_str()
            .unwrap()
            .to_string();
        let pred = dispatch(&fmt_req(
            "CreatePredictor",
            json!({
                "PredictorName": "mon-pred",
                "ForecastHorizon": 8,
                "InputDataConfig": {"DatasetGroupArn": group_arn},
                "FeaturizationConfig": {"ForecastFrequency": "1D"},
            }),
        ));
        let pred_arn = body_of(&pred)["PredictorArn"].as_str().unwrap().to_string();
        let fcst = dispatch(&fmt_req(
            "CreateForecast",
            json!({"ForecastName": "mon-f", "PredictorArn": pred_arn.clone()}),
        ));
        let f_arn = body_of(&fcst)["ForecastArn"].as_str().unwrap().to_string();

        // Monitor over the predictor.
        let mon = dispatch(&fmt_req(
            "CreateMonitor",
            json!({"MonitorName": "mon-1", "ResourceArn": pred_arn.clone()}),
        ));
        assert_eq!(mon.status, 200);
        let mon_arn = body_of(&mon)["MonitorArn"].as_str().unwrap().to_string();
        let described = dispatch(&fmt_req(
            "DescribeMonitor",
            json!({"MonitorArn": mon_arn.clone()}),
        ));
        assert_eq!(body_of(&described)["ResourceArn"], pred_arn.clone());
        let evals = dispatch(&fmt_req(
            "ListMonitorEvaluations",
            json!({"MonitorArn": mon_arn.clone()}),
        ));
        assert!(body_of(&evals)["PredictorMonitorEvaluations"].is_array());
        assert_eq!(
            dispatch(&fmt_req(
                "ListMonitorEvaluations",
                json!({"MonitorArn": mon_arn})
            ))
            .status,
            200
        );

        // Monitor parent must exist.
        let bad_mon = dispatch(&fmt_req(
            "CreateMonitor",
            json!({"MonitorName": "mon-bad", "ResourceArn": "arn:aws:forecast:us-east-1:482409018203:predictor/none"}),
        ));
        assert_eq!(bad_mon.status, 400);

        // Explainability + export.
        let exp = dispatch(&fmt_req(
            "CreateExplainability",
            json!({
                "ExplainabilityName": "exp-1",
                "ResourceArn": pred_arn.clone(),
                "ExplainabilityConfig": {
                    "TimeSeriesGranularity": "ALL",
                    "TimePointGranularity": "ALL"
                },
            }),
        ));
        assert_eq!(exp.status, 200);
        let exp_arn = body_of(&exp)["ExplainabilityArn"]
            .as_str()
            .unwrap()
            .to_string();

        let exp_export = dispatch(&fmt_req(
            "CreateExplainabilityExport",
            json!({
                "ExplainabilityExportName": "exp-export",
                "ExplainabilityArn": exp_arn.clone(),
                "Destination": {"S3Config": {"Path": "s3://b/p", "RoleArn": "arn:aws:iam::482409018203:role/x"}},
            }),
        ));
        assert_eq!(exp_export.status, 200);
        let exports = dispatch(&fmt_req("ListExplainabilityExports", json!({})));
        assert!(!body_of(&exports)["ExplainabilityExports"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            dispatch(&fmt_req(
                "CreateExplainabilityExport",
                json!({
                    "ExplainabilityExportName": "exp-export-bad",
                    "ExplainabilityArn": "arn:aws:forecast:us-east-1:482409018203:explainability/none",
                    "Destination": {"S3Config": {"Path": "s3://b/p", "RoleArn": "arn:aws:iam::482409018203:role/x"}},
                })
            )).status,
            400
        );

        // Forecast export + backtest export.
        let fexport = dispatch(&fmt_req(
            "CreateForecastExportJob",
            json!({
                "ForecastExportJobName": "f-export",
                "ForecastArn": f_arn.clone(),
                "Destination": {"S3Config": {"Path": "s3://b/p", "RoleArn": "arn:aws:iam::482409018203:role/x"}},
            }),
        ));
        assert_eq!(fexport.status, 200);
        let bt_export = dispatch(&fmt_req(
            "CreatePredictorBacktestExportJob",
            json!({
                "PredictorBacktestExportJobName": "bt-export",
                "PredictorArn": pred_arn.clone(),
                "Destination": {"S3Config": {"Path": "s3://b/p", "RoleArn": "arn:aws:iam::482409018203:role/x"}},
            }),
        ));
        assert_eq!(bt_export.status, 200);

        assert_eq!(
            dispatch(&fmt_req("DeleteMonitor", json!({"MonitorArn": mon_arn}))).status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DeleteExplainabilityExport",
                json!({"ExplainabilityExportArn": body_of(&exp_export)["ExplainabilityExportArn"]})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DeleteExplainability",
                json!({"ExplainabilityArn": exp_arn})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "DeleteForecastExportJob",
                json!({"ForecastExportJobArn": body_of(&fexport)["ForecastExportJobArn"]})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req("DeletePredictorBacktestExportJob", json!({"PredictorBacktestExportJobArn": body_of(&bt_export)["PredictorBacktestExportJobArn"]}))).status,
            200
        );
    }

    #[test]
    fn what_if_chain_and_stop_resume() {
        let group = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "wf-dsg", "Domain": "CUSTOM"}),
        ));
        let group_arn = body_of(&group)["DatasetGroupArn"]
            .as_str()
            .unwrap()
            .to_string();
        let pred = dispatch(&fmt_req(
            "CreatePredictor",
            json!({
                "PredictorName": "wf-pred",
                "ForecastHorizon": 5,
                "InputDataConfig": {"DatasetGroupArn": group_arn},
                "FeaturizationConfig": {"ForecastFrequency": "1W"},
            }),
        ));
        let pred_arn = body_of(&pred)["PredictorArn"].as_str().unwrap().to_string();
        let fcst = dispatch(&fmt_req(
            "CreateForecast",
            json!({"ForecastName": "wf-f", "PredictorArn": pred_arn}),
        ));
        let f_arn = body_of(&fcst)["ForecastArn"].as_str().unwrap().to_string();

        // Stop/Resume over an existing resource (both no-op 200).
        assert_eq!(
            dispatch(&fmt_req(
                "StopResource",
                json!({"ResourceArn": f_arn.clone()})
            ))
            .status,
            200
        );
        assert_eq!(
            dispatch(&fmt_req(
                "ResumeResource",
                json!({"ResourceArn": f_arn.clone()})
            ))
            .status,
            200
        );
        let bad_stop = dispatch(&fmt_req(
            "StopResource",
            json!({"ResourceArn": "arn:aws:forecast:us-east-1:482409018203:dataset/nope"}),
        ));
        assert_eq!(bad_stop.status, 400);

        // What-if chain: analysis -> forecast -> export.
        let wf = dispatch(&fmt_req(
            "CreateWhatIfAnalysis",
            json!({"WhatIfAnalysisName": "wf-a", "ForecastArn": f_arn.clone()}),
        ));
        assert_eq!(wf.status, 200);
        let wfa_arn = body_of(&wf)["WhatIfAnalysisArn"]
            .as_str()
            .unwrap()
            .to_string();

        let bad_wff = dispatch(&fmt_req(
            "CreateWhatIfForecast",
            json!({
                "WhatIfForecastName": "wff-bad",
                "WhatIfAnalysisArn": "arn:aws:forecast:us-east-1:482409018203:what-if-analysis/none",
            }),
        ));
        assert_eq!(bad_wff.status, 400);

        let wff = dispatch(&fmt_req(
            "CreateWhatIfForecast",
            json!({
                "WhatIfForecastName": "wff-1",
                "WhatIfAnalysisArn": wfa_arn.clone(),
            }),
        ));
        assert_eq!(wff.status, 200);
        let wff_arn = body_of(&wff)["WhatIfForecastArn"]
            .as_str()
            .unwrap()
            .to_string();

        let wfe = dispatch(&fmt_req(
            "CreateWhatIfForecastExport",
            json!({
                "WhatIfForecastExportName": "wff-export",
                "WhatIfForecastArns": [wff_arn.clone()],
                "Destination": {"S3Config": {"Path": "s3://b/p", "RoleArn": "arn:aws:iam::482409018203:role/x"}},
            }),
        ));
        assert_eq!(wfe.status, 200);

        // Lists cover every family (no 501 paths remain).
        for (msg, key) in [
            ("ListWhatIfAnalyses", "WhatIfAnalyses"),
            ("ListWhatIfForecasts", "WhatIfForecasts"),
            ("ListWhatIfForecastExports", "WhatIfForecastExports"),
            ("ListForecasts", "Forecasts"),
        ] {
            let listed = dispatch(&fmt_req(msg, json!({})));
            assert!(body_of(&listed).get(key).unwrap().is_array(), "{}", msg);
        }

        // Prune the whole chain through DeleteResourceTree.
        for arn in [
            wfe_arn(&body_of(&wfe)),
            wff_arn,
            wfa_arn,
            f_arn,
            pred_arn,
            group_arn,
        ] {
            let del = dispatch(&fmt_req("DeleteResourceTree", json!({"ResourceArn": arn})));
            assert_eq!(del.status, 200);
        }
    }

    fn wfe_arn(v: &serde_json::Value) -> String {
        v["WhatIfForecastExportArn"].as_str().unwrap().to_string()
    }

    #[test]
    fn tags_roundtrip() {
        let group = dispatch(&fmt_req(
            "CreateDatasetGroup",
            json!({"DatasetGroupName": "tag-dsg", "Domain": "CUSTOM"}),
        ));
        let arn = body_of(&group)["DatasetGroupArn"]
            .as_str()
            .unwrap()
            .to_string();

        let tagged = dispatch(&fmt_req(
            "TagResource",
            json!({"ResourceArn": arn.clone(), "Tags": [
                {"Key": "team", "Value": "coverage"},
                {"Key": "wave", "Value": "11"}
            ]}),
        ));
        assert_eq!(tagged.status, 200);
        assert!(tagged.body.is_empty());

        let tags = dispatch(&fmt_req(
            "ListTagsForResource",
            json!({"ResourceArn": arn.clone()}),
        ));
        let tags_v = body_of(&tags);
        let rows = tags_v["Tags"].as_array().unwrap();
        assert_eq!(rows.len(), 2);

        let untagged = dispatch(&fmt_req(
            "UntagResource",
            json!({"ResourceArn": arn.clone(), "TagKeys": ["wave"]}),
        ));
        assert_eq!(untagged.status, 200);
        let tags = dispatch(&fmt_req(
            "ListTagsForResource",
            json!({"ResourceArn": arn.clone()}),
        ));
        assert_eq!(body_of(&tags)["Tags"].as_array().unwrap().len(), 1);

        // Unknown arn -> RFNF on every tag route.
        let unknown = "arn:aws:forecast:us-east-1:482409018203:dataset-group/none";
        for msg in ["TagResource", "UntagResource", "ListTagsForResource"] {
            let r = dispatch(&fmt_req(msg, json!({"ResourceArn": arn_no()})));
            assert_eq!(r.status, 400, "{}", msg);
            assert_eq!(
                body_of(&r)["__type"],
                "ResourceNotFoundException",
                "{}",
                msg
            );
        }
        let r = dispatch(&fmt_req(
            "TagResource",
            json!({"ResourceArn": unknown, "Tags": [{"Key": "k", "Value": "v"}]}),
        ));
        assert_eq!(r.status, 400);
    }

    fn arn_no() -> String {
        "arn:aws:forecast:us-east-1:482409018203:dataset-group/none".to_string()
    }
}
