//! Athena backend — Amazon Athena (JSON protocol, `X-Amz-Target:
//! AmazonAthena.<Op>` over `application/x-amz-json-1.1`).
//!
//! Replaces the moto sidecar for `athena`. State is per-(account, region),
//! mirroring moto's `AthenaBackend`: work groups seeded with `primary`,
//! query executions that advance QUEUED → SUCCEEDED on first describe
//! (moto's `ManagedState` "immediate" progression), named queries, data
//! catalogs, capacity reservations, prepared statements, sessions,
//! notebooks and calculation executions. Tags live in a moto
//! `TaggingService`-shaped store keyed by resource ARN.
//!
//! Shape parity notes (verified against the sidecar):
//! - Errors are `{"__type": code, "message": ...}` HTTP 400: `error(...)`
//!   paths use `InvalidRequestException`, raised
//!   `InvalidArgumentException` paths use that code.
//! - `GetNamedQuery` on a missing id crashes moto with
//!   `'NoneType' object has no attribute 'name'` — crash-faithful as the
//!   werkzeug HTML 500 moto's server answers for unhandled exceptions.
//! - `CreateCapacityReservation` without `Tags` succeeds: moto's
//!   `CapacityReservation.__init__` normalizes `tags or []` and
//!   `TaggingService.tag_resource` no-ops on `None`.
//! - `GetPreparedStatement` on a missing name surfaces moto's
//!   `AthenaClientError` body verbatim (`Error`/`RequestId` envelope).
//! - Unknown operations answer `501 NotImplemented` like the sidecar.

use crate::proto::{json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

type OpResult = Result<Value, Response>;

fn invalid_request(message: &str) -> Response {
    json_error(400, "InvalidRequestException", message)
}

fn invalid_argument(message: &str) -> Response {
    json_error(400, "InvalidArgumentException", message)
}

/// moto crash on `GetNamedQuery` with a missing id (`nq.name` on None).
///
/// Crash-faithful: unhandled AttributeError → werkzeug HTML 500.
fn not_handled() -> Response {
    crate::proto::moto_crash_response()
}

/// moto `executions[<id>]` KeyError surfacing as the same werkzeug crash.
fn key_error(_id: &str) -> Response {
    crate::proto::moto_crash_response()
}

/// moto `AthenaClientError` (JsonRESTError) wire body.
fn athena_client_error(code: &str, message: &str) -> Response {
    let mut resp = Response::json_status(
        400,
        &json!({
            "Error": {
                "Code": code,
                "Message": message,
                "Type": "InvalidRequestException",
            },
            "RequestId": "6876f774-7273-11e4-85dc-39e55ca848d1",
        }),
    );
    resp.headers
        .push(("Content-Type".to_string(), "application/json".to_string()));
    resp.headers.push((
        "x-amzn-ErrorType".to_string(),
        "InvalidRequestException".to_string(),
    ));
    resp
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// moto `TaggingService`: ARN → ordered (key, value) tags.
#[derive(Default)]
struct Tagger {
    tags: HashMap<String, Vec<(String, Option<String>)>>,
}

impl Tagger {
    /// `tag_resource`: absent/null tags are a no-op while `[]` creates the
    /// empty entry moto's dict-assignment produces; a `Key` without a
    /// `Value` stores `None`; later tags for the same key update in place.
    fn tag_resource(&mut self, arn: &str, tags: Option<&Value>) {
        let Some(list) = tags else {
            return;
        };
        let Value::Array(items) = list else {
            return;
        };
        let entry = self.tags.entry(arn.to_string()).or_default();
        for tag in items {
            let (key, value) = (
                tag.get("Key").and_then(Value::as_str),
                tag.get("Value").and_then(Value::as_str),
            );
            let Some(key) = key else { continue };
            if entry.iter().any(|(k, _)| k == key) {
                for (k, v) in entry.iter_mut() {
                    if k == key {
                        *v = value.map(String::from);
                    }
                }
            } else {
                entry.push((key.to_string(), value.map(String::from)));
            }
        }
    }

    fn untag_resource(&mut self, arn: &str, keys: &[String]) {
        if let Some(entry) = self.tags.get_mut(arn) {
            entry.retain(|(k, _)| !keys.contains(k));
        }
    }

    fn delete_all(&mut self, arn: &str) {
        self.tags.remove(arn);
    }

    fn list(&self, arn: &str) -> Option<Value> {
        self.tags.get(arn).map(|entry| {
            Value::Array(
                entry
                    .iter()
                    .map(|(k, v)| match v {
                        Some(value) => json!({ "Key": k, "Value": value }),
                        None => json!({ "Key": k }),
                    })
                    .collect(),
            )
        })
    }
}

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

fn resource_arn(region: &str, account: u64, kind: &str, name: &str) -> String {
    format!(
        "arn:{p}:athena:{region}:{account}:{kind}/{name}",
        p = partition(region)
    )
}

struct WorkGroup {
    name: String,
    description: Value,
    configuration: Map<String, Value>,
    /// Class attribute in moto; per-instance once `UpdateWorkGroup` sets it.
    state: String,
}

impl WorkGroup {
    /// moto's constructor default-configuration fill applied to every
    /// `CreateWorkGroup` request (explicit values win).
    fn new(name: &str, description: Value, configuration: Option<&Value>) -> Self {
        let mut configuration = configuration
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let defaults: [(&str, Value); 5] = [
            ("EnableMinimumEncryptionConfiguration", Value::Bool(false)),
            ("EnforceWorkGroupConfiguration", Value::Bool(true)),
            (
                "EngineVersion",
                json!({
                    "EffectiveEngineVersion": "Athena engine version 3",
                    "SelectedEngineVersion": "AUTO",
                }),
            ),
            ("PublishCloudWatchMetricsEnabled", Value::Bool(false)),
            ("RequesterPaysEnabled", Value::Bool(false)),
        ];
        for (key, value) in defaults {
            configuration.entry(key.to_string()).or_insert(value);
        }
        WorkGroup {
            name: name.to_string(),
            description,
            configuration,
            state: "ENABLED".to_string(),
        }
    }
}

/// moto `Execution`: `ManagedState` with
/// `[("QUEUED", "RUNNING"), ("RUNNING", "SUCCEEDED")]` — under the default
/// "immediate" progression the first status read lands on SUCCEEDED.
struct Execution {
    query: Value,
    context: Value,
    /// Caller's `ResultConfiguration` with the `{id}.csv` output-location
    /// suffix merged in at start time (moto mutates the dict in place).
    config: Value,
    workgroup: Option<String>,
    execution_parameters: Option<Value>,
    start_time: f64,
    end_time: f64,
    status: String,
}

impl Execution {
    fn new(
        query: Value,
        context: Value,
        config: Option<Value>,
        execution_parameters: Option<Value>,
        exec_id: &str,
    ) -> Self {
        let now = crate::proto::now_unix();
        let mut config = config.unwrap_or(Value::Null);
        if let Some(cfg) = config.as_object_mut() {
            if let Some(location) = cfg.get("OutputLocation").and_then(Value::as_str) {
                let location = if location.ends_with('/') {
                    location.to_string()
                } else {
                    format!("{location}/")
                };
                cfg.insert(
                    "OutputLocation".to_string(),
                    Value::String(format!("{location}{exec_id}.csv")),
                );
            }
        }
        Execution {
            query,
            context,
            config,
            workgroup: None,
            execution_parameters,
            start_time: now,
            end_time: now,
            status: "QUEUED".to_string(),
        }
    }

    /// moto's "immediate" progression: every status read flips
    /// QUEUED/RUNNING to SUCCEEDED; CANCELLED is terminal.
    fn advance(&mut self) {
        if self.status == "QUEUED" || self.status == "RUNNING" {
            self.status = "SUCCEEDED".to_string();
        }
    }
}

/// The `GetQueryExecution` / `BatchGetQueryExecution` item shape.
fn execution_json(exec_id: &str, execution: &Execution) -> Value {
    let query_upper = execution.query.as_str().unwrap_or_default().to_uppercase();
    let statement_type = if ["ALTER", "CREATE", "DESCRIBE", "DROP", "MSCK", "SHOW"]
        .iter()
        .any(|ddl| query_upper.starts_with(ddl))
    {
        "DDL"
    } else {
        "DML"
    };
    let mut execution_obj = json!({
        "QueryExecutionId": exec_id,
        "Query": execution.query,
        "StatementType": statement_type,
        "ResultConfiguration": execution.config,
        "ResultReuseConfiguration": {
            "ResultReuseByAgeConfiguration": {"Enabled": false},
        },
        "QueryExecutionContext": execution.context,
        "Status": {
            "State": execution.status,
            "SubmissionDateTime": execution.start_time,
            "CompletionDateTime": execution.end_time,
        },
        "Statistics": {
            "EngineExecutionTimeInMillis": 0,
            "DataScannedInBytes": 0,
            "TotalExecutionTimeInMillis": 0,
            "QueryQueueTimeInMillis": 0,
            "ServicePreProcessingTimeInMillis": 0,
            "QueryPlanningTimeInMillis": 0,
            "ServiceProcessingTimeInMillis": 0,
            "ResultReuseInformation": {"ReusedPreviousResult": false},
        },
        "WorkGroup": execution.workgroup.clone(),
    });
    if let Some(params) = &execution.execution_parameters {
        execution_obj["ExecutionParameters"] = params.clone();
    }
    execution_obj
}

struct NamedQuery {
    name: String,
    description: Value,
    database: String,
    query_string: String,
    workgroup: String,
}

impl NamedQuery {
    /// The shape both `GetNamedQuery` and `BatchGetNamedQuery` return.
    fn to_json(&self, id: &str) -> Value {
        json!({
            "Name": self.name,
            "Description": self.description,
            "Database": self.database,
            "QueryString": self.query_string,
            "NamedQueryId": id,
            "WorkGroup": self.workgroup,
        })
    }
}

struct DataCatalog {
    catalog_type: Value,
    description: Value,
    parameters: Value,
}

impl DataCatalog {
    fn get_json(&self, name: &str) -> Value {
        json!({
            "Name": name,
            "Description": self.description,
            "Type": self.catalog_type,
            "Parameters": self.parameters,
        })
    }
}

struct CapacityReservation {
    target_dpus: Value,
    /// Raw request tag list echoed by `GetCapacityReservation` (moto stores
    /// the caller's list on the object).
    tags: Option<Value>,
}

struct PreparedStatement {
    workgroup: String,
    query_statement: String,
    description: Value,
    last_modified: f64,
}

struct Database {
    description: Value,
    parameters: Value,
}

#[derive(Default)]
struct TableMetadata {
    table_type: String,
    columns: Value,
    partition_keys: Value,
    parameters: Value,
    create_time: f64,
}

impl TableMetadata {
    fn to_json(&self, name: &str) -> Value {
        json!({
            "Name": name,
            "TableType": self.table_type,
            "Columns": self.columns,
            "PartitionKeys": self.partition_keys,
            "Parameters": self.parameters,
            "CreateTime": self.create_time,
        })
    }
}

struct Notebook {
    name: String,
    workgroup: String,
    notebook_type: Value,
    creation_time: f64,
    last_modified: f64,
    content: String,
}

impl Notebook {
    /// moto `get_notebook_metadata` / `export_notebook` metadata block.
    fn metadata_json(&self, id: &str) -> Value {
        json!({
            "NotebookId": id,
            "Name": self.name,
            "WorkGroup": self.workgroup,
            "CreationTime": self.creation_time,
            "Type": self.notebook_type,
            "LastModifiedTime": self.last_modified,
        })
    }
}

struct Calculation {
    session_id: String,
    description: String,
    code_block: String,
    state: String,
    submission: f64,
    completion: Option<f64>,
}

struct Session {
    workgroup: String,
    engine_configuration: Value,
    notebook_version: String,
    description: String,
    state: String,
    start_time: f64,
    end_time: Option<f64>,
}

impl Session {
    fn status_json(&self) -> Value {
        let mut status = json!({
            "State": self.state,
            "StartDateTime": self.start_time,
        });
        if let Some(end) = self.end_time {
            status["EndDateTime"] = json!(end);
        }
        status
    }

    fn to_json(&self, id: &str) -> Value {
        json!({
            "SessionId": id,
            "WorkGroup": self.workgroup,
            "EngineConfiguration": self.engine_configuration,
            "NotebookVersion": self.notebook_version,
            "Description": self.description.clone(),
            "Status": self.status_json(),
        })
    }
}

#[derive(Default)]
struct AthenaState {
    /// Vec-backed so list order matches moto's instance dicts.
    work_groups: Vec<(String, WorkGroup)>,
    executions: Vec<(String, Execution)>,
    named_queries: Vec<(String, NamedQuery)>,
    data_catalogs: Vec<(String, DataCatalog)>,
    capacity_reservations: Vec<(String, CapacityReservation)>,
    prepared_statements: Vec<(String, PreparedStatement)>,
    databases: HashMap<String, HashMap<String, Database>>,
    tables: HashMap<String, HashMap<String, TableMetadata>>,
    sessions: HashMap<String, Session>,
    notebooks: HashMap<String, Notebook>,
    calculation_executions: HashMap<String, Calculation>,
    tagger: Tagger,
}

impl AthenaState {
    /// moto `__init__`: seed the `primary` work group (default
    /// configuration, `EnforceWorkGroupConfiguration: false`). The seed
    /// call passes `tags=[]`, which still creates an empty tagger entry —
    /// `ListTagsForResource` on the primary ARN answers `{"Tags": []}`.
    fn init(region: &str, account: u64) -> Self {
        let mut st = AthenaState::default();
        let primary = WorkGroup::new(
            "primary",
            Value::String(String::new()),
            Some(&json!({
                "ResultConfiguration": {},
                "EnforceWorkGroupConfiguration": false,
            })),
        );
        st.work_groups.push(("primary".to_string(), primary));
        let primary_arn = resource_arn(region, account, "workgroup", "primary");
        st.tagger.tag_resource(&primary_arn, Some(&json!([])));
        st
    }

    fn get_work_group(&self, name: &str) -> Option<&WorkGroup> {
        self.work_groups
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, wg)| wg)
    }

    /// Shared guard behind StartQueryExecution / CreateNamedQuery /
    /// CreatePreparedStatement / StartSession / notebook creates: an
    /// empty work-group name skips the check (moto's truthiness test).
    fn require_work_group(&self, name: &str) -> Result<(), String> {
        if !name.is_empty() && self.get_work_group(name).is_none() {
            return Err("WorkGroup does not exist".to_string());
        }
        Ok(())
    }
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut AthenaState) -> T) -> T {
    let slot = store::slot::<AthenaState>("athena");
    let mut map = slot.write();
    f(map
        .entry((account, region.to_string()))
        .or_insert_with(|| AthenaState::init(region, account)))
}

// ---------------------------------------------------------------------------
// NextToken (moto Paginate: base64 JSON {"uniqueAttributes": "<id>"})
// ---------------------------------------------------------------------------

fn encode_token(id: &str) -> String {
    B64.encode(json!({"uniqueAttributes": id}).to_string())
}

/// Decoded next-token id (`Invalid token` on malformed input, like moto's
/// `InvalidToken` JsonRESTError).
fn decode_token(token: &str) -> Result<String, Response> {
    let bytes = B64
        .decode(token.as_bytes())
        .map_err(|_| invalid_request("Invalid token"))?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid_request("Invalid token"))?;
    let attrs = value
        .get("uniqueAttributes")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    // Multi-attribute tokens join on '|'; athena always has exactly one.
    Ok(attrs.split('|').next().unwrap_or_default().to_string())
}

// ---------------------------------------------------------------------------
// Query execution lifecycle
// ---------------------------------------------------------------------------

fn find_execution<'a>(st: &'a mut AthenaState, exec_id: &str) -> Option<&'a mut Execution> {
    st.executions
        .iter_mut()
        .find(|(key, _)| *key == exec_id)
        .map(|(_, execution)| execution)
}

fn advance_execution<F>(
    st: &mut AthenaState,
    exec_id: &str,
    f: impl FnOnce(&Execution) -> F,
) -> Option<F> {
    let execution = find_execution(st, exec_id)?;
    execution.advance();
    Some(f(execution))
}

fn op_start_query_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let query = body.get("QueryString").cloned().unwrap_or(Value::Null);
    let context = body
        .get("QueryExecutionContext")
        .cloned()
        .unwrap_or(Value::Null);
    let config = body
        .get("ResultConfiguration")
        .filter(|config| !config.is_null())
        .cloned();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let execution_parameters = body
        .get("ExecutionParameters")
        .filter(|params| !params.is_null())
        .cloned();
    st.require_work_group(&workgroup)
        .map_err(|e| invalid_request(&e))?;
    let exec_id = uuid::Uuid::new_v4().to_string();
    let mut execution = Execution::new(query, context, config, execution_parameters, &exec_id);
    if !workgroup.is_empty() {
        execution.workgroup = Some(workgroup);
    }
    st.executions.push((exec_id.clone(), execution));
    Ok(json!({ "QueryExecutionId": exec_id }))
}

fn op_get_query_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("QueryExecutionId").and_then(Value::as_str) else {
        // moto: `self.executions[exec_id]` KeyError surfaces as 500.
        return Err(key_error(""));
    };
    let Some(describe) = advance_execution(st, id, |e| execution_json(id, e)) else {
        return Err(key_error(id));
    };
    Ok(json!({ "QueryExecution": describe }))
}

fn op_list_query_executions(body: &Value, st: &mut AthenaState) -> OpResult {
    let workgroup = body.get("WorkGroup").and_then(Value::as_str);
    let ids: Vec<Value> = st
        .executions
        .iter()
        .filter(|(_, execution)| match workgroup {
            Some(wg) => execution.workgroup.as_deref() == Some(wg),
            None => true,
        })
        .map(|(id, _)| json!(id))
        .collect();
    Ok(json!({ "QueryExecutionIds": ids }))
}

fn op_stop_query_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let id = body
        .get("QueryExecutionId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(execution) = find_execution(st, id) else {
        return Err(key_error(id));
    };
    execution.status = "CANCELLED".to_string();
    Ok(json!({}))
}

fn op_batch_get_query_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let ids = body
        .get("QueryExecutionIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut executions = Vec::new();
    let mut unprocessed = Vec::new();
    for id in ids {
        let Some(id_str) = id.as_str() else { continue };
        match advance_execution(st, id_str, |e| execution_json(id_str, e)) {
            Some(describe) => executions.push(describe),
            None => unprocessed.push(json!({
                "QueryExecutionId": id_str,
                "ErrorCode": "INVALID_INPUT",
                "ErrorMessage": "INVALID_INPUT",
            })),
        }
    }
    Ok(json!({
        "QueryExecutions": executions,
        "UnprocessedQueryExecutionIds": unprocessed,
    }))
}

/// moto `get_query_results`: reading the status flips a QUEUED execution
/// to SUCCEEDED, so only stopped (CANCELLED) executions error out; the
/// predefined-results queue stays empty in rotocore (no moto-api), so the
/// answer is always the empty result set.
fn op_get_query_results(body: &Value, st: &mut AthenaState) -> OpResult {
    if let Some(id) = body.get("QueryExecutionId").and_then(Value::as_str) {
        if let Some(execution) = find_execution(st, id) {
            execution.advance();
            if execution.status != "SUCCEEDED" {
                return Err(invalid_request(&format!(
                    "Query has not yet finished. Current state: {}",
                    execution.status
                )));
            }
        }
    }
    Ok(json!({
        "ResultSet": {
            "Rows": [],
            "ResultSetMetadata": {"ColumnInfo": []},
        },
    }))
}

fn op_get_query_runtime_statistics(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("QueryExecutionId").and_then(Value::as_str) else {
        return Err(invalid_request("QueryExecution  was not found"));
    };
    match advance_execution(st, id, |e| e.status.clone()) {
        None => Err(invalid_request(&format!(
            "QueryExecution {id} was not found"
        ))),
        Some(state) => Ok(json!({
            "QueryRuntimeStatistics": {
                "OutputStage": {
                    "ExecutionTime": 100,
                    "InputBytes": 0,
                    "InputRows": 0,
                    "OutputBytes": 1,
                    "OutputRows": 1,
                    "StageId": 1,
                    "State": state,
                },
                "Rows": {
                    "InputBytes": 0,
                    "InputRows": 0,
                    "OutputBytes": 2,
                    "OutputRows": 2,
                },
                "Timeline": {
                    "EngineExecutionTimeInMillis": 0,
                    "QueryPlanningTimeInMillis": 0,
                    "QueryQueueTimeInMillis": 0,
                    "ServicePreProcessingTimeInMillis": 0,
                    "ServiceProcessingTimeInMillis": 0,
                    "TotalExecutionTimeInMillis": 0,
                },
            }
        })),
    }
}

// ---------------------------------------------------------------------------
// Work groups + resource tags
// ---------------------------------------------------------------------------

fn op_create_work_group(
    body: &Value,
    st: &mut AthenaState,
    account: u64,
    region: &str,
) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if st.work_groups.iter().any(|(key, _)| *key == name) {
        return Err(invalid_request("WorkGroup already exists"));
    }
    let description = body.get("Description").cloned().unwrap_or(Value::Null);
    let wg = WorkGroup::new(&name, description, body.get("Configuration"));
    st.work_groups.push((name.clone(), wg));
    let arn = resource_arn(region, account, "workgroup", &name);
    st.tagger.tag_resource(&arn, body.get("Tags"));
    Ok(json!({
        "CreateWorkGroupResponse": {
            "ResponseMetadata": {
                "RequestId": "384ac68d-3775-11df-8963-01868b7c937a",
            },
        },
    }))
}

fn op_list_work_groups(st: &AthenaState) -> OpResult {
    let groups: Vec<Value> = st
        .work_groups
        .iter()
        .map(|(key, wg)| {
            json!({
                "Name": key,
                "State": wg.state,
                "Description": wg.description,
                "CreationTime": crate::proto::now_unix(),
            })
        })
        .collect();
    Ok(json!({ "WorkGroups": groups }))
}

fn op_get_work_group(body: &Value, st: &AthenaState) -> OpResult {
    let name = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match st.get_work_group(name) {
        None => Ok(json!({ "WorkGroup": Value::Null })),
        Some(wg) => Ok(json!({
            "WorkGroup": {
                "Name": wg.name,
                "State": wg.state,
                "Configuration": wg.configuration,
                "Description": wg.description,
                "CreationTime": crate::proto::now_unix(),
            }
        })),
    }
}

fn op_update_work_group(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some((_, wg)) = st.work_groups.iter_mut().find(|(key, _)| *key == name) else {
        return Err(invalid_request("WorkGroup does not exist"));
    };
    if let Some(description) = body.get("Description").filter(|d| !d.is_null()) {
        wg.description = description.clone();
    }
    if let Some(state) = body.get("State").filter(|s| !s.is_null()) {
        wg.state = state.as_str().unwrap_or_default().to_string();
    }
    if let Some(updates) = body
        .get("ConfigurationUpdates")
        .and_then(Value::as_object)
        .filter(|updates| !updates.is_empty())
    {
        for (key, value) in updates {
            wg.configuration.insert(key.clone(), value.clone());
        }
    }
    Ok(json!({}))
}

fn op_delete_work_group(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.work_groups.retain(|(key, _)| *key != name);
    Ok(json!({}))
}

fn op_list_tags_for_resource(body: &Value, st: &AthenaState) -> OpResult {
    let arn = body
        .get("ResourceARN")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match st.tagger.list(arn) {
        Some(tags) => Ok(json!({ "Tags": tags })),
        None => Err(invalid_request(&format!(
            "Athena Resource, {arn} Does Not Exist"
        ))),
    }
}

fn op_tag_resource(body: &Value, st: &mut AthenaState) -> OpResult {
    let arn = body
        .get("ResourceARN")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.tagger.tag_resource(&arn, body.get("Tags"));
    Ok(json!({}))
}

fn op_untag_resource(body: &Value, st: &mut AthenaState) -> OpResult {
    let arn = body
        .get("ResourceARN")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let keys: Vec<String> = body
        .get("TagKeys")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    st.tagger.untag_resource(&arn, &keys);
    Ok(json!({}))
}

// ---------------------------------------------------------------------------
// Named queries
// ---------------------------------------------------------------------------

fn op_create_named_query(body: &Value, st: &mut AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .filter(|wg| !wg.is_empty())
        .unwrap_or("primary")
        .to_string();
    if let Some(wg) = body.get("WorkGroup").and_then(Value::as_str) {
        st.require_work_group(wg).map_err(|e| invalid_request(&e))?;
    }
    let id = uuid::Uuid::new_v4().to_string();
    let nq = NamedQuery {
        name: body
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        description: body.get("Description").cloned().unwrap_or(Value::Null),
        database: body
            .get("Database")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        query_string: body
            .get("QueryString")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        workgroup: workgroup.to_string(),
    };
    st.named_queries.push((id.clone(), nq));
    Ok(json!({ "NamedQueryId": id }))
}

fn op_get_named_query(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("NamedQueryId").and_then(Value::as_str) else {
        return Err(invalid_request("NamedQueryId is required"));
    };
    match st.named_queries.iter().find(|(key, _)| *key == id) {
        Some((_, nq)) => Ok(json!({ "NamedQuery": nq.to_json(id) })),
        None => Err(not_handled()),
    }
}

fn op_delete_named_query(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NamedQueryId").and_then(Value::as_str) else {
        return Ok(json!({}));
    };
    st.named_queries.retain(|(key, _)| key != id);
    Ok(json!({}))
}

fn op_update_named_query(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NamedQueryId").and_then(Value::as_str) else {
        return Ok(json!({}));
    };
    if let Some((_, nq)) = st.named_queries.iter_mut().find(|(key, _)| *key == id) {
        let name = body.get("Name").and_then(Value::as_str);
        let description = body.get("Description").and_then(Value::as_str);
        let query_string = body.get("QueryString").and_then(Value::as_str);
        if let Some(name) = name {
            nq.name = name.to_string();
        }
        if let Some(description) = description {
            nq.description = Value::String(description.to_string());
        }
        if let Some(query_string) = query_string {
            nq.query_string = query_string.to_string();
        }
    }
    Ok(json!({}))
}

fn op_batch_get_named_query(body: &Value, st: &AthenaState) -> OpResult {
    let ids = body
        .get("NamedQueryIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut named_queries = Vec::new();
    let mut unprocessed = Vec::new();
    for id in ids {
        let Some(id_str) = id.as_str() else { continue };
        match st.named_queries.iter().find(|(key, _)| key == id_str) {
            Some((_, nq)) => named_queries.push(nq.to_json(id_str)),
            None => unprocessed.push(json!({
                "NamedQueryId": id_str,
                "ErrorCode": "INVALID_INPUT",
                "ErrorMessage": "INVALID_INPUT",
            })),
        }
    }
    Ok(json!({
        "NamedQueries": named_queries,
        "UnprocessedNamedQueryIds": unprocessed,
    }))
}

/// moto `list_named_queries` (Paginate): work-group filter, default
/// limit 50, tokens carry the first id of the next page.
fn op_list_named_queries(body: &Value, st: &AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .filter(|wg| !wg.is_empty())
        .unwrap_or("primary");
    let items: Vec<&(String, NamedQuery)> = st
        .named_queries
        .iter()
        .filter(|(_, nq)| nq.workgroup == workgroup)
        .collect();
    let max_results = body
        .get("MaxResults")
        .and_then(Value::as_i64)
        .filter(|n| *n > 0)
        .unwrap_or(50) as usize;
    let index_start = match body.get("NextToken").and_then(Value::as_str) {
        Some(token) => {
            let token_id = decode_token(token)?;
            items
                .iter()
                .position(|(key, _)| *key == token_id)
                .ok_or_else(|| invalid_request("Resource not found!"))?
        }
        None => 0,
    };
    let index_end = (index_start + max_results).min(items.len());
    let page = &items[index_start..index_end];
    let ids: Vec<Value> = page.iter().map(|(id, _)| json!(id)).collect();
    let mut response = json!({ "NamedQueryIds": ids });
    if !page.is_empty() && index_end < items.len() {
        response["NextToken"] = json!(encode_token(&items[index_end].0));
    }
    Ok(response)
}

// ---------------------------------------------------------------------------
// Data catalogs
// ---------------------------------------------------------------------------

fn op_create_data_catalog(
    body: &Value,
    st: &mut AthenaState,
    account: u64,
    region: &str,
) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if st.data_catalogs.iter().any(|(key, _)| *key == name) {
        return Err(invalid_request("DataCatalog already exists"));
    }
    let catalog = DataCatalog {
        catalog_type: body.get("Type").cloned().unwrap_or(Value::Null),
        description: body.get("Description").cloned().unwrap_or(Value::Null),
        parameters: body.get("Parameters").cloned().unwrap_or(Value::Null),
    };
    st.data_catalogs.push((name.clone(), catalog));
    let arn = resource_arn(region, account, "datacatalog", &name);
    st.tagger.tag_resource(&arn, body.get("Tags"));
    Ok(json!({
        "CreateDataCatalogResponse": {
            "ResponseMetadata": {
                "RequestId": "384ac68d-3775-11df-8963-01868b7c937a",
            },
        },
    }))
}

fn op_get_data_catalog(body: &Value, st: &AthenaState) -> OpResult {
    let name = body.get("Name").and_then(Value::as_str).unwrap_or_default();
    match st.data_catalogs.iter().find(|(key, _)| *key == name) {
        Some((key, catalog)) => Ok(json!({ "DataCatalog": catalog.get_json(key) })),
        None => Ok(json!({ "DataCatalog": Value::Null })),
    }
}

fn op_list_data_catalogs(st: &AthenaState) -> OpResult {
    let summary: Vec<Value> = st
        .data_catalogs
        .iter()
        .map(|(key, catalog)| {
            json!({
                "CatalogName": key,
                "Type": catalog.catalog_type,
            })
        })
        .collect();
    Ok(json!({ "DataCatalogsSummary": summary }))
}

fn op_delete_data_catalog(
    body: &Value,
    st: &mut AthenaState,
    account: u64,
    region: &str,
) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some(index) = st.data_catalogs.iter().position(|(key, _)| *key == name) else {
        return Err(invalid_argument(&format!(
            "DataCatalog {name} is not found"
        )));
    };
    st.data_catalogs.remove(index);
    st.tagger
        .delete_all(&resource_arn(region, account, "datacatalog", &name));
    Ok(json!({}))
}

fn op_update_data_catalog(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some((_, catalog)) = st.data_catalogs.iter_mut().find(|(key, _)| *key == name) else {
        return Err(invalid_argument(&format!(
            "DataCatalog {name} is not found"
        )));
    };
    if let Some(kind) = body.get("Type").filter(|t| !t.is_null()) {
        catalog.catalog_type = kind.clone();
    }
    if let Some(description) = body.get("Description").filter(|d| !d.is_null()) {
        catalog.description = description.clone();
    }
    if let Some(parameters) = body.get("Parameters").filter(|p| !p.is_null()) {
        catalog.parameters = parameters.clone();
    }
    Ok(json!({}))
}

// ---------------------------------------------------------------------------
// Capacity reservations
// ---------------------------------------------------------------------------

fn op_create_capacity_reservation(
    body: &Value,
    st: &mut AthenaState,
    account: u64,
    region: &str,
) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    // moto: `CapacityReservation.__init__` normalizes missing tags to the
    // empty list (moto/athena/models.py:474) and its None-tolerant tagger
    // tags the primary ARN with [] — Get answers Tags: [] for tagless
    // creates.
    let tags = Some(
        body.get("Tags")
            .filter(|tags| !tags.is_null())
            .cloned()
            .unwrap_or(json!([])),
    );
    let reservation = CapacityReservation {
        target_dpus: body.get("TargetDpus").cloned().unwrap_or(Value::Null),
        tags: tags.clone(),
    };
    if let Some(tag_list) = tags.as_ref() {
        st.tagger.tag_resource(
            &resource_arn(region, account, "capacity-reservation", &name),
            Some(tag_list),
        );
    }
    st.capacity_reservations.push((name.clone(), reservation));
    Ok(json!({}))
}

fn op_get_capacity_reservation(body: &Value, st: &AthenaState) -> OpResult {
    let name = body.get("Name").and_then(Value::as_str).unwrap_or_default();
    match st
        .capacity_reservations
        .iter()
        .find(|(key, _)| *key == name)
    {
        Some((key, reservation)) => Ok(json!({
            "CapacityReservation": {
                "Name": key,
                "TargetDpus": reservation.target_dpus,
                "Tags": reservation.tags.clone(),
            }
        })),
        None => Err(invalid_request("Capacity reservation does not exist")),
    }
}

fn op_list_capacity_reservations(st: &AthenaState) -> OpResult {
    let reservations: Vec<Value> = st
        .capacity_reservations
        .iter()
        .map(|(key, reservation)| {
            json!({
                "Name": key,
                "TargetDpus": reservation.target_dpus,
                "CreationTime": crate::proto::now_unix(),
            })
        })
        .collect();
    Ok(json!({ "CapacityReservations": reservations }))
}

fn op_update_capacity_reservation(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some((_, reservation)) = st
        .capacity_reservations
        .iter_mut()
        .find(|(key, _)| *key == name)
    else {
        return Err(invalid_argument("Capacity Reservation does not exist"));
    };
    if let Some(dpus) = body.get("TargetDpus").filter(|d| !d.is_null()) {
        reservation.target_dpus = dpus.clone();
    }
    Ok(json!({}))
}

fn op_cancel_capacity_reservation(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some((_, reservation)) = st
        .capacity_reservations
        .iter_mut()
        .find(|(key, _)| *key == name)
    else {
        return Err(invalid_argument("Capacity Reservation does not exist"));
    };
    reservation.target_dpus = Value::from(0);
    Ok(json!({}))
}

fn op_delete_capacity_reservation(
    body: &Value,
    st: &mut AthenaState,
    account: u64,
    region: &str,
) -> OpResult {
    let name = body
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some(index) = st
        .capacity_reservations
        .iter()
        .position(|(key, _)| *key == name)
    else {
        return Err(invalid_argument("Capacity Reservation does not exist"));
    };
    st.capacity_reservations.remove(index);
    st.tagger.delete_all(&resource_arn(
        region,
        account,
        "capacity-reservation",
        &name,
    ));
    Ok(json!({}))
}

/// No existence check in moto — always answers with an empty assignment
/// list even for unknown reservation names.
fn op_get_capacity_assignment_configuration(body: &Value) -> OpResult {
    let name = body
        .get("CapacityReservationName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(json!({
        "CapacityAssignmentConfiguration": {
            "CapacityReservationName": name,
            "CapacityAssignments": [],
        }
    }))
}

fn op_put_capacity_assignment_configuration(body: &Value, st: &mut AthenaState) -> OpResult {
    let name = body
        .get("CapacityReservationName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if !st.capacity_reservations.iter().any(|(key, _)| *key == name) {
        return Err(invalid_argument("Capacity Reservation does not exist"));
    }
    Ok(json!({}))
}

// ---------------------------------------------------------------------------
// Prepared statements
// ---------------------------------------------------------------------------

fn op_create_prepared_statement(body: &Value, st: &mut AthenaState) -> OpResult {
    let statement_name = body
        .get("StatementName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.require_work_group(&workgroup)
        .map_err(|e| invalid_request(&e))?;
    let statement = PreparedStatement {
        workgroup,
        query_statement: body
            .get("QueryStatement")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        description: body.get("Description").cloned().unwrap_or(Value::Null),
        last_modified: crate::proto::now_unix(),
    };
    if let Some((_, existing)) = st
        .prepared_statements
        .iter_mut()
        .find(|(key, _)| *key == statement_name)
    {
        *existing = statement;
    } else {
        st.prepared_statements.push((statement_name, statement));
    }
    Ok(json!({}))
}

fn op_get_prepared_statement(body: &Value, st: &AthenaState) -> OpResult {
    let statement_name = body
        .get("StatementName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match st
        .prepared_statements
        .iter()
        .find(|(key, ps)| *key == statement_name && ps.workgroup == workgroup)
    {
        Some((key, ps)) => Ok(json!({
            "PreparedStatement": {
                "StatementName": key,
                "QueryStatement": ps.query_statement,
                "WorkGroupName": ps.workgroup,
                "Description": ps.description,
            }
        })),
        None => Err(athena_client_error(
            "ResourceNotFoundException",
            &format!("PreparedStatement {statement_name} was not found in {workgroup}"),
        )),
    }
}

fn op_delete_prepared_statement(body: &Value, st: &mut AthenaState) -> OpResult {
    let statement_name = body
        .get("StatementName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let index = st
        .prepared_statements
        .iter()
        .position(|(key, ps)| *key == statement_name && ps.workgroup == workgroup);
    match index {
        Some(index) => {
            st.prepared_statements.remove(index);
            Ok(json!({}))
        }
        None => Err(invalid_argument(&format!(
            "PreparedStatement {statement_name} was not found"
        ))),
    }
}

fn op_update_prepared_statement(body: &Value, st: &mut AthenaState) -> OpResult {
    let statement_name = body
        .get("StatementName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some((_, ps)) = st
        .prepared_statements
        .iter_mut()
        .find(|(key, ps)| *key == statement_name && ps.workgroup == workgroup)
    else {
        return Err(invalid_argument(&format!(
            "PreparedStatement {statement_name} was not found"
        )));
    };
    if let Some(query) = body.get("QueryStatement").and_then(Value::as_str) {
        ps.query_statement = query.to_string();
    }
    if let Some(description) = body.get("Description").and_then(Value::as_str) {
        ps.description = Value::String(description.to_string());
    }
    ps.last_modified = crate::proto::now_unix();
    Ok(json!({}))
}

fn op_list_prepared_statements(body: &Value, st: &AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let statements: Vec<Value> = st
        .prepared_statements
        .iter()
        .filter(|(_, ps)| ps.workgroup == workgroup)
        .map(|(key, ps)| {
            json!({
                "StatementName": key,
                "LastModifiedTime": ps.last_modified,
            })
        })
        .collect();
    Ok(json!({ "PreparedStatements": statements }))
}

fn op_batch_get_prepared_statement(body: &Value, st: &AthenaState) -> OpResult {
    let names = body
        .get("PreparedStatementNames")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut statements = Vec::new();
    let mut unprocessed = Vec::new();
    for name in names {
        let Some(name_str) = name.as_str() else {
            continue;
        };
        match st
            .prepared_statements
            .iter()
            .find(|(key, ps)| *key == name_str && ps.workgroup == workgroup)
        {
            Some((key, ps)) => statements.push(json!({
                "StatementName": key,
                "QueryStatement": ps.query_statement,
                "WorkGroupName": ps.workgroup,
                "Description": ps.description,
            })),
            None => unprocessed.push(json!({
                "StatementName": name_str,
                "ErrorCode": "INVALID_INPUT",
                "ErrorMessage": "INVALID_INPUT",
            })),
        }
    }
    Ok(json!({
        "PreparedStatements": statements,
        "UnprocessedPreparedStatementNames": unprocessed,
    }))
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

fn op_start_session(body: &Value, st: &mut AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.require_work_group(&workgroup)
        .map_err(|e| invalid_request(&e))?;
    let session_id = uuid::Uuid::new_v4().to_string();
    let session = Session {
        workgroup: workgroup.clone(),
        engine_configuration: body
            .get("EngineConfiguration")
            .filter(|config| !config.is_null())
            .cloned()
            .unwrap_or(json!({})),
        notebook_version: body
            .get("NotebookVersion")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        description: body
            .get("Description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        state: "IDLE".to_string(),
        start_time: crate::proto::now_unix(),
        end_time: None,
    };
    st.sessions.insert(session_id.clone(), session);
    Ok(json!({ "SessionId": session_id, "State": "IDLE" }))
}

fn op_get_session(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("SessionId").and_then(Value::as_str) else {
        return Err(invalid_request("Session  was not found"));
    };
    match st.sessions.get(id) {
        Some(session) => Ok(session.to_json(id)),
        None => Err(invalid_request(&format!("Session {id} was not found"))),
    }
}

fn op_get_session_status(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("SessionId").and_then(Value::as_str) else {
        return Err(invalid_request("Session  was not found"));
    };
    match st.sessions.get(id) {
        Some(session) => Ok(json!({
            "SessionId": id,
            "Status": session.status_json(),
        })),
        None => Err(invalid_request(&format!("Session {id} was not found"))),
    }
}

fn op_terminate_session(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("SessionId").and_then(Value::as_str) else {
        return Err(invalid_request("Session  was not found"));
    };
    match st.sessions.get_mut(id) {
        Some(session) => {
            session.state = "TERMINATED".to_string();
            session.end_time = Some(crate::proto::now_unix());
            Ok(json!({ "State": "TERMINATING" }))
        }
        None => Err(invalid_request(&format!("Session {id} was not found"))),
    }
}

fn op_list_sessions(body: &Value, st: &AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let sessions: Vec<Value> = st
        .sessions
        .iter()
        .filter(|(_, session)| session.workgroup == workgroup)
        .map(|(id, session)| {
            json!({
                "SessionId": id,
                "Description": session.description.clone(),
                "EngineVersion": "PySpark engine version 3",
                "NotebookVersion": session.notebook_version.clone(),
                "Status": session.status_json(),
            })
        })
        .collect();
    Ok(json!({
        "Sessions": sessions,
        "Description": "",
    }))
}

fn op_create_presigned_notebook_url(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("SessionId").and_then(Value::as_str) else {
        return Err(invalid_request("Session  was not found"));
    };
    if !st.sessions.contains_key(id) {
        return Err(invalid_request(&format!("Session {id} was not found")));
    }
    Ok(json!({
        "NotebookUrl": format!("https://athena-notebooks.amazonaws.com/{id}"),
        "AuthToken": format!("mock-auth-token-{id}"),
        "AuthTokenExpirationTime": crate::proto::unix_in(3600.0),
    }))
}

fn op_get_session_endpoint(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("SessionId").and_then(Value::as_str) else {
        return Err(invalid_request("Session  was not found"));
    };
    if !st.sessions.contains_key(id) {
        return Err(invalid_request(&format!("Session {id} was not found")));
    }
    Ok(json!({
        "SessionId": id,
        "Endpoint": format!("https://athena-sessions.amazonaws.com/{id}"),
    }))
}

fn op_list_executors(body: &Value) -> OpResult {
    let session_id = body
        .get("SessionId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(json!({
        "SessionId": session_id,
        "ExecutorsSummary": [],
    }))
}

// ---------------------------------------------------------------------------
// Notebooks
// ---------------------------------------------------------------------------

fn op_create_notebook(body: &Value, st: &mut AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.require_work_group(&workgroup)
        .map_err(|e| invalid_request(&e))?;
    let id = uuid::Uuid::new_v4().to_string();
    let now = crate::proto::now_unix();
    let notebook = Notebook {
        name: body
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        workgroup,
        notebook_type: Value::String("IPYNB".to_string()),
        creation_time: now,
        last_modified: now,
        content: String::new(),
    };
    st.notebooks.insert(id.clone(), notebook);
    Ok(json!({ "NotebookId": id }))
}

fn op_delete_notebook(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NotebookId").and_then(Value::as_str) else {
        return Ok(json!({}));
    };
    st.notebooks.remove(id);
    Ok(json!({}))
}

fn op_update_notebook(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NotebookId").and_then(Value::as_str) else {
        return Ok(json!({}));
    };
    if let Some(notebook) = st.notebooks.get_mut(id) {
        if let Some(payload) = body.get("Payload").and_then(Value::as_str) {
            notebook.content = payload.to_string();
        }
        if let Some(kind) = body.get("Type").and_then(Value::as_str) {
            notebook.notebook_type = Value::String(kind.to_string());
        }
        notebook.last_modified = crate::proto::now_unix();
    }
    Ok(json!({}))
}

fn op_update_notebook_metadata(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NotebookId").and_then(Value::as_str) else {
        return Ok(json!({}));
    };
    if let Some(notebook) = st.notebooks.get_mut(id) {
        if let Some(name) = body.get("Name").and_then(Value::as_str) {
            notebook.name = name.to_string();
        }
        notebook.last_modified = crate::proto::now_unix();
    }
    Ok(json!({}))
}

fn op_export_notebook(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("NotebookId").and_then(Value::as_str) else {
        return Err(invalid_request("Notebook  was not found"));
    };
    match st.notebooks.get(id) {
        Some(notebook) => Ok(json!({
            "NotebookMetadata": notebook.metadata_json(id),
            "Payload": notebook.content.clone(),
        })),
        None => Err(invalid_request(&format!("Notebook {id} was not found"))),
    }
}

fn op_import_notebook(body: &Value, st: &mut AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    st.require_work_group(&workgroup)
        .map_err(|e| invalid_request(&e))?;
    let id = uuid::Uuid::new_v4().to_string();
    let now = crate::proto::now_unix();
    let notebook = Notebook {
        name: body
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        workgroup,
        notebook_type: Value::String(
            body.get("Type")
                .and_then(Value::as_str)
                .unwrap_or("IPYNB")
                .to_string(),
        ),
        creation_time: now,
        last_modified: now,
        content: body
            .get("Payload")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    };
    st.notebooks.insert(id.clone(), notebook);
    Ok(json!({ "NotebookId": id }))
}

fn op_get_notebook_metadata(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("NotebookId").and_then(Value::as_str) else {
        return Err(invalid_request("Notebook  was not found"));
    };
    match st.notebooks.get(id) {
        Some(notebook) => Ok(json!({ "NotebookMetadata": notebook.metadata_json(id) })),
        None => Err(invalid_request(&format!("Notebook {id} was not found"))),
    }
}

fn op_list_notebook_metadata(body: &Value, st: &AthenaState) -> OpResult {
    let workgroup = body
        .get("WorkGroup")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let list: Vec<Value> = st
        .notebooks
        .iter()
        .filter(|(_, notebook)| notebook.workgroup == workgroup)
        .map(|(id, notebook)| notebook.metadata_json(id))
        .collect();
    Ok(json!({ "NotebookMetadataList": list }))
}

fn op_list_notebook_sessions() -> OpResult {
    Ok(json!({ "NotebookSessionsList": [] }))
}

// ---------------------------------------------------------------------------
// Calculation executions
// ---------------------------------------------------------------------------

fn op_start_calculation_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let session_id = body
        .get("SessionId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let calc_id = uuid::Uuid::new_v4().to_string();
    let calculation = Calculation {
        session_id,
        description: body
            .get("Description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        code_block: body
            .get("CodeBlock")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        state: "RUNNING".to_string(),
        submission: crate::proto::now_unix(),
        completion: None,
    };
    st.calculation_executions
        .insert(calc_id.clone(), calculation);
    Ok(json!({
        "CalculationExecutionId": calc_id,
        "State": "RUNNING",
    }))
}

fn op_stop_calculation_execution(body: &Value, st: &mut AthenaState) -> OpResult {
    let Some(id) = body.get("CalculationExecutionId").and_then(Value::as_str) else {
        return Err(invalid_request("CalculationExecution  was not found"));
    };
    match st.calculation_executions.get_mut(id) {
        Some(calc) => {
            calc.state = "CANCELED".to_string();
            calc.completion = Some(crate::proto::now_unix());
            Ok(json!({ "State": "CANCELED" }))
        }
        None => Err(invalid_request(&format!(
            "CalculationExecution {id} was not found"
        ))),
    }
}

fn op_get_calculation_execution(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("CalculationExecutionId").and_then(Value::as_str) else {
        return Err(invalid_request("CalculationExecution  was not found"));
    };
    match st.calculation_executions.get(id) {
        Some(calc) => Ok(calculation_json(calc, id)),
        None => Err(invalid_request(&format!(
            "CalculationExecution {id} was not found"
        ))),
    }
}

fn calculation_json(calc: &Calculation, id: &str) -> Value {
    let mut json_val = json!({
        "CalculationExecutionId": id,
        "SessionId": calc.session_id.clone(),
        "Description": calc.description.clone(),
        "CodeBlock": calc.code_block.clone(),
        "Status": {
            "State": calc.state,
            "SubmissionDateTime": calc.submission,
        },
    });
    if let Some(end) = calc.completion {
        json_val["Status"]["CompletionDateTime"] = json!(end);
    }
    json_val
}

fn op_get_calculation_execution_code(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("CalculationExecutionId").and_then(Value::as_str) else {
        return Err(invalid_request("CalculationExecution  was not found"));
    };
    match st.calculation_executions.get(id) {
        Some(calc) => Ok(json!({ "CodeBlock": calc.code_block.clone() })),
        None => Err(invalid_request(&format!(
            "CalculationExecution {id} was not found"
        ))),
    }
}

fn op_get_calculation_execution_status(body: &Value, st: &AthenaState) -> OpResult {
    let Some(id) = body.get("CalculationExecutionId").and_then(Value::as_str) else {
        return Err(invalid_request("CalculationExecution  was not found"));
    };
    match st.calculation_executions.get(id) {
        Some(calc) => Ok(json!({
            "Status": {
                "State": calc.state,
                "SubmissionDateTime": calc.submission,
                "CompletionDateTime": calc.completion,
            },
            "Statistics": {
                "DpuExecutionInMillis": 1000,
                "Progress": "100%",
            },
        })),
        None => Err(invalid_request(&format!(
            "CalculationExecution {id} was not found"
        ))),
    }
}

fn op_list_calculation_executions(body: &Value, st: &AthenaState) -> OpResult {
    let session_id = body
        .get("SessionId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let calculations: Vec<Value> = st
        .calculation_executions
        .iter()
        .filter(|(_, calc)| calc.session_id == session_id)
        .map(|(id, calc)| {
            json!({
                "CalculationExecutionId": id,
                "Description": calc.description.clone(),
                "Status": {
                    "State": calc.state,
                    "SubmissionDateTime": calc.submission,
                    "CompletionDateTime": calc.completion,
                },
            })
        })
        .collect();
    Ok(json!({ "Calculations": calculations }))
}

// ---------------------------------------------------------------------------
// Databases + table metadata
// ---------------------------------------------------------------------------

fn op_get_database(body: &Value, st: &AthenaState) -> OpResult {
    let catalog = body
        .get("CatalogName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let database_name = body
        .get("DatabaseName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match st
        .databases
        .get(catalog)
        .and_then(|dbs| dbs.get(database_name))
    {
        Some(db) => Ok(json!({
            "Database": {
                "Name": database_name,
                "Description": db.description,
                "Parameters": db.parameters,
            }
        })),
        None => Err(invalid_request(&format!(
            "Database {database_name} not found in catalog {catalog}"
        ))),
    }
}

fn op_list_databases(body: &Value, st: &AthenaState) -> OpResult {
    let catalog = body
        .get("CatalogName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let list: Vec<Value> = st
        .databases
        .get(catalog)
        .into_iter()
        .flatten()
        .map(|(name, db)| {
            json!({
                "Name": name,
                "Description": db.description,
                "Parameters": db.parameters,
            })
        })
        .collect();
    Ok(json!({ "DatabaseList": list }))
}

fn op_get_table_metadata(body: &Value, st: &AthenaState) -> OpResult {
    let catalog = body
        .get("CatalogName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let database = body
        .get("DatabaseName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let table = body
        .get("TableName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let table_key = format!("{catalog}.{database}");
    match st
        .tables
        .get(&table_key)
        .and_then(|tables| tables.get(table))
    {
        Some(meta) => Ok(json!({ "TableMetadata": meta.to_json(table) })),
        None => Err(invalid_request(&format!(
            "Table {table} not found in database {database}"
        ))),
    }
}

fn op_list_table_metadata(body: &Value, st: &AthenaState) -> OpResult {
    let catalog = body
        .get("CatalogName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let database = body
        .get("DatabaseName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let table_key = format!("{catalog}.{database}");
    let list: Vec<Value> = st
        .tables
        .get(&table_key)
        .into_iter()
        .flatten()
        .map(|(name, meta)| meta.to_json(name))
        .collect();
    Ok(json!({ "TableMetadataList": list }))
}

// ---------------------------------------------------------------------------
// Static lists + dashboard
// ---------------------------------------------------------------------------

fn op_list_engine_versions() -> OpResult {
    Ok(json!({
        "EngineVersions": [
            {
                "SelectedEngineVersion": "AUTO",
                "EffectiveEngineVersion": "Athena engine version 3",
            },
            {
                "SelectedEngineVersion": "Athena engine version 2",
                "EffectiveEngineVersion": "Athena engine version 2",
            },
            {
                "SelectedEngineVersion": "Athena engine version 3",
                "EffectiveEngineVersion": "Athena engine version 3",
            },
            {
                "SelectedEngineVersion": "PySpark engine version 3",
                "EffectiveEngineVersion": "PySpark engine version 3",
            },
        ]
    }))
}

fn op_list_application_dpu_sizes() -> OpResult {
    Ok(json!({
        "ApplicationDPUSizes": [
            {
                "ApplicationDPUSizeType": "NOTEBOOK",
                "SupportedDPUSizes": [4, 8, 16, 32, 48],
            },
            {
                "ApplicationDPUSizeType": "SPARK",
                "SupportedDPUSizes": [4, 8, 16, 32, 48, 64],
            },
        ]
    }))
}

fn op_get_resource_dashboard(body: &Value, st: &AthenaState) -> OpResult {
    let arn = body
        .get("ResourceARN")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(json!({
        "ResourceDashboard": {
            "ResourceArn": arn,
            "QueryCount": st.executions.len(),
            "NamedQueryCount": st.named_queries.len(),
        }
    }))
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let result: OpResult = with_state(req.account, &req.region, |st| {
        match req.operation.as_str() {
            // query execution lifecycle
            "StartQueryExecution" => op_start_query_execution(&body, st),
            "GetQueryExecution" => op_get_query_execution(&body, st),
            "ListQueryExecutions" => op_list_query_executions(&body, st),
            "StopQueryExecution" => op_stop_query_execution(&body, st),
            "BatchGetQueryExecution" => op_batch_get_query_execution(&body, st),
            "GetQueryResults" => op_get_query_results(&body, st),
            "GetQueryRuntimeStatistics" => op_get_query_runtime_statistics(&body, st),
            // work groups + tags
            "CreateWorkGroup" => op_create_work_group(&body, st, req.account, &req.region),
            "ListWorkGroups" => op_list_work_groups(st),
            "GetWorkGroup" => op_get_work_group(&body, st),
            "UpdateWorkGroup" => op_update_work_group(&body, st),
            "DeleteWorkGroup" => op_delete_work_group(&body, st),
            "ListTagsForResource" => op_list_tags_for_resource(&body, st),
            "TagResource" => op_tag_resource(&body, st),
            "UntagResource" => op_untag_resource(&body, st),
            // named queries
            "CreateNamedQuery" => op_create_named_query(&body, st),
            "GetNamedQuery" => op_get_named_query(&body, st),
            "DeleteNamedQuery" => op_delete_named_query(&body, st),
            "UpdateNamedQuery" => op_update_named_query(&body, st),
            "BatchGetNamedQuery" => op_batch_get_named_query(&body, st),
            "ListNamedQueries" => op_list_named_queries(&body, st),
            // data catalogs
            "CreateDataCatalog" => op_create_data_catalog(&body, st, req.account, &req.region),
            "GetDataCatalog" => op_get_data_catalog(&body, st),
            "ListDataCatalogs" => op_list_data_catalogs(st),
            "DeleteDataCatalog" => op_delete_data_catalog(&body, st, req.account, &req.region),
            "UpdateDataCatalog" => op_update_data_catalog(&body, st),
            // capacity reservations
            "CreateCapacityReservation" => {
                op_create_capacity_reservation(&body, st, req.account, &req.region)
            }
            "GetCapacityReservation" => op_get_capacity_reservation(&body, st),
            "ListCapacityReservations" => op_list_capacity_reservations(st),
            "UpdateCapacityReservation" => op_update_capacity_reservation(&body, st),
            "CancelCapacityReservation" => op_cancel_capacity_reservation(&body, st),
            "DeleteCapacityReservation" => {
                op_delete_capacity_reservation(&body, st, req.account, &req.region)
            }
            "GetCapacityAssignmentConfiguration" => op_get_capacity_assignment_configuration(&body),
            "PutCapacityAssignmentConfiguration" => {
                op_put_capacity_assignment_configuration(&body, st)
            }
            // prepared statements
            "CreatePreparedStatement" => op_create_prepared_statement(&body, st),
            "GetPreparedStatement" => op_get_prepared_statement(&body, st),
            "DeletePreparedStatement" => op_delete_prepared_statement(&body, st),
            "UpdatePreparedStatement" => op_update_prepared_statement(&body, st),
            "ListPreparedStatements" => op_list_prepared_statements(&body, st),
            "BatchGetPreparedStatement" => op_batch_get_prepared_statement(&body, st),
            // sessions
            "StartSession" => op_start_session(&body, st),
            "GetSession" => op_get_session(&body, st),
            "GetSessionStatus" => op_get_session_status(&body, st),
            "TerminateSession" => op_terminate_session(&body, st),
            "ListSessions" => op_list_sessions(&body, st),
            "CreatePresignedNotebookUrl" => op_create_presigned_notebook_url(&body, st),
            "GetSessionEndpoint" => op_get_session_endpoint(&body, st),
            "ListExecutors" => op_list_executors(&body),
            // notebooks
            "CreateNotebook" => op_create_notebook(&body, st),
            "DeleteNotebook" => op_delete_notebook(&body, st),
            "UpdateNotebook" => op_update_notebook(&body, st),
            "UpdateNotebookMetadata" => op_update_notebook_metadata(&body, st),
            "ExportNotebook" => op_export_notebook(&body, st),
            "ImportNotebook" => op_import_notebook(&body, st),
            "GetNotebookMetadata" => op_get_notebook_metadata(&body, st),
            "ListNotebookMetadata" => op_list_notebook_metadata(&body, st),
            "ListNotebookSessions" => op_list_notebook_sessions(),
            // calculation executions
            "StartCalculationExecution" => op_start_calculation_execution(&body, st),
            "StopCalculationExecution" => op_stop_calculation_execution(&body, st),
            "GetCalculationExecution" => op_get_calculation_execution(&body, st),
            "GetCalculationExecutionCode" => op_get_calculation_execution_code(&body, st),
            "GetCalculationExecutionStatus" => op_get_calculation_execution_status(&body, st),
            "ListCalculationExecutions" => op_list_calculation_executions(&body, st),
            // databases + tables
            "GetDatabase" => op_get_database(&body, st),
            "ListDatabases" => op_list_databases(&body, st),
            "GetTableMetadata" => op_get_table_metadata(&body, st),
            "ListTableMetadata" => op_list_table_metadata(&body, st),
            // static lists + dashboard
            "ListEngineVersions" => op_list_engine_versions(),
            "ListApplicationDPUSizes" => op_list_application_dpu_sizes(),
            "GetResourceDashboard" => op_get_resource_dashboard(&body, st),
            other => Err(json_error(
                501,
                "NotImplemented",
                &format!("The {other} action has not been implemented"),
            )),
        }
    });
    match result {
        Ok(value) => Response::json_status(200, &value),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(account: u64, op: &str, body: Value) -> Response {
        let req = Request {
            service: "athena".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            account,
            region: "us-east-1".to_string(),
        };
        dispatch(&req)
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Per-test isolation: a fresh (account, region) scope every call.
    fn fresh_account() -> u64 {
        let digits: String = uuid::Uuid::new_v4()
            .to_string()
            .chars()
            .filter(|c| c.is_ascii_digit())
            .take(12)
            .collect();
        format!("{digits:0<12}").parse().unwrap_or(123456789012)
    }

    fn start(account: u64, extra: Value) -> String {
        let mut body = json!({
            "QueryString": "SELECT 1",
            "WorkGroup": "primary",
            "ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"},
        });
        if let (Some(map), Some(extra_map)) = (body.as_object_mut(), extra.as_object()) {
            for (k, v) in extra_map {
                map.insert(k.clone(), v.clone());
            }
        }
        let resp = run(account, "StartQueryExecution", body);
        assert_eq!(resp.status, 200, "{:?}", resp.body);
        body_of(&resp)["QueryExecutionId"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    // ------------------------------------------------------------------
    // Family 1: query execution lifecycle
    // ------------------------------------------------------------------

    #[test]
    fn execution_start_describe_list_stop_batch_results() {
        let account = fresh_account();
        let id = start(account, json!({}));
        let resp = run(
            account,
            "GetQueryExecution",
            json!({"QueryExecutionId": &id}),
        );
        let qe = body_of(&resp)["QueryExecution"].clone();
        assert_eq!(qe["QueryExecutionId"], id.as_str());
        assert_eq!(qe["Query"], "SELECT 1");
        assert_eq!(qe["Status"]["State"], "SUCCEEDED");
        assert!(qe["Status"]["SubmissionDateTime"].is_number());
        // OutputLocation gains the /{id}.csv suffix
        assert_eq!(
            qe["ResultConfiguration"]["OutputLocation"],
            format!("s3://test-bucket/results/{id}.csv")
        );
        assert_eq!(qe["WorkGroup"], "primary");
        assert_eq!(qe["Statistics"]["DataScannedInBytes"], 0);
        assert_eq!(qe["StatementType"], "DML");

        let resp = run(account, "ListQueryExecutions", json!({}));
        let ids = body_of(&resp)["QueryExecutionIds"]
            .as_array()
            .unwrap()
            .clone();
        assert!(ids.iter().any(|v| v.as_str() == Some(id.as_str())));

        run(
            account,
            "StopQueryExecution",
            json!({"QueryExecutionId": &id}),
        );
        let resp = run(
            account,
            "GetQueryExecution",
            json!({"QueryExecutionId": &id}),
        );
        assert_eq!(
            body_of(&resp)["QueryExecution"]["Status"]["State"],
            "CANCELLED"
        );

        let resp = run(
            account,
            "BatchGetQueryExecution",
            json!({"QueryExecutionIds": [&id, "missing-id"]}),
        );
        let batch = body_of(&resp);
        assert_eq!(batch["QueryExecutions"][0]["QueryExecutionId"], id.as_str());
        assert_eq!(
            batch["UnprocessedQueryExecutionIds"][0]["ErrorCode"],
            "INVALID_INPUT"
        );

        // stopped executions refuse get_query_results (moto QueryStillRunning)
        let resp = run(account, "GetQueryResults", json!({"QueryExecutionId": &id}));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "InvalidRequestException");

        // unknown ids answer the empty result set like the sidecar
        let resp = run(
            account,
            "GetQueryResults",
            json!({"QueryExecutionId": "fake-id"}),
        );
        assert_eq!(resp.status, 200);
        assert_eq!(
            body_of(&resp)["ResultSet"]["ResultSetMetadata"]["ColumnInfo"],
            json!([])
        );
    }

    #[test]
    fn execution_list_filtered_by_workgroup() {
        let account = fresh_account();
        run(account, "QueryExecutionLifecycle", json!({})); // no-op warmup
        let _ = start(account, json!({"WorkGroup": "primary"}));
        let resp = run(
            account,
            "ListQueryExecutions",
            json!({"WorkGroup": "primary"}),
        );
        assert!(
            body_of(&resp)["QueryExecutionIds"]
                .as_array()
                .unwrap()
                .len()
                == 1
        );
        let resp = run(
            account,
            "ListQueryExecutions",
            json!({"WorkGroup": "other"}),
        );
        assert!(body_of(&resp)["QueryExecutionIds"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn execution_bad_workgroup_and_unknown_op() {
        let account = fresh_account();
        let resp = run(
            account,
            "StartQueryExecution",
            json!({"QueryString": "SELECT 1", "WorkGroup": "missing"}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "InvalidRequestException");

        let resp = run(account, "SomeUnknownAction", json!({}));
        assert_eq!(resp.status, 501);
        assert_eq!(body_of(&resp)["__type"], "NotImplemented");
    }

    #[test]
    fn execution_ddl_and_execution_parameters() {
        let account = fresh_account();
        let id = start(
            account,
            json!({
                "QueryString": "CREATE TABLE t (a int)",
                "ExecutionParameters": ["p"],
            }),
        );
        let resp = run(
            account,
            "GetQueryExecution",
            json!({"QueryExecutionId": &id}),
        );
        let qe = body_of(&resp)["QueryExecution"].clone();
        assert_eq!(qe["StatementType"], "DDL");
        assert_eq!(qe["ExecutionParameters"], json!(["p"]));

        // runtime statistics echo the execution state
        let resp = run(
            account,
            "GetQueryRuntimeStatistics",
            json!({"QueryExecutionId": &id}),
        );
        assert_eq!(
            body_of(&resp)["QueryRuntimeStatistics"]["OutputStage"]["State"],
            "SUCCEEDED"
        );
        let resp = run(
            account,
            "GetQueryRuntimeStatistics",
            json!({"QueryExecutionId": "nope"}),
        );
        assert_eq!(resp.status, 400);
    }

    // ------------------------------------------------------------------
    // Family 2: work groups + resource tags
    // ------------------------------------------------------------------

    fn wg_arn(account: u64, name: &str) -> String {
        format!("arn:aws:athena:us-east-1:{account}:workgroup/{name}")
    }

    #[test]
    fn work_group_crud_cycle() {
        let account = fresh_account();
        let resp = run(
            account,
            "CreateWorkGroup",
            json!({"Name": "wg-a", "Configuration": {"ResultConfiguration": {}}}),
        );
        assert_eq!(resp.status, 200);
        let resp = run(account, "GetWorkGroup", json!({"WorkGroup": "wg-a"}));
        let wg = body_of(&resp)["WorkGroup"].clone();
        assert_eq!(wg["Name"], "wg-a");
        assert_eq!(wg["State"], "ENABLED");
        assert_eq!(
            wg["Configuration"]["EnforceWorkGroupConfiguration"],
            Value::Bool(true)
        );
        assert_eq!(
            wg["Configuration"]["EngineVersion"]["SelectedEngineVersion"],
            "AUTO"
        );

        // duplicate creation errors like the sidecar
        let resp = run(account, "CreateWorkGroup", json!({"Name": "wg-a"}));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "InvalidRequestException");

        // create with explicit configuration values + description
        run(
            account,
            "CreateWorkGroup",
            json!({
                "Name": "wg-b",
                "Description": "mine",
                "Configuration": {"EnforceWorkGroupConfiguration": true},
            }),
        );
        let wg = body_of(&run(account, "GetWorkGroup", json!({"WorkGroup": "wg-b"})))["WorkGroup"]
            .clone();
        assert_eq!(wg["Description"], "mine");
        // seeded primary exists too
        let resp = run(account, "ListWorkGroups", json!({}));
        let list = body_of(&resp)["WorkGroups"].as_array().unwrap().clone();
        let names: Vec<&str> = list
            .iter()
            .map(|g| g["Name"].as_str().unwrap_or_default())
            .collect();
        assert!(names.contains(&"primary"));
        assert!(names.contains(&"wg-a"));

        // update: state + description
        let resp = run(
            account,
            "UpdateWorkGroup",
            json!({"WorkGroup": "wg-a", "State": "DISABLED"}),
        );
        assert_eq!(resp.status, 200);
        let wg = body_of(&run(account, "GetWorkGroup", json!({"WorkGroup": "wg-a"})))["WorkGroup"]
            .clone();
        assert_eq!(wg["State"], "DISABLED");
        run(
            account,
            "UpdateWorkGroup",
            json!({"WorkGroup": "wg-a", "Description": "updated"}),
        );
        let wg = body_of(&run(account, "GetWorkGroup", json!({"WorkGroup": "wg-a"})))["WorkGroup"]
            .clone();
        assert_eq!(wg["Description"], "updated");
        // update on missing group
        let resp = run(account, "UpdateWorkGroup", json!({"WorkGroup": "zzz"}));
        assert_eq!(resp.status, 400);

        run(account, "DeleteWorkGroup", json!({"WorkGroup": "wg-a"}));
        let resp = run(account, "GetWorkGroup", json!({"WorkGroup": "wg-a"}));
        assert_eq!(body_of(&resp)["WorkGroup"], Value::Null);
    }

    #[test]
    fn work_group_tags_resource_arns() {
        let account = fresh_account();
        run(
            account,
            "CreateWorkGroup",
            json!({
                "Name": "wg-tagged",
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            }),
        );
        let arn = wg_arn(account, "wg-tagged");
        let resp = run(account, "ListTagsForResource", json!({"ResourceARN": &arn}));
        let tags = body_of(&resp)["Tags"].clone();
        let tags = tags.as_array().unwrap();
        assert!(tags
            .iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "test"));
        assert!(tags
            .iter()
            .any(|t| t["Key"] == "team" && t["Value"] == "platform"));

        // tag_resource accumulates, untag_resource removes
        run(
            account,
            "TagResource",
            json!({
                "ResourceARN": &arn,
                "Tags": [{"Key": "stage", "Value": "x"}],
            }),
        );
        let resp = run(account, "ListTagsForResource", json!({"ResourceARN": &arn}));
        assert_eq!(body_of(&resp)["Tags"].as_array().unwrap().len(), 3);
        run(
            account,
            "UntagResource",
            json!({"ResourceARN": &arn, "TagKeys": ["stage"]}),
        );
        let resp = run(account, "ListTagsForResource", json!({"ResourceARN": &arn}));
        let list = body_of(&resp)["Tags"].as_array().unwrap().clone();
        let keys: Vec<&str> = list
            .iter()
            .map(|t| t["Key"].as_str().unwrap_or_default())
            .collect();
        assert!(!keys.contains(&"stage"));

        // unknown ARNs error; the seeded primary answers an empty list
        let resp = run(
            account,
            "ListTagsForResource",
            json!({"ResourceARN": wg_arn(account, "nope")}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            format!(
                "Athena Resource, {} Does Not Exist",
                wg_arn(account, "nope")
            )
        );
        let resp = run(
            account,
            "ListTagsForResource",
            json!({"ResourceARN": wg_arn(account, "primary")}),
        );
        assert_eq!(resp.status, 200);
        assert!(body_of(&resp)["Tags"].as_array().unwrap().is_empty());
    }

    // ------------------------------------------------------------------
    // Family 3: named queries
    // ------------------------------------------------------------------

    #[test]
    fn named_query_crud_and_batch() {
        let account = fresh_account();
        let resp = run(
            account,
            "CreateNamedQuery",
            json!({
                "Name": "nq-1",
                "Database": "default",
                "QueryString": "SELECT 1",
                "Description": "first",
            }),
        );
        let id = body_of(&resp)["NamedQueryId"].as_str().unwrap().to_string();

        let resp = run(account, "GetNamedQuery", json!({"NamedQueryId": &id}));
        let nq = body_of(&resp)["NamedQuery"].clone();
        assert_eq!(nq["Name"], "nq-1");
        assert_eq!(nq["Database"], "default");
        assert_eq!(nq["QueryString"], "SELECT 1");
        assert_eq!(nq["WorkGroup"], "primary");

        // update
        let resp = run(
            account,
            "UpdateNamedQuery",
            json!({
                "NamedQueryId": &id,
                "QueryString": "SELECT 2",
                "Description": "second",
            }),
        );
        assert_eq!(resp.status, 200);
        let nq = body_of(&run(account, "GetNamedQuery", json!({"NamedQueryId": &id})))
            ["NamedQuery"]
            .clone();
        assert_eq!(nq["QueryString"], "SELECT 2");
        assert_eq!(nq["Description"], "second");

        // batch get: found + unprocessed
        let resp = run(
            account,
            "BatchGetNamedQuery",
            json!({"NamedQueryIds": [&id, "missing"]}),
        );
        let batch = body_of(&resp);
        assert_eq!(batch["NamedQueries"][0]["Name"], "nq-1");
        assert_eq!(
            batch["UnprocessedNamedQueryIds"][0]["NamedQueryId"],
            "missing"
        );

        // missing get is the moto 'NoneType' crash — moto's server renders
        // unhandled exceptions as the exact werkzeug HTML 500 page.
        let resp = run(
            account,
            "GetNamedQuery",
            json!({"NamedQueryId": "00000000-0000-0000-0000-000000000000"}),
        );
        assert_eq!(resp.status, 500);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "Content-Type" && v == "text/html; charset=utf-8"),
            "{:?}",
            resp.headers
        );
        let html = String::from_utf8(resp.body.clone()).unwrap();
        assert!(html.starts_with("<!doctype html>"));
        assert!(
            html.contains("<title>500 Internal Server Error</title>"),
            "{html}"
        );

        // create against a missing workgroup errors
        let resp = run(
            account,
            "CreateNamedQuery",
            json!({
                "Name": "x",
                "Database": "default",
                "QueryString": "SELECT 1",
                "WorkGroup": "missing",
            }),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["message"], "WorkGroup does not exist");

        // list is workgroup-scoped and paginates
        let resp = run(
            account,
            "ListNamedQueries",
            json!({"WorkGroup": "primary", "MaxResults": 1}),
        );
        assert!(body_of(&resp)["NamedQueryIds"].as_array().unwrap().len() <= 1);

        run(account, "DeleteNamedQuery", json!({"NamedQueryId": &id}));
        let resp = run(account, "GetNamedQuery", json!({"NamedQueryId": &id}));
        assert_eq!(resp.status, 500);
    }

    #[test]
    fn named_query_list_pagination_and_filter() {
        let account = fresh_account();
        let mut ids = Vec::new();
        for i in 0..3 {
            let resp = run(
                account,
                "CreateNamedQuery",
                json!({
                    "Name": format!("nq-{i}"),
                    "Database": "default",
                    "QueryString": "SELECT 1",
                }),
            );
            ids.push(body_of(&resp)["NamedQueryId"].as_str().unwrap().to_string());
        }
        // page 1
        let resp = run(
            account,
            "ListNamedQueries",
            json!({"WorkGroup": "primary", "MaxResults": 2}),
        );
        let page1 = body_of(&resp);
        let page_ids: Vec<String> = page1["NamedQueryIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(page1["NextToken"].as_str().is_some());
        run(account, "ListNamedQueries", json!({"WorkGroup": "other"}));
        // follow the token to fetch the rest
        let resp = run(
            account,
            "ListNamedQueries",
            json!({"WorkGroup": "primary", "MaxResults": 2, "NextToken": page1["NextToken"]}),
        );
        let page2 = body_of(&resp);
        let page2_ids: Vec<String> = page2["NamedQueryIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let mut all = page_ids.clone();
        all.extend(page2_ids.iter().cloned());
        assert!(ids.iter().all(|id| all.contains(id)));
    }

    // ------------------------------------------------------------------
    // Family 4: data catalogs + capacity reservations
    // ------------------------------------------------------------------

    #[test]
    fn data_catalog_crud() {
        let account = fresh_account();
        let resp = run(
            account,
            "CreateDataCatalog",
            json!({
                "Name": "dc-1",
                "Type": "LAMBDA",
                "Description": "cat",
                "Parameters": {"metadata-function": "arn:x"},
            }),
        );
        assert_eq!(resp.status, 200);

        let catalog = body_of(&run(account, "GetDataCatalog", json!({"Name": "dc-1"})))
            ["DataCatalog"]
            .clone();
        assert_eq!(catalog["Name"], "dc-1");
        assert_eq!(catalog["Type"], "LAMBDA");
        assert_eq!(catalog["Description"], "cat");
        assert_eq!(catalog["Parameters"]["metadata-function"], "arn:x");

        let resp = run(account, "ListDataCatalogs", json!({}));
        let list = body_of(&resp)["DataCatalogsSummary"]
            .as_array()
            .unwrap()
            .clone();
        let names: Vec<&str> = list
            .iter()
            .map(|c| c["CatalogName"].as_str().unwrap_or_default())
            .collect();
        assert!(names.contains(&"dc-1"));

        run(
            account,
            "UpdateDataCatalog",
            json!({"Name": "dc-1", "Type": "LAMBDA", "Description": "updated"}),
        );
        let catalog = body_of(&run(account, "GetDataCatalog", json!({"Name": "dc-1"})))
            ["DataCatalog"]
            .clone();
        assert_eq!(catalog["Description"], "updated");

        run(account, "DeleteDataCatalog", json!({"Name": "dc-1"}));
        let resp = run(account, "ListDataCatalogs", json!({}));
        let list = body_of(&resp)["DataCatalogsSummary"]
            .as_array()
            .unwrap()
            .clone();
        let names: Vec<&str> = list
            .iter()
            .map(|c| c["CatalogName"].as_str().unwrap_or_default())
            .collect();
        assert!(!names.contains(&"dc-1"));

        let resp = run(account, "DeleteDataCatalog", json!({"Name": "gone"}));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["message"], "DataCatalog gone is not found");
        let resp = run(account, "UpdateDataCatalog", json!({"Name": "gone"}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn capacity_reservation_lifecycle_and_errors() {
        let account = fresh_account();
        // no Tags succeeds — moto normalizes `tags or []` and skips tagging
        let resp = run(
            account,
            "CreateCapacityReservation",
            json!({"Name": "cr-no-tags", "TargetDpus": 24}),
        );
        assert_eq!(resp.status, 200);
        let cr = body_of(&run(
            account,
            "GetCapacityReservation",
            json!({"Name": "cr-no-tags"}),
        ))["CapacityReservation"]
            .clone();
        assert_eq!(cr["TargetDpus"], 24);
        assert_eq!(cr["Tags"], json!([]));

        // with tags the reservation is created
        run(
            account,
            "CreateCapacityReservation",
            json!({
                "Name": "cr-1",
                "TargetDpus": 8,
                "Tags": [{"Key": "env", "Value": "prod"}],
            }),
        );
        let cr = body_of(&run(
            account,
            "GetCapacityReservation",
            json!({"Name": "cr-1"}),
        ))["CapacityReservation"]
            .clone();
        assert_eq!(cr["TargetDpus"], 8);
        assert_eq!(cr["Tags"][0]["Key"], "env");

        let resp = run(account, "ListCapacityReservations", json!({}));
        let list = body_of(&resp)["CapacityReservations"]
            .as_array()
            .unwrap()
            .clone();
        let names: Vec<&str> = list
            .iter()
            .map(|c| c["Name"].as_str().unwrap_or_default())
            .collect();
        assert!(names.contains(&"cr-1"));

        run(
            account,
            "UpdateCapacityReservation",
            json!({"Name": "cr-1", "TargetDpus": 16}),
        );
        let cr = body_of(&run(
            account,
            "GetCapacityReservation",
            json!({"Name": "cr-1"}),
        ))["CapacityReservation"]
            .clone();
        assert_eq!(cr["TargetDpus"], 16);
        run(
            account,
            "CancelCapacityReservation",
            json!({"Name": "cr-1"}),
        );
        let cr = body_of(&run(
            account,
            "GetCapacityReservation",
            json!({"Name": "cr-1"}),
        ))["CapacityReservation"]
            .clone();
        assert_eq!(cr["TargetDpus"], 0);

        run(
            account,
            "DeleteCapacityReservation",
            json!({"Name": "cr-1"}),
        );
        let resp = run(account, "GetCapacityReservation", json!({"Name": "cr-1"}));
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "Capacity reservation does not exist"
        );

        let resp = run(
            account,
            "CancelCapacityReservation",
            json!({"Name": "nope"}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "Capacity Reservation does not exist"
        );
        let resp = run(
            account,
            "PutCapacityAssignmentConfiguration",
            json!({
                "CapacityReservationName": "nope",
                "CapacityAssignments": [{"WorkGroupNames": ["primary"]}],
            }),
        );
        assert_eq!(resp.status, 400);
        // get always answers (moto does no existence check)
        let resp = run(
            account,
            "GetCapacityAssignmentConfiguration",
            json!({"CapacityReservationName": "not-there"}),
        );
        assert_eq!(resp.status, 200);
        assert!(!body_of(&resp)["CapacityAssignmentConfiguration"].is_null());
    }

    // ------------------------------------------------------------------
    // Family 5: prepared statements, sessions, notebooks, calculations,
    // databases/tables, static lists, dashboard
    // ------------------------------------------------------------------

    #[test]
    fn prepared_statement_crud() {
        let account = fresh_account();
        let resp = run(
            account,
            "CreatePreparedStatement",
            json!({
                "StatementName": "ps-1",
                "WorkGroup": "primary",
                "QueryStatement": "SELECT ? FROM t",
                "Description": "d",
            }),
        );
        assert_eq!(resp.status, 200);

        let ps = body_of(&run(
            account,
            "GetPreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary"}),
        ))["PreparedStatement"]
            .clone();
        assert_eq!(ps["StatementName"], "ps-1");
        assert_eq!(ps["WorkGroupName"], "primary");
        assert_eq!(ps["QueryStatement"], "SELECT ? FROM t");

        // bad workgroup creation
        let resp = run(
            account,
            "CreatePreparedStatement",
            json!({"StatementName": "x", "WorkGroup": "missing", "QueryStatement": "1"}),
        );
        assert_eq!(resp.status, 400);

        // update then read back
        run(
            account,
            "UpdatePreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary", "QueryStatement": "SELECT 1"}),
        );
        let ps = body_of(&run(
            account,
            "GetPreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary"}),
        ))["PreparedStatement"]
            .clone();
        assert_eq!(ps["QueryStatement"], "SELECT 1");

        let resp = run(
            account,
            "ListPreparedStatements",
            json!({"WorkGroup": "primary"}),
        );
        let list = body_of(&resp)["PreparedStatements"]
            .as_array()
            .unwrap()
            .clone();
        let names: Vec<&str> = list
            .iter()
            .map(|p| p["StatementName"].as_str().unwrap_or_default())
            .collect();
        assert!(names.contains(&"ps-1"));
        let resp = run(
            account,
            "BatchGetPreparedStatement",
            json!({"PreparedStatementNames": ["ps-1", "missing"], "WorkGroup": "primary"}),
        );
        let batch = body_of(&resp);
        assert_eq!(batch["PreparedStatements"][0]["WorkGroupName"], "primary");
        assert_eq!(
            batch["UnprocessedPreparedStatementNames"][0]["StatementName"],
            "missing"
        );

        run(
            account,
            "DeletePreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary"}),
        );
        // delete of a missing statement → InvalidArgumentException
        let resp = run(
            account,
            "DeletePreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary"}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "PreparedStatement ps-1 was not found"
        );
        // get of missing → moto AthenaClientError envelope
        let resp = run(
            account,
            "GetPreparedStatement",
            json!({"StatementName": "ps-1", "WorkGroup": "primary"}),
        );
        assert_eq!(resp.status, 400);
        let err = body_of(&resp);
        assert_eq!(err["Error"]["Code"], "ResourceNotFoundException");
        assert_eq!(
            err["Error"]["Message"],
            "PreparedStatement ps-1 was not found in primary"
        );
    }

    #[test]
    fn session_lifecycle_and_extras() {
        let account = fresh_account();
        let resp = run(
            account,
            "StartSession",
            json!({
                "WorkGroup": "primary",
                "EngineConfiguration": {"CoordinatorDpuSize": 1},
            }),
        );
        assert_eq!(body_of(&resp)["State"], "IDLE");
        let session_id = body_of(&resp)["SessionId"].as_str().unwrap().to_string();

        let session = body_of(&run(
            account,
            "GetSession",
            json!({"SessionId": &session_id}),
        ));
        assert_eq!(session["Status"]["State"], "IDLE");
        let status = body_of(&run(
            account,
            "GetSessionStatus",
            json!({"SessionId": &session_id}),
        ));
        assert_eq!(status["Status"]["State"], "IDLE");

        let resp = run(
            account,
            "TerminateSession",
            json!({"SessionId": &session_id}),
        );
        assert_eq!(body_of(&resp)["State"], "TERMINATING");

        let resp = run(account, "ListSessions", json!({"WorkGroup": "primary"}));
        assert!(body_of(&resp)["Sessions"].as_array().is_some());

        let signed = body_of(&run(
            account,
            "CreatePresignedNotebookUrl",
            json!({"SessionId": &session_id}),
        ));
        assert!(signed["NotebookUrl"]
            .as_str()
            .unwrap()
            .contains(&session_id));
        assert!(signed["AuthToken"]
            .as_str()
            .unwrap()
            .starts_with("mock-auth-token-"));

        let endpoint = body_of(&run(
            account,
            "GetSessionEndpoint",
            json!({"SessionId": &session_id}),
        ));
        assert_eq!(endpoint["SessionId"], session_id.as_str());

        let executors = body_of(&run(
            account,
            "ListExecutors",
            json!({"SessionId": &session_id}),
        ));
        assert_eq!(executors["SessionId"], session_id.as_str());
        assert!(executors["ExecutorsSummary"].as_array().unwrap().is_empty());

        let resp = run(account, "GetSession", json!({"SessionId": "fake"}));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["message"], "Session fake was not found");
        let resp = run(account, "TerminateSession", json!({"SessionId": "fake"}));
        assert_eq!(resp.status, 400);
        let resp = run(account, "GetSessionStatus", json!({"SessionId": "fake"}));
        assert_eq!(resp.status, 400);
        let resp = run(account, "GetSessionEndpoint", json!({"SessionId": "fake"}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn notebook_lifecycle() {
        let account = fresh_account();
        let id = body_of(&run(
            account,
            "CreateNotebook",
            json!({"WorkGroup": "primary", "Name": "nb-1"}),
        ))["NotebookId"]
            .as_str()
            .unwrap()
            .to_string();

        let exported = body_of(&run(account, "ExportNotebook", json!({"NotebookId": &id})));
        assert_eq!(exported["NotebookMetadata"]["NotebookId"], id.as_str());
        assert!(exported["Payload"].is_string());

        run(
            account,
            "UpdateNotebook",
            json!({"NotebookId": &id, "Payload": "{}"}),
        );
        let meta = body_of(&run(
            account,
            "GetNotebookMetadata",
            json!({"NotebookId": &id}),
        ));
        assert_eq!(meta["NotebookMetadata"]["Type"], "IPYNB");

        run(
            account,
            "UpdateNotebookMetadata",
            json!({"NotebookId": &id, "Name": "renamed"}),
        );
        let meta = body_of(&run(
            account,
            "GetNotebookMetadata",
            json!({"NotebookId": &id}),
        ));
        assert_eq!(meta["NotebookMetadata"]["Name"], "renamed");

        let resp = run(
            account,
            "ListNotebookMetadata",
            json!({"WorkGroup": "primary"}),
        );
        assert_eq!(
            body_of(&resp)["NotebookMetadataList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let resp = run(account, "ListNotebookSessions", json!({"NotebookId": &id}));
        assert!(body_of(&resp)["NotebookSessionsList"]
            .as_array()
            .unwrap()
            .is_empty());

        run(account, "DeleteNotebook", json!({"NotebookId": &id}));
        let resp = run(account, "GetNotebookMetadata", json!({"NotebookId": &id}));
        assert_eq!(resp.status, 400);
        // deleting a missing notebook is silently fine
        let resp = run(
            account,
            "DeleteNotebook",
            json!({"NotebookId": "fake-notebook-id"}),
        );
        assert_eq!(resp.status, 200);
        let resp = run(account, "ExportNotebook", json!({"NotebookId": "fake"}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn calculation_execution_lifecycle() {
        let account = fresh_account();
        let session_id = body_of(&run(
            account,
            "StartSession",
            json!({"WorkGroup": "primary"}),
        ))["SessionId"]
            .as_str()
            .unwrap()
            .to_string();

        let calc_id = body_of(&run(
            account,
            "StartCalculationExecution",
            json!({"SessionId": &session_id, "CodeBlock": "SELECT 1"}),
        ))["CalculationExecutionId"]
            .as_str()
            .unwrap()
            .to_string();

        let calc = body_of(&run(
            account,
            "GetCalculationExecution",
            json!({"CalculationExecutionId": &calc_id}),
        ));
        assert_eq!(calc["CodeBlock"], "SELECT 1");
        assert_eq!(calc["SessionId"], session_id.as_str());
        assert_eq!(calc["Status"]["State"], "RUNNING");

        let code = body_of(&run(
            account,
            "GetCalculationExecutionCode",
            json!({"CalculationExecutionId": &calc_id}),
        ));
        assert_eq!(code["CodeBlock"], "SELECT 1");

        let status = body_of(&run(
            account,
            "GetCalculationExecutionStatus",
            json!({"CalculationExecutionId": &calc_id}),
        ));
        assert_eq!(status["Statistics"]["DpuExecutionInMillis"], 1000);

        let listed = body_of(&run(
            account,
            "ListCalculationExecutions",
            json!({"SessionId": &session_id}),
        ));
        assert_eq!(listed["Calculations"].as_array().unwrap().len(), 1);

        let resp = run(
            account,
            "StopCalculationExecution",
            json!({"CalculationExecutionId": &calc_id}),
        );
        assert_eq!(body_of(&resp)["State"], "CANCELED");
        let resp = run(
            account,
            "StopCalculationExecution",
            json!({"CalculationExecutionId": &calc_id}),
        );
        assert_eq!(resp.status, 200); // stopped again is fine in moto
        let resp = run(
            account,
            "StopCalculationExecution",
            json!({"CalculationExecutionId": "fake"}),
        );
        assert_eq!(resp.status, 400);
        let resp = run(
            account,
            "GetCalculationExecution",
            json!({"CalculationExecutionId": "fake"}),
        );
        assert_eq!(
            body_of(&resp)["message"],
            "CalculationExecution fake was not found"
        );
    }

    #[test]
    fn databases_tables_and_static_lists() {
        let account = fresh_account();
        let resp = run(
            account,
            "GetDatabase",
            json!({"CatalogName": "AwsDataCatalog", "DatabaseName": "nope"}),
        );
        assert_eq!(
            body_of(&resp)["message"],
            "Database nope not found in catalog AwsDataCatalog"
        );
        let resp = run(
            account,
            "ListDatabases",
            json!({"CatalogName": "AwsDataCatalog"}),
        );
        assert!(body_of(&resp)["DatabaseList"]
            .as_array()
            .unwrap()
            .is_empty());

        let resp = run(
            account,
            "GetTableMetadata",
            json!({
                "CatalogName": "AwsDataCatalog",
                "DatabaseName": "default",
                "TableName": "missing",
            }),
        );
        assert_eq!(
            body_of(&resp)["message"],
            "Table missing not found in database default"
        );
        let resp = run(
            account,
            "ListTableMetadata",
            json!({"CatalogName": "AwsDataCatalog", "DatabaseName": "default"}),
        );
        assert!(body_of(&resp)["TableMetadataList"]
            .as_array()
            .unwrap()
            .is_empty());

        let themes = body_of(&run(account, "ListEngineVersions", json!({})));
        assert_eq!(themes["EngineVersions"].as_array().unwrap().len(), 4);
        let dpus = body_of(&run(account, "ListApplicationDPUSizes", json!({})));
        assert_eq!(dpus["ApplicationDPUSizes"].as_array().unwrap().len(), 2);

        // dashboard counts executions and named queries
        run(
            account,
            "CreateNamedQuery",
            json!({
                "Name": "dash-nq", "Database": "d", "QueryString": "SELECT 1"
            }),
        );
        let id = start(account, json!({"WorkGroup": "primary"}));
        let _ = id;
        let dash = body_of(&run(
            account,
            "GetResourceDashboard",
            json!({"ResourceARN": wg_arn(account, "primary")}),
        ));
        assert_eq!(dash["ResourceDashboard"]["QueryCount"], 1);
        assert!(
            dash["ResourceDashboard"]["NamedQueryCount"]
                .as_i64()
                .unwrap()
                >= 1
        );
    }
}
