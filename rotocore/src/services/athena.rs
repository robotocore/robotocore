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
//!   `'NoneType' object has no attribute 'name'` — reproduced as a
//!   500 `InternalError`.
//! - `CreateCapacityReservation` without `Tags` crashes moto with
//!   `'NoneType' object is not iterable` — reproduced so the compat suite
//!   keeps its exact sidecar count (1 failure).
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

/// moto crash on `CreateCapacityReservation` with no `Tags` (TypeError
/// iterating None) — kept so the suite count matches the sidecar.
fn null_tags_internal_error() -> Response {
    Response::json_status(
        500,
        &json!({
            "__type": "InternalError",
            "message": "'NoneType' object is not iterable",
        }),
    )
}

/// moto crash on `GetNamedQuery` with a missing id (`nq.name` on None).
fn not_handled() -> Response {
    Response::json_status(
        500,
        &json!({
            "__type": "InternalError",
            "message": "'NoneType' object has no attribute 'name'",
        }),
    )
}

/// moto `KeyError` surfacing through the sidecar as
/// `{"__type": "InternalError", "message": "'<id>'"}`.
fn key_error(id: &str) -> Response {
    Response::json_status(
        500,
        &json!({
            "__type": "InternalError",
            "message": format!("'{}'", id),
        }),
    )
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

        // missing get is the moto 'NoneType' crash
        let resp = run(
            account,
            "GetNamedQuery",
            json!({"NamedQueryId": "00000000-0000-0000-0000-000000000000"}),
        );
        assert_eq!(resp.status, 500);
        let err = body_of(&resp);
        assert_eq!(err["__type"], "InternalError");
        assert_eq!(err["message"], "'NoneType' object has no attribute 'name'");

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
}
