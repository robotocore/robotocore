//! Glue backend (JSON protocol, `X-Amz-Target: Glue.*`, content
//! `application/x-amz-json-1.1`).
//!
//! Port of moto's glue: the data catalog (databases, tables, partitions,
//! table versions, UDFs, catalogs), crawlers, jobs, triggers, workflows,
//! the schema registry, security configurations, dev endpoints,
//! interactive sessions, ML transforms, blueprints, data quality, and the
//! misc singletons the compat + lifecycle suites exercise.
//!
//! Wire conventions borrowed from moto's JSON serializer: null members are
//! omitted from response bodies, timestamps are unix-epoch seconds, and
//! errors are `{"__type": code, "message": msg}` with HTTP 400. Operations
//! rotocore does not serve answer 501 like the moto sidecar did.

use crate::proto::{now_iso, now_unix, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Default)]
struct DatabaseRec {
    input: Value,
    created: f64,
    tables: BTreeMap<String, TableRec>,
    udfs: BTreeMap<String, Value>, // FunctionInput entries (+ CreateTime)
}

#[derive(Default)]
struct TableRec {
    // Versions of TableInput; index 0 is version 1, the last is current.
    versions: Vec<Value>,
    created: f64,
    updated: Option<f64>,
    /// Partition records keyed by their values joined with U+001F.
    partitions: BTreeMap<String, PartitionRec>,
    partition_indexes: Vec<Value>,
    column_stats: BTreeMap<String, Value>,
}

#[derive(Clone, Default)]
struct PartitionRec {
    values: Vec<String>,
    input: Value,
    created: f64,
}

#[derive(Clone, Default)]
struct CrawlRec {
    crawl_id: String,
    status: String, // RUNNING -> COMPLETED / STOPPED
}

#[derive(Default)]
struct CrawlerRec {
    name: String,
    role: Value,
    database_name: Value,
    description: Value,
    targets: Value,
    schedule: Value,
    classifiers: Value,
    table_prefix: Value,
    schema_change_policy: Value,
    recrawl_policy: Value,
    lineage_configuration: Value,
    configuration: Value,
    crawler_security_configuration: Value,
    created: f64,
    crawls: Vec<CrawlRec>,
}

#[derive(Clone, Default)]
struct JobRunRec {
    id: String,
    previous_run_id: Option<String>,
    status: String, // STARTING -> RUNNING -> SUCCEEDED (STOPPED on batch stop)
    arguments: Option<Value>,
    allocated_capacity: Option<Value>,
    max_capacity: Option<Value>,
    timeout: Option<Value>,
    worker_type: Option<Value>,
    number_of_workers: Option<Value>,
    security_configuration: Option<Value>,
    notification_property: Option<Value>,
}

#[derive(Default)]
struct JobRec {
    /// Extracted job attributes (moto's FakeJob fields). Null members are
    /// omitted when the job is serialized.
    job: Value,
    created: f64,
    modified: f64,
    runs: Vec<JobRunRec>,
}

#[derive(Clone, Default)]
struct WorkflowRunRec {
    run_id: String,
    previous_run_id: Option<String>,
    properties: BTreeMap<String, String>,
    status: String, // RUNNING -> STOPPING after stop
    started: f64,
}

#[derive(Default)]
struct WorkflowRec {
    name: String,
    default_run_properties: Value,
    description: Value,
    max_concurrent_runs: Value,
    created: f64,
    modified: f64,
    runs: Vec<WorkflowRunRec>,
}

#[derive(Default)]
struct SessionRec {
    id: String,
    description: Value,
    role: Value,
    command: Value,
    timeout: Value,
    idle_timeout: Value,
    default_arguments: Value,
    connections: Value,
    max_capacity: Value,
    number_of_workers: Value,
    worker_type: Value,
    security_configuration: Value,
    glue_version: Value,
    tags: Value,
    request_origin: Value,
    state: String, // READY -> STOPPING
    created: f64,
    next_statement_id: u64,
    statements: BTreeMap<u64, Value>,
}

#[derive(Default)]
struct RegistryRec {
    name: String,
    arn: String,
    description: Value,
    tags: Value,
    created: f64,
    updated: f64,
    schemas: BTreeMap<String, SchemaRec>,
}

#[derive(Clone, Default)]
struct SchemaRec {
    registry_name: String,
    registry_arn: String,
    arn: String,
    schema_name: String,
    data_format: String,
    compatibility: String,
    description: Value,
    latest_schema_version: u64,
    next_schema_version: u64,
    first_version_id: String,
    versions: Vec<SchemaVersionRec>,
}

#[derive(Clone, Default)]
struct SchemaVersionRec {
    id: String,
    definition: String,
    version_number: u64,
    created: f64,
    metadata: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Default)]
struct TriggerRec {
    name: String,
    workflow_name: Value,
    trigger_type: String,
    schedule: Value,
    predicate: Option<Value>,
    actions: Vec<Value>,
    description: Value,
    start_on_creation: bool,
    event_batching_condition: Value,
}

#[derive(Clone, Default)]
struct ConnectionRec {
    name: String,
    input: Value,
    created: f64,
    updated: f64,
}

#[derive(Clone, Default)]
struct DevEndpointRec {
    name: String,
    role_arn: Value,
    security_group_ids: Value,
    subnet_id: String,
    worker_type: String,
    glue_version: String,
    number_of_workers: u64,
    number_of_nodes: u64,
    extra_python_libs_s3_path: Value,
    extra_jars_s3_path: Value,
    security_configuration: Value,
    arguments: Value,
    public_key: Value,
    public_keys: Value,
    created: f64,
    yarn_endpoint_address: Value,
    private_address: Value,
    zeppelin_port: u64,
    status: String,
    availability_zone: Value,
    vpc_id: Value,
}

#[derive(Clone, Default)]
struct BlueprintRec {
    name: String,
    blueprint_location: String,
    description: Value,
    created: f64,
}

#[derive(Clone, Default)]
struct MlTransformRec {
    transform_id: String,
    name: String,
    input_record_tables: Value,
    parameters: Value,
    role: Value,
    description: Value,
    glue_version: Value,
    max_capacity: Value,
    worker_type: Value,
    number_of_workers: Value,
    timeout: Value,
    max_retries: Value,
    created: f64,
    modified: f64,
}

#[derive(Clone, Default)]
struct IntegrationRec {
    name: String,
    source_arn: String,
    target_arn: String,
    description: Value,
    kms_key_id: Value,
    additional_encryption_context: Value,
    created: f64,
    modified: f64,
}

#[derive(Clone, Default)]
struct ConnectionTypeRec {
    connection_type: String,
    integration_type: String,
    connection_properties: Value,
    connector_authentication_configuration: Value,
    rest_configuration: Value,
    description: Value,
    capabilities: Value,
}

#[derive(Default)]
struct GlueState {
    databases: BTreeMap<String, DatabaseRec>,
    crawlers: BTreeMap<String, CrawlerRec>,
    jobs: BTreeMap<String, JobRec>,
    triggers: BTreeMap<String, TriggerRec>,
    workflows: BTreeMap<String, WorkflowRec>,
    connections: BTreeMap<String, ConnectionRec>,
    sessions: BTreeMap<String, SessionRec>,
    dev_endpoints: BTreeMap<String, DevEndpointRec>,
    registries: BTreeMap<String, RegistryRec>,
    security_configurations: BTreeMap<String, (Value, f64)>,
    blueprints: BTreeMap<String, BlueprintRec>,
    blueprint_runs: BTreeMap<String, Value>,
    ml_transforms: BTreeMap<String, MlTransformRec>,
    classifiers: BTreeMap<String, (String, Value)>, // name -> (kind, classifier dict)
    data_quality_rulesets: BTreeMap<String, (Value, f64)>,
    dq_recommendation_runs: BTreeMap<String, Value>,
    dq_evaluation_runs: BTreeMap<String, Value>,
    column_statistics_task_runs: BTreeMap<String, Value>,
    column_statistics_task_settings: BTreeMap<String, Value>,
    usage_profiles: BTreeMap<String, (Value, f64)>,
    custom_entity_types: BTreeMap<String, (String, Value)>, // name -> (regex, ContextWords)
    catalogs: BTreeMap<String, (Value, f64, f64)>,          // name -> (input, created, updated)
    resource_policies: BTreeMap<String, (String, String, f64, f64)>, // arn -> (json, hash, created, updated)
    data_catalog_encryption_settings: BTreeMap<String, Value>,
    integrations: BTreeMap<String, IntegrationRec>,
    integration_resource_properties: BTreeMap<String, Value>,
    integration_table_properties: BTreeMap<String, Value>,
    /// "catalog|db|table|type" -> (optimizer record, created)
    table_optimizers: BTreeMap<String, (Value, f64)>,
    connection_types: BTreeMap<String, ConnectionTypeRec>,
    identity_center_config: Option<Value>,
    mv_refresh_task_runs: BTreeMap<String, Value>,
    /// Resource ARN -> tag map (moto's TaggingService).
    tags: BTreeMap<String, BTreeMap<String, String>>,
    /// The (account, region) scope this partition belongs to.
    scope_region: String,
    scope_account: u64,
}

fn state() -> store::Slot<GlueState> {
    static SLOT: Lazy<store::Slot<GlueState>> = Lazy::new(|| store::slot("glue"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers: drop nulls like moto, 400-status AWS error shapes
// ---------------------------------------------------------------------------

fn nulls_dropped(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                if val.is_null() {
                    continue;
                }
                out.insert(k.clone(), nulls_dropped(val));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(nulls_dropped).collect()),
        other => other.clone(),
    }
}

fn ok(v: &Value) -> Response {
    Response::json(
        200,
        serde_json::to_vec(&nulls_dropped(v)).unwrap_or_default(),
    )
}

fn empty_ok() -> Response {
    ok(&json!({}))
}

fn err(status: u16, code: &str, message: &str) -> Response {
    Response::json(
        status,
        serde_json::to_vec(&json!({"__type": code, "message": message})).unwrap_or_default(),
    )
}

fn not_found(message: &str) -> Response {
    err(400, "EntityNotFoundException", message)
}

fn already_exists(message: &str) -> Response {
    err(400, "AlreadyExistsException", message)
}

fn invalid_input(message: &str) -> Response {
    err(400, "InvalidInputException", message)
}

/// GSR InvalidInputException has the same code, no-op alias for clarity.
fn gsr_invalid_input(message: &str) -> Response {
    invalid_input(message)
}

/// Deny-by-default answer for operations rotocore does not serve.
fn unimplemented(operation: &str) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The glue {operation} action has not been implemented"),
    )
}

/// Bridge a Result-returning lookup into a `Response`-returning handler.
macro_rules! try_resp {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

/// `jr_x`-style hex suffix (moto uses random_hex(n)).
fn hex_n(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    s.truncate(n);
    s
}

fn uuid4() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn make_arn(region: &str, account: u64, kind: &str, name: &str) -> String {
    format!("arn:aws:glue:{region}:{account}:{kind}/{name}")
}

fn str_field(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_string()
}

fn str_list(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|x| x.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn account_of(req: &Request) -> String {
    req.account.to_string()
}

fn arn_of(req: &Request, kind: &str, name: &str) -> String {
    format!("arn:aws:glue:{}:{}:{kind}/{name}", req.region, req.account)
}

fn tag_resource(g: &mut GlueState, arn: &str, tags: Option<&Value>) {
    if let Some(map) = tags.and_then(Value::as_object) {
        let entry = g.tags.entry(arn.to_string()).or_default();
        for (k, v) in map {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
    }
}

fn get_tag_dict(g: &GlueState, arn: &str) -> Value {
    match g.tags.get(arn) {
        Some(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), Value::String(v.clone()));
            }
            Value::Object(out)
        }
        None => json!({}),
    }
}

fn values_list(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|x| x.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// moto's `get_tables` Expression: `*` is a glob wildcard, all other
/// characters match literally.
fn wildcard_match(expression: &str, name: &str) -> bool {
    match expression.split_once('*') {
        None => name == expression,
        Some((prefix, suffix)) => {
            name.starts_with(prefix)
                && name.len() >= prefix.len() + suffix.len()
                && name.ends_with(suffix)
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "glue" {
        return None;
    }
    let p = parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let g = st.entry((req.account, req.region.clone())).or_default();
    g.scope_region = req.region.clone();
    g.scope_account = req.account;
    Some(handle(req, g, req.operation.as_str(), &p))
}

fn handle(req: &Request, g: &mut GlueState, op: &str, p: &Value) -> Response {
    match op {
        // --- Databases -------------------------------------------------
        "CreateDatabase" => create_database(req, g, p),
        "GetDatabase" => get_database(req, g, p),
        "GetDatabases" => get_databases(req, g),
        "UpdateDatabase" => update_database(g, p),
        "DeleteDatabase" => delete_database(g, p),
        // --- Tables + versions ------------------------------------------
        "CreateTable" => create_table(g, p),
        "GetTable" => get_table(req, g, p),
        "GetTables" => get_tables(req, g, p),
        "UpdateTable" => update_table(g, p),
        "DeleteTable" => delete_table(g, p),
        "BatchDeleteTable" => batch_delete_table(g, p),
        "GetTableVersions" => get_table_versions(req, g, p),
        "GetTableVersion" => get_table_version(req, g, p),
        "DeleteTableVersion" => delete_table_version(g, p),
        "BatchDeleteTableVersion" => try_resp!(batch_delete_table_version(g, p)),
        // --- Partitions ---------------------------------------------------
        "CreatePartition" => create_partition(g, p),
        "GetPartition" => get_partition(g, p),
        "GetPartitions" => get_partitions(g, p),
        "UpdatePartition" => update_partition(g, p),
        "DeletePartition" => delete_partition(g, p),
        "BatchGetPartition" => try_resp!(batch_get_partition(g, p)),
        "BatchCreatePartition" => try_resp!(batch_create_partition(g, p)),
        "BatchDeletePartition" => try_resp!(batch_delete_partition(g, p)),
        "BatchUpdatePartition" => try_resp!(batch_update_partition(g, p)),
        "GetPartitionIndexes" => in_table(g, p, |_, _| {
            ok(&json!({"PartitionIndexDescriptorList": []}))
        }),
        "CreatePartitionIndex" | "DeletePartitionIndex" => in_table(g, p, |_, _| empty_ok()),
        // --- UDFs ---------------------------------------------------------
        "CreateUserDefinedFunction" => try_resp!(create_user_defined_function(g, p)),
        "GetUserDefinedFunction" => try_resp!(get_user_defined_function(g, p)),
        "GetUserDefinedFunctions" => try_resp!(get_user_defined_functions(g, p)),
        "UpdateUserDefinedFunction" => try_resp!(update_user_defined_function(g, p)),
        "DeleteUserDefinedFunction" => try_resp!(delete_user_defined_function(g, p)),
        // --- Search / unfiltered metadata ----------------------------------
        "SearchTables" => search_tables(req, g, p),
        "GetUnfilteredTableMetadata" => get_unfiltered_table_metadata(req, g, p),
        "GetUnfilteredPartitionMetadata" => get_unfiltered_partition_metadata(g, p),
        "GetUnfilteredPartitionsMetadata" => get_unfiltered_partitions_metadata(g, p),
        // --- Column statistics -----------------------------------------------
        "UpdateColumnStatisticsForTable" => update_column_stats_for_table(g, p),
        "UpdateColumnStatisticsForPartition" => in_table(g, p, |_, _| ok(&json!({"Errors": []}))),
        "GetColumnStatisticsForTable" => get_column_statistics_for_table(g, p),
        "GetColumnStatisticsForPartition" => in_table(g, p, |_, _| {
            ok(&json!({"ColumnStatisticsList": [], "Errors": []}))
        }),
        "DeleteColumnStatisticsForTable" | "DeleteColumnStatisticsForPartition" => {
            in_table(g, p, |_, _| empty_ok())
        }
        // --- Catalogs --------------------------------------------------------
        "CreateCatalog" => create_catalog(g, p),
        "GetCatalog" => get_catalog(req, g, p),
        "UpdateCatalog" => update_catalog(g, p),
        "DeleteCatalog" => delete_catalog(g, p),
        "GetCatalogs" => get_catalogs(req, g),
        "ImportCatalogToGlue" => empty_ok(),
        "GetCatalogImportStatus" => ok(&json!({
            "ImportStatus": {
                "ImportCompleted": true,
                "ImportTime": now_unix(),
                "ImportedBy": "AWS",
            }
        })),
        // --- Crawlers ------------------------------------------------------
        "CreateCrawler" => create_crawler(req, g, p),
        "GetCrawler" => get_crawler(g, p),
        "GetCrawlers" => get_crawlers(g),
        "ListCrawlers" => list_crawlers(g),
        "DeleteCrawler" => delete_crawler(g, p),
        "StartCrawler" => start_crawler(g, p),
        "StopCrawler" => stop_crawler(g, p),
        "UpdateCrawler" => update_crawler(g, p),
        "UpdateCrawlerSchedule" => update_crawler_schedule(g, p),
        "StartCrawlerSchedule" | "StopCrawlerSchedule" => crawler_schedule_stub(g, p),
        "BatchGetCrawlers" => batch_get_crawlers(g, p),
        "GetCrawlerMetrics" => get_crawler_metrics(g, p),
        "ListCrawls" => list_crawls(g, p),
        // --- Jobs ----------------------------------------------------------
        "CreateJob" => create_job(req, g, p),
        "GetJob" => get_job(g, p),
        "GetJobs" => get_jobs(g),
        "UpdateJob" => update_job(g, p),
        "DeleteJob" => delete_job(g, p),
        "ListJobs" => list_jobs(g),
        "StartJobRun" => start_job_run(g, p),
        "GetJobRun" => try_resp!(get_job_run(g, p)),
        "GetJobRuns" => try_resp!(get_job_runs(g, p)),
        "BatchGetJobs" => batch_get_jobs(g, p),
        "BatchStopJobRun" => try_resp!(batch_stop_job_run(req, g, p)),
        "GetJobBookmark" | "ResetJobBookmark" => try_resp!(job_bookmark(g, p)),
        "UpdateJobFromSourceControl" => update_job_from_source_control(g, p),
        "UpdateSourceControlFromJob" => update_source_control_from_job(g, p),
        // --- Tags ----------------------------------------------------------
        "GetTags" => get_tags(g, p),
        "TagResource" => tag_resource_op(g, p),
        "UntagResource" => untag_resource_op(g, p),
        // --- Triggers ------------------------------------------------------
        "CreateTrigger" => create_trigger(req, g, p),
        "GetTrigger" => get_trigger(g, p),
        "GetTriggers" => get_triggers(g, p),
        "ListTriggers" => list_triggers(g, p),
        "BatchGetTriggers" => batch_get_triggers(g, p),
        "StartTrigger" => try_resp!(start_trigger(g, p)),
        "StopTrigger" => try_resp!(stop_trigger(g, p)),
        "UpdateTrigger" => try_resp!(update_trigger(g, p)),
        "DeleteTrigger" => try_resp!(delete_trigger(g, p)),
        // --- Workflows -----------------------------------------------------
        "CreateWorkflow" => create_workflow(req, g, p),
        "GetWorkflow" => try_resp!(get_workflow(g, p)),
        "ListWorkflows" => list_workflows(g),
        "DeleteWorkflow" => delete_workflow(g, p),
        "UpdateWorkflow" => try_resp!(update_workflow(g, p)),
        "BatchGetWorkflows" => batch_get_workflows(g, p),
        "StartWorkflowRun" => try_resp!(start_workflow_run(g, p)),
        "GetWorkflowRun" => try_resp!(get_workflow_run(g, p)),
        "GetWorkflowRuns" => try_resp!(get_workflow_runs(g, p)),
        "StopWorkflowRun" => try_resp!(stop_workflow_run(g, p)),
        "PutWorkflowRunProperties" => try_resp!(put_workflow_run_properties(g, p)),
        "GetWorkflowRunProperties" => try_resp!(get_workflow_run_properties(g, p)),
        "ResumeWorkflowRun" => try_resp!(resume_workflow_run(g, p)),
        // --- Schema registry ------------------------------------------------
        "CreateRegistry" => try_resp!(create_registry(req, g, p)),
        "DeleteRegistry" => try_resp!(delete_registry(g, p)),
        "GetRegistry" => try_resp!(get_registry(g, p)),
        "ListRegistries" => try_resp!(list_registries(g)),
        "UpdateRegistry" => try_resp!(update_registry(g, p)),
        "CreateSchema" => try_resp!(create_schema(g, p)),
        "GetSchema" => try_resp!(get_schema(g, p)),
        "DeleteSchema" => try_resp!(delete_schema(g, p)),
        "UpdateSchema" => try_resp!(update_schema(g, p)),
        "ListSchemas" => try_resp!(list_schemas(g, p)),
        "ListSchemaVersions" => try_resp!(list_schema_versions(g, p)),
        "RegisterSchemaVersion" => try_resp!(register_schema_version(g, p)),
        "GetSchemaVersion" => try_resp!(get_schema_version(g, p)),
        "GetSchemaByDefinition" => try_resp!(get_schema_by_definition(g, p)),
        "CheckSchemaVersionValidity" => try_resp!(check_schema_version_validity(p)),
        "DeleteSchemaVersions" => try_resp!(delete_schema_versions(p)),
        "GetSchemaVersionsDiff" => try_resp!(get_schema_versions_diff(g, p)),
        "PutSchemaVersionMetadata" => try_resp!(put_schema_version_metadata(g, p)),
        "QuerySchemaVersionMetadata" => try_resp!(query_schema_version_metadata(g, p)),
        "RemoveSchemaVersionMetadata" => try_resp!(remove_schema_version_metadata(g, p)),
        // --- Security configurations ----------------------------------------
        "CreateSecurityConfiguration" => try_resp!(create_security_configuration(g, p)),
        "GetSecurityConfiguration" => try_resp!(get_security_configuration(g, p)),
        "GetSecurityConfigurations" => try_resp!(get_security_configurations(g)),
        "DeleteSecurityConfiguration" => try_resp!(delete_security_configuration(g, p)),
        // --- Dev endpoints ---------------------------------------------------
        "CreateDevEndpoint" => try_resp!(create_dev_endpoint(g, p)),
        "GetDevEndpoint" => try_resp!(get_dev_endpoint(g, p)),
        "GetDevEndpoints" => try_resp!(get_dev_endpoints(g)),
        "ListDevEndpoints" => try_resp!(list_dev_endpoints(g)),
        "DeleteDevEndpoint" => try_resp!(delete_dev_endpoint(g, p)),
        "UpdateDevEndpoint" => try_resp!(update_dev_endpoint(g, p)),
        "BatchGetDevEndpoints" => try_resp!(batch_get_dev_endpoints(g, p)),
        // --- Sessions + statements --------------------------------------------
        "CreateSession" => try_resp!(create_session(g, p)),
        "GetSession" => try_resp!(get_session(g, p)),
        "ListSessions" => try_resp!(list_sessions(g)),
        "DeleteSession" => try_resp!(delete_session(g, p)),
        "StopSession" => try_resp!(stop_session(g, p)),
        "RunStatement" => try_resp!(run_statement(g, p)),
        "GetStatement" => try_resp!(get_statement(g, p)),
        "ListStatements" => try_resp!(list_statements(g, p)),
        "CancelStatement" => try_resp!(cancel_statement(g, p)),
        // --- Connections -------------------------------------------------------
        "CreateConnection" => try_resp!(create_connection(req, g, p)),
        "GetConnection" => try_resp!(get_connection(g, p)),
        "GetConnections" => try_resp!(get_connections(g)),
        "UpdateConnection" => try_resp!(update_connection(g, p)),
        "DeleteConnection" => try_resp!(delete_connection(g, p)),
        "BatchDeleteConnection" => try_resp!(batch_delete_connection(g, p)),
        // --- Resource policies + catalog encryption ----------------------------
        "PutResourcePolicy" => try_resp!(put_resource_policy(g, p)),
        "GetResourcePolicy" => try_resp!(get_resource_policy(g, p)),
        "DeleteResourcePolicy" => try_resp!(delete_resource_policy(g, p)),
        "GetResourcePolicies" => try_resp!(get_resource_policies(g)),
        "PutDataCatalogEncryptionSettings" => try_resp!(put_dces(g, p)),
        "GetDataCatalogEncryptionSettings" => try_resp!(get_dces(g, p)),
        // --- ML transforms + task runs -----------------------------------------
        "CreateMLTransform" => try_resp!(create_ml_transform(req, g, p)),
        "GetMLTransform" => try_resp!(get_ml_transform(g, p)),
        "GetMLTransforms" => try_resp!(get_ml_transforms(g)),
        "ListMLTransforms" => try_resp!(list_ml_transforms(g)),
        "DeleteMLTransform" => try_resp!(delete_ml_transform(g, p)),
        "UpdateMLTransform" => try_resp!(update_ml_transform(g, p)),
        "GetMLTaskRun" => try_resp!(get_ml_task_run(g, p)),
        "GetMLTaskRuns" => try_resp!(get_ml_task_runs(g, p)),
        "CancelMLTaskRun" => try_resp!(cancel_ml_task_run(g, p)),
        "StartMLEvaluationTaskRun" => try_resp!(start_ml_eval_task_run(g, p)),
        "StartMLLabelingSetGenerationTaskRun" => try_resp!(start_ml_eval_task_run(g, p)),
        "StartExportLabelsTaskRun" => try_resp!(start_ml_eval_task_run(g, p)),
        "StartImportLabelsTaskRun" => try_resp!(start_ml_eval_task_run(g, p)),
        // --- Blueprints + runs --------------------------------------------------
        "CreateBlueprint" => try_resp!(create_blueprint(g, p)),
        "GetBlueprint" => try_resp!(get_blueprint(g, p)),
        "ListBlueprints" => try_resp!(list_blueprints(g)),
        "DeleteBlueprint" => try_resp!(delete_blueprint(g, p)),
        "UpdateBlueprint" => try_resp!(update_blueprint(g, p)),
        "BatchGetBlueprints" => try_resp!(batch_get_blueprints(g, p)),
        "StartBlueprintRun" => try_resp!(start_blueprint_run(g, p)),
        "GetBlueprintRun" => try_resp!(get_blueprint_run(g, p)),
        "GetBlueprintRuns" => try_resp!(get_blueprint_runs(g, p)),
        // --- Data quality rulesets + runs ---------------------------------------
        "CreateDataQualityRuleset" => try_resp!(create_dq_ruleset(req, g, p)),
        "GetDataQualityRuleset" => try_resp!(get_dq_ruleset(g, p)),
        "ListDataQualityRulesets" => try_resp!(list_dq_rulesets(g)),
        "DeleteDataQualityRuleset" => try_resp!(delete_dq_ruleset(g, p)),
        "UpdateDataQualityRuleset" => try_resp!(update_dq_ruleset(g, p)),
        "StartDataQualityRuleRecommendationRun" => try_resp!(start_dq_recommendation_run(g, p)),
        "GetDataQualityRuleRecommendationRun" => try_resp!(get_dq_recommendation_run(g, p)),
        "CancelDataQualityRuleRecommendationRun" => empty_ok(),
        "StartDataQualityRulesetEvaluationRun" => try_resp!(start_dq_evaluation_run(g, p)),
        "GetDataQualityRulesetEvaluationRun" => try_resp!(get_dq_evaluation_run(g, p)),
        "CancelDataQualityRulesetEvaluationRun" => empty_ok(),
        "GetDataQualityResult" => try_resp!(get_dq_result(g, p)),
        "BatchGetDataQualityResult" => try_resp!(batch_get_dq_result(g, p)),
        "ListDataQualityResults" => ok(&json!({"Results": []})),
        "ListDataQualityRuleRecommendationRuns" => try_resp!(recommendations_list(g)),
        "ListDataQualityRulesetEvaluationRuns" => try_resp!(evaluations_list(g)),
        "GetDataQualityModel" | "GetDataQualityModelResult" => {
            try_resp!(get_dq_model(g, p))
        }
        "ListDataQualityStatistics" => ok(&json!({"Statistics": []})),
        "ListDataQualityStatisticAnnotations" => ok(&json!({"Annotations": []})),
        "BatchPutDataQualityStatisticAnnotation" => ok(&json!({"FailedInclusionAnnotations": []})),
        "PutDataQualityProfileAnnotation" => empty_ok(),
        // --- Everything else: deny by default (moto 501s too) ----------------
        _ => unimplemented(op),
    }
}

/// Run `f` when the addressed table exists; surface moto's errors otherwise.
fn in_table(g: &mut GlueState, p: &Value, f: impl FnOnce(&str, &str) -> Response) -> Response {
    match find_table(g, p) {
        Ok(t) => f(&t.db, &t.tbl),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// Lookup helpers (Result-based, bridged through try_resp! in handlers)
// ---------------------------------------------------------------------------

fn find_database_mut<'a>(g: &'a mut GlueState, db: &str) -> Result<&'a mut DatabaseRec, Response> {
    g.databases
        .get_mut(db)
        .ok_or_else(|| not_found(&format!("Database {db} not found.")))
}

fn table_name_from(p: &Value) -> String {
    if let Some(v) = p.get("TableName") {
        if let Some(s) = v.as_str() {
            return s.to_string();
        }
    }
    str_field(&p["TableInput"], "Name")
}

struct TableRef<'a> {
    db: String,
    tbl: String,
    rec: &'a mut TableRec,
}

/// Resolve the table addressed by the params (`TableName`, or
/// `TableInput.Name` where the op carries a TableInput). The database must
/// exist first — matching moto's error precedence.
fn find_table<'a>(g: &'a mut GlueState, p: &Value) -> Result<TableRef<'a>, Response> {
    find_table_key(g, p, "TableName")
}

/// Same resolution with an explicit table-name member: API v3 ops address a
/// table via `TableName`, Create/UpdateTable via `TableInput.Name`, and the
/// legacy GetTable/DeleteTable via `Name`.
fn find_table_key<'a>(
    g: &'a mut GlueState,
    p: &Value,
    member: &str,
) -> Result<TableRef<'a>, Response> {
    let db = str_field(p, "DatabaseName");
    let tbl = match p.get(member) {
        Some(v) if v.is_string() => v.as_str().unwrap_or_default().to_string(),
        _ => str_field(&p["TableInput"], "Name"),
    };
    let Some(db_rec) = g.databases.get_mut(&db) else {
        return Err(not_found(&format!("Database {db} not found.")));
    };
    let Some(rec) = db_rec.tables.get_mut(&tbl) else {
        return Err(not_found(&format!("Table {tbl} not found.")));
    };
    Ok(TableRef { db, tbl, rec })
}

struct PartRef<'a> {
    db: String,
    tbl: String,
    part: &'a mut PartitionRec,
}

/// Resolve (db, table, partition) in moto's error precedence: database,
/// then table, then the partition itself.
fn find_partition<'a>(g: &'a mut GlueState, p: &Value) -> Result<PartRef<'a>, Response> {
    let db = str_field(p, "DatabaseName");
    let tbl = str_field(p, "TableName");
    let values = str_list(&p["PartitionValues"]);
    let key = partition_key(&values);
    let Some(db_rec) = g.databases.get_mut(&db) else {
        return Err(not_found(&format!("Database {db} not found.")));
    };
    let Some(table) = db_rec.tables.get_mut(&tbl) else {
        return Err(not_found(&format!("Table {tbl} not found.")));
    };
    let Some(part) = table.partitions.get_mut(&key) else {
        return Err(not_found("Cannot find partition."));
    };
    Ok(PartRef { db, tbl, part })
}

fn partition_key(values: &[String]) -> String {
    values.join("\u{1f}")
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

fn database_as_dict(rec: &DatabaseRec, account: &str) -> Value {
    let input = &rec.input;
    let catalog_id = match input.get("CatalogId") {
        Some(v) if !v.is_null() => v.clone(),
        _ => json!(account),
    };
    json!({
        "Name": input["Name"],
        "Description": input["Description"],
        "LocationUri": input["LocationUri"],
        "Parameters": input["Parameters"],
        "CreateTime": rec.created,
        "CreateTableDefaultPermissions": input["CreateTableDefaultPermissions"],
        "TargetDatabase": input["TargetDatabase"],
        "CatalogId": catalog_id,
    })
}

fn create_database(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let mut input = p["DatabaseInput"].clone();
    let name = str_field(&input, "Name");
    if name.is_empty() {
        return invalid_input("DatabaseInput.Name is required.");
    }
    if g.databases.contains_key(&name) {
        return already_exists("Database already exists.");
    }
    if let Some(catalog_id) = p.get("CatalogId") {
        input["CatalogId"] = catalog_id.clone();
    }
    let rec = DatabaseRec {
        input,
        created: now_unix(),
        ..Default::default()
    };
    let arn = arn_of(req, "database", &name);
    tag_resource(g, &arn, p.get("Tags"));
    g.databases.insert(name, rec);
    empty_ok()
}

fn get_database(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    match g.databases.get(&name) {
        Some(rec) => ok(&json!({"Database": database_as_dict(rec, &account_of(req))})),
        None => not_found(&format!("Database {name} not found.")),
    }
}

fn get_databases(req: &Request, g: &mut GlueState) -> Response {
    let account = account_of(req);
    let list: Vec<Value> = g
        .databases
        .values()
        .map(|r| database_as_dict(r, &account))
        .collect();
    ok(&json!({ "DatabaseList": list }))
}

fn update_database(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    let input = p["DatabaseInput"].clone();
    match g.databases.get_mut(&name) {
        Some(rec) => {
            rec.input = input;
            empty_ok()
        }
        None => not_found(&format!("Database {name} not found.")),
    }
}

fn delete_database(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    if g.databases.remove(&name).is_none() {
        return not_found(&format!("Database {name} not found."));
    }
    empty_ok()
}

// Tables / versions / partitions / UDFs / catalogs follow in family chunks.

// ---------------------------------------------------------------------------
// Tables + versions
// ---------------------------------------------------------------------------

/// moto's `FakeTable.as_dict(version)`: base metadata merged with the
/// version's TableInput; `VersionId` always names the requested version.
fn table_version_dict(
    rec: &TableRec,
    db: &str,
    tbl: &str,
    account: &str,
    version: Option<usize>,
) -> Value {
    // moto keys table versions from 1 ("1", "2", ...). `version` is the
    // 0-based index into the versions vec; the wire id is index+1.
    let ver = version.unwrap_or_else(|| rec.versions.len().saturating_sub(1));
    let mut obj = json!({
        "DatabaseName": db,
        "Name": tbl,
        "CreateTime": rec.created,
        "CatalogId": account,
        "VersionId": (ver + 1).to_string(),
    });
    let base = obj.as_object_mut().unwrap();
    if let Some(input) = rec.versions.get(ver) {
        for (k, v) in input.as_object().unwrap_or(&Map::new()) {
            base.insert(k.clone(), nulls_dropped(v));
        }
    }
    if let Some(u) = rec.updated {
        base.insert("UpdateTime".into(), json!(u));
    }
    obj
}

fn create_table(g: &mut GlueState, p: &Value) -> Response {
    let input = p["TableInput"].clone();
    if input.is_null() || input.as_object().map(|o| o.is_empty()).unwrap_or(false) {
        return invalid_input("TableInput is required.");
    }
    let name = str_field(&input, "Name");
    if name.is_empty() {
        return invalid_input("TableInput.Name is required.");
    }
    let db = str_field(p, "DatabaseName");
    let db_rec = try_resp!(find_database_mut(g, &db));
    if db_rec.tables.contains_key(&name) {
        return already_exists("Table already exists.");
    }
    db_rec.tables.insert(
        name,
        TableRec {
            versions: vec![input],
            created: now_unix(),
            ..Default::default()
        },
    );
    empty_ok()
}

fn get_table(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let account = account_of(req);
    let t = try_resp!(find_table_key(g, p, "Name"));
    ok(&json!({
        "Table": table_version_dict(t.rec, &t.db, &t.tbl, &account, None),
    }))
}

fn get_tables(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let expression = p["Expression"].as_str();
    let account = account_of(req);
    let db_rec = try_resp!(find_database_mut(g, &db));
    let list: Vec<Value> = db_rec
        .tables
        .iter()
        .filter(|(name, _)| expression.map_or(true, |e| wildcard_match(e, name)))
        .map(|(name, rec)| table_version_dict(rec, &db, name, &account, None))
        .collect();
    ok(&json!({ "TableList": list }))
}

fn update_table(g: &mut GlueState, p: &Value) -> Response {
    let input = p["TableInput"].clone();
    if input.is_null() || input.as_object().map(|o| o.is_empty()).unwrap_or(false) {
        return invalid_input("TableInput is required.");
    }
    let t = try_resp!(find_table(g, p));
    t.rec.versions.push(input);
    t.rec.updated = Some(now_unix());
    empty_ok()
}

fn delete_table(g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let tbl = str_field(p, "Name");
    let db_rec = try_resp!(find_database_mut(g, &db));
    if db_rec.tables.remove(&tbl).is_none() {
        return not_found(&format!("Table {tbl} not found."));
    }
    empty_ok()
}

fn batch_delete_table(g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let db_rec = try_resp!(find_database_mut(g, &db));
    let mut errors: Vec<Value> = Vec::new();
    for tbl in p["TablesToDelete"].as_array().unwrap_or(&Vec::new()) {
        let name = tbl.as_str().unwrap_or_default().to_string();
        if db_rec.tables.remove(&name).is_none() {
            errors.push(json!({
                "TableName": name,
                "ErrorDetail": {
                    "ErrorCode": "EntityNotFoundException",
                    "ErrorMessage": "Table not found",
                },
            }));
        }
    }
    ok(&json!({ "Errors": errors }))
}

fn version_id_parse(ver: &str) -> Result<usize, Response> {
    match ver.parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(invalid_input(&format!(
            "invalid literal for int() with base 10: '{ver}'"
        ))),
    }
}

fn get_table_versions(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let tbl = str_field(p, "TableName");
    let account = account_of(req);
    let t = try_resp!(find_table(g, p));
    let mut list = Vec::new();
    for i in 1..=t.rec.versions.len() {
        let mut table = table_version_dict(t.rec, &db, &tbl, &account, Some(i - 1));
        if let Some(obj) = table.as_object_mut() {
            obj.insert("VersionId".into(), json!(i.to_string()));
        }
        list.push(json!({"Table": table, "VersionId": i.to_string()}));
    }
    ok(&json!({ "TableVersions": list }))
}

fn get_table_version(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let tbl = str_field(p, "TableName");
    let account = account_of(req);
    let ver = p["VersionId"].as_str().unwrap_or_default().to_string();
    let num = try_resp!(version_id_parse(&ver));
    let t = try_resp!(find_table(g, p));
    if num > t.rec.versions.len() {
        return not_found("Version not found.");
    }
    let table = table_version_dict(t.rec, &db, &tbl, &account, Some(num - 1));
    ok(&json!({
        "TableVersion": {
            "Table": table,
            "VersionId": ver,
        }
    }))
}

fn delete_table_version(g: &mut GlueState, p: &Value) -> Response {
    let ver = p["VersionId"].as_str().unwrap_or_default().to_string();
    let num = try_resp!(version_id_parse(&ver));
    let t = try_resp!(find_table(g, p));
    if num > t.rec.versions.len() {
        return not_found("Version not found.");
    }
    t.rec.versions.remove(num - 1);
    empty_ok()
}

fn batch_delete_table_version(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let t = find_table(g, p)?;
    let mut errors: Vec<Value> = Vec::new();
    for ver in p["VersionIds"].as_array().unwrap_or(&Vec::new()) {
        let ver = ver.as_str().unwrap_or_default().to_string();
        let deleted = match ver.parse::<usize>() {
            Ok(n) if n > 0 && n <= t.rec.versions.len() => {
                t.rec.versions.remove(n - 1);
                true
            }
            _ => false,
        };
        if !deleted {
            errors.push(json!({
                "TableName": t.tbl,
                "VersionId": ver,
                "ErrorDetail": {
                    "ErrorCode": "EntityNotFoundException",
                    "ErrorMessage": format!("Version {ver} not found"),
                },
            }));
        }
    }
    Ok(ok(&json!({ "Errors": errors })))
}

// ---------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------

fn partition_as_dict(rec: &PartitionRec, db: &str, tbl: &str) -> Value {
    let mut obj = json!({
        "DatabaseName": db,
        "TableName": tbl,
        "CreationTime": rec.created,
    });
    let base = obj.as_object_mut().unwrap();
    for (k, v) in rec.input.as_object().unwrap_or(&Map::new()) {
        base.insert(k.clone(), nulls_dropped(v));
    }
    obj
}

fn partition_input_values(input: &Value) -> Vec<String> {
    values_list(&input["Values"])
}

fn replace_partition(
    table: &mut BTreeMap<String, PartitionRec>,
    db: &str,
    tbl: &str,
    old_values: &[String],
    values: Vec<String>,
    input: Value,
) -> Response {
    let old_key = partition_key(old_values);
    let new_key = partition_key(&values);
    if old_key == new_key {
        if !table.contains_key(&old_key) {
            return not_found("Cannot find partition.");
        }
    } else {
        if table.remove(&old_key).is_none() {
            return not_found("Cannot find partition.");
        }
        if table.contains_key(&new_key) {
            return already_exists("Partition already exists.");
        }
    }
    let _ = (db, tbl);
    table.insert(
        new_key,
        PartitionRec {
            values,
            input,
            created: now_unix(),
        },
    );
    empty_ok()
}

fn create_partition(g: &mut GlueState, p: &Value) -> Response {
    let input = p["PartitionInput"].clone();
    let values = partition_input_values(&input);
    let key = partition_key(&values);
    let t = try_resp!(find_table(g, p));
    if t.rec.partitions.contains_key(&key) {
        return already_exists("Partition already exists.");
    }
    t.rec.partitions.insert(
        key,
        PartitionRec {
            values,
            input,
            created: now_unix(),
        },
    );
    empty_ok()
}

fn get_partition(g: &mut GlueState, p: &Value) -> Response {
    let part = try_resp!(find_partition(g, p));
    let db = part.db.clone();
    let tbl = part.tbl.clone();
    ok(&json!({
        "Partition": partition_as_dict(part.part, &db, &tbl),
    }))
}

fn get_partitions(g: &mut GlueState, p: &Value) -> Response {
    let db = str_field(p, "DatabaseName");
    let tbl = str_field(p, "TableName");
    let t = try_resp!(find_table(g, p));
    let list: Vec<Value> = t
        .rec
        .partitions
        .values()
        .map(|part| partition_as_dict(part, &db, &tbl))
        .collect();
    ok(&json!({ "Partitions": list }))
}

fn update_partition(g: &mut GlueState, p: &Value) -> Response {
    let old_values = str_list(&p["PartitionValueList"]);
    let input = p["PartitionInput"].clone();
    let values = partition_input_values(&input);
    let t = try_resp!(find_table(g, p));
    replace_partition(
        &mut t.rec.partitions,
        &t.db,
        &t.tbl,
        &old_values,
        values,
        input,
    )
}

fn delete_partition(g: &mut GlueState, p: &Value) -> Response {
    let values = str_list(&p["PartitionValues"]);
    let key = partition_key(&values);
    let t = try_resp!(find_table(g, p));
    if t.rec.partitions.remove(&key).is_none() {
        return not_found("Cannot find partition.");
    }
    empty_ok()
}

fn batch_create_partition(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let t = find_table(g, p)?;
    let mut errors: Vec<Value> = Vec::new();
    for input in p["PartitionInputList"].as_array().unwrap_or(&Vec::new()) {
        let values = partition_input_values(input);
        let key = partition_key(&values);
        if t.rec.partitions.contains_key(&key) {
            let vals: Vec<Value> = values.iter().map(|v| json!(v)).collect();
            errors.push(json!({
                "PartitionValues": vals,
                "ErrorDetail": {
                    "ErrorCode": "AlreadyExistsException",
                    "ErrorMessage": "Partition already exists.",
                },
            }));
        } else {
            t.rec.partitions.insert(
                key,
                PartitionRec {
                    values,
                    input: input.clone(),
                    created: now_unix(),
                },
            );
        }
    }
    Ok(ok(&json!({ "Errors": errors })))
}

fn batch_delete_partition(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let t = find_table(g, p)?;
    let mut errors: Vec<Value> = Vec::new();
    for want in p["PartitionsToDelete"].as_array().unwrap_or(&Vec::new()) {
        let values = values_list(&want["Values"]);
        let vals: Vec<Value> = values.iter().map(|v| json!(v)).collect();
        if t.rec.partitions.remove(&partition_key(&values)).is_none() {
            errors.push(json!({
                "PartitionValues": vals,
                "ErrorDetail": {
                    "ErrorCode": "EntityNotFoundException",
                    "ErrorMessage": "Partition not found.",
                },
            }));
        }
    }
    Ok(ok(&json!({ "Errors": errors })))
}

fn batch_get_partition(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let t = find_table(g, p)?;
    let mut list: Vec<Value> = Vec::new();
    for want in p["PartitionsToGet"].as_array().unwrap_or(&Vec::new()) {
        let values = values_list(&want["Values"]);
        if let Some(part) = t.rec.partitions.get(&partition_key(&values)) {
            list.push(partition_as_dict(part, &t.db, &t.tbl));
        }
    }
    Ok(ok(&json!({ "Partitions": list })))
}

fn batch_update_partition(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let t = find_table(g, p)?;
    let mut errors: Vec<Value> = Vec::new();
    for entry in p["Entries"].as_array().unwrap_or(&Vec::new()) {
        let old_values = str_list(&entry["PartitionValueList"]);
        let input = entry["PartitionInput"].clone();
        let values = partition_input_values(&input);
        let old_key = partition_key(&old_values);
        let new_key = partition_key(&values);
        let old_vals: Vec<Value> = old_values.iter().map(|v| json!(v)).collect();
        if old_key == new_key {
            if !t.rec.partitions.contains_key(&old_key) {
                errors.push(json!({
                    "PartitionValueList": old_vals,
                    "ErrorDetail": {
                        "ErrorCode": "EntityNotFoundException",
                        "ErrorMessage": "Partition not found.",
                    },
                }));
                continue;
            }
        } else {
            if t.rec.partitions.remove(&old_key).is_none() {
                errors.push(json!({
                    "PartitionValueList": old_vals,
                    "ErrorDetail": {
                        "ErrorCode": "EntityNotFoundException",
                        "ErrorMessage": "Partition not found.",
                    },
                }));
                continue;
            }
            if t.rec.partitions.contains_key(&new_key) {
                errors.push(json!({
                    "PartitionValueList": old_vals,
                    "ErrorDetail": {
                        "ErrorCode": "AlreadyExistsException",
                        "ErrorMessage": "Partition already exists.",
                    },
                }));
                continue;
            }
        }
        t.rec.partitions.insert(
            new_key,
            PartitionRec {
                values,
                input,
                created: now_unix(),
            },
        );
    }
    Ok(ok(&json!({ "Errors": errors })))
}

// ---------------------------------------------------------------------------
// User-defined functions
// ---------------------------------------------------------------------------

fn create_user_defined_function(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let db = str_field(p, "DatabaseName");
    let mut input = p["FunctionInput"].clone();
    let name = str_field(&input, "FunctionName");
    let db_rec = find_database_mut(g, &db)?;
    if db_rec.udfs.contains_key(&name) {
        return Err(already_exists("UserDefinedFunction already exists."));
    }
    input["CreateTime"] = json!(now_unix());
    db_rec.udfs.insert(name, input);
    Ok(empty_ok())
}

fn get_user_defined_function(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let db = str_field(p, "DatabaseName");
    let name = str_field(p, "FunctionName");
    let db_rec = find_database_mut(g, &db)?;
    match db_rec.udfs.get(&name) {
        Some(f) => Ok(ok(&json!({"UserDefinedFunction": f}))),
        None => Err(not_found(&format!("Function {name} not found"))),
    }
}

/// Subset of fnmatch: `*` wildcard, other characters literal (moto uses
/// Python's fnmatch to filter by `Pattern`).
fn fnmatch(pattern: &str, name: &str) -> bool {
    match pattern.split_once('*') {
        None => name == pattern,
        Some((prefix, suffix)) => {
            name.starts_with(prefix)
                && name.len() >= prefix.len() + suffix.len()
                && name.ends_with(suffix)
        }
    }
}

fn get_user_defined_functions(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let db = str_field(p, "DatabaseName");
    let pattern = p["Pattern"].as_str().unwrap_or("*");
    let db_rec = find_database_mut(g, &db)?;
    let list: Vec<Value> = db_rec
        .udfs
        .iter()
        .filter(|(name, _)| pattern.is_empty() || pattern == "*" || fnmatch(pattern, name))
        .map(|(_, f)| f.clone())
        .collect();
    Ok(ok(&json!({"UserDefinedFunctions": list})))
}

fn update_user_defined_function(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let db = str_field(p, "DatabaseName");
    let name = str_field(p, "FunctionName");
    let function_input = p["FunctionInput"].clone();
    let db_rec = find_database_mut(g, &db)?;
    match db_rec.udfs.get_mut(&name) {
        Some(rec) => {
            *rec = function_input;
            Ok(empty_ok())
        }
        None => Err(not_found(&format!("Function {name} not found"))),
    }
}

fn delete_user_defined_function(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let db = str_field(p, "DatabaseName");
    let name = str_field(p, "FunctionName");
    let db_rec = find_database_mut(g, &db)?;
    if db_rec.udfs.remove(&name).is_none() {
        return Err(not_found(&format!("Function {name} not found")));
    }
    Ok(empty_ok())
}

// ---------------------------------------------------------------------------
// SearchTables / unfiltered metadata
// ---------------------------------------------------------------------------

fn search_tables(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let account = account_of(req);
    let db_filter: Option<String> = p["Filters"]
        .as_array()
        .and_then(|fs| {
            fs.iter()
                .find(|f| str_field(f, "Key") == "DatabaseName")
                .map(|f| str_field(f, "Value"))
        })
        .filter(|v| !v.is_empty());
    let mut list: Vec<Value> = Vec::new();
    for (db_name, db_rec) in g.databases.iter() {
        if db_filter.as_ref().map_or(false, |f| f != db_name) {
            continue;
        }
        for (tbl_name, rec) in db_rec.tables.iter() {
            list.push(table_version_dict(rec, db_name, tbl_name, &account, None));
        }
    }
    ok(&json!({ "TableList": list }))
}

fn get_unfiltered_table_metadata(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let account = account_of(req);
    let t = try_resp!(find_table(g, p));
    ok(&json!({
        "Table": table_version_dict(t.rec, &t.db, &t.tbl, &account, None),
        "AuthorizedColumns": [],
        "IsRegisteredWithLakeFormation": false,
    }))
}

fn get_unfiltered_partition_metadata(g: &mut GlueState, p: &Value) -> Response {
    let t = try_resp!(find_table(g, p));
    let key = partition_key(&str_list(&p["PartitionValues"]));
    match t.rec.partitions.get(&key) {
        Some(part) => {
            let db = t.db.clone();
            let tbl = t.tbl.clone();
            ok(&json!({
                "Partition": partition_as_dict(part, &db, &tbl),
                "AuthorizedColumns": [],
                "IsRegisteredWithLakeFormation": false,
            }))
        }
        None => not_found("Cannot find partition."),
    }
}

fn get_unfiltered_partitions_metadata(g: &mut GlueState, p: &Value) -> Response {
    let t = try_resp!(find_table(g, p));
    let db = t.db.clone();
    let tbl = t.tbl.clone();
    let list: Vec<Value> = t
        .rec
        .partitions
        .values()
        .map(|part| {
            json!({
                "Partition": partition_as_dict(part, &db, &tbl),
                "AuthorizedColumns": [],
                "IsRegisteredWithLakeFormation": false,
            })
        })
        .collect();
    ok(&json!({ "UnfilteredPartitions": list }))
}

// ---------------------------------------------------------------------------
// Column statistics (per table)
// ---------------------------------------------------------------------------

fn update_column_stats_for_table(g: &mut GlueState, p: &Value) -> Response {
    let t = try_resp!(find_table(g, p));
    for stat in p["ColumnStatisticsList"].as_array().unwrap_or(&Vec::new()) {
        let name = str_field(stat, "ColumnName");
        if name.is_empty() {
            continue;
        }
        t.rec.column_stats.insert(name, stat.clone());
    }
    ok(&json!({ "Errors": [] }))
}

fn get_column_statistics_for_table(g: &mut GlueState, p: &Value) -> Response {
    let t = try_resp!(find_table(g, p));
    let stats: Vec<Value> = str_list(&p["ColumnNames"])
        .iter()
        .filter_map(|c| t.rec.column_stats.get(c).cloned())
        .collect();
    ok(&json!({ "ColumnStatisticsList": stats, "Errors": [] }))
}

// ---------------------------------------------------------------------------
// Catalogs
// ---------------------------------------------------------------------------

fn catalog_as_dict(name: &str, input: &Value, created: f64, updated: f64, req: &Request) -> Value {
    let mut obj = json!({
        "CatalogId": name,
        "Name": name,
        "ResourceArn": arn_of(req, "catalog", name),
        "CreateTime": created,
        "UpdateTime": updated,
        "Description": input["Description"],
        "Parameters": input["Parameters"],
    });
    obj.as_object_mut().unwrap().retain(|_, v| !v.is_null());
    obj
}

fn create_catalog(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    if g.catalogs.contains_key(&name) {
        return already_exists("Catalog already exists.");
    }
    let input = p["CatalogInput"].as_object().cloned().unwrap_or_default();
    let now = now_unix();
    g.catalogs.insert(name, (Value::Object(input), now, now));
    empty_ok()
}

fn get_catalog(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "CatalogId");
    match g.catalogs.get(&name) {
        Some((input, created, updated)) => ok(&json!({
            "Catalog": catalog_as_dict(&name, input, *created, *updated, req),
        })),
        None => not_found(&format!("Catalog {name} not found.")),
    }
}

fn update_catalog(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "CatalogId");
    let input = p["CatalogInput"].as_object().cloned().unwrap_or_default();
    match g.catalogs.get_mut(&name) {
        Some(rec) => {
            rec.0 = Value::Object(input);
            rec.2 = now_unix();
            empty_ok()
        }
        None => not_found(&format!("Catalog {name} not found.")),
    }
}

fn delete_catalog(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "CatalogId");
    if g.catalogs.remove(&name).is_none() {
        return not_found(&format!("Catalog {name} not found."));
    }
    empty_ok()
}

fn get_catalogs(req: &Request, g: &mut GlueState) -> Response {
    let mut list = Vec::new();
    for (name, (input, created, updated)) in g.catalogs.iter() {
        list.push(catalog_as_dict(name, input, *created, *updated, req));
    }
    ok(&json!({ "CatalogList": list }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "glue".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Fresh scope key so parallel tests don't share state.
    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn d(acct: u64, op: &str, body_val: Value) -> Response {
        let mut r = req(op, body_val);
        r.account = acct;
        dispatch(&r).unwrap()
    }

    fn err_code(resp: &Response) -> String {
        body(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    const SD: &str = r#"{"Columns":[{"Name":"col1","Type":"string"}],"Location":"s3://b/p"}"#;

    #[test]
    fn database_crud() {
        let acct = scope();
        let r = d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "db1", "Description": "d"}}),
        );
        assert_eq!(r.status, 200);

        let b = body(&d(acct, "GetDatabase", json!({"Name": "db1"})));
        assert_eq!(b["Database"]["Name"], "db1");
        assert_eq!(b["Database"]["Description"], "d");
        assert_eq!(b["Database"]["CatalogId"], acct.to_string());

        let dup = d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "db1"}}),
        );
        assert_eq!(err_code(&dup), "AlreadyExistsException");

        let b = body(&d(acct, "GetDatabases", json!({})));
        assert!(b["DatabaseList"].as_array().unwrap().len() == 1);

        assert_eq!(
            err_code(&d(acct, "GetDatabase", json!({"Name": "nope-dbx"}))),
            "EntityNotFoundException"
        );

        d(acct, "DeleteDatabase", json!({"Name": "db1"}));
        assert_eq!(
            err_code(&d(acct, "GetDatabase", json!({"Name": "db1"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(acct, "DeleteDatabase", json!({"Name": "db1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn database_update_and_null_dropping() {
        let acct = scope();
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "dbp", "Description": "orig"}}),
        );
        d(
            acct,
            "UpdateDatabase",
            json!({"Name": "dbp", "DatabaseInput": {"Name": "dbp", "Description": "updated"}}),
        );
        let b = body(&d(acct, "GetDatabase", json!({"Name": "dbp"})));
        assert_eq!(b["Database"]["Description"], "updated");
        // Null members (LocationUri etc.) are dropped on the wire.
        assert!(b["Database"].get("LocationUri").is_none());

        d(
            acct,
            "UpdateDatabase",
            json!({"Name": "zzz", "DatabaseInput": {}}),
        );
        let b = body(&d(acct, "GetDatabase", json!({"Name": "zzz"})));
        assert_eq!(b["__type"], "EntityNotFoundException");
    }

    fn make_db_and_table(account_key: &str) {
        let _ = account_key;
    }

    #[test]
    fn table_crud_and_versions() {
        let acct = scope();
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "tdb"}}),
        );
        let r = d(
            acct,
            "CreateTable",
            json!({
                "DatabaseName": "tdb",
                "TableInput": {"Name": "t1", "StorageDescriptor": serde_json::from_str::<Value>(SD).unwrap()}
            }),
        );
        assert_eq!(r.status, 200);

        let b = body(&d(
            acct,
            "GetTable",
            json!({"DatabaseName": "tdb", "Name": "t1"}),
        ));
        assert_eq!(b["Table"]["Name"], "t1");
        assert_eq!(b["Table"]["VersionId"], "1");
        assert_eq!(b["Table"]["StorageDescriptor"]["Location"], "s3://b/p");

        // Duplicate table fails
        assert_eq!(
            err_code(&d(
                acct,
                "CreateTable",
                json!({"DatabaseName": "tdb", "TableInput": {"Name": "t1"}})
            )),
            "AlreadyExistsException"
        );

        // Update bumps version
        d(
            acct,
            "UpdateTable",
            json!({"DatabaseName": "tdb", "TableInput": {"Name": "t1", "StorageDescriptor": {"Location": "s3://b/p2"}}}),
        );
        let b = body(&d(
            acct,
            "GetTableVersions",
            json!({"DatabaseName": "tdb", "TableName": "t1"}),
        ));
        assert_eq!(b["TableVersions"].as_array().unwrap().len(), 2);
        assert_eq!(b["TableVersions"][0]["VersionId"], "1");
        assert_eq!(b["TableVersions"][1]["VersionId"], "2");

        let b = body(&d(
            acct,
            "GetTableVersion",
            json!({"DatabaseName": "tdb", "TableName": "t1", "VersionId": "2"}),
        ));
        assert_eq!(b["TableVersion"]["VersionId"], "2");
        assert_eq!(
            b["TableVersion"]["Table"]["StorageDescriptor"]["Location"],
            "s3://b/p2"
        );

        // Missing version id
        assert_eq!(
            err_code(&d(
                acct,
                "GetTableVersion",
                json!({"DatabaseName": "tdb", "TableName": "t1", "VersionId": "9"}),
            )),
            "EntityNotFoundException"
        );
        // Non-numeric version
        assert_eq!(
            err_code(&d(
                acct,
                "GetTableVersion",
                json!({"DatabaseName": "tdb", "TableName": "t1", "VersionId": "x"}),
            )),
            "InvalidInputException"
        );

        // get_tables with expression
        d(
            acct,
            "CreateTable",
            json!({"DatabaseName": "tdb", "TableInput": {"Name": "t2_like"}}),
        );
        let b = body(&d(
            acct,
            "GetTables",
            json!({"DatabaseName": "tdb", "Expression": "t1*"}),
        ));
        let names: Vec<&str> = b["TableList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["t1"]);

        d(
            acct,
            "DeleteTable",
            json!({"DatabaseName": "tdb", "Name": "t1"}),
        );
        assert_eq!(
            err_code(&d(
                acct,
                "GetTable",
                json!({"DatabaseName": "tdb", "Name": "t1"}),
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn partition_ops() {
        let acct = scope();
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "pd"}}),
        );
        d(
            acct,
            "CreateTable",
            json!({
                "DatabaseName": "pd",
                "TableInput": {
                    "Name": "pt",
                    "PartitionKeys": [{"Name": "year", "Type": "string"}]
                }
            }),
        );
        d(
            acct,
            "CreatePartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionInput": {"Values": ["2024"], "StorageDescriptor": {"Location": "s3://b/p/y=2024"}}
            }),
        );
        let dup = d(
            acct,
            "CreatePartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionInput": {"Values": ["2024"]}
            }),
        );
        assert_eq!(err_code(&dup), "AlreadyExistsException");

        let b = body(&d(
            acct,
            "GetPartition",
            json!({"DatabaseName": "pd", "TableName": "pt", "PartitionValues": ["2024"]}),
        ));
        assert_eq!(b["Partition"]["Values"][0], "2024");
        assert_eq!(
            b["Partition"]["StorageDescriptor"]["Location"],
            "s3://b/p/y=2024"
        );

        let b = body(&d(
            acct,
            "GetPartitions",
            json!({"DatabaseName": "pd", "TableName": "pt"}),
        ));
        assert_eq!(b["Partitions"].as_array().unwrap().len(), 1);

        // batch_get_partition misses the "2025" one
        let b = body(&d(
            acct,
            "BatchGetPartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionsToGet": [{"Values": ["2024"]}, {"Values": ["2025"]}]
            }),
        ));
        assert_eq!(b["Partitions"].as_array().unwrap().len(), 1);

        // update in place
        d(
            acct,
            "UpdatePartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionValueList": ["2024"],
                "PartitionInput": {"Values": ["2024"], "StorageDescriptor": {"Location": "s3://u/2024"}}
            }),
        );
        let b = body(&d(
            acct,
            "GetPartition",
            json!({"DatabaseName": "pd", "TableName": "pt", "PartitionValues": ["2024"]}),
        ));
        assert_eq!(
            b["Partition"]["StorageDescriptor"]["Location"],
            "s3://u/2024"
        );

        // batch create + delete
        let b = body(&d(
            acct,
            "BatchCreatePartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionInputList": [{"Values": ["2023"]}, {"Values": ["2024"]}]
            }),
        ));
        assert!(b["Errors"]
            .as_array()
            .map(|e| e.len() == 1)
            .unwrap_or(false)); // 2024 dup

        let b = body(&d(
            acct,
            "BatchDeletePartition",
            json!({
                "DatabaseName": "pd", "TableName": "pt",
                "PartitionsToDelete": [{"Values": ["2023"]}, {"Values": ["2025"]}]
            }),
        ));
        assert_eq!(b["Errors"].as_array().unwrap().len(), 1);
        assert_eq!(
            b["Errors"][0]["ErrorDetail"]["ErrorCode"],
            "EntityNotFoundException"
        );

        d(
            acct,
            "DeletePartition",
            json!({"DatabaseName": "pd", "TableName": "pt", "PartitionValues": ["2024"]}),
        );
        assert_eq!(
            err_code(&d(
                acct,
                "GetPartition",
                json!({"DatabaseName": "pd", "TableName": "pt", "PartitionValues": ["2024"]}),
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn partition_move_and_update() {
        let acct = scope();
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "pm"}}),
        );
        d(
            acct,
            "CreateTable",
            json!({"DatabaseName": "pm", "TableInput": {"Name": "pt", "PartitionKeys": [{"Name": "dt", "Type": "string"}]}}),
        );
        d(
            acct,
            "CreatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionInput": {"Values": ["a"]}}),
        );
        d(
            acct,
            "CreatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionInput": {"Values": ["b"]}}),
        );
        // Update to overwrite an existing partition → AlreadyExists
        let r = d(
            acct,
            "UpdatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionValueList": ["a"], "PartitionInput": {"Values": ["b"]}}),
        );
        assert_eq!(err_code(&r), "AlreadyExistsException");
        // moto pops the old partition before noticing the collision, so "a"
        // is gone until recreated.
        d(
            acct,
            "CreatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionInput": {"Values": ["a"]}}),
        );
        // Update missing â EntityNotFound → EntityNotFound
        let r = d(
            acct,
            "UpdatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionValueList": ["zz"], "PartitionInput": {"Values": ["c"]}}),
        );
        assert_eq!(err_code(&r), "EntityNotFoundException");
        // Batch update in place
        let b = body(&d(
            acct,
            "BatchUpdatePartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "Entries": [
                {"PartitionValueList": ["a"], "PartitionInput": {"Values": ["a"], "StorageDescriptor": {"Location": "s3://x"}}}
            ]}),
        ));
        assert!(b["Errors"].as_array().unwrap().is_empty());
        let b = body(&d(
            acct,
            "GetPartition",
            json!({"DatabaseName": "pm", "TableName": "pt", "PartitionValues": ["a"]}),
        ));
        assert_eq!(b["Partition"]["StorageDescriptor"]["Location"], "s3://x");
    }

    #[test]
    fn table_missing_db_error_precedence() {
        let acct = scope();
        // Missing database wins over missing table
        assert_eq!(
            err_code(&d(
                acct,
                "GetTable",
                json!({"DatabaseName": "nope-db", "Name": "nope-tbl"}),
            )),
            "EntityNotFoundException"
        );
        // TableInput missing entirely → InvalidInputException
        let r = d(acct, "CreateTable", json!({"DatabaseName": "nope-db"}));
        assert_eq!(err_code(&r), "InvalidInputException");
    }

    #[test]
    fn udf_lifecycle_and_search() {
        let acct = scope();
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "udb"}}),
        );
        let r = d(
            acct,
            "CreateUserDefinedFunction",
            json!({
                "DatabaseName": "udb",
                "FunctionInput": {"FunctionName": "f1", "ClassName": "C1", "OwnerType": "USER"}
            }),
        );
        assert_eq!(r.status, 200);
        let b = body(&d(
            acct,
            "GetUserDefinedFunction",
            json!({"DatabaseName": "udb", "FunctionName": "f1"}),
        ));
        assert_eq!(b["UserDefinedFunction"]["ClassName"], "C1");

        d(
            acct,
            "UpdateUserDefinedFunction",
            json!({
                "DatabaseName": "udb", "FunctionName": "f1",
                "FunctionInput": {"FunctionName": "f1", "ClassName": "C2"}
            }),
        );
        let b = body(&d(
            acct,
            "GetUserDefinedFunctions",
            json!({"DatabaseName": "udb", "Pattern": "*"}),
        ));
        assert_eq!(b["UserDefinedFunctions"][0]["ClassName"], "C2");

        d(
            acct,
            "DeleteUserDefinedFunction",
            json!({"DatabaseName": "udb", "FunctionName": "f1"}),
        );
        assert_eq!(
            err_code(&d(
                acct,
                "GetUserDefinedFunction",
                json!({"DatabaseName": "udb", "FunctionName": "f1"}),
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn catalog_lifecycle_and_column_stats() {
        let acct = scope();
        d(
            acct,
            "CreateCatalog",
            json!({"Name": "cat1", "CatalogInput": {"Description": "d"}}),
        );
        let b = body(&d(acct, "GetCatalog", json!({"CatalogId": "cat1"})));
        assert_eq!(b["Catalog"]["CatalogId"], "cat1");
        assert_eq!(b["Catalog"]["Description"], "d");

        d(
            acct,
            "UpdateCatalog",
            json!({"CatalogId": "cat1", "CatalogInput": {"Description": "u"}}),
        );
        let b = body(&d(acct, "GetCatalog", json!({"CatalogId": "cat1"})));
        assert_eq!(b["Catalog"]["Description"], "u");

        let r = d(
            acct,
            "UpdateCatalog",
            json!({"CatalogId": "zz", "CatalogInput": {}}),
        );
        assert_eq!(err_code(&r), "EntityNotFoundException");

        let b = body(&d(acct, "GetCatalogs", json!({})));
        assert_eq!(b["CatalogList"].as_array().unwrap().len(), 1);

        // Create duplicate
        let r = d(
            acct,
            "CreateCatalog",
            json!({"Name": "cat1", "CatalogInput": {}}),
        );
        assert_eq!(err_code(&r), "AlreadyExistsException");

        // Column statistics for table
        d(
            acct,
            "CreateDatabase",
            json!({"DatabaseInput": {"Name": "csdb"}}),
        );
        d(
            acct,
            "CreateTable",
            json!({"DatabaseName": "csdb", "TableInput": {"Name": "cst"}}),
        );
        d(
            acct,
            "UpdateColumnStatisticsForTable",
            json!({
                "DatabaseName": "csdb", "TableName": "cst",
                "ColumnStatisticsList": [{"ColumnName": "c1", "ColumnType": "string"}]
            }),
        );
        let b = body(&d(
            acct,
            "GetColumnStatisticsForTable",
            json!({"DatabaseName": "csdb", "TableName": "cst", "ColumnNames": ["c1"]}),
        ));
        assert_eq!(b["ColumnStatisticsList"][0]["ColumnName"], "c1");
        // unknown table → EntityNotFound
        let r = d(
            acct,
            "GetColumnStatisticsForTable",
            json!({"DatabaseName": "csdb", "TableName": "nope", "ColumnNames": ["c1"]}),
        );
        assert_eq!(err_code(&r), "EntityNotFoundException");

        d(acct, "DeleteCatalog", json!({"CatalogId": "cat1"}));
        assert_eq!(
            err_code(&d(acct, "GetCatalog", json!({"CatalogId": "cat1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn unimplemented_ops_answer_501() {
        let acct = scope();
        let r = d(acct, "BogusOp", json!({}));
        assert_eq!(r.status, 501);
        let b = body(&r);
        assert_eq!(b["__type"], "NotImplemented");
    }
}
// ---------------------------------------------------------------------------
// Crawlers
// ---------------------------------------------------------------------------

fn crawler_as_dict(rec: &CrawlerRec) -> Value {
    let mut obj = json!({
        "Name": rec.name,
        "Role": rec.role,
        "Targets": rec.targets,
        "DatabaseName": rec.database_name,
        "Description": rec.description,
        "Classifiers": rec.classifiers,
        "RecrawlPolicy": rec.recrawl_policy,
        "SchemaChangePolicy": rec.schema_change_policy,
        "LineageConfiguration": rec.lineage_configuration,
        "State": crawler_status(rec),
        "TablePrefix": rec.table_prefix,
        "CrawlElapsedTime": 0,
        "CreationTime": rec.created,
        "LastUpdated": rec.created,
        "Version": 1,
        "Configuration": rec.configuration,
        "CrawlerSecurityConfiguration": rec.crawler_security_configuration,
    });
    let base = obj.as_object_mut().unwrap();
    if rec.schedule.as_str().map_or(false, |s| !s.is_empty()) {
        base.insert(
            "Schedule".into(),
            json!({"ScheduleExpression": rec.schedule, "State": "SCHEDULED"}),
        );
    }
    if let Some(last) = rec.crawls.last() {
        base.insert(
            "LastCrawl".into(),
            json!({
                "LogGroup": "/aws-glue/crawlers",
                "LogStream": rec.name,
                "MessagePrefix": last.crawl_id,
                "StartTime": rec.created,
                "Status": last.status,
            }),
        );
    }
    obj
}

/// moto's `FakeCrawler.status`: driven by the latest crawl record only.
fn crawler_status(rec: &CrawlerRec) -> String {
    match rec.crawls.last().map(|c| c.status.as_str()) {
        Some("RUNNING") => "RUNNING".to_string(),
        Some("STOPPING") => "STOPPING".to_string(),
        _ => "READY".to_string(),
    }
}

fn find_crawler_mut<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut CrawlerRec, Response> {
    let name = str_field(p, "Name");
    g.crawlers
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Crawler {name} not found.")))
}

fn find_crawler<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut CrawlerRec, Response> {
    let name = str_field(p, "Name");
    g.crawlers
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Crawler {name} not found.")))
}

fn create_crawler(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    if g.crawlers.contains_key(&name) {
        return already_exists("Crawler already exists.");
    }
    let rec = CrawlerRec {
        name: name.clone(),
        role: p["Role"].clone(),
        database_name: p["DatabaseName"].clone(),
        description: p["Description"].clone(),
        targets: p["Targets"].clone(),
        schedule: p["Schedule"].clone(),
        classifiers: p["Classifiers"].clone(),
        table_prefix: p["TablePrefix"].clone(),
        schema_change_policy: p["SchemaChangePolicy"].clone(),
        recrawl_policy: p["RecrawlPolicy"].clone(),
        lineage_configuration: p["LineageConfiguration"].clone(),
        configuration: p["Configuration"].clone(),
        crawler_security_configuration: p["CrawlerSecurityConfiguration"].clone(),
        created: now_unix(),
        crawls: Vec::new(),
    };
    let arn = arn_of(req, "crawler", &name);
    tag_resource(g, &arn, p.get("Tags"));
    g.crawlers.insert(name, rec);
    empty_ok()
}

fn get_crawler(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_crawler(g, p));
    ok(&json!({ "Crawler": crawler_as_dict(rec) }))
}

fn get_crawlers(g: &mut GlueState) -> Response {
    let list: Vec<Value> = g.crawlers.values().map(crawler_as_dict).collect();
    ok(&json!({ "Crawlers": list }))
}

fn list_crawlers(g: &mut GlueState) -> Response {
    let names: Vec<Value> = g.crawlers.keys().map(|k| json!(k)).collect();
    ok(&json!({ "CrawlerNames": names }))
}

fn delete_crawler(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    if g.crawlers.remove(&name).is_none() {
        return not_found(&format!("Crawler {name} not found."));
    }
    empty_ok()
}

fn start_crawler(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_crawler(g, p));
    if rec.crawls.last().map_or(false, |c| c.status == "RUNNING") {
        return err(
            400,
            "CrawlerRunningException",
            &format!("Crawler with name {} has already started", rec.name),
        );
    }
    rec.crawls.push(CrawlRec {
        crawl_id: uuid4(),
        status: "RUNNING".to_string(),
    });
    empty_ok()
}

fn stop_crawler(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_crawler(g, p));
    let running = matches!(
        rec.crawls.last().map(|c| c.status.as_str()),
        Some("RUNNING")
    );
    if !running {
        return err(
            400,
            "CrawlerNotRunningException",
            &format!("Crawler with name {} isn't running", rec.name),
        );
    }
    rec.crawls.last_mut().unwrap().status = "STOPPING".to_string();
    empty_ok()
}

fn update_crawler(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_crawler(g, p));
    for member in [
        "Role",
        "DatabaseName",
        "Description",
        "Targets",
        "Schedule",
        "Classifiers",
        "TablePrefix",
        "SchemaChangePolicy",
        "RecrawlPolicy",
        "LineageConfiguration",
        "Configuration",
        "CrawlerSecurityConfiguration",
    ] {
        if let Some(v) = p.get(member) {
            match member {
                "Role" => rec.role = v.clone(),
                "DatabaseName" => rec.database_name = v.clone(),
                "Description" => rec.description = v.clone(),
                "Targets" => rec.targets = v.clone(),
                "Schedule" => rec.schedule = v.clone(),
                "Classifiers" => rec.classifiers = v.clone(),
                "TablePrefix" => rec.table_prefix = v.clone(),
                "SchemaChangePolicy" => rec.schema_change_policy = v.clone(),
                "RecrawlPolicy" => rec.recrawl_policy = v.clone(),
                "LineageConfiguration" => rec.lineage_configuration = v.clone(),
                "Configuration" => rec.configuration = v.clone(),
                "CrawlerSecurityConfiguration" => rec.crawler_security_configuration = v.clone(),
                _ => {}
            }
        }
    }
    empty_ok()
}

fn update_crawler_schedule(g: &mut GlueState, p: &Value) -> Response {
    // moto: `update_crawler_schedule` through the CrawlerName member.
    let name = str_field(p, "CrawlerName");
    let rec = g
        .crawlers
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Crawler {name} not found.")));
    let rec = try_resp!(rec);
    if let Some(v) = p.get("Schedule") {
        rec.schedule = v.clone();
    }
    empty_ok()
}

fn crawler_schedule_stub(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "CrawlerName");
    match g.crawlers.get(&name) {
        Some(_) => empty_ok(),
        None => not_found(&format!("Crawler {name} not found.")),
    }
}

fn batch_get_crawlers(g: &mut GlueState, p: &Value) -> Response {
    let wanted = str_list(&p["CrawlerNames"]);
    let mut found = Vec::new();
    let watch: Vec<String> = wanted.clone();
    for (name, rec) in g.crawlers.iter() {
        if watch.contains(name) {
            found.push(crawler_as_dict(rec));
        }
    }
    let found_names: Vec<String> = found.iter().map(|c| str_field(c, "Name")).collect();
    let missing: Vec<Value> = wanted
        .iter()
        .filter(|n| !found_names.contains(n))
        .map(|n| json!(n))
        .collect();
    ok(&json!({ "Crawlers": found, "CrawlersNotFound": missing }))
}

fn get_crawler_metrics(g: &mut GlueState, p: &Value) -> Response {
    let wanted = p["CrawlerNameList"]
        .as_array()
        .map(|a| str_list(&p["CrawlerNameList"]));
    let mut list = Vec::new();
    for name in g.crawlers.keys() {
        if let Some(wanted) = &wanted {
            if !wanted.contains(name) {
                continue;
            }
        }
        list.push(json!({
            "CrawlerName": name,
            "TimeLeftSeconds": 0.0,
            "StillEstimating": false,
            "LastRuntimeSeconds": 0.0,
            "MedianRuntimeSeconds": 0.0,
            "TablesCreated": 0,
            "TablesUpdated": 0,
            "TablesDeleted": 0,
        }));
    }
    ok(&json!({ "CrawlerMetricsList": list }))
}

fn list_crawls(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "CrawlerName");
    match g.crawlers.get(&name) {
        None => not_found(&format!("Crawler entry with name {name} does not exist")),
        Some(rec) => {
            let list: Vec<Value> = rec
                .crawls
                .iter()
                .map(|c| {
                    json!({
                        "CrawlId": c.crawl_id,
                        "DPUHour": 100.0,
                        "LogGroup": "/aws-glue/crawlers",
                        "LogStream": rec.name,
                        "MessagePrefix": c.crawl_id,
                        "StartTime": rec.created,
                        "State": "RUNNING",
                    })
                })
                .collect();
            ok(&json!({ "Crawls": list }))
        }
    }
}
// ---------------------------------------------------------------------------
// Jobs + job runs + bookmarks
// ---------------------------------------------------------------------------

fn job_as_dict(rec: &JobRec) -> Value {
    let mut obj = json!({
        "Name": rec.job["Name"],
        "CreatedOn": rec.created,
        "LastModifiedOn": rec.modified,
        "ExecutionProperty": rec.job["ExecutionProperty"],
        "Command": rec.job["Command"],
        "DefaultArguments": rec.job["DefaultArguments"],
    });
    let base = obj.as_object_mut().unwrap();
    for member in [
        "Description",
        "LogUri",
        "Role",
        "NonOverridableArguments",
        "Connections",
        "MaxRetries",
        "AllocatedCapacity",
        "Timeout",
        "MaxCapacity",
        "WorkerType",
        "NumberOfWorkers",
        "SecurityConfiguration",
        "NotificationProperty",
        "GlueVersion",
        "CodeGenConfigurationNodes",
        "ExecutionClass",
        "SourceControlDetails",
    ] {
        if !rec.job[member].is_null() {
            base.insert(member.to_string(), nulls_dropped(&rec.job[member]));
        }
    }
    for opt in ["ExecutionProperty", "Command", "DefaultArguments"] {
        if base.get(opt).map_or(true, |v| v.is_null()) {
            base.remove(opt);
        }
    }
    obj
}

fn find_job_mut<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut JobRec, Response> {
    let name = str_field(p, "JobName");
    g.jobs
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Job {name} not found.")))
}

fn create_job(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    let mut job = Map::new();
    for member in [
        "Name",
        "Description",
        "LogUri",
        "Role",
        "ExecutionProperty",
        "Command",
        "DefaultArguments",
        "NonOverridableArguments",
        "Connections",
        "MaxRetries",
        "AllocatedCapacity",
        "Timeout",
        "MaxCapacity",
        "SecurityConfiguration",
        "NotificationProperty",
        "GlueVersion",
        "NumberOfWorkers",
        "WorkerType",
        "CodeGenConfigurationNodes",
        "ExecutionClass",
        "SourceControlDetails",
    ] {
        if let Some(v) = p.get(member) {
            job.insert(member.to_string(), v.clone());
        }
    }
    let arn = arn_of(req, "job", &name);
    tag_resource(g, &arn, p.get("Tags"));
    g.jobs.insert(
        name.clone(),
        JobRec {
            job: Value::Object(job),
            created: now_unix(),
            modified: now_unix(),
            runs: Vec::new(),
        },
    );
    ok(&json!({ "Name": name }))
}

fn get_job(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_job_mut(g, p));
    ok(&json!({ "Job": job_as_dict(rec) }))
}

fn get_jobs(g: &mut GlueState) -> Response {
    let list: Vec<Value> = g.jobs.values().map(job_as_dict).collect();
    ok(&json!({ "Jobs": list }))
}

fn list_jobs(g: &mut GlueState) -> Response {
    // tag filtering (ListJobs Tags filter) is not exercised by the suites.
    let names: Vec<Value> = g.jobs.keys().map(|k| json!(k)).collect();
    ok(&json!({ "JobNames": names }))
}

fn update_job(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "JobName");
    let job_update = p["JobUpdate"].clone();
    let rec = try_resp!(find_job_mut(g, p));
    let mut job = rec.job.as_object().cloned().unwrap_or_default();
    if let Some(u) = job_update.as_object() {
        for (k, v) in u {
            job.insert(k.clone(), nulls_dropped(v));
        }
    }
    rec.job = Value::Object(job);
    rec.modified = now_unix();
    ok(&json!({ "JobName": name }))
}

fn delete_job(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "JobName");
    g.jobs.remove(&name);
    ok(&json!({ "JobName": name }))
}

fn create_job_run_from(rec: &JobRec, p: &Value) -> JobRunRec {
    JobRunRec {
        id: format!("jr_{}", hex_n(64)),
        previous_run_id: p["JobRunId"].as_str().map(str::to_string),
        status: "SUCCEEDED".to_string(), // moto 'immediate' transitions
        arguments: {
            let mut args = Map::new();
            for v in [rec.job["DefaultArguments"].clone(), p["Arguments"].clone()] {
                if let Some(m) = v.as_object() {
                    for (k, val) in m {
                        args.insert(k.clone(), val.clone());
                    }
                }
            }
            let _ = &rec;
            if args.is_empty() {
                None
            } else {
                Some(Value::Object(args))
            }
        },
        allocated_capacity: option_from_job_field(&rec.job, "AllocatedCapacity")
            .or_else(|| p["AllocatedCapacity"].as_u64().map(Value::from)),
        max_capacity: option_from_job_field(&rec.job, "MaxCapacity")
            .or_else(|| p["MaxCapacity"].as_f64().map(Value::from)),
        timeout: option_from_job_field(&rec.job, "Timeout")
            .or_else(|| p["Timeout"].as_u64().map(Value::from)),
        worker_type: option_from_job_field(&rec.job, "WorkerType").or_else(|| {
            p["WorkerType"]
                .as_str()
                .map(str::to_string)
                .map(Value::from)
        }),
        number_of_workers: option_from_job_field(&rec.job, "NumberOfWorkers")
            .or_else(|| p["NumberOfWorkers"].as_u64().map(Value::from)),
        security_configuration: p
            .get("SecurityConfiguration")
            .filter(|v| !v.is_null())
            .cloned()
            .or_else(|| option_from_job_field(&rec.job, "SecurityConfiguration")),
        notification_property: option_from_job_field(&p, "NotificationProperty")
            .or_else(|| option_from_job_field(&rec.job, "NotificationProperty")),
    }
}

/// Take a job-level member into an Option, flipping nulls to None.
fn option_from_job_field(job: &Value, member: &str) -> Option<Value> {
    match &job[member] {
        Value::Null => None,
        v => Some(v.clone()),
    }
}

fn job_run_as_dict(run: &JobRunRec, job_name: &str) -> Value {
    let mut obj = json!({
        "Id": run.id,
        "Attempt": 0,
        "JobName": job_name,
        "StartedOn": now_unix(),
        "LastModifiedOn": now_unix(),
        "CompletedOn": now_unix(),
        "JobRunState": run.status,
        "PredecessorRuns": [],
        "ExecutionTime": 123,
        "LogGroupName": "test/log",
        "GlueVersion": "0.9",
    });
    let base = obj.as_object_mut().unwrap();
    for (member, value) in [
        ("Arguments", run.arguments.clone()),
        ("AllocatedCapacity", run.allocated_capacity.clone()),
        ("MaxCapacity", run.max_capacity.clone()),
        ("NumberOfWorkers", run.number_of_workers.clone()),
        ("WorkerType", run.worker_type.clone()),
        ("NotificationProperty", run.notification_property.clone()),
        ("SecurityConfiguration", run.security_configuration.clone()),
        ("Timeout", run.timeout.clone()),
    ] {
        if let Some(v) = value {
            if !v.is_null() {
                base.insert(member.to_string(), nulls_dropped(&v));
            }
        }
    }
    if let Some(prev) = &run.previous_run_id {
        base.insert("PreviousRunId".into(), json!(prev));
    }
    obj
}

fn start_job_run(g: &mut GlueState, p: &Value) -> Response {
    let run = try_resp!(find_job_mut(g, p));
    let name = str_field(&run.job, "Name");
    let job_run = create_job_run_from(run, p);
    let id = job_run.id.clone();
    run.runs.push(job_run);
    let _ = name;
    ok(&json!({ "JobRunId": id }))
}

fn get_job_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_job_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    match rec.runs.iter().find(|r| r.id == run_id) {
        Some(run) => Ok(ok(&json!({
            "JobRun": job_run_as_dict(run, &run_id_job_name(&rec.job)),
        }))),
        None => Err(not_found(&format!("Job run {run_id} not found."))),
    }
}

fn run_id_job_name(job: &Value) -> String {
    str_field(job, "Name")
}

fn get_job_runs(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_job_mut(g, p)?;
    let job_name = str_field(&rec.job, "Name");
    let list: Vec<Value> = rec
        .runs
        .iter()
        .map(|r| job_run_as_dict(r, &job_name))
        .collect();
    Ok(ok(&json!({ "JobRuns": list })))
}

fn batch_get_jobs(g: &mut GlueState, p: &Value) -> Response {
    let wanted = str_list(&p["JobNames"]);
    let mut found = Vec::new();
    for name in wanted.iter() {
        if let Some(rec) = g.jobs.get(name) {
            found.push(rec.job.clone());
        }
    }
    let found_names: Vec<String> = found.iter().map(|j| str_field(j, "Name")).collect();
    let missing: Vec<Value> = wanted
        .iter()
        .filter(|n| !found_names.contains(n))
        .map(|n| json!(n))
        .collect();
    ok(&json!({ "Jobs": found, "JobsNotFound": missing }))
}

fn batch_stop_job_run(req: &Request, g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let job_name = str_field(p, "JobName");
    let rec = find_job_mut(g, p)?;
    let mut successful = Vec::new();
    let mut errors = Vec::new();
    if p["JobRunIds"].as_array().unwrap_or(&Vec::new()).is_empty() {
        let _ = req;
    }
    for run_id in str_list(&p["JobRunIds"]) {
        let hit = rec
            .runs
            .iter_mut()
            .find(|r| r.id == run_id)
            .map(|r| r.status.clone());
        match hit {
            Some(ref status)
                if ["STARTING", "RUNNING", "SUCCEEDED", "FAILED"].contains(&status.as_str()) =>
            {
                rec.runs.iter_mut().find(|r| r.id == run_id).unwrap().status =
                    "STOPPED".to_string();
                successful.push(json!({"JobName": job_name, "JobRunId": run_id}));
            }
            _ => errors.push(json!({
                "JobName": job_name,
                "JobRunId": run_id,
                "ErrorDetail": {
                    "ErrorCode": "EntityNotFoundException",
                    "ErrorMessage": format!("Job run {run_id} not found"),
                },
            })),
        }
    }
    Ok(ok(
        &json!({ "SuccessfulSubmissions": successful, "Errors": errors }),
    ))
}

fn job_bookmark(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let _ = find_job_mut(g, p)?;
    let name = str_field(p, "JobName");
    Ok(ok(&json!({
        "JobBookmarkEntry": {
            "JobName": name,
            "Version": 0,
            "Run": 0,
            "Attempt": 0,
            "JobBookmark": "",
        }
    })))
}

fn update_job_from_source_control(g: &mut GlueState, p: &Value) -> Response {
    let job_name = p["JobName"].as_str().map(str::to_string);
    if let Some(name) = &job_name {
        if !g.jobs.contains_key(name) {
            return not_found(&format!("Job {name} not found."));
        }
    }
    ok(&json!({ "JobName": job_name.unwrap_or_default() }))
}

fn update_source_control_from_job(g: &mut GlueState, p: &Value) -> Response {
    let job_name = p["JobName"].as_str().map(str::to_string);
    if let Some(name) = &job_name {
        if !g.jobs.contains_key(name) {
            return not_found(&format!("Job {name} not found."));
        }
    }
    ok(&json!({ "JobName": job_name.unwrap_or_default() }))
}
// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn get_tags(g: &mut GlueState, p: &Value) -> Response {
    let arn = str_field(p, "ResourceArn");
    ok(&json!({ "Tags": get_tag_dict(g, &arn) }))
}

fn tag_resource_op(g: &mut GlueState, p: &Value) -> Response {
    let arn = str_field(p, "ResourceArn");
    tag_resource(g, &arn, p.get("TagsToAdd"));
    empty_ok()
}

fn untag_resource_op(g: &mut GlueState, p: &Value) -> Response {
    let arn = str_field(p, "ResourceArn");
    if let Some(entry) = g.tags.get_mut(&arn) {
        for key in str_list(&p["TagsToRemove"]) {
            entry.remove(&key);
        }
    }
    empty_ok()
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

/// moto's `Action.as_dict`: only members the caller provided.
fn action_as_dict(a: &Value) -> Value {
    let mut obj = Map::new();
    for member in [
        "Arguments",
        "CrawlerName",
        "JobName",
        "NotificationProperty",
        "SecurityConfiguration",
        "Timeout",
    ] {
        if let Some(v) = a.get(member) {
            if !v.is_null() {
                obj.insert(member.to_string(), v.clone());
            }
        }
    }
    Value::Object(obj)
}

fn find_trigger_mut<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut TriggerRec, Response> {
    let name = str_field(p, "Name");
    g.triggers
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Trigger {name} not found.")))
}

fn trigger_as_dict(rec: &TriggerRec) -> Value {
    let mut obj = json!({
        "Name": rec.name,
        "Type": rec.trigger_type,
        "Actions": rec.actions.clone(),
        "State": if rec.start_on_creation { "ACTIVATED" } else if rec.state_activated() { "ACTIVATED" } else { "CREATED" },
    });
    let base = obj.as_object_mut().unwrap();
    if !rec.workflow_name.is_null() {
        base.insert("WorkflowName".into(), rec.workflow_name.clone());
    }
    if rec.trigger_type == "SCHEDULED" {
        base.insert("Schedule".into(), rec.schedule.clone());
    }
    if let Some(pred) = &rec.predicate {
        base.insert("Predicate".into(), pred.clone());
    }
    if let Some(Value::String(desc)) = Some(rec.description.clone()) {
        if !desc.is_empty() {
            base.insert("Description".into(), json!(desc));
        }
    }
    if !rec.event_batching_condition.is_null() {
        base.insert(
            "EventBatchingCondition".into(),
            rec.event_batching_condition.clone(),
        );
    }
    // Action dicts go through moto's Action.as_dict normalizer.
    if let Some(actions) = base.get_mut("Actions").and_then(Value::as_array_mut) {
        let normalized: Vec<Value> = actions
            .iter()
            .map(|a| {
                if a.is_object() && !a.as_object().unwrap().is_empty() {
                    action_as_dict(a)
                } else {
                    a.clone()
                }
            })
            .collect();
        *actions = normalized;
    }
    obj
}

impl TriggerRec {
    fn state_activated(&self) -> bool {
        false
    }
}

fn create_trigger(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    let trigger_type = str_field(p, "Type");
    if trigger_type == "CONDITIONAL" && p["Predicate"].is_null() {
        return invalid_input("Predicate cannot be null or empty");
    }
    let actions: Vec<Value> = p["Actions"]
        .as_array()
        .map(|a| a.iter().map(|v| action_as_dict(v)).collect())
        .unwrap_or_default();
    let rec = TriggerRec {
        name: name.clone(),
        workflow_name: p["WorkflowName"].clone(),
        trigger_type: trigger_type.clone(),
        schedule: p["Schedule"].clone(),
        predicate: p["Predicate"].as_object().map(|o| {
            let mut obj = Map::new();
            obj.insert(
                "Logical".into(),
                o.get("Logical").cloned().unwrap_or(json!("AND")),
            );
            obj.insert(
                "Conditions".into(),
                o.get("Conditions").cloned().unwrap_or(json!([])),
            );
            Value::Object(obj)
        }),
        actions,
        description: p["Description"].clone(),
        start_on_creation: p["StartOnCreation"].as_bool().unwrap_or(false),
        event_batching_condition: p["EventBatchingCondition"].clone(),
    };
    let arn = arn_of(req, "trigger", &name);
    tag_resource(g, &arn, p.get("Tags"));
    g.triggers.insert(name.clone(), rec);
    ok(&json!({ "Name": name }))
}

fn get_trigger(g: &mut GlueState, p: &Value) -> Response {
    let rec = try_resp!(find_trigger_mut(g, p));
    ok(&json!({ "Trigger": trigger_as_dict(rec) }))
}

fn triggers_by_dependency(g: &GlueState, dependent_job: Option<&str>) -> Vec<String> {
    match dependent_job {
        Some(job) => g
            .triggers
            .iter()
            .filter(|(_, t)| {
                t.actions
                    .iter()
                    .any(|a| a["JobName"].as_str().map_or(false, |jn| jn == job))
            })
            .map(|(n, _)| n.clone())
            .collect(),
        None => g.triggers.keys().cloned().collect(),
    }
}

fn get_triggers(g: &mut GlueState, p: &Value) -> Response {
    let dependent = p["DependentJobName"].as_str().map(str::to_string);
    let names = triggers_by_dependent(g, dependent.as_deref());
    let list: Vec<Value> = names
        .iter()
        .filter_map(|n| g.triggers.get(n).map(trigger_as_dict))
        .collect();
    ok(&json!({ "Triggers": list }))
}

fn triggers_by_dependent(g: &GlueState, dependent: Option<&str>) -> Vec<String> {
    match dependent {
        Some(job) => g
            .triggers
            .iter()
            .filter(|(_, t)| {
                t.actions
                    .iter()
                    .any(|a| a["JobName"].as_str().map_or(false, |jn| jn == job))
            })
            .map(|(n, _)| n.clone())
            .collect(),
        None => g.triggers.keys().cloned().collect(),
    }
}

fn list_triggers(g: &mut GlueState, p: &Value) -> Response {
    let dependent = p["DependentJobName"].as_str().map(str::to_string);
    let names = triggers_by_dependent(g, dependent.as_deref());
    let list: Vec<Value> = names.into_iter().map(|n| json!(n)).collect();
    ok(&json!({ "TriggerNames": list }))
}

fn batch_get_triggers(g: &mut GlueState, p: &Value) -> Response {
    let wanted = str_list(&p["TriggerNames"]);
    let mut found = Vec::new();
    for name in wanted.iter() {
        if let Some(rec) = g.triggers.get(name) {
            found.push(trigger_as_dict(rec));
        }
    }
    let found_names: Vec<String> = found.iter().map(|t| str_field(t, "Name")).collect();
    let missing: Vec<Value> = wanted
        .iter()
        .filter(|n| !found_names.contains(n))
        .map(|n| json!(n))
        .collect();
    ok(&json!({ "Triggers": found, "TriggersNotFound": missing }))
}

fn start_trigger(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_trigger_mut(g, p)?;
    rec.start_on_creation = true;
    let name = rec.name.clone();
    Ok(ok(&json!({ "Name": name })))
}

fn stop_trigger(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_trigger_mut(g, p)?;
    rec.start_on_creation = false;
    let name = rec.name.clone();
    Ok(ok(&json!({ "Name": name })))
}

fn update_trigger(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_trigger_mut(g, p)?;
    let update = p["TriggerUpdate"].clone();
    if let Some(u) = update.as_object() {
        if let Some(v) = u.get("Description") {
            rec.description = v.clone();
        }
        if let Some(v) = u.get("Schedule") {
            rec.schedule = v.clone();
        }
        if let Some(v) = u.get("Actions") {
            rec.actions = v
                .as_array()
                .map(|a| a.iter().map(action_as_dict).collect())
                .unwrap_or_default();
        }
        if let Some(v) = u.get("Predicate") {
            rec.predicate = v.as_object().map(|o| {
                let mut obj = Map::new();
                obj.insert(
                    "Logical".into(),
                    o.get("Logical").cloned().unwrap_or(json!("AND")),
                );
                obj.insert(
                    "Conditions".into(),
                    o.get("Conditions").cloned().unwrap_or(json!([])),
                );
                Value::Object(obj)
            });
        }
        if let Some(v) = u.get("EventBatchingCondition") {
            rec.event_batching_condition = v.clone();
        }
    }
    let name = rec.name.clone();
    Ok(ok(&json!({
        "Trigger": {
            "Name": name,
        }
    })))
}

fn delete_trigger(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.triggers.remove(&name).is_none() {
        return Err(not_found(&format!("Trigger {name} not found.")));
    }
    Ok(ok(&json!({ "Name": name })))
}

// ---------------------------------------------------------------------------
// Workflows + runs + run properties
// ---------------------------------------------------------------------------

fn find_workflow_mut<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut WorkflowRec, Response> {
    let name = str_field(p, "Name");
    g.workflows
        .get_mut(&name)
        .ok_or_else(|| not_found("Entity not found"))
}

fn workflow_as_dict(rec: &WorkflowRec) -> Value {
    let mut obj = json!({
        "CreatedOn": rec.created,
        "LastModifiedOn": rec.modified,
        "Name": rec.name,
    });
    let base = obj.as_object_mut().unwrap();
    if let Some(d) = rec.description.as_str() {
        if !d.is_empty() {
            base.insert("Description".into(), json!(d));
        }
    }
    if let Some(map) = rec.default_run_properties.as_object() {
        if !map.is_empty() {
            base.insert(
                "DefaultRunProperties".into(),
                rec.default_run_properties.clone(),
            );
        }
    }
    match rec.max_concurrent_runs.as_i64() {
        Some(n) if n > 0 => {
            base.insert("MaxConcurrentRuns".into(), json!(n));
        }
        _ => {}
    }
    if let Some(last) = rec.runs.last() {
        base.insert("LastRun".into(), workflow_run_as_dict(last, &rec.name));
    }
    obj
}

fn workflow_run_as_dict(run: &WorkflowRunRec, workflow_name: &str) -> Value {
    let mut obj = json!({
        "Name": workflow_name,
        "WorkflowRunId": run.run_id,
        "StartedOn": run.started,
        "Status": run.status,
    });
    let base = obj.as_object_mut().unwrap();
    if let Some(prev) = &run.previous_run_id {
        base.insert("PreviousRunId".into(), json!(prev));
    }
    if !run.properties.is_empty() {
        let mut map = Map::new();
        for (k, v) in run.properties.iter() {
            map.insert(k.clone(), json!(v));
        }
        base.insert("WorkflowRunProperties".into(), Value::Object(map));
    }
    obj
}

fn create_workflow(req: &Request, g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    let rec = WorkflowRec {
        name: name.clone(),
        default_run_properties: p["DefaultRunProperties"].clone(),
        description: p["Description"].clone(),
        max_concurrent_runs: p["MaxConcurrentRuns"].clone(),
        created: now_unix(),
        modified: now_unix(),
        runs: Vec::new(),
    };
    let arn = arn_of(req, "workflow", &name);
    tag_resource(g, &arn, p.get("Tags"));
    g.workflows.insert(name.clone(), rec);
    ok(&json!({ "Name": name }))
}

fn get_workflow(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    Ok(ok(&json!({ "Workflow": workflow_as_dict(rec) })))
}

fn list_workflows(g: &mut GlueState) -> Response {
    let names: Vec<Value> = g.workflows.keys().map(|k| json!(k)).collect();
    ok(&json!({ "Workflows": names }))
}

fn delete_workflow(g: &mut GlueState, p: &Value) -> Response {
    let name = str_field(p, "Name");
    g.workflows.remove(&name);
    ok(&json!({ "Name": name }))
}

fn update_workflow(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    let rec = find_workflow_mut(g, p)?;
    if let Some(v) = p.get("DefaultRunProperties") {
        if !v.is_null() {
            rec.default_run_properties = v.clone();
        }
    }
    if let Some(v) = p.get("Description") {
        if !v.is_null() {
            rec.description = v.clone();
        }
    }
    if let Some(v) = p.get("MaxConcurrentRuns") {
        if !v.is_null() {
            rec.max_concurrent_runs = v.clone();
        }
    }
    rec.modified = now_unix();
    Ok(ok(&json!({ "Name": name })))
}

fn batch_get_workflows(g: &mut GlueState, p: &Value) -> Response {
    let wanted = str_list(&p["Names"]);
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for name in wanted.iter() {
        match g.workflows.get(name) {
            Some(rec) => found.push(workflow_as_dict(rec)),
            None => missing.push(json!(name)),
        }
    }
    ok(&json!({ "MissingWorkflows": missing, "Workflows": found }))
}

fn start_workflow_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let previous_run_id = rec.runs.last().map(|r| r.run_id.clone());
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    if let Some(defaults) = rec.default_run_properties.as_object() {
        for (k, v) in defaults {
            if let Some(s) = v.as_str() {
                properties.insert(k.clone(), s.to_string());
            }
        }
    }
    if let Some(passed) = p["RunProperties"].as_object() {
        for (k, v) in passed {
            if let Some(s) = v.as_str() {
                properties.insert(k.clone(), s.to_string());
            }
        }
    }
    let run = WorkflowRunRec {
        run_id: format!("wr_{}", hex_n(64)),
        previous_run_id,
        properties,
        status: "RUNNING".to_string(),
        started: now_unix(),
    };
    let run_id = run.run_id.clone();
    rec.runs.push(run);
    Ok(ok(&json!({ "RunId": run_id })))
}

fn get_workflow_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    match rec.runs.iter().find(|r| r.run_id == run_id) {
        Some(run) => {
            let name = rec.name.clone();
            Ok(ok(&json!({ "Run": workflow_run_as_dict(run, &name) })))
        }
        None => Err(not_found("Entity not found")),
    }
}

fn get_workflow_runs(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let name = rec.name.clone();
    let list: Vec<Value> = rec
        .runs
        .iter()
        .map(|r| workflow_run_as_dict(r, &name))
        .collect();
    Ok(ok(&json!({ "Runs": list })))
}

fn stop_workflow_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    match rec.runs.iter_mut().find(|r| r.run_id == run_id) {
        Some(run) => {
            run.status = "STOPPING".to_string();
            Ok(empty_ok())
        }
        None => Err(not_found("Entity not found")),
    }
}

fn put_workflow_run_properties(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    let run = rec
        .runs
        .iter_mut()
        .find(|r| r.run_id == run_id)
        .ok_or_else(|| not_found("Entity not found"))?;
    if let Some(props) = p["RunProperties"].as_object() {
        for (k, v) in props {
            if let Some(s) = v.as_str() {
                run.properties.insert(k.clone(), s.to_string());
            }
        }
    }
    Ok(empty_ok())
}

fn get_workflow_run_properties(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    match rec.runs.iter().find(|r| r.run_id == run_id) {
        Some(run) => {
            let mut map = Map::new();
            for (k, v) in run.properties.iter() {
                map.insert(k.clone(), json!(v));
            }
            Ok(ok(&json!({ "RunProperties": Value::Object(map) })))
        }
        None => Err(not_found("Entity not found")),
    }
}

fn resume_workflow_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_workflow_mut(g, p)?;
    let run_id = str_field(p, "RunId");
    let props: Vec<(String, String)> = match rec.runs.iter().find(|r| r.run_id == run_id) {
        Some(run) => run
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => return Err(not_found("Entity not found")),
    };
    let new_run = WorkflowRunRec {
        run_id: format!("wr_{}", hex_n(64)),
        previous_run_id: Some(run_id),
        properties: props.into_iter().collect(),
        status: "RUNNING".to_string(),
        started: now_unix(),
    };
    let new_id = new_run.run_id.clone();
    rec.runs.push(new_run);
    Ok(ok(&json!({ "RunId": new_id, "NodeIds": p["NodeIds"] })))
}

#[cfg(test)]
mod family2_tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body_val: Value) -> Request {
        Request {
            service: "glue".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body_val).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn d(acct: u64, op: &str, v: Value) -> Response {
        let mut r = req(op, v);
        r.account = acct;
        dispatch(&r).unwrap()
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn err_code(resp: &Response) -> String {
        body(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn crawler_lifecycle_and_tags() {
        let acct = scope();
        let r = d(
            acct,
            "CreateCrawler",
            json!({
                "Name": "cr1", "Role": "r", "DatabaseName": "db",
                "Targets": {"S3Targets": [{"Path": "s3://x"}]},
                "Description": "desc", "TablePrefix": "dl_"
            }),
        );
        assert_eq!(r.status, 200);
        let b = body(&d(acct, "GetCrawler", json!({"Name": "cr1"})));
        assert_eq!(b["Crawler"]["Name"], "cr1");
        assert_eq!(b["Crawler"]["DatabaseName"], "db");
        assert_eq!(b["Crawler"]["Description"], "desc");
        assert_eq!(b["Crawler"]["State"], "READY");
        assert_eq!(b["Crawler"]["TablePrefix"], "dl_");
        assert!(b["Crawler"]["CreationTime"].as_f64().is_some());

        assert_eq!(
            err_code(&d(acct, "CreateCrawler", json!({"Name": "cr1"}))),
            "AlreadyExistsException"
        );

        let arn = "arn:aws:glue:us-east-1:123456789012:crawler/cr1";
        d(
            acct,
            "TagResource",
            json!({"ResourceArn": arn, "TagsToAdd": {"env": "t", "team": "x"}}),
        );
        let b = body(&d(acct, "GetTags", json!({"ResourceArn": arn})));
        assert_eq!(b["Tags"]["env"], "t");
        d(
            acct,
            "UntagResource",
            json!({"ResourceArn": arn, "TagsToRemove": ["env"]}),
        );
        let b = body(&d(acct, "GetTags", json!({"ResourceArn": arn})));
        assert!(b["Tags"].get("env").is_none());
        assert_eq!(b["Tags"]["team"], "x");

        assert_eq!(d(acct, "StartCrawler", json!({"Name": "cr1"})).status, 200);
        let b = body(&d(acct, "GetCrawler", json!({"Name": "cr1"})));
        assert_eq!(b["Crawler"]["State"], "RUNNING");
        assert_eq!(d(acct, "StopCrawler", json!({"Name": "cr1"})).status, 200);
        let b = body(&d(acct, "GetCrawler", json!({"Name": "cr1"})));
        assert_eq!(b["Crawler"]["State"], "STOPPING");

        assert_eq!(
            err_code(&d(acct, "StartCrawler", json!({"Name": "nocr"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(acct, "StopCrawler", json!({"Name": "nocr"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(
                acct,
                "StartCrawlerSchedule",
                json!({"CrawlerName": "nocr"})
            )),
            "EntityNotFoundException"
        );

        d(
            acct,
            "UpdateCrawler",
            json!({"Name": "cr1", "Description": "upd"}),
        );
        let b = body(&d(acct, "GetCrawler", json!({"Name": "cr1"})));
        assert_eq!(b["Crawler"]["Description"], "upd");

        let b = body(&d(
            acct,
            "BatchGetCrawlers",
            json!({"CrawlerNames": ["cr1", "m1"]}),
        ));
        assert_eq!(b["Crawlers"].as_array().unwrap().len(), 1);
        assert_eq!(b["CrawlersNotFound"].as_array().unwrap().len(), 1);

        let b = body(&d(acct, "GetCrawlerMetrics", json!({})));
        assert_eq!(b["CrawlerMetricsList"][0]["CrawlerName"], "cr1");

        assert_eq!(
            err_code(&d(acct, "ListCrawls", json!({"CrawlerName": "nocr"}))),
            "EntityNotFoundException"
        );

        d(acct, "DeleteCrawler", json!({"Name": "cr1"}));
        assert_eq!(
            err_code(&d(acct, "GetCrawler", json!({"Name": "cr1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn job_lifecycle_runs_bookmarks_tags() {
        let acct = scope();
        d(
            acct,
            "CreateJob",
            json!({
                "Name": "job1", "Role": "r",
                "Command": {"Name": "glueetl", "ScriptLocation": "s3://s.py"},
                "MaxRetries": 3, "Timeout": 120, "MaxCapacity": 10.0,
                "DefaultArguments": {"--x": "y"},
                "Tags": {"env": "test"}
            }),
        );
        let b = body(&d(acct, "GetJob", json!({"JobName": "job1"})));
        assert_eq!(b["Job"]["Name"], "job1");
        assert_eq!(b["Job"]["MaxRetries"], 3);
        assert_eq!(b["Job"]["Timeout"], 120);
        assert_eq!(b["Job"]["MaxCapacity"], 10.0);
        assert_eq!(b["Job"]["DefaultArguments"]["--x"], "y");
        assert!(b["Job"]["CreatedOn"].as_f64().is_some());

        let job_arn = format!("arn:aws:glue:us-east-1:{}:job/job1", acct);
        let b = body(&d(acct, "GetTags", json!({"ResourceArn": job_arn})));
        assert_eq!(b["Tags"]["env"], "test");

        d(acct, "StartJobRun", json!({"JobName": "job1"}));

        let b = body(&d(acct, "GetJobRuns", json!({"JobName": "job1"})));
        assert_eq!(b["JobRuns"].as_array().unwrap().len(), 1);
        assert_eq!(b["JobRuns"][0]["JobName"], "job1");
        let run_id = str_field(&b["JobRuns"][0], "Id");

        let b = body(&d(
            acct,
            "GetJobRun",
            json!({"JobName": "job1", "RunId": run_id.clone()}),
        ));
        assert_eq!(b["JobRun"]["JobRunState"], "SUCCEEDED");

        let r1 = str_field(
            &body(&d(acct, "StartJobRun", json!({"JobName": "job1"}))),
            "JobRunId",
        );
        assert_ne!(r1, run_id);

        let b = body(&d(acct, "GetJobBookmark", json!({"JobName": "job1"})));
        assert!(b.get("JobBookmarkEntry").is_some());
        let b = body(&d(acct, "ResetJobBookmark", json!({"JobName": "job1"})));
        assert!(b.get("JobBookmarkEntry").is_some());

        let b = body(&d(
            acct,
            "UpdateJob",
            json!({"JobName": "job1", "JobUpdate": {"Description": "upd"}}),
        ));
        assert_eq!(b["JobName"], "job1");
        let b = body(&d(acct, "GetJob", json!({"JobName": "job1"})));
        assert_eq!(b["Job"]["Description"], "upd");

        let b = body(&d(
            acct,
            "BatchGetJobs",
            json!({"JobNames": ["job1", "none"]}),
        ));
        assert_eq!(b["Jobs"].as_array().unwrap().len(), 1);
        assert_eq!(b["JobsNotFound"].as_array().unwrap().len(), 1);

        assert_eq!(
            err_code(&d(
                acct,
                "BatchStopJobRun",
                json!({"JobName": "none", "JobRunIds": ["x"]})
            )),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(acct, "GetJobBookmark", json!({"JobName": "none"}))),
            "EntityNotFoundException"
        );
        let b = body(&d(acct, "UpdateJobFromSourceControl", json!({})));
        assert_eq!(b["JobName"], "");

        let b = body(&d(acct, "ListJobs", json!({})));
        assert!(b["JobNames"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n == "job1"));

        let b = body(&d(acct, "DeleteJob", json!({"JobName": "job1"})));
        assert_eq!(b["JobName"], "job1");
        assert_eq!(
            err_code(&d(acct, "GetJob", json!({"JobName": "job1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn trigger_lifecycle() {
        let acct = scope();
        let b = body(&d(
            acct,
            "CreateTrigger",
            json!({
                "Name": "tr1", "Type": "SCHEDULED", "Schedule": "cron(0 12 * * ? *)",
                "Actions": [{"JobName": "jobA"}]
            }),
        ));
        assert_eq!(b["Name"], "tr1");

        let b = body(&d(acct, "GetTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Trigger"]["Name"], "tr1");
        assert_eq!(b["Trigger"]["Type"], "SCHEDULED");
        assert_eq!(b["Trigger"]["Schedule"], "cron(0 12 * * ? *)");
        assert_eq!(b["Trigger"]["Actions"][0]["JobName"], "jobA");
        assert_eq!(b["Trigger"]["State"], "CREATED");

        assert_eq!(
            err_code(&d(
                acct,
                "CreateTrigger",
                json!({"Name": "tr2", "Type": "CONDITIONAL", "Actions": []})
            )),
            "InvalidInputException"
        );
        assert_eq!(
            err_code(&d(acct, "GetTrigger", json!({"Name": "tr2"}))),
            "EntityNotFoundException"
        );

        let b = body(&d(acct, "StartTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Name"], "tr1");
        let b = body(&d(acct, "GetTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Trigger"]["State"], "ACTIVATED");
        let b = body(&d(acct, "StopTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Name"], "tr1");

        let b = body(&d(
            acct,
            "UpdateTrigger",
            json!({"Name": "tr1", "TriggerUpdate": {"Description": "upd"}}),
        ));
        assert_eq!(b["Trigger"]["Name"], "tr1");
        let b = body(&d(acct, "GetTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Trigger"]["Description"], "upd");

        let b = body(&d(acct, "GetTriggers", json!({})));
        assert_eq!(b["Triggers"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "GetTriggers", json!({"DependentJobName": "jobA"})));
        assert_eq!(b["Triggers"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "GetTriggers", json!({"DependentJobName": "nope"})));
        assert_eq!(b["Triggers"].as_array().unwrap().len(), 0);

        let b = body(&d(acct, "ListTriggers", json!({})));
        assert_eq!(b["TriggerNames"], json!(["tr1"]));

        let b = body(&d(
            acct,
            "BatchGetTriggers",
            json!({"TriggerNames": ["tr1", "z"]}),
        ));
        assert_eq!(b["Triggers"].as_array().unwrap().len(), 1);
        assert_eq!(b["TriggersNotFound"].as_array().unwrap().len(), 1);

        assert_eq!(
            err_code(&d(
                acct,
                "UpdateTrigger",
                json!({"Name": "nond", "TriggerUpdate": {}})
            )),
            "EntityNotFoundException"
        );

        let b = body(&d(acct, "DeleteTrigger", json!({"Name": "tr1"})));
        assert_eq!(b["Name"], "tr1");
        assert_eq!(
            err_code(&d(acct, "GetTrigger", json!({"Name": "tr1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn workflow_lifecycle_runs_and_properties() {
        let acct = scope();
        let b = body(&d(
            acct,
            "CreateWorkflow",
            json!({"Name": "wf1", "Description": "d"}),
        ));
        assert_eq!(b["Name"], "wf1");

        let b = body(&d(acct, "GetWorkflow", json!({"Name": "wf1"})));
        assert_eq!(b["Workflow"]["Name"], "wf1");
        assert_eq!(b["Workflow"]["Description"], "d");

        let b = body(&d(acct, "StartWorkflowRun", json!({"Name": "wf1"})));
        let run_id = str_field(&b, "RunId");
        assert!(run_id.starts_with("wr_"));

        let b = body(&d(
            acct,
            "GetWorkflowRun",
            json!({"Name": "wf1", "RunId": run_id.clone()}),
        ));
        assert_eq!(b["Run"]["Name"], "wf1");
        assert_eq!(b["Run"]["WorkflowRunId"], run_id);
        assert_eq!(b["Run"]["Status"], "RUNNING");

        let b = body(&d(acct, "GetWorkflowRuns", json!({"Name": "wf1"})));
        assert_eq!(b["Runs"].as_array().unwrap().len(), 1);

        d(
            acct,
            "PutWorkflowRunProperties",
            json!({"Name": "wf1", "RunId": run_id.clone(), "RunProperties": {"k1": "v1"}}),
        );
        let b = body(&d(
            acct,
            "GetWorkflowRunProperties",
            json!({"Name": "wf1", "RunId": run_id.clone()}),
        ));
        assert_eq!(b["RunProperties"]["k1"], "v1");

        d(
            acct,
            "StopWorkflowRun",
            json!({"Name": "wf1", "RunId": run_id.clone()}),
        );
        let b = body(&d(
            acct,
            "GetWorkflowRun",
            json!({"Name": "wf1", "RunId": run_id.clone()}),
        ));
        assert_eq!(b["Run"]["Status"], "STOPPING");

        let b = body(&d(
            acct,
            "ResumeWorkflowRun",
            json!({"Name": "wf1", "RunId": run_id, "NodeIds": ["n1"]}),
        ));
        assert!(str_field(&b, "RunId").starts_with("wr_"));

        let b = body(&d(acct, "ListWorkflows", json!({})));
        assert_eq!(b["Workflows"], json!(["wf1"]));

        let b = body(&d(
            acct,
            "BatchGetWorkflows",
            json!({"Names": ["wf1", "zz"]}),
        ));
        assert_eq!(b["Workflows"].as_array().unwrap().len(), 1);
        assert_eq!(b["MissingWorkflows"].as_array().unwrap().len(), 1);

        let b = body(&d(
            acct,
            "UpdateWorkflow",
            json!({"Name": "wf1", "Description": "u"}),
        ));
        assert_eq!(b["Name"], "wf1");

        d(acct, "DeleteWorkflow", json!({"Name": "wf1"}));
        assert_eq!(
            err_code(&d(acct, "GetWorkflow", json!({"Name": "wf1"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(acct, "StartWorkflowRun", json!({"Name": "wf1"}))),
            "EntityNotFoundException"
        );
        assert!(
            d(
                acct,
                "StopWorkflowRun",
                json!({"Name": "wf1", "RunId": "wr_x"})
            )
            .status
                >= 300
        );
    }
}
// ---------------------------------------------------------------------------
// Schema registry (moto's GSR family). ARNs are copied into the records so
// serializers don't need the state; scope strings are read into locals
// before mutable borrows.
// ---------------------------------------------------------------------------

const DEFAULT_REGISTRY: &str = "default-registry";

/// moto's `validate_registry_id`: RegistryId absent → default-registry.
fn registry_name_of(p: &Value) -> String {
    let id = &p["RegistryId"];
    if let Some(name) = id["RegistryName"].as_str() {
        if !name.is_empty() {
            return name.to_string();
        }
    }
    if let Some(arn) = id["RegistryArn"].as_str() {
        if !arn.is_empty() {
            return arn
                .rsplit('/')
                .next()
                .unwrap_or(DEFAULT_REGISTRY)
                .to_string();
        }
    }
    DEFAULT_REGISTRY.to_string()
}

fn make_schema_arn(region: &str, account: u64, registry: &str, schema: &str) -> String {
    make_arn(region, account, "schema", &format!("{registry}/{schema}"))
}

fn find_registry_mut<'a>(
    g: &'a mut GlueState,
    name: &str,
) -> Result<&'a mut RegistryRec, Response> {
    g.registries
        .get_mut(name)
        .ok_or_else(|| not_found(&format!("Registry is not found. RegistryName: {name}")))
}

fn registry_dict(rec: &RegistryRec) -> Value {
    json!({
        "RegistryArn": rec.arn,
        "RegistryName": rec.name,
        "Description": rec.description,
        "Tags": rec.tags.clone(),
    })
}

fn create_registry(req: &Request, g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "RegistryName");
    let arn = make_arn(&g.scope_region, g.scope_account, "registry", &name);
    if g.registries.contains_key(&name) {
        return Err(err(
            400,
            "AlreadyExistsException",
            &format!("Registry already exists. RegistryName: {name}"),
        ));
    }
    tag_resource(g, &arn, p.get("Tags"));
    g.registries.insert(
        name.clone(),
        RegistryRec {
            name: name.clone(),
            arn,
            description: p["Description"].clone(),
            tags: p["Tags"].clone(),
            created: now_unix(),
            updated: now_unix(),
            ..Default::default()
        },
    );
    Ok(ok(&json!({
        "RegistryArn": make_arn(&g.scope_region, g.scope_account, "registry", &name),
        "RegistryName": name,
        "Description": p["Description"],
    })))
}

fn delete_registry(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = registry_name_of(p);
    let dict = registry_dict(find_registry_mut(g, &name)?);
    g.registries.remove(&name);
    Ok(ok(&dict))
}

fn get_registry(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = registry_name_of(p);
    Ok(ok(&registry_dict(find_registry_mut(g, &name)?)))
}

fn list_registries(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g.registries.values().map(registry_dict).collect();
    Ok(ok(&json!({ "Registries": list })))
}

fn update_registry(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = registry_name_of(p);
    let rec = find_registry_mut(g, &name)?;
    if let Some(desc) = p.get("Description") {
        if !desc.is_null() {
            rec.description = desc.clone();
        }
    }
    rec.updated = now_unix();
    Ok(ok(&json!({
        "RegistryArn": rec.arn,
        "RegistryName": rec.name,
    })))
}

fn schema_dict(rec: &SchemaRec) -> Value {
    json!({
        "RegistryArn": rec.registry_arn,
        "RegistryName": rec.registry_name,
        "SchemaName": rec.schema_name,
        "SchemaArn": rec.arn,
        "DataFormat": rec.data_format,
        "Compatibility": rec.compatibility,
        "SchemaCheckpoint": 1,
        "LatestSchemaVersion": rec.latest_schema_version,
        "NextSchemaVersion": rec.next_schema_version,
        "SchemaStatus": "AVAILABLE",
        "SchemaVersionId": rec.first_version_id,
        "SchemaVersionStatus": "AVAILABLE",
        "Description": rec.description,
    })
}

fn schema_version_list_dict(v: &SchemaVersionRec) -> Value {
    json!({
        "SchemaVersionId": v.id,
        "VersionNumber": v.version_number,
        "Status": "AVAILABLE",
    })
}

fn schema_version_get_dict(v: &SchemaVersionRec, arn: &str, data_format: &str) -> Value {
    json!({
        "SchemaVersionId": v.id,
        "SchemaDefinition": v.definition,
        "SchemaArn": arn,
        "VersionNumber": v.version_number,
        "Status": "AVAILABLE",
        "CreatedTime": now_iso(),
        "DataFormat": data_format,
    })
}

fn schema_not_found_msg(registry: &str, schema: &str) -> String {
    format!("Schema is not found. RegistryName: {registry}, SchemaName: {schema}, SchemaArn: null")
}

/// moto's `validate_schema_id`: name pair or ARN; missing → SchemaNotFound.
fn resolve_schema_id(g: &GlueState, p: &Value) -> Result<(String, String), Response> {
    let id = &p["SchemaId"];
    if let Some(arn) = id["SchemaArn"].as_str() {
        if !arn.is_empty() {
            let mut parts = arn.rsplit('/');
            let schema_name = parts.next().unwrap_or_default().to_string();
            let registry_name = parts.next().unwrap_or_default().to_string();
            return if g
                .registries
                .get(&registry_name)
                .map_or(false, |r| r.schemas.contains_key(&schema_name))
            {
                Ok((registry_name, schema_name))
            } else {
                Err(not_found(&format!(
                    "Schema is not found. RegistryName: {registry_name}, SchemaName: {schema_name}, SchemaArn: {arn}"
                )))
            };
        }
    }
    let registry_name = id["RegistryName"].as_str().unwrap_or_default();
    let schema_name = id["SchemaName"].as_str().unwrap_or_default();
    if !registry_name.is_empty() && !schema_name.is_empty() {
        return if g
            .registries
            .get(registry_name)
            .map_or(false, |r| r.schemas.contains_key(schema_name))
        {
            Ok((registry_name.to_string(), schema_name.to_string()))
        } else {
            Err(not_found(&schema_not_found_msg(registry_name, schema_name)))
        };
    }
    Err(gsr_invalid_input(
        "At least one of (registryName and schemaName) or schemaArn has to be provided.",
    ))
}

fn is_uuid_shape(v: &str) -> bool {
    let parts: Vec<&str> = v.split('-').collect();
    parts.len() == 5
        && parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && v.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

fn valid_version_input(p: &Value, has_schema_id: bool) -> bool {
    let has_latest = !p["SchemaVersionNumber"]["LatestVersion"].is_null();
    let has_number = !p["SchemaVersionNumber"]["VersionNumber"].is_null();
    has_schema_id && (has_latest || has_number)
}

fn create_schema(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let registry_name = registry_name_of(p);
    let schema_name = str_field(p, "SchemaName");
    let data_format = str_field(p, "DataFormat");
    let compatibility = str_field(p, "Compatibility");
    let definition_opt = p["SchemaDefinition"].as_str();
    let definition = definition_opt.unwrap_or_default();
    if !["AVRO", "JSON", "PROTOBUF"].contains(&data_format.as_str()) {
        return Err(invalid_input("Data format is not valid."));
    }
    if ![
        "NONE",
        "DISABLED",
        "BACKWARD",
        "BACKWARD_ALL",
        "FORWARD",
        "FORWARD_ALL",
        "FULL",
        "FULL_ALL",
    ]
    .contains(&compatibility.as_str())
    {
        return Err(invalid_input("Compatibility is not valid."));
    }
    if (data_format == "AVRO" || data_format == "JSON")
        && definition_opt
            .map(|d| !d.is_empty() && serde_json::from_str::<Value>(d).is_err())
            .unwrap_or(false)
    {
        return Err(gsr_invalid_input(&format!(
            "Schema definition of {data_format} data format is invalid: Expecting property name enclosed in double quotes"
        )));
    }
    let (region, account) = (g.scope_region.clone(), g.scope_account);
    let reg_arn = make_arn(&region, account, "registry", &registry_name);
    let schema_arn = make_arn(
        &region,
        account,
        "schema",
        &format!("{registry_name}/{schema_name}"),
    );
    if registry_name == DEFAULT_REGISTRY && !g.registries.contains_key(&registry_name) {
        g.registries.insert(
            registry_name.clone(),
            RegistryRec {
                name: registry_name.clone(),
                arn: reg_arn.clone(),
                created: now_unix(),
                updated: now_unix(),
                ..Default::default()
            },
        );
    }
    tag_resource(g, &schema_arn, p.get("Tags"));
    let registry = g.registries.get_mut(&registry_name).ok_or_else(|| {
        not_found(&format!(
            "Registry is not found. RegistryName: {registry_name}"
        ))
    })?;
    if registry.schemas.contains_key(&schema_name) {
        return Err(err(
            400,
            "AlreadyExistsException",
            &format!("Schema already exists. SchemaName: {schema_name}"),
        ));
    }
    let version = SchemaVersionRec {
        id: uuid4(),
        definition: definition.to_string(),
        version_number: 1,
        created: now_unix(),
        ..Default::default()
    };
    let first_version_id = version.id.clone();
    let rec = SchemaRec {
        registry_name: registry_name.clone(),
        registry_arn: reg_arn,
        arn: schema_arn,
        schema_name: schema_name.clone(),
        data_format,
        compatibility,
        description: p["Description"].clone(),
        latest_schema_version: 1,
        next_schema_version: 2,
        first_version_id,
        versions: vec![version],
    };
    let out = schema_dict(&rec);
    registry.schemas.insert(schema_name, rec);
    Ok(ok(&out))
}

fn get_schema(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let out = schema_dict(rec);
    Ok(ok(&out))
}

fn delete_schema(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let out = json!({
        "SchemaName": rec.schema_name,
        "SchemaArn": rec.arn,
        "Status": "DELETING",
    });
    registry.schemas.remove(&schema_name);
    Ok(ok(&out))
}

fn update_schema(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get_mut(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    if let Some(c) = p.get("Compatibility") {
        if !c.is_null() {
            rec.compatibility = c.as_str().unwrap_or_default().to_string();
        }
    }
    if let Some(d) = p.get("Description") {
        if !d.is_null() {
            rec.description = d.clone();
        }
    }
    let out = schema_dict(rec);
    Ok(ok(&out))
}

fn list_schemas(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let mut list = Vec::new();
    if p["RegistryId"].is_null() {
        for registry in g.registries.values() {
            list.extend(registry.schemas.values().map(schema_dict));
        }
    } else {
        let name = registry_name_of(p);
        let registry = find_registry_mut(g, &name)?;
        list.extend(registry.schemas.values().map(schema_dict));
    }
    Ok(ok(&json!({ "Schemas": list })))
}

fn list_schema_versions(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let list: Vec<Value> = rec.versions.iter().map(schema_version_list_dict).collect();
    Ok(ok(&json!({ "Schemas": list })))
}

fn register_schema_version(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let definition = p["SchemaDefinition"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get_mut(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    if rec.compatibility == "DISABLED" {
        return Err(gsr_invalid_input(&format!(
            "Compatibility DISABLED does not allow versioning. SchemaId: SchemaId(schemaArn=null, schemaName={schema_name}, registryName={registry_name})"
        )));
    }
    for v in rec.versions.iter() {
        let same = if rec.data_format == "AVRO" || rec.data_format == "JSON" {
            match (
                serde_json::from_str::<Value>(&definition),
                serde_json::from_str::<Value>(&v.definition),
            ) {
                (Ok(a), Ok(b)) => a == b,
                _ => definition == v.definition,
            }
        } else {
            definition == v.definition
        };
        if same {
            return Ok(ok(&json!({
                "SchemaVersionId": v.id,
                "VersionNumber": v.version_number,
                "Status": "AVAILABLE",
            })));
        }
    }
    let version_number = rec.next_schema_version;
    rec.next_schema_version += 1;
    rec.latest_schema_version = version_number;
    let version = SchemaVersionRec {
        id: uuid4(),
        definition,
        version_number,
        created: now_unix(),
        ..Default::default()
    };
    let out = json!({
        "SchemaVersionId": version.id,
        "VersionNumber": version.version_number,
        "Status": "AVAILABLE",
    });
    rec.versions.push(version);
    Ok(ok(&out))
}

/// Owned view of a resolved schema version so handlers don't hold borrows.
struct ResolvedVersion {
    registry_name: String,
    schema_name: String,
    schema_arn: String,
    data_format: String,
    index: usize,
}

fn resolve_schema_version(g: &mut GlueState, p: &Value) -> Result<ResolvedVersion, Response> {
    resolve_schema_version_vn(g, "SchemaVersionNumber", p)
}

/// `number_member` varies per API (GetSchemaVersionsDiff uses
/// First/SecondSchemaVersionNumber).
fn resolve_schema_version_vn(
    g: &mut GlueState,
    number_member: &str,
    p: &Value,
) -> Result<ResolvedVersion, Response> {
    let region = g.scope_region.clone();
    let account = g.scope_account;
    // Direct SchemaVersionId.
    if let Some(vid) = p["SchemaVersionId"].as_str() {
        if !vid.is_empty() {
            if !is_uuid_shape(vid) {
                return Err(gsr_invalid_input(
                    "The parameter value contains one or more characters that are not valid. Parameter Name: SchemaVersionId",
                ));
            }
            if !p["SchemaId"].is_null() || !p["SchemaVersionNumber"].is_null() {
                return Err(gsr_invalid_input(
                    "No other input parameters can be specified when fetching by SchemaVersionId.",
                ));
            }
            for registry in g.registries.values() {
                for rec in registry.schemas.values() {
                    if let Some(idx) = rec.versions.iter().position(|v| v.id == vid) {
                        return Ok(ResolvedVersion {
                            registry_name: registry.name.clone(),
                            schema_name: rec.schema_name.clone(),
                            schema_arn: rec.arn.clone(),
                            data_format: rec.data_format.clone(),
                            index: idx,
                        });
                    }
                }
            }
            return Err(not_found(&format!(
                "Schema version is not found. SchemaVersionId: {vid}"
            )));
        }
    }
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    if p[number_member].is_null() && p["SchemaVersionNumber"].is_null() {
        return Err(gsr_invalid_input(
            "One of version number (or) latest version is required.",
        ));
    }
    let reg = g.registries.get(&registry_name).ok_or_else(|| {
        not_found(&format!(
            "Registry is not found. RegistryName: {registry_name}"
        ))
    })?;
    let rec = reg
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let want_latest = p[number_member]["LatestVersion"].as_bool().unwrap_or(false);
    let want_number = p[number_member]["VersionNumber"].as_u64();
    let idx = if want_latest {
        rec.versions.len().saturating_sub(1)
    } else {
        let n = want_number.unwrap_or(0);
        match rec.versions.iter().position(|v| v.version_number == n) {
            Some(i) => i,
            None => {
                return Err(not_found(&format!(
                    "Schema version is not found. RegistryName: {registry_name}, SchemaName: {schema_name}, SchemaArn: null, VersionNumber: {n}, isLatestVersion: {want_latest}"
                )))
            }
        }
    };
    Ok(ResolvedVersion {
        schema_arn: make_schema_arn(&region, account, &registry_name, &schema_name),
        registry_name,
        schema_name,
        data_format: rec.data_format.clone(),
        index: idx,
    })
}

fn schema_version_of<'a>(
    g: &'a mut GlueState,
    v: &ResolvedVersion,
) -> Result<&'a SchemaVersionRec, Response> {
    let registry = find_registry_mut(g, &v.registry_name)?;
    let rec = registry
        .schemas
        .get(&v.schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&v.registry_name, &v.schema_name)))?;
    rec.versions
        .get(v.index)
        .ok_or_else(|| not_found("Schema version is not found."))
}

fn get_schema_version(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let v = resolve_schema_version(g, p)?;
    let rec = schema_version_of(g, &v)?;
    Ok(ok(&schema_version_get_dict(
        rec,
        &v.schema_arn,
        &v.data_format,
    )))
}

fn get_schema_by_definition(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let region = g.scope_region.clone();
    let account = g.scope_account;
    let definition = p["SchemaDefinition"].as_str().unwrap_or_default();
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    for version in rec.versions.iter() {
        if version.definition == definition {
            return Ok(ok(&json!({
                "SchemaVersionId": version.id,
                "SchemaArn": make_schema_arn(&region, account, &registry_name, &schema_name),
                "Status": "AVAILABLE",
                "CreatedTime": now_iso(),
                "DataFormat": rec.data_format,
            })));
        }
    }
    Err(not_found(&schema_not_found_msg(
        &registry_name,
        &schema_name,
    )))
}

fn check_schema_version_validity(p: &Value) -> Result<Response, Response> {
    let format = str_field(p, "DataFormat");
    let mut valid = true;
    let mut error = "";
    if format == "JSON" && !definition_is_json_member(p, "SchemaDefinition") {
        valid = false;
        error = "Expecting property name enclosed in double quotes";
    }
    Ok(ok(&json!({ "Valid": valid, "Error": error })))
}

fn definition_is_json_member(p: &Value, member: &str) -> bool {
    p[member]
        .as_str()
        .map(|s| serde_json::from_str::<Value>(s).is_ok())
        .unwrap_or(true)
}

fn delete_schema_versions(p: &Value) -> Result<Response, Response> {
    let versions = str_field(p, "Versions");
    let mut list = Vec::new();
    for v in versions.split(',') {
        if let Ok(n) = v.trim().parse::<u64>() {
            list.push(json!({"VersionNumber": n, "Status": "SUCCESS"}));
        }
    }
    Ok(ok(&json!({ "SchemaVersionErrors": list })))
}

fn get_schema_versions_diff(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let first = resolve_schema_version_vn(g, "FirstSchemaVersionNumber", p)?;
    let second = resolve_schema_version_vn(g, "SecondSchemaVersionNumber", p)?;
    let (registry_name, schema_name) = resolve_schema_id(g, p)?;
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let first_def = rec
        .versions
        .get(first.index)
        .map(|v| v.definition.clone())
        .unwrap_or_default();
    let second_def = rec
        .versions
        .get(second.index)
        .map(|v| v.definition.clone())
        .unwrap_or_default();
    Ok(ok(&json!({
        "Diff": format!(
            "--- version 1\n+++ version 2\n-{first_def}\n+{second_def}"
        ),
    })))
}

fn qualified_version_mut<'a>(
    g: &'a mut GlueState,
    registry: &str,
    schema: &str,
    index: usize,
) -> Result<&'a mut SchemaVersionRec, Response> {
    let registry_ref = find_registry_mut(g, registry)?;
    let rec = registry_ref
        .schemas
        .get_mut(schema)
        .ok_or_else(|| not_found(&schema_not_found_msg(registry, schema)))?;
    rec.versions
        .get_mut(index)
        .ok_or_else(|| not_found("Schema version is not found."))
}

fn put_schema_version_metadata(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let v = resolve_schema_version(g, p)?;
    let key = str_field(&p["MetadataKeyValue"], "MetadataKey");
    let value = str_field(&p["MetadataKeyValue"], "MetadataValue");
    let version = qualified_version_mut(g, &v.registry_name, &v.schema_name, v.index)?;
    let entry = version.metadata.entry(key.clone()).or_default();
    if !entry.contains(&value) {
        entry.push(value.clone());
    }
    let version_number = version.version_number;
    let id = version.id.clone();
    Ok(ok(&json!({
        "SchemaVersionId": id,
        "LatestVersion": false,
        "VersionNumber": version_number,
        "MetadataKey": key,
        "MetadataValue": value,
    })))
}

fn locate_version_by_id<'a>(
    g: &'a mut GlueState,
    vid: &str,
) -> Result<(String, String, usize, String), Response> {
    for registry in g.registries.keys().cloned().collect::<Vec<_>>() {
        let reg = g.registries.get(&registry).expect("key exists");
        for (schema_name, rec) in reg.schemas.iter() {
            if let Some(idx) = rec.versions.iter().position(|v| v.id == vid) {
                return Ok((registry.clone(), schema_name.clone(), idx, rec.arn.clone()));
            }
        }
    }
    Err(not_found(&format!(
        "Schema version is not found. SchemaVersionId: {vid}"
    )))
}

fn query_schema_version_metadata(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let direct = p["SchemaVersionId"].as_str().unwrap_or_default();
    let (registry_name, schema_name, index) = if !direct.is_empty() {
        if !is_uuid_shape(direct) {
            return Err(gsr_invalid_input(
                "The parameter value contains one or more characters that are not valid. Parameter Name: SchemaVersionId",
            ));
        }
        let (r, s, i, _) = locate_version_by_id(g, direct)?;
        (r, s, i)
    } else {
        let v = resolve_schema_version(g, p)?;
        (v.registry_name, v.schema_name, v.index)
    };
    let registry = find_registry_mut(g, &registry_name)?;
    let rec = registry
        .schemas
        .get(&schema_name)
        .ok_or_else(|| not_found(&schema_not_found_msg(&registry_name, &schema_name)))?;
    let version = rec
        .versions
        .get(index)
        .ok_or_else(|| not_found("Schema version is not found."))?;
    let mut metadata = Map::new();
    for (k, values) in version.metadata.iter() {
        metadata.insert(k.clone(), json!(values));
    }
    Ok(ok(&json!({ "MetadataMap": {
        "SchemaVersionId": version.id,
        "MetadataKeyMap": metadata,
    } })))
}

fn remove_schema_version_metadata(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    if p["MetadataKeyValue"].is_null() {
        return Err(invalid_input("No metadata key/value provided."));
    }
    if p["SchemaVersionId"].is_null()
        && p["SchemaId"]["RegistryName"].is_null()
        && p["SchemaVersionNumber"].is_null()
    {
        return Err(gsr_invalid_input(
            "At least one of (registryName and schemaName) or schemaArn has to be provided.",
        ));
    }
    let direct = p["SchemaVersionId"].as_str().unwrap_or_default();
    let (registry_name, schema_name, index) = if !direct.is_empty() {
        if !is_uuid_shape(direct) {
            return Err(gsr_invalid_input(
                "The parameter value contains one or more characters that are not valid. Parameter Name: SchemaVersionId",
            ));
        }
        let (r, s, i, _) = locate_version_by_id(g, direct)?;
        (r, s, i)
    } else {
        let v = resolve_schema_version(g, p)?;
        (v.registry_name, v.schema_name, v.index)
    };
    let key = str_field(&p["MetadataKeyValue"], "MetadataKey");
    let value = str_field(&p["MetadataKeyValue"], "MetadataValue");
    let version = qualified_version_mut(g, &registry_name, &schema_name, index)?;
    if let Some(values) = version.metadata.get_mut(&key) {
        if let Some(at) = values.iter().position(|v| v == &value) {
            values.remove(at);
        }
    }
    let version_number = version.version_number;
    Ok(ok(&json!({
        "LatestVersion": false,
        "VersionNumber": version_number,
        "MetadataKey": key,
        "MetadataValue": value,
    })))
}

#[cfg(test)]
mod family3_tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body_val: Value) -> Request {
        Request {
            service: "glue".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body_val).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn d(acct: u64, op: &str, v: Value) -> Response {
        let mut r = req(op, v);
        r.account = acct;
        dispatch(&r).unwrap()
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn err_code(resp: &Response) -> String {
        body(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    fn create_registry(acct: u64, name: &str) -> Response {
        d(
            acct,
            "CreateRegistry",
            json!({"RegistryName": name, "Description": "test"}),
        )
    }

    fn schema_body(registry: &str, schema: &str) -> Value {
        json!({
            "RegistryId": {"RegistryName": registry},
            "SchemaName": schema,
            "DataFormat": "AVRO",
            "Compatibility": "NONE",
            "SchemaDefinition": r#"{"type":"record","name":"T","fields":[]}"#
        })
    }

    #[test]
    fn registry_crud() {
        let acct = scope();
        let r = create_registry(acct, "reg1");
        assert_eq!(r.status, 200);
        let b = body(&r);
        assert_eq!(b["RegistryName"], "reg1");
        assert!(b["RegistryArn"].as_str().unwrap().contains("registry/reg1"));

        let b = body(&d(
            acct,
            "GetRegistry",
            json!({"RegistryId": {"RegistryName": "reg1"}}),
        ));
        assert_eq!(b["RegistryName"], "reg1");
        assert_eq!(b["Description"], "test");

        assert_eq!(
            err_code(&d(
                acct,
                "GetRegistry",
                json!({"RegistryId": {"RegistryName": "nond"}})
            )),
            "EntityNotFoundException"
        );

        let b = body(&d(
            acct,
            "UpdateRegistry",
            json!({"RegistryId": {"RegistryName": "reg1"}, "Description": "u"}),
        ));
        assert_eq!(b["RegistryName"], "reg1");
        let b = body(&d(
            acct,
            "GetRegistry",
            json!({"RegistryId": {"RegistryName": "reg1"}}),
        ));
        assert_eq!(b["Description"], "u");

        let b = body(&d(acct, "ListRegistries", json!({})));
        assert_eq!(b["Registries"].as_array().unwrap().len(), 1);

        let b = body(&d(
            acct,
            "DeleteRegistry",
            json!({"RegistryId": {"RegistryName": "reg1"}}),
        ));
        assert_eq!(b["RegistryName"], "reg1");
        assert_eq!(
            err_code(&d(
                acct,
                "GetRegistry",
                json!({"RegistryId": {"RegistryName": "reg1"}})
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn schema_lifecycle_and_versions() {
        let acct = scope();
        // Schema without RegistryId lands in default-registry.
        let r = d(
            acct,
            "CreateSchema",
            json!({
                "SchemaName": "sc1", "DataFormat": "AVRO", "Compatibility": "NONE",
                "SchemaDefinition": r#"{"type":"record","name":"T","fields":[]}"#
            }),
        );
        assert_eq!(r.status, 200);
        let b = body(&r);
        assert_eq!(b["SchemaName"], "sc1");
        assert!(b["SchemaArn"]
            .as_str()
            .unwrap()
            .contains("schema/default-registry/sc1"));

        // register a second version
        let b = body(&d(
            acct,
            "RegisterSchemaVersion",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaDefinition": r#"{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}"#
            }),
        ));
        assert_eq!(b["VersionNumber"], 2);
        assert_eq!(b["Status"], "AVAILABLE");
        assert!(b["SchemaVersionId"].as_str().is_some());

        // identical definition compresses to version 2
        let b = body(&d(
            acct,
            "RegisterSchemaVersion",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaDefinition": r#"{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}"#
            }),
        ));
        assert_eq!(b["VersionNumber"], 2);

        // latest version
        let b = body(&d(
            acct,
            "GetSchemaVersion",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaVersionNumber": {"LatestVersion": true}
            }),
        ));
        assert_eq!(b["VersionNumber"], 2);
        assert_eq!(b["DataFormat"], "AVRO");
        assert!(b["SchemaDefinition"].as_str().is_some());

        // explicit version 1
        let b = body(&d(
            acct,
            "GetSchemaVersion",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaVersionNumber": {"VersionNumber": 1}
            }),
        ));
        assert_eq!(b["VersionNumber"], 1);

        // by definition
        let b = body(&d(
            acct,
            "GetSchemaByDefinition",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaDefinition": r#"{"type":"record","name":"T","fields":[]}"#
            }),
        ));
        assert!(b["SchemaVersionId"].as_str().is_some());
        assert_eq!(b["DataFormat"], "AVRO");

        // get_schema echoes compat; update compat
        let b = body(&d(
            acct,
            "GetSchema",
            json!({"SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"}}),
        ));
        assert_eq!(b["Compatibility"], "NONE");
        let b = body(&d(
            acct,
            "UpdateSchema",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "Compatibility": "BACKWARD"
            }),
        ));
        assert_eq!(b["SchemaName"], "sc1");
        let b = body(&d(
            acct,
            "GetSchema",
            json!({"SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"}}),
        ));
        assert_eq!(b["Compatibility"], "BACKWARD");

        // list schema versions
        let b = body(&d(
            acct,
            "ListSchemaVersions",
            json!({"SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"}}),
        ));
        assert_eq!(b["Schemas"].as_array().unwrap().len(), 2);

        // metadata
        let b = body(&d(
            acct,
            "PutSchemaVersionMetadata",
            json!({
                "SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"},
                "SchemaVersionNumber": {"VersionNumber": 1},
                "MetadataKeyValue": {"MetadataKey": "k", "MetadataValue": "v"}
            }),
        ));
        assert_eq!(b["MetadataKey"], "k");
        assert_eq!(b["MetadataValue"], "v");

        // delete + not found
        let b = body(&d(
            acct,
            "DeleteSchema",
            json!({"SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"}}),
        ));
        assert_eq!(b["Status"], "DELETING");
        assert_eq!(
            err_code(&d(
                acct,
                "GetSchema",
                json!({"SchemaId": {"SchemaName": "sc1", "RegistryName": "default-registry"}})
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn schema_validation_and_metadata_errors() {
        let acct = scope();
        assert_eq!(create_registry(acct, "reg2").status, 200);

        // bad data format
        let r = d(
            acct,
            "CreateSchema",
            json!({"RegistryId": {"RegistryName": "reg2"}, "SchemaName": "s",
                   "DataFormat": "X", "Compatibility": "NONE",
                   "SchemaDefinition": "{}"}),
        );
        assert_eq!(err_code(&r), "InvalidInputException");

        // bad compatibility
        let r = d(
            acct,
            "CreateSchema",
            json!({"RegistryId": {"RegistryName": "reg2"}, "SchemaName": "s",
                   "DataFormat": "AVRO", "Compatibility": "BOGUS"}),
        );
        assert_eq!(err_code(&r), "InvalidInputException");

        // invalid definition
        let r = d(
            acct,
            "CreateSchema",
            json!({"RegistryId": {"RegistryName": "reg2"}, "SchemaName": "s",
                   "DataFormat": "AVRO", "Compatibility": "NONE",
                   "SchemaDefinition": "not json"}),
        );
        assert_eq!(err_code(&r), "InvalidInputException");

        // valid schema for validation paths
        d(acct, "CreateSchema", schema_body("reg2", "s1"));

        // get schema via fake ids
        let r = d(
            acct,
            "GetSchema",
            json!({"SchemaId": {"SchemaName": "z", "RegistryName": "reg2"}}),
        );
        assert_eq!(err_code(&r), "EntityNotFoundException");

        // version with garbage id → InvalidInput
        let r = d(
            acct,
            "GetSchemaVersion",
            json!({"SchemaVersionId": "fake-uuid-0000-1234-abcd-ef0123456789"}),
        );
        assert_eq!(err_code(&r), "InvalidInputException");

        // remove metadata with no ids → InvalidInput
        let r = d(
            acct,
            "RemoveSchemaVersionMetadata",
            json!({"MetadataKeyValue": {"MetadataKey": "k", "MetadataValue": "v"}}),
        );
        assert_eq!(err_code(&r), "InvalidInputException");

        // check validity
        let b = body(&d(
            acct,
            "CheckSchemaVersionValidity",
            json!({"DataFormat": "AVRO", "SchemaDefinition": "{}"}),
        ));
        assert_eq!(b["Valid"], true);
        let b = body(&d(
            acct,
            "CheckSchemaVersionValidity",
            json!({"DataFormat": "JSON", "SchemaDefinition": "{nope}"}),
        ));
        assert_eq!(b["Valid"], false);

        // delete schema versions
        let b = body(&d(
            acct,
            "DeleteSchemaVersions",
            json!({"SchemaId": {"RegistryName": "reg2", "SchemaName": "s1"}, "Versions": "1,2"}),
        ));
        assert_eq!(b["SchemaVersionErrors"].as_array().unwrap().len(), 2);

        // diff (both versions exist after two registers)
        d(
            acct,
            "RegisterSchemaVersion",
            json!({
                "SchemaId": {"SchemaName": "s1", "RegistryName": "reg2"},
                "SchemaDefinition": r#"{"type":"record","name":"U"}"#
            }),
        );
        let b = body(&d(
            acct,
            "GetSchemaVersionsDiff",
            json!({
                "SchemaId": {"RegistryName": "reg2", "SchemaName": "s1"},
                "FirstSchemaVersionNumber": {"VersionNumber": 1},
                "SecondSchemaVersionNumber": {"VersionNumber": 2},
                "SchemaDiffType": "SYNTAX_DIFF"
            }),
        ));
        assert!(b.get("Diff").is_some());
    }

    #[test]
    fn default_registry_lifecycle() {
        let acct = scope();
        // GetRegistry on default-registry before any schema: moto auto-creates
        // the default registry lazily inside create_schema only — direct get
        // raises RegistryNotFound.
        let r = d(acct, "GetRegistry", json!({"RegistryId": {}}));
        assert_eq!(err_code(&r), "EntityNotFoundException");

        // create schema without RegistryId → default-registry created.
        d(
            acct,
            "CreateSchema",
            json!({"SchemaName": "a", "DataFormat": "AVRO", "Compatibility": "NONE"}),
        );
        let r = d(acct, "GetRegistry", json!({"RegistryId": {}}));
        assert_eq!(r.status, 200);
        let b = body(&d(acct, "ListSchemas", json!({})));
        assert_eq!(b["Schemas"].as_array().unwrap().len(), 1);
    }
}
// ---------------------------------------------------------------------------
// Security configurations
// ---------------------------------------------------------------------------

fn security_config_dict(name: &str, config: &Value, created: f64) -> Value {
    json!({
        "Name": name,
        "EncryptionConfiguration": config,
        "CreatedTimestamp": created,
        "Version": "1",
    })
}

fn create_security_configuration(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.security_configurations.contains_key(&name) {
        return Err(err(
            400,
            "AlreadyExistsException",
            &format!("SecurityConfiguration {name} already exists"),
        ));
    }
    let config = p["EncryptionConfiguration"].clone();
    let created = now_unix();
    g.security_configurations
        .insert(name.clone(), (config, created));
    Ok(ok(&json!({ "Name": name, "CreatedTimestamp": created })))
}

fn get_security_configuration(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    let (config, created) = g
        .security_configurations
        .get(&name)
        .ok_or_else(|| not_found(&format!("SecurityConfiguration {name} not found")))?;
    Ok(ok(
        &json!({ "SecurityConfiguration": security_config_dict(&name, config, *created) }),
    ))
}

fn get_security_configurations(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g
        .security_configurations
        .iter()
        .map(|(name, (config, created))| security_config_dict(name, config, *created))
        .collect();
    Ok(ok(&json!({ "SecurityConfigurations": list })))
}

fn delete_security_configuration(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.security_configurations.remove(&name).is_none() {
        return Err(not_found(&format!(
            "SecurityConfiguration {name} not found"
        )));
    }
    Ok(empty_ok())
}

// ---------------------------------------------------------------------------
// Dev endpoints
// ---------------------------------------------------------------------------

fn dev_endpoint_dict(rec: &DevEndpointRec) -> Value {
    let mut obj = json!({
        "EndpointName": rec.name,
        "RoleArn": rec.role_arn,
        "SecurityGroupIds": rec.security_group_ids,
        "SubnetId": rec.subnet_id,
        "YarnEndpointAddress": rec.yarn_endpoint_address,
        "PrivateAddress": rec.private_address,
        "ZeppelinRemoteSparkInterpreterPort": rec.zeppelin_port,
        "Status": rec.status,
        "WorkerType": rec.worker_type,
        "GlueVersion": rec.glue_version,
        "NumberOfWorkers": rec.number_of_workers,
        "NumberOfNodes": rec.number_of_nodes,
        "AvailabilityZone": rec.availability_zone,
        "VpcId": rec.vpc_id,
        "ExtraPythonLibsS3Path": rec.extra_python_libs_s3_path,
        "ExtraJarsS3Path": rec.extra_jars_s3_path,
        "CreatedTimestamp": rec.created,
        "LastModifiedTimestamp": rec.created,
        "PublicKey": rec.public_key,
        "PublicKeys": rec.public_keys,
        "SecurityConfiguration": rec.security_configuration,
        "Arguments": rec.arguments,
    });
    obj.as_object_mut().unwrap().retain(|_, v| !v.is_null());
    obj
}

fn find_dev_endpoint_mut<'a>(
    g: &'a mut GlueState,
    p: &Value,
) -> Result<&'a mut DevEndpointRec, Response> {
    let name = str_field(p, "EndpointName");
    g.dev_endpoints
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("DevEndpoint {name} not found")))
}

fn create_dev_endpoint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "EndpointName");
    if g.dev_endpoints.contains_key(&name) {
        return Err(err(
            400,
            "AlreadyExistsException",
            &format!("DevEndpoint {name} already exists"),
        ));
    }
    let rec = DevEndpointRec {
        name: name.clone(),
        role_arn: p["RoleArn"].clone(),
        security_group_ids: match p["SecurityGroupIds"].is_null() {
            true => json!([]),
            false => p["SecurityGroupIds"].clone(),
        },
        subnet_id: p["SubnetId"]
            .as_str()
            .unwrap_or("subnet-default")
            .to_string(),
        worker_type: p["WorkerType"].as_str().unwrap_or("Standard").to_string(),
        glue_version: p["GlueVersion"].as_str().unwrap_or("5.0").to_string(),
        number_of_workers: p["NumberOfWorkers"].as_u64().unwrap_or(1),
        number_of_nodes: p["NumberOfNodes"].as_u64().unwrap_or(1),
        extra_python_libs_s3_path: p["ExtraPythonLibsS3Path"].clone(),
        extra_jars_s3_path: p["ExtraJarsS3Path"].clone(),
        security_configuration: p["SecurityConfiguration"].clone(),
        arguments: match p["Arguments"].clone() {
            Value::Null => json!({}),
            v => v,
        },
        public_key: p["PublicKey"].clone(),
        public_keys: p["PublicKeys"].clone(),
        created: now_unix(),
        private_address: json!(null),
        yarn_endpoint_address: json!(null),
        zeppelin_port: 9000,
        status: "READY".to_string(),
        availability_zone: json!("us-east-1a"),
        vpc_id: json!("vpc-default"),
    };
    let dict = dev_endpoint_dict(&rec);
    g.dev_endpoints.insert(name, rec);
    Ok(ok(&dict))
}

fn get_dev_endpoint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_dev_endpoint_mut(g, p)?;
    Ok(ok(&json!({ "DevEndpoint": dev_endpoint_dict(rec) })))
}

fn get_dev_endpoints(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g.dev_endpoints.values().map(dev_endpoint_dict).collect();
    Ok(ok(&json!({ "DevEndpoints": list })))
}

fn list_dev_endpoints(g: &mut GlueState) -> Result<Response, Response> {
    let names: Vec<Value> = g.dev_endpoints.keys().map(|k| json!(k)).collect();
    Ok(ok(&json!({ "DevEndpointNames": names })))
}

fn delete_dev_endpoint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "EndpointName");
    if g.dev_endpoints.remove(&name).is_none() {
        return Err(not_found(&format!("DevEndpoint {name} not found")));
    }
    Ok(empty_ok())
}

fn update_dev_endpoint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_dev_endpoint_mut(g, p)?;
    if let Some(key) = p.get("PublicKey") {
        if !key.is_null() {
            rec.public_key = key.clone();
        }
    }
    Ok(empty_ok())
}

fn batch_get_dev_endpoints(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let wanted = str_list(&p["DevEndpointNames"]);
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for name in wanted.iter() {
        match g.dev_endpoints.get(name) {
            Some(rec) => found.push(dev_endpoint_dict(rec)),
            None => missing.push(json!(name)),
        }
    }
    let mut out = json!({ "DevEndpoints": found });
    if !missing.is_empty() {
        out["DevEndpointsNotFound"] = Value::Array(missing);
    }
    Ok(ok(&out))
}

// ---------------------------------------------------------------------------
// Interactive sessions + statements
// ---------------------------------------------------------------------------

fn session_dict(rec: &SessionRec) -> Value {
    json!({
        "Id": rec.id,
        "CreatedOn": rec.created,
        "Status": rec.state,
        "ErrorMessage": null,
        "Description": rec.description,
        "Role": rec.role,
        "Command": rec.command,
        "DefaultArguments": rec.default_arguments,
        "Connections": rec.connections,
        "Progress": 12.3,
        "MaxCapacity": 123.0,
        "SecurityConfiguration": rec.security_configuration,
        "GlueVersion": rec.glue_version,
    })
}

fn create_session(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let id = str_field(p, "Id");
    if g.sessions.contains_key(&id) {
        return Err(already_exists("Session already exists."));
    }
    let rec = SessionRec {
        id: id.clone(),
        description: p["Description"].clone(),
        role: p["Role"].clone(),
        command: p["Command"].clone(),
        timeout: p["Timeout"].clone(),
        idle_timeout: p["IdleTimeout"].clone(),
        default_arguments: p["DefaultArguments"].clone(),
        connections: p["Connections"].clone(),
        max_capacity: p["MaxCapacity"].clone(),
        number_of_workers: p["NumberOfWorkers"].clone(),
        worker_type: p["WorkerType"].clone(),
        security_configuration: p["SecurityConfiguration"].clone(),
        glue_version: p["GlueVersion"].clone(),
        tags: p["Tags"].clone(),
        request_origin: p["RequestOrigin"].clone(),
        state: "READY".to_string(),
        created: now_unix(),
        next_statement_id: 1,
        statements: BTreeMap::new(),
    };
    tag_resource(
        g,
        &make_arn(&g.scope_region, g.scope_account, "session", &id),
        p.get("Tags"),
    );
    g.sessions.insert(id, rec);
    Ok(empty_ok())
}

fn find_session_mut<'a>(g: &'a mut GlueState, p: &Value) -> Result<&'a mut SessionRec, Response> {
    // Statement ops address the session via `SessionId`; session ops via `Id`.
    let id = if p.get("SessionId").map_or(false, Value::is_string) {
        str_field(p, "SessionId")
    } else {
        str_field(p, "Id")
    };
    g.sessions
        .get_mut(&id)
        .ok_or_else(|| not_found(&format!("Session {id} not found.")))
}

fn get_session(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    Ok(ok(&json!({ "Session": session_dict(rec) })))
}

fn list_sessions(g: &mut GlueState) -> Result<Response, Response> {
    let mut ids = Vec::new();
    let mut sessions = Vec::new();
    for rec in g.sessions.values() {
        ids.push(json!(rec.id));
        sessions.push(session_dict(rec));
    }
    Ok(ok(&json!({ "Ids": ids, "Sessions": sessions })))
}

fn delete_session(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let id = str_field(p, "Id");
    if g.sessions.remove(&id).is_none() {
        return Err(not_found(&format!("Session {id} not found.")));
    }
    Ok(ok(&json!({ "Id": id })))
}

fn stop_session(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    if rec.state != "READY" {
        return Err(err(
            400,
            "IllegalSessionStateException",
            &format!("Session is in {} status", rec.state),
        ));
    }
    rec.state = "STOPPING".to_string();
    Ok(ok(&json!({ "Id": str_field(p, "Id") })))
}

fn run_statement(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    let id = rec.next_statement_id;
    rec.next_statement_id += 1;
    let code = p["Code"].as_str().unwrap_or_default().to_string();
    rec.statements.insert(
        id,
        json!({
            "Id": id,
            "Code": code,
            "State": "RUNNING",
            "Output": {},
            "Progress": 0.0,
        }),
    );
    Ok(ok(&json!({ "Id": id })))
}

fn get_statement(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    let id = p["Id"].as_u64().unwrap_or(0);
    let statement = rec
        .statements
        .get(&id)
        .ok_or_else(|| not_found(&format!("Statement {id} not found.")))?;
    let out = statement.clone();
    Ok(ok(&json!({ "Statement": out })))
}

fn list_statements(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    let list: Vec<Value> = rec.statements.values().cloned().collect();
    Ok(ok(&json!({ "Statements": list })))
}

fn cancel_statement(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_session_mut(g, p)?;
    let id = p["Id"].as_u64().unwrap_or(0);
    if let Some(stmt) = rec.statements.get_mut(&id) {
        stmt["State"] = json!("CANCELLED");
    }
    Ok(empty_ok())
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

fn connection_dict(rec: &ConnectionRec) -> Value {
    let mut obj = json!({
        "Name": rec.name,
        "Description": rec.input["Description"],
        "ConnectionProperties": rec.input["ConnectionProperties"],
        "AthenaProperties": rec.input["AthenaProperties"],
        "SparkProperties": rec.input["SparkProperties"],
        "PythonProperties": rec.input["PythonProperties"],
        "CreationTime": rec.created,
        "LastUpdatedTime": rec.updated,
        "PhysicalConnectionRequirements": rec.input["PhysicalConnectionRequirements"],
        "Status": "READY",
    });
    obj.as_object_mut().unwrap().retain(|_, v| !v.is_null());
    obj
}

fn find_connection_mut<'a>(
    g: &'a mut GlueState,
    p: &Value,
) -> Result<&'a mut ConnectionRec, Response> {
    let name = str_field(p, "Name");
    g.connections
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Connection {name} not found")))
}

fn create_connection(req: &Request, g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let input = p["ConnectionInput"].clone();
    let name = str_field(&input, "Name");
    if g.connections.contains_key(&name) {
        return Err(err(
            400,
            "AlreadyExistsException",
            &format!("Connection {name} already exists"),
        ));
    }
    let arn = make_arn(
        &g.scope_region.clone(),
        g.scope_account,
        "connection",
        &name,
    );
    tag_resource(g, &arn, p.get("Tags"));
    let now = now_unix();
    g.connections.insert(
        name,
        ConnectionRec {
            name: str_field(&input, "Name"),
            input,
            created: now,
            updated: now,
        },
    );
    let _ = req;
    Ok(ok(&json!({ "CreateConnectionStatus": "READY" })))
}

fn get_connection(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_connection_mut(g, p)?;
    Ok(ok(&json!({ "Connection": connection_dict(rec) })))
}

fn get_connections(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g.connections.values().map(connection_dict).collect();
    Ok(ok(&json!({ "ConnectionList": list })))
}

fn update_connection(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_connection_mut(g, p)?;
    let input = p["ConnectionInput"].clone();
    rec.input = input;
    rec.updated = now_unix();
    Ok(empty_ok())
}

fn delete_connection(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "ConnectionName");
    if g.connections.remove(&name).is_none() {
        return Err(not_found(&format!("Connection {name} not found")));
    }
    Ok(empty_ok())
}

fn batch_delete_connection(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let mut succeeded = Vec::new();
    let mut errors = Vec::new();
    for name in str_list(&p["ConnectionNameList"]) {
        if g.connections.remove(&name).is_some() {
            succeeded.push(json!(name));
        } else {
            errors.push(json!({
                "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Connection not found."}
            }));
        }
    }
    Ok(ok(&json!({ "Succeeded": succeeded, "Errors": errors })))
}

// ---------------------------------------------------------------------------
// Resource policies + data catalog encryption settings
// ---------------------------------------------------------------------------

fn default_catalog_arn(g: &GlueState) -> String {
    make_arn(&g.scope_region, g.scope_account, "catalog", "")
}

fn put_resource_policy(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let policy = p["PolicyInJson"].as_str().unwrap_or_default().to_string();
    let arn = p["ResourceArn"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| default_catalog_arn(g));
    let hash = md5_hash(policy.as_bytes());
    match g.resource_policies.get_mut(&arn) {
        Some(rec) => {
            rec.0 = policy;
            rec.1 = hash.clone();
            rec.3 = now_unix();
        }
        None => {
            g.resource_policies
                .insert(arn, (policy, hash.clone(), now_unix(), now_unix()));
        }
    }
    Ok(ok(&json!({ "PolicyHash": hash })))
}

fn md5_hash(data: &[u8]) -> String {
    hex::encode(md5::compute(data).0)
}

fn get_resource_policy(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let arn = p["ResourceArn"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| default_catalog_arn(g));
    match g.resource_policies.get(&arn) {
        Some((policy, hash, created, updated)) => Ok(ok(&json!({
            "PolicyInJson": policy,
            "PolicyHash": hash,
            "CreateTime": created,
            "UpdateTime": updated,
        }))),
        None => Err(not_found("Policy does not exist")),
    }
}

fn delete_resource_policy(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let arn = p["ResourceArn"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| default_catalog_arn(g));
    if g.resource_policies.remove(&arn).is_none() {
        return Err(not_found("Policy does not exist"));
    }
    Ok(empty_ok())
}

fn get_resource_policies(g: &mut GlueState) -> Result<Response, Response> {
    let mut list = Vec::new();
    for rec in g.resource_policies.values() {
        list.push(json!({
            "PolicyInJson": rec.0,
            "PolicyHash": rec.1,
            "CreateTime": rec.2,
            "UpdateTime": rec.3,
        }));
    }
    Ok(ok(&json!({ "GetResourcePoliciesResponseList": list })))
}

fn put_dces(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let catalog = p["CatalogId"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| g.scope_account.to_string());
    g.data_catalog_encryption_settings
        .insert(catalog, p["DataCatalogEncryptionSettings"].clone());
    Ok(empty_ok())
}

fn get_dces(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let catalog = p["CatalogId"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| g.scope_account.to_string());
    let stored = g.data_catalog_encryption_settings.get(&catalog);
    let err_enc = stored
        .map(|s| s["EncryptionAtRest"].clone())
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| json!({"CatalogEncryptionMode": "DISABLED"}));
    let conn_enc = stored
        .map(|s| s["ConnectionPasswordEncryption"].clone())
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| json!({"ReturnConnectionPasswordEncrypted": false}));
    Ok(ok(&json!({
        "DataCatalogEncryptionSettings": {
            "EncryptionAtRest": err_enc,
            "ConnectionPasswordEncryption": conn_enc,
        }
    })))
}

#[cfg(test)]
mod family4_tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body_val: Value) -> Request {
        Request {
            service: "glue".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body_val).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn d(acct: u64, op: &str, v: Value) -> Response {
        let mut r = req(op, v);
        r.account = acct;
        dispatch(&r).unwrap()
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn err_code(resp: &Response) -> String {
        body(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn security_configuration_crud() {
        let acct = scope();
        let r = d(
            acct,
            "CreateSecurityConfiguration",
            json!({
                "Name": "sc1",
                "EncryptionConfiguration": {"S3Encryption": [{"S3EncryptionMode": "DISABLED"}]}
            }),
        );
        assert_eq!(r.status, 200);
        assert!(body(&r)["CreatedTimestamp"].as_f64().is_some());

        let b = body(&d(acct, "GetSecurityConfiguration", json!({"Name": "sc1"})));
        assert_eq!(b["SecurityConfiguration"]["Name"], "sc1");
        assert!(b["SecurityConfiguration"]["EncryptionConfiguration"].is_object());

        let b = body(&d(acct, "GetSecurityConfigurations", json!({})));
        assert_eq!(b["SecurityConfigurations"].as_array().unwrap().len(), 1);

        d(acct, "DeleteSecurityConfiguration", json!({"Name": "sc1"}));
        assert_eq!(
            err_code(&d(acct, "GetSecurityConfiguration", json!({"Name": "sc1"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(
                acct,
                "GetSecurityConfiguration",
                json!({"Name": "nond"})
            )),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn dev_endpoint_lifecycle() {
        let acct = scope();
        let r = d(
            acct,
            "CreateDevEndpoint",
            json!({"EndpointName": "de1", "RoleArn": "arn:aws:iam::123456789012:role/r"}),
        );
        assert_eq!(r.status, 200);
        let b = body(&r);
        assert_eq!(b["EndpointName"], "de1");
        assert!(b["CreatedTimestamp"].as_f64().is_some());
        assert!(b["Arguments"].is_object());
        assert!(b["SecurityGroupIds"].is_array());

        let b = body(&d(acct, "GetDevEndpoint", json!({"EndpointName": "de1"})));
        assert_eq!(b["DevEndpoint"]["EndpointName"], "de1");
        assert_eq!(
            b["DevEndpoint"]["RoleArn"],
            "arn:aws:iam::123456789012:role/r"
        );

        let b = body(&d(acct, "GetDevEndpoints", json!({})));
        assert_eq!(b["DevEndpoints"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "ListDevEndpoints", json!({})));
        assert_eq!(b["DevEndpointNames"], json!(["de1"]));

        assert_eq!(
            err_code(&d(
                acct,
                "DeleteDevEndpoint",
                json!({"EndpointName": "nond"})
            )),
            "EntityNotFoundException"
        );

        let b = body(&d(
            acct,
            "BatchGetDevEndpoints",
            json!({"DevEndpointNames": ["de1", "zz"]}),
        ));
        assert_eq!(b["DevEndpoints"].as_array().unwrap().len(), 1);
        assert_eq!(b["DevEndpointsNotFound"].as_array().unwrap().len(), 1);

        d(acct, "DeleteDevEndpoint", json!({"EndpointName": "de1"}));
        assert_eq!(
            err_code(&d(acct, "GetDevEndpoint", json!({"EndpointName": "de1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn session_and_statements() {
        let acct = scope();
        assert_eq!(
            d(
                acct,
                "CreateSession",
                json!({"Id": "s1", "Role": "r", "Command": {"Name": "glueetl"}})
            )
            .status,
            200
        );

        let b = body(&d(acct, "GetSession", json!({"Id": "s1"})));
        assert_eq!(b["Session"]["Id"], "s1");
        assert_eq!(b["Session"]["Role"], "r");

        let b = body(&d(acct, "ListSessions", json!({})));
        assert_eq!(b["Ids"], json!(["s1"]));

        let b = body(&d(
            acct,
            "RunStatement",
            json!({"SessionId": "s1", "Code": "print(1)"}),
        ));
        let sid = b["Id"].as_u64().unwrap();

        let b = body(&d(
            acct,
            "GetStatement",
            json!({"SessionId": "s1", "Id": sid}),
        ));
        assert_eq!(b["Statement"]["Code"], "print(1)");

        let b = body(&d(acct, "ListStatements", json!({"SessionId": "s1"})));
        assert_eq!(b["Statements"].as_array().unwrap().len(), 1);

        assert_eq!(
            d(
                acct,
                "CancelStatement",
                json!({"SessionId": "s1", "Id": sid})
            )
            .status,
            200
        );

        let b = body(&d(acct, "StopSession", json!({"Id": "s1"})));
        assert_eq!(b["Id"], "s1");

        // stop a second time: still READY->STOPPING transition already applied? —
        // moto raises IllegalSessionStateException on non-READY.
        let r = d(acct, "StopSession", json!({"Id": "s1"}));
        assert_eq!(err_code(&r), "IllegalSessionStateException");

        let b = body(&d(acct, "DeleteSession", json!({"Id": "s1"})));
        assert_eq!(b["Id"], "s1");
        assert_eq!(
            err_code(&d(acct, "GetSession", json!({"Id": "s1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn connection_crud_and_batch_delete() {
        let acct = scope();
        let conn = json!({
            "Name": "c1", "ConnectionType": "JDBC",
            "ConnectionProperties": {"USERNAME": "u"}
        });
        let r = d(acct, "CreateConnection", json!({"ConnectionInput": conn}));
        assert_eq!(r.status, 200);
        assert_eq!(body(&r)["CreateConnectionStatus"], "READY");

        let b = body(&d(acct, "GetConnection", json!({"Name": "c1"})));
        assert_eq!(b["Connection"]["Name"], "c1");
        assert!(b["Connection"]["ConnectionProperties"].is_object());

        let b = body(&d(acct, "GetConnections", json!({})));
        assert_eq!(b["ConnectionList"].as_array().unwrap().len(), 1);

        d(
            acct,
            "UpdateConnection",
            json!({"Name": "c1", "ConnectionInput": {"Name": "c1",
                   "ConnectionProperties": {"USERNAME": "u2"}}}),
        );
        let b = body(&d(acct, "GetConnection", json!({"Name": "c1"})));
        assert_eq!(b["Connection"]["ConnectionProperties"]["USERNAME"], "u2");

        // duplicate
        assert_eq!(
            err_code(&d(
                acct,
                "CreateConnection",
                json!({"ConnectionInput": conn})
            )),
            "AlreadyExistsException"
        );

        // batch delete: one succeeds, one missing
        let b = body(&d(
            acct,
            "BatchDeleteConnection",
            json!({"ConnectionNameList": ["c1", "zz"]}),
        ));
        assert_eq!(b["Succeeded"].as_array().unwrap().len(), 1);
        assert_eq!(b["Errors"].as_array().unwrap().len(), 1);

        assert_eq!(
            err_code(&d(acct, "GetConnection", json!({"Name": "c1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn resource_policy_and_encryption() {
        let acct = scope();
        // No policy at first.
        assert_eq!(
            err_code(&d(acct, "GetResourcePolicy", json!({}))),
            "EntityNotFoundException"
        );

        let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let b = body(&d(
            acct,
            "PutResourcePolicy",
            json!({"PolicyInJson": policy}),
        ));
        assert!(!str_field(&b, "PolicyHash").is_empty());

        let b = body(&d(acct, "GetResourcePolicy", json!({})));
        assert_eq!(b["PolicyInJson"], policy);

        let b = body(&d(acct, "GetResourcePolicies", json!({})));
        assert_eq!(
            b["GetResourcePoliciesResponseList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        d(acct, "DeleteResourcePolicy", json!({}));
        assert_eq!(
            err_code(&d(acct, "GetResourcePolicy", json!({}))),
            "EntityNotFoundException"
        );
        // delete idempotent-style error per moto
        assert_eq!(
            err_code(&d(acct, "DeleteResourcePolicy", json!({}))),
            "EntityNotFoundException"
        );

        // data catalog encryption
        let b = body(&d(acct, "GetDataCatalogEncryptionSettings", json!({})));
        assert_eq!(
            b["DataCatalogEncryptionSettings"]["EncryptionAtRest"]["CatalogEncryptionMode"],
            "DISABLED"
        );
        d(
            acct,
            "PutDataCatalogEncryptionSettings",
            json!({"DataCatalogEncryptionSettings": {"EncryptionAtRest": {"CatalogEncryptionMode": ""}}}),
        );
        let b = body(&d(acct, "GetDataCatalogEncryptionSettings", json!({})));
        assert!(b["DataCatalogEncryptionSettings"].is_object());
    }
}
// ---------------------------------------------------------------------------
// ML transforms + task runs
// ---------------------------------------------------------------------------

fn ml_transform_dict(rec: &MlTransformRec) -> Value {
    let mut obj = json!({
        "TransformId": rec.transform_id,
        "Name": rec.name,
        "InputRecordTables": rec.input_record_tables,
        "Parameters": rec.parameters,
        "Role": rec.role,
        "Status": "NOT_READY",
        "CreatedOn": rec.created,
        "LastModifiedOn": rec.modified,
        "EvaluationMetrics": {},
        "Schema": [],
        "TransformEncryption": {},
    });
    let base = obj.as_object_mut().unwrap();
    if let Some(d) = rec.description.as_str() {
        if !d.is_empty() {
            base.insert("Description".into(), json!(d));
        }
    }
    for (member, value) in [
        ("GlueVersion", rec.glue_version.clone()),
        ("MaxCapacity", rec.max_capacity.clone()),
        ("WorkerType", rec.worker_type.clone()),
        ("NumberOfWorkers", rec.number_of_workers.clone()),
        ("Timeout", rec.timeout.clone()),
        ("MaxRetries", rec.max_retries.clone()),
    ] {
        if !value.is_null() {
            base.insert(member.to_string(), value);
        }
    }
    obj
}

fn find_ml_transform_mut<'a>(
    g: &'a mut GlueState,
    p: &Value,
) -> Result<&'a mut MlTransformRec, Response> {
    let id = str_field(p, "TransformId");
    g.ml_transforms
        .get_mut(&id)
        .ok_or_else(|| not_found(&format!("ML Task {id} not found.")))
}

fn create_ml_transform(req: &Request, g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let transform_id = format!("tfm_{}", hex_n(32));
    let name = str_field(p, "Name");
    let (region, account) = (g.scope_region.clone(), g.scope_account);
    tag_resource(
        g,
        &make_arn(&region, account, "mlTransform", &transform_id),
        p.get("Tags"),
    );
    g.ml_transforms.insert(
        transform_id.clone(),
        MlTransformRec {
            transform_id: transform_id.clone(),
            name,
            input_record_tables: p["InputRecordTables"].clone(),
            parameters: match p["Parameters"].clone() {
                Value::Null => json!({}),
                v => v,
            },
            role: p["Role"].clone(),
            description: p["Description"].clone(),
            glue_version: p["GlueVersion"].clone(),
            max_capacity: p["MaxCapacity"].clone(),
            worker_type: p["WorkerType"].clone(),
            number_of_workers: p["NumberOfWorkers"].clone(),
            timeout: p["Timeout"].clone(),
            max_retries: p["MaxRetries"].clone(),
            created: now_unix(),
            modified: now_unix(),
        },
    );
    Ok(ok(&json!({ "TransformId": transform_id })))
}

fn get_ml_transform(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_ml_transform_mut(g, p)?;
    Ok(ok(&ml_transform_dict(rec)))
}

fn get_ml_transforms(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g.ml_transforms.values().map(ml_transform_dict).collect();
    Ok(ok(&json!({ "Transforms": list })))
}

fn list_ml_transforms(g: &mut GlueState) -> Result<Response, Response> {
    let ids: Vec<Value> = g
        .ml_transforms
        .values()
        .map(|t| json!(t.transform_id))
        .collect();
    Ok(ok(&json!({ "TransformIds": ids })))
}

fn delete_ml_transform(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let id = str_field(p, "TransformId");
    if g.ml_transforms.remove(&id).is_none() {
        return Err(not_found(&format!("ML Task {id} not found.")));
    }
    Ok(ok(&json!({ "TransformId": id })))
}

fn update_ml_transform(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let id = str_field(p, "TransformId");
    let rec = find_ml_transform_mut(g, p)?;
    if let Some(u) = p.as_object() {
        for (member, field) in [
            ("Name", 0usize),
            ("Description", 1),
            ("Parameters", 2),
            ("Role", 3),
            ("GlueVersion", 4),
            ("MaxCapacity", 5),
            ("WorkerType", 6),
            ("NumberOfWorkers", 7),
            ("Timeout", 8),
            ("MaxRetries", 9),
        ] {
            if let Some(v) = u.get(member) {
                if !v.is_null() {
                    match field {
                        0 => rec.name = str_field(v, ""),
                        1 => rec.description = v.clone(),
                        2 => rec.parameters = v.clone(),
                        3 => rec.role = v.clone(),
                        4 => rec.glue_version = v.clone(),
                        5 => rec.max_capacity = v.clone(),
                        6 => rec.worker_type = v.clone(),
                        7 => rec.number_of_workers = v.clone(),
                        8 => rec.timeout = v.clone(),
                        _ => rec.max_retries = v.clone(),
                    }
                }
            }
        }
    }
    rec.modified = now_unix();
    Ok(ok(&json!({ "TransformId": id })))
}

fn get_ml_task_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let id = str_field(p, "TransformId");
    let _ = find_ml_transform_mut(g, p)?;
    Err(not_found(&format!(
        "MLTaskRun {} not found.",
        str_field(p, "TaskRunId")
    )))
    .map_err(|resp| {
        let _ = id;
        resp
    })
}

fn get_ml_task_runs(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let _ = find_ml_transform_mut(g, p)?;
    Ok(ok(&json!({ "TaskRuns": [] })))
}

fn cancel_ml_task_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let _ = find_ml_transform_mut(g, p)?;
    Err(not_found(&format!(
        "MLTaskRun {} not found.",
        str_field(p, "TaskRunId")
    )))
}

/// The start_* ML task runs: validate the transform, return a TaskRunId.
fn start_ml_eval_task_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let _ = find_ml_transform_mut(g, p)?;
    Ok(ok(&json!({ "TaskRunId": format!("tr_{}", hex_n(16)) })))
}

// ---------------------------------------------------------------------------
// Blueprints + runs
// ---------------------------------------------------------------------------

fn blueprint_dict(rec: &BlueprintRec) -> Value {
    let mut obj = json!({
        "Name": rec.name,
        "BlueprintLocation": rec.blueprint_location,
        "Status": "ACTIVE",
        "CreatedOn": now_unix(),
        "LastModifiedOn": now_unix(),
    });
    if let Some(d) = rec.description.as_str() {
        if !d.is_empty() {
            obj["Description"] = json!(d);
        }
    }
    obj
}

fn find_blueprint_mut<'a>(
    g: &'a mut GlueState,
    p: &Value,
) -> Result<&'a mut BlueprintRec, Response> {
    let name = str_field(p, "Name");
    g.blueprints
        .get_mut(&name)
        .ok_or_else(|| not_found(&format!("Blueprint {name} not found.")))
}

fn create_blueprint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.blueprints.contains_key(&name) {
        return Err(already_exists("Blueprint already exists."));
    }
    g.blueprints.insert(
        name.clone(),
        BlueprintRec {
            name: name.clone(),
            blueprint_location: str_field(p, "BlueprintLocation"),
            description: p["Description"].clone(),
            created: now_unix(),
        },
    );
    Ok(ok(&json!({ "Name": name })))
}

fn get_blueprint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let rec = find_blueprint_mut(g, p)?;
    Ok(ok(&json!({ "Blueprint": blueprint_dict(rec) })))
}

fn list_blueprints(g: &mut GlueState) -> Result<Response, Response> {
    let names: Vec<Value> = g.blueprints.keys().map(|k| json!(k)).collect();
    Ok(ok(&json!({ "Blueprints": names })))
}

fn delete_blueprint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.blueprints.remove(&name).is_none() {
        return Err(not_found(&format!("Blueprint {name} not found.")));
    }
    Ok(empty_ok())
}

fn update_blueprint(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = p["Name"].as_str().unwrap_or_default().to_string();
    match g.blueprints.get_mut(&name) {
        Some(rec) => {
            if let Some(loc) = p["BlueprintLocation"].as_str() {
                rec.blueprint_location = loc.to_string();
            }
            if let Some(d) = p.get("Description") {
                if !d.is_null() {
                    rec.description = d.clone();
                }
            }
            Ok(ok(&json!({ "Name": name })))
        }
        None => Err(not_found(&format!("Blueprint {name} not found."))),
    }
}

fn batch_get_blueprints(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let wanted = str_list(&p["Names"]);
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for name in wanted.iter() {
        match g.blueprints.get(name) {
            Some(rec) => found.push(blueprint_dict(rec)),
            None => missing.push(json!(name)),
        }
    }
    let mut out = json!({ "Blueprints": found });
    if !missing.is_empty() {
        out["MissingBlueprints"] = Value::Array(missing);
    }
    Ok(ok(&out))
}

fn start_blueprint_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "BlueprintName");
    if !g.blueprints.contains_key(&name) {
        return Err(not_found(&format!("Blueprint {name} not found.")));
    }
    let run_id = format!("bp_{}", hex_n(32));
    g.blueprint_runs.insert(
        run_id.clone(),
        json!({
            "BlueprintName": name,
            "RunId": run_id,
            "State": "RUNNING",
            "StartedOn": now_iso(),
            "RoleArn": p["RoleArn"],
            "Parameters": p["Parameters"],
        }),
    );
    Ok(ok(&json!({ "RunId": run_id })))
}

fn get_blueprint_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "BlueprintName");
    if !g.blueprints.contains_key(&name) {
        return Err(not_found(&format!("Blueprint {name} not found.")));
    }
    let run_id = str_field(p, "RunId");
    g.blueprint_runs
        .get(&run_id)
        .cloned()
        .map(|run| ok(&json!({ "BlueprintRun": run })))
        .ok_or_else(|| not_found(&format!("BlueprintRun {run_id} not found.")))
}

fn get_blueprint_runs(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "BlueprintName");
    if !g.blueprints.contains_key(&name) {
        return Err(not_found(&format!("Blueprint {name} not found.")));
    }
    let list: Vec<Value> = g
        .blueprint_runs
        .values()
        .filter(|r| str_field(r, "BlueprintName") == name)
        .cloned()
        .collect();
    Ok(ok(&json!({ "BlueprintRuns": list })))
}

// ---------------------------------------------------------------------------
// Data quality rulesets + runs + results
// ---------------------------------------------------------------------------

fn dq_ruleset_dict(rec: &(Value, f64)) -> Value {
    let mut obj = json!({
        "Name": rec.0["Name"],
        "Ruleset": rec.0["Ruleset"],
        "CreatedOn": rec.1,
        "LastModifiedOn": rec.1,
        "Description": rec.0["Description"],
        "TargetTable": rec.0["TargetTable"],
    });
    obj.as_object_mut().unwrap().retain(|_, v| !v.is_null());
    obj
}

fn create_dq_ruleset(req: &Request, g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    let created = now_unix();
    g.data_quality_rulesets.insert(
        name.clone(),
        (
            json!({
                "Name": name,
                "Ruleset": p["Ruleset"],
                "Description": p["Description"],
                "TargetTable": p["TargetTable"],
            }),
            created,
        ),
    );
    Ok(ok(&json!({ "Name": name })))
}

fn get_dq_ruleset(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    match g.data_quality_rulesets.get(&name) {
        Some(rec) => Ok(ok(&dq_ruleset_dict(rec))),
        None => Err(not_found(&format!("Ruleset {name} not found."))),
    }
}

fn list_dq_rulesets(g: &mut GlueState) -> Result<Response, Response> {
    let list: Vec<Value> = g
        .data_quality_rulesets
        .values()
        .map(dq_ruleset_dict)
        .collect();
    Ok(ok(&json!({ "Rulesets": list })))
}

fn delete_dq_ruleset(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    if g.data_quality_rulesets.remove(&name).is_none() {
        return Err(not_found(&format!("Ruleset {name} not found.")));
    }
    Ok(empty_ok())
}

fn update_dq_ruleset(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let name = str_field(p, "Name");
    match g.data_quality_rulesets.get_mut(&name) {
        Some((rec, _)) => {
            if let Some(v) = p.get("Ruleset") {
                if !v.is_null() {
                    rec["Ruleset"] = v.clone();
                }
            }
            if let Some(v) = p.get("Description") {
                if !v.is_null() {
                    rec["Description"] = v.clone();
                }
            }
            Ok(ok(&json!({ "Name": name })))
        }
        None => Err(not_found(&format!("Ruleset {name} not found."))),
    }
}

fn start_dq_recommendation_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let run_id = format!("dqrec_{}", hex_n(32));
    g.dq_recommendation_runs.insert(
        run_id.clone(),
        json!({
            "RunId": run_id,
            "DataSource": p["DataSource"],
            "Role": p["Role"],
            "Status": "RUNNING",
            "NumberOfWorkers": p["NumberOfWorkers"],
            "Timeout": p["Timeout"],
            "CreatedRulesetName": p["CreatedRulesetName"],
        }),
    );
    Ok(ok(&json!({ "RunId": run_id })))
}

fn get_dq_recommendation_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let run_id = str_field(p, "RunId");
    match g.dq_recommendation_runs.get(&run_id) {
        Some(run) => Ok(ok(run)),
        None => Err(not_found(&format!(
            "DataQualityRuleRecommendationRun {run_id} not found."
        ))),
    }
}

fn start_dq_evaluation_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let run_id = format!("dqeval_{}", hex_n(32));
    g.dq_evaluation_runs.insert(
        run_id.clone(),
        json!({
            "RunId": run_id,
            "DataSource": p["DataSource"],
            "Role": p["Role"],
            "RulesetNames": p["RulesetNames"],
            "Status": "RUNNING",
            "AdditionalRunOptions": {},
            "ResultIds": [],
            "AdditionalDataSources": p["AdditionalDataSources"],
            "NumberOfWorkers": p["NumberOfWorkers"],
            "Timeout": p["Timeout"],
        }),
    );
    Ok(ok(&json!({ "RunId": run_id })))
}

fn get_dq_evaluation_run(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let run_id = str_field(p, "RunId");
    match g.dq_evaluation_runs.get(&run_id) {
        Some(run) => Ok(ok(run)),
        None => Err(not_found(&format!(
            "DataQualityRulesetEvaluationRun {run_id} not found."
        ))),
    }
}

fn get_dq_result(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let result_id = str_field(p, "ResultId");
    Err(not_found(&format!(
        "DataQualityResult {result_id} not found."
    )))
}

fn batch_get_dq_result(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    Ok(ok(&json!({
        "Results": [],
        "ResultsNotFound": p["ResultIds"],
    })))
}

fn get_dq_model(g: &mut GlueState, p: &Value) -> Result<Response, Response> {
    let profile_id = str_field(p, "ProfileId");
    Err(not_found(&format!(
        "DataQualityModel for profile {profile_id} not found."
    )))
}

fn recommendations_list(g: &mut GlueState) -> Result<Response, Response> {
    Ok(ok(&json!({
        "Runs": g.dq_recommendation_runs.values().cloned().collect::<Vec<_>>(),
    })))
}

fn evaluations_list(g: &mut GlueState) -> Result<Response, Response> {
    Ok(ok(&json!({
        "Runs": g.dq_evaluation_runs.values().cloned().collect::<Vec<_>>(),
    })))
}

#[cfg(test)]
mod family5_tests {
    use super::*;
    use bytes::Bytes;

    fn req(op: &str, body_val: Value) -> Request {
        Request {
            service: "glue".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body_val).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn d(acct: u64, op: &str, v: Value) -> Response {
        let mut r = req(op, v);
        r.account = acct;
        dispatch(&r).unwrap()
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn err_code(resp: &Response) -> String {
        body(resp)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn ml_transform_lifecycle() {
        let acct = scope();
        let b = body(&d(
            acct,
            "CreateMLTransform",
            json!({
                "Name": "ml1",
                "InputRecordTables": [{"DatabaseName": "db", "TableName": "t"}],
                "Parameters": {"TransformType": "FIND_MATCHES"},
                "Role": "r"
            }),
        ));
        let tid = str_field(&b, "TransformId");
        assert!(tid.starts_with("tfm_"));

        let b = body(&d(
            acct,
            "GetMLTransform",
            json!({"TransformId": tid.clone()}),
        ));
        assert_eq!(b["TransformId"], tid);
        assert_eq!(b["Name"], "ml1");
        assert!(b["EvaluationMetrics"].is_object());
        assert!(b["Schema"].is_array());
        assert!(b["TransformEncryption"].is_object());

        let b = body(&d(acct, "GetMLTransforms", json!({})));
        assert_eq!(b["Transforms"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "ListMLTransforms", json!({})));
        assert_eq!(b["TransformIds"].as_array().unwrap()[0], tid);

        let b = body(&d(
            acct,
            "UpdateMLTransform",
            json!({"TransformId": tid.clone(), "Description": "upd"}),
        ));
        assert_eq!(b["TransformId"], tid);
        let b = body(&d(
            acct,
            "GetMLTransform",
            json!({"TransformId": tid.clone()}),
        ));
        assert_eq!(b["Description"], "upd");

        // task-run family
        assert_eq!(
            err_code(&d(
                acct,
                "GetMLTaskRun",
                json!({"TransformId": tid.clone(), "TaskRunId": "tr_x"})
            )),
            "EntityNotFoundException"
        );
        let b = body(&d(
            acct,
            "GetMLTaskRuns",
            json!({"TransformId": tid.clone()}),
        ));
        assert_eq!(b["TaskRuns"], json!([]));

        assert_eq!(
            err_code(&d(
                acct,
                "CancelMLTaskRun",
                json!({"TransformId": "bad", "TaskRunId": "x"})
            )),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(
                acct,
                "StartMLEvaluationTaskRun",
                json!({"TransformId": "bad"})
            )),
            "EntityNotFoundException"
        );

        let b = body(&d(
            acct,
            "DeleteMLTransform",
            json!({"TransformId": tid.clone()}),
        ));
        assert_eq!(b["TransformId"], tid);
        assert_eq!(
            err_code(&d(acct, "GetMLTransform", json!({"TransformId": tid}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn blueprint_lifecycle_and_runs() {
        let acct = scope();
        let b = body(&d(
            acct,
            "CreateBlueprint",
            json!({"Name": "bp1", "BlueprintLocation": "s3://x"}),
        ));
        assert_eq!(b["Name"], "bp1");

        let b = body(&d(acct, "GetBlueprint", json!({"Name": "bp1"})));
        assert_eq!(b["Blueprint"]["Name"], "bp1");
        assert_eq!(b["Blueprint"]["Status"], "ACTIVE");

        let b = body(&d(acct, "ListBlueprints", json!({})));
        assert_eq!(b["Blueprints"], json!(["bp1"]));

        let b = body(&d(
            acct,
            "StartBlueprintRun",
            json!({"BlueprintName": "bp1", "RoleArn": "r"}),
        ));
        let run_id = str_field(&b, "RunId");
        assert!(run_id.starts_with("bp_"));

        let b = body(&d(
            acct,
            "GetBlueprintRun",
            json!({"BlueprintName": "bp1", "RunId": run_id.clone()}),
        ));
        assert_eq!(b["BlueprintRun"]["State"], "RUNNING");

        let b = body(&d(
            acct,
            "GetBlueprintRuns",
            json!({"BlueprintName": "bp1"}),
        ));
        assert_eq!(b["BlueprintRuns"].as_array().unwrap().len(), 1);

        let b = body(&d(
            acct,
            "BatchGetBlueprints",
            json!({"Names": ["bp1", "z"]}),
        ));
        assert_eq!(b["Blueprints"].as_array().unwrap().len(), 1);
        assert_eq!(b["MissingBlueprints"].as_array().unwrap().len(), 1);

        assert_eq!(
            err_code(&d(
                acct,
                "GetBlueprintRun",
                json!({"BlueprintName": "nond", "RunId": run_id})
            )),
            "EntityNotFoundException"
        );

        let b = body(&d(
            acct,
            "UpdateBlueprint",
            json!({"Name": "bp1", "BlueprintLocation": "s3://v2"}),
        ));
        assert_eq!(b["Name"], "bp1");

        d(acct, "DeleteBlueprint", json!({"Name": "bp1"}));
        assert_eq!(
            err_code(&d(acct, "GetBlueprint", json!({"Name": "bp1"}))),
            "EntityNotFoundException"
        );
    }

    #[test]
    fn dq_ruleset_and_runs() {
        let acct = scope();
        let b = body(&d(
            acct,
            "CreateDataQualityRuleset",
            json!({"Name": "dqr1", "Ruleset": "Rules = []"}),
        ));
        assert_eq!(b["Name"], "dqr1");

        let b = body(&d(acct, "GetDataQualityRuleset", json!({"Name": "dqr1"})));
        assert_eq!(b["Ruleset"], "Rules = []");

        let b = body(&d(acct, "ListDataQualityRulesets", json!({})));
        assert_eq!(b["Rulesets"].as_array().unwrap().len(), 1);

        let b = body(&d(
            acct,
            "UpdateDataQualityRuleset",
            json!({"Name": "dqr1", "Ruleset": "Rules = [x]"}),
        ));
        assert_eq!(b["Name"], "dqr1");

        // runs
        let b = body(&d(
            acct,
            "StartDataQualityRuleRecommendationRun",
            json!({"DataSource": {"GlueTable": {"DatabaseName": "d", "TableName": "t"}}, "Role": "r"}),
        ));
        let rec_run = str_field(&b, "RunId");
        assert!(rec_run.starts_with("dqrec_"));

        let b = body(&d(
            acct,
            "GetDataQualityRuleRecommendationRun",
            json!({"RunId": rec_run.clone()}),
        ));
        assert_eq!(b["RunId"], rec_run);
        assert!(b["DataSource"].is_object());

        let b = body(&d(
            acct,
            "StartDataQualityRulesetEvaluationRun",
            json!({"DataSource": {}, "Role": "r", "RulesetNames": ["r"]}),
        ));
        let eval_run = str_field(&b, "RunId");
        let b = body(&d(
            acct,
            "GetDataQualityRulesetEvaluationRun",
            json!({"RunId": eval_run}),
        ));
        assert_eq!(b["Status"], "RUNNING");
        assert!(b["AdditionalRunOptions"].is_object());
        assert!(b["ResultIds"].is_array());

        // cancels are no-ops (200) even for fake ids
        assert_eq!(
            d(
                acct,
                "CancelDataQualityRuleRecommendationRun",
                json!({"RunId": "fake"})
            )
            .status,
            200
        );
        assert_eq!(
            d(
                acct,
                "CancelDataQualityRulesetEvaluationRun",
                json!({"RunId": "f"})
            )
            .status,
            200
        );

        // lists
        let b = body(&d(acct, "ListDataQualityRuleRecommendationRuns", json!({})));
        assert_eq!(b["Runs"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "ListDataQualityRulesetEvaluationRuns", json!({})));
        assert_eq!(b["Runs"].as_array().unwrap().len(), 1);
        let b = body(&d(acct, "ListDataQualityResults", json!({})));
        assert_eq!(b["Results"], json!([]));

        // batch get + model errors
        let b = body(&d(
            acct,
            "BatchGetDataQualityResult",
            json!({"ResultIds": ["x"]}),
        ));
        assert_eq!(b["ResultsNotFound"].as_array().unwrap().len(), 1);
        assert_eq!(
            err_code(&d(acct, "GetDataQualityResult", json!({"ResultId": "x"}))),
            "EntityNotFoundException"
        );
        assert_eq!(
            err_code(&d(
                acct,
                "GetDataQualityModel",
                json!({"ProfileId": "p", "StatisticId": "s"})
            )),
            "EntityNotFoundException"
        );

        assert_eq!(
            err_code(&d(acct, "GetDataQualityRuleset", json!({"Name": "zz"}))),
            "EntityNotFoundException"
        );

        d(acct, "DeleteDataQualityRuleset", json!({"Name": "dqr1"}));
        assert_eq!(
            err_code(&d(acct, "GetDataQualityRuleset", json!({"Name": "dqr1"}))),
            "EntityNotFoundException"
        );
    }
}
