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

use crate::proto::{now_unix, parse_json_body};
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
    allocated_capacity: Value,
    max_capacity: Value,
    timeout: Value,
    worker_type: Value,
    number_of_workers: Value,
    security_configuration: Value,
    notification_property: Value,
}

#[derive(Default)]
struct JobRec {
    /// Extracted job attributes (moto's FakeJob fields). Null members are
    /// omitted when the job is serialized.
    job: Value,
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
    description: Value,
    tags: Value,
    created: f64,
    updated: f64,
    schemas: BTreeMap<String, SchemaRec>,
}

#[derive(Clone, Default)]
struct SchemaRec {
    registry_name: String,
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
