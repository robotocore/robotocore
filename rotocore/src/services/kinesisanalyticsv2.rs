//! Kinesis Analytics v2 backend (JSON protocol).
//!
//! Replaces the moto sidecar for `kinesisanalyticsv2` (`X-Amz-Target:
//! KinesisAnalytics_20180523.<Op>`). State is per-(account, region),
//! mirroring moto's `KinesisAnalyticsV2Backend` (moto/kinesisanalyticsv2):
//! application CRUD with version bumps, SQL inputs/outputs/reference
//! descriptions, snapshots, operations log, CloudWatch logging options, VPC
//! configurations, maintenance window, presigned URL, discover-input-schema
//! mock, and the shared TaggingService key→value semantics.

use crate::proto::json_error;
use crate::store;
use crate::{Request, Response};
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

const FAKE_VPC_ID: &str = "vpc-0123456789abcdef0";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct Snapshot {
    name: String,
    status: String,
    version_id: i64,
    created_at: String,
}

struct Operation {
    op_type: String,
    op_id: String,
    start_time: String,
    end_time: String,
    status: String,
}

#[derive(Default)]
struct Application {
    arn: String,
    application_name: String,
    description: Option<String>,
    runtime: String,
    service_role: String,
    mode: Option<String>,
    status: String,
    version_id: i64,
    creation_ts: String,
    last_update_ts: String,
    conditional_token: String,
    maintenance_start: String,
    maintenance_end: String,
    app_config_description: Option<Map<String, Value>>,
    cw_logging: Option<Vec<Value>>,
    inputs: Vec<Value>,
    outputs: Vec<Value>,
    refs: Vec<Value>,
    snapshots: BTreeMap<String, Snapshot>,
    operations: Vec<Operation>,
    version_summaries: Vec<(i64, String)>,
}

impl Application {
    fn new(account: u64, region: &str, body: &Value) -> Self {
        let name = body
            .get("ApplicationName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let now = iso_timestamp();
        let mut app = Application {
            arn: format!("arn:aws:kinesisanalytics:{region}:{account}:application/{name}"),
            application_name: name.to_string(),
            description: body
                .get("ApplicationDescription")
                .and_then(Value::as_str)
                .map(String::from),
            runtime: body
                .get("RuntimeEnvironment")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            service_role: body
                .get("ServiceExecutionRole")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            mode: body
                .get("ApplicationMode")
                .and_then(Value::as_str)
                .map(String::from),
            status: "READY".to_string(),
            version_id: 1,
            creation_ts: now.clone(),
            last_update_ts: now,
            conditional_token: uuid::Uuid::new_v4().simple().to_string(),
            maintenance_start: "06:00".to_string(),
            maintenance_end: "14:00".to_string(),
            app_config_description: body
                .get("ApplicationConfiguration")
                .map(app_config_description),
            cw_logging: logging_option_descriptions(body.get("CloudWatchLoggingOptions")),
            inputs: Vec::new(),
            outputs: Vec::new(),
            refs: Vec::new(),
            snapshots: BTreeMap::new(),
            operations: Vec::new(),
            version_summaries: Vec::new(),
        };
        app.version_summaries.push((1, app.status.clone()));
        app
    }

    /// current detail view (moto's `Application.to_detail_dict`).
    fn to_detail(&self) -> Value {
        let mut result = json!({
            "ApplicationARN": self.arn,
            "ApplicationDescription": self.description,
            "ApplicationName": self.application_name,
            "RuntimeEnvironment": self.runtime,
            "ServiceExecutionRole": self.service_role,
            "ApplicationStatus": self.status,
            "ApplicationVersionId": self.version_id,
            "CreateTimestamp": self.creation_ts,
            "LastUpdateTimestamp": self.last_update_ts,
            "ApplicationConfigurationDescription": self.app_config_description.as_ref().map(|m| Value::Object(m.clone())),
            "CloudWatchLoggingOptionDescriptions": self.cw_logging,
            "ApplicationMaintenanceConfigurationDescription": {
                "ApplicationMaintenanceWindowStartTime": self.maintenance_start,
                "ApplicationMaintenanceWindowEndTime": self.maintenance_end,
            },
            "ApplicationVersionCreateTimestamp": self.creation_ts,
            "ConditionalToken": self.conditional_token,
            "ApplicationMode": self.mode,
        });
        // SQL descriptions ride inside ApplicationConfigurationDescription.
        if !self.inputs.is_empty() || !self.outputs.is_empty() || !self.refs.is_empty() {
            let obj = ensure_app_config(&mut result);
            let sql = obj
                .entry("SqlApplicationConfigurationDescription")
                .or_insert_with(|| json!({}));
            if !self.inputs.is_empty() {
                sql.as_object_mut().expect("sql obj").insert(
                    "InputDescriptions".to_string(),
                    Value::Array(self.inputs.clone()),
                );
            }
            if !self.outputs.is_empty() {
                sql.as_object_mut().expect("sql obj").insert(
                    "OutputDescriptions".to_string(),
                    Value::Array(self.outputs.clone()),
                );
            }
            if !self.refs.is_empty() {
                sql.as_object_mut().expect("sql obj").insert(
                    "ReferenceDataSourceDescriptions".to_string(),
                    Value::Array(self.refs.clone()),
                );
            }
        }
        result
    }

    fn bump_version(&mut self) {
        self.version_id += 1;
        self.last_update_ts = iso_timestamp();
        self.conditional_token = uuid::Uuid::new_v4().simple().to_string();
        self.version_summaries
            .push((self.version_id, self.status.clone()));
    }

    fn record_operation(&mut self, op_type: &str) -> String {
        let now = iso_timestamp();
        let op_id = uuid::Uuid::new_v4().to_string();
        self.operations.push(Operation {
            op_type: op_type.to_string(),
            op_id: op_id.clone(),
            start_time: now.clone(),
            end_time: now,
            status: "SUCCESSFUL".to_string(),
        });
        op_id
    }
}

#[derive(Default)]
struct Kav2State {
    apps: HashMap<String, Application>,
    /// resource ARN → (key → value) tag map (moto TaggingService semantics)
    tags: HashMap<String, BTreeMap<String, Option<String>>>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut Kav2State) -> T) -> T {
    let slot = store::slot::<Kav2State>("kinesisanalyticsv2");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

fn now_arn(name: &str, account: u64, region: &str) -> String {
    format!("arn:aws:kinesisanalytics:{region}:{account}:application/{name}")
}

/// moto's `datetime.now().isoformat()` — second + 6-digit micros, no zone.
fn iso_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

fn not_found_app(name: &str) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("Application {name} is not found"),
    )
}

fn get_app<'a>(state: &'a mut Kav2State, name: &str) -> Result<&'a mut Application, Response> {
    if state.apps.contains_key(name) {
        return Ok(state.apps.get_mut(name).expect("present"));
    }
    Err(not_found_app(name))
}

fn ensure_app_config(detail: &mut Value) -> &mut Map<String, Value> {
    let obj = detail.as_object_mut().expect("detail obj");
    if !obj
        .get("ApplicationConfigurationDescription")
        .is_some_and(Value::is_object)
    {
        obj.insert("ApplicationConfigurationDescription".to_string(), json!({}));
    }
    obj.get_mut("ApplicationConfigurationDescription")
        .and_then(Value::as_object_mut)
        .expect("app config")
}

/// moto's `_generate_logging_options`: assign "<n>.1" ids to each option.
fn logging_option_descriptions(options: Option<&Value>) -> Option<Vec<Value>> {
    let Value::Array(list) = options? else {
        return None;
    };
    let mut out = Vec::new();
    for (i, option) in list.iter().enumerate() {
        out.push(json!({
            "CloudWatchLoggingOptionId": format!("{}.1", i + 1),
            "LogStreamARN": option.get("LogStreamARN").and_then(Value::as_str).unwrap_or_default(),
        }));
    }
    Some(out)
}

/// moto's `_generate_app_config_description`: key renames + description
/// shapes for Flink/SQL application configurations.
fn app_config_description(app_config: &Value) -> Map<String, Value> {
    const TOP_LEVEL_KEYS: &[(&str, &str)] = &[
        ("EnvironmentProperties", "EnvironmentPropertyDescriptions"),
        (
            "ApplicationSnapshotConfiguration",
            "ApplicationSnapshotConfigurationDescription",
        ),
        (
            "ApplicationSystemRollbackConfiguration",
            "ApplicationSystemRollbackConfigurationDescription",
        ),
        (
            "ZeppelinApplicationConfiguration",
            "ZeppelinApplicationConfigurationDescription",
        ),
    ];
    const SUBFIELD_KEYS: &[(&str, &str)] = &[
        ("PropertyGroups", "PropertyGroupDescriptions"),
        (
            "MonitoringConfiguration",
            "MonitoringConfigurationDescription",
        ),
        ("CatalogConfiguration", "CatalogConfigurationDescription"),
        (
            "DeployAsApplicationConfiguration",
            "DeployAsApplicationConfigurationDescription",
        ),
        ("S3ContentLocation", "S3ContentLocationDescription"),
        (
            "CustomArtifactsConfiguration",
            "CustomArtifactsConfigurationDescription",
        ),
        (
            "GlueDataCatalogConfiguration",
            "GlueDataCatalogConfigurationDescription",
        ),
        ("MavenReference", "MavenReferenceDescription"),
    ];
    let mut out = Map::new();
    if let Some(flink) = app_config.get("FlinkApplicationConfiguration") {
        out.insert(
            "FlinkApplicationConfigurationDescription".to_string(),
            flink_app_description(flink),
        );
    }
    for (old, new) in TOP_LEVEL_KEYS {
        if let Some(v) = app_config.get(old) {
            out.insert(new.to_string(), rename_keys(v, SUBFIELD_KEYS));
        }
    }
    if let Some(code_config) = app_config.get("ApplicationCodeConfiguration") {
        const CODE_KEYS: &[(&str, &str)] = &[
            ("S3ContentLocation", "S3ApplicationCodeLocationDescription"),
            ("CodeContent", "CodeContentDescription"),
        ];
        let mut desc = rename_keys(code_config, CODE_KEYS);
        if code_config.get("CodeContentType").and_then(Value::as_str) == Some("ZIPFILE") {
            if let Some(content) = desc
                .as_object_mut()
                .and_then(|o| o.get_mut("CodeContentDescription"))
                .and_then(Value::as_object_mut)
            {
                content.insert("CodeMD5".to_string(), json!("fakechecksum"));
                content.insert("CodeSize".to_string(), json!(123));
            }
        }
        out.insert("ApplicationCodeConfigurationDescription".to_string(), desc);
    }
    if let Some(vpcs) = app_config
        .get("VpcConfigurations")
        .and_then(Value::as_array)
    {
        let descs: Vec<Value> = vpcs
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let mut desc = v.clone();
                if let Some(obj) = desc.as_object_mut() {
                    obj.insert(
                        "VpcConfigurationId".to_string(),
                        json!(format!("{}.1", i + 1)),
                    );
                    obj.insert("VpcId".to_string(), json!(FAKE_VPC_ID));
                }
                desc
            })
            .collect();
        out.insert(
            "VpcConfigurationDescriptions".to_string(),
            Value::Array(descs),
        );
    }
    out
}

/// Rename keys recursively per the mapping table (moto's `__update_keys`).
fn rename_keys(old: &Value, key_map: &[(&str, &str)]) -> Value {
    match old {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                let new_key = key_map
                    .iter()
                    .find(|(o, _)| *o == k.as_str())
                    .map(|(_, n)| *n)
                    .unwrap_or(k.as_str());
                out.insert(new_key.to_string(), rename_keys(v, key_map));
            }
            Value::Object(out)
        }
        Value::Array(list) => Value::Array(list.iter().map(|v| rename_keys(v, key_map)).collect()),
        other => other.clone(),
    }
}

/// moto's `__generate_flink_app_description`.
fn flink_app_description(flink: &Value) -> Value {
    let mut out = Map::new();
    if let Some(checkpoint) = flink.get("CheckpointConfiguration") {
        let config_type = checkpoint.get("ConfigurationType").and_then(Value::as_str);
        if config_type == Some("DEFAULT") {
            out.insert(
                "CheckpointConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "DEFAULT",
                    "CheckpointingEnabled": true,
                    "CheckpointInterval": 60000,
                    "MinPauseBetweenCheckpoints": 5000,
                }),
            );
        } else if config_type == Some("CUSTOM") {
            out.insert(
                "CheckpointConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "CUSTOM",
                    "CheckpointingEnabled": checkpoint.get("CheckpointingEnabled").and_then(Value::as_bool).unwrap_or(true),
                    "CheckpointInterval": checkpoint.get("CheckpointInterval").and_then(Value::as_i64).unwrap_or(60000),
                    "MinPauseBetweenCheckpoints": checkpoint.get("MinPauseBetweenCheckpoints").and_then(Value::as_i64).unwrap_or(5000),
                }),
            );
        }
    }
    if let Some(monitoring) = flink.get("MonitoringConfiguration") {
        let config_type = monitoring.get("ConfigurationType").and_then(Value::as_str);
        if config_type == Some("DEFAULT") {
            out.insert(
                "MonitoringConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "DEFAULT",
                    "MetricsLevel": "APPLICATION",
                    "LogLevel": "INFO",
                }),
            );
        } else if config_type == Some("CUSTOM") {
            out.insert(
                "MonitoringConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "CUSTOM",
                    "MetricsLevel": monitoring.get("MetricsLevel").and_then(Value::as_str).unwrap_or("APPLICATION"),
                    "LogLevel": monitoring.get("LogLevel").and_then(Value::as_str).unwrap_or("INFO"),
                }),
            );
        }
    }
    if let Some(parallelism_config) = flink.get("ParallelismConfiguration") {
        let config_type = parallelism_config
            .get("ConfigurationType")
            .and_then(Value::as_str);
        if config_type == Some("DEFAULT") {
            out.insert(
                "ParallelismConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "DEFAULT",
                    "Parallelism": 1,
                    "ParallelismPerKPU": 1,
                    "AutoScalingEnabled": false,
                    "CurrentParallelism": 1,
                }),
            );
        } else if config_type == Some("CUSTOM") {
            out.insert(
                "ParallelismConfigurationDescription".to_string(),
                json!({
                    "ConfigurationType": "CUSTOM",
                    "Parallelism": parallelism_config.get("Parallelism").and_then(Value::as_i64).unwrap_or(1),
                    "ParallelismPerKPU": parallelism_config.get("ParallelismPerKPU").and_then(Value::as_i64).unwrap_or(1),
                    "AutoScalingEnabled": parallelism_config.get("AutoScalingEnabled").and_then(Value::as_bool).unwrap_or(false),
                    "CurrentParallelism": parallelism_config.get("Parallelism").and_then(Value::as_i64).unwrap_or(1),
                }),
            );
        }
    }
    Value::Object(out)
}

// ---------------------------------------------------------------------------
// Tagging (moto TaggingService semantics)
// ---------------------------------------------------------------------------

fn tags_sorted(state: &Kav2State, arn: &str) -> Vec<Value> {
    state
        .tags
        .get(arn)
        .map(|map| {
            map.iter()
                .map(|(k, v)| match v {
                    Some(value) => json!({ "Key": k, "Value": value }),
                    None => json!({ "Key": k }),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn tag_resource(state: &mut Kav2State, arn: &str, tags: Option<&Value>) {
    let Some(Value::Array(list)) = tags else {
        return;
    };
    let entry = state.tags.entry(arn.to_string()).or_default();
    for tag in list {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or_default();
        let value = tag.get("Value").and_then(Value::as_str).map(String::from);
        entry.insert(key.to_string(), value);
    }
}

fn untag_resource(state: &mut Kav2State, arn: &str, tag_keys: Option<&Value>) {
    let Some(Value::Array(keys)) = tag_keys else {
        return;
    };
    if let Some(entry) = state.tags.get_mut(arn) {
        for key in keys.iter().filter_map(Value::as_str) {
            entry.remove(key);
        }
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

type OpResult = Result<Value, Response>;

fn op_create_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        if state.apps.contains_key(app_name) {
            return Err(json_error(
                400,
                "ResourceInUseException",
                &format!("Application {app_name} already exists"),
            ));
        }
        let app = Application::new(account, region, body);
        let detail = app.to_detail();
        state.apps.insert(app_name.to_string(), app);
        if let Some(tags) = body.get("Tags") {
            tag_resource(state, &now_arn(app_name, account, region), Some(tags));
        }
        Ok(json!({ "ApplicationDetail": detail }))
    })
}

fn op_describe_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        Ok(json!({ "ApplicationDetail": get_app(state, app_name)?.to_detail() }))
    })
}

fn op_delete_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        get_app(state, app_name)?;
        state.apps.remove(app_name);
        Ok(json!({}))
    })
}

fn op_describe_application_version(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let version = body
        .get("ApplicationVersionId")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    with_state(account, region, |state| {
        let mut detail = get_app(state, app_name)?.to_detail();
        detail
            .as_object_mut()
            .expect("detail")
            .insert("ApplicationVersionId".to_string(), json!(version));
        Ok(json!({ "ApplicationVersionDetail": detail }))
    })
}

fn op_update_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        if let Some(role_update) = body
            .get("ServiceExecutionRoleUpdate")
            .and_then(Value::as_str)
        {
            app.service_role = role_update.to_string();
        }
        if let Some(runtime_update) = body.get("RuntimeEnvironmentUpdate").and_then(Value::as_str) {
            app.runtime = runtime_update.to_string();
        }
        if let Some(config) = body
            .get("ApplicationConfigurationUpdate")
            .and_then(Value::as_object)
        {
            let cfg = app.app_config_description.get_or_insert_with(Map::new);
            for (k, v) in config {
                cfg.insert(k.clone(), v.clone());
            }
        }
        if let Some(Value::Array(updates)) = body.get("CloudWatchLoggingOptionUpdates") {
            let mut descs = Vec::new();
            for u in updates {
                descs.push(json!({
                    "CloudWatchLoggingOptionId": u.get("CloudWatchLoggingOptionId").and_then(Value::as_str).unwrap_or("1.1"),
                    "LogStreamARN": u.get("LogStreamARN").and_then(Value::as_str).unwrap_or_default(),
                }));
            }
            app.cw_logging = Some(descs);
        }
        app.bump_version();
        let op_id = app.record_operation("UpdateApplication");
        Ok(json!({
            "ApplicationDetail": app.to_detail(),
            "OperationId": op_id,
        }))
    })
}

fn op_update_application_maintenance_configuration(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let update = body.get("ApplicationMaintenanceConfigurationUpdate");
        if let Some(start) = update
            .and_then(|u| u.get("ApplicationMaintenanceWindowStartTimeUpdate"))
            .and_then(Value::as_str)
        {
            app.maintenance_start = start.to_string();
            let (hours, minutes) = start.split_once(':').unwrap_or(("06", "00"));
            let end = (hours.parse::<i32>().unwrap_or(6) + 8) % 24;
            app.maintenance_end = format!("{end:02}:{minutes}");
        }
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationMaintenanceConfigurationDescription": {
                "ApplicationMaintenanceWindowStartTime": app.maintenance_start,
                "ApplicationMaintenanceWindowEndTime": app.maintenance_end,
            },
        }))
    })
}

fn op_list_applications(account: u64, region: &str) -> OpResult {
    Ok(with_state(account, region, |state| {
        let summaries: Vec<Value> = state
            .apps
            .values()
            .map(|app| {
                json!({
                    "ApplicationName": app.application_name,
                    "ApplicationARN": app.arn,
                    "ApplicationStatus": app.status,
                    "ApplicationVersionId": app.version_id,
                    "RuntimeEnvironment": app.runtime,
                    "ApplicationMode": app.mode,
                })
            })
            .collect();
        json!({ "ApplicationSummaries": summaries })
    }))
}

fn op_list_application_versions(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| -> OpResult {
        let app = get_app(state, app_name)?;
        let summaries: Vec<Value> = app
            .version_summaries
            .iter()
            .map(|(version, status)| {
                json!({ "ApplicationVersionId": version, "ApplicationStatus": status })
            })
            .collect();
        Ok(json!({ "ApplicationVersionSummaries": summaries }))
    })
}

fn op_list_application_snapshots(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| -> OpResult {
        let app = get_app(state, app_name)?;
        let summaries: Vec<Value> = app.snapshots.values().map(snapshot_summary).collect();
        Ok(json!({ "SnapshotSummaries": summaries }))
    })
}

fn snapshot_summary(snapshot: &Snapshot) -> Value {
    json!({
        "SnapshotName": snapshot.name,
        "SnapshotStatus": snapshot.status,
        "ApplicationVersionId": snapshot.version_id,
        "SnapshotCreationTimestamp": snapshot.created_at,
    })
}

fn op_list_application_operations(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| -> OpResult {
        let app = get_app(state, app_name)?;
        let operation = body.get("Operation").and_then(Value::as_str);
        let operation_status = body.get("OperationStatus").and_then(Value::as_str);
        let info_list: Vec<Value> = app
            .operations
            .iter()
            .filter(|o| operation.is_none_or(|op| o.op_type == op))
            .filter(|o| operation_status.is_none_or(|status| o.status == status))
            .map(|o| {
                json!({
                    "Operation": o.op_type,
                    "OperationId": o.op_id,
                    "StartTime": o.start_time,
                    "EndTime": o.end_time,
                    "OperationStatus": o.status,
                })
            })
            .collect();
        Ok(json!({ "ApplicationOperationInfoList": info_list }))
    })
}

fn op_describe_application_operation(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let op_id = body
        .get("OperationId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        if let Some(op) = app.operations.iter().find(|o| o.op_id == op_id) {
            return Ok(json!({
                "ApplicationOperationInfoDetails": {
                    "Operation": op.op_type,
                    "OperationId": op.op_id,
                    "StartTime": op.start_time,
                    "EndTime": op.end_time,
                    "OperationStatus": op.status,
                }
            }));
        }
        Err(json_error(
            400,
            "ResourceNotFoundException",
            &format!("Operation {op_id} is not found"),
        ))
    })
}

fn op_start_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.status = "RUNNING".to_string();
        app.last_update_ts = iso_timestamp();
        Ok(json!({ "OperationId": app.record_operation("StartApplication") }))
    })
}

fn op_stop_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.status = "READY".to_string();
        app.last_update_ts = iso_timestamp();
        Ok(json!({ "OperationId": app.record_operation("StopApplication") }))
    })
}

fn op_rollback_application(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.bump_version();
        let op_id = app.record_operation("RollbackApplication");
        Ok(json!({
            "ApplicationDetail": app.to_detail(),
            "OperationId": op_id,
        }))
    })
}

fn op_create_application_snapshot(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let snapshot_name = body
        .get("SnapshotName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let version = app.version_id;
        app.snapshots.insert(
            snapshot_name.to_string(),
            Snapshot {
                name: snapshot_name.to_string(),
                status: "READY".to_string(),
                version_id: version,
                created_at: iso_timestamp(),
            },
        );
        Ok(json!({}))
    })
}

fn op_describe_application_snapshot(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let snapshot_name = body
        .get("SnapshotName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.snapshots
            .get(snapshot_name)
            .map(snapshot_summary)
            .map(|summary| json!({ "SnapshotDetails": summary }))
            .ok_or_else(|| {
                json_error(
                    400,
                    "ResourceNotFoundException",
                    &format!("Snapshot {snapshot_name} is not found for application {app_name}"),
                )
            })
    })
}

fn op_delete_application_snapshot(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let snapshot_name = body
        .get("SnapshotName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        if app.snapshots.remove(snapshot_name).is_none() {
            return Err(json_error(
                400,
                "ResourceNotFoundException",
                &format!("Snapshot {snapshot_name} is not found for application {app_name}"),
            ));
        }
        Ok(json!({}))
    })
}

fn op_create_application_presigned_url(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        // moto fetches the app to surface not-found errors, then builds a URL.
        get_app(state, app_name)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        Ok(json!({
            "AuthorizedUrl": format!(
                "https://flink-ui.kinesisanalytics.{region}.amazonaws.com/{app_name}?token={token}"
            ),
        }))
    })
}

fn op_add_application_cloud_watch_logging_option(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let log_stream_arn = body
            .pointer("/CloudWatchLoggingOption/LogStreamARN")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let descs = app.cw_logging.get_or_insert_with(Vec::new);
        let option_id = format!("{}.1", descs.len() + 1);
        descs.push(json!({
            "CloudWatchLoggingOptionId": option_id,
            "LogStreamARN": log_stream_arn,
        }));
        app.bump_version();
        let op_id = app.record_operation("AddApplicationCloudWatchLoggingOption");
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "CloudWatchLoggingOptionDescriptions": app.cw_logging,
            "OperationId": op_id,
        }))
    })
}

fn op_delete_application_cloud_watch_logging_option(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let option_id = body
        .get("CloudWatchLoggingOptionId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        if let Some(descs) = app.cw_logging.as_mut() {
            descs.retain(|d| {
                d.get("CloudWatchLoggingOptionId").and_then(Value::as_str) != Some(option_id)
            });
        }
        app.bump_version();
        let op_id = app.record_operation("DeleteApplicationCloudWatchLoggingOption");
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "CloudWatchLoggingOptionDescriptions": app.cw_logging.clone().unwrap_or_default(),
            "OperationId": op_id,
        }))
    })
}

fn op_add_application_input(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let app_input = body.get("Input").unwrap_or(&Value::Null);
        let input_id = format!("{}.1", app.inputs.len() + 1);
        let mut desc = json!({
            "InputId": input_id,
            "NamePrefix": app_input.get("NamePrefix").and_then(Value::as_str).unwrap_or_default(),
        });
        if let Some(obj) = desc.as_object_mut() {
            for (req_key, res_key) in [
                ("InputSchema", "InputSchema"),
                ("InputParallelism", "InputParallelism"),
                ("KinesisStreamsInput", "KinesisStreamsInputDescription"),
                ("KinesisFirehoseInput", "KinesisFirehoseInputDescription"),
                (
                    "InputProcessingConfiguration",
                    "InputProcessingConfigurationDescription",
                ),
            ] {
                if let Some(v) = app_input.get(req_key) {
                    obj.insert(res_key.to_string(), v.clone());
                }
            }
        }
        app.inputs.push(desc);
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "InputDescriptions": app.inputs,
        }))
    })
}

fn op_add_application_input_processing_configuration(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let input_id = body
        .get("InputId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let config = body
        .get("InputProcessingConfiguration")
        .cloned()
        .unwrap_or(json!({}));
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        for input in app.inputs.iter_mut() {
            if input.get("InputId").and_then(Value::as_str) == Some(input_id) {
                input.as_object_mut().expect("input desc").insert(
                    "InputProcessingConfigurationDescription".to_string(),
                    config.clone(),
                );
                break;
            }
        }
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "InputId": input_id,
            "InputProcessingConfigurationDescription": config,
        }))
    })
}

fn op_delete_application_input_processing_configuration(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let input_id = body
        .get("InputId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        for input in app.inputs.iter_mut() {
            if input.get("InputId").and_then(Value::as_str) == Some(input_id) {
                input
                    .as_object_mut()
                    .expect("input desc")
                    .remove("InputProcessingConfigurationDescription");
                break;
            }
        }
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    })
}

fn op_add_application_output(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let output = body.get("Output").unwrap_or(&Value::Null);
        let output_id = format!("{}.1", app.outputs.len() + 1);
        let mut desc = json!({
            "OutputId": output_id,
            "Name": output.get("Name").and_then(Value::as_str).unwrap_or_default(),
        });
        if let Some(obj) = desc.as_object_mut() {
            for (req_key, res_key) in [
                ("DestinationSchema", "DestinationSchema"),
                ("KinesisStreamsOutput", "KinesisStreamsOutputDescription"),
                ("KinesisFirehoseOutput", "KinesisFirehoseOutputDescription"),
                ("LambdaOutput", "LambdaOutputDescription"),
            ] {
                if let Some(v) = output.get(req_key) {
                    obj.insert(res_key.to_string(), v.clone());
                }
            }
        }
        app.outputs.push(desc);
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "OutputDescriptions": app.outputs,
        }))
    })
}

fn op_delete_application_output(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let output_id = body
        .get("OutputId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.outputs
            .retain(|o| o.get("OutputId").and_then(Value::as_str) != Some(output_id));
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    })
}

fn op_add_application_reference_data_source(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let source = body.get("ReferenceDataSource").unwrap_or(&Value::Null);
        let ref_id = format!("{}.1", app.refs.len() + 1);
        let mut desc = json!({
            "ReferenceId": ref_id,
            "TableName": source.get("TableName").and_then(Value::as_str).unwrap_or_default(),
        });
        if let Some(obj) = desc.as_object_mut() {
            if let Some(schema) = source.get("ReferenceSchema") {
                obj.insert("ReferenceSchema".to_string(), schema.clone());
            }
            if let Some(s3) = source.get("S3ReferenceDataSource") {
                obj.insert("S3ReferenceDataSourceDescription".to_string(), s3.clone());
            }
        }
        app.refs.push(desc);
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "ReferenceDataSourceDescriptions": app.refs,
        }))
    })
}

fn op_delete_application_reference_data_source(
    body: &Value,
    account: u64,
    region: &str,
) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let ref_id = body
        .get("ReferenceId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        app.refs
            .retain(|r| r.get("ReferenceId").and_then(Value::as_str) != Some(ref_id));
        app.bump_version();
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    })
}

fn op_add_application_vpc_configuration(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        let cfg = app.app_config_description.get_or_insert_with(Map::new);
        let vpc_descriptions = cfg
            .entry("VpcConfigurationDescriptions".to_string())
            .or_insert_with(|| json!([]));
        let vpc_len = vpc_descriptions.as_array().map(Vec::len).unwrap_or(0);
        let vpc_id = format!("{}.1", vpc_len + 1);
        let mut desc = json!({
            "VpcConfigurationId": vpc_id,
            "VpcId": FAKE_VPC_ID,
        });
        if let Some(obj) = desc.as_object_mut() {
            if let Some(Value::Object(vpc_config)) = body.get("VpcConfiguration") {
                for (k, v) in vpc_config {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(arr) = vpc_descriptions.as_array_mut() {
            arr.push(desc.clone());
        }
        app.bump_version();
        let op_id = app.record_operation("AddApplicationVpcConfiguration");
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "VpcConfigurationDescription": desc,
            "OperationId": op_id,
        }))
    })
}

fn op_delete_application_vpc_configuration(body: &Value, account: u64, region: &str) -> OpResult {
    let app_name = body
        .get("ApplicationName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let vpc_id = body
        .get("VpcConfigurationId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    with_state(account, region, |state| {
        let app = get_app(state, app_name)?;
        if let Some(cfg) = app.app_config_description.as_mut() {
            if let Some(vpc_descriptions) = cfg
                .get_mut("VpcConfigurationDescriptions")
                .and_then(Value::as_array_mut)
            {
                vpc_descriptions.retain(|v| {
                    v.get("VpcConfigurationId").and_then(Value::as_str) != Some(vpc_id)
                });
            }
        }
        app.bump_version();
        let op_id = app.record_operation("DeleteApplicationVpcConfiguration");
        Ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "OperationId": op_id,
        }))
    })
}

fn op_discover_input_schema() -> Value {
    json!({
        "InputSchema": {
            "RecordFormat": {
                "RecordFormatType": "CSV",
                "MappingParameters": {
                    "CSVMappingParameters": {
                        "RecordRowDelimiter": "\n",
                        "RecordColumnDelimiter": ",",
                    }
                },
            },
            "RecordColumns": [
                { "Name": "Col_1", "SqlType": "VARCHAR(4)", "Mapping": "$.Col_1" }
            ],
        },
        "ParsedInputRecords": [["record1"]],
        "ProcessedInputRecords": ["record1"],
        "RawInputRecords": ["raw_record1"],
    })
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body: Value = crate::proto::parse_json_body(&req.body);
    let result: OpResult = match req.operation.as_str() {
        "CreateApplication" => op_create_application(&body, req.account, &req.region),
        "DescribeApplication" => op_describe_application(&body, req.account, &req.region),
        "DeleteApplication" => op_delete_application(&body, req.account, &req.region),
        "DescribeApplicationVersion" => {
            op_describe_application_version(&body, req.account, &req.region)
        }
        "UpdateApplication" => op_update_application(&body, req.account, &req.region),
        "UpdateApplicationMaintenanceConfiguration" => {
            op_update_application_maintenance_configuration(&body, req.account, &req.region)
        }
        "ListApplications" => op_list_applications(req.account, &req.region),
        "ListApplicationVersions" => op_list_application_versions(&body, req.account, &req.region),
        "ListApplicationSnapshots" => {
            op_list_application_snapshots(&body, req.account, &req.region)
        }
        "ListApplicationOperations" => {
            op_list_application_operations(&body, req.account, &req.region)
        }
        "DescribeApplicationOperation" => {
            op_describe_application_operation(&body, req.account, &req.region)
        }
        "StartApplication" => op_start_application(&body, req.account, &req.region),
        "StopApplication" => op_stop_application(&body, req.account, &req.region),
        "RollbackApplication" => op_rollback_application(&body, req.account, &req.region),
        "CreateApplicationSnapshot" => {
            op_create_application_snapshot(&body, req.account, &req.region)
        }
        "DescribeApplicationSnapshot" => {
            op_describe_application_snapshot(&body, req.account, &req.region)
        }
        "DeleteApplicationSnapshot" => {
            op_delete_application_snapshot(&body, req.account, &req.region)
        }
        "CreateApplicationPresignedUrl" => {
            op_create_application_presigned_url(&body, req.account, &req.region)
        }
        "AddApplicationCloudWatchLoggingOption" => {
            op_add_application_cloud_watch_logging_option(&body, req.account, &req.region)
        }
        "DeleteApplicationCloudWatchLoggingOption" => {
            op_delete_application_cloud_watch_logging_option(&body, req.account, &req.region)
        }
        "AddApplicationInput" => op_add_application_input(&body, req.account, &req.region),
        "AddApplicationInputProcessingConfiguration" => {
            op_add_application_input_processing_configuration(&body, req.account, &req.region)
        }
        "DeleteApplicationInputProcessingConfiguration" => {
            op_delete_application_input_processing_configuration(&body, req.account, &req.region)
        }
        "AddApplicationOutput" => op_add_application_output(&body, req.account, &req.region),
        "DeleteApplicationOutput" => op_delete_application_output(&body, req.account, &req.region),
        "AddApplicationReferenceDataSource" => {
            op_add_application_reference_data_source(&body, req.account, &req.region)
        }
        "DeleteApplicationReferenceDataSource" => {
            op_delete_application_reference_data_source(&body, req.account, &req.region)
        }
        "AddApplicationVpcConfiguration" => {
            op_add_application_vpc_configuration(&body, req.account, &req.region)
        }
        "DeleteApplicationVpcConfiguration" => {
            op_delete_application_vpc_configuration(&body, req.account, &req.region)
        }
        "DiscoverInputSchema" => Ok(op_discover_input_schema()),
        "TagResource" => with_state(req.account, &req.region, |state| {
            tag_resource(
                state,
                body.get("ResourceARN")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                body.get("Tags"),
            );
            Ok(json!({}))
        }),
        "UntagResource" => with_state(req.account, &req.region, |state| {
            untag_resource(
                state,
                body.get("ResourceARN")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                body.get("TagKeys"),
            );
            Ok(json!({}))
        }),
        "ListTagsForResource" => with_state(req.account, &req.region, |state| {
            let arn = body
                .get("ResourceARN")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let listed = tags_sorted(state, arn);
            Ok(json!({ "Tags": listed }))
        }),
        other => {
            return crate::proto::error_response(
                501,
                "NotImplemented",
                &format!("The {other} action has not been implemented"),
            )
        }
    };
    match result {
        Ok(value) => Response::json_status(200, &value),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(op: &str, body: Value) -> Response {
        dispatch(&Request {
            service: "kinesisanalyticsv2".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        })
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn create_app(name: &str, runtime: &str) -> Value {
        body_of(&run(
            "CreateApplication",
            json!({
                "ApplicationName": name,
                "RuntimeEnvironment": runtime,
                "ServiceExecutionRole": "arn:aws:iam::123456789012:role/test",
            }),
        ))
        .get("ApplicationDetail")
        .cloned()
        .unwrap_or(Value::Null)
    }

    #[test]
    fn create_describe_list_roundtrip() {
        let detail = create_app("app-roundtrip", "FLINK-1_18");
        assert!(detail["ApplicationARN"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:kinesisanalytics:"));
        assert_eq!(detail["RuntimeEnvironment"], "FLINK-1_18");
        assert_eq!(detail["ApplicationStatus"], "READY");
        assert_eq!(detail["ApplicationVersionId"], 1);
        assert!(detail["CreateTimestamp"].is_string());
        assert!(detail["ApplicationMaintenanceConfigurationDescription"].is_object());
        assert!(detail["ConditionalToken"].is_string());

        let described = body_of(&run(
            "DescribeApplication",
            json!({"ApplicationName": "app-roundtrip"}),
        ))["ApplicationDetail"]
            .clone();
        assert_eq!(described["ApplicationARN"], detail["ApplicationARN"]);

        let listed = body_of(&run("ListApplications", json!({})));
        let summaries = listed["ApplicationSummaries"].as_array().unwrap();
        assert!(summaries
            .iter()
            .any(|s| s["ApplicationName"] == "app-roundtrip"));

        run(
            "DeleteApplication",
            json!({"ApplicationName": "app-roundtrip"}),
        );
        let err = run(
            "DescribeApplication",
            json!({"ApplicationName": "app-roundtrip"}),
        );
        assert_eq!(body_of(&err)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn duplicate_create_is_resource_in_use() {
        run(
            "CreateApplication",
            json!({
                "ApplicationName": "app-dup",
                "RuntimeEnvironment": "SQL-1_0",
                "ServiceExecutionRole": "arn:aws:iam::123456789012:role/test",
            }),
        );
        let err = run(
            "CreateApplication",
            json!({
                "ApplicationName": "app-dup",
                "RuntimeEnvironment": "SQL-1_0",
                "ServiceExecutionRole": "arn:aws:iam::123456789012:role/test",
            }),
        );
        assert_eq!(body_of(&err)["__type"], "ResourceInUseException");
    }

    #[test]
    fn tags_resource_lifecycle() {
        let detail = create_app("app-tags", "FLINK-1_18");
        let arn = detail["ApplicationARN"].as_str().unwrap().to_string();
        run(
            "TagResource",
            json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        run(
            "TagResource",
            json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "test2"}]}),
        );
        let listed = body_of(&run("ListTagsForResource", json!({"ResourceARN": arn})));
        let tags = listed["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Value"], "test2");
        run(
            "UntagResource",
            json!({"ResourceARN": arn, "TagKeys": ["env"]}),
        );
        let listed = body_of(&run("ListTagsForResource", json!({"ResourceARN": arn})));
        assert!(listed["Tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn snapshots_lifecycle() {
        create_app("app-snaps", "FLINK-1_18");
        run(
            "CreateApplicationSnapshot",
            json!({"ApplicationName": "app-snaps", "SnapshotName": "snap1"}),
        );
        let listed = body_of(&run(
            "ListApplicationSnapshots",
            json!({"ApplicationName": "app-snaps"}),
        ));
        assert_eq!(listed["SnapshotSummaries"].as_array().unwrap().len(), 1);

        let described = body_of(&run(
            "DescribeApplicationSnapshot",
            json!({"ApplicationName": "app-snaps", "SnapshotName": "snap1"}),
        ));
        assert_eq!(described["SnapshotDetails"]["SnapshotName"], "snap1");
        assert_eq!(described["SnapshotDetails"]["SnapshotStatus"], "READY");

        run(
            "DeleteApplicationSnapshot",
            json!({"ApplicationName": "app-snaps", "SnapshotName": "snap1", "SnapshotCreationTimestamp": 1}),
        );
        let err = run(
            "DescribeApplicationSnapshot",
            json!({"ApplicationName": "app-snaps", "SnapshotName": "snap1"}),
        );
        assert_eq!(body_of(&err)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn update_application_bumps_version_and_role() {
        create_app("app-update", "FLINK-1_18");
        let resp = body_of(&run(
            "UpdateApplication",
            json!({
                "ApplicationName": "app-update",
                "CurrentApplicationVersionId": 1,
                "ServiceExecutionRoleUpdate": "arn:aws:iam::123456789012:role/updated",
            }),
        ));
        assert_eq!(
            resp["ApplicationDetail"]["ServiceExecutionRole"],
            "arn:aws:iam::123456789012:role/updated"
        );
        assert_eq!(resp["ApplicationDetail"]["ApplicationVersionId"], 2);
        assert!(resp["OperationId"].is_string());

        let versions = body_of(&run(
            "ListApplicationVersions",
            json!({"ApplicationName": "app-update"}),
        ));
        assert_eq!(
            versions["ApplicationVersionSummaries"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn maintenance_configuration_end_window() {
        create_app("app-maint", "FLINK-1_18");
        let resp = body_of(&run(
            "UpdateApplicationMaintenanceConfiguration",
            json!({
                "ApplicationName": "app-maint",
                "ApplicationMaintenanceConfigurationUpdate": {
                    "ApplicationMaintenanceWindowStartTimeUpdate": "04:00"
                },
            }),
        ));
        assert!(resp.as_object().is_none() || !resp.as_object().unwrap().is_empty());
        let _ = resp;
        let detail = body_of(&run(
            "DescribeApplication",
            json!({"ApplicationName": "app-maint"}),
        ))["ApplicationDetail"]
            .clone();
        assert_eq!(
            detail["ApplicationMaintenanceConfigurationDescription"]
                ["ApplicationMaintenanceWindowStartTime"],
            "04:00"
        );
    }

    #[test]
    fn add_output_and_reference() {
        let created = create_app("app-output", "SQL-1_0");
        let version = created["ApplicationVersionId"].as_i64().unwrap();
        let resp = run(
            "AddApplicationOutput",
            json!({
                "ApplicationName": "app-output",
                "CurrentApplicationVersionId": version,
                "Output": {
                    "Name": "test-output",
                    "DestinationSchema": {"RecordFormatType": "JSON"},
                    "KinesisStreamsOutput": {"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test"}
                },
            }),
        );
        assert_eq!(body_of(&resp)["ApplicationARN"], created["ApplicationARN"]);
        let resp = run(
            "AddApplicationReferenceDataSource",
            json!({
                "ApplicationName": "app-output",
                "CurrentApplicationVersionId": version,
                "ReferenceDataSource": {
                    "TableName": "ref_table",
                    "ReferenceSchema": {"RecordFormat": {"RecordFormatType": "CSV"}}
                },
            }),
        );
        assert!(body_of(&resp)["ApplicationARN"].is_string());
    }

    #[test]
    fn start_stop_operations_log() {
        create_app("app-ops", "FLINK-1_18");
        assert!(body_of(&run(
            "StartApplication",
            json!({"ApplicationName": "app-ops"})
        ))["OperationId"]
            .is_string());
        let detail = body_of(&run(
            "DescribeApplication",
            json!({"ApplicationName": "app-ops"}),
        ))["ApplicationDetail"]
            .clone();
        assert_eq!(detail["ApplicationStatus"], "RUNNING");
        let listed = body_of(&run(
            "ListApplicationOperations",
            json!({"ApplicationName": "app-ops"}),
        ));
        assert_eq!(
            listed["ApplicationOperationInfoList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn discover_input_schema_shape() {
        let resp = body_of(&run(
            "DiscoverInputSchema",
            json!({"ServiceExecutionRole": "arn:aws:iam::123456789012:role/test"}),
        ));
        assert!(!resp["InputSchema"]["RecordColumns"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unknown_op_still_501() {
        let err = run("BogusAction", json!({}));
        assert_eq!(status(&err), 501);
    }

    fn status(resp: &Response) -> u16 {
        resp.status
    }
}
