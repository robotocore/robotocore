//! SSM in-memory state models (mirrors moto's ssm backend semantics).

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use parking_lot::RwLock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

pub const PARAMETER_VERSION_LIMIT: usize = 100;
pub const PARAMETER_HISTORY_MAX_RESULTS: i64 = 50;
pub const PARAMETER_BY_PATH_MAX_RESULTS: i64 = 10;

/// Format a datetime the way moto serializes `str(datetime)`.
pub fn fmt_dt(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f+00:00").to_string()
}

/// Short hex string of `n` chars taken from a uuid4 hex.
pub fn hex_id(len: usize) -> String {
    Uuid::new_v4().simple().to_string().chars().take(len).collect()
}

pub fn uuid_str() -> String {
    Uuid::new_v4().to_string()
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// One immutable version of an SSM parameter.
#[derive(Debug, Clone)]
pub struct ParamVersion {
    pub version: u64,
    /// Stored value (for SecureString this is the `kms:keyid:value` form).
    pub value: String,
    pub parameter_type: String,
    pub data_type: String,
    pub description: Option<String>,
    pub allowed_pattern: Option<String>,
    pub keyid: Option<String>,
    pub tier: String,
    pub policies: Option<Value>,
    pub labels: Vec<String>,
    pub last_modified: DateTime<Utc>,
}

impl ParamVersion {
    pub fn decrypt_stored(&self, decrypt: bool) -> String {
        if !decrypt || self.parameter_type != "SecureString" {
            return self.value.clone();
        }
        let prefix = format!("kms:{}:", self.keyid.clone().unwrap_or_else(|| "default".into()));
        match self.value.strip_prefix(&prefix) {
            Some(rest) => rest.to_string(),
            None => self.value.clone(),
        }
    }

    /// Shape returned by GetParameter / GetParameters / GetParametersByPath.
    pub fn response_object(&self, name: &str, decrypt: bool, region: Option<&str>, account: u64) -> Value {
        let mut r = json!({
            "Name": name,
            "Type": self.parameter_type,
            "Value": self.decrypt_stored(decrypt),
            "Version": self.version,
            "LastModifiedDate": fmt_dt(&self.last_modified),
            "DataType": self.data_type,
            "Tier": self.tier,
        });
        if let Some(region) = region {
            let label = name.strip_prefix('/').unwrap_or(name);
            r["ARN"] = json!(format!("arn:aws:ssm:{region}:{account}:parameter/{label}"));
        }
        r
    }

    /// Shape returned by DescribeParameters / GetParameterHistory.
    pub fn describe_response_object(
        &self,
        name: &str,
        decrypt: bool,
        include_labels: bool,
        account: u64,
    ) -> Value {
        let mut r = self.response_object(name, decrypt, None, account);
        r["LastModifiedUser"] = json!("N/A");
        if let Some(d) = &self.description {
            if !d.is_empty() {
                r["Description"] = json!(d);
            }
        }
        if let Some(k) = &self.keyid {
            r["KeyId"] = json!(k);
        }
        if let Some(a) = &self.allowed_pattern {
            r["AllowedPattern"] = json!(a);
        }
        if include_labels {
            r["Labels"] = json!(self.labels);
        }
        r
    }
}

/// A parameter with its full version history (ascending by version).
#[derive(Debug, Default)]
pub struct Parameter {
    pub name: String,
    pub versions: RwLock<Vec<ParamVersion>>,
}

impl Parameter {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), versions: RwLock::new(Vec::new()) }
    }

    pub fn current(&self) -> Option<ParamVersion> {
        self.versions.read().last().cloned()
    }

    pub fn version(&self, n: u64) -> Option<ParamVersion> {
        self.versions.read().iter().find(|v| v.version == n).cloned()
    }

    pub fn version_by_label(&self, label: &str) -> Option<ParamVersion> {
        let versions = self.versions.read();
        let mut candidate: Option<ParamVersion> = None;
        for v in versions.iter() {
            if v.labels.iter().any(|l| l == label) {
                candidate = Some(v.clone());
            }
        }
        candidate
    }

    pub fn push_version(&self, v: ParamVersion) {
        let mut versions = self.versions.write();
        versions.push(v);
        if versions.len() > PARAMETER_VERSION_LIMIT {
            versions.remove(0);
        }
    }
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AccountPermission {
    pub account_id: String,
    pub version: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Document {
    pub owner: u64,
    pub name: String,
    pub version: String,
    pub version_name: Option<String>,
    pub content: String,
    pub content_json: Value,
    pub document_type: String,
    pub document_format: String,
    pub requires: Option<Value>,
    pub attachments: Option<Value>,
    pub target_type: Option<String>,
    pub status: String,
    pub description: Option<String>,
    pub schema_version: String,
    pub parameter_list: Option<Value>,
    pub created: DateTime<Utc>,
}

/// Parsed document content: (content JSON, schema version, description, parameter list).
pub type ParsedContent = (Value, String, Option<String>, Option<Value>);

impl Document {
    pub fn parse_content(
        content: &str,
        document_format: &str,
    ) -> Result<ParsedContent, &'static str> {
        if document_format != "JSON" && document_format != "YAML" {
            return Err("unsupported document format");
        }
        let parsed: Value = if document_format == "JSON" {
            serde_json::from_str(content).map_err(|_| "invalid JSON body")?
        } else {
            serde_yaml_from_str(content).ok_or("YAML content is not supported")?
        };
        let schema_version = parsed
            .get("schemaVersion")
            .map(v_to_string)
            .ok_or("missing schemaVersion")?;
        let description = parsed.get("description").map(|v| v.as_str().unwrap_or("").to_string());
        let parameter_list = parsed.get("parameters").map(doc_param_list);
        Ok((parsed, schema_version, description, parameter_list))
    }

    pub fn hash(&self) -> String {
        sha256_hex(&self.content)
    }

    /// The `DocumentDescription` / `Document` shape.
    pub fn describe(&self, tags: Option<Value>) -> Value {
        let mut base = json!({
            "Hash": self.hash(),
            "HashType": "Sha256",
            "Name": self.name,
            "Owner": self.owner.to_string(),
            "CreatedDate": fmt_dt(&self.created),
            "Status": self.status,
            "DocumentVersion": self.version,
            "Description": self.description.clone(),
            "Parameters": self.parameter_list.clone(),
            "PlatformTypes": ["Not Implemented (moto)"],
            "DocumentType": self.document_type,
            "SchemaVersion": self.schema_version,
            "DocumentFormat": self.document_format,
        });
        if let Some(vn) = &self.version_name {
            base["VersionName"] = json!(vn);
        }
        if let Some(tt) = &self.target_type {
            base["TargetType"] = json!(tt);
        }
        if let Some(tags) = tags {
            if tags.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                base["Tags"] = tags;
            }
        }
        base
    }

    /// The `DocumentIdentifiers` entry shape.
    pub fn list_describe(&self, tags: Option<Value>) -> Value {
        let mut base = json!({
            "Name": self.name,
            "Owner": self.owner.to_string(),
            "DocumentVersion": self.version,
            "DocumentType": self.document_type,
            "SchemaVersion": self.schema_version,
            "DocumentFormat": self.document_format,
            "PlatformTypes": ["Not Implemented (moto)"],
        });
        if let Some(vn) = &self.version_name {
            base["VersionName"] = json!(vn);
        }
        if let Some(tt) = &self.target_type {
            base["TargetType"] = json!(tt);
        }
        if let Some(requires) = &self.requires {
            if requires.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                base["Requires"] = requires.clone();
            }
        }
        if let Some(tags) = tags {
            if tags.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                base["Tags"] = tags;
            }
        }
        base
    }

    /// The GetDocument response body.
    pub fn get_document(&self, document_format: &str) -> Value {
        let content = if document_format == self.document_format {
            self.content.clone()
        } else {
            self.content_json.to_string()
        };
        let mut base = json!({
            "Name": self.name,
            "DocumentVersion": self.version,
            "Status": self.status,
            "Content": content,
            "DocumentType": self.document_type,
            "DocumentFormat": document_format,
        });
        if let Some(vn) = &self.version_name {
            base["VersionName"] = json!(vn);
        }
        if let Some(requires) = &self.requires {
            if requires.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                base["Requires"] = requires.clone();
            }
        }
        base
    }
}

/// Render a JSON scalar the way moto would stringify it.
fn v_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// moto's `generate_ssm_doc_param_list`.
fn doc_param_list(parameters: &Value) -> Value {
    let Some(map) = parameters.as_object() else { return Value::Null };
    let mut out = Vec::new();
    for (name, info) in map {
        let mut entry = json!({ "Name": name });
        if let Some(d) = info.get("Description") {
            entry["Description"] = d.clone();
        } else if let Some(d) = info.get("description") {
            entry["Description"] = d.clone();
        }
        if let Some(t) = info.get("type").or_else(|| info.get("Type")) {
            entry["Type"] = t.clone();
        }
        for key in ["default", "DefaultValue", "default_value"] {
            if let Some(dv) = info.get(key) {
                if !dv.is_null() {
                    entry["DefaultValue"] = match dv {
                        Value::String(s) => json!(s),
                        other => json!(other.to_string()),
                    };
                }
                break;
            }
        }
        out.push(entry);
    }
    json!(out)
}

fn serde_yaml_from_str(_content: &str) -> Option<Value> {
    // YAML document content is not supported natively; treat as unparsable.
    None
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Command {
    pub command_id: String,
    pub comment: String,
    pub document_name: String,
    pub max_concurrency: String,
    pub max_errors: String,
    pub notification_config: Value,
    pub output_s3_bucket_name: String,
    pub output_s3_key_prefix: String,
    pub output_s3_region: String,
    pub parameters: Value,
    pub service_role_arn: String,
    pub targets: Value,
    pub instance_ids: Vec<String>,
    pub requested_date_time: DateTime<Utc>,
    pub timeout_seconds: i64,
}

impl Command {
    pub fn response_object(&self) -> Value {
        json!({
            "CommandId": self.command_id,
            "Comment": self.comment,
            "CompletedCount": self.instance_ids.len(),
            "DeliveryTimedOutCount": 0,
            "DocumentName": self.document_name,
            "ErrorCount": 0,
            "ExpiresAfter": fmt_dt(&(self.requested_date_time + ChronoDuration::seconds(self.timeout_seconds))),
            "InstanceIds": self.instance_ids,
            "MaxConcurrency": self.max_concurrency,
            "MaxErrors": self.max_errors,
            "NotificationConfig": self.notification_config,
            "OutputS3Region": self.output_s3_region,
            "OutputS3BucketName": self.output_s3_bucket_name,
            "OutputS3KeyPrefix": self.output_s3_key_prefix,
            "Parameters": self.parameters,
            "RequestedDateTime": fmt_dt(&self.requested_date_time),
            "ServiceRole": self.service_role_arn,
            "Status": "Success",
            "StatusDetails": "Details placeholder",
            "TargetCount": self.instance_ids.len(),
            "Targets": self.targets,
            "TimeoutSeconds": self.timeout_seconds,
        })
    }

    pub fn invocation_response(&self, instance_id: &str) -> Value {
        json!({
            "CommandId": self.command_id,
            "InstanceId": instance_id,
            "Comment": self.comment,
            "DocumentName": self.document_name,
            "PluginName": "aws:runShellScript",
            "ResponseCode": 0,
            "ExecutionStartDateTime": self.requested_date_time.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            "ExecutionElapsedTime": "PT5M",
            "ExecutionEndDateTime": (self.requested_date_time + ChronoDuration::seconds(300)).format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            "Status": "Success",
            "StatusDetails": "Success",
            "StandardOutputContent": "",
            "StandardOutputUrl": "",
            "StandardErrorContent": "",
        })
    }
}

// ---------------------------------------------------------------------------
// Maintenance windows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MwTarget {
    pub window_id: String,
    pub window_target_id: String,
    pub resource_type: String,
    pub targets: Value,
    pub name: Option<String>,
    pub description: Option<String>,
    pub owner_information: Option<String>,
}

impl MwTarget {
    pub fn to_json(&self) -> Value {
        json!({
            "WindowId": self.window_id,
            "WindowTargetId": self.window_target_id,
            "ResourceType": self.resource_type,
            "Targets": self.targets,
            "OwnerInformation": self.owner_information.clone().unwrap_or_default(),
            "Name": self.name.clone(),
            "Description": self.description.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct MwTask {
    pub window_id: String,
    pub window_task_id: String,
    pub task_type: Option<String>,
    pub task_arn: Option<String>,
    pub service_role_arn: Option<String>,
    pub task_parameters: Option<Value>,
    pub task_invocation_parameters: Option<Value>,
    pub priority: Option<i64>,
    pub max_concurrency: Option<String>,
    pub max_errors: Option<String>,
    pub logging_info: Option<Value>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub targets: Option<Value>,
    pub cutoff_behavior: Option<String>,
    pub alarm_configurations: Option<Value>,
}

impl MwTask {
    pub fn to_json(&self) -> Value {
        json!({
            "WindowId": self.window_id,
            "WindowTaskId": self.window_task_id,
            "TaskType": self.task_type.clone(),
            "TaskArn": self.task_arn.clone(),
            "ServiceRoleArn": self.service_role_arn.clone(),
            "TaskParameters": self.task_parameters.clone(),
            "Priority": self.priority.clone(),
            "MaxConcurrency": self.max_concurrency.clone(),
            "MaxErrors": self.max_errors.clone(),
            "LoggingInfo": self.logging_info.clone(),
            "Name": self.name.clone(),
            "Description": self.description.clone(),
            "Targets": self.targets.clone(),
            "TaskInvocationParameters": self.task_invocation_parameters.clone(),
        })
    }
}

pub type BTreeMapTargetMap = RwLock<std::collections::BTreeMap<String, MwTarget>>;
pub type BTreeMapTaskMap = RwLock<std::collections::BTreeMap<String, MwTask>>;

#[derive(Debug, Default)]
pub struct MaintenanceWindow {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub duration: Option<i64>,
    pub cutoff: Option<i64>,
    pub schedule: Option<String>,
    pub schedule_timezone: Option<String>,
    pub schedule_offset: Option<i64>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub targets: BTreeMapTargetMap,
    pub tasks: BTreeMapTaskMap,
}

impl MaintenanceWindow {
    pub fn to_json(&self) -> Value {
        json!({
            "WindowId": self.id,
            "Name": self.name,
            "Description": self.description.clone(),
            "Enabled": true,
            "Duration": self.duration.clone(),
            "Cutoff": self.cutoff.clone(),
            "Schedule": self.schedule.clone(),
            "ScheduleTimezone": self.schedule_timezone.clone(),
            "ScheduleOffset": self.schedule_offset.clone(),
            "StartDate": self.start_date.clone(),
            "EndDate": self.end_date.clone(),
        })
    }

    pub fn generate_id() -> String {
        format!("mw-{}", hex_id(17))
    }
}


// ---------------------------------------------------------------------------
// Patch baselines
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PatchBaseline {
    pub id: String,
    pub name: String,
    pub operating_system: Option<String>,
    pub global_filters: Option<Value>,
    pub approval_rules: Option<Value>,
    pub approved_patches: Option<Value>,
    pub approved_patches_compliance_level: Option<String>,
    pub approved_patches_enable_non_security: Option<bool>,
    pub rejected_patches: Option<Value>,
    pub rejected_patches_action: Option<String>,
    pub description: Option<String>,
    pub sources: Option<Value>,
    pub default_baseline: bool,
    pub created: DateTime<Utc>,
}

impl PatchBaseline {
    pub fn to_json(&self) -> Value {
        json!({
            "BaselineId": self.id,
            "OperatingSystem": self.operating_system.clone(),
            "BaselineName": self.name,
            "GlobalFilters": self.global_filters.clone(),
            "ApprovalRules": self.approval_rules.clone(),
            "ApprovedPatches": self.approved_patches.clone(),
            "ApprovedPatchesComplianceLevel": self.approved_patches_compliance_level.clone(),
            "ApprovedPatchesEnableNonSecurity": self.approved_patches_enable_non_security.clone(),
            "RejectedPatches": self.rejected_patches.clone(),
            "RejectedPatchesAction": self.rejected_patches_action.clone(),
            "BaselineDescription": self.description.clone(),
            "Sources": self.sources.clone(),
            "DefaultBaseline": self.default_baseline,
        })
    }

    pub fn generate_id() -> String {
        format!("pb-{}", hex_id(17))
    }
}

// ---------------------------------------------------------------------------
// Associations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Association {
    pub association_id: String,
    pub name: String,
    pub instance_id: Option<String>,
    pub targets: Option<Value>,
    pub parameters: Option<Value>,
    pub schedule_expression: Option<String>,
    pub output_location: Option<Value>,
    pub association_name: Option<String>,
    pub document_version: String,
    pub max_errors: Option<String>,
    pub max_concurrency: Option<String>,
    pub compliance_severity: String,
    pub apply_only_at_cron_interval: bool,
    pub association_version: String,
    pub date: DateTime<Utc>,
    pub last_update_association_date: DateTime<Utc>,
    pub last_execution_date: DateTime<Utc>,
    pub status: String,
}

impl Association {
    pub fn targets_value(&self) -> Value {
        self.targets.clone().unwrap_or_else(|| json!([]))
    }

    pub fn parameters_value(&self) -> Value {
        self.parameters.clone().unwrap_or_else(|| json!({}))
    }

    pub fn to_json(&self) -> Value {
        let mut result = json!({
            "AssociationId": self.association_id,
            "Name": self.name,
            "AssociationVersion": self.association_version,
            "DocumentVersion": self.document_version,
            "Targets": self.targets_value(),
            "LastExecutionDate": fmt_dt(&self.last_execution_date),
            "Overview": {
                "Status": "Success",
                "AssociationStatusAggregatedCount": { "Success": 1 }
            },
            "ScheduleExpression": self.schedule_expression.clone(),
        });
        if let Some(an) = &self.association_name {
            result["AssociationName"] = json!(an);
        }
        if let Some(iid) = &self.instance_id {
            result["InstanceId"] = json!(iid);
        }
        result
    }

    pub fn describe(&self) -> Value {
        let mut result = json!({
            "AssociationId": self.association_id,
            "Name": self.name,
            "AssociationVersion": self.association_version,
            "DocumentVersion": self.document_version,
            "Targets": self.targets_value(),
            "Parameters": self.parameters_value(),
            "LastExecutionDate": fmt_dt(&self.last_execution_date),
            "Date": fmt_dt(&self.date),
            "LastUpdateAssociationDate": fmt_dt(&self.last_update_association_date),
            "Overview": {
                "Status": "Success",
                "AssociationStatusAggregatedCount": { "Success": 1 }
            },
            "Status": {
                "Date": fmt_dt(&self.date),
                "Name": self.status,
                "Message": format!("Associated with {}", self.name),
                "AdditionalInfo": "",
            },
            "ComplianceSeverity": self.compliance_severity,
            "ApplyOnlyAtCronInterval": self.apply_only_at_cron_interval,
        });
        if let Some(se) = &self.schedule_expression {
            result["ScheduleExpression"] = json!(se);
        }
        if let Some(ol) = &self.output_location {
            result["OutputLocation"] = ol.clone();
        }
        if let Some(an) = &self.association_name {
            result["AssociationName"] = json!(an);
        }
        if let Some(iid) = &self.instance_id {
            result["InstanceId"] = json!(iid);
        }
        if let Some(me) = &self.max_errors {
            result["MaxErrors"] = json!(me);
        }
        if let Some(mc) = &self.max_concurrency {
            result["MaxConcurrency"] = json!(mc);
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Ops items
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug)]
pub struct OpsItem {
    pub ops_item_id: String,
    pub title: String,
    pub description: Option<String>,
    pub source: String,
    pub status: String,
    pub priority: Option<i64>,
    pub category: Option<String>,
    pub severity: Option<String>,
    pub operational_data: Option<Value>,
    pub notifications: Option<Value>,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub version: String,
    pub related_items: RwLock<Vec<Value>>,
}

impl OpsItem {
    pub fn root_arn(&self, account: u64, region: &str) -> String {
        format!("arn:aws:ssm:{region}:{account}:opsitem/{}", self.ops_item_id)
    }

    pub fn to_json(&self, account: u64, region: &str) -> Value {
        json!({
            "OpsItemId": self.ops_item_id,
            "Title": self.title,
            "Description": self.description.clone(),
            "Source": self.source,
            "Status": self.status,
            "Priority": self.priority.clone(),
            "Category": self.category.clone(),
            "Severity": self.severity.clone(),
            "OperationalData": self.operational_data.clone(),
            "Notifications": self.notifications.clone(),
            "CreatedTime": fmt_dt(&self.created_time),
            "LastModifiedTime": fmt_dt(&self.last_modified_time),
            "CreatedBy": iam_root(account),
            "LastModifiedBy": iam_root(account),
            "Version": self.version,
            "ARN": self.root_arn(account, region),
        })
    }

    pub fn summary(&self, account: u64, _region: &str) -> Value {
        json!({
            "OpsItemId": self.ops_item_id,
            "Title": self.title,
            "Source": self.source,
            "Status": self.status,
            "Priority": self.priority.clone(),
            "Category": self.category.clone(),
            "Severity": self.severity.clone(),
            "CreatedTime": fmt_dt(&self.created_time),
            "LastModifiedTime": fmt_dt(&self.last_modified_time),
            "CreatedBy": iam_root(account),
            "LastModifiedBy": iam_root(account),
        })
    }
}

fn iam_root(account: u64) -> String {
    format!("arn:aws:iam::{account}:root")
}

// ---------------------------------------------------------------------------
// Activation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Activation {
    pub activation_id: String,
    pub iam_role: String,
    pub description: String,
    pub default_instance_name: String,
    pub registration_limit: i64,
    pub registrations_count: i64,
    pub expired: bool,
    pub expiration_date: String,
    pub created: DateTime<Utc>,
}

impl Activation {
    pub fn to_json(&self) -> Value {
        json!({
            "ActivationId": self.activation_id,
            "Description": self.description,
            "DefaultInstanceName": self.default_instance_name,
            "IamRole": self.iam_role,
            "RegistrationLimit": self.registration_limit,
            "RegistrationsCount": self.registrations_count,
            "ExpirationDate": self.expiration_date,
            "Expired": self.expired,
            "CreatedDate": fmt_dt(&self.created),
            "Tags": [],
        })
    }
}

// ---------------------------------------------------------------------------
// Resource data sync
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ResourceDataSync {
    pub sync_name: String,
    pub s3_destination: Option<Value>,
    pub sync_type: String,
    pub sync_source: Option<Value>,
    pub sync_created_time: DateTime<Utc>,
    pub last_sync_time: DateTime<Utc>,
    pub last_successful_sync_time: DateTime<Utc>,
    pub sync_last_modified_time: DateTime<Utc>,
    pub last_status: String,
}

impl ResourceDataSync {
    pub fn to_json(&self) -> Value {
        let mut result = json!({
            "SyncName": self.sync_name,
            "SyncType": self.sync_type,
            "SyncCreatedTime": fmt_dt(&self.sync_created_time),
            "LastSyncTime": fmt_dt(&self.last_sync_time),
            "LastSuccessfulSyncTime": fmt_dt(&self.last_successful_sync_time),
            "SyncLastModifiedTime": fmt_dt(&self.sync_last_modified_time),
            "LastStatus": self.last_status,
        });
        if let Some(d) = &self.s3_destination {
            result["S3Destination"] = d.clone();
        }
        if let Some(s) = &self.sync_source {
            result["SyncSource"] = s.clone();
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Automation executions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AutomationExecution {
    pub automation_execution_id: String,
    pub document_name: String,
    pub document_version: String,
    pub parameters: Option<Value>,
    pub target_parameter_name: Option<String>,
    pub targets: Option<Value>,
    pub mode: String,
    pub max_concurrency: Option<String>,
    pub max_errors: Option<String>,
    pub automation_execution_status: String,
    pub execution_start_time: DateTime<Utc>,
    pub execution_end_time: Option<DateTime<Utc>>,
    pub executed_by: String,
}

impl AutomationExecution {
    pub fn to_json(&self) -> Value {
        let mut result = json!({
            "AutomationExecutionId": self.automation_execution_id,
            "DocumentName": self.document_name,
            "DocumentVersion": self.document_version,
            "AutomationExecutionStatus": self.automation_execution_status,
            "ExecutionStartTime": fmt_dt(&self.execution_start_time),
            "ExecutedBy": self.executed_by,
            "Outputs": {},
            "Mode": self.mode,
            "Parameters": self.parameters.clone().unwrap_or_else(|| json!({})),
            "Targets": self.targets.clone().unwrap_or_else(|| json!([])),
        });
        if let Some(end) = &self.execution_end_time {
            result["ExecutionEndTime"] = json!(fmt_dt(end));
        }
        if let Some(mc) = &self.max_concurrency {
            result["MaxConcurrency"] = json!(mc);
        }
        if let Some(me) = &self.max_errors {
            result["MaxErrors"] = json!(me);
        }
        result
    }
}

// ---------------------------------------------------------------------------
// The per-(account, region) state store
// ---------------------------------------------------------------------------

/// Documents container storing all versions of a document.
#[derive(Debug, Default)]
pub struct Documents {
    pub versions: std::collections::BTreeMap<String, Arc<Document>>,
    pub default_version: String,
    pub latest_version: String,
    pub permissions: RwLock<Vec<(String, String, DateTime<Utc>)>>, // (account_id, version, created_at)
}

pub type TagTable = std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
>;
pub type DocumentTable = std::collections::BTreeMap<String, Arc<RwLock<Documents>>>;
pub type PatchGroupTable = std::collections::BTreeMap<
    String,
    Arc<RwLock<std::collections::BTreeMap<Option<String>, String>>>,
>;
pub type DocumentTagTable = std::collections::BTreeMap<String, Vec<(String, String)>>;

#[derive(Clone, Default)]
pub struct SsmState {
    pub parameters: Arc<RwLock<std::collections::BTreeMap<String, Arc<Parameter>>>>,
    pub documents: Arc<RwLock<DocumentTable>>,
    pub document_tags: Arc<RwLock<DocumentTagTable>>,
    pub resource_tags: Arc<RwLock<TagTable>>,
    pub commands: Arc<RwLock<Vec<Arc<Command>>>>,
    pub windows: Arc<RwLock<std::collections::BTreeMap<String, Arc<MaintenanceWindow>>>>,
    pub baselines: Arc<RwLock<std::collections::BTreeMap<String, Arc<PatchBaseline>>>>,
    pub patch_groups: Arc<RwLock<PatchGroupTable>>,
    pub associations: Arc<RwLock<std::collections::BTreeMap<String, Arc<RwLock<Association>>>>>,
    pub ops_items: Arc<RwLock<std::collections::BTreeMap<String, Arc<RwLock<OpsItem>>>>>,
    pub ops_metadata: Arc<RwLock<std::collections::BTreeMap<String, Arc<RwLock<OpsMetadata>>>>>,
    pub activations: Arc<RwLock<std::collections::BTreeMap<String, Arc<Activation>>>>,
    pub resource_data_syncs: Arc<RwLock<std::collections::BTreeMap<String, Arc<RwLock<ResourceDataSync>>>>>,
    pub automation_executions: Arc<RwLock<std::collections::BTreeMap<String, Arc<AutomationExecution>>>>,
    pub resource_policies: Arc<RwLock<std::collections::BTreeMap<String, RwLock<Value>>>>,
}

impl SsmState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_parameter(&self, name: &str) -> Option<Arc<Parameter>> {
        self.parameters.read().get(name).cloned()
    }

    /// Apply moto's ParameterDict name normalization.
    pub fn normalize_name(name: &str) -> String {
        if name.is_empty() {
            return name.to_string();
        }
        let stripped = name.strip_prefix('/').unwrap_or(name);
        if stripped.contains('/') {
            name.to_string()
        } else {
            stripped.to_string()
        }
    }

    /// Resolve a parameter by name (possibly with `:version` / `:label` selector).
    /// Mirrors moto: numeric selectors raise `ParameterVersionNotFound` when absent.
    pub fn resolve_parameter(
        &self,
        raw_name: &str,
    ) -> Result<Option<(Arc<Parameter>, ParamVersion)>, String> {
        let name = raw_name.to_string();
        let parts: Vec<&str> = name.split(':').collect();
        if parts.len() > 2 {
            return Ok(None);
        }
        let key = SsmState::normalize_name(parts[0]);
        let Some(param) = self.get_parameter(&key) else {
            return Ok(None);
        };
        if parts.len() == 1 {
            return Ok(param.current().map(|v| (param, v)));
        }
        let selector = parts[1];
        if selector.chars().all(|c| c.is_ascii_digit()) {
            if let Some(v) = param.version(selector.parse().unwrap_or(0)) {
                return Ok(Some((param, v)));
            }
            let non_empty = !param.versions.read().is_empty();
            if non_empty {
                return Err(name);
            }
            return Ok(None);
        }
        Ok(param.version_by_label(selector).map(|v| (param, v)))
    }

    /// Add a tag pair to a tagged resource type.
    pub fn add_tag(&self, resource_type: &str, resource_id: &str, key: &str, value: &str) {
        let mut tags = self.resource_tags.write();
        tags.entry(resource_type.to_string())
            .or_default()
            .entry(resource_id.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
    }

    pub fn list_tags(&self, resource_type: &str, resource_id: &str) -> Value {
        let tags = self.resource_tags.read();
        match tags
            .get(resource_type)
            .and_then(|t| t.get(resource_id))
        {
            Some(m) => {
                let list: Vec<Value> = m
                    .iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect();
                json!(list)
            }
            None => json!([]),
        }
    }

    pub fn document_tags(&self, name: &str) -> Value {
        // Tags for documents live in the shared resource_tags table
        // (add_tag("Document", ...)); the old document_tags table had no
        // writers, which left CreateDocument's Tags echo reading empty.
        let tags = self
            .resource_tags
            .read()
            .get("Document")
            .and_then(|m| m.get(name))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        json!(tags)
    }

    pub fn add_document_tag_entry(&self, dt: &Document, key: &str, value: &str) {
        let mut tags = self.document_tags.write();
        let entry = tags.entry(dt.name.clone()).or_default();
        entry.retain(|(k, _)| k != key);
        entry.push((key.to_string(), value.to_string()));
    }
}

/// Ops metadata container.
#[allow(dead_code)]
#[derive(Debug)]
pub struct OpsMetadata {
    pub resource_id: String,
    pub metadata: RwLock<Value>,
    pub created: DateTime<Utc>,
    pub last_modified: RwLock<DateTime<Utc>>,
}

impl OpsMetadata {
    pub fn arn(&self, account: u64, region: &str) -> String {
        format!("arn:aws:ssm:{region}:{account}:opsmetadata/{}", self.resource_id)
    }
}
