//! CloudWatch Logs in-memory state models.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A single log event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEventItem {
    pub timestamp: i64,
    pub ingestion_time: i64,
    pub message: String,
    pub event_id: u64,
}

/// A log stream.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LogStreamRec {
    pub name: String,
    /// Full ARN without any ":*" suffix.
    pub arn: String,
    pub creation_time: i64,
    pub stored_bytes: i64,
    pub upload_sequence_token: u64,
    pub first_event_timestamp: Option<i64>,
    pub last_event_timestamp: Option<i64>,
    pub last_ingestion_time: Option<i64>,
    pub events: Vec<LogEventItem>,
}

/// A log group with its nested streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogGroupRec {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub retention_in_days: Option<i64>,
    pub kms_key_id: Option<String>,
    pub streams: BTreeMap<String, LogStreamRec>,
    pub tags: BTreeMap<String, String>,
}

/// A subscription filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionFilterRec {
    pub log_group_name: String,
    pub filter_name: String,
    pub filter_pattern: String,
    pub destination_arn: String,
    pub role_arn: String,
    pub creation_time: i64,
}

/// A metric filter definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFilterRec {
    pub log_group_name: String,
    pub filter_name: String,
    pub filter_pattern: String,
    pub metric_transformations: serde_json::Value,
}

/// A destination for subscription filters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationRec {
    pub destination_name: String,
    pub arn: String,
    pub target_arn: String,
    pub role_arn: String,
    pub access_policy: Option<String>,
}

/// A resource-wide log access policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePolicyRec {
    pub policy_name: String,
    pub policy_document: String,
    pub description: Option<String>,
    pub last_updated_time: i64,
}

/// A CloudWatch Logs export task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportTaskRec {
    pub task_id: String,
    pub task_name: Option<String>,
    pub log_group_name: String,
    pub destination: String,
    pub from_time: i64,
    pub to: i64,
}

/// A saved Insights query definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinitionRec {
    pub query_definition_id: String,
    pub name: String,
    pub query_string: String,
    pub log_group_names: serde_json::Value,
    pub last_modified: i64,
}

/// A started Insights query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRec {
    pub query_id: String,
    pub status: String,
}

/// A log anomaly detector (new API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetectorRec {
    pub arn: String,
    pub log_group_arn_list: serde_json::Value,
    pub evaluation_frequency: Option<String>,
    pub filter_pattern: Option<String>,
    pub enabled: bool,
}

/// An account-level policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountPolicyRec {
    pub policy_name: String,
    pub policy_document: String,
    pub policy_type: String,
    pub scope: String,
    pub last_updated_time: i64,
}

/// A delivery source (new delivery API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverySourceRec {
    pub name: String,
    pub arn: String,
    pub resource_arns: serde_json::Value,
    pub service: String,
    pub log_type: String,
}

/// A delivery destination (new delivery API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryDestinationRec {
    pub name: String,
    pub arn: String,
    pub delivery_destination_type: String,
    pub output_format: Option<String>,
    pub configuration: serde_json::Value,
    pub policy: Option<String>,
}

/// A delivery linking source and destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRec {
    pub id: String,
    pub arn: String,
    pub delivery_source_name: String,
    pub delivery_destination_arn: String,
    pub delivery_destination_type: String,
}

/// Stored Insights query results: queryId -> (rows, statistics).
type QueryResultStore = BTreeMap<String, (Vec<serde_json::Value>, serde_json::Value)>;

/// The CloudWatch Logs state store (per account+region).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LogsState {
    #[serde(with = "crate::snapshot::arc_rw")]
    pub log_groups: Arc<RwLock<BTreeMap<String, LogGroupRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub subscription_filters: Arc<RwLock<Vec<SubscriptionFilterRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub metric_filters: Arc<RwLock<Vec<MetricFilterRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub destinations: Arc<RwLock<BTreeMap<String, DestinationRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub resource_policies: Arc<RwLock<BTreeMap<String, ResourcePolicyRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub export_tasks: Arc<RwLock<BTreeMap<String, ExportTaskRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub query_definitions: Arc<RwLock<BTreeMap<String, QueryDefinitionRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub queries: Arc<RwLock<BTreeMap<String, QueryRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub anomaly_detectors: Arc<RwLock<BTreeMap<String, AnomalyDetectorRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub account_policies: Arc<RwLock<BTreeMap<String, AccountPolicyRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub delivery_sources: Arc<RwLock<BTreeMap<String, DeliverySourceRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub delivery_destinations: Arc<RwLock<BTreeMap<String, DeliveryDestinationRec>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub deliveries: Arc<RwLock<BTreeMap<String, DeliveryRec>>>,
    /// Stored Insights query results: queryId -> (rows, statistics).
    #[serde(with = "crate::snapshot::arc_rw")]
    pub query_results: Arc<RwLock<QueryResultStore>>,
    /// Tags keyed by resource ARN (TagResource/UntagResource accept any ARN).
    #[serde(with = "crate::snapshot::arc_rw")]
    pub arn_tags: Arc<RwLock<BTreeMap<String, BTreeMap<String, String>>>>,
}

impl LogsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_group(&self, name: &str) -> Option<LogGroupRec> {
        self.log_groups.read().get(name).cloned()
    }
}
