//! CloudWatch Logs operation handler.
//!
//! Mirrors the moto-backed ground truth: JSON protocol (target prefix
//! Logs_20140328) with lowerCamelCase parameter names, moto-compatible
//! response shapes, tokens and error codes.

use crate::models::{
    AccountPolicyRec, AnomalyDetectorRec, DeliveryDestinationRec, DeliveryRec, DeliverySourceRec,
    DestinationRec, ExportTaskRec, LogEventItem, LogGroupRec, LogStreamRec, LogsState,
    MetricFilterRec, QueryDefinitionRec, QueryRec, ResourcePolicyRec, SubscriptionFilterRec,
};
use crate::protocol::{AwsRequest, AwsResponse};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LogsHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), LogsState>>,
}

/// Retention values accepted by AWS.
const VALID_RETENTION_DAYS: &[i64] = &[
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922,
    3288, 3653,
];

fn next_event_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::SeqCst)
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Tags parameter: map of {"key": "value"}.
fn tags_param(params: &Value, key: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(Value::Object(map)) = params.get(key) {
        for (k, v) in map {
            out.insert(k.clone(), v.as_str().unwrap_or("").to_string());
        }
    }
    out
}

/// Extract the "limit N" clause of a Logs Insights query.
fn extract_query_limit(query_string: &str) -> usize {
    for clause in query_string.split('|') {
        if let Some(rest) = clause.trim().strip_prefix("limit") {
            if let Ok(n) = rest.trim().parse::<usize>() {
                return n;
            }
        }
    }
    1000
}

/// moto-compatible event message matcher.
fn pattern_matches(pattern: &str, message: &str) -> bool {
    match pattern.chars().next() {
        None => true,
        Some(c) if c.is_alphabetic() => {
            // Single term: every space-separated word must be a substring.
            pattern.split(' ').all(|word| message.contains(word))
        }
        Some('"') => pattern.len() >= 2 && message.contains(&pattern[1..pattern.len() - 1]),
        Some(_) => true, // unsupported pattern types match everything
    }
}

/// moto-style token: "f/<56-digit index>" and "b/<56-digit index>".
fn token(direction: char, index: usize) -> String {
    format!("{direction}/{index:056}")
}

fn parse_token(tok: &str) -> Option<(char, usize)> {
    let mut chars = tok.chars();
    match chars.next() {
        Some(d @ ('f' | 'b')) => {
            if chars.next() != Some('/') {
                return None;
            }
            let rest = &tok[2..];
            if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let trimmed = rest.trim_start_matches('0');
            let index = if trimmed.is_empty() {
                0
            } else {
                trimmed.parse::<usize>().ok()?
            };
            Some((d, index))
        }
        _ => None,
    }
}

impl LogsHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> LogsState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    // ---- shared response helpers ----

    fn group_json_with_filters(
        g: &LogGroupRec,
        metric_filter_count: usize,
    ) -> Value {
        let stored: i64 = g.streams.values().map(|s| s.stored_bytes).sum();
        let mut out = json!({
            "arn": format!("{}:*", g.arn),
            "logGroupArn": g.arn,
            "creationTime": g.creation_time,
            "logGroupName": g.name,
            "metricFilterCount": metric_filter_count,
            "storedBytes": stored,
        });
        if let Some(d) = g.retention_in_days {
            out["retentionInDays"] = json!(d);
        }
        if let Some(k) = &g.kms_key_id {
            out["kmsKeyId"] = json!(k);
        }
        out
    }

    fn stream_json(s: &LogStreamRec) -> Value {
        let mut out = json!({
            "arn": s.arn,
            "creationTime": s.creation_time,
            "logStreamName": s.name,
            "storedBytes": s.stored_bytes,
        });
        if !s.events.is_empty() {
            out["firstEventTimestamp"] = json!(s.first_event_timestamp);
            out["lastEventTimestamp"] = json!(s.last_event_timestamp);
            out["lastIngestionTime"] = json!(s.last_ingestion_time);
            out["uploadSequenceToken"] = json!(format!("{:056}", s.upload_sequence_token));
        }
        out
    }

    fn not_found(msg: &str) -> AwsResponse {
        AwsResponse::error(400, "ResourceNotFoundException", msg)
    }

    fn missing_group() -> AwsResponse {
        Self::not_found("The specified log group does not exist.")
    }

    fn missing_stream() -> AwsResponse {
        Self::not_found("The specified log stream does not exist.")
    }

    fn invalid_param(msg: &str) -> AwsResponse {
        AwsResponse::error(400, "InvalidParameterException", msg)
    }

    fn str_param<'a>(params: &'a Value, key: &str) -> &'a str {
        params.get(key).and_then(|v| v.as_str()).unwrap_or("")
    }

    fn limit_param(params: &Value, key: &str, default: usize) -> usize {
        params
            .get(key)
            .and_then(|v| v.as_i64())
            .map(|v| v.clamp(0, 10_000) as usize)
            .unwrap_or(default)
    }

    fn str_vec(params: &Value, key: &str) -> Vec<String> {
        params
            .get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// ARN with the trailing DescribeLogGroups ":*" removed.
    fn arn_without_suffix(arn: &str) -> String {
        arn.strip_suffix(":*").unwrap_or(arn).to_string()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        match req.operation.as_str() {
            "CreateLogGroup" => self.create_log_group(&req),
            "DeleteLogGroup" => self.delete_log_group(&req),
            "DescribeLogGroups" => self.describe_log_groups(&req),
            "GetLogGroup" => self.get_log_group(&req),
            "CreateLogStream" => self.create_log_stream(&req),
            "DeleteLogStream" => self.delete_log_stream(&req),
            "DescribeLogStreams" => self.describe_log_streams(&req),
            "PutLogEvents" => self.put_log_events(&req),
            "GetLogEvents" => self.get_log_events(&req),
            "FilterLogEvents" => self.filter_log_events(&req),
            "PutRetentionPolicy" => self.put_retention_policy(&req, true),
            "DeleteRetentionPolicy" => self.put_retention_policy(&req, false),
            "AssociateKmsKey" => self.set_kms_key(&req, true),
            "DisassociateKmsKey" => self.set_kms_key(&req, false),
            "ListTagsLogGroup" => self.list_tags_log_group(&req),
            "TagLogGroup" => self.tag_log_group(&req),
            "UntagLogGroup" => self.untag_log_group(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),

            // NOTE (documented absence): PutMetricFilter stores the row and
            // DescribeMetricFilters reads it, but PutLogEvents never APPLIES
            // the transformation — no datapoints are emitted to CloudWatch
            // from metric filters, and PutSubscriptionFilter records are
            // stored without any delivery path. moto pushes metric filters
            // into the CW backend; the twin does NOT (gap vs moto, not a
            // 501-class absence — the describe/store surface works).
            "PutMetricFilter" => self.put_metric_filter(&req),
            "DeleteMetricFilter" => self.delete_metric_filter(&req),
            "DescribeMetricFilters" => self.describe_metric_filters(&req),
            "TestMetricFilter" => AwsResponse::json(200, json!({})),

            "PutSubscriptionFilter" => self.put_subscription_filter(&req),
            "DeleteSubscriptionFilter" => self.delete_subscription_filter(&req),
            "DescribeSubscriptionFilters" => self.describe_subscription_filters(&req),

            "PutDestination" => self.put_destination(&req),
            "DeleteDestination" => self.delete_destination(&req),
            "DescribeDestinations" => self.describe_destinations(&req),
            "PutDestinationPolicy" => self.put_destination_policy(&req),

            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DescribeResourcePolicies" => self.describe_resource_policies(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),

            "CreateExportTask" => self.create_export_task(&req),
            "DescribeExportTasks" => self.describe_export_tasks(&req),
            "CancelExportTask" => self.cancel_export_task(&req),

            "StartQuery" => self.start_query(&req),
            "GetQueryResults" => self.get_query_results(&req),
            "StopQuery" => self.stop_query(&req),
            "DescribeQueries" => self.describe_queries(&req),

            "PutQueryDefinition" => self.put_query_definition(&req),
            "DescribeQueryDefinitions" => self.describe_query_definitions(&req),
            "DeleteQueryDefinition" => self.delete_query_definition(&req),
            "ListLogGroupsForQuery" => Self::not_found("Query not found"),
            "GetQueryDefinition" => Self::not_found("Query definition not found"),

            "CreateLogAnomalyDetector" => self.create_log_anomaly_detector(&req),
            "DeleteLogAnomalyDetector" => self.delete_log_anomaly_detector(&req),
            "GetLogAnomalyDetector" => self.get_log_anomaly_detector(&req),
            "UpdateLogAnomalyDetector" => self.update_log_anomaly_detector(&req),
            "ListLogAnomalyDetectors" => self.list_log_anomaly_detectors(&req),
            "ListAnomalies" => AwsResponse::json(200, json!({ "anomalies": [] })),
            "UpdateAnomaly" => Self::not_found("Anomaly detector not found"),

            "PutAccountPolicy" => self.put_account_policy(&req),
            "DescribeAccountPolicies" => self.describe_account_policies(&req),
            "DeleteAccountPolicy" => self.delete_account_policy(&req),

            "PutDeliverySource" => self.put_delivery_source(&req),
            "GetDeliverySource" => self.get_delivery_source(&req),
            "DeleteDeliverySource" => self.delete_delivery_source(&req),
            "DescribeDeliverySources" => self.describe_delivery_sources(&req),
            "PutDeliveryDestination" => self.put_delivery_destination(&req),
            "GetDeliveryDestination" => self.get_delivery_destination(&req),
            "DeleteDeliveryDestination" => self.delete_delivery_destination(&req),
            "DescribeDeliveryDestinations" => self.describe_delivery_destinations(&req),
            "PutDeliveryDestinationPolicy" => self.delivery_destination_policy(&req, true),
            "GetDeliveryDestinationPolicy" => self.delivery_destination_policy(&req, false),
            "DeleteDeliveryDestinationPolicy" => self.delete_delivery_destination_policy(&req),
            "CreateDelivery" => self.create_delivery(&req),
            "GetDelivery" => self.get_delivery(&req),
            "DeleteDelivery" => self.delete_delivery(&req),
            "DescribeDeliveries" => AwsResponse::json(200, json!({ "deliveries": [] })),
            "UpdateDeliveryConfiguration" => {
                Self::not_found("Requested Delivery does not exist in this account.")
            }

            // Group-level policies addressed by logGroupIdentifier.
            "GetDataProtectionPolicy"
            | "PutDataProtectionPolicy"
            | "DeleteDataProtectionPolicy"
            | "PutIndexPolicy"
            | "DeleteIndexPolicy"
            | "PutBearerTokenAuthentication"
            | "PutLogGroupDeletionProtection" => self.group_identifier_guard(&req),
            "GetTransformer" | "DeleteTransformer" => {
                // Transformers are never stored: RNFE regardless (moto parity).
                Self::not_found("The specified log group does not have a transformer.")
            }

            "GetLogGroupFields" => AwsResponse::json(
                200,
                json!({ "logGroupFields": [
                    { "name": "@timestamp", "percent": 100 },
                    { "name": "@message", "percent": 100 },
                    { "name": "@logStream", "percent": 100 },
                ]}),
            ),
            "GetLogFields" => AwsResponse::json(200, json!({})),
            "ListLogGroups" => self.list_log_groups(&req),
            "ListAggregateLogGroupSummaries" => {
                AwsResponse::json(200, json!({ "aggregateLogGroupSummaries": [] }))
            }
            "ListScheduledQueries" => AwsResponse::json(200, json!({ "scheduledQueries": [] })),
            "ListIntegrations" => AwsResponse::json(200, json!({ "integrationSummaries": [] })),
            "GetScheduledQuery" | "DeleteScheduledQuery" => {
                Self::not_found("Scheduled query not found")
            }
            "DescribeConfigurationTemplates" => {
                AwsResponse::json(200, json!({ "configurationTemplates": [] }))
            }
            "DescribeImportTasks" => AwsResponse::json(200, json!({ "importTasks": [] })),
            "DescribeImportTaskBatches" => AwsResponse::json(200, json!({ "importBatches": [] })),
            "CancelImportTask" => Self::not_found("Import task not found"),
            "GetLogRecord" => Self::not_found("Record pointer not found"),
            "GetIntegration" | "DeleteIntegration" | "PutIntegration" => {
                Self::not_found("Integration not found")
            }
            other => AwsResponse::error(
                400,
                "ValidationException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    /// Guard shared by new group-level policy APIs addressed by
    /// logGroupIdentifier: RNFE when the group does not exist.
    fn group_identifier_guard(&self, req: &AwsRequest) -> AwsResponse {
        let identifier = Self::str_param(&req.params, "logGroupIdentifier");
        if identifier.is_empty() {
            return Self::invalid_param("logGroupIdentifier is required");
        }
        let state = self.get_state(req.account, &req.region);
        if state.get_group(identifier).is_none() {
            let arn = Self::arn_without_suffix(identifier);
            let exists = state.log_groups.read().values().any(|g| g.arn == arn);
            if !exists {
                return Self::missing_group();
            }
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- log groups ----

    fn create_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        if name.is_empty() {
            return Self::invalid_param("logGroupName is required");
        }
        let state = self.get_state(req.account, &req.region);
        if state.get_group(&name).is_some() {
            return AwsResponse::error(
                400,
                "ResourceAlreadyExistsException",
                "The specified log group already exists",
            );
        }
        let kms = Self::str_param(&req.params, "kmsKeyId");
        let group = LogGroupRec {
            name: name.clone(),
            arn: format!(
                "arn:aws:logs:{}:{}:log-group:{}",
                req.region, req.account, name
            ),
            creation_time: now_millis(),
            retention_in_days: None,
            kms_key_id: if kms.is_empty() {
                None
            } else {
                Some(kms.to_string())
            },
            streams: BTreeMap::new(),
            tags: tags_param(&req.params, "tags"),
        };
        if !group.tags.is_empty() {
            state
                .arn_tags
                .write()
                .insert(group.arn.clone(), group.tags.clone());
        }
        state.log_groups.write().insert(name, group);
        AwsResponse::json(200, json!({}))
    }

    fn delete_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        if state.log_groups.write().remove(&name).is_none() {
            return Self::missing_group();
        }
        state
            .subscription_filters
            .write()
            .retain(|f| f.log_group_name != name);
        state
            .metric_filters
            .write()
            .retain(|f| f.log_group_name != name);
        AwsResponse::json(200, json!({}))
    }

    fn describe_log_groups(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let prefix = Self::str_param(&req.params, "logGroupNamePrefix");
        let limit = Self::limit_param(&req.params, "limit", 50);
        let groups: Vec<Value> = state
            .log_groups
            .read()
            .iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .map(|(name, g)| {
                // The real count of PutMetricFilter rows on this group
                // (the hardcoded 0 contradicted DescribeMetricFilters).
                let count = state
                    .metric_filters
                    .read()
                    .iter()
                    .filter(|f| f.log_group_name == *name)
                    .count();
                Self::group_json_with_filters(g, count)
            })
            .take(limit)
            .collect();
        AwsResponse::json(
            200,
            json!({
                "logGroups": groups,
                "nextToken": Value::Null
            }),
        )
    }

    fn get_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = Self::str_param(&req.params, "logGroupName");
        match state.get_group(name) {
            Some(g) => {
                let metric_filter_count = state.metric_filters.read().iter().filter(|f| f.log_group_name == *name).count();
                AwsResponse::json(200, Self::group_json_with_filters(&g, metric_filter_count))
            }
            None => Self::missing_group(),
        }
    }

    fn list_log_groups(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let prefix = Self::str_param(&req.params, "logGroupNamePrefix");
        let limit = Self::limit_param(&req.params, "limit", 50);
        let groups: Vec<Value> = state
            .log_groups
            .read()
            .values()
            .filter(|g| g.name.starts_with(prefix))
            .map(|g| {
                let count = state
                    .metric_filters
                    .read()
                    .iter()
                    .filter(|f| f.log_group_name == g.name)
                    .count();
                Self::group_json_with_filters(g, count)
            })
            .take(limit)
            .collect();
        AwsResponse::json(200, json!({ "logGroups": groups }))
    }

    // ---- retention & kms ----

    fn put_retention_policy(&self, req: &AwsRequest, is_put: bool) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        let days = if is_put {
            match req.params.get("retentionInDays").and_then(|v| v.as_i64()) {
                Some(d) if VALID_RETENTION_DAYS.contains(&d) => Some(d),
                Some(_) => {
                    return Self::invalid_param(
                        "1 validation error detected: Value at 'retentionInDays' failed to satisfy constraint: Member must satisfy enum value set: [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653]",
                    );
                }
                None => return Self::invalid_param("retentionInDays is required"),
            }
        } else {
            None
        };
        let mut groups = state.log_groups.write();
        match groups.get_mut(&name) {
            Some(g) => {
                g.retention_in_days = days;
                AwsResponse::json(200, json!({}))
            }
            None => Self::missing_group(),
        }
    }

    fn set_kms_key(&self, req: &AwsRequest, associate: bool) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        let key = Self::str_param(&req.params, "kmsKeyId");
        if associate && key.is_empty() {
            return Self::invalid_param("kmsKeyId is required");
        }
        let mut groups = state.log_groups.write();
        match groups.get_mut(&name) {
            Some(g) => {
                g.kms_key_id = if associate {
                    Some(key.to_string())
                } else {
                    None
                };
                AwsResponse::json(200, json!({}))
            }
            None => Self::missing_group(),
        }
    }

    // ---- tags ----

    fn list_tags_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        match state.get_group(&name) {
            Some(g) => AwsResponse::json(200, json!({ "tags": g.tags })),
            None => Self::missing_group(),
        }
    }

    /// Apply tag mutations to the group addressed by `logGroupName` (legacy API)
    /// or `resourceArn` (new API).
    fn mutate_tags(
        &self,
        req: &AwsRequest,
        set: &[(String, String)],
        unset: &[String],
    ) -> AwsResponse {
        let arn = Self::str_param(&req.params, "resourceArn").to_string();
        let name = Self::str_param(&req.params, "logGroupName").to_string();
        let state = self.get_state(req.account, &req.region);
        let by_name = !name.is_empty();
        let mut groups = state.log_groups.write();
        let mut found = false;
        let mut matched_arn: Option<String> = None;
        for g in groups.values_mut() {
            if (by_name && g.name == name) || (!by_name && g.arn == Self::arn_without_suffix(&arn))
            {
                found = true;
                matched_arn = Some(g.arn.clone());
                for (k, v) in set {
                    g.tags.insert(k.clone(), v.clone());
                }
                for k in unset {
                    g.tags.remove(k);
                }
                break;
            }
        }
        if let Some(arn) = matched_arn {
            // Keep the ARN-keyed tag store (ListTagsForResource) in sync.
            if found {
                let tags = groups
                    .values()
                    .find(|g| g.arn == arn)
                    .map(|g| g.tags.clone())
                    .unwrap_or_default();
                state.arn_tags.write().insert(arn, tags);
            }
        }
        if !found {
            return Self::missing_group();
        }
        AwsResponse::json(200, json!({}))
    }

    fn tag_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let tags: Vec<(String, String)> = tags_param(&req.params, "tags").into_iter().collect();
        self.mutate_tags(req, &tags, &[])
    }

    fn untag_log_group(&self, req: &AwsRequest) -> AwsResponse {
        let keys = Self::str_vec(&req.params, "tags");
        self.mutate_tags(req, &[], &keys)
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let arn = Self::arn_without_suffix(Self::str_param(&req.params, "resourceArn"));
        let tags = state.arn_tags.read().get(&arn).cloned().unwrap_or_default();
        AwsResponse::json(200, json!({ "tags": tags }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let arn = Self::arn_without_suffix(Self::str_param(&req.params, "resourceArn"));
        let tags = tags_param(&req.params, "tags");
        if !tags.is_empty() {
            let mut all = state.arn_tags.write();
            let entry = all.entry(arn).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let arn = Self::arn_without_suffix(Self::str_param(&req.params, "resourceArn"));
        let keys = Self::str_vec(&req.params, "tagKeys");
        let mut all = state.arn_tags.write();
        if let Some(entry) = all.get_mut(&arn) {
            for k in &keys {
                entry.remove(k);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- log streams ----

    fn create_log_stream(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let group_name = Self::str_param(&req.params, "logGroupName");
        let stream_name = Self::str_param(&req.params, "logStreamName");
        let mut groups = state.log_groups.write();
        match groups.get_mut(group_name) {
            Some(g) => {
                if g.streams.contains_key(stream_name) {
                    return AwsResponse::error(
                        400,
                        "ResourceAlreadyExistsException",
                        "The specified log stream already exists",
                    );
                }
                g.streams.insert(
                    stream_name.to_string(),
                    LogStreamRec {
                        name: stream_name.to_string(),
                        arn: format!("{}:log-stream:{}", g.arn, stream_name),
                        creation_time: now_millis(),
                        ..Default::default()
                    },
                );
                AwsResponse::json(200, json!({}))
            }
            None => Self::missing_group(),
        }
    }

    fn delete_log_stream(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let group_name = Self::str_param(&req.params, "logGroupName");
        let stream_name = Self::str_param(&req.params, "logStreamName");
        let mut groups = state.log_groups.write();
        match groups.get_mut(group_name) {
            Some(g) => {
                if g.streams.remove(stream_name).is_none() {
                    return Self::missing_stream();
                }
                AwsResponse::json(200, json!({}))
            }
            None => Self::missing_group(),
        }
    }

    fn describe_log_streams(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let group_name = Self::str_param(&req.params, "logGroupName");
        let prefix = Self::str_param(&req.params, "logStreamNamePrefix");
        let limit = Self::limit_param(&req.params, "limit", 50);
        let order_by = Self::str_param(&req.params, "orderBy");
        let descending = req
            .params
            .get("descending")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        match state.get_group(group_name) {
            Some(g) => {
                let mut streams: Vec<&LogStreamRec> = g
                    .streams
                    .values()
                    .filter(|s| s.name.starts_with(prefix))
                    .collect();
                if order_by.is_empty() || order_by == "LogStreamName" {
                    streams.sort_by(|a, b| a.name.cmp(&b.name));
                } else {
                    streams.sort_by_key(|s| s.last_event_timestamp.unwrap_or(0));
                }
                if descending {
                    streams.reverse();
                }
                let page: Vec<Value> = streams
                    .iter()
                    .take(limit)
                    .map(|s| Self::stream_json(s))
                    .collect();
                AwsResponse::json(
                    200,
                    json!({
                        "logStreams": page,
                        "nextToken": Value::Null
                    }),
                )
            }
            None => Self::missing_group(),
        }
    }

    // ---- log events ----

    fn put_log_events(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName").to_string();
        let stream_name = Self::str_param(params, "logStreamName").to_string();
        let state = self.get_state(req.account, &req.region);

        let mut groups = state.log_groups.write();
        let group = match groups.get_mut(&group_name) {
            Some(g) => g,
            None => return Self::missing_group(),
        };
        let ingestion = now_millis();
        let stream = match group.streams.get_mut(&stream_name) {
            Some(s) => s,
            None => return Self::missing_stream(),
        };

        let events: Vec<Value> = params
            .get("logEvents")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Reject events older than 14 days / newer than 2 hours in the future.
        let oldest = now_millis() - 14 * 24 * 3600 * 1000;
        let newest = now_millis() + 2 * 3600 * 1000;
        let mut rejected = Map::new();
        let mut ingested: Vec<LogEventItem> = Vec::new();
        let mut prev_ts: Option<i64> = None;
        for (idx, e) in events.iter().enumerate() {
            let ts = match e.get("timestamp").and_then(|v| v.as_i64()) {
                Some(t) => t,
                None => return Self::invalid_param("timestamp is required"),
            };
            if prev_ts.is_some_and(|p| p > ts) {
                return Self::invalid_param(
                    "Log events in a single PutLogEvents request must be in chronological order.",
                );
            }
            if ts < oldest {
                rejected.insert("tooOldLogEventEndIndex".to_string(), json!(idx));
            } else if ts > newest {
                rejected.insert("tooNewLogEventStartIndex".to_string(), json!(idx));
            } else {
                ingested.push(LogEventItem {
                    timestamp: ts,
                    ingestion_time: ingestion,
                    message: e
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    event_id: next_event_id(),
                });
            }
            prev_ts = Some(ts);
        }

        let candidate_token = format!("{:056}", stream.upload_sequence_token + 1);
        if !ingested.is_empty() {
            let stored_len: i64 = ingested.iter().map(|e| e.message.len() as i64).sum();
            let min_ts = ingested.iter().map(|e| e.timestamp).min();
            let max_ts = ingested.iter().map(|e| e.timestamp).max();
            stream.upload_sequence_token = stream.upload_sequence_token.saturating_add(1);
            stream.stored_bytes += stored_len;
            stream.first_event_timestamp = stream
                .first_event_timestamp
                .zip(min_ts)
                .map(|(a, b)| a.min(b))
                .or(min_ts);
            stream.last_event_timestamp = stream
                .last_event_timestamp
                .zip(max_ts)
                .map(|(a, b)| a.max(b))
                .or(max_ts);
            stream.last_ingestion_time = Some(ingestion);
            stream.events.extend(ingested);
        }

        let mut out = Map::new();
        out.insert("nextSequenceToken".to_string(), json!(candidate_token));
        if !rejected.is_empty() {
            out.insert("rejectedLogEventsInfo".to_string(), Value::Object(rejected));
        }
        AwsResponse::json(200, Value::Object(out))
    }

    fn get_log_events(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName");
        let stream_name = Self::str_param(params, "logStreamName");
        let start_time = params.get("startTime").and_then(|v| v.as_i64());
        let end_time = params.get("endTime").and_then(|v| v.as_i64());
        let limit = Self::limit_param(params, "limit", 10_000);
        let start_from_head = params
            .get("startFromHead")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let next_token = Self::str_param(params, "nextToken");

        let state = self.get_state(req.account, &req.region);
        let group = match state.get_group(group_name) {
            Some(g) => g,
            None => return Self::missing_group(),
        };
        let stream = match group.streams.get(stream_name) {
            Some(s) => s,
            None => return Self::missing_stream(),
        };

        let events: Vec<&LogEventItem> = stream
            .events
            .iter()
            .filter(|e| {
                start_time.is_none_or(|t| e.timestamp >= t)
                    && end_time.is_none_or(|t| e.timestamp <= t)
            })
            .collect();

        // moto semantics with signed indices.
        let n = events.len() as i64;
        let final_index = n - 1;
        let limit_index = limit as i64 - 1;

        let parsed_token = if next_token.is_empty() {
            None
        } else {
            match parse_token(next_token) {
                Some(pair) => Some(pair),
                None => return Self::invalid_param("The specified nextToken is invalid."),
            }
        };

        let (mut start_index, mut end_index): (i64, i64) = match parsed_token {
            Some(('f', idx)) => (idx as i64 + 1, idx as i64 + 1 + limit_index),
            Some(('b', idx)) => (idx as i64 - 1 - limit_index, idx as i64 - 1),
            _ => {
                if start_from_head {
                    (0, limit_index)
                } else {
                    (final_index - limit_index, final_index)
                }
            }
        };

        if start_index < 0 {
            start_index = 0;
        } else if start_index > final_index {
            // Past the end: empty page, tokens anchored at the last index.
            let anchor = final_index.max(0) as usize;
            return AwsResponse::json(
                200,
                json!({
                    "events": [],
                    "nextForwardToken": token('f', anchor),
                    "nextBackwardToken": token('b', anchor),
                }),
            );
        }
        if end_index > final_index {
            end_index = final_index;
        }
        if end_index < 0 {
            return AwsResponse::json(
                200,
                json!({
                    "events": [],
                    "nextForwardToken": token('f', 0),
                    "nextBackwardToken": token('b', 0),
                }),
            );
        }

        let page: Vec<Value> = events[(start_index as usize)..=(end_index as usize)]
            .iter()
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "message": e.message,
                    "ingestionTime": e.ingestion_time,
                })
            })
            .collect();

        AwsResponse::json(
            200,
            json!({
                "events": page,
                "nextForwardToken": token('f', end_index.max(0) as usize),
                "nextBackwardToken": token('b', start_index.max(0) as usize),
            }),
        )
    }

    fn filter_log_events(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName").to_string();
        let pattern = Self::str_param(params, "filterPattern");
        let start_time = params.get("startTime").and_then(|v| v.as_i64());
        let end_time = params.get("endTime").and_then(|v| v.as_i64());
        let limit = Self::limit_param(params, "limit", 10_000);
        let next_token = Self::str_param(params, "nextToken");
        let interleaved = params
            .get("interleaved")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let stream_names = Self::str_vec(params, "logStreamNames");
        let stream_prefix = Self::str_param(params, "logStreamNamePrefix");

        let state = self.get_state(req.account, &req.region);
        let group = match state.get_group(&group_name) {
            Some(g) => g,
            None => return Self::missing_group(),
        };

        let mut searched: Vec<Value> = Vec::new();
        let mut results: Vec<Value> = Vec::new();
        for (name, stream) in group.streams.iter() {
            if !stream_names.is_empty() && !stream_names.contains(name) {
                continue;
            }
            if !stream_prefix.is_empty() && !name.starts_with(stream_prefix) {
                continue;
            }
            searched.push(json!({ "logStreamName": name, "searchedCompletely": true }));
            for e in stream.events.iter() {
                if !start_time.is_none_or(|t| e.timestamp >= t)
                    || !end_time.is_none_or(|t| e.timestamp <= t)
                {
                    continue;
                }
                if !pattern_matches(pattern, &e.message) {
                    continue;
                }
                results.push(json!({
                    "eventId": e.event_id.to_string(),
                    "ingestionTime": e.ingestion_time,
                    "logStreamName": name,
                    "message": e.message,
                    "timestamp": e.timestamp,
                }));
            }
        }
        if interleaved {
            results.sort_by_key(|e| e["timestamp"].as_i64().unwrap_or(0));
        }

        let mut first_index = 0usize;
        if !next_token.is_empty() {
            // Unrecognized tokens previously restarted from page 0 and
            // duplicated events — GetLogEvents answers InvalidParameterException
            // for a token it cannot parse; match that contract (moto parity).
            let mut found: Option<usize> = None;
            for (i, e) in results.iter().enumerate() {
                let t = format!(
                    "{}@{}@{}",
                    group_name,
                    e["logStreamName"].as_str().unwrap_or(""),
                    e["eventId"].as_str().unwrap_or("")
                );
                if t == next_token {
                    found = Some(i);
                    break;
                }
            }
            if found.is_none() {
                return Self::invalid_param("The specified nextToken is invalid.");
            }
            first_index = found.unwrap() + 1;
        }
        let last_index = results.len();
        let first_index = first_index.min(last_index);
        let page_end = first_index.saturating_add(limit).min(last_index);
        let page: Vec<Value> = results[first_index..page_end].to_vec();
        let new_token = if !page.is_empty() && page_end < last_index {
            let last = &page[page.len() - 1];
            json!(format!(
                "{}@{}@{}",
                group_name,
                last["logStreamName"].as_str().unwrap_or_default(),
                last["eventId"].as_str().unwrap_or_default()
            ))
        } else {
            Value::Null
        };

        AwsResponse::json(
            200,
            json!({
                "events": page,
                "searchedLogStreams": searched,
                "nextToken": new_token
            }),
        )
    }

    // ---- Insights queries ----

    fn start_query(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let query_string = Self::str_param(params, "queryString").to_string();
        if query_string.is_empty() {
            return Self::invalid_param("queryString is required");
        }
        let start_time = params
            .get("startTime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let end_time = params.get("endTime").and_then(|v| v.as_i64()).unwrap_or(0);
        if start_time >= end_time {
            return Self::invalid_param("Queries must have a start time earlier than the end time");
        }
        let mut log_groups = Self::str_vec(params, "logGroupNames");
        if log_groups.is_empty() {
            let single = Self::str_param(params, "logGroupName").to_string();
            if !single.is_empty() {
                log_groups.push(single);
            }
        }
        if log_groups.is_empty() {
            return Self::invalid_param("logGroupName is required");
        }
        let state = self.get_state(req.account, &req.region);
        let limit = extract_query_limit(&query_string).max(1);
        let mut rows: Vec<Value> = Vec::new();
        for name in &log_groups {
            let group = match state.get_group(name) {
                Some(g) => g,
                None => return Self::missing_group(),
            };
            for stream in group.streams.values() {
                for e in stream.events.iter() {
                    if e.timestamp >= start_time && e.timestamp <= end_time {
                        rows.push(json!([
                            { "field": "@timestamp", "value": e.timestamp.to_string() },
                            { "field": "@message", "value": e.message }
                        ]));
                    }
                }
            }
        }
        let matched = rows.len() as f64;
        rows.truncate(limit);
        let statistics = json!({
            "recordsMatched": matched,
            "recordsScanned": matched,
            "bytesScanned": matched * 16.0
        });
        let qid = uuid::Uuid::new_v4().to_string();
        state
            .query_results
            .write()
            .insert(qid.clone(), (rows, statistics));
        state.queries.write().insert(
            qid.clone(),
            QueryRec {
                query_id: qid.clone(),
                status: "Complete".to_string(),
            },
        );
        AwsResponse::json(200, json!({ "queryId": qid }))
    }

    fn get_query_results(&self, req: &AwsRequest) -> AwsResponse {
        let qid = Self::str_param(&req.params, "queryId");
        let state = self.get_state(req.account, &req.region);
        let status = state.queries.read().get(qid).map(|q| q.status.clone());
        match status {
            Some(status) => {
                let found = state.query_results.read().get(qid).cloned();
                let (results, statistics) = found.unwrap_or_else(|| (Vec::new(), json!({})));
                AwsResponse::json(
                    200,
                    json!({
                        "status": status,
                        "results": results,
                        "statistics": statistics
                    }),
                )
            }
            None => Self::not_found("Query not found"),
        }
    }

    fn stop_query(&self, req: &AwsRequest) -> AwsResponse {
        let qid = Self::str_param(&req.params, "queryId");
        let state = self.get_state(req.account, &req.region);
        let mut queries = state.queries.write();
        match queries.get_mut(qid) {
            Some(q) => {
                if q.status != "Complete" && q.status != "Failed" {
                    q.status = "Cancelled".to_string();
                }
                AwsResponse::json(200, json!({ "success": true }))
            }
            None => AwsResponse::json(200, json!({ "success": false })),
        }
    }

    fn describe_queries(&self, req: &AwsRequest) -> AwsResponse {
        let group = Self::str_param(&req.params, "logGroupName").to_string();
        let state = self.get_state(req.account, &req.region);
        let queries: Vec<Value> = state
            .queries
            .read()
            .values()
            .filter(|_q| group.is_empty())
            .map(|q| {
                json!({
                    "queryId": q.query_id,
                    "status": q.status,
                    "startTime": 0,
                    "endTime": 0,
                    "queryString": ""
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "queries": queries }))
    }

    // ---- query definitions ----

    fn put_query_definition(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let name = Self::str_param(params, "name").to_string();
        if name.is_empty() {
            return Self::invalid_param("name is required");
        }
        let query_string = Self::str_param(params, "queryString").to_string();
        if query_string.is_empty() {
            return Self::invalid_param("queryString is required");
        }
        let provided = Self::str_param(params, "queryDefinitionId").to_string();
        let state = self.get_state(req.account, &req.region);
        let qid = if provided.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            provided
        };
        state.query_definitions.write().insert(
            qid.clone(),
            QueryDefinitionRec {
                query_definition_id: qid.clone(),
                name,
                query_string,
                log_group_names: params.get("logGroupNames").cloned().unwrap_or(json!([])),
                last_modified: now_millis(),
            },
        );
        AwsResponse::json(200, json!({ "queryDefinitionId": qid }))
    }

    fn describe_query_definitions(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = Self::str_param(&req.params, "queryDefinitionNamePrefix");
        let state = self.get_state(req.account, &req.region);
        let defs: Vec<Value> = state
            .query_definitions
            .read()
            .values()
            .filter(|d| prefix.is_empty() || d.name.starts_with(prefix))
            .map(|d| {
                json!({
                    "queryDefinitionId": d.query_definition_id,
                    "name": d.name,
                    "queryString": d.query_string,
                    "logGroupNames": d.log_group_names,
                    "lastModified": d.last_modified
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "queryDefinitions": defs }))
    }

    fn delete_query_definition(&self, req: &AwsRequest) -> AwsResponse {
        let qid = Self::str_param(&req.params, "queryDefinitionId").to_string();
        let state = self.get_state(req.account, &req.region);
        let mut defs = state.query_definitions.write();
        match defs.remove(&qid) {
            Some(_) => AwsResponse::json(200, json!({ "success": true })),
            None => Self::not_found(&format!("Query definition with id [{qid}] does not exist.")),
        }
    }

    // ---- metric filters ----
    // ---- metric filters ----

    fn put_metric_filter(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName").to_string();
        let filter_name = Self::str_param(params, "filterName").to_string();
        if group_name.is_empty() {
            return Self::invalid_param("logGroupName is required");
        }
        if filter_name.is_empty() {
            return Self::invalid_param("filterName is required");
        }
        let state = self.get_state(req.account, &req.region);
        if state.get_group(&group_name).is_none() {
            return Self::missing_group();
        }
        let filter = MetricFilterRec {
            log_group_name: group_name.clone(),
            filter_name: filter_name.clone(),
            filter_pattern: Self::str_param(params, "filterPattern").to_string(),
            metric_transformations: params
                .get("metricTransformations")
                .cloned()
                .unwrap_or(json!([])),
        };
        let mut filters = state.metric_filters.write();
        filters.retain(|f| !(f.log_group_name == group_name && f.filter_name == filter_name));
        filters.push(filter);
        AwsResponse::json(200, json!({}))
    }

    fn delete_metric_filter(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName");
        let filter_name = Self::str_param(params, "filterName");
        let state = self.get_state(req.account, &req.region);
        state
            .metric_filters
            .write()
            .retain(|f| !(f.log_group_name == group_name && f.filter_name == filter_name));
        AwsResponse::json(200, json!({}))
    }

    fn describe_metric_filters(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName");
        let prefix = Self::str_param(params, "filterNamePrefix");
        let state = self.get_state(req.account, &req.region);
        let filters: Vec<Value> = state
            .metric_filters
            .read()
            .iter()
            .filter(|f| group_name.is_empty() || f.log_group_name == group_name)
            .filter(|f| prefix.is_empty() || f.filter_name.starts_with(prefix))
            .map(|f| {
                json!({
                    "filterName": f.filter_name,
                    "filterPattern": f.filter_pattern,
                    "logGroupName": f.log_group_name,
                    "metricTransformations": f.metric_transformations
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "metricFilters": filters }))
    }

    // ---- subscription filters ----

    fn put_subscription_filter(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName").to_string();
        let filter_name = Self::str_param(params, "filterName").to_string();
        if group_name.is_empty() {
            return Self::invalid_param("logGroupName is required");
        }
        if filter_name.is_empty() {
            return Self::invalid_param("filterName is required");
        }
        let destination_arn = Self::str_param(params, "destinationArn").to_string();
        if destination_arn.is_empty() {
            return Self::invalid_param("destinationArn is required");
        }
        let state = self.get_state(req.account, &req.region);
        if state.get_group(&group_name).is_none() {
            return Self::missing_group();
        }
        let mut subs = state.subscription_filters.write();
        if let Some(existing) = subs
            .iter_mut()
            .find(|f| f.log_group_name == group_name && f.filter_name == filter_name)
        {
            existing.filter_pattern = Self::str_param(params, "filterPattern").to_string();
            existing.destination_arn = destination_arn;
            existing.role_arn = Self::str_param(params, "roleArn").to_string();
            return AwsResponse::json(200, json!({}));
        }
        let group_count = subs
            .iter()
            .filter(|f| f.log_group_name == group_name)
            .count();
        if group_count >= 2 {
            return AwsResponse::error(
                400,
                "LimitExceededException",
                "You have reached the maximum number of subscription filters for this log group.",
            );
        }
        subs.push(SubscriptionFilterRec {
            log_group_name: group_name,
            filter_name,
            filter_pattern: Self::str_param(params, "filterPattern").to_string(),
            destination_arn,
            role_arn: Self::str_param(params, "roleArn").to_string(),
            creation_time: now_millis(),
        });
        AwsResponse::json(200, json!({}))
    }

    fn delete_subscription_filter(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName");
        let filter_name = Self::str_param(params, "filterName");
        let state = self.get_state(req.account, &req.region);
        let mut subs = state.subscription_filters.write();
        let before = subs.len();
        subs.retain(|f| !(f.log_group_name == group_name && f.filter_name == filter_name));
        if subs.len() == before {
            return Self::not_found("The specified subscription filter does not exist.");
        }
        AwsResponse::json(200, json!({}))
    }

    fn subscription_json(f: &SubscriptionFilterRec) -> Value {
        json!({
            "filterName": f.filter_name,
            "logGroupName": f.log_group_name,
            "filterPattern": f.filter_pattern,
            "destinationArn": f.destination_arn,
            "roleArn": f.role_arn,
            "distribution": "ByLogStream",
            "creationTime": f.creation_time
        })
    }

    fn describe_subscription_filters(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let group_name = Self::str_param(params, "logGroupName");
        let prefix = Self::str_param(params, "filterNamePrefix");
        let state = self.get_state(req.account, &req.region);
        if !group_name.is_empty() && state.get_group(group_name).is_none() {
            return Self::missing_group();
        }
        let filters: Vec<Value> = state
            .subscription_filters
            .read()
            .iter()
            .filter(|f| group_name.is_empty() || f.log_group_name == group_name)
            .filter(|f| prefix.is_empty() || f.filter_name.starts_with(prefix))
            .map(Self::subscription_json)
            .collect();
        AwsResponse::json(200, json!({ "subscriptionFilters": filters }))
    }

    // ---- destinations ----

    fn destination_json(d: &DestinationRec) -> Value {
        let mut out = Map::new();
        out.insert("destinationName".to_string(), json!(d.destination_name));
        out.insert("arn".to_string(), json!(d.arn));
        out.insert("targetArn".to_string(), json!(d.target_arn));
        out.insert("roleArn".to_string(), json!(d.role_arn));
        if let Some(policy) = &d.access_policy {
            out.insert("accessPolicy".to_string(), json!(policy));
        }
        Value::Object(out)
    }

    fn put_destination(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let name = Self::str_param(params, "destinationName").to_string();
        if name.is_empty() {
            return Self::invalid_param("destinationName is required");
        }
        let state = self.get_state(req.account, &req.region);
        let arn = format!(
            "arn:aws:logs:{}:{}:destination:{}",
            req.region, req.account, name
        );
        let mut destinations = state.destinations.write();
        let destination = destinations
            .entry(name.clone())
            .or_insert_with(|| DestinationRec {
                destination_name: name,
                arn: arn.clone(),
                target_arn: String::new(),
                role_arn: String::new(),
                access_policy: None,
            });
        let role_arn = Self::str_param(params, "roleArn").to_string();
        if !role_arn.is_empty() {
            destination.role_arn = role_arn;
        }
        let target_arn = Self::str_param(params, "targetArn").to_string();
        if !target_arn.is_empty() {
            destination.target_arn = target_arn;
        }
        AwsResponse::json(
            200,
            json!({ "destination": Self::destination_json(destination) }),
        )
    }

    fn delete_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "destinationName");
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.destinations.write();
        if !destinations.values().any(|d| d.destination_name == name) {
            return Self::not_found("The specified destination does not exist.");
        }
        destinations.retain(|_, d| d.destination_name != name);
        AwsResponse::json(200, json!({}))
    }

    fn describe_destinations(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = Self::str_param(&req.params, "DestinationNamePrefix");
        let limit = Self::limit_param(&req.params, "limit", 50);
        let state = self.get_state(req.account, &req.region);
        let destinations: Vec<Value> = state
            .destinations
            .read()
            .values()
            .filter(|d| prefix.is_empty() || d.destination_name.starts_with(prefix))
            .take(limit)
            .map(Self::destination_json)
            .collect();
        AwsResponse::json(200, json!({ "destinations": destinations }))
    }

    fn put_destination_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "destinationName");
        let policy = Self::str_param(&req.params, "accessPolicy").to_string();
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.destinations.write();
        match destinations
            .values_mut()
            .find(|d| d.destination_name == name)
        {
            Some(d) => {
                d.access_policy = Some(policy);
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found("The specified destination does not exist."),
        }
    }

    // ---- resource policies ----

    fn put_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "policyName").to_string();
        if name.is_empty() {
            return Self::invalid_param("policyName is required");
        }
        let document = Self::str_param(&req.params, "policyDocument").to_string();
        if document.is_empty() {
            return Self::invalid_param("policyDocument is required");
        }
        let state = self.get_state(req.account, &req.region);
        let mut policies = state.resource_policies.write();
        let policy = policies
            .entry(name.clone())
            .or_insert_with(|| ResourcePolicyRec {
                policy_name: name.clone(),
                policy_document: document.clone(),
                description: None,
                last_updated_time: now_millis(),
            });
        policy.policy_document = document;
        policy.last_updated_time = now_millis();
        let description = Self::str_param(&req.params, "description");
        if !description.is_empty() {
            policy.description = Some(description.to_string());
        }
        AwsResponse::json(
            200,
            json!({
                "resourcePolicy": {
                    "policyName": policy.policy_name,
                    "policyDocument": policy.policy_document,
                    "lastUpdatedTime": policy.last_updated_time
                }
            }),
        )
    }

    fn describe_resource_policies(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let policies: Vec<Value> = state
            .resource_policies
            .read()
            .values()
            .map(|p| {
                let mut out = Map::new();
                out.insert("policyName".to_string(), json!(p.policy_name));
                out.insert("policyDocument".to_string(), json!(p.policy_document));
                out.insert("lastUpdatedTime".to_string(), json!(p.last_updated_time));
                if let Some(d) = &p.description {
                    out.insert("description".to_string(), json!(d));
                }
                out.into()
            })
            .collect();
        AwsResponse::json(200, json!({ "resourcePolicies": policies }))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "policyName");
        let state = self.get_state(req.account, &req.region);
        let mut policies = state.resource_policies.write();
        if policies.remove(name).is_none() {
            return Self::not_found(&format!("Policy with name [{name}] does not exist"));
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- export tasks ----

    fn export_task_json(t: &ExportTaskRec) -> Value {
        let mut out = Map::new();
        out.insert("taskId".to_string(), json!(t.task_id));
        if let Some(name) = &t.task_name {
            out.insert("taskName".to_string(), json!(name));
        }
        out.insert("logGroupName".to_string(), json!(t.log_group_name));
        out.insert("destination".to_string(), json!(t.destination));
        out.insert("from".to_string(), json!(t.from_time));
        out.insert("to".to_string(), json!(t.to));
        out.insert(
            "status".to_string(),
            json!({ "code": "COMPLETED", "message": "Completed successfully" }),
        );
        Value::Object(out)
    }

    fn create_export_task(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let group_name = Self::str_param(&req.params, "logGroupName").to_string();
        if state.get_group(&group_name).is_none() {
            return Self::missing_group();
        }
        let task_id = uuid::Uuid::new_v4().to_string();
        let task_name = Self::str_param(&req.params, "taskName");
        let task = ExportTaskRec {
            task_id: task_id.clone(),
            task_name: if task_name.is_empty() {
                None
            } else {
                Some(task_name.to_string())
            },
            log_group_name: group_name,
            destination: Self::str_param(&req.params, "destination").to_string(),
            from_time: req.params.get("from").and_then(|v| v.as_i64()).unwrap_or(0),
            to: req.params.get("to").and_then(|v| v.as_i64()).unwrap_or(0),
        };
        state.export_tasks.write().insert(task_id.clone(), task);
        AwsResponse::json(200, json!({ "taskId": task_id }))
    }

    fn describe_export_tasks(&self, req: &AwsRequest) -> AwsResponse {
        let task_id = Self::str_param(&req.params, "taskId");
        let state = self.get_state(req.account, &req.region);
        let tasks = state.export_tasks.read();
        let tasks_json: Vec<Value> = if task_id.is_empty() {
            tasks.values().map(Self::export_task_json).collect()
        } else {
            match tasks.get(task_id) {
                Some(t) => vec![Self::export_task_json(t)],
                None => return Self::not_found("The specified export task does not exist."),
            }
        };
        AwsResponse::json(200, json!({ "exportTasks": tasks_json }))
    }

    fn cancel_export_task(&self, req: &AwsRequest) -> AwsResponse {
        let task_id = Self::str_param(&req.params, "taskId");
        let state = self.get_state(req.account, &req.region);
        if state.export_tasks.write().remove(task_id).is_none() {
            return Self::not_found("The specified export task does not exist.");
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- anomaly detectors ----

    fn create_log_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let _arn_list = req
            .params
            .get("logGroupArnList")
            .cloned()
            .unwrap_or(json!([]));
        let detector_name = Self::str_param(&req.params, "detectorName");
        let frequency = Self::str_param(&req.params, "evaluationFrequency");
        if !frequency.is_empty()
            && ![
                "ONE_MIN",
                "FIVE_MIN",
                "TEN_MIN",
                "FIFTEEN_MIN",
                "THIRTY_MIN",
                "ONE_HOUR",
            ]
            .contains(&frequency)
        {
            return Self::invalid_param(
                "1 validation error detected: Value at 'evaluationFrequency' failed to satisfy constraint: Member must satisfy enum value set: [ONE_MIN, FIVE_MIN, TEN_MIN, FIFTEEN_MIN, THIRTY_MIN, ONE_HOUR]",
            );
        }
        let state = self.get_state(req.account, &req.region);
        let identity = if detector_name.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            detector_name.to_string()
        };
        let arn = format!(
            "arn:aws:logs:{}:{}:anomaly-detector:{}",
            req.region, req.account, identity
        );
        state.anomaly_detectors.write().insert(
            arn.clone(),
            AnomalyDetectorRec {
                arn: arn.clone(),
                log_group_arn_list: req
                    .params
                    .get("logGroupArnList")
                    .cloned()
                    .unwrap_or(json!([])),
                evaluation_frequency: if frequency.is_empty() {
                    None
                } else {
                    Some(frequency.to_string())
                },
                filter_pattern: Some(Self::str_param(&req.params, "filterPattern").to_string()),
                enabled: true,
            },
        );
        AwsResponse::json(200, json!({ "anomalyDetectorArn": arn }))
    }

    fn delete_log_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::str_param(&req.params, "anomalyDetectorArn");
        let state = self.get_state(req.account, &req.region);
        if state.anomaly_detectors.write().remove(arn).is_none() {
            return Self::not_found("The specified anomaly detector does not exist.");
        }
        AwsResponse::json(200, json!({}))
    }

    fn get_log_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::str_param(&req.params, "anomalyDetectorArn");
        let state = self.get_state(req.account, &req.region);
        let found = state.anomaly_detectors.read().get(arn).cloned();
        match found {
            Some(d) => AwsResponse::json(
                200,
                json!({
                    "anomalyDetectorArn": d.arn,
                    "enabled": d.enabled
                }),
            ),
            None => Self::not_found("The specified anomaly detector does not exist."),
        }
    }

    fn update_log_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let arn = Self::str_param(&req.params, "anomalyDetectorArn");
        let state = self.get_state(req.account, &req.region);
        let mut detectors = state.anomaly_detectors.write();
        match detectors.get_mut(arn) {
            Some(d) => {
                let enabled = req
                    .params
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                d.enabled = enabled;
                d.evaluation_frequency =
                    Some(Self::str_param(&req.params, "evaluationFrequency").to_string());
                d.filter_pattern = Some(Self::str_param(&req.params, "filterPattern").to_string());
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found("The specified anomaly detector does not exist."),
        }
    }

    fn list_log_anomaly_detectors(&self, req: &AwsRequest) -> AwsResponse {
        let filter_arn = Self::str_param(&req.params, "filterLogGroupArn");
        let state = self.get_state(req.account, &req.region);
        let detectors: Vec<Value> = state
            .anomaly_detectors
            .read()
            .values()
            .filter(|d| {
                filter_arn.is_empty()
                    || d.log_group_arn_list
                        .as_array()
                        .map(|arr| arr.iter().any(|v| v.as_str() == Some(filter_arn)))
                        .unwrap_or(false)
            })
            .map(|d| {
                let mut out = Map::new();
                out.insert("anomalyDetectorArn".to_string(), json!(d.arn));
                if let Some(f) = &d.evaluation_frequency {
                    out.insert("evaluationFrequency".to_string(), json!(f));
                }
                out.insert("anomalyDetectorStatus".to_string(), json!("TRAINING"));
                Value::Object(out)
            })
            .collect();
        AwsResponse::json(200, json!({ "anomalyDetectors": detectors }))
    }

    // ---- account policies ----

    fn put_account_policy(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let name = Self::str_param(params, "policyName").to_string();
        if name.is_empty() {
            return Self::invalid_param("policyName is required");
        }
        let document = Self::str_param(params, "policyDocument");
        if document.is_empty() {
            return Self::invalid_param("policyDocument is required");
        }
        let policy_type = Self::str_param(params, "policyType").to_string();
        if policy_type != "DATA_PROTECTION_POLICY" && policy_type != "SUBSCRIPTION_FILTER_POLICY" {
            return Self::invalid_param(
                "1 validation error detected: Value at 'policyType' failed to satisfy constraint: Member must satisfy enum value set: [DATA_PROTECTION_POLICY, SUBSCRIPTION_FILTER_POLICY]",
            );
        }
        let scope = {
            let s = Self::str_param(params, "scope");
            if s.is_empty() {
                "ALL".to_string()
            } else {
                s.to_string()
            }
        };
        let state = self.get_state(req.account, &req.region);
        let policy = AccountPolicyRec {
            policy_name: name.clone(),
            policy_document: document.to_string(),
            policy_type: policy_type.clone(),
            scope,
            last_updated_time: now_millis(),
        };
        let updated = policy.last_updated_time;
        let key = format!("{policy_type}/{name}");
        state.account_policies.write().insert(key, policy);
        AwsResponse::json(
            200,
            json!({
                "accountPolicy": {
                    "accountId": req.account.to_string(),
                    "policyName": name,
                    "policyDocument": document,
                    "policyType": policy_type,
                    "scope": Self::str_param(params, "scope"),
                    "lastUpdatedTime": updated
                }
            }),
        )
    }

    fn describe_account_policies(&self, req: &AwsRequest) -> AwsResponse {
        let policy_type = Self::str_param(&req.params, "policyType");
        if policy_type != "DATA_PROTECTION_POLICY" && policy_type != "SUBSCRIPTION_FILTER_POLICY" {
            return Self::invalid_param(
                "1 validation error detected: Value at 'policyType' failed to satisfy constraint: Member must satisfy enum value set: [DATA_PROTECTION_POLICY, SUBSCRIPTION_FILTER_POLICY]",
            );
        }
        let state = self.get_state(req.account, &req.region);
        let account_id = req.account.to_string();
        let policies: Vec<Value> = state
            .account_policies
            .read()
            .values()
            .filter(|p| p.policy_type == policy_type)
            .map(|p| {
                json!({
                    "accountId": account_id,
                    "policyName": p.policy_name,
                    "policyDocument": p.policy_document,
                    "policyType": p.policy_type,
                    "scope": p.scope,
                    "lastUpdatedTime": p.last_updated_time
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "accountPolicies": policies }))
    }

    fn delete_account_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "policyName");
        let policy_type = Self::str_param(&req.params, "policyType");
        if policy_type != "DATA_PROTECTION_POLICY" && policy_type != "SUBSCRIPTION_FILTER_POLICY" {
            return Self::invalid_param(
                "1 validation error detected: Value at 'policyType' failed to satisfy constraint: Member must satisfy enum value set: [DATA_PROTECTION_POLICY, SUBSCRIPTION_FILTER_POLICY]",
            );
        }
        let state = self.get_state(req.account, &req.region);
        if state
            .account_policies
            .write()
            .remove(&format!("{policy_type}/{name}"))
            .is_none()
        {
            return Self::not_found(&format!("Policy with name [{name}] does not exist"));
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- delivery source/destination/delivery ----

    fn put_delivery_source(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let name = Self::str_param(params, "name").to_string();
        if name.is_empty() {
            return Self::invalid_param("name is required");
        }
        let resource_arn = Self::str_param(params, "resourceArn").to_string();
        if resource_arn.is_empty() {
            return Self::invalid_param("resourceArn is required");
        }
        let log_type = Self::str_param(params, "logType").to_string();
        if log_type.is_empty() {
            return Self::invalid_param("logType is required");
        }
        let service = resource_arn.split(':').nth(2).unwrap_or("").to_string();
        let state = self.get_state(req.account, &req.region);
        let source = DeliverySourceRec {
            name: name.clone(),
            arn: format!(
                "arn:aws:logs:{}:{}:delivery-source:{}",
                req.region, req.account, name
            ),
            resource_arns: json!([resource_arn]),
            service: service.clone(),
            log_type: log_type.clone(),
        };
        state.delivery_sources.write().insert(name.clone(), source);
        AwsResponse::json(
            200,
            json!({
                "deliverySource": {
                    "name": name,
                    "arn": format!("arn:aws:logs:{}:{}:delivery-source:{}", req.region, req.account, name),
                    "resourceArns": [resource_arn],
                    "service": service,
                    "logType": log_type
                }
            }),
        )
    }

    fn get_delivery_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "name");
        let state = self.get_state(req.account, &req.region);
        let found = state
            .delivery_sources
            .read()
            .values()
            .find(|s| s.name == name)
            .cloned();
        match found {
            Some(source) => AwsResponse::json(
                200,
                json!({
                    "deliverySource": {
                        "name": source.name,
                        "arn": source.arn,
                        "resourceArns": source.resource_arns,
                        "service": source.service,
                        "logType": source.log_type
                    }
                }),
            ),
            None => Self::not_found("Requested Delivery Source does not exist in this account."),
        }
    }

    fn delete_delivery_source(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "name");
        let state = self.get_state(req.account, &req.region);
        let mut sources = state.delivery_sources.write();
        if !sources.values().any(|s| s.name == name) {
            return Self::not_found("Requested Delivery Source does not exist in this account.");
        }
        sources.retain(|_, s| s.name != name);
        AwsResponse::json(200, json!({}))
    }

    fn describe_delivery_sources(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let sources: Vec<Value> = state
            .delivery_sources
            .read()
            .values()
            .map(|s| {
                json!({
                    "name": s.name,
                    "arn": s.arn,
                    "resourceArns": s.resource_arns,
                    "service": s.service,
                    "logType": s.log_type
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "deliverySources": sources }))
    }

    fn delivery_destination_type(resource_arn: &str) -> String {
        match resource_arn.split(':').nth(2) {
            Some("s3") => "S3".to_string(),
            Some("logs") => "CWL".to_string(),
            Some("firehose") => "FH".to_string(),
            _ => "S3".to_string(),
        }
    }

    fn delivery_destination_json(d: &DeliveryDestinationRec) -> Value {
        let mut out = Map::new();
        out.insert("name".to_string(), json!(d.name));
        out.insert("arn".to_string(), json!(d.arn));
        out.insert(
            "deliveryDestinationType".to_string(),
            json!(d.delivery_destination_type),
        );
        if let Some(f) = &d.output_format {
            out.insert("outputFormat".to_string(), json!(f));
        }
        out.insert(
            "deliveryDestinationConfiguration".to_string(),
            d.configuration.clone(),
        );
        if let Some(policy) = &d.policy {
            out.insert("deliveryDestinationPolicy".to_string(), json!(policy));
        }
        Value::Object(out)
    }

    fn put_delivery_destination(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let name = Self::str_param(params, "name").to_string();
        if name.is_empty() {
            return Self::invalid_param("name is required");
        }
        let configuration = params
            .get("deliveryDestinationConfiguration")
            .cloned()
            .unwrap_or(json!({}));
        let resource_arn = configuration
            .get("destinationResourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if resource_arn.is_empty() {
            return Self::invalid_param("deliveryDestinationConfiguration is required");
        }
        let state = self.get_state(req.account, &req.region);
        let destination_type = Self::delivery_destination_type(&resource_arn);
        let dest = DeliveryDestinationRec {
            name: name.clone(),
            arn: format!(
                "arn:aws:logs:{}:{}:delivery-destination:{}",
                req.region, req.account, name
            ),
            delivery_destination_type: destination_type.clone(),
            output_format: Some(Self::str_param(params, "outputFormat").to_string()),
            configuration,
            policy: None,
        };
        state
            .delivery_destinations
            .write()
            .insert(name.clone(), dest);
        AwsResponse::json(
            200,
            json!({
                "deliveryDestination": {
                    "name": name,
                    "arn": format!("arn:aws:logs:{}:{}:delivery-destination:{}", req.region, req.account, name),
                    "deliveryDestinationType": destination_type,
                    "outputFormat": Self::str_param(params, "outputFormat"),
                    "deliveryDestinationConfiguration": params
                        .get("deliveryDestinationConfiguration")
                        .cloned()
                        .unwrap_or(json!({}))
                }
            }),
        )
    }

    fn get_delivery_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "name");
        let state = self.get_state(req.account, &req.region);
        let found = state
            .delivery_destinations
            .read()
            .values()
            .find(|d| d.name == name)
            .cloned();
        match found {
            Some(d) => AwsResponse::json(
                200,
                json!({ "deliveryDestination": Self::delivery_destination_json(&d) }),
            ),
            None => {
                Self::not_found("Requested Delivery Destination does not exist in this account.")
            }
        }
    }

    fn delete_delivery_destination(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "name");
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.delivery_destinations.write();
        if !destinations.values().any(|d| d.name == name) {
            return Self::not_found(
                "Requested Delivery Destination does not exist in this account.",
            );
        }
        destinations.retain(|_, d| d.name != name);
        AwsResponse::json(200, json!({}))
    }

    fn describe_delivery_destinations(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let destinations: Vec<Value> = state
            .delivery_destinations
            .read()
            .values()
            .map(Self::delivery_destination_json)
            .collect();
        AwsResponse::json(200, json!({ "deliveryDestinations": destinations }))
    }

    fn delivery_destination_policy(&self, req: &AwsRequest, is_put: bool) -> AwsResponse {
        let name = Self::str_param(&req.params, "deliveryDestinationName");
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.delivery_destinations.write();
        match destinations.values_mut().find(|d| d.name == name) {
            Some(d) => {
                if is_put {
                    let policy =
                        Self::str_param(&req.params, "deliveryDestinationPolicy").to_string();
                    d.policy = Some(policy.clone());
                    AwsResponse::json(
                        200,
                        json!({
                            "policy": { "deliveryDestinationPolicy": policy }
                        }),
                    )
                } else {
                    AwsResponse::json(
                        200,
                        json!({
                            "policy": { "deliveryDestinationPolicy": d.policy.clone().unwrap_or_default() }
                        }),
                    )
                }
            }
            None => {
                Self::not_found("Requested Delivery Destination does not exist in this account.")
            }
        }
    }

    fn delete_delivery_destination_policy(&self, req: &AwsRequest) -> AwsResponse {
        let name = Self::str_param(&req.params, "deliveryDestinationName");
        let state = self.get_state(req.account, &req.region);
        let mut destinations = state.delivery_destinations.write();
        match destinations.values_mut().find(|d| d.name == name) {
            Some(d) => {
                d.policy = None;
                AwsResponse::json(200, json!({}))
            }
            None => {
                Self::not_found("Requested Delivery Destination does not exist in this account.")
            }
        }
    }

    fn create_delivery(&self, req: &AwsRequest) -> AwsResponse {
        let params = &req.params;
        let source_name = Self::str_param(params, "deliverySourceName");
        let destination_arn = Self::str_param(params, "deliveryDestinationArn");
        let state = self.get_state(req.account, &req.region);
        let has_dest = state
            .delivery_destinations
            .read()
            .values()
            .any(|d| d.arn == destination_arn);
        if !has_dest {
            return Self::not_found(
                "Requested Delivery Destination does not exist in this account.",
            );
        }
        let sources = state.delivery_sources.read();
        let source = sources.values().find(|s| s.name == source_name);
        if source.is_none() {
            return Self::not_found("Requested Delivery Source does not exist in this account.");
        }
        let destination_type = Self::delivery_destination_type(destination_arn);
        let id: String = uuid::Uuid::new_v4().simple().to_string()[..16].to_string();
        let delivery = DeliveryRec {
            id: id.clone(),
            arn: format!(
                "arn:aws:logs:{}:{}:delivery:{}",
                req.region, req.account, id
            ),
            delivery_source_name: source_name.to_string(),
            delivery_destination_arn: destination_arn.to_string(),
            delivery_destination_type: destination_type.clone(),
        };
        state.deliveries.write().insert(id.clone(), delivery);
        AwsResponse::json(
            200,
            json!({
                "delivery": {
                    "id": id,
                    "arn": format!("arn:aws:logs:{}:{}:delivery:{}", req.region, req.account, id),
                    "deliverySourceName": source_name,
                    "deliveryDestinationArn": destination_arn,
                    "deliveryDestinationType": destination_type,
                    "recordFields": [
                        "date", "time", "x-edge-location", "sc-bytes", "c-ip", "cs-method",
                        "cs(Host)", "cs-uri-stem", "sc-status", "cs(Referer)", "cs(User-Agent)",
                        "cs-uri-query", "cs(Cookie)", "x-edge-result-type", "x-edge-request-id",
                        "x-host-header", "cs-protocol", "cs-bytes", "time-taken", "x-forwarded-for"
                    ]
                }
            }),
        )
    }

    fn delivery_json(d: &DeliveryRec) -> Value {
        json!({
            "id": d.id,
            "arn": d.arn,
            "deliverySourceName": d.delivery_source_name,
            "deliveryDestinationArn": d.delivery_destination_arn,
            "deliveryDestinationType": d.delivery_destination_type
        })
    }

    fn get_delivery(&self, req: &AwsRequest) -> AwsResponse {
        let id = Self::str_param(&req.params, "id");
        let state = self.get_state(req.account, &req.region);
        let found = state
            .deliveries
            .read()
            .values()
            .find(|d| d.id == id)
            .cloned();
        match found {
            Some(d) => AwsResponse::json(200, json!({ "delivery": Self::delivery_json(&d) })),
            None => Self::not_found("Requested Delivery does not exist in this account."),
        }
    }

    fn delete_delivery(&self, req: &AwsRequest) -> AwsResponse {
        let id = Self::str_param(&req.params, "id");
        let state = self.get_state(req.account, &req.region);
        let mut deliveries = state.deliveries.write();
        if !deliveries.values().any(|d| d.id == id) {
            return Self::not_found("Requested Delivery does not exist in this account.");
        }
        deliveries.retain(|_, d| d.id != id);
        AwsResponse::json(200, json!({}))
    }
}

impl Default for LogsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "logs".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_put_and_get_log_events_with_tokens() {
        let handler = LogsHandler::new();
        handler.handle(make_req("CreateLogGroup", json!({ "logGroupName": "/t" })));
        handler.handle(make_req(
            "CreateLogStream",
            json!({ "logGroupName": "/t", "logStreamName": "s" }),
        ));
        let now = now_millis();
        handler.handle(make_req(
            "PutLogEvents",
            json!({
                "logGroupName": "/t",
                "logStreamName": "s",
                "logEvents": [
                    {"timestamp": now, "message": "one"},
                    {"timestamp": now + 1, "message": "two"}
                ]
            }),
        ));
        let resp = handler.handle(make_req(
            "GetLogEvents",
            json!({
                "logGroupName": "/t", "logStreamName": "s", "startFromHead": true, "limit": 1
            }),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
        assert!(body["nextForwardToken"].as_str().unwrap().starts_with("f/"));

        // Follow the forward token.
        let next = body["nextForwardToken"].as_str().unwrap().to_string();
        let resp2 = handler.handle(make_req(
            "GetLogEvents",
            json!({
                "logGroupName": "/t", "logStreamName": "s", "nextToken": next, "limit": 1
            }),
        ));
        let body2: Value = serde_json::from_str(&resp2.body).unwrap();
        assert_eq!(body2["events"][0]["message"], "two");
    }

    #[test]
    fn test_filter_log_events_pattern() {
        let handler = LogsHandler::new();
        handler.handle(make_req("CreateLogGroup", json!({ "logGroupName": "/f" })));
        handler.handle(make_req(
            "CreateLogStream",
            json!({ "logGroupName": "/f", "logStreamName": "s" }),
        ));
        let now = now_millis();
        handler.handle(make_req(
            "PutLogEvents",
            json!({
                "logGroupName": "/f",
                "logStreamName": "s",
                "logEvents": [
                    {"timestamp": now, "message": "ERROR boom"},
                    {"timestamp": now + 1, "message": "INFO fine"}
                ]
            }),
        ));
        let resp = handler.handle(make_req(
            "FilterLogEvents",
            json!({
                "logGroupName": "/f", "filterPattern": "ERROR"
            }),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["message"], "ERROR boom");
        assert_eq!(events[0]["logStreamName"], "s");
    }

    #[test]
    fn test_metric_filter_lifecycle() {
        let handler = LogsHandler::new();
        handler.handle(make_req("CreateLogGroup", json!({ "logGroupName": "/m" })));
        let put = handler.handle(make_req("PutMetricFilter", json!({
            "logGroupName": "/m",
            "filterName": "mf",
            "filterPattern": "ERROR",
            "metricTransformations": [{"metricName": "E", "metricNamespace": "N", "metricValue": "1"}]
        })));
        assert_eq!(put.status, 200);
        let desc = handler.handle(make_req(
            "DescribeMetricFilters",
            json!({
                "logGroupName": "/m", "filterNamePrefix": "mf"
            }),
        ));
        let body: Value = serde_json::from_str(&desc.body).unwrap();
        assert_eq!(body["metricFilters"].as_array().unwrap().len(), 1);
        handler.handle(make_req(
            "DeleteMetricFilter",
            json!({
                "logGroupName": "/m", "filterName": "mf"
            }),
        ));
        let after = handler.handle(make_req(
            "DescribeMetricFilters",
            json!({ "logGroupName": "/m" }),
        ));
        let body2: Value = serde_json::from_str(&after.body).unwrap();
        assert_eq!(body2["metricFilters"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_retention_validation() {
        let handler = LogsHandler::new();
        handler.handle(make_req("CreateLogGroup", json!({ "logGroupName": "/r" })));
        let bad = handler.handle(make_req(
            "PutRetentionPolicy",
            json!({
                "logGroupName": "/r", "retentionInDays": 999
            }),
        ));
        assert_eq!(bad.status, 400);
        let ok = handler.handle(make_req(
            "PutRetentionPolicy",
            json!({
                "logGroupName": "/r", "retentionInDays": 30
            }),
        ));
        assert_eq!(ok.status, 200);
        let groups = handler.handle(make_req("DescribeLogGroups", json!({})));
        let body: Value = serde_json::from_str(&groups.body).unwrap();
        assert_eq!(body["logGroups"][0]["retentionInDays"], 30);
    }
}
