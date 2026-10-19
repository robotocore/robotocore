//! AWS X-Ray backend — ports the py gateway's native xray provider (the py
//! lane's ground truth) and moto's xray responses for the ops the py provider
//! forwards to moto.
//!
//! Handling notes carried over:
//! - sampling rules/groups/tags/encryption/resource policies mirror the py
//!   provider (records created with CreatedAt/ModifiedAt 0.0, ARNs per-region)
//! - BatchGetTraces serializes stored segments into the moto wire shape
//!   `{"Traces": [{"Duration", "Id", "Segments": [{"Id", "Document"}]}],
//!   "UnprocessedTraceIds": [...]}` (moto's backend-level shape, which the
//!   sidecar already served; the raw SegmentCollection tuple is never sent)
//! - GetTraceSummaries answers the moto envelope (ApproximateTime,
//!   TracesProcessedCount, TraceSummaries) with py-engine-style summaries
//! - insight get/check ops raise InvalidRequestException like moto; the
//!   retrieval ops keep state per (account, region)

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<XrayState>> = Lazy::new(|| store::slot::<XrayState>("xray"));

#[derive(Default)]
struct XrayState {
    rules: HashMap<String, Value>,
    groups: HashMap<String, Value>,
    tags: HashMap<String, Vec<Value>>,
    encryption_config: Option<Value>,
    policies: HashMap<String, Value>,
    traces: HashMap<String, TraceEntry>,
    stats: HashMap<String, RollingStats>,
    retrievals: HashMap<String, String>,
    trace_segment_destination: Option<String>,
    indexing_rules: Option<Vec<Value>>,
    region: String,
    account: String,
}

#[derive(Default, Clone)]
struct TraceEntry {
    trace_id: String,
    duration: i64,
    segments: Vec<(String, String)>, // (segment id, raw doc)
    has_error: bool,
    has_fault: bool,
    has_throttle: bool,
    http_status: Option<Value>,
    services: Vec<String>,
    start_time: f64,
    end_time: f64,
}

#[derive(Default, Clone)]
struct RollingStats {
    latencies: Vec<f64>,
    error_rates: Vec<f64>,
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len().max(1) as f64
}

fn stdev(v: &[f64], m: f64) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let var = v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64;
    var.sqrt()
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let path = req.path.trim_start_matches('/');
    match path {
        "CreateSamplingRule" => st.create_sampling_rule(&params),
        "GetSamplingRules" => st.get_sampling_rules(),
        "DeleteSamplingRule" => st.delete_sampling_rule(&params),
        "UpdateSamplingRule" => st.update_sampling_rule(&params),
        "SamplingStatisticSummaries" => Response::json_status(
            200,
            &json!({"SamplingStatisticSummaries": [], "NextToken": Value::Null}),
        ),
        "SamplingTargets" => Response::json_status(
            200,
            &json!({
                "SamplingTargetDocuments": [],
                "LastRuleModification": crate::proto::now_unix(),
                "UnprocessedStatistics": [],
            }),
        ),
        "CreateGroup" => st.create_group(&params),
        "GetGroup" => st.get_group(&params),
        "Groups" => st.get_groups(),
        "DeleteGroup" => st.delete_group(&params),
        "UpdateGroup" => st.update_group(&params),
        "EncryptionConfig" => st.get_encryption_config(),
        "PutEncryptionConfig" => st.put_encryption_config(&params),
        "TagResource" => st.tag_resource(&params),
        "UntagResource" => st.untag_resource(&params),
        "ListTagsForResource" => st.list_tags_for_resource(&params),
        "PutResourcePolicy" => st.put_resource_policy(&params),
        "ListResourcePolicies" => st.list_resource_policies(),
        "DeleteResourcePolicy" => st.delete_resource_policy(&params),
        "TraceSegments" => st.put_trace_segments(&params),
        "Traces" => st.batch_get_traces(&params),
        "TraceSummaries" => st.get_trace_summaries(&params),
        "TraceGraph" => st.get_trace_graph(&params),
        "ServiceGraph" => st.get_service_graph(&params),
        "TelemetryRecords" => Response::json_status(200, &json!({})),
        "InsightSummaries" => st.get_insight_summaries(&params),
        "Insight" => st.insight_missing("InvalidRequestException", &params),
        "InsightEvents" => st.insight_missing("InvalidRequestException", &params),
        "InsightImpactGraph" => st.insight_missing("InvalidRequestException", &params),
        "TimeSeriesServiceStatistics" => Response::json_status(
            200,
            &json!({"TimeSeriesServiceStatistics": [], "ContainsOldGroupVersions": false}),
        ),
        "GetIndexingRules" => st.get_indexing_rules(),
        "UpdateIndexingRule" => st.update_indexing_rule(&params),
        "StartTraceRetrieval" => st.start_trace_retrieval(&params),
        "CancelTraceRetrieval" => st.cancel_trace_retrieval(&params),
        "GetRetrievedTracesGraph" => st.get_retrieved_traces_graph(&params),
        "ListRetrievedTraces" => st.list_retrieved_traces(&params),
        "GetTraceSegmentDestination" => st.get_trace_segment_destination(),
        "UpdateTraceSegmentDestination" => st.update_trace_segment_destination(&params),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn rule_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:xray:{region}:{account}:sampling-rule/{name}")
}

fn group_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:xray:{region}:{account}:group/{name}")
}

fn truthy(v: Option<&Value>) -> bool {
    matches!(v, Some(Value::Bool(true)))
}

impl XrayState {
    // ---- sampling rules ---------------------------------------------------

    fn create_sampling_rule(&mut self, params: &Value) -> Response {
        let rule_in = params.get("SamplingRule").cloned().unwrap_or(json!({}));
        let name = rule_in
            .get("RuleName")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("rule-{}", uuid::Uuid::new_v4().simple()));
        let rule = json!({
            "SamplingRule": {
                "RuleName": name,
                "RuleARN": rule_arn(&self.region, &self.account, &name),
                "ResourceARN": rule_in.get("ResourceARN").cloned().unwrap_or(json!("*")),
                "Priority": rule_in.get("Priority").cloned().unwrap_or(json!(1000)),
                "FixedRate": rule_in.get("FixedRate").cloned().unwrap_or(json!(0.05)),
                "ReservoirSize": rule_in.get("ReservoirSize").cloned().unwrap_or(json!(1)),
                "ServiceName": rule_in.get("ServiceName").cloned().unwrap_or(json!("*")),
                "ServiceType": rule_in.get("ServiceType").cloned().unwrap_or(json!("*")),
                "Host": rule_in.get("Host").cloned().unwrap_or(json!("*")),
                "HTTPMethod": rule_in.get("HTTPMethod").cloned().unwrap_or(json!("*")),
                "URLPath": rule_in.get("URLPath").cloned().unwrap_or(json!("*")),
                "Version": rule_in.get("Version").cloned().unwrap_or(json!(1)),
                "Attributes": rule_in.get("Attributes").cloned().unwrap_or(json!({})),
            },
            "CreatedAt": 0.0,
            "ModifiedAt": 0.0,
        });
        let arn = rule["SamplingRule"]["RuleARN"].as_str().unwrap_or("");
        if let Some(tags) = params.get("Tags") {
            if let Some(list) = tags.as_array() {
                self.tags.insert(arn.to_string(), list.clone());
            }
        }
        self.rules.insert(name.clone(), rule.clone());
        Response::json_status(200, &json!({"SamplingRuleRecord": rule}))
    }

    fn get_sampling_rules(&self) -> Response {
        let records: Vec<Value> = self.rules.values().cloned().collect();
        Response::json_status(
            200,
            &json!({"SamplingRuleRecords": records, "NextToken": Value::Null}),
        )
    }

    fn delete_sampling_rule(&mut self, params: &Value) -> Response {
        let name = params.get("RuleName").and_then(Value::as_str).unwrap_or("");
        let arn = params.get("RuleARN").and_then(Value::as_str).unwrap_or("");
        let mut key: Option<String> = None;
        if !name.is_empty() && self.rules.contains_key(name) {
            key = Some(name.to_string());
        } else if !arn.is_empty() {
            key = self.rules.values().find_map(|r| {
                let rule = &r["SamplingRule"];
                if rule.get("RuleARN").and_then(Value::as_str) == Some(arn) {
                    rule.get("RuleName")
                        .and_then(Value::as_str)
                        .map(String::from)
                } else {
                    None
                }
            });
        }
        let record = key
            .as_ref()
            .map(|k| self.rules.remove(k).unwrap_or(json!({})))
            .unwrap_or(json!({}));
        let removed_arn = record
            .get("SamplingRule")
            .and_then(|r| r.get("RuleARN"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if key.is_some() && !removed_arn.is_empty() {
            self.tags.remove(&removed_arn);
        }
        Response::json_status(200, &json!({"SamplingRuleRecord": record}))
    }

    fn update_sampling_rule(&mut self, params: &Value) -> Response {
        let update = params
            .get("SamplingRuleUpdate")
            .cloned()
            .unwrap_or(json!({}));
        let name = update
            .get("RuleName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let arn = update
            .get("RuleARN")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let key = if !name.is_empty() && self.rules.contains_key(&name) {
            Some(name.clone())
        } else if !arn.is_empty() {
            self.rules.values().find_map(|r| {
                let rule = &r["SamplingRule"];
                if rule.get("RuleARN").and_then(Value::as_str) == Some(arn.as_str()) {
                    rule.get("RuleName")
                        .and_then(Value::as_str)
                        .map(String::from)
                } else {
                    None
                }
            })
        } else {
            None
        };
        let Some(key) = key else {
            return crate::proto::json_error(
                400,
                "InvalidRequestException",
                &format!(
                    "Sampling rule not found: {}",
                    if name.is_empty() { &arn } else { &name }
                ),
            );
        };
        let record = self.rules.get_mut(&key).unwrap();
        if let (Some(rule), Some(update)) = (
            record
                .get_mut("SamplingRule")
                .and_then(Value::as_object_mut),
            update.as_object(),
        ) {
            for field in [
                "ResourceARN",
                "Priority",
                "FixedRate",
                "ReservoirSize",
                "ServiceName",
                "ServiceType",
                "Host",
                "HTTPMethod",
                "URLPath",
                "Attributes",
            ] {
                if let Some(v) = update.get(field) {
                    rule.insert(field.to_string(), v.clone());
                }
            }
        }
        if let Some(rule) = self.rules.get(&key) {
            return Response::json_status(200, &json!({"SamplingRuleRecord": rule}));
        }
        crate::proto::json_error(500, "InternalFailure", "rule vanished during update")
    }

    // ---- groups -----------------------------------------------------------

    fn create_group(&mut self, params: &Value) -> Response {
        let name = params
            .get("GroupName")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("group-{}", uuid::Uuid::new_v4().simple()));
        let group = json!({
            "GroupName": name,
            "GroupARN": group_arn(&self.region, &self.account, &name),
            "FilterExpression": params.get("FilterExpression").cloned().unwrap_or(json!("")),
            "InsightsConfiguration": params.get("InsightsConfiguration").cloned().unwrap_or_else(|| {
                json!({"InsightsEnabled": false, "NotificationsEnabled": false})
            }),
        });
        if let Some(tags) = params.get("Tags") {
            if let Some(list) = tags.as_array() {
                self.tags.insert(
                    group["GroupARN"].as_str().unwrap_or("").to_string(),
                    list.clone(),
                );
            }
        }
        self.groups.insert(name, group.clone());
        Response::json_status(200, &json!({"Group": group}))
    }

    fn find_group(&self, name: &str, arn: &str) -> Option<&Value> {
        if !name.is_empty() {
            if let Some(g) = self.groups.get(name) {
                return Some(g);
            }
        }
        self.groups
            .values()
            .find(|g| g.get("GroupARN").and_then(Value::as_str) == Some(arn))
    }

    fn get_group(&self, params: &Value) -> Response {
        let name = params
            .get("GroupName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let arn = params.get("GroupARN").and_then(Value::as_str).unwrap_or("");
        let group = self
            .find_group(name, arn)
            .cloned()
            .unwrap_or_else(|| json!({}));
        Response::json_status(200, &json!({"Group": group}))
    }

    fn get_groups(&self) -> Response {
        let groups: Vec<Value> = self.groups.values().cloned().collect();
        Response::json_status(200, &json!({"Groups": groups, "NextToken": Value::Null}))
    }

    fn delete_group(&mut self, params: &Value) -> Response {
        let name = params
            .get("GroupName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let arn = params.get("GroupARN").and_then(Value::as_str).unwrap_or("");
        let key = self
            .find_group(name, arn)
            .and_then(|g| g.get("GroupARN").and_then(Value::as_str).map(String::from));
        if let Some(garn) = key {
            self.groups
                .retain(|_, v| v.get("GroupARN").and_then(Value::as_str) != Some(garn.as_str()));
            self.tags.remove(&garn);
        }
        Response::json_status(200, &json!({}))
    }

    fn update_group(&mut self, params: &Value) -> Response {
        let name = params
            .get("GroupName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let arn = params.get("GroupARN").and_then(Value::as_str).unwrap_or("");
        let Some(key) = self
            .find_group(name, arn)
            .and_then(|g| g.get("GroupName").and_then(Value::as_str).map(String::from))
        else {
            let ident = if name.is_empty() { arn } else { name };
            return crate::proto::json_error(
                400,
                "InvalidRequestException",
                &format!("Group not found: {ident}"),
            );
        };
        let group = self.groups.get_mut(&key).unwrap();
        if let Some(f) = params.get("FilterExpression") {
            group["FilterExpression"] = f.clone();
        }
        if let Some(c) = params.get("InsightsConfiguration") {
            group["InsightsConfiguration"] = c.clone();
        }
        Response::json_status(200, &json!({"Group": group}))
    }

    // ---- encryption config ------------------------------------------------

    fn default_encryption() -> Value {
        json!({"Type": "NONE", "Status": "ACTIVE"})
    }

    fn get_encryption_config(&self) -> Response {
        let config = self
            .encryption_config
            .clone()
            .unwrap_or_else(Self::default_encryption);
        Response::json_status(200, &json!({"EncryptionConfig": config}))
    }

    fn put_encryption_config(&mut self, params: &Value) -> Response {
        let mut config = self
            .encryption_config
            .clone()
            .unwrap_or_else(Self::default_encryption);
        let enc_type = params
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("NONE")
            .to_string();
        config["Type"] = json!(enc_type);
        config["Status"] = json!("ACTIVE");
        if let Some(k) = params.get("KeyId") {
            if !k.is_null() {
                config["KeyId"] = k.clone();
            } else if enc_type == "NONE" {
                if let Some(map) = config.as_object_mut() {
                    map.remove("KeyId");
                }
            }
        }
        self.encryption_config = Some(config.clone());
        Response::json_status(200, &json!({"EncryptionConfig": config}))
    }

    // ---- tags -------------------------------------------------------------

    fn tag_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or("");
        let new_tags = params
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let existing = self.tags.entry(arn.to_string()).or_default();
        let mut existing_keys: HashMap<String, usize> = HashMap::new();
        for (i, t) in existing.iter().enumerate() {
            if let Some(k) = t.get("Key").and_then(Value::as_str) {
                existing_keys.insert(k.to_string(), i);
            }
        }
        for t in new_tags {
            let key = t
                .get("Key")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match existing_keys.get(&key) {
                Some(i) => existing[*i] = t,
                None => {
                    let _ = existing_keys.insert(key.clone(), existing.len());
                    existing.push(t);
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    fn untag_resource(&mut self, params: &Value) -> Response {
        let arn = params
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or("");
        let keys: Vec<&str> = params
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if let Some(existing) = self.tags.get_mut(arn) {
            existing
                .retain(|t| !keys.contains(&t.get("Key").and_then(Value::as_str).unwrap_or("")));
        }
        Response::json_status(200, &json!({}))
    }

    fn list_tags_for_resource(&self, params: &Value) -> Response {
        let arn = params
            .get("ResourceARN")
            .and_then(Value::as_str)
            .unwrap_or("");
        Response::json_status(
            200,
            &json!({"Tags": self.tags.get(arn), "NextToken": Value::Null}),
        )
    }

    // ---- resource policies ------------------------------------------------

    fn put_resource_policy(&mut self, params: &Value) -> Response {
        let name = params
            .get("PolicyName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let document = params
            .get("PolicyDocument")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let revision = uuid::Uuid::new_v4().to_string();
        let policy = json!({
            "PolicyName": name,
            "PolicyDocument": document,
            "PolicyRevisionId": revision,
            "LastUpdatedTime": 0.0,
        });
        self.policies.insert(name.to_string(), policy.clone());
        Response::json_status(200, &json!({"ResourcePolicy": policy}))
    }

    fn list_resource_policies(&self) -> Response {
        let policies: Vec<Value> = self.policies.values().cloned().collect();
        Response::json_status(
            200,
            &json!({"ResourcePolicies": policies, "NextToken": Value::Null}),
        )
    }

    fn delete_resource_policy(&mut self, params: &Value) -> Response {
        let name = params
            .get("PolicyName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let revision = params
            .get("PolicyRevisionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(existing) = self.policies.get(name) {
            let existing_rev = existing
                .get("PolicyRevisionId")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !revision.is_empty() && existing_rev != revision {
                return crate::proto::json_error(
                    400,
                    "InvalidPolicyRevisionIdException",
                    "The provided policy revision id does not match.",
                );
            }
            self.policies.remove(name);
        }
        Response::json_status(200, &json!({}))
    }

    // ---- traces -----------------------------------------------------------

    fn put_trace_segments(&mut self, params: &Value) -> Response {
        let docs = params
            .get("TraceSegmentDocuments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut unprocessed: Vec<Value> = Vec::new();
        for doc in docs {
            let raw = match doc.as_str() {
                Some(s) => s.to_string(),
                None => doc.to_string(),
            };
            let parsed: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    unprocessed.push(json!({
                        "Id": Value::Null,
                        "ErrorCode": "INVALID_DOCUMENT",
                        "Message": e.to_string(),
                    }));
                    continue;
                }
            };
            self.absorb_segment(&parsed, &raw);
        }
        Response::json_status(200, &json!({"UnprocessedTraceSegments": unprocessed}))
    }

    /// Feed a parsed segment document into both the trace store and the
    /// per-service rolling statistics (the py correlation engine behavior).
    fn absorb_segment(&mut self, seg: &Value, raw: &str) {
        let trace_id = seg.get("trace_id").and_then(Value::as_str).unwrap_or("");
        if trace_id.is_empty() {
            return;
        }
        // Rolling stats per service name.
        let name = seg.get("name").and_then(Value::as_str).unwrap_or("unknown");
        let start = seg.get("start_time").and_then(Value::as_f64).unwrap_or(0.0);
        let end = seg.get("end_time").and_then(Value::as_f64).unwrap_or(0.0);
        let stats = self.stats.entry(name.to_string()).or_default();
        if end > start {
            stats.latencies.push(end - start);
        }
        stats
            .error_rates
            .push(if truthy(seg.get("error")) { 1.0 } else { 0.0 });

        let entry = self.traces.entry(trace_id.to_string()).or_default();
        entry.trace_id = trace_id.to_string();
        let seg_id = seg
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        entry.segments.push((seg_id, format!("{raw}\n")));
        entry.start_time = if entry.start_time == 0.0 {
            start
        } else {
            entry.start_time.min(start)
        };
        entry.end_time = entry.end_time.max(end);
        entry.duration = (entry.end_time - entry.start_time) as i64;
        if truthy(seg.get("error")) {
            entry.has_error = true;
        }
        if truthy(seg.get("fault")) {
            entry.has_fault = true;
        }
        if truthy(seg.get("throttle")) {
            entry.has_throttle = true;
        }
        if let Some(status) = seg
            .get("http")
            .and_then(|h| h.get("response"))
            .and_then(|r| r.get("status"))
        {
            entry.http_status.get_or_insert(status.clone());
        }
        if !entry.services.iter().any(|s| s == name) {
            entry.services.push(name.to_string());
        }
    }

    fn batch_get_traces(&self, params: &Value) -> Response {
        let Some(ids) = params.get("TraceIds").and_then(Value::as_array) else {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter TraceIds is missing",
            );
        };
        let mut traces = Vec::new();
        let mut unprocessed = Vec::new();
        for id in ids {
            let id = id.as_str().unwrap_or("");
            match self.traces.get(id) {
                Some(entry) => {
                    let segments: Vec<Value> = entry
                        .segments
                        .iter()
                        .map(|(seg_id, raw)| json!({"Id": seg_id, "Document": raw}))
                        .collect();
                    traces.push(json!({
                        "Duration": entry.duration,
                        "Id": entry.trace_id,
                        "Segments": segments,
                    }));
                }
                None => unprocessed.push(id),
            }
        }
        Response::json_status(
            200,
            &json!({"Traces": traces, "UnprocessedTraceIds": unprocessed}),
        )
    }

    fn get_trace_summaries(&self, params: &Value) -> Response {
        let start = params
            .get("StartTime")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let end = params.get("EndTime").and_then(Value::as_f64).unwrap_or(0.0);
        let mut summaries = Vec::new();
        for entry in self.traces.values() {
            if entry.start_time < start || entry.end_time > end {
                continue;
            }
            let summary = json!({
                "Id": entry.trace_id,
                "Duration": entry.duration,
                "ResponseTime": entry.duration,
                "HasFault": entry.has_fault,
                "HasError": entry.has_error,
                "HasThrottle": entry.has_throttle,
                "Http": entry.http_status.as_ref().map(|s| json!({"HttpStatus": s})).unwrap_or(json!({})),
                "Annotations": {},
                "Users": [],
                "ServiceIds": entry.services.iter().map(|s| json!({"Name": s, "Names": [s], "AccountId": Value::Null, "Type": "AWS::Service"})).collect::<Vec<_>>(),
                "EntryPoint": Value::Null,
                "FaultRootCauses": [],
                "ErrorRootCauses": [],
                "ResponseTimeRootCauses": [],
                "Revision": 0,
                "MatchedEventTime": entry.start_time,
            });
            summaries.push(summary);
        }
        Response::json_status(
            200,
            &json!({
                "ApproximateTime": crate::proto::now_unix() as i64,
                "TracesProcessedCount": summaries.len(),
                "TraceSummaries": summaries,
            }),
        )
    }

    fn get_trace_graph(&self, _params: &Value) -> Response {
        Response::json_status(200, &json!({"Services": []}))
    }

    fn get_service_graph(&self, params: &Value) -> Response {
        let start = params.get("StartTime").cloned().unwrap_or(Value::Null);
        let end = params.get("EndTime").cloned().unwrap_or(Value::Null);
        Response::json_status(
            200,
            &json!({"StartTime": start, "EndTime": end, "Services": []}),
        )
    }

    // ---- insights ---------------------------------------------------------

    fn get_insight_summaries(&self, params: &Value) -> Response {
        let start = params
            .get("StartTime")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let end = params.get("EndTime").and_then(Value::as_f64).unwrap_or(0.0);
        let mut insights = Vec::new();
        let now = crate::proto::now_unix();
        for (name, stats) in &self.stats {
            if let Some(categories) = anomalies(stats) {
                insights.push(json!({
                    "InsightId": format!("{name}-{now}"),
                    "GroupARN": params.get("GroupARN"),
                    "GroupName": params.get("GroupName").cloned().unwrap_or(json!("Default")),
                    "RootCauseServiceId": {
                        "Name": name,
                        "Names": [name],
                        "AccountId": Value::Null,
                        "Type": "AWS::Service",
                    },
                    "Categories": categories,
                    "State": "ACTIVE",
                    "StartTime": start,
                    "EndTime": end,
                    "Summary": format!("Anomaly detected in {name}: {}", categories.join(", ")),
                    "ClientRequestImpactStatistics": {"FaultCount": 0, "OkCount": 0, "TotalCount": 0},
                    "RootCauseServiceRequestImpactStatistics": {"FaultCount": 0, "OkCount": 0, "TotalCount": 0},
                    "TopAnomalousServices": [{
                        "ServiceId": {
                            "Name": name,
                            "Names": [name],
                            "AccountId": Value::Null,
                            "Type": "AWS::Service",
                        }
                    }],
                    "LastUpdateTime": now,
                }));
            }
        }
        Response::json_status(200, &json!({"InsightSummaries": insights}))
    }

    /// Mirrors moto's insight stubs: unknown insight ids always raise.
    fn insight_missing(&self, code: &str, params: &Value) -> Response {
        let id = params
            .get("InsightId")
            .and_then(Value::as_str)
            .unwrap_or("");
        crate::proto::json_error(400, code, &format!("Insight {id} not found."))
    }

    // ---- indexing rules ---------------------------------------------------

    fn default_indexing_rules() -> Vec<Value> {
        vec![json!({
            "Name": "Default",
            "ModifiedAt": crate::proto::now_unix(),
            "Rule": {"Probabilistic": {
                "DesiredSamplingPercentage": 100.0,
                "ActualSamplingPercentage": 100.0,
            }},
        })]
    }

    fn get_indexing_rules(&self) -> Response {
        Response::json_status(
            200,
            &json!({"IndexingRules": self
                .indexing_rules
                .clone()
                .unwrap_or_else(Self::default_indexing_rules)}),
        )
    }

    fn update_indexing_rule(&mut self, params: &Value) -> Response {
        let name = params.get("Name").and_then(Value::as_str).unwrap_or("");
        let rule_in = params.get("Rule").cloned().unwrap_or(json!({}));
        if self.indexing_rules.is_none() {
            self.indexing_rules = Some(Self::default_indexing_rules());
        }
        let rules = self.indexing_rules.as_mut().unwrap();
        for rule in rules.iter_mut() {
            if rule.get("Name").and_then(Value::as_str) == Some(name) {
                if let Some(desired) = rule_in
                    .get("Probabilistic")
                    .and_then(|p| p.get("DesiredSamplingPercentage"))
                {
                    rule["Rule"]["Probabilistic"]["DesiredSamplingPercentage"] = desired.clone();
                    rule["Rule"]["Probabilistic"]["ActualSamplingPercentage"] = desired.clone();
                }
                rule["ModifiedAt"] = json!(crate::proto::now_unix());
                return Response::json_status(200, &json!({"IndexingRule": rule}));
            }
        }
        crate::proto::json_error(
            400,
            "InvalidRequestException",
            &format!("Indexing rule not found: {name}"),
        )
    }

    // ---- trace retrieval --------------------------------------------------

    fn start_trace_retrieval(&mut self, params: &Value) -> Response {
        if params.get("TraceIds").is_none() {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter TraceIds is missing",
            );
        }
        if params.get("StartTime").is_none() {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter StartTime is missing",
            );
        }
        if params.get("EndTime").is_none() {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter EndTime is missing",
            );
        }
        let token = uuid::Uuid::new_v4().to_string();
        self.retrievals
            .insert(token.clone(), "COMPLETE".to_string());
        Response::json_status(200, &json!({"RetrievalToken": token}))
    }

    fn cancel_trace_retrieval(&mut self, params: &Value) -> Response {
        let Some(token) = params.get("RetrievalToken").and_then(Value::as_str) else {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter RetrievalToken is missing",
            );
        };
        if let Some(status) = self.retrievals.get_mut(token) {
            *status = "CANCELLED".to_string();
        }
        Response::json_status(200, &json!({}))
    }

    fn get_retrieved_traces_graph(&self, params: &Value) -> Response {
        let Some(token) = params.get("RetrievalToken").and_then(Value::as_str) else {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter RetrievalToken is missing",
            );
        };
        Response::json_status(
            200,
            &json!({
                "RetrievalStatus": self.retrievals.get(token).cloned().unwrap_or_else(|| "COMPLETE".to_string()),
                "Services": [],
            }),
        )
    }

    fn list_retrieved_traces(&self, params: &Value) -> Response {
        let Some(token) = params.get("RetrievalToken").and_then(Value::as_str) else {
            return crate::proto::json_error(
                400,
                "MissingParameter",
                "Parameter RetrievalToken is missing",
            );
        };
        let fmt = params
            .get("TraceFormat")
            .and_then(Value::as_str)
            .unwrap_or("XRAY");
        Response::json_status(
            200,
            &json!({
                "RetrievalStatus": self.retrievals.get(token).cloned().unwrap_or_else(|| "COMPLETE".to_string()),
                "TraceFormat": fmt,
                "Traces": [],
            }),
        )
    }

    // ---- trace segment destination ----------------------------------------

    fn get_trace_segment_destination(&self) -> Response {
        Response::json_status(
            200,
            &json!({
                "Destination": self.trace_segment_destination.clone().unwrap_or_else(|| "XRay".to_string()),
                "Status": "ACTIVE",
            }),
        )
    }

    fn update_trace_segment_destination(&mut self, params: &Value) -> Response {
        if let Some(dest) = params.get("Destination").and_then(Value::as_str) {
            self.trace_segment_destination = Some(dest.to_string());
        }
        self.get_trace_segment_destination()
    }
}

/// Latency/error anomaly check matching the py correlation engine: flag a
/// service when 5+ samples exist and the recent mean exceeds 2 standard
/// deviations above the rolling mean.
fn anomalies(stats: &RollingStats) -> Option<Vec<&'static str>> {
    let mut categories = Vec::new();
    if stats.latencies.len() >= 5 {
        let m = mean(&stats.latencies);
        let s = stdev(&stats.latencies, m);
        let recent_mean = mean(&stats.latencies[stats.latencies.len() - 3..]);
        if s > 0.0 && recent_mean - m > 2.0 * s {
            categories.push("LATENCY");
        }
    }
    if stats.error_rates.len() >= 5 {
        let m = mean(&stats.error_rates);
        let s = stdev(&stats.error_rates, m);
        let recent_mean = mean(&stats.error_rates[stats.error_rates.len() - 3..]);
        if s > 0.0 && recent_mean - m > 2.0 * s {
            categories.push("FAULT");
        }
    }
    if categories.is_empty() {
        None
    } else {
        Some(categories)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // Unique account per test: the state slot is process-global, so tests
    // running concurrently must not share a scope.
    fn req(method: &str, path: &str, body: Value) -> Request {
        reqacct(method, path, body, 123456789001)
    }

    fn reqacct(method: &str, path: &str, body: Value, account: u64) -> Request {
        Request {
            service: "xray".into(),
            operation: path.into(),
            method: method.into(),
            path: format!("/{path}"),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn sampling_rule_lifecycle() {
        let created = body(dispatch(&req(
            "POST",
            "CreateSamplingRule",
            json!({"SamplingRule": {"RuleName": "r", "ResourceARN": "*", "Priority": 1000,
                "FixedRate": 0.05, "ReservoirSize": 1, "ServiceName": "*", "ServiceType": "*",
                "Host": "*", "HTTPMethod": "*", "URLPath": "*", "Version": 1}}),
        )));
        assert_eq!(
            created["SamplingRuleRecord"]["SamplingRule"]["RuleName"],
            "r"
        );
        assert_eq!(
            created["SamplingRuleRecord"]["SamplingRule"]["FixedRate"],
            0.05
        );

        let listed = body(dispatch(&req("POST", "GetSamplingRules", json!({}))));
        assert!(listed["SamplingRuleRecords"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["SamplingRule"]["RuleName"] == "r"));

        let updated = body(dispatch(&req(
            "POST",
            "UpdateSamplingRule",
            json!({"SamplingRuleUpdate": {"RuleName": "r", "FixedRate": 0.10, "Priority": 900}}),
        )));
        assert_eq!(
            updated["SamplingRuleRecord"]["SamplingRule"]["FixedRate"],
            0.10
        );
        assert_eq!(
            updated["SamplingRuleRecord"]["SamplingRule"]["Priority"],
            900
        );

        let deleted = body(dispatch(&req(
            "POST",
            "DeleteSamplingRule",
            json!({"RuleName": "r"}),
        )));
        assert_eq!(
            deleted["SamplingRuleRecord"]["SamplingRule"]["RuleName"],
            "r"
        );
    }

    #[test]
    fn group_lifecycle_with_filter() {
        let created = body(dispatch(&req(
            "POST",
            "CreateGroup",
            json!({"GroupName": "g", "FilterExpression": "service(\"x\")"}),
        )));
        assert_eq!(created["Group"]["GroupName"], "g");
        let arn = created["Group"]["GroupARN"].as_str().unwrap().to_string();

        let by_arn = body(dispatch(&req("POST", "GetGroup", json!({"GroupARN": arn}))));
        assert_eq!(by_arn["Group"]["GroupName"], "g");

        let updated = body(dispatch(&req(
            "POST",
            "UpdateGroup",
            json!({"GroupName": "g", "FilterExpression": "service(\"y\")"}),
        )));
        assert_eq!(updated["Group"]["FilterExpression"], "service(\"y\")");

        assert_eq!(
            dispatch(&req("POST", "DeleteGroup", json!({"GroupARN": arn}))).status,
            200
        );
        let after = body(dispatch(&req(
            "POST",
            "GetGroup",
            json!({"GroupName": "g"}),
        )));
        assert_eq!(after["Group"], json!({}));
    }

    #[test]
    fn tag_untag_resource() {
        dispatch(&reqacct(
            "POST",
            "CreateGroup",
            json!({"GroupName": "t"}),
            123456789002,
        ));
        let groups = body(dispatch(&reqacct(
            "POST",
            "Groups",
            json!({}),
            123456789002,
        )));
        let arn = groups["Groups"][0]["GroupARN"]
            .as_str()
            .unwrap()
            .to_string();
        let acct = 123456789002;

        dispatch(&reqacct(
            "POST",
            "TagResource",
            json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
            acct,
        ));
        let tags = body(dispatch(&reqacct(
            "POST",
            "ListTagsForResource",
            json!({"ResourceARN": arn}),
            acct,
        )));
        assert_eq!(tags["Tags"][0]["Key"], "env");

        dispatch(&reqacct(
            "POST",
            "UntagResource",
            json!({"ResourceARN": arn, "TagKeys": ["env"]}),
            acct,
        ));
        let after = body(dispatch(&reqacct(
            "POST",
            "ListTagsForResource",
            json!({"ResourceARN": arn}),
            acct,
        )));
        assert_eq!(after["Tags"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn resource_policies_revision_check() {
        let put = body(dispatch(&req(
            "POST",
            "PutResourcePolicy",
            json!({"PolicyName": "p", "PolicyDocument": "{}"}),
        )));
        let rev1 = put["ResourcePolicy"]["PolicyRevisionId"]
            .as_str()
            .unwrap()
            .to_string();

        let bad = dispatch(&req(
            "POST",
            "DeleteResourcePolicy",
            json!({"PolicyName": "p", "PolicyRevisionId": "wrong"}),
        ));
        assert_eq!(bad.status, 400);
        let err: Value = serde_json::from_slice(&bad.body).unwrap();
        assert_eq!(err["__type"], "InvalidPolicyRevisionIdException");

        let ok = dispatch(&req(
            "POST",
            "DeleteResourcePolicy",
            json!({"PolicyName": "p", "PolicyRevisionId": rev1}),
        ));
        assert_eq!(ok.status, 200);
    }

    #[test]
    fn traces_store_and_summaries() {
        let doc = json!({
            "trace_id": "1-12345678-abcdef012345678901234567",
            "id": "aaaa111122223333",
            "name": "svc",
            "start_time": 1000.0,
            "end_time": 1005.0,
        });
        let put = body(dispatch(&req(
            "POST",
            "TraceSegments",
            json!({"TraceSegmentDocuments": [doc.to_string()]}),
        )));
        assert_eq!(put["UnprocessedTraceSegments"].as_array().unwrap().len(), 0);

        let traces = body(dispatch(&req(
            "POST",
            "Traces",
            json!({"TraceIds": ["1-12345678-abcdef012345678901234567"]}),
        )));
        assert_eq!(traces["UnprocessedTraceIds"].as_array().unwrap().len(), 0);
        assert_eq!(traces["Traces"][0]["Duration"], 5);
        assert_eq!(traces["Traces"][0]["Segments"][0]["Id"], "aaaa111122223333");

        let summaries = body(dispatch(&req(
            "POST",
            "TraceSummaries",
            json!({"StartTime": 999.0, "EndTime": 1006.0}),
        )));
        assert_eq!(
            summaries["TraceSummaries"][0]["Id"],
            "1-12345678-abcdef012345678901234567"
        );
        assert!(summaries["ApproximateTime"].is_number());

        let miss = body(dispatch(&req(
            "POST",
            "Traces",
            json!({"TraceIds": ["1-12345678-000000000000000000000000"]}),
        )));
        assert_eq!(miss["UnprocessedTraceIds"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn insights_missing_raises() {
        let err = dispatch(&req("POST", "Insight", json!({"InsightId": "fake"})));
        assert_eq!(err.status, 400);
        let body: Value = serde_json::from_slice(&err.body).unwrap();
        assert_eq!(body["__type"], "InvalidRequestException");
    }

    #[test]
    fn encryption_config_round_trip() {
        assert_eq!(
            body(dispatch(&req("POST", "EncryptionConfig", json!({}))))["EncryptionConfig"]["Type"],
            "NONE"
        );
        let put = body(dispatch(&req(
            "POST",
            "PutEncryptionConfig",
            json!({"Type": "NONE"}),
        )));
        assert_eq!(put["EncryptionConfig"]["Type"], "NONE");
        assert_eq!(put["EncryptionConfig"]["Status"], "ACTIVE");
    }

    #[test]
    fn trace_retrieval_lifecycle() {
        let started = body(dispatch(&req(
            "POST",
            "StartTraceRetrieval",
            json!({"TraceIds": ["1-12345678-abcdef012345678901234567"],
                   "StartTime": 1000.0, "EndTime": 1001.0}),
        )));
        assert!(started["RetrievalToken"].as_str().is_some());
        let token = started["RetrievalToken"].as_str().unwrap().to_string();

        let graph = body(dispatch(&req(
            "POST",
            "GetRetrievedTracesGraph",
            json!({"RetrievalToken": token}),
        )));
        assert_eq!(graph["RetrievalStatus"], "COMPLETE");

        assert_eq!(
            dispatch(&req(
                "POST",
                "CancelTraceRetrieval",
                json!({"RetrievalToken": token})
            ))
            .status,
            200
        );
        let after = body(dispatch(&req(
            "POST",
            "GetRetrievedTracesGraph",
            json!({"RetrievalToken": token}),
        )));
        assert_eq!(after["RetrievalStatus"], "CANCELLED");
    }
}
