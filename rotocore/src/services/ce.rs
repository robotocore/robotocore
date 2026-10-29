//! AWS Cost Explorer (CE) backend (JSON protocol, target `AWSInsightsIndexService`).
//!
//! Port of moto's `ce` service: cost-category CRUD, anomaly monitors and
//! subscriptions, stub cost queries (moto returns empty results — no cost
//! data is tracked), allocation-tag management and the async start ops.
//!
//! Not-portable: moto's `/moto-api/static/ce/cost-and-usage-results`
//! results-injection queue is a moto test-API feature, not part of the
//! AWS surface — `GetCostAndUsage` returns moto's empty default.

use crate::proto::{json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use chrono::Datelike;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// arn -> CostCategoryDefinition record
    cost_categories: Vec<(String, Value)>,
    /// arn -> AnomalyMonitor record
    monitors: Vec<(String, Value)>,
    /// arn -> AnomalySubscription record
    subscriptions: Vec<(String, Value)>,
    /// TagKey -> CostAllocationTag record
    allocation_tags: Vec<(String, Value)>,
    /// arn -> {tag key: value} (moto's TaggingService; insertion-ordered)
    tags: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("ce"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

/// Python truthiness for the values moto checks (`if self.monitor_dimension:`).
fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
    }
}

/// moto's `iso_8601_datetime_without_milliseconds`: second precision + `Z`.
fn now_iso_seconds() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// moto's `first_day`: midnight on day 1 of the current month.
fn first_day() -> String {
    let now = chrono::Utc::now();
    format!("{:04}-{:02}-01T00:00:00Z", now.year(), now.month())
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

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ce" {
        return None;
    }
    let body = parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

// --- TaggingService mirror -------------------------------------------------

/// moto's `TaggingService.tag_resource`: merge/overwrite, keep position.
fn tag_apply(st: &mut State, arn: &str, tags: Option<&Value>) {
    let Some(list) = tags.and_then(Value::as_array) else {
        return;
    };
    if st.tags.iter().all(|(a, _)| a != arn) {
        st.tags.push((arn.to_string(), json!({})));
    }
    let (_, map) = st
        .tags
        .iter_mut()
        .find(|(a, _)| a == arn)
        .expect("just created");
    let obj = map.as_object_mut().expect("tag map");
    for tag in list {
        if let Some(key) = tag.get("Key").and_then(Value::as_str) {
            obj.insert(
                key.to_string(),
                tag.get("Value").cloned().unwrap_or(Value::Null),
            );
        }
    }
}

/// moto's `TaggingService.untag_resource_using_names`.
fn tag_remove(st: &mut State, arn: &str, keys: &[String]) {
    if let Some((_, map)) = st.tags.iter_mut().find(|(a, _)| a == arn) {
        if let Some(obj) = map.as_object_mut() {
            for key in keys {
                obj.remove(key);
            }
        }
    }
}

/// moto's `TaggingService.list_tags_for_resource`: [{"Key", "Value"}].
fn tag_list(st: &State, arn: &str) -> Vec<Value> {
    st.tags
        .iter()
        .find(|(a, _)| a == arn)
        .and_then(|(_, v)| v.as_object())
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({"Key": k, "Value": v}))
                .collect()
        })
        .unwrap_or_default()
}

// --- Services ----------------------------------------------------------------

fn cost_category_response(st: &mut State, req: &Request, p: &Value) -> Response {
    let account = req.account.to_string();
    let name = p
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let effective_start = p
        .get("EffectiveStart")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(first_day);
    let arn = format!(
        "arn:{}:ce::{account}:costcategory/{}",
        partition(&req.region),
        uuid::Uuid::new_v4()
    );
    let record = json!({
        "CostCategoryArn": arn,
        "Name": name,
        "EffectiveStart": effective_start,
        "RuleVersion": p.get("RuleVersion").cloned().unwrap_or(Value::Null),
        "Rules": p.get("Rules").cloned().unwrap_or(Value::Null),
        "DefaultValue": p.get("DefaultValue").cloned().unwrap_or(Value::Null),
        "SplitChargeRules": p.get("SplitChargeRules").cloned().unwrap_or(Value::Null),
    });
    st.cost_categories.push((arn.clone(), record));
    tag_apply(st, &arn, p.get("ResourceTags"));
    ok(&json!({"CostCategoryArn": arn, "EffectiveStart": effective_start}))
}

fn describe_cost_category<'a>(st: &'a State, arn: &str) -> Result<&'a Value, Response> {
    st.cost_categories
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v)
        .ok_or_else(|| {
            let ccd_id = arn.rsplit('/').next().unwrap_or(arn);
            not_found(&format!("No Cost Categories found with ID {ccd_id}"))
        })
}

fn update_cost_category_response(st: &mut State, p: &Value) -> Response {
    let arn = p
        .get("CostCategoryArn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if st.cost_categories.iter().all(|(a, _)| a != &arn) {
        let ccd_id = arn.rsplit('/').next().unwrap_or(&arn);
        return not_found(&format!("No Cost Categories found with ID {ccd_id}"));
    }
    let effective_start = p
        .get("EffectiveStart")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(first_day);
    if let Some((_, rec)) = st.cost_categories.iter_mut().find(|(a, _)| a == &arn) {
        rec["RuleVersion"] = p.get("RuleVersion").cloned().unwrap_or(Value::Null);
        rec["Rules"] = p.get("Rules").cloned().unwrap_or(Value::Null);
        rec["DefaultValue"] = p.get("DefaultValue").cloned().unwrap_or(Value::Null);
        rec["SplitChargeRules"] = p.get("SplitChargeRules").cloned().unwrap_or(Value::Null);
        rec["EffectiveStart"] = json!(effective_start);
    }
    ok(&json!({"CostCategoryArn": arn, "EffectiveStart": effective_start}))
}

fn make_monitor(req: &Request, monitor: &Value) -> Value {
    let arn = format!(
        "arn:{}:ce::{}:anomalymonitor/{}",
        partition(&req.region),
        req.account,
        uuid::Uuid::new_v4()
    );
    let creation = now_iso_seconds();
    let mut rec = json!({
        "MonitorArn": arn,
        "MonitorName": monitor.get("MonitorName").cloned().unwrap_or(Value::Null),
        "MonitorType": monitor.get("MonitorType").cloned().unwrap_or(Value::Null),
        "CreationDate": creation.clone(),
        "LastUpdatedDate": creation,
        "LastEvaluatedDate": "",
        "DimensionalValueCount": 0,
    });
    // moto only emits these keys when present (truthy).
    if truthy(monitor.get("MonitorDimension")) {
        rec["MonitorDimension"] = monitor["MonitorDimension"].clone();
    }
    if truthy(monitor.get("MonitorSpecification")) {
        rec["MonitorSpecification"] = monitor["MonitorSpecification"].clone();
    }
    rec
}

fn make_subscription(req: &Request, sub: &Value) -> Value {
    let arn = format!(
        "arn:{}:ce::{}:anomalysubscription/{}",
        partition(&req.region),
        req.account,
        uuid::Uuid::new_v4()
    );
    // moto: `threshold or 0.0` — absent/null/zero all normalize to 0.0.
    let threshold = sub
        .get("Threshold")
        .and_then(Value::as_f64)
        .filter(|f| *f != 0.0)
        .map(Value::from)
        .unwrap_or(json!(0.0));
    let mut rec = json!({
        "SubscriptionArn": arn,
        "SubscriptionName": sub.get("SubscriptionName").cloned().unwrap_or(Value::Null),
        "MonitorArnList": sub.get("MonitorArnList").cloned().unwrap_or(json!([])),
        "Subscribers": sub.get("Subscribers").cloned().unwrap_or(json!([])),
        "Frequency": sub.get("Frequency").cloned().unwrap_or(Value::Null),
        "Threshold": threshold,
        "AccountId": req.account.to_string(),
    });
    if truthy(sub.get("ThresholdExpression")) {
        rec["ThresholdExpression"] = sub["ThresholdExpression"].clone();
    }
    rec
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    match req.operation.as_str() {
        // --- Cost categories ---
        "CreateCostCategoryDefinition" => cost_category_response(st, req, p),
        "DescribeCostCategoryDefinition" => {
            let arn = p
                .get("CostCategoryArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match describe_cost_category(st, arn) {
                Ok(rec) => ok(&json!({"CostCategory": rec})),
                Err(resp) => resp,
            }
        }
        "UpdateCostCategoryDefinition" => update_cost_category_response(st, p),
        "DeleteCostCategoryDefinition" => {
            // moto pops without a not-found check.
            let arn = p
                .get("CostCategoryArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            st.cost_categories.retain(|(a, _)| a != &arn);
            ok(&json!({"CostCategoryArn": arn, "EffectiveEnd": ""}))
        }
        "ListCostCategoryDefinitions" => {
            let refs: Vec<Value> = st
                .cost_categories
                .iter()
                .map(|(_, rec)| {
                    json!({
                        "CostCategoryArn": rec["CostCategoryArn"],
                        "Name": rec["Name"],
                        "EffectiveStart": rec["EffectiveStart"],
                        "NumberOfRules": rec["Rules"]
                            .as_array()
                            .map(|r| r.len())
                            .unwrap_or(0),
                    })
                })
                .collect();
            ok(&json!({"CostCategoryReferences": refs}))
        }
        // --- Anomaly monitors ---
        "CreateAnomalyMonitor" => {
            let monitor = p.get("AnomalyMonitor").cloned().unwrap_or(json!({}));
            let rec = make_monitor(req, &monitor);
            let arn = rec["MonitorArn"].as_str().unwrap_or("").to_string();
            tag_apply(st, &arn, p.get("ResourceTags"));
            st.monitors.push((arn.clone(), rec));
            ok(&json!({"MonitorArn": arn}))
        }
        "GetAnomalyMonitors" => {
            if let Some(list) = p.get("MonitorArnList").and_then(Value::as_array) {
                if !list.is_empty() {
                    for arn in list {
                        let arn = arn.as_str().unwrap_or_default();
                        if !st.monitors.iter().any(|(a, _)| a == arn) {
                            return not_found(&format!("Monitor {arn} is not found"));
                        }
                    }
                    let monitors: Vec<Value> = list
                        .iter()
                        .filter_map(|arn| {
                            st.monitors
                                .iter()
                                .find(|(a, _)| a == arn.as_str().unwrap_or_default())
                                .map(|(_, v)| v.clone())
                        })
                        .collect();
                    return ok(&json!({"AnomalyMonitors": monitors}));
                }
            }
            let monitors: Vec<Value> = st.monitors.iter().map(|(_, v)| v.clone()).collect();
            ok(&json!({"AnomalyMonitors": monitors}))
        }
        "UpdateAnomalyMonitor" => {
            let arn = p
                .get("MonitorArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let found = st.monitors.iter().any(|(a, _)| a == &arn);
            if !found {
                return not_found(&format!("Monitor {arn} is not found"));
            }
            if let Some(name) = p
                .get("MonitorName")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                if let Some((_, rec)) = st.monitors.iter_mut().find(|(a, _)| a == &arn) {
                    rec["MonitorName"] = json!(name);
                }
            }
            if let Some((_, rec)) = st.monitors.iter_mut().find(|(a, _)| a == &arn) {
                rec["LastUpdatedDate"] = json!(now_iso_seconds());
            }
            ok(&json!({"MonitorArn": arn}))
        }
        "DeleteAnomalyMonitor" => {
            let arn = p
                .get("MonitorArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if st.monitors.iter().all(|(a, _)| a != &arn) {
                return not_found(&format!("Monitor {arn} is not found"));
            }
            st.monitors.retain(|(a, _)| a != &arn);
            ok(&json!({}))
        }
        // --- Anomaly subscriptions ---
        "CreateAnomalySubscription" => {
            let sub = p.get("AnomalySubscription").cloned().unwrap_or(json!({}));
            let rec = make_subscription(req, &sub);
            let arn = rec["SubscriptionArn"].as_str().unwrap_or("").to_string();
            tag_apply(st, &arn, p.get("ResourceTags"));
            st.subscriptions.push((arn.clone(), rec));
            ok(&json!({"SubscriptionArn": arn}))
        }
        "GetAnomalySubscriptions" => {
            let mut subs: Vec<Value> = Vec::new();
            if let Some(list) = p.get("SubscriptionArnList").and_then(Value::as_array) {
                if !list.is_empty() {
                    for arn in list {
                        let arn = arn.as_str().unwrap_or_default();
                        if let Some((_, rec)) = st.subscriptions.iter().find(|(a, _)| a == arn) {
                            subs.push(rec.clone());
                        } else {
                            return not_found(&format!("Subscription {arn} is not found"));
                        }
                    }
                } else {
                    subs.extend(st.subscriptions.iter().map(|(_, v)| v.clone()));
                }
            } else {
                subs.extend(st.subscriptions.iter().map(|(_, v)| v.clone()));
            }
            if let Some(mon) = p.get("MonitorArn").and_then(Value::as_str) {
                subs.retain(|s| {
                    s["MonitorArnList"]
                        .as_array()
                        .map(|l| l.iter().any(|a| a.as_str() == Some(mon)))
                        .unwrap_or(false)
                });
            }
            ok(&json!({"AnomalySubscriptions": subs}))
        }
        "UpdateAnomalySubscription" => {
            let arn = p
                .get("SubscriptionArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let Some((_, rec)) = st.subscriptions.iter_mut().find(|(a, _)| a == &arn) else {
                return not_found(&format!("Subscription {arn} is not found"));
            };
            if let Some(s) = p.get("SubscriptionName").filter(|v| !v.is_null()) {
                rec["SubscriptionName"] = s.clone();
            }
            if let Some(t) = p.get("Threshold").filter(|v| !v.is_null()) {
                rec["Threshold"] = t.clone();
            }
            if let Some(f) = p.get("Frequency").filter(|v| !v.is_null()) {
                rec["Frequency"] = f.clone();
            }
            if let Some(m) = p.get("MonitorArnList").filter(|v| !v.is_null()) {
                rec["MonitorArnList"] = m.clone();
            }
            if let Some(s) = p.get("Subscribers").filter(|v| !v.is_null()) {
                rec["Subscribers"] = s.clone();
            }
            if let Some(te) = p.get("ThresholdExpression").filter(|v| !v.is_null()) {
                rec["ThresholdExpression"] = te.clone();
            }
            ok(&json!({"SubscriptionArn": arn}))
        }
        "DeleteAnomalySubscription" => {
            let arn = p
                .get("SubscriptionArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if st.subscriptions.iter().all(|(a, _)| a != &arn) {
                return not_found(&format!("Subscription {arn} is not found"));
            }
            st.subscriptions.retain(|(a, _)| a != &arn);
            ok(&json!({}))
        }
        // --- Anomaly retrieval ---
        "GetAnomalies" => ok(&json!({"Anomalies": []})),
        "ProvideAnomalyFeedback" => {
            let id = p
                .get("AnomalyId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ok(&json!({"AnomalyId": id}))
        }
        // --- Cost queries (stub: no cost data is tracked) ---
        "GetCostAndUsage" | "GetCostAndUsageWithResources" => {
            ok(&json!({"ResultsByTime": [], "DimensionValueAttributes": []}))
        }
        "GetCostCategories" => {
            let names: Vec<&str> = st
                .cost_categories
                .iter()
                .filter_map(|(_, rec)| rec["Name"].as_str())
                .collect();
            let size = names.len();
            ok(&json!({
                "CostCategoryNames": names,
                "CostCategoryValues": [],
                "ReturnSize": size,
                "TotalSize": size,
            }))
        }
        "GetCostForecast" | "GetUsageForecast" => ok(&json!({
            "Total": {"Amount": "0", "Unit": "USD"},
            "ForecastResultsByTime": [],
        })),
        "GetDimensionValues" => {
            ok(&json!({"DimensionValues": [], "ReturnSize": 0, "TotalSize": 0}))
        }
        "GetReservationCoverage" => ok(&json!({"CoveragesByTime": [], "Total": {}})),
        "GetReservationPurchaseRecommendation" => ok(&json!({"Recommendations": []})),
        "GetReservationUtilization" => ok(&json!({"UtilizationsByTime": [], "Total": {}})),
        "GetRightsizingRecommendation" => ok(&json!({"RightsizingRecommendations": []})),
        "GetSavingsPlanPurchaseRecommendationDetails" => ok(&json!({})),
        "GetSavingsPlansCoverage" => ok(&json!({"SavingsPlansCoverages": []})),
        "GetSavingsPlansPurchaseRecommendation" => ok(&json!({})),
        "GetSavingsPlansUtilization" => ok(&json!({"Total": {}})),
        "GetSavingsPlansUtilizationDetails" => ok(&json!({"SavingsPlansUtilizationDetails": []})),
        "GetTags" => ok(&json!({"Tags": [], "ReturnSize": 0, "TotalSize": 0})),
        "GetApproximateUsageRecords" => {
            ok(&json!({"Services": {}, "TotalRecords": 0, "LookbackPeriod": {}}))
        }
        "GetCommitmentPurchaseAnalysis" => ok(&json!({})),
        "GetCostAndUsageComparisons" => ok(&json!({"CostAndUsageComparisons": []})),
        "GetCostComparisonDrivers" => ok(&json!({"CostComparisonDrivers": []})),
        // --- List operations ---
        "ListCommitmentPurchaseAnalyses" => ok(&json!({"AnalysisSummaryList": []})),
        "ListCostAllocationTagBackfillHistory" => ok(&json!({"BackfillRequests": []})),
        "ListCostAllocationTags" => {
            // moto filters with `if <param>:` truthiness — empty means no filter.
            let status = p
                .get("Status")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            let tag_keys: Vec<&str> = p
                .get("TagKeys")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let tag_type = p
                .get("Type")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            let tags: Vec<Value> = st
                .allocation_tags
                .iter()
                .filter(|(_, t)| status.map(|s| t["Status"] == json!(s)).unwrap_or(true))
                .filter(|(_, t)| {
                    tag_keys.is_empty()
                        || tag_keys.contains(&t["TagKey"].as_str().unwrap_or_default())
                })
                .filter(|(_, t)| tag_type.map(|ty| t["Type"] == json!(ty)).unwrap_or(true))
                .map(|(_, t)| t.clone())
                .collect();
            ok(&json!({"CostAllocationTags": tags}))
        }
        "ListCostCategoryResourceAssociations" => {
            ok(&json!({"CostCategoryResourceAssociations": []}))
        }
        "ListSavingsPlansPurchaseRecommendationGeneration" => {
            ok(&json!({"GenerationSummaryList": []}))
        }
        // --- Async start operations ---
        "StartCommitmentPurchaseAnalysis" => {
            let now = now_iso_seconds();
            ok(&json!({
                "AnalysisId": uuid::Uuid::new_v4(),
                "AnalysisStartedTime": now.clone(),
                "EstimatedCompletionTime": now,
            }))
        }
        "StartCostAllocationTagBackfill" => {
            let now = now_iso_seconds();
            let backfill = json!({
                "BackfillFrom": p.get("BackfillFrom").cloned().unwrap_or(Value::Null),
                "RequestedAt": now.clone(),
                "CompletedAt": "",
                "BackfillStatus": "SUCCEEDED",
                "LastUpdatedAt": now,
            });
            ok(&json!({"BackfillRequest": backfill}))
        }
        "StartSavingsPlansPurchaseRecommendationGeneration" => {
            let now = now_iso_seconds();
            ok(&json!({
                "RecommendationId": uuid::Uuid::new_v4(),
                "GenerationStartedTime": now.clone(),
                "EstimatedCompletionTime": now,
            }))
        }
        // --- Cost allocation tag management ---
        "UpdateCostAllocationTagsStatus" => {
            let entries = p.get("CostAllocationTagsStatus").and_then(Value::as_array);
            for entry in entries.unwrap_or(&vec![]) {
                let Some(key) = entry.get("TagKey").and_then(Value::as_str) else {
                    continue;
                };
                let status = entry.get("Status").cloned().unwrap_or(Value::Null);
                if let Some((_, rec)) = st.allocation_tags.iter_mut().find(|(k, _)| k == key) {
                    rec["Status"] = status;
                } else {
                    let new_tag = json!({
                        "TagKey": key,
                        "Type": "UserDefined",
                        "Status": status,
                        "LastUpdatedDate": now_iso_seconds(),
                    });
                    st.allocation_tags.push((key.to_string(), new_tag));
                }
            }
            ok(&json!({"Errors": []}))
        }
        // --- Tag management ---
        "ListTagsForResource" => {
            let arn = p
                .get("ResourceArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ok(&json!({"ResourceTags": tag_list(st, arn)}))
        }
        "TagResource" => {
            let arn = p
                .get("ResourceArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            tag_apply(st, arn, p.get("ResourceTags"));
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = p
                .get("ResourceArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let keys: Vec<String> = p
                .get("ResourceTagKeys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            tag_remove(st, &arn, &keys);
            ok(&json!({}))
        }
        _ => json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(op: &str, p: Value) -> Request {
        Request {
            service: "ce".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&p).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn run(op: &str, p: Value) -> (Response, Value) {
        let r = req(op, p);
        let body = parse_json_body(&r.body);
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let resp = handle(&r, b, &body);
        let parsed = serde_json::from_slice(&resp.body).unwrap();
        (resp, parsed)
    }

    #[test]
    fn cost_category_crud_and_tags() {
        let (_, created) = run(
            "CreateCostCategoryDefinition",
            json!({
                "Name": "cat1",
                "RuleVersion": "CostCategoryExpression.v1",
                "Rules": [{"Value": "v", "Rule": {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon S3"]}}}],
                "ResourceTags": [{"Key": "env", "Value": "prod"}],
            }),
        );
        assert_eq!(created["EffectiveStart"], json!(first_day()));
        let arn = created["CostCategoryArn"].as_str().unwrap().to_string();
        assert!(
            arn.starts_with("arn:aws:ce::123456789012:costcategory/"),
            "{arn}"
        );

        let (_, listed) = run("ListCostCategoryDefinitions", json!({}));
        let entry = listed["CostCategoryReferences"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["CostCategoryArn"] == json!(arn))
            .unwrap()
            .clone();
        assert_eq!(entry["NumberOfRules"], 1);
        assert_eq!(entry["Name"], "cat1");

        let described = run(
            "DescribeCostCategoryDefinition",
            json!({"CostCategoryArn": arn}),
        );
        assert_eq!(
            described.1["CostCategory"]["RuleVersion"],
            "CostCategoryExpression.v1"
        );
        assert_eq!(described.1["CostCategory"]["Rules"][0]["Value"], "v");

        let (_, updated) = run(
            "UpdateCostCategoryDefinition",
            json!({
                "CostCategoryArn": arn,
                "RuleVersion": "CostCategoryExpression.v1",
                "Rules": [{"Value": "w"}],
            }),
        );
        assert_eq!(updated["CostCategoryArn"], json!(arn));
        let described = run(
            "DescribeCostCategoryDefinition",
            json!({"CostCategoryArn": arn}),
        );
        assert_eq!(described.1["CostCategory"]["Rules"][0]["Value"], "w");

        let (_, tags) = run("ListTagsForResource", json!({"ResourceArn": arn}));
        assert_eq!(tags["ResourceTags"][0]["Key"], "env");
        assert_eq!(tags["ResourceTags"][0]["Value"], "prod");

        let (_, deleted) = run(
            "DeleteCostCategoryDefinition",
            json!({"CostCategoryArn": arn}),
        );
        assert_eq!(deleted["CostCategoryArn"], json!(arn));
        assert_eq!(deleted["EffectiveEnd"], "");

        let (resp, err) = run(
            "DescribeCostCategoryDefinition",
            json!({"CostCategoryArn": arn}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(err["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn anomaly_monitor_and_subscription_crud() {
        let (_, created) = run(
            "CreateAnomalyMonitor",
            json!({
                "AnomalyMonitor": {
                    "MonitorName": "mon1", "MonitorType": "DIMENSIONAL", "MonitorDimension": "SERVICE",
                },
            }),
        );
        let arn = created["MonitorArn"].as_str().unwrap().to_string();
        assert!(
            arn.starts_with("arn:aws:ce::123456789012:anomalymonitor/"),
            "{arn}"
        );

        let (_, listed) = run("GetAnomalyMonitors", json!({}));
        assert!(listed["AnomalyMonitors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["MonitorArn"] == json!(arn)));
        let m = listed["AnomalyMonitors"].as_array().unwrap()[0].clone();
        assert_eq!(m["LastEvaluatedDate"], "");
        assert_eq!(m["DimensionalValueCount"], 0);
        assert!(m.get("MonitorDimension").is_some());

        let (_, updated) = run(
            "UpdateAnomalyMonitor",
            json!({"MonitorArn": arn, "MonitorName": "mon2"}),
        );
        assert_eq!(updated["MonitorArn"], json!(arn));
        let (_, listed) = run("GetAnomalyMonitors", json!({}));
        assert!(listed["AnomalyMonitors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["MonitorName"] == json!("mon2")));

        let (_, sub) = run(
            "CreateAnomalySubscription",
            json!({
                "AnomalySubscription": {
                    "MonitorArnList": [arn.clone()],
                    "Subscribers": [{"Address": "a@b.c", "Type": "EMAIL"}],
                    "Frequency": "DAILY",
                    "SubscriptionName": "sub1",
                },
            }),
        );
        let sub_arn = sub["SubscriptionArn"].as_str().unwrap().to_string();
        let (_, listed) = run("GetAnomalySubscriptions", json!({}));
        let s = listed["AnomalySubscriptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["SubscriptionArn"] == json!(sub_arn))
            .unwrap();
        assert_eq!(s["Threshold"], 0.0);
        assert_eq!(s["Frequency"], "DAILY");
        assert_eq!(s["AccountId"], "123456789012");

        let (_, by_monitor) = run(
            "GetAnomalySubscriptions",
            json!({"MonitorArn": arn.clone()}),
        );
        assert_eq!(
            by_monitor["AnomalySubscriptions"].as_array().unwrap().len(),
            1
        );
        let (_, none) = run(
            "GetAnomalySubscriptions",
            json!({"MonitorArn": "arn:aws:ce::1:anomalymonitor/x"}),
        );
        assert_eq!(none["AnomalySubscriptions"].as_array().unwrap().len(), 0);

        let (_, del) = run(
            "DeleteAnomalySubscription",
            json!({"SubscriptionArn": sub_arn}),
        );
        assert_eq!(del, json!({}));
        let (resp, _) = run(
            "DeleteAnomalySubscription",
            json!({"SubscriptionArn": sub_arn}),
        );
        assert_eq!(resp.status, 400);

        run("DeleteAnomalyMonitor", json!({"MonitorArn": arn}));
        let (resp, _) = run("DeleteAnomalyMonitor", json!({"MonitorArn": arn}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn stub_queries_and_allocation_tags() {
        for op in [
            "GetCostAndUsageWithResources",
            "GetReservationCoverage",
            "GetRightsizingRecommendation",
        ] {
            let (resp, _) = run(op, json!({}));
            assert_eq!(resp.status, 200);
        }
        let (_, cau) = run("GetCostAndUsage", json!({}));
        assert!(cau.get("ResultsByTime").unwrap().is_array());
        assert!(cau.get("DimensionValueAttributes").unwrap().is_array());

        let (_, up) = run(
            "UpdateCostAllocationTagsStatus",
            json!({"CostAllocationTagsStatus": [{"TagKey": "team", "Status": "Active"}]}),
        );
        assert_eq!(up["Errors"].as_array().unwrap().len(), 0);
        let (_, listed) = run("ListCostAllocationTags", json!({}));
        assert_eq!(listed["CostAllocationTags"][0]["TagKey"], "team");
        assert_eq!(listed["CostAllocationTags"][0]["Type"], "UserDefined");
        assert_eq!(listed["CostAllocationTags"][0]["Status"], "Active");
        assert!(listed["CostAllocationTags"][0]
            .get("LastUpdatedDate")
            .is_some());

        let (_, by_status) = run("ListCostAllocationTags", json!({"Status": "Inactive"}));
        assert_eq!(by_status["CostAllocationTags"].as_array().unwrap().len(), 0);

        run(
            "UpdateCostAllocationTagsStatus",
            json!({"CostAllocationTagsStatus": [{"TagKey": "team", "Status": "Inactive"}]}),
        );
        let (_, listed) = run("ListCostAllocationTags", json!({}));
        assert_eq!(listed["CostAllocationTags"][0]["Status"], "Inactive");

        let (_, bf) = run(
            "StartCostAllocationTagBackfill",
            json!({"BackfillFrom": "2024-01-01"}),
        );
        assert_eq!(bf["BackfillRequest"]["BackfillStatus"], "SUCCEEDED");
        assert_eq!(bf["BackfillRequest"]["CompletedAt"], "");

        let (_, fb) = run(
            "ProvideAnomalyFeedback",
            json!({"AnomalyId": "a1", "Feedback": "YES"}),
        );
        assert_eq!(fb["AnomalyId"], "a1");

        let (_, untagged) = run(
            "ListTagsForResource",
            json!({"ResourceArn": "arn:aws:ce::1:x"}),
        );
        assert_eq!(untagged["ResourceTags"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn unimplemented_op_returns_501() {
        let (resp, err) = run("NotAnOperation", json!({}));
        assert_eq!(resp.status, 501);
        assert_eq!(err["__type"], "NotImplemented");
    }

    #[test]
    fn partition_names() {
        assert_eq!(partition("us-east-1"), "aws");
        assert_eq!(partition("cn-north-1"), "aws-cn");
        assert_eq!(partition("us-gov-west-1"), "aws-us-gov");
    }
}
