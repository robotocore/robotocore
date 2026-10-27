//! CloudTrail backend (JSON protocol, target
//! `com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101`).
//!
//! Port of moto's `cloudtrail`: trails (with moto's S3-bucket existence check
//! via the s3 crate's cross-service state bridge), event/insight selectors,
//! tags, event data stores, channels, queries, imports, federations,
//! dashboards, resource policies and org-admin registrations, per
//! (account, region).

use std::collections::HashMap;

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use serde_json::{json, Value};

#[derive(Clone)]
struct Trail {
    name: String,
    region: String,
    bucket_name: String,
    s3_key_prefix: Option<String>,
    sns_topic_name: Option<String>,
    is_global: bool,
    is_multi_region: bool,
    log_validation: bool,
    is_org_trail: bool,
    cw_log_group_arn: Option<String>,
    cw_role_arn: Option<String>,
    kms_key_id: Option<String>,
    is_logging: bool,
    started_epoch: Option<i64>,
    stopped_epoch: Option<i64>,
    event_selectors: Option<Value>,
    advanced_event_selectors: Option<Value>,
    insight_selectors: Option<Value>,
}

#[derive(Default)]
struct Audit {
    /// Resource ARN → {Key: Value} tags.
    tags: HashMap<String, HashMap<String, String>>,
}

struct Eds {
    arn: String,
    name: String,
    status: &'static str,
    advanced_event_selectors: Value,
    multi_region_enabled: bool,
    organization_enabled: bool,
    retention_period: i64,
    termination_protection_enabled: bool,
    kms_key_id: Option<String>,
    billing_mode: String,
    created_epoch: i64,
    updated_epoch: i64,
}

struct Channel {
    arn: String,
    name: String,
    source: String,
    destinations: Value,
}

struct Query {
    query_id: String,
    event_data_store_arn: String,
    query_statement: String,
    status: &'static str,
}

struct ImportState {
    import_id: String,
    source: String,
    destinations: Value,
    status: &'static str,
}

struct Dashboard {
    arn: String,
    name: String,
    kind: String,
    widgets: Value,
    refresh_schedule: Option<Value>,
    termination_protection_enabled: bool,
    tags_list: Value,
    created_epoch: i64,
    updated_epoch: i64,
}

#[derive(Default)]
pub struct CloudTrailState {
    trails: Vec<Trail>,
    audit: Audit,
    eds: Vec<Eds>,
    channels: Vec<Channel>,
    queries: Vec<Query>,
    imports: Vec<ImportState>,
    resource_policies: HashMap<String, String>,
    dashboards: Vec<Dashboard>,
    event_configurations: HashMap<String, Value>,
    delegated_admin: Option<String>,
}

/// Epoch seconds, moto's `datetime2int`.
fn epoch_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Trail ARN in moto's format.
fn trail_arn(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:cloudtrail:{region}:{account}:trail/{name}")
}

fn err(status: u16, code: &str, message: String) -> Response {
    json_error(status, code, &message)
}

fn not_found_trail(account: u64, name: &str) -> Response {
    err(
        400,
        "TrailNotFoundException",
        format!("Unknown trail: {name} for the user: {account}"),
    )
}

/// All trails of `account` visible from `region` (moto's shadow-trail scan:
/// `is_multi_region or trail.region == self.region`).
fn visible_trails(state: &store::Slot<CloudTrailState>, account: u64, region: &str) -> Vec<Trail> {
    let map = state.read();
    let mut keys: Vec<&ScopeKey> = map.keys().filter(|(acct, _)| *acct == account).collect();
    keys.sort();
    let mut out = Vec::new();
    for key in keys {
        let trail_region = &key.1;
        for t in &map.get(key).unwrap().trails {
            if t.is_multi_region || *trail_region == region {
                out.push(t.clone());
            }
        }
    }
    // moto iterates regions in backend-insertion order; sort by (region, name)
    // for determinism in place of that.
    out.sort_by(|a, b| a.region.cmp(&b.region).then(a.name.cmp(&b.name)));
    out
}

fn find_trail(
    state: &store::Slot<CloudTrailState>,
    account: u64,
    region: &str,
    name_or_arn: &str,
) -> Option<Trail> {
    visible_trails(state, account, region)
        .into_iter()
        .find(|t| t.name == name_or_arn || trail_arn(&t.region, account, &t.name) == name_or_arn)
}

fn trail_description(t: &Trail, account: u64, include_region: bool) -> Value {
    let mut desc = json!({
        "Name": t.name,
        "S3BucketName": t.bucket_name,
        "IncludeGlobalServiceEvents": t.is_global,
        "IsMultiRegionTrail": t.is_multi_region,
        "TrailARN": trail_arn(&t.region, account, &t.name),
        "LogFileValidationEnabled": t.log_validation,
        "IsOrganizationTrail": t.is_org_trail,
        "HasCustomEventSelectors": t.event_selectors.is_some() || t.advanced_event_selectors.is_some(),
        "HasInsightSelectors": t.insight_selectors.is_some(),
        "CloudWatchLogsLogGroupArn": t.cw_log_group_arn,
        "CloudWatchLogsRoleArn": t.cw_role_arn,
        "KmsKeyId": t.kms_key_id,
    });
    if let Some(p) = &t.s3_key_prefix {
        desc["S3KeyPrefix"] = Value::String(p.clone());
    }
    if let Some(topic) = &t.sns_topic_name {
        desc["SnsTopicName"] = Value::String(topic.clone());
        desc["SnsTopicARN"] = Value::String(format!("arn:aws:sns:{}:{account}:{topic}", t.region));
    }
    if include_region {
        desc["HomeRegion"] = Value::String(t.region.clone());
    }
    desc
}

fn trail_status_description(t: &Trail) -> Value {
    let mut desc = json!({
        "IsLogging": t.is_logging,
        "LatestDeliveryAttemptTime": "",
        "LatestNotificationAttemptTime": "",
        "LatestNotificationAttemptSucceeded": "",
        "LatestDeliveryAttemptSucceeded": "",
        "TimeLoggingStarted": "",
        "TimeLoggingStopped": "",
    });
    if t.is_logging {
        desc["StartLoggingTime"] = json!(t.started_epoch.unwrap_or_else(epoch_now));
        desc["LatestDeliveryTime"] = json!(epoch_now());
    }
    if let Some(stop) = t.stopped_epoch {
        desc["StopLoggingTime"] = json!(stop);
    }
    desc
}

fn eds_description(e: &Eds) -> Value {
    let mut desc = json!({
        "EventDataStoreArn": e.arn,
        "Name": e.name,
        "Status": e.status,
        "AdvancedEventSelectors": e.advanced_event_selectors,
        "MultiRegionEnabled": e.multi_region_enabled,
        "OrganizationEnabled": e.organization_enabled,
        "RetentionPeriod": e.retention_period,
        "TerminationProtectionEnabled": e.termination_protection_enabled,
        "CreatedTimestamp": e.created_epoch,
        "UpdatedTimestamp": e.updated_epoch,
        "BillingMode": e.billing_mode,
    });
    if let Some(kms) = &e.kms_key_id {
        desc["KmsKeyId"] = Value::String(kms.clone());
    }
    desc
}

fn channel_description(c: &Channel) -> Value {
    json!({
        "ChannelArn": c.arn,
        "Name": c.name,
        "Source": c.source,
        "Destinations": c.destinations,
    })
}

fn query_description(q: &Query) -> Value {
    json!({
        "QueryId": q.query_id,
        "QueryString": q.query_statement,
        "QueryStatus": q.status,
        "QueryStatistics": {
            "EventsMatched": 0,
            "EventsScanned": 0,
            "BytesScanned": 0,
            "ExecutionTimeInMillis": 100,
            "CreationTime": epoch_now(),
        },
    })
}

fn import_description(i: &ImportState) -> Value {
    json!({
        "ImportId": i.import_id,
        "ImportSource": { "S3": { "S3LocationUri": i.source } },
        "Destinations": i.destinations,
        "ImportStatus": i.status,
        "CreatedTimestamp": epoch_now(),
        "UpdatedTimestamp": epoch_now(),
    })
}

fn dashboard_to_dict(d: &Dashboard) -> Value {
    let mut out = json!({
        "DashboardArn": d.arn,
        "Name": d.name,
        "Type": d.kind,
        "Widgets": d.widgets,
        "TerminationProtectionEnabled": d.termination_protection_enabled,
        "TagsList": d.tags_list,
        "CreatedTimestamp": d.created_epoch,
        "UpdatedTimestamp": d.updated_epoch,
    });
    if let Some(rs) = &d.refresh_schedule {
        out["RefreshSchedule"] = rs.clone();
    }
    out
}

// --- dispatch -------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    handle(req, body).unwrap_or_else(|err_resp| err_resp)
}

fn scope_of(req: &Request) -> store::Slot<CloudTrailState> {
    let _ = req;
    store::slot::<CloudTrailState>("cloudtrail")
}

fn with_scope<T>(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    f: impl FnOnce(&mut CloudTrailState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

fn handle(req: &Request, body: Value) -> Result<Response, Response> {
    let slot = scope_of(req);
    match req.operation.as_str() {
        // --- Trails --------------------------------------------------------
        "CreateTrail" => create_trail(&slot, req, &body),
        "GetTrail" => get_trail(&slot, req, &body),
        "GetTrailStatus" => {
            let name = body_text(&body, "Name");
            describe_status(&slot, req, name)
        }
        "DescribeTrails" => describe_trails(&slot, req, &body),
        "ListTrails" => list_trails(&slot, req),
        "StartLogging" | "StopLogging" => set_logging(&slot, req, &body),
        "DeleteTrail" => {
            let name = body_text(&body, "Name");
            with_scope(&slot, req, |st| st.trails.retain(|t| t.name != name));
            Ok(ok_json(json!({})))
        }
        "UpdateTrail" => update_trail(&slot, req, &body),
        // --- Selectors -------------------------------------------------------
        "PutEventSelectors" => put_event_selectors(&slot, req, &body),
        "GetEventSelectors" => get_event_selectors(&slot, req, &body),
        "PutInsightSelectors" => put_insight_selectors(&slot, req, &body),
        "GetInsightSelectors" => get_insight_selectors(&slot, req, &body),
        // --- Tags --------------------------------------------------------------
        "AddTags" => {
            let rid = body_text(&body, "ResourceId");
            let tags = tag_pairs(&body, "TagsList");
            with_scope(&slot, req, |st| {
                let entry = st.audit.tags.entry(rid.clone()).or_default();
                for (k, v) in tags {
                    entry.insert(k, v);
                }
            });
            Ok(ok_json(json!({})))
        }
        "RemoveTags" => {
            let rid = body_text(&body, "ResourceId");
            let tags = tag_pairs(&body, "TagsList");
            with_scope(&slot, req, |st| {
                if let Some(entry) = st.audit.tags.get_mut(&rid) {
                    for (k, _) in tags {
                        entry.remove(&k);
                    }
                }
            });
            Ok(ok_json(json!({})))
        }
        "ListTags" => {
            let ids: Vec<String> = body
                .get("ResourceIdList")
                .and_then(|v| v.as_array())
                .and_then(|a| {
                    a.iter()
                        .map(|v| v.as_str().map(String::from))
                        .collect::<Option<Vec<_>>>()
                })
                .unwrap_or_default();
            let map = slot.read();
            let st = map.get(&(req.account, req.region.clone()));
            let list: Vec<Value> = ids
                .iter()
                .map(|id| {
                    let tags = st
                        .and_then(|s| s.audit.tags.get(id))
                        .map(|m| {
                            m.iter()
                                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    json!({ "ResourceId": id, "TagsList": tags })
                })
                .collect();
            Ok(Response::json_status(
                200,
                &json!({ "ResourceTagList": list }),
            ))
        }
        // --- Event data stores ---------------------------------------------------
        "CreateEventDataStore" => create_eds(&slot, req, &body),
        "GetEventDataStore" => get_eds(&slot, req, &body),
        "ListEventDataStores" => {
            let stores: Vec<Value> = with_scope(&slot, req, |st| {
                st.eds.iter().map(eds_description).collect()
            });
            Ok(Response::json_status(
                200,
                &json!({ "EventDataStores": stores }),
            ))
        }
        "DeleteEventDataStore" => delete_eds(&slot, req, &body),
        "UpdateEventDataStore" => update_eds(&slot, req, &body),
        "RestoreEventDataStore" => restore_eds(&slot, req, &body),
        // --- Channels ----------------------------------------------------------------
        "CreateChannel" => create_channel(&slot, req, &body),
        "GetChannel" => get_channel(&slot, req, &body),
        "ListChannels" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.channels
                    .iter()
                    .map(|c| json!({ "ChannelArn": c.arn, "Name": c.name }))
                    .collect()
            });
            Ok(Response::json_status(200, &json!({ "Channels": list })))
        }
        "DeleteChannel" => {
            let arn = body_text(&body, "Channel");
            match with_scope(&slot, req, |st| {
                st.channels.iter().position(|c| c.arn == arn)
            }) {
                Some(idx) => {
                    with_scope(&slot, req, |st| {
                        st.channels.remove(idx);
                    });
                    Ok(ok_json(json!({})))
                }
                None => Err(err(
                    400,
                    "ChannelNotFoundException",
                    format!("The channel {arn} was not found."),
                )),
            }
        }
        "UpdateChannel" => update_channel(&slot, req, &body),
        // --- Queries -------------------------------------------------------------
        "StartQuery" => {
            let stmt = body_text(&body, "QueryStatement");
            let eds_arn = body
                .get("EventDataStoreArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let qid = uuid::Uuid::new_v4().to_string();
            with_scope(&slot, req, |st| {
                st.queries.push(Query {
                    query_id: qid.clone(),
                    event_data_store_arn: eds_arn,
                    query_statement: stmt,
                    status: "COMPLETED",
                });
            });
            Ok(ok_json(json!({ "QueryId": qid })))
        }
        "DescribeQuery" => {
            let qid = body_text(&body, "QueryId");
            match with_scope(&slot, req, |st| {
                st.queries
                    .iter()
                    .find(|q| q.query_id == qid)
                    .map(query_description)
            }) {
                Some(desc) => Ok(Response::json_status(200, &desc)),
                None => Err(err(
                    400,
                    "QueryIdNotFoundException",
                    format!("The query ID {qid} was not found."),
                )),
            }
        }
        "GetQueryResults" => {
            let qid = body_text(&body, "QueryId");
            match with_scope(&slot, req, |st| {
                st.queries
                    .iter()
                    .find(|q| q.query_id == qid)
                    .map(|q| q.status.to_string())
            }) {
                Some(status) => Ok(Response::json_status(
                    200,
                    &json!({
                        "QueryStatus": status,
                        "QueryStatistics": { "ResultsCount": 0, "TotalResultsCount": 0, "BytesScanned": 0 },
                        "QueryResultRows": [],
                    }),
                )),
                None => Err(err(
                    400,
                    "QueryIdNotFoundException",
                    format!("The query ID {qid} was not found."),
                )),
            }
        }
        "ListQueries" => {
            let eds = body_text(&body, "EventDataStore");
            ensure_eds(&slot, req, &body)?;
            let queries: Vec<Value> = with_scope(&slot, req, |st| {
                st.queries
                    .iter()
                    .filter(|q| q.event_data_store_arn == eds || q.event_data_store_arn.is_empty())
                    .map(|q| json!({ "QueryId": q.query_id, "QueryStatus": q.status, "CreationTime": epoch_now() }))
                    .collect()
            });
            Ok(Response::json_status(200, &json!({ "Queries": queries })))
        }
        "CancelQuery" => {
            let qid = body_text(&body, "QueryId");
            match with_scope(&slot, req, |st| {
                st.queries.iter_mut().find(|q| q.query_id == qid).map(|q| {
                    q.status = "CANCELLED";
                    q.query_id.clone()
                })
            }) {
                Some(_) => Ok(ok_json(
                    json!({ "QueryId": qid, "QueryStatus": "CANCELLED" }),
                )),
                None => Err(err(
                    400,
                    "QueryIdNotFoundException",
                    format!("The query ID {qid} was not found."),
                )),
            }
        }
        // --- Imports ------------------------------------------------------------------
        "StartImport" => start_import(&slot, req, &body),
        "GetImport" => {
            let id = body_text(&body, "ImportId");
            match with_scope(&slot, req, |st| {
                st.imports
                    .iter()
                    .find(|i| i.import_id == id)
                    .map(import_description)
            }) {
                Some(d) => Ok(Response::json_status(200, &d)),
                None => Err(err(
                    400,
                    "ImportNotFoundException",
                    format!("The import {id} was not found."),
                )),
            }
        }
        "ListImports" => {
            let destination = body.get("Destination").and_then(|v| v.as_str());
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.imports
                    .iter()
                    .filter(|i| {
                        destination.is_none_or(|d| {
                            i.destinations
                                .as_array()
                                .map(|arr| arr.iter().any(|x| x.as_str() == Some(d)))
                                .unwrap_or(false)
                        })
                    })
                    .map(import_description)
                    .collect()
            });
            Ok(Response::json_status(200, &json!({ "Imports": list })))
        }
        "StopImport" => {
            let id = body_text(&body, "ImportId");
            match with_scope(&slot, req, |st| {
                st.imports.iter_mut().find(|i| i.import_id == id).map(|i| {
                    i.status = "STOPPED";
                })
            }) {
                Some(()) => {
                    let d = with_scope(&slot, req, |st| {
                        st.imports
                            .iter()
                            .find(|i| i.import_id == id)
                            .map(import_description)
                    });
                    Ok(Response::json_status(200, &d.unwrap()))
                }
                None => Err(err(
                    400,
                    "ImportNotFoundException",
                    format!("The import {id} was not found."),
                )),
            }
        }
        "ListImportFailures" => {
            let id = body_text(&body, "ImportId");
            let found = with_scope(&slot, req, |st| {
                st.imports.iter().any(|i| i.import_id == id)
            });
            if found {
                Ok(Response::json_status(
                    200,
                    &json!({ "Failures": [], "NextToken": Value::Null }),
                ))
            } else {
                Err(err(
                    400,
                    "ImportNotFoundException",
                    format!("The import {id} was not found."),
                ))
            }
        }
        // --- Federation -----------------------------------------------------------------
        "EnableFederation" => {
            let arn_text = body_text(&body, "EventDataStore");
            let role = body_text(&body, "FederationRoleArn");
            ensure_eds(&slot, req, &body)?;
            Ok(ok_json(
                json!({ "EventDataStoreArn": arn_text, "FederationStatus": "ENABLED", "FederationRoleArn": role }),
            ))
        }
        "DisableFederation" => {
            let arn_text = body_text(&body, "EventDataStore");
            ensure_eds(&slot, req, &body)?;
            Ok(ok_json(
                json!({ "EventDataStoreArn": arn_text, "FederationStatus": "DISABLED" }),
            ))
        }
        // --- Organization delegated admin -------------------------------------------------
        "RegisterOrganizationDelegatedAdmin" => {
            let member = body_text(&body, "MemberAccountId");
            with_scope(&slot, req, |st| st.delegated_admin = Some(member));
            Ok(ok_json(json!({})))
        }
        "DeregisterOrganizationDelegatedAdmin" => {
            let acct = body_text(&body, "DelegatedAdminAccountId");
            with_scope(&slot, req, |st| {
                if st.delegated_admin.as_deref() == Some(acct.as_str()) {
                    st.delegated_admin = None;
                }
            });
            Ok(ok_json(json!({})))
        }
        // --- Resource policy ---------------------------------------------------------------
        "PutResourcePolicy" => {
            let arn_text = body_text(&body, "ResourceArn");
            let policy = body_text(&body, "ResourcePolicy");
            with_scope(&slot, req, |st| {
                st.resource_policies
                    .insert(arn_text.clone(), policy.clone());
            });
            Ok(ok_json(
                json!({ "ResourceArn": arn_text, "ResourcePolicy": policy }),
            ))
        }
        "GetResourcePolicy" => {
            let arn_text = body_text(&body, "ResourceArn");
            match with_scope(&slot, req, |st| {
                st.resource_policies.get(&arn_text).cloned()
            }) {
                Some(policy) => Ok(ok_json(
                    json!({ "ResourceArn": arn_text, "ResourcePolicy": policy }),
                )),
                None => Err(err(
                    400,
                    "ResourceNotFoundException",
                    format!("The resource with ARN {arn_text} was not found."),
                )),
            }
        }
        "DeleteResourcePolicy" => {
            let arn_text = body_text(&body, "ResourceArn");
            match with_scope(&slot, req, |st| {
                st.resource_policies.remove(&arn_text).is_some()
            }) {
                true => Ok(ok_json(json!({}))),
                false => Err(err(
                    400,
                    "ResourceNotFoundException",
                    format!("The resource with ARN {arn_text} was not found."),
                )),
            }
        }
        // --- Dashboards -------------------------------------------------------------------
        "CreateDashboard" => create_dashboard(&slot, req, &body),
        "GetDashboard" => get_dashboard(&slot, req, &body),
        "ListDashboards" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.dashboards
                    .iter()
                    .map(|d| json!({ "DashboardArn": d.arn, "Type": d.kind }))
                    .collect()
            });
            Ok(Response::json_status(200, &json!({ "Dashboards": list })))
        }
        "UpdateDashboard" => update_dashboard(&slot, req, &body),
        "DeleteDashboard" => {
            let id = body_text(&body, "DashboardId");
            let found_idx = with_scope(&slot, req, |st| {
                st.dashboards
                    .iter()
                    .position(|d| d.name == id || d.arn == id)
            });
            match found_idx {
                Some(idx) => {
                    with_scope(&slot, req, |st| {
                        st.dashboards.remove(idx);
                    });
                    Ok(ok_json(json!({})))
                }
                None => Err(not_found_resource(id)),
            }
        }
        "StartDashboardRefresh" => {
            let id = body_text(&body, "DashboardId");
            ensure_dashboard(&slot, req, &id)?;
            Ok(ok_json(
                json!({ "RefreshId": uuid::Uuid::new_v4().to_string() }),
            ))
        }
        // --- Event configuration ------------------------------------------------------------
        "PutEventConfiguration" => {
            let key = body
                .get("EventDataStore")
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| {
                    body.get("TrailName")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .unwrap_or_default();
            let mut config = json!({});
            if let Some(ac) = body.get("AggregationConfigurations") {
                config["AggregationConfigurations"] = ac.clone();
            } else {
                config["AggregationConfigurations"] = json!([]);
            }
            if let Some(ck) = body.get("ContextKeySelectors") {
                config["ContextKeySelectors"] = ck.clone();
            } else {
                config["ContextKeySelectors"] = json!([]);
            }
            if let Some(size) = body.get("MaxEventSize") {
                config["MaxEventSize"] = size.clone();
            }
            config["EventDataStoreArn"] = if key.contains("eventdatastore") {
                Value::String(key.clone())
            } else {
                Value::Null
            };
            config["TrailARN"] = if key.contains("trail") {
                Value::String(key.clone())
            } else {
                Value::Null
            };
            with_scope(&slot, req, |st| {
                st.event_configurations.insert(key, config.clone());
            });
            Ok(Response::json_status(200, &config))
        }
        "GetEventConfiguration" => {
            let eds = body.get("EventDataStore").and_then(|v| v.as_str());
            let trail = body.get("TrailName").and_then(|v| v.as_str());
            let config = with_scope(&slot, req, |st| {
                let key = match (eds, trail) {
                    (Some(k), _) | (None, Some(k)) => k,
                    (None, None) => return json!({}),
                };
                st.event_configurations
                    .get(key)
                    .cloned()
                    .unwrap_or(json!({}))
            });
            Ok(Response::json_status(200, &config))
        }
        // --- Stub listings mirored from moto ----------------------------------------------
        "LookupEvents" => Ok(Response::json_status(
            200,
            &json!({ "Events": [], "NextToken": Value::Null }),
        )),
        "ListPublicKeys" => Ok(Response::json_status(
            200,
            &json!({ "PublicKeyList": [], "NextToken": Value::Null }),
        )),
        "GenerateQuery" => {
            let eds_list = body
                .get("EventDataStores")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|f| f.as_str())
                .unwrap_or("default");
            Ok(ok_json(json!({
                "QueryStatement": format!("SELECT eventTime, eventName FROM {eds_list} LIMIT 100"),
                "QueryAlias": "generated-query",
            })))
        }
        "SearchSampleQueries" => Ok(Response::json_status(
            200,
            &json!({ "SearchResults": [], "NextToken": Value::Null }),
        )),
        "ListInsightsData" => Ok(Response::json_status(200, &json!({ "Events": [] }))),
        "ListInsightsMetricData" => {
            let event_source = body_text_opt(&body, "EventSource").unwrap_or_default();
            let event_name = body_text_opt(&body, "EventName").unwrap_or_default();
            let insight_type =
                body_text_opt(&body, "InsightType").unwrap_or_else(|| "ApiCallRateInsight".into());
            let error_code = body_text_opt(&body, "ErrorCode").unwrap_or_else(|| "Success".into());
            Ok(Response::json_status(
                200,
                &json!({
                    "Timestamps": [],
                    "Values": [],
                    "EventSource": event_source,
                    "EventName": event_name,
                    "InsightType": insight_type,
                    "ErrorCode": error_code,
                    "NextToken": Value::Null,
                }),
            ))
        }
        "StartEventDataStoreIngestion" | "StopEventDataStoreIngestion" => {
            ensure_eds(&slot, req, &body)?;
            let new_status = if req.operation == "StartEventDataStoreIngestion" {
                "ENABLED"
            } else {
                "STOPPED_INGESTION"
            };
            with_scope(&slot, req, |st| {
                let arn_text = body_text(&body, "EventDataStore");
                if let Some(e) = find_eds(st, &arn_text) {
                    e.status = new_status;
                }
            });
            Ok(ok_json(json!({})))
        }
        _ => Err(err(
            400,
            "UnknownOperationException",
            format!("The {} action is not implemented", req.operation),
        )),
    }
}

fn ok_json(value: Value) -> Response {
    Response::json_status(200, &value)
}

fn not_found_resource(id: String) -> Response {
    err(
        400,
        "ResourceNotFoundException",
        format!("The resource {id} was not found."),
    )
}

fn body_text(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(|v| {
            v.as_str().map(String::from).or_else(|| match v {
                Value::Null => None,
                other => Some(other.to_string()),
            })
        })
        .unwrap_or_default()
}

fn body_text_opt(body: &Value, key: &str) -> Option<String> {
    body.get(key)
        .and_then(|v| v.as_str().map(String::from))
        .filter(|s| !s.is_empty())
}

fn tag_pairs(body: &Value, key: &str) -> Vec<(String, String)> {
    body.get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    let k = t.get("Key")?.as_str()?;
                    let v = t.get("Value").and_then(|v| v.as_str()).unwrap_or_default();
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn find_eds<'a>(st: &'a mut CloudTrailState, arn_or_name: &str) -> Option<&'a mut Eds> {
    st.eds
        .iter_mut()
        .find(|e| e.arn == arn_or_name || e.name == arn_or_name || e.arn.ends_with(arn_or_name))
}

fn eds_not_found(arn: &str) -> Response {
    err(
        400,
        "EventDataStoreNotFoundException",
        format!("The event data store {arn} was not found."),
    )
}

fn ensure_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    arn: &Value,
) -> Result<(), Response> {
    let arn_text = body_text(arn, "EventDataStore");
    let exists = with_scope(slot, req, |st| {
        st.eds
            .iter()
            .any(|e| e.arn == arn_text || e.name == arn_text || e.arn.ends_with(&arn_text))
    });
    if exists {
        Ok(())
    } else {
        Err(eds_not_found(&arn_text))
    }
}

fn ensure_dashboard(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    id: &str,
) -> Result<(), Response> {
    let exists = with_scope(slot, req, |st| {
        st.dashboards.iter().any(|d| d.name == id || d.arn == id)
    });
    if exists {
        Ok(())
    } else {
        Err(not_found_resource(id.to_string()))
    }
}

// --- trail ops ------------------------------------------------------------------------

fn create_trail(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let bucket = body_text(body, "S3BucketName");
    // moto's Trail.__init__ name checks + S3 bucket existence probe.
    if name.len() < 3 {
        return Err(err(
            400,
            "InvalidTrailNameException",
            "Trail name too short. Minimum allowed length: 3 characters.".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(err(400, "InvalidTrailNameException", format!("Trail name too long. Maximum allowed length: 128 characters. Specified name length: {} characters.", name.len())));
    }
    if !s3::bridge::bucket_exists(req.account, &req.region, &bucket) {
        return Err(err(
            400,
            "S3BucketDoesNotExistException",
            format!("S3 bucket {bucket} does not exist!"),
        ));
    }
    let is_global = body
        .get("IncludeGlobalServiceEvents")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let is_multi_region = body
        .get("IsMultiRegionTrail")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !is_global && is_multi_region {
        return Err(err(
            400,
            "InvalidParameterCombinationException",
            "Multi-Region trail must include global service events.".to_string(),
        ));
    }
    let tags_list = body.get("TagsList").cloned().unwrap_or(json!([]));
    let trail = Trail {
        name: name.clone(),
        region: req.region.clone(),
        bucket_name: bucket.clone(),
        s3_key_prefix: body_text_opt(body, "S3KeyPrefix"),
        sns_topic_name: body_text_opt(body, "SnsTopicName"),
        is_global,
        is_multi_region,
        log_validation: body
            .get("EnableLogFileValidation")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        is_org_trail: body
            .get("IsOrganizationTrail")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        cw_log_group_arn: body_text_opt(body, "CloudWatchLogsLogGroupArn"),
        cw_role_arn: body_text_opt(body, "CloudWatchLogsRoleArn"),
        kms_key_id: body_text_opt(body, "KmsKeyId"),
        is_logging: false,
        started_epoch: None,
        stopped_epoch: None,
        event_selectors: None,
        advanced_event_selectors: None,
        insight_selectors: None,
    };
    let description = trail_description(&trail, req.account, false);
    let arn = trail_arn(&trail.region, req.account, &name);
    with_scope(slot, req, |st| {
        st.trails.push(trail);
        if let Some(arr) = tags_list.as_array() {
            let entry = st.audit.tags.entry(arn.clone()).or_default();
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    entry.insert(k.to_string(), v.to_string());
                }
            }
        }
    });
    Ok(ok_json(description))
}

fn get_trail(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    if name.len() < 3 {
        return Err(err(
            400,
            "InvalidTrailNameException",
            format!("Trail name too short. Minimum allowed length: 3 characters. Specified name length: {} characters.", name.len()),
        ));
    }
    let account = req.account;
    match find_trail(slot, account, &req.region, &name) {
        Some(t) => Ok(ok_json(
            json!({ "Trail": trail_description(&t, account, true) }),
        )),
        None => Err(not_found_trail(account, &name)),
    }
}

fn describe_status(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    name: String,
) -> Result<Response, Response> {
    if name.len() < 3 {
        return Err(err(
            400,
            "InvalidTrailNameException",
            format!("Trail name too short. Minimum allowed length: 3 characters. Specified name length: {} characters.", name.len()),
        ));
    }
    let account = req.account;
    // moto fabricates the ARN in this method's error message.
    match find_trail(slot, account, &req.region, &name) {
        Some(t) => Ok(ok_json(trail_status_description(&t))),
        None => Err(not_found_trail(
            account,
            &trail_arn(&req.region, account, &name),
        )),
    }
}

/// DescribeTrails: shadow-trail visibility across the account's regions plus
/// moto's gateway-applied `trailNameList` filter (name or ARN, missing names
/// are skipped).
fn describe_trails(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let include_shadow = body
        .get("includeShadowTrails")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let name_filter: Option<Vec<String>> = body
        .get("trailNameList")
        .and_then(|v| v.as_array())
        .and_then(|a| {
            a.iter()
                .map(|x| x.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
        });
    let account = req.account;
    let list: Vec<Value> = visible_trails(slot, account, &req.region)
        .into_iter()
        .filter(|t| include_shadow || t.region == req.region)
        .filter(|t| match &name_filter {
            Some(names) => names
                .iter()
                .any(|n| *n == t.name || *n == trail_arn(&t.region, account, &t.name)),
            None => true,
        })
        .map(|t| trail_description(&t, account, true))
        .collect();
    Ok(ok_json(json!({ "trailList": list })))
}

fn list_trails(slot: &store::Slot<CloudTrailState>, req: &Request) -> Result<Response, Response> {
    let account = req.account;
    let shorts: Vec<Value> = visible_trails(slot, account, &req.region)
        .into_iter()
        .map(|t| {
            json!({
                "Name": t.name,
                "TrailARN": trail_arn(&t.region, account, &t.name),
                "HomeRegion": t.region,
            })
        })
        .collect();
    Ok(ok_json(json!({ "Trails": shorts })))
}

fn set_logging(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let start = req.operation == "StartLogging";
    with_scope(slot, req, |st| {
        if let Some(t) = st.trails.iter_mut().find(|t| t.name == name) {
            if start {
                t.is_logging = true;
                if t.started_epoch.is_none() {
                    t.started_epoch = Some(epoch_now());
                }
                t.stopped_epoch = None;
            } else {
                t.is_logging = false;
                t.stopped_epoch = Some(epoch_now());
            }
        }
    });
    Ok(ok_json(json!({})))
}

/// UpdateTrail: mutate fields that were passed (moto's per-field `is not None`).
fn update_trail(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let account = req.account;
    let updated = with_scope(slot, req, |st| {
        if let Some(t) = st
            .trails
            .iter_mut()
            .find(|t| t.name == name || trail_arn(&t.region, account, &t.name) == name)
        {
            if let Some(v) = body.get("S3BucketName").and_then(|v| v.as_str()) {
                t.bucket_name = v.to_string();
            }
            if let Some(v) = body.get("S3KeyPrefix").and_then(|v| v.as_str()) {
                t.s3_key_prefix = Some(v.to_string());
            }
            if let Some(v) = body.get("SnsTopicName").and_then(|v| v.as_str()) {
                t.sns_topic_name = Some(v.to_string());
            }
            if let Some(v) = body
                .get("IncludeGlobalServiceEvents")
                .and_then(|v| v.as_bool())
            {
                t.is_global = v;
            }
            if let Some(v) = body.get("IsMultiRegionTrail").and_then(|v| v.as_bool()) {
                t.is_multi_region = v;
            }
            if let Some(v) = body
                .get("EnableLogFileValidation")
                .and_then(|v| v.as_bool())
            {
                t.log_validation = v;
            }
            if let Some(v) = body.get("IsOrganizationTrail").and_then(|v| v.as_bool()) {
                t.is_org_trail = v;
            }
            if let Some(v) = body
                .get("CloudWatchLogsLogGroupArn")
                .and_then(|v| v.as_str())
            {
                t.cw_log_group_arn = Some(v.to_string());
            }
            if let Some(v) = body.get("CloudWatchLogsRoleArn").and_then(|v| v.as_str()) {
                t.cw_role_arn = Some(v.to_string());
            }
            if let Some(v) = body.get("KmsKeyId").and_then(|v| v.as_str()) {
                t.kms_key_id = Some(v.to_string());
            }
            return Some(trail_description(t, account, true));
        }
        None
    });
    match updated {
        Some(d) => Ok(ok_json(d)),
        None => Err(not_found_trail(account, &name)),
    }
}

fn find_trail_mut<'a>(
    st: &'a mut CloudTrailState,
    account: u64,
    name_or_arn: &str,
) -> Option<&'a mut Trail> {
    st.trails
        .iter_mut()
        .find(|t| t.name == name_or_arn || trail_arn(&t.region, account, &t.name) == name_or_arn)
}
// --- selectors -------------------------------------------------------------------------

fn put_event_selectors(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let trail_name = body_text(body, "TrailName");
    let event_selectors = body.get("EventSelectors").cloned();
    let advanced = body.get("AdvancedEventSelectors").cloned();
    let account = req.account;
    let result = with_scope(slot, req, |st| {
        let t = find_trail_mut(st, account, &trail_name)?;
        if event_selectors.is_some() {
            t.event_selectors = event_selectors.clone();
        } else if let Some(adv) = advanced.clone() {
            t.event_selectors = None;
            t.advanced_event_selectors = Some(adv);
        }
        let arn = trail_arn(&t.region, account, &t.name);
        Some((arn, event_selectors.clone(), advanced.clone()))
    });
    match result {
        Some((arn, ev, adv)) => Ok(ok_json(json!({
            "TrailARN": arn,
            "EventSelectors": ev.unwrap_or(Value::Null),
            "AdvancedEventSelectors": adv,
        }))),
        None => Err(not_found_trail(account, &trail_name)),
    }
}

fn get_event_selectors(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let trail_name = body_text(body, "TrailName");
    let account = req.account;
    let result = with_scope(slot, req, |st| {
        find_trail_mut(st, account, &trail_name).map(|t| {
            (
                trail_arn(&t.region, account, &t.name),
                t.event_selectors.clone().unwrap_or(json!([])),
                t.advanced_event_selectors.clone().unwrap_or(json!([])),
            )
        })
    });
    match result {
        Some((arn, ev, adv)) => Ok(ok_json(json!({
            "TrailARN": arn,
            "EventSelectors": ev,
            "AdvancedEventSelectors": adv,
        }))),
        None => Err(not_found_trail(account, &trail_name)),
    }
}

fn put_insight_selectors(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let trail_name = body_text(body, "TrailName");
    let selectors = body.get("InsightSelectors").cloned().unwrap_or(json!([]));
    let account = req.account;
    let result = with_scope(slot, req, |st| {
        find_trail_mut(st, account, &trail_name).map(|t| {
            t.insight_selectors = Some(selectors.clone());
            (trail_arn(&t.region, account, &t.name), selectors.clone())
        })
    });
    match result {
        Some((arn, sel)) => Ok(ok_json(json!({ "TrailARN": arn, "InsightSelectors": sel }))),
        None => Err(not_found_trail(account, &trail_name)),
    }
}

fn get_insight_selectors(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let trail_name = body_text(body, "TrailName");
    let account = req.account;
    let result = with_scope(slot, req, |st| {
        find_trail_mut(st, account, &trail_name).map(|t| {
            (
                trail_arn(&t.region, account, &t.name),
                t.insight_selectors.clone().unwrap_or(json!([])),
            )
        })
    });
    match result {
        Some((arn, sel)) => Ok(ok_json(json!({ "TrailARN": arn, "InsightSelectors": sel }))),
        None => Err(not_found_trail(account, &trail_name)),
    }
}

// --- event data stores -------------------------------------------------------------------

fn create_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let id = uuid::Uuid::new_v4();
    let arn = format!(
        "arn:aws:cloudtrail:{}:{}:eventdatastore/{}",
        req.region, req.account, id
    );
    let eds = Eds {
        arn: arn.clone(),
        name,
        status: "ENABLED",
        advanced_event_selectors: body
            .get("AdvancedEventSelectors")
            .cloned()
            .unwrap_or(json!({})),
        multi_region_enabled: body
            .get("MultiRegionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        organization_enabled: body
            .get("OrganizationEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        retention_period: body
            .get("RetentionPeriod")
            .and_then(|v| v.as_i64())
            .unwrap_or(2557),
        termination_protection_enabled: body
            .get("TerminationProtectionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        kms_key_id: body_text_opt(body, "KmsKeyId"),
        billing_mode: body
            .get("BillingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("EXTENDABLE_RETENTION_PRICING")
            .to_string(),
        created_epoch: epoch_now(),
        updated_epoch: epoch_now(),
    };
    let desc = eds_description(&eds);
    with_scope(slot, req, |st| {
        st.eds.push(eds);
        if let Some(arr) = body.get("TagsList").and_then(|v| v.as_array()) {
            let entry = st.audit.tags.entry(arn.clone()).or_default();
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    entry.insert(k.to_string(), v.to_string());
                }
            }
        }
    });
    Ok(ok_json(desc))
}

fn get_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "EventDataStore");
    let desc = with_scope(slot, req, |st| {
        find_eds(st, &arn_text).map(|e| eds_description(e))
    });
    desc.map(ok_json).ok_or_else(|| eds_not_found(&arn_text))
}

fn delete_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "EventDataStore");
    let removed = with_scope(slot, req, |st| {
        let pos = st
            .eds
            .iter()
            .position(|e| e.arn == arn_text || e.name == arn_text || e.arn.ends_with(&arn_text));
        match pos {
            Some(i) => {
                st.eds.remove(i);
                true
            }
            None => false,
        }
    });
    if removed {
        Ok(ok_json(json!({})))
    } else {
        Err(eds_not_found(&arn_text))
    }
}

fn update_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "EventDataStore");
    let updated = with_scope(slot, req, |st| {
        let e = find_eds(st, &arn_text)?;
        if let Some(v) = body.get("Name").and_then(|v| v.as_str()) {
            e.name = v.to_string();
        }
        if let Some(v) = body.get("AdvancedEventSelectors") {
            e.advanced_event_selectors = v.clone();
        }
        if let Some(v) = body.get("MultiRegionEnabled").and_then(|v| v.as_bool()) {
            e.multi_region_enabled = v;
        }
        if let Some(v) = body.get("OrganizationEnabled").and_then(|v| v.as_bool()) {
            e.organization_enabled = v;
        }
        if let Some(v) = body.get("RetentionPeriod").and_then(|v| v.as_i64()) {
            e.retention_period = v;
        }
        if let Some(v) = body
            .get("TerminationProtectionEnabled")
            .and_then(|v| v.as_bool())
        {
            e.termination_protection_enabled = v;
        }
        if let Some(v) = body_text_opt(body, "KmsKeyId") {
            e.kms_key_id = Some(v);
        }
        if let Some(v) = body.get("BillingMode").and_then(|v| v.as_str()) {
            e.billing_mode = v.to_string();
        }
        e.updated_epoch = epoch_now();
        Some(eds_description(e))
    });
    match updated {
        Some(d) => Ok(ok_json(d)),
        None => Err(eds_not_found(&arn_text)),
    }
}

fn restore_eds(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "EventDataStore");
    let restored = with_scope(slot, req, |st| {
        let e = find_eds(st, &arn_text)?;
        e.status = "ENABLED";
        e.updated_epoch = epoch_now();
        Some(eds_description(e))
    });
    match restored {
        Some(d) => Ok(ok_json(d)),
        None => Err(eds_not_found(&arn_text)),
    }
}

// --- channels ------------------------------------------------------------------------------

fn create_channel(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let source = body_text(body, "Source");
    let destinations = body.get("Destinations").cloned().unwrap_or(json!([]));
    let arn_text = format!(
        "arn:aws:cloudtrail:{}:{}:channel/{}",
        req.region,
        req.account,
        uuid::Uuid::new_v4()
    );
    let channel = Channel {
        arn: arn_text.clone(),
        name: name.clone(),
        source,
        destinations,
    };
    let desc = channel_description(&channel);
    with_scope(slot, req, |st| {
        st.channels.push(channel);
        if let Some(arr) = body.get("Tags").and_then(|v| v.as_array()) {
            let entry = st.audit.tags.entry(arn_text.clone()).or_default();
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(|x| x.as_str()),
                    t.get("Value").and_then(|x| x.as_str()),
                ) {
                    entry.insert(k.to_string(), v.to_string());
                }
            }
        }
    });
    Ok(ok_json(desc))
}

fn get_channel(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "Channel");
    let desc = with_scope(slot, req, |st| {
        st.channels
            .iter()
            .find(|c| c.arn == arn_text || c.name == arn_text)
            .map(channel_description)
    });
    match desc {
        Some(d) => Ok(ok_json(d)),
        None => Err(err(
            400,
            "ChannelNotFoundException",
            format!("The channel {arn_text} was not found."),
        )),
    }
}

fn update_channel(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let arn_text = body_text(body, "Channel");
    let updated = with_scope(slot, req, |st| {
        if let Some(c) = st.channels.iter_mut().find(|c| c.arn == arn_text) {
            if let Some(v) = body.get("Name").and_then(|v| v.as_str()) {
                c.name = v.to_string();
            }
            if let Some(v) = body.get("Source").and_then(|v| v.as_str()) {
                c.source = v.to_string();
            }
            if let Some(v) = body.get("Destinations") {
                c.destinations = v.clone();
            }
            return Some(channel_description(c));
        }
        None
    });
    match updated {
        Some(d) => Ok(ok_json(d)),
        None => Err(err(
            400,
            "ChannelNotFoundException",
            format!("The channel {arn_text} was not found."),
        )),
    }
}

// --- imports -----------------------------------------------------------------------------

fn start_import(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let import_id = body_text_opt(body, "ImportId");
    let source = body
        .get("ImportSource")
        .and_then(|v| v.get("S3"))
        .and_then(|v| v.get("S3LocationUri"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let destinations = body.get("Destinations").cloned().unwrap_or(json!([]));
    let desc = with_scope(slot, req, |st| {
        if let Some(id) = import_id {
            if let Some(imp) = st.imports.iter_mut().find(|i| i.import_id == id) {
                imp.status = "IN_PROGRESS";
                return Some(import_description(imp));
            }
        }
        let imp = ImportState {
            import_id: uuid::Uuid::new_v4().to_string(),
            source: source.clone(),
            destinations: destinations.clone(),
            status: "IN_PROGRESS",
        };
        let d = import_description(&imp);
        st.imports.push(imp);
        Some(d)
    });
    match desc {
        Some(d) => Ok(ok_json(d)),
        None => Ok(ok_json(json!({}))),
    }
}

// --- dashboards -------------------------------------------------------------------------

fn create_dashboard(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let name = body_text(body, "Name");
    let kind = if name == "AWSCloudTrail-Highlights" {
        "MANAGED"
    } else {
        "CUSTOM"
    };
    let dash = Dashboard {
        arn: format!(
            "arn:aws:cloudtrail:{}:{}:dashboard/{name}",
            req.region, req.account
        ),
        name: name.clone(),
        kind: kind.to_string(),
        widgets: body.get("Widgets").cloned().unwrap_or(json!([])),
        refresh_schedule: body.get("RefreshSchedule").cloned(),
        termination_protection_enabled: body
            .get("TerminationProtectionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        tags_list: body.get("TagsList").cloned().unwrap_or(json!([])),
        created_epoch: epoch_now(),
        updated_epoch: epoch_now(),
    };
    let dict = dashboard_to_dict(&dash);
    with_scope(slot, req, |st| {
        st.dashboards.push(dash);
    });
    Ok(ok_json(dict))
}

fn get_dashboard(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let id = body_text(body, "DashboardId");
    let dict = with_scope(slot, req, |st| {
        st.dashboards
            .iter()
            .find(|d| d.name == id || d.arn == id)
            .map(dashboard_to_dict)
    });
    match dict {
        Some(d) => Ok(ok_json(d)),
        None => Err(not_found_resource(id)),
    }
}

fn update_dashboard(
    slot: &store::Slot<CloudTrailState>,
    req: &Request,
    body: &Value,
) -> Result<Response, Response> {
    let id = body_text(body, "DashboardId");
    let updated = with_scope(slot, req, |st| {
        let d = st
            .dashboards
            .iter_mut()
            .find(|d| d.name == id || d.arn == id)?;
        if let Some(v) = body.get("Widgets") {
            d.widgets = v.clone();
        }
        if let Some(v) = body.get("RefreshSchedule") {
            d.refresh_schedule = Some(v.clone());
        }
        if let Some(v) = body
            .get("TerminationProtectionEnabled")
            .and_then(|v| v.as_bool())
        {
            d.termination_protection_enabled = v;
        }
        d.updated_epoch = epoch_now();
        Some(dashboard_to_dict(d))
    });
    match updated {
        Some(d) => Ok(ok_json(d)),
        None => Err(not_found_resource(id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    struct Ctx(u64);

    impl Ctx {
        fn req(&self, op: &str, body: Value) -> Request {
            Request {
                service: "cloudtrail".into(),
                operation: op.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "x-amz-target".to_string(),
                    format!("com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.{op}"),
                )]),
                body: Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    fn fresh_ctx() -> Ctx {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(131111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn bucket(ctx: &Ctx) -> String {
        format!("{}-ct-bkt", ctx.0)
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Register a bucket in the s3 bridge so CreateTrail's existence check passes.
    fn register_bucket(account: u64, bucket: &str) {
        let state = s3::models::S3State::new();
        state.put_bucket(std::sync::Arc::new(s3::models::Bucket::new(
            bucket.to_string(),
            "us-east-1".to_string(),
        )));
        s3::bridge::register_state(account, "us-east-1", &state);
    }

    fn create_trail(ctx: &Ctx, name: &str) -> Value {
        let bucket = bucket(ctx);
        register_bucket(ctx.0, &bucket);
        let resp = dispatch(&ctx.req(
            "CreateTrail",
            json!({ "Name": name, "S3BucketName": bucket }),
        ));
        assert_eq!(resp.status, 200, "create_trail failed: {:?}", resp.body);
        parsed(&resp)
    }

    #[test]
    fn trail_lifecycle_from_create_to_delete() {
        let ctx = fresh_ctx();
        let created = create_trail(&ctx, "wt-trail");
        assert_eq!(created["Name"], "wt-trail");
        assert!(created["TrailARN"]
            .as_str()
            .unwrap()
            .ends_with("trail/wt-trail"));

        let got = parsed(&dispatch(
            &ctx.req("GetTrail", json!({ "Name": "wt-trail" })),
        ));
        assert_eq!(got["Trail"]["Name"], "wt-trail");
        assert!(got["Trail"]["HomeRegion"].as_str().is_some());

        // Start/stop logging flips IsLogging.
        dispatch(&ctx.req("StartLogging", json!({ "Name": "wt-trail" })));
        let st = parsed(&dispatch(
            &ctx.req("GetTrailStatus", json!({ "Name": "wt-trail" })),
        ));
        assert_eq!(st["IsLogging"], true);
        dispatch(&ctx.req("StopLogging", json!({ "Name": "wt-trail" })));
        let st = parsed(&dispatch(
            &ctx.req("GetTrailStatus", json!({ "Name": "wt-trail" })),
        ));
        assert_eq!(st["IsLogging"], false);

        dispatch(&ctx.req("DeleteTrail", json!({ "Name": "wt-trail" })));
        let gone = dispatch(&ctx.req("GetTrail", json!({ "Name": "wt-trail" })));
        assert_eq!(gone.status, 400);
        let e = parsed(&gone);
        assert_eq!(e["__type"], "TrailNotFoundException");
    }

    #[test]
    fn create_trail_requires_s3_bucket() {
        let ctx = fresh_ctx();
        let resp = dispatch(&ctx.req(
            "CreateTrail",
            json!({ "Name": "no-bucket-trail", "S3BucketName": "definitely-not-there" }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(parsed(&resp)["__type"], "S3BucketDoesNotExistException");
    }

    #[test]
    fn update_trail_mutates_passed_fields_only() {
        let ctx = fresh_ctx();
        create_trail(&ctx, "wt-up");
        let upd = parsed(&dispatch(&ctx.req(
            "UpdateTrail",
            json!({ "Name": "wt-up", "EnableLogFileValidation": true, "IsMultiRegionTrail": true }),
        )));
        assert_eq!(upd["Name"], "wt-up");
        assert_eq!(upd["LogFileValidationEnabled"], true);
        assert_eq!(upd["IsMultiRegionTrail"], true);

        let got = parsed(&dispatch(&ctx.req("GetTrail", json!({ "Name": "wt-up" }))));
        assert_eq!(got["Trail"]["LogFileValidationEnabled"], true);
    }

    #[test]
    fn describe_and_list_trails() {
        let ctx = fresh_ctx();
        create_trail(&ctx, "dt-a");
        create_trail(&ctx, "dt-b");

        let all = parsed(&dispatch(&ctx.req("DescribeTrails", json!({}))));
        assert_eq!(all["trailList"].as_array().unwrap().len(), 2);

        let filtered = parsed(&dispatch(
            &ctx.req("DescribeTrails", json!({ "trailNameList": ["dt-a"] })),
        ));
        let names: Vec<&str> = filtered["trailList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["dt-a"]);

        let empty = parsed(&dispatch(&ctx.req(
            "DescribeTrails",
            json!({ "trailNameList": ["never-existed"] }),
        )));
        assert_eq!(empty["trailList"], json!([]));

        let listed = parsed(&dispatch(&ctx.req("ListTrails", json!({}))));
        let trails = listed["Trails"].as_array().unwrap();
        assert!(trails.iter().all(|t| { t["HomeRegion"] == "us-east-1" }));
        assert_eq!(trails.len(), 2);
    }

    #[test]
    fn event_and_insight_selectors_round_trip() {
        let ctx = fresh_ctx();
        create_trail(&ctx, "wt-sel");
        let put = parsed(&dispatch(&ctx.req(
            "PutEventSelectors",
            json!({
                "TrailName": "wt-sel",
                "EventSelectors": [{"ReadWriteType": "All", "IncludeManagementEvents": true}],
            }),
        )));
        assert_eq!(put["EventSelectors"][0]["ReadWriteType"], "All");

        let got = parsed(&dispatch(
            &ctx.req("GetEventSelectors", json!({ "TrailName": "wt-sel" })),
        ));
        assert_eq!(got["EventSelectors"].as_array().unwrap().len(), 1);

        let put_i = parsed(&dispatch(&ctx.req(
            "PutInsightSelectors",
            json!({
                "TrailName": "wt-sel",
                "InsightSelectors": [{"InsightType": "ApiCallRateInsight"}],
            }),
        )));
        assert_eq!(
            put_i["InsightSelectors"][0]["InsightType"],
            "ApiCallRateInsight"
        );
        let got_i = parsed(&dispatch(
            &ctx.req("GetInsightSelectors", json!({ "TrailName": "wt-sel" })),
        ));
        assert_eq!(
            got_i["InsightSelectors"][0]["InsightType"],
            "ApiCallRateInsight"
        );
    }

    #[test]
    fn tag_crud_on_trail_arn() {
        let ctx = fresh_ctx();
        let creation = create_trail(&ctx, "wt-tag");
        let arn = creation["TrailARN"].as_str().unwrap().to_string();

        dispatch(&ctx.req(
            "AddTags",
            json!({
                "ResourceId": arn,
                "TagsList": [{"Key": "env", "Value": "test"}],
            }),
        ));
        let tags = parsed(&dispatch(
            &ctx.req("ListTags", json!({ "ResourceIdList": [arn] })),
        ));
        assert_eq!(tags["ResourceTagList"][0]["TagsList"][0]["Key"], "env");

        dispatch(&ctx.req(
            "RemoveTags",
            json!({
                "ResourceId": arn,
                "TagsList": [{"Key": "env", "Value": "test"}],
            }),
        ));
        let tags = parsed(&dispatch(
            &ctx.req("ListTags", json!({ "ResourceIdList": [arn] })),
        ));
        assert_eq!(tags["ResourceTagList"][0]["TagsList"], json!([]));
    }

    #[test]
    fn eds_lifecycle() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "CreateEventDataStore",
            json!({ "Name": "eds-a", "RetentionPeriod": 90 }),
        )));
        assert_eq!(created["Status"], "ENABLED");
        assert_eq!(created["RetentionPeriod"], 90);
        let arn = created["EventDataStoreArn"].as_str().unwrap().to_string();

        let got = parsed(&dispatch(
            &ctx.req("GetEventDataStore", json!({ "EventDataStore": arn })),
        ));
        assert_eq!(got["Name"], "eds-a");

        let upd = parsed(&dispatch(&ctx.req(
            "UpdateEventDataStore",
            json!({ "EventDataStore": arn, "Name": "eds-a2", "RetentionPeriod": 180 }),
        )));
        assert_eq!(upd["Name"], "eds-a2");
        assert_eq!(upd["RetentionPeriod"], 180);

        dispatch(&ctx.req("DeleteEventDataStore", json!({ "EventDataStore": arn })));
        let gone = dispatch(&ctx.req("GetEventDataStore", json!({ "EventDataStore": arn })));
        assert_eq!(parsed(&gone)["__type"], "EventDataStoreNotFoundException");

        let missing = dispatch(&ctx.req(
            "RestoreEventDataStore",
            json!({ "EventDataStore": "arn:aws:cloudtrail:us-east-1:1:eventdatastore/none" }),
        ));
        assert_eq!(
            parsed(&missing)["__type"],
            "EventDataStoreNotFoundException"
        );
    }

    #[test]
    fn channel_lifecycle_and_not_found() {
        let ctx = fresh_ctx();
        let ch = parsed(&dispatch(&ctx.req(
            "CreateChannel",
            json!({
                "Name": "chan",
                "Source": "Custom",
                "Destinations": [{"Type": "EVENT_DATA_STORE", "Location": "arn:fake"}],
            }),
        )));
        let arn = ch["ChannelArn"].as_str().unwrap().to_string();

        let listed = parsed(&dispatch(&ctx.req("ListChannels", json!({}))));
        assert!(listed["Channels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["ChannelArn"] == arn));

        let got = parsed(&dispatch(&ctx.req("GetChannel", json!({ "Channel": arn }))));
        assert_eq!(got["Name"], "chan");
        assert_eq!(got["Source"], "Custom");

        let missing = dispatch(&ctx.req("GetChannel", json!({ "Channel": "arn:fake" })));
        assert_eq!(parsed(&missing)["__type"], "ChannelNotFoundException");

        dispatch(&ctx.req("DeleteChannel", json!({ "Channel": arn })));
        let del = dispatch(&ctx.req("GetChannel", json!({ "Channel": arn })));
        assert_eq!(parsed(&del)["__type"], "ChannelNotFoundException");
    }

    #[test]
    fn query_flow() {
        let ctx = fresh_ctx();
        let eds = parsed(&dispatch(
            &ctx.req("CreateEventDataStore", json!({ "Name": "eds-q" })),
        ));
        let eds_arn = eds["EventDataStoreArn"].as_str().unwrap().to_string();

        let q = parsed(&dispatch(&ctx.req(
            "StartQuery",
            json!({ "QueryStatement": format!("SELECT * FROM {} LIMIT 1", eds_arn.rsplit('/').next().unwrap()) }),
        )));
        let qid = q["QueryId"].as_str().unwrap().to_string();

        let desc = parsed(&dispatch(
            &ctx.req("DescribeQuery", json!({ "QueryId": qid })),
        ));
        assert_eq!(desc["QueryId"], qid);
        assert_eq!(desc["QueryStatus"], "COMPLETED");

        let results = parsed(&dispatch(
            &ctx.req("GetQueryResults", json!({ "QueryId": qid })),
        ));
        assert!(results["QueryStatistics"]["ResultsCount"].is_number());

        let listed = parsed(&dispatch(
            &ctx.req("ListQueries", json!({ "EventDataStore": eds_arn })),
        ));
        assert!(!listed["Queries"].as_array().unwrap().is_empty());

        let cancelled = parsed(&dispatch(
            &ctx.req("CancelQuery", json!({ "QueryId": qid })),
        ));
        assert_eq!(cancelled["QueryStatus"], "CANCELLED");
    }

    #[test]
    fn dashboard_and_resource_policy() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(
            &ctx.req("CreateDashboard", json!({ "Name": "dash-1" })),
        ));
        assert_eq!(created["Widgets"], json!([]));
        assert!(created["DashboardArn"].as_str().is_some());

        let got = parsed(&dispatch(
            &ctx.req("GetDashboard", json!({ "DashboardId": "dash-1" })),
        ));
        assert_eq!(got["Name"], "dash-1");

        dispatch(&ctx.req("DeleteDashboard", json!({ "DashboardId": "dash-1" })));
        let missing = dispatch(&ctx.req("GetDashboard", json!({ "DashboardId": "dash-1" })));
        assert_eq!(parsed(&missing)["__type"], "ResourceNotFoundException");

        let put = parsed(&dispatch(&ctx.req(
            "PutResourcePolicy",
            json!({ "ResourceArn": "arn:aws:cloudtrail:us-east-1:1:trail/t", "ResourcePolicy": "{}" }),
        )));
        assert!(put["ResourceArn"].as_str().is_some());
        let get = parsed(&dispatch(&ctx.req(
            "GetResourcePolicy",
            json!({ "ResourceArn": "arn:aws:cloudtrail:us-east-1:1:trail/t" }),
        )));
        assert_eq!(get["ResourcePolicy"], "{}");
        dispatch(&ctx.req(
            "DeleteResourcePolicy",
            json!({ "ResourceArn": "arn:aws:cloudtrail:us-east-1:1:trail/t" }),
        ));
        let gone = dispatch(&ctx.req(
            "GetResourcePolicy",
            json!({ "ResourceArn": "arn:aws:cloudtrail:us-east-1:1:trail/t" }),
        ));
        assert_eq!(gone.status, 400);
        assert_eq!(parsed(&gone)["__type"], "ResourceNotFoundException");
    }
}
