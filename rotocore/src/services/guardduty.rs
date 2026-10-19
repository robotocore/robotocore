//! AWS GuardDuty backend — port of moto's `guardduty` engine.
//!
//! Rest-json with camelCase request members and lowercase camelCase response
//! members. Detectors embed filters, ip sets, threat-intel + entity sets,
//! publishing destinations, members and sample findings (moto's shapes);
//! org admins, invitations, malware plans/scans live at the top level. Tags
//! ride /tags/{arn} and resolve through moto's `_get_resource_by_arn` walk
//! (detector arn + nested set arns).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<GdState>> = Lazy::new(|| store::slot::<GdState>("guardduty"));

fn now_stamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%fZ")
        .to_string()
}

fn new_hex32() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..32].to_string()
}

fn null_if_absent(v: Option<Value>) -> Option<Value> {
    v.filter(|x| !x.is_null())
}

#[derive(Default)]
struct GdState {
    detectors: HashMap<String, Detector>,
    admin_account_ids: Vec<String>,
    malware_plans: HashMap<String, Value>,
    region: String,
    account: String,
}

struct Detector {
    arn: String,
    service_role: String,
    created_at: String,
    updated_at: String,
    finding_publish_freq: String,
    enabled: bool,
    datasources: Value,
    tags: Map<String, Value>,
    features: Value,
    filters: HashMap<String, Filter>,
    ip_sets: HashMap<String, Filter>,
    threat_intel_sets: HashMap<String, Filter>,
    threat_entity_sets: HashMap<String, Filter>,
    trusted_entity_sets: HashMap<String, Filter>,
    publishing_destinations: HashMap<String, Value>,
    members: HashMap<String, Value>,
    findings: HashMap<String, Value>,
    administrator_id: Option<String>,
}

/// moto's Filter doubles as the set-record shape: plain filters render
/// {name, action, description, findingCriteria, rank} while resource sets
/// render {name, format, location, status, tags} with their own ARN + tags.
#[derive(Clone, Default)]
struct Filter {
    name: String,
    action: Value,
    description: Value,
    finding_criteria: Value,
    rank: Value,
    arn: Option<String>,
    tags_map: Map<String, Value>,
    entity_format: Option<Value>,
    location: Option<Value>,
    status: Option<Value>,
}

impl Filter {
    fn set_to_json(&self) -> Value {
        json!({
            "name": self.name,
            "format": self.entity_format.clone().unwrap_or(json!("")),
            "location": self.location.clone().unwrap_or(json!("")),
            "status": self.status.clone().unwrap_or(json!("INACTIVE")),
            "tags": Value::Object(self.tags_map.clone()),
        })
    }

    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "action": self.action,
            "description": self.description,
            "findingCriteria": self.finding_criteria,
            "rank": self.rank,
        })
    }
}

impl Detector {
    fn set_record(
        id: &str,
        set_id: &str,
        kind: &str,
        params: &Value,
        region: &str,
        account: &str,
    ) -> Filter {
        let arn = format!("arn:aws:guardduty:{region}:{account}:detector/{id}/{kind}/{set_id}");
        Filter {
            name: params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            action: json!(null),
            description: json!(String::new()),
            finding_criteria: json!({}),
            rank: json!(1),
            arn: Some(arn),
            tags_map: params
                .get("tags")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            entity_format: params.get("format").cloned(),
            location: params.get("location").cloned(),
            status: Some(json!(if params
                .get("activate")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "ACTIVE"
            } else {
                "INACTIVE"
            })),
        }
    }

    fn to_json(&self) -> Value {
        let enabled_sub = |key: &str, sub: &str| -> &'static str {
            if self
                .datasources
                .get(key)
                .and_then(|v| v.get(sub))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "ENABLED"
            } else {
                "DISABLED"
            }
        };
        json!({
            "createdAt": self.created_at,
            "findingPublishingFrequency": self.finding_publish_freq,
            "serviceRole": self.service_role,
            "status": if self.enabled { "ENABLED" } else { "DISABLED" },
            "updatedAt": self.updated_at,
            "dataSources": {
                "cloudTrail": {"status": "DISABLED"},
                "dnsLogs": {"status": "DISABLED"},
                "flowLogs": {"status": "DISABLED"},
                "s3Logs": {"status": enabled_sub("s3Logs", "enable")},
                "kubernetes": {"auditLogs": {"status": enabled_sub("kubernetes", "auditLogs")}},
            },
            "tags": Value::Object(self.tags.clone()),
            "features": self.features,
        })
    }
}

fn set_exit<'a>(detector: &'a Detector, kind: &str) -> Option<&'a HashMap<String, Filter>> {
    match kind {
        "ipset" => Some(&detector.ip_sets),
        "threatintelset" => Some(&detector.threat_intel_sets),
        "threatentityset" => Some(&detector.threat_entity_sets),
        "trustedentityset" => Some(&detector.trusted_entity_sets),
        _ => None,
    }
}

fn set_enter<'a>(
    detector: &'a mut Detector,
    kind: &str,
) -> Option<&'a mut HashMap<String, Filter>> {
    match kind {
        "ipset" => Some(&mut detector.ip_sets),
        "threatintelset" => Some(&mut detector.threat_intel_sets),
        "threatentityset" => Some(&mut detector.threat_entity_sets),
        "trustedentityset" => Some(&mut detector.trusted_entity_sets),
        _ => None,
    }
}

fn set_id_members(detector: &Detector, kind: &str) -> Vec<String> {
    match kind {
        "ipset" => detector.ip_sets.keys().cloned().collect(),
        "threatintelset" => detector.threat_intel_sets.keys().cloned().collect(),
        "threatentityset" => detector.threat_entity_sets.keys().cloned().collect(),
        _ => detector.trusted_entity_sets.keys().cloned().collect(),
    }
}

fn set_id_key(kind: &str) -> &'static str {
    match kind {
        "ipset" => "ipSetId",
        "threatintelset" => "threatIntelSetId",
        "threatentityset" => "threatEntitySetId",
        _ => "trustedEntitySetId",
    }
}

fn set_ids_key(kind: &str) -> &'static str {
    match kind {
        "ipset" => "ipSetIds",
        "threatintelset" => "threatIntelSetIds",
        "threatentityset" => "threatEntitySetIds",
        _ => "trustedEntitySetIds",
    }
}

fn account_ids_from(params: &Value) -> Vec<String> {
    // helper defined below

    params
        .get("accountIds")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn detector_not_found() -> Response {
    crate::proto::json_error(
        400,
        "BadRequestException",
        "The request is rejected because no detector exists for the account.",
    )
}

fn filter_not_found(name: &str) -> Response {
    crate::proto::json_error(
        400,
        "BadRequestException",
        &format!("The filter {name} does not exist"),
    )
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }

    let method = req.method.as_str();
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    let segments: Vec<&str> = path.split('/').collect();

    // /tags/{arn} REST tagging
    if segments.len() >= 2 && segments[0] == "tags" {
        let arn = crate::proto::url_decode(&path["tags/".len()..]);
        return match method {
            "GET" => st.list_tags_for_resource(&arn),
            "POST" => st.tag_resource(&arn, &params),
            "DELETE" => {
                let keys: Vec<String> = crate::proto::query_map(&req.query_string)
                    .get("tagKeys")
                    .cloned()
                    .unwrap_or_default();
                st.untag_resource(&arn, &keys)
            }
            _ => unimplemented(req),
        };
    }

    // Per-set GET/POST/DELETE bodies for the four nested set kinds.
    let is_set_route = matches!(
        segments.as_slice(),
        [
            "detector",
            _,
            "ipset" | "threatintelset" | "threatentityset" | "trustedentityset",
            _
        ]
    );
    if is_set_route {
        let kind = segments[2];
        let id = segments[1];
        let set_id = segments[3];
        return match method {
            "GET" => st.get_set(id, set_id, kind),
            "POST" => st.update_set(id, set_id, kind, &params),
            "DELETE" => st.delete_set(id, set_id, kind),
            _ => unimplemented(req),
        };
    }

    match segments.as_slice() {
        ["detector"] => match method {
            "POST" => st.create_detector(&params),
            "GET" => st.list_detectors(),
            _ => unimplemented(req),
        },
        ["detector", id] => match method {
            "GET" => st.get_detector(id),
            "POST" => st.update_detector(id, &params),
            "DELETE" => st.delete_detector(id),
            _ => unimplemented(req),
        },
        ["detector", id, "filter"] => match method {
            "POST" => st.create_filter(id, &params),
            "GET" => st.list_filters(id),
            _ => unimplemented(req),
        },
        ["detector", id, "filter", name] => match method {
            "GET" => st.get_filter(id, name),
            "POST" => st.update_filter(id, name, &params),
            "DELETE" => st.delete_filter(id, name),
            _ => unimplemented(req),
        },
        ["detector", id, "ipset"] => match method {
            "POST" => st.create_set(id, &params, "ipset"),
            "GET" => st.list_set_ids(id, "ipset"),
            _ => unimplemented(req),
        },
        ["detector", id, "threatintelset"] => match method {
            "POST" => st.create_set(id, &params, "threatintelset"),
            "GET" => st.list_set_ids(id, "threatintelset"),
            _ => unimplemented(req),
        },
        ["detector", id, "threatentityset"] => match method {
            "POST" => st.create_set(id, &params, "threatentityset"),
            "GET" => st.list_set_ids(id, "threatentityset"),
            _ => unimplemented(req),
        },
        ["detector", id, "trustedentityset"] => match method {
            "POST" => st.create_set(id, &params, "trustedentityset"),
            "GET" => st.list_set_ids(id, "trustedentityset"),
            _ => unimplemented(req),
        },
        ["detector", id, "findings"] => match method {
            "POST" => st.list_findings(id),
            _ => unimplemented(req),
        },
        ["detector", id, "findings", "create"] => st.create_sample_findings(id, &params),
        ["detector", id, "findings", "get"] => st.get_findings(id, &params),
        ["detector", id, "findings", "statistics"] => st.findings_statistics(id),
        ["detector", id, "findings", "archive"] => st.archive_findings(id, &params, true),
        ["detector", id, "findings", "unarchive"] => st.archive_findings(id, &params, false),
        ["detector", id, "findings", "feedback"] => st.update_findings_feedback(id, &params),
        ["detector", id, "master"] => st.get_master_or_admin(id, "master"),
        ["detector", id, "administrator"] => st.get_master_or_admin(id, "administrator"),
        ["detector", id, "master", "disassociate"]
        | ["detector", id, "administrator", "disassociate"] => st.disassociate_from_admin(id),
        ["detector", id, "member"] => match method {
            "POST" => st.create_members(id, &params),
            "GET" => st.list_members(id),
            _ => unimplemented(req),
        },
        ["detector", id, "member", "get"] => st.get_members(id, &params),
        ["detector", id, "member", "delete"] => st.delete_members(id, &params),
        ["detector", id, "member", "start"] => st.toggle_members(id, &params, "ENABLED"),
        ["detector", id, "member", "stop"] => st.toggle_members(id, &params, "DISABLED"),
        ["detector", id, "member", "invite"] => st.toggle_members(id, &params, "INVITED"),
        ["detector", id, "member", "disassociate"] => st.toggle_members(id, &params, "REMOVED"),
        ["detector", id, "member", "detector", "get"] => st.get_member_detectors(id, &params),
        ["detector", id, "member", "detector", "update"] => st.unprocessed_ok(id),
        ["detector", id, "freeTrial", "daysRemaining"] => st.free_trial_days(id, &params),
        ["detector", id, "usage", "statistics"] => st.usage_statistics(id),
        ["detector", id, "malware-scans"] => st.malware_scans(id),
        ["detector", id, "malware-scan-settings"] => match method {
            "GET" => st.malware_scan_settings(id),
            "POST" => st.ok(id),
            _ => unimplemented(req),
        },
        ["detector", id, "publishingDestination"] => match method {
            "POST" => st.create_publishing_destination(id, &params),
            "GET" => st.list_publishing_destinations(id),
            _ => unimplemented(req),
        },
        ["detector", id, "publishingDestination", dest_id] => match method {
            "GET" => st.describe_publishing_destination(id, dest_id),
            "POST" => st.update_publishing_destination(id, dest_id, &params),
            "DELETE" => st.delete_publishing_destination(id, dest_id),
            _ => unimplemented(req),
        },
        ["detector", id, "coverage"] => st.coverage_list(id),
        ["detector", id, "coverage", "statistics"] => st.coverage_statistics(id),
        ["detector", id, "admin"] => match method {
            "GET" => st.organization_configuration(id),
            "POST" => st.ok(id),
            _ => unimplemented(req),
        },
        ["malware-protection-plan"] => match method {
            "POST" => st.create_malware_protection_plan(&params),
            "GET" => st.list_malware_protection_plans(),
            _ => unimplemented(req),
        },
        ["malware-protection-plan", plan_id] => match method {
            "GET" => st.get_malware_protection_plan(plan_id),
            "PATCH" => st.update_malware_protection_plan(plan_id, &params),
            "DELETE" => st.delete_malware_protection_plan(plan_id),
            _ => unimplemented(req),
        },
        ["malware-scan"] => Response::json_status(200, &json!({"scans": []})),
        ["malware-scan", "start"] => st.start_malware_scan(),
        ["malware-scan", scan_id] => st.get_malware_scan(scan_id),
        ["object-malware-scan", "send"] => st.start_malware_scan(),
        ["invitation"] => Response::json_status(200, &json!({"invitations": []})),
        ["invitation", "decline"] | ["invitation", "delete"] => {
            Response::json_status(200, &json!({"unprocessedAccounts": []}))
        }
        ["invitation", "count"] => Response::json_status(200, &json!({"invitationsCount": 0})),
        ["organization", "statistics"] => st.organization_statistics(),
        ["admin"] => st.organization_admin_accounts(),
        ["admin", "enable"] => st.update_organization_admin(&params, true),
        ["admin", "disable"] => st.update_organization_admin(&params, false),
        _ => unimplemented(req),
    }
}

fn unimplemented(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

impl GdState {
    fn detector(&self, id: &str) -> Result<&Detector, Response> {
        self.detectors.get(id).ok_or_else(detector_not_found)
    }

    fn detector_mut(&mut self, id: &str) -> Result<&mut Detector, Response> {
        self.detectors.get_mut(id).ok_or_else(detector_not_found)
    }

    // ---- detectors ----------------------------------------------------------

    fn create_detector(&mut self, params: &Value) -> Response {
        let mut freq = params
            .get("findingPublishingFrequency")
            .and_then(Value::as_str)
            .unwrap_or("SIX_HOURS")
            .to_string();
        if !["FIFTEEN_MINUTES", "ONE_HOUR", "SIX_HOURS"].contains(&freq.as_str()) {
            freq = "SIX_HOURS".into();
        }
        let id = new_hex32();
        self.detectors.insert(
            id.clone(),
            Detector {
                arn: format!(
                    "arn:aws:guardduty:{region}:{account}:detector/{id}",
                    region = self.region,
                    account = self.account
                ),
                service_role: format!(
                    "arn:aws:iam::{account}:role/aws-service-role/guardduty.amazonaws.com/AWSServiceRoleForAmazonGuardDuty",
                    account = self.account
                ),
                created_at: now_stamp(),
                updated_at: now_stamp(),
                finding_publish_freq: freq,
                enabled: params.get("enable").and_then(Value::as_bool).unwrap_or(false),
                datasources: params.get("dataSources").cloned().unwrap_or(json!({})),
                tags: params.get("tags").and_then(Value::as_object).cloned().unwrap_or_default(),
                features: params.get("features").cloned().unwrap_or(json!([])),
                filters: HashMap::new(),
                ip_sets: HashMap::new(),
                threat_intel_sets: HashMap::new(),
                threat_entity_sets: HashMap::new(),
                trusted_entity_sets: HashMap::new(),
                publishing_destinations: HashMap::new(),
                members: HashMap::new(),
                findings: HashMap::new(),
                administrator_id: None,
            },
        );
        Response::json_status(200, &json!({"detectorId": id}))
    }

    fn list_detectors(&self) -> Response {
        Response::json_status(
            200,
            &json!({"detectorIds": self.detectors.keys().cloned().collect::<Vec<_>>()}),
        )
    }

    fn get_detector(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => Response::json_status(200, &detector.to_json()),
            Err(resp) => resp,
        }
    }

    fn update_detector(&mut self, id: &str, params: &Value) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let detector = self.detector_mut(id).unwrap();
        if let Some(b) = params.get("enable") {
            detector.enabled = b.as_bool().unwrap_or(false);
        }
        if let Some(freq) = params
            .get("findingPublishingFrequency")
            .and_then(Value::as_str)
        {
            detector.finding_publish_freq = freq.to_string();
        }
        if let Some(ds) = params.get("dataSources") {
            detector.datasources = ds.clone();
        }
        if let Some(features) = params.get("features") {
            detector.features = features.clone();
        }
        detector.updated_at = now_stamp();
        Response::json_status(200, &json!({}))
    }

    fn delete_detector(&mut self, id: &str) -> Response {
        self.detectors.remove(id);
        Response::json_status(200, &json!({}))
    }

    // ---- filters ------------------------------------------------------------

    fn create_filter(&mut self, id: &str, params: &Value) -> Response {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match self.detector_mut(id) {
            Ok(detector) => {
                detector.filters.insert(
                    name.clone(),
                    Filter {
                        name: name.clone(),
                        action: params.get("action").cloned().unwrap_or(json!(null)),
                        description: params.get("description").cloned().unwrap_or(json!("")),
                        finding_criteria: params
                            .get("findingCriteria")
                            .cloned()
                            .unwrap_or(json!({})),
                        rank: params.get("rank").cloned().unwrap_or(json!(1)),
                        ..Filter::default()
                    },
                );
                Response::json_status(200, &json!({"name": name}))
            }
            Err(resp) => resp,
        }
    }

    fn get_filter(&self, id: &str, name: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => match detector.filters.get(name) {
                Some(filter) => Response::json_status(200, &filter.to_json()),
                None => filter_not_found(name),
            },
            Err(resp) => resp,
        }
    }

    fn list_filters(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => Response::json_status(
                200,
                &json!({"filterNames": detector.filters.keys().cloned().collect::<Vec<_>>()}),
            ),
            Err(resp) => resp,
        }
    }

    fn update_filter(&mut self, id: &str, name: &str, params: &Value) -> Response {
        let Some(detector) = self.detectors.get_mut(id) else {
            return detector_not_found();
        };
        let Some(filter) = detector.filters.get_mut(name) else {
            return filter_not_found(name);
        };
        if let Some(v) = null_if_absent(params.get("action").cloned()) {
            filter.action = v;
        }
        if let Some(v) = null_if_absent(params.get("description").cloned()) {
            filter.description = v;
        }
        if let Some(v) = null_if_absent(params.get("findingCriteria").cloned()) {
            filter.finding_criteria = v;
        }
        if let Some(v) = null_if_absent(params.get("rank").cloned()) {
            filter.rank = v;
        }
        Response::json_status(200, &json!({"name": name}))
    }

    fn delete_filter(&mut self, id: &str, name: &str) -> Response {
        match self.detectors.get_mut(id) {
            Some(detector) => {
                detector.filters.remove(name);
                Response::json_status(200, &json!({}))
            }
            None => detector_not_found(),
        }
    }

    // ---- sets ----------------------------------------------------------------

    fn create_set(&mut self, id: &str, params: &Value, kind: &str) -> Response {
        if self.detector(id).is_err() {
            return detector_not_found();
        }
        let set_id = new_hex32();
        let record = Detector::set_record(
            id,
            &set_id,
            kind,
            params,
            &self.region.clone(),
            &self.account.clone(),
        );
        let detector = self.detector_mut(id).unwrap();
        set_enter(detector, kind)
            .expect("valid kind")
            .insert(set_id.clone(), record);
        Response::json_status(200, &json!({set_id_key(kind): set_id}))
    }

    fn get_set(&self, id: &str, set_id: &str, kind: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => match set_exit(detector, kind).and_then(|m| m.get(set_id)) {
                Some(set) => Response::json_status(200, &set.set_to_json()),
                None => self.set_not_found(id, kind, set_id),
            },
            Err(resp) => resp,
        }
    }

    fn update_set(&mut self, id: &str, set_id: &str, kind: &str, params: &Value) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let detector = self.detector_mut(id).unwrap();
        let Some(set) = set_enter(detector, kind).and_then(|m| m.get_mut(set_id)) else {
            return self.set_not_found(id, kind, set_id);
        };
        if let Some(v) = null_if_absent(params.get("name").cloned()) {
            set.name = v.as_str().unwrap_or("").to_string();
        }
        if let Some(v) = null_if_absent(params.get("location").cloned()) {
            set.location = Some(v);
        }
        if let Some(activate) = params.get("activate").and_then(Value::as_bool) {
            set.status = Some(json!(if activate { "ACTIVE" } else { "INACTIVE" }));
        }
        Response::json_status(200, &json!({}))
    }

    fn delete_set(&mut self, id: &str, set_id: &str, kind: &str) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let detector = self.detector_mut(id).unwrap();
        let removed = set_enter(detector, kind)
            .expect("valid kind")
            .remove(set_id)
            .is_some();
        if !removed {
            return self.set_not_found(id, kind, set_id);
        }
        Response::json_status(200, &json!({}))
    }

    fn list_set_ids(&self, id: &str, kind: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => Response::json_status(
                200,
                &json!({set_ids_key(kind): set_id_members(detector, kind)}),
            ),
            Err(resp) => resp,
        }
    }

    fn set_not_found(&self, id: &str, kind: &str, set_id: &str) -> Response {
        crate::proto::json_error(
            400,
            "BadRequestException",
            &format!(
                "The {kind} with arn arn:aws:guardduty:{region}:{account}:detector/{id}/{kind}/{set_id} does not exist",
                region = self.region,
                account = self.account
            ),
        )
    }

    // ---- findings ------------------------------------------------------------

    fn create_sample_findings(&mut self, id: &str, params: &Value) -> Response {
        if self.detector(id).is_err() {
            return detector_not_found();
        }
        let region = self.region.clone();
        let account = self.account.clone();
        let detector = self.detector_mut(id).unwrap();
        for finding_type in params
            .get("findingTypes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let fid = new_hex32();
            detector.findings.insert(
                fid.clone(),
                json!({
                    "accountId": account,
                    "arn": format!(
                        "arn:aws:guardduty:{region}:{account}:detector/{id}/finding/{fid}",
                        region = region,
                        account = account
                    ),
                    "id": fid,
                    "region": region,
                    "type": finding_type.as_str().unwrap_or(""),
                    "severity": 5.0,
                    "createdAt": now_stamp(),
                    "updatedAt": now_stamp(),
                    "resource": {},
                    "service": {"serviceName": "guardduty"},
                }),
            );
        }
        Response::json_status(200, &json!({}))
    }

    fn list_findings(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => Response::json_status(
                200,
                &json!({"findingIds": detector.findings.keys().cloned().collect::<Vec<_>>()}),
            ),
            Err(resp) => resp,
        }
    }

    fn get_findings(&self, id: &str, params: &Value) -> Response {
        match self.detector(id) {
            Ok(detector) => {
                let findings: Vec<Value> = params
                    .get("findingIds")
                    .and_then(Value::as_array)
                    .map(|ids| {
                        ids.iter()
                            .filter_map(Value::as_str)
                            .filter_map(|f| detector.findings.get(f))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                Response::json_status(200, &json!({"findings": findings}))
            }
            Err(resp) => resp,
        }
    }

    fn findings_statistics(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => {
                let mut count_by_severity = Map::new();
                for finding in detector.findings.values() {
                    let key = finding
                        .get("severity")
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    let current = count_by_severity
                        .get(&key)
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    count_by_severity.insert(key, json!(current + 1));
                }
                Response::json_status(
                    200,
                    &json!({"findingStatistics": {"countBySeverity": Value::Object(count_by_severity)}}),
                )
            }
            Err(resp) => resp,
        }
    }

    fn archive_findings(&mut self, id: &str, params: &Value, archived: bool) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        if let Some(ids) = params.get("findingIds").and_then(Value::as_array) {
            let detector = self.detector_mut(id).unwrap();
            for fid in ids {
                if let Some(finding) = fid.as_str().and_then(|f| detector.findings.get_mut(f)) {
                    finding["service"]["archived"] = json!(archived);
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    fn update_findings_feedback(&mut self, id: &str, params: &Value) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let feedback = params.get("feedback").cloned().unwrap_or(json!(""));
        if let Some(ids) = params.get("findingIds").and_then(Value::as_array) {
            let detector = self.detector_mut(id).unwrap();
            for fid in ids {
                if let Some(finding) = fid.as_str().and_then(|f| detector.findings.get_mut(f)) {
                    finding["service"]["userFeedback"] = feedback.clone();
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    // ---- organization -----------------------------------------------------------

    fn update_organization_admin(&mut self, params: &Value, enable: bool) -> Response {
        let account_id = params
            .get("adminAccountId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if enable {
            self.admin_account_ids.push(account_id);
        } else {
            self.admin_account_ids.retain(|a| *a != account_id);
        }
        Response::json_status(200, &json!({}))
    }

    fn organization_admin_accounts(&self) -> Response {
        let accounts: Vec<Value> = self
            .admin_account_ids
            .iter()
            .map(|a| json!({"adminAccountId": a, "adminStatus": "ENABLED"}))
            .collect();
        Response::json_status(200, &json!({"adminAccounts": accounts}))
    }

    fn organization_configuration(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(
                200,
                &json!({
                    "autoEnable": false,
                    "memberAccountLimitReached": false,
                    "dataSources": {
                        "s3Logs": {"autoEnable": false},
                        "kubernetes": {"auditLogs": {"autoEnable": false}},
                        "malwareProtection": {
                            "scanEc2InstanceWithFindings": {"ebsVolumes": {"autoEnable": false}},
                        },
                    },
                    "autoEnableOrganizationMembers": "NONE",
                    "features": [],
                }),
            ),
            Err(resp) => resp,
        }
    }

    fn organization_statistics(&self) -> Response {
        Response::json_status(
            200,
            &json!({"organizationDetails": {
                "updatedAt": now_stamp(),
                "organizationStatistics": {
                    "totalAccountsCount": 0,
                    "memberAccountsCount": 0,
                    "activeAccountsCount": 0,
                    "enabledAccountsCount": 0,
                    "countByFeature": [],
                },
            }}),
        )
    }

    // ---- members ----------------------------------------------------------------

    fn create_members(&mut self, id: &str, params: &Value) -> Response {
        if self.detector(id).is_err() {
            return detector_not_found();
        }
        let master_id = self.account.clone();
        if let Some(details) = params.get("accountDetails").and_then(Value::as_array) {
            let detector = self.detector_mut(id).unwrap();
            for detail in details {
                let acct = detail
                    .get("accountId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let email = detail
                    .get("email")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                detector.members.insert(
                    acct.clone(),
                    json!({
                        "accountId": acct,
                        "email": email,
                        "detectorId": id,
                        "masterId": master_id,
                        "administratorId": master_id,
                        "relationshipStatus": "CREATED",
                        "invitedAt": now_stamp(),
                        "updatedAt": now_stamp(),
                    }),
                );
            }
        }
        Response::json_status(200, &json!({"unprocessedAccounts": []}))
    }

    fn get_members(&self, id: &str, params: &Value) -> Response {
        match self.detector(id) {
            Ok(detector) => {
                let mut members = Vec::new();
                let mut unprocessed = Vec::new();
                for acct in account_ids_from(params) {
                    match detector.members.get(&acct) {
                        Some(m) => members.push(m.clone()),
                        None => unprocessed.push(json!({
                            "accountId": acct,
                            "result": "Account not found as a member",
                        })),
                    }
                }
                Response::json_status(
                    200,
                    &json!({"members": members, "unprocessedAccounts": unprocessed}),
                )
            }
            Err(resp) => resp,
        }
    }

    fn list_members(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => Response::json_status(
                200,
                &json!({"members": detector.members.values().cloned().collect::<Vec<_>>()}),
            ),
            Err(resp) => resp,
        }
    }

    fn delete_members(&mut self, id: &str, params: &Value) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let mut unprocessed = Vec::new();
        let detector = self.detector_mut(id).unwrap();
        for acct in account_ids_from(params) {
            if detector.members.remove(&acct).is_none() {
                unprocessed.push(json!({
                    "accountId": acct,
                    "result": "Account not found as a member",
                }));
            }
        }
        Response::json_status(200, &json!({"unprocessedAccounts": unprocessed}))
    }

    fn toggle_members(&mut self, id: &str, params: &Value, status: &str) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let mut unprocessed = Vec::new();
        let detector = self.detector_mut(id).unwrap();
        for acct in account_ids_from(params) {
            match detector.members.get_mut(&acct) {
                Some(member) => member["relationshipStatus"] = json!(status),
                None => unprocessed.push(json!({
                    "accountId": acct,
                    "result": "Account not found as a member",
                })),
            }
        }
        Response::json_status(200, &json!({"unprocessedAccounts": unprocessed}))
    }

    fn get_member_detectors(&self, id: &str, params: &Value) -> Response {
        if self.detector(id).is_err() {
            return detector_not_found();
        }
        let members: Vec<Value> = account_ids_from(params)
            .iter()
            .map(|acct| {
                json!({
                    "accountId": acct,
                    "dataSources": {
                        "cloudTrail": {"status": "DISABLED"},
                        "dnsLogs": {"status": "DISABLED"},
                        "flowLogs": {"status": "DISABLED"},
                        "s3Logs": {"status": "DISABLED"},
                        "kubernetes": {"auditLogs": {"status": "DISABLED"}},
                        "malwareProtection": {
                            "scanEc2InstanceWithFindings": {"ebsVolumes": {"status": "DISABLED"}},
                        },
                    },
                    "features": [],
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"memberDetectors": members, "unprocessedAccounts": []}),
        )
    }

    fn free_trial_days(&self, id: &str, params: &Value) -> Response {
        if self.detector(id).is_err() {
            return detector_not_found();
        }
        let mut ids = account_ids_from(params);
        if ids.is_empty() {
            ids = vec![self.account.clone()];
        }
        let accounts: Vec<Value> = ids
            .iter()
            .map(|acct| {
                json!({
                    "accountId": acct,
                    "dataSources": {
                        "cloudTrail": {"freeTrialDaysRemaining": 0},
                        "dnsLogs": {"freeTrialDaysRemaining": 0},
                        "flowLogs": {"freeTrialDaysRemaining": 0},
                        "s3Logs": {"freeTrialDaysRemaining": 0},
                        "kubernetes": {"auditLogs": {"freeTrialDaysRemaining": 0}},
                        "malwareProtection": {
                            "scanEc2InstanceWithFindings": {"ebsVolumes": {"freeTrialDaysRemaining": 0}},
                        },
                    },
                    "features": [],
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({"accounts": accounts, "unprocessedAccounts": []}),
        )
    }

    fn usage_statistics(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(
                200,
                &json!({"usageStatistics": {
                    "sumByAccount": [],
                    "sumByDataSource": [],
                    "sumByResource": [],
                    "sumByFeature": [],
                    "topResources": [],
                }}),
            ),
            Err(resp) => resp,
        }
    }

    fn unprocessed_ok(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(200, &json!({"unprocessedAccounts": []})),
            Err(resp) => resp,
        }
    }

    fn ok(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(200, &json!({})),
            Err(resp) => resp,
        }
    }

    // ---- publishing destinations --------------------------------------------------

    fn create_publishing_destination(&mut self, id: &str, params: &Value) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let dest_id = new_hex32();
        let dest = json!({
            "destinationId": dest_id.clone(),
            "destinationType": params.get("destinationType").cloned().unwrap_or(json!("S3")),
            "destinationProperties": params.get("destinationProperties").cloned().unwrap_or(json!({})),
            "publishingFailureStartTimestamp": 0,
            "status": "PUBLISHING",
        });
        self.detector_mut(id)
            .unwrap()
            .publishing_destinations
            .insert(dest_id.clone(), dest);
        Response::json_status(200, &json!({"destinationId": dest_id}))
    }

    fn dest_not_found(&self, id: &str, dest_id: &str) -> Response {
        crate::proto::json_error(
            400,
            "BadRequestException",
            &format!(
                "arn:aws:guardduty:{region}:{account}:detector/{id}/publishingDestination/{dest_id}",
                region = self.region,
                account = self.account
            ),
        )
    }

    fn describe_publishing_destination(&self, id: &str, dest_id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => match detector.publishing_destinations.get(dest_id) {
                Some(dest) => Response::json_status(200, dest),
                None => self.dest_not_found(id, dest_id),
            },
            Err(resp) => resp,
        }
    }

    fn list_publishing_destinations(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(detector) => {
                let destinations: Vec<Value> = detector
                    .publishing_destinations
                    .values()
                    .map(|d| {
                        json!({
                            "destinationId": d["destinationId"],
                            "destinationType": d["destinationType"],
                            "status": d["status"],
                        })
                    })
                    .collect();
                Response::json_status(200, &json!({"destinations": destinations}))
            }
            Err(resp) => resp,
        }
    }

    fn update_publishing_destination(
        &mut self,
        id: &str,
        dest_id: &str,
        params: &Value,
    ) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let detector = self.detector_mut(id).unwrap();
        match detector.publishing_destinations.get_mut(dest_id) {
            Some(dest) => {
                if let Some(props) = null_if_absent(params.get("destinationProperties").cloned()) {
                    dest["destinationProperties"] = props;
                }
                Response::json_status(200, &json!({}))
            }
            None => self.dest_not_found(id, dest_id),
        }
    }

    fn delete_publishing_destination(&mut self, id: &str, dest_id: &str) -> Response {
        if self.detector_mut(id).is_err() {
            return detector_not_found();
        }
        let detector = self.detector_mut(id).unwrap();
        if detector.publishing_destinations.remove(dest_id).is_none() {
            return self.dest_not_found(id, dest_id);
        }
        Response::json_status(200, &json!({}))
    }

    // ---- misc --------------------------------------------------------------------

    fn coverage_statistics(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(
                200,
                &json!({"coverageStatistics": {"countByResourceType": {}, "countByCoverageStatus": {}}}),
            ),
            Err(resp) => resp,
        }
    }

    /// ListCoverage returns Resources + NextToken (empty page like moto).
    fn coverage_list(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(200, &json!({"resources": [], "nextToken": null})),
            Err(resp) => resp,
        }
    }

    /// DescribeMalwareScans returns Scans + NextToken (empty page like moto).
    fn malware_scans(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(200, &json!({"scans": [], "nextToken": null})),
            Err(resp) => resp,
        }
    }

    fn malware_scan_settings(&self, id: &str) -> Response {
        match self.detector(id) {
            Ok(_) => Response::json_status(
                200,
                &json!({
                    "scanResourceCriteria": {"include": {}, "exclude": {}},
                    "ebsSnapshotPreservation": "NO_RETENTION",
                }),
            ),
            Err(resp) => resp,
        }
    }

    // ---- tags ------------------------------------------------------------------

    fn list_tags_for_resource(&self, arn: &str) -> Response {
        let tags = self.locate_tags(arn).cloned().unwrap_or_default();
        Response::json_status(200, &json!({"tags": Value::Object(tags)}))
    }

    fn locate_tags(&self, arn: &str) -> Option<&Map<String, Value>> {
        for detector in self.detectors.values() {
            if detector.arn == arn {
                return Some(&detector.tags);
            }
            for map in [
                &detector.ip_sets,
                &detector.threat_intel_sets,
                &detector.threat_entity_sets,
                &detector.trusted_entity_sets,
            ] {
                for set in map.values() {
                    if set.arn.as_deref() == Some(arn) {
                        return Some(&set.tags_map);
                    }
                }
            }
        }
        None
    }

    fn tag_resource(&mut self, arn: &str, params: &Value) -> Response {
        let Some(tags) = params.get("tags").and_then(Value::as_object) else {
            return Response::json_status(200, &json!({}));
        };
        for detector in self.detectors.values_mut() {
            if detector.arn == arn {
                for (k, v) in tags {
                    detector.tags.insert(k.clone(), v.clone());
                }
                return Response::json_status(200, &json!({}));
            }
            for map in [
                &mut detector.ip_sets,
                &mut detector.threat_intel_sets,
                &mut detector.threat_entity_sets,
                &mut detector.trusted_entity_sets,
            ] {
                for set in map.values_mut() {
                    if set.arn.as_deref() == Some(arn) {
                        for (k, v) in tags {
                            set.tags_map.insert(k.clone(), v.clone());
                        }
                        return Response::json_status(200, &json!({}));
                    }
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    fn untag_resource(&mut self, arn: &str, keys: &[String]) -> Response {
        for detector in self.detectors.values_mut() {
            if detector.arn == arn {
                for key in keys {
                    detector.tags.remove(key);
                }
                return Response::json_status(200, &json!({}));
            }
            for map in [
                &mut detector.ip_sets,
                &mut detector.threat_intel_sets,
                &mut detector.threat_entity_sets,
                &mut detector.trusted_entity_sets,
            ] {
                for set in map.values_mut() {
                    if set.arn.as_deref() == Some(arn) {
                        for key in keys {
                            set.tags_map.remove(key);
                        }
                        return Response::json_status(200, &json!({}));
                    }
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    // ---- malware protection plans --------------------------------------------------

    fn create_malware_protection_plan(&mut self, params: &Value) -> Response {
        let plan_id = new_hex32();
        self.malware_plans.insert(
            plan_id.clone(),
            json!({
                "malwareProtectionPlanId": plan_id.clone(),
                "arn": format!(
                    "arn:aws:guardduty:{region}:{account}:malware-protection-plan/{plan_id}",
                    region = self.region,
                    account = self.account
                ),
                "role": params.get("role").cloned().unwrap_or(json!("")),
                "protectedResource": params.get("protectedResource").cloned().unwrap_or(json!({})),
                "actions": params.get("actions").cloned().unwrap_or(json!({})),
                "tags": params.get("tags").cloned().unwrap_or(json!({})),
                "status": "ACTIVE",
                "createdAt": now_stamp(),
            }),
        );
        Response::json_status(200, &json!({"malwareProtectionPlanId": plan_id}))
    }

    fn malware_plan_not_found(&self, plan_id: &str) -> Response {
        crate::proto::json_error(
            400,
            "ResourceNotFoundException",
            &format!(
                "arn:aws:guardduty:{region}:{account}:malware-protection-plan/{plan_id}",
                region = self.region,
                account = self.account
            ),
        )
    }

    fn get_malware_protection_plan(&self, plan_id: &str) -> Response {
        match self.malware_plans.get(plan_id) {
            Some(plan) => Response::json_status(200, plan),
            None => self.malware_plan_not_found(plan_id),
        }
    }

    fn update_malware_protection_plan(&mut self, plan_id: &str, params: &Value) -> Response {
        match self.malware_plans.get_mut(plan_id) {
            Some(plan) => {
                if let Some(role) = null_if_absent(params.get("role").cloned()) {
                    plan["role"] = role;
                }
                if let Some(actions) = null_if_absent(params.get("actions").cloned()) {
                    plan["actions"] = actions;
                }
                if let Some(resource) = null_if_absent(params.get("protectedResource").cloned()) {
                    plan["protectedResource"] = resource;
                }
                Response::json_status(200, &json!({}))
            }
            None => self.malware_plan_not_found(plan_id),
        }
    }

    fn delete_malware_protection_plan(&mut self, plan_id: &str) -> Response {
        if self.malware_plans.remove(plan_id).is_none() {
            return self.malware_plan_not_found(plan_id);
        }
        Response::json_status(200, &json!({}))
    }

    fn get_malware_scan(&self, scan_id: &str) -> Response {
        Response::json_status(
            200,
            &json!({
                "scanId": scan_id,
                "status": "COMPLETED",
                "scanStartTime": now_stamp(),
                "scanEndTime": now_stamp(),
                "triggerDetails": {},
                "resourceDetails": {},
                "scanResultDetails": {"scanResult": "CLEAN"},
                "totalBytes": 0,
                "fileCount": 0,
            }),
        )
    }

    fn start_malware_scan(&self) -> Response {
        Response::json_status(200, &json!({"scanId": new_hex32()}))
    }

    fn disassociate_from_admin(&mut self, id: &str) -> Response {
        match self.detector_mut(id) {
            Ok(_) => Response::json_status(200, &json!({})),
            Err(resp) => resp,
        }
    }
}

impl GdState {
    fn get_master_or_admin(&self, id: &str, member_key: &str) -> Response {
        match self.detector(id) {
            Err(resp) => return resp,
            Ok(detector) => {
                if let Some(admin) = &detector.administrator_id {
                    return Response::json_status(
                        200,
                        &json!({member_key: {"accountId": admin, "relationshipStatus": "ENABLED"}}),
                    );
                }
                if let Some(first) = self.admin_account_ids.first() {
                    return Response::json_status(
                        200,
                        &json!({member_key: {"accountId": first, "relationshipStatus": "ENABLED"}}),
                    );
                }
                Response::json_status(200, &json!({}))
            }
        }
    }

    fn list_malware_protection_plans(&self) -> Response {
        Response::json_status(
            200,
            &json!({"malwareProtectionPlans": self
                .malware_plans
                .keys()
                .map(|id| json!({"malwareProtectionPlanId": id}))
                .collect::<Vec<_>>()}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "guardduty".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 123456789001,
            region: "us-east-1".into(),
        }
    }

    fn json_body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn detector() -> String {
        json_body(dispatch(&req("POST", "/detector", json!({"enable": true}))))["detectorId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn detector_lifecycle() {
        let id = detector();
        let body = json_body(dispatch(&req("GET", &format!("/detector/{id}"), json!({}))));
        assert_eq!(body["status"], "ENABLED");
        assert!(body["serviceRole"]
            .as_str()
            .unwrap()
            .contains("guardduty.amazonaws.com"));

        let listed = json_body(dispatch(&req("GET", "/detector", json!({}))));
        assert!(listed["detectorIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d.as_str() == Some(id.as_str())));

        assert_eq!(
            dispatch(&req("DELETE", &format!("/detector/{id}"), json!({}))).status,
            200
        );
        let miss = dispatch(&req("GET", &format!("/detector/{id}"), json!({})));
        assert_eq!(miss.status, 400);
    }

    #[test]
    fn filters_and_ip_sets() {
        let id = detector();
        dispatch(&req(
            "POST",
            &format!("/detector/{id}/filter"),
            json!({"name": "f1", "action": "ARCHIVE", "description": "d", "rank": 1, "findingCriteria": {"Criterion": {}}}),
        ));
        let body = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/filter/f1"),
            json!({}),
        )));
        assert_eq!(body["action"], "ARCHIVE");
        let names = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/filter"),
            json!({}),
        )));
        assert_eq!(names["filterNames"], json!(["f1"]));

        let ip = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/ipset"),
            json!({"name": "ips", "format": "TXT", "location": "s3://x/f", "activate": true}),
        )));
        let ip_set_id = ip["ipSetId"].as_str().unwrap().to_string();
        let got = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/ipset/{ip_set_id}"),
            json!({}),
        )));
        assert_eq!(got["status"], "ACTIVE");
        let sets = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/ipset"),
            json!({}),
        )));
        assert!(sets["ipSetIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d.as_str() == Some(ip_set_id.as_str())));

        let tis = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/threatintelset"),
            json!({"name": "t", "format": "TXT", "location": "s3://x/f", "activate": false}),
        )));
        let tis_id = tis["threatIntelSetId"].as_str().unwrap().to_string();
        let got = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/threatintelset/{tis_id}"),
            json!({}),
        )));
        assert_eq!(got["status"], "INACTIVE");
    }

    #[test]
    fn findings_flow() {
        let id = detector();
        dispatch(&req(
            "POST",
            &format!("/detector/{id}/findings/create"),
            json!({"findingTypes": ["Recon:EC2/PortProbe"]}),
        ));
        let listed = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/findings"),
            json!({}),
        )));
        let ids = listed["findingIds"].as_array().unwrap();
        assert!(!ids.is_empty());

        let got = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/findings/get"),
            json!({"findingIds": ids}),
        )));
        assert_eq!(got["findings"][0]["severity"], 5.0);

        let stats = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/findings/statistics"),
            json!({}),
        )));
        assert!(stats["findingStatistics"]["countBySeverity"].is_object());
    }

    #[test]
    fn members_and_publishing() {
        let id = detector();
        dispatch(&req(
            "POST",
            &format!("/detector/{id}/member"),
            json!({"accountDetails": [{"accountId": "777777777777", "email": "a@b.c"}]}),
        ));
        let members = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/member"),
            json!({}),
        )));
        assert_eq!(members["members"][0]["accountId"], "777777777777");

        let dest = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/publishingDestination"),
            json!({"destinationType": "S3", "destinationProperties": {"destinationArn": "arn:s3:b", "kmsKeyArn": "arn:kms"}}),
        )));
        let dest_id = dest["destinationId"].as_str().unwrap().to_string();
        let described = json_body(dispatch(&req(
            "GET",
            &format!("/detector/{id}/publishingDestination/{dest_id}"),
            json!({}),
        )));
        assert_eq!(described["status"], "PUBLISHING");

        let fake = dispatch(&req(
            "GET",
            &format!("/detector/{id}/publishingDestination/fakeid"),
            json!({}),
        ));
        assert_eq!(fake.status, 400);
    }

    #[test]
    fn tags_and_admin() {
        let id = detector();
        let body = json_body(dispatch(&req("GET", &format!("/detector/{id}"), json!({}))));
        let arn = format!("arn:aws:guardduty:us-east-1:123456789001:detector/{id}");
        assert_eq!(body["tags"], json!({}));

        dispatch(&req(
            "POST",
            &format!("/tags/{arn}"),
            json!({"tags": {"env": "t"}}),
        ));

        dispatch(&req(
            "POST",
            "/admin/enable",
            json!({"adminAccountId": "888888888888"}),
        ));
        let admins = json_body(dispatch(&req("GET", "/admin", json!({}))));
        assert_eq!(admins["adminAccounts"][0]["adminAccountId"], "888888888888");
    }

    #[test]
    fn entity_sets_and_plans() {
        let id = detector();
        let tes = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/threatentityset"),
            json!({"name": "e", "format": "TXT", "location": "s3://x/e", "activate": true}),
        )));
        assert!(tes["threatEntitySetId"].as_str().is_some());

        let trusted = json_body(dispatch(&req(
            "POST",
            &format!("/detector/{id}/trustedentityset"),
            json!({"name": "t", "format": "TXT", "location": "s3://x/t", "activate": false}),
        )));
        assert!(trusted["trustedEntitySetId"].as_str().is_some());

        let plan = json_body(dispatch(&req(
            "POST",
            "/malware-protection-plan",
            json!({"role": "arn:aws:iam::123456789001:role/r"}),
        )));
        assert!(plan["malwareProtectionPlanId"].as_str().is_some());

        let scan = json_body(dispatch(&req("POST", "/malware-scan/start", json!({}))));
        assert!(scan["scanId"].as_str().is_some());
    }
}
