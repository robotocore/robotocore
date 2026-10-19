//! AWS Shield backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `shield` (`X-Amz-Target: AWSShield_20160616.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `ShieldBackend` (moto/shield): protections, protection groups, the
//! account subscription, tags (moto's `TaggingService`), DRT access,
//! emergency contacts / proactive engagement, health-check associations,
//! application-layer automatic response, and the fake attack surface (moto
//! holds no attacks, so describe always 404s and the lists stay empty).
//!
//! Contract: probes/shield.json (per-operation moto-era behavior) and
//! tests/compatibility/test_shield_compat.py. Timestamps mirror moto:
//! subscription times are `%d/%m/%Y, %H:%M:%S` strings, attack-statistics
//! ranges are unix floats. `DescribeProtection` with neither selector keeps
//! moto's unguarded crash — a 500 `InternalError` — as recorded in the probe.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

const SUB_TIME_FMT: &str = "%d/%m/%Y, %H:%M:%S";

/// moto's default subscription: one year, 31_536_000s commitment.
const TIME_COMMITMENT_SECONDS: i64 = 31_536_000;
const SUBSCRIPTION_YEAR_DAYS: i64 = 365;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, ShieldStore>>>;

fn stores() -> Stores {
    store::slot::<ShieldStore>("shield")
}

/// Per-(account, region) in-memory state for AWS Shield.
#[derive(Default)]
struct ShieldStore {
    /// protection id -> protection
    protections: BTreeMap<String, Protection>,
    /// group id -> protection group
    protection_groups: BTreeMap<String, ProtectionGroup>,
    subscription: Option<Subscription>,
    /// arn -> {tag key: value} — moto's TaggingService
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
    drt_role_arn: Option<String>,
    drt_log_buckets: Vec<String>,
    /// Emergency contacts exactly as the client sent them.
    emergency_contacts: Vec<Value>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut ShieldStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(PartialEq)]
struct Protection {
    name: String,
    resource_arn: String,
    protection_id: String,
    protection_arn: String,
    /// moto's derived list-filter type (ELASTIC_IP_ALLOCATION, ...).
    resource_type: String,
    health_check_ids: Vec<String>,
    /// `None` while disabled — moto clears to `{}`, which its to_dict drops.
    application_layer_automatic_response: Option<Value>,
}

impl Protection {
    fn new(account: u64, name: &str, resource_arn: &str) -> Self {
        let protection_id = Uuid::new_v4().to_string();
        Protection {
            name: name.to_string(),
            resource_arn: resource_arn.to_string(),
            protection_arn: format!("arn:aws:shield::{account}:protection/{protection_id}"),
            protection_id,
            resource_type: shield_resource_type(resource_arn),
            health_check_ids: Vec::new(),
            application_layer_automatic_response: None,
        }
    }

    /// moto's `Protection.to_dict()`: falsy members drop off the wire.
    fn to_dict(&self) -> Value {
        let mut out = json!({
            "Id": self.protection_id,
            "Name": self.name,
            "ResourceArn": self.resource_arn,
            "HealthCheckIds": self.health_check_ids,
            "ProtectionArn": self.protection_arn,
        });
        if let Some(cfg) = &self.application_layer_automatic_response {
            out["ApplicationLayerAutomaticResponseConfiguration"] = cfg.clone();
        }
        if self.name.is_empty() {
            out.as_object_mut().expect("object").remove("Name");
        }
        if self.health_check_ids.is_empty() {
            out.as_object_mut()
                .expect("object")
                .remove("HealthCheckIds");
        }
        out
    }
}

/// moto's `Protection.__init__` resource-type derivation.
fn shield_resource_type(resource_arn: &str) -> String {
    let service = resource_arn.split(':').nth(2).unwrap_or("");
    match service {
        "cloudfront" => "CLOUDFRONT_DISTRIBUTION",
        "globalaccelerator" => "GLOBAL_ACCELERATOR",
        "route53" => "ROUTE_53_HOSTED_ZONE",
        "ec2" => "ELASTIC_IP_ALLOCATION",
        "elasticloadbalancing" => {
            let segment = resource_arn
                .rsplit(':')
                .next()
                .unwrap_or("")
                .split('/')
                .nth(1);
            if segment == Some("app") {
                "APPLICATION_LOAD_BALANCER"
            } else {
                "CLASSIC_LOAD_BALANCER"
            }
        }
        _ => "",
    }
    .to_string()
}

struct ProtectionGroup {
    protection_group_id: String,
    aggregation: String,
    pattern: String,
    resource_type: Option<String>,
    members: Vec<String>,
    protection_group_arn: String,
}

impl ProtectionGroup {
    fn new(
        account: u64,
        protection_group_id: &str,
        aggregation: &str,
        pattern: &str,
        resource_type: Option<&str>,
        members: Vec<String>,
    ) -> Self {
        ProtectionGroup {
            protection_group_arn: format!(
                "arn:aws:shield::{account}:protection-group/{protection_group_id}"
            ),
            protection_group_id: protection_group_id.to_string(),
            aggregation: aggregation.to_string(),
            pattern: pattern.to_string(),
            resource_type: resource_type.map(String::from),
            members,
        }
    }

    fn to_dict(&self) -> Value {
        let mut out = json!({
            "ProtectionGroupId": self.protection_group_id,
            "Aggregation": self.aggregation,
            "Pattern": self.pattern,
            "Members": self.members,
            "ProtectionGroupArn": self.protection_group_arn,
        });
        if let Some(rt) = &self.resource_type {
            out["ResourceType"] = json!(rt);
        }
        out
    }
}

struct Subscription {
    start_time: String,
    end_time: String,
    auto_renew: String,
    proactive_engagement_status: String,
    subscription_arn: String,
}

impl Subscription {
    fn new(account: u64) -> Self {
        // moto: 12 lowercase hex digits formatted xxxx-xxxx-xxxx.
        let hex = Uuid::new_v4().simple().to_string();
        let subscription_arn = format!(
            "arn:aws:shield::{account}:subscription/{}-{}-{}",
            &hex[0..4],
            &hex[4..8],
            &hex[8..12]
        );
        Subscription {
            start_time: shield_time(0),
            end_time: shield_time(SUBSCRIPTION_YEAR_DAYS),
            auto_renew: "ENABLED".to_string(),
            proactive_engagement_status: "ENABLED".to_string(),
            subscription_arn,
        }
    }

    /// moto's dataclass defaults plus the live toggles.
    fn to_dict(&self) -> Value {
        json!({
            "StartTime": self.start_time,
            "EndTime": self.end_time,
            "TimeCommitmentInSeconds": TIME_COMMITMENT_SECONDS,
            "AutoRenew": self.auto_renew,
            "Limits": [{"Type": "MitigationCapacityUnits", "Max": 10000}],
            "ProactiveEngagementStatus": self.proactive_engagement_status,
            "SubscriptionLimits": {
                "ProtectionLimits": {"ProtectedResourceTypeLimits": [
                    {"Type": "ELASTIC_IP_ADDRESS", "Max": 100},
                    {"Type": "APPLICATION_LOAD_BALANCER", "Max": 50},
                ]},
                "ProtectionGroupLimits": {
                    "MaxProtectionGroups": 20,
                    "PatternTypeLimits": {"ArbitraryPatternLimits": {"MaxMembers": 100}},
                },
            },
            "SubscriptionArn": self.subscription_arn,
        })
    }
}

fn shield_time(days_from_now: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::days(days_from_now))
        .format(SUB_TIME_FMT)
        .to_string()
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "shield" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut ShieldStore) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    match req.operation.as_str() {
        // --- subscription -------------------------------------------------
        "CreateSubscription" => {
            st.subscription = Some(Subscription::new(account));
            ok_empty()
        }
        "DescribeSubscription" => match &st.subscription {
            Some(sub) => ok(json!({ "Subscription": sub.to_dict() })),
            None => not_found("The subscription does not exist."),
        },
        "DeleteSubscription" => {
            st.subscription = None;
            ok_empty()
        }
        "UpdateSubscription" => match &mut st.subscription {
            Some(sub) => {
                if let Some(renew) = body.get("AutoRenew").and_then(Value::as_str) {
                    sub.auto_renew = renew.to_string();
                }
                ok_empty()
            }
            None => not_found("The subscription does not exist."),
        },
        "GetSubscriptionState" => ok(json!({
            "SubscriptionState": if st.subscription.is_some() { "ACTIVE" } else { "INACTIVE" }
        })),
        // --- protections ----------------------------------------------------
        "CreateProtection" => create_protection(st, account, &body),
        "DescribeProtection" => describe_protection(st, &body),
        "DeleteProtection" => {
            if st
                .protections
                .remove(as_str(&body, "ProtectionId"))
                .is_some()
            {
                ok_empty()
            } else {
                not_found("The referenced protection does not exist.")
            }
        }
        "ListProtections" => {
            if let Err(resp) = validate_list_protections_filters(&body) {
                return resp;
            }
            let list: Vec<Value> = filtered_protections(st, body.get("InclusionFilters"))
                .into_iter()
                .map(|p| p.to_dict())
                .collect();
            ok(json!({ "Protections": list }))
        }
        // --- protection groups -----------------------------------------------
        "CreateProtectionGroup" => {
            let gid = as_str(&body, "ProtectionGroupId");
            if st.protection_groups.contains_key(gid) {
                return already_exists("The referenced protection group already exists.");
            }
            let pg = ProtectionGroup::new(
                account,
                gid,
                as_str(&body, "Aggregation"),
                as_str(&body, "Pattern"),
                body.get("ResourceType").and_then(Value::as_str),
                as_str_list(body.get("Members")),
            );
            let arn = pg.protection_group_arn.clone();
            st.protection_groups.insert(gid.to_string(), pg);
            // moto: `if tags:` — an absent/empty list never reaches the tagger.
            if let Some(tags) = body.get("Tags").and_then(Value::as_array) {
                if !tags.is_empty() {
                    tag_resource(st, &arn, Some(tags));
                }
            }
            ok_empty()
        }
        "DescribeProtectionGroup" => {
            match st.protection_groups.get(as_str(&body, "ProtectionGroupId")) {
                Some(pg) => ok(json!({ "ProtectionGroup": pg.to_dict() })),
                None => not_found("The referenced protection group does not exist."),
            }
        }
        "UpdateProtectionGroup" => {
            let gid = as_str(&body, "ProtectionGroupId");
            match st.protection_groups.get_mut(gid) {
                Some(pg) => {
                    pg.aggregation = as_str(&body, "Aggregation").to_string();
                    pg.pattern = as_str(&body, "Pattern").to_string();
                    pg.resource_type = body
                        .get("ResourceType")
                        .and_then(Value::as_str)
                        .map(String::from);
                    // moto: `if members is not None:` — null/absent keeps them.
                    if let Some(members) = body.get("Members").filter(|m| !m.is_null()) {
                        pg.members = as_str_list(Some(members));
                    }
                    ok_empty()
                }
                None => not_found("The referenced protection group does not exist."),
            }
        }
        "DeleteProtectionGroup" => {
            let gid = as_str(&body, "ProtectionGroupId");
            if st.protection_groups.remove(gid).is_some() {
                ok_empty()
            } else {
                not_found("The referenced protection group does not exist.")
            }
        }
        "ListProtectionGroups" => {
            let list: Vec<Value> = filtered_groups(st, body.get("InclusionFilters"))
                .into_iter()
                .map(|g| g.to_dict())
                .collect();
            ok(json!({ "ProtectionGroups": list }))
        }
        "ListResourcesInProtectionGroup" => {
            match st.protection_groups.get(as_str(&body, "ProtectionGroupId")) {
                Some(pg) => ok(json!({ "ResourceArns": pg.members })),
                None => not_found("The referenced protection group does not exist."),
            }
        }
        // --- health checks -----------------------------------------------------
        "AssociateHealthCheck" => with_health_check(st, &body, |p| {
            let hc = health_check_id_from_arn(as_str(&body, "HealthCheckArn"));
            if !p.health_check_ids.contains(&hc) {
                p.health_check_ids.push(hc);
            }
            ok_empty()
        }),
        "DisassociateHealthCheck" => with_health_check(st, &body, |p| {
            let hc = health_check_id_from_arn(as_str(&body, "HealthCheckArn"));
            if let Some(pos) = p.health_check_ids.iter().position(|id| *id == hc) {
                p.health_check_ids.remove(pos);
            }
            ok_empty()
        }),
        // --- application-layer automatic response ------------------------------
        "EnableApplicationLayerAutomaticResponse" | "UpdateApplicationLayerAutomaticResponse" => {
            set_automatic_response(
                st,
                as_str(&body, "ResourceArn"),
                Some(json!({
                    "Status": "ENABLED",
                    "Action": body.get("Action").cloned().unwrap_or(json!({})),
                })),
            )
        }
        "DisableApplicationLayerAutomaticResponse" => {
            set_automatic_response(st, as_str(&body, "ResourceArn"), None)
        }
        // --- DRT access ---------------------------------------------------------
        "AssociateDRTLogBucket" => {
            let bucket = as_str(&body, "LogBucket");
            if !bucket.is_empty() && !st.drt_log_buckets.iter().any(|b| b == bucket) {
                st.drt_log_buckets.push(bucket.to_string());
            }
            ok_empty()
        }
        "DisassociateDRTLogBucket" => {
            let bucket = as_str(&body, "LogBucket");
            if let Some(pos) = st.drt_log_buckets.iter().position(|b| b == bucket) {
                st.drt_log_buckets.remove(pos);
            }
            ok_empty()
        }
        "AssociateDRTRole" => {
            st.drt_role_arn = Some(as_str(&body, "RoleArn").to_string());
            ok_empty()
        }
        "DisassociateDRTRole" => {
            st.drt_role_arn = None;
            ok_empty()
        }
        "DescribeDRTAccess" => {
            let mut out = Map::new();
            if let Some(role) = st.drt_role_arn.as_deref().filter(|r| !r.is_empty()) {
                out.insert("RoleArn".into(), json!(role));
            }
            if !st.drt_log_buckets.is_empty() {
                out.insert("LogBucketList".into(), json!(st.drt_log_buckets));
            }
            ok(Value::Object(out))
        }
        // --- emergency contacts / proactive engagement --------------------------
        "UpdateEmergencyContactSettings" => {
            st.emergency_contacts = as_value_list(body.get("EmergencyContactList"));
            ok_empty()
        }
        "DescribeEmergencyContactSettings" => {
            ok(json!({ "EmergencyContactList": st.emergency_contacts }))
        }
        "AssociateProactiveEngagementDetails" => {
            st.emergency_contacts = as_value_list(body.get("EmergencyContactList"));
            ok_empty()
        }
        "EnableProactiveEngagement" => set_proactive_engagement(st, "ENABLED"),
        "DisableProactiveEngagement" => set_proactive_engagement(st, "DISABLED"),
        // --- tags -----------------------------------------------------------------
        "TagResource" => {
            tag_resource(
                st,
                as_str(&body, "ResourceARN"),
                body.get("Tags").and_then(Value::as_array),
            );
            ok_empty()
        }
        "UntagResource" => {
            untag_resource(
                st,
                as_str(&body, "ResourceARN"),
                body.get("TagKeys").and_then(Value::as_array),
            );
            ok_empty()
        }
        "ListTagsForResource" => {
            let tags = st
                .tags
                .get(as_str(&body, "ResourceARN"))
                .map(|row| {
                    Value::Array(
                        row.iter()
                            .map(|(k, v)| json!({ "Key": k, "Value": tag_value(v.as_deref()) }))
                            .collect(),
                    )
                })
                .unwrap_or_else(|| json!([]));
            ok(json!({ "Tags": tags }))
        }
        // --- attacks (moto holds none) ----------------------------------------------
        "DescribeAttack" => not_found("The referenced attack does not exist."),
        "ListAttacks" => ok(json!({ "AttackSummaries": [] })),
        "DescribeAttackStatistics" => {
            let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
            ok(json!({
                "TimeRange": {
                    "FromInclusive": now - (30.0 * 86_400.0),
                    "ToExclusive": now,
                },
                "DataItems": [],
            }))
        }
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

fn create_protection(st: &mut ShieldStore, account: u64, body: &Value) -> Response {
    // moto checks the duplicate before validating the ARN.
    let name = as_str(body, "Name");
    let resource_arn = as_str(body, "ResourceArn");
    if st
        .protections
        .values()
        .any(|p| p.resource_arn == resource_arn)
    {
        return already_exists("The referenced protection already exists.");
    }
    if let Err(resp) = validate_resource_arn(resource_arn) {
        return resp;
    }
    let protection = Protection::new(account, name, resource_arn);
    let protection_id = protection.protection_id.clone();
    let protection_arn = protection.protection_arn.clone();
    tag_resource(
        st,
        &protection_arn,
        body.get("Tags").and_then(Value::as_array),
    );
    st.protections.insert(protection_id.clone(), protection);
    ok(json!({ "ProtectionId": protection_id }))
}

/// moto's `validate_resource_arn`: Shield protects five (service, resource
/// type) pairs; everything else is `InvalidResourceException`.
fn validate_resource_arn(resource_arn: &str) -> Result<(), Response> {
    const VALID: &[(&str, &str)] = &[
        ("elasticloadbalancing", "loadbalancer"),
        ("cloudfront", "distribution"),
        ("globalaccelerator", "accelerator"),
        ("route53", "hostedzone"),
        ("ec2", "eip-allocation"),
    ];
    let parts: Vec<&str> = resource_arn.splitn(6, ':').collect();
    if parts.len() < 6 {
        // moto's parse_arn raises on malformed ARNs; we answer with the
        // missing-resource-type message.
        return Err(invalid_resource(
            "Relative ID must be in the form '<resource>/<id>'.",
        ));
    }
    let service = parts[2];
    let resource = parts[5];
    let colon = resource.find(':');
    let slash = resource.find('/');
    let resource_type = match (colon, slash) {
        (Some(c), Some(s)) => Some(if c < s { c } else { s }),
        (Some(c), None) => Some(c),
        (None, Some(s)) => Some(s),
        (None, None) => None,
    }
    .map(|idx| &resource[..idx]);

    if resource_type.is_some_and(|rt| VALID.contains(&(service, rt))) {
        Ok(())
    } else {
        Err(invalid_resource(&match resource_type {
            Some(rt) => format!("Unrecognized resource '{rt}' of service '{service}'."),
            None => "Relative ID must be in the form '<resource>/<id>'.".to_string(),
        }))
    }
}

fn describe_protection(st: &mut ShieldStore, body: &Value) -> Response {
    let protection_id = as_str(body, "ProtectionId");
    let resource_arn = as_str(body, "ResourceArn");
    if !protection_id.is_empty() && !resource_arn.is_empty() {
        return invalid_parameter(
            "Invalid parameter. You must provide one value, either protectionId or resourceArn, but not both.",
        );
    }
    let found = if !resource_arn.is_empty() {
        st.protections
            .values()
            .find(|p| p.resource_arn == resource_arn)
    } else if !protection_id.is_empty() {
        st.protections.get(protection_id)
    } else {
        // moto checks neither selector → describe_protection returns None →
        // the sidecar crashed (`'NoneType' object has no attribute
        // 'to_dict'`), recorded as 500 InternalError in probes/shield.json.
        return json_error(
            500,
            "InternalError",
            "'NoneType' object has no attribute 'to_dict'",
        );
    };
    match found {
        Some(p) => ok(json!({ "Protection": p.to_dict() })),
        None => not_found("The referenced protection does not exist."),
    }
}

/// Run `f` for the protection named in the body (health checks select by
/// `ProtectionId`), mapping absence to moto's error.
fn with_health_check(
    st: &mut ShieldStore,
    body: &Value,
    f: impl FnOnce(&mut Protection) -> Response,
) -> Response {
    match st.protections.get_mut(as_str(body, "ProtectionId")) {
        Some(p) => f(p),
        None => not_found("The referenced protection does not exist."),
    }
}

fn set_automatic_response(
    st: &mut ShieldStore,
    resource_arn: &str,
    config: Option<Value>,
) -> Response {
    let Some(p) = st
        .protections
        .values_mut()
        .find(|p| p.resource_arn == resource_arn)
    else {
        return not_found("The referenced protection does not exist.");
    };
    p.application_layer_automatic_response = config;
    ok_empty()
}

fn set_proactive_engagement(st: &mut ShieldStore, status: &str) -> Response {
    match &mut st.subscription {
        Some(sub) => {
            sub.proactive_engagement_status = status.to_string();
            ok_empty()
        }
        None => not_found("The subscription does not exist."),
    }
}

/// moto's health-check id: the segment after the final '/'.
fn health_check_id_from_arn(arn: &str) -> String {
    if arn.contains('/') {
        arn.rsplit('/').next().unwrap_or(arn).to_string()
    } else {
        arn.to_string()
    }
}

// ---------------------------------------------------------------------------
// List filters (mirroring moto's python truthiness)
// ---------------------------------------------------------------------------

/// `ListProtections` rejects filter lists with more than one entry.
fn validate_list_protections_filters(body: &Value) -> Result<(), Response> {
    let Some(filters) = body.get("InclusionFilters").filter(|f| truthy(f)) else {
        return Ok(());
    };
    for (key, attr) in [
        ("ResourceArns", "resourceArns"),
        ("ProtectionNames", "protectionNames"),
        ("ResourceTypes", "resourceTypes"),
    ] {
        if let Some(list) = filters.get(key).and_then(Value::as_array) {
            if list.len() > 1 {
                return Err(validation_error(&format!(
                    "Error validating the following inputs: inclusionFilters.{attr}"
                )));
            }
        }
    }
    Ok(())
}

/// moto's list_protections: an AND over the three single-value filters.
/// Mirrors its truthiness quirks — `None`/`{}` filters list everything, a
/// present-but-empty match set drops out of the intersection, and with no
/// applicable filter set at all the result is empty.
fn filtered_protections<'a>(st: &'a ShieldStore, filters: Option<&Value>) -> Vec<&'a Protection> {
    let all: Vec<&'a Protection> = st.protections.values().collect();
    let Some(filters) = filters.filter(|f| truthy(f)) else {
        return all;
    };
    let checks = [
        (
            "ResourceArns",
            (|p: &Protection, v: &str| p.resource_arn == v) as fn(&Protection, &str) -> bool,
        ),
        (
            "ProtectionNames",
            (|p: &Protection, v: &str| p.name == v) as fn(&Protection, &str) -> bool,
        ),
        (
            "ResourceTypes",
            (|p: &Protection, v: &str| p.resource_type == v) as fn(&Protection, &str) -> bool,
        ),
    ];
    let mut filters_applied: Vec<Vec<&'a Protection>> = Vec::new();
    for (key, eq) in checks {
        let Some(list) = filters
            .get(key)
            .and_then(Value::as_array)
            .filter(|l| !l.is_empty())
        else {
            continue;
        };
        let Some(value) = list[0].as_str() else {
            continue;
        };
        let hits: Vec<&'a Protection> = all.iter().copied().filter(|p| eq(p, value)).collect();
        if !hits.is_empty() {
            filters_applied.push(hits);
        }
    }
    if filters_applied.is_empty() {
        return Vec::new();
    }
    all.into_iter()
        .filter(|p| filters_applied.iter().all(|m| m.contains(p)))
        .collect()
}

/// moto's list_protection_groups: independent list filters, each ignored when
/// absent or empty.
fn filtered_groups<'a>(st: &'a ShieldStore, filters: Option<&Value>) -> Vec<&'a ProtectionGroup> {
    let all: Vec<&'a ProtectionGroup> = st.protection_groups.values().collect();
    let Some(filters) = filters.filter(|f| truthy(f)) else {
        return all;
    };
    // A present filter list constrains; absent/empty means "no filter". A
    // group without a ResourceType never matches a ResourceTypes filter.
    let allows = |key: &str, value: Option<&str>| -> bool {
        match filters.get(key).and_then(Value::as_array) {
            None => true,
            Some(list) if list.is_empty() => true,
            Some(list) => match value {
                None => false,
                Some(v) => list.iter().any(|item| item.as_str() == Some(v)),
            },
        }
    };
    all.into_iter()
        .filter(|g| {
            allows("ProtectionGroupIds", Some(&g.protection_group_id))
                && allows("Patterns", Some(&g.pattern))
                && allows("ResourceTypes", g.resource_type.as_deref())
                && allows("Aggregations", Some(&g.aggregation))
        })
        .collect()
}

/// Python truthiness for filter objects: false for null and empty containers.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Tags (moto's TaggingService semantics)
// ---------------------------------------------------------------------------

fn tag_resource(st: &mut ShieldStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        // A member without `Value` stores null, just like moto's tagger.
        row.insert(
            key.to_string(),
            tag.get("Value").and_then(Value::as_str).map(String::from),
        );
    }
}

fn untag_resource(st: &mut ShieldStore, arn: &str, tag_keys: Option<&Vec<Value>>) {
    let Some(keys) = tag_keys else {
        return;
    };
    if let Some(row) = st.tags.get_mut(arn) {
        for key in keys {
            if let Some(name) = key.as_str() {
                row.remove(name);
            }
        }
    }
}

fn tag_value(value: Option<&str>) -> Value {
    match value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

fn already_exists(message: &str) -> Response {
    json_error(400, "ResourceAlreadyExistsException", message)
}

fn invalid_resource(message: &str) -> Response {
    json_error(400, "InvalidResourceException", message)
}

fn invalid_parameter(message: &str) -> Response {
    json_error(400, "InvalidParameterException", message)
}

fn validation_error(message: &str) -> Response {
    json_error(400, "ValidationException", message)
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn as_value_list(value: Option<&Value>) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .map(|l| l.to_vec())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(100_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "shield".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("shield dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn eip_arn(hex: &str) -> String {
        format!("arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-{hex}")
    }

    fn make_protection(account: u64, name: &str, hex: &str) -> (String, String) {
        let resource = eip_arn(hex);
        let (status, body) = send(
            account,
            "CreateProtection",
            json!({ "Name": name, "ResourceArn": resource }),
        );
        assert_eq!(status, 200, "create_protection failed: {body}");
        let id = body["ProtectionId"]
            .as_str()
            .expect("ProtectionId")
            .to_string();
        (id, resource)
    }

    #[test]
    fn protection_lifecycle() {
        let account = next_account();
        let (id, _resource) = make_protection(account, "prot-1", "aabbccdd");

        let (status, body) = send(account, "DescribeProtection", json!({ "ProtectionId": id }));
        assert_eq!(status, 200);
        let prot = &body["Protection"];
        assert_eq!(prot["Id"], json!(id));
        assert_eq!(prot["Name"], "prot-1");
        assert!(prot["ResourceArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:ec2:"));
        assert!(prot["ProtectionArn"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:shield::{account}:protection/")));
        // Untouched protections expose no health checks / automatic response.
        assert!(prot.get("HealthCheckIds").is_none());
        assert!(prot
            .get("ApplicationLayerAutomaticResponseConfiguration")
            .is_none());

        let (status, body) = send(account, "ListProtections", json!({}));
        assert_eq!(status, 200);
        assert!(body["Protections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["Name"] == "prot-1"));

        let (status, _) = send(account, "DeleteProtection", json!({ "ProtectionId": id }));
        assert_eq!(status, 200);
        let (status, body) = send(account, "DescribeProtection", json!({ "ProtectionId": id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // After deletion the resource is available again.
        let (status, _) = send(
            account,
            "CreateProtection",
            json!({ "Name": "prot-2", "ResourceArn": eip_arn("aabbccdd") }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn create_protection_rejects_unknown_resource_types() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateProtection",
            json!({ "Name": "x", "ResourceArn": "arn:aws:shield:us-east-1:123456789012:thing/whatever" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidResourceException");
        assert_eq!(
            body["message"],
            "Unrecognized resource 'thing' of service 'shield'."
        );

        // A bare relative id (no '/' or ':') fails the same way moto does.
        let (status, body) = send(
            account,
            "CreateProtection",
            json!({ "Name": "x", "ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eipalloc-1" }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["message"],
            "Relative ID must be in the form '<resource>/<id>'."
        );
    }

    #[test]
    fn create_protection_rejects_duplicate_resource() {
        let account = next_account();
        let resource = eip_arn("deadbeef");
        let (status, _) = send(
            account,
            "CreateProtection",
            json!({ "Name": "first", "ResourceArn": resource }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "CreateProtection",
            json!({ "Name": "second", "ResourceArn": resource }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceAlreadyExistsException");
    }

    #[test]
    fn describe_protection_selector_rules() {
        let account = next_account();
        // Neither selector keeps moto's unguarded-crash 500.
        let (status, body) = send(account, "DescribeProtection", json!({}));
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");

        // Both selectors together are rejected.
        let (id, resource) = make_protection(account, "both", "abc12345");
        let (status, body) = send(
            account,
            "DescribeProtection",
            json!({ "ProtectionId": id, "ResourceArn": resource }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterException");

        // Unknown ids are not found.
        let (status, body) = send(
            account,
            "DescribeProtection",
            json!({ "ProtectionId": "does-not-exist" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Lookup by ResourceArn works.
        let (status, body) = send(
            account,
            "DescribeProtection",
            json!({ "ResourceArn": resource }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Protection"]["Id"], json!(id));
    }

    #[test]
    fn list_protections_filters() {
        let account = next_account();
        let (id1, arn1) = make_protection(account, "filter-a", "11111111");
        let (_id2, arn2) = make_protection(account, "filter-b", "22222222");

        let (status, body) = send(account, "ListProtections", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Protections"].as_array().unwrap().len(), 2);

        // Single-valued ResourceArns filter.
        let (status, body) = send(
            account,
            "ListProtections",
            json!({ "InclusionFilters": { "ResourceArns": [arn1] } }),
        );
        assert_eq!(status, 200);
        let listed = body["Protections"].as_array().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["Id"], json!(id1));

        // More than one value per filter is a ValidationException.
        let (status, body) = send(
            account,
            "ListProtections",
            json!({ "InclusionFilters": { "ResourceArns": [arn1, arn2] } }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ValidationException");

        // An empty filter object lists everything; a filter object without
        // any known key yields nothing (moto's intersection quirk).
        let (status, body) = send(
            account,
            "ListProtections",
            json!({ "InclusionFilters": {} }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Protections"].as_array().unwrap().len(), 2);
        let (status, body) = send(
            account,
            "ListProtections",
            json!({ "InclusionFilters": { "UnknownKey": "x" } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Protections"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn protection_delete_requires_existence() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DeleteProtection",
            json!({ "ProtectionId": "ghost" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn tag_lifecycle() {
        let account = next_account();
        let (id, _) = make_protection(account, "tagged", "44444444");
        let arn = {
            let (_, body) = send(account, "DescribeProtection", json!({ "ProtectionId": id }));
            body["Protection"]["ProtectionArn"]
                .as_str()
                .unwrap()
                .to_string()
        };

        let (status, _) = send(
            account,
            "TagResource",
            json!({
                "ResourceARN": arn,
                "Tags": [{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
            }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(status, 200);
        let tags: serde_json::Map<String, Value> = body["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| (t["Key"].as_str().unwrap().to_string(), t["Value"].clone()))
            .collect();
        assert_eq!(tags.get("Env"), Some(&json!("test")));
        assert_eq!(tags.get("Team"), Some(&json!("dev")));

        let (status, _) = send(
            account,
            "UntagResource",
            json!({ "ResourceARN": arn, "TagKeys": ["Env"] }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        let keys: Vec<&str> = body["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["Key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["Team"]);

        // Unknown ARNs list empty and never error.
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": "arn:aws:shield::123456789012:protection/none" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"], json!([]));
    }

    #[test]
    fn subscription_lifecycle() {
        let account = next_account();
        // Before any subscription exists.
        let (status, body) = send(account, "DescribeSubscription", json!({}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
        let (_, body) = send(account, "GetSubscriptionState", json!({}));
        assert_eq!(body["SubscriptionState"], "INACTIVE");

        let (status, _) = send(account, "CreateSubscription", json!({}));
        assert_eq!(status, 200);

        let (_, body) = send(account, "DescribeSubscription", json!({}));
        let sub = &body["Subscription"];
        assert_eq!(sub["TimeCommitmentInSeconds"], json!(31_536_000));
        assert_eq!(sub["AutoRenew"], "ENABLED");
        assert_eq!(sub["ProactiveEngagementStatus"], "ENABLED");
        assert!(sub["StartTime"].is_string());
        assert!(sub["EndTime"].is_string());
        assert!(sub["SubscriptionArn"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:shield::{account}:subscription/")));
        assert_eq!(
            sub["Limits"][0],
            json!({"Type": "MitigationCapacityUnits", "Max": 10000})
        );
        assert_eq!(
            sub["SubscriptionLimits"]["ProtectionGroupLimits"]["MaxProtectionGroups"],
            json!(20)
        );

        let (_, body) = send(account, "GetSubscriptionState", json!({}));
        assert_eq!(body["SubscriptionState"], "ACTIVE");

        let (status, _) = send(
            account,
            "UpdateSubscription",
            json!({ "AutoRenew": "DISABLED" }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(account, "DescribeSubscription", json!({}));
        assert_eq!(body["Subscription"]["AutoRenew"], "DISABLED");

        // Proactive engagement toggles require the subscription.
        let (status, _) = send(account, "EnableProactiveEngagement", json!({}));
        assert_eq!(status, 200);
        let (status, _) = send(account, "DisableProactiveEngagement", json!({}));
        assert_eq!(status, 200);
        let (_, body) = send(account, "DescribeSubscription", json!({}));
        assert_eq!(
            body["Subscription"]["ProactiveEngagementStatus"],
            "DISABLED"
        );

        let (status, _) = send(account, "DeleteSubscription", json!({}));
        assert_eq!(status, 200);
        let (_, body) = send(account, "GetSubscriptionState", json!({}));
        assert_eq!(body["SubscriptionState"], "INACTIVE");
        let (status, _) = send(account, "DescribeSubscription", json!({}));
        assert_eq!(status, 400);
        // Updating a missing subscription is an error again.
        let (status, _) = send(
            account,
            "UpdateSubscription",
            json!({ "AutoRenew": "ENABLED" }),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn protection_group_lifecycle() {
        let account = next_account();
        let member = eip_arn("77777777");

        let (status, _) = send(
            account,
            "CreateProtectionGroup",
            json!({
                "ProtectionGroupId": "pg-1",
                "Aggregation": "SUM",
                "Pattern": "ALL",
                "Members": [member],
            }),
        );
        assert_eq!(status, 200);

        // Duplicates are rejected.
        let (status, body) = send(
            account,
            "CreateProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1", "Aggregation": "SUM", "Pattern": "ALL" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceAlreadyExistsException");

        let (_, body) = send(
            account,
            "DescribeProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1" }),
        );
        let pg = &body["ProtectionGroup"];
        assert_eq!(
            pg,
            &json!({
                "ProtectionGroupId": "pg-1",
                "Aggregation": "SUM",
                "Pattern": "ALL",
                "Members": [member],
                "ProtectionGroupArn": format!("arn:aws:shield::{account}:protection-group/pg-1"),
            })
        );

        let (_, body) = send(
            account,
            "ListResourcesInProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1" }),
        );
        assert_eq!(body["ResourceArns"], json!([member]));

        // Update changes aggregation (and keeps members when not passed).
        let (status, _) = send(
            account,
            "UpdateProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1", "Aggregation": "MEAN", "Pattern": "ALL" }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1" }),
        );
        assert_eq!(body["ProtectionGroup"]["Aggregation"], "MEAN");
        assert_eq!(body["ProtectionGroup"]["Members"], json!([member]));

        let (_, body) = send(account, "ListProtectionGroups", json!({}));
        assert!(body["ProtectionGroups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["ProtectionGroupId"] == "pg-1"));

        // Absent/incomplete groups behave moto's way.
        for (op, proto) in [
            (
                "DescribeProtectionGroup",
                "The referenced protection group does not exist.",
            ),
            (
                "UpdateProtectionGroup",
                "The referenced protection group does not exist.",
            ),
            (
                "DeleteProtectionGroup",
                "The referenced protection group does not exist.",
            ),
        ] {
            let (status, body) = send(account, op, json!({ "ProtectionGroupId": "missing" }));
            assert_eq!(status, 400, "{op}: {body}");
            assert_eq!(body["__type"], "ResourceNotFoundException", "{op}");
            assert_eq!(body["message"], proto, "{op}");
        }
        let (status, body) = send(
            account,
            "ListResourcesInProtectionGroup",
            json!({ "ProtectionGroupId": "ghost" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (status, _) = send(
            account,
            "DeleteProtectionGroup",
            json!({ "ProtectionGroupId": "pg-1" }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn protection_group_filtering() {
        let account = next_account();
        send(
            account,
            "CreateProtectionGroup",
            json!({ "ProtectionGroupId": "g1", "Aggregation": "SUM", "Pattern": "ALL" }),
        );
        send(
            account,
            "CreateProtectionGroup",
            json!({
                "ProtectionGroupId": "g2",
                "Aggregation": "MEAN",
                "Pattern": "ARBITRARY",
                "ResourceType": "ELASTIC_IP_ADDRESS",
            }),
        );

        let (_, body) = send(
            account,
            "ListProtectionGroups",
            json!({ "InclusionFilters": { "ProtectionGroupIds": ["g2"] } }),
        );
        let ids: Vec<&str> = body["ProtectionGroups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["ProtectionGroupId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["g2"]);

        let (_, body) = send(
            account,
            "ListProtectionGroups",
            json!({ "InclusionFilters": { "Patterns": ["ARBITRARY"], "Aggregations": ["MEAN"] } }),
        );
        assert_eq!(body["ProtectionGroups"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["ProtectionGroups"][0]["ResourceType"],
            "ELASTIC_IP_ADDRESS"
        );

        // Absent keys mean "no filter".
        let (_, body) = send(
            account,
            "ListProtectionGroups",
            json!({ "InclusionFilters": {} }),
        );
        assert_eq!(body["ProtectionGroups"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn health_check_lifecycle() {
        let account = next_account();
        let (id, _) = make_protection(account, "hc-prot", "66666666");
        let hc_arn = "arn:aws:route53:::healthcheck/12345678-1234-1234-1234-123456789012";

        let (status, body) = send(
            account,
            "AssociateHealthCheck",
            json!({ "ProtectionId": id, "HealthCheckArn": hc_arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));

        let (_, body) = send(account, "DescribeProtection", json!({ "ProtectionId": id }));
        assert_eq!(
            body["Protection"]["HealthCheckIds"],
            json!(["12345678-1234-1234-1234-123456789012"])
        );

        let (status, _) = send(
            account,
            "DisassociateHealthCheck",
            json!({ "ProtectionId": id, "HealthCheckArn": hc_arn }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(account, "DescribeProtection", json!({ "ProtectionId": id }));
        assert!(body["Protection"].get("HealthCheckIds").is_none());

        // Unknown protections are refused on both verbs.
        for op in ["AssociateHealthCheck", "DisassociateHealthCheck"] {
            let (status, body) = send(
                account,
                op,
                json!({ "ProtectionId": "ghost", "HealthCheckArn": hc_arn }),
            );
            assert_eq!(status, 400, "{op}");
            assert_eq!(body["__type"], "ResourceNotFoundException", "{op}");
        }
    }

    #[test]
    fn application_layer_automatic_response_lifecycle() {
        let account = next_account();
        let (_id, resource) = make_protection(account, "alarm", "88888888");

        let (status, _) = send(
            account,
            "EnableApplicationLayerAutomaticResponse",
            json!({ "ResourceArn": resource, "Action": {"Block": {}} }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeProtection",
            json!({ "ResourceArn": resource }),
        );
        assert_eq!(
            body["Protection"]["ApplicationLayerAutomaticResponseConfiguration"],
            json!({"Status": "ENABLED", "Action": {"Block": {}}})
        );

        // Update swaps the action.
        let (status, _) = send(
            account,
            "UpdateApplicationLayerAutomaticResponse",
            json!({ "ResourceArn": resource, "Action": {"Count": {}} }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeProtection",
            json!({ "ResourceArn": resource }),
        );
        assert_eq!(
            body["Protection"]["ApplicationLayerAutomaticResponseConfiguration"]["Action"],
            json!({"Count": {}})
        );

        // Disable removes the configuration from the wire entirely.
        let (status, _) = send(
            account,
            "DisableApplicationLayerAutomaticResponse",
            json!({ "ResourceArn": resource }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "DescribeProtection",
            json!({ "ResourceArn": resource }),
        );
        assert!(body["Protection"]
            .get("ApplicationLayerAutomaticResponseConfiguration")
            .is_none());

        // Unknown resources always 404.
        for op in [
            "EnableApplicationLayerAutomaticResponse",
            "UpdateApplicationLayerAutomaticResponse",
            "DisableApplicationLayerAutomaticResponse",
        ] {
            let (status, body) = send(
                account,
                op,
                json!({ "ResourceArn": eip_arn("ffffffff"), "Action": {"Block": {}} }),
            );
            assert_eq!(status, 400, "{op}");
            assert_eq!(body["__type"], "ResourceNotFoundException", "{op}");
        }
    }

    #[test]
    fn drt_access_lifecycle() {
        let account = next_account();
        // Nothing associated yet — both keys stay absent (moto behavior).
        let (status, body) = send(account, "DescribeDRTAccess", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));

        let (status, _) = send(
            account,
            "AssociateDRTRole",
            json!({ "RoleArn": "arn:aws:iam::123456789012:role/ShieldDRT" }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "AssociateDRTLogBucket",
            json!({ "LogBucket": "my-shield-logs-bucket" }),
        );
        assert_eq!(status, 200);

        let (_, body) = send(account, "DescribeDRTAccess", json!({}));
        assert_eq!(
            body,
            json!({
                "RoleArn": "arn:aws:iam::123456789012:role/ShieldDRT",
                "LogBucketList": ["my-shield-logs-bucket"],
            })
        );

        // Disassociation removes exactly what was disassociated.
        send(
            account,
            "DisassociateDRTLogBucket",
            json!({ "LogBucket": "my-shield-logs-bucket" }),
        );
        send(account, "DisassociateDRTRole", json!({}));
        let (_, body) = send(account, "DescribeDRTAccess", json!({}));
        assert_eq!(body, json!({}));
    }

    #[test]
    fn emergency_contacts_and_proactive_engagement() {
        let account = next_account();

        // Contacts may be stored without a subscription (moto does not care).
        let (status, _) = send(
            account,
            "AssociateProactiveEngagementDetails",
            json!({ "EmergencyContactList": [{"EmailAddress": "security@example.com"}] }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "UpdateEmergencyContactSettings",
            json!({ "EmergencyContactList": [{"EmailAddress": "oncall@example.com"}] }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(account, "DescribeEmergencyContactSettings", json!({}));
        assert_eq!(
            body["EmergencyContactList"],
            json!([{"EmailAddress": "oncall@example.com"}])
        );

        // Engagement toggles without a subscription fail.
        let (status, body) = send(account, "EnableProactiveEngagement", json!({}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
        assert_eq!(body["message"], "The subscription does not exist.");

        send(account, "CreateSubscription", json!({}));
        let (status, _) = send(account, "EnableProactiveEngagement", json!({}));
        assert_eq!(status, 200);
        let (_, body) = send(account, "DescribeSubscription", json!({}));
        assert_eq!(body["Subscription"]["ProactiveEngagementStatus"], "ENABLED");
    }

    #[test]
    fn attack_surface_is_empty() {
        let account = next_account();

        let (status, body) = send(
            account,
            "DescribeAttack",
            json!({ "AttackId": "nonexistent-id" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
        assert_eq!(body["message"], "The referenced attack does not exist.");

        let (status, body) = send(account, "ListAttacks", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["AttackSummaries"], json!([]));

        let (status, body) = send(account, "DescribeAttackStatistics", json!({}));
        assert_eq!(status, 200);
        let from = body["TimeRange"]["FromInclusive"].as_f64().unwrap();
        let to = body["TimeRange"]["ToExclusive"].as_f64().unwrap();
        assert!(from < to, "statistics timerange should ascend");
        assert_eq!(body["DataItems"], json!([]));
    }

    #[test]
    fn unknown_operation_is_not_implemented() {
        let account = next_account();
        let (status, body) = send(account, "NopeOperation", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "s3".to_string(),
            operation: "CreateProtection".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123456789012,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }
}
