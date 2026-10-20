//! EventBridge Scheduler backend — port of moto's `scheduler` module.
//!
//! The 2021-06-30 API is rest-json without an `X-Amz-Target` header:
//! botocore signs a `scheduler` credential scope and the operation comes
//! from method + path, so this module routes on path segments like
//! `pipes` does. State is per-(account, region), mirroring moto's
//! `EventBridgeSchedulerBackend`: schedules live inside schedule groups,
//! a `default` group is seeded at backend creation, tags are a moto
//! `TaggingService`-shaped store keyed by resource ARN, and the two list
//! operations paginate the moto way (base64 JSON `NextToken` naming the
//! item that starts the next page).
//!
//! Wire-shape notes (moto ground truth):
//! - Timestamps are epoch-second floats; `to_dict` keys are always
//!   present, so absent fields serialize as `null`.
//! - `State` defaults to `ENABLED`, `ScheduleExpressionTimezone` to
//!   `UTC`, and every target gains the standard `RetryPolicy` defaults.
//! - ListSchedules' group filter arrives on the query string as
//!   `ScheduleGroup` (the model's locationName), not `GroupName`.
//! - `ScheduleExists` is a 400 `ConflictException` (moto's JsonRESTError
//!   default status, not the real service's 409).
//! - Creating/reading against a missing group key-raises inside moto
//!   (HTTP 500); handled here as the 404 `ResourceNotFoundException`
//!   the same backend builds for the get path.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::proto::{json_error, now_unix, parse_json_body, query_first, query_map, url_decode};
use crate::store::{self, Slot};
use crate::{Request, Response};

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

static SLOT: Lazy<Slot<Backend>> = Lazy::new(|| store::slot::<Backend>("scheduler"));

#[derive(Default)]
struct Backend {
    /// True once the precreated `default` group has been seeded (moto
    /// seeds it at backend `__init__`; deleting it must not respawn it).
    seeded: bool,
    /// Schedule group name -> moto `ScheduleGroup.to_dict()` record.
    groups: BTreeMap<String, Value>,
    /// Full schedule records. moto nests them inside their group; the
    /// flat vec carries GroupName, and a group's records vanish with it.
    schedules: Vec<Value>,
    /// moto TaggingService: ARN -> (key, value) pairs in tag insertion
    /// order; `Value::Null` for tags submitted without a value.
    tags: BTreeMap<String, Vec<(String, Value)>>,
}

// ---------------------------------------------------------------------------
// shape helpers
// ---------------------------------------------------------------------------

fn schedule_arn(account: u64, region: &str, group: &str, name: &str) -> String {
    format!("arn:aws:scheduler:{region}:{account}:schedule/{group}/{name}")
}

fn group_arn(account: u64, region: &str, name: &str) -> String {
    format!("arn:aws:scheduler:{region}:{account}:schedule-group/{name}")
}

fn group_record(name: &str, account: u64, region: &str, created: Value) -> Value {
    json!({
        "Arn": group_arn(account, region, name),
        "CreationDate": created,
        "LastModificationDate": created,
        "Name": name,
        "State": "ACTIVE",
    })
}

/// The retry policy moto injects when a target omits `RetryPolicy`.
fn default_retry_policy() -> Value {
    json!({"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 185})
}

/// moto schedule dict: every key present, `null` when unset. `short` cuts
/// the target to its Arn for ListSchedules summaries.
fn schedule_to_json(schedule: &Value, short: bool) -> Value {
    let mut out = schedule.clone();
    if short {
        out["Target"] = json!({"Arn": out["Target"]["Arn"]});
    }
    out
}

/// `group or "default"` — moto coerces empty/absent group names.
fn group_param(value: Option<&str>) -> &str {
    value.filter(|v| !v.is_empty()).unwrap_or("default")
}

/// `timezone or "UTC"` — moto expands an absent/empty timezone to UTC.
fn inherited_timezone(value: Option<&str>) -> Value {
    json!(value.filter(|t| !t.is_empty()).unwrap_or("UTC"))
}

// ---------------------------------------------------------------------------
// errors (messages verbatim from moto's scheduler exceptions)
// ---------------------------------------------------------------------------

fn conflict_schedule(name: &str) -> Response {
    json_error(
        400,
        "ConflictException",
        &format!("Schedule {name} already exists."),
    )
}

fn not_found_group(name: &str) -> Response {
    json_error(
        404,
        "ResourceNotFoundException",
        &format!("Schedule group {name} does not exist."),
    )
}

fn not_found_schedule(name: &str) -> Response {
    json_error(
        404,
        "ResourceNotFoundException",
        &format!("Schedule {name} does not exist."),
    )
}

fn ok_body(v: &Value) -> Response {
    Response::json_status(200, v)
}

fn empty_ok() -> Response {
    ok_body(&json!({}))
}

// ---------------------------------------------------------------------------
// dispatch — operation comes from method + path (rest-json, no X-Amz-Target)
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "scheduler" {
        return None;
    }
    let slot = state();
    let mut guard = slot.write();
    let backend = guard.entry((req.account, req.region.clone())).or_default();
    if !backend.seeded {
        backend.groups.insert(
            "default".to_string(),
            group_record("default", req.account, &req.region, Value::Null),
        );
        backend.seeded = true;
    }
    Some(handle(backend, req.account, &req.region, req))
}

fn state() -> Slot<Backend> {
    SLOT.clone()
}

fn handle(b: &mut Backend, account: u64, region: &str, req: &Request) -> Response {
    let path = &req.path;
    let query = query_map(&req.query_string);
    let body = parse_json_body(&req.body);
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(url_decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();

    match (req.method.as_str(), segments.as_slice()) {
        ("GET", ["schedules"]) => list_schedules(b, &query),
        ("GET", ["schedule-groups"]) => list_schedule_groups(b, &query),
        ("POST", ["schedules", name]) => create_schedule(b, account, region, name, &body),
        ("GET", ["schedules", name]) => get_schedule(b, name, &query),
        ("PUT", ["schedules", name]) => update_schedule(b, name, &body),
        ("DELETE", ["schedules", name]) => delete_schedule(b, name, &query),
        ("POST", ["schedule-groups", name]) => {
            create_schedule_group(b, account, region, name, &body)
        }
        ("GET", ["schedule-groups", name]) => get_schedule_group(b, name),
        ("DELETE", ["schedule-groups", name]) => delete_schedule_group(b, name),
        ("GET", ["tags", ..]) => match arn_from_path(path) {
            Some(arn) => list_tags_for_resource(b, &arn),
            None => unimplemented(req),
        },
        ("POST", ["tags", ..]) => match arn_from_path(path) {
            Some(arn) => tag_resource(b, &arn, &body),
            None => unimplemented(req),
        },
        ("DELETE", ["tags", ..]) => match arn_from_path(path) {
            Some(arn) => {
                untag_resource(b, &arn, &query.get("TagKeys").cloned().unwrap_or_default())
            }
            None => unimplemented(req),
        },
        _ => unimplemented(req),
    }
}

fn unimplemented(req: &Request) -> Response {
    json_error(
        501,
        "NotImplemented",
        &format!(
            "The {} {} action has not been implemented",
            req.method, req.path
        ),
    )
}

/// The tail after `/tags/`, percent-decoded (botocore percent-encodes the
/// whole ResourceArn URI member; schedule/group names never hit this op).
fn arn_from_path(path: &str) -> Option<String> {
    let idx = path.find("/tags/")?;
    let arn = url_decode(&path[idx + "/tags/".len()..]);
    if arn.is_empty() {
        return None;
    }
    Some(arn)
}

// ---------------------------------------------------------------------------
// schedules
// ---------------------------------------------------------------------------

fn create_schedule(b: &mut Backend, account: u64, region: &str, name: &str, p: &Value) -> Response {
    let group_name = group_param(p.get("GroupName").and_then(Value::as_str));
    let Some(group) = b.groups.get(group_name) else {
        return not_found_group(group_name);
    };
    let group_name = group["Name"].as_str().unwrap_or(group_name).to_string();
    if b.schedules
        .iter()
        .any(|s| s["GroupName"] == json!(group_name) && s["Name"] == json!(name))
    {
        return conflict_schedule(name);
    }
    if let Some(resp) = validate_start_date(p) {
        return resp;
    }
    let mut schedule = json!({
        "Arn": schedule_arn(account, region, &group_name, name),
        "Name": name,
        "GroupName": group_name,
        "Description": Value::Null,
        "ScheduleExpression": Value::Null,
        "ScheduleExpressionTimezone": "UTC",
        "FlexibleTimeWindow": Value::Null,
        "Target": Value::Null,
        "ActionAfterCompletion": Value::Null,
        "State": "ENABLED",
        "KmsKeyArn": Value::Null,
        "StartDate": Value::Null,
        "EndDate": Value::Null,
        "CreationDate": now_unix(),
        "LastModificationDate": now_unix(),
    });
    for key in [
        "Description",
        "ScheduleExpression",
        "FlexibleTimeWindow",
        "ActionAfterCompletion",
        "State",
        "KmsKeyArn",
        "StartDate",
        "EndDate",
    ] {
        if let Some(val) = p.get(key).filter(|v| !v.is_null()) {
            schedule[key] = val.clone();
        }
    }
    // moto: `schedule_expression_timezone or "UTC"` even when the client sends ""
    schedule["ScheduleExpressionTimezone"] =
        inherited_timezone(p.get("ScheduleExpressionTimezone").and_then(Value::as_str));
    // Target arrives with the RetryPolicy defaults moto applies on store
    if let Some(target) = p.get("Target").filter(|t| !t.is_null()) {
        schedule["Target"] = with_retry_policy_defaults(target);
    }
    b.schedules.push(schedule.clone());
    ok_body(&json!({"ScheduleArn": schedule["Arn"]}))
}

/// moto's `Schedule._validate_start_date`: a recurring schedule may not
/// start more than five minutes in the past.
fn validate_start_date(p: &Value) -> Option<Response> {
    let start = p.get("StartDate").cloned().unwrap_or(Value::Null);
    if start.is_null() {
        return None;
    }
    let recurrence = p
        .get("ScheduleExpression")
        .and_then(Value::as_str)
        .map(|expr| expr.contains('*'))
        .unwrap_or(false);
    if !recurrence {
        return None;
    }
    let start = start.as_f64()?;
    if start < now_unix() - 300.0 {
        return Some(json_error(
            400,
            "ValidationException",
            "The StartDate you specify cannot be earlier than 5 minutes ago.",
        ));
    }
    None
}

/// Injects the `RetryPolicy` defaults into a client target (mutating a
/// copy, as moto's `Schedule.validate_target` does).
fn with_retry_policy_defaults(target: &Value) -> Value {
    let mut target = target.clone();
    if let Some(obj) = target.as_object_mut() {
        if !obj.contains_key("RetryPolicy") {
            obj.insert("RetryPolicy".to_string(), default_retry_policy());
        }
    }
    target
}

fn get_schedule(b: &mut Backend, name: &str, query: &BTreeMap<String, Vec<String>>) -> Response {
    let group_name = group_param(query_first(query, "groupName"));
    let Some(group) = b.groups.get(group_name) else {
        return not_found_group(group_name);
    };
    match b
        .schedules
        .iter()
        .find(|s| s["GroupName"] == group["Name"] && s["Name"] == json!(name))
    {
        Some(schedule) => ok_body(&schedule_to_json(schedule, false)),
        None => not_found_schedule(name),
    }
}

fn update_schedule(b: &mut Backend, name: &str, p: &Value) -> Response {
    let group_name = group_param(p.get("GroupName").and_then(Value::as_str));
    let Some(group) = b.groups.get(group_name) else {
        return not_found_group(group_name);
    };
    let Some(schedule) = b
        .schedules
        .iter_mut()
        .find(|s| s["GroupName"] == group["Name"] && s["Name"] == json!(name))
    else {
        return not_found_schedule(name);
    };
    // moto's Schedule.update assigns every field (optional ones clear to
    // None when the request omits them), then stamps a new modification date
    for key in [
        "Description",
        "ScheduleExpression",
        "FlexibleTimeWindow",
        "KmsKeyArn",
        "StartDate",
        "EndDate",
        "State",
    ] {
        match p.get(key) {
            Some(val) => schedule[key] = val.clone(),
            None => schedule[key] = Value::Null,
        }
    }
    let timezone = p.get("ScheduleExpressionTimezone").and_then(Value::as_str);
    schedule["ScheduleExpressionTimezone"] = inherited_timezone(timezone);
    if let Some(target) = p.get("Target").filter(|t| !t.is_null()) {
        schedule["Target"] = with_retry_policy_defaults(target);
    }
    schedule["LastModificationDate"] = json!(now_unix());
    let arn = schedule["Arn"].clone();
    ok_body(&json!({"ScheduleArn": arn}))
}

fn delete_schedule(b: &mut Backend, name: &str, query: &BTreeMap<String, Vec<String>>) -> Response {
    let group_name = group_param(query_first(query, "groupName"));
    let Some(group) = b.groups.get(group_name) else {
        return not_found_group(group_name);
    };
    let before = b.schedules.len();
    b.schedules
        .retain(|s| !(s["GroupName"] == group["Name"] && s["Name"] == json!(name)));
    if before == b.schedules.len() {
        return not_found_schedule(name);
    }
    empty_ok()
}

/// moto sorts schedules new-to-old by creation date (`unique_attribute`
/// arn only feeds the page token).
fn ordered_schedules(b: &Backend) -> Vec<Value> {
    let mut records: Vec<&Value> = b.schedules.iter().collect();
    records.sort_by(|a, c| {
        c["CreationDate"]
            .as_f64()
            .partial_cmp(&a["CreationDate"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    records
        .into_iter()
        .map(|s| schedule_to_json(s, true))
        .collect()
}

fn list_schedules(b: &mut Backend, query: &BTreeMap<String, Vec<String>>) -> Response {
    let group_filter = query_first(query, "ScheduleGroup");
    let state_filter = query_first(query, "State");
    let name_prefix = query_first(query, "NamePrefix");
    let filtered: Vec<Value> = ordered_schedules(b)
        .into_iter()
        .filter(|s| {
            let group_ok = group_filter.is_none() || s["GroupName"].as_str() == group_filter;
            let state_ok = state_filter.is_none() || s["State"].as_str() == state_filter;
            let prefix_ok = name_prefix
                .map(|prefix| s["Name"].as_str().is_some_and(|n| n.starts_with(prefix)))
                .unwrap_or(true);
            group_ok && state_ok && prefix_ok
        })
        .collect();
    let scope = format!(
        "schedules group={:?} state={:?} prefix={:?}",
        group_filter, state_filter, name_prefix
    );
    paginate_page(
        &filtered,
        query_first(query, "MaxResults").and_then(|v| v.parse::<usize>().ok()),
        query_first(query, "NextToken"),
        &scope,
        |item| item["Arn"].as_str().unwrap_or("").to_string(),
        "Schedules",
    )
}

// ---------------------------------------------------------------------------
// schedule groups
// ---------------------------------------------------------------------------

fn create_schedule_group(
    b: &mut Backend,
    account: u64,
    region: &str,
    name: &str,
    p: &Value,
) -> Response {
    let record = group_record(name, account, region, json!(now_unix()));
    b.groups.insert(name.to_string(), record.clone());
    if let Some(tags) = p.get("Tags").and_then(Value::as_array) {
        store_tags(b, record["Arn"].as_str().unwrap_or_default(), tags);
    }
    ok_body(&json!({"ScheduleGroupArn": record["Arn"]}))
}

fn get_schedule_group(b: &mut Backend, name: &str) -> Response {
    match b.groups.get(name) {
        Some(group) => ok_body(group),
        None => not_found_group(name),
    }
}

fn delete_schedule_group(b: &mut Backend, name: &str) -> Response {
    if b.groups.remove(name).is_none() {
        return not_found_group(name);
    }
    // moto holds schedules inside the group object, so they vanish too
    b.schedules.retain(|s| s["GroupName"] != json!(name));
    empty_ok()
}

fn list_schedule_groups(b: &mut Backend, query: &BTreeMap<String, Vec<String>>) -> Response {
    let name_prefix = query_first(query, "NamePrefix");
    // moto: default group first, then newest-to-oldest by creation date
    let mut groups: Vec<&Value> = b
        .groups
        .values()
        .filter(|g| {
            name_prefix
                .map(|p| g["Name"].as_str().is_some_and(|n| n.starts_with(p)))
                .unwrap_or(true)
        })
        .collect();
    groups.sort_by(|a, c| {
        let rank = |g: &Value| g["Name"].as_str() != Some("default");
        if rank(a) != rank(c) {
            return rank(a).cmp(&rank(c));
        }
        // newest-to-oldest within the tiers (default always sorts first)
        let ca = a["CreationDate"].as_f64().unwrap_or(0.0);
        let cc = c["CreationDate"].as_f64().unwrap_or(0.0);
        cc.partial_cmp(&ca).unwrap_or(std::cmp::Ordering::Equal)
    });
    let scope = format!("schedule-groups prefix={:?}", name_prefix);
    paginate_page(
        &groups.into_iter().cloned().collect::<Vec<Value>>(),
        query_first(query, "MaxResults").and_then(|v| v.parse::<usize>().ok()),
        query_first(query, "NextToken"),
        &scope,
        |item| item["Arn"].as_str().unwrap_or("").to_string(),
        "ScheduleGroups",
    )
}

// ---------------------------------------------------------------------------
// tags (moto TaggingService keyed by resource ARN)
// ---------------------------------------------------------------------------

fn list_tags_for_resource(b: &mut Backend, arn: &str) -> Response {
    ok_body(&json!({"Tags": tag_list(b, arn)}))
}

fn tag_resource(b: &mut Backend, arn: &str, p: &Value) -> Response {
    if let Some(tags) = p.get("Tags").and_then(Value::as_array) {
        store_tags(b, arn, tags);
    }
    empty_ok()
}

fn untag_resource(b: &mut Backend, arn: &str, keys: &[String]) -> Response {
    if let Some(existing) = b.tags.get_mut(arn) {
        existing.retain(|(key, _)| !keys.contains(key));
    }
    empty_ok()
}

fn tag_list(b: &mut Backend, arn: &str) -> Vec<Value> {
    b.tags
        .get(arn)
        .map(|tags| {
            tags.iter()
                .map(|(key, value)| json!({"Key": key, "Value": value}))
                .collect()
        })
        .unwrap_or_default()
}

/// moto `TaggingService.tag_resource`: merge — re-setting a key updates the
/// value in place; new keys append at the end.
fn store_tags(b: &mut Backend, arn: &str, tags: &[Value]) {
    let entry = b.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            continue;
        };
        let value = tag.get("Value").cloned().unwrap_or(Value::Null);
        if let Some(slot) = entry.iter_mut().find(|(k, _)| k == key) {
            slot.1 = value;
        } else {
            entry.push((key.to_string(), value));
        }
    }
}

// ---------------------------------------------------------------------------
// pagination — moto Paginator shape: the token is base64 JSON carrying
// `uniqueAttributes` (the item that starts the next page) plus a checksum
// over the non-page parameters (moto hashes its input kwargs; tokens are
// only meaningful within one server run, as is theirs).
// ---------------------------------------------------------------------------

/// FNV-1a, a stable stand-in for moto's salted session hash.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |h, byte| {
        (h ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn encode_token(arn: &str, checksum: u64) -> String {
    B64.encode(json!({"uniqueAttributes": arn, "parameterChecksum": checksum}).to_string())
}

fn decode_token(token: &str) -> Result<(String, u64), Response> {
    let invalid = || json_error(400, "InvalidToken", "Invalid Token: Invalid token");
    let raw = B64.decode(token).ok();
    let parsed: Value = serde_json::from_slice(&raw.ok_or_else(invalid)?).map_err(|_| invalid())?;
    let arn = parsed["uniqueAttributes"]
        .as_str()
        .map_or_else(|| Err(invalid()), |arn| Ok(arn.to_string()))?;
    let checksum = parsed["parameterChecksum"].as_u64().unwrap_or(0);
    Ok((arn, checksum))
}

fn paginate_page(
    filtered: &[Value],
    max_results: Option<usize>,
    next_token: Option<&str>,
    scope: &str,
    token_arn: impl Fn(&Value) -> String,
    result_key: &str,
) -> Response {
    // moto: `max_results if max_results else default` — 0 falls back to 50
    let limit = match max_results {
        Some(0) | None => 50,
        Some(n) => n,
    };
    let checksum = fnv1a(scope.as_bytes());
    let index_start = match next_token.filter(|t| !t.is_empty()) {
        None => 0,
        Some(token) => {
            let (arn, token_checksum) = match decode_token(token) {
                Ok(pair) => pair,
                Err(resp) => return resp,
            };
            if token_checksum != checksum {
                // moto: InvalidToken(f"Input inconsistent with page token: {token}")
                return json_error(
                    400,
                    "InvalidToken",
                    &format!("Invalid Token: Input inconsistent with page token: {token}"),
                );
            }
            match filtered.iter().position(|item| token_arn(item) == arn) {
                Some(idx) => idx,
                None => {
                    return json_error(400, "InvalidToken", "Invalid Token: Resource not found!")
                }
            }
        }
    };
    let index_end = (index_start + limit).min(filtered.len());
    let page: Vec<Value> = filtered[index_start..index_end].to_vec();
    // moto builds the token from the first item *after* the page
    let token = if !page.is_empty() && index_end < filtered.len() {
        Some(encode_token(&token_arn(&filtered[index_end]), checksum))
    } else {
        None
    };
    ok_body(&json!({result_key: page, "NextToken": token}))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> Backend {
        let mut b = Backend::default();
        b.groups.insert(
            "default".to_string(),
            group_record("default", 123456789012, "us-east-1", Value::Null),
        );
        b.seeded = true;
        b
    }

    fn target() -> Value {
        json!({
            "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
            "RoleArn": "arn:aws:iam::123456789012:role/role",
        })
    }

    fn create(b: &mut Backend, name: &str, group: &str) -> Value {
        let resp = create_schedule(
            b,
            123456789012,
            "us-east-1",
            name,
            &json!({
                "GroupName": group,
                "ScheduleExpression": "rate(1 hour)",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": target(),
            }),
        );
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn create_get_update_delete_roundtrip() {
        let mut b = backend();
        let created = create(&mut b, "s1", "default");
        let arn = created["ScheduleArn"].as_str().unwrap().to_string();
        assert_eq!(
            arn,
            "arn:aws:scheduler:us-east-1:123456789012:schedule/default/s1"
        );

        let got = get_schedule(&mut b, "s1", &BTreeMap::new());
        let got: Value = serde_json::from_slice(&got.body).unwrap();
        assert_eq!(got["Name"], json!("s1"));
        assert_eq!(got["GroupName"], json!("default"));
        assert_eq!(got["State"], json!("ENABLED"));
        assert_eq!(got["ScheduleExpressionTimezone"], json!("UTC"));
        assert_eq!(
            got["Target"]["RetryPolicy"],
            json!({"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 185})
        );
        assert_eq!(got["Arn"], json!(arn));

        let upd = json!({
            "ScheduleExpression": "rate(30 minutes)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": target(),
        });
        let updated = update_schedule(&mut b, "s1", &upd);
        assert_eq!(updated.status, 200);
        let got2 = get_schedule(&mut b, "s1", &BTreeMap::new());
        let got2: Value = serde_json::from_slice(&got2.body).unwrap();
        assert_eq!(got2["ScheduleExpression"], json!("rate(30 minutes)"));

        let gone = delete_schedule(&mut b, "s1", &BTreeMap::new());
        assert_eq!(gone.status, 200);
        let again = delete_schedule(&mut b, "s1", &BTreeMap::new());
        assert_eq!(again.status, 404);
    }

    #[test]
    fn create_conflicts_and_missing_group() {
        let mut b = backend();
        create(&mut b, "dup", "default");
        let again = create(&mut b, "dup", "default");
        assert_eq!(again["__type"], json!("ConflictException"));

        let missing = create_schedule(
            &mut b,
            123456789012,
            "us-east-1",
            "out-group",
            &json!({
                "GroupName": "nope",
                "ScheduleExpression": "rate(1 hour)",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": target(),
            }),
        );
        assert_eq!(missing.status, 404);
        let body: Value = serde_json::from_slice(&missing.body).unwrap();
        assert_eq!(body["__type"], json!("ResourceNotFoundException"));
    }

    #[test]
    fn list_filters_and_short_shape() {
        let mut b = backend();
        create(&mut b, "prefix-a", "default");
        create(&mut b, "prefix-b", "default");
        create(&mut b, "other", "default");
        let mut q = BTreeMap::new();
        q.insert("NamePrefix".to_string(), vec!["prefix-".to_string()]);
        let listed = list_schedules(&mut b, &q);
        let listed: Value = serde_json::from_slice(&listed.body).unwrap();
        let names: Vec<&str> = listed["Schedules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"prefix-a"));
        for s in listed["Schedules"].as_array().unwrap() {
            assert_eq!(
                s["Target"],
                json!({"Arn": "arn:aws:sqs:us-east-1:123456789012:queue"})
            );
        }
    }

    #[test]
    fn group_lifecycle_cascades() {
        let mut b = backend();
        let resp = create_schedule_group(&mut b, 123456789012, "us-east-1", "g1", &json!({}));
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["ScheduleGroupArn"].as_str().unwrap().to_string();
        create(&mut b, "keeper", "g1");

        let got = get_schedule_group(&mut b, "g1");
        let got: Value = serde_json::from_slice(&got.body).unwrap();
        assert_eq!(got["State"], json!("ACTIVE"));
        assert_eq!(got["Arn"], json!(arn));

        delete_schedule_group(&mut b, "g1");
        assert_eq!(get_schedule_group(&mut b, "g1").status, 404);
        // moto nests schedules in the group: they are gone with it
        let mut q = BTreeMap::new();
        q.insert("ScheduleGroup".to_string(), vec!["g1".to_string()]);
        let listed = list_schedules(&mut b, &q);
        let listed: Value = serde_json::from_slice(&listed.body).unwrap();
        assert!(listed["Schedules"].as_array().unwrap().is_empty());
        // default group still present and listed first
        let groups = list_schedule_groups(&mut b, &BTreeMap::new());
        let groups: Value = serde_json::from_slice(&groups.body).unwrap();
        assert_eq!(groups["ScheduleGroups"][0]["Name"], json!("default"));
    }

    #[test]
    fn tags_roundtrip() {
        let mut b = backend();
        let created = create(&mut b, "tagged", "default");
        let arn = created["ScheduleArn"].as_str().unwrap().to_string();
        tag_resource(
            &mut b,
            &arn,
            &json!({"Tags": [{"Key": "env", "Value": "test"}]}),
        );
        let tags = list_tags_for_resource(&mut b, &arn);
        let tags: Value = serde_json::from_slice(&tags.body).unwrap();
        assert_eq!(tags["Tags"][0], json!({"Key": "env", "Value": "test"}));
        untag_resource(&mut b, &arn, &["env".to_string()]);
        let tags = list_tags_for_resource(&mut b, &arn);
        let tags: Value = serde_json::from_slice(&tags.body).unwrap();
        assert!(tags["Tags"].as_array().unwrap().is_empty());
        // unknown ARN still answers with an empty tag list
        let tags = list_tags_for_resource(
            &mut b,
            "arn:aws:scheduler:us-east-1:123456789012:schedule-group/none",
        );
        let tags: Value = serde_json::from_slice(&tags.body).unwrap();
        assert!(tags["Tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn pagination_tokens_roundtrip() {
        let mut b = backend();
        for name in ["p1", "p2", "p3"] {
            create(&mut b, name, "default");
        }
        let mut q = BTreeMap::new();
        q.insert("MaxResults".to_string(), vec!["2".to_string()]);
        let page1 = list_schedules(&mut b, &q);
        let page1: Value = serde_json::from_slice(&page1.body).unwrap();
        assert_eq!(page1["Schedules"].as_array().unwrap().len(), 2);
        let token = page1["NextToken"].as_str().unwrap().to_string();

        let mut q2 = q.clone();
        q2.insert("NextToken".to_string(), vec![token.clone()]);
        let page2 = list_schedules(&mut b, &q2);
        let page2: Value = serde_json::from_slice(&page2.body).unwrap();
        assert_eq!(page2["Schedules"].as_array().unwrap().len(), 1);
        assert!(page2["NextToken"].is_null());

        // same token with different filters is rejected, like moto
        let mut q3 = q.clone();
        q3.insert("NextToken".to_string(), vec![token]);
        q3.insert("State".to_string(), vec!["DISABLED".to_string()]);
        assert_eq!(list_schedules(&mut b, &q3).status, 400);
    }

    #[test]
    fn start_date_validation_matches_moto_rule() {
        // a recurring schedule may not start >5min in the past
        let mut b = backend();
        let p = json!({
            "ScheduleExpression": "cron(0 12 * * ? *)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": target(),
            "StartDate": now_unix() - 3600.0,
        });
        let resp = create_schedule(&mut b, 123456789012, "us-east-1", "late", &p);
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["__type"], json!("ValidationException"));
    }
}
