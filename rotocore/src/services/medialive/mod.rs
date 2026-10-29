//! MediaLive backend (REST-JSON).
//!
//! Port of moto's medialive backend as shipped in the deployed fork
//! (`moto/medialive/{models,responses,urls}.py`). Wire bodies mirror moto's
//! `json.dumps` output key-for-key: the channel/input ARN quirk
//! (`arn:{partition}:medialive:channel:{id}` — no region/account segment) is
//! preserved along with moto's create-time field drops (e.g. inputs keep the
//! passed `securityGroups` empty; only updates ever assign it). List
//! operations paginate with moto's token format (base64 json
//! `{"uniqueAttributes": "<arn>"}`) and the default limit is 100. Transient
//! states (CREATING/STARTING/STOPPING/DELETING/UPDATING) resolve on describe
//! exactly like moto's `_resolve_transient_states`. Unrouted shapes return
//! None so the gateway 501s exactly like a request moto never routed.

use crate::proto::{json_error, now_iso, parse_json_body, query_first, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

/// Records for one resource kind: id → stored (output-shaped) object.
type Records = Vec<(String, Value)>;

#[derive(Default)]
struct State {
    channels: Records,
    inputs: Records,
    input_security_groups: Records,
    multiplexes: Records,
    /// key: `{multiplex_id}/{program_name}`
    multiplex_programs: Records,
    clusters: Records,
    networks: Records,
    /// key: `{cluster_id}/{node_id}`
    nodes: Records,
    /// key: `{cluster_id}/{group_id}`
    channel_placement_groups: Records,
    sdi_sources: Records,
    cw_alarm_template_groups: Records,
    cw_alarm_templates: Records,
    eb_rule_template_groups: Records,
    eb_rule_templates: Records,
    signal_maps: Records,
    /// moto's TaggingService: arn → key → value.
    tags: std::collections::HashMap<String, Map<String, Value>>,
    /// channel_id → schedule actions.
    schedules: std::collections::HashMap<String, Vec<Value>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("medialive"));
    SLOT.clone()
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

fn hex_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn not_found(what: String) -> Response {
    // moto's NotFoundException: JsonRESTError with code 404.
    json_error(404, "NotFoundException", &what)
}

// --- python `or` fallbacks on request body fields --------------------------

/// A member passed through as-is (`Channel.destinations = kwargs.get(...)`):
/// absent or explicit null stay null.
fn passthrough(params: &Value, key: &str) -> Value {
    params.get(key).cloned().unwrap_or(Value::Null)
}

/// `value or []` — also collapses null to a list (python truthiness).
fn or_empty_list(params: &Value, key: &str) -> Value {
    match params.get(key).and_then(Value::as_array) {
        Some(v) if !v.is_empty() => json!(v),
        _ => json!([]),
    }
}

/// `value or {}` (an empty mapping is falsy in python — same result either way).
fn or_empty_object(params: &Value, key: &str) -> Value {
    match params.get(key).and_then(Value::as_object) {
        Some(v) if !v.is_empty() => json!(v),
        _ => json!({}),
    }
}

/// `value or default` for strings (`channel_class or "STANDARD"`).
fn or_str(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn store_tags(
    tags: &mut std::collections::HashMap<String, Map<String, Value>>,
    arn: &str,
    given: Option<&Value>,
) {
    if let Some(map) = given.and_then(Value::as_object) {
        if !map.is_empty() {
            let entry = tags.entry(arn.to_string()).or_default();
            for (k, v) in map {
                entry.insert(k.clone(), v.clone());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Path handling — moto binds `[^/.]+` for id segments (urls.py) and `.+` for
// the tags ARN. A segment containing `.` would fail that bind and falls
// through to the gateway's 501, like a request moto never routed.
// ---------------------------------------------------------------------------

struct Seg {
    text: String,
    /// False when the decoded segment contains `.` or `/` — moto bind failure.
    bindable: bool,
}

fn split_path(path: &str) -> Option<Vec<Seg>> {
    let trimmed = path.strip_prefix('/')?;
    let mut segs = Vec::new();
    for raw in trimmed.split('/') {
        if raw.is_empty() {
            return None;
        }
        let text = url_decode(raw);
        let bindable = !text.contains('.') && !text.contains('/');
        segs.push(Seg { text, bindable });
    }
    Some(segs)
}

fn find<'a>(records: &'a Records, key: &str) -> Option<&'a Value> {
    records.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn find_mut<'a>(records: &'a mut Records, key: &str) -> Option<&'a mut Value> {
    records.iter_mut().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn take(records: &mut Records, key: &str) -> Option<Value> {
    let pos = records.iter().position(|(k, _)| *k == key)?;
    let (_, v) = records.remove(pos);
    Some(v)
}

// ---------------------------------------------------------------------------
// Transient state resolution (moto's _resolve_transient_states).
// ---------------------------------------------------------------------------

/// Channel/Multiplex: CREATING/STOPPING → IDLE, STARTING → RUNNING, DELETING →
/// DELETED, UPDATING → the state held before the update. Input: CREATING →
/// DETACHED, DELETING → DELETED. The other kinds swap CREATING → IDLE and
/// DELETING → DELETED.
fn resolve_transient(obj: &mut Value, kind: &str) {
    let mut next: Option<String> = None;
    match kind {
        "channel" | "multiplex" => match obj["state"].as_str().unwrap_or("") {
            "CREATING" | "STOPPING" => next = Some("IDLE".into()),
            "STARTING" => next = Some("RUNNING".into()),
            "DELETING" => next = Some("DELETED".into()),
            "UPDATING" => next = obj["previousState"].as_str().map(String::from),
            _ => {}
        },
        "input" => match obj["state"].as_str().unwrap_or("") {
            "CREATING" => next = Some("DETACHED".into()),
            "DELETING" => next = Some("DELETED".into()),
            _ => {}
        },
        _ => match obj["state"].as_str().unwrap_or("") {
            "CREATING" => next = Some("IDLE".into()),
            "DELETING" => next = Some("DELETED".into()),
            _ => {}
        },
    }
    if let Some(next) = next {
        obj["state"] = json!(next);
        obj.as_object_mut().unwrap().remove("previousState");
    }
}

/// Wire `Channel.to_dict()` — `pipelinesRunningCount` derives from the class;
/// the internal `previousState` bookkeeping key never reaches the wire.
fn channel_full(c: &Value) -> Value {
    let mut out = c.clone();
    let class = c["channelClass"].as_str().unwrap_or("STANDARD");
    out["pipelinesRunningCount"] = json!(if class == "SINGLE_PIPELINE" { 1 } else { 2 });
    out.as_object_mut().unwrap().remove("previousState");
    out
}

fn channel_ex(c: &Value, exclude: &[&str]) -> Value {
    let mut out = channel_full(c);
    for key in exclude {
        out.as_object_mut().unwrap().remove(*key);
    }
    out
}

fn full_arn(req: &Request, kind: &str, id: &str) -> String {
    format!(
        "arn:{}:medialive:{}:{}:{}:{}",
        partition(&req.region),
        req.region,
        req.account,
        kind,
        id
    )
}

// ---------------------------------------------------------------------------
// Pagination — moto's PaginatedResult tokens are botocore-encoded
// `{"uniqueAttributes": "<arn>"}` dicts (base64 of json, no checksum), the
// default limit is 100, and an unresolvable token is moto's InvalidToken.
// ---------------------------------------------------------------------------

const PAGINATE_DEFAULT: usize = 100;

fn encode_token(arn: &str) -> String {
    B64.encode(serde_json::to_vec(&json!({ "uniqueAttributes": arn })).unwrap_or_default())
}

fn invalid_token() -> Response {
    json_error(400, "InvalidToken", "Invalid Token: Resource not found!")
}

fn token_start_index(arns: &[String], token: &str) -> usize {
    let decoded = B64
        .decode(token)
        .ok()
        .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
        .and_then(|v| v["uniqueAttributes"].as_str().map(String::from));
    match decoded {
        Some(marker) if !marker.is_empty() => {
            arns.iter().position(|a| *a == marker).unwrap_or(usize::MAX)
        }
        _ => usize::MAX,
    }
}

fn paginated_result(
    arns: Vec<String>,
    items: Vec<Value>,
    max_results: Option<&str>,
    next_token: Option<&str>,
    key: &str,
) -> Response {
    let limit = max_results
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(PAGINATE_DEFAULT);
    let start = match next_token {
        Some(token) => {
            let idx = token_start_index(&arns, token);
            if idx == usize::MAX {
                return invalid_token();
            }
            idx
        }
        None => 0,
    };
    let end = (start + limit).min(items.len());
    let page: Vec<Value> = items[start..end].to_vec();
    let token = if !page.is_empty() && end < items.len() {
        Some(encode_token(&arns[end]))
    } else {
        None
    };
    ok_json(&json!({ key: page, "nextToken": token }))
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "medialive" {
        return None;
    }
    let segs = split_path(&req.path)?;
    let slot = state();
    let mut guard = slot.write();
    guard.entry((req.account, req.region.clone())).or_default();
    let st = guard.get_mut(&(req.account, req.region.clone()))?;
    route(st, req, &segs)
}

fn route(st: &mut State, req: &Request, segs: &[Seg]) -> Option<Response> {
    let segs1 = segs.first()?;
    if segs1.text != "prod" || segs.len() == 1 {
        return None;
    }
    let rest = &segs[1..];
    let method = req.method.as_str();
    let body = || parse_json_body(&req.body);
    let query = query_map(&req.query_string);

    match (rest[0].text.as_str(), rest.len()) {
        // ---- Channels ----
        ("channels", 1) => match method {
            "POST" => Some(create_channel(st, &body(), req)),
            "GET" => Some(list_channels(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("channels", 2) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match method {
                "GET" => Some(describe_channel(st, id)),
                "DELETE" => Some(delete_channel(st, id)),
                "PUT" => Some(update_channel(st, id, &body())),
                _ => None,
            }
        }
        ("channels", 3) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match (method, rest[2].text.as_str()) {
                ("POST", "start") => Some(with_channel(st, id, |c| {
                    c["state"] = json!("STARTING");
                    ok_json(&channel_full(c))
                })),
                ("POST", "stop") => Some(with_channel(st, id, |c| {
                    c["state"] = json!("STOPPING");
                    ok_json(&channel_full(c))
                })),
                ("POST", "restartChannelPipelines") => {
                    Some(with_channel(st, id, |c| ok_json(&channel_full(c))))
                }
                ("PUT", "channelClass") => Some(update_channel_class(st, id, &body())),
                ("PUT", "schedule") => Some(batch_update_schedule(st, id, &body())),
                ("DELETE", "schedule") => Some(delete_schedule(st, id)),
                ("GET", "schedule") => Some(describe_schedule(st, id)),
                ("GET", "thumbnails") => Some(describe_thumbnails(st, id)),
                ("GET", "alerts") => Some(ok_json(&json!({"alerts": [], "nextToken": null}))),
                _ => None,
            }
        }
        // ---- Inputs ----
        ("inputs", 1) => match method {
            "POST" => Some(create_input(st, &body(), req, None)),
            "GET" => Some(list_inputs(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("inputs", 2) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match method {
                "GET" => Some(describe_input(st, id)),
                "DELETE" => Some(delete_input(st, id)),
                "PUT" => Some(update_input(st, id, &body())),
                _ => None,
            }
        }
        ("inputs", 3) if rest[1].bindable && rest[2].text == "partners" && method == "POST" => {
            Some(create_input(st, &body(), req, Some(rest[1].text.as_str())))
        }
        // ---- Input security groups ----
        ("inputSecurityGroups", 1) => match method {
            "POST" => Some(create_input_security_group(st, &body(), req)),
            "GET" => Some(list_input_security_groups(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("inputSecurityGroups", 2) if rest[1].bindable => {
            let id = rest[1].text.to_string();
            match method {
                "GET" => {
                    if let Some(g) = find(&st.input_security_groups, &id) {
                        Some(ok_json(&g.clone()))
                    } else {
                        Some(not_found(format!("InputSecurityGroup {id} not found")))
                    }
                }
                "DELETE" => {
                    if take(&mut st.input_security_groups, &id).is_some() {
                        Some(ok_json(&json!({})))
                    } else {
                        Some(not_found(format!("InputSecurityGroup {id} not found")))
                    }
                }
                "PUT" => Some(update_input_security_group(st, &id, &body())),
                _ => None,
            }
        }
        // ---- Multiplexes ----
        ("multiplexes", 1) => match method {
            "POST" => Some(create_multiplex(st, &body(), req)),
            "GET" => Some(list_multiplexes(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("multiplexes", 2) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match method {
                "GET" => Some(describe_multiplex(st, id)),
                "DELETE" => Some(delete_multiplex(st, id)),
                "PUT" => Some(update_multiplex(st, id, &body())),
                _ => None,
            }
        }
        ("multiplexes", 3) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match (method, rest[2].text.as_str()) {
                ("POST", "start") => Some(multiplex_state(st, id, "STARTING")),
                ("POST", "stop") => Some(multiplex_state(st, id, "STOPPING")),
                ("GET", "programs") => Some(list_multiplex_programs(st, id)),
                ("POST", "programs") => Some(create_multiplex_program(st, id, &body())),
                ("GET", "alerts") => Some(list_multiplex_alerts(st, id)),
                _ => None,
            }
        }
        ("multiplexes", 4)
            if rest[1].bindable && rest[2].text == "programs" && rest[3].bindable =>
        {
            let id = rest[1].text.as_str();
            let name = rest[3].text.as_str();
            match method {
                "GET" => Some(describe_multiplex_program(st, id, name)),
                "DELETE" => Some(delete_multiplex_program(st, id, name)),
                "PUT" => Some(update_multiplex_program(st, id, name, &body())),
                _ => None,
            }
        }
        // ---- Clusters ----
        ("clusters", 1) => match method {
            "POST" => Some(create_cluster(st, &body(), req)),
            "GET" => Some(list_clusters(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("clusters", 2) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match method {
                "GET" => Some(with_cluster(st, id, |c| {
                    resolve_transient(c, "cluster");
                    ok_json(&c.clone())
                })),
                "DELETE" => Some(delete_cluster(st, id)),
                "PUT" => Some(update_cluster(st, id, &body())),
                _ => None,
            }
        }
        ("clusters", 3) if rest[1].bindable => {
            let cluster_id = rest[1].text.as_str();
            match (method, rest[2].text.as_str()) {
                ("GET", "alerts") => Some(list_cluster_alerts(st, cluster_id)),
                ("GET", "nodes") => {
                    if cluster_missing(st, cluster_id) {
                        return Some(not_found(format!("Cluster {cluster_id} not found")));
                    }
                    let nodes = composite_items(&st.nodes, cluster_id);
                    Some(ok_json(&json!({"nodes": nodes, "nextToken": null})))
                }
                ("POST", "nodes") => Some(create_node(st, cluster_id, &body(), req)),
                ("GET", "channelplacementgroups") => {
                    Some(list_channel_placement_groups(st, cluster_id))
                }
                ("POST", "channelplacementgroups") => {
                    Some(create_channel_placement_group(st, cluster_id, &body(), req))
                }
                ("POST", "nodeRegistrationScript") => {
                    Some(create_node_registration_script(st, cluster_id))
                }
                _ => None,
            }
        }
        ("clusters", 4) if rest[1].bindable && rest[2].bindable && rest[3].bindable => {
            let cluster_id = rest[1].text.as_str();
            let sub_id = rest[3].text.as_str();
            match (method, rest[2].text.as_str()) {
                ("GET", "nodes") => Some(describe_node(st, cluster_id, sub_id)),
                ("DELETE", "nodes") => Some(delete_node(st, cluster_id, sub_id)),
                ("PUT", "nodes") => Some(update_node(st, cluster_id, sub_id, &body())),
                ("GET", "channelplacementgroups") => {
                    Some(describe_channel_placement_group(st, cluster_id, sub_id))
                }
                ("DELETE", "channelplacementgroups") => {
                    Some(delete_channel_placement_group(st, cluster_id, sub_id))
                }
                ("PUT", "channelplacementgroups") => Some(update_channel_placement_group(
                    st,
                    cluster_id,
                    sub_id,
                    &body(),
                )),
                _ => None,
            }
        }
        ("clusters", 5)
            if rest[1].bindable
                && rest[2].text == "nodes"
                && rest[3].bindable
                && rest[4].text == "state"
                && method == "PUT" =>
        {
            Some(update_node_state(
                st,
                rest[1].text.as_str(),
                rest[3].text.as_str(),
                &body(),
            ))
        }
        // ---- Networks ----
        ("networks", 1) => match method {
            "POST" => Some(create_network(st, &body(), req)),
            "GET" => Some(list_networks(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("networks", 2) if rest[1].bindable => {
            let id = rest[1].text.as_str();
            match method {
                "GET" => Some(with_network(st, id, |n| {
                    resolve_transient(n, "network");
                    ok_json(&n.clone())
                })),
                "DELETE" => Some(delete_network(st, id)),
                "PUT" => Some(update_network(st, id, &body())),
                _ => None,
            }
        }
        // ---- SDI sources ----
        ("sdiSources", 1) => match method {
            "POST" => Some(create_sdi_source(st, &body(), req)),
            "GET" => Some(list_sdi_sources(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("sdiSources", 2) if rest[1].bindable => {
            let id = rest[1].text.to_string();
            match method {
                "GET" => {
                    if let Some(s) = find(&st.sdi_sources, &id) {
                        Some(ok_json(&s.clone()))
                    } else {
                        Some(not_found(format!("SdiSource {id} not found")))
                    }
                }
                "DELETE" => {
                    if take(&mut st.sdi_sources, &id).is_some() {
                        Some(ok_json(&json!({})))
                    } else {
                        Some(not_found(format!("SdiSource {id} not found")))
                    }
                }
                "PUT" => Some(update_sdi_source(st, &id, &body())),
                _ => None,
            }
        }
        // ---- CloudWatch alarm template groups ----
        ("cloudwatch-alarm-template-groups", 1) => match method {
            "POST" => Some(create_cw_alarm_template_group(st, &body(), req)),
            "GET" => Some(list_cw_alarm_template_groups(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("cloudwatch-alarm-template-groups", 2) if rest[1].bindable => {
            let identifier = rest[1].text.as_str();
            match method {
                "GET" => Some(get_cw_alarm_template_group(st, identifier)),
                "DELETE" => Some(delete_cw_alarm_template_group(st, identifier)),
                "PATCH" => Some(update_cw_alarm_template_group(st, identifier, &body())),
                _ => None,
            }
        }
        // ---- CloudWatch alarm templates ----
        ("cloudwatch-alarm-templates", 1) => match method {
            "POST" => Some(create_cw_alarm_template(st, &body(), req)),
            "GET" => Some(list_cw_alarm_templates(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("cloudwatch-alarm-templates", 2) if rest[1].bindable => {
            let identifier = rest[1].text.as_str();
            match method {
                "GET" => Some(get_cw_alarm_template(st, identifier)),
                "DELETE" => Some(delete_cw_alarm_template(st, identifier)),
                "PATCH" => Some(update_cw_alarm_template(st, identifier, &body())),
                _ => None,
            }
        }
        // ---- EventBridge rule template groups ----
        ("eventbridge-rule-template-groups", 1) => match method {
            "POST" => Some(create_eb_rule_template_group(st, &body(), req)),
            "GET" => Some(list_eb_rule_template_groups(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("eventbridge-rule-template-groups", 2) if rest[1].bindable => {
            let identifier = rest[1].text.as_str();
            match method {
                "GET" => Some(get_eb_rule_template_group(st, identifier)),
                "DELETE" => Some(delete_eb_rule_template_group(st, identifier)),
                "PATCH" => Some(update_eb_rule_template_group(st, identifier, &body())),
                _ => None,
            }
        }
        // ---- EventBridge rule templates ----
        ("eventbridge-rule-templates", 1) => match method {
            "POST" => Some(create_eb_rule_template(st, &body(), req)),
            "GET" => Some(list_eb_rule_templates(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("eventbridge-rule-templates", 2) if rest[1].bindable => {
            let identifier = rest[1].text.as_str();
            match method {
                "GET" => Some(get_eb_rule_template(st, identifier)),
                "DELETE" => Some(delete_eb_rule_template(st, identifier)),
                "PATCH" => Some(update_eb_rule_template(st, identifier, &body())),
                _ => None,
            }
        }
        // ---- Signal maps ----
        ("signal-maps", 1) => match method {
            "POST" => Some(create_signal_map(st, &body(), req)),
            "GET" => Some(list_signal_maps(
                st,
                query_first(&query, "maxResults"),
                query_first(&query, "nextToken"),
            )),
            _ => None,
        },
        ("signal-maps", 2) if rest[1].bindable => {
            let identifier = rest[1].text.as_str();
            match method {
                "GET" => Some(get_signal_map(st, identifier)),
                "DELETE" => Some(delete_signal_map(st, identifier)),
                "PATCH" => Some(start_update_signal_map(st, identifier, &body())),
                _ => None,
            }
        }
        ("signal-maps", 3) if rest[1].bindable && rest[2].text == "monitor-deployment" => {
            let identifier = rest[1].text.as_str();
            match method {
                "POST" => Some(start_monitor_deployment(st, identifier, &body())),
                "DELETE" => Some(start_delete_monitor_deployment(st, identifier)),
                _ => None,
            }
        }
        // ---- Tags (ARN path may contain dots: moto binds `.+`). ----
        ("tags", 2) => tag_ops(st, req, &rest[1].text, method),
        // ---- Batch operations ----
        ("batch", 2) => match (method, rest[1].text.as_str()) {
            ("POST", "delete") => Some(batch_delete(st, &body())),
            ("POST", "start") => Some(batch_start(st, &body())),
            ("POST", "stop") => Some(batch_stop(st, &body())),
            _ => None,
        },
        // ---- Account configuration ----
        ("accountConfiguration", 1) => match method {
            "GET" => Some(ok_json(
                &json!({"accountConfiguration": {"kmsKeyId": null}}),
            )),
            "PUT" => Some(ok_json(&json!({
                "accountConfiguration": body()
                    .get("accountConfiguration")
                    .cloned()
                    .unwrap_or_else(|| json!({"kmsKeyId": null})),
            }))),
            _ => None,
        },
        // ---- Offerings / reservations (moto stubs) ----
        ("offerings", 1) if method == "GET" => {
            Some(ok_json(&json!({"offerings": [], "nextToken": null})))
        }
        ("offerings", 2) if rest[1].bindable && method == "GET" => {
            let id = rest[1].text.clone();
            Some(not_found(format!("Offering {id} not found")))
        }
        ("offerings", 3) if rest[1].bindable && rest[2].text == "purchase" && method == "POST" => {
            let id = rest[1].text.clone();
            Some(not_found(format!("Offering {id} not found")))
        }
        ("reservations", 1) if method == "GET" => {
            Some(ok_json(&json!({"reservations": [], "nextToken": null})))
        }
        ("reservations", 2) if rest[1].bindable => {
            let id = rest[1].text.clone();
            match method {
                "GET" | "DELETE" | "PUT" => Some(not_found(format!("Reservation {id} not found"))),
                _ => None,
            }
        }
        // ---- Input devices (moto stubs: 404 unless list) ----
        ("inputDevices", 1) if method == "GET" => {
            Some(ok_json(&json!({"inputDevices": [], "nextToken": null})))
        }
        ("inputDevices", 2) if rest[1].bindable => {
            let id = rest[1].text.clone();
            match method {
                "GET" | "PUT" => Some(device_not_found(&id)),
                _ => None,
            }
        }
        ("inputDevices", 3) if rest[1].bindable => {
            let id = rest[1].text.clone();
            match (method, rest[2].text.as_str()) {
                ("GET", "thumbnailData") => Some(device_not_found(&id)),
                (
                    "POST",
                    "accept"
                    | "cancel"
                    | "reject"
                    | "reboot"
                    | "start"
                    | "stop"
                    | "startInputDeviceMaintenanceWindow"
                    | "transfer",
                ) => Some(device_not_found(&id)),
                _ => None,
            }
        }
        ("inputDeviceTransfers", 1) if method == "GET" => Some(ok_json(
            &json!({"inputDeviceTransfers": [], "nextToken": null}),
        )),
        // ---- Misc ----
        ("claimDevice", 1) if method == "POST" => Some(ok_json(&json!({}))),
        ("versions", 1) if method == "GET" => Some(ok_json(&json!({
            "versions": [{"engineVersions": [{"version": "1.0"}]}]
        }))),
        _ => None,
    }
}

/// Composite rows (`{cluster_id}/{node_id}` and friends) for one cluster.
fn composite_items(records: &Records, cluster_id: &str) -> Vec<Value> {
    records
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{cluster_id}/")))
        .map(|(_, v)| v.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

fn create_channel(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    // moto quirk: channel ARNs carry no region/account segment.
    let arn = format!("arn:{}:medialive:channel:{}", partition(&req.region), id);
    let channel = json!({
        "arn": arn.clone(),
        "cdiInputSpecification": passthrough(params, "cdiInputSpecification"),
        "channelClass": or_str(params, "channelClass", "STANDARD"),
        "destinations": passthrough(params, "destinations"),
        "egressEndpoints": [],
        "encoderSettings": passthrough(params, "encoderSettings"),
        "id": id.clone(),
        "inputAttachments": passthrough(params, "inputAttachments"),
        "inputSpecification": passthrough(params, "inputSpecification"),
        "logLevel": passthrough(params, "logLevel"),
        "name": passthrough(params, "name"),
        "pipelineDetails": [],
        "roleArn": passthrough(params, "roleArn"),
        "state": "CREATING",
        "tags": passthrough(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.channels.push((id.clone(), channel));
    with_channel(st, &id, |c| {
        ok_json(&json!({"channel": channel_ex(c, &["pipelinesRunningCount"])}))
    })
}

fn with_channel(st: &mut State, id: &str, f: impl FnOnce(&mut Value) -> Response) -> Response {
    match find_mut(&mut st.channels, id) {
        Some(c) => f(c),
        None => not_found(format!("Channel {id} not found")),
    }
}

fn describe_channel(st: &mut State, id: &str) -> Response {
    with_channel(st, id, |c| {
        resolve_transient(c, "channel");
        ok_json(&channel_full(c))
    })
}

fn delete_channel(st: &mut State, id: &str) -> Response {
    with_channel(st, id, |c| {
        c["state"] = json!("DELETING");
        ok_json(&channel_full(c))
    })
}

fn update_channel(st: &mut State, id: &str, params: &Value) -> Response {
    with_channel(st, id, |c| {
        for key in [
            "cdiInputSpecification",
            "destinations",
            "encoderSettings",
            "inputAttachments",
            "inputSpecification",
            "logLevel",
            "name",
            "roleArn",
        ] {
            c[key] = passthrough(params, key);
        }
        resolve_transient(c, "channel");
        let current = c["state"].as_str().unwrap_or("IDLE").to_string();
        c["previousState"] = json!(current);
        c["state"] = json!("UPDATING");
        ok_json(&json!({"channel": channel_full(c)}))
    })
}

fn update_channel_class(st: &mut State, id: &str, params: &Value) -> Response {
    with_channel(st, id, |c| {
        // moto assigns the class unconditionally (None allowed); destinations
        // only when present.
        c["channelClass"] = passthrough(params, "channelClass");
        if let Some(destinations) = params.get("destinations") {
            c["destinations"] = destinations.clone();
        }
        resolve_transient(c, "channel");
        let current = c["state"].as_str().unwrap_or("IDLE").to_string();
        c["previousState"] = json!(current);
        c["state"] = json!("UPDATING");
        ok_json(&json!({"channel": channel_full(c)}))
    })
}

fn list_channels(st: &mut State, max_results: Option<&str>, next_token: Option<&str>) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, c) in &st.channels {
        arns.push(c["arn"].as_str().unwrap_or_default().to_string());
        items.push(channel_ex(c, &["encoderSettings", "pipelineDetails"]));
    }
    paginated_result(arns, items, max_results, next_token, "channels")
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

fn create_input(
    st: &mut State,
    params: &Value,
    req: &Request,
    parent_id: Option<&str>,
) -> Response {
    let id = hex_id();
    // moto quirk: input ARNs carry no region/account segment.
    let arn = format!("arn:{}:medialive:input:{}", partition(&req.region), id);
    let input = if let Some(parent_id) = parent_id {
        let Some(parent) = find(&st.inputs, parent_id) else {
            return not_found(format!("Input {parent_id} not found"));
        };
        json!({
            "arn": arn.clone(),
            "attachedChannels": [],
            "destinations": parent["destinations"].clone(),
            "id": id.clone(),
            "inputClass": "STANDARD",
            "inputDevices": json!([]),
            "inputPartnerIds": json!([parent_id]),
            "inputSourceType": "STATIC",
            "mediaConnectFlows": json!([]),
            "name": json!(format!(
                "{}-partner",
                parent["name"].as_str().unwrap_or_default()
            )),
            "roleArn": parent["roleArn"].clone(),
            "securityGroups": [],
            "sources": json!([]),
            "state": "CREATING",
            "tags": or_empty_object(params, "tags"),
            "type": parent["type"].clone(),
        })
    } else {
        json!({
            "arn": arn.clone(),
            "attachedChannels": [],
            "destinations": passthrough(params, "destinations"),
            "id": id.clone(),
            "inputClass": "STANDARD",
            "inputDevices": passthrough(params, "inputDevices"),
            "inputPartnerIds": json!([]),
            "inputSourceType": "STATIC",
            "mediaConnectFlows": passthrough(params, "mediaConnectFlows"),
            "name": passthrough(params, "name"),
            "roleArn": passthrough(params, "roleArn"),
            "securityGroups": [],
            "sources": passthrough(params, "sources"),
            "state": "CREATING",
            "tags": passthrough(params, "tags"),
            "type": passthrough(params, "type"),
        })
    };
    store_tags(&mut st.tags, &arn, params.get("tags"));
    if let Some(parent_id) = parent_id {
        if let Some(parent) = find_mut(&mut st.inputs, parent_id) {
            if let Some(arr) = parent["inputPartnerIds"].as_array_mut() {
                arr.push(json!(id.clone()));
            }
        }
    }
    st.inputs.push((id.clone(), input.clone()));
    ok_json(&json!({ "input": input }))
}

fn with_input(st: &mut State, id: &str, f: impl FnOnce(&mut Value) -> Response) -> Response {
    match find_mut(&mut st.inputs, id) {
        Some(i) => f(i),
        None => not_found(format!("Input {id} not found")),
    }
}

fn describe_input(st: &mut State, id: &str) -> Response {
    with_input(st, id, |i| {
        resolve_transient(i, "input");
        ok_json(&i.clone())
    })
}

fn delete_input(st: &mut State, id: &str) -> Response {
    match find_mut(&mut st.inputs, id) {
        Some(i) => {
            i["state"] = json!("DELETING");
            ok_json(&json!({}))
        }
        None => not_found(format!("Input {id} not found")),
    }
}

fn update_input(st: &mut State, id: &str, params: &Value) -> Response {
    with_input(st, id, |i| {
        i["destinations"] = passthrough(params, "destinations");
        i["inputDevices"] = passthrough(params, "inputDevices");
        i["securityGroups"] = passthrough(params, "inputSecurityGroups");
        i["mediaConnectFlows"] = passthrough(params, "mediaConnectFlows");
        i["name"] = passthrough(params, "name");
        i["roleArn"] = passthrough(params, "roleArn");
        i["sources"] = passthrough(params, "sources");
        ok_json(&json!({ "input": i.clone() }))
    })
}

fn list_inputs(st: &mut State, max_results: Option<&str>, next_token: Option<&str>) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, i) in &st.inputs {
        arns.push(i["arn"].as_str().unwrap_or_default().to_string());
        items.push(i.clone());
    }
    paginated_result(arns, items, max_results, next_token, "inputs")
}

// ---------------------------------------------------------------------------
// Input security groups
// ---------------------------------------------------------------------------

fn create_input_security_group(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "inputSecurityGroup", &id);
    let group = json!({
        "arn": arn.clone(),
        "id": id.clone(),
        "inputs": [],
        "state": "IDLE",
        "tags": or_empty_object(params, "tags"),
        "whitelistRules": or_empty_list(params, "whitelistRules"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.input_security_groups.push((id, group.clone()));
    ok_json(&json!({ "securityGroup": group }))
}

fn list_input_security_groups(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, g) in &st.input_security_groups {
        arns.push(g["arn"].as_str().unwrap_or_default().to_string());
        items.push(g.clone());
    }
    paginated_result(arns, items, max_results, next_token, "inputSecurityGroups")
}

fn update_input_security_group(st: &mut State, id: &str, params: &Value) -> Response {
    match find_mut(&mut st.input_security_groups, id) {
        Some(g) => {
            if let Some(tags) = params.get("tags") {
                g["tags"] = tags.clone();
            }
            if let Some(rules) = params.get("whitelistRules") {
                g["whitelistRules"] = rules.clone();
            }
            ok_json(&json!({ "securityGroup": g.clone() }))
        }
        None => not_found(format!("InputSecurityGroup {id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Multiplexes
// ---------------------------------------------------------------------------

fn create_multiplex(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "multiplex", &id);
    let multiplex = json!({
        "arn": arn.clone(),
        "availabilityZones": or_empty_list(params, "availabilityZones"),
        "destinations": [],
        "id": id.clone(),
        "multiplexSettings": or_empty_object(params, "multiplexSettings"),
        "name": passthrough(params, "name"),
        "pipelinesRunningCount": 0,
        "programCount": 0,
        "state": "CREATING",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.multiplexes.push((id, multiplex.clone()));
    ok_json(&json!({ "multiplex": multiplex }))
}

fn program_count_for(st: &State, multiplex_id: &str) -> usize {
    st.multiplex_programs
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{multiplex_id}/")))
        .count()
}

fn describe_multiplex(st: &mut State, id: &str) -> Response {
    let count = program_count_for(st, id);
    match find_mut(&mut st.multiplexes, id) {
        Some(m) => {
            resolve_transient(m, "multiplex");
            m["programCount"] = json!(count);
            ok_json(&m.clone())
        }
        None => not_found(format!("Multiplex {id} not found")),
    }
}

fn list_multiplexes(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, m) in &st.multiplexes {
        arns.push(m["arn"].as_str().unwrap_or_default().to_string());
        items.push(m.clone());
    }
    paginated_result(arns, items, max_results, next_token, "multiplexes")
}

fn delete_multiplex(st: &mut State, id: &str) -> Response {
    match find_mut(&mut st.multiplexes, id) {
        Some(m) => {
            m["state"] = json!("DELETING");
            ok_json(&m.clone())
        }
        None => not_found(format!("Multiplex {id} not found")),
    }
}

fn multiplex_state(st: &mut State, id: &str, state: &str) -> Response {
    match find_mut(&mut st.multiplexes, id) {
        Some(m) => {
            resolve_transient(m, "multiplex");
            m["state"] = json!(state);
            ok_json(&m.clone())
        }
        None => not_found(format!("Multiplex {id} not found")),
    }
}

fn update_multiplex(st: &mut State, id: &str, params: &Value) -> Response {
    match find_mut(&mut st.multiplexes, id) {
        Some(m) => {
            if let Some(settings) = params.get("multiplexSettings") {
                m["multiplexSettings"] = settings.clone();
            }
            if let Some(name) = params.get("name") {
                m["name"] = name.clone();
            }
            resolve_transient(m, "multiplex");
            ok_json(&json!({ "multiplex": m.clone() }))
        }
        None => not_found(format!("Multiplex {id} not found")),
    }
}

fn list_multiplex_alerts(st: &mut State, id: &str) -> Response {
    if find(&st.multiplexes, id).is_none() {
        return not_found(format!("Multiplex {id} not found"));
    }
    ok_json(&json!({"alerts": [], "nextToken": null}))
}

// ---------------------------------------------------------------------------
// Multiplex programs
// ---------------------------------------------------------------------------

fn program_key(multiplex_id: &str, program_name: &str) -> String {
    format!("{multiplex_id}/{program_name}")
}

fn create_multiplex_program(st: &mut State, multiplex_id: &str, params: &Value) -> Response {
    if find(&st.multiplexes, multiplex_id).is_none() {
        return not_found(format!("Multiplex {multiplex_id} not found"));
    }
    let name = params
        .get("programName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let program = json!({
        "channelId": Value::Null,
        "multiplexProgramSettings": or_empty_object(params, "multiplexProgramSettings"),
        "packetIdentifiersMap": {},
        "pipelineDetails": [],
        "programName": name.clone(),
    });
    st.multiplex_programs
        .push((program_key(multiplex_id, &name), program.clone()));
    ok_json(&json!({ "multiplexProgram": program }))
}

fn describe_multiplex_program(st: &mut State, multiplex_id: &str, name: &str) -> Response {
    if find(&st.multiplexes, multiplex_id).is_none() {
        return not_found(format!("Multiplex {multiplex_id} not found"));
    }
    match find(&st.multiplex_programs, &program_key(multiplex_id, name)) {
        Some(p) => ok_json(&p.clone()),
        None => not_found(format!("MultiplexProgram {name} not found")),
    }
}

fn list_multiplex_programs(st: &mut State, multiplex_id: &str) -> Response {
    if find(&st.multiplexes, multiplex_id).is_none() {
        return not_found(format!("Multiplex {multiplex_id} not found"));
    }
    let programs = composite_items(&st.multiplex_programs, multiplex_id);
    ok_json(&json!({"multiplexPrograms": programs, "nextToken": null}))
}

fn delete_multiplex_program(st: &mut State, multiplex_id: &str, name: &str) -> Response {
    if find(&st.multiplexes, multiplex_id).is_none() {
        return not_found(format!("Multiplex {multiplex_id} not found"));
    }
    match take(&mut st.multiplex_programs, &program_key(multiplex_id, name)) {
        Some(p) => ok_json(&p),
        None => not_found(format!("MultiplexProgram {name} not found")),
    }
}

fn update_multiplex_program(
    st: &mut State,
    multiplex_id: &str,
    name: &str,
    params: &Value,
) -> Response {
    if find(&st.multiplexes, multiplex_id).is_none() {
        return not_found(format!("Multiplex {multiplex_id} not found"));
    }
    match find_mut(&mut st.multiplex_programs, &program_key(multiplex_id, name)) {
        Some(p) => {
            if let Some(settings) = params.get("multiplexProgramSettings") {
                p["multiplexProgramSettings"] = settings.clone();
            }
            ok_json(&json!({ "multiplexProgram": p.clone() }))
        }
        None => not_found(format!("MultiplexProgram {name} not found")),
    }
}

// ---------------------------------------------------------------------------
// Clusters
// ---------------------------------------------------------------------------

fn create_cluster(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "cluster", &id);
    let cluster = json!({
        "arn": arn.clone(),
        "channelIds": [],
        "clusterType": or_str(params, "clusterType", "ON_PREMISES"),
        "id": id.clone(),
        "instanceRoleArn": passthrough(params, "instanceRoleArn"),
        "name": passthrough(params, "name"),
        "networkSettings": or_empty_object(params, "networkSettings"),
        "state": "CREATING",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.clusters.push((id, cluster.clone()));
    ok_json(&cluster)
}

fn cluster_missing(st: &State, cluster_id: &str) -> bool {
    find(&st.clusters, cluster_id).is_none()
}

fn with_cluster(st: &mut State, id: &str, f: impl FnOnce(&mut Value) -> Response) -> Response {
    match find_mut(&mut st.clusters, id) {
        Some(c) => f(c),
        None => not_found(format!("Cluster {id} not found")),
    }
}

fn delete_cluster(st: &mut State, id: &str) -> Response {
    with_cluster(st, id, |c| {
        c["state"] = json!("DELETING");
        ok_json(&c.clone())
    })
}

fn update_cluster(st: &mut State, id: &str, params: &Value) -> Response {
    with_cluster(st, id, |c| {
        if let Some(name) = params.get("name") {
            c["name"] = name.clone();
        }
        if let Some(settings) = params.get("networkSettings") {
            c["networkSettings"] = settings.clone();
        }
        ok_json(&c.clone())
    })
}

fn list_clusters(st: &mut State, max_results: Option<&str>, next_token: Option<&str>) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, c) in &st.clusters {
        arns.push(c["arn"].as_str().unwrap_or_default().to_string());
        items.push(c.clone());
    }
    paginated_result(arns, items, max_results, next_token, "clusters")
}

fn list_cluster_alerts(st: &mut State, cluster_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    ok_json(&json!({"alerts": [], "nextToken": null}))
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

fn create_node(st: &mut State, cluster_id: &str, params: &Value, req: &Request) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    let id = hex_id();
    let arn = full_arn(req, "node", &id);
    let node = json!({
        "arn": arn.clone(),
        "channelPlacementGroups": [],
        "clusterId": cluster_id,
        "connectionState": "DISCONNECTED",
        "id": id.clone(),
        "instanceArn": Value::Null,
        "managedInstanceId": Value::Null,
        "name": passthrough(params, "name"),
        "nodeInterfaceMappings": or_empty_list(params, "nodeInterfaceMappings"),
        "role": or_str(params, "role", "BACKUP"),
        "state": "IDLE",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.nodes.push((format!("{cluster_id}/{id}"), node.clone()));
    ok_json(&node)
}

fn describe_node(st: &mut State, cluster_id: &str, node_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(&mut st.nodes, &format!("{cluster_id}/{node_id}")) {
        Some(n) => {
            resolve_transient(n, "node");
            ok_json(&n.clone())
        }
        None => not_found(format!("Node {node_id} not found")),
    }
}

fn delete_node(st: &mut State, cluster_id: &str, node_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(&mut st.nodes, &format!("{cluster_id}/{node_id}")) {
        Some(n) => {
            n["state"] = json!("DELETING");
            ok_json(&n.clone())
        }
        None => not_found(format!("Node {node_id} not found")),
    }
}

fn update_node(st: &mut State, cluster_id: &str, node_id: &str, params: &Value) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(&mut st.nodes, &format!("{cluster_id}/{node_id}")) {
        Some(n) => {
            if let Some(name) = params.get("name") {
                n["name"] = name.clone();
            }
            if let Some(role) = params.get("role") {
                n["role"] = role.clone();
            }
            ok_json(&n.clone())
        }
        None => not_found(format!("Node {node_id} not found")),
    }
}

fn update_node_state(st: &mut State, cluster_id: &str, node_id: &str, params: &Value) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(&mut st.nodes, &format!("{cluster_id}/{node_id}")) {
        Some(n) => {
            if let Some(state) = params.get("state") {
                n["state"] = state.clone();
            }
            ok_json(&n.clone())
        }
        None => not_found(format!("Node {node_id} not found")),
    }
}

fn create_node_registration_script(st: &mut State, cluster_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    ok_json(&json!({
        "nodeRegistrationScript":
            "#!/bin/bash\n# Mock node registration script\necho 'registered'"
    }))
}

// ---------------------------------------------------------------------------
// Channel placement groups
// ---------------------------------------------------------------------------

fn create_channel_placement_group(
    st: &mut State,
    cluster_id: &str,
    params: &Value,
    req: &Request,
) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    let id = hex_id();
    let arn = full_arn(req, "channelPlacementGroup", &id);
    let group = json!({
        "arn": arn.clone(),
        "channels": [],
        "clusterId": cluster_id,
        "id": id.clone(),
        "name": passthrough(params, "name"),
        "nodes": or_empty_list(params, "nodes"),
        "state": "IDLE",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.channel_placement_groups
        .push((format!("{cluster_id}/{id}"), group.clone()));
    ok_json(&group)
}

fn describe_channel_placement_group(st: &mut State, cluster_id: &str, group_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(
        &mut st.channel_placement_groups,
        &format!("{cluster_id}/{group_id}"),
    ) {
        Some(g) => ok_json(&g.clone()),
        None => not_found(format!("ChannelPlacementGroup {group_id} not found")),
    }
}

fn list_channel_placement_groups(st: &mut State, cluster_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    let groups = composite_items(&st.channel_placement_groups, cluster_id);
    ok_json(&json!({"channelPlacementGroups": groups, "nextToken": null}))
}

fn delete_channel_placement_group(st: &mut State, cluster_id: &str, group_id: &str) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(
        &mut st.channel_placement_groups,
        &format!("{cluster_id}/{group_id}"),
    ) {
        Some(g) => {
            g["state"] = json!("DELETING");
            ok_json(&g.clone())
        }
        None => not_found(format!("ChannelPlacementGroup {group_id} not found")),
    }
}

fn update_channel_placement_group(
    st: &mut State,
    cluster_id: &str,
    group_id: &str,
    params: &Value,
) -> Response {
    if cluster_missing(st, cluster_id) {
        return not_found(format!("Cluster {cluster_id} not found"));
    }
    match find_mut(
        &mut st.channel_placement_groups,
        &format!("{cluster_id}/{group_id}"),
    ) {
        Some(g) => {
            if let Some(name) = params.get("name") {
                g["name"] = name.clone();
            }
            if let Some(nodes) = params.get("nodes") {
                g["nodes"] = nodes.clone();
            }
            ok_json(&g.clone())
        }
        None => not_found(format!("ChannelPlacementGroup {group_id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Networks
// ---------------------------------------------------------------------------

fn create_network(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "network", &id);
    let network = json!({
        "arn": arn.clone(),
        "associatedClusterIds": [],
        "id": id.clone(),
        "ipPools": or_empty_list(params, "ipPools"),
        "name": passthrough(params, "name"),
        "routes": or_empty_list(params, "routes"),
        "state": "IDLE",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.networks.push((id, network.clone()));
    ok_json(&network)
}

fn with_network(st: &mut State, id: &str, f: impl FnOnce(&mut Value) -> Response) -> Response {
    match find_mut(&mut st.networks, id) {
        Some(n) => f(n),
        None => not_found(format!("Network {id} not found")),
    }
}

fn delete_network(st: &mut State, id: &str) -> Response {
    with_network(st, id, |n| {
        n["state"] = json!("DELETING");
        ok_json(&n.clone())
    })
}

fn update_network(st: &mut State, id: &str, params: &Value) -> Response {
    with_network(st, id, |n| {
        if let Some(pools) = params.get("ipPools") {
            n["ipPools"] = pools.clone();
        }
        if let Some(name) = params.get("name") {
            n["name"] = name.clone();
        }
        if let Some(routes) = params.get("routes") {
            n["routes"] = routes.clone();
        }
        ok_json(&n.clone())
    })
}

fn list_networks(st: &mut State, max_results: Option<&str>, next_token: Option<&str>) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, n) in &st.networks {
        arns.push(n["arn"].as_str().unwrap_or_default().to_string());
        items.push(n.clone());
    }
    paginated_result(arns, items, max_results, next_token, "networks")
}

// ---------------------------------------------------------------------------
// SDI sources
// ---------------------------------------------------------------------------

fn create_sdi_source(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "sdiSource", &id);
    let source = json!({
        "arn": arn.clone(),
        "id": id.clone(),
        "inputs": [],
        "mode": or_str(params, "mode", "QUADRANT"),
        "name": passthrough(params, "name"),
        "state": "IDLE",
        "tags": or_empty_object(params, "tags"),
        "type": or_str(params, "type", "SINGLE"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.sdi_sources.push((id, source.clone()));
    ok_json(&source)
}

fn update_sdi_source(st: &mut State, id: &str, params: &Value) -> Response {
    match find_mut(&mut st.sdi_sources, id) {
        Some(s) => {
            if let Some(mode) = params.get("mode") {
                s["mode"] = mode.clone();
            }
            if let Some(name) = params.get("name") {
                s["name"] = name.clone();
            }
            if let Some(ty) = params.get("type") {
                s["type"] = ty.clone();
            }
            ok_json(&s.clone())
        }
        None => not_found(format!("SdiSource {id} not found")),
    }
}

fn list_sdi_sources(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, s) in &st.sdi_sources {
        arns.push(s["arn"].as_str().unwrap_or_default().to_string());
        items.push(s.clone());
    }
    paginated_result(arns, items, max_results, next_token, "sdiSources")
}

// ---------------------------------------------------------------------------
// CloudWatch alarm template groups + templates
// ---------------------------------------------------------------------------

fn create_cw_alarm_template_group(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "cloudWatchAlarmTemplateGroup", &id);
    let now = now_iso();
    let group = json!({
        "arn": arn.clone(),
        "createdAt": now.clone(),
        "description": or_str(params, "description", ""),
        "id": id.clone(),
        "modifiedAt": now,
        "name": passthrough(params, "name"),
        "tags": or_empty_object(params, "tags"),
        "templateCount": 0,
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.cw_alarm_template_groups.push((id, group.clone()));
    ok_json(&group)
}

fn get_cw_alarm_template_group(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.cw_alarm_template_groups, identifier) {
        Some(key) => match find(&st.cw_alarm_template_groups, &key) {
            Some(g) => ok_json(&g.clone()),
            None => not_found(format!(
                "CloudWatchAlarmTemplateGroup {identifier} not found"
            )),
        },
        None => not_found(format!(
            "CloudWatchAlarmTemplateGroup {identifier} not found"
        )),
    }
}

fn list_cw_alarm_template_groups(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, g) in &st.cw_alarm_template_groups {
        arns.push(g["arn"].as_str().unwrap_or_default().to_string());
        items.push(g.clone());
    }
    paginated_result(
        arns,
        items,
        max_results,
        next_token,
        "cloudWatchAlarmTemplateGroups",
    )
}

fn delete_cw_alarm_template_group(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.cw_alarm_template_groups, identifier) {
        Some(key) => {
            take(&mut st.cw_alarm_template_groups, &key);
            ok_json(&json!({}))
        }
        None => not_found(format!(
            "CloudWatchAlarmTemplateGroup {identifier} not found"
        )),
    }
}

fn update_cw_alarm_template_group(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.cw_alarm_template_groups, identifier) else {
        return not_found(format!(
            "CloudWatchAlarmTemplateGroup {identifier} not found"
        ));
    };
    match find_mut(&mut st.cw_alarm_template_groups, &key) {
        Some(g) => {
            if let Some(description) = params.get("description") {
                g["description"] = description.clone();
            }
            g["modifiedAt"] = json!(now_iso());
            ok_json(&g.clone())
        }
        None => not_found(format!(
            "CloudWatchAlarmTemplateGroup {identifier} not found"
        )),
    }
}

/// moto resolves identifiers by stored id first, then name, then arn.
fn resolve_identifier(records: &Records, identifier: &str) -> Option<String> {
    if find(records, identifier).is_some() {
        return Some(identifier.to_string());
    }
    for (key, row) in records {
        if row["name"].as_str() == Some(identifier) || row["arn"].as_str() == Some(identifier) {
            return Some(key.clone());
        }
    }
    None
}

fn create_cw_alarm_template(st: &mut State, params: &Value, req: &Request) -> Response {
    let group_identifier = params
        .get("groupIdentifier")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(group_key) = resolve_identifier(&st.cw_alarm_template_groups, group_identifier) else {
        return not_found(format!(
            "CloudWatchAlarmTemplateGroup {group_identifier} not found"
        ));
    };
    let group_id = find(&st.cw_alarm_template_groups, &group_key)
        .and_then(|g| g["id"].as_str().map(String::from))
        .unwrap_or_default();
    let id = hex_id();
    let arn = full_arn(req, "cloudWatchAlarmTemplate", &id);
    let now = now_iso();
    let template = json!({
        "arn": arn.clone(),
        "comparisonOperator": passthrough(params, "comparisonOperator"),
        "createdAt": now.clone(),
        "datapointsToAlarm": passthrough(params, "datapointsToAlarm"),
        "description": or_str(params, "description", ""),
        "evaluationPeriods": passthrough(params, "evaluationPeriods"),
        "groupId": group_id,
        "id": id.clone(),
        "metricName": passthrough(params, "metricName"),
        "modifiedAt": now,
        "name": passthrough(params, "name"),
        "period": passthrough(params, "period"),
        "statistic": passthrough(params, "statistic"),
        "tags": or_empty_object(params, "tags"),
        "targetResourceType": passthrough(params, "targetResourceType"),
        "threshold": passthrough(params, "threshold"),
        "treatMissingData": passthrough(params, "treatMissingData"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    if let Some(g) = find_mut(&mut st.cw_alarm_template_groups, &group_key) {
        g["templateCount"] = json!(g["templateCount"].as_i64().unwrap_or(0) + 1);
    }
    st.cw_alarm_templates.push((id, template.clone()));
    ok_json(&template)
}

fn get_cw_alarm_template(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.cw_alarm_templates, identifier) {
        Some(key) => match find(&st.cw_alarm_templates, &key) {
            Some(t) => ok_json(&t.clone()),
            None => not_found(format!("CloudWatchAlarmTemplate {identifier} not found")),
        },
        None => not_found(format!("CloudWatchAlarmTemplate {identifier} not found")),
    }
}

fn list_cw_alarm_templates(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, t) in &st.cw_alarm_templates {
        arns.push(t["arn"].as_str().unwrap_or_default().to_string());
        items.push(t.clone());
    }
    paginated_result(
        arns,
        items,
        max_results,
        next_token,
        "cloudWatchAlarmTemplates",
    )
}

fn delete_cw_alarm_template(st: &mut State, identifier: &str) -> Response {
    let Some(key) = resolve_identifier(&st.cw_alarm_templates, identifier) else {
        return not_found(format!("CloudWatchAlarmTemplate {identifier} not found"));
    };
    let group_id = find(&st.cw_alarm_templates, &key)
        .and_then(|t| t["groupId"].as_str().map(String::from))
        .unwrap_or_default();
    if let Some(g) = find_mut(&mut st.cw_alarm_template_groups, &group_id) {
        g["templateCount"] = json!(g["templateCount"].as_i64().unwrap_or(0) - 1);
    }
    take(&mut st.cw_alarm_templates, &key);
    ok_json(&json!({}))
}

fn update_cw_alarm_template(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.cw_alarm_templates, identifier) else {
        return not_found(format!("CloudWatchAlarmTemplate {identifier} not found"));
    };
    if let Some(group_identifier) = params.get("groupIdentifier").and_then(Value::as_str) {
        let Some(group_key) = resolve_identifier(&st.cw_alarm_template_groups, group_identifier)
        else {
            return not_found(format!(
                "CloudWatchAlarmTemplateGroup {group_identifier} not found"
            ));
        };
        let group_id = find(&st.cw_alarm_template_groups, &group_key)
            .and_then(|g| g["id"].as_str().map(String::from))
            .unwrap_or_default();
        if let Some(t) = find_mut(&mut st.cw_alarm_templates, &key) {
            t["groupId"] = json!(group_id);
        }
    }
    match find_mut(&mut st.cw_alarm_templates, &key) {
        Some(t) => {
            for wire_key in [
                "comparisonOperator",
                "datapointsToAlarm",
                "description",
                "evaluationPeriods",
                "metricName",
                "period",
                "statistic",
                "targetResourceType",
                "threshold",
                "treatMissingData",
            ] {
                if let Some(v) = params.get(wire_key) {
                    t[wire_key] = v.clone();
                }
            }
            t["modifiedAt"] = json!(now_iso());
            ok_json(&t.clone())
        }
        None => not_found(format!("CloudWatchAlarmTemplate {identifier} not found")),
    }
}

// ---------------------------------------------------------------------------
// EventBridge rule template groups + templates
// ---------------------------------------------------------------------------

fn create_eb_rule_template_group(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "eventBridgeRuleTemplateGroup", &id);
    let now = now_iso();
    let group = json!({
        "arn": arn.clone(),
        "createdAt": now.clone(),
        "description": or_str(params, "description", ""),
        "id": id.clone(),
        "modifiedAt": now,
        "name": passthrough(params, "name"),
        "tags": or_empty_object(params, "tags"),
        "templateCount": 0,
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.eb_rule_template_groups.push((id, group.clone()));
    ok_json(&group)
}

fn get_eb_rule_template_group(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.eb_rule_template_groups, identifier) {
        Some(key) => match find(&st.eb_rule_template_groups, &key) {
            Some(g) => ok_json(&g.clone()),
            None => not_found(format!(
                "EventBridgeRuleTemplateGroup {identifier} not found"
            )),
        },
        None => not_found(format!(
            "EventBridgeRuleTemplateGroup {identifier} not found"
        )),
    }
}

fn list_eb_rule_template_groups(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, g) in &st.eb_rule_template_groups {
        arns.push(g["arn"].as_str().unwrap_or_default().to_string());
        items.push(g.clone());
    }
    paginated_result(
        arns,
        items,
        max_results,
        next_token,
        "eventBridgeRuleTemplateGroups",
    )
}

fn delete_eb_rule_template_group(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.eb_rule_template_groups, identifier) {
        Some(key) => {
            take(&mut st.eb_rule_template_groups, &key);
            ok_json(&json!({}))
        }
        None => not_found(format!(
            "EventBridgeRuleTemplateGroup {identifier} not found"
        )),
    }
}

fn update_eb_rule_template_group(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.eb_rule_template_groups, identifier) else {
        return not_found(format!(
            "EventBridgeRuleTemplateGroup {identifier} not found"
        ));
    };
    match find_mut(&mut st.eb_rule_template_groups, &key) {
        Some(g) => {
            if let Some(description) = params.get("description") {
                g["description"] = description.clone();
            }
            g["modifiedAt"] = json!(now_iso());
            ok_json(&g.clone())
        }
        None => not_found(format!(
            "EventBridgeRuleTemplateGroup {identifier} not found"
        )),
    }
}

fn create_eb_rule_template(st: &mut State, params: &Value, req: &Request) -> Response {
    let group_identifier = params
        .get("groupIdentifier")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(group_key) = resolve_identifier(&st.eb_rule_template_groups, group_identifier) else {
        return not_found(format!(
            "EventBridgeRuleTemplateGroup {group_identifier} not found"
        ));
    };
    let group_id = find(&st.eb_rule_template_groups, &group_key)
        .and_then(|g| g["id"].as_str().map(String::from))
        .unwrap_or_default();
    let id = hex_id();
    let arn = full_arn(req, "eventBridgeRuleTemplate", &id);
    let now = now_iso();
    let template = json!({
        "arn": arn.clone(),
        "createdAt": now.clone(),
        "description": or_str(params, "description", ""),
        "eventTargets": or_empty_list(params, "eventTargets"),
        "eventType": passthrough(params, "eventType"),
        "groupId": group_id,
        "id": id.clone(),
        "modifiedAt": now,
        "name": passthrough(params, "name"),
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    if let Some(g) = find_mut(&mut st.eb_rule_template_groups, &group_key) {
        g["templateCount"] = json!(g["templateCount"].as_i64().unwrap_or(0) + 1);
    }
    st.eb_rule_templates.push((id, template.clone()));
    ok_json(&template)
}

fn get_eb_rule_template(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.eb_rule_templates, identifier) {
        Some(key) => match find(&st.eb_rule_templates, &key) {
            Some(t) => ok_json(&t.clone()),
            None => not_found(format!("EventBridgeRuleTemplate {identifier} not found")),
        },
        None => not_found(format!("EventBridgeRuleTemplate {identifier} not found")),
    }
}

fn list_eb_rule_templates(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, t) in &st.eb_rule_templates {
        arns.push(t["arn"].as_str().unwrap_or_default().to_string());
        items.push(t.clone());
    }
    paginated_result(
        arns,
        items,
        max_results,
        next_token,
        "eventBridgeRuleTemplates",
    )
}

fn delete_eb_rule_template(st: &mut State, identifier: &str) -> Response {
    let Some(key) = resolve_identifier(&st.eb_rule_templates, identifier) else {
        return not_found(format!("EventBridgeRuleTemplate {identifier} not found"));
    };
    let group_id = find(&st.eb_rule_templates, &key)
        .and_then(|t| t["groupId"].as_str().map(String::from))
        .unwrap_or_default();
    if let Some(g) = find_mut(&mut st.eb_rule_template_groups, &group_id) {
        g["templateCount"] = json!(g["templateCount"].as_i64().unwrap_or(0) - 1);
    }
    take(&mut st.eb_rule_templates, &key);
    ok_json(&json!({}))
}

fn update_eb_rule_template(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.eb_rule_templates, identifier) else {
        return not_found(format!("EventBridgeRuleTemplate {identifier} not found"));
    };
    if let Some(group_identifier) = params.get("groupIdentifier").and_then(Value::as_str) {
        let Some(group_key) = resolve_identifier(&st.eb_rule_template_groups, group_identifier)
        else {
            return not_found(format!(
                "EventBridgeRuleTemplateGroup {group_identifier} not found"
            ));
        };
        let group_id = find(&st.eb_rule_template_groups, &group_key)
            .and_then(|g| g["id"].as_str().map(String::from))
            .unwrap_or_default();
        if let Some(t) = find_mut(&mut st.eb_rule_templates, &key) {
            t["groupId"] = json!(group_id);
        }
    }
    match find_mut(&mut st.eb_rule_templates, &key) {
        Some(t) => {
            for wire_key in ["description", "eventTargets", "eventType"] {
                if let Some(v) = params.get(wire_key) {
                    t[wire_key] = v.clone();
                }
            }
            t["modifiedAt"] = json!(now_iso());
            ok_json(&t.clone())
        }
        None => not_found(format!("EventBridgeRuleTemplate {identifier} not found")),
    }
}

// ---------------------------------------------------------------------------
// Signal maps
// ---------------------------------------------------------------------------

fn create_signal_map(st: &mut State, params: &Value, req: &Request) -> Response {
    let id = hex_id();
    let arn = full_arn(req, "signalMap", &id);
    let now = now_iso();
    let signal_map = json!({
        "arn": arn.clone(),
        "cloudWatchAlarmTemplateGroupIds": or_empty_list(
            params,
            "cloudWatchAlarmTemplateGroupIdentifiers",
        ),
        "createdAt": now.clone(),
        "description": or_str(params, "description", ""),
        "discoveryEntryPointArn": passthrough(params, "discoveryEntryPointArn"),
        "errorMessage": Value::Null,
        "eventBridgeRuleTemplateGroupIds": or_empty_list(
            params,
            "eventBridgeRuleTemplateGroupIdentifiers",
        ),
        "failedMediaResourceMap": {},
        "id": id.clone(),
        "lastDiscoveredAt": Value::Null,
        "lastSuccessfulMonitorDeploymentAt": Value::Null,
        "mediaResourceMap": {},
        "modifiedAt": now,
        "monitorChangesPendingDeployment": false,
        "monitorDeployment": {},
        "name": passthrough(params, "name"),
        "status": "CREATE_COMPLETE",
        "tags": or_empty_object(params, "tags"),
    });
    store_tags(&mut st.tags, &arn, params.get("tags"));
    st.signal_maps.push((id, signal_map.clone()));
    ok_json(&signal_map)
}

fn get_signal_map(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.signal_maps, identifier) {
        Some(key) => match find(&st.signal_maps, &key) {
            Some(sm) => ok_json(&sm.clone()),
            None => not_found(format!("SignalMap {identifier} not found")),
        },
        None => not_found(format!("SignalMap {identifier} not found")),
    }
}

fn list_signal_maps(
    st: &mut State,
    max_results: Option<&str>,
    next_token: Option<&str>,
) -> Response {
    let mut arns: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for (_, sm) in &st.signal_maps {
        arns.push(sm["arn"].as_str().unwrap_or_default().to_string());
        items.push(sm.clone());
    }
    paginated_result(arns, items, max_results, next_token, "signalMaps")
}

fn delete_signal_map(st: &mut State, identifier: &str) -> Response {
    match resolve_identifier(&st.signal_maps, identifier) {
        Some(key) => {
            take(&mut st.signal_maps, &key);
            ok_json(&json!({}))
        }
        None => not_found(format!("SignalMap {identifier} not found")),
    }
}

fn start_update_signal_map(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.signal_maps, identifier) else {
        return not_found(format!("SignalMap {identifier} not found"));
    };
    match find_mut(&mut st.signal_maps, &key) {
        Some(sm) => {
            if let Some(v) = params.get("cloudWatchAlarmTemplateGroupIdentifiers") {
                sm["cloudWatchAlarmTemplateGroupIds"] = v.clone();
            }
            if let Some(v) = params.get("description") {
                sm["description"] = v.clone();
            }
            if let Some(v) = params.get("discoveryEntryPointArn") {
                sm["discoveryEntryPointArn"] = v.clone();
            }
            if let Some(v) = params.get("eventBridgeRuleTemplateGroupIdentifiers") {
                sm["eventBridgeRuleTemplateGroupIds"] = v.clone();
            }
            if let Some(v) = params.get("name") {
                sm["name"] = v.clone();
            }
            sm["modifiedAt"] = json!(now_iso());
            sm["status"] = json!("UPDATE_COMPLETE");
            ok_json(&sm.clone())
        }
        None => not_found(format!("SignalMap {identifier} not found")),
    }
}

fn start_monitor_deployment(st: &mut State, identifier: &str, params: &Value) -> Response {
    let Some(key) = resolve_identifier(&st.signal_maps, identifier) else {
        return not_found(format!("SignalMap {identifier} not found"));
    };
    let dry_run = params
        .get("dryRun")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match find_mut(&mut st.signal_maps, &key) {
        Some(sm) => {
            sm["monitorDeployment"] =
                json!({"status": if dry_run { "DRY_RUN_COMPLETE" } else { "COMPLETE" }});
            sm["monitorChangesPendingDeployment"] = json!(false);
            sm["modifiedAt"] = json!(now_iso());
            ok_json(&sm.clone())
        }
        None => not_found(format!("SignalMap {identifier} not found")),
    }
}

fn start_delete_monitor_deployment(st: &mut State, identifier: &str) -> Response {
    let Some(key) = resolve_identifier(&st.signal_maps, identifier) else {
        return not_found(format!("SignalMap {identifier} not found"));
    };
    match find_mut(&mut st.signal_maps, &key) {
        Some(sm) => {
            sm["monitorDeployment"] = json!({});
            sm["modifiedAt"] = json!(now_iso());
            ok_json(&sm.clone())
        }
        None => not_found(format!("SignalMap {identifier} not found")),
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tag_ops(st: &mut State, req: &Request, arn: &str, method: &str) -> Option<Response> {
    match method {
        "POST" => {
            let body = parse_json_body(&req.body);
            store_tags(&mut st.tags, arn, body.get("tags"));
            Some(ok_json(&json!({})))
        }
        "GET" => {
            let empty = Map::new();
            let tags = st.tags.get(arn).unwrap_or(&empty);
            Some(ok_json(&json!({ "tags": tags })))
        }
        "DELETE" => {
            // botocore serializes DeleteTags' repeated TagKeys as query params.
            let keys = query_map(&req.query_string).get("tagKeys").cloned();
            if let Some(keys) = keys {
                if let Some(entry) = st.tags.get_mut(arn) {
                    for k in keys {
                        entry.remove(&k);
                    }
                }
            }
            Some(ok_json(&json!({})))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

fn channel_exists(st: &State, channel_id: &str) -> bool {
    st.channels.iter().any(|(k, _)| k == channel_id)
}

fn batch_update_schedule(st: &mut State, channel_id: &str, params: &Value) -> Response {
    if !channel_exists(st, channel_id) {
        return not_found(format!("Channel {channel_id} not found"));
    }
    st.schedules.entry(channel_id.to_string()).or_default();

    let mut deleted_actions: Vec<Value> = Vec::new();
    if let Some(names) = params
        .get("deletes")
        .and_then(|d| d.get("actionNames"))
        .and_then(Value::as_array)
    {
        let names: Vec<&str> = names.iter().filter_map(Value::as_str).collect();
        let mut kept = Vec::new();
        if let Some(existing) = st.schedules.get(channel_id) {
            for action in existing {
                let name = action["actionName"].as_str().unwrap_or_default();
                if !names.contains(&name) {
                    kept.push(action.clone());
                }
            }
        }
        st.schedules.insert(channel_id.to_string(), kept);
        deleted_actions = names.iter().map(|n| json!(n)).collect();
    }

    let mut created_actions: Vec<Value> = Vec::new();
    if let Some(actions) = params
        .get("creates")
        .and_then(|c| c.get("scheduleActions"))
        .and_then(Value::as_array)
    {
        for action in actions {
            st.schedules
                .get_mut(channel_id)
                .expect("schedule entry seeded above")
                .push(action.clone());
            created_actions.push(action.clone());
        }
    }

    ok_json(&json!({
        "creates": {"scheduleActions": created_actions},
        "deletes": {"actionNames": deleted_actions},
    }))
}

fn describe_schedule(st: &mut State, channel_id: &str) -> Response {
    if !channel_exists(st, channel_id) {
        return not_found(format!("Channel {channel_id} not found"));
    }
    let actions = st.schedules.get(channel_id).cloned().unwrap_or_default();
    ok_json(&json!({"scheduleActions": actions, "nextToken": null}))
}

fn delete_schedule(st: &mut State, channel_id: &str) -> Response {
    if !channel_exists(st, channel_id) {
        return not_found(format!("Channel {channel_id} not found"));
    }
    st.schedules.insert(channel_id.to_string(), Vec::new());
    ok_json(&json!({}))
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

fn batch_failed(id: &str) -> Value {
    json!({"arn": "", "code": "404", "id": id, "message": "Not found"})
}

fn batch_delete(st: &mut State, params: &Value) -> Response {
    let mut failed = Vec::new();
    let mut successful = Vec::new();

    if let Some(ids) = params.get("channelIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.channels, id) {
                Some(c) => {
                    c["state"] = json!("DELETING");
                    successful.push(json!({"arn": c["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    if let Some(ids) = params.get("inputIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.inputs, id) {
                Some(i) => {
                    i["state"] = json!("DELETING");
                    successful.push(json!({"arn": i["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    if let Some(ids) = params
        .get("inputSecurityGroupIds")
        .and_then(Value::as_array)
    {
        for id in ids.iter().filter_map(Value::as_str) {
            if take(&mut st.input_security_groups, id).is_some() {
                successful.push(json!({ "id": id }));
            } else {
                failed.push(batch_failed(id));
            }
        }
    }
    if let Some(ids) = params.get("multiplexIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.multiplexes, id) {
                Some(m) => {
                    m["state"] = json!("DELETING");
                    successful.push(json!({"arn": m["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    ok_json(&json!({"failed": failed, "successful": successful}))
}

fn batch_start(st: &mut State, params: &Value) -> Response {
    let mut failed = Vec::new();
    let mut successful = Vec::new();
    if let Some(ids) = params.get("channelIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.channels, id) {
                Some(c) => {
                    c["state"] = json!("STARTING");
                    successful.push(json!({"arn": c["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    if let Some(ids) = params.get("multiplexIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.multiplexes, id) {
                Some(m) => {
                    m["state"] = json!("STARTING");
                    successful.push(json!({"arn": m["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    ok_json(&json!({"failed": failed, "successful": successful}))
}

fn batch_stop(st: &mut State, params: &Value) -> Response {
    let mut failed = Vec::new();
    let mut successful = Vec::new();
    if let Some(ids) = params.get("channelIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.channels, id) {
                Some(c) => {
                    c["state"] = json!("STOPPING");
                    successful.push(json!({"arn": c["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    if let Some(ids) = params.get("multiplexIds").and_then(Value::as_array) {
        for id in ids.iter().filter_map(Value::as_str) {
            match find_mut(&mut st.multiplexes, id) {
                Some(m) => {
                    m["state"] = json!("STOPPING");
                    successful.push(json!({"arn": m["arn"], "id": id}));
                }
                None => failed.push(batch_failed(id)),
            }
        }
    }
    ok_json(&json!({"failed": failed, "successful": successful}))
}

// ---------------------------------------------------------------------------
// Thumbnails / device stubs
// ---------------------------------------------------------------------------

fn describe_thumbnails(st: &mut State, channel_id: &str) -> Response {
    if !channel_exists(st, channel_id) {
        return not_found(format!("Channel {channel_id} not found"));
    }
    ok_json(&json!({"thumbnailDetails": []}))
}

fn device_not_found(id: &str) -> Response {
    not_found(format!("InputDevice {id} not found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: impl Into<String>, body: Value) -> Request {
        Request {
            service: "medialive".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn scoped(mut r: Request, acct: u64) -> Request {
        r.account = acct;
        r
    }

    /// A request that carries a query string (as the gateway ships them).
    fn req_query(method: &str, path: &str, query: &str, body: Value) -> Request {
        let mut r = req(method, path, body);
        r.query_string = query.to_string();
        r
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Percent-encode an ARN for use as one path segment (botocore-style).
    fn url_encode_path(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    fn create_input(r: &Request) -> Value {
        body(&dispatch(r).unwrap())["input"].clone()
    }

    fn create_channel_body() -> Value {
        json!({
            "name": "ch-1",
            "encoderSettings": {"timecodeConfig": {"source": "EMBEDDED"}},
            "inputSpecification": {"codec": "AVC", "resolution": "HD"},
        })
    }

    #[test]
    fn channel_lifecycle_resolves_transient_states() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        let created = body(&dispatch(&r).unwrap())["channel"].clone();
        let id = created["id"].as_str().unwrap().to_string();

        // moto quirk: the create response carries no pipelinesRunningCount.
        assert_eq!(created["state"], "CREATING");
        assert!(created.get("pipelinesRunningCount").is_none());
        // Describing a fresh channel resolves CREATING → IDLE.
        r = scoped(req("GET", format!("/prod/channels/{id}"), json!({})), acct);
        let desc = body(&dispatch(&r).unwrap());
        assert_eq!(desc["state"], "IDLE");
        assert_eq!(desc["pipelinesRunningCount"], 2);

        // STARTING resolves to RUNNING on describe.
        r = scoped(
            req("POST", format!("/prod/channels/{id}/start"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "STARTING");
        r = scoped(req("GET", format!("/prod/channels/{id}"), json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "RUNNING");

        // STOPPING resolves to IDLE.
        r = scoped(
            req("POST", format!("/prod/channels/{id}/stop"), json!({})),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(req("GET", format!("/prod/channels/{id}"), json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "IDLE");

        // DELETING resolves to DELETED (still listed in describe).
        r = scoped(
            req("DELETE", format!("/prod/channels/{id}"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETING");
        r = scoped(req("GET", format!("/prod/channels/{id}"), json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETED");

        // Unknown channel → moto's NotFoundException.
        r = scoped(req("GET", "/prod/channels/nope", json!({})), acct);
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], "NotFoundException");
    }

    #[test]
    fn channel_arn_omits_region_and_account() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        let created = body(&dispatch(&r).unwrap())["channel"].clone();
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(
            created["arn"].as_str().unwrap(),
            format!("arn:aws:medialive:channel:{id}")
        );
    }

    #[test]
    fn update_channel_class_derives_pipeline_count() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        let id = body(&dispatch(&r).unwrap())["channel"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        r = scoped(
            req(
                "PUT",
                format!("/prod/channels/{id}/channelClass"),
                json!({"channelClass": "SINGLE_PIPELINE"}),
            ),
            acct,
        );
        let resp = body(&dispatch(&r).unwrap())["channel"].clone();
        assert_eq!(resp["channelClass"], "SINGLE_PIPELINE");
        assert_eq!(resp["state"], "UPDATING");
        // UPDATING resolves back on the next describe.
        r = scoped(req("GET", format!("/prod/channels/{id}"), json!({})), acct);
        let desc = body(&dispatch(&r).unwrap());
        assert_eq!(desc["channelClass"], "SINGLE_PIPELINE");
        assert_eq!(desc["pipelinesRunningCount"], 1);
    }

    #[test]
    fn list_channels_excludes_encoder_settings_and_paginates() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        assert!(body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .is_some());
        // An empty body is enough for a second channel.
        r = scoped(req("POST", "/prod/channels", json!({"name": "ch-2"})), acct);
        let second_arn = body(&dispatch(&r).unwrap())["channel"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r = scoped(
            req_query("GET", "/prod/channels", "maxResults=1", json!({})),
            acct,
        );
        let page1 = body(&dispatch(&r).unwrap());
        assert_eq!(page1["channels"].as_array().unwrap().len(), 1);
        assert!(page1["channels"][0].get("encoderSettings").is_none());
        let token = page1["nextToken"].as_str().unwrap().to_string();
        // moto's PaginatedResult token: base64 of {"uniqueAttributes": "<arn>"}.
        let marker = B64
            .decode(token.as_bytes())
            .ok()
            .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
            .and_then(|v| v["uniqueAttributes"].as_str().map(String::from))
            .unwrap();
        assert_eq!(marker, second_arn);

        r = scoped(
            req_query(
                "GET",
                "/prod/channels",
                &format!("maxResults=1&nextToken={token}"),
                json!({}),
            ),
            acct,
        );
        let page2 = body(&dispatch(&r).unwrap());
        let arns: Vec<&str> = page2["channels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["arn"].as_str().unwrap())
            .collect();
        assert_eq!(arns, vec![second_arn.as_str()]);
        assert!(page2["nextToken"].is_null());

        // moto's invalid-token path: unresolvable marker → InvalidToken 400.
        r = scoped(
            req_query("GET", "/prod/channels", "nextToken=!!!", json!({})),
            acct,
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "InvalidToken");
    }

    #[test]
    fn input_lifecycle_and_partner_inputs() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/inputs",
                json!({
                    "name": "inp-1",
                    "type": "URL_PULL",
                    "inputSecurityGroups": ["9031-77"],
                }),
            ),
            acct,
        );
        let input = create_input(&r);
        let id = input["id"].as_str().unwrap().to_string();
        // moto quirk: create inputs keep the passed securityGroups empty; only
        // updates ever assign them.
        assert_eq!(input["securityGroups"], json!([]));
        assert_eq!(input["state"], "CREATING");
        assert_eq!(input["inputPartnerIds"], json!([]));

        r = scoped(req("GET", format!("/prod/inputs/{id}"), json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DETACHED");

        // Partner input: name is `{parent}-partner` and both sides point at
        // each other.
        r = scoped(
            req("POST", format!("/prod/inputs/{id}/partners"), json!({})),
            acct,
        );
        let partner = body(&dispatch(&r).unwrap())["input"].clone();
        let partner_id = partner["id"].as_str().unwrap().to_string();
        assert_eq!(partner["name"], "inp-1-partner");
        assert_eq!(partner["inputPartnerIds"], json!([id]));
        assert_eq!(partner["state"], "CREATING");

        r = scoped(req("GET", format!("/prod/inputs/{id}"), json!({})), acct);
        assert_eq!(
            body(&dispatch(&r).unwrap())["inputPartnerIds"],
            json!([partner_id])
        );

        // Update assigns securityGroups (and other passed fields).
        r = scoped(
            req(
                "PUT",
                format!("/prod/inputs/{id}"),
                json!({"name": "inp-1b", "inputSecurityGroups": ["9031-77"]}),
            ),
            acct,
        );
        let updated = body(&dispatch(&r).unwrap())["input"].clone();
        assert_eq!(updated["name"], "inp-1b");
        assert_eq!(updated["securityGroups"], json!(["9031-77"]));

        // DELETING resolves to DELETED on describe.
        r = scoped(req("DELETE", format!("/prod/inputs/{id}"), json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(req("GET", format!("/prod/inputs/{id}"), json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETED");
    }

    #[test]
    fn input_security_group_crud() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/inputSecurityGroups",
                json!({"whitelistRules": [{"cidr": "10.0.0.0/8"}], "tags": {"env": "test"}}),
            ),
            acct,
        );
        let group = body(&dispatch(&r).unwrap())["securityGroup"].clone();
        let id = group["id"].as_str().unwrap().to_string();
        assert_eq!(group["state"], "IDLE");
        assert_eq!(group["whitelistRules"], json!([{"cidr": "10.0.0.0/8"}]));

        r = scoped(
            req(
                "PUT",
                format!("/prod/inputSecurityGroups/{id}"),
                json!({"whitelistRules": [{"cidr": "192.168.0.0/16"}]}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["securityGroup"]["whitelistRules"],
            json!([{"cidr": "192.168.0.0/16"}])
        );

        r = scoped(
            req(
                "DELETE",
                format!("/prod/inputSecurityGroups/{id}"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(
            req("GET", format!("/prod/inputSecurityGroups/{id}"), json!({})),
            acct,
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(body(&resp)["__type"], "NotFoundException");
    }

    #[test]
    fn multiplex_lifecycle_with_programs() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/multiplexes",
                json!({"name": "mux-1", "multiplexSettings": {"transportStreamBitrate": 1000000}}),
            ),
            acct,
        );
        let mux = body(&dispatch(&r).unwrap())["multiplex"].clone();
        let mux_id = mux["id"].as_str().unwrap().to_string();
        assert_eq!(
            mux["arn"].as_str().unwrap(),
            format!("arn:aws:medialive:us-east-1:{acct}:multiplex:{mux_id}")
        );
        // Describing resolves CREATING → IDLE and recounts programs.
        r = scoped(
            req("GET", format!("/prod/multiplexes/{mux_id}"), json!({})),
            acct,
        );
        let desc = body(&dispatch(&r).unwrap());
        assert_eq!(desc["state"], "IDLE");
        assert_eq!(desc["programCount"], 0);

        r = scoped(
            req(
                "POST",
                format!("/prod/multiplexes/{mux_id}/programs"),
                json!({"programName": "prog-1", "multiplexProgramSettings": {"programNumber": 1}}),
            ),
            acct,
        );
        let program = body(&dispatch(&r).unwrap())["multiplexProgram"].clone();
        assert_eq!(program["programName"], "prog-1");

        r = scoped(
            req(
                "PUT",
                format!("/prod/multiplexes/{mux_id}/programs/prog-1"),
                json!({"multiplexProgramSettings": {"programNumber": 2}}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["multiplexProgram"]["multiplexProgramSettings"]
                ["programNumber"],
            2
        );

        r = scoped(
            req(
                "GET",
                format!("/prod/multiplexes/{mux_id}/programs"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["multiplexPrograms"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        r = scoped(
            req("GET", format!("/prod/multiplexes/{mux_id}"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["programCount"], 1);

        r = scoped(
            req(
                "POST",
                format!("/prod/multiplexes/{mux_id}/start"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(
            req("GET", format!("/prod/multiplexes/{mux_id}"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "RUNNING");

        r = scoped(
            req(
                "POST",
                format!("/prod/multiplexes/{mux_id}/stop"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "STOPPING");
        r = scoped(
            req("GET", format!("/prod/multiplexes/{mux_id}"), json!({})),
            acct,
        );
        // Describing resolves STOPPING → IDLE.
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "IDLE");

        r = scoped(
            req(
                "DELETE",
                format!("/prod/multiplexes/{mux_id}/programs/prog-1"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["programName"], "prog-1");
        r = scoped(
            req("DELETE", format!("/prod/multiplexes/{mux_id}"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETING");
    }

    #[test]
    fn tags_round_trip_on_list_and_delete() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/inputs",
                json!({"name": "inp-tags", "type": "URL_PULL", "tags": {"k1": "v1"}}),
            ),
            acct,
        );
        let input = create_input(&r);
        let arn = input["arn"].as_str().unwrap().to_string();

        r = scoped(
            req(
                "POST",
                format!("/prod/tags/{}", url_encode_path(&arn)),
                json!({"tags": {"k2": "v2"}}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = scoped(
            req(
                "GET",
                format!("/prod/tags/{}", url_encode_path(&arn)),
                json!({}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["tags"],
            json!({"k1": "v1", "k2": "v2"})
        );

        r = scoped(
            req_query(
                "DELETE",
                &format!("/prod/tags/{}", url_encode_path(&arn)),
                "tagKeys=k1&tagKeys=k2",
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(
            req(
                "GET",
                format!("/prod/tags/{}", url_encode_path(&arn)),
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["tags"], json!({}));
    }

    #[test]
    fn schedule_round_trip_on_a_channel() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        let id = body(&dispatch(&r).unwrap())["channel"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let action = json!({"actionName": "pause", "scheduleActionSettings": {}});
        r = scoped(
            req(
                "PUT",
                format!("/prod/channels/{id}/schedule"),
                json!({
                    "creates": {"scheduleActions": [action.clone()]},
                    "deletes": {"actionNames": ["gone"]},
                }),
            ),
            acct,
        );
        let result = body(&dispatch(&r).unwrap());
        assert_eq!(result["creates"]["scheduleActions"], json!([action]));
        assert_eq!(result["deletes"]["actionNames"], json!(["gone"]));

        r = scoped(
            req("GET", format!("/prod/channels/{id}/schedule"), json!({})),
            acct,
        );
        let desc = body(&dispatch(&r).unwrap());
        assert_eq!(desc["scheduleActions"], json!([action]));
        assert!(desc["nextToken"].is_null());

        r = scoped(
            req("DELETE", format!("/prod/channels/{id}/schedule"), json!({})),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(
            req("GET", format!("/prod/channels/{id}/schedule"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["scheduleActions"], json!([]));

        // Unknown channel 404s for all three schedule verbs.
        r = scoped(req("GET", "/prod/channels/nope/schedule", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn batch_operations_report_failed_shaped_entries() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(req("POST", "/prod/channels", create_channel_body()), acct);
        let id = body(&dispatch(&r).unwrap())["channel"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        r = scoped(
            req(
                "POST",
                "/prod/batch/delete",
                json!({"channelIds": [id.clone(), "missing"]}),
            ),
            acct,
        );
        let result = body(&dispatch(&r).unwrap());
        assert_eq!(
            result["successful"],
            json!([{
                "arn": format!("arn:aws:medialive:channel:{id}"),
                "id": id,
            }])
        );
        assert_eq!(
            result["failed"],
            json!([{"arn": "", "code": "404", "id": "missing", "message": "Not found"}])
        );

        r = scoped(
            req(
                "POST",
                "/prod/batch/start",
                json!({"channelIds": [id.clone()]}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["successful"][0]["id"], id);

        r = scoped(
            req(
                "POST",
                "/prod/batch/stop",
                json!({"multiplexIds": ["nope"]}),
            ),
            acct,
        );
        let result = body(&dispatch(&r).unwrap());
        assert!(result["successful"].as_array().unwrap().is_empty());
        assert_eq!(result["failed"][0]["id"], "nope");
    }

    #[test]
    fn cluster_nodes_and_channel_placement_groups() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/clusters",
                json!({"name": "cl-1", "clusterType": "ON_PREMISES"}),
            ),
            acct,
        );
        let cluster_id = body(&dispatch(&r).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Missing cluster → NotFoundException for every sub-resource verb.
        r = scoped(req("POST", "/prod/clusters/nope/nodes", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 404);
        r = scoped(
            req(
                "POST",
                "/prod/clusters/nope/nodeRegistrationScript",
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 404);

        r = scoped(
            req(
                "POST",
                format!("/prod/clusters/{cluster_id}/nodes"),
                json!({"name": "node-1"}),
            ),
            acct,
        );
        let node = body(&dispatch(&r).unwrap());
        let node_id = node["id"].as_str().unwrap().to_string();
        assert_eq!(node["connectionState"], "DISCONNECTED");
        assert_eq!(node["role"], "BACKUP");
        assert!(node["instanceArn"].is_null());

        r = scoped(
            req(
                "PUT",
                format!("/prod/clusters/{cluster_id}/nodes/{node_id}/state"),
                json!({"state": "ACTIVE"}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "ACTIVE");

        r = scoped(
            req(
                "POST",
                format!("/prod/clusters/{cluster_id}/nodeRegistrationScript"),
                json!({}),
            ),
            acct,
        );
        let script = body(&dispatch(&r).unwrap())["nodeRegistrationScript"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(script.starts_with("#!/bin/bash"));

        r = scoped(
            req(
                "POST",
                format!("/prod/clusters/{cluster_id}/channelplacementgroups"),
                json!({"name": "cpg-1", "nodes": [node_id.clone()]}),
            ),
            acct,
        );
        let group = body(&dispatch(&r).unwrap());
        let group_id = group["id"].as_str().unwrap().to_string();
        assert_eq!(group["state"], "IDLE");
        assert_eq!(group["nodes"], json!([node_id]));

        r = scoped(
            req(
                "GET",
                format!("/prod/clusters/{cluster_id}/nodes"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["nodes"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        r = scoped(
            req(
                "DELETE",
                format!("/prod/clusters/{cluster_id}/channelplacementgroups/{group_id}"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETING");
    }

    #[test]
    fn networks_and_sdi_sources_crud() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/networks",
                json!({"name": "net-1", "ipPools": [{"cidr": "10.0.0.0/24"}]}),
            ),
            acct,
        );
        let network = body(&dispatch(&r).unwrap());
        let net_id = network["id"].as_str().unwrap().to_string();
        assert_eq!(network["state"], "IDLE");
        assert_eq!(network["associatedClusterIds"], json!([]));

        r = scoped(
            req(
                "PUT",
                format!("/prod/networks/{net_id}"),
                json!({"name": "net-1b"}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["name"], "net-1b");

        r = scoped(
            req("POST", "/prod/sdiSources", json!({"name": "sdi-1"})),
            acct,
        );
        let sdi = body(&dispatch(&r).unwrap());
        let sdi_id = sdi["id"].as_str().unwrap().to_string();
        assert_eq!(sdi["mode"], "QUADRANT");
        assert_eq!(sdi["type"], "SINGLE");

        r = scoped(
            req(
                "PUT",
                format!("/prod/sdiSources/{sdi_id}"),
                json!({"type": "QUAD"}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["type"], "QUAD");

        r = scoped(req("GET", "/prod/networks", json!({})), acct);
        assert_eq!(
            body(&dispatch(&r).unwrap())["networks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        r = scoped(req("GET", "/prod/sdiSources", json!({})), acct);
        assert_eq!(
            body(&dispatch(&r).unwrap())["sdiSources"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        r = scoped(
            req("DELETE", format!("/prod/networks/{net_id}"), json!({})),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        // moto keeps the record; describing resolves DELETING → DELETED.
        r = scoped(
            req("GET", format!("/prod/networks/{net_id}"), json!({})),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "DELETED");
    }

    #[test]
    fn alarm_templates_manage_group_membership() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/cloudwatch-alarm-template-groups",
                json!({"name": "grp-1"}),
            ),
            acct,
        );
        let group = body(&dispatch(&r).unwrap());
        let group_id = group["id"].as_str().unwrap().to_string();
        assert_eq!(group["templateCount"], 0);

        r = scoped(
            req(
                "POST",
                "/prod/cloudwatch-alarm-templates",
                json!({
                    "name": "tmpl-1",
                    "groupIdentifier": "grp-1",
                    "metricName": "NetworkIn",
                    "threshold": 1000.0,
                    "statistic": "Average",
                }),
            ),
            acct,
        );
        let tmpl = body(&dispatch(&r).unwrap());
        assert_eq!(tmpl["groupId"], json!(group_id));
        let tmpl_id = tmpl["id"].as_str().unwrap().to_string();

        // Resolvable by stored id, and its group by name.
        r = scoped(
            req(
                "GET",
                format!("/prod/cloudwatch-alarm-templates/{tmpl_id}"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["metricName"].as_str(),
            Some("NetworkIn")
        );
        r = scoped(
            req(
                "GET",
                "/prod/cloudwatch-alarm-template-groups/grp-1",
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["templateCount"], 1);

        r = scoped(
            req(
                "PATCH",
                format!("/prod/cloudwatch-alarm-templates/{tmpl_id}"),
                json!({"threshold": 2000.0}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["threshold"], 2000.0);

        r = scoped(
            req(
                "DELETE",
                format!("/prod/cloudwatch-alarm-templates/{tmpl_id}"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = scoped(
            req(
                "GET",
                "/prod/cloudwatch-alarm-template-groups/grp-1",
                json!({}),
            ),
            acct,
        );
        assert_eq!(body(&dispatch(&r).unwrap())["templateCount"], 0);

        r = scoped(
            req(
                "DELETE",
                format!("/prod/cloudwatch-alarm-template-groups/{group_id}"),
                json!({}),
            ),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn signal_maps_track_monitor_deployment() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        let mut r = scoped(
            req(
                "POST",
                "/prod/signal-maps",
                json!({"name": "sig-1", "discoveryEntryPointArn": "arn:aws:medialive:channel:1"}),
            ),
            acct,
        );
        let map = body(&dispatch(&r).unwrap());
        let id = map["id"].as_str().unwrap().to_string();
        assert_eq!(map["status"], "CREATE_COMPLETE");
        assert!(map["errorMessage"].is_null());

        r = scoped(
            req(
                "PATCH",
                format!("/prod/signal-maps/{id}"),
                json!({"name": "sig-1b"}),
            ),
            acct,
        );
        let updated = body(&dispatch(&r).unwrap());
        assert_eq!(updated["name"], "sig-1b");
        assert_eq!(updated["status"], "UPDATE_COMPLETE");

        r = scoped(
            req(
                "POST",
                format!("/prod/signal-maps/{id}/monitor-deployment"),
                json!({"dryRun": true}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["monitorDeployment"]["status"],
            "DRY_RUN_COMPLETE"
        );

        r = scoped(
            req(
                "DELETE",
                format!("/prod/signal-maps/{id}/monitor-deployment"),
                json!({}),
            ),
            acct,
        );
        let cleared = body(&dispatch(&r).unwrap());
        assert_eq!(cleared["monitorDeployment"], json!({}));

        r = scoped(
            req("DELETE", format!("/prod/signal-maps/{id}"), json!({})),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn absent_wire_shapes_stay_stubbed() {
        let acct = uuid::Uuid::new_v4().as_u128() as u64;
        // moto's untouched namespaces: offered but always 404 (no local state).
        let mut r = scoped(req("GET", "/prod/inputDevices/dev-1", json!({})), acct);
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 404);
        assert_eq!(
            body(&resp)["message"].as_str(),
            Some("InputDevice dev-1 not found")
        );
        r = scoped(req("GET", "/prod/offerings/o-1", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 404);
        r = scoped(
            req("POST", "/prod/offerings/o-1/purchase", json!({"count": 1})),
            acct,
        );
        assert_eq!(dispatch(&r).unwrap().status, 404);
        r = scoped(req("DELETE", "/prod/reservations/r-1", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 404);
        r = scoped(req("GET", "/prod/inputDevices", json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["inputDevices"], json!([]));
        r = scoped(req("GET", "/prod/offerings", json!({})), acct);
        assert_eq!(body(&dispatch(&r).unwrap())["offerings"], json!([]));

        // Moto's always-fresh account configuration.
        r = scoped(req("GET", "/prod/accountConfiguration", json!({})), acct);
        assert_eq!(
            body(&dispatch(&r).unwrap())["accountConfiguration"]["kmsKeyId"],
            Value::Null
        );
        r = scoped(
            req(
                "PUT",
                "/prod/accountConfiguration",
                json!({"accountConfiguration": {"kmsKeyId": "alias/k"}}),
            ),
            acct,
        );
        assert_eq!(
            body(&dispatch(&r).unwrap())["accountConfiguration"]["kmsKeyId"].as_str(),
            Some("alias/k")
        );
        r = scoped(req("GET", "/prod/versions", json!({})), acct);
        assert_eq!(
            body(&dispatch(&r).unwrap())["versions"],
            json!([{"engineVersions": [{"version": "1.0"}]}])
        );
        // Alert stubs mirror moto's existence checks.
        r = scoped(req("GET", "/prod/channels/x/alerts", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 200); // moto never checks the channel
        r = scoped(req("GET", "/prod/multiplexes/x/alerts", json!({})), acct);
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }
}
