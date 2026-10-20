//! MediaConnect backend (REST-JSON).
//!
//! Port of moto's mediaconnect as of the deployed fork (commit b30eb48d4),
//! which routes the full current-botocore surface: flows, bridges, gateways,
//! gateway instances, offerings/reservations, router inputs, network
//! interfaces and outputs, plus tags. Wire bodies mirror moto's `json.dumps`
//! output key-for-key (camelCase per the spec's locationNames — including
//! moto's occasional `flow_arn`-style keys, which botocore simply never
//! maps). Unrouted shapes return None so the gateway 501s exactly like a
//! request moto never routed.

use crate::proto::{json_error, parse_json_body, query_first, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

type Records = Vec<(String, Value)>;

/// The three router resource kinds, which share routes and behavior.
#[derive(Clone, Copy, PartialEq)]
enum RouterKind {
    Input,
    NetworkInterface,
    Output,
}

#[derive(Default)]
struct State {
    flows: Records,
    bridges: Records,
    gateways: Records,
    gateway_instances: Records,
    router_inputs: Records,
    router_network_interfaces: Records,
    router_outputs: Records,
    reservations: Records,
    offerings: Records,
    /// moto's TaggingService: arn → key → value, one tagger for both the
    /// regular and the global tag operations.
    tags: std::collections::HashMap<String, std::collections::BTreeMap<String, String>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("mediaconnect"));
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

fn not_found(what: &str) -> Response {
    // moto's mediaconnect.NotFoundException is a JsonRESTError with code 400.
    json_error(400, "NotFoundException", what)
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto truthiness for optional dict params (`if self.maintenance:`).
fn truthy(v: &Value) -> bool {
    v.as_object().is_some_and(|o| !o.is_empty())
}

fn as_string(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or_default().to_string()
}

// ---------------------------------------------------------------------------
// Path handling: moto's url_paths bind one segment as `[^/.]+`.
// ---------------------------------------------------------------------------

struct Seg {
    text: String,
    /// False when the *raw* segment contains `.` or `/` (moto bind failure):
    /// moto's `[^/.]+` regex runs against the still-percent-encoded URL, so
    /// `%2F`/`%2E` escapes bind; only literal dots and slashes do not.
    bindable: bool,
}

fn split_path(path: &str) -> Option<Vec<Seg>> {
    let trimmed = path.strip_prefix('/')?;
    let mut segs = Vec::new();
    for raw in trimmed.split('/') {
        if raw.is_empty() {
            // Duplicate/trailing slashes do not bind moto's `[^/.]+`.
            return None;
        }
        let bindable = !raw.contains('.') && !raw.contains('/');
        let text = url_decode(raw);
        segs.push(Seg { text, bindable });
    }
    Some(segs)
}

fn seg_text(seg: &Seg) -> Option<&str> {
    seg.bindable.then_some(seg.text.as_str())
}

fn find<'a>(records: &'a Records, key: &str) -> Option<&'a Value> {
    records.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn find_mut<'a>(records: &'a mut Records, key: &str) -> Option<&'a mut Value> {
    records.iter_mut().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn take(records: &mut Records, key: &str) -> Option<Value> {
    let pos = records.iter().position(|(k, _)| *k == key)?;
    Some(records.remove(pos).1)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "mediaconnect" {
        return None;
    }
    let segs = split_path(&req.path)?;
    let slot = state();
    let mut guard = slot.write();
    guard
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| State::seeded(&req.account, &req.region));
    let st = guard.get_mut(&(req.account, req.region.clone()))?;
    route(st, req, &segs)
}

impl State {
    /// moto's MediaConnectBackend.__init__ seeds three offerings.
    fn seeded(account: &u64, region: &str) -> Self {
        let mut st = State::default();
        for i in 0..3 {
            let arn = format!(
                "arn:{}:mediaconnect:{region}:{account}:offering:{}",
                partition(region),
                hex_id()
            );
            st.offerings.push((
                arn.clone(),
                json!({
                    "offeringArn": arn,
                    "currencyCode": "USD",
                    "duration": 12,
                    "durationUnits": "MONTHS",
                    "offeringDescription": format!("Test offering {}", i + 1),
                    "pricePerUnit": format!("0.{}0", i + 1),
                    "priceUnits": "HOURLY",
                    "resourceSpecification": {
                        "resourceType": "Mbps_Outbound_Bandwidth",
                        "reservedBitrate": 100,
                    },
                }),
            ));
        }
        st
    }
}

fn route(st: &mut State, req: &Request, segs: &[Seg]) -> Option<Response> {
    // /tags/{Arn} (+ /tags/global/{Arn}) — root level, no /v1 prefix.
    if segs.len() == 2 && segs[0].text == "tags" {
        if let Some(arn) = seg_text(&segs[1]) {
            return tag_ops(st, req, arn);
        }
        return None;
    }
    if segs.len() == 3 && segs[0].text == "tags" && segs[1].text == "global" {
        if let Some(arn) = seg_text(&segs[2]) {
            return tag_ops(st, req, arn);
        }
        return None;
    }

    let segs1 = segs.first()?;
    if segs1.text != "v1" || segs.len() == 1 {
        return None;
    }
    let rest = &segs[1..];
    match rest[0].text.as_str() {
        "flows" => flow_routes(st, req, rest),
        "bridges" => bridge_routes(st, req, rest),
        "gateways" => gateway_routes(st, req, rest),
        "gateway-instances" => gateway_instance_routes(st, req, rest),
        "entitlements" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            Some(list_entitlements(st, max_results))
        }
        "offerings" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            Some(list_offerings(st, max_results))
        }
        "offerings" if rest.len() == 2 => {
            let term = seg_text(&rest[1])?;
            match req.method.as_str() {
                "GET" => Some(describe_offering(st, term)),
                "POST" => Some(purchase_offering(st, term, &parse_json_body(&req.body))),
                _ => None,
            }
        }
        "reservations" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            Some(list_reservations(st, max_results))
        }
        "reservations" if rest.len() == 2 => {
            let term = seg_text(&rest[1])?;
            match req.method.as_str() {
                "GET" => Some(describe_reservation(st, term)),
                _ => None,
            }
        }
        "routerInput" => router_resource_routes(st, req, rest, RouterKind::Input),
        "routerInputs" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            match req.method.as_str() {
                "POST" => Some(list_router_resource(
                    st,
                    params_get(&req.body, "filterArn"),
                    max_results,
                    RouterKind::Input,
                )),
                "GET" => Some(batch_router(st, query.get("arns"), RouterKind::Input)),
                _ => None,
            }
        }
        "routerNetworkInterface" => {
            router_resource_routes(st, req, rest, RouterKind::NetworkInterface)
        }
        "routerNetworkInterfaces" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            match req.method.as_str() {
                "POST" => Some(list_router_resource(
                    st,
                    params_get(&req.body, "filterArn"),
                    max_results,
                    RouterKind::NetworkInterface,
                )),
                "GET" => Some(batch_router(
                    st,
                    query.get("arns"),
                    RouterKind::NetworkInterface,
                )),
                _ => None,
            }
        }
        "routerOutput" => router_resource_routes(st, req, rest, RouterKind::Output),
        "routerOutputs" if rest.len() == 1 => {
            let query = query_map(&req.query_string);
            let max_results = query_first(&query, "maxResults");
            match req.method.as_str() {
                "POST" => Some(list_router_resource(
                    st,
                    params_get(&req.body, "filterArn"),
                    max_results,
                    RouterKind::Output,
                )),
                "GET" => Some(batch_router(st, query.get("arns"), RouterKind::Output)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn params_get(body: &[u8], key: &str) -> Option<Value> {
    parse_json_body(body).get(key).cloned()
}

fn tag_ops(st: &mut State, req: &Request, arn: &str) -> Option<Response> {
    match req.method.as_str() {
        "POST" => Some(tag_resource(st, arn, &params_get(&req.body, "tags"))),
        "GET" => Some(list_tags_for_resource(st, arn)),
        "DELETE" => {
            let keys = query_map(&req.query_string).get("tagKeys").cloned();
            Some(untag_resource(st, arn, keys.as_ref()))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tag_resource(st: &mut State, arn: &str, tags: &Option<Value>) -> Response {
    if let Some(obj) = tags.as_ref().and_then(Value::as_object) {
        let entry = st.tags.entry(arn.to_string()).or_default();
        for (k, v) in obj {
            entry.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
        }
    }
    ok_json(&json!({}))
}

fn untag_resource(st: &mut State, arn: &str, tag_keys: Option<&Vec<String>>) -> Response {
    if let Some(keys) = tag_keys {
        if let Some(entry) = st.tags.get_mut(arn) {
            for k in keys {
                entry.remove(k);
            }
        }
    }
    ok_json(&json!({}))
}

fn list_tags_for_resource(st: &mut State, arn: &str) -> Response {
    let Some(tags) = st.tags.get(arn) else {
        return not_found("Resource not found.");
    };
    ok_json(&json!({ "tags": tags }))
}

// ---------------------------------------------------------------------------
// Flows
// ---------------------------------------------------------------------------

fn flow_routes(st: &mut State, req: &Request, rest: &[Seg]) -> Option<Response> {
    let method = req.method.as_str();
    match rest.len() {
        // POST/GET /v1/flows
        1 => match method {
            "POST" => Some(create_flow(
                st,
                &parse_json_body(&req.body),
                &req.region,
                req.account,
            )),
            "GET" => Some(list_flows(
                st,
                query_first(&query_map(&req.query_string), "maxResults"),
            )),
            _ => None,
        },
        // GET/DELETE/PUT /v1/flows/{FlowArn}
        2 if rest[1].bindable => match method {
            "GET" => Some(describe_flow(st, &rest[1].text)),
            "DELETE" => Some(delete_flow(st, &rest[1].text)),
            "PUT" => Some(update_flow(st, &rest[1].text, &parse_json_body(&req.body))),
            _ => None,
        },
        3 => {
            let leaf = seg_text(&rest[2])?;
            // POST /v1/flows/start|stop/{FlowArn}
            if method == "POST" && (rest[1].text == "start" || rest[1].text == "stop") {
                let status = if rest[1].text == "start" {
                    "STARTING"
                } else {
                    "STOPPING"
                };
                return match find_mut(&mut st.flows, leaf) {
                    Some(f) => {
                        f["status"] = json!(status);
                        Some(ok_json(
                            &json!({ "flowArn": f["flowArn"], "status": status }),
                        ))
                    }
                    None => Some(not_found("Flow not found.")),
                };
            }
            let flow_arn = seg_text(&rest[1])?;
            match (method, rest[2].text.as_str()) {
                ("GET", "source-metadata") => Some(describe_flow_source_metadata(st, flow_arn)),
                ("GET", "source-thumbnail") => Some(describe_flow_source_thumbnail(st, flow_arn)),
                ("POST", "source") => {
                    Some(add_flow_sources(st, flow_arn, &parse_json_body(&req.body)))
                }
                ("POST", "outputs") => {
                    Some(add_flow_outputs(st, flow_arn, &parse_json_body(&req.body)))
                }
                ("POST", "vpcInterfaces") => Some(add_flow_vpc_interfaces(
                    st,
                    flow_arn,
                    &parse_json_body(&req.body),
                )),
                ("POST", "entitlements") => Some(grant_flow_entitlements(
                    st,
                    flow_arn,
                    &parse_json_body(&req.body),
                )),
                ("POST", "mediaStreams") => Some(add_flow_media_streams(
                    st,
                    flow_arn,
                    &parse_json_body(&req.body),
                )),
                _ => None,
            }
        }
        // /v1/flows/{FlowArn}/<sub>/{bound}
        4 if rest[1].bindable && rest[2].bindable => {
            let flow_arn = &rest[1].text;
            let bound = &rest[3].text;
            match (method, rest[2].text.as_str()) {
                ("PUT", "source") => Some(update_flow_source(
                    st,
                    flow_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                ("DELETE", "source") => Some(remove_flow_source(st, flow_arn, bound)),
                ("DELETE", "outputs") => Some(remove_flow_output(st, flow_arn, bound)),
                ("PUT", "outputs") => Some(update_flow_output(
                    st,
                    flow_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                ("DELETE", "vpcInterfaces") => Some(remove_flow_vpc_interface(st, flow_arn, bound)),
                ("DELETE", "entitlements") => Some(revoke_flow_entitlement(st, flow_arn, bound)),
                ("PUT", "entitlements") => Some(update_flow_entitlement(
                    st,
                    flow_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                ("DELETE", "mediaStreams") => Some(remove_flow_media_stream(st, flow_arn, bound)),
                ("PUT", "mediaStreams") => Some(update_flow_media_stream(
                    st,
                    flow_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Bridges
// ---------------------------------------------------------------------------

fn bridge_routes(st: &mut State, req: &Request, rest: &[Seg]) -> Option<Response> {
    let method = req.method.as_str();
    match rest.len() {
        1 => match method {
            "POST" => Some(create_bridge(
                st,
                &parse_json_body(&req.body),
                &req.region,
                req.account,
            )),
            "GET" => Some(list_bridges(
                st,
                query_first(&query_map(&req.query_string), "maxResults"),
            )),
            _ => None,
        },
        2 => {
            let bridge_arn = seg_text(&rest[1])?;
            match method {
                "GET" => Some(describe_bridge(st, bridge_arn)),
                "DELETE" => Some(delete_bridge(st, bridge_arn)),
                "PUT" => Some(update_bridge(st, bridge_arn, &parse_json_body(&req.body))),
                _ => None,
            }
        }
        3 if rest[1].bindable && rest[2].bindable => {
            let bridge_arn = &rest[1].text;
            match (method, rest[2].text.as_str()) {
                ("PUT", "state") => Some(update_bridge_state(
                    st,
                    bridge_arn,
                    &parse_json_body(&req.body),
                )),
                ("POST", "outputs") => Some(add_bridge_outputs(
                    st,
                    bridge_arn,
                    &parse_json_body(&req.body),
                )),
                ("POST", "sources") => Some(add_bridge_sources(
                    st,
                    bridge_arn,
                    &parse_json_body(&req.body),
                )),
                _ => None,
            }
        }
        4 if rest[1].bindable && rest[2].bindable && rest[3].bindable => {
            let bridge_arn = &rest[1].text;
            let bound = &rest[3].text;
            match (method, rest[2].text.as_str()) {
                ("DELETE", "outputs") => Some(remove_bridge_output(st, bridge_arn, bound)),
                ("PUT", "outputs") => Some(update_bridge_output(
                    st,
                    bridge_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                ("DELETE", "sources") => Some(remove_bridge_source(st, bridge_arn, bound)),
                ("PUT", "sources") => Some(update_bridge_source(
                    st,
                    bridge_arn,
                    bound,
                    &parse_json_body(&req.body),
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

/// moto's Bridge.to_dict(): `sourceFailoverConfig`/`egressGatewayBridge`/
/// `ingressGatewayBridge` only when truthy.
fn bridge_full(b: &Value) -> Value {
    let mut out = json!({
        "bridgeArn": b["bridgeArn"],
        "bridgeState": b["bridgeState"],
        "name": b["name"],
        "placementArn": b["placementArn"],
        "sources": b["sources"],
        "outputs": b["outputs"],
    });
    if truthy(&b["sourceFailoverConfig"]) {
        out["sourceFailoverConfig"] = b["sourceFailoverConfig"].clone();
    }
    if truthy(&b["egressGatewayBridge"]) {
        out["egressGatewayBridge"] = b["egressGatewayBridge"].clone();
    }
    if truthy(&b["ingressGatewayBridge"]) {
        out["ingressGatewayBridge"] = b["ingressGatewayBridge"].clone();
    }
    out
}

fn create_bridge(st: &mut State, params: &Value, region: &str, account: u64) -> Response {
    let id = hex_id();
    let part = partition(region);
    let name = as_string(params.get("name"));
    let bridge_arn = format!("arn:{part}:mediaconnect:{region}:{account}:bridge:{id}:{name}");
    // Source entries are unions: {flowSource: …} | {networkSource: …}; moto
    // injects a `bridge-source` ARN into whichever member is present.
    let mut sources = params["sources"].as_array().cloned().unwrap_or_default();
    for source in sources.iter_mut() {
        let inner = if source.get("flowSource").is_some() {
            source.get_mut("flowSource")
        } else {
            source.get_mut("networkSource")
        };
        if let Some(inner) = inner {
            inner["sourceArn"] = json!(format!(
                "arn:{part}:mediaconnect:{region}:{account}:bridge-source:{}:{}",
                hex_id(),
                inner["name"].as_str().unwrap_or_default()
            ));
        }
    }
    let mut outputs = params["outputs"].as_array().cloned().unwrap_or_default();
    for output in outputs.iter_mut() {
        if let Some(inner) = output.get_mut("networkOutput") {
            inner["outputArn"] = json!(format!(
                "arn:{part}:mediaconnect:{region}:{account}:bridge-output:{}:{}",
                hex_id(),
                inner["name"].as_str().unwrap_or_default()
            ));
        }
    }
    let bridge = json!({
        "bridgeArn": bridge_arn,
        "bridgeState": "ACTIVE",
        "name": name,
        "placementArn": as_string(params.get("placementArn")),
        "sources": sources,
        "outputs": outputs,
        "sourceFailoverConfig": params["sourceFailoverConfig"].clone(),
        "egressGatewayBridge": params["egressGatewayBridge"].clone(),
        "ingressGatewayBridge": params["ingressGatewayBridge"].clone(),
    });
    let arn = bridge["bridgeArn"].as_str().unwrap_or_default().to_string();
    st.bridges.push((arn, bridge));
    let stored = st.bridges.last().unwrap();
    ok_json(&json!({ "bridge": bridge_full(&stored.1) }))
}

fn bridge_missing() -> Response {
    not_found("Bridge not found.")
}

fn list_bridges(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut bridges: Vec<Value> = st
        .bridges
        .iter()
        .map(|(_, b)| {
            json!({
                "bridgeArn": b["bridgeArn"],
                "bridgeState": b["bridgeState"],
                "bridgeType": "TRANSPARENT_PIPELINE",
                "name": b["name"],
                "placementArn": b["placementArn"],
            })
        })
        .collect();
    if let Some(n) = limit {
        bridges.truncate(n);
    }
    ok_json(&json!({ "bridges": bridges }))
}

fn describe_bridge(st: &mut State, bridge_arn: &str) -> Response {
    match find(&st.bridges, bridge_arn) {
        Some(b) => ok_json(&json!({ "bridge": bridge_full(b) })),
        None => bridge_missing(),
    }
}

fn delete_bridge(st: &mut State, bridge_arn: &str) -> Response {
    match take(&mut st.bridges, bridge_arn) {
        Some(_) => ok_json(&json!({ "bridgeArn": bridge_arn })),
        None => bridge_missing(),
    }
}

fn update_bridge(st: &mut State, bridge_arn: &str, params: &Value) -> Response {
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    if !params["sourceFailoverConfig"].is_null() {
        b["sourceFailoverConfig"] = params["sourceFailoverConfig"].clone();
    }
    if !params["egressGatewayBridge"].is_null() {
        b["egressGatewayBridge"] = params["egressGatewayBridge"].clone();
    }
    if !params["ingressGatewayBridge"].is_null() {
        b["ingressGatewayBridge"] = params["ingressGatewayBridge"].clone();
    }
    ok_json(&json!({ "bridge": bridge_full(b) }))
}

fn update_bridge_state(st: &mut State, bridge_arn: &str, params: &Value) -> Response {
    let desired = params["desiredState"].as_str().unwrap_or("ACTIVE");
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    b["bridgeState"] = json!(desired);
    ok_json(&json!({ "bridgeArn": b["bridgeArn"], "desiredState": b["bridgeState"] }))
}

fn add_bridge_outputs(st: &mut State, bridge_arn: &str, params: &Value) -> Response {
    // moto's backend knows region/account; the ARN carries both.
    let region = parse_region_from_arn(bridge_arn).unwrap_or_else(|| "us-east-1".into());
    let part = partition(&region);
    let account = parse_account_from_arn(bridge_arn).unwrap_or(0);
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let mut outputs = params["outputs"].as_array().cloned().unwrap_or_default();
    for output in outputs.iter_mut() {
        if let Some(inner) = output.get_mut("networkOutput") {
            let id = hex_id();
            inner["outputArn"] = json!(format!(
                "arn:{part}:mediaconnect:{region}:{account}:bridge-output:{id}:{}",
                inner["name"].as_str().unwrap_or_default()
            ));
        }
    }
    let mut existing = b["outputs"].as_array().cloned().unwrap_or_default();
    existing.extend(outputs.clone());
    b["outputs"] = json!(existing);
    ok_json(&json!({ "bridgeArn": bridge_arn, "outputs": outputs }))
}

/// `arn:aws:mediaconnect:us-east-1:123:...` → "us-east-1"
fn parse_region_from_arn(arn: &str) -> Option<String> {
    arn.split(':')
        .nth(3)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn parse_account_from_arn(arn: &str) -> Option<u64> {
    arn.split(':').nth(4).and_then(|p| p.parse().ok())
}

fn remove_bridge_output(st: &mut State, bridge_arn: &str, output_name: &str) -> Response {
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let original = b["outputs"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|output| output["networkOutput"]["name"].as_str() != Some(output_name))
        .cloned()
        .collect();
    if kept.len() == original.len() {
        return not_found(&format!("output {output_name} not found"));
    }
    b["outputs"] = json!(kept);
    ok_json(&json!({ "bridgeArn": bridge_arn, "outputName": output_name }))
}

fn update_bridge_output(
    st: &mut State,
    bridge_arn: &str,
    output_name: &str,
    params: &Value,
) -> Response {
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let mut outputs = b["outputs"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for output in outputs.iter_mut() {
        if output["networkOutput"]["name"].as_str() == Some(output_name) {
            if let (Some(t), Some(u)) = (
                output
                    .get_mut("networkOutput")
                    .and_then(|v| v.as_object_mut()),
                params.get("networkOutput").and_then(|v| v.as_object()),
            ) {
                for (k, v) in u {
                    t.insert(k.clone(), v.clone());
                }
            }
            hit = Some(output.clone());
            break;
        }
    }
    if let Some(updated) = hit {
        b["outputs"] = json!(outputs);
        return ok_json(&json!({ "bridgeArn": bridge_arn, "output": updated }));
    }
    not_found(&format!("output {output_name} not found"))
}

fn add_bridge_sources(st: &mut State, bridge_arn: &str, params: &Value) -> Response {
    let region = parse_region_from_arn(bridge_arn).unwrap_or_else(|| "us-east-1".into());
    let part = partition(&region);
    let account = parse_account_from_arn(bridge_arn).unwrap_or(0);
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let mut sources = params["sources"].as_array().cloned().unwrap_or_default();
    for source in sources.iter_mut() {
        let inner = if source.get("flowSource").is_some() {
            source.get_mut("flowSource")
        } else {
            source.get_mut("networkSource")
        };
        if let Some(inner) = inner {
            inner["sourceArn"] = json!(format!(
                "arn:{part}:mediaconnect:{region}:{account}:bridge-source:{}:{}",
                hex_id(),
                inner["name"].as_str().unwrap_or_default()
            ));
        }
    }
    let mut existing = b["sources"].as_array().cloned().unwrap_or_default();
    existing.extend(sources.clone());
    b["sources"] = json!(existing);
    ok_json(&json!({ "bridgeArn": bridge_arn, "sources": sources }))
}

fn remove_bridge_source(st: &mut State, bridge_arn: &str, source_name: &str) -> Response {
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let original = b["sources"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|source| {
            source["flowSource"]["name"].as_str() != Some(source_name)
                && source["networkSource"]["name"].as_str() != Some(source_name)
        })
        .cloned()
        .collect();
    if kept.len() == original.len() {
        return not_found(&format!("source {source_name} not found"));
    }
    b["sources"] = json!(kept);
    ok_json(&json!({ "bridgeArn": bridge_arn, "sourceName": source_name }))
}

fn update_bridge_source(
    st: &mut State,
    bridge_arn: &str,
    source_name: &str,
    params: &Value,
) -> Response {
    let Some(b) = find_mut(&mut st.bridges, bridge_arn) else {
        return bridge_missing();
    };
    let mut sources = b["sources"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for source in sources.iter_mut() {
        if source["flowSource"]["name"].as_str() == Some(source_name) {
            if let (Some(t), Some(u)) = (
                source.get_mut("flowSource").and_then(|v| v.as_object_mut()),
                params.get("flowSource").and_then(|v| v.as_object()),
            ) {
                for (k, v) in u {
                    t.insert(k.clone(), v.clone());
                }
            }
            hit = Some(source.clone());
            break;
        }
        if source["networkSource"]["name"].as_str() == Some(source_name) {
            if let (Some(t), Some(u)) = (
                source
                    .get_mut("networkSource")
                    .and_then(|v| v.as_object_mut()),
                params.get("networkSource").and_then(|v| v.as_object()),
            ) {
                for (k, v) in u {
                    t.insert(k.clone(), v.clone());
                }
            }
            hit = Some(source.clone());
            break;
        }
    }
    if let Some(updated) = hit {
        b["sources"] = json!(sources);
        return ok_json(&json!({ "bridgeArn": bridge_arn, "source": updated }));
    }
    not_found(&format!("source {source_name} not found"))
}

// ---------------------------------------------------------------------------
// Gateways and gateway instances
// ---------------------------------------------------------------------------

fn gateway_routes(st: &mut State, req: &Request, rest: &[Seg]) -> Option<Response> {
    let method = req.method.as_str();
    match rest.len() {
        1 => match method {
            "POST" => Some(create_gateway(
                st,
                &parse_json_body(&req.body),
                &req.region,
                req.account,
            )),
            "GET" => Some(list_gateways(
                st,
                query_first(&query_map(&req.query_string), "maxResults"),
            )),
            _ => None,
        },
        2 => {
            let gateway_arn = seg_text(&rest[1])?;
            match method {
                "GET" => Some(describe_gateway(st, gateway_arn)),
                "DELETE" => Some(delete_gateway(st, gateway_arn)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn gateway_missing() -> Response {
    not_found("Gateway not found.")
}

fn gateway_instance_routes(st: &mut State, req: &Request, rest: &[Seg]) -> Option<Response> {
    let method = req.method.as_str();
    match rest.len() {
        1 => match method {
            "GET" => Some(list_gateway_instances(
                st,
                query_first(&query_map(&req.query_string), "filterArn"),
                query_first(&query_map(&req.query_string), "maxResults"),
            )),
            _ => None,
        },
        2 => {
            let instance_arn = seg_text(&rest[1])?;
            match method {
                "GET" => Some(describe_gateway_instance(st, instance_arn)),
                "DELETE" => Some(deregister_gateway_instance(st, instance_arn)),
                "PUT" => Some(update_gateway_instance(
                    st,
                    instance_arn,
                    &parse_json_body(&req.body),
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

fn create_gateway(st: &mut State, params: &Value, region: &str, account: u64) -> Response {
    let id = hex_id();
    let part = partition(region);
    let name = as_string(params.get("name"));
    let gateway_arn = format!("arn:{part}:mediaconnect:{region}:{account}:gateway:{id}:{name}");
    let gateway = json!({
        "egressCidrBlocks": params["egressCidrBlocks"].as_array().cloned().unwrap_or_default(),
        "gatewayArn": gateway_arn,
        "gatewayMessages": [],
        "gatewayState": "ACTIVE",
        "name": name,
        "networks": params["networks"].as_array().cloned().unwrap_or_default(),
    });
    let arn = gateway["gatewayArn"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    st.gateways.push((arn, gateway));
    let stored = st.gateways.last().unwrap();
    ok_json(&json!({ "gateway": stored.1.clone() }))
}

fn list_gateways(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut gateways: Vec<Value> = st
        .gateways
        .iter()
        .map(|(_, g)| {
            json!({
                "gatewayArn": g["gatewayArn"],
                "gatewayState": g["gatewayState"],
                "name": g["name"],
            })
        })
        .collect();
    if let Some(n) = limit {
        gateways.truncate(n);
    }
    ok_json(&json!({ "gateways": gateways }))
}

fn describe_gateway(st: &mut State, gateway_arn: &str) -> Response {
    match find(&st.gateways, gateway_arn) {
        Some(g) => ok_json(&json!({ "gateway": g.clone() })),
        None => gateway_missing(),
    }
}

fn delete_gateway(st: &mut State, gateway_arn: &str) -> Response {
    match take(&mut st.gateways, gateway_arn) {
        Some(_) => ok_json(&json!({ "gatewayArn": gateway_arn })),
        None => gateway_missing(),
    }
}

fn instance_missing() -> Response {
    not_found("Gateway instance not found.")
}

fn describe_gateway_instance(st: &mut State, instance_arn: &str) -> Response {
    match find(&st.gateway_instances, instance_arn) {
        Some(i) => ok_json(&json!({ "gatewayInstance": i.clone() })),
        None => instance_missing(),
    }
}

fn deregister_gateway_instance(st: &mut State, instance_arn: &str) -> Response {
    match take(&mut st.gateway_instances, instance_arn) {
        Some(_) => ok_json(&json!({ "gatewayInstanceArn": instance_arn })),
        None => instance_missing(),
    }
}

fn list_gateway_instances(
    st: &State,
    filter_arn: Option<&str>,
    max_results: Option<&str>,
) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut instances: Vec<Value> = st
        .gateway_instances
        .iter()
        .filter(|(_, i)| filter_arn.is_none_or(|f| i["gatewayArn"].as_str() == Some(f)))
        .map(|(_, i)| {
            json!({
                "gatewayArn": i["gatewayArn"],
                "gatewayInstanceArn": i["gatewayInstanceArn"],
                "instanceId": i["instanceId"],
                "instanceState": i["instanceState"],
            })
        })
        .collect();
    if let Some(n) = limit {
        instances.truncate(n);
    }
    ok_json(&json!({ "instances": instances }))
}

fn update_gateway_instance(st: &mut State, instance_arn: &str, params: &Value) -> Response {
    let Some(i) = find_mut(&mut st.gateway_instances, instance_arn) else {
        return instance_missing();
    };
    let placement = params["bridgePlacement"].as_str().unwrap_or("AVAILABLE");
    i["bridgePlacement"] = json!(placement);
    ok_json(&json!({
        "bridgePlacement": i["bridgePlacement"],
        "gatewayInstanceArn": i["gatewayInstanceArn"],
    }))
}

// ---------------------------------------------------------------------------
// Offerings and reservations
// ---------------------------------------------------------------------------

fn list_offerings(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut offerings: Vec<Value> = st.offerings.iter().map(|(_, o)| o.clone()).collect();
    if let Some(n) = limit {
        offerings.truncate(n);
    }
    ok_json(&json!({ "offerings": offerings }))
}

fn describe_offering(st: &mut State, offering_arn: &str) -> Response {
    match find(&st.offerings, offering_arn) {
        Some(o) => ok_json(&json!({ "offering": o.clone() })),
        None => not_found("Offering not found."),
    }
}

fn purchase_offering(st: &mut State, offering_arn: &str, params: &Value) -> Response {
    let Some(offering) = find(&st.offerings, offering_arn).cloned() else {
        return not_found("Offering not found.");
    };
    let region = parse_region_from_arn(offering_arn).unwrap_or_else(|| "us-east-1".into());
    let account = parse_account_from_arn(offering_arn).unwrap_or(0);
    let id = hex_id();
    let reservation_arn = format!(
        "arn:{}:mediaconnect:{region}:{account}:reservation:{id}",
        partition(&region)
    );
    let reservation = json!({
        "currencyCode": offering["currencyCode"],
        "duration": offering["duration"],
        "durationUnits": offering["durationUnits"],
        "end": "2025-01-01T00:00:00Z",
        "offeringArn": offering["offeringArn"],
        "offeringDescription": offering["offeringDescription"],
        "pricePerUnit": offering["pricePerUnit"],
        "priceUnits": offering["priceUnits"],
        "reservationArn": reservation_arn,
        "reservationName": as_string(params.get("reservationName")),
        "reservationState": "ACTIVE",
        "resourceSpecification": offering["resourceSpecification"],
        "start": as_string(params.get("start")),
    });
    let arn = reservation["reservationArn"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    st.reservations.push((arn, reservation.clone()));
    ok_json(&json!({ "reservation": reservation }))
}

fn list_reservations(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut reservations: Vec<Value> = st.reservations.iter().map(|(_, r)| r.clone()).collect();
    if let Some(n) = limit {
        reservations.truncate(n);
    }
    ok_json(&json!({ "reservations": reservations }))
}

fn describe_reservation(st: &mut State, reservation_arn: &str) -> Response {
    match find(&st.reservations, reservation_arn) {
        Some(r) => ok_json(&json!({ "reservation": r.clone() })),
        None => not_found("Reservation not found."),
    }
}

fn list_entitlements(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut entitlements: Vec<Value> = st
        .flows
        .iter()
        .flat_map(|(_, f)| f["entitlements"].as_array().cloned().unwrap_or_default())
        .map(|ent| entitlement_list_row(&ent))
        .collect();
    if let Some(n) = limit {
        entitlements.truncate(n);
    }
    ok_json(&json!({ "entitlements": entitlements }))
}

/// moto's list_entitlements rows: fee percent, arn, and the name.
fn entitlement_list_row(ent: &Value) -> Value {
    json!({
        "dataTransferSubscriberFeePercent":
            ent.get("dataTransferSubscriberFeePercent").and_then(Value::as_i64).unwrap_or(0),
        "entitlementArn": ent["entitlementArn"],
        "entitlementName": ent["name"],
    })
}

// ---------------------------------------------------------------------------
// Flow operation bodies
// ---------------------------------------------------------------------------

/// moto's Flow.to_dict(): the stored record projected onto moto's key set;
/// `maintenance` only when truthy (moto's `if self.maintenance:`).
fn flow_full(f: &Value) -> Value {
    let mut out = json!({
        "availabilityZone": f["availabilityZone"],
        "description": f["description"],
        "egressIp": f["egressIp"],
        "entitlements": f["entitlements"],
        "flowArn": f["flowArn"],
        "mediaStreams": f["mediaStreams"],
        "name": f["name"],
        "outputs": f["outputs"],
        "source": f["source"],
        "sourceFailoverConfig": f["sourceFailoverConfig"],
        "sources": f["sources"],
        "status": f["status"],
        "vpcInterfaces": f["vpcInterfaces"],
    });
    if truthy(&f["maintenance"]) {
        out["maintenance"] = f["maintenance"].clone();
    }
    out
}

fn list_flows(st: &State, max_results: Option<&str>) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let mut flows: Vec<Value> = st
        .flows
        .iter()
        .map(|(_, f)| {
            json!({
                "availabilityZone": f["availabilityZone"],
                "description": f["description"],
                "flowArn": f["flowArn"],
                "name": f["name"],
                "sourceType": "OWNED",
                "status": f["status"],
            })
        })
        .collect();
    if let Some(n) = limit {
        flows.truncate(n);
    }
    ok_json(&json!({ "flows": flows }))
}

/// moto's resolve_transient_states: STARTING→ACTIVE, STOPPING→STANDBY,
/// UPDATING→previous status.
fn resolve_transient(f: &mut Value) {
    match f["status"].as_str() {
        Some("STARTING") => f["status"] = json!("ACTIVE"),
        Some("STOPPING") => f["status"] = json!("STANDBY"),
        Some("UPDATING") => {
            let prev = f["_previousStatus"].clone();
            f["status"] = prev;
            f["_previousStatus"] = Value::Null;
        }
        _ => {}
    }
}

fn describe_flow(st: &mut State, flow_arn: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return not_found("Flow not found.");
    };
    resolve_transient(f);
    ok_json(&json!({ "flow": flow_full(f) }))
}

fn delete_flow(st: &mut State, flow_arn: &str) -> Response {
    let Some(f) = take(&mut st.flows, flow_arn) else {
        return not_found("Flow not found.");
    };
    ok_json(&json!({ "flowArn": f["flowArn"], "status": f["status"] }))
}

fn update_flow(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return not_found("Flow not found.");
    };
    if !params["sourceFailoverConfig"].is_null() {
        f["sourceFailoverConfig"] = params["sourceFailoverConfig"].clone();
    }
    if !params["maintenance"].is_null() {
        f["maintenance"] = params["maintenance"].clone();
    }
    f["_previousStatus"] = f["status"].clone();
    f["status"] = json!("UPDATING");
    ok_json(&json!({ "flow": flow_full(f) }))
}

fn create_flow(st: &mut State, params: &Value, region: &str, account: u64) -> Response {
    let id = hex_id();
    let part = partition(region);
    let name = as_string(params.get("name"));
    let flow_arn = format!("arn:{part}:mediaconnect:{region}:{account}:flow:{id}:{name}");
    let mut sources = params["sources"].as_array().cloned().unwrap_or_default();
    // moto: `if self.source and not self.sources: self.sources = [self.source]`
    // — the same dict stays aliased, so ARN injection lands in both.
    let mut aliased = false;
    if sources.is_empty() && truthy_source(&params["source"]) {
        sources = vec![params["source"].clone()];
        aliased = true;
    }
    // moto's _create_flow_add_details
    for (index, source) in sources.iter_mut().enumerate() {
        source["sourceArn"] = json!(format!(
            "arn:{part}:mediaconnect:{region}:{account}:source:{id}:{}",
            source["name"].as_str().unwrap_or_default()
        ));
        if source["entitlementArn"].is_null() {
            source["ingestIp"] = json!(format!("127.0.0.{index}"));
        }
    }
    let mut outputs = params["outputs"].as_array().cloned().unwrap_or_default();
    for (index, output) in outputs.iter_mut().enumerate() {
        if matches!(
            output["protocol"].as_str(),
            Some("srt-listener") | Some("zixi-pull")
        ) {
            output["listenerAddress"] = json!(format!("{index}.0.0.0"));
        }
        let output_id = hex_id();
        output["outputArn"] = json!(format!(
            "arn:{part}:mediaconnect:{region}:{account}:output:{output_id}:{}",
            output["name"].as_str().unwrap_or_default()
        ));
    }
    let mut entitlements = params["entitlements"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for ent in entitlements.iter_mut() {
        let ent_id = hex_id();
        ent["entitlementArn"] = json!(format!(
            "arn:{part}:mediaconnect:{region}:{account}:entitlement:{ent_id}:{}",
            ent["name"].as_str().unwrap_or_default()
        ));
    }
    let source = if aliased {
        sources.first().cloned().unwrap_or(Value::Null)
    } else {
        params["source"].clone()
    };
    let flow = json!({
        "availabilityZone": params["availabilityZone"].clone(),
        "entitlements": entitlements,
        "name": name,
        "outputs": outputs,
        "source": source,
        "sourceFailoverConfig": params["sourceFailoverConfig"].clone(),
        "sources": sources,
        "vpcInterfaces": params["vpcInterfaces"].clone(),
        "mediaStreams": params["mediaStreams"].clone(),
        "status": "STANDBY",
        "_previousStatus": Value::Null,
        "maintenance": params["maintenance"].clone(),
        "description": "A Moto test flow",
        "flowArn": flow_arn,
        "egressIp": "127.0.0.1",
        "id": id,
    });
    let arn = flow["flowArn"].as_str().unwrap_or_default().to_string();
    st.flows.push((arn, flow));
    let stored = st.flows.last().unwrap();
    ok_json(&json!({ "flow": flow_full(&stored.1) }))
}

/// moto's `if self.source and not self.sources` — a dict counts, null/{}
/// don't... except `{}` is falsy for dicts; an empty `{}` source is falsy.
fn truthy_source(v: &Value) -> bool {
    v.as_object().is_some_and(|o| !o.is_empty())
}

fn flow_missing(flow_arn: &str) -> Response {
    not_found(&format!("flow with arn={flow_arn} not found"))
}

fn add_flow_sources(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let region = parse_region_from_arn(flow_arn).unwrap_or_else(|| "us-east-1".into());
    let part = partition(&region);
    let account = parse_account_from_arn(flow_arn).unwrap_or(0);
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut sources = params["sources"].as_array().cloned().unwrap_or_default();
    for source in sources.iter_mut() {
        let id = hex_id();
        source["sourceArn"] = json!(format!(
            "arn:{part}:mediaconnect:{region}:{account}:source:{id}:{}",
            source["name"].as_str().unwrap_or_default()
        ));
    }
    f["sources"] = json!(sources);
    ok_json(&json!({ "flow_arn": flow_arn, "sources": sources }))
}

fn update_flow_source(
    st: &mut State,
    flow_arn: &str,
    source_arn: &str,
    params: &Value,
) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut sources = f["sources"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for source in sources.iter_mut() {
        if source.get("sourceArn").and_then(Value::as_str) == Some(source_arn) {
            let fields = [
                ("decryption", "decryption"),
                ("description", "description"),
                ("entitlementArn", "entitlementArn"),
                ("ingestPort", "ingestPort"),
                ("maxBitrate", "maxBitrate"),
                ("maxLatency", "maxLatency"),
                ("maxSyncBuffer", "maxSyncbuffer"),
                (
                    "mediaStreamSourceConfigurations",
                    "mediaStreamSourceConfigurations",
                ),
                ("minLatency", "minLatency"),
                ("protocol", "protocol"),
                ("senderControlPort", "senderControlPort"),
                ("senderIpAddress", "senderIpAddress"),
                ("streamId", "streamId"),
                ("vpcInterfaceName", "vpcInterfaceName"),
                ("whitelistCidr", "whitelistCidr"),
            ];
            for (key, param) in fields {
                source[key] = params.get(param).cloned().unwrap_or(Value::Null);
            }
            hit = Some(source.clone());
            break;
        }
    }
    f["sources"] = json!(sources);
    match hit {
        Some(updated) => ok_json(&json!({ "flow_arn": flow_arn, "source": updated })),
        None => ok_json(&json!({ "flow_arn": flow_arn, "source": {} })),
    }
}

fn remove_flow_source(st: &mut State, flow_arn: &str, source_arn: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let original = f["sources"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|s| s.get("sourceArn").and_then(Value::as_str) != Some(source_arn))
        .cloned()
        .collect();
    if kept.len() == original.len() {
        return not_found(&format!("source with arn={source_arn} not found"));
    }
    f["sources"] = json!(kept);
    ok_json(&json!({ "flowArn": flow_arn, "sourceArn": source_arn }))
}

fn add_flow_outputs(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let outputs = params["outputs"].as_array().cloned().unwrap_or_default();
    f["outputs"] = json!(outputs);
    ok_json(&json!({ "flow_arn": flow_arn, "outputs": outputs }))
}

fn remove_flow_output(st: &mut State, flow_arn: &str, output_name: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let original = f["outputs"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|o| o["name"].as_str() != Some(output_name))
        .cloned()
        .collect();
    f["outputs"] = json!(kept);
    ok_json(&json!({ "flow_arn": flow_arn, "output_name": output_name }))
}

fn update_flow_output(
    st: &mut State,
    flow_arn: &str,
    output_arn: &str,
    params: &Value,
) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut outputs = f["outputs"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for output in outputs.iter_mut() {
        if output.get("outputArn").and_then(Value::as_str) == Some(output_arn) {
            for key in [
                "cidrAllowList",
                "description",
                "destination",
                "encryption",
                "maxLatency",
                "mediaStreamOutputConfiguration",
                "minLatency",
                "port",
                "protocol",
                "remoteId",
                "senderControlPort",
                "senderIpAddress",
                "smoothingLatency",
                "streamId",
                "vpcInterfaceAttachment",
            ] {
                output[key] = params.get(key).cloned().unwrap_or(Value::Null);
            }
            hit = Some(output.clone());
            break;
        }
    }
    if let Some(updated) = hit {
        f["outputs"] = json!(outputs);
        return ok_json(&json!({ "flowArn": flow_arn, "output": updated }));
    }
    not_found(&format!("output with arn={output_arn} not found"))
}

fn add_flow_vpc_interfaces(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let vpc_interfaces = params["vpcInterfaces"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    f["vpcInterfaces"] = json!(vpc_interfaces.clone());
    ok_json(&json!({ "flow_arn": flow_arn, "vpc_interfaces": vpc_interfaces }))
}

fn remove_flow_vpc_interface(st: &mut State, flow_arn: &str, name: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let original = f["vpcInterfaces"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|v| v["name"].as_str() != Some(name))
        .cloned()
        .collect();
    f["vpcInterfaces"] = json!(kept);
    ok_json(&json!({ "flow_arn": flow_arn, "vpc_interface_name": name }))
}

fn grant_flow_entitlements(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let region = parse_region_from_arn(flow_arn).unwrap_or_else(|| "us-east-1".into());
    let part = partition(&region);
    let account = parse_account_from_arn(flow_arn).unwrap_or(0);
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut granted = params["entitlements"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for ent in granted.iter_mut() {
        let id = hex_id();
        ent["entitlementArn"] = json!(format!(
            "arn:{part}:mediaconnect:{region}:{account}:entitlement:{id}:{}",
            ent["name"].as_str().unwrap_or_default()
        ));
    }
    let mut existing = f["entitlements"].as_array().cloned().unwrap_or_default();
    existing.extend(granted.clone());
    f["entitlements"] = json!(existing);
    ok_json(&json!({ "flow_arn": flow_arn, "entitlements": granted }))
}

fn revoke_flow_entitlement(st: &mut State, flow_arn: &str, ent_arn: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let original = f["entitlements"].as_array().cloned().unwrap_or_default();
    let mut found = false;
    let kept: Vec<Value> = original
        .iter()
        .filter(|ent| {
            if !found && ent.get("entitlementArn").and_then(Value::as_str) == Some(ent_arn) {
                found = true;
                false
            } else {
                true
            }
        })
        .cloned()
        .collect();
    if !found {
        return not_found(&format!("entitlement with arn={ent_arn} not found"));
    }
    f["entitlements"] = json!(kept);
    ok_json(&json!({ "flowArn": flow_arn, "entitlementArn": ent_arn }))
}

fn update_flow_entitlement(
    st: &mut State,
    flow_arn: &str,
    ent_arn: &str,
    params: &Value,
) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut entitlements = f["entitlements"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for ent in entitlements.iter_mut() {
        if ent.get("entitlementArn").and_then(Value::as_str) == Some(ent_arn) {
            for key in [
                "description",
                "encryption",
                "entitlementStatus",
                "name",
                "subscribers",
            ] {
                ent[key] = params.get(key).cloned().unwrap_or(Value::Null);
            }
            hit = Some(ent.clone());
            break;
        }
    }
    if let Some(updated) = hit {
        f["entitlements"] = json!(entitlements);
        return ok_json(&json!({ "flowArn": flow_arn, "entitlement": updated }));
    }
    not_found(&format!("entitlement with arn={ent_arn} not found"))
}

fn add_flow_media_streams(st: &mut State, flow_arn: &str, params: &Value) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut streams = params["mediaStreams"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for stream in streams.iter_mut() {
        if stream.get("mediaStreamId").is_none() {
            // moto: random.randint(1, 999999)
            let raw = uuid::Uuid::new_v4().as_u128() % 999_999 + 1;
            stream["mediaStreamId"] = json!(raw as u64);
        }
        if stream.get("mediaStreamType").is_none() {
            stream["mediaStreamType"] = json!("video");
        }
        if stream.get("fmt").is_none() {
            stream["fmt"] = json!({});
        }
    }
    let mut existing = f["mediaStreams"].as_array().cloned().unwrap_or_default();
    existing.extend(streams.clone());
    f["mediaStreams"] = json!(existing);
    ok_json(&json!({ "flowArn": flow_arn, "mediaStreams": streams }))
}

fn remove_flow_media_stream(st: &mut State, flow_arn: &str, name: &str) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let original = f["mediaStreams"].as_array().cloned().unwrap_or_default();
    let kept: Vec<Value> = original
        .iter()
        .filter(|s| s["mediaStreamName"].as_str() != Some(name))
        .cloned()
        .collect();
    if kept.len() == original.len() {
        return not_found(&format!("media stream {name} not found"));
    }
    f["mediaStreams"] = json!(kept);
    ok_json(&json!({ "flowArn": flow_arn, "mediaStreamName": name }))
}

fn update_flow_media_stream(
    st: &mut State,
    flow_arn: &str,
    name: &str,
    params: &Value,
) -> Response {
    let Some(f) = find_mut(&mut st.flows, flow_arn) else {
        return flow_missing(flow_arn);
    };
    let mut streams = f["mediaStreams"].as_array().cloned().unwrap_or_default();
    let mut hit = None;
    for stream in streams.iter_mut() {
        if stream["mediaStreamName"].as_str() == Some(name) {
            let fields = [
                "attributes",
                "clockRate",
                "description",
                "mediaStreamType",
                "videoFormat",
            ];
            for key in fields {
                if !params[key].is_null() {
                    stream[key] = params[key].clone();
                }
            }
            hit = Some(stream.clone());
            break;
        }
    }
    if let Some(updated) = hit {
        f["mediaStreams"] = json!(streams);
        return ok_json(&json!({ "flowArn": flow_arn, "mediaStream": updated }));
    }
    not_found(&format!("media stream {name} not found"))
}

fn describe_flow_source_metadata(st: &mut State, flow_arn: &str) -> Response {
    // moto checks existence only (describe_flow is the op that resolves
    // transient states); this handler must not mutate the flow's status.
    if find(&st.flows, flow_arn).is_none() {
        return not_found("Flow not found.");
    }
    ok_json(&json!({
        "flowArn": flow_arn,
        "messages": [],
        "timestamp": "2024-01-01T00:00:00Z",
        "transportMediaInfo": {},
    }))
}

fn describe_flow_source_thumbnail(st: &mut State, flow_arn: &str) -> Response {
    // moto checks existence only.
    if find(&st.flows, flow_arn).is_none() {
        return not_found("Flow not found.");
    };
    ok_json(&json!({
        "thumbnail": {
            "flowArn": flow_arn,
            "thumbnail": "",
            "timestamp": "2024-01-01T00:00:00Z",
        }
    }))
}

// ---------------------------------------------------------------------------
// Router inputs / network interfaces / outputs
// ---------------------------------------------------------------------------

/// Shared routing for the three singular router resources; the shapes are
/// near-identical in moto.
fn router_store(st: &mut State, kind: RouterKind) -> &mut Records {
    match kind {
        RouterKind::Input => &mut st.router_inputs,
        RouterKind::NetworkInterface => &mut st.router_network_interfaces,
        RouterKind::Output => &mut st.router_outputs,
    }
}

fn router_missing(kind: RouterKind) -> Response {
    not_found(match kind {
        RouterKind::Input => "Router input not found.",
        RouterKind::NetworkInterface => "Router network interface not found.",
        RouterKind::Output => "Router output not found.",
    })
}

fn router_resource_routes(
    st: &mut State,
    req: &Request,
    rest: &[Seg],
    kind: RouterKind,
) -> Option<Response> {
    let method = req.method.as_str();
    match rest.len() {
        1 => match method {
            "POST" => Some(match kind {
                RouterKind::Input => {
                    create_router_input(st, &parse_json_body(&req.body), &req.region, req.account)
                }
                RouterKind::NetworkInterface => create_router_network_interface(
                    st,
                    &parse_json_body(&req.body),
                    &req.region,
                    req.account,
                ),
                RouterKind::Output => {
                    create_router_output(st, &parse_json_body(&req.body), &req.region, req.account)
                }
            }),
            _ => None,
        },
        2 => {
            let arn = seg_text(&rest[1])?;
            match method {
                "GET" => Some(match find(router_store(st, kind), arn) {
                    Some(record) => ok_json(&json!({ router_wrap_key(kind): record.clone() })),
                    None => router_missing(kind),
                }),
                "PUT" => Some(update_router_resource(
                    st,
                    kind,
                    arn,
                    &parse_json_body(&req.body),
                )),
                "DELETE" => Some(delete_router_resource(st, kind, arn)),
                _ => None,
            }
        }
        3 => {
            // Literal third-segment actions some first: start/stop/restart
            // (/v1/routerInput/{action}/{Arn}) and takeRouterInput
            // (/v1/routerOutput/takeRouterInput/{RouterOutputArn}).
            match (method, rest[1].text.as_str()) {
                ("POST", action)
                    if action == "start" || action == "stop" || action == "restart" =>
                {
                    let arn = seg_text(&rest[2])?;
                    let state_name = if action == "stop" {
                        "STOPPED"
                    } else {
                        "ACTIVE"
                    };
                    let has = find(router_store(st, kind), arn).is_some();
                    if !has {
                        return Some(router_missing(kind));
                    }
                    let record = find_mut(router_store(st, kind), arn).unwrap();
                    record["state"] = json!(state_name);
                    Some(ok_json(&json!({ "arn": arn, "state": state_name })))
                }
                ("PUT", "takeRouterInput") if kind == RouterKind::Output => {
                    let arn = seg_text(&rest[2])?;
                    Some(take_router_input(st, arn, &parse_json_body(&req.body)))
                }
                _ => {
                    // Otherwise rest[1] is the resource and rest[2] is a GET
                    // sub-resource (router inputs only).
                    let arn = seg_text(&rest[1])?;
                    match (method, rest[2].text.as_str(), kind) {
                        ("GET", "source-metadata", RouterKind::Input) => {
                            Some(get_router_input_source_metadata(st, arn))
                        }
                        ("GET", "thumbnail", RouterKind::Input) => {
                            Some(get_router_input_thumbnail(st, arn))
                        }
                        _ => None,
                    }
                }
            }
        }
        _ => None,
    }
}

/// moto's keys: get_router_input returns `routerInput`, etc.
fn router_wrap_key(kind: RouterKind) -> &'static str {
    match kind {
        RouterKind::Input => "routerInput",
        RouterKind::NetworkInterface => "routerNetworkInterface",
        RouterKind::Output => "routerOutput",
    }
}

fn create_router_input(st: &mut State, params: &Value, region: &str, account: u64) -> Response {
    let part = partition(region);
    let id = hex_id();
    let name = as_string(params.get("name"));
    let arn = format!("arn:{part}:mediaconnect:{region}:{account}:router-input:{id}:{name}");
    let record = json!({
        "arn": arn,
        "dataType": default_or(params.get("dataType"), "sdi"),
        "gatewayArn": as_string(params.get("gatewayArn")),
        "ip": as_string(params.get("ip")),
        "name": name,
        "networkInterfaceArn": as_string(params.get("networkInterfaceArn")),
        "port": params["port"].as_i64().unwrap_or(0),
        "routerArn": as_string(params.get("routerArn")),
        "state": "ACTIVE",
    });
    let key = record["arn"].as_str().unwrap_or_default().to_string();
    st.router_inputs.push((key, record.clone()));
    ok_json(&json!({ "routerInput": record.clone() }))
}

fn create_router_network_interface(
    st: &mut State,
    params: &Value,
    region: &str,
    account: u64,
) -> Response {
    let part = partition(region);
    let id = hex_id();
    let name = as_string(params.get("name"));
    let arn =
        format!("arn:{part}:mediaconnect:{region}:{account}:router-network-interface:{id}:{name}");
    let record = json!({
        "arn": arn,
        "defaultGateway": default_or(params.get("defaultGateway"), "10.0.0.1"),
        "gatewayArn": as_string(params.get("gatewayArn")),
        "ipAddress": default_or(params.get("ipAddress"), "10.0.0.1"),
        "name": name,
        "networkName": as_string(params.get("networkName")),
        "routerArn": as_string(params.get("routerArn")),
        "state": "ACTIVE",
        "subnetMask": default_or(params.get("subnetMask"), "255.255.255.0"),
    });
    let key = record["arn"].as_str().unwrap_or_default().to_string();
    st.router_network_interfaces.push((key, record.clone()));
    ok_json(&json!({ "routerNetworkInterface": record }))
}

fn default_or(v: Option<&Value>, default: &str) -> Value {
    match v {
        Some(Value::String(s)) if !s.is_empty() => json!(s),
        Some(Value::Number(n)) => json!(n),
        _ => json!(default),
    }
}

fn create_router_output(st: &mut State, params: &Value, region: &str, account: u64) -> Response {
    let part = partition(region);
    let id = hex_id();
    let name = as_string(params.get("name"));
    let arn = format!("arn:{part}:mediaconnect:{region}:{account}:router-output:{id}:{name}");
    let record = json!({
        "arn": arn,
        "dataType": default_or(params.get("dataType"), "sdi"),
        "gatewayArn": as_string(params.get("gatewayArn")),
        "ip": as_string(params.get("ip")),
        "name": name,
        "networkInterfaceArn": as_string(params.get("networkInterfaceArn")),
        "port": params["port"].as_i64().unwrap_or(0),
        "routerArn": as_string(params.get("routerArn")),
        "routerInputArn": "",
        "state": "ACTIVE",
    });
    let key = record["arn"].as_str().unwrap_or_default().to_string();
    st.router_outputs.push((key, record.clone()));
    ok_json(&json!({ "routerOutput": record }))
}

/// moto's update_router_* only writes the fields it knows, when provided.
fn update_router_resource(st: &mut State, kind: RouterKind, arn: &str, params: &Value) -> Response {
    let Some(record) = find_mut(router_store(st, kind), arn) else {
        return router_missing(kind);
    };
    for key in ["name", "dataType", "ip", "port", "networkInterfaceArn"] {
        if !params[key].is_null() {
            record[key] = params[key].clone();
        }
    }
    if kind == RouterKind::NetworkInterface {
        for key in ["networkName", "subnetMask", "defaultGateway", "ipAddress"] {
            if !params[key].is_null() {
                record[key] = params[key].clone();
            }
        }
    }
    ok_json(&json!({ router_wrap_key(kind): record.clone() }))
}

fn delete_router_resource(st: &mut State, kind: RouterKind, arn: &str) -> Response {
    match take(router_store(st, kind), arn) {
        Some(_) => ok_json(&json!({ "arn": arn })),
        None => router_missing(kind),
    }
}

/// List for the three plural POST routes; moto filters by router or gateway
/// arn and truncates by maxResults.
fn list_router_resource(
    st: &State,
    filter_arn: Option<Value>,
    max_results: Option<&str>,
    kind: RouterKind,
) -> Response {
    let limit: Option<usize> = max_results.and_then(|v| v.parse().ok());
    let filter = filter_arn.as_ref().and_then(Value::as_str);
    let records = match kind {
        RouterKind::Input => &st.router_inputs,
        RouterKind::NetworkInterface => &st.router_network_interfaces,
        RouterKind::Output => &st.router_outputs,
    };
    let mut rows: Vec<Value> = records
        .iter()
        .filter(|(_, record)| match filter {
            Some(filter) => {
                record["routerArn"].as_str() == Some(filter)
                    || record["gatewayArn"].as_str() == Some(filter)
            }
            None => true,
        })
        .map(|(_, record)| record.clone())
        .collect();
    if let Some(n) = limit {
        rows.truncate(n);
    }
    let key = match kind {
        RouterKind::Input => "routerInputs",
        RouterKind::NetworkInterface => "routerNetworkInterfaces",
        RouterKind::Output => "routerOutputs",
    };
    ok_json(&json!({ key: rows }))
}

fn batch_router(st: &State, arns: Option<&Vec<String>>, kind: RouterKind) -> Response {
    let records = router_store_ref(st, kind);
    let rows: Vec<Value> = arns
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|arn| find(records, arn).cloned())
        .collect();
    let key = match kind {
        RouterKind::Input => "routerInputs",
        RouterKind::NetworkInterface => "routerNetworkInterfaces",
        RouterKind::Output => "routerOutputs",
    };
    ok_json(&json!({ key: rows }))
}

fn router_store_ref(st: &State, kind: RouterKind) -> &Records {
    match kind {
        RouterKind::Input => &st.router_inputs,
        RouterKind::NetworkInterface => &st.router_network_interfaces,
        RouterKind::Output => &st.router_outputs,
    }
}

fn get_router_input_source_metadata(st: &mut State, arn: &str) -> Response {
    if find(&st.router_inputs, arn).is_none() {
        return router_missing(RouterKind::Input);
    }
    ok_json(&json!({
        "routerInputArn": arn,
        "messages": [],
        "timestamp": "2024-01-01T00:00:00Z",
        "transportMediaInfo": {},
    }))
}

fn get_router_input_thumbnail(st: &mut State, arn: &str) -> Response {
    if find(&st.router_inputs, arn).is_none() {
        return router_missing(RouterKind::Input);
    }
    ok_json(&json!({
        "thumbnail": {
            "routerInputArn": arn,
            "thumbnail": "",
            "timestamp": "2024-01-01T00:00:00Z",
        }
    }))
}

fn take_router_input(st: &mut State, router_output_arn: &str, params: &Value) -> Response {
    let input_arn = as_string(params.get("routerInputArn"));
    let Some(ro) = find_mut(&mut st.router_outputs, router_output_arn) else {
        return router_missing(RouterKind::Output);
    };
    ro["routerInputArn"] = json!(input_arn);
    ok_json(&json!({ "routerOutput": ro.clone() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: impl Into<String>, body: Value) -> Request {
        Request {
            service: "mediaconnect".into(),
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

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn scope() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn create_flow_body() -> Value {
        json!({
            "name": "flow-1",
            "source": {
                "name": "source-1",
                "protocol": "zixi-push",
                "whitelistCidr": "0.0.0.0/0",
            },
        })
    }

    #[test]
    fn flow_lifecycle() {
        let acct = scope();
        let mut r = req("POST", "/v1/flows", create_flow_body());
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        let flow = body(&resp)["flow"].clone();
        assert_eq!(flow["name"], "flow-1");
        assert_eq!(flow["status"], "STANDBY");
        let arn = flow["flowArn"].as_str().unwrap().to_string();
        assert!(arn.contains(":flow:"));

        // List shows it with sourceType OWNED.
        r = req("GET", "/v1/flows", json!({}));
        r.account = acct;
        let listed = body(&dispatch(&r).unwrap())["flows"][0].clone();
        assert_eq!(listed["sourceType"], "OWNED");

        // Describe with an encoded ARN segment.
        r = req(
            "GET",
            "/v1/flows/arn%3Aaws%3Amediaconnect%3Aus-east-1%3A123456789012",
            json!({}),
        );
        // That arn does not match; expect 400 not_found.
        r.path = format!("/v1/flows/{}", urlencoded(&arn));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["flow"]["name"].as_str(),
            Some("flow-1")
        );

        // Start → STARTING
        r = req(
            "POST",
            format!("/v1/flows/start/{}", urlencoded(&arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["status"], "STARTING");

        // Stop → STOPPING
        r = req(
            "POST",
            format!("/v1/flows/stop/{}", urlencoded(&arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["status"], "STOPPING");

        // Delete
        r = req(
            "DELETE",
            format!("/v1/flows/{}", urlencoded(&arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        r = req("GET", format!("/v1/flows/{}", urlencoded(&arn)), json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 400);
    }

    fn urlencoded(arn: &str) -> String {
        arn.replace(':', "%3A")
    }

    #[test]
    fn outputs_add_and_remove() {
        let acct = scope();
        let mut r = req("POST", "/v1/flows", create_flow_body());
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["flow"]["flowArn"]
            .as_str()
            .unwrap()
            .to_string();
        let enc = urlencoded(&arn);

        r = req(
            "POST",
            format!("/v1/flows/{enc}/outputs"),
            json!({"outputs": [{"protocol": "zixi-push", "name": "out1"}]}),
        );
        r.account = acct;
        let added = body(&dispatch(&r).unwrap());
        assert_eq!(added["outputs"][0]["name"], "out1");

        r = req("DELETE", format!("/v1/flows/{enc}/outputs/out1"), json!({}));
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("GET", format!("/v1/flows/{enc}"), json!({}));
        r.account = acct;
        let flow = body(&dispatch(&r).unwrap())["flow"].clone();
        assert_eq!(flow["outputs"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn tags_roundtrip() {
        let acct = scope();
        let mut r = req("POST", "/v1/flows", create_flow_body());
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["flow"]["flowArn"]
            .as_str()
            .unwrap()
            .to_string();
        let enc = urlencoded(&arn);

        r = req(
            "POST",
            format!("/tags/{enc}"),
            json!({"tags": {"env": "test"}}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("GET", format!("/tags/{enc}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["tags"]["env"], "test");

        r = req("DELETE", format!("/tags/{enc}"), json!({}));
        r.query_string = "tagKeys=env".into();
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("GET", format!("/tags/{enc}"), json!({}));
        r.account = acct;
        // moto's tagger keeps the (now empty) entry: an empty Tags map, 200.
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        assert!(body(&resp)["tags"]
            .as_object()
            .is_some_and(|t| t.is_empty()));
    }

    #[test]
    fn bridge_lifecycle() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/v1/bridges",
            json!({
                "name": "bridge-1",
                "placementArn": "arn:aws:mediaconnect:us-east-1:123456789012:bridge:test",
                "sources": [{"networkSource": {"name": "src1", "protocol": "zixi-push"}}],
            }),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        let bridge = body(&resp)["bridge"].clone();
        assert_eq!(bridge["bridgeState"], "ACTIVE");
        assert_eq!(bridge["sources"][0]["networkSource"]["name"], "src1");
        let arn = bridge["bridgeArn"].as_str().unwrap().to_string();
        let enc = urlencoded(&arn);

        r = req("GET", format!("/v1/bridges/{enc}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["bridge"]["name"], "bridge-1");

        // Unbindable segments leave dispatch → gateway 501.
        r.path = "/v1/bridges/arn%3Awith.dot".into();
        r.account = acct;
        assert!(dispatch(&r).is_none());
    }

    #[test]
    fn offerings_and_reservations() {
        let acct = scope();
        let mut r = req("GET", "/v1/offerings", json!({}));
        r.account = acct;
        let offerings = body(&dispatch(&r).unwrap())["offerings"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(offerings.len(), 3);
        assert!(offerings[0]["offeringArn"].as_str().is_some());
        assert_eq!(offerings[0]["currencyCode"], "USD");

        let arn = offerings[0]["offeringArn"].as_str().unwrap().to_string();
        let enc = urlencoded(&arn);
        r = req("GET", format!("/v1/offerings/{enc}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["offering"]["duration"], 12);

        r = req(
            "POST",
            format!("/v1/offerings/{enc}"),
            json!({"reservationName": "res-1", "start": "2026-01-01T00:00:00Z"}),
        );
        r.account = acct;
        let reservation = body(&dispatch(&r).unwrap())["reservation"].clone();
        assert_eq!(reservation["reservationName"], "res-1");
        assert_eq!(reservation["reservationState"], "ACTIVE");

        r = req("GET", "/v1/reservations", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["reservations"][0]["reservationName"],
            "res-1"
        );
    }

    #[test]
    fn router_input_crud() {
        let acct = scope();
        let mut r = req(
            "POST",
            "/v1/routerNetworkInterface",
            json!({"name": "rni-1"}),
        );
        r.account = acct;
        let rni_arn = body(&dispatch(&r).unwrap())["routerNetworkInterface"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "POST",
            "/v1/routerInput",
            json!({"name": "ri-1", "maximumBitrate": 1000000, "routingScope": "SESSION", "tier": "STANDARD"}),
        );
        r.account = acct;
        let ri = body(&dispatch(&r).unwrap())["routerInput"].clone();
        assert_eq!(ri["name"], "ri-1");
        assert_eq!(ri["state"], "ACTIVE");
        let ri_arn = ri["arn"].as_str().unwrap().to_string();

        r = req(
            "GET",
            format!("/v1/routerInput/{}", urlencoded(&ri_arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["routerInput"]["name"], "ri-1");

        r = req(
            "POST",
            format!("/v1/routerInput/start/{}", urlencoded(&ri_arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["state"], "ACTIVE");

        r = req("POST", "/v1/routerInputs", json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["routerInputs"][0]["name"],
            "ri-1"
        );

        r = req(
            "DELETE",
            format!("/v1/routerInput/{}", urlencoded(&ri_arn)),
            json!({}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);
        let _ = rni_arn;
    }

    #[test]
    fn take_router_input_sets_input_arn() {
        let acct = scope();
        let mut r = req("POST", "/v1/routerOutput", json!({"name": "ro-1"}));
        r.account = acct;
        let ro_arn = body(&dispatch(&r).unwrap())["routerOutput"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        r = req(
            "PUT",
            format!("/v1/routerOutput/takeRouterInput/{}", urlencoded(&ro_arn)),
            json!({"routerInputArn": "arn:aws:mediaconnect:us-east-1:123456789012:router-input:x"}),
        );
        r.account = acct;
        let out = body(&dispatch(&r).unwrap())["routerOutput"].clone();
        assert!(out["routerInputArn"].as_str().unwrap().ends_with(":x"));
    }

    // moto's `[^/.]+` binds against the percent-encoded URL, so ARN paths
    // containing %2F (like the prober's auto-filled `thing/probe-test`) still
    // reach the handlers, which answer moto's not-found errors — never 501.
    #[test]
    fn percent_encoded_slash_in_arn_still_binds() {
        let acct = scope();
        let filler = "arn%3Aaws%3Amediaconnect%3Aus-east-1%3A123456789012%3Athing%2Fprobe-test";
        // GET describe: moto's NotFoundException "Flow not found.".
        let mut r = req("GET", format!("/v1/flows/{filler}"), json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["__type"], "NotFoundException");

        // POST on a sub-resource: moto's "flow with arn=… not found".
        r = req(
            "POST",
            format!("/v1/flows/{filler}/outputs"),
            json!({"outputs": [{"name": "out", "protocol": "zixi-push"}]}),
        );
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert!(body(&resp)["message"]
            .as_str()
            .unwrap()
            .starts_with("flow with arn=arn:aws:mediaconnect:us-east-1:123456789012"));

        // Bridges, gateways and gateway instances bind the same way with
        // their own "not found." messages.
        r = req("GET", format!("/v1/bridges/{filler}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["message"], "Bridge not found.");
        r = req("GET", format!("/v1/gateways/{filler}"), json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["message"],
            "Gateway not found."
        );
        r = req("GET", format!("/v1/gateway-instances/{filler}"), json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["message"],
            "Gateway instance not found."
        );

        // Router resources and tags round-trip through the same bind.
        r = req(
            "PUT",
            format!("/v1/routerOutput/takeRouterInput/{filler}"),
            json!({}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["message"],
            "Router output not found."
        );
        r = req("GET", format!("/tags/{filler}"), json!({}));
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["message"],
            "Resource not found."
        );
        r = req(
            "PUT",
            format!("/v1/routerNetworkInterface/{filler}"),
            json!({}),
        );
        r.account = acct;
        assert_eq!(
            body(&dispatch(&r).unwrap())["message"],
            "Router network interface not found."
        );
    }

    // %2E is an escape moto's regex never sees — the encoded segment binds,
    // decodes to a dot, and simply does not match any record.
    #[test]
    fn percent_encoded_dot_binds() {
        let acct = scope();
        let mut r = req("GET", "/v1/flows/arn%3Awith%2Edot", json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body(&resp)["message"], "Flow not found.");
    }

    // source-metadata and source-thumbnail only check existence (moto
    // resolves transient states exclusively in describe_flow), so a started
    // flow's stored status stays STARTING — observable through delete_flow.
    #[test]
    fn source_metadata_does_not_resolve_transient_status() {
        let acct = scope();
        let mut r = req("POST", "/v1/flows", create_flow_body());
        r.account = acct;
        let arn = body(&dispatch(&r).unwrap())["flow"]["flowArn"]
            .as_str()
            .unwrap()
            .to_string();
        let enc = urlencoded(&arn);

        r = req("POST", format!("/v1/flows/start/{enc}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["status"], "STARTING");

        r = req("GET", format!("/v1/flows/{enc}/source-metadata"), json!({}));
        r.account = acct;
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(body(&resp)["flowArn"], arn);
        assert_eq!(body(&resp)["messages"], json!([]));

        // A thumbnail-only look must not flip the status either.
        r = req(
            "GET",
            format!("/v1/flows/{enc}/source-thumbnail"),
            json!({}),
        );
        r.account = acct;
        assert_eq!(dispatch(&r).unwrap().status, 200);

        r = req("DELETE", format!("/v1/flows/{enc}"), json!({}));
        r.account = acct;
        assert_eq!(body(&dispatch(&r).unwrap())["status"], "STARTING");
    }
}
