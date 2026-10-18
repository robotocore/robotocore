//! AppMesh backend (REST-JSON, `/v20190125/...` paths).
//!
//! Replaces the moto sidecar for `appmesh` following moto's `AppMeshBackend`
//! per-(account, region) model: meshes hold virtual nodes/routers/services/
//! gateways; routers hold routes, gateways hold gateway routes. Tags are
//! `(key, value)` lists per resource ARN with `extend` + key-filtered removal.
//!
//! Divergence: moto re-parses each `spec` into dataclasses and re-serializes
//! (dropping default/absent branches); rotocore stores the caller's spec
//! verbatim and echoes it back — wire shapes for the supported flows match
//! on the keys the suites and models observe.

use crate::store;
use crate::{Request, Response};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_resp(status: u16, body: &Value) -> Response {
    Response::new(
        status,
        "application/json",
        serde_json::to_vec(body).unwrap_or_default(),
    )
}

fn ok_json(body: &Value) -> Response {
    json_resp(200, body)
}

/// moto appmesh errors are JSON-Rest errors: `{"__type": code, "message"}`.
fn mesh_error(code: &str, message: &str) -> Response {
    json_resp(400, &json!({ "__type": code, "message": message }))
}

/// Multi-value query params (`tagKeys` can repeat).
fn query_multi(query_string: &str, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for pair in query_string.split('&') {
        let (k, v) = match pair.find('=') {
            Some(i) => (&pair[..i], &pair[i + 1..]),
            None => (pair, ""),
        };
        if k == key && !v.is_empty() {
            out.push(percent_decode(v));
        }
    }
    out
}

fn query_single(query_string: &str, key: &str) -> Option<String> {
    query_multi(query_string, key).into_iter().next()
}

/// Percent-decode a path or query component.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Common metadata for every mesh resource.
#[derive(Default, Clone)]
struct Metadata {
    arn: String,
    created_at: i64,
    last_updated_at: i64,
    uid: String,
    version: i64,
}

impl Metadata {
    fn new(arn: String) -> Self {
        let now = Utc::now().timestamp();
        Metadata {
            arn,
            created_at: now,
            last_updated_at: now,
            uid: uuid::Uuid::new_v4().simple().to_string(),
            version: 1,
        }
    }

    fn update(&mut self) {
        self.last_updated_at = Utc::now().timestamp();
        self.version += 1;
    }
}

#[derive(Default, Clone)]
struct Resource {
    name: String,
    meta: Metadata,
    /// raw spec (echoed back verbatim)
    #[allow(dead_code)]
    parent: String,
    spec: Value,
}

#[derive(Default)]
struct Mesh {
    name: String,
    meta: Metadata,
    spec: Value,
    #[allow(dead_code)]
    tags: Vec<(String, String)>,
    nodes: BTreeMap<String, Resource>,
    routers: BTreeMap<String, Resource>,
    routes_of_router: BTreeMap<String, BTreeMap<String, Resource>>,
    services: BTreeMap<String, Resource>,
    gateways: BTreeMap<String, Resource>,
    gateway_routes: BTreeMap<String, BTreeMap<String, Resource>>,
}

#[derive(Default)]
pub struct AppMeshState {
    meshes: BTreeMap<String, Mesh>,
    /// arn → tags
    tags: BTreeMap<String, Vec<(String, String)>>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut AppMeshState) -> T) -> T {
    let slot = store::slot::<AppMeshState>("appmesh");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

fn now_epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

/// moto's "YYYY-MM-DD HH:MM:SS" (utc) timestamp rendering.
fn fmt_ts(epoch: i64) -> String {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_opt(epoch, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

const MESH_OWNER: &str = "AKIAIOSFODNN7EXAMPLE";

// ---------------------------------------------------------------------------
// Shape helpers
// ---------------------------------------------------------------------------

fn metadata_json(meta: &Metadata) -> Value {
    json!({
        "arn": meta.arn,
        "createdAt": fmt_ts(meta.created_at),
        "lastUpdatedAt": fmt_ts(meta.last_updated_at),
        "meshOwner": MESH_OWNER,
        "resourceOwner": MESH_OWNER,
        "uid": meta.uid,
        "version": meta.version,
    })
}

fn list_summary(name_key: &str, name: &str, mesh_name: &str, meta: &Metadata) -> Value {
    json!({
        "arn": meta.arn,
        "createdAt": fmt_ts(meta.created_at),
        "lastUpdatedAt": fmt_ts(meta.last_updated_at),
        "meshName": mesh_name,
        "meshOwner": MESH_OWNER,
        "resourceOwner": MESH_OWNER,
        "version": meta.version,
        name_key: name,
    })
}

fn tags_json(tags: &[(String, String)]) -> Value {
    Value::Array(
        tags.iter()
            .map(|(k, v)| json!({ "key": k, "value": v }))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Route the request into a resource_op. `paths` example:
/// /v20190125/meshes/{mesh}/virtualNodes/{name}
pub fn dispatch(req: &Request) -> Response {
    let path = req.path.trim_matches('/');
    let segments: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    if segments.first() != Some(&"v20190125") {
        return mesh_error("NotImplemented", &format!("{} {}", req.method, req.path));
    }
    let rest = &segments[1..];
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
    };
    with_state(req.account, &req.region, |state| {
        route_resource(
            state,
            &req.method,
            rest,
            &body,
            req.account,
            &req.region,
            &req.query_string,
        )
    })
}

/// `segments` are post-`v20190125` path split.
fn route_resource(
    state: &mut AppMeshState,
    method: &str,
    segments: &[&str],
    body: &Value,
    account: u64,
    region: &str,
    query: &str,
) -> Response {
    match segments {
        ["meshes"] => match method {
            "PUT" => {
                let name = body
                    .get("meshName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if state.meshes.contains_key(name) {
                    return mesh_error(
                        "MeshNameAlreadyTakenError",
                        &format!("There is already a mesh with the name {name}."),
                    );
                }
                let arn = mesh_arn(account, region, name);
                let mesh = Mesh {
                    name: name.to_string(),
                    meta: Metadata::new(arn.clone()),
                    spec: json!({
                        "egressFilter": { "type": body.pointer("/spec/egressFilter/type").and_then(Value::as_str) },
                        "serviceDiscovery": {
                            "ipPreference": body.pointer("/spec/serviceDiscovery/ipPreference").and_then(Value::as_str)
                        },
                    }),
                    ..Mesh::default()
                };
                // tags accepted as {"tags": [{"key","value"}...]}
                let mut tags = Vec::new();
                for t in body
                    .get("tags")
                    .and_then(Value::as_array)
                    .unwrap_or(&vec![])
                {
                    tags.push((
                        t.get("key")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        t.get("value")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    ));
                }
                state.tags.insert(arn, tags);
                state.meshes.insert(name.to_string(), mesh);
                ok_json(&mesh_to_json(state.meshes.get(name).expect("fresh")))
            }
            "GET" => {
                let meshes: Vec<Value> = state
                    .meshes
                    .values()
                    .map(|m| {
                        json!({
                            "arn": m.meta.arn,
                            "createdAt": fmt_ts(m.meta.created_at),
                            "lastUpdatedAt": fmt_ts(m.meta.last_updated_at),
                            "meshName": m.name,
                            "meshOwner": MESH_OWNER,
                            "resourceOwner": MESH_OWNER,
                            "version": m.meta.version,
                        })
                    })
                    .collect();
                ok_json(&json!({ "nextToken": Value::Null, "meshes": meshes }))
            }
            _ => mesh_error("NotImplemented", &format!("{method} on /meshes")),
        },
        ["meshes", mesh_name] => {
            let mesh_exists = state.meshes.contains_key(*mesh_name);
            let Some(_) = mesh_exists.then_some(()) else {
                return mesh_error(
                    "MeshNotFound",
                    &format!("There are no meshes with the name {mesh_name}."),
                );
            };
            match method {
                "GET" | "PUT" => {
                    if method == "PUT" {
                        // update spec (mutation happens on the stored mesh)
                        let stored = state.meshes.get_mut(*mesh_name).expect("present");
                        if let Some(egress) = body
                            .pointer("/spec/egressFilter/type")
                            .and_then(Value::as_str)
                        {
                            stored.spec["egressFilter"]["type"] = json!(egress);
                        }
                        if let Some(pref) = body
                            .pointer("/spec/serviceDiscovery/ipPreference")
                            .and_then(Value::as_str)
                        {
                            stored.spec["serviceDiscovery"]["ipPreference"] = json!(pref);
                        }
                        stored.meta.update();
                    }
                    let stored = state.meshes.get(*mesh_name).expect("present");
                    ok_json(&mesh_to_json(stored))
                }
                "DELETE" => {
                    let stored = state.meshes.get(*mesh_name).expect("present");
                    let mut deleting = mesh_to_json(stored);
                    if let Some(obj) = deleting.get_mut("status") {
                        *obj = json!({ "status": "DELETED" });
                    }
                    state.meshes.remove(*mesh_name);
                    state.tags.remove(&mesh_arn(account, region, mesh_name));
                    ok_json(&deleting)
                }
                _ => mesh_error("NotImplemented", &format!("{method} on mesh")),
            }
        }
        ["meshes", mesh_name, "virtualNodes"] => match method {
            "PUT" => op_create_node(state, mesh_name, body, account, region),
            "GET" => op_list_nodes(state, mesh_name),
            _ => mesh_error("NotImplemented", &format!("{method} on virtualNodes")),
        },
        ["meshes", mesh_name, "virtualNodes", node_name] => {
            op_node_item(state, method, mesh_name, node_name, body, account, region)
        }
        ["meshes", mesh_name, "virtualRouters"] => match method {
            "PUT" => op_create_router(state, mesh_name, body, account, region),
            "GET" => op_list_routers(state, mesh_name),
            _ => mesh_error("NotImplemented", &format!("{method} on virtualRouters")),
        },
        ["meshes", mesh_name, "virtualRouters", router_name] => {
            op_router_item(state, method, mesh_name, router_name, body, account, region)
        }
        ["meshes", mesh_name, "virtualRouter", router_name, "routes"] => match method {
            "PUT" => op_create_route(state, mesh_name, router_name, body, account, region),
            "GET" => op_list_routes(state, mesh_name, router_name),
            _ => mesh_error("NotImplemented", &format!("{method} on routes")),
        },
        ["meshes", mesh_name, "virtualRouter", router_name, "routes", route_name] => op_route_item(
            state,
            method,
            mesh_name,
            router_name,
            route_name,
            body,
            account,
            region,
        ),
        ["meshes", mesh_name, "virtualServices"] => match method {
            "PUT" => op_create_service(state, mesh_name, body, account, region),
            "GET" => op_list_services(state, mesh_name),
            _ => mesh_error("NotImplemented", &format!("{method} on virtualServices")),
        },
        ["meshes", mesh_name, "virtualServices", svc_name] => {
            op_service_item(state, method, mesh_name, svc_name, body, account, region)
        }
        ["meshes", mesh_name, "virtualGateways"] => match method {
            "PUT" => op_create_gateway(state, mesh_name, body, account, region),
            "GET" => op_list_gateways(state, mesh_name),
            _ => mesh_error("NotImplemented", &format!("{method} on virtualGateways")),
        },
        ["meshes", mesh_name, "virtualGateways", gw_name] => {
            op_gateway_item(state, method, mesh_name, gw_name, body, account, region)
        }
        ["meshes", mesh_name, "virtualGateway", gw_name, "gatewayRoutes"] => match method {
            "PUT" => op_create_gateway_route(state, mesh_name, gw_name, body, account, region),
            "GET" => op_list_gateway_routes(state, mesh_name, gw_name),
            _ => mesh_error("NotImplemented", &format!("{method} on gatewayRoutes")),
        },
        ["meshes", mesh_name, "virtualGateway", gw_name, "gatewayRoutes", gr_name] => {
            op_gateway_route_item(
                state, method, mesh_name, gw_name, gr_name, body, account, region,
            )
        }
        ["tags"] if method == "GET" => op_tags(state, "GET", body, query),
        ["tag"] if method == "PUT" => op_tags(state, "POST", body, query),
        ["untag"] if method == "PUT" => op_tags(state, "DELETE", body, query),
        _ => mesh_error("NotImplemented", &format!("path {segments:?}")),
    }
}

// ---------------------------------------------------------------------------
// Op implementations
// ---------------------------------------------------------------------------

fn mesh_arn(account: u64, region: &str, mesh_name: &str) -> String {
    format!("arn:aws:appmesh:{region}:{account}:{mesh_name}")
}

fn nested_arn(account: u64, region: &str, mesh: &str, kind: &str, name: &str) -> String {
    format!("arn:aws:appmesh:{region}:{account}:mesh/{mesh}/{kind}/{name}")
}

/// The Mesh object view (moto Mesh.to_dict).
fn mesh_to_json(mesh: &Mesh) -> Value {
    json!({
        "meshName": mesh.name,
        "metadata": metadata_json(&mesh.meta),
        "spec": mesh.spec,
        "status": { "status": "ACTIVE" },
    })
}

fn require_mesh<'a>(state: &'a AppMeshState, name: &str) -> Result<&'a Mesh, Response> {
    state.meshes.get(name).ok_or_else(|| {
        mesh_error(
            "MeshNotFound",
            &format!("There are no meshes with the name {name}."),
        )
    })
}

fn resource_to_json(
    kind_key: &str,
    name: &str,
    mesh_name: &str,
    parent: &str,
    res: &Resource,
) -> Value {
    let mut base = json!({
        "meshName": mesh_name,
        "metadata": metadata_json(&res.meta),
        "spec": res.spec,
        "status": { "status": "ACTIVE" },
    });
    let obj = base.as_object_mut().expect("resource obj");
    obj.insert(kind_key.to_string(), json!(name));
    if !parent.is_empty() {
        // routes live under virtualRouter*, gateway routes under virtualGateway*
        if kind_key == "gatewayRouteName" {
            obj.insert("virtualGatewayName".to_string(), json!(parent));
        } else {
            obj.insert("virtualRouterName".to_string(), json!(parent));
        }
    }
    base
}

fn tags_mut<'a>(state: &'a mut AppMeshState, arn: &str) -> &'a mut Vec<(String, String)> {
    state.tags.entry(arn.to_string()).or_default()
}

fn store_tags(state: &mut AppMeshState, arn: &str, body: &Value) {
    // Register the resource ARN so later Tag operations resolve it.
    tags_mut(state, arn);
    if let Some(list) = body.get("tags").and_then(Value::as_array) {
        for t in list {
            tags_mut(state, arn).push((
                t.get("key")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                t.get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            ));
        }
    }
}

// --- virtual nodes ---------------------------------------------------------

fn op_create_node(
    state: &mut AppMeshState,
    mesh_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let name = body
        .get("virtualNodeName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = nested_arn(account, region, mesh_name, "virtualNode", &name);
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: String::new(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("checked");
    mesh.nodes.insert(name.clone(), res);
    let node = mesh.nodes.get(&name).expect("inserted");
    ok_json(&resource_to_json(
        "virtualNodeName",
        &name,
        mesh_name,
        "",
        node,
    ))
}

fn op_list_nodes(state: &AppMeshState, mesh_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let nodes: Vec<Value> = state
        .meshes
        .get(mesh_name)
        .expect("checked")
        .nodes
        .values()
        .map(|n| list_summary("virtualNodeName", &n.name, mesh_name, &n.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "virtualNodes": nodes }))
}

fn op_node_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    node_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    match method {
        "GET" | "DELETE" | "PUT" => {
            let exists = state
                .meshes
                .get(mesh_name)
                .map(|m| m.nodes.contains_key(node_name))
                .unwrap_or(false);
            if !exists {
                return mesh_error(
                    "VirtualNodeNotFound",
                    &format!("There are no mesh resources with the name {node_name} in mesh {mesh_name}."),
                );
            }
            if method == "PUT" {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                if let Some(spec) = body.get("spec") {
                    mesh.nodes.get_mut(node_name).expect("checked").spec = spec.clone();
                    mesh.nodes
                        .get_mut(node_name)
                        .expect("checked")
                        .meta
                        .update();
                }
            }
            if method == "DELETE" {
                let arn = nested_arn(account, region, mesh_name, "virtualNode", node_name);
                let mut deleted = {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    let node = mesh.nodes.remove(node_name).expect("checked");
                    resource_to_json("virtualNodeName", node_name, mesh_name, "", &node)
                };
                if let Some(obj) = deleted.get_mut("status") {
                    *obj = json!({ "status": "DELETED" });
                }
                state.tags.remove(&arn);
                return ok_json(&deleted);
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let node = mesh.nodes.get(node_name).expect("checked");
            ok_json(&resource_to_json(
                "virtualNodeName",
                node_name,
                mesh_name,
                "",
                node,
            ))
        }
        _ => mesh_error("NotImplemented", &format!("{method} on virtualNode")),
    }
}

// --- virtual routers -------------------------------------------------------

fn op_create_router(
    state: &mut AppMeshState,
    mesh_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let name = body
        .get("virtualRouterName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = nested_arn(account, region, mesh_name, "virtualRouter", &name);
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: String::new(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("checked");
    mesh.routers.insert(name.clone(), res.clone());
    mesh.routes_of_router.entry(name.clone()).or_default();
    ok_json(&resource_to_json(
        "virtualRouterName",
        &name,
        mesh_name,
        "",
        &res,
    ))
}

fn op_list_routers(state: &AppMeshState, mesh_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let routers: Vec<Value> = state
        .meshes
        .get(mesh_name)
        .expect("checked")
        .routers
        .values()
        .map(|r| list_summary("virtualRouterName", &r.name, mesh_name, &r.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "virtualRouters": routers }))
}

fn op_router_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    router_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    match method {
        "GET" | "PUT" | "DELETE" => {
            let exists = state
                .meshes
                .get(mesh_name)
                .map(|m| m.routers.contains_key(router_name))
                .unwrap_or(false);
            if !exists {
                return mesh_error(
                    "VirtualRouterNotFound",
                    &format!("The mesh {mesh_name} does not have a virtual router with the name {router_name}."),
                );
            }
            if method == "PUT" {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                if let Some(spec) = body.get("spec") {
                    mesh.routers.get_mut(router_name).expect("checked").spec = spec.clone();
                    mesh.routers
                        .get_mut(router_name)
                        .expect("checked")
                        .meta
                        .update();
                }
            }
            if method == "DELETE" {
                let arn = nested_arn(account, region, mesh_name, "virtualRouter", router_name);
                let mut deleted = {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    let router = mesh.routers.remove(router_name).expect("checked");
                    mesh.routes_of_router.remove(router_name);
                    router
                };
                deleted.meta.last_updated_at = now_epoch();
                let mut view =
                    resource_to_json("virtualRouterName", router_name, mesh_name, "", &deleted);
                if let Some(obj) = view.get_mut("status") {
                    *obj = json!({ "status": "DELETED" });
                }
                state.tags.remove(&arn);
                return ok_json(&view);
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let router = &mesh.routers[router_name];
            ok_json(&resource_to_json(
                "virtualRouterName",
                router_name,
                mesh_name,
                "",
                router,
            ))
        }
        _ => mesh_error("NotImplemented", &format!("{method} on virtualRouter")),
    }
}

// --- routes (inside routers) ----------------------------------------------

fn op_create_route(
    state: &mut AppMeshState,
    mesh_name: &str,
    router_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(mesh_routes) = state.meshes.get(mesh_name) else {
        return mesh_error(
            "MeshNotFound",
            &format!("There are no meshes with the name {mesh_name}."),
        );
    };
    if !mesh_routes.routers.contains_key(router_name) {
        return mesh_error(
            "VirtualRouterNotFound",
            &format!(
                "The mesh {mesh_name} does not have a virtual router with the name {router_name}."
            ),
        );
    }
    let name = body
        .get("routeName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = format!(
        "arn:aws:appmesh:{region}:{account}:mesh/{mesh_name}/virtualRouter/{router_name}/route/{name}"
    );
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: router_name.to_string(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("present");
    mesh.routes_of_router
        .entry(router_name.to_string())
        .or_default()
        .insert(name.clone(), res);
    let route = mesh
        .routes_of_router
        .get(router_name)
        .and_then(|r| r.get(&name))
        .expect("inserted");
    ok_json(&resource_to_json(
        "routeName",
        &name,
        mesh_name,
        router_name,
        route,
    ))
}

fn op_list_routes(state: &AppMeshState, mesh_name: &str, router_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let mesh = state.meshes.get(mesh_name).expect("checked");
    let Some(routes) = mesh.routes_of_router.get(router_name) else {
        return mesh_error(
            "VirtualRouterNotFound",
            &format!(
                "The mesh {mesh_name} does not have a virtual router with the name {router_name}."
            ),
        );
    };
    let list: Vec<Value> = routes
        .values()
        .map(|r| list_summary("routeName", &r.name, mesh_name, &r.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "routes": list }))
}

#[allow(clippy::too_many_arguments)]
fn op_route_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    router_name: &str,
    route_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let readable = state
        .meshes
        .get(mesh_name)
        .and_then(|m| m.routes_of_router.get(router_name))
        .and_then(|r| r.get(route_name))
        .is_some();
    if !readable {
        return mesh_error(
            "RouteNotFound",
            &format!("The mesh {mesh_name} does not have a virtual router {router_name} with a route named {route_name}."),
        );
    }
    match method {
        "GET" => {
            let mesh = state.meshes.get(mesh_name).expect("present");
            let route = &mesh.routes_of_router[router_name][route_name];
            ok_json(&resource_to_json(
                "routeName",
                route_name,
                mesh_name,
                router_name,
                route,
            ))
        }
        "PUT" => {
            if let Some(spec) = body.get("spec") {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                let route = mesh
                    .routes_of_router
                    .get_mut(router_name)
                    .and_then(|r| r.get_mut(route_name))
                    .expect("checked");
                route.spec = spec.clone();
                route.meta.update();
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let route = &mesh.routes_of_router[router_name][route_name];
            ok_json(&resource_to_json(
                "routeName",
                route_name,
                mesh_name,
                router_name,
                route,
            ))
        }
        "DELETE" => {
            let arn = format!(
                "arn:aws:appmesh:{region}:{account}:mesh/{mesh_name}/virtualRouter/{router_name}/route/{route_name}"
            );
            let mut deleted = {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                mesh.routes_of_router
                    .get_mut(router_name)
                    .expect("checked")
                    .remove(route_name)
                    .expect("checked")
            };
            deleted.meta.last_updated_at = now_epoch();
            let mut view =
                resource_to_json("routeName", route_name, mesh_name, router_name, &deleted);
            if let Some(obj) = view.get_mut("status") {
                *obj = json!({ "status": "DELETED" });
            }
            state.tags.remove(&arn);
            ok_json(&view)
        }
        _ => mesh_error("NotImplemented", &format!("{method} on route")),
    }
}

// --- virtual services ------------------------------------------------------

fn op_create_service(
    state: &mut AppMeshState,
    mesh_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let name = body
        .get("virtualServiceName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = nested_arn(account, region, mesh_name, "virtualService", &name);
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: String::new(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("present");
    mesh.services.insert(name.clone(), res);
    let svc = mesh.services.get(&name).expect("inserted");
    ok_json(&resource_to_json(
        "virtualServiceName",
        &name,
        mesh_name,
        "",
        svc,
    ))
}

fn op_list_services(state: &AppMeshState, mesh_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let list: Vec<Value> = state
        .meshes
        .get(mesh_name)
        .expect("checked")
        .services
        .values()
        .map(|s| list_summary("virtualServiceName", &s.name, mesh_name, &s.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "virtualServices": list }))
}

fn op_service_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    svc_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    match method {
        "GET" | "PUT" | "DELETE" => {
            let exists = state
                .meshes
                .get(mesh_name)
                .map(|m| m.services.contains_key(svc_name))
                .unwrap_or(false);
            if !exists {
                return mesh_error(
                    "VirtualServiceNotFound",
                    &format!(
                        "There are no mesh resources with the name {svc_name} in mesh {mesh_name}."
                    ),
                );
            }
            if method == "PUT" {
                if let Some(spec) = body.get("spec") {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    let svc = mesh.services.get_mut(svc_name).expect("checked");
                    svc.spec = spec.clone();
                    svc.meta.update();
                }
            }
            if method == "DELETE" {
                let arn = nested_arn(account, region, mesh_name, "virtualService", svc_name);
                let mut deleted = {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    mesh.services.remove(svc_name).expect("checked")
                };
                let _ = &mut deleted;
                let mut view =
                    resource_to_json("virtualServiceName", svc_name, mesh_name, "", &deleted);
                if let Some(obj) = view.get_mut("status") {
                    *obj = json!({ "status": "DELETED" });
                }
                state.tags.remove(&arn);
                return ok_json(&view);
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let svc = &mesh.services[svc_name];
            ok_json(&resource_to_json(
                "virtualServiceName",
                svc_name,
                mesh_name,
                "",
                svc,
            ))
        }
        _ => mesh_error("NotImplemented", &format!("{method} on virtualService")),
    }
}

// --- virtual gateways ------------------------------------------------------

fn op_create_gateway(
    state: &mut AppMeshState,
    mesh_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let name = body
        .get("virtualGatewayName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = nested_arn(account, region, mesh_name, "virtualGateway", &name);
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: String::new(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("present");
    mesh.gateways.insert(name.clone(), res);
    mesh.gateway_routes.entry(name.clone()).or_default();
    let gw = mesh.gateways.get(&name).expect("inserted");
    ok_json(&resource_to_json(
        "virtualGatewayName",
        &name,
        mesh_name,
        "",
        gw,
    ))
}

fn op_list_gateways(state: &AppMeshState, mesh_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let list: Vec<Value> = state
        .meshes
        .get(mesh_name)
        .expect("checked")
        .gateways
        .values()
        .map(|g| list_summary("virtualGatewayName", &g.name, mesh_name, &g.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "virtualGateways": list }))
}

fn op_gateway_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    gw_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    match method {
        "GET" | "PUT" | "DELETE" => {
            let exists = state
                .meshes
                .get(mesh_name)
                .map(|m| m.gateways.contains_key(gw_name))
                .unwrap_or(false);
            if !exists {
                return mesh_error(
                    "VirtualGatewayNotFound",
                    &format!("There is no virtual gateway with the name {gw_name} inside the mesh {mesh_name}."),
                );
            }
            if method == "PUT" {
                if let Some(spec) = body.get("spec") {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    let gw = mesh.gateways.get_mut(gw_name).expect("checked");
                    gw.spec = spec.clone();
                    gw.meta.update();
                }
            }
            if method == "DELETE" {
                let arn = nested_arn(account, region, mesh_name, "virtualGateway", gw_name);
                let mut deleted = {
                    let mesh = state.meshes.get_mut(mesh_name).expect("present");
                    let del = mesh.gateways.remove(gw_name).expect("checked");
                    mesh.gateway_routes.remove(gw_name);
                    del
                };
                deleted.meta.last_updated_at = now_epoch();
                let mut view =
                    resource_to_json("virtualGatewayName", gw_name, mesh_name, "", &deleted);
                if let Some(obj) = view.get_mut("status") {
                    *obj = json!({ "status": "DELETED" });
                }
                state.tags.remove(&arn);
                return ok_json(&view);
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let gw = &mesh.gateways[gw_name];
            ok_json(&resource_to_json(
                "virtualGatewayName",
                gw_name,
                mesh_name,
                "",
                gw,
            ))
        }
        _ => mesh_error("NotImplemented", &format!("{method} on virtualGateway")),
    }
}

// --- gateway routes --------------------------------------------------------

fn gateway_route_arn(account: u64, region: &str, mesh: &str, gw: &str, name: &str) -> String {
    format!(
        "arn:aws:appmesh:{region}:{account}:mesh/{mesh}/virtualGateway/{gw}/gatewayRoute/{name}"
    )
}

fn op_create_gateway_route(
    state: &mut AppMeshState,
    mesh_name: &str,
    gw_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let exists = state
        .meshes
        .get(mesh_name)
        .map(|m| m.gateways.contains_key(gw_name))
        .unwrap_or(false);
    if !exists {
        return mesh_error(
            "VirtualGatewayNotFound",
            &format!(
                "There is no virtual gateway with the name {gw_name} inside the mesh {mesh_name}."
            ),
        );
    }
    let name = body
        .get("gatewayRouteName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = gateway_route_arn(account, region, mesh_name, gw_name, &name);
    let res = Resource {
        name: name.clone(),
        meta: Metadata::new(arn.clone()),
        spec: body.get("spec").cloned().unwrap_or_else(|| json!({})),
        parent: gw_name.to_string(),
    };
    store_tags(state, &arn, body);
    let mesh = state.meshes.get_mut(mesh_name).expect("present");
    mesh.gateway_routes
        .entry(gw_name.to_string())
        .or_default()
        .insert(name.clone(), res);
    let gr = mesh
        .gateway_routes
        .get(gw_name)
        .and_then(|g| g.get(&name))
        .expect("inserted");
    ok_json(&resource_to_json(
        "gatewayRouteName",
        &name,
        mesh_name,
        gw_name,
        gr,
    ))
}

fn op_list_gateway_routes(state: &AppMeshState, mesh_name: &str, gw_name: &str) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let mesh = state.meshes.get(mesh_name).expect("checked");
    let Some(routes) = mesh.gateway_routes.get(gw_name) else {
        return mesh_error(
            "VirtualGatewayNotFound",
            &format!(
                "There is no virtual gateway with the name {gw_name} inside the mesh {mesh_name}."
            ),
        );
    };
    let list: Vec<Value> = routes
        .values()
        .map(|g| list_summary("gatewayRouteName", &g.name, mesh_name, &g.meta))
        .collect();
    ok_json(&json!({ "nextToken": Value::Null, "gatewayRoutes": list }))
}

#[allow(clippy::too_many_arguments)]
fn op_gateway_route_item(
    state: &mut AppMeshState,
    method: &str,
    mesh_name: &str,
    gw_name: &str,
    gr_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    if let Err(resp) = require_mesh(state, mesh_name) {
        return resp;
    }
    let gateway_exists = state
        .meshes
        .get(mesh_name)
        .map(|m| m.gateways.contains_key(gw_name))
        .unwrap_or(false);
    if !gateway_exists {
        return mesh_error(
            "VirtualGatewayNotFound",
            &format!(
                "There is no virtual gateway with the name {gw_name} inside the mesh {mesh_name}."
            ),
        );
    }
    let exists = state
        .meshes
        .get(mesh_name)
        .and_then(|m| m.gateway_routes.get(gw_name))
        .map(|g| g.contains_key(gr_name))
        .unwrap_or(false);
    if !exists {
        return mesh_error(
            "GatewayRouteNotFound",
            &format!(
                "There is no gateway route with the name {gr_name} inside the mesh {mesh_name}."
            ),
        );
    }
    match method {
        "GET" => {
            let mesh = state.meshes.get(mesh_name).expect("present");
            let gr = &mesh.gateway_routes[gw_name][gr_name];
            ok_json(&resource_to_json(
                "gatewayRouteName",
                gr_name,
                mesh_name,
                gw_name,
                gr,
            ))
        }
        "PUT" => {
            if let Some(spec) = body.get("spec") {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                let gr = mesh
                    .gateway_routes
                    .get_mut(gw_name)
                    .and_then(|g| g.get_mut(gr_name))
                    .expect("checked");
                gr.spec = spec.clone();
                gr.meta.update();
            }
            let mesh = state.meshes.get(mesh_name).expect("present");
            let gr = &mesh.gateway_routes[gw_name][gr_name];
            ok_json(&resource_to_json(
                "gatewayRouteName",
                gr_name,
                mesh_name,
                gw_name,
                gr,
            ))
        }
        "DELETE" => {
            let arn = gateway_route_arn(account, region, mesh_name, gw_name, gr_name);
            let deleted = {
                let mesh = state.meshes.get_mut(mesh_name).expect("present");
                mesh.gateway_routes
                    .get_mut(gw_name)
                    .expect("checked")
                    .remove(gr_name)
                    .expect("checked")
            };
            let mut view =
                resource_to_json("gatewayRouteName", gr_name, mesh_name, gw_name, &deleted);
            if let Some(obj) = view.get_mut("status") {
                *obj = json!({ "status": "DELETED" });
            }
            state.tags.remove(&arn);
            ok_json(&view)
        }
        _ => mesh_error("NotImplemented", &format!("{method} on gatewayRoute")),
    }
}

// --- tags ------------------------------------------------------------------

/// get_resource_with_arn equivalent: resourceArn arrives in the body for
/// the Python gateway's moto path, or as a querystring member per the spec
/// for direct SDK calls.
fn op_tags(state: &mut AppMeshState, method: &str, body: &Value, query: &str) -> Response {
    let arn = body
        .get("resourceArn")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| query_single(query, "resourceArn"))
        .unwrap_or_default();
    match method {
        "GET" => {
            if !state.tags.contains_key(arn.as_str()) {
                // moto raises ResourceNotFound for unknown arns
                return mesh_error(
                    "ResourceNotFound",
                    &format!("There are no mesh resources with the arn {arn}."),
                );
            }
            let tags = tags_json(state.tags.get(arn.as_str()).expect("present"));
            ok_json(&json!({ "nextToken": Value::Null, "tags": tags }))
        }
        "POST" => {
            let mut new_tags = Vec::new();
            for t in body
                .get("tags")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                new_tags.push((
                    t.get("key")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    t.get("value")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                ));
            }
            if !new_tags.is_empty() {
                if !state.tags.contains_key(arn.as_str()) {
                    return mesh_error(
                        "ResourceNotFound",
                        &format!("There are no mesh resources with the arn {arn}."),
                    );
                }
                tags_mut(state, &arn).extend(new_tags);
            }
            json_resp(200, &json!({}))
        }
        "DELETE" => {
            // tagKeys arrives in the JSON body per the spec; some callers
            // also repeat them as ?tagKeys= query params.
            let mut keys: Vec<String> = body
                .get("tagKeys")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            keys.extend(query_multi(query, "tagKeys"));
            if let Some(tags) = state.tags.get_mut(&arn) {
                tags.retain(|(k, _)| !keys.contains(&k.clone()));
            }
            json_resp(200, &json!({}))
        }
        _ => mesh_error("NotImplemented", method),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(method: &str, path: &str, body: Value) -> Response {
        dispatch(&Request {
            service: "appmesh".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        })
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn create_mesh(name: &str) {
        let resp = run("PUT", "/v20190125/meshes", json!({ "meshName": name }));
        assert_eq!(
            resp.status,
            200,
            "create_mesh body = {}",
            String::from_utf8_lossy(&resp.body)
        );
    }

    #[test]
    fn mesh_lifecycle() {
        create_mesh("unit-mesh");
        let described = body_of(&run("GET", "/v20190125/meshes/unit-mesh", json!({})));
        assert_eq!(described["meshName"], "unit-mesh");
        assert!(described["metadata"]["version"].as_i64().unwrap() >= 1);
        assert!(described["spec"].is_object());
        assert!(described["status"].is_object());

        let listed = body_of(&run("GET", "/v20190125/meshes", json!({})));
        assert!(listed["meshes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["meshName"] == "unit-mesh"));

        run(
            "PUT",
            "/v20190125/meshes/unit-mesh",
            json!({"spec": {"egressFilter": {"type": "ALLOW_ALL"}}}),
        );
        let described = body_of(&run("GET", "/v20190125/meshes/unit-mesh", json!({})));
        assert_eq!(described["metadata"]["version"], 2);

        run("DELETE", "/v20190125/meshes/unit-mesh", json!({}));
        let gone = run("GET", "/v20190125/meshes/unit-mesh", json!({}));
        assert_eq!(gone.status, 400);
        assert!(String::from_utf8_lossy(&gone.body).contains("MeshNotFound"));
    }

    #[test]
    fn virtual_node_cycle() {
        create_mesh("mesh-nodes");
        let resp = run(
            "PUT",
            "/v20190125/meshes/mesh-nodes/virtualNodes",
            json!({
                "virtualNodeName": "node1",
                "spec": {"listeners": [], "serviceDiscovery": {"dns": {"hostname": "test.local"}}}
            }),
        );
        let node = body_of(&resp);
        assert_eq!(node["virtualNodeName"], "node1");
        assert_eq!(node["meshName"], "mesh-nodes");

        let listed = body_of(&run(
            "GET",
            "/v20190125/meshes/mesh-nodes/virtualNodes",
            json!({}),
        ));
        assert!(listed["virtualNodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["virtualNodeName"] == "node1"));

        let described = body_of(&run(
            "GET",
            "/v20190125/meshes/mesh-nodes/virtualNodes/node1",
            json!({}),
        ));
        assert!(described["spec"].is_object());
        assert!(described["status"].is_object());
        assert!(described["metadata"].is_object());

        run(
            "DELETE",
            "/v20190125/meshes/mesh-nodes/virtualNodes/node1",
            json!({}),
        );
        let gone = run(
            "GET",
            "/v20190125/meshes/mesh-nodes/virtualNodes/node1",
            json!({}),
        );
        assert!(String::from_utf8_lossy(&gone.body).contains("VirtualNodeNotFound"));
    }

    #[test]
    fn router_and_routes_cycle() {
        create_mesh("mesh-routes");
        run(
            "PUT",
            "/v20190125/meshes/mesh-routes/virtualRouters",
            json!({"virtualRouterName": "router1", "spec": {"listeners": [{"portMapping": {"port": 8080, "protocol": "http"}}]}}),
        );
        run(
            "PUT",
            "/v20190125/meshes/mesh-routes/virtualNodes",
            json!({"virtualNodeName": "target", "spec": {}}),
        );
        let resp = run(
            "PUT",
            "/v20190125/meshes/mesh-routes/virtualRouter/router1/routes",
            json!({
                "routeName": "r1",
                "spec": {"httpRoute": {"match": {"prefix": "/"}, "action": {"weightedTargets": [{"virtualNode": "target", "weight": 100}]}}}
            }),
        );
        assert_eq!(body_of(&resp)["routeName"], "r1");
        assert_eq!(body_of(&resp)["virtualRouterName"], "router1");
        let listed = body_of(&run(
            "GET",
            "/v20190125/meshes/mesh-routes/virtualRouter/router1/routes",
            json!({}),
        ));
        assert!(listed["routes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["routeName"] == "r1"));
        run(
            "DELETE",
            "/v20190125/meshes/mesh-routes/virtualRouter/router1/routes/r1",
            json!({}),
        );
        let gone = run(
            "GET",
            "/v20190125/meshes/mesh-routes/virtualRouter/router1/routes/r1",
            json!({}),
        );
        assert!(String::from_utf8_lossy(&gone.body).contains("RouteNotFound"));
    }

    #[test]
    fn gateway_routes_cycle() {
        create_mesh("mesh-gw");
        run(
            "PUT",
            "/v20190125/meshes/mesh-gw/virtualGateways",
            json!({"virtualGatewayName": "gw1", "spec": {"listeners": []}}),
        );
        let resp = run(
            "PUT",
            "/v20190125/meshes/mesh-gw/virtualGateway/gw1/gatewayRoutes",
            json!({"gatewayRouteName": "gr1", "spec": {}}),
        );
        assert_eq!(body_of(&resp)["gatewayRouteName"], "gr1");
        let listed = body_of(&run(
            "GET",
            "/v20190125/meshes/mesh-gw/virtualGateway/gw1/gatewayRoutes",
            json!({}),
        ));
        assert_eq!(listed["gatewayRoutes"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn tag_list_roundtrip() {
        create_mesh("mesh-tags");
        let described = body_of(&run("GET", "/v20190125/meshes/mesh-tags", json!({})));
        let arn = described["metadata"]["arn"].as_str().unwrap().to_string();
        run(
            "PUT",
            "/v20190125/tag",
            json!({"resourceArn": arn, "tags": [{"key": "env", "value": "test"}]}),
        );
        let listed = body_of(&run("GET", "/v20190125/tags", json!({"resourceArn": arn})));
        let tags = listed["tags"].as_array().unwrap();
        assert!(tags.iter().any(|t| t["key"] == "env"));
        run(
            "PUT",
            "/v20190125/untag",
            json!({"resourceArn": arn, "tagKeys": ["env"]}),
        );
        let listed = body_of(&run("GET", "/v20190125/tags", json!({"resourceArn": arn})));
        assert!(listed["tags"].as_array().unwrap().is_empty());
    }
}
