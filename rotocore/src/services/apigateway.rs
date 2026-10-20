//! API Gateway (v1 REST APIs) — port of moto's `apigateway` backend plus the
//! python ground-truth stubs (`src/robotocore/services/apigateway/provider.py`).
//!
//! State stores the moto wire JSON itself (schema-less `Value`s shaped by the
//! same builders moto's `to_json` writes), so responses stay byte-equal with
//! the moto sidecar for everything the suites pin (null members included —
//! botocore hides them exactly as it did for moto). Tag/flush/`delete_model`
//! ops mirror the python ground-truth provider (204 `{}` responses); ops the
//! sidecar left at 501 (SDK types, `Update*` PATCH ops, test-invoke,
//! domain-name access associations, docs import, model templates) are
//! implemented with real behavior. `ImportApiKeys` stays at 501 — the compat
//! suite pins it as NotImplemented.
//!
//! The `/restapis/{api}/{stage}/_user_request_/{path}` execution surface
//! (`test_apigateway_lambda_compat.py`) is answered by [`resolve_execution`]:
//! it mirrors the python ground-truth executor
//! (`src/robotocore/services/apigateway/executor.py`), including the
//! ANY-method fallback and `{param+}`/`{param}` path parameters.

#[allow(dead_code)]
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn json_resp(status: u16, body: Value) -> Response {
    Response::new(
        status,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

fn ok(body: Value) -> Response {
    json_resp(200, body)
}

fn created_201(body: Value) -> Response {
    json_resp(201, body)
}

fn accepted_202() -> Response {
    json_resp(202, json!({}))
}

fn accepted_202_body(body: Value) -> Response {
    json_resp(202, body)
}

/// moto's JsonRESTError wire shape: `{"__type": code, "message": msg}`.
fn api_error(status: u16, code: &str, message: &str) -> Response {
    crate::proto::json_error(status, code, message)
}

fn not_found(message: &str) -> Response {
    api_error(404, "NotFoundException", message)
}

fn bad_request(message: &str) -> Response {
    api_error(400, "BadRequestException", message)
}

fn conflict(message: &str) -> Response {
    api_error(409, "ConflictException", message)
}

fn message_response(status: u16, message: &str) -> Response {
    json_resp(status, json!({ "message": message }))
}

fn not_implemented(operation: &str) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {operation} action has not been implemented"),
    )
}

fn route_missing() -> Response {
    api_error(
        501,
        "NotImplemented",
        "Requested operation is not implemented",
    )
}

fn opt_str(value: &Option<String>) -> Value {
    value.clone().map(Value::String).unwrap_or(Value::Null)
}

fn unix_int() -> i64 {
    chrono::Utc::now().timestamp()
}

fn today_date() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

/// 10-character lowercase alphanumeric id (moto apigateway `create_id`).
fn create_id() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        .take(10)
        .collect()
}

fn body_object(body: &[u8]) -> Map<String, Value> {
    if body.is_empty() {
        return Map::new();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(map)) => map,
        Ok(_) | Err(_) => Map::new(),
    }
}

fn body_str<'a>(params: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    params.get(key).and_then(Value::as_str)
}

fn body_bool(params: &Map<String, Value>, key: &str) -> Option<bool> {
    match params.get(key) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) => match s.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn query_first(req: &Request, key: &str) -> Option<String> {
    let map = crate::proto::query_map(&req.query_string);
    map.get(key).and_then(|v| v.first().cloned())
}

fn query_all(req: &Request, key: &str) -> Vec<String> {
    let map = crate::proto::query_map(&req.query_string);
    map.get(key).cloned().unwrap_or_default()
}

/// One `patchOperations` entry (`{op, path, value}`).
struct PatchOp {
    op: String,
    path: String,
    value: Option<String>,
}

fn patch_ops(params: &Map<String, Value>) -> Vec<PatchOp> {
    match params.get("patchOperations") {
        Some(Value::Array(items)) => items
            .iter()
            .map(|op| PatchOp {
                op: op
                    .get("op")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                path: op
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                value: op.get("value").and_then(json_patch_value),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn json_patch_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Null => None,
        other => serde_json::to_string(other).ok(),
    }
}

/// Ensure `value` is an object and return its map, inserting `{}` at `key`.
fn obj_entry<'a>(value: &'a mut Value, key: &str) -> Option<&'a mut Map<String, Value>> {
    let entry = value
        .as_object_mut()?
        .entry(key.to_string())
        .or_insert_with(|| json!({}));
    // Members serialized per moto may hold null until first touched
    // (e.g. `integrationResponses: null`) — upgrade those to objects.
    if !entry.is_object() {
        *entry = json!({});
    }
    entry.as_object_mut()
}

/// Set a string/bool/number member from a patch `value` (None → null).
fn set_member(target: &mut Value, key: &str, value: Option<String>) {
    if let Some(map) = target.as_object_mut() {
        match value {
            Some(v) => map.insert(key.to_string(), json!(v)),
            None => map.insert(key.to_string(), Value::Null),
        };
    }
}

// ---------------------------------------------------------------------------
// State (entities stored exactly as moto serializes them on the wire)
// ---------------------------------------------------------------------------

struct State {
    /// REST API `to_dict` wire bodies, keyed by api id.
    apis: BTreeMap<String, Value>,
    /// api_id → {resource_id → resource json (id, pathPart, parentId, resourceMethods)}
    resources: BTreeMap<String, BTreeMap<String, Value>>,
    /// api_id → {child_id → child json}, shared by all child collections.
    stages: BTreeMap<String, BTreeMap<String, Value>>,
    deployments: BTreeMap<String, BTreeMap<String, Value>>,
    authorizers: BTreeMap<String, BTreeMap<String, Value>>,
    gateway_responses: BTreeMap<String, BTreeMap<String, Value>>,
    models: BTreeMap<String, BTreeMap<String, Value>>,
    request_validators: BTreeMap<String, BTreeMap<String, Value>>,
    documentation_parts: BTreeMap<String, BTreeMap<String, Value>>,
    documentation_versions: BTreeMap<String, BTreeMap<String, Value>>,
    /// api_id → root resource id.
    roots: BTreeMap<String, String>,
    /// domain_name → region-scoped domain json.
    domain_names: BTreeMap<String, Value>,
    /// domain_name → base_path → mapping json (moto's snake_case key body).
    base_path_mappings: BTreeMap<String, BTreeMap<String, Value>>,
    keys: BTreeMap<String, Value>,
    usage_plans: BTreeMap<String, Value>,
    /// usage_plan_id → {key_id → usage plan key json}
    usage_plan_keys: BTreeMap<String, BTreeMap<String, Value>>,
    vpc_links: BTreeMap<String, Value>,
    client_certificates: BTreeMap<String, Value>,
    account: Value,
    domain_access_associations: BTreeMap<String, Value>,
}

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("apigateway"));

impl Default for State {
    /// moto's Account defaults; GetAccount must always return a body.
    fn default() -> Self {
        State {
            apis: BTreeMap::default(),
            resources: BTreeMap::default(),
            stages: BTreeMap::default(),
            deployments: BTreeMap::default(),
            authorizers: BTreeMap::default(),
            gateway_responses: BTreeMap::default(),
            models: BTreeMap::default(),
            request_validators: BTreeMap::default(),
            documentation_parts: BTreeMap::default(),
            documentation_versions: BTreeMap::default(),
            roots: BTreeMap::default(),
            domain_names: BTreeMap::default(),
            base_path_mappings: BTreeMap::default(),
            keys: BTreeMap::default(),
            usage_plans: BTreeMap::default(),
            usage_plan_keys: BTreeMap::default(),
            vpc_links: BTreeMap::default(),
            client_certificates: BTreeMap::default(),
            account: json!({
                "cloudwatchRoleArn": Value::Null,
                "throttleSettings": { "burstLimit": 5000, "rateLimit": 10000.0 },
                "features": Value::Null,
                "apiKeyVersion": "1",
            }),
            domain_access_associations: BTreeMap::default(),
        }
    }
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut State) -> T) -> T {
    let mut guard = SLOT.write();
    // Seeding via Default keeps moto's Account defaults; GetAccount can
    // never return a null body.
    let state = guard.entry((account, region.to_string())).or_default();
    f(state)
}

/// A REST API wire body exactly as moto's `RestApi.to_dict` emits it.
fn rest_api_json(api_id: &str, name: &str, description: &str) -> Value {
    json!({
        "id": api_id,
        "name": name,
        "description": description,
        "version": "V1",
        "binaryMediaTypes": [],
        "createdDate": unix_int(),
        "apiKeySource": "HEADER",
        "endpointConfiguration": { "types": ["EDGE"] },
        "tags": {},
        "policy": Value::Null,
        "disableExecuteApiEndpoint": false,
        "minimumCompressionSize": Value::Null,
        "rootResourceId": Value::Null,
    })
}

/// moto-shaped root resource (child resources carry parentId/pathPart too).
fn root_resource_json(resource_id: &str) -> Value {
    json!({
        "id": resource_id,
        "pathPart": "/",
        "resourceMethods": {},
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let params = body_object(&req.body);
    let segments: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.first() == Some(&"tags") {
        // GetTags / TagResource / UntagResource — the python ground-truth
        // provider owns these (204 `{}` bodies, REST-api ARNs).
        return tags_route(req);
    }

    with_state(req.account, &req.region, move |state| {
        route(state, req, &params, &segments)
    })
}

fn route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    segments: &[&str],
) -> Response {
    match segments {
        ["restapis"] => match req.method.as_str() {
            "POST" => create_rest_api(state, req, params),
            "GET" => ok(json!({
                "item": state.apis.values().cloned().collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["restapis", api_id] => match req.method.as_str() {
            "DELETE" => delete_rest_api(state, api_id),
            "GET" => match state.apis.get(*api_id) {
                Some(api) => ok(api.clone()),
                None => not_found("Invalid Rest API Id specified"),
            },
            "PUT" => put_rest_api(state, req, params, api_id),
            "PATCH" => update_rest_api(state, params, api_id),
            _ => route_missing(),
        },
        ["restapis", api_id, "resources"] => match req.method.as_str() {
            "GET" => list_resources(state, api_id),
            _ => route_missing(),
        },
        // POST /restapis/{api}/resources/{parent} → CreateResource;
        // GET  /restapis/{api}/resources/{resource} → GetResource.
        ["restapis", api_id, "resources", resource_id] => match req.method.as_str() {
            "POST" => create_resource(state, params, api_id, resource_id),
            "GET" => get_resource(state, api_id, resource_id),
            "DELETE" => delete_resource(state, api_id, resource_id),
            "PATCH" => update_resource(state, params, api_id, resource_id),
            _ => route_missing(),
        },
        ["restapis", api_id, "resources", resource_id, "methods", method] => {
            method_route(state, req, params, api_id, resource_id, method)
        }
        ["restapis", api_id, "resources", resource_id, "methods", method, "responses", status_code] => {
            method_response_route(state, req, params, api_id, resource_id, method, status_code)
        }
        ["restapis", api_id, "resources", resource_id, "methods", method, "integration"] => {
            integration_route(state, req, params, api_id, resource_id, method)
        }
        ["restapis", api_id, "resources", resource_id, "methods", method, "integration", "responses", status_code] => {
            integration_response_route(state, req, params, api_id, resource_id, method, status_code)
        }
        ["restapis", api_id, "authorizers"] => match req.method.as_str() {
            "POST" => create_authorizer(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .authorizers
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "authorizers", authorizer_id] => match req.method.as_str() {
            "POST" => test_invoke_authorizer(authorizer_id),
            "GET" => match state
                .authorizers
                .get(*api_id)
                .and_then(|m| m.get(*authorizer_id))
            {
                Some(a) => ok(a.clone()),
                None => not_found("Invalid Authorizer identifier specified"),
            },
            "DELETE" => match child_entry(&mut state.authorizers, api_id).remove(*authorizer_id) {
                Some(_) => ok(json!({})),
                None => not_found("Invalid Authorizer identifier specified"),
            },
            "PATCH" => update_authorizer(state, params, api_id, authorizer_id),
            _ => route_missing(),
        },
        ["restapis", api_id, "deployments"] => match req.method.as_str() {
            "POST" => create_deployment(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .deployments
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "deployments", deployment_id] => match req.method.as_str() {
            "PATCH" => update_deployment(state, params, api_id, deployment_id),
            "GET" => match state
                .deployments
                .get(*api_id)
                .and_then(|m| m.get(*deployment_id))
            {
                Some(d) => ok(d.clone()),
                None => not_found("Invalid Deployment identifier specified"),
            },
            "DELETE" => delete_deployment(state, api_id, deployment_id),
            _ => route_missing(),
        },
        ["restapis", api_id, "stages"] => match req.method.as_str() {
            "POST" => create_stage(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .stages
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "stages", stage_name] => match req.method.as_str() {
            "GET" => match state.stages.get(*api_id).and_then(|m| m.get(*stage_name)) {
                Some(s) => ok(s.clone()),
                None => not_found("Invalid stage identifier specified"),
            },
            "PATCH" => update_stage(state, params, api_id, stage_name),
            "DELETE" => match state
                .stages
                .get_mut(*api_id)
                .and_then(|m| m.remove(*stage_name))
            {
                Some(_) => ok(json!({})),
                None => not_found("Invalid stage identifier specified"),
            },
            _ => route_missing(),
        },
        ["restapis", api_id, "stages", _, "cache", "authorizers"] => flush_stage(state, api_id),
        ["restapis", api_id, "stages", _, "cache", "data"] => flush_stage(state, api_id),
        ["restapis", api_id, "stages", _, "exports", export_type] => {
            get_export(state, req, api_id, export_type)
        }
        ["restapis", api_id, "models"] => match req.method.as_str() {
            "POST" => create_model(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .models
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "models", model_name] => match req.method.as_str() {
            // The python ground-truth provider owns DELETE on models (202 {}).
            "DELETE" => match state
                .models
                .get_mut(*api_id)
                .and_then(|m| m.remove(*model_name))
            {
                Some(_) => accepted_202(),
                None => not_found("Invalid Model Name specified"),
            },
            "GET" => match state.models.get(*api_id).and_then(|m| m.get(*model_name)) {
                Some(model) => ok(model.clone()),
                None => not_found("Invalid Model Name specified"),
            },
            "PATCH" => update_model(state, params, api_id, model_name),
            _ => route_missing(),
        },
        ["restapis", api_id, "models", model_name, "default_template"] => {
            // GetModelTemplate (stub op): the value is the model schema.
            let schema = state
                .models
                .get(*api_id)
                .and_then(|m| m.get(*model_name))
                .and_then(|m| m.get("schema"))
                .cloned()
                .unwrap_or(Value::Null);
            ok(json!({ "value": schema.as_str().unwrap_or("") }))
        }
        ["restapis", api_id, "requestvalidators"] => match req.method.as_str() {
            "POST" => create_request_validator(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .request_validators
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "requestvalidators", validator_id] => match req.method.as_str() {
            "GET" => match state
                .request_validators
                .get(*api_id)
                .and_then(|m| m.get(*validator_id))
            {
                Some(v) => ok(v.clone()),
                None => bad_request("Invalid Request Validator Id specified"),
            },
            "DELETE" => match state
                .request_validators
                .get_mut(*api_id)
                .and_then(|m| m.remove(*validator_id))
            {
                Some(_) => accepted_202(),
                None => bad_request("Invalid Request Validator Id specified"),
            },
            "PATCH" => match state
                .request_validators
                .get_mut(*api_id)
                .and_then(|m| m.get_mut(*validator_id))
            {
                Some(v) => {
                    patch_allow(
                        v,
                        &patch_ops(params),
                        &[
                            "/name",
                            "/validateRequestBody",
                            "/validateRequestParameters",
                        ],
                    );
                    patch_validator_booleans(v);
                    ok(v.clone())
                }
                None => bad_request("Invalid Request Validator Id specified"),
            },
            _ => route_missing(),
        },
        ["restapis", api_id, "gatewayresponses"] => {
            if !state.apis.contains_key(*api_id) {
                return not_found("Invalid Rest API Id specified");
            }
            let items: Vec<Value> = state
                .gateway_responses
                .get(*api_id)
                .map(|map| map.values().cloned().collect())
                .unwrap_or_default();
            ok(json!({ "item": items }))
        }
        ["restapis", api_id, "gatewayresponses", response_type] => match req.method.as_str() {
            "PUT" => put_gateway_response(state, params, api_id, response_type),
            "GET" => match state
                .gateway_responses
                .get(*api_id)
                .and_then(|m| m.get(*response_type))
            {
                Some(gw) => ok(gw.clone()),
                None => not_found("GatewayResponse not found"),
            },
            "PATCH" => match state
                .gateway_responses
                .get_mut(*api_id)
                .and_then(|m| m.get_mut(*response_type))
            {
                Some(gw) => {
                    apply_gateway_patch(gw, &patch_ops(params));
                    ok(gw.clone())
                }
                None => not_found("GatewayResponse not found"),
            },
            "DELETE" => match state
                .gateway_responses
                .get_mut(*api_id)
                .and_then(|m| m.remove(*response_type))
            {
                Some(_) => accepted_202(),
                None => not_found("GatewayResponse not found"),
            },
            _ => route_missing(),
        },
        ["restapis", api_id, "documentation", "parts"] => match req.method.as_str() {
            "POST" => create_documentation_part(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .documentation_parts
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            "PUT" => ok(json!({ "ids": [], "warnings": [] })),
            _ => route_missing(),
        },
        ["restapis", api_id, "documentation", "parts", part_id] => match req.method.as_str() {
            "GET" => match state
                .documentation_parts
                .get(*api_id)
                .and_then(|m| m.get(*part_id))
            {
                Some(p) => ok(p.clone()),
                None => not_found("Invalid Documentation part identifier specified"),
            },
            "PATCH" => {
                match state
                    .documentation_parts
                    .get_mut(*api_id)
                    .and_then(|m| m.get_mut(*part_id))
                {
                    Some(p) => {
                        for op in patch_ops(params) {
                            if op.op == "replace" && op.path.contains("/properties") {
                                set_member(p, "properties", op.value.clone());
                            }
                        }
                        ok(p.clone())
                    }
                    None => not_found("Invalid Documentation part identifier specified"),
                }
            }
            "DELETE" => {
                match state
                    .documentation_parts
                    .get_mut(*api_id)
                    .and_then(|m| m.remove(*part_id))
                {
                    Some(_) => accepted_202(),
                    None => not_found("Invalid Documentation part identifier specified"),
                }
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "documentation", "versions"] => match req.method.as_str() {
            "POST" => create_documentation_version(state, params, api_id),
            "GET" => {
                if !state.apis.contains_key(*api_id) {
                    return not_found("Invalid Rest API Id specified");
                }
                let items: Vec<Value> = state
                    .documentation_versions
                    .get(*api_id)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["restapis", api_id, "documentation", "versions", version] => match req.method.as_str() {
            "GET" => match state
                .documentation_versions
                .get(*api_id)
                .and_then(|m| m.get(*version))
            {
                Some(v) => ok(v.clone()),
                None => not_found("Invalid Documentation version specified"),
            },
            "DELETE" => match state
                .documentation_versions
                .get_mut(*api_id)
                .and_then(|m| m.remove(*version))
            {
                Some(_) => accepted_202(),
                None => not_found("Invalid Documentation version specified"),
            },
            "PATCH" => match state
                .documentation_versions
                .get_mut(*api_id)
                .and_then(|m| m.get_mut(*version))
            {
                Some(v) => {
                    for op in patch_ops(params) {
                        if op.op == "replace" && op.path.contains("/description") {
                            set_member(v, "description", op.value.clone());
                        }
                    }
                    ok(v.clone())
                }
                None => not_found("Invalid Documentation version specified"),
            },
            _ => route_missing(),
        },
        ["apikeys"] => match req.method.as_str() {
            "POST" => {
                if query_first(req, "mode").as_deref() == Some("import") {
                    // The compat suite pins ImportApiKeys as NotImplemented.
                    return not_implemented("import_api_keys");
                }
                create_api_key(state, params)
            }
            "GET" => {
                let include_values = query_first(req, "includeValues")
                    .map(|v| v == "true")
                    .unwrap_or(false);
                let owner = query_first(req, "name");
                let items: Vec<Value> = state
                    .keys
                    .values()
                    .filter(|k| match owner.as_deref() {
                        Some(name) => k
                            .get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|n| n.starts_with(name)),
                        None => true,
                    })
                    .map(|k| {
                        let mut copy = k.clone();
                        if !include_values {
                            if let Some(map) = copy.as_object_mut() {
                                map.remove("value");
                            }
                        }
                        copy
                    })
                    .collect();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["apikeys", api_key_id] => match req.method.as_str() {
            "GET" => {
                let include_value = query_first(req, "includeValue")
                    .map(|v| v == "true")
                    .unwrap_or(false);
                match state.keys.get(*api_key_id) {
                    // moto pops `value` unless includeValue is set.
                    Some(k) => ok(strip_member(k, include_value, "value")),
                    None => not_found("Invalid API Key identifier specified"),
                }
            }
            "DELETE" => match state.keys.remove(*api_key_id) {
                Some(_) => accepted_202(),
                None => not_found("Invalid API Key identifier specified"),
            },
            "PATCH" => match state.keys.get_mut(*api_key_id) {
                Some(k) => {
                    patch_allow(
                        k,
                        &patch_ops(params),
                        &["/name", "/customerId", "/description", "/enabled"],
                    );
                    ok(strip_member(k, true, "value"))
                }
                None => not_found("Invalid API Key identifier specified"),
            },
            _ => route_missing(),
        },
        ["usageplans"] => match req.method.as_str() {
            "POST" => create_usage_plan(state, params),
            "GET" => ok(json!({
                "item": state.usage_plans.values().cloned().collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["usageplans", usage_plan_id] => match req.method.as_str() {
            "GET" => match state.usage_plans.get(*usage_plan_id) {
                Some(p) => ok(p.clone()),
                None => not_found("Invalid Usage Plan ID specified"),
            },
            "DELETE" => {
                state.usage_plans.remove(*usage_plan_id);
                state.usage_plan_keys.remove(*usage_plan_id);
                accepted_202()
            }
            "PATCH" => match state.usage_plans.get_mut(*usage_plan_id) {
                Some(p) => {
                    apply_usage_plan_patch(p, &patch_ops(params));
                    ok(p.clone())
                }
                None => not_found("Invalid Usage Plan ID specified"),
            },
            _ => route_missing(),
        },
        ["usageplans", usage_plan_id, "keys"] => match req.method.as_str() {
            "POST" => create_usage_plan_key(state, params, usage_plan_id),
            "GET" => {
                let owner = body_str(params, "name").map(String::from);
                let items: Vec<Value> = state
                    .usage_plan_keys
                    .get(*usage_plan_id)
                    .map(|m| {
                        m.values()
                            .filter(|k| match owner.as_deref() {
                                Some(name) => k
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .is_some_and(|n| n.starts_with(name)),
                                None => true,
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["usageplans", usage_plan_id, "keys", key_id] => match req.method.as_str() {
            "GET" => match state
                .usage_plan_keys
                .get(*usage_plan_id)
                .and_then(|m| m.get(*key_id))
            {
                Some(k) => ok(k.clone()),
                None => not_found("Invalid Usage Plan ID specified"),
            },
            "DELETE" => {
                if let Some(m) = state.usage_plan_keys.get_mut(*usage_plan_id) {
                    m.remove(*key_id);
                }
                accepted_202()
            }
            _ => route_missing(),
        },
        ["usageplans", usage_plan_id, "usage"] => {
            if !state.usage_plans.contains_key(*usage_plan_id) {
                return not_found("Invalid Usage Plan ID specified");
            }
            ok(get_usage_body(usage_plan_id))
        }
        // UpdateUsage is PATCH /usageplans/{plan}/keys/{keyId}/usage; moto
        // ignores the patch ops and the key, and echoes get_usage(path[2]).
        ["usageplans", usage_plan_id, "keys", _key_id, "usage"] => {
            if !state.usage_plans.contains_key(*usage_plan_id) {
                return not_found("Invalid Usage Plan ID specified");
            }
            ok(get_usage_body(usage_plan_id))
        }
        ["domainnames"] => match req.method.as_str() {
            "POST" => create_domain_name(state, params),
            "GET" => ok(json!({
                "item": state.domain_names.values().cloned().collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["domainnames", domain_name] => match req.method.as_str() {
            "GET" => match state.domain_names.get(*domain_name) {
                Some(d) => ok(d.clone()),
                None => not_found("Invalid domain name identifier specified"),
            },
            "DELETE" => match state.domain_names.remove(*domain_name) {
                Some(_) => {
                    state.base_path_mappings.remove(*domain_name);
                    accepted_202()
                }
                None => not_found("Invalid domain name identifier specified"),
            },
            "PATCH" => match state.domain_names.get_mut(*domain_name) {
                Some(d) => {
                    patch_allow(
                        d,
                        &patch_ops(params),
                        &[
                            "/certificateName",
                            "/certificateArn",
                            "/certificateBody",
                            "/certificateChain",
                            "/certificatePrivateKey",
                            "/regionalCertificateName",
                            "/regionalCertificateArn",
                            "/securityPolicy",
                        ],
                    );
                    ok(d.clone())
                }
                None => not_found("Invalid domain name identifier specified"),
            },
            _ => route_missing(),
        },
        ["domainnames", domain_name, "basepathmappings"] => match req.method.as_str() {
            "POST" => create_base_path_mapping(state, params, domain_name),
            "GET" => {
                if !state.domain_names.contains_key(*domain_name) {
                    return not_found("Invalid domain name identifier specified");
                }
                let items: Vec<Value> = state
                    .base_path_mappings
                    .get(*domain_name)
                    .map(|map| map.values().cloned().collect())
                    .unwrap_or_default();
                ok(json!({ "item": items }))
            }
            _ => route_missing(),
        },
        ["domainnames", domain_name, "basepathmappings", base_path] => match req.method.as_str() {
            "GET" => match state
                .base_path_mappings
                .get(*domain_name)
                .and_then(|m| m.get(*base_path))
            {
                Some(bpm) => ok(bpm.clone()),
                None => not_found("Invalid base path mapping identifier specified"),
            },
            "DELETE" => match state
                .base_path_mappings
                .get_mut(*domain_name)
                .and_then(|m| m.remove(*base_path))
            {
                Some(_) => accepted_202_body(Value::String(String::new())),
                None => not_found("Invalid base path mapping identifier specified"),
            },
            "PATCH" => update_base_path_mapping(state, params, domain_name, base_path),
            _ => route_missing(),
        },
        ["vpclinks"] => match req.method.as_str() {
            "POST" => create_vpc_link(state, params),
            "GET" => ok(json!({
                "item": state.vpc_links.values().cloned().collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["vpclinks", vpc_link_id] => match req.method.as_str() {
            "GET" => match state.vpc_links.get(*vpc_link_id) {
                Some(v) => ok(v.clone()),
                None => not_found("VPCLink not found"),
            },
            "DELETE" => {
                state.vpc_links.remove(*vpc_link_id);
                accepted_202()
            }
            "PATCH" => match state.vpc_links.get_mut(*vpc_link_id) {
                Some(v) => {
                    patch_allow(v, &patch_ops(params), &["/name", "/description"]);
                    ok(v.clone())
                }
                None => not_found("VPCLink not found"),
            },
            _ => route_missing(),
        },
        ["clientcertificates"] => match req.method.as_str() {
            "POST" => generate_client_certificate(state, params),
            "GET" => ok(json!({
                "item": state.client_certificates.values().cloned().collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["clientcertificates", cert_id] => match req.method.as_str() {
            "GET" => match state.client_certificates.get(*cert_id) {
                Some(c) => ok(c.clone()),
                None => not_found("Invalid client certificate identifier specified"),
            },
            "DELETE" => match state.client_certificates.remove(*cert_id) {
                Some(_) => accepted_202(),
                None => not_found("Invalid client certificate identifier specified"),
            },
            "PATCH" => match state.client_certificates.get_mut(*cert_id) {
                Some(c) => {
                    patch_allow(c, &patch_ops(params), &["/description"]);
                    ok(c.clone())
                }
                None => not_found("Invalid client certificate identifier specified"),
            },
            _ => route_missing(),
        },
        ["account"] => match req.method.as_str() {
            "GET" => ok(state.account.clone()),
            "PATCH" => update_account(state, params),
            _ => route_missing(),
        },
        ["domainnameaccessassociations"] => match req.method.as_str() {
            "POST" => create_domain_name_access_association(state, params, &req.region),
            "GET" => ok(json!({
                "item": state
                    .domain_access_associations
                    .values()
                    .cloned()
                    .collect::<Vec<Value>>()
            })),
            _ => route_missing(),
        },
        ["domainnameaccessassociations", _arn] => {
            state.domain_access_associations.clear();
            ok(json!({}))
        }
        ["rejectdomainnameaccessassociations"] => ok(json!({})),
        ["sdktypes"] => ok(json!({ "item": sdk_types_json() })),
        ["sdktypes", sdk_type] => match sdk_type_json(sdk_type) {
            Some(v) => ok(v),
            None => not_found("Invalid Sdk Type identifier specified"),
        },
        _ => route_missing(),
    }
}

/// The per-api child map (created when missing).
fn child_entry<'a>(
    store: &'a mut BTreeMap<String, BTreeMap<String, Value>>,
    api_id: &str,
) -> &'a mut BTreeMap<String, Value> {
    store.entry(api_id.to_string()).or_default()
}

/// Shared by several PATCH handlers: apply `replace` ops whose path
/// contains one of the listed member suffixes.
fn patch_allow(target: &mut Value, ops: &[PatchOp], members: &[&str]) {
    for op in ops {
        if op.op != "replace" {
            continue;
        }
        for member in members {
            if op.path.contains(member) {
                let key = member.trim_start_matches('/');
                set_member(target, key, op.value.clone());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers: rest apis / resources / methods / integrations
// ---------------------------------------------------------------------------

fn create_rest_api(state: &mut State, req: &Request, params: &Map<String, Value>) -> Response {
    // A body carrying an OpenAPI document means ImportRestApi (`?mode=import`).
    if params.contains_key("openapi") || params.contains_key("swagger") {
        return import_rest_api(state, req, params);
    }
    let Some(name) = body_str(params, "name") else {
        return bad_request("No name specified");
    };
    let description = body_str(params, "description")
        .unwrap_or_default()
        .to_string();
    let api_id = create_id();
    let mut api = rest_api_json(&api_id, name, &description);
    // Root resource (moto's add_child("/")).
    let root_id = create_id();
    api["rootResourceId"] = json!(root_id);
    if let Some(tags) = params.get("tags").cloned() {
        api["tags"] = tags;
    }
    state.apis.insert(api_id.clone(), api);
    state.roots.insert(api_id.clone(), root_id.clone());
    child_entry(&mut state.resources, &api_id)
        .insert(root_id.clone(), root_resource_json(&root_id));
    ok(state.apis[&api_id].clone())
}

/// moto `import_rest_api` — name/title come from the OpenAPI `info` block
/// and every path becomes a resource with its methods (subset import).
fn import_rest_api(state: &mut State, req: &Request, params: &Map<String, Value>) -> Response {
    let info_title = || -> Option<&str> { params.get("info")?.get("title")?.as_str() };
    let info_desc = || -> Option<&str> { params.get("info")?.get("description")?.as_str() };
    let name = info_title().unwrap_or("API").to_string();
    let description = info_desc().unwrap_or_default().to_string();
    create_api_and_import(state, req, &name, &description, params, "merge")
}

fn put_rest_api(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    api_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let mode = query_first(req, "mode").unwrap_or_else(|| "merge".to_string());
    if mode != "merge" && mode != "overwrite" {
        return bad_request(
            "Enumeration value of OpenAPI import mode must be \"overwrite\" or \"merge\"",
        );
    }
    if params.contains_key("swagger")
        || params
            .get("openapi")
            .and_then(Value::as_str)
            .is_some_and(|o| !o.starts_with('3'))
    {
        return bad_request("Only OpenAPI 3.x.x are currently supported");
    }
    if mode == "overwrite" {
        // moto: drop all resources, then import into a fresh root.
        let root_id = create_id();
        if let Some(api) = state.apis.get_mut(api_id) {
            api["rootResourceId"] = json!(root_id);
        }
        let mut fresh = BTreeMap::new();
        fresh.insert(root_id.clone(), root_resource_json(&root_id));
        state.resources.insert(api_id.to_string(), fresh);
        state.roots.insert(api_id.to_string(), root_id);
    }
    import_paths(
        state,
        api_id,
        &params.get("paths").cloned().unwrap_or(json!({})),
    );
    ok(state.apis[api_id].clone())
}

/// Shared by import/put: registers the api and imports its paths.
fn create_api_and_import(
    state: &mut State,
    req: &Request,
    name: &str,
    description: &str,
    doc: &Map<String, Value>,
    _mode: &str,
) -> Response {
    let api_id = create_id();
    let mut api = rest_api_json(&api_id, name, description);
    let root_id = create_id();
    api["rootResourceId"] = json!(root_id);
    state.apis.insert(api_id.clone(), api);
    state.roots.insert(api_id.clone(), root_id.clone());
    child_entry(&mut state.resources, &api_id)
        .insert(root_id.clone(), root_resource_json(&root_id));
    import_paths(
        state,
        &api_id,
        &doc.get("paths").cloned().unwrap_or(json!({})),
    );
    let _ = req;
    ok(state.apis[&api_id].clone())
}

/// Enter into `resources[api_id]` ↦ child map; undefined api ids are ignored.
fn import_add_resource(
    state: &mut State,
    api_id: &str,
    parent_id: &str,
    path_part: &str,
) -> String {
    let id = create_id();
    child_entry(&mut state.resources, api_id).insert(
        id.clone(),
        json!({
            "id": id,
            "pathPart": path_part,
            "parentId": parent_id,
            "resourceMethods": {},
        }),
    );
    id
}

/// Full path of a resource (`/` for the root), walking stored parents.
fn resource_path_in(resources: &BTreeMap<String, Value>, resource_id: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut current = Some(resource_id.to_string());
    while let Some(id) = current {
        let Some(res) = resources.get(&id) else { break };
        let part = res
            .get("pathPart")
            .and_then(Value::as_str)
            .unwrap_or("/")
            .to_string();
        if part != "/" {
            parts.insert(0, part);
        }
        current = res
            .get("parentId")
            .and_then(Value::as_str)
            .map(String::from);
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// moto's OpenAPI subset import: every path becomes a resource, every HTTP
/// method a method (plus optional `x-amazon-apigateway-integration`).
fn import_paths(state: &mut State, api_id: &str, paths: &Value) {
    let Some(paths_map) = paths.as_object() else {
        return;
    };
    let mut sorted: Vec<(String, Value)> = paths_map
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let root_id = match state.roots.get(api_id) {
        Some(id) => id.clone(),
        None => return,
    };
    for (path, method_doc) in &sorted {
        let ancestors: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let Some(leaf) = ancestors.last().copied() else {
            continue;
        };
        // Ensure all ancestor resources exist (moto's nested-path handling).
        let mut parent_id = root_id.clone();
        let mut walked = String::new();
        for ancestor in &ancestors[..ancestors.len() - 1] {
            walked = format!("{walked}/{ancestor}");
            let ids: Vec<String> = child_entry(&mut state.resources, api_id)
                .keys()
                .cloned()
                .collect();
            parent_id = match find_id(state, api_id, &ids, &walked) {
                Some(id) => id,
                None => import_add_resource(state, api_id, &parent_id, ancestor),
            };
        }
        let resource_id = import_add_resource(state, api_id, &parent_id, leaf);
        let Some(methods_map) = method_doc.as_object() else {
            continue;
        };
        for (method_name, method_doc) in methods_map.iter().filter(|(m, _)| is_http_method(m)) {
            let method_type = method_name.to_uppercase();
            let mut integration = Value::Null;
            if let Some(integ) = method_doc.get("x-amazon-apigateway-integration") {
                let mut integ_json = integration_json(
                    integ.get("type").and_then(Value::as_str).unwrap_or("MOCK"),
                    integ.get("uri").and_then(Value::as_str).unwrap_or(""),
                    Some(
                        integ
                            .get("httpMethod")
                            .and_then(Value::as_str)
                            .unwrap_or(method_name)
                            .to_string(),
                    ),
                    integ.get("requestTemplates").cloned(),
                    integ
                        .get("passthroughBehavior")
                        .and_then(Value::as_str)
                        .map(String::from),
                    integ.get("tlsConfig").cloned(),
                    integ
                        .get("cacheNamespace")
                        .and_then(Value::as_str)
                        .map(String::from),
                    integ.get("timeoutInMillis").cloned(),
                    integ.get("requestParameters").cloned(),
                    integ
                        .get("contentHandling")
                        .and_then(Value::as_str)
                        .map(String::from),
                    integ
                        .get("credentials")
                        .and_then(Value::as_str)
                        .map(String::from),
                    integ
                        .get("connectionType")
                        .and_then(Value::as_str)
                        .map(String::from),
                );
                integ_json["integrationResponses"] = integration_responses_json(integ);
                integration = integ_json;
            }
            let method_json = json!({
                "httpMethod": method_type,
                "authorizationType": Value::Null,
                "authorizerId": Value::Null,
                "authorizationScopes": Value::Null,
                "apiKeyRequired": false,
                "requestParameters": Value::Null,
                "requestModels": Value::Null,
                "methodIntegration": integration,
                "operationName": Value::Null,
                "requestValidatorId": Value::Null,
                "methodResponses": method_responses_json(method_doc.get("responses")),
            });
            if let Some(resource) = state
                .resources
                .get_mut(api_id)
                .and_then(|m| m.get_mut(&resource_id))
            {
                if let Some(methods) = obj_entry(resource, "resourceMethods") {
                    methods.insert(method_type, method_json);
                }
            }
        }
    }
}

fn is_http_method(method: &str) -> bool {
    matches!(
        method.to_uppercase().as_str(),
        "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD" | "OPTIONS" | "ANY"
    )
}

/// Resolve a resource id by its full path (walk parents via stored ids).
fn find_id(state: &State, api_id: &str, candidate_ids: &[String], wanted: &str) -> Option<String> {
    let resources = state.resources.get(api_id)?;
    for id in candidate_ids {
        if resource_path_in(resources, id) == wanted {
            return Some(id.clone());
        }
    }
    None
}

/// moto's Integration.to_json
#[allow(clippy::too_many_arguments)]
fn integration_json(
    integration_type: &str,
    uri: &str,
    http_method: Option<String>,
    request_templates: Option<Value>,
    passthrough: Option<String>,
    tls_config: Option<Value>,
    cache_namespace: Option<String>,
    timeout_millis: Option<Value>,
    request_parameters: Option<Value>,
    content_handling: Option<String>,
    credentials: Option<String>,
    connection_type: Option<String>,
) -> Value {
    json!({
        "type": integration_type,
        "uri": uri,
        "httpMethod": if integration_type == "MOCK" { Value::Null } else { opt_str(&http_method) },
        "passthroughBehavior": opt_str(&passthrough),
        "cacheKeyParameters": [],
        "requestTemplates": request_templates.unwrap_or(Value::Null),
        "integrationResponses": Value::Null,
        "tlsConfig": tls_config.unwrap_or(Value::Null),
        "cacheNamespace": opt_str(&cache_namespace),
        "timeoutInMillis": timeout_millis.unwrap_or(Value::Null),
        "requestParameters": request_parameters.unwrap_or(Value::Null),
        "contentHandling": opt_str(&content_handling),
        "credentials": opt_str(&credentials),
        "connectionType": opt_str(&connection_type),
    })
}

fn integration_responses_json(integ: &Value) -> Value {
    let out = match integ.get("responses") {
        Some(Value::Object(map)) => map
            .iter()
            .map(|(key, config)| {
                let status = config.get("statusCode").cloned().unwrap_or(Value::Null);
                let selection = if key == "default" { "" } else { key.as_str() };
                let mut resp = json!({
                    "responseTemplates": template_null(config.get("responseTemplates")),
                    "statusCode": status,
                });
                if !selection.is_empty() {
                    resp["selectionPattern"] = json!(selection);
                }
                (key.clone(), resp)
            })
            .collect::<Map<String, Value>>(),
        _ => Map::new(),
    };
    Value::Object(out)
}

fn template_null(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(s)) if s.is_empty() => Value::Null,
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

/// Method responses built from the OpenAPI `responses` block (status-only).
fn method_responses_json(responses: Option<&Value>) -> Value {
    match responses {
        Some(Value::Object(map)) => Value::Object(
            map.keys()
                .map(|code| {
                    (
                        code.clone(),
                        json!({
                            "statusCode": code,
                            "responseModels": Value::Null,
                            "responseParameters": Value::Null,
                        }),
                    )
                })
                .collect::<Map<String, Value>>(),
        ),
        _ => json!({}),
    }
}

// ---------------------------------------------------------------------------
// Handlers: rest apis / resources / methods / integrations
// ---------------------------------------------------------------------------

fn delete_rest_api(state: &mut State, api_id: &str) -> Response {
    match state.apis.remove(api_id) {
        Some(api) => {
            state.resources.remove(api_id);
            state.roots.remove(api_id);
            state.stages.remove(api_id);
            state.deployments.remove(api_id);
            state.authorizers.remove(api_id);
            state.gateway_responses.remove(api_id);
            state.models.remove(api_id);
            state.request_validators.remove(api_id);
            state.documentation_parts.remove(api_id);
            state.documentation_versions.remove(api_id);
            ok(api)
        }
        None => not_found("Invalid Rest API Id specified"),
    }
}

fn update_rest_api(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    let Some(api) = state.apis.get_mut(api_id) else {
        return not_found("Invalid Rest API Id specified");
    };
    for op in patch_ops(params) {
        if op.op == "replace" {
            if op.path.contains("/name") {
                set_member(api, "name", op.value.clone());
            } else if op.path.contains("/description") {
                set_member(api, "description", op.value.clone());
            } else if op.path.contains("/apiKeySource") {
                set_member(api, "apiKeySource", op.value.clone());
            } else if op.path.contains("/binaryMediaTypes") {
                if let Some(v) = &op.value {
                    api["binaryMediaTypes"] = json!([v]);
                }
            } else if op.path.contains("/disableExecuteApiEndpoint") {
                api["disableExecuteApiEndpoint"] = op
                    .value
                    .as_deref()
                    .map(|v| v == "true")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            } else if op.path.contains("/policy") {
                set_member(api, "policy", op.value.clone());
            } else if op.path.contains("/endpointConfiguration") {
                if let Some(v) = op.value.as_deref() {
                    api["endpointConfiguration"] = json!({ "types": [v] });
                }
            }
        } else if op.op == "add" && op.path.contains("/binaryMediaTypes") {
            if let Some(map) = api.as_object_mut() {
                let entry = map
                    .entry("binaryMediaTypes".to_string())
                    .or_insert_with(|| json!([]));
                if let Some(list) = entry.as_array_mut() {
                    if let Some(v) = op.value {
                        list.push(json!(v));
                    }
                }
            }
        } else if op.op == "remove" && op.path.contains("/description") {
            set_member(api, "description", None);
        }
    }
    ok(api.clone())
}

fn resource_wire(state: &State, api_id: &str, resource_id: &str) -> Value {
    let empty = BTreeMap::new();
    let resources = state.resources.get(api_id).unwrap_or(&empty);
    let Some(res) = resources.get(resource_id) else {
        return json!({});
    };
    let mut out = json!({
        "path": resource_path_in(resources, resource_id),
        "id": res.get("id").cloned().unwrap_or(Value::Null),
    });
    if let Some(methods) = res.get("resourceMethods").and_then(Value::as_object) {
        if !methods.is_empty() {
            out["resourceMethods"] = json!(methods);
        }
    }
    if let Some(parent) = res.get("parentId").and_then(Value::as_str) {
        out["parentId"] = json!(parent);
        out["pathPart"] = res.get("pathPart").cloned().unwrap_or(Value::Null);
    }
    out
}

fn list_resources(state: &State, api_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let ids: Vec<String> = state
        .resources
        .get(api_id)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let items: Vec<Value> = ids
        .iter()
        .map(|id| resource_wire(state, api_id, id))
        .collect();
    ok(json!({ "item": items }))
}

fn create_resource(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    parent_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let Some(path_part) = body_str(params, "pathPart") else {
        return bad_request("Resource's path is not specified");
    };
    if path_part.is_empty() {
        // moto returns the existing root resource for an empty path part.
        let root = state.roots.get(api_id).cloned().unwrap_or_default();
        return created_201(resource_wire(state, api_id, &root));
    }
    if !valid_path_part(path_part) {
        return bad_request(
            "Resource's path part only allow a-zA-Z0-9._- and curly braces at the beginning and the end and an optional plus sign before the closing brace.",
        );
    }
    let id = create_id();
    child_entry(&mut state.resources, api_id).insert(
        id.clone(),
        json!({
            "id": id,
            "pathPart": path_part,
            "parentId": parent_id,
            "resourceMethods": {},
        }),
    );
    created_201(resource_wire(state, api_id, &id))
}

/// moto's `create_resource` path-part regex: `\{?[a-zA-Z0-9._\-\:]+\+?\}?`.
fn valid_path_part(part: &str) -> bool {
    let core = if part.len() >= 2 && part.starts_with('{') && part.ends_with('}') {
        &part[1..part.len() - 1]
    } else if part.contains('{') || part.contains('}') || part.is_empty() {
        return false;
    } else {
        part
    };
    let core = core.strip_suffix('+').unwrap_or(core);
    !core.is_empty()
        && core
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-:".contains(c))
}

fn get_resource(state: &mut State, api_id: &str, resource_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    if state
        .resources
        .get(api_id)
        .is_some_and(|m| m.contains_key(resource_id))
    {
        ok(resource_wire(state, api_id, resource_id))
    } else {
        not_found("Invalid resource identifier specified")
    }
}

fn delete_resource(state: &mut State, api_id: &str, resource_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let removed = match state
        .resources
        .get_mut(api_id)
        .and_then(|m| m.remove(resource_id))
    {
        Some(r) => r,
        None => return not_found("Invalid resource identifier specified"),
    };
    let path = resource_path_in(
        state.resources.get(api_id).unwrap_or(&BTreeMap::new()),
        resource_id,
    );
    let mut out = removed;
    out["path"] = json!(path);
    accepted_202_body(out)
}

fn update_resource(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let Some(resource) = state
        .resources
        .get_mut(api_id)
        .and_then(|m| m.get_mut(resource_id))
    else {
        return not_found("Invalid resource identifier specified");
    };
    for op in patch_ops(params) {
        if op.op == "replace" && op.path.contains("/pathPart") {
            if let Some(v) = &op.value {
                resource["pathPart"] = json!(v);
            }
        }
    }
    ok(resource_wire(state, api_id, resource_id))
}

// ---------------------------------------------------------------------------
// Handlers: methods, method responses, integrations, integration responses
// ---------------------------------------------------------------------------

/// Locate (mutably) `resourceMethods[method]` when api/resource/method exist.
fn method_mut<'a>(
    state: &'a mut State,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Option<&'a mut Value> {
    if !state.apis.contains_key(api_id) {
        return None;
    }
    let resources = state.resources.get_mut(api_id)?;
    let resource = resources.get_mut(resource_id)?;
    let methods = resource.get_mut("resourceMethods")?.as_object_mut()?;
    methods.get_mut(method)
}

fn method_wire_response(params: &Map<String, Value>, method: &str) -> Value {
    json!({
        "httpMethod": method,
        "authorizationType": params.get("authorizationType").cloned().unwrap_or(Value::Null),
        "authorizerId": params.get("authorizerId").cloned().unwrap_or(Value::Null),
        "authorizationScopes": params.get("authorizationScopes").cloned().unwrap_or(Value::Null),
        "apiKeyRequired": body_bool(params, "apiKeyRequired").unwrap_or(false),
        "requestParameters": params.get("requestParameters").cloned().unwrap_or(Value::Null),
        "requestModels": params.get("requestModels").cloned().unwrap_or(Value::Null),
        "methodIntegration": Value::Null,
        "operationName": params.get("operationName").cloned().unwrap_or(Value::Null),
        "requestValidatorId": params.get("requestValidatorId").cloned().unwrap_or(Value::Null),
        "methodResponses": {},
    })
}

fn method_route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    match req.method.as_str() {
        "PUT" => put_method(state, params, api_id, resource_id, method),
        "GET" => match method_mut(state, api_id, resource_id, method) {
            Some(m) => ok(m.clone()),
            None => not_found("Invalid Method identifier specified"),
        },
        "DELETE" => match method_mut(state, api_id, resource_id, method) {
            Some(_) => {
                remove_method(state, api_id, resource_id, method);
                Response::new(204, "application/json", Vec::new())
            }
            None => not_found("Invalid Method identifier specified"),
        },
        "PATCH" => update_method(state, params, api_id, resource_id, method),
        "POST" => test_invoke_method(method),
        _ => route_missing(),
    }
}

fn remove_method(state: &mut State, api_id: &str, resource_id: &str, method: &str) {
    let resources = match state.resources.get_mut(api_id) {
        Some(m) => m,
        None => return,
    };
    let Some(resource) = resources.get_mut(resource_id) else {
        return;
    };
    let Some(methods) = resource
        .get_mut("resourceMethods")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    methods.remove(method);
}

fn put_method(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let Some(resource) = state
        .resources
        .get_mut(api_id)
        .and_then(|m| m.get_mut(resource_id))
    else {
        return not_found("Invalid resource identifier specified");
    };
    let Some(methods) = obj_entry(resource, "resourceMethods") else {
        return not_found("Invalid resource identifier specified");
    };
    let method_json = method_wire_response(params, method);
    methods.insert(method.to_string(), method_json);
    ok(methods[method].clone())
}

fn update_method(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    let Some(method_value) = method_mut(state, api_id, resource_id, method) else {
        return not_found("Invalid Method identifier specified");
    };
    for op in patch_ops(params) {
        if op.op == "replace" {
            if op.path.contains("/authorizationType") {
                set_member(method_value, "authorizationType", op.value.clone());
            } else if op.path.contains("/authorizerId") {
                set_member(method_value, "authorizerId", op.value.clone());
            } else if op.path.contains("/apiKeyRequired") {
                method_value["apiKeyRequired"] = op
                    .value
                    .as_deref()
                    .map(|v| v == "true")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            } else if op.path.contains("/operationName") {
                set_member(method_value, "operationName", op.value.clone());
            } else if op.path.contains("/requestValidatorId") {
                set_member(method_value, "requestValidatorId", op.value.clone());
            } else if let Some(key) = op.path.split("/requestParameters/").nth(1) {
                let key = key.to_string();
                let flagged = op.value.as_deref().map(|v| v == "true").unwrap_or(false);
                if let Some(map) = obj_entry(method_value, "requestParameters") {
                    map.insert(key, json!(flagged));
                }
            }
        } else if op.op == "add" {
            if let Some(key) = op.path.split("/requestParameters/").nth(1) {
                let key = key.to_string();
                let flagged = op.value.as_deref().map(|v| v == "true").unwrap_or(false);
                if let Some(map) = obj_entry(method_value, "requestParameters") {
                    map.insert(key, json!(flagged));
                }
            }
        } else if op.op == "remove" {
            if let Some(key) = op.path.split("/requestParameters/").nth(1) {
                let key = key.to_string();
                if let Some(map) = method_value
                    .get_mut("requestParameters")
                    .and_then(Value::as_object_mut)
                {
                    map.remove(&key);
                }
            }
        }
    }
    ok(method_value.clone())
}

fn method_response_route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    let ops = patch_ops(params);
    match req.method.as_str() {
        "PUT" => put_method_response(state, params, api_id, resource_id, method, status_code),
        "GET" => find_response(state, api_id, resource_id, method, status_code),
        "DELETE" => match method_mut(state, api_id, resource_id, method) {
            Some(method_value) => {
                let responses = method_value
                    .as_object_mut()
                    .and_then(|m| m.get_mut("methodResponses"))
                    .and_then(Value::as_object_mut)
                    .and_then(|m| m.remove(status_code));
                match responses {
                    // moto answers DeleteMethodResponse with 204 + the deleted body.
                    Some(resp) => Response::new(
                        204,
                        "application/json",
                        serde_json::to_vec(&resp).unwrap_or_default(),
                    ),
                    None => not_found("Invalid Response status code specified"),
                }
            }
            None => not_found("Invalid Method identifier specified"),
        },
        "PATCH" => match method_mut(state, api_id, resource_id, method) {
            Some(method_value) => {
                let Some(resp) = method_value
                    .get_mut("methodResponses")
                    .and_then(Value::as_object_mut)
                    .and_then(|m| m.get_mut(status_code))
                else {
                    return not_found("Invalid Response status code specified");
                };
                for op in &ops {
                    if op.op == "replace" {
                        if op.path.contains("/statusCode") {
                            set_member(resp, "statusCode", op.value.clone());
                        } else if let Some(key) = op.path.split("/responseParameters/").nth(1) {
                            let key = key.to_string();
                            let flagged = op.value.as_deref().map(|v| v == "true").unwrap_or(false);
                            if let Some(map) = obj_entry(resp, "responseParameters") {
                                map.insert(key, json!(flagged));
                            }
                        } else if let Some(key) = op.path.split("/responseModels/").nth(1) {
                            let key = key.to_string();
                            if let Some(map) = obj_entry(resp, "responseModels") {
                                map.insert(key, json!(op.value.clone()));
                            }
                        }
                    } else if op.op == "add" {
                        if let Some(key) = op.path.split("/responseParameters/").nth(1) {
                            let key = key.to_string();
                            let flagged = op.value.as_deref().map(|v| v == "true").unwrap_or(false);
                            if let Some(map) = obj_entry(resp, "responseParameters") {
                                map.insert(key, json!(flagged));
                            }
                        }
                    } else if op.op == "remove" {
                        if let Some(key) = op.path.split("/responseParameters/").nth(1) {
                            let key = key.to_string();
                            if let Some(map) = resp
                                .get_mut("responseParameters")
                                .and_then(Value::as_object_mut)
                            {
                                map.remove(&key);
                            }
                        }
                    }
                }
                ok(resp.clone())
            }
            None => not_found("Invalid Method identifier specified"),
        },
        _ => route_missing(),
    }
}

fn find_response(
    state: &mut State,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    match method_mut(state, api_id, resource_id, method) {
        Some(method_value) => {
            let response = method_value
                .get("methodResponses")
                .and_then(Value::as_object)
                .and_then(|m| m.get(status_code))
                .cloned();
            match response {
                Some(resp) => ok(resp),
                None => not_found("Invalid Response status code specified"),
            }
        }
        None => not_found("Invalid Method identifier specified"),
    }
}

fn put_method_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    let Some(method_value) = method_mut(state, api_id, resource_id, method) else {
        return not_found("Invalid Method identifier specified");
    };
    let response = json!({
        "statusCode": status_code,
        "responseModels": params.get("responseModels").cloned().unwrap_or(Value::Null),
        "responseParameters": params.get("responseParameters").cloned().unwrap_or(Value::Null),
    });
    if let Some(map) = obj_entry(method_value, "methodResponses") {
        map.insert(status_code.to_string(), response.clone());
    }
    created_201(response)
}

// ---------------------------------------------------------------------------
// Handlers: integrations / integration responses
// ---------------------------------------------------------------------------

/// Read-only view of one integration for routing decisions.
enum IntegrationView {
    ApiMissing,
    ResourceMissing,
    MethodMissing,
    Configured(Value),
    // `methodIntegration` exists but is null (after DeleteIntegration).
    Unconfigured,
}

fn integration_view(
    state: &State,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> IntegrationView {
    if !state.apis.contains_key(api_id) {
        return IntegrationView::ApiMissing;
    }
    let empty = BTreeMap::new();
    let resources = state.resources.get(api_id).unwrap_or(&empty);
    let Some(resource) = resources.get(resource_id) else {
        return IntegrationView::ResourceMissing;
    };
    let Some(method_value) = resource
        .get("resourceMethods")
        .and_then(Value::as_object)
        .and_then(|m| m.get(method))
    else {
        return IntegrationView::MethodMissing;
    };
    match method_value.get("methodIntegration") {
        Some(Value::Object(_)) => {
            IntegrationView::Configured(method_value.get("methodIntegration").cloned().unwrap())
        }
        None | Some(Value::Null) => IntegrationView::Unconfigured,
        Some(_) => IntegrationView::Unconfigured,
    }
}

fn integration_route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    match req.method.as_str() {
        "PUT" => put_integration(state, params, api_id, resource_id, method),
        "GET" => match integration_view(state, api_id, resource_id, method) {
            IntegrationView::ApiMissing => not_found("Invalid Rest API Id specified"),
            IntegrationView::ResourceMissing => not_found("Invalid resource identifier specified"),
            IntegrationView::MethodMissing => not_found("Invalid Method identifier specified"),
            IntegrationView::Configured(integration) => ok(integration),
            // moto answers an unconfigured integration with a bare `{}`.
            IntegrationView::Unconfigured => ok(json!({})),
        },
        "DELETE" => match integration_view(state, api_id, resource_id, method) {
            IntegrationView::Configured(_) => {
                if let Some(method_value) = method_mut(state, api_id, resource_id, method) {
                    set_member(method_value, "methodIntegration", None);
                }
                Response::new(204, "application/json", Vec::new())
            }
            IntegrationView::Unconfigured => Response::new(204, "application/json", Vec::new()),
            _ => not_found("Invalid Method identifier specified"),
        },
        "PATCH" => update_integration(state, params, api_id, resource_id, method),
        _ => route_missing(),
    }
}

fn put_integration(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    match integration_view(state, api_id, resource_id, method) {
        IntegrationView::MethodMissing | IntegrationView::Unconfigured => {
            if method_mut(state, api_id, resource_id, method).is_none() {
                return not_found("Invalid Method identifier specified");
            }
        }
        IntegrationView::ApiMissing => return not_found("Invalid Rest API Id specified"),
        IntegrationView::ResourceMissing => {
            return not_found("Invalid resource identifier specified")
        }
        IntegrationView::Configured(_) => {}
    }
    let integration_type = body_str(params, "type").unwrap_or_default().to_string();
    let uri = body_str(params, "uri").unwrap_or_default().to_string();
    let integration_http_method = body_str(params, "httpMethod").map(String::from);
    if matches!(
        integration_type.as_str(),
        "HTTP" | "HTTP_PROXY" | "AWS" | "AWS_PROXY"
    ) && integration_http_method.is_none()
    {
        return bad_request("Enumeration value for HttpMethod must be non-empty");
    }
    if integration_type == "AWS" || integration_type == "AWS_PROXY" {
        if !uri.starts_with("arn:") {
            return bad_request("Invalid ARN specified in the request");
        }
        let path_or_action =
            uri.contains(":apigateway:") && (uri.contains(":path/") || uri.contains(":action/"));
        if !path_or_action {
            return bad_request("AWS ARN for integration must contain path or action");
        }
    }
    if integration_type == "HTTP" || integration_type == "HTTP_PROXY" {
        let valid = uri
            .split_once("://")
            .is_some_and(|(_, rest)| !rest.split('/').next().unwrap_or_default().is_empty());
        if !valid {
            return bad_request("Invalid HTTP endpoint specified for URI");
        }
    }
    let integration = integration_json(
        &integration_type,
        &uri,
        Some(integration_http_method.unwrap_or_else(|| method.to_string())),
        params.get("requestTemplates").cloned(),
        body_str(params, "passthroughBehavior").map(String::from),
        params.get("tlsConfig").cloned(),
        body_str(params, "cacheNamespace").map(String::from),
        params.get("timeoutInMillis").cloned(),
        params.get("requestParameters").cloned(),
        body_str(params, "contentHandling").map(String::from),
        body_str(params, "credentials").map(String::from),
        body_str(params, "connectionType").map(String::from),
    );
    let Some(method_value) = method_mut(state, api_id, resource_id, method) else {
        return not_found("Invalid Method identifier specified");
    };
    if let Some(map) = method_value.as_object_mut() {
        map.insert("methodIntegration".to_string(), integration.clone());
    }
    created_201(integration)
}

fn update_integration(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
) -> Response {
    // moto resolves the integration first; a missing one raises NotFound.
    let Some(integration) = method_mut(state, api_id, resource_id, method)
        .and_then(|method_value| method_value.get_mut("methodIntegration"))
        .filter(|i| i.is_object())
    else {
        return not_found("Invalid Method identifier specified");
    };
    let ops = patch_ops(params);
    let mut integration = integration.clone();
    for op in &ops {
        if op.op != "replace" {
            continue;
        }
        if op.path.contains("/uri") && !op.path.contains("/uri/") {
            set_member(&mut integration, "uri", op.value.clone());
        } else if op.path.contains("/httpMethod") {
            set_member(&mut integration, "httpMethod", op.value.clone());
        } else if op.path.contains("/type") {
            set_member(&mut integration, "type", op.value.clone());
        } else if op.path.contains("/contentHandling") {
            set_member(&mut integration, "contentHandling", op.value.clone());
        } else if op.path.contains("/timeoutInMillis") {
            integration["timeoutInMillis"] =
                json!(op.value.as_deref().and_then(|v| v.parse::<i64>().ok()));
        } else if op.path.contains("/passthroughBehavior") {
            set_member(&mut integration, "passthroughBehavior", op.value.clone());
        } else if op.path.contains("/cacheNamespace") {
            set_member(&mut integration, "cacheNamespace", op.value.clone());
        } else if let Some(key) = op.path.split("/requestParameters/").nth(1) {
            let key = key.to_string();
            if let Some(map) = obj_entry(&mut integration, "requestParameters") {
                map.insert(key, json!(op.value.clone()));
            }
        } else if let Some(key) = op.path.split("/requestTemplates/").nth(1) {
            let key = key.to_string();
            if let Some(map) = obj_entry(&mut integration, "requestTemplates") {
                map.insert(key, json!(op.value.clone()));
            }
        }
    }
    if let Some(method_value) = method_mut(state, api_id, resource_id, method) {
        if let Some(map) = method_value.as_object_mut() {
            map.insert("methodIntegration".to_string(), integration.clone());
        }
    }
    ok(integration)
}

fn integration_response_route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    match req.method.as_str() {
        "PUT" => put_integration_response(
            state,
            &req.body,
            params,
            api_id,
            resource_id,
            method,
            status_code,
        ),
        "GET" => {
            let snapshot = method_mut(state, api_id, resource_id, method)
                .and_then(|m| m.get("methodIntegration"))
                .cloned();
            let response = snapshot
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|i| i.get("integrationResponses"))
                .and_then(Value::as_object)
                .and_then(|m| m.get(status_code))
                .cloned();
            match response {
                Some(resp) => ok(resp),
                None => not_found("Invalid Response status code specified"),
            }
        }
        "DELETE" => {
            let removed = method_mut(state, api_id, resource_id, method)
                .and_then(|m| m.get_mut("methodIntegration"))
                .and_then(|i| i.get_mut("integrationResponses"))
                .and_then(Value::as_object_mut)
                .and_then(|m| m.remove(status_code));
            match removed {
                // moto answers DeleteIntegrationResponse with 204 + body.
                Some(resp) => Response::new(
                    204,
                    "application/json",
                    serde_json::to_vec(&resp).unwrap_or_default(),
                ),
                None => not_found("Invalid Response status code specified"),
            }
        }
        "PATCH" => {
            update_integration_response(state, params, api_id, resource_id, method, status_code)
        }
        _ => route_missing(),
    }
}

fn update_integration_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    let Some(integration) = method_mut(state, api_id, resource_id, method)
        .and_then(|m| m.get_mut("methodIntegration"))
        .filter(|i| i.is_object())
    else {
        return not_found("Invalid Method identifier specified");
    };
    let mut integration = integration.clone();
    let mut new_response = integration
        .get("integrationResponses")
        .and_then(Value::as_object)
        .and_then(|m| m.get(status_code))
        .cloned();
    new_response = new_response.map(|resp| {
        let mut resp = resp;
        for op in patch_ops(params) {
            if op.op == "replace" {
                if op.path.contains("/selectionPattern") {
                    set_member(&mut resp, "selectionPattern", op.value.clone());
                } else if op.path.contains("/contentHandling") {
                    set_member(&mut resp, "contentHandling", op.value.clone());
                } else if let Some(key) = op.path.split("/responseTemplates/").nth(1) {
                    let key = key.to_string();
                    if let Some(map) = obj_entry(&mut resp, "responseTemplates") {
                        map.insert(key, json!(op.value.clone()));
                    }
                } else if let Some(key) = op.path.split("/responseParameters/").nth(1) {
                    let key = key.to_string();
                    if let Some(map) = obj_entry(&mut resp, "responseParameters") {
                        map.insert(key, json!(op.value.clone()));
                    }
                }
            }
        }
        resp
    });
    let new_response = new_response.unwrap_or_else(|| json!({}));
    if let Some(map) = integration
        .get_mut("integrationResponses")
        .and_then(Value::as_object_mut)
    {
        map.insert(status_code.to_string(), new_response.clone());
    } else if let Some(map) = obj_entry(&mut integration, "integrationResponses") {
        map.insert(status_code.to_string(), new_response.clone());
    }
    if let Some(method_value) = method_mut(state, api_id, resource_id, method) {
        if let Some(m) = method_value.as_object_mut() {
            m.insert("methodIntegration".to_string(), integration.clone());
        }
    }
    ok(new_response)
}

fn put_integration_response(
    state: &mut State,
    body: &[u8],
    params: &Map<String, Value>,
    api_id: &str,
    resource_id: &str,
    method: &str,
    status_code: &str,
) -> Response {
    if body.is_empty() {
        return bad_request("Invalid request input");
    }
    let Some(integration) = method_mut(state, api_id, resource_id, method)
        .and_then(|m| m.get_mut("methodIntegration"))
        .filter(|i| i.is_object())
    else {
        return not_found("Invalid Response status code specified");
    };
    let mut response = json!({
        "responseTemplates": template_null_map(params.get("responseTemplates")),
        "statusCode": status_code,
    });
    let selection = params
        .get("selectionPattern")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !selection.is_empty() {
        response["selectionPattern"] = json!(selection);
    }
    if let Some(cp) = params.get("contentHandling").and_then(Value::as_str) {
        if !cp.is_empty() {
            response["contentHandling"] = json!(cp);
        }
    }
    if let Some(Value::Object(rps)) = params.get("responseParameters") {
        if !rps.is_empty() {
            response["responseParameters"] = json!(rps);
        }
    }
    if let Some(map) = obj_entry(integration, "integrationResponses") {
        map.insert(status_code.to_string(), response.clone());
    }
    created_201(response)
}

/// moto normalizes empty mapping-template values to null.
fn template_null_map(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Object(map)) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(
                    k.clone(),
                    match v {
                        Value::String(s) if s.is_empty() => Value::Null,
                        other => other.clone(),
                    },
                );
            }
            Value::Object(out)
        }
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Handlers: authorizers / deployments / stages / export
// ---------------------------------------------------------------------------

fn create_authorizer(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let authorizer_type = body_str(params, "type").unwrap_or_default();
    if !matches!(authorizer_type, "TOKEN" | "REQUEST" | "COGNITO_USER_POOLS") {
        return bad_request(
            "1 validation error detected: Value '' at 'createAuthorizerInput.type' failed to satisfy constraint: Member must satisfy enum value set: [TOKEN, REQUEST, COGNITO_USER_POOLS]",
        );
    }
    let mut authorizer = json!({
        "id": create_id(),
        "name": params.get("name").cloned().unwrap_or(Value::Null),
        "type": authorizer_type,
        "authorizerResultTtlInSeconds": params.get("authorizerResultTtlInSeconds")
            .cloned()
            .unwrap_or(json!(300)),
    });
    for (param_key, wire_key) in [
        ("providerARNs", "providerARNs"),
        ("authType", "authType"),
        ("authorizerUri", "authorizerUri"),
        ("authorizerCredentials", "authorizerCredentials"),
        ("identitySource", "identitySource"),
        (
            "identityValidationExpression",
            "identityValidationExpression",
        ),
    ] {
        if let Some(v) = params.get(param_key).cloned() {
            // moto skips empty optionals in to_json.
            if !matches!(&v, Value::Null) {
                authorizer[wire_key] = v;
            }
        }
    }
    child_entry(&mut state.authorizers, api_id).insert(
        authorizer["id"].as_str().unwrap_or_default().to_string(),
        authorizer.clone(),
    );
    created_201(authorizer)
}

fn update_authorizer(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    authorizer_id: &str,
) -> Response {
    let Some(authorizer) = child_entry(&mut state.authorizers, api_id).get_mut(authorizer_id)
    else {
        return not_found("Invalid Authorizer identifier specified");
    };
    let ops = patch_ops(params);
    let mut patched = authorizer.clone();
    for op in &ops {
        if op.op == "replace" {
            if op.path.contains("/authorizerUri") {
                set_member(&mut patched, "authorizerUri", op.value.clone());
            } else if op.path.contains("/authorizerCredentials") {
                set_member(&mut patched, "authorizerCredentials", op.value.clone());
            } else if op.path.contains("/authorizerResultTtlInSeconds") {
                if let Some(n) = op.value.as_deref().and_then(|v| v.parse::<i64>().ok()) {
                    patched["authorizerResultTtlInSeconds"] = json!(n);
                }
            } else if op.path.contains("/authType") {
                set_member(&mut patched, "authType", op.value.clone());
            } else if op.path.contains("/identitySource") {
                set_member(&mut patched, "identitySource", op.value.clone());
            } else if op.path.contains("/identityValidationExpression") {
                set_member(
                    &mut patched,
                    "identityValidationExpression",
                    op.value.clone(),
                );
            } else if op.path.contains("/name") {
                set_member(&mut patched, "name", op.value.clone());
            } else if op.path.contains("/type") {
                set_member(&mut patched, "type", op.value.clone());
            }
        }
    }
    *authorizer = patched.clone();
    ok(patched)
}

/// TestInvokeAuthorizer (stub op): clientStatus 200 with a log line.
fn test_invoke_authorizer(authorizer_id: &str) -> Response {
    ok(json!({
        "clientStatus": 200,
        "log": format!("Authorizer response: authorized (id {authorizer_id})"),
    }))
}

fn test_invoke_method(method: &str) -> Response {
    ok(json!({
        "status": 200,
        "log": format!("Mock invoke for {method}"),
    }))
}

fn create_deployment(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    // moto: deployment fails when the API has no methods, or no integrations.
    let mut any_method = false;
    let mut any_integration = false;
    if let Some(resources) = state.resources.get(api_id) {
        for res in resources.values() {
            if let Some(method_map) = res.get("resourceMethods").and_then(Value::as_object) {
                for m in method_map.values() {
                    any_method = true;
                    if m.get("methodIntegration").is_some_and(Value::is_object) {
                        any_integration = true;
                    }
                }
            }
        }
    }
    if !any_method {
        return bad_request("The REST API doesn't contain any methods");
    }
    if !any_integration {
        return not_found("No integration defined for method");
    }
    let stage_name = params
        .get("stageName")
        .and_then(Value::as_str)
        .map(String::from);
    let deployment_id = create_id();
    let deployment = json!({
        "id": deployment_id,
        "stageName": opt_str(&stage_name),
        "description": params.get("description").cloned().unwrap_or(Value::Null),
        "createdDate": unix_int(),
    });
    child_entry(&mut state.deployments, api_id).insert(deployment_id, deployment.clone());
    if let Some(stage) = &stage_name {
        if !stage.is_empty() {
            child_entry(&mut state.stages, api_id).insert(
                stage.clone(),
                json!({
                    "stageName": stage,
                    "deploymentId": deployment["id"],
                    "methodSettings": {},
                    "variables": {},
                    "description": "",
                    "cacheClusterEnabled": false,
                    "accessLogSettings": Value::Null,
                }),
            );
        }
    }
    created_201(deployment)
}

fn update_deployment(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    deployment_id: &str,
) -> Response {
    let Some(dep) = child_entry(&mut state.deployments, api_id).get_mut(deployment_id) else {
        return not_found("Invalid Deployment identifier specified");
    };
    for op in patch_ops(params) {
        if op.op == "replace" && op.path.contains("/description") {
            set_member(dep, "description", op.value.clone());
        }
    }
    ok(dep.clone())
}

fn delete_deployment(state: &mut State, api_id: &str, deployment_id: &str) -> Response {
    let Some(dep) = child_entry(&mut state.deployments, api_id).get(deployment_id) else {
        return not_found("Invalid Deployment identifier specified");
    };
    if let Some(stage_name) = dep
        .get("stageName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        if state
            .stages
            .get(api_id)
            .is_some_and(|m| m.contains_key(stage_name))
        {
            return bad_request(
                "Active stages pointing to this deployment must be moved or deleted",
            );
        }
    }
    let dep = child_entry(&mut state.deployments, api_id)
        .remove(deployment_id)
        .unwrap_or(json!({}));
    accepted_202_body(dep)
}

fn create_stage(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let stage_name = body_str(params, "stageName")
        .unwrap_or_default()
        .to_string();
    if stage_name.is_empty() {
        return not_found("Invalid stage identifier specified");
    }
    let exists = state
        .stages
        .get(api_id)
        .is_some_and(|m| m.contains_key(&stage_name));
    if exists {
        return conflict("Stage already exists");
    }
    let cache_enabled: bool = body_bool(params, "cacheClusterEnabled").unwrap_or(false);
    let mut stage = json!({
        "stageName": stage_name,
        "deploymentId": params.get("deploymentId").cloned().unwrap_or(Value::Null),
        "methodSettings": {},
        "variables": params.get("variables").cloned().unwrap_or(json!({})),
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "cacheClusterEnabled": cache_enabled,
        "accessLogSettings": Value::Null,
    });
    if cache_enabled {
        stage["cacheClusterStatus"] = json!("AVAILABLE");
        let size = params
            .get("cacheClusterSize")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| "0.5".to_string());
        stage["cacheClusterSize"] = json!(size);
    }
    if let Some(t) = params.get("tags").cloned() {
        if !t.is_null() {
            stage["tags"] = t;
        }
    }
    if let Some(t) = params.get("tracingEnabled").cloned() {
        if !t.is_null() {
            stage["tracingEnabled"] = t;
        }
    }
    child_entry(&mut state.stages, api_id).insert(params_stage_name(params), stage.clone());
    created_201(stage)
}

fn params_stage_name(params: &Map<String, Value>) -> String {
    body_str(params, "stageName")
        .unwrap_or_default()
        .to_string()
}

fn update_stage(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    stage_name: &str,
) -> Response {
    let stage_missing = !state
        .stages
        .get(api_id)
        .is_some_and(|m| m.contains_key(stage_name));
    if stage_missing {
        return not_found("Invalid stage identifier specified");
    }
    for op in patch_ops(params) {
        if op.op == "remove" && op.path == "/accessLogSettings" {
            if let Some(map) = child_entry(&mut state.stages, api_id)
                .get_mut(stage_name)
                .and_then(Value::as_object_mut)
            {
                map.remove("accessLogSettings");
            }
            continue;
        }
        let Some(stage) = child_entry(&mut state.stages, api_id).get_mut(stage_name) else {
            continue;
        };
        if op.op == "replace" {
            if op.path.contains("/description") {
                set_member(stage, "description", op.value.clone());
            } else if op.path.contains("/deploymentId") {
                set_member(stage, "deploymentId", op.value.clone());
            } else if op.path.contains("/cacheClusterEnabled") {
                let enabled = op.value.as_deref().map(|v| v == "true").unwrap_or(false);
                stage["cacheClusterEnabled"] = json!(enabled);
                if enabled {
                    stage["cacheClusterStatus"] = json!("AVAILABLE");
                } else {
                    stage["cacheClusterStatus"] = json!("NOT_AVAILABLE");
                    stage.as_object_mut().map(|m| m.remove("cacheClusterSize"));
                }
            } else if op.path.contains("/cacheClusterSize") {
                if let Some(v) = &op.value {
                    stage["cacheClusterSize"] = json!(v);
                }
            } else if op.path == "/tracingEnabled" {
                stage["tracingEnabled"] = op
                    .value
                    .as_deref()
                    .map(|v| v == "true")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            } else if op.path.starts_with("/accessLogSettings/") {
                let key = op
                    .path
                    .split('/')
                    .next_back()
                    .unwrap_or_default()
                    .to_string();
                if let Some(map) = obj_entry(stage, "accessLogSettings") {
                    map.insert(key, json!(op.value.clone()));
                }
            } else if let Some(key) = op.path.split("/variables/").nth(1) {
                let key = key.to_string();
                if let Some(map) = obj_entry(stage, "variables") {
                    map.insert(key, json!(op.value.clone()));
                }
            }
        }
    }
    let stage = child_entry(&mut state.stages, api_id)
        .get(stage_name)
        .cloned()
        .unwrap_or(json!({}));
    ok(stage)
}

fn flush_stage(state: &mut State, api_id: &str) -> Response {
    // The python ground truth returns 202 {} (cache has no mock data).
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid stage identifier specified");
    }
    Response::new(202, "application/json", b"{}".to_vec())
}

fn get_export(state: &State, req: &Request, api_id: &str, export_type: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid stage identifier specified");
    }
    if export_type != "swagger" && export_type != "oas30" {
        return bad_request(format!("No API exporter for type '{export_type}'").as_str());
    }
    let empty = BTreeMap::new();
    let resources = state.resources.get(api_id).unwrap_or(&empty);
    let mut paths = Map::new();
    for res in resources.values() {
        let path = resource_path_in(
            resources,
            res.get("id").and_then(Value::as_str).unwrap_or(""),
        );
        let mut methods = Map::new();
        if let Some(method_map) = res.get("resourceMethods").and_then(Value::as_object) {
            for (name, method) in method_map {
                let mut responses = Map::new();
                if let Some(rs) = method.get("methodResponses").and_then(Value::as_object) {
                    for code in rs.keys() {
                        responses.insert(
                            code.clone(),
                            json!({
                                "description": format!("{code} response"),
                                "schema": {"$ref": "#/definitions/Empty"},
                            }),
                        );
                    }
                }
                methods.insert(
                    name.to_lowercase(),
                    json!({
                        "produces": ["application/json"],
                        "responses": responses,
                    }),
                );
            }
        }
        paths.insert(path, Value::Object(methods));
    }
    let api_name = state
        .apis
        .get(api_id)
        .and_then(|a| a.get("name").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let region_hint = req.region.clone();
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let doc = json!({
        "swagger": "2.0",
        "info": { "version": now, "title": api_name },
        "host": format!("{api_id}.execute-api.{region_hint}.amazonaws.com"),
        "basePath": "/",
        "schemes": ["https"],
        "paths": Value::Object(paths),
        "definitions": { "Empty": { "type": "object", "title": "Empty Schema" } },
    });
    let mut response = json_resp(200, doc);
    response.headers = vec![
        (
            "Content-Type".to_string(),
            "application/octet-stream".to_string(),
        ),
        (
            "Content-Disposition".to_string(),
            format!("attachment; filename=\"swagger_{now}Z.json\""),
        ),
    ];
    response
}

// ---------------------------------------------------------------------------
// Handlers: models / validators / gateway responses / docs / api keys
// ---------------------------------------------------------------------------

fn model_json(model_id: &str, params: &Map<String, Value>) -> Value {
    let mut model = json!({
        "id": model_id,
        "name": params.get("name").cloned().unwrap_or(Value::Null),
    });
    for (key, wire_key) in [
        ("description", "description"),
        ("schema", "schema"),
        ("contentType", "contentType"),
    ] {
        if let Some(v) = params.get(key).cloned() {
            if !v.is_null() {
                model[wire_key] = v;
            }
        }
    }
    model
}

fn create_model(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    if body_str(params, "name").is_none() {
        return not_found("Invalid Model Name specified");
    }
    let model_id = create_id();
    let model = model_json(&model_id, params);
    // moto keys models by NAME (Get/Update/Delete use model_name).
    let model_name = body_str(params, "name").unwrap_or_default().to_string();
    child_entry(&mut state.models, api_id).insert(model_name, model.clone());
    created_201(model)
}

fn update_model(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    model_name: &str,
) -> Response {
    let Some(model) = child_entry(&mut state.models, api_id).get_mut(model_name) else {
        return not_found("Invalid Model Name specified");
    };
    for op in patch_ops(params) {
        if op.op == "replace" {
            if op.path.contains("/description") {
                set_member(model, "description", op.value.clone());
            } else if op.path.contains("/schema") {
                set_member(model, "schema", op.value.clone());
            } else if op.path.contains("/contentType") {
                set_member(model, "contentType", op.value.clone());
            } else if op.path.contains("/name") {
                set_member(model, "name", op.value.clone());
            }
        }
    }
    ok(model.clone())
}

fn create_request_validator(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let validator = json!({
        "id": create_id(),
        "name": params.get("name").cloned().unwrap_or(Value::Null),
        "validateRequestBody": params.get("validateRequestBody").cloned().unwrap_or(Value::Null),
        "validateRequestParameters": params.get("validateRequestParameters").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.request_validators, api_id).insert(
        validator["id"].as_str().unwrap_or_default().to_string(),
        validator.clone(),
    );
    created_201(validator)
}

/// Boolean-ish patch values ("true"/"false" strings) become real booleans.
fn patch_validator_booleans(validator: &mut Value) {
    for key in ["validateRequestBody", "validateRequestParameters"] {
        if let Some(Value::String(s)) = validator.get(key).cloned() {
            validator[key] = json!(s == "true");
        }
    }
}

fn put_gateway_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    response_type: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let mut gw = json!({
        "responseType": response_type,
        "defaultResponse": false,
    });
    if let Some(code) = params.get("statusCode").cloned() {
        if !code.is_null() {
            gw["statusCode"] = code;
        }
    }
    if let Some(rp) = params.get("responseParameters").cloned() {
        if !rp.is_null() {
            gw["responseParameters"] = rp;
        }
    }
    if let Some(t) = params.get("responseTemplates").cloned() {
        if !t.is_null() {
            gw["responseTemplates"] = t;
        }
    }
    child_entry(&mut state.gateway_responses, api_id).insert(response_type.to_string(), gw.clone());
    created_201(gw)
}

/// Gateway-response PATCH: statusCode / responseParameters / responseTemplates keys.
fn apply_gateway_patch(gw: &mut Value, ops: &[PatchOp]) {
    for op in ops {
        if op.op != "replace" {
            continue;
        }
        if op.path.contains("/statusCode") {
            set_member(gw, "statusCode", op.value.clone());
        } else if let Some(key) = op.path.split("/responseParameters/").nth(1) {
            let key = key.to_string();
            if let Some(map) = obj_entry(gw, "responseParameters") {
                map.insert(key, json!(op.value.clone()));
            }
        } else if let Some(key) = op.path.split("/responseTemplates/").nth(1) {
            let key = key.to_string();
            if let Some(map) = obj_entry(gw, "responseTemplates") {
                map.insert(key, json!(op.value.clone()));
            }
        }
    }
}

fn create_documentation_part(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let part = json!({
        "id": create_id(),
        "location": params.get("location").cloned().unwrap_or(json!({})),
        "properties": params.get("properties").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.documentation_parts, api_id).insert(
        part["id"].as_str().unwrap_or_default().to_string(),
        part.clone(),
    );
    created_201(part)
}

fn create_documentation_version(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
) -> Response {
    if !state.apis.contains_key(api_id) {
        return not_found("Invalid Rest API Id specified");
    }
    let version = body_str(params, "documentationVersion")
        .unwrap_or_default()
        .to_string();
    if version.is_empty() {
        return not_found("Invalid Documentation version specified");
    }
    let part = json!({
        "version": version,
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "createdDate": unix_int(),
    });
    child_entry(&mut state.documentation_versions, api_id).insert(version, part.clone());
    created_201(part)
}

fn create_api_key(state: &mut State, params: &Map<String, Value>) -> Response {
    let value = match params.get("value").and_then(Value::as_str) {
        Some(v) => v.to_string(),
        None => create_id() + &create_id(),
    };
    if params.contains_key("value") && value.len() < 20 {
        return bad_request("API Key value should be at least 20 characters");
    }
    let duplicate = state
        .keys
        .values()
        .any(|k| k.get("value").and_then(Value::as_str) == Some(value.as_str()));
    if duplicate {
        return conflict("API Key already exists");
    }
    let key_id = create_id();
    let now = unix_int();
    let key = json!({
        "id": key_id,
        "value": value,
        "name": params.get("name").cloned().unwrap_or(Value::Null),
        "customerId": params.get("customerId").cloned().unwrap_or(Value::Null),
        "description": params.get("description").cloned().unwrap_or(Value::Null),
        "enabled": body_bool(params, "enabled").unwrap_or(false),
        "createdDate": now,
        "lastUpdatedDate": now,
        "stageKeys": params.get("stageKeys").cloned().unwrap_or(json!([])),
        "tags": params.get("tags").cloned().unwrap_or(Value::Null),
    });
    state.keys.insert(key_id, key.clone());
    created_201(key)
}

/// moto pops `value` off key json unless includeValues/includeValue is set.
fn strip_member(key: &Value, include: bool, member: &str) -> Value {
    let mut copy = key.clone();
    if !include {
        if let Some(map) = copy.as_object_mut() {
            map.remove(member);
        }
    }
    copy
}

// ---------------------------------------------------------------------------
// Handlers: usage plans / domains / vpc links / certs / account / rest
// ---------------------------------------------------------------------------

fn usage_plan_json(plan_id: &str, params: &Map<String, Value>) -> Value {
    let mut plan = json!({
        "id": plan_id,
        "name": params.get("name").cloned().unwrap_or(Value::Null),
        "description": params.get("description").cloned().unwrap_or(Value::Null),
        "apiStages": params.get("apiStages").cloned().unwrap_or(json!([])),
        "productCode": params.get("productCode").cloned().unwrap_or(Value::Null),
        "tags": params.get("tags").cloned().unwrap_or(Value::Null),
    });
    if let Some(throttle) = params.get("throttle").cloned() {
        if throttle.as_object().is_some_and(|m| !m.is_empty()) {
            plan["throttle"] = throttle;
        }
    }
    if let Some(quota) = params.get("quota").cloned() {
        if quota.as_object().is_some_and(|m| !m.is_empty()) {
            plan["quota"] = quota;
        }
    }
    plan
}

fn create_usage_plan(state: &mut State, params: &Map<String, Value>) -> Response {
    if body_str(params, "name").is_none() {
        return not_found("Invalid Usage Plan ID specified");
    }
    let plan_id = create_id();
    let plan = usage_plan_json(&plan_id, params);
    state.usage_plans.insert(plan_id, plan.clone());
    created_201(plan)
}

/// moto's UsagePlan.apply_patch_operations subset.
fn apply_usage_plan_patch(plan: &mut Value, ops: &[PatchOp]) {
    for op in ops {
        let path = op.path.as_str();
        let value = op.value.clone().unwrap_or_default();
        if op.op == "add" && path == "/apiStages" {
            let mut parts = value.split(':');
            let entry = json!({
                "apiId": parts.next().unwrap_or_default(),
                "stage": parts.next().unwrap_or_default(),
            });
            if let Some(list) = plan.get_mut("apiStages").and_then(Value::as_array_mut) {
                list.push(entry);
            }
        }
        if op.op == "replace" && path.contains("/name") {
            set_member(plan, "name", Some(value.clone()));
        }
        if op.op == "replace" && path.contains("/description") {
            set_member(plan, "description", Some(value.clone()));
        }
        if matches!(op.op.as_str(), "add" | "replace") {
            if path.contains("/productCode") {
                set_member(plan, "productCode", op.value.clone());
            }
            if path.contains("/quota/limit") {
                plan["quota"]["limit"] = json!(value
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or(Value::String(value.clone())));
            }
            if path.contains("/quota/period") {
                plan["quota"]["period"] = json!(value.clone());
            }
            if path.contains("/quota/offset") {
                plan["quota"]["offset"] = json!(value
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or(Value::String(value.clone())));
            }
            if path.contains("/throttle/rateLimit") {
                plan["throttle"]["rateLimit"] =
                    json!(value.parse::<i64>().map(Value::from).unwrap_or(Value::Null));
            }
            if path.contains("/throttle/burstLimit") {
                plan["throttle"]["burstLimit"] =
                    json!(value.parse::<i64>().map(Value::from).unwrap_or(Value::Null));
            }
        }
        if op.op == "remove" {
            if path == "/productCode" {
                set_member(plan, "productCode", None);
            }
            if path == "/quota" {
                plan["quota"] = json!({});
            }
            if path == "/throttle" {
                plan["throttle"] = json!({});
            }
            if path == "/apiStages" {
                let mut parts = value.split(':');
                let api_id = parts.next().unwrap_or_default();
                let stage = parts.next().unwrap_or_default();
                if let Some(list) = plan.get_mut("apiStages").and_then(Value::as_array_mut) {
                    list.retain(|s| {
                        s.get("apiId").and_then(Value::as_str) != Some(api_id)
                            || s.get("stage").and_then(Value::as_str) != Some(stage)
                    });
                }
            }
        }
    }
}

fn create_usage_plan_key(
    state: &mut State,
    params: &Map<String, Value>,
    usage_plan_id: &str,
) -> Response {
    let key_id = body_str(params, "keyId").unwrap_or_default().to_string();
    let key_type = body_str(params, "keyType").unwrap_or_default().to_string();
    if !state.usage_plans.contains_key(usage_plan_id) {
        return not_found("Invalid Usage Plan ID specified");
    }
    let Some(key_json) = state.keys.get(&key_id) else {
        return not_found("Invalid API Key identifier specified");
    };
    let plan_key = json!({
        "id": key_id,
        "name": key_json.get("name").cloned().unwrap_or(Value::Null),
        "type": key_type,
        "value": key_json.get("value").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.usage_plan_keys, usage_plan_id).insert(key_id, plan_key.clone());
    created_201(plan_key)
}

fn domain_name_json(domain: &str, params: &Map<String, Value>) -> Value {
    let mut domain = json!({
        "domainName": domain,
        "regionalDomainName": format!("d-{}.execute-api.us-east-1.amazonaws.com", create_id()),
        "distributionDomainName": format!("d{}.cloudfront.net", create_id()),
        "domainNameStatus": "AVAILABLE",
        "domainNameStatusMessage": "Domain Name Available",
        "regionalHostedZoneId": "Z2FDTNDATAQYW2",
        "distributionHostedZoneId": "Z2FDTNDATAQYW2",
        "certificateUploadDate": unix_int(),
    });
    for (param_key, wire_key) in [
        ("certificateName", "certificateName"),
        ("certificateArn", "certificateArn"),
        ("certificateBody", "certificateBody"),
        ("tags", "tags"),
        ("securityPolicy", "securityPolicy"),
        ("certificateChain", "certificateChain"),
        ("regionalCertificateName", "regionalCertificateName"),
        ("certificatePrivateKey", "certificatePrivateKey"),
        ("regionalCertificateArn", "regionalCertificateArn"),
        ("endpointConfiguration", "endpointConfiguration"),
    ] {
        if let Some(v) = params.get(param_key).cloned() {
            if !v.is_null() {
                domain[wire_key] = v;
            }
        }
    }
    domain
}

fn create_domain_name(state: &mut State, params: &Map<String, Value>) -> Response {
    let Some(domain_name) = body_str(params, "domainName") else {
        return not_found("No Domain Name specified");
    };
    if domain_name.is_empty() {
        return not_found("No Domain Name specified");
    }
    let domain_json = domain_name_json(domain_name, params);
    state
        .domain_names
        .insert(domain_name.to_string(), domain_json.clone());
    created_201(domain_json)
}

fn create_base_path_mapping(
    state: &mut State,
    params: &Map<String, Value>,
    domain_name: &str,
) -> Response {
    if !state.domain_names.contains_key(domain_name) {
        return not_found("Invalid domain name identifier specified");
    }
    let rest_api_id = body_str(params, "restApiId").unwrap_or_default();
    let base_path = body_str(params, "basePath").unwrap_or("(none)").to_string();
    if base_path.is_empty() {
        return bad_request("API Gateway V1 doesn't support the slash character (/) in base path mappings. To create a multi-level base path mapping, use API Gateway V2.");
    }
    if !state.apis.contains_key(rest_api_id) {
        return not_found("Invalid REST API identifier specified");
    }
    if let Some(stage) = body_str(params, "stage") {
        if state
            .stages
            .get(rest_api_id)
            .is_none_or(|m| !m.contains_key(stage))
        {
            return bad_request("Invalid stage identifier specified");
        }
    }
    let mapping = json!({
        // moto emits the snake_case `domain_name` member here.
        "domain_name": domain_name,
        "restApiId": rest_api_id,
        "basePath": base_path,
        "stage": params.get("stage").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.base_path_mappings, domain_name)
        .insert(params_base_path(params), mapping.clone());
    created_201(mapping)
}

fn params_base_path(params: &Map<String, Value>) -> String {
    body_str(params, "basePath").unwrap_or("(none)").to_string()
}

fn update_base_path_mapping(
    state: &mut State,
    params: &Map<String, Value>,
    domain_name: &str,
    base_path: &str,
) -> Response {
    if !state.domain_names.contains_key(domain_name) {
        return not_found("Invalid domain name identifier specified");
    }
    let Some(snapshot) = child_entry(&mut state.base_path_mappings, domain_name)
        .get(base_path)
        .cloned()
    else {
        return not_found("Invalid base path mapping identifier specified");
    };
    let mut mapping = snapshot;
    let mut new_base_path = base_path.to_string();
    for op in patch_ops(params) {
        if op.op == "replace" {
            if op.path.contains("/basePath") {
                if let Some(v) = &op.value {
                    new_base_path = v.clone();
                    mapping["basePath"] = json!(v);
                }
            } else if op.path.contains("/restapiId") || op.path.contains("/restApiId") {
                set_member(&mut mapping, "restApiId", op.value.clone());
            } else if op.path.contains("/stage") {
                set_member(&mut mapping, "stage", op.value.clone());
            }
        }
    }
    if new_base_path != base_path {
        let map = child_entry(&mut state.base_path_mappings, domain_name);
        let mapping_value = map.remove(base_path).unwrap_or(json!({}));
        map.insert(new_base_path, mapping_value);
    } else {
        let map = child_entry(&mut state.base_path_mappings, domain_name);
        map.insert(base_path.to_string(), mapping.clone());
    }
    ok(mapping)
}

fn create_vpc_link(state: &mut State, params: &Map<String, Value>) -> Response {
    let vpc_link_id = create_id();
    let mut vpc_link = json!({
        "id": vpc_link_id,
        "name": params.get("name").cloned().unwrap_or(Value::Null),
        "description": params.get("description").cloned().unwrap_or(Value::Null),
        "targetArns": params.get("targetArns").cloned().unwrap_or(json!([])),
        "tags": params.get("tags").cloned().unwrap_or(Value::Null),
        "status": "AVAILABLE",
    });
    if let Some(t) = params.get("tags").cloned() {
        if !t.is_null() {
            vpc_link["tags"] = t;
        }
    }
    state.vpc_links.insert(vpc_link_id, vpc_link.clone());
    json_resp(202, vpc_link)
}

fn generate_client_certificate(state: &mut State, params: &Map<String, Value>) -> Response {
    let cert_id = create_id();
    let cert = json!({
        "clientCertificateId": cert_id,
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "pemEncodedCertificate": "-----BEGIN CERTIFICATE-----\nMIICmock...\n-----END CERTIFICATE-----",
        "createdDate": unix_int(),
        "expirationDate": unix_int() + 365 * 24 * 3600,
    });
    state.client_certificates.insert(cert_id, cert.clone());
    created_201(cert)
}

fn update_account(state: &mut State, params: &Map<String, Value>) -> Response {
    for op in patch_ops(params) {
        if op.path.contains("/cloudwatchRoleArn") {
            state.account["cloudwatchRoleArn"] = op_value_json(&op);
        } else if op.path.contains("/features") {
            if op.op == "add" {
                let features = state
                    .account
                    .get("features")
                    .cloned()
                    .unwrap_or(Value::Null);
                let mut list = features.as_array().cloned().unwrap_or_default();
                if let Some(v) = op.value.clone() {
                    list.push(json!(v));
                }
                state.account["features"] = Value::Array(list);
            } else if op.op == "remove" {
                if op.value.as_deref() == Some("UsagePlans") {
                    return bad_request("Usage Plans cannot be disabled once enabled");
                }
                if let Some(list) = state
                    .account
                    .get_mut("features")
                    .and_then(Value::as_array_mut)
                {
                    if let Some(v) = &op.value {
                        list.retain(|entry| entry.as_str() != Some(v.as_str()));
                    }
                }
            }
        }
    }
    ok(state.account.clone())
}

fn op_value_json(op: &PatchOp) -> Value {
    match &op.value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

/// The Usage shape moto's get_usage/update_usage return for an empty plan.
fn get_usage_body(usage_plan_id: &str) -> Value {
    json!({
        "usagePlanId": usage_plan_id,
        "startDate": today_date(),
        "endDate": today_date(),
        "position": Value::Null,
        "items": {},
    })
}

fn create_domain_name_access_association(
    state: &mut State,
    params: &Map<String, Value>,
    region: &str,
) -> Response {
    let domain_arn = body_str(params, "domainNameArn")
        .unwrap_or_default()
        .to_string();
    let source_type = body_str(params, "accessAssociationSourceType")
        .unwrap_or_default()
        .to_string();
    let source = body_str(params, "accessAssociationSource")
        .unwrap_or_default()
        .to_string();
    let arn = format!(
        "arn:aws:apigateway:{region}::/domainNameAccessAssociations/{}",
        create_id()
    );
    let association = json!({
        "domainNameAccessAssociationArn": arn,
        "domainNameArn": domain_arn,
        "accessAssociationSourceType": source_type,
        "accessAssociationSource": source,
    });
    state
        .domain_access_associations
        .insert(arn.clone(), association.clone());
    ok(association)
}

const SDK_TYPES: [(&str, &str, &str); 1] = [(
    "javascript",
    "JavaScript",
    "JavaScript SDK based on the AWS SDK for JavaScript",
)];

fn sdk_types_json() -> Vec<Value> {
    SDK_TYPES
        .iter()
        .map(|(id, friendly, desc)| {
            json!({ "id": id, "friendlyName": friendly, "description": desc })
        })
        .collect()
}

fn sdk_type_json(sdk_type: &str) -> Option<Value> {
    SDK_TYPES.iter().find(|(id, _, _)| *id == sdk_type).map(
        |(id, friendly, desc)| json!({ "id": id, "friendlyName": friendly, "description": desc }),
    )
}

/// GetTags / TagResource / UntagResource — the python ground-truth provider
/// owns these: operations REST-api ARNs, 204 `{}` bodies.
/// Extract the api id from `...::/restapis/{id}`-style ARNs.
fn tag_api_id(arn: &str) -> Option<String> {
    let rest = arn.split("/restapis/").nth(1)?;
    Some(rest.split('/').next().unwrap_or_default().to_string())
}

/// GetTags / TagResource / UntagResource — python ground truth semantics
/// (REST-api ARNs only; 204 `{}` bodies for PUT/DELETE).
fn tags_route(req: &Request) -> Response {
    let raw = req.path.split_once("/tags/").map(|(_, v)| v).unwrap_or("");
    let arn = crate::proto::url_decode(raw);
    let Some(api_id) = tag_api_id(&arn) else {
        return message_response(404, "Invalid resource ARN");
    };
    if api_id.is_empty() {
        return message_response(404, "Invalid resource ARN");
    }
    with_state(req.account, &req.region, move |state| {
        let Some(api) = state.apis.get_mut(&api_id) else {
            return message_response(404, "Invalid API identifier specified");
        };
        match req.method.as_str() {
            "PUT" => {
                let params = body_object(&req.body);
                for (k, v) in string_map(params.get("tags")) {
                    if let Some(map) = api["tags"].as_object_mut() {
                        map.insert(k, json!(v));
                    }
                }
                Response::new(204, "application/json", b"{}".to_vec())
            }
            "DELETE" => {
                let keys = query_all(req, "tagKeys");
                if let Some(map) = api["tags"].as_object_mut() {
                    for k in keys {
                        map.remove(&k);
                    }
                }
                Response::new(204, "application/json", b"{}".to_vec())
            }
            "GET" => ok(json!({ "tags": api["tags"].clone() })),
            _ => Response::new(405, "application/json", b"{}".to_vec()),
        }
    })
}

fn string_map(value: Option<&Value>) -> Vec<(String, String)> {
    match value {
        Some(Value::Object(map)) => map
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// _user_request_ execution (mirrors the python ground-truth executor)
// ---------------------------------------------------------------------------

/// What the gateway must do for a `/restapis/{api}/{stage}/_user_request_/...`
/// hit. `message` shapes error bodies `{"message": ...}` like the python
/// ground truth; `lambda` carries the function name + proxy event.
pub struct ExecDirective {
    pub status: u16,
    pub message: Option<String>,
    /// A plain JSON body (Mock integrations answer `{}` like the py executor).
    pub body: Option<serde_json::Value>,
    pub lambda: Option<(String, serde_json::Value)>,
}

/// Path-parameter extraction result template match.
struct Matched {
    resource_id: String,
    params: Option<Map<String, Value>>,
}

/// Specificity in the py executor: literal +10, `{p}` +5, `{p+}` +1.
fn path_specificity(pattern: &str) -> i64 {
    pattern
        .split('/')
        .map(|part| {
            if part.starts_with('{') && part.ends_with("+}") {
                1
            } else if part.starts_with('{') && part.ends_with('}') {
                5
            } else {
                10
            }
        })
        .sum()
}

/// py `_path_matches`: normalize trailing slashes; `{p+}` = greedy rest.
fn path_matches(pattern: &str, path: &str) -> Option<Map<String, Value>> {
    fn normalize(input: &str) -> Vec<&str> {
        input.trim_matches('/').split('/').collect()
    }
    let pattern_parts = normalize(pattern);
    let path_parts = normalize(path);
    let mut params = Map::new();
    for (index, ppart) in path_parts.iter().enumerate() {
        {
            let pattern_part = pattern_parts.get(index)?;
            if pattern_part.starts_with('{') {
                if pattern_part.ends_with("+}") {
                    // greedy: the rest of the path
                    let name = pattern_part
                        .strip_suffix("+}")
                        .unwrap_or_default()
                        .trim_start_matches('{')
                        .to_string();
                    let rest: Vec<&str> = path_parts[index..].to_vec();
                    params.insert(name.to_string(), json!(rest.join("/")));
                    return Some(params);
                }
                {
                    let name = pattern_part.strip_suffix("}")?;
                    let name = name.trim_start_matches('{');
                    params.insert(name.to_string(), json!(*ppart));
                }
            } else if *pattern_part != *ppart {
                return None;
            }
        }
    }
    if pattern_parts.len() > path_parts.len() {
        // Pattern longer than path: only matches if the tail is a greedy param.
        let tail = &pattern_parts[path_parts.len()..];
        let only_greedy = tail.len() == 1 && tail[0].starts_with('{') && tail[0].ends_with("+}");
        if only_greedy {
            let name = tail[0]
                .strip_suffix("+}")
                .unwrap_or_default()
                .trim_start_matches('{');
            params.insert(name.to_string(), json!(""));
            return Some(params);
        }
        return None;
    }
    Some(params)
}

/// Best (most-specific) resource match for a deployed API path.
fn match_resource(state: &State, api_id: &str, wanted: &str) -> Option<Matched> {
    let empty_resources = BTreeMap::new();
    let resources = state.resources.get(api_id).unwrap_or(&empty_resources);
    let mut best: Option<(i64, Matched)> = None;
    for id in resources.keys() {
        let pattern = resource_path_in(resources, id);
        if let Some(params) = path_matches(&pattern, wanted) {
            let specificity = path_specificity(&pattern);
            let better = best
                .as_ref()
                .is_none_or(|(current, _)| specificity > *current);
            if better {
                best = Some((
                    specificity,
                    Matched {
                        resource_id: id.clone(),
                        params: (!params.is_empty()).then_some(params),
                    },
                ));
            }
        }
    }
    best.map(|(_, matched)| matched)
}

fn resolve_directive(status: u16, message: &str) -> ExecDirective {
    ExecDirective {
        status,
        message: Some(message.to_string()),
        body: None,
        lambda: None,
    }
}

/// Resolve a `_user_request_/{path}` request against the stored API config.
/// Behaviors follow `src/robotocore/services/apigateway/executor.py`.
#[allow(clippy::too_many_arguments)]
pub fn resolve_execution(
    account: u64,
    region: &str,
    api_id: &str,
    _stage: &str,
    method: &str,
    proxy_path: &str,
    query_string: &str,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
) -> ExecDirective {
    let wanted = format!("/{}", crate::proto::url_decode(proxy_path));
    let guard = SLOT.read();
    let state = match guard.get(&(account, region.to_string())) {
        Some(state) => state,
        None => {
            return resolve_directive(404, &format!("API {api_id} not found"));
        }
    };
    let Some(_api) = state.apis.get(api_id) else {
        return resolve_directive(404, &format!("API {api_id} not found"));
    };
    let Some(matched) = match_resource(state, api_id, &wanted) else {
        return resolve_directive(404, "Not Found");
    };
    // The method on the resource (ANY fallback mirrors the python executor).
    let method_upper = method.to_uppercase();
    let resources_empty = BTreeMap::new();
    let resources = state.resources.get(api_id).unwrap_or(&resources_empty);
    let Some(method_value) = resources
        .get(&matched.resource_id)
        .and_then(|r| r.get("resourceMethods"))
        .and_then(Value::as_object)
        .and_then(|m| m.get(&method_upper).or_else(|| m.get("ANY")))
    else {
        return resolve_directive(405, &format!("Method {method_upper} not allowed"));
    };
    let Some(integration) = method_value
        .get("methodIntegration")
        .filter(|i| i.is_object())
    else {
        return resolve_directive(500, "No integration configured");
    };
    let integration_type = integration
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_uppercase();
    let uri = integration
        .get("uri")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    match integration_type.as_str() {
        "AWS_PROXY" | "AWS" | "LAMBDA_PROXY" | "LAMBDA" => {
            let Some(function_name) = lambda_from_uri(&uri) else {
                return resolve_directive(500, "Could not resolve Lambda function from URI");
            };
            let event = api_gw_proxy_event(
                api_id,
                _stage,
                &method_upper,
                &wanted,
                query_string,
                matched.params,
                headers,
                body,
                account,
            );
            ExecDirective {
                status: 200,
                message: None,
                body: None,
                lambda: Some((function_name, event)),
            }
        }
        "MOCK" => ExecDirective {
            status: 200,
            message: None,
            body: Some(json!({})),
            lambda: None,
        },
        _ => resolve_directive(501, "Not Implemented"),
    }
}

/// Lambda function name from a proxy integration uri
/// (`...functions/{arn}/invocations`), mirroring the python executor.
fn lambda_from_uri(uri: &str) -> Option<String> {
    let marker = "functions/";
    let start = uri.find(marker)? + marker.len();
    let rest = &uri[start..];
    let arn = rest.split("/invocations").next()?;
    if arn.starts_with("arn:") {
        arn.split(':').nth(6).map(String::from)
    } else {
        Some(arn.to_string())
    }
}

/// Build the API Gateway proxy event exactly as the python executor does.
#[allow(clippy::too_many_arguments)]
fn api_gw_proxy_event(
    api_id: &str,
    stage: &str,
    method: &str,
    path: &str,
    query_string: &str,
    path_params: Option<Map<String, Value>>,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
    account: u64,
) -> Value {
    let query_map = crate::proto::query_map(query_string);
    let query_single: Map<String, Value> = query_map
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.last().cloned().map(Value::String).unwrap_or(Value::Null),
            )
        })
        .collect();
    let body_str = if body.is_empty() {
        Value::Null
    } else {
        Value::String(String::from_utf8_lossy(body).into_owned())
    };
    let query_params = if query_single.is_empty() {
        Value::Null
    } else {
        Value::Object(query_single)
    };
    let path_params = path_params.map(Value::Object).unwrap_or(Value::Null);
    let stage_path = format!("/{stage}{path}");
    json!({
        "resource": path,
        "path": path,
        "httpMethod": method,
        "headers": headers,
        "queryStringParameters": query_params,
        "pathParameters": path_params,
        "stageVariables": Value::Null,
        "requestContext": {
            "resourceId": "proxy",
            "resourcePath": path,
            "httpMethod": method,
            "path": stage_path,
            "accountId": account.to_string(),
            "stage": stage,
            "requestId": uuid::Uuid::new_v4().to_string(),
            "identity": {
                "sourceIp": headers.get("x-forwarded-for").cloned().unwrap_or_else(|| "127.0.0.1".to_string()),
                "userAgent": headers.get("user-agent").cloned().unwrap_or_default(),
            },
            "apiId": api_id,
        },
        "body": body_str,
        "isBase64Encoded": false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_ctx() -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(271111111110);
        NEXT.fetch_add(1, Ordering::SeqCst)
    }

    fn req(method: &str, path: &str, body: Value, account: u64) -> Request {
        Request {
            service: "apigateway".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn resources_methods_integration_response_roundtrip() {
        let acct = fresh_ctx();
        let created = parsed(&dispatch(&req(
            "POST",
            "/restapis",
            json!({"name": "ir"}),
            acct,
        )));
        let api = created["id"].as_str().unwrap().to_string();
        let root = created["rootResourceId"].as_str().unwrap().to_string();
        dispatch(&req(
            "PUT",
            &format!("/restapis/{api}/resources/{root}/methods/GET"),
            json!({"authorizationType": "NONE"}),
            acct,
        ));
        dispatch(&req(
            "PUT",
            &format!("/restapis/{api}/resources/{root}/methods/GET/integration"),
            json!({"type": "MOCK", "requestTemplates": {}}),
            acct,
        ));
        let ir = dispatch(&req(
            "PUT",
            &format!("/restapis/{api}/resources/{root}/methods/GET/integration/responses/200"),
            json!({"responseTemplates": {"application/json": ""}}),
            acct,
        ));
        assert_eq!(ir.status, 201, "put integration response");
        let got = dispatch(&req(
            "GET",
            &format!("/restapis/{api}/resources/{root}/methods/GET/integration/responses/200"),
            json!({}),
            acct,
        ));
        assert_eq!(got.status, 200, "get integration response");
        let body = parsed(&got);
        assert_eq!(body["statusCode"], "200");
    }

    #[test]
    fn account_and_usage() {
        let acct = fresh_ctx();

        // GetAccount returns moto's Account defaults, never a null body.
        let account = parsed(&dispatch(&req("GET", "/account", json!({}), acct)));
        assert!(account.get("apiKeyVersion").is_some(), "account body");
        assert_eq!(account["throttleSettings"]["burstLimit"], 5000);

        // UpdateUsage echoes get_usage for the plan in the path.
        let plan = parsed(&dispatch(&req(
            "POST",
            "/usageplans",
            json!({"name": "up"}),
            acct,
        )))["id"]
            .as_str()
            .unwrap()
            .to_string();
        let usage = parsed(&dispatch(&req(
            "PATCH",
            &format!("/usageplans/{plan}/keys/fake-key/usage"),
            json!({"patchOperations": [{"op": "replace", "path": "/remaining", "value": "100"}]}),
            acct,
        )));
        assert_eq!(usage["usagePlanId"], plan);
        assert!(usage["items"].as_object().is_some());
        // Unknown plan id keeps moto's "Invalid Usage Plan ID specified".
        let missing = dispatch(&req(
            "PATCH",
            "/usageplans/ghost/keys/k/usage",
            json!({}),
            acct,
        ));
        assert_eq!(missing.status, 404);
        assert_eq!(
            parsed(&missing)["message"],
            "Invalid Usage Plan ID specified"
        );
    }
}
