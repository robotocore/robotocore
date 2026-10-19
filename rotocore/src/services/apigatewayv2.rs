//! API Gateway v2 (HTTP / WebSocket APIs) — port of the python ground-truth
//! provider (`src/robotocore/services/apigatewayv2/provider.py`), which the
//! compat suite exercises at `test_apigatewayv2_compat.py`.
//!
//! State is the wire JSON itself; wire keys are camelCase (AWS rest-json
//! `locationName`s), exactly what the python provider emits after its
//! Pascal→camel pass. Creates answer 201, deletes 204, everything else 200;
//! unknown resources raise `NotFound` (`Code: NotFoundException`, body
//! `{"Code", "Message"}`). Portal / product-page / routing-rule families
//! are not modeled — every op answers NotFound, which both the python
//! ground truth and the suite expect.
//!
//! Stage / authorizer / deployment / vpc-link / domain-name / api-mapping
//! PATCH updates are supported (python answers; moto 501s them), as are
//! `ImportApi` (PUT /v2/apis), `ExportApi`, and the stage-level
//! access-log/route-settings/cache deletes.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Helpers
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

fn created(status: u16, body: Value) -> Response {
    json_resp(status, body)
}

fn no_content() -> Response {
    Response::new(204, "application/json", Vec::new())
}

/// py `_error`: `{"Message": msg, "Code": code}`.
fn v2_error(status: u16, code: &str, message: &str) -> Response {
    json_resp(status, json!({ "Message": message, "Code": code }))
}

fn not_found(message: &str) -> Response {
    v2_error(404, "NotFoundException", message)
}

fn bad_request(message: &str) -> Response {
    v2_error(400, "BadRequestException", message)
}

fn route_unknown(method: &str, path: &str) -> Response {
    not_found(&format!("Unknown path: {method} {path}"))
}

fn unix_iso() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Short random id (py `_short_id`).
fn create_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..10].to_string()
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

/// Read a member under either the PascalCase or the camelCase wire key.
fn param<'a>(params: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(v) = params.get(key) {
        return Some(v);
    }
    // PascalCase → camelCase: "ApiMappingKey" → "apiMappingKey".
    let mut chars = key.chars();
    if let Some(first) = chars.next() {
        return params.get(&format!("{}{}", first.to_lowercase(), chars.as_str()));
    }
    None
}

// ---------------------------------------------------------------------------
// State (entities stored as the python provider emits them, camelCase)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct State {
    apis: BTreeMap<String, Value>,
    routes: BTreeMap<String, BTreeMap<String, Value>>,
    integrations: BTreeMap<String, BTreeMap<String, Value>>,
    stages: BTreeMap<String, BTreeMap<String, Value>>,
    authorizers: BTreeMap<String, BTreeMap<String, Value>>,
    deployments: BTreeMap<String, BTreeMap<String, Value>>,
    models: BTreeMap<String, BTreeMap<String, Value>>,
    // api → integration → response id → response
    integration_responses: BTreeMap<String, BTreeMap<String, BTreeMap<String, Value>>>,
    // api → route → response id → response
    route_responses: BTreeMap<String, BTreeMap<String, BTreeMap<String, Value>>>,
    domain_names: BTreeMap<String, Value>,
    api_mappings: BTreeMap<String, BTreeMap<String, Value>>,
    vpc_links: BTreeMap<String, Value>,
}

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("apigatewayv2"));

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut State) -> T) -> T {
    let mut guard = SLOT.write();
    let state = guard.entry((account, region.to_string())).or_default();
    f(state)
}

fn child_entry<'a>(
    store: &'a mut BTreeMap<String, BTreeMap<String, Value>>,
    key: &str,
) -> &'a mut BTreeMap<String, Value> {
    store.entry(key.to_string()).or_default()
}

/// The api id of an ARN like `arn:aws:apigateway:us-east-1::/apis/{id}`.
fn api_id_from_arn(arn: &str) -> Option<String> {
    let rest = arn.split("/apis/").nth(1)?;
    Some(rest.split('/').next().unwrap_or_default().to_string())
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let params = body_object(&req.body);
    let segments: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.first() == Some(&"v2") && segments.get(1) == Some(&"tags") {
        return tags_route(req);
    }

    with_state(req.account, &req.region, move |state| {
        route(state, req, &params, &segments)
    })
}

/// Reply chain for route(): handlers own their own lookups/errors.
fn route(
    state: &mut State,
    req: &Request,
    params: &Map<String, Value>,
    segments: &[&str],
) -> Response {
    let method = req.method.as_str();
    let unknown = || route_unknown(req.method.as_str(), req.path.as_str());
    match segments {
        ["v2", "apis"] => match method {
            "POST" => create_api(state, params, req.region.as_str()),
            "GET" => get_apis(state),
            "PUT" => import_api(state, params, req.region.as_str()),
            _ => unknown(),
        },
        ["v2", "apis", api_id] => match method {
            "GET" => {
                if let Err(resp) = require_api(state, api_id) {
                    return resp;
                }
                ok(state.apis[*api_id].clone())
            }
            "PATCH" => update_api(state, params, api_id),
            "DELETE" => delete_api(state, api_id),
            "PUT" => reimport_api(state, params, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "cors"] => match method {
            "DELETE" => {
                if let Err(resp) = require_api(state, api_id) {
                    return resp;
                }
                if let Some(api) = state.apis.get_mut(*api_id) {
                    if let Some(map) = api.as_object_mut() {
                        map.remove("corsConfiguration");
                    }
                }
                no_content()
            }
            _ => unknown(),
        },
        ["v2", "apis", api_id, "exports", specification] => {
            export_api(state, api_id, specification)
        }
        ["v2", "apis", api_id, "routes"] => match method {
            "POST" => create_route(state, params, api_id),
            "GET" => get_routes(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "routes", route_id] => match method {
            "GET" => get_route(state, api_id, route_id),
            "PATCH" => update_route(state, params, api_id, route_id),
            "DELETE" => delete_route(state, api_id, route_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "routes", route_id, "routeresponses"] => match method {
            "POST" => create_route_response(state, params, api_id, route_id),
            "GET" => get_route_responses(state, api_id, route_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "routes", route_id, "routeresponses", response_id] => match method {
            "GET" => get_route_response(state, api_id, route_id, response_id),
            "PATCH" => update_route_response(state, params, api_id, route_id, response_id),
            "DELETE" => delete_route_response(state, api_id, route_id, response_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "routes", route_id, "requestparameters", param_key] => {
            if method != "DELETE" {
                return unknown();
            }
            let param_key = crate::proto::url_decode(param_key);
            if let Err(resp) = with_route(state, api_id, route_id, |route| {
                let has_key = route
                    .get("requestParameters")
                    .and_then(Value::as_object)
                    .is_some_and(|m| m.contains_key(param_key.as_str()));
                if has_key {
                    if let Some(entry) = route
                        .as_object_mut()
                        .and_then(|m| m.get_mut("requestParameters"))
                        .and_then(Value::as_object_mut)
                    {
                        entry.remove(&param_key);
                    }
                }
            }) {
                return resp;
            }
            no_content()
        }
        ["v2", "apis", api_id, "integrations"] => match method {
            "POST" => create_integration(state, params, api_id),
            "GET" => get_integrations(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "integrations", integration_id] => match method {
            "GET" => get_integration(state, api_id, integration_id),
            "PATCH" => update_integration(state, params, api_id, integration_id),
            "DELETE" => delete_integration(state, api_id, integration_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "integrations", integration_id, "integrationresponses"] => {
            match method {
                "POST" => create_integration_response(state, params, api_id, integration_id),
                "GET" => get_integration_responses(state, api_id, integration_id),
                _ => unknown(),
            }
        }
        ["v2", "apis", api_id, "integrations", integration_id, "integrationresponses", response_id] => {
            match method {
                "GET" => get_integration_response(state, api_id, integration_id, response_id),
                "PATCH" => {
                    update_integration_response(state, params, api_id, integration_id, response_id)
                }
                "DELETE" => delete_integration_response(state, api_id, integration_id, response_id),
                _ => unknown(),
            }
        }
        ["v2", "apis", api_id, "stages"] => match method {
            "POST" => create_stage(state, params, api_id),
            "GET" => get_stages(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "stages", stage_name] => match method {
            "GET" => get_stage(state, api_id, stage_name),
            "PATCH" => update_stage(state, params, api_id, stage_name),
            "DELETE" => delete_stage(state, api_id, stage_name),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "stages", stage_name, "accesslogsettings"] => match method {
            "DELETE" => {
                if let Err(resp) = require_stage(state, api_id, stage_name) {
                    return resp;
                }
                if let Some(stage) = child_entry(&mut state.stages, api_id).get_mut(*stage_name) {
                    if let Some(map) = stage.as_object_mut() {
                        map.remove("accessLogSettings");
                    }
                }
                no_content()
            }
            _ => unknown(),
        },
        ["v2", "apis", api_id, "stages", stage_name, "routesettings", route_key] => match method {
            "DELETE" => {
                if let Err(resp) = require_stage(state, api_id, stage_name) {
                    return resp;
                }
                if let Some(stage) = child_entry(&mut state.stages, api_id).get_mut(*stage_name) {
                    if let Some(map) = stage
                        .get_mut("routeSettings")
                        .and_then(Value::as_object_mut)
                    {
                        map.remove(*route_key);
                    }
                }
                no_content()
            }
            _ => unknown(),
        },
        ["v2", "apis", api_id, "stages", stage_name, "cache", "authorizers"] => match method {
            "DELETE" => {
                if let Err(resp) = require_stage(state, api_id, stage_name) {
                    return resp;
                }
                no_content()
            }
            _ => unknown(),
        },
        ["v2", "apis", api_id, "authorizers"] => match method {
            "POST" => create_authorizer(state, params, api_id),
            "GET" => get_authorizers(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "authorizers", authorizer_id] => match method {
            "GET" => get_authorizer(state, api_id, authorizer_id),
            "PATCH" => update_authorizer(state, params, api_id, authorizer_id),
            "DELETE" => delete_authorizer(state, api_id, authorizer_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "deployments"] => match method {
            "POST" => create_deployment(state, params, api_id),
            "GET" => get_deployments(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "deployments", deployment_id] => match method {
            "GET" => get_deployment(state, api_id, deployment_id),
            "PATCH" => update_deployment(state, params, api_id, deployment_id),
            "DELETE" => delete_deployment(state, api_id, deployment_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "models"] => match method {
            "POST" => create_model(state, params, api_id),
            "GET" => get_models(state, api_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "models", model_id] => match method {
            "GET" => get_model(state, api_id, model_id),
            "PATCH" => update_model(state, params, api_id, model_id),
            "DELETE" => delete_model(state, api_id, model_id),
            _ => unknown(),
        },
        ["v2", "apis", api_id, "models", model_id, "template"] => {
            get_model_template(state, api_id, model_id)
        }
        ["v2", "domainnames"] => match method {
            "POST" => create_domain_name(state, params, req.region.as_str()),
            "GET" => get_domain_names(state),
            _ => unknown(),
        },
        ["v2", "domainnames", domain_name] => match method {
            "GET" => get_domain_name(state, domain_name),
            "PATCH" => update_domain_name(state, params, domain_name),
            "DELETE" => delete_domain_name(state, domain_name),
            _ => unknown(),
        },
        ["v2", "domainnames", domain_name, "apimappings"] => match method {
            "POST" => create_api_mapping(state, params, domain_name),
            "GET" => get_api_mappings(state, domain_name),
            _ => unknown(),
        },
        ["v2", "domainnames", domain_name, "apimappings", mapping_id] => match method {
            "GET" => get_api_mapping(state, domain_name, mapping_id),
            "PATCH" => update_api_mapping(state, params, domain_name, mapping_id),
            "DELETE" => delete_api_mapping(state, domain_name, mapping_id),
            _ => unknown(),
        },
        ["v2", "vpclinks"] => match method {
            "POST" => create_vpc_link(state, params),
            "GET" => get_vpc_links(state),
            _ => unknown(),
        },
        ["v2", "vpclinks", vpc_link_id] => match method {
            "GET" => get_vpc_link(state, vpc_link_id),
            "PATCH" => update_vpc_link(state, params, vpc_link_id),
            "DELETE" => delete_vpc_link(state, vpc_link_id),
            _ => unknown(),
        },
        // Portal / portal-product / product-page / routing-rule surfaces are
        // not modeled → NotFound (matches the python provider).
        _ => route_unknown(req.method.as_str(), req.path.as_str()),
    }
}

// ---------------------------------------------------------------------------
// Handlers: apis
// ---------------------------------------------------------------------------

/// Verify the api exists; `Err(response)` carries the 404 to early-return.
fn require_api(state: &State, api_id: &str) -> Result<(), Response> {
    if state.apis.contains_key(api_id) {
        Ok(())
    } else {
        Err(not_found(&format!("API {api_id} not found")))
    }
}

fn require_stage(state: &State, api_id: &str, stage_name: &str) -> Result<(), Response> {
    require_api(state, api_id)?;
    if state
        .stages
        .get(api_id)
        .is_some_and(|m| m.contains_key(stage_name))
    {
        Ok(())
    } else {
        Err(not_found(&format!("Stage {stage_name} not found")))
    }
}

fn create_api(state: &mut State, params: &Map<String, Value>, region: &str) -> Response {
    let protocol_type = body_str(params, "protocolType")
        .unwrap_or("HTTP")
        .to_string();
    if protocol_type != "HTTP" && protocol_type != "WEBSOCKET" {
        return bad_request("Invalid protocol specified. Must be one of [HTTP, WEBSOCKET]");
    }
    let api_id = create_id();
    let route_selection = body_str(params, "routeSelectionExpression")
        .map(String::from)
        .unwrap_or_else(|| {
            if protocol_type == "WEBSOCKET" {
                "$request.body.action".to_string()
            } else {
                "${request.method} ${request.path}".to_string()
            }
        });
    let api = json!({
        "apiId": api_id,
        "apiEndpoint": format!("https://{api_id}.execute-api.{region}.amazonaws.com"),
        "name": params.get("name").cloned().unwrap_or(json!("")),
        "protocolType": protocol_type,
        "routeSelectionExpression": route_selection,
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "disableSchemaValidation": params.get("disableSchemaValidation").cloned().unwrap_or(json!(false)),
        "disableExecuteApiEndpoint": params.get("disableExecuteApiEndpoint").cloned().unwrap_or(json!(false)),
        "apiKeySelectionExpression": params.get("apiKeySelectionExpression").cloned().unwrap_or(json!("$request.header.x-api-key")),
        "corsConfiguration": params.get("corsConfiguration").cloned().unwrap_or(Value::Null),
        "version": params.get("version").cloned().unwrap_or(json!("")),
        "tags": params.get("tags").cloned().unwrap_or(json!({})),
        "createdDate": unix_iso(),
    });
    state.apis.insert(api_id, api.clone());
    created(201, api)
}

fn get_apis(state: &mut State) -> Response {
    ok(json!({ "items": state.apis.values().cloned().collect::<Vec<Value>>() }))
}

fn update_api(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let Some(api) = state.apis.get_mut(api_id) else {
        return not_found(&format!("API {api_id} not found"));
    };
    let mut patched = api.clone();
    for key in [
        "name",
        "description",
        "routeSelectionExpression",
        "corsConfiguration",
        "version",
        "disableSchemaValidation",
        "disableExecuteApiEndpoint",
        "apiKeySelectionExpression",
    ] {
        if let Some(value) = param(params, capitalized(key).as_str()) {
            if let Some(map) = patched.as_object_mut() {
                map.insert(key.to_string(), value.clone());
            }
        }
    }
    *api = patched.clone();
    ok(patched)
}

/// `name` → `Name` (keys in the wire bodies are camelCase).
fn capitalized(key: &str) -> String {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        None => key.to_string(),
    }
}

fn delete_api(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    state.apis.remove(api_id);
    state.routes.remove(api_id);
    state.integrations.remove(api_id);
    state.stages.remove(api_id);
    state.authorizers.remove(api_id);
    state.deployments.remove(api_id);
    state.models.remove(api_id);
    state.integration_responses.remove(api_id);
    state.route_responses.remove(api_id);
    no_content()
}

/// ImportApi (PUT /v2/apis): create an API from an OpenAPI body, taking the
/// title from `info`. Routes are imported by `import_paths`-styled logic.
fn import_api(state: &mut State, params: &Map<String, Value>, region: &str) -> Response {
    let spec = spec_from(params);
    let name = spec
        .pointer("/info/title")
        .and_then(Value::as_str)
        .unwrap_or("Imported API")
        .to_string();
    let mut create_params = Map::new();
    create_params.insert("name".to_string(), json!(name));
    if let Some(protocol) = spec
        .get("x-amazon-apigateway-protocol")
        .and_then(Value::as_str)
    {
        create_params.insert("protocolType".to_string(), json!(protocol));
    }
    if let Some(version) = spec.pointer("/info/version").cloned() {
        create_params.insert("version".to_string(), version);
    }
    create_api(state, &create_params, region)
}

fn reimport_api(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let spec = spec_from(params);
    let Some(api) = state.apis.get_mut(api_id) else {
        return not_found(&format!("API {api_id} not found"));
    };
    if let Some(title) = spec.pointer("/info/title").and_then(Value::as_str) {
        api["name"] = json!(title);
    }
    if let Some(version) = spec.pointer("/info/version").and_then(Value::as_str) {
        api["version"] = json!(version);
    }
    ok(api.clone())
}

/// The OpenAPI body of ImportApi/ReimportApi arrives as a JSON member.
fn spec_from(params: &Map<String, Value>) -> Value {
    let body = param(params, "Body").cloned().unwrap_or(Value::Null);
    match body {
        Value::String(text) => serde_json::from_str(&text).unwrap_or(json!({})),
        Value::Object(map) => Value::Object(map),
        _ => json!({}),
    }
}

// ---------------------------------------------------------------------------
// Handlers: routes / integration responses helpers
// ---------------------------------------------------------------------------

fn with_route(
    state: &mut State,
    api_id: &str,
    route_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.routes, api_id).get_mut(route_id) {
        Some(route) => {
            f(route);
            Ok(())
        }
        None => Err(not_found(&format!("Route {route_id} not found"))),
    }
}

fn create_route(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let route_id = create_id();
    let route = json!({
        "routeId": route_id,
        "routeKey": params.get("routeKey").cloned().unwrap_or(json!("")),
        "target": params.get("target").cloned().unwrap_or(json!("")),
        "authorizationType": params.get("authorizationType").cloned().unwrap_or(json!("NONE")),
        "authorizerId": params.get("authorizerId").cloned().unwrap_or(Value::Null),
        "apiKeyRequired": params.get("apiKeyRequired").cloned().unwrap_or(json!(false)),
        "modelSelectionExpression": params.get("modelSelectionExpression").cloned().unwrap_or(Value::Null),
        "operationName": params.get("operationName").cloned().unwrap_or(Value::Null),
        "requestModels": params.get("requestModels").cloned().unwrap_or(Value::Null),
        "requestParameters": params.get("requestParameters").cloned().unwrap_or(Value::Null),
        "routeResponseSelectionExpression": params.get("routeResponseSelectionExpression").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.routes, api_id).insert(route_id, route.clone());
    created(201, route)
}

fn get_routes(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.routes, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn get_route(state: &mut State, api_id: &str, route_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_route(state, api_id, route_id, |route| {
        snapshot = Some(route.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Route {route_id} not found")),
    }
}

fn update_route(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    route_id: &str,
) -> Response {
    if let Err(resp) = with_route(state, api_id, route_id, |_| {}) {
        return resp;
    }
    if let Err(resp) = with_route(state, api_id, route_id, |route| {
        let mut patched = route.clone();
        for key in [
            "routeKey",
            "target",
            "authorizationType",
            "authorizerId",
            "apiKeyRequired",
            "operationName",
            "modelSelectionExpression",
            "requestModels",
            "requestParameters",
            "routeResponseSelectionExpression",
        ] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *route = patched;
    }) {
        return resp;
    }
    let snapshot = child_entry(&mut state.routes, api_id)
        .get(route_id)
        .cloned();
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Route {route_id} not found")),
    }
}

fn delete_route(state: &mut State, api_id: &str, route_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    match child_entry(&mut state.routes, api_id).remove(route_id) {
        Some(_) => no_content(),
        None => not_found(&format!("Route {route_id} not found")),
    }
}

// --- route responses -------------------------------------------------------

fn create_route_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    route_id: &str,
) -> Response {
    if let Err(resp) = with_route(state, api_id, route_id, |_| {}) {
        return resp;
    }
    let response_id = create_id();
    let response = json!({
        "routeResponseId": response_id,
        "routeResponseKey": params.get("routeResponseKey").cloned().unwrap_or(json!("$default")),
        "modelSelectionExpression": params.get("modelSelectionExpression").cloned().unwrap_or(Value::Null),
        "responseModels": params.get("responseModels").cloned().unwrap_or(Value::Null),
        "responseParameters": params.get("responseParameters").cloned().unwrap_or(Value::Null),
    });
    state
        .route_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(route_id.to_string())
        .or_default()
        .insert(response_id, response.clone());
    created(201, response)
}

fn get_route_responses(state: &mut State, api_id: &str, route_id: &str) -> Response {
    if let Err(resp) = with_route(state, api_id, route_id, |_| {}) {
        return resp;
    }
    ok(json!({
        "items": state
            .route_responses
            .entry(api_id.to_string())
            .or_default()
            .entry(route_id.to_string())
            .or_default()
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn with_route_response(
    state: &mut State,
    api_id: &str,
    route_id: &str,
    response_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    with_route(state, api_id, route_id, |_| {})?;
    let Some(response) = state
        .route_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(route_id.to_string())
        .or_default()
        .get_mut(response_id)
    else {
        return Err(not_found(&format!(
            "Route response {response_id} not found"
        )));
    };
    f(response);
    Ok(())
}

fn get_route_response(
    state: &mut State,
    api_id: &str,
    route_id: &str,
    response_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_route_response(state, api_id, route_id, response_id, |r| {
        snapshot = Some(r.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Route response not found"),
    }
}

fn update_route_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    route_id: &str,
    response_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_route_response(state, api_id, route_id, response_id, |resp| {
        let mut patched = resp.clone();
        for key in [
            "routeResponseKey",
            "modelSelectionExpression",
            "responseModels",
            "responseParameters",
        ] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *resp = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Route response not found"),
    }
}

fn delete_route_response(
    state: &mut State,
    api_id: &str,
    route_id: &str,
    response_id: &str,
) -> Response {
    if let Err(resp) = with_route_response(state, api_id, route_id, response_id, |_| {}) {
        return resp;
    }
    state
        .route_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(route_id.to_string())
        .or_default()
        .remove(response_id);
    no_content()
}

// --- integrations -----------------------------------------------------------

fn with_integration(
    state: &mut State,
    api_id: &str,
    integration_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.integrations, api_id).get_mut(integration_id) {
        Some(integration) => {
            f(integration);
            Ok(())
        }
        None => Err(not_found(&format!(
            "Integration {integration_id} not found"
        ))),
    }
}

fn create_integration(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let integration_id = create_id();
    let integration = json!({
        "integrationId": integration_id,
        "integrationType": params.get("integrationType").cloned().unwrap_or(json!("AWS_PROXY")),
        "integrationUri": params.get("integrationUri").cloned().unwrap_or(json!("")),
        "integrationMethod": params.get("integrationMethod").cloned().unwrap_or(json!("POST")),
        "payloadFormatVersion": params.get("payloadFormatVersion").cloned().unwrap_or(json!("2.0")),
        "connectionType": params.get("connectionType").cloned().unwrap_or(json!("INTERNET")),
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "timeoutInMillis": params.get("timeoutInMillis").cloned().unwrap_or(json!(30000)),
        "requestParameters": params.get("requestParameters").cloned().unwrap_or(Value::Null),
        "requestTemplates": params.get("requestTemplates").cloned().unwrap_or(Value::Null),
        "responseParameters": params.get("responseParameters").cloned().unwrap_or(Value::Null),
        "templateSelectionExpression": params.get("templateSelectionExpression").cloned().unwrap_or(Value::Null),
        "credentialsArn": params.get("credentialsArn").cloned().unwrap_or(Value::Null),
        "contentHandlingStrategy": params.get("contentHandlingStrategy").cloned().unwrap_or(Value::Null),
        "connectionId": params.get("connectionId").cloned().unwrap_or(Value::Null),
        "integrationSubtype": params.get("integrationSubtype").cloned().unwrap_or(Value::Null),
        "passthroughBehavior": params.get("passthroughBehavior").cloned().unwrap_or(Value::Null),
        "tlsConfig": params.get("tlsConfig").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.integrations, api_id).insert(integration_id, integration.clone());
    created(201, integration)
}

fn get_integrations(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.integrations, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn get_integration(state: &mut State, api_id: &str, integration_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_integration(state, api_id, integration_id, |i| {
        snapshot = Some(i.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Integration not found"),
    }
}

fn update_integration(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    integration_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_integration(state, api_id, integration_id, |integration| {
        let mut patched = integration.clone();
        for key in [
            "integrationType",
            "integrationUri",
            "integrationMethod",
            "payloadFormatVersion",
            "description",
            "timeoutInMillis",
            "requestParameters",
            "requestTemplates",
            "connectionType",
            "connectionId",
            "contentHandlingStrategy",
            "credentialsArn",
            "integrationSubtype",
            "passthroughBehavior",
            "templateSelectionExpression",
            "tlsConfig",
            "responseParameters",
        ] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *integration = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Integration not found"),
    }
}

fn delete_integration(state: &mut State, api_id: &str, integration_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    match child_entry(&mut state.integrations, api_id).remove(integration_id) {
        Some(_) => no_content(),
        None => not_found(&format!("Integration {integration_id} not found")),
    }
}

// --- integration responses --------------------------------------------------

fn create_integration_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    integration_id: &str,
) -> Response {
    if let Err(resp) = with_integration(state, api_id, integration_id, |_| {}) {
        return resp;
    }
    let response_id = create_id();
    let response = json!({
        "integrationResponseId": response_id,
        "integrationResponseKey": params.get("integrationResponseKey").cloned().unwrap_or(json!("$default")),
        "contentHandlingStrategy": params.get("contentHandlingStrategy").cloned().unwrap_or(Value::Null),
        "responseParameters": params.get("responseParameters").cloned().unwrap_or(Value::Null),
        "responseTemplates": params.get("responseTemplates").cloned().unwrap_or(Value::Null),
        "templateSelectionExpression": params.get("templateSelectionExpression").cloned().unwrap_or(Value::Null),
    });
    state
        .integration_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(integration_id.to_string())
        .or_default()
        .insert(response_id, response.clone());
    created(201, response)
}

fn with_integration_response(
    state: &mut State,
    api_id: &str,
    integration_id: &str,
    response_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    with_integration(state, api_id, integration_id, |_| {})?;
    let Some(response) = state
        .integration_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(integration_id.to_string())
        .or_default()
        .get_mut(response_id)
    else {
        return Err(not_found(&format!(
            "Integration response {response_id} not found"
        )));
    };
    f(response);
    Ok(())
}

fn get_integration_responses(state: &mut State, api_id: &str, integration_id: &str) -> Response {
    if let Err(resp) = with_integration(state, api_id, integration_id, |_| {}) {
        return resp;
    }
    ok(json!({
        "items": state
            .integration_responses
            .entry(api_id.to_string())
            .or_default()
            .entry(integration_id.to_string())
            .or_default()
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn get_integration_response(
    state: &mut State,
    api_id: &str,
    integration_id: &str,
    response_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_integration_response(state, api_id, integration_id, response_id, |r| {
        snapshot = Some(r.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Integration response not found"),
    }
}

fn update_integration_response(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    integration_id: &str,
    response_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) =
        with_integration_response(state, api_id, integration_id, response_id, |resp| {
            let mut patched = resp.clone();
            for key in [
                "integrationResponseKey",
                "contentHandlingStrategy",
                "responseParameters",
                "responseTemplates",
                "templateSelectionExpression",
            ] {
                if let Some(value) = param(params, capitalized(key).as_str()) {
                    if let Some(map) = patched.as_object_mut() {
                        map.insert(key.to_string(), value.clone());
                    }
                }
            }
            *resp = patched.clone();
            snapshot = Some(patched);
        })
    {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Integration response not found"),
    }
}

fn delete_integration_response(
    state: &mut State,
    api_id: &str,
    integration_id: &str,
    response_id: &str,
) -> Response {
    if let Err(resp) = with_integration_response(state, api_id, integration_id, response_id, |_| {})
    {
        return resp;
    }
    state
        .integration_responses
        .entry(api_id.to_string())
        .or_default()
        .entry(integration_id.to_string())
        .or_default()
        .remove(response_id);
    no_content()
}

// ---------------------------------------------------------------------------
// Handlers: stages / authorizers / deployments / models
// ---------------------------------------------------------------------------

fn with_stage(
    state: &mut State,
    api_id: &str,
    stage_name: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.stages, api_id).get_mut(stage_name) {
        Some(stage) => {
            f(stage);
            Ok(())
        }
        None => Err(not_found(&format!("Stage {stage_name} not found"))),
    }
}

fn create_stage(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let stage_name = body_str(params, "stageName")
        .unwrap_or("$default")
        .to_string();
    if child_entry(&mut state.stages, api_id).contains_key(&stage_name) {
        return v2_error(
            409,
            "ConflictException",
            &format!("Stage {stage_name} already exists"),
        );
    }
    let stage = json!({
        "stageName": stage_name,
        "autoDeploy": params.get("autoDeploy").cloned().unwrap_or(json!(false)),
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "stageVariables": params.get("stageVariables").cloned().unwrap_or(json!({})),
        "deploymentId": params.get("deploymentId").cloned().unwrap_or(json!("")),
        "defaultRouteSettings": params.get("defaultRouteSettings").cloned().unwrap_or(json!({})),
        "routeSettings": params.get("routeSettings").cloned().unwrap_or(json!({})),
        "accessLogSettings": params.get("accessLogSettings").cloned().unwrap_or(Value::Null),
        "tags": params.get("tags").cloned().unwrap_or(json!({})),
        "createdDate": unix_iso(),
        "lastUpdatedDate": unix_iso(),
    });
    child_entry(&mut state.stages, api_id).insert(stage_name, stage.clone());
    created(201, stage)
}

fn get_stages(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.stages, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn get_stage(state: &mut State, api_id: &str, stage_name: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_stage(state, api_id, stage_name, |stage| {
        snapshot = Some(stage.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Stage {stage_name} not found")),
    }
}

fn update_stage(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    stage_name: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_stage(state, api_id, stage_name, |stage| {
        let mut patched = stage.clone();
        for key in [
            "autoDeploy",
            "description",
            "stageVariables",
            "deploymentId",
            "defaultRouteSettings",
            "routeSettings",
            "accessLogSettings",
            "tags",
            "clientCertificateId",
        ] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        patched["lastUpdatedDate"] = json!(unix_iso());
        *stage = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Stage {stage_name} not found")),
    }
}

fn delete_stage(state: &mut State, api_id: &str, stage_name: &str) -> Response {
    if let Err(resp) = require_stage(state, api_id, stage_name) {
        return resp;
    }
    child_entry(&mut state.stages, api_id).remove(stage_name);
    no_content()
}

// --- authorizers ------------------------------------------------------------

fn create_authorizer(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let authorizer_id = create_id();
    let authorizer = json!({
        "authorizerId": authorizer_id,
        "authorizerType": params.get("authorizerType").cloned().unwrap_or(json!("JWT")),
        "name": params.get("name").cloned().unwrap_or(json!("")),
        "identitySource": params.get("identitySource").cloned().unwrap_or(json!("$request.header.Authorization")),
        "jwtConfiguration": params.get("jwtConfiguration").cloned().unwrap_or(Value::Null),
        "authorizerUri": params.get("authorizerUri").cloned().unwrap_or(Value::Null),
        "authorizerCredentialsArn": params.get("authorizerCredentialsArn").cloned().unwrap_or(Value::Null),
        "authorizerResultTtlInSeconds": params.get("authorizerResultTtlInSeconds").cloned().unwrap_or(json!(300)),
        "authorizerPayloadFormatVersion": params.get("authorizerPayloadFormatVersion").cloned().unwrap_or(Value::Null),
        "enableSimpleResponses": params.get("enableSimpleResponses").cloned().unwrap_or(json!(false)),
        "identityValidationExpression": params.get("identityValidationExpression").cloned().unwrap_or(Value::Null),
    });
    child_entry(&mut state.authorizers, api_id).insert(authorizer_id, authorizer.clone());
    created(201, authorizer)
}

fn get_authorizers(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.authorizers, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn with_authorizer(
    state: &mut State,
    api_id: &str,
    authorizer_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.authorizers, api_id).get_mut(authorizer_id) {
        Some(authorizer) => {
            f(authorizer);
            Ok(())
        }
        None => Err(not_found(&format!("Authorizer {authorizer_id} not found"))),
    }
}

fn get_authorizer(state: &mut State, api_id: &str, authorizer_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_authorizer(state, api_id, authorizer_id, |a| {
        snapshot = Some(a.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Authorizer not found"),
    }
}

fn update_authorizer(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    authorizer_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_authorizer(state, api_id, authorizer_id, |authorizer| {
        let mut patched = authorizer.clone();
        for key in [
            "authorizerType",
            "name",
            "identitySource",
            "jwtConfiguration",
            "authorizerUri",
            "authorizerResultTtlInSeconds",
            "authorizerCredentialsArn",
            "authorizerPayloadFormatVersion",
            "enableSimpleResponses",
            "identityValidationExpression",
        ] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *authorizer = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Authorizer not found"),
    }
}

fn delete_authorizer(state: &mut State, api_id: &str, authorizer_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    match child_entry(&mut state.authorizers, api_id).remove(authorizer_id) {
        Some(_) => no_content(),
        None => not_found(&format!("Authorizer {authorizer_id} not found")),
    }
}

// --- deployments ------------------------------------------------------------

fn create_deployment(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let deployment_id = create_id();
    let deployment = json!({
        "deploymentId": deployment_id,
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "deploymentStatus": "DEPLOYED",
        "stageName": params.get("stageName").cloned().unwrap_or(Value::Null),
        "createdDate": unix_iso(),
        "autoDeployed": false,
    });
    // py: when a StageName is given, point that stage at this deployment.
    if let Some(stage_name) = body_str(params, "stageName") {
        if let Some(stage) = child_entry(&mut state.stages, api_id).get_mut(stage_name) {
            stage["deploymentId"] = json!(deployment_id.clone());
        }
    }
    child_entry(&mut state.deployments, api_id).insert(deployment_id, deployment.clone());
    created(201, deployment)
}

fn get_deployments(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.deployments, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn with_deployment(
    state: &mut State,
    api_id: &str,
    deployment_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.deployments, api_id).get_mut(deployment_id) {
        Some(deployment) => {
            f(deployment);
            Ok(())
        }
        None => Err(not_found(&format!("Deployment {deployment_id} not found"))),
    }
}

fn get_deployment(state: &mut State, api_id: &str, deployment_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_deployment(state, api_id, deployment_id, |d| {
        snapshot = Some(d.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Deployment not found"),
    }
}

fn update_deployment(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    deployment_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_deployment(state, api_id, deployment_id, |deployment| {
        if let Some(desc) = param(params, "Description") {
            deployment["description"] = desc.clone();
        }
        snapshot = Some(deployment.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("Deployment not found"),
    }
}

fn delete_deployment(state: &mut State, api_id: &str, deployment_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    match child_entry(&mut state.deployments, api_id).remove(deployment_id) {
        Some(_) => no_content(),
        None => not_found(&format!("Deployment {deployment_id} not found")),
    }
}

// --- models -----------------------------------------------------------------

fn create_model(state: &mut State, params: &Map<String, Value>, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let model_id = create_id();
    let model = json!({
        "modelId": model_id,
        "contentType": params.get("contentType").cloned().unwrap_or(json!("application/json")),
        "description": params.get("description").cloned().unwrap_or(json!("")),
        "name": params.get("name").cloned().unwrap_or(json!("")),
        "schema": params.get("schema").cloned().unwrap_or(json!("")),
    });
    child_entry(&mut state.models, api_id).insert(model_id, model.clone());
    created(201, model)
}

fn get_models(state: &mut State, api_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    ok(json!({
        "items": child_entry(&mut state.models, api_id)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn with_model(
    state: &mut State,
    api_id: &str,
    model_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    require_api(state, api_id)?;
    match child_entry(&mut state.models, api_id).get_mut(model_id) {
        Some(model) => {
            f(model);
            Ok(())
        }
        None => Err(not_found(&format!("Model {model_id} not found"))),
    }
}

fn get_model(state: &mut State, api_id: &str, model_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_model(state, api_id, model_id, |model| {
        snapshot = Some(model.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Model {model_id} not found")),
    }
}

fn update_model(
    state: &mut State,
    params: &Map<String, Value>,
    api_id: &str,
    model_id: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_model(state, api_id, model_id, |model| {
        let mut patched = model.clone();
        for key in ["contentType", "description", "name", "schema"] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *model = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("Model {model_id} not found")),
    }
}

fn delete_model(state: &mut State, api_id: &str, model_id: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    match child_entry(&mut state.models, api_id).remove(model_id) {
        Some(_) => no_content(),
        None => not_found(&format!("Model {model_id} not found")),
    }
}

/// GetModelTemplate: value = the model schema (absent model → NotFound).
fn get_model_template(state: &mut State, api_id: &str, model_id: &str) -> Response {
    let mut schema: Option<Value> = None;
    if let Err(resp) = with_model(state, api_id, model_id, |model| {
        schema = model.get("schema").cloned();
    }) {
        return resp;
    }
    ok(json!({ "value": schema.unwrap_or(json!("")) }))
}

// ---------------------------------------------------------------------------
// Handlers: domains / api mappings / vpc links / export / tags
// ---------------------------------------------------------------------------

fn with_domain(
    state: &mut State,
    domain_name: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    match state.domain_names.get_mut(domain_name) {
        Some(domain) => {
            f(domain);
            Ok(())
        }
        None => Err(not_found(
            "The domain name resource specified in the request was not found.",
        )),
    }
}

fn create_domain_name(state: &mut State, params: &Map<String, Value>, region: &str) -> Response {
    let domain_name = body_str(params, "domainName")
        .unwrap_or_default()
        .to_string();
    if domain_name.is_empty() {
        return bad_request("Invalid Domain name specified");
    }
    let domain = json!({
        "domainName": domain_name,
        "domainNameConfigurations": params.get("domainNameConfigurations").cloned().unwrap_or(json!([])),
        "mutualTlsAuthentication": params.get("mutualTlsAuthentication").cloned().unwrap_or(Value::Null),
        "tags": params.get("tags").cloned().unwrap_or(json!({})),
        "apiMappingSelectionExpression": "$request.basepath",
        "domainNameArn": format!("arn:aws:apigateway:{region}::/domainnames/{domain_name}"),
    });
    state.domain_names.insert(domain_name, domain.clone());
    created(201, domain)
}

fn get_domain_names(state: &mut State) -> Response {
    ok(json!({
        "items": state.domain_names.values().cloned().collect::<Vec<Value>>()
    }))
}

fn get_domain_name(state: &mut State, domain_name: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_domain(state, domain_name, |domain| {
        snapshot = Some(domain.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("The domain name resource specified in the request was not found."),
    }
}

fn update_domain_name(
    state: &mut State,
    params: &Map<String, Value>,
    domain_name: &str,
) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_domain(state, domain_name, |domain| {
        let mut patched = domain.clone();
        for key in ["domainNameConfigurations", "mutualTlsAuthentication"] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *domain = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("The domain name resource specified in the request was not found."),
    }
}

fn delete_domain_name(state: &mut State, domain_name: &str) -> Response {
    match state.domain_names.remove(domain_name) {
        Some(_) => {
            state.api_mappings.retain(|domain, _| domain != domain_name);
            no_content()
        }
        None => not_found("The domain name resource specified in the request was not found."),
    }
}

// --- api mappings ------------------------------------------------------------

fn create_api_mapping(
    state: &mut State,
    params: &Map<String, Value>,
    domain_name: &str,
) -> Response {
    if let Err(resp) = with_domain(state, domain_name, |_| {}) {
        return resp;
    }
    let mapping_id = create_id();
    let mapping = json!({
        "apiMappingId": mapping_id,
        "apiId": params.get("apiId").cloned().unwrap_or(json!("")),
        "apiMappingKey": params.get("apiMappingKey").cloned().unwrap_or(json!("")),
        "stage": params.get("stage").cloned().unwrap_or(json!("")),
    });
    child_entry(&mut state.api_mappings, domain_name).insert(mapping_id, mapping.clone());
    created(201, mapping)
}

fn get_api_mappings(state: &mut State, domain_name: &str) -> Response {
    // The mapping list doesn't verify the domain (py semantics).
    ok(json!({
        "items": child_entry(&mut state.api_mappings, domain_name)
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    }))
}

fn with_api_mapping(
    state: &mut State,
    domain_name: &str,
    mapping_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    match child_entry(&mut state.api_mappings, domain_name).get_mut(mapping_id) {
        Some(mapping) => {
            f(mapping);
            Ok(())
        }
        None => Err(not_found(
            "The api mapping resource specified in the request was not found.",
        )),
    }
}

fn get_api_mapping(state: &mut State, domain_name: &str, mapping_id: &str) -> Response {
    if !state.domain_names.contains_key(domain_name) {
        return not_found("The domain name resource specified in the request was not found.");
    }
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_api_mapping(state, domain_name, mapping_id, |mapping| {
        snapshot = Some(mapping.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("The api mapping resource specified in the request was not found."),
    }
}

fn update_api_mapping(
    state: &mut State,
    params: &Map<String, Value>,
    domain_name: &str,
    mapping_id: &str,
) -> Response {
    if !state.domain_names.contains_key(domain_name) {
        return not_found("The domain name resource specified in the request was not found.");
    }
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_api_mapping(state, domain_name, mapping_id, |mapping| {
        let mut patched = mapping.clone();
        for key in ["apiId", "apiMappingKey", "stage"] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *mapping = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found("The api mapping resource specified in the request was not found."),
    }
}

fn delete_api_mapping(state: &mut State, domain_name: &str, mapping_id: &str) -> Response {
    if let Err(resp) = with_api_mapping(state, domain_name, mapping_id, |_| {}) {
        return resp;
    }
    child_entry(&mut state.api_mappings, domain_name).remove(mapping_id);
    no_content()
}

// --- vpc links ---------------------------------------------------------------

fn create_vpc_link(state: &mut State, params: &Map<String, Value>) -> Response {
    let vpc_link_id = create_id();
    let vpc_link = json!({
        "vpcLinkId": vpc_link_id,
        "name": params.get("name").cloned().unwrap_or(json!("")),
        "subnetIds": params.get("subnetIds").cloned().unwrap_or(json!([])),
        "securityGroupIds": params.get("securityGroupIds").cloned().unwrap_or(json!([])),
        "vpcLinkStatus": "AVAILABLE",
        "vpcLinkStatusMessage": "VPC link is ready to route traffic",
        "vpcLinkVersion": "V2",
        "tags": params.get("tags").cloned().unwrap_or(json!({})),
        "createdDate": unix_iso(),
    });
    state.vpc_links.insert(vpc_link_id, vpc_link.clone());
    created(201, vpc_link)
}

fn get_vpc_links(state: &mut State) -> Response {
    ok(json!({
        "items": state.vpc_links.values().cloned().collect::<Vec<Value>>()
    }))
}

fn with_vpc_link(
    state: &mut State,
    vpc_link_id: &str,
    f: impl FnOnce(&mut Value),
) -> Result<(), Response> {
    match state.vpc_links.get_mut(vpc_link_id) {
        Some(link) => {
            f(link);
            Ok(())
        }
        None => Err(not_found(&format!("VPC link {vpc_link_id} not found"))),
    }
}

fn get_vpc_link(state: &mut State, vpc_link_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_vpc_link(state, vpc_link_id, |link| {
        snapshot = Some(link.clone());
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("VPC link {vpc_link_id} not found")),
    }
}

fn update_vpc_link(state: &mut State, params: &Map<String, Value>, vpc_link_id: &str) -> Response {
    let mut snapshot: Option<Value> = None;
    if let Err(resp) = with_vpc_link(state, vpc_link_id, |link| {
        let mut patched = link.clone();
        for key in ["name", "securityGroupIds"] {
            if let Some(value) = param(params, capitalized(key).as_str()) {
                if let Some(map) = patched.as_object_mut() {
                    map.insert(key.to_string(), value.clone());
                }
            }
        }
        *link = patched.clone();
        snapshot = Some(patched);
    }) {
        return resp;
    }
    match snapshot {
        Some(value) => ok(value),
        None => not_found(&format!("VPC link {vpc_link_id} not found")),
    }
}

fn delete_vpc_link(state: &mut State, vpc_link_id: &str) -> Response {
    match state.vpc_links.remove(vpc_link_id) {
        Some(_) => no_content(),
        None => not_found(&format!("VPC link {vpc_link_id} not found")),
    }
}

// --- export -----------------------------------------------------------------

/// ExportApi: minimal OAS export built from routes (fake api → NotFound).
fn export_api(state: &mut State, api_id: &str, specification: &str) -> Response {
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    if specification != "JSON" && specification != "YAML" {
        return bad_request("Invalid specification. Must be one of [JSON, YAML]");
    }
    if let Err(resp) = require_api(state, api_id) {
        return resp;
    }
    let snapshot = state.apis.get(api_id).cloned();
    let Some(api) = snapshot else {
        return not_found(&format!("API {api_id} not found"));
    };
    let mut paths: BTreeMap<String, Value> = BTreeMap::new();
    if let Some(routes) = state.routes.get(api_id) {
        for route in routes.values() {
            let Some((method, path)) = route
                .get("routeKey")
                .and_then(Value::as_str)
                .and_then(|key| key.split_once(' '))
                .map(|(m, p)| (m.to_lowercase(), p.to_string()))
            else {
                continue;
            };
            let entry = paths.entry(path).or_insert_with(|| json!({}));
            if let Some(path_map) = entry.as_object_mut() {
                path_map.insert(
                    method,
                    json!({ "responses": { "200": { "description": "OK" } } }),
                );
            }
        }
    }
    let doc = json!({
        "openapi": "3.0.1",
        "info": {
            "title": api.get("name").and_then(Value::as_str).unwrap_or(""),
            "version": api.get("version").and_then(Value::as_str).unwrap_or("1.0"),
        },
        "paths": paths,
    });
    let mut response = json_resp(200, doc);
    response.headers = vec![(
        "Content-Type".to_string(),
        "application/octet-stream".to_string(),
    )];
    response
}

// --- tags -------------------------------------------------------------------

/// GetTags / TagResource / UntagResource on `/v2/tags/{arn}`; tags live on
/// the api (ARNs pointing at `/apis/{id}`, the shape the suite exercises).
fn tags_route(req: &Request) -> Response {
    let raw = req.path.split_once("/tags/").map(|(_, v)| v).unwrap_or("");
    let arn = crate::proto::url_decode(raw);
    with_state(req.account, &req.region, move |state| {
        let api_id = api_id_from_arn(&arn);
        let api = api_id.as_ref().and_then(|id| state.apis.get_mut(id));
        let Some(api) = api else {
            return ok(json!({ "tags": {} }));
        };
        match req.method.as_str() {
            "POST" => {
                let params = body_object(&req.body);
                if let Some(tags) = param(&params, "Tags").and_then(Value::as_object).cloned() {
                    if let Some(map) = api["tags"].as_object_mut() {
                        for (k, v) in tags {
                            map.insert(k, v);
                        }
                    }
                }
                ok(json!({}))
            }
            "DELETE" => {
                let map = crate::proto::query_map(&req.query_string);
                let keys = map.get("tagKeys").cloned().unwrap_or_default();
                if let Some(tags) = api["tags"].as_object_mut() {
                    for key in keys {
                        tags.remove(&key);
                    }
                }
                no_content()
            }
            "GET" => ok(json!({ "tags": api["tags"].clone() })),
            _ => route_unknown(req.method.as_str(), req.path.as_str()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_ctx() -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(281111111110);
        NEXT.fetch_add(1, Ordering::SeqCst)
    }

    fn req(method: &str, path: &str, body: Value, account: u64) -> Request {
        Request {
            service: "apigatewayv2".into(),
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
    fn api_crud_and_routes() {
        let acct = fresh_ctx();
        let created = parsed(&dispatch(&req(
            "POST",
            "/v2/apis",
            json!({"protocolType": "HTTP", "name": "v2-api"}),
            acct,
        )));
        assert_eq!(created["protocolType"], "HTTP");
        assert_eq!(created["name"], "v2-api");
        let api_id = created["apiId"].as_str().unwrap().to_string();
        assert_eq!(
            created["apiEndpoint"],
            format!("https://{api_id}.execute-api.us-east-1.amazonaws.com")
        );

        let route = parsed(&dispatch(&req(
            "POST",
            &format!("/v2/apis/{api_id}/routes"),
            json!({"routeKey": "GET /test"}),
            acct,
        )));
        assert_eq!(route["routeKey"], "GET /test");
        let route_id = route["routeId"].as_str().unwrap().to_string();

        let integration = parsed(&dispatch(&req(
            "POST",
            &format!("/v2/apis/{api_id}/integrations"),
            json!({"integrationType": "AWS_PROXY", "integrationUri": "arn:aws:lambda:us-east-1:123456789012:function:fn"}),
            acct,
        )));
        assert_eq!(integration["integrationType"], "AWS_PROXY");

        let stage = parsed(&dispatch(&req(
            "POST",
            &format!("/v2/apis/{api_id}/stages"),
            json!({"stageName": "dev", "stageVariables": {"env": "stg"}}),
            acct,
        )));
        assert_eq!(stage["stageVariables"]["env"], "stg");

        // Update round-trip.
        let updated = parsed(&dispatch(&req(
            "PATCH",
            &format!("/v2/apis/{api_id}"),
            json!({"name": "renamed"}),
            acct,
        )));
        assert_eq!(updated["name"], "renamed");

        // Tags by ARN.
        dispatch(&req(
            "POST",
            &format!("/v2/tags/arn%3Aaws%3Aapigateway%3Aus-east-1%3A%3A%2Fapis%2F{api_id}"),
            json!({"tags": {"env": "test"}}),
            acct,
        ));
        let tags = parsed(&dispatch(&req(
            "GET",
            &format!("/v2/tags/arn%3Aaws%3Aapigateway%3Aus-east-1%3A%3A%2Fapis%2F{api_id}"),
            json!({}),
            acct,
        )));
        assert_eq!(tags["tags"]["env"], "test");

        dispatch(&req(
            "DELETE",
            &format!("/v2/apis/{api_id}"),
            json!({}),
            acct,
        ));
        dispatch(&req(
            "DELETE",
            &format!("/v2/apis/{api_id}/routes/{route_id}"),
            json!({}),
            acct,
        ));
        let list = parsed(&dispatch(&req("GET", "/v2/apis", json!({}), acct)));
        assert!(list["items"].as_array().is_some_and(|a| a.is_empty()));
    }

    #[test]
    fn unknown_resources_not_found() {
        let acct = fresh_ctx();
        let get_api = dispatch(&req("GET", "/v2/apis/ghost", json!({}), acct));
        assert_eq!(get_api.status, 404);
        let body = parsed(&get_api);
        assert_eq!(body["Code"], "NotFoundException");
        assert_eq!(
            dispatch(&req(
                "DELETE",
                "/v2/apis/ghost/deployments/dep",
                json!({}),
                acct
            ))
            .status,
            404
        );
        assert_eq!(
            dispatch(&req(
                "DELETE",
                "/v2/apis/ghost/stages/stage/accesslogsettings",
                json!({}),
                acct
            ))
            .status,
            404
        );
        assert_eq!(
            dispatch(&req("GET", "/v2/apis/ghost/exports/JSON", json!({}), acct)).status,
            404
        );
        // Unmodeled portal surfaces answer NotFound like the py provider.
        assert_eq!(
            dispatch(&req("GET", "/v2/portals/p1", json!({}), acct)).status,
            404
        );
        assert_eq!(
            dispatch(&req("DELETE", "/v2/apis/ghost/cors", json!({}), acct)).status,
            404
        );
    }

    #[test]
    fn deployment_points_stage_and_vpclinks() {
        let acct = fresh_ctx();
        let api_id = parsed(&dispatch(&req(
            "POST",
            "/v2/apis",
            json!({"protocolType": "HTTP", "name": "dep-api"}),
            acct,
        )))["apiId"]
            .as_str()
            .unwrap()
            .to_string();
        dispatch(&req(
            "POST",
            &format!("/v2/apis/{api_id}/stages"),
            json!({"stageName": "prod"}),
            acct,
        ));
        let dep = parsed(&dispatch(&req(
            "POST",
            &format!("/v2/apis/{api_id}/deployments"),
            json!({"stageName": "prod"}),
            acct,
        )));
        assert_eq!(dep["deploymentStatus"], "DEPLOYED");
        let stage = parsed(&dispatch(&req(
            "GET",
            &format!("/v2/apis/{api_id}/stages/prod"),
            json!({}),
            acct,
        )));
        assert_eq!(stage["deploymentId"], dep["deploymentId"]);

        let link = parsed(&dispatch(&req(
            "POST",
            "/v2/vpclinks",
            json!({"name": "lnk", "subnetIds": ["subnet-1"]}),
            acct,
        )));
        assert_eq!(link["vpcLinkStatus"], "AVAILABLE");
        let link_id = link["vpcLinkId"].as_str().unwrap().to_string();
        let renamed = parsed(&dispatch(&req(
            "PATCH",
            &format!("/v2/vpclinks/{link_id}"),
            json!({"name": "renamed-link"}),
            acct,
        )));
        assert_eq!(renamed["name"], "renamed-link");
        dispatch(&req(
            "DELETE",
            &format!("/v2/vpclinks/{link_id}"),
            json!({}),
            acct,
        ));
        assert_eq!(
            dispatch(&req(
                "GET",
                &format!("/v2/vpclinks/{link_id}"),
                json!({}),
                acct
            ))
            .status,
            404
        );
    }
}
