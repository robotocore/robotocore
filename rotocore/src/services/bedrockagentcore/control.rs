//! Bedrock AgentCore control plane — `bedrock-agentcore-control` handlers.
//!
//! Field-for-field ports of moto 5.2.4's `bedrockagentcorecontrol`
//! responses.py/models.py for the runtime/endpoint/gateway/target/memory
//! families, plus the code-interpreter CRUD the moto fork does not implement
//! (modeled from botocore's shapes; see the module header in `super` for the
//! exceeds-moto rationale). A missing required member on the moto-parity
//! families reproduces the fork's `params[key]` KeyError crash page; the
//! code-interpreter family answers a modeled ValidationException instead.
//! Unmodeled control operations answer the generic modeled 501.

use super::*;
use crate::proto::query_map;

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub(crate) fn route(req: &Request, op: &str, st: &mut AgentCoreStore) -> Response {
    let body = parse_body(req);
    let account = req.account;
    let region = req.region.clone();
    match op {
        "CreateCodeInterpreter" => create_code_interpreter(st, &region, account, &body),
        "GetCodeInterpreter" => get_code_interpreter(req, st, &region, account),
        "DeleteCodeInterpreter" => delete_code_interpreter(req, st),
        "ListCodeInterpreters" => list_code_interpreters(req, st, &region, account),
        "CreateAgentRuntime" => create_agent_runtime(st, &region, account, &body),
        "GetAgentRuntime" => get_agent_runtime(req, st),
        "UpdateAgentRuntime" => update_agent_runtime(req, st, &body),
        "DeleteAgentRuntime" => delete_agent_runtime(req, st),
        "ListAgentRuntimes" => list_agent_runtimes(st),
        "ListAgentRuntimeVersions" => list_agent_runtime_versions(req, st),
        "CreateAgentRuntimeEndpoint" => create_agent_runtime_endpoint(req, st, &body),
        "GetAgentRuntimeEndpoint" => get_agent_runtime_endpoint(req, st),
        "UpdateAgentRuntimeEndpoint" => update_agent_runtime_endpoint(req, st, &body),
        "DeleteAgentRuntimeEndpoint" => delete_agent_runtime_endpoint(req, st),
        "ListAgentRuntimeEndpoints" => list_agent_runtime_endpoints(req, st),
        "CreateGateway" => create_gateway(st, &region, account, &body),
        "GetGateway" => get_gateway(req, st),
        "UpdateGateway" => update_gateway(req, st, &body),
        "DeleteGateway" => delete_gateway(req, st),
        "ListGateways" => list_gateways(st),
        "CreateGatewayTarget" => create_gateway_target(req, st, &body),
        "GetGatewayTarget" => get_gateway_target(req, st),
        "UpdateGatewayTarget" => update_gateway_target(req, st, &body),
        "DeleteGatewayTarget" => delete_gateway_target(req, st),
        "ListGatewayTargets" => list_gateway_targets(req, st),
        "CreateMemory" => create_memory(st, &region, account, &body),
        "GetMemory" => get_memory(req, st),
        "UpdateMemory" => update_memory(req, st, &body),
        "DeleteMemory" => delete_memory(req, st),
        "ListMemories" => list_memories(st),
        other => not_implemented(other),
    }
}

/// Control-plane tag surface (moto's `TaggingService`, no ARN validation —
/// the fork's `tag_resource` stores whatever ARN it is given).
pub(crate) fn route_tags(req: &Request, op: &str, st: &mut AgentCoreStore) -> Response {
    let body = parse_body(req);
    let arn = req.path.trim_start_matches("/tags/").to_string();
    match op {
        "TagResource" => {
            if let Some(resp) = require(&body, &["tags"]) {
                return resp;
            }
            if let Some(tags) = body.get("tags").and_then(Value::as_object) {
                let row = st.tags.entry(arn).or_default();
                for (key, value) in tags {
                    row.insert(key.clone(), value.clone());
                }
            }
            ok(json!({}))
        }
        "UntagResource" => {
            // botocore puts tagKeys on the querystring for this op (shape's
            // location) — moto reads `self._get_param("tagKeys", [])`.
            let keys = query_map(&req.query_string)
                .get("tagKeys")
                .cloned()
                .unwrap_or_default();
            if let Some(row) = st.tags.get_mut(&arn) {
                for key in keys {
                    row.remove(&key);
                }
            }
            ok(json!({}))
        }
        "ListTagsForResource" => {
            let tags = st
                .tags
                .get(&arn)
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<serde_json::Map<String, Value>>()
                })
                .unwrap_or_default();
            ok(json!({ "tags": tags }))
        }
        other => not_implemented(other),
    }
}

// ---------------------------------------------------------------------------
// required-member crash mirror (moto's `params[key]` KeyError → 500 page)
// ---------------------------------------------------------------------------

fn require(body: &Value, keys: &[&str]) -> Option<Response> {
    for key in keys {
        if !body.get(key).map(|v| !v.is_null()).unwrap_or(false) {
            return Some(crate::proto::moto_crash_response());
        }
    }
    None
}

fn missing(kind: &str, id: &str) -> Response {
    not_found(&format!("Could not find {kind} with ID {id}"))
}

/// moto's ManagedState default "immediate" progression on a status read.
fn tick_ready<T: CanTick>(record: &mut T) {
    record.tick();
}

trait CanTick {
    fn tick(&mut self);
}

macro_rules! impl_tick {
    ($($t:ty),*) => {$(
        impl CanTick for $t {
            fn tick(&mut self) {
                advance(&mut self.status, "READY");
            }
        }
    )*};
}
impl_tick!(
    AgentRuntime,
    AgentRuntimeEndpoint,
    Gateway,
    GatewayTarget,
    CodeInterpreter
);

impl CanTick for Memory {
    /// Memories transition CREATING → ACTIVE (moto's memory transitions).
    fn tick(&mut self) {
        advance(&mut self.status, "ACTIVE");
    }
}

// ---------------------------------------------------------------------------
// code interpreters (exceeds-moto set, modeled from botocore shapes)
// ---------------------------------------------------------------------------

/// The pre-provisioned SYSTEM interpreter id (botocore's id enum).
const SYSTEM_INTERPRETER: &str = "aws.codeinterpreter.v1";

fn ensure_system_interpreter(st: &mut AgentCoreStore) {
    if !st.code_interpreters.contains_key(SYSTEM_INTERPRETER) {
        st.code_interpreters
            .insert(SYSTEM_INTERPRETER.into(), CodeInterpreter::system());
    }
}

fn ci_not_found(id: &str) -> Response {
    missing("Code Interpreter", id)
}

fn create_code_interpreter(
    st: &mut AgentCoreStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    if !body.get("name").map(|v| !v.is_null()).unwrap_or(false) {
        return validation("name is a required parameter");
    }
    let network_mode = body
        .get("networkConfiguration")
        .and_then(|c| c.get("networkMode"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if !matches!(network_mode, "PUBLIC" | "SANDBOX" | "VPC") {
        return validation("networkMode must be one of PUBLIC, SANDBOX, VPC");
    }
    let record = CodeInterpreter::new(body);
    let arn = record.ci_arn(region, account);
    // moto's TaggingService: create-time tags land in the tag store.
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            let row = st.tags.entry(arn.clone()).or_default();
            for (key, value) in tags {
                row.insert(key.clone(), value.clone());
            }
        }
    }
    let out = json!({
        "codeInterpreterId": record.id,
        "codeInterpreterArn": arn,
        "createdAt": record.created_at,
        "status": record.status,
    });
    st.code_interpreters.insert(record.id.clone(), record);
    ok(out)
}

fn get_code_interpreter(
    req: &Request,
    st: &mut AgentCoreStore,
    region: &str,
    account: u64,
) -> Response {
    let id = seg_from_end(req.path.as_str(), 1);
    ensure_system_interpreter(st);
    let Some(record) = st.code_interpreters.get_mut(&id) else {
        return ci_not_found(&id);
    };
    tick_ready(record);
    ok(json!({ "codeInterpreter": record.to_dict(region, account) }))
}

fn delete_code_interpreter(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = seg_from_end(req.path.as_str(), 1);
    if id == SYSTEM_INTERPRETER {
        return conflict("The system code interpreter cannot be deleted");
    }
    if st.code_interpreters.remove(&id).is_none() {
        return ci_not_found(&id);
    }
    ok_status(
        202,
        json!({
            "codeInterpreterId": id,
            "status": "DELETING",
            "lastUpdatedAt": now(),
        }),
    )
}

fn list_code_interpreters(
    req: &Request,
    st: &mut AgentCoreStore,
    region: &str,
    account: u64,
) -> Response {
    ensure_system_interpreter(st);
    let query = query_map(&req.query_string);
    let type_filter = query_first_str(&query, "type");
    let max_results = query_first_str(&query, "maxResults").and_then(|m| m.parse::<usize>().ok());
    let token_id = query_first_str(&query, "nextToken").and_then(decode_token);

    // The immediate status read applies to list consultations too.
    for record in st.code_interpreters.values_mut() {
        tick_ready(record);
    }
    let summaries: Vec<Value> = st
        .code_interpreters
        .values()
        .filter(|ci| match type_filter {
            Some("SYSTEM") => ci.is_system,
            Some("CUSTOM") => !ci.is_system,
            _ => true,
        })
        .map(|ci| ci.to_summary(region, account))
        .collect();
    let (page, next_token) = id_page(summaries, "codeInterpreterId", max_results, token_id);
    ok(json!({
        "codeInterpreterSummaries": page,
        "nextToken": next_token,
    }))
}

fn query_first_str<'a>(
    query: &'a std::collections::BTreeMap<String, Vec<String>>,
    key: &str,
) -> Option<&'a str> {
    query.get(key)?.first().map(String::as_str)
}

// ---------------------------------------------------------------------------
// agent runtimes (moto parity)
// ---------------------------------------------------------------------------

fn create_agent_runtime(
    st: &mut AgentCoreStore,
    region: &str,
    account: u64,
    body: &Value,
) -> Response {
    if let Some(resp) = require(
        body,
        &[
            "agentRuntimeName",
            "agentRuntimeArtifact",
            "roleArn",
            "networkConfiguration",
        ],
    ) {
        return resp;
    }
    let runtime = AgentRuntime::new(region, account, body);
    let out = json!({
        "agentRuntimeArn": runtime.runtime_arn(),
        "workloadIdentityDetails": {
            "workloadIdentityArn": runtime.workload_identity_arn()
        },
        "agentRuntimeId": runtime.agent_runtime_id,
        "agentRuntimeVersion": runtime.agent_runtime_version,
        "createdAt": runtime.created_at,
        "status": "CREATING",
    });
    st.agent_runtimes
        .insert(runtime.agent_runtime_id.clone(), runtime);
    ok(out)
}

fn get_agent_runtime(req: &Request, st: &mut AgentCoreStore) -> Response {
    // GET/PUT/DELETE /runtimes/{agentRuntimeId}/
    let id = seg_from_end(req.path.as_str(), 1);
    let Some(runtime) = st.agent_runtimes.get_mut(&id) else {
        return missing("Agent Runtime", &id);
    };
    tick_ready(runtime);
    ok(runtime.to_dict())
}

fn update_agent_runtime(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let id = seg_from_end(req.path.as_str(), 1);
    if let Some(resp) = require(
        body,
        &["agentRuntimeArtifact", "roleArn", "networkConfiguration"],
    ) {
        return resp;
    }
    let Some(runtime) = st.agent_runtimes.get_mut(&id) else {
        return missing("Agent Runtime", &id);
    };
    runtime.update(body);
    let out = json!({
        "agentRuntimeArn": runtime.runtime_arn(),
        "agentRuntimeId": runtime.agent_runtime_id,
        "workloadIdentityDetails": {
            "workloadIdentityArn": runtime.workload_identity_arn()
        },
        "agentRuntimeVersion": runtime.agent_runtime_version,
        "createdAt": runtime.created_at,
        "lastUpdatedAt": runtime.last_updated_at,
        "status": "UPDATING",
    });
    ok(out)
}

fn delete_agent_runtime(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = seg_from_end(req.path.as_str(), 1);
    let Some(runtime) = st.agent_runtimes.get(&id) else {
        return missing("Agent Runtime", &id);
    };
    let agent_runtime_id = runtime.agent_runtime_id.clone();
    // moto cascades the endpoints for this runtime (key.0 == id).
    let stale: Vec<(String, String)> = st
        .agent_runtime_endpoints
        .keys()
        .filter(|(rid, _)| rid == &agent_runtime_id)
        .cloned()
        .collect();
    for key in stale {
        st.agent_runtime_endpoints.remove(&key);
    }
    st.agent_runtimes.remove(&agent_runtime_id);
    ok(json!({ "status": "DELETING", "agentRuntimeId": agent_runtime_id }))
}

fn list_agent_runtimes(st: &mut AgentCoreStore) -> Response {
    for runtime in st.agent_runtimes.values_mut() {
        tick_ready(runtime);
    }
    let summaries: Vec<Value> = st
        .agent_runtimes
        .values()
        .map(AgentRuntime::to_summary)
        .collect();
    ok(json!({ "agentRuntimes": summaries }))
}

fn list_agent_runtime_versions(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = seg_from_end(req.path.as_str(), 2);
    let Some(runtime) = st.agent_runtimes.get(&id) else {
        return missing("Agent Runtime", &id);
    };
    // moto: list(reversed(runtime.versions)).
    let versions: Vec<Value> = runtime.versions.iter().rev().cloned().collect();
    ok(json!({ "agentRuntimes": versions }))
}

// ---------------------------------------------------------------------------
// agent runtime endpoints (moto parity)
// ---------------------------------------------------------------------------

fn endpoint_not_found(agent_runtime_id: &str, endpoint_name: &str) -> Response {
    not_found(&format!(
        "Could not find endpoint {endpoint_name} for Agent Runtime {agent_runtime_id}"
    ))
}

/// URI keys for the runtime-endpoint family. POST/PUT create hits
/// `/runtimes/{agentRuntimeId}/runtime-endpoints/` (id at seg 2); the
/// per-endpoint URIs carry the name too (id seg 3, name seg 1).
fn endpoint_create_id(req: &Request) -> String {
    seg_from_end(req.path.as_str(), 2)
}

fn endpoint_pair(req: &Request) -> (String, String) {
    (
        seg_from_end(req.path.as_str(), 3),
        seg_from_end(req.path.as_str(), 1),
    )
}

fn runtime_exists(st: &AgentCoreStore, id: &str) -> bool {
    st.agent_runtimes.contains_key(id)
}

fn create_agent_runtime_endpoint(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let id = endpoint_create_id(req);
    if let Some(resp) = require(body, &["name"]) {
        return resp;
    }
    if !runtime_exists(st, &id) {
        return missing("Agent Runtime", &id);
    }
    let endpoint_name = vstr(body, "name").unwrap_or_default();
    if st
        .agent_runtime_endpoints
        .contains_key(&(id.clone(), endpoint_name.clone()))
    {
        return conflict(&format!(
            "Endpoint {endpoint_name} already exists for Agent Runtime {id}"
        ));
    }
    let endpoint = {
        let runtime = st
            .agent_runtimes
            .get(&id)
            .expect("runtime existence checked above");
        AgentRuntimeEndpoint::new(runtime, body)
    };
    let created_at = endpoint.created_at.clone();
    let out = json!({
        "targetVersion": endpoint.target_version,
        "agentRuntimeEndpointArn": endpoint.endpoint_arn(),
        "agentRuntimeArn": endpoint.agent_runtime_arn,
        "agentRuntimeId": endpoint.agent_runtime_id,
        "endpointName": endpoint.name,
        "status": "CREATING",
        "createdAt": created_at,
    });
    st.agent_runtime_endpoints
        .insert((id, endpoint_name), endpoint);
    ok(out)
}

fn get_agent_runtime_endpoint(req: &Request, st: &mut AgentCoreStore) -> Response {
    let (id, endpoint_name) = endpoint_pair(req);
    if !runtime_exists(st, &id) {
        return missing("Agent Runtime", &id);
    }
    let Some(endpoint) = st
        .agent_runtime_endpoints
        .get_mut(&(id.clone(), endpoint_name.clone()))
    else {
        return endpoint_not_found(&id, &endpoint_name);
    };
    tick_ready(endpoint);
    let mut out = json!({
        "agentRuntimeEndpointArn": endpoint.endpoint_arn(),
        "agentRuntimeArn": endpoint.agent_runtime_arn,
        "status": endpoint.status,
        "createdAt": endpoint.created_at,
        "lastUpdatedAt": endpoint.last_updated_at,
        "name": endpoint.name,
        "id": endpoint.endpoint_id,
        "targetVersion": endpoint.target_version,
        "liveVersion": endpoint.live_version,
    });
    if !endpoint.description.is_empty() {
        out["description"] = json!(endpoint.description);
    }
    ok(out)
}

fn update_agent_runtime_endpoint(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let (id, endpoint_name) = endpoint_pair(req);
    if !runtime_exists(st, &id) {
        return missing("Agent Runtime", &id);
    }
    let Some(endpoint) = st
        .agent_runtime_endpoints
        .get_mut(&(id.clone(), endpoint_name.clone()))
    else {
        return endpoint_not_found(&id, &endpoint_name);
    };
    endpoint.update(body);
    ok(json!({
        "agentRuntimeEndpointArn": endpoint.endpoint_arn(),
        "agentRuntimeArn": endpoint.agent_runtime_arn,
        "status": "UPDATING",
        "createdAt": endpoint.created_at,
        "lastUpdatedAt": endpoint.last_updated_at,
        "liveVersion": endpoint.live_version,
        "targetVersion": endpoint.target_version,
    }))
}

fn delete_agent_runtime_endpoint(req: &Request, st: &mut AgentCoreStore) -> Response {
    let (id, endpoint_name) = endpoint_pair(req);
    if !runtime_exists(st, &id) {
        return missing("Agent Runtime", &id);
    }
    match st
        .agent_runtime_endpoints
        .remove(&(id.clone(), endpoint_name.clone()))
    {
        Some(endpoint) => ok(json!({
            "status": "DELETING",
            "agentRuntimeId": endpoint.agent_runtime_id,
            "endpointName": endpoint.name,
        })),
        None => endpoint_not_found(&id, &endpoint_name),
    }
}

fn list_agent_runtime_endpoints(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = endpoint_create_id(req);
    if !runtime_exists(st, &id) {
        return missing("Agent Runtime", &id);
    }
    for ((rid, _), ep) in st.agent_runtime_endpoints.iter_mut() {
        if rid == &id {
            tick_ready(ep);
        }
    }
    let endpoints: Vec<Value> = st
        .agent_runtime_endpoints
        .iter()
        .filter(|((rid, _), _)| rid == &id)
        .map(|(_, ep)| ep.to_summary())
        .collect();
    ok(json!({ "runtimeEndpoints": endpoints }))
}

// ---------------------------------------------------------------------------
// gateways + targets (moto parity)
// ---------------------------------------------------------------------------

/// moto overrides `result["status"] = "..."` after serialization.
fn merge_status(mut dict: Value, status: &str) -> Value {
    dict["status"] = json!(status);
    dict
}

fn gateway_not_found(id: &str) -> Response {
    missing("Gateway", id)
}

fn gateway_id_from(req: &Request) -> String {
    seg_from_end(req.path.as_str(), 1)
}

fn gateway_target_key(req: &Request) -> (String, String) {
    // /gateways/{gatewayIdentifier}/targets/{targetId}/
    (
        seg_from_end(req.path.as_str(), 3),
        seg_from_end(req.path.as_str(), 1),
    )
}

fn create_gateway(st: &mut AgentCoreStore, region: &str, account: u64, body: &Value) -> Response {
    if let Some(resp) = require(body, &["name", "roleArn", "protocolType", "authorizerType"]) {
        return resp;
    }
    let gateway = Gateway::new(region, account, body);
    let out = merge_status(gateway.to_dict(), "CREATING");
    st.gateways.insert(gateway.gateway_id.clone(), gateway);
    ok(out)
}

fn get_gateway(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = gateway_id_from(req);
    let Some(gateway) = st.gateways.get_mut(&id) else {
        return gateway_not_found(&id);
    };
    tick_ready(gateway);
    ok(gateway.to_dict())
}

fn update_gateway(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let id = gateway_id_from(req);
    if let Some(resp) = require(body, &["name", "roleArn", "protocolType", "authorizerType"]) {
        return resp;
    }
    let Some(gateway) = st.gateways.get_mut(&id) else {
        return gateway_not_found(&id);
    };
    gateway.update(body);
    ok(merge_status(gateway.to_dict(), "UPDATING"))
}

fn delete_gateway(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = gateway_id_from(req);
    let Some(gateway) = st.gateways.get(&id) else {
        return gateway_not_found(&id);
    };
    let gateway_id = gateway.gateway_id.clone();
    // moto cascades the gateway's targets.
    let stale: Vec<(String, String)> = st
        .gateway_targets
        .keys()
        .filter(|(gid, _)| gid == &gateway_id)
        .cloned()
        .collect();
    for key in stale {
        st.gateway_targets.remove(&key);
    }
    st.gateways.remove(&gateway_id);
    ok(json!({ "gatewayId": gateway_id, "status": "DELETING" }))
}

fn list_gateways(st: &mut AgentCoreStore) -> Response {
    for gateway in st.gateways.values_mut() {
        tick_ready(gateway);
    }
    let summaries: Vec<Value> = st.gateways.values().map(Gateway::to_summary).collect();
    ok(json!({ "items": summaries }))
}

fn create_gateway_target(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let gateway_id = seg_from_end(req.path.as_str(), 2);
    if let Some(resp) = require(body, &["name", "targetConfiguration"]) {
        return resp;
    }
    let Some(target) = st
        .gateways
        .get(&gateway_id)
        .map(|gateway| GatewayTarget::new(&gateway.region, gateway, body))
    else {
        return gateway_not_found(&gateway_id);
    };
    let target_id = target.target_id.clone();
    let out = merge_status(target.to_dict(), "CREATING");
    st.gateway_targets.insert((gateway_id, target_id), target);
    ok(out)
}

fn target_not_found(gateway_id: &str, target_id: &str) -> Response {
    not_found(&format!(
        "Could not find Target {target_id} for Gateway {gateway_id}"
    ))
}

fn get_gateway_target(req: &Request, st: &mut AgentCoreStore) -> Response {
    let key = gateway_target_key(req);
    match st.gateway_targets.get_mut(&key) {
        Some(target) => {
            tick_ready(target);
            ok(target.to_dict())
        }
        None => target_not_found(&key.0, &key.1),
    }
}

fn update_gateway_target(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let key = gateway_target_key(req);
    if let Some(resp) = require(body, &["name", "targetConfiguration"]) {
        return resp;
    }
    match st.gateway_targets.get_mut(&key) {
        Some(target) => {
            target.update(body);
            ok(merge_status(target.to_dict(), "UPDATING"))
        }
        None => target_not_found(&key.0, &key.1),
    }
}

fn delete_gateway_target(req: &Request, st: &mut AgentCoreStore) -> Response {
    let key = gateway_target_key(req);
    match st.gateway_targets.remove(&key) {
        Some(target) => {
            let gateway_arn = target.target_gateway_arn();
            let target_id = target.target_id;
            ok(json!({
                "gatewayArn": gateway_arn,
                "targetId": target_id,
                "status": "DELETING",
            }))
        }
        None => target_not_found(&key.0, &key.1),
    }
}

fn list_gateway_targets(req: &Request, st: &mut AgentCoreStore) -> Response {
    let gateway_id = seg_from_end(req.path.as_str(), 2);
    if !st.gateways.contains_key(&gateway_id) {
        return gateway_not_found(&gateway_id);
    }
    for (key, target) in st.gateway_targets.iter_mut() {
        if key.0 == gateway_id {
            tick_ready(target);
        }
    }
    let summaries: Vec<Value> = st
        .gateway_targets
        .iter()
        .filter(|(key, _)| key.0 == gateway_id)
        .map(|(_, t)| t.to_summary())
        .collect();
    ok(json!({ "items": summaries }))
}

// ---------------------------------------------------------------------------
// memories (moto parity)
// ---------------------------------------------------------------------------

fn memory_not_found(id: &str) -> Response {
    missing("Memory", id)
}

fn create_memory(st: &mut AgentCoreStore, region: &str, account: u64, body: &Value) -> Response {
    if let Some(resp) = require(body, &["name", "eventExpiryDuration"]) {
        return resp;
    }
    let memory = Memory::new(region, account, body);
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            let arn = memory.memory_arn();
            let row = st.tags.entry(arn).or_default();
            for (key, value) in tags {
                row.insert(key.clone(), value.clone());
            }
        }
    }
    let out = json!({ "memory": merge_status(memory.to_dict(), "CREATING") });
    st.memories.insert(memory.memory_id.clone(), memory);
    ok(out)
}

fn get_memory(req: &Request, st: &mut AgentCoreStore) -> Response {
    // GET /memories/{memoryId}/details — id is seg 2 from the tail.
    let id = seg_from_end(req.path.as_str(), 2);
    let Some(memory) = st.memories.get_mut(&id) else {
        return memory_not_found(&id);
    };
    tick_ready(memory);
    ok(json!({ "memory": memory.to_dict() }))
}

fn update_memory(req: &Request, st: &mut AgentCoreStore, body: &Value) -> Response {
    let id = seg_from_end(req.path.as_str(), 2);
    let Some(memory) = st.memories.get_mut(&id) else {
        return memory_not_found(&id);
    };
    let ts = now();
    memory.update(body, &ts);
    ok(json!({ "memory": memory.to_dict() }))
}

fn delete_memory(req: &Request, st: &mut AgentCoreStore) -> Response {
    let id = seg_from_end(req.path.as_str(), 2);
    match st.memories.remove(&id) {
        Some(memory) => ok(json!({
            "memoryId": memory.memory_id,
            "status": "DELETING",
        })),
        None => memory_not_found(&id),
    }
}

fn list_memories(st: &mut AgentCoreStore) -> Response {
    for memory in st.memories.values_mut() {
        tick_ready(memory);
    }
    let summaries: Vec<Value> = st.memories.values().map(Memory::to_summary).collect();
    ok(json!({ "memories": summaries }))
}

// ---------------------------------------------------------------------------
// tests — control-plane CRUD, tags, statuses (own account per test: the
// state slot is process-global)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn reqacct(method: &str, path: &str, query: &str, body: Value, account: u64) -> Request {
        Request {
            service: "bedrock-agentcore-control".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: Option<Response>) -> Value {
        serde_json::from_slice(&resp.unwrap().body).unwrap()
    }

    fn status(resp: Option<Response>) -> u16 {
        resp.unwrap().status
    }

    #[test]
    fn code_interpreter_lifecycle_with_tags() {
        let account = 437000000001;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/code-interpreters",
            "",
            json!({
                "name": "my-interp",
                "description": "custom sandbox",
                "executionRoleArn": "arn:aws:iam::437000000001:role/ci",
                "networkConfiguration": {"networkMode": "SANDBOX"},
                "tags": {"Env": "dev", "Team": "agents"},
            }),
            account,
        )));
        assert_eq!(created["status"], "CREATING");
        let ci_id = created["codeInterpreterId"].as_str().unwrap().to_string();
        // botocore's custom id pattern: {name}-{10hex}
        assert!(ci_id.starts_with("my-interp-"));
        assert_eq!(ci_id.len(), "my-interp-".len() + 10);
        assert!(created["codeInterpreterArn"]
            .as_str()
            .unwrap()
            .contains("code-interpreter-custom/"));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/code-interpreters/{ci_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["codeInterpreter"]["status"], "READY");
        assert_eq!(got["codeInterpreter"]["name"], "my-interp");
        assert_eq!(
            got["codeInterpreter"]["networkConfiguration"]["networkMode"],
            "SANDBOX"
        );
        let ci_arn = got["codeInterpreter"]["codeInterpreterArn"]
            .as_str()
            .unwrap()
            .to_string();

        // tags ride the control plane's ARN-greedy /tags/{arn} routes
        assert_eq!(
            status(dispatch(&reqacct(
                "POST",
                &format!("/tags/{ci_arn}"),
                "",
                json!({"tags": {"Extra": "1"}}),
                account,
            ))),
            200
        );
        let tags = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{ci_arn}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(tags["tags"]["Env"], "dev");
        assert_eq!(tags["tags"]["Extra"], "1");
        let untagged = dispatch(&reqacct(
            "DELETE",
            &format!("/tags/{ci_arn}"),
            "tagKeys=Env",
            json!({}),
            account,
        ));
        assert_eq!(status(untagged), 200);
        let tags_after = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{ci_arn}"),
            "",
            json!({}),
            account,
        )));
        assert!(tags_after["tags"].get("Env").is_none());
        assert_eq!(tags_after["tags"]["Team"], "agents");

        // List: SYSTEM lazily seeded; `type=CUSTOM` hides it
        let listed = body(dispatch(&reqacct(
            "POST",
            "/code-interpreters",
            "maxResults=50",
            json!({}),
            account,
        )));
        let ids: Vec<&str> = listed["codeInterpreterSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["codeInterpreterId"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"aws.codeinterpreter.v1"));
        assert!(ids.contains(&ci_id.as_str()));
        let customs = body(dispatch(&reqacct(
            "POST",
            "/code-interpreters",
            "type=CUSTOM",
            json!({}),
            account,
        )));
        assert_eq!(
            customs["codeInterpreterSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // the system interpreter boots READY and cannot be deleted
        let system = body(dispatch(&reqacct(
            "GET",
            "/code-interpreters/aws.codeinterpreter.v1",
            "",
            json!({}),
            account,
        )));
        assert_eq!(system["codeInterpreter"]["status"], "READY");
        let sys_delete = dispatch(&reqacct(
            "DELETE",
            "/code-interpreters/aws.codeinterpreter.v1",
            "",
            json!({}),
            account,
        ));
        assert_eq!(body(sys_delete)["__type"], "ConflictException");

        // delete → 202 DELETING; then the record is gone
        let deleted = dispatch(&reqacct(
            "DELETE",
            &format!("/code-interpreters/{ci_id}"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(deleted.unwrap().status, 202);
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/code-interpreters/{ci_id}"),
            "",
            json!({}),
            account,
        ));
        let gone_body = body(gone);
        assert_eq!(gone_body["__type"], "ResourceNotFoundException");
        assert!(gone_body["message"]
            .as_str()
            .unwrap()
            .contains("Could not find Code Interpreter"));

        // bad networkMode — the modeled ValidationException
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                "/code-interpreters",
                "",
                json!({"name": "x", "networkConfiguration": {"networkMode": "NOPE"}}),
                account,
            )))["__type"],
            "ValidationException"
        );
    }

    #[test]
    fn agent_runtime_lifecycle() {
        let account = 437000000002;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/runtimes/",
            "",
            json!({
                "agentRuntimeName": "my-runtime",
                "agentRuntimeArtifact": {"containerConfiguration": {"uri": "123.dkr.ecr"}},
                "roleArn": "arn:aws:iam::437000000002:role/rt",
                "networkConfiguration": {"networkMode": "PUBLIC"},
                "protocolConfiguration": {"serverProtocol": "MCP"},
            }),
            account,
        )));
        assert_eq!(created["status"], "CREATING");
        let rt_id = created["agentRuntimeId"].as_str().unwrap().to_string();
        assert!(rt_id.starts_with('a'));
        assert!(!rt_id.contains(':'));
        assert!(created["workloadIdentityDetails"]["workloadIdentityArn"]
            .as_str()
            .unwrap()
            .contains("workload-identity-directory/default/workload-identity/"));
        let rt_arn_v1 = created["agentRuntimeArn"].as_str().unwrap().to_string();

        // get advances CREATING -> READY
        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/runtimes/{rt_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["status"], "READY");
        assert_eq!(got["agentRuntimeVersion"], "1");

        // update bumps the version and re-arns with the same uuid resource
        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/runtimes/{rt_id}/"),
            "",
            json!({
                "agentRuntimeArtifact": {"s3": "new"},
                "roleArn": "arn:aws:iam::437000000002:role/rt2",
                "networkConfiguration": {"networkMode": "PUBLIC"},
                "description": "updated",
            }),
            account,
        )));
        assert_eq!(updated["status"], "UPDATING");
        assert_eq!(updated["agentRuntimeVersion"], "2");
        assert_ne!(updated["agentRuntimeArn"], rt_arn_v1.as_str());
        assert!(updated["agentRuntimeArn"]
            .as_str()
            .unwrap()
            .contains("agent/"));

        // versions list (newest first, moto's reversed snapshot order)
        let versions = body(dispatch(&reqacct(
            "POST",
            &format!("/runtimes/{rt_id}/versions/"),
            "",
            json!({}),
            account,
        )));
        let versions = versions["agentRuntimes"].as_array().unwrap().clone();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["agentRuntimeVersion"], "2");

        // endpoints: create, conflict de-dup, list, cascade on delete
        let endpoint_create = body(dispatch(&reqacct(
            "PUT",
            &format!("/runtimes/{rt_id}/runtime-endpoints/"),
            "",
            json!({"name": "end-main"}),
            account,
        )));
        assert_eq!(endpoint_create["status"], "CREATING");
        assert_eq!(endpoint_create["targetVersion"], "2");
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                &format!("/runtimes/{rt_id}/runtime-endpoints/"),
                "",
                json!({"name": "end-main"}),
                account,
            )))["__type"],
            "ConflictException"
        );
        assert_eq!(
            body(dispatch(&reqacct(
                "POST",
                &format!("/runtimes/{rt_id}/runtime-endpoints/"),
                "",
                json!({}),
                account,
            )))["runtimeEndpoints"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // delete the runtime — endpoints cascade
        assert_eq!(
            body(dispatch(&reqacct(
                "DELETE",
                &format!("/runtimes/{rt_id}/"),
                "",
                json!({}),
                account,
            )))["status"],
            "DELETING"
        );
        let gone = body(dispatch(&reqacct(
            "GET",
            &format!("/runtimes/{rt_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(gone["__type"], "ResourceNotFoundException");
        assert_eq!(
            gone["message"].as_str().unwrap(),
            "Could not find Agent Runtime with ID {rt_id}".replace("{rt_id}", &rt_id)
        );
        // lacking required members answers moto's raw 500 page
        let crashed = dispatch(&reqacct("PUT", "/runtimes/", "", json!({}), account));
        let crashed = crashed.unwrap();
        assert_eq!(crashed.status, 500);
        assert!(String::from_utf8_lossy(&crashed.body).contains("Internal Server Error"));
    }

    #[test]
    fn gateway_and_target_lifecycle() {
        let account = 437000000003;
        let created = body(dispatch(&reqacct(
            "POST",
            "/gateways/",
            "",
            json!({
                "name": "gw",
                "roleArn": "arn:aws:iam::437000000003:role/gw",
                "protocolType": "MCP",
                "authorizerType": "CUSTOM_JWT",
                "kmsKeyArn": "arn:aws:kms:us-east-1:437000000003:key/k",
            }),
            account,
        )));
        assert_eq!(created["status"], "CREATING");
        let gw_id = created["gatewayId"].as_str().unwrap().to_string();
        assert_eq!(
            created["gatewayUrl"],
            format!("https://{gw_id}.gateway.bedrock-agentcore.us-east-1.amazonaws.com")
        );
        // falsy-dropping: no protocolConfiguration / description here
        assert!(created.get("description").is_none());
        assert_eq!(
            created["kmsKeyArn"],
            "arn:aws:kms:us-east-1:437000000003:key/k"
        );

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/gateways/{gw_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["status"], "READY");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/gateways/{gw_id}/"),
            "",
            json!({
                "name": "gw2",
                "roleArn": "arn:aws:iam::437000000003:role/gw",
                "protocolType": "MCP",
                "authorizerType": "CUSTOM_JWT",
            }),
            account,
        )));
        assert_eq!(updated["status"], "UPDATING");
        assert_eq!(updated["name"], "gw2");

        // targets require the gateway and dedupe-not (moto keys by target id)
        let target_create = body(dispatch(&reqacct(
            "POST",
            &format!("/gateways/{gw_id}/targets/"),
            "",
            json!({"name": "tgt", "targetConfiguration": {"mcp": {"server": "x"}}}),
            account,
        )));
        assert_eq!(target_create["status"], "CREATING");
        assert!(target_create["gatewayArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("gateway/{gw_id}")));
        let target_id = target_create["targetId"].as_str().unwrap().to_string();

        let got_target = body(dispatch(&reqacct(
            "GET",
            &format!("/gateways/{gw_id}/targets/{target_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got_target["status"], "READY");

        // unknown gateway -> the fork's exact message
        let missing_gw = body(dispatch(&reqacct(
            "GET",
            "/gateways/nope1/",
            "",
            json!({}),
            account,
        )));
        assert_eq!(missing_gw["__type"], "ResourceNotFoundException");
        assert_eq!(
            missing_gw["message"],
            "Could not find Gateway with ID nope1"
        );

        assert_eq!(
            body(dispatch(&reqacct(
                "DELETE",
                &format!("/gateways/{gw_id}/targets/{target_id}/"),
                "",
                json!({}),
                account,
            )))["status"],
            "DELETING"
        );
        assert_eq!(
            body(dispatch(&reqacct(
                "DELETE",
                &format!("/gateways/{gw_id}/"),
                "",
                json!({}),
                account,
            )))["status"],
            "DELETING"
        );
        // list after delete is empty
        assert_eq!(
            body(dispatch(&reqacct(
                "GET",
                "/gateways/",
                "",
                json!({}),
                account
            )))["items"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn memory_lifecycle_with_strategies() {
        let account = 437000000004;
        let created = body(dispatch(&reqacct(
            "POST",
            "/memories/create",
            "",
            json!({
                "name": "facts",
                "eventExpiryDuration": 60,
                "description": "useful",
                "memoryStrategies": [{"semanticMemoryStrategy": {"name": "s1"}}],
                "tags": {"tier": "gold"},
            }),
            account,
        )));
        let mem = &created["memory"];
        assert_eq!(mem["status"], "CREATING");
        let mem_id = mem["id"].as_str().unwrap().to_string();
        assert!(mem_id.starts_with('m'));
        assert_eq!(mem["eventExpiryDuration"], 60);
        assert_eq!(mem["strategies"][0]["type"], "SEMANTIC");
        let mem_arn = mem["arn"].as_str().unwrap().to_string();

        // waiters MemoryCreated: GetMemory carries status ACTIVE
        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/memories/{mem_id}/details"),
            "",
            json!({}),
            account,
        )))["memory"]
            .clone();
        assert_eq!(got["status"], "ACTIVE");

        // tags via the memory ARN
        let tags = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{mem_arn}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(tags["tags"]["tier"], "gold");

        // update: swap expiry + delete the strategy through the update shape
        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/memories/{mem_id}/update"),
            "",
            json!({"eventExpiryDuration": 120}),
            account,
        )));
        assert_eq!(updated["memory"]["eventExpiryDuration"], 120);

        let listed = body(dispatch(&reqacct(
            "POST",
            "/memories/",
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["memories"].as_array().unwrap().len(), 1);

        assert_eq!(
            body(dispatch(&reqacct(
                "DELETE",
                &format!("/memories/{mem_id}/delete"),
                "",
                json!({}),
                account,
            )))["status"],
            "DELETING"
        );
        let gone = body(dispatch(&reqacct(
            "GET",
            &format!("/memories/{mem_id}/details"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(gone["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn unmodeled_control_uris_answer_501_or_404() {
        let account = 437000000005;
        // evaluators are registered but unmodeled -> the generic 501
        let eval = dispatch(&reqacct(
            "POST",
            "/evaluators/create",
            "",
            json!({}),
            account,
        ))
        .unwrap();
        assert_eq!(eval.status, 501);
        assert!(String::from_utf8_lossy(&eval.body)
            .contains("create_evaluator action has not been implemented"));
        // no botocore URI matches -> moto's bare 404
        assert_eq!(
            dispatch(&reqacct("GET", "/not-a-surface/", "", json!({}), account))
                .unwrap()
                .status,
            404
        );
        // the data plane answers through the same arm (its own handlers
        // land with the data-plane commit; see data.rs tests)
    }
}
