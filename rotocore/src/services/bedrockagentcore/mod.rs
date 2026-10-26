//! AWS Bedrock AgentCore backend — control plane + data plane (rest-json).
//!
//! Registers both botocore services: `bedrock-agentcore-control` (the
//! code-interpreter / agent-runtime / gateway / memory control surface) and
//! `bedrock-agentcore` (the data plane). Both models carry signing name
//! `bedrock-agentcore`, so the router delivers every wire request on the
//! `bedrock-agentcore` arm and this module splits the planes by their (fully
//! disjoint) requestUri families — the method+path template-match idiom of
//! the sibling `bedrockagent` module.
//!
//! **This module deliberately exceeds moto.** moto 5.2.4 implements no
//! code-interpreter surface at all (`moto/bedrockagentcorecontrol` covers
//! agent runtimes, endpoints, gateways, targets, memories and tags;
//! `moto/bedrockagentcore` covers only memory events) — every
//! `*CodeInterpreter*` operation answers moto's 501. Here the user-facing
//! code-interpreter op-set is real CRUD: control-plane Create/Get/List/
//! Delete of custom interpreters plus the pre-provisioned SYSTEM interpreter
//! (`aws.codeinterpreter.v1`, seeded lazily per scope, undeletable —
//! ConflictException), and data-plane session CRUD (Start/Get/Stop/List).
//! Everything moto does implement keeps its shapes, ids and statuses 1:1
//! (pinned fork `moto/bedrockagentcorecontrol/models.py`), and the modeled
//! exceptions reuse moto's exact messages.
//!
//! Stream ops stay genuinely absent: `InvokeCodeInterpreter` (data plane)
//! returns an event-stream payload (`CodeInterpreterStreamOutput`,
//! `eventstream: true` in botocore), which this gateway does not serve — the
//! generic 501 NotImplemented stands, like moto. The runtimes
//! `InvokeAgentRuntime*` data plane is likewise unimplemented (moto has no
//! handler either); its remaining surface answers the modeled 501.
//!
//! moto quirks kept 1:1:
//! - ids: agent runtimes `a{9hex}-{10hex}`, endpoints `e{9hex}-{10hex}`,
//!   gateway ids / target ids 10 hex, memories `m{9hex}-{10hex}`, strategy
//!   ids `s{9hex}-{10hex}`; runtime ARNs carry a fresh uuid4 resource:
//!   `agent/{uuid}:{version}`;
//! - ManagedState: records are born CREATING and the first `.status` read
//!   jumps straight to the final transition (moto's default "immediate"
//!   progression) — READY, or ACTIVE for memories. `update_*` sets
//!   UPDATING, which no transition covers, so it sticks forever;
//! - create responses force the transitional status (CREATING/UPDATING) on
//!   the wire; get/list responses carry the advanced status;
//! - gateway/runtime workloadIdentityDetails point at the default workload
//!   identity directory (moto's stub);
//! - data-plane events mirror moto's `BedrockAgentCoreBackend`:
//!   ValidationException on falsy actor/session/payload with python-style
//!   messages, event ids `{epoch-ms}#{16hex}`, branch default
//!   `{"name": "main"}`, `nextToken: null`, and no memoryId existence
//!   checks.
//!
//! Unimplemented botocore surface answers the sibling's moto-modeled 501
//! (`The <snake_case> action has not been implemented`); paths matching no
//! botocore URI at all answer moto's bare 404.

#![allow(dead_code)] // stub-state scaffolding; the #371 follow-up commits consume every helper

use crate::proto::{now_iso, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

pub mod control;
pub mod data;

// ---------------------------------------------------------------------------
// botocore rest-json requestUri tables (method, template, operation)
// ---------------------------------------------------------------------------

/// Control plane (`botocore/data/bedrock-agentcore-control/2023-06-05`).
static CONTROL_RULES: &[(&str, &str, &str)] = &[
    // code interpreters — control CRUD (exceeded-vs-moto set)
    ("PUT", "/code-interpreters", "CreateCodeInterpreter"),
    (
        "GET",
        "/code-interpreters/{codeInterpreterId}",
        "GetCodeInterpreter",
    ),
    (
        "DELETE",
        "/code-interpreters/{codeInterpreterId}",
        "DeleteCodeInterpreter",
    ),
    ("POST", "/code-interpreters", "ListCodeInterpreters"),
    // agent runtimes
    ("PUT", "/runtimes/", "CreateAgentRuntime"),
    ("GET", "/runtimes/{agentRuntimeId}/", "GetAgentRuntime"),
    ("PUT", "/runtimes/{agentRuntimeId}/", "UpdateAgentRuntime"),
    (
        "DELETE",
        "/runtimes/{agentRuntimeId}/",
        "DeleteAgentRuntime",
    ),
    ("POST", "/runtimes/", "ListAgentRuntimes"),
    (
        "POST",
        "/runtimes/{agentRuntimeId}/versions/",
        "ListAgentRuntimeVersions",
    ),
    (
        "PUT",
        "/runtimes/{agentRuntimeId}/runtime-endpoints/",
        "CreateAgentRuntimeEndpoint",
    ),
    (
        "GET",
        "/runtimes/{agentRuntimeId}/runtime-endpoints/{endpointName}/",
        "GetAgentRuntimeEndpoint",
    ),
    (
        "PUT",
        "/runtimes/{agentRuntimeId}/runtime-endpoints/{endpointName}/",
        "UpdateAgentRuntimeEndpoint",
    ),
    (
        "DELETE",
        "/runtimes/{agentRuntimeId}/runtime-endpoints/{endpointName}/",
        "DeleteAgentRuntimeEndpoint",
    ),
    (
        "POST",
        "/runtimes/{agentRuntimeId}/runtime-endpoints/",
        "ListAgentRuntimeEndpoints",
    ),
    // gateways + targets
    ("POST", "/gateways/", "CreateGateway"),
    ("GET", "/gateways/{gatewayIdentifier}/", "GetGateway"),
    ("PUT", "/gateways/{gatewayIdentifier}/", "UpdateGateway"),
    ("DELETE", "/gateways/{gatewayIdentifier}/", "DeleteGateway"),
    ("GET", "/gateways/", "ListGateways"),
    (
        "PUT",
        "/gateways/{gatewayIdentifier}/synchronizeTargets",
        "SynchronizeGatewayTargets",
    ),
    (
        "POST",
        "/gateways/{gatewayIdentifier}/targets/",
        "CreateGatewayTarget",
    ),
    (
        "GET",
        "/gateways/{gatewayIdentifier}/targets/{targetId}/",
        "GetGatewayTarget",
    ),
    (
        "PUT",
        "/gateways/{gatewayIdentifier}/targets/{targetId}/",
        "UpdateGatewayTarget",
    ),
    (
        "DELETE",
        "/gateways/{gatewayIdentifier}/targets/{targetId}/",
        "DeleteGatewayTarget",
    ),
    (
        "GET",
        "/gateways/{gatewayIdentifier}/targets/",
        "ListGatewayTargets",
    ),
    // memories
    ("POST", "/memories/create", "CreateMemory"),
    ("GET", "/memories/{memoryId}/details", "GetMemory"),
    ("PUT", "/memories/{memoryId}/update", "UpdateMemory"),
    ("DELETE", "/memories/{memoryId}/delete", "DeleteMemory"),
    ("POST", "/memories/", "ListMemories"),
    // resource policies
    ("PUT", "/resourcepolicy/{resourceArn}", "PutResourcePolicy"),
    ("GET", "/resourcepolicy/{resourceArn}", "GetResourcePolicy"),
    (
        "DELETE",
        "/resourcepolicy/{resourceArn}",
        "DeleteResourcePolicy",
    ),
    // evaluators
    ("POST", "/evaluators/create", "CreateEvaluator"),
    ("GET", "/evaluators/{evaluatorId}", "GetEvaluator"),
    ("PUT", "/evaluators/{evaluatorId}", "UpdateEvaluator"),
    ("DELETE", "/evaluators/{evaluatorId}", "DeleteEvaluator"),
    ("POST", "/evaluators", "ListEvaluators"),
    // browsers
    ("PUT", "/browsers", "CreateBrowser"),
    ("GET", "/browsers/{browserId}", "GetBrowser"),
    ("DELETE", "/browsers/{browserId}", "DeleteBrowser"),
    ("POST", "/browsers", "ListBrowsers"),
    ("PUT", "/browser-profiles", "CreateBrowserProfile"),
    ("GET", "/browser-profiles/{profileId}", "GetBrowserProfile"),
    (
        "DELETE",
        "/browser-profiles/{profileId}",
        "DeleteBrowserProfile",
    ),
    ("POST", "/browser-profiles", "ListBrowserProfiles"),
    // online evaluation configs
    (
        "POST",
        "/online-evaluation-configs/create",
        "CreateOnlineEvaluationConfig",
    ),
    (
        "GET",
        "/online-evaluation-configs/{onlineEvaluationConfigId}",
        "GetOnlineEvaluationConfig",
    ),
    (
        "PUT",
        "/online-evaluation-configs/{onlineEvaluationConfigId}",
        "UpdateOnlineEvaluationConfig",
    ),
    (
        "DELETE",
        "/online-evaluation-configs/{onlineEvaluationConfigId}",
        "DeleteOnlineEvaluationConfig",
    ),
    (
        "POST",
        "/online-evaluation-configs",
        "ListOnlineEvaluationConfigs",
    ),
    // policy engines + policies + generations
    ("POST", "/policy-engines", "CreatePolicyEngine"),
    ("GET", "/policy-engines/{policyEngineId}", "GetPolicyEngine"),
    (
        "PATCH",
        "/policy-engines/{policyEngineId}",
        "UpdatePolicyEngine",
    ),
    (
        "DELETE",
        "/policy-engines/{policyEngineId}",
        "DeletePolicyEngine",
    ),
    ("GET", "/policy-engines", "ListPolicyEngines"),
    (
        "POST",
        "/policy-engines/{policyEngineId}/policies",
        "CreatePolicy",
    ),
    (
        "GET",
        "/policy-engines/{policyEngineId}/policies/{policyId}",
        "GetPolicy",
    ),
    (
        "PATCH",
        "/policy-engines/{policyEngineId}/policies/{policyId}",
        "UpdatePolicy",
    ),
    (
        "DELETE",
        "/policy-engines/{policyEngineId}/policies/{policyId}",
        "DeletePolicy",
    ),
    (
        "GET",
        "/policy-engines/{policyEngineId}/policies",
        "ListPolicies",
    ),
    (
        "POST",
        "/policy-engines/{policyEngineId}/policy-generations",
        "StartPolicyGeneration",
    ),
    (
        "GET",
        "/policy-engines/{policyEngineId}/policy-generations/{policyGenerationId}",
        "GetPolicyGeneration",
    ),
    (
        "GET",
        "/policy-engines/{policyEngineId}/policy-generations",
        "ListPolicyGenerations",
    ),
    (
        "GET",
        "/policy-engines/{policyEngineId}/policy-generations/{policyGenerationId}/assets",
        "ListPolicyGenerationAssets",
    ),
    // identities: credential providers, workload identities, token vault
    (
        "POST",
        "/identities/CreateApiKeyCredentialProvider",
        "CreateApiKeyCredentialProvider",
    ),
    (
        "POST",
        "/identities/DeleteApiKeyCredentialProvider",
        "DeleteApiKeyCredentialProvider",
    ),
    (
        "POST",
        "/identities/GetApiKeyCredentialProvider",
        "GetApiKeyCredentialProvider",
    ),
    (
        "POST",
        "/identities/UpdateApiKeyCredentialProvider",
        "UpdateApiKeyCredentialProvider",
    ),
    (
        "POST",
        "/identities/ListApiKeyCredentialProviders",
        "ListApiKeyCredentialProviders",
    ),
    (
        "POST",
        "/identities/CreateOauth2CredentialProvider",
        "CreateOauth2CredentialProvider",
    ),
    (
        "POST",
        "/identities/DeleteOauth2CredentialProvider",
        "DeleteOauth2CredentialProvider",
    ),
    (
        "POST",
        "/identities/GetOauth2CredentialProvider",
        "GetOauth2CredentialProvider",
    ),
    (
        "POST",
        "/identities/UpdateOauth2CredentialProvider",
        "UpdateOauth2CredentialProvider",
    ),
    (
        "POST",
        "/identities/ListOauth2CredentialProviders",
        "ListOauth2CredentialProviders",
    ),
    (
        "POST",
        "/identities/CreateWorkloadIdentity",
        "CreateWorkloadIdentity",
    ),
    (
        "POST",
        "/identities/DeleteWorkloadIdentity",
        "DeleteWorkloadIdentity",
    ),
    (
        "POST",
        "/identities/GetWorkloadIdentity",
        "GetWorkloadIdentity",
    ),
    (
        "POST",
        "/identities/UpdateWorkloadIdentity",
        "UpdateWorkloadIdentity",
    ),
    (
        "POST",
        "/identities/ListWorkloadIdentities",
        "ListWorkloadIdentities",
    ),
    ("POST", "/identities/get-token-vault", "GetTokenVault"),
    (
        "POST",
        "/identities/set-token-vault-cmk",
        "SetTokenVaultCMK",
    ),
];

/// Data plane (`botocore/data/bedrock-agentcore/2024-02-28`).
static DATA_RULES: &[(&str, &str, &str)] = &[
    // code interpreter sessions
    (
        "PUT",
        "/code-interpreters/{codeInterpreterIdentifier}/sessions/start",
        "StartCodeInterpreterSession",
    ),
    (
        "GET",
        "/code-interpreters/{codeInterpreterIdentifier}/sessions/get",
        "GetCodeInterpreterSession",
    ),
    (
        "PUT",
        "/code-interpreters/{codeInterpreterIdentifier}/sessions/stop",
        "StopCodeInterpreterSession",
    ),
    (
        "POST",
        "/code-interpreters/{codeInterpreterIdentifier}/sessions/list",
        "ListCodeInterpreterSessions",
    ),
    // invoke — event-stream return, answers the genuine 501
    (
        "POST",
        "/code-interpreters/{codeInterpreterIdentifier}/tools/invoke",
        "InvokeCodeInterpreter",
    ),
    // memory events + derived lists
    ("POST", "/memories/{memoryId}/events", "CreateEvent"),
    (
        "GET",
        "/memories/{memoryId}/actor/{actorId}/sessions/{sessionId}/events/{eventId}",
        "GetEvent",
    ),
    (
        "DELETE",
        "/memories/{memoryId}/actor/{actorId}/sessions/{sessionId}/events/{eventId}",
        "DeleteEvent",
    ),
    (
        "POST",
        "/memories/{memoryId}/actor/{actorId}/sessions/{sessionId}",
        "ListEvents",
    ),
    (
        "POST",
        "/memories/{memoryId}/actor/{actorId}/sessions",
        "ListSessions",
    ),
    ("POST", "/memories/{memoryId}/actors", "ListActors"),
    // memory records
    (
        "POST",
        "/memories/{memoryId}/memoryRecords",
        "ListMemoryRecords",
    ),
    (
        "POST",
        "/memories/{memoryId}/memoryRecords/batchCreate",
        "BatchCreateMemoryRecords",
    ),
    (
        "POST",
        "/memories/{memoryId}/memoryRecords/batchDelete",
        "BatchDeleteMemoryRecords",
    ),
    (
        "POST",
        "/memories/{memoryId}/memoryRecords/batchUpdate",
        "BatchUpdateMemoryRecords",
    ),
    (
        "GET",
        "/memories/{memoryId}/memoryRecord/{memoryRecordId}",
        "GetMemoryRecord",
    ),
    (
        "DELETE",
        "/memories/{memoryId}/memoryRecords/{memoryRecordId}",
        "DeleteMemoryRecord",
    ),
    (
        "POST",
        "/memories/{memoryId}/retrieve",
        "RetrieveMemoryRecords",
    ),
    // memory extraction jobs
    (
        "POST",
        "/memories/{memoryId}/extractionJobs/start",
        "StartMemoryExtractionJob",
    ),
    (
        "POST",
        "/memories/{memoryId}/extractionJobs",
        "ListMemoryExtractionJobs",
    ),
    // browser sessions + profiles
    (
        "GET",
        "/browsers/{browserIdentifier}/sessions/get",
        "GetBrowserSession",
    ),
    (
        "PUT",
        "/browsers/{browserIdentifier}/sessions/start",
        "StartBrowserSession",
    ),
    (
        "PUT",
        "/browsers/{browserIdentifier}/sessions/stop",
        "StopBrowserSession",
    ),
    (
        "PUT",
        "/browsers/{browserIdentifier}/sessions/streams/update",
        "UpdateBrowserStream",
    ),
    (
        "POST",
        "/browsers/{browserIdentifier}/sessions/list",
        "ListBrowserSessions",
    ),
    (
        "PUT",
        "/browser-profiles/{profileIdentifier}/save",
        "SaveBrowserSessionProfile",
    ),
    // runtime invocations
    (
        "POST",
        "/runtimes/{agentRuntimeArn}/invocations",
        "InvokeAgentRuntime",
    ),
    (
        "POST",
        "/runtimes/{agentRuntimeArn}/commands",
        "InvokeAgentRuntimeCommand",
    ),
    (
        "POST",
        "/runtimes/{agentRuntimeArn}/stopruntimesession",
        "StopRuntimeSession",
    ),
    (
        "GET",
        "/runtimes/{agentRuntimeArn}/invocations/.well-known/agent-card.json",
        "GetAgentCard",
    ),
    // identities: resource-workload tokens
    ("POST", "/identities/api-key", "GetResourceApiKey"),
    ("POST", "/identities/oauth2/token", "GetResourceOauth2Token"),
    (
        "POST",
        "/identities/CompleteResourceTokenAuth",
        "CompleteResourceTokenAuth",
    ),
    (
        "POST",
        "/identities/GetWorkloadAccessToken",
        "GetWorkloadAccessToken",
    ),
    (
        "POST",
        "/identities/GetWorkloadAccessTokenForJWT",
        "GetWorkloadAccessTokenForJWT",
    ),
    (
        "POST",
        "/identities/GetWorkloadAccessTokenForUserId",
        "GetWorkloadAccessTokenForUserId",
    ),
    // evaluations
    ("POST", "/evaluations/evaluate/{evaluatorId}", "Evaluate"),
];

/// Every table op whose handler lives in `control::route` (the tag surface
/// is handled separately by `::route_tags`).
const CONTROL_OPS: &[&str] = &[
    "CreateCodeInterpreter",
    "GetCodeInterpreter",
    "DeleteCodeInterpreter",
    "ListCodeInterpreters",
    "CreateAgentRuntime",
    "GetAgentRuntime",
    "UpdateAgentRuntime",
    "DeleteAgentRuntime",
    "ListAgentRuntimes",
    "ListAgentRuntimeVersions",
    "CreateAgentRuntimeEndpoint",
    "GetAgentRuntimeEndpoint",
    "UpdateAgentRuntimeEndpoint",
    "DeleteAgentRuntimeEndpoint",
    "ListAgentRuntimeEndpoints",
    "CreateGateway",
    "GetGateway",
    "UpdateGateway",
    "DeleteGateway",
    "ListGateways",
    "SynchronizeGatewayTargets",
    "CreateGatewayTarget",
    "GetGatewayTarget",
    "UpdateGatewayTarget",
    "DeleteGatewayTarget",
    "ListGatewayTargets",
    "CreateMemory",
    "GetMemory",
    "UpdateMemory",
    "DeleteMemory",
    "ListMemories",
];

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "bedrock-agentcore" && req.service != "bedrock-agentcore-control" {
        return None;
    }
    let slot: Arc<RwLock<HashMap<ScopeKey, AgentCoreStore>>> =
        store::slot::<AgentCoreStore>("bedrock-agentcore");
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    // The control-plane tag routes carry the ARN in the URI and ARNs contain
    // slashes — prefix-match them before the segment tables.
    if let Some(op) = tag_operation(req) {
        return Some(control::route_tags(req, op, st));
    }
    let Some(op) = wire_operation(req) else {
        // moto: no botocore URI matched the method+path — bare 404.
        return Some(Response::new(404, "text/plain", Vec::new()));
    };
    if CONTROL_OPS.contains(&op) {
        Some(control::route(req, op, st))
    } else {
        Some(data::route(req, op, st))
    }
}

/// The control plane's tag surface (`/tags/{resourceArn}`, ARN-greedy).
fn tag_operation(req: &Request) -> Option<&'static str> {
    const PREFIX: &str = "/tags/";
    if !req.path.starts_with(PREFIX) || req.path.len() <= PREFIX.len() {
        return None;
    }
    match req.method.as_str() {
        "GET" => Some("ListTagsForResource"),
        "POST" => Some("TagResource"),
        "DELETE" => Some("UntagResource"),
        _ => None,
    }
}

/// Operation lookup across plane tables (control first — the URI families
/// are segment-disjoint, so ordering only disambiguates by exactness).
fn wire_operation(req: &Request) -> Option<&'static str> {
    let matched = |(method, uri, _): &(&str, &str, &'static str)| {
        *method == req.method.as_str() && template_matches(uri, req.path.as_str())
    };
    CONTROL_RULES
        .iter()
        .chain(DATA_RULES.iter())
        .find(|row| matched(row))
        .map(|(_, _, op)| *op)
}

/// Sibling's moto-style template anchoring: trailing-slash shape agrees,
/// same segment count, literals exact, placeholders on any single segment.
pub fn template_matches(uri: &str, path: &str) -> bool {
    if uri.ends_with('/') != path.ends_with('/') {
        return false;
    }
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let tpl: Vec<&str> = uri.split('/').filter(|s| !s.is_empty()).collect();
    tpl.len() == segs.len()
        && tpl
            .iter()
            .zip(&segs)
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}')) || t == s)
}

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

/// Per-(account, region) in-memory state for Bedrock AgentCore.
#[derive(Default)]
pub(crate) struct AgentCoreStore {
    // --- control plane ---
    /// custom + seeded-system interpreter id -> interpreter
    code_interpreters: BTreeMap<String, CodeInterpreter>,
    agent_runtimes: BTreeMap<String, AgentRuntime>,
    /// (agentRuntimeId, endpointName) -> endpoint — moto's keying
    agent_runtime_endpoints: BTreeMap<(String, String), AgentRuntimeEndpoint>,
    gateways: BTreeMap<String, Gateway>,
    /// (gatewayId, targetId) -> target — moto's keying
    gateway_targets: BTreeMap<(String, String), GatewayTarget>,
    memories: BTreeMap<String, Memory>,
    /// ARN -> {tag key: value} — moto's TaggingService.
    tags: BTreeMap<String, BTreeMap<String, Value>>,
    // --- data plane ---
    /// (memoryId, actorId, sessionId) -> events, insertion ordered
    events: BTreeMap<(String, String, String), Vec<EventRecord>>,
    /// (codeInterpreterId, sessionId) -> session
    code_interpreter_sessions: BTreeMap<(String, String), CodeInterpreterSession>,
}

// ---------------------------------------------------------------------------
// shared record view helpers
// ---------------------------------------------------------------------------

/// moto's `ManagedState` with the default "immediate" progression: the first
/// status read jumps to the final transition; UPDATING (not a transition
/// endpoint) sticks.
pub(crate) fn advance(status: &mut String, ready: &str) {
    if status == "CREATING" {
        *status = ready.to_string();
    }
}

/// moto's `get_partition`.
pub(crate) fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region == "us-iso-east-1" || region == "us-iso-west-1" {
        "aws-iso"
    } else if region == "us-isob-east-1" {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// moto's `mock_random.get_random_hex(n)` — lowercase hex.
pub(crate) fn hex_id(n: usize) -> String {
    let full = Uuid::new_v4().simple().to_string();
    full[..n].to_string()
}

pub(crate) fn now() -> String {
    now_iso()
}

pub(crate) fn vstr(v: &Value, k: &str) -> Option<String> {
    v.get(k)?.as_str().map(String::from)
}

pub(crate) fn vnon_null(v: &Value, k: &str) -> Option<Value> {
    match v.get(k)? {
        Value::Null => None,
        out => Some(out.clone()),
    }
}

pub(crate) fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

pub(crate) fn ok_status(status: u16, body: Value) -> Response {
    Response::json(status, serde_json::to_vec(&body).unwrap_or_default())
}

pub(crate) fn not_found(message: &str) -> Response {
    crate::proto::json_error(400, "ResourceNotFoundException", message)
}

pub(crate) fn conflict(message: &str) -> Response {
    crate::proto::json_error(400, "ConflictException", message)
}

pub(crate) fn validation(message: &str) -> Response {
    crate::proto::json_error(400, "ValidationException", message)
}

/// moto's `NotImplementedError` as the sidecar surfaced it: HTTP 501 with
/// the snake-case action name.
pub(crate) fn not_implemented(op: &str) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake_case(op)),
    )
}

/// moto's `camelcase_to_underscores` (moto/core/utils.py).
pub(crate) fn snake_case(op: &str) -> String {
    let chars: Vec<char> = op.chars().collect();
    let mut out = String::with_capacity(chars.len() + 8);
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars
                .get(i + 1)
                .map(char::is_ascii_lowercase)
                .unwrap_or(false);
            let camel_transition =
                c.is_ascii_uppercase() && (next_lower || !prev.is_ascii_uppercase());
            if camel_transition {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Opaque continuation tokens (sibling token shape: base64
/// `{"uniqueAttributes": "<next page's first id>"}`).
pub(crate) fn id_page(
    items: Vec<Value>,
    id_field: &str,
    max_results: Option<usize>,
    token: Option<String>,
) -> (Vec<Value>, Value) {
    let start = match token.as_deref().filter(|t| !t.is_empty()) {
        Some(next_id) => items
            .iter()
            .position(|item| vstr(item, id_field).as_deref() == Some(next_id))
            .unwrap_or(items.len()),
        None => 0,
    };
    let end = match max_results.filter(|m| *m > 0) {
        Some(max) => (start + max).min(items.len()),
        None => items.len(),
    };
    let page = items[start..end].to_vec();
    let next_token = match items.get(end) {
        Some(next) => match vstr(next, id_field) {
            Some(next_id) => {
                Value::String(B64.encode(json!({ "uniqueAttributes": next_id }).to_string()))
            }
            None => Value::Null,
        },
        None => Value::Null,
    };
    (page, next_token)
}

pub(crate) fn decode_token(token: &str) -> Option<String> {
    let bytes = B64.decode(token).ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    vstr(&value, "uniqueAttributes")
}

/// Non-empty path segments (ids ride as raw segments, moto-style).
pub(crate) fn path_segs(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// `n`-th non-empty path segment counted from the tail (1-based).
pub(crate) fn seg_from_end(path: &str, n: usize) -> String {
    let segs = path_segs(path);
    segs.get(segs.len().saturating_sub(n))
        .copied()
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn parse_body(req: &Request) -> Value {
    parse_json_body(&req.body)
}

/// moto's `if x:` wire rule — absent/None/empty/zero members drop off the
/// response dict (moto builds `{**base, **optional if truthy}`).
pub(crate) fn drop_falsy_mut(value: &mut Value) {
    if let Value::Object(map) = value {
        map.retain(|_, v| match v {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64() != Some(0.0),
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
        });
    }
}

/// Drop genuinely-absent (`None`) members only — empty lists/objects and
/// zero survive (moto's `to_dict` on non-optional members).
pub(crate) fn drop_null_mut(value: &mut Value) {
    if let Value::Object(map) = value {
        map.retain(|_, v| !v.is_null());
    }
}

// ---------------------------------------------------------------------------
// records — field-for-field moto 5.2.4 (bedrockagentcorecontrol/models.py)
// ---------------------------------------------------------------------------

pub(crate) struct AgentRuntime {
    pub region: String,
    pub account: u64,
    pub agent_runtime_id: String,
    pub arn_resource: String,
    pub agent_runtime_version: String,
    pub agent_runtime_name: String,
    pub agent_runtime_artifact: Value,
    pub role_arn: String,
    pub network_configuration: Value,
    pub description: String,
    pub authorizer_configuration: Option<Value>,
    pub request_header_configuration: Option<Value>,
    pub protocol_configuration: Option<Value>,
    pub lifecycle_configuration: Value,
    pub environment_variables: Option<Value>,
    pub status: String,
    pub created_at: String,
    pub last_updated_at: String,
    pub versions: Vec<Value>,
}

impl AgentRuntime {
    pub fn new(region: &str, account: u64, body: &Value) -> Self {
        let name = vstr(body, "agentRuntimeName").unwrap_or_default();
        let id = format!("a{}-{}", hex_id(9), hex_id(10));
        let version = "1".to_string();
        let mut record = AgentRuntime {
            region: region.to_string(),
            account,
            agent_runtime_id: id.clone(),
            arn_resource: Uuid::new_v4().to_string(),
            agent_runtime_version: version.clone(),
            agent_runtime_name: name.clone(),
            agent_runtime_artifact: vnon_null(body, "agentRuntimeArtifact").unwrap_or(json!({})),
            role_arn: vstr(body, "roleArn").unwrap_or_default(),
            network_configuration: vnon_null(body, "networkConfiguration").unwrap_or(json!({})),
            description: vstr(body, "description").unwrap_or_default(),
            authorizer_configuration: vnon_null(body, "authorizerConfiguration"),
            request_header_configuration: vnon_null(body, "requestHeaderConfiguration"),
            protocol_configuration: vnon_null(body, "protocolConfiguration"),
            lifecycle_configuration: body
                .get("lifecycleConfiguration")
                .cloned()
                .unwrap_or(json!({})),
            environment_variables: vnon_null(body, "environmentVariables"),
            status: "CREATING".to_string(),
            created_at: now(),
            last_updated_at: now(),
            versions: Vec::new(),
        };
        record.versions.push(record.version_snapshot());
        record
    }

    pub(crate) fn runtime_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:agent/{}:{}",
            partition(&self.region),
            self.region,
            self.account,
            self.arn_resource,
            self.agent_runtime_version
        )
    }

    pub fn workload_identity_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:workload-identity-directory/default/workload-identity/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.agent_runtime_id
        )
    }

    /// moto's `_snapshot_version` row.
    fn version_snapshot(&self) -> Value {
        json!({
            "agentRuntimeArn": self.runtime_arn(),
            "agentRuntimeId": self.agent_runtime_id,
            "agentRuntimeVersion": self.agent_runtime_version,
            "agentRuntimeName": self.agent_runtime_name,
            "description": self.description,
            "lastUpdatedAt": self.last_updated_at,
            "status": self.status,
        })
    }

    /// moto's `AgentRuntime.update` — same fields, version bump, UPDATING.
    pub fn update(&mut self, body: &Value) {
        if let Some(artifact) = vnon_null(body, "agentRuntimeArtifact") {
            self.agent_runtime_artifact = artifact;
        }
        if let Some(role) = vstr(body, "roleArn") {
            self.role_arn = role;
        }
        if let Some(net) = vnon_null(body, "networkConfiguration") {
            self.network_configuration = net;
        }
        if let Some(description) = vstr(body, "description") {
            self.description = description;
        }
        if let Some(auth) = vnon_null(body, "authorizerConfiguration") {
            self.authorizer_configuration = Some(auth);
        }
        if let Some(hdr) = vnon_null(body, "requestHeaderConfiguration") {
            self.request_header_configuration = Some(hdr);
        }
        if let Some(proto) = vnon_null(body, "protocolConfiguration") {
            self.protocol_configuration = Some(proto);
        }
        if let Some(lc) = body.get("lifecycleConfiguration") {
            if !lc.is_null() {
                self.lifecycle_configuration = lc.clone();
            }
        }
        if let Some(env) = vnon_null(body, "environmentVariables") {
            self.environment_variables = Some(env);
        }
        let new_version = (self.agent_runtime_version.parse::<u64>().unwrap_or(1) + 1).to_string();
        self.agent_runtime_version = new_version;
        self.last_updated_at = now();
        self.status = "UPDATING".to_string();
        self.versions.push(self.version_snapshot());
    }

    /// moto's `to_summary()`.
    pub fn to_summary(&self) -> Value {
        json!({
            "agentRuntimeArn": self.runtime_arn(),
            "agentRuntimeId": self.agent_runtime_id,
            "agentRuntimeVersion": self.agent_runtime_version,
            "agentRuntimeName": self.agent_runtime_name,
            "description": self.description,
            "lastUpdatedAt": self.last_updated_at,
            "status": self.status,
        })
    }

    /// moto's get response (create/serialisation drops falsy members).
    pub fn to_dict(&self) -> Value {
        let mut v = json!({
            "agentRuntimeArn": self.runtime_arn(),
            "agentRuntimeName": self.agent_runtime_name,
            "agentRuntimeId": self.agent_runtime_id,
            "agentRuntimeVersion": self.agent_runtime_version,
            "agentRuntimeArtifact": self.agent_runtime_artifact,
            "roleArn": self.role_arn,
            "networkConfiguration": self.network_configuration,
            "createdAt": self.created_at,
            "lastUpdatedAt": self.last_updated_at,
            "status": self.status,
            "lifecycleConfiguration": self.lifecycle_configuration,
            "description": self.description,
            "workloadIdentityDetails": {
                "workloadIdentityArn": self.workload_identity_arn()
            },
            "protocolConfiguration": self.protocol_configuration,
            "environmentVariables": self.environment_variables,
            "authorizerConfiguration": self.authorizer_configuration,
            "requestHeaderConfiguration": self.request_header_configuration,
        });
        drop_null_mut(&mut v);
        v
    }
}

pub(crate) struct AgentRuntimeEndpoint {
    pub region: String,
    pub account: u64,
    pub endpoint_id: String,
    pub arn_resource: String,
    pub name: String,
    pub agent_runtime_id: String,
    /// moto captures the runtime's ARN at endpoint creation and keeps it.
    pub agent_runtime_arn: String,
    pub target_version: String,
    pub live_version: String,
    pub description: String,
    pub status: String,
    pub created_at: String,
    pub last_updated_at: String,
}

impl AgentRuntimeEndpoint {
    pub fn new(runtime: &AgentRuntime, body: &Value) -> Self {
        let name = vstr(body, "name").unwrap_or_default();
        AgentRuntimeEndpoint {
            region: runtime.region.clone(),
            account: runtime.account,
            endpoint_id: format!("e{}-{}", hex_id(9), hex_id(10)),
            arn_resource: Uuid::new_v4().to_string(),
            name,
            agent_runtime_id: runtime.agent_runtime_id.clone(),
            agent_runtime_arn: runtime.runtime_arn(),
            target_version: vstr(body, "agentRuntimeVersion")
                .unwrap_or_else(|| runtime.agent_runtime_version.clone()),
            live_version: vstr(body, "agentRuntimeVersion")
                .unwrap_or_else(|| runtime.agent_runtime_version.clone()),
            description: vstr(body, "description").unwrap_or_default(),
            status: "CREATING".to_string(),
            created_at: now(),
            last_updated_at: now(),
        }
    }

    pub fn endpoint_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:agentEndpoint/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.arn_resource
        )
    }

    pub fn update(&mut self, body: &Value) {
        if let Some(version) = vstr(body, "agentRuntimeVersion") {
            self.target_version = version;
        }
        if let Some(description) = vstr(body, "description") {
            self.description = description;
        }
        self.last_updated_at = now();
        self.status = "UPDATING".to_string();
    }

    /// moto's `to_summary()` — falsy members (blank description) drop.
    pub fn to_summary(&self) -> Value {
        let mut v = json!({
            "name": self.name,
            "agentRuntimeEndpointArn": self.endpoint_arn(),
            "agentRuntimeArn": self.agent_runtime_arn,
            "status": self.status,
            "id": self.endpoint_id,
            "description": self.description,
            "createdAt": self.created_at,
            "lastUpdatedAt": self.last_updated_at,
        });
        drop_falsy_mut(&mut v);
        v
    }
}

pub(crate) struct Gateway {
    pub region: String,
    pub account: u64,
    pub gateway_id: String,
    pub name: String,
    pub role_arn: String,
    pub protocol_type: String,
    pub authorizer_type: String,
    pub description: String,
    pub protocol_configuration: Option<Value>,
    pub authorizer_configuration: Option<Value>,
    pub kms_key_arn: Option<String>,
    pub interceptor_configurations: Option<Value>,
    pub policy_engine_configuration: Option<Value>,
    pub exception_level: Option<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

impl Gateway {
    pub fn new(region: &str, account: u64, body: &Value) -> Self {
        let gateway_id = hex_id(10);
        Gateway {
            region: region.to_string(),
            account,
            gateway_id,
            name: vstr(body, "name").unwrap_or_default(),
            role_arn: vstr(body, "roleArn").unwrap_or_default(),
            protocol_type: vstr(body, "protocolType").unwrap_or_default(),
            authorizer_type: vstr(body, "authorizerType").unwrap_or_default(),
            description: vstr(body, "description").unwrap_or_default(),
            protocol_configuration: vnon_null(body, "protocolConfiguration"),
            authorizer_configuration: vnon_null(body, "authorizerConfiguration"),
            kms_key_arn: vstr(body, "kmsKeyArn"),
            interceptor_configurations: vnon_null(body, "interceptorConfigurations"),
            policy_engine_configuration: vnon_null(body, "policyEngineConfiguration"),
            exception_level: vstr(body, "exceptionLevel"),
            status: "CREATING".to_string(),
            created_at: now(),
            updated_at: now(),
        }
    }

    pub fn gateway_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:gateway/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.gateway_id
        )
    }

    pub fn gateway_url(&self) -> String {
        format!(
            "https://{}.gateway.bedrock-agentcore.{}.amazonaws.com",
            self.gateway_id, self.region
        )
    }

    pub fn workload_identity_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:workload-identity-directory/default/workload-identity/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.gateway_id
        )
    }

    pub fn update(&mut self, body: &Value) {
        if let Some(name) = vstr(body, "name") {
            self.name = name;
        }
        if let Some(role) = vstr(body, "roleArn") {
            self.role_arn = role;
        }
        if let Some(protocol) = vstr(body, "protocolType") {
            self.protocol_type = protocol;
        }
        if let Some(authorizer) = vstr(body, "authorizerType") {
            self.authorizer_type = authorizer;
        }
        if let Some(description) = vstr(body, "description") {
            self.description = description;
        }
        if let Some(v) = vnon_null(body, "protocolConfiguration") {
            self.protocol_configuration = Some(v);
        }
        if let Some(v) = vnon_null(body, "authorizerConfiguration") {
            self.authorizer_configuration = Some(v);
        }
        if let Some(v) = vstr(body, "kmsKeyArn") {
            self.kms_key_arn = Some(v);
        }
        if let Some(v) = vnon_null(body, "interceptorConfigurations") {
            self.interceptor_configurations = Some(v);
        }
        if let Some(v) = vnon_null(body, "policyEngineConfiguration") {
            self.policy_engine_configuration = Some(v);
        }
        if let Some(v) = vstr(body, "exceptionLevel") {
            self.exception_level = Some(v);
        }
        self.updated_at = now();
        self.status = "UPDATING".to_string();
    }

    /// moto's `to_dict()` — optional members only when truthy.
    pub fn to_dict(&self) -> Value {
        let mut v = json!({
            "gatewayArn": self.gateway_arn(),
            "gatewayId": self.gateway_id,
            "gatewayUrl": self.gateway_url(),
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "status": self.status,
            "name": self.name,
            "roleArn": self.role_arn,
            "protocolType": self.protocol_type,
            "authorizerType": self.authorizer_type,
            "description": self.description,
            "workloadIdentityDetails": {
                "workloadIdentityArn": self.workload_identity_arn()
            },
            "protocolConfiguration": self.protocol_configuration,
            "authorizerConfiguration": self.authorizer_configuration,
            "kmsKeyArn": self.kms_key_arn,
            "interceptorConfigurations": self.interceptor_configurations,
            "policyEngineConfiguration": self.policy_engine_configuration,
            "exceptionLevel": self.exception_level,
        });
        drop_falsy_mut(&mut v);
        v
    }

    /// moto's `to_summary()`.
    pub fn to_summary(&self) -> Value {
        let mut v = json!({
            "gatewayId": self.gateway_id,
            "name": self.name,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "authorizerType": self.authorizer_type,
            "protocolType": self.protocol_type,
            "description": self.description,
        });
        drop_falsy_mut(&mut v);
        v
    }
}

pub(crate) struct GatewayTarget {
    pub region: String,
    pub account: u64,
    pub target_id: String,
    pub name: String,
    pub gateway_id: String,
    pub target_configuration: Value,
    pub description: String,
    pub credential_provider_configurations: Value,
    pub metadata_configuration: Option<Value>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

impl GatewayTarget {
    pub fn new(region: &str, gateway: &Gateway, body: &Value) -> Self {
        GatewayTarget {
            region: region.to_string(),
            account: gateway.account,
            target_id: hex_id(10),
            name: vstr(body, "name").unwrap_or_default(),
            gateway_id: gateway.gateway_id.clone(),
            target_configuration: body
                .get("targetConfiguration")
                .cloned()
                .unwrap_or(json!({})),
            description: vstr(body, "description").unwrap_or_default(),
            credential_provider_configurations: body
                .get("credentialProviderConfigurations")
                .cloned()
                .unwrap_or(json!([])),
            metadata_configuration: vnon_null(body, "metadataConfiguration"),
            status: "CREATING".to_string(),
            created_at: now(),
            updated_at: now(),
        }
    }

    pub fn update(&mut self, body: &Value) {
        if let Some(name) = vstr(body, "name") {
            self.name = name;
        }
        if let Some(configuration) = body.get("targetConfiguration").filter(|c| !c.is_null()) {
            self.target_configuration = configuration.clone();
        }
        if let Some(description) = vstr(body, "description") {
            self.description = description;
        }
        if let Some(v) = vnon_null(body, "credentialProviderConfigurations") {
            self.credential_provider_configurations = v;
        }
        if let Some(v) = vnon_null(body, "metadataConfiguration") {
            self.metadata_configuration = Some(v);
        }
        self.updated_at = now();
        self.status = "UPDATING".to_string();
    }

    pub fn to_dict(&self) -> Value {
        let mut v = json!({
            "gatewayArn": self.target_gateway_arn(),
            "targetId": self.target_id,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "status": self.status,
            "name": self.name,
            "targetConfiguration": self.target_configuration,
            "credentialProviderConfigurations": self.credential_provider_configurations,
            "description": self.description,
            "metadataConfiguration": self.metadata_configuration,
        });
        drop_falsy_mut(&mut v);
        v
    }

    pub fn to_summary(&self) -> Value {
        let mut v = json!({
            "targetId": self.target_id,
            "name": self.name,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "description": self.description,
        });
        drop_falsy_mut(&mut v);
        v
    }

    fn target_gateway_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:gateway/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.gateway_id
        )
    }
}

pub(crate) struct Memory {
    pub region: String,
    pub account: u64,
    pub memory_id: String,
    pub name: String,
    pub event_expiry_duration: i64,
    pub description: String,
    pub encryption_key_arn: Option<String>,
    pub memory_execution_role_arn: Option<String>,
    pub strategies: Vec<Value>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

impl Memory {
    pub fn new(region: &str, account: u64, body: &Value) -> Self {
        let memory_id = format!("m{}-{}", hex_id(9), hex_id(10));
        let mut record = Memory {
            region: region.to_string(),
            account,
            memory_id,
            name: vstr(body, "name").unwrap_or_default(),
            event_expiry_duration: body
                .get("eventExpiryDuration")
                .and_then(Value::as_i64)
                .unwrap_or(90),
            description: vstr(body, "description").unwrap_or_default(),
            encryption_key_arn: vstr(body, "encryptionKeyArn"),
            memory_execution_role_arn: vstr(body, "memoryExecutionRoleArn"),
            strategies: Vec::new(),
            status: "CREATING".to_string(),
            created_at: now(),
            updated_at: now(),
        };
        let now_ts = now();
        if let Some(strategies) = body.get("memoryStrategies").and_then(Value::as_array) {
            for input in strategies {
                record.add_strategy(input, &now_ts);
            }
        }
        record
    }

    /// moto's `_add_strategy` (type map, s-prefixed id, ACTIVE).
    pub fn add_strategy(&mut self, input: &Value, now_ts: &str) {
        let strategy_id = format!("s{}-{}", hex_id(9), hex_id(10));
        let (strategy_type_key, strategy_data) = input
            .as_object()
            .and_then(|o| o.iter().next())
            .map(|(k, v)| (k.clone(), v.clone()))
            .unwrap_or_else(|| ("customMemoryStrategy".into(), json!({})));
        let strategy_type = match strategy_type_key.as_str() {
            "semanticMemoryStrategy" => "SEMANTIC",
            "summaryMemoryStrategy" => "SUMMARIZATION",
            "userPreferenceMemoryStrategy" => "USER_PREFERENCE",
            "customMemoryStrategy" => "CUSTOM",
            "episodicMemoryStrategy" => "EPISODIC",
            _ => "CUSTOM",
        };
        self.strategies.push(json!({
            "strategyId": strategy_id,
            "name": strategy_data.get("name").and_then(Value::as_str).unwrap_or(""),
            "description": strategy_data.get("description").and_then(Value::as_str).unwrap_or(""),
            "type": strategy_type,
            "namespaces": strategy_data.get("namespaces").cloned().unwrap_or(json!([])),
            "status": "ACTIVE",
            "createdAt": now_ts,
            "updatedAt": now_ts,
        }));
    }

    pub fn memory_arn(&self) -> String {
        format!(
            "arn:{}:bedrock-agentcore:{}:{}:memory/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.memory_id
        )
    }

    pub fn update(&mut self, body: &Value, now_ts: &str) {
        if let Some(description) = vstr(body, "description") {
            self.description = description;
        }
        if let Some(duration) = body.get("eventExpiryDuration").and_then(Value::as_i64) {
            self.event_expiry_duration = duration;
        }
        if let Some(role) = vstr(body, "memoryExecutionRoleArn") {
            self.memory_execution_role_arn = Some(role);
        }
        if let Some(strategies) = body.get("memoryStrategies").and_then(Value::as_object) {
            if let Some(add) = strategies
                .get("addMemoryStrategies")
                .and_then(Value::as_array)
            {
                for input in add {
                    self.add_strategy(input, now_ts);
                }
            }
            if let Some(modifies) = strategies
                .get("modifyMemoryStrategies")
                .and_then(Value::as_array)
            {
                for modify in modifies {
                    if let Some(sid) = vstr(modify, "memoryStrategyId") {
                        for s in self.strategies.iter_mut() {
                            if s["strategyId"].as_str() == Some(sid.as_str()) {
                                if let Some(d) = modify.get("description") {
                                    s["description"] = d.clone();
                                }
                                if let Some(ns) = modify.get("namespaces") {
                                    s["namespaces"] = ns.clone();
                                }
                                s["updatedAt"] = json!(now_ts);
                                break;
                            }
                        }
                    }
                }
            }
            if let Some(deletes) = strategies
                .get("deleteMemoryStrategies")
                .and_then(Value::as_array)
            {
                let ids: Vec<Option<&str>> = deletes
                    .iter()
                    .map(|d| d.get("memoryStrategyId").and_then(Value::as_str))
                    .collect();
                self.strategies
                    .retain(|s| !ids.contains(&s["strategyId"].as_str()));
            }
        }
        self.updated_at = now_ts.to_string();
        self.status = "ACTIVE".to_string();
    }

    pub fn to_dict(&self) -> Value {
        let mut v = json!({
            "arn": self.memory_arn(),
            "id": self.memory_id,
            "name": self.name,
            "eventExpiryDuration": self.event_expiry_duration,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "description": self.description,
            "encryptionKeyArn": self.encryption_key_arn,
            "memoryExecutionRoleArn": self.memory_execution_role_arn,
            "strategies": self.strategies,
        });
        drop_falsy_mut(&mut v);
        v
    }

    pub fn to_summary(&self) -> Value {
        json!({
            "arn": self.memory_arn(),
            "id": self.memory_id,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
    }
}

pub(crate) struct CodeInterpreter {
    /// id is either the SYSTEM sentinel (`aws.codeinterpreter.v1`) or a
    /// custom `{name}-{10hex}`.
    pub id: String,
    pub name: String,
    pub is_system: bool,
    pub description: Option<String>,
    pub execution_role_arn: Option<String>,
    pub network_configuration: Value,
    pub certificates: Option<Value>,
    pub status: String,
    pub created_at: String,
    pub last_updated_at: String,
}

impl CodeInterpreter {
    pub fn new(body: &Value) -> Self {
        let name = vstr(body, "name").unwrap_or_default();
        CodeInterpreter {
            id: format!("{name}-{}", hex_id(10)),
            name,
            is_system: false,
            description: vstr(body, "description").filter(|d| !d.is_empty()),
            execution_role_arn: vstr(body, "executionRoleArn").filter(|d| !d.is_empty()),
            network_configuration: vnon_null(body, "networkConfiguration")
                .unwrap_or(json!({"networkMode": "SANDBOX"})),
            certificates: body.get("certificates").filter(|v| !v.is_null()).cloned(),
            status: "CREATING".to_string(),
            created_at: now(),
            last_updated_at: now(),
        }
    }

    /// The lazily-seeded SYSTEM interpreter.
    pub fn system() -> Self {
        CodeInterpreter {
            id: "aws.codeinterpreter.v1".to_string(),
            name: "aws.codeinterpreter.v1".to_string(),
            is_system: true,
            description: None,
            execution_role_arn: None,
            network_configuration: json!({"networkMode": "SANDBOX"}),
            certificates: None,
            status: "READY".to_string(),
            created_at: now(),
            last_updated_at: now(),
        }
    }

    pub fn ci_arn(&self, region: &str, account: u64) -> String {
        let resource = if self.is_system {
            format!("code-interpreter/{}", self.id)
        } else {
            format!("code-interpreter-custom/{}", self.id)
        };
        format!(
            "arn:{}:bedrock-agentcore:{region}:{account}:{resource}",
            partition(region)
        )
    }

    /// moto's summary trio plus createdAt (required by the botocore shape).
    pub fn to_summary(&self, region: &str, account: u64) -> Value {
        let mut v = json!({
            "codeInterpreterId": self.id,
            "codeInterpreterArn": self.ci_arn(region, account),
            "name": self.name,
            "description": self.description,
            "status": self.status,
            "createdAt": self.created_at,
        });
        drop_falsy_mut(&mut v);
        v
    }

    /// GetCodeInterpreterResponse — required members always present.
    pub fn to_dict(&self, region: &str, account: u64) -> Value {
        let mut v = json!({
            "codeInterpreterId": self.id,
            "codeInterpreterArn": self.ci_arn(region, account),
            "name": self.name,
            "description": self.description,
            "executionRoleArn": self.execution_role_arn,
            "networkConfiguration": self.network_configuration,
            "status": self.status,
            "certificates": self.certificates,
            "createdAt": self.created_at,
            "lastUpdatedAt": self.last_updated_at,
        });
        drop_null_mut(&mut v);
        v
    }
}

pub(crate) struct CodeInterpreterSession {
    pub ci_id: String,
    pub session_id: String,
    pub name: Option<String>,
    pub status: String,
    pub certificates: Option<Value>,
    pub session_timeout_seconds: Option<i64>,
    pub created_at: String,
    pub last_updated_at: String,
}

impl CodeInterpreterSession {
    pub fn new(ci_id: &str, body: &Value) -> Self {
        CodeInterpreterSession {
            ci_id: ci_id.to_string(),
            session_id: hex_id(16),
            name: vstr(body, "name").filter(|n| !n.is_empty()),
            status: "READY".to_string(),
            certificates: body.get("certificates").filter(|v| !v.is_null()).cloned(),
            session_timeout_seconds: body.get("sessionTimeoutSeconds").and_then(Value::as_i64),
            created_at: now(),
            last_updated_at: now(),
        }
    }

    pub fn to_summary(&self) -> Value {
        let mut v = json!({
            "codeInterpreterIdentifier": self.ci_id,
            "sessionId": self.session_id,
            "name": self.name,
            "status": self.status,
            "createdAt": self.created_at,
        });
        drop_falsy_mut(&mut v);
        v
    }

    pub fn to_dict(&self) -> Value {
        let mut v = json!({
            "codeInterpreterIdentifier": self.ci_id,
            "sessionId": self.session_id,
            "name": self.name,
            "createdAt": self.created_at,
            "sessionTimeoutSeconds": self.session_timeout_seconds,
            "status": self.status,
            "certificates": self.certificates,
        });
        drop_null_mut(&mut v);
        v
    }
}

#[derive(Clone)]
pub(crate) struct EventRecord {
    pub memory_id: String,
    pub actor_id: String,
    pub session_id: String,
    pub event_id: String,
    pub event_timestamp: Value,
    pub payload: Value,
    /// moto: `branch or {"name": "main"}`.
    pub branch: Value,
    /// moto: `metadata or {}`.
    pub metadata: Value,
}

impl EventRecord {
    /// moto's `Event.to_dict` — payload only when requested, metadata only
    /// when truthy.
    pub fn to_dict(&self, include_payload: bool) -> Value {
        let mut v = json!({
            "memoryId": self.memory_id,
            "actorId": self.actor_id,
            "sessionId": self.session_id,
            "eventId": self.event_id,
            "eventTimestamp": self.event_timestamp,
            "branch": self.branch,
        });
        if include_payload {
            v["payload"] = self.payload.clone();
        }
        let metadata_truthy = self
            .metadata
            .as_object()
            .map(|m| !m.is_empty())
            .unwrap_or(false);
        if metadata_truthy {
            v["metadata"] = self.metadata.clone();
        }
        v
    }
}
