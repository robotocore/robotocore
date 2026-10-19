//! AWS Bedrock Agents backend — moto-equivalent (rest-json).
//!
//! Replaces the moto sidecar for `bedrock-agent` (botocore bundle
//! `bedrockagent`). Request URIs are the botocore rest-json templates (e.g.
//! `GET /agents/{agentId}/`); this module resolves the operation from the
//! HTTP method + requestUri, exactly like modules `bedrock` and `ram`.
//!
//! moto's fork surface is small: `AgentsforBedrockResponse` implements only
//! the agent / knowledge-base / tag trio below (`moto/bedrockagent/` —
//! models.py, responses.py, urls.py are byte-identical between moto 5.1.23
//! and the deployed fork `2016563fd`). Every other botocore operation still
//! resolves against the spec-driven URI table and then hits moto's
//! `NotImplementedError` — the 501 with the snake-case action name is
//! reproduced here, and truly unmatchable paths answer moto's bare 404.
//!
//! State is per-(account, region), mirroring moto's `AgentsforBedrockBackend`:
//! agents and knowledge bases keyed by name-derived ids, plus moto's
//! `TaggingService` store over agent and knowledge-base ARNs. moto quirks
//! kept 1:1:
//! - ids are `name + str(uuid4())[:8]` (first 8 hex of the hyphenated form);
//! - `to_dict` drops falsy members (an agent's empty `failureReasons: []`
//!   vanishes, blank descriptions vanish, `recommendedActions: ["action"]`
//!   survives);
//! - agents are born `PREPARED`, so `DeleteAgent` succeeds without
//!   `skipResourceInUseCheck` (that query-string flag matters only for
//!   non-`PREPARED` agents, which this model never produces — moto raises
//!   `ConflictException` there);
//! - knowledge bases validate `knowledgeBaseConfiguration.type == VECTOR`
//!   and the `storageConfiguration.type`
//!   OPENSEARCH_SERVERLESS/PINECONE/REDIS_ENTERPRISE_CLOUD/RDS enum at
//!   creation; moto's ValidationException carries a constant message (the
//!   constraint detail lands in a misused template arg and never reaches the
//!   wire);
//! - deletes answer the pre-delete status `"DELETING"` and leave the tag
//!   row behind (moto leaks it — the ARN check keeps it unreachable);
//! - timestamps are moto's `unix_time()` floats;
//! - pagination is moto's `paginate()`: pages of `maxResults` (default 100)
//!   summaries, resume at the `unique_attribute` id carried by the base64
//!   `{"uniqueAttributes": "<id>"}` token (a decode failure is moto's
//!   `InvalidToken("Invalid token")`, an unknown id is
//!   `InvalidToken("Resource not found!")`).
//!
//! Path quirks mirrored from moto: agent ids come from `split("/")[-2]`
//! (the models' URIs carry a trailing slash) and knowledge-base ids from the
//! last segment — both kept percent-encoded (moto never unquotes these),
//! while tag ARNs are taken after the last `/tags/` (tag routes match
//! greedily — ARNs contain slashes) and percent-decoded.

use crate::proto::{json_error, parse_json_body, query_first, query_map, url_decode};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// botocore rest-json requestUri table (method, template, operation)
// ---------------------------------------------------------------------------

/// The full botocore `bedrock-agent` surface (70 ops): the trio of moto
/// handlers first, then every remaining modeled operation (all answer moto's
/// NotImplementedError 501). The tag routes are absent from this table —
/// their `resourceArn` URI label is greedy (ARNs contain slashes), so they
/// are prefix-matched in [`wire_operation`].
static RULES: &[(&str, &str, &str)] = &[
    // POST — the implemented list/pagination operations
    ("POST", "/agents/", "ListAgents"),
    ("POST", "/knowledgebases/", "ListKnowledgeBases"),
    // GET — implemented lookups
    ("GET", "/agents/{agentId}/", "GetAgent"),
    ("GET", "/knowledgebases/{knowledgeBaseId}", "GetKnowledgeBase"),
    // PUT — implemented creates
    ("PUT", "/agents/", "CreateAgent"),
    ("PUT", "/knowledgebases/", "CreateKnowledgeBase"),
    // DELETE — implemented deletes
    ("DELETE", "/agents/{agentId}/", "DeleteAgent"),
    ("DELETE", "/knowledgebases/{knowledgeBaseId}", "DeleteKnowledgeBase"),
    // POST
    ("POST", "/flows/", "CreateFlow"),
    ("POST", "/flows/{flowIdentifier}/", "PrepareFlow"),
    ("POST", "/flows/validate-definition", "ValidateFlowDefinition"),
    ("POST", "/flows/{flowIdentifier}/aliases", "CreateFlowAlias"),
    ("POST", "/flows/{flowIdentifier}/versions", "CreateFlowVersion"),
    ("POST", "/prompts/", "CreatePrompt"),
    ("POST", "/prompts/{promptIdentifier}/versions", "CreatePromptVersion"),
    (
        "POST",
        "/agents/{agentId}/agentversions/",
        "ListAgentVersions",
    ),
    (
        "POST",
        "/agents/{agentId}/agentversions/{agentVersion}/actiongroups/",
        "ListAgentActionGroups",
    ),
    (
        "POST",
        "/agents/{agentId}/agentversions/{agentVersion}/agentcollaborators/",
        "ListAgentCollaborators",
    ),
    (
        "POST",
        "/agents/{agentId}/agentversions/{agentVersion}/knowledgebases/",
        "ListAgentKnowledgeBases",
    ),
    ("POST", "/agents/{agentId}/agentaliases/", "ListAgentAliases"),
    ("POST", "/agents/{agentId}/", "PrepareAgent"),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/",
        "ListDataSources",
    ),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/documents",
        "ListKnowledgeBaseDocuments",
    ),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/documents/deleteDocuments",
        "DeleteKnowledgeBaseDocuments",
    ),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/documents/getDocuments",
        "GetKnowledgeBaseDocuments",
    ),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/ingestionjobs/",
        "ListIngestionJobs",
    ),
    (
        "POST",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/ingestionjobs/{ingestionJobId}/stop",
        "StopIngestionJob",
    ),
    // GET — unimplemented surface
    (
        "GET",
        "/agents/{agentId}/agentversions/{agentVersion}/",
        "GetAgentVersion",
    ),
    (
        "GET",
        "/agents/{agentId}/agentversions/{agentVersion}/actiongroups/{actionGroupId}/",
        "GetAgentActionGroup",
    ),
    (
        "GET",
        "/agents/{agentId}/agentversions/{agentVersion}/agentcollaborators/{collaboratorId}/",
        "GetAgentCollaborator",
    ),
    (
        "GET",
        "/agents/{agentId}/agentversions/{agentVersion}/knowledgebases/{knowledgeBaseId}/",
        "GetAgentKnowledgeBase",
    ),
    (
        "GET",
        "/agents/{agentId}/agentaliases/{agentAliasId}/",
        "GetAgentAlias",
    ),
    (
        "GET",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}",
        "GetDataSource",
    ),
    (
        "GET",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/ingestionjobs/{ingestionJobId}",
        "GetIngestionJob",
    ),
    ("GET", "/flows/", "ListFlows"),
    ("GET", "/flows/{flowIdentifier}/", "GetFlow"),
    ("GET", "/flows/{flowIdentifier}/aliases", "ListFlowAliases"),
    (
        "GET",
        "/flows/{flowIdentifier}/aliases/{aliasIdentifier}",
        "GetFlowAlias",
    ),
    ("GET", "/flows/{flowIdentifier}/versions", "ListFlowVersions"),
    (
        "GET",
        "/flows/{flowIdentifier}/versions/{flowVersion}/",
        "GetFlowVersion",
    ),
    ("GET", "/prompts/", "ListPrompts"),
    ("GET", "/prompts/{promptIdentifier}/", "GetPrompt"),
    // PUT — unimplemented surface
    ("PUT", "/agents/{agentId}/", "UpdateAgent"),
    ("PUT", "/agents/{agentId}/agentaliases/", "CreateAgentAlias"),
    (
        "PUT",
        "/agents/{agentId}/agentaliases/{agentAliasId}/",
        "UpdateAgentAlias",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/actiongroups/",
        "CreateAgentActionGroup",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/actiongroups/{actionGroupId}/",
        "UpdateAgentActionGroup",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/agentcollaborators/",
        "AssociateAgentCollaborator",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/agentcollaborators/{collaboratorId}/",
        "UpdateAgentCollaborator",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/knowledgebases/",
        "AssociateAgentKnowledgeBase",
    ),
    (
        "PUT",
        "/agents/{agentId}/agentversions/{agentVersion}/knowledgebases/{knowledgeBaseId}/",
        "UpdateAgentKnowledgeBase",
    ),
    ("PUT", "/knowledgebases/{knowledgeBaseId}", "UpdateKnowledgeBase"),
    (
        "PUT",
        "/knowledgebases/{knowledgeBaseId}/datasources/",
        "CreateDataSource",
    ),
    (
        "PUT",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}",
        "UpdateDataSource",
    ),
    (
        "PUT",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/documents",
        "IngestKnowledgeBaseDocuments",
    ),
    (
        "PUT",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}/ingestionjobs/",
        "StartIngestionJob",
    ),
    ("PUT", "/flows/{flowIdentifier}/", "UpdateFlow"),
    (
        "PUT",
        "/flows/{flowIdentifier}/aliases/{aliasIdentifier}",
        "UpdateFlowAlias",
    ),
    ("PUT", "/prompts/{promptIdentifier}/", "UpdatePrompt"),
    // DELETE — unimplemented surface
    (
        "DELETE",
        "/agents/{agentId}/agentaliases/{agentAliasId}/",
        "DeleteAgentAlias",
    ),
    (
        "DELETE",
        "/agents/{agentId}/agentversions/{agentVersion}/",
        "DeleteAgentVersion",
    ),
    (
        "DELETE",
        "/agents/{agentId}/agentversions/{agentVersion}/actiongroups/{actionGroupId}/",
        "DeleteAgentActionGroup",
    ),
    (
        "DELETE",
        "/agents/{agentId}/agentversions/{agentVersion}/agentcollaborators/{collaboratorId}/",
        "DisassociateAgentCollaborator",
    ),
    (
        "DELETE",
        "/agents/{agentId}/agentversions/{agentVersion}/knowledgebases/{knowledgeBaseId}/",
        "DisassociateAgentKnowledgeBase",
    ),
    (
        "DELETE",
        "/knowledgebases/{knowledgeBaseId}/datasources/{dataSourceId}",
        "DeleteDataSource",
    ),
    ("DELETE", "/flows/{flowIdentifier}/", "DeleteFlow"),
    (
        "DELETE",
        "/flows/{flowIdentifier}/aliases/{aliasIdentifier}",
        "DeleteFlowAlias",
    ),
    (
        "DELETE",
        "/flows/{flowIdentifier}/versions/{flowVersion}/",
        "DeleteFlowVersion",
    ),
    ("DELETE", "/prompts/{promptIdentifier}/", "DeletePrompt"),
];

// ---------------------------------------------------------------------------
// state (moto's AgentsforBedrockBackend)
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<std::collections::HashMap<ScopeKey, BedrockAgentStore>>>;

fn stores() -> Stores {
    store::slot::<BedrockAgentStore>("bedrock-agent")
}

/// Per-(account, region) in-memory state for AWS Bedrock Agents.
#[derive(Default)]
struct BedrockAgentStore {
    /// agent id -> agent
    agents: BTreeMap<String, Agent>,
    /// knowledge-base id -> knowledge base
    knowledge_bases: BTreeMap<String, KnowledgeBase>,
    /// arn -> {tag key: value} — moto's TaggingService over agent and
    /// knowledge-base ARNs (values kept raw, like moto's tag dict).
    tags: BTreeMap<String, BTreeMap<String, Value>>,
}

/// moto's `Agent` (moto/bedrockagent/models.py).
struct Agent {
    agent_name: String,
    client_token: Option<String>,
    instruction: Option<String>,
    foundation_model: Option<String>,
    description: Option<String>,
    idle_session_ttl_in_seconds: Option<i64>,
    agent_resource_role_arn: Option<String>,
    customer_encryption_key_arn: Option<String>,
    prompt_override_configuration: Option<Value>,
    region: String,
    account: u64,
    created_at: f64,
    updated_at: f64,
    prepared_at: f64,
    agent_status: String,
    agent_id: String,
    agent_version: String,
}

impl Agent {
    /// moto's `Agent.__init__` off a CreateAgent JSON body.
    fn new(account: u64, region: &str, body: &Value) -> Self {
        let agent_name = vstr(body, "agentName").unwrap_or_default();
        Agent {
            agent_name: agent_name.clone(),
            client_token: vstr(body, "clientToken"),
            instruction: vstr(body, "instruction"),
            foundation_model: vstr(body, "foundationModel"),
            description: vstr(body, "description"),
            idle_session_ttl_in_seconds: body
                .get("idleSessionTTLInSeconds")
                .and_then(Value::as_i64),
            agent_resource_role_arn: vstr(body, "agentResourceRoleArn"),
            customer_encryption_key_arn: vstr(body, "customerEncryptionKeyArn"),
            prompt_override_configuration: vnon_null(body, "promptOverrideConfiguration"),
            region: region.to_string(),
            account,
            created_at: unix_time(),
            updated_at: unix_time(),
            prepared_at: unix_time(),
            // moto starts every agent PREPARED.
            agent_status: "PREPARED".to_string(),
            agent_id: format!("{}{}", agent_name, short_id()),
            agent_version: "1.0".to_string(),
        }
    }

    fn agent_arn(&self) -> String {
        agent_arn(&self.region, self.account, &self.agent_id)
    }

    /// moto's `to_dict()` — falsy members drop off the wire.
    fn to_dict(&self) -> Value {
        let mut v = json!({
            "agentId": self.agent_id,
            "agentName": self.agent_name,
            "agentArn": self.agent_arn(),
            "agentVersion": self.agent_version,
            "clientToken": self.client_token,
            "instruction": self.instruction,
            "agentStatus": self.agent_status,
            "foundationModel": self.foundation_model,
            "description": self.description,
            "idleSessionTTLInSeconds": self.idle_session_ttl_in_seconds,
            "agentResourceRoleArn": self.agent_resource_role_arn,
            "customerEncryptionKeyArn": self.customer_encryption_key_arn,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "preparedAt": self.prepared_at,
            "failureReasons": [],
            "recommendedActions": ["action"],
            "promptOverrideConfiguration": self.prompt_override_configuration,
        });
        drop_falsy_mut(&mut v);
        v
    }

    /// moto's `dict_summary()` — falsy members drop.
    fn summary(&self) -> Value {
        let mut v = json!({
            "agentId": self.agent_id,
            "agentName": self.agent_name,
            "agentStatus": self.agent_status,
            "description": self.description,
            "updatedAt": self.updated_at,
            "latestAgentVersion": self.agent_version,
        });
        drop_falsy_mut(&mut v);
        v
    }
}

/// moto's `KnowledgeBase` (moto/bedrockagent/models.py). moto accepts a
/// `clientToken` but never surfaces it (`to_dict` omits it) — dropped here.
struct KnowledgeBase {
    name: String,
    description: Option<String>,
    role_arn: Option<String>,
    knowledge_base_configuration: Value,
    storage_configuration: Value,
    region: String,
    account: u64,
    knowledge_base_id: String,
    created_at: f64,
    updated_at: f64,
    status: String,
}

impl KnowledgeBase {
    /// moto's `KnowledgeBase.__init__` — the configuration-type validations
    /// raise its ValidationException (constant wire message) on failure.
    fn new(account: u64, region: &str, body: &Value) -> Result<Self, ()> {
        let name = vstr(body, "name").unwrap_or_default();
        let kb_config = vnon_null(body, "knowledgeBaseConfiguration")
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let storage_config = vnon_null(body, "storageConfiguration")
            .unwrap_or(Value::Object(serde_json::Map::new()));
        if kb_config.get("type").and_then(Value::as_str) != Some("VECTOR") {
            return Err(());
        }
        if !matches!(
            storage_config.get("type").and_then(Value::as_str),
            Some("OPENSEARCH_SERVERLESS" | "PINECONE" | "REDIS_ENTERPRISE_CLOUD" | "RDS")
        ) {
            return Err(());
        }
        Ok(KnowledgeBase {
            knowledge_base_id: format!("{}{}", name, short_id()),
            name,
            description: vstr(body, "description"),
            role_arn: vstr(body, "roleArn"),
            knowledge_base_configuration: kb_config,
            storage_configuration: storage_config,
            region: region.to_string(),
            account,
            created_at: unix_time(),
            updated_at: unix_time(),
            status: "Active".to_string(),
        })
    }

    fn knowledge_base_arn(&self) -> String {
        knowledge_base_arn(&self.region, self.account, &self.knowledge_base_id)
    }

    /// moto's `to_dict()` — falsy members drop off the wire.
    fn to_dict(&self) -> Value {
        let mut v = json!({
            "knowledgeBaseId": self.knowledge_base_id,
            "name": self.name,
            "knowledgeBaseArn": self.knowledge_base_arn(),
            "description": self.description,
            "roleArn": self.role_arn,
            "knowledgeBaseConfiguration": self.knowledge_base_configuration,
            "storageConfiguration": self.storage_configuration,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "failureReasons": [],
        });
        drop_falsy_mut(&mut v);
        v
    }

    /// moto's `dict_summary()` — falsy members drop.
    fn summary(&self) -> Value {
        let mut v = json!({
            "knowledgeBaseId": self.knowledge_base_id,
            "name": self.name,
            "description": self.description,
            "status": self.status,
            "updatedAt": self.updated_at,
        });
        drop_falsy_mut(&mut v);
        v
    }
}

// ---------------------------------------------------------------------------
// record helpers (moto/core/utils.py + moto's common idioms)
// ---------------------------------------------------------------------------

fn agent_arn(region: &str, account: u64, agent_id: &str) -> String {
    format!(
        "arn:{}:bedrock:{region}:{account}:agent/{agent_id}",
        partition(region)
    )
}

fn knowledge_base_arn(region: &str, account: u64, kb_id: &str) -> String {
    format!(
        "arn:{}:bedrock:{region}:{account}:knowledge-base/{kb_id}",
        partition(region)
    )
}

/// moto's `str(uuid4())[:8]` identifier quirk — the first 8 hex chars of the
/// hyphenated form.
fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// moto's `unix_time()`.
fn unix_time() -> f64 {
    Utc::now().timestamp_micros() as f64 / 1_000_000.0
}

/// moto's `get_partition`.
fn partition(region: &str) -> &'static str {
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

/// moto's `{k: v for k, v in dct.items() if v}` — falsy members drop
/// (`recommendedActions: ["action"]` survives, `failureReasons: []` doesn't).
fn drop_falsy_mut(value: &mut Value) {
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

fn vstr(v: &Value, k: &str) -> Option<String> {
    v.get(k)?.as_str().map(String::from)
}

fn vnon_null(v: &Value, k: &str) -> Option<Value> {
    match v.get(k)? {
        Value::Null => None,
        out => Some(out.clone()),
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    // botocore signs the `bedrock-agent` model with the credential scope
    // `bedrock` (its signing name), so the router delivers those requests on
    // the shared `bedrock` service arm. This module runs first on that arm:
    // the bedrock-agent URI families (/agents, /knowledgebases, /flows,
    // /prompts, /tags/...) are path-disjoint from bedrock's own, so
    // non-matching paths return None and the bedrock backend answers.
    if req.service != "bedrock" && req.service != "bedrock-agent" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        if req.service == "bedrock" {
            // Not a bedrock-agent URI — the bedrock backend takes it.
            return None;
        }
        // moto: no botocore URI matched the method+path — bare 404.
        return Some(Response::new(404, "text/plain", Vec::new()));
    };
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    Some(route(req, op, st))
}

/// Resolve the operation, then route. Only the moto fork's implemented trio
/// reaches its handlers; the rest answer moto's `NotImplementedError`.
fn route(req: &Request, op: &'static str, st: &mut BedrockAgentStore) -> Response {
    let body = parse_json_body(&req.body);
    // moto: resource_arn = unquote(self.path.split("/tags/")[-1]).
    let tag_arn = match req.path.rfind("/tags/") {
        Some(i) => url_decode(&req.path[i + "/tags/".len()..]),
        None => String::new(),
    };
    match op {
        "CreateAgent" => create_agent(req, &body, st),
        "GetAgent" => get_agent(req, st),
        "ListAgents" => list_agents(&body, st),
        "DeleteAgent" => delete_agent(req, st),
        "CreateKnowledgeBase" => create_knowledge_base(req, &body, st),
        "GetKnowledgeBase" => get_knowledge_base(req, st),
        "ListKnowledgeBases" => list_knowledge_bases(&body, st),
        "DeleteKnowledgeBase" => delete_knowledge_base(req, st),
        "TagResource" => tag_resource(&tag_arn, &body, st),
        "UntagResource" => untag_resource(req, &tag_arn, st),
        "ListTagsForResource" => list_tags_for_resource(&tag_arn, st),
        _ => not_implemented(op),
    }
}

/// Operation lookup: the greedy tag routes resolve by prefix, everything
/// else by a (method, segment-count) template match with moto's anchoring —
/// a template's trailing-slash shape must agree with the path's.
fn wire_operation(req: &Request) -> Option<&'static str> {
    if let Some(rest) = req.path.strip_prefix("/tags/") {
        if rest.is_empty() {
            return None;
        }
        return match req.method.as_str() {
            "GET" => Some("ListTagsForResource"),
            "POST" => Some("TagResource"),
            "DELETE" => Some("UntagResource"),
            _ => None,
        };
    }
    RULES
        .iter()
        .find(|(method, uri, _)| {
            *method == req.method.as_str() && template_matches(uri, req.path.as_str())
        })
        .map(|(_, _, op)| *op)
}

/// moto's regex-style URI match (anchored `^...$`): a template matches a
/// path when their trailing-slash shape agrees and every segment matches
/// (literal segments exactly, placeholders on any single segment).
fn template_matches(uri: &str, path: &str) -> bool {
    if uri.ends_with('/') != path.ends_with('/') {
        return false;
    }
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    let tpl: Vec<&str> = uri.trim_matches('/').split('/').collect();
    tpl.len() == segs.len()
        && tpl
            .iter()
            .zip(&segs)
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}')) || t == s)
}

/// moto's `NotImplementedError` as the old sidecar surfaced it: HTTP 501
/// with the snake-case action name (`camelcase_to_underscores`).
fn not_implemented(op: &str) -> Response {
    json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake_case(op)),
    )
}

/// moto's `camelcase_to_underscores` (moto/core/utils.py) — the two
/// boundaries are: an UPPER-lower run seen after any char, and any letter
/// seen after a lower/digit char.
fn snake_case(op: &str) -> String {
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

// ---------------------------------------------------------------------------
// handlers (moto/bedrockagent/responses.py)
// ---------------------------------------------------------------------------

fn create_agent(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let agent = Agent::new(req.account, &req.region, body);
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            let arn = agent.agent_arn();
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    let out = json!({ "agent": agent.to_dict() });
    st.agents.insert(agent.agent_id.clone(), agent);
    ok(out)
}

fn get_agent(req: &Request, st: &BedrockAgentStore) -> Response {
    // moto: agent_id = self.path.split("/")[-2] — the URI carries a slash.
    let agent_id = path_from_end(req.path.as_str(), 2);
    match st.agents.get(&agent_id) {
        Some(agent) => ok(json!({ "agent": agent.to_dict() })),
        None => not_found(&format!("Agent {agent_id} not found")),
    }
}

/// Path segment `n` positions from the tail, kept raw (moto never unquotes
/// agent/knowledge-base ids); 2 is moto's `split("/")[-2]` for the
/// trailing-slash agent URIs, 1 its `split("/")[-1]`.
fn path_from_end(path: &str, n: usize) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    parts
        .get(parts.len().saturating_sub(n))
        .copied()
        .unwrap_or_default()
        .to_string()
}

fn list_agents(body: &Value, st: &BedrockAgentStore) -> Response {
    let summaries: Vec<Value> = st.agents.values().map(Agent::summary).collect();
    match paginate(summaries, "agentId", body) {
        Ok((page, token)) => ok(json!({ "agentSummaries": page, "nextToken": token })),
        Err(resp) => resp,
    }
}

fn delete_agent(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let agent_id = path_from_end(req.path.as_str(), 2);
    let skip =
        query_first(&query_map(&req.query_string), "skipResourceInUseCheck").map(|v| v == "true");
    let Some(agent) = st.agents.get(&agent_id) else {
        return not_found(&format!("Agent {agent_id} not found"));
    };
    // moto: skip_resource_in_use_check or the agent must be PREPARED.
    if skip.unwrap_or(false) || agent.agent_status == "PREPARED" {
        // moto flips the status to DELETING, then deletes — the response
        // carries the pre-delete status.
        st.agents.remove(&agent_id);
        ok(json!({ "agentId": agent_id, "agentStatus": "DELETING" }))
    } else {
        conflict(&format!("Agent {agent_id} is in use"))
    }
}

fn create_knowledge_base(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let kb = match KnowledgeBase::new(req.account, &req.region, body) {
        Ok(kb) => kb,
        Err(()) => {
            // moto's ValidationException — the constraint detail is a
            // misused template arg and never reaches the wire.
            return validation();
        }
    };
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            let arn = kb.knowledge_base_arn();
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    let out = json!({ "knowledgeBase": kb.to_dict() });
    st.knowledge_bases.insert(kb.knowledge_base_id.clone(), kb);
    ok(out)
}

fn get_knowledge_base(req: &Request, st: &BedrockAgentStore) -> Response {
    // moto: knowledge_base_id = self.path.split("/")[-1].
    let kb_id = path_from_end(req.path.as_str(), 1);
    match st.knowledge_bases.get(&kb_id) {
        Some(kb) => ok(json!({ "knowledgeBase": kb.to_dict() })),
        None => not_found(&format!("Knowledge base {kb_id} not found")),
    }
}

fn list_knowledge_bases(body: &Value, st: &BedrockAgentStore) -> Response {
    let summaries: Vec<Value> = st
        .knowledge_bases
        .values()
        .map(KnowledgeBase::summary)
        .collect();
    match paginate(summaries, "knowledgeBaseId", body) {
        Ok((page, token)) => ok(json!({ "knowledgeBaseSummaries": page, "nextToken": token })),
        Err(resp) => resp,
    }
}

fn delete_knowledge_base(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let kb_id = path_from_end(req.path.as_str(), 1);
    match st.knowledge_bases.remove(&kb_id) {
        Some(_) => ok(json!({ "knowledgeBaseId": kb_id, "status": "DELETING" })),
        None => not_found(&format!("Knowledge base {kb_id} not found")),
    }
}

fn tag_resource(tag_arn: &str, body: &Value, st: &mut BedrockAgentStore) -> Response {
    // moto validates the ARN against live agent/knowledge-base ARNs first.
    if !arn_exists(st, tag_arn) {
        return not_found(&format!("Resource {tag_arn} not found"));
    }
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        // moto's `tags or {}` keeps the row for an empty dict, but an empty
        // row is observably identical to no row.
        for (key, value) in tags {
            tag_one(st, tag_arn, key, value.clone());
        }
    }
    ok(json!({}))
}

fn untag_resource(req: &Request, tag_arn: &str, st: &mut BedrockAgentStore) -> Response {
    if !arn_exists(st, tag_arn) {
        return not_found(&format!("Resource {tag_arn} not found"));
    }
    // moto: tag_keys = self.querystring.get("tagKeys", []).
    if let Some(row) = st.tags.get_mut(tag_arn) {
        for key in query_map(&req.query_string)
            .get("tagKeys")
            .cloned()
            .unwrap_or_default()
        {
            row.remove(&key);
        }
    }
    ok(json!({}))
}

fn list_tags_for_resource(tag_arn: &str, st: &BedrockAgentStore) -> Response {
    if !arn_exists(st, tag_arn) {
        return not_found(&format!("Resource {tag_arn} not found"));
    }
    let tags = st
        .tags
        .get(tag_arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<serde_json::Map<String, Value>>()
        })
        .unwrap_or_default();
    ok(json!({ "tags": tags }))
}

fn tag_one(st: &mut BedrockAgentStore, arn: &str, key: &str, value: Value) {
    st.tags
        .entry(arn.to_string())
        .or_default()
        .insert(key.to_string(), value);
}

/// moto's `_list_arns()` — the ARN must belong to a live agent or
/// knowledge base.
fn arn_exists(st: &BedrockAgentStore, arn: &str) -> bool {
    st.agents.values().any(|a| a.agent_arn() == arn)
        || st
            .knowledge_bases
            .values()
            .any(|k| k.knowledge_base_arn() == arn)
}

// ---------------------------------------------------------------------------
// pagination (moto's Paginator over `unique_attribute` ids)
// ---------------------------------------------------------------------------

/// moto's `paginate()` for `list_agents`/`list_knowledge_bases`: a page of
/// `maxResults` (default 100) summaries, resuming at the record whose id
/// matches the token-carried `{"uniqueAttributes": "<id>"}`.
fn paginate(
    items: Vec<Value>,
    id_field: &str,
    body: &Value,
) -> Result<(Vec<Value>, Option<String>), Response> {
    // moto: max_results = int(maxResults) if maxResults else None → the
    // PAGINATION_MODEL's 100 default (0/absent fall back, mirroring
    // Python's falsy check).
    let max_results = body
        .get("maxResults")
        .and_then(Value::as_i64)
        .filter(|n| *n > 0)
        .unwrap_or(100) as usize;
    let mut start = 0usize;
    // moto's starting token: an empty/absent nextToken skips the resume.
    if let Some(attrs) = vstr(body, "nextToken").filter(|t| !t.is_empty()) {
        let attrs = decode_token(&attrs)?;
        match items
            .iter()
            .position(|item| vstr(item, id_field).as_deref() == Some(attrs.as_str()))
        {
            Some(i) => start = i,
            None => return Err(invalid_token("Resource not found!")),
        }
    }
    let end = (start + max_results).min(items.len());
    let page = items[start..end].to_vec();
    // moto builds the token from the first record *after this page*.
    let token = if !page.is_empty() && end < items.len() {
        Some(B64.encode(json!({ "uniqueAttributes": vstr(&items[end], id_field) }).to_string()))
    } else {
        None
    };
    Ok((page, token))
}

/// Decode moto's base64 `{"uniqueAttributes": "..."}` token: standard
/// base64 of the encoded JSON. Decode failure carries moto's
/// `InvalidToken("Invalid token")` shape straight onto the wire.
fn decode_token(token: &str) -> Result<String, Response> {
    let bytes = B64
        .decode(token)
        .map_err(|_| invalid_token("Invalid token"))?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid_token("Invalid token"))?;
    Ok(vstr(&value, "uniqueAttributes").unwrap_or_default())
}

fn invalid_token(detail: &str) -> Response {
    json_error(400, "InvalidToken", &format!("Invalid Token: {detail}"))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

fn conflict(message: &str) -> Response {
    json_error(400, "ConflictException", message)
}

fn validation() -> Response {
    // moto's Bedrock-agent ValidationException message constant (the detail
    // is a misused template arg and never reaches the wire).
    json_error(
        400,
        "ValidationException",
        "Input validation failed. Check your request parameters and retry the request.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Unique account per test: the state slot is process-global, so tests
    // running concurrently must not share a scope.
    fn reqacct(method: &str, path: &str, query: &str, body: Value, account: u64) -> Request {
        Request {
            service: "bedrock-agent".into(),
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
    fn agent_lifecycle() {
        let account = 123456789001;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({
                "agentName": "my-agent",
                "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x",
                "instruction": "do things",
                "idleSessionTTLInSeconds": 600,
            }),
            account,
        )));
        let agent = &created["agent"];
        assert_eq!(agent["agentName"], "my-agent");
        assert_eq!(agent["agentStatus"], "PREPARED");
        assert_eq!(agent["agentVersion"], "1.0");
        assert_eq!(agent["recommendedActions"], json!(["action"]));
        assert!(agent["failureReasons"].is_null()); // falsy-dropped
        assert!(agent["agentId"].as_str().unwrap().starts_with("my-agent"));
        let agent_id = agent["agentId"].as_str().unwrap().to_string();
        let agent_arn = agent["agentArn"].as_str().unwrap().to_string();
        assert!(agent_arn.ends_with(&format!("agent/{agent_id}")));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["agent"]["agentId"], agent_id.as_str());

        let listed = body(dispatch(&reqacct(
            "POST",
            "/agents/",
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["agentSummaries"].as_array().unwrap().len(), 1);
        assert!(listed["nextToken"].is_null());

        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/agents/{agent_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["agentStatus"], "DELETING");

        // moto: the record is gone afterwards.
        assert_eq!(
            status(dispatch(&reqacct(
                "GET",
                &format!("/agents/{agent_id}/"),
                "",
                json!({}),
                account,
            ))),
            400
        );
        assert_eq!(
            body(dispatch(&reqacct(
                "GET",
                &format!("/agents/{agent_id}/"),
                "",
                json!({}),
                account,
            )))["__type"],
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn knowledge_base_validation() {
        let account = 123456789002;
        let kb = &body(dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "kb",
                "roleArn": "arn:aws:iam::123456789012:role/x",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "PINECONE"}
            }),
            account,
        )))["knowledgeBase"];
        assert_eq!(kb["status"], "Active");
        assert!(kb["knowledgeBaseId"].as_str().unwrap().starts_with("kb"));

        // Non-VECTOR knowledgeBaseConfiguration -> moto's ValidationException
        // with the constant message.
        let resp = dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "kb2",
                "knowledgeBaseConfiguration": {"type": "NOT_VECTOR"},
                "storageConfiguration": {"type": "PINECONE"}
            }),
            account,
        ));
        assert_eq!(status(resp), 400);
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                "/knowledgebases/",
                "",
                json!({
                    "name": "kb2",
                    "knowledgeBaseConfiguration": {"type": "NOT_VECTOR"},
                    "storageConfiguration": {"type": "PINECONE"}
                }),
                account,
            )))["message"],
            "Input validation failed. Check your request parameters and retry the request."
        );

        // Bad storageConfiguration type -> the same shape.
        let resp = dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "kb3",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "MYSQL"}
            }),
            account,
        ));
        assert_eq!(status(resp), 400);
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                "/knowledgebases/",
                "",
                json!({
                    "name": "kb3",
                    "knowledgeBaseConfiguration": {"type": "VECTOR"},
                    "storageConfiguration": {"type": "MYSQL"}
                }),
                account,
            )))["__type"],
            "ValidationException"
        );
    }

    #[test]
    fn tags_roundtrip() {
        let account = 123456789003;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "kb",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "RDS"},
                "tags": {"team": "triage"}
            }),
            account,
        )));
        let arn = created["knowledgeBase"]["knowledgeBaseArn"]
            .as_str()
            .unwrap()
            .to_string();

        let tags = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{arn}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(tags["tags"]["team"], "triage");

        // Overwrite a key and add one (moto's TaggingService upserts).
        dispatch(&reqacct(
            "POST",
            &format!("/tags/{arn}"),
            "",
            json!({"tags": {"team": "t2", "extra": "x"}}),
            account,
        ))
        .unwrap();
        let tags = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{arn}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(tags["tags"], json!({"team": "t2", "extra": "x"}));

        // UntagResource reads tagKeys from the query string.
        dispatch(&reqacct(
            "DELETE",
            &format!("/tags/{arn}"),
            "tagKeys=team",
            json!({}),
            account,
        ))
        .unwrap();
        let tags = body(dispatch(&reqacct(
            "GET",
            &format!("/tags/{arn}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(tags["tags"], json!({"extra": "x"}));

        // An ARN owned by no live resource answers moto's ResourceNotFound.
        assert_eq!(
            status(dispatch(&reqacct(
                "GET",
                "/tags/arn:aws:bedrock:us-east-1:1:agent/nope",
                "",
                json!({}),
                account,
            ))),
            400
        );
        assert_eq!(
            body(dispatch(&reqacct(
                "GET",
                "/tags/arn:aws:bedrock:us-east-1:1:agent/nope",
                "",
                json!({}),
                account,
            )))["__type"],
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn agent_pagination() {
        let account = 123456789004;
        for name in ["a1", "a2", "a3"] {
            dispatch(&reqacct(
                "PUT",
                "/agents/",
                "",
                json!({ "agentName": name }),
                account,
            ))
            .unwrap();
        }
        let page1 = body(dispatch(&reqacct(
            "POST",
            "/agents/",
            "",
            json!({ "maxResults": 2 }),
            account,
        )));
        assert_eq!(page1["agentSummaries"].as_array().unwrap().len(), 2);
        let token = page1["nextToken"].as_str().unwrap().to_string();

        let page2 = body(dispatch(&reqacct(
            "POST",
            "/agents/",
            "",
            json!({ "maxResults": 2, "nextToken": token }),
            account,
        )));
        assert_eq!(page2["agentSummaries"].as_array().unwrap().len(), 1);
        assert!(page2["nextToken"].is_null());

        // A token that does not decode as moto's base64 token raises
        // InvalidToken("Invalid token") - 400 InvalidToken.
        let failed = body(dispatch(&reqacct(
            "POST",
            "/agents/",
            "",
            json!({ "nextToken": "not a real token" }),
            account,
        )));
        assert_eq!(failed["__type"], "InvalidToken");

        // A token whose record no longer exists -> "Resource not found!".
        let stale = B64.encode(json!({ "uniqueAttributes": "no-such-id" }).to_string());
        let resp = dispatch(&reqacct(
            "POST",
            "/agents/",
            "",
            json!({ "nextToken": stale }),
            account,
        ));
        assert_eq!(status(resp), 400);
        assert_eq!(
            body(dispatch(&reqacct(
                "POST",
                "/agents/",
                "",
                json!({ "nextToken": stale }),
                account,
            )))["message"],
            "Invalid Token: Resource not found!"
        );
    }

    #[test]
    fn unimplemented_ops_answer_moto_501() {
        let account = 123456789005;
        // PUT /agents/{agentId}/ routes as UpdateAgent, which has no moto
        // handler -> 501 with the snake-case action name.
        assert_eq!(
            status(dispatch(&reqacct(
                "PUT",
                "/agents/abc123/",
                "",
                json!({}),
                account
            ))),
            501
        );
        assert_eq!(
            body(dispatch(&reqacct(
                "PUT",
                "/agents/abc123/",
                "",
                json!({}),
                account
            )))["message"],
            "The update_agent action has not been implemented"
        );

        // A trailing-slash shape that no botocore URI carries answers
        // moto's bare 404.
        assert_eq!(
            status(dispatch(&reqacct("PUT", "/agents", "", json!({}), account))),
            404
        );
    }
}
