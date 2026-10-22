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
    // --- modeled families (moto implements none of these; records are
    // plain robotocore Value dicts keyed by composite ids) ---
    /// `agentId|actionGroupId` -> action group record
    action_groups: BTreeMap<String, Value>,
    /// `agentId|agentAliasId` -> agent alias record
    agent_aliases: BTreeMap<String, Value>,
    /// `agentId|version|collaboratorId` -> agent collaborator record
    collaborators: BTreeMap<String, Value>,
    /// `agentId|version|knowledgeBaseId` -> agent knowledge-base record
    agent_knowledge_bases: BTreeMap<String, Value>,
    /// `knowledgeBaseId|dataSourceId` -> data source record
    data_sources: BTreeMap<String, Value>,
    /// `knowledgeBaseId|dataSourceId|docIdentifier` -> document detail
    documents: BTreeMap<String, Value>,
    /// `knowledgeBaseId|dataSourceId|ingestionJobId` -> ingestion job
    ingestion_jobs: BTreeMap<String, Value>,
    /// flow id -> flow record (versions live in `flow_versions` too)
    flows: BTreeMap<String, Value>,
    /// `flowId|version` -> flow version record
    flow_versions: BTreeMap<String, Value>,
    /// `flowId|flowAliasId` -> flow alias record
    flow_aliases: BTreeMap<String, Value>,
    /// prompt id -> prompt record (its wire body carries the DRAFT version)
    prompts: BTreeMap<String, Value>,
    /// `promptId|version` -> every named version of a prompt
    prompt_versions: BTreeMap<String, Value>,
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
        // ------------------------------------------------------------------
        // agent-side modeled families (moto has no handler)
        // ------------------------------------------------------------------
        "UpdateAgent" => update_agent(req, &body, st),
        "PrepareAgent" => prepare_agent(req, st),
        "ListAgentVersions" => list_agent_versions(req, st),
        "GetAgentVersion" => get_agent_version(req, st),
        "DeleteAgentVersion" => delete_agent_version(req, st),
        "CreateAgentAlias" => create_agent_alias(req, &body, st),
        "GetAgentAlias" => get_agent_alias(req, st),
        "UpdateAgentAlias" => update_agent_alias(req, &body, st),
        "DeleteAgentAlias" => delete_agent_alias(req, st),
        "ListAgentAliases" => list_agent_aliases(req, st),
        "CreateAgentActionGroup" => create_agent_action_group(req, &body, st),
        "GetAgentActionGroup" => get_agent_action_group(req, st),
        "UpdateAgentActionGroup" => update_agent_action_group(req, &body, st),
        "DeleteAgentActionGroup" => delete_agent_action_group(req, st),
        "ListAgentActionGroups" => list_agent_action_groups(req, st),
        "AssociateAgentCollaborator" => associate_agent_collaborator(req, &body, st),
        "GetAgentCollaborator" => get_agent_collaborator(req, st),
        "UpdateAgentCollaborator" => update_agent_collaborator(req, &body, st),
        "DisassociateAgentCollaborator" => disassociate_agent_collaborator(req, st),
        "ListAgentCollaborators" => list_agent_collaborators(req, st),
        "AssociateAgentKnowledgeBase" => associate_agent_knowledge_base(req, &body, st),
        "GetAgentKnowledgeBase" => get_agent_knowledge_base(req, st),
        "UpdateAgentKnowledgeBase" => update_agent_knowledge_base(req, &body, st),
        "DisassociateAgentKnowledgeBase" => disassociate_agent_knowledge_base(req, st),
        "ListAgentKnowledgeBases" => list_agent_knowledge_bases(req, st),
        // ------------------------------------------------------------------
        // knowledge-base side modeled families
        // ------------------------------------------------------------------
        "UpdateKnowledgeBase" => update_knowledge_base(req, &body, st),
        "CreateDataSource" => create_data_source(req, &body, st),
        "GetDataSource" => get_data_source(req, st),
        "UpdateDataSource" => update_data_source(req, &body, st),
        "DeleteDataSource" => delete_data_source(req, st),
        "ListDataSources" => list_data_sources(req, st),
        "IngestKnowledgeBaseDocuments" => ingest_documents(req, &body, st),
        "GetKnowledgeBaseDocuments" => get_kb_documents(req, &body, st),
        "DeleteKnowledgeBaseDocuments" => delete_kb_documents(req, &body, st),
        "ListKnowledgeBaseDocuments" => list_kb_documents(req, st),
        "StartIngestionJob" => start_ingestion_job(req, &body, st),
        "StopIngestionJob" => stop_ingestion_job(req, st),
        "GetIngestionJob" => get_ingestion_job(req, st),
        "ListIngestionJobs" => list_ingestion_jobs(req, &body, st),
        // ------------------------------------------------------------------
        // flows and prompts
        // ------------------------------------------------------------------
        "CreateFlow" => create_flow(req, &body, st),
        "GetFlow" => get_flow(req, st),
        "UpdateFlow" => update_flow(req, &body, st),
        "PrepareFlow" => prepare_flow(req, st),
        "DeleteFlow" => delete_flow(req, st),
        "ListFlows" => list_flows(&body, st),
        "CreateFlowVersion" => create_flow_version(req, &body, st),
        "GetFlowVersion" => get_flow_version(req, st),
        "ListFlowVersions" => list_flow_versions(req, st),
        "DeleteFlowVersion" => delete_flow_version(req, st),
        "CreateFlowAlias" => create_flow_alias(req, &body, st),
        "GetFlowAlias" => get_flow_alias(req, st),
        "UpdateFlowAlias" => update_flow_alias(req, &body, st),
        "DeleteFlowAlias" => delete_flow_alias(req, st),
        "ListFlowAliases" => list_flow_aliases(req, st),
        "ValidateFlowDefinition" => validate_flow_definition(&body),
        "CreatePrompt" => create_prompt(req, &body, st),
        "GetPrompt" => get_prompt(req, st),
        "UpdatePrompt" => update_prompt(req, &body, st),
        "DeletePrompt" => delete_prompt(req, st),
        "ListPrompts" => list_prompts(req, &body, st),
        "CreatePromptVersion" => create_prompt_version(req, &body, st),
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

// ---------------------------------------------------------------------------
// path + record helpers (modeled families)
// ---------------------------------------------------------------------------

/// Non-empty path segments (moto left agent/knowledge-base ids
/// percent-encoded; splitting on '/' is enough for id lookups).
fn path_segs(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// `n`-th non-empty path segment counted from the tail (1-based).
fn seg_from_end(path: &str, n: usize) -> String {
    let segs = path_segs(path);
    match segs.len().checked_sub(n) {
        Some(i) => segs[i].to_string(),
        None => String::new(),
    }
}

/// Echo the listed body members into a fresh record map.
fn wire_row(echo: &[&str], body: &Value, extra: &[(&str, Value)]) -> Value {
    let mut map = serde_json::Map::new();
    for key in echo {
        if let Some(v) = body.get(*key) {
            map.insert((*key).to_string(), v.clone());
        }
    }
    for (key, v) in extra {
        map.insert(key.to_string(), v.clone());
    }
    Value::Object(map)
}

fn bedrock_arn(kind: &str, region: &str, account: u64, id: &str) -> String {
    format!(
        "arn:{}:bedrock:{region}:{account}:{kind}/{}",
        partition(region),
        id
    )
}

fn require_agent<'a>(st: &'a BedrockAgentStore, agent_id: &str) -> Result<&'a Agent, Response> {
    match st.agents.get(agent_id) {
        Some(agent) => Ok(agent),
        None => Err(not_found(&format!("Agent {agent_id} not found"))),
    }
}

fn require_kb<'a>(st: &'a BedrockAgentStore, kb_id: &str) -> Result<&'a KnowledgeBase, Response> {
    match st.knowledge_bases.get(kb_id) {
        Some(kb) => Ok(kb),
        None => Err(not_found(&format!("Knowledge base {kb_id} not found"))),
    }
}

fn require_flow<'a>(st: &'a BedrockAgentStore, flow_id: &str) -> Result<&'a Value, Response> {
    match st.flows.get(flow_id) {
        Some(flow) => Ok(flow),
        None => Err(not_found(&format!("Flow {flow_id} not found"))),
    }
}

fn require_data_source<'a>(
    st: &'a BedrockAgentStore,
    kb_id: &str,
    ds_id: &str,
) -> Result<&'a Value, Response> {
    let key = format!("{kb_id}|{ds_id}");
    match st.data_sources.get(&key) {
        Some(ds) => Ok(ds),
        None => Err(not_found(&format!("Data source {ds_id} not found"))),
    }
}

fn require_ingestion_job<'a>(
    st: &'a BedrockAgentStore,
    kb_id: &str,
    ds_id: &str,
    job_id: &str,
) -> Result<&'a Value, Response> {
    let key = format!("{kb_id}|{ds_id}|{job_id}");
    match st.ingestion_jobs.get(&key) {
        Some(job) => Ok(job),
        None => Err(not_found(&format!("Ingestion job {job_id} not found"))),
    }
}

// ---------------------------------------------------------------------------
// agent-side modeled families (moto has no handler; botocore shapes rule)
// ---------------------------------------------------------------------------

/// PUT /agents/{agentId}/ — UpdateAgent. Only declared-update members move;
/// moto-born agents keep their id/created-at.
fn update_agent(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let agent_id = path_from_end(req.path.as_str(), 2);
    let Some(agent) = st.agents.get_mut(&agent_id) else {
        return not_found(&format!("Agent {agent_id} not found"));
    };
    agent.agent_name = vstr(body, "agentName").unwrap_or_else(|| agent.agent_name.clone());
    if let Some(v) = vstr(body, "foundationModel") {
        agent.foundation_model = Some(v);
    }
    if let Some(v) = vstr(body, "instruction") {
        agent.instruction = Some(v);
    }
    if let Some(v) = vstr(body, "description") {
        agent.description = Some(v);
    }
    if let Some(v) = body.get("idleSessionTTLInSeconds").and_then(Value::as_i64) {
        agent.idle_session_ttl_in_seconds = Some(v);
    }
    if let Some(v) = vstr(body, "agentResourceRoleArn") {
        agent.agent_resource_role_arn = Some(v);
    }
    if let Some(v) = vstr(body, "customerEncryptionKeyArn") {
        agent.customer_encryption_key_arn = Some(v);
    }
    if let Some(v) = vnon_null(body, "promptOverrideConfiguration") {
        agent.prompt_override_configuration = Some(v);
    }
    agent.updated_at = unix_time();
    let out = json!({ "agent": agent.to_dict() });
    ok(out)
}

/// POST /agents/{agentId}/ — PrepareAgent. Agents are born PREPARED, so the
/// prepare re-affirmation answers the settled state (moto's to_dict swap).
fn prepare_agent(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let agent_id = path_from_end(req.path.as_str(), 2);
    let Some(agent) = st.agents.get_mut(&agent_id) else {
        return not_found(&format!("Agent {agent_id} not found"));
    };
    agent.agent_status = "PREPARED".to_string();
    agent.updated_at = unix_time();
    agent.prepared_at = unix_time();
    ok(json!({
        "agentId": agent_id,
        "agentStatus": "PREPARED",
        "agentVersion": agent.agent_version,
        "preparedAt": agent.prepared_at,
    }))
}

/// POST /agents/{agentId}/agentversions/ — ListAgentVersions. The model
/// keeps a single live DRAFT version ("1.0") per agent.
fn list_agent_versions(req: &Request, st: &BedrockAgentStore) -> Response {
    let agent_id = seg_from_end(req.path.as_str(), 2);
    let agent = match require_agent(st, &agent_id) {
        Ok(agent) => agent,
        Err(resp) => return resp,
    };
    ok(json!({
        "agentVersionSummaries": [{
            "agentName": agent.agent_name,
            "agentStatus": agent.agent_status,
            "agentVersion": agent.agent_version,
            "createdAt": agent.created_at,
            "updatedAt": agent.updated_at,
        }],
        "nextToken": Value::Null,
    }))
}

/// GET /agents/{agentId}/agentversions/{agentVersion}/ — GetAgentVersion.
fn get_agent_version(req: &Request, st: &BedrockAgentStore) -> Response {
    let version = seg_from_end(req.path.as_str(), 1);
    let agent_id = seg_from_end(req.path.as_str(), 3);
    let Some(agent) = st.agents.get(&agent_id) else {
        return not_found(&format!("Agent {agent_id} not found"));
    };
    if version != agent.agent_version {
        return not_found(&format!("Agent version {version} not found"));
    }
    let mut v = agent.to_dict();
    if let Some(map) = v.as_object_mut() {
        if let Some(version) = map.remove("agentVersion") {
            map.insert("version".to_string(), version);
        }
    }
    ok(json!({ "agentVersion": v }))
}

/// DELETE /agents/{agentId}/agentversions/{agentVersion}/ — the only live
/// version leaves, so the agent record goes with it (status DELETING).
fn delete_agent_version(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let agent_id = seg_from_end(req.path.as_str(), 3);
    let version = seg_from_end(req.path.as_str(), 1);
    let Some(agent) = st.agents.get(&agent_id) else {
        return not_found(&format!("Agent {agent_id} not found"));
    };
    if version != agent.agent_version {
        return not_found(&format!("Agent version {version} not found"));
    }
    st.agents.remove(&agent_id);
    ok(json!({
        "agentId": agent_id,
        "agentVersion": version,
        "agentStatus": "DELETING",
    }))
}

// --- aliases -----------------------------------------------------------------

fn create_agent_alias(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let agent_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let alias_name = vstr(body, "agentAliasName").unwrap_or_default();
    let alias_id = format!("{}{}", alias_name, short_id());
    let arn = bedrock_arn(
        "agent-alias",
        &req.region,
        req.account,
        &format!("{agent_id}/{alias_id}"),
    );
    let record = wire_row(
        &["description", "routingConfiguration", "clientToken"],
        body,
        &[
            ("agentId", json!(agent_id)),
            ("agentAliasId", json!(alias_id)),
            ("agentAliasName", json!(alias_name)),
            ("agentAliasArn", json!(arn)),
            ("agentAliasStatus", json!("PREPARED")),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    st.agent_aliases
        .insert(format!("{agent_id}|{alias_id}"), record.clone());
    ok(json!({ "agentAlias": record }))
}

fn agent_and_alias_ids(req: &Request) -> (String, String) {
    // GET/PUT/DELETE /agents/{agentId}/agentaliases/{agentAliasId}/
    let alias_id = seg_from_end(req.path.as_str(), 1);
    let agent_id = seg_from_end(req.path.as_str(), 3);
    (agent_id, alias_id)
}

fn get_agent_alias(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, alias_id) = agent_and_alias_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    match st.agent_aliases.get(&format!("{agent_id}|{alias_id}")) {
        Some(alias) => ok(json!({ "agentAlias": alias })),
        None => not_found(&format!("Agent alias {alias_id} not found")),
    }
}

fn update_agent_alias(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, alias_id) = agent_and_alias_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{alias_id}");
    let Some(alias) = st.agent_aliases.get_mut(&key) else {
        return not_found(&format!("Agent alias {alias_id} not found"));
    };
    if let Some(v) = vstr(body, "agentAliasName") {
        alias["agentAliasName"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        alias["description"] = json!(v);
    }
    if let Some(v) = body.get("routingConfiguration") {
        alias["routingConfiguration"] = v.clone();
    }
    if let Some(v) = vstr(body, "aliasInvocationState") {
        alias["aliasInvocationState"] = json!(v);
    }
    alias["updatedAt"] = json!(unix_time());
    ok(json!({ "agentAlias": alias.clone() }))
}

fn delete_agent_alias(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, alias_id) = agent_and_alias_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{alias_id}");
    match st.agent_aliases.remove(&key) {
        Some(_) => ok(json!({
            "agentId": agent_id,
            "agentAliasId": alias_id,
            "agentAliasStatus": "DELETING",
        })),
        None => not_found(&format!("Agent alias {alias_id} not found")),
    }
}

/// POST /agents/{agentId}/agentaliases/ — ListAgentAliases.
fn list_agent_aliases(req: &Request, st: &BedrockAgentStore) -> Response {
    let agent_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .agent_aliases
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{agent_id}|")))
        .map(|(k, alias)| {
            let _ = k;
            json!({
                "agentAliasId": alias["agentAliasId"],
                "agentAliasName": alias["agentAliasName"],
                "description": alias.get("description").cloned().unwrap_or(Value::Null),
                "routingConfiguration": alias.get("routingConfiguration").cloned().unwrap_or(json!([])),
                "agentAliasStatus": alias["agentAliasStatus"],
                "createdAt": alias["createdAt"],
                "updatedAt": alias["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "agentAliasSummaries": rows, "nextToken": Value::Null }))
}

// --- action groups ------------------------------------------------------------

/// The (agentId, agentVersion) pair for /agents/{id}/agentversions/{v}/... URIs.
fn agent_version_ids(req: &Request, depth_from_tail_of_version: usize) -> (String, String) {
    // depth counts segments from the tail to the version segment: 2 for
    // URIs ending at the version (…agentversions/{v}/), 3 for one more
    // child (…actiongroups/{g}/), 5 for two.
    let version = seg_from_end(req.path.as_str(), depth_from_tail_of_version);
    // The agent id always sits two segments behind the version value.
    let agent = seg_from_end(req.path.as_str(), depth_from_tail_of_version + 2);
    (agent, version)
}

fn action_group_rows(group: &Value, want_summary: bool) -> Value {
    if want_summary {
        json!({
            "actionGroupId": group["actionGroupId"],
            "actionGroupName": group["actionGroupName"],
            "actionGroupState": group["actionGroupState"],
            "description": group.get("description").cloned().unwrap_or(Value::Null),
            "updatedAt": group["updatedAt"],
        })
    } else {
        group.clone()
    }
}

fn create_agent_action_group(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let group_name = vstr(body, "actionGroupName").unwrap_or_default();
    let group_id = short_id();
    let record = wire_row(
        &[
            "clientToken",
            "description",
            "parentActionGroupSignature",
            "parentActionGroupSignatureParams",
            "actionGroupExecutor",
            "apiSchema",
            "functionSchema",
        ],
        body,
        &[
            ("agentId", json!(agent_id)),
            ("agentVersion", json!(version)),
            ("actionGroupId", json!(group_id)),
            ("actionGroupName", json!(group_name)),
            (
                "actionGroupState",
                json!(vstr(body, "actionGroupState").unwrap_or_else(|| "ENABLED".to_string())),
            ),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    st.action_groups
        .insert(format!("{agent_id}|{version}|{group_id}"), record.clone());
    ok(json!({ "agentActionGroup": record }))
}

fn get_agent_action_group(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let group_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    match st
        .action_groups
        .get(&format!("{agent_id}|{version}|{group_id}"))
    {
        Some(group) => ok(json!({ "agentActionGroup": group })),
        None => not_found(&format!("Action group {group_id} not found")),
    }
}

fn update_agent_action_group(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let group_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{group_id}");
    let Some(group) = st.action_groups.get_mut(&key) else {
        return not_found(&format!("Action group {group_id} not found"));
    };
    if let Some(v) = vstr(body, "actionGroupName") {
        group["actionGroupName"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        group["description"] = json!(v);
    }
    if let Some(v) = vstr(body, "parentActionGroupSignature") {
        group["parentActionGroupSignature"] = json!(v);
    }
    if let Some(v) = body.get("parentActionGroupSignatureParams") {
        group["parentActionGroupSignatureParams"] = v.clone();
    }
    if let Some(v) = body.get("actionGroupExecutor") {
        group["actionGroupExecutor"] = v.clone();
    }
    if let Some(v) = body.get("apiSchema") {
        group["apiSchema"] = v.clone();
    }
    if let Some(v) = body.get("functionSchema") {
        group["functionSchema"] = v.clone();
    }
    if let Some(v) = vstr(body, "actionGroupState") {
        group["actionGroupState"] = json!(v);
    }
    group["updatedAt"] = json!(unix_time());
    ok(json!({ "agentActionGroup": group.clone() }))
}

fn delete_agent_action_group(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let group_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{group_id}");
    match st.action_groups.remove(&key) {
        Some(_) => Response::json(200, Vec::new()),
        None => not_found(&format!("Action group {group_id} not found")),
    }
}

fn list_agent_action_groups(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .action_groups
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{agent_id}|{version}|")))
        .map(|(_, g)| action_group_rows(g, true))
        .collect();
    ok(json!({ "actionGroupSummaries": rows, "nextToken": Value::Null }))
}

// --- agent collaborators ------------------------------------------------------

fn associate_agent_collaborator(
    req: &Request,
    body: &Value,
    st: &mut BedrockAgentStore,
) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let collaborator_name = vstr(body, "collaboratorName").unwrap_or_default();
    let collaborator_id = short_id();
    let record = wire_row(
        &[
            "agentDescriptor",
            "collaborationInstruction",
            "relayConversationHistory",
            "clientToken",
        ],
        body,
        &[
            ("agentId", json!(agent_id)),
            ("agentVersion", json!(version)),
            ("collaboratorId", json!(collaborator_id)),
            ("collaboratorName", json!(collaborator_name)),
            ("createdAt", json!(unix_time())),
            ("lastUpdatedAt", json!(unix_time())),
        ],
    );
    st.collaborators.insert(
        format!("{agent_id}|{version}|{collaborator_id}"),
        record.clone(),
    );
    ok(json!({ "agentCollaborator": record }))
}

fn get_agent_collaborator(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let collaborator_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    match st
        .collaborators
        .get(&format!("{agent_id}|{version}|{collaborator_id}"))
    {
        Some(collab) => ok(json!({ "agentCollaborator": collab })),
        None => not_found(&format!("Collaborator {collaborator_id} not found")),
    }
}

fn update_agent_collaborator(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let collaborator_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{collaborator_id}");
    let Some(collab) = st.collaborators.get_mut(&key) else {
        return not_found(&format!("Collaborator {collaborator_id} not found"));
    };
    if let Some(v) = vstr(body, "collaboratorName") {
        collab["collaboratorName"] = json!(v);
    }
    if let Some(v) = vstr(body, "collaborationInstruction") {
        collab["collaborationInstruction"] = json!(v);
    }
    if let Some(v) = body.get("agentDescriptor") {
        collab["agentDescriptor"] = v.clone();
    }
    if let Some(v) = vstr(body, "relayConversationHistory") {
        collab["relayConversationHistory"] = json!(v);
    }
    collab["lastUpdatedAt"] = json!(unix_time());
    ok(json!({ "agentCollaborator": collab.clone() }))
}

fn disassociate_agent_collaborator(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 3);
    let collaborator_id = seg_from_end(req.path.as_str(), 1);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{collaborator_id}");
    match st.collaborators.remove(&key) {
        Some(_) => Response::json(200, Vec::new()),
        None => not_found(&format!("Collaborator {collaborator_id} not found")),
    }
}

fn list_agent_collaborators(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .collaborators
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{agent_id}|{version}|")))
        .map(|(_, c)| {
            json!({
                "agentId": c["agentId"],
                "agentVersion": c["agentVersion"],
                "collaboratorId": c["collaboratorId"],
                "agentDescriptor": c["agentDescriptor"],
                "collaborationInstruction": c["collaborationInstruction"],
                "relayConversationHistory": c.get("relayConversationHistory").cloned().unwrap_or(json!(null)),
                "collaboratorName": c["collaboratorName"],
                "createdAt": c["createdAt"],
                "lastUpdatedAt": c["lastUpdatedAt"],
            })
        })
        .collect();
    ok(json!({ "agentCollaboratorSummaries": rows, "nextToken": Value::Null }))
}

// --- agent knowledge-base associations ---------------------------------------

fn associate_agent_knowledge_base(
    req: &Request,
    body: &Value,
    st: &mut BedrockAgentStore,
) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let kb_id = vstr(body, "knowledgeBaseId").unwrap_or_default();
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    let record = wire_row(
        &["description"],
        body,
        &[
            ("agentId", json!(agent_id)),
            ("agentVersion", json!(version)),
            ("knowledgeBaseId", json!(kb_id)),
            (
                "knowledgeBaseState",
                json!(vstr(body, "knowledgeBaseState").unwrap_or_else(|| "ENABLED".to_string())),
            ),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    st.agent_knowledge_bases
        .insert(format!("{agent_id}|{version}|{kb_id}"), record.clone());
    ok(json!({ "agentKnowledgeBase": record }))
}

fn get_agent_knowledge_base(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version, kb_id) = agent_version_kb_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    match st
        .agent_knowledge_bases
        .get(&format!("{agent_id}|{version}|{kb_id}"))
    {
        Some(assoc) => ok(json!({ "agentKnowledgeBase": assoc })),
        None => not_found(&format!("Agent knowledge base {kb_id} not found")),
    }
}

/// The (agentId, agentVersion, knowledgeBaseId) triple for
/// /agents/{a}/agentversions/{v}/knowledgebases/{kb}/ URIs.
fn agent_version_kb_ids(req: &Request) -> (String, String, String) {
    let kb_id = seg_from_end(req.path.as_str(), 1);
    let version = seg_from_end(req.path.as_str(), 3);
    let agent_id = seg_from_end(req.path.as_str(), 5);
    (agent_id, version, kb_id)
}

fn update_agent_knowledge_base(
    req: &Request,
    body: &Value,
    st: &mut BedrockAgentStore,
) -> Response {
    let (agent_id, version, kb_id) = agent_version_kb_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{kb_id}");
    let Some(assoc) = st.agent_knowledge_bases.get_mut(&key) else {
        return not_found(&format!("Agent knowledge base {kb_id} not found"));
    };
    if let Some(v) = vstr(body, "description") {
        assoc["description"] = json!(v);
    }
    if let Some(v) = vstr(body, "knowledgeBaseState") {
        assoc["knowledgeBaseState"] = json!(v);
    }
    assoc["updatedAt"] = json!(unix_time());
    ok(json!({ "agentKnowledgeBase": assoc.clone() }))
}

fn disassociate_agent_knowledge_base(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (agent_id, version, kb_id) = agent_version_kb_ids(req);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let key = format!("{agent_id}|{version}|{kb_id}");
    match st.agent_knowledge_bases.remove(&key) {
        Some(_) => Response::json(200, Vec::new()),
        None => not_found(&format!("Agent knowledge base {kb_id} not found")),
    }
}

/// POST /agents/{agentId}/agentversions/{v}/knowledgebases/ — list.
fn list_agent_knowledge_bases(req: &Request, st: &BedrockAgentStore) -> Response {
    let (agent_id, version) = agent_version_ids(req, 2);
    if let Err(resp) = require_agent(st, &agent_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .agent_knowledge_bases
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{agent_id}|{version}|")))
        .map(|(_, assoc)| {
            json!({
                "knowledgeBaseId": assoc["knowledgeBaseId"],
                "description": assoc.get("description").cloned().unwrap_or(Value::Null),
                "knowledgeBaseState": assoc["knowledgeBaseState"],
                "updatedAt": assoc["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "agentKnowledgeBaseSummaries": rows, "nextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// knowledge-base side modeled families
// ---------------------------------------------------------------------------

/// PUT /knowledgebases/{knowledgeBaseId} — UpdateKnowledgeBase.
fn update_knowledge_base(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let kb_id = seg_from_end(req.path.as_str(), 1);
    let Some(kb) = st.knowledge_bases.get_mut(&kb_id) else {
        return not_found(&format!("Knowledge base {kb_id} not found"));
    };
    if let Some(v) = vstr(body, "name") {
        kb.name = v;
    }
    if let Some(v) = vstr(body, "description") {
        kb.description = Some(v);
    }
    if let Some(v) = vstr(body, "roleArn") {
        kb.role_arn = Some(v);
    }
    if let Some(v) = vnon_null(body, "knowledgeBaseConfiguration") {
        kb.knowledge_base_configuration = v;
    }
    if let Some(v) = vnon_null(body, "storageConfiguration") {
        kb.storage_configuration = v;
    }
    kb.updated_at = unix_time();
    ok(json!({ "knowledgeBase": kb.to_dict() }))
}

// --- data sources --------------------------------------------------------------

/// POST /knowledgebases/{kbId}/datasources/ entries: [.. .. datasources]
fn kb_ds_ids(req: &Request, ds_from_tail: usize) -> (String, String) {
    let ds = seg_from_end(req.path.as_str(), ds_from_tail);
    let kb = seg_from_end(req.path.as_str(), ds_from_tail + 2);
    (kb, ds)
}

fn create_data_source(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let kb_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    let ds_name = vstr(body, "name").unwrap_or_default();
    let ds_id = format!("{}{}", ds_name, short_id());
    let arn = bedrock_arn(
        "data-source",
        &req.region,
        req.account,
        &format!("{kb_id}/{ds_id}"),
    );
    let record = wire_row(
        &[
            "description",
            "dataSourceConfiguration",
            "serverSideEncryptionConfiguration",
            "vectorIngestionConfiguration",
            "dataDeletionPolicy",
        ],
        body,
        &[
            ("knowledgeBaseId", json!(kb_id)),
            ("dataSourceId", json!(ds_id)),
            ("name", json!(ds_name)),
            ("dataSourceArn", json!(arn)),
            ("status", json!("AVAILABLE")),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    st.data_sources
        .insert(format!("{kb_id}|{ds_id}"), record.clone());
    ok(json!({ "dataSource": record }))
}

fn get_data_source(req: &Request, st: &BedrockAgentStore) -> Response {
    // GET/DELETE/PUT /knowledgebases/{kbId}/datasources/{dsId} (no slash).
    let (kb_id, ds_id) = kb_ds_ids(req, 1);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    match st.data_sources.get(&format!("{kb_id}|{ds_id}")) {
        Some(ds) => ok(json!({ "dataSource": ds })),
        None => not_found(&format!("Data source {ds_id} not found")),
    }
}

fn update_data_source(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 1);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    let key = format!("{kb_id}|{ds_id}");
    let Some(ds) = st.data_sources.get_mut(&key) else {
        return not_found(&format!("Data source {ds_id} not found"));
    };
    if let Some(v) = vstr(body, "name") {
        ds["name"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        ds["description"] = json!(v);
    }
    if let Some(v) = body.get("dataSourceConfiguration") {
        ds["dataSourceConfiguration"] = v.clone();
    }
    if let Some(v) = vstr(body, "dataDeletionPolicy") {
        ds["dataDeletionPolicy"] = json!(v);
    }
    if let Some(v) = body.get("serverSideEncryptionConfiguration") {
        ds["serverSideEncryptionConfiguration"] = v.clone();
    }
    if let Some(v) = body.get("vectorIngestionConfiguration") {
        ds["vectorIngestionConfiguration"] = v.clone();
    }
    ds["updatedAt"] = json!(unix_time());
    ok(json!({ "dataSource": ds.clone() }))
}

fn delete_data_source(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 1);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    let key = format!("{kb_id}|{ds_id}");
    match st.data_sources.remove(&key) {
        Some(_) => ok(json!({
            "knowledgeBaseId": kb_id,
            "dataSourceId": ds_id,
            "status": "DELETING",
        })),
        None => not_found(&format!("Data source {ds_id} not found")),
    }
}

/// POST /knowledgebases/{kbId}/datasources/ — listing.
fn list_data_sources(req: &Request, st: &BedrockAgentStore) -> Response {
    let kb_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .data_sources
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{kb_id}|")))
        .map(|(_, ds)| {
            json!({
                "knowledgeBaseId": ds["knowledgeBaseId"],
                "dataSourceId": ds["dataSourceId"],
                "name": ds["name"],
                "status": ds["status"],
                "description": ds.get("description").cloned().unwrap_or(Value::Null),
                "updatedAt": ds["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "dataSourceSummaries": rows, "nextToken": Value::Null }))
}

// --- documents ----------------------------------------------------------------

/// The custom identifier string of a DocumentIdentifier body member.
fn document_identifier_key(identifier: &Value) -> String {
    // custom: {customDocumentIdentifier: {custom: <string>}}
    let custom = identifier
        .get("custom")
        .and_then(|c| c.get("customDocumentIdentifier"))
        .and_then(|i| i.get("custom"))
        .and_then(Value::as_str);
    match custom {
        Some(id) => id.to_string(),
        None => {
            // s3Location: {uri: ...}
            identifier
                .get("s3Location")
                .and_then(|s| s.get("uri"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string()
        }
    }
}

fn document_detail(
    kb_id: &str,
    ds_id: &str,
    identifier_key: &str,
    identifier: &Value,
    status: &str,
) -> Value {
    json!({
        "knowledgeBaseId": kb_id,
        "dataSourceId": ds_id,
        "identifier": identifier,
        "status": status,
        "updatedAt": unix_time(),
        "identifierKey": identifier_key,
    })
}

fn ingest_documents(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let mut details: Vec<Value> = Vec::new();
    if let Some(docs) = body.get("documents").and_then(Value::as_array) {
        for doc in docs {
            let identifier = doc.get("identifier").cloned().unwrap_or(json!({}));
            let key = document_identifier_key(&identifier);
            let detail = document_detail(&kb_id, &ds_id, &key, &identifier, "INDEXED");
            st.documents
                .insert(format!("{kb_id}|{ds_id}|{key}"), detail.clone());
            details.push(json!({
                "knowledgeBaseId": kb_id,
                "dataSourceId": ds_id,
                "identifier": identifier,
                "status": "INDEXED",
                "updatedAt": unix_time(),
            }));
        }
    }
    ok(json!({ "documentDetails": details }))
}

fn kb_documents_response(
    st: &mut BedrockAgentStore,
    kb_id: &str,
    ds_id: &str,
    identifiers: &[Value],
    delete: bool,
) -> Response {
    let mut details: Vec<Value> = Vec::new();
    for identifier in identifiers {
        let key = document_identifier_key(identifier);
        let full = format!("{kb_id}|{ds_id}|{key}");
        let known = st.documents.contains_key(&full);
        if delete && known {
            st.documents.remove(&full);
        }
        let status = if known { "INDEXED" } else { "NOT_FOUND" };
        details.push(json!({
            "knowledgeBaseId": kb_id,
            "dataSourceId": ds_id,
            "identifier": identifier,
            "status": status,
            "updatedAt": unix_time(),
        }));
    }
    ok(json!({ "documentDetails": details }))
}

fn get_kb_documents(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 3);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let identifiers = body
        .get("documentIdentifiers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    kb_documents_response(st, &kb_id, &ds_id, &identifiers, false)
}

fn delete_kb_documents(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 3);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let identifiers = body
        .get("documentIdentifiers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    kb_documents_response(st, &kb_id, &ds_id, &identifiers, true)
}

/// POST /knowledgebases/{kbId}/datasources/{dsId}/documents (list).
fn list_kb_documents(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let details: Vec<Value> = st
        .documents
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{kb_id}|{ds_id}|")))
        .map(|(_, d)| {
            json!({
                "knowledgeBaseId": d["knowledgeBaseId"],
                "dataSourceId": d["dataSourceId"],
                "identifier": d["identifier"],
                "identifierKey": d["identifierKey"],
                "status": d["status"],
                "updatedAt": d["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "documentDetails": details, "nextToken": Value::Null }))
}

// --- ingestion jobs -----------------------------------------------------------

fn start_ingestion_job(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let job_id = format!("ingestion-job-{}", short_id());
    let record = wire_row(
        &["description"],
        body,
        &[
            ("knowledgeBaseId", json!(kb_id)),
            ("dataSourceId", json!(ds_id)),
            ("ingestionJobId", json!(job_id)),
            ("status", json!("COMPLETE")),
            ("startedAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
            (
                "statistics",
                json!({
                    "numberOfNewDocumentsIndexed": 0,
                    "numberOfModifiedDocumentsIndexed": 0,
                    "numberOfDocumentsDeleted": 0,
                    "numberOfDocumentsFailed": 0,
                    "numberOfDocumentsScanned": 0,
                }),
            ),
        ],
    );
    st.ingestion_jobs
        .insert(format!("{kb_id}|{ds_id}|{job_id}"), record.clone());
    ok(json!({ "ingestionJob": record }))
}

fn stop_ingestion_job(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let job_id = seg_from_end(req.path.as_str(), 2);
    let (kb_id, ds_id) = kb_ds_ids(req, 4);
    if let Err(resp) = require_ingestion_job(st, &kb_id, &ds_id, &job_id) {
        return resp;
    }
    let key = format!("{kb_id}|{ds_id}|{job_id}");
    let Some(job) = st.ingestion_jobs.get_mut(&key) else {
        return not_found(&format!("Ingestion job {job_id} not found"));
    };
    job["status"] = json!("STOPPED");
    job["updatedAt"] = json!(unix_time());
    ok(json!({ "ingestionJob": job.clone() }))
}

fn get_ingestion_job(req: &Request, st: &BedrockAgentStore) -> Response {
    let job_id = seg_from_end(req.path.as_str(), 1);
    let (kb_id, ds_id) = kb_ds_ids(req, 3);
    if let Err(resp) = require_ingestion_job(st, &kb_id, &ds_id, &job_id) {
        return resp;
    }
    match st.ingestion_jobs.get(&format!("{kb_id}|{ds_id}|{job_id}")) {
        Some(job) => ok(json!({ "ingestionJob": job })),
        None => not_found(&format!("Ingestion job {job_id} not found")),
    }
}

fn list_ingestion_jobs(req: &Request, body: &Value, st: &BedrockAgentStore) -> Response {
    let (kb_id, ds_id) = kb_ds_ids(req, 2);
    if let Err(resp) = require_kb(st, &kb_id) {
        return resp;
    }
    if let Err(resp) = require_data_source(st, &kb_id, &ds_id) {
        return resp;
    }
    let _ = body;
    let rows: Vec<Value> = st
        .ingestion_jobs
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{kb_id}|{ds_id}|")))
        .map(|(_, job)| {
            json!({
                "knowledgeBaseId": job["knowledgeBaseId"],
                "dataSourceId": job["dataSourceId"],
                "ingestionJobId": job["ingestionJobId"],
                "description": job.get("description").cloned().unwrap_or(Value::Null),
                "status": job["status"],
                "startedAt": job["startedAt"],
                "updatedAt": job["updatedAt"],
                "statistics": job.get("statistics").cloned().unwrap_or(json!(null)),
            })
        })
        .collect();
    ok(json!({ "ingestionJobSummaries": rows, "nextToken": Value::Null }))
}

// ---------------------------------------------------------------------------
// flows and prompts (moto has no handler)
// ---------------------------------------------------------------------------

fn create_flow(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let name = vstr(body, "name").unwrap_or_default();
    let flow_id = format!("{}{}", name, short_id());
    let arn = bedrock_arn("flow", &req.region, req.account, &flow_id);
    let record = wire_row(
        &[
            "description",
            "executionRoleArn",
            "customerEncryptionKeyArn",
            "definition",
        ],
        body,
        &[
            ("name", json!(name)),
            ("id", json!(flow_id)),
            ("arn", json!(arn)),
            ("status", json!("NotPrepared")),
            ("version", json!("1")),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    st.flows.insert(flow_id.clone(), record.clone());
    st.flow_versions
        .insert(format!("{flow_id}|1"), record.clone());
    ok(record)
}

fn get_flow(req: &Request, st: &BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 1);
    match st.flows.get(&flow_id) {
        Some(flow) => {
            let mut body = flow.clone();
            body["validations"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Flow {flow_id} not found")),
    }
}

fn update_flow(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 1);
    let Some(flow) = st.flows.get_mut(&flow_id) else {
        return not_found(&format!("Flow {flow_id} not found"));
    };
    if let Some(v) = vstr(body, "name") {
        flow["name"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        flow["description"] = json!(v);
    }
    if let Some(v) = vstr(body, "executionRoleArn") {
        flow["executionRoleArn"] = json!(v);
    }
    if let Some(v) = vstr(body, "customerEncryptionKeyArn") {
        flow["customerEncryptionKeyArn"] = json!(v);
    }
    if let Some(v) = body.get("definition") {
        flow["definition"] = v.clone();
        // A definition change returns the flow to NotPrepared (real-AWS
        // semantics); PrepareFlow flips it back.
        flow["status"] = json!("NotPrepared");
    }
    flow["updatedAt"] = json!(unix_time());
    ok(flow.clone())
}

fn prepare_flow(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 1);
    let Some(flow) = st.flows.get_mut(&flow_id) else {
        return not_found(&format!("Flow {flow_id} not found"));
    };
    flow["status"] = json!("Prepared");
    flow["updatedAt"] = json!(unix_time());
    ok(json!({ "id": flow_id, "status": flow["status"].clone() }))
}

fn delete_flow(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 1);
    if st.flows.remove(&flow_id).is_none() {
        return not_found(&format!("Flow {flow_id} not found"));
    }
    let prefix = format!("{flow_id}|");
    let vers: Vec<String> = st
        .flow_versions
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    for v in vers {
        st.flow_versions.remove(&v);
    }
    let aliases: Vec<String> = st
        .flow_aliases
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    for a in aliases {
        st.flow_aliases.remove(&a);
    }
    ok(json!({ "id": flow_id }))
}

/// GET /flows/ — ListFlows.
fn list_flows(body: &Value, st: &BedrockAgentStore) -> Response {
    let _ = body;
    let rows: Vec<Value> = st
        .flows
        .values()
        .map(|f| {
            json!({
                "name": f["name"],
                "description": f.get("description").cloned().unwrap_or(Value::Null),
                "id": f["id"],
                "arn": f["arn"],
                "status": f["status"],
                "createdAt": f["createdAt"],
                "updatedAt": f["updatedAt"],
                "version": f["version"],
            })
        })
        .collect();
    ok(json!({ "flowSummaries": rows, "nextToken": Value::Null }))
}

fn create_flow_version(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 2);
    let _ = body;
    let Some(flow) = st.flows.get(&flow_id) else {
        return not_found(&format!("Flow {flow_id} not found"));
    };
    // Version numbers are the stored max + 1.
    let next = st
        .flow_versions
        .keys()
        .filter(|k| k.starts_with(&format!("{flow_id}|")))
        .filter_map(|k| k.rsplit('|').next().and_then(|v| v.parse::<i64>().ok()))
        .max()
        .unwrap_or(1)
        + 1;
    let mut record = flow.clone();
    record["version"] = json!(next.to_string());
    record["status"] = json!("Prepared");
    record["createdAt"] = json!(unix_time());
    st.flow_versions
        .insert(format!("{flow_id}|{next}"), record.clone());
    ok(record)
}

fn get_flow_version(req: &Request, st: &BedrockAgentStore) -> Response {
    let version = seg_from_end(req.path.as_str(), 1);
    let flow_id = seg_from_end(req.path.as_str(), 3);
    match st.flow_versions.get(&format!("{flow_id}|{version}")) {
        Some(record) => ok(record.clone()),
        None => not_found(&format!("Flow version {version} not found")),
    }
}

/// GET /flows/{flowIdentifier}/versions — ListFlowVersions.
fn list_flow_versions(req: &Request, st: &BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_flow(st, &flow_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .flow_versions
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{flow_id}|")))
        .map(|(_, f)| {
            json!({
                "id": f["id"],
                "arn": f["arn"],
                "status": f["status"],
                "createdAt": f["createdAt"],
                "version": f["version"],
            })
        })
        .collect();
    ok(json!({ "flowVersionSummaries": rows, "nextToken": Value::Null }))
}

fn delete_flow_version(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let version = seg_from_end(req.path.as_str(), 1);
    let flow_id = seg_from_end(req.path.as_str(), 3);
    let key = format!("{flow_id}|{version}");
    match st.flow_versions.remove(&key) {
        Some(_) => ok(json!({ "id": flow_id, "version": version })),
        None => not_found(&format!("Flow version {version} not found")),
    }
}

// --- flow aliases -------------------------------------------------------------

fn create_flow_alias(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_flow(st, &flow_id) {
        return resp;
    }
    let alias_name = vstr(body, "name").unwrap_or_default();
    let alias_id = format!("{}{}", alias_name, short_id());
    let arn = bedrock_arn(
        format!("flow/{flow_id}/alias").as_str(),
        &req.region,
        req.account,
        &alias_id,
    );
    let record = wire_row(
        &[
            "description",
            "routingConfiguration",
            "concurrencyConfiguration",
        ],
        body,
        &[
            ("name", json!(alias_name)),
            ("flowId", json!(flow_id)),
            ("id", json!(alias_id)),
            ("arn", json!(arn)),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    st.flow_aliases
        .insert(format!("{flow_id}|{alias_id}"), record.clone());
    ok(record)
}

fn get_flow_alias(req: &Request, st: &BedrockAgentStore) -> Response {
    let alias_id = seg_from_end(req.path.as_str(), 1);
    let flow_id = seg_from_end(req.path.as_str(), 3);
    match st.flow_aliases.get(&format!("{flow_id}|{alias_id}")) {
        Some(alias) => ok(alias.clone()),
        None => not_found(&format!("Flow alias {alias_id} not found")),
    }
}

fn update_flow_alias(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let alias_id = seg_from_end(req.path.as_str(), 1);
    let flow_id = seg_from_end(req.path.as_str(), 3);
    let key = format!("{flow_id}|{alias_id}");
    let Some(alias) = st.flow_aliases.get_mut(&key) else {
        return not_found(&format!("Flow alias {alias_id} not found"));
    };
    if let Some(v) = vstr(body, "name") {
        alias["name"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        alias["description"] = json!(v);
    }
    if let Some(v) = body.get("routingConfiguration") {
        alias["routingConfiguration"] = v.clone();
    }
    if let Some(v) = body.get("concurrencyConfiguration") {
        alias["concurrencyConfiguration"] = v.clone();
    }
    alias["updatedAt"] = json!(unix_time());
    ok(alias.clone())
}

fn delete_flow_alias(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let alias_id = seg_from_end(req.path.as_str(), 1);
    let flow_id = seg_from_end(req.path.as_str(), 3);
    let key = format!("{flow_id}|{alias_id}");
    match st.flow_aliases.remove(&key) {
        Some(_) => ok(json!({ "flowId": flow_id, "id": alias_id })),
        None => not_found(&format!("Flow alias {alias_id} not found")),
    }
}

/// GET /flows/{flowIdentifier}/aliases — ListFlowAliases.
fn list_flow_aliases(req: &Request, st: &BedrockAgentStore) -> Response {
    let flow_id = seg_from_end(req.path.as_str(), 2);
    if let Err(resp) = require_flow(st, &flow_id) {
        return resp;
    }
    let rows: Vec<Value> = st
        .flow_aliases
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{flow_id}|")))
        .map(|(_, alias)| {
            json!({
                "name": alias["name"],
                "description": alias.get("description").cloned().unwrap_or(Value::Null),
                "routingConfiguration": alias["routingConfiguration"],
                "concurrencyConfiguration": alias.get("concurrencyConfiguration").cloned().unwrap_or(json!(null)),
                "flowId": alias["flowId"],
                "id": alias["id"],
                "arn": alias["arn"],
                "createdAt": alias["createdAt"],
                "updatedAt": alias["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "flowAliasSummaries": rows, "nextToken": Value::Null }))
}

/// POST /flows/validate-definition — the modeled validator accepts any
/// shape-conformant definition (no graph analysis), answering no errors.
fn validate_flow_definition(body: &Value) -> Response {
    let _ = body;
    ok(json!({ "validations": [] }))
}

// --- prompts ------------------------------------------------------------------

fn create_prompt(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let name = vstr(body, "name").unwrap_or_default();
    let prompt_id = format!("{}{}", name, short_id());
    let arn = bedrock_arn("prompt", &req.region, req.account, &prompt_id);
    let record = wire_row(
        &[
            "description",
            "customerEncryptionKeyArn",
            "defaultVariant",
            "variants",
        ],
        body,
        &[
            ("name", json!(name)),
            ("id", json!(prompt_id)),
            ("arn", json!(arn)),
            ("version", json!("1")),
            ("createdAt", json!(unix_time())),
            ("updatedAt", json!(unix_time())),
        ],
    );
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if !tags.is_empty() {
            for (key, value) in tags {
                tag_one(st, &arn, key, value.clone());
            }
        }
    }
    st.prompts.insert(prompt_id.clone(), record.clone());
    st.prompt_versions
        .insert(format!("{prompt_id}|1"), record.clone());
    ok(record)
}

fn get_prompt(req: &Request, st: &BedrockAgentStore) -> Response {
    let prompt_id = seg_from_end(req.path.as_str(), 1);
    match st.prompts.get(&prompt_id) {
        Some(prompt) => ok(prompt.clone()),
        None => not_found(&format!("Prompt {prompt_id} not found")),
    }
}

fn update_prompt(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let prompt_id = seg_from_end(req.path.as_str(), 1);
    let Some(prompt) = st.prompts.get_mut(&prompt_id) else {
        return not_found(&format!("Prompt {prompt_id} not found"));
    };
    if let Some(v) = vstr(body, "name") {
        prompt["name"] = json!(v);
    }
    if let Some(v) = vstr(body, "description") {
        prompt["description"] = json!(v);
    }
    if let Some(v) = vstr(body, "customerEncryptionKeyArn") {
        prompt["customerEncryptionKeyArn"] = json!(v);
    }
    if let Some(v) = vstr(body, "defaultVariant") {
        prompt["defaultVariant"] = json!(v);
    }
    if let Some(v) = body.get("variants") {
        prompt["variants"] = v.clone();
    }
    prompt["updatedAt"] = json!(unix_time());
    ok(prompt.clone())
}

fn delete_prompt(req: &Request, st: &mut BedrockAgentStore) -> Response {
    let prompt_id = seg_from_end(req.path.as_str(), 1);
    let Some(prompt) = st.prompts.remove(&prompt_id) else {
        return not_found(&format!("Prompt {prompt_id} not found"));
    };
    let prefix = format!("{prompt_id}|");
    let keys: Vec<String> = st
        .prompt_versions
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    for k in keys {
        st.prompt_versions.remove(&k);
    }
    ok(json!({
        "id": prompt_id,
        "version": prompt.get("version").cloned().unwrap_or(json!(null)),
    }))
}

/// GET /prompts/ — ListPrompts.
fn list_prompts(req: &Request, body: &Value, st: &BedrockAgentStore) -> Response {
    let _ = req;
    let _ = body;
    let rows: Vec<Value> = st
        .prompts
        .values()
        .map(|prompt| {
            json!({
                "name": prompt["name"],
                "description": prompt.get("description").cloned().unwrap_or(Value::Null),
                "id": prompt["id"],
                "arn": prompt["arn"],
                "version": prompt["version"],
                "createdAt": prompt["createdAt"],
                "updatedAt": prompt["updatedAt"],
            })
        })
        .collect();
    ok(json!({ "promptSummaries": rows, "nextToken": Value::Null }))
}

/// POST /prompts/{promptIdentifier}/versions — CreatePromptVersion.
fn create_prompt_version(req: &Request, body: &Value, st: &mut BedrockAgentStore) -> Response {
    let prompt_id = seg_from_end(req.path.as_str(), 2);
    let Some(prompt) = st.prompts.get(&prompt_id) else {
        return not_found(&format!("Prompt {prompt_id} not found"));
    };
    let next = st
        .prompt_versions
        .keys()
        .filter(|k| k.starts_with(&format!("{prompt_id}|")))
        .filter_map(|k| k.rsplit('|').next().and_then(|v| v.parse::<i64>().ok()))
        .max()
        .unwrap_or(1)
        + 1;
    let mut record = prompt.clone();
    record["version"] = json!(next.to_string());
    record["createdAt"] = json!(unix_time());
    record["updatedAt"] = json!(unix_time());
    if let Some(v) = vstr(body, "description") {
        record["description"] = json!(v);
    }
    st.prompt_versions
        .insert(format!("{prompt_id}|{next}"), record.clone());
    ok(record)
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
    fn update_agent_and_unmatched_paths() {
        let account = 123456789005;
        // PUT /agents/{agentId}/ routes as UpdateAgent: a modeled handler
        // (UpdateAgent was moto's NotImplementedError before this wave).
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({
                "agentName": "upd-agent",
                "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x",
                "instruction": "old",
                "idleSessionTTLInSeconds": 600,
            }),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();
        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/agents/{agent_id}/"),
            "",
            json!({
                "agentName": "upd-agent",
                "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x",
                "instruction": "new text",
                "idleSessionTTLInSeconds": 300,
            }),
            account,
        )));
        assert_eq!(updated["agent"]["instruction"], "new text");
        assert_eq!(updated["agent"]["idleSessionTTLInSeconds"], 300);

        // A trailing-slash shape that no botocore URI carries answers
        // moto's bare 404 (gateway-level unmatchable path).
        assert_eq!(
            status(dispatch(&reqacct("PUT", "/agents", "", json!({}), account))),
            404
        );
    }

    #[test]
    fn agent_versions_prepare_lifecycle() {
        let account = 123456789006;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({"agentName": "ver-agent", "idleSessionTTLInSeconds": 600,
                   "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x"}),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();

        let prep = body(dispatch(&reqacct(
            "POST",
            &format!("/agents/{agent_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(prep["agentStatus"], "PREPARED");
        assert_eq!(prep["agentVersion"], "1.0");

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("/agents/{agent_id}/agentversions/"),
            "",
            json!({}),
            account,
        )));
        let rows = listed["agentVersionSummaries"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["agentVersion"], "1.0");

        let version = body(dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/agentversions/1.0/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(version["agentVersion"]["version"], "1.0");
        assert_eq!(version["agentVersion"]["agentName"], "ver-agent");

        // Unknown version -> RFNF.
        let missing = dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/agentversions/9.9/"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(missing), 400);

        // Deleting the only version removes the agent.
        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/agents/{agent_id}/agentversions/1.0/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["agentStatus"], "DELETING");
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(gone), 400);
    }

    #[test]
    fn agent_alias_lifecycle() {
        let account = 123456789007;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({"agentName": "alias-agent", "idleSessionTTLInSeconds": 600,
                   "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x"}),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();

        // Alias over an unknown agent -> RFNF.
        let bad = dispatch(&reqacct(
            "PUT",
            "/agents/no-such-agent/agentaliases/",
            "",
            json!({"agentAliasName": "bad-alias"}),
            account,
        ));
        assert_eq!(status(bad), 400);

        let alias = body(dispatch(&reqacct(
            "PUT",
            &format!("/agents/{agent_id}/agentaliases/"),
            "",
            json!({"agentAliasName": "prod", "routingConfiguration": [{"agentVersion": "1.0"}]}),
            account,
        )))["agentAlias"]
            .clone();
        let alias_id = alias["agentAliasId"].as_str().unwrap().to_string();
        assert!(alias_id.starts_with("prod"));
        assert_eq!(alias["agentAliasStatus"], "PREPARED");
        let arn = alias["agentAliasArn"].as_str().unwrap().to_string();
        assert!(arn.contains("agent-alias/"));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/agentaliases/{alias_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["agentAlias"]["agentAliasName"], "prod");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/agents/{agent_id}/agentaliases/{alias_id}/"),
            "",
            json!({"agentAliasName": "prod2", "routingConfiguration": [{"agentVersion": "1.0"}]}),
            account,
        )));
        assert_eq!(updated["agentAlias"]["agentAliasName"], "prod2");

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("/agents/{agent_id}/agentaliases/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["agentAliasSummaries"].as_array().unwrap().len(), 1);

        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/agents/{agent_id}/agentaliases/{alias_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["agentAliasStatus"], "DELETING");
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/agents/{agent_id}/agentaliases/{alias_id}/"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(gone), 400);
    }

    #[test]
    fn action_group_lifecycle() {
        let account = 123456789008;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({"agentName": "ag-agent", "idleSessionTTLInSeconds": 600,
                   "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x"}),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();
        let base = format!("/agents/{agent_id}/agentversions/1.0/actiongroups");

        let group = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/"),
            "",
            json!({
                "actionGroupName": "bookings",
                "description": "reserve",
                "actionGroupExecutor": {"lambda": "arn:aws:lambda:us-east-1:123456789012:function:book"},
                "functionSchema": {"functions": [{"name": "book", "description": "book it"}]},
            }),
            account,
        )))["agentActionGroup"].clone();
        let group_id = group["actionGroupId"].as_str().unwrap().to_string();
        assert_eq!(group["actionGroupState"], "ENABLED");
        assert_eq!(group["agentVersion"], "1.0");

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("{base}/{group_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["agentActionGroup"]["actionGroupName"], "bookings");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/{group_id}/"),
            "",
            json!({
                "actionGroupName": "bookings2",
                "actionGroupState": "DISABLED",
            }),
            account,
        )));
        assert_eq!(updated["agentActionGroup"]["actionGroupName"], "bookings2");
        assert_eq!(updated["agentActionGroup"]["actionGroupState"], "DISABLED");

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("{base}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["actionGroupSummaries"].as_array().unwrap().len(), 1);

        assert_eq!(
            status(dispatch(&reqacct(
                "DELETE",
                &format!("{base}/{group_id}/"),
                "",
                json!({}),
                account,
            ))),
            200
        );
        assert_eq!(
            status(dispatch(&reqacct(
                "GET",
                &format!("{base}/{group_id}/"),
                "",
                json!({}),
                account,
            ))),
            400
        );
    }

    #[test]
    fn collaborator_lifecycle() {
        let account = 123456789009;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({"agentName": "collab-agent", "idleSessionTTLInSeconds": 600,
                   "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x"}),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();
        let base = format!("/agents/{agent_id}/agentversions/1.0/agentcollaborators");

        let assoc = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/"),
            "",
            json!({
                "collaboratorName": "helper",
                "collaborationInstruction": "help me",
                "agentDescriptor": {"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/A/B"},
                "relayConversationHistory": "TO_COLLABORATOR",
            }),
            account,
        )))["agentCollaborator"].clone();
        let collaborator_id = assoc["collaboratorId"].as_str().unwrap().to_string();
        assert_eq!(assoc["relayConversationHistory"], "TO_COLLABORATOR");

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("{base}/{collaborator_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["agentCollaborator"]["collaboratorName"], "helper");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/{collaborator_id}/"),
            "",
            json!({
                "collaboratorName": "helper2",
                "collaborationInstruction": "help more",
                "agentDescriptor": {"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/A/C"},
            }),
            account,
        )));
        assert_eq!(updated["agentCollaborator"]["collaboratorName"], "helper2");

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("{base}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(
            listed["agentCollaboratorSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            status(dispatch(&reqacct(
                "DELETE",
                &format!("{base}/{collaborator_id}/"),
                "",
                json!({}),
                account,
            ))),
            200
        );
        assert_eq!(
            status(dispatch(&reqacct(
                "GET",
                &format!("{base}/{collaborator_id}/"),
                "",
                json!({}),
                account,
            ))),
            400
        );
    }

    #[test]
    fn agent_knowledge_base_assoc_lifecycle() {
        let account = 123456789010;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/agents/",
            "",
            json!({"agentName": "kb-assoc-agent", "idleSessionTTLInSeconds": 600,
                   "agentResourceRoleArn": "arn:aws:iam::123456789012:role/x"}),
            account,
        )));
        let agent_id = created["agent"]["agentId"].as_str().unwrap().to_string();
        let kb = body(dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "assoc-kb",
                "roleArn": "arn:aws:iam::123456789012:role/x",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "PINECONE"},
            }),
            account,
        )));
        let kb_id = kb["knowledgeBase"]["knowledgeBaseId"]
            .as_str()
            .unwrap()
            .to_string();

        // Unknown KB -> RFNF.
        let bad = dispatch(&reqacct(
            "PUT",
            &format!("/agents/{agent_id}/agentversions/1.0/knowledgebases/"),
            "",
            json!({"knowledgeBaseId": "missing-kb", "description": "d"}),
            account,
        ));
        assert_eq!(status(bad), 400);

        let base = format!("/agents/{agent_id}/agentversions/1.0/knowledgebases");
        let assoc = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/"),
            "",
            json!({"knowledgeBaseId": kb_id, "description": "docs"}),
            account,
        )))["agentKnowledgeBase"]
            .clone();
        assert_eq!(assoc["knowledgeBaseState"], "ENABLED");
        assert_eq!(assoc["knowledgeBaseId"], kb_id);

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("{base}/{kb_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["agentKnowledgeBase"]["description"], "docs");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("{base}/{kb_id}/"),
            "",
            json!({"knowledgeBaseId": kb_id, "knowledgeBaseState": "DISABLED"}),
            account,
        )));
        assert_eq!(
            updated["agentKnowledgeBase"]["knowledgeBaseState"],
            "DISABLED"
        );

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("{base}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(
            listed["agentKnowledgeBaseSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            status(dispatch(&reqacct(
                "DELETE",
                &format!("{base}/{kb_id}/"),
                "",
                json!({}),
                account,
            ))),
            200
        );
        assert_eq!(
            status(dispatch(&reqacct(
                "GET",
                &format!("{base}/{kb_id}/"),
                "",
                json!({}),
                account,
            ))),
            400
        );
    }

    #[test]
    fn update_knowledge_base_lifecycle() {
        let account = 123456789011;
        let created = body(dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "upd-kb",
                "roleArn": "arn:aws:iam::123456789012:role/x",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "RDS"},
            }),
            account,
        )));
        let kb_id = created["knowledgeBase"]["knowledgeBaseId"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/knowledgebases/{kb_id}"),
            "",
            json!({
                "name": "upd-kb2",
                "roleArn": "arn:aws:iam::123456789012:role/y",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "description": "new text",
            }),
            account,
        )));
        assert_eq!(updated["knowledgeBase"]["name"], "upd-kb2");
        assert_eq!(updated["knowledgeBase"]["description"], "new text");

        let missing = dispatch(&reqacct(
            "PUT",
            "/knowledgebases/missing-kb-id",
            "",
            json!({"name": "n", "roleArn": "r", "knowledgeBaseConfiguration": {"type": "VECTOR"}}),
            account,
        ));
        assert_eq!(status(missing), 400);
    }

    #[test]
    fn data_source_document_job_lifecycle() {
        let account = 123456789012;
        let kb = body(dispatch(&reqacct(
            "PUT",
            "/knowledgebases/",
            "",
            json!({
                "name": "ds-kb",
                "roleArn": "arn:aws:iam::123456789012:role/x",
                "knowledgeBaseConfiguration": {"type": "VECTOR"},
                "storageConfiguration": {"type": "OPENSEARCH_SERVERLESS"},
            }),
            account,
        )));
        let kb_id = kb["knowledgeBase"]["knowledgeBaseId"]
            .as_str()
            .unwrap()
            .to_string();

        // Data source round trip.
        let ds = body(dispatch(&reqacct(
            "PUT",
            &format!("/knowledgebases/{kb_id}/datasources/"),
            "",
            json!({
                "name": "web-ds",
                "dataSourceConfiguration": {"type": "S3", "s3Configuration": {
                    "bucketArn": "arn:aws:s3:::docs", "inclusionPrefixes": ["a/"]
                }},
            }),
            account,
        )))["dataSource"]
            .clone();
        let ds_id = ds["dataSourceId"].as_str().unwrap().to_string();
        assert_eq!(ds["status"], "AVAILABLE");
        assert!(ds_id.starts_with("web-ds"));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["dataSource"]["name"], "web-ds");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}"),
            "",
            json!({
                "name": "web-ds2",
                "dataSourceConfiguration": {"type": "S3", "s3Configuration": {
                    "bucketArn": "arn:aws:s3:::docs2"
                }},
            }),
            account,
        )));
        assert_eq!(updated["dataSource"]["name"], "web-ds2");

        let listed = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["dataSourceSummaries"].as_array().unwrap().len(), 1);

        // Documents: ingest -> get -> list -> delete.
        let doc_identifier = json!({"custom": {"customDocumentIdentifier": {"custom": "doc-one"}}});
        let ingested = body(dispatch(&reqacct(
            "PUT",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents"),
            "",
            json!({"documents": [{"identifier": doc_identifier}]}),
            account,
        )));
        assert_eq!(
            ingested["documentDetails"].as_array().unwrap()[0]["status"],
            "INDEXED"
        );

        let got_docs = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents/getDocuments"),
            "",
            json!({"documentIdentifiers": [doc_identifier]}),
            account,
        )));
        assert_eq!(
            got_docs["documentDetails"].as_array().unwrap()[0]["status"],
            "INDEXED"
        );

        let listed_docs = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed_docs["documentDetails"].as_array().unwrap().len(), 1);

        // Unknown identifier -> NOT_FOUND in the modeled detail set.
        let unknown_docs = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents/getDocuments"),
            "",
            json!({"documentIdentifiers": [{"custom": {"customDocumentIdentifier": {"custom": "doc-none"}}}]}),
            account,
        )));
        assert_eq!(
            unknown_docs["documentDetails"].as_array().unwrap()[0]["status"],
            "NOT_FOUND"
        );

        let deleted_docs = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents/deleteDocuments"),
            "",
            json!({"documentIdentifiers": [doc_identifier]}),
            account,
        )));
        assert_eq!(
            deleted_docs["documentDetails"].as_array().unwrap()[0]["status"],
            "INDEXED"
        );
        let after_delete = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/documents"),
            "",
            json!({}),
            account,
        )));
        assert!(after_delete["documentDetails"]
            .as_array()
            .unwrap()
            .is_empty());

        // Ingestion jobs: start -> get -> stop -> list.
        let job = body(dispatch(&reqacct(
            "PUT",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/ingestionjobs/"),
            "",
            json!({"description": "initial load"}),
            account,
        )))["ingestionJob"]
            .clone();
        let job_id = job["ingestionJobId"].as_str().unwrap().to_string();
        assert_eq!(job["status"], "COMPLETE");
        assert!(job["startedAt"].as_f64().is_some());

        let got_job = body(dispatch(&reqacct(
            "GET",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/ingestionjobs/{job_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got_job["ingestionJob"]["status"], "COMPLETE");

        let stopped = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/ingestionjobs/{job_id}/stop"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(stopped["ingestionJob"]["status"], "STOPPED");

        let listed_jobs = body(dispatch(&reqacct(
            "POST",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}/ingestionjobs/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(
            listed_jobs["ingestionJobSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Delete the data source last.
        let del = body(dispatch(&reqacct(
            "DELETE",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(del["status"], "DELETING");
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/knowledgebases/{kb_id}/datasources/{ds_id}"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(gone), 400);
    }

    #[test]
    fn flow_lifecycle() {
        let account = 123456789013;
        let flow = body(dispatch(&reqacct(
            "POST",
            "/flows/",
            "",
            json!({
                "name": "support-flow",
                "executionRoleArn": "arn:aws:iam::123456789012:role/x",
                "definition": {"nodes": [], "connections": []},
            }),
            account,
        )));
        assert_eq!(flow["status"], "NotPrepared");
        assert_eq!(flow["version"], "1");
        let flow_id = flow["id"].as_str().unwrap().to_string();
        assert!(flow_id.starts_with("support-flow"));
        assert!(flow["arn"].as_str().unwrap().contains("flow/"));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["name"], "support-flow");
        assert!(got["validations"].is_array());

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/flows/{flow_id}/"),
            "",
            json!({
                "name": "support-flow",
                "executionRoleArn": "arn:aws:iam::123456789012:role/y",
                "definition": {"nodes": [], "connections": []},
            }),
            account,
        )));
        assert_eq!(
            updated["executionRoleArn"],
            "arn:aws:iam::123456789012:role/y"
        );

        let prepped = body(dispatch(&reqacct(
            "POST",
            &format!("/flows/{flow_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(prepped["status"], "Prepared");

        // Version 2 (the stored max + 1), copied from the flow.
        let version = body(dispatch(&reqacct(
            "POST",
            &format!("/flows/{flow_id}/versions"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(version["version"], "2");

        let got_version = body(dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/versions/2/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got_version["version"], "2");

        let versions = body(dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/versions"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(
            versions["flowVersionSummaries"].as_array().unwrap().len(),
            2
        );

        // Alias round trip.
        let alias = body(dispatch(&reqacct(
            "POST",
            &format!("/flows/{flow_id}/aliases"),
            "",
            json!({
                "name": "live",
                "routingConfiguration": [{"flowVersion": "2"}],
            }),
            account,
        )));
        let alias_id = alias["id"].as_str().unwrap().to_string();
        assert!(alias_id.starts_with("live"));
        let got_alias = body(dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/aliases/{alias_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got_alias["name"], "live");
        let alias_updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/flows/{flow_id}/aliases/{alias_id}"),
            "",
            json!({
                "name": "live2",
                "routingConfiguration": [{"flowVersion": "2"}],
            }),
            account,
        )));
        assert_eq!(alias_updated["name"], "live2");
        let aliases = body(dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/aliases"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(aliases["flowAliasSummaries"].as_array().unwrap().len(), 1);

        // Validate (modeled never contradicts a shape-conformant body).
        let validations = body(dispatch(&reqacct(
            "POST",
            "/flows/validate-definition",
            "",
            json!({"definition": {"nodes": [], "connections": []}}),
            account,
        )));
        assert!(validations["validations"].as_array().unwrap().is_empty());

        // List before delete.
        let flows = body(dispatch(&reqacct("GET", "/flows/", "", json!({}), account)));
        assert!(!flows["flowSummaries"].as_array().unwrap().is_empty());

        // Delete version then flow (aliases go with it).
        assert_eq!(
            status(dispatch(&reqacct(
                "DELETE",
                &format!("/flows/{flow_id}/versions/2/"),
                "",
                json!({}),
                account,
            ))),
            200
        );
        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/flows/{flow_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["id"], flow_id);
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/flows/{flow_id}/"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(gone), 400);
    }

    #[test]
    fn prompt_lifecycle() {
        let account = 123456789014;
        let prompt = body(dispatch(&reqacct(
            "POST",
            "/prompts/",
            "",
            json!({
                "name": "summarize",
                "defaultVariant": "v1",
                "variants": [{"name": "v1", "templateType": "TEXT", "templateConfiguration": {
                    "text": {"text": "Summarize {{input}}"}
                }}],
            }),
            account,
        )));
        assert_eq!(prompt["version"], "1");
        let prompt_id = prompt["id"].as_str().unwrap().to_string();
        assert!(prompt_id.starts_with("summarize"));

        let got = body(dispatch(&reqacct(
            "GET",
            &format!("/prompts/{prompt_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["defaultVariant"], "v1");

        let updated = body(dispatch(&reqacct(
            "PUT",
            &format!("/prompts/{prompt_id}/"),
            "",
            json!({
                "name": "summarize",
                "defaultVariant": "v2",
            }),
            account,
        )));
        assert_eq!(updated["defaultVariant"], "v2");

        let version2 = body(dispatch(&reqacct(
            "POST",
            &format!("/prompts/{prompt_id}/versions"),
            "",
            json!({"description": "snapshot"}),
            account,
        )));
        assert_eq!(version2["version"], "2");

        let listed = body(dispatch(&reqacct(
            "GET",
            "/prompts/",
            "",
            json!({}),
            account,
        )));
        assert!(!listed["promptSummaries"].as_array().unwrap().is_empty());

        let deleted = body(dispatch(&reqacct(
            "DELETE",
            &format!("/prompts/{prompt_id}/"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(deleted["id"], prompt_id);
        let gone = dispatch(&reqacct(
            "GET",
            &format!("/prompts/{prompt_id}/"),
            "",
            json!({}),
            account,
        ));
        assert_eq!(status(gone), 400);
    }
}
