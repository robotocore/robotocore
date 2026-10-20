//! Lex Models V2 (`lexv2models`) — port of the deployed moto fork's
//! `moto/lexv2models` backend (the surface authority; no probe file exists
//! for this service).
//!
//! moto implements 15 operations: bots (create/describe/update/list/delete),
//! bot aliases (same five), resource policies (create/describe/update/delete)
//! and tags (tag/untag/list). Everything else in the botocore surface —
//! locales, versions, intents, slots, imports/exports, analytics, test sets,
//! custom vocabularies — falls through to the gateway's generic `501`,
//! matching moto's `NotImplementedError` for those actions (`call_action`
//! raises when the handler method is absent from the response class).
//!
//! Operation resolution happens here on `method + path` because the gateway
//! has no REST spec table for this service (`operation` arrives empty —
//! lexv2-models is rest-json with no `X-Amz-Target`).
//!
//! Wire fidelity notes (verified against the deployed fork):
//! - responses carry moto's member set, including empty `failureReasons`,
//!   `botAliasHistoryEvents` and `parentBotNetworks` on describe, and the
//!   auto-created `test` alias (`botVersion "1"`) every bot gets;
//! - `list_bot_aliases` is unfiltered across bots in the scope and echoes
//!   the requested `botId` back (moto behavior, preserved);
//! - bot alias ARNs use moto's literal `$` separators:
//!   `arn:aws:lex:{region}:{account}:bot-alias/${botId}/${aliasId}`;
//! - moto crashes (KeyError → HTTP 500) on missing resources; this backend
//!   answers botocore's modeled `ResourceNotFoundException` (400 — the
//!   model's default status for lexv2-models errors) instead. For resource
//!   policies moto raises a bare `Exception` on revision mismatch (500);
//!   this backend answers the modeled `ConflictException` and only enforces
//!   the check when `expectedRevisionId` is present (moto compares even
//!   `None` against the revision id, which makes the optional parameter
//!   mandatory — a crash, not behavior to reproduce).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State (per account+region, mirroring moto's LexModelsV2Backend)
// ---------------------------------------------------------------------------

struct Bot {
    id: String,
    name: String,
    description: Value,
    role_arn: String,
    data_privacy: Value,
    idle_session_ttl: Value,
    bot_type: Value,
    bot_members: Value,
    status: String,
    created: String,
    updated: String,
}

impl Bot {
    fn from_body(body: &Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: str_field(body, "botName").unwrap_or_default().to_string(),
            description: body.get("description").cloned().unwrap_or(Value::Null),
            role_arn: str_field(body, "roleArn").unwrap_or_default().to_string(),
            data_privacy: body.get("dataPrivacy").cloned().unwrap_or(Value::Null),
            idle_session_ttl: body
                .get("idleSessionTTLInSeconds")
                .cloned()
                .unwrap_or(Value::Null),
            bot_type: body.get("botType").cloned().unwrap_or(Value::Null),
            bot_members: body.get("botMembers").cloned().unwrap_or(Value::Null),
            status: "CREATING".to_string(),
            created: crate::proto::now_iso(),
            updated: crate::proto::now_iso(),
        }
    }

    fn arn(&self, account: u64, region: &str) -> String {
        format!("arn:aws:lex:{region}:{account}:bot/{}", self.id)
    }
}

struct BotAlias {
    id: String,
    name: String,
    description: Value,
    version: Value,
    locale_settings: Value,
    log_settings: Value,
    sentiment_settings: Value,
    status: String,
    bot_id: String,
    created: String,
    updated: String,
}

impl BotAlias {
    fn from_body(body: &Value, bot_id: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: str_field(body, "botAliasName")
                .unwrap_or_default()
                .to_string(),
            description: body.get("description").cloned().unwrap_or(Value::Null),
            version: body.get("botVersion").cloned().unwrap_or(Value::Null),
            locale_settings: body
                .get("botAliasLocaleSettings")
                .cloned()
                .unwrap_or(Value::Null),
            log_settings: body
                .get("conversationLogSettings")
                .cloned()
                .unwrap_or(Value::Null),
            sentiment_settings: body
                .get("sentimentAnalysisSettings")
                .cloned()
                .unwrap_or(Value::Null),
            status: "CREATING".to_string(),
            bot_id: bot_id.to_string(),
            created: crate::proto::now_iso(),
            updated: crate::proto::now_iso(),
        }
    }

    /// moto's ARN uses literal `$` separators around both ids (FakeBotAlias:
    /// `bot-alias/${bot_requested}` → `bot-alias/$botId/$aliasId`).
    fn arn(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:lex:{region}:{account}:bot-alias/${}/${}",
            self.bot_id, self.id
        )
    }
}

struct ResourcePolicy {
    policy: String,
    revision_id: String,
}

#[derive(Default)]
struct State {
    bots: BTreeMap<String, Bot>,
    bot_aliases: BTreeMap<String, BotAlias>,
    resource_policies: BTreeMap<String, ResourcePolicy>,
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("lexv2models"));

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

fn ok(body: Value) -> Option<Response> {
    Some(json_resp(200, body))
}

/// rest-json lexv2-models error. botocore resolves the code from `__type`
/// (text after `#`); the lexv2-models model leaves error statuses unset, so
/// botocore defaults them to 400.
fn error_response(status: u16, code: &str, message: &str) -> Option<Response> {
    Some(json_resp(
        status,
        json!({ "__type": format!("com.amazonaws.lexmodelsv2#{code}"), "message": message }),
    ))
}

fn not_found(resource: &str) -> Option<Response> {
    error_response(
        400,
        "ResourceNotFoundException",
        &format!("{resource} not found"),
    )
}

fn conflict(message: &str) -> Option<Response> {
    error_response(400, "ConflictException", message)
}

fn str_field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

/// `expectedRevisionId` rides in the querystring for rest-json; accept a body
/// member too for direct callers.
fn revision_param(req: &Request, body: &Value) -> Option<String> {
    crate::proto::query_map(&req.query_string)
        .get("expectedRevisionId")
        .and_then(|v| v.first().cloned())
        .or_else(|| {
            body.get("expectedRevisionId")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn set_tags(state: &mut State, arn: &str, tags: &Value) {
    match tags {
        Value::Object(map) if !map.is_empty() => {
            let entry = state.tags.entry(arn.to_string()).or_default();
            for (k, v) in map {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    let path = req.path.trim_matches('/');
    let body = crate::proto::parse_json_body(&req.body);

    // Tag and policy ops are prefix-matched first: their requestUri carries a
    // resource ARN which moto captures greedily past the first `/`.
    if let Some(arn) = path.strip_prefix("tags/") {
        if arn.is_empty() {
            return None;
        }
        let parsed_arn = crate::proto::url_decode(arn);
        return match req.method.as_str() {
            "POST" => ok(tag_resource(req, &parsed_arn, &body)),
            "DELETE" => ok(untag_resource(req, &parsed_arn, &body)),
            "GET" => ok(list_tags_for_resource(req, &parsed_arn)),
            _ => None, // moto action could not be matched in the botocore table
        };
    }
    // Anything after `/policy/` before a trailing `/<other>` is the ARN;
    // statement sub-operations are not in moto's handler surface (501).
    if let Some(rest) = path.strip_prefix("policy/") {
        if rest.is_empty() {
            return None;
        }
        let trimmed = rest.strip_suffix('/');
        let trimmed = trimmed.unwrap_or(rest);
        if trimmed.ends_with("/statements") {
            return None;
        }
        let arn = crate::proto::url_decode(trimmed);
        return match req.method.as_str() {
            "POST" => create_resource_policy(req, &arn, &body),
            "GET" => describe_resource_policy(req, &arn),
            "PUT" => update_resource_policy(req, &arn, &body),
            "DELETE" => delete_resource_policy(req, &arn, &body),
            _ => None,
        };
    }

    let segments: Vec<&str> = path.split('/').collect();
    match (req.method.as_str(), segments.as_slice()) {
        // Bots
        ("PUT", ["bots"]) => create_bot(req, &body),
        ("POST", ["bots"]) => list_bots(req),
        ("GET", ["bots", bot_id]) => describe_bot(req, bot_id),
        ("PUT", ["bots", bot_id]) => update_bot(req, bot_id, &body),
        ("DELETE", ["bots", bot_id]) => delete_bot(req, bot_id),
        // Bot aliases — for rest-json botocore supplies botId via the URI,
        // not the body.
        ("PUT", ["bots", bot_id, "botaliases"]) => create_bot_alias(req, bot_id, &body),
        ("POST", ["bots", bot_id, "botaliases"]) => list_bot_aliases(req, bot_id),
        ("GET", ["bots", _bot_id, "botaliases", alias_id]) => describe_bot_alias(req, alias_id),
        ("PUT", ["bots", bot_id, "botaliases", alias_id]) => {
            update_bot_alias(req, alias_id, bot_id, &body)
        }
        ("DELETE", ["bots", _bot_id, "botaliases", alias_id]) => delete_bot_alias(req, alias_id),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Bots
// ---------------------------------------------------------------------------

fn create_bot(req: &Request, body: &Value) -> Option<Response> {
    let bot = Bot::from_body(body);
    let arn = bot.arn(req.account, &req.region);
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let bot_tags = body.get("botTags").cloned().unwrap_or(json!({}));
    set_tags(state, &arn, &bot_tags);

    // moto creates the `test` alias on every bot.
    let mut test_body = serde_json::Map::new();
    test_body.insert(
        "botAliasName".to_string(),
        Value::String("test".to_string()),
    );
    test_body.insert("description".to_string(), Value::String("test".to_string()));
    test_body.insert("botVersion".to_string(), Value::String("1".to_string()));
    let test_alias = BotAlias::from_body(&Value::Object(test_body), &bot.id);
    state.bot_aliases.insert(test_alias.id.clone(), test_alias);

    let bot_tags = tags_json(state, &arn);
    let response = json!({
        "botId": bot.id,
        "botName": bot.name,
        "description": bot.description,
        "roleArn": bot.role_arn,
        "dataPrivacy": bot.data_privacy,
        "idleSessionTTLInSeconds": bot.idle_session_ttl,
        "botStatus": bot.status,
        "creationDateTime": bot.created,
        "botTags": bot_tags,
        "testBotAliasTags": body.get("testBotAliasTags").cloned().unwrap_or(Value::Null),
        "botType": bot.bot_type,
        "botMembers": bot.bot_members,
    });
    state.bots.insert(bot.id.clone(), bot);
    drop(guard);
    ok(response)
}

fn describe_bot(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let bot = match state.bots.get(bot_id) {
        Some(b) => b,
        None => return not_found(&format!("Bot {bot_id}")),
    };
    ok(json!({
        "botId": bot.id,
        "botName": bot.name,
        "description": bot.description,
        "roleArn": bot.role_arn,
        "dataPrivacy": bot.data_privacy,
        "idleSessionTTLInSeconds": bot.idle_session_ttl,
        "botStatus": bot.status,
        "creationDateTime": bot.created,
        "lastUpdatedDateTime": bot.updated,
        "botType": bot.bot_type,
        "botMembers": bot.bot_members,
        "failureReasons": [],
    }))
}

fn update_bot(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let bot = match state.bots.get_mut(bot_id) {
        Some(b) => b,
        None => return not_found(&format!("Bot {bot_id}")),
    };
    if let Some(name) = str_field(body, "botName") {
        bot.name = name.to_string();
    }
    if let Some(description) = body.get("description") {
        bot.description = description.clone();
    }
    if let Some(role_arn) = str_field(body, "roleArn") {
        bot.role_arn = role_arn.to_string();
    }
    if let Some(data_privacy) = body.get("dataPrivacy") {
        bot.data_privacy = data_privacy.clone();
    }
    if let Some(ttl) = body.get("idleSessionTTLInSeconds") {
        bot.idle_session_ttl = ttl.clone();
    }
    if let Some(bot_type) = body.get("botType") {
        bot.bot_type = bot_type.clone();
    }
    if let Some(bot_members) = body.get("botMembers") {
        bot.bot_members = bot_members.clone();
    }
    bot.updated = crate::proto::now_iso();
    ok(json!({
        "botId": bot.id,
        "botName": bot.name,
        "description": bot.description,
        "roleArn": bot.role_arn,
        "dataPrivacy": bot.data_privacy,
        "idleSessionTTLInSeconds": bot.idle_session_ttl,
        "botStatus": bot.status,
        "creationDateTime": bot.created,
        "lastUpdatedDateTime": bot.updated,
        "botType": bot.bot_type,
        "botMembers": bot.bot_members,
    }))
}

fn list_bots(req: &Request) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let summaries: Vec<Value> = state
        .bots
        .values()
        .map(|bot| {
            json!({
                "botId": bot.id,
                "botName": bot.name,
                "description": bot.description,
                "botStatus": bot.status,
                "latestBotVersion": 1,
                "lastUpdatedDateTime": bot.updated,
                "botType": bot.bot_type,
            })
        })
        .collect();
    ok(json!({
        "botSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn delete_bot(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.bots.remove(bot_id) {
        Some(bot) => ok(json!({ "botId": bot.id, "botStatus": bot.status })),
        None => not_found(&format!("Bot {bot_id}")),
    }
}

// ---------------------------------------------------------------------------
// Bot aliases
// ---------------------------------------------------------------------------

fn create_bot_alias(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    // moto's _get_param precedence is querystring, then body, then the uri
    // path param — so a body member wins and real botocore callers (which
    // pass botId only in the uri) fall through to the uri value.
    let bot_id = str_field(body, "botId").unwrap_or(bot_id).to_string();
    let alias = BotAlias::from_body(body, &bot_id);
    let arn = alias.arn(req.account, &req.region);
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    set_tags(state, &arn, &body.get("tags").cloned().unwrap_or(json!({})));
    let tags = tags_json(state, &arn);
    let response = json!({
        "botAliasId": alias.id,
        "botAliasName": alias.name,
        "description": alias.description,
        "botVersion": alias.version,
        "botAliasLocaleSettings": alias.locale_settings,
        "conversationLogSettings": alias.log_settings,
        "sentimentAnalysisSettings": alias.sentiment_settings,
        "botAliasStatus": alias.status,
        "creationDateTime": alias.created,
        "botId": alias.bot_id,
        "tags": tags,
    });
    state.bot_aliases.insert(alias.id.clone(), alias);
    drop(guard);
    ok(response)
}

fn describe_bot_alias(req: &Request, alias_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.bot_aliases.get(alias_id) {
        Some(alias) => ok(json!({
            "botAliasId": alias.id,
            "botAliasName": alias.name,
            "description": alias.description,
            "botVersion": alias.version,
            "botAliasLocaleSettings": alias.locale_settings,
            "conversationLogSettings": alias.log_settings,
            "sentimentAnalysisSettings": alias.sentiment_settings,
            "botAliasHistoryEvents": [],
            "botAliasStatus": alias.status,
            "botId": alias.bot_id,
            "creationDateTime": alias.created,
            "lastUpdatedDateTime": alias.updated,
            "parentBotNetworks": [],
        })),
        None => not_found(&format!("Bot alias {alias_id}")),
    }
}

fn update_bot_alias(req: &Request, alias_id: &str, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let alias = match state.bot_aliases.get_mut(alias_id) {
        Some(alias) => alias,
        None => return not_found(&format!("Bot alias {alias_id}")),
    };
    if let Some(name) = str_field(body, "botAliasName") {
        alias.name = name.to_string();
    }
    if let Some(description) = body.get("description") {
        alias.description = description.clone();
    }
    if let Some(version) = body.get("botVersion") {
        alias.version = version.clone();
    }
    if let Some(settings) = body.get("botAliasLocaleSettings") {
        alias.locale_settings = settings.clone();
    }
    if let Some(settings) = body.get("conversationLogSettings") {
        alias.log_settings = settings.clone();
    }
    if let Some(settings) = body.get("sentimentAnalysisSettings") {
        alias.sentiment_settings = settings.clone();
    }
    // moto updates bot_id unconditionally via _get_param (body wins, uri
    // fallback); same result here.
    if let Some(from_body) = str_field(body, "botId") {
        alias.bot_id = from_body.to_string();
    } else {
        alias.bot_id = bot_id.to_string();
    }
    alias.updated = crate::proto::now_iso();
    ok(json!({
        "botAliasId": alias.id,
        "botAliasName": alias.name,
        "description": alias.description,
        "botVersion": alias.version,
        "botAliasLocaleSettings": alias.locale_settings,
        "conversationLogSettings": alias.log_settings,
        "sentimentAnalysisSettings": alias.sentiment_settings,
        "botAliasStatus": alias.status,
        "botId": alias.bot_id,
        "creationDateTime": alias.created,
        "lastUpdatedDateTime": alias.updated,
    }))
}

fn list_bot_aliases(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    // moto iterates every stored alias without filtering on bot_id and
    // returns the requested botId verbatim; preserved as-is.
    let summaries: Vec<Value> = state
        .bot_aliases
        .values()
        .map(|alias| {
            json!({
                "botAliasId": alias.id,
                "botAliasName": alias.name,
                "description": alias.description,
                "botVersion": alias.version,
                "botAliasStatus": alias.status,
                "creationDateTime": alias.created,
                "lastUpdatedDateTime": alias.updated,
            })
        })
        .collect();
    ok(json!({
        "botAliasSummaries": summaries,
        "nextToken": Value::Null,
        "botId": bot_id,
    }))
}

fn delete_bot_alias(req: &Request, alias_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.bot_aliases.remove(alias_id) {
        Some(alias) => ok(json!({
            "botAliasId": alias.id,
            "botId": alias.bot_id,
            "botAliasStatus": alias.status,
        })),
        None => not_found(&format!("Bot alias {alias_id}")),
    }
}

// ---------------------------------------------------------------------------
// Resource policies
// ---------------------------------------------------------------------------

fn create_resource_policy(req: &Request, arn: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let rp = ResourcePolicy {
        policy: str_field(body, "policy").unwrap_or_default().to_string(),
        revision_id: uuid::Uuid::new_v4().to_string(),
    };
    let revision_id = rp.revision_id.clone();
    state.resource_policies.insert(arn.to_string(), rp);
    ok(json!({ "resourceArn": arn, "revisionId": revision_id }))
}

fn describe_resource_policy(req: &Request, arn: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.resource_policies.get(arn) {
        Some(rp) => ok(json!({
            "resourceArn": arn,
            "policy": rp.policy,
            "revisionId": rp.revision_id,
        })),
        None => not_found(&format!("Resource policy for {arn}")),
    }
}

fn update_resource_policy(req: &Request, arn: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let rp = match state.resource_policies.get_mut(arn) {
        Some(rp) => rp,
        None => return not_found(&format!("Resource policy for {arn}")),
    };
    if let Some(expected) = revision_param(req, body) {
        if expected != rp.revision_id {
            return conflict("Revision ID mismatch");
        }
    }
    if let Some(policy) = str_field(body, "policy") {
        rp.policy = policy.to_string();
    }
    rp.revision_id = uuid::Uuid::new_v4().to_string();
    ok(json!({ "resourceArn": arn, "revisionId": rp.revision_id }))
}

fn delete_resource_policy(req: &Request, arn: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let existing = match state.resource_policies.get(arn) {
        Some(rp) => rp.revision_id.clone(),
        None => return not_found(&format!("Resource policy for {arn}")),
    };
    if let Some(expected) = revision_param(req, body) {
        if expected != existing {
            return conflict("Revision ID mismatch");
        }
    }
    state.resource_policies.remove(arn);
    ok(json!({ "resourceArn": arn, "revisionId": existing }))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tag_resource(req: &Request, arn: &str, body: &Value) -> Value {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    set_tags(state, arn, &body.get("tags").cloned().unwrap_or(json!({})));
    json!({})
}

fn untag_resource(req: &Request, arn: &str, body: &Value) -> Value {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    // botocore serializes `tagKeys` as a querystring parameter
    // (`?tagKeys=a&tagKeys=b`); fall back to the body for direct calls.
    let query_keys = crate::proto::query_map(&req.query_string);
    let keys: Vec<String> = query_keys.get("tagKeys").cloned().unwrap_or_else(|| {
        body.get("tagKeys")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|key| key.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    });
    if let Some(entry) = state.tags.get_mut(arn) {
        for key in &keys {
            entry.remove(key);
        }
    }
    json!({})
}

fn list_tags_for_resource(req: &Request, arn: &str) -> Value {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    json!({ "tags": tags_json(state, arn) })
}

fn tags_json(state: &State, arn: &str) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(entry) = state.tags.get(arn) {
        for (k, v) in entry {
            out.insert(k.clone(), Value::String(v.clone()));
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The state slot is keyed by (account, region) and shared across the
    /// test binary, so every test uses its own account.
    fn req(method: &str, path: &str, account: u64, body: &Value) -> Request {
        let (path, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "lexv2models".into(),
            operation: String::new(),
            method: method.into(),
            path,
            query_string: query,
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn resp_json(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// ARN as it rides in a rest-json requestUri (botocore quotes ':' and '/'
    /// inside the path member).
    fn enc(arn: &str) -> String {
        arn.replace('%', "%25")
            .replace(':', "%3A")
            .replace('/', "%2F")
    }

    #[test]
    fn bot_lifecycle() {
        let account = 111111111001;
        let created = dispatch(&req(
            "PUT",
            "/bots",
            account,
            &json!({
                "botName": "b",
                "roleArn": "arn:aws:iam::123456789012:role/r",
                "dataPrivacy": {"childDirected": false},
                "idleSessionTTLInSeconds": 300,
            }),
        ))
        .unwrap();
        let created = resp_json(&created);
        let bot_id = created["botId"].as_str().unwrap().to_string();
        assert_eq!(created["botStatus"], "CREATING");
        assert_eq!(created["idleSessionTTLInSeconds"], json!(300));

        let described = resp_json(
            &dispatch(&req("GET", &format!("/bots/{bot_id}"), account, &json!({}))).unwrap(),
        );
        assert_eq!(described["botName"], "b");
        assert_eq!(described["failureReasons"], json!([]));

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}"),
                account,
                &json!({"botName": "b2"}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["botName"], "b2");

        let listed = resp_json(&dispatch(&req("POST", "/bots", account, &json!({}))).unwrap());
        let ids: Vec<&str> = listed["botSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["botId"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&bot_id.as_str()));

        let deleted = resp_json(
            &dispatch(&req(
                "DELETE",
                &format!("/bots/{bot_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(deleted["botId"], json!(bot_id));

        let gone = dispatch(&req("GET", &format!("/bots/{bot_id}"), account, &json!({}))).unwrap();
        assert_eq!(gone.status, 400);
        assert!(resp_json(&gone)["__type"]
            .as_str()
            .unwrap()
            .ends_with("ResourceNotFoundException"));
    }

    #[test]
    fn moto_test_alias_and_dollar_arn() {
        let account = 111111111002;
        let created =
            resp_json(&dispatch(&req("PUT", "/bots", account, &json!({"botName": "b"}))).unwrap());
        let bot_id = created["botId"].as_str().unwrap().to_string();

        let alias = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}/botaliases"),
                account,
                &json!({"botAliasName": "prod", "botVersion": "1", "tags": {"layer": "alias"}}),
            ))
            .unwrap(),
        );
        let alias_id = alias["botAliasId"].as_str().unwrap().to_string();
        assert_eq!(alias["botAliasStatus"], "CREATING");
        assert_eq!(alias["tags"], json!({"layer": "alias"}));

        // moto's alias ARN uses literal `$` separators.
        let alias_arn = format!("arn:aws:lex:us-east-1:{account}:bot-alias/${bot_id}/${alias_id}");
        let tags = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/tags/{}", enc(&alias_arn)),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(tags["tags"]["layer"], "alias");

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/bots/{bot_id}/botaliases/{alias_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["botAliasHistoryEvents"], json!([]));
        assert_eq!(described["parentBotNetworks"], json!([]));

        // moto auto-creates the `test` alias (botVersion "1") with every bot.
        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/botaliases"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        let names: Vec<&str> = listed["botAliasSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["botAliasName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"test"));
        assert!(names.contains(&"prod"));
        assert_eq!(listed["botId"], json!(bot_id));
    }

    #[test]
    fn tag_and_policy_roundtrip() {
        let account = 111111111003;
        let created = resp_json(
            &dispatch(&req(
                "PUT",
                "/bots",
                account,
                &json!({"botName": "b", "botTags": {"k": "v"}}),
            ))
            .unwrap(),
        );
        assert_eq!(created["botTags"], json!({"k": "v"}));
        let bot_id = created["botId"].as_str().unwrap().to_string();
        let arn = format!("arn:aws:lex:us-east-1:{account}:bot/{bot_id}");
        let arn_enc = enc(&arn);

        assert_eq!(
            resp_json(
                &dispatch(&req(
                    "GET",
                    &format!("/tags/{arn_enc}"),
                    account,
                    &json!({})
                ))
                .unwrap()
            )["tags"]["k"],
            "v"
        );

        dispatch(&req(
            "POST",
            &format!("/tags/{arn_enc}"),
            account,
            &json!({"tags": {"k2": "v2"}}),
        ));
        dispatch(&req(
            "DELETE",
            &format!("/tags/{arn_enc}?tagKeys=k"),
            account,
            &json!({}),
        ));
        let tags = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/tags/{arn_enc}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        )["tags"]
            .clone();
        assert_eq!(tags["k"], json!(null));
        assert_eq!(tags["k2"], "v2");

        // Resource policy lifecycle; expectedRevisionId rides in the
        // querystring for rest-json.
        let created = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/policy/{arn_enc}"),
                account,
                &json!({"policy": "p"}),
            ))
            .unwrap(),
        );
        let rev = created["revisionId"].as_str().unwrap().to_string();

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/policy/{arn_enc}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["policy"], "p");
        assert_eq!(described["revisionId"], json!(rev));

        let conflict = dispatch(&req(
            "PUT",
            &format!("/policy/{arn_enc}?expectedRevisionId=bogus"),
            account,
            &json!({"policy": "q"}),
        ))
        .unwrap();
        assert_eq!(conflict.status, 400);
        assert!(resp_json(&conflict)["__type"]
            .as_str()
            .unwrap()
            .ends_with("ConflictException"));

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/policy/{arn_enc}?expectedRevisionId={rev}"),
                account,
                &json!({"policy": "q"}),
            ))
            .unwrap(),
        );
        let rev2 = updated["revisionId"].as_str().unwrap().to_string();
        assert_ne!(rev2, rev);

        let conflict = dispatch(&req(
            "DELETE",
            &format!("/policy/{arn_enc}?expectedRevisionId=bogus"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(conflict.status, 400);
        let deleted = resp_json(
            &dispatch(&req(
                "DELETE",
                &format!("/policy/{arn_enc}?expectedRevisionId={rev2}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(deleted["revisionId"], json!(rev2));
        assert_eq!(
            dispatch(&req(
                "GET",
                &format!("/policy/{arn_enc}"),
                account,
                &json!({})
            ))
            .unwrap()
            .status,
            400
        );
    }
}
