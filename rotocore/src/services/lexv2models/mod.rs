//! Lex Models V2 (`lexv2models`) — port of the deployed moto fork's
//! `moto/lexv2models` backend plus the rest of the botocore surface.
//!
//! moto implements 15 operations: bots (create/describe/update/list/delete),
//! bot aliases (same five), resource policies (create/describe/update/delete)
//! and tags (tag/untag/list); those mirror the fork wire-for-wire. The
//! remaining 85 modeled operations — locales, versions, intents, slots,
//! slot types, built-ins, imports/exports, bot recommendations, analytics
//! and utterances, test sets/executions/generations/discrepancy reports,
//! replicas, resource generation, the analyzer, custom vocabularies,
//! resource-policy statements — are absent from moto (every action
//! `NotImplementedError`s). They are implemented here directly from the
//! botocore wire model (`models.lex.v2-2020-08-07`): with the bot's
//! modeled status enums (e.g. `Creating`, `Building`, `InProgress`,
//! `Processing`), 202/204 status codes where the model declares them, and
//! the modeled `ResourceNotFoundException` (400 — the model's default
//! status for lexv2-models errors) for missing parents/resources.
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
//!   answers botocore's modeled `ResourceNotFoundException` (400) instead.
//!   For resource policies moto raises a bare `Exception` on revision
//!   mismatch (500); this backend answers the modeled `ConflictException`
//!   and only enforces the check when `expectedRevisionId` is present
//!   (moto compares even `None` against the revision id, which makes the
//!   optional parameter mandatory — a crash, not behavior to reproduce).

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
    // bot/{bot}/{version} -> version record (created by CreateBotVersion)
    bot_versions: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale} -> locale record
    locales: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale}/intent/{intent}
    intents: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale}/intent/{intent}/slot/{slot}
    slots: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale}/slottype/{type}
    slot_types: BTreeMap<String, Value>,
    // {resource}/{exportId} -> export record ("export"/"import")
    exports: BTreeMap<String, Value>,
    imports: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale}/botrecommendation/{id}
    bot_recommendations: BTreeMap<String, Value>,
    // {testSetId} -> test set; testsetgeneration/{id} -> generation
    test_sets: BTreeMap<String, Value>,
    test_set_generations: BTreeMap<String, Value>,
    test_set_discrepancy: BTreeMap<String, Value>,
    test_executions: BTreeMap<String, Value>,
    // bot/{bot}/{replicaRegion} -> replica record
    bot_replicas: BTreeMap<String, Value>,
    // bot/{bot}/{version}/{locale}/generation/{id} -> resource-generation record
    resource_generations: BTreeMap<String, Value>,
    // bot/{bot} -> analyzer request records
    analyzer: BTreeMap<String, Vec<Value>>,
    // bot/{bot}/{version}/{locale} -> custom vocabulary record
    custom_vocab: BTreeMap<String, Value>,
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
    // statement sub-operations ride on the same resource ARN.
    if let Some(rest) = path.strip_prefix("policy/") {
        if rest.is_empty() {
            return None;
        }
        let trimmed = rest.strip_suffix('/');
        let trimmed = trimmed.unwrap_or(rest);
        if trimmed.ends_with("/statements") {
            let arn = crate::proto::url_decode(trimmed.strip_suffix("/statements").unwrap());
            return match req.method.as_str() {
                "POST" => create_resource_policy_statement(req, &arn, &body),
                _ => None, // no other statement op ends at `/statements`
            };
        }
        // the model's requestUri carries a trailing `/` after the statement
        // id; match with or without it.
        if let Some((encoded_arn, statement_id)) = trimmed.rsplit_once("/statements/") {
            return match req.method.as_str() {
                "DELETE" => delete_resource_policy_statement(
                    req,
                    &crate::proto::url_decode(encoded_arn),
                    statement_id,
                ),
                _ => None,
            };
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
        // Bot versions
        ("PUT", ["bots", bot_id, "botversions"]) => create_bot_version(req, bot_id, &body),
        ("POST", ["bots", bot_id, "botversions"]) => list_bot_versions(req, bot_id),
        ("GET", ["bots", bot_id, "botversions", version]) => {
            describe_bot_version(req, bot_id, version)
        }
        ("DELETE", ["bots", bot_id, "botversions", version]) => {
            delete_bot_version(req, bot_id, version)
        }
        // Bot locales (build/list/create/update/delete)
        ("PUT", ["bots", bot_id, "botversions", version, "botlocales"]) => {
            create_bot_locale(req, bot_id, version, &body)
        }
        ("POST", ["bots", bot_id, "botversions", version, "botlocales"]) => {
            list_bot_locales(req, bot_id, version)
        }
        ("GET", ["bots", bot_id, "botversions", version, "botlocales", locale_id]) => {
            describe_bot_locale(req, bot_id, version, locale_id)
        }
        ("PUT", ["bots", bot_id, "botversions", version, "botlocales", locale_id]) => {
            update_bot_locale(req, bot_id, version, locale_id, &body)
        }
        ("DELETE", ["bots", bot_id, "botversions", version, "botlocales", locale_id]) => {
            delete_bot_locale(req, bot_id, version, locale_id)
        }
        ("POST", ["bots", bot_id, "botversions", version, "botlocales", locale_id]) => {
            build_bot_locale(req, bot_id, version, locale_id)
        }
        // Intents
        ("PUT", ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents"]) => {
            create_intent(req, bot_id, version, locale_id, &body)
        }
        ("POST", ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents"]) => {
            list_intents(req, bot_id, version, locale_id)
        }
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id],
        ) => describe_intent(req, bot_id, version, locale_id, intent_id),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id],
        ) => update_intent(req, bot_id, version, locale_id, intent_id, &body),
        (
            "DELETE",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id],
        ) => delete_intent(req, bot_id, version, locale_id, intent_id),
        // Slots
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id, "slots"],
        ) => create_slot(req, bot_id, version, locale_id, intent_id, &body),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id, "slots"],
        ) => list_slots(req, bot_id, version, locale_id, intent_id),
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id, "slots", slot_id],
        ) => describe_slot(req, bot_id, version, locale_id, intent_id, slot_id),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id, "slots", slot_id],
        ) => update_slot(req, bot_id, version, locale_id, intent_id, slot_id, &body),
        (
            "DELETE",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "intents", intent_id, "slots", slot_id],
        ) => delete_slot(req, bot_id, version, locale_id, intent_id, slot_id),
        // Slot types
        ("PUT", ["bots", bot_id, "botversions", version, "botlocales", locale_id, "slottypes"]) => {
            create_slot_type(req, bot_id, version, locale_id, &body)
        }
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "slottypes"],
        ) => list_slot_types(req, bot_id, version, locale_id),
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "slottypes", slot_type_id],
        ) => describe_slot_type(req, bot_id, version, locale_id, slot_type_id),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "slottypes", slot_type_id],
        ) => update_slot_type(req, bot_id, version, locale_id, slot_type_id, &body),
        (
            "DELETE",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "slottypes", slot_type_id],
        ) => delete_slot_type(req, bot_id, version, locale_id, slot_type_id),
        // Built-in intents and slot types
        ("POST", ["builtins", "locales", locale_id, "intents"]) => {
            list_builtin_intents(req, locale_id)
        }
        ("POST", ["builtins", "locales", locale_id, "slottypes"]) => {
            list_builtin_slot_types(req, locale_id)
        }
        // Imports / exports
        ("PUT", ["imports"]) => start_import(req, &body),
        ("POST", ["imports"]) => list_imports(req),
        ("GET", ["imports", import_id]) => describe_import(req, import_id),
        ("DELETE", ["imports", import_id]) => delete_import(req, import_id),
        ("PUT", ["exports"]) => create_export(req, &body),
        ("POST", ["exports"]) => list_exports(req),
        ("GET", ["exports", export_id]) => describe_export(req, export_id),
        ("PUT", ["exports", export_id]) => update_export(req, export_id),
        ("DELETE", ["exports", export_id]) => delete_export(req, export_id),
        ("POST", ["createuploadurl"]) => create_upload_url(req),
        // Bot recommendations
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations"],
        ) => start_bot_recommendation(req, bot_id, version, locale_id, &body),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations"],
        ) => list_bot_recommendations(req, bot_id, version, locale_id),
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations", rec_id],
        ) => describe_bot_recommendation(req, bot_id, version, locale_id, rec_id),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations", rec_id],
        ) => update_bot_recommendation(req, bot_id, version, locale_id, rec_id, &body),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations", rec_id, "stopbotrecommendation"],
        ) => stop_bot_recommendation(req, bot_id, version, locale_id, rec_id),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations", rec_id, "intents"],
        ) => list_recommended_intents(req, bot_id, version, locale_id, rec_id),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "botrecommendations", rec_id, "associatedtranscripts"],
        ) => search_associated_transcripts(req, bot_id, version, locale_id, rec_id),
        // Analytics and utterances
        ("POST", ["bots", bot_id, "analytics", metric]) => analytics_metrics(req, bot_id, metric),
        ("POST", ["bots", bot_id, "aggregatedutterances"]) => {
            list_aggregated_utterances(req, bot_id, &body)
        }
        ("DELETE", ["bots", bot_id, "utterances"]) => delete_utterances(req, bot_id),
        // Test sets, executions, generations, discrepancy reports
        ("POST", ["testsets"]) => list_test_sets(req),
        ("GET", ["testsets", test_set_id]) => describe_test_set(req, test_set_id),
        ("PUT", ["testsets", test_set_id]) => update_test_set(req, test_set_id, &body),
        ("DELETE", ["testsets", test_set_id]) => delete_test_set(req, test_set_id),
        ("POST", ["testsets", test_set_id, "records"]) => list_test_set_records(req, test_set_id),
        ("POST", ["testsets", test_set_id, "testexecutions"]) => {
            start_test_execution(req, test_set_id, &body)
        }
        ("POST", ["testsets", test_set_id, "testsetdiscrepancy"]) => {
            create_test_set_discrepancy_report(req, test_set_id, &body)
        }
        ("PUT", ["testsetgenerations"]) => start_test_set_generation(req, &body),
        ("GET", ["testsetgenerations", gen_id]) => describe_test_set_generation(req, gen_id),
        ("GET", ["testsetdiscrepancy", report_id]) => {
            describe_test_set_discrepancy_report(req, report_id)
        }
        ("POST", ["testexecutions"]) => list_test_executions(req),
        ("GET", ["testexecutions", exec_id]) => describe_test_execution(req, exec_id),
        ("GET", ["testexecutions", exec_id, "artifacturl"]) => {
            get_test_execution_artifacts_url(req, exec_id)
        }
        ("POST", ["testexecutions", exec_id, "results"]) => {
            list_test_execution_result_items(req, exec_id)
        }
        // Bot replicas
        ("PUT", ["bots", bot_id, "replicas"]) => create_bot_replica(req, bot_id, &body),
        ("POST", ["bots", bot_id, "replicas"]) => list_bot_replicas(req, bot_id),
        ("GET", ["bots", bot_id, "replicas", replica_region]) => {
            describe_bot_replica(req, bot_id, replica_region)
        }
        ("DELETE", ["bots", bot_id, "replicas", replica_region]) => {
            delete_bot_replica(req, bot_id, replica_region)
        }
        ("POST", ["bots", bot_id, "replicas", replica_region, "botaliases"]) => {
            list_bot_alias_replicas(req, bot_id, replica_region)
        }
        ("POST", ["bots", bot_id, "replicas", replica_region, "botversions"]) => {
            list_bot_version_replicas(req, bot_id, replica_region)
        }
        // Bot resource generation and analyzer
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "startgeneration"],
        ) => start_bot_resource_generation(req, bot_id, version, locale_id, &body),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "generations"],
        ) => list_bot_resource_generations(req, bot_id, version, locale_id),
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "generations", gen_id],
        ) => describe_bot_resource_generation(req, bot_id, version, locale_id, gen_id),
        ("POST", ["bots", bot_id, "botversions", version, "botlocales", locale_id, "generate"]) => {
            generate_bot_element(req, bot_id, version, locale_id, &body)
        }
        ("POST", ["bots", bot_id, "botanalyzer"]) => start_bot_analyzer(req, bot_id, &body),
        ("POST", ["bots", bot_id, "botanalyzer", "history"]) => {
            list_bot_analyzer_history(req, bot_id)
        }
        ("POST", ["bots", bot_id, "botanalyzer", "describe", request_id]) => {
            describe_bot_analyzer_recommendation(req, bot_id, request_id)
        }
        ("PUT", ["bots", bot_id, "botanalyzer", request_id, "stop"]) => {
            stop_bot_analyzer(req, bot_id, request_id)
        }
        ("DELETE", ["bots", bot_id, "botanalyzer", request_id]) => {
            delete_bot_analyzer_recommendation(req, bot_id, request_id)
        }
        // Custom vocabularies
        (
            "DELETE",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary"],
        ) => delete_custom_vocabulary(req, bot_id, version, locale_id),
        (
            "GET",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary", "DEFAULT", "metadata"],
        ) => describe_custom_vocabulary_metadata(req, bot_id, version, locale_id),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary", "DEFAULT", "list"],
        ) => list_custom_vocabulary_items(req, bot_id, version, locale_id),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary", "DEFAULT", "batchcreate"],
        ) => batch_custom_vocabulary(req, bot_id, version, locale_id, &body, "create"),
        (
            "POST",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary", "DEFAULT", "batchdelete"],
        ) => batch_custom_vocabulary(req, bot_id, version, locale_id, &body, "delete"),
        (
            "PUT",
            ["bots", bot_id, "botversions", version, "botlocales", locale_id, "customvocabulary", "DEFAULT", "batchupdate"],
        ) => batch_custom_vocabulary(req, bot_id, version, locale_id, &body, "update"),
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

// ---------------------------------------------------------------------------
// New family helpers
// ---------------------------------------------------------------------------

fn uuid_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn locale_key(bot_id: &str, version: &str, locale_id: &str) -> String {
    format!("{bot_id}/{version}/{locale_id}")
}

fn assert_bot(state: &State, bot_id: &str) -> Result<(), String> {
    if state.bots.contains_key(bot_id) {
        Ok(())
    } else {
        Err(format!("Bot {bot_id}"))
    }
}

/// Restrict a stored record to the members the op declares in its output
/// shape — everything else is stored for describe/list but never echoed
/// back where the model has no member for it.
fn pick(record: &Value, members: &[&str]) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(obj) = record.as_object() {
        for name in members {
            if let Some(v) = obj.get(*name) {
                if !v.is_null() {
                    out.insert((*name).to_string(), v.clone());
                }
            }
        }
    }
    Value::Object(out)
}

fn set_field(record: &mut Value, field: &str, body: &Value) {
    if let Some(v) = body.get(field).filter(|v| !v.is_null()) {
        record[field] = v.clone();
    }
}

// ---------------------------------------------------------------------------
// Bot versions
// ---------------------------------------------------------------------------

fn create_bot_version(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let bot = match state.bots.get(bot_id) {
        Some(b) => b,
        None => return not_found(&format!("Bot {bot_id}")),
    };
    let count = state
        .bot_versions
        .keys()
        .filter(|k| k.starts_with(&format!("{bot_id}/")))
        .count();
    let version = (count + 1).to_string();
    let keys = [
        "botId",
        "botName",
        "botVersion",
        "botStatus",
        "creationDateTime",
        "description",
        "botVersionLocaleSpecification",
    ];
    let record = json!({
        "botId": bot.id,
        "botName": bot.name,
        "botVersion": version,
        "botStatus": bot.status,
        "creationDateTime": crate::proto::now_iso(),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "botVersionLocaleSpecification": body
            .get("botVersionLocaleSpecification")
            .cloned()
            .unwrap_or(Value::Null),
        "roleArn": bot.role_arn,
        "dataPrivacy": bot.data_privacy,
        "idleSessionTTLInSeconds": bot.idle_session_ttl,
        "botType": bot.bot_type,
        "botMembers": bot.bot_members,
        "failureReasons": [],
        "parentBotNetworks": [],
    });
    record.as_object().map(|_| ()).unwrap_or_default();
    state
        .bot_versions
        .insert(format!("{bot_id}/{version}"), record.clone());
    drop(guard);
    Some(json_resp(202, pick(&record, &keys)))
}

fn describe_bot_version(req: &Request, bot_id: &str, version: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.bot_versions.get(&format!("{bot_id}/{version}")) {
        Some(r) => ok(r.clone()),
        None => not_found(&format!("Bot version {version}")),
    }
}

fn list_bot_versions(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{bot_id}/");
    let summaries: Vec<Value> = state
        .bot_versions
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "botName",
                    "botVersion",
                    "botStatus",
                    "creationDateTime",
                    "description",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersionSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn delete_bot_version(req: &Request, bot_id: &str, version: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = format!("{bot_id}/{version}");
    match state.bot_versions.remove(&key) {
        Some(r) => Some(json_resp(
            202,
            json!({
                "botId": bot_id,
                "botVersion": r["botVersion"],
                "botStatus": r["botStatus"],
            }),
        )),
        None => not_found(&format!("Bot version {version}")),
    }
}

// ---------------------------------------------------------------------------
// Bot locales
// ---------------------------------------------------------------------------

fn create_bot_locale(req: &Request, bot_id: &str, version: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let record = json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": body.get("localeId").cloned().unwrap_or(Value::Null),
        "localeName": body.get("localeName").cloned().unwrap_or(Value::Null),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "nluIntentConfidenceThreshold": body
            .get("nluIntentConfidenceThreshold")
            .cloned()
            .unwrap_or(Value::Null),
        "voiceSettings": body.get("voiceSettings").cloned().unwrap_or(Value::Null),
        "unifiedSpeechSettings": body
            .get("unifiedSpeechSettings")
            .cloned()
            .unwrap_or(Value::Null),
        "speechRecognitionSettings": body
            .get("speechRecognitionSettings")
            .cloned()
            .unwrap_or(Value::Null),
        "generativeAISettings": body
            .get("generativeAISettings")
            .cloned()
            .unwrap_or(Value::Null),
        "speechDetectionSensitivity": body
            .get("speechDetectionSensitivity")
            .cloned()
            .unwrap_or(Value::Null),
        "botLocaleStatus": "Creating",
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state.locales.insert(
        locale_key(
            bot_id,
            version,
            str_field(&record, "localeId").unwrap_or_default(),
        ),
        record.clone(),
    );
    drop(guard);
    Some(json_resp(
        202,
        pick(
            &record,
            &[
                "botId",
                "botVersion",
                "localeId",
                "localeName",
                "nluIntentConfidenceThreshold",
                "description",
                "voiceSettings",
                "unifiedSpeechSettings",
                "speechRecognitionSettings",
                "generativeAISettings",
                "speechDetectionSensitivity",
                "botLocaleStatus",
                "creationDateTime",
            ],
        ),
    ))
}

fn describe_bot_locale(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.locales.get(&locale_key(bot_id, version, locale_id)) {
        Some(r) => {
            let intents_count = state
                .intents
                .keys()
                .filter(|k| k.starts_with(&format!("{bot_id}/{version}/{locale_id}/intent/")))
                .count();
            let slot_types_count = state
                .slot_types
                .keys()
                .filter(|k| k.starts_with(&format!("{bot_id}/{version}/{locale_id}/slottype/")))
                .count();
            let mut body = r.clone();
            body["intentsCount"] = json!(intents_count);
            body["slotTypesCount"] = json!(slot_types_count);
            body["botLocaleHistoryEvents"] = json!([]);
            body["recommendedActions"] = json!([]);
            body["failureReasons"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Bot locale {locale_id}")),
    }
}

fn update_bot_locale(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = locale_key(bot_id, version, locale_id);
    let record = match state.locales.get_mut(&key) {
        Some(r) => r,
        None => return not_found(&format!("Bot locale {locale_id}")),
    };
    for field in [
        "localeName",
        "description",
        "nluIntentConfidenceThreshold",
        "voiceSettings",
        "unifiedSpeechSettings",
        "speechRecognitionSettings",
        "generativeAISettings",
        "speechDetectionSensitivity",
    ] {
        set_field(record, field, body);
    }
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let keys = [
        "botId",
        "botVersion",
        "localeId",
        "localeName",
        "nluIntentConfidenceThreshold",
        "description",
        "voiceSettings",
        "unifiedSpeechSettings",
        "speechRecognitionSettings",
        "generativeAISettings",
        "speechDetectionSensitivity",
        "botLocaleStatus",
        "creationDateTime",
        "lastUpdatedDateTime",
        "failureReasons",
        "recommendedActions",
    ];
    let response = pick(record, &keys);
    drop(guard);
    Some(json_resp(202, response))
}

fn delete_bot_locale(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = locale_key(bot_id, version, locale_id);
    match state.locales.remove(&key) {
        Some(_) => Some(json_resp(
            202,
            json!({
                "botId": bot_id,
                "botVersion": version,
                "localeId": locale_id,
                "botLocaleStatus": "Deleting",
            }),
        )),
        None => not_found(&format!("Bot locale {locale_id}")),
    }
}

fn list_bot_locales(req: &Request, bot_id: &str, version: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{bot_id}/{version}/");
    let summaries: Vec<Value> = state
        .locales
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "localeId",
                    "localeName",
                    "description",
                    "botLocaleStatus",
                    "lastUpdatedDateTime",
                    "lastBuildSubmittedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "botLocaleSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn build_bot_locale(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = locale_key(bot_id, version, locale_id);
    let record = match state.locales.get_mut(&key) {
        Some(r) => r,
        None => return not_found(&format!("Bot locale {locale_id}")),
    };
    record["botLocaleStatus"] = json!("Building");
    record["lastBuildSubmittedDateTime"] = json!(crate::proto::now_iso());
    let response = json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "botLocaleStatus": "Building",
        "lastBuildSubmittedDateTime": record["lastBuildSubmittedDateTime"],
    });
    drop(guard);
    Some(json_resp(202, response))
}

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

const INTENT_FULL: [&str; 21] = [
    "intentId",
    "intentName",
    "intentDisplayName",
    "description",
    "parentIntentSignature",
    "sampleUtterances",
    "dialogCodeHook",
    "fulfillmentCodeHook",
    "intentConfirmationSetting",
    "intentClosingSetting",
    "initialResponseSetting",
    "qnAIntentConfiguration",
    "qInConnectIntentConfiguration",
    "inputContexts",
    "outputContexts",
    "kendraConfiguration",
    "botId",
    "botVersion",
    "localeId",
    "creationDateTime",
    "lastUpdatedDateTime",
];

fn intent_key(bot_id: &str, version: &str, locale_id: &str, intent_id: &str) -> String {
    format!("{}/{}/{}/intent/{}", bot_id, version, locale_id, intent_id)
}

fn create_intent(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let intent_id = uuid_id();
    let record = json!({
        "intentId": intent_id,
        "intentName": str_field(body, "intentName").unwrap_or_default(),
        "intentDisplayName": body.get("intentDisplayName").cloned().unwrap_or(Value::Null),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "parentIntentSignature": body.get("parentIntentSignature").cloned().unwrap_or(Value::Null),
        "sampleUtterances": body.get("sampleUtterances").cloned().unwrap_or(Value::Null),
        "dialogCodeHook": body.get("dialogCodeHook").cloned().unwrap_or(Value::Null),
        "fulfillmentCodeHook": body.get("fulfillmentCodeHook").cloned().unwrap_or(Value::Null),
        "intentConfirmationSetting": body
            .get("intentConfirmationSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "intentClosingSetting": body.get("intentClosingSetting").cloned().unwrap_or(Value::Null),
        "initialResponseSetting": body.get("initialResponseSetting").cloned().unwrap_or(Value::Null),
        "qnAIntentConfiguration": body.get("qnAIntentConfiguration").cloned().unwrap_or(Value::Null),
        "qInConnectIntentConfiguration": body
            .get("qInConnectIntentConfiguration")
            .cloned()
            .unwrap_or(Value::Null),
        "inputContexts": body.get("inputContexts").cloned().unwrap_or(Value::Null),
        "outputContexts": body.get("outputContexts").cloned().unwrap_or(Value::Null),
        "kendraConfiguration": body.get("kendraConfiguration").cloned().unwrap_or(Value::Null),
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state.intents.insert(
        intent_key(bot_id, version, locale_id, &intent_id),
        record.clone(),
    );
    drop(guard);
    // CreateIntentResponse declares no lastUpdatedDateTime member.
    let create_view: Vec<&str> = INTENT_FULL
        .iter()
        .copied()
        .filter(|m| *m != "lastUpdatedDateTime")
        .collect();
    ok(pick(&record, &create_view))
}

fn describe_intent(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let full = intent_key(bot_id, version, locale_id, intent_id);
    let record = match state.intents.get(&full) {
        Some(r) => r.clone(),
        None => return not_found(&format!("Intent {intent_id}")),
    };
    // an intent's slots surface as slotPriorities in the describe envelope.
    let slots_cap = format!("{full}/slot/");
    let mut slot_priorities: Vec<Value> = state
        .slots
        .iter()
        .filter(|(k, _)| k.starts_with(&slots_cap))
        .enumerate()
        .map(|(i, (_, s))| {
            json!({
                "priority": i + 1,
                "slotId": s["slotId"],
            })
        })
        .collect();
    let mut body = record.clone();
    if !slot_priorities.is_empty() {
        if let Some(v) = slot_priorities.pop() {
            slot_priorities.insert(0, v);
        }
        body["slotPriorities"] = Value::Array(slot_priorities);
    }
    drop(guard);
    ok(body)
}

fn update_intent(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = intent_key(bot_id, version, locale_id, intent_id);
    let record = match state.intents.get_mut(&key) {
        Some(r) => r,
        None => return not_found(&format!("Intent {intent_id}")),
    };
    for field in [
        "intentName",
        "intentDisplayName",
        "description",
        "parentIntentSignature",
        "sampleUtterances",
        "dialogCodeHook",
        "fulfillmentCodeHook",
        "intentConfirmationSetting",
        "intentClosingSetting",
        "initialResponseSetting",
        "qnAIntentConfiguration",
        "qInConnectIntentConfiguration",
        "inputContexts",
        "outputContexts",
        "kendraConfiguration",
    ] {
        set_field(record, field, body);
    }
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    // UpdateIntentResponse echoes only a compact member set.
    let response = json!({
        "intentId": record["intentId"],
        "intentName": record["intentName"],
        "creationDateTime": record["creationDateTime"],
        "lastUpdatedDateTime": record["lastUpdatedDateTime"],
    });
    drop(guard);
    ok(response)
}

fn delete_intent(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = intent_key(bot_id, version, locale_id, intent_id);
    if state.intents.remove(&key).is_none() {
        return not_found(&format!("Intent {intent_id}"));
    }
    let slots_cap = format!("{key}/slot/");
    let slots: Vec<String> = state
        .slots
        .keys()
        .filter(|k| k.starts_with(&slots_cap))
        .cloned()
        .collect();
    for k in slots {
        state.slots.remove(&k);
    }
    drop(guard);
    Some(json_resp(204, json!({})))
}

// ---------------------------------------------------------------------------
// Slots
// ---------------------------------------------------------------------------

fn create_slot(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    if !state.intents.contains_key(&ikey) {
        return not_found(&format!("Intent {intent_id}"));
    }
    let slot_id = uuid_id();
    let record = json!({
        "slotId": slot_id,
        "slotName": str_field(body, "slotName").unwrap_or_default(),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "slotTypeId": body.get("slotTypeId").cloned().unwrap_or(Value::Null),
        "valueElicitationSetting": body
            .get("valueElicitationSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "obfuscationSetting": body.get("obfuscationSetting").cloned().unwrap_or(Value::Null),
        "multipleValuesSetting": body.get("multipleValuesSetting").cloned().unwrap_or(Value::Null),
        "subSlotSetting": body.get("subSlotSetting").cloned().unwrap_or(Value::Null),
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "intentId": intent_id,
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state
        .slots
        .insert(format!("{ikey}/slot/{slot_id}"), record.clone());
    drop(guard);
    ok(record)
}

fn describe_slot(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
    slot_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    match state.slots.get(&format!("{ikey}/slot/{slot_id}")) {
        Some(r) => ok(r.clone()),
        None => not_found(&format!("Slot {slot_id}")),
    }
}

fn update_slot(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
    slot_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    let record = match state.slots.get_mut(&format!("{ikey}/slot/{slot_id}")) {
        Some(r) => r,
        None => return not_found(&format!("Slot {slot_id}")),
    };
    for field in [
        "slotName",
        "description",
        "slotTypeId",
        "valueElicitationSetting",
        "obfuscationSetting",
        "multipleValuesSetting",
        "subSlotSetting",
    ] {
        set_field(record, field, body);
    }
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let response = json!({
        "slotId": record["slotId"],
        "slotName": record["slotName"],
        "creationDateTime": record["creationDateTime"],
        "lastUpdatedDateTime": record["lastUpdatedDateTime"],
    });
    drop(guard);
    ok(response)
}

fn delete_slot(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
    slot_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    match state.slots.remove(&format!("{ikey}/slot/{slot_id}")) {
        Some(_) => Some(json_resp(204, json!({}))),
        None => not_found(&format!("Slot {slot_id}")),
    }
}

// ---------------------------------------------------------------------------
// Slot types
// ---------------------------------------------------------------------------

fn slot_type_key(bot_id: &str, version: &str, locale_id: &str, slot_type_id: &str) -> String {
    format!(
        "{}/{}/{}/slottype/{}",
        bot_id, version, locale_id, slot_type_id
    )
}

fn create_slot_type(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let slot_type_id = uuid_id();
    let record = json!({
        "slotTypeId": slot_type_id,
        "slotTypeName": str_field(body, "slotTypeName").unwrap_or_default(),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "slotTypeValues": body.get("slotTypeValues").cloned().unwrap_or(Value::Null),
        "valueSelectionSetting": body
            .get("valueSelectionSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "parentSlotTypeSignature": body
            .get("parentSlotTypeSignature")
            .cloned()
            .unwrap_or(Value::Null),
        "externalSourceSetting": body
            .get("externalSourceSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "compositeSlotTypeSetting": body
            .get("compositeSlotTypeSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state.slot_types.insert(
        slot_type_key(bot_id, version, locale_id, &slot_type_id),
        record.clone(),
    );
    drop(guard);
    // CreateSlotTypeResponse declares no lastUpdatedDateTime member.
    let mut view = serde_json::Map::new();
    if let Some(o) = record.as_object() {
        for (k, v) in o {
            if k != "lastUpdatedDateTime" && !v.is_null() {
                view.insert(k.clone(), v.clone());
            }
        }
    }
    ok(Value::Object(view))
}

fn describe_slot_type(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    slot_type_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .slot_types
        .get(&slot_type_key(bot_id, version, locale_id, slot_type_id))
    {
        Some(r) => ok(r.clone()),
        None => not_found(&format!("Slot type {slot_type_id}")),
    }
}

fn update_slot_type(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    slot_type_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = slot_type_key(bot_id, version, locale_id, slot_type_id);
    let record = match state.slot_types.get_mut(&key) {
        Some(r) => r,
        None => return not_found(&format!("Slot type {slot_type_id}")),
    };
    for field in [
        "slotTypeName",
        "description",
        "slotTypeValues",
        "valueSelectionSetting",
        "parentSlotTypeSignature",
        "externalSourceSetting",
        "compositeSlotTypeSetting",
    ] {
        set_field(record, field, body);
    }
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let response = json!({
        "slotTypeId": record["slotTypeId"],
        "slotTypeName": record["slotTypeName"],
        "creationDateTime": record["creationDateTime"],
        "lastUpdatedDateTime": record["lastUpdatedDateTime"],
    });
    drop(guard);
    Some(json_resp(202, response))
}

fn delete_slot_type(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    slot_type_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .slot_types
        .remove(&slot_type_key(bot_id, version, locale_id, slot_type_id))
    {
        Some(_) => Some(json_resp(204, json!({}))),
        None => not_found(&format!("Slot type {slot_type_id}")),
    }
}

// ---------------------------------------------------------------------------
// List intents / slots / slot types
// ---------------------------------------------------------------------------

fn list_intents(req: &Request, bot_id: &str, version: &str, locale_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{}/intent/", locale_key(bot_id, version, locale_id));
    let summaries: Vec<Value> = state
        .intents
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "intentId",
                    "intentName",
                    "intentDisplayName",
                    "description",
                    "parentIntentSignature",
                    "inputContexts",
                    "outputContexts",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "intentSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn list_slots(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    intent_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    let cap = format!("{ikey}/slot/");
    let summaries: Vec<Value> = state
        .slots
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            json!({
                "slotId": r["slotId"],
                "slotName": r["slotName"],
                "description": r["description"],
                "slotConstraint": "Required",
                "slotTypeId": r["slotTypeId"],
                "lastUpdatedDateTime": r["lastUpdatedDateTime"],
            })
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "intentId": intent_id,
        "slotSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn list_slot_types(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{}/{}/{}/slottype/", bot_id, version, locale_id);
    let summaries: Vec<Value> = state
        .slot_types
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "slotTypeId",
                    "slotTypeName",
                    "description",
                    "parentSlotTypeSignature",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "slotTypeSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

// ---------------------------------------------------------------------------
// Built-in intents and slot types
// ---------------------------------------------------------------------------

const BUILT_IN_INTENTS: [(&str, &str); 4] = [
    (
        "AMAZON.CancelIntent",
        "Automatically ends a conversation or cancels a transaction.",
    ),
    (
        "AMAZON.FallbackIntent",
        "Matches user input when the bot cannot understand it.",
    ),
    (
        "AMAZON.StopIntent",
        "Stops an action or transaction in progress.",
    ),
    (
        "AMAZON.NavigateHomeIntent",
        "Navigates the user back to the home screen.",
    ),
];

const BUILT_IN_SLOT_TYPES: [(&str, &str); 4] = [
    ("AMAZON.AlphaNumeric", "Matches alphanumeric characters."),
    ("AMAZON.Date", "Matches a date the user says."),
    ("AMAZON.Number", "Matches a number the user says."),
    (
        "AMAZON.Person",
        "Matches a first name or a first and last name.",
    ),
];

fn list_builtin_intents(_req: &Request, locale_id: &str) -> Option<Response> {
    let summaries: Vec<Value> = BUILT_IN_INTENTS
        .iter()
        .map(|(sig, desc)| json!({"intentSignature": sig, "description": desc}))
        .collect();
    ok(json!({
        "localeId": locale_id,
        "builtInIntentSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn list_builtin_slot_types(_req: &Request, locale_id: &str) -> Option<Response> {
    let summaries: Vec<Value> = BUILT_IN_SLOT_TYPES
        .iter()
        .map(|(sig, desc)| json!({"slotTypeSignature": sig, "description": desc}))
        .collect();
    ok(json!({
        "localeId": locale_id,
        "builtInSlotTypeSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

// ---------------------------------------------------------------------------
// Imports and exports
// ---------------------------------------------------------------------------

fn start_import(req: &Request, body: &Value) -> Option<Response> {
    // importId is a required body member for rest-json (the URI does not
    // carry it).
    let import_id = str_field(body, "importId").unwrap_or_default().to_string();
    if import_id.is_empty() {
        return conflict("importId is required");
    }
    let record = json!({
        "importId": import_id,
        "resourceSpecification": body.get("resourceSpecification").cloned().unwrap_or(Value::Null),
        "mergeStrategy": str_field(body, "mergeStrategy").unwrap_or_default(),
        "importStatus": "InProgress",
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    state.imports.insert(import_id.clone(), record.clone());
    drop(guard);
    // StartImportResponse echoes only a compact member set...
    Some(json_resp(
        202,
        json!({
            "importId": record["importId"],
            "resourceSpecification": record["resourceSpecification"],
            "mergeStrategy": record["mergeStrategy"],
            "importStatus": record["importStatus"],
            "creationDateTime": record["creationDateTime"],
        }),
    ))
}

fn describe_import(req: &Request, import_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.imports.get(import_id) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Import {import_id}")),
    }
}

fn delete_import(req: &Request, import_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.imports.remove(import_id) {
        Some(_) => Some(json_resp(
            202,
            json!({"importId": import_id, "importStatus": "Deleting"}),
        )),
        None => not_found(&format!("Import {import_id}")),
    }
}

fn list_imports(req: &Request) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let summaries: Vec<Value> = state
        .imports
        .values()
        .map(|r| {
            pick(
                r,
                &[
                    "importId",
                    "importedResourceId",
                    "importedResourceName",
                    "importStatus",
                    "mergeStrategy",
                    "creationDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "importSummaries": summaries,
        "nextToken": Value::Null,
        "botId": Value::Null,
        "botVersion": Value::Null,
        "localeId": Value::Null,
    }))
}

fn create_export(req: &Request, body: &Value) -> Option<Response> {
    let export_id = uuid_id();
    let record = json!({
        "exportId": export_id,
        "resourceSpecification": body.get("resourceSpecification").cloned().unwrap_or(Value::Null),
        "fileFormat": str_field(body, "fileFormat").unwrap_or_default(),
        "exportStatus": "InProgress",
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    state.exports.insert(export_id.clone(), record.clone());
    drop(guard);
    Some(json_resp(202, record))
}

fn describe_export(req: &Request, export_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.exports.get(export_id) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            body["downloadUrl"] = json!(format!(
                "https://lex-modeling-orders.s3.us-west-2.amazonaws.com/exports/{export_id}"
            ));
            ok(body)
        }
        None => not_found(&format!("Export {export_id}")),
    }
}

fn update_export(req: &Request, export_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let record = match state.exports.get_mut(export_id) {
        Some(r) => r,
        None => return not_found(&format!("Export {export_id}")),
    };
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let response = json!({
        "exportId": record["exportId"],
        "resourceSpecification": record["resourceSpecification"],
        "fileFormat": record["fileFormat"],
        "exportStatus": record["exportStatus"],
        "creationDateTime": record["creationDateTime"],
        "lastUpdatedDateTime": record["lastUpdatedDateTime"],
    });
    drop(guard);
    Some(json_resp(202, response))
}

fn delete_export(req: &Request, export_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.exports.remove(export_id) {
        Some(_) => Some(json_resp(
            202,
            json!({"exportId": export_id, "exportStatus": "Deleting"}),
        )),
        None => not_found(&format!("Export {export_id}")),
    }
}

fn list_exports(req: &Request) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let summaries: Vec<Value> = state
        .exports
        .values()
        .map(|r| {
            pick(
                r,
                &[
                    "exportId",
                    "resourceSpecification",
                    "fileFormat",
                    "exportStatus",
                    "creationDateTime",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "exportSummaries": summaries,
        "nextToken": Value::Null,
        "botId": Value::Null,
        "botVersion": Value::Null,
        "localeId": Value::Null,
    }))
}

fn create_upload_url(_req: &Request) -> Option<Response> {
    let import_id = uuid_id();
    ok(json!({
        "importId": import_id,
        "uploadUrl": format!(
            "https://lexv2-models-import.s3.us-west-2.amazonaws.com/{import_id}/files.zip"
        ),
    }))
}

// ---------------------------------------------------------------------------
// Bot recommendations
// ---------------------------------------------------------------------------

fn start_bot_recommendation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let rec_id = uuid_id();
    let record = json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "botRecommendationId": rec_id,
        "botRecommendationStatus": "Processing",
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
        "transcriptSourceSetting": body
            .get("transcriptSourceSetting")
            .cloned()
            .unwrap_or(Value::Null),
        "encryptionSetting": body.get("encryptionSetting").cloned().unwrap_or(Value::Null),
    });
    state.bot_recommendations.insert(
        format!("{bot_id}/{version}/{locale_id}/botrecommendation/{rec_id}"),
        record.clone(),
    );
    drop(guard);
    Some(json_resp(
        202,
        pick(
            &record,
            &[
                "botId",
                "botVersion",
                "localeId",
                "botRecommendationId",
                "botRecommendationStatus",
                "creationDateTime",
                "transcriptSourceSetting",
                "encryptionSetting",
            ],
        ),
    ))
}

fn rec_key(bot_id: &str, version: &str, locale_id: &str, rec_id: &str) -> String {
    format!(
        "{}/{}/{}/botrecommendation/{}",
        bot_id, version, locale_id, rec_id
    )
}

fn require_recommendation(
    state: &State,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
) -> Result<String, Option<Response>> {
    let key = rec_key(bot_id, version, locale_id, rec_id);
    if state.bot_recommendations.contains_key(&key) {
        Ok(key)
    } else {
        Err(not_found(&format!("Bot recommendation {rec_id}")))
    }
}

fn describe_bot_recommendation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match require_recommendation(state, bot_id, version, locale_id, rec_id) {
        Ok(key) => match state.bot_recommendations.get(&key) {
            Some(r) => {
                let mut body = r.clone();
                body["failureReasons"] = json!([]);
                Some(json_resp(200, body))
            }
            None => not_found(&format!("Bot recommendation {rec_id}")),
        },
        Err(resp) => resp,
    }
}

fn list_bot_recommendations(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{}/{}/{}/botrecommendation/", bot_id, version, locale_id);
    let summaries: Vec<Value> = state
        .bot_recommendations
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "botRecommendationId",
                    "botRecommendationStatus",
                    "creationDateTime",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "botRecommendationSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn update_bot_recommendation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = match require_recommendation(state, bot_id, version, locale_id, rec_id) {
        Ok(k) => k,
        Err(resp) => match resp {
            Some(r) => return Some(r),
            None => return not_found(&format!("Bot recommendation {rec_id}")),
        },
    };
    let record = state.bot_recommendations.get_mut(&key).unwrap();
    if let Some(v) = body.get("encryptionSetting").filter(|v| !v.is_null()) {
        record["encryptionSetting"] = v.clone();
    }
    record["botRecommendationStatus"] = json!("Updating");
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let response = json!({
        "botId": record["botId"],
        "botVersion": record["botVersion"],
        "localeId": record["localeId"],
        "botRecommendationId": record["botRecommendationId"],
        "botRecommendationStatus": record["botRecommendationStatus"],
        "creationDateTime": record["creationDateTime"],
        "lastUpdatedDateTime": record["lastUpdatedDateTime"],
        "transcriptSourceSetting": record["transcriptSourceSetting"],
        "encryptionSetting": record["encryptionSetting"],
    });
    drop(guard);
    Some(json_resp(202, response))
}

fn stop_bot_recommendation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = match require_recommendation(state, bot_id, version, locale_id, rec_id) {
        Ok(k) => k,
        Err(resp) => match resp {
            Some(r) => return Some(r),
            None => return not_found(&format!("Bot recommendation {rec_id}")),
        },
    };
    let record = state.bot_recommendations.get_mut(&key).unwrap();
    record["botRecommendationStatus"] = json!("Stopping");
    let response = json!({
        "botId": record["botId"],
        "botVersion": record["botVersion"],
        "localeId": record["localeId"],
        "botRecommendationId": record["botRecommendationId"],
        "botRecommendationStatus": record["botRecommendationStatus"],
    });
    drop(guard);
    Some(json_resp(202, response))
}

fn list_recommended_intents(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(resp) = require_recommendation(state, bot_id, version, locale_id, rec_id) {
        return match resp {
            Some(r) => Some(r),
            None => not_found(&format!("Bot recommendation {rec_id}")),
        };
    }
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "botRecommendationId": rec_id,
        "summaryList": [],
        "nextToken": Value::Null,
    }))
}

fn search_associated_transcripts(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    rec_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(resp) = require_recommendation(state, bot_id, version, locale_id, rec_id) {
        return match resp {
            Some(r) => Some(r),
            None => not_found(&format!("Bot recommendation {rec_id}")),
        };
    }
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "botRecommendationId": rec_id,
        "associatedTranscripts": [],
        "nextIndex": Value::Null,
        "totalResults": json!(0),
    }))
}

// ---------------------------------------------------------------------------
// Analytics
// ---------------------------------------------------------------------------

fn analytics_metrics(req: &Request, bot_id: &str, metric: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    // each op names its result member differently in the model
    let member = match metric {
        "sessions" => "sessions",
        "utterances" => "utterances",
        "intentpaths" => "nodeSummaries",
        _ => "results",
    };
    ok(json!({
        "botId": bot_id,
        member: [],
        "nextToken": Value::Null,
    }))
}

fn list_aggregated_utterances(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    ok(json!({
        "botId": bot_id,
        "botAliasId": Value::Null,
        "localeId": body.get("localeId").cloned().unwrap_or(Value::Null),
        "aggregationDuration": body
            .get("aggregationDuration")
            .cloned()
            .unwrap_or(Value::Null),
        "aggregationWindowStartTime": crate::proto::now_iso(),
        "aggregationWindowEndTime": crate::proto::now_iso(),
        "aggregationLastRefreshedDateTime": crate::proto::now_iso(),
        "aggregatedUtterancesSummaries": [],
        "nextToken": Value::Null,
    }))
}

fn delete_utterances(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    drop(guard);
    Some(json_resp(204, json!({})))
}

// ---------------------------------------------------------------------------
// Test sets, executions, generations, discrepancy reports
// ---------------------------------------------------------------------------

fn start_test_set_generation(req: &Request, body: &Value) -> Option<Response> {
    let gen_id = uuid_id();
    let test_set_id = uuid_id();
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let test_set = json!({
        "testSetId": test_set_id,
        "testSetName": str_field(body, "testSetName").unwrap_or_default(),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "modality": "Text",
        "status": "Ready",
        "roleArn": str_field(body, "roleArn").unwrap_or_default(),
        "storageLocation": body.get("storageLocation").cloned().unwrap_or(Value::Null),
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state
        .test_sets
        .insert(test_set_id.clone(), test_set.clone());
    let record = json!({
        "testSetGenerationId": gen_id,
        "testSetGenerationStatus": "Generating",
        "testSetId": test_set_id,
        "testSetName": body.get("testSetName").cloned().unwrap_or(Value::Null),
        "description": body.get("description").cloned().unwrap_or(Value::Null),
        "storageLocation": body.get("storageLocation").cloned().unwrap_or(Value::Null),
        "generationDataSource": body
            .get("generationDataSource")
            .cloned()
            .unwrap_or(Value::Null),
        "roleArn": str_field(body, "roleArn").unwrap_or_default(),
        "testSetTags": body.get("testSetTags").cloned().unwrap_or(Value::Null),
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state.test_set_generations.insert(gen_id, record.clone());
    let response = pick(
        &record,
        &[
            "testSetGenerationId",
            "testSetGenerationStatus",
            "testSetName",
            "description",
            "storageLocation",
            "generationDataSource",
            "roleArn",
            "testSetTags",
            "creationDateTime",
        ],
    );
    drop(guard);
    Some(json_resp(202, response))
}

fn describe_test_set_generation(req: &Request, gen_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.test_set_generations.get(gen_id) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Test set generation {gen_id}")),
    }
}

fn describe_test_set(req: &Request, test_set_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.test_sets.get(test_set_id) {
        Some(r) => ok(r.clone()),
        None => not_found(&format!("Test set {test_set_id}")),
    }
}

fn update_test_set(req: &Request, test_set_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let record = match state.test_sets.get_mut(test_set_id) {
        Some(r) => r,
        None => return not_found(&format!("Test set {test_set_id}")),
    };
    for field in ["testSetName", "description"] {
        set_field(record, field, body);
    }
    record["lastUpdatedDateTime"] = json!(crate::proto::now_iso());
    let response = record.clone();
    drop(guard);
    ok(response)
}

fn delete_test_set(req: &Request, test_set_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if state.test_sets.remove(test_set_id).is_none() {
        return not_found(&format!("Test set {test_set_id}"));
    }
    drop(guard);
    Some(json_resp(204, json!({})))
}

fn list_test_sets(req: &Request) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    // TestSetSummary carries the same member set as the describe body.
    let summaries: Vec<Value> = state
        .test_sets
        .values()
        .map(|r| {
            pick(
                r,
                &[
                    "testSetId",
                    "testSetName",
                    "description",
                    "modality",
                    "status",
                    "roleArn",
                    "numTurns",
                    "storageLocation",
                    "creationDateTime",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({"testSets": summaries, "nextToken": Value::Null}))
}

fn list_test_set_records(req: &Request, test_set_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state.test_sets.contains_key(test_set_id) {
        return not_found(&format!("Test set {test_set_id}"));
    }
    ok(json!({"testSetRecords": [], "nextToken": Value::Null}))
}

fn start_test_execution(req: &Request, test_set_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let test_set = match state.test_sets.get(test_set_id) {
        Some(t) => t.clone(),
        None => return not_found(&format!("Test set {test_set_id}")),
    };
    let exec_id = uuid_id();
    let exec = json!({
        "testExecutionId": exec_id,
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
        "testExecutionStatus": "Pending",
        "testSetId": test_set_id,
        "testSetName": test_set["testSetName"],
        "target": body.get("target").cloned().unwrap_or(Value::Null),
        "apiMode": str_field(body, "apiMode").unwrap_or_default(),
        "testExecutionModality": test_set["modality"],
    });
    let response = pick(
        &exec,
        &[
            "testExecutionId",
            "creationDateTime",
            "testSetId",
            "testSetName",
            "target",
            "apiMode",
            "testExecutionModality",
        ],
    );
    state.test_executions.insert(exec_id.clone(), exec);
    drop(guard);
    Some(json_resp(202, response))
}

fn describe_test_execution(req: &Request, exec_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.test_executions.get(exec_id) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Test execution {exec_id}")),
    }
}

fn list_test_executions(req: &Request) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    // executions were stored at start_test_execution time (cloned before the
    // guard dropped); describe handles the rest.
    let summaries: Vec<Value> = state
        .test_executions
        .values()
        .map(|r| {
            pick(
                r,
                &[
                    "testExecutionId",
                    "creationDateTime",
                    "lastUpdatedDateTime",
                    "testExecutionStatus",
                    "testSetId",
                    "testSetName",
                    "target",
                    "apiMode",
                    "testExecutionModality",
                ],
            )
        })
        .collect();
    ok(json!({"testExecutions": summaries, "nextToken": Value::Null}))
}

fn list_test_execution_result_items(req: &Request, exec_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state.test_executions.contains_key(exec_id) {
        return not_found(&format!("Test execution {exec_id}"));
    }
    ok(json!({"testExecutionResults": [], "nextToken": Value::Null}))
}

fn get_test_execution_artifacts_url(req: &Request, exec_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state.test_executions.contains_key(exec_id) {
        return not_found(&format!("Test execution {exec_id}"));
    }
    ok(json!({
        "testExecutionId": exec_id,
        "downloadArtifactsUrl": format!(
            "https://lex-testing-scripts.s3.us-west-2.amazonaws.com/{exec_id}/artifacts.zip"
        ),
    }))
}

fn create_test_set_discrepancy_report(
    req: &Request,
    test_set_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state.test_sets.contains_key(test_set_id) {
        return not_found(&format!("Test set {test_set_id}"));
    }
    let report_id = uuid_id();
    let record = json!({
        "testSetDiscrepancyReportId": report_id,
        "testSetId": test_set_id,
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDataTime": crate::proto::now_iso(),
        "target": body.get("target").cloned().unwrap_or(Value::Null),
        "testSetDiscrepancyReportStatus": "InProgress",
    });
    state.test_set_discrepancy.insert(report_id, record.clone());
    drop(guard);
    Some(json_resp(
        202,
        pick(
            &record,
            &[
                "testSetDiscrepancyReportId",
                "testSetId",
                "creationDateTime",
                "target",
            ],
        ),
    ))
}

fn describe_test_set_discrepancy_report(req: &Request, report_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state.test_set_discrepancy.get(report_id) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            ok(body)
        }
        None => not_found(&format!("Test set discrepancy report {report_id}")),
    }
}

// ---------------------------------------------------------------------------
// Bot replicas
// ---------------------------------------------------------------------------

fn create_bot_replica(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let replica_region = str_field(body, "replicaRegion")
        .unwrap_or_default()
        .to_string();
    if replica_region.is_empty() {
        return conflict("replicaRegion is required");
    }
    let record = json!({
        "botId": bot_id,
        "replicaRegion": replica_region,
        "sourceRegion": req.region,
        "botReplicaStatus": "Enabling",
        "creationDateTime": crate::proto::now_iso(),
        "failureReasons": [],
    });
    state.bot_replicas.insert(
        format!(
            "{bot_id}/{}",
            record["replicaRegion"].as_str().unwrap_or_default()
        ),
        record.clone(),
    );
    drop(guard);
    Some(json_resp(
        202,
        pick(
            &record,
            &[
                "botId",
                "replicaRegion",
                "sourceRegion",
                "botReplicaStatus",
                "creationDateTime",
            ],
        ),
    ))
}

fn describe_bot_replica(req: &Request, bot_id: &str, replica_region: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .bot_replicas
        .get(&format!("{bot_id}/{replica_region}"))
    {
        Some(r) => Some(json_resp(200, r.clone())),
        None => not_found(&format!("Bot replica {replica_region}")),
    }
}

fn delete_bot_replica(req: &Request, bot_id: &str, replica_region: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .bot_replicas
        .remove(&format!("{bot_id}/{replica_region}"))
    {
        Some(r) => Some(json_resp(
            202,
            json!({
                "botId": r["botId"],
                "replicaRegion": r["replicaRegion"],
                "botReplicaStatus": "Deleting",
            }),
        )),
        None => not_found(&format!("Bot replica {replica_region}")),
    }
}

fn list_bot_replicas(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{bot_id}/");
    let summaries: Vec<Value> = state
        .bot_replicas
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "replicaRegion",
                    "creationDateTime",
                    "botReplicaStatus",
                    "failureReasons",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "sourceRegion": req.region,
        "botReplicaSummaries": summaries,
    }))
}

fn list_bot_alias_replicas(req: &Request, bot_id: &str, replica_region: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state
        .bot_replicas
        .contains_key(&format!("{bot_id}/{replica_region}"))
    {
        return not_found(&format!("Bot replica {replica_region}"));
    }
    ok(json!({
        "botId": bot_id,
        "replicaRegion": replica_region,
        "sourceRegion": req.region,
        "botAliasReplicaSummaries": [],
        "nextToken": Value::Null,
    }))
}

fn list_bot_version_replicas(
    req: &Request,
    bot_id: &str,
    replica_region: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state
        .bot_replicas
        .contains_key(&format!("{bot_id}/{replica_region}"))
    {
        return not_found(&format!("Bot replica {replica_region}"));
    }
    ok(json!({
        "botId": bot_id,
        "replicaRegion": replica_region,
        "sourceRegion": req.region,
        "botVersionReplicaSummaries": [],
        "nextToken": Value::Null,
    }))
}

// ---------------------------------------------------------------------------
// Bot resource generation
// ---------------------------------------------------------------------------

fn start_bot_resource_generation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let gen_id = uuid_id();
    let record = json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "generationId": gen_id,
        "generationStatus": "Processing",
        "generationInputPrompt": str_field(body, "generationInputPrompt").unwrap_or_default(),
        "modelArn": format!(
            "arn:aws:lex:{region}:{account}:model/{gen_id}",
            region = req.region,
            account = req.account,
        ),
        "creationDateTime": crate::proto::now_iso(),
        "lastUpdatedDateTime": crate::proto::now_iso(),
    });
    state.resource_generations.insert(
        format!("{bot_id}/{version}/{locale_id}/generation/{gen_id}"),
        record.clone(),
    );
    drop(guard);
    Some(json_resp(202, record))
}

fn describe_bot_resource_generation(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    gen_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let key = format!("{bot_id}/{version}/{locale_id}/generation/{gen_id}");
    match state.resource_generations.get(&key) {
        Some(r) => {
            let mut body = r.clone();
            body["failureReasons"] = json!([]);
            body["generatedBotLocaleUrl"] = json!(format!(
                "https://lex-generated-resources.s3.us-west-2.amazonaws.com/{gen_id}/locale.json"
            ));
            ok(body)
        }
        None => not_found(&format!("Bot resource generation {gen_id}")),
    }
}

fn list_bot_resource_generations(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let cap = format!("{bot_id}/{version}/{locale_id}/generation/");
    let summaries: Vec<Value> = state
        .resource_generations
        .iter()
        .filter(|(k, _)| k.starts_with(&cap))
        .map(|(_, r)| {
            pick(
                r,
                &[
                    "generationId",
                    "generationStatus",
                    "creationDateTime",
                    "lastUpdatedDateTime",
                ],
            )
        })
        .collect();
    ok(json!({
        "botId": bot_id,
        "botVersion": version,
        "localeId": locale_id,
        "generationSummaries": summaries,
        "nextToken": Value::Null,
    }))
}

fn generate_bot_element(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let intent_id = str_field(body, "intentId").unwrap_or_default();
    let ikey = intent_key(bot_id, version, locale_id, intent_id);
    let intent_name = match state.intents.get(&ikey) {
        Some(i) => i["intentName"].as_str().unwrap_or_default().to_string(),
        None => return not_found(&format!("Intent {intent_id}")),
    };
    drop(guard);
    Some(json_resp(
        202,
        json!({
            "botId": bot_id,
            "botVersion": version,
            "localeId": locale_id,
            "intentId": intent_id,
            "sampleUtterances": [
                {"utterance": format!("Tell me about {intent_name}")},
                {"utterance": format!("I want {intent_name}")},
            ],
        }),
    ))
}

// ---------------------------------------------------------------------------
// Bot analyzer
// ---------------------------------------------------------------------------

fn require_analyzer(
    state: &State,
    bot_id: &str,
    request_id: &str,
) -> Result<Value, Option<Response>> {
    if let Err(bot) = assert_bot(state, bot_id) {
        return Err(not_found(&bot));
    }
    match state.analyzer.get(bot_id).and_then(|h| {
        h.iter()
            .find(|r| r["botAnalyzerRequestId"] == json!(request_id))
            .cloned()
    }) {
        Some(record) => Ok(record),
        None => Err(not_found(&format!("Bot analyzer request {request_id}"))),
    }
}

fn start_bot_analyzer(req: &Request, bot_id: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let request_id = uuid_id();
    let record = json!({
        "botAnalyzerRequestId": request_id,
        "botAnalyzerStatus": "Processing",
        "botId": bot_id,
        "botVersion": body.get("botVersion").cloned().unwrap_or(Value::Null),
        "localeId": body.get("localeId").cloned().unwrap_or(Value::Null),
        "creationDateTime": crate::proto::now_iso(),
    });
    state
        .analyzer
        .entry(bot_id.to_string())
        .or_default()
        .push(record.clone());
    drop(guard);
    Some(json_resp(202, record))
}

fn describe_bot_analyzer_recommendation(
    req: &Request,
    bot_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match require_analyzer(state, bot_id, request_id) {
        Ok(record) => Some(json_resp(200, record)),
        Err(resp) => resp,
    }
}

fn stop_bot_analyzer(req: &Request, bot_id: &str, request_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let record = match require_analyzer(state, bot_id, request_id) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if let Some(entry) = state.analyzer.get_mut(bot_id) {
        if let Some(rec) = entry
            .iter_mut()
            .find(|r| r["botAnalyzerRequestId"] == json!(request_id))
        {
            rec["botAnalyzerStatus"] = json!("Stopping");
        }
    }
    let response = json!({
        "botAnalyzerRequestId": request_id,
        "botAnalyzerStatus": "Stopping",
        "botId": bot_id,
        "botVersion": record["botVersion"],
        "localeId": record["localeId"],
    });
    drop(guard);
    Some(json_resp(200, response))
}

fn delete_bot_analyzer_recommendation(
    req: &Request,
    bot_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(resp) = require_analyzer(state, bot_id, request_id) {
        return resp;
    }
    if let Some(entry) = state.analyzer.get_mut(bot_id) {
        entry.retain(|r| r["botAnalyzerRequestId"] != json!(request_id));
    }
    drop(guard);
    Some(json_resp(204, json!({})))
}

fn list_bot_analyzer_history(req: &Request, bot_id: &str) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    let summaries: Vec<Value> = state
        .analyzer
        .get(bot_id)
        .map(|records| {
            records
                .iter()
                .map(|r| {
                    pick(
                        r,
                        &[
                            "botAnalyzerRequestId",
                            "botAnalyzerStatus",
                            "creationDateTime",
                        ],
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({
        "botId": bot_id,
        "botVersion": Value::Null,
        "localeId": Value::Null,
        "botAnalyzerHistoryList": summaries,
        "nextToken": Value::Null,
    }))
}

// ---------------------------------------------------------------------------
// Custom vocabularies
// ---------------------------------------------------------------------------

fn vocab_key(bot_id: &str, version: &str, locale_id: &str) -> String {
    format!("{bot_id}/{version}/{locale_id}/customvocabulary/DEFAULT")
}

fn batch_custom_vocabulary(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
    body: &Value,
    mode: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    if let Err(bot) = assert_bot(state, bot_id) {
        return not_found(&bot);
    }
    if !state
        .locales
        .contains_key(&locale_key(bot_id, version, locale_id))
    {
        return not_found(&format!("Bot locale {locale_id}"));
    }
    let key = vocab_key(bot_id, version, locale_id);
    // The custom vocabulary is created implicitly on the first batch call.
    if !state.custom_vocab.contains_key(&key) {
        state.custom_vocab.insert(
            key.clone(),
            json!({
                "customVocabularyStatus": "Ready",
                "creationDateTime": crate::proto::now_iso(),
                "items": [],
            }),
        );
    }
    let vocab = state.custom_vocab.get_mut(&key).unwrap();
    if !vocab["items"].is_array() {
        vocab["items"] = json!([]);
    }
    let list = body
        .get("customVocabularyItemList")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut errors: Vec<Value> = Vec::new();
    let mut resources: Vec<Value> = Vec::new();
    for item in list {
        let item_id = str_field(&item, "itemId").unwrap_or_default().to_string();
        if item_id.is_empty() {
            errors.push(json!({"itemId": item_id, "errorMessage": "itemId is required"}));
            continue;
        }
        let exists = vocab["items"]
            .as_array()
            .map(|a| a.iter().any(|i| i["itemId"] == json!(item_id)))
            .unwrap_or_default();
        match mode {
            "create" => {
                if exists {
                    errors.push(json!({
                        "itemId": item_id,
                        "errorMessage": "item already exists",
                    }));
                } else {
                    resources.push(item.clone());
                    if let Some(items) = vocab["items"].as_array_mut() {
                        items.push(item.clone());
                    }
                }
            }
            "update" => {
                if !exists {
                    errors.push(json!({
                        "itemId": item_id,
                        "errorMessage": "item not found",
                    }));
                } else if let Some(items) = vocab["items"].as_array_mut() {
                    if let Some(rec) = items.iter_mut().find(|i| i["itemId"] == json!(item_id)) {
                        for field in ["phrase", "weight", "displayAs"] {
                            set_field(rec, field, &item);
                        }
                        resources.push(rec.clone());
                    }
                }
            }
            _ => {
                if !exists {
                    errors.push(json!({
                        "itemId": item_id,
                        "errorMessage": "item not found",
                    }));
                } else if let Some(items) = vocab["items"].as_array_mut() {
                    items.retain(|i| i["itemId"] != json!(item_id));
                    resources.push(item);
                }
            }
        }
    }
    if let Some(items) = vocab["items"].as_array_mut() {
        items.shrink_to_fit();
    }
    let status = vocab["customVocabularyStatus"].clone();
    let creation = vocab["creationDateTime"].clone();
    drop(guard);
    Some(json_resp(
        200,
        json!({
            "botId": bot_id,
            "botVersion": version,
            "localeId": locale_id,
            "resources": resources,
            "errors": errors,
            "customVocabularyStatus": status,
            "creationDateTime": creation,
        }),
    ))
}

fn describe_custom_vocabulary_metadata(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .custom_vocab
        .get(&vocab_key(bot_id, version, locale_id))
    {
        Some(v) => Some(json_resp(
            200,
            json!({
                "botId": bot_id,
                "botVersion": version,
                "localeId": locale_id,
                "customVocabularyStatus": v["customVocabularyStatus"],
                "creationDateTime": v["creationDateTime"],
                "lastUpdatedDateTime": v["creationDateTime"],
            }),
        )),
        None => not_found(&format!("Custom vocabulary for locale {locale_id}")),
    }
}

fn list_custom_vocabulary_items(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .custom_vocab
        .get(&vocab_key(bot_id, version, locale_id))
    {
        Some(v) => ok(json!({
            "botId": bot_id,
            "botVersion": version,
            "localeId": locale_id,
            "customVocabularyItems": v["items"],
            "nextToken": Value::Null,
        })),
        None => not_found(&format!("Custom vocabulary for locale {locale_id}")),
    }
}

fn delete_custom_vocabulary(
    req: &Request,
    bot_id: &str,
    version: &str,
    locale_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    match state
        .custom_vocab
        .remove(&vocab_key(bot_id, version, locale_id))
    {
        Some(_) => Some(json_resp(
            202,
            json!({
                "botId": bot_id,
                "botVersion": version,
                "localeId": locale_id,
                "customVocabularyStatus": "Deleting",
            }),
        )),
        None => not_found(&format!("Custom vocabulary for locale {locale_id}")),
    }
}

// ---------------------------------------------------------------------------
// Resource policy statements
// ---------------------------------------------------------------------------

/// Statement ops mutate the policy document stored by the resource-policy
/// family: a missing policy grows one from an empty statement list.
fn create_resource_policy_statement(req: &Request, arn: &str, body: &Value) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let statement_id = str_field(body, "statementId")
        .unwrap_or_default()
        .to_string();
    if statement_id.is_empty() {
        return conflict("statementId is required");
    }
    let existing = state.resource_policies.get(arn);
    let mut policy: Value = match existing.map(|rp| rp.policy.as_str()) {
        Some(raw) if !raw.is_empty() => match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(_) => return conflict("Resource policy is not a valid JSON document"),
        },
        _ => json!({"Version": "2012-10-17", "Statement": []}),
    };
    // expectedRevisionId rides the querystring like the resource-policy ops.
    if let Some(expected) = revision_param(req, body) {
        if let Some(rp) = existing {
            if expected != rp.revision_id {
                return conflict("Revision ID mismatch");
            }
        }
    }
    if !policy["Statement"].is_array() {
        policy["Statement"] = json!([]);
    }
    let statements = policy["Statement"].as_array_mut().unwrap();
    if statements
        .iter()
        .any(|s| s.get("Sid").and_then(Value::as_str) == Some(statement_id.as_str()))
    {
        return conflict(format!("Statement {statement_id} already exists").as_str());
    }
    let mut statement = serde_json::Map::new();
    statement.insert("Sid".into(), json!(statement_id));
    if let Some(effect) = str_field(body, "effect") {
        statement.insert("Effect".into(), json!(effect));
    }
    if let Some(principal) = body.get("principal").filter(|v| !v.is_null()) {
        statement.insert("Principal".into(), principal.clone());
    }
    if let Some(action) = body.get("action").filter(|v| !v.is_null()) {
        statement.insert("Action".into(), action.clone());
    }
    if let Some(condition) = body.get("condition").filter(|v| !v.is_null()) {
        statement.insert("Condition".into(), condition.clone());
    }
    statements.push(Value::Object(statement));
    let revision_id = uuid_id();
    let policy_str = serde_json::to_string(&policy).unwrap_or_default();
    state.resource_policies.insert(
        arn.to_string(),
        ResourcePolicy {
            policy: policy_str,
            revision_id: revision_id.clone(),
        },
    );
    drop(guard);
    ok(json!({"resourceArn": arn, "revisionId": revision_id}))
}

fn delete_resource_policy_statement(
    req: &Request,
    arn: &str,
    statement_id: &str,
) -> Option<Response> {
    let mut guard = SLOT.write();
    let state = guard.entry((req.account, req.region.clone())).or_default();
    let rp = match state.resource_policies.get(arn) {
        Some(rp) => rp,
        None => return not_found(&format!("Resource policy for {arn}")),
    };
    if let Some(expected) = revision_param(req, &Value::Null) {
        if expected != rp.revision_id {
            return conflict("Revision ID mismatch");
        }
    }
    let mut policy: Value = match serde_json::from_str(&rp.policy) {
        Ok(v) => v,
        Err(_) => return not_found(&format!("Statement {statement_id}")),
    };
    if !policy["Statement"].is_array() {
        return not_found(&format!("Statement {statement_id}"));
    }
    let statements = policy["Statement"].as_array_mut().unwrap();
    let before = statements.len();
    statements.retain(|s| s.get("Sid").and_then(Value::as_str) != Some(statement_id));
    if statements.len() == before {
        return not_found(&format!("Statement {statement_id}"));
    }
    let revision_id = uuid_id();
    let policy_str = serde_json::to_string(&policy).unwrap_or_default();
    state.resource_policies.insert(
        arn.to_string(),
        ResourcePolicy {
            policy: policy_str,
            revision_id: revision_id.clone(),
        },
    );
    drop(guard);
    ok(json!({"resourceArn": arn, "revisionId": revision_id}))
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

    /// Seed bot + DRAFT-version-ish locale that hierarchical families hang off.
    fn seed_locale(account: u64, locale_id: &str) -> (String, String) {
        let bot = resp_json(
            &dispatch(&req(
                "PUT",
                "/bots",
                account,
                &json!({"botName": "b", "roleArn": "arn:aws:iam::123456789012:role/r"}),
            ))
            .unwrap(),
        );
        let bot_id = bot["botId"].as_str().unwrap().to_string();
        let created_version = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}/botversions"),
                account,
                &json!({"botVersionLocaleSpecification": {"en-US": {"sourceBotVersion": "DRAFT"}}}),
            ))
            .unwrap(),
        );
        assert_eq!(created_version["botVersion"], "1");
        let version = created_version["botVersion"].as_str().unwrap().to_string();
        let locale = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}/botversions/{version}/botlocales"),
                account,
                &json!({
                    "localeId": locale_id,
                    "localeName": "English (US)",
                    "nluIntentConfidenceThreshold": 0.40,
                }),
            ))
            .unwrap(),
        );
        assert_eq!(locale["botLocaleStatus"], "Creating");
        assert_eq!(locale["nluIntentConfidenceThreshold"], json!(0.40));
        (bot_id, locale_id.to_string())
    }

    #[test]
    fn locale_family_roundtrip() {
        let account = 111111111004;
        let version_path;
        {
            let (bot_id, locale_id) = seed_locale(account, "en-US");
            version_path = format!("/bots/{bot_id}/botversions/1");

            let described = resp_json(
                &dispatch(&req(
                    "GET",
                    &format!("{version_path}/botlocales/{locale_id}"),
                    account,
                    &json!({}),
                ))
                .unwrap(),
            );
            assert_eq!(described["localeName"], "English (US)");
            assert_eq!(described["intentsCount"], json!(0));

            let built = resp_json(
                &dispatch(&req(
                    "POST",
                    &format!("{version_path}/botlocales/{locale_id}"),
                    account,
                    &json!({}),
                ))
                .unwrap(),
            );
            assert_eq!(built["botLocaleStatus"], "Building");

            let listed = resp_json(
                &dispatch(&req(
                    "POST",
                    &format!("{version_path}/botlocales"),
                    account,
                    &json!({}),
                ))
                .unwrap(),
            );
            assert_eq!(listed["botLocaleSummaries"][0]["localeId"], "en-US");

            let updated = resp_json(
                &dispatch(&req(
                    "PUT",
                    &format!("{version_path}/botlocales/{locale_id}"),
                    account,
                    &json!({"nluIntentConfidenceThreshold": 0.80}),
                ))
                .unwrap(),
            );
            assert_eq!(updated["nluIntentConfidenceThreshold"], json!(0.80));

            let deleted = resp_json(
                &dispatch(&req(
                    "DELETE",
                    &format!("{version_path}/botlocales/{locale_id}"),
                    account,
                    &json!({}),
                ))
                .unwrap(),
            );
            assert_eq!(deleted["botLocaleStatus"], "Deleting");
            let gone = dispatch(&req(
                "GET",
                &format!("{version_path}/botlocales/{locale_id}"),
                account,
                &json!({}),
            ))
            .unwrap();
            assert_eq!(gone.status, 400);
        }
        // bot version family rides the same hierarchy
        let bot_id = version_path
            .split('/')
            .nth(2)
            .unwrap_or_default()
            .to_string();
        let deleted = resp_json(
            &dispatch(&req(
                "DELETE",
                &format!("/bots/{bot_id}/botversions/1"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(deleted["botVersion"], "1");
        let gone = dispatch(&req(
            "GET",
            &format!("/bots/{bot_id}/botversions/1"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(gone.status, 400);
    }

    #[test]
    fn intent_and_slot_roundtrip() {
        let account = 111111111005;
        let (bot_id, locale_id) = seed_locale(account, "en-US");
        let base = format!("/bots/{bot_id}/botversions/1/botlocales/{locale_id}");

        let intent = resp_json(
        &dispatch(&req(
            "PUT",
            &format!("{base}/intents"),
            account,
            &json!({"intentName": "OrderFlowers", "sampleUtterances": [{"utterance": "i want flowers"}]}),
        ))
        .unwrap(),
    );
        assert_eq!(intent["intentName"], "OrderFlowers");
        let intent_id = intent["intentId"].as_str().unwrap().to_string();

        let slot = resp_json(
        &dispatch(&req(
            "PUT",
            &format!("{base}/intents/{intent_id}/slots"),
            account,
            &json!({"slotName": "FlowerType", "valueElicitationSetting": {"slotConstraint": "Required"}}),
        ))
        .unwrap(),
    );
        let slot_id = slot["slotId"].as_str().unwrap().to_string();

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("{base}/intents/{intent_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["intentName"], "OrderFlowers");
        assert_eq!(described["slotPriorities"][0]["slotId"], json!(slot_id));

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/intents"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(listed["intentSummaries"][0]["intentName"], "OrderFlowers");

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/intents/{intent_id}/slots"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(listed["slotSummaries"][0]["slotName"], "FlowerType");

        // generated utterances reference the intent name
        let generated = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/generate"),
                account,
                &json!({"intentId": intent_id}),
            ))
            .unwrap(),
        );
        assert_eq!(
            generated["sampleUtterances"][0]["utterance"],
            "Tell me about OrderFlowers"
        );

        let deleted = dispatch(&req(
            "DELETE",
            &format!("{base}/intents/{intent_id}/slots/{slot_id}"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
        let deleted = dispatch(&req(
            "DELETE",
            &format!("{base}/intents/{intent_id}"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
        assert_eq!(
            dispatch(&req(
                "GET",
                &format!("{base}/intents/{intent_id}"),
                account,
                &json!({})
            ))
            .unwrap()
            .status,
            400
        );
    }

    #[test]
    fn slot_type_family_and_builtins() {
        let account = 111111111006;
        let (bot_id, locale_id) = seed_locale(account, "en-US");
        let base = format!("/bots/{bot_id}/botversions/1/botlocales/{locale_id}");

        let created = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("{base}/slottypes"),
                account,
                &json!({"slotTypeName": "FlowerTypes"}),
            ))
            .unwrap(),
        );
        assert_eq!(created["slotTypeName"], "FlowerTypes");
        let slot_type_id = created["slotTypeId"].as_str().unwrap().to_string();

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("{base}/slottypes/{slot_type_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["slotTypeName"], "FlowerTypes");

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("{base}/slottypes/{slot_type_id}"),
                account,
                &json!({"slotTypeName": "FlowerTypes2"}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["slotTypeName"], "FlowerTypes2");

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/slottypes"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["slotTypeSummaries"][0]["slotTypeName"],
            "FlowerTypes2"
        );

        let builtins = resp_json(
            &dispatch(&req(
                "POST",
                "/builtins/locales/en-US/intents",
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        let signatures: Vec<&str> = builtins["builtInIntentSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["intentSignature"].as_str().unwrap())
            .collect();
        assert!(signatures.contains(&"AMAZON.FallbackIntent"));

        let builtins = resp_json(
            &dispatch(&req(
                "POST",
                "/builtins/locales/en-US/slottypes",
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            builtins["builtInSlotTypeSummaries"][0]["slotTypeSignature"],
            "AMAZON.AlphaNumeric"
        );

        let deleted = dispatch(&req(
            "DELETE",
            &format!("{base}/slottypes/{slot_type_id}"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
    }

    #[test]
    fn import_export_upload_roundtrip() {
        let account = 111111111007;
        let imported = resp_json(
            &dispatch(&req(
                "PUT",
                "/imports",
                account,
                &json!({
                    "importId": "import-1",
                    "mergeStrategy": "Overwrite",
                    "resourceSpecification": {"botImportSpecification": {"botName": "b"}},
                }),
            ))
            .unwrap(),
        );
        assert_eq!(imported["importStatus"], "InProgress");
        assert_eq!(imported["mergeStrategy"], "Overwrite");

        let described =
            resp_json(&dispatch(&req("GET", "/imports/import-1", account, &json!({}))).unwrap());
        assert_eq!(described["importStatus"], "InProgress");
        assert_eq!(described["failureReasons"], json!([]));

        let listed = resp_json(&dispatch(&req("POST", "/imports", account, &json!({}))).unwrap());
        assert_eq!(listed["importSummaries"][0]["importId"], "import-1");

        let deleted = dispatch(&req("DELETE", "/imports/import-1", account, &json!({}))).unwrap();
        assert_eq!(deleted.status, 202);
        assert_eq!(resp_json(&deleted)["importStatus"], "Deleting");

        let exported = resp_json(
            &dispatch(&req(
                "PUT",
                "/exports",
                account,
                &json!({
                    "fileFormat": "LexJson",
                    "resourceSpecification": {"botExportSpecification": {"botId": "b1"}},
                }),
            ))
            .unwrap(),
        );
        let export_id = exported["exportId"].as_str().unwrap().to_string();
        assert_eq!(exported["exportStatus"], "InProgress");

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/exports/{export_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["exportId"], json!(export_id));

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/exports/{export_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert!(described["downloadUrl"]
            .as_str()
            .unwrap()
            .contains(&export_id));

        let listed = resp_json(&dispatch(&req("POST", "/exports", account, &json!({}))).unwrap());
        assert_eq!(listed["exportSummaries"][0]["exportId"], json!(export_id));

        let upload =
            resp_json(&dispatch(&req("POST", "/createuploadurl", account, &json!({}))).unwrap());
        assert!(upload["uploadUrl"]
            .as_str()
            .unwrap()
            .contains(upload["importId"].as_str().unwrap()));
    }

    #[test]
    fn bot_recommendation_family() {
        let account = 111111111008;
        let (bot_id, locale_id) = seed_locale(account, "en-US");
        let base = format!("/bots/{bot_id}/botversions/1/botlocales/{locale_id}");

        let started = resp_json(
        &dispatch(&req(
            "PUT",
            &format!("{base}/botrecommendations"),
            account,
            &json!({"transcriptSourceSetting": {"S3BucketTranscriptSource": {"bucket": "probe-bucket"}}}),
        ))
        .unwrap(),
    );
        assert_eq!(started["botRecommendationStatus"], "Processing");
        let rec_id = started["botRecommendationId"].as_str().unwrap().to_string();

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("{base}/botrecommendations/{rec_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["botRecommendationStatus"], "Processing");
        assert_eq!(described["failureReasons"], json!([]));

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/botrecommendations"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["botRecommendationSummaries"][0]["botRecommendationId"],
            json!(rec_id)
        );

        let updated = resp_json(
        &dispatch(&req(
            "PUT",
            &format!("{base}/botrecommendations/{rec_id}"),
            account,
            &json!({"encryptionSetting": {"kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/k"}}),
        ))
        .unwrap(),
    );
        assert_eq!(updated["botRecommendationStatus"], "Updating");

        let stopped = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("{base}/botrecommendations/{rec_id}/stopbotrecommendation"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(stopped["botRecommendationStatus"], "Stopping");

        let intents = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/botrecommendations/{rec_id}/intents"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(intents["summaryList"], json!([]));

        let transcripts = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/botrecommendations/{rec_id}/associatedtranscripts"),
                account,
                &json!({"filters": [{"name": "Intent"}]}),
            ))
            .unwrap(),
        );
        assert_eq!(transcripts["totalResults"], json!(0));
    }

    #[test]
    fn analytics_and_utterances() {
        let account = 111111111009;
        let (bot_id, _locale_id) = seed_locale(account, "en-US");

        for metric in [
            "intentmetrics",
            "intentstagemetrics",
            "sessionmetrics",
            "utterancemetrics",
        ] {
            let listed = resp_json(
                &dispatch(&req(
                    "POST",
                    &format!("/bots/{bot_id}/analytics/{metric}"),
                    account,
                    &json!({}),
                ))
                .unwrap(),
            );
            assert_eq!(listed["results"], json!([]));
        }

        let paths = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/analytics/intentpaths"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(paths["nodeSummaries"], json!([]));

        let sessions = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/analytics/sessions"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(sessions["sessions"], json!([]));

        let utterances = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/analytics/utterances"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(utterances["utterances"], json!([]));

        let aggregated = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/aggregatedutterances"),
                account,
                &json!({"localeId": "en-US", "aggregationDuration": "OneDay"}),
            ))
            .unwrap(),
        );
        assert_eq!(aggregated["aggregatedUtterancesSummaries"], json!([]));
        assert_eq!(aggregated["aggregationDuration"], "OneDay");

        let deleted = dispatch(&req(
            "DELETE",
            &format!("/bots/{bot_id}/utterances"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
    }

    #[test]
    fn test_set_family_roundtrip() {
        let account = 111111111010;
        let generation = resp_json(
            &dispatch(&req(
                "PUT",
                "/testsetgenerations",
                account,
                &json!({
                    "testSetName": "set",
                    "roleArn": "arn:aws:iam::123456789012:role/r",
                    "storageLocation": {"s3BucketName": "bucket", "s3Path": "path"},
                    "generationDataSource": {"conversationLogsDataSource": {}},
                }),
            ))
            .unwrap(),
        );
        let gen_id = generation["testSetGenerationId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(generation["testSetGenerationStatus"], "Generating");

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/testsetgenerations/{gen_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        let test_set_id = described["testSetId"].as_str().unwrap().to_string();
        assert_eq!(described["failureReasons"], json!([]));

        let listed = resp_json(&dispatch(&req("POST", "/testsets", account, &json!({}))).unwrap());
        assert_eq!(listed["testSets"][0]["testSetId"], json!(test_set_id));

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/testsets/{test_set_id}"),
                account,
                &json!({"testSetName": "set2"}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["testSetName"], "set2");

        let records = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/testsets/{test_set_id}/records"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(records["testSetRecords"], json!([]));

        let executed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/testsets/{test_set_id}/testexecutions"),
                account,
                &json!({"target": {"botAliasTarget": {}}, "apiMode": "Streaming"}),
            ))
            .unwrap(),
        );
        let exec_id = executed["testExecutionId"].as_str().unwrap().to_string();
        assert_eq!(executed["testExecutionModality"], "Text");

        let listed =
            resp_json(&dispatch(&req("POST", "/testexecutions", account, &json!({}))).unwrap());
        assert_eq!(
            listed["testExecutions"][0]["testExecutionId"],
            json!(exec_id)
        );

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/testexecutions/{exec_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["testExecutionStatus"], "Pending");
        assert_eq!(described["failureReasons"], json!([]));

        let results = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/testexecutions/{exec_id}/results"),
                account,
                &json!({"resultFilterBy": {"endResultFilter": "Complete"}}),
            ))
            .unwrap(),
        );
        assert_eq!(results["testExecutionResults"], json!([]));

        let artifacts = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/testexecutions/{exec_id}/artifacturl"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert!(artifacts["downloadArtifactsUrl"]
            .as_str()
            .unwrap()
            .contains(&exec_id));

        let report = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/testsets/{test_set_id}/testsetdiscrepancy"),
                account,
                &json!({"target": {"botAliasTarget": {}}}),
            ))
            .unwrap(),
        );
        let report_id = report["testSetDiscrepancyReportId"]
            .as_str()
            .unwrap()
            .to_string();
        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/testsetdiscrepancy/{report_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["testSetDiscrepancyReportStatus"], "InProgress");

        let deleted = dispatch(&req(
            "DELETE",
            &format!("/testsets/{test_set_id}"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
        assert_eq!(
            dispatch(&req(
                "GET",
                &format!("/testsets/{test_set_id}"),
                account,
                &json!({})
            ))
            .unwrap()
            .status,
            400
        );
    }

    #[test]
    fn replica_family_roundtrip() {
        let account = 111111111011;
        let created =
            resp_json(&dispatch(&req("PUT", "/bots", account, &json!({"botName": "b"}))).unwrap());
        let bot_id = created["botId"].as_str().unwrap().to_string();

        let replica = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}/replicas"),
                account,
                &json!({"replicaRegion": "eu-west-1"}),
            ))
            .unwrap(),
        );
        assert_eq!(replica["botReplicaStatus"], "Enabling");
        assert_eq!(replica["sourceRegion"], "us-east-1");

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/bots/{bot_id}/replicas/eu-west-1"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["replicaRegion"], "eu-west-1");

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/replicas"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["botReplicaSummaries"][0]["botReplicaStatus"],
            "Enabling"
        );

        let aliases = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/replicas/eu-west-1/botaliases"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(aliases["botAliasReplicaSummaries"], json!([]));

        let versions = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/replicas/eu-west-1/botversions"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(versions["botVersionReplicaSummaries"], json!([]));

        let deleted = dispatch(&req(
            "DELETE",
            &format!("/bots/{bot_id}/replicas/eu-west-1"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 202);
        assert_eq!(resp_json(&deleted)["botReplicaStatus"], "Deleting");
    }

    #[test]
    fn resource_generation_and_analyzer() {
        let account = 111111111012;
        let (bot_id, locale_id) = seed_locale(account, "en-US");
        let base = format!("/bots/{bot_id}/botversions/1/botlocales/{locale_id}");

        let started = resp_json(
        &dispatch(&req(
            "PUT",
            &format!("{base}/startgeneration"),
            account,
            &json!({"generationInputPrompt": "build a pickup-and-deliver bot that handles standard bot intents"}),
        ))
        .unwrap(),
    );
        let gen_id = started["generationId"].as_str().unwrap().to_string();
        assert_eq!(started["generationStatus"], "Processing");
        assert!(started["modelArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:lex:us-east-1:"));

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("{base}/generations/{gen_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["generationStatus"], "Processing");

        let listed = resp_json(
            &dispatch(&req(
                "POST",
                &format!("{base}/generations"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["generationSummaries"][0]["generationId"],
            json!(gen_id)
        );

        // analyzer lifecycle
        let started = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/botanalyzer"),
                account,
                &json!({"analysisScope": "BotLocale", "localeId": locale_id, "botVersion": "1"}),
            ))
            .unwrap(),
        );
        let request_id = started["botAnalyzerRequestId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(started["botAnalyzerStatus"], "Processing");

        let history = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/botanalyzer/history"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(
            history["botAnalyzerHistoryList"][0]["botAnalyzerStatus"],
            "Processing"
        );

        let described = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/botanalyzer/describe/{request_id}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(described["botAnalyzerStatus"], "Processing");

        let stopped = resp_json(
            &dispatch(&req(
                "PUT",
                &format!("/bots/{bot_id}/botanalyzer/{request_id}/stop"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(stopped["botAnalyzerStatus"], "Stopping");

        let deleted = dispatch(&req(
            "DELETE",
            &format!("/bots/{bot_id}/botanalyzer/{request_id}"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(deleted.status, 204);
        assert_eq!(
            dispatch(&req(
                "POST",
                &format!("/bots/{bot_id}/botanalyzer/describe/{request_id}"),
                account,
                &json!({}),
            ))
            .unwrap()
            .status,
            400
        );
    }

    #[test]
    fn policy_statement_roundtrip() {
        let account = 111111111013;
        let created =
            resp_json(&dispatch(&req("PUT", "/bots", account, &json!({"botName": "b"}))).unwrap());
        let bot_id = created["botId"].as_str().unwrap().to_string();
        let arn = format!("arn:aws:lex:us-east-1:{account}:bot/{bot_id}");
        let arn_enc = enc(&arn);

        let added = resp_json(
            &dispatch(&req(
                "POST",
                &format!("/policy/{arn_enc}/statements"),
                account,
                &json!({
                    "statementId": "s1",
                    "effect": "Allow",
                    "principal": ["arn:aws:iam::123456789012:role/others"],
                    "action": ["lex:*"],
                }),
            ))
            .unwrap(),
        );
        let rev = added["revisionId"].as_str().unwrap().to_string();
        assert_eq!(added["resourceArn"], json!(arn));

        // duplicate statement id conflicts
        let conflict = dispatch(&req(
            "POST",
            &format!("/policy/{arn_enc}/statements"),
            account,
            &json!({"statementId": "s1", "effect": "Allow", "principal": [], "action": []}),
        ))
        .unwrap();
        assert_eq!(conflict.status, 400);

        let described = resp_json(
            &dispatch(&req(
                "GET",
                &format!("/policy/{arn_enc}"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        let policy: Value = serde_json::from_str(described["policy"].as_str().unwrap()).unwrap();
        assert_eq!(policy["Statement"][0]["Sid"], "s1");
        assert_eq!(policy["Statement"][0]["Effect"], "Allow");

        // a bogus revision conflicts
        let conflict = dispatch(&req(
            "DELETE",
            &format!("/policy/{arn_enc}/statements/s1?expectedRevisionId=bogus"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(conflict.status, 400);

        let deleted = resp_json(
            &dispatch(&req(
                "DELETE",
                &format!("/policy/{arn_enc}/statements/s1"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(deleted["resourceArn"], json!(arn));
        // statement gone now
        let missing = dispatch(&req(
            "DELETE",
            &format!("/policy/{arn_enc}/statements/s1"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(missing.status, 400);
        let _ = rev;
    }

    #[test]
    fn custom_vocabulary_roundtrip() {
        let account = 111111111014;
        let (bot_id, locale_id) = seed_locale(account, "en-US");
        let base = format!("/bots/{bot_id}/botversions/1/botlocales/{locale_id}");
        let list = &format!("{base}/customvocabulary/DEFAULT/list");
        let batch_create = &format!("{base}/customvocabulary/DEFAULT/batchcreate");
        let batch_update = &format!("{base}/customvocabulary/DEFAULT/batchupdate");
        let batch_delete = &format!("{base}/customvocabulary/DEFAULT/batchdelete");

        // before any batch call the vocabulary is absent
        assert_eq!(
            dispatch(&req(
                "GET",
                &format!("{base}/customvocabulary/DEFAULT/metadata"),
                account,
                &json!({}),
            ))
            .unwrap()
            .status,
            400
        );

        let added = resp_json(
            &dispatch(&req(
                "PUT",
                batch_create,
                account,
                &json!({"customVocabularyItemList": [
                    {"itemId": "i1", "phrase": "flowers", "weight": 1},
                ]}),
            ))
            .unwrap(),
        );
        assert_eq!(added["errors"], json!([]));
        assert_eq!(added["customVocabularyStatus"], "Ready");

        let metadata = resp_json(
            &dispatch(&req(
                "GET",
                &format!("{base}/customvocabulary/DEFAULT/metadata"),
                account,
                &json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(metadata["customVocabularyStatus"], "Ready");

        let listed = resp_json(&dispatch(&req("POST", list, account, &json!({}))).unwrap());
        assert_eq!(listed["customVocabularyItems"][0]["phrase"], "flowers");

        let updated = resp_json(
            &dispatch(&req(
                "PUT",
                batch_update,
                account,
                &json!({"customVocabularyItemList": [
                    {"itemId": "i1", "phrase": "roses", "weight": 2},
                ]}),
            ))
            .unwrap(),
        );
        assert_eq!(updated["resources"][0]["phrase"], "roses");

        let dup = resp_json(
            &dispatch(&req(
                "PUT",
                batch_create,
                account,
                &json!({"customVocabularyItemList": [
                    {"itemId": "i1", "phrase": "roses"},
                ]}),
            ))
            .unwrap(),
        );
        assert_eq!(dup["errors"][0]["errorMessage"], "item already exists");

        let removed = resp_json(
            &dispatch(&req(
                "POST",
                batch_delete,
                account,
                &json!({"customVocabularyItemList": [
                    {"itemId": "i1"},
                ]}),
            ))
            .unwrap(),
        );
        assert_eq!(removed["resources"][0]["itemId"], "i1");

        let gone = dispatch(&req(
            "DELETE",
            &format!("{base}/customvocabulary"),
            account,
            &json!({}),
        ))
        .unwrap();
        assert_eq!(gone.status, 202);
        assert_eq!(resp_json(&gone)["customVocabularyStatus"], "Deleting");
    }
}
