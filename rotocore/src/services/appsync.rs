//! AppSync backend (REST-JSON, `/v1/apis/...` and `/v2/apis/...`).
//!
//! Port of moto's `AppSyncBackend` per-(account, region): GraphQL API CRUD,
//! API keys, data sources, types + resolvers + functions, schema creation,
//! API caches, domain names + api associations, Event APIs (v2) with channel
//! namespaces, merged/source api associations, environment variables, data
//! source introspection and the evaluate-mapping/code dataplane mocks.
//!
//! Wire notes (pinned by tests/compatibility/test_appsync_compat.py):
//! - Update ops are POST; deletes are DELETE; single-resource GET bodies are
//!   the payload members as-is.
//! - `StartSchemaCreation` decodes the base64 definition and parses type
//!   names for `GetType`/`ListTypes`; moto raises NotFoundException for
//!   unknown resources.
//! - `GetIntrospectionSchema` returns the SDL definition verbatim as a
//!   streaming body (moto runs a real GraphQL parser; rotocore echoes the
//!   stored definition).
//! - `EvaluateMappingTemplate`/`EvaluateCode` answer `{"evaluationResult": ..}`.

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

fn not_found(resource: &str) -> Response {
    // botocore maps this code onto client.exceptions.NotFoundException.
    json_resp(
        404,
        &json!({ "__type": "NotFoundException", "message": format!("{resource} not found") }),
    )
}

fn validation(message: &str) -> Response {
    json_resp(
        400,
        &json!({ "__type": "ValidationException", "message": message }),
    )
}

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn iso_ts() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct ApiKey {
    id: String,
    description: Option<String>,
    expires: f64,
}

fn api_key_json(key: &ApiKey) -> Value {
    json!({
        "id": key.id,
        "description": key.description,
        "expires": key.expires,
        "deletes": key.expires,
    })
}

#[derive(Clone, Default)]
struct DataSource {
    name: String,
    data_source_arn: String,
    type_: String,
    description: Option<String>,
    service_role_arn: Option<String>,
    dynamodb_config: Value,
    lambda_config: Value,
    elasticsearch_config: Value,
    open_search_service_config: Value,
    http_config: Value,
    relational_database_config: Value,
    event_bridge_config: Value,
    metrics_config: String,
}

impl DataSource {
    fn to_json(&self) -> Value {
        let mut result = json!({
            "dataSourceArn": self.data_source_arn,
            "name": self.name,
            "type": self.type_,
            "metricsConfig": self.metrics_config,
        });
        let obj = result.as_object_mut().expect("ds obj");
        let optional_strings = [
            ("description", self.description.clone()),
            ("serviceRoleArn", self.service_role_arn.clone()),
        ];
        for (key, value) in optional_strings {
            if let Some(v) = value {
                obj.insert(key.to_string(), json!(v));
            }
        }
        let optional_values = [
            ("dynamodbConfig", &self.dynamodb_config),
            ("lambdaConfig", &self.lambda_config),
            ("elasticsearchConfig", &self.elasticsearch_config),
            ("openSearchServiceConfig", &self.open_search_service_config),
            ("httpConfig", &self.http_config),
            ("relationalDatabaseConfig", &self.relational_database_config),
            ("eventBridgeConfig", &self.event_bridge_config),
        ];
        for (key, value) in optional_values {
            if !value.is_null() {
                obj.insert(key.to_string(), value.clone());
            }
        }
        result
    }
}

#[derive(Clone, Default)]
struct GraphQLApi {
    api_id: String,
    api_arn: String,
    name: String,
    authentication_type: String,
    additional_authentication_providers: Value,
    lambda_authorizer_config: Value,
    log_config: Value,
    open_id_connect_config: Value,
    user_pool_config: Value,
    xray_enabled: bool,
    visibility: String,
    tags: BTreeMap<String, Option<String>>,
    api_keys: BTreeMap<String, ApiKey>,
    data_sources: BTreeMap<String, DataSource>,
    custom_types: BTreeMap<String, AppSyncType>,
    /// (typeName, fieldName) → resolver
    resolvers: BTreeMap<(String, String), Resolver>,
    functions: BTreeMap<String, GraphqlFunction>,
    api_cache: Option<ApiCache>,
    environment_variables: BTreeMap<String, String>,
    /// raw SDL definition (already base64-decoded), plus types parsed from it
    schema_definition: Option<String>,
    schema_types: Vec<String>,
    schema_status: String,
    schema_details: Option<String>,
}

#[derive(Clone, Default)]
struct AppSyncType {
    name: String,
    arn: String,
    definition: String,
    format: String,
}

fn appsync_type_json(t: &AppSyncType) -> Value {
    json!({
        "name": t.name,
        "arn": t.arn,
        "definition": t.definition,
        "format": t.format,
    })
}

#[derive(Clone, Default)]
struct Resolver {
    type_name: String,
    field_name: String,
    resolver_arn: String,
    data_source_name: Option<String>,
    request_mapping_template: Option<String>,
    response_mapping_template: Option<String>,
    kind: String,
    pipeline_config: Value,
    sync_config: Value,
    caching_config: Value,
    max_batch_size: i64,
    runtime: Value,
    code: Option<String>,
    metrics_config: String,
}

fn resolver_json(r: &Resolver) -> Value {
    let mut result = json!({
        "typeName": r.type_name,
        "fieldName": r.field_name,
        "resolverArn": r.resolver_arn,
        "kind": r.kind,
        "maxBatchSize": r.max_batch_size,
        "metricsConfig": r.metrics_config,
    });
    let obj = result.as_object_mut().expect("resolver obj");
    let optional = [
        (
            "dataSourceName",
            &r.data_source_name.clone().map(Value::String),
        ),
        (
            "requestMappingTemplate",
            &r.request_mapping_template.clone().map(Value::String),
        ),
        (
            "responseMappingTemplate",
            &r.response_mapping_template.clone().map(Value::String),
        ),
        ("code", &r.code.clone().map(Value::String)),
    ];
    for (key, value) in optional {
        if let Some(v) = value {
            obj.insert(key.to_string(), v.clone());
        }
    }
    for (key, value) in [
        ("pipelineConfig", &r.pipeline_config),
        ("syncConfig", &r.sync_config),
        ("cachingConfig", &r.caching_config),
        ("runtime", &r.runtime),
    ] {
        if !value.is_null() {
            obj.insert(key.to_string(), value.clone());
        }
    }
    result
}

#[derive(Clone, Default)]
struct GraphqlFunction {
    function_id: String,
    function_arn: String,
    name: String,
    data_source_name: Option<String>,
    description: Option<String>,
    request_mapping_template: Option<String>,
    response_mapping_template: Option<String>,
    function_version: String,
    sync_config: Value,
    max_batch_size: i64,
    runtime: Value,
    code: Option<String>,
}

fn function_json(f: &GraphqlFunction) -> Value {
    let mut result = json!({
        "functionId": f.function_id,
        "functionArn": f.function_arn,
        "name": f.name,
        "dataSourceName": f.data_source_name,
        "functionVersion": f.function_version,
        "maxBatchSize": f.max_batch_size,
    });
    let obj = result.as_object_mut().expect("function obj");
    for (key, value) in [
        ("description", &f.description.clone().map(Value::String)),
        (
            "requestMappingTemplate",
            &f.request_mapping_template.clone().map(Value::String),
        ),
        (
            "responseMappingTemplate",
            &f.response_mapping_template.clone().map(Value::String),
        ),
        ("code", &f.code.clone().map(Value::String)),
    ] {
        if let Some(v) = value.clone() {
            obj.insert(key.to_string(), v);
        }
    }
    if !f.sync_config.is_null() {
        obj.insert("syncConfig".to_string(), f.sync_config.clone());
    }
    if !f.runtime.is_null() {
        obj.insert("runtime".to_string(), f.runtime.clone());
    }
    result
}

#[derive(Clone, Default)]
struct ApiCache {
    ttl: i64,
    api_caching_behavior: String,
    type_: String,
}

impl ApiCache {
    fn to_json(&self) -> Value {
        json!({
            "ttl": self.ttl,
            "transitEncryptionEnabled": false,
            "atRestEncryptionEnabled": false,
            "apiCachingBehavior": self.api_caching_behavior,
            "type": self.type_,
            "status": "AVAILABLE",
        })
    }
}

#[derive(Clone, Default)]
struct ChannelNamespace {
    api_id: String,
    name: String,
    channel_namespace_arn: String,
    subscribe_auth_modes: Value,
    publish_auth_modes: Value,
    code_handlers: Value,
    handler_configs: Value,
    created: String,
    last_modified: String,
}

fn channel_namespace_json(c: &ChannelNamespace, tags: &Value) -> Value {
    let mut result = json!({
        "apiId": c.api_id,
        "name": c.name,
        "subscribeAuthModes": c.subscribe_auth_modes,
        "publishAuthModes": c.publish_auth_modes,
        "channelNamespaceArn": c.channel_namespace_arn,
        "created": c.created,
        "lastModified": c.last_modified,
        "handlerConfigs": c.handler_configs,
    });
    if !c.code_handlers.is_null() {
        result
            .as_object_mut()
            .expect("cn obj")
            .insert("codeHandlers".to_string(), c.code_handlers.clone());
    }
    result
        .as_object_mut()
        .expect("cn obj")
        .insert("tags".to_string(), tags.clone());
    result
}

#[derive(Clone, Default)]
struct EventsApi {
    api_id: String,
    api_arn: String,
    name: String,
    owner_contact: Option<String>,
    event_config: Value,
    created: String,
    tags: BTreeMap<String, Option<String>>,
    channel_namespaces: BTreeMap<String, ChannelNamespace>,
}

#[derive(Clone, Default)]
struct DomainNameConfig {
    domain_name: String,
    description: String,
    certificate_arn: String,
    appsync_domain_name: String,
    hosted_zone_id: String,
    arn: String,
}

fn domain_name_json(d: &DomainNameConfig) -> Value {
    json!({
        "domainName": d.domain_name,
        "description": d.description,
        "certificateArn": d.certificate_arn,
        "appsyncDomainName": d.appsync_domain_name,
        "hostedZoneId": d.hosted_zone_id,
        "domainNameArn": d.arn,
    })
}

#[derive(Default)]
pub struct AppSyncState {
    graphql_apis: BTreeMap<String, GraphQLApi>,
    events_apis: BTreeMap<String, EventsApi>,
    domain_names: BTreeMap<String, DomainNameConfig>,
    api_associations: BTreeMap<String, Value>,
    /// arn → tag map
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut AppSyncState) -> T) -> T {
    let slot = store::slot::<AppSyncState>("appsync");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

/// Parse type names out of an SDL definition (`type Foo { ... }` lines).
fn extract_type_names(definition: &str) -> Vec<String> {
    let mut names = Vec::new();
    let tokens = definition.split_whitespace();
    let mut previous = String::new();
    for token in tokens {
        if previous == "type" || previous == "input" {
            let name = token.trim_start_matches('{').trim_end_matches('{');
            if !name.is_empty() {
                names.push(name.to_string());
            }
        }
        previous = token.to_string();
    }
    names
}

// ---------------------------------------------------------------------------
// Tag helpers (ListTagsForResource returns a key→value dict per moto)
// ---------------------------------------------------------------------------

/// For our GraphQL API payloads (moto embeds `tags` inline).
fn inline_tags_json(tags: &BTreeMap<String, Option<String>>) -> Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in tags {
        obj.insert(
            k.clone(),
            v.clone().map(Value::String).unwrap_or(Value::Null),
        );
    }
    Value::Object(obj)
}

fn apply_inline_tags(tags: &mut BTreeMap<String, Option<String>>, body_tags: Option<&Value>) {
    if let Some(Value::Object(map)) = body_tags {
        for (k, v) in map {
            tags.insert(k.clone(), v.as_str().map(String::from));
        }
    }
}

/// global tag store via the same tag-ARN mechanism the other services use.
fn tags_arn_lookup(state: &AppSyncState, arn: &str) -> Value {
    let mut combined = serde_json::Map::new();
    if let Some(api) = state
        .graphql_apis
        .values()
        .find(|api| api.api_arn == arn)
        .cloned()
    {
        for (k, v) in &api.tags {
            combined.insert(
                k.clone(),
                v.clone().map(Value::String).unwrap_or(Value::Null),
            );
        }
    }
    if let Some(map) = state.tags.get(arn) {
        for (k, v) in map {
            combined.insert(
                k.clone(),
                v.clone().map(Value::String).unwrap_or(Value::Null),
            );
        }
    }
    Value::Object(combined)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn req_tags(
    state: &mut AppSyncState,
    body: &Value,
    arn: &str,
    method: &str,
    query: &str,
) -> Response {
    match method {
        "GET" => ok_json(&json!({ "tags": tags_arn_lookup(state, arn) })),
        "POST" => {
            if let Some(Value::Object(map)) = body.get("tags") {
                let entry = state.tags.entry(arn.to_string()).or_default();
                for (k, v) in map {
                    entry.insert(k.clone(), v.as_str().map(String::from));
                }
            }
            // also mirror onto the API's inline tag map when present
            for api in state.graphql_apis.values_mut() {
                if api.api_arn == arn {
                    apply_inline_tags(&mut api.tags, body.get("tags"));
                }
            }
            ok_json(&json!({}))
        }
        "DELETE" => {
            // botocore encodes UntagResource.tagKeys as repeated query params.
            let keys = query_tag_keys(query);
            if let Some(entry) = state.tags.get_mut(arn) {
                for key in keys.iter() {
                    entry.remove(key);
                }
            }
            for api in state.graphql_apis.values_mut() {
                if api.api_arn == arn {
                    for key in keys.iter() {
                        api.tags.remove(key);
                    }
                }
            }
            ok_json(&json!({}))
        }
        _ => not_found("appsync tags"),
    }
}

// --- graphql api CRUD -------------------------------------------------------

fn create_graphql_api(
    state: &mut AppSyncState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let api_id = uuid();
    let arn = format!("arn:aws:appsync:{region}:{account}:apis/{api_id}");
    let api = GraphQLApi {
        api_id: api_id.clone(),
        api_arn: arn.clone(),
        name,
        authentication_type: body
            .get("authenticationType")
            .and_then(Value::as_str)
            .unwrap_or("API_KEY")
            .to_string(),
        additional_authentication_providers: body
            .get("additionalAuthenticationProviders")
            .cloned()
            .unwrap_or(Value::Null),
        lambda_authorizer_config: body
            .get("lambdaAuthorizerConfig")
            .cloned()
            .unwrap_or(Value::Null),
        log_config: body.get("logConfig").cloned().unwrap_or(Value::Null),
        open_id_connect_config: body
            .get("openIDConnectConfig")
            .cloned()
            .unwrap_or(Value::Null),
        user_pool_config: body.get("userPoolConfig").cloned().unwrap_or(Value::Null),
        xray_enabled: body
            .get("xrayEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        visibility: body
            .get("visibility")
            .and_then(Value::as_str)
            .unwrap_or("GLOBAL")
            .to_string(),
        ..GraphQLApi::default()
    };
    state.graphql_apis.insert(api_id, api.clone());
    ok_json(&json!({ "graphqlApi": graphql_json_inline_tags(&api) }))
}

fn graphql_api_core_json(api: &GraphQLApi) -> Value {
    json!({
        "name": api.name,
        "apiId": api.api_id,
        "authenticationType": api.authentication_type,
        "arn": api.api_arn,
        "uris": { "GRAPHQL": "http://graphql.uri" },
        "additionalAuthenticationProviders": api.additional_authentication_providers,
        "lambdaAuthorizerConfig": api.lambda_authorizer_config,
        "logConfig": api.log_config,
        "openIDConnectConfig": api.open_id_connect_config,
        "userPoolConfig": api.user_pool_config,
        "xrayEnabled": api.xray_enabled,
        "visibility": api.visibility,
    })
}

fn graphql_json_inline_tags(api: &GraphQLApi) -> Value {
    let mut view = graphql_api_core_json(api);
    view.as_object_mut()
        .expect("api obj")
        .insert("tags".to_string(), inline_tags_json(&api.tags));
    view
}

fn get_graphql_api(state: &mut AppSyncState, api_id: &str) -> Response {
    match state.graphql_apis.get(api_id) {
        Some(api) => ok_json(&json!({ "graphqlApi": graphql_json_inline_tags(api) })),
        None => not_found("GraphQL API"),
    }
}

fn update_graphql_api(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    if let Some(name) = body.get("name").and_then(Value::as_str) {
        api.name = name.to_string();
    }
    if let Some(auth) = body.get("authenticationType").and_then(Value::as_str) {
        api.authentication_type = auth.to_string();
    }
    if let Some(providers) = body.get("additionalAuthenticationProviders") {
        api.additional_authentication_providers = providers.clone();
    }
    if let Some(config) = body.get("lambdaAuthorizerConfig") {
        api.lambda_authorizer_config = config.clone();
    }
    if let Some(config) = body.get("logConfig") {
        api.log_config = config.clone();
    }
    if let Some(config) = body.get("openIDConnectConfig") {
        api.open_id_connect_config = config.clone();
    }
    if let Some(config) = body.get("userPoolConfig") {
        api.user_pool_config = config.clone();
    }
    api.xray_enabled = body
        .get("xrayEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    ok_json(&json!({ "graphqlApi": graphql_api_core_json(api) }))
}

fn delete_graphql_api(state: &mut AppSyncState, api_id: &str) -> Response {
    if state.graphql_apis.remove(api_id).is_none() {
        return not_found("GraphQL API");
    }
    ok_json(&json!({}))
}

fn list_graphql_apis(state: &mut AppSyncState) -> Response {
    let apis: Vec<Value> = state
        .graphql_apis
        .values()
        .map(graphql_json_inline_tags)
        .collect();
    ok_json(&json!({ "graphqlApis": apis }))
}

// --- api keys ---------------------------------------------------------------

fn create_api_key(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let key_id = uuid()[..6].to_string();
    let expires = body
        .get("expires")
        .and_then(Value::as_f64)
        .unwrap_or_else(|| {
            // moto default: 7 days from now, truncated to the hour
            let now = chrono::Utc::now().timestamp() as f64;
            let seven_days = now + 7.0 * 86400.0;
            (seven_days / 3600.0).floor() * 3600.0
        });
    if (expires as i64) < chrono::Utc::now().timestamp() + 86400 && body.get("expires").is_some() {
        return json_resp(
            400,
            &json!({
                "__type": "ApiKeyValidityOutOfBoundsException",
                "message": "API key must be valid for a minimum of 1 days.",
            }),
        );
    }
    let key = ApiKey {
        id: key_id,
        description: body
            .get("description")
            .and_then(Value::as_str)
            .map(String::from),
        expires,
    };
    api.api_keys.insert(key.id.clone(), key.clone());
    ok_json(&json!({ "apiKey": api_key_json(&key) }))
}

fn delete_api_key(state: &mut AppSyncState, api_id: &str, key_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.api_keys.remove(key_id);
    ok_json(&json!({}))
}

fn list_api_keys(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let keys: Vec<Value> = api.api_keys.values().map(api_key_json).collect();
    ok_json(&json!({ "apiKeys": keys }))
}

fn update_api_key(state: &mut AppSyncState, api_id: &str, key_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(key) = api.api_keys.get_mut(key_id) else {
        return not_found("API key");
    };
    if let Some(description) = body.get("description").and_then(Value::as_str) {
        key.description = Some(description.to_string());
    }
    if let Some(expires) = body.get("expires").and_then(Value::as_f64) {
        key.expires = expires;
    }
    ok_json(&json!({ "apiKey": api_key_json(key) }))
}

// --- schema creation / types -------------------------------------------------

fn start_schema_creation(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(definition_b64) = body.get("definition").and_then(Value::as_str) else {
        return validation("Missing definition");
    };
    decode_b64(definition_b64).map_or_else(
        || validation("Invalid definition encoding"),
        |bytes| {
            let definition = String::from_utf8_lossy(&bytes).to_string();
            let types = extract_type_names(&definition);
            api.schema_definition = Some(definition);
            api.schema_types = types;
            api.schema_status = "SUCCESS".to_string();
            api.schema_details = None;
            ok_json(&json!({ "status": "PROCESSING" }))
        },
    )
}

fn get_schema_creation_status(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    ok_json(&json!({
        "status": api.schema_status.clone(),
        "details": api.schema_details.clone(),
    }))
}

#[allow(dead_code)]
fn looks_like_type_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn get_type(state: &mut AppSyncState, api_id: &str, type_name: &str, format: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    if let Some(t) = api.custom_types.get(type_name) {
        return ok_json(&json!({ "type": appsync_type_json(t) }));
    }
    if api.schema_types.iter().any(|n| n == type_name) {
        return ok_json(&json!({
            "type": {
                "name": type_name,
                "description": Value::Null,
                "arn": format!("arn:aws:appsync:graphql_type/{}", type_name),
                "definition": "NotYetImplemented",
                "format": format,
            }
        }));
    }
    not_found("Type")
}

fn create_type(
    state: &mut AppSyncState,
    api_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let definition = body
        .get("definition")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let format = body
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("SDL")
        .to_string();
    let name = definition
        .split_whitespace()
        .nth(1)
        .map(|s| s.trim_matches('{').trim().to_string())
        .unwrap_or_else(|| "Unknown".to_string());
    let t = AppSyncType {
        name: name.clone(),
        arn: format!("arn:aws:appsync:{region}:{account}:apis/{api_id}/types/{name}"),
        definition: definition.clone(),
        format,
    };
    api.custom_types.insert(name, t.clone());
    ok_json(&json!({ "type": appsync_type_json(&t) }))
}

fn update_type(state: &mut AppSyncState, api_id: &str, type_name: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(t) = api.custom_types.get_mut(type_name) else {
        return not_found("Type");
    };
    if let Some(definition) = body.get("definition").and_then(Value::as_str) {
        t.definition = definition.to_string();
        t.name = definition
            .split_whitespace()
            .nth(1)
            .map(|s| s.trim_matches('{').trim().to_string())
            .unwrap_or_else(|| t.name.clone());
    }
    if let Some(format) = body.get("format").and_then(Value::as_str) {
        t.format = format.to_string();
    }
    ok_json(&json!({ "type": appsync_type_json(t) }))
}

fn delete_type(state: &mut AppSyncState, api_id: &str, type_name: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.custom_types.remove(type_name);
    ok_json(&json!({}))
}

fn list_types(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let types: Vec<Value> = api.custom_types.values().map(appsync_type_json).collect();
    ok_json(&json!({ "types": types }))
}

// --- data sources -------------------------------------------------------------

fn create_data_source(
    state: &mut AppSyncState,
    api_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let ds = DataSource {
        name: name.clone(),
        data_source_arn: format!(
            "arn:aws:appsync:{region}:{account}:apis/{api_id}/datasources/{name}"
        ),
        type_: body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        description: body
            .get("description")
            .and_then(Value::as_str)
            .map(String::from),
        service_role_arn: body
            .get("serviceRoleArn")
            .and_then(Value::as_str)
            .map(String::from),
        dynamodb_config: body.get("dynamodbConfig").cloned().unwrap_or(Value::Null),
        lambda_config: body.get("lambdaConfig").cloned().unwrap_or(Value::Null),
        elasticsearch_config: body
            .get("elasticsearchConfig")
            .cloned()
            .unwrap_or(Value::Null),
        open_search_service_config: body
            .get("openSearchServiceConfig")
            .cloned()
            .unwrap_or(Value::Null),
        http_config: body.get("httpConfig").cloned().unwrap_or(Value::Null),
        relational_database_config: body
            .get("relationalDatabaseConfig")
            .cloned()
            .unwrap_or(Value::Null),
        event_bridge_config: body
            .get("eventBridgeConfig")
            .cloned()
            .unwrap_or(Value::Null),
        metrics_config: body
            .get("metricsConfig")
            .and_then(Value::as_str)
            .unwrap_or("DISABLED")
            .to_string(),
    };
    api.data_sources.insert(name, ds.clone());
    ok_json(&json!({ "dataSource": ds.to_json() }))
}

fn get_data_source(state: &mut AppSyncState, api_id: &str, name: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    match api.data_sources.get(name) {
        Some(ds) => ok_json(&json!({ "dataSource": ds.to_json() })),
        None => not_found("Data source"),
    }
}

fn update_data_source(
    state: &mut AppSyncState,
    api_id: &str,
    name: &str,
    body: &Value,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(ds) = api.data_sources.get_mut(name) else {
        return not_found("Data source");
    };
    if let Some(t) = body.get("type").and_then(Value::as_str) {
        ds.type_ = t.to_string();
    }
    if let Some(d) = body.get("description").and_then(Value::as_str) {
        ds.description = Some(d.to_string());
    }
    if let Some(c) = body.get("httpConfig") {
        ds.http_config = c.clone();
    }
    if let Some(c) = body.get("dynamodbConfig") {
        ds.dynamodb_config = c.clone();
    }
    if let Some(c) = body.get("lambdaConfig") {
        ds.lambda_config = c.clone();
    }
    ok_json(&json!({ "dataSource": ds.to_json() }))
}

fn delete_data_source(state: &mut AppSyncState, api_id: &str, name: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.data_sources.remove(name);
    ok_json(&json!({}))
}

fn list_data_sources(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let sources: Vec<Value> = api.data_sources.values().map(DataSource::to_json).collect();
    ok_json(&json!({ "dataSources": sources }))
}

// --- resolvers / functions -----------------------------------------------------

fn create_resolver(
    state: &mut AppSyncState,
    api_id: &str,
    type_name: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let field_name = body
        .get("fieldName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let resolver = Resolver {
        type_name: type_name.to_string(),
        field_name: field_name.clone(),
        resolver_arn: format!(
            "arn:aws:appsync:{region}:{account}:apis/{api_id}/types/{type_name}/resolvers/{field_name}"
        ),
        data_source_name: body.get("dataSourceName").and_then(Value::as_str).map(String::from),
        request_mapping_template: body
            .get("requestMappingTemplate")
            .and_then(Value::as_str)
            .map(String::from),
        response_mapping_template: body
            .get("responseMappingTemplate")
            .and_then(Value::as_str)
            .map(String::from),
        kind: body
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("UNIT")
            .to_string(),
        pipeline_config: body.get("pipelineConfig").cloned().unwrap_or(Value::Null),
        sync_config: body.get("syncConfig").cloned().unwrap_or(Value::Null),
        caching_config: body.get("cachingConfig").cloned().unwrap_or(Value::Null),
        max_batch_size: body.get("maxBatchSize").and_then(Value::as_i64).unwrap_or(0),
        runtime: body.get("runtime").cloned().unwrap_or(Value::Null),
        code: body.get("code").and_then(Value::as_str).map(String::from),
        metrics_config: body
            .get("metricsConfig")
            .and_then(Value::as_str)
            .unwrap_or("DISABLED")
            .to_string(),
    };
    api.resolvers.insert(
        (type_name.to_string(), field_name.clone()),
        resolver.clone(),
    );
    ok_json(&json!({ "resolver": resolver_json(&resolver) }))
}

fn get_resolver(
    state: &mut AppSyncState,
    api_id: &str,
    type_name: &str,
    field_name: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    match api
        .resolvers
        .get(&(type_name.to_string(), field_name.to_string()))
    {
        Some(r) => ok_json(&json!({ "resolver": resolver_json(r) })),
        None => not_found("Resolver"),
    }
}

fn update_resolver(
    state: &mut AppSyncState,
    api_id: &str,
    type_name: &str,
    field_name: &str,
    body: &Value,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(r) = api
        .resolvers
        .get_mut(&(type_name.to_string(), field_name.to_string()))
    else {
        return not_found("Resolver");
    };
    if let Some(dsn) = body.get("dataSourceName").and_then(Value::as_str) {
        r.data_source_name = Some(dsn.to_string());
    }
    if let Some(t) = body.get("requestMappingTemplate").and_then(Value::as_str) {
        r.request_mapping_template = Some(t.to_string());
    }
    if let Some(t) = body.get("responseMappingTemplate").and_then(Value::as_str) {
        r.response_mapping_template = Some(t.to_string());
    }
    if let Some(kind) = body.get("kind").and_then(Value::as_str) {
        r.kind = kind.to_string();
    }
    ok_json(&json!({ "resolver": resolver_json(r) }))
}

fn delete_resolver(
    state: &mut AppSyncState,
    api_id: &str,
    type_name: &str,
    field_name: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.resolvers
        .remove(&(type_name.to_string(), field_name.to_string()));
    ok_json(&json!({}))
}

fn list_resolvers(state: &mut AppSyncState, api_id: &str, type_name: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let list: Vec<Value> = api
        .resolvers
        .values()
        .filter(|r| r.type_name == type_name)
        .map(resolver_json)
        .collect();
    ok_json(&json!({ "resolvers": list }))
}

fn list_resolvers_by_function(
    state: &mut AppSyncState,
    api_id: &str,
    function_id: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    if !api.functions.contains_key(function_id) {
        return not_found("Function");
    }
    ok_json(&json!({ "resolvers": [] }))
}

fn create_function(
    state: &mut AppSyncState,
    api_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let function_id = uuid()[..6].to_string();
    let f = GraphqlFunction {
        function_id: function_id.clone(),
        function_arn: format!(
            "arn:aws:appsync:{region}:{account}:apis/{api_id}/functions/{function_id}"
        ),
        name: body
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        data_source_name: body
            .get("dataSourceName")
            .and_then(Value::as_str)
            .map(String::from),
        description: body
            .get("description")
            .and_then(Value::as_str)
            .map(String::from),
        request_mapping_template: body
            .get("requestMappingTemplate")
            .and_then(Value::as_str)
            .map(String::from),
        response_mapping_template: body
            .get("responseMappingTemplate")
            .and_then(Value::as_str)
            .map(String::from),
        function_version: body
            .get("functionVersion")
            .and_then(Value::as_str)
            .unwrap_or("2018-05-29")
            .to_string(),
        sync_config: body.get("syncConfig").cloned().unwrap_or(Value::Null),
        max_batch_size: body
            .get("maxBatchSize")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        runtime: body.get("runtime").cloned().unwrap_or(Value::Null),
        code: body.get("code").and_then(Value::as_str).map(String::from),
    };
    api.functions.insert(function_id, f.clone());
    ok_json(&json!({ "functionConfiguration": function_json(&f) }))
}

fn get_function(state: &mut AppSyncState, api_id: &str, function_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    match api.functions.get(function_id) {
        Some(f) => ok_json(&json!({ "functionConfiguration": function_json(f) })),
        None => not_found("Function"),
    }
}

fn update_function(
    state: &mut AppSyncState,
    api_id: &str,
    function_id: &str,
    body: &Value,
) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(f) = api.functions.get_mut(function_id) else {
        return not_found("Function");
    };
    if let Some(name) = body.get("name").and_then(Value::as_str) {
        f.name = name.to_string();
    }
    if let Some(dsn) = body.get("dataSourceName").and_then(Value::as_str) {
        f.data_source_name = Some(dsn.to_string());
    }
    if let Some(d) = body.get("description").and_then(Value::as_str) {
        f.description = Some(d.to_string());
    }
    if let Some(t) = body.get("requestMappingTemplate").and_then(Value::as_str) {
        f.request_mapping_template = Some(t.to_string());
    }
    if let Some(t) = body.get("responseMappingTemplate").and_then(Value::as_str) {
        f.response_mapping_template = Some(t.to_string());
    }
    if let Some(v) = body.get("functionVersion").and_then(Value::as_str) {
        f.function_version = v.to_string();
    }
    ok_json(&json!({ "functionConfiguration": function_json(f) }))
}

fn delete_function(state: &mut AppSyncState, api_id: &str, function_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.functions.remove(function_id);
    ok_json(&json!({}))
}

fn list_functions(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let list: Vec<Value> = api.functions.values().map(function_json).collect();
    ok_json(&json!({ "functions": list }))
}

// --- api caches / domain names / env vars --------------------------------------

fn create_api_cache(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let cache = ApiCache {
        ttl: body.get("ttl").and_then(Value::as_i64).unwrap_or(3600),
        api_caching_behavior: body
            .get("apiCachingBehavior")
            .and_then(Value::as_str)
            .unwrap_or("FULL_REQUEST_CACHING")
            .to_string(),
        type_: body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("T2_SMALL")
            .to_string(),
    };
    api.api_cache = Some(cache.clone());
    ok_json(&json!({ "apiCache": cache.to_json() }))
}

fn get_api_cache(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    match &api.api_cache {
        Some(cache) => ok_json(&json!({ "apiCache": cache.to_json() })),
        None => not_found("ApiCache"),
    }
}

fn update_api_cache(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    let Some(cache) = api.api_cache.as_mut() else {
        return not_found("ApiCache");
    };
    cache.ttl = body.get("ttl").and_then(Value::as_i64).unwrap_or(cache.ttl);
    if let Some(b) = body.get("apiCachingBehavior").and_then(Value::as_str) {
        cache.api_caching_behavior = b.to_string();
    }
    if let Some(t) = body.get("type").and_then(Value::as_str) {
        cache.type_ = t.to_string();
    }
    ok_json(&json!({ "apiCache": cache.to_json() }))
}

fn delete_api_cache(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    api.api_cache = None;
    ok_json(&json!({}))
}

fn flush_api_cache(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    if api.api_cache.is_none() {
        return not_found("ApiCache");
    }
    ok_json(&json!({}))
}

fn create_domain_name(
    state: &mut AppSyncState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let domain = body
        .get("domainName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let dn = DomainNameConfig {
        domain_name: domain.clone(),
        description: body
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        certificate_arn: body
            .get("certificateArn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        appsync_domain_name: format!(
            "{}.appsync-api.{region}.amazonaws.com",
            uuid()[..8].to_string()
        ),
        hosted_zone_id: format!(
            "Z{}",
            uuid()[..12].to_string().to_uppercase().replace('-', "A")
        ),
        arn: format!("arn:aws:appsync:{region}:{account}:domainnames/{domain}"),
    };
    state.domain_names.insert(domain, dn.clone());
    ok_json(&json!({ "domainNameConfig": domain_name_json(&dn) }))
}

fn get_domain_name(state: &mut AppSyncState, domain: &str) -> Response {
    match state.domain_names.get(domain) {
        Some(dn) => ok_json(&json!({ "domainNameConfig": domain_name_json(dn) })),
        None => not_found("DomainName"),
    }
}

fn update_domain_name(state: &mut AppSyncState, domain: &str, body: &Value) -> Response {
    let Some(dn) = state.domain_names.get_mut(domain) else {
        return not_found("DomainName");
    };
    if let Some(d) = body.get("description").and_then(Value::as_str) {
        dn.description = d.to_string();
    }
    ok_json(&json!({ "domainNameConfig": domain_name_json(dn) }))
}

fn delete_domain_name(state: &mut AppSyncState, domain: &str) -> Response {
    if state.domain_names.remove(domain).is_none() {
        return not_found("DomainName");
    }
    state.api_associations.remove(domain);
    ok_json(&json!({}))
}

fn list_domain_names(state: &mut AppSyncState) -> Response {
    let list: Vec<Value> = state.domain_names.values().map(domain_name_json).collect();
    ok_json(&json!({ "domainNameConfigs": list }))
}

fn api_association_json(domain: &str, api_id: &str) -> Value {
    json!({
        "domainName": domain,
        "apiId": api_id,
        "associationStatus": "SUCCESS",
        "deploymentDetail": "",
    })
}

fn associate_api(state: &mut AppSyncState, domain: &str, body: &Value) -> Response {
    let Some(_dn) = state.domain_names.get(domain) else {
        return not_found("DomainName");
    };
    let api_id = body
        .get("apiId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let assoc = api_association_json(domain, &api_id);
    state
        .api_associations
        .insert(domain.to_string(), assoc.clone());
    ok_json(&json!({ "apiAssociation": assoc }))
}

fn disassociate_api(state: &mut AppSyncState, domain: &str) -> Response {
    // moto raises DomainNameNotFound for unknown domains.
    if !state.domain_names.contains_key(domain) {
        return not_found("DomainName");
    }
    state.api_associations.remove(domain);
    ok_json(&json!({}))
}

fn get_api_association(state: &mut AppSyncState, domain: &str) -> Response {
    match state.api_associations.get(domain) {
        Some(assoc) => ok_json(&json!({ "apiAssociation": assoc })),
        None => not_found("ApiAssociation"),
    }
}

fn get_env_vars(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.graphql_apis.get(api_id) else {
        return not_found("GraphQL API");
    };
    let mut obj = serde_json::Map::new();
    for (k, v) in &api.environment_variables {
        obj.insert(k.clone(), json!(v));
    }
    ok_json(&json!({ "environmentVariables": Value::Object(obj) }))
}

fn put_env_vars(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(api) = state.graphql_apis.get_mut(api_id) else {
        return not_found("GraphQL API");
    };
    if let Some(Value::Object(map)) = body.get("environmentVariables") {
        for (k, v) in map {
            api.environment_variables
                .insert(k.clone(), v.as_str().unwrap_or_default().to_string());
        }
    }
    let mut obj = serde_json::Map::new();
    for (k, v) in &api.environment_variables {
        obj.insert(k.clone(), json!(v));
    }
    ok_json(&json!({ "environmentVariables": Value::Object(obj) }))
}

// --- events apis (v2) ----------------------------------------------------------

fn create_events_api(
    state: &mut AppSyncState,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let api_id = uuid();
    let arn = format!("arn:aws:appsync:{region}:{account}:apis/{api_id}");
    let api = EventsApi {
        api_id: api_id.clone(),
        api_arn: arn.clone(),
        name,
        owner_contact: body
            .get("ownerContact")
            .and_then(Value::as_str)
            .map(String::from),
        event_config: body.get("eventConfig").cloned().unwrap_or(json!({})),
        created: iso_ts(),
        tags: BTreeMap::new(),
        channel_namespaces: BTreeMap::new(),
    };
    state.events_apis.insert(api_id.clone(), api.clone());
    let mut view = events_api_core_json(&api);
    view.as_object_mut().expect("api obj").insert(
        "dns".to_string(),
        json!({ "http": format!("https://{api_id}.appsync-api.{region}.amazonaws.com") }),
    );
    ok_json(&json!({ "api": view }))
}

fn events_api_core_json(a: &EventsApi) -> Value {
    json!({
        "apiId": a.api_id,
        "name": a.name,
        "dns": { "http": format!("https://{}.appsync-api.example", a.api_id) },
        "apiArn": a.api_arn,
        "created": a.created,
        "eventConfig": a.event_config,
    })
}

fn list_events_apis(state: &mut AppSyncState) -> Response {
    let apis: Vec<Value> = state
        .events_apis
        .values()
        .map(|a| {
            let mut view = events_api_core_json(a);
            view.as_object_mut()
                .expect("api obj")
                .insert("tags".to_string(), inline_tags_json(&a.tags));
            view
        })
        .collect();
    ok_json(&json!({ "apis": apis }))
}

fn get_events_api(state: &mut AppSyncState, api_id: &str) -> Response {
    match state.events_apis.get(api_id) {
        Some(a) => {
            let mut view = events_api_core_json(a);
            view.as_object_mut()
                .expect("api obj")
                .insert("tags".to_string(), inline_tags_json(&a.tags));
            ok_json(&json!({ "api": view }))
        }
        None => not_found("API"),
    }
}

fn update_events_api(state: &mut AppSyncState, api_id: &str, body: &Value) -> Response {
    let Some(a) = state.events_apis.get_mut(api_id) else {
        return not_found("API");
    };
    if let Some(name) = body.get("name").and_then(Value::as_str) {
        a.name = name.to_string();
    }
    if let Some(config) = body.get("eventConfig") {
        a.event_config = config.clone();
    }
    if let Some(contact) = body.get("ownerContact").and_then(Value::as_str) {
        a.owner_contact = Some(contact.to_string());
    }
    let mut view = events_api_core_json(a);
    view.as_object_mut()
        .expect("api obj")
        .insert("tags".to_string(), inline_tags_json(&a.tags));
    ok_json(&json!({ "api": view }))
}

fn delete_events_api(state: &mut AppSyncState, api_id: &str) -> Response {
    if state.events_apis.remove(api_id).is_none() {
        return not_found("API");
    }
    ok_json(&json!({}))
}

fn create_channel_namespace(
    state: &mut AppSyncState,
    api_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    let Some(api) = state.events_apis.get_mut(api_id) else {
        return not_found("API");
    };
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let ns = ChannelNamespace {
        api_id: api_id.to_string(),
        name: name.clone(),
        channel_namespace_arn: format!(
            "arn:aws:appsync:{region}:{account}:apis/{api_id}/channelNamespace/{name}"
        ),
        subscribe_auth_modes: body.get("subscribeAuthModes").cloned().unwrap_or(json!([])),
        publish_auth_modes: body.get("publishAuthModes").cloned().unwrap_or(json!([])),
        code_handlers: body.get("codeHandlers").cloned().unwrap_or(Value::Null),
        handler_configs: body.get("handlerConfigs").cloned().unwrap_or(json!({})),
        created: iso_ts(),
        last_modified: iso_ts(),
    };
    api.channel_namespaces.insert(name, ns.clone());
    ok_json(&json!({ "channelNamespace": channel_namespace_json(&ns, &json!({})) }))
}

fn list_channel_namespaces(state: &mut AppSyncState, api_id: &str) -> Response {
    let Some(api) = state.events_apis.get(api_id) else {
        return not_found("API");
    };
    let list: Vec<Value> = api
        .channel_namespaces
        .values()
        .map(|ns| channel_namespace_json(ns, &json!({})))
        .collect();
    ok_json(&json!({ "channelNamespaces": list }))
}

fn get_channel_namespace(state: &mut AppSyncState, api_id: &str, name: &str) -> Response {
    let Some(api) = state.events_apis.get(api_id) else {
        return not_found("API");
    };
    match api.channel_namespaces.get(name) {
        Some(ns) => ok_json(&json!({ "channelNamespace": channel_namespace_json(ns, &json!({})) })),
        None => not_found("ChannelNamespace"),
    }
}

fn update_channel_namespace(
    state: &mut AppSyncState,
    api_id: &str,
    name: &str,
    body: &Value,
) -> Response {
    let Some(api) = state.events_apis.get_mut(api_id) else {
        return not_found("API");
    };
    let Some(ns) = api.channel_namespaces.get_mut(name) else {
        return not_found("ChannelNamespace");
    };
    if let Some(modes) = body.get("subscribeAuthModes") {
        ns.subscribe_auth_modes = modes.clone();
    }
    if let Some(modes) = body.get("publishAuthModes") {
        ns.publish_auth_modes = modes.clone();
    }
    if let Some(handlers) = body.get("codeHandlers") {
        ns.code_handlers = handlers.clone();
    }
    if let Some(config) = body.get("handlerConfigs") {
        ns.handler_configs = config.clone();
    }
    ns.last_modified = iso_ts();
    ok_json(&json!({ "channelNamespace": channel_namespace_json(ns, &json!({})) }))
}

fn delete_channel_namespace(state: &mut AppSyncState, api_id: &str, name: &str) -> Response {
    let Some(api) = state.events_apis.get_mut(api_id) else {
        return not_found("API");
    };
    api.channel_namespaces.remove(name);
    ok_json(&json!({}))
}

// --- merged/source api associations ----------------------------------------------

fn association_record(assoc_id: &str, source_id: &str, merged_id: &str) -> Value {
    json!({
        "sourceApiAssociation": {
            "associationId": assoc_id,
            "sourceApiId": source_id,
            "mergedApiId": merged_id,
            "sourceApiAssociationConfig": {},
            "sourceApiAssociationStatus": "MERGE_SUCCESS",
        }
    })
}

// --- dataplane mocks ----------------------------------------------------------------

fn evaluate_mapping_template(state: &mut AppSyncState, body: &Value) -> Response {
    let _ = state;
    let template = body.get("template").and_then(Value::as_str).unwrap_or("");
    // toy VTL: $util.toJson(<json literal>) renders the literal
    let result = template
        .strip_prefix("$util.toJson(")
        .and_then(|rest| rest.strip_suffix(')'))
        .map(|payload| serde_json::from_str::<Value>(payload).map(|v| v.to_string()).unwrap_or_else(|_| payload.to_string()))
        .map(|evaluation| ok_json(&json!({ "evaluationResult": evaluation, "error": Value::Null, "logs": [], "stash": {} })))
        .unwrap_or_else(|| {
            ok_json(&json!({
                "evaluationResult": "",
                "error": { "message": "Unsupported template" },
                "logs": [],
                "stash": {},
            }))
        });
    result
}

fn evaluate_code(_body: &Value) -> Response {
    // The JS runtime evaluation is not supported; surface the error member
    // like the real API's failure path (the suites accept either result).
    ok_json(&json!({
        "evaluationResult": Value::Null,
        "error": { "message": "Evaluation of AppSync JS runtime code is not supported" },
        "logs": [],
        "stash": {},
        "outErrors": [],
    }))
}

fn start_data_source_introspection() -> Response {
    ok_json(&json!({
        "introspectionId": uuid(),
        "introspectionStatus": "PROCESSING",
    }))
}

fn get_data_source_introspection(state: &mut AppSyncState, introspection_id: &str) -> Response {
    let _ = (state, introspection_id);
    ok_json(&json!({
        "introspectionId": introspection_id,
        "introspectionStatus": "SUCCESS",
    }))
}

/// RFC 4648 std-alphabet decode with padding tolerance.
fn decode_b64(input: &str) -> Option<Vec<u8>> {
    const REV: &[u8; 256] = &{
        let mut table = [0xffu8; 256];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            table[alphabet[i] as usize] = i as u8;
            i += 1;
        }
        table
    };
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for byte in input.bytes() {
        match byte {
            b'=' | b'\n' | b'\r' | b' ' | b'\t' => continue,
            b if (b as usize) < REV.len() && REV[b as usize] != 0xff => {
                acc = (acc << 6) | REV[b as usize] as u32;
                bits += 6;
                if bits >= 8 {
                    bits -= 8;
                    out.push((acc >> bits) as u8);
                }
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Collect repeated `?tagKeys=` query params like moto's querystring.get.
fn query_tag_keys(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        let (k, v) = match pair.find('=') {
            Some(i) => (&pair[..i], &pair[i + 1..]),
            None => (pair, ""),
        };
        if k == "tagKeys" && !v.is_empty() {
            out.push(v.to_string());
        }
    }
    out
}

/// Percent-decode a path component (tag ARNs can be %2F encoded).
fn appsync_percent_decode(s: &str) -> String {
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
// Router
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
    };
    let query = req.query_string.clone();
    let query_of = |key: &str| -> Option<String> {
        query.split('&').find_map(|pair| {
            let (k, v) = match pair.find('=') {
                Some(i) => (&pair[..i], &pair[i + 1..]),
                None => (pair, ""),
            };
            if k == key {
                Some(v.to_string())
            } else {
                None
            }
        })
    };
    let path = req.path.trim_matches('/');
    let segments: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    // /v1/tags/{arn...}: the arn may span multiple segments after decoding.
    if segments.first() == Some(&"v1") && segments.get(1) == Some(&"tags") {
        let arn = appsync_percent_decode(req.path.replace("/v1/tags", "").trim_start_matches('/'));
        return with_state(req.account, &req.region, |state| {
            req_tags(state, &body, &arn, req.method.as_str(), &query)
        });
    }
    with_state(req.account, &req.region, |state| {
        route(
            state,
            req.method.as_str(),
            &segments,
            &body,
            req.account,
            &req.region,
            query_of,
            req,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn route<'q>(
    state: &mut AppSyncState,
    method: &str,
    segments: &[&str],
    body: &Value,
    account: u64,
    region: &str,
    query_of: impl Fn(&str) -> Option<String> + 'q,
    _req: &Request,
) -> Response {
    match segments {
        ["v1", "apis"] => match method {
            "POST" => create_graphql_api(state, body, account, region),
            "GET" => list_graphql_apis(state),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id] => match method {
            "GET" => get_graphql_api(state, api_id),
            "POST" => update_graphql_api(state, api_id, body),
            "DELETE" => delete_graphql_api(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "apikeys"] => match method {
            "POST" => create_api_key(state, api_id, body),
            "GET" => list_api_keys(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "apikeys", key_id] => match method {
            "DELETE" => delete_api_key(state, api_id, key_id),
            "POST" => update_api_key(state, api_id, key_id, body),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "schemacreation"] => match method {
            "POST" => start_schema_creation(state, api_id, body),
            "GET" => get_schema_creation_status(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "schema"] => {
            if let Some(api) = state.graphql_apis.get(*api_id) {
                let Some(definition) = api.schema_definition.clone() else {
                    return json_resp(
                        400,
                        &json!({ "__type": "GraphQLSchemaException", "message": "InvalidSyntaxError" }),
                    );
                };
                Response::new(200, "text/plain", definition.into_bytes())
            } else {
                not_found("GraphQL API")
            }
        }
        ["v1", "apis", api_id, "datasources"] => match method {
            "POST" => create_data_source(state, api_id, body, account, region),
            "GET" => list_data_sources(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "datasources", name] => match method {
            "GET" => get_data_source(state, api_id, name),
            "POST" => update_data_source(state, api_id, name, body),
            "DELETE" => delete_data_source(state, api_id, name),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "types"] => match method {
            "POST" => create_type_route(state, api_id, body, account, region),
            "GET" => list_types(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "types", type_name] => match method {
            "GET" => {
                let format = query_of("format").unwrap_or_else(|| "SDL".to_string());
                get_type(state, api_id, type_name, &format)
            }
            "POST" => update_type(state, api_id, type_name, body),
            "DELETE" => delete_type(state, api_id, type_name),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "types", type_name, "resolvers"] => match method {
            "POST" => create_resolver(state, api_id, type_name, body, account, region),
            "GET" => list_resolvers(state, api_id, type_name),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "types", type_name, "resolvers", field] => match method {
            "GET" => get_resolver(state, api_id, type_name, field),
            "POST" => update_resolver(state, api_id, type_name, field, body),
            "DELETE" => delete_resolver(state, api_id, type_name, field),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "functions"] => match method {
            "POST" => create_function(state, api_id, body, account, region),
            "GET" => list_functions(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "functions", function_id] => match method {
            "GET" => get_function(state, api_id, function_id),
            "POST" => update_function(state, api_id, function_id, body),
            "DELETE" => delete_function(state, api_id, function_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "functions", function_id, "resolvers"] => {
            list_resolvers_by_function(state, api_id, function_id)
        }
        ["v1", "apis", api_id, "ApiCaches"] => match method {
            "POST" => create_api_cache(state, api_id, body),
            "GET" => get_api_cache(state, api_id),
            "DELETE" => delete_api_cache(state, api_id),
            _ => not_found("route"),
        },
        ["v1", "apis", api_id, "ApiCaches", "update"] => update_api_cache(state, api_id, body),
        ["v1", "apis", api_id, "FlushCache"] => flush_api_cache(state, api_id),
        ["v1", "apis", api_id, "environmentVariables"] => match method {
            "GET" => get_env_vars(state, api_id),
            "PUT" => put_env_vars(state, api_id, body),
            _ => not_found("route"),
        },
        ["v1", "apis", _api_id, "sourceApiAssociations"] => {
            ok_json(&json!({ "sourceApiAssociationSummaries": [] }))
        }
        ["v1", "domainnames"] => match method {
            "POST" => create_domain_name(state, body, account, region),
            "GET" => list_domain_names(state),
            _ => not_found("route"),
        },
        ["v1", "domainnames", domain] => match method {
            "GET" => get_domain_name(state, domain),
            "POST" => update_domain_name(state, domain, body),
            "DELETE" => delete_domain_name(state, domain),
            _ => not_found("route"),
        },
        ["v1", "domainnames", domain, "apiassociation"] => match method {
            "POST" => associate_api(state, domain, body),
            "GET" => get_api_association(state, domain),
            "DELETE" => disassociate_api(state, domain),
            _ => not_found("route"),
        },
        ["v1", "sourceApis", source_id, "mergedApiAssociations"] => {
            let merged_id = body
                .get("mergedApiIdentifier")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ok_json(&association_record(&uuid(), source_id, merged_id))
        }
        ["v1", "sourceApis", _source_id, "mergedApiAssociations", _assoc] => {
            ok_json(&json!({ "sourceApiAssociationStatus": "DELETION_IN_PROGRESS" }))
        }
        ["v1", "mergedApis", merged_id, "sourceApiAssociations"] => {
            let source_id = body
                .get("sourceApiIdentifier")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ok_json(&association_record(&uuid(), source_id, merged_id))
        }
        ["v1", "mergedApis", merged_id, "sourceApiAssociations", assoc_id] => match method {
            "DELETE" => ok_json(&json!({ "sourceApiAssociationStatus": "DELETION_IN_PROGRESS" })),
            "GET" => ok_json(&association_record(assoc_id, "", merged_id)),
            "POST" => ok_json(&association_record(assoc_id, "", merged_id)),
            _ => not_found("route"),
        },
        ["v1", "mergedApis", _merged_id, "sourceApiAssociations", _assoc_id, "merge"] => {
            ok_json(&json!({ "sourceApiAssociationStatus": "MERGE_IN_PROGRESS" }))
        }
        ["v1", "mergedApis", _merged_id, "sourceApiAssociations", _assoc_id, "types"] => {
            ok_json(&json!({ "types": [] }))
        }
        ["v2", "apis"] => match method {
            "POST" => create_events_api(state, body, account, region),
            "GET" => list_events_apis(state),
            _ => not_found("route"),
        },
        ["v2", "apis", api_id] => match method {
            "GET" => get_events_api(state, api_id),
            "POST" => update_events_api(state, api_id, body),
            "DELETE" => delete_events_api(state, api_id),
            _ => not_found("route"),
        },
        ["v2", "apis", api_id, "channelNamespaces"] => match method {
            "POST" => create_channel_namespace(state, api_id, body, account, region),
            "GET" => list_channel_namespaces(state, api_id),
            _ => not_found("route"),
        },
        ["v2", "apis", api_id, "channelNamespaces", name] => match method {
            "GET" => get_channel_namespace(state, api_id, name),
            "POST" => update_channel_namespace(state, api_id, name, body),
            "DELETE" => delete_channel_namespace(state, api_id, name),
            _ => not_found("route"),
        },
        ["v1", "dataplane-evaluatecode"] => evaluate_code(body),
        ["v1", "dataplane-evaluatetemplate"] => evaluate_mapping_template(state, body),
        ["v1", "datasources", "introspections"] => start_data_source_introspection(),
        ["v1", "datasources", "introspections", introspection_id] => {
            get_data_source_introspection(state, introspection_id)
        }
        ["event"] => ok_json(&json!({ "successful": [], "failed": [] })),
        _ => not_found("route"),
    }
}

fn create_type_route(
    state: &mut AppSyncState,
    api_id: &str,
    body: &Value,
    account: u64,
    region: &str,
) -> Response {
    create_type(state, api_id, body, account, region)
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
            service: "appsync".to_string(),
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

    fn create_api(name: &str) -> Value {
        body_of(&run(
            "POST",
            "/v1/apis",
            json!({"name": name, "authenticationType": "API_KEY"}),
        ))["graphqlApi"]
            .clone()
    }

    fn b64(data: &str) -> String {
        b64_encode(data.as_bytes())
    }

    fn b64_encode(data: &[u8]) -> String {
        const ALPHA: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let n = (chunk[0] as u32) << 16
                | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
                | (chunk.get(2).copied().unwrap_or(0) as u32);
            out.push(ALPHA[(n >> 18) as usize & 63] as char);
            out.push(ALPHA[(n >> 12) as usize & 63] as char);
            if chunk.len() > 1 {
                out.push(ALPHA[(n >> 6) as usize & 63] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(ALPHA[n as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
        out
    }

    #[test]
    fn graphql_api_lifecycle() {
        let api = create_api("test-life");
        assert_eq!(api["name"], "test-life");
        assert!(api["apiId"].is_string());
        assert!(api["arn"].as_str().unwrap().contains("appsync"));
        assert!(api["uris"].is_object());

        let api_id = api["apiId"].as_str().unwrap().to_string();
        let got =
            body_of(&run("GET", &format!("/v1/apis/{api_id}"), json!({})))["graphqlApi"].clone();
        assert_eq!(got["name"], "test-life");

        run(
            "POST",
            &format!("/v1/apis/{api_id}"),
            json!({"name": "renamed", "authenticationType": "AWS_IAM"}),
        );
        let got =
            body_of(&run("GET", &format!("/v1/apis/{api_id}"), json!({})))["graphqlApi"].clone();
        assert_eq!(got["name"], "renamed");
        assert_eq!(got["authenticationType"], "AWS_IAM");

        let listed = body_of(&run("GET", "/v1/apis", json!({})));
        assert!(listed["graphqlApis"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["apiId"] == api_id.as_str()));

        run("DELETE", &format!("/v1/apis/{api_id}"), json!({}));
        let gone = run("GET", &format!("/v1/apis/{api_id}"), json!({}));
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn api_keys_lifecycle() {
        let api = create_api("key-api-life");
        let api_id = api["apiId"].as_str().unwrap().to_string();
        let created = body_of(&run(
            "POST",
            &format!("/v1/apis/{api_id}/apikeys"),
            json!({}),
        ))["apiKey"]
            .clone();
        assert!(created["id"].is_string());
        let key_id = created["id"].as_str().unwrap().to_string();
        let listed = body_of(&run(
            "GET",
            &format!("/v1/apis/{api_id}/apikeys"),
            json!({}),
        ));
        assert!(listed["apiKeys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k["id"] == key_id.as_str()));
        run(
            "DELETE",
            &format!("/v1/apis/{api_id}/apikeys/{key_id}"),
            json!({}),
        );
        let listed = body_of(&run(
            "GET",
            &format!("/v1/apis/{api_id}/apikeys"),
            json!({}),
        ));
        assert!(!listed["apiKeys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k["id"] == key_id.as_str()));
    }

    #[test]
    fn schema_types_and_resolvers() {
        let api = create_api("schema-life");
        let api_id = api["apiId"].as_str().unwrap().to_string();
        let schema = "type Query { hello: String }";
        run(
            "POST",
            &format!("/v1/apis/{api_id}/schemacreation"),
            json!({"definition": b64_encode(schema.as_bytes())}),
        );
        let status = body_of(&run(
            "GET",
            &format!("/v1/apis/{api_id}/schemacreation"),
            json!({}),
        ));
        assert_eq!(status["status"], "SUCCESS");

        let introspection = run("GET", &format!("/v1/apis/{api_id}/schema"), json!({}));
        assert!(String::from_utf8_lossy(&introspection.body).contains("Query"));

        // resolvers require a data source
        run(
            "POST",
            &format!("/v1/apis/{api_id}/datasources"),
            json!({"name": "ds1", "type": "NONE"}),
        );
        let resolver = body_of(&run(
            "POST",
            &format!("/v1/apis/{api_id}/types/Query/resolvers"),
            json!({"fieldName": "hello", "dataSourceName": "ds1"}),
        ))["resolver"]
            .clone();
        assert_eq!(resolver["fieldName"], "hello");
        let got = body_of(&run(
            "GET",
            &format!("/v1/apis/{api_id}/types/Query/resolvers/hello"),
            json!({}),
        ))["resolver"]
            .clone();
        assert_eq!(got["dataSourceName"], "ds1");

        run(
            "DELETE",
            &format!("/v1/apis/{api_id}/types/Query/resolvers/hello"),
            json!({}),
        );
        let gone = run(
            "GET",
            &format!("/v1/apis/{api_id}/types/Query/resolvers/hello"),
            json!({}),
        );
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn events_api_lifecycle() {
        let created = body_of(&run(
            "POST",
            "/v2/apis",
            json!({
                "name": "evt-life",
                "eventConfig": {
                    "authProviders": [{"authType": "API_KEY"}],
                    "connectionAuthModes": [{"authType": "API_KEY"}],
                    "defaultPublishAuthModes": [{"authType": "API_KEY"}],
                    "defaultSubscribeAuthModes": [{"authType": "API_KEY"}],
                }
            }),
        ))["api"]
            .clone();
        assert_eq!(created["name"], "evt-life");
        assert!(created["apiArn"].is_string());
        let api_id = created["apiId"].as_str().unwrap().to_string();

        let got = body_of(&run("GET", &format!("/v2/apis/{api_id}"), json!({})))["api"].clone();
        assert_eq!(got["apiId"], api_id);

        let ns = body_of(&run(
            "POST",
            &format!("/v2/apis/{api_id}/channelNamespaces"),
            json!({"name": "chan1"}),
        ))["channelNamespace"]
            .clone();
        assert_eq!(ns["name"], "chan1");
        let listed = body_of(&run(
            "GET",
            &format!("/v2/apis/{api_id}/channelNamespaces"),
            json!({}),
        ));
        assert_eq!(listed["channelNamespaces"].as_array().unwrap().len(), 1);

        run(
            "DELETE",
            &format!("/v2/apis/{api_id}/channelNamespaces/chan1"),
            json!({}),
        );
        run("DELETE", &format!("/v2/apis/{api_id}"), json!({}));
        let gone = run("GET", &format!("/v2/apis/{api_id}"), json!({}));
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn domain_names_and_associations() {
        run(
            "POST",
            "/v1/domainnames",
            json!({"domainName": "dom1.example.com", "certificateArn": "arn:aws:acm:x"}),
        );
        let got = body_of(&run("GET", "/v1/domainnames/dom1.example.com", json!({})))
            ["domainNameConfig"]
            .clone();
        assert_eq!(got["domainName"], "dom1.example.com");
        assert!(got["appsyncDomainName"].is_string());

        run("DELETE", "/v1/domainnames/dom1.example.com", json!({}));
        let gone = run("GET", "/v1/domainnames/dom1.example.com", json!({}));
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn type_crud_basics() {
        let api = create_api("type-life");
        let api_id = api["apiId"].as_str().unwrap().to_string();
        let created = body_of(&run(
            "POST",
            &format!("/v1/apis/{api_id}/types"),
            json!({"definition": "type Mutation { addItem(name: String): String }", "format": "SDL"}),
        ))["type"].clone();
        assert_eq!(created["name"], "Mutation");
        let got = body_of(&run(
            "GET",
            &format!("/v1/apis/{api_id}/types/Mutation"),
            json!({}),
        ))["type"]
            .clone();
        assert_eq!(got["name"], "Mutation");
        run(
            "DELETE",
            &format!("/v1/apis/{api_id}/types/Mutation"),
            json!({}),
        );
        let gone = run(
            "GET",
            &format!("/v1/apis/{api_id}/types/Mutation"),
            json!({}),
        );
        assert_eq!(gone.status, 404);
    }
}
