//! Amazon MQ backend (REST-JSON; `/v1/*` paths).
//!
//! Port of moto's mq: brokers, configurations, revisions, users, tags, the
//! static engine-type/instance-option catalogs, and moto's quirks (the
//! `[ACTIVEMQ]`-only engine-type error message, silent user overwrite,
//! silent delete of unknown users). Wire shapes were captured from moto
//! 5.1.23 rest-json serialization: each response emits exactly the
//! output-shape members moto's `XFormedAttributePicker` resolves; members
//! that are `None` are omitted.
//!
//! One moto crash path is normalized instead of reproduced: `DeleteBroker`
//! and `RebootBroker` on an unknown broker id raise a raw `KeyError` in
//! moto (visible as `InternalError`/500 in `probes/mq.json`); both answer
//! the modeled `NotFoundException` here — only 501 means "not implemented".

use crate::store;
use crate::{Request, Response};
use base64::Engine;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

mod configuration;

use configuration::DEFAULT_CONFIGURATION_DATA;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct MqUser {
    console_access: bool,
    groups: Value,
}

struct Broker {
    id: String,
    arn: String,
    name: String,
    created: String,
    state: String,
    authentication_strategy: Option<String>,
    auto_minor_version_upgrade: Option<bool>,
    /// `{"current": ..., "history": [...]}` per moto's `Broker.configurations`.
    configurations: Value,
    deployment_mode: Option<String>,
    encryption_options: Option<Value>,
    engine_type: Option<String>,
    engine_version: Option<String>,
    host_instance_type: Option<String>,
    ldap_server_metadata: Option<Value>,
    logs: Value,
    maintenance_window_start_time: Value,
    publicly_accessible: Option<bool>,
    security_groups: Option<Value>,
    storage_type: Option<String>,
    subnet_ids: Value,
    instances: Value,
    /// Username is carried by the map key, like moto's `Broker._users` dict.
    users: BTreeMap<String, MqUser>,
}

impl Broker {
    fn user_summaries(&self) -> Value {
        Value::Array(
            self.users
                .keys()
                .map(|u| json!({ "username": u }))
                .collect(),
        )
    }

    /// DescribeBrokerResponse members moto resolves from the broker object.
    fn describe_json(&self, tags: &Value) -> Value {
        let mut v = json!({
            "brokerArn": self.arn,
            "brokerId": self.id,
            "brokerName": self.name,
            "brokerState": self.state,
            "brokerInstances": self.instances,
            "configurations": self.configurations,
            "created": self.created,
            "logs": self.logs,
            "maintenanceWindowStartTime": self.maintenance_window_start_time,
            "subnetIds": self.subnet_ids,
            "users": self.user_summaries(),
            "tags": tags,
        });
        if let Some(vv) = self.authentication_strategy.as_ref() {
            v["authenticationStrategy"] = json!(vv);
        }
        if let Some(vv) = self.auto_minor_version_upgrade {
            v["autoMinorVersionUpgrade"] = json!(vv);
        }
        if let Some(vv) = self.deployment_mode.as_ref() {
            v["deploymentMode"] = json!(vv);
        }
        if let Some(vv) = self.encryption_options.as_ref() {
            v["encryptionOptions"] = vv.clone();
        }
        if let Some(vv) = self.engine_type.as_ref() {
            v["engineType"] = json!(vv);
        }
        if let Some(vv) = self.engine_version.as_ref() {
            v["engineVersion"] = json!(vv);
        }
        if let Some(vv) = self.host_instance_type.as_ref() {
            v["hostInstanceType"] = json!(vv);
        }
        if let Some(vv) = self.ldap_server_metadata.as_ref() {
            v["ldapServerMetadata"] = vv.clone();
        }
        if let Some(vv) = self.publicly_accessible {
            v["publiclyAccessible"] = json!(vv);
        }
        if let Some(vv) = self.security_groups.as_ref() {
            v["securityGroups"] = vv.clone();
        }
        if let Some(vv) = self.storage_type.as_ref() {
            v["storageType"] = json!(vv);
        }
        v
    }

    /// UpdateBrokerResponse members moto resolves from the broker object —
    /// a subset of describe (no arn/instances/name/state/configurations;
    /// `configuration` never resolves because moto stores that dict under
    /// the differently-named `configurations` attribute).
    fn update_response_json(&self) -> Value {
        let mut v = json!({
            "brokerId": self.id,
            "logs": self.logs,
            "maintenanceWindowStartTime": self.maintenance_window_start_time,
        });
        if let Some(vv) = self.authentication_strategy.as_ref() {
            v["authenticationStrategy"] = json!(vv);
        }
        if let Some(vv) = self.auto_minor_version_upgrade {
            v["autoMinorVersionUpgrade"] = json!(vv);
        }
        if let Some(vv) = self.engine_version.as_ref() {
            v["engineVersion"] = json!(vv);
        }
        if let Some(vv) = self.host_instance_type.as_ref() {
            v["hostInstanceType"] = json!(vv);
        }
        if let Some(vv) = self.ldap_server_metadata.as_ref() {
            v["ldapServerMetadata"] = vv.clone();
        }
        if let Some(vv) = self.security_groups.as_ref() {
            v["securityGroups"] = vv.clone();
        }
        v
    }

    /// BrokerSummary members moto resolves from the broker object.
    fn summary_json(&self) -> Value {
        json!({
            "brokerArn": self.arn,
            "brokerId": self.id,
            "brokerName": self.name,
            "brokerState": self.state,
            "created": self.created,
            "deploymentMode": self.deployment_mode,
            "engineType": self.engine_type,
            "hostInstanceType": self.host_instance_type,
        })
    }
}

struct Revision {
    created: String,
    description: Option<String>,
    data: String,
}

struct Configuration {
    id: String,
    arn: String,
    created: String,
    name: String,
    engine_type: String,
    engine_version: Option<String>,
    authentication_strategy: String,
    /// revision id -> revision, ascending.
    revisions: BTreeMap<u64, Revision>,
}

impl Configuration {
    fn latest(&self) -> (u64, &Revision) {
        self.revisions
            .iter()
            .next_back()
            .map(|(k, v)| (*k, v))
            .expect("configurations always carry a revision")
    }

    fn latest_revision_json(&self) -> Value {
        let (rid, rev) = self.latest();
        let mut latest = json!({ "created": rev.created, "revision": rid });
        if let Some(d) = rev.description.as_ref() {
            latest["description"] = json!(d);
        }
        latest
    }

    /// CreateConfigurationResponse / DescribeConfigurationResponse members
    /// moto resolves from the Configuration object. `tags` is `Some` only
    /// for describe, where moto attaches `resp.tags` (an empty map still
    /// serializes as `"tags": {}`).
    fn describe_json(&self, tags: Option<Value>) -> Value {
        let mut v = json!({
            "arn": self.arn,
            "created": self.created,
            "id": self.id,
            "latestRevision": self.latest_revision_json(),
            "name": self.name,
        });
        v["authenticationStrategy"] = json!(self.authentication_strategy);
        v["engineType"] = json!(self.engine_type);
        if let Some(vv) = self.engine_version.as_ref() {
            v["engineVersion"] = json!(vv);
        }
        if let Some(t) = tags {
            v["tags"] = t;
        }
        v
    }

    /// UpdateConfigurationResponse members (a different output shape: no
    /// authenticationStrategy/engineType on the wire).
    fn update_response_json(&self) -> Value {
        json!({
            "arn": self.arn,
            "created": self.created,
            "id": self.id,
            "latestRevision": self.latest_revision_json(),
            "name": self.name,
        })
    }
}

#[derive(Default)]
struct State {
    brokers: BTreeMap<String, Broker>,
    configs: BTreeMap<String, Configuration>,
    /// ARN -> {key: value}, moto's TaggingService.
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("mq"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok_json(status: u16, v: &Value) -> Response {
    Response::json(status, serde_json::to_vec(v).unwrap_or_default())
}

fn empty(status: u16) -> Response {
    ok_json(status, &json!({}))
}

/// moto serializes both `message` and `Message` plus the errorAttribute on
/// rest-json error bodies.
fn error_response(status: u16, code: &str, attribute: &str, message: String) -> Response {
    let body = json!({
        "__type": code,
        "errorAttribute": attribute,
        "message": message,
        "Message": message,
    });
    let mut resp = ok_json(status, &body);
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn unknown_broker(broker_id: &str) -> Response {
    error_response(
        404,
        "NotFoundException",
        "broker-id",
        format!("Can't find requested broker [{broker_id}]. Make sure your broker exists."),
    )
}

fn unknown_configuration(config_id: &str) -> Response {
    error_response(
        404,
        "NotFoundException",
        "configuration_id",
        format!(
            "Can't find requested configuration [{config_id}]. Make sure your configuration exists."
        ),
    )
}

fn unknown_user(username: &str) -> Response {
    error_response(
        404,
        "NotFoundException",
        "username",
        format!("Can't find requested user [{username}]. Make sure your user exists."),
    )
}

/// moto's `UnknownEngineType` message lists only ACTIVEMQ even though
/// RABBITMQ is accepted — kept bug-for-bug (the probe capture confirms).
fn unknown_engine_type(engine_type: &str) -> Response {
    error_response(
        400,
        "BadRequestException",
        "engineType",
        format!("Broker engine type [{engine_type}] is invalid. Valid values are: [ACTIVEMQ]"),
    )
}

/// ISO-8601 with microseconds — moto's `iso8601` timestamp format for
/// non-zero microsecond values (`%Y-%m-%dT%H:%M:%S.%fZ`).
fn now_ts() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

/// moto's `mock_random.get_random_hex(length)`.
fn random_hex(len: usize) -> String {
    uuid::Uuid::new_v4().simple().to_string()[..len].to_string()
}

fn broker_arn(account: u64, region: &str, id: &str) -> String {
    format!(
        "arn:{}:mq:{region}:{account}:broker:{id}",
        partition(region)
    )
}

fn config_arn(account: u64, region: &str, id: &str) -> String {
    format!(
        "arn:{}:mq:{region}:{account}:configuration:{id}",
        partition(region)
    )
}

fn std_b64_decode(data: &str) -> Option<String> {
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}

/// True when config data enables LDAP authorization; moto base64-decodes
/// and walks the parsed XML for `cachedLDAPAuthorizationMap` inside the
/// authorization plugin map. Containment on the decoded (or raw) bytes
/// reaches the same verdict for well-formed inputs.
fn has_ldap_auth(data: &str) -> bool {
    std_b64_decode(data)
        .map(|s| s.contains("cachedLDAPAuthorizationMap"))
        .unwrap_or_else(|| data.contains("cachedLDAPAuthorizationMap"))
}

/// Merge a `{"k": "v", ...}` map into the tag store, mirroring
/// `TaggingService.tag_resource` + `convert_dict_to_tags_input`.
fn apply_tags(st: &mut State, arn: &str, tags: Option<&Value>) {
    if let Some(Value::Object(map)) = tags {
        let entry = st.tags.entry(arn.to_string()).or_default();
        for (k, v) in map {
            if let Some(vs) = v.as_str() {
                entry.insert(k.clone(), vs.to_string());
            }
        }
    }
}

fn tag_json(st: &State, arn: &str) -> Value {
    Value::Object(
        st.tags
            .get(arn)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
            .unwrap_or_default(),
    )
}

fn is_empty_array(v: &Value) -> bool {
    matches!(v, Value::Array(a) if a.is_empty())
}

fn groups_or_empty(p: &Value) -> Value {
    if let Some(arr) = p.get("groups").and_then(Value::as_array) {
        return Value::Array(arr.clone());
    }
    json!([])
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "mq" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let region = req.region.as_str();
    let account = req.account;
    let path = req.path.as_str();
    let method = req.method.as_str();

    if path == "/v1/brokers" {
        return match method {
            "POST" => create_broker_handler(account, region, st, p),
            "GET" => {
                let list: Vec<Value> = st.brokers.values().map(|b| b.summary_json()).collect();
                ok_json(200, &json!({ "brokerSummaries": list }))
            }
            _ => empty(200),
        };
    }

    if path == "/v1/broker-engine-types" {
        return match method {
            "GET" => ok_json(
                200,
                &json!({
                    "brokerEngineTypes": [
                        {
                            "engineType": "ACTIVEMQ",
                            "engineVersions": [
                                {"name": "5.17.6"},
                                {"name": "5.16.7"},
                                {"name": "5.15.16"},
                            ],
                        },
                        {
                            "engineType": "RABBITMQ",
                            "engineVersions": [
                                {"name": "3.13.7"},
                                {"name": "3.12.13"},
                                {"name": "3.11.28"},
                            ],
                        },
                    ]
                }),
            ),
            _ => empty(200),
        };
    }

    if path == "/v1/broker-instance-options" {
        return match method {
            "GET" => ok_json(200, &broker_instance_options(region)),
            _ => empty(200),
        };
    }

    if let Some(rest) = path.strip_prefix("/v1/brokers/") {
        let (broker_id, tail) = match rest.split_once('/') {
            Some((id, t)) => (id, Some(t)),
            None => (rest, None),
        };
        if let Some(tail) = tail {
            if tail == "users" {
                return match st.brokers.get(broker_id) {
                    Some(b) => ok_json(
                        200,
                        &json!({ "brokerId": broker_id, "users": b.user_summaries() }),
                    ),
                    None => unknown_broker(broker_id),
                };
            }
            if let Some(username) = tail.strip_prefix("users/") {
                return match method {
                    // moto overwrites an existing user silently.
                    "POST" => match st.brokers.get_mut(broker_id) {
                        Some(b) => {
                            b.users.insert(
                                username.to_string(),
                                MqUser {
                                    console_access: p
                                        .get("consoleAccess")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false),
                                    groups: groups_or_empty(p),
                                },
                            );
                            empty(200)
                        }
                        None => unknown_broker(broker_id),
                    },
                    "PUT" => match st.brokers.get_mut(broker_id) {
                        Some(b) => match b.users.get_mut(username) {
                            Some(u) => {
                                u.console_access = p
                                    .get("consoleAccess")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                let groups = p.get("groups").cloned().unwrap_or_else(|| json!([]));
                                if !is_empty_array(&groups) {
                                    u.groups = groups;
                                }
                                empty(200)
                            }
                            None => unknown_user(username),
                        },
                        None => unknown_broker(broker_id),
                    },
                    "GET" => match st.brokers.get(broker_id) {
                        Some(b) => match (b.users.get(username), b.id.as_str()) {
                            (Some(u), id) => ok_json(
                                200,
                                &json!({
                                    "brokerId": id,
                                    "consoleAccess": u.console_access,
                                    "groups": u.groups,
                                    "username": username,
                                }),
                            ),
                            (None, _) => unknown_user(username),
                        },
                        None => unknown_broker(broker_id),
                    },
                    // moto deletes unknown usernames silently.
                    "DELETE" => match st.brokers.get_mut(broker_id) {
                        Some(b) => {
                            b.users.remove(username);
                            empty(200)
                        }
                        None => unknown_broker(broker_id),
                    },
                    _ => empty(200),
                };
            }
            if tail == "promote" {
                return match st.brokers.get_mut(broker_id) {
                    Some(b) => {
                        b.state = "RUNNING".to_string();
                        ok_json(200, &json!({ "brokerId": broker_id }))
                    }
                    None => unknown_broker(broker_id),
                };
            }
            if tail == "reboot" {
                // moto's Broker.reboot() is a no-op; unknown brokers raise a
                // raw KeyError in moto, normalized to the modeled 404 here.
                return match st.brokers.get_mut(broker_id) {
                    Some(_) => empty(200),
                    None => unknown_broker(broker_id),
                };
            }
        }
        // /v1/brokers/{id}: describe / update / delete.
        return match method {
            "GET" => match st.brokers.get(broker_id) {
                Some(b) => {
                    let tags = tag_json(st, &b.arn);
                    ok_json(200, &b.describe_json(&tags))
                }
                None => unknown_broker(broker_id),
            },
            "PUT" => match st.brokers.get_mut(broker_id) {
                Some(b) => {
                    apply_broker_update(b, p);
                    ok_json(200, &b.update_response_json())
                }
                None => unknown_broker(broker_id),
            },
            "DELETE" => match st.brokers.remove(broker_id) {
                Some(b) => ok_json(200, &json!({ "brokerId": b.id })),
                None => unknown_broker(broker_id),
            },
            _ => empty(200),
        };
    }

    if path == "/v1/configurations" {
        return match method {
            "POST" => create_configuration_handler(account, region, st, p),
            "GET" => {
                let list: Vec<Value> = st.configs.values().map(|c| c.describe_json(None)).collect();
                ok_json(200, &json!({ "configurations": list }))
            }
            _ => empty(200),
        };
    }

    if let Some(rest) = path.strip_prefix("/v1/configurations/") {
        let (config_id, tail) = match rest.split_once('/') {
            Some((id, t)) => (id, Some(t)),
            None => (rest, None),
        };
        return match tail {
            None => match method {
                "GET" => match st.configs.get(config_id) {
                    Some(c) => {
                        let tags = tag_json(st, &c.arn);
                        ok_json(200, &c.describe_json(Some(tags)))
                    }
                    None => unknown_configuration(config_id),
                },
                "PUT" => update_configuration_handler(st, config_id, p),
                "DELETE" => match st.configs.remove(config_id) {
                    Some(c) => ok_json(200, &json!({ "configurationId": c.id })),
                    None => unknown_configuration(config_id),
                },
                _ => empty(200),
            },
            Some("revisions") if method == "GET" => match st.configs.get(config_id) {
                Some(c) => {
                    let revisions: Vec<Value> = c
                        .revisions
                        .iter()
                        .map(|(rid, r)| {
                            let mut v = json!({ "created": r.created, "revision": rid });
                            if let Some(d) = r.description.as_ref() {
                                v["description"] = json!(d);
                            }
                            v
                        })
                        .collect();
                    ok_json(
                        200,
                        &json!({ "configurationId": config_id, "revisions": revisions }),
                    )
                }
                None => unknown_configuration(config_id),
            },
            Some(t) if t.starts_with("revisions/") && method == "GET" => {
                let revision = t.trim_start_matches("revisions/");
                match st.configs.get(config_id) {
                    Some(c) => {
                        let found = revision
                            .parse::<u64>()
                            .ok()
                            .and_then(|rid| c.revisions.get(&rid).map(|r| (rid, r)));
                        match found {
                            Some((_rid, r)) => {
                                let mut v = json!({
                                    "configurationId": config_id,
                                    "created": r.created,
                                    "data": r.data,
                                });
                                if let Some(d) = r.description.as_ref() {
                                    v["description"] = json!(d);
                                }
                                ok_json(200, &v)
                            }
                            // moto crashes with a raw KeyError on an unknown
                            // revision; normalized to the modeled 404 here.
                            None => unknown_configuration(config_id),
                        }
                    }
                    None => unknown_configuration(config_id),
                }
            }
            _ => empty(200),
        };
    }

    if let Some(raw) = path.strip_prefix("/v1/tags/").filter(|s| !s.is_empty()) {
        let arn = crate::proto::url_decode(raw);
        return match method {
            "GET" => ok_json(200, &json!({ "tags": tag_json(st, &arn) })),
            "POST" => {
                apply_tags(st, &arn, p.get("tags"));
                empty(204)
            }
            "DELETE" => {
                let qm = crate::proto::query_map(&req.query_string);
                let keys = qm.get("tagKeys").cloned().unwrap_or_default();
                if let Some(entry) = st.tags.get_mut(&arn) {
                    for k in keys {
                        entry.remove(&k);
                    }
                }
                empty(204)
            }
            _ => empty(200),
        };
    }

    ok_json(404, &json!({}))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn create_broker_handler(account: u64, region: &str, st: &mut State, p: &Value) -> Response {
    let broker_name = p.get("brokerName").and_then(Value::as_str).unwrap_or("");
    let engine_type = p.get("engineType").and_then(Value::as_str).unwrap_or("");
    let engine_version = p.get("engineVersion").and_then(Value::as_str).unwrap_or("");
    let mut configuration = p.get("configuration").cloned();
    if configuration.is_none() {
        // moto creates a default configuration first; engine-type validation
        // happens there (the only BadRequest path for CreateBroker without
        // an explicit configuration).
        match st.create_configuration(
            account,
            region,
            &format!("{broker_name}-configuration"),
            engine_type,
            engine_version,
            None,
        ) {
            Err(resp) => return resp,
            Ok(cfg) => {
                configuration = Some(json!({ "id": cfg.id, "revision": 1 }));
            }
        }
    }
    create_broker_record(account, region, st, p, configuration, engine_type)
}

fn create_broker_record(
    account: u64,
    region: &str,
    st: &mut State,
    p: &Value,
    configuration: Option<Value>,
    engine_type: &str,
) -> Response {
    let id = random_hex(6);
    let arn = broker_arn(account, region, &id);
    let deployment_mode = p
        .get("deploymentMode")
        .and_then(Value::as_str)
        .unwrap_or("");
    let is_rabbit = engine_type.eq_ignore_ascii_case("RABBITMQ");

    // Log defaults: general=false always; audit=false for ACTIVEMQ only.
    let mut logs = p
        .get("logs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if !logs.contains_key("general") {
        logs.insert("general".to_string(), json!(false));
    }
    if !logs.contains_key("audit") && !is_rabbit {
        logs.insert("audit".to_string(), json!(false));
    }

    let maintenance = p
        .get("maintenanceWindowStartTime")
        .and_then(Value::as_object)
        .cloned()
        .map(Value::Object)
        .unwrap_or_else(
            || json!({ "dayOfWeek": "Sunday", "timeOfDay": "00:00", "timeZone": "UTC" }),
        );

    let mut subnet_ids = p.get("subnetIds").cloned().unwrap_or_else(|| json!([]));
    if is_empty_array(&subnet_ids) {
        subnet_ids = match deployment_mode {
            "CLUSTER_MULTI_AZ" => {
                json!(["default-az1", "default-az2", "default-az3", "default-az4"])
            }
            "ACTIVE_STANDBY_MULTI_AZ" => json!(["active-subnet", "standby-subnet"]),
            _ => json!(["default-subnet"]),
        };
    }

    let (console_url, endpoints) = if is_rabbit {
        (
            format!("https://0000.mq.{region}.amazonaws.com"),
            json!(["amqps://mockmq:5671"]),
        )
    } else {
        (
            format!("https://0000.mq.{region}.amazonaws.com:8162"),
            json!([
                "ssl://mockmq:61617",
                "amqp+ssl://mockmq:5671",
                "stomp+ssl://mockmq:61614",
                "mqtt+ssl://mockmq:8883",
                "wss://mockmq:61619",
            ]),
        )
    };
    let mut instances = vec![json!({
        "consoleURL": console_url,
        "endpoints": endpoints,
        "ipAddress": "192.168.0.1",
    })];
    if deployment_mode == "ACTIVE_STANDBY_MULTI_AZ" {
        instances.push(json!({
            "consoleURL": instances[0]["consoleURL"],
            "endpoints": instances[0]["endpoints"],
            "ipAddress": "192.168.0.2",
        }));
    }

    let mut users = BTreeMap::new();
    if let Some(list) = p.get("users").and_then(Value::as_array) {
        for u in list {
            let username = u.get("username").and_then(Value::as_str).unwrap_or("");
            if username.is_empty() {
                continue;
            }
            users.insert(
                username.to_string(),
                MqUser {
                    console_access: u
                        .get("consoleAccess")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    groups: u
                        .get("groups")
                        .and_then(Value::as_array)
                        .cloned()
                        .map(Value::Array)
                        .unwrap_or_else(|| json!([])),
                },
            );
        }
    }

    st.brokers.insert(
        id.clone(),
        Broker {
            id: id.clone(),
            arn: arn.clone(),
            name: p
                .get("brokerName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            created: now_ts(),
            state: "RUNNING".to_string(),
            authentication_strategy: p
                .get("authenticationStrategy")
                .and_then(Value::as_str)
                .map(String::from),
            auto_minor_version_upgrade: p.get("autoMinorVersionUpgrade").and_then(Value::as_bool),
            configurations: json!({ "current": configuration, "history": [] }),
            deployment_mode: p
                .get("deploymentMode")
                .and_then(Value::as_str)
                .map(String::from),
            encryption_options: p
                .get("encryptionOptions")
                .cloned()
                .or_else(|| Some(json!({ "useAwsOwnedKey": true }))),
            engine_type: Some(engine_type.to_string()),
            engine_version: p
                .get("engineVersion")
                .and_then(Value::as_str)
                .map(String::from),
            host_instance_type: p
                .get("hostInstanceType")
                .and_then(Value::as_str)
                .map(String::from),
            ldap_server_metadata: p.get("ldapServerMetadata").cloned(),
            logs: Value::Object(logs),
            maintenance_window_start_time: maintenance,
            publicly_accessible: p.get("publiclyAccessible").and_then(Value::as_bool),
            security_groups: p.get("securityGroups").cloned(),
            storage_type: p
                .get("storageType")
                .and_then(Value::as_str)
                .map(String::from),
            subnet_ids,
            instances: Value::Array(instances),
            users,
        },
    );
    apply_tags(st, &arn, p.get("tags")); // moto tags the broker on create.
    ok_json(200, &json!({ "brokerArn": arn, "brokerId": id }))
}

/// The UpdateBroker mutation set (moto `Broker.update`): only provided,
/// non-None values overwrite.
fn apply_broker_update(b: &mut Broker, p: &Value) {
    if let Some(v) = p.get("authenticationStrategy").and_then(Value::as_str) {
        if !v.is_empty() {
            b.authentication_strategy = Some(v.to_string());
        }
    }
    if let Some(v) = p.get("autoMinorVersionUpgrade") {
        if !v.is_null() {
            b.auto_minor_version_upgrade = v.as_bool();
        }
    }
    if let Some(v) = p.get("configuration") {
        if !v.is_null() {
            if let Some(obj) = b.configurations.as_object_mut() {
                let current = obj.get("current").cloned().unwrap_or(Value::Null);
                if !current.is_null() {
                    obj.entry("history".to_string())
                        .or_insert_with(|| json!([]))
                        .as_array_mut()
                        .expect("history is an array")
                        .push(current);
                }
                obj.insert("current".to_string(), v.clone());
            }
        }
    }
    if let Some(v) = p.get("engineVersion").and_then(Value::as_str) {
        if !v.is_empty() {
            b.engine_version = Some(v.to_string());
        }
    }
    if let Some(v) = p.get("hostInstanceType").and_then(Value::as_str) {
        if !v.is_empty() {
            b.host_instance_type = Some(v.to_string());
        }
    }
    if let Some(v) = p.get("ldapServerMetadata") {
        if !v.is_null() {
            b.ldap_server_metadata = Some(v.clone());
        }
    }
    if let Some(v) = p.get("logs") {
        if !v.is_null() {
            b.logs = v.clone();
        }
    }
    if let Some(v) = p.get("maintenanceWindowStartTime") {
        if !v.is_null() {
            b.maintenance_window_start_time = v.clone();
        }
    }
    if let Some(v) = p.get("securityGroups") {
        if !v.is_null() {
            b.security_groups = Some(v.clone());
        }
    }
}

fn create_configuration_handler(account: u64, region: &str, st: &mut State, p: &Value) -> Response {
    let name = p.get("name").and_then(Value::as_str).unwrap_or("");
    let engine_type = p.get("engineType").and_then(Value::as_str).unwrap_or("");
    let engine_version = p.get("engineVersion").and_then(Value::as_str).unwrap_or("");
    match st.create_configuration(
        account,
        region,
        name,
        engine_type,
        engine_version,
        p.get("tags"),
    ) {
        Err(resp) => resp,
        Ok(c) => {
            // CreateConfigurationResponse has no `tags` member on the wire.
            ok_json(200, &c.describe_json(None))
        }
    }
}

fn update_configuration_handler(st: &mut State, config_id: &str, p: &Value) -> Response {
    match st.configs.get_mut(config_id) {
        Some(c) => {
            let data = p
                .get("data")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let description = p
                .get("description")
                .and_then(Value::as_str)
                .map(String::from);
            let next = c.latest().0 + 1;
            c.revisions.insert(
                next,
                Revision {
                    created: now_ts(),
                    description,
                    data: data.clone(),
                },
            );
            c.authentication_strategy = if has_ldap_auth(&data) {
                "ldap".to_string()
            } else {
                "simple".to_string()
            };
            ok_json(200, &c.update_response_json())
        }
        None => unknown_configuration(config_id),
    }
}

/// The static engine-type / instance-option catalogs moto serves.
fn broker_instance_options(region: &str) -> Value {
    let mut options = Vec::new();
    for engine in ["ACTIVEMQ", "RABBITMQ"] {
        for host in [
            "mq.m5.large",
            "mq.m5.xlarge",
            "mq.m5.2xlarge",
            "mq.m5.4xlarge",
            "mq.t3.micro",
        ] {
            options.push(json!({
                "availabilityZones": [
                    {"name": format!("{region}a")},
                    {"name": format!("{region}b")},
                    {"name": format!("{region}c")},
                ],
                "engineType": engine,
                "hostInstanceType": host,
                "storageType": "EBS",
                "supportedDeploymentModes": ["SINGLE_INSTANCE", "ACTIVE_STANDBY_MULTI_AZ"],
                "supportedEngineVersions": [if engine == "ACTIVEMQ" { "5.17.6" } else { "3.13.7" }],
            }));
        }
    }
    json!({ "brokerInstanceOptions": options })
}

impl State {
    /// moto's `MQBackend.create_configuration`.
    fn create_configuration(
        &mut self,
        account: u64,
        region: &str,
        name: &str,
        engine_type: &str,
        engine_version: &str,
        tags: Option<&Value>,
    ) -> Result<&Configuration, Response> {
        if !["ACTIVEMQ", "RABBITMQ"].contains(&engine_type.to_uppercase().as_str()) {
            return Err(unknown_engine_type(engine_type));
        }
        let id = format!("c-{}", random_hex(6));
        let arn = config_arn(account, region, &id);
        let authentication_strategy = if has_ldap_auth(DEFAULT_CONFIGURATION_DATA) {
            "ldap"
        } else {
            "simple"
        };
        self.configs.insert(
            id.clone(),
            Configuration {
                id: id.clone(),
                arn: arn.clone(),
                created: now_ts(),
                name: name.to_string(),
                engine_type: engine_type.to_string(),
                engine_version: if engine_version.is_empty() {
                    None
                } else {
                    Some(engine_version.to_string())
                },
                authentication_strategy: authentication_strategy.to_string(),
                revisions: BTreeMap::from([(
                    1,
                    Revision {
                        created: now_ts(),
                        description: Some(format!(
                            "Auto-generated default for {name} on {engine_type} {engine_version}"
                        )),
                        data: DEFAULT_CONFIGURATION_DATA.to_string(),
                    },
                )]),
            },
        );
        apply_tags(self, &arn, tags);
        Ok(self.configs.get(&id).expect("just inserted"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: &str, path: &str, query: &str, body: Value, account: u64) -> Request {
        Request {
            service: "mq".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    /// Route a request through `handle` exactly as `dispatch` would. Each
    /// test passes its own account id — the state slot is process-global.
    fn call(method: &str, path: &str, query: &str, body: Value, account: u64) -> Response {
        let request = req(method, path, query, body, account);
        let p = crate::proto::parse_json_body(&request.body);
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((account, "us-east-1".into())).or_default();
        handle(&request, b, &p)
    }

    fn json_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn create_broker_call(name: &str, users: Value, account: u64) -> Value {
        json_of(&call(
            "POST",
            "/v1/brokers",
            "",
            json!({
                "brokerName": name,
                "engineType": "ACTIVEMQ",
                "engineVersion": "5.17.6",
                "hostInstanceType": "mq.t3.micro",
                "deploymentMode": "SINGLE_INSTANCE",
                "publiclyAccessible": false,
                "users": users,
            }),
            account,
        ))
    }

    #[test]
    fn broker_lifecycle() {
        let acct = 123456789101;
        let created = create_broker_call("b1", json!([{"username": "admin"}]), acct);
        let id = created["brokerId"].as_str().unwrap().to_string();
        assert!(created["brokerArn"].as_str().unwrap().contains(":broker:"));

        let desc = json_of(&call(
            "GET",
            &format!("/v1/brokers/{id}"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(desc["brokerName"], "b1");
        assert_eq!(desc["brokerState"], "RUNNING");
        assert_eq!(desc["engineType"], "ACTIVEMQ");
        assert_eq!(desc["subnetIds"], json!(["default-subnet"]));
        assert_eq!(desc["encryptionOptions"]["useAwsOwnedKey"], true);
        assert_eq!(desc["logs"]["general"], false);
        assert_eq!(desc["users"][0]["username"], "admin");
        assert!(desc.get("autoMinorVersionUpgrade").is_none());
        assert!(desc["created"].as_str().unwrap().ends_with('Z'));

        let upd = json_of(&call(
            "PUT",
            &format!("/v1/brokers/{id}"),
            "",
            json!({"autoMinorVersionUpgrade": true}),
            acct,
        ));
        assert_eq!(upd["brokerId"], id.as_str());
        assert_eq!(upd["autoMinorVersionUpgrade"], true);

        let listed = json_of(&call("GET", "/v1/brokers", "", json!({}), acct));
        assert!(listed["brokerSummaries"].as_array().unwrap().len() == 1);

        let deleted = json_of(&call(
            "DELETE",
            &format!("/v1/brokers/{id}"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(deleted["brokerId"], id.as_str());
        let gone = call("GET", &format!("/v1/brokers/{id}"), "", json!({}), acct);
        assert_eq!(gone.status, 404);
        assert!(String::from_utf8_lossy(&gone.body).contains("NotFoundException"));
    }

    #[test]
    fn broker_unknown_returns_404() {
        let acct = 123456789102;
        for (method, path) in [
            ("GET", "/v1/brokers/missing-id"),
            ("DELETE", "/v1/brokers/missing-id"),
            ("POST", "/v1/brokers/missing-id/reboot"),
            ("GET", "/v1/brokers/missing-id/users"),
        ] {
            let resp = call(method, path, "", json!({}), acct);
            assert_eq!(resp.status, 404, "{method} {path}");
            assert!(
                String::from_utf8_lossy(&resp.body).contains("Can't find requested broker"),
                "{method} {path}"
            );
        }
    }

    #[test]
    fn configuration_lifecycle() {
        let acct = 123456789103;
        let bad = call(
            "POST",
            "/v1/configurations",
            "",
            json!({"name": "x", "engineType": "probe-test-value", "engineVersion": "5.15.0"}),
            acct,
        );
        assert_eq!(bad.status, 400);
        let err = json_of(&bad);
        assert_eq!(err["__type"], "BadRequestException");
        assert_eq!(err["errorAttribute"], "engineType");
        assert!(String::from_utf8_lossy(&bad.body).contains("Valid values are: [ACTIVEMQ]"));

        let created = json_of(&call(
            "POST",
            "/v1/configurations",
            "",
            json!({"name": "cfg", "engineType": "ACTIVEMQ", "engineVersion": "5.17.6"}),
            acct,
        ));
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["authenticationStrategy"], "simple");
        assert_eq!(created["latestRevision"]["revision"], 1);
        assert!(created["arn"].as_str().unwrap().contains(":configuration:"));
        assert!(created.get("tags").is_none(), "create response has no tags");

        let desc = json_of(&call(
            "GET",
            &format!("/v1/configurations/{id}"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(desc["name"], "cfg");
        assert_eq!(desc["engineType"], "ACTIVEMQ");
        assert!(desc.get("tags").is_some(), "describe always carries tags");

        let data_b64 = base64::engine::general_purpose::STANDARD.encode("xml-data");
        let updated = json_of(&call(
            "PUT",
            &format!("/v1/configurations/{id}"),
            "",
            json!({"data": data_b64, "description": "v2"}),
            acct,
        ));
        assert_eq!(updated["latestRevision"]["revision"], 2);
        assert_eq!(updated["latestRevision"]["description"], "v2");

        let rev = json_of(&call(
            "GET",
            &format!("/v1/configurations/{id}/revisions/1"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(rev["configurationId"], id.as_str());
        assert!(rev["data"].as_str().unwrap().contains("<broker"));

        let revs = json_of(&call(
            "GET",
            &format!("/v1/configurations/{id}/revisions"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(revs["revisions"].as_array().unwrap().len(), 2);

        let deleted = json_of(&call(
            "DELETE",
            &format!("/v1/configurations/{id}"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(deleted["configurationId"], id.as_str());
        assert_eq!(
            call(
                "DELETE",
                &format!("/v1/configurations/{id}"),
                "",
                json!({}),
                acct
            )
            .status,
            404
        );
    }

    #[test]
    fn default_configuration_created_with_broker() {
        let acct = 123456789104;
        let created = create_broker_call("b2", json!([]), acct);
        let desc = json_of(&call(
            "GET",
            &format!("/v1/brokers/{}", created["brokerId"].as_str().unwrap()),
            "",
            json!({}),
            acct,
        ));
        let current = &desc["configurations"]["current"];
        assert!(current["id"].as_str().unwrap().starts_with("c-"));
        assert_eq!(current["revision"], 1);
    }

    #[test]
    fn engine_type_catalog() {
        let acct = 123456789105;
        let types = json_of(&call("GET", "/v1/broker-engine-types", "", json!({}), acct));
        assert_eq!(types["brokerEngineTypes"][0]["engineType"], "ACTIVEMQ");
        assert_eq!(
            types["brokerEngineTypes"][0]["engineVersions"][0]["name"],
            "5.17.6"
        );

        let opts = json_of(&call(
            "GET",
            "/v1/broker-instance-options",
            "",
            json!({}),
            acct,
        ));
        assert_eq!(opts["brokerInstanceOptions"].as_array().unwrap().len(), 10);
        assert_eq!(
            opts["brokerInstanceOptions"][0]["availabilityZones"][0]["name"],
            "us-east-1a"
        );
    }

    #[test]
    fn user_and_tag_roundtrip() {
        let acct = 123456789106;
        let created = create_broker_call("b3", json!([{"username": "root"}]), acct);
        let id = created["brokerId"].as_str().unwrap().to_string();

        let listed = json_of(&call(
            "GET",
            &format!("/v1/brokers/{id}/users"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(listed["users"][0]["username"], "root");

        call(
            "POST",
            &format!("/v1/brokers/{id}/users/alice"),
            "",
            json!({"password": "x", "groups": ["a"], "consoleAccess": true}),
            acct,
        );
        let user = json_of(&call(
            "GET",
            &format!("/v1/brokers/{id}/users/alice"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(user["consoleAccess"], true);
        assert_eq!(user["groups"], json!(["a"]));
        assert_eq!(user["brokerId"], id.as_str());

        call(
            "PUT",
            &format!("/v1/brokers/{id}/users/alice"),
            "",
            json!({"groups": ["admins"]}),
            acct,
        );
        let user = json_of(&call(
            "GET",
            &format!("/v1/brokers/{id}/users/alice"),
            "",
            json!({}),
            acct,
        ));
        assert_eq!(user["groups"], json!(["admins"]));

        assert_eq!(
            call("GET", "/v1/brokers/999/users/alice", "", json!({}), acct).status,
            404
        );
        assert_eq!(
            call(
                "GET",
                &format!("/v1/brokers/{id}/users/ghost"),
                "",
                json!({}),
                acct
            )
            .status,
            404
        );
        call(
            "DELETE",
            &format!("/v1/brokers/{id}/users/alice"),
            "",
            json!({}),
            acct,
        );
        assert_eq!(
            call(
                "GET",
                &format!("/v1/brokers/{id}/users/alice"),
                "",
                json!({}),
                acct
            )
            .status,
            404
        );

        let promote = json_of(&call(
            "POST",
            &format!("/v1/brokers/{id}/promote"),
            "",
            json!({"mode": "SWITCHOVER"}),
            acct,
        ));
        assert_eq!(promote["brokerId"], id.as_str());
        assert_eq!(
            call("POST", "/v1/brokers/404/promote", "", json!({}), acct).status,
            404
        );

        let reboot_status = call(
            "POST",
            &format!("/v1/brokers/{id}/reboot"),
            "",
            json!({}),
            acct,
        )
        .status;
        assert_eq!(reboot_status, 200);

        let arn = created["brokerArn"].as_str().unwrap().to_string();
        // The ARN rides in the path segment like botocore's uri substitution;
        // ':' needs no percent-encoding for moto's parser and url_decode
        // is a pass-through for it.
        let tags_path = format!("/v1/tags/{arn}");
        call(
            "POST",
            &tags_path,
            "",
            json!({"tags": {"env": "test", "team": "dev"}}),
            acct,
        );
        let tags = json_of(&call("GET", &tags_path, "", json!({}), acct));
        assert_eq!(tags["tags"]["env"], "test");
        call("DELETE", &tags_path, "tagKeys=team", json!({}), acct);
        let tags = json_of(&call("GET", &tags_path, "", json!({}), acct));
        assert!(tags["tags"].get("team").is_none());
        assert_eq!(tags["tags"]["env"], "test");
    }
}
