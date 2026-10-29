//! AWS Cloud Map (`servicediscovery`) backend — JSON 1.1 protocol.
//!
//! Port of moto's `servicediscovery`: namespaces (HTTP / private+public DNS)
//! with async-style operation objects, services with DNS/health configs,
//! registerable instances, discovery queries, service attributes and a
//! tag store, all per (account, region).
//!
//! Wire shapes mirror the moto module exactly, including the quirks:
//! - `UpdateHttpNamespace` answers lowercase `operationId` (moto bug).
//! - `DeleteService` is silent even for unknown ids.
//! - `ListOperations` prunes state while listing — operations whose targets
//!   don't hold a live namespace id (including service/instance ops) are
//!   dropped.
//! - DNS namespaces mint a plausible hosted-zone id (moto creates a real
//!   route53 zone; rotocore keeps the response shape without the
//!   cross-backend side effect).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------- state

struct Namespace {
    id: String,
    arn: String,
    name: Option<String>,
    ns_type: String,
    creator_request_id: Option<String>,
    description: Option<String>,
    dns_properties: Value,
    http_properties: Value,
    vpc: Option<String>,
    created: f64,
    updated: f64,
}

impl Namespace {
    fn to_json(&self) -> Value {
        json!({
            "Arn": self.arn,
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
            "Type": self.ns_type,
            "Properties": {
                "DnsProperties": self.dns_properties,
                "HttpProperties": self.http_properties,
            },
            "CreateDate": self.created,
            "UpdateDate": self.updated,
            "CreatorRequestId": self.creator_request_id,
        })
    }
}

struct Service {
    id: String,
    arn: String,
    name: Option<String>,
    namespace_id: Option<String>,
    description: Option<String>,
    creator_request_id: Option<String>,
    dns_config: Option<Value>,
    health_check_config: Option<Value>,
    health_check_custom_config: Option<Value>,
    service_type: Option<String>,
    created: f64,
    instances: Vec<Instance>,
    /// instance id -> registration revision (bumped on register/deregister).
    instances_revision: BTreeMap<String, u64>,
    service_attributes: BTreeMap<String, String>,
}

impl Service {
    fn to_json(&self) -> Value {
        json!({
            "Arn": self.arn,
            "Id": self.id,
            "Name": self.name,
            "NamespaceId": self.namespace_id,
            "CreateDate": self.created,
            "Description": self.description,
            "CreatorRequestId": self.creator_request_id,
            "DnsConfig": self.dns_config,
            "HealthCheckConfig": self.health_check_config,
            "HealthCheckCustomConfig": self.health_check_custom_config,
            "Type": self.service_type,
        })
    }
}

#[derive(Clone)]
struct Instance {
    instance_id: String,
    creator_request_id: String,
    attributes: Value,
    health_status: String,
}

impl Instance {
    fn to_json(&self) -> Value {
        json!({
            "Id": self.instance_id,
            "CreatorRequestId": self.creator_request_id,
            "Attributes": self.attributes,
        })
    }
}

struct Operation {
    id: String,
    status: String,
    operation_type: String,
    created: f64,
    updated: f64,
    targets: BTreeMap<String, String>,
}

impl Operation {
    /// moto emits `{Id, Status}` for ListOperations, the full shape for
    /// GetOperation.
    fn to_json(&self, short: bool) -> Value {
        if short {
            json!({"Id": self.id, "Status": self.status})
        } else {
            json!({
                "Id": self.id,
                "Status": self.status,
                "Type": self.operation_type,
                "CreateDate": self.created,
                "UpdateDate": self.updated,
                "Targets": self.targets,
            })
        }
    }
}

#[derive(Default)]
struct State {
    namespaces: BTreeMap<String, Namespace>,
    services: BTreeMap<String, Service>,
    operations: BTreeMap<String, Operation>,
    /// resource arn -> {tag key: tag value} (moto's TaggingService).
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut State) -> T) -> T {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("servicediscovery"));
    let mut map = SLOT.write();
    f(map.entry((account, region.to_string())).or_default())
}

// ------------------------------------------------------------ helpers

fn js(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(String::from)
}

fn object_at(v: &Value, key: &str) -> Map<String, Value> {
    v.get(key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

/// moto `random_id` — lowercase letters and digits.
fn random_id(size: usize) -> String {
    random_chars(size, b"abcdefghijklmnopqrstuvwxyz0123456789")
}

/// moto route53 zone ids — 22 uppercase letters/digits.
fn random_zone_id() -> String {
    random_chars(22, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}

fn random_chars(size: usize, alphabet: &[u8]) -> String {
    (0..size)
        .map(|_| {
            let i = (next_u64() % alphabet.len() as u64) as usize;
            alphabet[i] as char
        })
        .collect()
}

thread_local! {
    static RNG: Cell<u64> = const { Cell::new(0) };
}

fn next_u64() -> u64 {
    RNG.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1);
            x = nanos | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        cell.set(x);
        x
    })
}

fn ok_response(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

/// moto's JsonRESTError members all use the default 400 status.
fn error_response(status: u16, code: &str, message: &str) -> Response {
    crate::proto::json_error(status, code, message)
}

fn namespace_not_found(id: &str) -> Response {
    error_response(400, "NamespaceNotFound", id)
}

fn service_not_found(id: &str) -> Response {
    error_response(400, "ServiceNotFound", id)
}

fn instance_not_found(id: &str) -> Response {
    error_response(400, "InstanceNotFound", id)
}

fn operation_not_found() -> Response {
    error_response(400, "OperationNotFound", "")
}

fn conflicting_domain_exists(vpc: &str) -> Response {
    error_response(400, "ConflictingDomainExists", vpc)
}

fn custom_health_not_found(service_id: &str) -> Response {
    error_response(400, "CustomHealthNotFound", service_id)
}

fn invalid_input(msg: &str) -> Response {
    error_response(400, "InvalidInput", msg)
}

/// moto gets the partition prefix by region.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else {
        "aws"
    }
}

fn namespace_arn(region: &str, account: u64, id: &str) -> String {
    format!(
        "arn:{}:servicediscovery:{region}:{account}:namespace/{id}",
        partition(region)
    )
}

fn service_arn(region: &str, account: u64, id: &str) -> String {
    format!(
        "arn:{}:servicediscovery:{region}:{account}:service/{id}",
        partition(region)
    )
}

/// moto's `paginate`: offset slicing with a numeric next token. An invalid
/// token returns an empty page with no follow-up token.
fn paginate<T>(
    items: Vec<T>,
    max_results: Option<u64>,
    next_token: Option<&str>,
) -> (Vec<T>, Option<String>) {
    let token = next_token.unwrap_or("0");
    if token.is_empty() || !token.chars().all(|c| c.is_ascii_digit()) {
        return (Vec::new(), None);
    }
    let start: usize = token.parse().unwrap_or(0);
    let take = max_results.map(|m| m as usize).unwrap_or(items.len());
    let end = start + take;
    if end >= items.len() {
        (items.into_iter().skip(start).collect(), None)
    } else {
        (
            items.into_iter().skip(start).take(take).collect(),
            Some(end.to_string()),
        )
    }
}

/// moto's NotImplementedError message uses the snake_case action name.
fn snake(op: &str) -> String {
    let mut out = String::new();
    for (i, c) in op.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

fn unknown_op(op: &str) -> Response {
    error_response(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake(op)),
    )
}

// -------------------------------------------------------------- tags

/// moto TaggingService::tag_resource — `None` tags are a no-op, keys
/// overwrite.
fn tag_resource(st: &mut State, arn: &str, tags: Option<&Value>) {
    let Some(list) = tags.and_then(Value::as_array) else {
        return;
    };
    let slot = st.tags.entry(arn.to_string()).or_default();
    for tag in list {
        if let Some(key) = tag.get("Key").and_then(Value::as_str) {
            let value = tag.get("Value").and_then(Value::as_str).unwrap_or("");
            slot.insert(key.to_string(), value.to_string());
        }
    }
}

/// moto TaggingService::untag_resource_using_names.
fn untag_resource(st: &mut State, arn: &str, tag_keys: Option<&Value>) {
    let Some(keys) = tag_keys.and_then(Value::as_array) else {
        return;
    };
    if let Some(tags) = st.tags.get_mut(arn) {
        for key in keys {
            if let Some(name) = key.as_str() {
                tags.remove(name);
            }
        }
    }
}

fn list_tags(st: &State, arn: &str) -> Value {
    let mut out = Vec::new();
    if let Some(tags) = st.tags.get(arn) {
        for (key, value) in tags {
            out.push(json!({"Key": key, "Value": value}));
        }
    }
    json!({ "Tags": out })
}

// ----------------------------------------------------------- dispatch

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "servicediscovery" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    Some(handle(req, &body))
}

fn handle(req: &Request, body: &Value) -> Response {
    match req.operation.as_str() {
        "CreateHttpNamespace" => create_http_namespace(req, body),
        "CreatePrivateDnsNamespace" => create_dns_namespace(req, body, true),
        "CreatePublicDnsNamespace" => create_dns_namespace(req, body, false),
        "CreateService" => create_service(req, body),
        "DeleteNamespace" => delete_namespace(req, body),
        "DeleteService" => delete_service(req, body),
        "DeleteServiceAttributes" => delete_service_attributes(req, body),
        "DeregisterInstance" => deregister_instance(req, body),
        "DiscoverInstances" => discover_instances(req, body),
        "DiscoverInstancesRevision" => discover_instances_revision(req, body),
        "GetInstance" => get_instance(req, body),
        "GetInstancesHealthStatus" => get_instances_health_status(req, body),
        "GetNamespace" => get_namespace(req, body),
        "GetOperation" => get_operation(req, body),
        "GetService" => get_service_op(req, body),
        "GetServiceAttributes" => get_service_attributes(req, body),
        "ListInstances" => list_instances(req, body),
        "ListNamespaces" => list_namespaces(req),
        "ListOperations" => list_operations(req),
        "ListServices" => list_services(req),
        "ListTagsForResource" => list_tags_for_resource(req, body),
        "RegisterInstance" => register_instance(req, body),
        "TagResource" => tag_resource_op(req, body),
        "UntagResource" => untag_resource_op(req, body),
        "UpdateHttpNamespace" => update_dns_namespace(req, body),
        "UpdateInstanceCustomHealthStatus" => update_instance_custom_health_status(req, body),
        "UpdatePrivateDnsNamespace" => update_dns_namespace(req, body),
        "UpdatePublicDnsNamespace" => update_dns_namespace(req, body),
        "UpdateService" => update_service(req, body),
        "UpdateServiceAttributes" => update_service_attributes(req, body),
        _ => unknown_op(req.operation.as_str()),
    }
}

fn now_unix() -> f64 {
    crate::proto::now_unix()
}

fn create_operation(st: &mut State, operation_type: &str, targets: Vec<(&str, String)>) -> String {
    let id = format!("{}-{}", random_id(32), random_id(8));
    let now = now_unix();
    st.operations.insert(
        id.clone(),
        Operation {
            id: id.clone(),
            status: "SUCCESS".to_string(),
            operation_type: operation_type.to_string(),
            created: now,
            updated: now,
            targets: targets
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        },
    );
    id
}

// --------------------------------------------------------- namespaces

fn create_namespace(
    req: &Request,
    body: &Value,
    ns_type: &str,
    dns_properties: Value,
    http_properties: Value,
    vpc: Option<String>,
) -> Value {
    let id = format!("ns-{}", random_id(20));
    let arn = namespace_arn(&req.region, req.account, &id);
    with_state(req.account, &req.region, |st| {
        st.namespaces.insert(
            id.clone(),
            Namespace {
                id: id.clone(),
                arn,
                name: js(body, "Name"),
                ns_type: ns_type.to_string(),
                creator_request_id: js(body, "CreatorRequestId"),
                description: js(body, "Description"),
                dns_properties,
                http_properties,
                vpc,
                created: now_unix(),
                updated: now_unix(),
            },
        );
        if let Some(tags) = body.get("Tags") {
            let arn = namespace_arn(&req.region, req.account, &id);
            tag_resource(st, &arn, Some(tags));
        }
        let op_id = create_operation(st, "CREATE_NAMESPACE", vec![("NAMESPACE", id.clone())]);
        json!({ "OperationId": op_id })
    })
}

fn create_http_namespace(req: &Request, body: &Value) -> Response {
    let name = body.get("Name").cloned().unwrap_or(Value::Null);
    // moto: dns_properties {"SOA": {}}, http_properties {"HttpName": name}.
    ok_response(create_namespace(
        req,
        body,
        "HTTP",
        json!({"SOA": {}}),
        json!({ "HttpName": name }),
        None,
    ))
}

fn create_dns_namespace(req: &Request, body: &Value, private: bool) -> Response {
    let vpc = js(body, "Vpc");
    let mut dns_properties = Value::Object(
        body.get("Properties")
            .and_then(|p| p.get("DnsProperties"))
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
    );

    if private {
        // moto compares every namespace's vpc against the request's.
        let conflict = with_state(req.account, &req.region, |st| {
            st.namespaces
                .values()
                .any(|ns| ns.vpc == vpc)
                .then(|| vpc.clone())
        });
        if let Some(vpc) = conflict {
            return conflicting_domain_exists(&vpc.unwrap_or_default());
        }
    }
    dns_properties["HostedZoneId"] = json!(random_zone_id());

    let ns_type = if private { "DNS_PRIVATE" } else { "DNS_PUBLIC" };
    let ns_vpc = if private { vpc } else { None };
    ok_response(create_namespace(
        req,
        body,
        ns_type,
        dns_properties,
        json!({}),
        ns_vpc,
    ))
}

fn delete_namespace(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        if st.namespaces.remove(&id).is_none() {
            return namespace_not_found(&id);
        }
        let op_id = create_operation(st, "DELETE_NAMESPACE", vec![("NAMESPACE", id)]);
        ok_response(json!({ "OperationId": op_id }))
    })
}

fn get_namespace(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        match st.namespaces.get(&id) {
            Some(ns) => ok_response(json!({ "Namespace": ns.to_json() })),
            None => namespace_not_found(&id),
        }
    })
}

/// Shared body of UpdateHttpNamespace / UpdatePrivateDnsNamespace /
/// UpdatePublicDnsNamespace (moto's three update_*_namespace methods).
fn update_dns_namespace(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    let is_http = req.operation == "UpdateHttpNamespace";
    // moto indexes params["Namespace"]; boto marks the member required.
    let Some(ns_obj) = body.get("Namespace").and_then(Value::as_object) else {
        return invalid_input("Namespace is required");
    };
    if is_http && !ns_obj.contains_key("Description") {
        // moto: update_http_namespace requires a Description member.
        return invalid_input("Description is required");
    }
    // HTTP assigns the description unconditionally (null clears it); DNS
    // namespaces only assign when it is a string. Properties are applied
    // per namespace kind; moto never refreshes UpdateDate here.
    with_state(req.account, &req.region, |st| {
        let Some(ns) = st.namespaces.get_mut(&id) else {
            return namespace_not_found(&id);
        };
        if is_http {
            ns.description = ns_obj
                .get("Description")
                .and_then(Value::as_str)
                .map(String::from);
            if let Some(properties) = ns_obj.get("Properties") {
                if let Some(http_properties) = properties.get("HttpProperties") {
                    ns.http_properties = http_properties.clone();
                }
            }
        } else {
            if let Some(description) = ns_obj.get("Description").and_then(Value::as_str) {
                ns.description = Some(description.to_string());
            }
            // moto replaces DnsProperties wholesale (HostedZoneId included).
            if let Some(properties) = ns_obj
                .get("Properties")
                .and_then(|p| p.get("DnsProperties"))
            {
                ns.dns_properties = properties.clone();
            }
        }
        let ns_id = ns.id.clone();
        let op_id = create_operation(st, "UPDATE_NAMESPACE", vec![("NAMESPACE", ns_id)]);
        if is_http {
            // moto bug: this one operation answers a lowercase key.
            ok_response(json!({ "operationId": op_id }))
        } else {
            ok_response(json!({ "OperationId": op_id }))
        }
    })
}

fn list_namespaces(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let out: Vec<Value> = st.namespaces.values().map(Namespace::to_json).collect();
        ok_response(json!({ "Namespaces": out }))
    })
}

fn list_operations(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        // moto prunes while listing: operations without a live namespace
        // target (service/instance ops included) disappear from state.
        let live: Vec<String> = st.namespaces.keys().cloned().collect();
        st.operations.retain(|_, op| {
            op.targets
                .get("NAMESPACE")
                .map(|id| live.contains(id))
                .unwrap_or(false)
        });
        let out: Vec<Value> = st.operations.values().map(|op| op.to_json(true)).collect();
        ok_response(json!({ "Operations": out }))
    })
}

fn get_operation(req: &Request, body: &Value) -> Response {
    let id = js(body, "OperationId").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        match st.operations.get(&id) {
            Some(op) => ok_response(json!({ "Operation": op.to_json(false) })),
            None => operation_not_found(),
        }
    })
}

// ------------------------------------------------------------ services

fn create_service(req: &Request, body: &Value) -> Response {
    let id = format!("srv-{}", random_id(8));
    let arn = service_arn(&req.region, req.account, &id);
    with_state(req.account, &req.region, |st| {
        st.services.insert(
            id.clone(),
            Service {
                id: id.clone(),
                arn,
                name: js(body, "Name"),
                namespace_id: js(body, "NamespaceId"),
                description: js(body, "Description"),
                creator_request_id: js(body, "CreatorRequestId"),
                dns_config: body.get("DnsConfig").cloned(),
                health_check_config: body.get("HealthCheckConfig").cloned(),
                health_check_custom_config: body.get("HealthCheckCustomConfig").cloned(),
                service_type: js(body, "Type"),
                created: now_unix(),
                instances: Vec::new(),
                instances_revision: BTreeMap::new(),
                service_attributes: BTreeMap::new(),
            },
        );
        if let Some(tags) = body.get("Tags") {
            let arn = service_arn(&req.region, req.account, &id);
            tag_resource(st, &arn, Some(tags));
        }
        let svc = st.services.get(&id);
        ok_response(json!({ "Service": svc.unwrap().to_json() }))
    })
}

fn find_service<'a>(st: &'a State, service_id: &str) -> Result<&'a Service, Response> {
    match st.services.get(service_id) {
        Some(svc) => Ok(svc),
        None => Err(service_not_found(service_id)),
    }
}

fn get_service_op(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    with_state(req.account, &req.region, |st| match find_service(st, &id) {
        Ok(svc) => ok_response(json!({ "Service": svc.to_json() })),
        Err(e) => e,
    })
}

fn delete_service(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        // moto pops silently — no error for unknown ids.
        st.services.remove(&id);
        ok_response(json!({}))
    })
}

fn list_services(req: &Request) -> Response {
    with_state(req.account, &req.region, |st| {
        let out: Vec<Value> = st.services.values().map(Service::to_json).collect();
        ok_response(json!({ "Services": out }))
    })
}

fn update_service(req: &Request, body: &Value) -> Response {
    let id = js(body, "Id").unwrap_or_default();
    let details = match body.get("Service").and_then(Value::as_object) {
        Some(d) => d.clone(),
        None => return invalid_input("Service is required"),
    };
    with_state(req.account, &req.region, |st| {
        let Some(service) = st.services.get_mut(&id) else {
            return service_not_found(&id);
        };
        if let Some(description) = details.get("Description") {
            service.description = description.as_str().map(String::from);
        }
        let dns_config = match details.get("DnsConfig") {
            Some(config) => {
                let mut merged = service
                    .dns_config
                    .as_ref()
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                merged.insert(
                    "DnsRecords".to_string(),
                    config.get("DnsRecords").cloned().unwrap_or(Value::Null),
                );
                Some(Value::Object(merged))
            }
            // Omitting DnsConfig in an update deletes the configuration.
            None => None,
        };
        service.dns_config = dns_config;
        if let Some(hcc) = details.get("HealthCheckConfig") {
            service.health_check_config = Some(hcc.clone());
        }
        let service_id = service.id.clone();
        let op_id = create_operation(st, "UPDATE_SERVICE", vec![("SERVICE", service_id)]);
        ok_response(json!({ "OperationId": op_id }))
    })
}

// ------------------------------------------------- service attributes

fn get_service_attributes(req: &Request, body: &Value) -> Response {
    let id = js(body, "ServiceId").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        let svc = match find_service(st, &id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let attributes: Map<String, Value> = svc
            .service_attributes
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        ok_response(json!({
            "ServiceAttributes": {
                "ServiceArn": svc.arn,
                "Attributes": attributes,
            }
        }))
    })
}

fn update_service_attributes(req: &Request, body: &Value) -> Response {
    let id = js(body, "ServiceId").unwrap_or_default();
    let attributes = body
        .get("Attributes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        let Some(service) = st.services.get_mut(&id) else {
            return service_not_found(&id);
        };
        for (key, value) in &attributes {
            let value = value.as_str().map(String::from).unwrap_or_default();
            service.service_attributes.insert(key.clone(), value);
        }
        ok_response(json!({}))
    })
}

/// moto iterates the Attributes member and pops its keys — both a list of
/// keys and a key->action map end up behaving the same.
fn delete_service_attributes(req: &Request, body: &Value) -> Response {
    let id = js(body, "ServiceId").unwrap_or_default();
    let attributes: Vec<String> = match body.get("Attributes") {
        Some(Value::Array(list)) => list
            .iter()
            .filter_map(Value::as_str)
            .map(String::from)
            .collect(),
        Some(Value::Object(map)) => map.keys().cloned().collect(),
        _ => Vec::new(),
    };
    with_state(req.account, &req.region, |st| {
        let Some(service) = st.services.get_mut(&id) else {
            return service_not_found(&id);
        };
        for key in &attributes {
            service.service_attributes.remove(key);
        }
        ok_response(json!({}))
    })
}

// ----------------------------------------------------------- instances

fn register_instance(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let instance_id = js(body, "InstanceId").unwrap_or_default();
    let creator_request_id = js(body, "CreatorRequestId")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| random_id(32));
    let attributes = body.get("Attributes").cloned().unwrap_or_else(|| json!({}));
    with_state(req.account, &req.region, |st| {
        if !st.services.contains_key(&service_id) {
            return service_not_found(&service_id);
        }
        let service = st.services.get_mut(&service_id).unwrap();
        let instance = Instance {
            instance_id: instance_id.clone(),
            creator_request_id,
            attributes,
            health_status: "HEALTHY".to_string(),
        };
        // moto keeps duplicates — get_instance answers the first match.
        service.instances.push(instance);
        *service
            .instances_revision
            .entry(instance_id.clone())
            .or_insert(0) += 1;
        let op_id = create_operation(st, "REGISTER_INSTANCE", vec![("INSTANCE", instance_id)]);
        ok_response(json!({ "OperationId": op_id }))
    })
}

fn deregister_instance(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let instance_id = js(body, "InstanceId").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        let Some(service) = st.services.get_mut(&service_id) else {
            return service_not_found(&service_id);
        };
        let Some(index) = service
            .instances
            .iter()
            .position(|i| i.instance_id == instance_id)
        else {
            return instance_not_found(&instance_id);
        };
        service.instances.remove(index);
        *service
            .instances_revision
            .entry(instance_id.clone())
            .or_insert(0) += 1;
        let op_id = create_operation(st, "DEREGISTER_INSTANCE", vec![("INSTANCE", instance_id)]);
        ok_response(json!({ "OperationId": op_id }))
    })
}

fn get_instance(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let instance_id = js(body, "InstanceId").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        let service = match find_service(st, &service_id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match service
            .instances
            .iter()
            .find(|i| i.instance_id == instance_id)
        {
            Some(instance) => ok_response(json!({ "Instance": instance.to_json() })),
            None => instance_not_found(&instance_id),
        }
    })
}

fn list_instances(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let max_results = body.get("MaxResults").and_then(Value::as_u64);
    let next_token = js(body, "NextToken");
    with_state(req.account, &req.region, |st| {
        let svc = match find_service(st, &service_id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let (page, new_token) = paginate(
            svc.instances.iter().map(Instance::to_json).collect(),
            max_results,
            next_token.as_deref(),
        );
        let mut out = json!({ "Instances": page });
        if let Some(token) = new_token {
            out["NextToken"] = json!(token);
        }
        ok_response(out)
    })
}

fn get_instances_health_status(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let max_results = body.get("MaxResults").and_then(Value::as_u64);
    let next_token = js(body, "NextToken");
    with_state(req.account, &req.region, |st| {
        let svc = match find_service(st, &service_id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        // moto raises ServiceNotFound before touching the Instances member.
        let records: Vec<(String, String)> = match body.get("Instances") {
            None => svc
                .instances
                .iter()
                .map(|i| (i.instance_id.clone(), i.health_status.clone()))
                .collect(),
            Some(list) => match list.as_array() {
                Some(list) => svc
                    .instances
                    .iter()
                    .filter(|i| {
                        list.iter()
                            .any(|v| v.as_str() == Some(i.instance_id.as_str()))
                    })
                    .map(|i| (i.instance_id.clone(), i.health_status.clone()))
                    .collect(),
                None => return invalid_input("Instances must be a list"),
            },
        };
        let (page, new_token) = paginate(records, max_results, next_token.as_deref());
        let mut status = Map::new();
        for (id, health) in page {
            status.insert(id, json!(health));
        }
        let mut out = json!({ "Status": status });
        if let Some(token) = new_token {
            out["NextToken"] = json!(token);
        }
        ok_response(out)
    })
}

fn update_instance_custom_health_status(req: &Request, body: &Value) -> Response {
    let service_id = js(body, "ServiceId").unwrap_or_default();
    let instance_id = js(body, "InstanceId").unwrap_or_default();
    let status = js(body, "Status").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        // moto validates the status value before the instance lookup.
        if status != "HEALTHY" && status != "UNHEALTHY" {
            return custom_health_not_found(&service_id);
        }
        let Some(service) = st.services.get_mut(&service_id) else {
            return service_not_found(&service_id);
        };
        let Some(instance) = service
            .instances
            .iter_mut()
            .find(|i| i.instance_id == instance_id)
        else {
            return instance_not_found(&instance_id);
        };
        instance.health_status = status;
        ok_response(json!({}))
    })
}

// ----------------------------------------------------------- discovery

/// moto's `_filter_instances` — health status filter, query parameters
/// (all must match) and best-effort optional parameters.
fn filter_instances(
    all: &[Instance],
    query_parameters: &Map<String, Value>,
    optional_parameters: &Map<String, Value>,
    health_status: &str,
) -> Vec<usize> {
    let mut has_healthy = false;
    let mut kept: Vec<usize> = Vec::new();
    for (idx, instance) in all.iter().enumerate() {
        if health_status != "ALL"
            && health_status != "HEALTHY_OR_ELSE_ALL"
            && instance.health_status != health_status
        {
            continue;
        }
        if instance.health_status == "HEALTHY" {
            has_healthy = true;
        }
        let matches_query = query_parameters.iter().all(|(param, value)| {
            instance
                .attributes
                .as_object()
                .and_then(|a| a.get(param))
                .is_some_and(|v| v == value)
        });
        if !matches_query {
            continue;
        }
        kept.push(idx);
    }
    if has_healthy && health_status == "HEALTHY_OR_ELSE_ALL" {
        kept.retain(|&idx| all[idx].health_status == "HEALTHY");
    }
    // Optional parameters are best-effort: when no instance matches them
    // all, the pre-optional list stands.
    let opt_filtered: Vec<usize> = kept
        .iter()
        .copied()
        .filter(|&idx| {
            optional_parameters.iter().all(|(param, value)| {
                all[idx]
                    .attributes
                    .as_object()
                    .and_then(|a| a.get(param))
                    .is_some_and(|v| v == value)
            })
        })
        .collect();
    if opt_filtered.is_empty() {
        kept
    } else {
        opt_filtered
    }
}

/// Locate (namespace, service) by name the way moto's discover backend
/// does, validating HealthStatus first. Returns cloned instance lists so
/// the state lock is not held across response building.
fn discover_target(
    st: &State,
    namespace_name: &Option<String>,
    service_name: &Option<String>,
    health_status: &str,
) -> Result<(Vec<Instance>, BTreeMap<String, u64>), Response> {
    if !matches!(
        health_status,
        "HEALTHY" | "UNHEALTHY" | "ALL" | "HEALTHY_OR_ELSE_ALL"
    ) {
        return Err(invalid_input("Invalid health status"));
    }
    let Some(namespace) = st.namespaces.values().find(|ns| ns.name == *namespace_name) else {
        return Err(namespace_not_found(
            namespace_name.as_deref().unwrap_or_default(),
        ));
    };
    let Some(service) = st.services.values().find(|s| {
        // moto: service name AND namespace id must match the request.
        s.name == *service_name && s.namespace_id.as_deref() == Some(namespace.id.as_str())
    }) else {
        return Err(service_not_found(
            service_name.as_deref().unwrap_or_default(),
        ));
    };
    Ok((
        service.instances.clone(),
        service.instances_revision.clone(),
    ))
}

fn discover_instances(req: &Request, body: &Value) -> Response {
    let namespace_name = js(body, "NamespaceName");
    let service_name = js(body, "ServiceName");
    let health_status = js(body, "HealthStatus").unwrap_or_else(|| "ALL".to_string());
    let query_parameters = object_at(body, "QueryParameters");
    let optional_parameters = object_at(body, "OptionalParameters");
    let max_results = body.get("MaxResults").and_then(Value::as_u64);
    with_state(req.account, &req.region, |st| {
        let (instances, revisions) =
            match discover_target(st, &namespace_name, &service_name, &health_status) {
                Ok(found) => found,
                Err(e) => return e,
            };
        let keep = filter_instances(
            &instances,
            &query_parameters,
            &optional_parameters,
            &health_status,
        );
        // moto paginates after filtering; the handler ignores NextToken.
        let (page, _new_token) = paginate(keep, max_results, None);
        let mut out: Vec<Value> = Vec::new();
        let mut revision_total: u64 = 0;
        for idx in page {
            let instance = &instances[idx];
            revision_total += revisions.get(&instance.instance_id).copied().unwrap_or(0);
            out.push(json!({
                "InstanceId": instance.instance_id,
                "NamespaceName": namespace_name,
                "ServiceName": service_name,
                "Attributes": instance.attributes,
                "HealthStatus": instance.health_status,
            }));
        }
        ok_response(json!({
            "Instances": out,
            "InstancesRevision": revision_total,
        }))
    })
}

fn discover_instances_revision(req: &Request, body: &Value) -> Response {
    let namespace_name = js(body, "NamespaceName");
    let service_name = js(body, "ServiceName");
    let health_status = js(body, "HealthStatus").unwrap_or_else(|| "ALL".to_string());
    let query_parameters = object_at(body, "QueryParameters");
    let optional_parameters = object_at(body, "OptionalParameters");
    with_state(req.account, &req.region, |st| {
        let (instances, revisions) =
            match discover_target(st, &namespace_name, &service_name, &health_status) {
                Ok(found) => found,
                Err(e) => return e,
            };
        let keep = filter_instances(
            &instances,
            &query_parameters,
            &optional_parameters,
            &health_status,
        );
        // moto sums the full (unpaginated) revision map of the filtered set.
        let total: u64 = keep
            .iter()
            .map(|idx| {
                revisions
                    .get(&instances[*idx].instance_id)
                    .copied()
                    .unwrap_or(0)
            })
            .sum();
        ok_response(json!({ "InstancesRevision": total }))
    })
}

// --------------------------------------------------------------- tags

fn tag_resource_op(req: &Request, body: &Value) -> Response {
    let arn = js(body, "ResourceARN").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        tag_resource(st, &arn, body.get("Tags"));
        ok_response(json!({}))
    })
}

fn untag_resource_op(req: &Request, body: &Value) -> Response {
    let arn = js(body, "ResourceARN").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        untag_resource(st, &arn, body.get("TagKeys"));
        ok_response(json!({}))
    })
}

fn list_tags_for_resource(req: &Request, body: &Value) -> Response {
    let arn = js(body, "ResourceARN").unwrap_or_default();
    with_state(req.account, &req.region, |st| {
        ok_response(list_tags(st, &arn))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;
    use std::collections::HashMap;

    /// Test-local dispatch with the same call shape; `servicediscovery`
    /// dispatch is always `Some` for its own service, so unwrap it.
    fn dispatch(req: &Request) -> Response {
        super::dispatch(req).expect("servicediscovery dispatch handled the request")
    }

    // Each test gets its own account for in-memory isolation.
    fn fmt_req(account: u64, op: &str, body: Value) -> Request {
        Request {
            service: "servicediscovery".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: crate::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body_of(r: &Response) -> Value {
        serde_json::from_slice(&r.body).unwrap()
    }

    fn error_code(r: &Response) -> String {
        body_of(r)["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    fn create_http_namespace(account: u64, name: &str) -> (String, String) {
        let r = dispatch(&fmt_req(
            account,
            "CreateHttpNamespace",
            json!({"Name": name}),
        ));
        assert_eq!(r.status, 200);
        let op_id = body_of(&r)["OperationId"].as_str().unwrap().to_string();
        let op = dispatch(&fmt_req(
            account,
            "GetOperation",
            json!({"OperationId": op_id}),
        ));
        assert_eq!(op.status, 200);
        let targets = &body_of(&op)["Operation"]["Targets"];
        (op_id, targets["NAMESPACE"].as_str().unwrap().to_string())
    }

    fn create_service(account: u64, name: &str, ns_id: &str) -> Value {
        let r = dispatch(&fmt_req(
            account,
            "CreateService",
            json!({"Name": name, "NamespaceId": ns_id}),
        ));
        assert_eq!(r.status, 200);
        body_of(&r)["Service"].clone()
    }

    fn register(account: u64, service_id: &str, instance_id: &str, attrs: Value) {
        let r = dispatch(&fmt_req(
            account,
            "RegisterInstance",
            json!({"ServiceId": service_id, "InstanceId": instance_id, "Attributes": attrs}),
        ));
        assert_eq!(r.status, 200, "register failed: {}", r.status);
    }

    #[test]
    fn namespace_round_trip() {
        let a = 111111110001;
        let name = "ns-roundtrip.local";
        let (op_id, ns_id) = create_http_namespace(a, name);

        let ns = dispatch(&fmt_req(a, "GetNamespace", json!({"Id": ns_id})));
        assert_eq!(ns.status, 200);
        let ns_json = body_of(&ns)["Namespace"].clone();
        assert_eq!(ns_json["Id"], json!(ns_id));
        assert_eq!(ns_json["Name"], json!(name));
        assert_eq!(ns_json["Type"], json!("HTTP"));
        assert_eq!(
            ns_json["Properties"]["HttpProperties"]["HttpName"],
            json!(name)
        );
        assert!(ns_json["Arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:servicediscovery:"));

        let list = dispatch(&fmt_req(a, "ListNamespaces", json!({})));
        let ids: Vec<String> = body_of(&list)["Namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["Id"].as_str().unwrap().to_string())
            .collect();
        assert!(ids.contains(&ns_id));

        let op = dispatch(&fmt_req(a, "GetOperation", json!({"OperationId": op_id})));
        assert_eq!(body_of(&op)["Operation"]["Status"], json!("SUCCESS"));

        let del = dispatch(&fmt_req(a, "DeleteNamespace", json!({"Id": ns_id})));
        assert_eq!(del.status, 200);
        let gone = dispatch(&fmt_req(a, "GetNamespace", json!({"Id": ns_id})));
        assert_eq!(gone.status, 400);
        assert_eq!(error_code(&gone), "NamespaceNotFound");
    }

    #[test]
    fn service_round_trip_with_tags() {
        let a = 111111110002;
        let (_, ns_id) = create_http_namespace(a, "svc-rt.local");
        let svc = create_service(a, "svc-rt", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();
        assert!(svc["Arn"].as_str().unwrap().contains("service/"));
        // Fields moto always serializes, null when unset.
        assert!(svc["DnsConfig"].is_null());
        assert!(svc["Type"].is_null());

        let get = dispatch(&fmt_req(a, "GetService", json!({"Id": service_id})));
        assert_eq!(body_of(&get)["Service"]["Name"], json!("svc-rt"));

        // tags
        let arn = svc["Arn"].as_str().unwrap().to_string();
        dispatch(&fmt_req(
            a,
            "TagResource",
            json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "t"}]}),
        ));
        let tags = dispatch(&fmt_req(
            a,
            "ListTagsForResource",
            json!({"ResourceARN": arn}),
        ));
        assert_eq!(body_of(&tags)["Tags"][0]["Key"], json!("env"));
        dispatch(&fmt_req(
            a,
            "UntagResource",
            json!({"ResourceARN": arn, "TagKeys": ["env"]}),
        ));
        let tags = dispatch(&fmt_req(
            a,
            "ListTagsForResource",
            json!({"ResourceARN": arn}),
        ));
        assert_eq!(body_of(&tags)["Tags"], json!([]));

        let del = dispatch(&fmt_req(a, "DeleteService", json!({"Id": service_id})));
        assert_eq!(del.status, 200);
        let list = dispatch(&fmt_req(a, "ListServices", json!({})));
        let ids: Vec<String> = body_of(&list)["Services"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["Id"].as_str().unwrap().to_string())
            .collect();
        assert!(!ids.contains(&service_id));

        // delete_service is silent for unknown ids (moto quirk)
        let silent = dispatch(&fmt_req(a, "DeleteService", json!({"Id": "srv-999"})));
        assert_eq!(silent.status, 200);
    }

    #[test]
    fn update_service_semantics() {
        let a = 111111110003;
        let (_, ns_id) = create_http_namespace(a, "upd-svc.local");
        let svc = create_service(a, "upd-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();

        let upd = dispatch(&fmt_req(
            a,
            "UpdateService",
            json!({"Id": service_id, "Service": {
                "Description": "d1",
                "DnsConfig": {"DnsRecords": [{"Type": "A", "TTL": 60}]}
            }}),
        ));
        assert_eq!(upd.status, 200);
        assert!(body_of(&upd)["OperationId"].as_str().is_some());

        let get = dispatch(&fmt_req(a, "GetService", json!({"Id": service_id})));
        let svc_json = body_of(&get)["Service"].clone();
        assert_eq!(svc_json["Description"], json!("d1"));
        assert_eq!(svc_json["DnsConfig"]["DnsRecords"][0]["TTL"], json!(60));

        // Omitting DnsConfig deletes it (moto mirrors the documented AWS rule).
        dispatch(&fmt_req(
            a,
            "UpdateService",
            json!({"Id": service_id, "Service": {}}),
        ));
        let get = dispatch(&fmt_req(a, "GetService", json!({"Id": service_id})));
        assert!(body_of(&get)["Service"]["DnsConfig"].is_null());

        let missing = dispatch(&fmt_req(
            a,
            "UpdateService",
            json!({"Id": "srv-none", "Service": {}}),
        ));
        assert_eq!(missing.status, 400);
        assert_eq!(error_code(&missing), "ServiceNotFound");
    }

    #[test]
    fn instances_register_get_list_deregister() {
        let a = 111111110004;
        let (_, ns_id) = create_http_namespace(a, "inst.local");
        let svc = create_service(a, "inst-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();

        register(
            a,
            &service_id,
            "i-1",
            json!({"AWS_INSTANCE_IPV4": "10.0.0.1"}),
        );
        register(a, &service_id, "i-2", json!({"AWS_INSTANCE_PORT": "80"}));

        let got = dispatch(&fmt_req(
            a,
            "GetInstance",
            json!({"ServiceId": service_id, "InstanceId": "i-1"}),
        ));
        assert_eq!(
            body_of(&got)["Instance"]["Attributes"]["AWS_INSTANCE_IPV4"],
            json!("10.0.0.1")
        );
        assert!(body_of(&got)["Instance"]["CreatorRequestId"]
            .as_str()
            .is_some());

        let list = dispatch(&fmt_req(
            a,
            "ListInstances",
            json!({"ServiceId": service_id}),
        ));
        let ids: Vec<String> = body_of(&list)["Instances"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["Id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["i-1".to_string(), "i-2".to_string()]);

        let dereg = dispatch(&fmt_req(
            a,
            "DeregisterInstance",
            json!({"ServiceId": service_id, "InstanceId": "i-1"}),
        ));
        assert!(body_of(&dereg)["OperationId"].as_str().is_some());
        let gone = dispatch(&fmt_req(
            a,
            "GetInstance",
            json!({"ServiceId": service_id, "InstanceId": "i-1"}),
        ));
        assert_eq!(error_code(&gone), "InstanceNotFound");

        let bad_service = dispatch(&fmt_req(
            a,
            "GetInstance",
            json!({"ServiceId": "srv-none", "InstanceId": "i-1"}),
        ));
        assert_eq!(error_code(&bad_service), "ServiceNotFound");
    }

    #[test]
    fn update_instance_custom_health_status() {
        let a = 111111110005;
        let (_, ns_id) = create_http_namespace(a, "health.local");
        let svc = create_service(a, "health-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();
        register(a, &service_id, "h-1", json!({}));

        let upd = dispatch(&fmt_req(
            a,
            "UpdateInstanceCustomHealthStatus",
            json!({"ServiceId": service_id, "InstanceId": "h-1", "Status": "UNHEALTHY"}),
        ));
        assert_eq!(upd.status, 200);
        let health = dispatch(&fmt_req(
            a,
            "GetInstancesHealthStatus",
            json!({"ServiceId": service_id}),
        ));
        assert_eq!(body_of(&health)["Status"]["h-1"], json!("UNHEALTHY"));
    }

    #[test]
    fn discovery_round_trip() {
        let a = 111111110006;
        let ns_name = "disc.local";
        let (_, ns_id) = create_http_namespace(a, ns_name);
        let svc = create_service(a, "disc-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();
        register(
            a,
            &service_id,
            "d-1",
            json!({"AWS_INSTANCE_IPV4": "10.0.0.9", "AWS_INSTANCE_PORT": "8080"}),
        );
        register(
            a,
            &service_id,
            "d-2",
            json!({"AWS_INSTANCE_IPV4": "10.0.0.10"}),
        );

        let disc = dispatch(&fmt_req(
            a,
            "DiscoverInstances",
            json!({"NamespaceName": ns_name, "ServiceName": "disc-svc"}),
        ));
        assert_eq!(disc.status, 200);
        let out = body_of(&disc);
        assert_eq!(out["Instances"].as_array().unwrap().len(), 2);
        assert_eq!(out["InstancesRevision"], json!(2));
        assert_eq!(out["Instances"][0]["HealthStatus"], json!("HEALTHY"));

        let filtered = dispatch(&fmt_req(
            a,
            "DiscoverInstances",
            json!({
                "NamespaceName": ns_name,
                "ServiceName": "disc-svc",
                "QueryParameters": {"AWS_INSTANCE_PORT": "8080"},
                "MaxResults": 1
            }),
        ));
        let fout = body_of(&filtered);
        assert_eq!(fout["Instances"].as_array().unwrap().len(), 1);
        assert_eq!(fout["Instances"][0]["InstanceId"], json!("d-1"));

        // InstancesRevision ignores MaxResults (moto paginates instances but
        // sums only the returned page's revisions... actually the returned
        // page only).
        let rev = dispatch(&fmt_req(
            a,
            "DiscoverInstancesRevision",
            json!({"NamespaceName": ns_name, "ServiceName": "disc-svc"}),
        ));
        assert_eq!(body_of(&rev)["InstancesRevision"], json!(2));

        let bad = dispatch(&fmt_req(
            a,
            "DiscoverInstances",
            json!({
                "NamespaceName": ns_name,
                "ServiceName": "disc-svc",
                "HealthStatus": "Bogus"
            }),
        ));
        assert_eq!(bad.status, 400);
        assert_eq!(error_code(&bad), "InvalidInput");

        let no_such = dispatch(&fmt_req(
            a,
            "DiscoverInstances",
            json!({"NamespaceName": "gone.local", "ServiceName": "disc-svc"}),
        ));
        assert_eq!(error_code(&no_such), "NamespaceNotFound");
    }

    #[test]
    fn service_attribute_ops() {
        let a = 111111110007;
        let (_, ns_id) = create_http_namespace(a, "attrs.local");
        let svc = create_service(a, "attrs-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();

        dispatch(&fmt_req(
            a,
            "UpdateServiceAttributes",
            json!({"ServiceId": service_id, "Attributes": {"k": "v"}}),
        ));
        let get = dispatch(&fmt_req(
            a,
            "GetServiceAttributes",
            json!({"ServiceId": service_id}),
        ));
        let attrs = body_of(&get)["ServiceAttributes"].clone();
        assert_eq!(attrs["ServiceArn"], svc["Arn"]);
        assert_eq!(attrs["Attributes"]["k"], json!("v"));

        dispatch(&fmt_req(
            a,
            "DeleteServiceAttributes",
            json!({"ServiceId": service_id, "Attributes": {"k": "REMOVE"}}),
        ));
        let get = dispatch(&fmt_req(
            a,
            "GetServiceAttributes",
            json!({"ServiceId": service_id}),
        ));
        assert_eq!(body_of(&get)["ServiceAttributes"]["Attributes"], json!({}));

        let missing = dispatch(&fmt_req(
            a,
            "GetServiceAttributes",
            json!({"ServiceId": "srv-none"}),
        ));
        assert_eq!(missing.status, 400);
        assert_eq!(error_code(&missing), "ServiceNotFound");
    }

    #[test]
    fn namespace_updates_and_errors() {
        let a = 111111110008;
        let (_, ns_id) = create_http_namespace(a, "upd.local");

        // lowercase operationId — moto bug preserved
        let upd = dispatch(&fmt_req(
            a,
            "UpdateHttpNamespace",
            json!({"Id": ns_id, "Namespace": {"Description": "updated"}}),
        ));
        assert_eq!(upd.status, 200);
        assert!(body_of(&upd)["operationId"].as_str().is_some());
        assert!(body_of(&upd).get("OperationId").is_none());

        let no_desc = dispatch(&fmt_req(
            a,
            "UpdateHttpNamespace",
            json!({"Id": ns_id, "Namespace": {}}),
        ));
        assert_eq!(error_code(&no_desc), "InvalidInput");

        let not_found = dispatch(&fmt_req(
            a,
            "UpdateHttpNamespace",
            json!({"Id": "ns-none", "Namespace": {"Description": "x"}}),
        ));
        assert_eq!(error_code(&not_found), "NamespaceNotFound");
    }

    #[test]
    fn private_dns_namespace_and_conflicts() {
        let a = 111111110009;
        let create = dispatch(&fmt_req(
            a,
            "CreatePrivateDnsNamespace",
            json!({"Name": "priv.local", "Vpc": "vpc-1"}),
        ));
        assert_eq!(create.status, 200);
        let op_id = body_of(&create)["OperationId"]
            .as_str()
            .unwrap()
            .to_string();
        let op = dispatch(&fmt_req(a, "GetOperation", json!({"OperationId": op_id})));
        let ns_id = body_of(&op)["Operation"]["Targets"]["NAMESPACE"]
            .as_str()
            .unwrap()
            .to_string();

        let ns = dispatch(&fmt_req(a, "GetNamespace", json!({"Id": ns_id})));
        let ns_json = body_of(&ns)["Namespace"].clone();
        assert_eq!(ns_json["Type"], json!("DNS_PRIVATE"));
        assert!(ns_json["Properties"]["DnsProperties"]["HostedZoneId"].is_string());

        let conflict = dispatch(&fmt_req(
            a,
            "CreatePrivateDnsNamespace",
            json!({"Name": "other.local", "Vpc": "vpc-1"}),
        ));
        assert_eq!(error_code(&conflict), "ConflictingDomainExists");

        let upd = dispatch(&fmt_req(
            a,
            "UpdatePrivateDnsNamespace",
            json!({"Id": ns_id, "Namespace": {"Description": "d"}}),
        ));
        assert!(body_of(&upd)["OperationId"].as_str().is_some());
    }

    #[test]
    fn list_operations_prunes_without_namespace_target() {
        let a = 111111110010;
        let (_, ns_id) = create_http_namespace(a, "ops.local");
        let svc = create_service(a, "ops-svc", &ns_id);
        let service_id = svc["Id"].as_str().unwrap().to_string();
        // UPDATE_SERVICE operation has no NAMESPACE target.
        dispatch(&fmt_req(
            a,
            "UpdateService",
            json!({"Id": service_id, "Service": {"Description": "d"}}),
        ));
        let list = dispatch(&fmt_req(a, "ListOperations", json!({})));
        let ops = body_of(&list)["Operations"].as_array().cloned().unwrap();
        assert!(ops.iter().all(|o| o["Id"].as_str().is_some()));

        let unknown = dispatch(&fmt_req(
            a,
            "GetOperation",
            json!({"OperationId": "not-an-operation"}),
        ));
        assert_eq!(unknown.status, 400);
        assert_eq!(error_code(&unknown), "OperationNotFound");
        assert_eq!(body_of(&unknown)["message"], json!(""));
    }

    #[test]
    fn unknown_op_is_501() {
        let r = dispatch(&fmt_req(111111110011, "NonsenseOp", json!({})));
        assert_eq!(r.status, 501);
    }

    #[test]
    fn account_isolation() {
        let a = 111111110012;
        let b = 111111110013;
        create_http_namespace(a, "only-a.local");
        let list = dispatch(&fmt_req(b, "ListNamespaces", json!({})));
        let names: Vec<String> = body_of(&list)["Namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["Name"].as_str().unwrap_or_default().to_string())
            .collect();
        assert!(!names.contains(&"only-a.local".to_string()));
    }
}
