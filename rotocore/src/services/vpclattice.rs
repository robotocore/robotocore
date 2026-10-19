//! AWS VPC Lattice backend (REST-JSON; botocore route table per (method, URI)).
//!
//! Port of moto's `vpclattice` backend (models.py/responses.py/urls.py):
//! services, service networks, service-network/vpc associations, rules,
//! access log subscriptions, auth policies, resource policies and tagging.
//! Wire shapes are moto's `to_dict` dicts; id prefixes (`svc-`, `sn-`,
//! `als-`) mirror moto's generators, including moto's rule id which renders
//! the literal string `rule-[0-9a-z]{17}`.
//!
//! Routing is moto's two-layer model: a request only reaches the backend
//! when the path matches one of moto's `urls.py` gate patterns
//! (`/services/**`, `/servicenetworks/**`, `/servicenetworkvpcassociations`,
//! `/accesslogsubscriptions/**`, `/tags/**`, `/authpolicy/**`,
//! `/resourcepolicy/**`); the operation is then resolved from HTTP method +
//! botocore requestUri. Resolved operations without a moto handler answer
//! moto's raised `NotImplementedError` (501); paths the urls.py gate never
//! routes answer moto's 404 Empty. moto's list-services reads `MaxResults`/
//! `NextToken` while the wire carries `maxResults`/`nextToken`, so those
//! lists always serve up to the default 50 items and ignore any token; the
//! token they mint follows botocore's encoder (base64 JSON of the first
//! excluded item's unique attribute).
//!
//! Lookup-by-ARN follows moto's handlers: the full path is decoded and the
//! id taken after the last `/`, so `arn:…:service/svc-x` resolves as `svc-x`.

use crate::proto::url_decode;
use crate::store;
use crate::{Request, Response};
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct ServiceEntry {
    id: String,
    arn: String,
    name: Value,
    auth_type: Value,
    certificate_arn: Option<String>,
    custom_domain_name: Option<String>,
    dns_domain: String,
    dns_hosted_zone: String,
}

impl ServiceEntry {
    /// moto's `VPCLatticeService.to_dict` (key insertion order).
    fn to_wire(&self) -> Value {
        json!({
            "arn": self.arn,
            "authType": self.auth_type,
            "certificateArn": self.certificate_arn.clone().unwrap_or_default(),
            "customDomainName": self.custom_domain_name.clone().unwrap_or_default(),
            "dnsEntry": {
                "domainName": self.dns_domain,
                "hostedZoneId": self.dns_hosted_zone,
            },
            "id": self.id,
            "name": self.name,
            "status": "ACTIVE",
        })
    }
}

struct ServiceNetworkEntry {
    id: String,
    arn: String,
    name: Value,
    auth_type: Value,
    sharing_config: Value,
}

impl ServiceNetworkEntry {
    /// moto's `VPCLatticeServiceNetwork.to_dict`.
    fn to_wire(&self) -> Value {
        json!({
            "arn": self.arn,
            "authType": self.auth_type,
            "id": self.id,
            "name": self.name,
            "sharingConfig": self.sharing_config,
        })
    }
}

struct SnvaEntry {
    id: String,
    arn: String,
    security_group_ids: Vec<Value>,
    tags: Value,
}

impl SnvaEntry {
    /// moto's `VPCLatticeServiceNetworkVpcAssociation.to_dict`.
    fn to_wire(&self) -> Value {
        json!({
            "arn": self.arn,
            "createdBy": "user",
            "id": self.id,
            "securityGroupIds": self.security_group_ids,
            "status": "ACTIVE",
            "tags": self.tags,
        })
    }
}

struct RuleEntry {
    id: String,
    arn: String,
    name: Value,
    priority: Value,
    action: Value,
    match_value: Value,
    tags: Value,
}

struct AlsEntry {
    id: String,
    arn: String,
    created_at: String,
    destination_arn: Value,
    last_updated_at: String,
    resource_arn: String,
    resource_id: String,
    log_type: Value,
    tags: Value,
}

impl AlsEntry {
    /// moto's `VPCLatticeAccessLogSubscription.to_dict`.
    fn to_wire(&self) -> Value {
        json!({
            "arn": self.arn,
            "createdAt": self.created_at,
            "destinationArn": self.destination_arn,
            "id": self.id,
            "lastUpdatedAt": self.last_updated_at,
            "resourceArn": self.resource_arn,
            "resourceId": self.resource_id,
            "serviceNetworkLogType": self.log_type,
            "tags": self.tags,
        })
    }
}

struct AuthPolicy {
    policy: Value,
    created_at: String,
    last_updated_at: String,
}

#[derive(Default)]
struct State {
    services: Vec<ServiceEntry>,
    service_networks: Vec<ServiceNetworkEntry>,
    snvas: Vec<SnvaEntry>,
    /// moto keys every rule by the same literal id (`rule-[0-9a-z]{17}`),
    /// so a new rule replaces the previous one.
    rules: Vec<RuleEntry>,
    access_log_subscriptions: Vec<AlsEntry>,
    /// resource ids trimmed of their ARN prefix (`svc-…`/`sn-…`) -> policy.
    auth_policies: BTreeMap<String, AuthPolicy>,
    /// resourceArn -> inline policy document (moto resource_policies).
    resource_policies: BTreeMap<String, Value>,
    /// moto's TaggingService: ARN -> {tag key -> raw value}.
    tags: BTreeMap<String, BTreeMap<String, Value>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("vpclattice"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn empty_ok() -> Response {
    ok(&json!({}))
}

/// moto's `JsonRESTError`: `{"__type": code, "message": msg}` (HTTP 400).
fn error(status: u16, code: &str, message: &str) -> Response {
    let body = json!({ "__type": code, "message": message });
    let mut resp = Response::json(status, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

fn not_found(message: impl AsRef<str>) -> Response {
    error(400, "ResourceNotFoundException", message.as_ref())
}

fn validation(message: impl AsRef<str>) -> Response {
    error(400, "ValidationException", message.as_ref())
}

/// moto raises `NotImplementedError("The <snake action> action has not been
/// implemented")` in `call_action` when the routed operation has no handler.
fn not_implemented(snake_action: &str) -> Response {
    error(
        501,
        "NotImplemented",
        &format!("The {snake_action} action has not been implemented"),
    )
}

/// moto's `call_action` answers `404, headers, ""` when the URI resolves to
/// no botocore operation — unknown paths behind the urls.py gate included.
fn unrouted() -> Response {
    Response::new(404, "text/html", Vec::new())
}

/// moto's `camelcase_to_underscores`: `CreateServiceNetwork` ->
/// `create_service_network`.
fn snake_case(op: &str) -> String {
    let mut out = String::with_capacity(op.len() + 4);
    for (i, c) in op.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Python's `datetime.now(timezone.utc).isoformat()` shape.
fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
        .to_string()
}

/// moto's id generators: `f"svc-{str(uuid.uuid4())[:17]}"` (17 chars of the
/// hyphenated uuid4 form).
fn prefixed_id(prefix: &str) -> String {
    let u = uuid::Uuid::new_v4().to_string();
    format!("{prefix}{}", &u[..17])
}

/// moto's `VPCLatticeDNSEntry.hosted_zone_id`: `Z{rand(100000..999999)}XYZ`,
/// seeded from a fresh uuid instead of the random module.
fn random_hosted_zone() -> String {
    let u = uuid::Uuid::new_v4();
    let n = u.as_u128() % 900_000 + 100_000;
    format!("Z{n}XYZ")
}

/// Take the value after the last `/` of the decoded path (moto's
/// `unquote(self.path).split("/")[-1]`): passing an ARN yields its id.
fn last_path_segment(path: &str) -> String {
    url_decode(path)
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// The first `{param}` path segment after `prefix`, raw (botocore URI labels
/// are non-greedy `[^/]+`).
fn path_segment_after<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    path.strip_prefix(prefix)
        .map(|rest| rest.split('/').next().unwrap_or(""))
        .filter(|seg| !seg.is_empty())
}

/// moto reads path params via `_get_param` (one unquote) and its vpclattice
/// handlers unquote again — mirror the double decode on the raw segment.
fn double_decode(seg: &str) -> String {
    url_decode(&url_decode(seg))
}

// ---------------------------------------------------------------------------
// moto urls.py gate + botocore operation resolution
// ---------------------------------------------------------------------------

/// moto's urls.py gate (werkzeug rules, strict slashes off): the request
/// only reaches `VPCLatticeResponse.dispatch` through one of these paths.
/// The `/tags/…` etc. rules carry greedy `.+` labels and span slashes.
fn passes_url_gate(path: &str) -> bool {
    let prefixed_nonempty = |p: &str| path.starts_with(p) && path.len() > p.len();
    path == "/accesslogsubscriptions"
        || prefixed_nonempty("/accesslogsubscriptions/")
        || path == "/services"
        || prefixed_nonempty("/services/")
        || path == "/servicenetworks"
        || prefixed_nonempty("/servicenetworks/")
        || path == "/servicenetworkvpcassociations"
        || prefixed_nonempty("/tags/")
        || prefixed_nonempty("/authpolicy/")
        || prefixed_nonempty("/resourcepolicy/")
}

/// (method, botocore requestUri template, operation) for every vpc-lattice
/// operation in the botocore model.
const ROUTES: &[(&str, &str, &str)] = &[
    (
        "PATCH",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules",
        "BatchUpdateRule",
    ),
    (
        "POST",
        "/accesslogsubscriptions",
        "CreateAccessLogSubscription",
    ),
    (
        "POST",
        "/services/{serviceIdentifier}/listeners",
        "CreateListener",
    ),
    (
        "POST",
        "/resourceconfigurations",
        "CreateResourceConfiguration",
    ),
    ("POST", "/resourcegateways", "CreateResourceGateway"),
    (
        "POST",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules",
        "CreateRule",
    ),
    ("POST", "/services", "CreateService"),
    ("POST", "/servicenetworks", "CreateServiceNetwork"),
    (
        "POST",
        "/servicenetworkresourceassociations",
        "CreateServiceNetworkResourceAssociation",
    ),
    (
        "POST",
        "/servicenetworkserviceassociations",
        "CreateServiceNetworkServiceAssociation",
    ),
    (
        "POST",
        "/servicenetworkvpcassociations",
        "CreateServiceNetworkVpcAssociation",
    ),
    ("POST", "/targetgroups", "CreateTargetGroup"),
    (
        "DELETE",
        "/accesslogsubscriptions/{accessLogSubscriptionIdentifier}",
        "DeleteAccessLogSubscription",
    ),
    (
        "DELETE",
        "/authpolicy/{resourceIdentifier}",
        "DeleteAuthPolicy",
    ),
    (
        "DELETE",
        "/domainverifications/{domainVerificationIdentifier}",
        "DeleteDomainVerification",
    ),
    (
        "DELETE",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}",
        "DeleteListener",
    ),
    (
        "DELETE",
        "/resourceconfigurations/{resourceConfigurationIdentifier}",
        "DeleteResourceConfiguration",
    ),
    (
        "DELETE",
        "/resourceendpointassociations/{resourceEndpointAssociationIdentifier}",
        "DeleteResourceEndpointAssociation",
    ),
    (
        "DELETE",
        "/resourcegateways/{resourceGatewayIdentifier}",
        "DeleteResourceGateway",
    ),
    (
        "DELETE",
        "/resourcepolicy/{resourceArn}",
        "DeleteResourcePolicy",
    ),
    (
        "DELETE",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules/{ruleIdentifier}",
        "DeleteRule",
    ),
    ("DELETE", "/services/{serviceIdentifier}", "DeleteService"),
    (
        "DELETE",
        "/servicenetworks/{serviceNetworkIdentifier}",
        "DeleteServiceNetwork",
    ),
    (
        "DELETE",
        "/servicenetworkresourceassociations/{serviceNetworkResourceAssociationIdentifier}",
        "DeleteServiceNetworkResourceAssociation",
    ),
    (
        "DELETE",
        "/servicenetworkserviceassociations/{serviceNetworkServiceAssociationIdentifier}",
        "DeleteServiceNetworkServiceAssociation",
    ),
    (
        "DELETE",
        "/servicenetworkvpcassociations/{serviceNetworkVpcAssociationIdentifier}",
        "DeleteServiceNetworkVpcAssociation",
    ),
    (
        "DELETE",
        "/targetgroups/{targetGroupIdentifier}",
        "DeleteTargetGroup",
    ),
    (
        "POST",
        "/targetgroups/{targetGroupIdentifier}/deregistertargets",
        "DeregisterTargets",
    ),
    (
        "GET",
        "/accesslogsubscriptions/{accessLogSubscriptionIdentifier}",
        "GetAccessLogSubscription",
    ),
    ("GET", "/authpolicy/{resourceIdentifier}", "GetAuthPolicy"),
    (
        "GET",
        "/domainverifications/{domainVerificationIdentifier}",
        "GetDomainVerification",
    ),
    (
        "GET",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}",
        "GetListener",
    ),
    (
        "GET",
        "/resourceconfigurations/{resourceConfigurationIdentifier}",
        "GetResourceConfiguration",
    ),
    (
        "GET",
        "/resourcegateways/{resourceGatewayIdentifier}",
        "GetResourceGateway",
    ),
    ("GET", "/resourcepolicy/{resourceArn}", "GetResourcePolicy"),
    (
        "GET",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules/{ruleIdentifier}",
        "GetRule",
    ),
    ("GET", "/services/{serviceIdentifier}", "GetService"),
    (
        "GET",
        "/servicenetworks/{serviceNetworkIdentifier}",
        "GetServiceNetwork",
    ),
    (
        "GET",
        "/servicenetworkresourceassociations/{serviceNetworkResourceAssociationIdentifier}",
        "GetServiceNetworkResourceAssociation",
    ),
    (
        "GET",
        "/servicenetworkserviceassociations/{serviceNetworkServiceAssociationIdentifier}",
        "GetServiceNetworkServiceAssociation",
    ),
    (
        "GET",
        "/servicenetworkvpcassociations/{serviceNetworkVpcAssociationIdentifier}",
        "GetServiceNetworkVpcAssociation",
    ),
    (
        "GET",
        "/targetgroups/{targetGroupIdentifier}",
        "GetTargetGroup",
    ),
    (
        "GET",
        "/accesslogsubscriptions",
        "ListAccessLogSubscriptions",
    ),
    ("GET", "/domainverifications", "ListDomainVerifications"),
    (
        "GET",
        "/services/{serviceIdentifier}/listeners",
        "ListListeners",
    ),
    (
        "GET",
        "/resourceconfigurations",
        "ListResourceConfigurations",
    ),
    (
        "GET",
        "/resourceendpointassociations",
        "ListResourceEndpointAssociations",
    ),
    ("GET", "/resourcegateways", "ListResourceGateways"),
    (
        "GET",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules",
        "ListRules",
    ),
    (
        "GET",
        "/servicenetworkresourceassociations",
        "ListServiceNetworkResourceAssociations",
    ),
    (
        "GET",
        "/servicenetworkserviceassociations",
        "ListServiceNetworkServiceAssociations",
    ),
    (
        "GET",
        "/servicenetworkvpcassociations",
        "ListServiceNetworkVpcAssociations",
    ),
    (
        "GET",
        "/servicenetworkvpcendpointassociations",
        "ListServiceNetworkVpcEndpointAssociations",
    ),
    ("GET", "/servicenetworks", "ListServiceNetworks"),
    ("GET", "/services", "ListServices"),
    ("GET", "/tags/{resourceArn}", "ListTagsForResource"),
    ("GET", "/targetgroups", "ListTargetGroups"),
    (
        "POST",
        "/targetgroups/{targetGroupIdentifier}/listtargets",
        "ListTargets",
    ),
    ("PUT", "/authpolicy/{resourceIdentifier}", "PutAuthPolicy"),
    ("PUT", "/resourcepolicy/{resourceArn}", "PutResourcePolicy"),
    (
        "POST",
        "/targetgroups/{targetGroupIdentifier}/registertargets",
        "RegisterTargets",
    ),
    ("POST", "/domainverifications", "StartDomainVerification"),
    ("POST", "/tags/{resourceArn}", "TagResource"),
    ("DELETE", "/tags/{resourceArn}", "UntagResource"),
    (
        "PATCH",
        "/accesslogsubscriptions/{accessLogSubscriptionIdentifier}",
        "UpdateAccessLogSubscription",
    ),
    (
        "PATCH",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}",
        "UpdateListener",
    ),
    (
        "PATCH",
        "/resourceconfigurations/{resourceConfigurationIdentifier}",
        "UpdateResourceConfiguration",
    ),
    (
        "PATCH",
        "/resourcegateways/{resourceGatewayIdentifier}",
        "UpdateResourceGateway",
    ),
    (
        "PATCH",
        "/services/{serviceIdentifier}/listeners/{listenerIdentifier}/rules/{ruleIdentifier}",
        "UpdateRule",
    ),
    ("PATCH", "/services/{serviceIdentifier}", "UpdateService"),
    (
        "PATCH",
        "/servicenetworks/{serviceNetworkIdentifier}",
        "UpdateServiceNetwork",
    ),
    (
        "PATCH",
        "/servicenetworkvpcassociations/{serviceNetworkVpcAssociationIdentifier}",
        "UpdateServiceNetworkVpcAssociation",
    ),
    (
        "PATCH",
        "/targetgroups/{targetGroupIdentifier}",
        "UpdateTargetGroup",
    ),
];

/// Match a `{…}`-labelled requestUri template against the request path:
/// literal segments must be equal, `{…}` labels are botocore's non-greedy
/// `[^/]+` placeholders (at least one character).
fn template_matches(template: &str, path: &str) -> bool {
    let mut t = template.split('/');
    let mut p = path.split('/');
    loop {
        match (t.next(), p.next()) {
            (Some(ts), Some(ps)) => {
                if ts.starts_with('{') {
                    if ps.is_empty() {
                        return false;
                    }
                } else if ts != ps {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Resolve the operation like moto's `_get_action`: a `?Action=` query
/// parameter or `x-amz-target` header wins, else (method, botocore
/// requestUri) with the longest matching pattern (most specific first).
fn resolve_operation(req: &Request, path: &str) -> Option<&'static str> {
    let qmap = crate::proto::query_map(&req.query_string);
    if let Some(action) = crate::proto::query_first(&qmap, "Action") {
        return ROUTES
            .iter()
            .find(|(_, _, op)| *op == action)
            .map(|(_, _, op)| *op);
    }
    if let Some(target) = req.headers.get("x-amz-target") {
        let suffix = target.rsplit('.').next().unwrap_or("");
        return ROUTES
            .iter()
            .find(|(_, _, op)| *op == suffix)
            .map(|(_, _, op)| *op);
    }
    let method = req.method.as_str();
    ROUTES
        .iter()
        .filter(|(m, template, _)| *m == method && template_matches(template, path))
        .max_by_key(|(_, template, _)| template.len())
        .map(|(_, _, op)| *op)
}

// ---------------------------------------------------------------------------
// State helpers (moto's backend methods)
// ---------------------------------------------------------------------------

/// moto's `tag_resource`: TaggingService.tag_resource upserts each key and
/// registers the (possibly empty) entry either way.
fn tag_resource(st: &mut State, arn: &str, tags: &Value) {
    let map = st.tags.entry(arn.to_string()).or_default();
    if let Some(obj) = tags.as_object() {
        for (k, v) in obj {
            map.insert(k.clone(), v.clone());
        }
    }
}

fn find_service<'a>(st: &'a State, id: &str) -> Option<&'a ServiceEntry> {
    st.services.iter().find(|s| s.id == id)
}

fn find_service_network<'a>(st: &'a State, id: &str) -> Option<&'a ServiceNetworkEntry> {
    st.service_networks.iter().find(|s| s.id == id)
}

/// Paginate like moto's `@paginate` with its default limit (50): when the
/// list overflows, the next token names the first excluded item's unique
/// attribute in botocore's token encoding.
fn paginated_list(items: Vec<Value>) -> Value {
    let (page, next_token) = if items.len() > 50 {
        let next_id = items[50].get("id").and_then(Value::as_str).unwrap_or("");
        let token = base64::engine::general_purpose::STANDARD
            .encode(format!("{{\"uniqueAttributes\": \"{next_id}\"}}"));
        (items[..50].to_vec(), json!(token))
    } else {
        (items, Value::Null)
    };
    json!({"items": page, "nextToken": next_token})
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "vpclattice" {
        return None;
    }
    // werkzeug runs with strict slashes off, so trailing slashes don't count.
    let path = req.path.trim_end_matches('/');
    if !passes_url_gate(path) {
        return Some(unrouted());
    }
    let Some(op) = resolve_operation(req, path) else {
        return Some(unrouted());
    };
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();

    match op {
        "CreateService" => Some(create_service(b, &req.region, req.account, &body)),
        "GetService" => Some(get_service(b, path)),
        "ListServices" => Some(list_services(b)),
        "CreateServiceNetwork" => Some(create_service_network(b, &req.region, req.account, &body)),
        "GetServiceNetwork" => Some(get_service_network(b, path)),
        "ListServiceNetworks" => Some(list_service_networks(b)),
        "CreateServiceNetworkVpcAssociation" => Some(create_service_network_vpc_association(
            b,
            &req.region,
            req.account,
            &body,
        )),
        "CreateRule" => Some(create_rule(b, &req.region, req.account, &body, path)),
        "ListTagsForResource" => match path_segment_after(path, "/tags/") {
            Some(seg) => Some(list_tags_for_resource(b, &double_decode(seg))),
            None => Some(unrouted()),
        },
        "TagResource" => match path_segment_after(path, "/tags/") {
            Some(seg) => Some(tag_resource_op(b, &double_decode(seg), &body)),
            None => Some(unrouted()),
        },
        "UntagResource" => match path_segment_after(path, "/tags/") {
            Some(seg) => Some(untag_resource_op(b, &double_decode(seg), &req.query_string)),
            None => Some(unrouted()),
        },
        "CreateAccessLogSubscription" => Some(create_access_log_subscription(
            b,
            &req.region,
            req.account,
            &body,
        )),
        "GetAccessLogSubscription" => Some(get_access_log_subscription(b, path)),
        "ListAccessLogSubscriptions" => Some(list_access_log_subscriptions(b, req)),
        "UpdateAccessLogSubscription" => Some(update_access_log_subscription(b, path, &body)),
        "DeleteAccessLogSubscription" => Some(delete_access_log_subscription(b, path)),
        "PutAuthPolicy" => match path_segment_after(path, "/authpolicy/") {
            Some(seg) => Some(put_auth_policy(b, &double_decode(seg), &body)),
            None => Some(unrouted()),
        },
        "GetAuthPolicy" => match path_segment_after(path, "/authpolicy/") {
            Some(seg) => Some(get_auth_policy(b, &double_decode(seg))),
            None => Some(unrouted()),
        },
        "DeleteAuthPolicy" => match path_segment_after(path, "/authpolicy/") {
            Some(seg) => Some(delete_auth_policy(b, &double_decode(seg))),
            None => Some(unrouted()),
        },
        "PutResourcePolicy" => match path_segment_after(path, "/resourcepolicy/") {
            Some(seg) => Some(put_resource_policy(b, &double_decode(seg), &body)),
            None => Some(unrouted()),
        },
        "GetResourcePolicy" => match path_segment_after(path, "/resourcepolicy/") {
            Some(seg) => Some(get_resource_policy(b, &double_decode(seg))),
            None => Some(unrouted()),
        },
        "DeleteResourcePolicy" => match path_segment_after(path, "/resourcepolicy/") {
            Some(seg) => Some(delete_resource_policy(b, &double_decode(seg))),
            None => Some(unrouted()),
        },
        _ => Some(not_implemented(&snake_case(op))),
    }
}

// ---------------------------------------------------------------------------
// Handlers (moto's responses.py, one per implemented operation)
// ---------------------------------------------------------------------------

fn create_service(st: &mut State, region: &str, account: u64, p: &Value) -> Response {
    let id = prefixed_id("svc-");
    let arn = format!("arn:aws:vpc-lattice:{region}:{account}:service/{id}");
    let custom_domain_name = p
        .get("customDomainName")
        .and_then(Value::as_str)
        .map(String::from);
    let dns_domain = custom_domain_name
        .clone()
        .unwrap_or_else(|| format!("{id}.{region}.vpclattice.amazonaws.com"));
    let entry = ServiceEntry {
        arn: arn.clone(),
        name: p.get("name").cloned().unwrap_or(Value::Null),
        auth_type: p.get("authType").cloned().unwrap_or(Value::Null),
        certificate_arn: p
            .get("certificateArn")
            .and_then(Value::as_str)
            .map(String::from),
        custom_domain_name,
        dns_domain,
        dns_hosted_zone: random_hosted_zone(),
        id,
    };
    let wire = entry.to_wire();
    st.services.push(entry);
    // moto also tags the fresh ARN (with no tags if none were given).
    tag_resource(st, &arn, p.get("tags").unwrap_or(&json!({})));
    ok(&wire)
}

fn get_service(st: &mut State, path: &str) -> Response {
    let identifier = last_path_segment(path);
    match find_service(st, &identifier) {
        Some(s) => ok(&s.to_wire()),
        None => not_found(&identifier),
    }
}

fn list_services(st: &State) -> Response {
    let items: Vec<Value> = st.services.iter().map(|s| s.to_wire()).collect();
    ok(&paginated_list(items))
}

fn create_service_network(st: &mut State, region: &str, account: u64, p: &Value) -> Response {
    let id = prefixed_id("sn-");
    let arn = format!("arn:aws:vpc-lattice:{region}:{account}:servicenetwork/{id}");
    let entry = ServiceNetworkEntry {
        arn: arn.clone(),
        name: p.get("name").cloned().unwrap_or(Value::Null),
        auth_type: p.get("authType").cloned().unwrap_or(Value::Null),
        sharing_config: p.get("sharingConfig").cloned().unwrap_or(json!({})),
        id,
    };
    let wire = entry.to_wire();
    st.service_networks.push(entry);
    tag_resource(st, &arn, p.get("tags").unwrap_or(&json!({})));
    ok(&wire)
}

fn get_service_network(st: &mut State, path: &str) -> Response {
    let identifier = last_path_segment(path);
    match find_service_network(st, &identifier) {
        Some(sn) => ok(&sn.to_wire()),
        None => not_found(&identifier),
    }
}

fn list_service_networks(st: &State) -> Response {
    let items: Vec<Value> = st.service_networks.iter().map(|s| s.to_wire()).collect();
    ok(&paginated_list(items))
}

fn create_service_network_vpc_association(
    st: &mut State,
    region: &str,
    account: u64,
    p: &Value,
) -> Response {
    // moto builds the id from the raw input identifiers' first four chars
    // (ARNs included) without resolving either resource, and reports
    // `createdBy: "user"` with an always-ACTIVE status.
    let sn_input = p
        .get("serviceNetworkIdentifier")
        .and_then(Value::as_str)
        .unwrap_or("");
    let vpc_input = p.get("vpcIdentifier").and_then(Value::as_str).unwrap_or("");
    let sn4: String = sn_input.chars().take(4).collect();
    let vpc4: String = vpc_input.chars().take(4).collect();
    let id = format!("snva-{sn4}-{vpc4}");
    let arn = format!("arn:aws:vpc-lattice:{region}:{account}:servicenetworkvpcassociation/{id}");
    let security_group_ids: Vec<Value> = p
        .get("securityGroupIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let entry = SnvaEntry {
        arn: arn.clone(),
        security_group_ids,
        tags: p.get("tags").cloned().unwrap_or(json!({})),
        id,
    };
    let wire = entry.to_wire();
    st.snvas.push(entry);
    tag_resource(st, &arn, p.get("tags").unwrap_or(&json!({})));
    ok(&wire)
}

fn create_rule(st: &mut State, region: &str, account: u64, p: &Value, path: &str) -> Response {
    // Path shape at this point: /services/{sid}/listeners/{lid}/rules.
    let mut segments = path.split('/');
    let _services = segments.next();
    let sid = segments.next().unwrap_or("");
    let _listeners = segments.next();
    let lid = segments.next().unwrap_or("");
    let id = "rule-[0-9a-z]{17}"; // moto's literal (modulo a templating bug)
    let arn = format!(
        "arn:aws:vpc-lattice:{region}:{account}:service/{sid}/listener/listener-{lid}/rule/{id}"
    );
    let entry = RuleEntry {
        id: id.to_string(),
        arn: arn.clone(),
        name: p.get("name").cloned().unwrap_or(Value::Null),
        priority: p.get("priority").cloned().unwrap_or(Value::Null),
        action: p.get("action").cloned().unwrap_or(json!({})),
        match_value: p.get("match").cloned().unwrap_or(json!({})),
        tags: p.get("tags").cloned().unwrap_or(json!({})),
    };
    let wire = json!({
        "arn": entry.arn,
        "id": id,
        "name": entry.name,
        "priority": entry.priority,
        "action": entry.action,
        "match": entry.match_value,
        "serviceIdentifier": sid,
        "listenerIdentifier": lid,
        "tags": entry.tags,
    });
    st.rules.retain(|r| r.id != id);
    st.rules.push(entry);
    tag_resource(st, &arn, p.get("tags").unwrap_or(&json!({})));
    ok(&wire)
}

fn list_tags_for_resource(st: &State, arn: &str) -> Response {
    let tags = st.tags.get(arn).cloned().unwrap_or_default();
    let tags: Value = Value::Object(tags.into_iter().collect::<Map<String, Value>>());
    ok(&json!({ "tags": tags }))
}

fn tag_resource_op(st: &mut State, arn: &str, p: &Value) -> Response {
    tag_resource(st, arn, p.get("tags").unwrap_or(&json!({})));
    empty_ok()
}

fn untag_resource_op(st: &mut State, arn: &str, query: &str) -> Response {
    // moto's `_get_param("tagKeys")` only sees the first query value and
    // `untag_resource` re-wraps the scalar — exactly one key is removed.
    let qmap = crate::proto::query_map(query);
    let tag_keys = crate::proto::query_first(&qmap, "tagKeys");
    if let (Some(key), Some(map)) = (tag_keys, st.tags.get_mut(arn)) {
        map.remove(key);
    }
    empty_ok()
}

/// moto's `create_access_log_subscription`: the resource must exist and be
/// a service network (`sn-`) or a service (`svc-`).
fn create_access_log_subscription(
    st: &mut State,
    region: &str,
    account: u64,
    p: &Value,
) -> Response {
    let resource_identifier = p
        .get("resourceIdentifier")
        .and_then(Value::as_str)
        .unwrap_or("");
    let (resource_arn, resource_id) = if resource_identifier.starts_with("sn-") {
        match find_service_network(st, resource_identifier) {
            Some(sn) => (sn.arn.clone(), sn.id.clone()),
            None => {
                return not_found(format!("Resource {resource_identifier} not found"));
            }
        }
    } else if resource_identifier.starts_with("svc-") {
        match find_service(st, resource_identifier) {
            Some(s) => (s.arn.clone(), s.id.clone()),
            None => {
                return not_found(format!("Resource {resource_identifier} not found"));
            }
        }
    } else {
        return validation("Invalid parameter resourceIdentifier, must start with 'sn-' or 'svc-'");
    };
    let log_type = p
        .get("serviceNetworkLogType")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .unwrap_or("SERVICE");
    let id = prefixed_id("als-");
    let arn = format!("arn:aws:vpc-lattice:{region}:{account}:accesslogsubscription/{id}");
    let now = now_iso();
    let entry = AlsEntry {
        arn: arn.clone(),
        created_at: now.clone(),
        destination_arn: p.get("destinationArn").cloned().unwrap_or(Value::Null),
        last_updated_at: now,
        resource_arn,
        resource_id,
        log_type: json!(log_type),
        tags: p.get("tags").cloned().unwrap_or(json!({})),
        id,
    };
    let wire = entry.to_wire();
    st.access_log_subscriptions.push(entry);
    ok(&wire)
}

fn find_access_log_subscription<'a>(st: &'a State, id: &str) -> Option<&'a AlsEntry> {
    st.access_log_subscriptions.iter().find(|s| s.id == id)
}

fn get_access_log_subscription(st: &mut State, path: &str) -> Response {
    let identifier = last_path_segment(path);
    match find_access_log_subscription(st, &identifier) {
        Some(sub) => ok(&sub.to_wire()),
        None => not_found(format!("Access Log Subscription {identifier} not found")),
    }
}

fn list_access_log_subscriptions(st: &State, req: &Request) -> Response {
    // moto filters by resourceId or resourceArn equality, slices with
    // maxResults (the wire's lower-case key does match here), and always
    // reports an empty nextToken.
    let qmap = crate::proto::query_map(&req.query_string);
    let resource_identifier = crate::proto::query_first(&qmap, "resourceIdentifier").unwrap_or("");
    let max_results =
        crate::proto::query_first(&qmap, "maxResults").and_then(|v| v.parse::<usize>().ok());
    let items: Vec<Value> = st
        .access_log_subscriptions
        .iter()
        .filter(|sub| {
            sub.resource_id == resource_identifier || sub.resource_arn == resource_identifier
        })
        .map(|s| s.to_wire())
        .take(max_results.unwrap_or(usize::MAX))
        .collect();
    ok(&json!({"items": items, "nextToken": ""}))
}

fn update_access_log_subscription(st: &mut State, path: &str, p: &Value) -> Response {
    let identifier = last_path_segment(path);
    let Some(sub) = st
        .access_log_subscriptions
        .iter_mut()
        .find(|s| s.id == identifier)
    else {
        return not_found(format!("Access Log Subscription {identifier} not found"));
    };
    sub.destination_arn = p.get("destinationArn").cloned().unwrap_or(Value::Null);
    sub.last_updated_at = now_iso();
    ok(&sub.to_wire())
}

fn delete_access_log_subscription(st: &mut State, path: &str) -> Response {
    let identifier = last_path_segment(path);
    let Some(idx) = st
        .access_log_subscriptions
        .iter()
        .position(|s| s.id == identifier)
    else {
        return not_found(format!("Access Log Subscription {identifier} not found"));
    };
    st.access_log_subscriptions.remove(idx);
    empty_ok()
}

/// moto keys auth policies by the resource id with any ARN prefix trimmed
/// off (`arn:…` inputs reduce to the path segment after the final `/`).
fn trim_resource_identifier(identifier: &str) -> String {
    if identifier.starts_with("arn:") {
        identifier
            .rsplit('/')
            .next()
            .unwrap_or(identifier)
            .to_string()
    } else {
        identifier.to_string()
    }
}

/// Auth-policy state tracks the resource's current authType: `NONE` means
/// the policy is `Inactive`, anything else (or a deleted resource on GET)
/// keeps whatever the real state resolves to below.
fn policy_state(auth_type: &Value) -> &'static str {
    if auth_type.as_str() == Some("NONE") {
        "Inactive"
    } else {
        "Active"
    }
}

fn put_auth_policy(st: &mut State, resource_identifier: &str, p: &Value) -> Response {
    let original = resource_identifier;
    let trimmed = trim_resource_identifier(original);
    let auth_type = if let Some(id) = trimmed.strip_prefix("sn-") {
        let _ = id;
        match find_service_network(st, &trimmed) {
            Some(sn) => sn.auth_type.clone(),
            None => return not_found(format!("Resource {trimmed} not found")),
        }
    } else if trimmed.starts_with("svc-") {
        match find_service(st, &trimmed) {
            Some(s) => s.auth_type.clone(),
            None => return not_found(format!("Resource {trimmed} not found")),
        }
    } else {
        return validation("Invalid parameter resourceIdentifier, must start with 'sn-' or 'svc-'");
    };
    let now = now_iso();
    let policy = p.get("policy").cloned().unwrap_or(Value::Null);
    let state = policy_state(&auth_type);
    let wire = json!({"policy": policy, "state": state});
    match st.auth_policies.get_mut(&trimmed) {
        Some(existing) => {
            existing.policy = policy;
            existing.last_updated_at = now;
        }
        None => {
            st.auth_policies.insert(
                trimmed,
                AuthPolicy {
                    policy,
                    created_at: now.clone(),
                    last_updated_at: now,
                },
            );
        }
    }
    ok(&wire)
}

fn get_auth_policy(st: &State, resource_identifier: &str) -> Response {
    let original = resource_identifier;
    let trimmed = trim_resource_identifier(original);
    let Some(policy) = st.auth_policies.get(&trimmed) else {
        return not_found(format!("Resource {original} not found"));
    };
    let wire_source = json!({
        "policy": policy.policy,
        "createdAt": policy.created_at,
        "lastUpdatedAt": policy.last_updated_at,
    });
    // moto recomputes the state against the (possibly deleted) resource;
    // a resource whose authType is NONE (or no resource) is `Inactive`.
    let state = if trimmed.starts_with("sn-") {
        find_service_network(st, &trimmed)
            .filter(|sn| sn.auth_type.as_str() != Some("NONE"))
            .map(|_| "Active")
            .unwrap_or("Inactive")
    } else {
        find_service(st, &trimmed)
            .filter(|s| s.auth_type.as_str() != Some("NONE"))
            .map(|_| "Active")
            .unwrap_or("Inactive")
    };
    let mut wire = wire_source;
    wire["state"] = json!(state);
    ok(&wire)
}

fn delete_auth_policy(st: &mut State, resource_identifier: &str) -> Response {
    let original = resource_identifier;
    let trimmed = trim_resource_identifier(original);
    if st.auth_policies.remove(&trimmed).is_none() {
        return not_found(format!("Resource {original} not found"));
    }
    empty_ok()
}

fn put_resource_policy(st: &mut State, resource_arn: &str, p: &Value) -> Response {
    st.resource_policies.insert(
        resource_arn.to_string(),
        p.get("policy").cloned().unwrap_or(Value::Null),
    );
    empty_ok()
}

fn get_resource_policy(st: &State, resource_arn: &str) -> Response {
    match st.resource_policies.get(resource_arn) {
        Some(policy) => ok(&json!({"policy": policy})),
        None => not_found(format!("Resource {resource_arn} not found")),
    }
}

fn delete_resource_policy(st: &mut State, resource_arn: &str) -> Response {
    if st.resource_policies.remove(resource_arn).is_none() {
        return not_found(format!("Resource {resource_arn} not found"));
    }
    empty_ok()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Each test gets its own (account, region) state bucket: the slot is
    /// process-global across the whole test binary, so share nothing.
    fn next_account() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(123456789013);
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn raw_req(account: u64, method: &str, path: &str, query: &str, body: Value) -> Request {
        Request {
            service: "vpclattice".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn json_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn service_lifecycle() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let created = dispatch(&req("POST", "/services", "", json!({"name": "svc"})));
        let created = json_of(&created.unwrap());
        let id = created["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("svc-"));
        assert_eq!(created["status"], "ACTIVE");
        assert_eq!(created["certificateArn"], "");
        assert!(created["dnsEntry"]["hostedZoneId"]
            .as_str()
            .unwrap()
            .starts_with('Z'));

        // lookup by id and by ARN both resolve through the last path piece
        // (the ARN is one URI segment, like boto3's request serialization)
        let got = dispatch(&req("GET", &format!("/services/{id}"), "", json!({}))).unwrap();
        assert_eq!(json_of(&got)["id"], json!(id));
        let arn = created["arn"].as_str().unwrap().to_string();
        let got = dispatch(&req(
            "GET",
            &format!("/services/{}", url_encode_path(&arn)),
            "",
            json!({}),
        ))
        .unwrap();
        assert_eq!(json_of(&got)["id"], json!(id));

        let listed = dispatch(&req("GET", "/services", "", json!({}))).unwrap();
        let listed = json_of(&listed);
        assert_eq!(listed["items"].as_array().unwrap().len(), 1);
        assert_eq!(listed["items"][0]["name"], "svc");

        let missing = dispatch(&req("GET", "/services/svc-nope", "", json!({}))).unwrap();
        assert_eq!(missing.status, 400);
        assert_eq!(json_of(&missing)["message"], "svc-nope");
    }

    #[test]
    fn service_network_lifecycle() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let created = dispatch(&req(
            "POST",
            "/servicenetworks",
            "",
            json!({"name": "sn", "authType": "NONE", "sharingConfig": {"enabled": false}}),
        ))
        .unwrap();
        let created = json_of(&created);
        let id = created["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("sn-"));
        assert_eq!(created["sharingConfig"], json!({"enabled": false}));

        let got = dispatch(&req(
            "GET",
            &format!("/servicenetworks/{id}"),
            "",
            json!({}),
        ))
        .unwrap();
        assert_eq!(json_of(&got)["id"], json!(id));

        let snvas = dispatch(&req(
            "POST",
            "/servicenetworkvpcassociations",
            "",
            json!({
                "serviceNetworkIdentifier": id,
                "vpcIdentifier": "vpc-12345678",
                "securityGroupIds": ["sg-12345678"],
            }),
        ))
        .unwrap();
        let snvas = json_of(&snvas);
        assert_eq!(snvas["id"], format!("snva-{}-vpc-", &id[..4]));
        assert_eq!(snvas["createdBy"], "user");
        assert_eq!(snvas["status"], "ACTIVE");
        assert_eq!(snvas["securityGroupIds"], json!(["sg-12345678"]));
    }

    #[test]
    fn tags_roundtrip_and_first_key_untag() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let created = dispatch(&req(
            "POST",
            "/services",
            "",
            json!({"name": "svc", "tags": {"a": "1", "b": "2"}}),
        ))
        .unwrap();
        let created_json = json_of(&created);
        let arn = created_json["arn"].as_str().unwrap().to_string();
        let arn_path = url_encode_path(&arn);

        let listed = dispatch(&req("GET", &format!("/tags/{arn_path}"), "", json!({}))).unwrap();
        let listed = json_of(&listed);
        assert_eq!(listed["tags"]["a"], "1");
        assert_eq!(listed["tags"]["b"], "2");

        // moto removes exactly one key per untag call (first query value).
        dispatch(&req(
            "DELETE",
            &format!("/tags/{arn_path}"),
            "tagKeys=a",
            json!({}),
        ))
        .unwrap();
        let listed = dispatch(&req("GET", &format!("/tags/{arn_path}"), "", json!({}))).unwrap();
        assert_eq!(json_of(&listed)["tags"], json!({"b": "2"}));
    }

    #[test]
    fn auth_policy_lifecycle() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let service = dispatch(&req(
            "POST",
            "/services",
            "",
            json!({"name": "svc", "authType": "AWS_IAM"}),
        ))
        .unwrap();
        let service = json_of(&service);
        let svc_arn = service["arn"].as_str().unwrap().to_string();
        let svc_id = service["id"].as_str().unwrap().to_string();

        let put = dispatch(&req(
            "PUT",
            &format!("/authpolicy/{}", url_encode_path(&svc_arn)),
            "",
            json!({"policy": "{}"}),
        ))
        .unwrap();
        let put = json_of(&put);
        assert_eq!(put["state"], "Active");

        let got = dispatch(&req(
            "GET",
            &format!("/authpolicy/{}", url_encode_path(&svc_arn)),
            "",
            json!({}),
        ))
        .unwrap();
        let got = json_of(&got);
        assert_eq!(got["policy"], "{}");
        assert_eq!(got["state"], "Active");
        assert!(got["createdAt"].as_str().is_some());

        dispatch(&req(
            "DELETE",
            &format!("/authpolicy/{}", url_encode_path(&svc_arn)),
            "",
            json!({}),
        ))
        .unwrap();
        let got = dispatch(&req(
            "GET",
            &format!("/authpolicy/{}", url_encode_path(&svc_arn)),
            "",
            json!({}),
        ))
        .unwrap();
        assert_eq!(got.status, 400);
        assert_eq!(
            json_of(&got)["message"],
            json!(format!("Resource {svc_arn} not found"))
        );
        let svc_id_path = url_encode_path(&svc_id);
        let got = dispatch(&req(
            "GET",
            &format!("/authpolicy/{svc_id_path}"),
            "",
            json!({}),
        ))
        .unwrap();
        assert_eq!(
            json_of(&got)["message"],
            json!(format!("Resource {svc_id} not found"))
        );

        // invalid prefix -> ValidationException
        let bad = dispatch(&req(
            "PUT",
            "/authpolicy/invalid-id1234567",
            "",
            json!({"policy": "{}"}),
        ))
        .unwrap();
        assert_eq!(bad.status, 400);
        assert_eq!(
            json_of(&bad),
            json!({
                "__type": "ValidationException",
                "message": "Invalid parameter resourceIdentifier, must start with 'sn-' or 'svc-'",
            })
        );
    }

    /// Percent-encode an ARN the way an SDK path segment would.
    fn url_encode_path(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                other => out.push_str(&format!("%{other:02X}")),
            }
        }
        out
    }

    #[test]
    fn access_log_subscriptions() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let sn = dispatch(&req("POST", "/servicenetworks", "", json!({"name": "sn"}))).unwrap();
        let sn = json_of(&sn);
        let id = sn["id"].as_str().unwrap().to_string();

        let created = dispatch(&req(
            "POST",
            "/accesslogsubscriptions",
            "",
            json!({
                "resourceIdentifier": id,
                "destinationArn": "arn:aws:s3:::logs",
                "serviceNetworkLogType": "RESOURCE",
            }),
        ))
        .unwrap();
        let created = json_of(&created);
        let als_id = created["id"].as_str().unwrap().to_string();
        assert!(als_id.starts_with("als-"));
        assert_eq!(created["resourceArn"], sn["arn"]);
        assert_eq!(created["serviceNetworkLogType"], "RESOURCE");

        // missing-but-plausible id -> RNFE with moto's exact message
        let bad = dispatch(&req(
            "POST",
            "/accesslogsubscriptions",
            "",
            json!({"resourceIdentifier": "sn-invalid-sn-id14", "destinationArn": "x"}),
        ))
        .unwrap();
        assert_eq!(
            json_of(&bad)["message"],
            "Resource sn-invalid-sn-id14 not found"
        );
        // wrong prefix -> ValidationException
        let bad = dispatch(&req(
            "POST",
            "/accesslogsubscriptions",
            "",
            json!({"resourceIdentifier": "invalid-sn-id1234", "destinationArn": "x"}),
        ))
        .unwrap();
        assert_eq!(json_of(&bad)["__type"], "ValidationException");

        let listed = dispatch(&req(
            "GET",
            "/accesslogsubscriptions",
            &format!("resourceIdentifier={id}"),
            json!({}),
        ))
        .unwrap();
        assert_eq!(json_of(&listed)["items"].as_array().unwrap().len(), 1);

        let updated = dispatch(&req(
            "PATCH",
            &format!("/accesslogsubscriptions/{als_id}"),
            "",
            json!({"destinationArn": "arn:aws:s3:::logs2"}),
        ))
        .unwrap();
        assert_eq!(json_of(&updated)["destinationArn"], "arn:aws:s3:::logs2");

        dispatch(&req(
            "DELETE",
            &format!("/accesslogsubscriptions/{als_id}"),
            "",
            json!({}),
        ))
        .unwrap();
        let listed = dispatch(&req(
            "GET",
            "/accesslogsubscriptions",
            &format!("resourceIdentifier={id}"),
            json!({}),
        ))
        .unwrap();
        assert_eq!(json_of(&listed)["items"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn rules_and_moto_boundaries() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);

        let created = dispatch(&req(
            "POST",
            "/services/svc-abcdef12345678901/listeners/listener-1234567890123456/rules",
            "",
            json!({
                "name": "rule",
                "priority": 1,
                "action": {"forward": {"targetGroups": [{"targetGroupIdentifier": "tg-1"}]}},
                "match": {"httpMatch": {"pathMatch": {"match": {"exact": "/p"}}}},
            }),
        ))
        .unwrap();
        let created = json_of(&created);
        assert_eq!(created["id"], "rule-[0-9a-z]{17}");
        assert_eq!(created["priority"], 1);

        // Operations routed by moto's urls.py but without a moto handler
        // answer moto's 501 with the snake-case action name.
        let listener_ops = dispatch(&req(
            "GET",
            "/services/svc-abcdef12345678901/listeners",
            "",
            json!({}),
        ))
        .unwrap();
        assert_eq!(listener_ops.status, 501);
        let body = json_of(&listener_ops);
        assert_eq!(body["__type"], "NotImplemented");
        assert_eq!(
            body["message"],
            "The list_listeners action has not been implemented"
        );
    }

    #[test]
    fn gate_404s_ungated_paths() {
        let acct = next_account();
        let req = |m: &str, p: &str, q: &str, b: Value| raw_req(acct, m, p, q, b);
        let resp = dispatch(&req("POST", "/targetgroups", "", json!({"name": "tg"})));
        assert_eq!(resp.unwrap().status, 404);
        let resp = dispatch(&req(
            "GET",
            "/servicenetworkvpcassociations/snva-1-2",
            "",
            json!({}),
        ));
        assert_eq!(resp.unwrap().status, 404);
    }
}
