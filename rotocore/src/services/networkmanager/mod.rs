//! AWS Network Manager backend (rest-json, one kebab-case URI per operation).
//!
//! Port of moto's `networkmanager` backend: global networks with sites,
//! links, devices and connections, core networks with policy versions and
//! change sets, typed attachments, connect peers, peerings, associations,
//! route analyses, resource policies, routing-policy labels, and
//! organization service access. Wire shapes are moto's PascalCase JSON
//! dicts; moto errors render as HTTP 400 with the `x-amzn-ErrorType`
//! header and moto's `description` body (moto's `JsonRESTError`).
//!
//! Requests arrive signed for the `networkmanager` credential scope, with
//! the operation resolved the moto way: HTTP method + botocore requestUri —
//! a terminal path label matches the raw remainder greedily (moto's
//! `uri_to_regexp` turns the last label, greedy or not, into `.+`), while
//! every other label must be one non-empty percent-encoded segment (botocore
//! encodes `/` inside a `location: uri` member). Most-specific templates
//! (deep literal prefixes) win over shorter greedy parents, as moto's
//! longest-regexp-first ordering does. Path and query members are
//! percent-decoded only after splitting, mirroring moto's `unquote(part)`
//! arithmetic; GET/DELETE list/query members use botocore's lower-camel
//! wire keys, POST/PATCH bodies botocore's PascalCase member names.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// moto stores most groups inside per-global-network maps created in
/// `create_global_network` and pruned in `delete_global_network`.
type Map<T> = BTreeMap<String, T>;

#[derive(Default)]
struct State {
    global_networks: Map<Value>,
    core_networks: Map<Value>,
    sites: Map<Map<Value>>,
    links: Map<Map<Value>>,
    devices: Map<Map<Value>>,
    connections: Map<Map<Value>>,
    attachments: Map<Value>,
    /// Typed attachment extras keyed by attachment id; the base `Attachment`
    /// entry stays in `attachments` (moto shares one object in both maps).
    vpc_attachments: Map<Value>,
    connect_attachments: Map<Value>,
    site_to_site_vpn_attachments: Map<Value>,
    transit_gateway_route_table_attachments: Map<Value>,
    direct_connect_gateway_attachments: Map<Value>,
    connect_peers: Map<Value>,
    peerings: Map<Value>,
    transit_gateway_peerings: Map<Value>,
    core_network_policies: Map<Vec<Value>>,
    resource_policies: Map<String>,
    connect_peer_associations: Map<Map<Value>>,
    customer_gateway_associations: Map<Map<Value>>,
    link_associations: Map<Vec<Value>>,
    transit_gateway_connect_peer_associations: Map<Map<Value>>,
    transit_gateway_registrations: Map<Map<Value>>,
    route_analyses: Map<Map<Value>>,
    core_network_prefix_list_associations: Map<Vec<Value>>,
    routing_policy_labels: Map<Map<String>>,
    organization_service_access: Option<String>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("networkmanager"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Identity helpers (moto's `_now`, `mock_random.get_random_hex`, ARN format)
// ---------------------------------------------------------------------------

/// moto's `_now`: UTC ISO-8601 with microseconds and a trailing `Z`.
fn now_str() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

/// moto's `mock_random.get_random_hex(n)` (moto emits 18 hex chars).
fn rand_hex(n: usize) -> String {
    let s = uuid::Uuid::new_v4().simple().to_string();
    s.chars().take(n).collect()
}

fn resource_id(prefix: &str) -> String {
    format!("{prefix}-{}", rand_hex(18))
}

/// moto's ARN shape for this service: the owner account sits where a region
/// would go and the account slot stays empty
/// (`arn:{partition}:networkmanager:{account}:<kind>/<id>`).
fn nm_arn(partition: &str, account: u64, kind: &str, id: &str) -> String {
    format!("arn:{partition}:networkmanager:{account}:{kind}/{id}")
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

/// Tags from a JSON body member (`[{Key, Value}]`), defaulting to `[]`.
fn tags_of(p: &Value) -> Value {
    p.get("Tags")
        .and_then(Value::as_array)
        .map(|a| {
            json!(a
                .iter()
                .filter(|t| t.get("Key").is_some())
                .cloned()
                .collect::<Vec<_>>())
        })
        .unwrap_or_else(|| json!([]))
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn error(status: u16, code: &str, body: Value) -> Response {
    let mut resp = Response::json(status, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), format!("{code}:")));
    resp
}

/// moto's `ResourceNotFound`: 400, `NotFoundException`, body listing the
/// offending id.
fn not_found(resource_id: &str) -> Response {
    error(
        400,
        "NotFoundException",
        json!({
            "ResourceId": resource_id,
            "Message": "Resource not found.",
        }),
    )
}

/// moto's `ValidationError("Incorrect input.")` for the paginated Get/List
/// operations when the global network does not exist.
fn input_validation_error() -> Response {
    error(
        400,
        "ValidationException",
        json!({"__type": "ValidationException", "message": "Incorrect input."}),
    )
}

fn not_implemented(op: &str) -> Response {
    let e = json!({
        "__type": "NotImplemented",
        "message": format!("The {op} action has not been implemented"),
    });
    Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Operation resolution: (method, requestUri) -> operation, per botocore's
// networkmanager model. Deep literal prefixes sort first so a shorter rule
// with a greedy terminal capture cannot swallow its own child URIs (moto
// sorts the compiled regexps by length descending).
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str, &str)] = &[
    ("POST", "/global-networks", "CreateGlobalNetwork"),
    ("POST", "/global-networks/{globalNetworkId}/sites", "CreateSite"),
    (
        "POST",
        "/global-networks/{globalNetworkId}/links",
        "CreateLink",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/devices",
        "CreateDevice",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/connections",
        "CreateConnection",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/link-associations",
        "AssociateLink",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/connect-peer-associations",
        "AssociateConnectPeer",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/customer-gateway-associations",
        "AssociateCustomerGateway",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/transit-gateway-connect-peer-associations",
        "AssociateTransitGatewayConnectPeer",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/transit-gateway-registrations",
        "RegisterTransitGateway",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/route-analyses",
        "StartRouteAnalysis",
    ),
    (
        "POST",
        "/global-networks/{globalNetworkId}/network-routes",
        "GetNetworkRoutes",
    ),
    ("POST", "/core-networks", "CreateCoreNetwork"),
    (
        "POST",
        "/core-networks/{coreNetworkId}/core-network-policy",
        "PutCoreNetworkPolicy",
    ),
    (
        "POST",
        "/core-networks/{coreNetworkId}/core-network-change-sets/{policyVersionId}/execute",
        "ExecuteCoreNetworkChangeSet",
    ),
    (
        "POST",
        "/core-networks/{coreNetworkId}/core-network-routing-information",
        "ListCoreNetworkRoutingInformation",
    ),
    (
        "POST",
        "/core-networks/{coreNetworkId}/core-network-policy-versions/{policyVersionId}/restore",
        "RestoreCoreNetworkPolicyVersion",
    ),
    ("POST", "/connect-attachments", "CreateConnectAttachment"),
    (
        "POST",
        "/site-to-site-vpn-attachments",
        "CreateSiteToSiteVpnAttachment",
    ),
    (
        "POST",
        "/transit-gateway-route-table-attachments",
        "CreateTransitGatewayRouteTableAttachment",
    ),
    (
        "POST",
        "/direct-connect-gateway-attachments",
        "CreateDirectConnectGatewayAttachment",
    ),
    ("POST", "/vpc-attachments", "CreateVpcAttachment"),
    ("POST", "/connect-peers", "CreateConnectPeer"),
    (
        "POST",
        "/transit-gateway-peerings",
        "CreateTransitGatewayPeering",
    ),
    (
        "POST",
        "/prefix-list",
        "CreateCoreNetworkPrefixListAssociation",
    ),
    (
        "POST",
        "/routing-policy-label",
        "PutAttachmentRoutingPolicyLabel",
    ),
    (
        "POST",
        "/organizations/service-access",
        "StartOrganizationServiceAccessUpdate",
    ),
    (
        "POST",
        "/attachments/{attachmentId}/accept",
        "AcceptAttachment",
    ),
    (
        "POST",
        "/attachments/{attachmentId}/reject",
        "RejectAttachment",
    ),
    ("POST", "/resource-policy/{resourceArn}", "PutResourcePolicy"),
    ("POST", "/tags/{resourceArn}", "TagResource"),
    ("GET", "/global-networks", "DescribeGlobalNetworks"),
    ("GET", "/global-networks/{globalNetworkId}/sites", "GetSites"),
    ("GET", "/global-networks/{globalNetworkId}/links", "GetLinks"),
    ("GET", "/global-networks/{globalNetworkId}/devices", "GetDevices"),
    (
        "GET",
        "/global-networks/{globalNetworkId}/connections",
        "GetConnections",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/link-associations",
        "GetLinkAssociations",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/connect-peer-associations",
        "GetConnectPeerAssociations",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/customer-gateway-associations",
        "GetCustomerGatewayAssociations",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/transit-gateway-connect-peer-associations",
        "GetTransitGatewayConnectPeerAssociations",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/transit-gateway-registrations",
        "GetTransitGatewayRegistrations",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/network-resource-count",
        "GetNetworkResourceCounts",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/network-resources",
        "GetNetworkResources",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/network-resource-relationships",
        "GetNetworkResourceRelationships",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/network-telemetry",
        "GetNetworkTelemetry",
    ),
    (
        "GET",
        "/global-networks/{globalNetworkId}/route-analyses/{routeAnalysisId}",
        "GetRouteAnalysis",
    ),
    ("GET", "/core-networks", "ListCoreNetworks"),
    ("GET", "/core-networks/{coreNetworkId}", "GetCoreNetwork"),
    (
        "GET",
        "/core-networks/{coreNetworkId}/core-network-policy",
        "GetCoreNetworkPolicy",
    ),
    (
        "GET",
        "/core-networks/{coreNetworkId}/core-network-policy-versions",
        "ListCoreNetworkPolicyVersions",
    ),
    (
        "GET",
        "/core-networks/{coreNetworkId}/core-network-change-sets/{policyVersionId}",
        "GetCoreNetworkChangeSet",
    ),
    (
        "GET",
        "/core-networks/{coreNetworkId}/core-network-change-events/{policyVersionId}",
        "GetCoreNetworkChangeEvents",
    ),
    (
        "GET",
        "/connect-attachments/{attachmentId}",
        "GetConnectAttachment",
    ),
    ("GET", "/connect-peers", "ListConnectPeers"),
    ("GET", "/connect-peers/{connectPeerId}", "GetConnectPeer"),
    (
        "GET",
        "/site-to-site-vpn-attachments/{attachmentId}",
        "GetSiteToSiteVpnAttachment",
    ),
    (
        "GET",
        "/transit-gateway-route-table-attachments/{attachmentId}",
        "GetTransitGatewayRouteTableAttachment",
    ),
    (
        "GET",
        "/direct-connect-gateway-attachments/{attachmentId}",
        "GetDirectConnectGatewayAttachment",
    ),
    ("GET", "/vpc-attachments/{attachmentId}", "GetVpcAttachment"),
    (
        "GET",
        "/transit-gateway-peerings/{peeringId}",
        "GetTransitGatewayPeering",
    ),
    ("GET", "/peerings", "ListPeerings"),
    ("GET", "/attachments", "ListAttachments"),
    (
        "GET",
        "/prefix-list/core-network/{coreNetworkId}",
        "ListCoreNetworkPrefixListAssociations",
    ),
    (
        "GET",
        "/routing-policy-label/core-network/{coreNetworkId}",
        "ListAttachmentRoutingPolicyAssociations",
    ),
    (
        "GET",
        "/organizations/service-access",
        "ListOrganizationServiceAccessStatus",
    ),
    ("GET", "/resource-policy/{resourceArn}", "GetResourcePolicy"),
    ("GET", "/tags/{resourceArn}", "ListTagsForResource"),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}",
        "DeleteGlobalNetwork",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/sites/{siteId}",
        "DeleteSite",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/links/{linkId}",
        "DeleteLink",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/devices/{deviceId}",
        "DeleteDevice",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/connections/{connectionId}",
        "DeleteConnection",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/link-associations",
        "DisassociateLink",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/connect-peer-associations/{connectPeerId}",
        "DisassociateConnectPeer",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/customer-gateway-associations/{customerGatewayArn}",
        "DisassociateCustomerGateway",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/transit-gateway-connect-peer-associations/{transitGatewayConnectPeerArn}",
        "DisassociateTransitGatewayConnectPeer",
    ),
    (
        "DELETE",
        "/global-networks/{globalNetworkId}/transit-gateway-registrations/{transitGatewayArn}",
        "DeregisterTransitGateway",
    ),
    ("DELETE", "/core-networks/{coreNetworkId}", "DeleteCoreNetwork"),
    (
        "DELETE",
        "/core-networks/{coreNetworkId}/core-network-policy-versions/{policyVersionId}",
        "DeleteCoreNetworkPolicyVersion",
    ),
    ("DELETE", "/connect-peers/{connectPeerId}", "DeleteConnectPeer"),
    ("DELETE", "/attachments/{attachmentId}", "DeleteAttachment"),
    ("DELETE", "/peerings/{peeringId}", "DeletePeering"),
    (
        "DELETE",
        "/prefix-list/{prefixListArn}/core-network/{coreNetworkId}",
        "DeleteCoreNetworkPrefixListAssociation",
    ),
    (
        "DELETE",
        "/resource-policy/{resourceArn}",
        "DeleteResourcePolicy",
    ),
    (
        "DELETE",
        "/routing-policy-label/core-network/{coreNetworkId}/attachment/{attachmentId}",
        "RemoveAttachmentRoutingPolicyLabel",
    ),
    ("DELETE", "/tags/{resourceArn}", "UntagResource"),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}",
        "UpdateGlobalNetwork",
    ),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}/sites/{siteId}",
        "UpdateSite",
    ),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}/links/{linkId}",
        "UpdateLink",
    ),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}/devices/{deviceId}",
        "UpdateDevice",
    ),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}/connections/{connectionId}",
        "UpdateConnection",
    ),
    (
        "PATCH",
        "/global-networks/{globalNetworkId}/network-resources/{resourceArn}/metadata",
        "UpdateNetworkResourceMetadata",
    ),
    ("PATCH", "/core-networks/{coreNetworkId}", "UpdateCoreNetwork"),
    (
        "PATCH",
        "/direct-connect-gateway-attachments/{attachmentId}",
        "UpdateDirectConnectGatewayAttachment",
    ),
    (
        "PATCH",
        "/vpc-attachments/{attachmentId}",
        "UpdateVpcAttachment",
    ),
];

/// moto's route match: every non-terminal label must be one full raw
/// segment; a terminal label captures the raw remainder greedily.
fn rule_matches(template: &str, raw_path: &str) -> bool {
    let t_segs: Vec<&str> = template.trim_start_matches('/').split('/').collect();
    let p_segs: Vec<&str> = raw_path.trim_start_matches('/').split('/').collect();
    let terminal_param = t_segs
        .last()
        .is_some_and(|s| s.starts_with('{') && s.ends_with('}'));
    if terminal_param && p_segs.len() < t_segs.len() {
        return false;
    }
    if !terminal_param && p_segs.len() != t_segs.len() {
        return false;
    }
    for (i, ts) in t_segs.iter().enumerate() {
        if ts.starts_with('{') && ts.ends_with('}') {
            if p_segs.get(i).copied().unwrap_or("").is_empty() {
                return false;
            }
        } else if p_segs.get(i) != Some(ts) {
            return false;
        }
    }
    true
}

/// Resolve the operation from the HTTP method + raw request path the moto
/// way (botocore requestUri table). Deeper templates win; under equal
/// depth, more literal segments win.
fn resolve_operation(method: &str, raw_path: &str) -> Option<&'static str> {
    static TEMPLATES: Lazy<Vec<(&'static str, &'static str, &'static str)>> = Lazy::new(|| {
        let mut rules: Vec<(&'static str, &'static str, &'static str)> = RULES.to_vec();
        rules.sort_by(|a, b| {
            let depth = |t: &str| t.trim_start_matches('/').split('/').count();
            let literals = |t: &str| {
                t.trim_start_matches('/')
                    .split('/')
                    .filter(|s| !s.starts_with('{'))
                    .count()
            };
            depth(b.1)
                .cmp(&depth(a.1))
                .then_with(|| literals(b.1).cmp(&literals(a.1)))
        });
        rules
    });
    TEMPLATES
        .iter()
        .filter(|(m, _, _)| *m == method)
        .find(|(_, template, _)| rule_matches(template, raw_path))
        .map(|(_, _, op)| *op)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "networkmanager" {
        return None;
    }
    let Some(op) = resolve_operation(&req.method, &req.path) else {
        return Some(not_implemented("action"));
    };
    let body = crate::proto::parse_json_body(&req.body);
    let query = crate::proto::query_map(&req.query_string);

    let slot = state();
    let mut st = slot.write();
    let key = (req.account, req.region.clone());
    let s = st.entry(key).or_default();
    Some(handle(s, op, &body, req, &query))
}

// ---------------------------------------------------------------------------
// Parameter helpers
// ---------------------------------------------------------------------------

/// moto's `unquote(self.path.split("/")[-(back + 1)])`: back=0 is the last
/// segment (`parts[-1]`), back=2 the third from the end (`parts[-3]`). The
/// leading empty element of the split reproduces moto's indexing; values
/// are percent-decoded only now, after the raw split.
fn mpart(path: &str, back: usize) -> String {
    let segs: Vec<&str> = path.split('/').collect();
    match segs.len().checked_sub(back + 1) {
        Some(idx) => crate::proto::url_decode(segs[idx]),
        None => String::new(),
    }
}

/// moto's `unquote(self.path.split("/<sep>/")[-1])` for greedy suffix reads.
fn suffix_after(path: &str, marker: &str) -> String {
    match path.rfind(marker) {
        Some(idx) => crate::proto::url_decode(&path[idx + marker.len()..]),
        None => String::new(),
    }
}

/// First string for a query key.
fn q_str<'a>(query: &'a BTreeMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    query.get(key).and_then(|v| v.first()).map(String::as_str)
}

/// Repeated query values (botocore list members).
fn q_list(query: &BTreeMap<String, Vec<String>>, key: &str) -> Vec<String> {
    query.get(key).cloned().unwrap_or_default()
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

fn q_i64(query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<i64> {
    query
        .get(key)
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<i64>().ok())
}

/// moto's paginated lists: slice up to `maxResults` (limit_default 100 for
/// this service), returning a remainder token when truncated.
fn paginate(list: Vec<Value>, max_results: Option<usize>) -> (Vec<Value>, Option<String>) {
    match max_results.filter(|m| *m > 0 && *m < list.len()) {
        Some(limit) => (list[..limit].to_vec(), Some(limit.to_string())),
        None => (list, None),
    }
}

fn next_token(token: Option<String>) -> Value {
    match token {
        Some(t) => json!(t),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Resource builders (moto's model constructors rendered as wire dicts;
// creators flip the initial CREATING/PENDING state to AVAILABLE before the
// response is rendered, exactly as moto's response handlers do)
// ---------------------------------------------------------------------------

fn global_network_json(
    account: u64,
    part: &'static str,
    description: Option<&str>,
    tags: Value,
) -> Value {
    let gn_id = resource_id("global-network");
    json!({
        "GlobalNetworkId": gn_id,
        "GlobalNetworkArn": nm_arn(part, account, "global-network", &gn_id),
        "Description": description,
        "Tags": tags,
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn core_network_json(
    account: u64,
    part: &'static str,
    global_network_id: &str,
    description: Option<&str>,
    tags: Value,
    policy_document: Value,
) -> Value {
    let cn_id = resource_id("core-network");
    json!({
        "CoreNetworkId": cn_id,
        "CoreNetworkArn": nm_arn(part, account, "core-network", &cn_id),
        "GlobalNetworkId": global_network_id,
        "OwnerAccountId": account.to_string(),
        "Description": description,
        "Tags": tags,
        "PolicyDocument": policy_document,
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn site_json(
    account: u64,
    part: &'static str,
    global_network_id: &str,
    description: Option<&str>,
    location: Value,
    tags: Value,
) -> Value {
    let site_id = resource_id("site");
    json!({
        "SiteId": site_id,
        "SiteArn": nm_arn(part, account, "site", &site_id),
        "GlobalNetworkId": global_network_id,
        "Description": description,
        "Location": location,
        "Tags": tags,
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn link_json(account: u64, part: &'static str, global_network_id: &str, body: &Value) -> Value {
    let link_id = resource_id("link");
    json!({
        "LinkId": link_id,
        "LinkArn": nm_arn(part, account, "link", &link_id),
        "GlobalNetworkId": global_network_id,
        "Description": param_str(body, "Description"),
        "Type": body.get("Type").cloned().unwrap_or(Value::Null),
        "Bandwidth": body.get("Bandwidth").cloned().unwrap_or(Value::Null),
        "Provider": param_str(body, "Provider"),
        "SiteId": param_str(body, "SiteId"),
        "Tags": tags_of(body),
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn device_json(account: u64, part: &'static str, global_network_id: &str, body: &Value) -> Value {
    let device_id = resource_id("device");
    json!({
        "DeviceId": device_id,
        "DeviceArn": nm_arn(part, account, "device", &device_id),
        "GlobalNetworkId": global_network_id,
        "AWSLocation": body.get("AWSLocation")
            .cloned()
            .unwrap_or(Value::Null),
        "Description": param_str(body, "Description"),
        "Type": param_str(body, "Type"),
        "Vendor": param_str(body, "Vendor"),
        "Model": param_str(body, "Model"),
        "SerialNumber": param_str(body, "SerialNumber"),
        "Location": body.get("Location").cloned().unwrap_or(Value::Null),
        "SiteId": param_str(body, "SiteId"),
        "Tags": tags_of(body),
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn connection_json(
    account: u64,
    part: &'static str,
    global_network_id: &str,
    body: &Value,
) -> Value {
    let conn_id = resource_id("connection");
    json!({
        "ConnectionId": conn_id,
        "ConnectionArn": nm_arn(part, account, "connection", &conn_id),
        "GlobalNetworkId": global_network_id,
        "DeviceId": param_str(body, "DeviceId"),
        "ConnectedDeviceId": param_str(body, "ConnectedDeviceId"),
        "LinkId": param_str(body, "LinkId"),
        "ConnectedLinkId": param_str(body, "ConnectedLinkId"),
        "Description": param_str(body, "Description"),
        "Tags": tags_of(body),
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

/// moto's base `Attachment` dict; creators flip the state to AVAILABLE
/// before the response is rendered.
fn base_attachment_json(
    account: u64,
    attachment_type: &str,
    core_network_id: Option<&str>,
    core_network_arn: Option<&str>,
    edge_location: Option<&str>,
    tags: Value,
    resource_arn: Option<String>,
) -> (String, Value) {
    let att_id = resource_id("attachment");
    let now = now_str();
    (
        att_id.clone(),
        json!({
            "AttachmentId": att_id,
            "CoreNetworkId": core_network_id,
            "CoreNetworkArn": core_network_arn,
            "OwnerAccountId": account.to_string(),
            "AttachmentType": attachment_type,
            "EdgeLocation": edge_location.unwrap_or("us-east-1"),
            "ResourceArn": resource_arn,
            "Tags": tags,
            "State": "AVAILABLE",
            "SegmentName": Value::Null,
            "CreatedAt": now,
            "UpdatedAt": now,
            "AttachmentPolicyRuleNumber": Value::Null,
        }),
    )
}

fn peering_json(account: u64, core_network_id: &str, core_network_arn: &str, tags: Value) -> Value {
    let peering_id = resource_id("peering");
    json!({
        "PeeringId": peering_id,
        "CoreNetworkId": core_network_id,
        "CoreNetworkArn": core_network_arn,
        "OwnerAccountId": account.to_string(),
        "PeeringType": "TRANSIT_GATEWAY",
        "EdgeLocation": "us-east-1",
        "ResourceArn": Value::Null,
        "Tags": tags,
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
    })
}

fn connect_peer_json(
    account: u64,
    part: &'static str,
    connect_attachment_id: &str,
    body: &Value,
) -> Value {
    let cp_id = resource_id("connect-peer");
    let core_network_address = body
        .get("CoreNetworkAddress")
        .cloned()
        .unwrap_or(Value::Null);
    let peer_address = body.get("PeerAddress").cloned().unwrap_or(Value::Null);
    let inside_cidr_blocks = body
        .get("InsideCidrBlocks")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let configuration = json!({
        "CoreNetworkAddress": core_network_address,
        "PeerAddress": peer_address,
        "InsideCidrBlocks": inside_cidr_blocks,
        "Protocol": "GRE",
        "BgpConfigurations": [],
    });
    json!({
        "ConnectPeerId": cp_id,
        "ConnectPeerArn": nm_arn(part, account, "connect-peer", &cp_id),
        "ConnectAttachmentId": connect_attachment_id,
        "CoreNetworkId": Value::Null,
        "EdgeLocation": "us-east-1",
        "CoreNetworkAddress": core_network_address,
        "PeerAddress": peer_address,
        "BgpOptions": body.get("BgpOptions").cloned().unwrap_or(Value::Null),
        "InsideCidrBlocks": inside_cidr_blocks,
        "Configuration": configuration,
        "Tags": tags_of(body),
        "State": "AVAILABLE",
        "CreatedAt": now_str(),
        "SubnetArn": body.get("SubnetArn").cloned().unwrap_or(Value::Null),
    })
}

fn policy_json(
    core_network_id: &str,
    version_id: i64,
    document: &str,
    description: Value,
) -> Value {
    json!({
        "CoreNetworkId": core_network_id,
        "PolicyVersionId": version_id,
        "Alias": "LATEST",
        "Description": description,
        "PolicyDocument": document,
        "CreatedAt": now_str(),
        "ChangeSetState": "READY_TO_EXECUTE",
        "PolicyErrors": [],
    })
}

/// moto's `CoreNetworkPolicy.to_version_dict()` (no document / errors).
fn policy_version_json(policy: &Value) -> Value {
    json!({
        "CoreNetworkId": policy["CoreNetworkId"],
        "PolicyVersionId": policy["PolicyVersionId"],
        "Alias": policy["Alias"],
        "Description": policy["Description"],
        "CreatedAt": policy["CreatedAt"],
        "ChangeSetState": policy["ChangeSetState"],
    })
}

fn route_analysis_json(
    account: u64,
    global_network_id: &str,
    source: Value,
    destination: Value,
    include_return_path: Value,
    use_middleboxes: Value,
) -> Value {
    json!({
        "GlobalNetworkId": global_network_id,
        "OwnerAccountId": account.to_string(),
        "RouteAnalysisId": resource_id("route-analysis"),
        "Source": source,
        "Destination": destination,
        "IncludeReturnPath": include_return_path,
        "UseMiddleboxes": use_middleboxes,
        "StartTimestamp": now_str(),
        "Status": "COMPLETED",
        "ForwardPath": {"CompletionStatus": {"ResultCode": "CONNECTED"}},
        "ReturnPath": if include_return_path.as_bool().unwrap_or(false) {
            json!({"CompletionStatus": {"ResultCode": "CONNECTED"}})
        } else {
            json!({})
        },
    })
}

fn organization_status_json(enabled: bool) -> Value {
    json!({
        "OrganizationId": format!("o-{}", rand_hex(10)),
        "OrganizationAwsServiceAccessStatus": if enabled { "ENABLED" } else { "DISABLED" },
        "SLRDeploymentStatus": "SUCCEEDED",
        "AccountStatusList": [],
    })
}

// ---------------------------------------------------------------------------
// State helpers
// ---------------------------------------------------------------------------

fn gn_exists(st: &State, gn_id: &str) -> Result<(), Response> {
    if st.global_networks.contains_key(gn_id) {
        Ok(())
    } else {
        Err(not_found(gn_id))
    }
}

fn core_network<'a>(st: &'a State, cn_id: &str) -> Result<&'a Value, Response> {
    st.core_networks.get(cn_id).ok_or_else(|| not_found(cn_id))
}

/// Existence-only check used by the policy/change-set arms.
fn core_network_checked(st: &State, cn_id: &str) -> Result<(), Response> {
    core_network(st, cn_id).map(|_| ())
}

/// moto's `_get_resource_from_arn`: resolves the trailing `<kind>/<id>`
/// against the backend maps (site/link/device/connection flatten their
/// per-global-network sub-maps). Unknown kinds resolve to nothing.
fn resource_from_arn_mut<'a>(st: &'a mut State, arn: &str) -> Result<&'a mut Value, Response> {
    let tail = arn.rsplit(':').next().unwrap_or_default();
    let (kind, name) = tail.split_once('/').ok_or_else(|| not_found(arn))?;
    match kind {
        "core-network" => st.core_networks.get_mut(name).ok_or_else(|| not_found(arn)),
        "global-network" => st
            .global_networks
            .get_mut(name)
            .ok_or_else(|| not_found(arn)),
        "attachment" => st.attachments.get_mut(name).ok_or_else(|| not_found(arn)),
        "connect-peer" => st.connect_peers.get_mut(name).ok_or_else(|| not_found(arn)),
        "peering" => st.peerings.get_mut(name).ok_or_else(|| not_found(arn)),
        "site" | "link" | "device" | "connection" => {
            for inner in st.sites.values_mut() {
                if let Some(v) = inner.get_mut(name).filter(|_| kind == "site") {
                    return Ok(v);
                }
            }
            for inner in st.links.values_mut() {
                if let Some(v) = inner.get_mut(name).filter(|_| kind == "link") {
                    return Ok(v);
                }
            }
            for inner in st.devices.values_mut() {
                if let Some(v) = inner.get_mut(name).filter(|_| kind == "device") {
                    return Ok(v);
                }
            }
            for inner in st.connections.values_mut() {
                if let Some(v) = inner.get_mut(name).filter(|_| kind == "connection") {
                    return Ok(v);
                }
            }
            Err(not_found(arn))
        }
        _ => Err(not_found(arn)),
    }
}

/// moto's `tag_resource`: the new tags are appended (duplicates possible,
/// list tags later pick up the last occurrence).
fn extend_tags(resource: &mut Value, incoming: &[Value]) {
    if let Some(array) = resource.get_mut("Tags").and_then(Value::as_array_mut) {
        for tag in incoming {
            array.push(tag.clone());
        }
    }
}

fn remove_tags(resource: &mut Value, keys: &[String]) {
    if let Some(array) = resource.get_mut("Tags").and_then(Value::as_array_mut) {
        array.retain(|tag| {
            tag.get("Key")
                .and_then(Value::as_str)
                .is_none_or(|k| !keys.iter().any(|key| key == k))
        });
    }
}

fn param_bool(p: &Value, key: &str, default: bool) -> Value {
    p.get(key).cloned().unwrap_or(json!(default))
}

/// Typed-attachment wire dict: moto's typed model `to_dict()` renders the
/// shared base `Attachment` dict plus the type's own members.
fn render_typed(base: &Value, extras: &Value, keys: &[&str]) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("Attachment".to_string(), base.clone());
    for key in keys {
        out.insert((*key).to_string(), extras[*key].clone());
    }
    Value::Object(out)
}

const VPC_KEYS: &[&str] = &["SubnetArns", "Options"];
const CONNECT_KEYS: &[&str] = &["TransportAttachmentId", "Options"];
const VPN_KEYS: &[&str] = &["VpnConnectionArn"];
const TGW_RT_KEYS: &[&str] = &["PeeringId", "TransitGatewayRouteTableArn"];
const DX_KEYS: &[&str] = &["DirectConnectGatewayArn", "EdgeLocations"];

fn handle(
    st: &mut State,
    op: &str,
    body: &Value,
    req: &Request,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let path = &req.path;
    let account = req.account;
    let part = partition(&req.region);
    let page = |list: Vec<Value>| -> (Vec<Value>, Value) {
        let (items, token) = paginate(
            list,
            q_i64(query, "maxResults").and_then(|m| usize::try_from(m).ok()),
        );
        (items, next_token(token))
    };
    match op {
        // ---------------------------------------------------------------
        // Global networks
        // ---------------------------------------------------------------
        "CreateGlobalNetwork" => {
            let gn =
                global_network_json(account, part, param_str(body, "Description"), tags_of(body));
            let gn_id = gn["GlobalNetworkId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.global_networks.insert(gn_id.clone(), gn.clone());
            // moto's create_global_network opens every per-network group.
            st.sites.insert(gn_id.clone(), Map::new());
            st.links.insert(gn_id.clone(), Map::new());
            st.devices.insert(gn_id.clone(), Map::new());
            st.connections.insert(gn_id, Map::new());
            ok(&json!({"GlobalNetwork": gn}))
        }
        "DeleteGlobalNetwork" => {
            let gn_id = mpart(path, 0);
            let Some(mut gn) = st.global_networks.remove(&gn_id) else {
                return not_found(&gn_id);
            };
            gn["State"] = json!("DELETING");
            st.sites.remove(&gn_id);
            st.links.remove(&gn_id);
            st.devices.remove(&gn_id);
            st.connections.remove(&gn_id);
            st.connect_peer_associations.remove(&gn_id);
            st.customer_gateway_associations.remove(&gn_id);
            st.link_associations.remove(&gn_id);
            st.transit_gateway_connect_peer_associations.remove(&gn_id);
            st.transit_gateway_registrations.remove(&gn_id);
            st.route_analyses.remove(&gn_id);
            ok(&json!({"GlobalNetwork": gn}))
        }
        "UpdateGlobalNetwork" => {
            let gn_id = mpart(path, 0);
            let Some(gn) = st.global_networks.get_mut(&gn_id) else {
                return not_found(&gn_id);
            };
            if let Some(description) = param_str(body, "Description") {
                gn["Description"] = json!(description);
            }
            ok(&json!({"GlobalNetwork": gn}))
        }
        "DescribeGlobalNetworks" => {
            let ids = q_list(query, "globalNetworkIds");
            let list: Vec<Value> = if ids.is_empty() {
                st.global_networks.values().cloned().collect()
            } else {
                st.global_networks
                    .iter()
                    .filter(|(id, _)| ids.iter().any(|i| i == *id))
                    .map(|(_, v)| v.clone())
                    .collect()
            };
            let (items, token) = page(list);
            ok(&json!({"GlobalNetworks": items, "nextToken": token}))
        }

        // ---------------------------------------------------------------
        // Core networks
        // ---------------------------------------------------------------
        "CreateCoreNetwork" => {
            let gn_id = param_str(body, "GlobalNetworkId")
                .unwrap_or_default()
                .to_string();
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let cn = core_network_json(
                account,
                part,
                &gn_id,
                param_str(body, "Description"),
                tags_of(body),
                body.get("PolicyDocument").cloned().unwrap_or(Value::Null),
            );
            let cn_id = cn["CoreNetworkId"].as_str().unwrap_or_default().to_string();
            st.core_networks.insert(cn_id.clone(), cn.clone());
            st.core_network_policies.insert(cn_id.clone(), Vec::new());
            st.core_network_prefix_list_associations
                .insert(cn_id.clone(), Vec::new());
            st.routing_policy_labels.insert(cn_id, Map::new());
            ok(&json!({"CoreNetwork": cn}))
        }
        "DeleteCoreNetwork" => {
            let cn_id = mpart(path, 0);
            let Some(mut cn) = st.core_networks.remove(&cn_id) else {
                return not_found(&cn_id);
            };
            cn["State"] = json!("DELETING");
            st.core_network_policies.remove(&cn_id);
            st.core_network_prefix_list_associations.remove(&cn_id);
            st.routing_policy_labels.remove(&cn_id);
            ok(&json!({"CoreNetwork": cn}))
        }
        "UpdateCoreNetwork" => {
            let cn_id = mpart(path, 0);
            let Some(cn) = st.core_networks.get_mut(&cn_id) else {
                return not_found(&cn_id);
            };
            if let Some(description) = param_str(body, "Description") {
                cn["Description"] = json!(description);
            }
            ok(&json!({"CoreNetwork": cn}))
        }
        "ListCoreNetworks" => {
            let list: Vec<Value> = st.core_networks.values().cloned().collect();
            let (items, token) = page(list);
            ok(&json!({"CoreNetworks": items, "NextToken": token}))
        }
        "GetCoreNetwork" => {
            let cn_id = mpart(path, 0);
            match core_network(st, &cn_id) {
                Ok(cn) => ok(&json!({"CoreNetwork": cn})),
                Err(e) => e,
            }
        }

        // ---------------------------------------------------------------
        // Tags
        // ---------------------------------------------------------------
        "TagResource" | "UntagResource" | "ListTagsForResource" => {
            let arn = suffix_after(path, "/tags/");
            match resource_from_arn_mut(st, &arn) {
                Ok(resource) => {
                    if op == "TagResource" {
                        let incoming: Vec<Value> = body
                            .get("Tags")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        extend_tags(resource, &incoming);
                    } else if op == "UntagResource" {
                        let keys = q_list(query, "tagKeys");
                        remove_tags(resource, &keys);
                    }
                    if op == "ListTagsForResource" {
                        ok(&json!({"TagList": resource["Tags"].clone()}))
                    } else {
                        ok(&json!({}))
                    }
                }
                Err(e) => e,
            }
        }

        // ---------------------------------------------------------------
        // Sites
        // ---------------------------------------------------------------
        "CreateSite" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let site = site_json(
                account,
                part,
                &gn_id,
                param_str(body, "Description"),
                body.get("Location").cloned().unwrap_or(Value::Null),
                tags_of(body),
            );
            let site_id = site["SiteId"].as_str().unwrap_or_default().to_string();
            st.sites
                .entry(gn_id)
                .or_default()
                .insert(site_id, site.clone());
            ok(&json!({"Site": site}))
        }
        "DeleteSite" => {
            let gn_id = mpart(path, 2);
            let site_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut site) = st.sites.entry(gn_id).or_default().remove(&site_id) else {
                return not_found(&site_id);
            };
            site["State"] = json!("DELETING");
            ok(&json!({"Site": site}))
        }
        "UpdateSite" => {
            let gn_id = mpart(path, 2);
            let site_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(site) = st.sites.entry(gn_id).or_default().get_mut(&site_id) else {
                return not_found(&site_id);
            };
            if let Some(description) = param_str(body, "Description") {
                site["Description"] = json!(description);
            }
            if let Some(location) = body.get("Location") {
                site["Location"] = location.clone();
            }
            ok(&json!({"Site": site}))
        }
        "GetSites" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "siteIds");
            let mut list: Vec<Value> = st
                .sites
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|s| ids.iter().any(|i| s["SiteId"].as_str() == Some(i.as_str())));
            }
            let (items, token) = page(list);
            ok(&json!({"Sites": items, "nextToken": token}))
        }

        // ---------------------------------------------------------------
        // Links
        // ---------------------------------------------------------------
        "CreateLink" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let link = link_json(account, part, &gn_id, body);
            let link_id = link["LinkId"].as_str().unwrap_or_default().to_string();
            st.links
                .entry(gn_id)
                .or_default()
                .insert(link_id, link.clone());
            ok(&json!({"Link": link}))
        }
        "DeleteLink" => {
            let gn_id = mpart(path, 2);
            let link_id = mpart(path, 0);
            // moto pops `self.links[gn][link]` in one try; both misses give
            // NotFound(link_id).
            let Some(mut link) = st.links.entry(gn_id).or_default().remove(&link_id) else {
                return not_found(&link_id);
            };
            link["State"] = json!("DELETING");
            ok(&json!({"Link": link}))
        }
        "UpdateLink" => {
            let gn_id = mpart(path, 2);
            let link_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(link) = st.links.entry(gn_id).or_default().get_mut(&link_id) else {
                return not_found(&link_id);
            };
            if let Some(description) = param_str(body, "Description") {
                link["Description"] = json!(description);
            }
            if let Some(type_) = param_str(body, "Type") {
                link["Type"] = json!(type_);
            }
            if let Some(bandwidth) = body.get("Bandwidth") {
                link["Bandwidth"] = bandwidth.clone();
            }
            if let Some(provider) = param_str(body, "Provider") {
                link["Provider"] = json!(provider);
            }
            ok(&json!({"Link": link}))
        }
        "GetLinks" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "linkIds");
            let mut list: Vec<Value> = st
                .links
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|l| ids.iter().any(|i| l["LinkId"].as_str() == Some(i.as_str())));
            }
            let (items, token) = page(list);
            ok(&json!({"Links": items, "nextToken": token}))
        }

        // ---------------------------------------------------------------
        // Devices
        // ---------------------------------------------------------------
        "CreateDevice" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let device = device_json(account, part, &gn_id, body);
            let device_id = device["DeviceId"].as_str().unwrap_or_default().to_string();
            st.devices
                .entry(gn_id)
                .or_default()
                .insert(device_id, device.clone());
            ok(&json!({"Device": device}))
        }
        "DeleteDevice" => {
            let gn_id = mpart(path, 2);
            let device_id = mpart(path, 0);
            let Some(mut device) = st.devices.entry(gn_id).or_default().remove(&device_id) else {
                return not_found(&device_id);
            };
            device["State"] = json!("DELETING");
            ok(&json!({"Device": device}))
        }
        "UpdateDevice" => {
            let gn_id = mpart(path, 2);
            let device_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(device) = st.devices.entry(gn_id).or_default().get_mut(&device_id) else {
                return not_found(&device_id);
            };
            if let Some(aws_location) = body.get("AWSLocation") {
                device["AWSLocation"] = aws_location.clone();
            }
            if let Some(description) = param_str(body, "Description") {
                device["Description"] = json!(description);
            }
            if let Some(type_) = param_str(body, "Type") {
                device["Type"] = json!(type_);
            }
            if let Some(vendor) = param_str(body, "Vendor") {
                device["Vendor"] = json!(vendor);
            }
            if let Some(model) = param_str(body, "Model") {
                device["Model"] = json!(model);
            }
            if let Some(serial) = param_str(body, "SerialNumber") {
                device["SerialNumber"] = json!(serial);
            }
            if let Some(location) = body.get("Location") {
                device["Location"] = location.clone();
            }
            if let Some(site_id) = param_str(body, "SiteId") {
                device["SiteId"] = json!(site_id);
            }
            ok(&json!({"Device": device}))
        }
        "GetDevices" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "deviceIds");
            let mut list: Vec<Value> = st
                .devices
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|d| {
                    ids.iter()
                        .any(|i| d["DeviceId"].as_str() == Some(i.as_str()))
                });
            }
            let (items, token) = page(list);
            ok(&json!({"Devices": items, "nextToken": token}))
        }

        // ---------------------------------------------------------------
        // Connections
        // ---------------------------------------------------------------
        "CreateConnection" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let conn = connection_json(account, part, &gn_id, body);
            let conn_id = conn["ConnectionId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.connections
                .entry(gn_id)
                .or_default()
                .insert(conn_id, conn.clone());
            ok(&json!({"Connection": conn}))
        }
        "DeleteConnection" => {
            let gn_id = mpart(path, 2);
            let conn_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut conn) = st.connections.entry(gn_id).or_default().remove(&conn_id) else {
                return not_found(&conn_id);
            };
            conn["State"] = json!("DELETING");
            ok(&json!({"Connection": conn}))
        }
        "UpdateConnection" => {
            let gn_id = mpart(path, 2);
            let conn_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(conn) = st.connections.entry(gn_id).or_default().get_mut(&conn_id) else {
                return not_found(&conn_id);
            };
            if let Some(link_id) = param_str(body, "LinkId") {
                conn["LinkId"] = json!(link_id);
            }
            if let Some(connected) = param_str(body, "ConnectedLinkId") {
                conn["ConnectedLinkId"] = json!(connected);
            }
            if let Some(description) = param_str(body, "Description") {
                conn["Description"] = json!(description);
            }
            ok(&json!({"Connection": conn}))
        }
        "GetConnections" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "connectionIds");
            let device_id = q_str(query, "deviceId");
            let mut list: Vec<Value> = st
                .connections
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|c| {
                    ids.iter()
                        .any(|i| c["ConnectionId"].as_str() == Some(i.as_str()))
                });
            }
            if let Some(dev) = device_id {
                list.retain(|c| c["DeviceId"].as_str() == Some(dev));
            }
            let (items, token) = page(list);
            ok(&json!({"Connections": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Attachments (base + typed)
        // ---------------------------------------------------------------
        "CreateVpcAttachment" => {
            let cn_id = param_str(body, "CoreNetworkId").unwrap_or_default();
            let Ok(cn) = core_network(st, cn_id) else {
                return not_found(cn_id);
            };
            let cn_arn = cn["CoreNetworkArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let (att_id, att) = base_attachment_json(
                account,
                "VPC",
                Some(cn_id),
                Some(&cn_arn),
                None,
                tags_of(body),
                param_str(body, "VpcArn").map(str::to_string),
            );
            let extras = json!({
                "SubnetArns": body.get("SubnetArns").cloned().unwrap_or_else(|| json!([])),
                "Options": body.get("Options").cloned().unwrap_or_else(|| {
                    json!({"Ipv6Support": false, "ApplianceModeSupport": false})
                }),
            });
            st.attachments.insert(att_id.clone(), att.clone());
            st.vpc_attachments.insert(att_id, extras.clone());
            ok(&json!({"VpcAttachment": render_typed(&att, &extras, VPC_KEYS)}))
        }
        "GetVpcAttachment" => {
            let att_id = mpart(path, 0);
            match (st.attachments.get(&att_id), st.vpc_attachments.get(&att_id)) {
                (Some(base), Some(extras)) => {
                    ok(&json!({"VpcAttachment": render_typed(base, extras, VPC_KEYS)}))
                }
                _ => not_found(&att_id),
            }
        }
        "UpdateVpcAttachment" => {
            let att_id = mpart(path, 0);
            let Some(base) = st.attachments.get_mut(&att_id) else {
                return not_found(&att_id);
            };
            base["UpdatedAt"] = json!(now_str());
            let Some(extras) = st.vpc_attachments.get_mut(&att_id) else {
                return not_found(&att_id);
            };
            if let Some(add) = body.get("AddSubnetArns").and_then(Value::as_array) {
                if let Some(list) = extras["SubnetArns"].as_array_mut() {
                    for arn in add {
                        list.push(arn.clone());
                    }
                }
            }
            if let Some(remove) = body.get("RemoveSubnetArns").and_then(Value::as_array) {
                if let Some(list) = extras["SubnetArns"].as_array_mut() {
                    list.retain(|s| !remove.iter().any(|r| r.as_str() == s.as_str()));
                }
            }
            if let Some(options) = body.get("Options") {
                if !options.is_null() {
                    extras["Options"] = options.clone();
                }
            }
            ok(&json!({"VpcAttachment": render_typed(base, extras, VPC_KEYS)}))
        }
        "CreateConnectAttachment" => {
            let cn_id = param_str(body, "CoreNetworkId").unwrap_or_default();
            let Ok(cn) = core_network(st, cn_id) else {
                return not_found(cn_id);
            };
            let cn_arn = cn["CoreNetworkArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let (att_id, att) = base_attachment_json(
                account,
                "CONNECT",
                Some(cn_id),
                Some(&cn_arn),
                param_str(body, "EdgeLocation"),
                tags_of(body),
                None,
            );
            let extras = json!({
                "TransportAttachmentId": param_str(body, "TransportAttachmentId").unwrap_or_default(),
                "Options": body
                    .get("Options")
                    .cloned()
                    .unwrap_or_else(|| json!({"Protocol": "GRE"})),
            });
            st.attachments.insert(att_id.clone(), att.clone());
            st.connect_attachments.insert(att_id, extras.clone());
            ok(&json!({"ConnectAttachment": render_typed(&att, &extras, CONNECT_KEYS)}))
        }
        "GetConnectAttachment" => {
            let att_id = mpart(path, 0);
            match (
                st.attachments.get(&att_id),
                st.connect_attachments.get(&att_id),
            ) {
                (Some(base), Some(extras)) => {
                    ok(&json!({"ConnectAttachment": render_typed(base, extras, CONNECT_KEYS)}))
                }
                _ => not_found(&att_id),
            }
        }
        "CreateSiteToSiteVpnAttachment" => {
            let cn_id = param_str(body, "CoreNetworkId").unwrap_or_default();
            let Ok(cn) = core_network(st, cn_id) else {
                return not_found(cn_id);
            };
            let cn_arn = cn["CoreNetworkArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let (att_id, att) = base_attachment_json(
                account,
                "SITE_TO_SITE_VPN",
                Some(cn_id),
                Some(&cn_arn),
                None,
                tags_of(body),
                param_str(body, "VpnConnectionArn").map(str::to_string),
            );
            let extras = json!({
                "VpnConnectionArn": param_str(body, "VpnConnectionArn").unwrap_or_default(),
            });
            st.attachments.insert(att_id.clone(), att.clone());
            st.site_to_site_vpn_attachments
                .insert(att_id, extras.clone());
            ok(&json!({"SiteToSiteVpnAttachment": render_typed(&att, &extras, VPN_KEYS)}))
        }
        "GetSiteToSiteVpnAttachment" => {
            let att_id = mpart(path, 0);
            match (
                st.attachments.get(&att_id),
                st.site_to_site_vpn_attachments.get(&att_id),
            ) {
                (Some(base), Some(extras)) => {
                    ok(&json!({"SiteToSiteVpnAttachment": render_typed(base, extras, VPN_KEYS)}))
                }
                _ => not_found(&att_id),
            }
        }
        "CreateTransitGatewayRouteTableAttachment" => {
            let peering_id = param_str(body, "PeeringId").unwrap_or_default().to_string();
            // moto searches the typed transit-gateway peerings for one whose
            // peering id matches.
            let Some(peering) = st
                .transit_gateway_peerings
                .values()
                .find(|tgp| tgp["Peering"]["PeeringId"].as_str() == Some(peering_id.as_str()))
                .cloned()
            else {
                return not_found(&peering_id);
            };
            let cn_id = peering["Peering"]["CoreNetworkId"]
                .as_str()
                .map(str::to_string);
            let cn_arn = peering["Peering"]["CoreNetworkArn"]
                .as_str()
                .map(str::to_string);
            let (att_id, att) = base_attachment_json(
                account,
                "TRANSIT_GATEWAY_ROUTE_TABLE",
                cn_id.as_deref(),
                cn_arn.as_deref(),
                None,
                tags_of(body),
                param_str(body, "TransitGatewayRouteTableArn").map(str::to_string),
            );
            let extras = json!({
                "PeeringId": peering_id,
                "TransitGatewayRouteTableArn": param_str(body, "TransitGatewayRouteTableArn").unwrap_or_default(),
            });
            st.attachments.insert(att_id.clone(), att.clone());
            st.transit_gateway_route_table_attachments
                .insert(att_id, extras.clone());
            ok(
                &json!({"TransitGatewayRouteTableAttachment": render_typed(&att, &extras, TGW_RT_KEYS)}),
            )
        }
        "GetTransitGatewayRouteTableAttachment" => {
            let att_id = mpart(path, 0);
            match (
                st.attachments.get(&att_id),
                st.transit_gateway_route_table_attachments.get(&att_id),
            ) {
                (Some(base), Some(extras)) => ok(&json!({
                    "TransitGatewayRouteTableAttachment": render_typed(base, extras, TGW_RT_KEYS)
                })),
                _ => not_found(&att_id),
            }
        }
        "CreateDirectConnectGatewayAttachment" => {
            let cn_id = param_str(body, "CoreNetworkId").unwrap_or_default();
            let Ok(cn) = core_network(st, cn_id) else {
                return not_found(cn_id);
            };
            let cn_arn = cn["CoreNetworkArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let edge0 = body
                .get("EdgeLocations")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_str);
            let (att_id, att) = base_attachment_json(
                account,
                "DIRECT_CONNECT_GATEWAY",
                Some(cn_id),
                Some(&cn_arn),
                edge0,
                tags_of(body),
                param_str(body, "DirectConnectGatewayArn").map(str::to_string),
            );
            let extras = json!({
                "DirectConnectGatewayArn": param_str(body, "DirectConnectGatewayArn").unwrap_or_default(),
                "EdgeLocations": body.get("EdgeLocations").cloned().unwrap_or_else(|| json!([])),
            });
            st.attachments.insert(att_id.clone(), att.clone());
            st.direct_connect_gateway_attachments
                .insert(att_id, extras.clone());
            ok(&json!({"DirectConnectGatewayAttachment": render_typed(&att, &extras, DX_KEYS)}))
        }
        "GetDirectConnectGatewayAttachment" => {
            let att_id = mpart(path, 0);
            match (
                st.attachments.get(&att_id),
                st.direct_connect_gateway_attachments.get(&att_id),
            ) {
                (Some(base), Some(extras)) => ok(
                    &json!({"DirectConnectGatewayAttachment": render_typed(base, extras, DX_KEYS)}),
                ),
                _ => not_found(&att_id),
            }
        }
        "UpdateDirectConnectGatewayAttachment" => {
            let att_id = mpart(path, 0);
            let Some(extras) = st.direct_connect_gateway_attachments.get_mut(&att_id) else {
                return not_found(&att_id);
            };
            if let Some(locations) = body.get("EdgeLocations") {
                extras["EdgeLocations"] = locations.clone();
            }
            if let Some(base) = st.attachments.get_mut(&att_id) {
                base["UpdatedAt"] = json!(now_str());
            }
            let base = st.attachments.get(&att_id).cloned().unwrap_or(Value::Null);
            ok(&json!({"DirectConnectGatewayAttachment": render_typed(&base, extras, DX_KEYS)}))
        }
        "DeleteAttachment" => {
            let att_id = mpart(path, 0);
            let Some(mut att) = st.attachments.remove(&att_id) else {
                return not_found(&att_id);
            };
            att["State"] = json!("DELETING");
            st.vpc_attachments.remove(&att_id);
            st.connect_attachments.remove(&att_id);
            st.site_to_site_vpn_attachments.remove(&att_id);
            st.transit_gateway_route_table_attachments.remove(&att_id);
            st.direct_connect_gateway_attachments.remove(&att_id);
            ok(&json!({"Attachment": att}))
        }
        "AcceptAttachment" => {
            let att_id = mpart(path, 1);
            let Some(att) = st.attachments.get_mut(&att_id) else {
                return not_found(&att_id);
            };
            att["State"] = json!("AVAILABLE");
            att["UpdatedAt"] = json!(now_str());
            ok(&json!({"Attachment": att}))
        }
        "RejectAttachment" => {
            let att_id = mpart(path, 1);
            let Some(att) = st.attachments.get_mut(&att_id) else {
                return not_found(&att_id);
            };
            att["State"] = json!("REJECTED");
            att["UpdatedAt"] = json!(now_str());
            ok(&json!({"Attachment": att}))
        }
        "ListAttachments" => {
            let cn_filter = q_str(query, "coreNetworkId");
            let type_filter = q_str(query, "attachmentType");
            let edge_filter = q_str(query, "edgeLocation");
            let state_filter = q_str(query, "state");
            let mut list: Vec<Value> = st.attachments.values().cloned().collect();
            if let Some(cn) = cn_filter {
                list.retain(|a| a["CoreNetworkId"].as_str() == Some(cn));
            }
            if let Some(t) = type_filter {
                list.retain(|a| a["AttachmentType"].as_str() == Some(t));
            }
            if let Some(e) = edge_filter {
                list.retain(|a| a["EdgeLocation"].as_str() == Some(e));
            }
            if let Some(s) = state_filter {
                list.retain(|a| a["State"].as_str() == Some(s));
            }
            let (items, token) = page(list);
            ok(&json!({"Attachments": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Connect peers
        // ---------------------------------------------------------------
        "CreateConnectPeer" => {
            let connect_attachment_id = param_str(body, "ConnectAttachmentId")
                .unwrap_or_default()
                .to_string();
            if !st.connect_attachments.contains_key(&connect_attachment_id) {
                return not_found(&connect_attachment_id);
            }
            let cp = connect_peer_json(account, part, &connect_attachment_id, body);
            let cp_id = cp["ConnectPeerId"].as_str().unwrap_or_default().to_string();
            st.connect_peers.insert(cp_id, cp.clone());
            ok(&json!({"ConnectPeer": cp}))
        }
        "GetConnectPeer" => {
            let cp_id = mpart(path, 0);
            match st.connect_peers.get(&cp_id) {
                Some(cp) => ok(&json!({"ConnectPeer": cp})),
                None => not_found(&cp_id),
            }
        }
        "DeleteConnectPeer" => {
            let cp_id = mpart(path, 0);
            let Some(mut cp) = st.connect_peers.remove(&cp_id) else {
                return not_found(&cp_id);
            };
            cp["State"] = json!("DELETING");
            ok(&json!({"ConnectPeer": cp}))
        }
        "ListConnectPeers" => {
            let attach_filter = q_str(query, "connectAttachmentId");
            let mut list: Vec<Value> = st.connect_peers.values().cloned().collect();
            if let Some(aid) = attach_filter {
                list.retain(|cp| cp["ConnectAttachmentId"].as_str() == Some(aid));
            }
            let (items, token) = page(list);
            ok(&json!({"ConnectPeers": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Peerings
        // ---------------------------------------------------------------
        "CreateTransitGatewayPeering" => {
            let cn_id = param_str(body, "CoreNetworkId")
                .unwrap_or_default()
                .to_string();
            let Ok(cn) = core_network(st, &cn_id) else {
                return not_found(&cn_id);
            };
            let cn_arn = cn["CoreNetworkArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let mut peering = peering_json(account, &cn_id, &cn_arn, tags_of(body));
            peering["ResourceArn"] = body
                .get("TransitGatewayArn")
                .cloned()
                .unwrap_or(Value::Null);
            let peering_id = peering["PeeringId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let extras = json!({
                "Peering": peering.clone(),
                "TransitGatewayArn": body.get("TransitGatewayArn").cloned().unwrap_or(Value::Null),
                "TransitGatewayPeeringAttachmentId": resource_id("tgw-peering"),
            });
            st.peerings.insert(peering_id.clone(), peering);
            st.transit_gateway_peerings
                .insert(peering_id, extras.clone());
            ok(&json!({"TransitGatewayPeering": extras}))
        }
        "GetTransitGatewayPeering" => {
            let peering_id = mpart(path, 0);
            match st.transit_gateway_peerings.get(&peering_id) {
                Some(extras) => ok(&json!({"TransitGatewayPeering": extras})),
                None => not_found(&peering_id),
            }
        }
        "DeletePeering" => {
            let peering_id = mpart(path, 0);
            let Some(mut peering) = st.peerings.remove(&peering_id) else {
                return not_found(&peering_id);
            };
            peering["State"] = json!("DELETING");
            st.transit_gateway_peerings.remove(&peering_id);
            ok(&json!({"Peering": peering}))
        }
        "ListPeerings" => {
            let cn_filter = q_str(query, "coreNetworkId");
            let type_filter = q_str(query, "peeringType");
            let edge_filter = q_str(query, "edgeLocation");
            let state_filter = q_str(query, "state");
            let mut list: Vec<Value> = st.peerings.values().cloned().collect();
            if let Some(cn) = cn_filter {
                list.retain(|p| p["CoreNetworkId"].as_str() == Some(cn));
            }
            if let Some(t) = type_filter {
                list.retain(|p| p["PeeringType"].as_str() == Some(t));
            }
            if let Some(e) = edge_filter {
                list.retain(|p| p["EdgeLocation"].as_str() == Some(e));
            }
            if let Some(s) = state_filter {
                list.retain(|p| p["State"].as_str() == Some(s));
            }
            let (items, token) = page(list);
            ok(&json!({"Peerings": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Core network policy
        // ---------------------------------------------------------------
        "PutCoreNetworkPolicy" => {
            let cn_id = mpart(path, 1);
            if let Err(e) = core_network(st, &cn_id) {
                return e;
            }
            let document = param_str(body, "PolicyDocument")
                .unwrap_or_default()
                .to_string();
            let description = body.get("Description").cloned().unwrap_or(Value::Null);
            let mut policies = st
                .core_network_policies
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            let version_id = policies.len() as i64 + 1;
            let policy = policy_json(&cn_id, version_id, &document, description);
            policies.push(policy.clone());
            st.core_network_policies.insert(cn_id, policies);
            ok(&json!({"CoreNetworkPolicy": policy}))
        }
        "GetCoreNetworkPolicy" => {
            let cn_id = mpart(path, 1);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            let policies = st
                .core_network_policies
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            if policies.is_empty() {
                return not_found(&cn_id);
            }
            if let Some(wanted) = q_i64(query, "policyVersionId") {
                return match policies
                    .iter()
                    .find(|p| p["PolicyVersionId"].as_i64() == Some(wanted))
                {
                    Some(policy) => ok(&json!({"CoreNetworkPolicy": policy})),
                    None => not_found(&wanted.to_string()),
                };
            }
            if let Some(alias) = q_str(query, "alias") {
                if let Some(policy) = policies.iter().find(|p| p["Alias"].as_str() == Some(alias)) {
                    return ok(&json!({"CoreNetworkPolicy": policy}));
                }
            }
            ok(&json!({"CoreNetworkPolicy": policies.last().cloned().unwrap_or(Value::Null)}))
        }
        "DeleteCoreNetworkPolicyVersion" => {
            let cn_id = mpart(path, 2);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            let Ok(pvid) = mpart(path, 0).parse::<i64>() else {
                return input_validation_error();
            };
            let Some(policies) = st.core_network_policies.get_mut(&cn_id) else {
                return not_found(&pvid.to_string());
            };
            match policies
                .iter()
                .position(|p| p["PolicyVersionId"].as_i64() == Some(pvid))
            {
                Some(index) => {
                    let policy = policies.remove(index);
                    ok(&json!({"CoreNetworkPolicy": policy}))
                }
                None => not_found(&pvid.to_string()),
            }
        }
        "RestoreCoreNetworkPolicyVersion" => {
            let cn_id = mpart(path, 3);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            let Ok(pvid) = mpart(path, 1).parse::<i64>() else {
                return input_validation_error();
            };
            let mut policies = st
                .core_network_policies
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            let Some(found) = policies
                .iter()
                .find(|p| p["PolicyVersionId"].as_i64() == Some(pvid))
            else {
                return not_found(&pvid.to_string());
            };
            let version_id = policies.len() as i64 + 1;
            let new_policy = policy_json(
                &cn_id,
                version_id,
                found["PolicyDocument"].as_str().unwrap_or_default(),
                found["Description"].clone(),
            );
            policies.push(new_policy.clone());
            st.core_network_policies.insert(cn_id, policies);
            ok(&json!({"CoreNetworkPolicy": new_policy}))
        }
        "ListCoreNetworkPolicyVersions" => {
            let cn_id = mpart(path, 1);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            let policies = st
                .core_network_policies
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            let list: Vec<Value> = policies.iter().map(policy_version_json).collect();
            let (items, token) = page(list);
            ok(&json!({"CoreNetworkPolicyVersions": items, "NextToken": token}))
        }
        "ExecuteCoreNetworkChangeSet" => {
            let cn_id = mpart(path, 3);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            let Ok(pvid) = mpart(path, 1).parse::<i64>() else {
                return input_validation_error();
            };
            let mut policies = st
                .core_network_policies
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            if !policies
                .iter()
                .any(|p| p["PolicyVersionId"].as_i64() == Some(pvid))
            {
                return not_found(&pvid.to_string());
            }
            for p in policies.iter_mut() {
                if p["Alias"].as_str() == Some("LIVE") {
                    p["Alias"] = json!("LATEST");
                }
            }
            for p in policies.iter_mut() {
                if p["PolicyVersionId"].as_i64() == Some(pvid) {
                    p["Alias"] = json!("LIVE");
                    p["ChangeSetState"] = json!("EXECUTION_SUCCEEDED");
                }
            }
            st.core_network_policies.insert(cn_id, policies);
            ok(&json!({}))
        }
        "GetCoreNetworkChangeSet" => {
            let cn_id = mpart(path, 2);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            ok(&json!({"CoreNetworkChanges": []}))
        }
        "GetCoreNetworkChangeEvents" => {
            let cn_id = mpart(path, 2);
            if let Err(e) = core_network_checked(st, &cn_id) {
                return e;
            }
            ok(&json!({"CoreNetworkChangeEvents": []}))
        }

        // ---------------------------------------------------------------
        // Resource policies
        // ---------------------------------------------------------------
        "PutResourcePolicy" => {
            let arn = suffix_after(path, "/resource-policy/");
            let document = param_str(body, "PolicyDocument").unwrap_or_default();
            st.resource_policies.insert(arn, document.to_string());
            ok(&json!({}))
        }
        "GetResourcePolicy" => {
            let arn = suffix_after(path, "/resource-policy/");
            match st.resource_policies.get(&arn) {
                Some(document) => ok(&json!({"PolicyDocument": document})),
                None => ok(&json!({})),
            }
        }
        "DeleteResourcePolicy" => {
            let arn = suffix_after(path, "/resource-policy/");
            st.resource_policies.remove(&arn);
            ok(&json!({}))
        }

        // ---------------------------------------------------------------
        // Associations
        // ---------------------------------------------------------------
        "AssociateConnectPeer" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let assoc = json!({
                "GlobalNetworkId": gn_id,
                "ConnectPeerId": param_str(body, "ConnectPeerId"),
                "DeviceId": param_str(body, "DeviceId"),
                "LinkId": param_str(body, "LinkId"),
                "State": "AVAILABLE",
            });
            let cp_id = assoc["ConnectPeerId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.connect_peer_associations
                .entry(gn_id)
                .or_default()
                .insert(cp_id, assoc.clone());
            ok(&json!({"ConnectPeerAssociation": assoc}))
        }
        "DisassociateConnectPeer" => {
            let gn_id = mpart(path, 2);
            let cp_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut assoc) = st
                .connect_peer_associations
                .entry(gn_id)
                .or_default()
                .remove(&cp_id)
            else {
                return not_found(&cp_id);
            };
            assoc["State"] = json!("DELETED");
            ok(&json!({"ConnectPeerAssociation": assoc}))
        }
        "GetConnectPeerAssociations" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "connectPeerIds");
            let mut list: Vec<Value> = st
                .connect_peer_associations
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|a| {
                    ids.iter()
                        .any(|i| a["ConnectPeerId"].as_str() == Some(i.as_str()))
                });
            }
            let (items, token) = page(list);
            ok(&json!({"ConnectPeerAssociations": items, "NextToken": token}))
        }
        "AssociateCustomerGateway" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let assoc = json!({
                "GlobalNetworkId": gn_id,
                "CustomerGatewayArn": param_str(body, "CustomerGatewayArn"),
                "DeviceId": param_str(body, "DeviceId"),
                "LinkId": param_str(body, "LinkId"),
                "State": "AVAILABLE",
            });
            let key = assoc["CustomerGatewayArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.customer_gateway_associations
                .entry(gn_id)
                .or_default()
                .insert(key, assoc.clone());
            ok(&json!({"CustomerGatewayAssociation": assoc}))
        }
        "DisassociateCustomerGateway" => {
            let gn_id = mpart(path, 2);
            let cgw_arn = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut assoc) = st
                .customer_gateway_associations
                .entry(gn_id)
                .or_default()
                .remove(&cgw_arn)
            else {
                return not_found(&cgw_arn);
            };
            assoc["State"] = json!("DELETED");
            ok(&json!({"CustomerGatewayAssociation": assoc}))
        }
        "GetCustomerGatewayAssociations" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "customerGatewayArns");
            let mut list: Vec<Value> = st
                .customer_gateway_associations
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|a| {
                    ids.iter()
                        .any(|i| a["CustomerGatewayArn"].as_str() == Some(i.as_str()))
                });
            }
            let (items, token) = page(list);
            ok(&json!({"CustomerGatewayAssociations": items, "NextToken": token}))
        }
        "AssociateLink" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let assoc = json!({
                "GlobalNetworkId": gn_id,
                "DeviceId": param_str(body, "DeviceId"),
                "LinkId": param_str(body, "LinkId"),
                "LinkAssociationState": "AVAILABLE",
            });
            st.link_associations
                .entry(gn_id)
                .or_default()
                .push(assoc.clone());
            ok(&json!({"LinkAssociation": assoc}))
        }
        "DisassociateLink" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let device_id = q_str(query, "deviceId");
            let link_id = q_str(query, "linkId");
            let assocs = st.link_associations.entry(gn_id).or_default();
            let position = assocs.iter().position(|a| {
                a["DeviceId"].as_str() == device_id && a["LinkId"].as_str() == link_id
            });
            match position {
                Some(index) => {
                    let mut assoc = assocs.remove(index);
                    assoc["LinkAssociationState"] = json!("DELETED");
                    ok(&json!({"LinkAssociation": assoc}))
                }
                None => not_found(&format!(
                    "{}/{}",
                    device_id.unwrap_or_default(),
                    link_id.unwrap_or_default()
                )),
            }
        }
        "GetLinkAssociations" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let device_filter = q_str(query, "deviceId");
            let link_filter = q_str(query, "linkId");
            let mut list = st
                .link_associations
                .get(&gn_id)
                .cloned()
                .unwrap_or_default();
            if let Some(dev) = device_filter {
                list.retain(|a| a["DeviceId"].as_str() == Some(dev));
            }
            if let Some(link) = link_filter {
                list.retain(|a| a["LinkId"].as_str() == Some(link));
            }
            let (items, token) = page(list);
            ok(&json!({"LinkAssociations": items, "NextToken": token}))
        }
        "AssociateTransitGatewayConnectPeer" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let assoc = json!({
                "GlobalNetworkId": gn_id,
                "TransitGatewayConnectPeerArn": param_str(body, "TransitGatewayConnectPeerArn"),
                "DeviceId": param_str(body, "DeviceId"),
                "LinkId": param_str(body, "LinkId"),
                "State": "AVAILABLE",
            });
            let key = assoc["TransitGatewayConnectPeerArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.transit_gateway_connect_peer_associations
                .entry(gn_id)
                .or_default()
                .insert(key, assoc.clone());
            ok(&json!({"TransitGatewayConnectPeerAssociation": assoc}))
        }
        "DisassociateTransitGatewayConnectPeer" => {
            let gn_id = mpart(path, 2);
            let arn = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut assoc) = st
                .transit_gateway_connect_peer_associations
                .entry(gn_id)
                .or_default()
                .remove(&arn)
            else {
                return not_found(&arn);
            };
            assoc["State"] = json!("DELETED");
            ok(&json!({"TransitGatewayConnectPeerAssociation": assoc}))
        }
        "GetTransitGatewayConnectPeerAssociations" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "transitGatewayConnectPeerArns");
            let mut list: Vec<Value> = st
                .transit_gateway_connect_peer_associations
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|a| {
                    ids.iter()
                        .any(|i| a["TransitGatewayConnectPeerArn"].as_str() == Some(i.as_str()))
                });
            }
            let (items, token) = page(list);
            ok(&json!({"TransitGatewayConnectPeerAssociations": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Transit gateway registrations
        // ---------------------------------------------------------------
        "RegisterTransitGateway" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let reg = json!({
                "GlobalNetworkId": gn_id,
                "TransitGatewayArn": param_str(body, "TransitGatewayArn"),
                "State": {"Code": "AVAILABLE", "Message": ""},
            });
            let key = reg["TransitGatewayArn"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.transit_gateway_registrations
                .entry(gn_id)
                .or_default()
                .insert(key, reg.clone());
            ok(&json!({"TransitGatewayRegistration": reg}))
        }
        "DeregisterTransitGateway" => {
            let gn_id = mpart(path, 2);
            let arn = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let Some(mut reg) = st
                .transit_gateway_registrations
                .entry(gn_id)
                .or_default()
                .remove(&arn)
            else {
                return not_found(&arn);
            };
            reg["State"] = json!({"Code": "DELETING", "Message": ""});
            ok(&json!({"TransitGatewayRegistration": reg}))
        }
        "GetTransitGatewayRegistrations" => {
            let gn_id = mpart(path, 1);
            if gn_exists(st, &gn_id).is_err() {
                return input_validation_error();
            }
            let ids = q_list(query, "transitGatewayArns");
            let mut list: Vec<Value> = st
                .transit_gateway_registrations
                .get(&gn_id)
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if !ids.is_empty() {
                list.retain(|r| {
                    ids.iter()
                        .any(|i| r["TransitGatewayArn"].as_str() == Some(i.as_str()))
                });
            }
            let (items, token) = page(list);
            ok(&json!({"TransitGatewayRegistrations": items, "NextToken": token}))
        }

        // ---------------------------------------------------------------
        // Route analyses
        // ---------------------------------------------------------------
        "StartRouteAnalysis" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let ra = route_analysis_json(
                account,
                &gn_id,
                body.get("Source").cloned().unwrap_or_else(|| json!({})),
                body.get("Destination")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                param_bool(body, "IncludeReturnPath", false),
                param_bool(body, "UseMiddleboxes", false),
            );
            let ra_id = ra["RouteAnalysisId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.route_analyses
                .entry(gn_id)
                .or_default()
                .insert(ra_id, ra.clone());
            ok(&json!({"RouteAnalysis": ra}))
        }
        "GetRouteAnalysis" => {
            let gn_id = mpart(path, 2);
            let ra_id = mpart(path, 0);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            match st.route_analyses.get(&gn_id).and_then(|m| m.get(&ra_id)) {
                Some(ra) => ok(&json!({"RouteAnalysis": ra})),
                None => not_found(&ra_id),
            }
        }

        // ---------------------------------------------------------------
        // Network resources (moto's stubs)
        // ---------------------------------------------------------------
        "GetNetworkResources" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            ok(&json!({"NetworkResources": []}))
        }
        "GetNetworkResourceRelationships" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            ok(&json!({"Relationships": []}))
        }
        "GetNetworkResourceCounts" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            ok(&json!({"NetworkResourceCounts": []}))
        }
        "GetNetworkRoutes" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let identifier = body
                .get("RouteTableIdentifier")
                .cloned()
                .unwrap_or_else(|| json!({}));
            ok(&json!({
                "RouteTableArn": identifier["TransitGatewayRouteTableArn"].as_str().unwrap_or_default(),
                "CoreNetworkSegmentEdge": identifier["CoreNetworkSegmentEdge"],
                "RouteTableType": "TRANSIT_GATEWAY_ROUTE_TABLE",
                "RouteTableTimestamp": now_str(),
                "NetworkRoutes": [],
            }))
        }
        "GetNetworkTelemetry" => {
            let gn_id = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            ok(&json!({"NetworkTelemetry": []}))
        }
        "UpdateNetworkResourceMetadata" => {
            let gn_id = mpart(path, 3);
            let resource_arn = mpart(path, 1);
            if let Err(e) = gn_exists(st, &gn_id) {
                return e;
            }
            let metadata = body.get("Metadata").cloned().unwrap_or_else(|| json!({}));
            ok(&json!({"ResourceArn": resource_arn, "Metadata": metadata}))
        }

        // ---------------------------------------------------------------
        // Core network prefix list associations
        // ---------------------------------------------------------------
        "CreateCoreNetworkPrefixListAssociation" => {
            let cn_id = param_str(body, "CoreNetworkId");
            if let Some(cn) = cn_id {
                if let Err(e) = core_network(st, cn) {
                    return e;
                }
            }
            // moto reads the nonexistent `PrefixList` body member, so every
            // stored association carries an empty prefix list (its delete
            // therefore never matches a real ARN).
            let assoc = json!({
                "CoreNetworkId": cn_id,
                "PrefixList": "",
                "SegmentName": body.get("SegmentName").cloned().unwrap_or(Value::Null),
            });
            st.core_network_prefix_list_associations
                .entry(cn_id.unwrap_or_default().to_string())
                .or_default()
                .push(assoc.clone());
            ok(&json!({"PrefixListAssociation": assoc}))
        }
        "DeleteCoreNetworkPrefixListAssociation" => {
            let prefix_list_arn = mpart(path, 2);
            let cn_id = mpart(path, 0);
            let assocs = st
                .core_network_prefix_list_associations
                .entry(cn_id)
                .or_default();
            let position = assocs
                .iter()
                .position(|a| a["PrefixList"].as_str() == Some(prefix_list_arn.as_str()));
            match position {
                Some(index) => {
                    let assoc = assocs.remove(index);
                    ok(&json!({"PrefixListAssociation": assoc}))
                }
                None => not_found(&prefix_list_arn),
            }
        }
        "ListCoreNetworkPrefixListAssociations" => {
            let cn_id = mpart(path, 0);
            if let Err(e) = core_network(st, &cn_id) {
                return e;
            }
            let list = st
                .core_network_prefix_list_associations
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            ok(&json!({"PrefixListAssociations": list}))
        }

        // ---------------------------------------------------------------
        // Routing policy labels
        // ---------------------------------------------------------------
        "PutAttachmentRoutingPolicyLabel" => {
            // moto reads the nonexistent `Label` body member, so the stored
            // label is always the empty string.
            let attachment_id = param_str(body, "AttachmentId")
                .unwrap_or_default()
                .to_string();
            let Some(attachment) = st.attachments.get(&attachment_id).cloned() else {
                return not_found(&attachment_id);
            };
            let cn_id = attachment["CoreNetworkId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            st.routing_policy_labels
                .entry(cn_id)
                .or_default()
                .insert(attachment_id.clone(), String::new());
            ok(&json!({"AttachmentId": attachment_id, "Label": ""}))
        }
        "RemoveAttachmentRoutingPolicyLabel" => {
            // moto never checks existence here; it pops what it finds and
            // always answers the attachment id.
            let cn_id = mpart(path, 2);
            let attachment_id = mpart(path, 0);
            st.routing_policy_labels
                .entry(cn_id)
                .or_default()
                .remove(&attachment_id);
            if let Some(att) = st.attachments.get_mut(&attachment_id) {
                att["RoutingPolicyLabel"] = Value::Null;
            }
            ok(&json!({"AttachmentId": attachment_id}))
        }
        "ListAttachmentRoutingPolicyAssociations" => {
            let cn_id = mpart(path, 0);
            if let Err(e) = core_network(st, &cn_id) {
                return e;
            }
            let labels = st
                .routing_policy_labels
                .get(&cn_id)
                .cloned()
                .unwrap_or_default();
            let list: Vec<Value> = labels
                .iter()
                .map(|(aid, label)| json!({"AttachmentId": aid, "Label": label}))
                .collect();
            ok(&json!({"AttachmentRoutingPolicyAssociations": list}))
        }
        "ListCoreNetworkRoutingInformation" => {
            let cn_id = mpart(path, 1);
            if let Err(e) = core_network(st, &cn_id) {
                return e;
            }
            ok(&json!({"CoreNetworkRoutingInformation": []}))
        }

        // ---------------------------------------------------------------
        // Organization service access
        // ---------------------------------------------------------------
        "StartOrganizationServiceAccessUpdate" => {
            let action = param_str(body, "Action").unwrap_or("ENABLE").to_string();
            st.organization_service_access = Some(action);
            let enabled = st.organization_service_access.as_deref() == Some("ENABLE");
            ok(&json!({"OrganizationStatus": organization_status_json(enabled)}))
        }
        "ListOrganizationServiceAccessStatus" => {
            let enabled = st.organization_service_access.as_deref() == Some("ENABLE");
            ok(&json!({"OrganizationStatus": organization_status_json(enabled)}))
        }

        _ => not_implemented(op),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn mk(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "networkmanager".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: Default::default(),
            body: serde_json::to_vec(&body)
                .map(Bytes::from)
                .unwrap_or_default(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn run(req: &Request) -> Response {
        dispatch(req).unwrap()
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn err_code(resp: &Response) -> String {
        resp.headers
            .iter()
            .find(|(k, _)| k == "x-amzn-ErrorType")
            .map(|(_, v)| v.trim_end_matches(':').to_string())
            .unwrap_or_else(|| {
                body_of(resp)["__type"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
    }

    /// Creates one global network on a fresh account so tests do not share
    /// state; returns the wire dict.
    fn fresh_gn(account: u64, description: &str) -> Value {
        let mut req = mk(
            "POST",
            "/global-networks",
            json!({"Description": description}),
        );
        req.account = account;
        body_of(&run(&req))["GlobalNetwork"].clone()
    }

    #[test]
    fn routes_follow_botocore_requesturis() {
        assert_eq!(
            resolve_operation("POST", "/global-networks"),
            Some("CreateGlobalNetwork")
        );
        assert_eq!(
            resolve_operation("GET", "/global-networks/gn-1/devices"),
            Some("GetDevices")
        );
        assert_eq!(
            resolve_operation("POST", "/attachments/att-1/accept"),
            Some("AcceptAttachment")
        );
        assert_eq!(
            resolve_operation("DELETE", "/tags/arn%3Aaws%3Awhat%2Fever%2Fmore"),
            Some("UntagResource")
        );
        // Greedy terminal still matches a multi-segment raw ARN.
        assert_eq!(
            resolve_operation("GET", "/tags/arn:x:global-network/gn-1"),
            Some("ListTagsForResource")
        );
        assert_eq!(
            resolve_operation("DELETE", "/global-networks/gn-1"),
            Some("DeleteGlobalNetwork")
        );
        // Deeper literal prefixes beat the shorter greedy parent.
        assert_eq!(
            resolve_operation("DELETE", "/global-networks/gn-1/sites/site-1"),
            Some("DeleteSite")
        );
        assert_eq!(
            resolve_operation("PATCH", "/core-networks/cn-1"),
            Some("UpdateCoreNetwork")
        );
        assert_eq!(
            resolve_operation("GET", "/routing-policy-label/core-network/cn-1"),
            Some("ListAttachmentRoutingPolicyAssociations")
        );
        assert_eq!(
            resolve_operation("DELETE", "/prefix-list/p-arn/core-network/cn-1"),
            Some("DeleteCoreNetworkPrefixListAssociation")
        );
        assert_eq!(
            resolve_operation(
                "POST",
                "/core-networks/cn-1/core-network-change-sets/3/execute"
            ),
            Some("ExecuteCoreNetworkChangeSet")
        );
        assert_eq!(
            resolve_operation("GET", "/core-networks/cn-1/core-network-policy"),
            Some("GetCoreNetworkPolicy")
        );
        assert_eq!(resolve_operation("PATCH", "/nonsense/x"), None);
    }

    #[test]
    fn global_network_lifecycle() {
        let account = 111000111001;
        let gn = fresh_gn(account, "lifecycle-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();
        let gn_arn = gn["GlobalNetworkArn"].as_str().unwrap().to_string();
        assert_eq!(gn["State"], json!("AVAILABLE"));
        assert_eq!(gn["Description"], json!("lifecycle-gn"));
        assert!(gn_arn.starts_with(&format!("arn:aws:networkmanager:{account}:global-network/")));

        // describe by id
        let mut req = mk("GET", "/global-networks", json!({}));
        req.account = account;
        req.query_string = format!("globalNetworkIds={}", urlencoding::encode(&gn_id));
        let listed = body_of(&run(&req));
        assert!(listed["GlobalNetworks"].as_array().unwrap().len() == 1);
        assert!(listed["nextToken"].is_null());

        // update description
        let mut req = mk(
            "PATCH",
            &format!("/global-networks/{gn_id}"),
            json!({"Description": "renamed"}),
        );
        req.account = account;
        let updated = body_of(&run(&req));
        assert_eq!(updated["GlobalNetwork"]["Description"], json!("renamed"));

        // delete and confirm gone
        let mut req = mk("DELETE", &format!("/global-networks/{gn_id}"), json!({}));
        req.account = account;
        let deleted = body_of(&run(&req));
        assert_eq!(deleted["GlobalNetwork"]["State"], json!("DELETING"));
        let fake = "global-network-000000000000000000";
        let mut req = mk("DELETE", &format!("/global-networks/{fake}"), json!({}));
        req.account = account;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["ResourceId"], json!(fake));
        assert_eq!(err_code(&resp), "NotFoundException");
    }

    #[test]
    fn site_and_tag_round_trip() {
        let account = 111000111002;
        let gn = fresh_gn(account, "site-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();

        let mut req = mk(
            "POST",
            &format!("/global-networks/{gn_id}/sites"),
            json!({"Description": "hq", "Location": {"Address": "1 Main St"}}),
        );
        req.account = account;
        let site = body_of(&run(&req))["Site"].clone();
        assert_eq!(site["GlobalNetworkId"], json!(gn_id));
        assert_eq!(site["State"], json!("AVAILABLE"));

        // list with a real gn
        let mut req = mk("GET", &format!("/global-networks/{gn_id}/sites"), json!({}));
        req.account = account;
        assert_eq!(body_of(&run(&req))["Sites"].as_array().unwrap().len(), 1);

        // unknown gn lists raise ValidationException (moto's paginated Get)
        let mut req = mk(
            "GET",
            "/global-networks/global-network-000000000000000000/devices",
            json!({}),
        );
        req.account = account;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(err_code(&resp), "ValidationException");

        // tags on the created site arn
        let site_arn = site["SiteArn"].as_str().unwrap().to_string();
        let mut req = mk(
            "POST",
            &format!("/tags/{}", urlencoding::encode(&site_arn)),
            json!({"Tags": [{"Key": "env", "Value": "test"}]}),
        );
        req.account = account;
        assert_eq!(run(&req).status, 200);
        let mut req = mk(
            "GET",
            &format!("/tags/{}", urlencoding::encode(&site_arn)),
            json!({}),
        );
        req.account = account;
        let tags = body_of(&run(&req));
        assert_eq!(tags["TagList"], json!([{"Key": "env", "Value": "test"}]));

        // untag by key
        let mut req = mk(
            "DELETE",
            &format!("/tags/{}", urlencoding::encode(&site_arn)),
            json!({}),
        );
        req.account = account;
        req.query_string = "tagKeys=env".to_string();
        assert_eq!(run(&req).status, 200);
        // NotFound: an unknown kind / id answers ResourceNotFound.
        let fake_arn = "arn:aws:networkmanager:att-site:device/device-000000000000000000";
        let mut req = mk(
            "GET",
            &format!("/tags/{}", urlencoding::encode(fake_arn)),
            json!({}),
        );
        req.account = account;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn core_network_policy_lifecycle() {
        let account = 111000111003;
        let gn = fresh_gn(account, "cn-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();

        let mut req = mk(
            "POST",
            "/core-networks",
            json!({"GlobalNetworkId": gn_id, "Description": "core"}),
        );
        req.account = account;
        let cn = body_of(&run(&req))["CoreNetwork"].clone();
        let cn_id = cn["CoreNetworkId"].as_str().unwrap().to_string();
        assert_eq!(cn["State"], json!("AVAILABLE"));
        assert_eq!(cn["OwnerAccountId"], json!(account.to_string()));

        // get before any policy -> NotFound
        let mut req = mk(
            "GET",
            &format!("/core-networks/{cn_id}/core-network-policy"),
            json!({}),
        );
        req.account = account;
        assert_eq!(err_code(&run(&req)), "NotFoundException");

        // put a policy -> version 1
        let mut req = mk(
            "POST",
            &format!("/core-networks/{cn_id}/core-network-policy"),
            json!({"PolicyDocument": "{\"version\":\"2021.12\"}"}),
        );
        req.account = account;
        let policy = body_of(&run(&req))["CoreNetworkPolicy"].clone();
        assert_eq!(policy["PolicyVersionId"], json!(1));
        assert_eq!(policy["Alias"], json!("LATEST"));
        assert_eq!(policy["ChangeSetState"], json!("READY_TO_EXECUTE"));

        // list versions
        let mut req = mk(
            "GET",
            &format!("/core-networks/{cn_id}/core-network-policy-versions"),
            json!({}),
        );
        req.account = account;
        let versions = body_of(&run(&req))["CoreNetworkPolicyVersions"].clone();
        assert_eq!(versions.as_array().unwrap().len(), 1);
        assert!(versions[0].get("PolicyDocument").is_none());

        // execute change set for v1 -> LIVE / EXECUTION_SUCCEEDED
        let mut req = mk(
            "POST",
            &format!("/core-networks/{cn_id}/core-network-change-sets/1/execute"),
            json!({}),
        );
        req.account = account;
        assert_eq!(run(&req).status, 200);
        let mut req = mk(
            "GET",
            &format!("/core-networks/{cn_id}/core-network-policy"),
            json!({}),
        );
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["CoreNetworkPolicy"]["Alias"],
            json!("LIVE")
        );

        // executing a missing version is a NotFound
        let mut req = mk(
            "POST",
            &format!("/core-networks/{cn_id}/core-network-change-sets/9/execute"),
            json!({}),
        );
        req.account = account;
        let resp = run(&req);
        assert_eq!(resp.status, 400);
        assert_eq!(err_code(&resp), "NotFoundException");
    }

    #[test]
    fn attachment_lifecycle() {
        let account = 111000111004;
        let gn = fresh_gn(account, "att-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();

        let mut req = mk("POST", "/core-networks", json!({"GlobalNetworkId": gn_id}));
        req.account = account;
        let cn_id = body_of(&run(&req))["CoreNetwork"]["CoreNetworkId"]
            .as_str()
            .unwrap()
            .to_string();

        let mut req = mk(
            "POST",
            "/vpc-attachments",
            json!({"CoreNetworkId": cn_id, "VpcArn": "arn:aws:ec2:us-east-1:1:vpc/vpc-1",
                   "SubnetArns": ["arn:aws:ec2:us-east-1:1:subnet/sub-1"]}),
        );
        req.account = account;
        let vpc_att = body_of(&run(&req))["VpcAttachment"].clone();
        assert_eq!(vpc_att["Attachment"]["AttachmentType"], json!("VPC"));
        assert_eq!(vpc_att["Attachment"]["CoreNetworkId"], json!(cn_id));
        assert_eq!(vpc_att["Attachment"]["State"], json!("AVAILABLE"));
        assert_eq!(
            vpc_att["SubnetArns"],
            json!(["arn:aws:ec2:us-east-1:1:subnet/sub-1"])
        );

        let att_id = vpc_att["Attachment"]["AttachmentId"]
            .as_str()
            .unwrap()
            .to_string();

        // typed read
        let mut req = mk("GET", &format!("/vpc-attachments/{att_id}"), json!({}));
        req.account = account;
        let got = body_of(&run(&req))["VpcAttachment"].clone();
        assert_eq!(got["Options"]["Ipv6Support"], json!(false));
        assert_eq!(got["Attachment"]["AttachmentId"], json!(att_id));

        // accept -> AVAILABLE; reject -> REJECTED
        let mut req = mk("POST", &format!("/attachments/{att_id}/accept"), json!({}));
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["Attachment"]["State"],
            json!("AVAILABLE")
        );

        let mut req = mk("POST", &format!("/attachments/{att_id}/reject"), json!({}));
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["Attachment"]["State"],
            json!("REJECTED")
        );

        // list with a type filter (wire camel keys)
        let mut req = mk("GET", "/attachments", json!({}));
        req.account = account;
        req.query_string =
            "attachmentType=VPC&coreNetworkId=core-network-000000000000000000".to_string();
        assert_eq!(
            body_of(&run(&req))["Attachments"].as_array().unwrap().len(),
            0
        );

        // delete removes base and typed entries
        let mut req = mk("DELETE", &format!("/attachments/{att_id}"), json!({}));
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["Attachment"]["State"],
            json!("DELETING")
        );
        let mut req = mk("GET", &format!("/vpc-attachments/{att_id}"), json!({}));
        req.account = account;
        assert_eq!(err_code(&run(&req)), "NotFoundException");
    }

    #[test]
    fn peering_lifecycle_and_errors() {
        let account = 111000111005;
        let gn = fresh_gn(account, "peer-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();

        let mut req = mk("POST", "/core-networks", json!({"GlobalNetworkId": gn_id}));
        req.account = account;
        let cn_id = body_of(&run(&req))["CoreNetwork"]["CoreNetworkId"]
            .as_str()
            .unwrap()
            .to_string();

        // peering with an unknown core network -> NotFound
        let mut req = mk(
            "POST",
            "/transit-gateway-peerings",
            json!({"CoreNetworkId": "core-network-000000000000000000", "TransitGatewayArn": "arn:aws:ec2:us-east-1:1:transit-gateway/tgw-1"}),
        );
        req.account = account;
        assert_eq!(err_code(&run(&req)), "NotFoundException");

        // peering -> delete
        let mut req = mk(
            "POST",
            "/transit-gateway-peerings",
            json!({"CoreNetworkId": cn_id, "TransitGatewayArn": "arn:aws:ec2:us-east-1:1:transit-gateway/tgw-1"}),
        );
        req.account = account;
        let extras = body_of(&run(&req))["TransitGatewayPeering"].clone();
        assert_eq!(extras["Peering"]["PeeringType"], json!("TRANSIT_GATEWAY"));
        assert_eq!(
            extras["Peering"]["ResourceArn"],
            json!("arn:aws:ec2:us-east-1:1:transit-gateway/tgw-1")
        );
        let peering_id = extras["Peering"]["PeeringId"].as_str().unwrap().to_string();

        // route table attachment requires a real peering
        let mut req = mk(
            "POST",
            "/transit-gateway-route-table-attachments",
            json!({"PeeringId": peering_id, "TransitGatewayRouteTableArn": "arn:aws:ec2:us-east-1:1:transit-gateway-route-table/rtb-1"}),
        );
        req.account = account;
        let att = body_of(&run(&req))["TransitGatewayRouteTableAttachment"].clone();
        assert_eq!(att["Attachment"]["CoreNetworkId"], json!(cn_id));
        assert_eq!(att["PeeringId"], json!(peering_id));
        assert_eq!(
            att["Attachment"]["AttachmentType"],
            json!("TRANSIT_GATEWAY_ROUTE_TABLE")
        );

        let mut req = mk("DELETE", &format!("/peerings/{peering_id}"), json!({}));
        req.account = account;
        assert_eq!(body_of(&run(&req))["Peering"]["State"], json!("DELETING"));
        let mut req = mk("DELETE", "/peerings/peering-000000000000000000", json!({}));
        req.account = account;
        assert_eq!(err_code(&run(&req)), "NotFoundException");
    }

    #[test]
    fn resource_policies_and_org_service_access() {
        let account = 111000111006;
        let gn = fresh_gn(account, "policy-gn");
        let gn_arn = gn["GlobalNetworkArn"].as_str().unwrap().to_string();

        // default org status is DISABLED
        let mut req = mk("GET", "/organizations/service-access", json!({}));
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["OrganizationStatus"]["OrganizationAwsServiceAccessStatus"],
            json!("DISABLED")
        );

        let mut req = mk(
            "POST",
            "/organizations/service-access",
            json!({"Action": "ENABLE"}),
        );
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["OrganizationStatus"]["OrganizationAwsServiceAccessStatus"],
            json!("ENABLED")
        );

        // resource policy put/get/delete
        let mut req = mk(
            "POST",
            &format!("/resource-policy/{}", urlencoding::encode(&gn_arn)),
            json!({"PolicyDocument": "{\"Version\":\"2012-10-17\"}"}),
        );
        req.account = account;
        assert_eq!(run(&req).status, 200);
        let mut req = mk(
            "GET",
            &format!("/resource-policy/{}", urlencoding::encode(&gn_arn)),
            json!({}),
        );
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["PolicyDocument"],
            json!("{\"Version\":\"2012-10-17\"}")
        );
        let mut req = mk(
            "DELETE",
            &format!("/resource-policy/{}", urlencoding::encode(&gn_arn)),
            json!({}),
        );
        req.account = account;
        assert_eq!(run(&req).status, 200);
        let mut req = mk(
            "GET",
            &format!("/resource-policy/{}", urlencoding::encode(&gn_arn)),
            json!({}),
        );
        req.account = account;
        assert_eq!(body_of(&run(&req)), json!({}));
    }

    #[test]
    fn route_analysis_round_trip() {
        let account = 111000111007;
        let gn = fresh_gn(account, "ra-gn");
        let gn_id = gn["GlobalNetworkId"].as_str().unwrap().to_string();

        let mut req = mk(
            "POST",
            &format!("/global-networks/{gn_id}/route-analyses"),
            json!({
                "Source": {"IpAddress": "10.0.0.1"},
                "Destination": {"IpAddress": "10.0.1.1"}
            }),
        );
        req.account = account;
        let ra = body_of(&run(&req))["RouteAnalysis"].clone();
        assert_eq!(ra["GlobalNetworkId"], json!(gn_id));
        assert_eq!(ra["Status"], json!("COMPLETED"));
        assert_eq!(
            ra["ForwardPath"]["CompletionStatus"]["ResultCode"],
            json!("CONNECTED")
        );

        let ra_id = ra["RouteAnalysisId"].as_str().unwrap().to_string();
        let mut req = mk(
            "GET",
            &format!("/global-networks/{gn_id}/route-analyses/{ra_id}"),
            json!({}),
        );
        req.account = account;
        assert_eq!(
            body_of(&run(&req))["RouteAnalysis"]["RouteAnalysisId"],
            json!(ra_id)
        );

        let mut req = mk(
            "GET",
            &format!("/global-networks/{gn_id}/route-analyses/route-analysis-000000000000000000"),
            json!({}),
        );
        req.account = account;
        let resp = run(&req);
        assert_eq!(err_code(&resp), "NotFoundException");
    }

    #[test]
    fn unknown_route_is_not_implemented() {
        let mut req = mk(
            "GET",
            "/global-networks/gn-1/nonexistent-subresource",
            json!({}),
        );
        req.account = 111000111008;
        let resp = run(&req);
        assert_eq!(resp.status, 501);
        assert_eq!(body_of(&resp)["__type"], json!("NotImplemented"));
    }
}
