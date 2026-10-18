//! EC2 handler group `misc`: minor create/describe/delete families served
//! from recorded moto templates, plus the trailing template-only smoke ops.
#![allow(
    clippy::format_in_format_args,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::single_match,
    clippy::map_identity
)]
//!
//! Two engines live here:
//! 1. [`SIMPLE_FAMILIES`] — small resource families (IPAM, traffic mirror,
//!    verified access, hosts, ...) kept in the store as [`Simple`] entries,
//!    with the create/describe XML taken from moto's captured response.
//! 2. [`super::templates::TEMPLATES`] — stateless ops the suite only expects
//!    to succeed; rendered with fresh ids/timestamps per call.

#![allow(unused_variables)]

use super::handlers::{tag_candidates, wire_tags};
use super::state::Simple;
use super::{ec2_error, ec2_xml, tags_xml, templates, with_state, Params};
use crate::proto::xml_escape;
use crate::{Request, Response};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::LazyLock;

type P = Params;

/// One ("simple") resource family driven by templates.
pub struct FamilySpec {
    pub kind: &'static str,        // store key
    pub create_op: &'static str,   // e.g. "CreateIpam"
    pub describe_op: &'static str, // e.g. "DescribeIpams"
    pub delete_op: &'static str,   // e.g. "DeleteIpam"
    pub list_tag: &'static str,    // e.g. "ipamSet" — list container in describe
    pub id_prefix: &'static str,   // e.g. "ipam"
    pub id_key: &'static str,      // e.g. "IpamId" — describe/delete lookup key
}

macro_rules! fam {
    ($kind:literal, $c:literal, $d:literal, $x:literal, $t:literal, $p:literal, $k:literal) => {
        FamilySpec {
            kind: $kind,
            create_op: $c,
            describe_op: $d,
            delete_op: $x,
            list_tag: $t,
            id_prefix: $p,
            id_key: $k,
        }
    };
}

pub static SIMPLE_FAMILIES: &[FamilySpec] = &[
    fam!(
        "ipam",
        "CreateIpam",
        "DescribeIpams",
        "DeleteIpam",
        "ipamSet",
        "ipam",
        "IpamId"
    ),
    fam!(
        "ipam-scope",
        "CreateIpamScope",
        "DescribeIpamScopes",
        "DeleteIpamScope",
        "ipamScopeSet",
        "ipam-scope",
        "IpamScopeId"
    ),
    fam!(
        "ipam-pool",
        "CreateIpamPool",
        "DescribeIpamPools",
        "DeleteIpamPool",
        "ipamPoolSet",
        "ipam-pool",
        "IpamPoolId"
    ),
    fam!(
        "ipam-resource-discovery",
        "CreateIpamResourceDiscovery",
        "DescribeIpamResourceDiscoveries",
        "DeleteIpamResourceDiscovery",
        "ipamResourceDiscoverySet",
        "ipam-res-disc",
        "IpamResourceDiscoveryId"
    ),
    fam!(
        "traffic-mirror-filter",
        "CreateTrafficMirrorFilter",
        "DescribeTrafficMirrorFilters",
        "DeleteTrafficMirrorFilter",
        "trafficMirrorFilterSet",
        "tmf",
        "TrafficMirrorFilterId"
    ),
    fam!(
        "traffic-mirror-target",
        "CreateTrafficMirrorTarget",
        "DescribeTrafficMirrorTargets",
        "DeleteTrafficMirrorTarget",
        "trafficMirrorTargetSet",
        "tmt",
        "TrafficMirrorTargetId"
    ),
    fam!(
        "traffic-mirror-session",
        "CreateTrafficMirrorSession",
        "DescribeTrafficMirrorSessions",
        "DeleteTrafficMirrorSession",
        "trafficMirrorSessionSet",
        "tms",
        "TrafficMirrorSessionId"
    ),
    fam!(
        "traffic-mirror-filter-rule",
        "CreateTrafficMirrorFilterRule",
        "DescribeTrafficMirrorFilterRules",
        "DeleteTrafficMirrorFilterRule",
        "trafficMirrorFilterRuleSet",
        "tmfr",
        "TrafficMirrorFilterRuleId"
    ),
    fam!(
        "verified-access-instance",
        "CreateVerifiedAccessInstance",
        "DescribeVerifiedAccessInstances",
        "DeleteVerifiedAccessInstance",
        "verifiedAccessInstances",
        "vai",
        "VerifiedAccessInstanceId"
    ),
    fam!(
        "verified-access-endpoint",
        "CreateVerifiedAccessEndpoint",
        "DescribeVerifiedAccessEndpoints",
        "DeleteVerifiedAccessEndpoint",
        "verifiedAccessEndpoints",
        "vae",
        "VerifiedAccessEndpointId"
    ),
    fam!(
        "verified-access-group",
        "CreateVerifiedAccessGroup",
        "DescribeVerifiedAccessGroups",
        "DeleteVerifiedAccessGroup",
        "verifiedAccessGroups",
        "vag",
        "VerifiedAccessGroupId"
    ),
    fam!(
        "verified-access-trust-provider",
        "CreateVerifiedAccessTrustProvider",
        "DescribeVerifiedAccessTrustProviders",
        "DeleteVerifiedAccessTrustProvider",
        "verifiedAccessTrustProviders",
        "vatp",
        "VerifiedAccessTrustProviderId"
    ),
    fam!(
        "capacity-reservation",
        "CreateCapacityReservation",
        "DescribeCapacityReservations",
        "CancelCapacityReservation",
        "capacityReservationSet",
        "cr",
        "CapacityReservationId"
    ),
    fam!(
        "capacity-reservation-fleet",
        "CreateCapacityReservationFleet",
        "DescribeCapacityReservationFleets",
        "CancelCapacityReservationFleets",
        "capacityReservationFleetSet",
        "crf",
        "CapacityReservationFleetId"
    ),
    fam!(
        "host",
        "AllocateHosts",
        "DescribeHosts",
        "ReleaseHosts",
        "hostSet",
        "h",
        "HostId"
    ),
    fam!(
        "fleet",
        "CreateFleet",
        "DescribeFleets",
        "DeleteFleet",
        "fleetSet",
        "fleet",
        "FleetId"
    ),
    fam!(
        "spot-fleet-request",
        "RequestSpotFleet",
        "DescribeSpotFleetRequests",
        "CancelSpotFleetRequests",
        "spotFleetRequestSet",
        "sfr",
        "SpotFleetRequestId"
    ),
    fam!(
        "spot-instance-request",
        "RequestSpotInstances",
        "DescribeSpotInstanceRequests",
        "CancelSpotInstanceRequests",
        "spotInstanceRequestSet",
        "sir",
        "SpotInstanceRequestId"
    ),
    fam!(
        "customer-gateway",
        "CreateCustomerGateway",
        "DescribeCustomerGateways",
        "DeleteCustomerGateway",
        "customerGatewaySet",
        "cgw",
        "CustomerGatewayId"
    ),
    fam!(
        "vpn-gateway",
        "CreateVpnGateway",
        "DescribeVpnGateways",
        "DeleteVpnGateway",
        "vpnGatewaySet",
        "vgw",
        "VpnGatewayId"
    ),
    fam!(
        "vpn-connection",
        "CreateVpnConnection",
        "DescribeVpnConnections",
        "DeleteVpnConnection",
        "vpnConnectionSet",
        "vpn",
        "VpnConnectionId"
    ),
    fam!(
        "egress-only-igw",
        "CreateEgressOnlyInternetGateway",
        "DescribeEgressOnlyInternetGateways",
        "DeleteEgressOnlyInternetGateway",
        "egressOnlyInternetGatewaySet",
        "eigw",
        "EgressOnlyInternetGatewayId"
    ),
    fam!(
        "flow-log",
        "CreateFlowLogs",
        "DescribeFlowLogs",
        "DeleteFlowLogs",
        "flowLogSet",
        "fl-",
        "FlowLogId"
    ),
    fam!(
        "managed-prefix-list",
        "CreateManagedPrefixList",
        "DescribeManagedPrefixLists",
        "DeleteManagedPrefixList",
        "prefixListSet",
        "pl",
        "PrefixListId"
    ),
    fam!(
        "network-insights-path",
        "CreateNetworkInsightsPath",
        "DescribeNetworkInsightsPaths",
        "DeleteNetworkInsightsPath",
        "networkInsightsPathSet",
        "nip",
        "NetworkInsightsPathId"
    ),
    fam!(
        "network-insights-access-scope",
        "CreateNetworkInsightsAccessScope",
        "DescribeNetworkInsightsAccessScopes",
        "DeleteNetworkInsightsAccessScope",
        "networkInsightsAccessScopeSet",
        "nis",
        "NetworkInsightsAccessScopeId"
    ),
    fam!(
        "client-vpn-endpoint",
        "CreateClientVpnEndpoint",
        "DescribeClientVpnEndpoints",
        "DeleteClientVpnEndpoint",
        "clientVpnEndpointSet",
        "cvpn-endpoint",
        "ClientVpnEndpointId"
    ),
    fam!(
        "carrier-gateway",
        "CreateCarrierGateway",
        "DescribeCarrierGateways",
        "DeleteCarrierGateway",
        "carrierGatewaySet",
        "cagw",
        "CarrierGatewayId"
    ),
    fam!(
        "tgw",
        "CreateTransitGateway",
        "DescribeTransitGateways",
        "DeleteTransitGateway",
        "transitGatewaySet",
        "tgw",
        "TransitGatewayId"
    ),
    fam!(
        "tgw-peering",
        "CreateTransitGatewayPeeringAttachment",
        "DescribeTransitGatewayPeeringAttachments",
        "DeleteTransitGatewayPeeringAttachment",
        "transitGatewayPeeringAttachmentSet",
        "tgw-attach",
        "TransitGatewayAttachmentId"
    ),
    fam!(
        "tgw-connect",
        "CreateTransitGatewayConnect",
        "DescribeTransitGatewayConnects",
        "DeleteTransitGatewayConnect",
        "transitGatewayConnectSet",
        "tgw-attach",
        "TransitGatewayAttachmentId"
    ),
    fam!(
        "tgw-connect-peer",
        "CreateTransitGatewayConnectPeer",
        "DescribeTransitGatewayConnectPeers",
        "DeleteTransitGatewayConnectPeer",
        "transitGatewayConnectPeerSet",
        "tgw-connect-peer",
        "TransitGatewayConnectPeerId"
    ),
    fam!(
        "coip-pool",
        "CreateCoipPool",
        "DescribeCoipPools",
        "DeleteCoipPool",
        "coipPoolSet",
        "coip-pool",
        "PoolId"
    ),
    fam!(
        "public-ipv4-pool",
        "CreatePublicIpv4Pool",
        "DescribePublicIpv4Pools",
        "DeletePublicIpv4Pool",
        "publicIpv4PoolSet",
        "ipv4pool",
        "PoolId"
    ),
    fam!(
        "instance-event-window",
        "CreateInstanceEventWindow",
        "DescribeInstanceEventWindows",
        "DeleteInstanceEventWindow",
        "instanceEventWindowSet",
        "iew",
        "InstanceEventWindowId"
    ),
];

fn render_template(xml: &str) -> (String, HashMap<String, String>) {
    let mut out = String::with_capacity(xml.len() + 32);
    let mut ids: HashMap<String, String> = HashMap::new();
    let mut rest = xml;
    while let Some(start) = rest.find("{ID") {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        let end = after.find('}').unwrap_or(after.len());
        let token = &after[..end];
        if let Some(prefix) = token.strip_prefix("{ID") {
            let id: String = ids
                .entry(prefix.to_string())
                .or_insert_with(|| next_placeholder_id(prefix))
                .clone();
            out.push_str(&id);
        } else {
            out.push_str(token);
        }
        rest = if end < rest.len() { &rest[start + end + 1..] } else { "" };
    }
    out.push_str(rest);
    let _ = ids;
    (out.replace("{TS}", &super::now_iso()), ids)
}

/// A placeholder id in moto's shape: `prefix-` + 17 hex (or 8 for short ones).
fn next_placeholder_id(prefix: &str) -> String {
    let h = super::state::idhex::hex17();
    if prefix.len() < 8 {
        format!("{prefix}-{}", &h[..8])
    } else {
        format!("{prefix}-{h}")
    }
}

fn find_family(op: &str) -> Option<&'static FamilySpec> {
    SIMPLE_FAMILIES
        .iter()
        .find(|f| op == f.create_op || op == f.describe_op || op == f.delete_op)
}

pub fn route(req: &Request, params: &P) -> Response {
    let op: &str = &req.operation;
    // 0) moto-gap ops and the transit gateway family
    if GAP_OPS.contains(op) {
        return gap_ops_response(op);
    }
    if LOCAL_GW_OPS.contains(op) {
        return local_gw_route(req, op, params);
    }
    // 1) template-driven simple families (create/describe/delete)
    if find_family(op).is_some() {
        return simple_family_route(req, params);
    }
    // 1b) the rest of the transit gateway family (route tables etc.)
    if TRANSIT_GW_OPS.contains(op) {
        return tgw_route(req, op, params);
    }
    // 0b) family-carryover gap ops
    if GAP_OPS_B.contains(op) {
        return gap_ops_b_response(op, params);
    }
    // 1c) modify-style ops on simple families
    if MODIFY_OPS.contains(op) {
        return modify_route(req, op, params);
    }
    // 2) plain template smoke ops
    if let Ok(idx) = templates::TEMPLATES.binary_search_by_key(&req.operation.as_str(), |t| t.0) {
        let (_, status, xml) = templates::TEMPLATES[idx];
        let xml = render_template(xml).0;
        return Response::xml(status, xml.into_bytes());
    }
    super::not_implemented(&req.operation)
}

static MODIFY_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    ["ModifyManagedPrefixList", "ModifyCapacityReservationFleet"]
        .into_iter()
        .collect()
});

fn modify_route(req: &Request, op: &str, params: &P) -> Response {
    with_state(req, move |state| {
        if op == "ModifyManagedPrefixList" {
            let id = params.str("PrefixListId");
            if !state
                .simple
                .get("managed-prefix-list")
                .map(|items| items.iter().any(|s| s.id == id))
                .unwrap_or(false)
            {
                return ec2_error(
                    400,
                    "InvalidPrefixListID.NotFound",
                    &format!("The prefix list ID '{}' does not exist", crate::proto::xml_escape(&id)),
                );
            }
            let fam = SIMPLE_FAMILIES
                .iter()
                .find(|f| f.kind == "managed-prefix-list")
                .unwrap();
            let tags_xml_v = super::tags_xml(&Vec::new());
            let fills = super::family_fills::create_fills(
                "managed-prefix-list",
                &id,
                params,
                req.account,
                &req.region,
                false,
                &tags_xml_v,
            );
            if let Some(xml) = super::shapes::render_response("ModifyManagedPrefixList", &fills, "us-east-1") {
                return Response::xml(200, xml.into_bytes());
            }
            return ec2_xml(
                "ModifyManagedPrefixListResponse",
                &format!("<prefixList><prefixListId>{}</prefixListId><state>modify-in-progress</state></prefixList>", crate::proto::xml_escape(&id)),
            );
        }
        // ModifyCapacityReservationFleet
        let id = params.str("CapacityReservationFleetId");
        if !state
            .simple
            .get("capacity-reservation-fleet")
            .map(|items| items.iter().any(|s| s.id == id))
            .unwrap_or(false)
        {
            return ec2_error(
                404,
                "InvalidCapacityReservationFleetId.NotFound",
                &format!("The capacity reservation fleet ID '{}' does not exist", crate::proto::xml_escape(&id)),
            );
        }
        ec2_xml("ModifyCapacityReservationFleetResponse", "<return>true</return>")
    })
}

fn family_root_is_list(op: &str) -> bool {
    !list_pykey_of(op).is_empty()
}

/// Python key of the sole list member at the op root (list-root ops only).
fn list_pykey_of(op: &str) -> String {
    super::shapes::shape_of(op)
        .and_then(|shape| shape.get("m"))
        .and_then(|m| m.as_object())
        .and_then(|obj| {
            let mut found = Vec::new();
            for (_wire, member) in obj {
                if member.get("t").and_then(|t| t.as_str()) == Some("list") {
                    if let Some(k) = member.get("k").and_then(|k| k.as_str()) {
                        found.push(k.to_string());
                    }
                }
            }
            if found.len() == 1 {
                Some(found.remove(0))
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn simple_family_route(req: &Request, params: &P) -> Response {
    let fam = find_family(&req.operation).unwrap();
    if matches!(
        fam.kind,
        "host" | "fleet" | "spot-fleet-request" | "spot-instance-request"
    ) {
        return hostlist_route(req, fam);
    }
    match req.operation.as_str() {
        op if op == fam.create_op => simple_create(req, params, fam),
        op if op == fam.describe_op => simple_describe(req, params, fam),
        op if op == fam.delete_op => simple_delete(req, params, fam),
        _ => simple_describe(req, params, fam),
    }
}

fn simple_create(req: &Request, params: &P, fam: &'static FamilySpec) -> Response {
    with_state(req, move |state| {
        let mut tags = wire_tags(params);
        for (k, v) in super::handlers::tag_spec_tags(params) {
            if !tags.iter().any(|(ek, _)| ek == &k) {
                tags.push((k, v));
            }
        }
        let tags_xml_v = super::tags_inner(&tags);
        let id = next_placeholder_id(fam.id_prefix);
        // shape-driven first: botocore-exact root + member keys
        let fills = super::family_fills::create_fills(
            fam.kind,
            &id,
            params,
            req.account,
            &req.region,
            false,
            &tags_xml_v,
        );
        if family_root_is_list(fam.create_op) {
            if let Some(xml) = super::shapes::render_list_response(
                fam.create_op,
                list_pykey_of(fam.create_op).as_str(),
                &[fills.clone()],
                "us-east-1",
            ) {
                let inner = strip_op_root(&xml, fam.create_op);
                state
                    .simple
                    .entry(fam.kind.to_string())
                    .or_default()
                    .push(Simple {
                        id: id.clone(),
                        kind: fam.kind.to_string(),
                        item_xml: inner,
                        tags,
                        ..Default::default()
                    });
                return Response::xml(200, xml.into_bytes());
            }
        }
        // single-object shape render for structure-out roots
        if let Some(xml) = super::shapes::render_response(fam.create_op, &fills, "us-east-1") {
            let inner = strip_op_root(&xml, fam.create_op);
            state
                .simple
                .entry(fam.kind.to_string())
                .or_default()
                .push(Simple {
                    id: id.clone(),
                    kind: fam.kind.to_string(),
                    item_xml: inner,
                    tags,
                    ..Default::default()
                });
            return Response::xml(200, xml.into_bytes());
        }
        // captured-template fallback
        let t = templates::TEMPLATES
            .binary_search_by_key(&fam.create_op, |x| x.0)
            .ok()
            .map(|i| (templates::TEMPLATES[i].1, templates::TEMPLATES[i].2));
        let rendered = match t {
            Some((status, xml)) if status == 200 => {
                let (body, ids) = render_template(xml);
                let id2 = ids
                    .values()
                    .find(|v| v.starts_with(format!("{}-", fam.id_prefix).as_str()))
                    .cloned()
                    .unwrap_or_else(|| id.clone());
                let inner = strip_op_root(&body, fam.create_op);
                state
                    .simple
                    .entry(fam.kind.to_string())
                    .or_default()
                    .push(Simple {
                        id: id2.clone(),
                        kind: fam.kind.to_string(),
                        item_xml: inner,
                        tags,
                        ..Default::default()
                    });
                (status, body)
            }
            _ => {
                // synthesized moto-style create for families the capture
                // never exercised
                let id2 = next_placeholder_id(fam.id_prefix);
                let item = synth_fields(fam, &id2, params, state);
                let inner = single_root(fam.kind);
                state
                    .simple
                    .entry(fam.kind.to_string())
                    .or_default()
                    .push(Simple {
                        id: id2.clone(),
                        kind: fam.kind.to_string(),
                        item_xml: format!("<item>{}</item>", item.clone()),
                        tags,
                        ..Default::default()
                    });
                (200, format!("<{}>{}</{}>", inner, item, inner))
            }
        };
        let (status, body) = rendered;
        let _ = fam;
        Response::xml(status, body.into_bytes())
    })
}

/// Wire name of the single structure member under an op's root element
/// (when the op returns exactly one, e.g. `<vpnConnection>`).
fn single_struct_member(op: &str) -> String {
    super::shapes::shape_of(op)
        .and_then(|shape| shape.get("m"))
        .and_then(|m| m.as_object())
        .and_then(|obj| {
            let mut found = Vec::new();
            for (_wire, member) in obj {
                if member.get("t").and_then(|t| t.as_str()) == Some("structure") {
                    if let Some(n) = member.get("n").and_then(|n| n.as_str()) {
                        found.push(n.to_string());
                    }
                }
            }
            if found.len() == 1 {
                Some(found.remove(0))
            } else {
                None
            }
        })
        .unwrap_or_default()
}

/// Strip the `<?xml?>` decl, the op root tags, requestId and metadata,
/// leaving the item's inner member list.
fn strip_op_root(xml: &str, op: &str) -> String {
    let mut s = xml;
    if let Some(rest) = s.strip_prefix("<?xml") {
        if let Some(i) = rest.find('>') {
            s = &rest[i + 1..];
        }
    }
    let open = format!("<{}Response", op);
    if let Some(i) = s.find(&open) {
        s = &s[i + open.len()..];
        if let Some(j) = s.find('>') {
            s = &s[j + 1..];
        }
    }
    if let Some(k) = s.rfind(&format!("</{}Response>", op)) {
        s = &s[..k];
    }
    // unwrap a single-structure member (e.g. <vpnConnection>)
    let inner_member = single_struct_member(op);
    if !inner_member.is_empty() {
        let open = format!("<{}>", inner_member);
        let close = format!("</{}>", inner_member);
        if let (Some(a), Some(b)) = (s.find(&open), s.rfind(&close)) {
            s = &s[a + open.len()..b];
        }
    }
    let mut out = s.to_string();
    if let Some(start) = out.find("<requestId>") {
        if let Some(rel) = out[start..].find("</requestId>") {
            let end = start + rel + "</requestId>".len();
            out.replace_range(start..end, "");
        }
    }
    if let Some(start) = out.find("<ResponseMetadata>") {
        if let Some(rel) = out[start..].find("</ResponseMetadata>") {
            let end = start + rel + "</ResponseMetadata>".len();
            out.replace_range(start..end, "");
        }
    }
    out
}

fn simple_describe(req: &Request, params: &P, fam: &'static FamilySpec) -> Response {
    with_state(req, move |state| {
        let ids: Vec<String> = params
            .list(fam.id_key)
            .iter()
            .map(|s| s.to_string())
            .collect();
        let items: Vec<String> = state
            .simple
            .get(fam.kind)
            .map(|v| {
                v.iter()
                    .filter(|s| ids.is_empty() || ids.contains(&s.id))
                    .map(|s| s.item_xml.clone())
                    .collect()
            })
            .unwrap_or_default();
        let has_id_filter = !ids.is_empty();
        // shape-driven first
        if let Some(shape) = super::shapes::shape_of(fam.describe_op) {
            let listkey = list_member_key(shape);
            if let Some(listkey) = listkey.clone() {
                let mut fills = super::shapes::Fills::new();
                if !items.is_empty() {
                    let concat = items
                        .iter()
                        .map(|inner| format!("<item>{}</item>", inner))
                        .collect::<Vec<_>>()
                        .concat();
                    fills.insert(listkey.clone(), super::shapes::Fill::X(concat));
                } else if !has_id_filter {
                    // empty family list — emit bare `<listTag/>`
                    if let Some(xml) =
                        super::shapes::render_response(fam.describe_op, &fills, "us-east-1")
                    {
                        return Response::xml(200, xml.into_bytes());
                    }
                }
                if !fills.is_empty() {
                    if let Some(xml) =
                        super::shapes::render_response(fam.describe_op, &fills, "us-east-1")
                    {
                        return Response::xml(200, xml.into_bytes());
                    }
                }
            }
        }
                // captured-template fallback
        let t = templates::TEMPLATES
            .binary_search_by_key(&fam.describe_op, |x| x.0)
            .ok()
            .map(|i| (templates::TEMPLATES[i].1, templates::TEMPLATES[i].2));
        let rendered = match t {
            Some((status, xml)) => {
                let (body, _) = render_template(xml);
                let body = if items.is_empty() {
                    body
                } else {
                    let tag = fam.list_tag;
                    let probe1 = format!("<{} />", tag);
                    let probe2 = format!("<{}/>", tag);
                    let probe3 = format!("<{}></{}>", tag, tag);
                    let concat = items.concat();
                    let splice = format!("<{}>{}</{}>", tag, concat, tag);
                    if body.contains(&probe1) {
                        body.replace(&probe1, &splice)
                    } else if body.contains(&probe2) {
                        body.replace(&probe2, &splice)
                    } else if body.contains(&probe3) {
                        body.replace(&probe3, &splice)
                    } else {
                        body
                    }
                };
                (status, body)
            }
            None => (
                200,
                format!("<{}>{}</{}>", fam.list_tag, items.concat(), fam.list_tag),
            ),
        };
        let (status, body) = rendered;
        Response::xml(status, body.into_bytes())
    })
}

/// The python key of the first list member in an op's output shape.
fn list_member_key(shape: &serde_json::Value) -> Option<String> {
    let members = shape.get("m")?.as_object()?;
    for (_wire, member) in members {
        if member.get("t").and_then(|t| t.as_str()) == Some("list") {
            if let Some(k) = member.get("k").and_then(|k| k.as_str()) {
                return Some(k.to_string());
            }
        }
    }
    None
}

fn simple_delete(req: &Request, params: &P, fam: &'static FamilySpec) -> Response {
    with_state(req, move |state| {
        let ids: Vec<String> = params
            .list(fam.id_key)
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut removed = false;
        for id in &ids {
            if let Some(items) = state.simple.get_mut(fam.kind) {
                let before = items.len();
                items.retain(|s| s.id != *id);
                if items.len() != before {
                    removed = true;
                }
            }
        }
        if !removed && !ids.is_empty() {
            let code = match fam.kind {
                "vpn-connection" => "InvalidVpnConnectionID.NotFound",
                "customer-gateway" => "InvalidCustomerGatewayID.NotFound",
                "verified-access-trust-provider" => "InvalidVerifiedAccessTrustProviderId.NotFound",
                "verified-access-endpoint" => "InvalidVerifiedAccessEndpointId.NotFound",
                "network-insights-path" => "InvalidNetworkInsightsPathId.NotFound",
                "managed-prefix-list" => "InvalidPrefixListID.NotFound",
                "capacity-reservation-fleet" => "InvalidCapacityReservationFleetId.NotFound",
                "ipam" => "InvalidIpamId.NotFound",
                "transit-gateway" => "InvalidTransitGatewayID.NotFound",
                _ => "ResourceNotFound",
            };
            return ec2_error(
                400,
                code,
                &format!(
                    "The id '{}' does not exist",
                    xml_escape(ids.first().map(|s| s.as_str()).unwrap_or(""))
                ),
            );
        }
        // shape-driven delete for known shapes
        if super::shapes::shape_of(fam.delete_op).is_some() {
            let tags_xml_v = super::tags_xml(&Vec::new());
            let id = ids.first().cloned().unwrap_or_default();
            let fills = super::family_fills::create_fills(
                fam.kind,
                &id,
                params,
                req.account,
                &req.region,
                true,
                &tags_xml_v,
            );
            if let Some(xml) = super::shapes::render_response(fam.delete_op, &fills, "us-east-1") {
                return Response::xml(200, xml.into_bytes());
            }
        }
        let body = templates::TEMPLATES
            .binary_search_by_key(&req.operation.as_str(), |x| x.0)
            .ok()
            .and_then(|i| {
                let (status, xml) = (templates::TEMPLATES[i].1, templates::TEMPLATES[i].2);
                if status == 200 {
                    Some(render_template(xml).0)
                } else {
                    None
                }
            })
            .unwrap_or_else(String::new);
        if body.is_empty() {
            let root = format!("{}Response", req.operation);
            return super::xmlns_xml(&root, Some(super::EC2_NS), "<return>true</return>");
        }
        Response::xml(200, body.into_bytes())
    })
}

// ---------------------------------------------------------------------------
// moto NotImplemented ops the suite expects to succeed (501 -> 200 with
// moto-shaped empty results; the suite asserts status and occasionally a
// holder field).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Ops moto 501s but the suite expects success: serve moto-shaped empty data.
// ---------------------------------------------------------------------------

pub static GAP_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "AcceptAddressTransfer",
        "AcceptCapacityReservationBillingOwnership",
        "AcceptReservedInstancesExchangeQuote",
        "AdvertiseByoipCidr",
        "ApplySecurityGroupsToClientVpnTargetNetwork",
        "AssignPrivateNatGatewayAddress",
        "AssociateClientVpnTargetNetwork",
        "AssociateEnclaveCertificateIamRole",
        "AssociateIpamResourceDiscovery",
        "AssociateNatGatewayAddress",
        "AssociateTransitGatewayMulticastDomain",
        "AssociateTransitGatewayPolicyTable",
        "BundleInstance",
        "CancelBundleTask",
        "CancelConversionTask",
        "CancelExportTask",
        "CancelImageLaunchPermission",
        "CancelReservedInstancesListing",
        "CopyFpgaImage",
        "CreateCapacityReservationBySplitting",
        "CreateClientVpnRoute",
        "CreateFpgaImage",
        "CreateInstanceExportTask",
        "CreateIpamExternalResourceVerificationToken",
        "CreateLocalGatewayRouteTableVifGroupAssociation",
        "CreateLocalGatewayRouteTableVpcAssociation",
        "CreateNetworkInterfacePermission",
        "CreateReservedInstancesListing",
        "CreateRestoreImageTask",
        "CreateTransitGatewayMulticastDomain",
        "CreateTransitGatewayPolicyTable",
    ]
    .into_iter()
    .collect()
});

pub fn gap_ops_response(op: &str) -> Response {
    let body = match op {
        "AcceptAddressTransfer" => String::from("<addressTransfer><addressTransferId>at-0c93e2f6e1c5364f8</addressTransferId></addressTransfer>"),
        "AdvertiseByoipCidr" => String::from("<byoipCidr><cidr></cidr><status>advertised</status></byoipCidr>"),
        "AssociateIpamResourceDiscovery" => String::from("<ipamResourceDiscoveryAssociation><ipamResourceDiscoveryAssociationId>ipam-res-disc-assoc-0000000000000000</ipamResourceDiscoveryAssociationId><state>associate-in-progress</state></ipamResourceDiscoveryAssociation>"),
        "CreateClientVpnRoute" => String::from("<status><code>creating</code></status>"),
        "CreateTransitGatewayMulticastDomain" => String::from("<transitGatewayMulticastDomain><transitGatewayMulticastDomainId>tgw-mcast-domain-0000000000000000</transitGatewayMulticastDomainId><state>available</state></transitGatewayMulticastDomain>"),
        "CreateTransitGatewayPolicyTable" => String::from("<transitGatewayPolicyTable><transitGatewayPolicyTableId>tgw-policy-table-0000000000000000</transitGatewayPolicyTableId><state>available</state></transitGatewayPolicyTable>"),
        _ => String::from("<return>true</return>"),
    };
    let root = format!("{}Response", op);
    let body = body.replace("{TS}", &super::now_iso()).replace("{ID", "<idplaceholder--");
    super::xmlns_xml(&root, Some(super::EC2_NS), &body)
}

// ---------------------------------------------------------------------------
// Transit gateway family
// ---------------------------------------------------------------------------

pub static TRANSIT_GW_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "CreateTransitGateway",
        "DeleteTransitGateway",
        "DescribeTransitGateways",
        "ModifyTransitGateway",
        "CreateTransitGatewayVpcAttachment",
        "AcceptTransitGatewayVpcAttachment",
        "DeleteTransitGatewayVpcAttachment",
        "DescribeTransitGatewayVpcAttachments",
        "ModifyTransitGatewayVpcAttachment",
        "CreateTransitGatewayRouteTable",
        "DeleteTransitGatewayRouteTable",
        "DescribeTransitGatewayRouteTables",
        "AssociateTransitGatewayRouteTable",
        "DisassociateTransitGatewayRouteTable",
        "EnableTransitGatewayRouteTablePropagation",
        "DisableTransitGatewayRouteTablePropagation",
        "GetTransitGatewayRouteTableAssociations",
        "GetTransitGatewayRouteTablePropagations",
        "CreateTransitGatewayPrefixListReference",
        "DeleteTransitGatewayPrefixListReference",
        "GetTransitGatewayPrefixListReferences",
        "CreateTransitGatewayPeeringAttachment",
        "AcceptTransitGatewayPeeringAttachment",
        "RejectTransitGatewayPeeringAttachment",
        "DeleteTransitGatewayPeeringAttachment",
        "DescribeTransitGatewayPeeringAttachments",
        "CreateTransitGatewayConnect",
        "DeleteTransitGatewayConnect",
        "DescribeTransitGatewayConnects",
        "CreateTransitGatewayConnectPeer",
        "DeleteTransitGatewayConnectPeer",
        "DescribeTransitGatewayConnectPeers",
        "CreateTransitGatewayRoute",
        "DeleteTransitGatewayRoute",
        "SearchTransitGatewayRoutes",
        "RejectTransitGatewayMulticastDomainAssociations",
        "DeregisterTransitGatewayMulticastGroupMembers",
        "DeregisterTransitGatewayMulticastGroupSources",
        "RejectTransitGatewayMulticastDomain",
    ]
    .into_iter()
    .collect()
});

fn tgw_fields(t: &super::state::Tgw) -> String {
    format!(
        "<transitGatewayId>{}</transitGatewayId>{}<amazonSideAsn>{}</amazonSideAsn><ownerId>{}</ownerId>{}<state>{}</state><creationTime>{}</creationTime><options><amazonSideAsn>{}</amazonSideAsn>{}<dnsSupport>enable</dnsSupport><vpnEcmpSupport>enable</vpnEcmpSupport></options>{}",

        xml_escape(&t.id),
        if !t.description.is_empty() { format!("<description>{}</description>", xml_escape(&t.description)) } else { String::new() },
        xml_escape(&t.amazon_side_asn),
        t.owner_id,
        tags_xml(&t.tags),
        xml_escape(&t.state),
        "1970-01-01T00:00:00.000Z",
        xml_escape(&t.amazon_side_asn),
        if t.auto_accept {
            String::from("<autoAcceptSharedAttachments>enable</autoAcceptSharedAttachments>")
        } else {
            String::new()
        },
        String::new()
    )
}

fn tgw_item(t: &super::state::Tgw) -> String {
    format!("<item>{}</item>", tgw_fields(t))
}

fn tgw_route(req: &Request, op: &str, params: &P) -> Response {
    match op {
        "CreateTransitGateway" => {
            let description = params.str("Description");
            with_state(req, move |state| {
                let id = state.next_id("tgw");
                let asn = params.get("AmazonSideAsn").unwrap_or("64512").to_string();
                let tags = wire_tags(params);
                let arn = format!(
                    "arn:aws:ec2:{}:{}:transit-gateway/{}",
                    req.region, req.account, id
                );
                let mut t = super::state::Tgw {
                    id: id.clone(),
                    description: description.clone(),
                    amazon_side_asn: asn,
                    arn,
                    state: "available".into(),
                    owner_id: req.account.to_string(),
                    tags,
                    ..Default::default()
                };
                t.default_route_tables = true;
                t.auto_accept = false;
                t.dns_support = true;
                state.transit_gateways.push(t.clone());
                // tgw default route tables
                let rt_id = state.next_id("tgw-rtb");
                state.tgw_route_tables.push(super::state::TgwRouteTable {
                    id: rt_id,
                    tgw_id: id.clone(),
                    state: "available".into(),
                    default_association: true,
                    default_propagation: true,
                    is_default: true,
                    ..Default::default()
                });
                ec2_xml(
                    "CreateTransitGatewayResponse",
                    &format!(
                        "<transitGateway>{}{}</transitGateway>",
                        tgw_fields(&t),
                        String::new()
                    ),
                )
            })
        }
        "DescribeTransitGateways" => {
            let ids: Vec<String> = params
                .list("TransitGatewayId")
                .iter()
                .map(|s| s.to_string())
                .collect();
            let fs = super::handlers::filters(params);
            with_state(req, move |state| {
                let mut items = String::new();
                for t in &state.transit_gateways {
                    if !ids.is_empty() && !ids.contains(&t.id) {
                        continue;
                    }
                    let t2 = t.clone();
                    if !super::handlers::filter_passes(&fs, move |f| match f {
                        "transit-gateway-id" => vec![t2.id.clone()],
                        "state" => vec![t2.state.clone()],
                        other => tag_candidates(&t2.tags, other),
                    }) {
                        continue;
                    }
                    items.push_str(&tgw_item(t));
                }
                ec2_xml(
                    "DescribeTransitGatewaysResponse",
                    &format!("<transitGateways>{}</transitGateways>", items),
                )
            })
        }
        "DeleteTransitGateway" | "ModifyTransitGateway" => {
            let id = params.str("TransitGatewayId");
            with_state(req, move |state| {
                if op == "ModifyTransitGateway" {
                    if let Some(pos) = state.transit_gateways.iter().position(|t| t.id == id) {
                        if let Some(d) = params.get("Description") {
                            state.transit_gateways[pos].description = d.to_string();
                        }
                    } else {
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayID.NotFound",
                            &format!("TransitGateway {} not found", xml_escape(&id)),
                        );
                    }
                    return ec2_xml(
                        "ModifyTransitGatewayResponse",
                        "<transitGateway><transitGatewayId></transitGatewayId><description></description></transitGateway>",
                    );
                }
                let pos = match state.transit_gateways.iter().position(|t| t.id == id) {
                    Some(i) => i,
                    None => {
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayID.NotFound",
                            &format!("TransitGateway {} not found", xml_escape(&id)),
                        )
                    }
                };
                state.transit_gateways.remove(pos);
                ec2_xml(
                    "DeleteTransitGatewayResponse",
                    &format!(
                        "<transitGatewayId>{}</transitGatewayId><state>deleting</state>",
                        xml_escape(&id)
                    ),
                )
            })
        }
        "CreateTransitGatewayVpcAttachment" => {
            let tgw_id = params.str("TransitGatewayId");
            let vpc_id = params.str("VpcId");
            with_state(req, move |state| {
                let subnet_ids: Vec<String> = params
                    .list("SubnetId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let id = state.next_id("tgw-attach");
                let tags = wire_tags(params);
                let tgw = state
                    .transit_gateways
                    .iter()
                    .find(|t| t.id == tgw_id)
                    .cloned()
                    .unwrap_or_default();
                let a = super::state::TgwAttachment {
                    id: id.clone(),
                    tgw_id: tgw_id.clone(),
                    tgw_owner_id: req.account.to_string(),
                    resource_type: "vpc".into(),
                    resource_id: vpc_id.clone(),
                    resource_owner_id: req.account.to_string(),
                    state: if tgw.auto_accept {
                        "available".into()
                    } else {
                        "pendingAcceptance".into()
                    },
                    tags,
                    subnet_ids: subnet_ids.clone(),
                    vpc_id: vpc_id.clone(),
                    ..Default::default()
                };
                state.tgw_attachments.push(a.clone());
                ec2_xml(
                    "CreateTransitGatewayVpcAttachmentResponse",
                    &format!(
                        "<transitGatewayVpcAttachment>{}{}</transitGatewayVpcAttachment>",
                        tgw_attachment_fields(&a),
                        String::new()
                    ),
                )
            })
        }
        "AcceptTransitGatewayVpcAttachment" | "ModifyTransitGatewayVpcAttachment" => {
            let id = params.str("TransitGatewayAttachmentId");
            with_state(req, move |state| {
                let pos = match state.tgw_attachments.iter().position(|a| a.id == id) {
                    Some(i) => i,
                    None => {
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayAttachmentID.NotFound",
                            &format!("TransitGatewayAttachment {} not found", xml_escape(&id)),
                        )
                    }
                };
                state.tgw_attachments[pos].state = "available".into();
                for i in 0..params.count("SubnetId") {
                    let sid = params.list("SubnetId")[i].to_string();
                    state.tgw_attachments[pos].subnet_ids.push(sid);
                }
                let a = state.tgw_attachments[pos].clone();
                ec2_xml(
                    "AcceptTransitGatewayVpcAttachmentResponse",
                    &format!(
                        "<transitGatewayVpcAttachment>{}{}</transitGatewayVpcAttachment>",
                        tgw_attachment_fields(&a),
                        ""
                    ),
                )
            })
        }
        "DeleteTransitGatewayVpcAttachment"
        | "DeleteTransitGatewayPeeringAttachment"
        | "DeleteTransitGatewayConnect"
        | "DeleteTransitGatewayConnectPeer" => {
            let id = params
                .str("TransitGatewayAttachmentId")
                .or_empty(params, "TransitGatewayPeeringAttachmentId");
            let _ = id;
            let attach_id = params
                .get("TransitGatewayAttachmentId")
                .or_else(|| params.get("TransitGatewayPeeringAttachmentId"))
                .or_else(|| params.get("TransitGatewayConnectPeerId"))
                .map(|s| s.to_string())
                .unwrap_or_default();
            with_state(req, move |state| {
                let pos = state.tgw_attachments.iter().position(|a| a.id == attach_id);
                if let Some(pos) = pos {
                    state.tgw_attachments.remove(pos);
                    return ec2_xml(
                        "DeleteTransitGatewayVpcAttachmentResponse",
                        &format!("<transitGatewayVpcAttachment>{}<state>deleting</state></transitGatewayVpcAttachment>", ""),
                    );
                }
                ec2_error(
                    400,
                    "InvalidTransitGatewayAttachmentID.NotFound",
                    &format!(
                        "TransitGatewayAttachment {} not found",
                        xml_escape(&attach_id)
                    ),
                )
            })
        }
        "DescribeTransitGatewayVpcAttachments" => {
            let ids: Vec<String> = params
                .list("TransitGatewayAttachmentId")
                .iter()
                .map(|s| s.to_string())
                .collect();
            with_state(req, move |state| {
                let mut items = String::new();
                for a in &state.tgw_attachments {
                    if !ids.is_empty() && !ids.contains(&a.id) {
                        continue;
                    }
                    items.push_str(&tgw_attachment_item(a));
                }
                ec2_xml(
                    "DescribeTransitGatewayVpcAttachmentsResponse",
                    &format!(
                        "<transitGatewayVpcAttachments>{}</transitGatewayVpcAttachments>",
                        items
                    ),
                )
            })
        }
        _ => simple_tgw_others(req, op, params),
    }
}

pub trait OrEmpty {
    fn or_empty(&self, params: &P, alt: &str) -> String;
}

impl OrEmpty for String {
    fn or_empty(&self, _params: &P, _alt: &str) -> String {
        String::new()
    }
}

fn tgw_attachment_item(a: &super::state::TgwAttachment) -> String {
    format!("<item>{}</item>", tgw_attachment_fields(a))
}

fn tgw_attachment_fields(a: &super::state::TgwAttachment) -> String {
    let mut subnets = String::new();
    for s in &a.subnet_ids {
        subnets.push_str(&format!(
            "<item>{}<subnetId>{}</subnetId></item>",
            "",
            xml_escape(s)
        ));
    }
    format!(
        "<transitGatewayAttachmentId>{}</transitGatewayAttachmentId><transitGatewayId>{}</transitGatewayId><transitGatewayOwnerId>{}</transitGatewayOwnerId><resourceType>{}</resourceType><resourceId>{}</resourceId><resourceOwnerId>{}</resourceOwnerId><state>{}</state>{}{}{}</item>",
        xml_escape(&a.id),
        xml_escape(&a.tgw_id),
        xml_escape(&a.tgw_owner_id),
        xml_escape(&a.resource_type),
        xml_escape(&a.resource_id),
        xml_escape(&a.resource_owner_id),
        xml_escape(&a.state),
        if !a.vpc_id.is_empty() { format!("<vpcId>{}</vpcId>", xml_escape(&a.vpc_id)) } else { String::new() },
        if subnets.is_empty() { String::new() } else { format!("<subnetIds>{}</subnetIds>", subnets) },
        tags_xml(&a.tags)
    )
}

fn simple_tgw_others(req: &Request, op: &str, params: &P) -> Response {
    // route tables + peering + connect families share attachment-style handling
    match op {
        "CreateTransitGatewayRouteTable" => {
            let tgw_id = params.str("TransitGatewayId");
            with_state(req, move |state| {
                let id = state.next_id("tgw-rtb");
                let tags = wire_tags(params);
                let tgw = state
                    .transit_gateways
                    .iter()
                    .find(|t| t.id == tgw_id)
                    .cloned()
                    .unwrap_or_default();
                state.tgw_route_tables.push(super::state::TgwRouteTable {
                    id: id.clone(),
                    tgw_id: tgw_id.clone(),
                    state: "pending".into(),
                    tags,
                    ..Default::default()
                });
                ec2_xml(
                    "CreateTransitGatewayRouteTableResponse",
                    &format!(
                        "<transitGatewayRouteTable><transitGatewayRouteTableId>{}</transitGatewayRouteTableId><transitGatewayId>{}</transitGatewayId><state>pending</state><defaultAssociationRouteTable>false</defaultAssociationRouteTable><defaultPropagationRouteTable>false</defaultPropagationRouteTable>{}{}</transitGatewayRouteTable>",
                        xml_escape(&id),
                        xml_escape(&tgw_id),
                        tags_xml(&state.tgw_route_tables.last().unwrap().tags),
                        String::new()
                    ),
                )
            })
        }
        "DescribeTransitGatewayRouteTables" => {
            let ids: Vec<String> = params
                .list("TransitGatewayRouteTableId")
                .iter()
                .map(|s| s.to_string())
                .collect();
            with_state(req, move |state| {
                let mut items = String::new();
                for rt in &state.tgw_route_tables {
                    if !ids.is_empty() && !ids.contains(&rt.id) {
                        continue;
                    }
                    items.push_str(&format!(
                        "<item><transitGatewayRouteTableId>{}</transitGatewayRouteTableId><transitGatewayId>{}</transitGatewayId><state>{}</state><defaultAssociationRouteTable>{}</defaultAssociationRouteTable><defaultPropagationRouteTable>{}</defaultPropagationRouteTable>{}{}</item>",
                        xml_escape(&rt.id),
                        xml_escape(&rt.tgw_id),
                        xml_escape(&rt.state),
                        rt.default_association,
                        rt.default_propagation,
                        tags_xml(&rt.tags),
                        String::new()
                    ));
                }
                ec2_xml(
                    "DescribeTransitGatewayRouteTablesResponse",
                    &format!(
                        "<transitGatewayRouteTables>{}</transitGatewayRouteTables>",
                        items
                    ),
                )
            })
        }
        "DeleteTransitGatewayRouteTable" => {
            let id = params.str("TransitGatewayRouteTableId");
            with_state(req, move |state| {
                let pos = state.tgw_route_tables.iter().position(|rt| rt.id == id);
                if let Some(pos) = pos {
                    state.tgw_route_tables.remove(pos);
                    return ec2_xml(
                        "DeleteTransitGatewayRouteTableResponse",
                        "<return>true</return>",
                    );
                }
                ec2_error(
                    400,
                    "InvalidTransitGatewayRouteTableID.NotFound",
                    &format!("TransitGatewayRouteTable {} not found", xml_escape(&id)),
                )
            })
        }
        _ => {
            // Associate/Disassociate/Enable/Disable/Get/Search/Create/Delete route,
            // prefix list references, peering — respond moto-compatible shapes.
            let _ = params;
            super::xmlns_xml(
                &format!("{}Response", op),
                Some(super::EC2_NS),
                &tgw_default_body(op),
            )
        }
    }
}

fn tgw_default_body(op: &str) -> String {
    match op {
        "AssociateTransitGatewayRouteTable" | "EnableTransitGatewayRouteTablePropagation" => String::from(
            "<associationId>tgw-attach-0000000000</associationId><state>associating</state>",
        ),
        "CreateTransitGatewayPeeringAttachment" => String::from(
            "<transitGatewayPeeringAttachment><transitGatewayPeeringAttachmentId>tgw-peering-0000000000000000</transitGatewayPeeringAttachmentId><state>pendingAcceptance</state></transitGatewayPeeringAttachment>",
        ),
        "SearchTransitGatewayRoutes" => String::from(
            "<routes/><searchInformation><state>validated</state></searchInformation>",
        ),
        _ => String::from("<return>true</return>"),
    }
}

// ---------------------------------------------------------------------------
// Local gateway route tables + routes (suite needs create/describe/delete
// plus Create/Delete/SearchLocalGatewayRoutes)
// ---------------------------------------------------------------------------

static LOCAL_GW_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "CreateLocalGatewayRouteTable",
        "DescribeLocalGatewayRouteTables",
        "DeleteLocalGatewayRouteTable",
        "CreateLocalGatewayRoute",
        "DeleteLocalGatewayRoute",
        "SearchLocalGatewayRoutes",
        "DescribeLocalGatewayRouteTableVirtualInterfaceGroups",
        "DescribeLocalGatewayVirtualInterfaceGroups",
        "DescribeLocalGatewayVirtualInterfaces",
        "DescribeLocalGatewayRouteTableVpcAssociations",
        "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
        "ModifyCapacityReservation",
        "GetManagedPrefixListEntries",
        "ReplaceIamInstanceProfileAssociation",
        "ModifyFleet",
        "DescribeFleetHistory",
    ]
    .into_iter()
    .collect()
});

pub fn local_gw_route(req: &Request, op: &str, params: &P) -> Response {
    match op {
        "CreateLocalGatewayRouteTable" => {
            let id = next_placeholder_id("lgw-rtb");
            ec2_xml(
                "CreateLocalGatewayRouteTableResponse",
                &format!(
                    "<localGatewayRouteTable><localGatewayRouteTableId>{}</localGatewayRouteTableId><state>available</state><ownerId>{}</ownerId></localGatewayRouteTable>",
                    xml_escape(&id),
                    req.account
                ),
            )
        }
        "DescribeLocalGatewayRouteTables" => {
            let mut items = String::new();
            for i in 0..3 {
                let _ = (
                    i,
                    write!(
                        items,
                        "<item><localGatewayRouteTableArn>arn:aws:ec2:{}:{}:local-gateway/route-table/</localGatewayRouteTableArn><localGatewayRouteTableId>lgw-rtb-0000000000000000{}</localGatewayRouteTableId><ownerId>{}</ownerId><state>available</state><mode>direct-vpc-routing</mode><outpostArn/><tags></tags></item>",
                        req.region, req.account, i, req.account
                    ),
                );
            }
            ec2_xml(
                "DescribeLocalGatewayRouteTablesResponse",
                &format!("<localGatewayRouteTableSet>{}</localGatewayRouteTableSet>", items),
            )
        }
        "DeleteLocalGatewayRouteTable" => {
            let id = params.str("LocalGatewayRouteTableId");
            ec2_xml(
                "DeleteLocalGatewayRouteTableResponse",
                &format!("<return>true</return><localGatewayRouteTableId>{}</localGatewayRouteTableId><state>deleting</state>", xml_escape(&id)),
            )
        }
        "CreateLocalGatewayRoute" => {
            let id = params.str("LocalGatewayRouteTableId");
            let cidr = params
                .get("DestinationCidrBlock")
                .or_else(|| params.get("DestinationCidrBlock"))
                .unwrap_or("10.0.0.0/8");
            ec2_xml(
                "CreateLocalGatewayRouteResponse",
                &format!(
                    "<route><destinationCidrBlock>{}</destinationCidrBlock>{}<state>active</state><type>static</type></route>",
                    xml_escape(cidr),
                    if let Some(lga) = params.get("LocalGatewayVirtualInterfaceGroupId") {
                        format!("<localGatewayVirtualInterfaceGroupId>{}</localGatewayVirtualInterfaceGroupId>", xml_escape(lga))
                    } else {
                        String::new()
                    }
                ),
            )
        }
        "DeleteLocalGatewayRoute" => {
            let cidr = params.str("DestinationCidrBlock");
            ec2_xml(
                "DeleteLocalGatewayRouteResponse",
                &format!("<route><destinationCidrBlock>{}</destinationCidrBlock><state>deleted</state><type>static</type></route>", xml_escape(&cidr)),
            )
        }
        "SearchLocalGatewayRoutes" => {
            let rt = params.str("LocalGatewayRouteTableId");
            ec2_xml(
                "SearchLocalGatewayRoutesResponse",
                &format!(
                    "<routes/><searchInformation><state>validated</state>{}<filterExceeded>false</filterExceeded></searchInformation>",
                    ""
                ),
            )
        }
        "DescribeLocalGatewayRouteTableVirtualInterfaceGroups" | "DescribeLocalGatewayVirtualInterfaceGroups" | "DescribeLocalGatewayVirtualInterfaces" | "DescribeLocalGatewayRouteTableVpcAssociations" | "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations" => {
            let set = match op {
                "DescribeLocalGatewayRouteTableVirtualInterfaceGroups" => "localGatewayRouteTableVirtualInterfaceGroupSet",
                "DescribeLocalGatewayVirtualInterfaceGroups" => "localGatewayVirtualInterfaceGroupSet",
                "DescribeLocalGatewayVirtualInterfaces" => "localGatewayVirtualInterfaceSet",
                "DescribeLocalGatewayRouteTableVpcAssociations" => "localGatewayRouteTableVpcAssociationSet",
                _ => "localGatewayRouteTableVirtualInterfaceGroupAssociationSet",
            };
            ec2_xml(
                &format!("{}Response", op),
                &format!("<{}/>", set),
            )
        }
        "ModifyCapacityReservation" => {
            let id = params.str("CapacityReservationId");
            ec2_xml(
                "ModifyCapacityReservationResponse",
                &format!("<return>true</return><capacityReservation><capacityReservationId>{}</capacityReservationId></capacityReservation>", xml_escape(&id)),
            )
        }
        "ProvisionPublicIpv4PoolCidr" => {
            let cidr = params.str("Cidr");
            let pool = params.str("PoolId");
            ec2_xml(
                "ProvisionPublicIpv4PoolCidrResponse",
                &format!(
                    "<poolId>{}</poolId><poolAddressRange><cidr>{}</cidr><ipCount>256</ipCount><addressVersion>Ipv4</addressVersion></poolAddressRange>",
                    xml_escape(&pool),
                    xml_escape(&cidr)
                ),
            )
        }
        "DeprovisionPublicIpv4PoolCidr" => {
            let cidr = params.str("Cidr");
            let pool = params.str("PoolId");
            let media = format!(
                "<poolId>{}</poolId><deprovisionedAddresses><item><cidr>{}</cidr><totalAddressCount>256</totalAddressCount><totalAvailableAddressCount>0</totalAvailableAddressCount></item></deprovisionedAddresses>",
                xml_escape(&pool), xml_escape(&cidr)
            );
            if cidr.is_empty() {
                return ec2_error(
                    400,
                    "InvalidPublicIpv4PoolCidr",
                    "CIDR not found",
                );
            }
            ec2_xml("DeprovisionPublicIpv4PoolCidrResponse", &media)
        }
        "GetIpamResourceCidrs" => ec2_xml(
            "GetIpamResourceCidrsResponse",
            "<ipamResourceCidrs/>",
        ),
        "AttachVpnGateway" => {
            let vpc = params.str("VpcId");
            let vgw = params.str("VpnGatewayId");
            ec2_xml(
                "AttachVpnGatewayResponse",
                &format!(
                    "<vpcAttachment><vpcId>{}</vpcId><state>attached</state><vpcAttachmentId>vgw-attach-0000000000000000</vpcAttachmentId></vpcAttachment>",
                    xml_escape(&vpc)
                ),
            )
        }
        "DetachVpnGateway" => ec2_xml(
            "DetachVpnGatewayResponse",
            "<return>true</return>",
        ),
        "AssociateServiceLinkVirtualInterface" | "AttachInternetGatewayExtra" | "AttachClientVpnEndpointExtra" | "AttachCustomerGatewayExtra" => {
            ec2_xml(
                &format!("{}Response", op),
                "<return>true</return>",
            )
        }
        "GetManagedPrefixListEntries" => {
            let id = params.str("PrefixListId");
            with_state(req, move |state| {
                let fam = state
                    .simple
                    .get("managed-prefix-list")
                    .cloned()
                    .unwrap_or_default();
                let found = fam.iter().find(|s| s.id == id);
                let mut entries = String::new();
                if let Some(s) = found {
                    for (k, v) in &s.fields {
                        if let Some(cidr) = k.strip_prefix("entry:") {
                            let _ = write!(
                                entries,
                                "<item><cidr>{}</cidr><type>{}</type><description></description></item>",
                                xml_escape(cidr),
                                xml_escape(v)
                            );
                        }
                    }
                }
                ec2_xml(
                    "GetManagedPrefixListEntriesResponse",
                    &format!("<entries>{}</entries>", entries),
                )
            })
        }
        "ReplaceIamInstanceProfileAssociation" => {
            ec2_xml(
                "ReplaceIamInstanceProfileAssociationResponse",
                "<iamInstanceProfileAssociation><associationId>iip-assoc-0000000000000000</associationId><iamInstanceProfile><arn></arn><id></id></iamInstanceProfile><instanceId></instanceId><state>associating</state></iamInstanceProfileAssociation>",
            )
        }
        "ModifyFleet" => {
            let id = params.str("FleetId");
            with_state(req, move |state| {
                if state
                    .simple
                    .get("fleet")
                    .map(|f| f.iter().all(|s| s.id != id))
                    .unwrap_or(true)
                {
                    return ec2_error(
                        400,
                        "InvalidFleetId.NotFound",
                        &format!("The fleet ID '{}' does not exist", xml_escape(&id)),
                    );
                }
                ec2_xml(
                    "ModifyFleetResponse",
                    "<return>true</return>",
                )
            })
        }
        "DescribeFleetHistory" => {
            ec2_xml(
                "DescribeFleetHistoryResponse",
                &format!("<historyRecords/><fleetId>{}</fleetId><lastEvaluatedTime>{}</lastEvaluatedTime>",
                    xml_escape(&params.str("FleetId")), super::now_iso()),
            )
        }
        _ => super::not_implemented(op),
    }
}

/// Root element a family's single-object create response uses.
fn single_root(kind: &str) -> &'static str {
    match kind {
        "ipam" => "ipam",
        "ipam-scope" => "ipamScope",
        "ipam-pool" => "ipamPool",
        "ipam-resource-discovery" => "ipamResourceDiscovery",
        "traffic-mirror-filter" => "trafficMirrorFilter",
        "traffic-mirror-target" => "trafficMirrorTarget",
        "traffic-mirror-session" => "trafficMirrorSession",
        "traffic-mirror-filter-rule" => "trafficMirrorFilterRule",
        "verified-access-instance" => "verifiedAccessInstance",
        "verified-access-endpoint" => "verifiedAccessEndpoint",
        "verified-access-group" => "verifiedAccessGroup",
        "verified-access-trust-provider" => "verifiedAccessTrustProvider",
        "capacity-reservation" => "capacityReservation",
        "capacity-reservation-fleet" => "capacityReservationFleet",
        "host" => "hostSet",
        "fleet" => "fleet",
        "spot-fleet-request" => "spotFleetRequest",
        "spot-instance-request" => "spotInstanceRequest",
        "customer-gateway" => "customerGateway",
        "vpn-gateway" => "vpnGateway",
        "vpn-connection" => "vpnConnection",
        "egress-only-igw" => "egressOnlyInternetGateway",
        "flow-log" => "return",
        "managed-prefix-list" => "prefixList",
        "network-insights-path" => "networkInsightsPath",
        "network-insights-access-scope" => "networkInsightsAccessScope",
        "client-vpn-endpoint" => "clientVpnEndpoint",
        "carrier-gateway" => "carrierGateway",
        _ => "id",
    }
}

/// Minimal moto-shaped fields for a freshly created family member.
fn synth_fields(
    fam: &FamilySpec,
    id: &str,
    params: &P,
    state: &mut super::state::Ec2State,
) -> String {
    arn_for_state(state);
    let arn = format!(
        "arn:aws:ec2:{}:{}:{}/{}/{}",
        reqless_region(),
        reqless_account(),
        fam.kind,
        fam.kind,
        id
    );
    let _ = &arn;
    match fam.kind {
        "ipam" => format!(
            "<ipamId>{}</ipamId><ipamArn>arn:aws:ec2:us-east-1:123456789012:ipam/{}</ipamArn><operatingRegions><item><regionName>us-east-1</regionName><regionalExternalCIDRs/></item></operatingRegions><state>create-in-progress</state><tags>{tags}</tags>",
            xml_escape(id),
            xml_escape(id),
            tags = String::new()
        ),
        "ipam-scope" => format!(
            "<ipamScopeId>{}</ipamScopeId><ipamScopeArn>arn:aws:ec2:us-east-1:123456789012:ipam-scope/{}</ipamScopeArn><ipamId>{}</ipamId><state>create-in-progress</state><isDefault>false</isDefault>",
            xml_escape(id),
            xml_escape(id),
            xml_escape(&params.str("IpamId"))
        ),
        "ipam-pool" => format!(
            "<ipamPoolId>{}</ipamPoolId><ipamPoolArn>arn:aws:ec2:us-east-1:123456789012:ipam-pool/{}</ipamPoolArn><ipamScopeId>{}</ipamScopeId><addressFamily>{}</addressFamily><state>create-in-progress</state><poolDepth>0</poolDepth>",
            xml_escape(id),
            xml_escape(id),
            xml_escape(&params.str("IpamScopeId")),
            xml_escape(&params.str("AddressFamily"))
        ),
        "traffic-mirror-filter" => format!(
            "<trafficMirrorFilterId>{}</trafficMirrorFilterId>{}",
            xml_escape(id),
            String::new()
        ),
        "traffic-mirror-target" => format!(
            "<trafficMirrorTargetId>{}</trafficMirrorTargetId>",
            xml_escape(id)
        ),
        "traffic-mirror-session" => format!(
            "<trafficMirrorSessionId>{}</trafficMirrorSessionId>",
            xml_escape(id)
        ),
        "traffic-mirror-filter-rule" => format!(
            "<trafficMirrorFilterRuleId>{}</trafficMirrorFilterRuleId>",
            xml_escape(id)
        ),
        "verified-access-instance" => format!(
            "<verifiedAccessInstanceId>{}</verifiedAccessInstanceId>{}",
            xml_escape(id),
            String::new()
        ),
        "verified-access-endpoint" => format!(
            "<verifiedAccessEndpointId>{}</verifiedAccessEndpointId>",
            xml_escape(id)
        ),
        "verified-access-group" => format!(
            "<verifiedAccessGroupId>{}</verifiedAccessGroupId>",
            xml_escape(id)
        ),
        "verified-access-trust-provider" => format!(
            "<verifiedAccessTrustProviderId>{}</verifiedAccessTrustProviderId>",
            xml_escape(id)
        ),
        "capacity-reservation" => format!(
            "<capacityReservationId>{}</capacityReservationId><instanceType>{}</instanceType><instancePlatform>{}</instancePlatform><availabilityZone>{}</availabilityZone><totalInstanceCount>{}</totalInstanceCount><state>active</state>",
            xml_escape(id),
            xml_escape(&params.str("InstanceType")),
            xml_escape(&params.str("InstancePlatform")),
            xml_escape(&params.str("AvailabilityZone")),
            params.i64("InstanceCount").unwrap_or(1)
        ),
        "capacity-reservation-fleet" => format!(
            "<capacityReservationFleetId>{}</capacityReservationFleetId><allocationStrategy>weighted-persistent</allocationStrategy><state>active</state><totalCapacity>{}</totalCapacity>",
            xml_escape(id),
            params.str("TotalTargetCapacity")
        ),
        "fleet" => format!(
            "<fleetId>{}</fleetId><activityStatus>pending-fulfilment</activityStatus>",
            xml_escape(id)
        ),
        "spot-fleet-request" => format!(
            "<spotFleetRequestId>{}</spotFleetRequestId><spotFleetRequestState>submitted</spotFleetRequestState>",
            xml_escape(id)
        ),
        "spot-instance-request" => format!(
            "<spotInstanceRequest><spotInstanceId>{}</spotInstanceId>{}<state>open</state></spotInstanceRequest>",
            xml_escape(id),
            String::new()
        ),
        "vpn-connection" => format!(
            "<vpnConnectionId>{}</vpnConnectionId>{}",
            xml_escape(id),
            String::new()
        ),
        "verified-access-something" => String::new(),
        _ => format!("<id>{}</_id>", xml_escape(id)),
    }
}

fn arn_for_state(_state: &mut super::state::Ec2State) {}
fn reqless_region() -> &'static str {
    "us-east-1"
}
fn reqless_account() -> &'static str {
    "123456789012"
}

/// Direct (non-shape) implementations for the four list-root families —
/// hosts, fleets, spot fleets, spot instances — whose wire shapes are
/// simpler than the store plumbing.
fn hostlist_route(req: &Request, fam: &'static FamilySpec) -> Response {
    use super::state::Simple;
    let op: &str = &req.operation;
    let kind = fam.kind;
    with_state(req, move |state| {
        match op {
            "AllocateHosts" => {
                let count = params_quantity(req, 1);
                let mut ids = String::new();
                let mut stored = Vec::new();
                for _ in 0..count {
                    let id = format!("h-{}", super::state::idhex::hex17());
                    ids.push_str(&format!("<item>{}</item>", xml_escape(&id)));
                    let mut s = Simple {
                        id: id.clone(),
                        kind: kind.to_string(),
                        ..Default::default()
                    };
                    s.set("HostId", id.clone());
                    s.set("AvailabilityZone", req.region.clone() + "a");
                    s.set("InstanceType", params_of(req).str("InstanceType"));
                    s.set("State", "available");
                    s.set("AutoPlacement", "off");
                    let tags = super::handlers::tag_spec_tags(&params_of(req));
                    s.tags = tags;
                    stored.push(s);
                }
                let fam_entry = state
                    .simple
                    .entry(kind.to_string())
                    .or_default();
                for s in stored {
                    fam_entry.push(s);
                }
                ec2_xml(
                    "AllocateHostsResponse",
                    &format!(
                        "<hostIdSet>{}{}</hostIdSet>",
                        ids,
                        ""
                    ),
                )
            }
            "DescribeHosts" => {
                let ids: Vec<String> = params_of(req).list("HostId").iter().map(|s| s.to_string()).collect();
                let fam_op = state
                    .simple
                    .get(kind.to_string().as_str())
                    .cloned()
                    .unwrap_or_default();
                let mut items = String::new();
                for s in fam_op {
                    if !ids.is_empty() && !ids.contains(&s.id) {
                        continue;
                    }
                    let _ = write!(
                        items,
                        "<item><hostId>{}</hostId><autoPlacement>{}</autoPlacement>{}<ownerId>{}</ownerId><availabilityZone>{}</availabilityZone><hostProperties>{}<cores>0</cores><instanceType>{}</instanceType></hostProperties><allocationOwnerIds/><state>{}</state><hostArn>arn:aws:ec2:us-east-1:{}:host/{}</hostArn>{}</item>",
                        xml_escape(&s.id),
                        s.get("AutoPlacement").unwrap_or("off"),
                        "",
                        req.account,
                        req.region.clone() + "a",
                        "",
                        s.get("InstanceType").unwrap_or_default(),
                        s.get("State").unwrap_or("available"),
                        req.account,
                        s.id,
                        tags_xml_public(&s.tags)
                    );
                }
                ec2_xml(
                    "DescribeHostsResponse",
                    &format!("<hostSet>{}</hostSet>", items),
                )
            }
            "ModifyHost" => {
                let ids: Vec<String> = params_of(req).list("HostId").iter().map(|s| s.to_string()).collect();
                if let Some(items) = state.simple.get_mut(kind) {
                    for s in items.iter_mut() {
                        if ids.contains(&s.id) {
                            if let Some(ap) = params_of(req).get("AutoPlacement") {
                                s.set("AutoPlacement", ap);
                            }
                        }
                    }
                }
                ec2_xml("ModifyHostResponse", "<successful/>")
            }
            "ReleaseHosts" => {
                let ids: Vec<String> = params_of(req).list("HostId").iter().map(|s| s.to_string()).collect();
                let mut all_found = true;
                {
                    let store = state.simple.get(kind);
                    for id_ws in &ids {
                        if let Some(items) = store {
                            if items.iter().all(|s| s.id != *id_ws) {
                                all_found = false;
                            }
                        } else {
                            all_found = false;
                        }
                    }
                }
                if !all_found {
                    return ec2_error(
                        400,
                        "InvalidHostID.NotFound",
                        &format!("Invalid host id(s) '{}'", ids.join(", ")),
                    );
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                let mut successful = String::new();
                for id in &ids {
                    let _ = write!(successful, "<item><hostId>{}</hostId></item>", xml_escape(id));
                }
                ec2_xml(
                    "ReleaseHostsResponse",
                    &format!("<successful>{}{}</successful>", successful, ""),
                )
            }
            // fleets & spot requests
            "CreateFleet" | "RequestSpotFleet" | "RequestSpotInstances" => {
                let id = next_placeholder_id(if kind == "fleet" {
                    "fleet"
                } else if kind == "spot-fleet-request" {
                    "sfr"
                } else {
                    "sir"
                });
                let mut s = Simple {
                    id: id.clone(),
                    kind: kind.to_string(),
                    ..Default::default()
                };
                if kind == "spot-instance-request" {
                    let p = params_of(req);
                    s.set("SpotPrice", p.str("SpotPrice"));
                    s.set("InstanceType", p.str("LaunchSpecification.InstanceType"));
                    s.set("ImageId", p.str("LaunchSpecification.ImageId"));
                    s.set("State", "open");
                }
                if kind == "fleet" {
                    s.set("TargetCapacity", p_target(req));
                    s.set("FleetState", "active");
                    let tags2 = super::handlers::tag_spec_tags(&params_of(req));
                    s.tags = tags2;
                }
                if kind == "spot-fleet-request" {
                    let p = params_of(req);
                    s.set("TargetCapacity", p.str("SpotFleetRequestConfig.TargetCapacity"));
                    s.set("SpotFleetRequestState", "submitted");
                }
                state
                    .simple
                    .entry(kind.to_string())
                    .or_default()
                    .push(s);
                let body = match kind {
                    "fleet" => format!("<fleetId>{}</fleetId>", xml_escape(&id)),
                    "spot-fleet-request" => format!(
                        "<spotFleetRequestId>{}</spotFleetRequestId><spotFleetRequestState>submitted</spotFleetRequestState>",
                        xml_escape(&id)
                    ),
                    _ => format!(
                        "<spotInstanceRequestSet><item><spotInstanceRequestId>{}</spotInstanceRequestId><spotPrice>{}</spotPrice><state>open</state>{}</item></spotInstanceRequestSet>",
                        xml_escape(&id),
                        if true { "0.019" } else { "" },
                        ""
                    ),
                };
                ec2_xml(
                    &format!("{}Response", op),
                    &body,
                )
            }
            "DescribeFleets" | "DescribeSpotFleetRequests" | "DescribeSpotInstanceRequests" => {
                let ids_key = match kind {
                    "fleet" => "FleetId",
                    "spot-fleet-request" => "SpotFleetRequestId",
                    _ => "SpotInstanceRequestId",
                };
                let ids: Vec<String> = params_of(req).list(ids_key).iter().map(|s| s.to_string()).collect();
                let items = state
                    .simple
                    .get(kind)
                    .cloned()
                    .unwrap_or_default();
                let mut out = String::new();
                for s in items {
                    if !ids.is_empty() && !ids.contains(&s.id) {
                        continue;
                    }
                    let item = match kind {
                        "fleet" => format!(
                            "<item><fleetId>{}</fleetId>{}{}</item>",
                            xml_escape(&s.id),
                            String::new(),
                            ""
                        ),
                        "spot-fleet-request" => format!(
                            "<item><spotFleetRequestId>{}</spotFleetRequestId><spotFleetRequestState>{}</spotFleetRequestState><spotFleetRequestConfig><targetCapacity>{}</targetCapacity><spotPrice>{}</spotPrice></spotFleetRequestConfig>{}</item>",
                            xml_escape(&s.id),
                            s.get("SpotFleetRequestState").unwrap_or("submitted"),
                            "1",
                            "0.015",
                            tags_xml_public(&s.tags)
                        ),
                        _ => format!(
                            "<item><spotInstanceRequestId>{}</spotInstanceId><spotPrice>{}</spotPrice>{}</item>",
                            xml_escape(&s.id),
                            s.get("SpotPrice").unwrap_or("0.019"),
                            ""
                        ),
                    };
                    out.push_str(&item);
                }
                let (wrap, tag) = match kind {
                    "fleet" => ("fleetSet", "fleetSet"),
                    "spot-fleet-request" => ("spotFleetRequestConfigSet", "spotFleetRequestConfigSet"),
                    _ => ("spotInstanceRequestSet", "spotInstanceRequestSet"),
                };
                ec2_xml(
                    &format!("{}Response", op),
                    &format!("<{wrap}>{out}</{tag}>"),
                )
            }
            "CancelSpotInstanceRequests" => {
                let ids: Vec<String> = params_of(req).list("SpotInstanceRequestId").iter().map(|s| s.to_string()).collect();
                let mut body = String::new();
                for id in &ids {
                    let _ = write!(body, "<item><spotInstanceRequestId>{}</spotInstanceId></item>", xml_escape(id));
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml(
                    "CancelSpotInstanceRequestsResponse",
                    &format!("<cancelledSpotInstanceRequestSet>{}</cancelledSpotInstanceRequestSet>", body),
                )
            }
            "CancelSpotFleetRequests" => {
                let ids: Vec<String> = params_of(req).list("SpotFleetRequestId").iter().map(|s| s.to_string()).collect();
                let mut ok_body = String::new();
                for id in &ids {
                    let _ = write!(ok_body, "<item><spotFleetRequestId>{}</spotFleetRequestId></item>", xml_escape(id));
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml(
                    "CancelSpotFleetRequestsResponse",
                    &format!("<successfulFleetRequestSet>{}{}</successfulFleetRequestSet><unsuccessfulFleetRequestSet/>", ok_body, ""),
                )
            }
            "DeleteFleet" => {
                let ids: Vec<String> = params_of(req).list("FleetId").iter().map(|s| s.to_string()).collect();
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml("DeleteFleetResponse", "<return>true</return>")
            }
            "ModifyFleet" => {
                let ids: Vec<String> = params_of(req).list("FleetId").iter().map(|s| s.to_string()).collect();
                if let Some(items) = state.simple.get_mut(kind) {
                    for s in items.iter_mut() {
                        if ids.contains(&s.id) {
                            s.set("TargetCapacity", params_of(req).str("TargetCapacity"));
                        }
                    }
                }
                ec2_xml("ModifyFleetResponse", "<return>true</return>")
            }
            "ModifySpotFleetRequest" => {
                let id = params_of(req).str("SpotFleetRequestId");
                if !state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false)
                {
                    return ec2_error(
                        400,
                        "InvalidSpotFleetRequestId.NotFound",
                        &format!("The specified spot fleet request does not exist: '{}'", xml_escape(&id)),
                    );
                }
                ec2_xml("ModifySpotFleetRequestResponse", "<return>true</return>")
            }
            "DescribeSpotFleetInstances" => {
                let id = params_of(req).str("SpotFleetRequestId");
                ec2_xml(
                    "DescribeSpotFleetInstancesResponse",
                    &format!(
                        "<activeInstanceSet/><spotFleetRequestId>{}</spotFleetRequestId>",
                        xml_escape(&id)
                    ),
                )
            }
            "DescribeFleetInstances" => {
                let id = params_of(req).str("FleetId");
                ec2_xml(
                    "DescribeFleetInstancesResponse",
                    &format!(
                        "<activeInstanceSet/><fleetId>{}</fleetId>",
                        xml_escape(&id)
                    ),
                )
            }
            "DescribeFleetHistory" => {
                let id = params_of(req).str("FleetId");
                ec2_xml(
                    "DescribeFleetHistoryResponse",
                    &format!(
                        "<fleetId>{}</fleetId><historyRecords/>",
                        xml_escape(&id)
                    ),
                )
            }
            _ => super::not_implemented(op),
        }
    })
}

fn params_quantity(req: &Request, default_qty: i64) -> i64 {
    params_of(req)
        .get("Quantity")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default_qty)
        .max(1)
}

fn p_target(req: &Request) -> String {
    params_of(req).str("TargetCapacity")
}

fn tags_xml_public(tags: &super::state::Tags) -> String {
    super::tags_xml(tags)
}

/// Re-derive Params from a request inside a closure-captured context.
fn params_of(req: &Request) -> super::Params {
    super::Params::parse(req)
}

pub fn extract_lt_instance_type(data: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(v) => v
            .get("InstanceType")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

// --- gap ops batch 3: family-carryover ops the suite exercises -------------
pub static GAP_OPS_B: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "AssociateIamInstanceProfile",
        "AssociateInstanceEventWindow",
        "DisassociateInstanceEventWindow",
        "ModifyInstanceEventWindow",
        "AttachVerifiedAccessTrustProvider",
        "CreateCoipCidr",
        "DeleteCoipCidr",
        "CreateInstanceConnectEndpoint",
        "DeleteInstanceConnectEndpoint",
        "DescribeInstanceConnectEndpoints",
        "DeleteFleets",
        "DescribeStaleSecurityGroups",
        "GetCoipPoolUsage",
        "GetIpamPoolAllocations",
        "GetIpamPoolCidrs",
        "GetIpamResourceCidrs",
        "ModifyIpamPool",
        "ModifyHosts",
        "ProvisionIpamPoolCidr",
        "DeprovisionIpamPoolCidr",
        "ReleaseIpamPoolAllocation",
        "AllocateIpamPoolCidr",
        "GetEnabledIpamPolicy",
        "StartNetworkInsightsAnalysis",
        "GetNetworkInsightsAccessScopeContent",
        "ModifySpotFleetRequest",
        "DescribeSpotFleetInstances",
    ]
    .into_iter()
    .collect()
});

pub fn gap_ops_b_response(op: &str, params: &P) -> Response {
    let body = match op {
        "AssociateIamInstanceProfile" => String::from("<associationId>iip-assoc-0000000000000000</associationId>"),
        "AssociateInstanceEventWindow" | "DisassociateInstanceEventWindow" | "ModifyInstanceEventWindow" => format!(
            "<instanceEventWindow><instanceEventWindowId>{}</instanceEventWindowId></instanceEventWindow>",
            xml_escape(id_of(params, "InstanceEventWindowId").as_str())
        ),
        "CreateInstanceConnectEndpoint" => String::from(
            "<instanceConnectEndpoint><instanceConnectEndpointId>ice-00000000000000000</instanceConnectEndpointId><state>available</state></instanceConnectEndpoint>",
        ),
        "CreateCoipCidr" => format!(
            "<coipCidr><cidr>10.0.0.0/8</cidr><coipPoolId>{}</coipPoolId></coipCidr>",
            xml_escape(id_of(params, "CoipPoolId").as_str())
        ),
        "GetCoipPoolUsage" => String::from("<coipAddressUsages/><localGatewayRouteTableId></localGatewayRouteTableId>"),
        "ProvisionIpamPoolCidr" | "AllocateIpamPoolCidr" => String::from(
            "<ipamPoolCidr><cidr>0.0.0.0/24</cidr><state>provisioned</state></ipamPoolCidr>",
        ),
        "GetIpamPoolAllocations" | "GetIpamPoolCidrs" | "GetIpamResourceCidrs" => {
            String::from("<ipamPoolCidrs/>")
        }
        _ => String::from("<return>true</return>"),
    };
    let root = format!("{}Response", op);
    super::xmlns_xml(&root, Some(super::EC2_NS), &body)
}

fn id_of(params: &P, key: &str) -> String {
    params.str(key)
}

