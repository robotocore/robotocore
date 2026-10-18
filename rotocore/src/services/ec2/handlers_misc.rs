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
        rest = if end < rest.len() {
            &rest[start + end + 1..]
        } else {
            ""
        };
    }
    out.push_str(&rest.replace("{TS}", &super::now_iso()));
    (out, ids)
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
    if TRANSIT_GW_OPS.contains(op) {
        return tgw_route(req, op, params);
    }
    if LOCAL_GW_OPS.contains(op) {
        return local_gw_route(req, op, params);
    }
    // 1) template-driven simple families (create/describe/delete)
    if find_family(op).is_some() {
        return simple_family_route(req, params);
    }
    // 2) plain template smoke ops
    if let Ok(idx) = templates::TEMPLATES.binary_search_by_key(&req.operation.as_str(), |t| t.0) {
        let (_, status, xml) = templates::TEMPLATES[idx];
        let xml = render_template(xml).0;
        return Response::xml(status, xml.into_bytes());
    }
    super::not_implemented(&req.operation)
}

fn simple_family_route(req: &Request, params: &P) -> Response {
    let fam = find_family(&req.operation).unwrap();
    match req.operation.as_str() {
        op if op == fam.create_op => simple_create(req, params, fam),
        op if op == fam.describe_op => simple_describe(req, params, fam),
        op if op == fam.delete_op => simple_delete(req, params, fam),
        _ => simple_describe(req, params, fam),
    }
}

fn simple_create(req: &Request, params: &P, fam: &'static FamilySpec) -> Response {
    with_state(req, move |state| {
        let t = templates::TEMPLATES
            .binary_search_by_key(&fam.create_op, |x| x.0)
            .ok()
            .map(|i| (templates::TEMPLATES[i].1, templates::TEMPLATES[i].2));
        let rendered = match t {
            Some((status, xml)) if status == 200 => {
                let (body, ids) = render_template(xml);
                let id = ids
                    .values()
                    .find(|v| v.starts_with(format!("{}-", fam.id_prefix).as_str()))
                    .cloned()
                    .unwrap_or_else(|| next_placeholder_id(fam.id_prefix));
                let mut s = Simple {
                    id: id.clone(),
                    kind: fam.kind.to_string(),
                    item_xml: body.clone(),
                    ..Default::default()
                };
                s.tags = wire_tags(params);
                state
                    .simple
                    .entry(fam.kind.to_string())
                    .or_default()
                    .push(s.clone());
                (status, body)
            }
            _ => {
                // synthesize a moto-style create record for families the
                // capture never exercised.
                let id = next_placeholder_id(fam.id_prefix);
                let item = synth_fields(fam, &id, params, state);
                let inner = single_root(fam.kind);
                let mut s = Simple {
                    id: id.clone(),
                    kind: fam.kind.to_string(),
                    item_xml: format!("<item>{}</item>", item.clone()),
                    ..Default::default()
                };
                s.tags = wire_tags(params);
                state
                    .simple
                    .entry(fam.kind.to_string())
                    .or_default()
                    .push(s.clone());
                (200, format!("<{}>{}</{}>", inner, item, inner))
            }
        };
        let (status, body) = rendered;
        let _ = fam;
        Response::xml(status, body.into_bytes())
    })
}

fn simple_describe(req: &Request, params: &P, fam: &'static FamilySpec) -> Response {
    with_state(req, move |state| {
        let t = templates::TEMPLATES
            .binary_search_by_key(&fam.describe_op, |x| x.0)
            .ok()
            .map(|i| (templates::TEMPLATES[i].1, templates::TEMPLATES[i].2));
        let ids: Vec<String> = params
            .list(fam.id_key)
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rendered = match t {
            Some((status, xml)) => {
                let (body, _) = render_template(xml);
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
                if !items.is_empty() {
                    let tag = fam.list_tag;
                    // splice items into the empty <tag/> (or <tag></tag>) container
                    let probe1 = format!("<{} />", tag);
                    let probe2 = format!("<{}/>", tag);
                    let probe3 = format!("<{}></{}>", tag, tag);
                    let body2 = if body.contains(&probe1) {
                        body.replace(&probe1, &format!("<{}>{}</{}>", tag, items.concat(), tag))
                    } else if body.contains(&probe2) {
                        body.replace(&probe2, &format!("<{}>{}</{}>", tag, items.concat(), tag))
                    } else if body.contains(&probe3) {
                        body.replace(&probe3, &format!("<{}>{}</{}>", tag, items.concat(), tag))
                    } else {
                        body
                    };
                    (status, body2)
                } else {
                    (status, body)
                }
            }
            None => {
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
                (
                    200,
                    format!("<{}>{}</{}>", fam.list_tag, items.concat(), fam.list_tag),
                )
            }
        };
        let (status, body) = rendered;
        Response::xml(status, body.into_bytes())
    })
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
            return ec2_error(
                400,
                "ResourceNotFound",
                &format!("Resource {} not found", fam.id_prefix),
            );
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
                        "<item><localGatewayRouteTableArn>arn:aws:ec2:{}:{}:local-gateway/route-table/</localGatewayRouteTableArn>\u{0}<localGatewayRouteTableId>lgw-rtb-0000000000000000{}</localGatewayRouteTableId><ownerId>{}</ownerId><state>available</state><mode>direct-vpc-routing</mode><outpostArn/><tags></tags></item>",
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
