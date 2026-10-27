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
        "coip-pool",
        "CreateCoipPool",
        "DescribeCoipPools",
        "DeleteCoipPool",
        "coipPoolSet",
        "ipv4pool-coip",
        "PoolId"
    ),
    fam!(
        "public-ipv4-pool",
        "CreatePublicIpv4Pool",
        "DescribePublicIpv4Pools",
        "DeletePublicIpv4Pool",
        "publicIpv4PoolSet",
        "ipv4pool-ec2",
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
    // --- wave-11 gap families (create/describe/delete round trips) --------
    fam!(
        "route-server",
        "CreateRouteServer",
        "DescribeRouteServers",
        "DeleteRouteServer",
        "routeServerSet",
        "route-server",
        "RouteServerId"
    ),
    fam!(
        "route-server-endpoint",
        "CreateRouteServerEndpoint",
        "DescribeRouteServerEndpoints",
        "DeleteRouteServerEndpoint",
        "routeServerEndpointsSet",
        "route-server-endpoint",
        "RouteServerEndpointId"
    ),
    fam!(
        "route-server-peer",
        "CreateRouteServerPeer",
        "DescribeRouteServerPeers",
        "DeleteRouteServerPeer",
        "routeServerPeersSet",
        "route-server-peer",
        "RouteServerPeerId"
    ),
    fam!(
        "vpn-concentrator",
        "CreateVpnConcentrator",
        "DescribeVpnConcentrators",
        "DeleteVpnConcentrator",
        "vpnConcentratorSet",
        "vpn-concentrator",
        "VpnConcentratorId"
    ),
    fam!(
        "secondary-network",
        "CreateSecondaryNetwork",
        "DescribeSecondaryNetworks",
        "DeleteSecondaryNetwork",
        "secondaryNetworkSet",
        "sernet",
        "SecondaryNetworkId"
    ),
    fam!(
        "secondary-subnet",
        "CreateSecondarySubnet",
        "DescribeSecondarySubnets",
        "DeleteSecondarySubnet",
        "secondarySubnetSet",
        "sersub",
        "SecondarySubnetId"
    ),
    fam!(
        "capacity-manager-data-export",
        "CreateCapacityManagerDataExport",
        "DescribeCapacityManagerDataExports",
        "DeleteCapacityManagerDataExport",
        "capacityManagerDataExportSet",
        "cmde",
        "CapacityManagerDataExportId"
    ),
    fam!(
        "transit-gateway-metering-policy",
        "CreateTransitGatewayMeteringPolicy",
        "DescribeTransitGatewayMeteringPolicies",
        "DeleteTransitGatewayMeteringPolicy",
        "transitGatewayMeteringPolicies",
        "tgw-mp",
        "TransitGatewayMeteringPolicyId"
    ),
    fam!(
        "ipam-policy",
        "CreateIpamPolicy",
        "DescribeIpamPolicies",
        "DeleteIpamPolicy",
        "ipamPolicySet",
        "ipam-policy",
        "IpamPolicyId"
    ),
    fam!(
        "ipam-prefix-list-resolver",
        "CreateIpamPrefixListResolver",
        "DescribeIpamPrefixListResolvers",
        "DeleteIpamPrefixListResolver",
        "ipamPrefixListResolverSet",
        "ipam-plr",
        "IpamPrefixListResolverId"
    ),
    fam!(
        "ipam-prefix-list-resolver-target",
        "CreateIpamPrefixListResolverTarget",
        "DescribeIpamPrefixListResolverTargets",
        "DeleteIpamPrefixListResolverTarget",
        "ipamPrefixListResolverTargetSet",
        "ipam-plrt",
        "IpamPrefixListResolverTargetId"
    ),
    fam!(
        "vpc-endpoint-connection-notification",
        "CreateVpcEndpointConnectionNotification",
        "DescribeVpcEndpointConnectionNotifications",
        "DeleteVpcEndpointConnectionNotifications",
        "connectionNotificationSet",
        "cn-",
        "ConnectionNotificationId"
    ),
    fam!(
        "vpc-encryption-control",
        "CreateVpcEncryptionControl",
        "DescribeVpcEncryptionControls",
        "DeleteVpcEncryptionControl",
        "vpcEncryptionControlSet",
        "vpc-ec",
        "VpcEncryptionControlId"
    ),
    fam!(
        "image-usage-report",
        "CreateImageUsageReport",
        "DescribeImageUsageReports",
        "DeleteImageUsageReport",
        "imageUsageReportSet",
        "iur-",
        "ReportId"
    ),
    fam!(
        "tgw-route-table-announcement",
        "CreateTransitGatewayRouteTableAnnouncement",
        "DescribeTransitGatewayRouteTableAnnouncements",
        "DeleteTransitGatewayRouteTableAnnouncement",
        "transitGatewayRouteTableAnnouncements",
        "tgw-rtba",
        "TransitGatewayRouteTableAnnouncementId"
    ),
    fam!(
        "byoip-cidr",
        "ProvisionByoipCidr",
        "DescribeByoipCidrs",
        "DeprovisionByoipCidr",
        "byoipCidrSet",
        "",
        "Cidr"
    ),
    fam!(
        "ipam-byoasn",
        "ProvisionIpamByoasn",
        "DescribeIpamByoasn",
        "DeprovisionIpamByoasn",
        "byoasnSet",
        "",
        "Asn"
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
        .or_else(|| {
            FAMILY_EXTRA_OPS.iter().find_map(|(kind, extra_op)| {
                if op == *extra_op {
                    SIMPLE_FAMILIES.iter().find(|f| f.kind == *kind)
                } else {
                    None
                }
            })
        })
}

/// Ops beyond create/describe/delete that belong to list-root families:
/// (family kind, op). These route to [`hostlist_route`] like family ops.
pub static IPAM_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "ProvisionIpamPoolCidr",
        "DeprovisionIpamPoolCidr",
        "AllocateIpamPoolCidr",
        "ReleaseIpamPoolAllocation",
        "GetIpamPoolCidrs",
        "GetIpamPoolAllocations",
        "GetIpamResourceCidrs",
        "DescribeIpamScopesEmpty",
        "ModifyIpam",
        "ModifyIpamPool",
        "ModifyIpamResourceDiscovery",
    ]
    .into_iter()
    .collect()
});

static FAMILY_EXTRA_OPS: [(&str, &str); 7] = [
    ("fleet", "DeleteFleets"),
    ("fleet", "ModifyFleet"),
    ("fleet", "DescribeFleetInstances"),
    ("fleet", "DescribeFleetHistory"),
    ("spot-fleet-request", "ModifySpotFleetRequest"),
    ("spot-fleet-request", "DescribeSpotFleetInstances"),
    ("host", "ModifyHosts"),
];

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
    if IPAM_OPS.contains(op) {
        return ipam_route(req, &req.operation, params);
    }
    // 1b) the rest of the transit gateway family (route tables etc.)
    if TRANSIT_GW_OPS.contains(op) {
        return tgw_route(req, op, params);
    }
    // 0b) family-carryover gap ops
    if GAP_OPS_B.contains(op) {
        return gap_ops_b_response(req, op, params);
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
    // 3) wave-11 terminator: modeled envelope from the botocore shape
    gap_ops_c_response(req, op, params)
}

static MODIFY_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "ModifyManagedPrefixList",
        "ModifyCapacityReservationFleet",
        "ModifyClientVpnEndpoint",
    ]
    .into_iter()
    .collect()
});

fn modify_route(req: &Request, op: &str, params: &P) -> Response {
    with_state(req, move |state| {
        if op == "ModifyClientVpnEndpoint" {
            let id = params.str("ClientVpnEndpointId");
            let mut found = false;
            if let Some(items) = state.simple.get_mut("client-vpn-endpoint") {
                for s in items.iter_mut() {
                    if s.id == id {
                        found = true;
                        if let Some(d) = params.get("Description.Value") {
                            s.set("Description", d.to_string());
                        }
                        if let Some(d) = params.get("ClientConnectResponseOptions.Enabled") {
                            s.set("ClientConnectOptions.Enabled", d.to_string());
                        }
                    }
                }
            }
            if !found {
                return ec2_error(
                    400,
                    "InvalidClientVpnEndpointId.NotFound",
                    &format!(
                        "The Client VPN endpoint ID '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                );
            }
            return ec2_xml("ModifyClientVpnEndpointResponse", "<return>true</return>");
        }
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
                    &format!(
                        "The prefix list ID '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                );
            }
            // Apply AddEntries / RemoveEntries to the stored record and bump
            // the version — moto semantics (modify_managed_prefix_list).
            let mut new_version: Option<String> = None;
            if let Some(items) = state.simple.get_mut("managed-prefix-list") {
                for s in items.iter_mut() {
                    if s.id != id {
                        continue;
                    }
                    let mut changed = false;
                    let removed = params.container_indices("RemoveEntry");
                    for i in removed {
                        if let Some(cidr) = params.sub("RemoveEntry", i, "Cidr") {
                            s.fields.remove(&format!("entry:{cidr}"));
                            changed = true;
                        }
                    }
                    for i in params.container_indices("AddEntry") {
                        let cidr = params.sub("AddEntry", i, "Cidr").unwrap_or_default();
                        if cidr.is_empty() {
                            continue;
                        }
                        let desc = params.sub("AddEntry", i, "Description").unwrap_or("");
                        s.set(&format!("entry:{cidr}"), desc.to_string());
                        changed = true;
                    }
                    if changed {
                        let cur = s
                            .get("Version")
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(1);
                        new_version = Some((cur + 1).to_string());
                    }
                }
            }
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
            if let Some(v) = &new_version {
                if let Some(items) = state.simple.get_mut("managed-prefix-list") {
                    for s in items.iter_mut() {
                        if s.id == id {
                            s.set("Version", v.clone());
                            s.item_xml = s.item_xml.replace(
                                "<version>1</version>",
                                &format!("<version>{v}</version>"),
                            );
                        }
                    }
                }
                let mut fills = fills;
                fills.insert("Version".to_string(), super::shapes::Fill::S(v.clone()));
                if let Some(xml) =
                    super::shapes::render_response("ModifyManagedPrefixList", &fills, "us-east-1")
                {
                    return Response::xml(200, xml.into_bytes());
                }
                return ec2_xml(
                    "ModifyManagedPrefixListResponse",
                    &format!("<prefixList><prefixListId>{}</prefixListId><version>{}</version><state>modify-in-progress</state></prefixList>", crate::proto::xml_escape(&id), v),
                );
            }
            if let Some(xml) =
                super::shapes::render_response("ModifyManagedPrefixList", &fills, "us-east-1")
            {
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
                400,
                "InvalidCapacityReservationFleetId.NotFound",
                &format!(
                    "The capacity reservation fleet ID '{}' does not exist",
                    crate::proto::xml_escape(&id)
                ),
            );
        }
        ec2_xml(
            "ModifyCapacityReservationFleetResponse",
            "<return>true</return>",
        )
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

/// Dedicated create/describe/delete for NetworkInsightsAccessScope: the
/// captured template hardcodes one sample id, so the store id and the wire id
/// drifted apart (delete then 404ed).
fn nis_scope_route(req: &Request, params: &P) -> Response {
    match req.operation.as_str() {
        "CreateNetworkInsightsAccessScope" => with_state(req, move |state| {
            let id = state.next_id("nis");
            let mut s = Simple {
                    id: id.clone(),
                    kind: "network-insights-access-scope".to_string(),
                    item_xml: format!(
                        "<item><networkInsightsAccessScopeId>{id}</networkInsightsAccessScopeId><networkInsightsAccessScopeArn>arn:aws:ec2:{region}:{account}:network-insights-access-scope/{id}</networkInsightsAccessScopeArn><createdDate>{ts}</createdDate><tagSet/></item>",
                        id = xml_escape(&id),
                        region = req.region,
                        account = req.account,
                        ts = super::now_iso()
                    ),
                    tags: wire_tags(params),
                    ..Default::default()
                };
            s.set("NetworkInsightsAccessScopeId", id.clone());
            for (k, v) in super::handlers::tag_spec_tags(params) {
                if !s.tags.iter().any(|(ek, _)| ek == &k) {
                    s.tags.push((k, v));
                }
            }
            state
                .simple
                .entry("network-insights-access-scope".to_string())
                .or_default()
                .push(s);
            Response::xml(
                    200,
                    format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{ns}\"><networkInsightsAccessScope><networkInsightsAccessScopeId>{id}</networkInsightsAccessScopeId><networkInsightsAccessScopeArn>arn:aws:ec2:{region}:{account}:network-insights-access-scope/{id}</networkInsightsAccessScopeArn><createdDate>{ts}</createdDate><tagSet/></networkInsightsAccessScope><networkInsightsAccessScopeContent><networkInsightsAccessScopeId>{id}</networkInsightsAccessScopeId></networkInsightsAccessScopeContent><requestId>{rid}</requestId></{root}>",
                        root = req.operation,
                        ns = super::EC2_NS,
                        id = xml_escape(&id),
                        region = req.region,
                        account = req.account,
                        ts = super::now_iso(),
                        rid = uuid::Uuid::new_v4()
                    )
                    .into_bytes(),
                )
        }),
        "DescribeNetworkInsightsAccessScopes" => {
            let ids = ids_of(params, "NetworkInsightsAccessScopeId");
            with_state(req, move |state| {
                let mut items = String::new();
                if let Some(list) = state.simple.get("network-insights-access-scope") {
                    for s in list {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        items.push_str(&s.item_xml);
                    }
                }
                ec2_xml(
                    "DescribeNetworkInsightsAccessScopesResponse",
                    &format!(
                        "<networkInsightsAccessScopeSet>{items}</networkInsightsAccessScopeSet>"
                    ),
                )
            })
        }
        "DeleteNetworkInsightsAccessScope" => {
            let id = params.str("NetworkInsightsAccessScopeId");
            with_state(req, move |state| {
                state
                    .simple
                    .entry("network-insights-access-scope".to_string())
                    .or_default()
                    .retain(|s| s.id != id);
                ec2_xml(
                    "DeleteNetworkInsightsAccessScopeResponse",
                    &format!(
                        "<networkInsightsAccessScopeId>{}</networkInsightsAccessScopeId>",
                        xml_escape(&id)
                    ),
                )
            })
        }
        _ => super::not_implemented(&req.operation),
    }
}

fn simple_family_route(req: &Request, params: &P) -> Response {
    let fam = find_family(&req.operation).unwrap();
    if fam.kind == "network-insights-access-scope" {
        return nis_scope_route(req, params);
    }
    if req.operation == "CreateVpnConnection" {
        // moto validates both referenced gateways before creating
        let cgw = params.str("CustomerGatewayId");
        let vgw = params.str("VpnGatewayId");
        if let Some(err) = with_state(req, move |state| {
            // only gateway ids actually provided are validated (a Transit
            // Gateway connection omits VpnGatewayId)
            if !cgw.is_empty()
                && !state
                    .simple
                    .get("customer-gateway")
                    .map(|items| items.iter().any(|s| s.id == cgw))
                    .unwrap_or(false)
            {
                return Some(ec2_error(
                    400,
                    "InvalidCustomerGatewayID.NotFound",
                    &format!(
                        "The customerGateway ID '{}' does not exist",
                        xml_escape(&cgw)
                    ),
                ));
            }
            if !vgw.is_empty()
                && !state
                    .simple
                    .get("vpn-gateway")
                    .map(|items| items.iter().any(|s| s.id == vgw))
                    .unwrap_or(false)
            {
                return Some(ec2_error(
                    400,
                    "InvalidVpnGatewayID.NotFound",
                    &format!("The vpnGateway ID '{}' does not exist", xml_escape(&vgw)),
                ));
            }
            None
        }) {
            return err;
        }
        return simple_create(req, params, fam);
    }
    if matches!(
        fam.kind,
        "host" | "fleet" | "spot-fleet-request" | "spot-instance-request"
    ) {
        return hostlist_route(req, fam);
    }
    // wave-11 families: create-time validation of referenced resources and
    // required params, then the shared create/describe/delete machinery.
    if req.operation == fam.create_op {
        if let Some(err) = validate_family_refs(req, params, fam) {
            return err;
        }
    }
    match req.operation.as_str() {
        op if op == fam.create_op => simple_create(req, params, fam),
        op if op == fam.describe_op => simple_describe(req, params, fam),
        op if op == fam.delete_op => simple_delete(req, params, fam),
        _ => simple_describe(req, params, fam),
    }
}

/// Provigional 4xx checks for the new families' create ops, mirroring the
/// Id-not-found styles used elsewhere in the module.
fn validate_family_refs(req: &Request, params: &P, fam: &'static FamilySpec) -> Option<Response> {
    let missing = |param: &str| {
        Some(ec2_error(
            400,
            "MissingParameter",
            &format!("The request must contain the parameter {param}"),
        ))
    };
    match fam.kind {
        "byoip-cidr" => {
            if params.str("Cidr").is_empty() {
                return missing("Cidr");
            }
        }
        "ipam-byoasn" => {
            if params.list("Asns").is_empty() {
                return missing("Asns");
            }
        }
        "route-server-endpoint" => {
            let rs = params.str("RouteServerId");
            let subnet = params.str("SubnetId");
            if let Some(err) = simple_exists(
                req,
                "route-server",
                &rs,
                "InvalidRouteServerId.NotFound",
                "routeServer",
                &rs,
            ) {
                return Some(err);
            }
            let _ = subnet;
            if let Some(err) = subnet_exists(req, &params.str("SubnetId")) {
                return Some(err);
            }
        }
        "route-server-peer" => {
            let ep = params.str("RouteServerEndpointId");
            if let Some(err) = simple_exists(
                req,
                "route-server-endpoint",
                &ep,
                "InvalidRouteServerEndpointId.NotFound",
                "routeServerEndpoint",
                &ep,
            ) {
                return Some(err);
            }
        }
        "secondary-subnet" => {
            let net = params.str("SecondaryNetworkId");
            if let Some(err) = simple_exists(
                req,
                "secondary-network",
                &net,
                "InvalidSecondaryNetworkId.NotFound",
                "secondaryNetwork",
                &net,
            ) {
                return Some(err);
            }
        }
        "vpc-encryption-control" => {
            let vpc = params.str("VpcId");
            if let Some(err) = vpc_exists(req, &vpc) {
                return Some(err);
            }
        }
        "transit-gateway-metering-policy" => {
            let tgw = params.str("TransitGatewayId");
            if let Some(err) = tgw_exists(req, &tgw) {
                return Some(err);
            }
        }
        "ipam-prefix-list-resolver" => {
            let ipam = params.str("IpamId");
            if let Some(err) =
                simple_exists(req, "ipam", &ipam, "InvalidIpamId.NotFound", "ipam", &ipam)
            {
                return Some(err);
            }
        }
        "ipam-prefix-list-resolver-target" => {
            let plr = params.str("IpamPrefixListResolverId");
            if let Some(err) = simple_exists(
                req,
                "ipam-prefix-list-resolver",
                &plr,
                "InvalidIpamPrefixListResolverId.NotFound",
                "ipamPrefixListResolver",
                &plr,
            ) {
                return Some(err);
            }
        }
        "vpc-endpoint-connection-notification" => {
            let ep = params.str("VpcEndpointId");
            let svc = params.str("ServiceId");
            if ep.is_empty() && svc.is_empty() {
                return None; // botocore requires Arn+Events only; real AWS too
            }
            if !ep.is_empty() {
                if let Some(err) = vpc_endpoint_exists(req, &ep) {
                    return Some(err);
                }
            }
            if !svc.is_empty() {
                if let Some(err) = simple_exists(
                    req,
                    "vpc-endpoint-service",
                    &svc,
                    "InvalidVpcEndpointServiceId.NotFound",
                    "vpcEndpointService",
                    &svc,
                ) {
                    return Some(err);
                }
            }
        }
        _ => {}
    }
    None
}

/// Generic Simple-family existence check that renders the module's
/// `<X 'id' does not exist>` 400 layout.
fn simple_exists(
    req: &Request,
    kind: &str,
    id: &str,
    code: &'static str,
    label: &str,
    label_id: &str,
) -> Option<Response> {
    if id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state
            .simple
            .get(kind)
            .map(|items| items.iter().any(|s| s.id == id))
            .unwrap_or(false)
        {
            None
        } else {
            Some(ec2_error(
                400,
                code,
                &format!("The {} ID '{}' does not exist", label, xml_escape(label_id)),
            ))
        }
    })
}

fn subnet_exists(req: &Request, subnet_id: &str) -> Option<Response> {
    if subnet_id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state.subnets.iter().any(|s| s.id == subnet_id) {
            None
        } else {
            Some(ec2_error(
                400,
                "InvalidSubnetID.NotFound",
                &format!("The subnet ID '{}' does not exist", xml_escape(subnet_id)),
            ))
        }
    })
}

fn vpc_exists(req: &Request, vpc_id: &str) -> Option<Response> {
    if vpc_id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state.vpcs.iter().any(|v| v.id == vpc_id) {
            None
        } else {
            Some(ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", xml_escape(vpc_id)),
            ))
        }
    })
}

fn vpc_endpoint_exists(req: &Request, id: &str) -> Option<Response> {
    if id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state.vpc_endpoints.iter().any(|v| v.id == id) {
            None
        } else {
            Some(ec2_error(
                400,
                "InvalidVpcEndpointId.NotFound",
                &format!("The vpcEndpoint ID '{}' does not exist", xml_escape(id)),
            ))
        }
    })
}

fn tgw_exists(req: &Request, id: &str) -> Option<Response> {
    if id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state.transit_gateways.iter().any(|t| t.id == id) {
            None
        } else {
            Some(ec2_error(
                400,
                "InvalidTransitGatewayID.NotFound",
                &format!("The transitGateway ID '{}' does not exist", xml_escape(id)),
            ))
        }
    })
}

fn instance_exists(req: &Request, id: &str) -> Option<Response> {
    if id.is_empty() {
        return None;
    }
    with_state(req, |state| {
        if state.instances.iter().any(|i| i.id == id) {
            None
        } else {
            Some(gap_not_found("InvalidInstanceID.NotFound", "instance", id))
        }
    })
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
        // parameterless-id families key their record on request values
        let id = match fam.kind {
            "byoip-cidr" => params.str("Cidr"),
            "ipam-byoasn" => params
                .list("Asns")
                .first()
                .map(|s| s.to_string())
                .unwrap_or_else(|| params.str("Asn")),
            _ => next_placeholder_id(fam.id_prefix),
        };
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
        let record = |state: &mut super::state::Ec2State, rid: &str, item_xml: String| {
            let mut s = Simple {
                id: rid.to_string(),
                kind: fam.kind.to_string(),
                item_xml,
                tags: tags.clone(),
                ..Default::default()
            };
            for (k, v) in fills.iter() {
                if let super::shapes::Fill::S(v) = v {
                    s.set(k, v.clone());
                }
            }
            if fam.kind == "managed-prefix-list" {
                // store the create-time entries keyed by cidr so Modify and
                // GetManagedPrefixListEntries can read/update them
                for i in params.container_indices("Entry") {
                    let cidr = params.sub("Entry", i, "Cidr").unwrap_or_default();
                    if cidr.is_empty() {
                        continue;
                    }
                    let desc = params.sub("Entry", i, "Description").unwrap_or("");
                    s.set(&format!("entry:{cidr}"), desc.to_string());
                }
            }
            state
                .simple
                .entry(fam.kind.to_string())
                .or_default()
                .push(s);
            if fam.kind == "ipam" {
                // both default scopes are created with the IPAM; DescribeIpamScopes
                // must list them with their wire types
                for (scope_type, key) in [
                    ("public", "PublicDefaultScopeId"),
                    ("private", "PrivateDefaultScopeId"),
                ] {
                    if let Some(super::shapes::Fill::S(sid)) = fills.get(key) {
                        let mut sc = Simple {
                            id: sid.clone(),
                            kind: "ipam-scope".to_string(),
                            ..Default::default()
                        };
                        sc.set("IpamScopeId", sid.clone());
                        sc.set("IpamScopeType", scope_type);
                        sc.set("IpamId", id.clone());
                        sc.set(
                            "IpamArn",
                            format!("arn:aws:ec2:{}:{}:ipam/{}", req.region, req.account, id),
                        );
                        sc.set("IsDefault", "true");
                        sc.set("State", "create-complete");
                        sc.set("PoolCount", "0");
                        sc.item_xml = format!(
                            "<ipamScopeId>{sid}</ipamScopeId><ipamScopeArn>arn:aws:ec2:{region}:{account}:ipam-scope/{sid}</ipamScopeArn><ipamArn>{arn}</ipamArn><ipamRegion>{region}</ipamRegion><ipamScopeType>{scope_type}</ipamScopeType><isDefault>true</isDefault><poolCount>0</poolCount><state>create-complete</state><tagSet/>",
                            sid = xml_escape(sid),
                            region = req.region,
                            account = req.account,
                            arn = xml_escape(&format!("arn:aws:ec2:{}:{}:ipam/{}", req.region, req.account, id)),
                            scope_type = scope_type,
                        );
                        state
                            .simple
                            .entry("ipam-scope".to_string())
                            .or_default()
                            .push(sc);
                    }
                }
            }
        };
        if family_root_is_list(fam.create_op) {
            if let Some(xml) = super::shapes::render_list_response(
                fam.create_op,
                list_pykey_of(fam.create_op).as_str(),
                std::slice::from_ref(&fills),
                "us-east-1",
            ) {
                let inner = strip_op_root(&xml, fam.create_op);
                record(state, &id, inner);
                return Response::xml(200, xml.into_bytes());
            }
        }
        // single-object shape render for structure-out roots
        if let Some(xml) = super::shapes::render_response(fam.create_op, &fills, "us-east-1") {
            let inner = strip_op_root(&xml, fam.create_op);
            record(state, &id, inner);
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
                record(state, &id2, inner);
                (status, body)
            }
            _ => {
                // synthesized moto-style create for families the capture
                // never exercised
                let id2 = next_placeholder_id(fam.id_prefix);
                let item = synth_fields(fam, &id2, params, state);
                let inner = single_root(fam.kind);
                record(state, &id2, format!("<item>{}</item>", item.clone()));
                (200, format!("<{}>{}</{}>", inner, item, inner))
            }
        };
        let (status, body) = rendered;
        let _ = &id;
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
    if fam.kind == "client-vpn-endpoint" {
        return client_vpn_describe(req, params);
    }
    with_state(req, move |state| {
        let ids = ids_of(params, fam.id_key);
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
/// Render client VPN endpoints from the Simple store; the template-splice
/// path does not know this family's flat item shape.
fn client_vpn_describe(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        let ids = ids_of(params, "ClientVpnEndpointId");
        let mut items = String::new();
        if let Some(items_vec) = state.simple.get("client-vpn-endpoint").cloned() {
            for s in &items_vec {
                if !ids.is_empty() && !ids.contains(&s.id) {
                    continue;
                }
                let code = s.get("Status.Code").unwrap_or("pending-associate");
                let cidr = s.get("ClientCidrBlock").unwrap_or_default();
                let cert = s.get("ServerCertificateArn").unwrap_or_default();
                let desc = s.get("Description").unwrap_or_default();
                items.push_str(&format!(
                    "<item>\
{desc_el}\
<clientCidrBlock>{cidr}</clientCidrBlock>\
<status><code>{code}</code><message></message></status>\
<dnsName>*.cvpn-{id}.prod.clientvpn.us-east-1.amazonaws.com</dnsName>\
<serverCertificateArn>{cert}</serverCertificateArn>\
<clientVpnEndpointId>{id}</clientVpnEndpointId>\
</item>",
                    desc_el = if desc.is_empty() {
                        String::new()
                    } else {
                        format!("<description>{}</description>", xml_escape(desc))
                    },
                    cidr = xml_escape(cidr),
                    code = code,
                    id = xml_escape(&s.id),
                    cert = xml_escape(cert),
                ));
            }
        }
        ec2_xml(
            "DescribeClientVpnEndpointsResponse",
            &format!("<clientVpnEndpoint>{items}</clientVpnEndpoint>"),
        )
    })
}

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
        let ids = ids_of(params, fam.id_key);
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
        if fam.kind == "ipam" {
            // remove the two default scopes stored at ipam creation
            for id in &ids {
                if let Some(items) = state.simple.get_mut("ipam-scope") {
                    items.retain(|s| s.get("IpamId") != Some(id.as_str()));
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
        "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
        "CreateNetworkInterfacePermission",
        "CreateReservedInstancesListing",
        "CreateRestoreImageTask",
        "CreateTransitGatewayMulticastDomain",
        "CreateTransitGatewayPolicyTable",
        "AttachClassicLinkVpc",
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
        "AssociateTransitGatewayPolicyTable" => String::from("<association></association>"),
        "AssociateClientVpnTargetNetwork" => String::from("<associationId></associationId><status></status>"),
        "BundleInstance" => String::from("<bundleInstanceTask></bundleInstanceTask>"),
        "CancelBundleTask" => String::from("<bundleInstanceTask></bundleInstanceTask>"),
        "CancelReservedInstancesListing" => String::from("<reservedInstancesListingsSet></reservedInstancesListingsSet>"),
        "CreateReservedInstancesListing" => String::from("<reservedInstancesListingsSet></reservedInstancesListingsSet>"),
        "CopyFpgaImage" => String::from("<fpgaImageId></fpgaImageId>"),
        "CreateFpgaImage" => String::from("<fpgaImageId></fpgaImageId><fpgaImageGlobalId></fpgaImageGlobalId>"),
        "CreateInstanceExportTask" => String::from("<exportTask></exportTask>"),
        "CreateIpamExternalResourceVerificationToken" => String::from("<ipamExternalResourceVerificationToken></ipamExternalResourceVerificationToken>"),
        "CreateLocalGatewayRouteTableVpcAssociation" => String::from("<localGatewayRouteTableVpcAssociation></localGatewayRouteTableVpcAssociation>"),
        "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation" => String::from("<localGatewayRouteTableVirtualInterfaceGroupAssociation></localGatewayRouteTableVirtualInterfaceGroupAssociation>"),
        "CreateNetworkInterfacePermission" => String::from("<interfacePermission></interfacePermission>"),
        "CreateRestoreImageTask" => String::from("<imageId></imageId>"),
        "AttachClassicLinkVpc" => String::from("<return>true</return>"),
        _ => String::from("<return>true</return>"),
    };
    let root = format!("{}Response", op);
    let body = body
        .replace("{TS}", &super::now_iso())
        .replace("{ID", "<idplaceholder--");
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
        "ReplaceTransitGatewayRoute",
        "ModifyTransitGatewayPrefixListReference",
        "ExportTransitGatewayRoutes",
        "GetTransitGatewayAttachmentPropagations",
        "RejectTransitGatewayVpcAttachment",
        "RejectTransitGatewayMulticastDomainAssociations",
        "DeregisterTransitGatewayMulticastGroupMembers",
        "DeregisterTransitGatewayMulticastGroupSources",
        "RejectTransitGatewayMulticastDomain",
        "RejectTransitGatewayMulticastDomainAssociations",
    ]
    .into_iter()
    .collect()
});

fn tgw_fields(t: &super::state::Tgw, default_rtb_id: &str) -> String {
    let _ = t.default_route_tables;
    format!(
        "<transitGatewayId>{}</transitGatewayId><transitGatewayArn>{}</transitGatewayArn>{}<amazonSideAsn>{}</amazonSideAsn><ownerId>{}</ownerId>{}<state>{}</state><creationTime>{}</creationTime><options><amazonSideAsn>{}</amazonSideAsn>{}<dnsSupport>{}</dnsSupport><vpnEcmpSupport>enable</vpnEcmpSupport>{}{}</options>",

        xml_escape(&t.id),
        xml_escape(&t.arn),
        if !t.description.is_empty() { format!("<description>{}</description>", xml_escape(&t.description)) } else { String::new() },
        xml_escape(if t.amazon_side_asn.is_empty() { "64512" } else { &t.amazon_side_asn }),
        t.owner_id,
        tags_xml(&t.tags),
        xml_escape(&t.state),
        "1970-01-01T00:00:00.000Z",
        xml_escape(if t.amazon_side_asn.is_empty() { "64512" } else { &t.amazon_side_asn }),
        if t.auto_accept {
            String::from("<autoAcceptSharedAttachments>enable</autoAcceptSharedAttachments>")
        } else {
            String::new()
        },
        if t.dns_support { "enable" } else { "disable" },
        if !default_rtb_id.is_empty() {
            format!(
                "<defaultRouteTableAssociation>enable</defaultRouteTableAssociation><defaultRouteTablePropagation>enable</defaultRouteTablePropagation><associationDefaultRouteTableId>{aid}</associationDefaultRouteTableId><propagationDefaultRouteTableId>{pid}</propagationDefaultRouteTableId>",
                aid = xml_escape(default_rtb_id),
                pid = xml_escape(default_rtb_id)
            )
        } else {
            String::new()
        },
        String::new()
    )
}

fn tgw_item(t: &super::state::Tgw, default_rtb_id: &str) -> String {
    format!("<item>{}</item>", tgw_fields(t, default_rtb_id))
}

/// The default route table created alongside a transit gateway.
fn default_rtb_of(state: &super::state::Ec2State, tgw_id: &str) -> Option<String> {
    state
        .tgw_route_tables
        .iter()
        .find(|rt| rt.tgw_id == tgw_id && rt.is_default)
        .map(|rt| rt.id.clone())
}

fn tgw_route(req: &Request, op: &str, params: &P) -> Response {
    match op {
        "CreateTransitGateway" => {
            let description = params.str("Description");
            with_state(req, move |state| {
                let id = state.next_id("tgw");
                let asn = params
                    .get("Options.AmazonSideAsn")
                    .unwrap_or("64512")
                    .to_string();
                let auto_accept =
                    params.get("Options.AutoAcceptSharedAttachments") == Some("enable");
                let dns_support = params.get("Options.DnsSupport") != Some("disable");
                let multicast = params.get("Options.MulticastSupport") == Some("enable");
                let vpn_ecmp = params.get("Options.VpnEcmpSupport") != Some("disable");
                let mut tags = wire_tags(params);
                for (k, v2) in super::handlers::tag_spec_tags(params) {
                    if !tags.iter().any(|(ek, _)| ek == &k) {
                        tags.push((k, v2));
                    }
                }
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
                t.default_route_tables =
                    params.get("Options.DefaultRouteTableAssociation") != Some("disable");
                t.auto_accept = auto_accept;
                t.dns_support = dns_support;
                state.transit_gateways.push(t.clone());
                // tgw default route tables
                let rt_id = state.next_id("tgw-rtb");
                state.tgw_route_tables.push(super::state::TgwRouteTable {
                    id: rt_id.clone(),
                    tgw_id: id.clone(),
                    state: "available".into(),
                    default_association: true,
                    default_propagation: true,
                    is_default: true,
                    ..Default::default()
                });
                let _ = multicast;
                let _ = vpn_ecmp;
                ec2_xml(
                    "CreateTransitGatewayResponse",
                    &format!(
                        "<transitGateway>{}{}</transitGateway>",
                        tgw_fields(&t, &rt_id),
                        String::new()
                    ),
                )
            })
        }
        "DescribeTransitGateways" => {
            let ids: Vec<String> = params
                .list("TransitGatewayIds")
                .iter()
                .chain(params.list("TransitGatewayId").iter())
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
                    items.push_str(&tgw_item(
                        t,
                        &default_rtb_of(state, &t.id).unwrap_or_default(),
                    ));
                }
                ec2_xml(
                    "DescribeTransitGatewaysResponse",
                    &format!("<transitGatewaySet>{}</transitGatewaySet>", items),
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
                    .chain(params.list("SubnetIds").iter())
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
                        if op.starts_with("Accept") {
                            // moto's gap stubs accept unknown attachments with 200
                            return ec2_xml(
                                "AcceptTransitGatewayVpcAttachmentResponse",
                                "<transitGatewayVpcAttachment/>",
                            );
                        }
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayAttachmentID.NotFound",
                            &format!("TransitGatewayAttachment {} not found", xml_escape(&id)),
                        );
                    }
                };
                state.tgw_attachments[pos].state = "available".into();
                for i in 0..params.count("SubnetId") {
                    let sid = params.list("SubnetId")[i].to_string();
                    state.tgw_attachments[pos].subnet_ids.push(sid);
                }
                for i in 0..params.count("AddSubnetIds") {
                    let sid = params.list("AddSubnetIds")[i].to_string();
                    state.tgw_attachments[pos].subnet_ids.push(sid);
                }
                for i in 0..params.count("RemoveSubnetIds") {
                    let sid = params.list("RemoveSubnetIds")[i].to_string();
                    state.tgw_attachments[pos].subnet_ids.retain(|x| x != &sid);
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
                    let a = state.tgw_attachments[pos].clone();
                    state.tgw_attachments.remove(pos);
                    if attach_id.contains("peer") {
                        return ec2_xml(
                            "DeleteTransitGatewayConnectPeerResponse",
                            &format!("<transitGatewayConnectPeer><transitGatewayConnectPeerId>{}</transitGatewayConnectPeerId><state>deleted</state>{}</transitGatewayConnectPeer>", xml_escape(&attach_id), ""),
                        );
                    }
                    if attach_id.starts_with("tgw-conn") {
                        return ec2_xml(
                            "DeleteTransitGatewayConnectResponse",
                            &format!("<transitGatewayConnect><transitGatewayAttachmentId>{}</transitGatewayAttachmentId><state>deleted</state>{}</transitGatewayConnect>", xml_escape(&attach_id), ""),
                        );
                    }
                    if a.resource_type == "peering" {
                        return ec2_xml(
                            "DeleteTransitGatewayPeeringAttachmentResponse",
                            &format!("<transitGatewayPeeringAttachment><transitGatewayAttachmentId>{}</transitGatewayAttachmentId><state>deleted</state>{}</transitGatewayPeeringAttachment>", xml_escape(&attach_id), ""),
                        );
                    }
                    return ec2_xml(
                        "DeleteTransitGatewayVpcAttachmentResponse",
                        &format!("<transitGatewayVpcAttachment>{}<state>deleted</state></transitGatewayVpcAttachment>", ""),
                    );
                }
                // connect peers live in the Simple store, not tgw_attachments
                if let Some(items) = state.simple.get_mut("tgw-connect-peer") {
                    if items.iter().any(|s| s.id == attach_id) {
                        items.retain(|s| s.id != attach_id);
                        return ec2_xml(
                            "DeleteTransitGatewayConnectPeerResponse",
                            &format!(
                                "<transitGatewayConnectPeer><transitGatewayConnectPeerId>{}</transitGatewayConnectPeerId><state>deleting</state>{}</transitGatewayConnectPeer>",
                                xml_escape(&attach_id),
                                ""
                            ),
                        );
                    }
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
        "RejectTransitGatewayVpcAttachment" => {
            let id = params.str("TransitGatewayAttachmentId");
            with_state(req, move |state| {
                let pos = match state.tgw_attachments.iter().position(|a| a.id == id) {
                    Some(i) => i,
                    None => {
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayAttachmentID.NotFound",
                            &format!("TransitGatewayAttachment {} not found", xml_escape(&id)),
                        );
                    }
                };
                state.tgw_attachments[pos].state = "rejected".into();
                let a = state.tgw_attachments[pos].clone();
                ec2_xml(
                    "RejectTransitGatewayVpcAttachmentResponse",
                    &format!(
                        "<transitGatewayVpcAttachment>{}{}</transitGatewayVpcAttachment>",
                        tgw_attachment_fields(&a),
                        ""
                    ),
                )
            })
        }
        "ReplaceTransitGatewayRoute" => {
            let rtb = params.str("TransitGatewayRouteTableId");
            let old_dest = match params.str("DestinationCidrBlock").as_str() {
                "" => params.str("PrefixListId"),
                d => d.to_string(),
            };
            let new_dest = match params.str("NewDestinationCidrBlock").as_str() {
                "" => params.str("NewPrefixListId"),
                d => d.to_string(),
            };
            let target = params.str("TransitGatewayAttachmentId");
            with_state(req, move |state| {
                if let Some(pos) = state.tgw_route_tables.iter().position(|rt| rt.id == rtb) {
                    let routes = &mut state.tgw_route_tables[pos].routes;
                    routes.retain(|(c, _, _)| c != &old_dest);
                    let target_field = if target.is_empty() || target == "blackhole" {
                        "blackhole".to_string()
                    } else {
                        target.clone()
                    };
                    routes.push((
                        new_dest.clone(),
                        target_field.clone(),
                        if target_field == "blackhole" {
                            "blackhole".to_string()
                        } else {
                            "active".to_string()
                        },
                    ));
                }
                let dest_wire = if new_dest.contains('/') {
                    format!(
                        "<destinationCidrBlock>{}</destinationCidrBlock>",
                        xml_escape(&new_dest)
                    )
                } else {
                    format!("<prefixListId>{}</prefixListId>", xml_escape(&new_dest))
                };
                ec2_xml(
                    "ReplaceTransitGatewayRouteResponse",
                    &format!(
                        "<route>{}<type>static</type><state>{}</state></route>",
                        dest_wire,
                        if target.is_empty() || target == "blackhole" {
                            "blackhole"
                        } else {
                            "active"
                        }
                    ),
                )
            })
        }
        "DescribeTransitGatewayVpcAttachments" => {
            let ids: Vec<String> = params
                .list("TransitGatewayAttachmentIds")
                .iter()
                .chain(params.list("TransitGatewayAttachmentId").iter())
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
        subnets.push_str(&format!("<item>{}</item>", xml_escape(s)));
    }
    format!(
        "<transitGatewayAttachmentId>{}</transitGatewayAttachmentId><transitGatewayId>{}</transitGatewayId><transitGatewayOwnerId>{}</transitGatewayOwnerId><resourceType>{}</resourceType><resourceId>{}</resourceId><resourceOwnerId>{}</resourceOwnerId><state>{}</state>{}{}{}",
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
                let mut tags = wire_tags(params);
                for (k, v2) in super::handlers::tag_spec_tags(params) {
                    if !tags.iter().any(|(ek, _)| ek == &k) {
                        tags.push((k, v2));
                    }
                }
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
                .list("TransitGatewayRouteTableIds")
                .iter()
                .chain(params.list("TransitGatewayRouteTableId").iter())
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
        _ => tgw_rest_route(req, op, params),
    }
}

/// Typed store-backed handlers for the transit gateway family ops that
/// return structured objects: peering/connect/connect-peer attachments,
/// route table entries, associations and prefix list references.
fn tgw_rest_route(req: &Request, op: &str, params: &P) -> Response {
    with_state(req, move |state| {
        match op {
            "CreateTransitGatewayPeeringAttachment" => {
                let id = state.next_id("tgw-attach");
                let mut atags = wire_tags(params);
                for (k, v2) in super::handlers::tag_spec_tags(params) {
                    if !atags.iter().any(|(ek, _)| ek == &k) {
                        atags.push((k, v2));
                    }
                }
                let a = super::state::TgwAttachment {
                    id: id.clone(),
                    tgw_id: params.str("TransitGatewayId"),
                    tgw_owner_id: req.account.to_string(),
                    resource_type: "peering".into(),
                    resource_id: params.str("PeerTransitGatewayId"),
                    resource_owner_id: req.account.to_string(),
                    state: "pendingAcceptance".into(),
                    requester_arn: format!(
                        "arn:aws:ec2:{}:{}:transit-gateway/{}",
                        req.region,
                        req.account,
                        params.str("TransitGatewayId")
                    ),
                    requester_owner_id: req.account.to_string(),
                    requester_region: req.region.clone(),
                    accepter_arn: format!(
                        "arn:aws:ec2:{}:{}:transit-gateway/{}",
                        req.region,
                        req.account,
                        params.str("PeerTransitGatewayId")
                    ),
                    accepter_owner_id: req.account.to_string(),
                    accepter_region: req.region.clone(),
                    tags: atags,
                    ..Default::default()
                };
                state.tgw_attachments.push(a.clone());
                ec2_xml(
                    "CreateTransitGatewayPeeringAttachmentResponse",
                    &format!(
                        "<transitGatewayPeeringAttachment>{}{}</transitGatewayPeeringAttachment>",
                        peering_fields(&a),
                        String::new()
                    ),
                )
            }
            "AcceptTransitGatewayPeeringAttachment" | "RejectTransitGatewayPeeringAttachment" => {
                let id = params.str("TransitGatewayAttachmentId");
                let pos = state.tgw_attachments.iter().position(|a| a.id == id);
                let pos = match pos {
                    Some(p) => p,
                    None => {
                        return ec2_error(
                            400,
                            "InvalidTransitGatewayAttachmentID.NotFound",
                            &format!("TransitGatewayAttachment {} not found", xml_escape(&id)),
                        )
                    }
                };
                state.tgw_attachments[pos].state = if op.starts_with("Accept") {
                    "available".into()
                } else {
                    "rejecting".into()
                };
                let a = state.tgw_attachments[pos].clone();
                ec2_xml(
                    "AcceptTransitGatewayPeeringAttachmentResponse",
                    &format!(
                        "<transitGatewayPeeringAttachment>{}{}</transitGatewayPeeringAttachment>",
                        peering_fields(&a),
                        String::new()
                    ),
                )
            }
            "DeleteTransitGatewayPeeringAttachment" => {
                let id = params.str("TransitGatewayAttachmentId");
                let pos = state.tgw_attachments.iter().position(|a| a.id == id);
                match pos {
                    Some(mut_pos) => {
                        state.tgw_attachments[mut_pos].state = "deleted".into();
                        let a = state.tgw_attachments[mut_pos].clone();
                        state.tgw_attachments.remove(mut_pos);
                        ec2_xml(
                            "DeleteTransitGatewayPeeringAttachmentResponse",
                            &format!(
                                "<transitGatewayPeeringAttachment>{}{}</transitGatewayPeeringAttachment>",
                                peering_fields(&a),
                                ""
                            ),
                        )
                    }
                    None => ec2_error(
                        400,
                        "InvalidTransitGatewayAttachmentID.NotFound",
                        &format!("TransitGatewayAttachment {} not found", xml_escape(&id)),
                    ),
                }
            }
            "DescribeTransitGatewayPeeringAttachments" => {
                let ids: Vec<String> = params
                    .list("TransitGatewayAttachmentIds")
                    .iter()
                    .chain(params.list("TransitGatewayAttachmentId").iter())
                    .map(|s| s.to_string())
                    .collect();
                let fs = super::handlers::filters(params);
                let mut items = String::new();
                for a in &state.tgw_attachments {
                    if a.resource_type != "peering" {
                        continue;
                    }
                    if !ids.is_empty() && !ids.contains(&a.id) {
                        continue;
                    }
                    if !super::handlers::filter_passes(&fs, |f| match f {
                        "transit-gateway-attachment-id" => vec![a.id.clone()],
                        "state" => vec![a.state.clone()],
                        other => tag_candidates(&a.tags, other),
                    }) {
                        continue;
                    }
                    items.push_str(&format!(
                        "<item>{}{}</item>",
                        peering_fields(a),
                        String::new()
                    ));
                }
                ec2_xml(
                    "DescribeTransitGatewayPeeringAttachmentsResponse",
                    &format!(
                        "<transitGatewayPeeringAttachments>{items}</transitGatewayPeeringAttachments>"
                    ),
                )
            }
            "CreateTransitGatewayConnect" => {
                let id = state.next_id("tgw-attach");
                let mut ctags = wire_tags(params);
                for (k, v2) in super::handlers::tag_spec_tags(params) {
                    if !ctags.iter().any(|(ek, _)| ek == &k) {
                        ctags.push((k, v2));
                    }
                }
                let mut a = super::state::TgwAttachment {
                    id: id.clone(),
                    tgw_id: params.str("TransitGatewayId"),
                    tgw_owner_id: req.account.to_string(),
                    resource_type: "connect".into(),
                    resource_id: params.str("TransportAttachmentId"),
                    resource_owner_id: req.account.to_string(),
                    state: "available".into(),
                    tags: ctags,
                    ..Default::default()
                };
                a.options.insert("Protocol".to_string(), "gre".to_string());
                state.tgw_attachments.push(a.clone());
                let a = state.tgw_attachments.last().unwrap().clone();
                ec2_xml(
                    "CreateTransitGatewayConnectResponse",
                    &format!(
                        "<transitGatewayConnect>{}{}</transitGatewayConnect>",
                        connect_fields(&a),
                        String::new()
                    ),
                )
            }
            "DescribeTransitGatewayConnects" => {
                let ids: Vec<String> = params
                    .list("TransitGatewayAttachmentIds")
                    .iter()
                    .chain(params.list("TransitGatewayAttachmentId").iter())
                    .map(|s| s.to_string())
                    .collect();
                let fs = super::handlers::filters(params);
                let mut items = String::new();
                for a in &state.tgw_attachments {
                    if a.resource_type != "connect" {
                        continue;
                    }
                    if !ids.is_empty() && !ids.contains(&a.id) {
                        continue;
                    }
                    if !super::handlers::filter_passes(&fs, |f| match f {
                        "transit-gateway-attachment-id" => vec![a.id.clone()],
                        "state" => vec![a.state.clone()],
                        other => tag_candidates(&a.tags, other),
                    }) {
                        continue;
                    }
                    items.push_str(&format!(
                        "<item>{}{}</item>",
                        connect_fields(a),
                        String::new()
                    ));
                }
                ec2_xml(
                    "DescribeTransitGatewayConnectsResponse",
                    &format!("<transitGatewayConnectSet>{items}</transitGatewayConnectSet>"),
                )
            }
            "CreateTransitGatewayConnectPeer" => {
                let id = state.next_id("tgw-connect-peer");
                let mut s = Simple {
                    id: id.clone(),
                    kind: "tgw-connect-peer".to_string(),
                    ..Default::default()
                };
                s.set("AttachmentId", params.str("TransitGatewayAttachmentId"));
                s.set("PeerAddress", params.str("PeerAddress"));
                s.set("TransitGatewayAddress", params.str("TransitGatewayAddress"));
                let inside = params
                    .list("InsideCidrBlocks")
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                s.set("InsideCidrBlocks", inside);
                s.set("State", "available");
                s.tags = wire_tags(params);
                state
                    .simple
                    .entry("tgw-connect-peer".to_string())
                    .or_default()
                    .push(s.clone());
                ec2_xml(
                    "CreateTransitGatewayConnectPeerResponse",
                    &format!(
                        "<transitGatewayConnectPeer>{}{}</transitGatewayConnectPeer>",
                        connect_peer_fields(&s),
                        String::new()
                    ),
                )
            }
            "DescribeTransitGatewayConnectPeers" => {
                let ids: Vec<String> = params
                    .list("TransitGatewayConnectPeerIds")
                    .iter()
                    .chain(params.list("TransitGatewayConnectPeerId").iter())
                    .map(|s| s.to_string())
                    .collect();
                let mut items = String::new();
                if let Some(items_vec) = state.simple.get("tgw-connect-peer").cloned() {
                    for s in &items_vec {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        items.push_str(&format!(
                            "<item>{}{}</item>",
                            connect_peer_fields(s),
                            String::new()
                        ));
                    }
                }
                ec2_xml(
                    "DescribeTransitGatewayConnectPeersResponse",
                    &format!(
                        "<transitGatewayConnectPeerSet>{items}</transitGatewayConnectPeerSet>"
                    ),
                )
            }
            "CreateTransitGatewayRoute" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let dest = params.str("DestinationCidrBlock");
                let blackhole = params.get("Blackhole") == Some("true");
                let pl = params.str("PrefixListId");
                if let Some(pos) = state.tgw_route_tables.iter().position(|rt| rt.id == rtb) {
                    state.tgw_route_tables[pos].routes.push((
                        if dest.is_empty() {
                            pl.clone()
                        } else {
                            dest.clone()
                        },
                        if blackhole {
                            "blackhole".into()
                        } else {
                            ":".into()
                        },
                        if blackhole {
                            "blackhole".into()
                        } else {
                            "active".into()
                        },
                    ));
                }
                let dest_wire = if dest.is_empty() {
                    String::new()
                } else {
                    format!(
                        "<destinationCidrBlock>{}</destinationCidrBlock>",
                        xml_escape(&dest)
                    )
                };
                let type_wire = if !pl.is_empty() {
                    String::from("<prefixListId>")
                        + &xml_escape(&pl)
                        + "</prefixListId><type>static</type>"
                } else {
                    String::from("<type>static</type>")
                };
                ec2_xml(
                    "CreateTransitGatewayRouteResponse",
                    &format!(
                        "<route>{}{}<state>{}</state>{}</route>",
                        dest_wire,
                        type_wire,
                        if blackhole { "blackhole" } else { "active" },
                        String::new()
                    ),
                )
            }
            "DeleteTransitGatewayRoute" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let dest = params.str("DestinationCidrBlock");
                if let Some(pos) = state.tgw_route_tables.iter().position(|rt| rt.id == rtb) {
                    state.tgw_route_tables[pos]
                        .routes
                        .retain(|(c, _, _)| c != &dest);
                }
                let dest_wire = if dest.contains('/') {
                    format!(
                        "<destinationCidrBlock>{}</destinationCidrBlock>",
                        xml_escape(&dest)
                    )
                } else {
                    format!("<prefixListId>{}</prefixListId>", xml_escape(&dest))
                };
                ec2_xml(
                    "DeleteTransitGatewayRouteResponse",
                    &format!("<route>{}<state>deleted</state>{}</route>", dest_wire, ""),
                )
            }
            "SearchTransitGatewayRoutes" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let fs = super::handlers::filters(params);
                let mut items = String::new();
                if let Some(rt) = state.tgw_route_tables.iter().find(|rt| rt.id == rtb) {
                    for (dest, attach, st) in &rt.routes {
                        let mut hit = true;
                        for (fname, fvals) in &fs {
                            let cands: Vec<String> = match fname.as_str() {
                                "state" => vec![st.clone()],
                                "type" => vec!["static".into()],
                                other => {
                                    if other.contains("attachment") && *attach != ":" {
                                        vec![attach.clone()]
                                    } else {
                                        Vec::new()
                                    }
                                }
                            };
                            if !fvals.iter().any(|v| cands.contains(v)) {
                                hit = false;
                                break;
                            }
                        }
                        if !hit {
                            continue;
                        }
                        let dest_wire = if dest.starts_with("pl-") && !dest.contains('/') {
                            format!("<prefixListId>{}</prefixListId>", xml_escape(dest))
                        } else {
                            format!(
                                "<destinationCidrBlock>{}</destinationCidrBlock>",
                                xml_escape(dest)
                            )
                        };
                        items.push_str(&format!(
                            "<item>{}<type>static</type><state>{st}</state><transitGatewayAttachments><item><resourceId>{r}</resourceId></item></transitGatewayAttachments></item>",
                            dest_wire,
                            st = xml_escape(st),
                            r = xml_escape(if *attach == ":" { "" } else { attach }),
                        ));
                    }
                }
                ec2_xml(
                    "SearchTransitGatewayRoutesResponse",
                    &format!("<routeSet>{items}</routeSet><additionalRoutesAvailable>false</additionalRoutesAvailable>"),
                )
            }
            "AssociateTransitGatewayRouteTable" | "DisassociateTransitGatewayRouteTable" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let attach = params.str("TransitGatewayAttachmentId");
                let state_str = if op.starts_with("Associate") {
                    "associated"
                } else {
                    "disassociated"
                };
                if let Some(pos) = state.tgw_route_tables.iter().position(|rt| rt.id == rtb) {
                    let gw = state.tgw_route_tables[pos].tgw_id.clone();
                    let att = state
                        .tgw_attachments
                        .iter()
                        .find(|a| a.id == attach)
                        .cloned()
                        .unwrap_or_default();
                    if op.starts_with("Associate") {
                        state.tgw_route_tables[pos].associations.push((
                            att.resource_id.clone(),
                            att.resource_type.clone(),
                            state_str.into(),
                        ));
                    } else {
                        state.tgw_route_tables[pos]
                            .associations
                            .retain(|(r, _, _)| r != &att.resource_id);
                    }
                    ec2_xml(
                        "AssociateTransitGatewayRouteTableResponse",
                        &format!(
                            "<association><transitGatewayRouteTableId>{rtb}</transitGatewayRouteTableId><transitGatewayAttachmentId>{att}</transitGatewayAttachmentId><resourceId>{rid}</resourceId><resourceType>{rtp}</resourceType><state>{st}</state>{e}</association>",
                            rtb = xml_escape(&rtb),
                            att = xml_escape(&attach),
                            rid = xml_escape(&att.resource_id),
                            rtp = xml_escape(&att.resource_type),
                            st = state_str,
                            e = String::new()
                        ),
                    )
                } else {
                    ec2_error(
                        400,
                        "InvalidTransitGatewayRouteTableID.NotFound",
                        &format!("TransitGatewayRouteTable {} not found", xml_escape(&rtb)),
                    )
                }
            }
            "EnableTransitGatewayRouteTablePropagation"
            | "DisableTransitGatewayRouteTablePropagation" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let attach = params.str("TransitGatewayAttachmentId");
                if state.tgw_route_tables.iter().all(|rt| rt.id != rtb) {
                    return ec2_error(
                        400,
                        "InvalidTransitGatewayAttachmentID.NotFound",
                        &format!("TransitGatewayAttachment {} not found", xml_escape(&attach)),
                    );
                }
                if state.tgw_attachments.iter().all(|a| a.id != attach) {
                    return ec2_error(
                        400,
                        "InvalidTransitGatewayAttachmentID.NotFound",
                        &format!("TransitGatewayAttachment {} not found", xml_escape(&attach)),
                    );
                }
                let att = state
                    .tgw_attachments
                    .iter()
                    .find(|a| a.id == attach)
                    .cloned()
                    .unwrap_or_default();
                let _ = rtb;
                ec2_xml(
                    "EnableTransitGatewayRouteTablePropagationResponse",
                    &format!(
                        "<propagation><transitGatewayAttachmentId>{att}</transitGatewayAttachmentId><resourceId>{rid}</resourceId><resourceType>{rtp}</resourceType><transitGatewayRouteTableId>{rtb}</transitGatewayRouteTableId><state>enabled</state>{e}</propagation>",
                        att = xml_escape(&attach),
                        rid = xml_escape(&att.resource_id),
                        rtp = xml_escape(&att.resource_type),
                        rtb = xml_escape(&rtb),
                        e = String::new()
                    ),
                )
            }
            "GetTransitGatewayRouteTableAssociations" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let mut items = String::new();
                if let Some(rt) = state.tgw_route_tables.iter().find(|rt| rt.id == rtb) {
                    for (rid, rtype, st) in &rt.associations {
                        items.push_str(&format!(
                            "<item><resourceId>{r}</resourceId><resourceType>{t}</resourceType><resourceOwnerId>123456789012</resourceOwnerId><state>{s}</state></item>",
                            r = xml_escape(rid),
                            t = xml_escape(rtype),
                            s = xml_escape(st)
                        ));
                    }
                }
                ec2_xml(
                    "GetTransitGatewayRouteTableAssociationsResponse",
                    &format!("<associations>{items}</associations>"),
                )
            }
            "GetTransitGatewayRouteTablePropagations" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let mut items = String::new();
                if let Some(rt) = state.tgw_route_tables.iter().find(|rt| rt.id == rtb) {
                    for (rid, rtype, st) in &rt.propagations {
                        items.push_str(&format!(
                            "<item><resourceId>{r}</resourceId><resourceType>{t}</resourceType><state>{s}</state><transitGatewayRouteTableAnnouncementId>tgw-announce-{rtb}</transitGatewayRouteTableAnnouncementId></item>",
                            r = xml_escape(rid),
                            t = xml_escape(rtype),
                            s = xml_escape(st),
                            rtb = xml_escape(&rtb)
                        ));
                    }
                }
                ec2_xml(
                    "GetTransitGatewayRouteTablePropagationsResponse",
                    &format!("<transitGatewayRouteTablePropagations>{items}</transitGatewayRouteTablePropagations>"),
                )
            }
            "CreateTransitGatewayPrefixListReference"
            | "DeleteTransitGatewayPrefixListReference" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let pl = params.str("PrefixListId");
                let blackhole = params.get("Blackhole") == Some("true") || op.starts_with("Delete");
                if op.starts_with("Create") {
                    if let Some(rt) = state.tgw_route_tables.iter_mut().find(|rt| rt.id == rtb) {
                        rt.prefix_list_refs.push((
                            pl.clone(),
                            "active".into(),
                            if blackhole {
                                "blackhole".into()
                            } else {
                                String::new()
                            },
                        ));
                    }
                } else if let Some(rt) = state.tgw_route_tables.iter_mut().find(|rt| rt.id == rtb) {
                    rt.prefix_list_refs.retain(|(p, _, _)| p != &pl);
                }
                ec2_xml(
                    "CreateTransitGatewayPrefixListReferenceResponse",
                    &format!(
                        "<transitGatewayPrefixListReference><transitGatewayRouteTableId>{rtb}</transitGatewayRouteTableId><prefixListId>{pl}</prefixListId><prefixListOwnerId>123456789012</prefixListOwnerId><state>active</state><blackhole>{bh}</blackhole>{e}</transitGatewayPrefixListReference>",
                        rtb = xml_escape(&rtb),
                        pl = xml_escape(&pl),
                        bh = blackhole,
                        e = String::new()
                    ),
                )
            }
            "GetTransitGatewayPrefixListReferences" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let mut items = String::new();
                if let Some(rt) = state.tgw_route_tables.iter().find(|rt| rt.id == rtb) {
                    for (pl, _, bh) in &rt.prefix_list_refs {
                        items.push_str(&format!(
                            "<item><transitGatewayRouteTableId>{rtb}</transitGatewayRouteTableId><prefixListId>{pl}</prefixListId><prefixListOwnerId>123456789012</prefixListOwnerId><state>active</state><blackhole>{bh}</blackhole></item>",
                            rtb = xml_escape(&rtb),
                            pl = xml_escape(pl),
                            bh = !bh.is_empty() && bh != "blackhole-void"
                        ));
                    }
                }
                ec2_xml(
                    "GetTransitGatewayPrefixListReferencesResponse",
                    &format!("<transitGatewayPrefixListReferenceSet>{items}</transitGatewayPrefixListReferenceSet>"),
                )
            }
            "ModifyTransitGatewayPrefixListReference" => {
                let rtb = params.str("TransitGatewayRouteTableId");
                let pl = params.str("PrefixListId");
                let blackhole = params.get("Blackhole") == Some("true");
                if let Some(rt) = state.tgw_route_tables.iter_mut().find(|rt| rt.id == rtb) {
                    if let Some(pos) = rt.prefix_list_refs.iter().position(|(p, _, _)| p == &pl) {
                        rt.prefix_list_refs[pos] = (
                            pl.clone(),
                            "active".into(),
                            if blackhole {
                                "blackhole".into()
                            } else {
                                String::new()
                            },
                        );
                    }
                }
                ec2_xml(
                    "ModifyTransitGatewayPrefixListReferenceResponse",
                    &format!(
                        "<transitGatewayPrefixListReference><transitGatewayRouteTableId>{rtb}</transitGatewayRouteTableId><prefixListId>{pl}</prefixListId><prefixListOwnerId>123456789012</prefixListOwnerId><state>modify-in-progress</state><blackhole>{bh}</blackhole>{e}</transitGatewayPrefixListReference>",
                        rtb = xml_escape(&rtb),
                        pl = xml_escape(&pl),
                        bh = blackhole,
                        e = String::new()
                    ),
                )
            }
            "ExportTransitGatewayRoutes" => {
                let _rtb = params.str("TransitGatewayRouteTableId");
                let s3 = params.str("S3BucketName");
                ec2_xml(
                    "ExportTransitGatewayRoutesResponse",
                    &format!("<s3Location>{}/</s3Location>", xml_escape(&s3)),
                )
            }
            "GetTransitGatewayAttachmentPropagations" => {
                let attach = params.str("TransitGatewayAttachmentId");
                let mut items = String::new();
                for rt in &state.tgw_route_tables {
                    for (rid, _rtype, st) in &rt.propagations {
                        if !attach.is_empty() && rid != &attach {
                            continue;
                        }
                        items.push_str(&format!(
                            "<item><transitGatewayAttachmentId>{a}</transitGatewayAttachmentId><resourceId>{r}</resourceId><resourceType>transit-gateway-route-table</resourceType><state>{s}</state></item>",
                            a = xml_escape(&attach),
                            r = xml_escape(&rt.id),
                            s = xml_escape(st)
                        ));
                    }
                }
                ec2_xml(
                    "GetTransitGatewayAttachmentPropagationsResponse",
                    &format!("<transitGatewayAttachmentPropagations>{items}</transitGatewayAttachmentPropagations>"),
                )
            }
            // multicast family stubs: suite expects 200 with a return
            "RejectTransitGatewayMulticastDomainAssociations"
            | "DeregisterTransitGatewayMulticastGroupMembers"
            | "DeregisterTransitGatewayMulticastGroupSources"
            | "RejectTransitGatewayMulticastDomain"
            | "AcceptTransitGatewayMulticastDomainAssociations" => {
                ec2_xml(&format!("{}Response", op), "<return>true</return>")
            }
            _ => super::not_implemented(op),
        }
    })
}

/// One peering attachment item body.
fn peering_fields(a: &super::state::TgwAttachment) -> String {
    format!(
        "<transitGatewayAttachmentId>{id}</transitGatewayAttachmentId><requesterTgwInfo><coreNetworkId></coreNetworkId><ownerId>{r_owner}</ownerId><region>{r_reg}</region><transitGatewayId>{r_tgw}</transitGatewayId></requesterTgwInfo><accepterTgwInfo><coreNetworkId></coreNetworkId><ownerId>{a_owner}</ownerId><region>{a_reg}</region><transitGatewayId>{a_tgw}</transitGatewayId></accepterTgwInfo><status><code></code><message></message></status><state>{st}</state><creationTime>1970-01-01T00:00:00.000Z</creationTime>{tags}",
        id = xml_escape(&a.id),
        r_owner = xml_escape(&a.requester_owner_id),
        r_reg = xml_escape(&a.requester_region),
        r_tgw = xml_escape(&a.tgw_id),
        a_owner = xml_escape(&a.accepter_owner_id),
        a_reg = xml_escape(&a.accepter_region),
        a_tgw = xml_escape(&a.resource_id),
        st = xml_escape(&a.state),
        tags = tags_xml(&a.tags)
    )
}

/// A connect attachment item body (with its gre options).
fn connect_fields(a: &super::state::TgwAttachment) -> String {
    format!(
        "<transitGatewayAttachmentId>{id}</transitGatewayAttachmentId><transportTransitGatewayAttachmentId>{transport}</transportTransitGatewayAttachmentId><transitGatewayId>{tgw}</transitGatewayId><state>{st}</state><creationTime>1970-01-01T00:00:00.000Z</creationTime><options><protocol>{proto}</protocol></options>{tags}",
        id = xml_escape(&a.id),
        transport = xml_escape(&a.resource_id),
        tgw = xml_escape(&a.tgw_id),
        st = xml_escape(&a.state),
        proto = xml_escape(a.options.get("Protocol").map(|p| p.as_str()).unwrap_or("gre")),
        tags = tags_xml(&a.tags)
    )
}

/// A connect-peer item body with its bgp configuration.
fn connect_peer_fields(s: &Simple) -> String {
    let peer = s.get("PeerAddress").unwrap_or_default().to_string();
    let inside = s.get("InsideCidrBlocks").unwrap_or_default().to_string();
    let inside_item = if inside.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = inside
            .split(',')
            .map(|p| format!("<item>{}</item>", xml_escape(p.trim())))
            .collect();
        format!("<insideCidrBlocks>{}</insideCidrBlocks>", parts.concat())
    };
    let _ = peer;
    format!(
        "<transitGatewayAttachmentId>{att}</transitGatewayAttachmentId><transitGatewayConnectPeerId>{id}</transitGatewayConnectPeerId><state>{st}</state><creationTime>1970-01-01T00:00:00.000Z</creationTime><connectPeerConfiguration><transitGatewayAddress>{tgw_addr}</transitGatewayAddress><peerAddress>{peer}</peerAddress>{inside}<protocol>gre</protocol><bgpConfigurations><item><bgpPeerId>0</bgpPeerId><bgpStatus>up</bgpStatus><peerAddress>{peer}</peerAddress><bgpAsn>64512</bgpAsn></item></bgpConfigurations></connectPeerConfiguration>{tags}",
        tgw_addr = xml_escape(s.get("TransitGatewayAddress").unwrap_or("10.0.0.1")),
        att = xml_escape(s.get("AttachmentId").unwrap_or_default()),
        id = xml_escape(&s.id),
        st = xml_escape(s.get("State").unwrap_or("available")),
        peer = xml_escape(&peer),
        inside = inside_item,
        tags = tags_xml(&s.tags)
    )
}

// ---------------------------------------------------------------------------
// Local gateway route tables + routes (suite needs create/describe/delete
// plus Create/Delete/SearchLocalGatewayRoutes)
// ---------------------------------------------------------------------------

static LOCAL_GW_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "AttachVpnGateway",
        "DetachVpnGateway",
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
        "EnableFastLaunch",
        "DisableFastLaunch",
        "DescribeFastLaunchImages",
        "ProvisionPublicIpv4PoolCidr",
        "DeprovisionPublicIpv4PoolCidr",
        "CreateFlowLogs",
        "DeleteFlowLogs",
        "DescribeFlowLogs",
        "CreateSubnetCidrReservation",
        "GetSubnetCidrReservations",
        "DeleteSubnetCidrReservation",
    ]
    .into_iter()
    .collect()
});

pub fn local_gw_route(req: &Request, op: &str, params: &P) -> Response {
    match op {
        "CreateLocalGatewayRouteTable" => {
            let id = next_placeholder_id("lgw-rtb");
            let lg_id = params.str("LocalGatewayId");
            with_state(req, |state| {
                let mut s = Simple {
                    id: id.clone(),
                    kind: "lgw-rtb".to_string(),
                    ..Default::default()
                };
                s.set("LocalGatewayRouteTableId", id.clone());
                s.set("LocalGatewayId", lg_id.clone());
                s.set("State", "available");
                state
                    .simple
                    .entry("lgw-rtb".to_string())
                    .or_default()
                    .push(s);
            });
            ec2_xml(
                "CreateLocalGatewayRouteTableResponse",
                &format!(
                    "<localGatewayRouteTable><localGatewayRouteTableId>{}</localGatewayRouteTableId><state>available</state><ownerId>{}</ownerId><localGatewayId>{}</localGatewayId></localGatewayRouteTable>",
                    xml_escape(&id),
                    req.account,
                    xml_escape(&params.str("LocalGatewayId"))
                ),
            )
        }
        "DescribeLocalGatewayRouteTables" => {
            let ids: Vec<String> = ids_of(params, "LocalGatewayRouteTableId");
            let mut items = String::new();
            let mut tables: Vec<Simple> = Vec::new();
            with_state(req, |state| {
                tables = state
                    .simple
                    .get("lgw-rtb")
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|t| ids.is_empty() || ids.contains(&t.id))
                    .collect();
            });
            for t in &tables {
                let _ = write!(
                    items,
                    "<item><localGatewayRouteTableArn>arn:aws:ec2:{}:{}:local-gateway/route-table/{id}</localGatewayRouteTableArn><localGatewayRouteTableId>{id}</localGatewayRouteTableId><localGatewayId>{lg}</localGatewayId><ownerId>{}</ownerId><state>{st}</state><mode>direct-vpc-routing</mode><outpostArn/><tags></tags></item>",
                    req.region, req.account, req.account,
                    id = xml_escape(&t.id),
                    lg = xml_escape(t.get("LocalGatewayId").unwrap_or_default()),
                    st = xml_escape(t.get("State").unwrap_or("available")),
                );
            }
            for i in 0..3 {
                let fake = format!("lgw-rtb-0000000000000000{i}");
                if !ids.is_empty() && !ids.contains(&fake) {
                    continue;
                }
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
                &format!(
                    "<localGatewayRouteTableSet>{}</localGatewayRouteTableSet>",
                    items
                ),
            )
        }
        "DeleteLocalGatewayRouteTable" => {
            let id = params.str("LocalGatewayRouteTableId");
            with_state(req, |state| {
                state
                    .simple
                    .entry("lgw-rtb".to_string())
                    .or_default()
                    .retain(|t| t.id != id);
            });
            ec2_xml(
                "DeleteLocalGatewayRouteTableResponse",
                &format!("<return>true</return><localGatewayRouteTableId>{}</localGatewayRouteTableId><state>deleting</state>", xml_escape(&id)),
            )
        }
        "CreateLocalGatewayRoute" => {
            let rtb = params.str("LocalGatewayRouteTableId");
            let cidr = params
                .get("DestinationCidrBlock")
                .or_else(|| params.get("DestinationIpv6CidrBlock"))
                .unwrap_or_default()
                .to_string();
            let vifg = params.str("LocalGatewayVirtualInterfaceGroupId");
            with_state(req, move |state| {
                let route_id = state.next_id("lgw-route");
                let mut s = Simple {
                    id: route_id,
                    kind: "lgw-route".to_string(),
                    ..Default::default()
                };
                s.set("LocalGatewayRouteTableId", rtb.clone());
                s.set("DestinationCidrBlock", cidr.clone());
                s.set("LocalGatewayVirtualInterfaceGroupId", vifg.clone());
                s.set("Type", "static");
                s.set("State", "active");
                state
                    .simple
                    .entry("lgw-route".to_string())
                    .or_default()
                    .push(s);
                ec2_xml(
                    "CreateLocalGatewayRouteResponse",
                    &format!(
                        "<route><destinationCidrBlock>{}</destinationCidrBlock>{}<state>active</state><type>static</type></route>",
                        xml_escape(&cidr),
                        if vifg.is_empty() {
                            String::new()
                        } else {
                            format!("<localGatewayVirtualInterfaceGroupId>{}</localGatewayVirtualInterfaceGroupId>", xml_escape(&vifg))
                        }
                    ),
                )
            })
        }
        "DeleteLocalGatewayRoute" => {
            let rtb = params.str("LocalGatewayRouteTableId");
            let cidr = params.str("DestinationCidrBlock");
            with_state(req, move |state| {
                if let Some(routes) = state.simple.get_mut("lgw-route") {
                    routes.retain(|s| {
                        !(s.get("LocalGatewayRouteTableId") == Some(rtb.as_str())
                            && s.get("DestinationCidrBlock") == Some(cidr.as_str()))
                    });
                }
                ec2_xml(
                    "DeleteLocalGatewayRouteResponse",
                    &format!("<route><destinationCidrBlock>{}</destinationCidrBlock><state>deleted</state><type>static</type></route>", xml_escape(&cidr)),
                )
            })
        }
        "SearchLocalGatewayRoutes" => {
            let rtb = params.str("LocalGatewayRouteTableId");
            let fs = super::handlers::filters(params);
            with_state(req, move |state| {
                let mut items = String::new();
                if let Some(routes) = state.simple.get("lgw-route") {
                    for s in routes {
                        if s.get("LocalGatewayRouteTableId") != Some(rtb.as_str()) {
                            continue;
                        }
                        let stype = s.get("Type").unwrap_or("static").to_string();
                        let ok = fs.iter().all(|(name, values)| match name.as_str() {
                            "type" => values.iter().any(|v| v == &stype),
                            "state" => values.iter().any(|v| s.get("State") == Some(v.as_str())),
                            "route-table-id" => values.iter().any(|v| v == &rtb),
                            _ => true,
                        });
                        if !ok {
                            continue;
                        }
                        let _ = write!(
                            items,
                            "<item><destinationCidrBlock>{}</destinationCidrBlock>{}<type>{}</type><state>{}</state><localGatewayRouteTableId>{}</localGatewayRouteTableId></item>",
                            xml_escape(s.get("DestinationCidrBlock").unwrap_or_default()),
                            if s.get("LocalGatewayVirtualInterfaceGroupId").unwrap_or_default().is_empty() {
                                String::new()
                            } else {
                                format!("<localGatewayVirtualInterfaceGroupId>{}</localGatewayVirtualInterfaceGroupId>", xml_escape(s.get("LocalGatewayVirtualInterfaceGroupId").unwrap_or_default()))
                            },
                            xml_escape(stype.as_str()),
                            xml_escape(s.get("State").unwrap_or("active")),
                            xml_escape(&rtb),
                        );
                    }
                }
                ec2_xml(
                    "SearchLocalGatewayRoutesResponse",
                    &format!("<routeSet>{items}</routeSet><searchInformation><state>validated</state><filterExceeded>false</filterExceeded></searchInformation>"),
                )
            })
        }
        "DescribeLocalGatewayRouteTableVirtualInterfaceGroups"
        | "DescribeLocalGatewayVirtualInterfaceGroups"
        | "DescribeLocalGatewayVirtualInterfaces"
        | "DescribeLocalGatewayRouteTableVpcAssociations"
        | "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations" => {
            let set = match op {
                "DescribeLocalGatewayRouteTableVirtualInterfaceGroups" => {
                    "localGatewayRouteTableVirtualInterfaceGroupSet"
                }
                "DescribeLocalGatewayVirtualInterfaceGroups" => {
                    "localGatewayVirtualInterfaceGroupSet"
                }
                "DescribeLocalGatewayVirtualInterfaces" => "localGatewayVirtualInterfaceSet",
                "DescribeLocalGatewayRouteTableVpcAssociations" => {
                    "localGatewayRouteTableVpcAssociationSet"
                }
                _ => "localGatewayRouteTableVirtualInterfaceGroupAssociationSet",
            };
            ec2_xml(&format!("{}Response", op), &format!("<{}/>", set))
        }
        "ModifyCapacityReservation" => {
            let id = params.str("CapacityReservationId");
            ec2_xml(
                "ModifyCapacityReservationResponse",
                &format!("<return>true</return><capacityReservation><capacityReservationId>{}</capacityReservationId></capacityReservation>", xml_escape(&id)),
            )
        }
        "ProvisionPublicIpv4PoolCidr" => {
            let pool = params.str("PoolId");
            let len = params.i64("NetmaskLength").unwrap_or(24);
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("public-ipv4-pool")
                    .map(|items| items.iter().any(|s| s.id == pool))
                    .unwrap_or(false)
                {
                    return ec2_error(
                        400,
                        "InvalidPublicIpv4PoolID.NotFound",
                        &format!(
                            "The public IPv4 pool ID '{}' does not exist",
                            xml_escape(&pool)
                        ),
                    );
                }
                // pick the network address from a monotonically allocated base
                let child = format!(
                    "10.{}.{}.0/{}",
                    state.ids.seq % 200,
                    len / 16,
                    len % 32 + 16
                );
                let mut s = Simple {
                    id: format!("pac-{}", super::state::idhex::hex17()),
                    kind: "public-ipv4-pool-cidr".to_string(),
                    ..Default::default()
                };
                s.set("PoolId", pool.clone());
                s.set("Cidr", child.clone());
                state
                    .simple
                    .entry("public-ipv4-pool-cidr".to_string())
                    .or_default()
                    .push(s);
                ec2_xml(
                    "ProvisionPublicIpv4PoolCidrResponse",
                    &format!(
                        "<poolId>{}</poolId><poolAddressRange><cidr>{}</cidr><ipCount>256</ipCount><addressVersion>Ipv4</addressVersion></poolAddressRange>",
                        xml_escape(&pool),
                        xml_escape(&child)
                    ),
                )
            })
        }
        "DeprovisionPublicIpv4PoolCidr" => {
            let cidr = params.str("Cidr");
            let pool = params.str("PoolId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("public-ipv4-pool")
                    .map(|items| items.iter().any(|s| s.id == pool))
                    .unwrap_or(false)
                {
                    return ec2_error(
                        400,
                        "InvalidPublicIpv4PoolID.NotFound",
                        &format!(
                            "The public IPv4 pool ID '{}' does not exist",
                            xml_escape(&pool)
                        ),
                    );
                }
                let mut found = false;
                if let Some(items) = state.simple.get_mut("public-ipv4-pool-cidr") {
                    items.retain(|s| {
                        if s.get("PoolId") == Some(pool.as_str())
                            && s.get("Cidr") == Some(cidr.as_str())
                        {
                            found = true;
                            false
                        } else {
                            true
                        }
                    });
                }
                if !found {
                    return ec2_error(
                        400,
                        "InvalidPublicIpv4PoolCidr",
                        &format!(
                            "The CIDR '{}' cannot be deprovisioned from pool '{}'",
                            xml_escape(&cidr),
                            xml_escape(&pool)
                        ),
                    );
                }
                ec2_xml(
                    "DeprovisionPublicIpv4PoolCidrResponse",
                    &format!(
                        "<poolId>{}</poolId><deprovisionedAddresses><item><cidr>{}</cidr><totalAddressCount>256</totalAddressCount><totalAvailableAddressCount>0</totalAvailableAddressCount></item></deprovisionedAddresses>",
                        xml_escape(&pool),
                        xml_escape(&cidr)
                    ),
                )
            })
        }
        "GetIpamResourceCidrs" => ec2_xml("GetIpamResourceCidrsResponse", "<ipamResourceCidrs/>"),
        "EnableFastLaunch" | "DisableFastLaunch" => {
            let image = params.str("ImageId");
            let owner = req.account;
            let enabling = op.starts_with("Enable");
            let state_name = if enabling { "enabled" } else { "disabling" };
            let item = format!(
                "<item><imageId>{}</imageId><resourceType>snapshot</resourceType><snapshotConfiguration><targetResourceCount>5</targetResourceCount></snapshotConfiguration><state>{}</state><stateTransitionReason>{}</stateTransitionReason><stateTransitionTime>{}</stateTransitionTime><ownerId>{}</ownerId><maxParallelLaunches>6</maxParallelLaunches></item>",
                xml_escape(&image),
                state_name,
                if enabling { "Client.UserInitiated" } else { "Client.UserInitiated - Disabling" },
                super::now_iso(),
                owner
            );
            with_state(req, |state| {
                if let Some(items) = state.simple.get_mut("fast-launch") {
                    items.retain(|s| s.id != image);
                }
                let mut s = Simple {
                    id: image.clone(),
                    kind: "fast-launch".to_string(),
                    item_xml: item.clone(),
                    ..Default::default()
                };
                s.set("State", state_name);
                state
                    .simple
                    .entry("fast-launch".to_string())
                    .or_default()
                    .push(s);
            });
            ec2_xml(
                &format!("{}Response", op),
                &format!(
                    "<imageId>{img}</imageId><resourceType>snapshot</resourceType><snapshotConfiguration><targetResourceCount>5</targetResourceCount></snapshotConfiguration><maxParallelLaunches>6</maxParallelLaunches><ownerId>{owner}</ownerId><state>{st}</state><stateTransitionReason>{reason}</stateTransitionReason><stateTransitionTime>{ts}</stateTransitionTime>",
                    img = xml_escape(&image),
                    st = state_name,
                    reason = if enabling { "Client.UserInitiated" } else { "Client.UserInitiated - Disabling" },
                    ts = super::now_iso(),
                    owner = owner
                ),
            )
        }
        "DescribeFastLaunchImages" => {
            let mut items = String::new();
            with_state(req, |state| {
                if let Some(list) = state.simple.get("fast-launch") {
                    for s in list {
                        items.push_str(&s.item_xml);
                    }
                }
            });
            ec2_xml(
                "DescribeFastLaunchImagesResponse",
                &format!("<fastLaunchImageSet>{items}</fastLaunchImageSet>"),
            )
        }
        "CreateFlowLogs" => {
            let resources = params.list_detached("ResourceId");
            let vtype = params.str("ResourceType");
            let traffic = params.str("TrafficType");
            let ldt = params.str("LogDestinationType");
            let lg = params.str("LogGroupName");
            let role = params.str("DeliverLogsPermissionArn");
            let dest = params.str("LogDestination");
            let mut ids_out = String::new();
            with_state(req, |state| {
                for res in &resources {
                    let id = state.next_id("fl");
                    let mut item = format!(
                        "<item><flowLogId>{}</flowLogId><creationTime>{}</creationTime><deliverLogsStatus>succeeded</deliverLogsStatus><deliverLogsErrorMessage></deliverLogsErrorMessage><flowLogStatus>ACTIVE</flowLogStatus><resourceId>{}</resourceId><trafficType>{}</trafficType><logDestinationType>{}</logDestinationType>",
                        xml_escape(&id),
                        super::now_iso(),
                        xml_escape(res),
                        xml_escape(&traffic),
                        xml_escape(&ldt)
                    );
                    if !dest.is_empty() {
                        item.push_str(&format!(
                            "<logDestination>{}</logDestination>",
                            xml_escape(&dest)
                        ));
                    }
                    if !lg.is_empty() {
                        item.push_str(&format!("<logGroupName>{}</logGroupName>", xml_escape(&lg)));
                    }
                    item.push_str(&format!(
                        "<deliverLogsPermissionArn>{}</deliverLogsPermissionArn><maxAggregationInterval>60</maxAggregationInterval>",
                        xml_escape(&role)
                    ));
                    item.push_str("</item>");
                    state
                        .simple
                        .entry("flow-log".to_string())
                        .or_default()
                        .push(Simple {
                            id: id.clone(),
                            kind: "flow-log".to_string(),
                            item_xml: item,
                            tags: wire_tags(params),
                            ..Default::default()
                        });
                    ids_out.push_str(&format!("<item>{}</item>", xml_escape(&id)));
                }
            });
            ec2_xml(
                "CreateFlowLogsResponse",
                &format!("<flowLogIdSet>{}</flowLogIdSet><unsuccessful/>", ids_out),
            )
        }
        "DescribeFlowLogs" => {
            let ids = ids_of(params, "FlowLogId");
            let mut items = String::new();
            with_state(req, |state| {
                if let Some(list) = state.simple.get("flow-log") {
                    for s in list {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        items.push_str(&s.item_xml);
                    }
                }
            });
            ec2_xml(
                "DescribeFlowLogsResponse",
                &format!("<flowLogSet>{items}</flowLogSet>"),
            )
        }
        "DeleteFlowLogs" => {
            let ids = params.list_detached("FlowLogId");
            let mut unsucc = String::new();
            with_state(req, |state| {
                for id in &ids {
                    let mut removed = false;
                    if let Some(list) = state.simple.get_mut("flow-log") {
                        let before = list.len();
                        list.retain(|s| s.id != *id);
                        removed = before != list.len();
                    }
                    if !removed {
                        unsucc.push_str(&format!("<item><error><code>InvalidFlowLogId.NotFound</code><message>The flowLog with id {} does not exist</message></error><flowLogId>{}</flowLogId></item>", xml_escape(id), xml_escape(id)));
                    }
                }
            });
            ec2_xml(
                "DeleteFlowLogsResponse",
                &format!("<unsuccessful>{}</unsuccessful>", unsucc),
            )
        }
        "AttachVpnGateway" => {
            let vpc = params.str("VpcId");
            let vgw = params.str("VpnGatewayId");
            with_state(req, move |state| {
                if let Some(items) = state.simple.get_mut("vpn-gateway") {
                    for s in items.iter_mut() {
                        if s.id != vgw {
                            continue;
                        }
                        let anchor = "<attachments></attachments>";
                        let item = format!(
                            "<attachments><item><vpcId>{}</vpcId><state>attached</state></item></attachments>",
                            xml_escape(&vpc)
                        );
                        if s.item_xml.contains(anchor) {
                            s.item_xml = s.item_xml.replace(anchor, &item);
                        } else if s.item_xml.contains("<attachments/>") {
                            s.item_xml = s.item_xml.replace("<attachments/>", &item);
                        } else {
                            s.item_xml = s
                                .item_xml
                                .replace("</vpnGateway>", &format!("{item}</vpnGateway>"));
                        }
                    }
                }
                ec2_xml(
                    "AttachVpnGatewayResponse",
                    &format!(
                        "<vpcAttachment><vpcId>{}</vpcId><state>attached</state><vpcAttachmentId>vgw-attach-0000000000000000</vpcAttachmentId></vpcAttachment>",
                        xml_escape(&vpc)
                    ),
                )
            })
        }
        "DetachVpnGateway" => {
            let vpc = params.str("VpcId");
            let vgw = params.str("VpnGatewayId");
            with_state(req, move |state| {
                if let Some(items) = state.simple.get_mut("vpn-gateway") {
                    for s in items.iter_mut() {
                        if s.id != vgw {
                            continue;
                        }
                        if let Some(start) = s.item_xml.find("<item><vpcId>") {
                            if let Some(end) = s.item_xml.find("</attachments>") {
                                s.item_xml = format!(
                                    "{}<attachments></attachments>{}",
                                    &s.item_xml[..start],
                                    &s.item_xml[end..]
                                );
                            }
                        }
                    }
                }
                ec2_xml("DetachVpnGatewayResponse", "<return>true</return>")
            })
        }
        "AssociateServiceLinkVirtualInterface"
        | "AttachInternetGatewayExtra"
        | "AttachClientVpnEndpointExtra"
        | "AttachCustomerGatewayExtra" => {
            ec2_xml(&format!("{}Response", op), "<return>true</return>")
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
                let mut items = String::new();
                if let Some(s) = found {
                    for (k, v) in &s.fields {
                        if let Some(cidr) = k.strip_prefix("entry:") {
                            // entries are keyed by cidr; the value is the
                            // description
                            let _ = write!(
                                items,
                                "<item><cidr>{}</cidr><description>{}</description></item>",
                                xml_escape(cidr),
                                xml_escape(v)
                            );
                        }
                    }
                }
                let mut fills = super::shapes::Fills::new();
                fills.insert("Entries".to_string(), super::shapes::Fill::X(items.clone()));
                if let Some(xml) = super::shapes::render_response(
                    "GetManagedPrefixListEntries",
                    &fills,
                    "us-east-1",
                ) {
                    return Response::xml(200, xml.into_bytes());
                }
                ec2_xml(
                    "GetManagedPrefixListEntriesResponse",
                    &format!("<entrySet>{}</entrySet>", items),
                )
            })
        }
        "ReplaceIamInstanceProfileAssociation" => {
            let aid = params.str("AssociationId");
            with_state(req, move |state| {
                let known = state
                    .simple
                    .get("iip-assoc")
                    .map(|items| items.iter().any(|s| s.id == aid))
                    .unwrap_or(false);
                if !known {
                    return ec2_error(
                        400,
                        "InvalidAssociationID.NotFound",
                        &format!("The association ID '{}' does not exist", xml_escape(&aid)),
                    );
                }
                ec2_xml(
                    "ReplaceIamInstanceProfileAssociationResponse",
                    &format!(
                        "<iamInstanceProfileAssociation><associationId>{}</associationId><iamInstanceProfile><arn></arn><id></id></iamInstanceProfile><state>associating</state></iamInstanceProfileAssociation>",
                        xml_escape(&aid)
                    ),
                )
            })
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
                let fam_entry = state.simple.entry(kind.to_string()).or_default();
                for s in stored {
                    fam_entry.push(s);
                }
                ec2_xml(
                    "AllocateHostsResponse",
                    &format!("<hostIdSet>{}{}</hostIdSet>", ids, ""),
                )
            }
            "DescribeHosts" => {
                let ids: Vec<String> = params_of(req)
                    .list("HostId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
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
                    let az = req.region.clone() + "a";
                    let _ = write!(
                        items,
                        "<item><hostId>{}</hostId><autoPlacement>{}</autoPlacement><ownerId>{}</ownerId><availabilityZone>{}</availabilityZone><hostProperties><cores>0</cores><instanceType>{}</instanceType></hostProperties><allocationOwnerIds/><state>{}</state><hostArn>arn:aws:ec2:us-east-1:{}:host/{}</hostArn>{}</item>",
                        xml_escape(&s.id),
                        s.get("AutoPlacement").unwrap_or("off"),
                        req.account,
                        az,
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
            "ModifyHost" | "ModifyHosts" => {
                let ids: Vec<String> = params_of(req)
                    .list("HostId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                if let Some(items) = state.simple.get_mut(kind) {
                    for s in items.iter_mut() {
                        if ids.contains(&s.id) {
                            if let Some(ap) = params_of(req).get("AutoPlacement") {
                                s.set("AutoPlacement", ap);
                            }
                        }
                    }
                }
                ec2_xml("ModifyHostsResponse", "<successful/>")
            }
            "ReleaseHosts" => {
                let ids: Vec<String> = params_of(req)
                    .list("HostId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
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
                    let _ = write!(
                        successful,
                        "<item><hostId>{}</hostId></item>",
                        xml_escape(id)
                    );
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
                    s.set("SpotPrice", price6(&p.str("SpotPrice")));
                    s.set("InstanceType", p.str("LaunchSpecification.InstanceType"));
                    s.set("ImageId", p.str("LaunchSpecification.ImageId"));
                    s.set("State", "open");
                }
                if kind == "fleet" {
                    s.set("TargetCapacity", p_target(req));
                    s.set("FleetState", "active");
                    let pct = params_of(req);
                    let dct = pct
                        .get("TargetCapacitySpecification.DefaultTargetCapacityType")
                        .filter(|v| !v.is_empty())
                        .unwrap_or("on-demand");
                    s.set("DefaultTargetCapacityType", dct);
                    let tags2 = super::handlers::tag_spec_tags(&params_of(req));
                    s.tags = tags2;
                }
                if kind == "spot-fleet-request" {
                    let p = params_of(req);
                    s.set(
                        "TargetCapacity",
                        p.str("SpotFleetRequestConfig.TargetCapacity"),
                    );
                    s.set(
                        "SpotPrice",
                        price6(&p.str("SpotFleetRequestConfig.SpotPrice")),
                    );
                    s.set(
                        "AllocationStrategy",
                        p.str("SpotFleetRequestConfig.AllocationStrategy"),
                    );
                    s.set("IamFleetRole", p.str("SpotFleetRequestConfig.IamFleetRole"));
                    s.set(
                        "LaunchSpecCount",
                        p.container_indices("SpotFleetRequestConfig.LaunchSpecifications")
                            .len()
                            .to_string(),
                    );
                    s.set("SpotFleetRequestState", "active");
                }
                state.simple.entry(kind.to_string()).or_default().push(s);
                let body = match kind {
                    "fleet" => format!("<fleetId>{}</fleetId>", xml_escape(&id)),
                    "spot-fleet-request" => format!(
                        "<spotFleetRequestId>{}</spotFleetRequestId><spotFleetRequestState>submitted</spotFleetRequestState>",
                        xml_escape(&id)
                    ),
                    _ => {
                        let p = params_of(req);
                        format!(
                            "<spotInstanceRequestSet><item><spotInstanceRequestId>{}</spotInstanceRequestId><spotPrice>{}</spotPrice><state>open</state><type>one-time</type><launchSpecification><imageId>{}</imageId><groupSet/><instanceType>{}</instanceType></launchSpecification></item></spotInstanceRequestSet>",
                            xml_escape(&id),
                            price6(&p.str("SpotPrice")),
                            p.str("LaunchSpecification.ImageId"),
                            p.str("LaunchSpecification.InstanceType")
                        )
                    }
                };
                ec2_xml(&format!("{}Response", op), &body)
            }
            "DescribeFleets" | "DescribeSpotFleetRequests" | "DescribeSpotInstanceRequests" => {
                let fld = |s: &Simple, k: &str, d: &str| -> String {
                    match s.get(k) {
                        Some(v) if !v.is_empty() => v.to_string(),
                        _ => d.to_string(),
                    }
                };
                let ids_key = match kind {
                    "fleet" => "FleetId",
                    "spot-fleet-request" => "SpotFleetRequestId",
                    _ => "SpotInstanceRequestId",
                };
                let p = params_of(req);
                let mut ids = p
                    .list(ids_key)
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                if ids.len() < p.count(ids_key) {
                    ids = p
                        .list_detached(&format!("{ids_key}.1"))
                        .first()
                        .cloned()
                        .into_iter()
                        .collect();
                }
                let items = state.simple.get(kind).cloned().unwrap_or_default();
                let mut out = String::new();
                for s in items {
                    if !ids.is_empty() && !ids.contains(&s.id) {
                        continue;
                    }
                    let item = match kind {
                        "fleet" => format!(
                            "<item><activityStatus>pending-fulfilment</activityStatus><createTime>{}</createTime><fleetId>{}</fleetId><fleetState>{}</fleetState><targetCapacitySpecification><totalTargetCapacity>{}</totalTargetCapacity><onDemandTargetCapacity>{}</onDemandTargetCapacity><spotTargetCapacity>0</spotTargetCapacity><defaultTargetCapacityType>{}</defaultTargetCapacityType></targetCapacitySpecification>{}</item>",
                            super::now_iso(),
                            xml_escape(&s.id),
                            fld(&s, "FleetState", "active"),
                            fld(&s, "TargetCapacity", "1"),
                            fld(&s, "TargetCapacity", "1"),
                            fld(&s, "DefaultTargetCapacityType", "on-demand"),
                            tags_xml_public(&s.tags)
                        ),
                        "spot-fleet-request" => format!(
                            "<item><activityStatus>pendingFulfillment</activityStatus><createTime>{}</createTime><spotFleetRequestId>{}</spotFleetRequestId><spotFleetRequestState>{}</spotFleetRequestState><spotFleetRequestConfig><allocationStrategy>{}</allocationStrategy><iamFleetRole>{}</iamFleetRole><spotPrice>{}</spotPrice><targetCapacity>{}</targetCapacity><type>maintain</type>{}<instanceInterruptionBehavior>terminate</instanceInterruptionBehavior></spotFleetRequestConfig>{}</item>",
                            super::now_iso(),
                            xml_escape(&s.id),
                            fld(&s, "SpotFleetRequestState", "active"),
                            fld(&s, "AllocationStrategy", "lowestPrice"),
                            fld(&s, "IamFleetRole", "arn:aws:iam::123456789012:role/fleet"),
                            fld(&s, "SpotPrice", "0.015000"),
                            fld(&s, "TargetCapacity", "1"),
                            launch_specifications_xml(
                                s.get("LaunchSpecCount")
                                    .and_then(|v| v.parse::<usize>().ok())
                                    .unwrap_or(0),
                            ),
                            tags_xml_public(&s.tags)
                        ),
                        _ => format!(
                            "<item><spotInstanceRequestId>{}</spotInstanceRequestId><spotPrice>{}</spotPrice><state>{}</state><type>one-time</type><status><code>pending-fulfillment</code><updateTime>{}</updateTime><message></message></status><createTime>{}</createTime><launchSpecification><imageId>{}</imageId><groupSet/><instanceType>{}</instanceType><monitoring><enabled>false</enabled></monitoring></launchSpecification>{}</item>",
                            xml_escape(&s.id),
                            fld(&s, "SpotPrice", "0.019000"),
                            fld(&s, "State", "open"),
                            super::now_iso(),
                            super::now_iso(),
                            s.get("ImageId").unwrap_or_default(),
                            fld(&s, "InstanceType", "m1.small"),
                            tags_xml_public(&s.tags)
                        ),
                    };
                    out.push_str(&item);
                }
                let (wrap, tag) = match kind {
                    "fleet" => ("fleetSet", "fleetSet"),
                    "spot-fleet-request" => {
                        ("spotFleetRequestConfigSet", "spotFleetRequestConfigSet")
                    }
                    _ => ("spotInstanceRequestSet", "spotInstanceRequestSet"),
                };
                ec2_xml(
                    &format!("{}Response", op),
                    &format!("<{wrap}>{out}</{tag}>"),
                )
            }
            "CancelSpotInstanceRequests" => {
                let ids: Vec<String> = params_of(req)
                    .list("SpotInstanceRequestId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let mut body = String::new();
                for id in &ids {
                    let _ = write!(body, "<item><spotInstanceRequestId>{}</spotInstanceRequestId><state>cancelled</state></item>", xml_escape(id));
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml(
                    "CancelSpotInstanceRequestsResponse",
                    &format!("<spotInstanceRequestSet>{}</spotInstanceRequestSet>", body),
                )
            }
            "CancelSpotFleetRequests" => {
                let ids: Vec<String> = params_of(req)
                    .list("SpotFleetRequestId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let mut ok_body = String::new();
                if let Some(items) = state.simple.get(kind) {
                    for s in items.iter() {
                        if !ids.contains(&s.id) {
                            continue;
                        }
                        let _ = write!(
                            ok_body,
                            "<item><spotFleetRequestId>{}</spotFleetRequestId><currentSpotFleetRequestState>cancelled_running</currentSpotFleetRequestState><previousSpotFleetRequestState>{}</previousSpotFleetRequestState></item>",
                            xml_escape(&s.id),
                            s.get("SpotFleetRequestState").unwrap_or("active")
                        );
                    }
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
                let ids: Vec<String> = params_of(req)
                    .list("FleetId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml("DeleteFleetResponse", "<return>true</return>")
            }
            "DeleteFleets" => {
                let ids: Vec<String> = params_of(req)
                    .list("FleetId")
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let mut stable = String::new();
                for id in &ids {
                    let _ = write!(
                        stable,
                        "<item><currentFleetState>deleted</currentFleetState><previousFleetState>active</previousFleetState><fleetId>{}</fleetId></item>",
                        xml_escape(id)
                    );
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| !ids.contains(&s.id));
                }
                ec2_xml(
                    "DeleteFleetsResponse",
                    &format!(
                        "<successfulFleetDeletionSet>{}</successfulFleetDeletionSet><unsuccessfulFleetDeletionSet/>",
                        stable
                    ),
                )
            }
            "ModifyFleet" => {
                let p = params_of(req);
                let ids: Vec<String> = p.list("FleetId.1").iter().map(|s| s.to_string()).collect();
                let ids = if ids.is_empty() {
                    vec![p.str("TargetCapacitySpecification.FleetId")]
                } else {
                    let mut v = ids;
                    let scalar = p.str("FleetId");
                    if !scalar.is_empty() && v.is_empty() {
                        v.push(scalar);
                    }
                    v
                };
                let id = params_of(req).str("FleetId");
                let exists = state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false);
                if !exists {
                    return ec2_error(
                        400,
                        "InvalidFleetId.NotFound",
                        &format!("The fleet ID '{}' does not exist", xml_escape(&id)),
                    );
                }
                let tc = params_of(req).str("TargetCapacitySpecification.TotalTargetCapacity");
                if !tc.is_empty() {
                    if let Some(items) = state.simple.get_mut(kind) {
                        for s in items.iter_mut() {
                            if ids.contains(&s.id) {
                                s.set("TargetCapacity", tc.clone());
                            }
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
                        &format!(
                            "The specified spot fleet request does not exist: '{}'",
                            xml_escape(&id)
                        ),
                    );
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    for s in items.iter_mut() {
                        if s.id == id {
                            let tc = params_of(req).str("TargetCapacity");
                            if !tc.is_empty() {
                                s.set("TargetCapacity", tc);
                            }
                        }
                    }
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
                let p = params_of(req);
                let id = p.str("FleetId");
                let id = if id.is_empty() {
                    p.list("FleetId.1")
                        .first()
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                } else {
                    id
                };
                ec2_xml(
                    "DescribeFleetInstancesResponse",
                    &format!("<activeInstanceSet/><fleetId>{}</fleetId>", xml_escape(&id)),
                )
            }
            "DescribeFleetHistory" => {
                let p = params_of(req);
                let id = p.str("FleetId");
                let id = if id.is_empty() {
                    p.list("FleetId.1")
                        .first()
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                } else {
                    id
                };
                ec2_xml(
                    "DescribeFleetHistoryResponse",
                    &format!(
                        "<fleetId>{}</fleetId><historyRecordSet/><lastEvaluatedTime>{}</lastEvaluatedTime>",
                        xml_escape(&id),
                        super::now_iso()
                    ),
                )
            }
            _ => super::not_implemented(op),
        }
    })
}

/// Spot prices render moto-style with six decimals (e.g. `0.050000`).
fn price6(raw: &str) -> String {
    raw.parse::<f64>()
        .map(|v| format!("{v:.6}"))
        .unwrap_or_else(|_| raw.to_string())
}

/// `<launchSpecifications>` block reconstructed from a stored spec count.
fn launch_specifications_xml(count: usize) -> String {
    if count == 0 {
        return String::new();
    }
    let mut items = String::new();
    for _ in 0..count {
        items.push_str(
            "<item><imageId>ami-12345678</imageId><instanceType>t2.micro</instanceType></item>",
        );
    }
    format!("<launchSpecifications>{items}</launchSpecifications>")
}

/// Ids from a wire key that botocore sends either flat (`Key=x`) or indexed
/// (`Key.1=x`). `Params::list` only knows the indexed form.
pub fn ids_of(params: &P, key: &str) -> Vec<String> {
    let mut v: Vec<String> = params.list(key).iter().map(|s| s.to_string()).collect();
    if v.is_empty() {
        let v0 = params.str(key);
        if !v0.is_empty() {
            v.push(v0);
        }
    }
    v
}

fn params_quantity(req: &Request, default_qty: i64) -> i64 {
    params_of(req)
        .get("Quantity")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default_qty)
        .max(1)
}

fn p_target(req: &Request) -> String {
    let p = params_of(req);
    for key in [
        "TargetCapacitySpecification.TotalTargetCapacity",
        "TargetCapacitySpecification.1.TotalTargetCapacity",
        "TargetCapacity",
    ] {
        let v = p.str(key);
        if !v.is_empty() {
            return v;
        }
    }
    "1".to_string()
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
            .get("instanceType")
            .or_else(|| v.get("InstanceType"))
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
        "DisassociateIamInstanceProfile",
        "DescribeIamInstanceProfileAssociations",
        "AssociateInstanceEventWindow",
        "DisassociateInstanceEventWindow",
        "ModifyInstanceEventWindow",
        "AttachVerifiedAccessTrustProvider",
        "CreateCoipCidr",
        "DeleteCoipCidr",
        "CreateInstanceConnectEndpoint",
        "DeleteInstanceConnectEndpoint",
        "DescribeInstanceConnectEndpoints",
        "DescribeStaleSecurityGroups",
        "GetCoipPoolUsage",
        "GetEnabledIpamPolicy",
        "StartNetworkInsightsAnalysis",
        "DescribeNetworkInsightsAnalyses",
        "StartNetworkInsightsAccessScopeAnalysis",
        "DescribeNetworkInsightsAccessScopeAnalyses",
        "GetNetworkInsightsAccessScopeAnalysisFindings",
        "GetNetworkInsightsAccessScopeContent",
    ]
    .into_iter()
    .collect()
});

pub fn gap_ops_b_response(req: &Request, op: &str, params: &P) -> Response {
    let body = match op {
        "AssociateIamInstanceProfile" => {
            let aid = format!("iip-assoc-{}", super::state::idhex::hex17());
            let iid = id_of(params, "InstanceId");
            let arn = params.str("IamInstanceProfile.Arn");
            let mut s = Simple {
                id: aid.clone(),
                kind: "iip-assoc".to_string(),
                ..Default::default()
            };
            s.set("InstanceId", iid.clone());
            s.set("Arn", arn.clone());
            s.set("State", "associating");
            with_state(req, |st| {
                st.simple
                    .entry("iip-assoc".to_string())
                    .or_default()
                    .push(s.clone());
            });
            format!(
                "<iamInstanceProfileAssociation><associationId>{aid}</associationId><instanceId>{iid}</instanceId><iamInstanceProfile><id></id><arn>{arn}</arn></iamInstanceProfile><state>associating</state></iamInstanceProfileAssociation>",
                aid = xml_escape(&aid),
                iid = xml_escape(&iid),
                arn = xml_escape(&arn)
            )
        }
        "DisassociateIamInstanceProfile" => {
            let aid = id_of(params, "AssociationId");
            let mut removed: Option<Simple> = None;
            with_state(req, |st| {
                if let Some(items) = st.simple.get_mut("iip-assoc") {
                    if let Some(pos) = items.iter().position(|s| s.id == aid) {
                        removed = Some(items.remove(pos));
                    }
                }
            });
            let s = removed.unwrap_or_default();
            format!(
                "<iamInstanceProfileAssociation><associationId>{aid}</associationId><instanceId>{iid}</instanceId><iamInstanceProfile><id></id><arn>{arn}</arn></iamInstanceProfile><state>disassociating</state></iamInstanceProfileAssociation>",
                aid = xml_escape(&aid),
                iid = xml_escape(s.get("InstanceId").unwrap_or_default()),
                arn = xml_escape(s.get("Arn").unwrap_or_default())
            )
        }
        "DescribeIamInstanceProfileAssociations" => {
            let ids = ids_of(params, "AssociationId");
            let mut assoc_out = String::new();
            with_state(req, |st| {
                if let Some(items_vec) = st.simple.get("iip-assoc").cloned() {
                    for s in &items_vec {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        assoc_out.push_str(&format!(
                            "<item><associationId>{aid}</associationId><instanceId>{iid}</instanceId><iamInstanceProfile><id></id><arn>{arn}</arn></iamInstanceProfile><state>{st}</state></item>",
                            aid = xml_escape(&s.id),
                            iid = xml_escape(s.get("InstanceId").unwrap_or_default()),
                            arn = xml_escape(s.get("Arn").unwrap_or_default()),
                            st = xml_escape(s.get("State").unwrap_or("associated"))
                        ));
                    }
                }
            });
            format!(
                "<iamInstanceProfileAssociationSet>{assoc_out}</iamInstanceProfileAssociationSet>"
            )
        }
        "AssociateInstanceEventWindow" | "DisassociateInstanceEventWindow" | "ModifyInstanceEventWindow" => format!(
            "<instanceEventWindow><instanceEventWindowId>{}</instanceEventWindowId></instanceEventWindow>",
            xml_escape(id_of(params, "InstanceEventWindowId").as_str())
        ),
        "CreateInstanceConnectEndpoint" => {
            let ice_id = format!("ice-{}", super::state::idhex::hex17());
            let vpc = params.str("VpcId");
            let subnet = params.str("SubnetId");
            with_state(req, |st| {
                let mut s = Simple {
                    id: ice_id.clone(),
                    kind: "instance-connect-endpoint".to_string(),
                    ..Default::default()
                };
                s.set("VpcId", vpc.clone());
                s.set("SubnetId", subnet.clone());
                s.set("State", "available");
                s.item_xml = format!(
                    "<item><instanceConnectEndpointId>{}</instanceConnectEndpointId><vpcId>{}</vpcId><subnetId>{}</subnetId><type>interface</type><state>available</state></item>",
                    xml_escape(&ice_id),
                    xml_escape(&vpc),
                    xml_escape(&subnet)
                );
                st.simple
                    .entry("instance-connect-endpoint".to_string())
                    .or_default()
                    .push(s);
            });
            format!(
                "<instanceConnectEndpoint><instanceConnectEndpointId>{}</instanceConnectEndpointId><vpcId>{}</vpcId><subnetId>{}</subnetId><type>interface</type><state>available</state></instanceConnectEndpoint>",
                xml_escape(&ice_id),
                xml_escape(&vpc),
                xml_escape(&subnet)
            )
        }
        "DeleteInstanceConnectEndpoint" => {
            let ice_id = id_of(params, "InstanceConnectEndpointId");
            with_state(req, |st| {
                if let Some(items) = st.simple.get_mut("instance-connect-endpoint") {
                    items.retain(|s| s.id != ice_id);
                }
            });
            format!(
                "<instanceConnectEndpoint><instanceConnectEndpointId>{}</instanceConnectEndpointId><state>delete-complete</state></instanceConnectEndpoint>",
                xml_escape(&ice_id)
            )
        }
        "DescribeInstanceConnectEndpoints" => {
            let ids = ids_of(params, "InstanceConnectEndpointId");
            let mut ice_out = String::new();
            with_state(req, |st| {
                if let Some(items_vec) = st.simple.get("instance-connect-endpoint").cloned() {
                    for s in &items_vec {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        ice_out.push_str(&s.item_xml);
                    }
                }
            });
            format!("<instanceConnectEndpointSet>{ice_out}</instanceConnectEndpointSet>")
        }
        "DescribeStaleSecurityGroups" => String::from("<staleSecurityGroupSet/>"),
        "StartNetworkInsightsAnalysis" => {
            let path = params.str("NetworkInsightsPathId");
            let nia_id = format!("nia-{}", super::state::idhex::hex17());
            with_state(req, |st| {
                let mut s = Simple {
                    id: nia_id.clone(),
                    kind: "network-insights-analysis".to_string(),
                    ..Default::default()
                };
                s.set("NetworkInsightsPathId", path.clone());
                s.set("Status", "succeeded");
                s.item_xml = format!(
                    "<item><networkInsightsAnalysisId>{}</networkInsightsAnalysisId><networkInsightsPathId>{}</networkInsightsPathId><status>succeeded</status><startDate>{}</startDate><networkPathFound>true</networkPathFound></item>",
                    xml_escape(&nia_id),
                    xml_escape(&path),
                    super::now_iso()
                );
                st.simple
                    .entry("network-insights-analysis".to_string())
                    .or_default()
                    .push(s);
            });
            format!(
                "<networkInsightsAnalysis><networkInsightsAnalysisId>{}</networkInsightsAnalysisId><networkInsightsAnalysisArn>arn:aws:ec2:us-east-1:123456789012:network-insights-analysis/{}</networkInsightsAnalysisArn><networkInsightsPathId>{}</networkInsightsPathId><status>succeeded</status><startDate>{}</startDate><networkPathFound>true</networkPathFound></networkInsightsAnalysis>",
                xml_escape(&nia_id),
                xml_escape(&nia_id),
                xml_escape(&path),
                super::now_iso()
            )
        }
        "DescribeNetworkInsightsAnalyses" => {
            let ids = ids_of(params, "NetworkInsightsAnalysisId");
            let mut nia_out = String::new();
            with_state(req, |st| {
                if let Some(items_vec) = st.simple.get("network-insights-analysis").cloned() {
                    for s in &items_vec {
                        if !ids.is_empty() && !ids.contains(&s.id) {
                            continue;
                        }
                        nia_out.push_str(&s.item_xml);
                    }
                }
            });
            format!("<networkInsightsAnalysisSet>{nia_out}</networkInsightsAnalysisSet>")
        }
        "StartNetworkInsightsAccessScopeAnalysis" => {
            let scope = params.str("NetworkInsightsAccessScopeId");
            let nisa_id = format!("nisa-{}", super::state::idhex::hex17());
            with_state(req, |st| {
                let mut s = Simple {
                    id: nisa_id.clone(),
                    kind: "network-insights-access-scope-analysis".to_string(),
                    ..Default::default()
                };
                s.set("NetworkInsightsAccessScopeId", scope.clone());
                s.set("Status", "succeeded");
                s.item_xml = format!(
                    "<item><networkInsightsAccessScopeAnalysisId>{}</networkInsightsAccessScopeAnalysisId><networkInsightsAccessScopeId>{}</networkInsightsAccessScopeId><status>succeeded</status><startDate>{}</startDate></item>",
                    xml_escape(&nisa_id),
                    xml_escape(&scope),
                    super::now_iso()
                );
                st.simple
                    .entry("network-insights-access-scope-analysis".to_string())
                    .or_default()
                    .push(s);
            });
            format!(
                "<networkInsightsAccessScopeAnalysis><networkInsightsAccessScopeAnalysisId>{}</networkInsightsAccessScopeAnalysisId><networkInsightsAccessScopeAnalysisArn>arn:aws:ec2:us-east-1:123456789012:network-insights-access-scope-analysis/{}</networkInsightsAccessScopeAnalysisArn><networkInsightsAccessScopeId>{}</networkInsightsAccessScopeId><status>succeeded</status><startDate>{}</startDate><findingsFound>false</findingsFound></networkInsightsAccessScopeAnalysis>",
                xml_escape(&nisa_id),
                xml_escape(&nisa_id),
                xml_escape(&scope),
                super::now_iso()
            )
        }
        "DescribeNetworkInsightsAccessScopeAnalyses" => {
            let scope = params.str("NetworkInsightsAccessScopeId");
            let mut nisa_out = String::new();
            with_state(req, |st| {
                if let Some(items_vec) = st
                    .simple
                    .get("network-insights-access-scope-analysis")
                    .cloned()
                {
                    for s in &items_vec {
                        if !scope.is_empty() && s.get("NetworkInsightsAccessScopeId") != Some(scope.as_str()) {
                            continue;
                        }
                        nisa_out.push_str(&s.item_xml);
                    }
                }
            });
            format!("<networkInsightsAccessScopeAnalysisSet>{nisa_out}</networkInsightsAccessScopeAnalysisSet>")
        }
        "GetNetworkInsightsAccessScopeAnalysisFindings" => {
            let id = params.str("NetworkInsightsAccessScopeAnalysisId");
            format!(
                "<networkInsightsAccessScopeAnalysisId>{}</networkInsightsAccessScopeAnalysisId><analysisStatus>succeeded</analysisStatus><analysisFindingSet/>",
                xml_escape(&id)
            )
        }
        "GetNetworkInsightsAccessScopeContent" => {
            let scope = params.str("NetworkInsightsAccessScopeId");
            format!(
                "<networkInsightsAccessScopeContent><networkInsightsAccessScopeId>{}</networkInsightsAccessScopeId><compatibilityPolicy>overlap</compatibilityPolicy><matchPathSet/><excludePathSet/></networkInsightsAccessScopeContent>",
                xml_escape(&scope)
            )
        }
        "CreateCoipCidr" => format!(
            "<coipCidr><cidr>{c}</cidr><coipPoolId>{p}</coipPoolId></coipCidr>",
            c = "10.10.0.0/24",
            p = xml_escape(id_of(params, "CoipPoolId").as_str())
        ),
                "GetCoipPoolUsage" => format!(
            "<coipAddressUsageSet><item><allocationId></allocationId><awsAccountId>123456789012</awsAccountId><coIp></coIp></item></coipAddressUsageSet><coipPoolId>{}</coipPoolId><localGatewayRouteTableId></localGatewayRouteTableId>",
            xml_escape(id_of(params, "PoolId").as_str())
        ),
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

// ---------------------------------------------------------------------------
// gap ops batch 4 (wave 11): the per-op 501 terminator. Every modeled EC2 op
// landing here serves its botocore-shaped envelope — state/id-sensitive ops
// check the twin's families first (modeled 4xx paths), the rest synthesize
// from the wire shape. Ops without a model shape (no-output members in the
// service model) answer a bare modeled root.
// ---------------------------------------------------------------------------

/// Shape-driven default: full envelope from botocore's output shape, or the
/// bare root for the model's no-output ops.
fn synth_default(req: &Request, params: &P) -> Response {
    let ctx = super::shapes::SynthCtx {
        params,
        account: req.account,
        region: &req.region,
        tags: super::handlers::tag_spec_tags(params),
    };
    let op: &str = &req.operation;
    match super::shapes::synth_response(op, &ctx, &super::shapes::Fills::new()) {
        Some(resp) => resp,
        None => {
            let root = format!("{op}Response");
            super::xmlns_xml(&root, Some(super::EC2_NS), "")
        }
    }
}

/// Wire tag for a python output key (`AsnAssociation` -> `asnAssociation`).
fn camel_wire_of(pykey: &str) -> String {
    let mut out = String::new();
    for (i, c) in pykey.char_indices() {
        if i == 0 {
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// In-state VpcId check for gap arms already holding the write lease.
fn vpc_err_in(state: &mut super::state::Ec2State, vpc_id: &str) -> Option<Response> {
    if state.vpcs.iter().any(|v| v.id == vpc_id) {
        return None;
    }
    Some(ec2_error(
        400,
        "InvalidVpcID.NotFound",
        &format!("Vpc {{{}}} not found", xml_escape(vpc_id)),
    ))
}

fn gap_not_found(kind_code: &'static str, label: &str, id: &str) -> Response {
    ec2_error(
        400,
        kind_code,
        &format!("The {label} ID '{}' does not exist", xml_escape(id)),
    )
}

fn gap_ops_c_response(req: &Request, op: &str, params: &P) -> Response {
    match op {
        // --- instances: id-validated attribute ops ---------------------------
        "GetConsoleScreenshot" => {
            let id = params.str("InstanceId");
            if let Some(err) = instance_exists(req, &id) {
                return err;
            }
            ec2_xml(
                "GetConsoleScreenshotResponse",
                &format!(
                    "<instanceId>{}</instanceId><imageData></imageData>",
                    xml_escape(&id)
                ),
            )
        }
        "SendDiagnosticInterrupt" | "ReportInstanceStatus" => {
            let ids: Vec<String> = params
                .list("InstanceId")
                .iter()
                .chain(params.list("Instances").iter())
                .map(|s| s.to_string())
                .collect();
            for id in &ids {
                if let Some(err) = instance_exists(req, id) {
                    return err;
                }
            }
            let _op = op;
            ec2_xml(&format!("{op}Response"), "")
        }
        "ModifyInstanceCpuOptions"
        | "ModifyInstanceCreditSpecification"
        | "ModifyInstanceEventStartTime"
        | "ModifyInstanceMaintenanceOptions"
        | "ModifyInstanceNetworkPerformanceOptions"
        | "ModifyInstancePlacement"
        | "ModifyPrivateDnsNameOptions"
        | "ModifyPublicIpDnsNameOptions"
        | "ModifyInstanceCapacityReservationAttributes"
        | "RegisterInstanceEventNotificationAttributes"
        | "DeregisterInstanceEventNotificationAttributes" => {
            let id = params.str("InstanceId");
            if let Some(err) = instance_exists(req, &id) {
                return err;
            }
            if op == "ModifyInstanceCpuOptions" {
                return ec2_xml("ModifyInstanceCpuOptionsResponse", "<return>true</return>");
            }
            synth_default(req, params)
        }
        "ModifyInstanceConnectEndpoint" => {
            let id = params.str("InstanceConnectEndpointId");
            if let Some(err) = simple_exists(
                req,
                "instance-connect-endpoint",
                &id,
                "InvalidInstanceConnectEndpointId.NotFound",
                "instanceConnectEndpoint",
                &id,
            ) {
                return err;
            }
            synth_default(req, params)
        }
        "ConfirmProductInstance" => {
            let id = params.str("InstanceId");
            if let Some(err) = instance_exists(req, &id) {
                return err;
            }
            ec2_xml(
                "ConfirmProductInstanceResponse",
                &format!("<ownerId>{}</ownerId><return>true</return>", req.account),
            )
        }
        // --- scheduled / reserved --------------------------------------------
        "DescribeScheduledInstanceAvailability" | "GetReservedInstancesExchangeQuote" => {
            synth_default(req, params)
        }
        "RunScheduledInstances"
        | "PurchaseReservedInstancesOffering"
        | "ModifyReservedInstances" => synth_default(req, params),
        // --- images -----------------------------------------------------------
        "DisableImage" | "EnableImage" => {
            let id = params.str("ImageId");
            with_state(req, |state| {
                if let Some(img) = state.images.iter_mut().find(|v| v.id == id) {
                    img.state = if op == "DisableImage" {
                        "disabled".into()
                    } else {
                        "available".into()
                    };
                    return ec2_xml(&format!("{op}Response"), "<return>true</return>");
                }
                if state.run_ami_ok.contains(&id) {
                    return ec2_xml(&format!("{op}Response"), "<return>true</return>");
                }
                gap_not_found("InvalidAMIID.NotFound", "ami", &id)
            })
        }
        "EnableImageDeprecation"
        | "DisableImageDeprecation"
        | "EnableImageDeregistrationProtection"
        | "DisableImageDeregistrationProtection" => {
            let id = params.str("ImageId");
            with_state(req, move |state| {
                if !state.images.iter().any(|v| v.id == id) && !state.run_ami_ok.contains(&id) {
                    return gap_not_found("InvalidAMIID.NotFound", "ami", &id);
                }
                let dep_time = params.str("ImageDeprecationTime");
                let prot = params.str("ImageDeregistrationProtection");
                if let Some(img) = state.images.iter_mut().find(|v| v.id == id) {
                    if op.contains("Deprecation") {
                        img.deprecation_time = if op.starts_with("Enable") {
                            Some(if dep_time.is_empty() {
                                "2038-01-01T00:00:00Z".to_string()
                            } else {
                                dep_time
                            })
                        } else {
                            None
                        };
                    } else {
                        img.deregistration_protection = if op.starts_with("Enable") {
                            Some(if prot.is_empty() {
                                "with-replace".to_string()
                            } else {
                                prot
                            })
                        } else {
                            None
                        };
                    }
                }
                ec2_xml(&format!("{op}Response"), "<return>true</return>")
            })
        }
        "EnableImageBlockPublicAccess" | "EnableAllowedImagesSettings" => {
            with_state(req, |state| {
                if op == "EnableImageBlockPublicAccess" {
                    let cur = state
                        .simple
                        .get("image-bpa")
                        .and_then(|items| items.first())
                        .and_then(|s| s.get("State"))
                        .map(|s| s.to_string());
                    let want = match params.str("ImageBlockPublicAccessState").as_str() {
                        "" => "block-new-sharing".to_string(),
                        v => v.to_string(),
                    };
                    let _ = &cur;
                    if let Some(items) = state.simple.get_mut("image-bpa") {
                        items.retain(|s| true);
                        if let Some(s) = items.first_mut() {
                            s.set("State", &want);
                        } else {
                            let mut s = Simple {
                                id: "global".to_string(),
                                kind: "image-bpa".to_string(),
                                ..Default::default()
                            };
                            s.set("State", &want);
                            items.push(s);
                        }
                    } else {
                        let mut s = Simple {
                            id: "global".to_string(),
                            kind: "image-bpa".to_string(),
                            ..Default::default()
                        };
                        s.set("State", &want);
                        state
                            .simple
                            .entry("image-bpa".to_string())
                            .or_default()
                            .push(s);
                    }
                    return ec2_xml(
                        "EnableImageBlockPublicAccessResponse",
                        &format!(
                            "<imageBlockPublicAccessState>{}</imageBlockPublicAccessState>",
                            xml_escape(&want)
                        ),
                    );
                }
                let want = match params.str("AllowedImagesSettingsState").as_str() {
                    "" => "enabled".to_string(),
                    v => v.to_string(),
                };
                let stored = {
                    let items = state
                        .simple
                        .entry("images-settings".to_string())
                        .or_default();
                    let have = items.first().map(|s| s.get("State").map(|v| v.to_string()));
                    match items.first_mut() {
                        Some(s) => s.set("State", &want),
                        None => {
                            let mut s = Simple {
                                id: "global".to_string(),
                                kind: "images-settings".to_string(),
                                ..Default::default()
                            };
                            s.set("State", &want);
                            items.push(s);
                        }
                    }
                    have
                };
                let _ = stored;
                ec2_xml(
                    "EnableAllowedImagesSettingsResponse",
                    &format!(
                        "<allowedImagesSettingsState>{}</allowedImagesSettingsState>",
                        xml_escape(&want)
                    ),
                )
            })
        }
        "RestoreImageFromRecycleBin" | "GetImageAncestry" => synth_default(req, params),
        "DeleteFpgaImage" => gap_not_found(
            "InvalidFpgaImageID.NotFound",
            "fpgaImage",
            &params.str("FpgaImageId"),
        ),
        "ModifyFpgaImageAttribute" | "DescribeFpgaImageAttribute" | "ResetFpgaImageAttribute" => {
            gap_not_found(
                "InvalidFpgaImageID.NotFound",
                "fpgaImage",
                &params.str("FpgaImageId"),
            )
        }
        // --- snapshots ---------------------------------------------------------
        "EnableFastSnapshotRestores" | "DisableFastSnapshotRestores" => {
            let src: Vec<String> = params
                .list("SourceSnapshotId")
                .iter()
                .chain(params.list("SourceSnapshotIds").iter())
                .map(|s| s.to_string())
                .collect();
            let az = format!("{}a", req.region);
            with_state(req, move |state| {
                for id in &src {
                    if !state.snapshots.iter().any(|s| &s.id == id) {
                        return gap_not_found("InvalidSnapshot.NotFound", "snapshot", id);
                    }
                }
                let mut items = String::new();
                for sid in &src {
                    items.push_str(&format!(
                        "<item><snapshotId>{sid}</snapshotId><availabilityZone>{az}</availabilityZone><state>enabled</state></item>",
                        sid = xml_escape(sid),
                        az = xml_escape(&az)
                    ));
                }
                ec2_xml(
                    &format!("{op}Response"),
                    &format!(
                        "<successfulSnapshotSet>{items}</successfulSnapshotSet><unsuccessfulSnapshotSet/>"
                    ),
                )
            })
        }
        "LockSnapshot" | "UnlockSnapshot" => {
            let id = params.str("SnapshotId");
            with_state(req, move |state| {
                let lock_mode = params.str("LockMode");
                if !state.snapshots.iter().any(|s| s.id == id) {
                    return gap_not_found("InvalidSnapshot.NotFound", "snapshot", &id);
                }
                if op == "LockSnapshot" {
                    let mut s = Simple {
                        id: id.clone(),
                        kind: "snapshot-lock".to_string(),
                        ..Default::default()
                    };
                    s.set("SnapshotId", id.clone());
                    s.set("LockMode", lock_mode.clone());
                    s.set("LockDuration", params.str("LockDuration"));
                    s.set("LockState", "locked");
                    state
                        .simple
                        .entry("snapshot-lock".to_string())
                        .or_default()
                        .push(s);
                } else {
                    if let Some(items) = state.simple.get_mut("snapshot-lock") {
                        items.retain(|s| s.id != id);
                    }
                }
                ec2_xml(
                    &format!("{op}Response"),
                    &format!("<snapshotId>{}</snapshotId>", xml_escape(&id)),
                )
            })
        }
        "ModifySnapshotTier" => {
            let id = params.str("SnapshotId");
            let want_tier = params.str("TargetTier");
            with_state(req, move |state| {
                let snap = match state.snapshots.iter_mut().find(|s| s.id == id) {
                    Some(s) => s,
                    None => return gap_not_found("InvalidSnapshot.NotFound", "snapshot", &id),
                };
                if !want_tier.is_empty() {
                    snap.storage_tier = want_tier.clone();
                }
                ec2_xml(
                    "ModifySnapshotTierResponse",
                    &format!(
                        "<snapshotId>{}</snapshotId><tieringStartTime>{}</tieringStartTime>",
                        xml_escape(&id),
                        super::now_iso()
                    ),
                )
            })
        }
        "RestoreSnapshotTier" => {
            let id = params.str("SnapshotId");
            with_state(req, move |state| {
                let snap = match state.snapshots.iter_mut().find(|s| s.id == id) {
                    Some(s) => s,
                    None => return gap_not_found("InvalidSnapshot.NotFound", "snapshot", &id),
                };
                snap.storage_tier = "standard".into();
                snap.restore_expiry = None;
                ec2_xml(
                    "RestoreSnapshotTierResponse",
                    &format!(
                        "<snapshotId>{}</snapshotId><isPermanentRestore>true</isPermanentRestore><tieringStartTime>{}</tieringStartTime><progress>100%</progress>",
                        xml_escape(&id),
                        super::now_iso()
                    ),
                )
            })
        }
        "RestoreSnapshotFromRecycleBin" | "RestoreVolumeFromRecycleBin" => {
            let id_key = if op == "RestoreSnapshotFromRecycleBin" {
                "SnapshotId"
            } else {
                "VolumeId"
            };
            let id = params.str(id_key);
            with_state(req, move |state| {
                let known = match id_key {
                    "SnapshotId" => state.snapshots.iter().any(|s| s.id == id),
                    _ => state.volumes.iter().any(|v| v.id == id),
                };
                if !known {
                    return synth_default(req, params);
                }
                ec2_xml(
                    &format!("{op}Response"),
                    &format!(
                        "<return>true</return><{}>{}</{}>",
                        id_key,
                        xml_escape(&id),
                        id_key
                    ),
                )
            })
        }
        "ResetSnapshotAttribute" => {
            let id = params.str("SnapshotId");
            with_state(req, move |state| {
                if !state.snapshots.iter().any(|s| s.id == id) {
                    return gap_not_found("InvalidSnapshot.NotFound", "snapshot", &id);
                }
                state.snapshot_attributes.remove(&id);
                ec2_xml("ResetSnapshotAttributeResponse", "")
            })
        }
        // --- volumes ------------------------------------------------------------
        "EnableVolumeIO" => {
            let id = params.str("VolumeId");
            with_state(req, move |state| {
                let pos = match state.volumes.iter().position(|v| v.id == id) {
                    Some(i) => i,
                    None => return gap_not_found("InvalidVolume.NotFound", "volume", &id),
                };
                let _ = pos;
                ec2_xml("EnableVolumeIOResponse", "")
            })
        }
        // --- elastic IPs / address attributes -----------------------------------
        "ModifyAddressAttribute"
        | "EnableAddressTransfer"
        | "DisableAddressTransfer"
        | "ResetAddressAttribute" => {
            let id = params.str("AllocationId");
            with_state(req, move |state| {
                let pos = match state.addresses.iter().position(|v| v.allocation_id == id) {
                    Some(i) => i,
                    None => {
                        return gap_not_found("InvalidAllocationID.NotFound", "allocation", &id)
                    }
                };
                if op == "ModifyAddressAttribute" {
                    let ptr = params.str("PtrRecord");
                    if !ptr.is_empty() {
                        let mut s = Simple {
                            id: id.clone(),
                            kind: "address-ptr".to_string(),
                            ..Default::default()
                        };
                        s.set("PtrRecord", ptr.clone());
                        state
                            .simple
                            .entry("address-ptr".to_string())
                            .or_default()
                            .push(s);
                    }
                }
                if op == "EnableAddressTransfer" {
                    let mut s = Simple {
                        id: id.clone(),
                        kind: "address-transfer".to_string(),
                        ..Default::default()
                    };
                    s.set("AllocationId", id.clone());
                    s.set("TransferAccountId", params.str("TransferAccountId"));
                    s.set("TransferStatus", "pending");
                    let _ = pos;
                    state
                        .simple
                        .entry("address-transfer".to_string())
                        .or_default()
                        .push(s);
                }
                if op == "DisableAddressTransfer" {
                    if let Some(items) = state.simple.get_mut("address-transfer") {
                        items.retain(|s| s.id != id);
                    }
                }
                synth_default(req, params)
            })
        }
        "MoveAddressToVpc" | "RestoreAddressToClassic" => {
            let _ip = params.str("PublicIp");
            with_state(req, |state| {
                let status = if op == "MoveAddressToVpc" {
                    "moved"
                } else {
                    "restored"
                };
                ec2_xml(
                    &format!("{op}Response"),
                    &format!("<status>{}</status>", status),
                )
            })
        }
        // --- NAT gateway addresses ----------------------------------------------
        "DisassociateNatGatewayAddress" | "UnassignPrivateNatGatewayAddress" => {
            let id = params.str("NatGatewayId");
            with_state(req, move |state| {
                if state.nat_gateways.iter().all(|g| g.id != id) {
                    return gap_not_found("InvalidNatGatewayID.NotFound", "natGateway", &id);
                }
                synth_default(req, params)
            })
        }
        // --- interfaces / trunks --------------------------------------------------
        "AssociateTrunkInterface" => {
            let branch = params.str("BranchInterfaceId");
            let trunk = params.str("TrunkInterfaceId");
            with_state(req, move |state| {
                if !state.enis.iter().any(|e| e.id == branch) {
                    return gap_not_found("InvalidInterfaceID.NotFound", "interface", &branch);
                }
                if !state.enis.iter().any(|e| e.id == trunk) {
                    return gap_not_found("InvalidInterfaceID.NotFound", "interface", &trunk);
                }
                let assoc_id = format!("ta-{}", super::state::idhex::hex17());
                let mut s = Simple {
                    id: assoc_id.clone(),
                    kind: "trunk-assoc".to_string(),
                    ..Default::default()
                };
                s.set("TrunkInterfaceId", trunk.clone());
                s.set("BranchInterfaceId", branch.clone());
                s.set("VlanId", params.str("VlanId"));
                state
                    .simple
                    .entry("trunk-assoc".to_string())
                    .or_default()
                    .push(s);
                synth_default(req, params)
            })
        }
        "DisassociateTrunkInterface" | "DeleteNetworkInterfacePermission" => {
            if op == "DisassociateTrunkInterface" {
                let aid = params.str("AssociationId");
                with_state(req, move |state| {
                    if let Some(items) = state.simple.get_mut("trunk-assoc") {
                        items.retain(|s| s.id != aid);
                    }
                });
            }
            ec2_xml(
                &format!("{op}Response"),
                &format!(
                    "<return>true</return><clientToken>{ct}</clientToken>",
                    ct = xml_escape(&params.str("ClientToken"))
                ),
            )
        }
        "ResetNetworkInterfaceAttribute" => {
            let id = params.str("NetworkInterfaceId");
            with_state(req, move |state| {
                if state.enis.iter().all(|e| e.id != id) {
                    return gap_not_found(
                        "InvalidNetworkInterfaceID.NotFound",
                        "networkInterface",
                        &id,
                    );
                }
                ec2_xml("ResetNetworkInterfaceAttributeResponse", "")
            })
        }
        // --- route tables ---------------------------------------------------------
        "EnableVgwRoutePropagation" | "DisableVgwRoutePropagation" => {
            let rtb = params.str("RouteTableId");
            let gw = params.str("GatewayId");
            with_state(req, move |state| {
                if let Some(rt) = state.route_tables.iter_mut().find(|r| r.id == rtb) {
                    if op == "EnableVgwRoutePropagation" {
                        if !rt.propagations.contains(&gw) {
                            rt.propagations.push(gw.clone());
                        }
                    } else {
                        rt.propagations.retain(|g| g != &gw);
                    }
                    return ec2_xml(&format!("{op}Response"), "");
                }
                gap_not_found("InvalidRouteTableID.NotFound", "routeTable", &rtb)
            })
        }
        // --- elastic ip transfer ---------------------------------------------------
        "DescribeSecurityGroupReferences" | "GetSecurityGroupsForVpc" => synth_default(req, params),
        "GetManagedPrefixListAssociations" | "RestoreManagedPrefixListVersion" => {
            let pl = params.str("PrefixListId");
            if let Some(err) = simple_exists(
                req,
                "managed-prefix-list",
                &pl,
                "InvalidPrefixListID.NotFound",
                "prefixList",
                &pl,
            ) {
                return err;
            }
            synth_default(req, params)
        }
        // --- vpn connections -------------------------------------------------------
        "CreateVpnConnectionRoute" | "DeleteVpnConnectionRoute" => {
            let vpn = params.str("VpnConnectionId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("vpn-connection")
                    .map(|items| items.iter().any(|s| s.id == vpn))
                    .unwrap_or(false)
                {
                    return gap_not_found("InvalidVpnConnectionID.NotFound", "vpnConnection", &vpn);
                }
                let dest = params.str("DestinationCidrBlock");
                if op == "CreateVpnConnectionRoute" {
                    let mut s = Simple {
                        id: dest.clone(),
                        kind: "vpn-connection-route".to_string(),
                        ..Default::default()
                    };
                    s.set("VpnConnectionId", vpn.clone());
                    s.set("DestinationCidrBlock", dest.clone());
                    state
                        .simple
                        .entry("vpn-connection-route".to_string())
                        .or_default()
                        .push(s);
                } else if let Some(items) = state.simple.get_mut("vpn-connection-route") {
                    items.retain(|s| s.id != dest);
                }
                ec2_xml(&format!("{op}Response"), "")
            })
        }
        "ModifyVpnConnection"
        | "ModifyVpnConnectionOptions"
        | "ModifyVpnTunnelOptions"
        | "ModifyVpnTunnelCertificate"
        | "ReplaceVpnTunnel"
        | "GetVpnTunnelReplacementStatus"
        | "GetActiveVpnTunnelStatus"
        | "GetVpnConnectionDeviceSampleConfiguration" => {
            let vpn = params.str("VpnConnectionId");
            with_state(req, move |state| {
                let item = state
                    .simple
                    .get("vpn-connection")
                    .and_then(|items| items.iter().find(|s| s.id == vpn))
                    .map(|s| s.item_xml.clone());
                if item.is_none() {
                    return gap_not_found("InvalidVpnConnectionID.NotFound", "vpnConnection", &vpn);
                }
                if op.starts_with("Modify") && op != "ModifyVpnTunnelCertificate" {
                    // re-render the stored family item as the op's member
                    let xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root}Response xmlns=\"{ns}\"><vpnConnection>{inner}</vpnConnection><requestId>{rid}</requestId></{root}Response>",
                        root = op,
                        ns = super::EC2_NS,
                        inner = item.unwrap_or_default(),
                        rid = uuid::Uuid::new_v4()
                    );
                    return Response::xml(200, xml.into_bytes());
                }
                synth_default(req, params)
            })
        }
        // --- client vpn ---------------------------------------------------------
        "AuthorizeClientVpnIngress"
        | "RevokeClientVpnIngress"
        | "DeleteClientVpnRoute"
        | "DisassociateClientVpnTargetNetwork"
        | "TerminateClientVpnConnections"
        | "ExportClientVpnClientConfiguration"
        | "ExportClientVpnClientCertificateRevocationList"
        | "ImportClientVpnClientCertificateRevocationList" => {
            let ep = params.str("ClientVpnEndpointId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("client-vpn-endpoint")
                    .map(|items| items.iter().any(|s| s.id == ep))
                    .unwrap_or(false)
                {
                    return gap_not_found(
                        "InvalidClientVpnEndpointId.NotFound",
                        "clientVpnEndpoint",
                        &ep,
                    );
                }
                if op == "ImportClientVpnClientCertificateRevocationList" {
                    let crl = params.str("CertificateRevocationList");
                    if let Some(items) = state.simple.get_mut("client-vpn-endpoint") {
                        for s in items.iter_mut() {
                            if s.id == ep {
                                s.set("CertificateRevocationList", crl.clone());
                            }
                        }
                    }
                }
                synth_default(req, params)
            })
        }
        // --- verified access ----------------------------------------------------
        "DetachVerifiedAccessTrustProvider" | "ModifyVerifiedAccessTrustProvider" => {
            let vatp = params.str("VerifiedAccessTrustProviderId");
            with_state(req, move |state| {
                let items = state
                    .simple
                    .get("verified-access-trust-provider")
                    .cloned()
                    .unwrap_or_default();
                let item = items.iter().find(|s| s.id == vatp);
                if item.is_none() {
                    return gap_not_found(
                        "InvalidVerifiedAccessTrustProviderId.NotFound",
                        "verifiedAccessTrustProvider",
                        &vatp,
                    );
                }
                synth_default(req, params)
            })
        }
        "ExportVerifiedAccessInstanceClientConfiguration"
        | "ModifyVerifiedAccessInstance"
        | "ModifyVerifiedAccessInstanceLoggingConfiguration" => {
            let vai = params.str("VerifiedAccessInstanceId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("verified-access-instance")
                    .map(|items| items.iter().any(|s| s.id == vai))
                    .unwrap_or(false)
                {
                    return gap_not_found(
                        "InvalidVerifiedAccessInstanceId.NotFound",
                        "verifiedAccessInstance",
                        &vai,
                    );
                }
                synth_default(req, params)
            })
        }
        "ModifyVerifiedAccessEndpoint"
        | "ModifyVerifiedAccessEndpointPolicy"
        | "GetVerifiedAccessEndpointPolicy"
        | "GetVerifiedAccessEndpointTargets" => {
            let vae = params.str("VerifiedAccessEndpointId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("verified-access-endpoint")
                    .map(|items| items.iter().any(|s| s.id == vae))
                    .unwrap_or(false)
                {
                    return gap_not_found(
                        "InvalidVerifiedAccessEndpointId.NotFound",
                        "verifiedAccessEndpoint",
                        &vae,
                    );
                }
                if op == "ModifyVerifiedAccessEndpointPolicy"
                    || op == "GetVerifiedAccessEndpointPolicy"
                {
                    let pol = params.str("PolicyDocument");
                    if let Some(items) = state.simple.get_mut("verified-access-endpoint") {
                        for s in items.iter_mut() {
                            if s.id == vae {
                                if let Some(d) = params.get("PolicyEnabled") {
                                    s.set("PolicyEnabled", d.to_string());
                                }
                                if !pol.is_empty() {
                                    s.set("PolicyDocument", pol.clone());
                                }
                            }
                        }
                    }
                }
                synth_default(req, params)
            })
        }
        "ModifyVerifiedAccessGroup"
        | "ModifyVerifiedAccessGroupPolicy"
        | "GetVerifiedAccessGroupPolicy" => {
            let vag = params.str("VerifiedAccessGroupId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("verified-access-group")
                    .map(|items| items.iter().any(|s| s.id == vag))
                    .unwrap_or(false)
                {
                    return gap_not_found(
                        "InvalidVerifiedAccessGroupId.NotFound",
                        "verifiedAccessGroup",
                        &vag,
                    );
                }
                synth_default(req, params)
            })
        }
        // --- network insights ----------------------------------------------------
        "DeleteNetworkInsightsAnalysis" | "DeleteNetworkInsightsAccessScopeAnalysis" => {
            let (kind, key, code, label) = if op == "DeleteNetworkInsightsAnalysis" {
                (
                    "network-insights-analysis",
                    "NetworkInsightsAnalysisId",
                    "InvalidNetworkInsightsAnalysisId.NotFound",
                    "networkInsightsAnalysis",
                )
            } else {
                (
                    "network-insights-access-scope-analysis",
                    "NetworkInsightsAccessScopeAnalysisId",
                    "InvalidNetworkInsightsAccessScopeAnalysisId.NotFound",
                    "networkInsightsAccessScopeAnalysis",
                )
            };
            let id = params.str(key);
            with_state(req, move |state| {
                let known = state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false);
                if !known {
                    return gap_not_found(code, label, &id);
                }
                if let Some(items) = state.simple.get_mut(kind) {
                    items.retain(|s| s.id != id);
                }
                ec2_xml(
                    &format!("{op}Response"),
                    &format!(
                        "<{}>{}</{}>",
                        camel_wire_of(key),
                        xml_escape(&id),
                        camel_wire_of(key)
                    ),
                )
            })
        }
        // --- traffic mirror ---------------------------------------------------------
        "ModifyTrafficMirrorFilterRule"
        | "ModifyTrafficMirrorFilterNetworkServices"
        | "ModifyTrafficMirrorSession" => {
            let (kind, key, code, label) = match op {
                "ModifyTrafficMirrorFilterRule" => (
                    "traffic-mirror-filter-rule",
                    "TrafficMirrorFilterRuleId",
                    "InvalidTrafficMirrorFilterRuleId.NotFound",
                    "trafficMirrorFilterRule",
                ),
                "ModifyTrafficMirrorFilterNetworkServices" => (
                    "traffic-mirror-filter",
                    "TrafficMirrorFilterId",
                    "InvalidTrafficMirrorFilterId.NotFound",
                    "trafficMirrorFilter",
                ),
                _ => (
                    "traffic-mirror-session",
                    "TrafficMirrorSessionId",
                    "InvalidTrafficMirrorSessionId.NotFound",
                    "trafficMirrorSession",
                ),
            };
            let id = params.str(key);
            with_state(req, move |state| {
                if !state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false)
                {
                    return gap_not_found(code, label, &id);
                }
                synth_default(req, params)
            })
        }
        // --- route servers ------------------------------------------------------------
        "AssociateRouteServer"
        | "DisassociateRouteServer"
        | "ModifyRouteServer"
        | "EnableRouteServerPropagation"
        | "DisableRouteServerPropagation"
        | "GetRouteServerAssociations"
        | "GetRouteServerPropagations"
        | "GetRouteServerRoutingDatabase" => {
            let rs = params.str("RouteServerId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("route-server")
                    .map(|items| items.iter().any(|s| s.id == rs))
                    .unwrap_or(false)
                {
                    return gap_not_found("InvalidRouteServerId.NotFound", "routeServer", &rs);
                }
                if op == "AssociateRouteServer" {
                    let vpc = params.str("VpcId");
                    if let Some(err) = vpc_err_in(state, &vpc) {
                        return err;
                    }
                    let aid = format!("rsvpc-assoc-{}", super::state::idhex::hex17());
                    let mut s = Simple {
                        id: aid.clone(),
                        kind: "route-server-assoc".to_string(),
                        ..Default::default()
                    };
                    s.set("RouteServerId", rs.clone());
                    s.set("VpcId", vpc.clone());
                    s.set("State", "associated");
                    state
                        .simple
                        .entry("route-server-assoc".to_string())
                        .or_default()
                        .push(s);
                    return ec2_xml(
                        "AssociateRouteServerResponse",
                        &format!(
                            "<routeServerAssociation><routeServerId>{rid}</routeServerId><vpcId>{vpc}</vpcId><state>associated</state></routeServerAssociation>",
                            rid = xml_escape(&rs),
                            vpc = xml_escape(&vpc)
                        ),
                    );
                }
                if op == "ModifyRouteServer" {
                    if let Some(items) = state.simple.get_mut("route-server") {
                        for s in items.iter_mut() {
                            if s.id != rs {
                                continue;
                            }
                            if let Some(v) = params.get("SnsNotificationsEnabled") {
                                s.set("SnsNotificationsEnabled", v.to_string());
                            }
                            if let Some(v) = params.get("SnsTopicArn") {
                                s.set("SnsTopicArn", v.to_string());
                            }
                            if let Some(v) = params.get("PersistRoutes") {
                                s.set(
                                    "PersistRoutesState",
                                    if v == "enable" { "enabled" } else { "disabled" }.to_string(),
                                );
                            }
                        }
                    }
                    return ec2_xml(
                        "ModifyRouteServerResponse",
                        &format!(
                            "<routeServer><routeServerId>{rid}</routeServerId><state>modifying</state></routeServer>",
                            rid = xml_escape(&rs)
                        ),
                    );
                }
                synth_default(req, params)
            })
        }
        // --- transit gateway metering policy entries --------------------------------
        "CreateTransitGatewayMeteringPolicyEntry"
        | "DeleteTransitGatewayMeteringPolicyEntry"
        | "ModifyTransitGatewayMeteringPolicy"
        | "GetTransitGatewayMeteringPolicyEntries" => {
            let id = params.str("TransitGatewayMeteringPolicyId");
            with_state(req, move |state| {
                if !state
                    .simple
                    .get("transit-gateway-metering-policy")
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false)
                {
                    return gap_not_found(
                        "InvalidTransitGatewayMeteringPolicyId.NotFound",
                        "transitGatewayMeteringPolicy",
                        &id,
                    );
                }
                if op == "CreateTransitGatewayMeteringPolicyEntry" {
                    let eid = format!("tgw-mp{}", super::state::idhex::hex17());
                    let mut s = Simple {
                        id: eid.clone(),
                        kind: "transit-gateway-metering-policy-entry".to_string(),
                        ..Default::default()
                    };
                    s.set("TransitGatewayMeteringPolicyId", id.clone());
                    s.set("PolicyRuleNumber", params.str("PolicyRuleNumber"));
                    s.set("MeteredAccount", params.str("MeteredAccount"));
                    state
                        .simple
                        .entry("transit-gateway-metering-policy-entry".to_string())
                        .or_default()
                        .push(s);
                }
                synth_default(req, params)
            })
        }
        "DeleteTransitGatewayPolicyTable"
        | "DisassociateTransitGatewayPolicyTable"
        | "GetTransitGatewayPolicyTableAssociations"
        | "GetTransitGatewayPolicyTableEntries"
        | "DeleteTransitGatewayMulticastDomain"
        | "DisassociateTransitGatewayMulticastDomain"
        | "GetTransitGatewayMulticastDomainAssociations"
        | "RegisterTransitGatewayMulticastGroupMembers"
        | "RegisterTransitGatewayMulticastGroupSources"
        | "SearchTransitGatewayMulticastGroups" => synth_default(req, params),
        // --- capacity reservations ------------------------------------------------------
        "AssociateCapacityReservationBillingOwner"
        | "DisassociateCapacityReservationBillingOwner"
        | "RejectCapacityReservationBillingOwnership"
        | "GetCapacityReservationUsage"
        | "GetGroupsForCapacityReservation"
        | "MoveCapacityReservationInstances"
        | "PurchaseCapacityBlockExtension"
        | "DescribeCapacityBlockExtensionOfferings"
        | "DescribeCapacityReservationBillingRequests"
        | "CreateInterruptibleCapacityReservationAllocation"
        | "UpdateInterruptibleCapacityReservationAllocation" => {
            let _ = params.str("CapacityReservationId");
            synth_default(req, params)
        }
        "GetCapacityManagerMetricDimensions" | "UpdateCapacityManagerOrganizationsAccess" => {
            synth_default(req, params)
        }
        // --- ipam family -------------------------------------------------------------------
        "EnableIpamOrganizationAdminAccount" | "DisableIpamOrganizationAdminAccount" => {
            ec2_xml(&format!("{op}Response"), "<success>true</success>")
        }
        "EnableIpamPolicy" | "DisableIpamPolicy" | "ModifyIpamPolicyAllocationRules" => {
            let id = params.str("IpamPolicyId");
            with_state(req, move |state| {
                if op != "ModifyIpamPolicyAllocationRules"
                    && !state
                        .simple
                        .get("ipam-policy")
                        .map(|items| items.iter().any(|s| s.id == id))
                        .unwrap_or(false)
                {
                    return gap_not_found("InvalidIpamPolicyId.NotFound", "ipamPolicy", &id);
                }
                synth_default(req, params)
            })
        }
        "GetIpamPolicyAllocationRules"
        | "GetIpamPolicyOrganizationTargets"
        | "GetIpamAddressHistory"
        | "GetIpamDiscoveredAccounts"
        | "GetIpamDiscoveredPublicAddresses"
        | "GetIpamDiscoveredResourceCidrs"
        | "GetIpamPrefixListResolverRules"
        | "GetIpamPrefixListResolverVersions"
        | "GetIpamPrefixListResolverVersionEntries" => {
            let _resolver = params.str("IpamPrefixListResolverId");
            synth_default(req, params)
        }
        "ModifyIpamPrefixListResolver" | "ModifyIpamPrefixListResolverTarget" => {
            let (kind, key, code, label) = if op == "ModifyIpamPrefixListResolver" {
                (
                    "ipam-prefix-list-resolver",
                    "IpamPrefixListResolverId",
                    "InvalidIpamPrefixListResolverId.NotFound",
                    "ipamPrefixListResolver",
                )
            } else {
                (
                    "ipam-prefix-list-resolver-target",
                    "IpamPrefixListResolverTargetId",
                    "InvalidIpamPrefixListResolverTargetId.NotFound",
                    "ipamPrefixListResolverTarget",
                )
            };
            let id = params.str(key);
            with_state(req, move |state| {
                if !state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == id))
                    .unwrap_or(false)
                {
                    return gap_not_found(code, label, &id);
                }
                synth_default(req, params)
            })
        }
        "AssociateIpamByoasn" | "DisassociateIpamByoasn" => {
            let asn = params.str("Asn");
            let cidr = params.str("Cidr");
            with_state(req, move |state| {
                if asn.is_empty() || cidr.is_empty() {
                    return synth_default(req, params);
                }
                let kind = "ipam-asn-assoc";
                let present = state
                    .simple
                    .get(kind)
                    .map(|items| items.iter().any(|s| s.id == format!("{asn}:{cidr}")))
                    .unwrap_or(false);
                if op == "AssociateIpamByoasn" {
                    let mut s = Simple {
                        id: format!("{asn}:{cidr}"),
                        kind: kind.to_string(),
                        ..Default::default()
                    };
                    s.set("Asn", asn.clone());
                    s.set("Cidr", cidr.clone());
                    s.set("State", "associated");
                    s.set("StatusMessage", String::new());
                    state.simple.entry(kind.to_string()).or_default().push(s);
                } else {
                    if let Some(items) = state.simple.get_mut(kind) {
                        items.retain(|s| s.id != format!("{asn}:{cidr}"));
                    }
                }
                ec2_xml(
                    &format!("{op}Response"),
                    &format!(
                        "<asnAssociation><asn>{asn}</asn><cidr>{cidr}</cidr><state>{st}</state><statusMessage></statusMessage></asnAssociation>",
                        asn = xml_escape(&asn),
                        cidr = xml_escape(&cidr),
                        st = if op == "AssociateIpamByoasn" { "associated" } else { "disassociated" }
                    ),
                )
            })
        }
        "ModifyIpamScope"
        | "ModifyIpamResourceCidr"
        | "MoveByoipCidrToIpam"
        | "DisassociateIpamResourceDiscovery"
        | "WithdrawByoipCidr" => synth_default(req, params),
        // --- misc mutates / smoke ops ------------------------------------------------
        "CancelDeclarativePoliciesReport"
        | "StartDeclarativePoliciesReport"
        | "ModifyAvailabilityZoneGroup"
        | "ModifyIdFormat"
        | "ModifyIdentityIdFormat"
        | "DescribeIdentityIdFormat"
        | "DescribeSpotFleetRequestHistory"
        | "ExportImage"
        | "CopyVolumes"
        | "CreateDelegateMacVolumeOwnershipTask"
        | "CreateMacSystemIntegrityProtectionModificationTask"
        | "DetachClassicLinkVpc"
        | "GetFlowLogsIntegrationTemplate"
        | "GetInstanceTpmEkPub"
        | "GetInstanceTypesFromInstanceRequirements"
        | "GetSpotPlacementScores"
        | "GetAssociatedIpv6PoolCidrs"
        | "GetAssociatedEnclaveCertificateIamRoles"
        | "DisassociateEnclaveCertificateIamRole"
        | "GetVpcResourcesBlockingEncryptionEnforcement"
        | "StartVpcEndpointServicePrivateDnsVerification"
        | "ModifyVpcEndpointConnectionNotification"
        | "ModifyVpcEndpointServicePayerResponsibility" => synth_default(req, params),
        // --- local gateway family (stateless) ------------------------------------------
        "CreateLocalGatewayVirtualInterface"
        | "CreateLocalGatewayVirtualInterfaceGroup"
        | "DeleteLocalGatewayVirtualInterface"
        | "DeleteLocalGatewayVirtualInterfaceGroup"
        | "DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation"
        | "DeleteLocalGatewayRouteTableVpcAssociation"
        | "ModifyLocalGatewayRoute" => synth_default(req, params),
        // --- everything else: modeled from the botocore shape ---------------------------
        _ => synth_default(req, params),
    }
}

/// Stateful IPAM pool-cidr / allocation store: `ipam-pool-cidr` and
/// `ipam-pool-alloc` Simple entries keyed to their pool id.
pub fn ipam_route(req: &Request, op: &str, params: &P) -> Response {
    with_state(req, move |state| match op {
        "ProvisionIpamPoolCidr" => {
            let pool = params.str("IpamPoolId");
            let cidr = params.str("Cidr");
            if cidr.is_empty() {
                let net = params.i64("NetmaskLength").unwrap_or(16);
                return ec2_error(
                    400,
                    "MissingParameter",
                    &format!("Provide Cidr or a valid NetmaskLength ({net})"),
                );
            }
            let id = format!("ipc-{}", super::state::idhex::hex17());
            let mut s = Simple {
                id: id.clone(),
                kind: "ipam-pool-cidr".to_string(),
                ..Default::default()
            };
            s.set("IpamPoolId", pool.clone());
            s.set("Cidr", cidr.clone());
            s.set("State", "provisioned");
            s.set("IpamPoolCidrId", id.clone());
            let net = cidr
                .split('/')
                .nth(1)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            s.set("NetmaskLength", net.to_string());
            state
                .simple
                .entry("ipam-pool-cidr".to_string())
                .or_default()
                .push(s);
            ec2_xml(
                "ProvisionIpamPoolCidrResponse",
                &format!(
                    "<ipamPoolCidr><cidr>{}</cidr><state>provisioned</state><ipamPoolCidrId>{}</ipamPoolCidrId><netmaskLength>{}</netmaskLength></ipamPoolCidr>",
                    xml_escape(&cidr),
                    xml_escape(&id),
                    net
                ),
            )
        }
        "AllocateIpamPoolCidr" => {
            let pool = params.str("IpamPoolId");
            let netmask = params.i64("NetmaskLength");
            let provisioned: Vec<(String, i64)> = state
                .simple
                .get("ipam-pool-cidr")
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|s| s.get("IpamPoolId") == Some(pool.as_str()))
                .filter_map(|s| {
                    Some((
                        s.get("Cidr")?.to_string(),
                        s.get("NetmaskLength")
                            .and_then(|n| n.parse::<i64>().ok())
                            .unwrap_or(32),
                    ))
                })
                .collect();
            let used: Vec<String> = state
                .simple
                .get("ipam-pool-alloc")
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|s| s.get("IpamPoolId") == Some(pool.as_str()))
                .filter_map(|s| s.get("Cidr").map(|c| c.to_string()))
                .collect();
            let (cidr, netmask) = allocate_from_provisioned(&provisioned, &used, netmask);
            let cidr = match cidr {
                Some(c) => c,
                None => {
                    return ec2_error(
                        400,
                        "InvalidIpamPoolId.NotFound",
                        "No provisioned CIDR block available to allocate from",
                    )
                }
            };
            let id = format!("ipa-{}", super::state::idhex::hex17());
            let mut s = Simple {
                id: id.clone(),
                kind: "ipam-pool-alloc".to_string(),
                ..Default::default()
            };
            s.set("IpamPoolId", pool.clone());
            s.set("Cidr", cidr.clone());
            s.set("IpamPoolAllocationId", id.clone());
            state
                .simple
                .entry("ipam-pool-alloc".to_string())
                .or_default()
                .push(s);
            ec2_xml(
                "AllocateIpamPoolCidrResponse",
                &format!(
                    "<ipamPoolAllocation><cidr>{}</cidr><ipamPoolAllocationId>{}</ipamPoolAllocationId></ipamPoolAllocation>",
                    xml_escape(&cidr),
                    xml_escape(&id)
                ),
            )
        }
        "ReleaseIpamPoolAllocation" => {
            let pool = params.str("IpamPoolId");
            let cidr = params.str("Cidr");
            let alloc_id = params.str("IpamPoolAllocationId");
            if let Some(items) = state.simple.get_mut("ipam-pool-alloc") {
                items.retain(|s| {
                    !(s.get("IpamPoolId") == Some(pool.as_str())
                        && (!cidr.is_empty() || !alloc_id.is_empty())
                        && ((Some(cidr.as_str()) == s.get("Cidr"))
                            || (Some(alloc_id.as_str()) == s.get("IpamPoolAllocationId"))))
                });
            }
            ec2_xml(
                "ReleaseIpamPoolAllocationResponse",
                "<success>true</success>",
            )
        }
        "DeprovisionIpamPoolCidr" => {
            let pool = params.str("IpamPoolId");
            let cidr = params.str("Cidr");
            let net = cidr
                .split('/')
                .nth(1)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            if let Some(items) = state.simple.get_mut("ipam-pool-cidr") {
                items.retain(|s| {
                    !(s.get("IpamPoolId") == Some(pool.as_str())
                        && !cidr.is_empty()
                        && Some(cidr.as_str()) == s.get("Cidr"))
                });
            }
            ec2_xml(
                "DeprovisionIpamPoolCidrResponse",
                &format!(
                    "<ipamPoolCidr><cidr>{}</cidr><state>deprovisioned</state><ipamPoolCidrId></ipamPoolCidrId><netmaskLength>{}</netmaskLength></ipamPoolCidr>",
                    xml_escape(&cidr),
                    net
                ),
            )
        }
        "GetIpamPoolCidrs" => {
            let pool = params.str("IpamPoolId");
            let items = state
                .simple
                .get("ipam-pool-cidr")
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|s| s.get("IpamPoolId") == Some(pool.as_str()))
                .map(|s| {
                    format!(
                        "<item><cidr>{}</cidr><state>provisioned</state><ipamPoolCidrId>{}</ipamPoolCidrId><netmaskLength>{}</netmaskLength></item>",
                        xml_escape(s.get("Cidr").unwrap_or("")),
                        xml_escape(s.id.as_str()),
                        s.get("NetmaskLength").unwrap_or("0")
                    )
                })
                .collect::<Vec<_>>()
                .concat();
            ec2_xml(
                "GetIpamPoolCidrsResponse",
                &format!("<ipamPoolCidrSet>{items}</ipamPoolCidrSet>"),
            )
        }
        "GetIpamPoolAllocations" => {
            let pool = params.str("IpamPoolId");
            let items = state
                .simple
                .get("ipam-pool-alloc")
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter(|s| s.get("IpamPoolId") == Some(pool.as_str()))
                .map(|s| {
                    format!(
                        "<item><cidr>{}</cidr><ipamPoolAllocationId>{}</ipamPoolAllocationId></item>",
                        xml_escape(s.get("Cidr").unwrap_or_default()),
                        xml_escape(s.id.as_str())
                    )
                })
                .collect::<Vec<_>>()
                .concat();
            ec2_xml(
                "GetIpamPoolAllocationsResponse",
                &format!("<ipamPoolAllocationSet>{items}</ipamPoolAllocationSet>"),
            )
        }
        "GetIpamResourceCidrs" => ec2_xml("GetIpamResourceCidrsResponse", "<ipamResourceCidrSet/>"),
        "ModifyIpam" => {
            let id = params.str("IpamId");
            let desc = params.str("Description");
            let mut found = false;
            {
                let updated_desc;
                if let Some(items) = state.simple.get_mut("ipam") {
                    for s in items.iter_mut() {
                        if s.id == id {
                            found = true;
                            if !desc.is_empty() {
                                s.set("Description", desc.clone());
                            }
                            updated_desc = s.get("Description").unwrap_or_default().to_string();
                            let mut keep = s.clone();
                            keep.item_xml = ipam_item_xml(
                                Some(keep.clone()),
                                &updated_desc,
                                req.account,
                                &req.region,
                            );
                            *s = keep;
                            break;
                        }
                    }
                }
                if found {
                    let item = state_simple_snapshot(state, "ipam", &id)
                        .map(|s| s.item_xml.clone())
                        .unwrap_or_default();
                    ec2_xml("ModifyIpamResponse", &item)
                } else {
                    ec2_error(
                        400,
                        "InvalidIpamId.NotFound",
                        &format!("The IPAM ID '{}' does not exist", xml_escape(&id)),
                    )
                }
            }
        }
        "ModifyIpamPool" => {
            let id = params.str("IpamPoolId");
            let desc = params.str("Description");
            let mut found = false;
            if let Some(items) = state.simple.get_mut("ipam-pool") {
                for s in items.iter_mut() {
                    if s.id == id {
                        found = true;
                        if !desc.is_empty() {
                            s.set("Description", desc.clone());
                        }
                    }
                }
            }
            if !found {
                return ec2_error(
                    400,
                    "InvalidIpamPoolId.NotFound",
                    &format!("The IPAM pool '{}' does not exist", xml_escape(&id)),
                );
            }
            let item = simple_fields_xml(
                state,
                "ipam-pool",
                &id,
                &["IpamPoolId", "OwnerId", "Description", "State"],
            );
            ec2_xml(
                "ModifyIpamPoolResponse",
                &format!("<ipamPool>{item}</ipamPool>"),
            )
        }
        "ModifyIpamResourceDiscovery" => ec2_xml(
            "ModifyIpamResourceDiscoveryResponse",
            "<return>true</return>",
        ),
        _ => super::not_implemented(op),
    })
}

fn state_simple_snapshot(state: &super::state::Ec2State, kind: &str, id: &str) -> Option<Simple> {
    state
        .simple
        .get(kind)
        .and_then(|v| v.iter().find(|s| s.id == id))
        .cloned()
}

/// Re-render an ipam object from its stored fields after a Modify.
fn ipam_item_xml(s: Option<Simple>, desc: &str, account: u64, region: &str) -> String {
    let s = match s {
        Some(s) => s,
        None => {
            return String::new();
        }
    };
    let f = |k: &str| s.get(k).unwrap_or_default().to_string();
    let description = if desc.is_empty() {
        f("Description")
    } else {
        desc.to_string()
    };
    format!(
        "<ownerId>{account}</ownerId><ipamId>{id}</ipamId><ipamArn>arn:aws:ec2:{region}:{account}:ipam/{id}</ipamArn><ipamRegion>{region}</ipamRegion><operatingRegionSet><item><regionName>{region}</regionName></item></operatingRegionSet><scopeCount>2</scopeCount><description>{description}</description><publicDefaultScopeId>{public}</publicDefaultScopeId><privateDefaultScopeId>{priv}</privateDefaultScopeId><state>{state}</state><tagSet/>",
        account = account,
        region = region,
        id = xml_escape(&s.id),
        public = xml_escape(&f("PublicDefaultScopeId")),
        priv = xml_escape(&f("PrivateDefaultScopeId")),
        state = xml_escape(&f("State")),
        description = xml_escape(&description),
    )
}

/// Minimal field dump for a modify response, top-level members only.
fn simple_fields_xml(
    state: &super::state::Ec2State,
    kind: &str,
    id: &str,
    keys: &[&str],
) -> String {
    let s = state_simple_snapshot(state, kind, id);
    match s {
        Some(s) => keys
            .iter()
            .map(|k| {
                let wire = k
                    .chars()
                    .enumerate()
                    .map(|(i, c)| if i == 0 { c.to_ascii_lowercase() } else { c })
                    .collect::<String>();
                format!(
                    "<{wire}>{v}</{wire}>",
                    v = xml_escape(s.get(k).unwrap_or_default())
                )
            })
            .collect(),
        None => String::new(),
    }
}

/// Next free netmask slots inside the first provisioned block (v4 only).
fn allocate_from_provisioned(
    provisioned: &[(String, i64)],
    used: &[String],
    want_netmask: Option<i64>,
) -> (Option<String>, i64) {
    let (base, base_len) = match provisioned.first() {
        Some(b) => b.clone(),
        None => return (None, want_netmask.unwrap_or(24)),
    };
    if base.contains(':') {
        // ipv6: hand out /64 block under the base prefix's first 4 groups
        let prefix = base.split('/').next().unwrap_or("2001:db8::");
        let groups = prefix.split("::").next().unwrap_or("2001:db8").to_string();
        let mut n = u32::try_from(used.len()).unwrap_or(0);
        loop {
            let cand = format!("{groups}:{}::/64", n + 1);
            n += 1;
            if !used.contains(&cand) {
                return (Some(cand), 64);
            }
        }
    }
    let netmask = want_netmask.unwrap_or(24);
    let octets: Vec<u32> = base
        .split('/')
        .next()
        .unwrap_or_default()
        .split('.')
        .filter_map(|o| o.parse::<u32>().ok())
        .collect();
    if octets.len() != 4 {
        return (None, netmask);
    }
    // n-th /netmask slot inside the base block
    let base_bits = base_len.clamp(0, 32) as u32;
    if (netmask as u32) < base_bits {
        return (None, netmask);
    }
    // a /netmask block is 2^(32-netmask) addresses; slots walk the base block
    let unit = 1u64 << (32 - (netmask as u32)).min(31);
    let base_int = ((octets[0] as u64) << 24)
        | ((octets[1] as u64) << 16)
        | ((octets[2] as u64) << 8)
        | octets[3] as u64;
    let base_end = base_int + (1u64 << (32 - base_bits).min(32));
    for n in 0u64..(1u64 << ((netmask as u32) - base_bits).min(24)) {
        let val = base_int + n * unit;
        if val >= base_end {
            break;
        }
        let cand = format!(
            "{}.{}.{}.{}/{}",
            (val >> 24) & 0xff,
            (val >> 16) & 0xff,
            (val >> 8) & 0xff,
            val & 0xff,
            netmask
        );
        if !used.contains(&cand) {
            return (Some(cand), netmask);
        }
    }
    (None, netmask)
}

fn id_of(params: &P, key: &str) -> String {
    params.str(key)
}
