//! Per-family create/delete fills rendered through botocore's wire shapes.
//!
//! Fill keys are the python keys botocore uses for the op's output members
//! (see `shapes.json`); the renderer emits each member with its wire
//! `locationName`, so all botocore parses are exact.

use super::shapes::{Fill, Fills};
use super::Params;
use crate::proto::xml_escape;

type P = Params;

/// Build the fill set for a family's single-object create (or delete).
fn arn2(srv: &str, region: &str, account: u64, id: &str) -> String {
    format!("arn:aws:ec2:{}:{}:{}/{}", region, account, srv, id)
}

pub fn create_fills(
    kind: &str,
    id: &str,
    params: &P,
    account: u64,
    region: &str,
    deleting: bool,
    tags_xml: &str,
) -> Fills {
    let mut f: Fills = Fills::new();
    let arn_of = |srv: &str| {
        format!(
            "arn:aws:ec2:{}:{}:{}/{}",
            region,
            account,
            srv,
            xml_escape(id)
        )
    };
    let account_s = account.to_string();
    match kind {
        "ipam" => {
            f.insert("IpamId".to_string(), Fill::S(id.to_string()));
            f.insert("IpamArn".to_string(), Fill::S(arn_of("ipam")));
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert("IpamRegion".to_string(), Fill::S(region.to_string()));
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            f.insert("ScopeCount".to_string(), Fill::S("2".into()));
            f.insert(
                "PublicDefaultScopeId".to_string(),
                Fill::S(format!("ipam-scope-{}", super::state::idhex::hex17())),
            );
            f.insert(
                "PrivateDefaultScopeId".to_string(),
                Fill::S(format!("ipam-scope-{}", super::state::idhex::hex17())),
            );
            f.insert(
                "OperatingRegions".to_string(),
                Fill::X("<item><regionName>us-east-1</regionName></item>".to_string()),
            );
            f.insert("Tier".to_string(), Fill::S("advanced".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-scope" => {
            f.insert("IpamScopeId".to_string(), Fill::S(id.to_string()));
            f.insert("IpamScopeArn".to_string(), Fill::S(arn_of("ipam-scope")));
            let ipam_id = params.str("IpamId");
            let region = region.to_string();
            f.insert(
                "IpamArn".to_string(),
                Fill::S(arn2("ipam", &region, account, &ipam_id)),
            );
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert("IpamRegion".to_string(), Fill::S(region.to_string()));
            f.insert("IpamScopeType".to_string(), Fill::S("private".into()));
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
            f.insert("IsDefault".to_string(), Fill::S("false".into()));
            f.insert("PoolCount".to_string(), Fill::S("0".into()));
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-pool" => {
            f.insert("IpamPoolId".to_string(), Fill::S(id.to_string()));
            f.insert("IpamPoolArn".to_string(), Fill::S(arn_of("ipam-pool")));
            f.insert("IpamScopeArn".to_string(), Fill::S(arn_of("ipam-scope")));
            let ipam_id = params.str("IpamId");
            let region = region.to_string();
            f.insert(
                "IpamArn".to_string(),
                Fill::S(arn2("ipam", &region, account, &ipam_id)),
            );
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert("IpamRegion".to_string(), Fill::S(region.to_string()));
            f.insert("Locale".to_string(), Fill::S(region.to_string()));
            f.insert(
                "AddressFamily".to_string(),
                Fill::S(params.str("AddressFamily")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            f.insert("PoolDepth".to_string(), Fill::S("0".into()));
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
            f.insert("AutoImport".to_string(), Fill::S("false".into()));
            f.insert("PubliclyAdvertisable".to_string(), Fill::S("false".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "customer-gateway" => {
            f.insert("CustomerGatewayId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert("Type".to_string(), Fill::S(params.str("Type")));
            f.insert("IpAddress".to_string(), Fill::S(params.str("IpAddress")));
            f.insert(
                "BgpAsn".to_string(),
                Fill::S(match params.i64("BgpAsn") {
                    Some(v) => v.to_string(),
                    None => "65000".to_string(),
                }),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "vpn-gateway" => {
            f.insert("VpnGatewayId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert("Type".to_string(), Fill::S(params.str("Type")));
            f.insert(
                "AmazonSideAsn".to_string(),
                Fill::S(match params.get("AmazonSideAsn") {
                    Some(a) if !a.is_empty() => a.to_string(),
                    _ => "64512".to_string(),
                }),
            );
            f.insert(
                "AvailabilityZone".to_string(),
                Fill::S(params.str("AvailabilityZone")),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "vpn-connection" => {
            f.insert("VpnConnectionId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert("Type".to_string(), Fill::S(params.str("Type")));
            f.insert(
                "CustomerGatewayId".to_string(),
                Fill::S(params.str("CustomerGatewayId")),
            );
            f.insert(
                "VpnGatewayId".to_string(),
                Fill::S(params.str("VpnGatewayId")),
            );
            f.insert(
                "TransitGatewayId".to_string(),
                Fill::S(params.str("TransitGatewayId")),
            );
            f.insert("Category".to_string(), Fill::S("VPN".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "traffic-mirror-filter-rule" => {
            f.insert(
                "TrafficMirrorFilterRuleId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "TrafficMirrorFilterId".to_string(),
                Fill::S(params.str("TrafficMirrorFilterId")),
            );
            f.insert(
                "TrafficDirection".to_string(),
                Fill::S(params.str("TrafficDirection")),
            );
            f.insert(
                "RuleNumber".to_string(),
                Fill::S(params.get("RuleNumber").unwrap_or("1").to_string()),
            );
            f.insert("RuleAction".to_string(), Fill::S(params.str("RuleAction")));
            f.insert(
                "Protocol".to_string(),
                Fill::S(params.get("Protocol").unwrap_or("-1").to_string()),
            );
            f.insert(
                "DestinationCidrBlock".to_string(),
                Fill::S(params.str("DestinationCidrBlock")),
            );
            f.insert(
                "SourceCidrBlock".to_string(),
                Fill::S(params.str("SourceCidrBlock")),
            );
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
        }
        "coip-pool" => {
            f.insert("PoolId".to_string(), Fill::S(id.to_string()));
            f.insert("PoolArn".to_string(), Fill::S(arn_of("coip-pool")));
            f.insert(
                "LocalGatewayRouteTableId".to_string(),
                Fill::S(params.str("LocalGatewayRouteTableId")),
            );
        }
        "public-ipv4-pool" => {
            f.insert("PoolId".to_string(), Fill::S(id.to_string()));
        }
        "instance-event-window" => {
            f.insert("InstanceEventWindowId".to_string(), Fill::S(id.to_string()));
            f.insert("Name".to_string(), Fill::S(params.str("Name")));
            f.insert("State".to_string(), Fill::S("active".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "network-insights-path" => {
            f.insert("NetworkInsightsPathId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "NetworkInsightsPathArn".to_string(),
                Fill::S(arn_of("network-insights-path")),
            );
            f.insert("Source".to_string(), Fill::S(params.str("Source")));
            f.insert(
                "Destination".to_string(),
                Fill::S(params.str("Destination")),
            );
            f.insert("Protocol".to_string(), Fill::S(params.str("Protocol")));
            f.insert("CreatedDate".to_string(), Fill::S(super::now_iso()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "egress-only-igw" => {
            f.insert(
                "EgressOnlyInternetGatewayId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "tgw-peering" => {
            f.insert(
                "TransitGatewayAttachmentId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert("State".to_string(), Fill::S("pendingAcceptance".into()));
            let tgw_id = params.str("TransitGatewayId");
            let peer_id = params.str("PeerTransitGatewayId");
            f.insert(
                "RequesterTgwInfo".to_string(),
                Fill::X(format!(
                    "<item><transitGatewayId>{}</transitGatewayId><ownerId>{}</ownerId><region>{}</region></item>",
                    xml_escape(&tgw_id),
                    account,
                    region
                )),
            );
            f.insert(
                "AccepterTgwInfo".to_string(),
                Fill::X(format!(
                    "<item><transitGatewayId>{}</transitGatewayId><ownerId>{}</ownerId><region>{}</region></item>",
                    xml_escape(&peer_id),
                    account,
                    params.str("PeerRegion")
                )),
            );
            f.insert(
                "Status".to_string(),
                Fill::X("<item><code>pendingAcceptance</code></item>".into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "tgw-connect" => {
            f.insert(
                "TransitGatewayAttachmentId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "TransportTransitGatewayAttachmentId".to_string(),
                Fill::S(params.str("TransportTransitGatewayAttachmentId")),
            );
            f.insert("State".to_string(), Fill::S("pending".into()));
        }
        "tgw-connect-peer" => {
            f.insert(
                "TransitGatewayConnectPeerId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "TransitGatewayAttachmentId".to_string(),
                Fill::S(params.str("TransitGatewayAttachmentId")),
            );
            f.insert("State".to_string(), Fill::S("pending".into()));
            f.insert(
                "PeerAddress".to_string(),
                Fill::S(params.str("PeerAddress")),
            );
            f.insert(
                "TransitGatewayAddress".to_string(),
                Fill::S(params.str("TransitGatewayAddress")),
            );
            let cidrs: Vec<String> = params
                .list("InsideCidrBlock")
                .iter()
                .map(|s| xml_escape(s))
                .collect();
            if !cidrs.is_empty() {
                f.insert(
                "ConnectPeerConfiguration".to_string(),
                    Fill::X(format!(
                        "<item><transitGatewayAddress>{}</transitGatewayAddress><peerAddress>{}</peerAddress><bgpConfigurations/><insideCidrBlocks>{}</insideCidrBlocks></item>",
                        xml_escape(&params.str("TransitGatewayAddress")),
                        xml_escape(&params.str("PeerAddress")),
                        cidrs.concat()
                    )),
                );
            }
        }
        "tgw" => {
            f.insert("TransitGatewayId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "TransitGatewayArn".to_string(),
                Fill::S(arn_of("transit-gateway")),
            );
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert(
                "AmazonSideAsn".to_string(),
                Fill::S(match params.get("AmazonSideAsn") {
                    Some(a) if !a.is_empty() => a.to_string(),
                    _ => "64512".to_string(),
                }),
            );
            f.insert(
                "Tags".to_string(),
                Fill::X(if tags_xml.is_empty() {
                    String::new()
                } else {
                    tags_xml.to_string()
                }),
            );
        }
        "capacity-reservation" => {
            f.insert("CapacityReservationId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "InstanceType".to_string(),
                Fill::S(params.str("InstanceType")),
            );
            f.insert(
                "InstancePlatform".to_string(),
                Fill::S(params.str("InstancePlatform")),
            );
            f.insert(
                "AvailabilityZone".to_string(),
                Fill::S(params.str("AvailabilityZone")),
            );
            f.insert(
                "TotalInstanceCount".to_string(),
                Fill::S(params.get("InstanceCount").unwrap_or("1").to_string()),
            );
            f.insert(
                "AvailableInstanceCount".to_string(),
                Fill::S(params.get("InstanceCount").unwrap_or("1").to_string()),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "cancelled" } else { "active" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "capacity-reservation-fleet" => {
            f.insert(
                "CapacityReservationFleetId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "AllocationStrategy".to_string(),
                Fill::S(match params.get("AllocationStrategy") {
                    Some(a) if !a.is_empty() => a.to_string(),
                    _ => "prioritized".to_string(),
                }),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "cancelled" } else { "active" }.into()),
            );
            f.insert(
                "TotalTargetCapacity".to_string(),
                Fill::S(params.get("TotalTargetCapacity").unwrap_or("1").to_string()),
            );
            f.insert("TotalFulfilledCapacity".to_string(), Fill::S("0.0".into()));
        }
        "fleet" => {
            f.insert("FleetId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "ActivityStatus".to_string(),
                Fill::S("pending-fulfilment".into()),
            );
        }
        "spot-fleet-request" => {
            f.insert("SpotFleetRequestId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "SpotFleetRequestState".to_string(),
                Fill::S(if deleting { "cancelled" } else { "submitted" }.into()),
            );
        }
        "spot-instance-request" => {
            f.insert("SpotInstanceRequestId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "cancelled" } else { "open" }.into()),
            );
            f.insert("SpotPrice".to_string(), Fill::S(params.str("SpotPrice")));
            f.insert("Type".to_string(), Fill::S("one-time".into()));
        }
        "managed-prefix-list" => {
            f.insert("PrefixListId".to_string(), Fill::S(id.to_string()));
            f.insert("PrefixListArn".to_string(), Fill::S(arn_of("prefix-list")));
            f.insert(
                "AddressFamily".to_string(),
                Fill::S(params.str("AddressFamily")),
            );
            f.insert(
                "MaxEntries".to_string(),
                Fill::S(params.get("MaxEntries").unwrap_or("10").to_string()),
            );
            f.insert("Version".to_string(), Fill::S("1".into()));
            f.insert(
                "PrefixListName".to_string(),
                Fill::S(params.str("PrefixListName")),
            );
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
        }
        "traffic-mirror-filter" => {
            f.insert("TrafficMirrorFilterId".to_string(), Fill::S(id.to_string()));
        }
        "traffic-mirror-target" => {
            f.insert("TrafficMirrorTargetId".to_string(), Fill::S(id.to_string()));
        }
        "traffic-mirror-session" => {
            f.insert(
                "TrafficMirrorSessionId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "verified-access-instance" => {
            f.insert(
                "VerifiedAccessInstanceId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
        }
        "verified-access-group" => {
            f.insert("VerifiedAccessGroupId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "VerifiedAccessInstanceId".to_string(),
                Fill::S(params.str("VerifiedAccessInstanceId")),
            );
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
        }
        "verified-access-endpoint" => {
            f.insert(
                "VerifiedAccessEndpointId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "verified-access-trust-provider" => {
            f.insert(
                "VerifiedAccessTrustProviderId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "ipam-resource-discovery" => {
            f.insert(
                "IpamResourceDiscoveryId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "client-vpn-endpoint" => {
            f.insert("ClientVpnEndpointId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "Status".to_string(),
                Fill::X(String::from(if deleting {
                    "<code>deleting</code><message></message>"
                } else {
                    "<code>pending-associate</code><message></message>"
                })),
            );
            f.insert(
                "DnsName".to_string(),
                Fill::S(format!(
                    "*.cvpn-{}.prod.clientvpn.us-east-1.amazonaws.com",
                    id
                )),
            );
            f.insert(
                "ClientCidrBlock".to_string(),
                Fill::S(params.str("ClientCidrBlock")),
            );
            f.insert(
                "ServerCertificateArn".to_string(),
                Fill::S(params.str("ServerCertificateArn")),
            );
        }
        "network-insights-access-scope" => {
            f.insert(
                "NetworkInsightsAccessScopeId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "host" => {
            let count = params
                .get("Quantity")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(1)
                .max(1);
            let mut items = String::new();
            for _ in 0..count {
                items.push_str(&format!("<item>{id}{empty}</item>", id = id, empty = ""));
            }
            f.insert("HostIds".to_string(), Fill::X(items));
        }
        // --- wave-11 families: create + delete fills -------------------------
        "route-server" => {
            f.insert("RouteServerId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "AmazonSideAsn".to_string(),
                Fill::S(match params.get("AmazonSideAsn") {
                    Some(a) if !a.is_empty() => a.to_string(),
                    _ => "64512".to_string(),
                }),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert(
                "PersistRoutesState".to_string(),
                Fill::S(
                    match params.get("PersistRoutes") {
                        Some("enable") => "enabled",
                        _ => "disabled",
                    }
                    .into(),
                ),
            );
            f.insert(
                "PersistRoutesDuration".to_string(),
                Fill::S(
                    params
                        .get("PersistRoutesDuration")
                        .unwrap_or("")
                        .to_string(),
                ),
            );
            f.insert(
                "SnsNotificationsEnabled".to_string(),
                Fill::S(
                    if params.bool("SnsNotificationsEnabled").unwrap_or(false) {
                        "true"
                    } else {
                        "false"
                    }
                    .into(),
                ),
            );
            f.insert(
                "SnsTopicArn".to_string(),
                Fill::S(params.str("SnsTopicArn")),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "route-server-endpoint" => {
            f.insert("RouteServerEndpointId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "RouteServerId".to_string(),
                Fill::S(params.str("RouteServerId")),
            );
            f.insert("SubnetId".to_string(), Fill::S(params.str("SubnetId")));
            f.insert("VpcId".to_string(), Fill::S(params.str("VpcId")));
            f.insert("EniAddress".to_string(), Fill::S("10.0.0.5".into()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "route-server-peer" => {
            f.insert("RouteServerPeerId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "RouteServerEndpointId".to_string(),
                Fill::S(params.str("RouteServerEndpointId")),
            );
            f.insert(
                "PeerAddress".to_string(),
                Fill::S(params.str("PeerAddress")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "vpn-concentrator" => {
            f.insert("VpnConcentratorId".to_string(), Fill::S(id.to_string()));
            f.insert(
                "Type".to_string(),
                Fill::S(match params.str("Type").as_str() {
                    "" => "generic".to_string(),
                    t => t.to_string(),
                }),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert(
                "TransitGatewayId".to_string(),
                Fill::S(params.str("TransitGatewayId")),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "secondary-network" | "secondary-subnet" => {
            if fam_id_is(kind, "secondary-network") {
                f.insert("SecondaryNetworkId".to_string(), Fill::S(id.to_string()));
            } else {
                f.insert("SecondarySubnetId".to_string(), Fill::S(id.to_string()));
                f.insert(
                    "SecondaryNetworkId".to_string(),
                    Fill::S(params.str("SecondaryNetworkId")),
                );
            }
            let net_kind = if fam_id_is(kind, "secondary-network") {
                "secondary-network"
            } else {
                "secondary-subnet"
            };
            f.insert("OwnerId".to_string(), Fill::S(account.to_string()));
            if fam_id_is(kind, "secondary-network") {
                f.insert("Type".to_string(), Fill::S("rdma".into()));
            }
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            let cidr = params.str("Ipv4CidrBlock");
            if !cidr.is_empty() {
                f.insert(
                    "Ipv4CidrBlockAssociations".to_string(),
                    Fill::X(format!(
                        "<item><associationId>{}</associationId><cidrBlock>{}</cidrBlock><state>associated</state><stateReason></stateReason></item>",
                        super::state::idhex::hex17(),
                        xml_escape(&cidr)
                    )),
                );
            }
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
            let _ = net_kind;
        }
        "capacity-manager-data-export" => {
            f.insert(
                "CapacityManagerDataExportId".to_string(),
                Fill::S(id.to_string()),
            );
        }
        "transit-gateway-metering-policy" => {
            f.insert(
                "TransitGatewayMeteringPolicyId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "TransitGatewayId".to_string(),
                Fill::S(params.str("TransitGatewayId")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "active" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-policy" => {
            f.insert("IpamPolicyId".to_string(), Fill::S(id.to_string()));
            f.insert("OwnerId".to_string(), Fill::S(account.to_string()));
            f.insert("IpamPolicyArn".to_string(), Fill::S(arn_of("ipam-policy")));
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-prefix-list-resolver" => {
            f.insert(
                "IpamPrefixListResolverId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "IpamPrefixListResolverArn".to_string(),
                Fill::S(arn_of("ipam-prefix-list-resolver")),
            );
            f.insert("OwnerId".to_string(), Fill::S(account.to_string()));
            f.insert(
                "AddressFamily".to_string(),
                Fill::S(params.str("AddressFamily")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "delete-in-progress"
                    } else {
                        "create-complete"
                    }
                    .into(),
                ),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-prefix-list-resolver-target" => {
            f.insert(
                "IpamPrefixListResolverTargetId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "IpamPrefixListResolverTargetArn".to_string(),
                Fill::S(arn_of("ipam-prefix-list-resolver-target")),
            );
            f.insert(
                "IpamPrefixListResolverId".to_string(),
                Fill::S(params.str("IpamPrefixListResolverId")),
            );
            f.insert("OwnerId".to_string(), Fill::S(account.to_string()));
            f.insert(
                "PrefixListId".to_string(),
                Fill::S(params.str("PrefixListId")),
            );
            f.insert(
                "PrefixListRegion".to_string(),
                Fill::S(params.str("PrefixListRegion")),
            );
            f.insert("TrackLatestVersion".to_string(), Fill::S("false".into()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "created" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "vpc-endpoint-connection-notification" => {
            f.insert(
                "ConnectionNotificationId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "ConnectionNotificationType".to_string(),
                Fill::S("SNS".into()),
            );
            f.insert(
                "ConnectionNotificationArn".to_string(),
                Fill::S(params.str("ConnectionNotificationArn")),
            );
            let events: Vec<String> = params
                .list("ConnectionEvent")
                .iter()
                .map(|s| xml_escape(s))
                .collect();
            if !events.is_empty() {
                f.insert("ConnectionEvents".to_string(), Fill::X(events.concat()));
            }
            f.insert(
                "ConnectionNotificationState".to_string(),
                Fill::S("Enabled".into()),
            );
        }
        "vpc-encryption-control" => {
            f.insert(
                "VpcEncryptionControlId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert("VpcId".to_string(), Fill::S(params.str("VpcId")));
            f.insert("Mode".to_string(), Fill::S("enforce".into()));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
        }
        "image-usage-report" => {
            f.insert("ReportId".to_string(), Fill::S(id.to_string()));
        }
        "tgw-route-table-announcement" => {
            f.insert(
                "TransitGatewayRouteTableAnnouncementId".to_string(),
                Fill::S(id.to_string()),
            );
            f.insert(
                "TransitGatewayRouteTableId".to_string(),
                Fill::S(params.str("TransitGatewayRouteTableId")),
            );
            f.insert(
                "AnnouncementDirection".to_string(),
                Fill::S(params.str("AnnouncementDirection")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "deleting" } else { "active" }.into()),
            );
            f.insert("CreationTime".to_string(), Fill::S(super::now_iso()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "byoip-cidr" => {
            f.insert("Cidr".to_string(), Fill::S(params.str("Cidr")));
            f.insert(
                "Description".to_string(),
                Fill::S(params.str("Description")),
            );
            f.insert(
                "State".to_string(),
                Fill::S(
                    if deleting {
                        "deprovisioned"
                    } else {
                        "provisioned"
                    }
                    .into(),
                ),
            );
        }
        "ipam-byoasn" => {
            f.insert(
                "Asn".to_string(),
                Fill::S(
                    params
                        .list("Asns")
                        .first()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| params.str("Asn")),
                ),
            );
            f.insert("IpamId".to_string(), Fill::S(params.str("IpamId")));
            f.insert(
                "State".to_string(),
                Fill::S(if deleting { "inactive" } else { "active" }.into()),
            );
        }
        "mac-modification-task" => {
            f.insert("MacModificationTaskId".to_string(), Fill::S(id.to_string()));
            f.insert("InstanceId".to_string(), Fill::S(params.str("InstanceId")));
            f.insert("Status".to_string(), Fill::S("succeeded".into()));
            f.insert("StartTime".to_string(), Fill::S(super::now_iso()));
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        _ => {
            f.insert("Id".to_string(), Fill::S(id.to_string()));
        }
    }
    if deleting {
        f.insert("Return".to_string(), Fill::S("true".into()));
    }
    f
}

/// kind comparison used inside the shared secondary-network/subnet builder.
fn fam_id_is(kind: &str, want: &str) -> bool {
    kind == want
}
