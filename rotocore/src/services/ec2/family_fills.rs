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
            if !tags_xml.is_empty() {
                f.insert("Tags".to_string(), Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-scope" => {
            f.insert("IpamScopeId".to_string(), Fill::S(id.to_string()));
            f.insert("IpamScopeArn".to_string(), Fill::S(arn_of("ipam-scope")));
            f.insert("IpamArn".to_string(), Fill::S(arn_of("ipam")));
            f.insert("OwnerId".to_string(), Fill::S(account_s.clone()));
            f.insert("IpamRegion".to_string(), Fill::S(region.to_string()));
            f.insert("IpamScopeType".to_string(), Fill::S("public".into()));
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
            f.insert("IpamArn".to_string(), Fill::S(arn_of("ipam")));
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
                Fill::S(
                    params
                        .i64("BgpAsn")
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                ),
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
                Fill::S(params.str("AmazonSideAsn")),
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
                Fill::S(params.str("AmazonSideAsn")),
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
                "State".to_string(),
                Fill::S(if deleting { "cancelled" } else { "active" }.into()),
            );
            f.insert(
                "TotalCapacity".to_string(),
                Fill::S(params.get("TotalTargetCapacity").unwrap_or("1").to_string()),
            );
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
        }
        "verified-access-group" => {
            f.insert("VerifiedAccessGroupId".to_string(), Fill::S(id.to_string()));
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
                Fill::X("<item><code>pending</code></item>".into()),
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
                items.push_str(&format!("<item>{}{}</item>", id, ""));
            }
            f.insert("HostIds".to_string(), Fill::X(items));
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
