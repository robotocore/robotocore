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
            f.insert("IpamId", Fill::S(id.to_string()));
            f.insert("IpamArn", Fill::S(arn_of("ipam")));
            f.insert("OwnerId", Fill::S(account_s.clone()));
            f.insert("IpamRegion", Fill::S(region.to_string()));
            f.insert(
                "Description",
                Fill::S(params.str("Description")),
            );
            f.insert(
                "State",
                Fill::S(if deleting { "delete-in-progress" } else { "create-complete" }.into()),
            );
            f.insert("ScopeCount", Fill::S("2".into()));
            f.insert(
                "PublicDefaultScopeId",
                Fill::S(format!("ipam-scope-{}", super::state::idhex::hex17())),
            );
            f.insert(
                "PrivateDefaultScopeId",
                Fill::S(format!("ipam-scope-{}", super::state::idhex::hex17())),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-scope" => {
            f.insert("IpamScopeId", Fill::S(id.to_string()));
            f.insert("IpamScopeArn", Fill::S(arn_of("ipam-scope")));
            f.insert("IpamArn", Fill::S(arn_of("ipam")));
            f.insert("OwnerId", Fill::S(account_s.clone()));
            f.insert("IpamRegion", Fill::S(region.to_string()));
            f.insert("IpamScopeType", Fill::S("public".into()));
            f.insert(
                "Description",
                Fill::S(params.str("Description")),
            );
            f.insert("IsDefault", Fill::S("false".into()));
            f.insert("PoolCount", Fill::S("0".into()));
            f.insert(
                "State",
                Fill::S(if deleting { "delete-in-progress" } else { "create-complete" }.into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "ipam-pool" => {
            f.insert("IpamPoolId", Fill::S(id.to_string()));
            f.insert("IpamPoolArn", Fill::S(arn_of("ipam-pool")));
            f.insert("IpamScopeArn", Fill::S(arn_of("ipam-scope")));
            f.insert("IpamArn", Fill::S(arn_of("ipam")));
            f.insert("OwnerId", Fill::S(account_s.clone()));
            f.insert("IpamRegion", Fill::S(region.to_string()));
            f.insert("Locale", Fill::S(region.to_string()));
            f.insert("AddressFamily", Fill::S(params.str("AddressFamily")));
            f.insert(
                "State",
                Fill::S(if deleting { "delete-in-progress" } else { "create-complete" }.into()),
            );
            f.insert("PoolDepth", Fill::S("0".into()));
            f.insert(
                "Description",
                Fill::S(params.str("Description")),
            );
            f.insert("AutoImport", Fill::S("false".into()));
            f.insert("PubliclyAdvertisable", Fill::S("false".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "customer-gateway" => {
            f.insert("CustomerGatewayId", Fill::S(id.to_string()));
            f.insert("State", Fill::S(if deleting { "deleting" } else { "available" }.into()));
            f.insert("Type", Fill::S(params.str("Type")));
            f.insert("IpAddress", Fill::S(params.str("IpAddress")));
            f.insert(
                "BgpAsn",
                Fill::S(params.i64("BgpAsn").map(|v| v.to_string()).unwrap_or_default()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "vpn-gateway" => {
            f.insert("VpnGatewayId", Fill::S(id.to_string()));
            f.insert("State", Fill::S(if deleting { "deleting" } else { "available" }.into()));
            f.insert("Type", Fill::S(params.str("Type")));
            f.insert(
                "AmazonSideAsn",
                Fill::S(params.str("AmazonSideAsn")),
            );
            f.insert("AvailabilityZone", Fill::S(params.str("AvailabilityZone")));
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "vpn-connection" => {
            f.insert("VpnConnectionId", Fill::S(id.to_string()));
            f.insert(
                "State",
                Fill::S(if deleting { "deleting" } else { "available" }.into()),
            );
            f.insert("Type", Fill::S(params.str("Type")));
            f.insert("CustomerGatewayId", Fill::S(params.str("CustomerGatewayId")));
            f.insert("VpnGatewayId", Fill::S(params.str("VpnGatewayId")));
            f.insert(
                "TransitGatewayId",
                Fill::S(params.str("TransitGatewayId")),
            );
            f.insert("Category", Fill::S("VPN".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "traffic-mirror-filter-rule" => {
            f.insert(
                "TrafficMirrorFilterRuleId",
                Fill::S(id.to_string()),
            );
            f.insert(
                "TrafficMirrorFilterId",
                Fill::S(params.str("TrafficMirrorFilterId")),
            );
            f.insert("TrafficDirection", Fill::S(params.str("TrafficDirection")));
            f.insert(
                "RuleNumber",
                Fill::S(params
                    .get("RuleNumber")
                    .unwrap_or("1")
                    .to_string()),
            );
            f.insert("RuleAction", Fill::S(params.str("RuleAction")));
            f.insert(
                "Protocol",
                Fill::S(params
                    .get("Protocol")
                    .unwrap_or("-1")
                    .to_string()),
            );
            f.insert("DestinationCidrBlock", Fill::S(params.str("DestinationCidrBlock")));
            f.insert("SourceCidrBlock", Fill::S(params.str("SourceCidrBlock")));
            f.insert("Description", Fill::S(params.str("Description")));
        }
        "coip-pool" => {
            f.insert("PoolId", Fill::S(id.to_string()));
            f.insert("PoolArn", Fill::S(arn_of("coip-pool")));
            f.insert(
                "LocalGatewayRouteTableId",
                Fill::S(params.str("LocalGatewayRouteTableId")),
            );
        }
        "public-ipv4-pool" => {
            f.insert("PoolId", Fill::S(id.to_string()));
        }
        "instance-event-window" => {
            f.insert("InstanceEventWindowId", Fill::S(id.to_string()));
            f.insert("Name", Fill::S(params.str("Name")));
            f.insert("State", Fill::S("active".into()));
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "network-insights-path" => {
            f.insert("NetworkInsightsPathId", Fill::S(id.to_string()));
            f.insert(
                "NetworkInsightsPathArn",
                Fill::S(arn_of("network-insights-path")),
            );
            f.insert("Source", Fill::S(params.str("Source")));
            f.insert("Destination", Fill::S(params.str("Destination")));
            f.insert("Protocol", Fill::S(params.str("Protocol")));
            f.insert(
                "CreatedDate",
                Fill::S(super::now_iso()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "egress-only-igw" => {
            f.insert("EgressOnlyInternetGatewayId", Fill::S(id.to_string()));
        }
        "tgw-peering" => {
            f.insert(
                "TransitGatewayAttachmentId",
                Fill::S(id.to_string()),
            );
            f.insert("State", Fill::S("pendingAcceptance".into()));
            let tgw_id = params.str("TransitGatewayId");
            let peer_id = params.str("PeerTransitGatewayId");
            f.insert(
                "RequesterTgwInfo",
                Fill::X(format!(
                    "<item><transitGatewayId>{}</transitGatewayId><ownerId>{}</ownerId><region>{}</region></item>",
                    xml_escape(&tgw_id),
                    account,
                    region
                )),
            );
            f.insert(
                "AccepterTgwInfo",
                Fill::X(format!(
                    "<item><transitGatewayId>{}</transitGatewayId><ownerId>{}</ownerId><region>{}</region></item>",
                    xml_escape(&peer_id),
                    account,
                    params.str("PeerRegion")
                )),
            );
            f.insert(
                "Status",
                Fill::X("<item><code>pendingAcceptance</code></item>".into()),
            );
            if !tags_xml.is_empty() {
                f.insert("Tags", Fill::X(tags_xml.to_string()));
            }
        }
        "tgw-connect" => {
            f.insert(
                "TransitGatewayAttachmentId",
                Fill::S(id.to_string()),
            );
            f.insert(
                "TransportTransitGatewayAttachmentId",
                Fill::S(params.str("TransportTransitGatewayAttachmentId")),
            );
            f.insert("State", Fill::S("pending".into()));
        }
        "tgw-connect-peer" => {
            f.insert("TransitGatewayConnectPeerId", Fill::S(id.to_string()));
            f.insert(
                "TransitGatewayAttachmentId",
                Fill::S(params.str("TransitGatewayAttachmentId")),
            );
            f.insert("State", Fill::S("pending".into()));
            f.insert(
                "PeerAddress",
                Fill::S(params.str("PeerAddress")),
            );
            f.insert(
                "TransitGatewayAddress",
                Fill::S(params.str("TransitGatewayAddress")),
            );
            let cidrs: Vec<String> = params
                .list("InsideCidrBlock")
                .iter()
                .map(|s| xml_escape(s))
                .collect();
            if !cidrs.is_empty() {
                f.insert(
                    "ConnectPeerConfiguration",
                    Fill::X(format!(
                        "<item><transitGatewayAddress>{}</transitGatewayAddress><peerAddress>{}</peerAddress><bgpConfigurations/><insideCidrBlocks>{}</insideCidrBlocks></item>",
                        xml_escape(&params.str("TransitGatewayAddress")),
                        xml_escape(&params.str("PeerAddress")),
                        cidrs.concat()
                    )),
                );
            }
        }
        _ => {
            f.insert("Id", Fill::S(id.to_string()));
        }
    }
    f
}
