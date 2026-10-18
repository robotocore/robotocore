//! EC2 handler group 3: NAT gateways, network interfaces, network ACLs,
//! DHCP options, VPC peering, VPC endpoints.
#![allow(
    clippy::format_in_format_args,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::single_match,
    clippy::single_match_else,
    clippy::map_identity
)]
#![allow(unused_variables)]

use super::handlers::{filter_passes, ok_xml, tag_candidates, wire_tags};
use super::state::{Eni, EniAttachment, NaclEntry};
use super::{ec2_error, ec2_xml, now_iso, tags_xml, with_state, Params};
use crate::proto::xml_escape;
use crate::{Request, Response};
use std::fmt::Write as _;

type P = Params;

pub fn route(req: &Request, params: &P) -> Response {
    match req.operation.as_str() {
        // NAT gateways
        "CreateNatGateway" => create_nat_gateway(req, params),
        "DeleteNatGateway" => delete_nat_gateway(req, params),
        "DescribeNatGateways" => describe_nat_gateways(req, params),
        "AssociateNatGatewayAddress" => simple_true(req, "AssociateNatGatewayAddressResponse"),
        "AssignPrivateNatGatewayAddress" => {
            simple_true(req, "AssignPrivateNatGatewayAddressResponse")
        }
        "UnassignNatGatewayAddress" => simple_true(req, "UnassignNatGatewayAddressResponse"),

        // Network interfaces
        "CreateNetworkInterface" => create_eni(req, params),
        "DeleteNetworkInterface" => delete_eni(req, params),
        "DescribeNetworkInterfaces" => describe_enis(req, params),
        "DescribeNetworkInterfaceAttribute" => describe_eni_attribute(req, params),
        "ModifyNetworkInterfaceAttribute" => modify_eni_attribute(req, params),
        "AttachNetworkInterface" => attach_eni(req, params),
        "DetachNetworkInterface" => detach_eni(req, params),
        "AssignPrivateIpAddresses" => assign_private_ips(req, params),
        "UnassignPrivateIpAddresses" => unassign_private_ips(req, params),
        "AssignIpv6Addresses" => assign_ipv6_addresses(req, params),
        "UnassignIpv6Addresses" => unassign_ipv6_addresses(req, params),

        // Network ACLs
        "CreateNetworkAcl" => create_network_acl(req, params),
        "DeleteNetworkAcl" => delete_network_acl(req, params),
        "DescribeNetworkAcls" => describe_network_acls(req, params),
        "CreateNetworkAclEntry" => create_network_acl_entry(req, params),
        "DeleteNetworkAclEntry" => delete_network_acl_entry(req, params),
        "ReplaceNetworkAclEntry" => create_network_acl_entry(req, params),
        "ReplaceNetworkAclAssociation" => replace_network_acl_association(req, params),

        // DHCP options
        "CreateDhcpOptions" => create_dhcp_options(req, params),
        "DeleteDhcpOptions" => delete_dhcp_options(req, params),
        "DescribeDhcpOptions" => describe_dhcp_options(req, params),
        "AssociateDhcpOptions" => associate_dhcp_options(req, params),

        // VPC peering
        "CreateVpcPeeringConnection" => create_peering(req, params),
        "DeleteVpcPeeringConnection" => delete_peering(req, params),
        "DescribeVpcPeeringConnections" => describe_peerings(req, params),
        "AcceptVpcPeeringConnection" => accept_peering(req, params),
        "RejectVpcPeeringConnection" => reject_peering(req, params),
        "ModifyVpcPeeringConnectionOptions" => describe_peerings(req, params),

        // VPC endpoints
        "CreateVpcEndpoint" => create_vpc_endpoint(req, params),
        "DeleteVpcEndpoints" => delete_vpc_endpoints(req, params),
        "DescribeVpcEndpoints" => describe_vpc_endpoints(req, params),
        "ModifyVpcEndpoint" => modify_vpc_endpoint(req, params),
        "DescribeVpcEndpointServices" => describe_vpc_endpoint_services(req, params),
        "CreateVpcEndpointServiceConfiguration" => create_vpc_endpoint_service(req, params),
        "DeleteVpcEndpointServiceConfigurations" => delete_vpc_endpoint_service(req, params),
        "DescribeVpcEndpointServiceConfigurations" => {
            describe_vpc_endpoint_services_cfg(req, params)
        }

        _ => super::handlers_compute::route(req, params),
    }
}

pub fn simple_true(req: &Request, root: &str) -> Response {
    ec2_xml(root, "<return>true</return>")
}

// ---------------------------------------------------------------------------
// NAT gateways
// ---------------------------------------------------------------------------

fn require_nat(state: &super::state::Ec2State, id: &str) -> Result<usize, Response> {
    match state.nat_gateways.iter().position(|g| g.id == id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidNatGatewayID.NotFound",
            &format!("The natGateway ID '{}' does not exist", xml_escape(id)),
        )),
    }
}

fn nat_item(g: &super::state::NatGateway) -> String {
    let mut addrs = String::new();
    let _ = write!(
        addrs,
        "<item><allocationId></allocationId><networkInterfaceId></networkInterfaceId><privateIp></privateIp><publicIp></publicIp>"
    );
    let _ = write!(addrs, "<status>assigned</status>");
    addrs.push_str("</item>");
    let _ = addrs;
    format!(
        "<item><natGatewayId>{}</natGatewayId>{}{}<vpcId>{}</vpcId><state>{}</state><createTime>{}</createTime><natGatewayAddresses/>{}</item>",
        xml_escape(&g.id),
        if g.subnet_id.is_empty() { String::new() } else { format!("<subnetId>{}</subnetId>", xml_escape(&g.subnet_id)) },
        if g.connectivity_type.is_empty() { String::new() } else { format!("<connectivityType>{}</connectivityType>", xml_escape(&g.connectivity_type)) },
        xml_escape(&g.vpc_id),
        xml_escape(&g.state),
        xml_escape(&g.create_time),
        tags_xml(&g.tags)
    )
}

fn create_nat_gateway(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    let connectivity = params
        .get("ConnectivityType")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "public".to_string());
    with_state(req, move |state| {
        let id = state.next_id("nat");
        let tags = wire_tags(params);
        state.nat_gateways.push(super::state::NatGateway {
            id: id.clone(),
            state: "pending".into(),
            subnet_id: subnet_id.clone(),
            vpc_id: state
                .subnets
                .iter()
                .find(|s| s.id == subnet_id)
                .map(|s| s.vpc_id.clone())
                .unwrap_or_default(),
            create_time: now_iso(),
            connectivity_type: connectivity,
            tags,
        });
        ec2_xml(
            "CreateNatGatewayResponse",
            &format!(
                "<natGateway><natGatewayId>{}</natGatewayId><subnetId>{}</subnetId><state>pending</state><createTime>{}</createTime>{}</natGateway><clientToken/>",
                xml_escape(&id),
                xml_escape(&subnet_id),
                super::now_iso(),
                ""
            ),
        )
    })
}

fn delete_nat_gateway(req: &Request, params: &P) -> Response {
    let id = params.str("NatGatewayId");
    with_state(req, move |state| {
        if require_nat(state, &id).is_err() {
            return require_nat(state, &id).err().unwrap();
        }
        let pos = state
            .nat_gateways
            .iter()
            .position(|g| g.id == id)
            .unwrap_or(0);
        state.nat_gateways[pos].state = "deleting".into();
        state.nat_gateways.remove(pos);
        ec2_xml(
            "DeleteNatGatewayResponse",
            &format!("<natGatewayId>{}</natGatewayId>", xml_escape(&id)),
        )
    })
}

fn describe_nat_gateways(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("NatGatewayId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    let _ = fs;
    let names = params.list("Filter");
    with_state(req, move |state| {
        let ids = ids.clone();
        let mut items = String::new();
        for g in &state.nat_gateways {
            if !ids.is_empty() && !ids.contains(&g.id) {
                continue;
            }
            items.push_str(&nat_item(g));
        }
        ec2_xml(
            "DescribeNatGatewaysResponse",
            &format!("<natGatewaySet>{}</natGatewaySet>", items),
        )
    })
}

// ---------------------------------------------------------------------------
// ENIs
// ---------------------------------------------------------------------------

fn require_eni(state: &super::state::Ec2State, id: &str) -> Result<usize, Response> {
    match state.enis.iter().position(|e| e.id == id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidNetworkInterfaceID.NotFound",
            &format!("The interface ID '{}' does not exist", xml_escape(id)),
        )),
    }
}

pub fn eni_item(region: &str, account: u64, e: &Eni) -> String {
    let mut groups = String::new();
    for (gid, name) in &e.groups {
        let _ = write!(
            groups,
            "<item><groupId>{}</groupId><groupName>{}</groupName></item>",
            crate::proto::xml_escape(gid),
            crate::proto::xml_escape(name)
        );
    }
    let attachment = if let Some(a) = &e.attached {
        format!(
            "<attachTime>{}</attachTime><attachmentId>{}</attachmentId><deleteOnTermination>{}</deleteOnTermination><deviceIndex>{}</deviceIndex><status>{}</status>",
            crate::proto::xml_escape(&a.attach_time),
            crate::proto::xml_escape(&a.attachment_id),
            a.delete_on_termination,
            a.device_index,
            crate::proto::xml_escape(&a.status)
        )
    } else {
        String::new()
    };
    let mac = if e.mac_address.is_empty() {
        "02:00:00:00:00:00".to_string()
    } else {
        e.mac_address.clone()
    };
    let mut privat = e.private_ip.clone();
    if privat.is_empty() {
        privat = "10.0.0.10".to_string();
    }
    let sub_addr = "ip-10-0-0-10.ec2.internal";
    format!(
        "<item><groupName>default</groupName>{}<description>{}</description><ownerId>{}</ownerId>{}<privateDnsName>ip-{}.ec2.internal</privateDnsName><requesterId>08672-1917129</requesterId><requesterManaged>{}</requesterManaged><macAddress>{}</macAddress><networkInterfaceId>{}</networkInterfaceId>{}<privateIpAddress>{}</privateIpAddress><sourceDestCheck>{}</sourceDestCheck>{}<subnetId>{}</subnetId><vpcId>{}</vpcId>{}</item>",
        if !e.groups.is_empty() || e.interface_type == "interface" { String::new() } else { String::new() },
        crate::proto::xml_escape(&e.description),
        e.owner_id,
        attachment,
        uuid::Uuid::new_v4(),
        e.requester_managed,
        crate::proto::xml_escape(&mac),
        crate::proto::xml_escape(&e.id),
        if !e.groups.is_empty() {
            format!("<groupSet>{}</groupSet>", groups)
        } else {
            String::new()
        },
        crate::proto::xml_escape(&privat),
        e.source_dest_check,
        tags_xml(&e.tags),
        crate::proto::xml_escape(&e.subnet_id),
        crate::proto::xml_escape(&e.vpc_id),
        String::new()
    )
}

fn create_eni(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    let description = params.str("Description");
    let private_ip = params.str("PrivateIpAddress");
    with_state(req, move |state| {
        let subnet = match state.subnets.iter().find(|s| s.id == subnet_id).cloned() {
            Some(s) => s,
            None => {
                return ec2_error(
                    400,
                    "InvalidSubnetID.NotFound",
                    &format!(
                        "The subnet ID '{}' does not exist",
                        crate::proto::xml_escape(&subnet_id)
                    ),
                )
            }
        };
        let id = state.next_id("eni");
        let groups: Vec<(String, String)> = (0..params.count("SecurityGroupId"))
            .map(|i| {
                let gid = params.list("SecurityGroupId")[i].to_string();
                let gname = state
                    .security_groups
                    .iter()
                    .find(|sg| sg.id == gid)
                    .map(|sg| sg.name.clone())
                    .unwrap_or_default();
                (gid, gname)
            })
            .collect();
        let mut eni = Eni {
            id: id.clone(),
            subnet_id: subnet.id.clone(),
            vpc_id: subnet.vpc_id.clone(),
            description: description.clone(),
            owner_id: req.account.to_string(),
            status: "available".into(),
            mac_address: format!("02:00:00:{}", crate::proto::xml_escape("")),
            private_ip: private_ip.clone(),
            groups,
            interface_type: "interface".into(),
            source_dest_check: true,
            ..Default::default()
        };
        if eni.private_ip.is_empty() {
            let n = uuid::Uuid::new_v4().as_u128();
            eni.private_ip = format!(
                "10.{}.{}.{}",
                (n >> 24) % 250,
                (n >> 16) % 250,
                (n >> 8) % 250
            );
        }
        if eni.mac_address.is_empty() || params.has("_") {
            // derive a stable-looking mac from the uuid
            let n = uuid::Uuid::new_v4().as_u128();
            eni.mac_address = format!(
                "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                (n >> 40) & 0xff,
                (n >> 32) & 0xff,
                (n >> 24) & 0xff,
                (n >> 16) & 0xff,
                (n >> 8) & 0xff
            );
        }
        let tags = wire_tags(params);
        if !tags.is_empty() {
            eni.tags = tags;
        }
        state.enis.push(eni.clone());
        ec2_xml(
            "CreateNetworkInterfaceResponse",
            &format!(
                "<networkInterface>{}</networkInterface>",
                eni_item(&req.region, req.account, &eni)
            ),
        )
    })
}

fn delete_eni(req: &Request, params: &P) -> Response {
    let id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        if require_eni(state, &id).is_err() {
            return require_eni(state, &id).err().unwrap();
        }
        let pos = state.enis.iter().position(|e| e.id == id).unwrap_or(0);
        if state.enis[pos].attached.is_some() {
            return ec2_error(
                400,
                "InvalidNetworkInterfaceID.InUse",
                "The network interface is currently in use",
            );
        }
        state.enis.remove(pos);
        ok_xml("DeleteNetworkInterfaceResponse")
    })
}

fn describe_enis(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("NetworkInterfaceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let snapshot: Vec<Eni> = state.enis.clone();
        let mut items = String::new();
        for e in &snapshot {
            if !ids.is_empty() && !ids.contains(&e.id) {
                continue;
            }
            let e2 = e.clone();
            let _ = e2;
            if !filter_passes(&fs, move |f| match f {
                "network-interface-id" | "eni-id" => vec![e.id.clone()],
                "subnet-id" => vec![e.subnet_id.clone()],
                "vpc-id" => vec![e.vpc_id.clone()],
                "description" => vec![e.description.clone()],
                "status" => vec![e.status.clone()],
                "interface-type" => vec![e.interface_type.clone()],
                "attachment.instance-id" => e
                    .attached
                    .as_ref()
                    .map(|a| vec![a.instance_id.clone()])
                    .unwrap_or_default(),
                "group-id" | "security-group-id" => {
                    e.groups.iter().map(|(g, _)| g.clone()).collect()
                }
                "private-ip-address" if !e.private_ip.is_empty() => vec![e.private_ip.clone()],
                other => tag_candidates(&e.tags, other),
            }) {
                continue;
            }
            items.push_str(&eni_item(req.region.as_str(), req.account, e));
        }
        ec2_xml(
            "DescribeNetworkInterfacesResponse",
            &format!("<networkInterfaceSet>{}</networkInterfaceSet>", items),
        )
    })
}

fn describe_eni_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        let pos = match require_eni(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let e = state.enis[pos].clone();
        let attr = params.str("Attribute");
        let mut body = String::new();
        let _ = write!(
            body,
            "<networkInterfaceId>{}</networkInterfaceId>",
            crate::proto::xml_escape(&id)
        );
        let _ = &attr;
        let _ = write!(
            body,
            "<description>{}</description>",
            crate::proto::xml_escape(&e.description)
        );
        ec2_xml("DescribeNetworkInterfaceAttributeResponse", &body)
    })
}

fn modify_eni_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        let pos = match require_eni(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let e = &mut state.enis[pos];
        if let Some(b) = params.bool("SourceDestCheck.Value") {
            e.source_dest_check = b;
        }
        if let Some(d) = params.get("Description.Value") {
            e.description = d.to_string();
        }
        ok_xml("ModifyNetworkInterfaceAttributeResponse")
    })
}

fn attach_eni(req: &Request, params: &P) -> Response {
    let eni_id = params.str("NetworkInterfaceId");
    let instance_id = params.str("InstanceId");
    let device_index = params.i64("DeviceIndex").unwrap_or(-1);
    with_state(req, move |state| {
        if require_eni(state, &eni_id).is_err() {
            return require_eni(state, &eni_id).err().unwrap();
        }
        let epos = state.enis.iter().position(|e| e.id == eni_id).unwrap_or(0);
        let instance_exists = state.instances.iter().any(|i| i.id == instance_id);
        if !instance_exists {
            return ec2_error(
                400,
                "InvalidInstanceId.NotFound",
                &format!(
                    "The instance ID '{}' does not exist",
                    crate::proto::xml_escape(&instance_id)
                ),
            );
        }
        state.enis[epos].status = "in-use".into();
        let attachment_id = state.next_id("eni-attach");
        state.enis[epos].attached = Some(EniAttachment {
            attachment_id: attachment_id.clone(),
            instance_id: instance_id.clone(),
            device_index: device_index as i32,
            status: "attached".into(),
            delete_on_termination: false,
            attach_time: now_iso(),
        });
        ec2_xml(
            "AttachNetworkInterfaceResponse",
            &format!(
                "<attachmentId>{}</attachmentId><networkCardIndex>0</networkCardIndex>",
                crate::proto::xml_escape(&attachment_id)
            ),
        )
    })
}

fn detach_eni(req: &Request, params: &P) -> Response {
    let attachment_id = params.str("AttachmentId");
    with_state(req, move |state| {
        let pos = state.enis.iter().position(|e| {
            e.attached
                .as_ref()
                .map(|a| a.attachment_id == attachment_id)
                .unwrap_or(false)
        });
        let pos = match pos {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidAttachmentID.NotFound",
                    &format!(
                        "The attachment ID '{}' does not exist",
                        crate::proto::xml_escape(&attachment_id)
                    ),
                )
            }
        };
        state.enis[pos].attached = None;
        state.enis[pos].status = "available".into();
        ok_xml("DetachNetworkInterfaceResponse")
    })
}

fn assign_private_ips(req: &Request, params: &P) -> Response {
    let eni_id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        if require_eni(state, &eni_id).is_err() {
            return require_eni(state, &eni_id).err().unwrap();
        }
        let asked = params.i64("SecondaryPrivateIpAddressCount").unwrap_or(1);
        let mut assigned = Vec::new();
        let pos = state.enis.iter().position(|e| e.id == eni_id).unwrap_or(0);
        for _ in 0..asked {
            let n = uuid::Uuid::new_v4().as_u128();
            assigned.push(format!(
                "10.{}.{}.{}",
                (n >> 24) % 250,
                (n >> 16) % 250,
                (n >> 8) % 250
            ));
        }
        for ip in &assigned {
            state.enis[pos].secondary_private_ips.push(ip.clone());
        }
        let mut body = String::new();
        for ip in &assigned {
            let _ = write!(
                body,
                "<item><privateIpAddress>{}</privateIpAddress></item>",
                ip
            );
        }
        ec2_xml("AssignPrivateIpAddressesResponse", &body)
    })
}

fn unassign_private_ips(req: &Request, params: &P) -> Response {
    let eni_id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        if require_eni(state, &eni_id).is_err() {
            return require_eni(state, &eni_id).err().unwrap();
        }
        let pos = state.enis.iter().position(|e| e.id == eni_id).unwrap_or(0);
        let ips: Vec<String> = params
            .list("PrivateIpAddress")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let before = state.enis[pos].secondary_private_ips.len();
        state.enis[pos]
            .secondary_private_ips
            .retain(|ip| !ips.contains(ip));
        if before != state.enis[pos].secondary_private_ips.len() {
            ok_xml("UnassignPrivateIpAddressesResponse")
        } else {
            ec2_error(
                400,
                "InvalidParameterValue",
                "The address to be unassigned is not assigned to this interface",
            )
        }
    })
}

fn assign_ipv6_addresses(req: &Request, params: &P) -> Response {
    let eni_id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        if require_eni(state, &eni_id).is_err() {
            return require_eni(state, &eni_id).err().unwrap();
        }
        let asked = params.list("Ipv6Address").len();
        if asked == 0 {
            return ec2_error(
                400,
                "InvalidParameterValue",
                "The parameter must be an IPv6 address or an Ipv6Prefix or Ipv6AddressCount",
            );
        }
        let mut out = String::new();
        let pos = state.enis.iter().position(|e| e.id == eni_id).unwrap_or(0);
        for ip in params.list("Ipv6Address") {
            let v = ip.to_string();
            state.enis[pos].ipv6_addresses.push(v.clone());
            let _ = write!(out, "<item><ipv6Address>{}</ipv6Address></item>", v);
        }
        ec2_xml(
            "AssignIpv6AddressesResponse",
            &format!("<assignedIpv6Addresses>{}</assignedIpv6Addresses>", out),
        )
    })
}

fn unassign_ipv6_addresses(req: &Request, params: &P) -> Response {
    let eni_id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        let pos = state.enis.iter().position(|e| e.id == eni_id).map(|i| i);
        let _ = pos;
        if state.enis.iter().all(|e| e.id != eni_id) {
            return ec2_error(
                400,
                "InvalidNetworkInterfaceID.NotFound",
                &format!(
                    "The interface ID '{}' does not exist",
                    crate::proto::xml_escape(&eni_id)
                ),
            );
        }
        ok_xml("UnassignIpv6AddressesResponse")
    })
}

// ---------------------------------------------------------------------------
// Network ACLs
// ---------------------------------------------------------------------------

fn create_network_acl(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        if !state.vpcs.iter().any(|v| v.id == vpc_id) {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", crate::proto::xml_escape(&vpc_id)),
            );
        }
        let id = state.next_id("acl");
        let tags = wire_tags(params);
        let mut acl = super::state::NetworkAcl {
            id: id.clone(),
            vpc_id: vpc_id.clone(),
            entries: vec![
                NaclEntry {
                    rule_number: 100,
                    protocol: "-1".into(),
                    rule_action: "allow".into(),
                    egress: false,
                    cidr: "0.0.0.0/0".into(),
                    port_range: None,
                    icmp: None,
                },
                NaclEntry {
                    rule_number: 32766,
                    protocol: "-1".into(),
                    rule_action: "deny".into(),
                    egress: false,
                    cidr: "0.0.0.0/0".into(),
                    port_range: None,
                    icmp: None,
                },
                NaclEntry {
                    rule_number: 32767,
                    protocol: "-1".into(),
                    rule_action: "deny".into(),
                    egress: true,
                    cidr: "0.0.0.0/0".into(),
                    port_range: None,
                    icmp: None,
                },
            ],
            ..Default::default()
        };
        if !tags.is_empty() {
            acl.tags = tags;
        }
        state.network_acls.push(acl);
        ec2_xml(
            "CreateNetworkAclResponse",
            &format!(
                "<networkAcl><networkAclId>{}</networkAclId><vpcId>{}</vpcId><isDefault>false</isDefault><entrySet/><associationSet/>{}</networkAcl>",
                crate::proto::xml_escape(&id),
                crate::proto::xml_escape(&vpc_id),
                ""
            ),
        )
    })
}

fn delete_network_acl(req: &Request, params: &P) -> Response {
    let id = params.str("NetworkAclId");
    with_state(req, move |state| {
        let pos = match state.network_acls.iter().position(|a| a.id == id) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidNetworkAclID.NotFound",
                    &format!(
                        "The network ACL '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                )
            }
        };
        if state.network_acls[pos].is_default {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                "The network ACL is the default one and cannot be deleted",
            );
        }
        state.network_acls.remove(pos);
        ok_xml("DeleteNetworkAclResponse")
    })
}

fn describe_network_acls(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("NetworkAclId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let ids = ids.clone();
        let mut items = String::new();
        for a in &state.network_acls {
            if !ids.is_empty() && !ids.contains(&a.id) {
                continue;
            }
            let mut entries = String::new();
            for e in &a.entries {
                let _ = write!(
                    entries,
                    "<item><ruleNumber>{}</ruleNumber><protocol>{}</protocol><ruleAction>{}</ruleAction><egress>{}</egress><cidrBlock>{}</cidrBlock></item>",
                    e.rule_number,
                    crate::proto::xml_escape(&e.protocol),
                    crate::proto::xml_escape(&e.rule_action),
                    e.egress,
                    crate::proto::xml_escape(&e.cidr)
                );
            }
            let mut associations = String::new();
            for (assoc_id, subnet_id) in &a.associations {
                let _ = write!(
                    associations,
                    "<item><networkAclAssociationId>{}</networkAclAssociationId><networkAclId>{}</networkAclId><subnetId>{}</subnetId></item>",
                    crate::proto::xml_escape(assoc_id),
                    crate::proto::xml_escape(&a.id),
                    crate::proto::xml_escape(subnet_id)
                );
            }
            let _ = write!(
                items,
                "<item><networkAclId>{}</networkAclId><vpcId>{}</vpcId><isDefault>{}</isDefault><entrySet>{}</entrySet><associationSet>{}</associationSet>{}</item>",
                crate::proto::xml_escape(&a.id),
                crate::proto::xml_escape(&a.vpc_id),
                a.is_default,
                entries,
                associations,
                tags_xml(&a.tags)
            );
        }
        ec2_xml(
            "DescribeNetworkAclsResponse",
            &format!("<networkAclSet>{}</networkAclSet>", items),
        )
    })
}

fn create_network_acl_entry(req: &Request, params: &P) -> Response {
    let acl_id = params.str("NetworkAclId");
    with_state(req, move |state| {
        let pos = match state.network_acls.iter().position(|a| a.id == acl_id) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidNetworkAclID.NotFound",
                    &format!(
                        "The network ACL '{}' does not exist",
                        crate::proto::xml_escape(&acl_id)
                    ),
                )
            }
        };
        let entry = NaclEntry {
            rule_number: params.i64("RuleNumber").unwrap_or(0),
            protocol: params
                .get("Protocol")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "all".into()),
            rule_action: params
                .get("RuleAction")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "allow".into()),
            egress: params.bool("Egress").unwrap_or(false),
            cidr: params
                .get("CidrBlock")
                .map(|s| s.to_string())
                .unwrap_or_default(),
            port_range: None,
            icmp: None,
        };
        state.network_acls[pos].entries.push(entry);
        ok_xml("CreateNetworkAclEntryResponse")
    })
}

fn delete_network_acl_entry(req: &Request, params: &P) -> Response {
    let acl_id = params.str("NetworkAclId");
    with_state(req, move |state| {
        if !state.network_acls.iter().any(|a| a.id == acl_id) {
            return ec2_error(
                400,
                "InvalidNetworkAclID.NotFound",
                &format!(
                    "The network ACL '{}' does not exist",
                    crate::proto::xml_escape(&acl_id)
                ),
            );
        }
        let rule_number = params.i64("RuleNumber").unwrap_or(-1);
        let egress = params.bool("Egress").unwrap_or(false);
        let pos = state
            .network_acls
            .iter()
            .position(|a| a.id == acl_id)
            .unwrap_or(0);
        state.network_acls[pos]
            .entries
            .retain(|e| !(e.rule_number == rule_number && e.egress == egress));
        ok_xml("DeleteNetworkAclEntryResponse")
    })
}

fn replace_network_acl_association(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    let acl_id = params.str("NetworkAclId");
    with_state(req, move |state| {
        // find & move
        let mut target_subnet: Option<String> = None;
        for a in state.network_acls.iter() {
            for (a_id, sub_id) in &a.associations {
                if a_id == &assoc_id {
                    target_subnet = Some(sub_id.clone());
                }
            }
        }
        let target_subnet = match target_subnet {
            Some(v) => v,
            None => {
                return ec2_error(
                    400,
                    "InvalidAssociationID.NotFound",
                    &format!(
                        "The association ID '{}' does not exist",
                        crate::proto::xml_escape(&assoc_id)
                    ),
                )
            }
        };
        for a in state.network_acls.iter_mut() {
            a.associations.retain(|(id, _)| id != &assoc_id);
        }
        let pos = state
            .network_acls
            .iter()
            .position(|a| a.id == acl_id)
            .unwrap_or(0);
        let new_assoc = state.next_id("aclassoc");
        state.network_acls[pos]
            .associations
            .push((new_assoc.clone(), target_subnet.clone()));
        ec2_xml(
            "ReplaceNetworkAclAssociationResponse",
            &format!(
                "<newAssociationId>{}</newAssociationId>",
                crate::proto::xml_escape(&new_assoc)
            ),
        )
    })
}

// ---------------------------------------------------------------------------
// DHCP options
// ---------------------------------------------------------------------------

fn dhc_item(d: &super::state::DhcpOptions) -> String {
    let mut configs = String::new();
    for (key, values) in &d.configurations {
        let mut vals = String::new();
        for v in values {
            let _ = write!(vals, "<value>{}</value>", crate::proto::xml_escape(v));
        }
        let _ = write!(
            configs,
            "<item><key>{}</key><values>{}</values></item>",
            crate::proto::xml_escape(key),
            vals
        );
    }
    format!(
        "<item><dhcpOptionsId>{}</dhcpOptionsId>{}{}</item>",
        crate::proto::xml_escape(&d.id),
        if configs.is_empty() {
            String::new()
        } else {
            format!("<dhcpConfiguration>{}</dhcpConfiguration>", configs)
        },
        tags_xml(&d.tags)
    )
}

fn create_dhcp_options(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        let id = state.next_id("dopt");
        let mut configs: Vec<(String, Vec<String>)> = Vec::new();
        for i in params.container_indices("".trim()) {
            let _ = i;
        }
        let keys: &[&str] = &[
            "domain-name-servers",
            "domain-name",
            "ntp-servers",
            "netbios-name-servers",
        ];
        let wire_keys: &[&str] = &[
            "DomainNameServers",
            "DomainName",
            "NtpServers",
            "NetbiosNameServers",
        ];
        for (i, wk) in wire_keys.iter().enumerate() {
            let vals: Vec<String> = params.list(wk).iter().map(|s| s.to_string()).collect();
            if !vals.is_empty() {
                configs.push((keys[i].to_string(), vals));
            }
        }
        let tags = wire_tags(params);
        let mut d = super::state::DhcpOptions {
            id: id.clone(),
            configurations: configs,
            ..Default::default()
        };
        if !tags.is_empty() {
            d.tags = tags;
        }
        state.dhcp_options.push(d);
        ec2_xml(
            "CreateDhcpOptionsResponse",
            &format!(
                "<dhcpOptions><dhcpOptionsId>{}</dhcpOptionsId><dhcpConfiguration></dhcpConfiguration></dhcpOptions>",
                crate::proto::xml_escape(&id)
            ),
        )
    })
}

fn delete_dhcp_options(req: &Request, params: &P) -> Response {
    let id = params.str("DhcpOptionsId");
    with_state(req, move |state| {
        let pos = match state.dhcp_options.iter().position(|d| d.id == id) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidDhcpOptionID.NotFound",
                    &format!("Unknowndhcp options set: {}", crate::proto::xml_escape(&id)),
                )
            }
        };
        state.dhcp_options.remove(pos);
        ok_xml("DeleteDhcpOptionsResponse")
    })
}

fn describe_dhcp_options(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("DhcpOptionsId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let ids = ids.clone();
        let items: Vec<String> = state
            .dhcp_options
            .iter()
            .filter(|d| ids.is_empty() || ids.contains(&d.id))
            .map(dhc_item)
            .collect();
        ec2_xml(
            "DescribeDhcpOptionsResponse",
            &format!("<dhcpOptionsSet>{}</dhcpOptionsSet>", items.concat()),
        )
    })
}

fn associate_dhcp_options(req: &Request, params: &P) -> Response {
    let dopt_id = params.str("DhcpOptionsId");
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        if require_vpc_id(state, &vpc_id).is_err() {
            return require_vpc_id(state, &vpc_id).err().unwrap();
        }
        if dopt_id != "default" && !state.dhcp_options.iter().any(|d| d.id == dopt_id) {
            return ec2_error(
                400,
                "InvalidDhcpOptionID.NotFound",
                &format!(
                    "Unknown dhcp options set: {}",
                    crate::proto::xml_escape(&dopt_id)
                ),
            );
        }
        let pos = state.vpcs.iter().position(|v| v.id == vpc_id).unwrap_or(0);
        state.vpcs[pos].dhcp_options_id = dopt_id.clone();
        ok_xml("AssociateDhcpOptionsResponse")
    })
}

fn require_vpc_id(state: &super::state::Ec2State, vpc_id: &str) -> Result<(), Response> {
    if state.vpcs.iter().any(|v| v.id == vpc_id) {
        Ok(())
    } else {
        Err(ec2_error(
            400,
            "InvalidVpcID.NotFound",
            &format!("Vpc {{{}}} not found", crate::proto::xml_escape(vpc_id)),
        ))
    }
}

// ---------------------------------------------------------------------------
// VPC peering
// ---------------------------------------------------------------------------

fn peering_item(p: &super::state::VpcPeering) -> String {
    format!(
        "<item><requesterVpcInfo><ownerId>123456789012</ownerId><vpcId>{}</vpcId><region>us-east-1</region></requesterVpcInfo><accepterVpcInfo>{}<ownerId>123456789012</ownerId><vpcId>{}</vpcId><region>us-east-1</region></accepterVpcInfo>{}<sourceVpcInfo>{}<ownerId></ownerId><vpcId></vpcId></sourceVpcInfo><expiration>2026-10-25T00:00:00.000Z</expiration><vpcPeeringConnectionId>{}</vpcPeeringConnectionId>{}{}</item>",
        crate::proto::xml_escape(&p.requester_vpc_id),
        if p.accepter_dns { "" } else { "" },
        crate::proto::xml_escape(&p.accepter_vpc_id),
        tags_xml(&p.tags),
        "",
        crate::proto::xml_escape(&p.id),
        if p.status_message.is_some() {
            format!("<statusMessage>{}</statusMessage>", crate::proto::xml_escape(p.status_message.clone().unwrap_or_default().as_str()))
        } else {
            String::new()
        },
        ""
    )
}

fn create_peering(req: &Request, params: &P) -> Response {
    let requester = params
        .get("VpcId")
        .or_else(|| params.get("RequesterVpcId"))
        .map(|s| s.to_string())
        .unwrap_or_default();
    let accepter = params.str("PeerVpcId");
    with_state(req, move |state| {
        if !state.vpcs.iter().any(|v| v.id == requester) {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", crate::proto::xml_escape(&requester)),
            );
        }
        if !state.vpcs.iter().any(|v| v.id == accepter) {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", crate::proto::xml_escape(&accepter)),
            );
        }
        let id = state.next_id("pcx");
        let tags = wire_tags(params);
        let mut p = super::state::VpcPeering {
            id: id.clone(),
            requester_vpc_id: requester,
            accepter_vpc_id: accepter,
            state: "pending-acceptance".into(),
            state_code: 713,
            status_message: Some("Pending Acceptance by ".to_string()),
            tags,
            expiration: "3000-01-01T00:00:00.000Z".into(),
            ..Default::default()
        };
        p.status_message = Some("Pending Acceptance by 123456789012".to_string());
        state.vpc_peerings.push(p);
        ec2_xml(
            "CreateVpcPeeringConnectionResponse",
            &format!(
                "<vpcPeeringConnection><requesterVpcInfo><ownerId>{}</ownerId><vpcId>{}</vpcId><region>{}</region></requesterVpcInfo><accepterVpcInfo><ownerId>{}</ownerId><vpcId>{}</vpcId><region>{}</region></accepterVpcInfo><status><code>pending-acceptance</code></status><sourceVpcInfo><ownerId></ownerId><vpcId></vpcId></sourceVpcInfo><expiration>3000-01-01T00:00:00.000Z</expiration><vpcPeeringConnectionId>{}</vpcPeeringConnectionId></vpcPeeringConnection>",
                req.account,
                "",
                req.region,
                req.account,
                "",
                req.region,
                crate::proto::xml_escape(&id)
            ),
        )
    })
}

fn find_peering(state: &super::state::Ec2State, id: &str) -> Option<usize> {
    state.vpc_peerings.iter().position(|p| p.id == id)
}

fn delete_peering(req: &Request, params: &P) -> Response {
    let id = params.str("VpcPeeringConnectionId");
    with_state(req, move |state| {
        match find_peering(state, &id) {
            Some(pos) => {
                state.vpc_peerings.remove(pos);
            }
            None => {
                return ec2_error(
                    400,
                    "InvalidVpcPeeringConnectionId.NotFound",
                    &format!(
                        "The vpc peering connection '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                )
            }
        }
        ok_xml("DeleteVpcPeeringConnectionResponse")
    })
}

fn describe_peerings(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("VpcPeeringConnectionId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let ids = ids.clone();
        let items: Vec<String> = state
            .vpc_peerings
            .iter()
            .filter(|p| ids.is_empty() || ids.contains(&p.id))
            .map(peering_item)
            .collect();
        ec2_xml(
            "DescribeVpcPeeringConnectionsResponse",
            &format!(
                "<vpcPeeringConnectionSet>{}</vpcPeeringConnectionSet>",
                items.concat()
            ),
        )
    })
}

fn accept_peering(req: &Request, params: &P) -> Response {
    let id = params.str("VpcPeeringConnectionId");
    with_state(req, move |state| {
        match find_peering(state, &id) {
            Some(pos) => {
                state.vpc_peerings[pos].state = "active".into();
                state.vpc_peerings[pos].state_code = 716;
            }
            None => {
                return ec2_error(
                    404,
                    "InvalidVpcPeeringConnectionId.NotFound",
                    &format!(
                        "The vpc peering connection '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                )
            }
        }
        ec2_xml(
            "AcceptVpcPeeringConnectionResponse",
            &format!(
                "<vpcPeeringConnection><vpcPeeringConnectionId>{}</vpcPeeringConnectionId><status><code>active</code></status><requesterVpcInfo><ownerId>123456789012</ownerId><vpcId></vpcId><region>us-east-1</region></requesterVpcInfo><accepterVpcInfo><ownerId>123456789012</ownerId><vpcId></vpcId><region>us-east-1</region></accepterVpcInfo>{}</vpcPeeringConnection>",
                crate::proto::xml_escape(&id),
                ""
            ),
        )
    })
}

fn reject_peering(req: &Request, params: &P) -> Response {
    let id = params.str("VpcPeeringConnectionId");
    with_state(req, move |state| {
        match find_peering(state, &id) {
            Some(pos) => {
                state.vpc_peerings[pos].state = "rejected".into();
                state.vpc_peerings[pos].state_code = 714;
            }
            None => {
                return ec2_error(
                    404,
                    "InvalidVpcPeeringConnectionId.NotFound",
                    &format!(
                        "The vpc peering connection '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                )
            }
        }
        ec2_xml(
            "RejectVpcPeeringConnectionResponse",
            &format!(
                "<vpcPeeringConnection><vpcPeeringConnectionId>{}</vpcPeeringConnectionId><status><code>rejected</code></status>{}</vpcPeeringConnection>",
                crate::proto::xml_escape(&id),
                ""
            ),
        )
    })
}

// ---------------------------------------------------------------------------
// VPC endpoints
// ---------------------------------------------------------------------------

fn endpoint_item(e: &super::state::VpcEndpoint) -> String {
    let mut rts = String::new();
    for rt in &e.route_table_ids {
        rts.push_str(&format!(
            "<item><routeTableId>{}</routeTableId></item>",
            crate::proto::xml_escape(rt)
        ));
    }
    let mut subs = String::new();
    for s in &e.subnet_ids {
        subs.push_str(&format!(
            "<item><subnetId>{}</subnetId></item>",
            crate::proto::xml_escape(s)
        ));
    }
    let mut sgs = String::new();
    for s in &e.security_group_ids {
        sgs.push_str(&format!(
            "<item><groupId>{}</groupId></item>",
            crate::proto::xml_escape(s)
        ));
    }
    format!(
        "<item><vpcEndpointId>{}</vpcEndpointId>{}<vpcId>{}</vpcId><serviceName>{}</serviceName>{}<state>{}</state>{}<policyDocument></policyDocument>{}<requesterManaged>false</requesterManaged>{}{}{}</item>",
        crate::proto::xml_escape(&e.id),
        if e.endpoint_type.is_empty() { String::new() } else { format!("<vpcEndpointType>{}</vpcEndpointType>", crate::proto::xml_escape(&e.endpoint_type)) },
        crate::proto::xml_escape(&e.vpc_id),
        crate::proto::xml_escape(&e.service_name),
        if rts.is_empty() { String::new() } else { format!("<routeTableIds>{}</routeTableIds>", rts) },
        crate::proto::xml_escape(&e.state),
        if !e.subnet_ids.is_empty() {
            format!("<subnetIds>{}</subnetIds>", subs)
        } else {
            String::new()
        },
        if !e.security_group_ids.is_empty() {
            format!("<groupSet>{}</groupSet>", sgs)
        } else {
            String::new()
        },
        if e.private_dns {
            "<privateDnsEnabled>true</privateDnsEnabled>"
        } else {
            "<privateDnsEnabled>false</privateDnsEnabled>"
        },
        String::new(),
        tags_xml(&e.tags)
    )
}

fn create_vpc_endpoint(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    let service = params.str("ServiceName");
    let endpoint_type = params
        .get("VpcEndpointType")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Gateway".into());
    with_state(req, move |state| {
        if !state.vpcs.iter().any(|v| v.id == vpc_id) {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", crate::proto::xml_escape(&vpc_id)),
            );
        }
        let id = state.next_id("vpce");
        let route_tables: Vec<String> = params
            .list("RouteTableId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let subnets: Vec<String> = params
            .list("SubnetId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sgs: Vec<String> = params
            .list("SecurityGroupId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let tags = wire_tags(params);
        let e = super::state::VpcEndpoint {
            id: id.clone(),
            vpc_id: vpc_id.clone(),
            service_name: service.clone(),
            endpoint_type: endpoint_type.clone(),
            state: "available".into(),
            route_table_ids: route_tables.clone(),
            subnet_ids: subnets.clone(),
            security_group_ids: sgs.clone(),
            private_dns: params.bool("PrivateDnsEnabled").unwrap_or(false),
            tags,
            ..Default::default()
        };
        state.vpc_endpoints.push(e);
        ec2_xml(
            "CreateVpcEndpointResponse",
            &format!(
                "<vpcEndpoint><vpcEndpointId>{}</vpcEndpointId><vpcId>{}</vpcId><serviceName>{}</serviceName>{}<state>available</state><policyDocument></policyDocument>{}{}{}</vpcEndpoint>",
                crate::proto::xml_escape(&id),
                crate::proto::xml_escape(&vpc_id),
                crate::proto::xml_escape(&service),
                if !route_tables.is_empty() {
                    format!("<routeTableIds>{}{}</routeTableIds>",
                        route_tables.iter().map(|rt| format!("<item><routeTableId>{}</routeTableId></item>", crate::proto::xml_escape(rt))).collect::<String>(),
                        "")
                } else {
                    String::new()
                },
                String::new(),
                String::new(),
                ""
            ),
        )
    })
}

fn delete_vpc_endpoints(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("VpcEndpointId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let mut unsuccessful = String::new();
        for id in &ids {
            match state.vpc_endpoints.iter().position(|e| e.id == *id) {
                Some(pos) => {
                    state.vpc_endpoints.remove(pos);
                }
                None => {
                    let _ = write!(
                        unsuccessful,
                        "<item><error><code>InvalidVpcEndpointId.NotFound</code><message>The Vpc Endpoint ID '{}' does not exist</code></error><resourceId>{}</resourceId></item>",
                        crate::proto::xml_escape(id),
                        crate::proto::xml_escape(id)
                    );
                }
            }
        }
        if unsuccessful.is_empty() {
            ec2_xml("DeleteVpcEndpointsResponse", "<unsuccessful/>")
        } else {
            let _ = &mut unsuccessful;
            ec2_xml(
                "DeleteVpcEndpointsResponse",
                &format!("<unsuccessful>{}</unsuccessful>", unsuccessful),
            )
        }
    })
}

fn describe_vpc_endpoints(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("VpcEndpointId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let ids = ids.clone();
        let snapshot: Vec<super::state::VpcEndpoint> = state.vpc_endpoints.clone();
        let mut items = String::new();
        for e in &snapshot {
            if !ids.is_empty() && !ids.contains(&e.id) {
                continue;
            }
            if !filter_passes(&fs, move |f| match f {
                "vpc-endpoint-id" => vec![e.id.clone()],
                "service-name" => vec![e.service_name.clone()],
                "vpc-id" => vec![e.vpc_id.clone()],
                "state" => vec![e.state.clone()],
                "vpc-endpoint-type" => vec![e.endpoint_type.clone()],
                other => tag_candidates(&e.tags, other),
            }) {
                continue;
            }
            let it = endpoint_item(e);
            items.push_str(&it);
        }
        ec2_xml(
            "DescribeVpcEndpointsResponse",
            &format!("<vpcEndpointSet>{}</vpcEndpointSet>", items),
        )
    })
}

fn modify_vpc_endpoint(req: &Request, params: &P) -> Response {
    let id = params.str("VpcEndpointId");
    with_state(req, move |state| {
        let pos = match state.vpc_endpoints.iter().position(|e| e.id == id) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidVpcEndpointId.NotFound",
                    &format!(
                        "The Vpc Endpoint ID '{}' does not exist",
                        crate::proto::xml_escape(&id)
                    ),
                )
            }
        };
        if let Some(p) = params.get("PolicyDocument") {
            state.vpc_endpoints[pos].policy = p.to_string();
        }
        if let Some(b) = params.bool("PrivateDnsEnabled") {
            state.vpc_endpoints[pos].private_dns = b;
        }
        if let Some(sid) = params.get("SubnetId") {
            let sid = sid.to_string();
            state.vpc_endpoints[pos].subnet_ids.push(sid);
        }
        if let Some(rt) = params.get("RemoveRouteTableId") {
            let rt = rt.to_string();
            state.vpc_endpoints[pos]
                .route_table_ids
                .retain(|x| x != &rt);
        }
        ec2_xml("ModifyVpcEndpointResponse", "<return>true</return>")
    })
}

fn describe_vpc_endpoint_services(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeVpcEndpointServicesResponse",
        r#"<serviceDetailSet><item><serviceId>vpce-svc-0931094d2e6e2a2b6</serviceId><serviceName>com.amazonaws.us-east-1.s3</serviceName><serviceType><item><serviceType>Gateway</serviceType></item></serviceType><availabilityZones><item>us-east-1a</item><item>us-east-1b</item><item>us-east-1c</item><item>us-east-1d</item><item>us-east-1e</item><item>us-east-1f</item></availabilityZones><owner>amazon</owner><baseEndpointDnsNames><item>s3.us-east-1.amazonaws.com</item></baseEndpointDnsNames><vpcEndpointPolicySupported>true</vpcEndpointPolicySupported><acceptanceRequired>false</acceptanceRequired><managesVpcEndpoints>false</managesVpcEndpoints><privateDnsName/> </item></serviceDetailSet><vpcEndpointServiceSet/>"#,
    )
}

fn create_vpc_endpoint_service(req: &Request, params: &P) -> Response {
    let acceptance = params.bool("AcceptanceRequired").unwrap_or(false);
    with_state(req, move |state| {
        let nlbs: Vec<String> = params
            .list("NetworkLoadBalancerArn")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let id = state.next_id("vpce-svc");
        let tags = wire_tags(params);
        state
            .vpc_endpoint_services
            .push(super::state::VpcEndpointService {
                id: id.clone(),
                service_name: String::new(),
                acceptance_required: acceptance,
                nlb_arns: nlbs,
                tags,
                permissions: Vec::new(),
            });
        ec2_xml(
            "CreateVpcEndpointServiceConfigurationResponse",
            &format!(
                "<value><id>{}</id></vpcEndpointService>{}",
                crate::proto::xml_escape(&id),
                ""
            ),
        )
    })
}

fn delete_vpc_endpoint_service(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("ServiceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        for id in &ids {
            if let Some(pos) = state.vpc_endpoint_services.iter().position(|s| s.id == *id) {
                state.vpc_endpoint_services.remove(pos);
            }
        }
        ec2_xml(
            "DeleteVpcEndpointServiceConfigurationsResponse",
            "<serviceConfigurations/>",
        )
    })
}

fn describe_vpc_endpoint_services_cfg(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeVpcEndpointServiceConfigurationsResponse",
        "<serviceConfigurations/>",
    )
}
