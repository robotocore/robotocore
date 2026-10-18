//! EC2 handler group 2: security groups, internet gateways, elastic IPs,
//! route tables.
#![allow(
    clippy::format_in_format_args,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::single_match,
    clippy::map_identity
)]
#![allow(unused_variables)]

use super::handlers::filters as req_filters;
use super::handlers::{filter_passes, ok_xml, tag_candidates, wire_tags};

use super::state::{Ec2State, IpPermission, Route, RouteTable, SecurityGroup};
use super::{ec2_error, ec2_xml, tags_xml, with_state, Params};
use crate::proto::xml_escape;
use crate::{Request, Response};
use std::fmt::Write as _;

type P = Params;

pub fn route(req: &Request, params: &P) -> Response {
    match req.operation.as_str() {
        "CreateSecurityGroup" => create_security_group(req, params),
        "DeleteSecurityGroup" => delete_security_group(req, params),
        "DescribeSecurityGroups" => describe_security_groups(req, params),
        "DescribeSecurityGroupRules" => describe_security_group_rules(req, params),
        "AuthorizeSecurityGroupIngress" => authorize_security_group(req, params, false),
        "AuthorizeSecurityGroupEgress" => authorize_security_group(req, params, true),
        "RevokeSecurityGroupIngress" => revoke_security_group(req, params, false),
        "RevokeSecurityGroupEgress" => revoke_security_group(req, params, true),
        "UpdateSecurityGroupRuleDescriptionsIngress" => {
            update_sg_rule_descriptions(req, params, false)
        }
        "UpdateSecurityGroupRuleDescriptionsEgress" => {
            update_sg_rule_descriptions(req, params, true)
        }
        "ModifySecurityGroupRules" => modify_security_group_rules(req, params),
        "AssociateSecurityGroupVpc" => associate_sg_vpc(req, params),
        "DisassociateSecurityGroupVpc" => disassociate_sg_vpc(req, params),
        "DescribeSecurityGroupVpcAssociations" => describe_sg_vpc_associations(req, params),

        "CreateInternetGateway" => create_internet_gateway(req, params),
        "DeleteInternetGateway" => delete_internet_gateway(req, params),
        "DescribeInternetGateways" => describe_internet_gateways(req, params),
        "AttachInternetGateway" => attach_internet_gateway(req, params),
        "DetachInternetGateway" => detach_internet_gateway(req, params),
        "CreateCarrierGateway" => create_carrier_gateway(req, params),
        "DeleteCarrierGateway" => delete_carrier_gateway(req, params),
        "DescribeCarrierGateways" => describe_carrier_gateways(req, params),

        "AllocateAddress" => allocate_address(req, params),
        "DescribeAddresses" => describe_addresses(req, params),
        "ReleaseAddress" => release_address(req, params),
        "AssociateAddress" => associate_address(req, params),
        "DisassociateAddress" => disassociate_address(req, params),

        "CreateRouteTable" => create_route_table(req, params),
        "DeleteRouteTable" => delete_route_table(req, params),
        "DescribeRouteTables" => describe_route_tables(req, params),
        "CreateRoute" => create_route(req, params),
        "DeleteRoute" => delete_route(req, params),
        "ReplaceRoute" => replace_route(req, params),
        "AssociateRouteTable" => associate_route_table(req, params),
        "DisassociateRouteTable" => disassociate_route_table(req, params),
        "ReplaceRouteTableAssociation" => replace_route_table_association(req, params),

        _ => super::handlers_net2::route(req, params),
    }
}

// ---------------------------------------------------------------------------
// Security groups
// ---------------------------------------------------------------------------

fn require_sg<'a>(
    state: &'a mut Ec2State,
    group_id: &str,
) -> Result<&'a mut SecurityGroup, Response> {
    match state.security_groups.iter_mut().find(|s| s.id == group_id) {
        Some(s) => Ok(s),
        None => Err(ec2_error(
            400,
            "InvalidGroup.NotFound",
            &format!(
                "The security group '{}' does not exist",
                xml_escape(group_id)
            ),
        )),
    }
}

fn ip_permission_item(account: &str, perm: &IpPermission) -> String {
    let mut out = String::from("<item><ipProtocol>");
    out.push_str(&xml_escape(&perm.protocol));
    out.push_str("</ipProtocol>");
    if let (Some(fp), Some(tp)) = (perm.from_port, perm.to_port) {
        let _ = write!(out, "<fromPort>{}</fromPort><toPort>{}</toPort>", fp, tp);
    }
    if !perm.user_id_group_pairs.is_empty() {
        out.push_str("<groups>");
        for (gid, name, owner, desc) in &perm.user_id_group_pairs {
            out.push_str("<item>");
            if !owner.is_empty() {
                let _ = write!(out, "<userId>{}</userId>", xml_escape(owner));
            }
            if !gid.is_empty() {
                let _ = write!(out, "<groupId>{}</groupId>", xml_escape(gid));
            }
            if !name.is_empty() {
                let _ = write!(out, "<groupName>{}</groupName>", xml_escape(name));
            }
            out.push_str("</item>");
        }
        out.push_str("</groups>");
    } else {
        out.push_str("<groups/>");
    }
    out.push_str("<ipRanges>");
    for (cidr, desc) in &perm.ip_ranges {
        if desc.is_empty() {
            let _ = write!(out, "<item><cidrIp>{}</cidrIp></item>", xml_escape(cidr));
        } else {
            let _ = write!(
                out,
                "<item><cidrIp>{}</cidrIp><description>{}</description></item>",
                xml_escape(cidr),
                xml_escape(desc)
            );
        }
    }
    out.push_str("</ipRanges>");
    out.push_str("<ipv6Ranges>");
    for (cidr, desc) in &perm.ipv6_ranges {
        if desc.is_empty() {
            let _ = write!(
                out,
                "<item><cidrIpv6>{}</cidrIpv6></item>",
                xml_escape(cidr)
            );
        } else {
            let _ = write!(
                out,
                "<item><cidrIpv6>{}</cidrIpv6><description>{}</description></item>",
                xml_escape(cidr),
                xml_escape(desc)
            );
        }
    }
    out.push_str("</ipv6Ranges>");
    out.push_str("<prefixListIds/>");
    out.push_str("</item>");
    out
}

fn sg_item(account: u64, sg: &SecurityGroup) -> String {
    let mut perms = String::new();
    for p in &sg.permissions {
        perms.push_str(&ip_permission_item(&account.to_string(), p));
    }
    let mut egress = String::new();
    for p in &sg.egress {
        egress.push_str(&ip_permission_item(&account.to_string(), p));
    }
    let arn = format!(
        "arn:aws:ec2:{}:{}:security-group/{}",
        "us-east-1",
        account,
        xml_escape(&sg.id)
    );
    format!(
        "<item><groupId>{}</groupId><ipPermissionsEgress>{}</ipPermissionsEgress>{}<vpcId>{}</vpcId><securityGroupArn>{}</securityGroupArn><ownerId>{}</ownerId><groupName>{}</groupName><groupDescription>{}</groupDescription><ipPermissions>{}</ipPermissions></item>",
        xml_escape(&sg.id),
        egress,
        tags_xml(&sg.tags),
        xml_escape(&sg.vpc_id),
        arn,
        sg.owner_id,
        xml_escape(&sg.name),
        xml_escape(&sg.description),
        perms
    )
}

fn create_security_group(req: &Request, params: &P) -> Response {
    let name = params.str("GroupName");
    let description = params.str("GroupDescription");
    let vpc_id = params.str("VpcId");
    if description.is_empty() {
        return ec2_error(
            400,
            "MissingParameter",
            "The request must contain the parameter GroupDescription",
        );
    }
    if name.is_empty() {
        return ec2_error(
            400,
            "MissingParameter",
            "The request must contain the parameter GroupName",
        );
    }
    with_state(req, move |state| {
        let vpc = if vpc_id.is_empty() {
            state
                .vpcs
                .iter()
                .find(|v| v.is_default)
                .map(|v| v.id.clone())
                .unwrap_or_default()
        } else {
            match state.vpcs.iter().find(|v| v.id == vpc_id) {
                Some(v) => v.id.clone(),
                None => {
                    return ec2_error(
                        400,
                        "InvalidVpcID.NotFound",
                        &format!("Vpc {{{}}} not found", xml_escape(&vpc_id)),
                    )
                }
            }
        };
        if state
            .security_groups
            .iter()
            .any(|s| s.name == name && s.vpc_id == vpc)
        {
            return ec2_error(
                400,
                "InvalidGroup.Duplicate",
                &format!(
                    "The security group '{}' already exists",
                    xml_escape(name.as_str())
                ),
            );
        }
        let sg_id = state.next_id("sg");
        let mut sg = SecurityGroup {
            id: sg_id.clone(),
            name: name.clone(),
            description: description.clone(),
            vpc_id: vpc.clone(),
            owner_id: req.account.to_string(),
            ..Default::default()
        };
        sg.egress = vec![IpPermission {
            protocol: "-1".into(),
            ..Default::default()
        }];
        let tags = super::handlers::wire_tags(params);
        if !tags.is_empty() {
            sg.tags = tags.clone();
        }
        state.security_groups.push(sg);
        ec2_xml(
            "CreateSecurityGroupResponse",
            &format!(
                "<return>true</return><groupId>{}</groupId>",
                xml_escape(&sg_id)
            ),
        )
    })
}

fn delete_security_group(req: &Request, params: &P) -> Response {
    let group_id = params.str("GroupId");
    let group_name = params.str("GroupName");
    with_state(req, move |state| {
        let pos = if !group_id.is_empty() {
            state.security_groups.iter().position(|s| s.id == group_id)
        } else if !group_name.is_empty() {
            // name lookup within a VPC or the default vpc
            let vpc_id = params.str("VpcId");
            let vpc = if vpc_id.is_empty() {
                state
                    .vpcs
                    .iter()
                    .filter(|v| v.is_default)
                    .map(|v| v.id.clone())
                    .next()
                    .unwrap_or_default()
            } else {
                vpc_id.clone()
            };
            state
                .security_groups
                .iter()
                .position(|s| s.name == group_name && s.vpc_id == vpc)
        } else {
            None
        };
        let pos = match pos {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidGroup.NotFound",
                    &format!(
                        "The security group '{}' does not exist",
                        xml_escape(group_id.as_str())
                    ),
                )
            }
        };
        if state.security_groups[pos].name == "default" {
            return ec2_error(
                400,
                "InvalidGroup.InUse",
                "The security group 'default' cannot be deleted",
            );
        }
        state.security_groups.remove(pos);
        ok_xml("DeleteSecurityGroupResponse")
    })
}

fn describe_security_groups(req: &Request, params: &P) -> Response {
    let group_ids: Vec<String> = params
        .list("GroupId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let group_names: Vec<String> = params
        .list("GroupName")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = req_filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let items: Vec<String> = state
            .security_groups
            .iter()
            .filter(|s| group_ids.is_empty() || group_ids.contains(&s.id))
            .filter(|s| group_names.is_empty() || group_names.contains(&s.name))
            .filter(|s| {
                filter_passes(&fs, |f| match f {
                    "vpc-id" => vec![s.vpc_id.clone()],
                    "group-id" => vec![s.id.clone()],
                    "group-name" => vec![s.name.clone()],
                    "owner-id" => vec![s.owner_id.clone()],
                    "description" => vec![s.description.clone()],
                    other => tag_candidates(&s.tags, other),
                })
            })
            .map(|s| sg_item(account, s))
            .collect();
        ec2_xml(
            "DescribeSecurityGroupsResponse",
            &format!("<securityGroupInfo>{}</securityGroupInfo>", items.concat()),
        )
    })
}

fn parse_permissions(params: &P) -> Vec<IpPermission> {
    let mut out = Vec::new();
    for i in params.container_indices("IpPermissions") {
        let base = format!("IpPermissions.{}", i + 1);
        let mut perm = IpPermission {
            protocol: params
                .sub("IpPermissions", i, "IpProtocol")
                .unwrap_or("-1")
                .to_string(),
            from_port: params
                .sub("IpPermissions", i, "FromPort")
                .and_then(|v| v.parse::<i64>().ok()),
            to_port: params
                .sub("IpPermissions", i, "ToPort")
                .and_then(|v| v.parse::<i64>().ok()),
            ..Default::default()
        };
        let ranges = params.container_indices(&format!("{base}.IpRanges"));
        for r in ranges {
            let rb = format!("{base}.IpRanges");
            if let Some(cidr) = params.sub(&rb, r, "CidrIp") {
                perm.ip_ranges.push((
                    cidr.to_string(),
                    params
                        .sub(&rb, r, "Description")
                        .unwrap_or_default()
                        .to_string(),
                ));
            }
        }
        let v6 = params.container_indices(&format!("{base}.Ipv6Ranges"));
        for r in v6 {
            let rb = format!("{base}.Ipv6Ranges");
            if let Some(cidr) = params.sub(&rb, r, "CidrIpv6") {
                perm.ipv6_ranges.push((
                    cidr.to_string(),
                    params
                        .sub(&rb, r, "Description")
                        .unwrap_or_default()
                        .to_string(),
                ));
            }
        }
        let pls = params.container_indices(&format!("{base}.PrefixListIds"));
        for r in pls {
            let rb = format!("{base}.PrefixListIds");
            if let Some(pl) = params.sub(&rb, r, "PrefixListId") {
                perm.prefix_list_ids.push((
                    pl.to_string(),
                    params
                        .sub(&rb, r, "Description")
                        .unwrap_or_default()
                        .to_string(),
                ));
            }
        }
        let pairs = params.container_indices(&format!("{base}.UserIdGroupPairs"));
        for r in pairs {
            let rb = format!("{base}.UserIdGroupPairs");
            perm.user_id_group_pairs.push((
                params
                    .sub(&rb, r, "GroupId")
                    .unwrap_or_default()
                    .to_string(),
                params
                    .sub(&rb, r, "GroupName")
                    .unwrap_or_default()
                    .to_string(),
                params.sub(&rb, r, "UserId").unwrap_or_default().to_string(),
                params
                    .sub(&rb, r, "Description")
                    .unwrap_or_default()
                    .to_string(),
            ));
        }
        out.push(perm);
    }
    out
}

fn authorize_security_group(req: &Request, params: &P, egress: bool) -> Response {
    let by_id = params.str("GroupId");
    let by_name = params.str("GroupName");
    with_state(req, move |state| {
        let pos = if !by_id.is_empty() {
            state.security_groups.iter().position(|s| s.id == by_id)
        } else {
            let vpc = state
                .vpcs
                .iter()
                .filter(|v| v.is_default)
                .map(|v| v.id.clone())
                .next()
                .unwrap_or_default();
            state
                .security_groups
                .iter()
                .position(|s| s.name == by_name && s.vpc_id == vpc)
        };
        let pos = match pos {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidGroup.NotFound",
                    &format!("The security group '{}' does not exist", xml_escape(&by_id)),
                )
            }
        };
        let new_perms = parse_permissions(params);
        // group references must resolve to security groups in the same VPC.
        let vpc_for_pos = state.security_groups[pos].vpc_id.clone();
        for new in &new_perms {
            for (gid, _, _, _) in &new.user_id_group_pairs {
                if !gid.is_empty()
                    && !state
                        .security_groups
                        .iter()
                        .any(|s| s.id == gid.as_str() && s.vpc_id == vpc_for_pos)
                {
                    return ec2_error(
                        400,
                        "InvalidGroup.NotFound",
                        &format!("The security group '{}' does not exist", xml_escape(gid)),
                    );
                }
            }
        }
        let duplicate = if egress {
            state.security_groups[pos]
                .egress
                .iter()
                .any(|existing| new_perms.iter().any(|p| existing.key_match(p)))
        } else {
            state.security_groups[pos]
                .permissions
                .iter()
                .any(|existing| new_perms.iter().any(|p| existing.key_match(p)))
        };
        if duplicate {
            return ec2_error(
                409,
                "InvalidPermission.Duplicate",
                "The specified rule already exists",
            );
        }
        if egress {
            for p in new_perms {
                state.security_groups[pos].egress.push(p);
            }
        } else {
            for p in new_perms {
                state.security_groups[pos].permissions.push(p);
            }
        }
        ok_xml("AuthorizeSecurityGroupResponse")
    })
}

fn revoke_security_group(req: &Request, params: &P, egress: bool) -> Response {
    let by_id = params.str("GroupId");
    let by_name = params.str("GroupName");
    with_state(req, move |state| {
        let pos = if !by_id.is_empty() {
            state.security_groups.iter().position(|s| s.id == by_id)
        } else {
            let vpc = state
                .vpcs
                .iter()
                .filter(|v| v.is_default)
                .map(|v| v.id.clone())
                .next()
                .unwrap_or_default();
            state
                .security_groups
                .iter()
                .position(|s| s.name == by_name && s.vpc_id == vpc)
        };
        let pos = match pos {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidGroup.NotFound",
                    &format!("The security group '{}' does not exist", xml_escape(&by_id)),
                )
            }
        };
        let new_perms = parse_permissions(params);
        let retained: Vec<IpPermission> = if egress {
            state.security_groups[pos]
                .egress
                .iter()
                .filter(|existing| !new_perms.iter().any(|p| existing.key_match(p)))
                .cloned()
                .collect()
        } else {
            state.security_groups[pos]
                .permissions
                .iter()
                .filter(|existing| !new_perms.iter().any(|p| existing.key_match(p)))
                .cloned()
                .collect()
        };
        if egress {
            let had = state.security_groups[pos].egress.len();
            state.security_groups[pos].egress = retained;
            if had == state.security_groups[pos].egress.len() && !new_perms.is_empty() {
                return ec2_error(
                    400,
                    "InvalidPermission.NotFound",
                    "The specified rule does not exist in this security group",
                );
            }
        } else {
            let had = state.security_groups[pos].permissions.len();
            state.security_groups[pos].permissions = retained;
            if had != state.security_groups[pos].permissions.len() {
                return ok_xml("RevokeSecurityGroupResponse");
            }
            if !new_perms.is_empty() {
                return ec2_error(
                    400,
                    "InvalidPermission.NotFound",
                    "The specified rule does not exist in this security group",
                );
            }
        }
        ok_xml("RevokeSecurityGroupResponse")
    })
}

fn update_sg_rule_descriptions(req: &Request, params: &P, egress: bool) -> Response {
    let by_id = params.str("GroupId");
    with_state(req, move |state| {
        let pos = match state.security_groups.iter().position(|s| s.id == by_id) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidGroup.NotFound",
                    &format!("The security group '{}' does not exist", xml_escape(&by_id)),
                )
            }
        };
        let new_perms = parse_permissions(params);
        if egress {
            for p in &new_perms {
                for existing in state.security_groups[pos].egress.iter_mut() {
                    if existing.key_match(p) {
                        existing.ip_ranges = p.ip_ranges.clone();
                        existing.ipv6_ranges = p.ipv6_ranges.clone();
                        existing.prefix_list_ids = p.prefix_list_ids.clone();
                    }
                }
            }
        } else {
            for p in &new_perms {
                for existing in state.security_groups[pos].permissions.iter_mut() {
                    if existing.key_match(p) {
                        existing.ip_ranges = p.ip_ranges.clone();
                        existing.ipv6_ranges = p.ipv6_ranges.clone();
                        existing.prefix_list_ids = p.prefix_list_ids.clone();
                    }
                }
            }
        }
        ok_xml("UpdateSecurityGroupRuleDescriptionsResponse")
    })
}

fn modify_security_group_rules(req: &Request, params: &P) -> Response {
    ok_xml("ModifySecurityGroupRulesResponse")
}

fn associate_sg_vpc(req: &Request, params: &P) -> Response {
    let sg_id = params.str("GroupId");
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        if require_sg(state, &sg_id).is_err() {
            return require_sg(state, &sg_id).err().unwrap();
        }
        if !state.vpcs.iter().any(|v| v.id == vpc_id) {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", xml_escape(&vpc_id)),
            );
        }
        let assoc_id = state.next_id("sgvpa");
        let rule_id = state.next_id("sgr");
        let pos = state
            .security_groups
            .iter()
            .position(|s| s.id == sg_id)
            .unwrap_or(0);
        state.security_groups[pos]
            .tags
            .push((format!("sgvpa:{}", assoc_id), vpc_id.clone()));
        ec2_xml(
            "AssociateSecurityGroupVpcResponse",
            &format!(
                "<state>associated</state><securityGroupVpcAssociationId>{}</securityGroupVpcAssociationId>",
                xml_escape(&assoc_id)
            ),
        )
    })
}

fn disassociate_sg_vpc(req: &Request, params: &P) -> Response {
    let sg_id = params.str("GroupId");
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        let target = "sgvpa:".to_string();
        let mut removed = false;
        if let Some(pos) = state.security_groups.iter().position(|s| s.id == sg_id) {
            let tags = state.security_groups[pos].tags.clone();
            let keep: Vec<(String, String)> = tags
                .iter()
                .filter(|(k, v)| {
                    if k.starts_with(&target) && v == &vpc_id {
                        removed = true;
                        false
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            state.security_groups[pos].tags = keep;
        }
        if !removed {
            return ec2_error(
                400,
                "InvalidGroup.NotFound",
                "The security group Vpc association does not exist",
            );
        }
        ec2_xml(
            "DisassociateSecurityGroupVpcResponse",
            "<state>disassociated</state>",
        )
    })
}

fn describe_sg_vpc_associations(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        let mut body = String::new();
        for sg in &state.security_groups {
            for (k, v) in &sg.tags {
                if let Some(assoc_id) = k.strip_prefix("sgvpa:") {
                    let _ = write!(
                        body,
                        "<item><securityGroupVpcAssociationId>{}</securityGroupVpcAssociationId><groupId>{}</groupId><vpcId>{}</vpcId><state>associated</state></item>",
                        xml_escape(assoc_id),
                        xml_escape(&sg.id),
                        xml_escape(v)
                    );
                }
            }
        }
        ec2_xml(
            "DescribeSecurityGroupVpcAssociationsResponse",
            &format!(
                "<securityGroupVpcAssociations>{}</securityGroupVpcAssociations>",
                body
            ),
        )
    })
}

/// Serialized rule id for a (sg, direction, index) triple.
fn sg_rule_id(sg_id: &str, egress: bool, idx: usize) -> String {
    format!(
        "sgr-{}-{}{:04}",
        &sg_id[3..],
        if egress { "e" } else { "i" },
        idx + 1
    )
}

fn describe_security_group_rules(req: &Request, params: &P) -> Response {
    let rule_ids: Vec<String> = params
        .list("SecurityGroupRuleId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let group_ids: Vec<String> = params
        .list("GroupId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = req_filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let mut body = String::new();
        for sg in &state.security_groups {
            let _ = &sg.id;
            if sg_rule_filters_fail(sg, &group_ids, &fs) {
                continue;
            }
            for (egress, perms) in [
                (false, sg.permissions.as_slice()),
                (true, sg.egress.as_slice()),
            ] {
                for (idx, perm) in perms.iter().enumerate() {
                    let id = sg_rule_id(&sg.id, egress, idx);
                    if !rule_ids.is_empty() && !rule_ids.contains(&id) {
                        continue;
                    }
                    for (cidr, descr) in &perm.ip_ranges {
                        let _ = write!(
                            body,
                            "<item><securityGroupRuleId>{}</securityGroupRuleId><groupId>{}</groupId><isEgress>{}</isEgress><ipProtocol>{}</ipProtocol><cidrIpv4>{}</cidrIpv4><referencedSecurityGroup/>",
                            xml_escape(&id),
                            xml_escape(&sg.id),
                            egress,
                            xml_escape(&perm.protocol),
                            xml_escape(cidr)
                        );
                        if let (Some(f), Some(t)) = (perm.from_port, perm.to_port) {
                            let _ =
                                write!(body, "<fromPort>{}</fromPort><toPort>{}</toPort>", f, t);
                        }
                        if !descr.is_empty() {
                            let _ =
                                write!(body, "<description>{}</description>", xml_escape(descr));
                        }
                        body.push_str("</item>");
                    }
                    for (g, _, owner, _) in &perm.user_id_group_pairs {
                        let _ = write!(
                            body,
                            "<item><securityGroupRuleId>{}</securityGroupRuleId><groupId>{}</groupId><isEgress>{}</isEgress><ipProtocol>{}</ipProtocol><referencedSecurityGroup><groupId>{}</groupId><userId>{}</userId></referencedSecurityGroup>",
                            xml_escape(&id),
                            xml_escape(&sg.id),
                            egress,
                            xml_escape(&perm.protocol),
                            xml_escape(g),
                            xml_escape(owner)
                        );
                        if let (Some(f), Some(t)) = (perm.from_port, perm.to_port) {
                            let _ =
                                write!(body, "<fromPort>{}</fromPort><toPort>{}</toPort>", f, t);
                        }
                        body.push_str("</item>");
                    }
                }
            }
        }
        ec2_xml(
            "DescribeSecurityGroupRulesResponse",
            &format!("<securityGroupRuleSet>{}</securityGroupRuleSet>", body),
        )
    })
}

fn sg_rule_filters_fail(
    sg: &SecurityGroup,
    group_ids: &[String],
    fs: &[(String, Vec<String>)],
) -> bool {
    if !group_ids.is_empty() && !group_ids.contains(&sg.id) {
        return true;
    }
    !filter_passes(fs, |f| match f {
        "group-id" => vec![sg.id.clone()],
        "group-owner-id" => vec![sg.owner_id.clone()],
        "security-group-rule-id" => (0..sg.permissions.len())
            .map(|i| sg_rule_id(&sg.id, false, i))
            .chain((0..sg.egress.len()).map(|i| sg_rule_id(&sg.id, true, i)))
            .collect(),
        other => tag_candidates(&sg.tags, other),
    })
}

// ---------------------------------------------------------------------------
// Internet gateways
// ---------------------------------------------------------------------------

fn require_igw(state: &mut Ec2State, igw_id: &str) -> Result<usize, Response> {
    match state.internet_gateways.iter().position(|g| g.id == igw_id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidInternetGatewayID.NotFound",
            &format!("The gateway ID '{}' does not exist", xml_escape(igw_id)),
        )),
    }
}

pub fn igw_item(igw: &super::state::InternetGateway) -> String {
    let mut attachments = String::new();
    for (vpc, state_) in &igw.attachments {
        let _ = write!(
            attachments,
            "<item><vpcId>{}</vpcId><state>{}</state></item>",
            xml_escape(vpc),
            xml_escape(state_)
        );
    }
    format!(
        "<item><internetGatewayId>{}</internetGatewayId><attachmentSet>{}</attachmentSet>{}<tagSet/></item>",
        xml_escape(&igw.id),
        attachments,
        ""
    )
}

pub fn igw_item_full(igw: &super::state::InternetGateway) -> String {
    let mut attachments = String::new();
    for (vpc, state_) in &igw.attachments {
        let _ = write!(
            attachments,
            "<item><vpcId>{}</vpcId><state>{}</state></item>",
            xml_escape(vpc),
            xml_escape(state_)
        );
    }
    format!(
        "<item><internetGatewayId>{}</internetGatewayId><attachmentSet>{}</attachmentSet><ownerId>{}</ownerId>{}<tagSet>{}</tagSet></item>",
        xml_escape(&igw.id),
        attachments,
        "",
        "",
        tags_xml(&igw.tags)
    )
}

fn create_internet_gateway(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        let igw_id = state.next_id("igw");
        let tags = wire_tags(params);
        let mut igw = super::state::InternetGateway {
            id: igw_id.clone(),
            ..Default::default()
        };
        if !tags.is_empty() {
            igw.tags = tags;
        }
        state.internet_gateways.push(igw);
        ec2_xml(
            "CreateInternetGatewayResponse",
            &format!(
                "<internetGateway><internetGatewayId>{}</internetGatewayId><attachmentSet/><ownerId>{}</ownerId><tagSet></tagSet></internetGateway>",
                xml_escape(&igw_id),
                req.account
            ),
        )
    })
}

fn delete_internet_gateway(req: &Request, params: &P) -> Response {
    let igw_id = params.str("InternetGatewayId");
    with_state(req, move |state| {
        if require_igw(state, &igw_id).is_err() {
            return require_igw(state, &igw_id).err().unwrap();
        }
        let pos = state
            .internet_gateways
            .iter()
            .position(|g| g.id == igw_id)
            .unwrap_or(0);
        if !state.internet_gateways[pos].attachments.is_empty() {
            return ec2_error(
                400,
                "InvalidInternetGatewayID.NotFound",
                &format!("Gateway '{}' is currently attached", xml_escape(&igw_id)),
            );
        }
        state.internet_gateways.remove(pos);
        ok_xml("DeleteInternetGatewayResponse")
    })
}

fn describe_internet_gateways(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InternetGatewayId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = req_filters(params);
    with_state(req, move |state| {
        let ids = ids.clone();
        let items: Vec<String> = state
            .internet_gateways
            .iter()
            .filter(|g| ids.is_empty() || ids.contains(&g.id))
            .filter(|g| {
                filter_passes(&fs, |f| match f {
                    "internet-gateway-id" | "igw-id" => vec![g.id.clone()],
                    "attachment.state" => g.attachments.iter().map(|(_, s)| s.clone()).collect(),
                    "attachment.vpc-id" => g.attachments.iter().map(|(v, _)| v.clone()).collect(),
                    other => tag_candidates(&g.tags, other),
                })
            })
            .map(|g| igw_item_full(&g.clone()))
            .collect();
        ec2_xml(
            "DescribeInternetGatewaysResponse",
            &format!(
                "<internetGatewaySet>{}</internetGatewaySet>",
                items.concat()
            ),
        )
    })
}

fn attach_internet_gateway(req: &Request, params: &P) -> Response {
    let igw_id = params.str("InternetGatewayId");
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        if require_igw(state, &igw_id).is_err() {
            return require_igw(state, &igw_id).err().unwrap();
        }
        let pos = match state.internet_gateways.iter().position(|g| g.id == igw_id) {
            Some(i) => i,
            None => return ok_xml("AttachInternetGatewayResponse"),
        };
        if state.internet_gateways[pos]
            .attachments
            .iter()
            .any(|(v, _)| v == &vpc_id)
        {
            return ec2_error(
                400,
                "Resource.AlreadyAssociated",
                "Resource is already associated",
            );
        }
        state.internet_gateways[pos]
            .attachments
            .push((vpc_id.clone(), "available".into()));
        ok_xml("AttachInternetGatewayResponse")
    })
}

fn detach_internet_gateway(req: &Request, params: &P) -> Response {
    let igw_id = params.str("InternetGatewayId");
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        match state.internet_gateways.iter().position(|g| g.id == igw_id) {
            Some(pos) => {
                let before = state.internet_gateways[pos].attachments.len();
                state.internet_gateways[pos]
                    .attachments
                    .retain(|(v, _)| v != &vpc_id);
                if state.internet_gateways[pos].attachments.len() == before {
                    return ec2_error(
                        400,
                        "Gateway_NotAttached",
                        &format!(
                            "Gateway '{}' is not attached to '{}'",
                            xml_escape(igw_id.as_str()),
                            xml_escape(vpc_id.as_str())
                        ),
                    );
                }
            }
            None => {
                return ec2_error(
                    400,
                    "InvalidInternetGatewayID.NotFound",
                    &format!("The gateway ID '{}' does not exist", xml_escape(&igw_id)),
                )
            }
        }
        ok_xml("DetachInternetGatewayResponse")
    })
}

fn create_carrier_gateway(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        let cg_id = state.next_id("carrier-gateway");
        let tags = wire_tags(params);
        let mut s = super::state::Simple {
            id: cg_id.clone(),
            kind: "carrier-gateway".into(),
            ..Default::default()
        };
        s.set("VpcId", vpc_id.clone());
        s.set("State", "available");
        s.set("OwnerId", req.account.to_string());
        if !tags.is_empty() {
            s.tags = tags;
        }
        state
            .simple
            .entry("carrier-gateway".to_string())
            .or_default()
            .push(s);
        ec2_xml(
            "CreateCarrierGatewayResponse",
            &format!(
                "<carrierGateway><carrierGatewayId>{}</carrierGatewayId><vpcId>{}</vpcId><state>available</state><ownerId>{}</ownerId>{}</carrierGateway>",
                xml_escape(&cg_id),
                xml_escape(&vpc_id),
                req.account,
                ""
            ),
        )
    })
}

fn delete_carrier_gateway(req: &Request, params: &P) -> Response {
    let cg_id = params.str("CarrierGatewayId");
    with_state(req, move |state| {
        let fam = state
            .simple
            .entry("carrier-gateway".to_string())
            .or_default();
        let before = fam.len();
        fam.retain(|s| s.id != cg_id);
        if fam.len() == before {
            return ec2_error(
                400,
                "InvalidCarrierGatewayID.NotFound",
                &format!(
                    "The carrier gateway '{}' does not exist",
                    xml_escape(&cg_id)
                ),
            );
        }
        ok_xml("DeleteCarrierGatewayResponse")
    })
}

fn describe_carrier_gateways(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("CarrierGatewayId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let fam = state
            .simple
            .get("carrier-gateway")
            .cloned()
            .unwrap_or_default();
        let mut body = String::new();
        for s in &fam {
            if !ids.is_empty() && !ids.contains(&s.id) {
                continue;
            }
            let _ = write!(
                body,
                "<item><carrierGatewayId>{}</carrierGatewayId><vpcId>{}</vpcId><state>{}</state><ownerId>{}</ownerId>{}</item>",
                xml_escape(&s.id),
                xml_escape(s.get("VpcId").unwrap_or_default()),
                xml_escape(s.get("State").unwrap_or("available")),
                req.account,
                tags_xml(&s.tags)
            );
        }
        ec2_xml(
            "DescribeCarrierGatewaysResponse",
            &format!("<carrierGatewaySet>{}</carrierGatewaySet>", body),
        )
    })
}

// ---------------------------------------------------------------------------
// Elastic IPs
// ---------------------------------------------------------------------------

/// Random public IPv4 without reserved-looking octets (mirrors moto).
fn random_public_ip() -> String {
    let n = uuid::Uuid::new_v4().as_u128();
    let a = (n as u32 >> 24) & 0xff;
    let b = (n as u32 >> 16) & 0xff;
    let c = (n as u32 >> 8) & 0xff;
    let d = n as u32 & 0xff;
    format!("{}.{}.{}.{}", a.max(40) % 240 + 2, b, c, d)
}

fn find_alloc(state: &Ec2State, id: &str, public_ip: &str) -> Option<usize> {
    if !id.is_empty() {
        return state.addresses.iter().position(|a| a.allocation_id == id);
    }
    if !public_ip.is_empty() {
        return state
            .addresses
            .iter()
            .position(|a| a.public_ip == public_ip);
    }
    None
}

fn describe_addresses(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("AllocationId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let ips: Vec<String> = params
        .list("PublicIp")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = req_filters(params);
    with_state(req, move |state| {
        let mut body = String::new();
        for eip in &state.addresses {
            if !ids.is_empty() && !ids.contains(&eip.allocation_id) {
                continue;
            }
            if !ips.is_empty() && !ips.contains(&eip.public_ip) {
                continue;
            }
            if !filter_passes(&fs, |f| match f {
                "allocation-id" => vec![eip.allocation_id.clone()],
                "public-ip" => vec![eip.public_ip.clone()],
                "domain" | "network-interface-owner-id" => vec![eip.domain.clone()],
                "association-id" => eip
                    .association_id
                    .clone()
                    .map(|a| vec![a])
                    .unwrap_or_default(),
                "vpc-id" => eip.vpc_id.clone().map(|a| vec![a]).unwrap_or_default(),
                other => tag_candidates(&eip.tags, other),
            }) {
                continue;
            }
            let mut item = String::new();
            if !eip.allocation_id.is_empty() {
                let _ = write!(item, "<domain>{}</domain>", xml_escape("vpc"));
            }
            let _ = write!(item, "<publicIp>{}</publicIp>", xml_escape(&eip.public_ip));
            if let Some(a) = &eip.association_id {
                let _ = write!(item, "<associationId>{}</associationId>", xml_escape(a));
            }
            if let Some(v) = &eip.vpc_id {
                let _ = write!(item, "<vpcId>{}</vpcId>", xml_escape(v));
            }
            if let Some(i) = &eip.instance_id {
                let _ = write!(item, "<instanceId>{}</instanceId>", xml_escape(i));
            }
            if !eip.allocation_id.is_empty() {
                let _ = write!(
                    item,
                    "<allocationId>{}</allocationId>",
                    xml_escape(&eip.allocation_id)
                );
            }
            if !eip.private_ip_address.is_empty() {
                let _ = write!(
                    item,
                    "<privateIpAddress>{}</privateIpAddress>",
                    xml_escape(&eip.private_ip_address)
                );
            }
            if !eip.tags.is_empty() {
                item.push_str(&tags_xml(&eip.tags));
            }
            let _ = write!(body, "<item>{}</item>", item);
        }
        ec2_xml(
            "DescribeAddressesResponse",
            &format!("<addressesSet>{}</addressesSet>", body),
        )
    })
}

fn release_address(req: &Request, params: &P) -> Response {
    let alloc_id = params.str("AllocationId");
    let public_ip = params.str("PublicIp");
    with_state(req, move |state| {
        if find_alloc(state, &alloc_id, &public_ip).is_none() {
            return ec2_error(
                400,
                "InvalidAllocationID.NotFound",
                &format!(
                    "ElasticTouch allocation id '{}' was not found",
                    xml_escape(&alloc_id)
                ),
            );
        }
        let pos = find_alloc(state, &alloc_id, &public_ip).unwrap_or(0);
        state.addresses.remove(pos);
        ok_xml("ReleaseAddressResponse")
    })
}

fn associate_address(req: &Request, params: &P) -> Response {
    let alloc_id = params.str("AllocationId");
    let public_ip = params.str("PublicIp");
    let instance_id = params.str("InstanceId");
    let network_interface_id = params.str("NetworkInterfaceId");
    with_state(req, move |state| {
        // instance or ENI required unless the alloc exists (fog-style check)
        let pos = match find_alloc(state, &alloc_id, &public_ip) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidAllocationID.NotFound",
                    &format!(
                        "ElasticTouch allocation id '{}' was not found",
                        xml_escape(&alloc_id)
                    ),
                )
            }
        };
        // resolve target ENI & subnet
        let eni_id = if !network_interface_id.is_empty() {
            network_interface_id.clone()
        } else {
            match state.enis.iter().find(|e| {
                e.attached
                    .as_ref()
                    .map(|a| a.instance_id == instance_id)
                    .unwrap_or(false)
                    && e.attached
                        .as_ref()
                        .map(|a| a.device_index == 0)
                        .unwrap_or(false)
            }) {
                Some(e) => e.id.clone(),
                None => {
                    if instance_id.is_empty() {
                        return ec2_error(
                            400,
                            "MissingParameter",
                            "The request must contain the parameter InstanceId",
                        );
                    }
                    state.next_id("eni")
                }
            }
        };
        let vpc = state
            .enis
            .iter()
            .find(|e| e.id == eni_id)
            .map(|e| e.vpc_id.clone())
            .unwrap_or_default();
        let assoc_id = state.next_id("eipassoc");
        let private_ip = state
            .enis
            .iter()
            .find(|e| e.id == eni_id)
            .map(|e| e.private_ip.clone())
            .unwrap_or_default();
        let owner = req.account.to_string();
        state.addresses[pos] = super::state::ElasticIp {
            allocation_id: state.addresses[pos].allocation_id.clone(),
            public_ip: state.addresses[pos].public_ip.clone(),
            domain: "vpc".into(),
            private_ip_address: private_ip.clone(),
            association_id: Some(assoc_id.clone()),
            vpc_id: Some(vpc.clone()),
            instance_id: if instance_id.is_empty() {
                None
            } else {
                Some(instance_id.clone())
            },
            network_interface_id: if eni_id.is_empty() {
                None
            } else {
                Some(eni_id.clone())
            },
            network_interface_owner_id: Some(req.account.to_string()),
            tags: state.addresses[pos].tags.clone(),
        };
        if !eni_id.is_empty() {
            if let Some(e) = state.enis.iter_mut().find(|e| e.id == eni_id) {
                e.association_id = Some(assoc_id.clone());
                e.public_ip = Some(state.addresses[pos].public_ip.clone());
            }
        }
        let _ = owner;
        let mut body = format!(
            "<associationId>{}</associationId>{}",
            xml_escape(&assoc_id),
            ""
        );
        body = format!("<associationId>{}</associationId>", xml_escape(&assoc_id));
        ec2_xml("AssociateAddressResponse", &body)
    })
}

fn disassociate_address(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    with_state(req, move |state| {
        let pos = state
            .addresses
            .iter()
            .position(|a| a.association_id.as_deref() == Some(assoc_id.as_str()));
        match pos {
            Some(i) => {
                let eni = state.addresses[i]
                    .network_interface_id
                    .clone()
                    .unwrap_or_default();
                state.addresses[i].association_id = None;
                state.addresses[i].instance_id = None;
                state.addresses[i].network_interface_id = None;
                state.addresses[i].private_ip_address = String::new();
                state.addresses[i].vpc_id = None;
                if !eni.is_empty() {
                    if let Some(e) = state.enis.iter_mut().find(|e| e.id == eni) {
                        e.association_id = None;
                        e.public_ip = None;
                    }
                }
            }
            None => {
                return ec2_error(
                    400,
                    "InvalidAssociationID.NotFound",
                    &format!(
                        "ElasticTouch association id '{}' was not found",
                        xml_escape(&assoc_id)
                    ),
                )
            }
        }
        ok_xml("DisassociateAddressResponse")
    })
}

// ---------------------------------------------------------------------------
// Route tables
// ---------------------------------------------------------------------------

fn require_rt(state: &mut Ec2State, rt_id: &str) -> Result<usize, Response> {
    match state.route_tables.iter().position(|t| t.id == rt_id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidRouteTableID.NotFound",
            &format!("The route table '{}' does not exist", xml_escape(rt_id)),
        )),
    }
}

pub fn route_table_item(state: &Ec2State, account: u64, rt: &RouteTable) -> String {
    let mut routes = String::new();
    for r in &rt.routes {
        let mut target = r.target.clone().unwrap_or_default();
        let _ = &mut target;
        let _ = write!(
            routes,
            "<item><destinationCidrBlock>{}</destinationCidrBlock>{}<origin>{}</origin><state>{}</state></item>",
            xml_escape(&r.destination),
            match r.target.as_deref() {
                Some("local") | Some("local ") => "<gatewayId>local</gatewayId>".to_string(),
                Some(t) => {
                    if t.starts_with("igw-")
                        || t.starts_with("nat-")
                        || t.starts_with("pcx-")
                        || t.starts_with("tgw-")
                        || t.starts_with("vpce-")
                        || t.starts_with("eigw-")
                        || t.starts_with("eni-")
                    {
                        let _ = 1;
                        format!("<gatewayId>{}</gatewayId>", xml_escape(t))
                    } else if t.starts_with("eni-") {
                        format!("<networkInterfaceId>{}</networkInterfaceId>", xml_escape(t))
                    } else {
                        format!("<gatewayId>{}</gatewayId>", xml_escape(t))
                    }
                }
                None => String::new(),
            },
            xml_escape(&r.origin),
            xml_escape(&r.state)
        );
    }
    let mut associations = String::new();
    for (assoc_id, subnet_id, main) in &rt.associations {
        let _ = write!(
            associations,
            "<item><routeTableAssociationId>{}</routeTableAssociationId>{}<main>{}</main></item>",
            xml_escape(assoc_id),
            match subnet_id {
                Some(v) => format!("<subnetId>{}</subnetId>", xml_escape(v)),
                None => String::new(),
            },
            main
        );
    }
    format!(
        "<item><ownerId>{}</ownerId><associationSet>{}</associationSet><propagatingVgwSet/><routeTableId>{}</routeTableId><routeSet>{}</routeSet>{}<tagSet/><vpcId>{}</vpcId></item>",
        account,
        associations,
        xml_escape(&rt.id),
        routes,
        "",
        xml_escape(&rt.vpc_id)
    )
}

fn create_route_table(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        let has_vpc = state.vpcs.iter().any(|v| v.id == vpc_id);
        if !has_vpc {
            return ec2_error(
                400,
                "InvalidVpcID.NotFound",
                &format!("Vpc {{{}}} not found", xml_escape(&vpc_id)),
            );
        }
        let rt_id = state.next_id("rtb");
        let vpc_cidr = state
            .vpcs
            .iter()
            .find(|v| v.id == vpc_id)
            .map(|v| v.cidr.clone())
            .unwrap_or_default();
        let tags = wire_tags(params);
        let mut rt = RouteTable {
            id: rt_id.clone(),
            vpc_id: vpc_id.clone(),
            routes: vec![Route {
                destination: vpc_cidr,
                kind: "cidr".into(),
                target: Some("local".into()),
                state: "active".into(),
                origin: "CreateRouteTable".into(),
            }],
            ..Default::default()
        };
        if !tags.is_empty() {
            rt.tags = tags;
        }
        state.route_tables.push(rt);
        ec2_xml(
            "CreateRouteTableResponse",
            &format!(
                "<routeTable><routeTableId>{}</routeTableId><ownerId>{}</ownerId><vpcId>{}</vpcId><routeSet><item><destinationCidrBlock>{}</destinationCidrBlock><gatewayId>local</gatewayId><state>active</state><origin>CreateRouteTable</origin></item></routeSet><associationSet/><propagatingVgwSet/><tagSet/></routeTable>",
                xml_escape(&rt_id),
                req.account,
                xml_escape(&vpc_id),
                xml_escape(&state
                    .vpcs
                    .iter()
                    .find(|v| v.id == vpc_id)
                    .map(|v| v.cidr.clone())
                    .unwrap_or_default())
            ),
        )
    })
}

fn delete_route_table(req: &Request, params: &P) -> Response {
    let rt_id = params.str("RouteTableId");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        if !state.route_tables[pos].associations.is_empty() {
            return ec2_error(
                400,
                "DependencyViolation",
                "The vpc with ID {} has dependencies and cannot be deleted",
            );
        }
        state.route_tables.remove(pos);
        ok_xml("DeleteRouteTableResponse")
    })
}

fn describe_route_tables(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("RouteTableId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = req_filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let snapshot: Vec<RouteTable> = state.route_tables.clone();
        let mut items = String::new();
        for rt in &snapshot {
            if !ids.is_empty() && !ids.contains(&rt.id) {
                continue;
            }
            let rt2 = rt.clone();
            if !filter_passes(&fs, move |f| match f {
                "route-table-id" => vec![rt2.id.clone()],
                "vpc-id" => vec![rt2.vpc_id.clone()],
                "route.destination-cidr-block" => {
                    rt2.routes.iter().map(|r| r.destination.clone()).collect()
                }
                "route.gateway-id" => rt2.routes.iter().filter_map(|r| r.target.clone()).collect(),
                "association.main" => vec![if rt2.associations.iter().any(|(_, _, m)| *m) {
                    "true".to_string()
                } else {
                    "false".to_string()
                }],
                other => tag_candidates(&rt2.tags, other),
            }) {
                continue;
            }
            let item = route_table_item(state, account, rt);
            items.push_str(&item);
        }
        ec2_xml(
            "DescribeRouteTablesResponse",
            &format!("<routeTableSet>{}</routeTableSet>", items),
        )
    })
}

fn allocate_address(req: &Request, params: &P) -> Response {
    let domain = params
        .get("Domain")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let aws_vpc = !domain.is_empty();
    with_state(req, move |state| {
        let public_ip = random_public_ip();

        let body = if aws_vpc {
            let alloc_id = state.next_id("eipalloc");
            state.addresses.push(super::state::ElasticIp {
                allocation_id: alloc_id.clone(),
                public_ip: public_ip.clone(),
                domain: "vpc".into(),
                ..Default::default()
            });
            format!(
                "<publicIp>{}</publicIp><domain>vpc</domain><allocationId>{}</allocationId>",
                xml_escape(&public_ip),
                xml_escape(&alloc_id)
            )
        } else {
            state.addresses.push(super::state::ElasticIp {
                public_ip: public_ip.clone(),
                ..Default::default()
            });
            format!("<publicIp>{}</publicIp>", xml_escape(&public_ip))
        };
        ec2_xml("AllocateAddressResponse", &body)
    })
}

/// Resolve a route target param from the request (gateway/instance/nat/peering/tgw/eigw/vpce/eni).
fn route_target(params: &P) -> Option<(String, String)> {
    let candidates: &[&str] = &[
        "GatewayId",
        "InstanceId",
        "NetworkInterfaceId",
        "NatGatewayId",
        "VpcPeeringConnectionId",
        "TransitGatewayId",
        "EgressOnlyInternetGatewayId",
        "VpcEndpointId",
        "CarrierGatewayId",
        "LocalGatewayId",
    ];
    for key in candidates {
        if let Some(v) = params.get(key) {
            if !v.is_empty() {
                return Some((key.to_string(), v.to_string()));
            }
        }
    }
    None
}

fn route_in(state: &Ec2State, rt: &RouteTable, destination: &str) -> Option<usize> {
    rt.routes.iter().position(|r| r.destination == destination)
}

fn create_route(req: &Request, params: &P) -> Response {
    let rt_id = params.str("RouteTableId");
    let cidr = params.str("DestinationCidrBlock");
    let ipv6 = params.str("DestinationIpv6CidrBlock");
    let pl = params.str("DestinationPrefixListId");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        if cidr.is_empty() && ipv6.is_empty() && pl.is_empty() {
            return ec2_error(
                400,
                "MissingParameter",
                "The request must contain the parameter destinationCidrBlock or destinationIpv6CidrBlock or destinationPrefixListId",
            );
        }
        let destination = if !cidr.is_empty() {
            cidr.clone()
        } else if !ipv6.is_empty() {
            ipv6.clone()
        } else {
            pl.clone()
        };
        let target = match route_target(params) {
            Some((_k, v)) => Some(v),
            None => {
                return ec2_error(
                    400,
                    "MissingParameter",
                    "The request must contain at least one route target",
                )
            }
        };
        let existing = route_in(state, &state.route_tables[pos], &destination);
        if let Some(_i) = existing {
            return ec2_error(
                400,
                "InvalidParameterValue",
                "The route already exists in the route table",
            );
        }
        state.route_tables[pos].routes.push(Route {
            destination: destination.clone(),
            kind: if !ipv6.is_empty() {
                "ipv6".into()
            } else if !pl.is_empty() {
                "pl".into()
            } else {
                "cidr".into()
            },
            target,
            state: "active".into(),
            origin: "CreateRoute".into(),
        });
        ok_xml("CreateRouteResponse")
    })
}

fn delete_route(req: &Request, params: &P) -> Response {
    let rt_id = params.str("RouteTableId");
    let cidr = params.str("DestinationCidrBlock");
    let ipv6 = params.str("DestinationIpv6CidrBlock");
    let pl = params.str("DestinationPrefixListId");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        let destination = if !cidr.is_empty() {
            cidr.clone()
        } else if !ipv6.is_empty() {
            ipv6.clone()
        } else {
            pl.clone()
        };
        let before = state.route_tables[pos].routes.len();
        state.route_tables[pos]
            .routes
            .retain(|r| r.destination != destination);
        if state.route_tables[pos].routes.len() == before {
            return ec2_error(
                400,
                "InvalidRoute.NotFound",
                &format!(
                    "no route with destination-cidr-block '{}' in route table {}",
                    xml_escape(destination.as_str()),
                    xml_escape(rt_id.as_str())
                ),
            );
        }
        ok_xml("DeleteRouteResponse")
    })
}

fn replace_route(req: &Request, params: &P) -> Response {
    let rt_id = params.str("RouteTableId");
    let cidr = params.str("DestinationCidrBlock");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        if let Some(_i) = route_in(state, &state.route_tables[pos], &cidr) {
            let mut routes = state.route_tables[pos].routes.clone();
            if let Some(idx) = routes.iter().position(|r| r.destination == cidr.clone()) {
                routes[idx].target = route_target(params).map(|(_k, v)| v);
                state.route_tables[pos].routes = routes;
            }
            ok_xml("ReplaceRouteResponse")
        } else {
            create_route(req, params)
        }
    })
}

fn associate_route_table(req: &Request, params: &P) -> Response {
    let rt_id = params.str("RouteTableId");
    let subnet_id = params.str("SubnetId");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        if !subnet_id.is_empty() {
            if !state.subnets.iter().any(|s| s.id == subnet_id) {
                return ec2_error(
                    400,
                    "InvalidSubnetID.NotFound",
                    &format!("The subnet ID '{}' does not exist", xml_escape(&subnet_id)),
                );
            }
            _remove_existing_assoc(state, &subnet_id);
        }
        let assoc_id = state.next_id("rtbassoc");
        state.route_tables[pos].associations.push((
            assoc_id.clone(),
            if subnet_id.is_empty() {
                None
            } else {
                Some(subnet_id.clone())
            },
            false,
        ));
        ec2_xml(
            "AssociateRouteTableResponse",
            &format!("<associationId>{}</associationId>", xml_escape(&assoc_id)),
        )
    })
}

fn _remove_existing_assoc(state: &mut Ec2State, subnet_id: &str) {
    for rt in state.route_tables.iter_mut() {
        rt.associations
            .retain(|(_, s, _)| s.as_deref() != Some(subnet_id));
    }
}

fn disassociate_route_table(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    with_state(req, move |state| {
        for pos in 0..state.route_tables.len() {
            let had = state.route_tables[pos].associations.len();
            state.route_tables[pos]
                .associations
                .retain(|(a, _, _)| a != &assoc_id);
            if had != state.route_tables[pos].associations.len() {
                return ok_xml("DisassociateRouteTableResponse");
            }
        }
        ec2_error(
            400,
            "InvalidAssociationID.NotFound",
            &format!(
                "The association ID '{}' does not exist",
                xml_escape(&assoc_id)
            ),
        )
    })
}

fn replace_route_table_association(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    let rt_id = params.str("RouteTableId");
    with_state(req, move |state| {
        if require_rt(state, &rt_id).is_err() {
            return require_rt(state, &rt_id).err().unwrap();
        }
        // find the old association anywhere
        let mut found: Option<(usize, (String, Option<String>, bool))> = None;
        for (pos, rt) in state.route_tables.iter().enumerate() {
            if let Some(a) = rt.associations.iter().find(|(a, _, _)| a == &assoc_id) {
                found = Some((pos, a.clone()));
                break;
            }
        }
        let (_old_pos, (assoc_id, subnet_id, _main)) = match found {
            Some(x) => x,
            None => {
                return ec2_error(
                    400,
                    "InvalidAssociationID.NotFound",
                    &format!(
                        "The association ID '{}' does not exist",
                        xml_escape(&assoc_id)
                    ),
                )
            }
        };
        // remove from old table
        for rt in state.route_tables.iter_mut() {
            rt.associations.retain(|(a, _, _)| a != &assoc_id);
        }
        let pos = state
            .route_tables
            .iter()
            .position(|t| t.id == rt_id)
            .unwrap_or(0);
        let new_assoc = state.next_id("rtbassoc");
        state.route_tables[pos]
            .associations
            .push((new_assoc.clone(), subnet_id.clone(), false));
        ec2_xml(
            "ReplaceRouteTableAssociationResponse",
            &format!(
                "<newAssociationId>{}</newAssociationId>",
                xml_escape(&new_assoc)
            ),
        )
    })
}
