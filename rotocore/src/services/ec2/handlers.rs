//! EC2 handler group 1: metadata ops, tags, the VPC family, subnets.
//!
#![allow(
    clippy::format_in_format_args,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::single_match,
    clippy::map_identity
)]
//! XML shapes mirror moto's templates so botocore parses them identically.
//! Every handler acquires the (account, region) state write lock exactly
//! once via `with_state` and never re-acquires (deadlock rule).

#![allow(unused_variables)]

use super::state::{idhex, Ec2State, Route, RouteTable, SecurityGroup, Subnet, Tags, Vpc};
use super::{ec2_error, ec2_xml, tags_xml, with_state, Params};
use crate::proto::xml_escape;
use crate::{Request, Response};
use std::fmt::Write as _;

type P = Params;

// ---------------------------------------------------------------------------
// Shared helpers (public: other handler groups use them too)
// ---------------------------------------------------------------------------

/// Parse request `Filter.*` entries into (name, values).
pub fn filters(params: &P) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for i in params.container_indices("Filter") {
        let name = params.sub("Filter", i, "Name").unwrap_or("").to_string();
        let values = params.sub_list("Filter", i, "Value");
        out.push((name, values));
    }
    out
}

/// moto's generic filter model: every filter must find a matching candidate.
pub fn filter_passes(
    fs: &[(String, Vec<String>)],
    mut candidates: impl FnMut(&str) -> Vec<String>,
) -> bool {
    for (name, values) in fs {
        let cands = candidates(name);
        if !values.iter().any(|v| cands.iter().any(|c| c == v)) {
            return false;
        }
    }
    true
}

/// Candidate values a resource's tags contribute for standard tag filters.
pub fn tag_candidates(tags: &[(String, String)], name: &str) -> Vec<String> {
    if name == "tag" {
        return tags
            .iter()
            .flat_map(|(k, v)| [k.clone(), v.clone()])
            .collect();
    }
    if name == "tag-key" {
        return tags.iter().map(|(k, _)| k.clone()).collect();
    }
    if name == "tag-value" {
        return tags.iter().map(|(_, v)| v.clone()).collect();
    }
    if let Some(key) = name.strip_prefix("tag:") {
        return tags
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .collect();
    }
    Vec::new()
}

/// `<return>true</return>` no-op response for `root` action.
pub fn ok_xml(root: &str) -> Response {
    ec2_xml(root, "<return>true</return>")
}

// ---------------------------------------------------------------------------
// Group-1 dispatch
// ---------------------------------------------------------------------------

pub fn route(req: &Request, params: &P) -> Response {
    match req.operation.as_str() {
        // Metadata
        "DescribeRegions" => describe_regions(req, params),
        "DescribeAvailabilityZones" => describe_availability_zones(req, params),
        "DescribeAccountAttributes" => describe_account_attributes(req, params),
        "DescribeIdFormat" => describe_id_format(req, params),
        "DescribeAggregateIdFormat" => describe_aggregate_id_format(req, params),

        // Tags
        "CreateTags" => create_tags(req, params),
        "DeleteTags" => delete_tags(req, params),
        "DescribeTags" => describe_tags(req, params),

        // VPC family
        "CreateVpc" => create_vpc(req, params),
        "DeleteVpc" => delete_vpc(req, params),
        "DescribeVpcs" => describe_vpcs(req, params),
        "DescribeVpcAttribute" => describe_vpc_attribute(req, params),
        "ModifyVpcAttribute" => modify_vpc_attribute(req, params),
        "CreateDefaultVpc" => create_default_vpc(req, params),
        "AssociateVpcCidrBlock" => associate_vpc_cidr_block(req, params),
        "DisassociateVpcCidrBlock" => disassociate_vpc_cidr_block(req, params),
        "ModifyVpcTenancy" => modify_vpc_tenancy(req, params),
        "EnableVpcClassicLink" => set_classic_link(req, params, true),
        "DisableVpcClassicLink" => set_classic_link(req, params, false),
        "DescribeVpcClassicLink" => describe_classic_link(req, params),
        "DescribeVpcClassicLinkDnsSupport" => describe_classic_link_dns(req, params),
        "DescribeVpcBlockPublicAccessOptions" => vpc_bpa_options(req, params),
        "DescribeVpcBlockPublicAccessExclusions" => vpc_bpa_exclusions(req, params),
        "DeleteVpcBlockPublicAccessExclusion" => not_found_bpa(req, params),
        "PutVpcBlockPublicAccessExclusion" => not_found_bpa(req, params),
        "DescribeVpcEncryptionControls" => vpc_encryption_controls(req, params),

        // Subnets
        "CreateSubnet" => create_subnet(req, params),
        "DeleteSubnet" => delete_subnet(req, params),
        "DescribeSubnets" => describe_subnets(req, params),
        "CreateDefaultSubnet" => create_default_subnet(req, params),
        "ModifySubnetAttribute" => modify_subnet_attribute(req, params),
        "AssociateSubnetCidrBlock" => associate_subnet_cidr_block(req, params),
        "DisassociateSubnetCidrBlock" => disassociate_subnet_cidr_block(req, params),
        "CreateSubnetCidrReservation" => create_subnet_cidr_reservation(req, params),
        "DeleteSubnetCidrReservation" => delete_subnet_cidr_reservation(req, params),
        "GetSubnetCidrReservations" => get_subnet_cidr_reservations(req, params),

        _ => super::handlers_net::route(req, params),
    }
}

// ---------------------------------------------------------------------------
// Regions / AZs / account attributes / id formats
// ---------------------------------------------------------------------------

const REGIONS: &[&str] = &[
    "af-south-1",
    "ap-east-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-central-1",
    "ca-west-1",
    "cn-north-1",
    "cn-northwest-1",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
];

fn describe_regions(req: &Request, params: &P) -> Response {
    let wanted: Vec<String> = params
        .list("RegionName")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = filters(params);
    let mut body = String::new();
    for region in REGIONS {
        if !wanted.is_empty() && !wanted.iter().any(|w| w == region) {
            continue;
        }
        if !filter_passes(&fs, |f| tag_candidates(&[], f)) {
            continue;
        }
        let _ = write!(
            body,
            "<item><regionName>{}</regionName><regionEndpoint>{}.amazonaws.com</regionEndpoint></item>",
            xml_escape(region),
            xml_escape(region)
        );
    }
    ec2_xml(
        "DescribeRegionsResponse",
        &format!("<regionInfo>{}</regionInfo>", body),
    )
}

fn describe_availability_zones(req: &Request, params: &P) -> Response {
    let region = req.region.clone();
    let names: Vec<String> = params
        .list("ZoneName")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let filterss = filters(params);
    let known: Vec<(&str, &str)> = super::azones::AZONES
        .iter()
        .filter(|(r, _, _)| r == &region)
        .map(|(_, n, z)| (*n, *z))
        .collect();
    let zones: Vec<(String, String)> = if known.is_empty() {
        let base = region.split('-').next().unwrap_or("az").replace("-", "");
        (0..6)
            .map(|i| {
                (
                    format!("{region}{}", char::from(b'a' + i as u8)),
                    format!("{base}{:02?}-az{}", i + 1, i + 1),
                )
            })
            .collect()
    } else {
        known
            .iter()
            .map(|(n, z)| (n.to_string(), z.to_string()))
            .collect()
    };
    let mut body = String::new();
    for (idx, (az, az_id)) in zones.iter().enumerate() {
        if !names.is_empty() && !names.iter().any(|n| n == az) {
            continue;
        }
        let az2 = az.clone();
        let region2 = region.clone();
        if !filter_passes(&filterss, move |f| match f {
            "zoneName" | "zone-name" => vec![az2.clone()],
            "region-name" => vec![region2.clone()],
            "zoneType" | "zone-type" => vec!["availability-zone".to_string()],
            "zoneState" | "zone-state" | "state" => vec!["available".to_string()],
            "zoneId" | "zone-id" => vec![az_id.to_string()],
            _ => Vec::new(),
        }) {
            continue;
        }
        let _ = idx;
        let _ = write!(
            body,
            "<item><regionName>{}</regionName><zoneName>{}</zoneName><zoneId>{}</zoneId><zoneState>available</zoneState><zoneType>availability-zone</zoneType><messages/></item>",
            xml_escape(&region),
            xml_escape(az),
            xml_escape(az_id)
        );
    }
    ec2_xml(
        "DescribeAvailabilityZonesResponse",
        &format!("<availabilityZoneInfo>{}</availabilityZoneInfo>", body),
    )
}

fn describe_account_attributes(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeAccountAttributesResponse",
        r#"<accountAttributeSet><item><attributeName>supported-platforms</attributeName><attributeValueSet><item><attributeValue>EC2</attributeValue></item><item><attributeValue>VPC</attributeValue></item></attributeValueSet></item><item><attributeName>default-vpc</attributeName><attributeValueSet><item><attributeValue>none</attributeValue></item></attributeValueSet></item><item><attributeName>vpc-max-security-groups-per-interface</attributeName><attributeValueSet><item><attributeValue>5</attributeValue></item></attributeValueSet></item><item><attributeName>max-elastic-ips</attributeName><attributeValueSet><item><attributeValue>5</attributeValue></item></attributeValueSet></item><item><attributeName>max-instances</attributeName><attributeValueSet><item><attributeValue>20</attributeValue></item></attributeValueSet></item><item><attributeName>vpc-max-elastic-ips</attributeName><attributeValueSet><item><attributeValue>5</attributeValue></item></attributeValueSet></item></accountAttributeSet>"#,
    )
}

fn describe_id_format(req: &Request, params: &P) -> Response {
    ec2_xml("DescribeIdFormatResponse", "<statusSet/>")
}

fn describe_aggregate_id_format(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeAggregateIdFormatResponse",
        "<result>true</result><longIds>true</longIds><OrganizationsWithLongIds/><OnBehalfOf/>",
    )
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

/// One `resourceTags` entry in DescribeTags aggregate.
pub struct TagRow {
    pub id: String,
    pub kind: String,
    pub tags: Tags,
}

fn describe_tags(req: &Request, params: &P) -> Response {
    let fs = filters(params);
    let rows: Vec<(String, String, Tags)> = with_state(req, |state| state.all_tag_rows());
    let mut body = String::new();
    for (id, kind, tags) in &rows {
        for (k, v) in tags {
            let name = k.clone();
            let value = v.clone();
            if !filter_passes(&fs, move |f| match f {
                "resource-id" => vec![id.clone()],
                "resource-type" => vec![kind.clone()],
                "key" => vec![name.clone()],
                "value" => vec![value.clone()],
                t => tag_candidates(&[(name.clone(), value.clone())], t),
            }) {
                continue;
            }
            let _ = write!(
                body,
                "<item><resourceId>{}</resourceId><resourceType>{}</resourceType><key>{}</key><value>{}</value></item>",
                xml_escape(id),
                xml_escape(kind),
                xml_escape(k),
                xml_escape(v)
            );
        }
    }
    ec2_xml(
        "DescribeTagsResponse",
        &format!("<tagSet>{}</tagSet>", body),
    )
}

pub fn replace_tags(state: &mut Ec2State, ids: &[String], tags: &[(String, String)]) {
    for id in ids {
        let id: &str = id;
        if let Some(vpc) = state.vpcs.iter_mut().find(|v| v.id == id) {
            vpc.tags = replace_tag_list(&vpc.tags, tags);
        }
        if let Some(sub) = state.subnets.iter_mut().find(|v| v.id == id) {
            sub.tags = replace_tag_list(&sub.tags, tags);
        }
        if let Some(sg) = state.security_groups.iter_mut().find(|v| v.id == id) {
            sg.tags = replace_tag_list(&sg.tags, tags);
        }
        if let Some(rt) = state.route_tables.iter_mut().find(|v| v.id == id) {
            rt.tags = replace_tag_list(&rt.tags, tags);
        }
        if let Some(igw) = state.internet_gateways.iter_mut().find(|v| v.id == id) {
            igw.tags = replace_tag_list(&igw.tags, tags);
        }
        if let Some(nat) = state.nat_gateways.iter_mut().find(|v| v.id == id) {
            nat.tags = replace_tag_list(&nat.tags, tags);
        }
        if let Some(eip) = state.addresses.iter_mut().find(|v| v.allocation_id == id) {
            eip.tags = replace_tag_list(&eip.tags, tags);
        }
        if let Some(eni) = state.enis.iter_mut().find(|v| v.id == id) {
            eni.tags = replace_tag_list(&eni.tags, tags);
        }
        if let Some(v) = state.instances.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.images.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.volumes.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.snapshots.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.key_pairs.iter_mut().find(|v| v.name == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.placement_groups.iter_mut().find(|v| v.name == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.launch_templates.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.vpc_endpoints.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.vpc_endpoint_services.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.vpc_peerings.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.dhcp_options.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.network_acls.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.transit_gateways.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.tgw_attachments.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        if let Some(v) = state.tgw_route_tables.iter_mut().find(|v| v.id == id) {
            v.tags = replace_tag_list(&v.tags, tags);
        }
        for fam in state.simple.values_mut() {
            if let Some(v) = fam.iter_mut().find(|v| v.id == id) {
                v.tags = replace_tag_list(&v.tags, tags);
            }
        }
    }
}

pub fn replace_tag_list(existing: &Tags, new: &[(String, String)]) -> Tags {
    // TagSpecifications overwrite values for existing keys, keep others.
    let mut merged: Vec<(String, String)> = existing.clone();
    for (k, v) in new {
        if let Some(slot) = merged.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    merged
}

fn remove_tags(state: &mut Ec2State, ids: &[String], tags: &[(String, String)]) {
    for id in ids {
        let id: &str = id;
        if let Some(vpc) = state.vpcs.iter_mut().find(|v| v.id == id) {
            vpc.tags
                .retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.subnets.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.security_groups.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.route_tables.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.internet_gateways.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.nat_gateways.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.addresses.iter_mut().find(|v| v.allocation_id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.enis.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.instances.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.images.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.volumes.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.snapshots.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.key_pairs.iter_mut().find(|v| v.name == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.placement_groups.iter_mut().find(|v| v.name == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.launch_templates.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.vpc_endpoints.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.vpc_endpoint_services.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.vpc_peerings.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.dhcp_options.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.network_acls.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.transit_gateways.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.tgw_attachments.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        if let Some(s) = state.tgw_route_tables.iter_mut().find(|v| v.id == id) {
            s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
        }
        for fam in state.simple.values_mut() {
            if let Some(s) = fam.iter_mut().find(|v| v.id == id) {
                s.tags.retain(|(k, _)| !tags.iter().any(|(tk, _)| tk == k));
            }
        }
    }
}

/// Parse `Tag.N.Key/Value` request params into a list.
pub fn wire_tags(params: &P) -> Tags {
    let mut out: Tags = Vec::new();
    for i in params.container_indices("Tag") {
        let k = params.sub("Tag", i, "Key").unwrap_or("").to_string();
        let v = params.sub("Tag", i, "Value").unwrap_or("").to_string();
        if !k.is_empty() {
            out.push((k, v));
        }
    }
    out
}

/// Tags from `TagSpecification(s).N.Tag.M.Key/Value` (RunInstances-style).
pub fn tag_spec_tags(params: &P) -> Tags {
    let mut out: Tags = Vec::new();
    // botocore flattens TagSpecifications with either a singular or plural
    // member name depending on the operation model — accept both wire forms.
    for fam in ["TagSpecification", "TagSpecifications"] {
        for i in params.container_indices(fam) {
            let base = format!("{}.{}.Tag", fam, i + 1);
            for t in params.container_indices(&base) {
                let k = params.sub(&base, t, "Key").unwrap_or("").to_string();
                let v = params.sub(&base, t, "Value").unwrap_or("").to_string();
                if !k.is_empty() {
                    out.push((k, v));
                }
            }
        }
    }
    out
}

fn create_tags(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("ResourceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let tags = wire_tags(params);
    with_state(req, move |state| {
        replace_tags(state, &ids, &tags);
        ok_xml("CreateTagsResponse")
    })
}

fn delete_tags(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("ResourceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let tags = wire_tags(params);
    with_state(req, move |state| {
        remove_tags(state, &ids, &tags);
        ok_xml("DeleteTagsResponse")
    })
}

// ---------------------------------------------------------------------------
// VPC family
// ---------------------------------------------------------------------------

pub fn vpc_fields(account: u64, v: &Vpc) -> String {
    let mut cidrs = format!(
        "<item><associationId>{}</associationId><cidrBlock>{}</cidrBlock><cidrBlockState><state>associated</state></cidrBlockState></item>",
        xml_escape(&v.cidr_assoc_id),
        xml_escape(&v.cidr)
    );
    for (assoc_id, cidr, state) in &v.extra_cidrs {
        let _ = write!(
            cidrs,
            "<item><associationId>{}</associationId><cidrBlock>{}</cidrBlock><cidrBlockState><state>{}</state></cidrBlockState></item>",
            xml_escape(assoc_id),
            xml_escape(cidr),
            xml_escape(state)
        );
    }
    format!(
        "<ownerId>{}</ownerId><instanceTenancy>{}</instanceTenancy><ipv6CidrBlockAssociationSet/><cidrBlockAssociationSet>{}</cidrBlockAssociationSet><isDefault>{}</isDefault>{}<vpcId>{}</vpcId><state>{}</state><cidrBlock>{}</cidrBlock><dhcpOptionsId>{}</dhcpOptionsId>",
        account,
        xml_escape(&v.tenancy),
        cidrs,
        v.is_default,
        tags_xml(&v.tags),
        xml_escape(&v.id),
        xml_escape(&v.state),
        xml_escape(&v.cidr),
        xml_escape(&v.dhcp_options_id)
    )
}
pub fn vpc_item(account: u64, v: &Vpc) -> String {
    format!("<item>{}</item>", vpc_fields(account, v))
}

fn require_vpc<'a>(state: &'a mut Ec2State, vpc_id: &str) -> Result<&'a mut Vpc, Response> {
    match state.vpcs.iter_mut().find(|v| v.id == vpc_id) {
        Some(v) => Ok(v),
        None => Err(ec2_error(
            400,
            "InvalidVpcID.NotFound",
            &format!("Vpc {{{}}} not found", xml_escape(vpc_id)),
        )),
    }
}

fn create_vpc(req: &Request, params: &P) -> Response {
    let cidr = params.str("CidrBlock");
    if cidr.is_empty() {
        return ec2_error(
            400,
            "MissingParameter",
            "The request must contain the parameter CidrBlock",
        );
    }
    let tenancy = params.str("InstanceTenancy");
    let tenancy = if tenancy.is_empty() {
        "default".to_string()
    } else {
        tenancy
    };
    with_state(req, move |state| {
        let vpc_id = state.next_id("vpc");
        let assoc = state.next_id("vpc-cidr-assoc");
        let mine = tag_spec_tags(params);
        state.vpcs.push(Vpc {
            id: vpc_id.clone(),
            cidr: cidr.clone(),
            tenancy: tenancy.clone(),
            state: "available".into(),
            is_default: false,
            dhcp_options_id: "default".into(),
            cidr_assoc_id: assoc,
            enable_dns_support: false,
            enable_dns_hostnames: false,
            ..Default::default()
        });
        replace_tags(state, std::slice::from_ref(&vpc_id), &mine);
        // moto pairs a default security group with each new VPC.
        let sg = SecurityGroup {
            id: state.next_id("sg"),
            name: "default".into(),
            description: "default VPC security group".into(),
            vpc_id: vpc_id.clone(),
            owner_id: req.account.to_string(),
            ..Default::default()
        };
        state.security_groups.push(sg);
        // And a main route table.
        let rt_id = state.next_id("rtb");
        state.route_tables.push(RouteTable {
            id: rt_id,
            vpc_id: vpc_id.clone(),
            routes: vec![Route {
                destination: cidr.clone(),
                kind: "cidr".into(),
                target: Some("local".into()),
                state: "active".into(),
                origin: "CreateRouteTable".into(),
            }],
            ..Default::default()
        });
        let vpc = state.vpcs.last().unwrap().clone();
        ec2_xml(
            "CreateVpcResponse",
            &format!("<vpc>{}</vpc>", vpc_fields(req.account, &vpc)),
        )
    })
}

fn delete_vpc(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        let pos = match state.vpcs.iter().position(|v| v.id == vpc_id) {
            Some(i) => i,
            None => return require_vpc(state, &vpc_id).err().unwrap(),
        };
        state.vpcs.remove(pos);
        state.subnets.retain(|s| s.vpc_id != vpc_id);
        state.security_groups.retain(|s| s.vpc_id != vpc_id);
        state.route_tables.retain(|s| s.vpc_id != vpc_id);
        state.network_acls.retain(|s| s.vpc_id != vpc_id);
        state
            .internet_gateways
            .retain(|g| !g.attachments.iter().any(|(v, _)| v == &vpc_id));
        ok_xml("DeleteVpcResponse")
    })
}

fn describe_vpcs(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params.list("VpcId").iter().map(|s| s.to_string()).collect();
    let fs = filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let matches: Vec<(usize, Vpc)> = state
            .vpcs
            .iter()
            .enumerate()
            .filter(|(_, v)| ids.is_empty() || ids.iter().map(|s| s.as_str()).any(|s| s == v.id))
            .filter(|(_, v)| {
                let vpc_id = v.id.clone();
                filter_passes(&fs, |f| match f {
                    "vpc-id" => vec![vpc_id.clone()],
                    "cidr" | "cidr-block" => vec![v.cidr.clone()],
                    "owner-id" => vec![account.to_string()],
                    "state" => vec![v.state.clone()],
                    "dhcp-options-id" | "dhcpOptionsId" => vec![v.dhcp_options_id.clone()],
                    "is-default" | "isDefault" => vec![if v.is_default {
                        "true".to_string()
                    } else {
                        "false".to_string()
                    }],
                    "instance-tenancy" => vec![v.tenancy.clone()],
                    tagname => tag_candidates(&v.tags, f),
                })
            })
            .map(|(i, v)| (i, v.clone()))
            .collect();
        let items: Vec<String> = matches.iter().map(|(_, v)| vpc_item(account, v)).collect();
        ec2_xml(
            "DescribeVpcsResponse",
            &format!("<vpcSet>{}</vpcSet>", items.concat()),
        )
    })
}

fn vpc_attribute_response(root: &str, attr_xml: &str) -> Response {
    ec2_xml(root, attr_xml)
}

fn describe_vpc_attribute(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    let attr = params.str("Attribute");
    with_state(req, move |state| {
        let v = match require_vpc(state, &vpc_id) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let enable_dns_support = v.enable_dns_support;
        let enable_dns_hostnames = v.enable_dns_hostnames;
        let network_address_usage_metrics = v.naive_metrics;
        match attr.as_str() {
            "enableDnsSupport" => vpc_attribute_response(
                "DescribeVpcAttributeResponse",
                &format!("<vpcId>{}</vpcId><enableDnsSupport><value>{}</value></enableDnsSupport>", xml_escape(&vpc_id), enable_dns_support),
            ),
            "enableDnsHostnames" => vpc_attribute_response(
                "DescribeVpcAttributeResponse",
                &format!("<vpcId>{}</vpcId><enableDnsHostnames><value>{}</value></enableDnsHostnames>", xml_escape(&vpc_id), enable_dns_hostnames),
            ),
            "enableNetworkAddressUsageMetrics" => vpc_attribute_response(
                "DescribeVpcAttributeResponse",
                &format!("<vpcId>{}</vpcId><enableNetworkAddressUsageMetrics><value>{}</value></enableNetworkAddressUsageMetrics>", xml_escape(&vpc_id), network_address_usage_metrics),
            ),
            other => ec2_error(400, "InvalidParameterValue", &format!("Value ({other}) for parameter attribute is invalid. Unknown attribute.")),
        }
    })
}

fn modify_vpc_attribute(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    let dns_support = params.bool("EnableDnsSupport.Value");
    let dns_hostnames = params.bool("EnableDnsHostnames.Value");
    let naive_metrics = params.bool("EnableNetworkAddressUsageMetrics.Value");
    with_state(req, move |state| {
        let pos = state.vpcs.iter().position(|v| v.id == vpc_id);
        let pos = match pos {
            Some(i) => i,
            None => return require_vpc(state, &vpc_id).err().unwrap(),
        };
        if let Some(b) = dns_support {
            state.vpcs[pos].enable_dns_support = b;
        }
        if let Some(b) = dns_hostnames {
            state.vpcs[pos].enable_dns_hostnames = b;
        }
        if let Some(b) = naive_metrics {
            state.vpcs[pos].naive_metrics = b;
        }
        ok_xml("ModifyVpcAttributeResponse")
    })
}

fn create_default_vpc(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        if state.default_vpc_regions.contains(&req.region) {
            return ec2_error(
                400,
                "DefaultVpcAlreadyExists",
                "A Default VPC already exists for this account in this region.",
            );
        }
        state.ensure_default_vpc(&req.region, req.account);
        let vpc = state.vpcs.last().unwrap().clone();
        ec2_xml(
            "CreateDefaultVpcResponse",
            &format!("<vpc>{}</vpc>", vpc_fields(req.account, &vpc)),
        )
    })
}

fn associate_vpc_cidr_block(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    let mut cidr = params.str("CidrBlock");
    if cidr.is_empty() {
        cidr = params.str("Ipv6CidrBlock");
    }
    let is_ipv6 = params.has("Ipv6CidrBlock")
        || params
            .get("Ipv6NetmaskLength")
            .is_some_and(|v| !v.is_empty());
    with_state(req, move |state| {
        if require_vpc(state, &vpc_id).is_err() {
            return require_vpc(state, &vpc_id).err().unwrap();
        }
        let assoc_id = if is_ipv6 {
            state.next_id("ipv6-cidr-assoc")
        } else {
            state.next_id("vpc-cidr-assoc")
        };
        let pos = state.vpcs.iter().position(|v| v.id == vpc_id).unwrap_or(0);
        state.vpcs[pos]
            .extra_cidrs
            .push((assoc_id.clone(), cidr.clone(), "associated".into()));
        let vpc = state.vpcs[pos].clone();
        ec2_xml(
            "AssociateVpcCidrBlockResponse",
            &format!(
                "<cidrBlockAssociation><associationId>{}</associationId><cidrBlock>{}</cidrBlock><cidrBlockState><state>associated</state></cidrBlockState></cidrBlockAssociation>",
                xml_escape(&assoc_id),
                xml_escape(&cidr)
            ),
        )
    })
}

fn disassociate_vpc_cidr_block(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    with_state(req, move |state| {
        let mut found: Option<(usize, (String, String, String))> = None;
        for (i, v) in state.vpcs.iter().enumerate() {
            if v.cidr_assoc_id == assoc_id {
                return ec2_error(
                    400,
                    "InvalidVpcCidrBlockAssociationID.NotFound",
                    &format!(
                        "The association ID '{}' does not exist",
                        xml_escape(&assoc_id)
                    ),
                );
            }
            if let Some(idx) = v.extra_cidrs.iter().position(|(a, _, _)| a == &assoc_id) {
                found = Some((i, v.extra_cidrs[idx].clone()));
                break;
            }
        }
        let (vpc_idx, (assoc_id, cidr, _)) = match found {
            Some(x) => x,
            None => {
                return ec2_error(
                    400,
                    "InvalidVpcCidrBlockAssociationID.NotFound",
                    &format!(
                        "The association ID '{}' does not exist",
                        xml_escape(&assoc_id)
                    ),
                )
            }
        };
        let mut extra = state.vpcs[vpc_idx].extra_cidrs.clone();
        extra.retain(|(a, _, _)| a != &assoc_id);
        state.vpcs[vpc_idx].extra_cidrs = extra;
        ec2_xml(
            "DisassociateVpcCidrBlockResponse",
            &format!(
                "<cidrBlockAssociation><associationId>{}</associationId><cidrBlock>{}</cidrBlock><cidrBlockState><state>disassociating</state></cidrBlockState></cidrBlockAssociation>",
                xml_escape(&assoc_id),
                xml_escape(&cidr)
            ),
        )
    })
}

fn modify_vpc_tenancy(req: &Request, params: &P) -> Response {
    ok_xml("ModifyVpcTenancyResponse")
}

fn set_classic_link(req: &Request, params: &P, enable: bool) -> Response {
    let vpc_id = params.str("VpcId");
    with_state(req, move |state| {
        if require_vpc(state, &vpc_id).is_err() {
            return require_vpc(state, &vpc_id).err().unwrap();
        }
        let pos = state.vpcs.iter().position(|v| v.id == vpc_id).unwrap_or(0);
        state.vpcs[pos].classic_link = enable;
        ec2_xml(
            "VpcClassicLinkResponse",
            &format!(
                "<vpcId>{}</vpcId><return>{}</return>",
                xml_escape(&vpc_id),
                enable
            ),
        )
    })
}

fn describe_classic_link(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params.list("VpcId").iter().map(|s| s.to_string()).collect();
    let fs = filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let items: Vec<String> = state
            .vpcs
            .iter()
            .filter(|v| !v.classic_link)
            .filter(|v| ids.is_empty() || ids.iter().map(|s| s.as_str()).any(|s| s == v.id))
            .filter(|v| filter_passes(&fs, |f| tag_candidates(&v.tags, f)))
            .map(|v| vpc_item(account, v))
            .collect();
        ec2_xml(
            "DescribeVpcClassicLinkResponse",
            &format!("<vpcSet>{}</vpcSet>", items.concat()),
        )
    })
}

fn describe_classic_link_dns(req: &Request, params: &P) -> Response {
    let mut body = String::new();
    for id in params.list("VpcId") {
        let _ = write!(
            body,
            "<item><vpcId>{}</vpcId><classicLinkDnsSupported>false</classicLinkDnsSupport></item>",
            xml_escape(id)
        );
    }
    ec2_xml(
        "DescribeVpcClassicLinkDnsSupportResponse",
        &format!("<vpcs>{}</vpcs>", body),
    )
}

fn vpc_bpa_options(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeVpcBlockPublicAccessOptionsResponse",
        "<vpcBlockPublicAccessOptions><internetGatewayBlockMode>off</internetGatewayBlockMode><lastUpdateTimestamp>2026-10-18T00:00:00.000Z</lastUpdateTimestamp></vpcBlockPublicAccessOptions>",
    )
}

fn vpc_bpa_exclusions(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeVpcBlockPublicAccessExclusionsResponse",
        "<exclusions/><nextToken/>",
    )
}

fn not_found_bpa(req: &Request, params: &P) -> Response {
    ec2_error(
        400,
        "InvalidVpcBlockPublicAccessExclusionId.NotFound",
        "The VPC Block Public Access exclusion ID does not exist.",
    )
}

fn vpc_encryption_controls(req: &Request, params: &P) -> Response {
    ec2_xml(
        "DescribeVpcEncryptionControlsResponse",
        "<vpcEncryptionControls/><nextToken/>",
    )
}

// ---------------------------------------------------------------------------
// Subnets
// ---------------------------------------------------------------------------

pub fn subnet_fields(region: &str, account: u64, s: &Subnet) -> String {
    let mut available_ips: i64 = 251;
    if let Some(bits_str) = s.cidr.split('/').nth(1) {
        if let Ok(bits) = bits_str.parse::<i64>() {
            if (16..=32).contains(&bits) {
                available_ips = (1i64 << (32 - bits).min(20)) - 5;
            }
        }
    }
    let mut ipv6_set = String::new();
    for (assoc, cidr6) in &s.ipv6_assocs {
        let _ = write!(
            ipv6_set,
            "<item><associationId>{}</associationId><ipv6CidrBlock>{}</ipv6CidrBlock><cidrBlockState><state>associated</state></cidrBlockState></item>",
            xml_escape(assoc),
            xml_escape(cidr6)
        );
    }
    let has_ipv6 = !ipv6_set.is_empty();
    format!(
        "<availabilityZoneId>{}</availabilityZoneId><ownerId>{}</ownerId><assignIpv6AddressOnCreation>{}</assignIpv6AddressOnCreation><ipv6CidrBlockAssociationSet>{}</ipv6CidrBlockAssociationSet>{}<subnetArn>arn:aws:ec2:{}:{}:subnet/{}</subnetArn><ipv6Native>false</ipv6Native><subnetId>{}</subnetId><state>{}</state><vpcId>{}</vpcId><cidrBlock>{}</cidrBlock><availableIpAddressCount>{}</availableIpAddressCount><availabilityZone>{}</availabilityZone><defaultForAz>{}</defaultForAz><mapPublicIpOnLaunch>{}</mapPublicIpOnLaunch>{}{}",
        xml_escape(&s.availability_zone_id),
        account,
        has_ipv6,
        ipv6_set,
        tags_xml(&s.tags),
        xml_escape(region),
        account,
        xml_escape(&s.id),
        xml_escape(&s.id),
        xml_escape(&s.state),
        xml_escape(&s.vpc_id),
        xml_escape(&s.cidr),
        available_ips,
        xml_escape(&s.az),
        s.is_default,
        s.map_public_ip,
        if s.enable_dns64 { "<enableDns64>true</enableDns64>" } else { "" },
        if s.map_customer_owned_ip { "<mapCustomerOwnedIpOnLaunch>true</mapCustomerOwnedIpOnLaunch>" } else { "" }
    )
}

fn subnet_passes(fs: &[(String, Vec<String>)], s: &Subnet) -> bool {
    filter_passes(fs, move |f| subnet_cand(s, f))
}

pub fn subnet_item(region: &str, account: u64, s: &Subnet) -> String {
    format!("<item>{}</item>", subnet_fields(region, account, s))
}
fn require_subnet(state: &mut Ec2State, subnet_id: &str) -> Result<usize, Response> {
    match state.subnets.iter().position(|s| s.id == subnet_id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidSubnetID.NotFound",
            &format!("The subnet ID '{}' does not exist", xml_escape(subnet_id)),
        )),
    }
}

fn create_subnet(req: &Request, params: &P) -> Response {
    let vpc_id = params.str("VpcId");
    let cidr = params.str("CidrBlock");
    let az = params
        .get("AvailabilityZone")
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}a", req.region));
    let ipv6 = params.get("Ipv6CidrBlock").map(|s| s.to_string());
    with_state(req, move |state| {
        if require_vpc(state, &vpc_id).is_err() {
            return require_vpc(state, &vpc_id).err().unwrap();
        }
        if cidr.is_empty() {
            return ec2_error(
                400,
                "MissingParameter",
                "The request must contain the parameter CidrBlock",
            );
        }
        let subnet_id = state.next_id("subnet");
        let mine = tag_spec_tags(params);
        let mut subnet = Subnet {
            id: subnet_id.clone(),
            vpc_id: vpc_id.clone(),
            cidr: cidr.clone(),
            az: az.clone(),
            state: "available".into(),
            ipv6_cidr: ipv6.clone(),
            map_public_ip: false,
            availability_zone_id: format!("{}use1-az6", req.region),
            ..Default::default()
        };
        subnet.ipv6_assocs = ipv6
            .map(|c| vec![(format!("subnet-cidr-assoc-{}", idhex::hex17()), c)])
            .unwrap_or_default();
        state.subnets.push(subnet);
        replace_tags(state, std::slice::from_ref(&subnet_id), &mine);
        let s = state
            .subnets
            .iter()
            .find(|s| s.id == subnet_id)
            .cloned()
            .unwrap_or_default();
        ec2_xml(
            "CreateSubnetResponse",
            &format!(
                "<subnet>{}</subnet>",
                subnet_fields(&req.region, req.account, &s)
            ),
        )
    })
}

fn subnet_cand(s: &Subnet, f: &str) -> Vec<String> {
    match f {
        "subnet-id" | "id" => vec![s.id.clone()],
        "vpc-id" => vec![s.vpc_id.clone()],
        "cidr" | "cidr-block" => vec![s.cidr.clone()],
        "state" => vec![s.state.clone()],
        "availability-zone" | "az" => vec![s.az.clone()],
        "default-for-az" => vec![if s.is_default {
            "true".to_string()
        } else {
            "false".to_string()
        }],
        "map-public-ip-on-launch" => vec![if s.map_public_ip {
            "true".to_string()
        } else {
            "false".to_string()
        }],
        "availability-zone-id" => vec![s.availability_zone_id.clone()],
        "subnet-arn" => vec![format!(
            "arn:aws:ec2:{}:{}:subnet/{}",
            "us-east-1", "acc", s.id
        )],
        t => tag_candidates(&s.tags, t),
    }
}

fn describe_subnets(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("SubnetId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = filters(params);
    with_state(req, move |state| {
        let account = req.account;
        let region = req.region.clone();
        let items: Vec<String> = state
            .subnets
            .iter()
            .filter(|s| ids.is_empty() || ids.contains(&s.id))
            .filter(|s| subnet_passes(&fs, s))
            .map(|s| subnet_item(&region, account, s))
            .collect();
        ec2_xml(
            "DescribeSubnetsResponse",
            &format!("<subnetSet>{}</subnetSet>", items.concat()),
        )
    })
}

fn delete_subnet(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    with_state(req, move |state| {
        let pos = match require_subnet(state, &subnet_id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        state.subnets.remove(pos);
        ok_xml("DeleteSubnetResponse")
    })
}

fn create_default_subnet(req: &Request, params: &P) -> Response {
    let az = params.str("AvailabilityZone");
    with_state(req, move |state| {
        if state.vpcs.iter().all(|v| !v.is_default) {
            state.ensure_default_vpc(&req.region, req.account);
        }
        let default_vpc = state
            .vpcs
            .iter()
            .find(|&v| v.is_default)
            .cloned()
            .unwrap_or_default();
        let subnet_id = state.next_id("subnet");
        let subnet = Subnet {
            id: subnet_id.clone(),
            vpc_id: default_vpc.id.clone(),
            cidr: "172.31.32.0/20".into(),
            az: az.clone(),
            state: "available".into(),
            availability_zone_id: format!("{}use1-az1", req.region),
            is_default: true,
            ..Default::default()
        };
        state.subnets.push(subnet.clone());
        ec2_xml(
            "CreateDefaultSubnetResponse",
            &format!(
                "<subnet>{}</subnet>",
                subnet_fields(&req.region, req.account, &subnet)
            ),
        )
    })
}

fn modify_subnet_attribute(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    with_state(req, move |state| {
        let pos = match require_subnet(state, &subnet_id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let s = &mut state.subnets[pos];
        if let Some(b) = params.bool("MapPublicIpOnLaunch.Value") {
            s.map_public_ip = b;
        }
        if let Some(b) = params.bool("MapCustomerOwnedIpOnLaunch.Value") {
            s.map_customer_owned_ip = b;
        }
        if let Some(b) = params.bool("EnableDns64.Value") {
            s.enable_dns64 = b;
        }
        if let Some(b) = params.bool("EnableLniAtDeviceIndex.Value") {
            s.enable_lni_at_device_index = b;
        }
        if let Some(pool) = params.get("CustomerOwnedIpv4Pool") {
            s.customer_owned_ipv4_pool = pool.to_string();
        }
        ok_xml("ModifySubnetAttributeResponse")
    })
}

fn associate_subnet_cidr_block(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    let cidr = params.str("Ipv6CidrBlock");
    with_state(req, move |state| {
        let pos = match require_subnet(state, &subnet_id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let assoc_id = state.next_id("subnet-cidr-assoc");
        state.subnets[pos].ipv6_cidr = Some(cidr.clone());
        state.subnets[pos]
            .ipv6_assocs
            .push((assoc_id.clone(), cidr.clone()));
        ec2_xml(
            "AssociateSubnetCidrBlockResponse",
            &format!("<associationId>{}</associationId>", xml_escape(&assoc_id)),
        )
    })
}

fn disassociate_subnet_cidr_block(req: &Request, params: &P) -> Response {
    let assoc_id = params.str("AssociationId");
    with_state(req, move |state| {
        for i in 0..state.subnets.len() {
            if state.subnets[i]
                .ipv6_assocs
                .iter()
                .any(|(a, _)| a == &assoc_id)
            {
                state.subnets[i].ipv6_assocs.retain(|(a, _)| a != &assoc_id);
                return ok_xml("DisassociateSubnetCidrBlockResponse");
            }
        }
        ec2_error(
            400,
            "InvalidSubnetCidrBlockAssociationID.NotFound",
            &format!(
                "The association ID '{}' does not exist",
                xml_escape(&assoc_id)
            ),
        )
    })
}

fn create_subnet_cidr_reservation(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    let cidr = params.str("Cidr");
    let rtype = params.str("ReservationType");
    with_state(req, move |state| {
        let pos = match require_subnet(state, &subnet_id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let reservation_id = state.next_id("scr");
        state.subnets[pos].cidr_reservations.push((
            reservation_id.clone(),
            cidr.clone(),
            rtype.clone(),
        ));
        ec2_xml(
            "CreateSubnetCidrReservationResponse",
            &format!(
                "<subnetCidrReservation><subnetCidrReservationId>{}</subnetCidrReservationId><subnetId>{}</subnetId><cidr>{}</cidr><reservationType>{}</reservationType></subnetCidrReservation>",
                xml_escape(&reservation_id),
                xml_escape(&subnet_id),
                xml_escape(&cidr),
                xml_escape(&rtype)
            ),
        )
    })
}

fn delete_subnet_cidr_reservation(req: &Request, params: &P) -> Response {
    let reserve_id = params.str("ReservationId");
    with_state(req, move |state| {
        for s in state.subnets.iter_mut() {
            let before = s.cidr_reservations.len();
            s.cidr_reservations.retain(|(id, _, _)| id != &reserve_id);
            if before != s.cidr_reservations.len() {
                return ok_xml("DeleteSubnetCidrReservationResponse");
            }
        }
        ec2_error(
            400,
            "InvalidSubnetCidrReservationId.NotFound",
            &format!(
                "The subnet CIDR reservation '{}' does not exist",
                xml_escape(&reserve_id)
            ),
        )
    })
}

fn get_subnet_cidr_reservations(req: &Request, params: &P) -> Response {
    let subnet_id = params.str("SubnetId");
    with_state(req, move |state| {
        let res = state
            .subnets
            .iter()
            .find(|s| s.id == subnet_id)
            .map(|s| s.cidr_reservations.clone())
            .unwrap_or_default();
        let mut body = String::new();
        for (id, cidr, rtype) in &res {
            let _ = write!(
                body,
                "<item><subnetCidrReservationId>{}</subnetCidrReservationId><subnetId>{}</subnetId><cidr>{}</cidr><reservationType>{}</reservationType></item>",
                xml_escape(id),
                xml_escape(&subnet_id),
                xml_escape(cidr),
                xml_escape(rtype)
            );
        }
        ec2_xml(
            "GetSubnetCidrReservationsResponse",
            &format!("<subnetCidrReservations>{}</subnetCidrReservations>", body),
        )
    })
}
