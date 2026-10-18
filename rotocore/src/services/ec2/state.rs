#![allow(dead_code)]

//! EC2 resource state, keyed by (account, region) like moto's backends.
//!
//! One `Ec2State` partition per (account, region). Handlers acquire the
//! partition's write lock exactly once per request (never re-acquired —
//! see the kinesis split_shard self-deadlock incident) and mutate freely.
//!
//! Major resource families are typed structs; minor create/list/tag/delete
//! families share [`Simple`] with a family name (wire prefixes stay in the
//! handlers).

use std::collections::{BTreeMap, HashSet};

pub type Tags = Vec<(String, String)>;

#[derive(Debug, Clone, Default)]
pub struct Ids {
    pub seq: u64,
}

pub mod idhex {
    /// 17 lowercase hex chars, moto's standard EC2 id shape.
    pub fn hex17() -> String {
        format!("{:032x}", uuid::Uuid::new_v4().as_u128())[..17].to_string()
    }

    /// 16 hex chars.
    pub fn hex16() -> String {
        format!("{:032x}", uuid::Uuid::new_v4().as_u128())[..16].to_string()
    }

    /// 8 hex chars.
    pub fn hex8() -> String {
        format!("{:032x}", uuid::Uuid::new_v4().as_u128())[..8].to_string()
    }
}

// ---------------------------------------------------------------------------
// Networking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Vpc {
    pub id: String,
    pub cidr: String,
    pub tenancy: String,
    pub state: String,
    pub is_default: bool,
    pub dhcp_options_id: String,
    pub cidr_assoc_id: String,
    pub extra_cidrs: Vec<(String, String, String)>, // (assoc_id, cidr, state)
    pub enable_dns_support: bool,
    pub enable_dns_hostnames: bool,
    pub naive_metrics: bool,
    pub classic_link: bool,
    pub classic_link_dns: bool,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct Subnet {
    pub id: String,
    pub vpc_id: String,
    pub cidr: String,
    pub az: String,
    pub state: String,
    pub ipv6_cidr: Option<String>,
    pub ipv6_assocs: Vec<(String, String)>, // (assoc_id, cidr)
    pub map_public_ip: bool,
    pub map_customer_owned_ip: bool,
    pub enable_dns64: bool,
    pub enable_lni_at_device_index: bool,
    pub availability_zone_id: String,
    pub is_default: bool,
    pub customer_owned_ipv4_pool: String,
    pub tags: Tags,
    pub cidr_reservations: Vec<(String, String, String)>, // (id, cidr, type)
}

#[derive(Debug, Clone, Default)]
pub struct IpPermission {
    pub protocol: String,
    pub from_port: Option<i64>,
    pub to_port: Option<i64>,
    pub ip_ranges: Vec<(String, String)>, // (cidr, description)
    pub ipv6_ranges: Vec<(String, String)>,
    pub prefix_list_ids: Vec<(String, String)>, // (pl-id, description)
    pub user_id_group_pairs: Vec<(String, String, String, String)>, // (group_id, group_name, owner_id, description)
}

impl IpPermission {
    /// Wire-name equality on the request-denominated fields (moto's checks).
    pub fn key_match(&self, other: &IpPermission) -> bool {
        self.protocol == other.protocol
            && self.from_port == other.from_port
            && self.to_port == other.to_port
            && self.ip_ranges == other.ip_ranges
            && self.ipv6_ranges == other.ipv6_ranges
            && self.prefix_list_ids == other.prefix_list_ids
            && self
                .user_id_group_pairs
                .iter()
                .map(|(g, n, o, _)| (g.clone(), n.clone(), o.clone()))
                .collect::<Vec<_>>()
                == other
                    .user_id_group_pairs
                    .iter()
                    .map(|(g, n, o, _)| (g.clone(), n.clone(), o.clone()))
                    .collect::<Vec<_>>()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SecurityGroup {
    pub id: String,
    pub name: String,
    pub description: String,
    pub vpc_id: String,
    pub owner_id: String,
    pub permissions: Vec<IpPermission>,
    pub egress: Vec<IpPermission>,
    pub tags: Tags,
    pub rule_ids: BTreeMap<String, String>, // "ingress:0" -> sg-rule id
}

#[derive(Debug, Clone, Default)]
pub struct Route {
    pub destination: String, // cidr, ipv6, pl-id...
    pub kind: String,        // "cidr" | "ipv6" | "pl"
    pub target: Option<String>,
    pub state: String,
    pub origin: String,
}

#[derive(Debug, Clone, Default)]
pub struct RouteTable {
    pub id: String,
    pub vpc_id: String,
    pub routes: Vec<Route>,
    pub associations: Vec<(String, Option<String>, bool)>, // (assoc_id, subnet_id, main)
    pub propagations: Vec<String>,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct InternetGateway {
    pub id: String,
    pub attachments: Vec<(String, String)>, // (vpc_id, state)
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct NatGateway {
    pub id: String,
    pub state: String,
    pub subnet_id: String,
    pub vpc_id: String,
    pub create_time: String,
    pub connectivity_type: String,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct ElasticIp {
    pub allocation_id: String,
    pub public_ip: String,
    pub domain: String,
    pub private_ip_address: String,
    pub association_id: Option<String>,
    pub vpc_id: Option<String>,
    pub instance_id: Option<String>,
    pub network_interface_id: Option<String>,
    pub network_interface_owner_id: Option<String>,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct Eni {
    pub id: String,
    pub subnet_id: String,
    pub vpc_id: String,
    pub description: String,
    pub owner_id: String,
    pub status: String,
    pub mac_address: String,
    pub private_ip: String,
    pub public_ip: Option<String>,
    pub association_id: Option<String>,
    pub secondary_private_ips: Vec<String>,
    pub ipv6_addresses: Vec<String>,
    pub groups: Vec<(String, String)>,
    pub interface_type: String,
    pub source_dest_check: bool,
    pub attached: Option<EniAttachment>,
    pub tags: Tags,
    pub requester_managed: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EniAttachment {
    pub attachment_id: String,
    pub instance_id: String,
    pub device_index: i32,
    pub status: String,
    pub delete_on_termination: bool,
    pub attach_time: String,
}

#[derive(Debug, Clone, Default)]
pub struct DhcpOptions {
    pub id: String,
    pub configurations: Vec<(String, Vec<String>)>,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct NetworkAcl {
    pub id: String,
    pub vpc_id: String,
    pub is_default: bool,
    pub entries: Vec<NaclEntry>,
    pub associations: Vec<(String, String)>, // (assoc_id, subnet_id)
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct NaclEntry {
    pub rule_number: i64,
    pub protocol: String,
    pub rule_action: String,
    pub egress: bool,
    pub cidr: String,
    pub port_range: Option<(i64, i64)>,
    pub icmp: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct VpcPeering {
    pub id: String,
    pub requester_vpc_id: String,
    pub accepter_vpc_id: String,
    pub state: String,
    pub state_code: i64,
    pub status_message: Option<String>,
    pub tags: Tags,
    pub expiration: String,
    pub requester_dns: bool,
    pub requester_classic: bool,
    pub accepter_dns: bool,
    pub accepter_classic: bool,
}

#[derive(Debug, Clone, Default)]
pub struct VpcEndpoint {
    pub id: String,
    pub vpc_id: String,
    pub service_name: String,
    pub endpoint_type: String,
    pub state: String,
    pub route_table_ids: Vec<String>,
    pub subnet_ids: Vec<String>,
    pub security_group_ids: Vec<String>,
    pub policy: String,
    pub tags: Tags,
    pub private_dns: bool,
    pub requester_managed: bool,
    pub dns_entries: Vec<(String, String)>, // (dns_name, dns_ip)
}

#[derive(Debug, Clone, Default)]
pub struct VpcEndpointService {
    pub id: String,
    pub service_name: String,
    pub acceptance_required: bool,
    pub nlb_arns: Vec<String>,
    pub tags: Tags,
    pub permissions: Vec<String>,
}

// ---------------------------------------------------------------------------
// Transit gateway family
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Tgw {
    pub id: String,
    pub description: String,
    pub amazon_side_asn: String,
    pub arn: String,
    pub state: String,
    pub owner_id: String,
    pub tags: Tags,
    pub default_route_tables: bool,
    pub auto_accept: bool,
    pub dns_support: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TgwAttachment {
    pub id: String,
    pub tgw_id: String,
    pub tgw_owner_id: String,
    pub resource_type: String,
    pub resource_id: String,
    pub resource_owner_id: String,
    pub state: String,
    pub state_message: Option<String>,
    pub tags: Tags,
    pub subnet_ids: Vec<String>,
    pub vpc_id: String,
    pub requester_arn: String,
    pub requester_owner_id: String,
    pub requester_region: String,
    pub accepter_arn: String,
    pub accepter_owner_id: String,
    pub accepter_region: String,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct TgwRouteTable {
    pub id: String,
    pub tgw_id: String,
    pub state: String,
    pub default_association: bool,
    pub default_propagation: bool,
    pub tags: Tags,
    pub routes: Vec<(String, String, String)>, // (cidr, attachment_id|blackhole, state)
    pub associations: Vec<(String, String, String)>, // (resource_id, resource_type, state)
    pub propagations: Vec<(String, String, String)>,
    pub prefix_list_refs: Vec<(String, String, String)>, // (pl_id, ref state, blackhole?)
    pub is_default: bool,
}

// ---------------------------------------------------------------------------
// Compute
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Instance {
    pub id: String,
    pub image_id: String,
    pub instance_type: String,
    pub state: String,
    pub state_code: i64,
    pub subnet_id: String,
    pub vpc_id: String,
    pub az: String,
    pub placement_group: Option<String>,
    pub tenancy: String,
    pub key_name: Option<String>,
    pub security_group_ids: Vec<String>,
    pub source_dest_check: bool,
    pub ebs_optimized: bool,
    pub sriov_net_support: Option<String>,
    pub user_data: Option<String>,
    pub disable_api_termination: bool,
    pub disable_api_stop: bool,
    pub monitoring: bool,
    pub private_ip: String,
    pub ip6: Option<String>,
    pub kernel_id: Option<String>,
    pub ramdisk_id: Option<String>,
    pub client_token: String,
    pub ami_launch_index: i64,
    pub root_device_name: String,
    pub root_device_type: String,
    pub ebs_root_volume_ids: Vec<String>,
    pub tags: Tags,
    pub created_at: String,
    pub launch_time: String,
    pub lifecycle: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Image {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub state: String,
    pub owner_id: String,
    pub owner_alias: Option<String>,
    pub is_public: bool,
    pub root_device_type: String,
    pub root_device_name: String,
    pub architecture: String,
    pub virtualization_type: String,
    pub hypervisor: String,
    pub image_type: String,
    pub platform: String,
    pub sriov: String,
    pub ena_support: bool,
    pub kernel_id: Option<String>,
    pub ramdisk_id: Option<String>,
    pub image_location: String,
    pub usage_operation: String,
    pub boot_mode: Option<String>,
    pub source_image_id: Option<String>,
    pub block_device_mappings: Vec<Bdm>,
    pub launch_permissions: Vec<(String, String)>,
    pub tags: Tags,
    pub created_at: String,
    pub snapshot_description: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Bdm {
    pub device_name: String,
    pub ebs_snapshot_id: Option<String>,
    pub ebs_volume_size: Option<i64>,
    pub ebs_volume_type: String,
    pub ebs_delete_on_term: bool,
    pub ebs_encrypted: bool,
    pub ebs_iops: Option<i64>,
    pub ebs_throughput: Option<i64>,
    pub no_device: bool,
    pub virtual_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Volume {
    pub id: String,
    pub size: i64,
    pub vtype: String,
    pub state: String,
    pub snapshot_id: String,
    pub encrypted: bool,
    pub iops: Option<i64>,
    pub throughput: Option<i64>,
    pub az: String,
    pub create_time: String,
    pub tags: Tags,
    pub attachment: Option<VolumeAttachment>,
    pub multi_attach: bool,
    pub kms_key_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct VolumeAttachment {
    pub instance_id: String,
    pub device: String,
    pub status: String,
    pub attach_time: String,
    pub delete_on_termination: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub id: String,
    pub volume_id: String,
    pub state: String,
    pub progress: String,
    pub start_time: String,
    pub description: String,
    pub owner_id: String,
    pub owner_alias: Option<String>,
    pub tags: Tags,
    pub encrypted: bool,
    pub volume_size: i64,
    pub kms_key_id: Option<String>,
    pub storage_tier: String,
    pub restore_expiry: Option<String>,
    pub create_volume_permissions: Vec<(String, String)>,
    pub source_snapshot: Option<String>,
    pub source_region: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Keypair {
    pub name: String,
    pub fingerprint: String,
    pub material: String,
    pub key_pair_id: String,
    pub tags: Tags,
    pub key_type: String,
}

#[derive(Debug, Clone, Default)]
pub struct PlacementGroup {
    pub name: String,
    pub strategy: String,
    pub state: String,
    pub group_id: String,
    pub partition_count: Option<i64>,
    pub spread_level: Option<i64>,
    pub tags: Tags,
}

#[derive(Debug, Clone, Default)]
pub struct LaunchTemplateVersion {
    pub number: i64,
    pub data: String,
    pub description: Option<String>,
    pub created: String,
    pub default: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LaunchTemplate {
    pub id: String,
    pub name: String,
    pub description: String,
    pub created: String,
    pub default_version: i64,
    pub latest_version: i64,
    pub versions: Vec<LaunchTemplateVersion>,
    pub tags: Tags,
}

// ---------------------------------------------------------------------------
// Generic minor families
// ---------------------------------------------------------------------------

/// Resource for small create/describe/delete families: stored with a stable
/// id, generic fields (wire camelCase keys) and tags.
#[derive(Debug, Clone, Default)]
pub struct Simple {
    pub id: String,
    pub kind: String,
    pub fields: BTreeMap<String, String>,
    pub tags: Tags,
    /// Rendered wire item (with ids substituted at creation time).
    pub item_xml: String,
}

impl Simple {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(|s| s.as_str())
    }
    pub fn set(&mut self, key: &str, value: impl Into<String>) {
        self.fields.insert(key.to_string(), value.into());
    }
}

/// Per-scope EC2 state.
#[derive(Debug, Clone, Default)]
pub struct Ec2State {
    pub ids: Ids,

    // Typed families
    pub vpcs: Vec<Vpc>,
    pub subnets: Vec<Subnet>,
    pub security_groups: Vec<SecurityGroup>,
    pub route_tables: Vec<RouteTable>,
    pub internet_gateways: Vec<InternetGateway>,
    pub nat_gateways: Vec<NatGateway>,
    pub addresses: Vec<ElasticIp>,
    pub enis: Vec<Eni>,
    pub dhcp_options: Vec<DhcpOptions>,
    pub network_acls: Vec<NetworkAcl>,
    pub vpc_peerings: Vec<VpcPeering>,
    pub vpc_endpoints: Vec<VpcEndpoint>,
    pub vpc_endpoint_services: Vec<VpcEndpointService>,
    pub instances: Vec<Instance>,
    pub images: Vec<Image>,
    pub volumes: Vec<Volume>,
    pub snapshots: Vec<Snapshot>,
    pub key_pairs: Vec<Keypair>,
    pub placement_groups: Vec<PlacementGroup>,
    pub launch_templates: Vec<LaunchTemplate>,
    pub transit_gateways: Vec<Tgw>,
    pub tgw_attachments: Vec<TgwAttachment>,
    pub tgw_route_tables: Vec<TgwRouteTable>,

    // Generic minor families, keyed by family name.
    pub simple: BTreeMap<String, Vec<Simple>>,

    // Everything else, keyed per scope.
    pub deleted_amis: HashSet<String>,
    pub default_vpc_regions: HashSet<String>,
    pub serial_console_access: bool,
    pub snapshot_attributes: BTreeMap<String, BTreeMap<String, String>>, // id -> "create-volume-permissions" -> rendered list
    pub run_ami_ok: HashSet<String>, // image ids we can run with (created/amazon)
    pub spot_prices: Vec<(String, String, String)>, // (instance_type, price, az)
}

impl Ec2State {
    /// True when the (account, region) has its default VPC set up.
    pub fn ensure_default_vpc(&mut self, region: &str, account: u64) {
        if self.default_vpc_regions.contains(region) {
            return;
        }
        self.default_vpc_regions.insert(region.to_string());
        // moto creates the default VPC with 172.31.0.0/16 and 4 subnets.
        let cidr_blocks = ["172.31.16.0/20", "172.31.32.0/20"];
        let _ = cidr_blocks;
        let vpc_id = format!("vpc-{}", idhex::hex17());
        let _dopt_id = "default".to_string();
        let assoc_id = format!("vpc-cidr-assoc-{}", idhex::hex17());
        self.vpcs.push(Vpc {
            id: vpc_id.clone(),
            cidr: "172.31.0.0/16".into(),
            tenancy: "default".into(),
            state: "available".into(),
            is_default: true,
            dhcp_options_id: "default".into(),
            cidr_assoc_id: assoc_id,
            enable_dns_support: true,
            enable_dns_hostnames: true,
            ..Default::default()
        });
        // Default route table
        let rt_id = format!("rtb-{}", idhex::hex17());
        self.route_tables.push(RouteTable {
            id: rt_id,
            vpc_id: vpc_id.clone(),
            routes: vec![Route {
                destination: "172.31.0.0/16".into(),
                kind: "cidr".into(),
                target: Some("local".into()),
                state: "active".into(),
                origin: "CreateRouteTable".into(),
            }],
            ..Default::default()
        });
        // Default security group
        let sg_id = format!("sg-{}", idhex::hex17());
        let mut sg = SecurityGroup {
            id: sg_id.clone(),
            name: "default".into(),
            description: "default VPC security group".into(),
            vpc_id: vpc_id.clone(),
            owner_id: account.to_string(),
            ..Default::default()
        };
        sg.egress = vec![IpPermission {
            protocol: "-1".into(),
            ..Default::default()
        }];
        self.security_groups.push(sg);
        // 2 default subnets with the moto scheme
        for (i, cidr) in ["172.31.0.0/20", "172.31.16.0/20"].iter().enumerate() {
            let subnet_id = format!("subnet-{}", idhex::hex17());
            let az = format!("{}a", region);
            self.subnets.push(Subnet {
                id: subnet_id.clone(),
                vpc_id: vpc_id.clone(),
                cidr: cidr.to_string(),
                az: az.clone(),
                state: "available".into(),
                map_public_ip: false,
                availability_zone_id: "use1-az6".into(),
                is_default: true,
                ..Default::default()
            });
            let _ = i;
        }
        // moto's default ACL — created by default vpc
        let acl_id = format!("acl-{}", idhex::hex17());
        self.network_acls.push(NetworkAcl {
            id: acl_id,
            vpc_id: vpc_id.clone(),
            is_default: true,
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
                    rule_number: 32767,
                    protocol: "-1".into(),
                    rule_action: "deny".into(),
                    egress: false,
                    cidr: "0.0.0.0/0".into(),
                    port_range: None,
                    icmp: None,
                },
            ],
            ..Default::default()
        });
    }

    /// Assign a fresh id in moto's format.
    pub fn next_id(&mut self, prefix: &str) -> String {
        self.ids.seq += 1;
        format!("{}-{}", prefix, idhex::hex17())
    }

    pub fn next_id16(&mut self, prefix: &str) -> String {
        self.ids.seq += 1;
        format!("{}-{}", prefix, idhex::hex16())
    }

    pub fn next_id8(&mut self, prefix: &str) -> String {
        self.ids.seq += 1;
        format!("{}-{}", prefix, idhex::hex8())
    }

    /// Tag lookup helper.
    pub fn tags_of(&self, family: &str, id: &str) -> Option<&Tags> {
        self.simple
            .get(family)?
            .iter()
            .find(|s| s.id == id)
            .map(|s| &s.tags)
    }

    /// Resource id / type / tag rows across every family (DescribeTags).
    pub fn all_tag_rows(&self) -> Vec<(String, String, Tags)> {
        let mut rows: Vec<(String, String, Tags)> = Vec::new();
        macro_rules! push {
            ($item:expr, $kind:expr) => {
                if !$item.tags.is_empty() {
                    rows.push(($item.id.clone(), $kind.to_string(), $item.tags.clone()));
                }
            };
        }
        for v in &self.vpcs {
            push!(v, "vpc");
        }
        for v in &self.subnets {
            push!(v, "subnet");
        }
        for v in &self.security_groups {
            push!(v, "security-group");
        }
        for v in &self.route_tables {
            push!(v, "route-table");
        }
        for v in &self.internet_gateways {
            push!(v, "internet-gateway");
        }
        for v in &self.nat_gateways {
            push!(v, "natgateway");
        }
        for v in &self.enis {
            push!(v, "network-interface");
        }
        for v in &self.dhcp_options {
            push!(v, "dhcp-options");
        }
        for v in &self.network_acls {
            push!(v, "network-acl");
        }
        for v in &self.vpc_peerings {
            push!(v, "vpc-peering-connection");
        }
        for v in &self.instances {
            push!(v, "instance");
        }
        for v in &self.images {
            push!(v, "image");
        }
        for v in &self.volumes {
            push!(v, "volume");
        }
        for v in &self.snapshots {
            push!(v, "snapshot");
        }
        for v in &self.key_pairs {
            if !v.tags.is_empty() {
                rows.push((v.name.clone(), "key-pair".to_string(), v.tags.clone()));
            }
        }
        for v in &self.placement_groups {
            if !v.tags.is_empty() {
                rows.push((
                    v.name.clone(),
                    "placement-group".to_string(),
                    v.tags.clone(),
                ));
            }
        }
        for v in &self.launch_templates {
            push!(v, "launch-template");
        }
        for v in &self.transit_gateways {
            push!(v, "transit-gateway");
        }
        for v in &self.tgw_attachments {
            push!(v, "transit-gateway-attachment");
        }
        for v in &self.tgw_route_tables {
            push!(v, "transit-gateway-route-table");
        }
        for v in &self.addresses {
            if !v.tags.is_empty() {
                rows.push((
                    v.allocation_id.clone(),
                    "elastic-ip".to_string(),
                    v.tags.clone(),
                ));
            }
        }
        for v in &self.vpc_endpoints {
            push!(v, "vpc-endpoint");
        }
        for v in &self.vpc_endpoint_services {
            push!(v, "vpc-endpoint-service");
        }
        for fam in self.simple.values() {
            for v in fam {
                push!(v, v.kind.clone().as_str());
            }
        }
        rows
    }
}

/// Get (creating on first use) the process-global EC2 state slot.
pub fn shared() -> crate::store::Slot<Ec2State> {
    crate::store::slot::<Ec2State>("ec2")
}
