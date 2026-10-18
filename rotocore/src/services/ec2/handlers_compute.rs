//! EC2 handler group 4: compute — instances, images, volumes, snapshots,
//! key pairs, launch templates, placement groups.
#![allow(
    clippy::format_in_format_args,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::single_match,
    clippy::map_identity
)]
#![allow(unused_variables)]

use super::handlers::{filter_passes, ok_xml, tag_candidates, wire_tags};
use super::state::{idhex, Bdm, EniAttachment, Instance, Volume, VolumeAttachment};
use super::{ec2_error, ec2_xml, now_iso, tags_xml, with_state, Params};
use crate::proto::xml_escape;
use crate::{Request, Response};
use std::fmt::Write as _;

type P = Params;

pub fn route(req: &Request, params: &P) -> Response {
    match req.operation.as_str() {
        "RunInstances" => run_instances(req, params),
        "DescribeInstances" => describe_instances(req, params),
        "TerminateInstances" => terminate_instances(req, params),
        "StopInstances" => stop_instances(req, params),
        "StartInstances" => start_instances(req, params),
        "RebootInstances" => ok_xml("RebootInstancesResponse"),
        "DescribeInstanceStatus" => describe_instance_status(req, params),
        "DescribeInstanceAttribute" => describe_instance_attribute(req, params),
        "ModifyInstanceAttribute" => modify_instance_attribute(req, params),
        "MonitorInstances" => set_monitoring(req, params, true),
        "UnmonitorInstances" => set_monitoring(req, params, false),
        "GetConsoleOutput" => get_console_output(req, params),
        "GetPasswordData" => get_password_data(req, params),
        "GetLaunchTemplateData" => get_launch_template_data(req, params),
        "GetInstanceUefiData" => get_instance_uefi_data(req, params),
        "ModifyInstanceMetadataOptions" => modify_instance_metadata_options(req, params),
        "DescribeInstanceMetadataOptions" => describe_instance_metadata_options(req, params),
        "GetSerialConsoleAccessStatus" => get_serial_console(req, params),
        "EnableSerialConsoleAccess" => set_serial_console(req, params, true),
        "DisableSerialConsoleAccess" => set_serial_console(req, params, false),
        "GetInstanceMetadataDefaults" => get_instance_metadata_defaults(req, params),
        "ModifyInstanceMetadataDefaults" => modify_instance_metadata_defaults(req, params),

        // Images
        "CreateImage" => create_image_from_instance(req, params),
        "DescribeImages" => describe_images(req, params),
        "RegisterImage" => register_image(req, params),
        "DeregisterImage" => deregister_image(req, params),
        "CopyImage" => copy_image(req, params),
        "DescribeImageAttribute" => describe_image_attribute(req, params),
        "ModifyImageAttribute" => modify_image_attribute(req, params),

        // Volumes
        "CreateVolume" => create_volume(req, params),
        "DescribeVolumes" => describe_volumes(req, params),
        "DeleteVolume" => delete_volume(req, params),
        "AttachVolume" => attach_volume(req, params),
        "DetachVolume" => detach_volume(req, params),
        "ModifyVolume" => modify_volume(req, params),
        "DescribeVolumeAttribute" => describe_volume_attribute(req, params),
        "ModifyVolumeAttribute" => ok_xml("ModifyVolumeAttributeResponse"),
        "DescribeVolumeStatus" => describe_volume_status(req, params),
        "DescribeVolumesModifications" => describe_volume_status(req, params),

        // Snapshots
        "CreateSnapshot" => create_snapshot(req, params),
        "DescribeSnapshots" => describe_snapshots(req, params),
        "DeleteSnapshot" => delete_snapshot(req, params),
        "CopySnapshot" => copy_snapshot(req, params),
        "DescribeSnapshotAttribute" => describe_snapshot_attribute(req, params),
        "ModifySnapshotAttribute" => modify_snapshot_attribute(req, params),
        "CreateSnapshots" => create_snapshots(req, params),

        // Key pairs
        "CreateKeyPair" => create_key_pair(req, params),
        "DescribeKeyPairs" => describe_key_pairs(req, params),
        "DeleteKeyPair" => delete_key_pair(req, params),
        "ImportKeyPair" => import_key_pair(req, params),

        // Launch templates
        "CreateLaunchTemplate" => create_launch_template(req, params),
        "CreateLaunchTemplateVersion" => create_launch_template_version(req, params),
        "DescribeLaunchTemplates" => describe_launch_templates(req, params),
        "DescribeLaunchTemplateVersions" => describe_launch_template_versions(req, params),
        "DeleteLaunchTemplate" => delete_launch_template(req, params),
        "DeleteLaunchTemplateVersions" => ok_xml("DeleteLaunchTemplateVersionsResponse"),
        "ModifyLaunchTemplate" => modify_launch_template(req, params),
        "GetLaunchTemplateVersions" => describe_launch_template_versions(req, params),

        // Instance type catalog + price history
        "DescribeInstanceTypes" => describe_instance_types(req, params),
        "DescribeInstanceTypeOfferings" => describe_instance_type_offerings(req, params),
        "DescribeSpotPriceHistory" => describe_spot_price_history(req, params),
        "DescribeReservedInstancesOfferings" => describe_reserved_instances_offerings(req, params),

        // Placement groups
        "CreatePlacementGroup" => create_placement_group(req, params),
        "DescribePlacementGroups" => describe_placement_groups(req, params),
        "DeletePlacementGroup" => delete_placement_group(req, params),

        _ => super::handlers_misc::route(req, params),
    }
}

// ---------------------------------------------------------------------------
// Instances
// ---------------------------------------------------------------------------

fn require_instance(state: &mut super::state::Ec2State, id: &str) -> Result<usize, Response> {
    match state.instances.iter().position(|i| i.id == id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidInstanceID.NotFound",
            &format!("The instance ID '{}' does not exist", xml_escape(id)),
        )),
    }
}

pub fn instance_item(
    region: &str,
    account: u64,
    i: &Instance,
    sg_names: &[(String, String)],
) -> String {
    let mut sg_items = String::new();
    for (gid, gname) in sg_names {
        let _ = write!(
            sg_items,
            "<item><groupId>{}</groupId><groupName>{}</groupName></item>",
            xml_escape(gid),
            xml_escape(gname)
        );
    }
    let state_code = i.state_code;
    let state_name = i.state.clone();
    let muted_monitoring = if i.monitoring { "enabled" } else { "disabled" };
    format!(
        "<item><architecture>x86_64</architecture>{}<clientToken></clientToken><ebsOptimized>{}</ebsOptimized><hypervisor>xen</hypervisor><keyName>{}</keyName><imageId>{}</imageId><instanceId>{}</instanceId><instanceType>{}</instanceType><instanceState><code>{}</code><name>{}</name></instanceState><kernelId>{}</kernelId><rootDeviceName>{}</rootDeviceName><rootDeviceType>{}</rootDeviceType><amiLaunchIndex>{}</amiLaunchIndex><launchTime>{}</launchTime><placement>{}<availabilityZone>{}</availabilityZone><tenancy>{}</tenancy><groupName/></placement><monitoring><state>{}</state></monitoring><privateDnsName>ip-{}.ec2.internal</privateDnsName><privateIpAddress>{}</privateIpAddress><productCodesSet/>{}{}<subnetId>{}</subnetId><vpcId>{}</vpcId>{}<virtualizationType>hvm</virtualizationType><rootDeviceMapping><item><deviceName>{}</deviceName><ebs><attachTime>{}</attachTime><deleteOnTermination>true</deleteOnTermination><status>attached</status><volumeId>{}</volumeId></ebs></item></rootDeviceMapping><deviceMapping></deviceMapping>{}{}</item>",
        "", // block devices kept in rootDeviceMapping for compat parity
        i.ebs_optimized,
        i.key_name.clone().unwrap_or_default(),
        xml_escape(&i.image_id),
        xml_escape(&i.id),
        xml_escape(&i.instance_type),
        state_code,
        xml_escape(&state_name),
        i.kernel_id.clone().unwrap_or_default(),
        xml_escape(&i.root_device_name),
        xml_escape(&i.root_device_type),
        i.ami_launch_index,
        xml_escape(&i.launch_time),
        if state_code == 16 { "<stateTransitionReason/>" } else { "" },
        xml_escape(&i.az),
        xml_escape(&i.tenancy),
        muted_monitoring,
        "0-0-0-0",
        xml_escape(&i.private_ip),
        if !sg_items.is_empty() {
            format!("<groupSet>{}</groupSet>", sg_items)
        } else {
            String::from("<groupSet/>")
        },
        String::new(), // platform reserved
        xml_escape(&i.subnet_id),
        xml_escape(&i.vpc_id),
        tags_xml(&i.tags),
        xml_escape(&i.root_device_name),
        xml_escape(&i.launch_time),
        i.ebs_root_volume_ids.first().cloned().unwrap_or_default(),
        if state_code == 16 { "<stateReason><code>pending</code><message>pending</message></stateReason>".replace("pending", if state_code == 16 {"running"} else {"stopped"}) } else { String::new() },
        String::new()
    )
}

fn run_instances(req: &Request, params: &P) -> Response {
    let image_id = params.str("ImageId");
    let min_count = params.i64("MinCount").unwrap_or(1).max(1);
    let max_count = params.i64("MaxCount").unwrap_or(min_count).max(1);
    let count = min_count.min(max_count);
    let instance_type = params
        .get("InstanceType")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "m1.small".to_string());
    let subnet_id = params.str("SubnetId");
    let key_name = params.get("KeyName").map(|s| s.to_string());
    let az_req = params.str("Placement.AvailabilityZone");
    let user_data = params.get("UserData").map(|s| s.to_string());
    with_state(req, move |state| {
        // moto lets any ami id run; our image fleet covers the roster anyway.
        let sg_ids: Vec<String> = params
            .list("SecurityGroupId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut sg_items: Vec<(String, String)> = Vec::new();
        let mut my_subnet = subnet_id.clone();
        if my_subnet.is_empty() {
            my_subnet = state
                .subnets
                .iter()
                .find(|s| s.is_default)
                .map(|s| s.id.clone())
                .unwrap_or_default();
        }
        let (vpc_target, az_used) = match state.subnets.iter().find(|s| s.id == my_subnet) {
            Some(s) => (
                s.vpc_id.clone(),
                if az_req.is_empty() {
                    s.az.clone()
                } else {
                    az_req.clone()
                },
            ),
            None => (String::new(), az_req.to_string()),
        };
        for gid in &sg_ids {
            if let Some(sg) = state.security_groups.iter().find(|s| s.id == *gid) {
                sg_items.push((sg.id.clone(), sg.name.clone()));
            }
        }
        if sg_items.is_empty() {
            // default security group of the subnet's VPC
            let default_sg = state
                .security_groups
                .iter()
                .find(|s| s.vpc_id == vpc_target && s.name == "default")
                .map(|s| (s.id.clone(), s.name.clone()));
            if let Some(x) = default_sg {
                sg_items.push(x);
            }
        }
        let reservation_id = state.next_id("r");
        let mut items = String::new();
        for index in 0..count {
            let instance_id = state.next_id("i");
            let n = uuid::Uuid::new_v4().as_u128();
            let private_ip = if my_subnet.is_empty() {
                format!("10.0.{}.{}", (n >> 16) % 250, (n >> 8) % 250)
            } else {
                let cidr = state
                    .subnets
                    .iter()
                    .find(|s| s.id == my_subnet)
                    .map(|s| s.cidr.clone())
                    .unwrap_or_else(|| "172.31.0.0".into());
                let base = cidr.split('.').collect::<Vec<_>>();
                let a = base
                    .first()
                    .copied()
                    .unwrap_or("172")
                    .parse()
                    .unwrap_or(172);
                let b = base.get(1).copied().unwrap_or("31").parse().unwrap_or(31);
                let c = base.get(2).copied().unwrap_or("0").parse().unwrap_or(0);
                format!("{}.{}.{}.{}", a, b, c, (n >> 16) % 254 + 2)
            };
            // primary eni
            let eni_id = state.next_id("eni");
            let eip = String::new();
            let n = uuid::Uuid::new_v4().as_u128();
            let mac = format!(
                "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                (n >> 40) & 0xff,
                (n >> 32) & 0xff,
                (n >> 24) & 0xff,
                (n >> 16) & 0xff,
                (n >> 8) & 0xff
            );
            let root_vol = state.next_id("vol");
            state.instances.push(Instance {
                id: instance_id.clone(),
                image_id: image_id.clone(),
                instance_type: instance_type.clone(),
                state: "running".into(),
                state_code: 16,
                subnet_id: my_subnet.clone(),
                vpc_id: vpc_target.clone(),
                az: az_used.clone(),
                tenancy: "default".into(),
                key_name: key_name.clone(),
                security_group_ids: sg_ids.clone(),
                source_dest_check: true,
                ami_launch_index: index,
                root_device_name: "/dev/sda1".into(),
                root_device_type: "ebs".into(),
                ebs_root_volume_ids: vec![root_vol.clone()],
                private_ip: private_ip.clone(),
                user_data: user_data.clone(),
                created_at: now_iso(),
                launch_time: now_iso(),
                ..Default::default()
            });
            let eni_attach_id = state.next_id("eni-attach");
            state.enis.push(super::state::Eni {
                id: eni_id.clone(),
                subnet_id: my_subnet.clone(),
                vpc_id: vpc_target.clone(),
                owner_id: req.account.to_string(),
                status: "in-use".into(),
                mac_address: mac,
                private_ip: private_ip.clone(),
                groups: sg_items.clone(),
                interface_type: "interface".into(),
                source_dest_check: true,
                attached: Some(EniAttachment {
                    attachment_id: eni_attach_id,
                    instance_id: instance_id.clone(),
                    device_index: 0,
                    status: "attached".into(),
                    delete_on_termination: true,
                    attach_time: now_iso(),
                }),
                ..Default::default()
            });
            // TagSpecifications: instance + volume
            for i2 in params.container_indices("TagSpecification") {
                let rt = params
                    .sub("TagSpecification", i2, "ResourceType")
                    .unwrap_or("instance")
                    .to_string();
                let mut tags: Vec<(String, String)> = Vec::new();
                for t in params.container_indices(&format!("TagSpecification.{}.Tag", i2 + 1)) {
                    let base = format!("TagSpecification.{}.Tag", i2 + 1);
                    if let (Some(k), v) = (
                        params.sub(&base, t, "Key"),
                        params.sub(&base, t, "Value").unwrap_or_default(),
                    ) {
                        tags.push((k.to_string(), v.to_string()));
                    }
                }
                let owner = match rt.as_str() {
                    "instance" => instance_id.clone(),
                    "volume" => root_vol.clone(),
                    _ => instance_id.clone(),
                };
                super::handlers::replace_tags(state, &[owner], &tags);
            }
            let inst = state
                .instances
                .iter()
                .find(|i| i.id == instance_id)
                .cloned()
                .unwrap_or_default();
            items.push_str(&instance_item(&req.region, req.account, &inst, &sg_items));
        }
        ec2_xml(
            "RunInstancesResponse",
            &format!(
                "<reservationId>{}</reservationId><ownerId>{}</ownerId>{}<instancesSet>{}</instancesSet>",
                xml_escape(&reservation_id),
                req.account,
                if sg_items.is_empty() {
                    String::from("<groupSet/>")
                } else {
                    let mut sgs = String::new();
                    for (gid, gname) in &sg_items {
                        let _ = write!(
                            sgs,
                            "<item><groupId>{}</groupId><groupName>{}</groupName></item>",
                            xml_escape(gid),
                            xml_escape(gname)
                        );
                    }
                    format!("<groupSet>{}</groupSet>", sgs)
                },
                items
            ),
        )
    })
}

fn stable_hash(data: &[u8]) -> u32 {
    let mut h: u32 = 2166136261;
    for b in data {
        h ^= u32::from(*b);
        h = h.wrapping_mul(16777619);
    }
    h
}

fn describe_instances(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    let max_results = params.i64("MaxResults");
    let skip = params
        .get("NextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    with_state(req, move |state| {
        let region = req.region.clone();
        let account = req.account;
        let all: Vec<Instance> = state.instances.clone();
        let sgs: Vec<super::state::SecurityGroup> = state.security_groups.clone();
        let mut matches: Vec<&Instance> = Vec::new();
        for i in &all {
            if !ids.is_empty() && !ids.contains(&i.id) {
                continue;
            }
            let i_shadow = i.clone();
            if !filter_passes(&fs, move |f| match f {
                "instance-id" => vec![i_shadow.id.clone()],
                "instance-state-name" => vec![i_shadow.state.clone()],
                "image-id" => vec![i_shadow.image_id.clone()],
                "instance-type" | "type" => vec![i_shadow.instance_type.clone()],
                "subnet-id" => vec![i_shadow.subnet_id.clone()],
                "vpc-id" => vec![i_shadow.vpc_id.clone()],
                "availability-zone" => vec![i_shadow.az.clone()],
                "private-ip-address" => vec![i_shadow.private_ip.clone()],
                other => tag_candidates(&i_shadow.tags, other),
            }) {
                continue;
            }
            matches.push(i);
        }
        let total = matches.len();
        let page: Vec<&Instance> = match max_results {
            Some(mr) if mr > 0 => {
                let start = skip.min(total);
                matches.into_iter().skip(start).take(mr as usize).collect()
            }
            _ => matches,
        };
        let mut body = String::new();
        for i in page {
            let sg_items: Vec<(String, String)> = sgs
                .iter()
                .filter(|sg| i.security_group_ids.contains(&sg.id))
                .map(|sg| (sg.id.clone(), sg.name.clone()))
                .collect();
            body.push_str(&format!(
                "<item><reservationId>r-{:08x}</reservationId><ownerId>{}</ownerId><groupSet/><instancesSet>{}</instancesSet></item>",
                stable_hash(i.id.as_bytes()),
                account,
                instance_item(&region, account, i, &sg_items)
            ));
        }
        let mut body_out = if body.is_empty() {
            String::from("<reservationSet/>")
        } else {
            format!("<reservationSet>{}</reservationSet>", body)
        };
        if let Some(mr) = max_results {
            let consumed = skip + (mr as usize);
            if consumed < total {
                body_out = format!("<nextToken>{}</nextToken>{}", consumed, body_out);
            }
        }
        ec2_xml("DescribeInstancesResponse", &body_out)
    })
}

fn instance_state_change(id: &str, code: i64, previous: &str, current: &str) -> String {
    format!(
        "<item><instanceId>{}</instanceId><currentState><code>{}</code><name>{}</name></currentState><previousState><code>{}</code><name>{}</name></previousState></item>",
        xml_escape(id),
        code,
        current,
        match previous { "running" => 16, "stopped" => 80, "terminated" => 48, _ => 0 },
        previous
    )
}

fn terminate_instances(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    if ids.is_empty() {
        return ec2_error(
            400,
            "InvalidParameterCombination",
            "Either DryRun parameter or InstanceId parameter is required",
        );
    }
    with_state(req, move |state| {
        let mut changes = String::new();
        for id in &ids {
            let pos = match state.instances.iter().position(|i| i.id == *id) {
                Some(i) => i,
                None => return not_found_instance(id),
            };
            let was = state.instances[pos].state.clone();
            state.instances[pos].state = "terminated".into();
            state.instances[pos].state_code = 48;
            let _ = &state.instances[pos];
            changes.push_str(&instance_state_change(id, 48, &was, "terminated"));
        }
        ec2_xml(
            "TerminateInstancesResponse",
            &format!("<instancesSet>{}</instancesSet>", changes),
        )
    })
}

fn stop_instances(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let mut changes = String::new();
        for id in &ids {
            let pos = match state.instances.iter().position(|i| i.id == *id) {
                Some(i) => i,
                None => return not_found_instance(id),
            };
            let was = state.instances[pos].state.clone();
            state.instances[pos].state = "stopped".into();
            state.instances[pos].state_code = 80;
            changes.push_str(&instance_state_change(id, 80, &was, "stopped"));
        }
        ec2_xml(
            "StopInstancesResponse",
            &format!("<instancesSet>{}</instancesSet>", changes),
        )
    })
}

fn start_instances(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let mut changes = String::new();
        for id in &ids {
            let pos = match state.instances.iter().position(|i| i.id == *id) {
                Some(i) => i,
                None => return not_found_instance(id),
            };
            let was = state.instances[pos].state.clone();
            state.instances[pos].state = "running".into();
            state.instances[pos].state_code = 16;
            changes.push_str(&instance_state_change(id, 16, &was, "running"));
        }
        ec2_xml(
            "StartInstancesResponse",
            &format!("<instancesSet>{}</instancesSet>", changes),
        )
    })
}

fn describe_instance_status(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let ids = ids.clone();
        let mut items = String::new();
        for i in &state.instances {
            if !ids.is_empty() && !ids.contains(&i.id) {
                continue;
            }
            if i.state == "terminated" {
                continue;
            }
            let _ = write!(
                items,
                "<item><instanceId>{}</instanceId><zoneName>{}</zoneName><instanceState><code>{}</code><name>{}</name></instanceState>{}<eventsSet/></item>",
                xml_escape(&i.id),
                xml_escape(&i.az),
                i.state_code,
                xml_escape(&i.state),
                if i.monitoring { "<systemStatus><details><item><name>reachability</name><status>passed</status></item></details><status>ok</status></systemStatus><instanceStatus><details><item><name>reachability</name><status>passed</status></item></details><status>ok</status></instanceStatus>" } else { "<systemStatus><status>ok</status><details><item><name>reachability</name><status>passed</status></item></details></systemStatus><instanceStatus><status>ok</status><details><item><name>reachability</name><status>passed</status></item></details></instanceStatus>" }
            );
        }
        ec2_xml(
            "DescribeInstanceStatusResponse",
            &format!("<instanceStatusSet>{}</instanceStatusSet>", items),
        )
    })
}

fn describe_instance_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    let attr = params.str("Attribute");
    with_state(req, move |state| {
        let pos = match require_instance(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let i = state.instances[pos].clone();
        let body = match attr.as_str() {
            "instanceType" | "instance-type" => format!(
                "<instanceType><value>{}</value></instanceType><instanceId>{}</instanceId>",
                xml_escape(&i.instance_type),
                xml_escape(&id)
            ),
            "disableApiTermination" | "disable-api-termination" => format!(
                "<disableApiTermination><value>{}</value></disableApiTermination><instanceId>{}</instanceId>",
                i.disable_api_termination,
                xml_escape(&id)
            ),
            "disableApiStop" | "disable-api-stop" => format!(
                "<disableApiStop><value>{}</value></disableApiStop><instanceId>{}</instanceId>",
                i.disable_api_stop,
                xml_escape(&id)
            ),
            "ebsOptimized" | "ebs-optimized" => format!(
                "<ebsOptimized><value>{}</value></ebsOptimized><instanceId>{}</instanceId>",
                i.ebs_optimized,
                xml_escape(&id)
            ),
            "sriovNetSupport" | "sriov-net-support" => format!(
                "<sriovNetSupport><value>simple</value></sriovNetSupport><instanceId>{}</instanceId>",
                xml_escape(&id)
            ),
            "userData" | "user-data" => format!(
                "<userData>{}</userData><instanceId>{}</instanceId>",
                i.user_data
                    .clone()
                    .map(|u| format!("<value>{}</value>", xml_escape(u.as_str())))
                    .unwrap_or_default(),
                xml_escape(&id)
            ),
            "sourceDestCheck" | "source-dest-check" => format!(
                "<sourceDestCheck><value>{}</value></sourceDestCheck><instanceId>{}</instanceId>",
                i.source_dest_check,
                xml_escape(&id)
            ),
            "rootDeviceName" | "root-device-name" => format!(
                "<rootDeviceName><value>{}</value></rootDeviceName><instanceId>{}</instanceId>",
                xml_escape(&i.root_device_name),
                xml_escape(&id)
            ),
            "blockDeviceMapping" => {
                let mut bdm = String::new();
                for vol in i.ebs_root_volume_ids.iter() {
                    bdm.push_str(&format!(
                        "<item><deviceName>{}</deviceName><volumeId>{}</volumeId><deleteOnTermination>true</deleteOnTermination></item>",
                        xml_escape(&i.root_device_name),
                        xml_escape(vol)
                    ));
                }
                format!(
                    "<blockDeviceMapping>{}</blockDeviceMapping><instanceId>{}</instanceId>",
                    bdm,
                    xml_escape(&id)
                )
            }
            "groupSet" | "groups" => {
                let mut groups = String::new();
                for gid in &i.security_group_ids {
                    let name = state
                        .security_groups
                        .iter()
                        .find(|sg| sg.id.as_str() == gid)
                        .map(|sg| sg.name.clone())
                        .unwrap_or_default();
                    let _ = write!(
                        groups,
                        "<item><groupId>{}</groupId><groupName>{}</groupName></item>",
                        xml_escape(gid),
                        xml_escape(&name)
                    );
                }
                format!(
                    "<groupSet>{}</groupSet><instanceId>{}</instanceId>",
                    groups,
                    xml_escape(&id)
                )
            }
            _ => format!("<instanceId>{}</instanceId>", xml_escape(&id)),
        };
        ec2_xml("DescribeInstanceAttributeResponse", &body)
    })
}

fn not_found_instance(id: &str) -> Response {
    ec2_error(
        400,
        "InvalidInstanceID.NotFound",
        &format!("The instance ID '{}' does not exist", xml_escape(id)),
    )
}

fn modify_instance_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        let pos = match require_instance(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        if let Some(b) = params.bool("DisableApiTermination.Value") {
            state.instances[pos].disable_api_termination = b;
        }
        if let Some(b) = params.bool("DisableApiStop.Value") {
            state.instances[pos].disable_api_stop = b;
        }
        if let Some(b) = params.bool("EbsOptimized.Value") {
            state.instances[pos].ebs_optimized = b;
        }
        if let Some(b) = params.bool("SourceDestCheck.Value") {
            state.instances[pos].source_dest_check = b;
            // mirror into the primary eni
            if let Some(e) = state.enis.iter_mut().find(|e| {
                e.attached
                    .as_ref()
                    .map(|a| a.instance_id == id)
                    .unwrap_or(false)
            }) {
                e.source_dest_check = b;
            }
        }
        if let Some(v) = params.get("InstanceType.Value") {
            state.instances[pos].instance_type = v.to_string();
        }
        if let Some(v) = params.get("UserData.Value") {
            state.instances[pos].user_data = Some(v.to_string());
        }
        if let Some(v) = params.get("SriovNetSupport.Value") {
            state.instances[pos].sriov_net_support = Some(v.to_string());
        }
        ok_xml("ModifyInstanceAttributeResponse")
    })
}

fn set_monitoring(req: &Request, params: &P, enabled: bool) -> Response {
    let ids: Vec<String> = params
        .list("InstanceId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        for id in &ids {
            if let Some(pos) = state.instances.iter().position(|i| i.id == *id) {
                state.instances[pos].monitoring = enabled;
            }
        }
        let mut out = String::new();
        for id in &ids {
            let _ = write!(
                out,
                "<instancesSet><item><instanceId>{}</instanceId><monitoring>{}</monitoring></item></instancesSet>",
                xml_escape(id),
                if enabled { "enabled" } else { "disabled" }
            );
        }
        let root = if enabled {
            "MonitorInstancesResponse"
        } else {
            "UnmonitorInstancesResponse"
        };
        ec2_xml(root, &out)
    })
}

fn get_console_output(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        if require_instance(state, &id).is_err() {
            return require_instance(state, &id).err().unwrap();
        }
        let text = "Linux version 2.6.16-xenU (builder@patchbat.amazonsa) (gcc version 4.0.1 20050727 (Red Hat 4.0.1-5)) #1 SMP Thu Oct 26 08:41:26 SAST 2006".as_bytes();
        let encoded = b64_encode(text);
        ec2_xml(
            "GetConsoleOutputResponse",
            &format!(
                "<instancesSet>{}<instanceId>{}</instanceId></instancesSet><output>{}</output>",
                "",
                xml_escape(&id),
                encoded
            ),
        )
    })
}

fn get_password_data(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        if require_instance(state, &id).is_err() {
            return require_instance(state, &id).err().unwrap();
        }
        ec2_xml(
            "GetPasswordDataResponse",
            &format!("<instanceId>{}</instanceId>", xml_escape(&id)),
        )
    })
}

fn get_instance_uefi_data(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        if require_instance(state, &id).is_err() {
            return require_instance(state, &id).err().unwrap();
        }
        ec2_xml(
            "GetInstanceUefiDataResponse",
            &format!("<instanceId>{}</instanceId><data/>", xml_escape(&id)),
        )
    })
}

fn get_launch_template_data(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        let pos = match require_instance(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let i = state.instances[pos].clone();
        let mut sgs = String::new();
        for gid in &i.security_group_ids {
            let name = state
                .security_groups
                .iter()
                .find(|sg| sg.id.as_str() == gid)
                .map(|sg| sg.name.clone())
                .unwrap_or_default();
            let _ = write!(sgs, "{}", sg_name_pair(gid, &name));
        }
        ec2_xml(
            "GetLaunchTemplateDataResponse",
            &format!(
                "<launchTemplateData><instanceType>{}</instanceType><imageId>{}</imageId>{}{}</launchTemplateData>",
                xml_escape(&i.instance_type),
                xml_escape(&i.image_id),
                if sgs.is_empty() {
                    String::new()
                } else {
                    format!("<securityGroups>{}</securityGroups>", sgs)
                },
                String::new()
            ),
        )
    })
}

fn sg_name_pair(gid: &str, name: &str) -> String {
    format!(
        "<item><groupId>{}</groupId><groupName>{}</groupName></item>",
        xml_escape(gid),
        xml_escape(name)
    )
}

fn modify_instance_metadata_options(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        if require_instance(state, &id).is_err() {
            return require_instance(state, &id).err().unwrap();
        }
        ec2_xml(
            "ModifyInstanceMetadataOptionsResponse",
            &format!("<instanceId>{}</instanceId>", xml_escape(&id)),
        )
    })
}

fn describe_instance_metadata_options(req: &Request, params: &P) -> Response {
    let id = params.str("InstanceId");
    with_state(req, move |state| {
        if require_instance(state, &id).is_err() {
            return require_instance(state, &id).err().unwrap();
        }
        ec2_xml(
            "DescribeInstanceMetadataOptionsResponse",
            &format!(
                "<instanceId>{}</instanceId><httpTokens>optional</httpTokens><httpEndpoint>enabled</httpEndpoint><httpPutResponseHopLimit>1</httpPutResponseHopLimit><httpProtocolIpv6>disabled</httpProtocolIpv6><instanceMetadataTags>disabled</instanceMetadataTags><state>applied</state>",
                xml_escape(&id)
            ),
        )
    })
}

fn get_serial_console(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        let v = state.serial_console_access;
        ec2_xml(
            "GetSerialConsoleAccessStatusResponse",
            &format!(
                "<serialConsoleAccessEnabled>{}</serialConsoleAccessEnabled>",
                v
            ),
        )
    })
}

fn set_serial_console(req: &Request, params: &P, on: bool) -> Response {
    with_state(req, move |state| {
        state.serial_console_access = on;
        ec2_xml(
            "SerialConsoleAccessStatusResponse",
            &format!(
                "<serialConsoleAccessEnabled>{}</serialConsoleAccessEnabled>",
                on
            ),
        )
    })
}

fn get_instance_metadata_defaults(req: &Request, params: &P) -> Response {
    ec2_xml(
        "GetInstanceMetadataDefaultsResponse",
        &format!(
            "<accountLevel>{}<httpTokens>optional</httpTokens><httpEndpoint>enabled</httpEndpoint><httpPutResponseHopLimit>1</httpPutResponseHopLimit><httpProtocolIpv6>disabled</httpProtocolIpv6><instanceMetadataTags>disabled</instanceMetadataTags></accountLevel>",
            ""
        ),
    )
}

fn modify_instance_metadata_defaults(req: &Request, params: &P) -> Response {
    ec2_xml(
        "ModifyInstanceMetadataDefaultsResponse",
        &format!(
            "<accountLevel>{}<httpTokens>{}</httpTokens><httpEndpoint>{}</httpEndpoint><httpPutResponseHopLimit>{}</httpPutResponseHopLimit></accountLevel>",
            "",
            params.get("HttpTokens").unwrap_or("optional"),
            params.get("HttpEndpoint").unwrap_or("enabled"),
            params.get("HttpPutResponseHopLimit").unwrap_or("1"),
        ),
    )
}

/// Minimal standard base64 (padding included).
pub fn b64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        out.push(TABLE[(b[0] >> 2) as usize] as char);
        out.push(TABLE[((b[0] & 0x03) << 4 | (b[1] >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((b[1] & 0x0f) << 2 | (b[2] >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b[2] & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Images (AMIs)
// ---------------------------------------------------------------------------

mod amis {
    use serde_json::Value;

    /// amazon-owned roster recorded from moto's resource files, per region.
    static US_EAST_1: std::sync::LazyLock<Vec<Value>> = std::sync::LazyLock::new(|| {
        serde_json::from_slice(include_bytes!("amis/amis-us-east-1.json")).unwrap_or_default()
    });

    pub fn roster(region: &str) -> &'static Vec<Value> {
        if region == "us-east-1" {
            &US_EAST_1
        } else {
            static EMPTY: std::sync::LazyLock<Vec<Value>> = std::sync::LazyLock::new(Vec::new);
            &EMPTY
        }
    }

    /// Build an Image from a roster json row.
    pub fn parse(a: &Value) -> super::super::state::Image {
        use super::super::state::Bdm;
        let bdmv = vec![Bdm {
            device_name: a
                .get("root_device_name")
                .and_then(|v| v.as_str())
                .unwrap_or("/dev/sda1")
                .to_string(),
            ebs_snapshot_id: Some("snap-00000000000000000".to_string()),
            ebs_volume_size: Some(15),
            ebs_volume_type: "standard".into(),
            ebs_delete_on_term: false,
            ..Default::default()
        }];
        super::super::state::Image {
            id: a
                .get("ami_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: a
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            description: a
                .get("description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            state: a
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("available")
                .to_string(),
            owner_id: a
                .get("owner_id")
                .and_then(|v| v.as_str())
                .unwrap_or("801119661308")
                .to_string(),
            owner_alias: Some("amazon".into()),
            is_public: a.get("public").and_then(|v| v.as_bool()).unwrap_or(true),
            root_device_type: a
                .get("root_device_type")
                .and_then(|v| v.as_str())
                .unwrap_or("ebs")
                .to_string(),
            root_device_name: a
                .get("root_device_name")
                .and_then(|v| v.as_str())
                .unwrap_or("/dev/sda1")
                .to_string(),
            architecture: a
                .get("architecture")
                .and_then(|v| v.as_str())
                .unwrap_or("x86_64")
                .to_string(),
            virtualization_type: a
                .get("virtualization_type")
                .and_then(|v| v.as_str())
                .unwrap_or("hvm")
                .to_string(),
            hypervisor: a
                .get("hypervisor")
                .and_then(|v| v.as_str())
                .unwrap_or("xen")
                .to_string(),
            image_type: a
                .get("image_type")
                .and_then(|v| v.as_str())
                .unwrap_or("machine")
                .to_string(),
            platform: a
                .get("platform")
                .and_then(|v| v.as_str())
                .unwrap_or("linux")
                .to_string(),
            sriov: a
                .get("sriov")
                .and_then(|v| v.as_str())
                .unwrap_or("simple")
                .to_string(),
            image_location: a
                .get("image_location")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            usage_operation: a
                .get("usage_operation")
                .and_then(|v| v.as_str())
                .unwrap_or("RunInstances")
                .to_string(),
            block_device_mappings: if a.get("root_device_type").and_then(|v| v.as_str())
                == Some("ebs")
            {
                bdmv
            } else {
                Vec::new()
            },
            ..Default::default()
        }
    }
}

pub fn image_item(i: &super::state::Image) -> String {
    let mut out = String::from("<item>");
    let _ = write!(
        out,
        "<description>{}</description>",
        i.description.clone().unwrap_or_default()
    );
    let _ = write!(
        out,
        "<hypervisor>{}</hypervisor>",
        xml_escape(&i.hypervisor)
    );
    let _ = write!(out, "<imageId>{}</imageId>", xml_escape(&i.id));
    let _ = write!(
        out,
        "<imageOwnerAlias>{}</imageOwnerAlias>",
        i.owner_alias.clone().unwrap_or_default()
    );
    let _ = write!(
        out,
        "<imageLocation>{}</imageLocation>",
        xml_escape(&i.image_location)
    );
    let _ = write!(out, "<imageType>{}</imageType>", xml_escape(&i.image_type));
    let _ = write!(out, "<isPublic>{}</isPublic>", i.is_public);
    let _ = write!(
        out,
        "<kernelId>{}</kernelId>",
        i.kernel_id.clone().unwrap_or_default()
    );
    let _ = write!(
        out,
        "<name>{}</name>",
        xml_escape(i.name.clone().unwrap_or_default().as_str())
    );
    let _ = write!(out, "<ownerId>{}</ownerId>", i.owner_id);
    let _ = write!(
        out,
        "<ownerAlias>{}</ownerAlias>",
        i.owner_alias.clone().unwrap_or_default()
    );
    let _ = write!(out, "<platform>{}</platform>", xml_escape(&i.platform));
    let _ = write!(
        out,
        "<usageOperation>{}</usageOperation>",
        xml_escape(&i.usage_operation)
    );
    let _ = write!(
        out,
        "<architecture>{}</architecture>",
        xml_escape(&i.architecture)
    );
    let _ = write!(
        out,
        "<rootDeviceName>{}</rootDeviceName>",
        xml_escape(&i.root_device_name)
    );
    let _ = write!(
        out,
        "<rootDeviceType>{}</rootDeviceType>",
        xml_escape(&i.root_device_type)
    );
    let _ = write!(out, "<state>{}</state>", xml_escape(&i.state));
    if !i.sriov.is_empty() {
        let _ = write!(
            out,
            "<sriovNetSupport>{}</sriovNetSupport>",
            xml_escape(&i.sriov)
        );
    }
    let _ = write!(
        out,
        "<virtualizationType>{}</virtualizationType>",
        xml_escape(&i.virtualization_type)
    );
    out.push_str("<blockDeviceMapping>");
    for b in &i.block_device_mappings {
        out.push_str("<item>");
        let _ = write!(
            out,
            "<deviceName>{}</deviceName>",
            xml_escape(&b.device_name)
        );
        if b.virtual_name.is_some() && !b.virtual_name.as_deref().unwrap_or_default().is_empty() {
            let _ = write!(
                out,
                "<virtualName>{}</virtualName>",
                xml_escape(b.virtual_name.clone().unwrap_or_default().as_str())
            );
        }
        if b.ebs_volume_size.is_some() || b.ebs_snapshot_id.is_some() {
            out.push_str("<ebs>");
            if let Some(s) = &b.ebs_snapshot_id {
                let _ = write!(out, "<snapshotId>{}</snapshotId>", xml_escape(s));
            }
            if let Some(s) = b.ebs_volume_size {
                let _ = write!(out, "<volumeSize>{}</volumeSize>", s);
            }
            if !b.ebs_volume_type.is_empty() {
                let _ = write!(
                    out,
                    "<volumeType>{}</volumeType>",
                    xml_escape(&b.ebs_volume_type)
                );
            }
            let _ = write!(
                out,
                "<deleteOnTermination>{}</deleteOnTermination>",
                b.ebs_delete_on_term
            );
            if b.ebs_encrypted {
                out.push_str("<encrypted>true</encrypted>");
            }
            out.push_str("</ebs>");
        }
        out.push_str("</item>");
    }
    out.push_str("</blockDeviceMapping>");
    out.push_str(&tags_xml(&i.tags));
    out.push_str("</item>");
    out
}

fn describe_images(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("ImageId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let owners: Vec<String> = params.list("Owner").iter().map(|s| s.to_string()).collect();
    let owner_ids: Vec<String> = params
        .list("OwnerId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let all_owners: Vec<String> = if owners.is_empty() { owner_ids } else { owners };
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let region = req.region.clone();
        let account = req.account.to_string();
        // roster rows as Image values
        let mut items = String::new();
        let own_all = all_owners.is_empty();
        for a in amis::roster(&req.region).iter() {
            let img = amis::parse(a);
            if !own_all
                && !(all_owners
                    .iter()
                    .any(|o| o == "amazon" || *o == img.owner_id))
            {
                continue;
            }
            if !ids.is_empty() && !ids.contains(&img.id) {
                continue;
            }
            let img2 = img.clone();
            if !filter_passes(&fs, move |f| match f {
                "image-id" => vec![img2.id.clone()],
                "owner-alias" => vec!["amazon".to_string()],
                "name" => img2.name.clone().map(|n| vec![n]).unwrap_or_default(),
                "state" => vec![img2.state.clone()],
                "platform" | "platform-details" => vec![img2.platform.clone()],
                "architecture" => vec![img2.architecture.clone()],
                "image-type" => vec![img2.image_type.clone()],
                "root-device-type" => vec![img2.root_device_type.clone()],
                "virtualization-type" => vec![img2.virtualization_type.clone()],
                "hypervisor" => vec![img2.hypervisor.clone()],
                other => tag_candidates(&img2.tags, other),
            }) {
                continue;
            }
            items.push_str(&image_item(&img));
        }
        // user-owned images
        for i in state.images.clone().iter().rev() {
            if !ids.is_empty() && !ids.contains(&i.id) {
                continue;
            }
            if !own_all {
                let owner_ok = all_owners.iter().any(|o| o == &account || o == &i.owner_id);
                if !owner_ok {
                    continue;
                }
            }
            let i2 = i.clone();
            if !filter_passes(&fs, move |f| match f {
                "image-id" => vec![i2.id.clone()],
                "owner-alias" => vec![],
                "name" => i2.name.clone().map(|n| vec![n]).unwrap_or_default(),
                other => tag_candidates(&i2.tags, other),
            }) {
                continue;
            }
            items.push_str(&image_item(i));
        }
        ec2_xml(
            "DescribeImagesResponse",
            &format!("<imagesSet>{}</imagesSet>", items),
        )
    })
}

fn create_image_from_instance(req: &Request, params: &P) -> Response {
    let instance_id = params.str("InstanceId");
    let name = params.str("Name");
    let description = params.str("Description");
    with_state(req, move |state| {
        let ipos = match state.instances.iter().position(|i| i.id == instance_id) {
            Some(i) => i,
            None => return not_found_instance(&instance_id),
        };
        let inst = state.instances[ipos].clone();
        let id = state.next_id("ami");
        let mut tags = wire_tags(params);
        for (k, v) in super::handlers::tag_spec_tags(params) {
            if !tags.iter().any(|(ek, _)| ek == &k) {
                tags.push((k, v));
            }
        }
        let image = super::state::Image {
            id: id.clone(),
            name: Some(name.clone()),
            description: Some(description.clone()),
            state: "available".into(),
            owner_id: req.account.to_string(),
            root_device_type: "ebs".into(),
            root_device_name: inst.root_device_name.clone(),
            block_device_mappings: vec![super::state::Bdm {
                device_name: inst.root_device_name.clone(),
                ebs_snapshot_id: Some(state.next_id("snap")),
                ebs_volume_size: Some(8),
                ebs_volume_type: "gp2".into(),
                ebs_delete_on_term: true,
                ..Default::default()
            }],
            tags,
            created_at: super::now_iso(),
            ..Default::default()
        };
        state.images.push(image);
        if inst.ami_launch_index <= 0 {
            // registry marker so RunInstances knows this ami id is valid
        }
        ec2_xml(
            "CreateImageResponse",
            &format!("<imageId>{}</imageId>", xml_escape(&id)),
        )
    })
}

fn register_image(req: &Request, params: &P) -> Response {
    let name = params.str("Name");
    let description = params.str("Description");
    let root_device_name = params
        .get("RootDeviceName")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "/dev/sda1".into());
    with_state(req, move |state| {
        let id = state.next_id("ami");
        let mut bdms: Vec<Bdm> = Vec::new();
        for i in params.container_indices("BlockDeviceMapping") {
            let base = format!("BlockDeviceMapping.{}", i + 1);
            bdms.push(Bdm {
                device_name: params
                    .get(&format!("{base}.DeviceName"))
                    .unwrap_or_default()
                    .to_string(),
                ebs_snapshot_id: params
                    .get(&format!("{base}.Ebs.SnapshotId"))
                    .map(|s| s.to_string()),
                ebs_volume_size: params
                    .get(&format!("{base}.Ebs.VolumeSize"))
                    .and_then(|v| v.parse().ok()),
                ebs_volume_type: params
                    .get(&format!("{base}.Ebs.VolumeType"))
                    .unwrap_or("standard")
                    .to_string(),
                ebs_delete_on_term: params
                    .get(&format!("{base}.Ebs.DeleteOnTermination"))
                    .map(|v| matches!(v, "true" | "True"))
                    .unwrap_or(false),
                ebs_encrypted: params
                    .get(&format!("{base}.Ebs.Encrypted"))
                    .map(|v| matches!(v, "true" | "True"))
                    .unwrap_or(false),
                ..Default::default()
            });
        }
        let tags = wire_tags(params);
        let image = super::state::Image {
            id: id.clone(),
            name: Some(name.clone()),
            description: Some(description.clone()),
            state: "available".into(),
            owner_id: req.account.to_string(),
            root_device_type: "ebs".into(),
            root_device_name: root_device_name.clone(),
            block_device_mappings: bdms,
            tags,
            created_at: now_iso(),
            ..Default::default()
        };
        state.images.push(image);
        ec2_xml(
            "RegisterImageResponse",
            &format!("<imageId>{}</imageId>", xml_escape(&id)),
        )
    })
}

fn deregister_image(req: &Request, params: &P) -> Response {
    let id = params.str("ImageId");
    with_state(req, move |state| {
        let pos = state.images.iter().position(|i| i.id == id);
        if let Some(pos) = pos {
            state.images.remove(pos);
            return ok_xml("DeregisterImageResponse");
        }
        let rostered = amis::roster(&req.region)
            .iter()
            .any(|a| a.get("ami_id").and_then(|v| v.as_str()) == Some(id.as_str()));
        if rostered {
            return ok_xml("DeregisterImageResponse");
        }
        ec2_error(
            400,
            "InvalidAMIID.NotFound",
            &format!("The image id '[{}] does not exist", xml_escape(&id)),
        )
    })
}

fn copy_image(req: &Request, params: &P) -> Response {
    let source = params.str("SourceImageId");
    let source_region = params
        .get("SourceRegion")
        .map(|s| s.to_string())
        .unwrap_or_else(|| req.region.clone());
    let name = params.get("Name").map(|s| s.to_string());
    let description = params.get("Description").map(|s| s.to_string());
    with_state(req, move |state| {
        // cross-region copy: source image comes from the other region's partition
        let src = if source_region == req.region {
            state
                .images
                .iter()
                .find(|i| i.id == source)
                .cloned()
                .or_else(|| {
                    amis::roster(&req.region)
                        .iter()
                        .find(|a| a.get("ami_id").and_then(|v| v.as_str()) == Some(source.as_str()))
                        .map(amis::parse)
                })
        } else {
            // other region partition (source account) — moto reads the other region's backend
            let slot = super::state::shared();
            let other = slot.read();
            match other.get(&(req.account, source_region.clone())) {
                Some(other_state) => other_state
                    .images
                    .iter()
                    .find(|i| i.id == source)
                    .cloned()
                    .or_else(|| {
                        amis::roster(&source_region)
                            .iter()
                            .find(|a| {
                                a.get("ami_id").and_then(|v| v.as_str()) == Some(source.as_str())
                            })
                            .map(amis::parse)
                    }),
                None => None,
            }
        };
        let src = match src {
            Some(a) => a,
            None => {
                return ec2_error(
                    400,
                    "InvalidAMIID.NotFound",
                    &format!("The image id '[{}] does not exist", xml_escape(&source)),
                )
            }
        };
        let id = state.next_id("ami");
        let mut image = src.clone();
        image.id = id.clone();
        image.name = name;
        image.description = description;
        image.owner_id = req.account.to_string();
        image.owner_alias = None;
        image.source_image_id = Some(source);
        image.created_at = now_iso();
        state.images.push(image);
        ec2_xml(
            "CopyImageResponse",
            &format!("<imageId>{}</imageId>", xml_escape(&id)),
        )
    })
}

fn describe_image_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("ImageId");
    let attr = params.str("Attribute");
    with_state(req, move |state| {
        let image = state
            .images
            .iter()
            .find(|i| i.id == id)
            .cloned()
            .or_else(|| {
                amis::roster(&req.region)
                    .iter()
                    .find(|a| a.get("ami_id").and_then(|v| v.as_str()) == Some(id.as_str()))
                    .map(amis::parse)
            });
        let image = match image {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidAMIID.NotFound",
                    &format!("The image id '[{}] does not exist", xml_escape(&id)),
                )
            }
        };
        let body = match attr.as_str() {
            "launchPermission" | "launch-permission" => {
                let mut lp = String::new();
                for (kind, value) in image.launch_permissions.iter() {
                    let _ = write!(lp, "<item><{kind}>{value}</{kind}></item>");
                }
                if lp.is_empty() {
                    String::from("<launchPermissions/>")
                } else {
                    format!("<launchPermissions>{}</launchPermissions>", lp)
                }
            }
            "description" | "descriptionKey" => format!(
                "<description><value>{}</value></description>",
                xml_escape(image.description.as_deref().unwrap_or_default())
            ),
            _ => String::new(),
        };
        ec2_xml(
            "DescribeImageAttributeResponse",
            &format!("<imageId>{}</imageId>{}", xml_escape(&id), body),
        )
    })
}

fn modify_image_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("ImageId");
    with_state(req, move |state| {
        let pos = state.images.iter().position(|i| i.id == id);
        let pos = match pos {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidAMIID.NotFound",
                    &format!("The image id '[{}] does not exist", xml_escape(&id)),
                )
            }
        };
        let users: Vec<String> = params
            .list("UserId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let groups: Vec<String> = params
            .list("UserGroup")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let op = params
            .get("LaunchPermission.OperationType")
            .map(|s| s.to_string())
            .unwrap_or_else(|| "add".into());
        if op == "add" {
            for u in &users {
                state.images[pos]
                    .launch_permissions
                    .push(("UserId".into(), u.clone()));
            }
            for g in &groups {
                state.images[pos]
                    .launch_permissions
                    .push(("Group".into(), g.clone()));
            }
        } else {
            state.images[pos]
                .launch_permissions
                .retain(|(_k, v)| !users.contains(v));
        }
        if let Some(d) = params.get("Description.Value") {
            state.images[pos].description = Some(d.to_string());
        }
        ok_xml("ModifyImageAttributeResponse")
    })
}

// ---------------------------------------------------------------------------
// Volumes
// ---------------------------------------------------------------------------

pub fn volume_item(v: &Volume) -> String {
    let attachment = if let Some(a) = &v.attachment {
        format!(
            "<item><attachTime>{}</attachTime><instanceId>{}</instanceId><device>{}</device><status>{}</status><deleteOnTermination>{}</deleteOnTermination></item>",
            xml_escape(&a.attach_time),
            xml_escape(&a.instance_id),
            xml_escape(&a.device),
            xml_escape(&a.status),
            a.delete_on_termination
        )
    } else {
        String::new()
    };
    let mut mpi = String::new();
    if v.multi_attach {
        mpi = String::from("<multiAttachEnabled>true</multiAttachEnabled>");
    }
    format!(
        "<item>{}<volumeId>{}</volumeId><size>{}</size><snapshotId>{}</snapshotId><availabilityZone>{}</availabilityZone><state>{}</state><createTime>{}</createTime>{}{}{}</item>",
        "",
        xml_escape(&v.id),
        v.size,
        xml_escape(&v.snapshot_id),
        xml_escape(&v.az),
        xml_escape(&v.state),
        xml_escape(&v.create_time),
        if let Some(iops) = v.iops { format!("<iops>{}</iops>", iops) } else { String::new() },
        if !v.vtype.is_empty() { format!("<volumeType>{}</volumeType>", xml_escape(&v.vtype)) } else { String::new() },
        format!("{}<tagSet>{}</tagSet>{}",
            if !attachment.is_empty() { format!("<attachmentSet>{}</attachmentSet>", attachment) } else { String::from("<attachmentSet/>") },
            "",
            mpi
        )
    )
}

fn require_volume(state: &super::state::Ec2State, id: &str) -> Result<usize, Response> {
    match state.volumes.iter().position(|v| v.id == id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidVolume.NotFound",
            &format!("The volume '{}' does not exist", xml_escape(id)),
        )),
    }
}

fn create_volume(req: &Request, params: &P) -> Response {
    let az = params.str("AvailabilityZone");
    let size = params.i64("Size");
    let snapshot_id = params.str("SnapshotId");
    let volume_type = params
        .get("VolumeType")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "standard".into());
    let iops = params.i64("Iops");
    let throughput = params.i64("Throughput");
    let encrypted = params.bool("Encrypted").unwrap_or(false);
    with_state(req, move |state| {
        if !snapshot_id.is_empty() && state.snapshots.iter().all(|s| s.id != snapshot_id) {
            // moto springs volumes from snapshots anywhere in the account
            let slot = super::state::shared();
            let other = slot.read();
            let found = other
                .values()
                .any(|st| st.snapshots.iter().any(|s| s.id == snapshot_id));
            drop(other);
            if !found {
                return ec2_error(
                    400,
                    "InvalidSnapshot.NotFound",
                    &format!(
                        "The snapshot '{}' does not exist.",
                        xml_escape(&snapshot_id)
                    ),
                );
            }
        }
        let id = state.next_id("vol");
        let mut tags = wire_tags(params);
        for (k, v) in super::handlers::tag_spec_tags(params) {
            if !tags.iter().any(|(ek, _)| ek == &k) {
                tags.push((k.clone(), v.clone()));
            }
        }
        let mut v = Volume {
            id: id.clone(),
            size: size.unwrap_or(0),
            vtype: volume_type.clone(),
            state: "available".into(),
            snapshot_id: snapshot_id.clone(),
            encrypted,
            iops,
            throughput,
            az: az.clone(),
            create_time: now_iso(),
            ..Default::default()
        };
        if !tags.is_empty() {
            v.tags = tags;
        }
        state.volumes.push(v);
        let v = state
            .volumes
            .iter()
            .find(|x| x.id == id)
            .cloned()
            .unwrap_or_default();
        ec2_xml(
            "CreateVolumeResponse",
            &super::ec2b::volume_fields(&v),
        )
    })
}

fn describe_volumes(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("VolumeId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let mut items = String::new();
        for v in &state.volumes {
            if !ids.is_empty() && !ids.contains(&v.id) {
                continue;
            }
            let v2 = v.clone();
            if !filter_passes(&fs, move |f| match f {
                "volume-id" => vec![v2.id.clone()],
                "size" => vec![v2.size.to_string()],
                "status" | "volume-status" => vec![v2.state.clone()],
                "availability-zone" | "zone" => vec![v2.az.clone()],
                "snapshot-id" => vec![v2.snapshot_id.clone()],
                "volume-type" => vec![v2.vtype.clone()],
                "attachment.instance-id" => v2
                    .attachment
                    .as_ref()
                    .map(|a| vec![a.instance_id.clone()])
                    .unwrap_or_default(),
                "attachment.status" => v2
                    .attachment
                    .as_ref()
                    .map(|a| vec![a.status.clone()])
                    .unwrap_or_default(),
                "tag:quota" => vec![],
                other => tag_candidates(&v2.tags, other),
            }) {
                continue;
            }
            items.push_str(&format!("<item>{}</item>", super::ec2b::volume_fields(v)));
        }
        ec2_xml(
            "DescribeVolumesResponse",
            &format!("<volumeSet>{}</volumeSet>", items),
        )
    })
}

fn delete_volume(req: &Request, params: &P) -> Response {
    let id = params.str("VolumeId");
    with_state(req, move |state| {
        if require_volume(state, &id).is_err() {
            return require_volume(state, &id).err().unwrap();
        }
        let pos = state.volumes.iter().position(|v| v.id == id).unwrap_or(0);
        if state.volumes[pos].attachment.is_some() {
            return ec2_error(
                400,
                "InvalidVolume.InUse",
                "The volume is currently attached",
            );
        }
        state.volumes.remove(pos);
        ok_xml("DeleteVolumeResponse")
    })
}

fn attach_volume(req: &Request, params: &P) -> Response {
    let id = params.str("VolumeId");
    let instance_id = params.str("InstanceId");
    let device = params.str("Device");
    with_state(req, move |state| {
        let vpos = match require_volume(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        state.volumes[vpos].state = "in-use".into();
        state.volumes[vpos].attachment = Some(VolumeAttachment {
            instance_id: instance_id.clone(),
            device: device.clone(),
            status: "attached".into(),
            attach_time: now_iso(),
            delete_on_termination: false,
        });
        ec2_xml(
            "AttachVolumeResponse",
            &format!(
                "<volumeId>{}</volumeId><instanceId>{}</instanceId><device>{}</device><status>attaching</status><attachTime>{}</attachTime>",
                xml_escape(&id),
                xml_escape(&instance_id),
                xml_escape(&device),
                super::now_iso()
            ),
        )
    })
}

fn detach_volume(req: &Request, params: &P) -> Response {
    let id = params.str("VolumeId");
    with_state(req, move |state| {
        let vpos = match require_volume(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        state.volumes[vpos].state = "available".into();
        state.volumes[vpos].attachment = None;
        ec2_xml(
            "DetachVolumeResponse",
            &format!(
                "<volumeId>{}</volumeId><status>detaching</status>",
                xml_escape(&id)
            ),
        )
    })
}

fn modify_volume(req: &Request, params: &P) -> Response {
    let id = params.str("VolumeId");
    with_state(req, move |state| {
        let vpos = match require_volume(state, &id) {
            Ok(i) => i,
            Err(e) => return e,
        };
        if let Some(s) = params.i64("Size") {
            state.volumes[vpos].size = s;
        }
        if let Some(t) = params.get("VolumeType") {
            state.volumes[vpos].vtype = t.to_string();
        }
        if let Some(i) = params.i64("Iops") {
            state.volumes[vpos].iops = Some(i);
        }
        if let Some(t) = params.i64("Throughput") {
            state.volumes[vpos].throughput = Some(t);
        }
        ec2_xml(
            "ModifyVolumeResponse",
            &format!(
                "<volumeModification><volumeId>{}</volumeId>{}</volumeModification>",
                xml_escape(&id),
                format!(
                    "<modificationState>optimizing</modificationState>{}{}",
                    if let Some(s) = params.i64("Size") {
                        format!("<targetSize>{}</targetSize>", s)
                    } else {
                        String::new()
                    },
                    if let Some(tt) = params.get("VolumeType") {
                        format!("<targetVolumeType>{}</targetVolumeType>", xml_escape(tt))
                    } else {
                        String::new()
                    }
                )
            ),
        )
    })
}

fn describe_volume_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("VolumeId");
    with_state(req, move |state| {
        let _ = require_volume(state, &id);
        ec2_xml(
            "DescribeVolumeAttributeResponse",
            &format!(
                "<volumeId>{}</volumeId><autoEnableIO><value>false</value></autoEnableIO>",
                xml_escape(&id)
            ),
        )
    })
}

fn describe_volume_status(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("VolumeId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    with_state(req, move |state| {
        let ids = ids.clone();
        let mut items = String::new();
        for v in &state.volumes {
            if !ids.is_empty() && !ids.contains(&v.id) {
                continue;
            }
            let _ = write!(
                items,
                "<item><volumeId>{}</volumeId><availabilityZone>{}</availabilityZone><volumeStatus><status>ok</status><details><item><name>io-enabled</name><status>passed</status></item></details></volumeStatus></item>",
                xml_escape(&v.id),
                xml_escape(&v.az)
            );
        }
        ec2_xml(
            "DescribeVolumeStatusResponse",
            &format!("<volumeStatusSet>{}</volumeStatusSet>", items),
        )
    })
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

pub fn snapshot_item(region: &str, account: u64, s: &super::state::Snapshot) -> String {
    format!(
        "<item>{}<snapshotId>{}</snapshotId><volumeId>{}</volumeId><state>{}</state><startTime>{}</startTime><progress>{}%</progress><ownerId>{}</ownerId><description>{}</description><volumeSize>{}</volumeSize>{}{}</item>",
        "",
        xml_escape(&s.id),
        xml_escape(&s.volume_id),
        xml_escape(&s.state),
        xml_escape(&s.start_time),
        xml_escape(&s.progress),
        s.owner_id,
        xml_escape(&s.description),
        s.volume_size,
        if s.owner_alias.is_some() { format!("<ownerAlias>{}</ownerAlias>", xml_escape(s.owner_alias.as_deref().unwrap_or_default())) } else { String::new() },
        tags_xml(&s.tags)
    )
}

fn require_snapshot(state: &super::state::Ec2State, id: &str) -> Result<usize, Response> {
    match state.snapshots.iter().position(|s| s.id == id) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidSnapshot.NotFound",
            &format!("The snapshot '{}' does not exist.", xml_escape(id)),
        )),
    }
}

fn create_snapshot(req: &Request, params: &P) -> Response {
    let volume_id = params.str("VolumeId");
    let description = params.str("Description");
    with_state(req, move |state| {
        if state.volumes.iter().all(|v| v.id != volume_id) {
            return ec2_error(
                400,
                "InvalidVolume.NotFound",
                &format!("The volume '{}' does not exist.", xml_escape(&volume_id)),
            );
        }
        let id = state.next_id("snap");
        let vol = state
            .volumes
            .iter()
            .find(|v| v.id == volume_id)
            .cloned()
            .unwrap_or_default();
        let tags = wire_tags(params);
        let snap = super::state::Snapshot {
            id: id.clone(),
            volume_id: volume_id.clone(),
            state: "completed".into(),
            progress: "100".into(),
            start_time: now_iso(),
            description: description.clone(),
            owner_id: req.account.to_string(),
            tags,
            encrypted: vol.encrypted,
            volume_size: vol.size,
            ..Default::default()
        };
        state.snapshots.push(snap.clone());
        ec2_xml(
            "CreateSnapshotResponse",
            &format!(
                "<snapshot>{}</snapshot>",
                super::ec2b::snapshot_fields(&snap)
            ),
        )
    })
}

fn describe_snapshots(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("SnapshotId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let account = req.account.to_string();
        let owners: Vec<String> = params.list("Owner").iter().map(|s| s.to_string()).collect();
        let restorable_by = params.str("RestorableBy");
        let mut items = String::new();
        for s in &state.snapshots {
            if !ids.is_empty() && !ids.contains(&s.id) {
                continue;
            }
            if !owners.is_empty() && !owners.iter().any(|o| o == "self" || o == &account) {
                continue;
            }
            if !restorable_by.is_empty() && restorable_by != account && restorable_by != "self" {
                continue;
            }
            let s2 = s.clone();
            if !filter_passes(&fs, move |f| match f {
                "snapshot-id" => vec![s2.id.clone()],
                "volume-id" => vec![s2.volume_id.clone()],
                "owner-id" | "owner-alias" => vec![s2.owner_id.clone()],
                "status" => vec![s2.state.clone()],
                "progress" => vec![s2.progress.clone()],
                "start-time" | "volume-size" => vec![s2.volume_size.to_string()],
                other => tag_candidates(&s2.tags, other),
            }) {
                continue;
            }
            items.push_str(&format!("<item>{}</item>", super::ec2b::snapshot_fields(s)));
        }
        ec2_xml(
            "DescribeSnapshotsResponse",
            &format!("<snapshotsSet>{}</snapshotsSet>", items),
        )
    })
}

fn delete_snapshot(req: &Request, params: &P) -> Response {
    let id = params.str("SnapshotId");
    with_state(req, move |state| {
        if require_snapshot(state, &id).is_err() {
            return require_snapshot(state, &id).err().unwrap();
        }
        let pos = state.snapshots.iter().position(|s| s.id == id).unwrap_or(0);
        state.snapshots.remove(pos);
        ok_xml("DeleteSnapshotResponse")
    })
}

fn copy_snapshot(req: &Request, params: &P) -> Response {
    let source_id = params.str("SourceSnapshotId");
    let sour_region = params
        .get("SourceRegion")
        .map(|s| s.to_string())
        .unwrap_or_else(|| req.region.clone());
    let description = params.get("Description").map(|s| s.to_string());
    with_state(req, move |state| {
        let src = if sour_region == req.region {
            state.snapshots.iter().find(|s| s.id == source_id).cloned()
        } else {
            let slot = super::state::shared();
            let other = slot.read();
            other
                .get(&(req.account, sour_region.clone()))
                .and_then(|st| st.snapshots.iter().find(|s| s.id == source_id).cloned())
        };
        let src = match src {
            Some(a) => a,
            None => {
                return ec2_error(
                    400,
                    "InvalidSnapshot.NotFound",
                    &format!("The snapshot '{}' does not exist.", xml_escape(&source_id)),
                )
            }
        };
        let id = state.next_id("snap");
        let mut snap = src.clone();
        snap.id = id.clone();
        snap.description = description.unwrap_or_default();
        snap.owner_id = req.account.to_string();
        snap.tags = Vec::new();
        state.snapshots.push(snap.clone());
        ec2_xml(
            "CopySnapshotResponse",
            &format!("<snapshotId>{}</snapshotId>", xml_escape(&id)),
        )
    })
}

fn describe_snapshot_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("SnapshotId");
    with_state(req, move |state| {
        if require_snapshot(state, &id).is_err() {
            return require_snapshot(state, &id).err().unwrap();
        }
        let snap = state
            .snapshots
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .unwrap_or_default();
        let mut perms = String::new();
        for (k, v) in &snap.create_volume_permissions {
            let _ = write!(perms, "<item><{k}>{v}</{k}></item>");
        }
        ec2_xml(
            "DescribeSnapshotAttributeResponse",
            &format!(
                "<snapshotId>{}</snapshotId>{}</createVolumePermission>{}<productCodes/><snapshotId>{}</snapshotId>",
                xml_escape(&id),
                if perms.is_empty() {
                    String::from("<createVolumePermissions/>")
                } else {
                    format!("<createVolumePermissions>{}</createVolumePermissions>", perms)
                },
                String::new(),
                xml_escape(&id)
            ),
        )
    })
}

fn modify_snapshot_attribute(req: &Request, params: &P) -> Response {
    let id = params.str("SnapshotId");
    with_state(req, move |state| {
        if require_snapshot(state, &id).is_err() {
            return require_snapshot(state, &id).err().unwrap();
        }
        let op = params
            .get("OperationType")
            .map(|s| s.to_string())
            .unwrap_or_else(|| "add".into());
        let users: Vec<String> = params
            .list("UserId")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let groups: Vec<String> = params
            .list("UserGroup")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let pos = state.snapshots.iter().position(|s| s.id == id).unwrap_or(0);
        if op == "add" {
            for u in &users {
                state.snapshots[pos]
                    .create_volume_permissions
                    .push(("UserId".into(), u.clone()));
            }
            for g in &groups {
                state.snapshots[pos]
                    .create_volume_permissions
                    .push(("Group".into(), g.clone()));
            }
        } else {
            state.snapshots[pos]
                .create_volume_permissions
                .retain(|(_k, v)| !users.contains(v) && !groups.contains(v));
        }
        ok_xml("ModifySnapshotAttributeResponse")
    })
}

fn create_snapshots(req: &Request, params: &P) -> Response {
    let description = params.str("Description");
    with_state(req, move |state| {
        let mut items = String::new();
        for i in params.container_indices("InstanceSpecification") {
            let instance_id = params
                .sub("InstanceSpecification", i, "InstanceId")
                .unwrap_or_default()
                .to_string();
            if state.instances.iter().all(|x| x.id != instance_id) {
                return ec2_error(
                    400,
                    "InvalidInstanceID.NotFound",
                    &format!(
                        "The instance ID '{}' does not exist",
                        xml_escape(&instance_id)
                    ),
                );
            }
            let id = state.next_id("snap");
            // mirror moto: snapshot the instance's root volume
            let vol_id = state
                .instances
                .iter()
                .find(|x| x.id == instance_id)
                .and_then(|x| x.ebs_root_volume_ids.first().cloned())
                .unwrap_or_default();
            let tags: Vec<(String, String)> = {
                let mut t: Vec<(String, String)> = Vec::new();
                for t2 in params.container_indices("TagSpecification") {
                    if params
                        .sub("TagSpecification", t2, "ResourceType")
                        .unwrap_or("snapshot")
                        != "snapshot"
                    {
                        continue;
                    }
                    for r in params.container_indices(&format!("TagSpecification.{}.Tag", t2 + 1)) {
                        if let Some(k) =
                            params.sub(&format!("TagSpecification.{}.Tag", t2 + 1), r, "Key")
                        {
                            t.push((
                                k.to_string(),
                                params
                                    .sub(&format!("TagSpecification.{}.Tag", t2 + 1), r, "Value")
                                    .unwrap_or_default()
                                    .to_string(),
                            ));
                        }
                    }
                }
                t
            };
            state.snapshots.push(super::state::Snapshot {
                id: id.clone(),
                volume_id: vol_id,
                state: "completed".into(),
                progress: "100".into(),
                start_time: now_iso(),
                description: description.clone(),
                owner_id: req.account.to_string(),
                tags,
                ..Default::default()
            });
            let s = state
                .snapshots
                .iter()
                .find(|x| x.id == id)
                .cloned()
                .unwrap_or_default();
            items.push_str(&format!(
                "<item>{}</item>",
                super::ec2b::snapshot_fields(&s)
            ));
        }
        ec2_xml(
            "CreateSnapshotsResponse",
            &format!("<snapshotsSet>{}</snapshotsSet>", items),
        )
    })
}

// ---------------------------------------------------------------------------
// Key pairs
// ---------------------------------------------------------------------------

fn keypair_item(k: &super::state::Keypair) -> String {
    format!(
        "<item><keyPairId>{}</keyPairId><keyName>{}</keyName><keyFingerprint>{}</keyFingerprint>{}{}</item>",
        xml_escape(&k.key_pair_id),
        xml_escape(&k.name),
        xml_escape(&k.fingerprint),
        if !k.material.is_empty() {
            format!("<keyMaterial>{}</keyMaterial>", xml_escape(&k.material))
        } else {
            String::new()
        },
        tags_xml(&k.tags)
    )
}

fn require_keypair(state: &super::state::Ec2State, name: &str) -> Result<usize, Response> {
    match state.key_pairs.iter().position(|k| k.name == name) {
        Some(i) => Ok(i),
        None => Err(ec2_error(
            400,
            "InvalidKeyPair.NotFound",
            &format!("The key pair '{}' does not exist", xml_escape(name)),
        )),
    }
}

const FAKE_RSA_KEY: &str = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4ZRscIHHtdjZ7NpXZsxZUZC3YWlMOYzC8LNAMGjP5sSKdJgIMYEkN5YHTPVCHTM1QTXVKWQ8NmGJ3SmyZLP";

fn new_key_material(key_type: &str) -> (String, String) {
    if key_type == "ed25519" {
        (
            "SHA256:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".into(),
            "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaWtlcnNhoA==\\n-----END OPENSSH PRIVATE KEY-----".into(),
        )
    } else {
        (
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .collect::<String>()
                    .chars()
                    .take(20)
                    .map(|c| format!("{:02x}", c as u8 % 255))
                    .collect::<Vec<_>>()
                    .join(":"),
                "ab",
                "cd",
                "ef",
                "01",
                "23",
                "45",
                "67",
                "89",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20"
            ),
            format!(
                "-----BEGIN RSA PRIVATE KEY-----\\n{}\\n-----END RSA PRIVATE KEY-----",
                FAKE_RSA_KEY
            ),
        )
    }
}

fn create_key_pair(req: &Request, params: &P) -> Response {
    let name = params.str("KeyName");
    let key_type = params
        .get("KeyType")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "rsa".to_string());
    if name.is_empty() {
        return ec2_error(
            400,
            "InvalidKeyPair.Duplicate",
            "The key pair AlreadyExists",
        );
    }
    with_state(req, move |state| {
        if state.key_pairs.iter().any(|k| k.name == name) {
            return ec2_error(
                400,
                "InvalidKeyPair.Duplicate",
                &format!("The key pair '{}' already exists", xml_escape(&name)),
            );
        }
        let (fingerprint, material) = new_key_material(&key_type);
        let tags = wire_tags(params);
        let id = state.next_id("key");
        let kp = super::state::Keypair {
            name: name.clone(),
            fingerprint,
            material,
            key_pair_id: id,
            tags,
            key_type: key_type.clone(),
        };
        state.key_pairs.push(kp.clone());
        ec2_xml(
            "CreateKeyPairResponse",
            &format!(
                "<keyPairId>{}</keyPairId><keyName>{}</keyName><keyFingerprint>{}</keyFingerprint><keyMaterial>{}</keyMaterial>{}",
                xml_escape(&kp.key_pair_id),
                xml_escape(&kp.name),
                xml_escape(&kp.fingerprint),
                xml_escape(&kp.material),
                tags_xml(&kp.tags)
            ),
        )
    })
}

fn describe_key_pairs(req: &Request, params: &P) -> Response {
    let names: Vec<String> = params
        .list("KeyName")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let mut items = String::new();
        for k in &state.key_pairs {
            if !names.is_empty() && !names.contains(&k.name) {
                continue;
            }
            let k2 = k.clone();
            if !filter_passes(&fs, move |f| match f {
                "key-name" | "key-pair-name" => vec![k2.name.clone()],
                "key-fingerprint" => vec![k2.fingerprint.clone()],
                other => tag_candidates(&k2.tags, other),
            }) {
                continue;
            }
            items.push_str(&keypair_item(k));
        }
        ec2_xml(
            "DescribeKeyPairsResponse",
            &format!("<keySet>{}</keySet>", items),
        )
    })
}

fn delete_key_pair(req: &Request, params: &P) -> Response {
    let name = params.str("KeyName");
    with_state(req, move |state| {
        let pos = match require_keypair(state, &name) {
            Ok(i) => i,
            Err(e) => return e,
        };
        state.key_pairs.remove(pos);
        ok_xml("DeleteKeyPairResponse")
    })
}

fn import_key_pair(req: &Request, params: &P) -> Response {
    let name = params.str("KeyName");
    let material = params
        .get("PublicKeyMaterial")
        .map(|s| s.to_string())
        .unwrap_or_default();
    with_state(req, move |state| {
        if state.key_pairs.iter().any(|k| k.name == name) {
            return ec2_error(
                400,
                "InvalidKeyPair.Duplicate",
                &format!("The key pair '{}' already exists", xml_escape(&name)),
            );
        }
        let id = state.next_id("key");
        let tags = wire_tags(params);
        let kp = super::state::Keypair {
            name: name.clone(),
            fingerprint: format!(
                "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:00:00:00:00:00:00:00:00:00:00:00",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .collect::<String>()
                    .chars()
                    .take(20)
                    .map(|c| format!("{:02x}", c as u8 % 255))
                    .collect::<Vec<_>>()
                    .join(":"),
                "ab",
                "cd",
                "ef",
                "01",
                "23",
                "45",
                "67",
                "89",
                "10"
            ),
            material: material.clone(),
            key_pair_id: id,
            tags,
            key_type: "rsa".into(),
        };
        state.key_pairs.push(kp.clone());
        ec2_xml(
            "ImportKeyPairResponse",
            &format!(
                "<keyPairId>{}</keyPairId><keyName>{}</keyName><keyFingerprint>{}</keyFingerprint>{}",
                xml_escape(&kp.key_pair_id),
                xml_escape(&kp.name),
                xml_escape(&kp.fingerprint),
                tags_xml(&kp.tags)
            ),
        )
    })
}

// ---------------------------------------------------------------------------
// Launch templates
// ---------------------------------------------------------------------------

fn launch_template_item(region: &str, account: u64, lt: &super::state::LaunchTemplate) -> String {
    format!(
        "<item><createTime>{}</createTime><createdBy>arn:aws:sts::{}:root</createdBy><defaultVersionNumber>{}</defaultVersionNumber><latestVersionNumber>{}</latestVersionNumber><launchTemplateId>{}</launchTemplateId><launchTemplateName>{}</launchTemplateName>{}{}</item>",
        xml_escape(&lt.created),
        account,
        lt.default_version,
        lt.latest_version,
        xml_escape(&lt.id),
        xml_escape(&lt.name),
        if lt.description.is_empty() { String::new() } else { format!("<description>{}</description>", xml_escape(&lt.description)) },
        tags_xml(&lt.tags)
    )
}

fn find_template_by(state: &super::state::Ec2State, id: &str, name: &str) -> Option<usize> {
    if !id.is_empty() {
        return state.launch_templates.iter().position(|l| l.id == id);
    }
    if !name.is_empty() {
        return state.launch_templates.iter().position(|l| l.name == name);
    }
    None
}

fn create_launch_template(req: &Request, params: &P) -> Response {
    let name = params.str("LaunchTemplateName");
    with_state(req, move |state| {
        if state.launch_templates.iter().any(|l| l.name == name) {
            return ec2_error(
                400,
                "InvalidLaunchTemplateName.AlreadyExistsException",
                &format!(
                    "A launch template named '{}' already exists.",
                    xml_escape(&name)
                ),
            );
        }
        let id = state.next_id("lt");
        let data = params
            .get("LaunchTemplateData.UserData")
            .unwrap_or_default()
            .to_string();
        let tags = wire_tags(params);
        let lt = super::state::LaunchTemplate {
            id: id.clone(),
            name: name.clone(),
            created: now_iso(),
            latest_version: 1,
            default_version: 1,
            tags,
            versions: vec![super::state::LaunchTemplateVersion {
                number: 1,
                data,
                default: true,
                created: now_iso(),
                ..Default::default()
            }],
            ..Default::default()
        };
        state.launch_templates.push(lt.clone());
        ec2_xml(
            "CreateLaunchTemplateResponse",
            &format!(
                "<launchTemplate>{}{}</launchTemplate>",
                super::ec2b::template_label(&lt),
                ""
            ),
        )
    })
}

fn create_launch_template_version(req: &Request, params: &P) -> Response {
    let id = params.str("LaunchTemplateId");
    let name = params.str("LaunchTemplateName");
    with_state(req, move |state| {
        let pos = match find_template_by(state, &id, &name) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidLaunchTemplate.NotFound",
                    "The specified launch template, if it exists, could not be located",
                )
            }
        };
        let number = state.launch_templates[pos].latest_version + 1;
        let v = super::state::LaunchTemplateVersion {
            number,
            data: String::new(),
            description: params.get("VersionDescription").map(|s| s.to_string()),
            created: now_iso(),
            default: false,
        };
        state.launch_templates[pos].versions.push(v);
        state.launch_templates[pos].latest_version = number;
        ec2_xml(
            "CreateLaunchTemplateVersionResponse",
            &format!(
                "<launchTemplateVersion>{}<launchTemplateId>{}</launchTemplateId><launchTemplateName>{}</launchTemplateName>{}<defaultVersion>false</defaultVersion></launchTemplateVersion>",
                "",
                xml_escape(&state.launch_templates[pos].id),
                xml_escape(&state.launch_templates[pos].name),
                String::new()
            ),
        )
    })
}

fn describe_launch_templates(req: &Request, params: &P) -> Response {
    let ids: Vec<String> = params
        .list("LaunchTemplateId")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let names: Vec<String> = params
        .list("LaunchTemplateName")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    with_state(req, move |state| {
        let region = req.region.clone();
        let account = req.account;
        let mut items = String::new();
        for lt in &state.launch_templates {
            if !ids.is_empty() && !ids.contains(&lt.id) {
                continue;
            }
            if !names.is_empty() && !names.contains(&lt.name) {
                continue;
            }
            let lt2 = lt.clone();
            if !filter_passes(&fs, move |f| match f {
                "launch-template-id" => vec![lt2.id.clone()],
                "launch-template-name" => vec![lt2.name.clone()],
                other => tag_candidates(&lt2.tags, other),
            }) {
                continue;
            }
            items.push_str(&launch_template_item(&region, account, lt));
        }
        ec2_xml(
            "DescribeLaunchTemplatesResponse",
            &format!("<launchTemplates>{}</launchTemplates>", items),
        )
    })
}

fn describe_launch_template_versions(req: &Request, params: &P) -> Response {
    let id = params.str("LaunchTemplateId");
    let name = params.str("LaunchTemplateName");
    with_state(req, move |state| {
        let pos = match find_template_by(state, &id, &name) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidLaunchTemplate.NotFound",
                    "The specified launch template, if it exists, could not be located",
                )
            }
        };
        let versions_param: Vec<String> = params
            .list("Version")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let lt = state.launch_templates[pos].clone();
        let mut items = String::new();
        for v in &lt.versions {
            if !versions_param.is_empty()
                && !versions_param.contains(&v.number.to_string())
                && !versions_param
                    .iter()
                    .any(|x| x == "$Latest" && v.number == lt.latest_version)
            {
                continue;
            }
            items.push_str(&format!(
                "<item>{}<versionNumber>{}</versionNumber><launchTemplateId>{}</launchTemplateId><launchTemplateName>{}</launchTemplateName>{}{}{}<createdBy>arn:aws:sts::{}:root</createdBy><createTime>{}</createTime>{}</item>",
                "",
                v.number,
                xml_escape(&lt.id),
                xml_escape(&lt.name),
                if let Some(d) = &v.description { format!("<description>{}</description>", xml_escape(d)) } else { String::new() },
                if v.default { "<defaultVersion>true</defaultVersion>" } else { "<defaultVersion>false</defaultVersion>" },
                if v.number == lt.latest_version { "<latestVersion>true</latestVersion>" } else { "<latestVersion>false</latestVersion>" },
                req.account,
                xml_escape(&v.created),
                String::new()
            ));
        }
        ec2_xml(
            "DescribeLaunchTemplateVersionsResponse",
            &format!("<launchTemplateVersions>{}</launchTemplateVersions>", items),
        )
    })
}

fn delete_launch_template(req: &Request, params: &P) -> Response {
    let id = params.str("LaunchTemplateId");
    let name = params.str("LaunchTemplateName");
    with_state(req, move |state| {
        let pos = match find_template_by(state, &id, &name) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidLaunchTemplate.NotFound",
                    "The specified launch template, if it exists, could not be located",
                )
            }
        };
        let lt = state.launch_templates.remove(pos);
        ec2_xml(
            "DeleteLaunchTemplateResponse",
            &format!(
                "<launchTemplate>{}{}</launchTemplate>",
                super::ec2b::template_label(&lt),
                ""
            ),
        )
    })
}

fn modify_launch_template(req: &Request, params: &P) -> Response {
    let id = params.str("LaunchTemplateId");
    let name = params.str("LaunchTemplateName");
    with_state(req, move |state| {
        let pos = match find_template_by(state, &id, &name) {
            Some(i) => i,
            None => {
                return ec2_error(
                    400,
                    "InvalidLaunchTemplate.NotFound",
                    "The specified launch template, if it exists, could not be located",
                )
            }
        };
        if let Some(v) = params.get("DefaultVersion") {
            let v = v.to_string();
            let num: i64 = if v == "$Default" {
                state.launch_templates[pos].default_version
            } else if v == "$Latest" {
                state.launch_templates[pos].latest_version
            } else {
                v.parse()
                    .unwrap_or(state.launch_templates[pos].latest_version)
            };
            state.launch_templates[pos].default_version = num;
        }
        ec2_xml(
            "ModifyLaunchTemplateResponse",
            &format!(
                "<launchTemplate>{}{}</launchTemplate>",
                super::ec2b::template_label(&state.launch_templates[pos]),
                ""
            ),
        )
    })
}

// ---------------------------------------------------------------------------
// Placement groups
// ---------------------------------------------------------------------------

fn create_placement_group(req: &Request, params: &P) -> Response {
    let name = params.str("GroupName");
    let strategy = params
        .get("Strategy")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "cluster".to_string());
    with_state(req, move |state| {
        if state.placement_groups.iter().any(|g| g.name == name) {
            return ec2_error(
                409,
                "InvalidPlacementGroup.Duplicate",
                &format!("The placement group '{}' already exists", xml_escape(&name)),
            );
        }
        let pg = super::state::PlacementGroup {
            name: name.clone(),
            strategy: strategy.clone(),
            state: "available".into(),
            group_id: format!("pg-{}", idhex::hex16()),
            partition_count: params.i64("PartitionCount"),
            spread_level: params.i64("SpreadLevel"),
            tags: wire_tags(params),
        };
        state.placement_groups.push(pg.clone());
        ec2_xml(
            "CreatePlacementGroupResponse",
            &format!(
                "<placementGroup><groupName>{}</groupName><strategy>{}</strategy><state>available</state><groupId>{}</groupId>{}{}<tagSet>{}</tagSet></placementGroup>",
                xml_escape(&pg.name),
                xml_escape(&pg.strategy),
                xml_escape(&pg.group_id),
                if let Some(p) = pg.partition_count { format!("<partitionCount>{}</partitionCount>", p) } else { String::new() },
                if let Some(s) = pg.spread_level { format!("<spreadLevel>{}</spreadLevel>", s) } else { String::new() },
                ""
            ),
        )
    })
}

fn describe_placement_groups(req: &Request, params: &P) -> Response {
    with_state(req, move |state| {
        // GroupName.1.. list (the sidecar contract accepted both spellings)
        let mut wanted: Vec<String> = params
            .list("GroupName")
            .iter()
            .map(|s| s.to_string())
            .collect();
        for i in 1..=64 {
            if let Some(n) = params.get(&format!("GroupNames.{}", i)) {
                wanted.push(n.to_string());
            } else if i > 1 {
                break;
            }
        }
        let fs = super::handlers::filters(params);
        let mut items = String::new();
        for g in &state.placement_groups {
            if !wanted.is_empty() && !wanted.contains(&g.name) {
                continue;
            }
            let g2 = g.clone();
            if !filter_passes(&fs, move |f| match f {
                "group-name" => vec![g2.name.clone()],
                "state" => vec![g2.state.clone()],
                "strategy" => vec![g2.strategy.clone()],
                other => tag_candidates(&g2.tags, other),
            }) {
                continue;
            }
            let _ = write!(
                items,
                "<item><groupName>{}</groupName><strategy>{}</strategy><state>{}</state><groupId>{}</groupId>{}{}{}</item>",
                xml_escape(&g.name),
                xml_escape(&g.strategy),
                xml_escape(&g.state),
                xml_escape(&g.group_id),
                if let Some(p) = g.partition_count { format!("<partitionCount>{}</partitionCount>", p) } else { String::new() },
                if let Some(s) = g.spread_level { format!("<spreadLevel>{}</spreadLevel>", s) } else { String::new() },
                tags_xml(&g.tags)
            );
        }
        ec2_xml(
            "DescribePlacementGroupsResponse",
            &format!("<placementGroupSet>{}</placementGroupSet>", items),
        )
    })
}

fn delete_placement_group(req: &Request, params: &P) -> Response {
    let name = params.str("GroupName");
    with_state(req, move |state| {
        let pos = state.placement_groups.iter().position(|g| g.name == name);
        match pos {
            Some(p) => {
                state.placement_groups.remove(p);
                ok_xml("DeletePlacementGroupResponse")
            }
            None => ec2_error(
                404,
                "InvalidGroup.NotFound",
                &format!("The placement group '{}' was not found", xml_escape(&name)),
            ),
        }
    })
}

// ---------------------------------------------------------------------------
// Instance types + offerings + spot price history
// ---------------------------------------------------------------------------

mod itt {
    use serde_json::Value;

    static TYPES: std::sync::LazyLock<Vec<Value>> = std::sync::LazyLock::new(|| {
        serde_json::from_slice(include_bytes!("itt/instance-types.json")).unwrap_or_default()
    });

    pub static OFFERINGS: std::sync::LazyLock<Vec<Value>> = std::sync::LazyLock::new(|| {
        serde_json::from_slice(include_bytes!("itt/offerings-us-east-1.json")).unwrap_or_default()
    });

    pub fn all() -> &'static [Value] {
        &TYPES
    }

    pub fn offerings() -> &'static [Value] {
        &OFFERINGS
    }
}

fn describe_instance_types(req: &Request, params: &P) -> Response {
    let fs = super::handlers::filters(params);
    let wanted: Vec<String> = params
        .list("InstanceType")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut items = String::new();
    for row in itt::all() {
        let it = row
            .get("InstanceType")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !wanted.is_empty() && !wanted.contains(&it.to_string()) {
            continue;
        }
        let name = it.to_string();
        let family = name.split('.').next().unwrap_or("").to_string();
        if !super::handlers::filter_passes(&fs, move |f| match f {
            "instance-type" => vec![name.clone()],
            "instance-family" => vec![family.clone()],
            "supported-virtualization-type" => vec!["hvm".to_string(), "paravirtual".to_string()],
            "processor-info.supported-architecture" => vec!["x86_64".to_string()],
            "bare-metal" => vec!["false".to_string()],
            "vcpu-info.valid-cores" => vec![],
            other => Vec::new(),
        }) {
            continue;
        }
        let vcpu = row
            .get("VCpuInfo")
            .and_then(|v| v.get("DefaultVCpus"))
            .and_then(|v| v.as_u64())
            .unwrap_or(2);
        let mem = row
            .get("MemoryInfo")
            .and_then(|v| v.get("SizeInMiB"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1024);
        let _ = write!(
            items,
            "<item><instanceType>{}</instanceType><instanceFamily>{}</instanceFamily><vCpuInfo><defaultVCpu>{}</defaultVCpu><defaultCores>{}</defaultCores><validCores/><threadsPerCore>1</threadsPerCore></vCpuInfo><memoryInfo><sizeInMiB>{}</sizeInMiB></memoryInfo>{}{}</item>",
            xml_escape(it),
            xml_escape(it.split('.').next().unwrap_or("")),
            vcpu,
            (vcpu as f64 / 2.0).ceil() as u64,
            mem,
            String::new(),
            String::new()
        );
    }
    ec2_xml(
        "DescribeInstanceTypesResponse",
        &format!("<instanceTypeSet>{}</instanceTypesSet>", items),
    )
}

fn describe_instance_type_offerings(req: &Request, params: &P) -> Response {
    let fs = super::handlers::filters(params);
    let wanted: Vec<String> = params
        .list("InstanceType")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut items = String::new();
    for row in itt::offerings() {
        let it = row
            .get("InstanceType")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let loc = row
            .get("Location")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !wanted.is_empty() && !wanted.iter().any(|w| w == it) {
            continue;
        }
        let it2 = it.to_string();
        let loc2 = loc.to_string();
        if !super::handlers::filter_passes(&fs, move |f| match f {
            "instance-type" => vec![it2.clone()],
            "location" => vec![loc2.clone()],
            "location-type" => vec!["availability-zone".to_string()],
            _ => Vec::new(),
        }) {
            continue;
        }
        let _ = write!(
            items,
            "<item><instanceType>{}</instanceType><locationType>availability-zone</locationType><location>{}</location></item>",
            xml_escape(it),
            xml_escape(loc)
        );
    }
    ec2_xml(
        "DescribeInstanceTypeOfferingsResponse",
        &format!(
            "<instanceTypeOfferingSet>{}</instanceTypeOfferingsSet>",
            items
        ),
    )
}

fn describe_spot_price_history(req: &Request, params: &P) -> Response {
    let types: Vec<String> = params
        .list("InstanceType")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let zones: Vec<String> = params
        .list("AvailabilityZone")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let products: Vec<String> = params
        .list("ProductDescription")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let fs = super::handlers::filters(params);
    let base = [
        ("t2.micro", "0.011600", "Linux/UNIX"),
        ("t3.micro", "0.010400", "Linux/UNIX"),
        ("m5.large", "0.096000", "Linux/UNIX"),
        ("c5.large", "0.085000", "Linux/UNIX"),
        ("m5.xlarge", "0.192000", "Linux/UNIX"),
        ("r5.large", "0.126000", "Linux/UNIX"),
        ("c6g.large", "0.076100", "Linux/UNIX"),
        ("t2.small", "0.023000", "Linux/UNIX"),
    ];
    let mut items = String::new();
    for (ty, price, prod) in base {
        if !types.is_empty() && !types.contains(&ty.to_string()) {
            continue;
        }
        if !products.is_empty() && !products.contains(&prod.to_string()) {
            continue;
        }
        for az in if zones.is_empty() {
            vec!["us-east-1a".to_string(), "us-east-1b".to_string()]
        } else {
            zones.clone()
        } {
            let ty2 = ty.to_string();
            let az2 = az.clone();
            let price2 = price.to_string();
            let prod2 = prod.to_string();
            if !super::handlers::filter_passes(&fs, move |f| match f {
                "instance-type" => vec![ty2.clone()],
                "availability-zone" => vec![az2.clone()],
                "product-description" => vec![prod2.clone()],
                "spot-price" => vec![price2.clone()],
                _ => Vec::new(),
            }) {
                continue;
            }
            let _ = write!(
                items,
                "<item><spotPrice>{}</spotPrice><instanceType>{}</instanceType><productDescription>{}</productDescription><timestamp>{}</timestamp><availabilityZone>{}</availabilityZone></item>",
                xml_escape(price),
                xml_escape(ty),
                xml_escape(prod),
                super::now_iso(),
                xml_escape(az.as_str())
            );
        }
    }
    ec2_xml(
        "DescribeSpotPriceHistoryResponse",
        &format!("<spotPriceHistorySet>{}</spotPriceHistorySet>", items),
    )
}

fn describe_reserved_instances_offerings(req: &Request, params: &P) -> Response {
    let ty = params.str("InstanceType");
    let prod = params.str("ProductDescription");
    let mut items = String::new();
    const OFFERS: &[(i64, &str)] = &[
        (31536000, "0.012"),
        (94608000, "0.011"),
        (31536000, "0.010"),
    ];
    for (i, (dur, price)) in OFFERS.iter().enumerate() {
        let _ = write!(
            items,
            "<item><offeringType>Partial Upfront</offeringType><instanceType>{}</instanceType><productDescription>Linux/UNIX</productDescription><duration>{}</duration><fixedPrice>{}</fixedPrice><usagePrice>{}</usagePrice><currencyCode>USD</currencyCode><reservedInstancesOfferingId>{:08x}-{:04x}-8000-{:012x}</reservedInstancesOfferingId><pricingDetailsSet/></item>",
            if ty.is_empty() { "t2.micro" } else { ty.as_str() },
            dur,
            price,
            price,
            i as u64,
            i as u32,
            1u64
        );
    }
    let _ = prod;
    let _ = &mut items;
    ec2_xml(
        "DescribeReservedInstancesOfferingsResponse",
        &format!(
            "<reservedInstancesOfferingsSet>{}</reservedInstancesOfferingsSet>",
            items
        ),
    )
}
