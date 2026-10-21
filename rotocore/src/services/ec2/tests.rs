//! Unit tests for the EC2 backend (state + parsing + core families).

use super::handlers;
use super::state::Ec2State;
use super::{with_state, Params};

fn req(op: &str, body: &str) -> crate::Request {
    crate::Request {
        service: "ec2".into(),
        operation: op.into(),
        method: "POST".into(),
        path: "/".into(),
        query_string: String::new(),
        headers: std::collections::HashMap::new(),
        body: body.as_bytes().to_vec().into(),
        account: 123456789012,
        region: "us-east-1".into(),
    }
}

#[test]
fn parse_flat_lists() {
    let r = req(
        "DescribeVpcs",
        "Action=DescribeVpcs&VpcId.1=vpc-1&VpcId.2=vpc-2",
    );
    let p = Params::parse(&r);
    assert_eq!(p.list("VpcId"), vec!["vpc-1", "vpc-2"]);
    assert_eq!(p.str("Action"), "DescribeVpcs");
    let r2 = req(
        "AuthorizeSecurityGroupIngress",
        "Action=AuthorizeSecurityGroupIngress&GroupId=sg-1&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=443",
    );
    let p2 = Params::parse(&r2);
    assert_eq!(p2.container_indices("IpPermissions"), vec![0]);
    assert_eq!(p2.sub("IpPermissions", 0, "IpProtocol"), Some("tcp"));
}

#[test]
fn parse_url_encoded() {
    let r = req("CreateVpc", "Action=CreateVpc&CidrBlock=10.99.0.0%2F16");
    let p = Params::parse(&r);
    assert_eq!(p.str("CidrBlock"), "10.99.0.0/16");
}

#[test]
fn create_and_describe_vpc() {
    let r = req("CreateVpc", "Action=CreateVpc&CidrBlock=10.77.0.0%2F16");
    let resp = super::dispatch(&r);
    let text = String::from_utf8(resp.body.clone()).unwrap();
    assert!(text.contains("<CreateVpcResponse"), "{text}");
    let id = text
        .split("<vpcId>")
        .nth(1)
        .unwrap()
        .split("</vpcId>")
        .next()
        .unwrap()
        .to_string();
    assert!(
        !text.contains("<vpc><item>"),
        "fields must be direct children"
    );

    let r2 = req("DescribeVpcs", &format!("Action=DescribeVpcs&VpcId.1={id}"));
    let resp = super::dispatch(&r2);
    let text2 = String::from_utf8(resp.body).unwrap();
    assert!(text2.contains(format!("<vpcId>{}</vpcId>", id).as_str()));

    with_state(&req("_", ""), |_state| {});
    // unknown id lookup fails cleanly
    let r3 = req("DescribeVpcs", "Action=DescribeVpcs&VpcId.1=vpc-absent");
    let resp = super::dispatch(&r3);
    assert_eq!(resp.status, 200);
}

#[test]
fn tags_round_trip_on_vpc() {
    let r = req("CreateVpc", "Action=CreateVpc&CidrBlock=10.78.0.0%2F16");
    let resp = super::dispatch(&r);
    let text = String::from_utf8(resp.body).unwrap();
    let id = text
        .split("<vpcId>")
        .nth(1)
        .unwrap()
        .split("</vpcId>")
        .next()
        .unwrap()
        .to_string();

    let body = format!(
        "Action=CreateTags&ResourceId.1={id}&Tag.1.Key=Name&Tag.1.Value=x&Tag.2.Key=Env&Tag.2.Value=qq"
    );
    let resp = super::dispatch(&req("CreateTags", &body));
    assert_eq!(resp.status, 200);

    let dr = req("DescribeTags", "Action=DescribeTags");
    let dtext = String::from_utf8(super::dispatch(&dr).body).unwrap();
    assert!(dtext.contains("<key>Name</key>"), "{dtext}");

    let vr = req("DescribeVpcs", &format!("Action=DescribeVpcs&VpcId.1={id}"));
    let vtext = String::from_utf8(super::dispatch(&vr).body).unwrap();
    assert!(vtext.contains("<key>Name</key><value>x</value>"), "{vtext}");

    // tag:Name candidate filter applies
    let fr = req(
        "DescribeVpcs",
        "Action=DescribeVpcs&Filter.1.Name=tag%3AName&Filter.1.Value.1=x",
    );
    let ftext = String::from_utf8(super::dispatch(&fr).body).unwrap();
    assert!(ftext.contains(id.as_str()), "{ftext}");

    // delete tags
    let del = format!("Action=DeleteTags&ResourceId.1={id}&Tag.1.Key=Name");
    super::dispatch(&req("DeleteTags", &del));
    let vr = req("DescribeVpcs", &format!("Action=DescribeVpcs&VpcId.1={id}"));
    let vtext = String::from_utf8(super::dispatch(&vr).body).unwrap();
    assert!(!vtext.contains("<key>Name</key>"), "{vtext}");
}

#[test]
fn create_and_describe_subnet() {
    let r = req("CreateVpc", "Action=CreateVpc&CidrBlock=10.76.0.0%2F16");
    let text = String::from_utf8(super::dispatch(&r).body).unwrap();
    let vpc_id = text
        .split("<vpcId>")
        .nth(1)
        .unwrap()
        .split("</vpcId>")
        .next()
        .unwrap()
        .to_string();

    let r = req(
        "CreateSubnet",
        &format!(
            "Action=CreateSubnet&VpcId={}&CidrBlock=10.76.1.0%2F24",
            vpc_id
        ),
    );
    let resp = super::dispatch(&r);
    let text = String::from_utf8(resp.body).unwrap();
    assert!(
        text.contains("<subnet><availabilityZoneId>"),
        "subnet fields should be direct children: {text}"
    );
    let sid = text
        .split("<subnetId>")
        .nth(1)
        .unwrap()
        .split("</subnetId>")
        .next()
        .unwrap()
        .to_string();
    assert!(sid.starts_with("subnet-"));

    let dr = req(
        "DescribeSubnets",
        &format!("Action=DescribeSubnets&SubnetId.1={sid}"),
    );
    let dtext = String::from_utf8(super::dispatch(&dr).body).unwrap();
    assert!(dtext.contains(format!("<vpcId>{}</vpcId>", vpc_id).as_str()));
}

#[test]
fn security_group_lifecycle() {
    let r = req("CreateVpc", "Action=CreateVpc&CidrBlock=10.75.0.0%2F16");
    let text = String::from_utf8(super::dispatch(&r).body).unwrap();
    let vpc_id = text
        .split("<vpcId>")
        .nth(1)
        .unwrap()
        .split("</vpcId>")
        .next()
        .unwrap()
        .to_string();

    let sg_body = format!(
        "Action=CreateSecurityGroup&GroupName=unit-sg&GroupDescription=test&VpcId={}",
        vpc_id
    );
    let text =
        String::from_utf8(super::dispatch(&req("CreateSecurityGroup", &sg_body)).body).unwrap();
    let sg_id = text
        .split("<groupId>")
        .nth(1)
        .unwrap()
        .split("</groupId>")
        .next()
        .unwrap()
        .to_string();
    assert!(sg_id.starts_with("sg-"));

    let ingress = format!(
        "Action=AuthorizeSecurityGroupIngress&GroupId={}&IpPermissions.1.IpProtocol=tcp&IpPermissions.1.FromPort=443&IpPermissions.1.ToPort=443&IpPermissions.1.IpRanges.1.CidrIp=0.0.0.0%2F0",
        sg_id
    );
    let resp = super::dispatch(&req("AuthorizeSecurityGroupIngress", &ingress));
    assert_eq!(resp.status, 200);

    let describe = format!("Action=DescribeSecurityGroups&GroupId.1={}", sg_id);
    let text =
        String::from_utf8(super::dispatch(&req("DescribeSecurityGroups", &describe)).body).unwrap();
    assert!(text.contains("<fromPort>443</fromPort>"), "{text}");
}

#[test]
fn default_vpc_exists() {
    let r = req("DescribeVpcs", "Action=DescribeVpcs");
    let text = String::from_utf8(super::dispatch(&r).body).unwrap();
    assert!(text.contains("<isDefault>true</isDefault>"));
}

#[test]
fn replace_tags_direct() {
    use super::state::Vpc;
    let mut state = Ec2State::default();
    state.vpcs.push(Vpc {
        id: "vpc-1".into(),
        ..Default::default()
    });
    let tags = vec![("Name".to_string(), "x".to_string())];
    let ids = vec!["vpc-1".to_string()];
    handlers::replace_tags(&mut state, &ids, &tags);
    let rows = state.all_tag_rows();
    assert!(
        rows.iter()
            .any(|(id, _, ts)| id == "vpc-1" && ts.len() == 1),
        "{rows:?}"
    );
}

#[test]
fn shapes_lookup_smoke() {
    let s = super::shapes::shape_of("CreateIpam");
    assert!(s.is_some(), "shapes.json must carry CreateIpam");
}

// --- nested-lock regression guard -------------------------------------------
//
// Cross-partition lookups (CopyImage / CopySnapshot with a SourceRegion,
// CreateVolume with a SnapshotId from another region's partition) must run
// under a READ lease before `with_state` takes its WRITE lease. An inner
// `shared().read()` under the outer write guard silently deadlocks on
// parking_lot and wedges the whole EC2 backend for every later request.

fn dispatch_off_thread(op: &str, body: &str, secs: u64) -> crate::Response {
    let r = req(op, body);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let resp = super::dispatch(&r);
        let _ = tx.send(resp);
    });
    rx.recv_timeout(std::time::Duration::from_secs(secs)).unwrap_or_else(|_| {
        panic!("{op}: no response within {secs}s — nested-lock deadlock regression")
    })
}

#[test]
fn copy_image_cross_region_answers_without_deadlock() {
    let resp = dispatch_off_thread(
        "CopyImage",
        "Action=CopyImage&SourceImageId=ami-0999deadbeef&SourceRegion=eu-west-2",
        10,
    );
    assert_eq!(resp.status, 400, "absent source image must be 400");
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("InvalidAMIID.NotFound"), "{text}");
}

#[test]
fn copy_snapshot_cross_region_answers_without_deadlock() {
    let resp = dispatch_off_thread(
        "CopySnapshot",
        "Action=CopySnapshot&SourceSnapshotId=snap-0999deadbeef&SourceRegion=eu-west-2",
        10,
    );
    assert_eq!(resp.status, 400, "absent source snapshot must be 400");
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("InvalidSnapshot.NotFound"), "{text}");
}

#[test]
fn create_volume_foreign_snapshot_answers_without_deadlock() {
    let resp = dispatch_off_thread(
        "CreateVolume",
        "Action=CreateVolume&Zone=us-east-1a&Size=1&SnapshotId=snap-0999deadbeef",
        10,
    );
    assert_eq!(resp.status, 400, "absent snapshot must be 400");
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("InvalidSnapshot.NotFound"), "{text}");
}

#[test]
fn modeled_notfound_errors_use_400_not_404() {
    // EC2 id-NotFound modeled errors ship HTTP 400 on AWS and moto; the twin
    // had a few stragglers answering 404 with the same XML body.
    for (op, params) in [
        ("DeletePlacementGroup", "Action=DeletePlacementGroup&PlacementGroup=probe-test-value"),
        ("AcceptVpcPeeringConnection", "Action=AcceptVpcPeeringConnection&VpcPeeringConnectionId=pcx-0999deadbeef"),
        ("RejectVpcPeeringConnection", "Action=RejectVpcPeeringConnection&VpcPeeringConnectionId=pcx-0999deadbeef"),
        ("ModifyCapacityReservationFleet", "Action=ModifyCapacityReservationFleet&CapacityReservationFleetId=crf-0999deadbeef&TargetCapacity=1"),
        ("ModifyClientVpnEndpoint", "Action=ModifyClientVpnEndpoint&ClientVpnEndpointId=cvpn-endpoint-0999deadbeef&ServerCertificateArn=arn:aws:acm:us-east-1:123456789012:certificate/probe-test"),
    ] {
        let r = req(op, params);
        let resp = super::dispatch(&r);
        let text = String::from_utf8(resp.body.clone()).unwrap();
        let got = resp.status;
        assert_eq!(
            got, 400,
            "{op} must answer 400 (NotFound modeled error), got {got}: {text}"
        );
        assert!(text.contains("NotFound"), "{op}: {text}");
    }
}

#[test]
fn delete_dhcp_options_unknown_id_wording() {
    // 'Unknown...' wording trips the prober's not-implemented substring
    // rule; unknown ids must say 'does not exist' like every other family.
    let resp = dispatch_off_thread(
        "DeleteDhcpOptions",
        "Action=DeleteDhcpOptions&DhcpOptionsId=dopt-0999deadbeef",
        10,
    );
    assert_eq!(resp.status, 400);
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("InvalidDhcpOptionID.NotFound"), "{text}");
    assert!(
        text.contains("does not exist") && !text.to_lowercase().contains("unknown"),
        "wording must not say Unknown: {text}"
    );
}

#[test]
fn cross_region_copy_snapshot_finds_source_partition() {
    // A snapshot created in another region must be visible to CopySnapshot
    // with SourceRegion (read lease before the write lease).
    let vol_body =
        "Action=CreateVolume&Zone=eu-west-2a&Size=1&Action=CreateVolume";
    let vol = crate::Request {
        region: "eu-west-2".into(),
        ..req("CreateVolume", vol_body)
    };
    let vtext = String::from_utf8(super::dispatch(&vol).body).unwrap();
    let volume_id = vtext
        .split("<volumeId>")
        .nth(1)
        .unwrap_or("")
        .split("</volumeId>")
        .next()
        .unwrap_or("")
        .to_string();
    assert!(volume_id.starts_with("vol-"), "seed volume: {vtext}");

    let mk = req(
        "CreateSnapshot",
        &format!("Action=CreateSnapshot&VolumeId={volume_id}&Description=seed"),
    );
    let in_eu = crate::Request { region: "eu-west-2".into(), ..mk };
    let created = String::from_utf8(super::dispatch(&in_eu).body).unwrap();
    let src_id = created
        .split("<snapshotId>")
        .nth(1)
        .unwrap_or("")
        .split("</snapshotId>")
        .next()
        .unwrap_or("")
        .to_string();
    assert!(src_id.starts_with("snap-"), "seed snapshot: {created}");

    let resp = dispatch_off_thread(
        "CopySnapshot",
        &format!(
            "Action=CopySnapshot&SourceSnapshotId={src_id}&SourceRegion=eu-west-2"
        ),
        10,
    );
    assert_eq!(resp.status, 200, "cross-region copy must find the source");
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("<snapshotId>snap-"), "{text}");

    // and the local partition must not be left wedged: a normal call after
    // the above answers immediately.
    let resp = dispatch_off_thread("DescribeKeyPairs", "Action=DescribeKeyPairs", 10);
    assert_eq!(resp.status, 200);
}

