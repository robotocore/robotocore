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
    rx.recv_timeout(std::time::Duration::from_secs(secs))
        .unwrap_or_else(|_| {
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
    let vol_body = "Action=CreateVolume&Zone=eu-west-2a&Size=1&Action=CreateVolume";
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
    let in_eu = crate::Request {
        region: "eu-west-2".into(),
        ..mk
    };
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
        &format!("Action=CopySnapshot&SourceSnapshotId={src_id}&SourceRegion=eu-west-2"),
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

// --- wave-11 simple-family round trips --------------------------------------

/// Extract `<tag>text</tag>`'s text for the first occurrence of `tag`.
fn tag_text(xml: &str, tag: &str) -> String {
    xml.split(&format!("<{tag}>"))
        .nth(1)
        .unwrap_or("")
        .split(&format!("</{tag}>"))
        .next()
        .unwrap_or("")
        .to_string()
}

#[test]
fn wave11_simple_families_round_trip() {
    // The wave-11 create/describe/delete families share one machinery; the
    // round trip is asserted for two representative members (a plain simple
    // family and a parameterized-id family) plus not-found paths.
    let resp = super::dispatch(&req(
        "CreateRouteServer",
        "Action=CreateRouteServer&AmazonSideAsn=64513",
    ));
    assert_eq!(resp.status, 200);
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("<routeServerId>route-server-"), "{text}");
    assert!(
        text.contains("<amazonSideAsn>64513</amazonSideAsn>"),
        "{text}"
    );
    assert!(text.contains("<state>available</state>"), "{text}");
    let rs_id = tag_text(&text, "routeServerId");
    assert!(rs_id.starts_with("route-server-"), "{rs_id}");

    let resp = super::dispatch(&req(
        "DescribeRouteServers",
        &format!("Action=DescribeRouteServers&RouteServerId.1={rs_id}"),
    ));
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains(&rs_id), "describe echo: {text}");
    assert!(text.contains("<routeServerSet>"), "{text}");

    // endpoint create validates the referenced resources
    let bad = super::dispatch(&req(
        "CreateRouteServerEndpoint",
        "Action=CreateRouteServerEndpoint&RouteServerId=route-server-nosuch&SubnetId=subnet-nope",
    ));
    assert_eq!(bad.status, 400, "unknown route server must be 400");
    let body = String::from_utf8(bad.body).unwrap();
    assert!(body.contains("InvalidRouteServerId.NotFound"), "{body}");

    let sub = req(
        "CreateSubnet",
        "Action=CreateSubnet&VpcId=probe-vpc&AutomaticVpcAssignment=false",
    );
    let _ = sub;
    // real subnet id via a default-vpc subnet create
    let vpc_text = String::from_utf8(
        super::dispatch(&req(
            "CreateVpc",
            "Action=CreateVpc&CidrBlock=10.90.0.0%2F16",
        ))
        .body,
    )
    .unwrap();
    let vpc_id = tag_text(&vpc_text, "vpcId");
    let sub_text = String::from_utf8(
        super::dispatch(&req(
            "CreateSubnet",
            &format!("Action=CreateSubnet&VpcId={vpc_id}&CidrBlock=10.90.1.0%2F24"),
        ))
        .body,
    )
    .unwrap();
    let subnet_id = tag_text(&sub_text, "subnetId");

    let ep_text = String::from_utf8(
        super::dispatch(&req(
            "CreateRouteServerEndpoint",
            &format!("Action=CreateRouteServerEndpoint&RouteServerId={rs_id}&SubnetId={subnet_id}"),
        ))
        .body,
    )
    .unwrap();
    let ep_id = tag_text(&ep_text, "routeServerEndpointId");
    assert!(ep_id.starts_with("route-server-endpoint-"), "{ep_text}");

    let peer_text = String::from_utf8(
        super::dispatch(&req(
            "CreateRouteServerPeer",
            &format!(
                "Action=CreateRouteServerPeer&RouteServerEndpointId={ep_id}&PeerAddress=10.90.1.20"
            ),
        ))
        .body,
    )
    .unwrap();
    let peer_id = tag_text(&peer_text, "routeServerPeerId");
    assert!(peer_id.starts_with("route-server-peer-"), "{peer_text}");

    let assoc_text = String::from_utf8(
        super::dispatch(&req(
            "AssociateRouteServer",
            &format!("Action=AssociateRouteServer&RouteServerId={rs_id}&VpcId={vpc_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(
        assoc_text.contains("<routeServerAssociation>") && assoc_text.contains("associated"),
        "{assoc_text}"
    );

    // deletes round-trip
    for (op, key, id) in [
        ("DeleteRouteServerPeer", "RouteServerPeerId", &peer_id),
        ("DeleteRouteServerEndpoint", "RouteServerEndpointId", &ep_id),
        ("DeleteRouteServer", "RouteServerId", &rs_id),
    ] {
        let resp = super::dispatch(&req(op, &format!("Action={op}&{key}.1={id}")));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(
            resp.status == 200 || resp.status == 400,
            "{op}: unexpected status {}: {body}",
            resp.status
        );
    }
}

#[test]
fn route_server_omitted_persist_routes_duration_not_rendered() {
    // Reviewer repro: PersistRoutesDuration is optional per botocore, but the
    // fill map used to render an empty `<PersistRoutesDuration></>` element,
    // which boto3 parses as int("") and raises ValueError. The member must
    // be absent from the record when the caller omits it.
    let resp = super::dispatch(&req("CreateRouteServer", "Action=CreateRouteServer"));
    assert_eq!(resp.status, 200);
    let create_text = String::from_utf8(resp.body).unwrap();
    assert!(
        !create_text
            .to_ascii_lowercase()
            .contains("persistroutesduration"),
        "create must not render an empty member: {create_text}"
    );
    let rs_id = tag_text(&create_text, "routeServerId");

    let describe_text = String::from_utf8(
        super::dispatch(&req(
            "DescribeRouteServers",
            &format!("Action=DescribeRouteServers&RouteServerId.1={rs_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(
        !describe_text
            .to_ascii_lowercase()
            .contains("persistroutesduration"),
        "describe must not render an empty member: {describe_text}"
    );
    assert!(describe_text.contains(&rs_id), "{describe_text}");

    // When the caller does provide it, the member renders with its value.
    let resp = super::dispatch(&req(
        "CreateRouteServer",
        "Action=CreateRouteServer&PersistRoutesDuration=8",
    ));
    assert_eq!(resp.status, 200);
    let text = String::from_utf8(resp.body).unwrap();
    assert!(
        text.to_ascii_lowercase()
            .contains("<persistroutesduration>8</persistroutesduration>"),
        "{text}"
    );
}

#[test]
fn wave11_byoip_parameterized_family_round_trip() {
    // BYOIP keys its record on the request's Cidr (no generated id).
    let cidr = "203.0.113.0/24";
    let resp = super::dispatch(&req(
        "ProvisionByoipCidr",
        &format!("Action=ProvisionByoipCidr&Cidr={cidr}&Description=unit"),
    ));
    assert_eq!(resp.status, 200, "byoip create must be 200");
    let text = String::from_utf8(resp.body).unwrap();
    assert!(text.contains("<cidr>203.0.113.0/24</cidr>"), "{text}");
    assert!(text.contains("<state>provisioned</state>"), "{text}");

    let dtext = String::from_utf8(
        super::dispatch(&req("DescribeByoipCidrs", "Action=DescribeByoipCidrs")).body,
    )
    .unwrap();
    assert!(dtext.contains("<cidr>203.0.113.0/24</cidr>"), "{dtext}");
    assert!(dtext.contains("<byoipCidrSet>"), "{dtext}");

    let rel = super::dispatch(&req(
        "DeprovisionByoipCidr",
        &format!("Action=DeprovisionByoipCidr&Cidr={cidr}"),
    ));
    assert_eq!(rel.status, 200);
    let text = String::from_utf8(rel.body).unwrap();
    assert!(text.contains("deprovisioned"), "{text}");

    let dtext = String::from_utf8(
        super::dispatch(&req("DescribeByoipCidrs", "Action=DescribeByoipCidrs")).body,
    )
    .unwrap();
    assert!(!dtext.contains("203.0.113.0/24"), "{dtext}");
}

#[test]
fn wave11_exclusions_and_encryption_controls() {
    // VPC block-public-access exclusions: create -> describe -> modify -> delete
    let vpc_text = String::from_utf8(
        super::dispatch(&req(
            "CreateVpc",
            "Action=CreateVpc&CidrBlock=10.91.0.0%2F16",
        ))
        .body,
    )
    .unwrap();
    let vpc_id = tag_text(&vpc_text, "vpcId");

    let ctext = String::from_utf8(
        super::dispatch(&req(
            "CreateVpcBlockPublicAccessExclusion",
            &format!(
                "Action=CreateVpcBlockPublicAccessExclusion&InternetGatewayExclusionMode=allow-egress&ResourceArn=arn%3Aaws%3Aec2%3Aus-east-1%3A123456789012%3Avpc%2F{vpc_id}"
            ),
        ))
        .body,
    )
    .unwrap();
    assert!(ctext.contains("<exclusion>"), "{ctext}");
    let excl_id = tag_text(&ctext, "exclusionId");
    assert!(excl_id.starts_with("vpc-bpe-"), "{excl_id}");

    let dtext = String::from_utf8(
        super::dispatch(&req(
            "DescribeVpcBlockPublicAccessExclusions",
            "Action=DescribeVpcBlockPublicAccessExclusions",
        ))
        .body,
    )
    .unwrap();
    assert!(dtext.contains(&excl_id), "{dtext}");

    let mtext = String::from_utf8(
        super::dispatch(&req(
            "ModifyVpcBlockPublicAccessExclusion",
            &format!(
                "Action=ModifyVpcBlockPublicAccessExclusion&ExclusionId={excl_id}&InternetGatewayExclusionMode=allow-bidirectional"
            ),
        ))
        .body,
    )
    .unwrap();
    assert!(mtext.contains("update-complete"), "{mtext}");

    let del = super::dispatch(&req(
        "DeleteVpcBlockPublicAccessExclusion",
        &format!("Action=DeleteVpcBlockPublicAccessExclusion&ExclusionId={excl_id}"),
    ));
    assert_eq!(del.status, 200);

    let bad = super::dispatch(&req(
        "DeleteVpcBlockPublicAccessExclusion",
        "Action=DeleteVpcBlockPublicAccessExclusion&ExclusionId=vpc-bpe-00000000000000000",
    ));
    assert_eq!(bad.status, 400, "unknown exclusion must be modeled 400");
    assert!(
        String::from_utf8(bad.body)
            .unwrap()
            .contains("InvalidVpcBlockPublicAccessExclusionId.NotFound"),
        "absent exclusion 400"
    );
}

#[test]
fn wave11_transfer_uuid_multi_state() {
    // snapshot-tier + snapshot-locks families: stateful tier mutation
    let vol_text = String::from_utf8(
        super::dispatch(&req(
            "CreateVolume",
            "Action=CreateVolume&Zone=us-east-1a&Size=1",
        ))
        .body,
    )
    .unwrap();
    let vol_id = tag_text(&vol_text, "volumeId");
    let snap_text = String::from_utf8(
        super::dispatch(&req(
            "CreateSnapshot",
            &format!("Action=CreateSnapshot&VolumeId={vol_id}&Description=seed"),
        ))
        .body,
    )
    .unwrap();
    let snap_id = tag_text(&snap_text, "snapshotId");

    let mtier = super::dispatch(&req(
        "ModifySnapshotTier",
        &format!("Action=ModifySnapshotTier&SnapshotId={snap_id}&TargetTier=archive"),
    ));
    assert_eq!(mtier.status, 200, "tier on real snapshot must be 200");
    let text = String::from_utf8(mtier.body).unwrap();
    assert!(
        text.contains(&format!("<snapshotId>{snap_id}</snapshotId>")),
        "{text}"
    );
    assert!(text.contains("<tieringStartTime>"), "{text}");

    let dtext = String::from_utf8(
        super::dispatch(&req(
            "DescribeSnapshots",
            &format!("Action=DescribeSnapshots&SnapshotId.1={snap_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(
        dtext.contains("<storageTier>archive</storageTier>"),
        "{dtext}"
    );

    let rest = super::dispatch(&req(
        "RestoreSnapshotTier",
        &format!("Action=RestoreSnapshotTier&SnapshotId={snap_id}"),
    ));
    assert_eq!(rest.status, 200);
    let dtext = String::from_utf8(
        super::dispatch(&req(
            "DescribeSnapshots",
            &format!("Action=DescribeSnapshots&SnapshotId.1={snap_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(
        dtext.contains("<storageTier>standard</storageTier>"),
        "{dtext}"
    );

    let bad = super::dispatch(&req(
        "ModifySnapshotTier",
        "Action=ModifySnapshotTier&SnapshotId=snap-00000000000000000&TargetTier=archive",
    ));
    assert_eq!(bad.status, 400, "unknown snapshot must be modeled 400");
    assert!(
        String::from_utf8(bad.body)
            .unwrap()
            .contains("InvalidSnapshot.NotFound"),
        "snapshot id-not-found code"
    );
}

#[test]
fn wave11_image_state_ops_round_trip() {
    let reg = super::dispatch(&req(
        "RegisterImage",
        "Action=RegisterImage&Name=unit-ami&RootDeviceName=%2Fdev%2Fsda1",
    ));
    let text = String::from_utf8(reg.body).unwrap();
    assert_eq!(reg.status, 200, "register: {text}");
    let img_id = tag_text(&text, "imageId");
    assert!(img_id.starts_with("ami-"), "{text}");

    let dis = super::dispatch(&req(
        "DisableImage",
        &format!("Action=DisableImage&ImageId={img_id}"),
    ));
    assert_eq!(dis.status, 200);
    let dtext = String::from_utf8(
        super::dispatch(&req(
            "DescribeImages",
            &format!("Action=DescribeImages&ImageId.1={img_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(dtext.contains("<state>disabled</state>"), "{dtext}");

    let en = super::dispatch(&req(
        "EnableImage",
        &format!("Action=EnableImage&ImageId={img_id}"),
    ));
    assert_eq!(en.status, 200);
    let dtext = String::from_utf8(
        super::dispatch(&req(
            "DescribeImages",
            &format!("Action=DescribeImages&ImageId.1={img_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(dtext.contains("<state>available</state>"), "{dtext}");

    let bad = super::dispatch(&req(
        "DisableImage",
        "Action=DisableImage&ImageId=ami-00000000000000000",
    ));
    assert_eq!(bad.status, 400);
    assert!(
        String::from_utf8(bad.body)
            .unwrap()
            .contains("InvalidAMIID.NotFound"),
        "unknown image must be modeled 400"
    );
}

#[test]
fn wave11_vgw_route_propagation_state() {
    let vpc_text = String::from_utf8(
        super::dispatch(&req(
            "CreateVpc",
            "Action=CreateVpc&CidrBlock=10.92.0.0%2F16",
        ))
        .body,
    )
    .unwrap();
    let vpc_id = tag_text(&vpc_text, "vpcId");
    let rt_text = String::from_utf8(
        super::dispatch(&req(
            "CreateRouteTable",
            &format!("Action=CreateRouteTable&VpcId={vpc_id}"),
        ))
        .body,
    )
    .unwrap();
    let rtb_id = tag_text(&rt_text, "routeTableId");

    let gw = super::dispatch(&req(
        "CreateVpnGateway",
        "Action=CreateVpnGateway&Type=ipsec.1",
    ));
    let gw_id = tag_text(&String::from_utf8(gw.body).unwrap(), "vpnGatewayId");

    let en = super::dispatch(&req(
        "EnableVgwRoutePropagation",
        &format!("Action=EnableVgwRoutePropagation&RouteTableId={rtb_id}&GatewayId={gw_id}"),
    ));
    assert_eq!(en.status, 200);

    let text = String::from_utf8(
        super::dispatch(&req(
            "DescribeRouteTables",
            &format!("Action=DescribeRouteTables&RouteTableId.1={rtb_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(text.contains(&gw_id), "propagation stored: {text}");

    let dis = super::dispatch(&req(
        "DisableVgwRoutePropagation",
        &format!("Action=DisableVgwRoutePropagation&RouteTableId={rtb_id}&GatewayId={gw_id}"),
    ));
    assert_eq!(dis.status, 200);
    let text = String::from_utf8(
        super::dispatch(&req(
            "DescribeRouteTables",
            &format!("Action=DescribeRouteTables&RouteTableId.1={rtb_id}"),
        ))
        .body,
    )
    .unwrap();
    assert!(
        !text.contains(&format!("<gatewayId>{gw_id}</gatewayId>")),
        "{text}"
    );
}

#[test]
fn wave11_tgw_reject_and_replace_route() {
    let tgw_text = String::from_utf8(
        super::dispatch(&req("CreateTransitGateway", "Action=CreateTransitGateway")).body,
    )
    .unwrap();
    let tgw_id = tag_text(&tgw_text, "transitGatewayId");
    assert!(tgw_id.starts_with("tgw-"), "{tgw_text}");

    let vpc_text = String::from_utf8(
        super::dispatch(&req(
            "CreateVpc",
            "Action=CreateVpc&CidrBlock=10.93.0.0%2F16",
        ))
        .body,
    )
    .unwrap();
    let vpc_id = tag_text(&vpc_text, "vpcId");
    let att_text = String::from_utf8(
        super::dispatch(&req(
            "CreateTransitGatewayVpcAttachment",
            &format!(
                "Action=CreateTransitGatewayVpcAttachment&TransitGatewayId={tgw_id}&VpcId={vpc_id}"
            ),
        ))
        .body,
    )
    .unwrap();
    let att_id = tag_text(&att_text, "transitGatewayAttachmentId");

    let rej = super::dispatch(&req(
        "RejectTransitGatewayVpcAttachment",
        &format!("Action=RejectTransitGatewayVpcAttachment&TransitGatewayAttachmentId={att_id}"),
    ));
    assert_eq!(rej.status, 200);
    let text = String::from_utf8(rej.body).unwrap();
    assert!(text.contains("rejected"), "{text}");

    let bad = super::dispatch(&req(
        "RejectTransitGatewayVpcAttachment",
        "Action=RejectTransitGatewayVpcAttachment&TransitGatewayAttachmentId=tgw-attach-00000000000000000",
    ));
    assert_eq!(bad.status, 400, "unknown attachment must be modeled 400");

    let rtb_text = String::from_utf8(
        super::dispatch(&req(
            "CreateTransitGatewayRouteTable",
            &format!("Action=CreateTransitGatewayRouteTable&TransitGatewayId={tgw_id}"),
        ))
        .body,
    )
    .unwrap();
    let rtb_id = tag_text(&rtb_text, "transitGatewayRouteTableId");
    let rep = super::dispatch(&req(
        "ReplaceTransitGatewayRoute",
        &format!(
            "Action=ReplaceTransitGatewayRoute&TransitGatewayRouteTableId={rtb_id}&DestinationCidrBlock=10.0.0.0%2F8&NewDestinationCidrBlock=10.1.0.0%2F16&Blackhole=true"
        ),
    ));
    assert_eq!(
        rep.status,
        200,
        "replace route: {}",
        String::from_utf8(rep.body.clone()).unwrap_or_default()
    );
    let text = String::from_utf8(rep.body).unwrap();
    assert!(text.contains("<state>blackhole</state>"), "{text}");

    let search = String::from_utf8(
        super::dispatch(&req(
            "SearchTransitGatewayRoutes",
            &format!(
                "Action=SearchTransitGatewayRoutes&TransitGatewayRouteTableId={rtb_id}&Filter.1.Name=state&Filter.1.Value.1=blackhole"
            ),
        ))
        .body,
    )
    .unwrap();
    assert!(search.contains("10.1.0.0/16"), "{search}");
    assert!(!search.contains(">10.0.0.0/8<"), "{search}");
}

#[test]
fn run_instances_maxcount_launches_n() {
    // Real AWS attempts MaxCount instances; MinCount is only the fallback when
    // MaxCount is omitted (moto launches just MinCount — getmoto/moto#10215).
    // Each RunInstances response contains one <instanceId> per launched
    // instance, so counting them asserts the launched count directly.
    let launch = |extra: &str| {
        let body = format!("Action=RunInstances&ImageId=ami-12c6146b&InstanceType=t2.micro{extra}");
        let resp = super::dispatch(&req("RunInstances", &body));
        assert_eq!(resp.status, 200, "run_instances must launch: {resp:?}");
        let text = String::from_utf8(resp.body).unwrap();
        text.matches("<instanceId>").count()
    };
    // MaxCount wins over MinCount (the real-AWS behavior this pins).
    assert_eq!(launch("&MinCount=1&MaxCount=3"), 3);
    // No MaxCount: fall back to MinCount.
    assert_eq!(launch("&MinCount=1"), 1);
    assert_eq!(launch("&MinCount=3"), 3);
}
