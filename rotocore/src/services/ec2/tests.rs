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

const NS: &str = "http://ec2.amazonaws.com/doc/2016-11-15/";

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
    let r = req(
        "CreateVpc",
        "Action=CreateVpc&CidrBlock=10.77.0.0%2F16",
    );
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
    assert!(!text.contains("<vpc><item>"), "fields must be direct children");

    let r2 = req(
        "DescribeVpcs",
        &format!("Action=DescribeVpcs&VpcId.1={id}"),
    );
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
        &format!(
            "Action=DescribeVpcs&Filter.1.Name=tag%3AName&Filter.1.Value.1=x"
        ),
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
    let text = String::from_utf8(super::dispatch(&req("CreateSecurityGroup", &sg_body)).body).unwrap();
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
    let text = String::from_utf8(super::dispatch(&req("DescribeSecurityGroups", &describe)).body).unwrap();
    assert!(text.contains("<fromPort>443</fromPort>"), "{text}");
}

#[test]
fn default_vpc_exists() {
    let r = req("DescribeVpcs", "Action=DescribeVpcs");
    let text = String::from_utf8(super::dispatch(&r).body).unwrap();
    assert!(text.contains("<isDefault>true</isDefault>"));
}

use super::{ec2_xml, EC2_NS};

#[test]
fn replace_tags_direct() {
    use super::state::Vpc;
    let mut state = Ec2State::default();
    state.vpcs.push(Vpc { id: "vpc-1".into(), ..Default::default() });
    let tags = vec![("Name".to_string(), "x".to_string())];
    let ids = vec!["vpc-1".to_string()];
    handlers::replace_tags(&mut state, &ids, &tags);
    let rows = state.all_tag_rows();
    assert!(rows.iter().any(|(id, _, ts)| id == "vpc-1" && ts.len() == 1), "{rows:?}");
}
