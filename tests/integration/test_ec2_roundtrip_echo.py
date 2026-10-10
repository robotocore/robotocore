"""Schema-driven round-trip echo conformance for EC2 resources.

Contract under test: for a parameter the botocore service model exposes BOTH as
an input member of the create operation and as a member of the resource's
output shape, a read-back must reproduce the value the caller sent.

This is the recurring defect class this suite guards: an emulated model stores
only the fields its original author happened to serialize, and every attribute
the provider schema sees but the read-back drops reaches the caller as null —
snapshot-driven IaC tools then see a diff on the next plan (replacement or
in-place churn). The expected-member set derives from botocore instead of a
hand list, so newly declared parameters surface automatically.
"""

from typing import Any

import botocore.session

_SESSION = botocore.session.get_session()

# Input members that no describe response echoes, by AWS design (they take part
# in the call only).
_ECHO_SKIP = {"ClientToken", "TagSpecifications", "DryRun"}

# Describe-op output wrapper: how to walk from the describe output shape to the
# resource shape (member names with "list" meaning "step into the list member").
_WRAPPER = {
    "CreateVpcEndpoint": ("DescribeVpcEndpoints", [("VpcEndpoints", "list")]),
    "CreateVolume": ("DescribeVolumes", [("Volumes", "list")]),
    "CreateSubnet": ("DescribeSubnets", [("Subnets", "list")]),
    "CreateRoute": ("DescribeRouteTables", [("RouteTables", "list"), ("Routes", "list")]),
}


def echoable_members(create_op: str) -> set[str]:
    """Input members of *create_op* that the resource's output shape also carries."""
    model = botocore.session.get_session().get_service_model("ec2")
    inputs = set(model.operation_model(create_op).input_shape.members)
    describe_op, walk = _WRAPPER[create_op]
    shape = model.operation_model(describe_op).output_shape
    for member, kind in walk:
        shape = shape.members[member]
        if kind == "list":
            shape = shape.member
    return inputs & set(shape.members) - _ECHO_SKIP


def test_every_resource_probe_declares_real_echo_members():
    """Guard on the harness itself: if botocore reshapes, the hardcoded probe
    expectations in the class below must be updated, not silently vacuous."""
    assert {"IpAddressType", "ServiceRegion", "PrivateDnsEnabled"} <= echoable_members("CreateVpcEndpoint")
    assert {"CoreNetworkArn", "CarrierGatewayId"} <= echoable_members("CreateRoute")
    assert {"AvailabilityZone", "Encrypted", "Iops", "Size", "VolumeType"} <= echoable_members("CreateVolume")
    assert {"AvailabilityZone", "VpcId", "Ipv6Native"} <= echoable_members("CreateSubnet")


class TestEchobackConformance:
    def test_vpc_endpoint_echoes_settable_members(self, make_boto_client: Any):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.70.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.70.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="echo-probe-sg", Description="echo probe", VpcId=vpc_id
        )["GroupId"]

        request: dict[str, Any] = {
            "VpcId": vpc_id,
            "ServiceName": "com.amazonaws.us-east-1.ssm",
            "VpcEndpointType": "Interface",
            "SubnetIds": [subnet_id],
            "SecurityGroupIds": [sg_id],
            "PrivateDnsEnabled": False,
            "IpAddressType": "dualstack",
            "ServiceRegion": "eu-central-1",
            "DnsOptions": {"DnsRecordIpType": "dualstack"},
        }
        created = ec2.create_vpc_endpoint(**request)["VpcEndpoint"]
        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[created["VpcEndpointId"]])[
            "VpcEndpoints"
        ][0]

        missing = [
            name for name, value in request.items()
            if name in echoable_members("CreateVpcEndpoint") and name not in read
        ]
        assert not missing, (
            f"CreateVpcEndpoint accepted but DescribeVpcEndpoints dropped: {missing} "
            "(snapshot tools see these as null after the write)"
        )
        assert read["IpAddressType"].lower() == "dualstack"  # AWS echoes its own casing
        assert read["ServiceRegion"] == "eu-central-1"
        assert read["DnsOptions"] == {"DnsRecordIpType": "dualstack"}

    def test_volume_echoes_settable_members(self, make_boto_client: Any):
        ec2 = make_boto_client("ec2")
        created = ec2.create_volume(
            AvailabilityZone="us-east-1a", Size=8, VolumeType="gp3", Encrypted=True
        )
        read = next(
            v for v in ec2.describe_volumes(VolumeIds=[created["VolumeId"]])["Volumes"]
        )
        for name in ("AvailabilityZone", "Encrypted", "Size", "VolumeType", "Iops"):
            assert name in read, f"DescribeVolumes dropped {name} after CreateVolume"
        assert read["Encrypted"] is True

    def test_subnet_echoes_settable_members(self, make_boto_client: Any):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.71.0.0/16")["Vpc"]["VpcId"]
        ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.71.4.0/24", AvailabilityZone="us-east-1a"
        )
        read = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["Subnets"][0]
        for name in ("AvailabilityZone", "VpcId", "CidrBlock"):
            assert name in read, f"DescribeSubnets dropped {name} after CreateSubnet"

    def test_route_echoes_core_network_arn(self, make_boto_client: Any):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.72.0.0/16")["Vpc"]["VpcId"]
        route_table_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
        arn = (
            "arn:aws:networkmanager:us-east-1:123456789012:core-network/"
            "core-network-0123456789abcdef0"
        )
        ec2.create_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock="10.72.1.0/24",
            CoreNetworkArn=arn,
        )
        route = next(
            r for r in ec2.describe_route_tables(RouteTableIds=[route_table_id])[
                "RouteTables"
            ][0]["Routes"]
            if r.get("DestinationCidrBlock") == "10.72.1.0/24"
        )
        assert route["CoreNetworkArn"] == arn
