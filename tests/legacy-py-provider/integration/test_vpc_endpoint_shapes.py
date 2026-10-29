"""Integration tests for `aws_vpc_endpoint` read-back fidelity.

AWS's DescribeVpcEndpoints response carries IpAddressType and DnsOptions
(DnsRecordIpType) for interface endpoints; gateway endpoints carry neither.
Emulators that leave those fields out make snapshot-driven IaC tools see the
attributes as unknown after every write, which forces endpoint replacement on
the next plan.

Covers:
1. Interface endpoint: default IpAddressType/DnsOptions and an explicit
   dualstack create round-trip through both CreateVpcEndpoint and
   DescribeVpcEndpoints.
2. Gateway endpoint: IpAddressType/DnsOptions absent, RouteTableIds present.
3. ModifyVpcEndpoint accepts IpAddressType/DnsOptions.
4. A security group deleted while still referenced by an endpoint no longer
   crashes DescribeVpcEndpoints.
"""


class TestInterfaceEndpointComputedFields:
    """CreateVpcEndpoint/DescribeVpcEndpoints must report IpAddressType and
    DnsOptions for interface endpoints."""

    def test_default_interface_endpoint_reports_ipv4(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="endpoint-probe", Description="endpoint probe", VpcId=vpc_id
        )["GroupId"]

        created = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.ssm",
            VpcEndpointType="Interface",
            SubnetIds=[subnet_id],
            SecurityGroupIds=[sg_id],
        )["VpcEndpoint"]
        assert created["IpAddressType"] == "IPv4"
        assert created["DnsOptions"] == {"DnsRecordIpType": "ipv4"}

        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[created["VpcEndpointId"]])[
            "VpcEndpoints"
        ][0]
        assert read["IpAddressType"] == "IPv4"
        assert read["DnsOptions"] == {"DnsRecordIpType": "ipv4"}

    def test_dualstack_interface_endpoint_round_trips(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="endpoint-probe2", Description="endpoint probe", VpcId=vpc_id
        )["GroupId"]

        created = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.ssm",
            VpcEndpointType="Interface",
            SubnetIds=[subnet_id],
            SecurityGroupIds=[sg_id],
            IpAddressType="dualstack",
            DnsOptions={"DnsRecordIpType": "dualstack"},
            ServiceRegion="eu-central-1",
        )["VpcEndpoint"]
        assert created["IpAddressType"] == "Dualstack"
        assert created["DnsOptions"] == {"DnsRecordIpType": "dualstack"}
        assert created["ServiceRegion"] == "eu-central-1"

        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[created["VpcEndpointId"]])[
            "VpcEndpoints"
        ][0]
        assert read["IpAddressType"] == "Dualstack"
        assert read["DnsOptions"] == {"DnsRecordIpType": "dualstack"}
        assert read["ServiceRegion"] == "eu-central-1"

    def test_service_region_defaults_to_the_endpoint_region(self, make_boto_client):
        """Without an explicit service_region the read-back reports the endpoint's own
        region, so a later plan does not flip the attribute to null and force replacement."""
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="endpoint-region-probe", Description="endpoint probe", VpcId=vpc_id
        )["GroupId"]

        created = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.ssm",
            VpcEndpointType="Interface",
            SubnetIds=[subnet_id],
            SecurityGroupIds=[sg_id],
        )["VpcEndpoint"]
        assert created["ServiceRegion"] == "us-east-1"
        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[created["VpcEndpointId"]])[
            "VpcEndpoints"
        ][0]
        assert read["ServiceRegion"] == "us-east-1"


class TestGatewayEndpointShape:
    """Gateway endpoints carry no IpAddressType/DnsOptions, matching AWS."""

    def test_gateway_endpoint_reports_route_tables_only(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        route_table_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

        created = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.s3",
            RouteTableIds=[route_table_id],
        )["VpcEndpoint"]
        assert created["VpcEndpointType"] == "Gateway"
        assert "IpAddressType" not in created
        assert "DnsOptions" not in created

        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[created["VpcEndpointId"]])[
            "VpcEndpoints"
        ][0]
        assert read["RouteTableIds"] == [route_table_id]
        assert "IpAddressType" not in read
        assert "DnsOptions" not in read


class TestModifyEndpointIpActionType:
    """ModifyVpcEndpoint accepts IpAddressType and DnsOptions."""

    def test_modify_updates_ip_address_type(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="endpoint-probe3", Description="endpoint probe", VpcId=vpc_id
        )["GroupId"]
        endpoint_id = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.ssm",
            VpcEndpointType="Interface",
            SubnetIds=[subnet_id],
            SecurityGroupIds=[sg_id],
        )["VpcEndpoint"]["VpcEndpointId"]

        ec2.modify_vpc_endpoint(
            VpcEndpointId=endpoint_id,
            IpAddressType="dualstack",
            DnsOptions={"DnsRecordIpType": "dualstack"},
        )
        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])["VpcEndpoints"][0]
        assert read["IpAddressType"] == "Dualstack"
        assert read["DnsOptions"] == {"DnsRecordIpType": "dualstack"}


class TestEndpointWithDeletedSecurityGroup:
    """A deleted SG referenced by an endpoint must not break the read-back.

    Moto previously dereferenced the missing security group and raised
    AttributeError inside DescribeVpcEndpoints serialization.
    """

    def test_describe_survives_deleted_security_group(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="endpoint-doomed-sg", Description="endpoint probe", VpcId=vpc_id
        )["GroupId"]
        endpoint_id = ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName="com.amazonaws.us-east-1.ssm",
            VpcEndpointType="Interface",
            SubnetIds=[subnet_id],
            SecurityGroupIds=[sg_id],
        )["VpcEndpoint"]["VpcEndpointId"]

        ec2.delete_security_group(GroupId=sg_id)

        read = ec2.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])["VpcEndpoints"][0]
        assert read["VpcEndpointId"] == endpoint_id
        assert read.get("Groups", []) == []
