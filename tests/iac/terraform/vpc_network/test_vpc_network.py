"""IaC test: terraform - vpc_network.

Validates VPC, subnets, and security group creation.
Resources are created via boto3 (mirroring the Terraform program).
"""

from __future__ import annotations

import pytest

from tests.iac.helpers.resource_validator import assert_vpc_exists

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def vpc_resources(ec2_client):
    """Create VPC, subnets, IGW, route table, and security group via boto3."""
    # VPC
    vpc = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2_client.create_tags(Resources=[vpc_id], Tags=[{"Key": "Name", "Value": "tf-app-vpc"}])

    # Subnets
    sub_a = ec2_client.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
    )
    subnet_a_id = sub_a["Subnet"]["SubnetId"]
    ec2_client.create_tags(Resources=[subnet_a_id], Tags=[{"Key": "Name", "Value": "tf-subnet-a"}])

    sub_b = ec2_client.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
    )
    subnet_b_id = sub_b["Subnet"]["SubnetId"]
    ec2_client.create_tags(Resources=[subnet_b_id], Tags=[{"Key": "Name", "Value": "tf-subnet-b"}])

    # Internet Gateway
    igw = ec2_client.create_internet_gateway()
    igw_id = igw["InternetGateway"]["InternetGatewayId"]
    ec2_client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

    # Route table with IGW route
    rt = ec2_client.create_route_table(VpcId=vpc_id)
    rt_id = rt["RouteTable"]["RouteTableId"]
    ec2_client.create_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
    assoc_a = ec2_client.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_a_id)
    assoc_a_id = assoc_a["AssociationId"]
    assoc_b = ec2_client.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_b_id)
    assoc_b_id = assoc_b["AssociationId"]

    # Security group
    sg = ec2_client.create_security_group(
        GroupName="tf-app-sg",
        Description="Application security group",
        VpcId=vpc_id,
    )
    sg_id = sg["GroupId"]
    ec2_client.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            },
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            },
        ],
    )

    yield {
        "vpc_id": vpc_id,
        "subnet_ids": [subnet_a_id, subnet_b_id],
        "security_group_id": sg_id,
    }

    # Cleanup
    ec2_client.delete_security_group(GroupId=sg_id)
    ec2_client.disassociate_route_table(AssociationId=assoc_a_id)
    ec2_client.disassociate_route_table(AssociationId=assoc_b_id)
    ec2_client.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    ec2_client.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2_client.delete_subnet(SubnetId=subnet_a_id)
    ec2_client.delete_subnet(SubnetId=subnet_b_id)
    ec2_client.delete_route_table(RouteTableId=rt_id)
    ec2_client.delete_vpc(VpcId=vpc_id)


class TestVpcNetwork:
    """Terraform VPC network: VPC + 2 subnets + security group."""

    def test_vpc_created(self, vpc_resources, ec2_client):
        vpc_id = vpc_resources["vpc_id"]
        vpc = assert_vpc_exists(ec2_client, vpc_id)
        assert vpc["CidrBlock"] == "10.0.0.0/16"

    def test_subnets_created(self, vpc_resources, ec2_client):
        subnet_ids = vpc_resources["subnet_ids"]
        assert len(subnet_ids) == 2

        resp = ec2_client.describe_subnets(SubnetIds=subnet_ids)
        subnets = resp["Subnets"]
        assert len(subnets) == 2

        azs = sorted(s["AvailabilityZone"] for s in subnets)
        assert azs == ["us-east-1a", "us-east-1b"]

        cidrs = sorted(s["CidrBlock"] for s in subnets)
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

    def test_security_group_created(self, vpc_resources, ec2_client):
        sg_id = vpc_resources["security_group_id"]

        resp = ec2_client.describe_security_groups(GroupIds=[sg_id])
        sgs = resp["SecurityGroups"]
        assert len(sgs) == 1

        ingress = sgs[0]["IpPermissions"]
        ingress_ports = sorted(r["FromPort"] for r in ingress)
        assert ingress_ports == [22, 80]

        for rule in ingress:
            assert rule["IpProtocol"] == "tcp"
            assert any(ip_range["CidrIp"] == "0.0.0.0/0" for ip_range in rule["IpRanges"])

    def test_route_table_has_igw_route(self, vpc_resources, ec2_client):
        """Verify a route table has a 0.0.0.0/0 route via an internet gateway."""
        vpc_id = vpc_resources["vpc_id"]
        rts = ec2_client.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        routes = []
        for rt in rts["RouteTables"]:
            routes.extend(rt.get("Routes", []))
        igw_routes = [r for r in routes if r.get("GatewayId", "").startswith("igw-")]
        assert any(r["DestinationCidrBlock"] == "0.0.0.0/0" for r in igw_routes)

    def test_subnet_associations(self, vpc_resources, ec2_client):
        """Verify subnets are associated with the VPC."""
        vpc_id = vpc_resources["vpc_id"]
        subnets = ec2_client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        assert len(subnets["Subnets"]) >= 2
