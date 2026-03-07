"""EC2 compatibility tests (basic operations)."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestEC2Operations:
    def test_describe_vpcs(self, ec2):
        response = ec2.describe_vpcs()
        # Default VPC should exist
        assert len(response["Vpcs"]) >= 1

    def test_describe_subnets(self, ec2):
        response = ec2.describe_subnets()
        assert "Subnets" in response

    def test_create_security_group(self, ec2):
        response = ec2.create_security_group(
            GroupName="test-sg",
            Description="Test security group",
        )
        sg_id = response["GroupId"]
        assert sg_id.startswith("sg-")

        described = ec2.describe_security_groups(GroupIds=[sg_id])
        assert len(described["SecurityGroups"]) == 1
        ec2.delete_security_group(GroupId=sg_id)

    def test_create_and_describe_key_pair(self, ec2):
        response = ec2.create_key_pair(KeyName="test-key")
        assert "KeyMaterial" in response

        pairs = ec2.describe_key_pairs(KeyNames=["test-key"])
        assert len(pairs["KeyPairs"]) == 1
        ec2.delete_key_pair(KeyName="test-key")

    def test_describe_instances(self, ec2):
        response = ec2.describe_instances()
        assert "Reservations" in response

    def test_describe_regions(self, ec2):
        response = ec2.describe_regions()
        region_names = [r["RegionName"] for r in response["Regions"]]
        assert "us-east-1" in region_names


class TestEC2VPCOperations:
    def test_create_and_describe_vpc(self, ec2):
        resp = ec2.create_vpc(CidrBlock="10.99.0.0/16")
        vpc_id = resp["Vpc"]["VpcId"]
        try:
            assert vpc_id.startswith("vpc-")
            described = ec2.describe_vpcs(VpcIds=[vpc_id])
            assert len(described["Vpcs"]) == 1
            assert described["Vpcs"][0]["CidrBlock"] == "10.99.0.0/16"
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_subnet_in_vpc(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.98.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.98.1.0/24")
            subnet_id = subnet_resp["Subnet"]["SubnetId"]
            assert subnet_id.startswith("subnet-")

            described = ec2.describe_subnets(SubnetIds=[subnet_id])
            assert len(described["Subnets"]) == 1
            assert described["Subnets"][0]["VpcId"] == vpc_id
            assert described["Subnets"][0]["CidrBlock"] == "10.98.1.0/24"

            ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2SecurityGroupIngress:
    def test_create_security_group_with_ingress(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.97.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg-ingress")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name,
                Description="SG with ingress rules",
                VpcId=vpc_id,
            )
            sg_id = sg_resp["GroupId"]

            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 443,
                        "ToPort": 443,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "HTTPS"}],
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 80,
                        "ToPort": 80,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "HTTP"}],
                    },
                ],
            )

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            sg = described["SecurityGroups"][0]
            ports = sorted([p["FromPort"] for p in sg["IpPermissions"]])
            assert 80 in ports
            assert 443 in ports

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ElasticIP:
    def test_allocate_and_describe_elastic_ip(self, ec2):
        alloc = ec2.allocate_address(Domain="vpc")
        alloc_id = alloc["AllocationId"]
        try:
            assert alloc["PublicIp"] is not None
            assert alloc_id.startswith("eipalloc-")

            described = ec2.describe_addresses(AllocationIds=[alloc_id])
            assert len(described["Addresses"]) == 1
            assert described["Addresses"][0]["PublicIp"] == alloc["PublicIp"]
        finally:
            ec2.release_address(AllocationId=alloc_id)


class TestEC2InternetGateway:
    def test_create_and_describe_internet_gateway(self, ec2):
        resp = ec2.create_internet_gateway()
        igw_id = resp["InternetGateway"]["InternetGatewayId"]
        try:
            assert igw_id.startswith("igw-")

            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            assert len(described["InternetGateways"]) == 1
            assert described["InternetGateways"][0]["InternetGatewayId"] == igw_id
        finally:
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)


class TestEC2KeyPairExtended:
    def test_create_key_pair_with_unique_name(self, ec2):
        key_name = _unique("keypair")
        resp = ec2.create_key_pair(KeyName=key_name)
        try:
            assert "KeyMaterial" in resp
            assert resp["KeyName"] == key_name
            assert resp["KeyFingerprint"] is not None

            pairs = ec2.describe_key_pairs(KeyNames=[key_name])
            assert len(pairs["KeyPairs"]) == 1
            assert pairs["KeyPairs"][0]["KeyName"] == key_name
        finally:
            ec2.delete_key_pair(KeyName=key_name)


class TestEC2AvailabilityZones:
    def test_describe_availability_zones(self, ec2):
        response = ec2.describe_availability_zones()
        assert len(response["AvailabilityZones"]) >= 1
        az = response["AvailabilityZones"][0]
        assert "ZoneName" in az
        assert "RegionName" in az
        assert az["State"] == "available"

    def test_describe_availability_zones_contains_us_east_1(self, ec2):
        response = ec2.describe_availability_zones()
        zone_names = [az["ZoneName"] for az in response["AvailabilityZones"]]
        assert any(z.startswith("us-east-1") for z in zone_names)


class TestEC2NetworkInterface:
    def test_create_describe_delete_network_interface(self, ec2):
        # Use default VPC subnet
        subnets = ec2.describe_subnets()
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        resp = ec2.create_network_interface(SubnetId=subnet_id)
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        try:
            assert eni_id.startswith("eni-")
            described = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
            assert len(described["NetworkInterfaces"]) == 1
            assert described["NetworkInterfaces"][0]["SubnetId"] == subnet_id
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)

    def test_network_interface_with_description(self, ec2):
        subnets = ec2.describe_subnets()
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        desc = _unique("eni-desc")
        resp = ec2.create_network_interface(SubnetId=subnet_id, Description=desc)
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        try:
            described = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
            assert described["NetworkInterfaces"][0]["Description"] == desc
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)


class TestEC2InternetGatewayAttach:
    def test_attach_detach_internet_gateway(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.80.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        try:
            ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0]["Attachments"]
            assert any(a["VpcId"] == vpc_id for a in attachments)
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0].get("Attachments", [])
            attached_vpcs = [a["VpcId"] for a in attachments if a.get("State") == "available"]
            assert vpc_id not in attached_vpcs
        finally:
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2RouteTable:
    def test_create_describe_delete_route_table(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.81.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            rt_resp = ec2.create_route_table(VpcId=vpc_id)
            rt_id = rt_resp["RouteTable"]["RouteTableId"]
            assert rt_id.startswith("rtb-")
            described = ec2.describe_route_tables(RouteTableIds=[rt_id])
            assert len(described["RouteTables"]) == 1
            assert described["RouteTables"][0]["VpcId"] == vpc_id
            ec2.delete_route_table(RouteTableId=rt_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_and_delete_route(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.82.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        try:
            rt_resp = ec2.create_route_table(VpcId=vpc_id)
            rt_id = rt_resp["RouteTable"]["RouteTableId"]
            ec2.create_route(
                RouteTableId=rt_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=igw_id,
            )
            described = ec2.describe_route_tables(RouteTableIds=[rt_id])
            destinations = [
                r.get("DestinationCidrBlock") for r in described["RouteTables"][0]["Routes"]
            ]
            assert "0.0.0.0/0" in destinations
            ec2.delete_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0")
            ec2.delete_route_table(RouteTableId=rt_id)
        finally:
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2DescribeImages:
    def test_describe_images_with_owner(self, ec2):
        response = ec2.describe_images(Owners=["amazon"])
        assert "Images" in response

    def test_describe_images_with_filter(self, ec2):
        response = ec2.describe_images(
            Filters=[{"Name": "architecture", "Values": ["x86_64"]}],
            Owners=["amazon"],
        )
        assert "Images" in response


class TestEC2LaunchTemplate:
    def test_create_describe_delete_launch_template(self, ec2):
        lt_name = _unique("lt")
        resp = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            assert resp["LaunchTemplate"]["LaunchTemplateName"] == lt_name
            described = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
            assert len(described["LaunchTemplates"]) == 1
            assert described["LaunchTemplates"][0]["LaunchTemplateName"] == lt_name
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)

    def test_describe_launch_template_versions(self, ec2):
        lt_name = _unique("lt-ver")
        resp = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
            assert len(versions["LaunchTemplateVersions"]) >= 1
            assert versions["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"] == "t2.micro"
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)
