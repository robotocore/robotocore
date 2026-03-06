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
