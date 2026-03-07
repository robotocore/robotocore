"""EC2 compatibility tests (basic operations)."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Shared fixtures with proper cleanup
# ---------------------------------------------------------------------------


@pytest.fixture
def vpc(ec2):
    """Create a VPC for tests that need one, clean up after."""
    cidr = "10.50.0.0/16"
    resp = ec2.create_vpc(CidrBlock=cidr)
    vpc_id = resp["Vpc"]["VpcId"]
    # Wait until available so dependent resources can be created
    ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_id])
    yield {"VpcId": vpc_id, "CidrBlock": cidr}
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except Exception:
        pass


@pytest.fixture
def security_group(ec2, vpc):
    """Create a security group in the test VPC."""
    sg_name = _unique("sg")
    resp = ec2.create_security_group(
        GroupName=sg_name,
        Description="Compat test SG",
        VpcId=vpc["VpcId"],
    )
    sg_id = resp["GroupId"]
    yield {"GroupId": sg_id, "GroupName": sg_name, "VpcId": vpc["VpcId"]}
    try:
        ec2.delete_security_group(GroupId=sg_id)
    except Exception:
        pass


@pytest.fixture
def subnet(ec2, vpc):
    """Create a subnet in the test VPC."""
    resp = ec2.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.50.1.0/24")
    subnet_id = resp["Subnet"]["SubnetId"]
    yield {"SubnetId": subnet_id, "VpcId": vpc["VpcId"], "CidrBlock": "10.50.1.0/24"}
    try:
        ec2.delete_subnet(SubnetId=subnet_id)
    except Exception:
        pass


@pytest.fixture
def internet_gateway(ec2):
    """Create an internet gateway, clean up after."""
    resp = ec2.create_internet_gateway()
    igw_id = resp["InternetGateway"]["InternetGatewayId"]
    yield {"InternetGatewayId": igw_id}
    try:
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    except Exception:
        pass


@pytest.fixture
def key_pair(ec2):
    """Create a key pair, clean up after."""
    name = _unique("keypair")
    resp = ec2.create_key_pair(KeyName=name)
    yield {"KeyName": name, "KeyFingerprint": resp["KeyFingerprint"]}
    try:
        ec2.delete_key_pair(KeyName=name)
    except Exception:
        pass


@pytest.fixture
def elastic_ip(ec2):
    """Allocate an Elastic IP, release after."""
    resp = ec2.allocate_address(Domain="vpc")
    yield {"AllocationId": resp["AllocationId"], "PublicIp": resp["PublicIp"]}
    try:
        ec2.release_address(AllocationId=resp["AllocationId"])
    except Exception:
        pass


# ---------------------------------------------------------------------------
# VPC CRUD
# ---------------------------------------------------------------------------


class TestEC2VPCCrud:
    def test_create_vpc_returns_valid_id(self, ec2):
        resp = ec2.create_vpc(CidrBlock="10.60.0.0/16")
        vpc_id = resp["Vpc"]["VpcId"]
        try:
            assert vpc_id.startswith("vpc-")
            assert resp["Vpc"]["CidrBlock"] == "10.60.0.0/16"
            assert resp["Vpc"]["State"] in ("available", "pending")
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_vpcs_by_id(self, ec2, vpc):
        described = ec2.describe_vpcs(VpcIds=[vpc["VpcId"]])
        assert len(described["Vpcs"]) == 1
        assert described["Vpcs"][0]["VpcId"] == vpc["VpcId"]
        assert described["Vpcs"][0]["CidrBlock"] == vpc["CidrBlock"]

    def test_describe_vpcs_with_filter(self, ec2, vpc):
        described = ec2.describe_vpcs(
            Filters=[{"Name": "cidr", "Values": [vpc["CidrBlock"]]}]
        )
        vpc_ids = [v["VpcId"] for v in described["Vpcs"]]
        assert vpc["VpcId"] in vpc_ids

    def test_delete_vpc(self, ec2):
        resp = ec2.create_vpc(CidrBlock="10.61.0.0/16")
        vpc_id = resp["Vpc"]["VpcId"]
        ec2.delete_vpc(VpcId=vpc_id)
        # Verify it is gone
        described = ec2.describe_vpcs(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        assert len(described["Vpcs"]) == 0

    def test_create_vpc_with_tags(self, ec2):
        tag_name = _unique("vpc-tag")
        resp = ec2.create_vpc(
            CidrBlock="10.62.0.0/16",
            TagSpecifications=[
                {
                    "ResourceType": "vpc",
                    "Tags": [{"Key": "Name", "Value": tag_name}],
                }
            ],
        )
        vpc_id = resp["Vpc"]["VpcId"]
        try:
            tags = {t["Key"]: t["Value"] for t in resp["Vpc"].get("Tags", [])}
            assert tags.get("Name") == tag_name
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_vpcs_default_exists(self, ec2):
        response = ec2.describe_vpcs()
        assert len(response["Vpcs"]) >= 1


# ---------------------------------------------------------------------------
# Subnet CRUD
# ---------------------------------------------------------------------------


class TestEC2SubnetCrud:
    def test_create_subnet(self, ec2, vpc):
        resp = ec2.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.50.2.0/24")
        subnet_id = resp["Subnet"]["SubnetId"]
        try:
            assert subnet_id.startswith("subnet-")
            assert resp["Subnet"]["VpcId"] == vpc["VpcId"]
            assert resp["Subnet"]["CidrBlock"] == "10.50.2.0/24"
        finally:
            ec2.delete_subnet(SubnetId=subnet_id)

    def test_describe_subnets_by_id(self, ec2, subnet):
        described = ec2.describe_subnets(SubnetIds=[subnet["SubnetId"]])
        assert len(described["Subnets"]) == 1
        assert described["Subnets"][0]["SubnetId"] == subnet["SubnetId"]
        assert described["Subnets"][0]["VpcId"] == subnet["VpcId"]

    def test_describe_subnets_with_filter(self, ec2, subnet):
        described = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [subnet["VpcId"]]}]
        )
        subnet_ids = [s["SubnetId"] for s in described["Subnets"]]
        assert subnet["SubnetId"] in subnet_ids

    def test_delete_subnet(self, ec2, vpc):
        resp = ec2.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.50.3.0/24")
        subnet_id = resp["Subnet"]["SubnetId"]
        ec2.delete_subnet(SubnetId=subnet_id)
        described = ec2.describe_subnets(
            Filters=[{"Name": "subnet-id", "Values": [subnet_id]}]
        )
        assert len(described["Subnets"]) == 0

    def test_create_subnet_with_az(self, ec2, vpc):
        resp = ec2.create_subnet(
            VpcId=vpc["VpcId"],
            CidrBlock="10.50.4.0/24",
            AvailabilityZone="us-east-1a",
        )
        subnet_id = resp["Subnet"]["SubnetId"]
        try:
            assert resp["Subnet"]["AvailabilityZone"] == "us-east-1a"
        finally:
            ec2.delete_subnet(SubnetId=subnet_id)


# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------


class TestEC2SecurityGroups:
    def test_create_security_group_in_vpc(self, ec2, vpc):
        sg_name = _unique("sg-create")
        resp = ec2.create_security_group(
            GroupName=sg_name,
            Description="Test SG creation",
            VpcId=vpc["VpcId"],
        )
        sg_id = resp["GroupId"]
        try:
            assert sg_id.startswith("sg-")
        finally:
            ec2.delete_security_group(GroupId=sg_id)

    def test_describe_security_group_by_id(self, ec2, security_group):
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        assert len(described["SecurityGroups"]) == 1
        sg = described["SecurityGroups"][0]
        assert sg["GroupId"] == security_group["GroupId"]
        assert sg["GroupName"] == security_group["GroupName"]
        assert sg["VpcId"] == security_group["VpcId"]

    def test_authorize_ingress_tcp(self, ec2, security_group):
        ec2.authorize_security_group_ingress(
            GroupId=security_group["GroupId"],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "10.0.0.0/8", "Description": "SSH"}],
                }
            ],
        )
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        perms = described["SecurityGroups"][0]["IpPermissions"]
        tcp_22 = [p for p in perms if p.get("FromPort") == 22]
        assert len(tcp_22) == 1
        assert tcp_22[0]["IpProtocol"] == "tcp"
        assert tcp_22[0]["IpRanges"][0]["CidrIp"] == "10.0.0.0/8"

    def test_authorize_egress(self, ec2, security_group):
        # First revoke default egress allow-all so we can add a specific one
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        existing_egress = described["SecurityGroups"][0]["IpPermissionsEgress"]
        if existing_egress:
            ec2.revoke_security_group_egress(
                GroupId=security_group["GroupId"],
                IpPermissions=existing_egress,
            )

        ec2.authorize_security_group_egress(
            GroupId=security_group["GroupId"],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "HTTPS out"}],
                }
            ],
        )
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        egress = described["SecurityGroups"][0]["IpPermissionsEgress"]
        https_out = [p for p in egress if p.get("FromPort") == 443]
        assert len(https_out) == 1

    def test_revoke_ingress(self, ec2, security_group):
        perm = {
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
        }
        ec2.authorize_security_group_ingress(
            GroupId=security_group["GroupId"],
            IpPermissions=[perm],
        )
        ec2.revoke_security_group_ingress(
            GroupId=security_group["GroupId"],
            IpPermissions=[perm],
        )
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        perms = described["SecurityGroups"][0]["IpPermissions"]
        mysql_rules = [p for p in perms if p.get("FromPort") == 3306]
        assert len(mysql_rules) == 0

    def test_authorize_multiple_ingress_rules(self, ec2, security_group):
        ec2.authorize_security_group_ingress(
            GroupId=security_group["GroupId"],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 80,
                    "ToPort": 80,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        )
        described = ec2.describe_security_groups(GroupIds=[security_group["GroupId"]])
        ports = sorted([p["FromPort"] for p in described["SecurityGroups"][0]["IpPermissions"]])
        assert 80 in ports
        assert 443 in ports

    def test_delete_security_group(self, ec2, vpc):
        sg_name = _unique("sg-del")
        resp = ec2.create_security_group(
            GroupName=sg_name,
            Description="To be deleted",
            VpcId=vpc["VpcId"],
        )
        sg_id = resp["GroupId"]
        ec2.delete_security_group(GroupId=sg_id)
        described = ec2.describe_security_groups(
            Filters=[{"Name": "group-id", "Values": [sg_id]}]
        )
        assert len(described["SecurityGroups"]) == 0


# ---------------------------------------------------------------------------
# Internet Gateways
# ---------------------------------------------------------------------------


class TestEC2InternetGateways:
    def test_create_internet_gateway(self, ec2, internet_gateway):
        assert internet_gateway["InternetGatewayId"].startswith("igw-")

    def test_describe_internet_gateway_by_id(self, ec2, internet_gateway):
        described = ec2.describe_internet_gateways(
            InternetGatewayIds=[internet_gateway["InternetGatewayId"]]
        )
        assert len(described["InternetGateways"]) == 1
        assert (
            described["InternetGateways"][0]["InternetGatewayId"]
            == internet_gateway["InternetGatewayId"]
        )

    def test_attach_and_detach_internet_gateway(self, ec2, vpc):
        resp = ec2.create_internet_gateway()
        igw_id = resp["InternetGateway"]["InternetGatewayId"]
        try:
            ec2.attach_internet_gateway(
                InternetGatewayId=igw_id,
                VpcId=vpc["VpcId"],
            )
            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0]["Attachments"]
            assert len(attachments) == 1
            assert attachments[0]["VpcId"] == vpc["VpcId"]
            assert attachments[0]["State"] in ("available", "attached")

            ec2.detach_internet_gateway(
                InternetGatewayId=igw_id,
                VpcId=vpc["VpcId"],
            )
            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0].get("Attachments", [])
            # After detach, attachments should be empty or state=detached
            attached = [a for a in attachments if a.get("State") in ("available", "attached")]
            assert len(attached) == 0
        finally:
            try:
                ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc["VpcId"])
            except Exception:
                pass
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)

    def test_delete_internet_gateway(self, ec2):
        resp = ec2.create_internet_gateway()
        igw_id = resp["InternetGateway"]["InternetGatewayId"]
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        described = ec2.describe_internet_gateways(
            Filters=[{"Name": "internet-gateway-id", "Values": [igw_id]}]
        )
        assert len(described["InternetGateways"]) == 0


# ---------------------------------------------------------------------------
# Route Tables
# ---------------------------------------------------------------------------


class TestEC2RouteTables:
    def test_create_route_table(self, ec2, vpc):
        resp = ec2.create_route_table(VpcId=vpc["VpcId"])
        rt_id = resp["RouteTable"]["RouteTableId"]
        try:
            assert rt_id.startswith("rtb-")
            assert resp["RouteTable"]["VpcId"] == vpc["VpcId"]
        finally:
            ec2.delete_route_table(RouteTableId=rt_id)

    def test_describe_route_tables(self, ec2, vpc):
        resp = ec2.create_route_table(VpcId=vpc["VpcId"])
        rt_id = resp["RouteTable"]["RouteTableId"]
        try:
            described = ec2.describe_route_tables(RouteTableIds=[rt_id])
            assert len(described["RouteTables"]) == 1
            assert described["RouteTables"][0]["RouteTableId"] == rt_id
            # Should have a local route by default
            routes = described["RouteTables"][0]["Routes"]
            local_routes = [r for r in routes if r.get("GatewayId") == "local"]
            assert len(local_routes) >= 1
        finally:
            ec2.delete_route_table(RouteTableId=rt_id)

    def test_create_route_to_igw(self, ec2, vpc):
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc["VpcId"])
        rt_resp = ec2.create_route_table(VpcId=vpc["VpcId"])
        rt_id = rt_resp["RouteTable"]["RouteTableId"]
        try:
            ec2.create_route(
                RouteTableId=rt_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=igw_id,
            )
            described = ec2.describe_route_tables(RouteTableIds=[rt_id])
            routes = described["RouteTables"][0]["Routes"]
            igw_routes = [r for r in routes if r.get("GatewayId") == igw_id]
            assert len(igw_routes) == 1
            assert igw_routes[0]["DestinationCidrBlock"] == "0.0.0.0/0"
        finally:
            ec2.delete_route_table(RouteTableId=rt_id)
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc["VpcId"])
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)

    def test_delete_route_table(self, ec2, vpc):
        resp = ec2.create_route_table(VpcId=vpc["VpcId"])
        rt_id = resp["RouteTable"]["RouteTableId"]
        ec2.delete_route_table(RouteTableId=rt_id)
        described = ec2.describe_route_tables(
            Filters=[{"Name": "route-table-id", "Values": [rt_id]}]
        )
        assert len(described["RouteTables"]) == 0


# ---------------------------------------------------------------------------
# Key Pairs
# ---------------------------------------------------------------------------


class TestEC2KeyPairs:
    def test_create_key_pair(self, ec2, key_pair):
        assert key_pair["KeyFingerprint"] is not None

    def test_describe_key_pair(self, ec2, key_pair):
        described = ec2.describe_key_pairs(KeyNames=[key_pair["KeyName"]])
        assert len(described["KeyPairs"]) == 1
        assert described["KeyPairs"][0]["KeyName"] == key_pair["KeyName"]

    def test_delete_key_pair(self, ec2):
        name = _unique("kp-del")
        ec2.create_key_pair(KeyName=name)
        ec2.delete_key_pair(KeyName=name)
        with pytest.raises(Exception):
            ec2.describe_key_pairs(KeyNames=[name])

    def test_create_key_pair_returns_material(self, ec2):
        name = _unique("kp-mat")
        resp = ec2.create_key_pair(KeyName=name)
        try:
            assert "KeyMaterial" in resp
            assert len(resp["KeyMaterial"]) > 0
            assert resp["KeyName"] == name
        finally:
            ec2.delete_key_pair(KeyName=name)


# ---------------------------------------------------------------------------
# Network Interfaces
# ---------------------------------------------------------------------------


class TestEC2NetworkInterfaces:
    def test_create_network_interface(self, ec2, subnet):
        resp = ec2.create_network_interface(SubnetId=subnet["SubnetId"])
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        try:
            assert eni_id.startswith("eni-")
            assert resp["NetworkInterface"]["SubnetId"] == subnet["SubnetId"]
            assert resp["NetworkInterface"]["VpcId"] == subnet["VpcId"]
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)

    def test_describe_network_interface(self, ec2, subnet):
        resp = ec2.create_network_interface(SubnetId=subnet["SubnetId"])
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        try:
            described = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
            assert len(described["NetworkInterfaces"]) == 1
            assert described["NetworkInterfaces"][0]["NetworkInterfaceId"] == eni_id
            assert described["NetworkInterfaces"][0]["SubnetId"] == subnet["SubnetId"]
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)

    def test_delete_network_interface(self, ec2, subnet):
        resp = ec2.create_network_interface(SubnetId=subnet["SubnetId"])
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        ec2.delete_network_interface(NetworkInterfaceId=eni_id)
        described = ec2.describe_network_interfaces(
            Filters=[{"Name": "network-interface-id", "Values": [eni_id]}]
        )
        assert len(described["NetworkInterfaces"]) == 0

    def test_create_network_interface_with_security_group(self, ec2, subnet, security_group):
        resp = ec2.create_network_interface(
            SubnetId=subnet["SubnetId"],
            Groups=[security_group["GroupId"]],
        )
        eni_id = resp["NetworkInterface"]["NetworkInterfaceId"]
        try:
            sg_ids = [g["GroupId"] for g in resp["NetworkInterface"]["Groups"]]
            assert security_group["GroupId"] in sg_ids
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)


# ---------------------------------------------------------------------------
# AMI / Images
# ---------------------------------------------------------------------------


class TestEC2Images:
    def test_describe_images_with_owner(self, ec2):
        """Describe images owned by amazon -- should return results."""
        resp = ec2.describe_images(
            Filters=[{"Name": "architecture", "Values": ["x86_64"]}],
            Owners=["amazon"],
        )
        assert "Images" in resp

    def test_describe_images_self(self, ec2):
        """Describe images owned by self -- should return empty or list."""
        resp = ec2.describe_images(Owners=["self"])
        assert "Images" in resp
        assert isinstance(resp["Images"], list)


# ---------------------------------------------------------------------------
# Elastic IPs
# ---------------------------------------------------------------------------


class TestEC2ElasticIPs:
    def test_allocate_address(self, ec2, elastic_ip):
        assert elastic_ip["AllocationId"].startswith("eipalloc-")
        assert elastic_ip["PublicIp"] is not None

    def test_describe_address_by_allocation_id(self, ec2, elastic_ip):
        described = ec2.describe_addresses(AllocationIds=[elastic_ip["AllocationId"]])
        assert len(described["Addresses"]) == 1
        assert described["Addresses"][0]["PublicIp"] == elastic_ip["PublicIp"]
        assert described["Addresses"][0]["AllocationId"] == elastic_ip["AllocationId"]

    def test_describe_address_with_filter(self, ec2, elastic_ip):
        described = ec2.describe_addresses(
            Filters=[{"Name": "public-ip", "Values": [elastic_ip["PublicIp"]]}]
        )
        assert len(described["Addresses"]) >= 1
        ips = [a["PublicIp"] for a in described["Addresses"]]
        assert elastic_ip["PublicIp"] in ips

    def test_release_address(self, ec2):
        resp = ec2.allocate_address(Domain="vpc")
        alloc_id = resp["AllocationId"]
        ec2.release_address(AllocationId=alloc_id)
        described = ec2.describe_addresses(
            Filters=[{"Name": "allocation-id", "Values": [alloc_id]}]
        )
        assert len(described["Addresses"]) == 0


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


class TestEC2Tagging:
    def test_create_and_describe_tags(self, ec2, vpc):
        tag_key = _unique("tag-key")
        tag_value = _unique("tag-val")
        ec2.create_tags(
            Resources=[vpc["VpcId"]],
            Tags=[{"Key": tag_key, "Value": tag_value}],
        )
        described = ec2.describe_tags(
            Filters=[
                {"Name": "resource-id", "Values": [vpc["VpcId"]]},
                {"Name": "key", "Values": [tag_key]},
            ]
        )
        assert len(described["Tags"]) == 1
        assert described["Tags"][0]["Key"] == tag_key
        assert described["Tags"][0]["Value"] == tag_value

    def test_delete_tags(self, ec2, vpc):
        tag_key = _unique("tag-del")
        ec2.create_tags(
            Resources=[vpc["VpcId"]],
            Tags=[{"Key": tag_key, "Value": "to-delete"}],
        )
        ec2.delete_tags(
            Resources=[vpc["VpcId"]],
            Tags=[{"Key": tag_key}],
        )
        described = ec2.describe_tags(
            Filters=[
                {"Name": "resource-id", "Values": [vpc["VpcId"]]},
                {"Name": "key", "Values": [tag_key]},
            ]
        )
        assert len(described["Tags"]) == 0


# ---------------------------------------------------------------------------
# Misc operations
# ---------------------------------------------------------------------------


class TestEC2MiscOperations:
    def test_describe_instances(self, ec2):
        response = ec2.describe_instances()
        assert "Reservations" in response

    def test_describe_regions(self, ec2):
        response = ec2.describe_regions()
        region_names = [r["RegionName"] for r in response["Regions"]]
        assert "us-east-1" in region_names

    def test_describe_availability_zones(self, ec2):
        response = ec2.describe_availability_zones()
        assert "AvailabilityZones" in response
        az_names = [az["ZoneName"] for az in response["AvailabilityZones"]]
        assert len(az_names) > 0

    def test_describe_account_attributes(self, ec2):
        response = ec2.describe_account_attributes()
        assert "AccountAttributes" in response
