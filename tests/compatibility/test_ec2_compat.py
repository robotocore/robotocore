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


class TestEC2Tags:
    def test_create_tags_on_vpc(self, ec2):
        """Create tags on a VPC and verify with DescribeTags."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.80.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            ec2.create_tags(
                Resources=[vpc_id],
                Tags=[
                    {"Key": "Name", "Value": "tagged-vpc"},
                    {"Key": "Env", "Value": "test"},
                ],
            )
            described = ec2.describe_vpcs(VpcIds=[vpc_id])
            tags = {t["Key"]: t["Value"] for t in described["Vpcs"][0].get("Tags", [])}
            assert tags["Name"] == "tagged-vpc"
            assert tags["Env"] == "test"
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_tags_on_subnet(self, ec2):
        """Create tags on a subnet and verify."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.81.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.81.1.0/24")
            subnet_id = subnet_resp["Subnet"]["SubnetId"]
            ec2.create_tags(
                Resources=[subnet_id],
                Tags=[{"Key": "Name", "Value": "tagged-subnet"}],
            )
            described = ec2.describe_subnets(SubnetIds=[subnet_id])
            tags = {t["Key"]: t["Value"] for t in described["Subnets"][0].get("Tags", [])}
            assert tags["Name"] == "tagged-subnet"
            ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_tags_with_filter(self, ec2):
        """Use DescribeTags with resource-id filter."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.82.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            ec2.create_tags(
                Resources=[vpc_id],
                Tags=[{"Key": "FilterTest", "Value": "yes"}],
            )
            response = ec2.describe_tags(
                Filters=[{"Name": "resource-id", "Values": [vpc_id]}]
            )
            keys = [t["Key"] for t in response["Tags"]]
            assert "FilterTest" in keys
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2InternetGatewayAttach:
    def test_attach_and_detach_internet_gateway(self, ec2):
        """Attach an IGW to a VPC, then detach it."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.83.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        try:
            ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0]["Attachments"]
            assert any(a["VpcId"] == vpc_id for a in attachments)

            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        finally:
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2RouteTables:
    def test_create_and_describe_route_table(self, ec2):
        """Create a route table and verify it exists."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.84.0.0/16")
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

    def test_create_route_in_route_table(self, ec2):
        """Create a route pointing to an IGW."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.85.0.0/16")
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
            routes = described["RouteTables"][0]["Routes"]
            cidrs = [r.get("DestinationCidrBlock") for r in routes]
            assert "0.0.0.0/0" in cidrs

            ec2.delete_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0")
            ec2.delete_route_table(RouteTableId=rt_id)
        finally:
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NetworkInterfaces:
    def test_create_and_describe_network_interface(self, ec2):
        """Create a network interface in a subnet."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.86.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.86.1.0/24")
            subnet_id = subnet_resp["Subnet"]["SubnetId"]

            eni_resp = ec2.create_network_interface(SubnetId=subnet_id)
            eni_id = eni_resp["NetworkInterface"]["NetworkInterfaceId"]
            assert eni_id.startswith("eni-")

            described = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
            assert len(described["NetworkInterfaces"]) == 1
            assert described["NetworkInterfaces"][0]["SubnetId"] == subnet_id

            ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2AvailabilityZones:
    def test_describe_availability_zones(self, ec2):
        """DescribeAvailabilityZones should return zones for us-east-1."""
        response = ec2.describe_availability_zones()
        assert len(response["AvailabilityZones"]) >= 1
        zone_names = [z["ZoneName"] for z in response["AvailabilityZones"]]
        assert any(z.startswith("us-east-1") for z in zone_names)

    def test_availability_zone_has_state(self, ec2):
        """Each AZ should have a State field."""
        response = ec2.describe_availability_zones()
        for az in response["AvailabilityZones"]:
            assert "State" in az
            assert "ZoneName" in az
            assert "RegionName" in az


class TestEC2SecurityGroupEgress:
    def test_authorize_and_revoke_egress(self, ec2):
        """Authorize and then revoke an egress rule."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.87.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg-egress")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name,
                Description="Egress test SG",
                VpcId=vpc_id,
            )
            sg_id = sg_resp["GroupId"]

            ec2.authorize_security_group_egress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
                    }
                ],
            )

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            sg = described["SecurityGroups"][0]
            egress_ports = [p.get("FromPort") for p in sg["IpPermissionsEgress"]]
            assert 8080 in egress_ports

            ec2.revoke_security_group_egress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
                    }
                ],
            )

            described2 = ec2.describe_security_groups(GroupIds=[sg_id])
            egress_ports2 = [p.get("FromPort") for p in described2["SecurityGroups"][0]["IpPermissionsEgress"]]
            assert 8080 not in egress_ports2

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_revoke_security_group_ingress(self, ec2):
        """Authorize an ingress rule and then revoke it."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.88.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg-revoke")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name,
                Description="Revoke ingress test",
                VpcId=vpc_id,
            )
            sg_id = sg_resp["GroupId"]

            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )

            ec2.revoke_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            ingress_ports = [p.get("FromPort") for p in described["SecurityGroups"][0]["IpPermissions"]]
            assert 22 not in ingress_ports
            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ExtendedOperations:
    def test_create_and_delete_key_pair(self, ec2):
        key_name = _unique("ext-key")
        resp = ec2.create_key_pair(KeyName=key_name)
        try:
            assert resp["KeyName"] == key_name
            assert "KeyMaterial" in resp
        finally:
            ec2.delete_key_pair(KeyName=key_name)

    def test_describe_key_pairs(self, ec2):
        key_name = _unique("ext-desc-key")
        ec2.create_key_pair(KeyName=key_name)
        try:
            response = ec2.describe_key_pairs(KeyNames=[key_name])
            names = [kp["KeyName"] for kp in response["KeyPairs"]]
            assert key_name in names
        finally:
            ec2.delete_key_pair(KeyName=key_name)

    def test_create_and_delete_security_group(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.88.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("ext-sg")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name,
                Description="Extended test security group",
                VpcId=vpc_id,
            )
            sg_id = sg_resp["GroupId"]
            assert sg_id.startswith("sg-")

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            assert len(described["SecurityGroups"]) == 1
            assert described["SecurityGroups"][0]["GroupName"] == sg_name

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2Images:
    def test_describe_images_with_owner(self, ec2):
        """DescribeImages should work with owner filter."""
        response = ec2.describe_images(Owners=["amazon"])
        assert "Images" in response


class TestEC2MultipleSubnets:
    def test_create_multiple_subnets(self, ec2):
        """Create multiple subnets in a VPC and verify."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.89.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        subnet_ids = []
        try:
            for i in range(3):
                s = ec2.create_subnet(VpcId=vpc_id, CidrBlock=f"10.89.{i}.0/24")
                subnet_ids.append(s["Subnet"]["SubnetId"])

            described = ec2.describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )
            found_ids = [s["SubnetId"] for s in described["Subnets"]]
            for sid in subnet_ids:
                assert sid in found_ids
        finally:
            for sid in subnet_ids:
                ec2.delete_subnet(SubnetId=sid)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2InstanceTypes:
    def test_describe_instance_types(self, ec2):
        """DescribeInstanceTypes returns results."""
        response = ec2.describe_instance_types()
        assert len(response["InstanceTypes"]) > 0

    def test_describe_images_owner_alias(self, ec2):
        """DescribeImages with owner-alias=amazon filter."""
        response = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        assert "Images" in response


class TestEC2RunInstances:
    def test_run_and_terminate_instances(self, ec2):
        """RunInstances / TerminateInstances with t2.micro."""
        # Get an AMI to use
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            # Fallback: use any available image
            images = ec2.describe_images()
        assert len(images["Images"]) > 0
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        try:
            assert instance_id.startswith("i-")
            assert resp["Instances"][0]["InstanceType"] == "t2.micro"
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_describe_instances_with_filters(self, ec2):
        """DescribeInstances with instance-state-name and tag:Name filters."""
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]
        tag_name = _unique("filter-test")

        resp = ec2.run_instances(
            ImageId=ami_id,
            InstanceType="t2.micro",
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": tag_name}],
                }
            ],
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        try:
            # Filter by state
            by_state = ec2.describe_instances(
                Filters=[{"Name": "instance-state-name", "Values": ["running", "pending"]}]
            )
            all_ids = [
                i["InstanceId"]
                for r in by_state["Reservations"]
                for i in r["Instances"]
            ]
            assert instance_id in all_ids

            # Filter by tag:Name
            by_tag = ec2.describe_instances(
                Filters=[{"Name": "tag:Name", "Values": [tag_name]}]
            )
            tag_ids = [
                i["InstanceId"]
                for r in by_tag["Reservations"]
                for i in r["Instances"]
            ]
            assert instance_id in tag_ids
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_describe_instance_status(self, ec2):
        """DescribeInstanceStatus returns valid response."""
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        try:
            status_resp = ec2.describe_instance_status(
                InstanceIds=[instance_id], IncludeAllInstances=True
            )
            assert "InstanceStatuses" in status_resp
            ids = [s["InstanceId"] for s in status_resp["InstanceStatuses"]]
            assert instance_id in ids
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_modify_instance_attribute(self, ec2):
        """ModifyInstanceAttribute to change instance type."""
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        try:
            # Stop instance first (required for modifying instance type)
            ec2.stop_instances(InstanceIds=[instance_id])
            waiter = ec2.get_waiter("instance_stopped")
            waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 1, "MaxAttempts": 10})
            ec2.modify_instance_attribute(
                InstanceId=instance_id, InstanceType={"Value": "t2.small"}
            )
            desc = ec2.describe_instances(InstanceIds=[instance_id])
            inst = desc["Reservations"][0]["Instances"][0]
            assert inst["InstanceType"] == "t2.small"
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])


class TestEC2Tags:
    def test_create_describe_delete_tags(self, ec2):
        """CreateTags / DescribeTags / DeleteTags lifecycle."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.80.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            ec2.create_tags(
                Resources=[vpc_id],
                Tags=[
                    {"Key": "Env", "Value": "test"},
                    {"Key": "Project", "Value": "robotocore"},
                ],
            )

            tags_resp = ec2.describe_tags(
                Filters=[{"Name": "resource-id", "Values": [vpc_id]}]
            )
            tag_keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "Env" in tag_keys
            assert "Project" in tag_keys

            ec2.delete_tags(
                Resources=[vpc_id], Tags=[{"Key": "Env"}]
            )
            tags_resp2 = ec2.describe_tags(
                Filters=[{"Name": "resource-id", "Values": [vpc_id]}]
            )
            tag_keys2 = [t["Key"] for t in tags_resp2["Tags"]]
            assert "Env" not in tag_keys2
            assert "Project" in tag_keys2
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2Volumes:
    def test_create_describe_delete_volume(self, ec2):
        """CreateVolume / DescribeVolumes / DeleteVolume."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10)
        vol_id = vol["VolumeId"]
        try:
            assert vol_id.startswith("vol-")
            assert vol["Size"] == 10

            described = ec2.describe_volumes(VolumeIds=[vol_id])
            assert len(described["Volumes"]) == 1
            assert described["Volumes"][0]["VolumeId"] == vol_id
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    @pytest.mark.xfail(reason="AttachVolume/DetachVolume may require running instance state")
    def test_attach_detach_volume(self, ec2):
        """AttachVolume / DetachVolume lifecycle."""
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        inst_resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = inst_resp["Instances"][0]["InstanceId"]
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10)
        vol_id = vol["VolumeId"]
        try:
            attach = ec2.attach_volume(
                VolumeId=vol_id, InstanceId=instance_id, Device="/dev/sdf"
            )
            assert attach["State"] in ("attaching", "attached")

            ec2.detach_volume(VolumeId=vol_id)
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])
            try:
                ec2.delete_volume(VolumeId=vol_id)
            except Exception:
                pass


class TestEC2Snapshots:
    def test_create_describe_delete_snapshot(self, ec2):
        """CreateSnapshot / DescribeSnapshots / DeleteSnapshot."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id, Description="test snapshot")
            snap_id = snap["SnapshotId"]
            assert snap_id.startswith("snap-")

            described = ec2.describe_snapshots(SnapshotIds=[snap_id])
            assert len(described["Snapshots"]) == 1
            assert described["Snapshots"][0]["VolumeId"] == vol_id

            ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2AMIs:
    def test_create_describe_deregister_image(self, ec2):
        """CreateImage / DescribeImages / DeregisterImage."""
        images = ec2.describe_images(
            Filters=[{"Name": "owner-alias", "Values": ["amazon"]}]
        )
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        inst_resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = inst_resp["Instances"][0]["InstanceId"]
        try:
            image_resp = ec2.create_image(
                InstanceId=instance_id, Name=_unique("test-ami")
            )
            new_ami_id = image_resp["ImageId"]
            assert new_ami_id.startswith("ami-")

            described = ec2.describe_images(ImageIds=[new_ami_id])
            assert len(described["Images"]) == 1

            ec2.deregister_image(ImageId=new_ami_id)
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])


class TestEC2InternetGatewayFull:
    def test_create_attach_detach_delete_igw(self, ec2):
        """Full IGW lifecycle: create, attach to VPC, detach, delete."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.70.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        try:
            ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            attachments = described["InternetGateways"][0]["Attachments"]
            assert any(a["VpcId"] == vpc_id for a in attachments)

            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        finally:
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2RouteTables:
    def test_create_describe_delete_route_table(self, ec2):
        """CreateRouteTable / DescribeRouteTables / DeleteRouteTable."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.60.0.0/16")
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
        """CreateRoute / DeleteRoute in a route table."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.61.0.0/16")
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
            routes = described["RouteTables"][0]["Routes"]
            dest_cidrs = [r.get("DestinationCidrBlock") for r in routes]
            assert "0.0.0.0/0" in dest_cidrs

            ec2.delete_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0")
            ec2.delete_route_table(RouteTableId=rt_id)
        finally:
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_associate_disassociate_route_table(self, ec2):
        """AssociateRouteTable / DisassociateRouteTable."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.62.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.62.1.0/24")
        subnet_id = subnet_resp["Subnet"]["SubnetId"]
        try:
            rt_resp = ec2.create_route_table(VpcId=vpc_id)
            rt_id = rt_resp["RouteTable"]["RouteTableId"]

            assoc = ec2.associate_route_table(
                RouteTableId=rt_id, SubnetId=subnet_id
            )
            assoc_id = assoc["AssociationId"]
            assert assoc_id.startswith("rtbassoc-")

            ec2.disassociate_route_table(AssociationId=assoc_id)
            ec2.delete_route_table(RouteTableId=rt_id)
        finally:
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NatGateway:
    def test_create_describe_delete_nat_gateway(self, ec2):
        """CreateNatGateway / DescribeNatGateways / DeleteNatGateway."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.50.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.50.1.0/24")
        subnet_id = subnet_resp["Subnet"]["SubnetId"]
        alloc = ec2.allocate_address(Domain="vpc")
        alloc_id = alloc["AllocationId"]
        try:
            nat = ec2.create_nat_gateway(
                SubnetId=subnet_id, AllocationId=alloc_id
            )
            nat_id = nat["NatGateway"]["NatGatewayId"]
            assert nat_id.startswith("nat-")

            described = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
            assert len(described["NatGateways"]) == 1

            ec2.delete_nat_gateway(NatGatewayId=nat_id)
        finally:
            ec2.release_address(AllocationId=alloc_id)
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NetworkInterfaces:
    def test_create_describe_delete_network_interface(self, ec2):
        """CreateNetworkInterface / DescribeNetworkInterfaces / DeleteNetworkInterface."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.40.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.40.1.0/24")
        subnet_id = subnet_resp["Subnet"]["SubnetId"]
        try:
            eni = ec2.create_network_interface(SubnetId=subnet_id)
            eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
            assert eni_id.startswith("eni-")

            described = ec2.describe_network_interfaces(
                NetworkInterfaceIds=[eni_id]
            )
            assert len(described["NetworkInterfaces"]) == 1
            assert described["NetworkInterfaces"][0]["SubnetId"] == subnet_id

            ec2.delete_network_interface(NetworkInterfaceId=eni_id)
        finally:
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2LaunchTemplates:
    def test_create_describe_delete_launch_template(self, ec2):
        """CreateLaunchTemplate / DescribeLaunchTemplates / DeleteLaunchTemplate."""
        lt_name = _unique("lt")
        resp = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            assert lt_id.startswith("lt-")
            assert resp["LaunchTemplate"]["LaunchTemplateName"] == lt_name

            described = ec2.describe_launch_templates(
                LaunchTemplateIds=[lt_id]
            )
            assert len(described["LaunchTemplates"]) == 1
            assert described["LaunchTemplates"][0]["LaunchTemplateName"] == lt_name
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)


class TestEC2PlacementGroups:
    @pytest.mark.xfail(reason="CreatePlacementGroup may not be implemented")
    def test_create_describe_delete_placement_group(self, ec2):
        """CreatePlacementGroup / DescribePlacementGroups / DeletePlacementGroup."""
        pg_name = _unique("pg")
        ec2.create_placement_group(GroupName=pg_name, Strategy="cluster")
        try:
            described = ec2.describe_placement_groups(GroupNames=[pg_name])
            assert len(described["PlacementGroups"]) == 1
            assert described["PlacementGroups"][0]["GroupName"] == pg_name
            assert described["PlacementGroups"][0]["Strategy"] == "cluster"
        finally:
            ec2.delete_placement_group(GroupName=pg_name)
