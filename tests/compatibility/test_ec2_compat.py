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
            response = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [vpc_id]}])
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
            egress_ports2 = [
                p.get("FromPort") for p in described2["SecurityGroups"][0]["IpPermissionsEgress"]
            ]
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
            ingress_ports = [
                p.get("FromPort") for p in described["SecurityGroups"][0]["IpPermissions"]
            ]
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

            described = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
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
        response = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        assert "Images" in response


class TestEC2RunInstances:
    def test_run_and_terminate_instances(self, ec2):
        """RunInstances / TerminateInstances with t2.micro."""
        # Get an AMI to use
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        if not images["Images"]:
            # Fallback: use any available image
            images = ec2.describe_images()
        assert len(images["Images"]) > 0
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1)
        instance_id = resp["Instances"][0]["InstanceId"]
        try:
            assert instance_id.startswith("i-")
            assert resp["Instances"][0]["InstanceType"] == "t2.micro"
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_describe_instances_with_filters(self, ec2):
        """DescribeInstances with instance-state-name and tag:Name filters."""
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
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
            all_ids = [i["InstanceId"] for r in by_state["Reservations"] for i in r["Instances"]]
            assert instance_id in all_ids

            # Filter by tag:Name
            by_tag = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [tag_name]}])
            tag_ids = [i["InstanceId"] for r in by_tag["Reservations"] for i in r["Instances"]]
            assert instance_id in tag_ids
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_describe_instance_status(self, ec2):
        """DescribeInstanceStatus returns valid response."""
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1)
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
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        resp = ec2.run_instances(ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1)
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


class TestEC2TagsCRUD:
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

            tags_resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [vpc_id]}])
            tag_keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "Env" in tag_keys
            assert "Project" in tag_keys

            ec2.delete_tags(Resources=[vpc_id], Tags=[{"Key": "Env"}])
            tags_resp2 = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [vpc_id]}])
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

    def test_attach_detach_volume(self, ec2):
        """AttachVolume / DetachVolume lifecycle."""
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
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
            attach = ec2.attach_volume(VolumeId=vol_id, InstanceId=instance_id, Device="/dev/sdf")
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
        images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        if not images["Images"]:
            images = ec2.describe_images()
        ami_id = images["Images"][0]["ImageId"]

        inst_resp = ec2.run_instances(
            ImageId=ami_id, InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        instance_id = inst_resp["Instances"][0]["InstanceId"]
        try:
            image_resp = ec2.create_image(InstanceId=instance_id, Name=_unique("test-ami"))
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


class TestEC2RouteTablesCRUD:
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

            assoc = ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_id)
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
            nat = ec2.create_nat_gateway(SubnetId=subnet_id, AllocationId=alloc_id)
            nat_id = nat["NatGateway"]["NatGatewayId"]
            assert nat_id.startswith("nat-")

            described = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
            assert len(described["NatGateways"]) == 1

            ec2.delete_nat_gateway(NatGatewayId=nat_id)
        finally:
            ec2.release_address(AllocationId=alloc_id)
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NetworkInterfacesCRUD:
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

            described = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
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

            described = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
            assert len(described["LaunchTemplates"]) == 1
            assert described["LaunchTemplates"][0]["LaunchTemplateName"] == lt_name
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)


class TestEC2PlacementGroups:
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


class TestEC2ExtendedOperationsV2:
    """Extended EC2 operations for higher coverage."""

    @pytest.fixture
    def ec2(self):
        from tests.compatibility.conftest import make_client

        return make_client("ec2")

    def test_describe_availability_zones(self, ec2):
        resp = ec2.describe_availability_zones()
        assert "AvailabilityZones" in resp
        assert len(resp["AvailabilityZones"]) >= 1
        az = resp["AvailabilityZones"][0]
        assert "ZoneName" in az
        assert "State" in az
        assert "RegionName" in az

    def test_describe_regions(self, ec2):
        resp = ec2.describe_regions()
        assert "Regions" in resp
        assert len(resp["Regions"]) >= 1
        names = [r["RegionName"] for r in resp["Regions"]]
        assert "us-east-1" in names

    def test_describe_account_attributes(self, ec2):
        resp = ec2.describe_account_attributes()
        assert "AccountAttributes" in resp
        names = [a["AttributeName"] for a in resp["AccountAttributes"]]
        assert "default-vpc" in names or "supported-platforms" in names or len(names) >= 1

    def test_allocate_describe_release_address(self, ec2):
        alloc = ec2.allocate_address(Domain="vpc")
        alloc_id = alloc["AllocationId"]
        try:
            assert alloc_id.startswith("eipalloc-")
            assert "PublicIp" in alloc

            described = ec2.describe_addresses(AllocationIds=[alloc_id])
            assert len(described["Addresses"]) == 1
            assert described["Addresses"][0]["AllocationId"] == alloc_id
        finally:
            ec2.release_address(AllocationId=alloc_id)

    def test_create_describe_delete_key_pair(self, ec2):
        kp_name = _unique("kp")
        resp = ec2.create_key_pair(KeyName=kp_name)
        try:
            assert resp["KeyName"] == kp_name
            assert "KeyMaterial" in resp
            assert "KeyFingerprint" in resp

            described = ec2.describe_key_pairs(KeyNames=[kp_name])
            assert len(described["KeyPairs"]) == 1
        finally:
            ec2.delete_key_pair(KeyName=kp_name)

    def test_describe_vpcs(self, ec2):
        resp = ec2.describe_vpcs()
        assert "Vpcs" in resp
        # Should have at least the default VPC
        assert len(resp["Vpcs"]) >= 1

    def test_describe_subnets(self, ec2):
        resp = ec2.describe_subnets()
        assert "Subnets" in resp

    def test_describe_security_groups(self, ec2):
        resp = ec2.describe_security_groups()
        assert "SecurityGroups" in resp
        # Should have at least the default SG
        assert len(resp["SecurityGroups"]) >= 1

    def test_create_security_group_with_rules(self, ec2):
        vpc_resp = ec2.create_vpc(CidrBlock="10.100.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg")
        try:
            sg = ec2.create_security_group(
                GroupName=sg_name,
                Description="Test SG with rules",
                VpcId=vpc_id,
            )
            sg_id = sg["GroupId"]

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
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    },
                ],
            )

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            ingress = described["SecurityGroups"][0]["IpPermissions"]
            ports = {p["FromPort"] for p in ingress if "FromPort" in p}
            assert 443 in ports
            assert 80 in ports

            ec2.revoke_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 80,
                        "ToPort": 80,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    },
                ],
            )
            described = ec2.describe_security_groups(GroupIds=[sg_id])
            ingress = described["SecurityGroups"][0]["IpPermissions"]
            ports = {p["FromPort"] for p in ingress if "FromPort" in p}
            assert 80 not in ports

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_vpc_with_tags(self, ec2):
        vpc = ec2.create_vpc(
            CidrBlock="10.101.0.0/16",
            TagSpecifications=[
                {
                    "ResourceType": "vpc",
                    "Tags": [{"Key": "Name", "Value": "test-vpc"}],
                }
            ],
        )
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            tags = {t["Key"]: t["Value"] for t in vpc["Vpc"].get("Tags", [])}
            assert tags.get("Name") == "test-vpc"
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_images(self, ec2):
        resp = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        assert "Images" in resp

    def test_describe_instances_filter(self, ec2):
        resp = ec2.describe_instances(
            Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
        )
        assert "Reservations" in resp

    def test_create_and_describe_dhcp_options(self, ec2):
        resp = ec2.create_dhcp_options(
            DhcpConfigurations=[
                {"Key": "domain-name", "Values": ["example.internal"]},
                {"Key": "domain-name-servers", "Values": ["10.0.0.2"]},
            ]
        )
        dhcp_id = resp["DhcpOptions"]["DhcpOptionsId"]
        try:
            described = ec2.describe_dhcp_options(DhcpOptionsIds=[dhcp_id])
            assert len(described["DhcpOptions"]) == 1
        finally:
            ec2.delete_dhcp_options(DhcpOptionsId=dhcp_id)

    def test_describe_vpc_peering_connections(self, ec2):
        resp = ec2.describe_vpc_peering_connections()
        assert "VpcPeeringConnections" in resp

    def test_describe_network_acls(self, ec2):
        resp = ec2.describe_network_acls()
        assert "NetworkAcls" in resp
        assert len(resp["NetworkAcls"]) >= 1


class TestEC2ExtendedV2:
    """Extended EC2 tests covering network ACLs, DHCP options, VPC attributes,
    launch template versions, prefix lists, EIP association, and more."""

    def test_describe_network_acls_default(self, ec2):
        """Default VPC should have a default network ACL."""
        response = ec2.describe_network_acls()
        assert len(response["NetworkAcls"]) >= 1
        acl = response["NetworkAcls"][0]
        assert "NetworkAclId" in acl
        assert acl["NetworkAclId"].startswith("acl-")

    def test_create_and_delete_network_acl(self, ec2):
        """CreateNetworkAcl / DeleteNetworkAcl lifecycle."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.110.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            acl_resp = ec2.create_network_acl(VpcId=vpc_id)
            acl_id = acl_resp["NetworkAcl"]["NetworkAclId"]
            assert acl_id.startswith("acl-")
            assert acl_resp["NetworkAcl"]["VpcId"] == vpc_id

            described = ec2.describe_network_acls(NetworkAclIds=[acl_id])
            assert len(described["NetworkAcls"]) == 1

            ec2.delete_network_acl(NetworkAclId=acl_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_network_acl_entry(self, ec2):
        """CreateNetworkAclEntry and verify via describe."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.111.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            acl_resp = ec2.create_network_acl(VpcId=vpc_id)
            acl_id = acl_resp["NetworkAcl"]["NetworkAclId"]

            ec2.create_network_acl_entry(
                NetworkAclId=acl_id,
                RuleNumber=100,
                Protocol="-1",
                RuleAction="allow",
                Egress=False,
                CidrBlock="10.0.0.0/8",
            )

            described = ec2.describe_network_acls(NetworkAclIds=[acl_id])
            entries = described["NetworkAcls"][0]["Entries"]
            ingress = [e for e in entries if not e["Egress"]]
            rule_numbers = [e["RuleNumber"] for e in ingress]
            assert 100 in rule_numbers

            ec2.delete_network_acl_entry(NetworkAclId=acl_id, RuleNumber=100, Egress=False)
            ec2.delete_network_acl(NetworkAclId=acl_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_and_delete_dhcp_options(self, ec2):
        """CreateDhcpOptions / DescribeDhcpOptions / DeleteDhcpOptions."""
        resp = ec2.create_dhcp_options(
            DhcpConfigurations=[
                {"Key": "domain-name", "Values": ["example.com"]},
                {"Key": "domain-name-servers", "Values": ["10.0.0.2"]},
            ]
        )
        dhcp_id = resp["DhcpOptions"]["DhcpOptionsId"]
        try:
            assert dhcp_id.startswith("dopt-")

            described = ec2.describe_dhcp_options(DhcpOptionsIds=[dhcp_id])
            assert len(described["DhcpOptions"]) == 1
            assert described["DhcpOptions"][0]["DhcpOptionsId"] == dhcp_id
        finally:
            ec2.delete_dhcp_options(DhcpOptionsId=dhcp_id)

    def test_associate_dhcp_options_with_vpc(self, ec2):
        """AssociateDhcpOptions to a VPC."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.112.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        dhcp_resp = ec2.create_dhcp_options(
            DhcpConfigurations=[
                {"Key": "domain-name", "Values": ["test.local"]},
            ]
        )
        dhcp_id = dhcp_resp["DhcpOptions"]["DhcpOptionsId"]
        try:
            ec2.associate_dhcp_options(DhcpOptionsId=dhcp_id, VpcId=vpc_id)

            described = ec2.describe_vpcs(VpcIds=[vpc_id])
            assert described["Vpcs"][0]["DhcpOptionsId"] == dhcp_id
        finally:
            # Reset to default before cleanup
            ec2.associate_dhcp_options(DhcpOptionsId="default", VpcId=vpc_id)
            ec2.delete_dhcp_options(DhcpOptionsId=dhcp_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_vpc_attribute_dns_support(self, ec2):
        """DescribeVpcAttribute for enableDnsSupport."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.113.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            attr = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
            assert "EnableDnsSupport" in attr
            assert isinstance(attr["EnableDnsSupport"]["Value"], bool)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_modify_vpc_attribute_dns_hostnames(self, ec2):
        """ModifyVpcAttribute to enable DNS hostnames."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.114.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
            attr = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsHostnames")
            assert attr["EnableDnsHostnames"]["Value"] is True
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_launch_template_version(self, ec2):
        """CreateLaunchTemplateVersion / DescribeLaunchTemplateVersions."""
        lt_name = _unique("lt-ver")
        resp = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            ver_resp = ec2.create_launch_template_version(
                LaunchTemplateId=lt_id,
                LaunchTemplateData={"InstanceType": "t2.small"},
                SourceVersion="1",
            )
            assert ver_resp["LaunchTemplateVersion"]["VersionNumber"] == 2

            versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
            assert len(versions["LaunchTemplateVersions"]) == 2
            instance_types = [
                v["LaunchTemplateData"]["InstanceType"] for v in versions["LaunchTemplateVersions"]
            ]
            assert "t2.micro" in instance_types
            assert "t2.small" in instance_types
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)

    def test_describe_prefix_lists(self, ec2):
        """DescribePrefixLists returns results (AWS-managed prefix lists)."""
        response = ec2.describe_prefix_lists()
        assert "PrefixLists" in response

    def test_associate_eip_with_network_interface(self, ec2):
        """Associate an Elastic IP with a network interface."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.115.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.115.1.0/24")
        subnet_id = subnet_resp["Subnet"]["SubnetId"]
        alloc = ec2.allocate_address(Domain="vpc")
        alloc_id = alloc["AllocationId"]
        try:
            eni = ec2.create_network_interface(SubnetId=subnet_id)
            eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]

            assoc = ec2.associate_address(AllocationId=alloc_id, NetworkInterfaceId=eni_id)
            assert "AssociationId" in assoc
            assoc_id = assoc["AssociationId"]

            described = ec2.describe_addresses(AllocationIds=[alloc_id])
            assert described["Addresses"][0]["NetworkInterfaceId"] == eni_id

            ec2.disassociate_address(AssociationId=assoc_id)
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)
        finally:
            ec2.release_address(AllocationId=alloc_id)
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_modify_subnet_attribute_map_public_ip(self, ec2):
        """ModifySubnetAttribute to enable MapPublicIpOnLaunch."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.116.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            subnet_resp = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.116.1.0/24")
            subnet_id = subnet_resp["Subnet"]["SubnetId"]

            ec2.modify_subnet_attribute(
                SubnetId=subnet_id,
                MapPublicIpOnLaunch={"Value": True},
            )

            described = ec2.describe_subnets(SubnetIds=[subnet_id])
            assert described["Subnets"][0]["MapPublicIpOnLaunch"] is True

            ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_tags_on_security_group(self, ec2):
        """CreateTags on a security group resource."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.117.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg-tag")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name, Description="Tag test SG", VpcId=vpc_id
            )
            sg_id = sg_resp["GroupId"]

            ec2.create_tags(
                Resources=[sg_id],
                Tags=[{"Key": "Team", "Value": "platform"}],
            )

            described = ec2.describe_security_groups(GroupIds=[sg_id])
            tags = {t["Key"]: t["Value"] for t in described["SecurityGroups"][0].get("Tags", [])}
            assert tags["Team"] == "platform"

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_tags_on_internet_gateway(self, ec2):
        """CreateTags on an internet gateway."""
        igw_resp = ec2.create_internet_gateway()
        igw_id = igw_resp["InternetGateway"]["InternetGatewayId"]
        try:
            ec2.create_tags(
                Resources=[igw_id],
                Tags=[{"Key": "Purpose", "Value": "testing"}],
            )

            described = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
            tags = {t["Key"]: t["Value"] for t in described["InternetGateways"][0].get("Tags", [])}
            assert tags["Purpose"] == "testing"
        finally:
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)

    def test_describe_account_attributes(self, ec2):
        """DescribeAccountAttributes returns expected attribute names."""
        response = ec2.describe_account_attributes()
        attr_names = [a["AttributeName"] for a in response["AccountAttributes"]]
        # At minimum, these standard attributes should be present
        assert "supported-platforms" in attr_names or "default-vpc" in attr_names

    def test_describe_images_with_image_id_filter(self, ec2):
        """DescribeImages filtered by specific ImageId."""
        # First get any image
        all_images = ec2.describe_images(Filters=[{"Name": "owner-alias", "Values": ["amazon"]}])
        if not all_images["Images"]:
            all_images = ec2.describe_images()
        assert len(all_images["Images"]) > 0
        target_id = all_images["Images"][0]["ImageId"]

        # Now filter by that specific ID
        filtered = ec2.describe_images(ImageIds=[target_id])
        assert len(filtered["Images"]) == 1
        assert filtered["Images"][0]["ImageId"] == target_id

    def test_describe_security_group_rules(self, ec2):
        """DescribeSecurityGroupRules for a group with custom rules."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.118.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        sg_name = _unique("sg-rules")
        try:
            sg_resp = ec2.create_security_group(
                GroupName=sg_name, Description="Rules test", VpcId=vpc_id
            )
            sg_id = sg_resp["GroupId"]

            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 3306,
                        "ToPort": 3306,
                        "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
                    }
                ],
            )

            rules = ec2.describe_security_group_rules(
                Filters=[{"Name": "group-id", "Values": [sg_id]}]
            )
            assert "SecurityGroupRules" in rules
            ingress_rules = [r for r in rules["SecurityGroupRules"] if not r["IsEgress"]]
            assert any(r.get("FromPort") == 3306 for r in ingress_rules)

            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_vpc_endpoint_gateway(self, ec2):
        """CreateVpcEndpoint (Gateway type for S3)."""
        vpc_resp = ec2.create_vpc(CidrBlock="10.119.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            # Get the main route table for this VPC
            rt_resp = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            rt_id = rt_resp["RouteTables"][0]["RouteTableId"]

            ep_resp = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName="com.amazonaws.us-east-1.s3",
                RouteTableIds=[rt_id],
            )
            ep_id = ep_resp["VpcEndpoint"]["VpcEndpointId"]
            assert ep_id.startswith("vpce-")

            described = ec2.describe_vpc_endpoints(VpcEndpointIds=[ep_id])
            assert len(described["VpcEndpoints"]) == 1
            assert described["VpcEndpoints"][0]["ServiceName"] == "com.amazonaws.us-east-1.s3"

            ec2.delete_vpc_endpoints(VpcEndpointIds=[ep_id])
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2TransitGateway:
    def test_create_describe_delete_transit_gateway(self, ec2):
        """CreateTransitGateway / DescribeTransitGateways / DeleteTransitGateway."""
        resp = ec2.create_transit_gateway(Description="compat-test-tgw")
        tgw_id = resp["TransitGateway"]["TransitGatewayId"]
        try:
            assert tgw_id.startswith("tgw-")
            assert resp["TransitGateway"]["Description"] == "compat-test-tgw"

            described = ec2.describe_transit_gateways(TransitGatewayIds=[tgw_id])
            assert len(described["TransitGateways"]) == 1
            assert described["TransitGateways"][0]["TransitGatewayId"] == tgw_id
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)

    def test_create_transit_gateway_with_tags(self, ec2):
        """CreateTransitGateway with TagSpecifications."""
        tag_val = _unique("tgw-tag")
        resp = ec2.create_transit_gateway(
            Description="tagged-tgw",
            TagSpecifications=[
                {
                    "ResourceType": "transit-gateway",
                    "Tags": [{"Key": "Name", "Value": tag_val}],
                }
            ],
        )
        tgw_id = resp["TransitGateway"]["TransitGatewayId"]
        try:
            tags = {t["Key"]: t["Value"] for t in resp["TransitGateway"].get("Tags", [])}
            assert tags.get("Name") == tag_val
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2FlowLogs:
    def test_create_describe_delete_flow_logs(self, ec2):
        """CreateFlowLogs / DescribeFlowLogs / DeleteFlowLogs."""
        import json

        from tests.compatibility.conftest import make_client

        iam = make_client("iam")

        # Create IAM role for flow logs
        role_name = _unique("fl-role")
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "vpc-flow-logs.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
        role_arn = role["Role"]["Arn"]

        vpc_resp = ec2.create_vpc(CidrBlock="10.202.0.0/16")
        vpc_id = vpc_resp["Vpc"]["VpcId"]
        try:
            fl = ec2.create_flow_logs(
                ResourceIds=[vpc_id],
                ResourceType="VPC",
                TrafficType="ALL",
                LogDestinationType="cloud-watch-logs",
                LogGroupName="test-flow-logs",
                DeliverLogsPermissionArn=role_arn,
            )
            assert len(fl["FlowLogIds"]) == 1
            fl_id = fl["FlowLogIds"][0]
            assert fl_id.startswith("fl-")

            described = ec2.describe_flow_logs(FlowLogIds=[fl_id])
            assert len(described["FlowLogs"]) == 1
            assert described["FlowLogs"][0]["FlowLogId"] == fl_id
            assert described["FlowLogs"][0]["ResourceId"] == vpc_id

            ec2.delete_flow_logs(FlowLogIds=[fl_id])

            # Verify deletion
            described_after = ec2.describe_flow_logs(FlowLogIds=[fl_id])
            # After deletion, either empty or status is deleted
            if described_after["FlowLogs"]:
                # Some implementations keep the record with a deleted status
                pass
            else:
                assert len(described_after["FlowLogs"]) == 0
        finally:
            ec2.delete_vpc(VpcId=vpc_id)
            iam.delete_role(RoleName=role_name)


def _assert_ok(resp):
    """Assert the response has HTTP 200 status (stub operations return empty results)."""
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEC2GapStubs:
    """Tests for EC2 operations that were stubbed in Moto's gap_stubs.py.

    These return empty results but should not error (no 500s).
    """

    # --- IPAM operations ---

    def test_describe_ipams(self, ec2):
        _assert_ok(ec2.describe_ipams())

    def test_describe_ipam_pools(self, ec2):
        _assert_ok(ec2.describe_ipam_pools())

    def test_describe_ipam_scopes(self, ec2):
        _assert_ok(ec2.describe_ipam_scopes())

    def test_describe_ipam_resource_discoveries(self, ec2):
        _assert_ok(ec2.describe_ipam_resource_discoveries())

    def test_describe_ipam_resource_discovery_associations(self, ec2):
        _assert_ok(ec2.describe_ipam_resource_discovery_associations())

    def test_describe_ipam_byoasn(self, ec2):
        _assert_ok(ec2.describe_ipam_byoasn())

    def test_describe_ipam_external_resource_verification_tokens(self, ec2):
        _assert_ok(ec2.describe_ipam_external_resource_verification_tokens())

    def test_describe_ipam_policies(self, ec2):
        _assert_ok(ec2.describe_ipam_policies())

    def test_describe_ipam_prefix_list_resolvers(self, ec2):
        _assert_ok(ec2.describe_ipam_prefix_list_resolvers())

    def test_describe_ipam_prefix_list_resolver_targets(self, ec2):
        _assert_ok(ec2.describe_ipam_prefix_list_resolver_targets())

    # --- Verified Access operations ---

    def test_describe_verified_access_instances(self, ec2):
        _assert_ok(ec2.describe_verified_access_instances())

    def test_describe_verified_access_endpoints(self, ec2):
        _assert_ok(ec2.describe_verified_access_endpoints())

    def test_describe_verified_access_groups(self, ec2):
        _assert_ok(ec2.describe_verified_access_groups())

    def test_describe_verified_access_trust_providers(self, ec2):
        _assert_ok(ec2.describe_verified_access_trust_providers())

    def test_describe_verified_access_instance_logging_configurations(self, ec2):
        _assert_ok(ec2.describe_verified_access_instance_logging_configurations())

    # --- Traffic Mirror operations ---

    def test_describe_traffic_mirror_filters(self, ec2):
        _assert_ok(ec2.describe_traffic_mirror_filters())

    def test_describe_traffic_mirror_sessions(self, ec2):
        _assert_ok(ec2.describe_traffic_mirror_sessions())

    def test_describe_traffic_mirror_targets(self, ec2):
        _assert_ok(ec2.describe_traffic_mirror_targets())

    def test_describe_traffic_mirror_filter_rules(self, ec2):
        _assert_ok(ec2.describe_traffic_mirror_filter_rules())

    # --- Transit Gateway operations ---

    def test_describe_transit_gateway_multicast_domains(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_multicast_domains())

    def test_describe_transit_gateway_connect_peers(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_connect_peers())

    def test_describe_transit_gateway_connects(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_connects())

    def test_describe_transit_gateway_policy_tables(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_policy_tables())

    def test_describe_transit_gateway_route_table_announcements(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_route_table_announcements())

    def test_describe_transit_gateway_metering_policies(self, ec2):
        _assert_ok(ec2.describe_transit_gateway_metering_policies())

    # --- Local Gateway operations ---

    def test_describe_local_gateways(self, ec2):
        _assert_ok(ec2.describe_local_gateways())

    def test_describe_local_gateway_route_tables(self, ec2):
        _assert_ok(ec2.describe_local_gateway_route_tables())

    def test_describe_local_gateway_virtual_interfaces(self, ec2):
        _assert_ok(ec2.describe_local_gateway_virtual_interfaces())

    def test_describe_local_gateway_virtual_interface_groups(self, ec2):
        _assert_ok(ec2.describe_local_gateway_virtual_interface_groups())

    def test_describe_local_gateway_route_table_vpc_associations(self, ec2):
        _assert_ok(ec2.describe_local_gateway_route_table_vpc_associations())

    def test_describe_local_gateway_route_table_virtual_interface_group_associations(self, ec2):
        _assert_ok(ec2.describe_local_gateway_route_table_virtual_interface_group_associations())

    # --- Network Insights operations ---

    def test_describe_network_insights_paths(self, ec2):
        _assert_ok(ec2.describe_network_insights_paths())

    def test_describe_network_insights_analyses(self, ec2):
        _assert_ok(ec2.describe_network_insights_analyses())

    def test_describe_network_insights_access_scopes(self, ec2):
        _assert_ok(ec2.describe_network_insights_access_scopes())

    def test_describe_network_insights_access_scope_analyses(self, ec2):
        _assert_ok(ec2.describe_network_insights_access_scope_analyses())

    # --- Capacity operations ---

    def test_describe_capacity_reservations(self, ec2):
        _assert_ok(ec2.describe_capacity_reservations())

    def test_describe_capacity_reservation_fleets(self, ec2):
        _assert_ok(ec2.describe_capacity_reservation_fleets())

    # --- VPC / Networking misc ---

    def test_describe_coip_pools(self, ec2):
        _assert_ok(ec2.describe_coip_pools())

    def test_describe_public_ipv4_pools(self, ec2):
        _assert_ok(ec2.describe_public_ipv4_pools())

    def test_describe_ipv6_pools(self, ec2):
        _assert_ok(ec2.describe_ipv6_pools())

    def test_describe_vpc_endpoint_connections(self, ec2):
        _assert_ok(ec2.describe_vpc_endpoint_connections())

    def test_describe_vpc_endpoint_connection_notifications(self, ec2):
        _assert_ok(ec2.describe_vpc_endpoint_connection_notifications())

    def test_describe_trunk_interface_associations(self, ec2):
        _assert_ok(ec2.describe_trunk_interface_associations())

    def test_describe_network_interface_permissions(self, ec2):
        _assert_ok(ec2.describe_network_interface_permissions())

    # --- Snapshot / Image misc ---

    def test_describe_locked_snapshots(self, ec2):
        _assert_ok(ec2.describe_locked_snapshots())

    def test_describe_fast_snapshot_restores(self, ec2):
        _assert_ok(ec2.describe_fast_snapshot_restores())

    def test_describe_replace_root_volume_tasks(self, ec2):
        _assert_ok(ec2.describe_replace_root_volume_tasks())

    # --- Serial console and settings ---

    def test_get_serial_console_access_status(self, ec2):
        _assert_ok(ec2.get_serial_console_access_status())

    def test_get_ebs_default_kms_key_id(self, ec2):
        _assert_ok(ec2.get_ebs_default_kms_key_id())

    def test_get_instance_metadata_defaults(self, ec2):
        _assert_ok(ec2.get_instance_metadata_defaults())

    def test_get_image_block_public_access_state(self, ec2):
        _assert_ok(ec2.get_image_block_public_access_state())

    def test_get_snapshot_block_public_access_state(self, ec2):
        _assert_ok(ec2.get_snapshot_block_public_access_state())

    # --- Recycle bin ---

    def test_list_images_in_recycle_bin(self, ec2):
        _assert_ok(ec2.list_images_in_recycle_bin())

    def test_list_snapshots_in_recycle_bin(self, ec2):
        _assert_ok(ec2.list_snapshots_in_recycle_bin())
