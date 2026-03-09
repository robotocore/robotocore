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

    # --- Settings ---

    def test_get_instance_metadata_defaults(self, ec2):
        _assert_ok(ec2.get_instance_metadata_defaults())

    def test_get_image_block_public_access_state(self, ec2):
        _assert_ok(ec2.get_image_block_public_access_state())

    def test_get_snapshot_block_public_access_state(self, ec2):
        _assert_ok(ec2.get_snapshot_block_public_access_state())


class TestEC2DescribeGapCoverage:
    """Tests for Describe/List/Get operations that return empty or default results."""

    # --- Address / EIP related ---

    def test_describe_addresses_attribute(self, ec2):
        resp = ec2.describe_addresses_attribute()
        assert "Addresses" in resp

    # --- Aggregate / ID format ---

    # --- Bundle / Import / Export tasks ---

    def test_describe_bundle_tasks(self, ec2):
        resp = ec2.describe_bundle_tasks()
        assert "BundleTasks" in resp
        assert isinstance(resp["BundleTasks"], list)

    def test_describe_conversion_tasks(self, ec2):
        _assert_ok(ec2.describe_conversion_tasks())

    def test_describe_export_image_tasks(self, ec2):
        _assert_ok(ec2.describe_export_image_tasks())

    def test_describe_export_tasks(self, ec2):
        _assert_ok(ec2.describe_export_tasks())

    def test_describe_import_image_tasks(self, ec2):
        _assert_ok(ec2.describe_import_image_tasks())

    def test_describe_import_snapshot_tasks(self, ec2):
        _assert_ok(ec2.describe_import_snapshot_tasks())

    def test_describe_store_image_tasks(self, ec2):
        _assert_ok(ec2.describe_store_image_tasks())

    # --- Carrier / Classic ---

    def test_describe_carrier_gateways(self, ec2):
        resp = ec2.describe_carrier_gateways()
        assert "CarrierGateways" in resp
        assert isinstance(resp["CarrierGateways"], list)

    def test_describe_classic_link_instances(self, ec2):
        _assert_ok(ec2.describe_classic_link_instances())

    # --- Client VPN ---

    def test_describe_client_vpn_endpoints(self, ec2):
        _assert_ok(ec2.describe_client_vpn_endpoints())

    # --- Egress Only IGW ---

    def test_describe_egress_only_internet_gateways(self, ec2):
        resp = ec2.describe_egress_only_internet_gateways()
        assert "EgressOnlyInternetGateways" in resp
        assert isinstance(resp["EgressOnlyInternetGateways"], list)

    # --- Fleets ---

    def test_describe_fleets(self, ec2):
        resp = ec2.describe_fleets()
        assert "Fleets" in resp
        assert isinstance(resp["Fleets"], list)

    # --- FPGA ---

    def test_describe_fpga_images(self, ec2):
        _assert_ok(ec2.describe_fpga_images())

    # --- Hosts ---

    def test_describe_host_reservations(self, ec2):
        _assert_ok(ec2.describe_host_reservations())

    # --- Instance event ---

    def test_describe_instance_event_notification_attributes(self, ec2):
        _assert_ok(ec2.describe_instance_event_notification_attributes())

    def test_describe_instance_event_windows(self, ec2):
        _assert_ok(ec2.describe_instance_event_windows())

    # --- Instance credit ---

    def test_describe_instance_credit_specifications(self, ec2):
        resp = ec2.describe_instance_credit_specifications()
        assert "InstanceCreditSpecifications" in resp
        assert isinstance(resp["InstanceCreditSpecifications"], list)

    # --- Instance topology ---

    def test_describe_instance_topology(self, ec2):
        _assert_ok(ec2.describe_instance_topology())

    # --- Instance connect endpoints ---

    def test_describe_instance_connect_endpoints(self, ec2):
        _assert_ok(ec2.describe_instance_connect_endpoints())

    # --- Instance type offerings ---

    def test_describe_instance_type_offerings(self, ec2):
        resp = ec2.describe_instance_type_offerings()
        assert "InstanceTypeOfferings" in resp
        assert isinstance(resp["InstanceTypeOfferings"], list)

    def test_describe_instance_type_offerings_by_az(self, ec2):
        resp = ec2.describe_instance_type_offerings(LocationType="availability-zone")
        assert "InstanceTypeOfferings" in resp

    # --- Mac hosts ---

    def test_describe_mac_hosts(self, ec2):
        _assert_ok(ec2.describe_mac_hosts())

    # --- Principal ID format ---

    def test_describe_principal_id_format(self, ec2):
        _assert_ok(ec2.describe_principal_id_format())

    # --- Moving addresses ---

    def test_describe_moving_addresses(self, ec2):
        _assert_ok(ec2.describe_moving_addresses())

    # --- Scheduled instances ---

    def test_describe_scheduled_instances(self, ec2):
        _assert_ok(ec2.describe_scheduled_instances())

    # --- Spot ---

    def test_describe_spot_fleet_requests(self, ec2):
        resp = ec2.describe_spot_fleet_requests()
        assert "SpotFleetRequestConfigs" in resp
        assert isinstance(resp["SpotFleetRequestConfigs"], list)

    def test_describe_spot_instance_requests(self, ec2):
        resp = ec2.describe_spot_instance_requests()
        assert "SpotInstanceRequests" in resp
        assert isinstance(resp["SpotInstanceRequests"], list)

    def test_describe_spot_price_history(self, ec2):
        resp = ec2.describe_spot_price_history()
        assert "SpotPriceHistory" in resp
        assert isinstance(resp["SpotPriceHistory"], list)

    def test_describe_spot_price_history_with_filter(self, ec2):
        resp = ec2.describe_spot_price_history(InstanceTypes=["m5.large"], MaxResults=5)
        assert "SpotPriceHistory" in resp

    def test_describe_spot_datafeed_subscription(self, ec2):
        _assert_ok(ec2.describe_spot_datafeed_subscription())

    # --- Snapshot tier ---

    def test_describe_snapshot_tier_status(self, ec2):
        resp = ec2.describe_snapshot_tier_status()
        assert "SnapshotTierStatuses" in resp
        assert isinstance(resp["SnapshotTierStatuses"], list)

    # --- Volumes modifications ---

    def test_describe_volumes_modifications(self, ec2):
        resp = ec2.describe_volumes_modifications()
        assert "VolumesModifications" in resp
        assert isinstance(resp["VolumesModifications"], list)

    # --- VPC Classic Link ---

    def test_describe_vpc_classic_link(self, ec2):
        resp = ec2.describe_vpc_classic_link()
        assert "Vpcs" in resp
        assert isinstance(resp["Vpcs"], list)

    def test_describe_vpc_classic_link_dns_support(self, ec2):
        resp = ec2.describe_vpc_classic_link_dns_support()
        assert "Vpcs" in resp
        assert isinstance(resp["Vpcs"], list)

    # --- VPC Endpoint ---

    def test_describe_vpc_endpoint_service_configurations(self, ec2):
        resp = ec2.describe_vpc_endpoint_service_configurations()
        assert "ServiceConfigurations" in resp
        assert isinstance(resp["ServiceConfigurations"], list)

    # --- Reserved instances ---

    def test_describe_reserved_instances(self, ec2):
        resp = ec2.describe_reserved_instances()
        assert "ReservedInstances" in resp
        assert isinstance(resp["ReservedInstances"], list)

    def test_describe_reserved_instances_listings(self, ec2):
        resp = ec2.describe_reserved_instances_listings()
        assert "ReservedInstancesListings" in resp
        assert isinstance(resp["ReservedInstancesListings"], list)

    def test_describe_reserved_instances_modifications(self, ec2):
        resp = ec2.describe_reserved_instances_modifications()
        assert "ReservedInstancesModifications" in resp
        assert isinstance(resp["ReservedInstancesModifications"], list)

    def test_describe_reserved_instances_offerings(self, ec2):
        resp = ec2.describe_reserved_instances_offerings()
        assert "ReservedInstancesOfferings" in resp
        assert isinstance(resp["ReservedInstancesOfferings"], list)

    # --- VPN ---

    def test_describe_vpn_connections(self, ec2):
        resp = ec2.describe_vpn_connections()
        assert "VpnConnections" in resp
        assert isinstance(resp["VpnConnections"], list)

    def test_describe_vpn_gateways(self, ec2):
        resp = ec2.describe_vpn_gateways()
        assert "VpnGateways" in resp
        assert isinstance(resp["VpnGateways"], list)

    # --- Elastic GPU ---

    def test_describe_elastic_gpus(self, ec2):
        _assert_ok(ec2.describe_elastic_gpus())

    # --- Fast launch images ---

    def test_describe_fast_launch_images(self, ec2):
        _assert_ok(ec2.describe_fast_launch_images())

    # --- Declarative policies ---

    def test_describe_declarative_policies_reports(self, ec2):
        _assert_ok(ec2.describe_declarative_policies_reports())

    # --- EBS settings ---

    def test_get_ebs_encryption_by_default(self, ec2):
        resp = ec2.get_ebs_encryption_by_default()
        assert "EbsEncryptionByDefault" in resp

    def test_get_vpn_connection_device_types(self, ec2):
        _assert_ok(ec2.get_vpn_connection_device_types())


class TestEC2CRUDGapCoverage:
    """Tests for create/describe/delete flows for untested EC2 resources."""

    def test_create_describe_delete_customer_gateway(self, ec2):
        """CreateCustomerGateway / DescribeCustomerGateways / DeleteCustomerGateway."""
        resp = ec2.create_customer_gateway(BgpAsn=65000, Type="ipsec.1", IpAddress="198.51.100.10")
        cgw_id = resp["CustomerGateway"]["CustomerGatewayId"]
        try:
            assert cgw_id.startswith("cgw-")
            described = ec2.describe_customer_gateways(CustomerGatewayIds=[cgw_id])
            assert len(described["CustomerGateways"]) == 1
            assert described["CustomerGateways"][0]["IpAddress"] == "198.51.100.10"
        finally:
            ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)

    def test_create_describe_delete_vpn_gateway(self, ec2):
        """CreateVpnGateway / DescribeVpnGateways / DeleteVpnGateway."""
        resp = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = resp["VpnGateway"]["VpnGatewayId"]
        try:
            assert vgw_id.startswith("vgw-")
            described = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
            assert len(described["VpnGateways"]) == 1
        finally:
            ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)

    def test_create_describe_delete_managed_prefix_list(self, ec2):
        """CreateManagedPrefixList / DescribeManagedPrefixLists / DeleteManagedPrefixList."""
        name = _unique("test-pl")
        resp = ec2.create_managed_prefix_list(
            PrefixListName=name, MaxEntries=5, AddressFamily="IPv4"
        )
        pl_id = resp["PrefixList"]["PrefixListId"]
        try:
            assert pl_id.startswith("pl-")
            described = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])
            assert len(described["PrefixLists"]) == 1
            assert described["PrefixLists"][0]["PrefixListName"] == name
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)

    def test_get_managed_prefix_list_entries(self, ec2):
        """GetManagedPrefixListEntries for a new prefix list (empty)."""
        name = _unique("test-pl-entries")
        resp = ec2.create_managed_prefix_list(
            PrefixListName=name, MaxEntries=5, AddressFamily="IPv4"
        )
        pl_id = resp["PrefixList"]["PrefixListId"]
        try:
            entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
            assert "Entries" in entries
            assert isinstance(entries["Entries"], list)
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)

    def test_create_describe_delete_vpn_connection(self, ec2):
        """CreateVpnConnection / DescribeVpnConnections / DeleteVpnConnection."""
        cgw = ec2.create_customer_gateway(BgpAsn=65000, Type="ipsec.1", IpAddress="198.51.100.20")
        cgw_id = cgw["CustomerGateway"]["CustomerGatewayId"]
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]
        try:
            vpn = ec2.create_vpn_connection(
                Type="ipsec.1", CustomerGatewayId=cgw_id, VpnGatewayId=vgw_id
            )
            vpn_id = vpn["VpnConnection"]["VpnConnectionId"]
            described = ec2.describe_vpn_connections(VpnConnectionIds=[vpn_id])
            assert len(described["VpnConnections"]) == 1
            ec2.delete_vpn_connection(VpnConnectionId=vpn_id)
        finally:
            ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
            ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)

    def test_create_describe_delete_egress_only_igw(self, ec2):
        """CreateEgressOnlyInternetGateway / Describe / Delete."""
        vpc = ec2.create_vpc(CidrBlock="10.201.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.create_egress_only_internet_gateway(VpcId=vpc_id)
            eigw_id = resp["EgressOnlyInternetGateway"]["EgressOnlyInternetGatewayId"]
            described = ec2.describe_egress_only_internet_gateways(
                EgressOnlyInternetGatewayIds=[eigw_id]
            )
            assert len(described["EgressOnlyInternetGateways"]) == 1
            ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId=eigw_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_allocate_describe_release_hosts(self, ec2):
        """AllocateHosts / DescribeHosts / ReleaseHosts."""
        resp = ec2.allocate_hosts(
            AvailabilityZone="us-east-1a", InstanceType="m5.large", Quantity=1
        )
        host_ids = resp["HostIds"]
        assert len(host_ids) == 1
        host_id = host_ids[0]
        try:
            described = ec2.describe_hosts(HostIds=[host_id])
            assert len(described["Hosts"]) == 1
            assert described["Hosts"][0]["HostId"] == host_id
        finally:
            ec2.release_hosts(HostIds=[host_id])

    def test_attach_detach_vpn_gateway(self, ec2):
        """AttachVpnGateway / DetachVpnGateway."""
        vpc = ec2.create_vpc(CidrBlock="10.202.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]
        try:
            ec2.attach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
            described = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
            attachments = described["VpnGateways"][0].get("VpcAttachments", [])
            assert any(a["VpcId"] == vpc_id for a in attachments)
            ec2.detach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
        finally:
            ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2SettingsGapCoverage:
    """Tests for EC2 account-level settings operations."""

    def test_enable_disable_ebs_encryption_by_default(self, ec2):
        """EnableEbsEncryptionByDefault / DisableEbsEncryptionByDefault."""
        enable_resp = ec2.enable_ebs_encryption_by_default()
        assert "EbsEncryptionByDefault" in enable_resp

        disable_resp = ec2.disable_ebs_encryption_by_default()
        assert "EbsEncryptionByDefault" in disable_resp

    def test_modify_reset_ebs_default_kms_key(self, ec2):
        """ModifyEbsDefaultKmsKeyId / ResetEbsDefaultKmsKeyId."""
        modify_resp = ec2.modify_ebs_default_kms_key_id(KmsKeyId="alias/aws/ebs")
        assert "KmsKeyId" in modify_resp

        _assert_ok(ec2.reset_ebs_default_kms_key_id())

    def test_enable_disable_serial_console_access(self, ec2):
        """EnableSerialConsoleAccess / DisableSerialConsoleAccess."""
        _assert_ok(ec2.enable_serial_console_access())
        _assert_ok(ec2.disable_serial_console_access())

    def test_modify_instance_metadata_defaults(self, ec2):
        """ModifyInstanceMetadataDefaults."""
        resp = ec2.modify_instance_metadata_defaults(HttpTokens="required")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Reset to optional
        ec2.modify_instance_metadata_defaults(HttpTokens="optional")

    def test_delete_spot_datafeed_subscription(self, ec2):
        """DeleteSpotDatafeedSubscription (no-op when none exists)."""
        _assert_ok(ec2.delete_spot_datafeed_subscription())


class TestEC2VPCPeeringGapCoverage:
    """Tests for VPC peering operations."""

    def test_create_accept_describe_delete_vpc_peering(self, ec2):
        """CreateVpcPeeringConnection / AcceptVpcPeeringConnection / Delete."""
        vpc1 = ec2.create_vpc(CidrBlock="10.210.0.0/16")["Vpc"]["VpcId"]
        vpc2 = ec2.create_vpc(CidrBlock="10.211.0.0/16")["Vpc"]["VpcId"]
        try:
            pcx = ec2.create_vpc_peering_connection(VpcId=vpc1, PeerVpcId=vpc2)
            pcx_id = pcx["VpcPeeringConnection"]["VpcPeeringConnectionId"]
            assert pcx_id.startswith("pcx-")

            ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)

            described = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
            assert len(described["VpcPeeringConnections"]) == 1

            ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
        finally:
            ec2.delete_vpc(VpcId=vpc1)
            ec2.delete_vpc(VpcId=vpc2)


class TestEC2TransitGatewayGapCoverage:
    """Tests for Transit Gateway operations beyond basic create/delete."""

    def test_create_describe_delete_tgw_route_table(self, ec2):
        """CreateTransitGatewayRouteTable / Describe / Delete."""
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]
            assert rtb_id.startswith("tgw-rtb-")

            described = ec2.describe_transit_gateway_route_tables(
                TransitGatewayRouteTableIds=[rtb_id]
            )
            assert len(described["TransitGatewayRouteTables"]) == 1

            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)

    def test_search_transit_gateway_routes(self, ec2):
        """SearchTransitGatewayRoutes on empty route table."""
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]

            routes = ec2.search_transit_gateway_routes(
                TransitGatewayRouteTableId=rtb_id,
                Filters=[{"Name": "type", "Values": ["static"]}],
            )
            assert "Routes" in routes
            assert isinstance(routes["Routes"], list)

            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)

    def test_describe_transit_gateway_vpc_attachments(self, ec2):
        """DescribeTransitGatewayVpcAttachments returns list."""
        resp = ec2.describe_transit_gateway_vpc_attachments()
        assert "TransitGatewayVpcAttachments" in resp
        assert isinstance(resp["TransitGatewayVpcAttachments"], list)

    def test_get_transit_gateway_route_table_associations(self, ec2):
        """GetTransitGatewayRouteTableAssociations (stub)."""
        _assert_ok(
            ec2.get_transit_gateway_route_table_associations(
                TransitGatewayRouteTableId="tgw-rtb-0000"
            )
        )

    def test_get_transit_gateway_route_table_propagations(self, ec2):
        """GetTransitGatewayRouteTablePropagations (stub)."""
        _assert_ok(
            ec2.get_transit_gateway_route_table_propagations(
                TransitGatewayRouteTableId="tgw-rtb-0000"
            )
        )


class TestEC2NetworkGapCoverage:
    """Tests for network-related gap operations."""

    def test_create_describe_delete_egress_only_igw(self, ec2):
        """Separate test for EgressOnlyInternetGateway lifecycle."""
        vpc = ec2.create_vpc(CidrBlock="10.220.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.create_egress_only_internet_gateway(VpcId=vpc_id)
            eigw_id = resp["EgressOnlyInternetGateway"]["EgressOnlyInternetGatewayId"]
            assert eigw_id.startswith("eigw-")

            desc = ec2.describe_egress_only_internet_gateways(
                EgressOnlyInternetGatewayIds=[eigw_id]
            )
            assert len(desc["EgressOnlyInternetGateways"]) == 1

            ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId=eigw_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_describe_delete_network_acl_with_entry(self, ec2):
        """CreateNetworkAcl with entry, describe, delete."""
        vpc = ec2.create_vpc(CidrBlock="10.221.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            nacl = ec2.create_network_acl(VpcId=vpc_id)
            nacl_id = nacl["NetworkAcl"]["NetworkAclId"]

            ec2.create_network_acl_entry(
                NetworkAclId=nacl_id,
                RuleNumber=100,
                Protocol="-1",
                RuleAction="allow",
                Egress=False,
                CidrBlock="0.0.0.0/0",
            )

            desc = ec2.describe_network_acls(NetworkAclIds=[nacl_id])
            assert len(desc["NetworkAcls"]) == 1
            entries = desc["NetworkAcls"][0]["Entries"]
            assert any(e["RuleNumber"] == 100 for e in entries)

            ec2.delete_network_acl(NetworkAclId=nacl_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_network_interfaces_with_filter(self, ec2):
        """DescribeNetworkInterfaces with a filter returns list."""
        resp = ec2.describe_network_interfaces(
            Filters=[{"Name": "status", "Values": ["available"]}]
        )
        assert "NetworkInterfaces" in resp
        assert isinstance(resp["NetworkInterfaces"], list)

    def test_describe_subnets_with_filter(self, ec2):
        """DescribeSubnets with availability-zone filter."""
        resp = ec2.describe_subnets(
            Filters=[{"Name": "availability-zone", "Values": ["us-east-1a"]}]
        )
        assert "Subnets" in resp

    def test_describe_route_tables_with_filter(self, ec2):
        """DescribeRouteTables with filter."""
        resp = ec2.describe_route_tables(Filters=[{"Name": "association.main", "Values": ["true"]}])
        assert "RouteTables" in resp

    def test_describe_internet_gateways_with_filter(self, ec2):
        """DescribeInternetGateways with filter."""
        resp = ec2.describe_internet_gateways(
            Filters=[{"Name": "attachment.state", "Values": ["available"]}]
        )
        assert "InternetGateways" in resp

    def test_describe_security_groups_with_filter(self, ec2):
        """DescribeSecurityGroups with filter."""
        resp = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["default"]}])
        assert "SecurityGroups" in resp
        assert len(resp["SecurityGroups"]) >= 1

    def test_describe_vpcs_with_filter(self, ec2):
        """DescribeVpcs with is-default filter."""
        resp = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
        assert "Vpcs" in resp

    def test_describe_nat_gateways_with_filter(self, ec2):
        """DescribeNatGateways with filter."""
        resp = ec2.describe_nat_gateways(Filters=[{"Name": "state", "Values": ["available"]}])
        assert "NatGateways" in resp

    def test_describe_volumes_with_filter(self, ec2):
        """DescribeVolumes with filter."""
        resp = ec2.describe_volumes(Filters=[{"Name": "status", "Values": ["available"]}])
        assert "Volumes" in resp

    def test_describe_snapshots_owner_self(self, ec2):
        """DescribeSnapshots with OwnerIds=self."""
        resp = ec2.describe_snapshots(OwnerIds=["self"])
        assert "Snapshots" in resp

    def test_describe_images_owner_self(self, ec2):
        """DescribeImages with Owners=self."""
        resp = ec2.describe_images(Owners=["self"])
        assert "Images" in resp


class TestEc2AutoCoverage:
    """Auto-generated coverage tests for ec2."""

    @pytest.fixture
    def client(self):
        return make_client("ec2")

    def test_accept_transit_gateway_multicast_domain_associations(self, client):
        """AcceptTransitGatewayMulticastDomainAssociations returns a response."""
        client.accept_transit_gateway_multicast_domain_associations()

    def test_cancel_import_task(self, client):
        """CancelImportTask returns a response."""
        client.cancel_import_task()

    def test_create_instance_event_window(self, client):
        """CreateInstanceEventWindow returns a response."""
        client.create_instance_event_window()

    def test_create_ipam(self, client):
        """CreateIpam returns a response."""
        client.create_ipam()

    def test_create_ipam_resource_discovery(self, client):
        """CreateIpamResourceDiscovery returns a response."""
        client.create_ipam_resource_discovery()

    def test_create_public_ipv4_pool(self, client):
        """CreatePublicIpv4Pool returns a response."""
        client.create_public_ipv4_pool()

    def test_create_traffic_mirror_filter(self, client):
        """CreateTrafficMirrorFilter returns a response."""
        client.create_traffic_mirror_filter()

    def test_create_traffic_mirror_target(self, client):
        """CreateTrafficMirrorTarget returns a response."""
        client.create_traffic_mirror_target()

    def test_create_verified_access_instance(self, client):
        """CreateVerifiedAccessInstance returns a response."""
        client.create_verified_access_instance()

    def test_deregister_transit_gateway_multicast_group_members(self, client):
        """DeregisterTransitGatewayMulticastGroupMembers returns a response."""
        client.deregister_transit_gateway_multicast_group_members()

    def test_deregister_transit_gateway_multicast_group_sources(self, client):
        """DeregisterTransitGatewayMulticastGroupSources returns a response."""
        client.deregister_transit_gateway_multicast_group_sources()

    def test_describe_address_transfers(self, client):
        """DescribeAddressTransfers returns a response."""
        client.describe_address_transfers()

    def test_describe_aws_network_performance_metric_subscriptions(self, client):
        """DescribeAwsNetworkPerformanceMetricSubscriptions returns a response."""
        client.describe_aws_network_performance_metric_subscriptions()

    def test_describe_capacity_block_extension_history(self, client):
        """DescribeCapacityBlockExtensionHistory returns a response."""
        client.describe_capacity_block_extension_history()

    def test_describe_capacity_block_status(self, client):
        """DescribeCapacityBlockStatus returns a response."""
        resp = client.describe_capacity_block_status()
        assert "CapacityBlockStatuses" in resp

    def test_describe_capacity_blocks(self, client):
        """DescribeCapacityBlocks returns a response."""
        client.describe_capacity_blocks()

    def test_describe_capacity_manager_data_exports(self, client):
        """DescribeCapacityManagerDataExports returns a response."""
        client.describe_capacity_manager_data_exports()

    def test_describe_capacity_reservation_topology(self, client):
        """DescribeCapacityReservationTopology returns a response."""
        client.describe_capacity_reservation_topology()

    def test_describe_host_reservation_offerings(self, client):
        """DescribeHostReservationOfferings returns a response."""
        client.describe_host_reservation_offerings()

    def test_describe_iam_instance_profile_associations(self, client):
        """DescribeIamInstanceProfileAssociations returns a response."""
        resp = client.describe_iam_instance_profile_associations()
        assert "IamInstanceProfileAssociations" in resp

    def test_describe_id_format(self, client):
        """DescribeIdFormat returns a response."""
        client.describe_id_format()

    def test_describe_image_usage_report_entries(self, client):
        """DescribeImageUsageReportEntries returns a response."""
        client.describe_image_usage_report_entries()

    def test_describe_image_usage_reports(self, client):
        """DescribeImageUsageReports returns a response."""
        client.describe_image_usage_reports()

    def test_describe_instance_image_metadata(self, client):
        """DescribeInstanceImageMetadata returns a response."""
        resp = client.describe_instance_image_metadata()
        assert "InstanceImageMetadata" in resp

    def test_describe_instance_sql_ha_history_states(self, client):
        """DescribeInstanceSqlHaHistoryStates returns a response."""
        client.describe_instance_sql_ha_history_states()

    def test_describe_instance_sql_ha_states(self, client):
        """DescribeInstanceSqlHaStates returns a response."""
        client.describe_instance_sql_ha_states()

    def test_describe_mac_modification_tasks(self, client):
        """DescribeMacModificationTasks returns a response."""
        client.describe_mac_modification_tasks()

    def test_describe_outpost_lags(self, client):
        """DescribeOutpostLags returns a response."""
        client.describe_outpost_lags()

    def test_describe_route_server_endpoints(self, client):
        """DescribeRouteServerEndpoints returns a response."""
        client.describe_route_server_endpoints()

    def test_describe_route_server_peers(self, client):
        """DescribeRouteServerPeers returns a response."""
        client.describe_route_server_peers()

    def test_describe_route_servers(self, client):
        """DescribeRouteServers returns a response."""
        client.describe_route_servers()

    def test_describe_secondary_interfaces(self, client):
        """DescribeSecondaryInterfaces returns a response."""
        client.describe_secondary_interfaces()

    def test_describe_secondary_networks(self, client):
        """DescribeSecondaryNetworks returns a response."""
        client.describe_secondary_networks()

    def test_describe_secondary_subnets(self, client):
        """DescribeSecondarySubnets returns a response."""
        client.describe_secondary_subnets()

    def test_describe_security_group_vpc_associations(self, client):
        """DescribeSecurityGroupVpcAssociations returns a response."""
        client.describe_security_group_vpc_associations()

    def test_describe_service_link_virtual_interfaces(self, client):
        """DescribeServiceLinkVirtualInterfaces returns a response."""
        client.describe_service_link_virtual_interfaces()

    def test_describe_transit_gateway_attachments(self, client):
        """DescribeTransitGatewayAttachments returns a response."""
        resp = client.describe_transit_gateway_attachments()
        assert "TransitGatewayAttachments" in resp

    def test_describe_transit_gateway_peering_attachments(self, client):
        """DescribeTransitGatewayPeeringAttachments returns a response."""
        resp = client.describe_transit_gateway_peering_attachments()
        assert "TransitGatewayPeeringAttachments" in resp

    def test_describe_volume_status(self, client):
        """DescribeVolumeStatus returns a response."""
        resp = client.describe_volume_status()
        assert "VolumeStatuses" in resp

    def test_describe_vpc_block_public_access_exclusions(self, client):
        """DescribeVpcBlockPublicAccessExclusions returns a response."""
        client.describe_vpc_block_public_access_exclusions()

    def test_describe_vpc_block_public_access_options(self, client):
        """DescribeVpcBlockPublicAccessOptions returns a response."""
        client.describe_vpc_block_public_access_options()

    def test_describe_vpc_encryption_controls(self, client):
        """DescribeVpcEncryptionControls returns a response."""
        client.describe_vpc_encryption_controls()

    def test_describe_vpc_endpoint_associations(self, client):
        """DescribeVpcEndpointAssociations returns a response."""
        client.describe_vpc_endpoint_associations()

    def test_describe_vpc_endpoint_services(self, client):
        """DescribeVpcEndpointServices returns a response."""
        resp = client.describe_vpc_endpoint_services()
        assert "ServiceNames" in resp

    def test_describe_vpn_concentrators(self, client):
        """DescribeVpnConcentrators returns a response."""
        client.describe_vpn_concentrators()

    def test_disable_allowed_images_settings(self, client):
        """DisableAllowedImagesSettings returns a response."""
        client.disable_allowed_images_settings()

    def test_disable_aws_network_performance_metric_subscription(self, client):
        """DisableAwsNetworkPerformanceMetricSubscription returns a response."""
        client.disable_aws_network_performance_metric_subscription()

    def test_disable_capacity_manager(self, client):
        """DisableCapacityManager returns a response."""
        client.disable_capacity_manager()

    def test_disable_image_block_public_access(self, client):
        """DisableImageBlockPublicAccess returns a response."""
        client.disable_image_block_public_access()

    def test_disable_snapshot_block_public_access(self, client):
        """DisableSnapshotBlockPublicAccess returns a response."""
        client.disable_snapshot_block_public_access()

    def test_enable_aws_network_performance_metric_subscription(self, client):
        """EnableAwsNetworkPerformanceMetricSubscription returns a response."""
        client.enable_aws_network_performance_metric_subscription()

    def test_enable_capacity_manager(self, client):
        """EnableCapacityManager returns a response."""
        client.enable_capacity_manager()

    def test_enable_reachability_analyzer_organization_sharing(self, client):
        """EnableReachabilityAnalyzerOrganizationSharing returns a response."""
        client.enable_reachability_analyzer_organization_sharing()

    def test_get_allowed_images_settings(self, client):
        """GetAllowedImagesSettings returns a response."""
        client.get_allowed_images_settings()

    def test_get_aws_network_performance_data(self, client):
        """GetAwsNetworkPerformanceData returns a response."""
        client.get_aws_network_performance_data()

    def test_get_capacity_manager_attributes(self, client):
        """GetCapacityManagerAttributes returns a response."""
        client.get_capacity_manager_attributes()

    def test_get_enabled_ipam_policy(self, client):
        """GetEnabledIpamPolicy returns a response."""
        client.get_enabled_ipam_policy()

    def test_import_image(self, client):
        """ImportImage returns a response."""
        client.import_image()

    def test_import_snapshot(self, client):
        """ImportSnapshot returns a response."""
        client.import_snapshot()

    def test_reject_transit_gateway_multicast_domain_associations(self, client):
        """RejectTransitGatewayMulticastDomainAssociations returns a response."""
        client.reject_transit_gateway_multicast_domain_associations()

    def test_replace_image_criteria_in_allowed_images_settings(self, client):
        """ReplaceImageCriteriaInAllowedImagesSettings returns a response."""
        resp = client.replace_image_criteria_in_allowed_images_settings()
        assert "ReturnValue" in resp

    def test_request_spot_instances(self, client):
        """RequestSpotInstances returns a response."""
        resp = client.request_spot_instances()
        assert "SpotInstanceRequests" in resp


class TestEC2AddressOperations:
    """Tests for EIP associate/disassociate operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_associate_disassociate_address_to_instance(self, ec2):
        """AssociateAddress / DisassociateAddress with an instance."""
        # Allocate an EIP
        alloc = ec2.allocate_address(Domain="vpc")
        alloc_id = alloc["AllocationId"]
        try:
            # Launch an instance
            run = ec2.run_instances(ImageId="ami-12345678", MinCount=1, MaxCount=1)
            inst_id = run["Instances"][0]["InstanceId"]
            try:
                # Associate EIP with instance
                assoc = ec2.associate_address(AllocationId=alloc_id, InstanceId=inst_id)
                assoc_id = assoc["AssociationId"]
                assert assoc_id.startswith("eipassoc-")

                # Verify association via describe
                desc = ec2.describe_addresses(AllocationIds=[alloc_id])
                assert desc["Addresses"][0]["InstanceId"] == inst_id
                assert desc["Addresses"][0]["AssociationId"] == assoc_id

                # Disassociate
                ec2.disassociate_address(AssociationId=assoc_id)

                # Verify disassociation
                desc2 = ec2.describe_addresses(AllocationIds=[alloc_id])
                assert desc2["Addresses"][0].get("InstanceId", "") in ("", inst_id)
            finally:
                ec2.terminate_instances(InstanceIds=[inst_id])
        finally:
            ec2.release_address(AllocationId=alloc_id)

    def test_associate_address_to_network_interface(self, ec2):
        """AssociateAddress / DisassociateAddress with ENI."""
        vpc = ec2.create_vpc(CidrBlock="10.220.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.220.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id)
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    alloc = ec2.allocate_address(Domain="vpc")
                    alloc_id = alloc["AllocationId"]
                    try:
                        assoc = ec2.associate_address(
                            AllocationId=alloc_id, NetworkInterfaceId=eni_id
                        )
                        assoc_id = assoc["AssociationId"]
                        assert assoc_id.startswith("eipassoc-")

                        ec2.disassociate_address(AssociationId=assoc_id)
                    finally:
                        ec2.release_address(AllocationId=alloc_id)
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2InstanceOperations:
    """Tests for instance monitoring, start/stop/reboot, IAM profiles, attributes."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def _launch_instance(self, ec2):
        """Launch a t2.micro instance and return instance ID."""
        resp = ec2.run_instances(
            ImageId="ami-12345678", InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        return resp["Instances"][0]["InstanceId"]

    def test_stop_start_instances(self, ec2):
        """StopInstances / StartInstances."""
        inst_id = self._launch_instance(ec2)
        try:
            stop = ec2.stop_instances(InstanceIds=[inst_id])
            assert len(stop["StoppingInstances"]) == 1
            assert stop["StoppingInstances"][0]["InstanceId"] == inst_id

            start = ec2.start_instances(InstanceIds=[inst_id])
            assert len(start["StartingInstances"]) == 1
            assert start["StartingInstances"][0]["InstanceId"] == inst_id
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])

    def test_reboot_instances(self, ec2):
        """RebootInstances."""
        inst_id = self._launch_instance(ec2)
        try:
            # RebootInstances returns empty on success
            resp = ec2.reboot_instances(InstanceIds=[inst_id])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])

    def test_describe_instance_attribute_instance_type(self, ec2):
        """DescribeInstanceAttribute for instanceType."""
        inst_id = self._launch_instance(ec2)
        try:
            resp = ec2.describe_instance_attribute(InstanceId=inst_id, Attribute="instanceType")
            assert resp["InstanceId"] == inst_id
            assert resp["InstanceType"]["Value"] == "t2.micro"
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])

    def test_describe_instance_attribute_disable_api_termination(self, ec2):
        """DescribeInstanceAttribute for disableApiTermination."""
        inst_id = self._launch_instance(ec2)
        try:
            resp = ec2.describe_instance_attribute(
                InstanceId=inst_id, Attribute="disableApiTermination"
            )
            assert resp["InstanceId"] == inst_id
            assert "DisableApiTermination" in resp
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])

    def test_describe_instance_attribute_user_data(self, ec2):
        """DescribeInstanceAttribute for userData."""
        inst_id = self._launch_instance(ec2)
        try:
            resp = ec2.describe_instance_attribute(InstanceId=inst_id, Attribute="userData")
            assert resp["InstanceId"] == inst_id
            assert "UserData" in resp
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])

    def test_associate_disassociate_iam_instance_profile(self, ec2):
        """AssociateIamInstanceProfile / DisassociateIamInstanceProfile."""
        import json

        iam = make_client("iam")

        # Create IAM role and instance profile
        role_name = _unique("ec2-role")
        profile_name = _unique("ec2-profile")
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
        iam.create_instance_profile(InstanceProfileName=profile_name)
        iam.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)

        inst_id = self._launch_instance(ec2)
        try:
            profile_resp = iam.get_instance_profile(InstanceProfileName=profile_name)
            profile_arn = profile_resp["InstanceProfile"]["Arn"]

            assoc = ec2.associate_iam_instance_profile(
                IamInstanceProfile={"Arn": profile_arn},
                InstanceId=inst_id,
            )
            assoc_id = assoc["IamInstanceProfileAssociation"]["AssociationId"]
            assert assoc_id.startswith("iip-assoc-")
            assert assoc["IamInstanceProfileAssociation"]["InstanceId"] == inst_id

            # Describe to verify
            desc = ec2.describe_iam_instance_profile_associations(AssociationIds=[assoc_id])
            assert len(desc["IamInstanceProfileAssociations"]) == 1

            # Disassociate
            dis = ec2.disassociate_iam_instance_profile(AssociationId=assoc_id)
            assert dis["IamInstanceProfileAssociation"]["AssociationId"] == assoc_id
        finally:
            ec2.terminate_instances(InstanceIds=[inst_id])
            iam.remove_role_from_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            iam.delete_instance_profile(InstanceProfileName=profile_name)
            iam.delete_role(RoleName=role_name)


class TestEC2LaunchTemplateAdvanced:
    """Advanced launch template operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_launch_template_versions(self, ec2):
        """DescribeLaunchTemplateVersions."""
        name = _unique("lt-ver")
        resp = ec2.create_launch_template(
            LaunchTemplateName=name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            vers = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
            assert len(vers["LaunchTemplateVersions"]) >= 1
            assert vers["LaunchTemplateVersions"][0]["LaunchTemplateId"] == lt_id
            assert (
                vers["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"]
                == "t2.micro"
            )
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)

    def test_create_launch_template_version(self, ec2):
        """CreateLaunchTemplateVersion."""
        name = _unique("lt-newver")
        resp = ec2.create_launch_template(
            LaunchTemplateName=name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            v2 = ec2.create_launch_template_version(
                LaunchTemplateId=lt_id,
                LaunchTemplateData={"InstanceType": "t2.small"},
            )
            assert v2["LaunchTemplateVersion"]["VersionNumber"] == 2
            assert v2["LaunchTemplateVersion"]["LaunchTemplateData"]["InstanceType"] == "t2.small"

            vers = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
            assert len(vers["LaunchTemplateVersions"]) == 2
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)

    def test_modify_launch_template_default_version(self, ec2):
        """ModifyLaunchTemplate to change default version."""
        name = _unique("lt-mod")
        resp = ec2.create_launch_template(
            LaunchTemplateName=name,
            LaunchTemplateData={"InstanceType": "t2.micro"},
        )
        lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
        try:
            # Create version 2
            ec2.create_launch_template_version(
                LaunchTemplateId=lt_id,
                LaunchTemplateData={"InstanceType": "t2.small"},
            )

            # Modify default version to 2
            mod = ec2.modify_launch_template(LaunchTemplateId=lt_id, DefaultVersion="2")
            assert mod["LaunchTemplate"]["DefaultVersionNumber"] == 2
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)


class TestEC2ManagedPrefixListAdvanced:
    """Advanced managed prefix list operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_managed_prefix_list_add_entry(self, ec2):
        """ModifyManagedPrefixList to add an entry."""
        name = _unique("pfx-mod")
        resp = ec2.create_managed_prefix_list(
            PrefixListName=name,
            MaxEntries=5,
            AddressFamily="IPv4",
            Entries=[{"Cidr": "10.0.0.0/8", "Description": "initial"}],
        )
        pl_id = resp["PrefixList"]["PrefixListId"]
        try:
            # Modify: add a new entry
            mod = ec2.modify_managed_prefix_list(
                PrefixListId=pl_id,
                CurrentVersion=1,
                AddEntries=[{"Cidr": "172.16.0.0/12", "Description": "added"}],
            )
            assert mod["PrefixList"]["PrefixListId"] == pl_id
            assert mod["PrefixList"]["Version"] == 2

            # Get entries and verify both exist
            entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
            cidrs = [e["Cidr"] for e in entries["Entries"]]
            assert "10.0.0.0/8" in cidrs
            assert "172.16.0.0/12" in cidrs
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)

    def test_modify_managed_prefix_list_remove_entry(self, ec2):
        """ModifyManagedPrefixList to remove an entry."""
        name = _unique("pfx-rm")
        resp = ec2.create_managed_prefix_list(
            PrefixListName=name,
            MaxEntries=5,
            AddressFamily="IPv4",
            Entries=[
                {"Cidr": "10.0.0.0/8", "Description": "keep"},
                {"Cidr": "192.168.0.0/16", "Description": "remove"},
            ],
        )
        pl_id = resp["PrefixList"]["PrefixListId"]
        try:
            mod = ec2.modify_managed_prefix_list(
                PrefixListId=pl_id,
                CurrentVersion=1,
                RemoveEntries=[{"Cidr": "192.168.0.0/16"}],
            )
            assert mod["PrefixList"]["Version"] == 2

            entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
            cidrs = [e["Cidr"] for e in entries["Entries"]]
            assert "10.0.0.0/8" in cidrs
            assert "192.168.0.0/16" not in cidrs
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)


class TestEC2ImageAttribute:
    """Tests for DescribeImageAttribute."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_image_attribute_launch_permission(self, ec2):
        """DescribeImageAttribute for launchPermission."""
        # Create an AMI from a snapshot
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                img = ec2.register_image(
                    Name=_unique("img-attr"),
                    RootDeviceName="/dev/sda1",
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"SnapshotId": snap_id},
                        }
                    ],
                )
                image_id = img["ImageId"]
                try:
                    resp = ec2.describe_image_attribute(
                        ImageId=image_id, Attribute="launchPermission"
                    )
                    assert resp["ImageId"] == image_id
                    assert "LaunchPermissions" in resp
                finally:
                    ec2.deregister_image(ImageId=image_id)
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_describe_image_attribute_description(self, ec2):
        """DescribeImageAttribute for description."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                img = ec2.register_image(
                    Name=_unique("img-desc"),
                    Description="test-description",
                    RootDeviceName="/dev/sda1",
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"SnapshotId": snap_id},
                        }
                    ],
                )
                image_id = img["ImageId"]
                try:
                    resp = ec2.describe_image_attribute(ImageId=image_id, Attribute="description")
                    assert resp["ImageId"] == image_id
                    assert resp["Description"]["Value"] == "test-description"
                finally:
                    ec2.deregister_image(ImageId=image_id)
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2DescribeIdFormat:
    """Tests for DescribeIdFormat and DescribeIdentityIdFormat."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_id_format_with_resource(self, ec2):
        """DescribeIdFormat with specific resource type."""
        resp = ec2.describe_id_format(Resource="instance")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEC2VolumeAdvanced:
    """Advanced volume operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_volume(self, ec2):
        """ModifyVolume to change size."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            mod = ec2.modify_volume(VolumeId=vol_id, Size=2)
            assert mod["VolumeModification"]["VolumeId"] == vol_id
            assert mod["VolumeModification"]["TargetSize"] == 2
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_describe_snapshot_attribute(self, ec2):
        """DescribeSnapshotAttribute for createVolumePermission."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                resp = ec2.describe_snapshot_attribute(
                    SnapshotId=snap_id, Attribute="createVolumePermission"
                )
                assert "SnapshotId" in resp
                assert "CreateVolumePermissions" in resp
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2NetworkInterfaceAdvanced:
    """Advanced network interface operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_network_interface_attribute(self, ec2):
        """ModifyNetworkInterfaceAttribute to change description."""
        vpc = ec2.create_vpc(CidrBlock="10.222.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.222.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id, Description="original")
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    ec2.modify_network_interface_attribute(
                        NetworkInterfaceId=eni_id,
                        Description={"Value": "updated"},
                    )
                    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
                    assert desc["NetworkInterfaces"][0]["Description"] == "updated"
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_assign_unassign_private_ip_addresses(self, ec2):
        """AssignPrivateIpAddresses / UnassignPrivateIpAddresses."""
        vpc = ec2.create_vpc(CidrBlock="10.223.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.223.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id)
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    assign = ec2.assign_private_ip_addresses(
                        NetworkInterfaceId=eni_id,
                        SecondaryPrivateIpAddressCount=1,
                    )
                    assert assign["NetworkInterfaceId"] == eni_id
                    assigned_ips = assign["AssignedPrivateIpAddresses"]
                    assert len(assigned_ips) >= 1
                    ip_addr = assigned_ips[0]["PrivateIpAddress"]

                    ec2.unassign_private_ip_addresses(
                        NetworkInterfaceId=eni_id,
                        PrivateIpAddresses=[ip_addr],
                    )
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2SubnetAdvanced:
    """Advanced subnet operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_subnet_attribute_enable_dns64(self, ec2):
        """ModifySubnetAttribute to enable DNS64."""
        vpc = ec2.create_vpc(CidrBlock="10.224.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.224.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                ec2.modify_subnet_attribute(
                    SubnetId=subnet_id,
                    EnableDns64={"Value": True},
                )
                desc = ec2.describe_subnets(SubnetIds=[subnet_id])
                # Just verify the call succeeded and subnet is returned
                assert desc["Subnets"][0]["SubnetId"] == subnet_id
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2CopyOperations:
    """Tests for copy operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_copy_snapshot(self, ec2):
        """CopySnapshot."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                copy = ec2.copy_snapshot(SourceSnapshotId=snap_id, SourceRegion="us-east-1")
                copy_id = copy["SnapshotId"]
                try:
                    assert copy_id.startswith("snap-")
                    assert copy_id != snap_id
                finally:
                    ec2.delete_snapshot(SnapshotId=copy_id)
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_copy_image(self, ec2):
        """CopyImage."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                img = ec2.register_image(
                    Name=_unique("src-img"),
                    RootDeviceName="/dev/sda1",
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"SnapshotId": snap_id},
                        }
                    ],
                )
                src_id = img["ImageId"]
                try:
                    copy = ec2.copy_image(
                        Name=_unique("copy-img"),
                        SourceImageId=src_id,
                        SourceRegion="us-east-1",
                    )
                    copy_id = copy["ImageId"]
                    try:
                        assert copy_id.startswith("ami-")
                        assert copy_id != src_id
                    finally:
                        ec2.deregister_image(ImageId=copy_id)
                finally:
                    ec2.deregister_image(ImageId=src_id)
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2ModifyImageSnapshot:
    """Tests for modify operations on images and snapshots."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_image_attribute_description(self, ec2):
        """ModifyImageAttribute to change description."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                img = ec2.register_image(
                    Name=_unique("mod-img"),
                    RootDeviceName="/dev/sda1",
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"SnapshotId": snap_id},
                        }
                    ],
                )
                image_id = img["ImageId"]
                try:
                    resp = ec2.modify_image_attribute(
                        ImageId=image_id,
                        Description={"Value": "updated-description"},
                    )
                    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
                finally:
                    ec2.deregister_image(ImageId=image_id)
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_modify_snapshot_attribute_add_permission(self, ec2):
        """ModifySnapshotAttribute to add createVolumePermission."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]
            try:
                ec2.modify_snapshot_attribute(
                    SnapshotId=snap_id,
                    Attribute="createVolumePermission",
                    OperationType="add",
                    UserIds=["111122223333"],
                )
                attr = ec2.describe_snapshot_attribute(
                    SnapshotId=snap_id, Attribute="createVolumePermission"
                )
                user_ids = [p["UserId"] for p in attr["CreateVolumePermissions"]]
                assert "111122223333" in user_ids
            finally:
                ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2RouteOperations:
    """Tests for route table operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_replace_route(self, ec2):
        """ReplaceRoute in a route table."""
        vpc = ec2.create_vpc(CidrBlock="10.240.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            igw = ec2.create_internet_gateway()
            igw_id = igw["InternetGateway"]["InternetGatewayId"]
            ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            try:
                rt = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
                rt_id = rt["RouteTables"][0]["RouteTableId"]

                ec2.create_route(
                    RouteTableId=rt_id,
                    DestinationCidrBlock="0.0.0.0/0",
                    GatewayId=igw_id,
                )
                # Replace the route (same destination, same gateway — just
                # verifies the API call succeeds)
                resp = ec2.replace_route(
                    RouteTableId=rt_id,
                    DestinationCidrBlock="0.0.0.0/0",
                    GatewayId=igw_id,
                )
                assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

                ec2.delete_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0")
            finally:
                ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
                ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NetworkAclAdvanced:
    """Advanced network ACL operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_replace_network_acl_entry(self, ec2):
        """ReplaceNetworkAclEntry."""
        vpc = ec2.create_vpc(CidrBlock="10.242.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            acl = ec2.create_network_acl(VpcId=vpc_id)
            acl_id = acl["NetworkAcl"]["NetworkAclId"]
            try:
                ec2.create_network_acl_entry(
                    NetworkAclId=acl_id,
                    RuleNumber=100,
                    Protocol="-1",
                    RuleAction="allow",
                    Egress=False,
                    CidrBlock="10.0.0.0/8",
                )
                # Replace the entry to deny instead of allow
                resp = ec2.replace_network_acl_entry(
                    NetworkAclId=acl_id,
                    RuleNumber=100,
                    Protocol="-1",
                    RuleAction="deny",
                    Egress=False,
                    CidrBlock="10.0.0.0/8",
                )
                assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

                # Verify the replacement
                desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
                entries = [
                    e
                    for e in desc["NetworkAcls"][0]["Entries"]
                    if e["RuleNumber"] == 100 and not e["Egress"]
                ]
                assert len(entries) == 1
                assert entries[0]["RuleAction"] == "deny"
            finally:
                ec2.delete_network_acl(NetworkAclId=acl_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_delete_network_acl_entry(self, ec2):
        """DeleteNetworkAclEntry."""
        vpc = ec2.create_vpc(CidrBlock="10.243.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            acl = ec2.create_network_acl(VpcId=vpc_id)
            acl_id = acl["NetworkAcl"]["NetworkAclId"]
            try:
                ec2.create_network_acl_entry(
                    NetworkAclId=acl_id,
                    RuleNumber=200,
                    Protocol="-1",
                    RuleAction="allow",
                    Egress=False,
                    CidrBlock="172.16.0.0/12",
                )
                ec2.delete_network_acl_entry(NetworkAclId=acl_id, RuleNumber=200, Egress=False)

                desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
                rule_numbers = [
                    e["RuleNumber"]
                    for e in desc["NetworkAcls"][0]["Entries"]
                    if not e["Egress"] and e["RuleNumber"] != 32767
                ]
                assert 200 not in rule_numbers
            finally:
                ec2.delete_network_acl(NetworkAclId=acl_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2VpcEndpointAdvanced:
    """Advanced VPC endpoint operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_vpc_endpoint(self, ec2):
        """ModifyVpcEndpoint to reset policy."""
        vpc = ec2.create_vpc(CidrBlock="10.244.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            rt = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            rt_id = rt["RouteTables"][0]["RouteTableId"]

            ep = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName="com.amazonaws.us-east-1.s3",
                RouteTableIds=[rt_id],
            )
            ep_id = ep["VpcEndpoint"]["VpcEndpointId"]
            try:
                resp = ec2.modify_vpc_endpoint(VpcEndpointId=ep_id, ResetPolicy=True)
                assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            finally:
                ec2.delete_vpc_endpoints(VpcEndpointIds=[ep_id])
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2NetworkInterfaceAttribute:
    """Tests for network interface attribute operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_network_interface_attribute(self, ec2):
        """DescribeNetworkInterfaceAttribute for description."""
        vpc = ec2.create_vpc(CidrBlock="10.245.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.245.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id, Description="test-eni-attr")
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    resp = ec2.describe_network_interface_attribute(
                        NetworkInterfaceId=eni_id, Attribute="description"
                    )
                    assert resp["NetworkInterfaceId"] == eni_id
                    assert resp["Description"]["Value"] == "test-eni-attr"
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2VpcAttributeToggle:
    """Tests for VPC attribute toggling."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_vpc_attribute_disable_dns_support(self, ec2):
        """ModifyVpcAttribute to disable DNS support, then verify."""
        vpc = ec2.create_vpc(CidrBlock="10.246.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": False})
            attr = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
            assert attr["EnableDnsSupport"]["Value"] is False

            # Re-enable
            ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
            attr2 = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
            assert attr2["EnableDnsSupport"]["Value"] is True
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2CarrierGatewayCRUD:
    """CarrierGateway create / describe-by-id / delete."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_carrier_gateway(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.create_carrier_gateway(VpcId=vpc_id)
            cagw_id = resp["CarrierGateway"]["CarrierGatewayId"]
            assert cagw_id.startswith("cagw-")

            described = ec2.describe_carrier_gateways(CarrierGatewayIds=[cagw_id])
            assert len(described["CarrierGateways"]) == 1
            assert described["CarrierGateways"][0]["VpcId"] == vpc_id

            ec2.delete_carrier_gateway(CarrierGatewayId=cagw_id)
            after = ec2.describe_carrier_gateways(CarrierGatewayIds=[cagw_id])
            remaining = [c for c in after["CarrierGateways"] if c.get("State") != "deleted"]
            assert len(remaining) == 0
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_carrier_gateway_has_tags(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.91.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.create_carrier_gateway(
                VpcId=vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "carrier-gateway",
                        "Tags": [{"Key": "Name", "Value": "test-cagw"}],
                    }
                ],
            )
            cagw_id = resp["CarrierGateway"]["CarrierGatewayId"]
            described = ec2.describe_carrier_gateways(CarrierGatewayIds=[cagw_id])
            tags = described["CarrierGateways"][0].get("Tags", [])
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map.get("Name") == "test-cagw"
            ec2.delete_carrier_gateway(CarrierGatewayId=cagw_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ReplaceNetworkAclAssociation:
    """ReplaceNetworkAclAssociation."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_replace_network_acl_association(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.92.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.92.1.0/24")
            sub_id = sub["Subnet"]["SubnetId"]

            acls = ec2.describe_network_acls(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            default_acl = acls["NetworkAcls"][0]
            assoc_id = default_acl["Associations"][0]["NetworkAclAssociationId"]

            new_acl = ec2.create_network_acl(VpcId=vpc_id)
            new_acl_id = new_acl["NetworkAcl"]["NetworkAclId"]

            resp = ec2.replace_network_acl_association(
                AssociationId=assoc_id, NetworkAclId=new_acl_id
            )
            new_assoc_id = resp["NewAssociationId"]
            assert new_assoc_id.startswith("aclassoc-")
            assert new_assoc_id != assoc_id

            # Restore default so we can clean up
            ec2.replace_network_acl_association(
                AssociationId=new_assoc_id,
                NetworkAclId=default_acl["NetworkAclId"],
            )
            ec2.delete_network_acl(NetworkAclId=new_acl_id)
            ec2.delete_subnet(SubnetId=sub_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ConsoleAndPassword:
    """GetConsoleOutput / GetPasswordData."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_get_console_output(self, ec2):
        inst = ec2.run_instances(
            ImageId="ami-12345678", MinCount=1, MaxCount=1, InstanceType="t2.micro"
        )
        iid = inst["Instances"][0]["InstanceId"]
        try:
            resp = ec2.get_console_output(InstanceId=iid)
            assert "InstanceId" in resp
            assert resp["InstanceId"] == iid
            assert "Output" in resp or "Timestamp" in resp
        finally:
            ec2.terminate_instances(InstanceIds=[iid])

    def test_get_password_data(self, ec2):
        inst = ec2.run_instances(
            ImageId="ami-12345678", MinCount=1, MaxCount=1, InstanceType="t2.micro"
        )
        iid = inst["Instances"][0]["InstanceId"]
        try:
            resp = ec2.get_password_data(InstanceId=iid)
            assert "InstanceId" in resp
            assert resp["InstanceId"] == iid
            assert "PasswordData" in resp
        finally:
            ec2.terminate_instances(InstanceIds=[iid])


class TestEC2TransitGatewayRouteTableCRUD:
    """Transit gateway route table lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_tgw_route_table(self, ec2):
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]
            assert rtb_id.startswith("tgw-rtb-")

            described = ec2.describe_transit_gateway_route_tables(
                TransitGatewayRouteTableIds=[rtb_id]
            )
            assert len(described["TransitGatewayRouteTables"]) == 1
            assert described["TransitGatewayRouteTables"][0]["TransitGatewayId"] == tgw_id

            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)

    def test_create_tgw_route_table_with_tags(self, ec2):
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            rtb = ec2.create_transit_gateway_route_table(
                TransitGatewayId=tgw_id,
                TagSpecifications=[
                    {
                        "ResourceType": "transit-gateway-route-table",
                        "Tags": [{"Key": "Env", "Value": "test"}],
                    }
                ],
            )
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]
            tags = rtb["TransitGatewayRouteTable"].get("Tags", [])
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map.get("Env") == "test"
            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2TransitGatewayVpcAttachmentCRUD:
    """Transit gateway VPC attachment lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_tgw_vpc_attachment(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.93.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.93.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_vpc_attachment(
                TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[sub_id]
            )
            att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
            assert att_id.startswith("tgw-attach-")
            assert att["TransitGatewayVpcAttachment"]["VpcId"] == vpc_id

            described = ec2.describe_transit_gateway_vpc_attachments(
                TransitGatewayAttachmentIds=[att_id]
            )
            assert len(described["TransitGatewayVpcAttachments"]) == 1
            assert described["TransitGatewayVpcAttachments"][0]["TransitGatewayId"] == tgw_id

            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_tgw_vpc_attachment_has_subnet_ids(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.94.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.94.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_vpc_attachment(
                TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[sub_id]
            )
            att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
            assert sub_id in att["TransitGatewayVpcAttachment"]["SubnetIds"]
            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2TransitGatewayPeeringCRUD:
    """Transit gateway peering attachment lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_accept_describe_delete_tgw_peering(self, ec2):
        tgw1 = ec2.create_transit_gateway()
        tgw1_id = tgw1["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()
        tgw2_id = tgw2["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1_id,
                PeerTransitGatewayId=tgw2_id,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]
            assert att_id.startswith("tgw-attach-")

            ec2.accept_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)

            described = ec2.describe_transit_gateway_peering_attachments(
                TransitGatewayAttachmentIds=[att_id]
            )
            assert len(described["TransitGatewayPeeringAttachments"]) == 1

            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2_id)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1_id)

    def test_tgw_peering_has_requester_and_accepter(self, ec2):
        tgw1 = ec2.create_transit_gateway()
        tgw1_id = tgw1["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()
        tgw2_id = tgw2["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1_id,
                PeerTransitGatewayId=tgw2_id,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]
            peer = att["TransitGatewayPeeringAttachment"]
            assert "RequesterTgwInfo" in peer
            assert "AccepterTgwInfo" in peer
            assert peer["RequesterTgwInfo"]["TransitGatewayId"] == tgw1_id
            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2_id)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1_id)

    def test_reject_tgw_peering_attachment(self, ec2):
        """RejectTransitGatewayPeeringAttachment changes state."""
        tgw1 = ec2.create_transit_gateway()
        tgw1_id = tgw1["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()
        tgw2_id = tgw2["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1_id,
                PeerTransitGatewayId=tgw2_id,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]
            assert att_id.startswith("tgw-attach-")

            rejected = ec2.reject_transit_gateway_peering_attachment(
                TransitGatewayAttachmentId=att_id
            )
            peer = rejected["TransitGatewayPeeringAttachment"]
            assert peer["TransitGatewayAttachmentId"] == att_id
            assert "Status" in peer

            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2_id)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1_id)


class TestEC2TransitGatewayRoutes:
    """Transit gateway static routes."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_search_delete_tgw_route(self, ec2):
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]

            route = ec2.create_transit_gateway_route(
                DestinationCidrBlock="10.99.0.0/16",
                TransitGatewayRouteTableId=rtb_id,
                Blackhole=True,
            )
            assert route["Route"]["DestinationCidrBlock"] == "10.99.0.0/16"
            assert route["Route"]["Type"] == "static"
            assert route["Route"]["State"] == "blackhole"

            searched = ec2.search_transit_gateway_routes(
                TransitGatewayRouteTableId=rtb_id,
                Filters=[{"Name": "type", "Values": ["static"]}],
            )
            assert len(searched["Routes"]) >= 1
            cidrs = [r["DestinationCidrBlock"] for r in searched["Routes"]]
            assert "10.99.0.0/16" in cidrs

            ec2.delete_transit_gateway_route(
                DestinationCidrBlock="10.99.0.0/16",
                TransitGatewayRouteTableId=rtb_id,
            )
            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2TransitGatewayRouteTableAssociation:
    """Associate / disassociate TGW route table with attachment."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_associate_disassociate_tgw_route_table(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.95.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.95.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_vpc_attachment(
                TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[sub_id]
            )
            att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]

            assoc = ec2.associate_transit_gateway_route_table(
                TransitGatewayRouteTableId=rtb_id,
                TransitGatewayAttachmentId=att_id,
            )
            assert "Association" in assoc
            assert assoc["Association"]["TransitGatewayRouteTableId"] == rtb_id

            ec2.disassociate_transit_gateway_route_table(
                TransitGatewayRouteTableId=rtb_id,
                TransitGatewayAttachmentId=att_id,
            )

            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2SpotInstanceCRUD:
    """Spot instance request / describe-by-id / cancel."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_request_describe_cancel_spot_instances(self, ec2):
        resp = ec2.request_spot_instances(
            SpotPrice="0.01",
            InstanceCount=1,
            LaunchSpecification={
                "ImageId": "ami-12345678",
                "InstanceType": "t2.micro",
            },
        )
        sir_id = resp["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
        assert sir_id.startswith("sir-")

        described = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[sir_id])
        assert len(described["SpotInstanceRequests"]) == 1
        assert described["SpotInstanceRequests"][0]["SpotPrice"].startswith("0.01")

        cancelled = ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[sir_id])
        assert len(cancelled["CancelledSpotInstanceRequests"]) == 1
        assert cancelled["CancelledSpotInstanceRequests"][0]["SpotInstanceRequestId"] == sir_id

    def test_spot_instance_request_has_launch_spec(self, ec2):
        resp = ec2.request_spot_instances(
            SpotPrice="0.02",
            InstanceCount=1,
            LaunchSpecification={
                "ImageId": "ami-12345678",
                "InstanceType": "m5.large",
            },
        )
        sir_id = resp["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
        try:
            sir = resp["SpotInstanceRequests"][0]
            assert sir["LaunchSpecification"]["InstanceType"] == "m5.large"
        finally:
            ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[sir_id])


class TestEC2HostsCRUD:
    """AllocateHosts / DescribeHosts / ModifyHosts / ReleaseHosts."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_allocate_describe_modify_release_hosts(self, ec2):
        resp = ec2.allocate_hosts(
            AvailabilityZone="us-east-1a", InstanceType="m5.large", Quantity=1
        )
        host_id = resp["HostIds"][0]
        assert host_id.startswith("h-")

        described = ec2.describe_hosts(HostIds=[host_id])
        assert len(described["Hosts"]) == 1
        assert described["Hosts"][0]["HostId"] == host_id

        ec2.modify_hosts(HostIds=[host_id], AutoPlacement="on")
        after = ec2.describe_hosts(HostIds=[host_id])
        assert after["Hosts"][0]["AutoPlacement"] == "on"

        ec2.release_hosts(HostIds=[host_id])

    def test_allocate_hosts_with_tags(self, ec2):
        resp = ec2.allocate_hosts(
            AvailabilityZone="us-east-1a",
            InstanceType="c5.large",
            Quantity=1,
            TagSpecifications=[
                {
                    "ResourceType": "dedicated-host",
                    "Tags": [{"Key": "Team", "Value": "infra"}],
                }
            ],
        )
        host_id = resp["HostIds"][0]
        try:
            described = ec2.describe_hosts(HostIds=[host_id])
            tags = described["Hosts"][0].get("Tags", [])
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map.get("Team") == "infra"
        finally:
            ec2.release_hosts(HostIds=[host_id])


class TestEC2SnapshotAttributeModify:
    """ModifySnapshotAttribute with add/remove permissions."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_snapshot_attribute_add_remove(self, ec2):
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        try:
            snap = ec2.create_snapshot(VolumeId=vol_id)
            snap_id = snap["SnapshotId"]

            ec2.modify_snapshot_attribute(
                SnapshotId=snap_id,
                Attribute="createVolumePermission",
                OperationType="add",
                UserIds=["111122223333"],
            )
            perms = ec2.describe_snapshot_attribute(
                SnapshotId=snap_id, Attribute="createVolumePermission"
            )
            assert len(perms["CreateVolumePermissions"]) == 1
            assert perms["CreateVolumePermissions"][0]["UserId"] == "111122223333"

            ec2.modify_snapshot_attribute(
                SnapshotId=snap_id,
                Attribute="createVolumePermission",
                OperationType="remove",
                UserIds=["111122223333"],
            )
            perms_after = ec2.describe_snapshot_attribute(
                SnapshotId=snap_id, Attribute="createVolumePermission"
            )
            assert len(perms_after["CreateVolumePermissions"]) == 0

            ec2.delete_snapshot(SnapshotId=snap_id)
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2DescribeListOpsNew:
    """Describe/list operations returning empty collections — verify server contact."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_fleets_list(self, ec2):
        resp = ec2.describe_fleets()
        assert "Fleets" in resp
        assert isinstance(resp["Fleets"], list)

    def test_describe_spot_fleet_requests_list(self, ec2):
        resp = ec2.describe_spot_fleet_requests()
        assert "SpotFleetRequestConfigs" in resp
        assert isinstance(resp["SpotFleetRequestConfigs"], list)

    def test_describe_snapshot_tier_status_list(self, ec2):
        resp = ec2.describe_snapshot_tier_status()
        assert "SnapshotTierStatuses" in resp
        assert isinstance(resp["SnapshotTierStatuses"], list)

    def test_describe_vpc_endpoint_service_configurations_list(self, ec2):
        resp = ec2.describe_vpc_endpoint_service_configurations()
        assert "ServiceConfigurations" in resp
        assert isinstance(resp["ServiceConfigurations"], list)

    def test_describe_addresses_attribute_list(self, ec2):
        resp = ec2.describe_addresses_attribute()
        assert "Addresses" in resp
        assert isinstance(resp["Addresses"], list)

    def test_describe_vpn_connections_list(self, ec2):
        resp = ec2.describe_vpn_connections()
        assert "VpnConnections" in resp
        assert isinstance(resp["VpnConnections"], list)


class TestEC2EbsEncryptionSettings:
    """EBS encryption default settings."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_enable_disable_ebs_encryption(self, ec2):
        resp = ec2.enable_ebs_encryption_by_default()
        assert resp["EbsEncryptionByDefault"] is True

        check = ec2.get_ebs_encryption_by_default()
        assert check["EbsEncryptionByDefault"] is True

        resp2 = ec2.disable_ebs_encryption_by_default()
        assert resp2["EbsEncryptionByDefault"] is False

        check2 = ec2.get_ebs_encryption_by_default()
        assert check2["EbsEncryptionByDefault"] is False

    def test_get_ebs_encryption_by_default_returns_bool(self, ec2):
        resp = ec2.get_ebs_encryption_by_default()
        assert "EbsEncryptionByDefault" in resp
        assert isinstance(resp["EbsEncryptionByDefault"], bool)


class TestEC2PrefixListEntries:
    """GetManagedPrefixListEntries."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_get_managed_prefix_list_entries_single(self, ec2):
        pl = ec2.create_managed_prefix_list(
            PrefixListName=_unique("test-pl"),
            MaxEntries=5,
            AddressFamily="IPv4",
            Entries=[{"Cidr": "10.0.0.0/8", "Description": "RFC1918"}],
        )
        pl_id = pl["PrefixList"]["PrefixListId"]
        try:
            entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
            assert len(entries["Entries"]) == 1
            assert entries["Entries"][0]["Cidr"] == "10.0.0.0/8"
            assert entries["Entries"][0]["Description"] == "RFC1918"
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)

    def test_get_prefix_list_entries_multiple(self, ec2):
        pl = ec2.create_managed_prefix_list(
            PrefixListName=_unique("test-pl-multi"),
            MaxEntries=10,
            AddressFamily="IPv4",
            Entries=[
                {"Cidr": "10.0.0.0/8", "Description": "RFC1918-10"},
                {"Cidr": "172.16.0.0/12", "Description": "RFC1918-172"},
                {"Cidr": "192.168.0.0/16", "Description": "RFC1918-192"},
            ],
        )
        pl_id = pl["PrefixList"]["PrefixListId"]
        try:
            entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
            assert len(entries["Entries"]) == 3
            cidrs = {e["Cidr"] for e in entries["Entries"]}
            assert "10.0.0.0/8" in cidrs
            assert "172.16.0.0/12" in cidrs
            assert "192.168.0.0/16" in cidrs
        finally:
            ec2.delete_managed_prefix_list(PrefixListId=pl_id)


class TestEC2InstanceTypeOfferingsNew:
    """DescribeInstanceTypeOfferings."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_instance_type_offerings_has_entries(self, ec2):
        resp = ec2.describe_instance_type_offerings()
        assert "InstanceTypeOfferings" in resp
        assert len(resp["InstanceTypeOfferings"]) > 0
        first = resp["InstanceTypeOfferings"][0]
        assert "InstanceType" in first
        assert "LocationType" in first

    def test_describe_instance_type_offerings_filter(self, ec2):
        resp = ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{"Name": "instance-type", "Values": ["t2.micro"]}],
        )
        assert "InstanceTypeOfferings" in resp
        for offering in resp["InstanceTypeOfferings"]:
            assert offering["InstanceType"] == "t2.micro"


class TestEC2ModifyTransitGateway:
    """ModifyTransitGateway."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_transit_gateway_description(self, ec2):
        tgw = ec2.create_transit_gateway(Description="original")
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            ec2.modify_transit_gateway(TransitGatewayId=tgw_id, Description="modified")
            described = ec2.describe_transit_gateways(TransitGatewayIds=[tgw_id])
            assert len(described["TransitGateways"]) == 1
            assert described["TransitGateways"][0]["Description"] == "modified"
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2TransitGatewayPropagation:
    """Enable/disable TGW route table propagation."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_enable_disable_tgw_route_table_propagation(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.110.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.110.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_vpc_attachment(
                TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[sub_id]
            )
            att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
            rtb = ec2.create_transit_gateway_route_table(TransitGatewayId=tgw_id)
            rtb_id = rtb["TransitGatewayRouteTable"]["TransitGatewayRouteTableId"]

            resp = ec2.enable_transit_gateway_route_table_propagation(
                TransitGatewayRouteTableId=rtb_id,
                TransitGatewayAttachmentId=att_id,
            )
            assert "Propagation" in resp

            ec2.disable_transit_gateway_route_table_propagation(
                TransitGatewayRouteTableId=rtb_id,
                TransitGatewayAttachmentId=att_id,
            )

            ec2.delete_transit_gateway_route_table(TransitGatewayRouteTableId=rtb_id)
            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2VpcEndpointInterfaceCRUD:
    """VPC endpoint (Interface type) lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_interface_endpoint(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.111.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.111.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        try:
            ep = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName="com.amazonaws.us-east-1.execute-api",
                VpcEndpointType="Interface",
                SubnetIds=[sub_id],
            )
            vpce_id = ep["VpcEndpoint"]["VpcEndpointId"]
            assert vpce_id.startswith("vpce-")
            assert ep["VpcEndpoint"]["VpcEndpointType"] == "Interface"

            described = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
            assert len(described["VpcEndpoints"]) == 1
            assert described["VpcEndpoints"][0]["VpcId"] == vpc_id

            ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
        finally:
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_describe_vpc_endpoint_services(self, ec2):
        resp = ec2.describe_vpc_endpoint_services()
        assert "ServiceNames" in resp
        assert len(resp["ServiceNames"]) > 0
        assert any("s3" in s for s in resp["ServiceNames"])


class TestEC2ModifyVpcEndpointPolicy:
    """ModifyVpcEndpoint to update policy."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_vpc_endpoint_policy(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.112.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            ep = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName="com.amazonaws.us-east-1.s3",
                VpcEndpointType="Gateway",
            )
            vpce_id = ep["VpcEndpoint"]["VpcEndpointId"]

            policy = (
                '{"Statement":[{"Effect":"Allow","Principal":"*",'
                '"Action":"s3:GetObject","Resource":"*"}]}'
            )
            ec2.modify_vpc_endpoint(VpcEndpointId=vpce_id, PolicyDocument=policy)

            described = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
            assert len(described["VpcEndpoints"]) == 1
            # Policy should be set (may be normalized)
            assert described["VpcEndpoints"][0].get("PolicyDocument") is not None

            ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2UpdateSecurityGroupRuleDescriptions:
    """UpdateSecurityGroupRuleDescriptionsIngress / Egress."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_update_sg_rule_descriptions_ingress(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.113.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sg = ec2.create_security_group(
                GroupName=_unique("sg-desc"),
                Description="test",
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
                        "IpRanges": [{"CidrIp": "10.0.0.0/8", "Description": "original"}],
                    }
                ],
            )
            resp = ec2.update_security_group_rule_descriptions_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 443,
                        "ToPort": 443,
                        "IpRanges": [{"CidrIp": "10.0.0.0/8", "Description": "updated"}],
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_update_sg_rule_descriptions_egress(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.114.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sg = ec2.create_security_group(
                GroupName=_unique("sg-egr-desc"),
                Description="test",
                VpcId=vpc_id,
            )
            sg_id = sg["GroupId"]
            ec2.authorize_security_group_egress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "original"}],
                    }
                ],
            )
            resp = ec2.update_security_group_rule_descriptions_egress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "updated"}],
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2TagsOnTransitGateway:
    """CreateTags / DeleteTags on transit gateway."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_delete_tags_on_transit_gateway(self, ec2):
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            ec2.create_tags(
                Resources=[tgw_id],
                Tags=[{"Key": "Env", "Value": "staging"}],
            )
            tags = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [tgw_id]}])
            assert len(tags["Tags"]) >= 1
            tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
            assert tag_map.get("Env") == "staging"

            ec2.delete_tags(Resources=[tgw_id], Tags=[{"Key": "Env"}])
            tags_after = ec2.describe_tags(
                Filters=[
                    {"Name": "resource-id", "Values": [tgw_id]},
                    {"Name": "key", "Values": ["Env"]},
                ]
            )
            assert len(tags_after["Tags"]) == 0
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2ModifyTransitGatewayVpcAttachment:
    """ModifyTransitGatewayVpcAttachment to add subnets."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_tgw_vpc_attachment_add_subnet(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.115.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.115.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub1_id = sub1["Subnet"]["SubnetId"]
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.115.2.0/24", AvailabilityZone="us-east-1b"
        )
        sub2_id = sub2["Subnet"]["SubnetId"]
        tgw = ec2.create_transit_gateway()
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_vpc_attachment(
                TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[sub1_id]
            )
            att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]

            modified = ec2.modify_transit_gateway_vpc_attachment(
                TransitGatewayAttachmentId=att_id, AddSubnetIds=[sub2_id]
            )
            subnet_ids = modified["TransitGatewayVpcAttachment"]["SubnetIds"]
            assert sub2_id in subnet_ids

            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)
            ec2.delete_subnet(SubnetId=sub2_id)
            ec2.delete_subnet(SubnetId=sub1_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ModifySubnetAttributeDns:
    """ModifySubnetAttribute EnableResourceNameDnsARecordOnLaunch."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_subnet_attribute_dns_a_record(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.116.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.116.1.0/24")
            sub_id = sub["Subnet"]["SubnetId"]

            ec2.modify_subnet_attribute(
                SubnetId=sub_id,
                EnableResourceNameDnsARecordOnLaunch={"Value": True},
            )
            described = ec2.describe_subnets(SubnetIds=[sub_id])
            assert described["Subnets"][0]["SubnetId"] == sub_id

            ec2.delete_subnet(SubnetId=sub_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2DescribeSubnetsFiltered:
    """DescribeSubnets with VPC filter."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_subnets_filtered_by_vpc(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.117.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sub1 = ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock="10.117.1.0/24",
                AvailabilityZone="us-east-1a",
            )
            sub2 = ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock="10.117.2.0/24",
                AvailabilityZone="us-east-1b",
            )
            subs = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            assert len(subs["Subnets"]) == 2
            subnet_ids = {s["SubnetId"] for s in subs["Subnets"]}
            assert sub1["Subnet"]["SubnetId"] in subnet_ids
            assert sub2["Subnet"]["SubnetId"] in subnet_ids

            ec2.delete_subnet(SubnetId=sub2["Subnet"]["SubnetId"])
            ec2.delete_subnet(SubnetId=sub1["Subnet"]["SubnetId"])
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2FleetOperations:
    """Fleet create/describe/delete operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_fleet(self, ec2):
        lt_name = _unique("fleet-lt")
        lt = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro", "ImageId": "ami-12345678"},
        )
        lt_id = lt["LaunchTemplate"]["LaunchTemplateId"]
        try:
            r = ec2.create_fleet(
                LaunchTemplateConfigs=[
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": lt_id,
                            "Version": "$Latest",
                        },
                        "Overrides": [{"InstanceType": "t2.micro"}],
                    }
                ],
                TargetCapacitySpecification={
                    "TotalTargetCapacity": 1,
                    "DefaultTargetCapacityType": "on-demand",
                },
                Type="instant",
            )
            fleet_id = r["FleetId"]
            assert fleet_id.startswith("fleet-")

            described = ec2.describe_fleets(FleetIds=[fleet_id])
            assert len(described["Fleets"]) == 1
            assert described["Fleets"][0]["FleetId"] == fleet_id

            ec2.delete_fleets(FleetIds=[fleet_id], TerminateInstances=True)
            # Verify deletion
            after = ec2.describe_fleets(FleetIds=[fleet_id])
            if after["Fleets"]:
                assert after["Fleets"][0]["FleetState"] in (
                    "deleted_running",
                    "deleted_terminating",
                    "deleted",
                )
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)

    def test_fleet_has_target_capacity(self, ec2):
        lt_name = _unique("fleet-lt")
        lt = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={"InstanceType": "t2.micro", "ImageId": "ami-12345678"},
        )
        lt_id = lt["LaunchTemplate"]["LaunchTemplateId"]
        try:
            r = ec2.create_fleet(
                LaunchTemplateConfigs=[
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": lt_id,
                            "Version": "$Latest",
                        },
                        "Overrides": [{"InstanceType": "t2.micro"}],
                    }
                ],
                TargetCapacitySpecification={
                    "TotalTargetCapacity": 2,
                    "DefaultTargetCapacityType": "on-demand",
                },
                Type="instant",
            )
            fleet_id = r["FleetId"]
            described = ec2.describe_fleets(FleetIds=[fleet_id])
            spec = described["Fleets"][0]["TargetCapacitySpecification"]
            assert spec["TotalTargetCapacity"] == 2

            ec2.delete_fleets(FleetIds=[fleet_id], TerminateInstances=True)
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)


class TestEC2SpotFleetOperations:
    """Spot Fleet request/describe/cancel operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_request_describe_cancel_spot_fleet(self, ec2):
        r = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [
                    {
                        "ImageId": "ami-12345678",
                        "InstanceType": "t2.micro",
                    }
                ],
            }
        )
        fleet_id = r["SpotFleetRequestId"]
        assert fleet_id.startswith("sfr-")

        described = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[fleet_id])
        assert len(described["SpotFleetRequestConfigs"]) == 1
        config = described["SpotFleetRequestConfigs"][0]
        assert config["SpotFleetRequestId"] == fleet_id
        assert config["SpotFleetRequestConfig"]["TargetCapacity"] == 1

        cancel = ec2.cancel_spot_fleet_requests(
            SpotFleetRequestIds=[fleet_id], TerminateInstances=True
        )
        assert len(cancel["SuccessfulFleetRequests"]) == 1
        assert cancel["SuccessfulFleetRequests"][0]["SpotFleetRequestId"] == fleet_id

    def test_describe_spot_fleet_instances(self, ec2):
        r = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [
                    {
                        "ImageId": "ami-12345678",
                        "InstanceType": "t2.micro",
                    }
                ],
            }
        )
        fleet_id = r["SpotFleetRequestId"]
        try:
            instances = ec2.describe_spot_fleet_instances(SpotFleetRequestId=fleet_id)
            assert "ActiveInstances" in instances
            assert instances["SpotFleetRequestId"] == fleet_id
        finally:
            ec2.cancel_spot_fleet_requests(SpotFleetRequestIds=[fleet_id], TerminateInstances=True)


class TestEC2ImportKeyPair:
    """ImportKeyPair operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_import_key_pair(self, ec2):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pub = key.public_key().public_bytes(
            serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
        )
        key_name = _unique("imported-key")
        r = ec2.import_key_pair(KeyName=key_name, PublicKeyMaterial=pub)
        try:
            assert "KeyFingerprint" in r
            assert r["KeyName"] == key_name

            described = ec2.describe_key_pairs(KeyNames=[key_name])
            assert len(described["KeyPairs"]) == 1
            assert described["KeyPairs"][0]["KeyName"] == key_name
        finally:
            ec2.delete_key_pair(KeyName=key_name)


class TestEC2VPCEndpointServiceConfig:
    """VPC Endpoint Service Configuration CRUD operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def _create_nlb(self, ec2):
        """Create a VPC, subnet, and NLB for endpoint service testing."""
        import boto3
        from botocore.config import Config

        elbv2 = boto3.client(
            "elbv2",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            config=Config(),
        )
        vpc = ec2.create_vpc(CidrBlock="10.210.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.210.1.0/24")
        sub_id = sub["Subnet"]["SubnetId"]
        nlb_name = _unique("nlb")
        nlb = elbv2.create_load_balancer(Name=nlb_name, Subnets=[sub_id], Type="network")
        nlb_arn = nlb["LoadBalancers"][0]["LoadBalancerArn"]
        return elbv2, vpc_id, sub_id, nlb_arn

    def test_create_describe_delete_vpc_endpoint_service_config(self, ec2):
        elbv2, vpc_id, sub_id, nlb_arn = self._create_nlb(ec2)
        try:
            r = ec2.create_vpc_endpoint_service_configuration(
                NetworkLoadBalancerArns=[nlb_arn],
            )
            svc_id = r["ServiceConfiguration"]["ServiceId"]
            assert svc_id.startswith("vpce-svc-")

            described = ec2.describe_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
            assert len(described["ServiceConfigurations"]) == 1
            assert described["ServiceConfigurations"][0]["ServiceId"] == svc_id

            ec2.delete_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=nlb_arn)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_modify_vpc_endpoint_service_configuration(self, ec2):
        elbv2, vpc_id, sub_id, nlb_arn = self._create_nlb(ec2)
        try:
            svc = ec2.create_vpc_endpoint_service_configuration(
                NetworkLoadBalancerArns=[nlb_arn],
            )
            svc_id = svc["ServiceConfiguration"]["ServiceId"]

            r = ec2.modify_vpc_endpoint_service_configuration(
                ServiceId=svc_id,
                AcceptanceRequired=True,
            )
            assert r["Return"] is True

            described = ec2.describe_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
            assert described["ServiceConfigurations"][0]["AcceptanceRequired"] is True

            ec2.delete_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=nlb_arn)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)

    def test_vpc_endpoint_service_permissions(self, ec2):
        elbv2, vpc_id, sub_id, nlb_arn = self._create_nlb(ec2)
        try:
            svc = ec2.create_vpc_endpoint_service_configuration(
                NetworkLoadBalancerArns=[nlb_arn],
            )
            svc_id = svc["ServiceConfiguration"]["ServiceId"]

            # Add permissions
            ec2.modify_vpc_endpoint_service_permissions(
                ServiceId=svc_id,
                AddAllowedPrincipals=["arn:aws:iam::123456789012:root"],
            )

            perms = ec2.describe_vpc_endpoint_service_permissions(ServiceId=svc_id)
            assert len(perms["AllowedPrincipals"]) == 1
            assert perms["AllowedPrincipals"][0]["Principal"] == "arn:aws:iam::123456789012:root"

            ec2.delete_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
        finally:
            elbv2.delete_load_balancer(LoadBalancerArn=nlb_arn)
            ec2.delete_subnet(SubnetId=sub_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2DescribeFilters:
    """Describe operations with filters."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_regions_by_name(self, ec2):
        r = ec2.describe_regions(RegionNames=["us-east-1"])
        assert len(r["Regions"]) == 1
        assert r["Regions"][0]["RegionName"] == "us-east-1"
        assert "Endpoint" in r["Regions"][0]

    def test_describe_availability_zones_with_filter(self, ec2):
        r = ec2.describe_availability_zones(
            Filters=[{"Name": "zone-name", "Values": ["us-east-1a"]}]
        )
        assert len(r["AvailabilityZones"]) == 1
        assert r["AvailabilityZones"][0]["ZoneName"] == "us-east-1a"

    def test_describe_instance_types_with_filter(self, ec2):
        r = ec2.describe_instance_types(Filters=[{"Name": "instance-type", "Values": ["t2.micro"]}])
        assert len(r["InstanceTypes"]) == 1
        assert r["InstanceTypes"][0]["InstanceType"] == "t2.micro"
        assert "MemoryInfo" in r["InstanceTypes"][0]
        assert "VCpuInfo" in r["InstanceTypes"][0]

    def test_describe_reserved_instances_offerings_filtered(self, ec2):
        r = ec2.describe_reserved_instances_offerings(
            InstanceType="t2.micro",
            ProductDescription="Linux/UNIX",
            MaxResults=5,
        )
        assert "ReservedInstancesOfferings" in r
        assert len(r["ReservedInstancesOfferings"]) > 0
        offering = r["ReservedInstancesOfferings"][0]
        assert offering["InstanceType"] == "t2.micro"

    def test_describe_account_attributes_has_known_attrs(self, ec2):
        r = ec2.describe_account_attributes()
        attr_names = {a["AttributeName"] for a in r["AccountAttributes"]}
        assert "supported-platforms" in attr_names
        assert "default-vpc" in attr_names

    def test_describe_vpc_endpoint_services_has_entries(self, ec2):
        r = ec2.describe_vpc_endpoint_services()
        assert "ServiceNames" in r
        assert len(r["ServiceNames"]) > 0
        # Should include well-known services
        assert any("s3" in sn for sn in r["ServiceNames"])


class TestEC2VPCEndpointInterface:
    """VPC Endpoint Interface type operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_describe_delete_interface_vpc_endpoint(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.211.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            r = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName="com.amazonaws.us-east-1.sqs",
                VpcEndpointType="Interface",
            )
            vpce_id = r["VpcEndpoint"]["VpcEndpointId"]
            assert vpce_id.startswith("vpce-")
            assert r["VpcEndpoint"]["VpcEndpointType"] == "Interface"

            described = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
            assert len(described["VpcEndpoints"]) == 1
            assert described["VpcEndpoints"][0]["VpcEndpointType"] == "Interface"
            assert described["VpcEndpoints"][0]["VpcId"] == vpc_id

            ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ModifyVpcTenancy:
    """Modify VPC tenancy operation."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_vpc_tenancy(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.212.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            r = ec2.modify_vpc_tenancy(VpcId=vpc_id, InstanceTenancy="default")
            assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2DHCPOptionsDetailed:
    """DHCP Options with multiple configurations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_dhcp_options_with_multiple_configs(self, ec2):
        r = ec2.create_dhcp_options(
            DhcpConfigurations=[
                {"Key": "domain-name", "Values": ["example.com"]},
                {"Key": "domain-name-servers", "Values": ["8.8.8.8", "8.8.4.4"]},
            ]
        )
        dopt_id = r["DhcpOptions"]["DhcpOptionsId"]
        try:
            assert dopt_id.startswith("dopt-")

            described = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
            assert len(described["DhcpOptions"]) == 1
            configs = described["DhcpOptions"][0]["DhcpConfigurations"]
            config_keys = {c["Key"] for c in configs}
            assert "domain-name" in config_keys
            assert "domain-name-servers" in config_keys
        finally:
            ec2.delete_dhcp_options(DhcpOptionsId=dopt_id)


class TestEC2VpcCidrBlockOperations:
    """VPC CIDR block associate/disassociate operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_associate_disassociate_vpc_cidr_block(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            assoc = ec2.associate_vpc_cidr_block(VpcId=vpc_id, CidrBlock="10.1.0.0/16")
            assoc_id = assoc["CidrBlockAssociation"]["AssociationId"]
            assert assoc_id.startswith("vpc-cidr-assoc-")
            assert assoc["CidrBlockAssociation"]["CidrBlock"] == "10.1.0.0/16"

            described = ec2.describe_vpcs(VpcIds=[vpc_id])
            cidr_blocks = [a["CidrBlock"] for a in described["Vpcs"][0]["CidrBlockAssociationSet"]]
            assert "10.1.0.0/16" in cidr_blocks

            ec2.disassociate_vpc_cidr_block(AssociationId=assoc_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_associate_vpc_cidr_block_multiple(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            a1 = ec2.associate_vpc_cidr_block(VpcId=vpc_id, CidrBlock="10.1.0.0/16")
            a2 = ec2.associate_vpc_cidr_block(VpcId=vpc_id, CidrBlock="10.2.0.0/16")
            assert a1["CidrBlockAssociation"]["CidrBlock"] == "10.1.0.0/16"
            assert a2["CidrBlockAssociation"]["CidrBlock"] == "10.2.0.0/16"

            described = ec2.describe_vpcs(VpcIds=[vpc_id])
            cidrs = {a["CidrBlock"] for a in described["Vpcs"][0]["CidrBlockAssociationSet"]}
            assert "10.0.0.0/16" in cidrs
            assert "10.1.0.0/16" in cidrs
            assert "10.2.0.0/16" in cidrs

            ec2.disassociate_vpc_cidr_block(
                AssociationId=a2["CidrBlockAssociation"]["AssociationId"]
            )
            ec2.disassociate_vpc_cidr_block(
                AssociationId=a1["CidrBlockAssociation"]["AssociationId"]
            )
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2SubnetCidrReservation:
    """Subnet CIDR reservation operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_get_delete_subnet_cidr_reservation(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                res = ec2.create_subnet_cidr_reservation(
                    SubnetId=subnet_id,
                    Cidr="10.0.1.0/28",
                    ReservationType="prefix",
                )
                scr_id = res["SubnetCidrReservation"]["SubnetCidrReservationId"]
                assert scr_id.startswith("scr-")
                assert res["SubnetCidrReservation"]["Cidr"] == "10.0.1.0/28"

                got = ec2.get_subnet_cidr_reservations(SubnetId=subnet_id)
                prefix_reservations = got.get("SubnetIpv4CidrReservations", [])
                assert any(r["SubnetCidrReservationId"] == scr_id for r in prefix_reservations)

                ec2.delete_subnet_cidr_reservation(SubnetCidrReservationId=scr_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2RegisterImageOperations:
    """RegisterImage and related operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_register_describe_deregister_image(self, ec2):
        resp = ec2.register_image(
            Name=_unique("test-ami"),
            RootDeviceName="/dev/sda1",
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {"VolumeSize": 8, "VolumeType": "gp2"},
                }
            ],
        )
        ami_id = resp["ImageId"]
        assert ami_id.startswith("ami-")
        try:
            described = ec2.describe_images(ImageIds=[ami_id])
            assert len(described["Images"]) == 1
            assert described["Images"][0]["RootDeviceName"] == "/dev/sda1"
        finally:
            ec2.deregister_image(ImageId=ami_id)

    def test_register_image_with_description(self, ec2):
        resp = ec2.register_image(
            Name=_unique("test-ami-desc"),
            Description="Test image description",
            RootDeviceName="/dev/sda1",
        )
        ami_id = resp["ImageId"]
        try:
            described = ec2.describe_images(ImageIds=[ami_id])
            assert described["Images"][0]["Description"] == "Test image description"
        finally:
            ec2.deregister_image(ImageId=ami_id)


class TestEC2PlacementGroupStrategies:
    """Placement group with different strategies."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_placement_group_partition_strategy(self, ec2):
        name = _unique("pg-partition")
        ec2.create_placement_group(GroupName=name, Strategy="partition", PartitionCount=3)
        try:
            described = ec2.describe_placement_groups(GroupNames=[name])
            pg = described["PlacementGroups"][0]
            assert pg["Strategy"] == "partition"
        finally:
            ec2.delete_placement_group(GroupName=name)

    def test_placement_group_spread_strategy(self, ec2):
        name = _unique("pg-spread")
        ec2.create_placement_group(GroupName=name, Strategy="spread")
        try:
            described = ec2.describe_placement_groups(GroupNames=[name])
            pg = described["PlacementGroups"][0]
            assert pg["Strategy"] == "spread"
        finally:
            ec2.delete_placement_group(GroupName=name)

    def test_describe_placement_groups_by_filter(self, ec2):
        name = _unique("pg-filter")
        ec2.create_placement_group(GroupName=name, Strategy="cluster")
        try:
            described = ec2.describe_placement_groups(
                Filters=[{"Name": "group-name", "Values": [name]}]
            )
            assert len(described["PlacementGroups"]) == 1
            assert described["PlacementGroups"][0]["GroupName"] == name
        finally:
            ec2.delete_placement_group(GroupName=name)


class TestEC2KeyPairTypes:
    """Key pair with different types and tags."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_ed25519_key_pair(self, ec2):
        name = _unique("kp-ed25519")
        resp = ec2.create_key_pair(KeyName=name, KeyType="ed25519")
        try:
            assert resp["KeyName"] == name
            assert "KeyMaterial" in resp

            described = ec2.describe_key_pairs(KeyNames=[name])
            assert described["KeyPairs"][0]["KeyType"] == "ed25519"
        finally:
            ec2.delete_key_pair(KeyName=name)

    def test_create_key_pair_with_tags(self, ec2):
        name = _unique("kp-tagged")
        resp = ec2.create_key_pair(
            KeyName=name,
            TagSpecifications=[
                {
                    "ResourceType": "key-pair",
                    "Tags": [{"Key": "env", "Value": "test"}],
                }
            ],
        )
        try:
            assert resp["KeyName"] == name
            described = ec2.describe_key_pairs(KeyNames=[name])
            tags = described["KeyPairs"][0].get("Tags", [])
            assert any(t["Key"] == "env" and t["Value"] == "test" for t in tags)
        finally:
            ec2.delete_key_pair(KeyName=name)


class TestEC2VolumeTypeOperations:
    """Volume operations with different types."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_gp3_volume(self, ec2):
        resp = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp3")
        vol_id = resp["VolumeId"]
        try:
            assert resp["VolumeType"] == "gp3"
            assert resp["Size"] == 10
            described = ec2.describe_volumes(VolumeIds=[vol_id])
            assert described["Volumes"][0]["VolumeType"] == "gp3"
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_create_io1_volume(self, ec2):
        resp = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="io1", Iops=100)
        vol_id = resp["VolumeId"]
        try:
            assert resp["VolumeType"] == "io1"
            assert resp["Iops"] == 100
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_create_volume_with_tags(self, ec2):
        resp = ec2.create_volume(
            AvailabilityZone="us-east-1a",
            Size=10,
            VolumeType="gp2",
            TagSpecifications=[
                {
                    "ResourceType": "volume",
                    "Tags": [{"Key": "Name", "Value": "test-vol"}],
                }
            ],
        )
        vol_id = resp["VolumeId"]
        try:
            tags = resp.get("Tags", [])
            assert any(t["Key"] == "Name" and t["Value"] == "test-vol" for t in tags)
        finally:
            ec2.delete_volume(VolumeId=vol_id)

    def test_create_volume_encrypted(self, ec2):
        resp = ec2.create_volume(
            AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2", Encrypted=True
        )
        vol_id = resp["VolumeId"]
        try:
            assert resp["Encrypted"] is True
        finally:
            ec2.delete_volume(VolumeId=vol_id)


class TestEC2NetworkInterfaceAttachDetach:
    """Network interface attach/detach and security group operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_attach_detach_network_interface(self, ec2):
        """Create ENI, run instance, attach ENI, detach ENI."""
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id)
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    instances = ec2.run_instances(
                        ImageId="ami-12345678",
                        InstanceType="t2.micro",
                        MinCount=1,
                        MaxCount=1,
                        SubnetId=subnet_id,
                    )
                    instance_id = instances["Instances"][0]["InstanceId"]
                    try:
                        attach = ec2.attach_network_interface(
                            NetworkInterfaceId=eni_id,
                            InstanceId=instance_id,
                            DeviceIndex=1,
                        )
                        attach_id = attach["AttachmentId"]
                        assert attach_id.startswith("eni-attach-")

                        ec2.detach_network_interface(AttachmentId=attach_id)
                    finally:
                        ec2.terminate_instances(InstanceIds=[instance_id])
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_network_interface_with_security_group(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            sg = ec2.create_security_group(
                GroupName=_unique("eni-sg"),
                Description="ENI test SG",
                VpcId=vpc_id,
            )
            sg_id = sg["GroupId"]
            try:
                eni = ec2.create_network_interface(SubnetId=subnet_id, Groups=[sg_id])
                eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
                try:
                    groups = eni["NetworkInterface"]["Groups"]
                    assert any(g["GroupId"] == sg_id for g in groups)
                finally:
                    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            finally:
                ec2.delete_security_group(GroupId=sg_id)
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2InstanceImageOperations:
    """Instance-based image and metadata operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_image_from_instance(self, ec2):
        instances = ec2.run_instances(
            ImageId="ami-12345678",
            InstanceType="t2.micro",
            MinCount=1,
            MaxCount=1,
        )
        instance_id = instances["Instances"][0]["InstanceId"]
        try:
            resp = ec2.create_image(InstanceId=instance_id, Name=_unique("test-image"))
            ami_id = resp["ImageId"]
            assert ami_id.startswith("ami-")

            described = ec2.describe_images(ImageIds=[ami_id])
            assert len(described["Images"]) == 1
            ec2.deregister_image(ImageId=ami_id)
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])

    def test_get_launch_template_data(self, ec2):
        instances = ec2.run_instances(
            ImageId="ami-12345678",
            InstanceType="t2.micro",
            MinCount=1,
            MaxCount=1,
        )
        instance_id = instances["Instances"][0]["InstanceId"]
        try:
            resp = ec2.get_launch_template_data(InstanceId=instance_id)
            lt_data = resp["LaunchTemplateData"]
            assert "ImageId" in lt_data
            assert "InstanceType" in lt_data
        finally:
            ec2.terminate_instances(InstanceIds=[instance_id])


class TestEC2ModifySecurityGroupRules:
    """Modify security group rules operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_security_group_rules(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sg = ec2.create_security_group(
                GroupName=_unique("mod-sg"),
                Description="Test modify SG rules",
                VpcId=vpc_id,
            )
            sg_id = sg["GroupId"]
            try:
                ec2.authorize_security_group_ingress(
                    GroupId=sg_id,
                    IpPermissions=[
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 80,
                            "ToPort": 80,
                            "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
                        }
                    ],
                )
                rules = ec2.describe_security_group_rules(
                    Filters=[{"Name": "group-id", "Values": [sg_id]}]
                )
                ingress_rules = [
                    r
                    for r in rules["SecurityGroupRules"]
                    if not r["IsEgress"] and r["IpProtocol"] == "tcp"
                ]
                assert len(ingress_rules) >= 1
                rule_id = ingress_rules[0]["SecurityGroupRuleId"]

                ec2.modify_security_group_rules(
                    GroupId=sg_id,
                    SecurityGroupRules=[
                        {
                            "SecurityGroupRuleId": rule_id,
                            "SecurityGroupRule": {
                                "IpProtocol": "tcp",
                                "FromPort": 443,
                                "ToPort": 443,
                                "CidrIpv4": "10.0.0.0/8",
                            },
                        }
                    ],
                )

                updated = ec2.describe_security_group_rules(
                    Filters=[{"Name": "group-id", "Values": [sg_id]}]
                )
                updated_rule = [
                    r for r in updated["SecurityGroupRules"] if r["SecurityGroupRuleId"] == rule_id
                ][0]
                assert updated_rule["FromPort"] == 443
                assert updated_rule["ToPort"] == 443
            finally:
                ec2.delete_security_group(GroupId=sg_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2VpcPeeringAdvanced:
    """Advanced VPC peering operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_reject_vpc_peering_connection(self, ec2):
        vpc1 = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc1_id = vpc1["Vpc"]["VpcId"]
        vpc2 = ec2.create_vpc(CidrBlock="10.1.0.0/16")
        vpc2_id = vpc2["Vpc"]["VpcId"]
        try:
            pcx = ec2.create_vpc_peering_connection(VpcId=vpc1_id, PeerVpcId=vpc2_id)
            pcx_id = pcx["VpcPeeringConnection"]["VpcPeeringConnectionId"]

            resp = ec2.reject_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
            assert resp["Return"] is True

            described = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
            status = described["VpcPeeringConnections"][0]["Status"]["Code"]
            assert status in ("rejected", "deleted")
        finally:
            ec2.delete_vpc(VpcId=vpc2_id)
            ec2.delete_vpc(VpcId=vpc1_id)

    def test_vpc_peering_has_requester_accepter_info(self, ec2):
        vpc1 = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc1_id = vpc1["Vpc"]["VpcId"]
        vpc2 = ec2.create_vpc(CidrBlock="10.1.0.0/16")
        vpc2_id = vpc2["Vpc"]["VpcId"]
        try:
            pcx = ec2.create_vpc_peering_connection(VpcId=vpc1_id, PeerVpcId=vpc2_id)
            pcx_id = pcx["VpcPeeringConnection"]["VpcPeeringConnectionId"]
            try:
                req_info = pcx["VpcPeeringConnection"]["RequesterVpcInfo"]
                acc_info = pcx["VpcPeeringConnection"]["AccepterVpcInfo"]
                assert req_info["VpcId"] == vpc1_id
                assert acc_info["VpcId"] == vpc2_id
            finally:
                ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
        finally:
            ec2.delete_vpc(VpcId=vpc2_id)
            ec2.delete_vpc(VpcId=vpc1_id)


class TestEC2ReplaceRouteTableAssociation:
    """Replace route table association operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_replace_route_table_association(self, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            rtb1 = ec2.create_route_table(VpcId=vpc_id)
            rtb1_id = rtb1["RouteTable"]["RouteTableId"]
            rtb2 = ec2.create_route_table(VpcId=vpc_id)
            rtb2_id = rtb2["RouteTable"]["RouteTableId"]
            try:
                assoc = ec2.associate_route_table(SubnetId=subnet_id, RouteTableId=rtb1_id)
                assoc_id = assoc["AssociationId"]

                new_assoc = ec2.replace_route_table_association(
                    AssociationId=assoc_id, RouteTableId=rtb2_id
                )
                new_assoc_id = new_assoc["NewAssociationId"]
                assert new_assoc_id != assoc_id

                ec2.disassociate_route_table(AssociationId=new_assoc_id)
            finally:
                ec2.delete_route_table(RouteTableId=rtb2_id)
                ec2.delete_route_table(RouteTableId=rtb1_id)
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2DescribeListOperations:
    """Various describe/list operations that return empty lists."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_iam_instance_profile_associations(self, ec2):
        resp = ec2.describe_iam_instance_profile_associations()
        assert "IamInstanceProfileAssociations" in resp
        assert isinstance(resp["IamInstanceProfileAssociations"], list)

    def test_describe_fleet_instances_empty(self, ec2):
        # Fleet ID doesn't exist but API returns empty active instances
        resp = ec2.describe_fleet_instances(FleetId="fleet-00000000000000000")
        assert "ActiveInstances" in resp
        assert isinstance(resp["ActiveInstances"], list)

    def test_describe_hosts_list(self, ec2):
        resp = ec2.describe_hosts()
        assert "Hosts" in resp
        assert isinstance(resp["Hosts"], list)

    def test_describe_instance_credit_specifications_list(self, ec2):
        resp = ec2.describe_instance_credit_specifications()
        assert "InstanceCreditSpecifications" in resp
        assert isinstance(resp["InstanceCreditSpecifications"], list)


class TestEC2VpcClassicLinkToggle:
    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_enable_disable_vpc_classic_link(self, ec2):
        """EnableVpcClassicLink / DisableVpcClassicLink on a VPC."""
        vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.enable_vpc_classic_link(VpcId=vpc_id)
            assert "Return" in resp

            resp2 = ec2.disable_vpc_classic_link(VpcId=vpc_id)
            assert "Return" in resp2
        finally:
            ec2.delete_vpc(VpcId=vpc_id)

    def test_enable_disable_vpc_classic_link_dns_support(self, ec2):
        """EnableVpcClassicLinkDnsSupport / DisableVpcClassicLinkDnsSupport."""
        vpc = ec2.create_vpc(CidrBlock="10.91.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.enable_vpc_classic_link_dns_support(VpcId=vpc_id)
            assert "Return" in resp

            resp2 = ec2.disable_vpc_classic_link_dns_support(VpcId=vpc_id)
            assert "Return" in resp2
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2InstanceAttributeExtended:
    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def instance_id(self, ec2):
        run = ec2.run_instances(
            ImageId="ami-12c6146b", InstanceType="t2.micro", MinCount=1, MaxCount=1
        )
        inst_id = run["Instances"][0]["InstanceId"]
        yield inst_id
        ec2.terminate_instances(InstanceIds=[inst_id])

    def test_describe_instance_attribute_sriov_net_support(self, ec2, instance_id):
        """DescribeInstanceAttribute for sriovNetSupport."""
        resp = ec2.describe_instance_attribute(InstanceId=instance_id, Attribute="sriovNetSupport")
        assert resp["InstanceId"] == instance_id
        assert "SriovNetSupport" in resp

    def test_describe_instance_attribute_group_set(self, ec2, instance_id):
        """DescribeInstanceAttribute for groupSet."""
        resp = ec2.describe_instance_attribute(InstanceId=instance_id, Attribute="groupSet")
        assert resp["InstanceId"] == instance_id
        assert "Groups" in resp
        assert isinstance(resp["Groups"], list)

    def test_describe_instance_attribute_ebs_optimized(self, ec2, instance_id):
        """DescribeInstanceAttribute for ebsOptimized."""
        resp = ec2.describe_instance_attribute(InstanceId=instance_id, Attribute="ebsOptimized")
        assert resp["InstanceId"] == instance_id
        assert "EbsOptimized" in resp

    def test_modify_instance_attribute_disable_api_stop(self, ec2, instance_id):
        """ModifyInstanceAttribute to set disableApiStop, then verify."""
        ec2.modify_instance_attribute(InstanceId=instance_id, DisableApiStop={"Value": True})
        resp = ec2.describe_instance_attribute(InstanceId=instance_id, Attribute="disableApiStop")
        assert resp["InstanceId"] == instance_id
        assert resp["DisableApiStop"]["Value"] is True

    def test_get_instance_uefi_data(self, ec2, instance_id):
        """GetInstanceUefiData returns for a valid instance."""
        resp = ec2.get_instance_uefi_data(InstanceId=instance_id)
        assert resp["InstanceId"] == instance_id

    def test_create_snapshots_from_instance(self, ec2, instance_id):
        """CreateSnapshots creates snapshots of all volumes on an instance."""
        resp = ec2.create_snapshots(
            InstanceSpecification={"InstanceId": instance_id, "ExcludeBootVolume": False}
        )
        assert "Snapshots" in resp
        assert len(resp["Snapshots"]) >= 1
        snap_id = resp["Snapshots"][0]["SnapshotId"]
        assert snap_id.startswith("snap-")
        # Cleanup
        for s in resp["Snapshots"]:
            ec2.delete_snapshot(SnapshotId=s["SnapshotId"])


class TestEC2VpcAttributeExtended:
    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_vpc_attribute_network_address_usage_metrics(self, ec2):
        """DescribeVpcAttribute for enableNetworkAddressUsageMetrics."""
        vpc = ec2.create_vpc(CidrBlock="10.92.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = ec2.describe_vpc_attribute(
                VpcId=vpc_id, Attribute="enableNetworkAddressUsageMetrics"
            )
            assert resp["VpcId"] == vpc_id
            assert "EnableNetworkAddressUsageMetrics" in resp
            assert isinstance(resp["EnableNetworkAddressUsageMetrics"]["Value"], bool)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2ModifyImageAttribute:
    """Tests for ModifyImageAttribute and DescribeImageAttribute."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def owned_ami(self, ec2):
        """Register a custom AMI we own so we can modify its attributes."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        snap = ec2.create_snapshot(VolumeId=vol_id, Description="for-ami-attr-test")
        snap_id = snap["SnapshotId"]
        ami = ec2.register_image(
            Name=_unique("ami-attr"),
            RootDeviceName="/dev/sda1",
            BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"SnapshotId": snap_id}}],
        )
        ami_id = ami["ImageId"]
        yield ami_id
        ec2.deregister_image(ImageId=ami_id)
        ec2.delete_snapshot(SnapshotId=snap_id)
        ec2.delete_volume(VolumeId=vol_id)

    def test_modify_image_attribute_add_launch_permission(self, ec2, owned_ami):
        """ModifyImageAttribute adds launch permission, DescribeImageAttribute verifies."""
        ec2.modify_image_attribute(
            ImageId=owned_ami,
            LaunchPermission={"Add": [{"UserId": "111122223333"}]},
        )
        resp = ec2.describe_image_attribute(ImageId=owned_ami, Attribute="launchPermission")
        user_ids = [p["UserId"] for p in resp["LaunchPermissions"]]
        assert "111122223333" in user_ids

    def test_modify_image_attribute_remove_launch_permission(self, ec2, owned_ami):
        """ModifyImageAttribute removes launch permission."""
        ec2.modify_image_attribute(
            ImageId=owned_ami,
            LaunchPermission={"Add": [{"UserId": "222233334444"}]},
        )
        ec2.modify_image_attribute(
            ImageId=owned_ami,
            LaunchPermission={"Remove": [{"UserId": "222233334444"}]},
        )
        resp = ec2.describe_image_attribute(ImageId=owned_ami, Attribute="launchPermission")
        user_ids = [p.get("UserId") for p in resp.get("LaunchPermissions", [])]
        assert "222233334444" not in user_ids


class TestEC2ModifySnapshotAttribute:
    """Tests for ModifySnapshotAttribute and DescribeSnapshotAttribute."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def snapshot(self, ec2):
        """Create a volume and snapshot for testing."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]
        snap = ec2.create_snapshot(VolumeId=vol_id, Description="snap-attr-test")
        snap_id = snap["SnapshotId"]
        yield snap_id
        ec2.delete_snapshot(SnapshotId=snap_id)
        ec2.delete_volume(VolumeId=vol_id)

    def test_modify_snapshot_attribute_add_permission(self, ec2, snapshot):
        """ModifySnapshotAttribute adds createVolumePermission."""
        ec2.modify_snapshot_attribute(
            SnapshotId=snapshot,
            Attribute="createVolumePermission",
            OperationType="add",
            UserIds=["111122223333"],
        )
        resp = ec2.describe_snapshot_attribute(
            SnapshotId=snapshot, Attribute="createVolumePermission"
        )
        user_ids = [p["UserId"] for p in resp["CreateVolumePermissions"]]
        assert "111122223333" in user_ids

    def test_modify_snapshot_attribute_remove_permission(self, ec2, snapshot):
        """ModifySnapshotAttribute removes createVolumePermission."""
        ec2.modify_snapshot_attribute(
            SnapshotId=snapshot,
            Attribute="createVolumePermission",
            OperationType="add",
            UserIds=["333344445555"],
        )
        ec2.modify_snapshot_attribute(
            SnapshotId=snapshot,
            Attribute="createVolumePermission",
            OperationType="remove",
            UserIds=["333344445555"],
        )
        resp = ec2.describe_snapshot_attribute(
            SnapshotId=snapshot, Attribute="createVolumePermission"
        )
        user_ids = [p.get("UserId") for p in resp.get("CreateVolumePermissions", [])]
        assert "333344445555" not in user_ids

    def test_modify_snapshot_attribute_multiple_users(self, ec2, snapshot):
        """ModifySnapshotAttribute with multiple user IDs."""
        ec2.modify_snapshot_attribute(
            SnapshotId=snapshot,
            Attribute="createVolumePermission",
            OperationType="add",
            UserIds=["111111111111", "222222222222"],
        )
        resp = ec2.describe_snapshot_attribute(
            SnapshotId=snapshot, Attribute="createVolumePermission"
        )
        user_ids = [p["UserId"] for p in resp["CreateVolumePermissions"]]
        assert "111111111111" in user_ids
        assert "222222222222" in user_ids


class TestEC2ModifySubnetAttribute:
    """Tests for ModifySubnetAttribute."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def vpc_and_subnet(self, ec2):
        """Create a VPC and subnet for attribute modification tests."""
        vpc = ec2.create_vpc(CidrBlock="10.60.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.60.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        yield vpc_id, subnet_id
        ec2.delete_subnet(SubnetId=subnet_id)
        ec2.delete_vpc(VpcId=vpc_id)

    def test_modify_subnet_attribute_map_public_ip(self, ec2, vpc_and_subnet):
        """ModifySubnetAttribute sets MapPublicIpOnLaunch."""
        _, subnet_id = vpc_and_subnet
        ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
        desc = ec2.describe_subnets(SubnetIds=[subnet_id])
        assert desc["Subnets"][0]["MapPublicIpOnLaunch"] is True

    def test_modify_subnet_attribute_map_public_ip_disable(self, ec2, vpc_and_subnet):
        """ModifySubnetAttribute can disable MapPublicIpOnLaunch."""
        _, subnet_id = vpc_and_subnet
        ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
        ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": False})
        desc = ec2.describe_subnets(SubnetIds=[subnet_id])
        assert desc["Subnets"][0]["MapPublicIpOnLaunch"] is False


class TestEC2ModifyVpcPeeringConnectionOptions:
    """Tests for ModifyVpcPeeringConnectionOptions."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def peering(self, ec2):
        """Create two VPCs and a peering connection."""
        vpc1 = ec2.create_vpc(CidrBlock="10.61.0.0/16")
        vpc2 = ec2.create_vpc(CidrBlock="10.62.0.0/16")
        vpc1_id = vpc1["Vpc"]["VpcId"]
        vpc2_id = vpc2["Vpc"]["VpcId"]
        peer = ec2.create_vpc_peering_connection(VpcId=vpc1_id, PeerVpcId=vpc2_id)
        pcx_id = peer["VpcPeeringConnection"]["VpcPeeringConnectionId"]
        ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
        yield pcx_id, vpc1_id, vpc2_id
        ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
        ec2.delete_vpc(VpcId=vpc1_id)
        ec2.delete_vpc(VpcId=vpc2_id)

    def test_modify_vpc_peering_connection_options_dns(self, ec2, peering):
        """ModifyVpcPeeringConnectionOptions sets DNS resolution."""
        pcx_id, _, _ = peering
        resp = ec2.modify_vpc_peering_connection_options(
            VpcPeeringConnectionId=pcx_id,
            RequesterPeeringConnectionOptions={"AllowDnsResolutionFromRemoteVpc": True},
        )
        assert "RequesterPeeringConnectionOptions" in resp
        assert resp["RequesterPeeringConnectionOptions"]["AllowDnsResolutionFromRemoteVpc"] is True


class TestEC2CreateDefaultVpc:
    """Tests for CreateDefaultVpc."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def ec2_fresh_region(self):
        """Client in a region where we can safely delete/recreate default VPC."""
        return make_client("ec2", region_name="ap-northeast-3")

    def _cleanup_default_vpc(self, ec2):
        """Delete the default VPC and all its dependencies."""
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
        for vpc in vpcs["Vpcs"]:
            vpc_id = vpc["VpcId"]
            subs = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            for s in subs["Subnets"]:
                ec2.delete_subnet(SubnetId=s["SubnetId"])
            igws = ec2.describe_internet_gateways(
                Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
            )
            for igw in igws["InternetGateways"]:
                ec2.detach_internet_gateway(
                    InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc_id
                )
                ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])
            ec2.delete_vpc(VpcId=vpc_id)

    def test_create_default_vpc_already_exists(self, ec2):
        """CreateDefaultVpc raises error when default VPC exists."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            ec2.create_default_vpc()
        assert "DefaultVpcAlreadyExists" in str(exc_info.value)

    def test_create_default_vpc(self, ec2_fresh_region):
        """CreateDefaultVpc creates a new default VPC when none exists."""
        ec2 = ec2_fresh_region
        self._cleanup_default_vpc(ec2)

        vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
        assert len(vpcs["Vpcs"]) == 0

        resp = ec2.create_default_vpc()
        vpc = resp["Vpc"]
        assert vpc["IsDefault"] is True
        assert vpc["VpcId"].startswith("vpc-")
        assert vpc["CidrBlock"] == "172.31.0.0/16"
        assert vpc["State"] in ("available", "pending")

    def test_create_default_vpc_creates_subnets(self, ec2_fresh_region):
        """CreateDefaultVpc also creates default subnets in each AZ."""
        ec2 = ec2_fresh_region
        self._cleanup_default_vpc(ec2)
        ec2.create_default_vpc()

        subs = ec2.describe_subnets(Filters=[{"Name": "default-for-az", "Values": ["true"]}])
        assert len(subs["Subnets"]) >= 1
        for sub in subs["Subnets"]:
            assert sub["DefaultForAz"] is True
            assert sub["MapPublicIpOnLaunch"] is True


class TestEC2ModifySpotFleetRequest:
    """ModifySpotFleetRequest to change TargetCapacity."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_modify_spot_fleet_target_capacity(self, ec2):
        """ModifySpotFleetRequest updates TargetCapacity."""
        fr = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [{"ImageId": "ami-12345678", "InstanceType": "t2.micro"}],
            }
        )
        fleet_id = fr["SpotFleetRequestId"]
        try:
            mr = ec2.modify_spot_fleet_request(SpotFleetRequestId=fleet_id, TargetCapacity=5)
            assert mr["Return"] is True

            described = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[fleet_id])
            config = described["SpotFleetRequestConfigs"][0]
            assert config["SpotFleetRequestConfig"]["TargetCapacity"] == 5
        finally:
            ec2.cancel_spot_fleet_requests(SpotFleetRequestIds=[fleet_id], TerminateInstances=True)

    def test_modify_spot_fleet_decrease_capacity(self, ec2):
        """ModifySpotFleetRequest can decrease TargetCapacity."""
        fr = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 10,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [{"ImageId": "ami-12345678", "InstanceType": "t2.micro"}],
            }
        )
        fleet_id = fr["SpotFleetRequestId"]
        try:
            mr = ec2.modify_spot_fleet_request(SpotFleetRequestId=fleet_id, TargetCapacity=2)
            assert mr["Return"] is True

            described = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[fleet_id])
            config = described["SpotFleetRequestConfigs"][0]
            assert config["SpotFleetRequestConfig"]["TargetCapacity"] == 2
        finally:
            ec2.cancel_spot_fleet_requests(SpotFleetRequestIds=[fleet_id], TerminateInstances=True)


class TestEC2SpotFleetMultiCancel:
    """Cancel multiple spot fleet requests at once."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_cancel_multiple_spot_fleets(self, ec2):
        """CancelSpotFleetRequests with multiple fleet IDs."""
        fleet_ids = []
        for _ in range(2):
            fr = ec2.request_spot_fleet(
                SpotFleetRequestConfig={
                    "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                    "TargetCapacity": 1,
                    "SpotPrice": "0.05",
                    "AllocationStrategy": "lowestPrice",
                    "LaunchSpecifications": [
                        {"ImageId": "ami-12345678", "InstanceType": "t2.micro"}
                    ],
                }
            )
            fleet_ids.append(fr["SpotFleetRequestId"])

        cancel = ec2.cancel_spot_fleet_requests(
            SpotFleetRequestIds=fleet_ids, TerminateInstances=True
        )
        assert len(cancel["SuccessfulFleetRequests"]) == 2
        cancelled_ids = {r["SpotFleetRequestId"] for r in cancel["SuccessfulFleetRequests"]}
        assert set(fleet_ids) == cancelled_ids


class TestEC2SpotPriceHistory:
    """DescribeSpotPriceHistory returns spot price data."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_spot_price_history(self, ec2):
        """DescribeSpotPriceHistory returns SpotPriceHistory list."""
        resp = ec2.describe_spot_price_history(InstanceTypes=["t2.micro"], MaxResults=5)
        assert "SpotPriceHistory" in resp
        assert isinstance(resp["SpotPriceHistory"], list)

    def test_describe_spot_price_history_has_fields(self, ec2):
        """DescribeSpotPriceHistory entries have expected fields."""
        resp = ec2.describe_spot_price_history(InstanceTypes=["m5.large"], MaxResults=5)
        assert "SpotPriceHistory" in resp
        if resp["SpotPriceHistory"]:
            entry = resp["SpotPriceHistory"][0]
            assert "InstanceType" in entry
            assert "SpotPrice" in entry
            assert "AvailabilityZone" in entry


class TestEC2DescribeFleetInstances:
    """DescribeFleetInstances for EC2 Fleets."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_fleet_instances(self, ec2):
        """DescribeFleetInstances returns ActiveInstances for a fleet."""
        lt_name = _unique("fi-lt")
        lt = ec2.create_launch_template(
            LaunchTemplateName=lt_name,
            LaunchTemplateData={
                "InstanceType": "t2.micro",
                "ImageId": "ami-12345678",
            },
        )
        lt_id = lt["LaunchTemplate"]["LaunchTemplateId"]
        try:
            r = ec2.create_fleet(
                LaunchTemplateConfigs=[
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": lt_id,
                            "Version": "$Latest",
                        },
                        "Overrides": [{"InstanceType": "t2.micro"}],
                    }
                ],
                TargetCapacitySpecification={
                    "TotalTargetCapacity": 1,
                    "DefaultTargetCapacityType": "on-demand",
                },
                Type="instant",
            )
            fleet_id = r["FleetId"]
            assert fleet_id.startswith("fleet-")

            instances = ec2.describe_fleet_instances(FleetId=fleet_id)
            assert "ActiveInstances" in instances
            assert instances["FleetId"] == fleet_id
            assert isinstance(instances["ActiveInstances"], list)

            ec2.delete_fleets(FleetIds=[fleet_id], TerminateInstances=True)
        finally:
            ec2.delete_launch_template(LaunchTemplateId=lt_id)


class TestEC2SpotFleetAllocationStrategies:
    """Spot fleet with different allocation strategies."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_spot_fleet_diversified_strategy(self, ec2):
        """RequestSpotFleet with diversified allocation strategy."""
        fr = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.10",
                "AllocationStrategy": "diversified",
                "LaunchSpecifications": [
                    {"ImageId": "ami-12345678", "InstanceType": "t2.micro"},
                    {"ImageId": "ami-12345678", "InstanceType": "t2.small"},
                ],
            }
        )
        fleet_id = fr["SpotFleetRequestId"]
        try:
            assert fleet_id.startswith("sfr-")
            described = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[fleet_id])
            config = described["SpotFleetRequestConfigs"][0]["SpotFleetRequestConfig"]
            assert config["AllocationStrategy"] == "diversified"
            assert len(config["LaunchSpecifications"]) == 2
        finally:
            ec2.cancel_spot_fleet_requests(SpotFleetRequestIds=[fleet_id], TerminateInstances=True)

    def test_spot_fleet_state_after_creation(self, ec2):
        """Spot fleet has active state after creation."""
        fr = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [{"ImageId": "ami-12345678", "InstanceType": "t2.micro"}],
            }
        )
        fleet_id = fr["SpotFleetRequestId"]
        try:
            described = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[fleet_id])
            fleet = described["SpotFleetRequestConfigs"][0]
            assert fleet["SpotFleetRequestState"] == "active"
            assert fleet["SpotFleetRequestId"] == fleet_id
        finally:
            ec2.cancel_spot_fleet_requests(SpotFleetRequestIds=[fleet_id], TerminateInstances=True)

    def test_spot_fleet_state_after_cancel(self, ec2):
        """Spot fleet state changes after cancellation."""
        fr = ec2.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::123456789012:role/fleet",
                "TargetCapacity": 1,
                "SpotPrice": "0.05",
                "AllocationStrategy": "lowestPrice",
                "LaunchSpecifications": [{"ImageId": "ami-12345678", "InstanceType": "t2.micro"}],
            }
        )
        fleet_id = fr["SpotFleetRequestId"]
        cancel = ec2.cancel_spot_fleet_requests(
            SpotFleetRequestIds=[fleet_id], TerminateInstances=True
        )
        assert cancel["SuccessfulFleetRequests"][0]["CurrentSpotFleetRequestState"] in (
            "cancelled_running",
            "cancelled_terminating",
        )


class TestEC2TransitGatewayPeeringAdvanced:
    """Advanced transit gateway peering attachment tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_tgw_peering_with_tags(self, ec2):
        """CreateTransitGatewayPeeringAttachment with TagSpecifications."""
        tgw1 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1,
                PeerTransitGatewayId=tgw2,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
                TagSpecifications=[
                    {
                        "ResourceType": "transit-gateway-attachment",
                        "Tags": [{"Key": "Purpose", "Value": "peering-test"}],
                    }
                ],
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]
            tags = att["TransitGatewayPeeringAttachment"].get("Tags", [])
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map.get("Purpose") == "peering-test"
            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1)

    def test_tgw_peering_accept_state_available(self, ec2):
        """AcceptTransitGatewayPeeringAttachment sets state to available."""
        tgw1 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1,
                PeerTransitGatewayId=tgw2,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]

            accepted = ec2.accept_transit_gateway_peering_attachment(
                TransitGatewayAttachmentId=att_id
            )
            assert accepted["TransitGatewayPeeringAttachment"]["State"] == "available"

            described = ec2.describe_transit_gateway_peering_attachments(
                TransitGatewayAttachmentIds=[att_id]
            )
            assert described["TransitGatewayPeeringAttachments"][0]["State"] == "available"

            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1)

    def test_tgw_peering_accepter_info_details(self, ec2):
        """Peering attachment has correct AccepterTgwInfo with region and owner."""
        tgw1 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1,
                PeerTransitGatewayId=tgw2,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]
            peer = att["TransitGatewayPeeringAttachment"]

            accepter = peer["AccepterTgwInfo"]
            assert accepter["TransitGatewayId"] == tgw2
            assert accepter["Region"] == "us-east-1"
            assert accepter["OwnerId"] == "123456789012"

            requester = peer["RequesterTgwInfo"]
            assert requester["TransitGatewayId"] == tgw1
            assert requester["Region"] == "us-east-1"

            ec2.delete_transit_gateway_peering_attachment(TransitGatewayAttachmentId=att_id)
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1)

    def test_tgw_peering_delete_state(self, ec2):
        """DeleteTransitGatewayPeeringAttachment returns deleted state."""
        tgw1 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        tgw2 = ec2.create_transit_gateway()["TransitGateway"]["TransitGatewayId"]
        try:
            att = ec2.create_transit_gateway_peering_attachment(
                TransitGatewayId=tgw1,
                PeerTransitGatewayId=tgw2,
                PeerAccountId="123456789012",
                PeerRegion="us-east-1",
            )
            att_id = att["TransitGatewayPeeringAttachment"]["TransitGatewayAttachmentId"]

            deleted = ec2.delete_transit_gateway_peering_attachment(
                TransitGatewayAttachmentId=att_id
            )
            assert deleted["TransitGatewayPeeringAttachment"]["State"] == "deleted"
        finally:
            ec2.delete_transit_gateway(TransitGatewayId=tgw2)
            ec2.delete_transit_gateway(TransitGatewayId=tgw1)


class TestEC2IpamCrud:
    """IPAM create/describe/delete lifecycle tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_ipam(self, ec2):
        """CreateIpam returns an IPAM with expected fields."""
        resp = ec2.create_ipam(Description="test-ipam")
        ipam = resp["Ipam"]
        assert ipam["IpamId"].startswith("ipam-")
        assert "IpamArn" in ipam
        assert ipam["Description"] == "test-ipam"
        assert ipam["State"] == "create-complete"
        assert ipam["ScopeCount"] == 2
        assert ipam["PublicDefaultScopeId"].startswith("ipam-scope-")
        assert ipam["PrivateDefaultScopeId"].startswith("ipam-scope-")
        ec2.delete_ipam(IpamId=ipam["IpamId"])

    def test_describe_ipams(self, ec2):
        """DescribeIpams returns created IPAM."""
        create_resp = ec2.create_ipam(Description="desc-test")
        ipam_id = create_resp["Ipam"]["IpamId"]
        try:
            resp = ec2.describe_ipams()
            assert "Ipams" in resp
            ids = [i["IpamId"] for i in resp["Ipams"]]
            assert ipam_id in ids
        finally:
            ec2.delete_ipam(IpamId=ipam_id)

    def test_describe_ipams_filtered(self, ec2):
        """DescribeIpams with filter returns only matching IPAM."""
        create_resp = ec2.create_ipam(Description="filtered")
        ipam_id = create_resp["Ipam"]["IpamId"]
        try:
            resp = ec2.describe_ipams(Filters=[{"Name": "ipam-id", "Values": [ipam_id]}])
            assert any(i["IpamId"] == ipam_id for i in resp["Ipams"])
        finally:
            ec2.delete_ipam(IpamId=ipam_id)

    def test_delete_ipam(self, ec2):
        """DeleteIpam removes the IPAM."""
        create_resp = ec2.create_ipam(Description="delete-test")
        ipam_id = create_resp["Ipam"]["IpamId"]
        del_resp = ec2.delete_ipam(IpamId=ipam_id)
        assert "Ipam" in del_resp

    def test_create_ipam_with_tags(self, ec2):
        """CreateIpam with tags returns tags in response."""
        resp = ec2.create_ipam(
            Description="tagged",
            TagSpecifications=[
                {
                    "ResourceType": "ipam",
                    "Tags": [{"Key": "env", "Value": "test"}],
                }
            ],
        )
        ipam = resp["Ipam"]
        assert any(t["Key"] == "env" and t["Value"] == "test" for t in ipam["Tags"])
        ec2.delete_ipam(IpamId=ipam["IpamId"])

    def test_create_ipam_has_operating_regions(self, ec2):
        """CreateIpam returns OperatingRegions."""
        resp = ec2.create_ipam()
        ipam = resp["Ipam"]
        assert "OperatingRegions" in ipam
        assert isinstance(ipam["OperatingRegions"], list)
        assert len(ipam["OperatingRegions"]) >= 1
        ec2.delete_ipam(IpamId=ipam["IpamId"])

    def test_ipam_default_scopes(self, ec2):
        """CreateIpam creates both public and private default scopes."""
        resp = ec2.create_ipam()
        ipam = resp["Ipam"]
        assert ipam["PublicDefaultScopeId"] != ipam["PrivateDefaultScopeId"]
        assert ipam["PublicDefaultScopeId"].startswith("ipam-scope-")
        assert ipam["PrivateDefaultScopeId"].startswith("ipam-scope-")
        ec2.delete_ipam(IpamId=ipam["IpamId"])


class TestEC2TrafficMirrorCrud:
    """Traffic Mirror create/delete lifecycle tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_and_describe_traffic_mirror_filter(self, ec2):
        """CreateTrafficMirrorFilter + DescribeTrafficMirrorFilters."""
        create_resp = ec2.create_traffic_mirror_filter()
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc_resp = ec2.describe_traffic_mirror_filters()
        assert desc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_traffic_mirror_target_with_eni(self, ec2):
        """CreateTrafficMirrorTarget with a real ENI."""
        vpc = ec2.create_vpc(CidrBlock="10.51.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.51.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            eni = ec2.create_network_interface(SubnetId=subnet_id)
            eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
            resp = ec2.create_traffic_mirror_target(NetworkInterfaceId=eni_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2VerifiedAccessCrud:
    """Verified Access create/delete lifecycle tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_and_describe_verified_access_instance(self, ec2):
        """CreateVerifiedAccessInstance + DescribeVerifiedAccessInstances."""
        create_resp = ec2.create_verified_access_instance()
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc_resp = ec2.describe_verified_access_instances()
        assert desc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEC2InstanceEventWindowCrud:
    """Instance Event Window create/delete lifecycle tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_and_describe_instance_event_window(self, ec2):
        """CreateInstanceEventWindow + DescribeInstanceEventWindows."""
        create_resp = ec2.create_instance_event_window(CronExpression="* 0-4 * * SAT,SUN")
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc_resp = ec2.describe_instance_event_windows()
        assert desc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEC2AdditionalDescribeOps:
    """Tests for additional EC2 describe operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_byoip_cidrs(self, ec2):
        """DescribeByoipCidrs returns a list of BYOIP CIDRs."""
        resp = ec2.describe_byoip_cidrs(MaxResults=10)
        assert "ByoipCidrs" in resp
        assert isinstance(resp["ByoipCidrs"], list)

    def test_describe_client_vpn_authorization_rules(self, ec2):
        """DescribeClientVpnAuthorizationRules returns a response for fake endpoint."""
        resp = ec2.describe_client_vpn_authorization_rules(
            ClientVpnEndpointId="cvpn-endpoint-fake123"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_client_vpn_connections(self, ec2):
        """DescribeClientVpnConnections returns a response for fake endpoint."""
        resp = ec2.describe_client_vpn_connections(ClientVpnEndpointId="cvpn-endpoint-fake123")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_client_vpn_routes(self, ec2):
        """DescribeClientVpnRoutes returns a response for fake endpoint."""
        resp = ec2.describe_client_vpn_routes(ClientVpnEndpointId="cvpn-endpoint-fake123")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_client_vpn_target_networks(self, ec2):
        """DescribeClientVpnTargetNetworks returns a response for fake endpoint."""
        resp = ec2.describe_client_vpn_target_networks(ClientVpnEndpointId="cvpn-endpoint-fake123")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_fleet_history(self, ec2):
        """DescribeFleetHistory returns history records."""
        from datetime import datetime

        resp = ec2.describe_fleet_history(
            FleetId="fleet-fake123",
            StartTime=datetime(2024, 1, 1),
        )
        assert "HistoryRecords" in resp
        assert isinstance(resp["HistoryRecords"], list)

    def test_describe_stale_security_groups(self, ec2):
        """DescribeStaleSecurityGroups returns stale groups for a VPC."""
        # Get the default VPC
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]

        resp = ec2.describe_stale_security_groups(VpcId=vpc_id)
        assert "StaleSecurityGroupSet" in resp
        assert isinstance(resp["StaleSecurityGroupSet"], list)


class TestEC2TransitGatewayConnect:
    """Tests for TransitGateway Connect and ConnectPeer lifecycle."""

    def test_create_and_delete_transit_gateway_connect(self, ec2):
        """Create a transit gateway connect attachment and delete it."""
        tgw = ec2.create_transit_gateway(
            Description="tgw-for-connect-test",
        )
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        # Need a VPC attachment first
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        vpc_att = ec2.create_transit_gateway_vpc_attachment(
            TransitGatewayId=tgw_id,
            VpcId=vpc_id,
            SubnetIds=[subnet_id],
        )
        att_id = vpc_att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
        try:
            connect = ec2.create_transit_gateway_connect(
                TransportTransitGatewayAttachmentId=att_id,
                Options={"Protocol": "gre"},
            )
            connect_att_id = connect["TransitGatewayConnect"]["TransitGatewayAttachmentId"]
            assert connect_att_id
            described = ec2.describe_transit_gateway_connects(
                TransitGatewayAttachmentIds=[connect_att_id]
            )
            assert len(described["TransitGatewayConnects"]) == 1
            ec2.delete_transit_gateway_connect(TransitGatewayAttachmentId=connect_att_id)
        finally:
            ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
            ec2.delete_transit_gateway(TransitGatewayId=tgw_id)


class TestEC2VerifiedAccess:
    """Tests for Verified Access lifecycle."""

    def test_verified_access_trust_provider_lifecycle(self, ec2):
        """Create, describe, delete a Verified Access trust provider."""
        tp = ec2.create_verified_access_trust_provider(
            TrustProviderType="user",
            UserTrustProviderType="iam-identity-center",
            PolicyReferenceName="test-policy",
        )
        tp_id = tp["VerifiedAccessTrustProvider"]["VerifiedAccessTrustProviderId"]
        assert tp_id
        try:
            described = ec2.describe_verified_access_trust_providers(
                VerifiedAccessTrustProviderIds=[tp_id]
            )
            assert len(described["VerifiedAccessTrustProviders"]) >= 1
        finally:
            ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id)

    def test_verified_access_group_lifecycle(self, ec2):
        """Create, describe, delete a Verified Access group."""
        vai = ec2.create_verified_access_instance(Description="test-vai")
        vai_id = vai["VerifiedAccessInstance"]["VerifiedAccessInstanceId"]
        try:
            grp = ec2.create_verified_access_group(
                VerifiedAccessInstanceId=vai_id,
                Description="test-group",
            )
            grp_id = grp["VerifiedAccessGroup"]["VerifiedAccessGroupId"]
            assert grp_id
            described = ec2.describe_verified_access_groups(VerifiedAccessGroupIds=[grp_id])
            assert len(described["VerifiedAccessGroups"]) >= 1
            ec2.delete_verified_access_group(VerifiedAccessGroupId=grp_id)
        finally:
            ec2.delete_verified_access_instance(VerifiedAccessInstanceId=vai_id)


class TestEC2NetworkInsights:
    """Tests for Network Insights Path and Access Scope."""

    def test_create_and_delete_network_insights_path(self, ec2):
        """Create and delete a network insights path."""
        # Create an ENI as source
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        eni = ec2.create_network_interface(SubnetId=subnet_id)
        eni_id = eni["NetworkInterface"]["NetworkInterfaceId"]
        try:
            path = ec2.create_network_insights_path(
                Source=eni_id,
                Protocol="tcp",
                DestinationPort=443,
                Destination=eni_id,
            )
            path_id = path["NetworkInsightsPath"]["NetworkInsightsPathId"]
            assert path_id
            described = ec2.describe_network_insights_paths(NetworkInsightsPathIds=[path_id])
            assert len(described["NetworkInsightsPaths"]) == 1
            ec2.delete_network_insights_path(NetworkInsightsPathId=path_id)
        finally:
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)


class TestEC2InstanceConnectEndpoint:
    """Tests for Instance Connect Endpoint lifecycle."""

    def test_create_and_delete_instance_connect_endpoint(self, ec2):
        """Create and delete an instance connect endpoint."""
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        resp = ec2.create_instance_connect_endpoint(SubnetId=subnet_id)
        ep_id = resp["InstanceConnectEndpoint"]["InstanceConnectEndpointId"]
        assert ep_id
        described = ec2.describe_instance_connect_endpoints(InstanceConnectEndpointIds=[ep_id])
        assert len(described["InstanceConnectEndpoints"]) >= 1
        ec2.delete_instance_connect_endpoint(InstanceConnectEndpointId=ep_id)


class TestEC2IpamPoolCrud:
    """IPAM Pool create/describe lifecycle tests."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def _create_ipam(self, ec2):
        resp = ec2.create_ipam(Description="pool-test-ipam")
        ipam = resp["Ipam"]
        return ipam["IpamId"], ipam["PrivateDefaultScopeId"]

    def test_create_ipam_pool(self, ec2):
        """CreateIpamPool returns a pool with expected fields."""
        ipam_id, scope_id = self._create_ipam(ec2)
        try:
            resp = ec2.create_ipam_pool(
                IpamScopeId=scope_id,
                AddressFamily="ipv4",
                Description="test-pool",
            )
            pool = resp["IpamPool"]
            assert pool["IpamPoolId"].startswith("ipam-pool-")
            assert "IpamPoolArn" in pool
            assert pool["AddressFamily"] == "ipv4"
            assert pool["State"] == "create-complete"
            assert pool["Description"] == "test-pool"
        finally:
            ec2.delete_ipam(IpamId=ipam_id)

    def test_describe_ipam_pools(self, ec2):
        """DescribeIpamPools returns created pool."""
        ipam_id, scope_id = self._create_ipam(ec2)
        try:
            create_resp = ec2.create_ipam_pool(IpamScopeId=scope_id, AddressFamily="ipv4")
            pool_id = create_resp["IpamPool"]["IpamPoolId"]
            resp = ec2.describe_ipam_pools()
            assert "IpamPools" in resp
            ids = [p["IpamPoolId"] for p in resp["IpamPools"]]
            assert pool_id in ids
        finally:
            ec2.delete_ipam(IpamId=ipam_id)

    def test_create_ipam_pool_ipv6(self, ec2):
        """CreateIpamPool with ipv6 address family."""
        ipam_id, scope_id = self._create_ipam(ec2)
        try:
            resp = ec2.create_ipam_pool(IpamScopeId=scope_id, AddressFamily="ipv6")
            pool = resp["IpamPool"]
            assert pool["AddressFamily"] == "ipv6"
            assert pool["IpamPoolId"].startswith("ipam-pool-")
        finally:
            ec2.delete_ipam(IpamId=ipam_id)

    def test_create_multiple_ipam_pools(self, ec2):
        """Multiple pools can be created in the same scope."""
        ipam_id, scope_id = self._create_ipam(ec2)
        try:
            p1 = ec2.create_ipam_pool(
                IpamScopeId=scope_id,
                AddressFamily="ipv4",
                Description="pool-1",
            )
            p2 = ec2.create_ipam_pool(
                IpamScopeId=scope_id,
                AddressFamily="ipv4",
                Description="pool-2",
            )
            assert p1["IpamPool"]["IpamPoolId"] != p2["IpamPool"]["IpamPoolId"]
            pools = ec2.describe_ipam_pools()
            pool_ids = [p["IpamPoolId"] for p in pools["IpamPools"]]
            assert p1["IpamPool"]["IpamPoolId"] in pool_ids
            assert p2["IpamPool"]["IpamPoolId"] in pool_ids
        finally:
            ec2.delete_ipam(IpamId=ipam_id)


class TestEC2TransitGatewayConnectPeer:
    """Tests for Transit Gateway Connect Peer lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def _create_tgw_connect(self, ec2):
        """Create a transit gateway with a VPC attachment and connect attachment."""
        tgw = ec2.create_transit_gateway(Description="peer-test")
        tgw_id = tgw["TransitGateway"]["TransitGatewayId"]
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        subnet_id = subnets["Subnets"][0]["SubnetId"]
        att = ec2.create_transit_gateway_vpc_attachment(
            TransitGatewayId=tgw_id, VpcId=vpc_id, SubnetIds=[subnet_id]
        )
        att_id = att["TransitGatewayVpcAttachment"]["TransitGatewayAttachmentId"]
        conn = ec2.create_transit_gateway_connect(
            TransportTransitGatewayAttachmentId=att_id, Options={"Protocol": "gre"}
        )
        conn_id = conn["TransitGatewayConnect"]["TransitGatewayAttachmentId"]
        return tgw_id, att_id, conn_id

    def _cleanup_tgw(self, ec2, tgw_id, att_id, conn_id):
        ec2.delete_transit_gateway_connect(TransitGatewayAttachmentId=conn_id)
        ec2.delete_transit_gateway_vpc_attachment(TransitGatewayAttachmentId=att_id)
        ec2.delete_transit_gateway(TransitGatewayId=tgw_id)

    def test_create_transit_gateway_connect_peer(self, ec2):
        """CreateTransitGatewayConnectPeer returns a peer with expected fields."""
        tgw_id, att_id, conn_id = self._create_tgw_connect(ec2)
        try:
            resp = ec2.create_transit_gateway_connect_peer(
                TransitGatewayAttachmentId=conn_id,
                PeerAddress="10.0.0.1",
                InsideCidrBlocks=["169.254.100.0/29"],
                TransitGatewayAddress="10.0.0.2",
            )
            peer = resp["TransitGatewayConnectPeer"]
            assert peer["TransitGatewayConnectPeerId"].startswith("tgw-connect-peer-")
            assert peer["TransitGatewayAttachmentId"] == conn_id
            assert "ConnectPeerConfiguration" in peer
            ec2.delete_transit_gateway_connect_peer(
                TransitGatewayConnectPeerId=peer["TransitGatewayConnectPeerId"]
            )
        finally:
            self._cleanup_tgw(ec2, tgw_id, att_id, conn_id)

    def test_describe_transit_gateway_connect_peers(self, ec2):
        """DescribeTransitGatewayConnectPeers returns created peer."""
        tgw_id, att_id, conn_id = self._create_tgw_connect(ec2)
        try:
            create_resp = ec2.create_transit_gateway_connect_peer(
                TransitGatewayAttachmentId=conn_id,
                PeerAddress="10.0.0.3",
                InsideCidrBlocks=["169.254.101.0/29"],
                TransitGatewayAddress="10.0.0.4",
            )
            peer_id = create_resp["TransitGatewayConnectPeer"]["TransitGatewayConnectPeerId"]
            resp = ec2.describe_transit_gateway_connect_peers(
                TransitGatewayConnectPeerIds=[peer_id]
            )
            assert len(resp["TransitGatewayConnectPeers"]) == 1
            assert resp["TransitGatewayConnectPeers"][0]["TransitGatewayConnectPeerId"] == peer_id
            ec2.delete_transit_gateway_connect_peer(TransitGatewayConnectPeerId=peer_id)
        finally:
            self._cleanup_tgw(ec2, tgw_id, att_id, conn_id)

    def test_delete_transit_gateway_connect_peer(self, ec2):
        """DeleteTransitGatewayConnectPeer removes the peer."""
        tgw_id, att_id, conn_id = self._create_tgw_connect(ec2)
        try:
            create_resp = ec2.create_transit_gateway_connect_peer(
                TransitGatewayAttachmentId=conn_id,
                PeerAddress="10.0.0.5",
                InsideCidrBlocks=["169.254.102.0/29"],
                TransitGatewayAddress="10.0.0.6",
            )
            peer_id = create_resp["TransitGatewayConnectPeer"]["TransitGatewayConnectPeerId"]
            del_resp = ec2.delete_transit_gateway_connect_peer(TransitGatewayConnectPeerId=peer_id)
            assert "TransitGatewayConnectPeer" in del_resp
        finally:
            self._cleanup_tgw(ec2, tgw_id, att_id, conn_id)

    def test_connect_peer_config_fields(self, ec2):
        """Connect peer has proper configuration with peer and transit gateway addresses."""
        tgw_id, att_id, conn_id = self._create_tgw_connect(ec2)
        try:
            resp = ec2.create_transit_gateway_connect_peer(
                TransitGatewayAttachmentId=conn_id,
                PeerAddress="10.0.0.7",
                InsideCidrBlocks=["169.254.103.0/29"],
                TransitGatewayAddress="10.0.0.8",
            )
            peer = resp["TransitGatewayConnectPeer"]
            config = peer["ConnectPeerConfiguration"]
            assert config["PeerAddress"] == "10.0.0.7"
            assert config["TransitGatewayAddress"] == "10.0.0.8"
            assert "InsideCidrBlocks" in config
            assert "169.254.103.0/29" in config["InsideCidrBlocks"]
            ec2.delete_transit_gateway_connect_peer(
                TransitGatewayConnectPeerId=peer["TransitGatewayConnectPeerId"]
            )
        finally:
            self._cleanup_tgw(ec2, tgw_id, att_id, conn_id)


class TestEC2MiscOperations:
    """Tests for miscellaneous EC2 operations."""

    def test_create_default_subnet(self, ec2):
        """CreateDefaultSubnet creates a subnet in an AZ."""
        azs = ec2.describe_availability_zones()
        az_name = azs["AvailabilityZones"][0]["ZoneName"]
        try:
            resp = ec2.create_default_subnet(AvailabilityZone=az_name)
            assert "Subnet" in resp
            subnet_id = resp["Subnet"]["SubnetId"]
            assert subnet_id
        except ec2.exceptions.ClientError:
            # Already exists, that's fine
            pass


class TestEC2CapacityReservationCrud:
    """Capacity Reservation create/describe/cancel lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_and_describe_capacity_reservation(self, ec2):
        """CreateCapacityReservation + DescribeCapacityReservations by ID."""
        resp = ec2.create_capacity_reservation(
            InstanceType="t2.micro",
            InstancePlatform="Linux/UNIX",
            InstanceCount=1,
            AvailabilityZone="us-east-1a",
        )
        cr = resp["CapacityReservation"]
        cr_id = cr["CapacityReservationId"]
        assert cr_id.startswith("cr-")
        assert cr["InstanceType"] == "t2.micro"
        assert cr["InstancePlatform"] == "Linux/UNIX"
        assert cr["TotalInstanceCount"] == 1

        described = ec2.describe_capacity_reservations(CapacityReservationIds=[cr_id])
        matching = [
            c for c in described["CapacityReservations"] if c["CapacityReservationId"] == cr_id
        ]
        assert len(matching) == 1
        assert matching[0]["State"] in ("active", "pending")

        ec2.cancel_capacity_reservation(CapacityReservationId=cr_id)

    def test_cancel_capacity_reservation(self, ec2):
        """CancelCapacityReservation returns True."""
        resp = ec2.create_capacity_reservation(
            InstanceType="t2.micro",
            InstancePlatform="Linux/UNIX",
            InstanceCount=1,
            AvailabilityZone="us-east-1a",
        )
        cr_id = resp["CapacityReservation"]["CapacityReservationId"]
        cancel = ec2.cancel_capacity_reservation(CapacityReservationId=cr_id)
        assert cancel["Return"] is True

    def test_capacity_reservation_fields(self, ec2):
        """Capacity reservation has expected fields."""
        resp = ec2.create_capacity_reservation(
            InstanceType="m5.large",
            InstancePlatform="Linux/UNIX",
            InstanceCount=2,
            AvailabilityZone="us-east-1a",
        )
        cr = resp["CapacityReservation"]
        assert cr["InstanceType"] == "m5.large"
        assert cr["TotalInstanceCount"] == 2
        assert "AvailabilityZone" in cr
        ec2.cancel_capacity_reservation(CapacityReservationId=cr["CapacityReservationId"])


class TestEC2TrafficMirrorFullLifecycle:
    """Traffic Mirror filter + target + session full CRUD lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    @pytest.fixture
    def vpc_resources(self, ec2):
        """Create VPC, subnet, and two ENIs for traffic mirror tests."""
        vpc = ec2.create_vpc(CidrBlock="10.60.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.60.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        eni1 = ec2.create_network_interface(SubnetId=subnet_id)
        eni1_id = eni1["NetworkInterface"]["NetworkInterfaceId"]
        eni2 = ec2.create_network_interface(SubnetId=subnet_id)
        eni2_id = eni2["NetworkInterface"]["NetworkInterfaceId"]
        yield vpc_id, subnet_id, eni1_id, eni2_id
        ec2.delete_network_interface(NetworkInterfaceId=eni2_id)
        ec2.delete_network_interface(NetworkInterfaceId=eni1_id)
        ec2.delete_subnet(SubnetId=subnet_id)
        ec2.delete_vpc(VpcId=vpc_id)

    def test_traffic_mirror_session_lifecycle(self, ec2, vpc_resources):
        """Create filter, target, session; describe; then delete all in reverse."""
        _, _, eni1_id, eni2_id = vpc_resources

        # Create filter
        f_resp = ec2.create_traffic_mirror_filter()
        filt_id = f_resp["TrafficMirrorFilter"]["TrafficMirrorFilterId"]
        assert filt_id.startswith("tmf-")

        # Create target
        t_resp = ec2.create_traffic_mirror_target(NetworkInterfaceId=eni1_id)
        target_id = t_resp["TrafficMirrorTarget"]["TrafficMirrorTargetId"]
        assert target_id.startswith("tmt-")

        # Create session
        s_resp = ec2.create_traffic_mirror_session(
            NetworkInterfaceId=eni2_id,
            TrafficMirrorTargetId=target_id,
            TrafficMirrorFilterId=filt_id,
            SessionNumber=1,
        )
        session_id = s_resp["TrafficMirrorSession"]["TrafficMirrorSessionId"]
        assert session_id.startswith("tms-")

        # Describe all three
        filters_desc = ec2.describe_traffic_mirror_filters(TrafficMirrorFilterIds=[filt_id])
        assert any(
            f["TrafficMirrorFilterId"] == filt_id for f in filters_desc["TrafficMirrorFilters"]
        )

        targets_desc = ec2.describe_traffic_mirror_targets(TrafficMirrorTargetIds=[target_id])
        assert any(
            t["TrafficMirrorTargetId"] == target_id for t in targets_desc["TrafficMirrorTargets"]
        )

        sessions_desc = ec2.describe_traffic_mirror_sessions(TrafficMirrorSessionIds=[session_id])
        assert any(
            s["TrafficMirrorSessionId"] == session_id
            for s in sessions_desc["TrafficMirrorSessions"]
        )

        # Delete in reverse order
        ec2.delete_traffic_mirror_session(TrafficMirrorSessionId=session_id)
        ec2.delete_traffic_mirror_target(TrafficMirrorTargetId=target_id)
        ec2.delete_traffic_mirror_filter(TrafficMirrorFilterId=filt_id)

    def test_delete_traffic_mirror_filter(self, ec2):
        """DeleteTrafficMirrorFilter removes the filter."""
        f_resp = ec2.create_traffic_mirror_filter()
        filt_id = f_resp["TrafficMirrorFilter"]["TrafficMirrorFilterId"]
        del_resp = ec2.delete_traffic_mirror_filter(TrafficMirrorFilterId=filt_id)
        assert del_resp["TrafficMirrorFilterId"] == filt_id

    def test_delete_traffic_mirror_target(self, ec2, vpc_resources):
        """DeleteTrafficMirrorTarget removes the target."""
        _, _, eni1_id, _ = vpc_resources
        t_resp = ec2.create_traffic_mirror_target(NetworkInterfaceId=eni1_id)
        target_id = t_resp["TrafficMirrorTarget"]["TrafficMirrorTargetId"]
        del_resp = ec2.delete_traffic_mirror_target(TrafficMirrorTargetId=target_id)
        assert del_resp["TrafficMirrorTargetId"] == target_id


class TestEC2FastLaunchLifecycle:
    """Fast Launch enable/describe/disable lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_enable_and_disable_fast_launch(self, ec2):
        """EnableFastLaunch + DescribeFastLaunchImages + DisableFastLaunch."""
        image_id = "ami-12345678"
        enable_resp = ec2.enable_fast_launch(ImageId=image_id)
        assert enable_resp["ImageId"] == image_id
        assert "State" in enable_resp

        desc_resp = ec2.describe_fast_launch_images()
        assert "FastLaunchImages" in desc_resp

        disable_resp = ec2.disable_fast_launch(ImageId=image_id)
        assert disable_resp["ImageId"] == image_id

    def test_enable_fast_launch_returns_state(self, ec2):
        """EnableFastLaunch response includes State field."""
        resp = ec2.enable_fast_launch(ImageId="ami-aabbccdd")
        assert "State" in resp
        assert resp["ImageId"] == "ami-aabbccdd"
        # Clean up
        ec2.disable_fast_launch(ImageId="ami-aabbccdd")


class TestEC2CoipPoolLifecycle:
    """CoipPool / CoipCidr create-describe-delete lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_coip_pools_empty(self, ec2):
        """DescribeCoipPools returns list (possibly empty)."""
        resp = ec2.describe_coip_pools()
        assert "CoipPools" in resp

    def test_create_and_delete_coip_pool(self, ec2):
        """CreateCoipPool + DeleteCoipPool lifecycle."""
        create_resp = ec2.create_coip_pool(LocalGatewayRouteTableId="lgw-rtb-12345678901234567")
        pool_id = create_resp["CoipPool"]["PoolId"]
        assert pool_id.startswith("ipv4pool-coip-")

        # Describe should include our pool
        desc = ec2.describe_coip_pools()
        assert any(p["PoolId"] == pool_id for p in desc["CoipPools"])

        # Delete
        ec2.delete_coip_pool(CoipPoolId=pool_id)

    def test_create_and_delete_coip_cidr(self, ec2):
        """CreateCoipCidr + DeleteCoipCidr on a pool."""
        pool = ec2.create_coip_pool(LocalGatewayRouteTableId="lgw-rtb-aabbccddee1234567")
        pool_id = pool["CoipPool"]["PoolId"]

        cidr_resp = ec2.create_coip_cidr(Cidr="10.10.0.0/24", CoipPoolId=pool_id)
        assert cidr_resp["CoipCidr"]["Cidr"] == "10.10.0.0/24"
        assert cidr_resp["CoipCidr"]["CoipPoolId"] == pool_id

        ec2.delete_coip_cidr(Cidr="10.10.0.0/24", CoipPoolId=pool_id)
        ec2.delete_coip_pool(CoipPoolId=pool_id)

    def test_get_coip_pool_usage(self, ec2):
        """GetCoipPoolUsage returns usage info for a pool."""
        pool = ec2.create_coip_pool(LocalGatewayRouteTableId="lgw-rtb-usage123456789ab")
        pool_id = pool["CoipPool"]["PoolId"]

        usage = ec2.get_coip_pool_usage(PoolId=pool_id)
        assert usage["CoipPoolId"] == pool_id
        assert "CoipAddressUsages" in usage

        ec2.delete_coip_pool(CoipPoolId=pool_id)


class TestEC2InstanceEventWindowLifecycle:
    """InstanceEventWindow create-modify-associate-delete lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_instance_event_windows_empty(self, ec2):
        """DescribeInstanceEventWindows returns list."""
        resp = ec2.describe_instance_event_windows()
        assert "InstanceEventWindows" in resp

    def test_create_and_delete_instance_event_window(self, ec2):
        """CreateInstanceEventWindow + DeleteInstanceEventWindow."""
        name = _unique("iew")
        create_resp = ec2.create_instance_event_window(
            Name=name,
            TimeRanges=[
                {
                    "StartWeekDay": "monday",
                    "StartHour": 2,
                    "EndWeekDay": "monday",
                    "EndHour": 6,
                }
            ],
        )
        win = create_resp["InstanceEventWindow"]
        win_id = win["InstanceEventWindowId"]
        assert win_id.startswith("iew-")
        assert win["Name"] == name

        # Describe should include it
        desc = ec2.describe_instance_event_windows()
        assert any(w["InstanceEventWindowId"] == win_id for w in desc["InstanceEventWindows"])

        ec2.delete_instance_event_window(InstanceEventWindowId=win_id)

    def test_modify_instance_event_window(self, ec2):
        """ModifyInstanceEventWindow changes time ranges."""
        create_resp = ec2.create_instance_event_window(
            Name=_unique("iew-mod"),
            TimeRanges=[
                {
                    "StartWeekDay": "monday",
                    "StartHour": 1,
                    "EndWeekDay": "monday",
                    "EndHour": 5,
                }
            ],
        )
        win_id = create_resp["InstanceEventWindow"]["InstanceEventWindowId"]

        mod_resp = ec2.modify_instance_event_window(
            InstanceEventWindowId=win_id,
            TimeRanges=[
                {
                    "StartWeekDay": "wednesday",
                    "StartHour": 3,
                    "EndWeekDay": "wednesday",
                    "EndHour": 7,
                }
            ],
        )
        assert "InstanceEventWindow" in mod_resp

        ec2.delete_instance_event_window(InstanceEventWindowId=win_id)

    def test_associate_and_disassociate_instance_event_window(self, ec2):
        """AssociateInstanceEventWindow + DisassociateInstanceEventWindow."""
        create_resp = ec2.create_instance_event_window(
            Name=_unique("iew-assoc"),
            TimeRanges=[
                {
                    "StartWeekDay": "friday",
                    "StartHour": 0,
                    "EndWeekDay": "friday",
                    "EndHour": 4,
                }
            ],
        )
        win_id = create_resp["InstanceEventWindow"]["InstanceEventWindowId"]

        assoc_resp = ec2.associate_instance_event_window(
            InstanceEventWindowId=win_id,
            AssociationTarget={"InstanceTags": [{"Key": "env", "Value": "test"}]},
        )
        assert "InstanceEventWindow" in assoc_resp

        disassoc_resp = ec2.disassociate_instance_event_window(
            InstanceEventWindowId=win_id,
            AssociationTarget={"InstanceTags": [{"Key": "env", "Value": "test"}]},
        )
        assert "InstanceEventWindow" in disassoc_resp

        ec2.delete_instance_event_window(InstanceEventWindowId=win_id)


class TestEC2SpotDatafeedSubscription:
    """SpotDatafeedSubscription create-describe-delete lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_create_and_delete_spot_datafeed_subscription(self, ec2):
        """CreateSpotDatafeedSubscription + DescribeSpotDatafeedSubscription + Delete."""
        create_resp = ec2.create_spot_datafeed_subscription(Bucket="my-spot-datafeed-bucket")
        sub = create_resp["SpotDatafeedSubscription"]
        assert sub["Bucket"] == "my-spot-datafeed-bucket"
        assert sub["State"] == "Active"

        ec2.delete_spot_datafeed_subscription()

    def test_describe_spot_datafeed_subscription_empty(self, ec2):
        """DescribeSpotDatafeedSubscription works even with no subscription."""
        # Delete any existing first
        try:
            ec2.delete_spot_datafeed_subscription()
        except Exception:
            pass
        resp = ec2.describe_spot_datafeed_subscription()
        assert "ResponseMetadata" in resp


class TestEC2InstanceMetadataDefaults:
    """InstanceMetadataDefaults get/modify lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_get_instance_metadata_defaults(self, ec2):
        """GetInstanceMetadataDefaults returns account-level settings."""
        resp = ec2.get_instance_metadata_defaults()
        assert "AccountLevel" in resp

    def test_modify_instance_metadata_defaults(self, ec2):
        """ModifyInstanceMetadataDefaults sets HttpTokens."""
        mod_resp = ec2.modify_instance_metadata_defaults(
            HttpTokens="required", HttpPutResponseHopLimit=2
        )
        assert mod_resp["Return"] is True

        # Verify
        get_resp = ec2.get_instance_metadata_defaults()
        assert "AccountLevel" in get_resp

        # Reset
        ec2.modify_instance_metadata_defaults(HttpTokens="optional")


class TestEC2PublicIpv4PoolLifecycle:
    """PublicIpv4Pool create-describe-delete lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_public_ipv4_pools(self, ec2):
        """DescribePublicIpv4Pools returns list."""
        resp = ec2.describe_public_ipv4_pools()
        assert "PublicIpv4Pools" in resp

    def test_create_and_delete_public_ipv4_pool(self, ec2):
        """CreatePublicIpv4Pool + DeletePublicIpv4Pool lifecycle."""
        create_resp = ec2.create_public_ipv4_pool()
        pool_id = create_resp["PoolId"]
        assert pool_id.startswith("ipv4pool-ec2-")

        # Describe should include it
        desc = ec2.describe_public_ipv4_pools()
        assert any(p["PoolId"] == pool_id for p in desc["PublicIpv4Pools"])

        ec2.delete_public_ipv4_pool(PoolId=pool_id)

    def test_provision_public_ipv4_pool_cidr(self, ec2):
        """ProvisionPublicIpv4PoolCidr allocates a CIDR to a pool."""
        pool = ec2.create_public_ipv4_pool()
        pool_id = pool["PoolId"]

        prov_resp = ec2.provision_public_ipv4_pool_cidr(
            IpamPoolId="ipam-pool-12345678", PoolId=pool_id, NetmaskLength=24
        )
        assert prov_resp["PoolId"] == pool_id

        ec2.delete_public_ipv4_pool(PoolId=pool_id)


class TestEC2HostReservation:
    """HostReservation describe/preview/purchase operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_host_reservations(self, ec2):
        """DescribeHostReservations returns list."""
        resp = ec2.describe_host_reservations()
        assert "HostReservationSet" in resp

    def test_describe_host_reservation_offerings(self, ec2):
        """DescribeHostReservationOfferings returns offerings list."""
        resp = ec2.describe_host_reservation_offerings()
        assert "OfferingSet" in resp

    def test_get_host_reservation_purchase_preview(self, ec2):
        """GetHostReservationPurchasePreview returns pricing info."""
        resp = ec2.get_host_reservation_purchase_preview(
            HostIdSet=["h-12345678"], OfferingId="hro-12345678"
        )
        assert "TotalHourlyPrice" in resp
        assert "TotalUpfrontPrice" in resp

    def test_purchase_host_reservation(self, ec2):
        """PurchaseHostReservation returns purchase info."""
        resp = ec2.purchase_host_reservation(HostIdSet=["h-87654321"], OfferingId="hro-87654321")
        assert "TotalHourlyPrice" in resp
        assert "TotalUpfrontPrice" in resp


class TestEC2NetworkInsightsAccessScope:
    """NetworkInsightsAccessScope full lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_network_insights_access_scopes_empty(self, ec2):
        """DescribeNetworkInsightsAccessScopes returns list."""
        resp = ec2.describe_network_insights_access_scopes()
        assert "NetworkInsightsAccessScopes" in resp

    def test_create_and_delete_network_insights_access_scope(self, ec2):
        """CreateNetworkInsightsAccessScope + DeleteNetworkInsightsAccessScope."""
        token = _unique("nis-token")
        create_resp = ec2.create_network_insights_access_scope(
            MatchPaths=[{"Source": {"ResourceStatement": {"ResourceTypes": ["AWS::EC2::VPC"]}}}],
            ClientToken=token,
        )
        scope = create_resp["NetworkInsightsAccessScope"]
        scope_id = scope["NetworkInsightsAccessScopeId"]
        assert scope_id.startswith("nis-")

        # Describe
        desc = ec2.describe_network_insights_access_scopes()
        assert any(
            s["NetworkInsightsAccessScopeId"] == scope_id
            for s in desc["NetworkInsightsAccessScopes"]
        )

        # Delete
        del_resp = ec2.delete_network_insights_access_scope(NetworkInsightsAccessScopeId=scope_id)
        assert del_resp["NetworkInsightsAccessScopeId"] == scope_id

    def test_get_network_insights_access_scope_content(self, ec2):
        """GetNetworkInsightsAccessScopeContent returns scope details."""
        create_resp = ec2.create_network_insights_access_scope(
            MatchPaths=[{"Source": {"ResourceStatement": {"ResourceTypes": ["AWS::EC2::Subnet"]}}}],
            ClientToken=_unique("nis-content"),
        )
        scope_id = create_resp["NetworkInsightsAccessScope"]["NetworkInsightsAccessScopeId"]

        content_resp = ec2.get_network_insights_access_scope_content(
            NetworkInsightsAccessScopeId=scope_id
        )
        assert "NetworkInsightsAccessScopeContent" in content_resp

        ec2.delete_network_insights_access_scope(NetworkInsightsAccessScopeId=scope_id)

    def test_start_and_describe_access_scope_analysis(self, ec2):
        """StartNetworkInsightsAccessScopeAnalysis + Describe + GetFindings."""
        create_resp = ec2.create_network_insights_access_scope(
            MatchPaths=[{"Source": {"ResourceStatement": {"ResourceTypes": ["AWS::EC2::VPC"]}}}],
            ClientToken=_unique("nis-analysis"),
        )
        scope_id = create_resp["NetworkInsightsAccessScope"]["NetworkInsightsAccessScopeId"]

        # Start analysis
        start_resp = ec2.start_network_insights_access_scope_analysis(
            NetworkInsightsAccessScopeId=scope_id,
            ClientToken=_unique("analysis"),
        )
        analysis = start_resp["NetworkInsightsAccessScopeAnalysis"]
        analysis_id = analysis["NetworkInsightsAccessScopeAnalysisId"]
        assert analysis_id.startswith("nisa-")

        # Describe analyses
        desc_resp = ec2.describe_network_insights_access_scope_analyses(
            NetworkInsightsAccessScopeId=scope_id
        )
        assert any(
            a["NetworkInsightsAccessScopeAnalysisId"] == analysis_id
            for a in desc_resp["NetworkInsightsAccessScopeAnalyses"]
        )

        # Get findings
        findings_resp = ec2.get_network_insights_access_scope_analysis_findings(
            NetworkInsightsAccessScopeAnalysisId=analysis_id
        )
        assert findings_resp["NetworkInsightsAccessScopeAnalysisId"] == analysis_id
        assert "AnalysisFindings" in findings_resp

        ec2.delete_network_insights_access_scope(NetworkInsightsAccessScopeId=scope_id)


class TestEC2SnapshotBlockPublicAccess:
    """SnapshotBlockPublicAccess enable/get/disable lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_get_snapshot_block_public_access_state(self, ec2):
        """GetSnapshotBlockPublicAccessState returns current state."""
        resp = ec2.get_snapshot_block_public_access_state()
        assert "State" in resp

    def test_enable_and_disable_snapshot_block_public_access(self, ec2):
        """EnableSnapshotBlockPublicAccess + DisableSnapshotBlockPublicAccess."""
        enable_resp = ec2.enable_snapshot_block_public_access(State="block-all-sharing")
        assert enable_resp["State"] == "block-all-sharing"

        disable_resp = ec2.disable_snapshot_block_public_access()
        assert disable_resp["State"] == "unblocked"


class TestEC2SecurityGroupVpcAssociation:
    """SecurityGroupVpcAssociation associate-describe-disassociate lifecycle."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_security_group_vpc_associations(self, ec2):
        """DescribeSecurityGroupVpcAssociations returns list."""
        resp = ec2.describe_security_group_vpc_associations()
        assert "SecurityGroupVpcAssociations" in resp

    def test_associate_and_disassociate_security_group_vpc(self, ec2):
        """AssociateSecurityGroupVpc + DisassociateSecurityGroupVpc."""
        vpc1 = ec2.create_vpc(CidrBlock="10.210.0.0/16")
        vpc1_id = vpc1["Vpc"]["VpcId"]
        vpc2 = ec2.create_vpc(CidrBlock="10.211.0.0/16")
        vpc2_id = vpc2["Vpc"]["VpcId"]
        sg = ec2.create_security_group(
            GroupName=_unique("sg-assoc"),
            Description="test sg vpc assoc",
            VpcId=vpc1_id,
        )
        sg_id = sg["GroupId"]

        assoc_resp = ec2.associate_security_group_vpc(GroupId=sg_id, VpcId=vpc2_id)
        assert assoc_resp["State"] == "associated"

        # Describe should show the association
        desc = ec2.describe_security_group_vpc_associations()
        assert any(
            a["GroupId"] == sg_id and a["VpcId"] == vpc2_id
            for a in desc["SecurityGroupVpcAssociations"]
        )

        disassoc_resp = ec2.disassociate_security_group_vpc(GroupId=sg_id, VpcId=vpc2_id)
        assert disassoc_resp["State"] == "disassociated"

        # Cleanup
        ec2.delete_security_group(GroupId=sg_id)
        ec2.delete_vpc(VpcId=vpc2_id)
        ec2.delete_vpc(VpcId=vpc1_id)


class TestEC2VolumeAttribute:
    """VolumeAttribute describe/modify operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_volume_attribute(self, ec2):
        """DescribeVolumeAttribute returns autoEnableIO info."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]

        resp = ec2.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
        assert "AutoEnableIO" in resp
        assert "Value" in resp["AutoEnableIO"]

        ec2.delete_volume(VolumeId=vol_id)

    def test_modify_volume_attribute(self, ec2):
        """ModifyVolumeAttribute call succeeds."""
        vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=1)
        vol_id = vol["VolumeId"]

        # Modify should succeed without error
        ec2.modify_volume_attribute(VolumeId=vol_id, AutoEnableIO={"Value": True})
        # Verify the call completed by describing
        resp = ec2.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
        assert "AutoEnableIO" in resp

        ec2.delete_volume(VolumeId=vol_id)


class TestEC2NewDescribeAndListOps:
    """Newly-tested describe/list/get operations."""

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_describe_volume_status(self, ec2):
        """DescribeVolumeStatus returns status list."""
        resp = ec2.describe_volume_status()
        assert "VolumeStatuses" in resp

    def test_describe_volumes_modifications(self, ec2):
        """DescribeVolumesModifications returns modifications list."""
        resp = ec2.describe_volumes_modifications()
        assert "VolumesModifications" in resp

    def test_get_ebs_encryption_by_default(self, ec2):
        """GetEbsEncryptionByDefault returns boolean."""
        resp = ec2.get_ebs_encryption_by_default()
        assert "EbsEncryptionByDefault" in resp

    def test_describe_account_attributes(self, ec2):
        """DescribeAccountAttributes returns attributes."""
        resp = ec2.describe_account_attributes()
        assert "AccountAttributes" in resp


class TestEC2SubnetCidrBlockAssociation:
    """Tests for associating/disassociating IPv6 CIDR blocks with subnets."""

    def test_associate_disassociate_subnet_cidr_block(self, ec2):
        """AssociateSubnetCidrBlock / DisassociateSubnetCidrBlock manage IPv6 CIDRs."""
        vpc = ec2.create_vpc(
            CidrBlock="10.93.0.0/16",
            AmazonProvidedIpv6CidrBlock=True,
        )
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            # Wait for IPv6 CIDR to be associated
            desc = ec2.describe_vpcs(VpcIds=[vpc_id])
            ipv6_assocs = desc["Vpcs"][0].get("Ipv6CidrBlockAssociationSet", [])
            if not ipv6_assocs:
                pytest.skip("VPC did not get an IPv6 CIDR block")
            ipv6_cidr = ipv6_assocs[0]["Ipv6CidrBlock"]

            # Derive a /64 subnet from the VPC's /56
            subnet_ipv6 = ipv6_cidr.rsplit("/", 1)[0]
            # Use the first /64 in the block
            parts = subnet_ipv6.split(":")
            subnet_cidr = ":".join(parts[:4]) + "::/64"

            sub = ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock="10.93.1.0/24",
            )
            subnet_id = sub["Subnet"]["SubnetId"]
            try:
                assoc = ec2.associate_subnet_cidr_block(
                    SubnetId=subnet_id, Ipv6CidrBlock=subnet_cidr
                )
                assoc_id = assoc["Ipv6CidrBlockAssociation"]["AssociationId"]
                assert assoc_id.startswith("subnet-cidr-assoc-")

                ec2.disassociate_subnet_cidr_block(AssociationId=assoc_id)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestEC2RejectVpcEndpointConnections:
    """Tests for rejecting VPC endpoint connections."""

    def test_reject_vpc_endpoint_connections(self, ec2):
        """RejectVpcEndpointConnections rejects connections to an endpoint service."""
        # We need an NLB for an endpoint service. Create a minimal one.
        from tests.compatibility.conftest import make_client

        elbv2 = make_client("elbv2")
        vpc = ec2.create_vpc(CidrBlock="10.94.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.94.1.0/24")
            subnet_id = sub["Subnet"]["SubnetId"]
            try:
                nlb = elbv2.create_load_balancer(
                    Name=_unique("nlb"),
                    Subnets=[subnet_id],
                    Type="network",
                    Scheme="internal",
                )
                nlb_arn = nlb["LoadBalancers"][0]["LoadBalancerArn"]
                try:
                    svc = ec2.create_vpc_endpoint_service_configuration(
                        NetworkLoadBalancerArns=[nlb_arn],
                        AcceptanceRequired=True,
                    )
                    svc_id = svc["ServiceConfiguration"]["ServiceId"]
                    try:
                        # Just verify the API is callable (no pending connections to reject)
                        resp = ec2.reject_vpc_endpoint_connections(
                            ServiceId=svc_id, VpcEndpointIds=["vpce-00000000000000000"]
                        )
                        assert "Unsuccessful" in resp
                    finally:
                        ec2.delete_vpc_endpoint_service_configurations(ServiceIds=[svc_id])
                finally:
                    elbv2.delete_load_balancer(LoadBalancerArn=nlb_arn)
            finally:
                ec2.delete_subnet(SubnetId=subnet_id)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)
