"""Compatibility tests for AWS Directory Service (DS)."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def ds():
    return make_client("ds")


@pytest.fixture
def ec2():
    return make_client("ec2")


@pytest.fixture
def directory(ds, ec2):
    """Create a SimpleAD directory with VPC infrastructure, clean up after test."""
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet1 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
    )
    subnet2 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
    )
    sid1 = subnet1["Subnet"]["SubnetId"]
    sid2 = subnet2["Subnet"]["SubnetId"]

    resp = ds.create_directory(
        Name="corp.example.com",
        Password="P@ssw0rd!",
        Size="Small",
        VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
    )
    dir_id = resp["DirectoryId"]

    yield dir_id

    # Cleanup: delete directory, subnets, VPC
    try:
        ds.delete_directory(DirectoryId=dir_id)
    except ClientError:
        pass
    for sid in [sid1, sid2]:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass


class TestDSDescribeOperations:
    """Test describe operations against an empty state."""

    def test_describe_directories_empty(self, ds):
        """describe_directories with no filter returns a list (possibly empty)."""
        resp = ds.describe_directories()
        assert "DirectoryDescriptions" in resp
        assert isinstance(resp["DirectoryDescriptions"], list)

    def test_describe_directories_invalid_id(self, ds):
        """describe_directories with a bogus ID raises ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.describe_directories(DirectoryIds=["d-bogus123"])
        assert exc_info.value.response["Error"]["Code"] in (
            "ValidationException",
            "EntityDoesNotExistException",
        )


class TestDSDirectoryOperations:
    """Test create, describe, and delete directory lifecycle."""

    def test_create_directory(self, directory, ds):
        """create_directory returns a DirectoryId and directory is describable."""
        assert directory.startswith("d-")

        resp = ds.describe_directories(DirectoryIds=[directory])
        descriptions = resp["DirectoryDescriptions"]
        assert len(descriptions) == 1
        d = descriptions[0]
        assert d["DirectoryId"] == directory
        assert d["Name"] == "corp.example.com"
        assert d["Size"] == "Small"
        assert d["Type"] == "SimpleAD"

    def test_delete_directory(self, ds, ec2):
        """delete_directory removes the directory so it cannot be described."""
        vpc = ec2.create_vpc(CidrBlock="10.1.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.1.1.0/24", AvailabilityZone="us-east-1a")
        s2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.1.2.0/24", AvailabilityZone="us-east-1b")
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]

        resp = ds.create_directory(
            Name="del.example.com",
            Password="P@ssw0rd!",
            Size="Small",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]

        # Delete it
        del_resp = ds.delete_directory(DirectoryId=dir_id)
        assert del_resp["DirectoryId"] == dir_id

        # Should no longer be found
        with pytest.raises(ClientError) as exc_info:
            ds.describe_directories(DirectoryIds=[dir_id])
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDSTags:
    """Test tagging operations on directories."""

    def test_add_and_list_tags(self, ds, directory):
        """add_tags_to_resource adds tags that are visible via list_tags_for_resource."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )

        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    def test_remove_tags(self, ds, directory):
        """remove_tags_from_resource removes specified tag keys."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
                {"Key": "cost-center", "Value": "123"},
            ],
        )

        ds.remove_tags_from_resource(ResourceId=directory, TagKeys=["team", "cost-center"])

        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert "env" in tags
        assert "team" not in tags
        assert "cost-center" not in tags

    def test_list_tags_empty(self, ds, directory):
        """list_tags_for_resource on a directory with no tags returns empty list."""
        resp = ds.list_tags_for_resource(ResourceId=directory)
        assert resp["Tags"] == []


class TestDsAutoCoverage:
    """Auto-generated coverage tests for ds."""

    @pytest.fixture
    def client(self):
        return make_client("ds")

    def test_describe_trusts(self, client):
        """DescribeTrusts returns a response."""
        resp = client.describe_trusts()
        assert "Trusts" in resp

    def test_get_directory_limits(self, client):
        """GetDirectoryLimits returns a response."""
        resp = client.get_directory_limits()
        assert "DirectoryLimits" in resp

    def test_list_log_subscriptions(self, client):
        """ListLogSubscriptions returns a response."""
        resp = client.list_log_subscriptions()
        assert "LogSubscriptions" in resp


class TestDsLDAPSSettings:
    """Test LDAPS enable/disable operations."""

    def test_enable_ldaps_unsupported_directory_type(self, ds, directory):
        """EnableLDAPS on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.enable_ldaps(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"

    def test_disable_ldaps_unsupported_directory_type(self, ds, directory):
        """DisableLDAPS on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.disable_ldaps(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"

    def test_describe_ldaps_settings(self, ds, directory):
        """DescribeLDAPSSettings on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.describe_ldaps_settings(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"
